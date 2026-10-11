import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { convertToLlm, ExtensionRunner, type ExtensionAPI, type ExtensionContext, type SessionEntry } from "@earendil-works/pi-coding-agent";
import type { ClassifierContext } from "@earendil-works/pi-ai";
import { afterEach, describe, expect, it, vi } from "vitest";

const loaded = vi.hoisted(() => vi.fn());
vi.mock("../src/memory/extractive-history.js", async (original) => {
  loaded();
  return original<typeof import("../src/memory/extractive-history.js")>();
});
type Handler = (event: any, context: ExtensionContext) => any;
const cleanups: (() => Promise<void>)[] = [];
afterEach(async () => {
  for (const cleanup of cleanups.splice(0)) await cleanup();
  vi.unstubAllEnvs();
});

async function fixture(enabled: boolean, memoryEnabled = true, fullCodeMode = false) {
  const cwd = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-extractive-hooks-"));
  const agentDir = path.join(cwd, "agent");
  fs.mkdirSync(agentDir);
  fs.mkdirSync(path.join(cwd, ".pi"));
  fs.writeFileSync(path.join(cwd, ".pi/fabric.json"), JSON.stringify({ fullCodeMode, memory: { enabled: memoryEnabled, extractive: { enabled } }, prewalk: { alwaysRearm: false }, mesh: { enabled: false }, components: [] }));
  vi.stubEnv("PI_CODING_AGENT_DIR", agentDir);
  for (const key of ["PI_FABRIC_PARENT_RUN", "PI_FABRIC_ACTOR_ID", "PI_FABRIC_DEPTH", "PI_FABRIC_CAPABILITY_REQUIREMENTS", "PI_FABRIC_CAPABILITY_DIGEST"]) vi.stubEnv(key, undefined);
  const handlers = new Map<string, Handler[]>();
  const listeners = new Map<string, (() => void)[]>();
  const pi = {
    events: { emit: vi.fn((name) => listeners.get(name)?.forEach((fn) => fn())), on: vi.fn((name, fn) => { listeners.set(name, [...(listeners.get(name) ?? []), fn]); return () => {}; }) },
    on: vi.fn((name, fn) => { handlers.set(name, [...(handlers.get(name) ?? []), fn]); }),
    getActiveTools: vi.fn(() => fullCodeMode ? ["fabric_exec"] : []), getAllTools: vi.fn(() => []),
    registerCommand: vi.fn(), registerMessageRenderer: vi.fn(), registerTool: vi.fn(),
    setActiveTools: vi.fn(), sendMessage: vi.fn(), appendEntry: vi.fn(), getThinkingLevel: vi.fn(() => "off"),
  } as unknown as ExtensionAPI;
  let branch = [{ type: "message", id: "u1", parentId: null, timestamp: "2025-01-01", message: { role: "user", content: "Do this only after review.", timestamp: 0 } }] as SessionEntry[];
  const classify = vi.fn(async (_model: unknown, request: ClassifierContext) => ({ stopReason: "stop", answers: Object.fromEntries(Object.keys(request.questions).map((id) => [id, { type: "bool", probability: 1 }])), timestamp: 0 }));
  const model = { type: "classifier", provider: "typesafe", id: "jev-latest" };
  const controller = new AbortController();
  const context = {
    cwd, hasUI: false, signal: controller.signal, isProjectTrusted: () => true,
    sessionManager: { getSessionId: () => cwd, getBranch: vi.fn(() => branch), getLeafId: () => branch.at(-1)?.id, getSessionFile: () => undefined },
    modelRegistry: { classify, getModelOfType: () => model, getAvailableOfType: async () => [model] },
    ui: { setStatus: vi.fn(), notify: vi.fn() },
  } as unknown as ExtensionContext;
  const emit = async (name: string, event: unknown = {}) => {
    const results = [];
    for (const handler of handlers.get(name) ?? []) results.push(await handler(event, context));
    return results;
  };
  // Exercise the host's cloning and system/tool restoration, not a mock merge.
  const runner = {
    extensions: [{ path: "extractive-test", handlers }], createContext: () => context,
    emitError: (error: { error: string }) => { throw new Error(error.error); },
  } as unknown as ExtensionRunner;
  const request = async (messages: any[]): Promise<any[]> => ExtensionRunner.prototype.emitContext.call(runner, messages);
  const before = (prompt = "current prompt") => emit("before_agent_start", { prompt, systemPrompt: "system", systemPromptOptions: { skills: [] } });
  const { default: register } = await import("../src/extension-bootstrap.js");
  await register(pi);
  cleanups.push(async () => { try { await emit("session_shutdown"); } finally { fs.rmSync(cwd, { recursive: true, force: true }); } });
  return { pi, context, classify, emit, before, request, controller, get branch() { return branch; }, set branch(value: SessionEntry[]) { branch = value; } };
}

describe("extractive extension integration", () => {
  it("default-off and memory-off register idle hooks without importing extraction or calling classifier", async () => {
    vi.resetModules(); loaded.mockClear();
    for (const [enabled, memoryEnabled] of [[false, true], [true, false]]) {
      const f = await fixture(enabled!, memoryEnabled!);
      await f.emit("session_start");
      await f.before();
      const prompt = { role: "user", content: "NEW PROMPT", timestamp: 10 };
      expect(await f.request([prompt])).toEqual([prompt]);
      expect(f.classify).not.toHaveBeenCalled();
    }
    expect(loaded).not.toHaveBeenCalled();
  });

  it("first-use loads once, preserves new prompt, injects one replay-safe ephemeral advisory and cancels on settings change", async () => {
    vi.resetModules(); loaded.mockClear();
    const f = await fixture(true);
    await f.emit("session_start");
    expect(loaded).not.toHaveBeenCalled();
    await f.before();
    expect(loaded).toHaveBeenCalledTimes(1);
    expect(f.classify).toHaveBeenCalledTimes(1);
    const prompt = { role: "user", content: "NEW PROMPT", timestamp: 10 };
    const messages = await f.request([prompt]);
    const view = messages.find((m) => m.customType === "fabric-extractive-history");
    expect(view.role).toBe("custom");
    expect(JSON.parse(view.content).kind).toBe("untrusted-historical-evidence");
    expect(messages.at(-1)).toEqual(prompt);
    expect(await f.request(messages)).toEqual(messages);
    await f.before();
    expect(f.classify).toHaveBeenCalledTimes(1);
    expect(f.pi.sendMessage).not.toHaveBeenCalled();
    expect(f.pi.appendEntry).not.toHaveBeenCalled();
    f.pi.events.emit("pi-fabric:extractive-config-changed", {});
    expect(await f.request(messages)).toEqual([prompt]);
  });

  it("preserves the old provider prefix across turns while replacing only the ephemeral suffix", async () => {
    const f = await fixture(true);
    await f.emit("session_start");
    const system = { role: "system", content: "STATIC SYSTEM", toolsAdded: [{ name: "read", description: "Read", parameters: { type: "object" } }], timestamp: 0 };
    const oldUser = { role: "user", content: "old history ".repeat(1000), timestamp: 1 };
    const oldReply = { role: "assistant", content: [{ type: "text", text: "old answer" }], timestamp: 2 };
    const prompt = { role: "user", content: "first query", timestamp: 3 };
    const history = [system, oldUser, oldReply];
    const beforeBytes = JSON.stringify(history);
    await f.before("first query");
    const first = await f.request([...history, prompt]);
    expect(first.slice(0, history.length)).toEqual(history);
    expect(first[history.length].customType).toBe("fabric-extractive-history");
    const reply = { role: "assistant", content: [{ type: "text", text: "new answer" }], timestamp: 4 };
    await f.emit("agent_end");
    f.branch = [...f.branch, { type: "message", id: "u2", parentId: "u1", timestamp: "2025-01-02", message: prompt } as SessionEntry];
    await f.before("second query");
    const nextPrompt = { role: "user", content: "second query", timestamp: 5 };
    // Also strip an old advisory if another context handler replays it.
    const second = await f.request([...first, reply, nextPrompt]);
    const stable = [...history, prompt, reply];
    expect(second.slice(0, stable.length)).toEqual(stable);
    expect(second[stable.length].customType).toBe("fabric-extractive-history");
    expect(second.at(-1)).toEqual(nextPrompt);
    expect(second.filter((m) => m.customType === "fabric-extractive-history")).toHaveLength(1);
    expect(JSON.stringify(convertToLlm(first.slice(0, history.length)))).toBe(JSON.stringify(convertToLlm(second.slice(0, history.length))));
    expect(JSON.stringify(history)).toBe(beforeBytes);
    expect(f.pi.sendMessage).not.toHaveBeenCalled();
    expect(f.pi.appendEntry).not.toHaveBeenCalled();
  });

  it("keeps one anchored advisory through cloned replays, tool pairs and mid-turn steering", async () => {
    const f = await fixture(true);
    await f.emit("session_start");
    await f.before();
    const past = { role: "user", content: "past", timestamp: 1 };
    const prompt = { role: "user", content: "start", timestamp: 2 };
    const first = await f.request([past, prompt]);
    const work = [
      { role: "assistant", content: [{ type: "toolCall", id: "call", name: "read", arguments: {} }], timestamp: 3 },
      { role: "toolResult", toolCallId: "call", toolName: "read", content: [{ type: "text", text: "result" }], isError: false, timestamp: 4 },
      { role: "user", content: "steering", timestamp: 5 },
    ];
    const extended = [...first, ...work];
    expect(first[1].customType).toBe("fabric-extractive-history");
    expect(await f.request(structuredClone(extended))).toEqual(extended);
    // Real requests start from the raw transcript, without our previous projection.
    expect(await f.request([past, prompt, ...work])).toEqual(extended);
    expect(f.classify).toHaveBeenCalledTimes(1);
  });

  it.each(["removed", "rewritten", "compacted"])("omits instead of relocating when the anchored prefix is %s", async (change) => {
    const f = await fixture(true);
    await f.emit("session_start");
    await f.before();
    const past = { role: "user", content: "past", timestamp: 1 };
    const prompt = { role: "user", content: "start", timestamp: 2 };
    const first = await f.request([past, prompt]);
    const steering = { role: "user", content: "steering", timestamp: 3 };
    const changed = change === "removed" ? [past, steering]
      : change === "rewritten" ? [{ ...past, content: "edited" }, prompt, steering]
      : [{ role: "compactionSummary", summary: "summary", tokensBefore: 100, timestamp: 0 }, prompt, steering];
    const advisory = first.find((m) => m.customType === "fabric-extractive-history");
    expect(await f.request([advisory, ...changed])).toEqual(changed);
    expect(await f.request([past, prompt, steering])).toEqual([past, prompt, steering]);
    // Only a new preparation can establish a new boundary after compaction.
    await f.before("new turn");
    const next = await f.request(changed);
    expect(next.at(-2).customType).toBe("fabric-extractive-history");
    expect(next.at(-1)).toEqual(steering);
    expect(f.classify).toHaveBeenCalledTimes(1);
  });

  it("omits the view when the first request has no user boundary", async () => {
    const f = await fixture(true);
    await f.emit("session_start");
    await f.before();
    const messages = [{ role: "compactionSummary", summary: "summary", tokensBefore: 100, timestamp: 0 }];
    expect(await f.request(messages)).toEqual(messages);
    const later = [...messages, { role: "user", content: "later", timestamp: 1 }];
    expect(await f.request(later)).toEqual(later);
  });

  it("never changes full-code system prompt bytes or prompt options when extraction changes", async () => {
    const prompts: string[] = [];
    const systems: string[] = [];
    for (const enabled of [false, true]) {
      const f = await fixture(enabled, true, true);
      await f.emit("session_start");
      for (const query of ["first query", "different second query"]) {
        const options = Object.freeze({ skills: Object.freeze([]) });
        const event = Object.freeze({ prompt: query, systemPrompt: "STATIC SYSTEM", systemPromptOptions: options });
        const results = await f.emit("before_agent_start", event);
        const forced = results.filter((r) => r?.systemPrompt !== undefined);
        expect(forced).toHaveLength(1);
        prompts.push(forced[0].systemPrompt);
        expect(event.systemPromptOptions).toEqual({ skills: [] });
        const system = { role: "system", content: forced[0].systemPrompt, toolsAdded: [{ name: "fabric_exec", parameters: { type: "object" } }], timestamp: 0 };
        const request = await f.request([system, { role: "user", content: query, timestamp: 1 }]);
        expect(request[0].content).toBe(system.content);
        systems.push(JSON.stringify(convertToLlm(request).filter((m) => m.role === "system")));
        expect(request.filter((m) => m.customType === "fabric-extractive-history")).toHaveLength(enabled ? 1 : 0);
        await f.emit("agent_end");
        f.branch = [...f.branch, { type: "message", id: query, parentId: "u1", timestamp: "2025-01-02", message: { role: "user", content: query, timestamp: 1 } } as SessionEntry];
      }
    }
    expect(new Set(prompts).size).toBe(1);
    expect(new Set(systems).size).toBe(1);
    expect(prompts[0]).not.toContain("untrusted-historical-evidence");
    expect(prompts[0]).not.toContain("Do this only after review.");
  });

  it("branch navigation removes the previous advisory and abort prevents later injection", async () => {
    const f = await fixture(true);
    await f.emit("session_start");
    await f.before();
    const messages = await f.request([{ role: "user", content: "current", timestamp: 0 }]);
    f.branch = [];
    await f.emit("session_tree");
    expect((await f.request(messages)).some((m) => m.customType === "fabric-extractive-history")).toBe(false);
    f.controller.abort();
    await f.before();
    expect((await f.request(messages)).some((m) => m.customType === "fabric-extractive-history")).toBe(false);
  });
});
