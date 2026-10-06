import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext, SessionEntry } from "@earendil-works/pi-coding-agent";
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

async function fixture(enabled: boolean, memoryEnabled = true) {
  const cwd = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-extractive-hooks-"));
  const agentDir = path.join(cwd, "agent");
  fs.mkdirSync(agentDir);
  fs.mkdirSync(path.join(cwd, ".pi"));
  fs.writeFileSync(path.join(cwd, ".pi/fabric.json"), JSON.stringify({ fullCodeMode: false, memory: { enabled: memoryEnabled, extractive: { enabled } }, prewalk: { alwaysRearm: false }, mesh: { enabled: false }, components: [] }));
  vi.stubEnv("PI_CODING_AGENT_DIR", agentDir);
  for (const key of ["PI_FABRIC_PARENT_RUN", "PI_FABRIC_ACTOR_ID", "PI_FABRIC_DEPTH", "PI_FABRIC_CAPABILITY_REQUIREMENTS", "PI_FABRIC_CAPABILITY_DIGEST"]) vi.stubEnv(key, undefined);
  const handlers = new Map<string, Handler[]>();
  const listeners = new Map<string, (() => void)[]>();
  const pi = {
    events: { emit: vi.fn((name) => listeners.get(name)?.forEach((fn) => fn())), on: vi.fn((name, fn) => { listeners.set(name, [...(listeners.get(name) ?? []), fn]); return () => {}; }) },
    on: vi.fn((name, fn) => { handlers.set(name, [...(handlers.get(name) ?? []), fn]); }),
    getActiveTools: vi.fn(() => []), getAllTools: vi.fn(() => []),
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
  const request = async (messages: any[]) => {
    for (const handler of handlers.get("context") ?? []) messages = (await handler({ messages }, context))?.messages ?? messages;
    return messages;
  };
  const before = () => emit("before_agent_start", { prompt: "current prompt", systemPrompt: "system", systemPromptOptions: { skills: [] } });
  const { default: register } = await import("../src/extension-bootstrap.js");
  await register(pi);
  cleanups.push(async () => { try { await emit("session_shutdown"); } finally { fs.rmSync(cwd, { recursive: true, force: true }); } });
  return { pi, context, classify, emit, before, request, controller, set branch(value: SessionEntry[]) { branch = value; } };
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
    expect(messages.at(-1)).toBe(prompt);
    expect(await f.request(messages)).toEqual(messages);
    await f.before();
    expect(f.classify).toHaveBeenCalledTimes(1);
    expect(f.pi.sendMessage).not.toHaveBeenCalled();
    expect(f.pi.appendEntry).not.toHaveBeenCalled();
    f.pi.events.emit("pi-fabric:extractive-config-changed", {});
    expect(await f.request(messages)).toEqual([prompt]);
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
