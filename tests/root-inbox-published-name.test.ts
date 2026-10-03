import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext, ToolDefinition } from "@earendil-works/pi-coding-agent";
import { describe, expect, it, vi } from "vitest";
import piFabric from "../src/index.js";

// The tool registered by the public extension, real runtime/providers/inbox and real turn-start
// hooks. Only Pi's host surface is simulated; publication never calls steer or followUp.
const main = async (cwd: string, sessionId: string, initialName?: string) => {
  let sessionName = initialName;
  const entries: unknown[] = [];
  const queued: unknown[] = [];
  const tools = new Map<string, ToolDefinition<any, any, any>>();
  const handlers = new Map<string, Array<(event: any, context: ExtensionContext) => unknown>>();
  const sendMessage = vi.fn((message: any, options: any) => {
    if (options?.deliverAs === "nextTurn") queued.push(message);
    else entries.push({ type: "custom_message", ...message });
  });
  const pi = {
    hostCapabilities: { turnProvenance: 1 }, sendMessage,
    events: { emit: vi.fn(), on: vi.fn(() => () => {}) },
    getThinkingLevel: () => "off", getSessionName: () => sessionName,
    getActiveTools: () => ["fabric_exec"], getAllTools: () => [], setActiveTools: vi.fn(),
    registerCommand: vi.fn(), registerMessageRenderer: vi.fn(),
    registerTool: (tool: ToolDefinition<any, any, any>) => tools.set(tool.name, tool),
    on: (name: string, handler: (event: any, context: ExtensionContext) => unknown) => {
      handlers.set(name, [...(handlers.get(name) ?? []), handler]);
      return () => handlers.set(name, (handlers.get(name) ?? []).filter(h => h !== handler));
    },
  } as unknown as ExtensionAPI;
  const context = {
    cwd, mode: "rpc", hasUI: false, isProjectTrusted: () => true,
    // Do not let the idle timer compete with the explicit turn-start probe.
    isIdle: () => false, hasPendingMessages: () => false, getContextUsage: () => undefined,
    modelRegistry: { getAvailable: () => [], find: () => undefined, getApiKeyAndHeaders: async () => ({ ok: true }) },
    sessionManager: { getSessionId: () => sessionId, getSessionFile: () => undefined,
      isPersisted: () => false, getBranch: () => entries, getEntries: () => entries, getLeafId: () => null },
    ui: { setStatus: vi.fn(), notify: vi.fn() },
  } as unknown as ExtensionContext;
  const emit = async (name: string, event: any = {}) => {
    for (const handler of handlers.get(name) ?? []) await handler(event, context);
  };
  let call = 0;
  const exec = async (code: string) => {
    const tool = tools.get("fabric_exec")!;
    expect(tool?.name).toBe("fabric_exec");
    const result = await tool.execute(`published-name-${++call}`, { code, resultFormat: "json" }, undefined, undefined, context);
    const text = result.content.filter(block => block.type === "text").map(block => block.text).join("\n");
    expect((result as { isError?: boolean }).isError, text).not.toBe(true);
    return JSON.parse(text);
  };
  await piFabric(pi);
  return { exec, emit, entries, sendMessage, rename: (name?: string) => { sessionName = name; },
    prompt: async () => {
      await emit("before_agent_start", { prompt: "recover work", systemPrompt: "",
        systemPromptOptions: { sections: {} } });
      // Capable Pi consumes nextTurn after hooks, before the first model inference.
      for (const message of queued.splice(0)) entries.push({ type: "custom_message", ...(message as object) });
    },
  };
};

describe("public fabric_exec published-name missed-delivery recovery (#3860)", () => {
  it.each([undefined, "duplicate-lead"])("refuses ambiguous published name %j in both inboxes after steer grace", async (name) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-inbox-ambiguous-name-"));
    for (const key of Object.keys(process.env)) if (key.startsWith("PI_FABRIC_")) vi.stubEnv(key, undefined);
    vi.stubEnv("SMARTY_AGENT_NAME", name);
    vi.stubEnv("SMARTY_ROLE", undefined);
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(root, "agent"));
    fs.mkdirSync(path.join(root, "agent"));
    fs.writeFileSync(path.join(root, "agent", "fabric.json"), JSON.stringify({
      autoReload: false, fullCodeMode: false, executor: { kernel: "typescript" }, ui: { enabled: false },
      mesh: { enabled: true, root: path.join(root, "mesh"), followUpFlushMs: 0 },
      agents: { enabled: false }, residency: { enabled: false }, records: { enabled: false },
      mcp: { enabled: false }, memory: { enabled: false }, jev: { enabled: false },
      prewalk: { enabled: false, alwaysRearm: false }, entropy: { compile: false }, speculation: { enabled: false },
    }));
    const first = await main(root, "aaaaaaaa-0000-4000-8000-000000000001");
    const second = await main(root, "bbbbbbbb-0000-4000-8000-000000000002");
    // Two actual launch-named roots share the environment in this host fixture. Use one
    // as the publisher; a third runtime would inherit the same name, unlike a real process.
    const sender = name ? first : await main(root, "cccccccc-0000-4000-8000-000000000003", "sender");
    try {
      await first.emit("session_start"); await second.emit("session_start");
      if (sender !== first) await sender.emit("session_start");
      await first.exec("return await agents.self();"); await second.exec("return await agents.self();");
      const alias = name ?? "main";
      let probe = 0;
      let roots: Array<{ id: string }> = [];
      const members = () => sender.exec(`return await agents.members({ kinds: ["root"], name: ${JSON.stringify(alias)} }); // roster ${++probe}`);
      await vi.waitFor(async () => { roots = await members(); expect(roots).toHaveLength(2); }, { timeout: 8000, interval: 100 });
      expect(roots.map((p: { id: string }) => p.id).sort()).toEqual([
        "session:aaaaaaaa-0000-4000-8000-000000000001", "session:bbbbbbbb-0000-4000-8000-000000000002",
      ]);
      const publish = async (to: string, text: string) => {
        // Age only the shadow. Advancing the directory clock would expire its live leases
        // and make an ambiguity test pass for the wrong reason (no live name matches).
        const clock = vi.spyOn(Date, "now").mockReturnValue(Date.now() - 61_000);
        try { return await sender.exec(`return await mesh.publish({
          topic: "fleet.work.ambiguous-recovery", kind: "ask", to: ${JSON.stringify(to)},
          text: ${JSON.stringify(text)}, data: { key: ${JSON.stringify(text)} } });`); }
        finally { clock.mockRestore(); }
      };
      const shadow = await publish(alias, "ambiguous handoff must never arrive");
      if (name) {
        const failure = await sender.exec(`try { await agents.followUp({ id: ${JSON.stringify(alias)}, message: "refused ambiguous route" }); }
          catch (error) { return { failure: String(error) }; } return { failure: "unexpected delivery" };`);
        expect(failure.failure).toContain("Ambiguous Fabric participant: duplicate-lead");
      }
      // A young shadow is not evidence of refusal: explicitly drain BOTH roots past the real grace.
      const clock = vi.spyOn(Date, "now").mockReturnValue(shadow.createdAt);
      try { await first.prompt(); await second.prompt(); }
      finally { clock.mockRestore(); }
      expect(first.sendMessage).not.toHaveBeenCalled(); expect(second.sendMessage).not.toHaveBeenCalled();
      // Leases are live at the normal clock while the shadow is now older than 60 seconds.
      expect(await members()).toHaveLength(2);
      await first.prompt(); await second.prompt();
      expect(first.sendMessage).not.toHaveBeenCalled(); expect(second.sendMessage).not.toHaveBeenCalled();
      // Canonical delivery remains available to each duplicate, and never crosses recipients.
      const exact = [];
      for (const p of roots) exact.push(await publish(p.id, "exact " + p.id));
      await first.prompt(); await second.prompt();
      for (const [recipient, id] of [[first, "session:aaaaaaaa-0000-4000-8000-000000000001"],
        [second, "session:bbbbbbbb-0000-4000-8000-000000000002"]] as const) {
        expect(recipient.sendMessage).toHaveBeenCalledTimes(1);
        expect(recipient.sendMessage).toHaveBeenLastCalledWith(expect.objectContaining({
          customType: "pi-fabric-inbox", details: expect.objectContaining({
            ids: [exact.find((event: { to: string }) => event.to === id).id],
          }),
        }), expect.objectContaining({ deliverAs: "nextTurn", triggerTurn: false }));
      }
      await first.prompt(); await second.prompt();
      expect(first.sendMessage).toHaveBeenCalledTimes(1); expect(second.sendMessage).toHaveBeenCalledTimes(1);
      if (sender !== first) expect(sender.sendMessage).not.toHaveBeenCalled();
    } finally {
      if (sender !== first) await sender.emit("session_shutdown");
      await second.emit("session_shutdown"); await first.emit("session_shutdown");
      vi.restoreAllMocks(); vi.unstubAllEnvs();
      fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
    }
  }, 30_000);

  it.each([
    [undefined, undefined, undefined, "main"],
    [undefined, "project-agent@x", undefined, "main"],
    [undefined, "project-agent@x", "  explicit-lead  ", "explicit-lead"],
    ["fabric-v2", "project-agent@x", undefined, "fabric-v2"],
    ["fabric-v2", "project-agent@x", "  explicit-lead  ", "fabric-v2"],
    ["fabric-v2", "project-agent@x", "bad/name", "fabric-v2"],
    ["bad/name", "project-agent@x", "  explicit-lead  ", "explicit-lead"],
    ["fabric-v2@x", "project-agent@x", undefined, "main"],
  ] as const)("recovers SMARTY_AGENT_NAME=%j SMARTY_ROLE=%j Pi name=%j as %j", async (agentName, role, piName, publishedName) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-inbox-published-name-"));
    for (const key of Object.keys(process.env)) if (key.startsWith("PI_FABRIC_")) vi.stubEnv(key, undefined);
    vi.stubEnv("SMARTY_ROLE", role);
    vi.stubEnv("SMARTY_AGENT_NAME", agentName);
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(root, "agent"));
    fs.mkdirSync(path.join(root, "agent"));
    fs.writeFileSync(path.join(root, "agent", "fabric.json"), JSON.stringify({
      autoReload: false, fullCodeMode: false, executor: { kernel: "typescript" }, ui: { enabled: false },
      mesh: { enabled: true, root: path.join(root, "mesh"), followUpFlushMs: 0 },
      agents: { enabled: false }, residency: { enabled: false }, records: { enabled: false },
      mcp: { enabled: false }, memory: { enabled: false }, jev: { enabled: false },
      prewalk: { enabled: false, alwaysRearm: false }, entropy: { compile: false }, speculation: { enabled: false },
    }));
    const owner = await main(root, "aaaaaaaa-0000-4000-8000-000000000001", piName);
    // This is the unique-name fixture: another Main in this process would inherit the
    // same SMARTY_AGENT_NAME and correctly make the launch alias ambiguous.
    try {
      await owner.emit("session_start");
      const self = await owner.exec("return await agents.self();");
      expect(self).toMatchObject({ id: "session:aaaaaaaa-0000-4000-8000-000000000001", name: publishedName });
      const publish = async (to: string, text: string) => {
        const clock = vi.spyOn(Date, "now").mockReturnValue(Date.now() - 61_000);
        try { return await owner.exec(`return await mesh.publish({
          topic: "fleet.work.role-recovery", kind: "ask", to: ${JSON.stringify(to)},
          text: ${JSON.stringify(text)}, data: { key: ${JSON.stringify(text)} } });`); }
        finally { clock.mockRestore(); }
      };
      const event = await publish(publishedName, "missed role handoff");
      const youngClock = vi.spyOn(Date, "now").mockReturnValue(event.createdAt);
      try { await owner.prompt(); } finally { youngClock.mockRestore(); }
      expect(owner.sendMessage).not.toHaveBeenCalled(); // Real 60-second steer grace, not a receipt.
      const recover = async (ids: string[]) => {
        await owner.prompt();
        expect(owner.sendMessage).toHaveBeenLastCalledWith(expect.objectContaining({
          customType: "pi-fabric-inbox", details: expect.objectContaining({ ids }),
        }), expect.objectContaining({ deliverAs: "nextTurn", triggerTurn: false }));
        const calls = owner.sendMessage.mock.calls.length;
        await owner.prompt();
        expect(owner.sendMessage).toHaveBeenCalledTimes(calls); // Confirmed inbox receipt, no repeat.
      };
      await recover([event.id]);
      expect(owner.sendMessage.mock.calls[0]![0].content).toContain("missed role handoff");
      // Invalid/raw names were never published and must not be accepted as aliases.
      if (piName) {
        await publish(piName, "raw invalid alias must not arrive");
        await owner.prompt();
        expect(owner.sendMessage).toHaveBeenCalledTimes(1);
      }
      // The role stamp is metadata only; an invalid/unpublished launch name is not an alias either.
      for (const alias of [role?.split("@")[0], agentName]) {
        if (!alias || alias === publishedName) continue;
        await publish(alias, "unpublished launch alias must not arrive");
        await owner.prompt();
        expect(owner.sendMessage).toHaveBeenCalledTimes(1);
      }
      for (const [next, expected] of [["renamed-lead", agentName === "fabric-v2" ? "fabric-v2" : "renamed-lead"],
        [undefined, agentName === "fabric-v2" ? "fabric-v2" : "main"]] as const) {
        const previous = (await owner.exec(`return await agents.self(); // previous ${next}`)).name;
        owner.rename(next);
        // Presence is republished by the existing heartbeat, not synchronously by getSessionName.
        let probe = 0;
        await vi.waitFor(async () => expect(await owner.exec(`return await agents.self(); // rename probe ${++probe}`))
          .toMatchObject({ id: self.id, name: expected }), { timeout: 8000, interval: 500 });
        if (previous !== expected) await publish(previous, "old alias must not arrive " + previous);
        // A valid launch name stays fixed across Pi renames; use distinct work keys per probe.
        const current = await publish(expected, `current name ${expected} after ${next ?? "clear"}`);
        const exact = await publish(self.id, `exact id ${expected} after ${next ?? "clear"}`);
        await recover([current.id, exact.id]);
      }
    } finally {
      await owner.emit("session_shutdown");
      vi.restoreAllMocks(); vi.unstubAllEnvs();
      fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
    }
  }, 30_000);
});
