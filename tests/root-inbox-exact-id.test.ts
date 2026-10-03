import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext, ToolDefinition } from "@earendil-works/pi-coding-agent";
import { describe, expect, it, vi } from "vitest";
import piFabric from "../src/index.js";

// The tool registered by the public extension, real runtime/providers/inbox and real turn-start
// hooks. Only Pi's host surface is simulated; recovery shadows never call steer or followUp.
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
    const result = await tool.execute(`exact-id-${++call}`, { code, resultFormat: "json" }, undefined, undefined, context);
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

const setupConfig = (root: string) => {
  for (const key of Object.keys(process.env)) if (key.startsWith("PI_FABRIC_")) vi.stubEnv(key, undefined);
  vi.stubEnv("SMARTY_AGENT_NAME", undefined);
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
};
const publishOld = async (sender: Awaited<ReturnType<typeof main>>, to: string, text: string) => {
  // Age only the shadow; keep the live directory leases and router checks real.
  const clock = vi.spyOn(Date, "now").mockReturnValue(Date.now() - 61_000);
  try { return await sender.exec(`return await mesh.publish({ topic: "fleet.work.exact-recovery", kind: "ask",
    to: ${JSON.stringify(to)}, text: ${JSON.stringify(text)}, data: { key: ${JSON.stringify(text)} } });`); }
  finally { clock.mockRestore(); }
};

describe("public fabric_exec exact-id missed-delivery recovery (#3860 scope cut)", () => {
  it.each(["main", "handoff-lead", "unique-lead"])("ignores name shadow %s after grace and recovers only the exact id", async (name) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-inbox-exact-id-"));
    setupConfig(root);
    const sender = await main(root, "aaaaaaaa-0000-4000-8000-000000000001", "sender-lead");
    const recipient = await main(root, "bbbbbbbb-0000-4000-8000-000000000002", name === "main" ? undefined : name);
    try {
      await sender.emit("session_start"); await recipient.emit("session_start");
      await sender.exec("return await agents.self();");
      const self = await recipient.exec("return await agents.self();");
      let probe = 0;
      await vi.waitFor(async () => expect(await sender.exec(`return await agents.members({ kinds: ["root"], name: ${JSON.stringify(name)} }); // ${++probe}`))
        .toEqual([expect.objectContaining({ id: self.id, name })]), { timeout: 8000, interval: 100 });
      if (name === "handoff-lead") await sender.exec(`return await agents.create({ name: "handoff-lead", instructions: "Stay idle", kernel: "typescript" });`);
      await publishOld(sender, name, "NAME_SHADOW_MUST_NOT_INJECT");
      if (name === "handoff-lead") {
        const route = await sender.exec(`try { await agents.followUp({ id: "handoff-lead", message: "REFUSED_ROUTE" }); }
          catch (error) { return { error: String(error) }; } return { error: "unexpected delivery" };`);
        expect(route.error).toContain("Ambiguous Fabric participant: handoff-lead");
      } else if (name === "main") {
        // `main` is caller-relative: successful direct delivery stays in the named sender.
        await sender.exec(`return await agents.followUp({ id: "main", message: "LOCAL_MAIN_DIRECT" });`);
        expect(sender.sendMessage).toHaveBeenCalledTimes(1);
      }
      // A refused route writes newer ops events ahead of the aged exact shadow.
      // Drain past their grace too; live recipient/conflict selection was asserted above.
      const drain = async () => {
        const clock = vi.spyOn(Date, "now").mockReturnValue(Date.now() + 61_000);
        try { await sender.prompt(); await recipient.prompt(); } finally { clock.mockRestore(); }
      };
      await drain();
      expect(recipient.sendMessage).not.toHaveBeenCalled();
      expect(sender.sendMessage).toHaveBeenCalledTimes(name === "main" ? 1 : 0);
      const exact = await publishOld(sender, self.id, "EXACT_ID_RECOVERED");
      await drain();
      expect(recipient.sendMessage).toHaveBeenCalledTimes(1);
      expect(recipient.sendMessage).toHaveBeenLastCalledWith(expect.objectContaining({
        customType: "pi-fabric-inbox", details: expect.objectContaining({ ids: [exact.id] }),
      }), expect.objectContaining({ deliverAs: "nextTurn", triggerTurn: false }));
      await recipient.prompt();
      expect(recipient.sendMessage).toHaveBeenCalledTimes(1);
    } finally {
      await recipient.emit("session_shutdown"); await sender.emit("session_shutdown");
      vi.restoreAllMocks(); vi.unstubAllEnvs();
      fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
    }
  }, 30_000);

  it("publishes the validated launch name but never uses it as an inbox alias", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-inbox-launch-name-"));
    setupConfig(root);
    vi.stubEnv("SMARTY_AGENT_NAME", "fabric-v2");
    vi.stubEnv("SMARTY_ROLE", "project-agent@x");
    const owner = await main(root, "cccccccc-0000-4000-8000-000000000003", "different-pi-name");
    try {
      await owner.emit("session_start");
      const self = await owner.exec("return await agents.self();");
      expect(self).toMatchObject({ id: "session:cccccccc-0000-4000-8000-000000000003", name: "fabric-v2" });
      for (const name of ["fabric-v2", "main", "different-pi-name", "project-agent"])
        await publishOld(owner, name, "IGNORED_NAME_" + name);
      await owner.prompt();
      expect(owner.sendMessage).not.toHaveBeenCalled();
      const exact = await publishOld(owner, self.id, "EXACT_LAUNCH_RECOVERY");
      await owner.prompt();
      expect(owner.sendMessage).toHaveBeenCalledTimes(1);
      expect(owner.sendMessage.mock.calls[0]![0].details.ids).toEqual([exact.id]);
    } finally {
      await owner.emit("session_shutdown");
      vi.restoreAllMocks(); vi.unstubAllEnvs();
      fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
    }
  }, 30_000);
});
