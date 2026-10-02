import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { normalizeFabricConfig } from "../src/config.js";
import { FabricRuntimeState } from "../src/fabric-runtime-state.js";
import { completionConsumed, pendingCompletions, saveCompletion, saveWorkerCompletion, type CompletionRecipient } from "../src/agents/completion-journal.js";
import type { AgentRunResult } from "../src/agents/types.js";
import { ResidencyClient } from "../src/residency/client.js";
import { rootParticipantName } from "../src/topology/participant-name.js";

// Astra round 4 P1: return addresses must carry the same normalized Pi session name that
// presence publishes, so a named lane successor (not an unnamed same-cwd Main) recovers results.
const main = (cwd: string, sessionId: string, sessionName?: string) => {
  const handlers = new Map<string, Array<(event: unknown, ctx: ExtensionContext) => unknown>>();
  const sessionFile = path.join(cwd, `${sessionId}.jsonl`);
  fs.writeFileSync(sessionFile, JSON.stringify({ type: "session", id: sessionId }) + "\n");
  const sendMessage = vi.fn((message: Record<string, unknown>) =>
    fs.appendFileSync(sessionFile, JSON.stringify({ type: "custom_message", ...message }) + "\n"));
  const pi = {
    on: vi.fn((name: string, handler: (event: unknown, ctx: ExtensionContext) => unknown) => {
      handlers.set(name, [...(handlers.get(name) ?? []), handler]); return () => {};
    }),
    events: { emit: vi.fn() }, getThinkingLevel: () => "off", getSessionName: () => sessionName, sendMessage,
  } as unknown as ExtensionAPI;
  const model = { provider: "fake", id: "fake-model", name: "fake", api: "openai-completions", reasoning: false,
    input: ["text"], contextWindow: 100_000, maxTokens: 1000, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } };
  const context = { cwd, mode: "rpc", hasUI: false, isProjectTrusted: () => true, model,
    isIdle: () => true, hasPendingMessages: () => false,
    modelRegistry: { getAvailable: () => [model], getAll: () => [model], find: (provider: string, id: string) => provider === model.provider && id === model.id ? model : undefined,
      getApiKeyAndHeaders: async () => ({ ok: true }), refresh: () => {} },
    sessionManager: { getSessionId: () => sessionId, getSessionFile: () => sessionFile,
      getBranch: () => [], getLeafId: () => null, getEntries: () => [] },
    ui: { setStatus: vi.fn(), notify: vi.fn() },
  } as unknown as ExtensionContext;
  const runtime = new FabricRuntimeState(pi, new CapturedToolCatalog(), { paths: {
    extension: path.join(cwd, "unused-extension.mjs"), worker: path.resolve("tests/fixtures/fake-worker.mjs"),
    residentHost: path.join(cwd, "unused-resident.mjs"), skills: cwd,
  } });
  const turn = () => { for (const handler of handlers.get("turn_end") ?? [])
    handler({ message: { role: "assistant", stopReason: "stop" } }, context); };
  const texts = () => sendMessage.mock.calls.map(call => String((call[0] as { content?: unknown }).content ?? ""));
  const status = (id: string) => runtime.registry.invoke("agents.status", { id }, {
    cwd, signal: undefined, parentToolCallId: "named-successor", nestedToolCallId: "agents.status",
    extensionContext: context, update() {}, approve: async () => {}, audits: [], maxResultChars: 100_000,
  }).then(value => JSON.stringify(value), (error: unknown) => String(error));
  return { runtime, context, sendMessage, turn, texts, status, id: `session:${sessionId}` };
};

const roots: string[] = [];
afterEach(() => {
  vi.restoreAllMocks(); vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
});
const pause = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

describe("named lane completion succession through FabricRuntimeState", () => {
  it("normalizes return-address names exactly like published presence", () => {
    expect(rootParticipantName("  probe-lane  ")).toBe("probe-lane");
    for (const value of [undefined, "", "  ", "bad/name", "a".repeat(61)]) expect(rootParticipantName(value)).toBe("main");
  });

  it("a named successor receives ordinary and durable orphan results once; unnamed and differently named Mains on the cwd do not", async () => {
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-named-successor-"))); roots.push(root);
    for (const key of Object.keys(process.env)) if (key.startsWith("PI_FABRIC_")) vi.stubEnv(key, undefined);
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(root, "agent"));
    vi.stubEnv("PI_FABRIC_RUN_ROOT", path.join(root, "runs"));
    const meshRoot = path.join(root, "mesh");
    const config = normalizeFabricConfig({ fullCodeMode: false,
      mesh: { enabled: true, root: meshRoot, actorPollMs: 20 },
      agents: { enabled: true, nice: 19, sessionExport: false }, records: { enabled: false },
      mcp: { enabled: false }, memory: { enabled: false }, jev: { enabled: false },
      prewalk: { enabled: false, alwaysRearm: false },
    });
    const clients: ResidencyClient[] = [];
    const start = ResidencyClient.prototype.start;
    vi.spyOn(ResidencyClient.prototype, "start").mockImplementation(function (this: ResidencyClient) {
      clients.push(this); return start.call(this);
    });
    const a = main(root, "aaaaaaaa-0000-4000-8000-00000000000a", "probe-lane");
    const started: Array<ReturnType<typeof main>> = [a];
    try {
      await a.runtime.initialize(a.context, config);
      // Ordinary return address, as persisted by the production launch path.
      const handle = await a.runtime.agents.spawn({ task: "ordinary", transport: "process", nice: 19, model: "fake/fake-model" });
      await a.runtime.agents.wait(handle.id);
      const manifest = JSON.parse(fs.readFileSync(path.join(root, "runs", handle.id, "completion-recipient.json"), "utf8")) as { recipient: CompletionRecipient };
      expect(manifest.recipient).toMatchObject({ rootId: a.id, name: "probe-lane" });
      // Durable (resident host/journal) return address, as configured by production initialization.
      const resident = clients.find(client => client.options.config.rootId === a.id)!;
      expect(resident.options.config.mainName).toBe("probe-lane");
      const durableRecipient: CompletionRecipient = { ...manifest.recipient, name: resident.options.config.mainName!,
        startedAt: resident.options.config.mainStartedAt! };
      await a.runtime.shutdown(); started.shift();

      const result = (id: string, text: string): AgentRunResult => ({ id, name: `child ${id.slice(0, 4)}`, task: "work", status: "completed",
        runner: "pi", transport: "process", cwd: root, startedAt: 1, updatedAt: 2, finishedAt: 2, turns: 1, toolCalls: 0,
        text, usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 } });
      const ordinary = result("b".repeat(32), "ORDINARY_NAMED_RESULT");
      const durable = result("c".repeat(32), "DURABLE_NAMED_RESULT");
      // The orphan worker journals through its host-owned launch manifest (supervisor gone).
      const run = path.join(root, "orphan-run"); fs.mkdirSync(run);
      fs.writeFileSync(path.join(run, "completion-recipient.json"), JSON.stringify({ meshRoot, recipient: manifest.recipient, supervisor: { pid: 2147483647 } }));
      saveWorkerCompletion(path.join(run, "status.json"), ordinary);
      saveCompletion(meshRoot, durableRecipient, durable);
      expect(pendingCompletions(meshRoot, root)).toHaveLength(2);

      await pause(20);
      const unnamed = main(root, "bbbbbbbb-0000-4000-8000-00000000000b"); started.push(unnamed);
      const other = main(root, "cccccccc-0000-4000-8000-00000000000c", "other-lane"); started.push(other);
      await unnamed.runtime.initialize(unnamed.context, config);
      await other.runtime.initialize(other.context, config);
      await pause(400); unnamed.turn(); other.turn();
      for (const bystander of [unnamed, other]) {
        expect(bystander.sendMessage).not.toHaveBeenCalled();
        for (const value of [ordinary, durable]) expect(await bystander.status(value.id)).not.toContain(value.text!);
      }
      for (const value of [ordinary, durable]) expect(completionConsumed(meshRoot, value.id)).toBe(false);

      const successor = main(root, "dddddddd-0000-4000-8000-00000000000d", "probe-lane"); started.push(successor);
      await successor.runtime.initialize(successor.context, config);
      await vi.waitFor(() => {
        successor.turn();
        const delivered = successor.texts().join("\n");
        expect(delivered).toContain(ordinary.text); expect(delivered).toContain(durable.text);
      }, { timeout: 8000, interval: 50 });
      await vi.waitFor(() => { for (const value of [ordinary, durable]) expect(completionConsumed(meshRoot, value.id)).toBe(true); },
        { timeout: 8000, interval: 50 });
      for (const value of [ordinary, durable]) expect(await successor.status(value.id)).toContain(value.text!);
      successor.turn(); successor.turn(); await pause(200); unnamed.turn(); other.turn();
      for (const value of [ordinary, durable])
        expect(successor.texts().filter(text => text.includes(value.text!))).toHaveLength(1);
      expect(unnamed.sendMessage).not.toHaveBeenCalled(); expect(other.sendMessage).not.toHaveBeenCalled();
    } finally {
      for (const value of started.reverse()) await value.runtime.shutdown();
    }
  }, 40_000);
});
