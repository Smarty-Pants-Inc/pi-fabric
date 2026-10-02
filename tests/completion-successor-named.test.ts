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
import { ResidentHost } from "../src/residency/host.js";
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
  return { runtime, context, sendMessage, turn, texts, status, rename: (name?: string) => { sessionName = name; }, id: `session:${sessionId}` };
};

const roots: string[] = [];
afterEach(() => {
  vi.restoreAllMocks(); vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
});
const pause = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

describe("named lane completion succession through FabricRuntimeState", () => {
  it.each([
    { admittedName: "old-lane", outcome: "stopped", missing: false },
    { admittedName: undefined, outcome: "stopped", missing: false },
    { admittedName: "old-lane", outcome: "failed", missing: false },
    { admittedName: "old-lane", outcome: "stopped", missing: true },
  ] as const)("round 6: prelaunch $outcome keeps admission name=$admittedName; missing=$missing fails closed", async ({ admittedName, outcome, missing }) => {
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-queued-successor-"))); roots.push(root);
    for (const key of Object.keys(process.env)) if (key.startsWith("PI_FABRIC_")) vi.stubEnv(key, undefined);
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(root, "agent"));
    vi.stubEnv("PI_FABRIC_RUN_ROOT", path.join(root, "runs"));
    const meshRoot = path.join(root, "mesh");
    const config = normalizeFabricConfig({ fullCodeMode: false,
      mesh: { enabled: true, root: meshRoot, actorPollMs: 20 },
      agents: { enabled: true, maxConcurrent: 1, nice: 19, sessionExport: false, retainRuns: true },
      records: { enabled: false }, mcp: { enabled: false }, memory: { enabled: false }, jev: { enabled: false },
      prewalk: { enabled: false, alwaysRearm: false },
    });
    const a = main(root, "aaaaaaaa-0000-4000-8000-00000000000a");
    a.context.isIdle = () => false;
    const started: Array<ReturnType<typeof main>> = [a];
    let queuedId: string | undefined;
    // Fault injection at the production publication boundary: discard only the
    // queued result's host-owned binding, without altering its private body.
    const enqueue = ResidencyClient.prototype.enqueueCompletion;
    if (missing) vi.spyOn(ResidencyClient.prototype, "enqueueCompletion").mockImplementation(function (this: ResidencyClient, result) {
      if (result.id === queuedId) return enqueue.call(this, result);
      return enqueue.apply(this, arguments as unknown as Parameters<typeof enqueue>);
    });
    try {
      await a.runtime.initialize(a.context, config);
      a.rename(admittedName);
      const blocker = await a.runtime.agents.spawn({ task: "HANG", name: "permit-holder", transport: "process", nice: 19, model: "fake/fake-model" });
      const privateTask = "ROUND6_PRIVATE_QUEUED_TASK";
      const queued = await a.runtime.agents.spawn({ task: privateTask, name: "queued-private", transport: "process", nice: 19, model: "fake/fake-model" });
      queuedId = queued.id;
      expect(queued.status).toBe("queued");
      expect(a.runtime.agents.runDirectory(queued.id)).toBeUndefined();
      a.rename("probe-lane");
      if (outcome === "failed") {
        const prepare = vi.spyOn(a.runtime.agents, "prepareModelForAdmission").mockRejectedValueOnce(new Error("ROUND6_PREMANIFEST_FAILURE"));
        await a.runtime.agents.stop(blocker.id);
        await vi.waitFor(() => expect(a.runtime.agents.status(queued.id).status).toBe("failed"), { timeout: 8000, interval: 20 });
        prepare.mockRestore();
      } else {
        const result = await a.runtime.agents.stop(queued.id);
        expect(result).toMatchObject({ task: privateTask, status: "stopped" });
      }
      expect(a.runtime.agents.runDirectory(queued.id)).toBeUndefined();
      expect(fs.existsSync(path.join(root, "runs", queued.id))).toBe(false);
      expect(a.runtime.agents.status(queued.id)).toMatchObject({ task: privateTask, status: outcome });
      const envelopes = pendingCompletions(meshRoot, root).filter(value => value.result.id === queued.id);
      if (missing) expect(envelopes).toEqual([]);
      else expect(envelopes).toMatchObject([{ recipient: { rootId: a.id, name: rootParticipantName(admittedName) }, result: { task: privateTask, status: outcome } }]);
      expect(completionConsumed(meshRoot, queued.id)).toBe(false);
      await a.runtime.shutdown(); started.shift();
      await pause(20);

      // For an unnamed admission, the unnamed lane IS the authorized successor.
      // For a named admission, an unrelated unnamed Main is a bystander too.
      const bystanderNames = admittedName ? [undefined, "other-lane", "probe-lane"] : ["other-lane", "probe-lane"];
      for (const [index, name] of bystanderNames.entries()) {
        const bystander = main(root, `bystander-${index}`, name); started.push(bystander);
        await bystander.runtime.initialize(bystander.context, config);
      }
      const bystanders = [...started];
      await pause(400);
      for (const bystander of bystanders) {
        bystander.turn();
        expect(bystander.texts().join("\n")).not.toContain(queued.id);
        expect(await bystander.status(queued.id)).not.toContain(privateTask);
        // An observer wait must not forge a consumption receipt either.
        const waited = await bystander.runtime.registry.invoke("agents.wait", { id: queued.id, timeoutMs: 100 }, {
          cwd: root, signal: AbortSignal.timeout(1000), parentToolCallId: "round6-wait", nestedToolCallId: "agents.wait",
          extensionContext: bystander.context, update() {}, approve: async () => {}, audits: [], maxResultChars: 100_000,
        }).then(value => JSON.stringify(value), (error: unknown) => String(error));
        expect(waited).not.toContain(privateTask);
        expect(completionConsumed(meshRoot, queued.id)).toBe(false);
      }
      const successor = main(root, "dddddddd-0000-4000-8000-00000000000d", admittedName); started.push(successor);
      await successor.runtime.initialize(successor.context, config);
      if (missing) {
        await pause(400); successor.turn();
        expect(successor.texts().join("\n")).not.toContain(queued.id);
        expect(await successor.status(queued.id)).not.toContain(privateTask);
        expect(completionConsumed(meshRoot, queued.id)).toBe(false);
        expect(pendingCompletions(meshRoot, root).some(value => value.result.id === queued.id)).toBe(false);
      } else {
        await vi.waitFor(() => { successor.turn(); expect(successor.texts().join("\n")).toContain(queued.id); }, { timeout: 8000, interval: 50 });
        expect(await successor.status(queued.id)).toContain(privateTask);
        await vi.waitFor(() => expect(completionConsumed(meshRoot, queued.id)).toBe(true), { timeout: 8000, interval: 50 });
        successor.turn(); await pause(100);
        expect(successor.texts().filter(text => text.includes(queued.id))).toHaveLength(1);
        for (const bystander of bystanders) expect(await bystander.status(queued.id)).not.toContain(privateTask);
      }
    } finally {
      for (const value of started.reverse()) await value.runtime.shutdown();
    }
  }, 40_000);
  it("binds ordinary and production ResidentHost results to a host rename before spawn, never the startup unnamed lane", async () => {
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-renamed-successor-"))); roots.push(root);
    for (const key of Object.keys(process.env)) if (key.startsWith("PI_FABRIC_")) vi.stubEnv(key, undefined);
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(root, "agent"));
    vi.stubEnv("PI_FABRIC_RUN_ROOT", path.join(root, "runs"));
    const meshRoot = path.join(root, "mesh");
    const config = normalizeFabricConfig({ fullCodeMode: false,
      mesh: { enabled: true, root: meshRoot, actorPollMs: 20 },
      agents: { enabled: true, nice: 19, sessionExport: false, retainRuns: true }, records: { enabled: false },
      mcp: { enabled: false }, memory: { enabled: false }, jev: { enabled: false },
      prewalk: { enabled: false, alwaysRearm: false },
    });
    const clients: ResidencyClient[] = [];
    const start = ResidencyClient.prototype.start;
    vi.spyOn(ResidencyClient.prototype, "start").mockImplementation(function (this: ResidencyClient) {
      clients.push(this); return start.call(this);
    });
    const a = main(root, "aaaaaaaa-0000-4000-8000-00000000000a");
    // Keep A busy so its ordinary inbox cannot consume results before it disappears.
    a.context.isIdle = () => false;
    const started: Array<ReturnType<typeof main>> = [a];
    let host: ResidentHost | undefined;
    try {
      await a.runtime.initialize(a.context, config);
      const resident = clients.find(client => client.options.config.rootId === a.id)!;
      expect(resident.options.config.mainName).toBe("main");
      // Start the real host while A is still unnamed: later launches must reread
      // the host-owned config, not the ResidentHost constructor's identity.
      host = new ResidentHost(structuredClone(resident.options.config));
      await host.start();
      a.rename("  probe-lane  ");
      const releasePath = path.join(root, "release-ordinary");
      const task = `LIVE_WITHOUT_PROGRESS ${JSON.stringify({ fakeWorkerReleasePath: releasePath })}`;
      const ordinary = await a.runtime.agents.spawn({ task, name: "ordinary-private", transport: "process", nice: 19, model: "fake/fake-model" });
      const durable = await resident.spawnAgent({ task, name: "durable-private", transport: "process", nice: 19, model: "fake/fake-model", residency: "durable" }, AbortSignal.timeout(8_000));
      const admittedName = (run: string) => JSON.parse(fs.readFileSync(path.join(run, "completion-recipient.json"), "utf8")).recipient.name;
      expect({ ordinary: admittedName(a.runtime.agents.runDirectory(ordinary.id)!),
        durable: admittedName(host.agents.runDirectory(durable.id)!) }).toEqual({ ordinary: "probe-lane", durable: "probe-lane" });
      // Both real workers are held until a later rename; settlement must keep
      // their admitted probe-lane address, including after config refresh.
      a.rename("post-spawn-lane");
      resident.syncPiModels();
      expect(resident.options.config.mainName).toBe("post-spawn-lane");
      fs.writeFileSync(releasePath, "release");
      await vi.waitFor(() => expect(pendingCompletions(meshRoot, root)).toHaveLength(2), { timeout: 8_000, interval: 50 });
      const results = pendingCompletions(meshRoot, root);
      expect(results.map(value => value.result.id).sort()).toEqual([ordinary.id, durable.id].sort());
      for (const envelope of results) expect(envelope.recipient).toMatchObject({ rootId: a.id, name: "probe-lane" });
      const durableRun = host.agents.runDirectory(durable.id)!;
      expect(JSON.parse(fs.readFileSync(path.join(durableRun, "completion-recipient.json"), "utf8")).recipient.name).toBe("probe-lane");
      for (const value of results) expect(completionConsumed(meshRoot, value.result.id)).toBe(false);
      await a.runtime.shutdown(); started.shift();
      await host.close(); host = undefined;

      await pause(20);
      const unnamed = main(root, "bbbbbbbb-0000-4000-8000-00000000000b"); started.push(unnamed);
      const other = main(root, "cccccccc-0000-4000-8000-00000000000c", "other-lane"); started.push(other);
      await unnamed.runtime.initialize(unnamed.context, config);
      await other.runtime.initialize(other.context, config);
      await pause(400); unnamed.turn(); other.turn();
      for (const bystander of [unnamed, other]) {
        expect(bystander.sendMessage).not.toHaveBeenCalled();
        for (const value of results) expect(await bystander.status(value.result.id)).not.toContain(value.result.text!);
      }
      for (const value of results) expect(completionConsumed(meshRoot, value.result.id)).toBe(false);
      // The claimant itself also starts unnamed and is renamed without reinitialization.
      const successor = main(root, "dddddddd-0000-4000-8000-00000000000d"); started.push(successor);
      await successor.runtime.initialize(successor.context, config);
      successor.rename("probe-lane");
      await vi.waitFor(() => {
        successor.turn();
        for (const value of results) expect(successor.texts().join("\n")).toContain(value.result.text);
      }, { timeout: 8_000, interval: 50 });
      await vi.waitFor(() => {
        for (const value of results) expect(completionConsumed(meshRoot, value.result.id)).toBe(true);
      }, { timeout: 8_000, interval: 50 });
      successor.turn(); successor.turn(); await pause(200); unnamed.turn(); other.turn();
      for (const value of results) {
        expect(await successor.status(value.result.id)).toContain(value.result.text!);
        expect(successor.texts().filter(text => text.includes(value.result.id))).toHaveLength(1);
        for (const bystander of [unnamed, other]) expect(await bystander.status(value.result.id)).not.toContain(value.result.text!);
      }
      expect(unnamed.sendMessage).not.toHaveBeenCalled(); expect(other.sendMessage).not.toHaveBeenCalled();
    } finally {
      await host?.close();
      for (const value of started.reverse()) await value.runtime.shutdown();
    }
  }, 40_000);

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
