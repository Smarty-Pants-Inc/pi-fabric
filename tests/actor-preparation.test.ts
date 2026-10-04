import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ACTOR_PREPARATION_TIMEOUT_MS, FABRIC_ACTOR_ACTIVATION_ALARM_TOPIC, ActorManager, ActorPreparationError, ActorPreparationTimeoutError } from "../src/actors/manager.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import * as predicate from "../src/actors/predicate.js";
import { AgentLaunchPreparationTimeoutError, AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import type { FabricCapabilityViewLease } from "../src/core/action-registry.js";

const cleanups: Array<() => Promise<void>> = [];
const waitFor = async (predicate: () => boolean, ms = 5_000) => {
  const deadline = Date.now() + ms;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("Timed out waiting for preparation recovery");
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
};
const pause = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
const deferred = <T>() => {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => { resolve = done; });
  return { promise, resolve };
};
type Options = NonNullable<ConstructorParameters<typeof ActorManager>[6]>;
const setup = (options: Options = {}, maxConcurrent = 20, agentOptions: NonNullable<ConstructorParameters<typeof AgentManager>[2]> = {}) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-preparation-"));
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const agents = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent }, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"), ...agentOptions,
  });
  const notices: string[] = [];
  const actors = new ActorManager("preparation", { id: "session:preparation", name: "main", kind: "main" }, mesh,
    { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, (delivery) => {
      if (delivery.message.source === "fabric-host") notices.push(delivery.message.text ?? "");
    },
    { actorRoot: path.join(root, "actors"), persistent: true, preparationTimeoutMs: 80, preparationRetryMs: 60, ...options });
  cleanups.push(async () => { await actors.close(); await agents.close(); fs.rmSync(root, { recursive: true, force: true }); });
  return { actors, agents, mesh, root, notices };
};
const retryEvent = (actors: ActorManager, id: string, phase: string) => actors.messages(id, 500).find((message) =>
  (message.data as { code?: string; phase?: string } | undefined)?.code === "FABRIC_ACTOR_PREPARATION_TIMEOUT" &&
  (message.data as { phase?: string } | undefined)?.phase === phase);
const readQueue = (sessionFile: string) => {
  const directory = path.dirname(sessionFile);
  const queue = fs.readdirSync(directory).find((file) => file.startsWith("queue-"))!;
  return JSON.parse(fs.readFileSync(path.join(directory, queue), "utf8")).items as Array<{ attempts: number; preparationAttempts?: number; resumed?: boolean }>;
};
afterEach(async () => { for (const cleanup of cleanups.splice(0).reverse()) await cleanup(); vi.restoreAllMocks(); });

describe("preparation documentation (#3307)", () => {
  it("documents the separate preparation counter and three-requeue limit", () => {
    const docs = fs.readFileSync("docs/agents.md", "utf8");
    expect(docs).toContain("`preparationAttempts` incremented");
    expect(docs).toContain("three preparation requeues");
    expect(docs).toContain("terminal exhaustion");
    expect(docs).not.toContain("with `attempts` incremented");
  });
});

describe("round-three accepted-work regressions (#3167)", () => {
  it("persists one activation during each held retry cleanup and executes it once after owner recreation", async () => {
    let stalled = false;
    const binding = deferred<void>();
    const releases: Array<ReturnType<typeof deferred<void>>> = [];
    const { actors: before, agents: oldAgents, mesh, root } = setup({
      preparationTimeoutMs: 150, preparationRetryMs: 60,
      resolvePiModel: (model) => stalled ? binding.promise.then(() => model) : model,
      acquireCapabilityView: async () => ({ satisfied: true, missing: [], optionalMissing: [],
        view: { id: "retry", digest: "retry", semanticDigest: "retry", bindings: {} },
        release: async () => { const gate = deferred<void>(); releases.push(gate); await gate.promise; },
      }),
    });
    cleanups.push(async () => { stalled = false; binding.resolve(); for (const gate of releases) gate.resolve(); });
    const actor = await before.create({ name: "exclusive-retry", model: "provider/test", instructions: "Reply", responseMode: "text", coalesce: false, requires: ["demo.echo"] });
    const oldRun = vi.spyOn(oldAgents, "run");
    stalled = true;
    const accepted = before.tell(actor.id, "original callerless activation");
    let snapshot = "";
    const originalId = accepted.messageId;
    const directory = path.dirname(actor.sessionFile!);
    for (let attempt = 1; attempt <= 3; attempt++) {
      await waitFor(() => releases.length === attempt);
      // Another accepted tell forces persistence while the failed activation's release is held.
      before.tell(actor.id, `persistence witness ${attempt}`);
      const queueFile = fs.readdirSync(directory).find((file) => file.startsWith("queue-"))!;
      snapshot = fs.readFileSync(path.join(directory, queueFile), "utf8");
      const items = JSON.parse(snapshot).items as Array<{ id: string; payload: unknown; preparationAttempts: number; attempts: number }>;
      expect(items.filter((item) => item.id === originalId)).toHaveLength(1);
      expect(new Set(items.map((item) => item.id)).size).toBe(items.length);
      expect(items.find((item) => item.id === originalId)).toMatchObject({ preparationAttempts: attempt, attempts: 0 });
      releases[attempt - 1]!.resolve();
      await waitFor(() => before.inFlightCount() === 0);
    }
    expect(oldRun).not.toHaveBeenCalled();
    await before.close(); await oldAgents.close();
    // Recreate from the exact snapshot taken during cleanup, not the later finalization write.
    const queueFile = fs.readdirSync(directory).find((file) => file.startsWith("queue-"))!;
    fs.writeFileSync(path.join(directory, queueFile), snapshot);
    const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "recreated-runs"),
    });
    const run = vi.spyOn(agents, "run");
    const after = new ActorManager("preparation", { id: "session:preparation", name: "main", kind: "main" }, mesh,
      { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {},
      { actorRoot: path.join(root, "actors"), persistent: true });
    cleanups.push(async () => { await after.close(); await agents.close(); });
    await waitFor(() => after.inFlightCount() === 0 && after.status(actor.id).queued === 0 && run.mock.calls.length === 4);
    await pause(150);
    expect(run.mock.calls.filter(([request]) => request.task.includes("original callerless activation"))).toHaveLength(1);
    expect(after.messages(actor.id).filter((message) => message.direction === "out" && !message.error)).toHaveLength(4);
    expect(fs.readdirSync(directory).filter((file) => file.startsWith("queue-"))).toEqual([]);
  });

  it("deduplicates repeated activation IDs restored from an older persisted snapshot", async () => {
    const { actors: before, agents, mesh, root } = setup({ preparationRetryMs: 1_000 });
    const actor = await before.create({ name: "legacy-duplicate", instructions: "Reply", responseMode: "text", coalesce: false });
    const registry = vi.spyOn(ActorRegistryStore.prototype, "withLock").mockRejectedValue(new Error("hold accepted work"));
    before.tell(actor.id, "restored only once");
    await waitFor(() => before.inFlightCount() === 0 && readQueue(actor.sessionFile!)[0]?.preparationAttempts === 1);
    const directory = path.dirname(actor.sessionFile!);
    const queueFile = path.join(directory, fs.readdirSync(directory).find((file) => file.startsWith("queue-"))!);
    const snapshot = JSON.parse(fs.readFileSync(queueFile, "utf8"));
    const record = snapshot.items[0];
    registry.mockRestore();
    await before.close();
    fs.writeFileSync(queueFile, JSON.stringify({ ...snapshot, items: [record, record, record] }));
    const run = vi.spyOn(agents, "run");
    const after = new ActorManager("preparation", { id: "session:preparation", name: "main", kind: "main" }, mesh,
      { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {},
      { actorRoot: path.join(root, "actors"), persistent: true });
    cleanups.push(async () => { await after.close(); });
    await waitFor(() => run.mock.calls.length > 0 && after.inFlightCount() === 0 && after.status(actor.id).queued === 0);
    expect(run).toHaveBeenCalledTimes(1);
  });
});

describe("round-four launch-preparation recovery (#3167)", () => {
  const scenarios = [false, true].flatMap((queued) => [false, true].flatMap((recreate) =>
    (["text", "directive"] as const).map((responseMode) => ({ queued, recreate, responseMode }))));
  it.each(scenarios)("durably retries confirmed-unlaunched auth (queued=$queued, recreate=$recreate, mode=$responseMode) exactly once", async ({ queued, recreate, responseMode }) => {
    const gate = deferred<void>();
    const authStarted = deferred<void>();
    const recovered = deferred<void>();
    let calls = 0;
    let resolved = false;
    const { actors: before, agents: oldAgents, mesh, root } = setup({ preparationRetryMs: 1_000 }, 1, {
      preparePiModel: async (model) => {
        if (model === "provider/stalled" && ++calls === 1) {
          authStarted.resolve();
          await gate.promise;
        }
        return model;
      },
    });
    cleanups.push(async () => { gate.resolve(); });
    const blocker = queued ? await oldAgents.spawn({ task: "HANG", model: "provider/healthy" }) : undefined;
    cleanups.push(async () => { if (blocker) await oldAgents.stop(blocker.id); });
    const actor = await before.create({ name: "auth-recovery", model: "provider/stalled", instructions: "Reply", responseMode, coalesce: false });
    const publish = mesh.publish.bind(mesh);
    vi.spyOn(mesh, "publish").mockImplementation(async request => {
      const event = await publish(request);
      if (request.topic === "fabric.actor.output" && request.from?.id === actor.id) recovered.resolve();
      return event;
    });
    const launches = vi.spyOn(oldAgents, "run");
    const accepted = before.tell(actor.id, "original accepted auth activation");
    let receiptId: string | undefined;
    if (blocker) {
      await waitFor(() => before.status(actor.id).status === "waiting");
      receiptId = before.status(actor.id).preparing!.runId;
      await pause(180); // Permit waiting remains exempt from the preparation deadline.
      expect(calls).toBe(0);
      expect(before.status(actor.id).status).toBe("waiting");
      await oldAgents.stop(blocker.id);
    }
    await authStarted.promise;
    expect(before.status(actor.id)).toMatchObject({ status: "preparing", preparing: { phase: "launch" } });
    expect(before.status(actor.id).inFlightRun).toBeUndefined();
    await waitFor(() => before.inFlightCount() === 0);
    expect(before.status(actor.id)).toMatchObject({ status: "queued", queued: 1 });
    const directory = path.dirname(actor.sessionFile!);
    const queueFile = path.join(directory, fs.readdirSync(directory).find((file) => file.startsWith("queue-"))!);
    const snapshot = fs.readFileSync(queueFile, "utf8");
    expect(JSON.parse(snapshot).items).toEqual([expect.objectContaining({ id: accepted.messageId, attempts: 0, preparationAttempts: 1 })]);
    expect(JSON.parse(snapshot).items[0].resumed).toBeUndefined();
    expect(resolved).toBe(false);
    expect(launches).toHaveBeenCalledTimes(1);
    if (receiptId) {
      expect(await oldAgents.wait(receiptId)).toMatchObject({ status: "failed", launchPreparationTimeoutMs: 80 });
      expect(oldAgents.status(receiptId).status).toBe("failed");
    }
    // Keep the exact timeout snapshot unexecuted while a slow healthy worker
    // proves permit reuse. Recreation, not a one-second retry race, owns recovery.
    if (recreate) before.pauseForRelease();
    // A fresh admission can use the released permit while the old auth promise is unresolved.
    const healthy = await oldAgents.spawn({ task: "healthy admission", model: "provider/healthy" });
    await oldAgents.wait(healthy.id);
    if (recreate) expect(launches).toHaveBeenCalledTimes(1);
    let actors = before;
    let agents = oldAgents;
    let executions = launches;
    if (recreate) {
      await before.close(); await oldAgents.close();
      // Recover the exact timeout snapshot, rather than relying on shutdown's memory.
      fs.writeFileSync(queueFile, snapshot);
      agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
        workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "recreated-runs"),
      });
      executions = vi.spyOn(agents, "run");
      actors = new ActorManager("preparation", { id: "session:preparation", name: "main", kind: "main" }, mesh,
        { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {},
        { actorRoot: path.join(root, "actors"), persistent: true });
      cleanups.push(async () => { await actors.close(); await agents.close(); });
    }
    // Recovery boots a real worker. Join its output event instead of giving a
    // loaded Windows runner five seconds to finish startup and journal recovery.
    // Vitest's existing test timeout remains the hang guard.
    await recovered.promise;
    await waitFor(() => actors.inFlightCount() === 0 && actors.status(actor.id).queued === 0);
    expect(resolved).toBe(false);
    expect(executions).toHaveBeenCalledTimes(recreate ? 1 : 2);
    expect(actors.messages(actor.id).filter((message) => message.direction === "out" && !message.error)).toHaveLength(1);
    if (receiptId && !recreate) expect(await agents.wait(receiptId)).toMatchObject({ status: "failed", launchPreparationTimeoutMs: 80 });
    resolved = true; gate.resolve(); await pause(150);
    expect(executions).toHaveBeenCalledTimes(recreate ? 1 : 2);
    expect(actors.status(actor.id)).toMatchObject({ status: "idle", queued: 0 });
    expect(actors.status(actor.id).preparing).toBeUndefined();
    expect(fs.readdirSync(directory).filter((file) => file.startsWith("queue-"))).toEqual([]);
    expect(agents.list().filter((run) => run.actorId === actor.id && run.status === "running")).toEqual([]);
    expect(fs.readdirSync(path.join(root, recreate ? "recreated-runs" : "runs"))).toHaveLength(recreate ? 0 : queued ? 2 : 1);
  });

  it.each([false, true])("exhausts the preparation retry budget in one terminal failure and one alarm (queued: %s)", async (queued) => {
    const gate = deferred<void>();
    let calls = 0;
    const { actors, agents, notices } = setup({ preparationRetryMs: 30 }, 1, { preparePiModel: async (model) => {
      if (model === "provider/stalled") { calls++; await gate.promise; }
      return model;
    } });
    cleanups.push(async () => { gate.resolve(); });
    const blocker = queued ? await agents.spawn({ task: "HANG", model: "provider/healthy" }) : undefined;
    cleanups.push(async () => { if (blocker) await agents.stop(blocker.id); });
    const actor = await actors.create({ name: "permanent-auth-failure", model: "provider/stalled", instructions: "Reply", responseMode: "directive", coalesce: false });
    const run = vi.spyOn(agents, "run");
    actors.tell(actor.id, "accepted but permanently stalled auth");
    if (blocker) {
      await waitFor(() => actors.status(actor.id).status === "waiting");
      await agents.stop(blocker.id);
    }
    await waitFor(() => calls === 4 && actors.inFlightCount() === 0 && actors.status(actor.id).queued === 0, 2_000);
    const failures = actors.messages(actor.id).filter((message) => message.direction === "out" && message.error);
    expect(failures).toHaveLength(4); // Three retry diagnostics, followed by exactly one terminal settlement.
    expect(failures.filter((message) => (message.data as { attempts?: number } | undefined)?.attempts !== undefined)).toHaveLength(3);
    expect(notices).toHaveLength(1);
    expect(notices[0]).toContain("Fabric host notice");
    expect(actors.status(actor.id)).toMatchObject({ status: "idle", queued: 0, lastError: expect.stringMatching(/preparation.*timed out/i) });
    expect(fs.readdirSync(path.dirname(actor.sessionFile!)).filter((file) => file.startsWith("queue-"))).toEqual([]);
    gate.resolve(); await pause(250);
    expect(calls).toBe(4);
    expect(run).toHaveBeenCalledTimes(4);
    expect(notices).toHaveLength(1);
    expect(actors.messages(actor.id).filter((message) => message.direction === "out" && !message.error)).toEqual([]);
  });

  it("preserves the exhausted preparation budget and alarms once after owner recreation", async () => {
    const gate = deferred<void>();
    let calls = 0;
    const { actors: before, agents: oldAgents, mesh, root, notices: beforeNotices } = setup({ preparationRetryMs: 80 }, 1, {
      preparePiModel: async (model) => { calls++; await gate.promise; return model; },
    });
    cleanups.push(async () => { gate.resolve(); });
    const actor = await before.create({ name: "exhausted-recreation", model: "provider/stalled", instructions: "Reply", responseMode: "text", coalesce: false });
    before.tell(actor.id, "preserve retry budget across owner recreation");
    await waitFor(() => calls === 3 && before.inFlightCount() === 0 && readQueue(actor.sessionFile!)[0]?.preparationAttempts === 3);
    expect(beforeNotices).toEqual([]); // Requeues have not terminally failed the activation.
    expect(before.status(actor.id).activationBlocked).toBeUndefined();
    expect(mesh.read({ topic: FABRIC_ACTOR_ACTIVATION_ALARM_TOPIC, limit: 20 })).toEqual([]);
    const directory = path.dirname(actor.sessionFile!);
    const queueFile = path.join(directory, fs.readdirSync(directory).find((file) => file.startsWith("queue-"))!);
    const snapshot = fs.readFileSync(queueFile, "utf8");
    await before.close(); await oldAgents.close();
    fs.writeFileSync(queueFile, snapshot);
    const notices: string[] = [];
    const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "recreated-runs"),
      preparePiModel: async (model) => { calls++; await gate.promise; return model; },
    });
    const run = vi.spyOn(agents, "run");
    const after = new ActorManager("preparation", { id: "session:preparation", name: "main", kind: "main" }, mesh,
      { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, (delivery) => {
        if (delivery.message.source === "fabric-host") notices.push(delivery.message.text ?? "");
      }, { actorRoot: path.join(root, "actors"), persistent: true, preparationTimeoutMs: 80, preparationRetryMs: 30 });
    cleanups.push(async () => { await after.close(); await agents.close(); });
    await waitFor(() => calls === 4 && after.inFlightCount() === 0 && after.status(actor.id).queued === 0);
    expect(run).toHaveBeenCalledTimes(1);
    expect(notices).toHaveLength(1);
    expect(after.status(actor.id)).toMatchObject({
      status: "idle", queued: 0, activationBlocked: { code: "unknown", count: 1 },
    });
    expect(after.messages(actor.id).filter(message => message.direction === "out" && message.error)).toHaveLength(4);
    expect(fs.readdirSync(directory).filter((file) => file.startsWith("queue-"))).toEqual([]);
    gate.resolve(); await pause(200);
    expect(calls).toBe(4);
    expect(run).toHaveBeenCalledTimes(1);
    expect(notices).toHaveLength(1);
    await waitFor(() => mesh.read({ topic: FABRIC_ACTOR_ACTIVATION_ALARM_TOPIC, limit: 20 }).length === 1);
    await after.close();
    const next = new ActorManager("preparation", { id: "session:preparation", name: "main", kind: "main" }, mesh,
      { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, (delivery) => {
        if (delivery.message.source === "fabric-host") notices.push(delivery.message.text ?? "");
      }, { actorRoot: path.join(root, "actors"), persistent: true });
    cleanups.push(async () => { await next.close(); });
    await pause(200);
    expect(next.status(actor.id)).toMatchObject({ status: "idle", queued: 0, activationBlocked: { code: "unknown", count: 1 } });
    expect(run).toHaveBeenCalledTimes(1); // Terminal work never returns after another restart.
    expect(notices).toHaveLength(1);
    expect(mesh.read({ topic: FABRIC_ACTOR_ACTIVATION_ALARM_TOPIC, limit: 20 })).toHaveLength(1);
  });

  it.each([false, true])("never retries an unconfirmed transport launch with a timeout-shaped error (queued: %s)", async (queued) => {
    const { actors, agents } = setup({}, 1);
    const blocker = queued ? await agents.spawn({ task: "HANG", model: "provider/healthy" }) : undefined;
    cleanups.push(async () => { if (blocker) await agents.stop(blocker.id); });
    const actor = await actors.create({ name: "unknown-launch", instructions: "Reply", responseMode: "text", coalesce: false });
    const error = Object.assign(new AgentLaunchPreparationTimeoutError(80), { launchOutcome: "unknown" });
    const transport = vi.spyOn(ProcessTransport.prototype, "launch").mockRejectedValueOnce(error);
    const run = vi.spyOn(agents, "run");
    actors.tell(actor.id, "do not duplicate an unconfirmed worker");
    let receiptId: string | undefined;
    if (blocker) {
      await waitFor(() => actors.status(actor.id).status === "waiting");
      receiptId = actors.status(actor.id).preparing!.runId;
      await agents.stop(blocker.id);
    }
    await waitFor(() => transport.mock.calls.length === 1 && actors.inFlightCount() === 0);
    await pause(200);
    expect(run).toHaveBeenCalledTimes(1);
    expect(transport).toHaveBeenCalledTimes(1);
    expect(actors.status(actor.id)).toMatchObject({ status: "idle", queued: 0 });
    if (receiptId) {
      const result = await agents.wait(receiptId);
      expect(result.status).toBe("failed");
      expect(result).not.toHaveProperty("launchPreparationTimeoutMs");
    }
  });

  it("never retries a launched worker even if its failure has the preparation timeout type", async () => {
    const { actors, agents } = setup();
    const actor = await actors.create({ name: "launched-timeout", instructions: "Reply", responseMode: "text", coalesce: false });
    const run = vi.spyOn(agents, "run").mockImplementationOnce(async (_request, _signal, onLaunched) => {
      onLaunched?.({ id: "already-launched" } as Parameters<NonNullable<typeof onLaunched>>[0]);
      throw new AgentLaunchPreparationTimeoutError(80);
    });
    await expect(actors.ask(actor.id, "do not duplicate a launched worker")).rejects.toThrow("timed out");
    await waitFor(() => actors.inFlightCount() === 0);
    await pause(200);
    expect(run).toHaveBeenCalledTimes(1);
    expect(actors.status(actor.id)).toMatchObject({ status: "idle", queued: 0 });
  });

  it.each([false, true])("keeps a genuinely refused admission terminal even if its message resembles a timeout (queued: %s)", async (queued) => {
    let calls = 0;
    const { actors, agents } = setup({}, 1, { preparePiModel: async (model) => {
      if (model === "provider/refused") { calls++; throw new Error("Agent launch preparation (model/auth) timed out after 80 ms"); }
      return model;
    } });
    const blocker = queued ? await agents.spawn({ task: "HANG", model: "provider/healthy" }) : undefined;
    cleanups.push(async () => { if (blocker) await agents.stop(blocker.id); });
    const actor = await actors.create({ name: "refused-auth", model: "provider/refused", instructions: "Reply", responseMode: "text", coalesce: false });
    const run = vi.spyOn(agents, "run");
    actors.tell(actor.id, "genuinely refused activation");
    let receiptId: string | undefined;
    if (blocker) {
      await waitFor(() => actors.status(actor.id).status === "waiting");
      receiptId = actors.status(actor.id).preparing!.runId;
      await agents.stop(blocker.id);
    }
    await waitFor(() => calls === 1 && actors.inFlightCount() === 0);
    await pause(200);
    expect(run).toHaveBeenCalledTimes(1);
    expect(actors.status(actor.id)).toMatchObject({ status: "idle", queued: 0 });
    expect(actors.messages(actor.id).filter((message) => message.direction === "out" && message.error)).toHaveLength(1);
    if (receiptId) {
      const result = await agents.wait(receiptId);
      expect(result.status).toBe("failed");
      expect(result).not.toHaveProperty("launchPreparationTimeoutMs");
    }
  });
});

describe("actor preparation (#3167)", () => {
  it("exposes a typed phase-specific timeout with a conservative production deadline", () => {
    expect(ACTOR_PREPARATION_TIMEOUT_MS).toBe(30_000);
    const error = new ActorPreparationTimeoutError("actor-id", "presence", 80);
    expect(error).toBeInstanceOf(ActorPreparationError);
    expect(error).toMatchObject({ code: "FABRIC_ACTOR_PREPARATION_TIMEOUT", actorId: "actor-id", phase: "presence", timeoutMs: 80 });
  });

  it("recovers an unresolved presence join and drains the queue while that write is still unresolved", async () => {
    const { actors, agents, mesh } = setup();
    const actor = await actors.create({ name: "stalled", instructions: "Reply", responseMode: "text", coalesce: false });
    const gate = deferred<void>();
    const put = mesh.put.bind(mesh);
    let released = false;
    let writes = 0;
    vi.spyOn(mesh, "put").mockImplementation(async (input) => {
      if (input.key.endsWith(actor.id)) { writes++; await gate.promise; }
      return put(input);
    });
    cleanups.push(async () => { released = true; gate.resolve(); });
    const run = vi.spyOn(agents, "run");
    actors.tell(actor.id, "first");
    actors.tell(actor.id, "second");
    await waitFor(() => writes === 1 && actors.status(actor.id).preparing?.phase === "presence");
    expect(actors.status(actor.id)).toMatchObject({ status: "preparing", queued: 1, preparing: { attempts: 0 } });
    expect(actors.status(actor.id).inFlightRun).toBeUndefined();
    await waitFor(() => retryEvent(actors, actor.id, "presence") !== undefined && actors.inFlightCount() === 0);
    expect(run).not.toHaveBeenCalled();
    expect(actors.status(actor.id)).toMatchObject({ status: "queued", queued: 2 });
    expect(readQueue(actor.sessionFile!)[0]).toMatchObject({ attempts: 0, preparationAttempts: 1 });
    expect(retryEvent(actors, actor.id, "presence")?.data).toMatchObject({ errorType: "ActorPreparationTimeoutError", attempts: 1 });
    await waitFor(() => actors.inFlightCount() === 0 && actors.status(actor.id).queued === 0);
    expect(run).toHaveBeenCalledTimes(2);
    expect(actors.status(actor.id)).toMatchObject({ status: "idle" });
    expect(actors.status(actor.id).inFlightRun).toBeUndefined();
    expect(released).toBe(false);
    expect(writes).toBe(1); // Ordering is retained, not broken to force a second write through.
    gate.resolve();
    await waitFor(() => (mesh.get(`actors/preparation/${actor.id}`)?.value as { status?: string })?.status === "idle");
  });

  it.each(["registry", "validity", "binding"] as const)("bounds an unresolved %s await and retries without resolving the old await", async (phase) => {
    const gate = deferred<void>();
    let stalled = false;
    let bindingCalls = 0;
    const { actors, agents } = setup({ resolvePiModel: (model) => stalled && bindingCalls++ === 0 ? gate.promise.then(() => model) : model });
    const actor = await actors.create({ name: phase, instructions: "Reply", responseMode: "text", coalesce: false,
      ...(phase === "binding" ? { model: "provider/test" } : {}),
      ...(phase === "validity" ? { validWhile: { version: 1, source: "() => true" } } : {}),
    });
    cleanups.push(async () => { gate.resolve(); });
    if (phase === "validity") {
      vi.spyOn(predicate, "evaluateActorValidWhile").mockImplementationOnce(() => gate.promise.then(() => ({ valid: true })));
    } else if (phase === "registry") {
      const withLock = ActorRegistryStore.prototype.withLock;
      let calls = 0;
      vi.spyOn(ActorRegistryStore.prototype, "withLock").mockImplementation(async function<T>(this: ActorRegistryStore, operation: () => T): Promise<T> {
        // The enqueue's advisory save and then the drain's required save both stall.
        if (++calls <= 2) await gate.promise;
        return withLock.call(this, operation) as Promise<T>;
      });
    } else stalled = true;
    const run = vi.spyOn(agents, "run");
    actors.tell(actor.id, "first");
    await waitFor(() => retryEvent(actors, actor.id, phase) !== undefined);
    expect(retryEvent(actors, actor.id, phase)?.data).toMatchObject({ attempts: 1 });
    expect(actors.status(actor.id).inFlightRun).toBeUndefined();
    await waitFor(() => actors.inFlightCount() === 0 && actors.status(actor.id).queued === 0);
    expect(run).toHaveBeenCalledTimes(1);
  });

  it("re-arms after a rejected preparation await as well as a timeout", async () => {
    const { actors, agents } = setup();
    const actor = await actors.create({ name: "rejected", instructions: "Reply", responseMode: "text" });
    const withLock = ActorRegistryStore.prototype.withLock;
    let calls = 0;
    vi.spyOn(ActorRegistryStore.prototype, "withLock").mockImplementation(function<T>(this: ActorRegistryStore, operation: () => T): Promise<T> {
      if (++calls === 2) return Promise.reject(new Error("temporary registry unavailable"));
      return withLock.call(this, operation) as Promise<T>;
    });
    actors.tell(actor.id, "retry me");
    await waitFor(() => actors.messages(actor.id).some((message) => message.error?.includes("temporary registry unavailable")));
    expect(actors.messages(actor.id).find((message) => message.error)?.data).toMatchObject({ code: "FABRIC_ACTOR_PREPARATION_FAILED", attempts: 1, phase: "registry" });
    await waitFor(() => actors.inFlightCount() === 0 && actors.status(actor.id).queued === 0);
    expect(agents.list().every((run) => run.status !== "running")).toBe(true);
  });

  it("survives three failed preparations and owner recreation, executing the accepted activation once", async () => {
    const { actors: before, agents: oldAgents, mesh, root } = setup({ preparationRetryMs: 120 });
    const actor = await before.create({ name: "retry-restart", instructions: "Reply", responseMode: "text",
      coalesce: false });
    const registry = vi.spyOn(ActorRegistryStore.prototype, "withLock")
      .mockRejectedValue(new Error("temporary registry unavailable"));
    const oldRun = vi.spyOn(oldAgents, "run");
    before.tell(actor.id, "accepted across preparation failures");
    await waitFor(() => before.messages(actor.id).filter((message) =>
      (message.data as { itemId?: string } | undefined)?.itemId !== undefined).length === 3 && before.inFlightCount() === 0);
    const saved = readQueue(actor.sessionFile!)[0]!;
    expect(saved.preparationAttempts ?? saved.attempts).toBe(3);
    expect(oldRun).not.toHaveBeenCalled();
    registry.mockRestore();
    await before.close();
    await oldAgents.close();
    const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "restarted-runs"),
    });
    const run = vi.spyOn(agents, "run");
    const after = new ActorManager("preparation", { id: "session:preparation", name: "main", kind: "main" }, mesh,
      { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {},
      { actorRoot: path.join(root, "actors"), persistent: true });
    cleanups.push(async () => { await after.close(); await agents.close(); });
    // Assert acceptance before waiting: the old head drops this item on its first restore.
    expect(after.messages(actor.id).filter((message) => message.error?.includes("Dropped a queued event"))).toEqual([]);
    await waitFor(() => after.inFlightCount() === 0 && after.status(actor.id).queued === 0 && run.mock.calls.length > 0);
    await pause(150);
    expect(run).toHaveBeenCalledTimes(1);
    expect(after.messages(actor.id).filter((message) => message.direction === "out" && !message.error)).toHaveLength(1);
    expect(fs.readdirSync(path.dirname(actor.sessionFile!)).filter((file) => file.startsWith("queue-"))).toEqual([]);
  });

  it("keeps finite unavailable-model errors terminal instead of retrying them", async () => {
    let unavailable = false;
    const { actors, agents } = setup({ resolvePiModel: (model) => unavailable
      ? Promise.reject(new Error("model no longer available")) : model });
    const actor = await actors.create({ name: "unavailable", instructions: "Reply", model: "provider/test", responseMode: "text" });
    const run = vi.spyOn(agents, "run");
    unavailable = true;
    await expect(actors.ask(actor.id, "do not retry a finite rejection")).rejects.toThrow("model no longer available");
    await waitFor(() => actors.inFlightCount() === 0);
    expect(actors.status(actor.id)).toMatchObject({ status: "idle", queued: 0 });
    expect(run).not.toHaveBeenCalled();
  });

  it("does not strand the next activation on a stalled cleanup presence write or rerun completed work", async () => {
    const { actors, agents, mesh } = setup();
    const actor = await actors.create({ name: "cleanup", instructions: "Reply", responseMode: "text", coalesce: false });
    const gate = deferred<void>();
    cleanups.push(async () => { gate.resolve(); });
    const put = mesh.put.bind(mesh);
    const cleanup = agents.cleanup.bind(agents);
    vi.spyOn(agents, "cleanup").mockImplementationOnce(async (...args) => {
      vi.spyOn(mesh, "put").mockImplementation(async (input) => {
        if (input.key.endsWith(actor.id)) await gate.promise;
        return put(input);
      });
      return cleanup(...args);
    });
    const run = vi.spyOn(agents, "run");
    actors.tell(actor.id, "first"); actors.tell(actor.id, "second");
    await waitFor(() => retryEvent(actors, actor.id, "presence") !== undefined);
    await waitFor(() => actors.inFlightCount() === 0 && actors.status(actor.id).queued === 0);
    expect(run).toHaveBeenCalledTimes(2);
    expect(actors.messages(actor.id).filter((message) => message.direction === "out" && !message.error)).toHaveLength(2);
    expect(actors.status(actor.id).inFlightRun).toBeUndefined();
  });

  it("bounds capability acquisition, aborts the old acquisition, and releases a late lease", async () => {
    const gate = deferred<FabricCapabilityViewLease>();
    const release = vi.fn(async () => {});
    const lease: FabricCapabilityViewLease = { satisfied: true, missing: [], optionalMissing: [],
      view: { id: "test", digest: "test", semanticDigest: "test", bindings: {} }, release };
    let firstSignal: AbortSignal | undefined;
    let calls = 0;
    const { actors } = setup({ acquireCapabilityView: async (_requirements, signal) => {
      if (++calls === 1) { firstSignal = signal; return gate.promise; }
      return lease;
    } });
    cleanups.push(async () => { gate.resolve(lease); });
    const actor = await actors.create({ name: "capabilities", instructions: "Reply", responseMode: "text", requires: ["demo.echo"] });
    actors.tell(actor.id, "first");
    await waitFor(() => retryEvent(actors, actor.id, "capabilities") !== undefined);
    expect(firstSignal?.aborted).toBe(true);
    await waitFor(() => actors.inFlightCount() === 0 && actors.status(actor.id).queued === 0);
    expect(release).toHaveBeenCalledTimes(1);
    gate.resolve(lease);
    await waitFor(() => release.mock.calls.length === 2);
  });

  it("retains the catalog wake received during capability acquisition, before the missing marker exists", async () => {
    const gate = deferred<FabricCapabilityViewLease>();
    const release = vi.fn(async () => {});
    const satisfied: FabricCapabilityViewLease = { satisfied: true, missing: [], optionalMissing: [],
      view: { id: "ready", digest: "ready", semanticDigest: "ready", bindings: {} }, release };
    let calls = 0;
    const { actors, agents } = setup({ acquireCapabilityView: () => ++calls === 1 ? gate.promise : Promise.resolve(satisfied) });
    cleanups.push(async () => { gate.resolve(satisfied); });
    const actor = await actors.create({ name: "early-catalog-wake", instructions: "Reply", responseMode: "text", requires: ["demo.echo"] });
    const run = vi.spyOn(agents, "run");
    actors.tell(actor.id, "accepted before catalog ready");
    await waitFor(() => calls === 1 && actors.status(actor.id).preparing?.phase === "capabilities");
    expect(actors.status(actor.id).missingCapabilities).toBeUndefined();
    actors.retryCapabilityWaiters(); actors.retryCapabilityWaiters(); // Coalesce, do not create two drains.
    await pause(10);
    gate.resolve({ satisfied: false, missing: ["demo.echo"], optionalMissing: [], release });
    await waitFor(() => actors.inFlightCount() === 0 && actors.status(actor.id).queued === 0 && run.mock.calls.length > 0);
    expect(calls).toBe(2);
    expect(run).toHaveBeenCalledTimes(1);
    expect(actors.messages(actor.id).filter((message) => message.direction === "out" && !message.error)).toHaveLength(1);
  });

  it("14 stalled presence writes neither starve four healthy actors nor impose a cap of four", async () => {
    const { actors, agents, mesh } = setup({ preparationTimeoutMs: 250 });
    const all: Awaited<ReturnType<ActorManager["create"]>>[] = [];
    for (let index = 0; index < 18; index++) all.push(await actors.create({ name: `review-${index}`, instructions: "Reply", responseMode: "text" }));
    const stalled = new Set(all.slice(0, 14).map((actor) => actor.id));
    const gate = deferred<void>();
    const put = mesh.put.bind(mesh);
    vi.spyOn(mesh, "put").mockImplementation(async (input) => {
      if (stalled.has(input.key.split("/").at(-1)!)) await gate.promise;
      return put(input);
    });
    cleanups.push(async () => { gate.resolve(); actors.haltAll(); await waitFor(() => actors.inFlightCount() === 0); });
    for (const actor of all) actors.tell(actor.id, "HANG");
    await waitFor(() => all.slice(14).every((actor) => actors.status(actor.id).inFlightRun !== undefined));
    expect(all.slice(14).every((actor) => retryEvent(actors, actor.id, "presence") === undefined)).toBe(true);
    await waitFor(() => all.every((actor) => actors.status(actor.id).inFlightRun !== undefined));
    expect(agents.list().filter((run) => run.status === "running")).toHaveLength(18);
    expect(all.slice(0, 14).every((actor) => retryEvent(actors, actor.id, "presence") !== undefined)).toBe(true);
    expect(all.every((actor) => actors.status(actor.id).status === "running" && actors.status(actor.id).preparing === undefined)).toBe(true);
  }, 20_000);

  it("reports permit waiters with queue positions, and every actor drains as permits free", async () => {
    const { actors, agents } = setup({}, 2);
    const blockers = await Promise.all(Array.from({ length: 2 }, () => agents.spawn({ task: "HANG" })));
    cleanups.push(async () => { await Promise.all(blockers.map((run) => agents.stop(run.id))); });
    const all: Awaited<ReturnType<ActorManager["create"]>>[] = [];
    for (let index = 0; index < 7; index++) all.push(await actors.create({ name: `waiter-${index}`, instructions: "Reply", responseMode: "text" }));
    for (const actor of all) actors.tell(actor.id, "queued behind permits");
    await waitFor(() => all.every((actor) => actors.status(actor.id).status === "waiting"));
    expect(all.map((actor) => actors.status(actor.id).preparing?.queuePosition).sort()).toEqual([1, 2, 3, 4, 5, 6, 7]);
    expect(all.every((actor) => actors.status(actor.id).inFlightRun === undefined)).toBe(true);
    await pause(200); // A legitimate permit wait is longer than the 80ms setup deadline.
    expect(all.every((actor) => actors.status(actor.id).status === "waiting")).toBe(true);
    await agents.stop(blockers[0]!.id);
    await waitFor(() => all.every((actor) => actors.status(actor.id).lastRunId !== undefined) && actors.inFlightCount() === 0);
    expect(all.every((actor) => actors.status(actor.id).queued === 0 && actors.status(actor.id).inFlightRun === undefined)).toBe(true);
  });
});
