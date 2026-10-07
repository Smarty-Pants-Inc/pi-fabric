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
import { MeshStore, type MeshEvent } from "../src/mesh/store.js";
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
      // Each held cleanup persisted one more failed preparation. A failed item rotates behind
      // newer work (smarty-dev#816 round 3), so the failures spread over the queued items.
      expect(items.find((item) => item.id === originalId)).toMatchObject({ attempts: 0 });
      expect(items.reduce((total, item) => total + item.preparationAttempts, 0)).toBe(attempt);
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

  it.each([false, true])("exhausts a caller-owned ask preparation budget in one terminal failure and one alarm (queued: %s)", async (queued) => {
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
    void actors.ask(actor.id, "accepted but permanently stalled auth").catch(() => undefined);
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
    expect(actors.status(actor.id)).toMatchObject({ status: "failed", queued: 0, lastError: expect.stringMatching(/preparation.*timed out/i) });
    expect(fs.readdirSync(path.dirname(actor.sessionFile!)).filter((file) => file.startsWith("queue-"))).toEqual([]);
    gate.resolve(); await pause(250);
    expect(calls).toBe(4);
    expect(run).toHaveBeenCalledTimes(4);
    expect(notices).toHaveLength(1);
    expect(actors.messages(actor.id).filter((message) => message.direction === "out" && !message.error)).toEqual([]);
  });

  it("#816 retains work beyond the preparation budget and alarms once across owner recreation", async () => {
    const gate = deferred<void>();
    let calls = 0;
    const { actors: before, agents: oldAgents, mesh, root, notices: beforeNotices } = setup({ preparationRetryMs: 20 }, 1, {
      preparePiModel: async model => { calls++; await gate.promise; return model; },
    });
    cleanups.push(async () => { gate.resolve(); });
    const actor = await before.create({ name: "pending-recreation", model: "provider/stalled", instructions: "Reply", responseMode: "text", coalesce: false });
    const accepted = before.tell(actor.id, "preserve accepted work beyond preparation budget");
    await waitFor(() => calls >= 4 && before.inFlightCount() === 0);
    expect(before.status(actor.id)).toMatchObject({ status: "failing-preparation", queued: 1, activationBlocked: { code: "failing-preparation" } });
    expect(beforeNotices).toHaveLength(1);
    const directory = path.dirname(actor.sessionFile!);
    const queueFile = path.join(directory, fs.readdirSync(directory).find(file => file.startsWith("queue-"))!);
    const snapshot = fs.readFileSync(queueFile, "utf8");
    expect(JSON.parse(snapshot).items).toEqual([expect.objectContaining({ id: accepted.messageId, preparationAttempts: 4, attempts: 0 })]);
    await waitFor(() => mesh.read({ topic: FABRIC_ACTOR_ACTIVATION_ALARM_TOPIC, limit: 20 }).length === 1);
    await before.close(); await oldAgents.close();
    fs.writeFileSync(queueFile, snapshot);
    const notices: string[] = [];
    const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "recreated-runs"),
      preparePiModel: async model => { calls++; await gate.promise; return model; },
    });
    const run = vi.spyOn(agents, "run");
    const after = new ActorManager("preparation", { id: "session:preparation", name: "main", kind: "main" }, mesh,
      { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, delivery => {
        if (delivery.message.source === "fabric-host") notices.push(delivery.message.text ?? "");
      }, { actorRoot: path.join(root, "actors"), persistent: true, preparationTimeoutMs: 80, preparationRetryMs: 20 });
    cleanups.push(async () => { await after.close(); await agents.close(); });
    await waitFor(() => calls >= 5 && after.inFlightCount() === 0);
    expect(after.status(actor.id)).toMatchObject({ status: "failing-preparation", queued: 1 });
    expect(readQueue(actor.sessionFile!)[0]).toMatchObject({ preparationAttempts: 5, attempts: 0 });
    expect(notices).toEqual([]);
    expect(mesh.read({ topic: FABRIC_ACTOR_ACTIVATION_ALARM_TOPIC, limit: 20 })).toHaveLength(1);
    gate.resolve();
    await waitFor(() => after.inFlightCount() === 0 && after.status(actor.id).queued === 0);
    expect(run).toHaveBeenCalledTimes(2); // one timed-out setup, one confirmed worker
    expect(after.messages(actor.id).filter(message => message.direction === "out" && !message.error)).toHaveLength(1);
    expect(after.status(actor.id)).toMatchObject({ status: "idle", queued: 0 });
    expect(after.status(actor.id).activationBlocked).toBeUndefined();
    expect(fs.readdirSync(directory).filter(file => file.startsWith("queue-"))).toEqual([]);
  });

  it.each([false, true])("never retries an unconfirmed transport launch with a timeout-shaped error (queued: %s)", async (queued) => {
    const { actors, agents, root } = setup({ closeGraceMs: 20 }, 1);
    // This synthetic unknown receipt has no exact worker handle to discharge custody.
    // Shutdown must report that obligation, not fabricate a failed activation/retry.
    cleanups.pop();
    cleanups.push(async () => {
      try {
        await expect(actors.close()).rejects.toThrow(/execution exit unconfirmed/);
        await expect(agents.close()).rejects.toThrow(/execution exit unconfirmed/);
      } finally { fs.rmSync(root, { recursive: true, force: true }); }
    });
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
    await waitFor(() => transport.mock.calls.length === 1 && agents.list().some(handle => handle.actorId === actor.id));
    receiptId ??= agents.list().find(handle => handle.actorId === actor.id)!.id;
    actors.tell(actor.id, "must not overlap the unknown launch");
    await pause(200);
    expect(run).toHaveBeenCalledTimes(1);
    expect(transport).toHaveBeenCalledTimes(1);
    expect(actors.inFlightCount()).toBe(1);
    expect(actors.status(actor.id).queued).toBe(1);
    await expect(agents.wait(receiptId, { timeoutMs: 20 })).rejects.toThrow(/still running/);
    await expect(agents.stop(receiptId)).rejects.toThrow(/execution exit unconfirmed/);
    expect(fs.existsSync(path.join(root, "runs", receiptId, "unresolved-worker.json"))).toBe(true);
    const replacement = await agents.spawn({ task: "must remain queued", model: "provider/healthy" });
    expect(replacement.status).toBe("queued");
    await agents.stop(replacement.id);
    expect(transport).toHaveBeenCalledTimes(1);
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
  it.each([1, 4])("#816 advances the host cursor past a routed event queued through %i failed preparations and processes it once", async failures => {
    const cursorRoot = fs.mkdtempSync(path.join(os.tmpdir(), "preparation-cursor-"));
    const cursorPath = path.join(cursorRoot, "cursor.json");
    cleanups.push(async () => { fs.rmSync(cursorRoot, { recursive: true, force: true }); });
    let failed = 0;
    let recover = false;
    const { actors, agents, mesh } = setup({ meshCursorPath: cursorPath, preparationRetryMs: 30,
      acquireCapabilityView: async () => {
        if (failed < failures) { failed++; throw new Error("temporary preparation unavailable"); }
        if (!recover) throw new Error("hold preparation for cursor inspection");
        return { satisfied: true, missing: [], optionalMissing: [],
          view: { id: "recovered", digest: "recovered", semanticDigest: "recovered", bindings: {} }, release: async () => {} };
      },
    });
    const actor = await actors.create({ name: "routed-review", instructions: "Reply", topics: ["github.review"],
      responseMode: "text", coalesce: false, residency: "durable", requires: ["demo.echo"] });
    const run = vi.spyOn(agents, "run");
    const event = await mesh.publish({ topic: "github.review", from: { id: "router", name: "router", kind: "main" }, data: { pr: 816 } });
    await waitFor(() => failed === failures && actors.inFlightCount() === 0);
    expect(actors.status(actor.id).queued).toBe(1);
    const directory = path.dirname(actor.sessionFile!);
    const queueFile = path.join(directory, fs.readdirSync(directory).find(file => file.startsWith("queue-"))!);
    const items = JSON.parse(fs.readFileSync(queueFile, "utf8")).items;
    expect(items).toHaveLength(1);
    expect(items[0]).toMatchObject({ payload: { id: event.id }, preparationAttempts: failures, attempts: 0 });
    expect(run).not.toHaveBeenCalled();
    const cursor = () => JSON.parse(fs.readFileSync(cursorPath, "utf8")) as { last?: { sequence: number; id: string } };
    // Accepted into the actor's persisted queue: the host cursor is not pinned on it.
    expect(cursor().last?.sequence ?? 0).toBeGreaterThanOrEqual(event.sequence);
    if (failures === 4) expect(actors.status(actor.id).status).toBe("failing-preparation");
    recover = true;
    await waitFor(() => run.mock.calls.length > 0 && actors.inFlightCount() === 0 && actors.status(actor.id).queued === 0);
    expect(run).toHaveBeenCalledTimes(1);
    expect(actors.messages(actor.id).filter(message => message.direction === "in" && message.source === "mesh:github.review")).toHaveLength(1);
    expect(actors.messages(actor.id).filter(message => message.direction === "out" && !message.error)).toHaveLength(1);
    expect(actors.status(actor.id).activationBlocked).toBeUndefined();
  });

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
    // The failed item rotated behind the newer one (smarty-dev#816 round 3).
    expect(readQueue(actor.sessionFile!)).toMatchObject([{ attempts: 0, preparationAttempts: 0 }, { attempts: 0, preparationAttempts: 1 }]);
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
    expect(actors.messages(actor.id).filter(message => message.direction === "out" && !message.error)).toHaveLength(1);
    // Resolving abandoned acquisitions must not commit an old preparation or
    // duplicate the accepted activation after recovery has already succeeded.
    gate.resolve();
    await pause(150);
    expect(run).toHaveBeenCalledTimes(1);
    expect(actors.status(actor.id)).toMatchObject({ status: "idle", queued: 0 });
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

// Round 2 of PR #554: acceptance is the receiver's persisted queue, never an activation
// outcome. A failing preparation retries from that queue and never pins the host cursor.
describe("#816 per-actor queue acceptance", () => {
  const from = { id: "router", name: "router", kind: "main" as const };
  type Preparation = { ok: boolean; failures: number };
  const fixture = (archive = false) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-queue-acceptance-"));
    cleanups.push(async () => { fs.rmSync(root, { recursive: true, force: true }); });
    const meshRoot = path.join(root, "mesh");
    if (archive) {
      fs.mkdirSync(meshRoot, { recursive: true });
      fs.mkdirSync(path.join(root, "archive"));
      fs.writeFileSync(path.join(meshRoot, "event-archive.json"), JSON.stringify({ version: 1, dir: path.join(root, "archive") }));
    }
    const mesh = new MeshStore(meshRoot, 64 * 1024, 100);
    const cursorPath = path.join(root, "cursor.json");
    let generation = 0;
    const host = (preparation: Preparation, maxReadEvents = 2, actorQueueLimit = DEFAULT_FABRIC_CONFIG.mesh.actorQueueLimit) => {
      const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
        workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, `runs-${generation++}`),
      });
      const actors = new ActorManager("preparation", { id: "session:preparation", name: "main", kind: "main" }, mesh,
        { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20, maxReadEvents, actorQueueLimit }, agents, () => {}, {
          actorRoot: path.join(root, "actors"), persistent: true, meshCursorPath: cursorPath,
          preparationTimeoutMs: 80, preparationRetryMs: 20,
          acquireCapabilityView: async () => {
            if (!preparation.ok) { preparation.failures++; throw new Error("temporary preparation unavailable"); }
            return { satisfied: true, missing: [], optionalMissing: [],
              view: { id: "ok", digest: "ok", semanticDigest: "ok", bindings: {} }, release: async () => {} };
          },
        });
      let closed = false;
      const close = async () => { if (closed) return; closed = true; await actors.close(); await agents.close(); };
      cleanups.push(close);
      return { actors, agents, close };
    };
    const cursor = () => JSON.parse(fs.readFileSync(cursorPath, "utf8")) as { last?: { sequence: number; id: string } };
    return { root, mesh, cursorPath, host, cursor };
  };
  const create = async (actors: ActorManager, failingTopic: string, healthyTopic: string) => ({
    failing: await actors.create({ name: "failing-prep", instructions: "Reply", topics: [failingTopic], responseMode: "text",
      coalesce: false, residency: "durable", requires: ["demo.echo"] }),
    healthy: await actors.create({ name: "healthy", instructions: "Reply", topics: [healthyTopic], responseMode: "text",
      coalesce: false, residency: "durable" }),
  });
  const replies = (actors: ActorManager, id: string) =>
    actors.messages(id, 500).filter(message => message.direction === "out" && !message.error).length;
  const queued = (sessionFile: string) => {
    const directory = path.dirname(sessionFile);
    const file = fs.readdirSync(directory).find(name => name.startsWith("queue-"));
    return file ? JSON.parse(fs.readFileSync(path.join(directory, file), "utf8")).items as Array<{ payload: { id?: string }; preparationAttempts: number }> : [];
  };
  // Seed the actors, stop the first host, then publish while no host reads: the next
  // host must read the backlog in several pages of two events.
  const backlog = async () => {
    const f = fixture();
    const seed = f.host({ ok: true, failures: 0 });
    const ids = await create(seed.actors, "github.review", "team.work");
    await seed.close();
    const review = await f.mesh.publish({ topic: "github.review", from, data: { pr: 816 } });
    const later = [];
    for (let index = 0; index < 5; index++) later.push(await f.mesh.publish({ topic: "team.work", from, data: { n: index } }));
    return { ...f, ...ids, review, later };
  };

  it("(1) a failing early item does not stop later pages reaching other actors; it retries from its queue once", async () => {
    const f = await backlog();
    const preparation = { ok: false, failures: 0 };
    const { actors, agents, mesh } = { ...f.host(preparation), mesh: f.mesh };
    const tail = vi.spyOn(mesh, "tail");
    const run = vi.spyOn(agents, "run");
    await waitFor(() => replies(actors, f.healthy.id) === 5 && preparation.failures >= 1);
    expect(tail.mock.results.filter(result => (result.value as { events: unknown[] }).events.length === 2).length).toBeGreaterThanOrEqual(2);
    expect(f.cursor().last?.sequence ?? 0).toBeGreaterThanOrEqual(f.later.at(-1)!.sequence);
    expect(actors.status(f.failing.id).queued).toBe(1);
    expect(queued(f.failing.sessionFile!)).toEqual([expect.objectContaining({ payload: expect.objectContaining({ id: f.review.id }) })]);
    expect(queued(f.failing.sessionFile!)[0]!.preparationAttempts).toBeGreaterThanOrEqual(1);
    expect(run).toHaveBeenCalledTimes(5);
    preparation.ok = true;
    await waitFor(() => replies(actors, f.failing.id) === 1 && actors.status(f.failing.id).queued === 0);
    await pause(100);
    expect(run).toHaveBeenCalledTimes(6);
    expect(replies(actors, f.failing.id)).toBe(1);
    expect(replies(actors, f.healthy.id)).toBe(5);
    expect(actors.messages(f.failing.id, 500).filter(message => message.direction === "in" && message.source === "mesh:github.review")).toHaveLength(1);
  });

  it("(3) a restart while the item is pending does not re-run processed events and processes it exactly once", async () => {
    const f = await backlog();
    const before = f.host({ ok: false, failures: 0 });
    // Processed means finished: the healthy actor's queue checkpoint no longer holds its work.
    await waitFor(() => replies(before.actors, f.healthy.id) === 5 && queued(f.healthy.sessionFile!).length === 0 &&
      (queued(f.failing.sessionFile!)[0]?.preparationAttempts ?? 0) >= 1 &&
      (f.cursor().last?.sequence ?? 0) >= f.later.at(-1)!.sequence);
    // Crash bytes: neither a close-time queue rewrite nor a close-time cursor flush.
    const directory = path.dirname(f.failing.sessionFile!);
    const queueFile = path.join(directory, fs.readdirSync(directory).find(name => name.startsWith("queue-"))!);
    const [queueBytes, cursorBytes] = [fs.readFileSync(queueFile, "utf8"), fs.readFileSync(f.cursorPath, "utf8")];
    await before.close();
    fs.writeFileSync(queueFile, queueBytes);
    fs.writeFileSync(f.cursorPath, cursorBytes);
    const after = f.host({ ok: true, failures: 0 });
    const run = vi.spyOn(after.agents, "run");
    await waitFor(() => replies(after.actors, f.failing.id) === 1 && after.actors.status(f.failing.id).queued === 0);
    await pause(200);
    expect(run).toHaveBeenCalledTimes(1);                       // only the pending item
    expect(replies(after.actors, f.failing.id)).toBe(1);
    expect(replies(after.actors, f.healthy.id)).toBe(5);         // nothing processed is re-run
    expect(after.actors.messages(f.healthy.id, 500).filter(message => message.direction === "in")).toHaveLength(5);
    expect(queued(f.failing.sessionFile!)).toEqual([]);
  });

  // Round 3 of PR #554: a sick actor's full queue must never hold the shared host cursor.
  it("(4) past a sick actor's queue cap, its events dead-letter, the cursor advances, and recovery runs each once", async () => {
    const f = fixture();
    const seed = f.host({ ok: true, failures: 0 }, 2, 2);
    // fleet.* work is what a full queue used to hold in the shared cursor (holdWhenFull).
    const ids = await create(seed.actors, "fleet.work.review", "fleet.work.ok");
    await seed.close();
    const failing: MeshEvent[] = [];
    const healthy: MeshEvent[] = [];
    for (let index = 0; index < 30; index++) {
      failing.push(await f.mesh.publish({ topic: "fleet.work.review", from, data: { n: index } }));
      if (index % 3 === 0) healthy.push(await f.mesh.publish({ topic: "fleet.work.ok", from, data: { n: index } }));
    }
    const lastSequence = Math.max(failing.at(-1)!.sequence, healthy.at(-1)!.sequence);
    const preparation = { ok: false, failures: 0 };
    const { actors, agents } = f.host(preparation, 2, 2);
    const tail = vi.spyOn(f.mesh, "tail");
    const run = vi.spyOn(agents, "run");
    // Queue limit 2: the queue (2) and overflow (16) hold 18, plus one in preparation.
    await waitFor(() => replies(actors, ids.healthy.id) === healthy.length && (f.cursor().last?.sequence ?? 0) >= lastSequence, 10_000);
    expect(tail.mock.results.filter(result => (result.value as { events: unknown[] }).events.length === 2).length).toBeGreaterThanOrEqual(10);
    const deadLetterFile = path.join(path.dirname(ids.failing.sessionFile!), "dead-letter.jsonl");
    const deadLetters = () => fs.existsSync(deadLetterFile)
      ? fs.readFileSync(deadLetterFile, "utf8").split("\n").filter(Boolean).map(line => (JSON.parse(line) as { event: MeshEvent }).event.id) : [];
    const dead = deadLetters();
    expect(dead.length).toBeGreaterThanOrEqual(30 - 19);
    expect(dead).toEqual(failing.slice(30 - dead.length).map(event => event.id));   // the newest, in order
    expect(actors.status(ids.failing.id).queued + dead.length).toBeGreaterThanOrEqual(29);
    expect(preparation.failures).toBeGreaterThanOrEqual(1);
    // One owner alarm for the actor, not one per event.
    const alarms = () => actors.messages(ids.failing.id, 500).filter(message =>
      message.source === "fabric-host" && (message.data as { reason?: string } | undefined)?.reason === "dead_letter");
    expect(alarms()).toHaveLength(1);
    expect(run).toHaveBeenCalledTimes(healthy.length);
    preparation.ok = true;
    const answered = () => actors.messages(ids.failing.id, 500).filter(message =>
      message.direction === "out" && !message.error && message.source !== "fabric-host").length;
    await waitFor(() => answered() === 30 && actors.status(ids.failing.id).queued === 0 && !fs.existsSync(deadLetterFile), 20_000);
    await pause(200);
    expect(run).toHaveBeenCalledTimes(healthy.length + 30);
    expect(answered()).toBe(30);
    expect(replies(actors, ids.healthy.id)).toBe(healthy.length);
    const accepted = actors.messages(ids.failing.id, 500).filter(message => message.direction === "in" && message.source === "mesh:fleet.work.review")
      .map(message => (message.data as { id: string }).id);
    expect([...accepted].sort()).toEqual(failing.map(event => event.id).sort());   // each exactly once
    expect(alarms()).toHaveLength(1);
  }, 40_000);

  it("(5) a failed preparation rotates its item behind newer work instead of keeping it first", async () => {
    const f = fixture();
    const seed = f.host({ ok: true, failures: 0 });
    const ids = await create(seed.actors, "github.review", "team.work");
    await seed.close();
    const first = await f.mesh.publish({ topic: "github.review", from, data: { n: 1 } });
    const second = await f.mesh.publish({ topic: "github.review", from, data: { n: 2 } });
    // Only the first preparation fails.
    const preparation = { failures: 0, get ok() { return this.failures >= 1; } };
    const { actors, agents } = f.host(preparation);
    const run = vi.spyOn(agents, "run");
    await waitFor(() => run.mock.calls.length === 2 && actors.status(ids.failing.id).queued === 0 && actors.inFlightCount() === 0);
    expect(preparation.failures).toBe(1);
    expect(run.mock.calls.map(([request]) => request.task.includes(first.id) ? "first" : request.task.includes(second.id) ? "second" : "?"))
      .toEqual(["second", "first"]);
  });

  it("(2) archive catch-up with one pending work item does not stop live reads", async () => {
    const f = fixture(true);
    const seed = f.host({ ok: true, failures: 0 }, 50);
    const ids = await create(seed.actors, "fleet.work.review", "fleet.work.ok");
    await seed.close();
    const pending = await f.mesh.publish({ topic: "fleet.work.review", from, data: { pr: 816 } });
    const archived = [await f.mesh.publish({ topic: "fleet.work.ok", from, data: { n: 1 } }),
      await f.mesh.publish({ topic: "fleet.work.ok", from, data: { n: 2 } })];
    const retained = await f.mesh.publish({ topic: "team.noise", from, text: "retained" });
    // A live-log rewrite drops the work: only the archive still holds it.
    const live = path.join(f.mesh.root, "events.jsonl");
    const suffix = fs.readFileSync(live, "utf8").split("\n").filter(line => line && JSON.parse(line).sequence >= retained.sequence);
    fs.writeFileSync(`${live}.tmp`, suffix.join("\n") + "\n");
    fs.renameSync(`${live}.tmp`, live);
    fs.writeFileSync(path.join(f.mesh.root, "generation"), "1");
    const preparation = { ok: false, failures: 0 };
    const { actors } = f.host(preparation, 50);
    await waitFor(() => replies(actors, ids.healthy.id) === 2 && preparation.failures >= 1);
    expect(queued(ids.failing.sessionFile!)).toEqual([expect.objectContaining({ payload: expect.objectContaining({ id: pending.id }) })]);
    // Live reads continue while the archived item still fails preparation.
    const fresh = await f.mesh.publish({ topic: "fleet.work.ok", from, data: { n: 3 } });
    await waitFor(() => replies(actors, ids.healthy.id) === 3);
    await waitFor(() => (f.cursor().last?.sequence ?? 0) >= fresh.sequence);
    expect(actors.status(ids.failing.id).queued).toBe(1);
    expect(archived.every(event => actors.messages(ids.healthy.id, 500).some(message =>
      (message.data as { id?: string } | undefined)?.id === event.id))).toBe(true);
    preparation.ok = true;
    await waitFor(() => replies(actors, ids.failing.id) === 1 && actors.status(ids.failing.id).queued === 0);
    await pause(100);
    expect(replies(actors, ids.failing.id)).toBe(1);
    expect(replies(actors, ids.healthy.id)).toBe(3);
  });
});
