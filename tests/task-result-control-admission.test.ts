import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { readHostLeases } from "../src/topology/host-leases.js";
import { ParticipantDirectory, type ParticipantSnapshotSource } from "../src/topology/participant-directory.js";
import { fakeManagerHeapWorker } from "./fixtures/manager-heap-probe.js";

// Keep reason: smarty-dev#17018, a recorded cross-component trust-boundary failure.
// Only worker execution is faked. Manager publication/authentication/cache eviction,
// runtime source registration, heartbeat, control claim/ACK and Main delivery are real.
const roots: string[] = [];
const cleanup: Array<() => Promise<unknown> | unknown> = [];
afterEach(async () => {
  for (const close of cleanup.splice(0).reverse()) await close();
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  // Every removable root was allocated by this test, never a real run store/home/worktree.
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const failure = (error: unknown): { code: string | undefined; message: string } => ({
  code: (error as { code?: string })?.code,
  message: error instanceof Error ? error.message : String(error),
});
const observe = async <T>(operation: () => T | Promise<T>) => {
  try { return { value: await operation(), error: null }; }
  catch (error) { return { value: undefined, error: failure(error) }; }
};

const runtimeFixture = async () => {
  const { FabricRuntimeState } = await import("../src/fabric-runtime-state.js");
  const { CapturedToolCatalog } = await import("../src/capture/catalog.js");
  const { normalizeFabricConfig } = await import("../src/config.js");
  const base = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "task-result-control-")));
  roots.push(base);
  const meshRoot = path.join(base, "mesh"), runRoot = path.join(base, "runs");
  fs.mkdirSync(runRoot, { mode: 0o700 });
  for (const name of Object.keys(process.env)) if (name.startsWith("PI_FABRIC_")) vi.stubEnv(name, undefined);
  vi.stubEnv("PI_FABRIC_PROJECT_ROOT", base);
  vi.stubEnv("PI_FABRIC_RUN_ROOT", runRoot);
  vi.stubEnv("PI_FABRIC_TMPDIR", path.join(base, "tmp"));
  vi.stubEnv("PI_CODING_AGENT_DIR", path.join(base, "agent"));
  const sessionId = "17018aaa-bbbb-cccc-dddd-eeeeeeeeeeee", ownerId = `session:${sessionId}`;
  const model = { provider: "faux", id: "m1", name: "fixture-only model" };
  let disposed = false;
  const liveContext = {
    cwd: base, hasUI: true, mode: "rpc", model,
    isIdle: () => true, hasPendingMessages: () => false, isProjectTrusted: () => true, abort: () => {},
    modelRegistry: { getAvailable: () => [model], find: () => model,
      getApiKeyAndHeaders: async () => ({ ok: true, apiKey: "unused-fixture-key" }) },
    sessionManager: { getSessionId: () => sessionId, getSessionFile: () => undefined,
      getBranch: () => [], getLeafId: () => null, getEntries: () => [] },
    ui: { setStatus: () => {}, notify: () => {} },
  } as unknown as ExtensionContext;
  const context = new Proxy(liveContext, { get(target, key, receiver) {
    if (disposed) throw new Error("This extension ctx is stale after session replacement or reload.");
    return Reflect.get(target, key, receiver);
  } });
  const sendMessage = vi.fn();
  const host = {
    on: () => () => {}, events: { emit: () => {}, on: () => () => {} }, sendMessage,
    appendEntry: () => {}, getThinkingLevel: () => "off", getSessionName: () => "integrity-main",
  } as unknown as ExtensionAPI;
  const config = normalizeFabricConfig({ fullCodeMode: false,
    mesh: { enabled: true, root: meshRoot, actorPollMs: 20, followUpFlushMs: 0, controlIncarnationFence: "enforce" },
    agents: { enabled: true, maxConcurrent: 1, budgetUsd: 0, retainRuns: true, sessionExport: false, notifyOnComplete: false },
    residency: { enabled: false }, mcp: { enabled: false }, memory: { enabled: false }, jev: { enabled: false },
    prewalk: { enabled: false, alwaysRearm: false } });
  const runtime = new FabricRuntimeState(host, new CapturedToolCatalog(), { paths: {
    extension: path.resolve("dist/index.js"), worker: path.resolve("tests/fixtures/fake-worker.mjs"),
    residentHost: path.join(base, "unused.mjs"), skills: base,
  } });
  let directory: ParticipantDirectory | undefined;
  const sources = new Map<string, ParticipantSnapshotSource>();
  const registerSource = ParticipantDirectory.prototype.registerSource;
  vi.spyOn(ParticipantDirectory.prototype, "registerSource").mockImplementation(function (this: ParticipantDirectory, source, options) {
    if (this.options.hostId === ownerId) {
      directory = this;
      if (options?.name) sources.set(options.name, source);
    }
    return registerSource.call(this, source, options); // Preserve real sources AND their isolation policy.
  });
  let control: FabricControlPlane | undefined;
  const start = FabricControlPlane.prototype.start;
  vi.spyOn(FabricControlPlane.prototype, "start").mockImplementation(function (this: FabricControlPlane, handler) {
    if (this.options.hostId === ownerId) control = this;
    return start.call(this, handler);
  });
  cleanup.push(async () => {
    try { await runtime.shutdown("exit"); }
    finally { await directory?.close(); }
  });
  vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(request =>
    fakeManagerHeapWorker(request, request.name === "invalidate UI list" ? 0 : 64_000));
  await runtime.initialize(context, config);
  const manager = runtime.agents;
  expect(manager).toBeInstanceOf(AgentManager);
  expect(directory).toBeInstanceOf(ParticipantDirectory);
  expect(directory!.heartbeatArmed).toBe(true);
  expect(directory!.canConsumeMesh()).toBe(true);
  expect(control).toBeInstanceOf(FabricControlPlane);
  const status = (id = "main") => runtime.registry.invoke("agents.status", { id }, {
    cwd: base, signal: undefined, parentToolCallId: "17018", nestedToolCallId: "agents.status",
    extensionContext: liveContext, update() {}, approve: async () => {}, audits: [], maxResultChars: 100_000,
  });
  return { base, meshRoot, runRoot, ownerId, runtime, manager, directory: directory!, control: control!, sendMessage, status, sources,
    dispose: () => { disposed = true; }, rebind: () => runtime.bindLifecycle(liveContext, () => true) };
};

it("a hard-linked, UI-evicted task result cannot block a real Main heartbeat or exactly-once remote followUp (#17018)", async () => {
  const { base, meshRoot, runRoot, ownerId, runtime, manager, directory, sendMessage, status } = await runtimeFixture();
  const senderId = "session:integrity-observer";
  const sender = new FabricControlPlane(runtime.mesh, { id: senderId, name: "observer", kind: "main" },
    { enabled: true, hostId: senderId, pollMs: 20, acknowledgementTimeoutMs: 1_000 });
  sender.start(() => ({ accepted: false }));
  cleanup.push(() => sender.close());
  const root = directory!.get(ownerId, undefined, { fresh: true })!;
  expect(root).toMatchObject({ id: ownerId, kind: "root", ownerHostId: ownerId });
  expect(root.ownerIncarnation).toBeTypeOf("string");
  const deliver = (message: string, idempotencyKey: string) => sender.request(root.ownerHostId, root.id, "followUp",
    { message, ownerIncarnation: root.ownerIncarnation, triggerTurn: false }, root.ownerIdentityId,
    { routedRemoteHost: null, timeoutMs: 1_000, idempotencyKey });
  // A positive control establishes the same native callback/control route before the attack.
  expect(await deliver("before result integrity failure", "17018-before")).toMatchObject({ acknowledged: true });
  expect(sendMessage).toHaveBeenCalledOnce();
  expect(sendMessage).toHaveBeenLastCalledWith(expect.objectContaining({
    content: expect.stringContaining("before result integrity failure"),
  }), expect.objectContaining({ deliverAs: "followUp", triggerTurn: false }));

  const genuine = await manager.run({ name: "integrity source", task: "complete a real managed task",
    transport: "process", extensions: false });
  expect(genuine).toMatchObject({ status: "completed" });
  expect(genuine.text).toHaveLength(64_000);
  expect(manager.isSettled(genuine.id)).toBe(true);
  const resultFile = path.join(runRoot, genuine.id, "manager-result.json");
  const resultBytes = fs.readFileSync(resultFile);
  const originalStat = fs.statSync(resultFile);
  expect(originalStat.nlink).toBe(1);
  expect(JSON.parse(resultBytes.toString())).toMatchObject({ id: genuine.id, status: "completed" });

  // Drive the real byte bound, without private-field edits or hundreds of synthetic handles.
  // A disk read by listForUi proves its in-memory snapshot was genuinely evicted.
  // Stop at the first such read: it is the first over-budget row, not an unreachable old row.
  const openSync = fs.openSync;
  let pressureRuns = 0, evictionReads = 0;
  const open = vi.spyOn(fs, "openSync").mockImplementation((file, flags, mode) => {
    if (String(file) === resultFile && new Error().stack?.includes("listForUi")) evictionReads++;
    return openSync(file, flags, mode);
  });
  for (; pressureRuns < 12 && evictionReads === 0;) {
    await manager.run({ task: `bounded UI pressure ${pressureRuns++}`, transport: "process", extensions: false });
    manager.listForUi();
  }
  expect(evictionReads, "public UI byte pressure must evict and rehydrate the actual task result").toBeGreaterThan(0);
  await directory!.refresh();
  expect(directory!.canConsumeMesh()).toBe(true);

  // Reproduce cp -al's identity change inside the test-owned directory only.
  const preserved = path.join(base, "preserved-manager-result.json");
  fs.linkSync(resultFile, preserved);
  expect(fs.statSync(resultFile).nlink).toBe(2);
  expect(fs.statSync(resultFile).ino).toBe(originalStat.ino);
  expect(fs.readFileSync(resultFile)).toEqual(resultBytes);
  // Invalidate the public list cache using another real run, too small to displace
  // the existing first over-budget row. Rehydration above pruned that row again.
  await manager.run({ name: "invalidate UI list", task: "invalidate the cached public list",
    transport: "process", extensions: false });
  sendMessage.mockClear();

  const strictReads = async () => ({
    status: (await observe(() => manager.status(genuine.id))).error,
    wait: (await observe(() => manager.wait(genuine.id))).error,
    // stop on an already-settled task is also an exact result read, not a relaunch.
    terminalResult: (await observe(() => manager.stop(genuine.id))).error,
  });
  const integrityBefore = await strictReads();
  const leaseBefore = readHostLeases(meshRoot).get(ownerId)!.updatedAt;
  const probes = [vi.spyOn(fs, "lstatSync"), vi.spyOn(fs, "statSync"), vi.spyOn(fs, "readFileSync"), open.mockClear()];
  const resultReaders = [vi.spyOn(manager, "listForUi"), vi.spyOn(manager, "list"),
    vi.spyOn(manager, "status"), vi.spyOn(manager, "wait")];
  const heartbeat = await observe(() => directory!.refresh());
  const heartbeatResultProbes = probes.reduce((count, probe) => count +
    probe.mock.calls.filter(([file]) => String(file) === resultFile).length, 0);
  for (const reader of resultReaders) {
    expect.soft(reader, "heartbeat must never enter a result-bearing manager reader").not.toHaveBeenCalled();
    reader.mockRestore();
  }
  const canConsumeAfterHeartbeat = directory!.canConsumeMesh();
  const leaseAfter = readHostLeases(meshRoot).get(ownerId)!.updatedAt;
  // Execute the real remote command EVEN IF the baseline heartbeat failed, so RED
  // demonstrates the user-visible blocked inbox rather than just a mocked gate.
  const remote = await observe(() => deliver("after result integrity failure", "17018-after"));
  const replay = remote.error === null
    ? await observe(() => deliver("after result integrity failure", "17018-after"))
    : undefined;
  const integrityAfter = await strictReads();
  const commands = runtime.mesh.read({ topic: "fabric.control.command", limit: 100 }).filter(event =>
    (event.data as { message?: string }).message === "after result integrity failure");
  const commandIds = new Set(commands.map(event => (event.data as { commandId: string }).commandId));
  const acknowledgements = runtime.mesh.read({ topic: "fabric.control.ack", limit: 100 }).filter(event =>
    commandIds.has((event.data as { commandId: string }).commandId));
  const claims = runtime.mesh.listAll("topology/control-seen/").filter(entry =>
    commandIds.has((entry.value as { commandId: string }).commandId));
  console.info("17018 actual runtime/control receipt", JSON.stringify({
    runId: genuine.id, pressureRuns, evictionReads, nlink: fs.statSync(resultFile).nlink,
    heartbeat: heartbeat.error, heartbeatResultProbes, canConsumeAfterHeartbeat, leaseBefore, leaseAfter,
    remote, replay, commandIds: [...commandIds], acknowledgements: acknowledgements.map(event => event.data),
    claims: claims.map(entry => entry.value), nativeCalls: sendMessage.mock.calls,
    integrityBefore, integrityAfter,
  }));

  // Collect the complete path before asserting the regression, preserving strict
  // result authentication both before and after the desired control admission.
  for (const reads of [integrityBefore, integrityAfter]) {
    for (const error of Object.values(reads)) expect.soft(error).toMatchObject({
      code: "FABRIC_MISSING_RESULT", message: expect.stringContaining("no saved terminal result"),
    });
  }
  await expect(status()).resolves.toMatchObject({ kind: "main", controlAdmission: "admitted" });
  expect.soft(heartbeat.error, "a task's integrity error is not a Main heartbeat failure").toBeNull();
  expect.soft(heartbeatResultProbes, "heartbeat presence must not authenticate result files").toBe(0);
  expect.soft(canConsumeAfterHeartbeat, "a corrupt result must not close Main control admission").toBe(true);
  expect.soft(leaseAfter).toBeGreaterThanOrEqual(leaseBefore);
  expect.soft(remote.error, "the real remote followUp must be admitted and ACKed").toBeNull();
  expect.soft(remote.value).toMatchObject({ queued: true, routed: "mesh", acknowledged: true });
  expect.soft(replay?.error).toBeNull();
  expect.soft(replay?.value).toMatchObject({ acknowledged: true, messageId: remote.value?.messageId });
  // FILE retries republish the same logical command; dedupe is at claim/delivery, not event append.
  expect.soft(commands).toHaveLength(2);
  expect.soft(commandIds.size).toBe(1);
  expect.soft(acknowledgements).toContainEqual(expect.objectContaining({ data: expect.objectContaining({
    accepted: true, ownerIncarnation: root.ownerIncarnation,
  }) }));
  expect.soft(claims).toHaveLength(1);
  expect.soft(sendMessage).toHaveBeenCalledOnce();
  expect.soft(sendMessage).toHaveBeenLastCalledWith(expect.objectContaining({
    content: expect.stringContaining("after result integrity failure"),
  }), expect.objectContaining({ deliverAs: "followUp", triggerTurn: false }));
  expect.soft(directory!.get(ownerId, undefined, { fresh: true })).toMatchObject({
    id: ownerId, ownerHostId: ownerId, ownerIncarnation: root.ownerIncarnation,
  });
}, 30_000);

it("the real runtime isolates only independent task/actor/provider source faults, not root publication", async () => {
  const { ProviderParticipantRegistry } = await import("../src/topology/provider-participants.js");
  const { runtime, manager, directory, control, ownerId, status } = await runtimeFixture();
  const task = await manager.run({ task: "publish a real managed task", transport: "process", extensions: false });
  await directory.refresh();
  expect(directory.get(task.id, undefined, { fresh: true })).toMatchObject({ id: task.id, kind: "agent" });
  const root = directory.get(ownerId, undefined, { fresh: true })!;
  const assertHealthy = async () => {
    await expect(directory.refresh()).resolves.toBeUndefined();
    expect(directory.canConsumeMesh()).toBe(true);
    expect(control.options.canConsumeMesh?.()).toBe(true);
    await expect(status()).resolves.toMatchObject({ controlAdmission: "admitted" });
    expect(directory.get(ownerId, undefined, { fresh: true })).toMatchObject({
      id: root.id, ownerHostId: root.ownerHostId, ownerIncarnation: root.ownerIncarnation,
    });
  };
  const taskSource = vi.spyOn(manager, "listForPresence").mockImplementation(() => { throw new Error("task source unavailable"); });
  await assertHealthy();
  expect(taskSource).toHaveBeenCalled();
  expect(directory.list({ scope: "project", includeStale: true, fresh: true }).some(row => row.id === task.id)).toBe(false);
  taskSource.mockRestore();
  await assertHealthy();
  expect(directory.get(task.id, undefined, { fresh: true })).toMatchObject({ id: task.id });

  const actorSource = vi.spyOn(runtime.actors, "listOwned").mockImplementation(() => { throw new Error("actor source unavailable"); });
  await assertHealthy();
  expect(actorSource).toHaveBeenCalled();
  actorSource.mockRestore();
  const providerSource = vi.spyOn(ProviderParticipantRegistry.prototype, "records").mockImplementation(() => {
    throw new Error("provider source unavailable");
  });
  await assertHealthy();
  expect(providerSource).toHaveBeenCalled();
  providerSource.mockRestore();

  const rootFailure = new Error("root publication unavailable");
  const rootSource = vi.spyOn(directory, "root").mockImplementation(() => { throw rootFailure; });
  await expect(directory.refresh()).rejects.toBe(rootFailure);
  expect(directory.canConsumeMesh()).toBe(false);
  expect(control.options.canConsumeMesh?.()).toBe(false);
  await expect(status()).resolves.toMatchObject({ controlAdmission: "gated",
    controlAdmissionReason: expect.stringContaining(rootFailure.message) });
  rootSource.mockRestore();
  // canConsumeMesh starts real recovery on a fatal FILE refresh. Drain any selection made before restore.
  await observe(() => directory.refresh());
  await assertHealthy();
}, 30_000);

it.each(["project", "session"] as const)("one %s actor's strict preparing-result fault preserves healthy actors, reports its id, and recovers (#17018 P2-1)", async (scope) => {
  const { base, runRoot, ownerId, runtime, manager, directory, control, status, sources, sendMessage } = await runtimeFixture();
  const deferred = () => {
    let resolve!: () => void;
    const promise = new Promise<void>(done => { resolve = done; });
    return { promise, resolve };
  };
  const blockerExit = deferred(), queued = deferred(), terminal = deferred(), handoff = deferred();
  let blockerAlive = true;
  vi.mocked(ProcessTransport.prototype.launch).mockImplementation(request => fakeManagerHeapWorker(request, 0, false,
    request.name === "hold actor permit" ? {
      closed: blockerExit.promise, alive: () => blockerAlive,
      stop: async () => { blockerAlive = false; blockerExit.resolve(); },
    } : undefined));
  const actors = runtime.actors;
  const damaged = await actors.create({ name: "damaged preparing actor", instructions: "Reply.", responseMode: "text",
    transport: "process", scope });
  const healthy = await actors.create({ name: "healthy peer actor", instructions: "Wait.", scope });
  const otherScope = await actors.create({ name: "healthy other-scope actor", instructions: "Wait.", scope: scope === "project" ? "session" : "project" });
  const blocker = await manager.spawn({ name: "hold actor permit", task: "hold the only permit", transport: "process", extensions: false });
  let activationId: string | undefined;
  const run = manager.run;
  // Make the brief stale-preparing window deterministic: defer delivery of native
  // launch notifications and the result to this actor, NOT execution/settlement.
  // onQueued is real; the same actual queued run then completes and authenticates
  // its own manager-result.json. No actor private fields or synthetic records.
  const delayed = vi.spyOn(manager, "run").mockImplementation(async function (this: AgentManager, ...args) {
    if (args[0].actorId !== damaged.id) return run.apply(this, args);
    const [request, signal, onSpawned, authorize, downgrade, onQueued, preparation] = args;
    if (!preparation) throw new Error("Actor activation must supply launch preparation options");
    const notifications: Array<() => void> = [];
    const result = await run.call(this, request, signal,
      handle => { notifications.push(() => onSpawned?.(handle)); }, authorize, downgrade,
      handle => { activationId = handle.id; onQueued?.(handle); queued.resolve(); },
      { ...preparation, onPreparing: () => { notifications.push(() => preparation?.onPreparing?.()); } });
    terminal.resolve();
    await handoff.promise;
    for (const notify of notifications) notify();
    return result;
  });
  const activation = actors.ask(damaged.id, "complete this real queued activation");
  // Attach the observer immediately so a failing test cannot leak a rejection.
  const outcome = observe(() => activation);
  cleanup.push(async () => {
    blockerAlive = false; blockerExit.resolve(); handoff.resolve();
    await outcome;
  });
  try {
    await queued.promise;
    expect(activationId).toBeTypeOf("string");
    expect(actors.status(damaged.id)).toMatchObject({ status: "waiting", preparing: { runId: activationId } });
    blockerAlive = false; blockerExit.resolve();
    await expect(manager.wait(blocker.id)).resolves.toMatchObject({ status: "completed" });
    await terminal.promise;
    expect(manager.isSettled(activationId!)).toBe(true);
    expect(actors.status(damaged.id)).toMatchObject({ preparing: { runId: activationId } });
    await directory.refresh();
    for (const actor of [damaged, healthy, otherScope]) expect(directory.get(actor.id, undefined, { fresh: true }))
      .toMatchObject({ id: actor.id, kind: "actor" });
    expect([...sources.keys()].sort()).toEqual(["actors", "agents", "providers"]);

    const resultFile = path.join(runRoot, activationId!, "manager-result.json");
    const bytes = fs.readFileSync(resultFile);
    const preserved = path.join(base, "preserved-actor-result.json");
    expect(fs.statSync(resultFile).nlink).toBe(1);
    fs.linkSync(resultFile, preserved);
    expect(fs.statSync(resultFile).nlink).toBe(2);
    expect(fs.readFileSync(resultFile)).toEqual(bytes);
    const strictError = (await observe(() => actors.listOwned(true))).error;
    expect(strictError).toMatchObject({ code: "FABRIC_MISSING_RESULT" });
    expect((await observe(() => actors.listOwned())).error).toEqual(strictError);
    expect((await observe(() => actors.list())).error).toEqual(strictError);
    expect((await observe(() => actors.status(damaged.id))).error).toEqual(strictError);
    const reportFailure = vi.fn();
    const records = sources.get("actors")!(reportFailure);
    expect(records.map(record => record.id).sort()).toEqual([healthy.id, otherScope.id].sort());
    expect(reportFailure).toHaveBeenCalledExactlyOnceWith(expect.objectContaining({ code: "FABRIC_MISSING_RESULT" }), damaged.id);
    await expect(directory.refresh()).resolves.toBeUndefined();
    expect(directory.isolatedSourceFailures()).toContainEqual({ source: "actors", participantId: damaged.id,
      reason: "participant snapshot source failed", since: expect.any(Number) });
    expect(directory.get(damaged.id, undefined, { fresh: true })).toBeUndefined();
    for (const actor of [healthy, otherScope]) expect(directory.get(actor.id, undefined, { fresh: true }))
      .toMatchObject({ id: actor.id, kind: "actor" });
    expect(directory.get(ownerId, undefined, { fresh: true })).toMatchObject({ id: ownerId, kind: "root" });
    expect(control.options.canConsumeMesh?.()).toBe(true);
    await expect(status()).resolves.toMatchObject({ controlAdmission: "admitted" });
    const senderId = "session:actor-integrity-observer";
    const sender = new FabricControlPlane(runtime.mesh, { id: senderId, name: "observer", kind: "main" },
      { enabled: true, hostId: senderId, pollMs: 20, acknowledgementTimeoutMs: 1_000 });
    sender.start(() => ({ accepted: false }));
    cleanup.push(() => sender.close());
    const root = directory.get(ownerId, undefined, { fresh: true })!;
    await expect(sender.request(root.ownerHostId, root.id, "followUp", {
      message: "control survives one damaged actor", ownerIncarnation: root.ownerIncarnation, triggerTurn: false,
    }, root.ownerIdentityId, { routedRemoteHost: null, timeoutMs: 1_000, idempotencyKey: `17018-actor-${scope}` }))
      .resolves.toMatchObject({ acknowledged: true });
    expect(sendMessage).toHaveBeenLastCalledWith(expect.objectContaining({
      content: expect.stringContaining("control survives one damaged actor"),
    }), expect.objectContaining({ deliverAs: "followUp", triggerTurn: false }));
    expect((await observe(() => manager.status(activationId!))).error).toEqual(strictError);
    // Recovery is actor lifecycle progress, NOT unlink/rewrite of an authenticated
    // file (unlink cannot restore ctime). Native archival may then retire its handle.
    handoff.resolve();
    expect((await outcome).error).toBeNull();
    expect(actors.status(damaged.id)).not.toHaveProperty("preparing");
    await directory.refresh();
    expect(directory.get(damaged.id, undefined, { fresh: true })).toMatchObject({ id: damaged.id, kind: "actor" });
    expect(directory.isolatedSourceFailures()).toEqual([]);
    reportFailure.mockClear();
    expect(sources.get("actors")!(reportFailure).map(record => record.id)).toContain(damaged.id);
    expect(reportFailure).not.toHaveBeenCalled();
    // Archival can report Unknown instead of MISSING_RESULT after consumption,
    // but must never make an unauthenticated result readable again.
    expect(await observe(() => manager.status(activationId!))).toMatchObject({ value: undefined, error: expect.any(Object) });
  } finally {
    blockerAlive = false; blockerExit.resolve(); handoff.resolve();
    await outcome;
    delayed.mockRestore();
  }
}, 30_000);

it("local Main status includes the real runtime's disposed-binding gate even with a confirmed FILE directory", async () => {
  const { directory, control, ownerId, dispose, rebind, status } = await runtimeFixture();
  expect(directory.canConsumeMesh()).toBe(true);
  expect(control.options.canConsumeMesh?.()).toBe(true);
  await expect(status()).resolves.toMatchObject({ controlAdmission: "admitted" });
  const confirmedAt = directory.confirmedAt();
  dispose(); // Native ctx invalidation, not a mocked admission result or a directory source fault.
  expect(control.options.canConsumeMesh?.()).toBe(false);
  expect(directory.canConsumeMesh()).toBe(true); // Preserve FILE's existing directory-only semantics.
  expect(directory.confirmedAt()).toBe(confirmedAt);
  expect(directory.heartbeatArmed).toBe(false);
  for (const id of ["main", ownerId]) await expect(status(id)).resolves.toMatchObject({
    kind: "main", controlAdmission: "gated", controlAdmissionReason: "host control not ready",
  });
  rebind();
  expect(control.options.canConsumeMesh?.()).toBe(true);
  expect(directory.heartbeatArmed).toBe(true);
  await expect(status()).resolves.toMatchObject({ controlAdmission: "admitted" });
}, 30_000);
