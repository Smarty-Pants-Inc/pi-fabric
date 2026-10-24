// Keep reason: data safety and the resident/actor trust boundary (#11962).
// Real actor queues, registry, completion fences, mesh pages and native fixture workers.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ActorManager } from "../src/actors/manager.js";
import { ActorDirectory } from "../src/actors/directory.js";
import { ActorMeshMonitor } from "../src/actors/mesh-monitor.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshEvent } from "../src/mesh/store.js";

const roots: string[] = [];
const closers: Array<() => Promise<void>> = [];
const identity = { id: "session:drain-test", name: "Main", kind: "main" as const, sessionId: "drain-test" };
const root = () => { const value = fs.mkdtempSync(path.join(os.tmpdir(), "actor-drain-")); roots.push(value); return value; };
const deferred = <T = void>() => {
  let resolve!: (value: T | PromiseLike<T>) => void;
  const promise = new Promise<T>(done => { resolve = done; });
  return { promise, resolve };
};
const flush = async () => { for (let i = 0; i < 20; ++i) await Promise.resolve(); };
function changed(actors: ActorManager, predicate: () => boolean): Promise<void> {
  if (predicate()) return Promise.resolve();
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => { unsubscribe(); reject(new Error("Actor boundary event deadline")); }, 12_000);
    const unsubscribe = actors.subscribe(() => {
      if (predicate()) { clearTimeout(timer); unsubscribe(); resolve(); }
    });
  });
}
function fixture(paused = true, defaultScope: "project" | "session" = "project") {
  const dir = root();
  const mesh = new MeshStore(path.join(dir, "mesh"), 64 * 1024, 100);
  const agents = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: true, notifyOnComplete: false }, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(dir, "runs"),
  });
  const actors = new ActorDirectory([identity.sessionId, identity, mesh,
    { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 50 }, agents, () => {}, {
      persistent: true, releasePaused: paused, meshCursorPath: path.join(dir, "cursor"),
      meshRetentionSweepPath: false, reapDeadSessionPresence: false,
    }], { project: path.join(dir, "project"), session: path.join(dir, "session") }, defaultScope);
  closers.push(async () => { await actors.close(); await agents.close(); });
  return { dir, mesh, agents, actors };
}
const spec = (name: string, scope: "project" | "session" = "project") => ({ name, scope, instructions: "Reply", residency: "durable" as const,
  responseMode: "text" as const, delivery: "mailbox" as const, tools: [], extensions: false });
const queueFile = (dir: string, id: string, scope = "project") => {
  const directory = path.join(dir, scope, id);
  return path.join(directory, fs.readdirSync(directory).find(name => name.startsWith("queue-") && name.endsWith(".json"))!);
};
const json = (file: string): any => JSON.parse(fs.readFileSync(file, "utf8"));
const incoming = (actors: ActorManager, id: string) => actors.messages(id).filter(message => message.direction === "in");

afterEach(async () => {
  vi.useRealTimers();
  vi.restoreAllMocks();
  for (const close of closers.splice(0).reverse()) await close();
  // The required Node preload records/retains these calls; native deletion is not used locally.
  for (const directory of roots.splice(0)) fs.rmSync(directory, { recursive: true, force: true });
});

describe("strict cooperative resident actor admission boundary", () => {
  it("settles pre-cut native runs naturally, holds FIFO tails and callback asks in both scopes, then resumes exactly once", async () => {
    const { dir, actors, agents } = fixture(false);
    const abort = vi.spyOn(agents, "stop");
    const values = await Promise.all((["project", "session"] as const).map(scope => actors.create(spec(`active-${scope}`, scope))));
    const releases = values.map(value => path.join(dir, `release-${value.scope}`));
    values.forEach((value, index) => actors.tell(value.id, "LIVE_WITH_PROGRESS", { fakeWorkerReleasePath: releases[index] }));
    await changed(actors, () => values.every(value => !!actors.status(value.id).inFlightRun));
    const tails = values.map(value => actors.tell(value.id, "fifo-tail").messageId);
    const asks = values.map(value => actors.ask(value.id, "callback-tail"));
    const askIds = values.map(value => incoming(actors, value.id).at(-1)!.id);
    actors.pauseForRelease({ strict: true });
    values.forEach(value => expect(() => actors.tell(value.id, "held-new")).toThrow(/admission held/));
    releases.forEach(file => fs.writeFileSync(file, "settle naturally"));
    await changed(actors, () => actors.inFlightCount() === 0);
    expect(abort).not.toHaveBeenCalled();
    values.forEach((value, index) => {
      expect(actors.status(value.id).queued).toBe(2);
      expect(actors.messages(value.id).filter(message => message.direction === "out")).toHaveLength(1);
      expect(incoming(actors, value.id).map(message => message.id).slice(-2)).toEqual([tails[index], askIds[index]]);
    });
    await expect(actors.checkpointForRelease()).rejects.toThrow(/callback asks retained in A/);
    expect(() => actors.releaseCheckpointReceipt()).toThrow(/checkpoint/);
    actors.resumeAfterRelease();
    await Promise.all(asks);
    await changed(actors, () => actors.inFlightCount() === 0);
    values.forEach(value => {
      expect(actors.status(value.id).queued).toBe(0);
      expect(actors.messages(value.id).filter(message => message.direction === "out")).toHaveLength(3);
    });
  }, 25_000);

  it.each(["project", "session"] as const)("checks disk queues/order/attempts, registry and completion obligations before both cursor receipts (default %s)", async defaultScope => {
    const { dir, actors } = fixture(true, defaultScope);
    const values = await Promise.all((["project", "session"] as const).map(scope => actors.create(spec(`queued-${scope}`, scope))));
    const ids = values.map(value => [actors.tell(value.id, "first").messageId, actors.tell(value.id, "second").messageId]);
    const completion = path.join(dir, "project", values[0]!.id, "child-completions");
    fs.mkdirSync(completion, { recursive: true });
    const completionId = "c".repeat(32);
    fs.writeFileSync(path.join(completion, `${completionId}.consumed`), JSON.stringify({ id: completionId, consumedAt: 123 }));
    actors.pauseForRelease({ strict: true });
    const cursor = vi.spyOn(ActorMeshMonitor.prototype, "checkpointForRelease");
    await actors.checkpointForRelease();
    const receipt = actors.releaseCheckpointReceipt() as any;
    values.forEach((value, index) => {
      const queue = receipt[value.scope!].queues.find((entry: any) => entry.actorId === value.id);
      expect(queue.items.map((item: any) => item.id)).toEqual(ids[index]);
      expect(queue.items.map((item: any) => [item.attempts, item.preparationAttempts, item.executionStarted])).toEqual([[0, 0, false], [0, 0, false]]);
      expect(queue.cleanHandover).toBe(true);
      expect(json(queue.file).items).toEqual(queue.items);
      expect(receipt[value.scope!].cursor.persisted).toBe(true);
    });
    expect(cursor).toHaveBeenCalledTimes(2);
    expect(receipt.project.files.some((file: any) => file.file.endsWith(`${completionId}.consumed`) && file.present)).toBe(true);
    const file = queueFile(dir, values[0]!.id);
    const original = fs.readFileSync(file, "utf8");
    const tampered = json(file); tampered.items.reverse(); tampered.items[0].attempts = 7;
    fs.writeFileSync(file, JSON.stringify(tampered));
    expect(() => actors.releaseCheckpointReceipt()).toThrow(/disk readback differs/);
    fs.writeFileSync(file, original);
    fs.writeFileSync(path.join(completion, `${completionId}.consumed`), "{}");
    expect(() => actors.releaseCheckpointReceipt()).toThrow(/disk readback differs/);
    actors.resumeAfterRelease();
    expect(() => actors.releaseCheckpointReceipt()).toThrow(/checkpoint/);
    await changed(actors, () => actors.inFlightCount() === 0);
  }, 20_000);

  it("checks both untouched empty scope registries as absent, without manufacturing registry rows", async () => {
    const { dir, actors } = fixture();
    actors.pauseForRelease({ strict: true });
    await actors.checkpointForRelease();
    const receipt = actors.releaseCheckpointReceipt() as any;
    for (const scope of ["project", "session"]) {
      expect(receipt[scope].queues).toEqual([]);
      expect(receipt[scope].files.find((file: any) => file.file.endsWith("actors.json"))).toMatchObject({ present: false });
      expect(fs.existsSync(path.join(dir, scope, "actors.json"))).toBe(false);
      expect(receipt[scope].cursor.persisted).toBe(true);
    }
  });

  it("vetoes unconfirmed actor notification publication without consuming the accepted queue or checkpointing a cursor", async () => {
    const { actors, mesh } = fixture();
    const actor = await actors.create(spec("notification-obligation"));
    const publish = mesh.publish.bind(mesh);
    vi.spyOn(mesh, "publish").mockImplementation(request => request.topic === "fabric.actor.input"
      ? Promise.reject(new Error("notification publication fault")) : publish(request));
    const id = actors.tell(actor.id, "accepted-notification").messageId;
    actors.pauseForRelease({ strict: true });
    await flush();
    const cursor = vi.spyOn(ActorMeshMonitor.prototype, "checkpointForRelease");
    await expect(actors.checkpointForRelease()).rejects.toThrow(/unconfirmed publication/);
    expect(cursor).not.toHaveBeenCalled();
    expect(incoming(actors, actor.id).map(message => message.id)).toContain(id);
    expect(actors.status(actor.id).queued).toBe(1);
  });

  it("holds ingress before skip/coalesce and suspends overdue settled windows without changing their original deadline or delivery ID", async () => {
    const { dir, actors } = fixture();
    const actor = await actors.create({ ...spec("settled"), events: ["agent_settled"], coalesce: false, activation: { minIntervalMs: 1_000 } });
    actors.resumeQueued();
    vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout"] });
    vi.setSystemTime(10_000);
    const payload = (marker: string) => ({ marker, session: { id: "observed-source" } });
    expect(actors.dispatchHostEvent("agent_settled", payload("leading"))).toBe(1);
    vi.advanceTimersByTime(100);
    actors.dispatchHostEvent("agent_settled", payload("original-pending"));
    // pi-fabric#1572: the new filter would skip the queued leading settle, which is admitted work. Before the guard it was
    // dropped at the head after resume; setActivationFilter now refuses that unless dropQueued, which holds it instead.
    await actors.setActivationFilter(actor.id, [{ id: "skip-host", source: ["host:agent_settled"] }], undefined, undefined, undefined, { dropQueued: true });
    const file = queueFile(dir, actor.id);
    const deadLetters = () => fs.readFileSync(path.join(dir, "project", actor.id, "dead-letter.jsonl"), "utf8").split("\n").filter(Boolean).map(line => JSON.parse(line));
    expect(deadLetters()).toEqual([expect.objectContaining({ source: "host:agent_settled", heldBy: expect.objectContaining({ ruleId: "skip-host" }) })]);
    const before = json(file).settledWindows;
    actors.pauseForRelease({ strict: true });
    expect(actors.dispatchObservedHostEvent("agent_settled", payload("must-not-skip-or-replace"))).toBe(0);
    expect(actors.dispatchHostEvent("input", {})).toBe(0);
    vi.advanceTimersByTime(5_000);
    await actors.checkpointForRelease();
    expect(json(file).settledWindows).toEqual(before);
    expect(json(file).items).toHaveLength(0); // held ingress admitted nothing; the leading settle stays held, not queued
    expect(deadLetters()).toHaveLength(1);
    expect(actors.status(actor.id).filterSkipped?.count ?? 0).toBe(0);
    actors.resumeAfterRelease();
    await flush();
    // Normal post-resume skip can remove an obligation-free empty queue file.
    expect(fs.existsSync(file) ? json(file).settledWindows : undefined).toBeUndefined();
    expect(actors.status(actor.id).filterSkipped?.count).toBeGreaterThanOrEqual(1);
    expect(incoming(actors, actor.id).some(message => JSON.stringify(message.data).includes("must-not-skip-or-replace"))).toBe(false);
  });

  it("preserves settled windows and unread authenticated root relays across a strict hold", async () => {
    const monitors: ActorMeshMonitor[] = [];
    vi.spyOn(ActorMeshMonitor.prototype, "start").mockImplementation(function (this: ActorMeshMonitor) { monitors.push(this); });
    const { dir, actors, mesh } = fixture();
    const values = await Promise.all((["project", "session"] as const).map(scope => actors.create({
      ...spec(`held-relay-${scope}`, scope), events: ["agent_settled"], coalesce: false, activation: { minIntervalMs: 1_000 },
    })));
    actors.resumeQueued(); // Bootstrap ready, while legacy releasePaused still gates workers.
    expect(monitors).toHaveLength(2);
    monitors.forEach(monitor => {
      expect(monitor.callbacks.canAdmit!()).toBe(true);
      expect(monitor.callbacks.beforePoll()).toBe(true);
    });
    vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout"] });
    vi.setSystemTime(10_000);
    const payload = (marker: string) => ({ marker }); // Authenticated root fallback, no session.id.
    expect(actors.dispatchHostEvent("agent_settled", payload("leading"))).toBe(2);
    vi.advanceTimersByTime(100);
    expect(actors.dispatchHostEvent("agent_settled", payload("original-pending"))).toBe(0);
    const files = values.map(value => queueFile(dir, value.id, value.scope));
    const snapshots = files.map(json);
    const cursors = monitors.map(monitor => {
      // start() is suppressed above; explicitly seed its untouched safe cursor.
      monitor.checkpointForRelease();
      return monitor.releaseCheckpointReceipt() as any;
    });
    actors.pauseForRelease({ strict: true });
    const relays = await Promise.all(values.map(value => mesh.publish({
      topic: "fabric.actor.host-event", kind: "agent_settled", from: identity, to: value.id,
      data: { version: 1, actorId: value.id, event: "agent_settled", payload: payload("held-relay"), mainRevision: 99, taskRevision: 99, idle: false },
    })));
    monitors.forEach((monitor, index) => {
      expect(monitor.callbacks.canAdmit!()).toBe(false);
      expect(monitor.callbacks.beforePoll()).toBe(false);
      expect(monitor.callbacks.onEvent(relays[index]!)).toBe(false);
      monitor.schedule(); // Real monitor poll must leave these original IDs unread.
    });
    expect(actors.dispatchObservedHostEvent("agent_settled", payload("held-local"))).toBe(0);
    actors.resumeQueued(); // Bootstrap/ownership readiness cannot lift the cooperative hold.
    vi.advanceTimersByTime(5_000);
    await flush();
    await actors.checkpointForRelease();
    const receipt = actors.releaseCheckpointReceipt() as any;
    values.forEach((value, index) => {
      const saved = json(files[index]!);
      expect(saved).toEqual(snapshots[index]); // IDs/order/attempts, freshness and pending receipt unchanged.
      expect(saved.settledWindows).toEqual([expect.objectContaining({
        sourceId: identity.id, acceptedAt: 10_000, intervalMs: 1_000,
        pending: expect.objectContaining({ payload: payload("original-pending"), sourceRootId: identity.id, observedAt: 10_100, deliveryId: expect.any(String) }),
      })]);
      expect(incoming(actors, value.id).map(message => message.data)).toEqual([payload("leading")]);
      expect(actors.status(value.id).queued).toBe(1);
      expect(actors.status(value.id).filterSkipped?.count ?? 0).toBe(0);
      expect(receipt[value.scope!].cursor).toMatchObject({ cursor: cursors[index].cursor, persisted: true });
      expect(receipt[value.scope!].cursor.last).toEqual(cursors[index].last);
      expect(receipt[value.scope!].cursor.last?.id).not.toBe(relays[index]!.id);
    });
    expect(actors.inFlightCount()).toBe(0);
  });

  it("limits trusted pre-cut authority to synchronous tell, not asks, creation, host events or later microtasks", async () => {
    const { actors } = fixture();
    const actor = await actors.create(spec("trusted-ingress", "session"));
    actors.pauseForRelease({ strict: true });
    const accepted = actors.withReleaseAdmission(() => actors.tell(actor.id, "already-accepted-handler"));
    expect(actors.status(actor.id).queued).toBe(1);
    expect(actors.inFlightCount()).toBe(0);
    expect(() => actors.withReleaseAdmission(() => actors.ask(actor.id, "new-ask"))).toThrow(/admission held/);
    expect(() => actors.withReleaseAdmission(() => actors.create(spec("new-create")))).toThrow(/synchronous/);
    await expect(actors.create(spec("new-create"))).rejects.toThrow(/admission held/);
    let late: unknown;
    actors.withReleaseAdmission(() => { queueMicrotask(() => { try { actors.tell(actor.id, "late"); } catch (error) { late = error; } }); });
    await flush();
    expect(late).toBeInstanceOf(Error);
    expect(String(late)).toMatch(/admission held/);
    await actors.checkpointForRelease();
    expect(JSON.stringify(actors.releaseCheckpointReceipt())).toContain(accepted.messageId);
    actors.resumeAfterRelease();
    await changed(actors, () => actors.inFlightCount() === 0);
  });

  it("invalidates an awaited registry checkpoint after timeout/resume before any late registry commit or cursor publication", async () => {
    const { actors } = fixture();
    const actor = await actors.create(spec("stale-checkpoint"));
    const id = actors.tell(actor.id, "accepted-before-cut").messageId;
    actors.pauseForRelease({ strict: true });
    const gate = deferred(), reached = deferred();
    const actual = ActorRegistryStore.prototype.withLock;
    let delay = true;
    vi.spyOn(ActorRegistryStore.prototype, "withLock").mockImplementation(function (this: ActorRegistryStore, ...args: any[]) {
      if (delay && args[2] === "manager-update") { reached.resolve(); return gate.promise.then(() => actual.apply(this, args as any)); }
      return actual.apply(this, args as any);
    });
    const cursor = vi.spyOn(ActorMeshMonitor.prototype, "checkpointForRelease");
    let valid = true;
    const checkpoint = actors.checkpointForRelease(() => { if (!valid) throw new Error("endpoint deadline invalidated"); });
    const rejected = expect(checkpoint).rejects.toThrow(/invalidated|stale/);
    await reached.promise;
    valid = false; delay = false;
    actors.resumeAfterRelease();
    gate.resolve();
    await rejected;
    expect(cursor).not.toHaveBeenCalled();
    expect(() => actors.releaseCheckpointReceipt()).toThrow(/checkpoint/);
    await changed(actors, () => actors.inFlightCount() === 0);
    expect(incoming(actors, actor.id).filter(message => message.id === id)).toHaveLength(1);
  });

  it("holds a constructor-scheduled parked restore across the cut with exact nonzero attempt/freshness fences", async () => {
    const { dir, mesh, actors, agents } = fixture();
    const actor = await actors.create({ ...spec("restored-evidence"), dedupeKey: "eventId" });
    const ids = [actors.tell(actor.id, "older").messageId, actors.tell(actor.id, "newer").messageId];
    await actors.close();
    const file = queueFile(dir, actor.id);
    const saved = json(file);
    saved.cleanHandover = true;
    saved.processedKeys = ["completed-fence"];
    saved.cancelledPredecessors = ["queue-0123456789abcdef.json"];
    saved.items.forEach((item: any, index: number) => {
      item.attempts = index + 1; item.preparationAttempts = index + 3;
      item.executionStarted = false; item.ownershipRefusals = index + 1;
    });
    fs.writeFileSync(file, JSON.stringify(saved));
    const restored = new ActorManager(identity.sessionId, identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 50 }, agents, () => {}, {
      actorRoot: path.join(dir, "project"), persistent: true, releasePaused: true,
      meshCursorPath: path.join(dir, "restored-cursor"), meshRetentionSweepPath: false, reapDeadSessionPresence: false,
    });
    closers.push(() => restored.close());
    // Constructor queued a restore microtask; cut before that microtask can coalesce/refill.
    restored.pauseForRelease({ strict: true });
    await flush();
    expect(restored.inFlightCount()).toBe(0);
    await restored.checkpointForRelease();
    const receipt = restored.releaseCheckpointReceipt() as any;
    expect(receipt.queues[0].items.map((item: any) => [item.id, item.attempts, item.preparationAttempts, item.ownershipRefusals])).toEqual([
      [ids[0], 1, 3, 1], [ids[1], 2, 4, 2],
    ]);
    expect(receipt.queues[0]).toMatchObject({ processedKeys: ["completed-fence"], cancelledPredecessors: ["queue-0123456789abcdef.json"] });
    expect(restored.messages(actor.id).filter(message => message.direction === "out")).toHaveLength(0);
    restored.resumeAfterRelease();
    await changed(restored, () => restored.inFlightCount() === 0 && restored.messages(actor.id).filter(message => message.direction === "out").length === 2);
    expect(restored.messages(actor.id).filter(message => message.direction === "out")).toHaveLength(2);
  });

  it("fences an asynchronous orphan adoption at its locked commit after the hold, keeping predecessor input intact", async () => {
    const { dir, mesh, actors, agents } = fixture();
    const actor = await actors.create(spec("orphan-await"));
    const id = actors.tell(actor.id, "original-owner-input").messageId;
    await actors.close();
    const predecessor = queueFile(dir, actor.id), before = fs.readFileSync(predecessor, "utf8");
    const gate = deferred(), reached = deferred(), finished = deferred();
    const actual = ActorRegistryStore.withLocks;
    let delay = true;
    vi.spyOn(ActorRegistryStore, "withLocks").mockImplementation((async (...args: any[]) => {
      if (delay && args[2] === "manager-adoption") {
        reached.resolve(); await gate.promise;
        try { return await actual.apply(ActorRegistryStore, args as any); }
        finally { finished.resolve(); }
      }
      return actual.apply(ActorRegistryStore, args as any);
    }) as typeof ActorRegistryStore.withLocks);
    const successorIdentity = { ...identity, id: "session:adopter", sessionId: "adopter" };
    const successor = new ActorManager("adopter", successorIdentity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 50 }, agents, () => {}, {
      actorRoot: path.join(dir, "project"), persistent: true, releasePaused: true, claimResidency: "durable",
      canManageActor: () => undefined, lineageAlive: () => false, adoptionGraceMs: 0,
      meshRetentionSweepPath: false, reapDeadSessionPresence: false,
    });
    closers.push(() => successor.close());
    expect(successor.owns(actor.id)).toBe(false);
    await reached.promise;
    successor.pauseForRelease({ strict: true });
    gate.resolve(); await finished.promise; await flush();
    expect(json(path.join(dir, "project", "actors.json")).actors.find((row: any) => row.id === actor.id).rootId).toBe(identity.id);
    expect(fs.readFileSync(predecessor, "utf8")).toBe(before);
    expect(json(predecessor).items[0].id).toBe(id);
    delay = false;
    successor.resumeAfterRelease();
    await changed(successor, () => successor.status(actor.id).rootId === successorIdentity.id);
    await changed(successor, () => successor.inFlightCount() === 0 && successor.messages(actor.id).some(message => message.direction === "out"));
    expect(successor.status(actor.id).id).toBe(actor.id);
  });

  it("lets a pre-cut actor preparation settle naturally without aborting it or starting its queued tail", async () => {
    const { dir, mesh, agents } = fixture();
    const gate = deferred<string>(), reached = deferred();
    let blocked = false;
    const prepared = new ActorManager(identity.sessionId, identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 50 }, agents, () => {}, {
      actorRoot: path.join(dir, "preparation"), persistent: true,
      resolvePiModel: model => { if (blocked) { reached.resolve(); return gate.promise; } return model; },
      meshRetentionSweepPath: false, reapDeadSessionPresence: false,
    });
    closers.push(() => prepared.close());
    const actor = await prepared.create({ ...spec("preparation"), model: "fixture/model" });
    blocked = true;
    prepared.tell(actor.id, "accepted-preparation");
    await reached.promise;
    const tail = prepared.tell(actor.id, "queued-tail").messageId;
    prepared.pauseForRelease({ strict: true });
    expect(prepared.inFlightCount()).toBe(1);
    blocked = false; gate.resolve("fixture/model");
    await changed(prepared, () => prepared.inFlightCount() === 0);
    expect(prepared.status(actor.id).queued).toBe(1);
    await prepared.checkpointForRelease();
    expect((prepared.releaseCheckpointReceipt() as any).queues[0].items.map((item: any) => item.id)).toEqual([tail]);
    prepared.resumeAfterRelease();
    await changed(prepared, () => prepared.inFlightCount() === 0);
    expect(prepared.messages(actor.id).filter(message => message.direction === "out")).toHaveLength(2);
  });

  it("never publishes a cursor after failed durable queue readback, including an empty completion fence", async () => {
    const { dir, actors } = fixture();
    const actor = await actors.create(spec("queue-readback-fault"));
    actors.tell(actor.id, "durable-input");
    actors.pauseForRelease({ strict: true });
    const file = queueFile(dir, actor.id);
    const actual = fs.readFileSync;
    vi.spyOn(fs, "readFileSync").mockImplementation(((...args: any[]) => args[0] === file
      ? JSON.stringify({ format: 1, cleanHandover: true, items: [] }) : actual.apply(fs, args as any)) as typeof fs.readFileSync);
    const cursor = vi.spyOn(ActorMeshMonitor.prototype, "checkpointForRelease");
    await expect(actors.checkpointForRelease()).rejects.toThrow(/queue release readback differs/);
    expect(cursor).not.toHaveBeenCalled();
    expect(actors.status(actor.id).queued).toBe(1);
  });

  it.each([false, true])("finishes exactly the accepted mesh prefix when the first callback arms hold (archive %s)", async archive => {
    const dir = root(), cursorPath = path.join(dir, "cursor.json");
    fs.writeFileSync(cursorPath, JSON.stringify({ format: 1, cursor: 0, last: { sequence: 0, id: "" } }));
    const events = [1, 2].map(sequence => ({ id: `prefix-${sequence}`, sequence, topic: "test.non-work", createdAt: Date.now() } as MeshEvent));
    let held = false;
    const delivered: string[] = [];
    const monitor = new ActorMeshMonitor({ root: dir, latestOffset: () => 20,
      tail: offset => ({ events: archive ? [] : events.slice(offset >= 10 ? 1 : 0), nextOffset: 20, cursors: offset >= 10 ? [20] : [10, 20] }),
      read: options => events.filter(event => event.sequence > (options?.after ?? 0)), oldestSequence: () => archive ? 3 : 1,
    }, { enabled: true, actorPollMs: 50, maxReadEvents: 100 }, {
      cursorPath, replayAll: true, beforePoll: () => !held, canAdmit: () => !held,
      onEvent: event => { delivered.push(event.id); if (event.sequence === 1) held = true; return true; },
    });
    closers.push(async () => monitor.close());
    monitor.start(); await flush(); monitor.checkpointForRelease();
    expect(delivered).toEqual(["prefix-1"]);
    expect(json(cursorPath).last).toEqual({ sequence: 1, id: "prefix-1" });
    held = false; monitor.schedule(); await flush();
    expect(delivered).toEqual(["prefix-1", "prefix-2"]);
    expect(json(cursorPath).last.id).toBe("prefix-2");
  });

  it.each([false, true])("holds original live/archive event IDs behind the cursor when a due-schedule await crosses the cut (archive %s)", async archive => {
    const dir = root(), cursorPath = path.join(dir, "cursor.json");
    fs.writeFileSync(cursorPath, JSON.stringify({ format: 1, cursor: 0, last: { sequence: 0, id: "" } }));
    const gate = deferred(), reached = deferred();
    let due: number | undefined = Date.now() - 1, held = false;
    const events = [1, 2].map(sequence => ({ id: `original-${sequence}`, sequence, topic: "fleet.work.drain", createdAt: Date.now() } as MeshEvent));
    const delivered: string[] = [];
    const monitor = new ActorMeshMonitor({ root: dir, latestOffset: () => 20,
      tail: () => ({ events: archive ? [] : events, nextOffset: 20, cursors: [10, 20] }),
      read: () => events, oldestSequence: () => archive ? 3 : 1,
      nextScheduleDueAt: () => due,
      releaseDueSchedules: async () => { reached.resolve(); await gate.promise; due = undefined; return []; },
    }, { enabled: true, actorPollMs: 50, maxReadEvents: 100 }, {
      cursorPath, beforePoll: () => !held, canAdmit: () => !held,
      onEvent: event => { delivered.push(event.id); return true; },
    });
    closers.push(async () => monitor.close());
    monitor.start();
    await reached.promise;
    held = true; gate.resolve(); await flush();
    monitor.checkpointForRelease();
    expect(delivered).toEqual([]);
    expect(json(cursorPath)).toEqual({ format: 1, cursor: 0, last: { sequence: 0, id: "" } });
    held = false; monitor.schedule(); await flush();
    expect(delivered).toEqual(events.map(event => event.id));
    expect(json(cursorPath).last.id).toBe("original-2");
  });
});
