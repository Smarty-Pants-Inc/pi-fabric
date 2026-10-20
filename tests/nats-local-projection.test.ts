import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { hostLeasePath, readHostLease } from "../src/topology/host-leases.js";
import * as hostLeaseLock from "../src/topology/host-lease-lock.js";
import * as participantFiles from "../src/topology/participant-files.js";
import { readParticipantFile } from "../src/topology/participant-files.js";
import { NatsLocalProjection } from "../src/topology/nats-local-projection.js";
import type { FabricKvHostLease, FabricParticipantRecord } from "../src/topology/types.js";
import type { FabricTransport, TransportBucket, TransportKv, TransportKvEntry, TransportKvEvent } from "../src/transport/types.js";

const MACHINE = "machine-a";
const ROOT = "session:projection-main";
const INCARNATION = "projection-incarnation-a";
const owner = `${ROOT}#${INCARNATION}`;
const hash = (value: string) => createHash("sha256").update(value).digest("hex");
const participantKvKey = (id: string) => `${MACHINE}.p.${hash(id).slice(0, 32)}`;
const leaseKvKey = (id: string) => `${MACHINE}.h.${hash(id).slice(0, 32)}`;
const localParticipantKey = (id: string) => `topology/participants/${hash(id)}`;
const roots: string[] = [];
const projections: NatsLocalProjection[] = [];

type KvValue = FabricParticipantRecord | FabricKvHostLease;
type Event = TransportKvEvent<unknown>;
type Watcher = { prefix: string; queue: Event[]; wake?: (() => void) | undefined; paused: boolean };

class MemoryKv implements TransportKv {
  readonly ttlMs: number;
  readonly values = new Map<string, TransportKvEntry<unknown>>();
  readonly watchers: Watcher[] = [];
  #revision = 0;
  #watchGenerations = 0;
  constructor(ttlMs: number, readonly machine: string) { this.ttlMs = ttlMs; }
  get revision(): number { return this.#revision; }
  get watchGeneration(): number { return this.#watchGenerations; }
  seed(key: string, owner: string, value: unknown): TransportKvEntry<unknown> {
    const entry = { key, machine: this.machine, owner, value, revision: ++this.#revision };
    this.values.set(key, entry);
    return entry;
  }
  async get<V>(key: string): Promise<TransportKvEntry<V> | undefined> {
    return this.values.get(key) as TransportKvEntry<V> | undefined;
  }
  async put<V>(key: string, owner: string, value: V, ifRevision: number): Promise<number> {
    const current = this.values.get(key);
    if ((ifRevision === 0 && current) || (ifRevision > 0 && current?.revision !== ifRevision)) throw new Error("KV conflict");
    const entry = { key, machine: this.machine, owner, value, revision: ++this.#revision };
    this.values.set(key, entry);
    this.#emit({ type: "put", entry });
    return entry.revision;
  }
  async delete(key: string, ifRevision: number): Promise<boolean> {
    const current = this.values.get(key);
    if (!current || current.revision !== ifRevision) return false;
    this.values.delete(key);
    this.#emit({ type: "gone", key, machine: this.machine, revision: ++this.#revision, reason: "deleted" });
    return true;
  }
  async *watch<V>(prefix: string, signal: AbortSignal): AsyncIterable<TransportKvEvent<V>> {
    this.#watchGenerations++;
    const watcher: Watcher = { prefix: prefix.slice(0, -1), queue: [], paused: false };
    this.watchers.push(watcher);
    const snapshot = function* (self: MemoryKv): Generator<Event> {
      for (const entry of self.values.values()) if (entry.key.startsWith(watcher.prefix)) yield { type: "put", entry };
      yield { type: "ready" };
    };
    try {
      yield* snapshot(this) as Generator<TransportKvEvent<V>>;
      while (!signal.aborted) {
        const event = await this.#next(watcher, signal);
        if (!event) return;
        if (event.type === "reset") {
          yield event as TransportKvEvent<V>;
          yield* snapshot(this) as Generator<TransportKvEvent<V>>;
        } else if (event.type === "ready" || event.type === "put" && event.entry.key.startsWith(watcher.prefix) ||
          event.type === "gone" && event.key.startsWith(watcher.prefix)) yield event as TransportKvEvent<V>;
      }
    } finally {
      const index = this.watchers.indexOf(watcher);
      if (index >= 0) this.watchers.splice(index, 1);
    }
  }
  pause(index: number, paused = true): void { if (this.watchers[index]) this.watchers[index]!.paused = paused; }
  deliverTo(index: number, event: Event): void {
    const watcher = this.watchers[index];
    if (!watcher) throw new Error(`missing watcher ${index}`);
    watcher.queue.push(event);
    watcher.wake?.();
  }
  #emit(event: Event): void {
    for (const watcher of this.watchers) {
      if (!watcher.paused && (event.type === "reset" || event.type === "put" && event.entry.key.startsWith(watcher.prefix) ||
        event.type === "gone" && event.key.startsWith(watcher.prefix))) {
        watcher.queue.push(event);
        watcher.wake?.();
      }
    }
  }
  #next(watcher: Watcher, signal: AbortSignal): Promise<Event | undefined> {
    const queued = watcher.queue.shift();
    if (queued) return Promise.resolve(queued);
    return new Promise(resolve => {
      const finish = (event?: Event): void => {
        signal.removeEventListener("abort", abort);
        watcher.wake = undefined;
        resolve(event);
      };
      const abort = (): void => finish();
      watcher.wake = () => finish(watcher.queue.shift());
      signal.addEventListener("abort", abort, { once: true });
    });
  }
}

function participant(id = ROOT, incarnation = INCARNATION, name = "Main", updatedAt = Date.now()): FabricParticipantRecord {
  return { format: 1, id, rootId: id, kind: "root", ownerHostId: id, ownerIdentityId: id,
    ownerIncarnation: incarnation, name, status: "idle", runner: "pi", transport: "host",
    capabilities: ["followUp"], controlProtocol: "v1", startedAt: updatedAt - 1, updatedAt };
}
function lease(id = ROOT, incarnation = INCARNATION, updatedAt = Date.now()): FabricKvHostLease {
  return { id, rootId: id, identityId: id, incarnation, pid: process.pid, pidStartTime: "1",
    bootId: "12345678-1234-4234-8234-123456789abc", updatedAt, expiresAt: updatedAt + 15_000 };
}
function fixture(options: { id?: string; record?: FabricParticipantRecord; lease?: FabricKvHostLease; seed?: boolean; now?: () => number; transportHostId?: string; leaseLive?: () => boolean } = {}) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "nats-local-projection-"));
  roots.push(root);
  const mesh = new MeshStore(path.join(root, "mesh"), 65_536, 1_000);
  const id = options.id ?? ROOT;
  const record = options.record ?? participant(id);
  const hostLease = options.lease ?? lease(id, record.ownerIncarnation ?? INCARNATION, record.updatedAt);
  const presence = new MemoryKv(600_000, MACHINE);
  const leases = new MemoryKv(15_000, MACHINE);
  let presenceEntry: TransportKvEntry<unknown> | undefined;
  let leaseEntry: TransportKvEntry<unknown> | undefined;
  if (options.seed !== false) {
    const incarnation = record.ownerIncarnation ?? INCARNATION;
    presenceEntry = presence.seed(participantKvKey(id), `${record.ownerHostId}#${incarnation}`, record);
    leaseEntry = leases.seed(leaseKvKey(id), `${id}#${hostLease.incarnation}`, hostLease);
  }
  const transport: FabricTransport = {
    kind: "nats", self: { machine: MACHINE, hostId: options.transportHostId ?? "projection-adapter" },
    async send() { throw new Error("unused"); }, async *inbox() { return; },
    kv(bucket: TransportBucket) { return bucket === "presence" ? presence : leases; },
    async drainState() { return { pending: 0, ackPending: 0 }; }, async close() {},
  };
  const projection = new NatsLocalProjection({ mesh, transport, leaseLive: options.leaseLive ?? (() => true),
    ...(options.now ? { now: options.now, monotonic: options.now } : {}) });
  projections.push(projection);
  return { root, mesh, id, record, hostLease, presence, leases, transport, projection, presenceEntry, leaseEntry };
}

async function start(f: ReturnType<typeof fixture>): Promise<void> {
  f.projection.start();
  await f.projection.whenReady();
}
async function waitUntil(check: () => void): Promise<void> { await vi.waitFor(check, { timeout: 2_000, interval: 5 }); }
function projectedParticipantRevision(meshRoot: string, id: string): number | undefined {
  const projected = readParticipantFile(meshRoot, localParticipantKey(id)) as
    { natsProjection?: { revision?: unknown } } | undefined;
  return typeof projected?.natsProjection?.revision === "number" ? projected.natsProjection.revision : undefined;
}
function projectedHostLeaseRevision(meshRoot: string, id: string): number | undefined {
  try {
    const projected = JSON.parse(fs.readFileSync(hostLeasePath(meshRoot, id), "utf8")) as
      { natsProjection?: { revision?: unknown } };
    return typeof projected.natsProjection?.revision === "number" ? projected.natsProjection.revision : undefined;
  } catch { return undefined; }
}

afterEach(async () => {
  await Promise.all(projections.splice(0).map(projection => projection.close()));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  vi.restoreAllMocks();
});

describe("NATS local presence projection", () => {
  it("projects the Path B machine snapshot into participant and host-lease files", async () => {
    const f = fixture();
    await start(f);
    expect(readParticipantFile(f.mesh.root, localParticipantKey(f.id))).toMatchObject({
      key: localParticipantKey(f.id), version: f.presenceEntry!.revision,
      value: { id: f.id, ownerIncarnation: INCARNATION, name: "Main" },
      natsProjection: { machine: MACHINE, kvKey: participantKvKey(f.id), revision: f.presenceEntry!.revision },
    });
    expect(readHostLease(f.mesh.root, f.id)).toMatchObject({
      id: f.id, rootId: f.id, identityId: f.id, incarnationToken: INCARNATION,
    });
    expect(f.projection.metrics()).toMatchObject({ machine: MACHINE, participantWrites: 1, hostLeaseWrites: 1 });
  });

  it("reprojects a current snapshot after local participant and lease files disappear", async () => {
    const f = fixture();
    await start(f);
    fs.rmSync(path.join(f.mesh.root, "participants", `${hash(f.id)}.json`), { force: true });
    fs.rmSync(hostLeasePath(f.mesh.root, f.id), { force: true });
    f.presence.deliverTo(0, { type: "reset" });
    f.leases.deliverTo(0, { type: "reset" });
    await waitUntil(() => {
      expect(readParticipantFile(f.mesh.root, localParticipantKey(f.id))).toMatchObject({ value: { id: f.id } });
      expect(readHostLease(f.mesh.root, f.id)).toMatchObject({ incarnationToken: INCARNATION });
    });
  });

  it("resnapshots and continues after a participant file-writer failure", async () => {
    const f = fixture();
    await start(f);
    const initialGeneration = f.presence.watchGeneration;
    const write = participantFiles.writeParticipantFileIf;
    let failed = false;
    vi.spyOn(participantFiles, "writeParticipantFileIf").mockImplementation(async (...args) => {
      if (!failed) { failed = true; throw new Error("injected participant writer failure"); }
      return write(...args);
    });

    const first = participant(f.id, INCARNATION, "Recovered participant", f.record.updatedAt + 1);
    let revision = await f.presence.put(participantKvKey(f.id), owner, first, f.presenceEntry!.revision);
    await waitUntil(() => expect(f.presence.watchGeneration).toBeGreaterThan(initialGeneration));
    await waitUntil(() => expect(projectedParticipantRevision(f.mesh.root, f.id)).toBe(revision));
    expect(failed).toBe(true);

    const later = participant(f.id, INCARNATION, "Later participant renewal", first.updatedAt + 1);
    revision = await f.presence.put(participantKvKey(f.id), owner, later, revision);
    await waitUntil(() => expect(projectedParticipantRevision(f.mesh.root, f.id)).toBe(revision));
    expect(await f.presence.delete(participantKvKey(f.id), revision)).toBe(true);
    await waitUntil(() => expect(readParticipantFile(f.mesh.root, localParticipantKey(f.id))).toBeUndefined());
  });

  it("resnapshots and continues after a host lease lock failure", async () => {
    const f = fixture();
    await start(f);
    const initialGeneration = f.leases.watchGeneration;
    const lock = hostLeaseLock.withHostLeaseLock;
    let failed = false;
    vi.spyOn(hostLeaseLock, "withHostLeaseLock").mockImplementation(async (...args) => {
      if (!failed) { failed = true; throw new Error("injected host lease lock failure"); }
      return lock(...args);
    });
    // A missing local lease bypasses the stat-only fast path and reaches the injected lock.
    fs.rmSync(hostLeasePath(f.mesh.root, f.id), { force: true });

    const first = lease(f.id, INCARNATION, f.hostLease.updatedAt + 1);
    let revision = await f.leases.put(leaseKvKey(f.id), `${f.id}#${INCARNATION}`, first, f.leaseEntry!.revision);
    await waitUntil(() => expect(f.leases.watchGeneration).toBeGreaterThan(initialGeneration));
    await waitUntil(() => expect(projectedHostLeaseRevision(f.mesh.root, f.id)).toBe(revision));
    await waitUntil(() => expect(f.projection.metrics().hostLeaseWrites).toBe(2));
    expect(failed).toBe(true);

    // Force the later renewal through the locked writer instead of the stat-only cache path.
    fs.rmSync(hostLeasePath(f.mesh.root, f.id), { force: true });
    const later = lease(f.id, INCARNATION, first.updatedAt + 1);
    revision = await f.leases.put(leaseKvKey(f.id), `${f.id}#${INCARNATION}`, later, revision);
    await waitUntil(() => expect(projectedHostLeaseRevision(f.mesh.root, f.id)).toBe(revision));
    expect(await f.leases.delete(leaseKvKey(f.id), revision)).toBe(true);
    await waitUntil(() => expect(readHostLease(f.mesh.root, f.id)).toBeUndefined());
  });

  it("reports steady lease-refresh writes and local reconciliation reads per minute", async () => {
    let clock = 1_000_000;
    const f = fixture({ record: participant(ROOT, INCARNATION, "Main", clock),
      lease: lease(ROOT, INCARNATION, clock), now: () => clock });
    await start(f);
    let pRevision = f.presenceEntry!.revision;
    let hRevision = f.leaseEntry!.revision;
    let changed = participant(f.id, INCARNATION, "Busy Main", clock + 30_000);
    for (let step = 1; step <= 12; step++) {
      clock = 1_000_000 + step * 5_000;
      hRevision = await f.leases.put(leaseKvKey(f.id), `${f.id}#${INCARNATION}`, lease(f.id, INCARNATION, clock), hRevision);
      if (step === 3) pRevision = await f.presence.put(participantKvKey(f.id), owner, f.record, pRevision);
      if (step === 6) pRevision = await f.presence.put(participantKvKey(f.id), owner, changed, pRevision);
      if (step === 12) pRevision = await f.presence.put(participantKvKey(f.id), owner, changed, pRevision);
      if (step % 2 === 0) await waitUntil(() => expect(f.projection.metrics().hostLeaseWrites).toBe(1 + step));
    }
    await waitUntil(() => expect(f.projection.metrics().participantReconcileReads).toBe(5));
    expect(f.projection.metrics()).toMatchObject({
      elapsedMs: 60_000, participantWrites: 4, hostLeaseWrites: 13,
      participantReconcileReads: 5, hostLeaseReconcileReads: 14, kvReconcileReads: 0,
      participantWritesPerMinute: 4, hostLeaseWritesPerMinute: 13,
      participantReconcileReadsPerMinute: 5, hostLeaseReconcileReadsPerMinute: 14,
      kvReconcileReadsPerMinute: 0,
    });
  });

  it("reports participant and host-lease mutation rates during churn", async () => {
    let clock = 2_000_000;
    const f = fixture({ record: participant(ROOT, INCARNATION, "Main", clock),
      lease: lease(ROOT, INCARNATION, clock), now: () => clock });
    await start(f);
    clock += 60_000;
    const additions: Array<{ id: string; pRevision: number; hRevision: number }> = [];
    for (let index = 0; index < 3; index++) {
      const id = `session:churn-${index}`;
      const record = participant(id, `churn-incarnation-${index}`, `Churn ${index}`, clock + index);
      const hostLease = lease(id, record.ownerIncarnation!, record.updatedAt);
      const pRevision = await f.presence.put(participantKvKey(id), `${id}#${record.ownerIncarnation}`, record, 0);
      const hRevision = await f.leases.put(leaseKvKey(id), `${id}#${record.ownerIncarnation}`, hostLease, 0);
      additions.push({ id, pRevision, hRevision });
    }
    await waitUntil(() => expect(f.projection.metrics()).toMatchObject({ participantWrites: 4, hostLeaseWrites: 4 }));
    const removed = additions[0]!;
    await f.presence.delete(participantKvKey(removed.id), removed.pRevision);
    await f.leases.delete(leaseKvKey(removed.id), removed.hRevision);
    await waitUntil(() => expect(f.projection.metrics()).toMatchObject({ participantWrites: 5, hostLeaseWrites: 5 }));
    expect(f.projection.metrics()).toMatchObject({
      elapsedMs: 60_000, participantWritesPerMinute: 5, hostLeaseWritesPerMinute: 5,
      participantReconcileReadsPerMinute: 6, hostLeaseReconcileReadsPerMinute: 7,
      kvReconcileReadsPerMinute: 2,
    });
  });

  it("propagates KV removals and reconciles a deletion missed during watch disconnection", async () => {
    const f = fixture();
    await start(f);
    const pRevision = f.presenceEntry!.revision;
    const hRevision = f.leaseEntry!.revision;
    expect(await f.presence.delete(participantKvKey(f.id), pRevision)).toBe(true);
    expect(await f.leases.delete(leaseKvKey(f.id), hRevision)).toBe(true);
    await waitUntil(() => {
      expect(readParticipantFile(f.mesh.root, localParticipantKey(f.id))).toBeUndefined();
      expect(readHostLease(f.mesh.root, f.id)).toBeUndefined();
    });

    const again = fixture({ id: "session:projection-reconnect" });
    await start(again);
    const key = participantKvKey(again.id);
    again.presence.pause(0);
    expect(await again.presence.delete(key, again.presenceEntry!.revision)).toBe(true);
    again.presence.pause(0, false);
    again.presence.deliverTo(0, { type: "reset" });
    await waitUntil(() => expect(readParticipantFile(again.mesh.root, localParticipantKey(again.id))).toBeUndefined());
  });

  it("does not replay a cached participant after an outage tombstone in the watch stream", async () => {
    let leaseLive = false;
    const f = fixture({ leaseLive: () => leaseLive });
    const key = participantKvKey(f.id);
    const writer = vi.spyOn(participantFiles, "writeParticipantFileIf");
    const observerAbort = new AbortController();
    const observer = f.presence.watch<unknown>(">", observerAbort.signal)[Symbol.asyncIterator]();
    try {
      await start(f); // The snapshot is cached while the owner lease is suspended.
      expect(readParticipantFile(f.mesh.root, localParticipantKey(f.id))).toBeUndefined();
      expect((await observer.next()).value).toMatchObject({ type: "put", entry: { key, revision: f.presenceEntry!.revision } });
      expect((await observer.next()).value).toEqual({ type: "ready" });

      f.presence.pause(0); // The projection misses the tombstone as its watch is disconnected.
      expect(await f.presence.delete(key, f.presenceEntry!.revision)).toBe(true);
      expect((await observer.next()).value).toMatchObject({ type: "gone", key });
      leaseLive = true; // Reconnect/reacquire starts the deferred projection replay.
      await f.projection.resumeAfterLeaseRecovery();
      expect(writer).not.toHaveBeenCalled();
      expect(readParticipantFile(f.mesh.root, localParticipantKey(f.id))).toBeUndefined();

      // The source watch has no follow-up PUT, and the local projection never materializes the stale row.
      expect(f.presence.watchers[1]?.queue).toEqual([]);
      expect(writer).not.toHaveBeenCalled();
      expect(readParticipantFile(f.mesh.root, localParticipantKey(f.id))).toBeUndefined();
    } finally {
      observerAbort.abort();
      await observer.return?.();
    }
  });

  it("replays the current participant revision after an outage update", async () => {
    let leaseLive = false;
    const f = fixture({ leaseLive: () => leaseLive });
    const key = participantKvKey(f.id);
    const observerAbort = new AbortController();
    const observer = f.presence.watch<unknown>(">", observerAbort.signal)[Symbol.asyncIterator]();
    try {
      await start(f);
      expect((await observer.next()).value).toMatchObject({ type: "put", entry: { key, revision: f.presenceEntry!.revision } });
      expect((await observer.next()).value).toEqual({ type: "ready" });
      f.presence.pause(0);
      const changed = participant(f.id, INCARNATION, "Recovered Main", f.record.updatedAt + 1);
      const revision = await f.presence.put(key, owner, changed, f.presenceEntry!.revision);
      expect((await observer.next()).value).toMatchObject({ type: "put", entry: { key, revision, value: changed } });

      leaseLive = true;
      await f.projection.resumeAfterLeaseRecovery();
      expect(readParticipantFile(f.mesh.root, localParticipantKey(f.id))).toMatchObject({
        value: changed, natsProjection: { revision },
      });
      expect(f.projection.metrics().participantWrites).toBe(1);
    } finally {
      observerAbort.abort();
      await observer.return?.();
    }
  });

  it("retains recreated participant and lease rows when the other projection receives an older tombstone", async () => {
    const initialLease = { ...lease(ROOT, INCARNATION), expiresAt: Date.now() + 120_000 };
    const f = fixture({ lease: initialLease });
    await start(f);
    const other = new NatsLocalProjection({ mesh: f.mesh, transport: f.transport, leaseLive: () => true });
    projections.push(other);
    other.start();
    await other.whenReady();
    expect(f.presence.watchers).toHaveLength(2);
    expect(f.leases.watchers).toHaveLength(2);
    for (const index of [0, 1]) { f.presence.pause(index); f.leases.pause(index); }

    const pKey = participantKvKey(f.id), hKey = leaseKvKey(f.id);
    expect(await f.presence.delete(pKey, f.presenceEntry!.revision)).toBe(true);
    const oldParticipantTombstone: Event = {
      type: "gone", key: pKey, machine: MACHINE, revision: f.presence.revision, reason: "deleted",
    };
    expect(await f.leases.delete(hKey, f.leaseEntry!.revision)).toBe(true);
    const oldLeaseTombstone: Event = {
      type: "gone", key: hKey, machine: MACHINE, revision: f.leases.revision, reason: "deleted",
    };

    const participantRevision = await f.presence.put(pKey, owner, f.record, 0);
    const currentParticipant = await f.presence.get(pKey);
    if (!currentParticipant) throw new Error("recreated participant row missing from MemoryKv");
    expect(currentParticipant.value).toEqual(f.record);
    const leaseRevision = await f.leases.put(hKey, `${f.id}#${INCARNATION}`, f.hostLease, 0);
    const currentLease = await f.leases.get(hKey);
    if (!currentLease) throw new Error("recreated lease row missing from MemoryKv");
    expect(currentLease.value).toEqual(f.hostLease);

    const projected = f.projection.metrics();
    f.presence.deliverTo(0, { type: "put", entry: currentParticipant });
    f.leases.deliverTo(0, { type: "put", entry: currentLease });
    await waitUntil(() => {
      const metrics = f.projection.metrics();
      expect(metrics.participantReconcileReads).toBeGreaterThan(projected.participantReconcileReads);
      expect(metrics.hostLeaseReconcileReads).toBeGreaterThan(projected.hostLeaseReconcileReads);
      expect(projectedParticipantRevision(f.mesh.root, f.id)).toBe(participantRevision);
      expect(projectedHostLeaseRevision(f.mesh.root, f.id)).toBe(leaseRevision);
    });

    const checked = other.metrics().kvReconcileReads;
    f.presence.deliverTo(1, oldParticipantTombstone);
    f.leases.deliverTo(1, oldLeaseTombstone);
    await waitUntil(() => expect(other.metrics().kvReconcileReads).toBeGreaterThanOrEqual(checked + 2));
    expect(readParticipantFile(f.mesh.root, localParticipantKey(f.id))).toMatchObject({ value: f.record });
    expect(readHostLease(f.mesh.root, f.id)).toMatchObject({
      id: f.id, rootId: f.id, identityId: f.id, incarnationToken: INCARNATION,
    });

    expect(await f.presence.delete(pKey, participantRevision)).toBe(true);
    const currentParticipantTombstone: Event = {
      type: "gone", key: pKey, machine: MACHINE, revision: f.presence.revision, reason: "deleted",
    };
    expect(await f.leases.delete(hKey, leaseRevision)).toBe(true);
    const currentLeaseTombstone: Event = {
      type: "gone", key: hKey, machine: MACHINE, revision: f.leases.revision, reason: "deleted",
    };
    f.presence.deliverTo(0, currentParticipantTombstone);
    f.leases.deliverTo(0, currentLeaseTombstone);
    await waitUntil(() => {
      expect(readParticipantFile(f.mesh.root, localParticipantKey(f.id))).toBeUndefined();
      expect(readHostLease(f.mesh.root, f.id)).toBeUndefined();
    });
  });

  it("fences delayed removals when a recreated row is reprojected after the KV absence read", async () => {
    const f = fixture();
    await start(f);
    const pKey = participantKvKey(f.id), hKey = leaseKvKey(f.id);
    const localKey = localParticipantKey(f.id), hostFile = hostLeasePath(f.mesh.root, f.id);
    const gated = new Set<string>();
    const release = deferred<void>();
    const markGated = (kind: string): void => { gated.add(kind); };

    const removeParticipant = participantFiles.removeParticipantFileIf;
    vi.spyOn(participantFiles, "removeParticipantFileIf").mockImplementation(async (...args) => {
      if (args[1] === localKey && !gated.has("participant")) {
        markGated("participant");
        await release.promise;
      }
      return removeParticipant(...args);
    });
    const withLeaseLock = hostLeaseLock.withHostLeaseLock;
    vi.spyOn(hostLeaseLock, "withHostLeaseLock").mockImplementation(async (mesh, file, operation, options) => {
      if (file === hostFile && !gated.has("lease")) {
        markGated("lease");
        await release.promise;
      }
      return withLeaseLock(mesh, file, operation, options);
    });

    try {
      expect(await f.presence.delete(pKey, f.presenceEntry!.revision)).toBe(true);
      expect(await f.leases.delete(hKey, f.leaseEntry!.revision)).toBe(true);
      await waitUntil(() => expect(gated.size).toBe(2));
      expect(f.projection.metrics().kvReconcileReads).toBe(2);

      // Each old tombstone has already read the key as absent, but its local removal
      // is paused. A separate watch snapshot now sees the recreated KV rows and
      // advances their persisted markers before those stale removals resume.
      const participantRevision = await f.presence.put(pKey, owner, f.record, 0);
      const leaseRevision = await f.leases.put(hKey, owner, f.hostLease, 0);
      const successor = new NatsLocalProjection({ mesh: f.mesh, transport: f.transport, leaseLive: () => true });
      projections.push(successor);
      successor.start();
      await successor.whenReady();
      await waitUntil(() => {
        expect(projectedParticipantRevision(f.mesh.root, f.id)).toBe(participantRevision);
        expect(projectedHostLeaseRevision(f.mesh.root, f.id)).toBe(leaseRevision);
      });

      release.resolve();
      await waitUntil(() => {
        expect(readParticipantFile(f.mesh.root, localKey)).toMatchObject({
          value: { id: f.id }, natsProjection: { revision: participantRevision },
        });
        expect(readHostLease(f.mesh.root, f.id)).toMatchObject({ id: f.id });
        expect(projectedHostLeaseRevision(f.mesh.root, f.id)).toBe(leaseRevision);
      });

      // A true current tombstone still removes both projections.
      vi.restoreAllMocks();
      expect(await f.presence.delete(pKey, participantRevision)).toBe(true);
      expect(await f.leases.delete(hKey, leaseRevision)).toBe(true);
      await waitUntil(() => {
        expect(readParticipantFile(f.mesh.root, localKey)).toBeUndefined();
        expect(readHostLease(f.mesh.root, f.id)).toBeUndefined();
      });
    } finally {
      release.resolve();
    }
  });

  it("refuses a stale-incarnation writer that resumes after a newer projection", async () => {
    const f = fixture();
    const delayed = deferred<void>();
    const entered = deferred<void>();
    const write = participantFiles.writeParticipantFileIf;
    let holdFirst = true;
    vi.spyOn(participantFiles, "writeParticipantFileIf").mockImplementation(async (...args) => {
      if (holdFirst) { holdFirst = false; entered.resolve(); await delayed.promise; }
      return write(...args);
    });
    f.projection.start();
    await entered.promise;

    const nextIncarnation = "projection-incarnation-b";
    const nextRecord = participant(f.id, nextIncarnation, "Successor", f.record.updatedAt - 1);
    expect(nextRecord.updatedAt).toBeLessThan(f.record.updatedAt);
    const nextLease = lease(f.id, nextIncarnation, nextRecord.updatedAt);
    const nextPresenceRevision = await f.presence.put(participantKvKey(f.id), `${f.id}#${nextIncarnation}`, nextRecord, f.presenceEntry!.revision);
    await f.leases.put(leaseKvKey(f.id), `${f.id}#${nextIncarnation}`, nextLease, f.leaseEntry!.revision);

    // A fresh adapter projects the successor while the older adapter is still inside its
    // initial file-writer call, reproducing the stale-owner race across host processes.
    const sharedProjection = new NatsLocalProjection({ mesh: f.mesh, transport: f.transport, leaseLive: () => true });
    projections.push(sharedProjection);
    sharedProjection.start();
    await sharedProjection.whenReady();
    await waitUntil(() => expect(readParticipantFile(f.mesh.root, localParticipantKey(f.id))).toMatchObject({
      value: { ownerIncarnation: nextIncarnation, name: "Successor" },
    }));
    delayed.resolve();
    await f.projection.whenReady();
    await new Promise(resolve => setTimeout(resolve, 20));
    expect(readParticipantFile(f.mesh.root, localParticipantKey(f.id))).toMatchObject({
      value: { ownerIncarnation: nextIncarnation, name: "Successor" },
      natsProjection: { revision: nextPresenceRevision },
    });
  });

  it("settles every removal in bounded unique-key participant and lease churn", async () => {
    const f = fixture();
    await start(f);
    const rows: Array<{ id: string; participantRevision: number; leaseRevision: number }> = [];
    for (let index = 0; index < 8; index++) {
      const id = `session:projection-churn-${index}`;
      const record = participant(id, `churn-incarnation-${index}`, `Churn ${index}`, f.record.updatedAt + index + 1);
      const hostLease = lease(id, record.ownerIncarnation!, record.updatedAt);
      const participantRevision = await f.presence.put(participantKvKey(id), `${id}#${record.ownerIncarnation}`, record, 0);
      const leaseRevision = await f.leases.put(leaseKvKey(id), `${id}#${record.ownerIncarnation}`, hostLease, 0);
      rows.push({ id, participantRevision, leaseRevision });
    }
    await waitUntil(() => expect(f.projection.metrics()).toMatchObject({
      participantWrites: rows.length + 1, hostLeaseWrites: rows.length + 1,
    }));
    for (const row of rows) {
      expect(await f.presence.delete(participantKvKey(row.id), row.participantRevision)).toBe(true);
      expect(await f.leases.delete(leaseKvKey(row.id), row.leaseRevision)).toBe(true);
    }
    await waitUntil(() => {
      for (const row of rows) {
        expect(readParticipantFile(f.mesh.root, localParticipantKey(row.id))).toBeUndefined();
        expect(readHostLease(f.mesh.root, row.id)).toBeUndefined();
      }
    });
  });

  it("fences a delayed stale-incarnation host-lease writer while allowing the successor", async () => {
    const f = fixture();
    const delayed = deferred<void>();
    const entered = deferred<void>();
    const lock = hostLeaseLock.withHostLeaseLock;
    let holdFirst = true;
    vi.spyOn(hostLeaseLock, "withHostLeaseLock").mockImplementation(async (...args) => {
      if (holdFirst) { holdFirst = false; entered.resolve(); await delayed.promise; }
      return lock(...args);
    });
    f.projection.start();
    await entered.promise;

    const nextIncarnation = "projection-incarnation-b";
    const nextLease = lease(f.id, nextIncarnation, f.hostLease.updatedAt - 1);
    expect(nextLease.updatedAt).toBeLessThan(f.hostLease.updatedAt);
    const nextRevision = await f.leases.put(leaseKvKey(f.id), `${f.id}#${nextIncarnation}`, nextLease, f.leaseEntry!.revision);
    const successor = new NatsLocalProjection({ mesh: f.mesh, transport: f.transport, leaseLive: () => true });
    projections.push(successor);
    successor.start();
    await successor.whenReady();
    delayed.resolve();
    await f.projection.whenReady();
    await waitUntil(() => expect(readHostLease(f.mesh.root, f.id)).toMatchObject({ incarnationToken: nextIncarnation }));
    expect(JSON.parse(fs.readFileSync(hostLeasePath(f.mesh.root, f.id), "utf8"))).toMatchObject({
      natsProjection: { revision: nextRevision, incarnation: nextIncarnation },
    });
    expect(f.projection.metrics().hostLeaseWrites).toBe(0);
  });

  // TransportHostLease's first-use process proof is Linux /proc-only.
  it.skipIf(process.platform !== "linux")("loads and starts the projection on the NATS directory's first-use path", async () => {
    const id = "session:nats-directory-first-use";
    const f = fixture({ id, seed: false, transportHostId: id });
    const transportStateDir = path.join(f.root, "private-transport-state");
    fs.mkdirSync(transportStateDir, { recursive: true, mode: 0o700 });
    const directory = new ParticipantDirectory(f.mesh, {
      enabled: true, hostId: id, rootId: id, identity: { id, name: "NATS Main", kind: "main" },
      transport: f.transport, transportStateDir,
      heartbeatMs: 60_000, leaseMs: 120_000,
    });
    directory.registerSource(() => [participant(id, directory.ownerIncarnation, "NATS Main")]);
    try {
      await directory.start();
      await waitUntil(() => {
        expect(readParticipantFile(f.mesh.root, localParticipantKey(id))).toMatchObject({
          value: { id, ownerIncarnation: directory.ownerIncarnation, name: "NATS Main" },
        });
        expect(readHostLease(f.mesh.root, id)).toMatchObject({ incarnationToken: directory.ownerIncarnation });
      });
    } finally { await directory.close(); }
    expect(readParticipantFile(f.mesh.root, localParticipantKey(id))).toBeUndefined();
    expect(readHostLease(f.mesh.root, id)).toBeUndefined();
  });

  it("does not construct or expose a projection for FILE transport", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "nats-local-projection-file-"));
    roots.push(root);
    const id = "session:file-transport";
    const identity = { id, name: "File Main", kind: "main" as const };
    const mesh = new MeshStore(path.join(root, "mesh"), 65_536, 1_000);
    const directory = new ParticipantDirectory(mesh, { enabled: true, hostId: id, rootId: id, identity,
      heartbeatMs: 60_000, leaseMs: 120_000 });
    const record = participant(id, "file-incarnation", "File Main");
    directory.registerSource(() => [record]);
    try {
      await directory.start();
      expect(directory.natsProjectionMetrics).toBeUndefined();
      expect(readParticipantFile(mesh.root, localParticipantKey(id))).toMatchObject({ value: { id, name: "File Main" } });
    } finally { await directory.close(); }
  });
});

function deferred<T = void>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  const promise = new Promise<T>(done => { resolve = done; });
  return { promise, resolve };
}
