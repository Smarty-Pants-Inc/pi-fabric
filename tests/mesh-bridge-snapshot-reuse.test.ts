import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { BridgeChange } from "../src/mesh/change-notifier.js";
import type { MeshStateEntry } from "../src/mesh/store.js";

// Only scheduling is replaced: every capture, state fence, lease read, mirror repair and
// held publication below uses the real SQLite MeshStore and real files in an owned temp root.
const observation = vi.hoisted(() => ({
  notify: new Map<string, (kind: BridgeChange) => void>(),
  inputs: [] as string[],
}));
vi.mock("../src/mesh/change-notifier.js", () => ({
  watchBridgeStore: (root: string, changed: (kind: BridgeChange) => void) => {
    observation.notify.set(root, changed);
    return () => { observation.notify.delete(root); };
  },
}));
vi.mock("../src/topology/host-leases.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../src/topology/host-leases.js")>();
  return { ...actual, readHostLeases: vi.fn(actual.readHostLeases) };
});
// Observe the exact canonical bytes passed to SHA-256 without exporting a private snapshot
// API or reproducing its implementation. Hashing itself is unchanged (including chained calls).
vi.mock("node:crypto", async (importOriginal) => {
  const actual = await importOriginal<typeof import("node:crypto")>();
  return { ...actual, createHash: (...args: Parameters<typeof actual.createHash>) => {
    const hash = actual.createHash(...args);
    const update = hash.update.bind(hash);
    hash.update = (data, encoding) => {
      if (typeof data === "string" && data.startsWith("{\"mirrored\":")) {
        const value = JSON.parse(data);
        if (Array.isArray(value.mirrored) && Array.isArray(value.relays) && (value.native || value.topology)) {
          observation.inputs.push(data);
        }
      }
      return update(data, encoding);
    };
    return hash;
  } };
});

const { StoreBridgeSide } = await import("../src/mesh/bridge.js");
const { MeshStore } = await import("../src/mesh/store.js");
const { SqliteStateBackend } = await import("../src/mesh/state-backend.js");
const { SqliteStateStore } = await import("../src/mesh/state-sqlite.js");
const { hostLeasePath, readHostLease, readHostLeases, writeHostLease } = await import("../src/topology/host-leases.js");
const { meshDirectoryStamp } = await import("../src/topology/publication-generation.js");
const { writeParticipantFile } = await import("../src/topology/participant-files.js");
import type { BridgePresence } from "../src/mesh/bridge.js";

const NOW = 1_800_000_000_000;
const PREFIXES = ["topology/hosts/", "topology/participants/", "topology/handoffs/"] as const;
const key = (kind: string, id: string) => `topology/${kind}/${createHash("sha256").update(id).digest("hex")}`;
const roots: string[] = [];
const cleanup: Array<() => void> = [];
afterEach(() => {
  vi.restoreAllMocks();
  for (const close of cleanup.splice(0).reverse()) close();
  vi.useRealTimers();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  observation.notify.clear();
  observation.inputs.length = 0;
  vi.mocked(readHostLeases).mockClear();
});
// Notification delivery is the event source; advance exactly the fixed coalescing window.
// Retry tests advance their one pending backoff separately, never sleep past multiple ticks.
const turn = () => vi.advanceTimersByTimeAsync(100);
const claimed = (name: string): BridgePresence => {
  const id = `session:${name}-00000000`;
  const identity = { id, name, kind: "main" as const, sessionId: `${name}-00000000` };
  return {
    hosts: [{ record: { format: 1, id, rootId: id, identity, startedAt: NOW - 10_000,
      incarnationToken: `${name}-token`, updatedAt: NOW, expiresAt: NOW + 120_000 }, expiresAt: NOW + 120_000, observedAt: NOW }],
    participants: [{ format: 1, id, kind: "root", rootId: id, ownerHostId: id, ownerIdentityId: id,
      name, label: name, status: "idle", runner: "pi", transport: "host", capabilities: ["steer"],
      sessionId: identity.sessionId, startedAt: NOW - 10_000, updatedAt: NOW, controlProtocol: "v1" }],
    reserved: [],
  };
};
const fixture = async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-snapshot-reuse-"));
  roots.push(root);
  const store = new MeshStore(root, 64 * 1024, 100, { stateBackend: "sqlite" });
  cleanup.push(() => store.closeState());
  const native = claimed("native"), direct = claimed("direct"), relay = claimed("relay");
  const host = { ...native.hosts[0]!.record, expiresAt: NOW + 1_000 };
  const participant = native.participants[0]!;
  const identity = host.identity;
  await store.writeBatch({ identity, ops: [
    { kind: "put", key: key("hosts", host.id), value: host },
    { kind: "put", key: key("participants", participant.id), value: participant },
    { kind: "put", key: key("handoffs", "session:moved-00000000"), value: { format: 1,
      id: "session:moved-00000000", sessionId: "moved-00000000", name: "moved", link: "far",
      handedOffAt: NOW - 1_000, operator: "operator", hub: "hub" } },
    // Malformed native rows reserve their attributable ids and keep the snapshot partial.
    { kind: "put", key: key("hosts", "session:invalid-00000000"), value: { id: "session:invalid-00000000" } },
  ] });
  const renew = (expiresAt: number) => writeHostLease(root, { id: host.id, rootId: host.rootId,
    identityId: identity.id, startedAt: host.startedAt,
    ...(host.incarnationToken !== undefined ? { incarnationToken: host.incarnationToken } : {}),
    updatedAt: NOW + expiresAt - (NOW + 60_000), expiresAt });
  renew(NOW + 60_000);
  const side = new StoreBridgeSide(store, "far", () => NOW);
  await side.mirror(direct);
  await new StoreBridgeSide(store, "other", () => NOW).mirror(relay);
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
  const changes: BridgeChange[] = [], failures: Error[] = [];
  const subscribe = (log?: (message: string) => void) => {
    const unsubscribe = side.subscribeChanges(kind => changes.push(kind), error => failures.push(error), log);
    cleanup.push(unsubscribe);
    expect(observation.notify.has(root)).toBe(true);
    return unsubscribe;
  };
  const fire = async () => {
    observation.notify.get(root)!("presence");
    await turn();
    expect(failures).toEqual([]);
  };
  return { root, store, side, native, direct, relay, host, participant, identity, renew, changes, failures, subscribe, fire };
};
type Fixture = Awaited<ReturnType<typeof fixture>>;
const frames = () => {
  expect(observation.inputs.length % 2).toBe(0);
  return Array.from({ length: observation.inputs.length / 2 }, (_, index) => ({
    content: observation.inputs[index * 2]!, topology: observation.inputs[index * 2 + 1]!,
  }));
};
const clearInputs = () => { observation.inputs.length = 0; vi.mocked(readHostLeases).mockClear(); };
const measure = (f: Fixture) => {
  let fenced = false, inReadSnapshot = false;
  const reads: Array<{ prefix: string; fresh: boolean; fenced: boolean; snapshot: boolean }> = [];
  const leaseReads: boolean[] = [], leaseSnapshots: boolean[] = [];
  const readLeases = vi.mocked(readHostLeases).getMockImplementation()!;
  vi.mocked(readHostLeases).mockImplementation((...args) => {
    leaseReads.push(fenced);
    leaseSnapshots.push(inReadSnapshot);
    return readLeases(...args);
  });
  const original = SqliteStateBackend.prototype.listAll;
  vi.spyOn(SqliteStateBackend.prototype, "listAll").mockImplementation(function (this: InstanceType<typeof SqliteStateBackend>, prefix = "", options = {}) {
    if (this.root === f.root) reads.push({ prefix, fresh: options.fresh === true, fenced, snapshot: inReadSnapshot });
    return original.call(this, prefix, options);
  });
  // listAllShared can materialize a complete SQLite snapshot instead of the three ranges.
  const shared = vi.spyOn(SqliteStateBackend.prototype, "listAllShared");
  const readSnapshot = f.store.withStateReadSnapshot.bind(f.store);
  const snapshots = vi.spyOn(f.store, "withStateReadSnapshot").mockImplementation(operation => readSnapshot(() => {
    inReadSnapshot = true;
    try { return operation(); } finally { inReadSnapshot = false; }
  }));
  const writeFence = f.store.withStateWriteFence.bind(f.store);
  const writers = vi.spyOn(f.store, "withStateWriteFence").mockImplementation(operation => writeFence(() => {
    fenced = true;
    try { return operation(); } finally { fenced = false; }
  }));
  const revisions = vi.spyOn(f.store, "stateRevision");
  const proofs = vi.spyOn(f.store, "stateUnchangedSince");
  const expectAttempts = (inside: boolean[]) => {
    expect(shared).not.toHaveBeenCalled();
    expect(reads).toHaveLength(inside.length * PREFIXES.length);
    for (const prefix of PREFIXES) expect(reads.filter(read => read.prefix === prefix)).toEqual(
      inside.map(snapshot => ({ prefix, fresh: true, fenced: false, snapshot })),
    );
    expect(vi.mocked(readHostLeases).mock.calls).toEqual(inside.map(() => [f.root]));
    expect(leaseReads).toEqual(inside.map(() => false));
    expect(leaseSnapshots).toEqual(inside);
    expect(frames()).toHaveLength(inside.length);
  };
  return { snapshots, writers, revisions, proofs, reads, leaseReads, expectAttempts };
};

// Keep: trust-boundary/freshness fences, and the recorded snapshot CPU/read amplification
// incident. Gate mechanisms/counts, not noisy elapsed-time thresholds.
describe("SQLite bridge snapshots reuse only proven unchanged observations", () => {
  it("reads each fresh backend prefix and scans leases once for all three projections", async () => {
    const f = await fixture();
    clearInputs();
    const m = measure(f);
    f.subscribe();
    const initial = frames()[0]!;
    expect(JSON.parse(initial.content).native).toMatchObject({ partial: true, droppedCount: 1,
      hosts: [{ record: { id: f.host.id, expiresAt: NOW + 60_000 }, expiresAt: NOW + 60_000 }],
      participants: [f.participant] });
    expect(JSON.parse(initial.content).mirrored).toHaveLength(2);
    expect(JSON.parse(initial.content).relays).toHaveLength(2);
    expect(f.changes).toEqual([]);
    m.expectAttempts([true]);
    expect(m.snapshots).toHaveBeenCalledTimes(1);
    expect(m.writers).toHaveBeenCalledTimes(1);
    expect(m.proofs).not.toHaveBeenCalled();
    expect(m.revisions).toHaveBeenCalledTimes(1);
  });

  it("skips unchanged captures and retains lease/presence classification on invalidation", async () => {
    const f = await fixture();
    f.subscribe();
    const initial = frames().at(-1)!;
    clearInputs();
    const m = measure(f);
    await f.fire();
    expect(frames()).toEqual([]);
    expect(m.snapshots).not.toHaveBeenCalled();
    expect(m.writers).not.toHaveBeenCalled();
    expect(f.changes).toEqual([]);
    // A new SQLite version with reordered JSON properties is semantically unchanged.
    await f.store.put({ key: key("participants", f.participant.id), identity: f.identity,
      value: Object.fromEntries(Object.entries(f.participant).reverse()) });
    await f.fire();
    expect(frames().at(-1)).toEqual(initial);
    expect(f.changes).toEqual([]);
    f.renew(NOW + 90_000);
    await f.fire();
    const extended = frames().at(-1)!;
    expect(extended.topology).toBe(initial.topology);
    expect(extended.content).not.toBe(initial.content);
    expect(JSON.parse(extended.content).native.hosts[0].expiresAt).toBe(NOW + 90_000);
    expect(f.changes.splice(0)).toEqual(["lease"]);
    f.renew(NOW + 70_000);
    await f.fire();
    expect(f.changes.splice(0)).toEqual(["presence"]); // Shortening must not be batched as renewal.
    await f.store.put({ key: key("participants", f.participant.id), identity: f.identity,
      value: { ...f.participant, status: "running" } });
    await f.fire();
    expect(JSON.parse(frames().at(-1)!.content).native.participants[0].status).toBe("running");
    expect(f.changes.splice(0)).toEqual(["presence"]);
    const handoffKey = key("handoffs", "session:moved-00000000");
    const handoff = f.store.get(handoffKey, { fresh: true })!;
    await f.store.put({ key: handoffKey, identity: f.identity, value: { ...handoff.value as object, link: "elsewhere" } });
    await f.fire();
    expect(JSON.parse(frames().at(-1)!.content).native.reserved).toEqual(expect.arrayContaining([
      "session:moved-00000000", "moved-00000000", "moved",
    ]));
    expect(f.changes.splice(0)).toEqual(["presence"]);
    const relayParticipant = f.relay.participants[0]!;
    await f.store.put({ key: key("participants", relayParticipant.id), identity: f.relay.hosts[0]!.record.identity,
      value: { ...relayParticipant, remoteHost: "other", status: "running" } });
    await f.fire();
    expect(JSON.parse(frames().at(-1)!.content).relays).toEqual(expect.arrayContaining([
      expect.objectContaining({ value: expect.objectContaining({ id: relayParticipant.id, status: "running" }) }),
    ]));
    expect(f.changes.splice(0)).toEqual(["presence"]);
    m.expectAttempts(Array<boolean>(6).fill(true));
  });

  it("debounces renewMirroredLeases without hiding native lease changes or leaving retries", async () => {
    const f = await fixture();
    const unsubscribe = f.subscribe();
    clearInputs();
    try {
      const notify = observation.notify.get(f.root)!;
      const source = f.direct.hosts[0]!;
      // The installed mirror was observed at NOW. A slightly newer local clock lets each
      // valid source sample advance its exact file version while every callback stays in-window.
      const renewalSide = new StoreBridgeSide(f.store, "far", () => NOW + 1_000);
      for (let step = 1; step <= 3; step += 1) {
        const observedAt = NOW + step * 100;
        await renewalSide.renewMirroredLeases([{
          record: { ...source.record, updatedAt: observedAt },
          expiresAt: observedAt + 20_000,
          observedAt,
        }], observedAt);
        notify("presence");
        await vi.advanceTimersByTimeAsync(20); // A separate event-loop turn, still before 100 ms.
        expect(frames()).toEqual([]);
      }
      expect(readHostLease(f.root, source.record.id)?.mirror).toMatchObject({
        remoteHost: "far", observedAt: NOW + 300, sourceObservedAt: NOW + 300,
        sourceUpdatedAt: NOW + 300, sourceExpiresAt: NOW + 20_300,
      });

      vi.mocked(readHostLeases).mockClear();
      const m = measure(f);
      await vi.advanceTimersByTimeAsync(40);
      expect(frames()).toHaveLength(1);
      expect(f.changes).toEqual([]); // Mirror-only renewal fields are not presence changes.
      m.expectAttempts([true]); // Three fresh prefixes and one lease scan in a read transaction, NEVER writer custody.
      expect(m.snapshots).toHaveBeenCalledTimes(1);
      expect(m.writers).not.toHaveBeenCalled();
      expect(m.reads.every(read => !read.fenced && read.snapshot)).toBe(true);
      expect(m.leaseReads).toEqual([false]);
      expect(vi.getTimerCount()).toBe(0);

      // A real native lease extension is still visible in the next independent burst.
      f.renew(NOW + 90_000);
      notify("presence");
      await vi.advanceTimersByTimeAsync(99);
      expect(frames()).toHaveLength(1);
      await vi.advanceTimersByTimeAsync(1);
      expect(frames()).toHaveLength(2);
      expect(JSON.parse(frames().at(-1)!.content).native.hosts[0].expiresAt).toBe(NOW + 90_000);
      expect(f.changes.splice(0)).toEqual(["lease"]);
      m.expectAttempts([true, true]);
      expect(m.snapshots).toHaveBeenCalledTimes(2);
      expect(m.writers).not.toHaveBeenCalled();

      // Unsubscribe cancels a newly queued notification; it cannot turn into retries or scans.
      const settledFrames = frames();
      notify("presence");
      await vi.advanceTimersByTimeAsync(50);
      expect(vi.getTimerCount()).toBe(1);
      unsubscribe();
      expect(vi.getTimerCount()).toBe(0);
      await vi.advanceTimersByTimeAsync(500);
      expect(frames()).toEqual(settledFrames);
      expect(f.changes).toEqual([]);
      expect(f.failures).toEqual([]);
      m.expectAttempts([true, true]);
      expect(m.snapshots).toHaveBeenCalledTimes(2);
      expect(m.writers).not.toHaveBeenCalled();
      expect(vi.getTimerCount()).toBe(0);
    } finally { unsubscribe(); }
  });

  it("settles once before taking the current consistent read snapshot outside writer custody", async () => {
    const f = await fixture();
    f.subscribe();
    const baseline = frames().at(-1)!;
    const writer = await SqliteStateStore.open(f.root, 64 * 1024, 100);
    cleanup.push(() => writer.close());
    await writer.put({ key: key("participants", f.participant.id), identity: f.identity,
      value: { ...f.participant, status: "obsolete" } });
    clearInputs();
    const m = measure(f);
    const fence = m.writers.getMockImplementation()!;
    const pending: Array<Promise<unknown>> = [];
    m.writers.mockImplementation(operation => {
      pending.push(writer.put({ key: key("participants", f.participant.id), identity: f.identity,
        value: f.participant }));
      expect(writer.get(key("participants", f.participant.id), { fresh: true })!.value).toEqual(f.participant);
      return fence(operation);
    });
    await f.fire();
    await Promise.all(pending);
    expect(frames()).toEqual([baseline]);
    expect(f.changes).toEqual([]);
    expect(m.writers).toHaveBeenCalledTimes(1);
    expect(m.snapshots).toHaveBeenCalledTimes(1);
    expect(m.revisions).toHaveBeenCalledTimes(1);
    m.expectAttempts([true]);
  });

  it("rejects obsolete SQLite read captures and accepts a third only on a later retry tick", async () => {
    const f = await fixture();
    f.subscribe();
    const baseline = frames().at(-1)!;
    const writer = await SqliteStateStore.open(f.root, 64 * 1024, 100, { lockTimeoutMs: 0, busyTimeoutMs: 0 });
    cleanup.push(() => writer.close());
    const pending: Array<Promise<unknown>> = [];
    const setStatus = (status: string) => {
      pending.push(writer.put({ key: key("participants", f.participant.id), identity: f.identity,
        value: { ...f.participant, status } }));
      expect(writer.get(key("participants", f.participant.id), { fresh: true })!.value).toMatchObject({ status });
    };
    setStatus("obsolete-0");
    clearInputs();
    const m = measure(f);
    const snapshot = m.snapshots.getMockImplementation()!;
    let entered = 0;
    m.snapshots.mockImplementation(operation => snapshot(() => {
      const value = operation();
      // A real WAL writer commits while this read transaction is still pinned. That is
      // legal for observation, impossible under writer custody, and invalidates this capture.
      entered += 1;
      if (entered <= 2) setStatus(entered === 1 ? "obsolete-1" : "idle");
      return value;
    }));
    await f.fire();
    expect(m.snapshots).toHaveBeenCalledTimes(1);
    expect(m.writers).toHaveBeenCalledTimes(1);
    expect(f.changes).toEqual([]);
    expect(vi.getTimerCount()).toBe(1);
    await turn(); // first retry: the second capture is also invalidated
    expect(m.snapshots).toHaveBeenCalledTimes(2);
    expect(m.writers).toHaveBeenCalledTimes(2);
    expect(f.changes).toEqual([]);
    expect(vi.getTimerCount()).toBe(1);
    await vi.advanceTimersByTimeAsync(199);
    expect(m.snapshots).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(1); // second retry has the doubled 200 ms backoff
    await Promise.all(pending);
    expect(frames().map(frame => JSON.parse(frame.content).native.participants[0].status))
      .toEqual(["obsolete-0", "obsolete-1", "idle"]);
    expect(frames().at(-1)).toEqual(baseline);
    expect(f.changes).toEqual([]); // Invalidated reads never replace the subscriber's baseline.
    expect(m.revisions).toHaveBeenCalledTimes(3);
    expect(m.snapshots).toHaveBeenCalledTimes(3);
    expect(m.writers).toHaveBeenCalledTimes(3);
    m.expectAttempts([true, true, true]);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("bounds repeated SQLite read invalidation to one capture per tick, never a final writer scan", async () => {
    const f = await fixture();
    const unsubscribe = f.subscribe();
    const writer = await SqliteStateStore.open(f.root, 64 * 1024, 100, { lockTimeoutMs: 0, busyTimeoutMs: 0 });
    cleanup.push(() => writer.close());
    await writer.put({ key: key("participants", f.participant.id), identity: f.identity,
      value: { ...f.participant, status: "obsolete" } });
    clearInputs();
    const m = measure(f);
    const snapshot = m.snapshots.getMockImplementation()!;
    const pending: Array<Promise<unknown>> = [];
    let version = 0;
    m.snapshots.mockImplementation(operation => snapshot(() => {
      const value = operation();
      const status = `churn-${++version}`;
      pending.push(writer.put({ key: key("participants", f.participant.id), identity: f.identity,
        value: { ...f.participant, status } }));
      expect(writer.get(key("participants", f.participant.id), { fresh: true })!.value).toMatchObject({ status });
      return value;
    }));
    await f.fire();
    expect(m.revisions).toHaveBeenCalledTimes(1);
    expect(m.snapshots).toHaveBeenCalledTimes(1);
    expect(m.writers).toHaveBeenCalledTimes(1);
    m.expectAttempts([true]);
    expect(f.changes).toEqual([]);
    expect(f.failures).toEqual([]);
    expect(vi.getTimerCount()).toBe(1);
    await vi.advanceTimersByTimeAsync(99);
    expect(m.snapshots).toHaveBeenCalledTimes(1);
    unsubscribe();
    expect(vi.getTimerCount()).toBe(0);
    await vi.advanceTimersByTimeAsync(500);
    await Promise.all(pending);
    expect(m.snapshots).toHaveBeenCalledTimes(1);
    expect(m.writers).toHaveBeenCalledTimes(1);
    m.expectAttempts([true]);
  });

  it.each(["participants", "host-leases"] as const)("retries directory-only %s churn on the next tick, without repeated writer custody", async directory => {
    const f = await fixture();
    let fileEntry: MeshStateEntry | undefined;
    if (directory === "participants") {
      fileEntry = f.store.get(key("participants", f.participant.id), { fresh: true })!;
      await f.store.delete({ key: fileEntry.key });
      writeParticipantFile(f.root, fileEntry);
    }
    f.subscribe();
    const baseline = frames().at(-1)!;
    const revision = f.store.stateRevision();
    const mutate = (step: number) => {
      if (fileEntry) writeParticipantFile(f.root, { ...fileEntry,
        value: { ...f.participant, status: step === 0 ? "obsolete-0" : "idle" } });
      else f.renew(NOW + 70_000 + step * 10_000);
    };
    mutate(0);
    clearInputs();
    const m = measure(f);
    const snapshot = m.snapshots.getMockImplementation()!;
    let entered = 0;
    m.snapshots.mockImplementation(operation => {
      const value = snapshot(operation);
      if (entered++ === 0) {
        const stamp = meshDirectoryStamp(f.root, directory);
        mutate(1);
        expect(meshDirectoryStamp(f.root, directory)).not.toBe(stamp);
      }
      return value;
    });
    await f.fire();
    expect(m.snapshots).toHaveBeenCalledTimes(1);
    expect(m.writers).not.toHaveBeenCalled();
    expect(f.changes).toEqual([]);
    expect(vi.getTimerCount()).toBe(1);
    await vi.advanceTimersByTimeAsync(99);
    expect(m.snapshots).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(1); // Invalidated files retry without another notification.
    expect(f.store.stateRevision()).toBe(revision);
    expect(m.proofs.mock.results.map(result => result.value)).toEqual([true, true]);
    expect(m.snapshots).toHaveBeenCalledTimes(2);
    expect(m.writers).not.toHaveBeenCalled();
    if (fileEntry) {
      expect(frames().map(frame => JSON.parse(frame.content).native.participants[0].status))
        .toEqual(["obsolete-0", "idle"]);
      expect(frames().at(-1)).toEqual(baseline);
      expect(f.changes).toEqual([]);
    } else {
      expect(frames().map(frame => JSON.parse(frame.content).native.hosts[0].expiresAt))
        .toEqual([NOW + 70_000, NOW + 80_000]);
      expect(frames().at(-1)!.topology).toBe(baseline.topology);
      expect(f.changes).toEqual(["lease"]);
    }
    m.expectAttempts([true, true]);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("retries an invalidated read capture to convergence and resets its backoff after success", async () => {
    const f = await fixture();
    const logs: string[] = [];
    const unsubscribe = f.subscribe(line => logs.push(line));
    clearInputs();
    const m = measure(f);
    const snapshot = m.snapshots.getMockImplementation()!;
    let invalidate = true, version = 0;
    m.snapshots.mockImplementation(operation => {
      const value = snapshot(operation);
      if (invalidate) f.renew(NOW + 70_001 + ++version);
      return value;
    });
    try {
      f.renew(NOW + 70_000); // A real changed file requires a read, then changes again during capture.
      await f.fire();
      expect(m.snapshots).toHaveBeenCalledTimes(1);
      expect(m.writers).not.toHaveBeenCalled();
      expect(f.changes).toEqual([]);
      expect(vi.getTimerCount()).toBe(1);
      await f.store.put({ key: key("participants", f.participant.id), identity: f.identity,
        value: { ...f.participant, status: "running" } });
      invalidate = false;
      await turn(); // Retry succeeds with current SQLite rows and current files.
      expect(m.snapshots).toHaveBeenCalledTimes(2);
      expect(m.writers).toHaveBeenCalledTimes(1);
      expect(f.changes.splice(0)).toEqual(["presence"]);
      expect(JSON.parse(frames().at(-1)!.content).native.participants[0].status).toBe("running");
      expect(logs).toEqual([]);
      expect(vi.getTimerCount()).toBe(0);
      // A fresh invalidation after success must again use the minimum 100 ms retry.
      invalidate = true;
      f.renew(NOW + 80_000);
      await f.fire();
      expect(m.snapshots).toHaveBeenCalledTimes(3);
      expect(vi.getTimerCount()).toBe(1);
      await vi.advanceTimersByTimeAsync(99);
      expect(m.snapshots).toHaveBeenCalledTimes(3);
      await vi.advanceTimersByTimeAsync(1);
      expect(m.snapshots).toHaveBeenCalledTimes(4);
      expect(m.writers).toHaveBeenCalledTimes(1);
      expect(vi.getTimerCount()).toBe(1);
      expect(f.changes).toEqual([]);
      expect(f.failures).toEqual([]);
      m.expectAttempts([true, true, true, true]);
    } finally { unsubscribe(); }
    expect(vi.getTimerCount()).toBe(0);
  });

  it("counts invalidated read captures in one minute diagnostic windows and is silent without deferrals", async () => {
    const f = await fixture();
    const logs: string[] = [];
    const unsubscribe = f.subscribe(line => logs.push(line));
    clearInputs();
    const m = measure(f);
    const snapshot = m.snapshots.getMockImplementation()!;
    let invalidate = false, version = 0;
    m.snapshots.mockImplementation(operation => {
      const value = snapshot(operation);
      if (invalidate) f.renew(NOW + 70_001 + ++version);
      return value;
    });
    try {
      await f.fire(); // A healthy reusable observation is silent and leaves no timer.
      expect(logs).toEqual([]);
      expect(vi.getTimerCount()).toBe(0);
      invalidate = true;
      f.renew(NOW + 70_000);
      await f.fire();
      expect(m.snapshots).toHaveBeenCalledTimes(1);
      await turn(); // A second invalidated capture, not three old-style writer proofs.
      expect(m.snapshots).toHaveBeenCalledTimes(2);
      expect(logs).toEqual([]);
      vi.setSystemTime(Date.now() + 60_000);
      await vi.advanceTimersByTimeAsync(199);
      expect(m.snapshots).toHaveBeenCalledTimes(2);
      await vi.advanceTimersByTimeAsync(1); // Third deferral closes the minute window.
      expect(logs).toEqual(["bridge far: capture deferred: 3 in last 60s (retry 400ms)"]);
      expect(m.snapshots).toHaveBeenCalledTimes(3);
      expect(m.writers).not.toHaveBeenCalled();
      expect(f.changes).toEqual([]);
      expect(f.failures).toEqual([]);
      m.expectAttempts([true, true, true]);
      expect(vi.getTimerCount()).toBe(1);
    } finally { unsubscribe(); }
    expect(vi.getTimerCount()).toBe(0);
  });

  it.each(["hosts", "participants", "lease"] as const)("detects externally removed mirror %s and repairs real held authority", async removed => {
    const f = await fixture();
    f.subscribe();
    const baseline = frames().at(-1)!;
    const id = f.direct.hosts[0]!.record.id;
    expect(f.side.holds(id, f.direct.hosts[0]!.record.identity)).toBe(true);
    if (removed === "lease") fs.unlinkSync(hostLeasePath(f.root, id));
    else await f.store.delete({ key: key(removed, id) });
    clearInputs();
    await f.fire();
    expect(f.changes.splice(0)).toEqual(["presence"]);
    const missing = frames().at(-1)!;
    expect(JSON.parse(missing.content).native).toEqual(JSON.parse(baseline.content).native);
    expect(missing.content).not.toBe(baseline.content);
    await f.side.mirror(f.direct);
    expect(f.store.get(key("hosts", id), { fresh: true })!.value).toMatchObject({ id, remoteHost: "far" });
    expect(f.store.get(key("participants", id), { fresh: true })!.value).toMatchObject({ id, remoteHost: "far" });
    expect(readHostLease(f.root, id)).toMatchObject({ id, mirrorLease: 1, mirror: { remoteHost: "far" } });
    await f.fire();
    expect(frames().at(-1)).toEqual(baseline);
    expect(f.changes.splice(0)).toEqual(["presence"]);
    expect(f.side.holds(id, f.direct.hosts[0]!.record.identity)).toBe(true);
    await f.side.publish({ topic: "fleet.work.snapshot", kind: "ask", from: f.direct.hosts[0]!.record.identity,
      to: f.host.id, text: "repaired", data: { bridge: { from: "far", id: `repair-${removed}` } } }, [id]);
    expect(f.store.read({ limit: 10 }).map(event => event.text)).toEqual(["repaired"]);
    // A remote can still never replace the native's reserved identity on this link.
    await f.side.mirror({ hosts: [...f.direct.hosts, ...f.native.hosts], participants: [...f.direct.participants, ...f.native.participants] });
    expect(f.store.get(key("hosts", f.host.id), { fresh: true })!.value).toEqual(f.host);
    expect(f.side.holds(f.host.id, f.identity)).toBe(false);
  });
});
