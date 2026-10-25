// L1 (smarty-dev#15310): NATS side of the A' bridge, presence both ways, against an in-memory
// multi-machine KV with the broker's CAS/watch semantics and the real KV participant directory.
import { createHash } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  NatsBridgePresence,
  NatsMirrorFencedError,
  projectNatsMirrorRecord,
  type NatsBridgeMirrorPresence,
} from "../src/mesh/bridge-nats-presence.js";
import type { BridgeHost } from "../src/mesh/bridge.js";
import { KvParticipantDirectory } from "../src/topology/participant-view.js";
import type { TransportHostLeaseProcessProof } from "../src/topology/host-leases.js";
import type { FabricKvHostLease, FabricParticipantRecord } from "../src/topology/types.js";
import {
  KvConflictError,
  TransportDeniedError,
  TransportUncertainError,
  type FabricTransport,
  type TransportBucket,
  type TransportKv,
  type TransportKvEntry,
  type TransportKvEvent,
} from "../src/transport/types.js";

const BRIDGE = "ryzen1-bridge";
const V2 = "v2-org";
const token = (id: string): string => createHash("sha256").update(id).digest("hex").slice(0, 32);
const pKey = (machine: string, id: string): string => `${machine}.p.${token(id)}`;
const hKey = (machine: string, id: string): string => `${machine}.h.${token(id)}`;
const FILE_ROOT = "session:11111111-1111-4111-8111-111111111111";
const NATS_ROOT = "session:22222222-2222-4222-8222-222222222222";
const PROOF: TransportHostLeaseProcessProof = { pid: 4242, pidStartTime: "777", bootId: "12345678-1234-4234-8234-123456789abc" };
const OTHER_PROOF: TransportHostLeaseProcessProof = { pid: 999, pidStartTime: "555", bootId: PROOF.bootId };

type Event = TransportKvEvent<unknown>;
interface Watcher { queue: Event[]; wake?: (() => void) | undefined }

/** One KV bucket shared by every machine: CAS revisions, ordered watches, explicit expiry. */
class MemoryBucket {
  readonly values = new Map<string, TransportKvEntry<unknown>>();
  readonly watchers = new Set<Watcher>();
  revision = 0;
  failNextPut: ((key: string) => "uncertain-stored" | "uncertain-lost" | undefined) | undefined;
  constructor(readonly ttlMs: number, readonly writes: string[] = []) {}
  emit(event: Event): void { for (const watcher of this.watchers) { watcher.queue.push(event); watcher.wake?.(); } }
  put(key: string, owner: string, value: unknown, ifRevision: number): number {
    const current = this.values.get(key);
    if ((ifRevision === 0 && current) || (ifRevision > 0 && current?.revision !== ifRevision)) throw new KvConflictError(key, ifRevision);
    const fault = this.failNextPut?.(key);
    if (fault) this.failNextPut = undefined;
    if (fault === "uncertain-lost") throw new TransportUncertainError(key, { owner, value });
    const entry = { key, machine: key.split(".")[0]!, owner, value: JSON.parse(JSON.stringify(value)) as unknown, revision: ++this.revision };
    this.writes.push(key);
    this.values.set(key, entry);
    this.emit({ type: "put", entry });
    if (fault === "uncertain-stored") throw new TransportUncertainError(key, { owner, value });
    return entry.revision;
  }
  delete(key: string, ifRevision: number): boolean {
    const current = this.values.get(key);
    if (!current || current.revision !== ifRevision) return false;
    this.values.delete(key);
    this.emit({ type: "gone", key, machine: key.split(".")[0]!, revision: ++this.revision, reason: "deleted" });
    return true;
  }
  expire(key: string): void {
    if (!this.values.delete(key)) return;
    this.emit({ type: "gone", key, machine: key.split(".")[0]!, revision: ++this.revision, reason: "expired" });
  }
}

class MemoryKv implements TransportKv {
  constructor(readonly bucket: MemoryBucket, readonly machine: string) {}
  get ttlMs(): number { return this.bucket.ttlMs; }
  async get<V>(key: string): Promise<TransportKvEntry<V> | undefined> { return this.bucket.values.get(key) as TransportKvEntry<V> | undefined; }
  async put<V>(key: string, owner: string, value: V, ifRevision: number): Promise<number> {
    if (key.split(".")[0] !== this.machine) throw new TransportDeniedError("KV write is outside the credential's machine prefix");
    return this.bucket.put(key, owner, value, ifRevision);
  }
  async delete(key: string, ifRevision: number): Promise<boolean> {
    if (key.split(".")[0] !== this.machine) throw new TransportDeniedError("KV write is outside the credential's machine prefix");
    return this.bucket.delete(key, ifRevision);
  }
  async *watch<V>(_prefix: string, signal: AbortSignal): AsyncIterable<TransportKvEvent<V>> {
    const watcher: Watcher = { queue: [] };
    for (const entry of this.bucket.values.values()) watcher.queue.push({ type: "put", entry });
    watcher.queue.push({ type: "ready" });
    this.bucket.watchers.add(watcher);
    try {
      while (!signal.aborted) {
        const event = watcher.queue.shift() ?? await new Promise<Event | undefined>(resolve => {
          const abort = (): void => { watcher.wake = undefined; resolve(undefined); };
          watcher.wake = () => { watcher.wake = undefined; signal.removeEventListener("abort", abort); resolve(watcher.queue.shift()); };
          signal.addEventListener("abort", abort, { once: true });
        });
        if (!event) return;
        yield event as TransportKvEvent<V>;
      }
    } finally { this.bucket.watchers.delete(watcher); }
  }
}

interface Clock { wall: number; mono: number }
const advance = (clock: Clock, ms: number): void => { clock.wall += ms; clock.mono += ms; };

function world() {
  const writes: string[] = [];
  const presence = new MemoryBucket(600_000, writes);
  const leases = new MemoryBucket(15_000, writes);
  const transport = (machine: string, hostId: string): FabricTransport => ({
    kind: "nats", self: { machine, hostId },
    async send() { throw new Error("unused"); }, async *inbox() { return; },
    kv(bucket: TransportBucket) { return new MemoryKv(bucket === "presence" ? presence : leases, machine); },
    async drainState() { return { pending: 0, ackPending: 0 }; }, async close() {},
  });
  return { presence, leases, transport, writes };
}

const openers: Array<{ close(): Promise<void> }> = [];
afterEach(async () => { await Promise.allSettled(openers.splice(0).map(item => item.close())); });

function bridge(w: ReturnType<typeof world>, options: { admit?: (record: FabricParticipantRecord, direction: string) => boolean;
  predecessorExited?: (proof: TransportHostLeaseProcessProof) => boolean; clock?: Clock; log?: string[] } = {}) {
  const clock = options.clock ?? { wall: 1_000_000, mono: 50_000 };
  const side = new NatsBridgePresence({
    transport: w.transport(BRIDGE, "bridge:ryzen1"), link: "nats", admit: options.admit ?? (() => true),
    processProof: PROOF, predecessorExited: options.predecessorExited ?? (() => false),
    now: () => clock.wall, monotonic: () => clock.mono, renewMs: 5_000,
    ...(options.log ? { log: (line: string) => { options.log!.push(line); } } : {}),
  });
  openers.push(side);
  side.start();
  return { side, clock };
}

function natsRoot(id = NATS_ROOT, incarnation = "nats-inc-1", overrides: Partial<FabricParticipantRecord> = {}): FabricParticipantRecord {
  return { format: 1, id, kind: "root", rootId: id, ownerHostId: id, ownerIdentityId: id, ownerIncarnation: incarnation,
    name: "kate-main", status: "idle", runner: "pi", transport: "host", capabilities: ["steer", "followUp", "fabric"],
    controlProtocol: "v1", startedAt: 999_000, updatedAt: 1_000_000, sessionId: id.slice("session:".length), cwd: "/work", ...overrides };
}
function natsLease(id: string, incarnation: string, updatedAt: number): FabricKvHostLease & { incarnationToken: string } {
  return { id, rootId: id, identityId: id, incarnation, incarnationToken: incarnation, pid: 7, pidStartTime: "70",
    bootId: "aaaaaaaa-1234-4234-8234-123456789abc", updatedAt, expiresAt: updatedAt + 15_000 };
}
/** A native NATS Main on `machine`: presence plus its own host lease. */
function seedNative(w: ReturnType<typeof world>, machine: string, record: FabricParticipantRecord, leaseAt: number): void {
  const owner = `${record.ownerHostId}#${record.ownerIncarnation}`;
  w.leases.put(hKey(machine, record.ownerHostId), owner, natsLease(record.ownerHostId, record.ownerIncarnation!, leaseAt), 0);
  w.presence.put(pKey(machine, record.id), owner, record, 0);
}

function fileRoot(id = FILE_ROOT, incarnation = "file-inc-1", overrides: Partial<FabricParticipantRecord> = {}): FabricParticipantRecord {
  return { format: 1, id, kind: "root", rootId: id, ownerHostId: id, ownerIdentityId: id, ownerIncarnation: incarnation,
    name: "paul-lead", status: "running", runner: "pi", transport: "host", capabilities: ["steer", "followUp", "fabric"],
    controlProtocol: "v1", startedAt: 900_000, updatedAt: 1_000_000, sessionId: id.slice("session:".length), cwd: "/home/paul/repo",
    herdrPane: "p1", mainProcess: { pid: 1, host: "ryzen1" }, mainBindings: false, interactive: true,
    services: [{ name: "x", version: 1 }], ...overrides } as FabricParticipantRecord;
}
function fileHost(id: string, observedAt: number, expiresAt: number, incarnationToken = "host-token-1"): BridgeHost {
  return { record: { format: 1, id, rootId: id, identity: { id, name: "paul-lead", kind: "main" }, incarnationToken,
    startedAt: 900_000, updatedAt: observedAt, expiresAt }, expiresAt, observedAt };
}
const filePresence = (clock: Clock, roots: FabricParticipantRecord[], extra: Partial<NatsBridgeMirrorPresence> = {}): NatsBridgeMirrorPresence =>
  ({ hosts: roots.map(root => fileHost(root.id, clock.wall, clock.wall + 60_000)), participants: roots, ...extra });

const waitFor = (check: () => void): Promise<void> => vi.waitFor(check, { timeout: 2_000, interval: 2 });

/** A real NATS receiver's directory on another machine, read-only. */
async function receiverView(w: ReturnType<typeof world>): Promise<KvParticipantDirectory> {
  const lease = { live: () => true, signal: new AbortController().signal };
  const view = new KvParticipantDirectory({ transport: w.transport(V2, "session:receiver"), lease,
    identityId: "session:receiver", ownerIncarnation: "receiver-inc", rootId: "session:receiver" });
  openers.push(view);
  view.start();
  await waitFor(() => expect(view.ready).toBe(true));
  return view;
}

describe("NATS presence -> file mirror snapshot", () => {
  it("returns an empty partial snapshot before both KV watches are ready", async () => {
    const w = world();
    const clock = { wall: 1_000_000, mono: 50_000 };
    const side = new NatsBridgePresence({ transport: w.transport(BRIDGE, "bridge:ryzen1"), link: "nats", admit: () => true,
      processProof: PROOF, now: () => clock.wall, monotonic: () => clock.mono });
    openers.push(side);
    expect(await side.presence()).toEqual({ hosts: [], participants: [], reserved: [], partial: true });
  });

  it("maps a live NATS root to one sampled host and one projected root participant", async () => {
    const w = world();
    seedNative(w, V2, natsRoot(), 1_000_000);
    const { side, clock } = bridge(w);
    await side.whenReady();
    const presence = await side.presence();
    expect(presence.reserved).toEqual([]);
    expect(presence.partial).toBeUndefined();
    expect(presence.hosts).toHaveLength(1);
    const host = presence.hosts[0]!;
    expect(host.record).toMatchObject({ format: 1, id: NATS_ROOT, rootId: NATS_ROOT, incarnationToken: "nats-inc-1",
      identity: { id: NATS_ROOT, name: "kate-main", kind: "main", sessionId: NATS_ROOT.slice(8) } });
    expect(host.observedAt).toBe(clock.wall);
    // Receipt-bounded: 15 s TTL less the 2 s safety, never more than the mirror freshness bound.
    expect(host.expiresAt).toBe(clock.wall + 13_000);
    expect(presence.participants).toEqual([projectNatsMirrorRecord(natsRoot())]);
    expect(presence.participants[0]).not.toHaveProperty("remoteHost");
  });

  it("never returns this link's own machine rows (no echo of mirrored file roots)", async () => {
    const w = world();
    const { side, clock } = bridge(w);
    await side.whenReady();
    await side.mirror(filePresence(clock, [fileRoot()]));
    expect(side.resolve(FILE_ROOT)?.machine).toBe(BRIDGE);
    expect((await side.presence()).hosts).toEqual([]);
  });

  it("drops an id with rows on two machines: ambiguous, so neither resolvable nor mirrored", async () => {
    const w = world();
    seedNative(w, V2, natsRoot(), 1_000_000);
    seedNative(w, "v3-org", natsRoot(NATS_ROOT, "nats-inc-other"), 1_000_000);
    const log: string[] = [];
    const { side } = bridge(w, { log });
    await side.whenReady();
    expect(side.resolve(NATS_ROOT)).toBeUndefined();
    expect((await side.presence()).hosts).toEqual([]);
    expect(log.some(line => line.includes("several machines"))).toBe(true);
  });

  it("requires the exact owner lease: another incarnation or a terminal status is not live", async () => {
    const w = world();
    const record = natsRoot();
    w.leases.put(hKey(V2, NATS_ROOT), `${NATS_ROOT}#nats-inc-0`, natsLease(NATS_ROOT, "nats-inc-0", 1_000_000), 0);
    w.presence.put(pKey(V2, NATS_ROOT), `${NATS_ROOT}#nats-inc-1`, record, 0);
    const third = "session:33333333-3333-4333-8333-333333333333";
    seedNative(w, V2, natsRoot(third, "inc-3", { status: "stopped" }), 1_000_000);
    const { side } = bridge(w);
    await side.whenReady();
    expect(side.resolve(NATS_ROOT)?.live).toBe(false);
    expect(side.resolve(third)?.live).toBe(false);
    expect((await side.presence()).hosts).toEqual([]);
  });

  it("loses authority when lease renewals stop arriving, and when the owner's own expiry passed", async () => {
    const w = world();
    seedNative(w, V2, natsRoot(), 1_000_000);
    const { side, clock } = bridge(w);
    await side.whenReady();
    expect((await side.presence()).hosts).toHaveLength(1);
    advance(clock, 12_999);
    expect((await side.presence()).hosts[0]!.expiresAt).toBe(clock.wall + 1);
    advance(clock, 1);
    expect((await side.presence()).hosts).toEqual([]);
    expect(side.resolve(NATS_ROOT)?.live).toBe(false);
    // A renewal received now restores it; an owner clock already past its expiry does not.
    w.leases.put(hKey(V2, NATS_ROOT), `${NATS_ROOT}#nats-inc-1`, natsLease(NATS_ROOT, "nats-inc-1", clock.wall),
      w.leases.values.get(hKey(V2, NATS_ROOT))!.revision);
    await waitFor(() => expect(side.resolve(NATS_ROOT)?.live).toBe(true));
    w.leases.put(hKey(V2, NATS_ROOT), `${NATS_ROOT}#nats-inc-1`, natsLease(NATS_ROOT, "nats-inc-1", clock.wall - 14_000),
      w.leases.values.get(hKey(V2, NATS_ROOT))!.revision);
    await waitFor(() => expect(side.resolve(NATS_ROOT)?.live).toBe(false));
  });

  it("drops an expired lease at once and ignores a JSON-claimed machine or mirror provenance", async () => {
    const w = world();
    seedNative(w, V2, natsRoot(), 1_000_000);
    const forged = "session:44444444-4444-4444-8444-444444444444";
    // A row whose body claims bridge provenance fails the receivers' own validator.
    seedNative(w, V2, natsRoot(forged, "inc-f", { remoteHost: "ryzen1" } as Partial<FabricParticipantRecord>), 1_000_000);
    const { side } = bridge(w);
    await side.whenReady();
    expect(side.resolve(forged)).toBeUndefined();
    expect(side.resolve(NATS_ROOT)?.machine).toBe(V2);
    w.leases.expire(hKey(V2, NATS_ROOT));
    await waitFor(() => expect(side.resolve(NATS_ROOT)?.live).toBe(false));
  });

  it("applies the allow-list in the toFile direction only for roots", async () => {
    const w = world();
    seedNative(w, V2, natsRoot(), 1_000_000);
    const decisions: string[] = [];
    const { side } = bridge(w, { admit: (record, direction) => { decisions.push(`${direction}:${record.id}`); return false; } });
    await side.whenReady();
    expect((await side.presence()).hosts).toEqual([]);
    expect(decisions).toEqual([`toFile:${NATS_ROOT}`]);
  });

  it("verifies a NATS sender only as live on its credential-bound source machine", async () => {
    const w = world();
    seedNative(w, V2, natsRoot(), 1_000_000);
    const { side, clock } = bridge(w);
    await side.whenReady();
    await side.mirror(filePresence(clock, [fileRoot()]));
    expect(side.verifySender(NATS_ROOT, V2)).toMatchObject({ ok: true, route: { machine: V2 } });
    expect(side.verifySender(NATS_ROOT, "v3-org")).toEqual({ ok: false, refuse: "FABRIC_SENDER_UNVERIFIED" });
    // This link's own mirrored rows are never a NATS sender (no loop through the bridge machine).
    expect(side.verifySender(FILE_ROOT, BRIDGE)).toEqual({ ok: false, refuse: "FABRIC_SENDER_UNVERIFIED" });
    expect(side.verifySender("session:unknown-0000", V2)).toEqual({ ok: false, refuse: "FABRIC_SENDER_UNVERIFIED" });
  });

  it("wakes the bridge on another machine's change, never on its own renewals", async () => {
    const w = world();
    const { side, clock } = bridge(w);
    await side.whenReady();
    const changes: string[] = [];
    side.subscribeChanges(kind => { changes.push(kind); });
    await side.mirror(filePresence(clock, [fileRoot()]));
    await side.writer.renew();
    expect(changes).toEqual([]);
    seedNative(w, V2, natsRoot(), clock.wall);
    await waitFor(() => expect(changes).toEqual(["lease", "presence"]));
  });
});

describe("file roots -> KV presence under the bridge machine", () => {
  it("writes a lease then a projected presence row that a real NATS receiver resolves as live on the bridge machine", async () => {
    const w = world();
    const { side, clock } = bridge(w);
    await side.whenReady();
    const outcome = await side.mirror(filePresence(clock, [fileRoot()]));
    expect(outcome).toEqual({ held: [FILE_ROOT], refused: [], withdrawn: [] });
    const lease = w.leases.values.get(hKey(BRIDGE, FILE_ROOT))!;
    const row = w.presence.values.get(pKey(BRIDGE, FILE_ROOT))!;
    // Lease first: a presence row is never advertised without the lease that makes it routable.
    expect(w.writes).toEqual([hKey(BRIDGE, FILE_ROOT), pKey(BRIDGE, FILE_ROOT)]);
    expect(lease.owner).toBe(`${FILE_ROOT}#file-inc-1`);
    expect(lease.value).toMatchObject({ id: FILE_ROOT, rootId: FILE_ROOT, identityId: FILE_ROOT, incarnation: "file-inc-1",
      incarnationToken: "file-inc-1", pid: PROOF.pid, pidStartTime: PROOF.pidStartTime, bootId: PROOF.bootId,
      updatedAt: clock.wall, expiresAt: clock.wall + 15_000 });
    expect(row.owner).toBe(`${FILE_ROOT}#file-inc-1`);
    expect(row.value).toEqual(projectNatsMirrorRecord(fileRoot()));
    for (const field of ["herdrPane", "mainProcess", "mainBindings", "services", "remoteHost"]) expect(row.value).not.toHaveProperty(field);
    const receiver = await receiverView(w);
    expect(receiver.resolve(FILE_ROOT)).toMatchObject({ machine: BRIDGE, ownerHostId: FILE_ROOT, ownerIdentityId: FILE_ROOT,
      ownerIncarnation: "file-inc-1", live: true });
    expect(side.holds(FILE_ROOT)).toBe(true);
  });

  it("carries only v1 canonical native roots with an owner incarnation, and only allow-listed ones", async () => {
    const w = world();
    const log: string[] = [];
    const allowed = "session:55555555-5555-4555-8555-555555555555";
    const { side, clock } = bridge(w, { log, admit: (record, direction) => direction === "toNats" && record.id !== allowed });
    await side.whenReady();
    const legacy = fileRoot("session:66666666-6666-4666-8666-666666666666", "x", { controlProtocol: "legacy" });
    const noIncarnation = fileRoot("session:77777777-7777-4777-8777-777777777777");
    delete (noIncarnation as { ownerIncarnation?: string }).ownerIncarnation;
    const relayed = fileRoot("session:88888888-8888-4888-8888-888888888888");
    const presence = filePresence(clock, [legacy, noIncarnation, relayed, fileRoot(allowed)]);
    presence.hosts[2] = { ...presence.hosts[2]!, record: { ...presence.hosts[2]!.record, remoteHost: "spoke", bridgeVia: "hub" } };
    const nonCanonical = fileRoot("main-session");
    presence.hosts.push(fileHost("main-session", clock.wall, clock.wall + 60_000));
    presence.participants.push(nonCanonical);
    expect(await side.mirror(presence)).toEqual({ held: [], refused: [], withdrawn: [] });
    expect(w.presence.values.size).toBe(0);
    expect(w.leases.values.size).toBe(0);
    expect(log.filter(line => line.includes("not carried"))).toHaveLength(2);
  });

  it("caps KV authority at the file sample: renewals stop at min(expiresAt, observedAt + 15 s)", async () => {
    const w = world();
    const { side, clock } = bridge(w);
    await side.whenReady();
    const presence = filePresence(clock, [fileRoot()]);
    presence.hosts[0] = fileHost(FILE_ROOT, clock.wall, clock.wall + 9_000);
    await side.mirror(presence);
    const first = w.leases.values.get(hKey(BRIDGE, FILE_ROOT))!.revision;
    advance(clock, 5_000);
    await side.writer.renew();
    const renewed = w.leases.values.get(hKey(BRIDGE, FILE_ROOT))!;
    expect(renewed.revision).toBeGreaterThan(first);
    expect((renewed.value as FabricKvHostLease).updatedAt).toBe(clock.wall);
    advance(clock, 4_000);
    await side.writer.renew();
    // The sample's own deadline passed: the lease is withdrawn, so no receiver routes to it.
    expect(w.leases.values.has(hKey(BRIDGE, FILE_ROOT))).toBe(false);
    expect(side.holds(FILE_ROOT)).toBe(false);
    expect(w.presence.values.has(pKey(BRIDGE, FILE_ROOT))).toBe(true);
    // A fresh sample reacquires it.
    await side.mirror(filePresence(clock, [fileRoot()]));
    expect(side.holds(FILE_ROOT)).toBe(true);
    // A long native lease still gets at most the 15 s mirror bound.
    advance(clock, 15_000);
    await side.writer.renew();
    expect(side.holds(FILE_ROOT)).toBe(false);
  });

  it("does not claim an already expired sample", async () => {
    const w = world();
    const { side, clock } = bridge(w);
    await side.whenReady();
    const presence = filePresence(clock, [fileRoot()]);
    presence.hosts[0] = fileHost(FILE_ROOT, clock.wall - 20_000, clock.wall + 60_000);
    expect(await side.mirror(presence)).toEqual({ held: [], refused: [], withdrawn: [] });
    expect(w.leases.values.size).toBe(0);
  });

  it("withdraws an omitted root on a complete snapshot and keeps it on a partial one", async () => {
    const w = world();
    const { side, clock } = bridge(w);
    await side.whenReady();
    await side.mirror(filePresence(clock, [fileRoot()]));
    expect(await side.mirror(filePresence(clock, [], { partial: true }))).toEqual({ held: [], refused: [], withdrawn: [] });
    expect(side.holds(FILE_ROOT)).toBe(true);
    expect(await side.mirror(filePresence(clock, [], { droppedCount: 1 }))).toEqual({ held: [], refused: [], withdrawn: [] });
    expect(side.holds(FILE_ROOT)).toBe(true);
    expect(await side.mirror(filePresence(clock, []))).toEqual({ held: [], refused: [], withdrawn: [FILE_ROOT] });
    expect(w.presence.values.size).toBe(0);
    expect(w.leases.values.size).toBe(0);
  });

  it("refuses an id that already has a row on another machine, and never touches that row", async () => {
    const w = world();
    seedNative(w, V2, natsRoot(FILE_ROOT, "v2-inc"), 1_000_000);
    const { side, clock } = bridge(w);
    await side.whenReady();
    expect(await side.mirror(filePresence(clock, [fileRoot()]))).toEqual({ held: [],
      refused: [{ id: FILE_ROOT, code: "FABRIC_BRIDGE_NATS_ID_ON_ANOTHER_MACHINE" }], withdrawn: [] });
    expect(w.presence.values.has(pKey(BRIDGE, FILE_ROOT))).toBe(false);
    expect(w.leases.values.has(hKey(BRIDGE, FILE_ROOT))).toBe(false);
    expect(side.resolve(FILE_ROOT)).toMatchObject({ machine: V2, live: true });
  });

  it("adopts a lease row only from this process or a provably exited predecessor", async () => {
    const w = world();
    const owner = `${FILE_ROOT}#file-inc-1`;
    w.leases.put(hKey(BRIDGE, FILE_ROOT), owner, { ...natsLease(FILE_ROOT, "file-inc-1", 990_000), ...OTHER_PROOF }, 0);
    let exited = false;
    const { side, clock } = bridge(w, { predecessorExited: proof => { expect(proof).toEqual(OTHER_PROOF); return exited; } });
    await side.whenReady();
    expect((await side.mirror(filePresence(clock, [fileRoot()]))).refused)
      .toEqual([{ id: FILE_ROOT, code: "FABRIC_BRIDGE_NATS_LEASE_CONFLICT" }]);
    expect((w.leases.values.get(hKey(BRIDGE, FILE_ROOT))!.value as FabricKvHostLease).pid).toBe(OTHER_PROOF.pid);
    exited = true;
    expect((await side.mirror(filePresence(clock, [fileRoot()]))).held).toEqual([FILE_ROOT]);
    expect((w.leases.values.get(hKey(BRIDGE, FILE_ROOT))!.value as FabricKvHostLease).pid).toBe(PROOF.pid);
  });

  it("never adopts a row with another owner incarnation, even from this process", async () => {
    const w = world();
    w.leases.put(hKey(BRIDGE, FILE_ROOT), `${FILE_ROOT}#file-inc-0`, { ...natsLease(FILE_ROOT, "file-inc-0", 990_000), ...PROOF }, 0);
    const { side, clock } = bridge(w, { predecessorExited: () => true });
    await side.whenReady();
    expect((await side.mirror(filePresence(clock, [fileRoot()]))).refused)
      .toEqual([{ id: FILE_ROOT, code: "FABRIC_BRIDGE_NATS_LEASE_CONFLICT" }]);
  });

  it("moves to a restarted file root's new incarnation and never accepts the superseded one again", async () => {
    const w = world();
    const { side, clock } = bridge(w);
    await side.whenReady();
    await side.mirror(filePresence(clock, [fileRoot()]));
    expect((await side.mirror(filePresence(clock, [fileRoot(FILE_ROOT, "file-inc-2")]))).held).toEqual([FILE_ROOT]);
    expect(w.leases.values.get(hKey(BRIDGE, FILE_ROOT))!.owner).toBe(`${FILE_ROOT}#file-inc-2`);
    expect(w.presence.values.get(pKey(BRIDGE, FILE_ROOT))!.owner).toBe(`${FILE_ROOT}#file-inc-2`);
    const receiver = await receiverView(w);
    expect(receiver.resolve(FILE_ROOT)).toMatchObject({ ownerIncarnation: "file-inc-2", live: true });
    const stale = await side.mirror(filePresence(clock, [fileRoot()]));
    expect(stale.refused).toEqual([{ id: FILE_ROOT, code: "FABRIC_BRIDGE_NATS_STALE_INCARNATION" }]);
    expect(stale.withdrawn).toEqual([FILE_ROOT]);
    await waitFor(() => expect(receiver.resolve(FILE_ROOT)).toBeUndefined());
  });

  it("settles an uncertain write by reading it back: a stored identical row is adopted", async () => {
    const w = world();
    const { side, clock } = bridge(w);
    await side.whenReady();
    w.presence.failNextPut = () => "uncertain-stored";
    expect((await side.mirror(filePresence(clock, [fileRoot()]))).held).toEqual([FILE_ROOT]);
    expect(side.holds(FILE_ROOT)).toBe(true);
    const revision = w.presence.values.get(pKey(BRIDGE, FILE_ROOT))!.revision;
    // A content change then CASes the adopted revision.
    await side.mirror(filePresence(clock, [fileRoot(FILE_ROOT, "file-inc-1", { status: "idle" })]));
    expect(w.presence.values.get(pKey(BRIDGE, FILE_ROOT))!.revision).toBeGreaterThan(revision);
  });

  it("reports an uncertain write that did not land and retries it on the next pass", async () => {
    const w = world();
    const { side, clock } = bridge(w);
    await side.whenReady();
    w.presence.failNextPut = () => "uncertain-lost";
    expect((await side.mirror(filePresence(clock, [fileRoot()]))).refused)
      .toEqual([{ id: FILE_ROOT, code: "FABRIC_TRANSPORT_UNCERTAIN" }]);
    expect((await side.mirror(filePresence(clock, [fileRoot()]))).held).toEqual([FILE_ROOT]);
  });

  it("rewrites presence only on a content change or the refresh deadline, not on updatedAt alone", async () => {
    const w = world();
    const { side, clock } = bridge(w);
    await side.whenReady();
    await side.mirror(filePresence(clock, [fileRoot()]));
    const revision = w.presence.values.get(pKey(BRIDGE, FILE_ROOT))!.revision;
    advance(clock, 1_000);
    await side.mirror(filePresence(clock, [fileRoot(FILE_ROOT, "file-inc-1", { updatedAt: clock.wall })]));
    expect(w.presence.values.get(pKey(BRIDGE, FILE_ROOT))!.revision).toBe(revision);
    advance(clock, 180_000);
    await side.mirror(filePresence(clock, [fileRoot()]));
    expect(w.presence.values.get(pKey(BRIDGE, FILE_ROOT))!.revision).toBeGreaterThan(revision);
  });

  it("stops renewing a lease another writer took, without deleting it", async () => {
    const w = world();
    const { side, clock } = bridge(w);
    await side.whenReady();
    await side.mirror(filePresence(clock, [fileRoot()]));
    const key = hKey(BRIDGE, FILE_ROOT);
    w.leases.put(key, `${FILE_ROOT}#file-inc-1`, { ...natsLease(FILE_ROOT, "file-inc-1", clock.wall), ...OTHER_PROOF },
      w.leases.values.get(key)!.revision);
    await expect(side.writer.renew()).rejects.toMatchObject({ code: "FABRIC_KV_CONFLICT" });
    expect(side.holds(FILE_ROOT)).toBe(false);
    expect((w.leases.values.get(key)!.value as FabricKvHostLease).pid).toBe(OTHER_PROOF.pid);
  });

  it("fences writes, then withdraws its own rows leases first", async () => {
    const w = world();
    const { side, clock } = bridge(w);
    await side.whenReady();
    await side.mirror(filePresence(clock, [fileRoot()]));
    await side.fence();
    await expect(side.mirror(filePresence(clock, [fileRoot()]))).rejects.toBeInstanceOf(NatsMirrorFencedError);
    const before = w.leases.values.get(hKey(BRIDGE, FILE_ROOT))!.revision;
    await side.writer.renew();
    expect(w.leases.values.get(hKey(BRIDGE, FILE_ROOT))!.revision).toBe(before);
    const order: string[] = [];
    const leasesDelete = w.leases.delete.bind(w.leases), presenceDelete = w.presence.delete.bind(w.presence);
    w.leases.delete = (key, revision) => { order.push("lease"); return leasesDelete(key, revision); };
    w.presence.delete = (key, revision) => { order.push("presence"); return presenceDelete(key, revision); };
    await side.withdraw();
    expect(order).toEqual(["lease", "presence"]);
    expect(w.leases.values.size + w.presence.values.size).toBe(0);
  });
});
