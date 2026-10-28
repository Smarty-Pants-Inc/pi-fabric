// Keep reason: reported expired-projection churn; exercise real mirror/outbox/file boundaries.
// All writes and fixture cleanup stay within each test's temporary store.
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { BRIDGE_LEASE_MS, BridgeStaleIncarnationError, StoreBridgeSide, type BridgeHost } from "../src/mesh/bridge.js";
import { CommitOutbox } from "../src/mesh/commit-outbox.js";
import { MeshStore } from "../src/mesh/store.js";
import { hostLeaseDirectoryStamp, hostLeasePath, hostLeasesStamp, mirroredOwnerRoutingStatus,
  readHostLeaseCurrent, writeHostLease } from "../src/topology/host-leases.js";
import { NatsLocalProjection } from "../src/topology/nats-local-projection.js";
import type { FabricTransport, TransportKv, TransportKvEntry } from "../src/transport/types.js";

const ID = "session:11111111-1111-4111-8111-111111111111";
const PEER = "noop-source";
const identity = { id: ID, name: "remote-root", kind: "main" as const };
const hostKey = "topology/hosts/" + createHash("sha256").update(ID).digest("hex");
const stores: MeshStore[] = [];
const roots: string[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  for (const store of stores.splice(0)) store.closeState();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

async function fixture(sampled = true) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-lease-noop-"));
  roots.push(root);
  const store = new MeshStore(root, 64 * 1024, 100, { stateBackend: "file" });
  stores.push(store);
  let now = 1_800_000_000_000;
  const side = new StoreBridgeSide(store, PEER, () => now);
  const source: BridgeHost = { record: { format: 1, id: ID, rootId: ID, identity,
    startedAt: now - 60_000, incarnationToken: "owner-A", updatedAt: now - 100,
    expiresAt: now + 7_000 }, expiresAt: now + 7_000, ...(sampled ? { observedAt: now } : {}) };
  const presence = () => ({ hosts: [source], participants: [] });
  await side.mirror(presence(), now);
  return { root, store, side, source, presence, now: () => now, advance: (ms: number) => { now += ms; },
    lease: () => readHostLeaseCurrent(root, ID)!, file: hostLeasePath(root, ID) };
}
type Fixture = Awaited<ReturnType<typeof fixture>>;
function snapshot(f: Fixture) {
  const stat = fs.statSync(f.file, { bigint: true });
  const directory = fs.statSync(path.dirname(f.file), { bigint: true });
  return { bytes: fs.readFileSync(f.file, "utf8"), ino: stat.ino.toString(), mtimeNs: stat.mtimeNs.toString(),
    directory: { ino: directory.ino.toString(), mtimeNs: directory.mtimeNs.toString(), ctimeNs: directory.ctimeNs.toString() },
    hostLeasesStamp: hostLeasesStamp(f.root), planStamp: hostLeaseDirectoryStamp(f.root) };
}
function measure(f: Fixture) {
  const writes = vi.spyOn(fs, "writeFileSync");
  const renames = vi.spyOn(fs, "renameSync");
  return { counts: () => ({ writes: writes.mock.calls.filter(([file]) => String(file) === f.file || String(file).startsWith(f.file + ".")).length,
    renames: renames.mock.calls.filter(([, to]) => String(to) === f.file).length }),
    reset: () => { writes.mockClear(); renames.mockClear(); } };
}
function evidence(label: string, f: Fixture, before: ReturnType<typeof snapshot>, measured: ReturnType<typeof measure>) {
  const after = snapshot(f);
  const { bytes: _beforeBytes, ...beforeMetadata } = before;
  const { bytes: _afterBytes, ...afterMetadata } = after;
  console.info("LEASE_NOOP_TRACE " + JSON.stringify({ label, root: f.root, ...measured.counts(),
    equalBytes: after.bytes === before.bytes, inodeUnchanged: after.ino === before.ino,
    mtimeUnchanged: after.mtimeNs === before.mtimeNs,
    directoryUnchanged: JSON.stringify(after.directory) === JSON.stringify(before.directory),
    stampUnchanged: after.hostLeasesStamp === before.hostLeasesStamp, before: beforeMetadata, after: afterMetadata }));
}
function expectNoWrite(f: Fixture, before: ReturnType<typeof snapshot>, measured: ReturnType<typeof measure>) {
  expect(measured.counts()).toEqual({ writes: 0, renames: 0 });
  expect(snapshot(f)).toEqual(before);
}

describe("expired mirrored lease compare-before-write trace", () => {
  it("12 complete mirror passes retain the identical expired sampled projection without filesystem churn", async () => {
    const f = await fixture();
    f.advance(BRIDGE_LEASE_MS + 1);
    expect(f.lease().expiresAt).toBeLessThan(f.now());
    expect(f.lease().writer).toBeUndefined();
    expect(mirroredOwnerRoutingStatus({ ...f.lease(), remoteHost: PEER }, f.now())).toBe("bridge-stale");
    const before = snapshot(f), measured = measure(f);
    for (let n = 0; n < 12; n++) {
      f.advance(1_000);
      await f.side.mirror(f.presence(), f.now()); // Full, not partial, with a fresh receiver anchor.
    }
    evidence("expired-sampled-12-full-passes", f, before, measured);
    expectNoWrite(f, before, measured);
  });

  it("a restart replays the real durable install outbox row without rewriting its expired lease", async () => {
    const f = await fixture();
    const outbox = new CommitOutbox(f.store, `bridge/${PEER}`, { id: `bridge:${PEER}`, name: PEER, kind: "main" }, {});
    const rowKey = outbox.rowKey(`lease:${ID}`);
    const row = f.store.get(rowKey, { fresh: true });
    expect(row).toBeDefined();
    expect(row!.value).toMatchObject({ kind: "lease", payload: { incarnationToken: "owner-A" } });
    f.advance(BRIDGE_LEASE_MS + 1);
    const before = snapshot(f), measured = measure(f);
    const custody = vi.spyOn(f.store, "leaseCustody");
    const restarted = new StoreBridgeSide(f.store, PEER, f.now);
    await restarted.mirror(f.presence(), f.now());
    expect(custody.mock.calls.filter(([file]) => file === f.file).length).toBeGreaterThanOrEqual(2); // replay + planned effect
    evidence("expired-durable-replay", f, before, measured);
    expectNoWrite(f, before, measured);
  });

  it("12 legacy unsampled passes with an identical receiver anchor also skip writes", async () => {
    const f = await fixture(false);
    const anchor = f.now();
    const before = snapshot(f), measured = measure(f);
    for (let n = 0; n < 12; n++) await f.side.mirror(f.presence(), anchor);
    evidence("legacy-12-full-passes", f, before, measured);
    expectNoWrite(f, before, measured);
  });

  it("12 identical valid watch renewals skip writes, then a changed sample writes exactly once", async () => {
    const f = await fixture();
    const before = snapshot(f), measured = measure(f);
    for (let n = 0; n < 12; n++) await f.side.renewMirroredLeases([f.source], f.now());
    evidence("watch-identical-12", f, before, measured);
    expectNoWrite(f, before, measured);
    f.advance(1_000);
    const renewed = { ...f.source, observedAt: f.now(), expiresAt: f.now() + 7_000,
      record: { ...f.source.record, updatedAt: f.now() } };
    await f.side.renewMirroredLeases([renewed], f.now());
    expect(measured.counts()).toEqual({ writes: 1, renames: 1 });
    expect(f.lease().mirror?.sourceObservedAt).toBe(f.now());
    expect(f.lease().expiresAt).toBe(f.now() + 7_000);
    measured.reset();
    const after = snapshot(f);
    await f.side.renewMirroredLeases([renewed], f.now());
    expectNoWrite(f, after, measured);
  });

  it("expired watch samples and fresh samples naming a changed identity/incarnation correctly refuse", async () => {
    const f = await fixture();
    f.advance(BRIDGE_LEASE_MS + 1);
    const before = snapshot(f), measured = measure(f);
    await f.side.renewMirroredLeases([f.source], f.now());
    for (const record of [
      { ...f.source.record, incarnationToken: "owner-B" },
      { ...f.source.record, identity: { ...identity, id: "session:22222222-2222-4222-8222-222222222222" } },
    ]) await f.side.renewMirroredLeases([{ record, observedAt: f.now(), expiresAt: f.now() + 7_000 }], f.now());
    expectNoWrite(f, before, measured);
  });

  it.each(["sample", "expiry", "updatedAt"] as const)("changed %s must write even when the old projection is expired", async field => {
    const f = await fixture();
    f.advance(BRIDGE_LEASE_MS + 1);
    if (field === "sample") f.source.observedAt! += 1;
    if (field === "expiry") f.source.expiresAt += 1;
    if (field === "updatedAt") f.source.record.updatedAt += 1;
    const before = snapshot(f), measured = measure(f);
    await f.side.mirror(f.presence(), f.now());
    evidence(`changed-${field}`, f, before, measured);
    expect(measured.counts()).toEqual({ writes: 1, renames: 1 });
    expect(snapshot(f).bytes).not.toBe(before.bytes);
  });

  it("changed locally proven superseded history is repaired once, not skipped by unchanged timestamps", async () => {
    const f = await fixture();
    f.advance(BRIDGE_LEASE_MS + 1);
    const held = f.store.get(hostKey, { fresh: true })!;
    await f.store.put({ key: hostKey, identity, value: { ...(held.value as object),
      bridgeSupersededTokens: ["prior-owner"], bridgeSupersededTokenSeen: { "prior-owner": 0 } } });
    const before = snapshot(f), measured = measure(f);
    await f.side.mirror(f.presence(), f.now());
    expect(measured.counts()).toEqual({ writes: 1, renames: 1 });
    expect(f.lease()).toMatchObject({ bridgeSupersededTokens: ["prior-owner"], bridgeSupersededTokenSeen: { "prior-owner": 0 } });
    measured.reset();
    const repaired = snapshot(f);
    for (let n = 0; n < 4; n++) await f.side.mirror(f.presence(), f.now());
    evidence("history-repair-then-4-noops", f, repaired, measured);
    expectNoWrite(f, repaired, measured);
    expect(repaired.bytes).not.toBe(before.bytes);
  });

  it("a full mirror writes a new incarnation once and then refuses the proven older incarnation", async () => {
    const f = await fixture();
    f.advance(BRIDGE_LEASE_MS + 1);
    const old = structuredClone(f.source);
    f.source.record = { ...f.source.record, incarnationToken: "owner-B", startedAt: f.now() };
    const measured = measure(f);
    await f.side.mirror(f.presence(), f.now());
    expect(measured.counts()).toEqual({ writes: 1, renames: 1 });
    expect(f.lease()).toMatchObject({ incarnationToken: "owner-B", bridgeSupersededTokens: ["owner-A"] });
    measured.reset();
    const before = snapshot(f);
    const restarted = new StoreBridgeSide(f.store, PEER, f.now);
    await expect(restarted.mirror({ hosts: [old], participants: [] }, f.now())).rejects.toBeInstanceOf(BridgeStaleIncarnationError);
    expectNoWrite(f, before, measured);
  });

  it("a live native lease claim is never replaced by an unchanged mirror/replay", async () => {
    const f = await fixture();
    writeHostLease(f.root, { ...f.lease(), incarnationToken: "native-owner", updatedAt: f.now(), expiresAt: f.now() + 60_000,
      session: { id: ID, startedAt: f.now(), updatedAt: f.now(), expiresAt: f.now() + 15_000 } });
    const before = snapshot(f), measured = measure(f);
    const restarted = new StoreBridgeSide(f.store, PEER, f.now);
    await restarted.mirror(f.presence(), f.now()); // Preservation need not reject the whole pass.
    expectNoWrite(f, before, measured);
  });

  it("12 distinct delayed samples legitimately write 12 already-expired no-writer projections", async () => {
    const f = await fixture();
    const before = snapshot(f), measured = measure(f);
    const uniqueContents = new Set<string>();
    for (let n = 0; n < 12; n++) {
      f.advance(1); // A strictly newer receiver observation than the installed sample.
      const anchor = f.now();
      f.source.observedAt! += 1; // New source observation, same source owner/update/expiry.
      f.advance(BRIDGE_LEASE_MS + 1); // Queue/commit completes after translated authority.
      await f.side.mirror(f.presence(), anchor);
      expect(f.lease().expiresAt).toBeLessThan(f.now());
      expect(f.lease().writer).toBeUndefined();
      uniqueContents.add(snapshot(f).bytes);
    }
    evidence("distinct-delayed-samples-12-expired-writes", f, before, measured);
    expect(uniqueContents.size).toBe(12);
    expect(measured.counts()).toEqual({ writes: 12, renames: 12 });
  });

  it("12 NATS snapshot restarts of the same expired KV revision do not rewrite the projected file", async () => {
    const f = await fixture();
    const natsId = "session:33333333-3333-4333-8333-333333333333";
    const entry: TransportKvEntry<unknown> = { machine: "noop-machine", key: "noop-machine.h." + createHash("sha256").update(natsId).digest("hex").slice(0, 32),
      owner: `${natsId}#nats-A`, revision: 1, value: { id: natsId, rootId: natsId, identityId: natsId,
        incarnation: "nats-A", pid: process.pid, pidStartTime: "1", bootId: "12345678-1234-4234-8234-123456789abc",
        updatedAt: f.now(), expiresAt: f.now() + 7_000 } };
    const bucket = (leases: boolean): TransportKv => ({
      async get() { return leases ? entry : undefined; },
      async *watch(_prefix: string, signal: AbortSignal) {
        if (leases) yield { type: "put", entry };
        yield { type: "ready" };
        if (!signal.aborted) await new Promise<void>(resolve => signal.addEventListener("abort", () => resolve(), { once: true }));
      },
    }) as unknown as TransportKv;
    const transport = { kind: "nats", self: { machine: entry.machine, hostId: "noop-adapter" },
      kv: (name: string) => bucket(name === "leases") } as unknown as FabricTransport;
    const run = async () => {
      const projection = new NatsLocalProjection({ mesh: f.store, transport, leaseLive: () => true, now: f.now });
      projection.start();
      try { await projection.whenReady(); return projection.metrics().hostLeaseWrites; }
      finally { await projection.close(); }
    };
    expect(await run()).toBe(1);
    f.advance(BRIDGE_LEASE_MS + 1);
    const target = { ...f, file: hostLeasePath(f.root, natsId) };
    const before = snapshot(target), measured = measure(target);
    for (let n = 0; n < 12; n++) expect(await run()).toBe(0);
    evidence("nats-expired-same-revision-12-restarts", target, before, measured);
    expectNoWrite(target, before, measured);
  });
});
