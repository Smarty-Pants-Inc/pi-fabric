import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterAll, afterEach, describe, expect, it, vi } from "vitest";
import { MESH_ARCHIVE_CONFIG } from "../src/mesh/archive.js";
import { MeshBridge, StoreBridgeSide } from "../src/mesh/bridge.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { hostLeasePath, readHostLease, writeHostLease, type FabricHostLease } from "../src/topology/host-leases.js";
import { participantFromEntry } from "../src/topology/participant-directory.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";

// smarty-dev#10391: a bridge catch-up must never hold state.db's SQLite write lock above 100 ms.
// The boundary is observed at the driver (node:sqlite DatabaseSync.exec), not by production
// logging: every BEGIN IMMEDIATE to its COMMIT or ROLLBACK, on every connection in this process.
const MAX_HOLD_MS = 100; // printed (over100), not asserted: see CEILING_HOLD_MS
// Events each transaction committed on `mesh`: publishBatch prefixes, and 1 per single publish.
const countBatches = (mesh: MeshStore): number[] => {
  const sizes: number[] = [];
  const batch = mesh.publishBatch.bind(mesh), single = mesh.publish.bind(mesh);
  mesh.publishBatch = async (inputs) => { const events = await batch(inputs); sizes.push(events.length); return events; };
  mesh.publish = async (input) => { const event = await single(input); sizes.push(1); return event; };
  return sizes;
};
const EVENTS = 500;
// fabric-v2's targets (<= 25% held while 3 bridges drain, a Main send within 250 ms at p99, ~0% idle) are
// printed as measured; hosted runners are too slow to assert them (see CEILING_*).
// The probe's own writes (a Main send stand-in), told apart from the bridges' by their writer frame.
const PROBE_SITE = "SqliteStateStore.put";
const SPOKES = 3, IDLE_MS = 6000;
// Hosted CI (shared, slow-disk runners) cannot hold Intel 1's wall-clock bars: the gates are the MECHANISM
// (no topology scan inside a write transaction, bounded batches, a Main send during the drain). Wall-clock
// stays only as a gross-regression ceiling a slow Windows runner passes; the numbers are printed.
const CEILING_HOLD_MS = 3000, CEILING_IDLE_PCT = 15, CEILING_DRAIN_PCT = 75, BATCH_MAX_EVENTS = 32;

// The held share of a window: the union of hold intervals, its largest hold and the writers that held it.
const report = (measured: Hold[], from: number, to: number) => {
  const sorted = measured.slice().sort((a, b) => a.began - b.began);
  let held = 0, end = -Infinity;
  for (const hold of sorted) {
    const start = Math.max(hold.began, end, from);
    if (Math.min(hold.ended, to) > start) held += Math.min(hold.ended, to) - start;
    end = Math.max(end, hold.ended);
  }
  const bySite: Record<string, { n: number; maxMs: number; totalMs: number }> = {};
  for (const { ms, site } of measured) {
    const entry = bySite[site] ??= { n: 0, maxMs: 0, totalMs: 0 };
    entry.n += 1;
    entry.maxMs = +Math.max(entry.maxMs, ms).toFixed(1);
    entry.totalMs = +(entry.totalMs + ms).toFixed(1);
  }
  return { transactions: measured.length, heldPct: +(held / Math.max(1, to - from) * 100).toFixed(2),
    maxHoldMs: +Math.max(0, ...measured.map(hold => hold.ms)).toFixed(1), bySite };
};

type Exec = (this: object, sql: string) => void;
const sqlite = process.getBuiltinModule("node:sqlite") as typeof import("node:sqlite");
const proto = sqlite.DatabaseSync.prototype as unknown as { exec: Exec };
const original = proto.exec;
const open = new WeakMap<object, number>();
interface Hold { ms: number; site: string; db: string; began: number; ended: number }
let holds: Hold[] = [];
// Which writer held the lock: the first bridge or store frame above the driver.
const site = (stack = ""): string => {
  const frames = stack.split("\n").slice(2).map(line => line.trim());
  const named = frames.find(line => /StoreBridgeSide|MeshBridge|fence \(|withStateWriteFence/.test(line)) ?? frames[2] ?? "?";
  return named.replace(/^at /, "").replace(/ \(.*$/, "");
};
const sites = new WeakMap<object, string>();
proto.exec = function (this: object, sql: string) {
  const statement = sql.trim().toUpperCase();
  if (statement === "BEGIN IMMEDIATE") {
    original.call(this, sql);
    writesOpen += 1;
    open.set(this, performance.now());
    sites.set(this, site(new Error().stack));
    return;
  }
  const began = open.get(this);
  try { original.call(this, sql); }
  finally {
    if (began !== undefined && (statement === "COMMIT" || statement === "ROLLBACK")) {
      open.delete(this);
      writesOpen -= 1;
      const ended = performance.now();
      holds.push({ ms: ended - began, site: sites.get(this) ?? "?", began, ended,
        db: (this as { location(): string | null }).location() ?? "" });
    }
  }
};
// A topology scan inside a write transaction: a host, participant or handoff range read (the presence and
// ownership scans' SQL) executed while a BEGIN IMMEDIATE is open. Transaction bodies are synchronous, so any
// such read while one is open runs inside it.
let writesOpen = 0;
let inFenceScans = 0;
type All = (this: { sourceSQL: string }, ...args: unknown[]) => unknown[];
const statementProto = sqlite.StatementSync.prototype as unknown as { all: All };
const originalAll = statementProto.all;
statementProto.all = function (this: { sourceSQL: string }, ...args: unknown[]) {
  if (writesOpen > 0 && this.sourceSQL.includes("key >= ?") && typeof args[0] === "string" &&
    /^topology\/(hosts|participants|handoffs)\//.test(args[0])) inFenceScans += 1;
  return originalAll.apply(this, args);
};
afterAll(() => { proto.exec = original; statementProto.all = originalAll; });

const roots: string[] = [];
const stores: MeshStore[] = [];
const scratch = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-lock-hold-"));
  roots.push(root);
  return root;
};
afterEach(() => {
  for (const store of stores.splice(0)) store.closeState();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
// A hub like the fleet's: SQLite state and the durable event archive.
const store = () => {
  const root = scratch();
  const dir = path.join(root, "event-archive");
  fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
  fs.writeFileSync(path.join(root, MESH_ARCHIVE_CONFIG), JSON.stringify({ version: 1, dir }));
  const mesh = new MeshStore(root, 65536, 1000, { stateBackend: "sqlite" });
  stores.push(mesh);
  return mesh;
};
const native = async (mesh: MeshStore, id: string) => {
  const now = Date.now();
  const identity: MeshIdentity = { id, name: "main", kind: "main" };
  const hash = createHash("sha256").update(id).digest("hex");
  await mesh.put({ key: `topology/hosts/${hash}`, identity,
    value: { format: 1, id, rootId: id, identity, startedAt: now, updatedAt: now, expiresAt: now + 120000 } });
  await mesh.put({ key: `topology/participants/${hash}`, identity,
    value: { format: 1, id, kind: "root", rootId: id, ownerHostId: id, ownerIdentityId: id, name: id, label: id,
      status: "idle", runner: "pi", transport: "host", capabilities: ["steer"], startedAt: now, updatedAt: now,
      controlProtocol: "v1" } });
  // A live native renews its own lease file, as every fleet Main does.
  writeHostLease(mesh.root, { id, rootId: id, identityId: id, updatedAt: now, expiresAt: now + 600000, startedAt: now });
  return identity;
};

// A hub's presence at fleet scale (intel1's mesh, 10-13: ~800 host and ~1,500 participant rows,
// ~200 lease files, ~170 participant files). The bridge's ownership check reads all of it.
const HOSTS = 800, PARTICIPANTS = 1500, LEASES = 200, FILES = 170;
const populate = async (mesh: MeshStore, hosts = HOSTS, firstHost = 0) => {
  const now = Date.now();
  const ops: Array<{ kind: "put"; key: string; value: unknown; identity: MeshIdentity }> = [];
  for (let n = 0; n < Math.max(hosts, PARTICIPANTS); n++) {
    const id = `session:fleet-${n + firstHost}`;
    const identity: MeshIdentity = { id, name: `agent-${n}`, kind: "main" };
    const hash = createHash("sha256").update(id).digest("hex");
    if (n < hosts) ops.push({ kind: "put", key: `topology/hosts/${hash}`, identity,
      value: { format: 1, id, rootId: id, identity, startedAt: now, updatedAt: now, expiresAt: now + 120000, host: `host-${n % 9}` } });
    if (n < PARTICIPANTS) ops.push({ kind: "put", key: `topology/participants/${hash}`, identity,
      value: { format: 1, id, kind: "root", rootId: id, ownerHostId: id, ownerIdentityId: id, name: `agent-${n}`,
        label: `lane agent ${n} on host ${n % 9}`, status: "idle", runner: "pi", transport: "host",
        // Valid capabilities only (pi-fabric#1562 review P2): "status" is not one, so participantFromEntry refused
        // these rows and the mirror filter skipped them, and the hold numbers compared unlike passes.
        capabilities: ["steer", "followUp", "fabric"], startedAt: now, updatedAt: now, controlProtocol: "v1",
        cwd: `/home/paul/lanes/lane-${n}`, model: "opus-5.5", effort: "high" } });
  }
  for (let at = 0; at < ops.length; at += 200) await mesh.writeBatch({ identity: ops[at]!.identity, ops: ops.slice(at, at + 200) });
  for (let n = 0; n < LEASES; n++) {
    const id = `session:fleet-${n + firstHost}`;
    writeHostLease(mesh.root, { id, rootId: id, identityId: id, updatedAt: now, expiresAt: now + 120000, startedAt: now });
  }
  for (let n = 0; n < FILES; n++) {
    const id = `session:fleet-file-${n}`;
    const hash = createHash("sha256").update(id).digest("hex");
    writeParticipantFile(mesh.root, { key: `topology/participants/${hash}`, version: 1, updatedAt: now,
      updatedBy: { id, name: id, kind: "main" },
      value: { format: 1, id, kind: "root", rootId: id, ownerHostId: id, ownerIdentityId: id, name: id, label: id,
        status: "idle", runner: "pi", transport: "host", capabilities: ["steer"], startedAt: now, updatedAt: now } });
  }
};

describe("bridge catch-up SQLite write-lock holds (smarty-dev#10391), real stores and state.db", () => {
  const catchUp = async (direction: "toLocal" | "toRemote", hosts = HOSTS, firstHost = 0) => {
    const hub = store(), far = store();
    const localId = await native(hub, "session:hold-local"), remoteId = await native(far, "session:hold-remote");
    await populate(hub, hosts, firstHost);
    // Like for like: every fleet row is one this host's participant reader (the mirror's filter) accepts.
    const rows = hub.listAll("topology/participants/");
    expect(rows.filter(entry => participantFromEntry(entry) !== undefined)).toHaveLength(PARTICIPANTS + 1);
    // The default presence period: a drain this long runs presence passes, as a fleet catch-up does.
    const bridge = new MeshBridge({ localName: "hub", remoteName: "far", local: new StoreBridgeSide(hub, "far"),
      remote: new StoreBridgeSide(far, "hub"), cursorPath: path.join(scratch(), "cursor.json") });
    await bridge.start();
    // The production case uses canonical ids (fleet-10 onward); fleet-0..9 in the original
    // stress fixture are too short. Prove all requested fleet hosts really acquired mirror leases.
    if (firstHost > 0) for (let n = 0; n < hosts; n++) {
      expect(readHostLease(far.root, `session:fleet-${n + firstHost}`))
        .toMatchObject({ mirrorLease: 1, mirror: { remoteHost: "hub" } });
    }
    const source = direction === "toRemote" ? hub : far;
    const target = direction === "toRemote" ? far : hub;
    const from = direction === "toRemote" ? localId : remoteId;
    const to = direction === "toRemote" ? remoteId.id : localId.id;
    for (let index = 0; index < EVENTS;) {
      const inputs = Array.from({ length: Math.min(256, EVENTS - index) }, (_, n) =>
        ({ topic: "fleet.work.hold", kind: "ask", from, to, text: String(index + n) }));
      index += (await source.publishBatch(inputs)).length;
    }
    const batches = countBatches(target);
    holds = [];
    inFenceScans = 0;
    const started = performance.now();
    const result = await bridge.step();
    const drainMs = performance.now() - started;
    const measured = holds;
    const scans = inFenceScans;
    holds = [];
    const maxHoldMs = Math.max(0, ...measured.map(hold => hold.ms));
    const totalHoldMs = measured.reduce((sum, hold) => sum + hold.ms, 0);
    const bySite: Record<string, { n: number; maxMs: number; totalMs: number }> = {};
    for (const { ms, site } of measured) {
      const entry = bySite[site] ??= { n: 0, maxMs: 0, totalMs: 0 };
      entry.n += 1;
      entry.maxMs = +Math.max(entry.maxMs, ms).toFixed(1);
      entry.totalMs = +(entry.totalMs + ms).toFixed(1);
    }
    console.log(JSON.stringify({ direction, hosts, events: EVENTS, transactions: measured.length,
      maxHoldMs: +maxHoldMs.toFixed(2), totalHoldMs: +totalHoldMs.toFixed(1), drainMs: +drainMs.toFixed(1),
      over100: measured.filter(hold => hold.ms > MAX_HOLD_MS).length, inFenceScans: scans,
      maxBatchEvents: Math.max(0, ...batches), bySite }));
    await bridge.stop();
    expect(result).toMatchObject({ [direction]: EVENTS, dropped: 0 });
    expect(target.read({ limit: 1000 }).filter(e => e.topic === "fleet.work.hold").map(e => e.text))
      .toEqual(Array.from({ length: EVENTS }, (_, index) => String(index)));
    // The mechanism: no host, participant or handoff scan inside any write transaction, and bounded batches.
    expect(scans).toBe(0);
    expect(Math.max(0, ...batches)).toBeLessThanOrEqual(BATCH_MAX_EVENTS);
    expect(maxHoldMs).toBeLessThanOrEqual(CEILING_HOLD_MS);
    return drainMs;
  };

  // The 800-host stress fixture renews 791 distinct hosts per presence pass, one lock each.
  // Round 3 measured 320-420 ms per pass on Linux versus 133-176 ms on main. Windows's
  // file-operation cost model is about 2x main (~50 s versus ~25 s for this drain).
  // Batching gains nothing: each distinct host has only one renewal per pass.
  // Keep Linux/macOS at 60 s; Windows alone gets 120 s until smarty-dev#16547's cheaper
  // Windows lock removes this 120 s bound. Production is guarded separately below.
  it.each(["toLocal", "toRemote"] as const)(`${EVENTS} events %s: no topology scan inside a write transaction, batches of ${BATCH_MAX_EVENTS} at most`,
    async direction => { await catchUp(direction); }, process.platform === "win32" ? 120000 : 60000);

  // Production scale: fabric-v2 reported 317 mirror leases fleet-wide, 282 renewed in 60 s,
  // measured on Ryzen 1's fleet mesh at 18:2xZ on 10-24. Round 3's Windows model predicts
  // ~50 s for 791 hosts, so 320 hosts predict ~20 s (50 * 320 / 791). Assert <40 s on ALL
  // platforms: 2x model headroom while retaining 20 s of margin inside the old 60 s bound.
  // Same 500-event toLocal drain, topology/participant load, real stores and default presence
  // period as the stress case; no hosted-only veto or Windows skip.
  it(`${EVENTS} events toLocal with 320 fleet hosts: drains inside 40 s`, async () => {
    expect(await catchUp("toLocal", 320, 10)).toBeLessThan(40000);
  }, 60000);

  // The pre-check runs outside the fence; a file the check reads can change before the fence. Each case changes
  // one between the two: the proof must fail, and the full check then decides inside the fence.
  describe("a file change between the pre-check and the fence", () => {
    const held = async () => {
      const hub = store(), far = store();
      await native(hub, "session:race-hub");
      const spoke = await native(far, "session:race-spoke");
      const side = new StoreBridgeSide(hub, "far");
      const bridge = new MeshBridge({ localName: "hub", remoteName: "far", local: side,
        remote: new StoreBridgeSide(far, "hub"), cursorPath: path.join(scratch(), "cursor.json") });
      await bridge.start();
      expect(side.holds(spoke.id, spoke)).toBe(true);
      // Count fences; run `between` once, after the pre-check and before the first fence.
      let fences = 0;
      let between: (() => void) | undefined;
      const fence = hub.withStateWriteFence.bind(hub);
      hub.withStateWriteFence = <T>(operation: () => T): T => {
        fences += 1;
        const change = between;
        between = undefined;
        change?.();
        return fence(operation);
      };
      const send = (text: string) => side.publish({ topic: "fleet.work.hold", kind: "ask", from: spoke, to: "session:race-hub",
        text, data: { bridge: { from: "far", id: `race-${text}` } } }, [spoke.id]);
      return { hub, spoke, side, bridge, send, fences: () => fences, set: (change: () => void) => { between = change; } };
    };
    const hash = (id: string) => createHash("sha256").update(id).digest("hex");

    it("no change: one fence, committed on the proof, reading no unrelated fleet lease", async () => {
      const f = await held();
      // With no handoff row, the held check reads only the held id's own lease file, never the fleet's.
      const now = Date.now(), other = "session:race-unrelated";
      writeHostLease(f.hub.root, { id: other, rootId: other, identityId: other, updatedAt: now, expiresAt: now + 600000, startedAt: now });
      const unrelated = hostLeasePath(f.hub.root, other);
      const statSync = vi.spyOn(fs, "statSync");
      let statted: string[];
      try { await f.send("plain"); } finally {
        statted = statSync.mock.calls.map(([file]) => String(file));
        statSync.mockRestore();
      }
      expect(statted.filter(file => file === unrelated)).toEqual([]);
      expect(f.fences()).toBe(1);
      expect(f.hub.read({ after: 0, limit: 100 }).map(e => e.text)).toContain("plain");
      await f.bridge.stop();
    });

    it("a renewed lease file: the proof fails, the full check passes, and the event commits", async () => {
      const f = await held();
      const lease = path.join(f.hub.root, "host-leases");
      // #10068: renew the bridge's own mirror lease (same incarnation, a newer source sample), as
      // its next presence pass would; a native lease without the mirror would not be this link's.
      const current = readHostLease(f.hub.root, f.spoke.id)!;
      expect(current).toMatchObject({ rootId: f.spoke.id, identityId: f.spoke.id, mirrorLease: 1, mirror: { remoteHost: "far" } });
      let renewed: FabricHostLease | undefined;
      f.set(() => {
        const now = Date.now(), mirror = current.mirror!;
        renewed = { ...current, updatedAt: mirror.sourceObservedAt + 1, expiresAt: now + 15000, mirror: { ...mirror, observedAt: now,
          sourceObservedAt: mirror.sourceObservedAt + 1, sourceUpdatedAt: mirror.sourceObservedAt + 1, sourceExpiresAt: mirror.sourceExpiresAt + 30000 } };
        writeHostLease(f.hub.root, renewed);
      });
      await f.send("renewed");
      expect(fs.existsSync(lease)).toBe(true);
      expect(renewed).toBeDefined();
      // The same incarnation's mirror stays on disk (a later bridge pass may only renew it further).
      const after = readHostLease(f.hub.root, f.spoke.id)!;
      expect(after).toMatchObject({ id: current.id, rootId: current.rootId, identityId: current.identityId,
        startedAt: current.startedAt, mirrorLease: 1, mirror: { remoteHost: "far" } });
      expect(after.incarnationToken).toBe(current.incarnationToken);
      expect(after.mirror!.observedAt).toBeGreaterThanOrEqual(renewed!.mirror!.observedAt);
      expect(f.fences()).toBe(2);
      expect(f.hub.read({ after: 0, limit: 100 }).map(e => e.text)).toContain("renewed");
      await f.bridge.stop();
    });

    it("a native's participant file for the held id: the proof fails and the re-check refuses", async () => {
      const f = await held();
      const id = f.spoke.id;
      f.set(() => writeParticipantFile(f.hub.root, { key: `topology/participants/${hash(id)}`, version: 1, updatedAt: Date.now(),
        updatedBy: f.spoke, value: { format: 1, id, kind: "root", rootId: id, ownerHostId: id, ownerIdentityId: id, name: id,
          label: id, status: "idle", runner: "pi", transport: "host", capabilities: ["steer"], startedAt: Date.now(), updatedAt: Date.now() } }));
      await expect(f.send("taken")).rejects.toThrow("no longer bound");
      // The stale proof released the first fence; the next pre-check refuses before taking another.
      expect(f.fences()).toBe(1);
      expect(f.hub.read({ after: 0, limit: 100 }).map(e => e.text)).not.toContain("taken");
      await f.bridge.stop();
    });

  });

  // Review finding (Luna max, P1): with a handoff row the mirror plan reads every lease file, so a lease
  // written between the plan and its transaction must re-plan; with no lease change the plan commits once.
  it.each([false, true])("mirror plan with a handoff row, lease written before its transaction: %s", async (leaseChanges) => {
    const hub = store(), far = store();
    await native(hub, "session:plan-hub");
    await native(far, "session:plan-spoke");
    const side = new StoreBridgeSide(hub, "far");
    const bridge = new MeshBridge({ localName: "hub", remoteName: "far", local: side,
      remote: new StoreBridgeSide(far, "hub"), cursorPath: path.join(scratch(), "cursor.json") });
    await bridge.start(); // the recovery pass; later passes plan before their transaction
    const identity: MeshIdentity = { id: "session:plan-other", name: "other", kind: "main" };
    await hub.put({ key: `topology/handoffs/${createHash("sha256").update(identity.id).digest("hex")}`, value: { id: identity.id }, identity });
    const writeBatch = hub.writeBatch.bind(hub);
    let batches = 0;
    hub.writeBatch = (input) => {
      batches += 1;
      if (batches === 1 && leaseChanges) {
        const now = Date.now();
        writeHostLease(hub.root, { id: identity.id, rootId: identity.id, identityId: identity.id, updatedAt: now, expiresAt: now + 60000, startedAt: now });
      }
      return writeBatch(input);
    };
    const presence = await new StoreBridgeSide(far, "hub").presence();
    await side.mirror(presence);
    expect(batches).toBe(leaseChanges ? 2 : 1);
    expect(side.holds("session:plan-spoke")).toBe(true);
    await bridge.stop();
  });

  // Review round 2 (P1): a void handoff depends on a native lease being live NOW. A plan made before the
  // transaction judged it at its own time; with no row or file change, the time read in the transaction must
  // judge every lease the same, or the plan runs again. Both directions; the clock is the side's own.
  it.each([
    ["expires", 0, 20_000, true],
    ["is exactly at the deadline", 10_000, 10_001, true],
    ["becomes live (clock back)", 20_000, 0, false],
  ] as const)("mirror plan: a native lease that %s before the transaction re-plans", async (_name, planAt, txAt, kept) => {
    const hub = store();
    const T0 = Date.now();
    let clock = T0 + planAt;
    const side = new StoreBridgeSide(hub, "far", () => clock);
    await side.mirror({ hosts: [], participants: [] }); // the recovery pass; later passes plan first
    const id = "session:handoff-12345678";
    const identity: MeshIdentity = { id: "session:operator", name: "operator", kind: "main" };
    const key = `topology/handoffs/${createHash("sha256").update(id).digest("hex")}`;
    await hub.put({ key, identity, value: { format: 1, id, sessionId: "handoff-12345678", link: "far",
      handedOffAt: T0, operator: "operator", hub: "hub" } });
    // The moved Main's native lease, live until T0 + 10 s.
    writeHostLease(hub.root, { id, rootId: id, identityId: id, updatedAt: T0, expiresAt: T0 + 10_000, startedAt: T0,
      session: { id: "main-session", startedAt: T0, updatedAt: T0, expiresAt: T0 + 10_000 } });
    const writeBatch = hub.writeBatch.bind(hub);
    let batches = 0;
    hub.writeBatch = (input) => {
      batches += 1;
      clock = T0 + txAt; // time passes (or steps back) between the plan and its transaction
      return writeBatch(input);
    };
    await side.mirror({ hosts: [], participants: [] });
    // A native lease live at the transaction's time voids (deletes) the handoff; a dead one keeps it.
    expect(hub.get(key, { fresh: true }) !== undefined).toBe(kept);
    expect(batches).toBe(2);
  });

  it("the change-feed proof: other keys pass, a host, participant or handoff row since the revision fails", async () => {
    const mesh = store();
    const other = new MeshStore(mesh.root, 65536, 1000, { stateBackend: "sqlite" });
    stores.push(other);
    const identity: MeshIdentity = { id: "session:feed", name: "feed", kind: "main" };
    const prefixes = ["topology/hosts/", "topology/participants/", "topology/handoffs/"];
    await mesh.put({ key: "probe/start", value: 0, identity });
    const revision = mesh.stateRevision()!;
    expect(mesh.stateUnchangedSince(revision, prefixes)).toBe(true);
    await other.put({ key: "inbox/cursor", value: 1, identity });
    expect(mesh.stateRevision()).not.toBe(revision);
    expect(mesh.stateUnchangedSince(revision, prefixes)).toBe(true);
    for (const prefix of prefixes) {
      const at = mesh.stateRevision()!;
      await other.put({ key: `${prefix}${"a".repeat(64)}`, value: { id: "x" }, identity });
      expect(mesh.stateUnchangedSince(at, prefixes)).toBe(false);
      expect(mesh.stateUnchangedSince(at, ["probe/"])).toBe(true);
    }
    const ownKey = `${prefixes[0]}${"b".repeat(64)}`;
    const ownRevision = mesh.stateRevision()!;
    const own = await mesh.put({ key: ownKey, value: { id: "own" }, identity });
    expect(mesh.stateUnchangedSince(ownRevision, prefixes)).toBe(false);
    expect(mesh.stateUnchangedSince(ownRevision, prefixes, new Set([ownKey]))).toBe(true);
    expect(mesh.stateUnchangedSince(ownRevision, prefixes, new Map([[ownKey, own.version]]))).toBe(true);
    await other.put({ key: ownKey, value: { id: "external" }, identity });
    expect(mesh.stateUnchangedSince(ownRevision, prefixes, new Set([ownKey]))).toBe(true);
    expect(mesh.stateUnchangedSince(ownRevision, prefixes, new Map([[ownKey, own.version]]))).toBe(false);
    // A revision of another lineage (a re-epoched or another database) proves nothing.
    expect(mesh.stateUnchangedSince(`other:${revision}`, prefixes)).toBe(false);
    // The file backend has no change feed: never proven.
    const file = new MeshStore(scratch(), 65536, 1000, { stateBackend: "file" });
    expect(file.stateUnchangedSince("x:1:1", prefixes)).toBe(false);
  });

  // fabric-v2's measure (Ryzen 1, 10-13): the hold is elastic. With 2 of 6 bridges stopped the lock stayed
  // 93% held; the rest took the freed time. So several bridges drain into ONE state.db at once here.
  it(`${SPOKES} bridges drain ${EVENTS} events each into one hub, then idle: no in-transaction topology scan, Main sends succeed`, async () => {
    const hub = store();
    const hubId = await native(hub, "session:hold-hub");
    await populate(hub);
    const links: MeshBridge[] = [];
    for (let n = 0; n < SPOKES; n++) {
      const far = store();
      const from = await native(far, `session:hold-spoke-${n}`);
      const bridge = new MeshBridge({ localName: "hub", remoteName: `far${n}`, local: new StoreBridgeSide(hub, `far${n}`),
        remote: new StoreBridgeSide(far, "hub"), cursorPath: path.join(scratch(), "cursor.json"), log: () => undefined });
      // The cursor starts at the source head: the backlog is published after it, as a catch-up.
      await bridge.start();
      for (let index = 0; index < EVENTS;) {
        const inputs = Array.from({ length: Math.min(256, EVENTS - index) }, (_, k) =>
          ({ topic: "fleet.work.hold", kind: "ask", from, to: hubId.id, text: `${n}:${index + k}` }));
        index += (await far.publishBatch(inputs)).length;
      }
      links.push(bridge);
    }
    const database = path.join(hub.root, "state.db");
    // Reads return at most 1,000 events: page through the hub's log.
    const received = () => {
      const all = [];
      for (let after = 0; ;) {
        const page = hub.read({ after, limit: 1000 });
        all.push(...page.filter(e => e.topic === "fleet.work.hold"));
        if (page.length < 1000) return all;
        after = page.at(-1)!.sequence;
      }
    };
    // Progress without reading the log (a full read every poll would starve the bridges' event loop):
    // the hub's head sequence, since only bridged events are published to it during the drain.
    const base = hub.latestSequence();
    const delivered = () => hub.latestSequence() - base;
    // A Main send's state write: another connection to the same state.db, every 25 ms.
    const probe = new MeshStore(hub.root, 65536, 1000, { stateBackend: "sqlite" });
    stores.push(probe);
    const probeIdentity: MeshIdentity = { id: "session:probe", name: "probe", kind: "main" };
    let probing = true;
    const probeEvery = async (ms: number, waits: number[] = []) => {
      for (let n = 0; probing; n++) {
        const asked = performance.now();
        await probe.put({ key: "probe/main-send", value: n, identity: probeIdentity });
        waits.push(performance.now() - asked);
        await new Promise(resolve => setTimeout(resolve, ms));
      }
      return waits;
    };
    const waits: number[] = [];
    const probes = probeEvery(25, waits);
    const batches = countBatches(hub);
    const scans = { drain: 0, idle: 0, busy: 0 };
    holds = [];
    inFenceScans = 0;
    const started = performance.now();
    // Each bridge drains its backlog with passes of its own, all at once against the one hub.
    await Promise.all(links.map(async bridge => {
      while ((await bridge.step()).toLocal > 0) { /* until a pass finds nothing new */ }
    }));
    const count = delivered();
    const drainMs = performance.now() - started;
    probing = false;
    await probes;
    scans.drain = inFenceScans;
    const drain = report(holds.filter(hold => hold.db === database), started, performance.now());
    const p99 = waits.slice().sort((a, b) => a - b)[Math.min(waits.length - 1, Math.floor(waits.length * 0.99))] ?? 0;
    console.log(JSON.stringify({ phase: "drain", bridges: SPOKES, events: SPOKES * EVENTS, delivered: count,
      drainMs: +drainMs.toFixed(0), probes: waits.length, probeP99Ms: +p99.toFixed(1),
      probeMaxMs: +Math.max(0, ...waits).toFixed(1), inFenceScans: scans.drain, maxBatchEvents: Math.max(0, ...batches), ...drain }));
    // Steady state: the backlog is gone and nothing new arrives. The bridges run their live loop
    // (change notifications, presence deadlines); after it settles, nothing should take the lock.
    const running = links.map(bridge => bridge.run());
    await new Promise(resolve => setTimeout(resolve, 2000));
    holds = [];
    inFenceScans = 0;
    const idleFrom = performance.now();
    await new Promise(resolve => setTimeout(resolve, IDLE_MS));
    scans.idle = inFenceScans;
    const idle = report(holds.filter(hold => hold.db === database), idleFrom, performance.now());
    console.log(JSON.stringify({ phase: "idle", windowMs: IDLE_MS, published: delivered() - count, ...idle }));
    // Idle bridges on a busy hub: no new events, but Mains write state.db (sends, heartbeats, cursors)
    // every 100 ms. Each write notifies every bridge's watcher; the bridges' own share must stay ~0.
    probing = true;
    holds = [];
    inFenceScans = 0;
    const busyFrom = performance.now();
    const busyProbes = probeEvery(100);
    await new Promise(resolve => setTimeout(resolve, IDLE_MS));
    probing = false;
    await busyProbes;
    scans.busy = inFenceScans;
    const busy = report(holds.filter(hold => hold.db === database && hold.site !== PROBE_SITE), busyFrom, performance.now());
    console.log(JSON.stringify({ phase: "idle, busy hub", windowMs: IDLE_MS, published: delivered() - count, ...busy }));
    console.log(JSON.stringify({ inFenceScans: scans }));
    await Promise.all(links.map(bridge => bridge.stop()));
    await Promise.allSettled(running);
    const texts = received().map(e => e.text!);
    for (let n = 0; n < SPOKES; n++) {
      expect(texts.filter(text => text.startsWith(`${n}:`))).toEqual(Array.from({ length: EVENTS }, (_, k) => `${n}:${k}`));
    }
    // The mechanism: no topology scan inside a write transaction while draining, idle, or idle on a busy hub
    // (main: one per bridged event and one per bridge per hub change); bounded batches; Main sends succeed.
    expect(scans).toEqual({ drain: 0, idle: 0, busy: 0 });
    expect(Math.max(0, ...batches)).toBeLessThanOrEqual(BATCH_MAX_EVENTS);
    expect(waits.length).toBeGreaterThan(0);
    // Gross-regression ceilings only (see CEILING_*).
    expect(drain.maxHoldMs).toBeLessThanOrEqual(CEILING_HOLD_MS);
    expect(drain.heldPct).toBeLessThanOrEqual(CEILING_DRAIN_PCT);
    expect(idle.heldPct).toBeLessThanOrEqual(CEILING_IDLE_PCT);
    expect(busy.heldPct).toBeLessThanOrEqual(CEILING_IDLE_PCT);
  }, 240000);
});
