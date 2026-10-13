import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterAll, afterEach, describe, expect, it } from "vitest";
import { MESH_ARCHIVE_CONFIG } from "../src/mesh/archive.js";
import { MeshBridge, StoreBridgeSide } from "../src/mesh/bridge.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { writeHostLease } from "../src/topology/host-leases.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";

// smarty-dev#10391: a bridge catch-up must never hold state.db's SQLite write lock above 100 ms.
// The boundary is observed at the driver (node:sqlite DatabaseSync.exec), not by production
// logging: every BEGIN IMMEDIATE to its COMMIT or ROLLBACK, on every connection in this process.
const MAX_HOLD_MS = 100;
const EVENTS = 500;
// fabric-v2's gate: at most 25% of a multi-bridge drain window held, a Main send gets the lock within
// 250 ms at p99, and an idle bridge holds it about 0% (no spin) over at least 5 s.
// The probe's own writes (a Main send stand-in), told apart from the bridges' by their writer frame.
const PROBE_SITE = "SqliteStateStore.put";
const SPOKES = 3, MAX_HELD_PCT = 25, PROBE_P99_MS = 250, IDLE_MS = 6000, IDLE_HELD_PCT = 1;

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
    open.set(this, performance.now());
    sites.set(this, site(new Error().stack));
    return;
  }
  const began = open.get(this);
  try { original.call(this, sql); }
  finally {
    if (began !== undefined && (statement === "COMMIT" || statement === "ROLLBACK")) {
      open.delete(this);
      const ended = performance.now();
      holds.push({ ms: ended - began, site: sites.get(this) ?? "?", began, ended,
        db: (this as { location(): string | null }).location() ?? "" });
    }
  }
};
afterAll(() => { proto.exec = original; });

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
const populate = async (mesh: MeshStore) => {
  const now = Date.now();
  const ops: Array<{ kind: "put"; key: string; value: unknown; identity: MeshIdentity }> = [];
  for (let n = 0; n < Math.max(HOSTS, PARTICIPANTS); n++) {
    const id = `session:fleet-${n}`;
    const identity: MeshIdentity = { id, name: `agent-${n}`, kind: "main" };
    const hash = createHash("sha256").update(id).digest("hex");
    if (n < HOSTS) ops.push({ kind: "put", key: `topology/hosts/${hash}`, identity,
      value: { format: 1, id, rootId: id, identity, startedAt: now, updatedAt: now, expiresAt: now + 120000, host: `host-${n % 9}` } });
    if (n < PARTICIPANTS) ops.push({ kind: "put", key: `topology/participants/${hash}`, identity,
      value: { format: 1, id, kind: "root", rootId: id, ownerHostId: id, ownerIdentityId: id, name: `agent-${n}`,
        label: `lane agent ${n} on host ${n % 9}`, status: "idle", runner: "pi", transport: "host",
        capabilities: ["steer", "followUp", "status"], startedAt: now, updatedAt: now, controlProtocol: "v1",
        cwd: `/home/paul/lanes/lane-${n}`, model: "opus-5.5", effort: "high" } });
  }
  for (let at = 0; at < ops.length; at += 200) await mesh.writeBatch({ identity: ops[at]!.identity, ops: ops.slice(at, at + 200) });
  for (let n = 0; n < LEASES; n++) {
    const id = `session:fleet-${n}`;
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
  it.each(["toLocal", "toRemote"] as const)(`${EVENTS} events %s: no write-lock hold above ${MAX_HOLD_MS} ms`, async direction => {
    const hub = store(), far = store();
    const localId = await native(hub, "session:hold-local"), remoteId = await native(far, "session:hold-remote");
    await populate(hub);
    // The default presence period: a drain this long runs presence passes, as a fleet catch-up does.
    const bridge = new MeshBridge({ localName: "hub", remoteName: "far", local: new StoreBridgeSide(hub, "far"),
      remote: new StoreBridgeSide(far, "hub"), cursorPath: path.join(scratch(), "cursor.json") });
    await bridge.start();
    const source = direction === "toRemote" ? hub : far;
    const target = direction === "toRemote" ? far : hub;
    const from = direction === "toRemote" ? localId : remoteId;
    const to = direction === "toRemote" ? remoteId.id : localId.id;
    for (let index = 0; index < EVENTS;) {
      const inputs = Array.from({ length: Math.min(256, EVENTS - index) }, (_, n) =>
        ({ topic: "fleet.work.hold", kind: "ask", from, to, text: String(index + n) }));
      index += (await source.publishBatch(inputs)).length;
    }
    holds = [];
    const started = performance.now();
    const result = await bridge.step();
    const drainMs = performance.now() - started;
    const measured = holds;
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
    console.log(JSON.stringify({ direction, events: EVENTS, transactions: measured.length,
      maxHoldMs: +maxHoldMs.toFixed(2), totalHoldMs: +totalHoldMs.toFixed(1), drainMs: +drainMs.toFixed(1),
      over100: measured.filter(hold => hold.ms > MAX_HOLD_MS).length, bySite }));
    await bridge.stop();
    expect(result).toMatchObject({ [direction]: EVENTS, dropped: 0 });
    expect(target.read({ limit: 1000 }).filter(e => e.topic === "fleet.work.hold").map(e => e.text))
      .toEqual(Array.from({ length: EVENTS }, (_, index) => String(index)));
    expect(maxHoldMs).toBeLessThanOrEqual(MAX_HOLD_MS);
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

    it("no change: one fence, committed on the proof", async () => {
      const f = await held();
      await f.send("plain");
      expect(f.fences()).toBe(1);
      expect(f.hub.read({ after: 0, limit: 100 }).map(e => e.text)).toContain("plain");
      await f.bridge.stop();
    });

    it("a renewed lease file: the proof fails, the full check passes, and the event commits", async () => {
      const f = await held();
      const lease = path.join(f.hub.root, "host-leases");
      const now = Date.now();
      f.set(() => writeHostLease(f.hub.root, { id: f.spoke.id, rootId: f.spoke.id, identityId: f.spoke.id,
        updatedAt: now, expiresAt: now + 30000, startedAt: now }));
      await f.send("renewed");
      expect(fs.existsSync(lease)).toBe(true);
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
    // A revision of another lineage (a re-epoched or another database) proves nothing.
    expect(mesh.stateUnchangedSince(`other:${revision}`, prefixes)).toBe(false);
    // The file backend has no change feed: never proven.
    const file = new MeshStore(scratch(), 65536, 1000, { stateBackend: "file" });
    expect(file.stateUnchangedSince("x:1:1", prefixes)).toBe(false);
  });

  // fabric-v2's measure (Ryzen 1, 10-13): the hold is elastic. With 2 of 6 bridges stopped the lock stayed
  // 93% held; the rest took the freed time. So several bridges drain into ONE state.db at once here.
  it(`${SPOKES} bridges drain ${EVENTS} events each into one hub: held share, a Main-send probe, then idle`, async () => {
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
    holds = [];
    const started = performance.now();
    // Each bridge drains its backlog with passes of its own, all at once against the one hub.
    await Promise.all(links.map(async bridge => {
      while ((await bridge.step()).toLocal > 0) { /* until a pass finds nothing new */ }
    }));
    const count = delivered();
    const drainMs = performance.now() - started;
    probing = false;
    await probes;
    const drain = report(holds.filter(hold => hold.db === database), started, performance.now());
    const p99 = waits.slice().sort((a, b) => a - b)[Math.min(waits.length - 1, Math.floor(waits.length * 0.99))] ?? 0;
    console.log(JSON.stringify({ phase: "drain", bridges: SPOKES, events: SPOKES * EVENTS, delivered: count,
      drainMs: +drainMs.toFixed(0), probes: waits.length, probeP99Ms: +p99.toFixed(1),
      probeMaxMs: +Math.max(0, ...waits).toFixed(1), ...drain }));
    // Steady state: the backlog is gone and nothing new arrives. The bridges run their live loop
    // (change notifications, presence deadlines); after it settles, nothing should take the lock.
    const running = links.map(bridge => bridge.run());
    await new Promise(resolve => setTimeout(resolve, 2000));
    holds = [];
    const idleFrom = performance.now();
    await new Promise(resolve => setTimeout(resolve, IDLE_MS));
    const idle = report(holds.filter(hold => hold.db === database), idleFrom, performance.now());
    console.log(JSON.stringify({ phase: "idle", windowMs: IDLE_MS, published: delivered() - count, ...idle }));
    // Idle bridges on a busy hub: no new events, but Mains write state.db (sends, heartbeats, cursors)
    // every 100 ms. Each write notifies every bridge's watcher; the bridges' own share must stay ~0.
    probing = true;
    holds = [];
    const busyFrom = performance.now();
    const busyProbes = probeEvery(100);
    await new Promise(resolve => setTimeout(resolve, IDLE_MS));
    probing = false;
    await busyProbes;
    const busy = report(holds.filter(hold => hold.db === database && hold.site !== PROBE_SITE), busyFrom, performance.now());
    console.log(JSON.stringify({ phase: "idle, busy hub", windowMs: IDLE_MS, published: delivered() - count, ...busy }));
    await Promise.all(links.map(bridge => bridge.stop()));
    await Promise.allSettled(running);
    const texts = received().map(e => e.text!);
    for (let n = 0; n < SPOKES; n++) {
      expect(texts.filter(text => text.startsWith(`${n}:`))).toEqual(Array.from({ length: EVENTS }, (_, k) => `${n}:${k}`));
    }
    expect(drain.maxHoldMs).toBeLessThanOrEqual(MAX_HOLD_MS);
    expect(drain.heldPct).toBeLessThanOrEqual(MAX_HELD_PCT);
    expect(p99).toBeLessThanOrEqual(PROBE_P99_MS);
    expect(idle.heldPct).toBeLessThanOrEqual(IDLE_HELD_PCT);
    expect(busy.heldPct).toBeLessThanOrEqual(IDLE_HELD_PCT);
  }, 240000);
});
