import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterAll, afterEach, describe, expect, it, vi } from "vitest";

// Fleet isolation disables diagnostics before imports; opt into the actual holder telemetry.
const telemetry = vi.hoisted(() => {
  const previous = process.env.PI_FABRIC_LOCK_STATS;
  process.env.PI_FABRIC_LOCK_STATS = "1";
  return { previous };
});
afterAll(() => {
  if (telemetry.previous === undefined) delete process.env.PI_FABRIC_LOCK_STATS;
  else process.env.PI_FABRIC_LOCK_STATS = telemetry.previous;
  for (const name of ["pi-fabric.mesh.lock-stats", "pi-fabric.mesh.lock-events"]) {
    const key = Symbol.for(name);
    const registry = globalThis as Record<symbol, { dispose(): void } | undefined>;
    registry[key]?.dispose();
    delete registry[key];
  }
});
import { MeshBridge, StoreBridgeSide, type BridgePublish } from "../src/mesh/bridge.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";

import { BRIDGE_BATCH_MAX_EVENTS as MAX_EVENTS, BRIDGE_BATCH_MAX_MS as MAX_MS } from "../src/mesh/event-log.js";
// One synchronous append/ownership check and scheduler jitter cannot be preempted.
const HOLD_MARGIN_MS = 25;
// Hosted runners (shared, slow disks; pi-fabric#558 Ubuntu saw 178 and 217 ms against MAX_MS + HOLD_MARGIN_MS)
// cannot hold a wall-clock bar. The gates are the mechanism (count cap, a writer interleaving the drain, order);
// wall-clock stays a gross-regression ceiling only, and the numbers are printed.
const CEILING_MS = 3000;
const roots: string[] = [];
const stores: MeshStore[] = [];
const scratch = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-txn-cap-"));
  roots.push(root);
  return root;
};
afterEach(() => {
  for (const store of stores.splice(0)) store.closeState();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const store = () => {
  const mesh = new MeshStore(scratch(), 65536, 1000, { stateBackend: "sqlite", lockHeldEventMs: 0.000001 });
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
  return identity;
};

// Transparent boundary observation/failure injection: every admitted prefix is applied by the
// real StoreBridgeSide, MeshStore, EventLog and SQLite backend, never a mocked implementation.
class ObservedSide extends StoreBridgeSide {
  readonly prefixes: number[] = [];
  afterPrefix?: () => void;
  failBefore = Infinity;
  failAfter = Infinity;
  override async publish(event: BridgePublish, held?: string[]) {
    if (this.prefixes.length === this.failBefore) throw new Error("simulated crash between batches");
    const receipt = await super.publish(event, held);
    this.prefixes.push(1);
    this.afterPrefix?.();
    if (this.prefixes.length === this.failAfter) throw new Error("simulated lost reply before checkpoint");
    return receipt;
  }
  override async publishBatch(inputs: Array<{ event: BridgePublish; held?: string[] }>) {
    if (this.prefixes.length === this.failBefore) throw new Error("simulated crash between batches");
    const receipt = await super.publishBatch(inputs);
    this.prefixes.push(receipt.length);
    this.afterPrefix?.();
    if (this.prefixes.length === this.failAfter) throw new Error("simulated lost reply before checkpoint");
    return receipt;
  }
}
const setup = async (direction: "toRemote" | "toLocal") => {
  const hub = store(), far = store();
  const localId = await native(hub, "session:cap-local"), remoteId = await native(far, "session:cap-remote");
  const local = new ObservedSide(hub, "far"), remote = new ObservedSide(far, "hub");
  const cursorPath = path.join(scratch(), "cursor.json");
  const options = { localName: "hub", remoteName: "far", local, remote, cursorPath, presenceMs: 120000 };
  const bridge = new MeshBridge(options);
  await bridge.start();
  const source = direction === "toRemote" ? hub : far;
  const target = direction === "toRemote" ? far : hub;
  const side = direction === "toRemote" ? remote : local;
  const from = direction === "toRemote" ? localId : remoteId;
  const to = direction === "toRemote" ? remoteId.id : localId.id;
  const inputs = Array.from({ length: 500 }, (_, index) => ({ topic: "fleet.work.cap", kind: "ask", from, to, text: String(index) }));
  const originals = [];
  for (let offset = 0; offset < inputs.length;) {
    const prefix = await source.publishBatch(inputs.slice(offset, offset + 256));
    originals.push(...prefix);
    offset += prefix.length;
  }
  return { bridge, options, cursorPath, source, target, side, originals };
};
interface Hold { kind: string; store: string; class: string; heldMs: number }
const holds = (mesh: MeshStore): Hold[] => fs.readFileSync(path.join(mesh.root, "lock-stats", "events.jsonl"), "utf8")
  .trim().split("\n").filter(Boolean).map(line => JSON.parse(line) as Hold);
const texts = (mesh: MeshStore) => mesh.read({ limit: 1000 }).map(event => event.text);

describe("bridge transaction cap (smarty-dev#10391), real SQLite stores", () => {
  it.each(["toRemote", "toLocal"] as const)("500 events: batches of at most MAX_EVENTS and a writer between %s batches", async direction => {
    const f = await setup(direction);
    const writer = new MeshStore(f.target.root, 65536, 1000, { stateBackend: "sqlite" });
    stores.push(writer);
    const before = holds(f.target).length;
    let writerAt = -1, writerWaitMs = Infinity, writerAfterBatches = -1;
    let writing: Promise<void> | undefined;
    f.side.afterPrefix = () => {
      if (writing) return;
      writing = new Promise<void>((resolve, reject) => setImmediate(() => {
        writerAt = f.target.read({ limit: 1000 }).length;
        const asked = performance.now();
        writer.put({ key: "probe/interleaved", value: writerAt, identity: { id: "concurrent", name: "concurrent", kind: "agent" } })
          .then(() => { writerWaitMs = performance.now() - asked; writerAfterBatches = f.side.prefixes.length; resolve(); }, reject);
      }));
    };
    let result;
    try { result = await f.bridge.step(); }
    finally { await writing; }
    const events = holds(f.target).slice(before);
    const meshHolds = events.filter(e => e.kind === "held" && e.store === "mesh-lock" && (e.class === "bridge" || e.class === "publish"));
    const stateHolds = events.filter(e => e.kind === "held" && e.store === "sqlite");
    const maxMeshHoldMs = Math.max(0, ...meshHolds.map(e => e.heldMs));
    const maxStateHoldMs = Math.max(0, ...stateHolds.map(e => e.heldMs));
    console.log(JSON.stringify({ direction, events: 500, batches: f.side.prefixes.length,
      maxBatchEvents: Math.max(...f.side.prefixes), meshTransactions: meshHolds.length, sqliteTransactions: stateHolds.length,
      maxMeshHoldMs, maxStateHoldMs, capMs: MAX_MS, marginMs: HOLD_MARGIN_MS, writerAt, writerAfterBatches,
      writerWaitMs: +writerWaitMs.toFixed(3) }));
    expect(result).toMatchObject({ [direction]: 500, dropped: 0 });
    expect(f.side.prefixes.length).toBeGreaterThanOrEqual(Math.ceil(500 / MAX_EVENTS));
    expect(f.side.prefixes.every(n => n > 0 && n <= MAX_EVENTS)).toBe(true);
    expect(meshHolds.length).toBe(f.side.prefixes.length);
    expect(stateHolds.length).toBe(direction === "toLocal" ? 500 : 0);
    // The writer asked after the first batch and committed while later batches were still to come: it got the
    // lock between batches, not after the drain.
    expect(writerAfterBatches).toBeGreaterThanOrEqual(1);
    expect(writerAfterBatches).toBeLessThan(f.side.prefixes.length);
    expect(writerAt).toBeGreaterThan(0);
    expect(writerAt).toBeLessThan(500);
    // Gross-regression ceilings only.
    expect(maxMeshHoldMs).toBeLessThanOrEqual(CEILING_MS);
    expect(maxStateHoldMs).toBeLessThanOrEqual(CEILING_MS);
    expect(writerWaitMs).toBeLessThan(CEILING_MS);
    expect(writer.get("probe/interleaved")?.value).toBe(writerAt);
    expect(texts(f.target)).toEqual(Array.from({ length: 500 }, (_, index) => String(index)));
  }, 30000);

  it("elapsed-time cap releases a real state fence before reaching the count cap, without a mocked clock", async () => {
    const mesh = store();
    const identity: MeshIdentity = { id: "timed", name: "timed", kind: "agent" };
    // Open/import SQLite before taking .lock, matching the bridge's presence startup path.
    await mesh.put({ key: "probe/initialized", value: true, identity });
    const before = holds(mesh).length;
    const sleep = new Int32Array(new SharedArrayBuffer(4));
    const inputs = Array.from({ length: MAX_EVENTS }, (_, index) => ({ topic: "fleet.work.cap", from: identity, text: String(index),
      fence: <T>(commit: () => T): T => mesh.withStateWriteFence(commit),
      admit: () => { Atomics.wait(sleep, 0, 0, 8); } }));
    const prefix = await mesh.publishBatch(inputs);
    const events = holds(mesh).slice(before);
    const maxMeshHoldMs = Math.max(...events.filter(e => e.store === "mesh-lock").map(e => e.heldMs));
    const maxStateHoldMs = Math.max(...events.filter(e => e.store === "sqlite").map(e => e.heldMs));
    console.log(JSON.stringify({ timed: true, applied: prefix.length, maxMeshHoldMs, maxStateHoldMs, capMs: MAX_MS, marginMs: HOLD_MARGIN_MS }));
    expect(prefix.length).toBeGreaterThan(0);
    expect(prefix.length).toBeLessThan(MAX_EVENTS);
    // The time cap (not the count cap) ended the prefix: it held at least MAX_MS. Upper bounds: ceilings only.
    expect(maxMeshHoldMs).toBeGreaterThanOrEqual(MAX_MS);
    expect(maxMeshHoldMs).toBeLessThanOrEqual(CEILING_MS);
    expect(maxStateHoldMs).toBeLessThanOrEqual(CEILING_MS);
    expect(texts(mesh)).toEqual(inputs.slice(0, prefix.length).map(e => e.text));
  });

  it.each(["between", "lost-checkpoint"] as const)("resumes 500 events after %s without loss or duplicates", async crash => {
    const f = await setup("toLocal");
    if (crash === "between") f.side.failBefore = 1;
    else f.side.failAfter = 1;
    await expect(f.bridge.step()).rejects.toThrow(crash === "between" ? "between batches" : "lost reply");
    const committed = f.target.read({ limit: 1000 });
    expect(committed.length).toBeGreaterThan(0);
    expect(committed.length).toBeLessThanOrEqual(MAX_EVENTS);
    const cursor = JSON.parse(fs.readFileSync(f.cursorPath, "utf8")).toLocal;
    expect(cursor.after).toBe(crash === "between" ? f.originals[committed.length - 1]!.sequence : 0);
    f.side.failBefore = f.side.failAfter = Infinity;
    const restarted = new MeshBridge(f.options);
    await restarted.start();
    await restarted.step();
    const all = f.target.read({ limit: 1000 });
    expect(all.map(e => e.text)).toEqual(Array.from({ length: 500 }, (_, index) => String(index)));
    expect(new Set(all.map(e => (e.data as BridgePublish["data"]).bridge.id)).size).toBe(500);
    expect(all.slice(0, committed.length).map(e => e.id)).toEqual(committed.map(e => e.id));
    expect(JSON.parse(fs.readFileSync(f.cursorPath, "utf8")).toLocal.after).toBe(f.originals.at(-1)!.sequence);
    console.log(JSON.stringify({ crash, committedBeforeCrash: committed.length, resumed: all.length, uniqueSourceIds: 500 }));
  }, 30000);
});
