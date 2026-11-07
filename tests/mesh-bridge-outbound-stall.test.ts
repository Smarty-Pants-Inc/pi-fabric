import { createHash } from "node:crypto";
import { EventEmitter } from "node:events";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance as monotonicClock } from "node:perf_hooks";
import { afterEach, describe, expect, it, vi } from "vitest";
import { BRIDGE_STALL_ALARM_TOPIC, MeshBridge, StoreBridgeSide } from "../src/mesh/bridge.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { writeHostLease } from "../src/topology/host-leases.js";

const owner: MeshIdentity = { id: "session:11111111-1111-4111-8111-111111111111", name: "owner", kind: "main" };
const peer: MeshIdentity = { id: "session:22222222-2222-4222-8222-222222222222", name: "peer", kind: "main" };
const OLD_AGE_MS = 241_000;
const STEP_MS = 20_000; // A real receipt every step: never stationary for the base's 180 s bound.
const STEPS = 31; // 620 s pending, beyond the reported ten-minute incident, not a new policy threshold.
const cleanup: Array<() => Promise<void>> = [];
afterEach(async () => {
  try { for (const close of cleanup.splice(0).reverse()) await close(); }
  finally { vi.restoreAllMocks(); }
});

async function fixture(backlog: number) {
  let wall = Date.now(), mono = monotonicClock.now();
  vi.spyOn(Date, "now").mockImplementation(() => wall);
  vi.spyOn(monotonicClock, "now").mockImplementation(() => mono);
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-outbound-stall-"));
  // A one-event real store page makes each gate one actual durable receipt, with original offsets.
  const hub = new MeshStore(path.join(root, "hub"), 64 * 1024, 1);
  const far = new MeshStore(path.join(root, "far"), 64 * 1024, 1);
  const cursorPath = path.join(root, "cursor.json");
  for (const [store, identity] of [[hub, owner], [far, peer]] as const) {
    const now = wall;
    const incarnationToken = `fixture:${identity.id}`;
    await store.put({ key: `topology/hosts/${createHash("sha256").update(identity.id).digest("hex")}`, identity,
      value: { format: 1, id: identity.id, rootId: identity.id, identity, startedAt: now, updatedAt: now,
        expiresAt: now + 86_400_000, incarnationToken } });
    writeHostLease(store.root, { id: identity.id, rootId: identity.id, identityId: identity.id, startedAt: now,
      updatedAt: now, expiresAt: now + 86_400_000, incarnationToken });
  }
  // Preserve actual store-created timestamps: append while the finite wall clock is earlier,
  // then start the bridge 4+ minutes later. No event, receipt, head or stall result is fabricated.
  wall -= OLD_AGE_MS;
  const source = [];
  for (let index = 0; index < backlog; index++) source.push(await hub.publish({
    topic: "fabric.control.command", kind: "followUp", from: owner, to: peer.id,
    data: { version: 1, commandId: `outbound-stall-${index}`, targetId: peer.id, operation: "followUp",
      replyTo: owner.id, ownerIncarnation: `fixture:${peer.id}`, requestedAt: wall, message: `backlog ${index}` },
  }));
  wall += OLD_AGE_MS;
  // Resume a real saved cursor, rather than allowing first-start baseline initialization to skip the backlog.
  fs.writeFileSync(cursorPath, JSON.stringify({ format: 1, local: "hub", remote: "far",
    toRemote: { after: 0, mark: 0 }, toLocal: { after: 0, mark: 0 } }));
  const signals = new EventEmitter();
  let failure: unknown, lastReadAfter = -1, lastProbeMono = -1, pings = 0;
  let gate: { after: number; release: () => void } | undefined;
  const local = new StoreBridgeSide(hub, "far");
  const remote = new StoreBridgeSide(far, "hub");
  const tail = local.tail.bind(local);
  local.tail = async (after, offset) => {
    const page = await tail(after, offset);
    lastReadAfter = after;
    if (page.events.length) await new Promise<void>(resolve => {
      gate = { after, release: () => { gate = undefined; resolve(); } };
      signals.emit("changed");
    });
    else signals.emit("changed");
    return page;
  };
  const alarms = () => hub.read({ topic: BRIDGE_STALL_ALARM_TOPIC, after: 0, limit: 100 });
  const bridge = new MeshBridge({ localName: "hub", remoteName: "far", local, remote, cursorPath,
    // Only observation cadence is shortened; both production stall thresholds remain unchanged.
    // Normal forwarding still uses real filesystem notifications. This timer only samples health.
    watchdogIntervalMs: 1,
    notifyWatchdog: () => { pings++; lastProbeMono = mono; signals.emit("changed"); },
    publishStallAlarm: async alarm => {
      await hub.publish({ topic: BRIDGE_STALL_ALARM_TOPIC, kind: "bridge-stall", from: owner, to: owner.id, data: alarm });
      signals.emit("changed");
    },
  });
  const running = bridge.run().catch(error => { failure = error; signals.emit("changed"); });
  cleanup.push(async () => {
    const stopping = bridge.stop();
    gate?.release();
    await stopping;
    await running;
    hub.closeState(); far.closeState();
    fs.rmSync(root, { recursive: true, force: true });
  });
  // Event-driven observation with a bounded failure deadline, not a polling carrier.
  const observed = (check: () => boolean): Promise<void> => new Promise((resolve, reject) => {
    const timer = setTimeout(() => finish(new Error("bridge observation timed out")), 3_000);
    const inspect = () => { if (failure) finish(failure); else if (check()) finish(); };
    function finish(error?: unknown) {
      clearTimeout(timer); signals.off("changed", inspect);
      if (error) reject(error); else resolve();
    }
    signals.on("changed", inspect); inspect();
  });
  const cursor = (): { toRemote: { after: number }; toLocal: { after: number } } =>
    JSON.parse(fs.readFileSync(cursorPath, "utf8"));
  const probe = () => observed(() => lastProbeMono === mono || alarms().length > 0);
  const advance = () => { wall += STEP_MS; mono += STEP_MS; };
  const deliver = async (sequence: number) => {
    await observed(() => gate !== undefined || alarms().length > 0);
    gate?.release();
    // The next real source read starts only after #forward has saved the confirmed prefix.
    await observed(() => lastReadAfter >= sequence || alarms().length > 0);
  };
  await observed(() => gate !== undefined);
  await probe();
  return { hub, far, source, cursor, alarms, advance, deliver, probe, pings: () => pings, failure: () => failure };
}

// Keep reason: ADDENDUM 2's real incident, 4+ min outbound delivery lag for 10+ min without a stall.
// These checks deliberately require only the requested stall alarm, not a new fatal-restart policy.
describe("outbound stall watchdog delivery-age regression", () => {
  it("raises a toRemote stall for a 4+ minute-old, pending backlog that advances for over ten minutes", async () => {
    const f = await fixture(64);
    const ages: number[] = [];
    for (let index = 0; index < STEPS && f.alarms().length === 0; index++) {
      f.advance();
      await f.deliver(f.source[index]!.sequence);
      const destination = f.far.read({ after: index, limit: 1 })[0]!;
      expect(destination).toMatchObject({ verification: "bridge", topic: "fabric.control.command",
        data: { commandId: `outbound-stall-${index}`, bridge: { from: "hub", id: f.source[index]!.id } } });
      ages.push(destination.createdAt - f.source[index]!.createdAt);
      expect(f.cursor().toRemote.after).toBeGreaterThan(index);
      expect(f.cursor().toRemote.after).toBeLessThan(f.hub.latestSequence());
      await f.probe();
    }
    expect(ages.length).toBeGreaterThan(0);
    expect(Math.min(...ages)).toBeGreaterThan(240_000);
    expect(f.failure()).toBeUndefined();
    console.log("OUTBOUND_WATCHDOG_PENDING", JSON.stringify({ elapsedMs: ages.length * STEP_MS,
      receipts: ages.length, minDeliveryAgeMs: Math.min(...ages), maxDeliveryAgeMs: Math.max(...ages),
      cursor: f.cursor(), sourceHead: f.hub.latestSequence(), destinationHead: f.far.latestSequence(),
      pings: f.pings(), stallAlarms: f.alarms().length }));
    expect(f.alarms(), "a continuously pending toRemote backlog stayed over four minutes old for over ten minutes despite real cursor progress")
      .toEqual(expect.arrayContaining([expect.objectContaining({ kind: "bridge-stall", data: expect.objectContaining({ direction: "toRemote" }) })]));
  });

  it("does not trip an idle leg merely because its last real delivery is old", async () => {
    const f = await fixture(1);
    await f.deliver(f.source[0]!.sequence);
    await f.probe();
    const destination = f.far.read({ after: 0, limit: 1 })[0]!;
    expect(destination).toMatchObject({ verification: "bridge", data: { commandId: "outbound-stall-0" } });
    expect(destination.createdAt - f.source[0]!.createdAt).toBeGreaterThan(240_000);
    const pingsBeforeIdle = f.pings();
    for (let index = 0; index < STEPS; index++) { f.advance(); await f.probe(); }
    expect(f.cursor().toRemote.after).toBe(f.hub.latestSequence());
    expect(f.cursor().toLocal.after).toBe(f.far.latestSequence());
    expect(f.failure()).toBeUndefined();
    expect(f.alarms()).toEqual([]);
    expect(f.pings() - pingsBeforeIdle).toBeGreaterThanOrEqual(STEPS);
    console.log("OUTBOUND_WATCHDOG_IDLE_PASS", JSON.stringify({ idleMs: STEPS * STEP_MS,
      cursor: f.cursor(), sourceHead: f.hub.latestSequence(), destinationHead: f.far.latestSequence(),
      pings: f.pings(), stallAlarms: f.alarms().length }));
  });
});
