import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshBridge, type BridgeSide } from "../src/mesh/bridge.js";
import { BridgeDeliveryAgeMonitor, type BridgeDeliveryAgeAlarm } from "../src/mesh/bridge-delivery-age.js";

const baseEvent = (createdAt: number, data: Record<string, unknown> = {}) => ({
  topic: "fabric.control.command", data, createdAt,
});

describe("bridge per-direction delivery age", () => {
  afterEach(() => { vi.restoreAllMocks(); vi.useRealTimers(); });

  // Keep: the reported slow membership failure path cannot be reached cheaply in a live flow.
  it("attributes a slow refresh on a not-addressed event, with exclusive totals matching wall time", async () => {
    vi.useFakeTimers({ now: 1_800_000_000_000 });
    const start = Date.now(), lines: string[] = [];
    vi.spyOn(performance, "now").mockImplementation(() => Date.now() - start);
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-drain-time-"));
    const empty = { hosts: [], participants: [], reserved: [] };
    const side = (): BridgeSide => ({ latestSequence: async () => 0, read: async () => ({ events: [], through: 0 }),
      presence: async () => empty, owned: async () => empty, mirror: async () => {}, mirrored: () => true,
      publish: async () => ({ sequence: 1 }), bridgedIds: async () => ({ ids: [], through: 0, done: true }) });
    const local = side(), bridge = new MeshBridge({ localName: "hub", remoteName: "far", local, remote: side(),
      cursorPath: path.join(root, "cursor.json"), presenceMs: 1_000_000, log: line => lines.push(line) });
    try {
      await bridge.start();
      local.read = async after => ({ events: after ? [] : [{ ...baseEvent(start), id: "event", kind: "followUp", sequence: 1,
        from: { id: "session:sender", name: "main", kind: "main" }, to: "session:unknown" }], through: 1 });
      vi.spyOn(bridge, "syncPresence").mockImplementation(() => new Promise(resolve => setTimeout(resolve, 60_125)));
      const draining = bridge.step();
      await vi.advanceTimersByTimeAsync(60_000);
      expect(lines.some(line => line.includes("membershipWaitMs=60000"))).toBe(true); // Even before the await settles.
      await vi.advanceTimersByTimeAsync(125);
      expect(await draining).toEqual({ toRemote: 0, toLocal: 0, dropped: 0 });
      await bridge.stop();
      const values: Record<string, number> = {};
      for (const line of lines.filter(line => line.startsWith("bridge drain-time v1 peer=far dir=toRemote "))) {
        for (const [, key, value] of line.matchAll(/(\w+)=(\d+)/g)) values[key!] = key!.endsWith("MaxMs")
          ? Math.max(values[key!] ?? 0, Number(value)) : (values[key!] ?? 0) + Number(value);
      }
      expect(values).toMatchObject({ pages: 2, events: 1, membershipWaitMs: 60_125, membershipWaitCount: 1, membershipWaitMaxMs: 60_125 });
      const sum = ["readMs", "resolveMs", "membershipWaitMs", "filterMs", "publishMs", "checkpointMs", "idleMs"]
        .reduce((total, key) => total + values[key]!, 0);
      expect(sum).toBeCloseTo(Date.now() - start, 0);
    } finally { await bridge.stop(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it("alarms once for a backlogged command, records both ages and cursor lag, then recovers", async () => {
    vi.useFakeTimers({ now: 1_800_000_000_000 });
    const start = Date.now();
    const alarms: BridgeDeliveryAgeAlarm[] = [];
    const lines: string[] = [];
    const monitor = new BridgeDeliveryAgeMonitor({ host: "hub", peer: "far", thresholdMs: 30_000,
      clock: Date.now, monotonic: () => Date.now() - start, alarm: alarm => { alarms.push(alarm); }, log: line => lines.push(line) });

    const stale = monitor.begin("toRemote", baseEvent(start - 65_000), 17)!;
    stale.complete(start + 5);
    expect(alarms).toHaveLength(1);
    expect(alarms[0]).toMatchObject({ state: "outage", direction: "toRemote", reason: "delivery-age" });
    expect(alarms).toHaveLength(1);

    const fresh = monitor.begin("toRemote", baseEvent(Date.now()), 0)!;
    fresh.complete(Date.now() + 3);
    expect(alarms).toHaveLength(2);
    expect(alarms[1]).toMatchObject({ state: "recovered", direction: "toRemote", maxDeliveryAgeMs: 65_005 });
    monitor.close();
    expect(lines).toHaveLength(1);
    expect(lines[0]).toContain("toRemote count=2");
    expect(lines[0]).toContain("deliveryP50Ms=32504");
    expect(lines[0]).toContain("deliveryMaxMs=65005");
    expect(lines[0]).toContain("cursorLagSequences=17 cursorLagSeconds=65");
    expect(lines[0]).toContain("toLocal count=0");
  });

  it("does not count a durable event refusal as a delivered event or recover an existing delivery outage", async () => {
    vi.useFakeTimers({ now: 1_800_000_000_000 });
    const alarms: BridgeDeliveryAgeAlarm[] = [], lines: string[] = [];
    const monitor = new BridgeDeliveryAgeMonitor({ host: "hub", peer: "far", thresholdMs: 30,
      clock: Date.now, monotonic: Date.now, alarm: alarm => { alarms.push(alarm); }, log: line => lines.push(line) });
    monitor.begin("toRemote", baseEvent(Date.now() - 100))!.complete(Date.now());
    expect(alarms).toHaveLength(1);
    await monitor.track("toRemote", baseEvent(Date.now()), async () => ({ kind: "refused" as const, sequence: 0 }));
    expect(alarms).toHaveLength(1); // no false recovery from a refused event
    monitor.close();
    expect(lines[0]).toContain("toRemote count=1"); // only the real delivery, not the refusal
  });

  it("alarms on delayed durable receipt even when the destination append itself is fast", async () => {
    vi.useFakeTimers({ now: 1_800_000_000_000 });
    const start = Date.now();
    const alarms: BridgeDeliveryAgeAlarm[] = [];
    const monitor = new BridgeDeliveryAgeMonitor({ host: "hub", peer: "far", thresholdMs: 30,
      clock: Date.now, monotonic: () => Date.now() - start, alarm: alarm => { alarms.push(alarm); } });
    let resolvePublish!: (receipt: { sequence: number; createdAt: number }) => void;
    const sent = monitor.track("toLocal", baseEvent(start), () => new Promise<{ sequence: number; createdAt: number }>(resolve => { resolvePublish = resolve; }));
    await vi.advanceTimersByTimeAsync(31);
    expect(alarms).toHaveLength(1);
    expect(alarms[0]).toMatchObject({ state: "outage", reason: "ack-age", maxDeliveryAgeMs: null, maxAckLatencyMs: 31 });
    resolvePublish({ sequence: 1, createdAt: start + 2 });
    await sent;
    const recovery = monitor.begin("toLocal", baseEvent(Date.now()))!;
    recovery.complete(Date.now());
    expect(alarms).toHaveLength(2);
    expect(alarms[1]).toMatchObject({ state: "recovered", maxDeliveryAgeMs: 2, maxAckLatencyMs: 31 });
    monitor.close();
  });

  it("alarms after a command deadline and ignores unrelated topics", async () => {
    vi.useFakeTimers({ now: 1_800_000_000_000 });
    const start = Date.now();
    const alarms: BridgeDeliveryAgeAlarm[] = [];
    const monitor = new BridgeDeliveryAgeMonitor({ host: "hub", peer: "far", thresholdMs: 10_000,
      clock: Date.now, monotonic: () => Date.now() - start, alarm: alarm => { alarms.push(alarm); } });
    expect(monitor.begin("toRemote", { topic: "ops.owner", data: {}, createdAt: start })).toBeUndefined();
    const deadline = monitor.begin("toRemote", baseEvent(start, { deadlineAt: start + 5 }))!;
    deadline.complete(start + 6);
    expect(alarms).toHaveLength(1);
    expect(alarms[0]).toMatchObject({ state: "outage", reason: "command-deadline" });
    const healthy = monitor.begin("toRemote", baseEvent(Date.now()))!;
    healthy.complete(Date.now());
    expect(alarms.map(alarm => alarm.state)).toEqual(["outage", "recovered"]);
    monitor.close();
  });
});
