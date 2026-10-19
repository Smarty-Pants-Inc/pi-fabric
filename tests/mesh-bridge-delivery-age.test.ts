import { afterEach, describe, expect, it, vi } from "vitest";
import { BridgeDeliveryAgeMonitor, type BridgeDeliveryAgeAlarm } from "../src/mesh/bridge-delivery-age.js";

const baseEvent = (createdAt: number, data: Record<string, unknown> = {}) => ({
  topic: "fabric.control.command", data, createdAt,
});

describe("bridge per-direction delivery age", () => {
  afterEach(() => vi.useRealTimers());

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
