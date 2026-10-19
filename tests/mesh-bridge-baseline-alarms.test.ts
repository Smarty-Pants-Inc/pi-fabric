import { afterEach, describe, expect, it, vi } from "vitest";
import { isBridgedTopic } from "../src/mesh/bridge.js";
import { BridgeLegacyBaselineAlarmEpisode, isLegacyBaselineAlarmEligible, LEGACY_BASELINE_ALARM_AFTER_MS,
  LEGACY_BASELINE_ALARM_RETRY_MS, type BridgeLegacyBaselineAlarm } from "../src/mesh/bridge-baseline-alarms.js";

const flush = async (): Promise<void> => {
  await new Promise<void>(resolve => setImmediate(resolve));
  await Promise.resolve();
};

afterEach(() => {
  vi.useRealTimers();
});

describe("legacy baseline alarm episode", () => {
  it("pages only a legacy peer with an active no-baseline episode", () => {
    expect(isLegacyBaselineAlarmEligible(false, 1)).toBe(true);
    expect(isLegacyBaselineAlarmEligible(true, 1)).toBe(false);
    expect(isLegacyBaselineAlarmEligible(undefined, 1)).toBe(false);
    expect(isLegacyBaselineAlarmEligible(false, 0)).toBe(false);
  });

  it("allows only addressed baseline alarm and recovery events across the bridge", () => {
    const to = "session:12345678-1234-1234-1234-123456789abc";
    expect(isBridgedTopic({ topic: "ops.owner", kind: "bridge.legacy-baseline.alarm", to })).toBe(true);
    expect(isBridgedTopic({ topic: "ops.owner", kind: "bridge.legacy-baseline.recovered", to })).toBe(true);
    expect(isBridgedTopic({ topic: "ops.owner", kind: "bridge.legacy-baseline.alarm" })).toBe(false);
  });

  it("retries a failed page on the next due cycle and clears after recovery", async () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    const now = 1_000_000;
    vi.setSystemTime(now);
    const sinceMs = now - LEGACY_BASELINE_ALARM_AFTER_MS - 1;
    const episode = new BridgeLegacyBaselineAlarmEpisode();
    const attempts: BridgeLegacyBaselineAlarm[] = [];
    const errors: unknown[] = [];
    let failOnce = true;
    const publish = async (alarm: BridgeLegacyBaselineAlarm): Promise<void> => {
      attempts.push(alarm);
      if (alarm.state === "alarm" && failOnce) {
        failOnce = false;
        throw new Error("temporary mesh publish failure");
      }
    };
    const onError = (error: unknown): void => { errors.push(error); };

    expect(episode.nextAttemptAt(sinceMs)).toBe(now);
    episode.observe({ peer: "intel1", sinceMs, at: now - 1 }, publish, onError);
    await flush();
    expect(attempts).toEqual([]);
    episode.observe({ peer: "intel1", sinceMs, at: now }, publish, onError);
    await flush();
    expect(attempts.map(alarm => alarm.state)).toEqual(["alarm"]);
    expect(errors).toHaveLength(1);
    expect(episode.nextAttemptAt(sinceMs)).toBe(now + LEGACY_BASELINE_ALARM_RETRY_MS);

    episode.observe({ peer: "intel1", sinceMs, at: now + LEGACY_BASELINE_ALARM_RETRY_MS - 1 }, publish, onError);
    await flush();
    expect(attempts.map(alarm => alarm.state)).toEqual(["alarm"]);
    episode.observe({ peer: "intel1", sinceMs, at: now + LEGACY_BASELINE_ALARM_RETRY_MS }, publish, onError);
    await flush();
    expect(attempts.map(alarm => alarm.state)).toEqual(["alarm", "alarm"]);
    expect(episode.nextAttemptAt(sinceMs)).toBeUndefined();

    episode.observe({ peer: "intel1", sinceMs: undefined, at: now + LEGACY_BASELINE_ALARM_RETRY_MS + 1 }, publish, onError);
    await flush();
    expect(attempts.map(alarm => alarm.state)).toEqual(["alarm", "alarm", "recovered"]);
    expect(errors).toHaveLength(1);
  });

  it("publishes an in-flight successful alarm before its recovery", async () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    const now = 2_000_000;
    vi.setSystemTime(now);
    const sinceMs = now - LEGACY_BASELINE_ALARM_AFTER_MS - 1;
    const episode = new BridgeLegacyBaselineAlarmEpisode();
    const states: string[] = [];
    let releaseAlarm!: () => void;
    const alarmBlocked = new Promise<void>(resolve => { releaseAlarm = resolve; });
    const publish = async (alarm: BridgeLegacyBaselineAlarm): Promise<void> => {
      states.push(alarm.state);
      if (alarm.state === "alarm") await alarmBlocked;
    };

    episode.observe({ peer: "intel1", sinceMs, at: now }, publish, () => undefined);
    await flush();
    episode.observe({ peer: "intel1", sinceMs: undefined, at: now + 1 }, publish, () => undefined);
    await flush();
    expect(states).toEqual(["alarm"]);

    releaseAlarm();
    await flush();
    expect(states).toEqual(["alarm", "recovered"]);
  });
});
