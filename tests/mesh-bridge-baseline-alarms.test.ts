import { PassThrough, Transform } from "node:stream";
import { performance as monotonicClock } from "node:perf_hooks";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { isBridgedTopic, MeshBridge, RemoteBridgeSide, serveBridgeAgent, StoreBridgeSide } from "../src/mesh/bridge.js";
import { MeshStore } from "../src/mesh/store.js";
import { BridgeLegacyBaselineAlarmEpisode, supportsLegacyBaselineAlarm, LEGACY_BASELINE_ALARM_AFTER_MS,
  LEGACY_BASELINE_ALARM_PUBLISH_TIMEOUT_MS, LEGACY_BASELINE_ALARM_RETRY_MS, type BridgeLegacyBaselineAlarm } from "../src/mesh/bridge-baseline-alarms.js";

const flush = async (): Promise<void> => {
  await new Promise<void>(resolve => setImmediate(resolve));
  await Promise.resolve();
};

const roots: string[] = [];
const stores: MeshStore[] = [];
const closers: Array<() => Promise<void>> = [];

const openStore = (): MeshStore => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-bridge-baseline-alarm-test-"));
  roots.push(root);
  const store = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100, { stateBackend: "sqlite" });
  stores.push(store);
  return store;
};

afterEach(async () => {
  for (const close of closers.splice(0).reverse()) await close();
  for (const store of stores.splice(0).reverse()) store.closeState();
  vi.useRealTimers();
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("legacy baseline alarm episode", () => {
  it("pages only a legacy peer with an active no-baseline episode", () => {
    expect(supportsLegacyBaselineAlarm(false)).toBe(true);
    expect(supportsLegacyBaselineAlarm(true)).toBe(false);
    expect(supportsLegacyBaselineAlarm(undefined)).toBe(false);
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
    let mono = 50;
    vi.spyOn(monotonicClock, "now").mockImplementation(() => mono);
    const sinceMs = now - LEGACY_BASELINE_ALARM_AFTER_MS - 1;
    const sinceMonotonicMs = 50;
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

    expect(episode.nextAttemptAt(sinceMonotonicMs)).toBe(sinceMonotonicMs + LEGACY_BASELINE_ALARM_AFTER_MS + 1);
    episode.observe({ peer: "intel1", sinceMs, sinceMonotonicMs, at: now - 1,
      monotonicAt: sinceMonotonicMs + LEGACY_BASELINE_ALARM_AFTER_MS }, publish, onError);
    await flush();
    expect(attempts).toEqual([]);
    mono = sinceMonotonicMs + LEGACY_BASELINE_ALARM_AFTER_MS + 1;
    episode.observe({ peer: "intel1", sinceMs, sinceMonotonicMs, at: now, monotonicAt: mono }, publish, onError);
    await flush();
    expect(attempts.map(alarm => alarm.state)).toEqual(["alarm"]);
    expect(errors).toHaveLength(1);
    expect(episode.nextAttemptAt(sinceMonotonicMs)).toBe(mono + LEGACY_BASELINE_ALARM_RETRY_MS);

    episode.observe({ peer: "intel1", sinceMs, sinceMonotonicMs, at: now + LEGACY_BASELINE_ALARM_RETRY_MS - 1,
      monotonicAt: mono + LEGACY_BASELINE_ALARM_RETRY_MS - 1 }, publish, onError);
    await flush();
    expect(attempts.map(alarm => alarm.state)).toEqual(["alarm"]);
    mono += LEGACY_BASELINE_ALARM_RETRY_MS;
    episode.observe({ peer: "intel1", sinceMs, sinceMonotonicMs, at: now + LEGACY_BASELINE_ALARM_RETRY_MS,
      monotonicAt: mono }, publish, onError);
    await flush();
    expect(attempts.map(alarm => alarm.state)).toEqual(["alarm", "alarm"]);
    expect(attempts[1]!.episodeKey).toBe(attempts[0]!.episodeKey);
    expect(episode.nextAttemptAt(sinceMonotonicMs)).toBeUndefined();

    episode.observe({ peer: "intel1", sinceMs: undefined, sinceMonotonicMs: undefined,
      at: now + LEGACY_BASELINE_ALARM_RETRY_MS + 1, monotonicAt: mono + 1 }, publish, onError);
    await flush();
    expect(attempts.map(alarm => alarm.state)).toEqual(["alarm", "alarm", "recovered"]);
    expect(attempts[2]!.episodeKey).toBe(attempts[0]!.episodeKey);
    expect(errors).toHaveLength(1);
  });

  it("publishes recovery on the bridge path after a legacy baseline ACK", async () => {
    let wall = 2_500_000;
    let mono = 10;
    vi.spyOn(Date, "now").mockImplementation(() => wall);
    vi.spyOn(monotonicClock, "now").mockImplementation(() => mono);
    const localStore = openStore();
    const remoteStore = openStore();
    const input = new PassThrough();
    const output = new PassThrough();
    let buffered = "";
    const legacyReplies = new Transform({ transform(chunk, _encoding, done) {
      buffered += String(chunk);
      for (;;) {
        const newline = buffered.indexOf("\n");
        if (newline < 0) break;
        const response = JSON.parse(buffered.slice(0, newline));
        buffered = buffered.slice(newline + 1);
        if (response.result?.partialPresence === true) delete response.result.partialPresence;
        this.push(`${JSON.stringify(response)}\n`);
      }
      done();
    } });
    legacyReplies.pipe(output);
    const local = new StoreBridgeSide(localStore, "intel1");
    const remoteAgent = new StoreBridgeSide(remoteStore, "hub");
    const serving = serveBridgeAgent(remoteAgent, input, legacyReplies);
    const remote = new RemoteBridgeSide(output, input);
    const alarms: BridgeLegacyBaselineAlarm[] = [];
    const bridge = new MeshBridge({ localName: "hub", remoteName: "intel1", local, remote,
      cursorPath: path.join(localStore.root, "intel1.cursor.json"), presenceMs: 0,
      publishLegacyBaselineAlarm: async alarm => { alarms.push(alarm); } });
    closers.push(async () => {
      await bridge.stop();
      remote.close();
      input.end();
      await serving;
      legacyReplies.destroy();
      output.end();
    });

    let localPresence: import("../src/mesh/bridge.js").BridgePresence = {
      hosts: [], participants: [], reserved: [], partial: true, droppedCount: 1,
    };
    vi.spyOn(local, "presence").mockImplementation(async () => localPresence);
    await bridge.syncPresence();
    expect(remote.supportsPartialPresence).toBe(false);
    expect(remote.legacyNoBaselineCycles).toBeGreaterThan(0);

    wall += LEGACY_BASELINE_ALARM_AFTER_MS + 1;
    mono += LEGACY_BASELINE_ALARM_AFTER_MS + 1;
    await bridge.syncPresence();
    await flush();
    expect(alarms.map(alarm => alarm.state)).toEqual(["alarm"]);

    localPresence = { hosts: [], participants: [], reserved: [] };
    await bridge.syncPresence();
    await flush();
    expect(remote.legacyNoBaselineCycles).toBe(0);
    expect(alarms.map(alarm => alarm.state)).toEqual(["alarm", "recovered"]);
    await bridge.syncPresence();
    await flush();
    expect(alarms.map(alarm => alarm.state)).toEqual(["alarm", "recovered"]);
  });

  it("uses monotonic elapsed time for page and retry despite wall-clock jumps", async () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    let wall = 5_000_000;
    let mono = 100;
    vi.spyOn(Date, "now").mockImplementation(() => wall);
    vi.spyOn(monotonicClock, "now").mockImplementation(() => mono);
    const sinceMs = wall;
    const sinceMonotonicMs = mono;
    const episode = new BridgeLegacyBaselineAlarmEpisode();
    const attempts: BridgeLegacyBaselineAlarm[] = [];
    let failOnce = true;
    const publish = async (alarm: BridgeLegacyBaselineAlarm): Promise<void> => {
      attempts.push(alarm);
      if (alarm.state === "alarm" && failOnce) { failOnce = false; throw new Error("temporary failure"); }
    };

    wall += 24 * 60 * 60 * 1_000;
    episode.observe({ peer: "intel1", sinceMs, sinceMonotonicMs, at: wall,
      monotonicAt: mono + 30_000 }, publish, () => undefined);
    await flush();
    expect(attempts).toEqual([]);
    expect(episode.nextAttemptAt(sinceMonotonicMs)).toBe(sinceMonotonicMs + LEGACY_BASELINE_ALARM_AFTER_MS + 1);

    mono += LEGACY_BASELINE_ALARM_AFTER_MS;
    wall = sinceMs - 60 * 60 * 1_000;
    episode.observe({ peer: "intel1", sinceMs, sinceMonotonicMs, at: wall, monotonicAt: mono }, publish, () => undefined);
    await flush();
    expect(attempts).toEqual([]);

    mono += 1;
    episode.observe({ peer: "intel1", sinceMs, sinceMonotonicMs, at: wall, monotonicAt: mono }, publish, () => undefined);
    await flush();
    expect(attempts.map(alarm => alarm.state)).toEqual(["alarm"]);
    expect(attempts[0]).toMatchObject({ sinceMs, at: wall, durationMs: LEGACY_BASELINE_ALARM_AFTER_MS + 1 });
    const retryAt = mono + LEGACY_BASELINE_ALARM_RETRY_MS;
    expect(episode.nextAttemptAt(sinceMonotonicMs)).toBe(retryAt);

    wall += 365 * 24 * 60 * 60 * 1_000;
    mono = retryAt - 1;
    episode.observe({ peer: "intel1", sinceMs, sinceMonotonicMs, at: wall, monotonicAt: mono }, publish, () => undefined);
    await flush();
    expect(attempts.map(alarm => alarm.state)).toEqual(["alarm"]);
    mono += 1;
    episode.observe({ peer: "intel1", sinceMs, sinceMonotonicMs, at: wall, monotonicAt: mono }, publish, () => undefined);
    await flush();
    expect(attempts.map(alarm => alarm.state)).toEqual(["alarm", "alarm"]);
    expect(attempts[1]!.episodeKey).toBe(attempts[0]!.episodeKey);
  });

  it("bounds a hung publish so a queued recovery can proceed", async () => {
    vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout"] });
    let mono = 500;
    vi.spyOn(monotonicClock, "now").mockImplementation(() => mono);
    const now = 3_000_000;
    vi.setSystemTime(now);
    const sinceMs = now - LEGACY_BASELINE_ALARM_AFTER_MS - 1;
    const sinceMonotonicMs = mono;
    const episode = new BridgeLegacyBaselineAlarmEpisode();
    const states: string[] = [];
    const errors: unknown[] = [];
    const publish = async (alarm: BridgeLegacyBaselineAlarm): Promise<void> => {
      states.push(alarm.state);
      if (alarm.state === "alarm") return new Promise<void>(() => undefined);
    };

    mono += LEGACY_BASELINE_ALARM_AFTER_MS + 1;
    episode.observe({ peer: "intel1", sinceMs, sinceMonotonicMs, at: now, monotonicAt: mono }, publish,
      error => errors.push(error));
    await flush();
    episode.observe({ peer: "intel1", sinceMs: undefined, sinceMonotonicMs: undefined, at: now + 1,
      monotonicAt: mono + 1 }, publish, error => errors.push(error));
    await flush();
    expect(states).toEqual(["alarm"]);

    await vi.advanceTimersByTimeAsync(LEGACY_BASELINE_ALARM_PUBLISH_TIMEOUT_MS);
    await flush();
    expect(states).toEqual(["alarm", "recovered"]);
    expect(errors).toHaveLength(1);
    expect(String(errors[0])).toContain("exceeded");
  });

  it("publishes an in-flight successful alarm before its recovery", async () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    const now = 2_000_000;
    vi.setSystemTime(now);
    let mono = 100;
    vi.spyOn(monotonicClock, "now").mockImplementation(() => mono);
    const sinceMs = now - LEGACY_BASELINE_ALARM_AFTER_MS - 1;
    const sinceMonotonicMs = 100;
    const episode = new BridgeLegacyBaselineAlarmEpisode();
    const states: string[] = [];
    let releaseAlarm!: () => void;
    const alarmBlocked = new Promise<void>(resolve => { releaseAlarm = resolve; });
    const publish = async (alarm: BridgeLegacyBaselineAlarm): Promise<void> => {
      states.push(alarm.state);
      if (alarm.state === "alarm") await alarmBlocked;
    };

    mono += LEGACY_BASELINE_ALARM_AFTER_MS + 1;
    episode.observe({ peer: "intel1", sinceMs, sinceMonotonicMs, at: now, monotonicAt: mono }, publish, () => undefined);
    await flush();
    episode.observe({ peer: "intel1", sinceMs: undefined, sinceMonotonicMs: undefined,
      at: now + 1, monotonicAt: mono + 1 }, publish, () => undefined);
    await flush();
    expect(states).toEqual(["alarm"]);

    releaseAlarm();
    await flush();
    expect(states).toEqual(["alarm", "recovered"]);
  });
});
