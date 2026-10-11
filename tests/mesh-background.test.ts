import os from "node:os";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MESH_OUTAGE_ALARM_AFTER_MS, MESH_OUTAGE_ALARM_TAG, MESH_OUTAGE_RECOVERY_SUCCESSES, MeshBackgroundQueue, MeshBackgroundRetry, type MeshOutageAlarm } from "../src/core/atomic-write.js";
import { FABRIC_MESH_OUTAGE_ALARM_TOPIC, meshAlarmRecipient, meshOutageAlarmSink } from "../src/mesh/store.js";

const CHIEF = "session:0a1b2c3d-0000-4000-8000-00000000c0f5";
import { MeshLockTimeoutError } from "../src/mesh.js";

const busy = () => new MeshLockTimeoutError(" held by pid 123 (alive, running)", 6, 8020);
afterEach(() => { vi.restoreAllMocks(); vi.useRealTimers(); });

describe("background mesh retry boundary", () => {
  it("backs off only typed timeouts, caps delay, logs holder once and resets backoff after recovery", async () => {
    vi.useFakeTimers();
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    vi.spyOn(Math, "random").mockReturnValue(0.5);
    const retry = new MeshBackgroundRetry("poll", 100, 400);
    const operation = vi.fn(async () => { throw busy(); });
    expect(await retry.run(operation)).toBe("retry");
    expect(retry.waitMs).toBe(50);
    expect(await retry.run(operation)).toBe("skipped");
    expect(operation).toHaveBeenCalledTimes(1);
    for (const wait of [50, 100, 200, 200]) {
      await vi.advanceTimersByTimeAsync(wait);
      expect(await retry.run(operation)).toBe("retry");
    }
    expect(retry.waitMs).toBe(200);
    expect(warn).toHaveBeenCalledOnce();
    expect(warn.mock.calls[0]![0]).toContain("held by pid 123");
    await vi.advanceTimersByTimeAsync(400);
    expect(await retry.run(() => undefined)).toBe("done");
    expect(retry.waitMs).toBe(0);
    expect(await retry.run(operation)).toBe("retry");
    expect(retry.waitMs).toBe(50);
    expect(warn).toHaveBeenCalledOnce();
  });

  it("randomizes within each ceiling independently and yields even on a zero draw", async () => {
    vi.useFakeTimers(); vi.spyOn(console, "warn").mockImplementation(() => {});
    const random = vi.spyOn(Math, "random");
    const retry = new MeshBackgroundRetry("poll", 100, 400);
    for (const [draw, ceiling, expected] of [[0, 100, 1], [0.75, 200, 150], [0.25, 400, 100], [0.999, 400, 399]]) {
      random.mockReturnValue(draw!); retry.failure(busy());
      expect(retry.waitMs).toBe(expected); expect(retry.waitMs).toBeLessThan(ceiling!);
      await vi.advanceTimersByTimeAsync(expected!);
    }
    retry.success(); random.mockReturnValue(0.75); retry.failure(busy()); expect(retry.waitMs).toBe(75);
  });

  it("contains sync/async non-lock bugs visibly without imposing lock backoff", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const retry = new MeshBackgroundRetry("handler");
    expect(await retry.run(() => { throw new Error("bug"); })).toBe("failed");
    expect(await retry.run(async () => { throw new Error("another bug"); })).toBe("failed");
    expect(retry.waitMs).toBe(0);
    expect(warn).toHaveBeenCalledTimes(2);
    expect(await retry.run(() => undefined)).toBe("done");
  });

  it("serializes one-shot events and retains them across timeouts without blocking later hook admission", async () => {
    vi.useFakeTimers(); vi.spyOn(console, "warn").mockImplementation(() => {});
    vi.spyOn(Math, "random").mockReturnValue(0.999);
    const queue = new MeshBackgroundQueue("events");
    let locked = true;
    const written: number[] = [];
    const first = vi.fn(() => { if (locked) throw busy(); written.push(1); });
    await queue.enqueue(first); // first failed attempt, not an infinite lock wait
    await queue.enqueue(() => written.push(2)); // admitted during outage without waiting
    expect(written).toEqual([]);
    await vi.advanceTimersByTimeAsync(100);
    expect(first).toHaveBeenCalledTimes(2);
    locked = false;
    await vi.advanceTimersByTimeAsync(200);
    expect(written).toEqual([1, 2]);
    await queue.close();
    expect(vi.getTimerCount()).toBe(0);
  });

  it("never retries an untyped/permanent failure and continues the notification queue", async () => {
    vi.spyOn(console, "warn").mockImplementation(() => {});
    const queue = new MeshBackgroundQueue("events");
    const fail = vi.fn(() => { throw new Error("Timed out waiting for the Fabric mesh lock"); });
    await queue.enqueue(fail);
    const later = vi.fn();
    await queue.enqueue(later);
    await queue.close();
    expect(fail).toHaveBeenCalledOnce(); expect(later).toHaveBeenCalledOnce();
  });

  it("release checkpoints reject pending retries until publication is confirmed", async () => {
    vi.useFakeTimers(); vi.spyOn(console, "warn").mockImplementation(() => {});
    const queue = new MeshBackgroundQueue("release");
    let locked = true;
    const publish = vi.fn(() => { if (locked) throw busy(); });
    await queue.enqueue(publish);
    await expect(queue.checkpointForRelease()).rejects.toThrow("unconfirmed publication");
    locked = false;
    await vi.advanceTimersByTimeAsync(100);
    await expect(queue.checkpointForRelease()).resolves.toBeUndefined();
    expect(publish).toHaveBeenCalledTimes(2);
    await queue.close();
  });

  it("release checkpoints remember permanent publication failures despite later successes", async () => {
    vi.spyOn(console, "warn").mockImplementation(() => {});
    const queue = new MeshBackgroundQueue("release");
    await queue.enqueue(() => { throw new Error("broken receipt"); });
    await queue.enqueue(() => undefined);
    await expect(queue.checkpointForRelease()).rejects.toThrow("unconfirmed publication");
    await queue.close();
  });

  it("close cancels pending retries, settles hook admission and fences the in-flight operation", async () => {
    vi.useFakeTimers(); vi.spyOn(console, "warn").mockImplementation(() => {});
    const queue = new MeshBackgroundQueue("events");
    const fail = vi.fn(() => { throw busy(); });
    await queue.enqueue(fail);
    await queue.close();
    await vi.advanceTimersByTimeAsync(10_000);
    expect(fail).toHaveBeenCalledOnce();
    expect(vi.getTimerCount()).toBe(0);
    const running = new MeshBackgroundQueue("running");
    let release!: () => void;
    const admitted = running.enqueue(() => new Promise<void>(resolve => { release = resolve; }));
    await Promise.resolve(); await Promise.resolve();
    let closed = false;
    const closing = running.close().then(() => { closed = true; });
    await Promise.resolve(); expect(closed).toBe(false);
    release(); await admitted; await closing;
    expect(closed).toBe(true);
  });
});

// smarty-dev#8825: a deferred-write outage that outlasts MESH_OUTAGE_ALARM_AFTER_MS raises ONE mesh alarm per path,
// and a confirmed write clears it (with one recovery event).
describe("background outage alarm (smarty-dev#8825)", () => {
  const capped = () => Object.assign(new Error("Fabric mesh state write refused: WAL cap"), { code: "FABRIC_MESH_STATE_WAL_CAP" });
  it("alarms once when a lock-timeout or WAL-cap outage outlasts the bound, and clears on success", async () => {
    vi.useFakeTimers();
    vi.spyOn(console, "warn").mockImplementation(() => {});
    const alarms: MeshOutageAlarm[] = [];
    const retry = new MeshBackgroundRetry("poll", 100, 400).alarmsTo((alarm) => { alarms.push(alarm); });
    for (let elapsed = 0; elapsed <= MESH_OUTAGE_ALARM_AFTER_MS * 2; elapsed += 30_000) {
      retry.failure(elapsed % 60_000 ? capped() : busy());
      await vi.advanceTimersByTimeAsync(30_000);
    }
    expect(alarms).toEqual([expect.objectContaining({ label: "poll", state: "outage", durationMs: MESH_OUTAGE_ALARM_AFTER_MS })]);
    // The journal line is written at alarm time, tagged, whatever the mesh does.
    expect(vi.mocked(console.warn).mock.calls.filter(([line]) => String(line).startsWith(`${MESH_OUTAGE_ALARM_TAG} {`))).toHaveLength(1);
    for (let i = 0; i < MESH_OUTAGE_RECOVERY_SUCCESSES; i++) { retry.success(); await vi.advanceTimersByTimeAsync(0); }
    expect(alarms.map((alarm) => alarm.state)).toEqual(["outage", "recovered"]);
    // A new outage shorter than the bound raises nothing; a non-deferred failure never counts as one.
    retry.failure(busy());
    await vi.advanceTimersByTimeAsync(MESH_OUTAGE_ALARM_AFTER_MS - 1);
    retry.failure(new Error("bug"));
    for (let i = 0; i < MESH_OUTAGE_RECOVERY_SUCCESSES; i++) retry.success();
    await vi.advanceTimersByTimeAsync(0);
    expect(alarms).toHaveLength(2);
  });

  it("a flapping success does not restart the window: the alarm still fires 5 min after the outage began", async () => {
    vi.useFakeTimers();
    vi.spyOn(console, "warn").mockImplementation(() => {});
    const alarms: MeshOutageAlarm[] = [];
    const retry = new MeshBackgroundRetry("poll", 100, 400).alarmsTo((alarm) => { alarms.push(alarm); });
    for (let elapsed = 0; elapsed <= MESH_OUTAGE_ALARM_AFTER_MS; elapsed += 60_000) {
      retry.failure(busy());
      retry.success(); // one write gets through between refusals
      retry.failure(busy());
      await vi.advanceTimersByTimeAsync(60_000);
    }
    expect(alarms).toEqual([expect.objectContaining({ state: "outage", durationMs: MESH_OUTAGE_ALARM_AFTER_MS })]);
  });

  it("a failed alarm publish is flushed before its recovery, and a failed recovery publish is kept and retried, in order", async () => {
    vi.useFakeTimers();
    vi.spyOn(console, "warn").mockImplementation(() => {});
    const accepted: MeshOutageAlarm[] = [];
    let failures = 0;
    const retry = new MeshBackgroundRetry("inbox", 100, 400).alarmsTo(async (alarm) => {
      if (failures > 0) { failures--; throw busy(); }
      accepted.push(alarm);
    });
    failures = 1; // the alarm's first publish hits the same outage
    retry.failure(busy());
    await vi.advanceTimersByTimeAsync(MESH_OUTAGE_ALARM_AFTER_MS);
    retry.failure(busy());
    await vi.advanceTimersByTimeAsync(0);
    expect(accepted).toEqual([]);
    // The writes come back; the pending alarm goes first, then the recovery (whose first publish also fails).
    retry.success(); await vi.advanceTimersByTimeAsync(0);
    expect(accepted.map((alarm) => alarm.state)).toEqual(["outage"]);
    failures = 1;
    retry.success(); retry.success(); await vi.advanceTimersByTimeAsync(0);
    expect(accepted.map((alarm) => alarm.state)).toEqual(["outage"]); // recovery rejected: kept
    retry.success(); await vi.advanceTimersByTimeAsync(0);
    expect(accepted.map((alarm) => alarm.state)).toEqual(["outage", "recovered"]);
  });

  it("retries the alarm publish when it fails, and publishes a fabric.alarm.mesh-outage event through the owner's mesh", async () => {
    vi.useFakeTimers();
    vi.stubEnv("PI_FABRIC_MESH_ALARM_TO", CHIEF);
    vi.spyOn(console, "warn").mockImplementation(() => {});
    const published: unknown[] = [];
    let fail = true;
    const mesh = { publish: async (input: unknown) => { if (fail) throw busy(); published.push(input); return input; } };
    const from = { id: "session:outage", name: "main", kind: "main" as const };
    const retry = new MeshBackgroundRetry("inbox", 100, 400)
      .alarmsTo(meshOutageAlarmSink(() => ({ mesh: mesh as never, from })));
    retry.failure(busy());
    await vi.advanceTimersByTimeAsync(MESH_OUTAGE_ALARM_AFTER_MS);
    retry.failure(busy()); // past the bound: the publish fails under the same outage
    await vi.advanceTimersByTimeAsync(1_000);
    fail = false;
    retry.failure(busy()); // tried again
    await vi.advanceTimersByTimeAsync(1_000);
    retry.failure(busy()); // already sent: not again
    await vi.advanceTimersByTimeAsync(1_000);
    expect(published).toEqual([expect.objectContaining({ topic: FABRIC_MESH_OUTAGE_ALARM_TOPIC, kind: "mesh-outage", from,
      to: CHIEF, data: expect.objectContaining({ label: "inbox", state: "outage", reason: "mesh lock timeout" }) })]);
    vi.unstubAllEnvs();
  });

  // pi-fabric#893 round 4 (owner decision, fabric-store): exactly two forms; anything else is refused.
  it("accepts only session:<uuid> or topic:fabric.alarm.<name>, and falls back to a session-form SMARTY_LEAD_SESSION", () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const lead = "session:11111111-2222-4333-8444-555555555555";
    expect(meshAlarmRecipient({ PI_FABRIC_MESH_ALARM_TO: CHIEF })).toEqual({ to: CHIEF });
    expect(meshAlarmRecipient({ PI_FABRIC_MESH_ALARM_TO: "topic:fabric.alarm.mesh-ops" })).toEqual({ topic: "fabric.alarm.mesh-ops" });
    for (const value of ["org", "org-kate", "fabric-store", "*", ` ${CHIEF}`, `${CHIEF}\n`, CHIEF.toUpperCase(),
      "session:not-a-uuid", "topic:fabric.alarm.", "topic:fabric.alarm.Mesh", "topic:ops.owner", "fabric.alarm.mesh-ops"]) {
      expect(meshAlarmRecipient({ PI_FABRIC_MESH_ALARM_TO: value }), value).toBeUndefined();
      expect(meshAlarmRecipient({ PI_FABRIC_MESH_ALARM_TO: value, SMARTY_LEAD_SESSION: lead }), value).toEqual({ to: lead });
      expect(meshAlarmRecipient({ PI_FABRIC_MESH_ALARM_TO: value, SMARTY_LEAD_SESSION: "org" }), value).toBeUndefined();
    }
    const refusals = warn.mock.calls.filter(([line]) => String(line).includes("PI_FABRIC_MESH_ALARM_TO refused"));
    expect(refusals.length).toBeGreaterThan(0);
    expect(refusals.length).toBeLessThanOrEqual(12); // one line per refused value, not per alarm
    expect(meshAlarmRecipient({})).toBeUndefined();
  });

  it("publishes a topic-form alarm on that topic, with no recipient field", async () => {
    vi.useFakeTimers();
    vi.stubEnv("PI_FABRIC_MESH_ALARM_TO", "topic:fabric.alarm.mesh-ops");
    vi.spyOn(console, "warn").mockImplementation(() => {});
    const published: Array<Record<string, unknown>> = [];
    const mesh = { publish: async (input: Record<string, unknown>) => { published.push(input); return input; } };
    const retry = new MeshBackgroundRetry("inbox", 100, 400)
      .alarmsTo(meshOutageAlarmSink(() => ({ mesh: mesh as never, from: { id: "session:x", name: "main", kind: "main" } })));
    retry.failure(busy());
    await vi.advanceTimersByTimeAsync(MESH_OUTAGE_ALARM_AFTER_MS);
    retry.failure(busy());
    await vi.advanceTimersByTimeAsync(0);
    expect(published).toEqual([expect.objectContaining({ topic: "fabric.alarm.mesh-ops", kind: "mesh-outage" })]);
    expect(published[0]).not.toHaveProperty("to");
    vi.unstubAllEnvs();
  });

  // pi-fabric#893 round 3 (R-org-delegate): an alarm goes to one recipient, never to the whole shared mesh.
  it("never publishes an outage alarm without a recipient: stderr only", async () => {
    vi.useFakeTimers();
    vi.stubEnv("PI_FABRIC_MESH_ALARM_TO", "");
    vi.stubEnv("SMARTY_LEAD_SESSION", "");
    vi.spyOn(process, "cwd").mockReturnValue(os.tmpdir()); // no .local/lead above it
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const publish = vi.fn(async (input: unknown) => input);
    const retry = new MeshBackgroundRetry("inbox", 100, 400)
      .alarmsTo(meshOutageAlarmSink(() => ({ mesh: { publish } as never, from: { id: "session:x", name: "main", kind: "main" } })));
    retry.failure(busy());
    await vi.advanceTimersByTimeAsync(MESH_OUTAGE_ALARM_AFTER_MS);
    retry.failure(busy());
    await vi.advanceTimersByTimeAsync(0);
    expect(publish).not.toHaveBeenCalled();
    expect(warn.mock.calls.some(([line]) => String(line).startsWith(`${MESH_OUTAGE_ALARM_TAG} {`))).toBe(true);
    vi.unstubAllEnvs();
  });

  // pi-fabric#893 round 3: a WAL-cap refusal's message carries reader pids and command lines; the retry line never does.
  it("logs a WAL-cap retry without the readers' pids or command lines", () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const refused = Object.assign(new Error("Fabric mesh state write refused ... (WAL readers: pid 4242 (S, 3m, node --token=hunter2) slot 0)"),
      { code: "FABRIC_MESH_STATE_WAL_CAP", walBytes: 120, capBytes: 96, readerCount: 1 });
    new MeshBackgroundRetry("poll", 100, 400).failure(refused);
    const lines = warn.mock.calls.map(([line]) => String(line)).join("\n");
    expect(lines).toContain("120 bytes, above the 96-byte cap; 1 WAL reader(s)");
    expect(lines).not.toMatch(/4242|hunter2|--token/);
  });

  // pi-fabric#893 CODE P2: any failure, deferred or not, breaks the recovery streak.
  it("success, success, a non-deferred failure, then success emits no recovery", async () => {
    vi.useFakeTimers();
    vi.spyOn(console, "warn").mockImplementation(() => {});
    const alarms: MeshOutageAlarm[] = [];
    const retry = new MeshBackgroundRetry("poll", 100, 400).alarmsTo((alarm) => { alarms.push(alarm); });
    retry.failure(busy());
    await vi.advanceTimersByTimeAsync(MESH_OUTAGE_ALARM_AFTER_MS);
    retry.failure(busy());
    retry.success(); retry.success();
    retry.failure(new Error("bug"));
    retry.success();
    await vi.advanceTimersByTimeAsync(0);
    expect(alarms.map((alarm) => alarm.state)).toEqual(["outage"]);
    retry.success(true); // a path's final write ends the outage at once
    await vi.advanceTimersByTimeAsync(0);
    expect(alarms.map((alarm) => alarm.state)).toEqual(["outage", "recovered"]);
    expect(retry.idle).toBe(true);
  });
});
