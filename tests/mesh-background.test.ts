import os from "node:os";
import childProcess from "node:child_process";
import fs from "node:fs";
import { PassThrough } from "node:stream";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { MESH_ALARM_ATTEMPT_MS, MESH_ALARM_MAX_ATTEMPTS, MESH_JOURNAL_ALARM_TAG, MESH_OUTAGE_ALARM_AFTER_MS, MESH_OUTAGE_ALARM_TAG, MESH_OUTAGE_RECOVERY_SUCCESSES, MeshBackgroundQueue, MeshBackgroundRetry, type MeshOutageAlarm } from "../src/core/atomic-write.js";
import { FABRIC_MESH_OUTAGE_ALARM_TOPIC, meshAlarmRecipient, meshOutageAlarmSink } from "../src/mesh/store.js";

const CHIEF = "session:0a1b2c3d-0000-4000-8000-00000000c0f5";
import { parse9729 } from "./helpers/alarm-contract.js";
import { MeshLockTimeoutError } from "../src/mesh.js";

const busy = () => new MeshLockTimeoutError(" held by pid 123 (alive, running)", 6, 8020);
// Safety gate: ALL alarm tests use an inert child. No unit test writes live journal alarms.
beforeEach(() => {
  vi.spyOn(childProcess, "execFile").mockImplementation(() => {
    const child = new childProcess.ChildProcess();
    child.stdin = new PassThrough();
    return child;
  });
});
afterEach(() => { vi.restoreAllMocks(); vi.unstubAllEnvs(); vi.unstubAllGlobals(); vi.useRealTimers(); });

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
  it("accepts only session:<uuid>, refuses the topic form and every other value, and falls back to a session-form lead", () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const lead = "session:11111111-2222-4333-8444-555555555555";
    expect(meshAlarmRecipient({ PI_FABRIC_MESH_ALARM_TO: CHIEF })).toBe(CHIEF);
    for (const value of ["topic:fabric.alarm.mesh-ops", "topic:fabric.alarm.mesh-outage", "org", "org-kate", "fabric-store", "*",
      ` ${CHIEF}`, `${CHIEF}\n`, CHIEF.toUpperCase(), "session:not-a-uuid", "topic:ops.owner", "fabric.alarm.mesh-ops"]) {
      expect(meshAlarmRecipient({ PI_FABRIC_MESH_ALARM_TO: value }), value).toBeUndefined();
      expect(meshAlarmRecipient({ PI_FABRIC_MESH_ALARM_TO: value, SMARTY_LEAD_SESSION: lead }), value).toBe(lead);
      expect(meshAlarmRecipient({ PI_FABRIC_MESH_ALARM_TO: value, SMARTY_LEAD_SESSION: "topic:fabric.alarm.x" }), value).toBeUndefined();
    }
    const refusals = warn.mock.calls.filter(([line]) => String(line).includes("PI_FABRIC_MESH_ALARM_TO refused"));
    expect(refusals.length).toBeGreaterThan(0);
    expect(refusals.length).toBeLessThanOrEqual(12); // one line per refused value, not per alarm
    expect(meshAlarmRecipient({})).toBeUndefined();
  });

  // pi-fabric#893 round 5: undelivered alarm events are bounded in count and in retries; a drop leaves one journal line.
  it("keeps at most 32 queued events, dropping the oldest, with one fabric-alarm journal line", async () => {
    vi.useFakeTimers();
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    // A publish that never settles (a hung mesh), inside one attempt deadline: nothing is delivered or retried yet, so
    // only the cap bounds the queue. A 1 ms outage bound keeps the whole run far inside MESH_ALARM_ATTEMPT_MS.
    const retry = new MeshBackgroundRetry("inbox", 100, 400).alarmsTo(() => new Promise(() => {}), 1);
    for (let outage = 0; outage < 20; outage++) { // 20 outages: 40 events (alarm and recovery), none delivered
      retry.failure(busy());
      await vi.advanceTimersByTimeAsync(2);
      retry.failure(busy());
      retry.success(true);
      await vi.advanceTimersByTimeAsync(0);
    }
    expect(retry.idle).toBe(false);
    const lines = warn.mock.calls.map(([line]) => String(line));
    // Every stderr alarm line, the outage and recovery lines included, is in the #9729 contract.
    for (const line of lines.filter((line) => line.startsWith(`${MESH_JOURNAL_ALARM_TAG} `))) expect(parse9729(line), line).toBeDefined();
    const journal = lines.map(parse9729).filter((alarm) => alarm?.reason === "mesh-alarm-undeliverable");
    expect(journal).toHaveLength(1);
    expect(journal[0]).toMatchObject({ label: "inbox", state: "outage", droppedOverflow: 1, droppedAfterRetries: 0, queued: 32 });
  });

  // pi-fabric#893 Astra P1 (comment 6119182479): an overflow while the head's publish is in flight drops (and counts)
  // that head; when the publish settles, the flush must not then remove the NEXT event unattempted.
  it("loses no event when an overflow drops the in-flight head: each is attempted or counted as dropped", async () => {
    vi.useFakeTimers();
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const attempted: string[] = [];
    let release!: () => void;
    const first = new Promise<void>((resolve) => { release = resolve; });
    // A 1 ms outage bound keeps the whole run inside one attempt deadline (smarty-dev#9848), so event 1 stays in flight.
    const retry = new MeshBackgroundRetry("inbox", 100, 400).alarmsTo((alarm) => {
      attempted.push(JSON.stringify(alarm));
      return attempted.length === 1 ? first : Promise.resolve(); // a delayed sink: the first publish hangs
    }, 1);
    // 16 outages (alarm and recovery) and one more alarm: 33 events, one over the cap of 32, while event 1 is in
    // flight. The overflow drops event 1 itself (attempted, and counted).
    for (let outage = 0; outage <= 16; outage++) {
      retry.failure(busy());
      await vi.advanceTimersByTimeAsync(2);
      retry.failure(busy());
      if (outage < 16) retry.success(true);
      await vi.advanceTimersByTimeAsync(0);
    }
    const raised = warn.mock.calls.map(([line]) => parse9729(String(line)))
      .filter((alarm) => alarm !== undefined && alarm.reason !== "mesh-alarm-undeliverable")
      .map((alarm) => JSON.stringify(alarm));
    expect(raised).toHaveLength(33);
    const dropped = warn.mock.calls.map(([line]) => parse9729(String(line))).filter((alarm) => alarm?.reason === "mesh-alarm-undeliverable");
    expect(dropped).toHaveLength(1);
    expect(dropped[0]).toMatchObject({ droppedOverflow: 1, droppedAfterRetries: 0, queued: 32 });
    expect(attempted).toEqual([raised[0]]);
    release();
    await vi.advanceTimersByTimeAsync(0);
    await retry.flushAlarms();
    // The one overflow drop was the in-flight event 1, already attempted: every one of the 33 was attempted.
    expect(attempted).toEqual(raised);
    for (let i = 0; i < MESH_OUTAGE_RECOVERY_SUCCESSES; i++) retry.success(true);
    await retry.flushAlarms();
  });

  // smarty-dev#9848: a publish that never settles counts as a failed attempt at MESH_ALARM_ATTEMPT_MS, so the attempt
  // limit still drains the queue; a late settle changes nothing. pi-fabric#979 Astra P2: with NO other trigger (no
  // flushAlarms(), failure() or success() call), advancing the deadlines alone reaches the bounded drop.
  it("a sink that never settles still runs the attempt limit: the event is dropped once and the queue drains", async () => {
    vi.useFakeTimers();
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    let attempts = 0;
    const retry = new MeshBackgroundRetry("inbox", 100, 400).alarmsTo(() => { attempts++; return new Promise(() => {}); });
    retry.failure(busy());
    await vi.advanceTimersByTimeAsync(MESH_OUTAGE_ALARM_AFTER_MS);
    retry.failure(busy()); // the alarm: attempt 1, which never settles
    for (let i = 0; i < MESH_ALARM_MAX_ATTEMPTS; i++) await vi.advanceTimersByTimeAsync(MESH_ALARM_ATTEMPT_MS);
    expect(attempts).toBe(MESH_ALARM_MAX_ATTEMPTS);
    expect(retry.idle).toBe(false); // the outage itself is still open; only the alarm queue drained
    await vi.advanceTimersByTimeAsync(MESH_ALARM_ATTEMPT_MS * 4);
    expect(attempts).toBe(MESH_ALARM_MAX_ATTEMPTS); // dropped: no further attempt
    const journal = warn.mock.calls.map(([line]) => parse9729(String(line))).filter((alarm) => alarm?.reason === "mesh-alarm-undeliverable");
    expect(journal).toHaveLength(1);
    expect(journal[0]).toMatchObject({ droppedAfterRetries: 1, queued: 0 });
    retry.success(true);
    await vi.advanceTimersByTimeAsync(MESH_ALARM_ATTEMPT_MS);
  });

  it("close() clears the retry a timed-out attempt scheduled: no attempt after close", async () => {
    vi.useFakeTimers();
    vi.spyOn(console, "warn").mockImplementation(() => {});
    let attempts = 0;
    const retry = new MeshBackgroundRetry("inbox", 100, 400).alarmsTo(() => { attempts++; return new Promise(() => {}); });
    retry.failure(busy());
    await vi.advanceTimersByTimeAsync(MESH_OUTAGE_ALARM_AFTER_MS);
    retry.failure(busy()); // attempt 1, which never settles
    await vi.advanceTimersByTimeAsync(MESH_ALARM_ATTEMPT_MS); // attempt 1 times out and schedules one retry
    expect(attempts).toBe(1);
    retry.close(); // clears that retry before it runs
    await vi.advanceTimersByTimeAsync(MESH_ALARM_ATTEMPT_MS * 4);
    expect(attempts).toBe(1);
  });

  it("drops an event after 8 failed attempts with one final journal line, and the queue empties", async () => {
    vi.useFakeTimers();
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    let attempts = 0;
    const retry = new MeshBackgroundRetry("inbox", 100, 400).alarmsTo(async () => { attempts++; throw busy(); });
    retry.failure(busy());
    await vi.advanceTimersByTimeAsync(MESH_OUTAGE_ALARM_AFTER_MS);
    retry.failure(busy()); // the alarm: attempt 1
    for (let i = 0; i < 20; i++) { await vi.advanceTimersByTimeAsync(1_000); await retry.flushAlarms(); }
    expect(attempts).toBe(MESH_ALARM_MAX_ATTEMPTS);
    const journal = warn.mock.calls.map(([line]) => parse9729(String(line))).filter((alarm) => alarm?.reason === "mesh-alarm-undeliverable");
    expect(journal).toHaveLength(1);
    expect(journal[0]).toMatchObject({ label: "inbox", state: "outage", droppedOverflow: 0, droppedAfterRetries: 1, queued: 0 });
    for (let i = 0; i < MESH_OUTAGE_RECOVERY_SUCCESSES; i++) retry.success(true);
    await retry.flushAlarms();
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
    // The stderr-only fallback line is in the #9729 contract.
    expect(warn.mock.calls.some(([line]) => parse9729(String(line))?.state === "outage")).toBe(true);
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

// smarty-dev#9657: drive the real emitter, but never run the native journal tool.
describe("pane outage journal mirror (smarty-dev#9657)", () => {
  type Callback = (error: childProcess.ExecFileException | null, stdout: string, stderr: string) => void;
  let journal: typeof import("../src/core/atomic-write.js");
  let writes: string[];
  let callbacks: Callback[];
  let children: childProcess.ChildProcess[];
  let fault: string;
  const stderrLines = () => vi.mocked(console.warn).mock.calls.map(([line]) => String(line))
    .filter((line) => line.startsWith(`${MESH_OUTAGE_ALARM_TAG} {`));
  const nativeError = (code: string | number) => Object.assign(new Error("journal unavailable"), { code, cmd: "systemd-cat" });
  const raiseOutage = async (retry: MeshBackgroundRetry) => {
    expect(retry.failure(busy())).toBe(true);
    await vi.advanceTimersByTimeAsync(1);
    expect(retry.failure(busy())).toBe(true);
  };
  beforeEach(async () => {
    vi.useFakeTimers();
    vi.stubGlobal("process", { ...process, platform: "linux" });
    vi.stubEnv("JOURNAL_STREAM", undefined);
    vi.spyOn(console, "warn").mockImplementation(() => {});
    writes = []; callbacks = []; children = []; fault = "";
    vi.mocked(childProcess.execFile).mockImplementation(((_file: string, _args: string[], options: childProcess.ExecFileOptions,
      callback: Callback) => {
      if (fault === "spawn-throw") throw nativeError("EACCES");
      if (fault === "ENOENT-throw") throw nativeError("ENOENT");
      const child = new childProcess.ChildProcess();
      child.stdin = fault === "missing-stdin" ? null : new PassThrough();
      child.stdin?.on("data", (chunk) => writes.push(String(chunk)));
      if (fault === "stdin-throw") vi.spyOn(child.stdin!, "end").mockImplementation(() => { throw nativeError("EPIPE"); });
      if (fault === "timeout") setTimeout(() => callback(Object.assign(nativeError("ETIMEDOUT"), { killed: true }), "", ""), options.timeout);
      callbacks.push(callback); children.push(child);
      return child;
    }) as typeof childProcess.execFile);
    // Fresh private memo/counter for each test, without a production reset/config seam.
    vi.resetModules();
    journal = await import("../src/core/atomic-write.js");
  });

  it("does no journal subprocess or fstat work on import, idle or an ordinary retry warning", async () => {
    const stat = vi.spyOn(fs, "fstatSync");
    vi.stubEnv("JOURNAL_STREAM", "1:2");
    vi.resetModules();
    const cold = await import("../src/core/atomic-write.js");
    const retry = new cold.MeshBackgroundRetry("idle");
    await vi.advanceTimersByTimeAsync(10);
    retry.failure(busy()); retry.success(true);
    expect(childProcess.execFile).not.toHaveBeenCalled();
    expect(stat).not.toHaveBeenCalled();
  });

  it("mirrors each outage and recovery line when JOURNAL_STREAM is unset", async () => {
    vi.stubEnv("PI_FABRIC_LANDLOCK_TEST", "reserved");
    const stat = vi.spyOn(fs, "fstatSync");
    const accepted: MeshOutageAlarm[] = [];
    const retry = new journal.MeshBackgroundRetry("pane").alarmsTo((alarm) => { accepted.push(alarm); }, 1);
    await raiseOutage(retry);
    retry.success(true); await retry.flushAlarms();
    expect(accepted.map((alarm) => alarm.state)).toEqual(["outage", "recovered"]);
    expect(childProcess.execFile).toHaveBeenCalledTimes(2);
    expect(writes).toEqual(stderrLines().map((line) => `${line}\n`));
    expect(writes).toHaveLength(2);
    expect(stat).not.toHaveBeenCalled();
    for (const [file, args, options] of vi.mocked(childProcess.execFile).mock.calls) {
      expect(file).toBe("systemd-cat");
      expect(args).toEqual(["--identifier", MESH_OUTAGE_ALARM_TAG]);
      expect(options).toMatchObject({ timeout: 2_000, maxBuffer: 4_096 });
      expect(options).not.toHaveProperty("shell");
      expect((options as childProcess.ExecFileOptions).env).not.toHaveProperty("PI_FABRIC_LANDLOCK_TEST");
    }
    for (const callback of callbacks) callback(null, "", "");
    expect(journal.meshJournalMirrorFailures()).toBe(0);
  });

  it.each(["", "garbage", "1", "1:", "1:2:3", " 1:2", "1:2\n", "1:2", "9007199254740992:9007199254740996"])
    ("mirrors identical lines for malformed or mismatched JOURNAL_STREAM %j", async (stream) => {
      vi.stubEnv("JOURNAL_STREAM", stream);
      vi.spyOn(fs, "fstatSync").mockReturnValue({ dev: 9007199254740993n, ino: 9007199254740995n } as fs.BigIntStats);
      const retry = new journal.MeshBackgroundRetry("pane").alarmsTo(() => {}, 1);
      await raiseOutage(retry); retry.success(true); await retry.flushAlarms();
      expect(writes).toEqual(stderrLines().map((line) => `${line}\n`));
      expect(writes).toHaveLength(2);
    });

  it("does not duplicate stderr already bound to the exact journal dev:ino, using bigint fstat", async () => {
    vi.stubEnv("JOURNAL_STREAM", "9007199254740993:9007199254740995");
    const stat = vi.spyOn(fs, "fstatSync").mockReturnValue({ dev: 9007199254740993n, ino: 9007199254740995n } as fs.BigIntStats);
    const accepted: MeshOutageAlarm[] = [];
    const retry = new journal.MeshBackgroundRetry("pane").alarmsTo((alarm) => { accepted.push(alarm); }, 1);
    await raiseOutage(retry); retry.success(true); await retry.flushAlarms();
    expect(stat).toHaveBeenCalledTimes(2);
    expect(stat).toHaveBeenCalledWith(2, { bigint: true });
    expect(stderrLines()).toHaveLength(2);
    expect(accepted.map((alarm) => alarm.state)).toEqual(["outage", "recovered"]);
    expect(childProcess.execFile).not.toHaveBeenCalled();
    expect(journal.meshJournalMirrorFailures()).toBe(0);
  });

  it("mirrors when fd 2 identity cannot be read", async () => {
    vi.stubEnv("JOURNAL_STREAM", "1:2");
    vi.spyOn(fs, "fstatSync").mockImplementation(() => { throw nativeError("EBADF"); });
    const retry = new journal.MeshBackgroundRetry("pane").alarmsTo(() => {}, 1);
    await raiseOutage(retry);
    expect(writes).toEqual(stderrLines().map((line) => `${line}\n`));
    expect(writes).toHaveLength(1);
  });

  it.each(["darwin", "win32", "freebsd"])("skips native journal work on %s without blocking the alarm sink", async (platform) => {
    vi.stubGlobal("process", { ...process, platform });
    vi.stubEnv("JOURNAL_STREAM", "1:2");
    const stat = vi.spyOn(fs, "fstatSync");
    const accepted: MeshOutageAlarm[] = [];
    const retry = new journal.MeshBackgroundRetry("pane").alarmsTo((alarm) => { accepted.push(alarm); }, 1);
    await raiseOutage(retry); retry.success(true); await retry.flushAlarms();
    expect(accepted.map((alarm) => alarm.state)).toEqual(["outage", "recovered"]);
    expect(stderrLines()).toHaveLength(2);
    expect(childProcess.execFile).not.toHaveBeenCalled(); expect(stat).not.toHaveBeenCalled();
    expect(journal.meshJournalMirrorFailures()).toBe(0);
  });

  it.each(["ENOENT-callback", "ENOENT-throw"])("counts %s once and skips the absent tool thereafter, across paths", async (mode) => {
    fault = mode;
    const accepted: MeshOutageAlarm[] = [];
    const retry = new journal.MeshBackgroundRetry("pane").alarmsTo((alarm) => { accepted.push(alarm); }, 1);
    await raiseOutage(retry);
    if (mode === "ENOENT-callback") {
      // EPIPE may arrive before the spawn callback: still remember ENOENT and count once.
      expect(() => children[0]!.stdin!.emit("error", nativeError("EPIPE"))).not.toThrow();
      expect(() => callbacks[0]!(nativeError("ENOENT"), "", "")).not.toThrow();
    }
    retry.success(true); await retry.flushAlarms();
    const other = new journal.MeshBackgroundRetry("other").alarmsTo(() => {}, 1);
    await raiseOutage(other);
    expect(accepted.map((alarm) => alarm.state)).toEqual(["outage", "recovered"]);
    expect(childProcess.execFile).toHaveBeenCalledOnce();
    expect(journal.meshJournalMirrorFailures()).toBe(1);
  });

  it.each(["nonzero", "maxBuffer", "stdin", "stdin-throw", "spawn-throw", "missing-stdin"])
    ("contains and counts %s failures without preventing mesh alarm delivery or future attempts", async (mode) => {
      fault = mode;
      const accepted: MeshOutageAlarm[] = [];
      const retry = new journal.MeshBackgroundRetry("pane").alarmsTo((alarm) => { accepted.push(alarm); }, 1);
      await raiseOutage(retry);
      if (mode === "nonzero") expect(() => callbacks[0]!(nativeError(1), "", "")).not.toThrow();
      if (mode === "maxBuffer") expect(() => callbacks[0]!(nativeError("ERR_CHILD_PROCESS_STDIO_MAXBUFFER"), "", "")).not.toThrow();
      if (mode === "stdin") {
        expect(() => children[0]!.stdin!.emit("error", nativeError("EPIPE"))).not.toThrow();
        expect(() => children[0]!.stdin!.emit("error", nativeError("EPIPE"))).not.toThrow();
        expect(() => callbacks[0]!(nativeError(1), "", "")).not.toThrow();
      }
      expect(journal.meshJournalMirrorFailures()).toBe(1);
      fault = "";
      expect(() => retry.success(true)).not.toThrow(); await retry.flushAlarms();
      expect(accepted.map((alarm) => alarm.state)).toEqual(["outage", "recovered"]);
      expect(childProcess.execFile).toHaveBeenCalledTimes(2);
      callbacks.at(-1)!(null, "", "");
      expect(journal.meshJournalMirrorFailures()).toBe(1);
    });

  it("bounds a timed-out child without delaying timers, recovery or mesh alarm delivery", async () => {
    fault = "timeout";
    const accepted: MeshOutageAlarm[] = [];
    const retry = new journal.MeshBackgroundRetry("pane").alarmsTo((alarm) => { accepted.push(alarm); }, 1);
    await raiseOutage(retry);
    const tick = vi.fn(); setTimeout(tick, 1);
    fault = ""; retry.success(true); await retry.flushAlarms();
    expect(accepted.map((alarm) => alarm.state)).toEqual(["outage", "recovered"]);
    expect(journal.meshJournalMirrorFailures()).toBe(0);
    await vi.advanceTimersByTimeAsync(1); expect(tick).toHaveBeenCalledOnce();
    await vi.advanceTimersByTimeAsync(1_999);
    expect(journal.meshJournalMirrorFailures()).toBe(1);
  });

  it.each(["attempts", "overflow"])("mirrors every structured emitter path including undeliverable by %s", async (limit) => {
    const retry = new journal.MeshBackgroundRetry("pane").alarmsTo(
      limit === "attempts" ? () => { throw busy(); } : () => new Promise(() => {}), 1);
    await raiseOutage(retry);
    if (limit === "attempts") {
      for (let i = 1; i < MESH_ALARM_MAX_ATTEMPTS; i++) await retry.flushAlarms();
      retry.success(true); await retry.flushAlarms();
    } else {
      retry.success(true);
      for (let i = 0; i < 16; i++) { await raiseOutage(retry); retry.success(true); }
    }
    const lines = stderrLines();
    expect(lines.map(parse9729).filter((alarm) => alarm?.reason === "mesh-alarm-undeliverable")).toHaveLength(1);
    expect(lines.some((line) => parse9729(line)?.state === "recovered")).toBe(true);
    expect(writes).toEqual(lines.map((line) => `${line}\n`));
    expect(childProcess.execFile).toHaveBeenCalledTimes(lines.length);
    retry.close();
  });
});
