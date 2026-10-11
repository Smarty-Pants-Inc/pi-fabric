import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { build } from "esbuild";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { MeshBackgroundRetry, MeshLockTimeoutError, meshRetryWarningWindowMs } from "../src/core/atomic-write.js";

const busy = () => new MeshLockTimeoutError(" held by pid 123 (alive, running)", 6, 8020);
afterEach(() => { vi.restoreAllMocks(); vi.useRealTimers(); vi.unstubAllEnvs(); });

// smarty-dev#8305 end to end: a real ParticipantDirectory heartbeat loop in its own Node
// process, behind real mesh lock files that process holds, on real timers. Only the window is
// shortened (PI_FABRIC_MESH_RETRY_WARNING_WINDOW_MS) so the run takes seconds, not minutes.
describe("real participant directory under a held mesh lock", () => {
  const label = "participant heartbeat/change refresh";
  const windowMs = 2_000;
  let root: string;
  let bundle: string;
  beforeAll(async () => {
    root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-retry-warning-rate-"));
    bundle = path.join(root, "probe.mjs");
    await build({ entryPoints: ["tests/fixtures/mesh-retry-warning-rate.ts"], outfile: bundle, bundle: true,
      platform: "node", format: "esm", packages: "external" });
    fs.symlinkSync(path.resolve("node_modules"), path.join(root, "node_modules"), process.platform === "win32" ? "junction" : "dir");
  });
  afterAll(() => fs.rmSync(root, { recursive: true, force: true }));

  it("prints at most one warning per window across a held outage and a flapping lock, then a final summary", async () => {
    const cwd = path.join(root, "probe");
    fs.mkdirSync(cwd);
    const result = await new Promise<{ code: number | null; output: string }>((resolve, reject) => {
      const child = spawn(process.execPath, ["--unhandled-rejections=strict", bundle, cwd],
        { env: { ...process.env, PI_FABRIC_MESH_RETRY_WARNING_WINDOW_MS: String(windowMs) } });
      let output = "";
      child.stdout.on("data", chunk => { output += chunk; });
      child.stderr.on("data", chunk => { output += chunk; });
      const timeout = setTimeout(() => child.kill("SIGKILL"), 75_000);
      child.on("error", reject);
      child.on("close", code => { clearTimeout(timeout); resolve({ code, output }); });
    });
    expect(result.code, result.output).toBe(0);
    const report = result.output.split("\n").find(line => line.startsWith("RATE "));
    expect(report, result.output).toBeDefined();
    const rate = JSON.parse(report!.slice(5)) as {
      windowMs: number; pid: number; outages: number; timeouts: number; lines: Array<{ at: number; text: string }> };
    const evidence = JSON.stringify(rate, null, 1);
    expect(rate.windowMs).toBe(windowMs);
    // Every warning is this path's: nothing else, and no unrelated-error lines, reached stderr.
    expect(rate.lines.every(line => line.text.startsWith(`[pi-fabric] ${label}: mesh lock `)), evidence).toBe(true);
    const retrying = rate.lines.filter(line => line.text.includes(": mesh lock timeout; retrying in "));
    const summary = rate.lines.at(-1)!;

    // At most one line per window (a few ms of slack for the probe's own clock read).
    for (let index = 1; index < rate.lines.length; index++) {
      expect(rate.lines[index]!.at - rate.lines[index - 1]!.at, evidence).toBeGreaterThanOrEqual(windowMs - 20);
    }
    // The first warning is immediate, names the live holder and has nothing to summarize.
    expect(rate.lines[0]!.text, evidence).toMatch(/mesh lock timeout; retrying in \d+ ms: FABRIC_MESH_LOCK_TIMEOUT/);
    expect(rate.lines[0]!.text, evidence).toContain(`held by pid ${rate.pid}`);
    expect(rate.lines[0]!.text, evidence).not.toContain("suppressed");
    // The held outage outlived a window: its later warning carries that window's count.
    expect(retrying[1]?.text, evidence).toMatch(/\((\d+) similar retries suppressed in the last \d+s\)$/);
    // A flapping lock no longer prints one warning per outage.
    expect(retrying.length, evidence).toBeLessThan(rate.outages);
    // The final summary follows the last real acquisition after the window.
    expect(summary.text, evidence).toMatch(
      new RegExp(`^\\[pi-fabric\\] ${label.replace("/", "\\/")}: mesh lock recovered \\(([1-9]\\d*) similar retries suppressed in the last \\d+s\\)$`));
    // Every typed lock timeout was either printed or counted in exactly one summary.
    const counted = rate.lines.reduce((sum, line) => sum + Number(/\((\d+) similar retries suppressed/.exec(line.text)?.[1] ?? 0), 0);
    expect(retrying.length + counted, evidence).toBe(rate.timeouts);
  }, 90_000);
});

describe("background mesh retry warning rate limit", () => {
  it("logs 100 short outages within 60 seconds once, then reports 99 suppressed retries at 60 seconds", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    vi.spyOn(Math, "random").mockReturnValue(0.5);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const retry = new MeshBackgroundRetry("participant heartbeat/change refresh");
    const operation = () => { throw busy(); };
    for (let outage = 0; outage < 100; outage++) {
      expect(await retry.run(operation)).toBe("retry");
      expect(retry.waitMs).toBe(50);
      if (outage === 0) {
        expect(warn).toHaveBeenCalledOnce();
        expect(warn.mock.calls[0]![0]).toContain("mesh lock timeout; retrying in 50 ms");
        expect(warn.mock.calls[0]![0]).toContain("held by pid 123");
        expect(warn.mock.calls[0]![0]).not.toContain("suppressed");
      }
      await vi.advanceTimersByTimeAsync(50);
      expect(await retry.run(() => undefined)).toBe("done");
      expect(retry.waitMs).toBe(0);
      await vi.advanceTimersByTimeAsync(450);
    }
    expect(warn).toHaveBeenCalledOnce();
    await vi.advanceTimersByTimeAsync(10_000);
    expect(await retry.run(operation)).toBe("retry");
    expect(warn).toHaveBeenCalledTimes(2);
    expect(warn.mock.calls[1]![0]).toMatch(/\(99 similar retries suppressed in the last 60s\)$/);

    await vi.advanceTimersByTimeAsync(50);
    expect(await retry.run(() => undefined)).toBe("done");
    await vi.advanceTimersByTimeAsync(59_950);
    expect(await retry.run(operation)).toBe("retry");
    expect(warn).toHaveBeenCalledTimes(3);
    expect(warn.mock.calls[2]![0]).not.toContain("suppressed");
  });

  it("suppresses through 59999 ms without recovery resetting the warning clock", () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const retry = new MeshBackgroundRetry("poll");
    retry.failure(busy());
    retry.success();
    vi.setSystemTime(59_999);
    retry.failure(busy());
    retry.success();
    expect(warn).toHaveBeenCalledOnce();
    vi.setSystemTime(60_000);
    retry.failure(busy());
    expect(warn).toHaveBeenCalledTimes(2);
    expect(warn.mock.calls[1]![0]).toMatch(/\(1 similar retries suppressed in the last 60s\)$/);
  });

  it("summarizes continuous retries using elapsed seconds and resets the count after reporting", () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const retry = new MeshBackgroundRetry("poll");
    retry.failure(busy());
    retry.failure(busy());
    retry.failure(busy());
    expect(warn).toHaveBeenCalledOnce();
    vi.setSystemTime(61_000);
    retry.failure(busy());
    expect(warn).toHaveBeenCalledTimes(2);
    expect(warn.mock.calls[1]![0]).toMatch(/\(2 similar retries suppressed in the last 61s\)$/);
    retry.failure(busy());
    vi.setSystemTime(121_000);
    retry.failure(busy());
    expect(warn).toHaveBeenCalledTimes(3);
    expect(warn.mock.calls[2]![0]).toMatch(/\(1 similar retries suppressed in the last 60s\)$/);
  });

  it("logs every non-transient error without clearing transient suppression", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const retry = new MeshBackgroundRetry("handler");
    retry.failure(busy());
    retry.success();
    for (const error of [new Error("bug"), "plain failure", new Error("Timed out waiting for the Fabric mesh lock")]) {
      expect(await retry.run(() => { throw error; })).toBe("failed");
      expect(retry.waitMs).toBe(0);
      retry.failure(busy());
      retry.success();
    }
    expect(warn).toHaveBeenCalledTimes(4);
    expect(warn.mock.calls.slice(1).map(call => call[0])).toEqual([
      "[pi-fabric] handler: background operation failed: bug",
      "[pi-fabric] handler: background operation failed: plain failure",
      "[pi-fabric] handler: background operation failed: Timed out waiting for the Fabric mesh lock",
    ]);
    vi.setSystemTime(60_000);
    retry.failure(busy());
    expect(warn).toHaveBeenCalledTimes(5);
    expect(warn.mock.calls[4]![0]).toMatch(/\(3 similar retries suppressed in the last 60s\)$/);
  });

  it("keeps warning budgets independent for separate background paths", () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const first = new MeshBackgroundRetry("first");
    const second = new MeshBackgroundRetry("second");
    first.failure(busy());
    second.failure(busy());
    expect(warn).toHaveBeenCalledTimes(2);
    expect(warn.mock.calls[0]![0]).toContain("first:");
    expect(warn.mock.calls[1]![0]).toContain("second:");
    first.success();
    second.success();
    first.failure(busy());
    second.failure(busy());
    expect(warn).toHaveBeenCalledTimes(2);
  });

  it("prints a final summary on the first acquisition after the window, and counts it as that window's line", () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const retry = new MeshBackgroundRetry("poll");
    retry.success();
    expect(warn).not.toHaveBeenCalled(); // nothing suppressed, nothing to say
    retry.failure(busy());
    retry.success();
    retry.failure(busy());
    retry.failure(busy());
    vi.setSystemTime(59_999);
    retry.success();
    expect(warn).toHaveBeenCalledOnce(); // inside the window
    vi.setSystemTime(65_000);
    retry.success();
    expect(warn).toHaveBeenCalledTimes(2);
    expect(warn.mock.calls[1]![0]).toBe("[pi-fabric] poll: mesh lock recovered (2 similar retries suppressed in the last 65s)");
    retry.success();
    expect(warn).toHaveBeenCalledTimes(2); // reported once
    vi.setSystemTime(70_000);
    retry.failure(busy()); // a new outage inside the summary's window is counted
    expect(warn).toHaveBeenCalledTimes(2);
    vi.setSystemTime(125_000);
    retry.failure(busy());
    expect(warn).toHaveBeenCalledTimes(3);
    expect(warn.mock.calls[2]![0]).toMatch(/retrying in \d+ ms: .*held by pid 123.*\(1 similar retries suppressed in the last 60s\)$/);
  });

  it("does not treat an unrelated failure as lock recovery", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const retry = new MeshBackgroundRetry("handler");
    retry.failure(busy());
    retry.failure(busy());
    vi.setSystemTime(90_000);
    expect(await retry.run(() => { throw new Error("bug"); })).toBe("failed");
    expect(retry.waitMs).toBe(0); // backoff still reset, as before
    expect(warn.mock.calls.map(call => call[0])).toEqual([
      expect.stringContaining("mesh lock timeout; retrying"),
      "[pi-fabric] handler: background operation failed: bug",
    ]);
  });

  // #792 hang: a read-only actor root made every participant refresh fail with EACCES at once,
  // and that failed attempt's own host-lease renewal re-woke the event-driven lifecycle broker,
  // whose canConsumeMesh gate re-ran the refresh: 84k-131k warnings at 100% CPU instead of 497.
  it("backs off a permission/read-only refusal instead of re-arming it at once", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const retry = new MeshBackgroundRetry("refresh");
    const refusal = (code: string) => Object.assign(new Error(`${code}: permission denied, mkdir 'actors.json.lock'`), { code });
    let attempts = 0;
    const refused = () => { attempts++; throw refusal("EACCES"); };
    expect(await retry.run(refused)).toBe("failed"); // still a failure, never lock-retry admission
    expect(retry.waitMs).toBeGreaterThan(0);
    // A re-wake caused by the failed attempt itself is skipped, not re-run.
    for (let i = 0; i < 1_000; i++) expect(await retry.run(refused)).toBe("skipped");
    expect(attempts).toBe(1);
    // Capped at 1 s, so a restored directory (or a transient Windows EPERM) recovers promptly.
    for (const error of [refusal("EROFS"), refusal("EPERM"), new Error("wrapped", { cause: refusal("EACCES") }),
      refusal("EACCES"), refusal("EACCES")]) {
      vi.advanceTimersByTime(1_000);
      expect(retry.waitMs).toBe(0);
      expect(await retry.run(() => { throw error; })).toBe("failed");
      expect(retry.waitMs).toBeGreaterThan(0);
      expect(retry.waitMs).toBeLessThanOrEqual(1_000);
    }
    // One line per #809 window, carrying the cause; recovery is summarized once the window passes.
    expect(warn).toHaveBeenCalledOnce();
    expect(warn.mock.calls[0]![0]).toMatch(/^\[pi-fabric\] refresh: filesystem access refused; retrying in \d+ ms: EACCES: permission denied/);
    vi.setSystemTime(61_000);
    expect(await retry.run(() => undefined)).toBe("done");
    expect(retry.waitMs).toBe(0);
    expect(warn.mock.calls[1]![0]).toBe("[pi-fabric] refresh: filesystem access recovered (5 similar retries suppressed in the last 61s)");
  });

  it("reads the window from PI_FABRIC_MESH_RETRY_WARNING_WINDOW_MS, defaulting to 60 s", () => {
    for (const [value, expected] of [[undefined, 60_000], ["", 60_000], [" ", 60_000], ["abc", 60_000], ["0", 60_000],
      ["-5", 60_000], ["Infinity", 60_000], ["1500", 1_500], ["2.9", 2]] as const) {
      if (value === undefined) vi.stubEnv("PI_FABRIC_MESH_RETRY_WARNING_WINDOW_MS", undefined as unknown as string);
      else vi.stubEnv("PI_FABRIC_MESH_RETRY_WARNING_WINDOW_MS", value);
      expect(meshRetryWarningWindowMs(), String(value)).toBe(expected);
    }
    vi.stubEnv("PI_FABRIC_MESH_RETRY_WARNING_WINDOW_MS", "250");
    expect(new MeshBackgroundRetry("poll").warningWindowMs).toBe(250);
    expect(new MeshBackgroundRetry("poll", 100, 5_000, 10).warningWindowMs).toBe(10);
  });
});
