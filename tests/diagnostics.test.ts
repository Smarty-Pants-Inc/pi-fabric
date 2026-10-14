import fs from "node:fs";
import childProcess from "node:child_process";
import { Console } from "node:console";
import { PassThrough } from "node:stream";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { configureFabricDiagnostics, fabricDiagnosticsHaveTerminalUI, fabricWarn, fabricError, fabricInfo, fabricDebug, fabricStderr } from "../src/core/diagnostics.js";
import { MeshBackgroundRetry, MeshLockTimeoutError } from "../src/core/atomic-write.js";

const scratch: string[] = [];
afterEach(() => {
  configureFabricDiagnostics();
  vi.restoreAllMocks();
  vi.useRealTimers();
  vi.unstubAllEnvs();
  for (const root of scratch.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const profile = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-diagnostics-"));
  scratch.push(root);
  vi.stubEnv("PI_CODING_AGENT_DIR", root);
  return { root, log: path.join(root, "fabric", "logs", "diagnostics.log") };
};
const streams = () => {
  // Vitest replaces global console with a reporter, which bypasses process.stderr.
  // Use Node's real formatting/stream path so the no-UI case measures stderr, not a console mock.
  const terminalConsole = new Console({ stdout: process.stdout, stderr: process.stderr });
  for (const level of ["warn", "error", "info", "debug"] as const) {
    vi.spyOn(console, level).mockImplementation(terminalConsole[level]);
  }
  return {
    stderr: vi.spyOn(process.stderr, "write").mockImplementation(() => true),
    stdout: vi.spyOn(process.stdout, "write").mockImplementation(() => true),
  };
};
const timeout = () => new MeshLockTimeoutError(" held by pid 123 (alive, running)", 6, 8020);

// Decision C: the real offline Pi TUI/held-lock proof is scripts/probe-main-diagnostics.py.
// These small tests additionally prove the no-UI counterexample, failure and rotation branches.
describe("Fabric diagnostics protect Pi's editor", () => {
  it("a UI-active mesh lock timeout writes nothing to either terminal stream and logs the unchanged warning", () => {
    const { log } = profile();
    const notify = vi.fn();
    configureFabricDiagnostics({ hasUI: true, mode: "tui", ui: { notify } });
    const { stderr, stdout } = streams();
    const retry = new MeshBackgroundRetry("participant heartbeat/change refresh");
    expect(retry.failure(timeout())).toBe(true);
    expect(stderr).not.toHaveBeenCalled();
    expect(stdout).not.toHaveBeenCalled();
    const line = fs.readFileSync(log, "utf8");
    expect(line).toMatch(/\[pi-fabric\] participant heartbeat\/change refresh: mesh lock timeout; retrying in \d+ ms: FABRIC_MESH_LOCK_TIMEOUT/);
    expect(line).toContain("held by pid 123 (alive, running)");
    expect(notify).toHaveBeenCalledOnce();
    expect(notify.mock.calls[0]![0]).toContain("Mesh lock contention");
    expect(notify.mock.calls[0]![0]).not.toContain("FABRIC_MESH_LOCK_TIMEOUT");
    if (process.platform !== "win32") {
      expect(fs.statSync(log).mode & 0o777).toBe(0o600);
      expect(fs.statSync(path.dirname(log)).mode & 0o777).toBe(0o700);
    }
  });

  it.each([undefined, { hasUI: false }, { hasUI: true, mode: "rpc" }])("without a terminal UI (%j) mesh lock warnings still reach stderr, never a log", (context) => {
    const { log } = profile();
    const notify = vi.fn();
    configureFabricDiagnostics(context && { ...context, ui: { notify } });
    expect(fabricDiagnosticsHaveTerminalUI()).toBe(false);
    const { stderr, stdout } = streams();
    new MeshBackgroundRetry("headless heartbeat").failure(timeout());
    expect(stderr).toHaveBeenCalledOnce();
    expect(String(stderr.mock.calls[0]![0])).toContain("[pi-fabric] headless heartbeat: mesh lock timeout; retrying");
    expect(stdout).not.toHaveBeenCalled();
    expect(fs.existsSync(log)).toBe(false);
    expect(notify).not.toHaveBeenCalled();
  });

  it.each([undefined, "bound"])("keeps outage/recovery off the TUI and mirrors both alarms even with %s journal stderr", async (binding) => {
    const { log } = profile();
    const notify = vi.fn();
    configureFabricDiagnostics({ hasUI: true, mode: "tui", ui: { notify } });
    expect(fabricDiagnosticsHaveTerminalUI()).toBe(true);
    // The real emitter runs; only its external journal child is inert. Never send test alarms to the host.
    const writes: string[] = [];
    const exec = vi.spyOn(childProcess, "execFile").mockImplementation(() => {
      const child = new childProcess.ChildProcess();
      child.stdin = new PassThrough();
      child.stdin.on("data", (chunk) => writes.push(String(chunk)));
      return child;
    });
    const stat = fs.fstatSync(2, { bigint: true });
    vi.stubEnv("JOURNAL_STREAM", binding ? `${stat.dev}:${stat.ino}` : undefined);
    const { stderr, stdout } = streams();
    vi.useFakeTimers();
    const accepted: string[] = [];
    const retry = new MeshBackgroundRetry("TUI outage").alarmsTo((alarm) => { accepted.push(alarm.state); }, 1);
    try {
      retry.failure(timeout());
      await vi.advanceTimersByTimeAsync(1);
      retry.failure(timeout());
      retry.success(true);
      await retry.flushAlarms();
      expect(accepted).toEqual(["outage", "recovered"]);
      expect(stderr).not.toHaveBeenCalled();
      expect(stdout).not.toHaveBeenCalled();
      const lines = fs.readFileSync(log, "utf8").trim().split("\n");
      const alarms = lines.filter((line) => line.includes("fabric.alarm.mesh-outage "));
      expect(alarms).toHaveLength(2);
      expect(alarms[0]).toContain('"state":"outage"');
      expect(alarms[1]).toContain('"state":"recovered"');
      if (process.platform === "linux") {
        expect(exec).toHaveBeenCalledTimes(2);
        expect(writes).toEqual(alarms.map((line) => `${line.slice(line.indexOf("fabric.alarm.mesh-outage "))}\n`));
      } else {
        expect(exec).not.toHaveBeenCalled();
      }
    } finally { retry.close(); }
  });

  it("preserves each console method and raw stderr bytes without a UI; raw stderr stays off an active TUI", () => {
    const { log } = profile();
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const error = vi.spyOn(console, "error").mockImplementation(() => {});
    const info = vi.spyOn(console, "info").mockImplementation(() => {});
    const debug = vi.spyOn(console, "debug").mockImplementation(() => {});
    const stderr = vi.spyOn(process.stderr, "write").mockImplementation(() => true);
    const stdout = vi.spyOn(process.stdout, "write").mockImplementation(() => true);
    const failure = new Error("failure");
    fabricWarn("warn %s", "exact");
    fabricError("error", failure);
    fabricInfo("info %d", 3);
    fabricDebug("debug", { value: true });
    fabricStderr("raw diagnostic\n");
    expect(warn.mock.calls).toEqual([["warn %s", "exact"]]);
    expect(error.mock.calls).toEqual([["error", failure]]);
    expect(info.mock.calls).toEqual([["info %d", 3]]);
    expect(debug.mock.calls).toEqual([["debug", { value: true }]]);
    expect(stderr.mock.calls).toEqual([["raw diagnostic\n"]]);
    expect(stdout).not.toHaveBeenCalled();
    expect(fs.existsSync(log)).toBe(false);
    configureFabricDiagnostics({ hasUI: true, mode: "tui", ui: { notify: vi.fn() } });
    fabricStderr("UI diagnostic\n");
    expect(stderr).toHaveBeenCalledOnce();
    expect(warn).toHaveBeenCalledOnce();
    expect(stdout).not.toHaveBeenCalled();
    expect(fs.readFileSync(log, "utf8")).toContain("UI diagnostic\\n");
  });

  it("logs every diagnostic, deduplicates varied mesh retries, and rebinds after session replacement", () => {
    const { log } = profile();
    vi.useFakeTimers();
    vi.setSystemTime(0);
    const notify = vi.fn();
    configureFabricDiagnostics({ hasUI: true, ui: { notify } });
    const { stderr, stdout } = streams();
    fabricWarn("[pi-fabric] one: mesh lock timeout; retrying in 50 ms");
    fabricWarn("[pi-fabric] two: mesh lock timeout; retrying in 100 ms");
    expect(notify).toHaveBeenCalledOnce();
    vi.setSystemTime(10 * 60 * 1000);
    fabricWarn("[pi-fabric] three: mesh lock timeout; retrying in 200 ms");
    expect(notify).toHaveBeenCalledTimes(2);
    const next = vi.fn();
    configureFabricDiagnostics({ hasUI: true, mode: "tui", ui: { notify: next } });
    fabricWarn("[pi-fabric] four: mesh lock timeout; retrying in 400 ms");
    expect(next).toHaveBeenCalledOnce();
    const error = new Error("failure\nsecond line");
    fabricError("error %s", error);
    fabricInfo("info %d", 3);
    fabricDebug("debug %j", { value: true });
    expect(stderr).not.toHaveBeenCalled();
    expect(stdout).not.toHaveBeenCalled();
    const lines = fs.readFileSync(log, "utf8").trim().split("\n");
    expect(lines).toHaveLength(7);
    expect(lines[4]).toContain("failure\\nsecond line");
    expect(lines[5]).toContain("info 3");
    expect(lines[6]).toContain('debug {"value":true}');
  });

  it.skipIf(process.platform === "win32")("refuses a symlink log without writing its target or either terminal stream", () => {
    const { root, log } = profile();
    fs.mkdirSync(path.dirname(log), { recursive: true, mode: 0o700 });
    const target = path.join(root, "unrelated.txt");
    fs.writeFileSync(target, "unchanged");
    fs.symlinkSync(target, log);
    const notify = vi.fn();
    configureFabricDiagnostics({ hasUI: true, mode: "tui", ui: { notify } });
    const { stderr, stdout } = streams();
    fabricWarn("symlink refused");
    expect(fs.readFileSync(target, "utf8")).toBe("unchanged");
    expect(notify.mock.calls[0]![0]).toContain("Diagnostic log unavailable:");
    expect(stderr).not.toHaveBeenCalled();
    expect(stdout).not.toHaveBeenCalled();
  });

  it("rotates the private log and does not fall back to terminal output when the log or host UI fails", () => {
    const { root, log } = profile();
    const notify = vi.fn((_message: string, _level: "warning" | "error" | "info") => { throw new Error("stale UI"); });
    configureFabricDiagnostics({ hasUI: true, mode: "tui", ui: { notify } });
    const { stderr, stdout } = streams();
    fabricWarn("first");
    fs.writeFileSync(log, "x".repeat(5 * 1024 * 1024), { mode: 0o600 });
    if (process.platform !== "win32") fs.chmodSync(log, 0o644);
    fabricError("after rotation");
    expect(fs.statSync(`${log}.1`).size).toBe(5 * 1024 * 1024);
    if (process.platform !== "win32") expect(fs.statSync(`${log}.1`).mode & 0o777).toBe(0o600);
    expect(fs.readFileSync(log, "utf8")).toContain("after rotation");
    fs.writeFileSync(log, "y".repeat(5 * 1024 * 1024));
    fabricWarn("second rotation");
    expect(fs.readFileSync(`${log}.1`, "utf8").startsWith("y")).toBe(true);
    const blocked = path.join(root, "not-a-directory");
    fs.writeFileSync(blocked, "blocking file");
    vi.stubEnv("PI_CODING_AGENT_DIR", blocked);
    expect(() => fabricWarn("disk failed")).not.toThrow();
    expect(notify.mock.calls.at(-1)![0]).toContain("Diagnostic log unavailable:");
    expect(stderr).not.toHaveBeenCalled();
    expect(stdout).not.toHaveBeenCalled();
  });
});
