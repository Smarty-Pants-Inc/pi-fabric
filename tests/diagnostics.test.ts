import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { configureFabricDiagnostics, fabricWarn } from "../src/core/diagnostics.js";

let root: string;
let logPath: string;
const notify = vi.fn();
beforeEach(() => {
  root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-diagnostics-"));
  logPath = path.join(root, "logs", "diagnostics.log");
  vi.stubEnv("PI_FABRIC_LOG", logPath);
  notify.mockReset();
  configureFabricDiagnostics({ hasUI: true, mode: "tui", ui: { notify } });
});
afterEach(() => {
  configureFabricDiagnostics();
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  fs.rmSync(root, { recursive: true, force: true });
});

describe("terminal-safe Fabric diagnostics", () => {
  it("does not paint real terminal streams during repeated mesh lock outages", () => {
    const atomic = path.resolve("src/core/atomic-write.ts");
    const diagnostics = path.resolve("src/core/diagnostics.ts");
    const child = spawnSync("bun", ["--eval", `
      import { MeshBackgroundRetry, MeshLockTimeoutError } from ${JSON.stringify(atomic)};
      import { configureFabricDiagnostics } from ${JSON.stringify(diagnostics)};
      let notices = 0;
      configureFabricDiagnostics({ hasUI: true, mode: "tui", ui: { notify() { notices++; } } });
      const retry = new MeshBackgroundRetry("interactive mesh");
      const error = new MeshLockTimeoutError("", 2, 10);
      retry.failure(error);
      if (notices !== 1) throw new Error("Missing UI notice");
      for (let i = 0; i < 30; i++) { retry.success(); retry.failure(error); }
      if (notices !== 1) throw new Error("Notice storm");
    `], { encoding: "utf8", env: { ...process.env, PI_FABRIC_LOG: logPath } });
    expect(child.status, child.stderr).toBe(0);
    expect(child.stderr).toBe("");
    expect(child.stdout).toBe("");
    const lines = fs.readFileSync(logPath, "utf8").trimEnd().split("\n");
    expect(lines).toHaveLength(31);
    expect(lines[0]).toMatch(/^\d{4}-\d\d-\d\dT.*Z \[pi-fabric\] interactive mesh: mesh lock timeout; retrying/);
    if (process.platform !== "win32") expect(fs.statSync(logPath).mode & 0o777).toBe(0o600);
  });

  it("logs every identical warning but notifies only once per ten-minute window", () => {
    let now = 1_900_000_000_000;
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const stderr = vi.spyOn(process.stderr, "write");
    const stdout = vi.spyOn(process.stdout, "write");
    fabricWarn("[pi-fabric] dedup regression");
    expect(fs.readFileSync(logPath, "utf8").trimEnd().split("\n")).toHaveLength(1);
    fabricWarn("[pi-fabric] dedup regression");
    expect(notify).toHaveBeenCalledTimes(1);
    expect(notify).toHaveBeenCalledWith(expect.stringContaining("dedup regression"), "warning");
    now += 10 * 60 * 1000;
    fabricWarn("[pi-fabric] dedup regression");
    expect(notify).toHaveBeenCalledTimes(2);
    expect(fs.readFileSync(logPath, "utf8").trimEnd().split("\n")).toHaveLength(3);
    expect(stderr).not.toHaveBeenCalled();
    expect(stdout).not.toHaveBeenCalled();
  });

  it.each([undefined, "rpc", "print", "json", "worker"])("preserves console.warn arguments without a terminal UI (%s)", mode => {
    configureFabricDiagnostics(mode ? { hasUI: true, mode, ui: { notify } } : undefined);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const error = new Error("headless stack");
    const object = { value: 42 };
    fabricWarn("headless %s", object, error);
    expect(warn).toHaveBeenCalledExactlyOnceWith("headless %s", object, error);
    expect(notify).not.toHaveBeenCalled();
    expect(fs.existsSync(logPath)).toBe(false);
  });

  it("formats objects and error stacks into one physical log line", () => {
    const error = new Error("stack regression");
    fabricWarn("diagnostic %s", "format", { value: 42 }, error);
    const log = fs.readFileSync(logPath, "utf8");
    expect(log.trimEnd().split("\n")).toHaveLength(1);
    expect(log).toContain("diagnostic format { value: 42 }");
    expect(log).toContain(error.stack!.replaceAll("\n", "\\n"));
  });

  it("rotates at five MiB, replacing just one previous rotation", () => {
    fs.mkdirSync(path.dirname(logPath), { recursive: true });
    const cap = 5 * 1024 * 1024;
    fs.writeFileSync(logPath, "x".repeat(cap), { mode: 0o600 });
    fs.writeFileSync(`${logPath}.1`, "old rotation");
    fabricWarn("rotation regression");
    expect(fs.statSync(`${logPath}.1`).size).toBe(cap);
    expect(fs.readFileSync(logPath, "utf8")).toContain("rotation regression");
    expect(fs.statSync(logPath).size).toBeLessThan(cap);
    expect(fs.readdirSync(path.dirname(logPath)).sort()).toEqual(["diagnostics.log", "diagnostics.log.1"]);
    if (process.platform !== "win32") expect(fs.statSync(logPath).mode & 0o777).toBe(0o600);
  });

  it("uses the existing agent directory when no log override is supplied", () => {
    vi.stubEnv("PI_FABRIC_LOG", "");
    vi.stubEnv("PI_CODING_AGENT_DIR", root);
    fabricWarn("default-path regression");
    expect(fs.readFileSync(path.join(root, "fabric", "logs", "diagnostics.log"), "utf8")).toContain("default-path regression");
  });

  it("never falls back to stderr if logging or the UI fails", () => {
    fs.writeFileSync(path.join(root, "not-a-directory"), "blocked");
    vi.stubEnv("PI_FABRIC_LOG", path.join(root, "not-a-directory", "log"));
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    notify.mockImplementationOnce(() => { throw new Error("stale UI"); });
    expect(() => fabricWarn("disk-failure regression")).not.toThrow();
    expect(warn).not.toHaveBeenCalled();
    expect(notify).toHaveBeenCalledWith(expect.stringContaining("Diagnostic log unavailable"), "warning");
  });

  it("binds diagnostics before discovery and session bootstrap and keeps raw warns centralized", () => {
    const index = fs.readFileSync("src/index.ts", "utf8");
    for (const event of ["resources_discover", "session_start"]) {
      expect(index).toContain(`pi.on("${event}", async (${event === "session_start" ? "event" : "_event"}, context) => {\n    configureFabricDiagnostics(context);`);
    }
    const files = fs.readdirSync("src", { recursive: true }).filter(file => String(file).endsWith(".ts"));
    const rawWarnings = files.filter(file => fs.readFileSync(path.join("src", String(file)), "utf8").includes("console.warn("));
    expect(rawWarnings.map(String)).toEqual([path.join("core", "agent-dir.ts")]);
  });
});
