import fs from "node:fs";
import { execFileSync, spawn } from "node:child_process";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { AgentManager } from "../src/agents/manager.js";
import { reportResidentExit, runResidentHostFromConfigPath } from "../src/residency/host.js";
import { residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";

const waitFor = async (predicate: () => boolean, boundMs: number): Promise<void> => {
  const until = Date.now() + boundMs;
  while (!predicate()) {
    if (Date.now() >= until) throw new Error("Timed out");
    await new Promise(resolve => setTimeout(resolve, 20));
  }
};
const snapshot = (directory: string, files = new Map<string, string>()): Map<string, string> => {
  for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
    const file = path.join(directory, entry.name);
    if (entry.isDirectory()) snapshot(file, files);
    else try { const stat = fs.statSync(file); files.set(file, `${stat.size}:${stat.mtimeMs}`); } catch { /* removed meanwhile */ }
  }
  return files;
};
const exitLines = (residencyRoot: string): Array<Record<string, unknown>> => {
  try {
    return fs.readFileSync(path.join(residencyRoot, "launcher.log"), "utf8").trim().split("\n").filter(Boolean)
      .map(line => JSON.parse(line) as Record<string, unknown>).filter(row => row.event === "resident-exit");
  } catch { return []; }
};
afterEach(() => { vi.restoreAllMocks(); });

const harness = (name: string) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), `resident-exit-${name}-`));
  const sessionId = `exit-${name}`;
  const meshRoot = path.join(root, "mesh");
  const residencyRoot = residentRoot(meshRoot, `session:${sessionId}`);
  const config: ResidentHostConfig = {
    format: 1, rootId: `session:${sessionId}`, sessionId, cwd: root, projectRoot: root, meshRoot,
    actorRoot: path.join(root, "actors"), sessionActorRoot: path.join(root, "session-actors"), residencyRoot,
    fullCodeMode: true, agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 },
    mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, retention: DEFAULT_FABRIC_CONFIG.retention,
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fabricExtensionPath: path.resolve("dist/index.js"),
    piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
  };
  fs.mkdirSync(residencyRoot, { recursive: true, mode: 0o700 });
  const configPath = path.join(residencyRoot, "config.json");
  fs.writeFileSync(configPath, JSON.stringify(config));
  const ownerPath = path.join(residencyRoot, "owner.json");
  // The hook: at owner.json removal (the release), what launcher.log and mesh/ hold.
  const atRelease: { lines?: Array<Record<string, unknown>>; files?: Map<string, string> } = {};
  const rmSync = fs.rmSync;
  vi.spyOn(fs, "rmSync").mockImplementation(((file: fs.PathLike, options?: fs.RmOptions) => {
    rmSync(file, options);
    if (String(file) === ownerPath) { atRelease.lines = exitLines(residencyRoot); atRelease.files = snapshot(meshRoot); }
  }) as typeof fs.rmSync);
  const late = (): string[] => [...snapshot(meshRoot)].filter(([file, stamp]) => atRelease.files!.get(file) !== stamp)
    .map(([file]) => path.relative(root, file));
  return { root, residencyRoot, configPath, ownerPath, atRelease, late };
};

// smarty-dev#7770: a clean idle exit must not look like an outside kill. The
// host names it in launcher.log itself, synchronously, while it owns the root.
// ponytail: Windows has no O_NOFOLLOW/O_NONBLOCK, so the host skips the line there
// by design (fail closed); a Windows-safe open is cut to smarty-dev#7950.
const posix = process.platform !== "win32";
it.skipIf(!posix)("an idle exit leaves one resident-exit idle-exit line, written before owner.json is removed", { timeout: 60_000 }, async () => {
  const h = harness("idle");
  try {
    const run = runResidentHostFromConfigPath(h.configPath);
    await waitFor(() => fs.existsSync(h.ownerPath), 30_000);
    // Skip the 30 s idle window: the host's idle check reads Date.now().
    const now = Date.now.bind(Date);
    vi.spyOn(Date, "now").mockImplementation(() => now() + 60_000);
    await run;
    expect(h.atRelease.lines).toEqual([expect.objectContaining({ event: "resident-exit", reason: "idle-exit", pid: process.pid })]);
    expect(exitLines(h.residencyRoot)).toHaveLength(1);
    expect(h.late()).toEqual([]);
  } finally {
    vi.restoreAllMocks();
    fs.rmSync(h.root, { recursive: true, force: true });
  }
});

it.skipIf(posix)("win32: an idle exit writes no exit line (no O_NOFOLLOW) and the release proceeds", { timeout: 60_000 }, async () => {
  const h = harness("idle-win32");
  try {
    const run = runResidentHostFromConfigPath(h.configPath);
    await waitFor(() => fs.existsSync(h.ownerPath), 30_000);
    const now = Date.now.bind(Date);
    vi.spyOn(Date, "now").mockImplementation(() => now() + 60_000);
    await run;
    expect(h.atRelease.lines).toEqual([]);
    expect(exitLines(h.residencyRoot)).toEqual([]);
    expect(fs.existsSync(h.ownerPath)).toBe(false);
  } finally {
    vi.restoreAllMocks();
    fs.rmSync(h.root, { recursive: true, force: true });
  }
});

// #1882: after the host releases owner.json the root may belong to the next
// generation: no exit line after it. (error.json handling is main's, unchanged here.)
it("a failure from close() after the release writes no exit line after the release", { timeout: 60_000 }, async () => {
  const h = harness("release");
  // The release is the last step of close(); this failure surfaces after it.
  vi.spyOn(AgentManager.prototype, "close").mockRejectedValue(new Error("injected teardown failure"));
  const controller = new AbortController();
  try {
    const run = runResidentHostFromConfigPath(h.configPath, controller.signal);
    await waitFor(() => fs.existsSync(h.ownerPath), 30_000);
    controller.abort();
    await expect(run).rejects.toThrow("injected teardown failure");
    // The line is POSIX-only (see `posix`); on win32 nothing is written.
    const expected = posix ? [expect.objectContaining({ event: "resident-exit", reason: "stopped" })] : [];
    expect(h.atRelease.lines).toEqual(expected);
    expect(exitLines(h.residencyRoot)).toEqual(expected);
    // Only main's own error.json write may follow the release; never launcher.log.
    expect(h.late().filter(file => path.basename(file) !== "error.json")).toEqual([]);
  } finally {
    vi.restoreAllMocks();
    fs.rmSync(h.root, { recursive: true, force: true });
  }
});

// A host that never owned the root has no fence: it writes no exit line.
// Another generation owns this root; the failing non-owner must not touch it.
it.each(posix ? ["launch-context", "lock-busy"] as const : ["launch-context"] as const)("a pre-ownership failure (%s) leaves a live owner's files byte-identical", { timeout: 60_000 }, async failure => {
  const h = harness(`pre-owner-${failure}`);
  const previous = process.env.PI_FABRIC_RESIDENT_LAUNCHER;
  const files = {
    [h.ownerPath]: JSON.stringify({ format: 1, hostId: "resident:other", pid: process.pid, token: "other-generation", startedAt: Date.now() }),
    [path.join(h.residencyRoot, "launcher.log")]: `${JSON.stringify({ event: "launcher-started", pid: 4242 })}\n`,
  };
  for (const [file, bytes] of Object.entries(files)) fs.writeFileSync(file, bytes, { mode: 0o600 });
  let holder: ReturnType<typeof spawn> | undefined;
  if (failure === "launch-context") {
    process.env.PI_FABRIC_RESIDENT_LAUNCHER = JSON.stringify({ pid: 1, processStartTime: "1", token: "x", entry: "x", runtime: "x" });
  } else {
    // The other generation is mid-startup: it holds the establishment lock.
    const ready = path.join(h.root, "holder-ready");
    holder = spawn("flock", ["-x", path.join(h.residencyRoot, "host-fence-establish.lock"), "sh", "-c", `touch '${ready}'; sleep 20`], { stdio: "ignore" });
    await waitFor(() => fs.existsSync(ready), 10_000);
  }
  try {
    const run = runResidentHostFromConfigPath(h.configPath);
    if (failure === "launch-context") {
      await expect(run).rejects.toThrow("Resident launcher identity is uncertain");
    } else {
      await expect(run).resolves.toBeUndefined(); // Already running: a silent return, as on main.
    }
    for (const [file, bytes] of Object.entries(files)) expect(fs.readFileSync(file, "utf8")).toBe(bytes);
  } finally {
    if (previous === undefined) delete process.env.PI_FABRIC_RESIDENT_LAUNCHER; else process.env.PI_FABRIC_RESIDENT_LAUNCHER = previous;
    holder?.kill("SIGKILL");
    vi.restoreAllMocks();
    fs.rmSync(h.root, { recursive: true, force: true });
  }
});

// Platform-neutral: a missing flag fails closed (on win32 both are missing).
it.each(["O_NOFOLLOW", "O_NONBLOCK"] as const)("skips the write without %s (fail closed)", flag => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "resident-exit-file-"));
  try {
    const constants: Partial<typeof fs.constants> = { ...fs.constants };
    delete constants[flag];
    reportResidentExit(root, "idle-exit", undefined, constants);
    expect(fs.existsSync(path.join(root, "launcher.log"))).toBe(false);
  } finally { fs.rmSync(root, { recursive: true, force: true }); }
});

describe.skipIf(!posix)("resident-exit line file safety (POSIX: FIFO, symlink, mode)", () => {
  const dir = (): string => fs.mkdtempSync(path.join(os.tmpdir(), "resident-exit-file-"));

  it("returns at once and writes nothing when launcher.log is a FIFO with no reader", { timeout: 15_000 }, async () => {
    const root = dir();
    const log = path.join(root, "launcher.log");
    execFileSync("mkfifo", [log]);
    // Safety net: a regression would block the open; a late reader unblocks it so the test fails on time.
    const rescue = spawn("sh", ["-c", `sleep 3; cat '${log}' > '${path.join(root, "drained")}'`], { stdio: "ignore" });
    try {
      const started = Date.now();
      reportResidentExit(root, "idle-exit");
      expect(Date.now() - started).toBeLessThan(1_000);
      expect(fs.lstatSync(log).isFIFO()).toBe(true);
    } finally {
      rescue.kill("SIGKILL");
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it("rejects a FIFO launcher.log that has a reader, writing nothing", { timeout: 15_000 }, async () => {
    const root = dir();
    const log = path.join(root, "launcher.log"), drained = path.join(root, "drained");
    execFileSync("mkfifo", [log]);
    const reader = spawn("sh", ["-c", `cat '${log}' > '${drained}'`], { stdio: "ignore" });
    try {
      await new Promise(resolve => setTimeout(resolve, 300)); // reader is blocked in open
      reportResidentExit(root, "idle-exit");
      await new Promise<void>(resolve => { reader.once("exit", () => resolve()); setTimeout(() => { reader.kill("SIGKILL"); }, 2_000); });
      expect(fs.readFileSync(drained, "utf8")).toBe("");
    } finally {
      reader.kill("SIGKILL");
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it("writes nothing and reports on stderr when owned() fails at the open", () => {
    const root = dir();
    const lines: string[] = [];
    const write = process.stderr.write.bind(process.stderr);
    vi.spyOn(process.stderr, "write").mockImplementation(((chunk: string | Uint8Array, ...rest: never[]) => {
      lines.push(String(chunk));
      return write(chunk, ...rest);
    }) as typeof process.stderr.write);
    try {
      reportResidentExit(root, "idle-exit", () => false);
      expect(fs.existsSync(path.join(root, "launcher.log"))).toBe(false);
      expect(lines.join("")).toContain("Fabric resident host exit (idle-exit) without a proven root fence");
    } finally { vi.restoreAllMocks(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it("does not follow a symlinked launcher.log", () => {
    const root = dir();
    try {
      const target = path.join(root, "elsewhere.log");
      fs.writeFileSync(target, "");
      fs.symlinkSync(target, path.join(root, "launcher.log"));
      reportResidentExit(root, "idle-exit");
      expect(fs.readFileSync(target, "utf8")).toBe("");
      expect(fs.lstatSync(path.join(root, "launcher.log")).isSymbolicLink()).toBe(true);
    } finally { fs.rmSync(root, { recursive: true, force: true }); }
  });

  it("tightens an existing 0644 launcher.log to 0600 and appends one line", () => {
    const root = dir();
    try {
      const log = path.join(root, "launcher.log");
      fs.writeFileSync(log, `${JSON.stringify({ event: "launcher-started" })}\n`);
      fs.chmodSync(log, 0o644);
      reportResidentExit(root, "stopped");
      expect(fs.statSync(log).mode & 0o777).toBe(0o600);
      const lines = fs.readFileSync(log, "utf8").trim().split("\n");
      expect(lines).toHaveLength(2);
      expect(JSON.parse(lines[1]!)).toMatchObject({ event: "resident-exit", reason: "stopped", pid: process.pid });
    } finally { fs.rmSync(root, { recursive: true, force: true }); }
  });
});

// host.lock replaced after this host locked its old inode: the old fd is no
// fence. beforeRelease must not write the successor's launcher.log.
describe.skipIf(!posix)("resident exit without a proven fence", () => {
  const captureStderr = (): string[] => {
    const lines: string[] = [];
    const write = process.stderr.write.bind(process.stderr);
    vi.spyOn(process.stderr, "write").mockImplementation(((chunk: string | Uint8Array, ...rest: never[]) => {
      lines.push(String(chunk));
      return write(chunk, ...rest);
    }) as typeof process.stderr.write);
    return lines;
  };
  const successor = (residencyRoot: string, ownerPath: string, withOwner: boolean): Record<string, string> => {
    // A new host.lock inode, as a successor generation would establish.
    const replacement = path.join(residencyRoot, "host.lock.next");
    fs.writeFileSync(replacement, "", { mode: 0o600 });
    fs.renameSync(replacement, path.join(residencyRoot, "host.lock"));
    const files: Record<string, string> = {
      [path.join(residencyRoot, "launcher.log")]: `${JSON.stringify({ event: "launcher-started", pid: 4343 })}\n`,
    };
    if (withOwner) files[ownerPath] = JSON.stringify({ format: 1, hostId: "resident:successor", pid: process.pid, token: "successor", startedAt: Date.now() });
    for (const [file, bytes] of Object.entries(files)) fs.writeFileSync(file, bytes, { mode: 0o600 });
    return files;
  };

  it("a startup failure after host.lock is replaced leaves the successor's files byte-identical", { timeout: 60_000 }, async () => {
    const h = harness("replaced-start");
    const stderr = captureStderr();
    let files: Record<string, string> | undefined;
    const mkdirSync = fs.mkdirSync;
    vi.spyOn(fs, "mkdirSync").mockImplementation(((dir: fs.PathLike, options?: fs.MakeDirectoryOptions) => {
      if (!files && String(dir) === path.join(h.residencyRoot, "requests")) {
        files = successor(h.residencyRoot, h.ownerPath, true);
        throw new Error("injected start failure");
      }
      return mkdirSync(dir, options);
    }) as typeof fs.mkdirSync);
    try {
      await expect(runResidentHostFromConfigPath(h.configPath)).rejects.toThrow("injected start failure");
      expect(files).toBeDefined();
      for (const [file, bytes] of Object.entries(files!)) expect(fs.readFileSync(file, "utf8")).toBe(bytes);
      expect(stderr.join("")).toContain("Fabric resident host exit (error) without a proven root fence");
    } finally {
      vi.restoreAllMocks();
      fs.rmSync(h.root, { recursive: true, force: true });
    }
  });

  it("a stop after host.lock is replaced writes no exit line, even with its own owner.json", { timeout: 60_000 }, async () => {
    const h = harness("replaced-stop");
    const stderr = captureStderr();
    const controller = new AbortController();
    try {
      const run = runResidentHostFromConfigPath(h.configPath, controller.signal);
      await waitFor(() => fs.existsSync(h.ownerPath), 30_000);
      const files = successor(h.residencyRoot, h.ownerPath, false); // the fence check alone must refuse
      controller.abort();
      await run;
      for (const [file, bytes] of Object.entries(files)) expect(fs.readFileSync(file, "utf8")).toBe(bytes);
      expect(stderr.join("")).toContain("Fabric resident host exit (stopped) without a proven root fence");
    } finally {
      vi.restoreAllMocks();
      fs.rmSync(h.root, { recursive: true, force: true });
    }
  });
});
