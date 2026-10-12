import fs from "node:fs";
import { spawn } from "node:child_process";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { AgentManager } from "../src/agents/manager.js";
import { runResidentHostFromConfigPath } from "../src/residency/host.js";
import { residentGenerationRecords } from "../src/residency/generation-exit.js";
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
/** smarty-dev#7950: the host's exit is its generation's exit record (no longer a launcher.log line). */
const exitLines = (residencyRoot: string): Array<Record<string, unknown>> =>
  residentGenerationRecords<Record<string, unknown>>(residencyRoot, "exit.json", 10).map(entry => entry.record ?? {});
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
// host names it itself, synchronously, while it owns the root: since smarty-dev#7950
// in its generation's exit record (O_EXCL), on every platform.
const posix = process.platform !== "win32";
it("an idle exit leaves one idle-exit record of the host, written before owner.json is removed", { timeout: 60_000 }, async () => {
  const h = harness("idle");
  try {
    const run = runResidentHostFromConfigPath(h.configPath);
    await waitFor(() => fs.existsSync(h.ownerPath), 30_000);
    // Skip the 30 s idle window: the host's idle check reads Date.now().
    const now = Date.now.bind(Date);
    vi.spyOn(Date, "now").mockImplementation(() => now() + 60_000);
    await run;
    expect(h.atRelease.lines).toEqual([expect.objectContaining({ reason: "idle-exit", writer: "host", pid: process.pid })]);
    expect(exitLines(h.residencyRoot)).toHaveLength(1);
    expect(h.late()).toEqual([]);
    // Nothing of it in the shared launcher.log any more.
    expect(fs.existsSync(path.join(h.residencyRoot, "launcher.log")) ? fs.readFileSync(path.join(h.residencyRoot, "launcher.log"), "utf8") : "")
      .not.toContain("resident-exit");
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
    const expected = [expect.objectContaining({ reason: "stopped", writer: "host" })];
    expect(h.atRelease.lines).toEqual(expected);
    expect(exitLines(h.residencyRoot)).toEqual(expected);
    // After the release only this generation's own error record may appear: never a shared root file.
    expect(h.late().filter(file => !file.includes(`${path.sep}generations${path.sep}`))).toEqual([]);
    expect(fs.existsSync(path.join(h.residencyRoot, "error.json"))).toBe(false);
    expect(residentGenerationRecords<{ error: string }>(h.residencyRoot, "error.json")[0]?.record?.error).toBe("injected teardown failure");
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
    // smarty-dev#7950: and nothing new in the root: no error.json, no generation of its own.
    expect(fs.existsSync(path.join(h.residencyRoot, "error.json"))).toBe(false);
    expect(fs.existsSync(path.join(h.residencyRoot, "generations"))).toBe(false);
  } finally {
    if (previous === undefined) delete process.env.PI_FABRIC_RESIDENT_LAUNCHER; else process.env.PI_FABRIC_RESIDENT_LAUNCHER = previous;
    holder?.kill("SIGKILL");
    vi.restoreAllMocks();
    fs.rmSync(h.root, { recursive: true, force: true });
  }
});

// The record's file safety (O_EXCL: no symlink follow, no FIFO block, once per generation) is in
// tests/residency-generation-exit.test.ts (smarty-dev#7950).

// host.lock replaced after this host locked its old inode: the old fd is no
// fence. Its exit and error records go to its own generation only (smarty-dev#7950),
// so the successor's files stay byte-identical with no fence check at all.
describe.skipIf(!posix)("resident exit without a proven fence", () => {
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
      expect(fs.existsSync(path.join(h.residencyRoot, "error.json"))).toBe(false);
    } finally {
      vi.restoreAllMocks();
      fs.rmSync(h.root, { recursive: true, force: true });
    }
  });

  it("a stop after host.lock is replaced leaves the successor's files byte-identical; its exit goes to its own generation", { timeout: 60_000 }, async () => {
    const h = harness("replaced-stop");
    const controller = new AbortController();
    try {
      const run = runResidentHostFromConfigPath(h.configPath, controller.signal);
      await waitFor(() => fs.existsSync(h.ownerPath), 30_000);
      const files = successor(h.residencyRoot, h.ownerPath, false); // the fence check alone must refuse
      controller.abort();
      await run;
      for (const [file, bytes] of Object.entries(files)) expect(fs.readFileSync(file, "utf8")).toBe(bytes);
      expect(exitLines(h.residencyRoot)).toEqual([expect.objectContaining({ reason: "stopped", writer: "host" })]);
    } finally {
      vi.restoreAllMocks();
      fs.rmSync(h.root, { recursive: true, force: true });
    }
  });
});
