import fs from "node:fs";
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
it("an idle exit leaves one resident-exit idle-exit line, written before owner.json is removed", { timeout: 60_000 }, async () => {
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

// #1882: after the host releases owner.json the root may belong to the next
// generation: no exit line, no error.json, nothing late.
it("a failure from close() after the release writes no line, no error.json, nothing late", { timeout: 60_000 }, async () => {
  const h = harness("release");
  // The release is the last step of close(); this failure surfaces after it.
  vi.spyOn(AgentManager.prototype, "close").mockRejectedValue(new Error("injected teardown failure"));
  const controller = new AbortController();
  try {
    const run = runResidentHostFromConfigPath(h.configPath, controller.signal);
    await waitFor(() => fs.existsSync(h.ownerPath), 30_000);
    controller.abort();
    await expect(run).rejects.toThrow("injected teardown failure");
    expect(h.atRelease.lines).toEqual([expect.objectContaining({ event: "resident-exit", reason: "stopped" })]);
    expect(exitLines(h.residencyRoot)).toHaveLength(1);
    expect(h.late()).toEqual([]);
    expect(fs.existsSync(path.join(h.residencyRoot, "error.json"))).toBe(false);
  } finally {
    vi.restoreAllMocks();
    fs.rmSync(h.root, { recursive: true, force: true });
  }
});

// A host that never owned the root has no fence: no error.json, stderr only.
it("a pre-ownership failure (launch-context throw) writes no error.json; stderr has it", { timeout: 60_000 }, async () => {
  const h = harness("pre-owner");
  const previous = process.env.PI_FABRIC_RESIDENT_LAUNCHER;
  process.env.PI_FABRIC_RESIDENT_LAUNCHER = JSON.stringify({ pid: 1, processStartTime: "1", token: "x", entry: "x", runtime: "x" });
  const stderr: string[] = [];
  const write = process.stderr.write.bind(process.stderr);
  vi.spyOn(process.stderr, "write").mockImplementation(((chunk: string | Uint8Array, ...rest: never[]) => {
    stderr.push(String(chunk));
    return write(chunk, ...rest);
  }) as typeof process.stderr.write);
  try {
    await expect(runResidentHostFromConfigPath(h.configPath)).rejects.toThrow("Resident launcher identity is uncertain");
    expect(fs.existsSync(path.join(h.residencyRoot, "error.json"))).toBe(false);
    expect(fs.existsSync(h.ownerPath)).toBe(false);
    expect(exitLines(h.residencyRoot)).toEqual([]);
    expect(stderr.join("")).toContain("Fabric resident host failed before owning its root: Resident launcher identity is uncertain");
  } finally {
    if (previous === undefined) delete process.env.PI_FABRIC_RESIDENT_LAUNCHER; else process.env.PI_FABRIC_RESIDENT_LAUNCHER = previous;
    vi.restoreAllMocks();
    fs.rmSync(h.root, { recursive: true, force: true });
  }
});

describe.skipIf(process.platform === "win32")("resident-exit line file safety", () => {
  const dir = (): string => fs.mkdtempSync(path.join(os.tmpdir(), "resident-exit-file-"));

  it("skips the write without O_NOFOLLOW (fail closed)", () => {
    const root = dir();
    try {
      const { O_NOFOLLOW: _omit, ...constants } = fs.constants;
      reportResidentExit(root, "idle-exit", constants);
      expect(fs.existsSync(path.join(root, "launcher.log"))).toBe(false);
      reportResidentExit(root, "idle-exit");
      expect(exitLines(root)).toEqual([expect.objectContaining({ reason: "idle-exit" })]);
    } finally { fs.rmSync(root, { recursive: true, force: true }); }
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
