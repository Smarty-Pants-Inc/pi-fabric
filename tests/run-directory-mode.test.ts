import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ActorLogStore } from "../src/actors/log-store.js";

// smarty-dev#9016: a host started from a login shell runs under umask 002 (Ubuntu USERGROUPS_ENAB + pam_umask). Run
// directories it created were 0775, and verified retention (pi-fabric#850) refuses group-writable trees, so those
// runs were never collected (2000 on Ryzen 1). Run directories and archived nested runs are now 0700 under any umask.
const roots: string[] = [];
let umask: number | undefined;
afterEach(() => {
  if (umask !== undefined) { process.umask(umask); umask = undefined; }
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const tmp = (prefix: string) => { const root = fs.mkdtempSync(path.join(os.tmpdir(), prefix)); roots.push(root); return root; };
const mode = (file: string) => fs.statSync(file).mode & 0o777;

it.skipIf(process.platform === "win32")("creates a run directory 0700 under umask 002 through the real AgentManager spawn", async () => {
  const root = tmp("fabric-run-mode-");
  umask = process.umask(0o002);
  vi.spyOn(ProcessTransport.prototype, "launch").mockResolvedValue({
    kind: "process", sessionId: "2147483647", isAlive: async () => false, waitForClose: async () => {}, stop: async () => {},
  });
  const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: true }, {
    runRoot: path.join(root, "runs"), workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
  });
  try {
    const handle = await manager.spawn({ task: "run directory mode", transport: "process", extensions: false });
    expect(mode(manager.runDirectory(handle.id)!)).toBe(0o700);
  } finally { await manager.close(); }
});

// pi-fabric#875 round 1 P1-1: mkdir's mode is umask-filtered. Under umask 0177 it gave 0600 (no search bit), so
// nothing could be written below. mkdirPrivate sets each directory it creates to exactly 0700 through its fd.
it.skipIf(process.platform === "win32")("creates a usable 0700 run directory under umask 0177 through the real AgentManager spawn", async () => {
  const root = tmp("fabric-run-mode-0177-");
  umask = process.umask(0o177);
  vi.spyOn(ProcessTransport.prototype, "launch").mockResolvedValue({
    kind: "process", sessionId: "2147483647", isAlive: async () => false, waitForClose: async () => {}, stop: async () => {},
  });
  const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: true }, {
    runRoot: path.join(root, "runs"), workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
  });
  try {
    const handle = await manager.spawn({ task: "run directory mode 0177", transport: "process", extensions: false });
    const directory = manager.runDirectory(handle.id)!;
    expect(mode(directory)).toBe(0o700);
    fs.writeFileSync(path.join(directory, "probe.txt"), "written");
    expect(fs.readFileSync(path.join(directory, "task.txt"), "utf8")).toBe("run directory mode 0177");
  } finally { await manager.close(); }
});

it.skipIf(process.platform === "win32")("mkdirPrivate makes every directory it creates 0700 under umask 0177, and leaves existing ones", async () => {
  const { mkdirPrivate } = await import("../src/storage/private-dir.js");
  const root = tmp("fabric-private-dir-");
  fs.chmodSync(root, 0o755);
  umask = process.umask(0o177);
  const deep = path.join(root, "a", "b", "c");
  mkdirPrivate(deep);
  for (const directory of [path.join(root, "a"), path.join(root, "a", "b"), deep]) expect(mode(directory), directory).toBe(0o700);
  fs.writeFileSync(path.join(deep, "file"), "x");
  expect(mode(root)).toBe(0o755);
});

// pi-fabric#875 round 1 P1-2: the archive's copied directories are changed through O_NOFOLLOW fds, never a path chmod,
// and only when every ancestor up to the actor log root is ours and closed.
const archiveFixture = () => {
  const root = tmp("fabric-archive-race-");
  const source = path.join(root, "source");
  fs.mkdirSync(path.join(source, "nested", "child"), { recursive: true });
  fs.writeFileSync(path.join(source, "status.json"), "{}");
  const sessionFile = path.join(root, "actor", "session.jsonl");
  fs.mkdirSync(path.dirname(sessionFile), { recursive: true, mode: 0o700 });
  fs.chmodSync(path.dirname(sessionFile), 0o700);
  const store = new ActorLogStore({ maxEventBytes: 8192 }, { eventContextChars: 1000 }, { actorRunArchiveMs: 7 * 24 * 60 * 60 * 1000 });
  return { root, source, sessionFile, store, child: path.join(root, "actor", "runs", "run-1", "nested", "child") };
};

it.skipIf(process.platform === "win32")("does not follow a symlink swapped in at a copied nested path", async () => {
  const { root, source, sessionFile, store, child } = archiveFixture();
  const outside = path.join(root, "outside");
  fs.mkdirSync(outside); fs.chmodSync(outside, 0o755);
  let swapped = false;
  const swap = () => { if (swapped) return; swapped = true; fs.rmSync(child, { recursive: true, force: true }); fs.symlinkSync(outside, child); };
  // The swap lands at the walk's first look at the copied child, by lstat (a path walk) or by open (an fd walk).
  const lstat = fs.lstatSync, open = fs.openSync;
  vi.spyOn(fs, "lstatSync").mockImplementation(((file: fs.PathLike, ...rest: unknown[]) => {
    const result = (lstat as (...args: unknown[]) => unknown)(file, ...rest);
    if (String(file) === child) swap();
    return result;
  }) as typeof fs.lstatSync);
  vi.spyOn(fs, "openSync").mockImplementation(((file: fs.PathLike, ...rest: unknown[]) => {
    if (String(file) === child) swap();
    return (open as (...args: unknown[]) => unknown)(file, ...rest);
  }) as typeof fs.openSync);
  await store.retainRun({ sessionFile }, "run-1", source);
  vi.restoreAllMocks();
  expect(swapped).toBe(true);
  expect(mode(outside)).toBe(0o755);
});

it.skipIf(process.platform === "win32")("leaves the copied tree as is when an ancestor up to the actor log root is group-writable", async () => {
  const { root, source, sessionFile, store, child } = archiveFixture();
  fs.chmodSync(path.dirname(sessionFile), 0o775);
  fs.chmodSync(path.join(source, "nested", "child"), 0o755);
  const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  await store.retainRun({ sessionFile }, "run-1", source);
  expect(mode(child)).toBe(0o755);
  expect(warn).toHaveBeenCalledWith(expect.stringMatching(/not our private directory/));
  expect(fs.existsSync(path.join(root, "actor", "runs", "run-1", "status.json"))).toBe(true);
});

it.skipIf(process.platform === "win32")("archives an actor run's nested tree with 0700 directories", async () => {
  const root = tmp("fabric-archive-mode-");
  const source = path.join(root, "source");
  const nested = path.join(source, "nested", "child", "nested", "grandchild");
  fs.mkdirSync(nested, { recursive: true, mode: 0o775 });
  for (const directory of [path.join(source, "nested"), path.join(source, "nested", "child"), path.join(source, "nested", "child", "nested"), nested]) fs.chmodSync(directory, 0o775);
  fs.writeFileSync(path.join(nested, "status.json"), "{}", { mode: 0o644 });
  fs.writeFileSync(path.join(source, "status.json"), "{}");
  const sessionFile = path.join(root, "actor", "session.jsonl");
  fs.mkdirSync(path.dirname(sessionFile), { recursive: true });
  const store = new ActorLogStore({ maxEventBytes: 8192 }, { eventContextChars: 1000 }, { actorRunArchiveMs: 7 * 24 * 60 * 60 * 1000 });
  await store.retainRun({ sessionFile }, "run-1", source);
  const archived = path.join(root, "actor", "runs", "run-1", "nested");
  for (const directory of [archived, path.join(archived, "child"), path.join(archived, "child", "nested"), path.join(archived, "child", "nested", "grandchild")]) {
    expect(mode(directory), directory).toBe(0o700);
  }
  expect(mode(path.join(archived, "child", "nested", "grandchild", "status.json"))).toBe(0o644);
});
