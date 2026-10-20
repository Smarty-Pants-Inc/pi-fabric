import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
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
  fs.writeFileSync(path.join(source, "events.jsonl"), "{\"type\":\"message_end\"}\n");
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
  // The swap lands at the walk's first look at the copied child, by lstat (a path walk) or by open (an fd walk, which
  // may reach it through /proc/self/fd/<archive fd>/nested/child).
  const tail = `${path.sep}nested${path.sep}child`;
  const lstat = fs.lstatSync, open = fs.openSync;
  vi.spyOn(fs, "lstatSync").mockImplementation(((file: fs.PathLike, ...rest: unknown[]) => {
    const result = (lstat as (...args: unknown[]) => unknown)(file, ...rest);
    if (String(file).endsWith(tail)) swap();
    return result;
  }) as typeof fs.lstatSync);
  vi.spyOn(fs, "openSync").mockImplementation(((file: fs.PathLike, ...rest: unknown[]) => {
    if (String(file).endsWith(tail)) swap();
    return (open as (...args: unknown[]) => unknown)(file, ...rest);
  }) as typeof fs.openSync);
  await store.retainRun({ sessionFile }, "run-1", source);
  vi.restoreAllMocks();
  expect(swapped).toBe(true);
  expect(mode(outside)).toBe(0o755);
});

// pi-fabric#875 round 2 P1-b: the destination chain is validated BEFORE any copy. An unsafe chain refuses the archive.
it.skipIf(process.platform === "win32")("does not archive below a group-writable ancestor above the anchor, and warns", async () => {
  const { root, source, sessionFile, store } = archiveFixture();
  // The fixture has no actors/ directory: the anchor is the actor's own directory, so `root` is above it.
  fs.chmodSync(root, 0o775);
  const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  await expect(store.retainRun({ sessionFile }, "run-1", source)).rejects.toThrow(/writable by group or others/);
  expect(fs.existsSync(path.join(root, "actor", "runs"))).toBe(false);
  expect(warn).toHaveBeenCalledWith(expect.stringMatching(/not archived: .*writable by group or others/));
  expect(fs.existsSync(path.join(source, "status.json"))).toBe(true);
});

it.skipIf(process.platform === "win32")("does not archive through a symlinked destination, and warns", async () => {
  const { root, source, sessionFile, store } = archiveFixture();
  const outside = path.join(root, "outside");
  fs.mkdirSync(outside, { mode: 0o700 });
  fs.mkdirSync(path.join(root, "actor", "runs"), { mode: 0o700 });
  fs.symlinkSync(outside, path.join(root, "actor", "runs", "run-1"));
  const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  await expect(store.retainRun({ sessionFile }, "run-1", source)).rejects.toThrow(/symbolic link/);
  expect(fs.readdirSync(outside)).toEqual([]);
  expect(warn).toHaveBeenCalledWith(expect.stringMatching(/not archived: .*symbolic link/));
});

// pi-fabric#875 round 3: our own group-writable containers at or below the caller's Fabric root (the anchor) are
// tightened on first use; above it, and for a foreign owner, the refusal holds.
it.skipIf(process.platform === "win32")("tightens our own 0775 container below the anchor to 0755 and creates the run 0700", async () => {
  const { mkdirPrivate } = await import("../src/storage/private-dir.js");
  const anchor = tmp("fabric-private-anchor-");
  const runs = path.join(anchor, "residency", "host", "runs");
  fs.mkdirSync(runs, { recursive: true });
  for (const directory of [path.join(anchor, "residency"), path.join(anchor, "residency", "host"), runs]) fs.chmodSync(directory, 0o775);
  const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  mkdirPrivate(path.join(runs, "run-1"), anchor);
  for (const directory of [path.join(anchor, "residency"), path.join(anchor, "residency", "host"), runs]) expect(mode(directory), directory).toBe(0o755);
  expect(mode(path.join(runs, "run-1"))).toBe(0o700);
  expect(warn).toHaveBeenCalledWith(expect.stringMatching(new RegExp(`tightened ${runs.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")} from 775 to 755`)));
});

it.skipIf(process.platform === "win32")("refuses the same 0775 container above the anchor", async () => {
  const { mkdirPrivate } = await import("../src/storage/private-dir.js");
  const root = tmp("fabric-private-above-");
  const shared = path.join(root, "shared");
  fs.mkdirSync(path.join(shared, "anchor"), { recursive: true });
  fs.chmodSync(shared, 0o775);
  expect(() => mkdirPrivate(path.join(shared, "anchor", "run"), path.join(shared, "anchor"))).toThrow(/writable by group or others/);
  expect(mode(shared)).toBe(0o775);
});

it.skipIf(process.platform === "win32")("refuses a foreign-owned group-writable container below the anchor (injected fstat)", async () => {
  const { mkdirPrivate, privateDirTestHooks } = await import("../src/storage/private-dir.js");
  const anchor = tmp("fabric-private-foreign-");
  const runs = path.join(anchor, "runs");
  fs.mkdirSync(runs); fs.chmodSync(runs, 0o775);
  privateDirTestHooks.stat = (directory, stat) => directory === runs ? Object.assign(Object.create(Object.getPrototypeOf(stat)), stat, { uid: 4242 }) : stat;
  try { expect(() => mkdirPrivate(path.join(runs, "run-1"), anchor)).toThrow(/owned by uid 4242/); }
  finally { delete privateDirTestHooks.stat; }
  expect(mode(runs)).toBe(0o775);
  expect(fs.readdirSync(runs)).toEqual([]);
});

// pi-fabric#875 round 4 (SEC P1): a healed archive directory may hold entries planted while it was writable. Entries
// are created O_EXCL|O_NOFOLLOW through the held fd; an existing one is replaced only when it is our regular file.
const plantedArchive = () => {
  const fixture = archiveFixture();
  fs.writeFileSync(path.join(fixture.source, "reply.json"), '{"text":"archived"}');
  const archive = path.join(fixture.root, "actor", "runs", "run-1");
  fs.mkdirSync(archive, { recursive: true });
  for (const directory of [path.join(fixture.root, "actor", "runs"), archive]) fs.chmodSync(directory, 0o775);
  return { ...fixture, archive };
};
it.skipIf(process.platform === "win32")("never writes through a symlink planted in a healed archive directory", async () => {
  const { root, source, sessionFile, store, archive } = plantedArchive();
  const victim = path.join(root, "victim");
  fs.writeFileSync(victim, "victim", { mode: 0o600 });
  fs.symlinkSync(victim, path.join(archive, "status.json"));
  await expect(store.retainRun({ sessionFile }, "run-1", source)).rejects.toThrow(/Unsafe actor run .*archive entry status\.json/);
  expect(fs.readFileSync(victim, "utf8")).toBe("victim");
  expect(fs.lstatSync(path.join(archive, "status.json")).isSymbolicLink()).toBe(true);
  expect(mode(archive)).toBe(0o755); // healed
  expect(fs.existsSync(path.join(archive, "reply.json"))).toBe(false);
});

it.skipIf(process.platform === "win32")("writes new archive entries 0600, and replaces an existing entry only when it is our own regular file", async () => {
  const { source, sessionFile, store, archive } = plantedArchive();
  fs.writeFileSync(path.join(archive, "status.json"), "stale", { mode: 0o644 });
  await store.retainRun({ sessionFile }, "run-1", source);
  expect(fs.readFileSync(path.join(archive, "status.json"), "utf8")).toBe("{}");
  for (const file of ["status.json", "reply.json"]) expect(mode(path.join(archive, file)), file).toBe(0o600);
  expect(fs.readdirSync(archive).filter(name => name.endsWith(".tmp"))).toEqual([]);
});

// pi-fabric#875 round 4 (CODE P1): no path-based fallback. Without a working fd base, creation fails closed.
it.skipIf(process.platform !== "linux")("on Linux, fails closed when /proc/self/fd is unusable, and creates nothing", async () => {
  const { mkdirPrivate, privateDirTestHooks } = await import("../src/storage/private-dir.js");
  const root = tmp("fabric-private-nofd-");
  privateDirTestHooks.fdBases = [path.join(root, "no-such-fd-base")];
  try { expect(() => mkdirPrivate(path.join(root, "run"))).toThrow(/does not resolve a held directory on this Linux host/); }
  finally { delete privateDirTestHooks.fdBases; }
  expect(fs.existsSync(path.join(root, "run"))).toBe(false);
});

// pi-fabric#875 round 5 (CODE P1): under umask 0400, mkdir(0700) gives 0300, which O_RDONLY cannot open. The new
// directory is set to 0700 right after our mkdir, before the open; archive files are set to 0600 through their fd.
it.skipIf(process.platform === "win32")("creates usable 0700 directories and 0600 archive files under umask 0400", async () => {
  const { mkdirPrivate } = await import("../src/storage/private-dir.js");
  const root = tmp("fabric-umask-0400-");
  const { source, sessionFile, store, root: archiveRoot } = archiveFixture();
  umask = process.umask(0o400);
  mkdirPrivate(path.join(root, "a", "run"));
  await store.retainRun({ sessionFile }, "run-1", source);
  process.umask(umask); umask = undefined;
  for (const directory of [path.join(root, "a"), path.join(root, "a", "run")]) expect(mode(directory), directory).toBe(0o700);
  const archive = path.join(archiveRoot, "actor", "runs", "run-1");
  expect(mode(archive)).toBe(0o700);
  expect(mode(path.join(archive, "status.json"))).toBe(0o600);
  expect(fs.readFileSync(path.join(archive, "status.json"), "utf8")).toBe("{}");
});

// pi-fabric#875 round 5: macOS has no traversable /dev/fd/N. There (forced here: no fd base, platform darwin) the
// validated descriptor-free walk is used, never a refusal of every run and never main's unvalidated mkdir.
describe.skipIf(process.platform === "win32")("without a traversable fd base (darwin)", () => {
  const darwin = async () => {
    const module = await import("../src/storage/private-dir.js");
    module.privateDirTestHooks.fdBases = [path.join(tmp("fabric-no-fd-base-"), "none")];
    module.privateDirTestHooks.platform = "darwin";
    restoreHooks = () => { delete module.privateDirTestHooks.fdBases; delete module.privateDirTestHooks.platform; };
    return module;
  };
  let restoreHooks: (() => void) | undefined;
  afterEach(() => { restoreHooks?.(); restoreHooks = undefined; });

  it("falls back to the validated walk: directories it creates are 0700, under umask 0177 too", async () => {
    const { mkdirPrivate } = await darwin();
    const root = tmp("fabric-darwin-walk-");
    umask = process.umask(0o177);
    mkdirPrivate(path.join(root, "a", "run"));
    for (const directory of [path.join(root, "a"), path.join(root, "a", "run")]) expect(mode(directory), directory).toBe(0o700);
    fs.writeFileSync(path.join(root, "a", "run", "file"), "x");
  });

  it("creates usable 0700 directories under umask 0400 too", async () => {
    const { mkdirPrivate } = await darwin();
    const root = tmp("fabric-darwin-0400-");
    umask = process.umask(0o400);
    mkdirPrivate(path.join(root, "a", "run"));
    process.umask(umask); umask = undefined;
    for (const directory of [path.join(root, "a"), path.join(root, "a", "run")]) expect(mode(directory), directory).toBe(0o700);
  });

  it("still refuses a symlinked ancestor", async () => {
    const { mkdirPrivate } = await darwin();
    const root = tmp("fabric-darwin-link-");
    const outside = path.join(root, "outside");
    fs.mkdirSync(outside, { mode: 0o700 });
    fs.symlinkSync(outside, path.join(root, "link"));
    expect(() => mkdirPrivate(path.join(root, "link", "run"))).toThrow(/symbolic link/);
    expect(fs.readdirSync(outside)).toEqual([]);
  });

  it("archives 0700/0600 and never writes through a planted leaf symlink", async () => {
    await darwin();
    const { root, source, sessionFile, store, archive } = plantedArchive();
    const victim = path.join(root, "victim");
    fs.writeFileSync(victim, "victim", { mode: 0o600 });
    fs.symlinkSync(victim, path.join(archive, "status.json"));
    await expect(store.retainRun({ sessionFile }, "run-1", source)).rejects.toThrow(/Unsafe actor run .*archive entry status\.json/);
    expect(fs.readFileSync(victim, "utf8")).toBe("victim");
    expect(mode(archive)).toBe(0o755);
    expect(fs.existsSync(path.join(archive, "reply.json"))).toBe(false);
    const fresh = await (async () => {
      const again = archiveFixture();
      await again.store.retainRun({ sessionFile: again.sessionFile }, "run-2", again.source);
      return path.join(again.root, "actor", "runs", "run-2");
    })();
    expect(mode(fresh)).toBe(0o700);
    expect(mode(path.join(fresh, "status.json"))).toBe(0o600);
  });
});

it.skipIf(process.platform === "win32" || !fs.existsSync("/dev/fd"))("works through a /dev/fd base", async () => {
  const { mkdirPrivate, privateDirTestHooks } = await import("../src/storage/private-dir.js");
  const root = tmp("fabric-private-devfd-");
  privateDirTestHooks.fdBases = ["/dev/fd"];
  try { mkdirPrivate(path.join(root, "a", "run")); }
  finally { delete privateDirTestHooks.fdBases; }
  expect(mode(path.join(root, "a", "run"))).toBe(0o700);
});

// pi-fabric#875 round 4 (CODE P2-a): never heal through a symlink, even root's.
it.skipIf(process.platform === "win32")("does not heal through a root-owned symlink below the anchor (injected lstat)", async () => {
  const { mkdirPrivate, privateDirTestHooks } = await import("../src/storage/private-dir.js");
  const anchor = tmp("fabric-private-heal-link-");
  const outside = path.join(tmp("fabric-private-outside-"), "owned");
  fs.mkdirSync(outside); fs.chmodSync(outside, 0o775);
  const link = path.join(anchor, "runs");
  fs.symlinkSync(outside, link);
  privateDirTestHooks.linkStat = (directory, stat) => directory === link ? Object.assign(Object.create(Object.getPrototypeOf(stat)), stat, { uid: 0 }) : stat;
  try { expect(() => mkdirPrivate(path.join(link, "run-1"), anchor)).toThrow(/writable by group or others/); }
  finally { delete privateDirTestHooks.linkStat; }
  expect(mode(outside)).toBe(0o775);
  expect(fs.readdirSync(outside)).toEqual([]);
});

// pi-fabric#875 round 2 P1-a: existing ancestors and a competing creator are validated, never skipped.
it.skipIf(process.platform === "win32")("mkdirPrivate refuses a symlinked existing ancestor and writes nothing through it", async () => {
  const { mkdirPrivate } = await import("../src/storage/private-dir.js");
  const root = tmp("fabric-private-link-");
  const outside = path.join(root, "outside");
  fs.mkdirSync(outside, { mode: 0o700 });
  fs.symlinkSync(outside, path.join(root, "link"));
  expect(() => mkdirPrivate(path.join(root, "link", "run"))).toThrow(/symbolic link/);
  expect(fs.readdirSync(outside)).toEqual([]);
});

it.skipIf(process.platform === "win32")("mkdirPrivate refuses an existing group-writable ancestor", async () => {
  const { mkdirPrivate } = await import("../src/storage/private-dir.js");
  const root = tmp("fabric-private-open-");
  fs.mkdirSync(path.join(root, "shared"));
  fs.chmodSync(path.join(root, "shared"), 0o775);
  expect(() => mkdirPrivate(path.join(root, "shared", "run"))).toThrow(/writable by group or others/);
  expect(fs.readdirSync(path.join(root, "shared"))).toEqual([]);
});

it.skipIf(process.platform === "win32").each(["a group-writable directory", "a symlink"])("mkdirPrivate refuses a competing creator that made %s at the component it was creating", async kind => {
  const { mkdirPrivate, privateDirTestHooks } = await import("../src/storage/private-dir.js");
  const root = tmp("fabric-private-race-");
  const outside = path.join(root, "outside");
  fs.mkdirSync(outside, { mode: 0o700 });
  const contested = path.join(root, "run");
  privateDirTestHooks.beforeCreate = directory => {
    if (directory !== contested) return;
    if (kind === "a symlink") fs.symlinkSync(outside, contested);
    else { fs.mkdirSync(contested); fs.chmodSync(contested, 0o777); }
  };
  try { expect(() => mkdirPrivate(path.join(contested, "inner"))).toThrow(/symbolic link|changed while it was created|writable by group or others/); }
  finally { delete privateDirTestHooks.beforeCreate; }
  expect(fs.readdirSync(outside)).toEqual([]);
  if (kind !== "a symlink") expect(fs.readdirSync(contested)).toEqual([]);
});

it.skipIf(process.platform === "win32")("archives an actor run's nested tree with 0700 directories", async () => {
  const root = tmp("fabric-archive-mode-");
  const source = path.join(root, "source");
  const nested = path.join(source, "nested", "child", "nested", "grandchild");
  fs.mkdirSync(nested, { recursive: true, mode: 0o775 });
  for (const directory of [path.join(source, "nested"), path.join(source, "nested", "child"), path.join(source, "nested", "child", "nested"), nested]) fs.chmodSync(directory, 0o775);
  fs.writeFileSync(path.join(nested, "status.json"), "{}", { mode: 0o644 });
  fs.writeFileSync(path.join(source, "status.json"), "{}");
  fs.writeFileSync(path.join(source, "events.jsonl"), "{\"type\":\"message_end\"}\n");
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
