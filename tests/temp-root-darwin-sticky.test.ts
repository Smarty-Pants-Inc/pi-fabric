import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { mkdirPrivate, privateDirTestHooks } from "../src/storage/private-dir.js";
import { fabricDataRoot } from "../src/storage/temp-root.js";

// smarty-dev#9527 (measured on m4): on darwin the private-dir walk refuses to pass a root-owned sticky shared ancestor
// (/tmp, /private/tmp), but the file-data root accepted one: PI_FABRIC_TMPDIR (or TMPDIR, when it is unset) nested
// under it was mkdir'ed, and AgentManager's managed run root was mkdtemp'ed there, before the later refusal. The
// preflight must refuse with the same #9527 message BEFORE any mkdir/mkdtemp, leaving the first missing ancestor absent.
//
// darwin is simulated on any POSIX runner: process.platform (own, configurable property, as temp-root-security does
// for win32) says darwin, and no fd base resolves (macOS has no traversable
// /dev/fd/N). The fixture is real, under the runner's own TMPDIR; only its `shared` directory is presented as root's
// sticky 1777 (path lstat/stat spies, and the walk's existing fstat hook). Real ancestors that are root sticky are
// presented as the per-user macOS layout (root 0755), so the only shared sticky directory is the injected one.
// Cleanup restores recorded mocks, env and hooks; fixtures stay in the runner's TMPDIR (no deletion path here).

const realTmp = os.tmpdir();
const restores: Array<() => void> = [];
afterEach(() => {
  for (const restore of restores.splice(0).reverse()) restore();
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
});

const asStat = (stat: fs.Stats, patch: Partial<fs.Stats>): fs.Stats =>
  Object.assign(Object.create(Object.getPrototypeOf(stat)), stat, patch);

const darwinFixture = (sticky: boolean) => {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(realTmp, "fabric-darwin-data-root-")));
  const shared = path.join(root, "shared");
  fs.mkdirSync(shared, { mode: 0o700 });
  fs.chmodSync(shared, 0o700);
  // Matched by path or by dev/ino, so the shared directory is presented as sticky when reached through an alias too.
  const real = fs.statSync(shared);
  const isShared = (file: string, stat: fs.Stats) =>
    path.resolve(file) === shared || (stat.isDirectory() && stat.dev === real.dev && stat.ino === real.ino);
  // Directories (by dev/ino) presented as root's 0755 to model the measured m4 system layout.
  const rootOwned = new Set<string>();
  const present = (file: string, stat: fs.Stats): fs.Stats => {
    if (stat.isDirectory() && rootOwned.has(`${stat.dev}:${stat.ino}`)) return asStat(stat, { uid: 0, mode: (stat.mode & ~0o7777) | 0o755 });
    if (sticky && isShared(file, stat)) return asStat(stat, { uid: 0, mode: (stat.mode & ~0o7777) | 0o1777 });
    if (stat.uid === 0 && (stat.mode & 0o1000) !== 0) return asStat(stat, { mode: (stat.mode & ~0o7777) | 0o755 });
    return stat;
  };
  const lstat = fs.lstatSync.bind(fs), stat = fs.statSync.bind(fs);
  vi.spyOn(fs, "lstatSync").mockImplementation(((file: fs.PathLike, ...rest: unknown[]) => {
    const result = (lstat as (...args: unknown[]) => fs.Stats | undefined)(file, ...rest);
    return result ? present(String(file), result) : result;
  }) as typeof fs.lstatSync);
  vi.spyOn(fs, "statSync").mockImplementation(((file: fs.PathLike, ...rest: unknown[]) => {
    const result = (stat as (...args: unknown[]) => fs.Stats | undefined)(file, ...rest);
    return result ? present(String(file), result) : result;
  }) as typeof fs.statSync);
  privateDirTestHooks.stat = (walked, seen) => present(walked, seen);
  privateDirTestHooks.fdBases = [path.join(root, "no-such-fd-base")];
  const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
  Object.defineProperty(process, "platform", { value: "darwin" });
  restores.push(() => {
    Object.defineProperty(process, "platform", platform);
    delete privateDirTestHooks.stat; delete privateDirTestHooks.fdBases;
  });
  for (const name of ["PI_FABRIC_TMPDIR", "PI_FABRIC_RUN_ROOT", "PI_FABRIC_BUDGET", "PI_FABRIC_BUDGET_FILE", "PI_FABRIC_BUDGET_ID"]) vi.stubEnv(name, undefined);
  vi.stubEnv("PI_FABRIC_DEPTH", "0");
  const mkdir = vi.spyOn(fs, "mkdirSync");
  const mkdtemp = vi.spyOn(fs, "mkdtempSync");
  const markRootOwned = (...directories: string[]) => {
    for (const directory of directories) { const seen = lstat(directory); rootOwned.add(`${seen.dev}:${seen.ino}`); }
  };
  return { root, shared, mkdir, mkdtemp, markRootOwned };
};

// Presents a system alias itself as root's to both the lstat spy and private-dir's linkStat hook.
const presentRootLink = (link: string) => {
  const lstat = vi.mocked(fs.lstatSync).getMockImplementation()!;
  const rootLink = (file: string, stat: fs.Stats) =>
    path.resolve(file) === link && stat.isSymbolicLink() ? asStat(stat, { uid: 0 }) : stat;
  vi.mocked(fs.lstatSync).mockImplementation(((file: fs.PathLike, ...rest: unknown[]) => {
    const result = (lstat as (...args: unknown[]) => fs.Stats | undefined)(file, ...rest);
    return result ? rootLink(String(file), result) : result;
  }) as typeof fs.lstatSync);
  privateDirTestHooks.linkStat = rootLink;
  restores.push(() => { delete privateDirTestHooks.linkStat; });
};

const errorCode = (run: () => unknown): string | undefined => {
  try { run(); } catch (error) { return (error as NodeJS.ErrnoException).code; }
  return undefined;
};
const modes = ["default os.tmpdir()", "PI_FABRIC_TMPDIR"] as const;
const useAlias = (mode: (typeof modes)[number], alias: string) => {
  if (mode === "PI_FABRIC_TMPDIR") vi.stubEnv("PI_FABRIC_TMPDIR", path.join(alias, "missing", "data"));
  else { vi.stubEnv("PI_FABRIC_TMPDIR", undefined); vi.spyOn(os, "tmpdir").mockReturnValue(alias); }
};

const sharedStickyRefusal = (shared: string) =>
  new RegExp(`${shared.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")} is a shared sticky directory[\\s\\S]*smarty-dev#9527`);
const mode = (file: string) => fs.statSync(file).mode & 0o777;
const createdBelow = (calls: unknown[][], directory: string) =>
  calls.map(call => path.resolve(String(call[0]))).filter(file => file === directory || file.startsWith(directory + path.sep));
const manager = () => new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: true, transport: "process", sessionExport: false }, {
  workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
  sweepPath: path.resolve("dist/storage/sweep-main.js"),
});

describe.skipIf(process.platform === "win32")("darwin file-data root under a root-owned sticky shared ancestor (smarty-dev#9527)", () => {
  it("fabricDataRoot refuses PI_FABRIC_TMPDIR nested under it before any mkdir; the first missing ancestor stays absent", () => {
    const { shared, mkdir, mkdtemp } = darwinFixture(true);
    const user = path.join(shared, "user");
    vi.stubEnv("PI_FABRIC_TMPDIR", path.join(user, "data"));
    expect(() => fabricDataRoot()).toThrow(sharedStickyRefusal(shared));
    expect(createdBelow(mkdir.mock.calls, shared)).toEqual([]);
    expect(mkdtemp).not.toHaveBeenCalled();
    expect(fs.existsSync(user)).toBe(false);
  });

  it("fabricDataRoot refuses the OS temp fallback (TMPDIR) under it when PI_FABRIC_TMPDIR is unset", () => {
    const { shared, mkdir, mkdtemp } = darwinFixture(true);
    const tmpdir = path.join(shared, "T");
    fs.mkdirSync(tmpdir, { mode: 0o700 });
    mkdir.mockClear();
    vi.spyOn(os, "tmpdir").mockReturnValue(tmpdir);
    expect(() => fabricDataRoot()).toThrow(sharedStickyRefusal(shared));
    expect(createdBelow(mkdir.mock.calls, shared)).toEqual([]);
    expect(mkdtemp).not.toHaveBeenCalled();
    expect(fs.readdirSync(tmpdir)).toEqual([]);
  });

  it("AgentManager's managed default run root refuses PI_FABRIC_TMPDIR under it before mkdtemp", () => {
    const { shared, mkdir, mkdtemp } = darwinFixture(true);
    const user = path.join(shared, "user");
    vi.stubEnv("PI_FABRIC_TMPDIR", path.join(user, "data"));
    expect(() => manager()).toThrow(sharedStickyRefusal(shared));
    expect(mkdtemp).not.toHaveBeenCalled();
    expect(createdBelow(mkdir.mock.calls, shared)).toEqual([]);
    expect(fs.existsSync(user)).toBe(false);
  });

  it("AgentManager's managed default run root refuses TMPDIR under it before mkdtemp (PI_FABRIC_TMPDIR unset)", () => {
    const { shared, mkdir, mkdtemp } = darwinFixture(true);
    const tmpdir = path.join(shared, "T");
    fs.mkdirSync(tmpdir, { mode: 0o700 });
    mkdir.mockClear();
    vi.spyOn(os, "tmpdir").mockReturnValue(tmpdir);
    expect(() => manager()).toThrow(sharedStickyRefusal(shared));
    expect(mkdtemp).not.toHaveBeenCalled();
    expect(createdBelow(mkdir.mock.calls, shared)).toEqual([]);
    expect(fs.readdirSync(tmpdir)).toEqual([]);
  });

  it("counterexample: the per-user macOS layout (root 0755 or our own ancestors) is accepted, with private 0700 roots", () => {
    const { shared, mkdtemp } = darwinFixture(false);
    const data = path.join(shared, "user", "data");
    vi.stubEnv("PI_FABRIC_TMPDIR", data);
    expect(fabricDataRoot()).toBe(data);
    expect(mode(path.join(shared, "user"))).toBe(0o700);
    expect(mode(data)).toBe(0o700);
    manager();
    expect(mkdtemp).toHaveBeenCalledTimes(1);
    const runRoots = fs.readdirSync(data).filter(name => name.startsWith("pi-fabric-runs-"));
    expect(runRoots).toHaveLength(1);
    const runRoot = path.join(data, runRoots[0]!);
    expect(mode(runRoot)).toBe(0o700);
    mkdirPrivate(path.join(runRoot, "run-1"), runRoot);
    expect(mode(path.join(runRoot, "run-1"))).toBe(0o700);
  });

  it("counterexample: unset PI_FABRIC_TMPDIR with a per-user os.tmpdir() beside the sticky one is accepted, 0700 managed run root", () => {
    const { root, shared, mkdtemp } = darwinFixture(true); // the injected sticky directory exists but is not an ancestor
    const tmpdir = path.join(root, "var", "T");
    fs.mkdirSync(tmpdir, { recursive: true, mode: 0o700 });
    fs.chmodSync(path.join(root, "var"), 0o700); fs.chmodSync(tmpdir, 0o700);
    vi.spyOn(os, "tmpdir").mockReturnValue(tmpdir);
    expect(fabricDataRoot()).toBe(tmpdir);
    manager();
    expect(mkdtemp).toHaveBeenCalledTimes(1);
    expect(path.dirname(path.resolve(String(mkdtemp.mock.calls[0]![0])))).toBe(tmpdir);
    const runRoots = fs.readdirSync(tmpdir).filter(name => name.startsWith("pi-fabric-runs-"));
    expect(runRoots).toHaveLength(1);
    const runRoot = path.join(tmpdir, runRoots[0]!);
    expect(mode(runRoot)).toBe(0o700);
    mkdirPrivate(path.join(runRoot, "run-1"), runRoot);
    expect(mode(path.join(runRoot, "run-1"))).toBe(0o700);
    expect(fs.readdirSync(shared)).toEqual([]);
  });

  it("os.tmpdir() default is a root-owned symlink alias of the shared sticky directory (/tmp -> /private/tmp): refused before mkdir/mkdtemp", () => {
    const { root, shared, mkdir, mkdtemp } = darwinFixture(true);
    const alias = path.join(root, "tmp");
    fs.symlinkSync(shared, alias, "dir");
    mkdir.mockClear();
    // The alias link is presented as root's (system layout) to both the lstat spy and the walk's linkStat hook.
    const lstat = vi.mocked(fs.lstatSync).getMockImplementation()!;
    const rootLink = (file: string, stat: fs.Stats) =>
      path.resolve(file) === alias && stat.isSymbolicLink() ? asStat(stat, { uid: 0 }) : stat;
    vi.mocked(fs.lstatSync).mockImplementation(((file: fs.PathLike, ...rest: unknown[]) => {
      const result = (lstat as (...args: unknown[]) => fs.Stats | undefined)(file, ...rest);
      return result ? rootLink(String(file), result) : result;
    }) as typeof fs.lstatSync);
    privateDirTestHooks.linkStat = rootLink;
    restores.push(() => { delete privateDirTestHooks.linkStat; });
    // The fixture really is an alias of shared, and the target is seen as root's sticky 1777 through it.
    expect(fs.realpathSync(alias)).toBe(shared);
    expect(fs.lstatSync(alias).isSymbolicLink()).toBe(true);
    expect(fs.lstatSync(alias).uid).toBe(0);
    const through = fs.statSync(alias);
    expect([through.uid, through.mode & 0o7777]).toEqual([0, 0o1777]);
    vi.stubEnv("PI_FABRIC_TMPDIR", undefined);
    vi.spyOn(os, "tmpdir").mockReturnValue(alias);
    const refusal = new RegExp(`(${[alias, shared].map(p => p.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")).join("|")}) is a shared sticky directory[\\s\\S]*smarty-dev#9527`);
    expect(() => fabricDataRoot()).toThrow(refusal);
    expect(() => manager()).toThrow(refusal);
    expect(mkdtemp).not.toHaveBeenCalled();
    expect([...createdBelow(mkdir.mock.calls, shared), ...createdBelow(mkdir.mock.calls, alias)]).toEqual([]);
    expect(fs.readdirSync(shared)).toEqual([]);
  });

  // Security P1: a non-sticky target can hide its shared-sticky physical ancestor from the lexical walk.
  it.each(modes)("%s: a symlink outside it naming a non-sticky descendant of the shared sticky directory is refused (#9527) before mkdir/mkdtemp", mode => {
    const { root, shared, mkdir, mkdtemp } = darwinFixture(true);
    const user = path.join(shared, "user");
    fs.mkdirSync(user, { mode: 0o700 }); fs.chmodSync(user, 0o700);
    fs.writeFileSync(path.join(user, "keep"), "");
    const alias = path.join(root, "user-tmp");
    fs.symlinkSync(user, alias, "dir");
    mkdir.mockClear();
    expect(alias.startsWith(shared + path.sep)).toBe(false);
    expect(fs.lstatSync(alias).isSymbolicLink()).toBe(true);
    expect(fs.realpathSync(alias)).toBe(user);
    const through = fs.statSync(alias);
    expect([through.uid, through.mode & 0o7777]).toEqual([process.getuid!(), 0o700]);
    const sharedSeen = fs.statSync(shared);
    expect([sharedSeen.uid, sharedSeen.mode & 0o7777]).toEqual([0, 0o1777]);
    useAlias(mode, alias);
    // Soft assertions exercise BOTH real entrypoints even on RED, exposing AgentManager's actual bootstrap write.
    expect.soft(() => fabricDataRoot()).toThrow(sharedStickyRefusal(shared));
    expect.soft(() => manager()).toThrow(sharedStickyRefusal(shared));
    expect(mkdtemp).not.toHaveBeenCalled();
    expect([...createdBelow(mkdir.mock.calls, shared), ...createdBelow(mkdir.mock.calls, alias)]).toEqual([]);
    expect(fs.readdirSync(user)).toEqual(["keep"]);
    expect(fs.readdirSync(shared)).toEqual(["user"]);
  });

  it.each(modes)("%s: a dangling link into the shared sticky directory fails closed (realpath ENOENT), nothing created", mode => {
    const { root, shared, mkdir, mkdtemp } = darwinFixture(true);
    const gone = path.join(shared, "gone");
    const alias = path.join(root, "dangling-tmp");
    fs.symlinkSync(gone, alias, "dir");
    mkdir.mockClear();
    expect(fs.lstatSync(alias).isSymbolicLink()).toBe(true);
    expect(errorCode(() => fs.realpathSync(alias))).toBe("ENOENT");
    useAlias(mode, alias);
    expect.soft(() => fabricDataRoot()).toThrow();
    expect.soft(() => manager()).toThrow();
    expect(mkdtemp).not.toHaveBeenCalled();
    expect([...createdBelow(mkdir.mock.calls, shared), ...createdBelow(mkdir.mock.calls, alias)]).toEqual([]);
    expect(fs.existsSync(gone)).toBe(false);
    expect(fs.readdirSync(shared)).toEqual([]);
  });

  it.each(modes)("%s: a realpath error (EACCES) on the data root fails closed, nothing created", mode => {
    const { root, shared, mkdir, mkdtemp } = darwinFixture(true);
    const target = path.join(root, "safe");
    fs.mkdirSync(target, { mode: 0o700 }); fs.chmodSync(target, 0o700);
    const alias = path.join(root, "denied-tmp");
    fs.symlinkSync(target, alias, "dir");
    mkdir.mockClear();
    // EACCES through the existing fs seam: our uid can access these real, private test fixtures.
    const realpath = fs.realpathSync, nativeRealpath = fs.realpathSync.native;
    const deny = (file: fs.PathLike) => {
      const resolved = path.resolve(String(file));
      if (resolved === alias || resolved.startsWith(alias + path.sep)) {
        throw Object.assign(new Error(`EACCES: permission denied, realpath '${resolved}'`), { code: "EACCES", errno: -13, syscall: "realpath", path: resolved });
      }
    };
    vi.spyOn(fs.realpathSync, "native").mockImplementation(((file: fs.PathLike, ...rest: unknown[]) => {
      deny(file); return (nativeRealpath as (...args: unknown[]) => string)(file, ...rest);
    }) as typeof fs.realpathSync.native);
    vi.spyOn(fs, "realpathSync").mockImplementation(((file: fs.PathLike, ...rest: unknown[]) => {
      deny(file); return (realpath as (...args: unknown[]) => string)(file, ...rest);
    }) as typeof fs.realpathSync);
    Object.assign(fs.realpathSync, { native: realpath.native });
    expect(errorCode(() => fs.realpathSync(alias))).toBe("EACCES");
    expect(errorCode(() => fs.realpathSync.native(alias))).toBe("EACCES");
    useAlias(mode, alias);
    expect.soft(() => fabricDataRoot()).toThrow();
    expect.soft(() => manager()).toThrow();
    expect(mkdtemp).not.toHaveBeenCalled();
    expect([...createdBelow(mkdir.mock.calls, target), ...createdBelow(mkdir.mock.calls, alias)]).toEqual([]);
    expect(fs.readdirSync(target)).toEqual([]);
    expect(fs.readdirSync(shared)).toEqual([]);
  });

  it("counterexample: the per-user physical layout alias /var/folders/../T -> /private/var/folders/../T is accepted by default, 0700 run root", () => {
    // Models lead-supplied m4 stat facts: system ancestors root0755, user hash directory ours0755, T ours0700.
    const { root, shared, mkdtemp, markRootOwned } = darwinFixture(true);
    const privateVar = path.join(root, "private", "var");
    const physical = path.join(privateVar, "folders", "xx", "user-hash", "T");
    fs.mkdirSync(physical, { recursive: true, mode: 0o700 });
    for (let dir = physical; dir !== root; dir = path.dirname(dir)) fs.chmodSync(dir, 0o700);
    fs.chmodSync(path.dirname(physical), 0o755);
    markRootOwned(path.join(root, "private"), privateVar, path.join(privateVar, "folders"), path.join(privateVar, "folders", "xx"));
    const varLink = path.join(root, "var");
    fs.symlinkSync(path.join("private", "var"), varLink, "dir");
    presentRootLink(varLink);
    const tmpdir = path.join(varLink, "folders", "xx", "user-hash", "T");
    expect(fs.realpathSync(tmpdir)).toBe(physical);
    expect(fs.lstatSync(varLink).uid).toBe(0);
    for (const dir of [path.join(root, "private"), privateVar, path.join(privateVar, "folders"), path.join(privateVar, "folders", "xx")]) {
      const seen = fs.statSync(dir);
      expect([seen.uid, seen.mode & 0o7777]).toEqual([0, 0o755]);
    }
    expect([fs.statSync(path.dirname(tmpdir)).uid, mode(path.dirname(tmpdir))]).toEqual([process.getuid!(), 0o755]);
    expect([fs.statSync(tmpdir).uid, mode(tmpdir)]).toEqual([process.getuid!(), 0o700]);
    vi.stubEnv("PI_FABRIC_TMPDIR", undefined);
    vi.spyOn(os, "tmpdir").mockReturnValue(tmpdir);
    expect([tmpdir, physical]).toContain(fabricDataRoot());
    manager();
    expect(mkdtemp).toHaveBeenCalledTimes(1);
    expect(fs.realpathSync(path.dirname(path.resolve(String(mkdtemp.mock.calls[0]![0]))))).toBe(physical);
    const runRoots = fs.readdirSync(physical).filter(name => name.startsWith("pi-fabric-runs-"));
    expect(runRoots).toHaveLength(1);
    const runRoot = path.join(physical, runRoots[0]!);
    expect(mode(runRoot)).toBe(0o700);
    mkdirPrivate(path.join(runRoot, "run-1"), runRoot);
    expect(mode(path.join(runRoot, "run-1"))).toBe(0o700);
    expect(fs.readdirSync(shared)).toEqual([]);
  });

  // Security P1 (alias bounce): A (outside) -> shared/<other uid's child>/linkB -> the safe per-user T. The final path and
  // the lexical chain of A look safe, but the other uid can repoint linkB after the preflight; the returned root must be
  // the vetted canonical T, so AgentManager's mkdtemp never re-resolves the mutable chain.
  it("default os.tmpdir(): an alias bouncing through another uid's child of the shared sticky directory returns canonical T; repointing it after preflight cannot move the run root", () => {
    const nativeMkdtemp = fs.mkdtempSync; // captured before the fixture spies on it
    const { root, shared, mkdir, mkdtemp } = darwinFixture(true);
    const physical = path.join(root, "home", "T");
    fs.mkdirSync(physical, { recursive: true, mode: 0o700 });
    fs.chmodSync(path.dirname(physical), 0o700); fs.chmodSync(physical, 0o700);
    const redirect = path.join(root, "redirect");
    fs.mkdirSync(redirect, { mode: 0o700 }); fs.chmodSync(redirect, 0o700);
    const child = path.join(shared, "other-user-child");
    fs.mkdirSync(child, { mode: 0o700 }); fs.chmodSync(child, 0o777);
    const linkB = path.join(child, "linkB");
    fs.symlinkSync(physical, linkB, "dir");
    const alias = path.join(root, "bounce-tmp");
    fs.symlinkSync(linkB, alias, "dir");
    // A real chown is impossible here: the child is presented (by dev/ino) as another uid's, world-writable 0777.
    const foreignUid = process.getuid!() + 1;
    const childSeen = fs.lstatSync(child);
    const presentForeign = (_file: string, stat: fs.Stats) =>
      stat.isDirectory() && stat.dev === childSeen.dev && stat.ino === childSeen.ino ? asStat(stat, { uid: foreignUid }) : stat;
    for (const spied of [fs.lstatSync, fs.statSync]) {
      const inner = vi.mocked(spied).getMockImplementation()!;
      vi.mocked(spied).mockImplementation(((file: fs.PathLike, ...rest: unknown[]) => {
        const result = (inner as (...args: unknown[]) => fs.Stats | undefined)(file, ...rest);
        return result ? presentForeign(String(file), result) : result;
      }) as never);
    }
    const hook = privateDirTestHooks.stat;
    privateDirTestHooks.stat = (walked, seen) => presentForeign(walked, hook ? hook(walked, seen) : seen);
    // The other uid's move, staged entirely inside the fixture root: atomically rename a replacement link over linkB
    // just after AgentManager evaluated fabricDataRoot(), then allocate with the real mkdtemp and its actual prefix.
    let repointed = false;
    mkdtemp.mockImplementation(((prefix: string, ...rest: unknown[]) => {
      if (!repointed) {
        repointed = true;
        const next = path.join(child, "linkB.next");
        fs.symlinkSync(redirect, next, "dir");
        fs.renameSync(next, linkB);
      }
      return (nativeMkdtemp as (...args: unknown[]) => string)(prefix, ...rest);
    }) as typeof fs.mkdtempSync);
    mkdir.mockClear();
    // Fixture facts: A is outside shared and resolves to the safe T only by bouncing through the foreign child.
    expect(alias.startsWith(shared + path.sep)).toBe(false);
    expect(physical.startsWith(shared + path.sep)).toBe(false);
    expect(fs.lstatSync(alias).isSymbolicLink()).toBe(true);
    expect(fs.readlinkSync(alias)).toBe(linkB);
    expect(fs.lstatSync(linkB).isSymbolicLink()).toBe(true);
    expect(fs.realpathSync(alias)).toBe(physical);
    const sharedSeen = fs.statSync(shared);
    expect([sharedSeen.uid, sharedSeen.mode & 0o7777]).toEqual([0, 0o1777]);
    const foreign = fs.lstatSync(child);
    expect([foreign.uid, foreign.mode & 0o7777]).toEqual([foreignUid, 0o777]);
    expect([fs.statSync(physical).uid, mode(physical)]).toEqual([process.getuid!(), 0o700]);
    expect([fs.statSync(alias).uid, mode(alias)]).toEqual([process.getuid!(), 0o700]);
    expect([fs.statSync(redirect).uid, mode(redirect)]).toEqual([process.getuid!(), 0o700]);
    expect(fs.readdirSync(redirect)).toEqual([]);
    useAlias("default os.tmpdir()", alias);
    // Soft: on RED the lexical A comes back, and the real manager below still shows the resulting wrong allocation.
    expect.soft(fabricDataRoot()).toBe(physical);
    expect(mkdtemp).not.toHaveBeenCalled();
    manager();
    expect(mkdtemp).toHaveBeenCalledTimes(1);
    const prefix = path.resolve(String(mkdtemp.mock.calls[0]![0]));
    expect.soft(path.dirname(prefix)).toBe(physical); // the canonical T, not the alias A
    expect(repointed).toBe(true);
    expect(fs.realpathSync(alias)).toBe(redirect); // the repoint really happened before allocation
    expect(fs.readdirSync(child)).toEqual(["linkB"]);
    expect(fs.readdirSync(redirect)).toEqual([]);
    const runRoots = fs.readdirSync(physical).filter(name => name.startsWith("pi-fabric-runs-"));
    expect(runRoots).toHaveLength(1);
    const runRoot = path.join(physical, runRoots[0]!);
    expect([fs.statSync(runRoot).uid, mode(runRoot)]).toEqual([process.getuid!(), 0o700]);
    expect([...createdBelow(mkdir.mock.calls, shared), ...createdBelow(mkdir.mock.calls, alias)]).toEqual([]);
    expect(fs.readdirSync(shared)).toEqual(["other-user-child"]);
  });

  // Input validation: an existing non-directory is refused even if the link target becomes a directory after stat.
  const entrypoints = ["fabricDataRoot", "AgentManager"] as const;
  it.each(modes.flatMap(m => entrypoints.map(entry => [m, entry] as const)))("%s via %s: an alias naming a regular file, changed to a directory after validation stat, is refused (#9527 non-directory), nothing created", (mode, entry) => {
    const { root, shared, mkdir, mkdtemp } = darwinFixture(true);
    const file = path.join(root, "file");
    fs.writeFileSync(file, "keep");
    const destination = path.join(root, "destination");
    fs.mkdirSync(destination, { mode: 0o700 }); fs.chmodSync(destination, 0o700);
    const alias = path.join(root, "file-tmp");
    fs.symlinkSync(file, alias, "file");
    // Check the fixture before installing the target-change hook.
    expect(fs.lstatSync(alias).isSymbolicLink()).toBe(true);
    expect(fs.realpathSync(alias)).toBe(file);
    expect(fs.lstatSync(file).isFile()).toBe(true);
    expect(fs.statSync(alias).isFile()).toBe(true);
    expect([fs.statSync(destination).uid, fs.statSync(destination).mode & 0o777]).toEqual([process.getuid!(), 0o700]);
    expect(fs.readdirSync(destination)).toEqual([]);
    // Return the existing file stat after changing the link to the empty directory, before any allocation.
    const inner = vi.mocked(fs.statSync).getMockImplementation()!;
    let changed = false;
    vi.mocked(fs.statSync).mockImplementation(((target: fs.PathLike, ...rest: unknown[]) => {
      const result = (inner as (...args: unknown[]) => fs.Stats | undefined)(target, ...rest);
      if (!changed && path.resolve(String(target)) === alias) {
        changed = true;
        const next = path.join(root, "file-tmp.next");
        fs.symlinkSync(destination, next, "dir");
        fs.renameSync(next, alias);
      }
      return result;
    }) as typeof fs.statSync);
    mkdir.mockClear(); mkdtemp.mockClear();
    useAlias(mode, alias);
    const refusal = new RegExp(`^(?=[\\s\\S]*${alias.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")})(?=[\\s\\S]*(?:not a directory|non-?directory))(?=[\\s\\S]*smarty-dev#9527)`, "i");
    // Soft: inspect the real entrypoint's filesystem effects even when the RED refusal assertion fails.
    if (entry === "fabricDataRoot") expect.soft(() => fabricDataRoot()).toThrow(refusal);
    else expect.soft(() => manager()).toThrow(refusal);
    expect(changed).toBe(true);
    expect(fs.realpathSync(alias)).toBe(destination);
    expect(mkdtemp).not.toHaveBeenCalled();
    expect(createdBelow(mkdir.mock.calls, root)).toEqual([]);
    expect(fs.readdirSync(destination)).toEqual([]);
    expect(fs.existsSync(path.join(destination, "missing"))).toBe(false);
    expect(fs.readFileSync(file, "utf8")).toBe("keep");
    expect(fs.readdirSync(root).sort()).toEqual(["destination", "file", "file-tmp", "shared"]);
    expect(fs.readdirSync(shared)).toEqual([]);
  });
});
