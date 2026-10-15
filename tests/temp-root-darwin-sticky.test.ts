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
  const present = (file: string, stat: fs.Stats): fs.Stats => {
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
  return { root, shared, mkdir, mkdtemp };
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
});
