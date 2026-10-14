import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { cutoverMeshState, importMeshState, meshBackendStatus, type MeshBackendOptions } from "../src/mesh/backend-migration.js";
import { main } from "../src/mesh/mesh-backend-cli.js";
import { decodeMeshStateFile, encodeMeshStateFile, type MeshStateFile } from "../src/mesh/state-file.js";
import { MeshStateUnsupportedError, openNodeSqlite, type SqliteConnection, type SqliteOpener, type SqliteStatement } from "../src/mesh/state-sqlite.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { pinImportRoot, type RefusalCleanupReport } from "../src/mesh/owned-import-files.js";
import { openPrivateStateDb } from "../src/mesh/state-gate.js";

// Review 1029: a deterministic randomBytes(16) failure (only the quarantine entry name draws 16 bytes), armed only at
// the importing refusal; every other call delegates to the real node:crypto.
const rng = vi.hoisted(() => ({ fail: false, hits: 0 }));
vi.mock("node:crypto", async (importOriginal) => {
  const actual = await importOriginal<typeof import("node:crypto")>();
  const randomBytes = ((size: number, ...rest: unknown[]) => {
    if (rng.fail && size === 16) { rng.hits += 1; throw Object.assign(new Error("injected RNG failure"), { code: "ERR_INJECTED_RNG" }); }
    return (actual.randomBytes as (...args: unknown[]) => unknown)(size, ...rest);
  }) as typeof actual.randomBytes;
  return { ...actual, default: { ...actual, randomBytes }, randomBytes };
});

// smarty-dev#9501 (no-deletion goal): a readiness refusal under the fence, before the importing commit, MOVES the
// state.db, -wal and -shm this run created (lstat identity re-checked, through the held directory fd, fence still held)
// into a private `.import-quarantine-*` slot and RETAINS every moved entry: no unlink and no rmdir, ever. The report
// names each retained entry by its root-relative path (`<slot>/<name>`, never just the directory) with warnings and
// operator manual-cleanup guidance. A late foreign replacement keeps its original name AND its quarantine alias.
// Exit-1 errors and committed phases keep the database. #4664: "recorded cleanup" installs the removal recorder BEFORE
// every run: any removal call (state.db*, quarantine entry or directory, protected inode) is recorded and dropped, and
// GREEN requires calls == []. "hosted cleanup" (CI) runs the same product with no recorder: nothing is deleted there
// either, and the reported quarantine paths must hold the created inodes.

const identity: MeshIdentity = { id: "tester", name: "tester", kind: "agent" };
const dirs: string[] = [];
const savedRelease = process.env.PI_FABRIC_RELEASE_SHA;
beforeEach(() => { process.env.PI_FABRIC_RELEASE_SHA = "fabric-rel-a"; });
afterEach(() => {
  vi.restoreAllMocks(); // before the teardown: the fixture removes only its own mkdtemp roots, with the real fs
  rng.fail = false;
  rng.hits = 0;
  if (savedRelease === undefined) delete process.env.PI_FABRIC_RELEASE_SHA; else process.env.PI_FABRIC_RELEASE_SHA = savedRelease;
  for (const dir of dirs.splice(0)) fs.rmSync(dir, { recursive: true, force: true });
});

const fileRoot = async (): Promise<string> => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-refusal-cleanup-"));
  dirs.push(root);
  const store = new MeshStore(root, 64 * 1024, 1_000, { stateBackend: "file" });
  await store.put({ key: "seed/a", value: { n: 1 }, identity });
  await store.put({ key: "seed/b", value: "two", identity });
  store.closeState();
  return root;
};

const DB_NAME = /^state\.db(?:-wal|-shm|-journal)?$/;
const idOf = (stat: fs.Stats | undefined): string | undefined => stat ? `${stat.dev}:${stat.ino}` : undefined;
const dbNames = (root: string): string[] => fs.readdirSync(root).filter(name => DB_NAME.test(name)).sort();

type Writer = { pid: number; release: string; mode: string };
type Commit = Parameters<NonNullable<MeshBackendOptions["beforeCommit"]>>[0];

/**
 * The CLI with a gate that refuses under the fence: a writer on an unproved release appears right before the
 * `refuseAt` commit (as in mesh-backend-readiness.test.ts). `opened` records the identity of each state.db open.
 */
const run = async (root: string, command: "import" | "cutover", input: { refuseAt?: Commit["phase"]; beforeCommit?: (commit: Commit) => void;
  onStep?: MeshBackendOptions["onStep"]; failCloseAfterRefusal?: boolean } = {}) => {
  let live: Writer[] = [{ pid: 7, release: "fabric-rel-a", mode: "sqlite" }];
  const opened: string[] = [];
  // failCloseAfterRefusal: the first close() once the refusal is armed throws WITHOUT closing (a failed close); the
  // fixture closes those connections itself after main() returns.
  let closeArmed = false;
  const unclosed: SqliteConnection[] = [];
  const open: SqliteOpener = (file) => {
    opened.push(idOf(fs.lstatSync(file))!);
    const db = openNodeSqlite(file);
    if (!input.failCloseAfterRefusal) return db;
    return { exec: (sql) => db.exec(sql), prepare: (sql) => db.prepare(sql), get isTransaction() { return db.isTransaction; },
      close: () => {
        if (closeArmed) { closeArmed = false; unclosed.push(db); throw Object.assign(new Error("EIO: injected close failure"), { code: "EIO" }); }
        db.close();
      } };
  };
  let out = "";
  let err = "";
  // holder@lease-probe is accepted: the native lease helper exists only after `bun run build`; the refusal under test
  // is fabric@fabric-rel-new, never accepted.
  const code = await main([command, "--root", root, "--json", "--accept-unready", "holder@lease-probe"], {
    census: async () => ({ writers: live }), gateCensus: () => ({ writers: live, unknown: [] }), builtinReaders: [],
    procScan: { listPids: () => [] }, stdout: (text) => { out += text; }, stderr: (text) => { err += text; },
    options: { open, ...(input.onStep ? { onStep: input.onStep } : {}), beforeCommit: (commit) => {
      input.beforeCommit?.(commit);
      if (commit.phase === input.refuseAt) { live = [...live, { pid: 11, release: "fabric-rel-new", mode: "sqlite" }]; closeArmed = true; }
    } },
  });
  // The names present when main() returned, before the fixture's own close (SQLite's close may remove its sidecars).
  const atExit = dbNames(root);
  for (const db of unclosed.splice(0)) db.close();
  const json = JSON.parse(out) as { ok: boolean; refused?: boolean; error?: string; cleanupWarnings?: string[]; retained?: string[]; reason?: string };
  return { code, json, err, opened, atExit };
};
const proveSqlite = async (root: string): Promise<void> => {
  let err = "";
  expect(await main(["reader-proof", "--root", root, "--backend", "sqlite"], { stderr: (text) => { err += text; }, stdout: () => undefined }), err).toBe(0);
  expect(dbNames(root)).toEqual([]);
};
const refusedUnderFence = (result: Awaited<ReturnType<typeof run>>, phase: Commit["phase"]): void => {
  expect(result.code, result.err).toBe(3);
  expect(result.json).toMatchObject({ ok: false, refused: true });
  expect(result.json.error).toContain(`(under the fence, before the ${phase} commit): fabric@fabric-rel-new`);
};

interface Removal { api: string; name: string; rel: string; viaDirFd: boolean; dirMatches: boolean; target: string | undefined; links: number;
  /** A nested/new-top-level path (a quarantine shape), and the identity `<root>/state.db` named at the call. */
  quarantine: boolean; original: string | undefined; meshLock: boolean; custodyLock: boolean }
/** The fence locks' own files and staging (`.lock`, `custody.lock`, `custody.lock.pending.*`): always the real fs. */
const LOCKISH = /^(?:\.lock|custody\.lock)(?:$|\.)/;

/**
 * #4664: installed BEFORE the CLI runs. Every unlink/rm of a state.db* name, by any path (`<root>/state.db`, or a
 * held directory fd `/proc/self/fd/N/state.db` or `/dev/fd/N/state.db`, normalized to the basename with the fd's
 * directory identity checked against the root), is recorded with the target's identity and both fence locks'
 * presence, and NEVER performed. `fail` makes a recorded call throw instead. Any other path goes to the real fs
 * (the fence locks' own owner files).
 */
/*
 * #9501 quarantine: also intercepted (recorded, never performed) are nested paths and directories under a top-level
 * entry that did not exist when the recorder was installed (any quarantine name, reached through `/proc/self/fd/<root
 * fd>/<q>/<name>` or `/proc/self/fd/<q fd>/<name>`), and any removal of an inode in `protect()` wherever it is. `name`
 * is the basename (the logical state.db* name), `rel` the path relative to the root, `links` the target's link count
 * at the call (a removal of an alias whose inode keeps another name is not a loss).
 */
const recordRemovals = (root: string, fail?: (name: string, target: string) => NodeJS.ErrnoException | undefined,
  protect: () => ReadonlySet<string | undefined> = () => new Set()): Removal[] => {
  const calls: Removal[] = [];
  const realRoot = fs.realpathSync(root);
  const realLstat = fs.lstatSync;
  const before = new Set(fs.readdirSync(root));
  const locate = (text: string): { abs: string; viaDirFd: boolean; dirMatches: boolean } | undefined => {
    const pinned = /^(?:\/proc\/self\/fd|\/dev\/fd)\/(\d+)((?:\/[^/]+)*)\/?$/.exec(text);
    try {
      if (pinned) {
        if (!fs.fstatSync(Number(pinned[1])).isDirectory()) return undefined;
        const dir = fs.readlinkSync(`/proc/self/fd/${pinned[1]}`);
        const inRoot = dir === realRoot || dir.startsWith(`${realRoot}/`);
        return { abs: path.join(dir, pinned[2] || "."), viaDirFd: true, dirMatches: inRoot };
      }
      const resolved = path.resolve(text);
      const abs = path.join(fs.realpathSync(path.dirname(resolved)), path.basename(resolved));
      return { abs, viaDirFd: false, dirMatches: path.dirname(abs) === realRoot || path.dirname(abs).startsWith(`${realRoot}/`) };
    } catch { return pinned ? { abs: text, viaDirFd: true, dirMatches: false } : undefined; }
  };
  const intercept = (api: string, target: fs.PathLike): boolean => {
    const text = target instanceof URL ? fileURLToPath(target) : String(target);
    const found = realLstat(text, { throwIfNoEntry: false });
    const where = locate(text);
    const rel = where ? path.relative(realRoot, where.abs) : text;
    const inside = where !== undefined && rel !== "" && !rel.startsWith("..") && !path.isAbsolute(rel);
    const first = rel.split("/")[0]!;
    const name = path.basename(where?.abs ?? text);
    // rmdir of a quarantine directory is recorded, never real (a recorded unlink leaves its entry: ENOTEMPTY otherwise).
    const quarantined = inside && !LOCKISH.test(first) && (first.startsWith(".import-quarantine")
      || (!before.has(first) && (rel.includes("/") || found?.isDirectory() === true)));
    const isDb = DB_NAME.test(name) && (inside || where?.viaDirFd === true || !LOCKISH.test(first));
    if (!isDb && !quarantined && !(found && protect().has(idOf(found)))) return false;
    const held = (lock: string) => fs.existsSync(path.join(root, lock, "owner"));
    calls.push({ api, name, rel, viaDirFd: where?.viaDirFd ?? false, dirMatches: where?.dirMatches ?? false, target: idOf(found),
      links: found?.nlink ?? 0, quarantine: quarantined, original: idOf(realLstat(path.join(root, "state.db"), { throwIfNoEntry: false })), meshLock: held(".lock"), custodyLock: held("custody.lock") });
    const error = fail?.(name, text);
    if (error) throw error;
    return true;
  };
  const real = { unlinkSync: fs.unlinkSync, rmSync: fs.rmSync, rmdirSync: fs.rmdirSync, unlink: fs.promises.unlink, rm: fs.promises.rm,
    rmdir: fs.promises.rmdir };
  vi.spyOn(fs, "unlinkSync").mockImplementation((target) => { if (!intercept("unlinkSync", target)) real.unlinkSync(target); });
  vi.spyOn(fs, "rmSync").mockImplementation((target, options) => { if (!intercept("rmSync", target)) real.rmSync(target, options); });
  vi.spyOn(fs, "rmdirSync").mockImplementation((target) => { if (!intercept("rmdirSync", target)) real.rmdirSync(target); });
  vi.spyOn(fs.promises, "unlink").mockImplementation(async (target) => { if (!intercept("promises.unlink", target)) await real.unlink(target); });
  vi.spyOn(fs.promises, "rm").mockImplementation(async (target, options) => { if (!intercept("promises.rm", target)) await real.rm(target, options); });
  vi.spyOn(fs.promises, "rmdir").mockImplementation(async (target) => { if (!intercept("promises.rmdir", target)) await real.rmdir(target); });
  return calls;
};

/** Each recorded removal: a path in this root, through a held directory fd, under both fence locks, at most once per path. */
const fencedRemovals = (calls: Removal[]): void => {
  for (const call of calls) expect(call).toMatchObject({ viaDirFd: true, dirMatches: true, meshLock: true, custodyLock: true });
  expect(new Set(calls.map(call => call.rel)).size).toBe(calls.length);
};

/**
 * No recorded removal of `id` except a quarantine alias removed after a successful restore: the original basename
 * already names that same inode at the call (and it has another link). Removing the original basename is never safe.
 */
const onlyRestoredAliasRemoved = (calls: Removal[], id: string | undefined): void => {
  expect(id).toBeDefined();
  expect(calls.filter(call => call.target === id
    && !(call.quarantine && call.rel !== "state.db" && call.original === id && call.links >= 2))).toEqual([]);
};

/** Every path under `root` (real fs, recursive) whose lstat identity is `id`, relative to the root. */
const whereIs = (root: string, id: string | undefined): string[] => {
  const out: string[] = [];
  const walk = (rel: string): void => {
    for (const entry of fs.readdirSync(path.join(root, rel))) {
      const child = rel ? `${rel}/${entry}` : entry;
      const stat = fs.lstatSync(path.join(root, child));
      if (idOf(stat) === id) out.push(child);
      if (stat.isDirectory()) walk(child);
    }
  };
  walk("");
  return out;
};

/** The reported quarantine entries: root-relative `<slot>/<name>` paths (an entry, never a bare directory). */
const quarantinePaths = (result: Awaited<ReturnType<typeof run>>): string[] => {
  const q = (result.json.retained ?? []).filter(kept => kept.startsWith(".import-quarantine"));
  for (const kept of q) expect(kept).toMatch(/^\.import-quarantine-[^/]+\/state\.db(?:-wal|-shm)?-[a-f0-9]{32}$/);
  return q;
};
/** Retained entries are warned about (both channels), each quarantine path by name, with manual-cleanup guidance. */
const operatorGuidance = (result: Awaited<ReturnType<typeof run>>, q: string[]): void => {
  const warnings = result.json.cleanupWarnings ?? [];
  expect(warnings.length).toBeGreaterThan(0);
  for (const kept of q) expect(warnings.some(text => text.includes(kept)), kept).toBe(true);
  expect(warnings.some(text => /manual/i.test(text))).toBe(true);
  for (const text of warnings) expect(result.err).toContain(text);
};

type RaceMode = "swap" | "swap-link-eexist" | "gone" | "gone-after-check" | "swap-in-quarantine" | "occupy";
/**
 * The same-UID race on the original basename, armed at the refusal (importing commit) and fired once at the first
 * check or move of `<root>/state.db` (by path or through a held root fd) while it still names this run's inode:
 * "swap": exactly AFTER an lstat returns the owned inode (or BEFORE a rename of it), the owned file is renamed to
 * moved-away.db and a foreign file is written as state.db. "swap-link-eexist": the same, and after the foreign inode is
 * renamed into a quarantine path a third file takes state.db (a link-back must fail EEXIST). "gone": BEFORE that
 * check or move the owned file is renamed to gone-elsewhere.db ("gone-after-check": after the checks, BEFORE the
 * rename). "swap"/"swap-link-eexist" fire BEFORE the rename, after the preflight and per-move checks. "swap-in-quarantine": exactly AFTER an lstat of the
 * MOVED entry `<slot>/state.db` returns the owned inode (and so BEFORE any unlink of it), the owned file is renamed to
 * moved-away.db and a foreign file is written at that quarantine path. "occupy" (for a sidecar `name`): BEFORE the
 * first check of `<root>/<name>`, a foreign file takes that name (an existing owned one is renamed to moved-away.db).
 * Real rename/write in this test's own mkdtemp root.
 */
const raceCleanup = (root: string, mode: RaceMode, name = "state.db") => {
  const realLstat = fs.lstatSync;
  const realRename = fs.renameSync;
  const db = path.join(root, name);
  const rootId = idOf(fs.statSync(root));
  const realRoot = fs.realpathSync(root);
  const race = { armed: false, fired: false, owned: undefined as string | undefined, foreign: undefined as string | undefined,
    third: undefined as string | undefined };
  const isRootDb = (target: fs.PathLike): boolean => {
    const text = String(target);
    const pinned = /^(?:\/proc\/self\/fd|\/dev\/fd)\/(\d+)\/([^/]+)$/.exec(text);
    try {
      if (pinned) return pinned[2] === name && idOf(fs.fstatSync(Number(pinned[1]))) === rootId;
      return path.basename(text) === name && fs.realpathSync(path.dirname(path.resolve(text))) === realRoot;
    } catch { return false; }
  };
  /** `<root>/.import-quarantine-*\/state.db`, by path or through the held root fd. */
  const isQuarantineDb = (target: fs.PathLike): boolean => {
    const text = String(target);
    const pinned = /^(?:\/proc\/self\/fd|\/dev\/fd)\/(\d+)\/\.import-quarantine-[^/]+\/state\.db(?:-[a-f0-9]{32})?$/.exec(text);
    try {
      if (pinned) return idOf(fs.fstatSync(Number(pinned[1]))) === rootId;
      return /^\.import-quarantine-[^/]+$/.test(path.basename(path.dirname(text))) && /^state\.db(?:-[a-f0-9]{32})?$/.test(path.basename(text))
        && fs.realpathSync(path.dirname(path.dirname(path.resolve(text)))) === realRoot;
    } catch { return false; }
  };
  const fire = (at = db): void => {
    race.fired = true;
    if (mode === "gone" || mode === "gone-after-check") { realRename(db, path.join(root, "gone-elsewhere.db")); return; }
    if (mode !== "occupy" || realLstat(at, { throwIfNoEntry: false })) realRename(at, path.join(root, "moved-away.db"));
    fs.writeFileSync(at, "foreign", { mode: 0o600, flag: "wx" });
    race.foreign = idOf(realLstat(at));
  };
  const live = (): boolean => race.armed && !race.fired && idOf(realLstat(db, { throwIfNoEntry: false })) === race.owned;
  const rootRace = mode !== "swap-in-quarantine" && mode !== "occupy";
  vi.spyOn(fs, "lstatSync").mockImplementation(((target: fs.PathLike, options?: fs.StatSyncOptions) => {
    if (mode === "occupy" && race.armed && !race.fired && isRootDb(target)) fire();
    if (mode === "gone" && isRootDb(target) && live()) fire();
    const result = realLstat(target, options);
    // The root races (swap, swap-link-eexist, gone-after-check) fire in renameSpy, just BEFORE the rename: the cleanup's
    // preflight lstat now precedes every move, so firing after the first owned lstat would hit the preflight instead
    // of the check-to-move window these cases target.
    if (mode === "swap-in-quarantine" && race.armed && !race.fired && isQuarantineDb(target)
      && idOf(result as fs.Stats | undefined) === race.owned) fire(String(target));
    return result;
  }) as typeof fs.lstatSync);
  vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
    if (rootRace && isRootDb(from) && live()) fire();
    realRename(from, to);
    if (mode === "swap-link-eexist" && race.foreign !== undefined && race.third === undefined && !isRootDb(to)
      && idOf(realLstat(to, { throwIfNoEntry: false })) === race.foreign) {
      fs.writeFileSync(db, "third", { mode: 0o600, flag: "wx" });
      race.third = idOf(realLstat(db));
    }
  });
  const arm = (commit: Commit): void => {
    if (commit.phase !== "importing") return;
    race.owned = idOf(realLstat(db, { throwIfNoEntry: false }));
    race.armed = true;
  };
  return { race, arm, protect: () => new Set([race.owned, race.foreign, race.third]) };
};

// ponytail: Linux only (held-directory-fd paths under /proc/self/fd, the gate's /proc scan). The readiness proofs
// fail closed off POSIX (mesh-backend-readiness.test.ts), so Windows has no refusal-under-the-fence path to clean.
describe.skipIf(process.platform !== "linux")("recorded cleanup", () => {
  it.each(["import", "cutover"] as const)("%s: a refusal before the importing commit quarantines this run's state.db* and removes nothing; state.json is unchanged", async (command) => {
    const root = await fileRoot();
    await proveSqlite(root);
    const before = fs.readFileSync(path.join(root, "state.json"));
    const calls = recordRemovals(root);
    const result = await run(root, command, { refuseAt: "importing" });
    refusedUnderFence(result, "importing");
    expect(fs.readFileSync(path.join(root, "state.json")).equals(before)).toBe(true);
    expect(calls).toEqual([]); // no unlink/rm/rmdir of anything, ever
    expect(dbNames(root)).toEqual([]); // the root names are free
    const q = quarantinePaths(result);
    const dbAt = q.find(kept => /^state\.db-[a-f0-9]{32}$/.test(path.basename(kept)));
    expect(dbAt).toBeDefined();
    expect(whereIs(root, result.opened[0])).toEqual([dbAt]);
    for (const kept of q) expect(fs.lstatSync(path.join(root, kept)).isFile(), kept).toBe(true);
    operatorGuidance(result, q);
    console.info("QUARANTINE_SEQUENCE", JSON.stringify({ case: `normal-${command}`, calls, report: result.json }));
  });

  it("keeps a state.db replaced by another inode, and warns on stderr and in --json", async () => {
    const root = await fileRoot();
    await proveSqlite(root);
    let foreign: string | undefined;
    const calls = recordRemovals(root);
    const result = await run(root, "import", { refuseAt: "importing", beforeCommit: (commit) => {
      if (commit.phase !== "importing") return;
      fs.renameSync(path.join(root, "state.db"), path.join(root, "moved-away.db"));
      fs.writeFileSync(path.join(root, "state.db"), "foreign", { mode: 0o600 });
      foreign = idOf(fs.lstatSync(path.join(root, "state.db")));
    } });
    refusedUnderFence(result, "importing");
    expect(foreign).toBeDefined();
    expect(calls).toEqual([]);
    expect(fs.readFileSync(path.join(root, "state.db"), "utf8")).toBe("foreign");
    expect(result.json.cleanupWarnings?.some(text => text.includes("state.db"))).toBe(true);
    for (const text of result.json.cleanupWarnings ?? []) expect(result.err).toContain(text);
  });

  it("a failed move into quarantine keeps the refusal (exit 3), keeps state.db at its name, removes nothing, and warns", async () => {
    const root = await fileRoot();
    await proveSqlite(root);
    const calls = recordRemovals(root);
    const realRename = fs.renameSync;
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (/^\/proc\/self\/fd\/\d+\/state\.db$/.test(String(from)) && String(to).includes(".import-quarantine")) {
        throw Object.assign(new Error(`EACCES: permission denied, rename '${String(from)}'`), { code: "EACCES" });
      }
      realRename(from, to);
    });
    const result = await run(root, "cutover", { refuseAt: "importing" });
    refusedUnderFence(result, "importing");
    expect(calls).toEqual([]);
    expect(idOf(fs.lstatSync(path.join(root, "state.db")))).toBe(result.opened[0]);
    expect(result.json.retained).toContain("state.db");
    const warnings = result.json.cleanupWarnings ?? [];
    expect(warnings.some(text => text.includes("state.db") && text.includes("EACCES"))).toBe(true);
    for (const text of warnings) expect(result.err).toContain(text);
  });

  it.each(["import-read", "import-commit"] as const)("an exit-1 error at %s keeps the database; a refused rerun keeps it too; the clean rerun converges", async (step) => {
    const root = await fileRoot();
    await proveSqlite(root);
    const calls = recordRemovals(root);
    const crashed = await run(root, "import", { onStep: (at) => { if (at === step) throw new Error("killed"); } });
    expect(crashed.code, crashed.err).toBe(1);
    expect(calls).toEqual([]);
    const kept = idOf(fs.lstatSync(path.join(root, "state.db")));
    expect((await meshBackendStatus(root)).backend).toBe(step === "import-read" ? "file" : "importing");

    // The database is not this run's: a refusal leaves it in place (a sidecar it created may go, nothing older).
    const existing = new Set(dbNames(root).map(name => idOf(fs.lstatSync(path.join(root, name)))));
    const refused = await run(root, "import", { refuseAt: "importing" });
    refusedUnderFence(refused, "importing");
    fencedRemovals(calls);
    expect(calls.filter(call => call.name === "state.db" || existing.has(call.target))).toEqual([]);
    expect(idOf(fs.lstatSync(path.join(root, "state.db")))).toBe(kept);

    const done = await run(root, "import");
    expect(done.code, done.err).toBe(0);
    expect((await meshBackendStatus(root)).backend).toBe("sqlite");
  });

  // fabric-store policy for #9501 (macOS: smarty-dev#11066; Windows: smarty-dev#7858): only a working /proc/self/fd
  // traversal of the held root fd permits removal, on every platform; there is no /dev/fd fallback and no platform-name
  // gate. Without it a refusal deletes nothing and reports what it kept (reason no-held-dir-fd); imports still succeed.
  // Modelled here by making /proc/self/fd/* unavailable to fs.statSync (the capability probe).
  it("no /proc/self/fd capability: a refusal deletes nothing and reports retained basenames (no /dev/fd fallback); a successful import continues", async () => {
    const root = await fileRoot();
    await proveSqlite(root);
    const other = await fileRoot();
    await proveSqlite(other);
    const calls = recordRemovals(root);
    const probes: string[] = [];
    const devFd: string[] = [];
    const realStat = fs.statSync;
    const watch = (target: fs.PathLike | number): void => { if (typeof target !== "number" && String(target).startsWith("/dev/fd")) devFd.push(String(target)); };
    vi.spyOn(fs, "statSync").mockImplementation(((target: fs.PathLike, options?: fs.StatSyncOptions) => {
      watch(target);
      if (String(target).startsWith("/proc/self/fd")) {
        probes.push(String(target));
        if (options?.throwIfNoEntry === false) return undefined;
        throw Object.assign(new Error(`ENOENT: no such file or directory, stat '${String(target)}'`), { code: "ENOENT" });
      }
      return realStat(target, options);
    }) as typeof fs.statSync);
    for (const api of ["lstatSync", "openSync"] as const) {
      const real = fs[api] as (...args: unknown[]) => unknown;
      vi.spyOn(fs, api).mockImplementation(((...args: unknown[]) => { watch(args[0] as fs.PathLike); return real(...args); }) as never);
    }
    const refused = await run(root, "import", { refuseAt: "importing" });
    const done = await run(other, "import");
    refusedUnderFence(refused, "importing");
    expect(probes.length).toBeGreaterThan(0);
    expect(devFd).toEqual([]);
    expect(calls).toEqual([]);
    expect(dbNames(root)).toContain("state.db");
    expect([...refused.json.retained ?? []].sort()).toEqual(dbNames(root));
    expect(refused.json.reason).toBe("no-held-dir-fd");
    expect(refused.err).toContain("no-held-dir-fd");
    for (const name of dbNames(root)) expect(refused.err).toContain(name);
    expect(done.code, done.err).toBe(0);
    expect((await meshBackendStatus(other)).backend).toBe("sqlite");
  });

  // Finding 2: a close that throws fails closed. Nothing is unlinked (the connection may still be open), the files
  // actually present are reported through the held root with a stable reason, and the refusal (exit 3) is unchanged.
  it("a failed database close keeps every state.db file, unlinks nothing, and reports retained basenames (close-failed)", async () => {
    const root = await fileRoot();
    await proveSqlite(root);
    const calls = recordRemovals(root);
    const result = await run(root, "cutover", { refuseAt: "importing", failCloseAfterRefusal: true });
    refusedUnderFence(result, "importing");
    expect(calls).toEqual([]);
    expect(result.atExit).toContain("state.db");
    expect(dbNames(root)).toContain("state.db");
    expect([...result.json.retained ?? []].sort()).toEqual(result.atExit);
    expect(result.json.reason).toBe("close-failed");
    const warnings = result.json.cleanupWarnings ?? [];
    expect(warnings.some(text => text.includes("EIO: injected close failure") && text.includes(root) && text.includes("close-failed"))).toBe(true);
    for (const text of warnings) expect(result.err).toContain(text);
    for (const name of result.atExit) expect(result.err).toContain(name);
    expect(result.err).toContain("cutover refused");
  });

  it("a cleanup check that fails keeps the database and reports retained basenames (cleanup-failed)", async () => {
    const root = await fileRoot();
    await proveSqlite(root);
    const calls = recordRemovals(root);
    const realLstat = fs.lstatSync;
    vi.spyOn(fs, "lstatSync").mockImplementation(((target: fs.PathLike, options?: fs.StatSyncOptions) => {
      if (/^\/proc\/self\/fd\/\d+\/state\.db-wal$/.test(String(target))) {
        throw Object.assign(new Error(`EIO: i/o error, lstat '${String(target)}'`), { code: "EIO" });
      }
      return realLstat(target, options);
    }) as typeof fs.lstatSync);
    const result = await run(root, "import", { refuseAt: "importing" });
    refusedUnderFence(result, "importing");
    fencedRemovals(calls);
    expect(calls.filter(call => call.name === "state.db" || call.name === "state.db-wal")).toEqual([]);
    expect(result.json.retained).toEqual(expect.arrayContaining(["state.db-wal", "state.db"]));
    expect(result.json.reason).toBe("cleanup-failed");
    const warnings = result.json.cleanupWarnings ?? [];
    expect(warnings.some(text => text.includes("EIO") && text.includes(root))).toBe(true);
    for (const text of warnings) expect(result.err).toContain(text);
  });

  it("an RNG failure for the quarantine entry name keeps the originals (cleanup-failed) and leaves no unreported quarantine directory", async () => {
    const root = await fileRoot();
    await proveSqlite(root);
    const calls = recordRemovals(root);
    const result = await run(root, "import", { refuseAt: "importing", beforeCommit: (commit) => { if (commit.phase === "importing") rng.fail = true; } });
    rng.fail = false;
    refusedUnderFence(result, "importing");
    expect(rng.hits).toBeGreaterThan(0);
    expect(calls).toEqual([]);
    expect(idOf(fs.lstatSync(path.join(root, "state.db")))).toBe(result.opened[0]);
    const onDisk = fs.readdirSync(root).filter(name => name.startsWith(".import-quarantine"));
    const reported = (result.json.retained ?? []).filter(kept => kept.startsWith(".import-quarantine"));
    expect(onDisk.filter(dir => !reported.some(kept => kept === dir || kept.startsWith(`${dir}/`))), "unreported quarantine dirs").toEqual([]);
    expect(onDisk).toEqual([]);
    expect([...result.json.retained ?? []].sort()).toEqual(dbNames(root));
    expect(result.json.reason).toBe("cleanup-failed");
    const warnings = result.json.cleanupWarnings ?? [];
    expect(warnings.some(text => text.includes("injected RNG failure") && text.includes(root))).toBe(true);
    for (const text of warnings) expect(result.err).toContain(text);
  });

  // A writer that ignored .lock rewrites state.json at import-read: the fence error (exit 3) precedes the importing
  // commit. A fresh root's state.db is this run's (removed under the fence); one left by an earlier exit-1 run is not.
  const rogueAt = (root: string) => (step: string): void => {
    if (step !== "import-read") return;
    const file = decodeMeshStateFile(path.join(root, "state.json"), 64 * 1024 * 1024, false) as MeshStateFile & { highWater: number };
    const version = file.highWater + 1;
    fs.writeFileSync(path.join(root, "state.json"), encodeMeshStateFile({ ...file, highWater: version,
      entries: { ...file.entries, "rogue/key": { key: "rogue/key", value: 1, version, updatedAt: Date.now(), updatedBy: identity } } } as never).serialized);
  };
  const rogueKept = (root: string): void => {
    const file = decodeMeshStateFile(path.join(root, "state.json"), 64 * 1024 * 1024, false) as MeshStateFile;
    expect(file.entries["rogue/key"]?.value).toBe(1);
  };

  it("a state.json moved at import-read (fresh root): quarantines only this run's state.db*, removes nothing", async () => {
    const root = await fileRoot();
    await proveSqlite(root);
    const calls = recordRemovals(root);
    const result = await run(root, "cutover", { onStep: rogueAt(root) });
    expect(result.code, result.err).toBe(3);
    expect(result.json.error).toMatch(/changed before the flag commit/);
    rogueKept(root);
    expect(calls).toEqual([]);
    expect(dbNames(root)).toEqual([]);
    const q = quarantinePaths(result);
    expect(whereIs(root, result.opened[0])).toEqual(q.filter(kept => /^state\.db-[a-f0-9]{32}$/.test(path.basename(kept))));
    expect(whereIs(root, result.opened[0])).toHaveLength(1);
    operatorGuidance(result, q);
  });

  it("refusals over a database an exit-1 run left keep its inode, bytes and meta (backend=file, epoch 0)", async () => {
    const root = await fileRoot();
    await proveSqlite(root);
    const calls = recordRemovals(root);
    const crashed = await run(root, "cutover", { onStep: (at) => { if (at === "import-read") throw new Error("killed"); } });
    expect(crashed.code, crashed.err).toBe(1);
    expect(calls).toEqual([]);
    const kept = idOf(fs.lstatSync(path.join(root, "state.db")));
    const existing = new Set(dbNames(root).map(name => idOf(fs.lstatSync(path.join(root, name)))));
    const sha = (): string => createHash("sha256").update(fs.readFileSync(path.join(root, "state.db"))).digest("hex");
    const bytesBefore = sha();
    const statusBefore = await meshBackendStatus(root);
    for (const attempt of ["file-move", "readiness"] as const) {
      const result = await run(root, "cutover", attempt === "file-move" ? { onStep: rogueAt(root) } : { refuseAt: "importing" });
      expect(result.code, `${attempt}: ${result.err}`).toBe(3);
      if (attempt === "file-move") expect(result.json.error).toMatch(/changed before the flag commit/);
      else refusedUnderFence(result, "importing");
    }
    rogueKept(root);
    fencedRemovals(calls);
    expect(calls.filter(call => call.name === "state.db" || existing.has(call.target))).toEqual([]);
    expect(idOf(fs.lstatSync(path.join(root, "state.db")))).toBe(kept);
    expect(sha()).toBe(bytesBefore);
    const statusAfter = await meshBackendStatus(root);
    expect(statusAfter).toMatchObject({ backend: "file", epoch: 0 });
    expect({ commit: statusAfter.commit, dbDigest: statusAfter.dbDigest }).toEqual({ commit: statusBefore.commit, dbDigest: statusBefore.dbDigest });
  });

  // #9501 security finding: a same-UID writer swaps the original basename after its lstat showed this run's inode and
  // before the removal (or the move into quarantine). The foreign inode must keep a name, with its bytes.
  it("a state.db swapped after the owned-inode check, before its move: the foreign file keeps its name and its quarantine alias", async () => {
    const root = await fileRoot();
    await proveSqlite(root);
    const { race, arm, protect } = raceCleanup(root, "swap");
    const calls = recordRemovals(root, undefined, protect);
    const result = await run(root, "import", { refuseAt: "importing", beforeCommit: arm });
    refusedUnderFence(result, "importing");
    expect(race.fired).toBe(true);
    onlyRestoredAliasRemoved(calls, race.foreign); // the dangerous call, named first
    expect(calls).toEqual([]); // no alias drop either
    console.info("QUARANTINE_SEQUENCE", JSON.stringify({ case: "mismatch-restored", race, calls, report: result.json }));
    expect(fs.readFileSync(path.join(root, "state.db"), "utf8")).toBe("foreign");
    expect(whereIs(root, race.owned)).toEqual(["moved-away.db"]);
    const q = quarantinePaths(result);
    const aliases = whereIs(root, race.foreign).filter(at => at !== "state.db");
    expect(whereIs(root, race.foreign)).toContain("state.db");
    expect(aliases).toHaveLength(1);
    expect(q).toContain(aliases[0]);
    expect(result.json.retained).toContain("state.db");
    operatorGuidance(result, q);
    expect((result.json.cleanupWarnings ?? []).some(text => text.includes(`${root}/state.db`))).toBe(true);
  });

  // The inside-quarantine window: the MOVED entry's lstat returned this run's inode, then (before any unlink of it) a
  // same-UID writer renames it away and writes a foreign file at the quarantine path. Unlinking there would drop the
  // foreign inode's last link: the new helper never unlinks, so the foreign file stays, reported by its path.
  it("a quarantine entry swapped after its owned-inode lstat, before any unlink: the foreign file keeps its quarantine path", async () => {
    const root = await fileRoot();
    await proveSqlite(root);
    const { race, arm, protect } = raceCleanup(root, "swap-in-quarantine");
    const calls = recordRemovals(root, undefined, protect);
    const result = await run(root, "import", { refuseAt: "importing", beforeCommit: arm });
    refusedUnderFence(result, "importing");
    console.info("QUARANTINE_SEQUENCE", JSON.stringify({ case: "swap-in-quarantine", race, calls, report: result.json }));
    expect(race.fired).toBe(true);
    expect(calls.filter(call => call.target === race.foreign)).toEqual([]); // the foreign last-link removal
    expect(calls).toEqual([]);
    expect(whereIs(root, race.owned)).toEqual(["moved-away.db"]);
    const kept = whereIs(root, race.foreign);
    expect(kept).toHaveLength(1);
    expect(kept[0]).toMatch(/^\.import-quarantine-[^/]+\/state\.db-[a-f0-9]{32}$/);
    expect(fs.readFileSync(path.join(root, kept[0]!), "utf8")).toBe("foreign");
    expect(dbNames(root)).toEqual([]);
    const q = quarantinePaths(result);
    expect(q).toContain(kept[0]);
    operatorGuidance(result, q);
  });

  // ROOT sidecar late replacement: a foreign -wal takes the sidecar name before its check. The sidecar name stays
  // occupied by the foreign file, so the main database stays at its original name (never quarantined without it).
  it("a foreign state.db-wal at the sidecar name keeps the main state.db at its original name; nothing is removed", async () => {
    const root = await fileRoot();
    await proveSqlite(root);
    const { race, arm, protect } = raceCleanup(root, "occupy", "state.db-wal");
    let owned: string | undefined;
    const calls = recordRemovals(root, undefined, () => new Set([...protect(), owned]));
    const result = await run(root, "cutover", { refuseAt: "importing", beforeCommit: (commit) => {
      arm(commit);
      if (commit.phase === "importing") owned = idOf(fs.lstatSync(path.join(root, "state.db")));
    } });
    refusedUnderFence(result, "importing");
    expect(race.fired).toBe(true);
    // SQLite unlinked the old WAL on close. Its creation fd must keep that inode alive, so the foreign WAL
    // cannot reuse its dev/ino and be mistaken for this run's file (smarty-dev#11604).
    expect(race.owned).toBeDefined();
    expect(race.foreign).toBeDefined();
    expect(race.foreign).not.toBe(race.owned);
    expect(calls).toEqual([]);
    expect(idOf(fs.lstatSync(path.join(root, "state.db")))).toBe(owned);
    expect(idOf(fs.lstatSync(path.join(root, "state.db-wal")))).toBe(race.foreign);
    expect(fs.readFileSync(path.join(root, "state.db-wal"), "utf8")).toBe("foreign");
    expect(result.json.retained).toEqual(expect.arrayContaining(["state.db-wal", "state.db"]));
    const warnings = result.json.cleanupWarnings ?? [];
    expect(warnings.length).toBeGreaterThan(0);
    for (const text of warnings) expect(result.err).toContain(text);
  });

  it("a swapped state.db whose link-back meets EEXIST stays in quarantine, reported by its relative path", async () => {
    const root = await fileRoot();
    await proveSqlite(root);
    const { race, arm, protect } = raceCleanup(root, "swap-link-eexist");
    const calls = recordRemovals(root, undefined, protect);
    const result = await run(root, "cutover", { refuseAt: "importing", beforeCommit: arm });
    refusedUnderFence(result, "importing");
    expect(race.fired).toBe(true);
    // Link-back failed: no removal of the foreign inode at all, nor of the retaken original or the moved owned file.
    expect(calls.filter(call => call.target === race.foreign || call.target === race.owned || call.target === race.third)).toEqual([]);
    expect(calls).toEqual([]);
    expect(race.third).toBeDefined();
    expect(fs.readFileSync(path.join(root, "state.db"), "utf8")).toBe("third");
    const kept = whereIs(root, race.foreign);
    expect(kept).toHaveLength(1);
    expect(kept[0]).toMatch(/^[^/]+\/state\.db-[a-f0-9]{32}$/);
    expect(fs.readFileSync(path.join(root, kept[0]!), "utf8")).toBe("foreign");
    expect(result.json.retained).toContain(kept[0]);
    console.info("QUARANTINE_SEQUENCE", JSON.stringify({ case: "link-back-eexist", race, calls, report: result.json }));
    operatorGuidance(result, quarantinePaths(result));
  });

  it("unchanged owned state.db: no removal; this run's inode is retained at its reported quarantine path", async () => {
    const root = await fileRoot();
    await proveSqlite(root);
    let owned: string | undefined;
    const calls = recordRemovals(root, undefined, () => new Set([owned]));
    const result = await run(root, "import", { refuseAt: "importing", beforeCommit: (commit) => {
      if (commit.phase === "importing") owned = idOf(fs.lstatSync(path.join(root, "state.db")));
    } });
    refusedUnderFence(result, "importing");
    expect(calls).toEqual([]);
    expect(owned).toBe(result.opened[0]);
    const q = quarantinePaths(result);
    const at = whereIs(root, owned);
    expect(at).toHaveLength(1);
    expect(q).toContain(at[0]);
    expect(result.json.retained ?? []).not.toContain("state.db");
    operatorGuidance(result, q);
  });

  it("a normal rerun after a quarantining refusal converges to sqlite; the quarantine stays, nothing is removed", async () => {
    const root = await fileRoot();
    await proveSqlite(root);
    const calls = recordRemovals(root); // installed BEFORE both runs
    const refused = await run(root, "import", { refuseAt: "importing" });
    refusedUnderFence(refused, "importing");
    const q = quarantinePaths(refused);
    expect(q.length).toBeGreaterThan(0);
    const held = new Map(q.map(kept => [kept, idOf(fs.lstatSync(path.join(root, kept)))]));
    const done = await run(root, "import");
    expect(done.code, done.err).toBe(0);
    expect((await meshBackendStatus(root)).backend).toBe("sqlite");
    expect(calls).toEqual([]);
    for (const [kept, id] of held) expect(idOf(fs.lstatSync(path.join(root, kept))), kept).toBe(id);
    expect(whereIs(root, refused.opened[0])).toEqual(q.filter(kept => /^state\.db-[a-f0-9]{32}$/.test(path.basename(kept))));
  });

  it.each(["gone", "gone-after-check"] as const)("an owned state.db disappears (%s): no state.db removal, the moved file survives", async (mode) => {
    const root = await fileRoot();
    await proveSqlite(root);
    const { race, arm, protect } = raceCleanup(root, mode);
    const calls = recordRemovals(root, undefined, protect);
    const result = await run(root, "import", { refuseAt: "importing", beforeCommit: arm });
    refusedUnderFence(result, "importing");
    expect(race.fired).toBe(true);
    expect(calls.filter(call => call.name === "state.db" || call.target === race.owned)).toEqual([]);
    expect(calls).toEqual([]);
    expect(whereIs(root, race.owned)).toEqual(["gone-elsewhere.db"]);
    expect(result.json.retained ?? []).not.toContain("state.db");
  });

  it("a refusal before the final sqlite commit keeps the committed importing database", async () => {
    const root = await fileRoot();
    await proveSqlite(root);
    const calls = recordRemovals(root);
    const result = await run(root, "cutover", { refuseAt: "sqlite" });
    refusedUnderFence(result, "sqlite");
    expect(calls).toEqual([]);
    expect((await meshBackendStatus(root)).backend).toBe("importing");
  });

  // smarty-dev#9501 P2 (review 1029): a WAL setup refusal inside FenceDb.open (the `PRAGMA journal_mode = WAL` check,
  // backend-migration.ts FenceDb.open) runs BEFORE owned.createSidecars and before cutoverWithFence's refusal cleanup,
  // so a state.db that owned.create made for this run is left at its root name. These call importMeshState and
  // cutoverMeshState directly with a pure fake SqliteOpener (no SQLite at all: a plain read descriptor on the checked
  // inode satisfies the gate's fd binding, exec/prepare are scripted). The removal recorder is installed BEFORE every
  // product run. "Gone" means the root names are free and the created inode is kept at its reported quarantine path:
  // the helper quarantines and never unlinks.
  type WalFakeMode = "sentinel" | "wrong-mode" | "ordinary-error";
  const walFake = (input: { mode: WalFakeMode; onRefuse?: () => void; failClose?: boolean }) => {
    const sentinel = new MeshStateUnsupportedError("injected WAL setup refusal (sentinel)");
    const ordinary = Object.assign(new Error("SQLITE_IOERR: injected ordinary WAL setup I/O error"), { code: "ERR_SQLITE_ERROR" });
    const log: string[] = [];
    const opened: string[] = [];
    const fds = new Set<number>();
    let armed = false;
    let closes = 0;
    const open: SqliteOpener = (file) => {
      opened.push(idOf(fs.lstatSync(file))!);
      const fd = fs.openSync(file, "r"); // the connection's own descriptor on the checked inode, never SQLite
      fds.add(fd);
      const statement = (sql: string): SqliteStatement => ({
        get: () => {
          log.push(`get ${sql}`);
          if (sql !== "PRAGMA journal_mode = WAL") throw new Error(`unexpected get: ${sql}`);
          armed = input.mode !== "ordinary-error";
          input.onRefuse?.();
          if (input.mode === "sentinel") throw sentinel;
          if (input.mode === "ordinary-error") throw ordinary;
          return { journal_mode: "delete" };
        },
        run: () => { throw new Error(`unexpected run: ${sql}`); },
        all: () => { throw new Error(`unexpected all: ${sql}`); },
      });
      return {
        exec: (sql) => {
          log.push(`exec ${sql}`);
          if (!/^PRAGMA (?:busy_timeout|trusted_schema) =/.test(sql)) throw new Error(`unexpected exec: ${sql}`);
        },
        prepare: (sql) => { log.push(`prepare ${sql}`); return statement(sql); },
        get isTransaction() { return false; },
        close: () => {
          closes += 1;
          log.push("close");
          if (input.failClose && armed) { armed = false; throw Object.assign(new Error("EIO: injected close failure"), { code: "EIO" }); }
          if (fds.delete(fd)) fs.closeSync(fd);
        },
      };
    };
    /** The fixture's own release of descriptors a failed close left open (real fs). */
    const release = (): void => { for (const fd of fds) { try { fs.closeSync(fd); } catch { /* closed */ } } fds.clear(); };
    return { open, sentinel, ordinary, log, opened, release, closes: () => closes };
  };
  type WalReport = { cleanupWarnings?: string[]; retained?: string[]; reason?: string };
  const walRun = async (root: string, command: "import" | "cutover", fake: ReturnType<typeof walFake>): Promise<unknown> => {
    try {
      await (command === "import" ? importMeshState : cutoverMeshState)(root, { open: fake.open });
    } catch (error) { return error; }
    finally { fake.release(); }
    throw new Error(`${command} unexpectedly succeeded`);
  };
  const reportOf = (error: unknown): WalReport => error as WalReport;
  const quarantineDirs = (root: string): string[] => fs.readdirSync(root).filter(name => name.startsWith(".import-quarantine"));
  /** The API report (no stderr here): each quarantine entry named in a warning, with manual-cleanup guidance. */
  const walGuidance = (report: WalReport, q: string[]): void => {
    const warnings = report.cleanupWarnings ?? [];
    expect(warnings.length).toBeGreaterThan(0);
    for (const kept of q) expect(warnings.some(text => text.includes(kept)), kept).toBe(true);
    expect(warnings.some(text => /manual/i.test(text))).toBe(true);
  };

  it.each([["sentinel", "import"], ["wrong-mode", "cutover"]] as const)("WAL setup refusal (%s, %s) on a fresh root: the error is rethrown, the root state.db* names are free, the created inode is kept at its reported quarantine path", async (mode, command) => {
    const root = await fileRoot();
    expect(dbNames(root)).toEqual([]);
    const before = fs.readFileSync(path.join(root, "state.json"));
    const fake = walFake({ mode });
    const calls = recordRemovals(root, undefined, () => new Set(fake.opened));
    const error = await walRun(root, command, fake);
    expect(fake.log).toContain("get PRAGMA journal_mode = WAL"); // the WAL setup path, before owned.createSidecars
    if (mode === "sentinel") expect(error).toBe(fake.sentinel);
    else {
      expect(error).toBeInstanceOf(MeshStateUnsupportedError);
      expect((error as Error).message).toMatch(/could not enter WAL mode \(delete\)/);
    }
    expect(calls).toEqual([]);
    expect(fs.readFileSync(path.join(root, "state.json")).equals(before)).toBe(true);
    const created = fake.opened[0];
    expect(created).toBeDefined();
    expect(dbNames(root)).toEqual([]); // state.db, -wal, -shm (and -journal) names are free
    const report = reportOf(error);
    const q = quarantinePaths({ json: report } as never);
    // Only the created database: no sidecar was created (createSidecars runs only after a successful WAL pragma).
    expect(q).toHaveLength(1);
    expect(q[0]).toMatch(/\/state\.db-[a-f0-9]{32}$/);
    expect(whereIs(root, created)).toEqual(q);
    expect(quarantineDirs(root)).toEqual([q[0]!.split("/")[0]]);
    expect(report.reason).toBe("quarantined-after-refusal");
    walGuidance(report, q);
  });

  it("WAL setup refusal over a pre-existing state.db: its exact inode and bytes stay at the name; no quarantine, no report", async () => {
    const root = await fileRoot();
    const db = path.join(root, "state.db");
    fs.writeFileSync(db, "pre-existing bytes", { mode: 0o600, flag: "wx" });
    const kept = idOf(fs.lstatSync(db));
    const fake = walFake({ mode: "sentinel" });
    const calls = recordRemovals(root, undefined, () => new Set([kept]));
    const error = await walRun(root, "import", fake);
    expect(fake.log).toContain("get PRAGMA journal_mode = WAL");
    expect(error).toBe(fake.sentinel);
    expect(calls).toEqual([]);
    expect(idOf(fs.lstatSync(db))).toBe(kept);
    expect(fs.readFileSync(db, "utf8")).toBe("pre-existing bytes");
    expect(whereIs(root, kept)).toEqual(["state.db"]);
    expect(quarantineDirs(root)).toEqual([]);
    expect(reportOf(error).retained ?? []).toEqual([]);
    expect(reportOf(error).cleanupWarnings ?? []).toEqual([]);
  });

  it("WAL setup refusal after state.db was replaced between its creation and the cleanup: the foreign inode and bytes stay, the moved owned file remains, the warning says not this run's file", async () => {
    const root = await fileRoot();
    const db = path.join(root, "state.db");
    let foreign: string | undefined;
    const fake = walFake({ mode: "sentinel", onRefuse: () => {
      fs.renameSync(db, path.join(root, "moved-away.db"));
      fs.writeFileSync(db, "foreign", { mode: 0o600, flag: "wx" });
      foreign = idOf(fs.lstatSync(db));
    } });
    const calls = recordRemovals(root, undefined, () => new Set([...fake.opened, foreign]));
    const error = await walRun(root, "cutover", fake);
    expect(error).toBe(fake.sentinel);
    expect(foreign).toBeDefined();
    expect(calls).toEqual([]);
    expect(whereIs(root, foreign)).toEqual(["state.db"]);
    expect(fs.readFileSync(db, "utf8")).toBe("foreign");
    expect(whereIs(root, fake.opened[0])).toEqual(["moved-away.db"]);
    const report = reportOf(error);
    expect(report.retained ?? []).toContain("state.db");
    expect(report.reason).toBe("not-this-runs-file");
    expect((report.cleanupWarnings ?? []).some(text => text.includes(`${root}/state.db`) && /not the file this run created/.test(text))).toBe(true);
  });

  it("WAL setup refusal with an untracked driver-created state.db-wal: no owned identity is invented for it; its inode and bytes keep the name", async () => {
    const root = await fileRoot();
    const wal = path.join(root, "state.db-wal");
    let driverWal: string | undefined;
    const fake = walFake({ mode: "sentinel", onRefuse: () => {
      fs.writeFileSync(wal, "driver wal", { mode: 0o600, flag: "wx" });
      driverWal = idOf(fs.lstatSync(wal));
    } });
    const calls = recordRemovals(root, undefined, () => new Set([...fake.opened, driverWal]));
    const error = await walRun(root, "import", fake);
    expect(error).toBe(fake.sentinel);
    expect(driverWal).toBeDefined();
    expect(calls).toEqual([]);
    expect(whereIs(root, driverWal)).toEqual(["state.db-wal"]); // never moved into quarantine as "this run's"
    expect(fs.readFileSync(wal, "utf8")).toBe("driver wal");
    expect(whereIs(root, fake.opened[0])).toEqual(["state.db"]); // an unowned sidecar also keeps its main database
    const report = reportOf(error);
    expect((report.retained ?? []).filter(kept => /state\.db-wal-[a-f0-9]{32}$/.test(kept))).toEqual([]);
    expect(report.retained ?? []).toEqual(expect.arrayContaining(["state.db-wal", "state.db"]));
    expect(report.reason).toBe("not-this-runs-file");
    expect((report.cleanupWarnings ?? []).some(text => text.includes(`${root}/state.db-wal`))).toBe(true);
  });

  it("WAL setup refusal whose connection close throws: the refusal is unchanged, the created state.db stays at its name (close-failed), nothing is moved or removed", async () => {
    const root = await fileRoot();
    const fake = walFake({ mode: "sentinel", failClose: true });
    const calls = recordRemovals(root, undefined, () => new Set(fake.opened));
    const error = await walRun(root, "cutover", fake);
    expect(error).toBe(fake.sentinel);
    expect((error as Error).message).toBe("injected WAL setup refusal (sentinel)");
    expect(fake.closes()).toBe(1); // a failed close must not be retried before cleanup or rethrow
    expect(calls).toEqual([]);
    expect(idOf(fs.lstatSync(path.join(root, "state.db")))).toBe(fake.opened[0]);
    expect(quarantineDirs(root)).toEqual([]);
    const report = reportOf(error);
    expect(report.reason).toBe("close-failed");
    expect(report.retained ?? []).toContain("state.db");
    expect((report.cleanupWarnings ?? []).some(text => text.includes("EIO: injected close failure") && text.includes("close-failed"))).toBe(true);
  });

  it("WAL setup refusal control: an ordinary (non-refusal) WAL pragma error keeps the created state.db at its name, with no quarantine and no report", async () => {
    const root = await fileRoot();
    const fake = walFake({ mode: "ordinary-error" });
    const calls = recordRemovals(root, undefined, () => new Set(fake.opened));
    const error = await walRun(root, "import", fake);
    expect(fake.log).toContain("get PRAGMA journal_mode = WAL");
    expect(error).toBe(fake.ordinary);
    expect(calls).toEqual([]);
    expect(idOf(fs.lstatSync(path.join(root, "state.db")))).toBe(fake.opened[0]);
    expect(quarantineDirs(root)).toEqual([]);
    expect(reportOf(error).retained ?? []).toEqual([]);
    expect(reportOf(error).cleanupWarnings ?? []).toEqual([]);
  });
});

// #4664: the real product with no recorder. Hosted CI only (CI=true), never in a lane. Nothing is deleted: the root
// names are free and the reported quarantine paths hold the created inodes.
describe.runIf(process.env.CI === "true" && process.platform === "linux")("hosted cleanup", () => {
  it.each(["import", "cutover"] as const)("%s: a refusal before the importing commit frees every root state.db* name into the reported quarantine", async (command) => {
    const root = await fileRoot();
    await proveSqlite(root);
    const before = fs.readFileSync(path.join(root, "state.json"));
    const result = await run(root, command, { refuseAt: "importing" });
    refusedUnderFence(result, "importing");
    expect(dbNames(root)).toEqual([]);
    expect(fs.readFileSync(path.join(root, "state.json")).equals(before)).toBe(true);
    expect((await meshBackendStatus(root)).backend).toBe("none");
    const q = quarantinePaths(result);
    expect(whereIs(root, result.opened[0])).toEqual(q.filter(kept => /^state\.db-[a-f0-9]{32}$/.test(path.basename(kept))));
    expect(whereIs(root, result.opened[0])).toHaveLength(1);
    operatorGuidance(result, q);
  });

  it("keeps a state.db replaced by another inode and warns", async () => {
    const root = await fileRoot();
    await proveSqlite(root);
    const result = await run(root, "import", { refuseAt: "importing", beforeCommit: (commit) => {
      if (commit.phase !== "importing") return;
      fs.renameSync(path.join(root, "state.db"), path.join(root, "moved-away.db"));
      fs.writeFileSync(path.join(root, "state.db"), "foreign", { mode: 0o600 });
    } });
    refusedUnderFence(result, "importing");
    expect(fs.readFileSync(path.join(root, "state.db"), "utf8")).toBe("foreign");
    expect(result.json.cleanupWarnings?.some(text => text.includes("state.db"))).toBe(true);
    expect(fs.readFileSync(path.join(root, "moved-away.db"), "utf8")).not.toBe("foreign");
  });

  it("an exit-1 error keeps the database and the rerun converges; a final-phase refusal keeps it", async () => {
    const root = await fileRoot();
    await proveSqlite(root);
    expect((await run(root, "import", { onStep: (at) => { if (at === "import-read") throw new Error("killed"); } })).code).toBe(1);
    expect(dbNames(root)).toContain("state.db");
    refusedUnderFence(await run(root, "cutover", { refuseAt: "sqlite" }), "sqlite");
    expect((await meshBackendStatus(root)).backend).toBe("importing");
    const done = await run(root, "import");
    expect(done.code, done.err).toBe(0);
    expect((await meshBackendStatus(root)).backend).toBe("sqlite");
  });

  // smarty-dev#11466 (r3): the real opener through the gate while this run's O_EXCL ownership pin on state.db is held.
  // Nothing is removed or renamed; the test teardown of the mkdtemp root is hosted only.
  it("the real opener passes the checked-inode gate while the ownership pin is held through close", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "r3-11466-"));
    try {
      const owned = pinImportRoot(root);
      expect(owned.pinned).toBe(true);
      expect(owned.create("state.db")).toBe(true);
      const file = path.join(root, "state.db");
      const pinned = fs.lstatSync(file);
      try {
        const db = openPrivateStateDb(file, openNodeSqlite);
        try {
          db.exec("CREATE TABLE t (v INTEGER)");
          db.prepare("INSERT INTO t (v) VALUES (?)").run(7);
          expect(db.prepare("SELECT v FROM t").get()).toEqual({ v: 7 });
        } finally {
          // The product's rule: a failed database close keeps the created-file pins (close-failed) before owned.close.
          try { db.close(); } catch (error) { owned.keepAfterFailure("close-failed", error); throw error; }
        }
        const after = fs.lstatSync(file);
        expect([after.dev, after.ino], "the pinned inode still names state.db").toEqual([pinned.dev, pinned.ino]);
      } finally { owned.close(); }
      expect(fs.readdirSync(root).filter(name => name.startsWith(".import-quarantine-")), "no cleanup moves").toEqual([]);
    } finally { fs.rmSync(root, { recursive: true, force: true }); }
  });
});

// smarty-dev#11466: a pure in-memory emulation of pinImportRoot's root (no real files, no SQLite, no removal). Every
// file this run creates must stay held by its own open fd through the database close, until owned.close() closes each
// exactly once. Ownership compares the HELD fd's fstat against the name's lstat (dev/ino and size/birthtime): a recycled
// dev/ino with other bytes, a dead held fd or an fstat failure fails closed and keeps every file, logged once.
// Linux only: physical cleanup needs a /proc/self/fd traversal of the held root (src/mesh/owned-import-files.ts:30
// PROC_FD, the pin probe at :83-89). Without it pinImportRoot stays unpinned and keeps every file (:143-153), so this
// held-descriptor and quarantine model proves physical cleanup on Linux only.
describe.runIf(process.platform === "linux")("emulated held-file ownership", () => {
  type Node = { dev: number; ino: number; size: number; birth: number; dir: boolean };
  const ROOT = "/emulated-11466-root";
  const WAL = "state.db-wal";
  const SHM = "state.db-shm";
  const FD0 = 9_000;
  // Unique fake fds across model instances: a retained (sticky failed-close) pin never meets a reused number.
  let fdCursor = FD0;
  const errno = (code: string): Error => Object.assign(new Error(`${code}: emulated`), { code });
  const emulate = (pre: Record<string, Partial<Node>> = {}) => {
    let nextIno = 200;
    let clock = 1_000;
    const firstFd = fdCursor;
    let nextFd = firstFd;
    const node = (over: Partial<Node> = {}): Node => ({ dev: 7, ino: nextIno++, size: 0, birth: clock++, dir: false, ...over });
    const rootNode = node({ dir: true });
    const names = new Map<string, Node>(Object.entries(pre).map(([name, over]) => [name, node(over)]));
    const fds = new Map<number, Node>();
    const closes = new Map<number, number>();
    const m = { names, fds, closes, removals: [] as string[], renames: [] as string[], mkdtemps: 0,
      fstatFail: undefined as ((n: Node) => Error | undefined) | undefined };
    const rel = (target: fs.PathLike): string | undefined => {
      const text = String(target);
      const proc = /^\/proc\/self\/fd\/(\d+)(?:\/(.*))?$/.exec(text);
      if (proc && fds.get(Number(proc[1])) === rootNode) return (proc[2] ?? "").replace(/^\.$/, "");
      if (text === ROOT) return "";
      return text.startsWith(`${ROOT}/`) ? text.slice(ROOT.length + 1) : undefined;
    };
    const at = (name: string): Node | undefined => name === "" ? rootNode : names.get(name);
    const statOf = (n: Node): fs.Stats => ({ dev: n.dev, ino: n.ino, size: n.size, birthtimeMs: n.birth, birthtime: new Date(n.birth),
      uid: process.getuid?.(), mode: n.dir ? 0o40700 : 0o100600, isFile: () => !n.dir, isDirectory: () => n.dir, isSymbolicLink: () => false }) as unknown as fs.Stats;
    // No real-fs fallback anywhere: an unmapped path or descriptor throws. The one path outside ROOT is "/", ROOT's
    // sole ancestor (assertPrivateAncestors), modeled as a private directory of this uid.
    const sysRoot: Node = { dev: 7, ino: 2, size: 0, birth: 0, dir: true };
    // Removal recorders never delegate: any call is recorded and dropped.
    const record = (api: string) => (target: unknown): void => { m.removals.push(`${api}:${String(target)}`); };
    vi.spyOn(fs, "unlinkSync").mockImplementation(record("unlinkSync"));
    vi.spyOn(fs, "rmSync").mockImplementation(record("rmSync"));
    vi.spyOn(fs, "rmdirSync").mockImplementation(record("rmdirSync"));
    vi.spyOn(fs.promises, "unlink").mockImplementation(async (target) => record("promises.unlink")(target));
    vi.spyOn(fs.promises, "rm").mockImplementation(async (target) => record("promises.rm")(target));
    vi.spyOn(fs.promises, "rmdir").mockImplementation(async (target) => record("promises.rmdir")(target));
    vi.spyOn(fs, "openSync").mockImplementation(((target: fs.PathLike, flags?: number | string) => {
      const name = rel(target);
      if (name === undefined) throw errno("EACCES");
      let n = at(name);
      if (n && name !== "" && typeof flags === "number" && (flags & fs.constants.O_EXCL)) throw errno("EEXIST");
      if (!n) {
        // Only a create flag makes a missing name, and only in an existing modeled directory.
        const creates = typeof flags === "number" ? (flags & fs.constants.O_CREAT) !== 0 : /[wa]/.test(String(flags ?? "r"));
        const slash = name.lastIndexOf("/");
        if (!creates || !(slash < 0 ? rootNode : at(name.slice(0, slash)))?.dir) throw errno("ENOENT");
        n = node(); names.set(name, n);
      }
      const fd = nextFd++;
      fdCursor = nextFd;
      fds.set(fd, n);
      return fd;
    }) as typeof fs.openSync);
    vi.spyOn(fs, "fstatSync").mockImplementation(((fd: number) => {
      const n = fds.get(fd);
      if (!n) throw errno("EBADF");
      const failure = n.dir ? undefined : m.fstatFail?.(n);
      if (failure) throw failure;
      return statOf(n);
    }) as typeof fs.fstatSync);
    vi.spyOn(fs, "closeSync").mockImplementation((fd: number) => {
      if (fd < firstFd || fd >= nextFd) throw errno("EBADF"); // never issued by the model: no real descriptor is touched
      closes.set(fd, (closes.get(fd) ?? 0) + 1);
      if (!fds.delete(fd)) throw errno("EBADF");
    });
    const lookup = ((target: fs.PathLike, options?: fs.StatSyncOptions) => {
      if (String(target) === "/") return statOf(sysRoot);
      const name = rel(target);
      if (name === undefined) throw errno("EACCES"); // fails closed even with throwIfNoEntry: false
      const n = at(name);
      if (n) return statOf(n);
      if (options?.throwIfNoEntry === false) return undefined;
      throw errno("ENOENT");
    }) as typeof fs.lstatSync;
    vi.spyOn(fs, "lstatSync").mockImplementation(lookup);
    vi.spyOn(fs, "statSync").mockImplementation(lookup as unknown as typeof fs.statSync);
    vi.spyOn(fs, "realpathSync").mockImplementation(((target: fs.PathLike) => {
      if (String(target) === ROOT) return ROOT;
      throw errno("EACCES");
    }) as typeof fs.realpathSync);
    vi.spyOn(fs, "mkdtempSync").mockImplementation(((prefix: string) => {
      const name = rel(prefix);
      if (name === undefined) throw errno("EACCES");
      const suffix = `q${m.mkdtemps++}`;
      names.set(`${name}${suffix}`, node({ dir: true }));
      return `${prefix}${suffix}`;
    }) as typeof fs.mkdtempSync);
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      const a = rel(from); const b = rel(to);
      const n = a === undefined ? undefined : names.get(a);
      if (b === undefined || !n) throw errno("ENOENT");
      m.renames.push(`${a}->${b}`);
      names.delete(a!); names.set(b, n);
    });
    vi.spyOn(fs, "linkSync").mockImplementation((from, to) => {
      const a = rel(from); const b = rel(to);
      const n = a === undefined ? undefined : names.get(a);
      if (b === undefined || !n) throw errno("ENOENT");
      if (names.has(b)) throw errno("EEXIST");
      names.set(b, n);
    });
    // The Linux gate (state-gate.ts filesystemRefusal, fdSnapshot, openPinned's link check), no real fallback.
    vi.spyOn(fs, "readFileSync").mockImplementation(((target: fs.PathOrFileDescriptor) => {
      if (String(target) === "/proc/self/mountinfo") return "22 1 8:1 / / rw,relatime shared:1 - ext4 /dev/sda1 rw\n";
      throw errno("EACCES");
    }) as typeof fs.readFileSync);
    vi.spyOn(fs, "readdirSync").mockImplementation(((target: fs.PathLike) => {
      if (String(target) === "/proc/self/fd") return [...fds.keys()].map(String);
      throw errno("EACCES");
    }) as typeof fs.readdirSync);
    vi.spyOn(fs, "readlinkSync").mockImplementation(((target: fs.PathLike) => {
      const proc = /^\/proc\/self\/fd\/(\d+)$/.exec(String(target));
      const n = proc ? fds.get(Number(proc[1])) : undefined;
      if (!n) throw errno("EBADF");
      if (n === rootNode) return ROOT;
      const name = [...names.entries()].find(([, v]) => v === n)?.[0];
      if (name === undefined) throw errno("EACCES");
      return `${ROOT}/${name}`;
    }) as typeof fs.readlinkSync);
    return m;
  };
  type Model = ReturnType<typeof emulate>;
  type Owned = ReturnType<typeof pinImportRoot>;
  /** Pin, create state.db and its sidecars, as the importing run does. */
  const created = (m: Model) => {
    const owned = pinImportRoot(ROOT);
    expect(owned.pinned).toBe(true);
    expect(owned.create("state.db")).toBe(true);
    owned.createSidecars();
    return { owned, db: m.names.get("state.db")!, wal: m.names.get(WAL)!, shm: m.names.get(SHM)! };
  };
  /** The emulated DatabaseSync.close(): SQLite drops its sidecar NAMES from the map; open descriptors stay live. */
  const nativeClose = (m: Model): void => { m.names.delete(WAL); m.names.delete(SHM); };
  const heldFiles = (m: Model): Node[] => [...m.fds.values()].filter(n => !n.dir);
  /** owned.close() releases every descriptor, each closed exactly once; nothing was ever removed. */
  const finish = (m: Model, owned: Owned): void => {
    owned.close();
    owned.close();
    expect(m.fds.size, "descriptors left open after owned.close()").toBe(0);
    expect([...m.closes.entries()].filter(([, count]) => count !== 1), "every descriptor closed exactly once").toEqual([]);
    expect(m.removals).toEqual([]);
  };
  const keptAll = (m: Model, report: RefusalCleanupReport, kept: Record<string, Node>, reason: RefusalCleanupReport["reason"]): void => {
    expect(m.renames, "nothing moved").toEqual([]);
    expect(m.mkdtemps, "no quarantine made").toBe(0);
    for (const [name, n] of Object.entries(kept)) {
      expect(m.names.get(name), `${name} kept at its name`).toBe(n);
      expect(report.retained).toContain(name);
    }
    expect(report.reason).toBe(reason);
    expect(report.cleanupWarnings.length).toBeGreaterThan(0);
    expect(new Set(report.cleanupWarnings).size, "logged once").toBe(report.cleanupWarnings.length);
    expect(new Set(report.retained).size, "retained once").toBe(report.retained.length);
  };

  it("holds an open fd for state.db and each sidecar through the native close", () => {
    const m = emulate();
    const { owned, db, wal, shm } = created(m);
    expect(heldFiles(m), "a held fd per created file").toEqual(expect.arrayContaining([db, wal, shm]));
    nativeClose(m);
    expect(heldFiles(m), "held fds outlive the sidecar names").toEqual(expect.arrayContaining([db, wal, shm]));
    owned.removeAfterRefusal();
    expect(heldFiles(m), "cleanup keeps the held fds until owned.close()").toEqual(expect.arrayContaining([db, wal, shm]));
    finish(m, owned);
  });

  it.each([
    ["recycled dev/ino with different size and birthtime", (old: Node): Partial<Node> => ({ size: 7, birth: old.birth + 5_000 })],
    ["recycled dev/ino with the same size but a changed birthtime", (old: Node): Partial<Node> => ({ size: old.size, birth: old.birth + 5_000 })],
    ["a distinct dev/ino", (): Partial<Node> => ({ dev: 8, size: 7 })],
  ] as const)("a foreign WAL after the close (%s) keeps the WAL and state.db at their names", (_, foreignOf) => {
    const m = emulate();
    const { owned, db, wal } = created(m);
    nativeClose(m);
    const foreign: Node = { ...wal, ...foreignOf(wal) };
    m.names.set(WAL, foreign);
    const report = owned.removeAfterRefusal();
    keptAll(m, report, { [WAL]: foreign, "state.db": db }, "not-this-runs-file");
    finish(m, owned);
  });

  it.each([
    ["a dead held fd (EBADF)", "EBADF"],
    ["an unknown fstat failure", "EIO"],
  ] as const)("%s at cleanup keeps every file, moves nothing", (_, code) => {
    const m = emulate();
    const { owned, db, wal, shm } = created(m);
    m.fstatFail = () => errno(code);
    let report: RefusalCleanupReport;
    try { report = owned.removeAfterRefusal(); }
    catch (error) { report = owned.keepAfterFailure("cleanup-failed", error); }
    keptAll(m, report, { [WAL]: wal, [SHM]: shm, "state.db": db }, "cleanup-failed");
    m.fstatFail = undefined;
    finish(m, owned);
  });

  it("this run's untouched files are freed into the reported quarantine, never removed", () => {
    const m = emulate();
    const { owned, db, wal, shm } = created(m);
    const report = owned.removeAfterRefusal();
    expect(report.reason).toBe("quarantined-after-refusal");
    for (const name of [WAL, SHM, "state.db"]) expect(m.names.has(name), `${name} freed`).toBe(false);
    expect(report.retained).toHaveLength(3);
    for (const entry of report.retained) {
      expect(entry).toMatch(/^\.import-quarantine-[^/]+\/state\.db(?:-wal|-shm)?-[a-f0-9]{32}$/);
      expect([db, wal, shm]).toContain(m.names.get(entry));
    }
    finish(m, owned);
  });

  it("after the close unlinked the sidecars, state.db alone is freed into the reported quarantine", () => {
    const m = emulate();
    const { owned, db } = created(m);
    nativeClose(m);
    const report = owned.removeAfterRefusal();
    expect(report.reason).toBe("quarantined-after-refusal");
    expect(report.retained).toHaveLength(1);
    expect(m.names.get(report.retained[0]!)).toBe(db);
    expect(m.names.has("state.db")).toBe(false);
    finish(m, owned);
  });

  it("an existing name is never claimed", () => {
    const m = emulate({ [WAL]: { size: 3 } });
    const existing = m.names.get(WAL)!;
    const owned = pinImportRoot(ROOT);
    expect(owned.create(WAL)).toBe(false);
    expect(owned.createdDb).toBe(false);
    expect(owned.create("state.db")).toBe(true);
    owned.createSidecars();
    const report = owned.removeAfterRefusal();
    expect(m.names.get(WAL)).toBe(existing);
    expect(m.names.get("state.db")).toBeDefined();
    expect(report.retained).toEqual(expect.arrayContaining([WAL, "state.db"]));
    finish(m, owned);
  });

  it("a failed close keeps every created-file pin until process death: owned.close releases only the root, once", () => {
    const m = emulate();
    const { owned, db, wal, shm } = created(m);
    const rootFds = [...m.fds.entries()].filter(([, n]) => n.dir).map(([fd]) => fd);
    expect(rootFds).toHaveLength(1);
    const report = owned.keepAfterFailure("close-failed", new Error("emulated close failure"));
    owned.close();
    owned.close();
    expect(heldFiles(m), "created-file fds stay live").toEqual(expect.arrayContaining([db, wal, shm]));
    expect(heldFiles(m)).toHaveLength(3);
    expect([...m.closes.keys()], "only the root fd was closed").toEqual(rootFds);
    expect(m.closes.get(rootFds[0]!), "root released once").toBe(1);
    expect(m.renames).toEqual([]);
    expect(m.removals).toEqual([]);
    expect(report.reason).toBe("close-failed");
    expect(report.cleanupWarnings).toHaveLength(1);
    const [warning] = report.cleanupWarnings;
    for (const name of ["state.db", WAL, SHM]) expect(warning).toContain(`${ROOT}/${name}`);
    expect(warning).toMatch(/fcntl/);
    expect(warning).toMatch(/process exits/);
  });

  it("the gate's failed close after a refused open rides on the unchanged refusal; close-failed keeps every pin", () => {
    const m = emulate();
    const { owned, db, wal, shm } = created(m);
    const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
    Object.defineProperty(process, "platform", { value: "darwin" }); // the post-open lstat path, not a platform claim
    const gate = (closeThrows: boolean): unknown => {
      const closeError = errno("EIO");
      const open = (() => {
        m.names.set("state.db", { ...db, ino: 99_999 }); // swapped after the precheck
        return { close: () => { if (closeThrows) throw closeError; } } as unknown as SqliteConnection;
      }) as SqliteOpener;
      try { openPrivateStateDb(`${ROOT}/state.db`, open); } catch (error) { return { error, closeError }; }
      throw new Error("the gate did not refuse");
    };
    let swapped: { error: Error & { sqliteCloseError?: unknown }; closeError: Error };
    let clean: { error: Error & { sqliteCloseError?: unknown } };
    try {
      clean = gate(false) as typeof clean;
      m.names.set("state.db", db);
      swapped = gate(true) as typeof swapped;
      m.names.set("state.db", db);
    } finally { Object.defineProperty(process, "platform", platform); }
    expect("sqliteCloseError" in clean.error, "a successful close carries no marker").toBe(false);
    expect(swapped.error).toBeInstanceOf(MeshStateUnsupportedError);
    expect((swapped.error as MeshStateUnsupportedError).code).toBe("FABRIC_MESH_STATE_UNSUPPORTED");
    expect(swapped.error.message).toBe(`Fabric mesh SQLite state refuses ${ROOT}/state.db: it was replaced while opening`);
    expect(swapped.error.sqliteCloseError).toBe(swapped.closeError);
    // The receiver's contract (FenceDb.open's first catch): keepAfterFailure("close-failed", marker), then owned.close().
    const report = owned.keepAfterFailure("close-failed", swapped.error.sqliteCloseError);
    const rootFds = [...m.fds.entries()].filter(([, n]) => n.dir).map(([fd]) => fd);
    owned.close();
    expect(report.reason).toBe("close-failed");
    expect(report.cleanupWarnings[0]).toMatch(/EIO: emulated/);
    expect(heldFiles(m), "created-file pins stay").toEqual(expect.arrayContaining([db, wal, shm]));
    expect([...m.closes.keys()], "only the root fd was closed").toEqual(rootFds);
    expect(m.renames).toEqual([]);
    expect(m.removals).toEqual([]);
  });

  it("a failed cleanup (not a failed close) still releases every created-file descriptor once", () => {
    const m = emulate();
    const { owned } = created(m);
    owned.keepAfterFailure("cleanup-failed", new Error("emulated cleanup failure"));
    finish(m, owned);
  });

  it("a failed fstat of a fresh create closes that descriptor once and claims nothing", () => {
    const m = emulate();
    const owned = pinImportRoot(ROOT);
    m.fstatFail = () => errno("EIO");
    expect(() => owned.create("state.db")).toThrow(/EIO/);
    m.fstatFail = undefined;
    expect(heldFiles(m), "no leaked descriptor").toEqual([]);
    expect(owned.createdDb).toBe(false);
    expect(owned.removeAfterRefusal()).toEqual({ cleanupWarnings: [], retained: [] });
    expect(m.renames).toEqual([]);
    finish(m, owned);
  });

  it("the model fails closed: unmapped paths and descriptors throw, and nothing is created, moved or removed", () => {
    const m = emulate();
    const before = [...m.names.keys()];
    // Unmapped requests must fail without reaching the host filesystem or any real descriptor.
    expect(() => fs.openSync("/tmp", fs.constants.O_RDONLY)).toThrow(/EACCES: emulated/);
    expect(() => fs.openSync(`${ROOT}/absent`, fs.constants.O_RDONLY)).toThrow(/ENOENT: emulated/);
    expect(() => fs.openSync(`${ROOT}/no-dir/state.db`, "wx", 0o600)).toThrow(/ENOENT: emulated/);
    expect(() => fs.fstatSync(0)).toThrow(/EBADF: emulated/);
    expect(() => fs.closeSync(FD0 - 1)).toThrow(/EBADF: emulated/);
    expect(() => fs.lstatSync("/tmp", { throwIfNoEntry: false })).toThrow(/EACCES: emulated/);
    expect(() => fs.statSync("/proc/self/fd/0/.", { throwIfNoEntry: false })).toThrow(/EACCES: emulated/);
    expect(() => fs.realpathSync("/tmp")).toThrow(/EACCES: emulated/);
    expect(fs.lstatSync(`${ROOT}/absent`, { throwIfNoEntry: false })).toBeUndefined();
    expect(() => fs.lstatSync(`${ROOT}/absent`)).toThrow(/ENOENT: emulated/);
    expect([...m.names.keys()], "nothing created").toEqual(before);
    expect(m.fds.size).toBe(0);
    expect(m.closes.size).toBe(0);
    expect(m.renames).toEqual([]);
    expect(m.mkdtemps).toBe(0);
    expect(m.removals).toEqual([]);
  });

  // smarty-dev#11466 r3: the Linux checked-inode gate (/proc/self/fd snapshots) while this run's O_EXCL ownership pin
  // on state.db is held. The pin predates both snapshots and is not a SQLite descriptor: it must never count as parked.
  const fakeDb = (m: Model, opensFile: boolean) => {
    const log = { closes: 0, sql: [] as string[], fd: undefined as number | undefined };
    const open = ((file: string) => {
      if (opensFile) log.fd = fs.openSync(file, "r");
      return {
        exec: (sql: string) => { log.sql.push(sql); },
        prepare: (sql: string) => { log.sql.push(sql); throw new Error("no SQL in the emulation"); },
        close: () => { log.closes += 1; if (log.fd !== undefined) fs.closeSync(log.fd); },
      } as unknown as SqliteConnection;
    }) as SqliteOpener;
    return { open, log, m };
  };
  const pinnedDb = (m: Model) => {
    const owned = pinImportRoot(ROOT);
    expect(owned.pinned).toBe(true);
    expect(owned.create("state.db")).toBe(true);
    return owned;
  };

  it("the Linux gate refuses an opener that never opened the checked file while the ownership pin is held", () => {
    const m = emulate();
    const owned = pinnedDb(m);
    const fake = fakeDb(m, false);
    let accepted: SqliteConnection | undefined;
    let error: unknown;
    try { accepted = openPrivateStateDb(`${ROOT}/state.db`, fake.open); } catch (caught) { error = caught; }
    if (accepted) accepted.close(); // release the product's live count before asserting
    expect(accepted, "the gate accepted a non-opening opener (the ownership pin counted as parked)").toBeUndefined();
    expect(error).toBeInstanceOf(MeshStateUnsupportedError);
    expect((error as Error).message).toBe(`Fabric mesh SQLite state refuses ${ROOT}/state.db: the connection's own descriptor `
      + "on it is not proven (0 new, 0 other, 0 parked here)");
    expect(fake.log.closes, "the fake is closed once").toBe(1);
    expect(fake.log.sql).toEqual([]);
    // The rejection does not poison the live count: a connection opening its own new descriptor then passes.
    const next = fakeDb(m, true);
    const db = openPrivateStateDb(`${ROOT}/state.db`, next.open);
    db.close();
    expect(next.log.closes).toBe(1);
    expect(next.log.sql).toEqual([]);
    expect(m.renames).toEqual([]);
    finish(m, owned);
  });

  it("the Linux gate passes an opener whose own new descriptor names the checked file while the ownership pin is held", () => {
    const m = emulate();
    const owned = pinnedDb(m);
    const fake = fakeDb(m, true);
    const db = openPrivateStateDb(`${ROOT}/state.db`, fake.open);
    expect(m.fds.get(fake.log.fd!)).toBe(m.names.get("state.db"));
    db.close();
    expect(fake.log.closes, "the fake is closed once").toBe(1);
    expect(fake.log.sql).toEqual([]);
    expect(m.renames).toEqual([]);
    expect(m.mkdtemps).toBe(0);
    finish(m, owned);
  });

  it("after a failed close the kept pin is still not gate evidence: a non-opening opener is refused", () => {
    const m = emulate();
    const owned = pinnedDb(m);
    owned.keepAfterFailure("close-failed", new Error("emulated close failure"));
    owned.close();
    expect(heldFiles(m), "the pin stays open").toEqual([m.names.get("state.db")]);
    const fake = fakeDb(m, false);
    expect(() => openPrivateStateDb(`${ROOT}/state.db`, fake.open)).toThrow(/not proven \(0 new, 0 other, 0 parked here\)/);
    expect(fake.log.closes).toBe(1);
  });

  it("after a normal owned.close the freed pin number, reused by a new opener, is seen (unregistered)", () => {
    const m = emulate();
    const owned = pinnedDb(m);
    const [pinFd] = [...m.fds.entries()].find(([, n]) => !n.dir)!;
    owned.close();
    const db = m.names.get("state.db")!;
    const open = (() => { m.fds.set(pinFd, db); // the kernel hands the freed number to SQLite
      return { close: () => { m.fds.delete(pinFd); } } as unknown as SqliteConnection; }) as SqliteOpener;
    openPrivateStateDb(`${ROOT}/state.db`, open).close();
    expect(m.fds.size).toBe(0);
  });
});
