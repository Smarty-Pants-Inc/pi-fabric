import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { meshBackendStatus, type MeshBackendOptions } from "../src/mesh/backend-migration.js";
import { main } from "../src/mesh/mesh-backend-cli.js";
import { decodeMeshStateFile, encodeMeshStateFile, type MeshStateFile } from "../src/mesh/state-file.js";
import { openNodeSqlite, type SqliteConnection, type SqliteOpener } from "../src/mesh/state-sqlite.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";

// smarty-dev#9501: a readiness refusal under the fence, before the importing commit, removes the state.db,
// -wal and -shm this run created (lstat identity re-checked, through the held directory fd, fence still held).
// Exit-1 errors and committed phases keep the database. #4664: "recorded cleanup" never deletes state.db*: every
// unlink of such a name is recorded (path, identity, held locks) and dropped. Only "hosted cleanup" (CI) deletes.

const identity: MeshIdentity = { id: "tester", name: "tester", kind: "agent" };
const dirs: string[] = [];
const savedRelease = process.env.PI_FABRIC_RELEASE_SHA;
beforeEach(() => { process.env.PI_FABRIC_RELEASE_SHA = "fabric-rel-a"; });
afterEach(() => {
  vi.restoreAllMocks(); // before the teardown: the fixture removes only its own mkdtemp roots, with the real fs
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

type RaceMode = "swap" | "swap-link-eexist" | "gone" | "gone-after-check";
/**
 * The same-UID race on the original basename, armed at the refusal (importing commit) and fired once at the first
 * check or move of `<root>/state.db` (by path or through a held root fd) while it still names this run's inode:
 * "swap": exactly AFTER an lstat returns the owned inode (or BEFORE a rename of it), the owned file is renamed to
 * moved-away.db and a foreign file is written as state.db. "swap-link-eexist": the same, and after the foreign inode is
 * renamed into a quarantine path a third file takes state.db (a link-back must fail EEXIST). "gone": BEFORE that
 * check or move the owned file is renamed to gone-elsewhere.db. Real rename/write in this test's own mkdtemp root.
 */
const raceCleanup = (root: string, mode: RaceMode) => {
  const realLstat = fs.lstatSync;
  const realRename = fs.renameSync;
  const db = path.join(root, "state.db");
  const rootId = idOf(fs.statSync(root));
  const realRoot = fs.realpathSync(root);
  const race = { armed: false, fired: false, owned: undefined as string | undefined, foreign: undefined as string | undefined,
    third: undefined as string | undefined };
  const isRootDb = (target: fs.PathLike): boolean => {
    const text = String(target);
    const pinned = /^(?:\/proc\/self\/fd|\/dev\/fd)\/(\d+)\/state\.db$/.exec(text);
    try {
      if (pinned) return idOf(fs.fstatSync(Number(pinned[1]))) === rootId;
      return path.basename(text) === "state.db" && fs.realpathSync(path.dirname(path.resolve(text))) === realRoot;
    } catch { return false; }
  };
  const fire = (): void => {
    race.fired = true;
    if (mode === "gone" || mode === "gone-after-check") { realRename(db, path.join(root, "gone-elsewhere.db")); return; }
    realRename(db, path.join(root, "moved-away.db"));
    fs.writeFileSync(db, "foreign", { mode: 0o600, flag: "wx" });
    race.foreign = idOf(realLstat(db));
  };
  const live = (): boolean => race.armed && !race.fired && idOf(realLstat(db, { throwIfNoEntry: false })) === race.owned;
  vi.spyOn(fs, "lstatSync").mockImplementation(((target: fs.PathLike, options?: fs.StatSyncOptions) => {
    if (mode === "gone" && isRootDb(target) && live()) fire();
    const result = realLstat(target, options);
    if (mode !== "gone" && isRootDb(target) && live() && idOf(result as fs.Stats | undefined) === race.owned) fire();
    return result;
  }) as typeof fs.lstatSync);
  vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
    if (isRootDb(from) && live()) fire();
    realRename(from, to);
    if (mode === "swap-link-eexist" && race.foreign !== undefined && race.third === undefined && !isRootDb(to)
      && idOf(realLstat(to, { throwIfNoEntry: false })) === race.foreign) {
      fs.writeFileSync(db, "third", { mode: 0o600, flag: "wx" });
      race.third = idOf(realLstat(db));
    }
  });
  const arm = (commit: Commit): void => {
    if (commit.phase !== "importing") return;
    race.owned = idOf(realLstat(db));
    race.armed = true;
  };
  return { race, arm, protect: () => new Set([race.owned, race.foreign, race.third]) };
};

// ponytail: Linux only (held-directory-fd paths under /proc/self/fd, the gate's /proc scan). The readiness proofs
// fail closed off POSIX (mesh-backend-readiness.test.ts), so Windows has no refusal-under-the-fence path to clean.
describe.skipIf(process.platform !== "linux")("recorded cleanup", () => {
  it.each(["import", "cutover"] as const)("%s: a refusal before the importing commit removes this run's state.db* under the fence; state.json is unchanged", async (command) => {
    const root = await fileRoot();
    await proveSqlite(root);
    const before = fs.readFileSync(path.join(root, "state.json"));
    const calls = recordRemovals(root);
    const result = await run(root, command, { refuseAt: "importing" });
    refusedUnderFence(result, "importing");
    expect(fs.readFileSync(path.join(root, "state.json")).equals(before)).toBe(true);
    fencedRemovals(calls);
    // The recorded calls deleted nothing: every state.db* name still here must have been removed by this run.
    expect(dbNames(root).filter(name => !calls.some(call => call.name === name))).toEqual([]);
    expect(calls.find(call => call.name === "state.db")?.target).toBe(result.opened[0]);
    expect(result.json.cleanupWarnings ?? []).toEqual([]);
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
    fencedRemovals(calls);
    expect(calls.filter(call => call.target === foreign)).toEqual([]);
    expect(fs.readFileSync(path.join(root, "state.db"), "utf8")).toBe("foreign");
    expect(result.json.cleanupWarnings?.some(text => text.includes("state.db"))).toBe(true);
    for (const text of result.json.cleanupWarnings ?? []) expect(result.err).toContain(text);
  });

  it("a failed removal keeps the refusal (exit 3) and warns on stderr and in --json", async () => {
    const root = await fileRoot();
    await proveSqlite(root);
    const calls = recordRemovals(root, (name, target) => name === "state.db"
      ? Object.assign(new Error(`EACCES: permission denied, unlink '${target}'`), { code: "EACCES" }) : undefined);
    const result = await run(root, "cutover", { refuseAt: "importing" });
    refusedUnderFence(result, "importing");
    fencedRemovals(calls);
    expect(calls.some(call => call.name === "state.db")).toBe(true);
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

  it("a state.json moved at import-read (fresh root): removes only this run's state.db*, under the fence", async () => {
    const root = await fileRoot();
    await proveSqlite(root);
    const calls = recordRemovals(root);
    const result = await run(root, "cutover", { onStep: rogueAt(root) });
    expect(result.code, result.err).toBe(3);
    expect(result.json.error).toMatch(/changed before the flag commit/);
    rogueKept(root);
    fencedRemovals(calls);
    expect(calls.find(call => call.name === "state.db")?.target).toBe(result.opened[0]);
    expect(dbNames(root).filter(name => !calls.some(call => call.name === name))).toEqual([]);
    expect(result.json.cleanupWarnings ?? []).toEqual([]);
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
  it("a state.db swapped after the owned-inode check, before its removal: the foreign file survives and is reported", async () => {
    const root = await fileRoot();
    await proveSqlite(root);
    const { race, arm, protect } = raceCleanup(root, "swap");
    const calls = recordRemovals(root, undefined, protect);
    const result = await run(root, "import", { refuseAt: "importing", beforeCommit: arm });
    refusedUnderFence(result, "importing");
    expect(race.fired).toBe(true);
    onlyRestoredAliasRemoved(calls, race.foreign);
    expect(calls.filter(call => call.target === race.owned)).toEqual([]); // the moved original is not at this run's name
    fencedRemovals(calls);
    // The recorder keeps the quarantine alias physically present; only the restored original is authoritative.
    const aliases = calls.filter(call => call.target === race.foreign && call.quarantine).map(call => call.rel);
    expect(whereIs(root, race.foreign).sort()).toEqual(["state.db", ...aliases].sort());
    console.info("QUARANTINE_SEQUENCE", JSON.stringify({ case: "mismatch-restored", race, calls, report: result.json }));
    expect(fs.readFileSync(path.join(root, "state.db"), "utf8")).toBe("foreign");
    expect(whereIs(root, race.owned)).toEqual(["moved-away.db"]);
    expect(result.json.retained).toContain("state.db");
    const warnings = result.json.cleanupWarnings ?? [];
    expect(warnings.some(text => text.includes(`${root}/state.db`))).toBe(true);
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
    fencedRemovals(calls);
    expect(race.third).toBeDefined();
    expect(fs.readFileSync(path.join(root, "state.db"), "utf8")).toBe("third");
    const kept = whereIs(root, race.foreign);
    expect(kept).toHaveLength(1);
    expect(kept[0]).toMatch(/^[^/]+\/state\.db$/);
    expect(fs.readFileSync(path.join(root, kept[0]!), "utf8")).toBe("foreign");
    expect(result.json.retained).toContain(kept[0]);
    console.info("QUARANTINE_SEQUENCE", JSON.stringify({ case: "link-back-eexist", race, calls, report: result.json }));
    const warnings = result.json.cleanupWarnings ?? [];
    expect(warnings.some(text => text.includes(kept[0]!))).toBe(true);
    for (const text of warnings) expect(result.err).toContain(text);
  });

  it("unchanged owned state.db: the one recorded state.db removal targets this run's inode, nothing retained", async () => {
    const root = await fileRoot();
    await proveSqlite(root);
    let owned: string | undefined;
    const calls = recordRemovals(root, undefined, () => new Set([owned]));
    const result = await run(root, "import", { refuseAt: "importing", beforeCommit: (commit) => {
      if (commit.phase === "importing") owned = idOf(fs.lstatSync(path.join(root, "state.db")));
    } });
    refusedUnderFence(result, "importing");
    fencedRemovals(calls);
    expect(owned).toBe(result.opened[0]);
    const dbCalls = calls.filter(call => call.name === "state.db");
    expect(dbCalls).toHaveLength(1);
    expect(dbCalls[0]!.target).toBe(owned);
    // Recorded, not performed: the owned inode is still at the path the removal named (original or quarantine).
    expect(whereIs(root, owned)).toContain(dbCalls[0]!.rel);
    expect(result.json.retained ?? []).toEqual([]);
    expect(result.json.cleanupWarnings ?? []).toEqual([]);
  });

  it.each(["gone", "gone-after-check"] as const)("an owned state.db disappears (%s): no state.db removal, the moved file survives", async (mode) => {
    const root = await fileRoot();
    await proveSqlite(root);
    const { race, arm, protect } = raceCleanup(root, mode);
    const calls = recordRemovals(root, undefined, protect);
    const result = await run(root, "import", { refuseAt: "importing", beforeCommit: arm });
    refusedUnderFence(result, "importing");
    expect(race.fired).toBe(true);
    fencedRemovals(calls);
    expect(calls.filter(call => call.name === "state.db" || call.target === race.owned)).toEqual([]);
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
});

// #4664: REAL deletion. Hosted CI only (CI=true), never in a lane. Same scenarios without the recorder.
describe.runIf(process.env.CI === "true" && process.platform === "linux")("hosted cleanup", () => {
  it.each(["import", "cutover"] as const)("%s: a refusal before the importing commit leaves no state.db*", async (command) => {
    const root = await fileRoot();
    await proveSqlite(root);
    const before = fs.readFileSync(path.join(root, "state.json"));
    const result = await run(root, command, { refuseAt: "importing" });
    refusedUnderFence(result, "importing");
    expect(dbNames(root)).toEqual([]);
    expect(fs.readFileSync(path.join(root, "state.json")).equals(before)).toBe(true);
    expect((await meshBackendStatus(root)).backend).toBe("none");
    expect(result.json.cleanupWarnings ?? []).toEqual([]);
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
});
