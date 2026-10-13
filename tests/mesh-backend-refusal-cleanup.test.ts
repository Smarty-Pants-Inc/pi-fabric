import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { meshBackendStatus, type MeshBackendOptions } from "../src/mesh/backend-migration.js";
import { main } from "../src/mesh/mesh-backend-cli.js";
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

interface Removal { api: string; name: string; viaDirFd: boolean; dirMatches: boolean; target: string | undefined; meshLock: boolean; custodyLock: boolean }

/**
 * #4664: installed BEFORE the CLI runs. Every unlink/rm of a state.db* name, by any path (`<root>/state.db`, or a
 * held directory fd `/proc/self/fd/N/state.db` or `/dev/fd/N/state.db`, normalized to the basename with the fd's
 * directory identity checked against the root), is recorded with the target's identity and both fence locks'
 * presence, and NEVER performed. `fail` makes a recorded call throw instead. Any other path goes to the real fs
 * (the fence locks' own owner files).
 */
const recordRemovals = (root: string, fail?: (name: string, target: string) => NodeJS.ErrnoException | undefined): Removal[] => {
  const calls: Removal[] = [];
  const rootId = idOf(fs.statSync(root));
  const realRoot = fs.realpathSync(root);
  const intercept = (api: string, target: fs.PathLike): boolean => {
    const text = target instanceof URL ? fileURLToPath(target) : String(target);
    const pinned = /^(?:\/proc\/self\/fd|\/dev\/fd)\/(\d+)\/([^/]+)$/.exec(text);
    const name = pinned ? pinned[2]! : path.basename(text);
    if (!DB_NAME.test(name)) return false;
    let dirMatches = false;
    try {
      if (pinned) { const held = fs.fstatSync(Number(pinned[1])); dirMatches = held.isDirectory() && idOf(held) === rootId; }
      else dirMatches = fs.realpathSync(path.dirname(path.resolve(text))) === realRoot;
    } catch { /* an unknown directory: dirMatches stays false */ }
    const held = (lock: string) => fs.existsSync(path.join(root, lock, "owner"));
    calls.push({ api, name, viaDirFd: pinned !== null, dirMatches, target: idOf(fs.lstatSync(text, { throwIfNoEntry: false })),
      meshLock: held(".lock"), custodyLock: held("custody.lock") });
    const error = fail?.(name, text);
    if (error) throw error;
    return true;
  };
  const real = { unlinkSync: fs.unlinkSync, rmSync: fs.rmSync, unlink: fs.promises.unlink, rm: fs.promises.rm };
  vi.spyOn(fs, "unlinkSync").mockImplementation((target) => { if (!intercept("unlinkSync", target)) real.unlinkSync(target); });
  vi.spyOn(fs, "rmSync").mockImplementation((target, options) => { if (!intercept("rmSync", target)) real.rmSync(target, options); });
  vi.spyOn(fs.promises, "unlink").mockImplementation(async (target) => { if (!intercept("promises.unlink", target)) await real.unlink(target); });
  vi.spyOn(fs.promises, "rm").mockImplementation(async (target, options) => { if (!intercept("promises.rm", target)) await real.rm(target, options); });
  return calls;
};

/** Each recorded removal: a name in this root, through the held directory fd, under both fence locks, at most once per name. */
const fencedRemovals = (calls: Removal[]): void => {
  for (const call of calls) expect(call).toMatchObject({ viaDirFd: true, dirMatches: true, meshLock: true, custodyLock: true });
  expect(new Set(calls.map(call => call.name)).size).toBe(calls.length);
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
