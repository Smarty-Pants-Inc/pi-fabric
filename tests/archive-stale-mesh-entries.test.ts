import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createRequire } from "node:module";
import { spawnSync } from "node:child_process";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

// Data-safety integration tests: real bytes, selection, publication, manifest and
// restore. Intercept filesystem removal, not the cyclic fence-lock module: the
// actual pinned lock release must run and its receipt must remain byte-identical.
// Every fixture stays below the inherited absolute private TMPDIR ceiling.

import { archiveStaleMeshEntries, archiveStaleMeshEntriesCli, hostLeasePastGrace, STALE_MESH_GRACE_MS } from "../src/topology/stale-mesh-entries.js";
import { hostLeasePath, readHostLeaseCurrent, renewHostLease, FabricHostLeaseSupersededError, type FabricHostLease } from "../src/topology/host-leases.js";
import { HostLeaseLockBusyError, type HostLeaseMesh } from "../src/topology/host-lease-lock.js";
import { acquireMeshCustodyLock } from "../src/mesh/custody-lock.js";
import { createHash } from "node:crypto";

const digest = (text: string): string => createHash("sha256").update(text).digest("hex");
const DEAD = 2_147_483_647;
const now = Date.now(), old = now - STALE_MESH_GRACE_MS - 60 * 60 * 1000;
let tmp: string;
const retainedDatabaseConnections: unknown[] = []; // avoid native last-close artifact cleanup
const put = (root: string, relative: string, value: unknown): string => {
  const file = path.join(root, relative);
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, typeof value === "string" ? value : JSON.stringify(value), { mode: 0o600 });
  fs.utimesSync(file, old / 1000, old / 1000);
  return file;
};
const lease = (id = "session:ended", extra: Partial<FabricHostLease> = {}): FabricHostLease => ({
  id, rootId: id, identityId: id, updatedAt: old, expiresAt: old, startedAt: old,
  incarnationToken: randomUUID(), session: { id: id.slice(8), startedAt: old, updatedAt: old, expiresAt: old },
  writer: { pid: DEAD, host: os.hostname(), releaseSha: "test", lockProtocol: 3, stateBackend: "file", startedAt: old }, ...extra,
});
const writeLease = (root: string, value: FabricHostLease): string => put(root,
  path.relative(root, hostLeasePath(root, value.id)), { format: 1, ...value });
const fixture = (full = false): { root: string; owner: string; lease: FabricHostLease; paths: string[] } => {
  const root = fs.mkdtempSync(path.join(tmp, "mesh-")), owner = "session:ended", value = lease(owner);
  const paths = [writeLease(root, value)];
  put(root, "state.json", { format: 1, entries: {} });
  if (full) {
    const id = "a".repeat(32), key = `topology/participants/${digest(id)}`;
    paths.push(put(root, `participants/${digest(id)}.json`, { format: 1, key, version: 1, updatedAt: old,
      updatedBy: { id: owner, kind: "main", name: "Ended" },
      value: { format: 1, id, kind: "actor", ownerHostId: owner, ownerIdentityId: owner, rootId: owner, updatedAt: old } }));
    paths.push(put(root, "main-followups/ended.owner.json", { rootId: owner, sessionId: "ended", ownerIdentityId: owner,
      pid: DEAD, host: os.hostname(), processStartedAt: "1" }));
    paths.push(put(root, "main-followups/ended.json", '{"version":1,"items":[]}\n'));
    paths.push(put(root, "actors/ended/actors.json", '{"format":1,"actors":[]}\n'));
    put(root, "main-followups/routes/fence.json", { permanent: true });
    put(root, "main-followups/successors/fence.json", { permanent: true });
    put(root, "main-followups/rotations/fence.json", { permanent: true });
  }
  return { root, owner, lease: value, paths };
};
const bytes = (files: string[]): Buffer[] => files.map(file => fs.readFileSync(file));
const sqliteState = (root: string): import("node:sqlite").DatabaseSync => {
  const { DatabaseSync } = createRequire(import.meta.url)("node:sqlite") as typeof import("node:sqlite");
  const db = new DatabaseSync(path.join(root, "state.db"));
  retainedDatabaseConnections.push(db);
  db.exec("PRAGMA journal_mode=WAL; CREATE TABLE meta(name TEXT PRIMARY KEY, value); CREATE TABLE kv(key TEXT PRIMARY KEY, value TEXT, version INTEGER, updated_at INTEGER, updated_by TEXT, bytes INTEGER)");
  const set = db.prepare("INSERT INTO meta(name,value) VALUES (?,?)");
  for (const [key, value] of [["schema", 1], ["backend", "sqlite"], ["epoch", 3], ["commit_no", 1], ["store_id", "writer-probe"]] as const) set.run(key, value);
  put(root, "state.json", { format: "sqlite", movedTo: "state.db", backend: "sqlite", epoch: 3 });
  for (const suffix of ["", "-wal", "-shm"]) fs.chmodSync(path.join(root, `state.db${suffix}`), 0o600);
  return db;
};
const separateSqliteWriter = (root: string, key: string): void => {
  const child = spawnSync(process.execPath, ["-e", `
    const { DatabaseSync } = require('node:sqlite');
    const db = new DatabaseSync(process.argv[1]);
    db.exec('PRAGMA busy_timeout=0; BEGIN IMMEDIATE');
    db.prepare('INSERT OR REPLACE INTO kv VALUES (?, ?, ?, ?, ?, ?)').run(process.argv[2],
      JSON.stringify({ unrelated: true }), 1, Date.now(), JSON.stringify({ id: 'session:other', kind: 'main' }), 0);
    db.exec("UPDATE meta SET value=value+1 WHERE name='commit_no'; COMMIT");
  `, path.join(root, "state.db"), key], { timeout: 5_000, encoding: "utf8" });
  expect(child.error).toBeUndefined();
  expect(child.status, child.stderr).toBe(0);
};
const observeSqliteReaders = () => {
  const sqlite = createRequire(import.meta.url)("node:sqlite") as typeof import("node:sqlite");
  const Native = sqlite.DatabaseSync, live = new Set<import("node:sqlite").DatabaseSync>(), statements: string[] = [];
  const opens = vi.spyOn(sqlite, "DatabaseSync").mockImplementation(function (...args: ConstructorParameters<typeof Native>) {
    const db = new Native(...args), exec = db.exec.bind(db), close = db.close.bind(db);
    live.add(db);
    db.exec = sql => { statements.push(sql); exec(sql); };
    db.close = () => { try { close(); } finally { live.delete(db); } };
    return db;
  } as unknown as typeof Native);
  return { opens, live, statements };
};
/** A fixture adapter using MeshStore.leaseCustody's exact legacy/domain preference
 * and its REAL custody lock, without constructing a store/census or fake mutex. */
const leaseGateMesh = (root: string, entered: () => void): HostLeaseMesh => ({
  root,
  async leaseCustody(file, operation, timeoutMs = 0, options = {}) {
    const name = digest(path.basename(file)), legacy = path.join(root, "host-lease-commits", name);
    const domain = fs.existsSync(legacy) ? legacy : path.join(root, "host-lease-commits", name.slice(0, 16));
    const release = await acquireMeshCustodyLock(domain, timeoutMs, { ...options, hostQualified: true, lockProtocol: 3 });
    try { entered(); return await operation(); } finally { release(); }
  },
});
/** Independent process: unlike a same-turn async contender, this really attempts
 * the canonical claim WHILE the parent's synchronous move holds S's gate. */
const busyCanonicalClaim = (root: string, replacement: FabricHostLease, expected: FabricHostLease): void => {
  const child = spawnSync(process.execPath, ["--input-type=module", "-e", `
    import fs from 'node:fs'; import path from 'node:path'; import { createHash, randomUUID } from 'node:crypto';
    import { createJiti } from 'jiti'; import { pathToFileURL } from 'node:url';
    const root = process.argv[1], replacement = JSON.parse(process.argv[2]), expected = JSON.parse(process.argv[3]);
    // Retain lock staging/receipts, just as the parent test does; no byte deletion.
    fs.rmSync = target => {
      const file = String(target);
      if (!file.startsWith(root + path.sep)) throw new Error('child removal outside fixture refused');
      if (fs.existsSync(file)) fs.renameSync(file, file + '.test-retained-' + randomUUID());
    };
    fs.rmdirSync = () => { throw new Error('child directory deletion refused'); };
    const unlink = fs.unlinkSync.bind(fs);
    fs.unlinkSync = file => {
      if (fs.lstatSync(file).nlink <= 1) throw new Error('child byte deletion refused');
      unlink(file);
    };
    const jiti = createJiti(pathToFileURL(process.cwd() + '/index.js').href);
    const { renewHostLease } = await jiti.import(path.join(process.cwd(), 'src/topology/host-leases.ts'));
    const { acquireMeshCustodyLock } = await jiti.import(path.join(process.cwd(), 'src/mesh/custody-lock.ts'));
    const { HostLeaseLockBusyError } = await jiti.import(path.join(process.cwd(), 'src/topology/host-lease-lock.ts'));
    let entered = false;
    const mesh = { root, async leaseCustody(file, operation, timeoutMs = 0, options = {}) {
      const hash = createHash('sha256').update(path.basename(file)).digest('hex');
      const legacy = path.join(root, 'host-lease-commits', hash);
      const domain = fs.existsSync(legacy) ? legacy : path.join(root, 'host-lease-commits', hash.slice(0, 16));
      const release = await acquireMeshCustodyLock(domain, timeoutMs, { ...options, hostQualified: true, lockProtocol: 3 });
      try { entered = true; return await operation(); } finally { release(); }
    } };
    try { await renewHostLease(mesh, replacement, { claim: true, expected, timeoutMs: 0 }); throw new Error('claim crossed held gate'); }
    catch (error) {
      if (!(error instanceof HostLeaseLockBusyError) || entered) throw error;
      process.stdout.write('canonical-claim-busy-before-entry');
    }
  `, root, JSON.stringify(replacement), JSON.stringify(expected)], {
    cwd: path.resolve(import.meta.dirname, ".."), timeout: 10_000, encoding: "utf8",
  });
  expect(child.error).toBeUndefined();
  expect(child.status, child.stderr).toBe(0);
  expect(child.stdout).toBe("canonical-claim-busy-before-entry");
};
/** Inject at the actual receipt syscall, including old string-write implementations. */
const receiptWrites = (inject: (write: { type: string; bytes: Buffer; offset: number; length: number;
  write: (length: number) => number }) => number) => {
  const original = fs.writeSync.bind(fs);
  return vi.spyOn(fs, "writeSync").mockImplementation(((fd: number, data: unknown, ...args: unknown[]) => {
    if (typeof data !== "string" && !Buffer.isBuffer(data)) return Reflect.apply(original, fs, [fd, data, ...args]);
    const buffer = typeof data === "string" ? Buffer.from(data, "utf8") : data;
    if (!buffer.toString("utf8").startsWith('{"type":')) return Reflect.apply(original, fs, [fd, data, ...args]);
    const type = String(JSON.parse(buffer.toString("utf8")).type);
    const offset = typeof data === "string" ? 0 : Number(args[0] ?? 0);
    const length = typeof data === "string" ? buffer.byteLength : Number(args[1] ?? buffer.byteLength - offset);
    return inject({ type, bytes: buffer, offset, length, write: count => original(fd, buffer, offset, count) });
  }) as typeof fs.writeSync);
};

beforeEach(() => {
  const base = process.env.TMPDIR;
  if (!base || !path.isAbsolute(base)) throw new Error("Archive tests require the runner's absolute private TMPDIR");
  const stat = fs.lstatSync(base);
  if (!stat.isDirectory() || stat.isSymbolicLink() || (process.platform !== "win32" &&
      ((stat.mode & 0o077) !== 0 || (typeof process.getuid === "function" && stat.uid !== process.getuid())))) {
    throw new Error("Archive test TMPDIR must be a private directory owned by the runner");
  }
  tmp = fs.mkdtempSync(path.join(base, "archive-stale-tests-"));
  const realKill = process.kill.bind(process);
  vi.spyOn(process, "kill").mockImplementation((pid, signal) => {
    if (pid === DEAD && signal === 0) throw Object.assign(new Error("synthetic dead writer"), { code: "ESRCH" });
    return realKill(pid, signal);
  });
  vi.spyOn(fs, "rmSync").mockImplementation(target => {
    const file = String(target);
    if (fs.existsSync(file)) fs.renameSync(file, path.basename(file) === "actors.json.lock"
      ? path.join(tmp, `actor-lock-receipt-${randomUUID()}`) // retain outside the canonical registry's sidecar namespace
      : `${file}.test-retained-${randomUUID()}`);
  });
  vi.spyOn(fs, "rmdirSync").mockImplementation(() => { throw new Error("Test forbids directory deletion"); });
  const unlink = fs.unlinkSync.bind(fs);
  vi.spyOn(fs, "unlinkSync").mockImplementation(file => {
    if (path.basename(String(file)) === "owner" && fs.lstatSync(file).nlink === 1) {
      const lock = fs.realpathSync(path.dirname(String(file)));
      expect(lock.startsWith(`${tmp}${path.sep}`)).toBe(true);
      fs.renameSync(lock, `${lock}.test-retained-${randomUUID()}`);
      return; // Actual pinned release observes its detached inode and closes its fd.
    }
    expect(fs.lstatSync(file).nlink, `unlink must not delete bytes: ${String(file)}`).toBeGreaterThan(1);
    unlink(file); // MOVE's second name only; the inode/bytes always remain.
  });
});
afterEach(() => {
  vi.restoreAllMocks();
  // Deliberately retain every fixture and intercepted lock receipt below the
  // inherited TMPDIR; never replace the runner's write-ceiling environment.
});

describe("offline reversible stale mesh archive", () => {
  it("dry-runs, applies idempotently, and restores the real four-directory slice byte-identically", async () => {
    const f = fixture(true), before = bytes(f.paths);
    const dry = await archiveStaleMeshEntriesCli(["--mesh", f.root, "--dry-run"]);
    expect(dry.counts).toEqual({ "host-leases": 1, participants: 1, "main-followups": 2, actors: 1 });
    expect(dry.examples.length).toBeGreaterThan(0);
    expect(fs.existsSync(path.join(f.root, "archive"))).toBe(false);
    expect(bytes(f.paths)).toEqual(before);
    const applied = await archiveStaleMeshEntries({ mesh: f.root, mode: "apply", now });
    expect(applied.counts).toEqual(dry.counts);
    for (const file of f.paths) expect(fs.existsSync(file)).toBe(false);
    expect(fs.statSync(path.join(f.root, "actors/ended")).isDirectory()).toBe(true);
    for (const dir of ["routes", "successors", "rotations"]) expect(fs.readFileSync(path.join(f.root, "main-followups", dir, "fence.json"), "utf8")).toBe('{"permanent":true}');
    expect((await archiveStaleMeshEntries({ mesh: f.root, mode: "apply", now })).counts).toEqual({ "host-leases": 0, participants: 0, "main-followups": 0, actors: 0 });
    const restored = await archiveStaleMeshEntriesCli(["--mesh", f.root, "--restore", applied.batch!]);
    expect(restored.counts).toEqual(applied.counts);
    expect(bytes(f.paths)).toEqual(before);
  });

  it("uses read-only SQLite facts, allows unrelated commits, restores, and vetoes changed exact-owner state", async () => {
    const f = fixture(true);
    const { DatabaseSync } = createRequire(import.meta.url)("node:sqlite") as typeof import("node:sqlite");
    const db = new DatabaseSync(path.join(f.root, "state.db"));
    retainedDatabaseConnections.push(db);
    db.exec("PRAGMA journal_mode=WAL; CREATE TABLE meta(name TEXT PRIMARY KEY, value); CREATE TABLE kv(key TEXT PRIMARY KEY, value TEXT, version INTEGER, updated_at INTEGER, updated_by TEXT, bytes INTEGER)");
    const set = db.prepare("INSERT INTO meta(name,value) VALUES (?,?)");
    for (const [key, value] of [["schema", 1], ["backend", "sqlite"], ["epoch", 3], ["commit_no", 1], ["store_id", "synthetic-store"]] as const) set.run(key, value);
    put(f.root, "state.json", { format: "sqlite", movedTo: "state.db", backend: "sqlite", epoch: 3 });
    for (const suffix of ["", "-wal", "-shm"]) fs.chmodSync(path.join(f.root, `state.db${suffix}`), 0o600);
    const before = bytes(f.paths);
    const markerBefore = fs.readFileSync(path.join(f.root, "state.json"));
    expect((await archiveStaleMeshEntries({ mesh: f.root, mode: "dry-run", now })).counts["host-leases"]).toBe(1);
    expect(fs.readFileSync(path.join(f.root, "state.json"))).toEqual(markerBefore);
    expect(db.prepare("SELECT value FROM meta WHERE name='commit_no'").get()?.value).toBe(1);
    const applied = await archiveStaleMeshEntries({ mesh: f.root, mode: "apply", now, afterSelection: () => {
      db.exec("BEGIN IMMEDIATE");
      db.prepare("INSERT INTO kv VALUES (?, ?, ?, ?, ?, ?)").run("unrelated/noise", JSON.stringify({ active: true }), 1, now,
        JSON.stringify({ id: "session:other", kind: "main" }), 0);
      db.exec("UPDATE meta SET value=2 WHERE name='commit_no'; COMMIT");
    } });
    expect(applied.counts).toEqual({ "host-leases": 1, participants: 1, "main-followups": 2, actors: 1 });
    await archiveStaleMeshEntries({ mesh: f.root, mode: "restore", batch: applied.batch! });
    expect(bytes(f.paths)).toEqual(before);
    expect(db.prepare("SELECT value FROM meta WHERE name='commit_no'").get()?.value).toBe(2);
    const identityChanged = await archiveStaleMeshEntries({ mesh: f.root, mode: "apply", now, afterSelection: () => {
      db.exec("UPDATE meta SET value='replacement-store' WHERE name='store_id'");
    } });
    expect(Object.values(identityChanged.counts).every(n => n === 0)).toBe(true);
    expect(bytes(f.paths)).toEqual(before);
    db.exec("UPDATE meta SET value='synthetic-store' WHERE name='store_id'");
    const changed = await archiveStaleMeshEntries({ mesh: f.root, mode: "apply", now, afterSelection: () => {
      const key = `topology/hosts/${digest(f.owner)}`;
      db.exec("BEGIN IMMEDIATE");
      db.prepare("INSERT INTO kv VALUES (?, ?, ?, ?, ?, ?)").run(key, JSON.stringify({ id: f.owner, rootId: f.owner,
        identity: { id: f.owner }, expiresAt: now + 60_000 }), 1, now, JSON.stringify({ id: f.owner, kind: "main" }), 0);
      db.exec("UPDATE meta SET value=3 WHERE name='commit_no'; COMMIT");
    } });
    expect(Object.values(changed.counts).every(n => n === 0)).toBe(true);
    expect(changed.retained.join(" ")).toContain("changed");
    expect(bytes(f.paths)).toEqual(before);
    db.exec("UPDATE meta SET value=4 WHERE name='epoch'");
    await expect(archiveStaleMeshEntries({ mesh: f.root, mode: "dry-run", now })).rejects.toThrow(/epoch/);
    // Intentionally retain the live fixture connection: native SQLite last-close
    // WAL/SHM cleanup must not delete test artifacts. No product data SQL is written.
  });

  it("opens every SQLite observation read-only and vetoes a changed exact owner with short read transactions", async () => {
    const f = fixture(true), before = bytes(f.paths), writer = sqliteState(f.root);
    const sqlite = createRequire(import.meta.url)("node:sqlite") as typeof import("node:sqlite");
    const Native = sqlite.DatabaseSync;
    const opens = vi.spyOn(sqlite, "DatabaseSync").mockImplementation(function (...args: ConstructorParameters<typeof Native>) {
      return new Native(...args);
    } as unknown as typeof Native);
    const sql = vi.spyOn(Native.prototype, "exec");
    const report = await archiveStaleMeshEntries({ mesh: f.root, mode: "apply", now, afterSelection: () => {
      // Counterexample: generic SQLite state mutation needs neither lease custody
      // nor file/shared .lock. Read-only archival cannot claim to exclude it.
      writer.exec("BEGIN IMMEDIATE");
      writer.prepare("INSERT INTO kv VALUES (?, ?, ?, ?, ?, ?)").run(`topology/hosts/${digest(f.owner)}`,
        JSON.stringify({ id: f.owner, expiresAt: now + 60_000 }), 1, now, JSON.stringify({ id: f.owner, kind: "main" }), 0);
      writer.exec("COMMIT");
    } });
    expect(opens.mock.calls.length).toBeGreaterThan(0);
    expect(opens.mock.calls.every(call => call[1]?.readOnly === true)).toBe(true);
    // Separate the explicit generic writer from archival's own SQL.
    const reads = sql.mock.calls.filter((_, index) => sql.mock.contexts[index] !== writer).map(call => call[0]);
    expect(reads.length).toBeGreaterThan(2);
    expect(reads.every(statement => statement === "BEGIN" || statement === "ROLLBACK")).toBe(true);
    expect(reads.filter(statement => statement === "BEGIN")).toHaveLength(reads.length / 2);
    expect(Object.values(report.counts).every(count => count === 0)).toBe(true);
    expect(report.retained.join(" ")).toContain("changed");
    expect(bytes(f.paths)).toEqual(before);
  });

  it("allows a separate real SQLite writer at the ACTUAL apply source-to-archive link with all readers closed", async () => {
    const f = fixture(), before = bytes(f.paths), db = sqliteState(f.root), observed = observeSqliteReaders();
    const link = fs.linkSync.bind(fs), sync = fs.fsyncSync.bind(fs);
    let atApplyLink = false;
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => { expect(observed.live.size).toBe(0); sync(fd); });
    vi.spyOn(fs, "linkSync").mockImplementation((from, to) => {
      if (String(from) === f.paths[0] && String(to).includes(`${path.sep}archive${path.sep}host-leases${path.sep}`)) {
        atApplyLink = true;
        expect(observed.live.size).toBe(0);
        separateSqliteWriter(f.root, "unrelated/at-apply-link");
      }
      link(from, to);
    });
    const applied = await archiveStaleMeshEntries({ mesh: f.root, mode: "apply", now });
    expect(atApplyLink).toBe(true);
    expect(applied.counts["host-leases"]).toBe(1);
    expect(observed.opens.mock.calls.every(call => call[1]?.readOnly === true)).toBe(true);
    expect(observed.statements.every(sql => sql === "BEGIN" || sql === "ROLLBACK")).toBe(true);
    expect(observed.statements.filter(sql => sql === "BEGIN")).toHaveLength(observed.statements.length / 2);
    expect(db.prepare("SELECT value FROM meta WHERE name='commit_no'").get()?.value).toBe(2);
    await archiveStaleMeshEntries({ mesh: f.root, mode: "restore", batch: applied.batch! });
    expect(bytes(f.paths)).toEqual(before);
  });

  it("freshly vetoes exact-owner state changed DURING intent fsync before any apply source moves", async () => {
    const f = fixture(true), before = bytes(f.paths), db = sqliteState(f.root), observed = observeSqliteReaders();
    const sync = fs.fsyncSync.bind(fs), link = fs.linkSync.bind(fs);
    let intentWritten = false, changed = false, sourceLinks = 0;
    receiptWrites(call => { if (call.type === "intent") intentWritten = true; return call.write(call.length); });
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      expect(observed.live.size).toBe(0);
      if (intentWritten && !changed) {
        changed = true;
        db.exec("BEGIN IMMEDIATE");
        db.prepare("INSERT INTO kv VALUES (?, ?, ?, ?, ?, ?)").run(`topology/hosts/${digest(f.owner)}`,
          JSON.stringify({ id: f.owner, rootId: f.owner, identity: { id: f.owner }, expiresAt: now + 60_000 }),
          1, now, JSON.stringify({ id: f.owner, kind: "main" }), 0);
        db.exec("COMMIT");
      }
      sync(fd);
    });
    vi.spyOn(fs, "linkSync").mockImplementation((from, to) => {
      if (f.paths.includes(String(from))) sourceLinks++;
      link(from, to);
    });
    const applied = await archiveStaleMeshEntries({ mesh: f.root, mode: "apply", now });
    expect(changed).toBe(true);
    expect(sourceLinks).toBe(0);
    expect(Object.values(applied.counts).every(count => count === 0)).toBe(true);
    expect(applied.retained.join(" ")).toContain("after intent fsync");
    expect(observed.statements.every(sql => sql === "BEGIN" || sql === "ROLLBACK")).toBe(true);
    expect(bytes(f.paths)).toEqual(before);
    expect(fs.readFileSync(path.join(f.root, "archive/manifests", `${applied.batch}.jsonl`), "utf8")).toContain('"type":"rolled-back"');
  });

  it("rechecks between paired moves and rolls back its own archived journal if fresh owner facts veto the owner move", async () => {
    const f = fixture(true), pair = f.paths.slice(2, 4), before = bytes(pair), db = sqliteState(f.root);
    const unlink = vi.mocked(fs.unlinkSync).getMockImplementation()!;
    let changed = false;
    vi.mocked(fs.unlinkSync).mockImplementation(file => {
      unlink(file);
      if (!changed && String(file) === f.paths[3]) {
        changed = true;
        db.prepare("INSERT INTO kv VALUES (?, ?, ?, ?, ?, ?)").run(`topology/hosts/${digest(f.owner)}`,
          JSON.stringify({ id: f.owner, expiresAt: now + 60_000 }), 1, now, JSON.stringify({ id: f.owner, kind: "main" }), 0);
      }
    });
    let applied;
    try { applied = await archiveStaleMeshEntries({ mesh: f.root, mode: "apply", now }); }
    finally { vi.mocked(fs.unlinkSync).mockImplementation(unlink); }
    expect(changed).toBe(true);
    expect(applied.counts["main-followups"]).toBe(0);
    expect(bytes(pair)).toEqual(before);
    expect(applied.retained.join(" ")).toContain("before move");
    const rows = fs.readFileSync(path.join(f.root, "archive/manifests", `${applied.batch}.jsonl`), "utf8")
      .trimEnd().split("\n").map(line => JSON.parse(line));
    const intent = rows.find(row => row.type === "intent" && row.directory === "main-followups");
    expect(rows.some(row => row.type === "rolled-back" && row.id === intent.id)).toBe(true);
  });

  it("keeps a concurrent canonical S claim outside a two-file move until the entire pair is gone", async () => {
    const f = fixture(true); sqliteState(f.root);
    const batch = "claim-fence", pair = f.paths.slice(2, 4), link = fs.linkSync.bind(fs);
    const unlink = vi.mocked(fs.unlinkSync).getMockImplementation()!;
    const replacement = { ...f.lease, incarnationToken: randomUUID(), expiresAt: now + 60_000,
      writer: { ...f.lease.writer!, pid: process.pid } };
    let wholePairMoved = false, entered = 0;
    const mesh = leaseGateMesh(f.root, () => {
      entered++;
      expect(wholePairMoved).toBe(true);
      for (const file of pair) {
        expect(fs.existsSync(file)).toBe(false);
        expect(fs.existsSync(path.join(f.root, "archive/main-followups", batch, path.basename(file)))).toBe(true);
      }
    });
    let claim: Promise<{ error?: unknown }> | undefined;
    vi.spyOn(fs, "linkSync").mockImplementation((from, to) => {
      if (String(from) === f.paths[3] && !claim) {
        const gateHash = digest(path.basename(f.paths[0]!));
        expect(fs.existsSync(path.join(f.root, "host-lease-commits", gateHash.slice(0, 16), "custody.lock"))).toBe(true);
        busyCanonicalClaim(f.root, replacement, f.lease);
        claim = renewHostLease(mesh, replacement, { claim: true, expected: f.lease, timeoutMs: 0 })
          .then(() => ({}), error => ({ error }));
      }
      link(from, to);
    });
    vi.mocked(fs.unlinkSync).mockImplementation(file => {
      unlink(file);
      if (String(file) === f.paths[2]) wholePairMoved = true; // owner LAST; journal was already moved.
    });
    let applied;
    try { applied = await archiveStaleMeshEntries({ mesh: f.root, mode: "apply", batch, now }); }
    finally { vi.mocked(fs.unlinkSync).mockImplementation(unlink); }
    expect(applied.counts["main-followups"]).toBe(2);
    expect(claim).toBeDefined();
    const first = await claim!;
    if (first.error) {
      // Canonical claim is zero-budget: Busy (or predecessor CAS superseded if
      // it enters after archival) is a receipt, not a promise that waits forever.
      expect(first.error instanceof HostLeaseLockBusyError || first.error instanceof FabricHostLeaseSupersededError).toBe(true);
      await renewHostLease(mesh, replacement, { claim: true, expected: readHostLeaseCurrent(f.root, f.owner), timeoutMs: 0 });
    }
    expect(entered).toBeGreaterThan(0);
    expect(readHostLeaseCurrent(f.root, f.owner)?.incarnationToken).toBe(replacement.incarnationToken);
  }, 30_000);

  it.each(["no-token", "protocol-2", "no-lease"])("retains only SQLite candidates lacking native lease authority (%s), with a dry-run reason", async kind => {
    const f = fixture(true); sqliteState(f.root);
    if (kind === "no-lease") fs.renameSync(f.paths[0]!, `${f.paths[0]}.retained`);
    else {
      const value = { ...f.lease, writer: { ...f.lease.writer!, lockProtocol: kind === "protocol-2" ? 2 : 3 } };
      if (kind === "no-token") delete value.incarnationToken;
      writeLease(f.root, value);
    }
    const retained = bytes(f.paths.filter(file => fs.existsSync(file)));
    const dry = await archiveStaleMeshEntries({ mesh: f.root, mode: "dry-run", now });
    expect(Object.values(dry.counts).every(count => count === 0)).toBe(true);
    expect(dry.retained.join(" ")).toContain("token-bearing local protocol-3 lease");
    const applied = await archiveStaleMeshEntries({ mesh: f.root, mode: "apply", now });
    expect(applied.counts).toEqual(dry.counts);
    expect(bytes(f.paths.filter(file => fs.existsSync(file)))).toEqual(retained);
    expect(fs.existsSync(path.join(f.root, "archive"))).toBe(false);
  });

  it("restores SQLite-backed meshes without opening a database or blocking a separate real writer at manifest fsync and link", async () => {
    const f = fixture(), before = bytes(f.paths);
    const applied = await archiveStaleMeshEntries({ mesh: f.root, mode: "apply", now });
    const db = sqliteState(f.root);
    const sqlite = createRequire(import.meta.url)("node:sqlite") as typeof import("node:sqlite");
    const opens = vi.spyOn(sqlite, "DatabaseSync").mockImplementation(() => { throw new Error("restore must not open SQLite"); });
    let commits = 0;
    const writer = (): void => {
      const child = spawnSync(process.execPath, ["-e", `
        const { DatabaseSync } = require('node:sqlite');
        const db = new DatabaseSync(process.argv[1]);
        db.exec("PRAGMA busy_timeout=0; BEGIN IMMEDIATE; UPDATE meta SET value=value+1 WHERE name='commit_no'; COMMIT");
      `, path.join(f.root, "state.db")], { timeout: 5_000, encoding: "utf8" });
      expect(child.error).toBeUndefined();
      expect(child.status, child.stderr).toBe(0);
      commits++;
    };
    const descriptors = new Map<number, string>(), open = fs.openSync.bind(fs), sync = fs.fsyncSync.bind(fs), link = fs.linkSync.bind(fs);
    vi.spyOn(fs, "openSync").mockImplementation(((...args: Parameters<typeof fs.openSync>) => {
      const fd = Reflect.apply(open, fs, args); descriptors.set(fd, String(args[0])); return fd;
    }) as typeof fs.openSync);
    let atReceipt = false, atLink = false;
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      if (!atReceipt && descriptors.get(fd)?.endsWith(`${applied.batch}.jsonl`)) { atReceipt = true; writer(); }
      sync(fd);
    });
    vi.spyOn(fs, "linkSync").mockImplementation((from, to) => {
      if (String(to) === f.paths[0]) { atLink = true; writer(); }
      link(from, to);
    });
    await archiveStaleMeshEntries({ mesh: f.root, mode: "restore", batch: applied.batch! });
    expect(atReceipt && atLink).toBe(true);
    expect(opens).not.toHaveBeenCalled();
    expect(commits).toBe(2);
    expect(db.prepare("SELECT value FROM meta WHERE name='commit_no'").get()?.value).toBe(3);
    expect(bytes(f.paths)).toEqual(before);
  });

  it("archives stale S projections while preserving live X's own lease even when X.rootId is S", async () => {
    const f = fixture(true), x = "host:live-root-alias";
    const xFile = writeLease(f.root, lease(x, { rootId: f.owner, identityId: x, expiresAt: now + 60_000,
      writer: { ...f.lease.writer!, pid: process.pid } }));
    const xBefore = fs.readFileSync(xFile), key = `topology/hosts/${digest(x)}`;
    put(f.root, "state.json", { format: 1, entries: { [key]: { key, version: 1, updatedAt: now,
      updatedBy: { id: x, kind: "host" }, value: { id: x, rootId: f.owner, identity: { id: x }, expiresAt: now + 60_000 } } } });
    const result = await archiveStaleMeshEntries({ mesh: f.root, mode: "apply", now });
    expect(result.counts).toEqual({ "host-leases": 1, participants: 1, "main-followups": 2, actors: 1 });
    for (const file of f.paths) expect(fs.existsSync(file)).toBe(false);
    expect(fs.readFileSync(xFile)).toEqual(xBefore);
    await archiveStaleMeshEntries({ mesh: f.root, mode: "restore", batch: result.batch! });
    for (const file of f.paths) expect(fs.existsSync(file)).toBe(true);
    expect(fs.readFileSync(xFile)).toEqual(xBefore);
  });

  it("vetoes a new live exact S lease appearing after selection, with no initial S lease", async () => {
    const f = fixture(true);
    fs.renameSync(f.paths[0]!, `${f.paths[0]}.retained`);
    const before = bytes(f.paths.slice(1));
    expect((await archiveStaleMeshEntries({ mesh: f.root, mode: "dry-run", now })).counts)
      .toEqual({ "host-leases": 0, participants: 1, "main-followups": 2, actors: 1 });
    let replacement: Buffer | undefined;
    const report = await archiveStaleMeshEntries({ mesh: f.root, mode: "apply", now, afterSelection: () => {
      replacement = fs.readFileSync(writeLease(f.root, { ...f.lease, expiresAt: now + 60_000,
        writer: { ...f.lease.writer!, pid: process.pid } }));
    } });
    expect(Object.values(report.counts).every(n => n === 0)).toBe(true);
    expect(bytes(f.paths.slice(1))).toEqual(before);
    expect(fs.readFileSync(f.paths[0]!)).toEqual(replacement);
    expect(report.retained.join(" ")).toContain("changed");
  });

  it.each(["host", "session", "reload"])("retains all S-owned entries with a live S %s deadline", async clock => {
    const f = fixture(true);
    writeLease(f.root, { ...f.lease, ...(clock === "host" ? { expiresAt: now + 10_000 } :
      clock === "reload" ? { reloadUntil: now + 10_000 } : { session: { ...f.lease.session!, expiresAt: now + 10_000 } }) });
    const result = await archiveStaleMeshEntries({ mesh: f.root, mode: "apply", now });
    expect(Object.values(result.counts).every(n => n === 0)).toBe(true);
    for (const file of f.paths) expect(fs.existsSync(file)).toBe(true);
  });

  it("rejects a lease renewed between selection and custody even if its new deadline is still aged", async () => {
    const f = fixture(true);
    const result = await archiveStaleMeshEntries({ mesh: f.root, mode: "apply", now, afterSelection: () => {
      writeLease(f.root, { ...f.lease, updatedAt: old + 1, expiresAt: old + 1 });
    } });
    expect(Object.values(result.counts).every(n => n === 0)).toBe(true);
    expect(result.retained.some(line => line.includes("changed"))).toBe(true);
    for (const file of f.paths) expect(fs.existsSync(file)).toBe(true);
  });

  it.each(["unrelated", "owner"])("allows unrelated file-store replacement but vetoes changed owner facts: %s", async kind => {
    const f = fixture(true), before = bytes(f.paths), key = `topology/hosts/${digest(f.owner)}`;
    const host = { key, version: 1, updatedAt: old, updatedBy: { id: f.owner, kind: "main" },
      value: { id: f.owner, rootId: f.owner, identity: { id: f.owner }, expiresAt: old } };
    put(f.root, "state.json", { format: 1, entries: { [key]: host } });
    const result = await archiveStaleMeshEntries({ mesh: f.root, mode: "apply", now, afterSelection: () => {
      put(f.root, "state.json", { format: 1, entries: kind === "owner"
        ? { [key]: { ...host, version: 2, updatedAt: old + 1 } }
        : { [key]: host, "unrelated/noise": { key: "unrelated/noise", version: 1, updatedAt: now,
          updatedBy: { id: "session:other", kind: "main" }, value: { active: true } } } });
    } });
    if (kind === "owner") {
      expect(Object.values(result.counts).every(n => n === 0)).toBe(true);
      expect(bytes(f.paths)).toEqual(before);
    } else {
      expect(result.counts).toEqual({ "host-leases": 1, participants: 1, "main-followups": 2, actors: 1 });
      await archiveStaleMeshEntries({ mesh: f.root, mode: "restore", batch: result.batch! });
      expect(bytes(f.paths)).toEqual(before);
    }
  });

  it("vetoes an aged exact-owner file lease when its canonical shared host state is live", async () => {
    const f = fixture(true), key = `topology/hosts/${digest(f.owner)}`;
    put(f.root, "state.json", { format: 1, entries: { [key]: { key, version: 1, updatedAt: now,
      updatedBy: { id: f.owner, kind: "main" }, value: { id: f.owner, rootId: f.owner,
        identity: { id: f.owner }, expiresAt: now + 10_000, incarnationToken: "replacement" } } } });
    expect(Object.values((await archiveStaleMeshEntries({ mesh: f.root, mode: "dry-run", now })).counts).every(n => n === 0)).toBe(true);
  });

  it.each(["remote", "missing-writer", "live-pid"])("retains unknown/insufficient standalone evidence: %s", async kind => {
    const f = fixture();
    const changed = { ...f.lease };
    if (kind === "missing-writer") delete changed.writer;
    else changed.writer = { ...changed.writer!, ...(kind === "remote" ? { host: "foreign-host" } : { pid: process.pid }) };
    writeLease(f.root, changed);
    expect((await archiveStaleMeshEntries({ mesh: f.root, mode: "apply", now })).counts["host-leases"]).toBe(0);
  });

  it("retains every nonempty actor registry, including apparently retired orphan rows and their owner evidence", async () => {
    const f = fixture(true);
    const registry = path.join(f.root, "actors/ended/actors.json");
    put(f.root, "actors/ended/actors.json", { format: 1, actors: [{ id: "a".repeat(32), rootId: f.owner,
      residency: "session", status: "stopped", sessionOrphan: { oldRoot: f.owner, orphanedAt: old,
        directorySettlement: { by: "session:observer", at: old, retiredAt: old, retiredBy: "session:observer" } } }] });
    const before = fs.readFileSync(registry);
    const result = await archiveStaleMeshEntries({ mesh: f.root, mode: "apply", now });
    expect(Object.values(result.counts).every(n => n === 0)).toBe(true);
    expect(result.retained.join(" ")).toContain("nonempty");
    expect(fs.readFileSync(registry)).toEqual(before);
  });

  it("freshly rechecks the owner's actor registry and preserves newly nonempty owner evidence", async () => {
    const f = fixture(true);
    const report = await archiveStaleMeshEntries({ mesh: f.root, mode: "apply", now, afterSelection: () => {
      put(f.root, "actors/ended/actors.json", { format: 1, actors: [{ id: "b".repeat(32), rootId: f.owner, status: "running" }] });
    } });
    expect(Object.values(report.counts).every(n => n === 0)).toBe(true);
    for (const file of f.paths) expect(fs.existsSync(file)).toBe(true);
  });

  it("freshly rechecks a selected source's generation, even when replacement bytes are identical", async () => {
    const f = fixture(true), participant = f.paths[1]!, original = fs.readFileSync(participant);
    const report = await archiveStaleMeshEntries({ mesh: f.root, mode: "apply", now, afterSelection: () => {
      fs.renameSync(participant, `${participant}.retained`);
      put(f.root, path.relative(f.root, participant), original.toString("utf8"));
    } });
    expect(report.counts.participants).toBe(0);
    expect(fs.readFileSync(participant)).toEqual(original);
    expect(report.retained.join(" ")).toContain("changed");
  });

  it("does not repeatedly parse an unchanged whole file store for each owner", async () => {
    const f = fixture(), ownerCount = 16, marker = "archive-store-scan-budget-marker";
    for (let index = 1; index < ownerCount; index++) writeLease(f.root, lease(`host:file-dead-${index}`));
    const entries = Object.fromEntries(Array.from({ length: ownerCount * 32 }, (_, index) => {
      const key = `unrelated/${index}`;
      return [key, { key, version: 1, updatedAt: old, updatedBy: { id: "session:other", kind: "main" }, value: { marker } }];
    }));
    put(f.root, "state.json", { format: 1, entries });
    const parses = vi.spyOn(JSON, "parse");
    const result = await archiveStaleMeshEntries({ mesh: f.root, mode: "apply", now });
    expect(result.counts["host-leases"]).toBe(ownerCount);
    // Canonical strict reader plus this operation's indexed snapshot; never one
    // parse of every unrelated row for each archived owner.
    expect(parses.mock.calls.filter(call => String(call[0]).includes(marker)).length).toBeLessThanOrEqual(3);
    expect(fs.existsSync(path.join(f.root, "archive/host-leases", result.batch!, path.basename(f.paths[0]!)))).toBe(true);
  });

  it("does one whole-store SQLite scan and bounded direct owner queries, without per-group global relisting", async () => {
    const f = fixture(), ownerCount = 24;
    for (let index = 1; index < ownerCount; index++) writeLease(f.root, lease(`host:dead-${index}`));
    const { DatabaseSync } = createRequire(import.meta.url)("node:sqlite") as typeof import("node:sqlite");
    const db = new DatabaseSync(path.join(f.root, "state.db"));
    retainedDatabaseConnections.push(db);
    db.exec("PRAGMA journal_mode=WAL; CREATE TABLE meta(name TEXT PRIMARY KEY, value); CREATE TABLE kv(key TEXT PRIMARY KEY, value TEXT, version INTEGER, updated_at INTEGER, updated_by TEXT, bytes INTEGER)");
    const set = db.prepare("INSERT INTO meta(name,value) VALUES (?,?)");
    for (const [key, value] of [["schema", 1], ["backend", "sqlite"], ["epoch", 3], ["commit_no", 1], ["store_id", "scaling-store"]] as const) set.run(key, value);
    const insert = db.prepare("INSERT INTO kv VALUES (?, ?, ?, ?, ?, ?)");
    db.exec("BEGIN IMMEDIATE");
    for (let index = 0; index < ownerCount * 32; index++) insert.run(`unrelated/${index}`, JSON.stringify({ text: "noise" }), 1, old,
      JSON.stringify({ id: "session:other", kind: "main" }), 0);
    db.exec("COMMIT");
    put(f.root, "state.json", { format: "sqlite", movedTo: "state.db", backend: "sqlite", epoch: 3 });
    for (const suffix of ["", "-wal", "-shm"]) fs.chmodSync(path.join(f.root, `state.db${suffix}`), 0o600);
    const queries = vi.spyOn(DatabaseSync.prototype, "prepare"), lists = vi.spyOn(fs, "readdirSync");
    const result = await archiveStaleMeshEntries({ mesh: f.root, mode: "apply", now });
    expect(result.counts["host-leases"]).toBe(ownerCount);
    const kvQueries = queries.mock.calls.map(call => String(call[0])).filter(sql => /FROM kv\b/.test(sql));
    expect(kvQueries.filter(sql => !sql.includes("WHERE key = ?"))).toHaveLength(1);
    expect(kvQueries.filter(sql => sql.includes("WHERE key = ?")).length).toBeGreaterThanOrEqual(ownerCount * 2);
    // At most four exact keys per owner, before intent and again after its fsync.
    expect(kvQueries.length).toBeLessThanOrEqual(1 + ownerCount * 8);
    for (const directory of ["host-leases", "actors", "participants", "main-followups"]) {
      const scans = lists.mock.calls.filter(call => String(call[0]) === path.join(f.root, directory));
      expect(scans.length, `${directory}: no per-group global relist`).toBeLessThanOrEqual(2);
    }
    expect(fs.readFileSync(path.join(f.root, "archive/manifests", `${result.batch}.jsonl`), "utf8").split('"type":"done"').length - 1)
      .toBe(ownerCount);
  });

  it("retains pending nextTurn journals and only archives an owner/journal pair", async () => {
    const f = fixture(true);
    put(f.root, "main-followups/ended.json", { version: 1, items: [{ id: "pending", deliverAs: "nextTurn" }] });
    const report = await archiveStaleMeshEntries({ mesh: f.root, mode: "apply", now });
    expect(report.counts["main-followups"]).toBe(0);
    expect(fs.existsSync(path.join(f.root, "main-followups/ended.owner.json"))).toBe(true);
    expect(JSON.parse(fs.readFileSync(path.join(f.root, "main-followups/ended.json"), "utf8")).items).toHaveLength(1);
  });

  it("refuses restore collisions before changing anything", async () => {
    const f = fixture(true), applied = await archiveStaleMeshEntries({ mesh: f.root, mode: "apply", now });
    put(f.root, path.relative(f.root, f.paths[0]!), "new-owner-bytes");
    await expect(archiveStaleMeshEntries({ mesh: f.root, mode: "restore", batch: applied.batch! })).rejects.toThrow(/collision/);
    expect(fs.readFileSync(f.paths[0]!, "utf8")).toBe("new-owner-bytes");
    expect(fs.existsSync(f.paths[1]!)).toBe(false);
  });

  it("resumes restore after canonical link succeeds but archive unlink crashes", async () => {
    const f = fixture(), before = fs.readFileSync(f.paths[0]!);
    const applied = await archiveStaleMeshEntries({ mesh: f.root, mode: "apply", now });
    const archived = path.join(f.root, "archive/host-leases", applied.batch!, path.basename(f.paths[0]!));
    const unlink = fs.unlinkSync;
    vi.spyOn(fs, "unlinkSync").mockImplementationOnce(() => { throw new Error("injected restore crash"); });
    await expect(archiveStaleMeshEntries({ mesh: f.root, mode: "restore", batch: applied.batch! }))
      .rejects.toThrow("injected restore crash");
    fs.unlinkSync = unlink;
    expect(fs.readFileSync(f.paths[0]!)).toEqual(before);
    expect(fs.readFileSync(archived)).toEqual(before);
    expect(fs.lstatSync(f.paths[0]!).ino).toBe(fs.lstatSync(archived).ino);
    const resumed = await archiveStaleMeshEntries({ mesh: f.root, mode: "restore", batch: applied.batch! });
    expect(resumed.counts["host-leases"]).toBe(1);
    expect(fs.readFileSync(f.paths[0]!)).toEqual(before);
    expect(fs.existsSync(archived)).toBe(false);
  });

  it("keeps a different-inode same-byte canonical replacement as a restore collision", async () => {
    const f = fixture(), before = fs.readFileSync(f.paths[0]!);
    const applied = await archiveStaleMeshEntries({ mesh: f.root, mode: "apply", now });
    const archived = path.join(f.root, "archive/host-leases", applied.batch!, path.basename(f.paths[0]!));
    fs.writeFileSync(f.paths[0]!, before, { mode: 0o600 });
    expect(fs.readFileSync(f.paths[0]!)).toEqual(before);
    expect(fs.lstatSync(f.paths[0]!).ino).not.toBe(fs.lstatSync(archived).ino);
    await expect(archiveStaleMeshEntries({ mesh: f.root, mode: "restore", batch: applied.batch! }))
      .rejects.toThrow(/collision/);
    expect(fs.readFileSync(f.paths[0]!)).toEqual(before);
    expect(fs.readFileSync(archived)).toEqual(before);
  });

  it("resumes a multi-file restore when the later pair is left double-named", async () => {
    const f = fixture(true), before = bytes(f.paths);
    const applied = await archiveStaleMeshEntries({ mesh: f.root, mode: "apply", now });
    const archivedJournal = path.join(f.root, "archive/main-followups", applied.batch!, "ended.json");
    const unlink = vi.mocked(fs.unlinkSync).getMockImplementation()!;
    vi.mocked(fs.unlinkSync).mockImplementation(file => {
      if (String(file) === archivedJournal) throw new Error("injected multi-file restore crash");
      return unlink(file);
    });
    try {
      await expect(archiveStaleMeshEntries({ mesh: f.root, mode: "restore", batch: applied.batch! }))
        .rejects.toThrow("injected multi-file restore crash");
    } finally { vi.mocked(fs.unlinkSync).mockImplementation(unlink); }
    await archiveStaleMeshEntries({ mesh: f.root, mode: "restore", batch: applied.batch! });
    expect(bytes(f.paths)).toEqual(before);
  });

  it("does not overwrite an existing apply destination", async () => {
    const f = fixture();
    const target = `archive/host-leases/collision/${path.basename(f.paths[0]!)}`;
    put(f.root, target, "preexisting archive bytes");
    await expect(archiveStaleMeshEntries({ mesh: f.root, mode: "apply", batch: "collision", now })).rejects.toThrow(/collision/);
    expect(fs.readFileSync(path.join(f.root, target), "utf8")).toBe("preexisting archive bytes");
    expect(fs.existsSync(f.paths[0]!)).toBe(true);
  });

  it("rejects traversal injected through a manifest, not only CLI input", async () => {
    const f = fixture(), applied = await archiveStaleMeshEntries({ mesh: f.root, mode: "apply", now });
    const file = path.join(f.root, `archive/manifests/${applied.batch}.jsonl`);
    const rows = fs.readFileSync(file, "utf8").trimEnd().split("\n").map(line => JSON.parse(line));
    rows.find(row => row.type === "intent").files[0].relative = "host-leases/../../outside.json";
    fs.writeFileSync(file, `${rows.map(row => JSON.stringify(row)).join("\n")}\n`);
    await expect(archiveStaleMeshEntries({ mesh: f.root, mode: "restore", batch: applied.batch! })).rejects.toThrow(/Unsafe/);
  });

  it("refuses archive tampering on restore", async () => {
    const f = fixture(), applied = await archiveStaleMeshEntries({ mesh: f.root, mode: "apply", now });
    const archived = `archive/host-leases/${applied.batch}/${path.basename(f.paths[0]!)}`;
    put(f.root, archived, "tampered");
    await expect(archiveStaleMeshEntries({ mesh: f.root, mode: "restore", batch: applied.batch! })).rejects.toThrow(/identity/);
    expect(fs.existsSync(f.paths[0]!)).toBe(false);
  });

  it("recovers a crash after the no-replace link, without deleting either byte payload", async () => {
    const f = fixture(), unlink = fs.unlinkSync;
    vi.spyOn(fs, "unlinkSync").mockImplementationOnce(() => { throw new Error("injected move crash"); });
    await expect(archiveStaleMeshEntries({ mesh: f.root, mode: "apply", batch: "crash", now })).rejects.toThrow("injected move crash");
    fs.unlinkSync = unlink;
    const bytesBefore = fs.readFileSync(f.paths[0]!);
    // Recovery rolls back the incomplete pair of names; then re-selection may archive it afresh.
    const applied = await archiveStaleMeshEntries({ mesh: f.root, mode: "apply", now });
    expect(fs.readFileSync(path.join(f.root, "archive/manifests/crash.jsonl"), "utf8")).toContain("rolled-back");
    await archiveStaleMeshEntries({ mesh: f.root, mode: "restore", batch: applied.batch! });
    expect(fs.readFileSync(f.paths[0]!)).toEqual(bytesBefore);
  });

  it("rejects a different-inode same-byte source after an intent-only apply crash", async () => {
    const f = fixture(), before = fs.readFileSync(f.paths[0]!), link = fs.linkSync.bind(fs);
    vi.spyOn(fs, "linkSync").mockImplementation((from, to) => {
      if (String(from) === f.paths[0]) throw new Error("injected before source link");
      link(from, to);
    });
    await expect(archiveStaleMeshEntries({ mesh: f.root, mode: "apply", batch: "intent-only", now }))
      .rejects.toThrow("injected before source link");
    vi.mocked(fs.linkSync).mockRestore();
    fs.renameSync(f.paths[0]!, `${f.paths[0]}.original-retained`);
    fs.writeFileSync(f.paths[0]!, before, { mode: 0o600 });
    await expect(archiveStaleMeshEntries({ mesh: f.root, mode: "apply", now })).rejects.toThrow(/Recovery source changed/);
    expect(fs.readFileSync(f.paths[0]!)).toEqual(before);
    expect(fs.readFileSync(`${f.paths[0]}.original-retained`)).toEqual(before);
  });

  it("fully writes short byte-buffer receipts and durably records intent before any source link", async () => {
    const f = fixture(true);
    f.paths.push(writeLease(f.root, lease("host:é🚦")));
    const before = bytes(f.paths), originalSync = fs.fsyncSync.bind(fs), link = fs.linkSync.bind(fs);
    let intentFd: number | undefined, durableIntent = false, links = 0;
    // A separate fd observer proves the intent's fsync precedes each move.
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => { originalSync(fd); if (fd === intentFd) durableIntent = true; });
    const open = fs.openSync.bind(fs);
    vi.spyOn(fs, "openSync").mockImplementation(((...args: Parameters<typeof fs.openSync>) => {
      const fd = Reflect.apply(open, fs, args);
      if (String(args[0]).endsWith(".jsonl")) intentFd = fd;
      return fd;
    }) as typeof fs.openSync);
    const writes = receiptWrites(call => {
      if (call.type === "intent" && call.offset === 0) durableIntent = false;
      return call.write(Math.min(7, call.length));
    });
    vi.spyOn(fs, "linkSync").mockImplementation((from, to) => {
      if (String(to).includes(`${path.sep}archive${path.sep}`)) { expect(durableIntent).toBe(true); links++; }
      link(from, to);
    });
    const applied = await archiveStaleMeshEntries({ mesh: f.root, mode: "apply", batch: "short-writes", now });
    expect(links).toBe(f.paths.length);
    expect(writes.mock.calls.length).toBeGreaterThan(20);
    writes.mockRestore();
    expect((await archiveStaleMeshEntries({ mesh: f.root, mode: "apply", now })).counts["host-leases"]).toBe(0);
    await archiveStaleMeshEntries({ mesh: f.root, mode: "restore", batch: applied.batch! });
    expect(bytes(f.paths)).toEqual(before);
  });

  it.each(["intent-zero", "intent-throw", "done-zero", "done-throw"])("recovers %s without losing earlier complete intents or torn trace bytes", async failure => {
    const f = fixture(true), before = bytes(f.paths), batch = `torn-${failure}`;
    const targetType = failure.startsWith("intent") ? "intent" : "done";
    let seen = 0, target: Buffer | undefined, failed = false;
    const writes = receiptWrites(call => {
      if (call.type === targetType && call.offset === 0 && !target && ++seen === 2) target = call.bytes;
      if (!failed && target?.equals(call.bytes)) {
        if (call.offset === 0) return call.write(failure.endsWith("throw") ? call.length - 1 : 13);
        failed = true;
        if (failure.endsWith("throw")) throw new Error("injected receipt failure");
        return 0;
      }
      return call.write(call.length);
    });
    try {
      await expect(archiveStaleMeshEntries({ mesh: f.root, mode: "apply", batch, now })).rejects.toThrow(/receipt/);
    } finally { writes.mockRestore(); }
    expect(failed).toBe(true);
    const manifestFile = path.join(f.root, "archive/manifests", `${batch}.jsonl`);
    const damaged = fs.readFileSync(manifestFile), completeEnd = damaged.lastIndexOf(0x0a) + 1;
    const complete = damaged.subarray(0, completeEnd);
    expect(complete.toString("utf8")).toContain('"type":"done"'); // Prior actor intent/completion survives.
    expect(fs.existsSync(f.paths[4]!)).toBe(false);
    if (targetType === "intent") expect(fs.existsSync(f.paths[1]!)).toBe(true); // No unauthorized participant move.
    else expect(fs.existsSync(f.paths[1]!)).toBe(false); // Complete intent, torn completion: rollback then reselect.
    const retry = await archiveStaleMeshEntries({ mesh: f.root, mode: "apply", batch: `retry-${failure}`, now });
    expect(retry.counts).toEqual({ "host-leases": 1, participants: 1, "main-followups": 2, actors: 0 });
    await archiveStaleMeshEntries({ mesh: f.root, mode: "restore", batch: retry.batch! });
    await archiveStaleMeshEntries({ mesh: f.root, mode: "restore", batch });
    expect(bytes(f.paths)).toEqual(before);
    const repaired = fs.readFileSync(manifestFile);
    expect(repaired.subarray(0, complete.byteLength)).toEqual(complete);
    const retained = fs.readdirSync(path.dirname(manifestFile)).filter(name => name.startsWith(`${batch}.retained-tail-`));
    expect(retained.some(name => fs.readFileSync(path.join(path.dirname(manifestFile), name)).equals(damaged))).toBe(true);
    expect((await archiveStaleMeshEntries({ mesh: f.root, mode: "restore", batch })).counts)
      .toEqual({ "host-leases": 0, participants: 0, "main-followups": 0, actors: 0 });
  });

  it("does not trim or repair a torn trailing receipt during noncustodial restore preflight", async () => {
    const f = fixture(), applied = await archiveStaleMeshEntries({ mesh: f.root, mode: "apply", batch: "preflight-tail", now });
    const manifestFile = path.join(f.root, "archive/manifests/preflight-tail.jsonl");
    fs.appendFileSync(manifestFile, '{"type":"restoring","id":"uncommitted-tail"}');
    const damaged = fs.readFileSync(manifestFile);
    put(f.root, path.relative(f.root, f.paths[0]!), "canonical replacement collision");
    const trims = vi.spyOn(fs, "ftruncateSync");
    await expect(archiveStaleMeshEntries({ mesh: f.root, mode: "restore", batch: applied.batch! })).rejects.toThrow(/collision/);
    expect(trims).not.toHaveBeenCalled();
    expect(fs.readFileSync(manifestFile)).toEqual(damaged);
    expect(fs.readdirSync(path.dirname(manifestFile)).some(name => name.includes("retained-tail"))).toBe(false);
  });

  it("rejects complete interior corruption without trimming any trace bytes", async () => {
    const f = fixture(), before = fs.readFileSync(f.paths[0]!);
    const applied = await archiveStaleMeshEntries({ mesh: f.root, mode: "apply", batch: "interior", now });
    const manifestFile = path.join(f.root, "archive/manifests/interior.jsonl");
    fs.appendFileSync(manifestFile, '{"type":"done",BROKEN}\n{"type":"done"');
    const damaged = fs.readFileSync(manifestFile);
    await expect(archiveStaleMeshEntries({ mesh: f.root, mode: "apply", now })).rejects.toThrow();
    await expect(archiveStaleMeshEntries({ mesh: f.root, mode: "restore", batch: applied.batch! })).rejects.toThrow();
    expect(fs.readFileSync(manifestFile)).toEqual(damaged);
    expect(fs.existsSync(f.paths[0]!)).toBe(false);
    expect(fs.readFileSync(path.join(f.root, "archive/host-leases/interior", path.basename(f.paths[0]!)))).toEqual(before);
  });

  it("rejects symlink roots, source entries, archive parents and traversal batch names", async () => {
    const f = fixture(), rootLink = path.join(tmp, "mesh-link");
    fs.symlinkSync(f.root, rootLink, "dir");
    await expect(archiveStaleMeshEntries({ mesh: rootLink, mode: "dry-run" })).rejects.toThrow(/Symlink/);
    await expect(archiveStaleMeshEntriesCli(["--mesh", f.root, "--restore", "../outside"])).rejects.toThrow(/batch/);
    await expect(archiveStaleMeshEntriesCli(["--mesh", `${f.root}/../outside`, "--apply"])).rejects.toThrow(/traversal/);
    const destination = path.join(tmp, "outside"); fs.mkdirSync(destination);
    fs.symlinkSync(destination, path.join(f.root, "archive"), "dir");
    await expect(archiveStaleMeshEntries({ mesh: f.root, mode: "apply", now })).rejects.toThrow(/Symlink/);
    expect(fs.readdirSync(destination)).toEqual([]);
    fs.renameSync(path.join(f.root, "archive"), path.join(f.root, "archive-link-retained"));
    fs.renameSync(f.paths[0]!, `${f.paths[0]}.retained`);
    fs.symlinkSync(`${f.paths[0]}.retained`, f.paths[0]!);
    await expect(archiveStaleMeshEntries({ mesh: f.root, mode: "dry-run", now })).rejects.toThrow(/Symlink/);
  });

  it("requires exactly one explicit mode and mesh; checks every helper deadline", async () => {
    for (const argv of [[], ["--apply"], ["--mesh", tmp], ["--mesh", tmp, "--apply", "--dry-run"]]) {
      await expect(archiveStaleMeshEntriesCli(argv)).rejects.toThrow();
    }
    const value = lease();
    expect(hostLeasePastGrace(value, now - STALE_MESH_GRACE_MS)).toBe(true);
    expect(hostLeasePastGrace({ ...value, reloadUntil: now }, now - STALE_MESH_GRACE_MS)).toBe(false);
    expect(hostLeasePastGrace({ ...value, session: { ...value.session!, updatedAt: now } }, now - STALE_MESH_GRACE_MS)).toBe(false);
  });
});
