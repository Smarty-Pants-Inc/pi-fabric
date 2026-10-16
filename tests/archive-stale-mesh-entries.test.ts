import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createRequire } from "node:module";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

// Data-safety integration tests: real bytes, selection, publication, manifest and
// restore. Intercept filesystem removal, not the cyclic fence-lock module: the
// actual pinned lock release must run and its receipt must remain byte-identical.
// Every fixture stays below the inherited absolute private TMPDIR ceiling.

import { archiveStaleMeshEntries, archiveStaleMeshEntriesCli, hostLeasePastGrace, STALE_MESH_GRACE_MS } from "../src/topology/stale-mesh-entries.js";
import { hostLeasePath, type FabricHostLease } from "../src/topology/host-leases.js";
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

  it("does one whole-store SQLite scan and bounded direct owner queries, without global relisting per group", async () => {
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
    expect(kvQueries.filter(sql => sql.includes("WHERE key = ?")).length).toBeGreaterThanOrEqual(ownerCount);
    expect(kvQueries.length).toBeLessThanOrEqual(1 + ownerCount * 4);
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
