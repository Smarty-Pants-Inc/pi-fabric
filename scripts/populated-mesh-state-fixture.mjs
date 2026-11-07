#!/usr/bin/env node
// Synthetic shape preparation only. No workers, topology simulation, or existing-root input.
// Call before admitting workers, under the caller's owned process group and deadline.
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import { pathToFileURL } from "node:url";

const STATE_LIMIT = 32 * 1024 * 1024, VALUE_LIMIT = 65536, TOMBSTONE_LIMIT = 1000;
const IDENTITY = Object.freeze({ id: "fixture-seeder", name: "Synthetic fixture", kind: "agent" });
const encoded = value => {
  const json = JSON.stringify(value);
  assert(json !== undefined, "value must be JSON encodable");
  return Buffer.byteLength(json);
};
const digest = value => createHash("sha256").update(value).digest("hex");
const integer = (value, name, max = Number.MAX_SAFE_INTEGER) => {
  assert(Number.isSafeInteger(value) && value >= 0 && value <= max, `invalid ${name}`);
  return value;
};
const add = (a, b) => integer(a + b, "sum");
const tableNames = ["kv", "changes", "tombstones", "meta"];

function parseProfile(profile) {
  assert(profile && profile.version === 1, "unsupported profile version");
  const tables = {};
  for (const name of tableNames) {
    const table = profile.tables?.[name];
    assert(table && Array.isArray(table.prefixes), `missing ${name} table/prefixes`);
    integer(table.rows, `${name} rows`, 100000);
    const seen = new Set();
    let rows = 0, bytes = 0;
    for (const group of table.prefixes) {
      assert(typeof group.prefix === "string" && group.prefix.length > 0 && group.prefix.length <= 256
        && !group.prefix.includes("/") && !seen.has(group.prefix), `invalid/duplicate ${name} prefix`);
      seen.add(group.prefix);
      integer(group.rows, `${name} prefix rows`, 100000);
      assert(group.rows > 0, "empty listed group");
      integer(group.valueBytes, `${name} value budget`, STATE_LIMIT);
      if (name === "kv") assert(group.valueBytes >= group.rows * 2, "value budget below JSON string minimum");
      rows = add(rows, group.rows); bytes = add(bytes, group.valueBytes);
    }
    assert(rows <= table.rows, `${name} listed rows exceed total`);
    for (const [field, actual] of [["listedPrefixRows", rows], ["unlistedRows", table.rows - rows],
      ["aggregateListedValueBytes", bytes]]) {
      if (table[field] !== undefined) assert.equal(table[field], actual, `${name} ${field} mismatch`);
    }
    if (name !== "kv") assert.equal(rows, table.rows, `${name} unlisted rows unsupported`);
    else assert(bytes + (table.rows - rows) * 2 <= STATE_LIMIT, "32 MiB aggregate value budget exceeded");
    tables[name] = { rows: table.rows, prefixes: table.prefixes.map(group => ({ ...group })), unlisted: table.rows - rows };
  }
  assert(tables.tombstones.rows <= TOMBSTONE_LIMIT, "tombstone cap exceeded");
  assert(tables.kv.rows > 0, "at least one live row required");
  const source = new Set(tables.kv.prefixes.map(group => group.prefix));
  for (const name of ["changes", "tombstones"]) for (const group of tables[name].prefixes) {
    assert(source.has(group.prefix), `${name} unmapped group`);
  }
  return tables;
}

/** Exact JSON VALUE budgets, not SQL kv.bytes. Overlays are caller-authored semantics.
 * Optional identity is inert synthetic envelope metadata, never live ownership.
 * pad:true explicitly permits adding inert fixturePadding to an archival object;
 * the helper never infers archive status or changes semantic fields. */
export function buildStateSeed(profile, { now = 0, overlays = [] } = {}) {
  integer(now, "timestamp");
  assert(Array.isArray(overlays), "overlays must be an array");
  const tables = parseProfile(profile), reserved = new Set(tables.kv.prefixes.map(group => group.prefix));
  const allocations = tables.kv.prefixes.map((group, ordinal) => {
    const aliased = !/^[a-z][a-z0-9]*(?:-[a-z0-9]+)*$/.test(group.prefix) || /\d{6}/.test(group.prefix);
    let namespace = group.prefix;
    if (aliased) {
      namespace = `fixture-ns-${ordinal}`;
      while (reserved.has(namespace)) namespace += "-x";
      reserved.add(namespace);
    }
    return { ordinal, namespace, aliased, rows: group.rows, valueBytes: group.valueBytes,
      overlayRows: 0, overlayBytes: 0, paddingBytes: 0, fillerRows: 0 };
  });
  const mapping = new Map(tables.kv.prefixes.map((group, index) => [group.prefix, allocations[index]]));
  let unknownNamespace = "fixture-unlisted";
  while (reserved.has(unknownNamespace)) unknownNamespace += "-x";
  const entries = Object.create(null), versions = Object.create(null), tombstoneOrder = [];
  const overlayGroups = new Map(allocations.map(group => [group.namespace, []]));
  const keys = new Set();
  for (const overlay of overlays) {
    assert(overlay && typeof overlay.key === "string" && overlay.key.length <= 2048
      && overlay.key.includes("/") && !/[\x00-\x1f]/.test(overlay.key), "invalid overlay key");
    assert(overlay.pad === undefined || typeof overlay.pad === "boolean", "pad must be boolean");
    const slash = overlay.key.indexOf("/"), group = mapping.get(overlay.key.slice(0, slash));
    assert(group, "unmapped overlay group");
    const key = group.namespace + overlay.key.slice(slash);
    assert(!keys.has(key), "duplicate overlay key"); keys.add(key);
    // Round trip rejects cycles/undefined and severs caller ownership. Measure UTF-8, not string length.
    const value = JSON.parse(JSON.stringify(overlay.value) ?? "null");
    assert(overlay.value !== undefined && encoded(value) === encoded(overlay.value), "invalid overlay JSON");
    const valueBytes = encoded(value);
    assert(valueBytes <= VALUE_LIMIT, "oversized overlay value");
    group.overlayRows++;
    group.overlayBytes = add(group.overlayBytes, valueBytes);
    assert(group.overlayRows <= group.rows, "overlay count exceeds group");
    assert(group.overlayBytes <= group.valueBytes, "overlay bytes exceed group");
    // Match ordinary MeshIdentity fields (event-log.ts), retaining an optional sessionId.
    // JSON capture, like captureStoragePut, severs caller ownership before any async work.
    const identity = overlay.identity === undefined ? IDENTITY : JSON.parse(JSON.stringify(overlay.identity) ?? "null");
    assert(identity && typeof identity === "object" && !Array.isArray(identity)
      && typeof identity.id === "string" && identity.id.length > 0 && typeof identity.name === "string"
      && ["main", "actor", "agent"].includes(identity.kind)
      && (identity.sessionId === undefined || typeof identity.sessionId === "string"), "invalid overlay identity");
    assert(identity.verified === undefined, "synthetic overlay identity cannot claim bridge verification");
    overlayGroups.get(group.namespace).push({ key, value, identity, pad: overlay.pad === true });
  }
  let revision = 0, allocatedStateBytes = 0;
  const insert = (key, value, identity = IDENTITY) => {
    assert(!Object.hasOwn(entries, key) && !Object.hasOwn(versions, key), "duplicate seed key");
    const valueBytes = encoded(value);
    assert(valueBytes <= VALUE_LIMIT, "value exceeds 64 KiB");
    allocatedStateBytes += valueBytes + encoded(identity) + 3 * Buffer.byteLength(key) + 96;
    assert(allocatedStateBytes + 256 <= STATE_LIMIT, "32 MiB state budget exceeded during allocation");
    revision = add(revision, 1);
    entries[key] = { key, value, version: revision, updatedAt: now, updatedBy: { ...identity } };
    versions[key] = revision;
  };
  for (const group of allocations) {
    const supplied = overlayGroups.get(group.namespace);
    group.fillerRows = group.rows - supplied.length;
    let remaining = group.valueBytes - group.overlayBytes;
    if (!group.fillerRows && remaining) {
      // Empty property costs bytes too; use only explicitly permitted objects, without overwriting fields.
      for (const item of supplied.filter(item => item.pad)) {
        assert(item.value && typeof item.value === "object" && !Array.isArray(item.value)
          && !Object.hasOwn(item.value, "fixturePadding"), "padding requires an archival object without fixturePadding");
        const original = encoded(item.value), empty = { ...item.value, fixturePadding: "" };
        const overhead = encoded(empty) - original;
        if (remaining < overhead) continue;
        const growth = Math.min(remaining, VALUE_LIMIT - original);
        if (growth < overhead) continue;
        item.value = { ...item.value, fixturePadding: "x".repeat(growth - overhead) };
        remaining -= growth; group.paddingBytes += growth;
        if (!remaining) break;
      }
      assert.equal(remaining, 0, "unspent bytes with zero filler rows; explicit archival padding required");
    }
    assert(!group.fillerRows || remaining >= group.fillerRows * 2, "remaining filler budget too small");
    for (const item of supplied) insert(item.key, item.value, item.identity);
    const quotient = group.fillerRows ? Math.floor(remaining / group.fillerRows) : 0;
    for (let i = 0, slot = 0; i < group.fillerRows; i++) {
      let key;
      do { key = `${group.namespace}/fixture/${String(slot++).padStart(6, "0")}`; } while (keys.has(key));
      const length = quotient + (i < remaining % group.fillerRows ? 1 : 0);
      assert(length <= VALUE_LIMIT, "filler value exceeds 64 KiB");
      insert(key, "x".repeat(length - 2));
    }
  }
  for (let i = 0; i < tables.kv.unlisted; i++) insert(`${unknownNamespace}/fixture/${String(i).padStart(6, "0")}`, "");
  for (const group of tables.tombstones.prefixes) for (let i = 0; i < group.rows; i++) {
    const key = `${mapping.get(group.prefix).namespace}/fixture-deleted/${String(i).padStart(6, "0")}`;
    assert(!Object.hasOwn(versions, key), "live/tombstone key overlap");
    allocatedStateBytes += 2 * Buffer.byteLength(key) + 32;
    assert(allocatedStateBytes + 256 <= STATE_LIMIT, "32 MiB tombstone/state budget exceeded");
    versions[key] = revision = add(revision, 1); tombstoneOrder.push(key);
  }
  const state = { format: 1, revisionFormat: 2, readGeneration: "00000000-0000-4000-8000-000000000001",
    entries, versions, tombstoneOrder, highWater: revision };
  assert.equal(Object.keys(entries).length, tables.kv.rows, "live count mismatch");
  assert.equal(new Set(tombstoneOrder).size, tables.tombstones.rows, "tombstone order mismatch");
  assert.equal(Object.keys(versions).length, tables.kv.rows + tables.tombstones.rows, "version count mismatch");
  let stateBytes = 0;
  for (const [key, entry] of Object.entries(entries)) {
    assert(Number.isSafeInteger(entry.version) && entry.version > 0 && entry.version === versions[key]
      && entry.version <= state.highWater && entry.key === key, "invalid live revision");
    stateBytes += encoded(entry.value) + encoded(entry.updatedBy) + 3 * Buffer.byteLength(key) + 96;
  }
  for (const key of tombstoneOrder) {
    assert(!Object.hasOwn(entries, key) && versions[key] > 0 && Number.isSafeInteger(versions[key])
      && versions[key] <= state.highWater, "invalid tombstone revision");
    stateBytes += 2 * Buffer.byteLength(key) + 32;
  }
  for (const group of allocations) {
    const actual = Object.values(entries).filter(entry => entry.key.startsWith(group.namespace + "/"));
    assert.equal(actual.length, group.rows);
    assert.equal(actual.reduce((sum, entry) => sum + encoded(entry.value), 0), group.valueBytes, "encoded budget mismatch");
  }
  assert.equal(stateBytes, allocatedStateBytes, "incremental state accounting mismatch");
  const inputBytes = encoded(state);
  assert(inputBytes <= STATE_LIMIT && stateBytes + 256 <= STATE_LIMIT, "32 MiB state/decoder budget exceeded");
  const prefixShape = name => tables[name].prefixes.map(group => ({ namespace: mapping.get(group.prefix).namespace, rows: group.rows }));
  const manifest = { format: 1, allocations, expected: { kv: tables.kv.rows, changes: tables.changes.rows,
    tombstones: tables.tombstones.rows, meta: tables.meta.rows, changesPrefixes: prefixShape("changes"),
    tombstonePrefixes: prefixShape("tombstones"), unlisted: { namespace: unknownNamespace, rows: tables.kv.unlisted,
      valueBytes: tables.kv.unlisted * 2 } }, inputBytes, stateBytes, initialHighWater: revision,
    maxValueBytes: Math.max(...Object.values(entries).map(entry => encoded(entry.value))),
    assumptions: ["Ballast matches logical shape, not semantic runtime work.",
      "Unlisted live rows use empty strings: two JSON value bytes each, not recovered source bytes.",
      "Namespace mappings are by source prefix ordinal; identifier-like labels are replaced.",
      "pad:true is caller permission for archival fixturePadding, not inferred live authority.",
      "Metadata values/digests are genuine migration history, not source byte-budget replicas."] };
  return { state, manifest };
}

const owned = new WeakMap();
let preparing = false;
const identity = (file, directory = false) => {
  const stat = fs.lstatSync(file, { bigint: true });
  assert(!stat.isSymbolicLink() && (directory ? stat.isDirectory() : stat.isFile()), "unsafe fixture path type");
  assert.equal(stat.uid, BigInt(process.getuid()), "fixture owner mismatch");
  assert.equal(stat.mode & 0o777n, directory ? 0o700n : 0o600n, "fixture permissions must be private");
  assert.equal(fs.realpathSync(file), file, "fixture path must be canonical");
  return Object.freeze({ file, device: String(stat.dev), inode: String(stat.ino) });
};
const same = (a, b) => a.file === b.file && a.device === b.device && a.inode === b.inode;
function attest(fixture) {
  const record = owned.get(fixture);
  assert(record && !record.closed, "only a helper-returned, open fixture is accepted");
  for (const [id, fd] of [[record.parent, record.parentFd], [record.root, record.rootFd]]) {
    assert(same(identity(id.file, true), id), "fixture directory identity changed");
    const stat = fs.fstatSync(fd, { bigint: true });
    assert(String(stat.dev) === id.device && String(stat.ino) === id.inode, "fixture directory pin changed");
  }
  assert.equal(fixture.root, record.root.file, "fixture root changed");
  assert(same(identity(record.database.file), record.database), "fixture DB identity changed");
  return record;
}
function save(file, value, raw = false) {
  const fd = fs.openSync(file, fs.constants.O_CREAT | fs.constants.O_EXCL | fs.constants.O_WRONLY | fs.constants.O_NOFOLLOW, 0o600);
  try { fs.writeFileSync(fd, raw ? value : JSON.stringify(value, null, 2) + "\n"); } finally { fs.closeSync(fd); }
}
const physical = file => {
  const stat = fs.lstatSync(file, { bigint: true, throwIfNoEntry: false });
  if (!stat) return { bytes: 0, allocatedBytes: 0, present: false };
  assert(stat.isFile() && !stat.isSymbolicLink(), "unsafe physical observation");
  return { bytes: Number(stat.size), allocatedBytes: Number(stat.blocks) * 512, present: true };
};
const pragmas = db => Object.fromEntries(["journal_mode", "synchronous", "wal_autocheckpoint", "journal_size_limit", "busy_timeout"]
  .map(name => [name, Object.values(db.prepare(`PRAGMA ${name}`).get())[0]]));

/** Short read-only snapshot, no checkpoint, no pinned reader after return. Paths/DB identity
 * come exclusively from the in-process ownership registry, never from an external root. */
export function inspectPopulatedMesh(fixture) {
  const record = attest(fixture), { DatabaseSync } = process.getBuiltinModule("node:sqlite");
  const db = new DatabaseSync(record.database.file, { readOnly: true, timeout: 0 });
  try {
    assert.equal(db.isTransaction, false);
    db.exec("BEGIN");
    const counts = Object.fromEntries(tableNames.map(name => [name, db.prepare(`SELECT count(*) AS n FROM ${name}`).get().n]));
    const prefixes = Object.fromEntries(["kv", "changes", "tombstones"].map(name => [name,
      db.prepare(`SELECT substr(key,1,instr(key,'/')-1) AS namespace,count(*) AS rows${name === "kv"
        ? ",sum(length(CAST(value AS BLOB))) AS valueBytes" : ""} FROM ${name} GROUP BY 1 ORDER BY 1`).all().map(row => ({ ...row }))]));
    const meta = Object.fromEntries(db.prepare("SELECT name,value FROM meta ORDER BY name").all().map(row => [row.name, row.value]));
    const invalid = db.prepare(`SELECT
      (SELECT count(*) FROM kv JOIN tombstones USING(key)) AS overlap,
      (SELECT count(*) FROM (SELECT version FROM kv UNION ALL SELECT version FROM tombstones UNION ALL SELECT version FROM changes)
       WHERE typeof(version)<>'integer' OR version<=0 OR version>9007199254740991 OR version>?) AS revisions,
      (SELECT count(*) FROM kv WHERE bytes<>length(CAST(value AS BLOB))+length(CAST(updated_by AS BLOB))+3*length(CAST(key AS BLOB))+96) AS accounting,
      (SELECT count(*) FROM changes WHERE deleted NOT IN (0,1) OR seq<=0 OR commit_no<=0) AS changes`).get(Number(meta.high_water));
    const feed = { ...db.prepare("SELECT min(seq) AS firstSeq,max(seq) AS lastSeq,min(commit_no) AS firstCommit,max(commit_no) AS lastCommit,count(DISTINCT commit_no) AS commits,sum(deleted) AS deleted FROM changes").get() };
    const tombstones = { ...db.prepare("SELECT count(DISTINCT ord) AS ords,min(ord) AS firstOrd,max(ord) AS lastOrd FROM tombstones").get() };
    const recomputedStateBytes = db.prepare(`SELECT (SELECT coalesce(sum(bytes),0) FROM kv)
      +(SELECT coalesce(sum(2*length(CAST(key AS BLOB))+32),0) FROM tombstones) AS n`).get().n;
    const values = createHash("sha256");
    for (const row of db.prepare("SELECT key,value,updated_by FROM kv ORDER BY key").all()) values.update(JSON.stringify(row) + "\n");
    const tombstoneDigest = digest(JSON.stringify(db.prepare("SELECT key,version,ord FROM tombstones ORDER BY ord").all()));
    const logical = { counts, prefixes, meta, feed, tombstones, invalid: { ...invalid }, recomputedStateBytes,
      valueDigest: values.digest("hex"), tombstoneDigest };
    db.exec("COMMIT");
    assert.equal(db.isTransaction, false);
    const observation = { database: { ...record.database }, logical, logicalDigest: digest(JSON.stringify(logical)),
      sqlite: db.prepare("SELECT sqlite_version() AS version").get().version,
      pages: Object.fromEntries(["page_size", "page_count", "freelist_count", "auto_vacuum"].map(name =>
        [name, Object.values(db.prepare(`PRAGMA ${name}`).get())[0]])),
      observerConnectionPolicy: pragmas(db), productionConnectionPolicies: record.policies.map(policy => ({ ...policy })),
      observerTotalChanges: db.prepare("SELECT total_changes() AS n").get().n };
    assert(same(identity(record.database.file), record.database), "DB identity changed during inspection");
    return { ...observation, physical: Object.fromEntries(["state.db", "state.db-wal", "state.db-shm", "events.jsonl"]
      .map(name => [name, physical(path.join(record.root.file, name))])), phase: "short readonly snapshot; no checkpoint requested" };
  } finally { if (db.isTransaction) db.exec("ROLLBACK"); db.close(); }
}

function assertTarget(fixture, snapshot) {
  const { expected, allocations } = fixture.manifest, { logical } = snapshot;
  for (const name of tableNames) assert.equal(logical.counts[name], expected[name], `${name} target mismatch`);
  const expectedKv = [...allocations.map(({ namespace, rows, valueBytes }) => ({ namespace, rows, valueBytes })),
    ...(expected.unlisted.rows ? [expected.unlisted] : [])].sort((a, b) => a.namespace.localeCompare(b.namespace));
  const sort = rows => [...rows].sort((a, b) => a.namespace.localeCompare(b.namespace));
  assert.deepEqual(sort(logical.prefixes.kv), sort(expectedKv), "KV prefix/encoded value shape mismatch");
  assert.deepEqual(sort(logical.prefixes.changes), sort(expected.changesPrefixes), "surviving change prefixes mismatch");
  assert.deepEqual(sort(logical.prefixes.tombstones), sort(expected.tombstonePrefixes), "tombstone prefixes mismatch");
  assert(Object.values(logical.invalid).every(value => value === 0), "invalid logical state");
  assert.equal(logical.feed.deleted, 0, "pre-worker seed feed must contain only surviving puts");
  assert.equal(logical.recomputedStateBytes, fixture.manifest.stateBytes);
  assert.equal(Number(logical.meta.state_bytes), logical.recomputedStateBytes);
  assert.equal(logical.tombstones.ords, expected.tombstones);
}

/** parent must be a fresh EMPTY caller-created 0700 directory beneath the OS temp parent.
 * Preparation temporarily installs an allowlisted private environment; call serially before workers.
 * fixture.close() releases directory pins only; the caller owns evidence retention/fixture removal. */
export async function createPopulatedMesh({ candidate, parent, profile, overlays, eventBytes = 56000000, eventLineBytes = 2048, ...unexpected }) {
  assert.equal(Object.keys(unexpected).length, 0, "unexpected options; no existing root accepted");
  assert(!preparing, "fixture preparation must be serial and before workers");
  const { state, manifest } = buildStateSeed(profile, { overlays });
  integer(eventBytes, "event bytes", 60000000);
  integer(eventLineBytes, "event line bytes", VALUE_LIMIT);
  assert(eventLineBytes >= 512 && eventLineBytes < VALUE_LIMIT, "event line budget must be 512..65535");
  assert.equal(manifest.expected.meta, 17, "ordinary migration history produces 17 metadata rows");
  assert(manifest.expected.changes >= 4096 && manifest.expected.changes < 4160, "unsupported retained-feed target");
  assert(typeof parent === "string" && path.isAbsolute(parent), "explicit scratch parent required");
  const base = fs.realpathSync(os.tmpdir()), relative = path.relative(base, parent);
  assert(relative && relative !== ".." && !relative.startsWith(".." + path.sep) && !path.isAbsolute(relative), "parent must be beneath the temp parent");
  const parentId = identity(parent, true);
  assert.equal(fs.readdirSync(parent).length, 0, "scratch parent must be fresh and empty");
  candidate = fs.realpathSync(candidate);
  const candidateFiles = ["dist/mesh.js", "dist/mesh/mesh-backend-cli.js", "bin/fabric-mesh-backend"];
  manifest.candidate = { files: Object.fromEntries(candidateFiles.map(file => [file, digest(fs.readFileSync(path.join(candidate, file)))])), node: process.version };
  const parentFd = fs.openSync(parent, fs.constants.O_RDONLY | fs.constants.O_DIRECTORY | fs.constants.O_NOFOLLOW);
  const envBefore = process.env;
  let fixture, record, store, restoreCapture;
  preparing = true;
  const deadline = Date.now() + 180000;
  const checkTime = () => assert(Date.now() < deadline, "fixture preparation deadline exceeded");
  const evidence = path.join(parent, "evidence");
  try {
    fs.mkdirSync(evidence, { mode: 0o700 });
    for (const name of ["home", "profile", "project", "runs", "exports"]) fs.mkdirSync(path.join(parent, name), { mode: 0o700 });
    const env = { PATH: envBefore.PATH, HOME: path.join(parent, "home"), TMPDIR: parent,
      XDG_CONFIG_HOME: path.join(parent, "home", "config"), XDG_STATE_HOME: path.join(parent, "home", "state"),
      XDG_CACHE_HOME: path.join(parent, "home", "cache"), PI_CODING_AGENT_DIR: path.join(parent, "profile"), PI_OFFLINE: "1",
      PI_FABRIC_PROJECT_ROOT: path.join(parent, "project"), PI_FABRIC_RUN_ROOT: path.join(parent, "runs"),
      PI_FABRIC_AGENT_DIR: path.join(parent, "exports"), PI_FABRIC_MESH_STATE_BACKEND: "sqlite" };
    process.env = env;
    const seed = path.join(parent, "seed.json"); save(seed, JSON.stringify(state), true);
    const receipts = [];
    const receipt = (step, result) => {
      const value = { step, at: new Date().toISOString(), result };
      const file = path.join(evidence, `${String(receipts.length).padStart(2, "0")}-${step}.json`);
      save(file, value); receipts.push({ step, file }); return result;
    };
    const cli = (step, args) => {
      checkTime(); if (fixture) attest(fixture);
      assert(same(identity(parent, true), parentId), "scratch parent changed");
      const argv = [path.join(candidate, "bin/fabric-mesh-backend"), ...args, "--json"];
      const done = spawnSync(process.execPath, argv, { env, cwd: parent, encoding: "utf8", timeout: 30000,
        maxBuffer: 4 * 1024 * 1024, stdio: ["ignore", "pipe", "pipe"] });
      save(path.join(evidence, step + ".stdout"), done.stdout ?? "", true);
      save(path.join(evidence, step + ".stderr"), done.stderr ?? "", true);
      assert(!done.error && done.status === 0 && done.signal === null, `${step} CLI failed: ${done.error ?? done.stderr}`);
      const result = JSON.parse(done.stdout);
      receipt(step, { command: argv, exitCode: done.status, output: result });
      assert.equal(result.ok, true, `${step} failed`); return result;
    };
    const imported = cli("scratch-import", ["import", "--scratch-proof-from", seed, "--scratch-parent", parent]);
    assert.equal(imported.scratchProof, true);
    assert(typeof imported.root === "string" && path.dirname(imported.root) === parent, "CLI must create a direct private child");
    const root = identity(imported.root, true), database = identity(path.join(root.file, "state.db"));
    const rootFd = fs.openSync(root.file, fs.constants.O_RDONLY | fs.constants.O_DIRECTORY | fs.constants.O_NOFOLLOW);
    record = { parent: parentId, parentFd, root, rootFd, database, policies: [], closed: false };
    fixture = { root: root.file, manifest, receipts, ownership: { parent: parentId, root, database }, databaseIdentity: database,
      close() { if (!record.closed) { record.closed = true; fs.closeSync(record.rootFd); fs.closeSync(record.parentFd); } } };
    owned.set(fixture, record);
    const initial = inspectPopulatedMesh(fixture);
    assert.equal(initial.logical.counts.meta, 13, "fresh import metadata mismatch");
    assert.equal(initial.logical.counts.tombstones, manifest.expected.tombstones);
    receipt("import-shape", initial);
    env.PI_FABRIC_MESH_ROOT = fixture.root;
    const { DatabaseSync } = process.getBuiltinModule("node:sqlite");
    const originalExec = DatabaseSync.prototype.exec;
    // Observe actual adapter connection-local policies after its last setup pragma; no changes to them.
    DatabaseSync.prototype.exec = function (sql, ...rest) {
      const result = originalExec.call(this, sql, ...rest);
      if (sql === "PRAGMA trusted_schema = OFF") {
        const main = this.prepare("PRAGMA database_list").all().find(row => row.name === "main");
        if (main?.file === database.file) record.policies.push({ phase: "ordinary-adapter-open", ...pragmas(this) });
      }
      return result;
    };
    restoreCapture = () => { DatabaseSync.prototype.exec = originalExec; };
    const { MeshStore } = await import(pathToFileURL(path.join(candidate, "dist/mesh.js")).href);
    checkTime(); attest(fixture);
    const open = () => new MeshStore(fixture.root, VALUE_LIMIT, 1000, { stateBackend: "sqlite", maxStateBytes: STATE_LIMIT,
      maxStateTombstones: TOMBSTONE_LIMIT, lockTimeoutMs: 2000, writeSignal: AbortSignal.timeout(Math.max(1, deadline - Date.now())) });
    // A genuine read/reopen also establishes the ordinary census directory. Close before migration;
    // an absent census directory is otherwise unready evidence, not proof of a stopped writer.
    store = open();
    assert.equal(store.listAll("", { fresh: true }).length, manifest.expected.kv);
    store.closeState(); store = undefined;
    receipt("pre-migration-read-closed", { noWorkers: true, snapshot: inspectPopulatedMesh(fixture) });
    const rolled = cli("rollback", ["rollback", "--root", fixture.root]);
    assert.equal(rolled.epoch, 2);
    const reimported = cli("reimport", ["import", "--root", fixture.root]);
    assert.equal(reimported.epoch, 3);
    receipt("reimport-shape", inspectPopulatedMesh(fixture));
    checkTime(); attest(fixture);
    store = open();
    const all = store.listAll("", { fresh: true });
    assert.equal(all.length, manifest.expected.kv);
    const groups = new Map(manifest.allocations.map(group => [group.namespace, all.filter(entry => entry.key.startsWith(group.namespace + "/"))]));
    const sacrifices = (64 - manifest.expected.kv % 64) % 64;
    manifest.feedPreparation = { sacrificialSinglePuts: sacrifices, targetSinglePuts: manifest.expected.changes,
      reason: "Align the import feed to a 64-sequence trim boundary; whole import commit is discarded." };
    let putCount = 0;
    const revisions = new Map(all.map(entry => [entry.key, entry.version]));
    const put = async entry => {
      checkTime(); attest(fixture);
      const updated = await store.put({ key: entry.key, value: entry.value, identity: entry.updatedBy });
      // Existing keys increment their own revision; high-water advances separately on every mutation.
      assert.equal(updated.version, revisions.get(entry.key) + 1);
      assert.deepEqual(updated.updatedBy, entry.updatedBy, "ordinary reseed changed row identity");
      revisions.set(entry.key, updated.version); putCount++;
      if (putCount === manifest.expected.changes) receipt("only-target-count-puts", inspectPopulatedMesh(fixture));
    };
    for (let i = 0; i < sacrifices; i++) await put(all[0]);
    for (const group of manifest.expected.changesPrefixes) {
      const choices = groups.get(group.namespace); assert(choices?.length, "change group has no live keys");
      for (let i = 0; i < group.rows; i++) await put(choices[i % choices.length]);
    }
    const beforeEvents = inspectPopulatedMesh(fixture); assertTarget(fixture, beforeEvents);
    assert.equal(Number(beforeEvents.logical.meta.high_water), state.highWater + putCount, "high-water allocation mismatch");
    assert.equal(beforeEvents.logical.tombstoneDigest, initial.logical.tombstoneDigest, "tombstones changed during preparation");
    assert.equal(beforeEvents.logical.valueDigest, initial.logical.valueDigest, "KV values/identities changed during puts");
    receipt("before-events", beforeEvents);
    // Ordinary bounded batches, approximately 2 KiB lines rather than a tiny giant-event cardinality.
    let bytes = 0, count = 0, previous = 0, maxLine = 0, batchCalls = 0, partialBatches = 0;
    const ids = new Set();
    while (bytes < eventBytes) {
      checkTime();
      const countHint = Math.min(32, Math.max(1, Math.ceil((eventBytes - bytes) / eventLineBytes)));
      const events = await store.publishBatch(Array.from({ length: countHint }, () => ({ topic: "fixture.seed", kind: "fixture",
        from: IDENTITY, text: "x".repeat(eventLineBytes - 512) })));
      // The production API may return a committed prefix when its hold budget expires.
      // Account only that prefix. These inert inputs have no pending semantics to replay;
      // uncertain/throwing publication is never retried by this helper.
      assert(events.length > 0 && events.length <= countHint, "publication made no bounded progress");
      batchCalls++; if (events.length < countHint) partialBatches++;
      for (const event of events) {
        const line = encoded(event) + 1;
        assert(line <= eventLineBytes && line <= VALUE_LIMIT, "event envelope exceeds bounded line");
        assert(!ids.has(event.id) && event.sequence > previous, "event identity/sequence invalid");
        assert(!event.to && event.topic === "fixture.seed" && event.from.id === IDENTITY.id, "unexpected event routing");
        ids.add(event.id); previous = event.sequence; count++; bytes += line; maxLine = Math.max(maxLine, line);
      }
    }
    assert(bytes >= eventBytes && bytes - eventBytes <= eventLineBytes, "event target must be within one bounded line");
    // Validate actual framing incrementally, never decoding the entire event log into memory.
    if (count) {
      attest(fixture);
      const fd = fs.openSync(path.join(fixture.root, "events.jsonl"), fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
      let pending = Buffer.alloc(0), diskBytes = 0, diskCount = 0, diskSequence = 0;
      const chunk = Buffer.alloc(VALUE_LIMIT);
      try {
        for (;;) {
          checkTime();
          const length = fs.readSync(fd, chunk, 0, chunk.length, null);
          if (!length) break;
          diskBytes += length; pending = Buffer.concat([pending, chunk.subarray(0, length)]);
          let newline;
          while ((newline = pending.indexOf(10)) !== -1) {
            assert(newline + 1 <= eventLineBytes, "stored event line exceeds budget");
            const event = JSON.parse(pending.subarray(0, newline).toString("utf8"));
            assert(ids.delete(event.id) && event.sequence > diskSequence, "stored event identity/sequence mismatch");
            assert(event.topic === "fixture.seed" && event.kind === "fixture" && !event.to
              && event.from.id === IDENTITY.id, "stored event is not synthetic publication");
            diskSequence = event.sequence; diskCount++; pending = pending.subarray(newline + 1);
          }
          assert(pending.length < VALUE_LIMIT, "unterminated oversized event line");
        }
        assert.equal(pending.length, 0, "incomplete event framing");
        assert.equal(diskBytes, bytes); assert.equal(diskCount, count); assert.equal(ids.size, 0);
      } finally { fs.closeSync(fd); }
    }
    const afterEvents = inspectPopulatedMesh(fixture);
    assert.equal(afterEvents.logicalDigest, beforeEvents.logicalDigest, "event publication altered state/feed");
    assert.equal(afterEvents.physical["events.jsonl"].bytes, bytes, "event framing byte mismatch");
    manifest.events = { requestedBytes: eventBytes, actualBytes: bytes, count, maxLineBytes: maxLine, eventLineBytes,
      batchCalls, partialBatches, verifiedStoredFraming: true,
      assumption: "Default bytes interpret MB as decimal; source event cardinality is unknown. Bounded synthetic publication only." };
    receipt("after-events", afterEvents);
    store.closeState(); store = undefined;
    attest(fixture); store = open();
    assert.equal(store.stateBackend, "sqlite");
    for (const group of manifest.allocations) {
      const sample = all.find(entry => entry.key.startsWith(group.namespace + "/"));
      assert.deepEqual(store.get(sample.key, { fresh: true }).value, state.entries[sample.key].value, "ordinary reopen sample mismatch");
    }
    const reopened = inspectPopulatedMesh(fixture); assertTarget(fixture, reopened);
    assert.equal(reopened.logicalDigest, afterEvents.logicalDigest, "ordinary reopen mutated logical state");
    receipt("ordinary-reopen", { snapshot: reopened, diagnostics: store.stateDiagnostics() });
    assert(record.policies.length >= 2, "actual production connection policy not captured");
    store.closeState(); store = undefined;
    restoreCapture(); restoreCapture = undefined;
    manifest.limitations = ["Physical DB/WAL allocation is observed, not forced to historical rounded sizes.",
      "No reader pins, fake WAL, checkpoint override, DDL padding, or vacuum. Last-close may remove WAL.",
      "Logical shape is exact only before workers; native startup legitimately changes it.",
      "The private environment is scoped to serial preparation; the fixture keeps directory pins, not SQLite handles."];
    const final = inspectPopulatedMesh(fixture); assertTarget(fixture, final);
    receipt("closed-baseline", final); save(path.join(evidence, "manifest.json"), manifest);
    save(path.join(evidence, "receipts.json"), receipts);
    return fixture;
  } catch (error) {
    save(path.join(evidence, "failure.json"), { error: String(error), stack: error.stack, ownership: fixture?.ownership });
    if (fixture) { fixture.close(); error.fixture = fixture; }
    throw error;
  } finally {
    try { store?.closeState(); } finally {
      restoreCapture?.(); process.env = envBefore; preparing = false;
      if (!fixture) fs.closeSync(parentFd);
    }
  }
}
