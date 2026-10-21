import fs from "node:fs";
import path from "node:path";
import { SqliteStateStore } from "../src/mesh/state-sqlite.js";

/** Explicit nondefault sensitivity knob, applied only to this run's private scratch root.
 * MeshStore does not expose retention. Route its real backend opens through the real store's
 * existing option rather than editing production SQL, manufacturing rows, or changing admission.
 */
export function configurePrivateFeed(root: string, retained: number) {
  if (retained === 4096) return () => undefined;
  const original = SqliteStateStore.openSync;
  SqliteStateStore.openSync = (openedRoot, eventBytes, readEvents, options = {}) => original.call(SqliteStateStore,
    openedRoot, eventBytes, readEvents, openedRoot === root ? { ...options, changesRetained: retained } : options);
  return () => { SqliteStateStore.openSync = original; };
}

export function sizes(root: string) {
  const bytes = (name: string) => { try { return fs.statSync(path.join(root, name)).size; } catch { return 0; } };
  return { dbBytes: bytes("state.db"), walBytes: bytes("state.db-wal"), shmBytes: bytes("state.db-shm"), eventLogBytes: bytes("events.jsonl") };
}

const aggregatePrefixes = ["topology", "actors", "mesh", "sessions", "residency", "github-ingress", "ci-wait", "dev-lead", "other"] as const;
const aggregatePrefix = (key: unknown): typeof aggregatePrefixes[number] => {
  const prefix = String(key).split("/", 1)[0];
  return aggregatePrefixes.find(allowed => allowed !== "other" && allowed === prefix) ?? "other";
};
export type InventoryOptions = { privacy: "aggregate" };

/** Calibration/barrier snapshots only, on a caller-owned private working root.
 * Never pass staged source here, and never checkpoint during measured phases.
 * Aggregate mode's output keys are allowlisted, not derived from private key strings.
 */
export function inventory(root: string, checkpoint = false, options?: InventoryOptions) {
  const sqlite = process.getBuiltinModule("node:sqlite") as typeof import("node:sqlite");
  const db = new sqlite.DatabaseSync(path.join(root, "state.db"), { readOnly: !checkpoint });
  try {
    if (checkpoint) db.prepare("PRAGMA wal_checkpoint(PASSIVE)").all();
    db.exec("BEGIN");
    const aggregate = options?.privacy === "aggregate";
    const prefixMix: Record<string, { rows: number; valueBytes: number }> = {};
    const rootPrefixMix: Record<string, { rows: number; valueBytes: number }> = {};
    const prefixGroups = Object.fromEntries(aggregatePrefixes.map(prefix => [prefix, { rows: 0, valueBytes: 0 }]));
    for (const row of db.prepare("SELECT key, length(CAST(value AS BLOB)) AS bytes FROM kv").all()) {
      if (aggregate) {
        const entry = prefixGroups[aggregatePrefix(row.key)]!;
        entry.rows++; entry.valueBytes += Number(row.bytes);
        continue;
      }
      const prefix = String(row.key).split("/").slice(0, 2).join("/") + "/";
      const entry = prefixMix[prefix] ??= { rows: 0, valueBytes: 0 };
      entry.rows++; entry.valueBytes += Number(row.bytes);
      const rootPrefix = String(row.key).split("/")[0]!;
      const rootEntry = rootPrefixMix[rootPrefix] ??= { rows: 0, valueBytes: 0 };
      rootEntry.rows++; rootEntry.valueBytes += Number(row.bytes);
    }
    const feed = db.prepare("SELECT count(*) AS rows, min(seq) AS minSeq, max(seq) AS maxSeq, min(commit_no) AS minCommit, max(commit_no) AS maxCommit FROM changes").get();
    const feedPrefixMix: Record<string, number> = {};
    const feedPrefixGroups: Record<string, number> = Object.fromEntries(aggregatePrefixes.map(prefix => [prefix, 0]));
    for (const row of db.prepare("SELECT key FROM changes").all()) {
      if (aggregate) { const prefix = aggregatePrefix(row.key); feedPrefixGroups[prefix] = feedPrefixGroups[prefix]! + 1; }
      else { const prefix = String(row.key).split("/")[0]!; feedPrefixMix[prefix] = (feedPrefixMix[prefix] ?? 0) + 1; }
    }
    const tombstones = db.prepare("SELECT count(*) AS rows, min(ord) AS minOrd, max(ord) AS maxOrd FROM tombstones").get();
    const metaRows = Number(db.prepare("SELECT count(*) AS rows FROM meta").get()?.rows);
    const meta = Object.fromEntries(db.prepare("SELECT name, value FROM meta WHERE name IN ('high_water', 'state_bytes', 'tombstone_ord', 'commit_no')").all()
      .map(row => [String(row.name), Number(row.value)]));
    const kvAccounting = db.prepare("SELECT sum(bytes) AS bytes, max(version) AS maxVersion FROM kv").get();
    const tombAccounting = db.prepare("SELECT sum(2*length(CAST(key AS BLOB))+32) AS bytes, max(version) AS maxVersion, max(ord) AS maxOrd FROM tombstones").get();
    const metadataConsistent = Number(meta.state_bytes) === Number(kvAccounting?.bytes ?? 0) + Number(tombAccounting?.bytes ?? 0) &&
      Number(meta.high_water) >= Math.max(Number(kvAccounting?.maxVersion ?? 0), Number(tombAccounting?.maxVersion ?? 0)) &&
      Number(meta.tombstone_ord) >= Number(tombAccounting?.maxOrd ?? 0) && Number(meta.commit_no) >= Number(feed?.maxCommit ?? 0);
    const quickCheck = db.prepare("PRAGMA quick_check").get()?.quick_check;
    const freePages = Number(db.prepare("PRAGMA freelist_count").get()?.freelist_count);
    const pageCount = Number(db.prepare("PRAGMA page_count").get()?.page_count);
    const pageSize = Number(db.prepare("PRAGMA page_size").get()?.page_size);
    const directories: Record<string, { entries: number; files: number; directories: number }> = {};
    for (const name of ["participants", "host-leases", "host-lease-commits", "ingress-cursors", "control-seen", "event-receipts", "actors", "lock-stats"]) {
      try {
        const entries = fs.readdirSync(path.join(root, name), { withFileTypes: true });
        directories[name] = { entries: entries.length, files: entries.filter(entry => entry.isFile()).length,
          directories: entries.filter(entry => entry.isDirectory()).length };
      } catch { directories[name] = { entries: 0, files: 0, directories: 0 }; }
    }
    if (aggregate) {
      const schema = db.prepare("SELECT sum(type='table') AS tables, sum(type='index') AS indexes, sum(type='table' AND name IN ('kv','meta','changes','tombstones')) AS requiredTables FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'").get();
      const numericRow = (row: Record<string, unknown> | undefined, names: string[]) => Object.fromEntries(names.map(name =>
        [name, row?.[name] === null || row?.[name] === undefined ? null : Number(row[name])]));
      return { ...sizes(root), logicalDbBytes: pageCount * pageSize, pageCount, pageSize, freePages,
        prefixGroups, feedPrefixGroups, kvRows: Object.values(prefixGroups).reduce((sum, entry) => sum + entry.rows, 0),
        feed: numericRow(feed, ["rows", "minSeq", "maxSeq", "minCommit", "maxCommit"]),
        tombstones: numericRow(tombstones, ["rows", "minOrd", "maxOrd"]), metaRows,
        meta: numericRow(meta, ["high_water", "state_bytes", "tombstone_ord", "commit_no"]),
        schema: numericRow(schema, ["tables", "indexes", "requiredTables"]),
        metadataConsistent, quickCheck: quickCheck === "ok", directories };
    }
    return { ...sizes(root), logicalDbBytes: pageCount * pageSize, pageCount, pageSize, freePages, prefixMix, rootPrefixMix,
      feed, feedPrefixMix, tombstones, metaRows, meta, metadataConsistent, quickCheck, directories,
      journalMode: db.prepare("PRAGMA journal_mode").get()?.journal_mode };
  } finally { if (db.isTransaction) db.exec("ROLLBACK"); db.close(); }
}
