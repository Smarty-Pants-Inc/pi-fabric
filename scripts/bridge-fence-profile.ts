import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import type { MeshStore, MeshBatchOperation, MeshStateEntry } from "../src/mesh/store.js";
import { inventory } from "./bridge-fence-inventory.js";

const hash = (text: string) => createHash("sha256").update(text).digest("hex");
const json = (file: string, value: unknown) => fs.writeFileSync(file, JSON.stringify(value), { mode: 0o600 });
const bytes = (value: unknown) => Buffer.byteLength(JSON.stringify(value));
const who = { id: "session:fence-repro-hub-00000000", name: "session:fence-repro-hub-00000000", kind: "main" as const };

/** Aggregate-only synthetic fixture. No live values or real account/repository identities.
 * Prefix counts/value-byte budgets are the lead's 22:47Z read-only Ry1 aggregates.
 * External application row schemas were NOT provided; those are inert JSON envelope fixtures,
 * explicitly not a production schema/census claim. Bridge-native topology uses actual records.
 */
export async function seedProductionExtras(store: MeshStore, targetDbBytes: number) {
  const now = Date.now();
  const groups = [
    ["github-ingress", 989, 850000], ["actors", 638, 580000], ["ci-wait", 550, 270000],
    ["mesh", 436, 370000], ["sessions", 260, 110000], ["residency", 181, 420000],
    ["dev-lead", 30, 280000], ["calibration-other", 87, 45000],
  ] as const;
  const all: Array<{ key: string; value: Record<string, unknown> }> = [];
  for (const [prefix, count, budget] of groups) {
    for (let n = 0; n < count; n++) {
      const key = `${prefix}/fence-synthetic-${String(n).padStart(8, "0")}`;
      const value: Record<string, unknown> = prefix === "actors" ? {
        id: `fence-actor-${n}`, scope: "session", rootId: who.id, name: `synthetic-worker-${n}`, status: "idle", runner: "pi",
        events: ["agent_end"], topics: ["fleet.work.fence15310"], delivery: "followUp", responseMode: "event", triggerTurn: true,
        coalesce: true, filterSkipped: { count: 0, lastKey: null, lastTopic: null, lastAt: null }, lastError: "",
        createdAt: now, updatedAt: now,
      } : { format: 1, id: `fence-${prefix}-${n}`, owner: who.id, status: "complete", updatedAt: now,
        syntheticAggregateFixture: true, data: "" };
      const field = prefix === "actors" ? "lastError" : "data";
      const size = Math.floor(budget / count) + (n < budget % count ? 1 : 0);
      value[field] = "synthetic local-only retained record; ".repeat(Math.ceil(Math.max(0, size - bytes(value)) / 36))
        .slice(0, Math.max(0, size - bytes(value)));
      all.push({ key, value });
    }
  }
  for (let at = 0; at < all.length; at += 200) await store.writeBatch({ identity: who,
    ops: all.slice(at, at + 200).map(entry => ({ kind: "put", ...entry })) });
  // Match topology's aggregate value bytes using schema-permitted accepted result text,
  // not extra unknown topology rows/aliases. The native host/participant submix is an assumption.
  const topology = store.listAll("topology/", { fresh: true });
  const controls = topology.filter(entry => entry.key.startsWith("topology/control-seen/"));
  const extra = Math.max(0, 1460000 - topology.reduce((sum, entry) => sum + bytes(entry.value), 0));
  await store.writeBatch({ identity: who, ops: controls.map((entry, n) => {
    const value = entry.value as Record<string, unknown>;
    return { kind: "put", key: entry.key, value: { ...value, acceptance: { ...(value.acceptance as object),
      result: { detail: "local-only accepted result ".repeat(Math.ceil((Math.floor(extra / controls.length) + 1) / 27))
        .slice(0, Math.floor(extra / controls.length) + (n < extra % controls.length ? 1 : 0)) } } } };
  }) });
  // A 14.29MB physical DB with ~4.3MB value text can include allocated/free history pages.
  // Grow real actor lastError values temporarily, then restore them, WITHOUT changing row
  // counts, deleting product state, adding fake tables, or leaving oversized current values.
  const actors = store.listAll("actors/", { fresh: true });
  let grown = 0;
  while (inventory(store.root).logicalDbBytes < targetDbBytes) {
    if (grown >= actors.length) throw new Error("bounded physical-history calibration exhausted actor rows");
    const chunk = actors.slice(grown, grown + 5);
    await store.writeBatch({ identity: who, ops: chunk.map(entry => ({ kind: "put", key: entry.key,
      value: { ...(entry.value as object), lastError: "synthetic historical error stack\n".repeat(1500) } })) });
    grown += chunk.length;
  }
  for (let at = 0; at < grown; at += 200) await store.writeBatch({ identity: who,
    ops: actors.slice(at, Math.min(grown, at + 200)).map(entry => ({ kind: "put", key: entry.key, value: entry.value })) });
  // Retained feed prefix composition, through REAL MeshStore mutations, not an unbounded feed.
  const pools = [store.listAll("topology/", { fresh: true }), store.listAll("actors/", { fresh: true }),
    store.listAll("mesh/", { fresh: true }), store.listAll("calibration-other/", { fresh: true })];
  const changes: MeshBatchOperation[] = [];
  for (const [group, count] of [[0, 1765], [1, 1265], [2, 999], [3, 19]] as const) {
    const pool = pools[group]!;
    for (let n = 0; n < count; n++) {
      const entry = pool[n % pool.length]!;
      changes.push({ kind: "put", key: entry.key, value: entry.value, identity: entry.updatedBy });
    }
  }
  // 282 real commits, 4048 changed rows, no feed-retention override.
  let at = 0;
  for (let commit = 0; commit < 282; commit++) {
    const count = Math.floor(changes.length / 282) + (commit < changes.length % 282 ? 1 : 0);
    await store.writeBatch({ identity: who, ops: changes.slice(at, at + count) }); at += count;
  }
  return { externalApplicationSchemaAssumption: "Inert aggregate-sized envelopes; app-specific schemas unavailable. Native topology and actor field names from repository types.",
    physicalHistoryAssumption: "Temporarily larger schema-permitted actor.lastError values restored; retained SQLite free pages model historical allocation, not a claimed production freelist census.",
    syntheticHistoricalActorRowsExpandedAndRestored: grown };
}

/** Valid private historical metadata offsets; no 66-million-row generation. */
export function seedHistoricalMetadata(root: string) {
  const sqlite = process.getBuiltinModule("node:sqlite") as typeof import("node:sqlite");
  const db = new sqlite.DatabaseSync(path.join(root, "state.db"));
  try {
    db.exec("BEGIN IMMEDIATE");
    const current = db.prepare("SELECT max(seq) AS seq, max(commit_no) AS commitNo FROM changes").get()!;
    const seqShift = 66556416 - Number(current.seq), commitShift = 4463077 - Number(current.commitNo);
    db.prepare("UPDATE changes SET seq = seq + ?, commit_no = commit_no + ?").run(seqShift, commitShift);
    db.prepare("UPDATE meta SET value = CAST(value AS INTEGER) + ? WHERE name = 'commit_no'").run(commitShift);
    const high = Math.max(66556416, Number(db.prepare("SELECT value FROM meta WHERE name = 'high_water'").get()?.value ?? 0));
    const tomb = db.prepare("INSERT INTO tombstones(key, version, ord) VALUES (?, ?, ?)");
    let stateBytes = 0;
    for (let n = 0; n < 1000; n++) {
      const prefix = n < 808 ? "topology" : n < 905 ? "residency" : n < 965 ? "decisions" : "calibration-other";
      const key = `${prefix}/historical-removed-${String(n).padStart(8, "0")}`;
      tomb.run(key, high + n + 1, n + 1); stateBytes += 2 * Buffer.byteLength(key) + 32;
    }
    db.prepare("UPDATE meta SET value = ? WHERE name = 'tombstone_ord'").run(1000);
    db.prepare("UPDATE meta SET value = ? WHERE name = 'high_water'").run(high + 1000);
    db.prepare("UPDATE meta SET value = CAST(value AS INTEGER) + ? WHERE name = 'state_bytes'").run(stateBytes);
    db.exec("COMMIT");
    return { syntheticLifetimeOffsets: { seqShift, commitShift, highWater: high + 1000 }, tombstones: 1000,
      tombstoneStateByteFormula: "2*UTF8(key)+32, same as state-sqlite.ts tombstoneBytes" };
  } catch (error) { try { db.exec("ROLLBACK"); } catch {} throw error; }
  finally { db.close(); }
}

/** Real-shaped finite retained snapshot files; no expiry/deletion/replay is exercised. */
export function seedProductionFiles(root: string, historicalEvents: number) {
  const now = Date.now(), receiptDir = path.join(root, "event-receipts");
  fs.mkdirSync(receiptDir, { recursive: true, mode: 0o700 });
  for (let n = 0; n < 33669; n++) {
    const dedupeKey = `fence-retained-receipt-${n}`;
    json(path.join(receiptDir, `${hash(dedupeKey)}.json`), { id: `fence-history-${n % historicalEvents}`,
      dedupeKey, sequence: n % historicalEvents + 1, topic: "fence.history", kind: "record", from: who,
      createdAt: now, data: { syntheticRetainedReceipt: true, ordinal: n } });
  }
  for (const directory of ["ingress-cursors", "control-seen", "actors", "lock-stats"]) fs.mkdirSync(path.join(root, directory), { recursive: true, mode: 0o700 });
  for (let n = 0; n < 114; n++) json(path.join(root, "ingress-cursors", `${hash(`fixture:ingress:${n}`)}.json`), {
    format: 1, topic: `fence.ingress.${n}`, after: n + 1, updatedAt: now, syntheticAggregateFixture: true,
  });
  for (let n = 0; n < 9519; n++) {
    const dir = path.join(root, "control-seen", hash(`historical-host-${n}`).slice(0, 32)); fs.mkdirSync(dir, { mode: 0o700 });
    const key = `topology/control-seen/${hash(`historical-command-${n}`)}`;
    const value = { format: 1, hostId: who.id, commandId: `fixture-${n}`, targetId: who.id,
      expiresAt: now + 86400000, sequence: n + 1, acceptance: { accepted: true, messageId: `fixture-${n}` } };
    json(path.join(dir, "state.json"), { format: 1, revisionFormat: 2, highWater: 1,
      entries: { [key]: { key, value, version: 1, updatedAt: now, updatedBy: who } } });
  }
  for (let n = 0; n < 950; n++) {
    const dir = path.join(root, "actors", hash(`historical-actor-root-${n}`).slice(0, 32)); fs.mkdirSync(dir, { mode: 0o700 });
    json(path.join(dir, "actors.json"), { format: 1, actors: [{ id: `historical-actor-${n}`, scope: "session", rootId: who.id,
      name: `fixture-actor-${n}`, status: "idle", runner: "pi", events: ["agent_end"], topics: ["fleet.work.fence15310"],
      delivery: "followUp", responseMode: "event", triggerTurn: true, coalesce: true, createdAt: now, updatedAt: now }] });
  }
  for (let n = 0; n < 5782; n++) json(path.join(root, "lock-stats", `synthetic-${100000 + n}.json`), {
    version: 1, host: "synthetic", pid: 100000 + n, root, startedAt: now - 86400000, updatedAt: now - 600000,
    writer: { argv1: "bridge-fence-retained-fixture" }, minutes: [],
  });
  const domains = path.join(root, "host-lease-commits"); fs.mkdirSync(domains, { recursive: true, mode: 0o700 });
  for (let n = 0; n < 2245; n++) fs.mkdirSync(path.join(domains, hash(`historical-domain-${n}`).slice(0, 16)), { mode: 0o700 });
  return { eventReceipts: 33669, hostLeaseCommitDomains: 2245, ingressCursorFiles: 114, controlSeenFiles: 9519, actorFiles: 950, lockStatsFiles: 5782,
    assumptions: "Receipt key/hash/envelope valid and nonexpired; retained receipt data synthetic, not full coupled archive receipt recovery evidence. Control-seen uses valid file-state envelopes; actors use registry envelopes; lock-stats use version1 records. Ingress cursor schema and exact production leaf layouts/subdirectory counts were not supplied and remain explicit fixture assumptions. No active lock-owner files seeded." };
}
