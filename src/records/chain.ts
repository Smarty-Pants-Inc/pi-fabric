import { createHash } from "node:crypto";
import { RecordsArgumentError } from "./kinds.js";
import type { SqlClient } from "./schema.js";

/**
 * The records hash chain (smarty-dev#754 R3). Each record's `prev_hash` is the SHA-256 of the
 * previous record's canonical bytes, per org, in seq order; the first record's is NULL.
 *
 * Canonical bytes: the UTF-8 of the stored row as JSON with sorted keys, every column in its
 * PostgreSQL text form (NULL as null), created_at as its exact epoch (numeric seconds with six
 * decimals: era-complete, so an AD-to-BC edit changes it; Infinity and -Infinity spelled out). Text forms,
 * not parsed values, so the hash covers exactly what is stored: a jsonb number or a microsecond
 * that JavaScript would round still changes the bytes. Anyone with psql can recompute it.
 *
 * ponytail: one origin per org database (the records service is its only writer), so seq orders
 * the org's chain.
 */

/** Every records column, as text. A new column joins the chain only through a new chain version. */
export const CHAIN_SELECT = `id::text AS id, org, origin, seq::text AS seq, ref, kind, author, author_name,
  extract(epoch FROM created_at)::text AS created_at,
  text, data::text AS data, supersedes::text AS supersedes, key, payload_hash, prev_hash`;

export type ChainRow = Record<string, string | null>;

export const canonicalBytes = (row: ChainRow): Buffer =>
  Buffer.from(JSON.stringify(Object.fromEntries(Object.keys(row).sort().map((key) => [key, row[key] ?? null]))), "utf8");

export const rowHash = (row: ChainRow): string => createHash("sha256").update(canonicalBytes(row)).digest("hex");

/** The org's last record's canonical hash: the next record's prev_hash (null before the first). */
export const lastHash = async (client: SqlClient, org: string): Promise<{ seq: number; hash: string } | undefined> => {
  const { rows } = await client.query<ChainRow>(`SELECT ${CHAIN_SELECT} FROM records WHERE org = $1 ORDER BY records.seq DESC LIMIT 1`, [org]);
  return rows[0] ? { seq: Number(rows[0].seq), hash: rowHash(rows[0]) } : undefined;
};

/** What the backup adapter writes to every target on each run. */
export interface RecordsAnchor { org: string; seq: number; hash: string | null; at: string }

export interface ChainBreak {
  org: string;
  seq: number;
  /**
   * prev_hash: the stored prev_hash is not the previous row's hash; gap: a seq is missing;
   * org / origin: a row the service would serve belongs to another org or origin (#118 S2).
   */
  reason: "prev_hash" | "gap" | "org" | "origin";
  expected: string | null;
  found: string | null;
}
/** An anchor that failed: the row at seq is missing (found null) or has another hash. */
export interface AnchorFailure { seq: number; hash: string; found: string | null }
/** The anchors a verify reports in full: only failures, at most this many (the rest are counted). */
export const MAX_REPORTED_ANCHOR_FAILURES = 20;
/** A request's anchor list: at most this many, each exactly {seq, hash} on the wire (#118 F2). */
export const MAX_ANCHORS = 10_000;
export interface RecordsVerifyResult {
  org: string;
  rows: number;
  last?: { seq: number; hash: string };
  /** No chain break and every supplied anchor holds. */
  ok: boolean;
  /** ok, and every row is covered by an anchor. */
  clean: boolean;
  break?: ChainBreak;
  /** Every supplied anchor is checked; the result stays small whatever their number (F2). */
  anchors: { checked: number; passed: number; failed: number };
  /** The first failed anchors, in seq order (at most MAX_REPORTED_ANCHOR_FAILURES); `failed` counts them all. */
  failedAnchors: AnchorFailure[];
  /** Rows after the latest supplied anchor: nothing off-host vouches for them yet. */
  unanchored?: { from: number; to: number };
  summary: string;
}

const HASH = /^[0-9a-f]{64}$/;
const BATCH = 1000;

/**
 * Anchors as the adapter wrote them ({seq, hash}, extra fields ignored); checked strictly. The
 * empty-chain anchor records.anchor gives before the first record, exactly {seq: 0, hash: null},
 * is valid and vouches for nothing, so it is dropped here (#118 F1). Any other null or malformed
 * hash is refused, never skipped.
 */
export const parseAnchors = (value: unknown): { seq: number; hash: string }[] => {
  if (value === undefined) return [];
  if (!Array.isArray(value) || value.length > MAX_ANCHORS) throw new RecordsArgumentError(`anchors must be an array of {seq, hash} (at most ${MAX_ANCHORS})`);
  return value.flatMap((entry) => {
    const anchor = (typeof entry === "object" && entry !== null ? entry : {}) as { seq?: unknown; hash?: unknown };
    if (anchor.seq === 0 && anchor.hash === null) return [];
    if (typeof anchor.seq !== "number" || !Number.isSafeInteger(anchor.seq) || anchor.seq < 1 || typeof anchor.hash !== "string" || !HASH.test(anchor.hash)) {
      throw new RecordsArgumentError("each anchor needs seq (a positive integer) and hash (64 lowercase hex), or is the empty-chain anchor {seq: 0, hash: null}");
    }
    return [{ seq: anchor.seq, hash: anchor.hash }];
  });
};

/** The anchors on the wire: only {seq, hash}, so MAX_ANCHORS of them fit one request line. */
export const wireAnchors = (value: unknown): unknown =>
  Array.isArray(value) ? value.map((entry) => typeof entry === "object" && entry !== null ? { seq: (entry as { seq?: unknown }).seq, hash: (entry as { hash?: unknown }).hash } : entry) : value;

/**
 * Recompute the org's chain from one snapshot (the caller's REPEATABLE READ transaction), report
 * its first break, check each anchor (the row at that seq exists with that hash), and name the
 * rows after the latest anchor. Rows are read in batches, so memory stays bounded.
 */
export const verifyChain = async (client: SqlClient, org: string, origin: string, anchors: readonly { seq: number; hash: string }[], signal?: AbortSignal): Promise<RecordsVerifyResult> => {
  const wanted = new Map<number, string | null>(anchors.map((anchor) => [anchor.seq, null]));
  let first: ChainBreak | undefined;
  let prev: { seq: number; hash: string } | undefined;
  let rows = 0;
  // Every row in the table, not only this org's: the chain covers exactly what the service could
  // serve, and a row of another org or origin is a break, never silently left out (#118 S2).
  let cursor: { seq: string; id: string } = { seq: "0", id: "00000000-0000-0000-0000-000000000000" };
  for (;;) {
    signal?.throwIfAborted();
    const batch = await client.query<ChainRow>(
      `SELECT ${CHAIN_SELECT} FROM records WHERE (records.seq, records.id) > ($1::bigint, $2::uuid) ORDER BY records.seq, records.id LIMIT ${BATCH}`, [cursor.seq, cursor.id]);
    for (const row of batch.rows) {
      cursor = { seq: row.seq!, id: row.id! };
      const seq = Number(row.seq);
      const hash = rowHash(row);
      if (row.org !== org || row.origin !== origin) {
        const field = row.org !== org ? "org" : "origin";
        first ??= { org, seq, reason: field, expected: field === "org" ? org : origin, found: row[field] ?? null };
        rows++;
        continue;
      }
      if (!first) {
        const expectedSeq = (prev?.seq ?? 0) + 1;
        if (seq !== expectedSeq) first = { org, seq, reason: "gap", expected: String(expectedSeq), found: String(seq) };
        else if (row.prev_hash !== (prev?.hash ?? null)) first = { org, seq, reason: "prev_hash", expected: prev?.hash ?? null, found: row.prev_hash ?? null };
      }
      if (wanted.has(seq)) wanted.set(seq, hash);
      prev = { seq, hash };
      rows++;
    }
    if (batch.rows.length < BATCH) break;
  }
  let passed = 0;
  let failed = 0;
  const failedAnchors: AnchorFailure[] = [];
  for (const anchor of [...anchors].sort((a, b) => a.seq - b.seq)) {
    const found = wanted.get(anchor.seq) ?? null;
    if (found === anchor.hash) { passed++; continue; }
    failed++;
    if (failedAnchors.length < MAX_REPORTED_ANCHOR_FAILURES) failedAnchors.push({ seq: anchor.seq, hash: anchor.hash, found });
  }
  const latest = anchors.reduce((max, anchor) => Math.max(max, anchor.seq), 0);
  const unanchored = prev && prev.seq > latest ? { from: latest + 1, to: prev.seq } : undefined;
  const ok = !first && failed === 0;
  const lines = [
    ...(first ? [first.reason === "org" || first.reason === "origin"
      ? `unexpected ${first.reason} at seq ${first.seq}: expected ${first.expected}, found ${first.found ?? "none"}`
      : `chain broken at ${org} seq ${first.seq} (${first.reason}): expected ${first.expected ?? "none"}, found ${first.found ?? "none"}`] : []),
    ...(failedAnchors[0] ? [`anchor ${org} seq ${failedAnchors[0].seq} fails: expected ${failedAnchors[0].hash}, found ${failedAnchors[0].found ?? "no row"}${failed > 1 ? ` (${failed} of ${anchors.length} anchors fail)` : ""}`] : []),
    ...(unanchored ? [`unanchored: seq ${unanchored.from}..${unanchored.to}`] : []),
  ];
  return {
    org, rows, ...(prev ? { last: prev } : {}), ok, clean: ok && !unanchored,
    ...(first ? { break: first } : {}), anchors: { checked: anchors.length, passed, failed }, failedAnchors, ...(unanchored ? { unanchored } : {}),
    summary: lines.length ? lines.join("\n") : `clean: ${rows} records, anchored through seq ${latest}`,
  };
};

/**
 * Migration v2's backfill: set prev_hash on every existing row, per org in seq order. Runs as the
 * migration role with the append-only trigger disabled inside the migration's transaction.
 */
export const backfillChain = async (client: SqlClient): Promise<void> => {
  const orgs = await client.query<{ org: string }>("SELECT DISTINCT org FROM records ORDER BY org");
  for (const { org } of orgs.rows) {
    let prev: { seq: number; hash: string } | undefined;
    for (;;) {
      const batch = await client.query<ChainRow>(`SELECT ${CHAIN_SELECT} FROM records WHERE org = $1 AND seq > $2 ORDER BY records.seq LIMIT ${BATCH}`, [org, prev?.seq ?? 0]);
      const ids: string[] = [];
      const hashes: (string | null)[] = [];
      for (const row of batch.rows) {
        row.prev_hash = prev?.hash ?? null;
        ids.push(row.id!);
        hashes.push(row.prev_hash);
        prev = { seq: Number(row.seq), hash: rowHash(row) };
      }
      if (ids.length) {
        await client.query("UPDATE records r SET prev_hash = v.h FROM unnest($1::uuid[], $2::text[]) AS v(id, h) WHERE r.id = v.id", [ids, hashes]);
      }
      if (batch.rows.length < BATCH) break;
    }
  }
};
