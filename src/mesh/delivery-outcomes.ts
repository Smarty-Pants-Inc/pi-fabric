import { randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { StringDecoder } from "node:string_decoder";
import { syncDirectoryChain, writeFileAtomic } from "../core/atomic-write.js";
import { verifiedRemove } from "../storage/verified-remove.js";

/** Host-owned send identity, retained with the receiver's durable queue, never user data. */
export interface DeliverySend {
  eventId: string;
  to: string;
  from: string;
  mode: "steer" | "followUp" | "publish";
}
export type DeliveryOutcome = "delivered" | "superseded" | "failed" | "unknown";
export interface DeliveryOutcomeRecord extends DeliverySend {
  outcome: DeliveryOutcome;
  reason: string;
  at: number;
  /** Refusals this one record stands for when coalesced (absent means one). */
  count?: number;
}
/** Per-partition cap. Over it, the oldest records are dropped behind one counted marker. */
export interface DeliveryOutcomeLimits { records: number; bytes: number }
export const DELIVERY_OUTCOME_LIMITS: Readonly<DeliveryOutcomeLimits> = Object.freeze({ records: 10_000, bytes: 4 * 1024 * 1024 });
/** The visible signal for dropped records. Never a DeliverySend, so never dedup evidence. */
export interface DeliveryOutcomeTruncation { truncated: true; dropped: number; at: number }
const day = (at: number): string => new Date(at).toISOString().slice(0, 10).replaceAll("-", "");

const outcomeKey = (record: DeliverySend & { outcome: DeliveryOutcome }): string =>
  JSON.stringify([record.eventId, record.to, record.from, record.mode, record.outcome]);
interface OutcomeIndex { dev: number; ino: number; offset: number; tail: string; decoder: StringDecoder; keys: Set<string>; lines: number }
const indexes = new Map<string, OutcomeIndex>();

/** Incremental read-only dedup evidence, rebuilt from retained JSONL after restart.
 * Writer ownership is still the native receiver/control fence, not this cache.
 */
const indexedOutcomes = (file: string): Readonly<OutcomeIndex> => {
  const fd = fs.openSync(file, fs.constants.O_RDONLY);
  try {
    const stat = fs.fstatSync(fd);
    let index = indexes.get(file);
    if (!index || index.dev !== stat.dev || index.ino !== stat.ino || stat.size < index.offset) {
      index = { dev: stat.dev, ino: stat.ino, offset: 0, tail: "", decoder: new StringDecoder("utf8"), keys: new Set(), lines: 0 };
      indexes.delete(file); indexes.set(file, index);
      while (indexes.size > 16) indexes.delete(indexes.keys().next().value!);
    }
    let buffer: Buffer | undefined;
    while (index.offset < stat.size) {
      buffer ??= Buffer.alloc(64 * 1024);
      const count = fs.readSync(fd, buffer, 0, Math.min(buffer.length, stat.size - index.offset), index.offset);
      if (!count) break;
      index.offset += count;
      const lines = (index.tail + index.decoder.write(buffer.subarray(0, count))).split("\n");
      index.tail = lines.pop()!;
      index.lines += lines.length;
      for (const line of lines) {
        try {
          const value = JSON.parse(line) as DeliveryOutcomeRecord;
          const send = deliverySend(value);
          if (send && ["delivered", "superseded", "failed", "unknown"].includes(value.outcome)) index.keys.add(outcomeKey({ ...send, outcome: value.outcome }));
        } catch { /* A malformed old line is not delivery evidence. */ }
      }
    }
    return index;
  } finally { fs.closeSync(fd); }
};

/** Read-only terminal receipt lookup; reuse the same incremental index as append deduplication. */
export const hasDeliveryOutcome = (meshRoot: string, send: DeliverySend, outcome: DeliveryOutcome): boolean => {
  const directory = path.join(meshRoot, "delivery-outcomes");
  let names: string[];
  try { names = fs.readdirSync(directory); }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return false; throw error; }
  const key = outcomeKey({ ...send, outcome });
  return names.some(name => /^\d{8}\.jsonl$/.test(name) && indexedOutcomes(path.join(directory, name)).keys.has(key));
};

/** Keep the newest half of each limit behind one marker carrying the cumulative dropped count. */
const compactPartition = (file: string, limits: DeliveryOutcomeLimits, at: number): void => {
  let dropped = 0;
  const records: string[] = [];
  for (const line of fs.readFileSync(file, "utf8").split("\n")) {
    if (!line) continue;
    try {
      const value = JSON.parse(line) as Partial<DeliveryOutcomeTruncation>;
      if (value?.truncated === true && Number.isSafeInteger(value.dropped) && value.dropped! >= 0) { dropped += value.dropped!; continue; }
    } catch { /* A malformed line is still a dropped or retained line, never a lost count. */ }
    records.push(line);
  }
  let start = records.length, bytes = 0;
  while (start > 0 && records.length - start < Math.floor(limits.records / 2)) {
    const size = Buffer.byteLength(records[start - 1]!) + 1;
    if (bytes + size > Math.floor(limits.bytes / 2)) break;
    bytes += size; start -= 1;
  }
  const marker: DeliveryOutcomeTruncation = { truncated: true, dropped: dropped + start, at };
  // ponytail: a compaction, not an expiry delete: the replacement keeps the newest records and the dropped count, and
  // an appender whose inode it replaced re-appends (nlink 0 check below).
  writeFileAtomic(file, [JSON.stringify(marker), ...records.slice(start)].join("\n") + "\n", { durable: true, mode: 0o600 });
  indexes.delete(file);
};
/** No mesh state/lock: one O_APPEND write and durability barrier per final observation.
 * Unknown is an observation, not a receipt; a later receiver-delivered line is valid.
 * Keep today's UTC partition and the preceding six (seven daily partitions).
 */
export const appendDeliveryOutcome = (
  meshRoot: string,
  send: DeliverySend,
  outcome: DeliveryOutcome,
  reason: string,
  at = Date.now(),
  options: { count?: number; limits?: DeliveryOutcomeLimits } = {},
): void => {
  const directory = path.join(meshRoot, "delivery-outcomes");
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  const cutoff = day(at - 6 * 24 * 60 * 60_000);
  const key = outcomeKey({ ...send, outcome });
  const retained: string[] = [];
  for (const name of fs.readdirSync(directory)) {
    if (!/^\d{8}\.jsonl$/.test(name)) continue;
    const file = path.join(directory, name);
    // smarty-dev#8907: the inode (and mtime) seen at the expiry judgment is the only one removed: a late append or a
    // replacement since then stays. Off Linux main's removal. The index goes only with a confirmed removal.
    let judged: fs.Stats | undefined;
    try { judged = fs.lstatSync(file); }
    catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") { indexes.delete(file); continue; } }
    if (name.slice(0, 8) >= cutoff) retained.push(file);
    // Files only (main's non-recursive rm): an expired non-file is left, never removed recursively.
    else if (judged?.isFile() === false) continue;
    else if (verifiedRemove(file, judged, { deleter: "deliveryOutcomeExpiry",
      fallback: () => { fs.rmSync(file, { force: true }); return true; } }).removed) indexes.delete(file);
  }
  for (const file of retained.sort().reverse()) {
    if (indexedOutcomes(file).keys.has(key)) {
      // A restart can repeat a final observation after append but before queue cleanup.
      // Windows FlushFileBuffers needs a writable handle (a read-only fsync is EPERM);
      // O_APPEND without a write keeps the receipt barrier unable to alter the journal.
      const receipt = fs.openSync(file, fs.constants.O_WRONLY | fs.constants.O_APPEND);
      try { fs.fsyncSync(receipt); } finally { fs.closeSync(receipt); }
      syncDirectoryChain(directory);
      return;
    }
  }
  const record: DeliveryOutcomeRecord = { ...send, outcome, reason, at, ...(options.count === undefined ? {} : { count: options.count }) };
  const line = Buffer.from(JSON.stringify(record) + "\n");
  const limits = options.limits ?? DELIVERY_OUTCOME_LIMITS;
  const file = path.join(directory, day(at) + ".jsonl");
  // A concurrent compaction can replace the inode under an O_APPEND writer; re-append
  // (bounded) unless the replacement already carries this record.
  for (let attempt = 0; attempt < 3; attempt++) {
    if (fs.existsSync(file)) {
      const index = indexedOutcomes(file);
      if (attempt > 0 && index.keys.has(key)) break;
      if (index.lines + 1 > limits.records || index.offset + line.length > limits.bytes) compactPartition(file, limits, at);
    }
    const fd = fs.openSync(file, fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_APPEND, 0o600);
    let replaced = false;
    try {
      if (fs.writeSync(fd, line) !== line.length) throw new Error("Short delivery outcome append");
      fs.fsyncSync(fd);
      replaced = fs.fstatSync(fd).nlink === 0;
    } finally { fs.closeSync(fd); }
    if (!replaced) break;
  }
  syncDirectoryChain(directory);
};

/** Repeated unknown-target refusals from one sender coalesce within this window. */
export const UNKNOWN_TARGET_WINDOW_MS = 10_000;
/** Distinct (sender, target) windows tracked at once; beyond it they share one per-sender window. */
export const UNKNOWN_TARGET_KEYS = 256;
interface UnknownTargetWindow { root: string; from: string; to: string; reason: string; until: number; count: number; timer?: NodeJS.Timeout }
const unknownTargets = new Map<string, UnknownTargetWindow>();
let unknownTargetExitHook: (() => void) | undefined;

const flushUnknownTarget = (key: string, at = Date.now()): void => {
  const window = unknownTargets.get(key);
  if (!window) return;
  if (window.timer) clearTimeout(window.timer);
  unknownTargets.delete(key);
  if (window.count) {
    appendDeliveryOutcome(window.root, { eventId: randomUUID(), to: window.to, from: window.from, mode: "publish" }, "failed",
      window.reason, at, { count: window.count });
  }
};

/** Write every pending coalesced count now (window close, process exit, tests). */
export const flushUnknownTargetOutcomes = (at = Date.now()): void => {
  for (const key of [...unknownTargets.keys()]) {
    try { flushUnknownTarget(key, at); } catch { /* A failed late count must not mask other flushes. */ }
  }
};

/** Bounded unknown-target evidence: the first refusal in a (sender, target) window is one durable
 * record; repeats inside it are counted and written as one record with `count` when it closes.
 */
export const recordUnknownTarget = (meshRoot: string, from: string, to: string, reason: string, at = Date.now()): void => {
  const exact = JSON.stringify([meshRoot, from, to]);
  const key = unknownTargets.has(exact) || unknownTargets.size < UNKNOWN_TARGET_KEYS ? exact : JSON.stringify([meshRoot, from, "*"]);
  const window = unknownTargets.get(key);
  if (window && at < window.until) { window.count += 1; return; }
  if (window) flushUnknownTarget(key, at);
  const shared = key !== exact;
  const opened: UnknownTargetWindow = {
    root: meshRoot, from, to: shared ? "*" : to, until: at + UNKNOWN_TARGET_WINDOW_MS, count: 0,
    reason: shared ? `Unknown Fabric targets (more than ${UNKNOWN_TARGET_KEYS} distinct, coalesced per sender)` : reason,
  };
  // Every window closes on its own: it frees its slot and writes any coalesced count.
  opened.timer = setTimeout(() => {
    if (unknownTargets.get(key) !== opened) return;
    try { flushUnknownTarget(key); } catch { /* The count is lost only with its mesh root. */ }
  }, UNKNOWN_TARGET_WINDOW_MS);
  opened.timer.unref?.();
  unknownTargets.set(key, opened);
  if (!unknownTargetExitHook) {
    unknownTargetExitHook = () => flushUnknownTargetOutcomes();
    process.once("exit", unknownTargetExitHook);
  }
  appendDeliveryOutcome(meshRoot, { eventId: randomUUID(), to, from, mode: "publish" }, "failed", reason, at);
};

/** Validate retained host metadata; never infer receipt identity from arbitrary message data. */
export const deliverySend = (value: unknown): DeliverySend | undefined => {
  if (!value || typeof value !== "object") return undefined;
  const send = value as DeliverySend;
  return typeof send.eventId === "string" && typeof send.to === "string" && typeof send.from === "string" &&
    (send.mode === "steer" || send.mode === "followUp" || send.mode === "publish")
    ? { eventId: send.eventId, to: send.to, from: send.from, mode: send.mode } : undefined;
};
