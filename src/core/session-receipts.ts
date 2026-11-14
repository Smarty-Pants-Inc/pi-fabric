import { createHash, type Hash } from "node:crypto";
import fs from "node:fs";
import { syncPathNamespace } from "./atomic-write.js";
import { SessionEntryCursor, type SessionEntryCursorManager } from "./preview.js";

export interface NativeRecoverySessionRef {
  sessionId: string; // native UUID, WITHOUT session: prefix
  sessionFile: string;
}
export interface NativeRecoveryBindingBase {
  version: 1;
  operationId: string;
  preparedDigest: string; // lowercase SHA-256, 64 hex chars
  preparedEntryId: string;
  source: NativeRecoverySessionRef;
  target: NativeRecoverySessionRef;
}
export type NativeRecoveryPreparedBinding = NativeRecoveryBindingBase & {
  phase: "prepared";
  receivedEntryId?: never;
};
export type NativeRecoveryReceivedBinding = NativeRecoveryBindingBase & {
  phase: "received";
  receivedEntryId: string;
};
export type NativeRecoveryBinding = NativeRecoveryPreparedBinding | NativeRecoveryReceivedBinding;
export interface NativeRecoveryParkIntent {
  version: 1;
  operationId: string;
  source: NativeRecoverySessionRef;
  target: NativeRecoverySessionRef;
}
export interface NativeRecoveryOccurrence {
  ordinal: number; // nonnegative safe integer, unique within operation
  state: "pending" | "appended" | "handled" | "ambiguous";
  // Generic native custom-message projection, not a Fabric-only native parser.
  // Exact original details, including nested items[], chain/delivery IDs and
  // inbox event receipts. No message bodies needed by C. Absent for noncustom input.
  customMessage?: { customType: string; details: unknown };
}
export type NativeRecoveryTransfer = NativeRecoveryBinding & {
  occurrences: readonly NativeRecoveryOccurrence[];
};
export type NativeRecoveryCustodyResult =
  | { status: "none" }
  | { status: "hold"; reason: string; operationId?: string }
  | { status: "validated"; transfers: readonly NativeRecoveryTransfer[] };
export type NativeRecoveryCustodyReader = (
  scope: NativeRecoverySessionRef,
) => NativeRecoveryCustodyResult;

/** A missing (including deferred-first-write) file is no receipt. All other uncertainty throws.
 * Reconfirm every barrier even when callers already cached this inode's entries. */
export const withConfirmedSessionFile = (
  file: string,
  visit: (fd: number, stat: fs.Stats) => void,
): boolean => {
  let fd: number;
  try {
    // Windows FlushFileBuffers requires a writable handle; never create, truncate or write.
    fd = fs.openSync(file, process.platform === "win32" ? "r+" : "r");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    return false;
  }
  try {
    const stat = fs.fstatSync(fd);
    fs.fsyncSync(fd);
    // Bind every namespace hop to the opened inode and recheck after all barriers.
    syncPathNamespace(file, stat);
    visit(fd, stat);
    return true;
  } finally {
    fs.closeSync(fd);
  }
};

export interface SessionReceiptManager extends SessionEntryCursorManager {
  getEntries(): readonly unknown[];
  getSessionFile?(): string | undefined;
  isPersisted?(): boolean;
}

type ReceiptMatcher = (line: string) => boolean;
type ReceiptProjector = (entry: unknown) => unknown;
type ReceiptEntryMatcher = (entry: unknown) => boolean;
const identityReceipt: ReceiptProjector = (entry) => entry;
export interface SessionReceiptSnapshot {
  count: number;
  entries: ReadonlyMap<number, unknown>;
  /** Opaque confirmed-prefix identity and this snapshot's delta. Absent on legacy hosts. */
  update?: { generation: object; from: number; entries: ReadonlyMap<number, unknown> };
}
interface ReceiptCache extends SessionReceiptSnapshot {
  file: string;
  sessionId: string | undefined;
  matches: ReceiptMatcher;
  matchesEntry?: ReceiptEntryMatcher;
  project: ReceiptProjector;
  stat: fs.Stats;
  carry: readonly Buffer[];
  /** SHA-256 state over every observed byte, including the incomplete final line. */
  hash: Hash;
}
// One current file/filter per manager, released with the host session manager. Filters must
// have stable identity; a different predicate must never inherit another filter's receipts.
const receiptCaches = new WeakMap<SessionReceiptManager, ReceiptCache>();
interface MemoryReceiptCache {
  cursor: SessionEntryCursor;
  count: number;
  entries: Map<number, unknown>;
  snapshot?: SessionReceiptSnapshot;
}
const memoryReceiptCaches = new WeakMap<SessionReceiptManager, Map<ReceiptMatcher, Map<ReceiptProjector, Map<ReceiptEntryMatcher | undefined, MemoryReceiptCache>>>>();
// Lazily initialize an untouched SHA-256 seed; copies start fresh prefix checks.
// Keep byte verification independent of receipt-identity hashing. Never update the seed itself.
let receiptHashSeed: Hash | undefined;
const newReceiptHash = (): Hash => (receiptHashSeed ??= createHash("sha256")).copy();

const readAt = (fd: number, buffer: Buffer, position: number): void => {
  let read = 0;
  while (read < buffer.length) {
    const bytes = fs.readSync(fd, buffer, read, buffer.length - read, position + read);
    if (bytes <= 0) throw new Error("Session receipt changed while reading");
    read += bytes;
  }
};

/** Snapshot matching, complete JSONL entries only after confirming the native write.
 * Pi indexes entries before persistence and defers a fresh session until its first assistant.
 * Only genuinely in-memory sessions may use that index as delivery evidence. */
export const confirmedSessionReceiptSnapshot = (
  manager: SessionReceiptManager,
  matches: ReceiptMatcher,
  project: ReceiptProjector = identityReceipt,
  matchesEntry?: ReceiptEntryMatcher,
): SessionReceiptSnapshot => {
  const memory = (): SessionReceiptSnapshot => {
    // Current Pi exposes point lookups for the active branch. Re-derive once, then follow only
    // entries appended since the previous leaf; retain projected receipt data, not transcript bodies.
    if (typeof manager.getLeafEntry === "function" && typeof manager.getEntry === "function") {
      let byFilter = memoryReceiptCaches.get(manager);
      if (!byFilter) memoryReceiptCaches.set(manager, byFilter = new Map());
      let byProjector = byFilter.get(matches);
      if (!byProjector) byFilter.set(matches, byProjector = new Map());
      let byEntryMatcher = byProjector.get(project);
      if (!byEntryMatcher) byProjector.set(project, byEntryMatcher = new Map());
      let cache = byEntryMatcher.get(matchesEntry);
      if (!cache) {
        cache = { cursor: new SessionEntryCursor(), count: 0, entries: new Map() };
        byEntryMatcher.set(matchesEntry, cache);
      }
      const read = cache.cursor.read(manager);
      if (!read.reset && read.entries.length === 0 && cache.snapshot) return cache.snapshot;
      const generation = !read.reset && cache.snapshot?.update?.generation || {};
      if (read.reset) { cache.count = 0; cache.entries = new Map(); }
      const from = cache.count;
      const appended = new Map<number, unknown>();
      for (const entry of read.entries) {
        const index = cache.count++;
        const line = matchesEntry ? undefined : JSON.stringify(entry);
        if (matchesEntry ? matchesEntry(entry) : typeof line === "string" && matches(line)) {
          const projected = project(entry);
          cache.entries.set(index, projected);
          appended.set(index, projected);
        }
      }
      return cache.snapshot = { count: cache.count, entries: cache.entries, update: { generation, from, entries: appended } };
    }
    // Older hosts without point lookup methods retain their compatibility behavior.
    const entries = manager.getEntries();
    return { count: entries.length, entries: new Map(entries.map((entry, index) => [index, project(entry)])) };
  };
  const empty = (): SessionReceiptSnapshot => ({ count: 0, entries: new Map() });
  if (manager.isPersisted?.() === false) return memory();
  const file = manager.getSessionFile?.();
  if (!file) return manager.isPersisted?.() === true ? empty() : memory();
  let snapshot = empty();
  try {
    const found = withConfirmedSessionFile(file, (fd, stat) => {
      let previous = receiptCaches.get(manager);
      const sessionId = manager.getSessionId?.();
      if (previous?.file !== file || previous.sessionId !== sessionId || previous.matches !== matches || previous.matchesEntry !== matchesEntry || previous.project !== project ||
          previous.stat.dev !== stat.dev || previous.stat.ino !== stat.ino || stat.size < previous.stat.size) {
        previous = undefined;
      }
      // An unchanged file owes the native barriers above, but no transcript reads. ctime
      // detects an in-place rewrite even if its writer restores the old mtime.
      if (previous && stat.size === previous.stat.size && stat.mtimeMs === previous.stat.mtimeMs &&
          stat.ctimeMs === previous.stat.ctimeMs) {
        snapshot = previous;
        return;
      }
      // Stat changes can hide rewrites anywhere in the old bytes, not just at the tail.
      // Verify the entire observed prefix (including carry) before retaining any projections.
      // On a true append this verified hash can continue directly over the new bytes.
      const buffer = Buffer.allocUnsafe(1 << 20);
      let hash = newReceiptHash();
      if (previous) {
        for (let position = 0; position < previous.stat.size;) {
          const length = Math.min(buffer.length, previous.stat.size - position);
          const view = buffer.subarray(0, length);
          readAt(fd, view, position);
          hash.update(view);
          position += length;
        }
        if (!hash.copy().digest().equals(previous.hash.copy().digest())) {
          previous = undefined;
          hash = newReceiptHash();
        }
      }
      let position = previous?.stat.size ?? 0;
      let count = previous?.count ?? 0;
      const entries = new Map(previous?.entries);
      const appended = new Map<number, unknown>();
      let carry = [...(previous?.carry ?? [])];
      while (position < stat.size) {
        const length = Math.min(buffer.length, stat.size - position);
        readAt(fd, buffer.subarray(0, length), position);
        const view = buffer.subarray(0, length);
        hash.update(view);
        let start = 0;
        for (let newline = view.indexOf(10); newline !== -1; newline = view.indexOf(10, start)) {
          const line = Buffer.concat([...carry, view.subarray(start, newline)]).toString("utf8");
          carry = [];
          start = newline + 1;
          // Count every complete line, including malformed and unrelated entries.
          const index = count++;
          if (matches(line)) {
            let entry: unknown, parsed = false;
            try { entry = JSON.parse(line); parsed = true; } catch { /* Malformed entries are not receipts. */ }
            // Keep only the consumer's receipt fields, not another complete
            // copy of every delivered body. Projection errors fail confirmation.
            if (parsed && (!matchesEntry || matchesEntry(entry))) {
              const projected = project(entry);
              entries.set(index, projected);
              appended.set(index, projected);
            }
          }
        }
        // Retain only one partial line, never unrelated complete history.
        position += length;
        if (start < length) carry.push(Buffer.from(view.subarray(start)));
      }
      const update = { generation: previous?.update?.generation ?? {}, from: previous?.count ?? 0, entries: appended };
      const cache: ReceiptCache = { file, sessionId, matches, ...(matchesEntry ? { matchesEntry } : {}), project, stat, count, entries, update, carry, hash };
      receiptCaches.set(manager, cache);
      snapshot = cache;
    });
    if (!found) receiptCaches.delete(manager);
    return snapshot;
  } catch (error) {
    // Never let a failed confirmation/read poison a later successful receipt snapshot.
    receiptCaches.delete(manager);
    throw error;
  }
};

/** Compatibility array view; root-inbox consumes the indexed Map directly to avoid holes. */
export const confirmedSessionEntries = (manager: SessionReceiptManager, matches: ReceiptMatcher): readonly unknown[] => {
  const hasPointLookups = typeof manager.getLeafEntry === "function" && typeof manager.getEntry === "function";
  if (manager.isPersisted?.() === false && !hasPointLookups) return manager.getEntries();
  if (!manager.getSessionFile?.() && manager.isPersisted?.() !== true && !hasPointLookups) return manager.getEntries();
  if (!manager.getSessionFile?.() && manager.isPersisted?.() === true) return [];
  const snapshot = confirmedSessionReceiptSnapshot(manager, matches);
  const entries: unknown[] = [];
  for (const [index, entry] of snapshot.entries) entries[index] = entry;
  entries.length = snapshot.count;
  return entries;
};
