import fs from "node:fs";
import { syncPathNamespace } from "./atomic-write.js";

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

export interface SessionReceiptManager {
  getEntries(): readonly unknown[];
  getSessionFile?(): string | undefined;
  isPersisted?(): boolean;
}

type ReceiptMatcher = (line: string) => boolean;
type ReceiptProjector = (entry: unknown) => unknown;
const identityReceipt: ReceiptProjector = (entry) => entry;
export interface SessionReceiptSnapshot {
  count: number;
  entries: ReadonlyMap<number, unknown>;
}
interface ReceiptCache extends SessionReceiptSnapshot {
  file: string;
  matches: ReceiptMatcher;
  project: ReceiptProjector;
  stat: fs.Stats;
  /** Confirmed complete-line offset; stat.size also includes the unconfirmed carry. */
  offset: number;
  carry: readonly Buffer[];
  tail: Buffer;
  partialTail: Buffer;
}
// One current file/filter per manager, released with the host session manager. Filters must
// have stable identity; a different predicate must never inherit another filter's receipts.
const receiptCaches = new WeakMap<SessionReceiptManager, ReceiptCache>();
const TAIL_BYTES = 64;

const readAt = (fd: number, buffer: Buffer, position: number): void => {
  let read = 0;
  while (read < buffer.length) {
    const bytes = fs.readSync(fd, buffer, read, buffer.length - read, position + read);
    if (bytes <= 0) throw new Error("Session receipt changed while reading");
    read += bytes;
  }
};

const tailMatches = (fd: number, offset: number, tail: Buffer): boolean => {
  const current = Buffer.allocUnsafe(tail.length);
  readAt(fd, current, offset - tail.length);
  return current.equals(tail);
};

/** Snapshot matching, complete JSONL entries only after confirming the native write.
 * Pi indexes entries before persistence and defers a fresh session until its first assistant.
 * Only genuinely in-memory sessions may use that index as delivery evidence. */
export const confirmedSessionReceiptSnapshot = (
  manager: SessionReceiptManager,
  matches: ReceiptMatcher,
  project: ReceiptProjector = identityReceipt,
): SessionReceiptSnapshot => {
  const memory = (): SessionReceiptSnapshot => {
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
      if (previous?.file !== file || previous.matches !== matches || previous.project !== project ||
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
      // A changed file must still contain the confirmed prefix. Check the carry's end too:
      // a writer can replace an incomplete line before completing it.
      if (previous && (!tailMatches(fd, previous.offset, previous.tail) ||
          (previous.stat.size > previous.offset && !tailMatches(fd, previous.stat.size, previous.partialTail)))) {
        previous = undefined;
      }
      let position = previous?.stat.size ?? 0;
      let offset = previous?.offset ?? 0;
      let count = previous?.count ?? 0;
      const entries = new Map(previous?.entries);
      let carry = [...(previous?.carry ?? [])];
      let tail = previous?.tail ?? Buffer.alloc(0);
      let endTail = previous?.partialTail ?? tail;
      const buffer = Buffer.allocUnsafe(1 << 20);
      while (position < stat.size) {
        const length = Math.min(buffer.length, stat.size - position);
        readAt(fd, buffer.subarray(0, length), position);
        const view = buffer.subarray(0, length);
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
            if (parsed) entries.set(index, project(entry));
          }
          offset = position + start;
        }
        // Retain only small rewrite guards and one partial line, never unrelated history.
        if (start > 0) tail = Buffer.from(Buffer.concat([endTail, view.subarray(0, start)]).subarray(-TAIL_BYTES));
        endTail = Buffer.from(Buffer.concat([endTail, view]).subarray(-TAIL_BYTES));
        position += length;
        if (start < length) carry.push(Buffer.from(view.subarray(start)));
      }
      snapshot = { count, entries };
      receiptCaches.set(manager, { file, matches, project, stat, offset, count, entries, carry, tail,
        partialTail: endTail });
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
  if (manager.isPersisted?.() === false) return manager.getEntries();
  if (!manager.getSessionFile?.() && manager.isPersisted?.() !== true) return manager.getEntries();
  const snapshot = confirmedSessionReceiptSnapshot(manager, matches);
  const entries: unknown[] = [];
  for (const [index, entry] of snapshot.entries) entries[index] = entry;
  entries.length = snapshot.count;
  return entries;
};
