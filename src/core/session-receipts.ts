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

/** Snapshot matching, complete JSONL entries only after confirming the native write.
 * Pi indexes entries before persistence and defers a fresh session until its first assistant.
 * Only genuinely in-memory sessions may use that index as delivery evidence. */
export const confirmedSessionEntries = (
  manager: SessionReceiptManager,
  matches: (line: string) => boolean,
): readonly unknown[] => {
  if (manager.isPersisted?.() === false) return manager.getEntries();
  const file = manager.getSessionFile?.();
  if (!file) return manager.isPersisted?.() === true ? [] : manager.getEntries();
  const entries: unknown[] = [];
  withConfirmedSessionFile(file, (fd, stat) => {
    const buffer = Buffer.allocUnsafe(1 << 20);
    let position = 0;
    let carry: Buffer[] = [];
    while (position < stat.size) {
      const read = fs.readSync(fd, buffer, 0, Math.min(buffer.length, stat.size - position), position);
      if (read <= 0) throw new Error("Session receipt changed while reading");
      const view = buffer.subarray(0, read);
      let start = 0;
      for (let newline = view.indexOf(10); newline !== -1; newline = view.indexOf(10, start)) {
        const line = Buffer.concat([...carry, view.subarray(start, newline)]).toString("utf8");
        carry = [];
        start = newline + 1;
        // Preserve history positions for compatibility lookbacks without retaining unrelated entries.
        const index = entries.length++;
        if (matches(line)) {
          try { entries[index] = JSON.parse(line); } catch { /* Torn/malformed entries are not receipts. */ }
        }
      }
      position += read;
      if (start < read) carry.push(Buffer.from(view.subarray(start)));
    }
    // A partial final line is not a confirmed entry.
  });
  return entries;
};
