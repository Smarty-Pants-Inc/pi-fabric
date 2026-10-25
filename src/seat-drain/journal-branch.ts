import { createHash } from "node:crypto";
import fs from "node:fs";

/**
 * A Pi session journal, read-only (the D3 bridge seam's `resume` read, rule T-FIX3): one pass over one no-follow
 * descriptor gives
 * - the current leaf (Pi takes the last entry on load) and whether the sealed leaf is on the current branch, with the
 *   entries after it there, each exactly {id, parentId, type, thinkingLevel} (thinkingLevel only for
 *   thinking_level_change, else null);
 * - the sha256 of the whole file and of its first `sealedBytes` bytes (the final seal's length);
 * - the bytes after `sealedBytes` (the suffix), kept only up to SUFFIX_MAX.
 * Message text is never retained or returned beyond the suffix bytes themselves.
 */
export const ENTRY_ID = /^[A-Za-z0-9._:-]{1,256}$/;
export const SUFFIX_MAX = 64 * 1024;
const MAX_APPENDED = 1_000;
const CHUNK = 1024 * 1024;

export interface AppendedEntry { id: string; parentId: string | null; type: string; thinkingLevel: string | null }

export interface JournalBranch {
  /** The current leaf: the last entry's id (null for an empty journal). */
  leafId: string | null;
  entries: number;
  /** The sealed leaf is the current leaf or one of its ancestors. */
  sealOnBranch: boolean;
  /** The entries after the sealed leaf on the current branch, in order (null when the seal is not on it). */
  appendedSinceSeal: AppendedEntry[] | null;
  truncated: boolean;
  journalSha256: string;
  journalBytes: number;
  /** sha256 of the first `sealedBytes` bytes; null when the file is shorter. */
  sealedSha256: string | null;
  suffixBytes: number;
  /** The suffix bytes, base64; null when longer than SUFFIX_MAX. */
  suffixBase64: string | null;
}

export class JournalBranchError extends Error {
  constructor(readonly code: string) { super(code); this.name = "JournalBranchError"; }
}

/** One entry's routing facts from a journal line; null for the session header. */
export const entryFacts = (line: string): (AppendedEntry & { parentId: string | null }) | null => {
  let value: Record<string, unknown>;
  try { value = JSON.parse(line) as Record<string, unknown>; } catch { throw new JournalBranchError("JOURNAL_LINE_INVALID"); }
  if (typeof value !== "object" || value === null) throw new JournalBranchError("JOURNAL_LINE_INVALID");
  if (value.type === "session") return null;
  if (typeof value.id !== "string" || !ENTRY_ID.test(value.id) || typeof value.type !== "string" ||
    !(value.parentId === null || (typeof value.parentId === "string" && ENTRY_ID.test(value.parentId)))) {
    throw new JournalBranchError("JOURNAL_ENTRY_INVALID");
  }
  return { id: value.id, parentId: value.parentId as string | null, type: value.type.slice(0, 64),
    thinkingLevel: value.type === "thinking_level_change" && typeof value.thinkingLevel === "string" ? value.thinkingLevel.slice(0, 64) : null };
};

/** `ids`, when given, collects every entry id in the file (the bridge seam's check that appended entries are new). */
export const journalBranch = (file: string, sealLeafId: string, sealedBytes = 0, maxBytes = 1024 * 1024 * 1024,
  ids?: Set<string>): JournalBranch => {
  const parents = new Map<string, string | null>();
  const facts = new Map<string, AppendedEntry>();
  let leafId: string | null = null;
  let entries = 0;
  const whole = createHash("sha256");
  const prefix = createHash("sha256");
  let total = 0;
  const suffix: Buffer[] = [];
  let suffixBytes = 0;
  const visit = (line: string): void => {
    if (!line.trim()) return;
    const entry = entryFacts(line);
    if (!entry) return;
    entries += 1;
    ids?.add(entry.id);
    parents.set(entry.id, entry.parentId);
    facts.set(entry.id, entry);
    leafId = entry.id;
  };
  let fd: number | undefined;
  try {
    fd = fs.openSync(file, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
    const stat = fs.fstatSync(fd);
    if (!stat.isFile()) throw new JournalBranchError("JOURNAL_NOT_REGULAR");
    if (stat.size > maxBytes) throw new JournalBranchError("JOURNAL_TOO_LARGE");
    const buffer = Buffer.alloc(CHUNK);
    let rest = Buffer.alloc(0);
    for (;;) {
      const read = fs.readSync(fd, buffer, 0, CHUNK, null);
      if (read === 0) break;
      const chunk = buffer.subarray(0, read);
      whole.update(chunk);
      // The sealed prefix and the suffix after it, split at `sealedBytes`.
      const inPrefix = Math.max(0, Math.min(read, sealedBytes - total));
      if (inPrefix > 0) prefix.update(chunk.subarray(0, inPrefix));
      if (inPrefix < read) {
        const tail = chunk.subarray(inPrefix);
        suffixBytes += tail.length;
        if (suffixBytes <= SUFFIX_MAX) suffix.push(Buffer.from(tail));
      }
      total += read;
      if (total > maxBytes) throw new JournalBranchError("JOURNAL_TOO_LARGE");
      let data = Buffer.concat([rest, chunk]);
      for (let newline = data.indexOf(0x0a); newline >= 0; newline = data.indexOf(0x0a)) {
        visit(data.subarray(0, newline).toString("utf8"));
        data = data.subarray(newline + 1);
      }
      rest = Buffer.from(data);
    }
    if (rest.length > 0) visit(rest.toString("utf8"));
  } catch (error) {
    if (error instanceof JournalBranchError) throw error;
    throw new JournalBranchError("JOURNAL_UNREADABLE");
  } finally { if (fd !== undefined) fs.closeSync(fd); }
  // The branch, leaf to root, then in order. A cycle or a missing parent is a broken journal, not a branch.
  const branch: string[] = [];
  const seen = new Set<string>();
  for (let id: string | null = leafId; id !== null; id = parents.get(id) ?? null) {
    if (seen.has(id) || !parents.has(id)) throw new JournalBranchError("JOURNAL_BRANCH_BROKEN");
    seen.add(id);
    branch.push(id);
  }
  branch.reverse();
  const at = branch.indexOf(sealLeafId);
  const after = at < 0 ? [] : branch.slice(at + 1);
  return {
    leafId, entries, sealOnBranch: at >= 0, truncated: after.length > MAX_APPENDED,
    appendedSinceSeal: at < 0 ? null : after.slice(0, MAX_APPENDED).map(id => ({ ...facts.get(id)! })),
    journalSha256: whole.digest("hex"), journalBytes: total,
    sealedSha256: total >= sealedBytes ? prefix.digest("hex") : null,
    suffixBytes, suffixBase64: suffixBytes <= SUFFIX_MAX ? Buffer.concat(suffix).toString("base64") : null,
  };
};
