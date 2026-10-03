import fs from "node:fs";
import crypto from "node:crypto";

export interface SessionStamp {
  dev: bigint;
  ino: bigint;
  size: bigint;
  mtimeNs: bigint;
  ctimeNs: bigint;
}
const snapshots = new Map<string, SessionStamp | undefined>();
/** Synchronous read groups share one observation, never a timed/TTL freshness waiver.
 * Final freshness checks must start a new group. */
export const withSessionFileSnapshot = <T>(file: string, operation: () => T): T => {
  if (snapshots.has(file)) return operation();
  const observed = sessionStamp(file);
  snapshots.set(file, observed);
  try { return operation(); } finally { snapshots.delete(file); }
};
export const sessionStamp = (file: string): SessionStamp | undefined =>
  snapshots.has(file) ? snapshots.get(file) : statSessionFile(file);
const statSessionFile = (file: string): SessionStamp | undefined => {
  try {
    const stat = fs.statSync(file, { bigint: true });
    return stat.isFile() ? { dev: stat.dev, ino: stat.ino, size: stat.size, mtimeNs: stat.mtimeNs, ctimeNs: stat.ctimeNs } : undefined;
  } catch { return undefined; }
};
const sameStamp = (a: SessionStamp, b: SessionStamp): boolean =>
  a.dev === b.dev && a.ino === b.ino && a.size === b.size && a.mtimeNs === b.mtimeNs && a.ctimeNs === b.ctimeNs;

interface Reduction<T = unknown> {
  value: T;
  consume: (value: T, record: unknown) => void;
}
interface SessionCache {
  stamp: SessionStamp;
  hash: crypto.Hash;
  sourceHash: string;
  offset: number;
  completeOffset: number;
  pending: Buffer;
  reductions: Map<string, Reduction>;
}
// Bound the number of retained files/policy variants. No raw records or transcript
// strings survive a scan: only reducers' results and the unfinished line do.
const cache = new Map<string, SessionCache>();
const CHUNK_BYTES = 64 * 1024;
const MAX_FILES = 16;
const MAX_VARIANTS = 8;

const scan = (file: string, state: SessionCache, reductions: Reduction[], hash: boolean): void => {
  const fd = fs.openSync(file, "r");
  try {
    const opened = fs.fstatSync(fd, { bigint: true });
    if (!sameStamp(state.stamp, opened)) throw new Error("Session changed before scan");
    const chunk = Buffer.alloc(CHUNK_BYTES);
    const fragments: Buffer[] = state.pending.length ? [state.pending] : [];
    let position = state.offset;
    while (position < Number(state.stamp.size)) {
      const count = fs.readSync(fd, chunk, 0, Math.min(chunk.length, Number(state.stamp.size) - position), position);
      if (!count) throw new Error("Session truncated during scan");
      const bytes = chunk.subarray(0, count);
      if (hash) state.hash.update(bytes);
      let start = 0;
      for (let end = bytes.indexOf(10); end !== -1; end = bytes.indexOf(10, start)) {
        const segment = bytes.subarray(start, end);
        const line = fragments.length ? Buffer.concat([...fragments, segment]).toString("utf8") : segment.toString("utf8");
        fragments.length = 0;
        if (line.trim()) {
          let record: unknown;
          try { record = JSON.parse(line); } catch { start = end + 1; state.completeOffset = position + start; continue; }
          for (const reduction of reductions) reduction.consume(reduction.value, record);
        }
        start = end + 1;
        state.completeOffset = position + start;
      }
      if (start < count) fragments.push(Buffer.from(bytes.subarray(start)));
      position += count;
    }
    const canonical = statSessionFile(file);
    if (!canonical || !sameStamp(state.stamp, canonical) || !sameStamp(state.stamp, fs.fstatSync(fd, { bigint: true }))) throw new Error("Session changed during scan");
    // Buffer, not decoded text: a UTF-8 code point may straddle the next append.
    state.pending = Buffer.concat(fragments);
    state.offset = position;
    if (hash) state.sourceHash = state.hash.copy().digest("hex");
  } finally { fs.closeSync(fd); }
};

const refresh = (file: string, observed: SessionStamp): SessionCache => {
  let state = cache.get(file);
  if (state && sameStamp(state.stamp, observed)) return state;
  if (state && state.stamp.dev === observed.dev && state.stamp.ino === observed.ino && observed.size > state.stamp.size) {
    state.stamp = observed;
    try { scan(file, state, [...state.reductions.values()], true); return state; }
    catch { cache.delete(file); throw new Error("Session changed during append"); }
  }
  state = { stamp: observed, hash: crypto.createHash("sha256"), sourceHash: "", offset: 0, completeOffset: 0, pending: Buffer.alloc(0), reductions: new Map() };
  // The first reducer is installed before scanning by readSessionDerived.
  cache.delete(file);
  cache.set(file, state);
  while (cache.size > MAX_FILES) cache.delete(cache.keys().next().value!);
  return state;
};

/** One stat on the idle path; no reads, decoding, JSON parsing, or hash work. */
export const readSessionDerived = <T>(
  file: string, key: string, create: () => T, consume: (value: T, record: unknown) => void,
  observed = sessionStamp(file),
): T | undefined => {
  if (!observed) { cache.delete(file); return undefined; }
  try {
    const state = refresh(file, observed);
    const existing = state.reductions.get(key);
    if (existing) return existing.value as T;
    const reduction: Reduction<T> = { value: create(), consume };
    if (state.offset === 0 && !state.sourceHash) {
      state.reductions.set(key, reduction as Reduction);
      scan(file, state, [reduction as Reduction], true);
    } else {
      // A new policy needs its own initial projection, never retained raw text.
      const projection = { ...state, offset: 0, completeOffset: 0, pending: Buffer.alloc(0) };
      scan(file, projection, [reduction as Reduction], false);
      state.reductions.set(key, reduction as Reduction);
    }
    while (state.reductions.size > MAX_VARIANTS) state.reductions.delete(state.reductions.keys().next().value!);
    return reduction.value;
  } catch { cache.delete(file); return undefined; }
};

export const sessionFingerprint = (file: string): { mtime: number; size: number; sourceHash: string } | null => {
  const observed = sessionStamp(file);
  if (!observed) { cache.delete(file); return null; }
  if (readSessionDerived(file, "fingerprint", () => null, () => {}, observed) === undefined) return null;
  const state = cache.get(file)!;
  const key = "fingerprint-result";
  const existing = state.reductions.get(key)?.value as { stamp: SessionStamp; result: { mtime: number; size: number; sourceHash: string } } | undefined;
  if (existing && sameStamp(existing.stamp, observed)) return existing.result;
  const result = { mtime: Number(observed.mtimeNs) / 1e6, size: Number(observed.size), sourceHash: state.sourceHash };
  state.reductions.set(key, { value: { stamp: observed, result }, consume: () => {} });
  return result;
};

/** Test diagnostics deliberately expose sizes/keys, never raw transcript data. */
export const sessionCacheUsage = (): Array<{ file: string; pendingBytes: number; completeOffset: number; variants: string[] }> =>
  [...cache].map(([file, state]) => ({ file, pendingBytes: state.pending.length, completeOffset: state.completeOffset, variants: [...state.reductions.keys()] }));
export const clearSessionTextCache = (): void => cache.clear();
