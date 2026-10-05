import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { writeFileAtomic } from "../core/atomic-write.js";
import type { MeshStateEntry } from "./store.js";

/** Optional read acceleration, not write authority. Old writers need not publish it. */
export interface JournalState {
  format: 1 | 2;
  readGeneration?: string;
  readJournalHash?: string;
  entries: Record<string, MeshStateEntry>;
  versions?: Record<string, number>;
  tombstoneOrder?: string[];
  revisionFormat?: 2;
  highWater?: number;
}
export interface JournalBase {
  generation: string | undefined;
  identity: string | undefined;
  hash: string | undefined;
  versionKeys: string[];
  tombstoneOrder: string;
}
export interface JournalCursor { inode: string; offset: number }
const MAX_JOURNAL_BYTES = 2 * 1024 * 1024;
const MAX_RECORD_BYTES = 256 * 1024;
const journalPath = (root: string): string => path.join(root, "state.read-journal.jsonl");
const digest = (text: string | Buffer): string => createHash("sha256").update(text).digest("hex");
const identityOf = (stat: fs.BigIntStats): string | undefined =>
  typeof stat.mtimeNs === "bigint" && typeof stat.ctimeNs === "bigint"
    ? `${stat.dev}:${stat.ino}:${stat.size}:${stat.mtimeNs}:${stat.ctimeNs}:${stat.birthtimeNs}` : undefined;
const uuid = (value: unknown): value is string => typeof value === "string" &&
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(value);
const record = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);
const own = (object: object, key: string): boolean => Object.hasOwn(object, key);

// Windows replacement changes the file id/creation time, not necessarily its rounded
// mtime or size. Bind that physical key to actual bytes too. Memoize only when the
// adapter supplies a usable file id and change time; otherwise hash each observation.
const windowsIdentities = new Map<string, { physical: string; identity: string }>();
/** Nanosecond metadata includes rename/in-place identity, not just lossy mtime milliseconds.
 * If the filesystem/adapter cannot supply it, keep the conservative header/payload fallback. */
export const stateReadIdentity = (file: string, maxBytes = Number.POSITIVE_INFINITY): string | undefined => {
  try {
    const stat = fs.statSync(file, { bigint: true });
    if (stat.size > maxBytes) return undefined; // Do not hash past a caller's read budget.
    const physical = identityOf(stat);
    if (physical === undefined || process.platform !== "win32") return physical;
    const key = path.resolve(file);
    const reusable = stat.ino > 0n && stat.ctimeNs > 0n && stat.birthtimeNs > 0n;
    const cached = reusable ? windowsIdentities.get(key) : undefined;
    if (cached?.physical === physical) return cached.identity;
    // A path read owns and closes its own handle. Never retain a descriptor across
    // observations: a replaced path can name a different file on Windows.
    const bytes = fs.readFileSync(file, { flag: "r" });
    if (BigInt(bytes.length) !== stat.size || identityOf(fs.statSync(file, { bigint: true })) !== physical) return undefined;
    const identity = `${physical}:${digest(bytes)}`;
    if (reusable) {
      if (windowsIdentities.size >= 64 && !windowsIdentities.has(key)) {
        windowsIdentities.delete(windowsIdentities.keys().next().value!);
      }
      windowsIdentities.set(key, { physical, identity });
    }
    return identity;
  } catch { return undefined; }
};

export const journalBase = (state: JournalState, file: string): JournalBase => ({
  generation: uuid(state.readGeneration) ? state.readGeneration : undefined,
  identity: stateReadIdentity(file),
  hash: state.readJournalHash,
  versionKeys: Object.keys(state.versions ?? {}),
  tombstoneOrder: JSON.stringify(state.tombstoneOrder),
});

/** Verify the hash committed in the delta body against canonical bytes, NOT the sidecar's
 * self-checksum. Only the second-field readJournalHash is excluded to avoid self-reference;
 * every other byte (including UUID, envelope, entries, ordering and UTF-8) is covered.
 * Read once per changed physical generation without parsing/traversal. POSIX streams
 * bounded chunks; Windows closes a path read before validating the captured bytes.
 * Descriptor/path identities stay pinned; races/failures only disable acceleration. */
const verifyCanonicalPayload = (root: string, generation: string, chainHash: string,
  identity: string, expected: unknown): boolean => {
  if (typeof expected !== "string" || !/^[0-9a-f]{64}$/.test(expected)) return false;
  const file = path.join(root, "state.json");
  let fd: number | undefined;
  try {
    if (process.platform === "win32") {
      // Holding a target handle while verifying can prevent an atomic replacement.
      // Read by path, close the handle, then validate/hash the captured bytes. The
      // post-read identity rejects a successful replacement; fallback opens the
      // canonical path anew after this function has released every handle.
      if (stateReadIdentity(file) !== identity) return false;
      const bytes = fs.readFileSync(file);
      const generationField = `{"readGeneration":"${generation}"`;
      const prefix = Buffer.from(`${generationField},"readJournalHash":"${chainHash}"`);
      return bytes.subarray(0, prefix.length).equals(prefix) &&
        createHash("sha256").update(generationField).update(bytes.subarray(prefix.length)).digest("hex") === expected &&
        stateReadIdentity(file) === identity;
    }
    fd = fs.openSync(file, "r");
    const stat = fs.fstatSync(fd, { bigint: true });
    if (identityOf(stat) !== identity || stat.size > BigInt(Number.MAX_SAFE_INTEGER)) return false;
    const generationField = `{"readGeneration":"${generation}"`;
    const prefix = Buffer.from(`${generationField},"readJournalHash":"${chainHash}"`);
    const header = Buffer.allocUnsafe(prefix.length);
    if (fs.readSync(fd, header, 0, header.length, 0) !== header.length || !header.equals(prefix)) return false;
    const hash = createHash("sha256").update(generationField);
    const buffer = Buffer.allocUnsafe(64 * 1024);
    const size = Number(stat.size);
    for (let offset = prefix.length; offset < size;) {
      const length = Math.min(buffer.length, size - offset);
      if (fs.readSync(fd, buffer, 0, length, offset) !== length) return false;
      hash.update(buffer.subarray(0, length));
      offset += length;
    }
    return hash.digest("hex") === expected && identityOf(fs.fstatSync(fd, { bigint: true })) === identity &&
      stateReadIdentity(file) === identity;
  } catch { return false; }
  finally { if (fd !== undefined) try { fs.closeSync(fd); } catch { /* best effort */ } }
};

interface PreparedStateJournal { hash: string; text: string }
/** Prepare a hash chain before the canonical rename. Its hash is committed IN state.json's
 * bounded header, so a forged/self-checksummed sidecar cannot supply fresh authority. */
export const prepareStateJournal = (state: JournalState, base: JournalBase | undefined,
  changedKeys: readonly string[], encodedEntries: ReadonlyMap<string, { entry: Buffer }>, canonicalPayload: Buffer): PreparedStateJournal | undefined => {
  try {
    if (!base?.generation || !base.identity || !uuid(state.readGeneration)) return undefined;
    // Reuse the exact canonical entry bytes: neither chain hashing nor publication should
    // traverse changed payloads again (including large batches and UTF-8/escaped keys).
    const entries: Record<string, string> = Object.create(null);
    const versions: Record<string, number | null> = Object.create(null);
    for (const key of new Set(changedKeys)) {
      entries[key] = own(state.entries, key) ? encodedEntries.get(key)!.entry.toString("utf8") : "null";
      versions[key] = state.versions?.[key] ?? null;
    }
    for (const key of base.versionKeys) if (!own(state.versions ?? {}, key)) versions[key] = null;
    const { entries: _entries, versions: _versions, readGeneration: _generation, readJournalHash: _hash, tombstoneOrder, ...envelope } = state;
    const head = JSON.stringify({ format: 1, previous: base.generation, previousIdentity: base.identity, previousHash: base.hash ?? null,
      generation: state.readGeneration, canonicalPayloadHash: digest(canonicalPayload), envelope });
    const members = Object.keys(entries).map(key => `${JSON.stringify(key)}:${entries[key]}`).join(",");
    const tombstones = JSON.stringify(tombstoneOrder);
    const text = `${head.slice(0, -1)},"entries":{${members}},"versions":${JSON.stringify(versions)}` +
      (tombstones === base.tombstoneOrder ? "" : `,"tombstoneOrder":${tombstones ?? "[]"}`) + "}";
    if (Buffer.byteLength(text) > MAX_RECORD_BYTES - 1024) return undefined;
    return { hash: digest(text), text };
  } catch { return undefined; }
};

/** Called after canonical rename under the existing lock. Missing/capped/failed publication
 * breaks the chain; readers fall back. Atomic rotation bounds history, with no extra timer/lock. */
export const appendStateJournal = (root: string, prepared: PreparedStateJournal | undefined, stamp: string): void => {
  try {
    if (!prepared) return;
    const identity = stateReadIdentity(path.join(root, "state.json"));
    if (!identity) return;
    const delta = `${prepared.text.slice(0, -1)},"identity":${JSON.stringify(identity)},"stamp":${JSON.stringify(stamp)}}`;
    const line = `{"checksum":"${digest(delta)}","delta":${delta}}\n`;
    if (Buffer.byteLength(line) > MAX_RECORD_BYTES) return;
    const file = journalPath(root);
    let size = 0;
    try { size = fs.statSync(file).size; } catch { /* absent */ }
    if (size + Buffer.byteLength(line) > MAX_JOURNAL_BYTES) writeFileAtomic(file, line);
    else fs.appendFileSync(file, line, { mode: 0o600 });
  } catch { /* A sidecar cannot reject or hide a canonical commit. */ }
};

/** Incremental replay pinned to the actual canonical payload. Terminal metadata alone is
 * untrusted: its chain-bound payload hash must match bytes read from the pinned canonical
 * descriptor. Missing/failed bindings fall back. Existing opaque snapshots stay immutable. */
export const replayStateJournal = (root: string, base: JournalState, generation: string, identity: string,
  stamp: string, baseIdentity: string | undefined, canonicalHash: string | undefined, cursor?: JournalCursor): { state: JournalState; cursor: JournalCursor } | undefined => {
  let fd: number | undefined;
  try {
    if (!uuid(base.readGeneration) || base.readGeneration === generation || !canonicalHash) return undefined;
    fd = fs.openSync(journalPath(root), "r");
    const stat = fs.fstatSync(fd);
    const inode = `${stat.dev}:${stat.ino}`;
    if (stat.size > MAX_JOURNAL_BYTES) return undefined;
    const offset = cursor?.inode === inode && cursor.offset <= stat.size ? cursor.offset : 0;
    const buffer = Buffer.allocUnsafe(stat.size - offset);
    if (fs.readSync(fd, buffer, 0, buffer.length, offset) !== buffer.length) return undefined;
    const text = buffer.toString("utf8");
    if (!text.endsWith("\n")) return undefined;
    let state = base, last: Record<string, unknown> | undefined;
    let previousIdentity = baseIdentity;
    let consumed = 0;
    for (const line of text.slice(0, -1).split("\n")) {
      const bytes = Buffer.byteLength(line);
      if (bytes > MAX_RECORD_BYTES) return undefined;
      consumed += bytes + 1;
      const row: unknown = JSON.parse(line);
      if (!record(row) || !record(row.delta) || row.checksum !== digest(JSON.stringify(row.delta))) return undefined;
      const delta = row.delta;
      // Before the cached generation: harmless history. Once replay started, gaps are fatal.
      if (delta.previous !== state.readGeneration) {
        if (state === base) continue;
        return undefined;
      }
      const { identity: _identity, stamp: _stamp, ...body } = delta;
      const hash = digest(JSON.stringify(body));
      if (delta.previousHash !== (state.readJournalHash ?? null) || delta.previousIdentity !== previousIdentity || delta.format !== 1 || !uuid(delta.generation) || !record(delta.envelope) ||
        ![1, 2].includes(delta.envelope.format as number) || !record(delta.entries) || !record(delta.versions) ||
        ["entries", "versions", "readGeneration", "readJournalHash", "tombstoneOrder"].some(key => own(delta.envelope as object, key))) return undefined;
      const entries = { ...state.entries }, versions = { ...state.versions };
      for (const [key, entry] of Object.entries(delta.entries)) {
        if (entry === null) delete entries[key];
        else {
          if (!record(entry) || entry.key !== key || !Number.isSafeInteger(entry.version) ||
            typeof entry.updatedAt !== "number" || !record(entry.updatedBy)) return undefined;
          Object.defineProperty(entries, key, { value: entry, enumerable: true, configurable: true, writable: true });
        }
      }
      for (const [key, version] of Object.entries(delta.versions)) {
        if (version === null) delete versions[key];
        else {
          if (!Number.isSafeInteger(version) || (version as number) < 0) return undefined;
          Object.defineProperty(versions, key, { value: version, enumerable: true, configurable: true, writable: true });
        }
      }
      const tombstones = delta.tombstoneOrder ?? state.tombstoneOrder;
      if (tombstones !== undefined && (!Array.isArray(tombstones) || tombstones.some(key => typeof key !== "string"))) return undefined;
      state = { ...delta.envelope, format: delta.envelope.format as 1 | 2, readGeneration: delta.generation, readJournalHash: hash,
        entries, versions, ...(tombstones === undefined ? {} : { tombstoneOrder: tombstones as string[] }) };
      last = delta;
      previousIdentity = typeof delta.identity === "string" ? delta.identity : undefined;
      if (state.readGeneration === generation) break;
    }
    if (!last || state.readGeneration !== generation || state.readJournalHash !== canonicalHash || last.identity !== identity || last.stamp !== stamp) return undefined;
    if (!verifyCanonicalPayload(root, generation, canonicalHash, identity, last.canonicalPayloadHash)) return undefined;
    // The descriptor must still contain exactly the captured prefix (no in-place truncation).
    const after = fs.fstatSync(fd);
    if (after.ino !== stat.ino || after.size < stat.size) return undefined;
    // The captured journal can include a later writer's record. Advance only through the
    // endpoint actually replayed, so the next read cannot skip that unconsumed UTF-8 tail.
    return { state, cursor: { inode, offset: offset + consumed } };
  } catch { return undefined; }
  finally { if (fd !== undefined) try { fs.closeSync(fd); } catch { /* best effort */ } }
};
