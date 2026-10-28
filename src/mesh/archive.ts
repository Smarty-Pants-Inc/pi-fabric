import { createHash } from "node:crypto";
import { ArchiveSha256, type Sha256State } from "./archive-sha256.js";
import { archiveRangeCache, type ArchiveRangeBlock, type ArchiveRangeIndex } from "./archive-range-cache.js";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { MeshSnapshotRetriesExhaustedError, writeFileAtomic } from "../core/atomic-write.js";
import type { MeshEvent } from "./store.js";
import type { IndexDirectoryBarrier } from "./receipt-namespace.js";

/**
 * The durable fleet log (smarty-dev#754, Paul's decision 2): every event of a mesh root as plain
 * append-only files, one per topic and UTC day, in the node's file tree (smarty-dev#1120):
 *
 *   <dir>/MESH.json                      the mesh root this archive belongs to
 *   <dir>/HEAD.json                      the last event known to be archived and live
 *   <dir>/PENDING.json                   a publish between its archive append and its commit
 *   <dir>/<yyyy>/<mm>/<dd>/<topic>.jsonl  one line per event, the same bytes as the live log
 *   <dir>/<yyyy>/<mm>/<dd>/SEAL.json     per file line count, sequence range and sha256
 *   <dir>/<yyyy>/<mm>/<dd>/.digest-*.json rebuildable SHA-256 checkpoints and counters
 * Range read hints live only in a bounded process-local cache. Legacy in-day .range-*
 * and .ranges.json sidecars are ignored and never modified, even on unsealed days.
 *   <dir>/DIGEST-REPAIR.json              deferred oversized legacy line (read off-lock)
 *   <dir>/<yyyy>/<mm>/<dd>/ABORTED.json  positive event-identity abort markers
 *   <dir>/sequence-index/<bucket>/<n>.json  exact line address and advisory live confirmation
 *
 * A publish normally commits when its event reaches the live log. Ordinary recovery cuts
 * pending appends back out; a keyed intent retry can first complete the live publication.
 * Old writers can restore/compact a live event without confirming its sidecar. Thus a false
 * marker (or PENDING) never hides archived bytes within the live sequence horizon and never
 * proves non-publication. An overtaken exact archive identity is retained, not republished:
 * prefer one archive delivery to loss when archive-only and old-compacted are ambiguous.
 * The mesh root enables the archive with `event-archive.json` ({ "version": 1, "dir": "/abs" }),
 * so every store of that root archives, whatever its process's configuration.
 * New writers call read-only preflights before acquiring the publish lock; mutation and
 * checkpoint installation remain under that lock. Legacy writers use the same lock and
 * ignore the new checkpoint/repair metadata, so optimistic reads are validated against them.
 */
export const MESH_ARCHIVE_CONFIG = "event-archive.json";
const MESH_ARCHIVE_SEQUENCE_INDEX = "sequence-index";

export interface MeshArchiveEntry {
  event: MeshEvent;
  line: string;
}

export interface MeshArchivePending {
  sequence: number;
  id: string;
  file: string;
  size: number;
  /** New writers restore this on rollback; old writers safely ignore it. */
  digestBefore?: DigestCheckpoint;
  /** Installed durably only after the live append has committed. */
  digestAfter?: DigestCheckpoint;
  /** New writers index the append before it can go live. Old writers ignore this field. */
  indexed?: true;
  /** Keyed publications also record their live commit in the sequence sidecar. */
  dedupe?: true;
}

interface MeshArchiveIndexEntry {
  sequence: number;
  id: string;
  file: string;
  offset: number;
  length: number;
  /** New-writer confirmation only. False cannot disprove an old-writer live recovery. */
  committed?: boolean;
}

interface ArchiveHead {
  sequence: number;
  id: string;
  file: string;
}

export interface MeshArchiveRecovery {
  /** This is the first publish of this boot: after a reboot, the synced archive lines are the truth. */
  rebooted: boolean;
  /** Archived events past the live log's end, in sequence order, to go live again. */
  promote: Array<MeshArchiveEntry & { file: string; offset?: number }>;
}

export interface MeshArchiveDigestRepair {
  file: string;
  identity: string;
  digest: DigestCheckpoint;
}

export interface MeshArchiveRecoveryPlan {
  dir: string;
  lastLive: number;
  metadata: string;
  pendingRepair?: { file: string; size: number; digest: DigestCheckpoint };
  identities: Array<{ file: string; identity: string }>;
  promote: Array<MeshArchiveEntry & { file: string; offset?: number }>;
}

/** An optimistic read raced an archive writer; retry before making any mutations. */
export class MeshArchiveRecoveryChanged extends Error {
  constructor() { super("Mesh archive changed during off-lock recovery"); }
}

interface SealFile {
  lines: number;
  firstSequence: number;
  lastSequence: number;
  sha256: string;
}

interface DigestCheckpoint {
  version: 1;
  identity: string;
  hash: Sha256State;
  lines: number;
  firstSequence: number;
  lastSequence: number;
}

// Range hints are derived only from scanned data, never from old on-disk sidecars.
const ARCHIVE_RANGE_BLOCK_BYTES = 2 * 1024;
const ARCHIVE_RANGE_MAX_BLOCKS = 1024;
// Names only, never segment identities. Bound both count and serialized name bytes.
const ARCHIVE_DAY_NAMES_CACHE_ENTRIES = 128;
const ARCHIVE_DAY_NAMES_CACHE_BYTES = 4 * 1024 * 1024;
const dayNamesCache = new Map<string, { identity: string; names: string[]; bytes: number }>();
let dayNamesCacheBytes = 0;
const forgetDayNames = (directory: string): void => {
  const cached = dayNamesCache.get(directory);
  if (cached) { dayNamesCacheBytes -= cached.bytes; dayNamesCache.delete(directory); }
};
interface ArchiveReadRequest { descriptor: number; position: number; length: number }
type ArchiveReadSteps<T> = Generator<ArchiveReadRequest | undefined, T, Buffer>;
interface ArchiveCandidate { event: MeshEvent; fileOrder: number; offset: number }
const compareArchiveCandidates = (a: ArchiveCandidate, b: ArchiveCandidate): number =>
  a.event.sequence - b.event.sequence || a.fileOrder - b.fileOrder || a.offset - b.offset;
const rangeOutside = (min: number | null, max: number | null, after: number, through: number): boolean =>
  (max !== null && max <= after) || (min !== null && min > through);

// Shared by all files/days touched by a sealing pass, not a per-file allowance.
export const ARCHIVE_DIGEST_SLICE_BYTES = 64 * 1024;
const fileIdentity = (stat: fs.Stats): string =>
  `${stat.dev}:${stat.ino}:${stat.size}:${stat.mtimeMs}:${stat.ctimeMs}`;

// Growth is NOT append proof: the same inode can be truncated, rewritten and regrown.
// Any changed identity invalidates both scanned bytes and pruned ranges, even beyond through.
const rangeReadCompatible = (before: string, current: string): boolean => before === current;
// The 200-append cold-read regression needs room for a bounded burst to settle while a
// multi-MB segment is read in 64 KiB slices. Never spin forever under continuous mutation.
const ARCHIVE_READ_ATTEMPTS = 8;

const errorCode = (error: unknown): string | undefined =>
  typeof error === "object" && error !== null && "code" in error ? String((error as { code: unknown }).code) : undefined;

export class MeshArchiveLookupUnavailableError extends Error {
  readonly retryable = true;
  constructor(message: string) {
    super(message);
    this.name = "MeshArchiveLookupUnavailableError";
  }
}

const pad = (value: number): string => String(value).padStart(2, "0");

// The topic grammar is file-safe except "/" and ":". "%" is outside it, so this reverses exactly.
// Encoding can make a valid 128-character topic longer than a file name may be (255 bytes):
// such a name keeps a prefix and ends in "~" and a hash of the topic. "~" is outside the grammar
// and the encoding, so it never equals another topic's plain name; the events keep the topic.
const MAX_FILE_NAME_BYTES = 255;
export const archiveFileName = (topic: string): string => {
  const encoded = topic.replaceAll("/", "%2F").replaceAll(":", "%3A");
  if (encoded.length + ".jsonl".length <= MAX_FILE_NAME_BYTES) return `${encoded}.jsonl`;
  return `${encoded.slice(0, 200)}~${createHash("sha256").update(topic).digest("hex").slice(0, 32)}.jsonl`;
};

const dayOf = (createdAt: number): string => {
  const date = new Date(createdAt);
  return `${date.getUTCFullYear()}/${pad(date.getUTCMonth() + 1)}/${pad(date.getUTCDate())}`;
};

const parseEvent = (line: string): MeshEvent | undefined => {
  try {
    const parsed = JSON.parse(line) as MeshEvent;
    return typeof parsed.sequence === "number" && typeof parsed.id === "string" ? parsed : undefined;
  } catch {
    return undefined;
  }
};

// Complete lines only: a last line without its newline is an append that never finished.
const completeLines = (text: string): string[] => {
  const lines = text.split("\n");
  lines.pop();
  return lines.filter(Boolean);
};

// fs.writeSync may write part of a buffer; an archived line must be whole before it counts.
const writeAll = (descriptor: number, bytes: Buffer): void => {
  for (let offset = 0; offset < bytes.length;) {
    const written = fs.writeSync(descriptor, bytes, offset, bytes.length - offset);
    if (written <= 0) throw new Error("Mesh event archive write made no progress");
    offset += written;
  }
};

// A process crash keeps the page cache, so every write before it survives. A power loss does
// not: only synced data does. BOOT, written durably once per boot before the boot's first
// archive append, tells recovery which of the two happened.
let bootIdentity: string | undefined;
export const currentBoot = (): string => {
  if (bootIdentity === undefined) {
    try {
      bootIdentity = fs.readFileSync("/proc/sys/kernel/random/boot_id", "utf8").trim();
    } catch {
      bootIdentity = `up:${Math.round((Date.now() / 1000 - os.uptime()) / 60)}`;
    }
  }
  return bootIdentity;
};

// A file's name lives in its directory, not in its data: a new name survives a power loss only
// once its directory is synced too.
const syncDirectory = (directory: string): void => {
  if (process.platform === "win32") return; // no directory handles to sync; NTFS journals names
  const descriptor = fs.openSync(directory, "r");
  try {
    fs.fsyncSync(descriptor);
  } finally {
    fs.closeSync(descriptor);
  }
};

const writeDurable = (file: string, text: string): void => {
  const temporary = `${file}.${process.pid}.tmp`;
  const descriptor = fs.openSync(temporary, "w", 0o600);
  try {
    writeAll(descriptor, Buffer.from(text, "utf8"));
    fs.fsyncSync(descriptor);
  } finally {
    fs.closeSync(descriptor);
  }
  fs.renameSync(temporary, file);
  syncDirectory(path.dirname(file));
};

// Truncate through its own read-write handle: Windows refuses to truncate through an append one.
const truncateTo = (file: string, size: number): void => {
  const descriptor = fs.openSync(file, "r+");
  try {
    if (fs.fstatSync(descriptor).size <= size) return;
    fs.ftruncateSync(descriptor, size);
    fs.fdatasyncSync(descriptor);
  } finally {
    fs.closeSync(descriptor);
  }
};

export class MeshArchive {

  constructor(readonly dir: string, readonly meshRoot: string) {}

  /** The archive a mesh root names, or undefined when it names none. A bad entry throws (fail closed). */
  static fromRoot(root: string): MeshArchive | undefined {
    let text: string;
    try {
      text = fs.readFileSync(path.join(root, MESH_ARCHIVE_CONFIG), "utf8");
    } catch (error) {
      if (errorCode(error) === "ENOENT") return undefined;
      throw error;
    }
    const parsed = JSON.parse(text) as { version?: unknown; dir?: unknown };
    if (parsed.version !== 1 || typeof parsed.dir !== "string" || !path.isAbsolute(parsed.dir)) {
      throw new Error(`Invalid mesh event archive configuration in ${path.join(root, MESH_ARCHIVE_CONFIG)}`);
    }
    return new MeshArchive(parsed.dir, root);
  }

  head(): ArchiveHead | undefined {
    return this.#readJson<ArchiveHead>("HEAD.json");
  }

  /** The first sequence the archive holds; it holds every committed event from there on. */
  firstSequence(): number | undefined {
    const first = this.#readJson<{ firstSequence?: unknown }>("MESH.json")?.firstSequence;
    return typeof first === "number" && Number.isSafeInteger(first) ? first : undefined;
  }

  pending(): MeshArchivePending | undefined {
    return this.#readJson<MeshArchivePending>("PENDING.json");
  }

  /**
   * Archives a publish's event before it goes live: records where it goes, writes the whole
   * line and syncs it. On any failure it cuts the line back out and throws.
   */
  begin(entry: MeshArchiveEntry, syncIndexDirectory?: IndexDirectoryBarrier): MeshArchivePending {
    this.#requireRoot();
    const relative = this.#fileFor(entry.event, this.#headDay());
    const absolute = path.join(this.dir, relative);
    this.#describeMesh(entry.event.sequence);
    fs.mkdirSync(path.dirname(absolute), { recursive: true, mode: 0o700 });
    let descriptor: number | undefined = fs.openSync(absolute, "a+", 0o600);
    let pending: MeshArchivePending | undefined;
    try {
      const { digest } = this.#repairAndLoadDigest(descriptor, relative);
      const before = fs.fstatSync(descriptor);
      pending = { sequence: entry.event.sequence, id: entry.event.id, file: relative, size: before.size,
        digestBefore: structuredClone(digest), indexed: true,
        ...(entry.event.dedupeKey ? { dedupe: true as const } : {}) };
      this.#advanceDigest(descriptor, relative, digest, { bytes: ARCHIVE_DIGEST_SLICE_BYTES });
      const fresh = pending.size === 0;
      this.#appendDigest(digest, pending.size, entry);
      pending.digestAfter = digest;
      writeFileAtomic(path.join(this.dir, "PENDING.json"), `${JSON.stringify(pending)}\n`);
      const bytes = Buffer.from(`${entry.line}\n`, "utf8");
      writeAll(descriptor, bytes);
      // The ONE durable barrier of an ordinary publish (smarty-dev#8305): the event's archive
      // bytes are on disk before it can go live. HEAD, PENDING, the digest checkpoint and an
      // unkeyed index are advisory metadata: losing them cannot erase the segment's event.
      fs.fdatasyncSync(descriptor);
      // A keyed index replaces a durable `absent` reservation: it is an intent-recovery fence
      // and stays durable, after the bytes it names. Nothing resolves an unkeyed index.
      this.#writeIndex(entry.event.sequence, { sequence: entry.event.sequence, id: entry.event.id, file: relative, offset: pending.size, length: bytes.length, ...(entry.event.dedupeKey ? { committed: false } : {}) }, entry.event.dedupeKey !== undefined, syncIndexDirectory);
      // Rare: a new segment's (and day's) name must be durable before its event goes live.
      if (fresh) this.#syncDays(relative);
    } catch (error) {
      if (descriptor !== undefined) fs.closeSync(descriptor);
      descriptor = undefined;
      if (pending) this.#cutBack(pending);
      throw error;
    } finally {
      if (descriptor !== undefined) fs.closeSync(descriptor);
    }
    return pending;
  }

  /** The event is live: it is committed. Moves the head and seals any closed day. */
  commit(pending: MeshArchivePending, sealClosedDays = true, syncIndexDirectory?: IndexDirectoryBarrier): void {
    // Advisory checkpoint of bytes begin() already synced: plain. A lost or stale one is a
    // valid prefix (rebuilt in slices); every truncation path saves its checkpoint durably.
    if (pending.digestAfter) this.#saveDigest(pending.file, pending.digestAfter, fs.statSync(path.join(this.dir, pending.file)), false);
    if (pending.dedupe) this.confirmLive(pending.sequence, pending.id, syncIndexDirectory);
    // PENDING goes first: a stop before the head moves leaves a head that is behind, and the
    // catch-up skips the event it finds already archived.
    // ponytail (smarty-dev#8907, every PENDING.json/DIGEST-REPAIR.json removal below): these are fixed-name control
    // markers in the archive root, never aged or owner-judged. Every writer and remover of those names, new and legacy
    // (module header), runs inside the mesh root's .lock publish hold, and each judgment (here the `pending` begin()
    // wrote in this hold, elsewhere a re-read under it) and its removal run synchronously in that one hold with no
    // await between, so no writer of the name can interleave. A path is kept: verifiedRemove's graveyard would add
    // namespace ops to every archived publish, and a refused marker would need new commit semantics.
    fs.rmSync(path.join(this.dir, "PENDING.json"), { force: true });
    this.#writeHead({ sequence: pending.sequence, id: pending.id, file: pending.file });
    if (sealClosedDays) this.#sealClosedDays(pending.file);
  }

  /** A direct live anchor (or a synced reboot promotion) proves publication, not just append. */
  confirmLive(sequence: number, id: string, syncIndexDirectory?: IndexDirectoryBarrier): void {
    const indexed = this.#readJson<MeshArchiveIndexEntry | { absent: true }>(path.relative(this.dir, this.#indexPath(sequence)));
    // Old catch-ups had no sidecar; only new reserved entries need this transition.
    if (!indexed || "absent" in indexed) return;
    if (indexed.id !== id) throw new MeshArchiveLookupUnavailableError("Cannot commit a mismatched archive sequence index");
    if (indexed.committed === false) this.#writeIndex(sequence, { ...indexed, committed: true }, true, syncIndexDirectory);
  }

  /**
   * A promoted keyed line is positive evidence: synced archive bytes, now live again. A power
   * loss between begin()'s fdatasync and its durable index leaves the `absent` reservation
   * (or a torn plain write): rebuild the exact address so settleIntent neither ignores nor
   * republishes it. A different identity at the sequence still fails closed (confirmLive).
   */
  #rebuildPromotedIndex(entry: MeshArchiveEntry & { file?: string; offset?: number }): void {
    const { event, line, file, offset } = entry;
    let indexed: MeshArchiveIndexEntry | { absent: true } | undefined;
    try { indexed = this.#readJson(path.relative(this.dir, this.#indexPath(event.sequence))); }
    catch (error) { if (!(error instanceof SyntaxError)) throw error; indexed = undefined; }
    const unusable = indexed === undefined || indexed === null || typeof indexed !== "object" ||
      ("absent" in indexed && indexed.absent === true) || typeof (indexed as MeshArchiveIndexEntry).id !== "string";
    if (unusable && file !== undefined && offset !== undefined) {
      this.#writeIndex(event.sequence, { sequence: event.sequence, id: event.id, file, offset,
        length: Buffer.byteLength(line, "utf8") + 1, committed: true });
      return;
    }
    this.confirmLive(event.sequence, event.id);
  }

  /** The event never went live: cut it back out of its file. */
  rollback(pending: MeshArchivePending): void {
    this.#cutBack(pending);
  }

  /** A legacy line larger than a slice is parsed only on this off-lock preflight.
   * It is read-only: no archive-level lock is needed, and install validates old writers too.
   */
  prepareDigestRepair(): MeshArchiveDigestRepair | undefined {
    const repair = this.#readJson<{ file: string }>("DIGEST-REPAIR.json");
    if (!repair) return undefined;
    const target = path.join(this.dir, repair.file);
    if (!fs.existsSync(target)) return { file: repair.file, identity: "missing", digest: {
      version: 1, identity: "missing", hash: new ArchiveSha256().state(), lines: 0, firstSequence: 0, lastSequence: 0 } };
    const descriptor = fs.openSync(target, "r");
    try {
      const stat = fs.fstatSync(descriptor);
      const digest = this.#loadDigest(repair.file, stat);
      const hash = new ArchiveSha256(digest.hash);
      let position = digest.hash.bytes;
      const parts: Buffer[] = [];
      while (position < stat.size) {
        const bytes = Buffer.allocUnsafe(Math.min(ARCHIVE_DIGEST_SLICE_BYTES, stat.size - position));
        const count = fs.readSync(descriptor, bytes, 0, bytes.length, position);
        if (count !== bytes.length) throw new MeshArchiveRecoveryChanged();
        const newline = bytes.indexOf(0x0a);
        const part = bytes.subarray(0, newline < 0 ? bytes.length : newline + 1);
        hash.update(part); parts.push(part); position += part.length;
        if (newline >= 0) break;
      }
      digest.hash = hash.state();
      for (const line of completeLines(Buffer.concat(parts).toString("utf8"))) {
        const event = parseEvent(line);
        if (event) this.#countLine(digest, event);
      }
      return { file: repair.file, identity: fileIdentity(stat), digest };
    } finally { fs.closeSync(descriptor); }
  }

  /** Called only under the mesh lock; a raced read is discarded without mutating its file. */
  installDigestRepair(repair: MeshArchiveDigestRepair | undefined): void {
    if (!repair || this.#readJson<{ file: string }>("DIGEST-REPAIR.json")?.file !== repair.file) return;
    const stat = fs.statSync(path.join(this.dir, repair.file), { throwIfNoEntry: false });
    if (!stat) {
      // ponytail: re-read DIGEST-REPAIR.json and removed in one synchronous .lock hold (see commit()).
      fs.rmSync(path.join(this.dir, "DIGEST-REPAIR.json"), { force: true });
      return;
    }
    if (fileIdentity(stat) !== repair.identity) return;
    this.#saveDigest(repair.file, repair.digest, stat);
    // ponytail: re-read DIGEST-REPAIR.json and removed in one synchronous .lock hold (see commit()).
    fs.rmSync(path.join(this.dir, "DIGEST-REPAIR.json"), { force: true });
  }

  #recoveryMetadata(): string {
    return JSON.stringify([this.#readText("BOOT"), this.#readText("HEAD.json"), this.#readText("PENDING.json"), this.#days()]);
  }

  /** Read-only preflight, called BEFORE taking the mesh lock. All reads are <=64 KiB.
   * Old writers need no new lock protocol: the locked validation fences their changes too.
   */
  prepareRecovery(lastLive: number): MeshArchiveRecoveryPlan | undefined {
    if (this.#readText("BOOT") === currentBoot()) return undefined;
    this.#requireRoot();
    const metadata = this.#recoveryMetadata();
    const identities: MeshArchiveRecoveryPlan["identities"] = [];
    const pending = this.pending();
    let pendingRepair: MeshArchiveRecoveryPlan["pendingRepair"];
    if (pending) {
      const absolute = path.join(this.dir, pending.file);
      if (fs.existsSync(absolute)) {
        const descriptor = fs.openSync(absolute, "r");
        try {
          const stat = fs.fstatSync(descriptor);
          identities.push({ file: pending.file, identity: fileIdentity(stat) });
          let position = stat.size;
          let size = 0;
          while (position > 0) {
            const start = Math.max(0, position - ARCHIVE_DIGEST_SLICE_BYTES);
            const bytes = Buffer.allocUnsafe(position - start);
            if (fs.readSync(descriptor, bytes, 0, bytes.length, start) !== bytes.length) throw new MeshArchiveRecoveryChanged();
            const newline = bytes.lastIndexOf(0x0a);
            if (newline >= 0) { size = start + newline + 1; break; }
            position = start;
          }
          const digest = this.#loadDigest(pending.file, stat);
          pendingRepair = { file: pending.file, size, digest: digest.hash.bytes <= size ? digest : {
            version: 1, identity: fileIdentity(stat), hash: new ArchiveSha256().state(), lines: 0, firstSequence: 0, lastSequence: 0 } };
        } finally { fs.closeSync(descriptor); }
      }
    }
    const found = new Map<string, MeshArchiveEntry & { file: string; offset: number }>();
    for (const day of this.#days()) {
      const directory = path.join(this.dir, day);
      identities.push({ file: day, identity: fileIdentity(fs.statSync(directory)) });
      const abortedFile = `${day}/ABORTED.json`;
      const abortedStat = fs.statSync(path.join(this.dir, abortedFile), { throwIfNoEntry: false });
      identities.push({ file: abortedFile, identity: abortedStat ? fileIdentity(abortedStat) : "missing" });
      const aborted = this.#readJson<Record<string, string>>(abortedFile);
      const seal = this.#readJson<{ files: Record<string, SealFile> }>(`${day}/SEAL.json`);
      if (seal && Math.max(0, ...Object.values(seal.files).map(file => file.lastSequence)) <= lastLive) continue;
      for (const name of fs.readdirSync(directory).filter(entry => entry.endsWith(".jsonl"))) {
        const file = `${day}/${name}`;
        const descriptor = fs.openSync(path.join(this.dir, file), "r");
        try {
          const stat = fs.fstatSync(descriptor);
          identities.push({ file, identity: fileIdentity(stat) });
          let position = stat.size;
          let carry = Buffer.alloc(0);
          let done = false;
          while (position > 0 && !done) {
            const start = Math.max(0, position - ARCHIVE_DIGEST_SLICE_BYTES);
            const chunk = Buffer.allocUnsafe(position - start);
            const count = fs.readSync(descriptor, chunk, 0, chunk.length, start);
            if (count !== chunk.length) throw new MeshArchiveRecoveryChanged();
            position = start;
            const bytes = Buffer.concat([chunk, carry]);
            const split = position === 0 ? 0 : bytes.indexOf(0x0a) + 1;
            if (!split && position > 0) { carry = bytes; continue; }
            carry = bytes.subarray(0, split);
            // Complete lines with their exact segment address (completeLines' split, by byte).
            const lines: Array<{ line: string; offset: number }> = [];
            for (let start = split; ;) {
              const newline = bytes.indexOf(0x0a, start);
              if (newline < 0) break;
              if (newline > start) lines.push({ line: bytes.subarray(start, newline).toString("utf8"), offset: position + start });
              start = newline + 1;
            }
            for (let i = lines.length - 1; i >= 0; i--) {
              const { line, offset } = lines[i]!;
              const event = parseEvent(line);
              if (!event) continue;
              if (event.sequence <= lastLive) { done = true; break; }
              if (aborted?.[event.sequence] !== event.id) found.set(event.id, { event, line, file, offset });
            }
          }
        } finally { fs.closeSync(descriptor); }
      }
    }
    return { dir: this.dir, lastLive, metadata, identities, ...(pendingRepair ? { pendingRepair } : {}),
      promote: [...found.values()].sort((a, b) => a.event.sequence - b.event.sequence) };
  }

  #validateRecovery(plan: MeshArchiveRecoveryPlan | undefined, lastLive: number): asserts plan is MeshArchiveRecoveryPlan {
    if (!plan || plan.dir !== this.dir || plan.lastLive > lastLive || plan.metadata !== this.#recoveryMetadata()) {
      throw new MeshArchiveRecoveryChanged();
    }
    for (const { file, identity } of plan.identities) {
      const stat = fs.statSync(path.join(this.dir, file), { throwIfNoEntry: false });
      if (identity !== (stat ? fileIdentity(stat) : "missing")) throw new MeshArchiveRecoveryChanged();
    }
  }

  /**
   * Settles what a stop left, under the mesh lock, before a publish.
   *
   * In the boot that BOOT names, a process crash kept every completed write. A pending record
   * at or below the head was committed (only its removal was lost), so it stays; any other is
   * cut back out, and if its event did go live, the store's catch-up archives it again.
   *
   * In a new boot, only synced data is sure: the live append, the head and PENDING's removal
   * of an acknowledged publish may all be gone, while its archive line was synced before the
   * publish returned. So nothing complete is discarded: a torn line is cut, and every archived
   * event past the live log's end, on any day, is returned to go live again. An event whose
   * publish never returned may come back too; across a power loss delivery is at least once.
   * BOOT-mismatch reads and torn-tail boundaries must be prepared off-lock. Validation under
   * this lock fences old and new writers before any mutation; a raced snapshot is retried.
   * The store then calls recovered(), which records this boot.
   *
   * rebootOnly defers same-boot pending cleanup, so an exact keyed retry can settle
   * its intent AFTER reboot promotion without ordinary cutback erasing its evidence.
   */
  recover(lastLive: number, plan?: MeshArchiveRecoveryPlan, rebootOnly = false): MeshArchiveRecovery {
    const pending = this.pending();
    if (this.#readText("BOOT") === currentBoot()) {
      if (pending && !rebootOnly) {
        // ponytail: PENDING.json read above and removed in one synchronous .lock hold (see commit()).
        if ((this.head()?.sequence ?? 0) >= pending.sequence) fs.rmSync(path.join(this.dir, "PENDING.json"), { force: true });
        else this.#cutBack(pending);
      }
      return { rebooted: false, promote: [] };
    }
    this.#requireRoot();
    this.#validateRecovery(plan, lastLive);
    if (pending) {
      if (plan.pendingRepair) {
        const { file, size, digest } = plan.pendingRepair;
        const absolute = path.join(this.dir, file);
        truncateTo(absolute, size);
        this.#saveDigest(file, digest, fs.statSync(absolute));
      }
      // ponytail: PENDING.json read above and removed in one synchronous .lock hold (see commit()).
      fs.rmSync(path.join(this.dir, "PENDING.json"), { force: true });
    }
    return { rebooted: true, promote: plan.promote.filter(entry => entry.event.sequence > lastLive) };
  }

  /** After a new boot's recovery: the promoted events are live. Records the boot, durably. */
  recovered(last: (MeshArchiveEntry & { file: string }) | undefined, promoted: Array<MeshArchiveEntry & { file?: string; offset?: number }> = []): void {
    for (const entry of promoted) if (entry.event.dedupeKey) this.#rebuildPromotedIndex(entry);
    if (last) this.#writeHead({ sequence: last.event.sequence, id: last.event.id, file: last.file });
    writeDurable(path.join(this.dir, "BOOT"), currentBoot());
  }

  /**
   * Archives events that are already live (appended by a store without the archive). An event
   * already at the end of its file is skipped, so a repeated catch-up adds nothing.
   */
  catchUp(entries: MeshArchiveEntry[]): void {
    if (entries.length === 0) return;
    this.#requireRoot();
    this.#describeMesh(entries[0]!.event.sequence);
    let floor = this.#headDay();
    const open = new Map<string, {
      descriptor: number;
      last: { sequence: number; id: string } | undefined;
      relative: string;
      digest: DigestCheckpoint;
    }>();
    let last: ArchiveHead | undefined;
    try {
      for (const { event, line } of entries) {
        const relative = this.#fileFor(event, floor);
        floor = relative.split("/").slice(0, 3).join("/");
        let file = open.get(relative);
        if (!file) {
          const absolute = path.join(this.dir, relative);
          fs.mkdirSync(path.dirname(absolute), { recursive: true, mode: 0o700 });
          const descriptor = fs.openSync(absolute, "a+", 0o600);
          const { last, digest } = this.#repairAndLoadDigest(descriptor, relative);
          this.#advanceDigest(descriptor, relative, digest, { bytes: ARCHIVE_DIGEST_SLICE_BYTES });
          file = { descriptor, last, relative, digest };
          open.set(relative, file);
        }
        const previous = file.last;
        last = { sequence: event.sequence, id: event.id, file: relative };
        const bytes = Buffer.from(`${line}\n`, "utf8");
        if (previous && (previous.sequence > event.sequence || (previous.sequence === event.sequence && previous.id === event.id))) {
          // Recovery may have rolled an indexed pending line back out before catch-up.
          // The exact last line can repair its sidecar without scanning this file.
          if (event.dedupeKey && previous.sequence === event.sequence && previous.id === event.id) {
            fs.fdatasyncSync(file.descriptor);
            this.#writeIndex(event.sequence, { sequence: event.sequence, id: event.id, file: relative, offset: fs.fstatSync(file.descriptor).size - bytes.length, length: bytes.length, committed: true });
          }
          continue;
        }
        const offset = fs.fstatSync(file.descriptor).size;
        writeAll(file.descriptor, bytes);
        this.#appendDigest(file.digest, offset, { event, line });
        if (event.dedupeKey) {
          fs.fdatasyncSync(file.descriptor);
          this.#writeIndex(event.sequence, { sequence: event.sequence, id: event.id, file: relative, offset, length: bytes.length, committed: true });
        }
        file.last = { sequence: event.sequence, id: event.id };
      }
      for (const { descriptor, relative, digest } of open.values()) {
        fs.fdatasyncSync(descriptor);
        this.#saveDigest(relative, digest, fs.fstatSync(descriptor));
      }
      // Before the head moves, sync the directories of every day this catch-up touched, even
      // for files that already held lines: a stopped catch-up leaves complete lines whose
      // names may never have been synced, and only a retry that finishes the syncs moves on.
      const days = new Map([...open.values()].map(({ relative }) => [relative.split("/").slice(0, 3).join("/"), relative]));
      for (const relative of days.values()) this.#syncDays(relative);
    } finally {
      for (const { descriptor } of open.values()) fs.closeSync(descriptor);
    }
    if (last) {
      this.#writeHead(last);
      this.#sealClosedDays(last.file);
    }
  }

  /**
   * Directly resolves one exact reserved sequence through the sidecar index. This deliberately
   * reads only the indexed archive line; it never scans archive topic files. A missing index is
   * an unavailable proof, not evidence that the event was not committed.
   */
  lookup(sequence: number): MeshEvent | undefined {
    return this.lookupEntry(sequence)?.event;
  }

  /** Exact archived bytes and advisory new-writer confirmation, never absence evidence. */
  lookupEntry(sequence: number, maxBytes = 64 * 1024 * 1024): (MeshArchiveEntry & { committed: boolean }) | undefined {
    if (!Number.isSafeInteger(sequence) || sequence < 1) throw new MeshArchiveLookupUnavailableError("Invalid mesh archive sequence lookup");
    const indexPath = this.#indexPath(sequence);
    let indexed: MeshArchiveIndexEntry | { sequence: number; absent: true };
    try {
      this.#requireRoot();
      if (fs.statSync(indexPath).size > 4096) throw new Error("Oversized archive sequence index");
      indexed = JSON.parse(fs.readFileSync(indexPath, "utf8"));
      if (typeof indexed !== "object" || indexed === null || Array.isArray(indexed)) throw new Error("Invalid archive sequence index");
    } catch {
      throw new MeshArchiveLookupUnavailableError(`Mesh archive sequence index is unavailable: ${indexPath}`);
    }
    if (indexed.sequence !== sequence) throw new MeshArchiveLookupUnavailableError(`Invalid mesh archive sequence index: ${indexPath}`);
    // Missing/corrupt sidecars are UNKNOWN. Only this durable negative reservation proves lack.
    if ("absent" in indexed && indexed.absent === true) return undefined;
    if (!("file" in indexed) || typeof indexed.id !== "string" || typeof indexed.file !== "string" ||
        !/^\d{4}\/\d{2}\/\d{2}\/[^/\\]+\.jsonl$/.test(indexed.file) ||
        !Number.isSafeInteger(indexed.offset) || indexed.offset < 0 ||
        !Number.isSafeInteger(indexed.length) || indexed.length < 1 || indexed.length > 64 * 1024 * 1024 ||
        (indexed.committed !== undefined && typeof indexed.committed !== "boolean")) {
      throw new MeshArchiveLookupUnavailableError(`Invalid mesh archive sequence index: ${indexPath}`);
    }
    // Callers doing pre-readiness recovery have a smaller aggregate byte budget.
    if (indexed.length > maxBytes) throw new MeshArchiveLookupUnavailableError("Mesh archive lookup exceeds caller byte budget");
    if (this.#readJson<Record<string, string>>(`${indexed.file.split("/").slice(0, 3).join("/")}/ABORTED.json`)?.[sequence] === indexed.id) return undefined;
    let descriptor: number | undefined;
    try {
      const file = path.join(this.dir, indexed.file);
      descriptor = fs.openSync(file, "r");
      const available = fs.fstatSync(descriptor).size - indexed.offset;
      // An old writer can cut PENDING back without updating its new sidecar. The file
      // ending at/before that address proves absence; a torn line after it does not.
      if (available <= 0) return undefined;
      let bytes = Buffer.allocUnsafe(Math.min(indexed.length, available));
      let count = fs.readSync(descriptor, bytes, 0, bytes.length, indexed.offset);
      let readBytes = count;
      let newline = bytes.subarray(0, count).indexOf(0x0a);
      // The old writer may reuse that address for a differently sized event. Resolve
      // only this one bounded line, never search the segment for the missing identity.
      while (newline < 0 && count === bytes.length && bytes.length < available && bytes.length < Math.min(64 * 1024 * 1024, maxBytes - readBytes)) {
        bytes = Buffer.allocUnsafe(Math.min(Math.max(bytes.length * 2, 4096), available, 64 * 1024 * 1024, maxBytes - readBytes));
        count = fs.readSync(descriptor, bytes, 0, bytes.length, indexed.offset);
        readBytes += count;
        newline = bytes.subarray(0, count).indexOf(0x0a);
      }
      if (newline < 0) throw new Error("short archive line");
      const line = bytes.subarray(0, newline).toString("utf8");
      const event = parseEvent(line);
      if (!event) throw new Error("invalid archive line");
      if (event.sequence !== sequence || event.id !== indexed.id) return undefined;
      if (newline + 1 !== indexed.length) throw new Error("archive index length mismatch");
      const pending = this.pending();
      return { event, line, committed: indexed.committed ?? !(pending?.sequence === sequence && pending.id === event.id) };
    } catch (error) {
      if (error instanceof MeshArchiveLookupUnavailableError) throw error;
      throw new MeshArchiveLookupUnavailableError(`Mesh archive sequence ${sequence} is unavailable`);
    } finally {
      if (descriptor !== undefined) fs.closeSync(descriptor);
    }
  }

  /**
   * Record a positively established abort, never inferred from a false commit marker.
   * Keep its bytes/addresses intact, but durably hide it before declaring the reservation
   * absent. The day marker also covers a death before the negative sidecar is installed.
   */
  abort(entry: MeshArchiveEntry): void {
    const indexed = this.#readJson<MeshArchiveIndexEntry>(path.relative(this.dir, this.#indexPath(entry.event.sequence)));
    if (!indexed || indexed.id !== entry.event.id || typeof indexed.file !== "string") {
      throw new MeshArchiveLookupUnavailableError("Cannot abort an unavailable archive sequence index");
    }
    const day = indexed.file.split("/").slice(0, 3).join("/");
    const aborted = this.#readJson<Record<string, string>>(`${day}/ABORTED.json`) ?? {};
    writeFileAtomic(path.join(this.dir, day, "ABORTED.json"), JSON.stringify({ ...aborted, [entry.event.sequence]: entry.event.id }), { durable: true });
    this.reserveLookup(entry.event.sequence);
    const pending = this.pending();
    if (pending?.sequence === entry.event.sequence && pending.id === entry.event.id) {
      // ponytail: PENDING.json read above and removed in one synchronous .lock hold (see commit()).
      fs.rmSync(path.join(this.dir, "PENDING.json"), { force: true });
    }
  }

  /**
   * Committed events after a sequence, in sequence order, for reads older than the live log.
   * `through` is the newest live sequence: nothing past it is published. Within that
   * horizon, only a positive abort hides an event; old writers may leave stale metadata.
   */
  readAfter(after: number, through: number, matches: (event: MeshEvent) => boolean, limit: number, topic?: string): MeshEvent[] {
    const scan = this.#readAfterSteps(after, through, matches, limit, topic);
    const empty = Buffer.alloc(0);
    try {
      let step = scan.next();
      while (!step.done) {
        const request = step.value;
        if (!request) { step = scan.next(empty); continue; }
        const bytes = Buffer.allocUnsafe(request.length);
        const count = fs.readSync(request.descriptor, bytes, 0, bytes.length, request.position);
        step = scan.next(bytes.subarray(0, count));
      }
      return step.value;
    } finally { scan.return([]); }
  }

  /** Same snapshot/order/publication rules as readAfter, with asynchronous bounded reads.
   * Metadata is bounded and synchronous; every file and every 64 KiB data read yields, even
   * on a cold index build. One oversized JSON event still needs one indivisible JSON.parse.
   */
  async readAfterAsync(after: number, through: number, matches: (event: MeshEvent) => boolean, limit: number, topic?: string): Promise<MeshEvent[]> {
    const scan = this.#readAfterSteps(after, through, matches, limit, topic);
    const empty = Buffer.alloc(0);
    try {
      let step = scan.next();
      while (!step.done) {
        const request = step.value;
        let bytes: Buffer;
        if (request) {
          const buffer = Buffer.allocUnsafe(request.length);
          bytes = await new Promise<Buffer>((resolve, reject) => {
            fs.read(request.descriptor, buffer, 0, buffer.length, request.position, (error, count) => {
              if (error) reject(error);
              else resolve(buffer.subarray(0, count));
            });
          });
        } else {
          await new Promise<void>(resolve => setImmediate(resolve));
          bytes = empty;
        }
        step = scan.next(bytes);
      }
      return step.value;
    } finally { scan.return([]); }
  }

  *#readAfterSteps(after: number, through: number, matches: (event: MeshEvent) => boolean, limit: number, topic?: string): ArchiveReadSteps<MeshEvent[]> {
    // An async read may overlap an old writer's cutback/replacement. Discard raced results,
    // never advance a caller's cursor with a partial page. Bound retries under a busy writer.
    for (let attempt = 0; ; attempt++) {
      try { return yield* this.#readAfterSnapshot(after, through, matches, limit, topic); }
      catch (error) {
        if (!(error instanceof MeshArchiveRecoveryChanged)) throw error;
        if (attempt + 1 >= ARCHIVE_READ_ATTEMPTS) throw new MeshSnapshotRetriesExhaustedError(attempt + 1);
        yield undefined;
      }
    }
  }

  *#readAfterSnapshot(after: number, through: number, matches: (event: MeshEvent) => boolean, limit: number, topic?: string): ArchiveReadSteps<MeshEvent[]> {
    const found: MeshEvent[] = [];
    for (const day of this.#days()) {
      const directory = path.join(this.dir, day);
      const abortedPath = path.join(directory, "ABORTED.json");
      const abortedStat = fs.statSync(abortedPath, { throwIfNoEntry: false });
      const abortedIdentity = abortedStat ? fileIdentity(abortedStat) : "missing";
      // Missing is common; avoid constructing/catching ENOENT twice per day. A final exact
      // stat still fences creation, replacement, removal and same-size abort rewrites.
      const aborted = abortedStat ? this.#readJson<Record<string, string>>(`${day}/ABORTED.json`) : undefined;
      const nameSnapshot = topic === undefined ? this.#readDayNames(directory) : undefined;
      const names = nameSnapshot?.names ?? [archiveFileName(topic!)];
      const dayIdentity = nameSnapshot?.identity ?? fileIdentity(fs.statSync(directory));
      const sealIdentity = this.#rangeFileIdentity(`${day}/SEAL.json`);
      // A missing seal is a generation too. Unsealed days still require exact segment
      // identities on every read: a same-directory append need not change dayIdentity.
      const generation = `${dayIdentity}|${sealIdentity}`;
      const catalog = archiveRangeCache.get(directory, generation);
      const identities: Array<{ absolute: string; identity: string }> = [];
      const work: Array<{ absolute: string; identity: string; order: number; block: ArchiveRangeBlock }> = [];
      let candidates: ArchiveCandidate[] = [];
      const capacity = limit > 0 ? Math.ceil(limit - found.length) : Infinity;
      const trim = (items: ArchiveCandidate[]): ArchiveCandidate[] => {
        items.sort(compareArchiveCandidates);
        return items.length > capacity ? items.slice(0, capacity) : items;
      };
      const collect = (items: ArchiveCandidate[], fileOrder: number) => (event: MeshEvent, offset: number): void => {
        if (event.sequence > after && event.sequence <= through && this.#isPublished(event, aborted) && matches(event)) {
          items.push({ event, fileOrder, offset });
          // Keep bounded memory even when cold-building an index for a huge file.
          if (items.length >= capacity * 2) items.splice(0, items.length, ...trim(items));
        }
      };
      for (const [order, name] of names.entries()) {
        yield undefined;
        const file = `${day}/${name}`;
        // readdir names are basenames; keep path normalization for caller-supplied topics.
        const absolute = nameSnapshot ? `${directory}${path.sep}${name}` : path.join(directory, name);
        const stat = fs.statSync(absolute, { throwIfNoEntry: false });
        const identity = stat ? fileIdentity(stat) : "missing";
        identities.push({ absolute, identity });
        if (!stat) continue;
        const saved = catalog.get(name);
        let index = saved?.identity === identity ? saved : undefined;
        if (!index) {
          // DigestCheckpoint counters have no checksum covering their extrema. A valid
          // hash state/byte count does not authenticate them: derive read hints from data.
          const descriptor = fs.openSync(absolute, "r");
          const items: ArchiveCandidate[] = [];
          try {
            if (!rangeReadCompatible(identity, fileIdentity(fs.fstatSync(descriptor)))) throw new MeshArchiveRecoveryChanged();
            const blocks = yield* this.#scanRangeBlocks(descriptor, 0, stat.size, collect(items, order));
            const handleIdentity = fileIdentity(fs.fstatSync(descriptor)), current = this.#rangeFileIdentity(file);
            if (!rangeReadCompatible(identity, handleIdentity) || !rangeReadCompatible(identity, current)) throw new MeshArchiveRecoveryChanged();
            const built = this.#makeRangeIndex(identity, blocks);
            index = built;
          } finally { fs.closeSync(descriptor); }
          catalog.set(name, index);
          candidates = trim(candidates.concat(items));
          continue; // Already parsed exactly once while building its index.
        }
        if (rangeOutside(index.min, index.max, after, through)) continue;
        for (const block of index.blocks) {
          if (!rangeOutside(block[2], block[3], after, through)) work.push({ absolute, identity, order, block });
        }
      }
      // A topic's first `limit` events are NOT enough: files can be out of order. Visit
      // blocks by their proved minimum, then prune only strictly beyond the global kth
      // candidate. Equal sequences retain the old file-enumeration/line order.
      work.sort((a, b) => (a.block[2] ?? -Infinity) - (b.block[2] ?? -Infinity) || a.order - b.order || a.block[0] - b.block[0]);
      for (const { absolute, identity, order, block } of work) {
        if (candidates.length >= capacity && block[2] !== null && block[2] > candidates[candidates.length - 1]!.event.sequence) continue;
        yield undefined;
        const descriptor = fs.openSync(absolute, "r");
        const items: ArchiveCandidate[] = [];
        try {
          if (!rangeReadCompatible(identity, fileIdentity(fs.fstatSync(descriptor)))) throw new MeshArchiveRecoveryChanged();
          yield* this.#scanRangeBlocks(descriptor, block[0], block[1], collect(items, order));
          if (!rangeReadCompatible(identity, fileIdentity(fs.fstatSync(descriptor)))) throw new MeshArchiveRecoveryChanged();
          // The final per-file path sweep below also fences replacement/unlink. Do not
          // repeat that pathname stat for every block of the same file.
        } finally { fs.closeSync(descriptor); }
        candidates = trim(candidates.concat(items));
      }
      for (const { absolute, identity } of identities) {
        yield undefined;
        const stat = fs.statSync(absolute, { throwIfNoEntry: false });
        if (!rangeReadCompatible(identity, stat ? fileIdentity(stat) : "missing")) throw new MeshArchiveRecoveryChanged();
      }
      const currentAborted = fs.statSync(abortedPath, { throwIfNoEntry: false });
      if (abortedIdentity !== (currentAborted ? fileIdentity(currentAborted) : "missing")) throw new MeshArchiveRecoveryChanged();
      if (dayIdentity !== fileIdentity(fs.statSync(directory)) ||
          sealIdentity !== this.#rangeFileIdentity(`${day}/SEAL.json`)) throw new MeshArchiveRecoveryChanged();
      // Install only a fenced snapshot in memory. No range read creates any file.
      if (topic === undefined && catalog.size > names.length) {
        const present = new Set(names);
        for (const name of catalog.keys()) if (!present.has(name)) catalog.delete(name);
      }
      archiveRangeCache.set(directory, generation, catalog);
      for (const candidate of trim(candidates)) found.push(candidate.event);
      if (found.length >= limit) break;
    }
    return found.slice(0, limit);
  }

  #readDayNames(directory: string): { identity: string; names: string[] } {
    const identity = fileIdentity(fs.statSync(directory));
    const cached = dayNamesCache.get(directory);
    if (cached?.identity === identity) {
      dayNamesCache.delete(directory); dayNamesCache.set(directory, cached);
      return cached;
    }
    forgetDayNames(directory);
    // A real enumeration belongs only to the generation observed on BOTH sides of it.
    const names = fs.readdirSync(directory).filter(name => name.endsWith(".jsonl"));
    if (fileIdentity(fs.statSync(directory)) !== identity) throw new MeshArchiveRecoveryChanged();
    const bytes = Buffer.byteLength(JSON.stringify(names));
    if (bytes <= ARCHIVE_DAY_NAMES_CACHE_BYTES) {
      while (dayNamesCache.size >= ARCHIVE_DAY_NAMES_CACHE_ENTRIES || dayNamesCacheBytes + bytes > ARCHIVE_DAY_NAMES_CACHE_BYTES) {
        forgetDayNames(dayNamesCache.keys().next().value!);
      }
      dayNamesCache.set(directory, { identity, names, bytes });
      dayNamesCacheBytes += bytes;
    }
    return { identity, names };
  }

  #rangeFileIdentity(relative: string): string {
    const stat = fs.statSync(path.join(this.dir, relative), { throwIfNoEntry: false });
    return stat ? fileIdentity(stat) : "missing";
  }

  #makeRangeIndex(identity: string, blocks: ArchiveRangeBlock[]): ArchiveRangeIndex {
    let min: number | null = null, max: number | null = null;
    if (blocks.length && blocks.every(block => block[2] !== null && block[3] !== null)) {
      min = blocks.reduce((value, block) => Math.min(value, block[2]!), Infinity);
      max = blocks.reduce((value, block) => Math.max(value, block[3]!), -Infinity);
    }
    return { identity, min, max, blocks };
  }

  *#scanRangeBlocks(descriptor: number, start: number, end: number, visit: (event: MeshEvent, offset: number) => void): ArchiveReadSteps<ArchiveRangeBlock[]> {
    const blocks: ArchiveRangeBlock[] = [];
    // Fine-grained small segments, bounded metadata for large ones. A giant-line split
    // consumes at least two targets, paying for its preceding partial block as well.
    const blockBytes = Math.max(ARCHIVE_RANGE_BLOCK_BYTES, Math.ceil((end - start) / (ARCHIVE_RANGE_MAX_BLOCKS - 1)));
    let position = start, lineStart = start, blockStart = start;
    let min = Infinity, max = -Infinity;
    let unknown = false;
    let parts: Buffer[] = [];
    const finishBlock = (offset: number): void => {
      blocks.push([blockStart, offset, !unknown && Number.isFinite(min) ? min : null, !unknown && Number.isFinite(max) ? max : null]);
      blockStart = offset; min = Infinity; max = -Infinity; unknown = false;
    };
    while (position < end) {
      const bytes = yield { descriptor, position, length: Math.min(ARCHIVE_DIGEST_SLICE_BYTES, end - position) };
      if (!bytes.length) throw new MeshArchiveRecoveryChanged();
      let offset = 0;
      for (;;) {
        const newline = bytes.indexOf(0x0a, offset);
        if (newline < 0) break;
        const part = bytes.subarray(offset, newline);
        const line = (parts.length ? Buffer.concat([...parts, part]) : part).toString("utf8");
        parts = [];
        // Isolate a giant event from earlier small events: paging those must not reparse
        // the giant line each time just because it straddled a nominal block boundary.
        if (lineStart > blockStart && position + newline + 1 - lineStart >= Math.max(ARCHIVE_DIGEST_SLICE_BYTES, blockBytes * 2)) finishBlock(lineStart);
        const event = line ? parseEvent(line) : undefined;
        if (event) {
          if (!Number.isFinite(event.sequence)) unknown = true;
          else { min = Math.min(min, event.sequence); max = Math.max(max, event.sequence); }
          visit(event, lineStart);
        }
        offset = newline + 1;
        lineStart = position + offset;
        if (lineStart - blockStart >= blockBytes) finishBlock(lineStart);
      }
      if (offset < bytes.length) parts.push(bytes.subarray(offset));
      position += bytes.length;
    }
    // A non-newline-terminated tail is deliberately not parsed, just as completeLines.
    if (blockStart < end) finishBlock(end);
    return blocks;
  }

  // Old recovery can leave PENDING/committed:false on an already-live event. Neither
  // is negative evidence. Only this exact positive abort may hide archived bytes.
  #isPublished(event: MeshEvent, aborted: Record<string, string> | undefined): boolean {
    return aborted?.[event.sequence] !== event.id;
  }

  // The event's day, but never a day before `floor`, the head's day: every sealed day is
  // older than the head's, so a clock set back cannot write into one or out of sequence order.
  // The floor depends only on the head and the events, so a retried catch-up places each event
  // where the interrupted one did, and finds it there.
  #fileFor(event: MeshEvent, floor: string | undefined): string {
    const day = floor !== undefined && floor > dayOf(event.createdAt) ? floor : dayOf(event.createdAt);
    return `${day}/${archiveFileName(event.topic)}`;
  }

  #headDay(): string | undefined {
    return this.head()?.file.split("/").slice(0, 3).join("/");
  }

  #days(): string[] {
    const numeric = (directory: string): string[] => {
      try {
        return fs.readdirSync(directory).filter((name) => /^\d+$/.test(name)).sort();
      } catch (error) {
        if (errorCode(error) === "ENOENT") return [];
        throw error;
      }
    };
    return numeric(this.dir).flatMap((year) =>
      numeric(path.join(this.dir, year)).flatMap((month) =>
        numeric(path.join(this.dir, year, month)).map((day) => `${year}/${month}/${day}`)));
  }

  // Every day before the current one is closed. Sealed days form a prefix, because days are
  // sealed oldest first, so the scan back stops at the first sealed day.
  #sealClosedDays(currentFile: string): void {
    const current = currentFile.split("/").slice(0, 3).join("/");
    const unsealed: string[] = [];
    for (const day of this.#days().reverse()) {
      if (day >= current) continue;
      if (fs.existsSync(path.join(this.dir, day, "SEAL.json"))) break;
      unsealed.unshift(day);
    }
    const budget = { bytes: ARCHIVE_DIGEST_SLICE_BYTES };
    for (const day of unsealed) {
      if (!this.#seal(day, budget)) break; // Preserve the sealed-prefix invariant.
    }
  }

  #cutBack(pending: MeshArchivePending): void {
    try {
      const absolute = path.join(this.dir, pending.file);
      truncateTo(absolute, pending.size);
      if (pending.digestBefore) this.#saveDigest(pending.file, pending.digestBefore, fs.statSync(absolute));
      if (pending.indexed) this.reserveLookup(pending.sequence);
    } catch (error) {
      if (errorCode(error) !== "ENOENT") throw error;
    }
    // ponytail: the caller's PENDING.json, read in this synchronous .lock hold (see commit()).
    fs.rmSync(path.join(this.dir, "PENDING.json"), { force: true });
  }

  /** Install a durable proof of non-append before creating a publication intent. */
  reserveLookup(sequence: number, syncIndexDirectory?: IndexDirectoryBarrier): void {
    this.#requireRoot();
    this.#writeIndex(sequence, { sequence, absent: true }, true, syncIndexDirectory);
  }

  #indexPath(sequence: number): string {
    return path.join(this.dir, MESH_ARCHIVE_SEQUENCE_INDEX, String(Math.floor(sequence / 1024)), `${sequence}.json`);
  }

  #writeIndex(sequence: number, entry: MeshArchiveIndexEntry | { sequence: number; absent: true }, durable = true, syncIndexDirectory?: IndexDirectoryBarrier): void {
    writeFileAtomic(this.#indexPath(sequence), JSON.stringify(entry), { durable,
      ...(syncIndexDirectory ? { syncDirectory: (directory: string) => syncIndexDirectory(directory, this.dir, this.meshRoot, sequence) } : {}),
    });
  }

  #readJson<T>(relative: string): T | undefined {
    try {
      return JSON.parse(fs.readFileSync(path.join(this.dir, relative), "utf8")) as T;
    } catch (error) {
      if (errorCode(error) === "ENOENT") return undefined;
      throw error;
    }
  }

  // The archive root is made by whoever enables the archive, never here: a missing root (an
  // unmounted disk, a moved tree) fails every publish instead of starting a new archive.
  #requireRoot(): void {
    if (!fs.statSync(this.dir, { throwIfNoEntry: false })?.isDirectory()) {
      throw new Error(`Mesh event archive directory is missing: ${this.dir}`);
    }
  }

  // A new or empty file's day, month and year directories may be new too: sync each, and the
  // root, so the whole path survives a power loss. Run whenever the file was empty before its
  // first line, so a retry after an interrupted attempt finishes the job.
  #syncDays(relative: string): void {
    const [year, month, day] = relative.split("/");
    for (const directory of [`${year}/${month}/${day}`, `${year}/${month}`, `${year}`, ""]) syncDirectory(path.join(this.dir, directory));
  }

  #readText(relative: string): string | undefined {
    try {
      return fs.readFileSync(path.join(this.dir, relative), "utf8").trim();
    } catch (error) {
      if (errorCode(error) === "ENOENT") return undefined;
      throw error;
    }
  }

  #writeHead(head: ArchiveHead): void {
    writeFileAtomic(path.join(this.dir, "HEAD.json"), `${JSON.stringify(head)}\n`);
  }

  // Once-per-archive bootstrap metadata, durable (smarty-dev#8305): nextEventAfter trusts
  // firstSequence to route compacted-history reads here. Its data is fsynced before the rename
  // and the first (fresh) segment's #syncDays syncs this root before any event goes live, so a
  // segment line never outlives MESH.json. Every later publish only sees it exists: no sync.
  #describeMesh(firstSequence: number): void {
    const file = path.join(this.dir, "MESH.json");
    if (fs.existsSync(file)) return;
    writeFileAtomic(file, `${JSON.stringify({
      version: 1, meshRoot: this.meshRoot, host: os.hostname(), firstSequence, createdAt: new Date().toISOString(),
    })}\n`, { durable: true });
  }

  // A torn last line belongs to an append that never returned: cut it, as the live log does.
  #repairAndReadLast(descriptor: number, file: string): { sequence: number; id: string } | undefined {
    let size = fs.fstatSync(descriptor).size;
    let tail = this.#lastLines(descriptor, size);
    if (tail.length && tail[tail.length - 1] !== 0x0a) {
      const cut = tail.lastIndexOf(0x0a) + 1;
      truncateTo(file, size - tail.length + cut);
      size = size - tail.length + cut;
      tail = this.#lastLines(descriptor, size);
    }
    const line = completeLines(tail.toString("utf8")).at(-1);
    const event = line === undefined ? undefined : parseEvent(line);
    return event ? { sequence: event.sequence, id: event.id } : undefined;
  }

  // The end of a file, back to the newline before its last line: 64 KiB covers almost every
  // event, so a publish reads one chunk, not the file.
  #lastLines(descriptor: number, size: number): Buffer {
    for (let readBytes = Math.min(size, 64 * 1024); ; readBytes = Math.min(size, readBytes * 4)) {
      const tail = Buffer.allocUnsafe(readBytes);
      fs.readSync(descriptor, tail, 0, readBytes, size - readBytes);
      if (readBytes === size || tail.lastIndexOf(0x0a, tail.length - 2) >= 0) return tail;
    }
  }
  #repairAndLoadDigest(descriptor: number, relative: string): {
    last: { sequence: number; id: string } | undefined; digest: DigestCheckpoint;
  } {
    let digest = this.#loadDigest(relative, fs.fstatSync(descriptor));
    const last = this.#repairAndReadLast(descriptor, path.join(this.dir, relative));
    const stat = fs.fstatSync(descriptor);
    if (digest.hash.bytes > stat.size) digest = this.#loadDigest(relative, stat);
    // Repair removed only a torn suffix, never any bytes in this validated durable prefix.
    return { last, digest };
  }

  #digestPath(relative: string): string {
    return `${path.posix.dirname(relative)}/.digest-${createHash("sha256").update(path.posix.basename(relative)).digest("hex")}.json`;
  }

  #loadDigest(relative: string, stat: fs.Stats): DigestCheckpoint {
    // A plain (smarty-dev#8305) checkpoint may be missing, stale or torn after a power loss:
    // it is a cache of the segment's own bytes, so anything unreadable is rebuilt from them.
    let saved: DigestCheckpoint | undefined;
    try { saved = this.#readJson<DigestCheckpoint>(this.#digestPath(relative)); }
    catch (error) { if (!(error instanceof SyntaxError)) throw error; }
    if (saved !== undefined && (typeof saved !== "object" || saved === null || typeof saved.identity !== "string" ||
        typeof saved.hash !== "object" || saved.hash === null || !Number.isSafeInteger(saved.hash.bytes) ||
        !Number.isSafeInteger(saved.lines) || !Number.isSafeInteger(saved.firstSequence) ||
        !Number.isSafeInteger(saved.lastSequence))) saved = undefined;
    const identity = fileIdentity(stat);
    const prior = saved?.identity.split(":");
    // Checkpoints are installed only after the live append (or catch-up). Both protocols
    // are append-only; rollback only removes an uncommitted suffix. A grown same-inode file
    // therefore retains this durable prefix, including after a crash before checkpointing.
    const appended = prior?.[0] === String(stat.dev) && prior[1] === String(stat.ino) &&
      Number(prior[2]) < stat.size;
    if (saved?.version === 1 && (saved.identity === identity || appended) && saved.hash.bytes <= stat.size) {
      // Reject a malformed checkpoint (rebuild below); never emit a false seal.
      try { new ArchiveSha256(saved.hash); return saved; } catch { /* rebuild */ }
    }
    // Replacement, shrink, or same-size modification: never trust a stale digest. Legacy
    // data is rebuilt in bounded slices, with oversized lines parsed only off-lock.
    return { version: 1, identity: fileIdentity(stat), hash: new ArchiveSha256().state(),
      lines: 0, firstSequence: 0, lastSequence: 0 };
  }

  #countLine(digest: DigestCheckpoint, event: MeshEvent): void {
    digest.firstSequence = digest.lines ? Math.min(digest.firstSequence, event.sequence) : event.sequence;
    digest.lastSequence = Math.max(digest.lastSequence, event.sequence);
    digest.lines++;
  }

  #advanceDigest(descriptor: number, relative: string, digest: DigestCheckpoint, budget: { bytes: number }): void {
    const size = fs.fstatSync(descriptor).size;
    const length = Math.min(budget.bytes, size - digest.hash.bytes);
    if (length <= 0) return;
    const bytes = Buffer.allocUnsafe(length);
    const read = fs.readSync(descriptor, bytes, 0, length, digest.hash.bytes);
    budget.bytes -= read;
    // End on a newline, except at EOF (old seals also hash a torn suffix). Oversized legacy
    // lines are deferred, never read without a byte budget.
    const end = digest.hash.bytes + read === size ? read : bytes.subarray(0, read).lastIndexOf(0x0a) + 1;
    if (!end) {
      if (!this.#readJson("DIGEST-REPAIR.json")) {
        writeFileAtomic(path.join(this.dir, "DIGEST-REPAIR.json"), JSON.stringify({ file: relative }));
      }
      return;
    }
    const chunk = bytes.subarray(0, end);
    digest.hash = new ArchiveSha256(digest.hash).update(chunk).state();
    for (const line of completeLines(chunk.toString("utf8"))) {
      const event = parseEvent(line);
      if (event) this.#countLine(digest, event);
    }
  }

  #appendDigest(digest: DigestCheckpoint, size: number, entry: MeshArchiveEntry): void {
    if (digest.hash.bytes !== size) return;
    digest.hash = new ArchiveSha256(digest.hash).update(Buffer.from(`${entry.line}\n`, "utf8")).state();
    this.#countLine(digest, entry.event);
  }

  #saveDigest(relative: string, digest: DigestCheckpoint, stat: fs.Stats, durable = true): void {
    digest.identity = fileIdentity(stat);
    writeFileAtomic(path.join(this.dir, this.#digestPath(relative)), `${JSON.stringify(digest)}\n`, { durable });
  }

  #seal(day: string, budget: { bytes: number }): boolean {
    const directory = path.join(this.dir, day);
    const files: Record<string, SealFile> = {};
    let complete = true;
    for (const name of fs.readdirSync(directory).filter((entry) => entry.endsWith(".jsonl")).sort()) {
      const relative = `${day}/${name}`;
      const descriptor = fs.openSync(path.join(directory, name), "r");
      try {
        const stat = fs.fstatSync(descriptor);
        const digest = this.#loadDigest(relative, stat);
        if (digest.hash.bytes !== stat.size) {
          const before = digest.hash.bytes;
          this.#advanceDigest(descriptor, relative, digest, budget);
          if (digest.hash.bytes !== before) this.#saveDigest(relative, digest, stat);
        }
        if (digest.hash.bytes !== stat.size) { complete = false; continue; }
        if (digest.lines) files[name] = { lines: digest.lines, firstSequence: digest.firstSequence,
          lastSequence: digest.lastSequence, sha256: new ArchiveSha256(digest.hash).digest() };
      } finally { fs.closeSync(descriptor); }
    }
    if (!complete) return false;
    writeFileAtomic(path.join(directory, "SEAL.json"), `${JSON.stringify({ version: 1, day, files })}\n`, { durable: true });
    return true;
  }
}
