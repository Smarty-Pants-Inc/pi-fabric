import { createHash, randomUUID } from "node:crypto";
import { ArchiveSha256, type Sha256State } from "./archive-sha256.js";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { writeFileAtomic } from "../core/atomic-write.js";
import type { MeshEvent } from "./store.js";

/**
 * The durable fleet log (smarty-dev#754, Paul's decision 2): every event of a mesh root as plain
 * append-only files, one per topic and UTC day, in the node's file tree (smarty-dev#1120):
 *
 *   <dir>/MESH.json                      the mesh root this archive belongs to
 *   <dir>/HEAD.json                      the last event known to be archived and live
 *   <dir>/PENDING.json                   a publish between its archive append and its commit
 *   <dir>/<yyyy>/<mm>/<dd>/<topic>.jsonl  one line per event, the same bytes as the live log
 *   <dir>/<yyyy>/<mm>/<dd>/SEAL.json     per file line count, sequence range and sha256
 *   <dir>/<yyyy>/<mm>/<dd>/.digest-*.json durable per-file SHA-256 state and counters
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
const MESH_ARCHIVE_UNSYNCED = "unsynced";

/**
 * Durability work a deferred begin/commit leaves for AFTER the mesh lock is released
 * (smarty-dev#4383): no archive fsync runs while the fleet-wide lock is held. A publish
 * returns only after runArchiveBarrier() succeeds, so an acknowledged event is as durable
 * as before. Absolute paths only.
 */
export interface MeshArchiveBarrier {
  /** Topic files whose appended lines need a data barrier. */
  data: Set<string>;
  /** Small metadata files (sequence index, digest checkpoint, seal) written by rename. */
  files: Set<string>;
  /** Directories whose new names need a barrier (opened as O_RDONLY). */
  dirs: Set<string>;
  /** A new topic file's day chain, synced as #syncDays does (opened "r"). */
  chains: Set<string>;
}

export const newArchiveBarrier = (): MeshArchiveBarrier => ({ data: new Set(), files: new Set(), dirs: new Set(), chains: new Set() });

const syncDirectoryHandle = (directory: string): void => {
  if (process.platform === "win32") return;
  const descriptor = fs.openSync(directory, fs.constants.O_RDONLY);
  try { fs.fsyncSync(descriptor); } finally { fs.closeSync(descriptor); }
};

/** Off-lock: one grouped pass, data first, then metadata files, then their names. */
export const runArchiveBarrier = (barrier: MeshArchiveBarrier): void => {
  for (const file of barrier.data) {
    const descriptor = fs.openSync(file, process.platform === "win32" ? "r+" : "r");
    try { fs.fdatasyncSync(descriptor); } finally { fs.closeSync(descriptor); }
  }
  for (const file of barrier.files) {
    let descriptor: number;
    // A later writer may already have replaced this name; its own barrier covers it.
    try { descriptor = fs.openSync(file, process.platform === "win32" ? "r+" : "r"); }
    catch (error) { if (errorCode(error) === "ENOENT") continue; throw error; }
    try { fs.fsyncSync(descriptor); } finally { fs.closeSync(descriptor); }
  }
  for (const directory of barrier.chains) syncDirectory(directory);
  for (const directory of barrier.dirs) syncDirectoryHandle(directory);
};

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
  promote: Array<MeshArchiveEntry & { file: string }>;
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
  promote: Array<MeshArchiveEntry & { file: string }>;
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

// Shared by all files/days touched by a sealing pass, not a per-file allowance.
export const ARCHIVE_DIGEST_SLICE_BYTES = 64 * 1024;
const fileIdentity = (stat: fs.Stats): string =>
  `${stat.dev}:${stat.ino}:${stat.size}:${stat.mtimeMs}:${stat.ctimeMs}`;

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
  begin(entry: MeshArchiveEntry, barrier?: MeshArchiveBarrier): MeshArchivePending {
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
      // Deferred: the caller syncs after releasing the mesh lock and before acknowledging.
      if (barrier) barrier.data.add(absolute);
      else fs.fdatasyncSync(descriptor);
      this.#writeIndex(entry.event.sequence, { sequence: entry.event.sequence, id: entry.event.id, file: relative, offset: pending.size, length: bytes.length, ...(entry.event.dedupeKey ? { committed: false } : {}) }, barrier);
      if (fresh) {
        if (barrier) for (const directory of this.#dayChain(relative)) barrier.chains.add(directory);
        else this.#syncDays(relative);
      }
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
  commit(pending: MeshArchivePending, sealClosedDays = true, barrier?: MeshArchiveBarrier): void {
    if (pending.digestAfter) this.#saveDigest(pending.file, pending.digestAfter, fs.statSync(path.join(this.dir, pending.file)), barrier);
    if (pending.dedupe) this.confirmLive(pending.sequence, pending.id, barrier);
    // PENDING goes first: a stop before the head moves leaves a head that is behind, and the
    // catch-up skips the event it finds already archived.
    fs.rmSync(path.join(this.dir, "PENDING.json"), { force: true });
    this.#writeHead({ sequence: pending.sequence, id: pending.id, file: pending.file });
    if (sealClosedDays) this.#sealClosedDays(pending.file, barrier);
  }

  /** A direct live anchor (or a synced reboot promotion) proves publication, not just append. */
  confirmLive(sequence: number, id: string, barrier?: MeshArchiveBarrier): void {
    const indexed = this.#readJson<MeshArchiveIndexEntry | { absent: true }>(path.relative(this.dir, this.#indexPath(sequence)));
    // Old catch-ups had no sidecar; only new reserved entries need this transition.
    if (!indexed || "absent" in indexed) return;
    if (indexed.id !== id) throw new MeshArchiveLookupUnavailableError("Cannot commit a mismatched archive sequence index");
    if (indexed.committed === false) this.#writeIndex(sequence, { ...indexed, committed: true }, barrier);
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
      fs.rmSync(path.join(this.dir, "DIGEST-REPAIR.json"), { force: true });
      return;
    }
    if (fileIdentity(stat) !== repair.identity) return;
    this.#saveDigest(repair.file, repair.digest, stat);
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
    const found = new Map<string, MeshArchiveEntry & { file: string }>();
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
            const lines = completeLines(bytes.subarray(split).toString("utf8"));
            for (let i = lines.length - 1; i >= 0; i--) {
              const line = lines[i]!;
              const event = parseEvent(line);
              if (!event) continue;
              if (event.sequence <= lastLive) { done = true; break; }
              if (aborted?.[event.sequence] !== event.id) found.set(event.id, { event, line, file });
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
      fs.rmSync(path.join(this.dir, "PENDING.json"), { force: true });
    }
    return { rebooted: true, promote: plan.promote.filter(entry => entry.event.sequence > lastLive) };
  }

  /** True when this boot has not yet run the archive's reboot recovery (BOOT names another boot). */
  rebootPending(): boolean {
    return this.#readText("BOOT") !== currentBoot();
  }

  /**
   * Off-lock, BEFORE a deferred (no fsync under the lock) begin: a durable note that live
   * events after `since` may have archive lines that are not yet synced. A power loss can
   * keep such a live line (any later data barrier or writeback syncs the live log) while
   * losing its archive line; reboot recovery then restores those lines from the live log
   * (restoreUnsynced). Removed once the publication's archive barrier has succeeded.
   */
  stageUnsynced(since: number): string {
    this.#requireRoot();
    const directory = path.join(this.dir, MESH_ARCHIVE_UNSYNCED);
    const fresh = !fs.existsSync(directory);
    fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
    const file = path.join(directory, `${process.pid}.${randomUUID()}.json`);
    const descriptor = fs.openSync(file, "wx", 0o600);
    try {
      writeAll(descriptor, Buffer.from(JSON.stringify({ since, pid: process.pid, boot: currentBoot(), at: Date.now() }), "utf8"));
      fs.fsyncSync(descriptor);
    } finally { fs.closeSync(descriptor); }
    syncDirectoryHandle(directory);
    if (fresh) syncDirectoryHandle(this.dir);
    return file;
  }

  clearUnsynced(file: string): void {
    fs.rmSync(file, { force: true });
  }

  #unsyncedMarkers(): Array<{ file: string; since?: number; pid?: number; boot?: string; at?: number }> {
    const directory = path.join(this.dir, MESH_ARCHIVE_UNSYNCED);
    let names: string[];
    try { names = fs.readdirSync(directory); }
    catch (error) { if (errorCode(error) === "ENOENT") return []; throw error; }
    return names.filter(name => name.endsWith(".json")).map(name => {
      const file = path.join(directory, name);
      try { return { file, ...(JSON.parse(fs.readFileSync(file, "utf8")) as { since?: number; pid?: number; boot?: string; at?: number }) }; }
      catch { return { file }; } // Torn or vanished: an unsynced marker of a dead attempt.
    });
  }

  /** Under the mesh lock, in reboot recovery only: markers of earlier boots and the lowest
   * live sequence they cover. A marker of THIS boot may belong to a waiting publisher. */
  previousBootUnsynced(): { since: number; files: string[] } | undefined {
    const boot = currentBoot();
    const markers = this.#unsyncedMarkers().filter(marker => marker.boot !== boot);
    if (!markers.length) return undefined;
    const since = Math.min(...markers.map(marker => Number.isSafeInteger(marker.since) && marker.since! >= 0 ? marker.since! : 0));
    return { since, files: markers.map(marker => marker.file) };
  }

  /** Same boot only: a dead process's marker is moot, its page-cache writes survived. */
  sweepUnsynced(alive: (pid: number) => boolean): void {
    const boot = currentBoot();
    for (const marker of this.#unsyncedMarkers()) {
      if (marker.boot !== boot || !Number.isSafeInteger(marker.pid) || marker.pid === process.pid || alive(marker.pid!)) continue;
      fs.rmSync(marker.file, { force: true });
    }
  }

  /**
   * Reboot recovery, under the mesh lock: re-archive live events (after an earlier boot's
   * unsynced marker) whose archive lines a power loss took. A present line (by its sequence
   * index or its exact bytes in its topic file) is never written twice; the head never moves
   * back. Synced here, before the markers go: this path is once per boot.
   */
  restoreUnsynced(entries: MeshArchiveEntry[], markers: string[]): void {
    const missing = entries.filter(entry => !this.#holdsLine(entry));
    if (missing.length) {
      const head = this.head();
      this.catchUp(missing);
      if (head && (this.head()?.sequence ?? 0) < head.sequence) writeDurable(path.join(this.dir, "HEAD.json"), `${JSON.stringify(head)}\n`);
    }
    for (const file of markers) fs.rmSync(file, { force: true });
    syncDirectoryHandle(path.join(this.dir, MESH_ARCHIVE_UNSYNCED));
  }

  #holdsLine(entry: MeshArchiveEntry): boolean {
    try { if (this.lookupEntry(entry.event.sequence)?.event.id === entry.event.id) return true; }
    catch { /* an unsynced index is not evidence either way: read the topic files */ }
    const candidates = new Set([this.#fileFor(entry.event, undefined), this.#fileFor(entry.event, this.#headDay())]);
    for (const relative of candidates) {
      let text: string;
      try { text = fs.readFileSync(path.join(this.dir, relative), "utf8"); }
      catch (error) { if (errorCode(error) === "ENOENT") continue; throw error; }
      if (text.startsWith(`${entry.line}\n`) || text.includes(`\n${entry.line}\n`)) return true;
    }
    return false;
  }

  /** After a new boot's recovery: the promoted events are live. Records the boot, durably. */
  recovered(last: (MeshArchiveEntry & { file: string }) | undefined, promoted: MeshArchiveEntry[] = []): void {
    for (const { event } of promoted) if (event.dedupeKey) this.confirmLive(event.sequence, event.id);
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
  lookupEntry(sequence: number): (MeshArchiveEntry & { committed: boolean }) | undefined {
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
      let newline = bytes.subarray(0, count).indexOf(0x0a);
      // The old writer may reuse that address for a differently sized event. Resolve
      // only this one bounded line, never search the segment for the missing identity.
      while (newline < 0 && count === bytes.length && bytes.length < available && bytes.length < 64 * 1024 * 1024) {
        bytes = Buffer.allocUnsafe(Math.min(Math.max(bytes.length * 2, 4096), available, 64 * 1024 * 1024));
        count = fs.readSync(descriptor, bytes, 0, bytes.length, indexed.offset);
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
      fs.rmSync(path.join(this.dir, "PENDING.json"), { force: true });
    }
  }

  /**
   * Committed events after a sequence, in sequence order, for reads older than the live log.
   * `through` is the newest live sequence: nothing past it is published. Within that
   * horizon, only a positive abort hides an event; old writers may leave stale metadata.
   */
  readAfter(after: number, through: number, matches: (event: MeshEvent) => boolean, limit: number, topic?: string): MeshEvent[] {
    const found: MeshEvent[] = [];
    for (const day of this.#days()) {
      const aborted = this.#readJson<Record<string, string>>(`${day}/ABORTED.json`);
      const directory = path.join(this.dir, day);
      const seal = this.#readJson<{ files: Record<string, SealFile> }>(`${day}/SEAL.json`);
      if (seal && Math.max(0, ...Object.values(seal.files).map((file) => file.lastSequence)) <= after) continue;
      const names = topic !== undefined
        ? [archiveFileName(topic)]
        : fs.readdirSync(directory).filter((name) => name.endsWith(".jsonl"));
      const dayEvents: MeshEvent[] = [];
      for (const name of names) {
        if (seal?.files[name] && seal.files[name].lastSequence <= after) continue;
        let text: string;
        try {
          text = fs.readFileSync(path.join(directory, name), "utf8");
        } catch (error) {
          if (errorCode(error) === "ENOENT") continue;
          throw error;
        }
        for (const line of completeLines(text)) {
          const event = parseEvent(line);
          if (event && event.sequence > after && event.sequence <= through && this.#isPublished(event, aborted) && matches(event)) {
            dayEvents.push(event);
          }
        }
      }
      found.push(...dayEvents.sort((left, right) => left.sequence - right.sequence));
      if (found.length >= limit) break;
    }
    return found.slice(0, limit);
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
  #sealClosedDays(currentFile: string, barrier?: MeshArchiveBarrier): void {
    const current = currentFile.split("/").slice(0, 3).join("/");
    const unsealed: string[] = [];
    for (const day of this.#days().reverse()) {
      if (day >= current) continue;
      if (fs.existsSync(path.join(this.dir, day, "SEAL.json"))) break;
      unsealed.unshift(day);
    }
    const budget = { bytes: ARCHIVE_DIGEST_SLICE_BYTES };
    for (const day of unsealed) {
      if (!this.#seal(day, budget, barrier)) break; // Preserve the sealed-prefix invariant.
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
    fs.rmSync(path.join(this.dir, "PENDING.json"), { force: true });
  }

  /** Install a durable proof of non-append before creating a publication intent. */
  reserveLookup(sequence: number, barrier?: MeshArchiveBarrier): void {
    this.#requireRoot();
    this.#writeIndex(sequence, { sequence, absent: true }, barrier);
  }

  #indexPath(sequence: number): string {
    return path.join(this.dir, MESH_ARCHIVE_SEQUENCE_INDEX, String(Math.floor(sequence / 1024)), `${sequence}.json`);
  }

  #writeIndex(sequence: number, entry: MeshArchiveIndexEntry | { sequence: number; absent: true }, barrier?: MeshArchiveBarrier): void {
    const file = this.#indexPath(sequence);
    if (!barrier) { writeFileAtomic(file, JSON.stringify(entry), { durable: true }); return; }
    const bucket = path.dirname(file);
    if (!fs.existsSync(bucket)) { barrier.dirs.add(path.dirname(bucket)); barrier.dirs.add(this.dir); }
    writeFileAtomic(file, JSON.stringify(entry));
    barrier.files.add(file);
    barrier.dirs.add(bucket);
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
    for (const directory of this.#dayChain(relative)) syncDirectory(directory);
  }

  #dayChain(relative: string): string[] {
    const [year, month, day] = relative.split("/");
    return [`${year}/${month}/${day}`, `${year}/${month}`, `${year}`, ""].map(directory => path.join(this.dir, directory));
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

  #describeMesh(firstSequence: number): void {
    const file = path.join(this.dir, "MESH.json");
    if (fs.existsSync(file)) return;
    writeFileAtomic(file, `${JSON.stringify({
      version: 1, meshRoot: this.meshRoot, host: os.hostname(), firstSequence, createdAt: new Date().toISOString(),
    })}\n`);
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
    const saved = this.#readJson<DigestCheckpoint>(this.#digestPath(relative));
    const identity = fileIdentity(stat);
    const prior = saved?.identity.split(":");
    // Checkpoints are installed only after the live append (or catch-up). Both protocols
    // are append-only; rollback only removes an uncommitted suffix. A grown same-inode file
    // therefore retains this durable prefix, including after a crash before checkpointing.
    const appended = prior?.[0] === String(stat.dev) && prior[1] === String(stat.ino) &&
      Number(prior[2]) < stat.size;
    if (saved?.version === 1 && (saved.identity === identity || appended) && saved.hash.bytes <= stat.size) {
      new ArchiveSha256(saved.hash); // Reject a malformed checkpoint; never emit a false seal.
      return saved;
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

  #saveDigest(relative: string, digest: DigestCheckpoint, stat: fs.Stats, barrier?: MeshArchiveBarrier): void {
    digest.identity = fileIdentity(stat);
    const file = path.join(this.dir, this.#digestPath(relative));
    writeFileAtomic(file, `${JSON.stringify(digest)}\n`, barrier ? undefined : { durable: true });
    if (barrier) { barrier.files.add(file); barrier.dirs.add(path.dirname(file)); }
  }

  #seal(day: string, budget: { bytes: number }, barrier?: MeshArchiveBarrier): boolean {
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
          if (digest.hash.bytes !== before) this.#saveDigest(relative, digest, stat, barrier);
        }
        if (digest.hash.bytes !== stat.size) { complete = false; continue; }
        if (digest.lines) files[name] = { lines: digest.lines, firstSequence: digest.firstSequence,
          lastSequence: digest.lastSequence, sha256: new ArchiveSha256(digest.hash).digest() };
      } finally { fs.closeSync(descriptor); }
    }
    if (!complete) return false;
    const sealFile = path.join(directory, "SEAL.json");
    writeFileAtomic(sealFile, `${JSON.stringify({ version: 1, day, files })}\n`, barrier ? undefined : { durable: true });
    if (barrier) { barrier.files.add(sealFile); barrier.dirs.add(directory); }
    return true;
  }
}
