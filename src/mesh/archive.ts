import { createHash } from "node:crypto";
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
 *
 * A publish commits when its event reaches the live log. Before that, the archive holds it
 * durably but no reader sees it; a publish that fails or crashes first is cut back out.
 * The mesh root enables the archive with `event-archive.json` ({ "version": 1, "dir": "/abs" }),
 * so every store of that root archives, whatever its process's configuration.
 * New writers call read-only preflights before acquiring the publish lock; mutation and
 * checkpoint installation remain under that lock. Legacy writers use the same lock and
 * ignore the new checkpoint/repair metadata, so optimistic reads are validated against them.
 */
export const MESH_ARCHIVE_CONFIG = "event-archive.json";

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
  begin(entry: MeshArchiveEntry): MeshArchivePending {
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
        digestBefore: structuredClone(digest) };
      this.#advanceDigest(descriptor, relative, digest, { bytes: ARCHIVE_DIGEST_SLICE_BYTES });
      const fresh = pending.size === 0;
      this.#appendDigest(digest, pending.size, entry);
      pending.digestAfter = digest;
      writeFileAtomic(path.join(this.dir, "PENDING.json"), `${JSON.stringify(pending)}\n`);
      writeAll(descriptor, Buffer.from(`${entry.line}\n`, "utf8"));
      fs.fdatasyncSync(descriptor);
      if (fresh) this.#syncDays(relative);
    } catch (error) {
      fs.closeSync(descriptor);
      descriptor = undefined;
      if (pending) this.#cutBack(pending);
      throw error;
    } finally {
      if (descriptor !== undefined) fs.closeSync(descriptor);
    }
    return pending;
  }

  /** The event is live: it is committed. Moves the head and seals any closed day. */
  commit(pending: MeshArchivePending): void {
    if (pending.digestAfter) this.#saveDigest(pending.file, pending.digestAfter, fs.statSync(path.join(this.dir, pending.file)));
    // PENDING goes first: a stop before the head moves leaves a head that is behind, and the
    // catch-up skips the event it finds already archived.
    fs.rmSync(path.join(this.dir, "PENDING.json"), { force: true });
    this.#writeHead({ sequence: pending.sequence, id: pending.id, file: pending.file });
    this.#sealClosedDays(pending.file);
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
              found.set(event.id, { event, line, file });
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
      if (!stat || identity !== fileIdentity(stat)) throw new MeshArchiveRecoveryChanged();
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
   */
  recover(lastLive: number, plan?: MeshArchiveRecoveryPlan): MeshArchiveRecovery {
    const pending = this.pending();
    if (this.#readText("BOOT") === currentBoot()) {
      if (pending) {
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

  /** After a new boot's recovery: the promoted events are live. Records the boot, durably. */
  recovered(last: (MeshArchiveEntry & { file: string }) | undefined): void {
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
        if (previous && (previous.sequence > event.sequence || (previous.sequence === event.sequence && previous.id === event.id))) {
          continue;
        }
        const size = fs.fstatSync(file.descriptor).size;
        writeAll(file.descriptor, Buffer.from(`${line}\n`, "utf8"));
        this.#appendDigest(file.digest, size, { event, line });
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
   * Committed events after a sequence, in sequence order, for reads older than the live log.
   * `through` is the newest live sequence: nothing past it, and no pending event, is committed.
   */
  readAfter(after: number, through: number, matches: (event: MeshEvent) => boolean, limit: number, topic?: string): MeshEvent[] {
    const pendingId = this.pending()?.id;
    const found: MeshEvent[] = [];
    for (const day of this.#days()) {
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
          if (event && event.sequence > after && event.sequence <= through && event.id !== pendingId && matches(event)) {
            dayEvents.push(event);
          }
        }
      }
      found.push(...dayEvents.sort((left, right) => left.sequence - right.sequence));
      if (found.length >= limit) break;
    }
    return found.slice(0, limit);
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
    } catch (error) {
      if (errorCode(error) !== "ENOENT") throw error;
    }
    fs.rmSync(path.join(this.dir, "PENDING.json"), { force: true });
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

  #saveDigest(relative: string, digest: DigestCheckpoint, stat: fs.Stats): void {
    digest.identity = fileIdentity(stat);
    writeFileAtomic(path.join(this.dir, this.#digestPath(relative)), `${JSON.stringify(digest)}\n`, { durable: true });
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
