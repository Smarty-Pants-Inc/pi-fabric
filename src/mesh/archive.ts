import { createHash } from "node:crypto";
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
 *   <dir>/<yyyy>/<mm>/<dd>/ABORTED.json  reserved events overtaken before live publication
 *   <dir>/sequence-index/<bucket>/<n>.json  exact line address and keyed live-commit proof
 *
 * A publish commits when its event reaches the live log. Before that, the archive holds it
 * durably but no reader sees it. Ordinary recovery cuts uncommitted appends back out;
 * a keyed intent retry can first complete the live publication. An overtaken reservation
 * is aborted instead: readers must never discover it behind their cursor.
 * The mesh root enables the archive with `event-archive.json` ({ "version": 1, "dir": "/abs" }),
 * so every store of that root archives, whatever its process's configuration. The store calls
 * this under its publish lock; nothing here locks.
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
  /** False until the keyed event reaches the live log; absent on legacy sidecars. */
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

interface SealFile {
  lines: number;
  firstSequence: number;
  lastSequence: number;
  sha256: string;
}

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
  begin(entry: MeshArchiveEntry): MeshArchivePending {
    this.#requireRoot();
    const relative = this.#fileFor(entry.event, this.#headDay());
    const absolute = path.join(this.dir, relative);
    this.#describeMesh(entry.event.sequence);
    fs.mkdirSync(path.dirname(absolute), { recursive: true, mode: 0o700 });
    let descriptor: number | undefined = fs.openSync(absolute, "a+", 0o600);
    let pending: MeshArchivePending | undefined;
    try {
      this.#repairAndReadLast(descriptor, absolute);
      pending = {
        sequence: entry.event.sequence,
        id: entry.event.id,
        file: relative,
        size: fs.fstatSync(descriptor).size,
        indexed: true,
        ...(entry.event.dedupeKey ? { dedupe: true as const } : {}),
      };
      const fresh = pending.size === 0;
      writeFileAtomic(path.join(this.dir, "PENDING.json"), `${JSON.stringify(pending)}\n`);
      const bytes = Buffer.from(`${entry.line}\n`, "utf8");
      writeAll(descriptor, bytes);
      fs.fdatasyncSync(descriptor);
      this.#writeIndex(entry.event.sequence, { sequence: entry.event.sequence, id: entry.event.id, file: relative, offset: pending.size, length: bytes.length, ...(entry.event.dedupeKey ? { committed: false } : {}) });
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
  commit(pending: MeshArchivePending, sealClosedDays = true): void {
    if (pending.dedupe) this.confirmLive(pending.sequence, pending.id);
    // PENDING goes first: a stop before the head moves leaves a head that is behind, and the
    // catch-up skips the event it finds already archived.
    fs.rmSync(path.join(this.dir, "PENDING.json"), { force: true });
    this.#writeHead({ sequence: pending.sequence, id: pending.id, file: pending.file });
    if (sealClosedDays) this.#sealClosedDays(pending.file);
  }

  /** A direct live anchor (or a synced reboot promotion) proves publication, not just append. */
  confirmLive(sequence: number, id: string): void {
    const indexed = this.#readJson<MeshArchiveIndexEntry | { absent: true }>(path.relative(this.dir, this.#indexPath(sequence)));
    // Old catch-ups had no sidecar; only new reserved entries need this transition.
    if (!indexed || "absent" in indexed) return;
    if (indexed.id !== id) throw new MeshArchiveLookupUnavailableError("Cannot commit a mismatched archive sequence index");
    if (indexed.committed === false) this.#writeIndex(sequence, { ...indexed, committed: true });
  }

  /** The event never went live: cut it back out of its file. */
  rollback(pending: MeshArchivePending): void {
    this.#cutBack(pending);
  }

  /**
   * Settles what a stop left, before a publish.
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
   * The store then calls recovered(), which records this boot.
   */
  recover(lastLive: number): MeshArchiveRecovery {
    const pending = this.pending();
    if (this.#readText("BOOT") === currentBoot()) {
      if (pending) {
        if ((this.head()?.sequence ?? 0) >= pending.sequence) fs.rmSync(path.join(this.dir, "PENDING.json"), { force: true });
        else this.#cutBack(pending);
      }
      return { rebooted: false, promote: [] };
    }
    this.#requireRoot();
    if (pending) {
      const absolute = path.join(this.dir, pending.file);
      if (fs.existsSync(absolute)) {
        const descriptor = fs.openSync(absolute, "a+", 0o600);
        try {
          this.#repairAndReadLast(descriptor, absolute);
        } finally {
          fs.closeSync(descriptor);
        }
      }
      fs.rmSync(path.join(this.dir, "PENDING.json"), { force: true });
    }
    const found = new Map<string, MeshArchiveEntry & { file: string }>();
    for (const day of this.#days()) {
      const aborted = this.#readJson<Record<string, string>>(`${day}/ABORTED.json`);
      const seal = this.#readJson<{ files: Record<string, SealFile> }>(`${day}/SEAL.json`);
      if (seal && Math.max(0, ...Object.values(seal.files).map((file) => file.lastSequence)) <= lastLive) continue;
      for (const name of fs.readdirSync(path.join(this.dir, day)).filter((entry) => entry.endsWith(".jsonl"))) {
        for (const line of completeLines(fs.readFileSync(path.join(this.dir, day, name), "utf8"))) {
          const event = parseEvent(line);
          if (event && event.sequence > lastLive && aborted?.[event.sequence] !== event.id) found.set(event.id, { event, line, file: `${day}/${name}` });
        }
      }
    }
    return { rebooted: true, promote: [...found.values()].sort((left, right) => left.event.sequence - right.event.sequence) };
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
          file = { descriptor, last: undefined, relative };
          open.set(relative, file);
          file.last = this.#repairAndReadLast(descriptor, absolute);
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
        if (event.dedupeKey) {
          fs.fdatasyncSync(file.descriptor);
          this.#writeIndex(event.sequence, { sequence: event.sequence, id: event.id, file: relative, offset, length: bytes.length, committed: true });
        }
        file.last = { sequence: event.sequence, id: event.id };
      }
      for (const { descriptor } of open.values()) fs.fdatasyncSync(descriptor);
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

  /** The exact archived bytes and whether they were ever committed to the live log. */
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
      const bytes = Buffer.allocUnsafe(indexed.length);
      const count = fs.readSync(descriptor, bytes, 0, bytes.length, indexed.offset);
      if (count !== bytes.length || bytes[bytes.length - 1] !== 0x0a) throw new Error("short archive line");
      const line = bytes.subarray(0, bytes.length - 1).toString("utf8");
      const event = parseEvent(line);
      if (!event || event.sequence !== sequence || event.id !== indexed.id) throw new Error("archive index mismatch");
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
   * A reserved event overtaken by the live log must never fill a hole behind a cursor.
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
   * `through` is the newest live sequence: nothing past it, and no pending event, is committed.
   */
  readAfter(after: number, through: number, matches: (event: MeshEvent) => boolean, limit: number, topic?: string): MeshEvent[] {
    const pendingId = this.pending()?.id;
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
          if (event && event.sequence > after && event.sequence <= through && event.id !== pendingId && aborted?.[event.sequence] !== event.id && this.#isPublished(event) && matches(event)) {
            dayEvents.push(event);
          }
        }
      }
      found.push(...dayEvents.sort((left, right) => left.sequence - right.sequence));
      if (found.length >= limit) break;
    }
    return found.slice(0, limit);
  }

  // PENDING can be overwritten by a mixed-version writer. Its archive-only reservation
  // must still stay invisible until live commitment, even when a later sequence is live.
  #isPublished(event: MeshEvent): boolean {
    if (!event.dedupeKey) return true;
    const indexed = this.#readJson<MeshArchiveIndexEntry | { absent: true }>(path.relative(this.dir, this.#indexPath(event.sequence)));
    return !indexed || (!("absent" in indexed) && indexed.committed !== false);
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
    for (const day of unsealed) this.#seal(day);
  }

  #cutBack(pending: MeshArchivePending): void {
    try {
      truncateTo(path.join(this.dir, pending.file), pending.size);
      if (pending.indexed) this.reserveLookup(pending.sequence);
    } catch (error) {
      if (errorCode(error) !== "ENOENT") throw error;
    }
    fs.rmSync(path.join(this.dir, "PENDING.json"), { force: true });
  }

  /** Install a durable proof of non-append before creating a publication intent. */
  reserveLookup(sequence: number): void {
    this.#requireRoot();
    this.#writeIndex(sequence, { sequence, absent: true });
  }

  #indexPath(sequence: number): string {
    return path.join(this.dir, MESH_ARCHIVE_SEQUENCE_INDEX, String(Math.floor(sequence / 1024)), `${sequence}.json`);
  }

  #writeIndex(sequence: number, entry: MeshArchiveIndexEntry | { sequence: number; absent: true }): void {
    writeFileAtomic(this.#indexPath(sequence), JSON.stringify(entry), { durable: true });
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

  #seal(day: string): void {
    const directory = path.join(this.dir, day);
    const files: Record<string, SealFile> = {};
    for (const name of fs.readdirSync(directory).filter((entry) => entry.endsWith(".jsonl")).sort()) {
      const bytes = fs.readFileSync(path.join(directory, name));
      let lines = 0;
      let firstSequence = Number.POSITIVE_INFINITY;
      let lastSequence = 0;
      for (const line of completeLines(bytes.toString("utf8"))) {
        const event = parseEvent(line);
        if (!event) continue;
        lines++;
        firstSequence = Math.min(firstSequence, event.sequence);
        lastSequence = Math.max(lastSequence, event.sequence);
      }
      if (lines === 0) continue;
      files[name] = { lines, firstSequence, lastSequence, sha256: createHash("sha256").update(bytes).digest("hex") };
    }
    writeFileAtomic(path.join(directory, "SEAL.json"), `${JSON.stringify({ version: 1, day, files })}\n`);
  }
}
