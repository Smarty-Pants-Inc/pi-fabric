import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { syncPathNamespaceDurable } from "../core/atomic-write.js";
import { acquireMeshCustodyLock, MESH_CUSTODY_LOCK_NAME } from "../mesh/custody-lock.js";

export type RecoveryJournalFailure = "deadline" | "aborted" | "store-unavailable" | "superseded";
export class RecoveryJournalError extends Error {
  override readonly name = "RecoveryJournalError";
  constructor(readonly reason: RecoveryJournalFailure, message: string, options?: ErrorOptions) { super(message, options); }
}
export interface RecoveryJournalRecord { readonly seq: number; readonly key: string; readonly bytes: Uint8Array }
export interface RecoveryJournalAppendOptions { readonly deadlineMs: number; readonly signal?: AbortSignal }
export interface RecoveryJournal {
  append(key: string, bytes: Uint8Array, options: RecoveryJournalAppendOptions): Promise<{ seq: number; existing: boolean }>;
  get(key: string): RecoveryJournalRecord | undefined;
  entries(): readonly RecoveryJournalRecord[];
  close(): Promise<void>;
}
export interface RecoveryJournalOptions { readonly directory: string; readonly storeId: string; readonly required?: boolean }

const hash = (bytes: Uint8Array | string): string => createHash("sha256").update(bytes).digest("hex");
const unavailable = (message: string, cause?: unknown): RecoveryJournalError =>
  new RecoveryJournalError("store-unavailable", message, { cause });
const checked = (options: RecoveryJournalAppendOptions): void => {
  if (options.signal?.aborted) throw new RecoveryJournalError("aborted", "Recovery append aborted");
  if (!Number.isFinite(options.deadlineMs) || Date.now() >= options.deadlineMs) throw new RecoveryJournalError("deadline", "Recovery append deadline elapsed");
};
const copy = (record: RecoveryJournalRecord): RecoveryJournalRecord => ({ seq: record.seq, key: record.key, bytes: Uint8Array.from(record.bytes) });
const IDENTITY = "journal.identity";
const RAW = /^raw-(\d{16})-([a-f0-9-]{36})\.jsonl$/;
const COMMIT = /^commit-(\d{16})-([a-f0-9-]{36})-([a-f0-9]{64})$/;
const identityBytes = (storeId: string): Buffer => Buffer.from(`${JSON.stringify({ version: 1, storeId })}\n`);
interface Retained { readonly stat: fs.Stats; readonly bytes: Buffer }
interface Frame { readonly version: 1; readonly storeId: string; readonly seq: number; readonly key: string; readonly bytes: string; readonly digest: string }
const digestFrame = (frame: Omit<Frame, "digest">): string => hash(JSON.stringify([frame.version, frame.storeId, frame.seq, frame.key, frame.bytes]));
const sameFile = (a: fs.Stats, b: fs.Stats): boolean => a.dev === b.dev && a.ino === b.ino && a.mode === b.mode && a.size === b.size;
const readRetained = (file: string): Retained => {
  const before = fs.lstatSync(file);
  if (!before.isFile() || before.isSymbolicLink()) throw unavailable("Recovery journal contains a non-regular file");
  const fd = fs.openSync(file, fs.constants.O_RDONLY | (fs.constants.O_NOFOLLOW ?? 0));
  try {
    if (!sameFile(before, fs.fstatSync(fd))) throw unavailable("Recovery journal file changed while opening");
    const bytes = fs.readFileSync(fd);
    const after = fs.fstatSync(fd);
    if (!sameFile(before, after) || !sameFile(after, fs.lstatSync(file)) || bytes.length !== after.size) throw unavailable("Recovery journal changed while reading");
    return { stat: after, bytes };
  } finally { fs.closeSync(fd); }
};
const barrier = (file: string, expected: Retained): void => {
  // A writable handle is required by FlushFileBuffers; opening does not mutate bytes.
  const fd = fs.openSync(file, fs.constants.O_RDWR | (fs.constants.O_NOFOLLOW ?? 0));
  try {
    if (!sameFile(expected.stat, fs.fstatSync(fd))) throw unavailable("Recovery journal changed before fsync");
    fs.fsyncSync(fd);
    syncPathNamespaceDurable(file, expected.stat, () => unavailable("Recovery journal namespace durability is unsupported"));
  } finally { fs.closeSync(fd); }
};
const createRetained = (file: string, bytes: Uint8Array): Retained => {
  const fd = fs.openSync(file, "wx", 0o600);
  try {
    let offset = 0;
    while (offset < bytes.length) {
      const count = fs.writeSync(fd, bytes, offset, bytes.length - offset);
      if (count <= 0) throw unavailable("Recovery journal write made no progress");
      offset += count;
    }
    fs.fsyncSync(fd);
    const expected = fs.fstatSync(fd);
    syncPathNamespaceDurable(file, expected, () => unavailable("Recovery journal namespace durability is unsupported"));
  } finally { fs.closeSync(fd); }
  const retained = readRetained(file);
  if (!retained.bytes.equals(Buffer.from(bytes))) throw unavailable("Recovery journal write readback mismatch");
  return retained;
};

/** One immutable raw segment per append; never repairs a tail in place. A zero-byte commit
 * witness names its segment and complete-frame hash. Witness names are atomic; successful
 * ACKs require BOTH file and full namespace barriers. After validating ALL witness targets
 * and complete sequence history, an unfinished raw segment without a witness is provably
 * unacknowledged: REQUIRED recovery retains/reflushes it and appends in a NEW segment. A
 * witnessed missing/truncated segment is incomplete committed evidence and fails closed.
 * Complete unwitnessed appends are adopted only after reflush, keeping their original seq.
 * The journal is local persistence, not a claim about replicated/off-victim storage. */
class FileRecoveryJournal implements RecoveryJournal {
  private readonly records = new Map<string, RecoveryJournalRecord>();
  private readonly retained = new Map<string, Retained>();
  private closed = false;
  private failed: RecoveryJournalError | undefined;
  private queue: Promise<unknown> = Promise.resolve();
  private nextSeq = 1;
  private readonly directoryStat: fs.Stats;
  private readonly owner: Retained;
  constructor(private readonly directory: string, private readonly storeId: string, private readonly release: () => void) {
    this.directoryStat = fs.lstatSync(directory);
    this.owner = readRetained(path.join(directory, MESH_CUSTODY_LOCK_NAME, "owner"));
  }
  private names(): string[] {
    return fs.readdirSync(this.directory).filter(name => name === IDENTITY || name.startsWith("raw-") || name.startsWith("commit-")).sort();
  }
  private assertAvailable(): void {
    if (this.closed) throw unavailable("Recovery journal is closed");
    if (this.failed) throw this.failed;
    try {
      const current = fs.lstatSync(this.directory);
      if (!current.isDirectory() || current.isSymbolicLink() || current.dev !== this.directoryStat.dev || current.ino !== this.directoryStat.ino) throw unavailable("Recovery journal directory identity changed");
      const owner = readRetained(path.join(this.directory, MESH_CUSTODY_LOCK_NAME, "owner"));
      if (!sameFile(this.owner.stat, owner.stat) || !this.owner.bytes.equals(owner.bytes)) throw unavailable("Recovery journal custody changed");
      const names = this.names();
      if (names.length !== this.retained.size || names.some(name => !this.retained.has(name))) throw unavailable("Recovery journal inventory changed; reopen required");
      for (const [name, expected] of this.retained) {
        const actual = readRetained(path.join(this.directory, name));
        if (!sameFile(expected.stat, actual.stat) || !expected.bytes.equals(actual.bytes)) throw unavailable("Recovery journal retained range changed; reopen required");
      }
    } catch (error) { this.failed = unavailable("Recovery journal availability lost; successful recovery required", error); throw this.failed; }
  }
  recover(): void {
    const names = this.names();
    for (const name of names) this.retained.set(name, readRetained(path.join(this.directory, name)));
    if (!this.retained.get(IDENTITY)?.bytes.equals(identityBytes(this.storeId))) throw unavailable("Recovery journal store identity conflict or incomplete identity");
    const complete = new Map<string, RecoveryJournalRecord>();
    const sequences = new Set<number>();
    for (const name of names) {
      if (name === IDENTITY || COMMIT.test(name)) continue;
      const match = RAW.exec(name);
      if (!match) throw unavailable("Unrecognized recovery journal segment");
      const raw = this.retained.get(name)!.bytes;
      // Do not grant authority yet: every witness target is checked below. Only after
      // that check can an unwitnessed unfinished frame be classified as unacknowledged.
      if (raw.length === 0 || raw.at(-1) !== 10) continue;
      let frame: Frame;
      try { frame = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(raw)); }
      catch (error) { throw unavailable("Corrupt complete recovery journal frame", error); }
      if (!frame || frame.version !== 1 || frame.storeId !== this.storeId || !Number.isSafeInteger(frame.seq) || frame.seq <= 0 ||
        String(frame.seq).padStart(16, "0") !== match[1] || typeof frame.key !== "string" || !frame.key || typeof frame.bytes !== "string" ||
        typeof frame.digest !== "string" || frame.digest !== digestFrame(frame) || Buffer.from(frame.bytes, "base64").toString("base64") !== frame.bytes ||
        !raw.equals(Buffer.from(`${JSON.stringify(frame)}\n`))) throw unavailable("Invalid recovery journal frame identity/hash/encoding");
      if (sequences.has(frame.seq) || this.records.has(frame.key)) throw unavailable("Conflicting recovery journal sequence or key");
      const record = { seq: frame.seq, key: frame.key, bytes: Uint8Array.from(Buffer.from(frame.bytes, "base64")) };
      sequences.add(frame.seq); this.records.set(frame.key, record); complete.set(name, record);
    }
    const witnessed = new Set<string>();
    for (const name of names) {
      if (!name.startsWith("commit-")) continue;
      const match = COMMIT.exec(name);
      if (!match || this.retained.get(name)!.bytes.length !== 0) throw unavailable("Invalid recovery journal commit witness");
      const rawName = `raw-${match[1]}-${match[2]}.jsonl`;
      const raw = this.retained.get(rawName);
      if (!raw || !complete.has(rawName) || hash(raw.bytes) !== match[3]) throw unavailable("Missing or incomplete committed recovery journal range");
      witnessed.add(rawName);
    }
    const sorted = [...complete.entries()].sort((a, b) => a[1].seq - b[1].seq);
    for (let index = 0; index < sorted.length; index++) if (sorted[index]![1].seq !== index + 1) throw unavailable("Incomplete recovery journal sequence history");
    // Reflush every retained byte, including interrupted tails, before recovering authority.
    for (const [name, retained] of this.retained) barrier(path.join(this.directory, name), retained);
    for (const [name] of sorted) if (!witnessed.has(name)) this.witness(name);
    this.nextSeq = sorted.length + 1;
    this.assertAvailable();
  }
  private witness(rawName: string): void {
    const match = RAW.exec(rawName)!;
    const name = `commit-${match[1]}-${match[2]}-${hash(this.retained.get(rawName)!.bytes)}`;
    this.retained.set(name, createRetained(path.join(this.directory, name), new Uint8Array()));
  }
  append(key: string, bytes: Uint8Array, options: RecoveryJournalAppendOptions): Promise<{ seq: number; existing: boolean }> {
    // Snapshot BEFORE the first await/queue turn, including a Buffer's shared backing view.
    const snapshot = Uint8Array.from(bytes);
    const bounds = { deadlineMs: options.deadlineMs, ...(options.signal ? { signal: options.signal } : {}) };
    const operation = this.queue.then(() => {
      checked(bounds); this.assertAvailable();
      if (!key || typeof key !== "string") throw new RecoveryJournalError("superseded", "Recovery journal requires a nonempty key");
      const existing = this.records.get(key);
      if (existing) {
        if (!Buffer.from(existing.bytes).equals(snapshot)) throw new RecoveryJournalError("superseded", "Recovery journal key is already bound to different bytes");
        checked(bounds); return { seq: existing.seq, existing: true };
      }
      if (!Number.isSafeInteger(this.nextSeq) || this.nextSeq > 9_999_999_999_999_999) throw unavailable("Recovery journal sequence exhausted");
      const frame = { version: 1 as const, storeId: this.storeId, seq: this.nextSeq, key, bytes: Buffer.from(snapshot).toString("base64") };
      const raw = Buffer.from(`${JSON.stringify({ ...frame, digest: digestFrame(frame) })}\n`);
      const name = `raw-${String(frame.seq).padStart(16, "0")}-${randomUUID()}.jsonl`;
      try {
        this.retained.set(name, createRetained(path.join(this.directory, name), raw));
        this.witness(name);
        this.records.set(key, { seq: frame.seq, key, bytes: snapshot }); this.nextSeq++;
        this.assertAvailable();
      } catch (error) { this.failed = unavailable("Recovery append could not confirm durable commit; reopen required", error); throw this.failed; }
      // A late/aborted append may have committed. Never erase it or allocate another seq.
      checked(bounds); return { seq: frame.seq, existing: false };
    });
    this.queue = operation.catch(() => undefined);
    return operation;
  }
  get(key: string): RecoveryJournalRecord | undefined { this.assertAvailable(); const record = this.records.get(key); return record ? copy(record) : undefined; }
  entries(): readonly RecoveryJournalRecord[] { this.assertAvailable(); return [...this.records.values()].sort((a, b) => a.seq - b.seq).map(copy); }
  async close(): Promise<void> { await this.queue; if (!this.closed) { this.closed = true; this.release(); } }
}

export async function openRecoveryJournal(options: RecoveryJournalOptions): Promise<RecoveryJournal> {
  options = Object.freeze({ ...options });
  let release: (() => void) | undefined;
  try {
    if (!options.storeId || typeof options.storeId !== "string" || !options.directory) throw unavailable("Recovery journal requires an exact store identity and directory");
    const directory = path.resolve(options.directory);
    const exists = fs.existsSync(directory);
    if (options.required && (!exists || !fs.existsSync(path.join(directory, IDENTITY)))) throw unavailable("Required committed recovery journal is missing");
    if (exists && (!fs.lstatSync(directory).isDirectory() || fs.lstatSync(directory).isSymbolicLink())) throw unavailable("Recovery journal directory is not a regular directory");
    fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
    // Protocol 3 refuses a foreign/unknown PID namespace and does not infer exclusion from
    // a different boot. Native exclusion/reclaim of a foreign lock is a separate owner port.
    release = await acquireMeshCustodyLock(directory, 0, { hostQualified: true, lockProtocol: 3 });
    const identity = path.join(directory, IDENTITY);
    if (!fs.existsSync(identity)) {
      if (options.required || fs.readdirSync(directory).some(name => name.startsWith("raw-") || name.startsWith("commit-"))) throw unavailable("Recovery journal identity is missing from retained evidence");
      createRetained(identity, identityBytes(options.storeId));
    }
    const journal = new FileRecoveryJournal(directory, options.storeId, release);
    journal.recover();
    return journal;
  } catch (error) {
    release?.();
    if (error instanceof RecoveryJournalError) throw error;
    throw unavailable("Recovery journal open/recovery failed", error);
  }
}
