// src/mesh/bridge-nats-ledger.ts: the durable ACK/dedupe/correlation ledger of A', the NATS <->
// file-mesh bridge (smarty-dev#15310, lane L3, C6). Contract: BridgeNatsLedger in bridge-nats-types.ts.
//
// Storage: one append-only JSONL file in the link's private ledger directory. Every line is a whole
// record (a command mapping or a seen event) with a checksum; replay keeps the last line per key.
// Every mutation appends one line and fsyncs it BEFORE the in-memory view changes and before its
// promise resolves, so anything a caller acted on is on disk. A crash can only lose a mutation whose
// promise never resolved, and every caller treats an unresolved mutation as not done:
//  - a lost claim was never published (the caller publishes only after claim() resolves);
//  - a lost markSent leaves the claim pending: resend with the same envelope id and bytes, which the
//    destination dedupes (Nats-Msg-Id inside its window, then the receiver's seen claim by commandId);
//  - a lost settle leaves the source delivery unacknowledged, so the ACK is received again.
// Load: ONE final line without its newline, or ONE final line that fails its checksum, is an unfinished
// append and is cut off. Anything more (a bad line followed by good ones, or two damaged appends) is
// corruption: the ledger refuses to open (exactly-once cannot be claimed over a damaged history).
// Directory entries are made durable (parent fsyncs); a failed directory fsync is an error.
// The directory is 0700 and the file 0600: the ledger holds command bodies and ACK results.
// One writer: the mesh's host-qualified custody lock (pid + process incarnation + machine + boot).
import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { syncPathNamespaceDurable } from "../core/atomic-write.js";
import {
  checkNativeIngressDirectory, encodeNativeIngressReceipt, nativeIngressReceiptForClaim, readNativeIngressReceipt,
  NATIVE_INGRESS_DIRECTORY, NATIVE_INGRESS_MAX_BYTES,
} from "./bridge-nats-ingress.js";
import { acquireMeshCustodyLock, MESH_CUSTODY_LOCK_NAME } from "./custody-lock.js";
import {
  BRIDGE_NATS_HORIZON_MS,
  BRIDGE_NATS_OPERATIONS,
  BRIDGE_NATS_REFUSAL,
  BRIDGE_NATS_SESSION_ID,
  type BridgeNatsAckRecord,
  type BridgeNatsClaimAction,
  type BridgeNatsClaimInput,
  type BridgeNatsClaimResult,
  type BridgeNatsCommandRecord,
  type BridgeNatsCommandState,
  type BridgeNatsDirection,
  type BridgeNatsLedger,
  type BridgeNatsSeenRecord,
} from "./bridge-nats-types.js";

export const BRIDGE_NATS_LEDGER_FILE = "ledger.jsonl";
/** The writer lock: a custody-lock directory inside the ledger directory. */
export const BRIDGE_NATS_LEDGER_LOCK = MESH_CUSTODY_LOCK_NAME;
/** One record line at most (a resolved command body is bounded by the 1 MiB NATS payload). */
export const BRIDGE_NATS_LEDGER_MAX_LINE_BYTES = 4 * 1024 * 1024;
/** Live records at most; past it a new claim is refused (never silently dropped). */
export const BRIDGE_NATS_LEDGER_MAX_RECORDS = 1_000_000;

export class BridgeNatsLedgerError extends Error {
  constructor(readonly code: "BRIDGE_NATS_LEDGER_CORRUPT" | "BRIDGE_NATS_LEDGER_LOCKED" | "BRIDGE_NATS_LEDGER_UNSAFE" |
    "BRIDGE_NATS_LEDGER_INVALID" | "BRIDGE_NATS_LEDGER_FULL" | "BRIDGE_NATS_LEDGER_CLOSED" | "BRIDGE_NATS_LEDGER_UNKNOWN", message: string) {
    super(message);
    this.name = "BridgeNatsLedgerError";
  }
}

export interface BridgeNatsLedgerOptions {
  /** The link's ledger directory (L5: natsLinkStatePaths(config).ledger). Created 0700 when absent. */
  readonly dir: string;
  readonly clock?: () => number;
  /** Compact when the file holds more than max(minCompactLines, 4 x live records) lines. */
  readonly minCompactLines?: number;
  /** How long to wait for the writer lock before refusing (default 2 s); a live holder is never displaced. */
  readonly lockTimeoutMs?: number;
}

type Line =
  | { readonly t: "cmd"; readonly r: BridgeNatsCommandRecord }
  | { readonly t: "seen"; readonly r: BridgeNatsSeenRecord };

const COMMAND_ID = /^[0-9a-f]{64}$/;
const DIGEST = /^[0-9a-f]{64}$/;
const MACHINE = /^[a-z0-9][a-z0-9-]{0,31}$/;
const STATES: readonly BridgeNatsCommandState[] = ["claimed", "uncertain", "sent", "acked", "ackForwarded"];

const isObject = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);
const text = (value: unknown, max = 512): value is string =>
  typeof value === "string" && value.length > 0 && value.length <= max && !/[\u0000-\u001f\u007f]/.test(value);
const finite = (value: unknown): value is number => typeof value === "number" && Number.isFinite(value);
const direction = (value: unknown): value is BridgeNatsDirection => value === "toNats" || value === "toFile";

/** Key-sorted JSON: the checksum and the "same ACK" test do not depend on property order. */
const canonical = (value: unknown): string => JSON.stringify(value, (_key, child: unknown) =>
  isObject(child) ? Object.fromEntries(Object.keys(child).sort().map(key => [key, child[key]])) : child) ?? "null";

const checksum = (line: Line): string => createHash("sha256").update(canonical(line), "utf8").digest("hex").slice(0, 32);
const seenKey = (dir: BridgeNatsDirection, eventId: string): string => JSON.stringify([dir, eventId]);
const originalKey = (dir: BridgeNatsDirection, requesterId: string, original: string): string =>
  JSON.stringify([dir, requesterId, original]);

const freeze = <T>(value: T): T => {
  if (typeof value === "object" && value !== null && !Object.isFrozen(value)) {
    Object.freeze(value);
    for (const child of Object.values(value)) freeze(child);
  }
  return value;
};
const cloneJson = <T>(value: T): T => JSON.parse(JSON.stringify(value)) as T;

/** Shape check of a command record read back from disk or built from a claim. */
const validCommand = (r: unknown): r is BridgeNatsCommandRecord => {
  if (!isObject(r) || r.v !== 1 || !STATES.includes(r.state as BridgeNatsCommandState) || !direction(r.direction)) return false;
  if (!COMMAND_ID.test(String(r.commandId)) || !text(r.originalCommandId) || !text(r.sourceEventId) || !DIGEST.test(String(r.bodyDigest)) ||
      !text(r.sourceMachine, 64) || !text(r.requesterId) || !text(r.replyTo) || !text(r.targetId) ||
      !(BRIDGE_NATS_OPERATIONS as readonly unknown[]).includes(r.operation) || !text(r.ownerHostId) || !text(r.ownerIdentityId) ||
      !finite(r.deadlineAt) || !finite(r.claimedAt) || !finite(r.expiresAt)) return false;
  if (!validIdentity(r.from) || (r.principal !== undefined && !validPrincipal(r.principal))) return false;
  if (r.origin !== "control" && r.origin !== "topic") return false;
  if (r.origin === "topic" && (r.operation !== "followUp" || r.direction !== "toNats")) return false;
  if (r.envelopeId !== undefined && !text(r.envelopeId)) return false;
  if (r.direction === "toNats" && (r.envelopeId === undefined || !isObject(r.resolvedBody) || !text(r.targetMachine, 64))) return false;
  if (r.ownerIncarnation !== undefined && !text(r.ownerIncarnation, 128)) return false;
  if (r.targetMachine !== undefined && !MACHINE.test(String(r.targetMachine))) return false;
  if ((r.operation === "cancel") !== (r.cancelCommandId !== undefined) || (r.cancelCommandId !== undefined &&
      (!COMMAND_ID.test(String(r.cancelCommandId)) || !text(r.originalCancelCommandId)))) return false;
  if (r.resolvedBody !== undefined && !isObject(r.resolvedBody)) return false;
  if (r.destinationSequence !== undefined && !(Number.isSafeInteger(r.destinationSequence) && (r.destinationSequence as number) >= 0)) return false;
  if ((r.state === "acked" || r.state === "ackForwarded") !== (r.ack !== undefined)) return false;
  if (r.ack !== undefined && !validAck(r.ack, r.commandId as string)) return false;
  if (r.direction === "toFile" && r.state === "sent" && r.envelopeId === undefined) return false;
  return true;
};

function validIdentity(value: unknown): boolean {
  return isObject(value) && text(value.id) && typeof value.name === "string" && value.name.length <= 512 &&
    (value.kind === "main" || value.kind === "actor" || value.kind === "agent") &&
    (value.sessionId === undefined || text(value.sessionId)) &&
    Object.keys(value).every(key => key === "id" || key === "name" || key === "kind" || key === "sessionId");
}

function validPrincipal(value: unknown): boolean {
  return isObject(value) && text(value.id, 256) && (value.binding === "herdr-client" || value.binding === "voice-call" ||
    value.binding === "org-agent") && Object.keys(value).every(key => key === "id" || key === "binding");
}

const validAck = (ack: unknown, commandId: string): ack is BridgeNatsAckRecord =>
  isObject(ack) && ack.v === 1 && ack.commandId === commandId && typeof ack.accepted === "boolean" && text(ack.fromId) &&
  text(ack.ackEventId) && isObject(ack.data) && finite(ack.receivedAt);

const validSeen = (r: unknown): r is BridgeNatsSeenRecord =>
  isObject(r) && r.v === 1 && text(r.eventId) && direction(r.direction) && finite(r.seenAt) && finite(r.expiresAt) &&
  (r.destinationId === undefined || text(r.destinationId)) &&
  (r.destinationSequence === undefined || (Number.isSafeInteger(r.destinationSequence) && (r.destinationSequence as number) >= 0));

const parseLine = (raw: string): Line | undefined => {
  let value: unknown;
  try { value = JSON.parse(raw); } catch { return undefined; }
  if (!isObject(value) || typeof value.h !== "string") return undefined;
  const line = value.t === "cmd" ? { t: "cmd" as const, r: value.r } : value.t === "seen" ? { t: "seen" as const, r: value.r } : undefined;
  if (!line || checksum(line as Line) !== value.h) return undefined;
  if (line.t === "cmd" ? !validCommand(line.r) : !validSeen(line.r)) return undefined;
  return freeze(line as Line);
};

const encode = (line: Line): string => {
  const encoded = `${JSON.stringify({ t: line.t, r: line.r, h: checksum(line) })}\n`;
  if (Buffer.byteLength(encoded, "utf8") > BRIDGE_NATS_LEDGER_MAX_LINE_BYTES) {
    throw new BridgeNatsLedgerError("BRIDGE_NATS_LEDGER_INVALID", "ledger record is larger than the line bound");
  }
  return encoded;
};

/**
 * The one decision for a claim result, never a wait (integrator note 8 item 3; E2E finding A: a resend of
 * an already-ACKed command must replay the recorded ACK at once, never block the link).
 */
export const bridgeNatsClaimAction = (result: BridgeNatsClaimResult): BridgeNatsClaimAction => {
  const { record } = result;
  if (result.status === "new") return { action: "publish", record };
  if (result.status === "conflict") return { action: "refuse", record, refuse: BRIDGE_NATS_REFUSAL.idConflict };
  if (result.status === "mismatch") return { action: "refuse", record, refuse: BRIDGE_NATS_REFUSAL.claimMismatch };
  if (record.ack !== undefined) return { action: "replayAck", record, ack: record.ack };
  return { action: record.state === "sent" ? "awaitAck" : "resend", record };
};

/** Write every byte (a regular-file write may return a short count) or throw. */
const writeAll = (fd: number, bytes: Buffer): void => {
  let written = 0;
  while (written < bytes.length) written += fs.writeSync(fd, bytes, written, bytes.length - written);
};

/**
 * A namespace barrier: fsync a directory so a created or renamed entry in it is durable. A real I/O
 * error is never mistaken for success. Only Windows (directories cannot be opened for fsync) skips it,
 * and only a non-Linux filesystem that reports the operation unsupported (EINVAL, ENOTSUP) passes.
 */
const fsyncDir = (dir: string): void => {
  if (process.platform === "win32") return;
  const fd = fs.openSync(dir, "r");
  try { fs.fsyncSync(fd); } catch (error) {
    const code = (error as NodeJS.ErrnoException).code;
    if (process.platform === "linux" || (code !== "EINVAL" && code !== "ENOTSUP")) throw error;
  } finally { fs.closeSync(fd); }
};

/** Create `dir` (0700) and make every newly created component durable in its parent. */
const makeDurableDir = (dir: string): void => {
  const target = path.resolve(dir);
  const first = fs.mkdirSync(target, { recursive: true, mode: 0o700 });
  if (first === undefined) return;
  let created = path.resolve(first);
  const rest = path.relative(created, target).split(path.sep).filter(Boolean);
  fsyncDir(path.dirname(created));
  for (const part of rest) {
    fsyncDir(created);
    created = path.join(created, part);
  }
};

const unsafeMode = (stat: fs.Stats, expected: "dir" | "file"): string | undefined => {
  const uid = process.geteuid?.();
  if (expected === "dir" ? !stat.isDirectory() : !stat.isFile()) return `is not a ${expected === "dir" ? "directory" : "regular file"}`;
  if (uid === undefined) return undefined;
  if (stat.uid !== uid) return "is owned by another user";
  if ((stat.mode & 0o077) !== 0) return "is readable or writable by group or others";
  return undefined;
};

/**
 * Open the ledger: take the writer lock, replay the file, cut an unfinished final append, compact.
 * The writer lock is the mesh's host-qualified custody lock (src/mesh/custody-lock.ts) inside the ledger
 * directory: a dead holder (same machine, dead or reused pid, or a previous boot) is recovered by renaming
 * its lock to a name derived from the judged lock itself, so two recoverers can never displace a live
 * successor; a live, foreign or unprovable holder is never displaced.
 */
export const openBridgeNatsLedger = async (options: BridgeNatsLedgerOptions): Promise<FileBridgeNatsLedger> => {
  const dir = path.resolve(options.dir);
  makeDurableDir(dir);
  const dirStat = fs.lstatSync(dir);
  const unsafeDir = unsafeMode(dirStat, "dir");
  if (unsafeDir) throw new BridgeNatsLedgerError("BRIDGE_NATS_LEDGER_UNSAFE", `ledger directory ${dir} ${unsafeDir}`);
  let release: () => void;
  try {
    release = await acquireMeshCustodyLock(dir, options.lockTimeoutMs ?? 2_000, { hostQualified: true });
  } catch (error) {
    throw Object.assign(new BridgeNatsLedgerError("BRIDGE_NATS_LEDGER_LOCKED",
      `ledger ${dir} is held by another live, foreign or unprovable process`), { cause: error });
  }
  try {
    const ledger = new FileBridgeNatsLedger(dir, release, options);
    ledger.load();
    return ledger;
  } catch (error) {
    release();
    throw error;
  }
};

export class FileBridgeNatsLedger implements BridgeNatsLedger {
  readonly #dir: string;
  readonly #file: string;
  readonly #release: () => void;
  readonly #clock: () => number;
  readonly #minCompactLines: number;
  readonly #commands = new Map<string, BridgeNatsCommandRecord>();
  readonly #originals = new Map<string, string>();
  readonly #seen = new Map<string, BridgeNatsSeenRecord>();
  #fd: number | undefined;
  #lines = 0;
  /** Bytes known durable in the file: an append that fails is cut back to here. */
  #size = 0;
  #queue: Promise<unknown> = Promise.resolve();
  #closed = false;
  /** Fail-stop: after a write/fsync failure nothing more is appended; reopen (restart) recovers. */
  #failed: unknown;

  constructor(dir: string, release: () => void, options: BridgeNatsLedgerOptions) {
    this.#dir = dir;
    this.#file = path.join(dir, BRIDGE_NATS_LEDGER_FILE);
    this.#release = release;
    this.#clock = options.clock ?? Date.now;
    this.#minCompactLines = Math.max(1, options.minCompactLines ?? 1024);
  }

  /** Replay the file (called once by openBridgeNatsLedger, under the lock). */
  load(): void {
    for (const name of fs.readdirSync(this.#dir)) {
      // A compaction that crashed before its rename left only a temporary file: the old file is intact.
      if (name.startsWith(`${BRIDGE_NATS_LEDGER_FILE}.compact-`)) fs.rmSync(path.join(this.#dir, name), { force: true });
    }
    const existing = fs.lstatSync(this.#file, { throwIfNoEntry: false });
    if (existing) {
      const unsafe = unsafeMode(existing, "file");
      if (unsafe) throw new BridgeNatsLedgerError("BRIDGE_NATS_LEDGER_UNSAFE", `ledger file ${this.#file} ${unsafe}`);
      const data = fs.readFileSync(this.#file);
      let offset = 0, good = 0, bad = 0, badAt: number | undefined;
      const lines: Line[] = [];
      while (offset < data.length) {
        const end = data.indexOf(0x0a, offset);
        if (end < 0) break; // an unfinished final append (no newline): cut below
        const line = parseLine(data.subarray(offset, end).toString("utf8"));
        if (!line) {
          bad += 1;
          badAt ??= offset;
        } else {
          if (badAt !== undefined) {
            throw new BridgeNatsLedgerError("BRIDGE_NATS_LEDGER_CORRUPT",
              `ledger ${this.#file} has a damaged record at byte ${badAt} before valid records; refusing to open`);
          }
          lines.push(line);
          good = end + 1;
        }
        offset = end + 1;
      }
      // Appends are sequential and each one's fsync returns before the next begins (a failure stops all
      // appends), so at most ONE append can be unfinished: either one damaged complete line or one
      // segment without its newline, never more. A longer damaged suffix is corruption, not a crash.
      if (bad > 1 || (bad === 1 && offset < data.length)) {
        throw new BridgeNatsLedgerError("BRIDGE_NATS_LEDGER_CORRUPT",
          `ledger ${this.#file} ends in more than one damaged record (from byte ${badAt}); refusing to open`);
      }
      if (good < data.length) {
        // Only the unacknowledged tail: an append whose fsync never returned to its caller.
        fs.truncateSync(this.#file, good);
        const fd = fs.openSync(this.#file, "r+");
        try { fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
      }
      for (const line of lines) this.#apply(line);
      this.#lines = lines.length;
    }
    this.#fd = fs.openSync(this.#file, "a", 0o600);
    fs.fchmodSync(this.#fd, 0o600);
    this.#size = fs.fstatSync(this.#fd).size;
    fsyncDir(this.#dir);
    this.#compactIfDue(true);
    if (this.#failed !== undefined) throw this.#failed;
  }

  #apply(line: Line): void {
    if (line.t === "cmd") {
      this.#commands.set(line.r.commandId, line.r);
      this.#originals.set(originalKey(line.r.direction, line.r.requesterId, line.r.originalCommandId), line.r.commandId);
    } else {
      this.#seen.set(seenKey(line.r.direction, line.r.eventId), line.r);
    }
  }

  #live(record: { readonly expiresAt: number }): boolean {
    return record.expiresAt >= this.#clock();
  }

  /** Durable first, then visible. Serialized: the decision and the write happen in one critical section. */
  #mutate<T>(work: () => T): Promise<T> {
    const run = this.#queue.then(() => {
      if (this.#closed) throw new BridgeNatsLedgerError("BRIDGE_NATS_LEDGER_CLOSED", "ledger is closed");
      if (this.#failed !== undefined) {
        throw new BridgeNatsLedgerError("BRIDGE_NATS_LEDGER_CLOSED", "ledger stopped after a write failure; restart the link");
      }
      return work();
    });
    this.#queue = run.catch(() => undefined);
    return run;
  }

  #append(line: Line): void {
    const encoded = encode(line); // throws before any byte is written
    const fd = this.#fd;
    if (fd === undefined) throw new BridgeNatsLedgerError("BRIDGE_NATS_LEDGER_CLOSED", "ledger is closed");
    const bytes = Buffer.from(encoded, "utf8");
    try {
      // One append syscall for one fully encoded line. Never finish a short append with another
      // write: stop and roll it back so a crash can leave only ONE incomplete final record.
      // This is not a disk/power atomicity guarantee; load still recovers that single torn tail.
      const written = fs.writeSync(fd, bytes, 0, bytes.length);
      if (written !== bytes.length) throw new Error(`ledger append was short (${written}/${bytes.length} bytes); restart the link`);
      fs.fsyncSync(fd);
    } catch (error) {
      // Never leave a torn line in front of later ones (load would refuse it as corruption). After a
      // failed fsync the page state is unknown, so stop appending either way: a restart reloads the file.
      this.#failed = error;
      try { fs.ftruncateSync(fd, this.#size); fs.fsyncSync(fd); } catch { /* the reload cuts an unfinished tail */ }
      throw error;
    }
    this.#size += bytes.length;
    this.#apply(freeze(line));
    this.#lines += 1;
    this.#compactIfDue(false);
  }

  /** Receipt cleanup runs only under this ledger's existing custody/maintenance, never a timer. */
  #pruneNativeIngress(): void {
    const directory = path.join(this.#dir, NATIVE_INGRESS_DIRECTORY);
    if (!fs.existsSync(directory)) return;
    checkNativeIngressDirectory(this.#dir);
    let changed = false;
    for (const name of fs.readdirSync(directory)) {
      // A crashed exclusive writer may leave an unpublished staging inode. No live peer writes here.
      if (/^[0-9a-f]{64}\.\d+\.[0-9a-f-]{36}\.tmp$/.test(name)) {
        fs.unlinkSync(path.join(directory, name)); changed = true;
        continue;
      }
      if (!/^[0-9a-f]{64}\.json$/.test(name)) continue;
      const id = name.slice(0, -5), claim = this.#commands.get(id);
      if (claim && this.#live(claim)) continue; // never prune before the claim horizon
      try {
        const receipt = readNativeIngressReceipt(this.#dir, id);
        if (receipt.expiresAt < this.#clock()) { fs.unlinkSync(path.join(directory, name)); changed = true; }
      } catch { /* Unreadable/corrupt evidence denies admission; do not guess its retention horizon. */ }
    }
    if (changed) fsyncDir(directory);
  }

  #compactIfDue(force: boolean): void {
    if (!force && this.#lines <= this.#minCompactLines) return;
    const live = [...this.#commands.values()].filter(record => this.#live(record)).length +
      [...this.#seen.values()].filter(record => this.#live(record)).length;
    const expired = this.#commands.size + this.#seen.size - live;
    if (force) this.#pruneNativeIngress();
    if (!(force && expired > 0) && this.#lines <= Math.max(this.#minCompactLines, 4 * live)) return;
    const temp = path.join(this.#dir, `${BRIDGE_NATS_LEDGER_FILE}.compact-${process.pid}-${Date.now()}`);
    const lines: Line[] = [
      ...[...this.#commands.values()].filter(record => this.#live(record)).map(r => ({ t: "cmd" as const, r })),
      ...[...this.#seen.values()].filter(record => this.#live(record)).map(r => ({ t: "seen" as const, r })),
    ];
    let renamed = false;
    try {
      const fd = fs.openSync(temp, "wx", 0o600);
      let size = 0;
      try {
        // Every byte of every record, or no rename: a short write is completed, never taken as done.
        for (const line of lines) {
          const bytes = Buffer.from(encode(line), "utf8");
          writeAll(fd, bytes);
          size += bytes.length;
        }
        fs.fsyncSync(fd);
      } finally { fs.closeSync(fd); }
      fs.renameSync(temp, this.#file);
      renamed = true;
      fsyncDir(this.#dir);
      if (this.#fd !== undefined) fs.closeSync(this.#fd);
      this.#fd = undefined;
      this.#fd = fs.openSync(this.#file, "a", 0o600);
      this.#size = size;
      this.#commands.clear(); this.#originals.clear(); this.#seen.clear();
      for (const line of lines) this.#apply(line);
      this.#lines = lines.length;
      if (!force) this.#pruneNativeIngress();
    } catch (error) {
      // Before the rename the old file is intact: keep appending to it and retry later. After it, the
      // open descriptor no longer names the ledger file: stop (fail-stop) rather than append to a ghost.
      if (!renamed) { try { fs.rmSync(temp, { force: true }); } catch { /* removed at the next load */ } }
      else {
        this.#failed = error;
        if (this.#fd !== undefined) { try { fs.closeSync(this.#fd); } catch { /* closing */ } this.#fd = undefined; }
      }
    }
  }

  #validateClaim(input: BridgeNatsClaimInput): void {
    const invalid = (field: string): never => { throw new BridgeNatsLedgerError("BRIDGE_NATS_LEDGER_INVALID", `claim ${field} is invalid`); };
    if (!direction(input.direction)) invalid("direction");
    if (!COMMAND_ID.test(String(input.commandId))) invalid("commandId");
    if (!text(input.originalCommandId)) invalid("originalCommandId");
    if (!text(input.sourceEventId)) invalid("sourceEventId");
    if (!DIGEST.test(String(input.bodyDigest))) invalid("bodyDigest");
    if (!(BRIDGE_NATS_OPERATIONS as readonly unknown[]).includes(input.operation)) invalid("operation");
    if (input.origin !== "control" && input.origin !== "topic") invalid("origin");
    if (input.origin === "topic" && (input.operation !== "followUp" || input.direction !== "toNats")) {
      invalid("origin (a topic route is a followUp toward NATS)");
    }
    if (!validIdentity(input.from)) invalid("from");
    if (input.principal !== undefined && !validPrincipal(input.principal)) invalid("principal");
    if (input.resolvedBody !== undefined && !isObject(input.resolvedBody)) invalid("resolvedBody");
    if (input.direction === "toNats") {
      if (!text(input.envelopeId)) invalid("envelopeId (required toward NATS)");
      if (!isObject(input.resolvedBody)) invalid("resolvedBody (required toward NATS)");
      if (!MACHINE.test(String(input.targetMachine))) invalid("targetMachine (required toward NATS)");
    } else if (input.envelopeId !== undefined) {
      invalid("envelopeId (recorded by markSent toward the file mesh)");
    }
    if ((input.operation === "cancel") !== (input.cancelCommandId !== undefined)) invalid("cancelCommandId");
    if (input.cancelCommandId !== undefined && (!COMMAND_ID.test(input.cancelCommandId) || !text(input.originalCancelCommandId))) {
      invalid("cancelCommandId");
    }
    if (!finite(input.deadlineAt)) invalid("deadlineAt");
  }

  claim(input: BridgeNatsClaimInput): Promise<BridgeNatsClaimResult> {
    return this.#mutate(() => {
      this.#validateClaim(input);
      const existing = this.#commands.get(input.commandId);
      if (existing && this.#live(existing)) {
        if (existing.bodyDigest !== input.bodyDigest) return { status: "conflict" as const, created: false as const, record: existing };
        if (existing.direction !== input.direction || existing.sourceMachine !== input.sourceMachine ||
            existing.requesterId !== input.requesterId || existing.originalCommandId !== input.originalCommandId ||
            existing.replyTo !== input.replyTo || existing.targetId !== input.targetId || existing.operation !== input.operation) {
          return { status: "mismatch" as const, created: false as const, record: existing };
        }
        // The old wire digest intentionally omits route/deadline/timestamp fields. Retained toFile
        // evidence is stricter: a receipt repair/resend must reuse the complete FIRST forwarded body,
        // sender and principal. Never backfill a pre-upgrade claim lacking that historical evidence.
        if (existing.direction === "toFile" && existing.resolvedBody !== undefined &&
            (input.resolvedBody === undefined || canonical(existing.resolvedBody) !== canonical(input.resolvedBody) ||
             canonical(existing.principal ?? null) !== canonical(input.principal ?? null) ||
             existing.from.id !== input.from.id || existing.from.kind !== input.from.kind ||
             // Optional Main sid/name are not credentials; the verified canonical id fixes its sid.
             (!(existing.from.kind === "main" && BRIDGE_NATS_SESSION_ID.test(existing.from.id)) &&
               existing.from.sessionId !== input.from.sessionId) || existing.ownerHostId !== input.ownerHostId ||
             existing.ownerIdentityId !== input.ownerIdentityId || existing.ownerIncarnation !== input.ownerIncarnation ||
             existing.deadlineAt !== input.deadlineAt)) {
          return { status: "mismatch" as const, created: false as const, record: existing };
        }
        return { status: "duplicate" as const, created: false as const, record: existing };
      }
      const live = [...this.#commands.values()].filter(record => this.#live(record)).length;
      if (live >= BRIDGE_NATS_LEDGER_MAX_RECORDS) throw new BridgeNatsLedgerError("BRIDGE_NATS_LEDGER_FULL", "ledger holds its maximum of live commands");
      const claimedAt = this.#clock();
      const record = cloneJson({ ...input, v: 1 as const, state: "claimed" as const, claimedAt,
        expiresAt: Math.max(input.deadlineAt, claimedAt) + BRIDGE_NATS_HORIZON_MS }) as BridgeNatsCommandRecord;
      if (!validCommand(record)) throw new BridgeNatsLedgerError("BRIDGE_NATS_LEDGER_INVALID", "claim does not form a valid record");
      this.#append({ t: "cmd", r: record });
      return { status: "new" as const, created: true as const, record: this.#commands.get(record.commandId)! };
    });
  }

  /** INTERNAL sole-writer projection. Called only after a fresh verified delivery's durable claim.
   * No event/body path or public publication can invoke this through the mesh. An old claim with no
   * exact retained body remains unavailable to native consumers, never backfilled from current data.
   */
  projectNativeIngress(commandId: string, link: string): Promise<void> {
    return this.#mutate(() => {
      const record = this.#require(commandId);
      if (record.resolvedBody === undefined) return;
      const receipt = nativeIngressReceiptForClaim(record, link);
      const directory = checkNativeIngressDirectory(this.#dir, true);
      try { fs.mkdirSync(directory, { mode: 0o700 }); } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
      }
      checkNativeIngressDirectory(this.#dir);
      const file = path.join(directory, `${commandId}.json`);
      const notDurable = (): Error => new Error("Native ingress receipt namespace is not durable on this platform");
      const confirmExisting = (): void => {
        const existing = readNativeIngressReceipt(this.#dir, commandId);
        if (canonical(existing) !== canonical(receipt)) throw new BridgeNatsLedgerError("BRIDGE_NATS_LEDGER_CORRUPT",
          "native ingress receipt conflicts with retained claim; never overwritten");
        const fd = fs.openSync(file, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW | fs.constants.O_NONBLOCK);
        try { fs.fsyncSync(fd); syncPathNamespaceDurable(file, fs.fstatSync(fd), notDurable); }
        finally { fs.closeSync(fd); }
      };
      if (fs.lstatSync(file, { throwIfNoEntry: false })) { confirmExisting(); return; }
      const bytes = Buffer.from(encodeNativeIngressReceipt(receipt), "utf8");
      if (bytes.length > NATIVE_INGRESS_MAX_BYTES) throw new BridgeNatsLedgerError("BRIDGE_NATS_LEDGER_INVALID", "native receipt exceeds byte bound");
      const temporary = path.join(directory, `${commandId}.${process.pid}.${randomUUID()}.tmp`);
      const fd = fs.openSync(temporary, fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | fs.constants.O_NOFOLLOW, 0o600);
      try {
        fs.fchmodSync(fd, 0o600);
        writeAll(fd, bytes);
        fs.fsyncSync(fd);
        // Hard-link publication is atomic AND create-only. rename would overwrite a conflicting receipt.
        let published = false;
        try { fs.linkSync(temporary, file); published = true; } catch (error) {
          if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
          confirmExisting();
        }
        if (published) syncPathNamespaceDurable(file, fs.fstatSync(fd), notDurable);
      } finally {
        fs.closeSync(fd);
        fs.unlinkSync(temporary);
      }
      // Namespace barrier includes publication, staging unlink, and all newly created parents.
      syncPathNamespaceDurable(file, fs.lstatSync(file), notDurable);
    });
  }

  #get(commandId: string): BridgeNatsCommandRecord | undefined {
    const record = this.#commands.get(commandId);
    return record && this.#live(record) ? record : undefined;
  }

  lookup(dir: BridgeNatsDirection, commandId: string): BridgeNatsCommandRecord | undefined {
    const record = this.#get(commandId);
    return record?.direction === dir ? record : undefined;
  }

  lookupOriginal(dir: BridgeNatsDirection, requesterId: string, originalCommandId: string): BridgeNatsCommandRecord | undefined {
    const commandId = this.#originals.get(originalKey(dir, requesterId, originalCommandId));
    return commandId === undefined ? undefined : this.lookup(dir, commandId);
  }

  #require(commandId: string): BridgeNatsCommandRecord {
    const record = this.#get(commandId);
    if (!record) throw new BridgeNatsLedgerError("BRIDGE_NATS_LEDGER_UNKNOWN", "no live claim for that commandId");
    return record;
  }

  #update(record: BridgeNatsCommandRecord, changes: Partial<BridgeNatsCommandRecord>): BridgeNatsCommandRecord {
    const next = cloneJson({ ...record, ...changes }) as BridgeNatsCommandRecord;
    if (!validCommand(next)) throw new BridgeNatsLedgerError("BRIDGE_NATS_LEDGER_INVALID", "update does not form a valid record");
    this.#append({ t: "cmd", r: next });
    return this.#commands.get(next.commandId)!;
  }

  markUncertain(commandId: string): Promise<BridgeNatsCommandRecord> {
    return this.#mutate(() => {
      const record = this.#require(commandId);
      // A confirmed send or a recorded ACK is never downgraded by a late uncertain outcome.
      return record.state === "claimed" ? this.#update(record, { state: "uncertain" }) : record;
    });
  }

  markSent(commandId: string, receipt: { readonly envelopeId?: string; readonly sequence?: number }): Promise<BridgeNatsCommandRecord> {
    return this.#mutate(() => {
      const record = this.#require(commandId);
      if (receipt.envelopeId !== undefined && !text(receipt.envelopeId)) {
        throw new BridgeNatsLedgerError("BRIDGE_NATS_LEDGER_INVALID", "markSent envelopeId is invalid");
      }
      if (record.envelopeId !== undefined && receipt.envelopeId !== undefined && receipt.envelopeId !== record.envelopeId) {
        throw new BridgeNatsLedgerError("BRIDGE_NATS_LEDGER_INVALID", "markSent names a different destination id");
      }
      if (record.direction === "toFile" && record.envelopeId === undefined && receipt.envelopeId === undefined) {
        throw new BridgeNatsLedgerError("BRIDGE_NATS_LEDGER_INVALID", "markSent toward the file mesh needs the appended event id");
      }
      if (record.state !== "claimed" && record.state !== "uncertain") {
        if (record.envelopeId !== undefined || receipt.envelopeId === undefined) return record;
        // An ACK can arrive before the append receipt: keep its state, record the destination id once.
        return this.#update(record, { envelopeId: receipt.envelopeId });
      }
      return this.#update(record, { state: "sent",
        ...(record.envelopeId === undefined && receipt.envelopeId !== undefined ? { envelopeId: receipt.envelopeId } : {}),
        ...(receipt.sequence !== undefined ? { destinationSequence: receipt.sequence } : {}) });
    });
  }

  settle(commandId: string, ack: BridgeNatsAckRecord): Promise<BridgeNatsCommandRecord> {
    return this.#mutate(() => {
      const record = this.#require(commandId);
      if (!validAck(ack, commandId)) throw new BridgeNatsLedgerError("BRIDGE_NATS_LEDGER_INVALID", "settle ACK is invalid");
      // Exactly one owner outcome per command: the first recorded ACK stands; a replay or a different one changes nothing.
      if (record.ack !== undefined) return record;
      return this.#update(record, { state: "acked", ack: cloneJson(ack) });
    });
  }

  markAckForwarded(commandId: string): Promise<BridgeNatsCommandRecord> {
    return this.#mutate(() => {
      const record = this.#require(commandId);
      if (record.ack === undefined) throw new BridgeNatsLedgerError("BRIDGE_NATS_LEDGER_INVALID", "no ACK is recorded to forward");
      return record.state === "ackForwarded" ? record : this.#update(record, { state: "ackForwarded" });
    });
  }

  pendingResends(): readonly BridgeNatsCommandRecord[] {
    return [...this.#commands.values()].filter(record => this.#live(record) && (record.state === "claimed" || record.state === "uncertain"))
      .sort((a, b) => a.claimedAt - b.claimedAt);
  }

  pendingAcks(): readonly BridgeNatsCommandRecord[] {
    return [...this.#commands.values()].filter(record => this.#live(record) && record.state === "acked")
      .sort((a, b) => a.claimedAt - b.claimedAt);
  }

  seen(dir: BridgeNatsDirection, eventId: string): BridgeNatsSeenRecord | undefined {
    const record = this.#seen.get(seenKey(dir, eventId));
    return record && this.#live(record) ? record : undefined;
  }

  markSeen(dir: BridgeNatsDirection, eventId: string,
    receipt: { readonly destinationId?: string; readonly destinationSequence?: number } = {}): Promise<BridgeNatsSeenRecord> {
    return this.#mutate(() => {
      const existing = this.seen(dir, eventId);
      if (existing) return existing; // the first receipt stands
      const seenAt = this.#clock();
      const record = cloneJson({ v: 1 as const, eventId, direction: dir, seenAt, expiresAt: seenAt + BRIDGE_NATS_HORIZON_MS,
        ...(receipt.destinationId !== undefined ? { destinationId: receipt.destinationId } : {}),
        ...(receipt.destinationSequence !== undefined ? { destinationSequence: receipt.destinationSequence } : {}) });
      if (!validSeen(record)) throw new BridgeNatsLedgerError("BRIDGE_NATS_LEDGER_INVALID", "seen record is invalid");
      this.#append({ t: "seen", r: record });
      return this.#seen.get(seenKey(dir, eventId))!;
    });
  }

  /** Waits for queued mutations, closes the file and releases the writer lock. */
  close(): Promise<void> {
    // Also after a fail-stop: the writer lock is always released by its own holder.
    const run = this.#queue.then(() => {
      if (this.#closed) return;
      this.#closed = true;
      try {
        if (this.#fd !== undefined) { try { fs.closeSync(this.#fd); } finally { this.#fd = undefined; } }
      } finally { this.#release(); }
    });
    this.#queue = run.catch(() => undefined);
    return run;
  }
}
