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
// Load: a final line without its newline, or a final line that fails its checksum, is an unfinished
// append and is cut off. A bad line followed by good ones is corruption: the ledger refuses to open
// (exactly-once cannot be claimed over a damaged history).
// The directory is 0700 and the file 0600: the ledger holds command bodies and ACK results.
// One writer: a lock file pins the owning process (pid + process incarnation + machine + boot).
import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import {
  ownProcessIncarnation,
  processIncarnation,
  readPhysicalHostIdentity,
  validProcessIncarnation,
} from "../core/atomic-write.js";
import {
  BRIDGE_NATS_HORIZON_MS,
  BRIDGE_NATS_OPERATIONS,
  type BridgeNatsAckRecord,
  type BridgeNatsClaimInput,
  type BridgeNatsClaimResult,
  type BridgeNatsCommandRecord,
  type BridgeNatsCommandState,
  type BridgeNatsDirection,
  type BridgeNatsLedger,
  type BridgeNatsSeenRecord,
} from "./bridge-nats-types.js";

export const BRIDGE_NATS_LEDGER_FILE = "ledger.jsonl";
export const BRIDGE_NATS_LEDGER_LOCK = "ledger.lock";
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

interface LockRecord { v: 1; pid: number; incarnation?: string; machineId?: string; bootId?: string; acquiredAt: number }

const pidAlive = (pid: number): boolean => {
  try { process.kill(pid, 0); return true; } catch (error) { return (error as NodeJS.ErrnoException).code === "EPERM"; }
};

/** Whether a recorded holder is provably gone: same machine and boot, and its pid dead or reused. */
const holderGone = async (holder: unknown): Promise<boolean> => {
  if (!isObject(holder) || holder.v !== 1 || !Number.isSafeInteger(holder.pid) || (holder.pid as number) <= 0) return false;
  const host = readPhysicalHostIdentity();
  if (!host || holder.machineId !== host.machineId) return false; // foreign or unprovable: never steal
  if (holder.bootId !== host.bootId) return typeof holder.bootId === "string"; // the machine rebooted since
  if (!pidAlive(holder.pid as number)) return true;
  if (typeof holder.incarnation !== "string" || !validProcessIncarnation(holder.incarnation)) return false;
  const current = await processIncarnation(holder.pid as number, 2_000);
  return validProcessIncarnation(current) && current !== holder.incarnation;
};

const fsyncDir = (dir: string): void => {
  let fd: number | undefined;
  try { fd = fs.openSync(dir, "r"); fs.fsyncSync(fd); } catch { /* not supported on this platform */ } finally {
    if (fd !== undefined) fs.closeSync(fd);
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

/** Open the ledger: take the writer lock, replay the file, cut an unfinished final append, compact. */
export const openBridgeNatsLedger = async (options: BridgeNatsLedgerOptions): Promise<FileBridgeNatsLedger> => {
  const dir = path.resolve(options.dir);
  fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
  const dirStat = fs.lstatSync(dir);
  const unsafeDir = unsafeMode(dirStat, "dir");
  if (unsafeDir) throw new BridgeNatsLedgerError("BRIDGE_NATS_LEDGER_UNSAFE", `ledger directory ${dir} ${unsafeDir}`);
  const lock = path.join(dir, BRIDGE_NATS_LEDGER_LOCK);
  const host = readPhysicalHostIdentity();
  const record: LockRecord = { v: 1, pid: process.pid, acquiredAt: (options.clock ?? Date.now)() };
  const incarnation = await ownProcessIncarnation();
  if (incarnation !== undefined && validProcessIncarnation(incarnation)) record.incarnation = incarnation;
  if (host) { record.machineId = host.machineId; record.bootId = host.bootId; }
  const body = `${JSON.stringify(record)}\n`;
  for (let attempt = 0; ; attempt++) {
    try {
      const fd = fs.openSync(lock, "wx", 0o600);
      try { fs.writeFileSync(fd, body); fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
      fsyncDir(dir);
      break;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "EEXIST" || attempt > 0) {
        if ((error as NodeJS.ErrnoException).code === "EEXIST") {
          throw new BridgeNatsLedgerError("BRIDGE_NATS_LEDGER_LOCKED", `ledger ${dir} is held by another process`);
        }
        throw error;
      }
      let raw = "", holder: unknown;
      try { raw = fs.readFileSync(lock, "utf8"); holder = JSON.parse(raw); } catch { holder = undefined; }
      if (!await holderGone(holder)) {
        throw new BridgeNatsLedgerError("BRIDGE_NATS_LEDGER_LOCKED", `ledger ${dir} is held by another live or unprovable process`);
      }
      // Remove only the exact stale holder judged above; a racing taker that already replaced it wins.
      if (fs.readFileSync(lock, "utf8") !== raw) {
        throw new BridgeNatsLedgerError("BRIDGE_NATS_LEDGER_LOCKED", `ledger ${dir} lock changed while recovering it`);
      }
      fs.unlinkSync(lock);
    }
  }
  try {
    const ledger = new FileBridgeNatsLedger(dir, lock, body, options);
    ledger.load();
    return ledger;
  } catch (error) {
    try { if (fs.readFileSync(lock, "utf8") === body) fs.unlinkSync(lock); } catch { /* best effort */ }
    throw error;
  }
};

export class FileBridgeNatsLedger implements BridgeNatsLedger {
  readonly #dir: string;
  readonly #file: string;
  readonly #lock: string;
  readonly #lockBody: string;
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

  constructor(dir: string, lock: string, lockBody: string, options: BridgeNatsLedgerOptions) {
    this.#dir = dir;
    this.#file = path.join(dir, BRIDGE_NATS_LEDGER_FILE);
    this.#lock = lock;
    this.#lockBody = lockBody;
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
      let offset = 0, good = 0, badAt: number | undefined;
      const lines: Line[] = [];
      while (offset < data.length) {
        const end = data.indexOf(0x0a, offset);
        if (end < 0) break; // unfinished final append: cut below
        const line = parseLine(data.subarray(offset, end).toString("utf8"));
        if (!line) {
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
      let written = 0;
      while (written < bytes.length) written += fs.writeSync(fd, bytes, written, bytes.length - written);
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

  #compactIfDue(force: boolean): void {
    if (!force && this.#lines <= this.#minCompactLines) return;
    const live = [...this.#commands.values()].filter(record => this.#live(record)).length +
      [...this.#seen.values()].filter(record => this.#live(record)).length;
    const expired = this.#commands.size + this.#seen.size - live;
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
        for (const line of lines) size += fs.writeSync(fd, encode(line));
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
      if (this.#fd !== undefined) { try { fs.closeSync(this.#fd); } finally { this.#fd = undefined; } }
      try { if (fs.readFileSync(this.#lock, "utf8") === this.#lockBody) fs.unlinkSync(this.#lock); } catch { /* already gone */ }
    });
    this.#queue = run.catch(() => undefined);
    return run;
  }
}
