import { copyFabricPrincipal, type FabricPrincipal } from "../fabric-provenance.js";
import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { writeFileAtomic, syncPathNamespace } from "../core/atomic-write.js";
import { readJsonlPage } from "../log-tail.js";
import { MeshArchive, MeshArchiveLookupUnavailableError, MeshArchiveRecoveryChanged, type MeshArchiveEntry, type MeshArchiveRecoveryPlan } from "./archive.js";
import { delay, errorCode, type MeshLock, type MeshStoreContext } from "./mesh-lock.js";
import { jsonClone } from "./state-file.js";

// Mesh events (smarty-dev#6477 L0): publish, publishBatch, dedupe receipts, the live log, its
// compaction, archive coupling and recovery, and cursor reads.

export interface MeshIdentity {
  id: string;
  name: string;
  kind: "main" | "actor" | "agent";
  sessionId?: string;
  /** Set only by the admitting mesh bridge, after its sender/ownership checks. */
  verified?: "bridge";
}

export interface MeshEvent {
  /** Runtime-captured originating principal; not an event.data field. */
  principal?: FabricPrincipal | undefined;
  /** Recorded at publication, never reconstructed from retained event payloads. */
  verification?: "mesh" | "bridge";
  id: string;
  sequence: number;
  /** Host-only once-publication identity; never accepted from the public mesh provider. */
  dedupeKey?: string;
  topic: string;
  kind: string;
  from: MeshIdentity;
  to?: string;
  text?: string;
  data?: unknown;
  createdAt: number;
}

export interface MeshPublishInput {
  topic: string;
  /** Host-only durable publication receipt; never accepted by the public provider. */
  dedupeKey?: string;
  /** Host-only durability fence; batches share this barrier across their prefix. */
  durable?: boolean;
  kind?: string;
  from: MeshIdentity;
  to?: string;
  text?: string;
  /** Checked under the lock before admission. */
  signal?: AbortSignal | undefined;
  /** Host-only relay metadata. */
  principal?: FabricPrincipal | undefined;
  /** A function receives commit time under the lock. */
  data?: unknown;
}

export interface MeshTailResult {
  events: MeshEvent[];
  nextOffset: number;
  /** The cursor just past each event, from the same read (so a reader can stop at any event). */
  cursors?: number[];
}

const TOPIC_PATTERN = /^[a-zA-Z0-9][a-zA-Z0-9._:/-]{0,127}$/;
const DEFAULT_MAX_EVENT_LOG_BYTES = 64 * 1024 * 1024;
const DEFAULT_RETAINED_EVENT_LOG_BYTES = 16 * 1024 * 1024;
const EVENT_READ_PAGE_BYTES = 4 * 1024 * 1024;
const EVENT_READ_CHUNK_BYTES = 64 * 1024;
// Line ends remembered from recent read({ after }) scans: enough for every reader near the log head.
const READ_HINT_LINES = 128;
const CURSOR_OFFSET_BASE = 2 ** 32;
/** A tail cursor's live-log generation: it changes when the log is rewritten. */
export const meshCursorGeneration = (cursor: number): number => Math.floor(cursor / CURSOR_OFFSET_BASE);
/** The cursor at the start of a generation's log. */
export const meshCursorAtStart = (generation: number): number => generation * CURSOR_OFFSET_BASE;

interface PreparedLiveCatchUp {
  dir: string;
  after: number;
  identity: string;
  entries: MeshArchiveEntry[];
}

const atomicWrite = (filePath: string, value: unknown, maxBytes = Number.POSITIVE_INFINITY): void => {
  // Compact: the file is rewritten under the mesh lock on every write, and indenting made it 22%
  // larger and slower to serialize (smarty-dev#2004).
  const serialized = JSON.stringify(value);
  if (Buffer.byteLength(serialized, "utf8") > maxBytes) {
    throw new Error(`Fabric mesh state exceeds ${maxBytes} bytes`);
  }
  writeFileAtomic(filePath, serialized);
};

interface MeshDedupeIntent {
  dedupeKey: string;
  reservedSequence: number;
  eventId: string;
  /** Byte offset captured before the live append; it makes recovery a direct read. */
  liveOffset: number;
  /** Captured before append: removing/changing archive configuration cannot authorize retry. */
  archiveDir?: string;
}

/** Durability work a single publish runs AFTER `.lock` is released and before it resolves
 * (smarty-dev#6477 E1). Set by the committed hold only; a failed attempt never sets it. */
interface AfterUnlock { finish?: (() => Promise<void> | void) | undefined }

// One live-log barrier per root, shared by every confirmation queued before it STARTS. A
// later append enqueues a new barrier, even while an earlier one is running (pi-fabric#550).
const eventBarriers = new Map<string, Promise<void>>();

export class MeshDedupeRecoveryError extends Error {
  readonly retryable = true;
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = "MeshDedupeRecoveryError";
  }
}

export interface EventLogOptions {
  maxEventLogBytes?: number;
  retainedEventLogBytes?: number;
}

export class EventLog {
  readonly root: string;
  readonly maxEventBytes: number;
  readonly maxReadEvents: number;
  readonly #lock: MeshLock;
  readonly #eventsPath: string;
  readonly #counterPath: string;
  readonly #generationPath: string;
  readonly #maxEventLogBytes: number;
  readonly #retainedEventLogBytes: number;
  /**
   * Line ends (sequence, offset) that recent read({ after }) scans passed, by rising sequence. A
   * read starts at the last one at or below its cursor. One remembered point was not enough:
   * several readers at one cursor (a host's lifecycle subscriptions after a new event) moved it
   * past each other, and all but the first scanned the whole log again (smarty-dev#557).
   */
  #readHints: {
    generation: number; inode: number;
    lines: Array<{ sequence: number; offset: number }>;
    /** LRU boundaries of readers paused behind the recent-line window (e.g. steer grace). */
    anchors: Map<number, { sequence: number; offset: number }>;
  } | undefined;
  #oldestLive: { identity: string; sequence: number | undefined } | undefined;
  #preparedLiveCatchUp: PreparedLiveCatchUp | undefined;

  constructor(context: MeshStoreContext, options: EventLogOptions) {
    const { root, maxEventBytes } = context;
    this.root = root;
    this.maxEventBytes = maxEventBytes;
    this.maxReadEvents = context.maxReadEvents;
    this.#lock = context.lock;
    this.#eventsPath = path.join(root, "events.jsonl");
    this.#counterPath = path.join(root, "sequence");
    this.#generationPath = path.join(root, "generation");
    this.#maxEventLogBytes = Math.min(
      CURSOR_OFFSET_BASE - 1,
      Math.max(maxEventBytes + 2, Math.floor(options.maxEventLogBytes ?? DEFAULT_MAX_EVENT_LOG_BYTES)),
    );
    this.#retainedEventLogBytes = Math.min(
      this.#maxEventLogBytes - 1,
      Math.max(
        maxEventBytes + 1,
        Math.floor(options.retainedEventLogBytes ?? DEFAULT_RETAINED_EVENT_LOG_BYTES),
      ),
    );
  }

  #dedupePath(dedupeKey: string, suffix: string): string {
    return path.join(this.root, "event-receipts", createHash("sha256").update(dedupeKey).digest("hex") + suffix);
  }

  #confirmEventFile(file: string): void {
    const fd = fs.openSync(file, process.platform === "win32" ? "r+" : "r");
    try { fs.fsyncSync(fd); syncPathNamespace(file, fs.fstatSync(fd)); } finally { fs.closeSync(fd); }
  }

  /** Fsync the live log and its namespace outside `.lock`. Group commit: callers whose append
   * completed before the barrier starts share one fsync (the jbd2 commit is the cost). */
  #confirmEventsAfterRelease(): Promise<void> {
    const file = this.#eventsPath;
    const queued = eventBarriers.get(file);
    if (queued) return queued;
    const barrier = new Promise<void>((resolve, reject) => {
      setImmediate(() => {
        eventBarriers.delete(file);
        try { this.#confirmEventFile(file); resolve(); }
        catch (error) { reject(error); }
      });
    });
    eventBarriers.set(file, barrier);
    return barrier;
  }

  /** After release: a no-archive keyed event's live barrier, then its durable receipt, then
   * the intent's removal. Until the receipt is durable the intent stays, so a crash (or a
   * concurrent same-key writer, or compaction) recovers exactly as from a death after the
   * live append; a second writer can only install the identical receipt. */
  #finishLiveReceipt(event: MeshEvent, intentPath: string, receiptPath: string): () => Promise<void> {
    return async () => {
      await this.#confirmEventsAfterRelease();
      writeFileAtomic(receiptPath, JSON.stringify(event), { durable: true });
      if (fs.existsSync(intentPath)) this.#removeDedupeIntent(intentPath);
    };
  }

  #readDedupeReceipt(dedupeKey: string, confirm = true): MeshEvent | undefined {
    const file = this.#dedupePath(dedupeKey, ".json");
    let text: string;
    try { text = fs.readFileSync(file, "utf8"); }
    catch (error) { if (errorCode(error) === "ENOENT") return undefined; throw error; }
    const event = JSON.parse(text) as MeshEvent;
    if (event.dedupeKey !== dedupeKey || typeof event.id !== "string" || !Number.isSafeInteger(event.sequence)) {
      throw new Error("Invalid event publication receipt");
    }
    // A visible rename whose final barrier failed is not yet a durable receipt.
    if (confirm) this.#confirmEventFile(file);
    return event;
  }

  #removeDedupeIntent(file: string): void {
    fs.rmSync(file, { force: true });
    syncPathNamespace(path.dirname(file));
  }

  /** Read the exact live line named by an intent; archive fallback is a separate direct lookup. */
  #readEventAtIntent(intent: MeshDedupeIntent): MeshEvent | undefined {
    let descriptor: number | undefined;
    try {
      descriptor = fs.openSync(this.#eventsPath, "r");
      const stat = fs.fstatSync(descriptor);
      if (intent.liveOffset >= stat.size) return undefined;
      const bytes = Buffer.allocUnsafe(Math.min(this.maxEventBytes + 1, stat.size - intent.liveOffset));
      const count = fs.readSync(descriptor, bytes, 0, bytes.length, intent.liveOffset);
      const newline = bytes.subarray(0, count).indexOf(0x0a);
      if (newline < 0) return undefined; // A partial append never committed.
      const event = JSON.parse(bytes.subarray(0, newline).toString("utf8")) as MeshEvent;
      return event.sequence === intent.reservedSequence && event.id === intent.eventId &&
        event.dedupeKey === intent.dedupeKey ? event : undefined;
    } catch (error) {
      if (errorCode(error) === "ENOENT" || error instanceof SyntaxError) return undefined;
      throw error;
    } finally {
      if (descriptor !== undefined) fs.closeSync(descriptor);
    }
  }

  #settleDedupeIntent(file: string, dedupeKey?: string, archive?: MeshArchive, after?: AfterUnlock): MeshEvent | undefined {
    let text: string;
    try { text = fs.readFileSync(file, "utf8"); }
    catch (error) { if (errorCode(error) === "ENOENT") return undefined; throw error; }
    const intent = JSON.parse(text) as MeshDedupeIntent;
    if (typeof intent.dedupeKey !== "string" || !intent.dedupeKey ||
        (dedupeKey !== undefined && intent.dedupeKey !== dedupeKey) ||
        !Number.isSafeInteger(intent.reservedSequence) || intent.reservedSequence < 1 ||
        typeof intent.eventId !== "string" || !intent.eventId ||
        !Number.isSafeInteger(intent.liveOffset) || intent.liveOffset < 0 ||
        (intent.archiveDir !== undefined && (typeof intent.archiveDir !== "string" || !path.isAbsolute(intent.archiveDir))) ||
        file !== this.#dedupePath(intent.dedupeKey, ".pending.json")) {
      throw new Error("Invalid event publication intent");
    }
    // A crash may also leave both the receipt and its intent. Never replace a receipt.
    const prior = this.#readDedupeReceipt(intent.dedupeKey, !after);
    if (prior && after) {
      // The original publisher installed the receipt after our first lookup missed (it writes
      // it after release). Confirm it and unlink the intent after release too: no fsync and
      // no namespace barrier under the lock, as on the no-archive off-lock path.
      const receiptPath = this.#dedupePath(intent.dedupeKey, ".json");
      after.finish = () => {
        this.#confirmEventFile(receiptPath);
        if (fs.existsSync(file)) this.#removeDedupeIntent(file);
      };
      return prior;
    }
    const live = prior ? undefined : this.#readEventAtIntent(intent);
    let event = prior ?? live;
    if (!event && intent.archiveDir !== undefined && archive?.dir !== intent.archiveDir) {
      throw new MeshDedupeRecoveryError(`Cannot recover dedupe intent ${intent.dedupeKey}: event archive configuration is unavailable`);
    }
    if (!event && !prior && archive) {
      let entry: (MeshArchiveEntry & { committed: boolean }) | undefined;
      try { entry = archive.lookupEntry(intent.reservedSequence); }
      catch (error) {
        if (error instanceof MeshArchiveLookupUnavailableError) {
          throw new MeshDedupeRecoveryError(`Cannot recover dedupe intent ${intent.dedupeKey}: event archive lookup is unavailable`, { cause: error });
        }
        throw error;
      }
      const archived = entry?.event;
      if (archived?.id === intent.eventId && archived.dedupeKey !== intent.dedupeKey) {
        throw new MeshDedupeRecoveryError(`Cannot recover dedupe intent ${intent.dedupeKey}: reserved archive key does not match`);
      }
      // A different archived identity positively proves this reservation is absent. Never
      // abort that other event; leave its archive visibility and index intact.
      if (entry && archived?.id === intent.eventId) {
        this.#repairEventLog();
        const lastLive = this.#readLastEventSequence();
        if (lastLive < archived.sequence) {
          // An archive append is not a publication. Restore its exact bytes before issuing
          // a receipt, and update the anchor first so another death cannot append it twice.
          let liveOffset = 0;
          try { liveOffset = fs.statSync(this.#eventsPath).size; }
          catch (error) { if (errorCode(error) !== "ENOENT") throw error; }
          writeFileAtomic(file, JSON.stringify({ ...intent, liveOffset }), { durable: true });
          fs.appendFileSync(this.#eventsPath, `${entry.line}\n`, { encoding: "utf8", mode: 0o600 });
          this.#confirmEventFile(this.#eventsPath);
        }
        // A false sidecar is not non-publication evidence: an old writer can recover a
        // completed live append without updating it, then compact away the live anchor.
        // An overtaken archive-only append is indistinguishable. Prefer its one archive
        // delivery over loss; never append behind the live sequence or publish a new id.
        archive.confirmLive(archived.sequence, archived.id);
        const pending = archive.pending();
        // Receipt recovery is direct metadata work. Leave closed-day sealing to the next
        // ordinary archive append, never scan history just to resolve this intent.
        if (pending?.id === archived.id && pending.sequence === archived.sequence) archive.commit(pending, false);
        event = archived;
      }
    }
    if (event && !prior && live && !archive && after) {
      // No-archive recovery of a dead publisher's live append: nothing durable is written
      // under the lock. The intent stays until the receipt is durable (after release).
      after.finish = this.#finishLiveReceipt(event, file, this.#dedupePath(intent.dedupeKey, ".json"));
      return event;
    }
    if (!event && !archive && after) {
      // No-archive, nothing committed at the intent's offset (a partial or failed append): the
      // unlink is ordered under the lock (a same-key writer may replace the intent next), its
      // namespace barrier runs after release. A crash before it may bring the intent back,
      // which settles to nothing again: no line can ever match its unique event id.
      fs.rmSync(file, { force: true });
      after.finish = () => syncPathNamespace(path.dirname(file));
      return undefined;
    }
    if (event && !prior) {
      if (live) {
        this.#confirmEventFile(this.#eventsPath);
        archive?.confirmLive(live.sequence, live.id);
        const pending = archive?.pending();
        if (pending?.id === live.id && pending.sequence === live.sequence) archive!.commit(pending, false);
      }
      writeFileAtomic(this.#dedupePath(intent.dedupeKey, ".json"), JSON.stringify(event), { durable: true });
    }
    this.#removeDedupeIntent(file);
    return event;
  }

  /** Under the mesh lock, settle all intents before a rewrite can invalidate byte offsets. */
  #settleDedupeIntents(archive?: MeshArchive): void {
    const directory = path.join(this.root, "event-receipts");
    let names: string[];
    try { names = fs.readdirSync(directory); }
    catch (error) { if (errorCode(error) === "ENOENT") return; throw error; }
    for (const name of names.filter(entry => /^[a-f0-9]{64}\.pending\.json$/.test(entry))) {
      this.#settleDedupeIntent(path.join(directory, name), undefined, archive);
    }
  }

  #preparePublish(input: MeshPublishInput, batch?: { appendStarted: boolean; bytes: number }, after?: AfterUnlock): () => MeshEvent {
    this.#validateTopic(input.topic);
    if (input.to !== undefined && !input.to.trim()) throw new Error("Mesh recipient is empty");
    const principal = input.principal;
    const stamp = typeof input.data === "function" ? input.data as (createdAt: number) => unknown : undefined;
    const fixedData = stamp ? undefined : input.data;
    input.signal?.throwIfAborted();
    // Historical reads are prepared off-lock and validated before any mutation.
    // An authoritative receipt needs no archive recovery, even if its mount is gone.
    let preflight: MeshArchive | undefined;
    if (!input.dedupeKey || !fs.existsSync(this.#dedupePath(input.dedupeKey, ".json"))) {
      try { preflight = MeshArchive.fromRoot(this.root); }
      catch (error) {
        if (input.dedupeKey && fs.existsSync(this.#dedupePath(input.dedupeKey, ".pending.json"))) {
          throw new MeshDedupeRecoveryError("Event archive configuration is unavailable during dedupe recovery", { cause: error });
        }
        throw error;
      }
    }
    let prepared: MeshArchiveRecoveryPlan | undefined;
    let digestRepair: ReturnType<MeshArchive["prepareDigestRepair"]>;
    let liveCatchUp: PreparedLiveCatchUp | undefined;
    try {
      prepared = preflight?.prepareRecovery(this.#readLastEventSequence());
      digestRepair = preflight?.prepareDigestRepair();
      if (preflight) liveCatchUp = this.#prepareLiveCatchUp(preflight);
    } catch (error) {
      if (!(error instanceof MeshArchiveRecoveryChanged) && input.dedupeKey &&
          fs.existsSync(this.#dedupePath(input.dedupeKey, ".pending.json"))) {
        throw new MeshDedupeRecoveryError("Event archive preflight is unavailable during dedupe recovery", { cause: error });
      }
      throw error;
    }
    return () => {
      if (after) after.finish = undefined;
      input.signal?.throwIfAborted();
      const receiptPath = input.dedupeKey ? this.#dedupePath(input.dedupeKey, ".json") : undefined;
      const intentPath = input.dedupeKey ? this.#dedupePath(input.dedupeKey, ".pending.json") : undefined;
      if (input.dedupeKey) {
        const prior = this.#readDedupeReceipt(input.dedupeKey, !after);
        if (prior) {
          // Receipt-before-unlink crash: the receipt is authoritative; finish cleanup.
          // A single publish confirms the visible receipt and unlinks after release.
          if (after) {
            after.finish = () => {
              this.#confirmEventFile(receiptPath!);
              if (fs.existsSync(intentPath!)) this.#removeDedupeIntent(intentPath!);
            };
          } else if (fs.existsSync(intentPath!)) this.#removeDedupeIntent(intentPath!);
          return prior;
        }
      }
      let archive: MeshArchive | undefined;
      try { archive = MeshArchive.fromRoot(this.root); }
      catch (error) {
        if (intentPath && fs.existsSync(intentPath)) throw new MeshDedupeRecoveryError("Event archive configuration is unavailable during dedupe recovery", { cause: error });
        throw error;
      }
      // Fence a historical live-log read BEFORE repair/recovery/intent mutations.
      if (archive && (archive.head()?.sequence ?? 0) < this.#readLastEventSequence()) {
        if (!liveCatchUp || liveCatchUp.dir !== archive.dir ||
          liveCatchUp.after > (archive.head()?.sequence ?? 0) ||
          liveCatchUp.identity !== this.#liveLogIdentity()) throw new MeshArchiveRecoveryChanged();
      }
      this.#repairEventLog();
      // Recover the whole reboot suffix before any one intent can advance the live horizon.
      // In the same boot, defer ordinary pending cutback until the exact retry has settled.
      // New keys still take only the normal recovery path, with no event-history lookup.
      if (input.dedupeKey) {
        if (archive && fs.existsSync(intentPath!)) {
          try { this.#recoverArchive(archive, true, prepared, liveCatchUp); }
          catch (error) {
            if (error instanceof MeshArchiveRecoveryChanged) throw error;
            throw new MeshDedupeRecoveryError("Event archive reboot recovery is unavailable during dedupe recovery", { cause: error });
          }
        }
        const prior = this.#settleDedupeIntent(intentPath!, input.dedupeKey, archive, after);
        if (prior) return prior;
      }
      if (archive) {
        this.#recoverArchive(archive, false, prepared, liveCatchUp);
        if (archive.dir === preflight?.dir) archive.installDigestRepair(digestRepair);
      }
      const createdAt = Date.now();
      const eventData = stamp ? jsonClone(stamp(createdAt)) : fixedData;
      const sequence = Math.max(this.#readSequence(), this.#readLastEventSequence()) + 1;
      const event: MeshEvent = {
        id: randomUUID(),
        ...(input.dedupeKey ? { dedupeKey: input.dedupeKey } : {}),
        sequence,
        topic: input.topic,
        kind: input.kind?.trim() || "message",
        from: jsonClone(input.from),
        ...(principal ? { principal } : {}),
        // Old bridges only wrote data.bridge. It can veto a native attestation, but
        // arbitrary payload data cannot establish bridge verification or any authority.
        ...(input.from.verified === "bridge" ? { verification: "bridge" as const }
          : eventData && typeof eventData === "object" && "bridge" in eventData ? {}
          : { verification: "mesh" as const }),
        ...(input.to ? { to: input.to } : {}),
        ...(input.text !== undefined ? { text: input.text } : {}),
        ...(eventData !== undefined ? { data: eventData } : {}),
        createdAt,
      };
      const line = JSON.stringify(event);
      if (Buffer.byteLength(line, "utf8") > this.maxEventBytes) {
        throw new Error(`Mesh event exceeds ${this.maxEventBytes} bytes`);
      }
      // The counter is a reservation: a crash after it leaves a gap, never a reused sequence.
      // The archive holds the event durably before it goes live (smarty-dev#754); the live
      // append commits it. If either step fails, the event is cut back out of the archive.
      // ponytail: the archive's fdatasync (~15 ms on Dev1's NVMe) runs under the lock, so a
      // burst of 160 publishes held other writers up to 1.3 s at 5x the fleet rate. If the
      // lock's held share matters (#816), sync after unlocking so concurrent syncs share a commit.
      atomicWrite(this.#counterPath, sequence);
      let liveOffset = 0;
      try { liveOffset = fs.statSync(this.#eventsPath).size; }
      catch (error) { if (errorCode(error) !== "ENOENT") throw error; }
      if (intentPath) {
        // A durable negative lookup exists before the intent. Only begin() can replace it
        // with the synced archive line address, before any live append.
        archive?.reserveLookup(sequence);
        // This is the crash fence: the intent is durable before the live append begins.
        writeFileAtomic(intentPath, JSON.stringify({
          dedupeKey: input.dedupeKey!, reservedSequence: sequence, eventId: event.id, liveOffset,
          ...(archive ? { archiveDir: archive.dir } : {}),
        } satisfies MeshDedupeIntent), { durable: true });
      }
      const pending = archive?.begin({ event, line });
      // Test-only process-death fence: unlike an append exception, no rollback can run.
      if (receiptPath && pending && process.env.PI_FABRIC_TEST_CRASH_AFTER_ARCHIVE_BEGIN === "1") process.kill(process.pid, "SIGKILL");
      try {
        if (batch) batch.appendStarted = true;
        fs.appendFileSync(this.#eventsPath, `${line}\n`, { encoding: "utf8", mode: 0o600 });
      } catch (error) {
        if (pending) archive!.rollback(pending);
        throw error;
      }
      // This distinct fence leaves the live event complete but the sidecar unconfirmed.
      if (receiptPath && pending && process.env.PI_FABRIC_TEST_CRASH_BEFORE_ARCHIVE_COMMIT === "1") process.kill(process.pid, "SIGKILL");
      if (pending) archive!.commit(pending);
      // Test-only crash fence for the installed-Pi recovery proof; production never sets this.
      if (receiptPath && process.env.PI_FABRIC_TEST_CRASH_AFTER_LIVE_APPEND === "1") process.kill(process.pid, "SIGKILL");
      if (receiptPath && after && !archive) {
        // No-archive keyed publish: the durable intent (above) is the crash fence; the live
        // barrier, receipt and unlink run after release, before the publish resolves.
        after.finish = this.#finishLiveReceipt(event, intentPath!, receiptPath);
      } else if (receiptPath) {
        // Archive-coupled and legacy batch receipts keep their locked protocol (smarty-dev#6000).
        this.#confirmEventFile(this.#eventsPath);
        writeFileAtomic(receiptPath, JSON.stringify(event), { durable: true });
        if (intentPath) this.#removeDedupeIntent(intentPath);
      }
      if (batch) batch.bytes = Buffer.byteLength(line, "utf8") + 1;
      else {
        // Compaction stays under the lock, as on main (off-lock compaction: smarty-dev#7002).
        this.#compactEventLog();
        if (input.durable && !receiptPath) {
          // Unkeyed durable publish: the live-log barrier runs after release (group commit).
          if (after) after.finish = () => this.#confirmEventsAfterRelease();
          else this.#confirmEventFile(this.#eventsPath);
        }
      }
      return event;
    };
  }

  async publish(input: MeshPublishInput): Promise<MeshEvent> {
    // Freeze ordinary payload/principal bytes once, even if archive validation retries.
    input = this.#capturePublication(input);
    const recoveryDeadline = Date.now() + this.#lock.lockTimeoutMs;
    const after: AfterUnlock = {};
    let event: MeshEvent;
    for (;;) {
      try { event = await this.#lock.withLock(this.#preparePublish(input, undefined, after), undefined, "publish"); break; }
      catch (error) {
        if (!(error instanceof MeshArchiveRecoveryChanged) || Date.now() >= recoveryDeadline) throw error;
        await delay(0);
      }
    }
    // Committed. Never retry from here: a failed barrier must not append the event twice.
    // The publish resolves only after its bytes (and any receipt) are durable.
    await after.finish?.();
    return event;
  }

  /** Commits a prefix in order under one lock. At most 256 events and 50 ms of work
   * (checked between events; a synchronous fsync/scheduler stall cannot be preempted).
   * Events retain publish's append/archive protocol, with one final durability barrier. A failed suffix
   * is retried by the caller after checkpointing the returned committed prefix.
   */
  async publishBatch(inputs: MeshPublishInput[]): Promise<MeshEvent[]> {
    if (!inputs.length || inputs.length > 256) throw new Error("Mesh publish batch must contain 1..256 events");
    inputs = inputs.map(input => this.#capturePublication(input));
    const recoveryDeadline = Date.now() + this.#lock.lockTimeoutMs;
    for (;;) {
      try {
        const prepared: Array<{ commit: () => MeshEvent; outcome: { appendStarted: boolean; bytes: number } }> = [];
        for (const input of inputs) {
          const outcome = { appendStarted: false, bytes: 0 };
          try { prepared.push({ commit: this.#preparePublish({ ...input, durable: false }, outcome), outcome }); }
          catch (error) { if (!prepared.length) throw error; break; }
        }
        return await this.#lock.withLock(() => {
          const started = performance.now();
          const events: MeshEvent[] = [];
          let bytes = 0;
          for (const { commit, outcome } of prepared) {
            // Keep the entire uncheckpointed prefix inside the retained tail, including
            // a line-boundary slack event, rather than compacting away its recovery IDs.
            if (events.length && (performance.now() - started >= 50 || bytes + 2 * this.maxEventBytes + 1 > this.#retainedEventLogBytes)) break;
            try { events.push(commit()); bytes += outcome.bytes; }
            catch (error) {
              // After append begins, success may be unknown. Stop rather than replay a
              // possibly complete event; a restarted bridge reconciles its committed IDs.
              if (outcome.appendStarted) throw new Error("Mesh batch publication outcome is uncertain; reconcile before retry", { cause: error });
              if (!events.length) throw error;
              break;
            }
          }
          this.#compactEventLog();
          this.#confirmEventFile(this.#eventsPath);
          return events;
        }, undefined, "bridge");
      } catch (error) {
        if (!(error instanceof MeshArchiveRecoveryChanged) || Date.now() >= recoveryDeadline) throw error;
        await delay(0);
      }
    }
  }

  #capturePublication(input: MeshPublishInput): MeshPublishInput {
    return { ...input, principal: copyFabricPrincipal(input.principal),
      data: typeof input.data === "function" || input.data === undefined ? input.data : jsonClone(input.data) };
  }

  read(
    input: {
      after?: number;
      topic?: string;
      to?: string;
      limit?: number;
    } = {},
  ): MeshEvent[] {
    if (input.topic !== undefined) this.#validateTopic(input.topic);
    const limit = Math.max(1, Math.min(Math.floor(input.limit ?? 100), this.maxReadEvents));
    const after = input.after === undefined ? undefined : Math.max(0, Math.floor(input.after));
    const events =
      after === undefined
        ? this.#readRecentEvents(input, limit)
        : this.#readArchivedAfter(after, input, limit) ?? this.#readEventsAfter(after, input, limit);
    return events.map((event) => jsonClone(event));
  }

  /**
   * The first committed event after a sequence. From the archive's first sequence on, only the
   * archive answers: it holds each event before the event goes live, so it is one coherent
   * source across a live-log rewrite (smarty-dev#754). Below it, and in a store without the
   * archive, the live log answers: it is the only source there, so an event a rewrite cuts
   * from that range is gone either way.
   */
  nextEventAfter(after: number): MeshEvent | undefined {
    const archive = MeshArchive.fromRoot(this.root);
    const first = archive?.firstSequence();
    if (!archive || first === undefined) return this.#cloned(this.#readEventsAfter(after, {}, 1)[0]);
    if (after + 1 < first) {
      // ponytail: not expected in practice (a newly set archive backfills the whole live log),
      // but the answer stays right if the archive ever starts above a live event.
      const live = this.#readEventsAfter(after, {}, 1)[0];
      if (live && live.sequence < first) return this.#cloned(live);
    }
    return this.#cloned(archive.readAfter(Math.max(after, first - 1), this.#readLastEventSequence(), () => true, 1)[0]);
  }

  #cloned(event: MeshEvent | undefined): MeshEvent | undefined {
    return event ? jsonClone(event) : undefined;
  }

  // A cursor older than the live log reads the archive, which holds every event since it was
  // set (smarty-dev#754). The oldest live sequence changes only when the log is rewritten.
  #readArchivedAfter(after: number, input: { topic?: string; to?: string }, limit: number): MeshEvent[] | undefined {
    let identity: string;
    try {
      const stat = fs.statSync(this.#eventsPath);
      identity = `${this.#readGeneration()}:${stat.ino}`;
    } catch (error) {
      if (errorCode(error) === "ENOENT") return undefined;
      throw error;
    }
    if (this.#oldestLive?.identity !== identity) this.#oldestLive = { identity, sequence: this.oldestSequence() };
    const oldest = this.#oldestLive.sequence;
    if (oldest === undefined || after + 1 >= oldest) return undefined;
    const archived = MeshArchive.fromRoot(this.root)
      ?.readAfter(after, this.#readLastEventSequence(), (event) => this.#eventMatches(event, input), limit, input.topic);
    return archived?.length ? archived : undefined;
  }

  // Before a publish, under the lock, bring the live log and the archive level. A publish that
  // stopped between its archive append and its commit is cut back out; if its event did go
  // live, the catch-up below archives it again from the live log. So do events that a store
  // without the archive appended (an older Fabric, or before the archive was set).
  #recoverArchive(archive: MeshArchive, rebootOnly = false, prepared?: MeshArchiveRecoveryPlan,
    liveCatchUp?: PreparedLiveCatchUp): void {
    const recovery = archive.recover(this.#readLastEventSequence(), prepared, rebootOnly);
    if (rebootOnly && !recovery.rebooted) return;
    if (recovery.rebooted) {
      // A power loss took live appends whose archive lines were synced: they go live again,
      // synced this time, before anything else can take their sequences.
      const last = recovery.promote.at(-1);
      if (last) {
        fs.appendFileSync(this.#eventsPath, recovery.promote.map(({ line }) => `${line}\n`).join(""), { encoding: "utf8", mode: 0o600 });
        const descriptor = fs.openSync(this.#eventsPath, "r+");
        try {
          fs.fdatasyncSync(descriptor);
        } finally {
          fs.closeSync(descriptor);
        }
        atomicWrite(this.#counterPath, Math.max(this.#readSequence(), last.event.sequence));
      }
      archive.recovered(last, recovery.promote);
    }
    const archived = archive.head()?.sequence ?? 0;
    if (archived < this.#readLastEventSequence()) {
      if (!liveCatchUp || liveCatchUp.dir !== archive.dir || liveCatchUp.after > archived) throw new MeshArchiveRecoveryChanged();
      archive.catchUp(liveCatchUp.entries.filter(entry => entry.event.sequence > archived));
    }
  }

  #liveLogIdentity(): string {
    try {
      const stat = fs.statSync(this.#eventsPath, { bigint: true });
      return `${stat.dev}:${stat.ino}:${stat.size}:${stat.mtimeNs ?? stat.mtimeMs}:${stat.ctimeNs ?? stat.ctimeMs}`;
    } catch (error) { if (errorCode(error) === "ENOENT") return "absent"; throw error; }
  }

  #prepareLiveCatchUp(archive: MeshArchive): PreparedLiveCatchUp | undefined {
    const after = archive.head()?.sequence ?? 0;
    if (after >= this.#readLastEventSequence()) {
      this.#preparedLiveCatchUp = undefined;
      return undefined;
    }
    const identity = this.#liveLogIdentity();
    const cached = this.#preparedLiveCatchUp;
    if (cached?.dir === archive.dir && cached.after === after && cached.identity === identity) return cached;
    // Only complete lines are committed. A torn suffix is repaired under custody;
    // capture its complete byte boundary here so it is never archived by this scan.
    const offset = this.#decodeCursor(this.latestOffset()).offset;
    const entries = this.#liveEntriesAfter(after, offset);
    if (this.#liveLogIdentity() !== identity) throw new MeshArchiveRecoveryChanged();
    return this.#preparedLiveCatchUp = { dir: archive.dir, after, identity, entries };
  }

  // Live lines after a sequence, oldest first. It reads back from the end, so the usual one or
  // two unarchived events cost one chunk.
  #liveEntriesAfter(after: number, endOffset?: number): MeshArchiveEntry[] {
    let descriptor: number | undefined;
    try {
      descriptor = fs.openSync(this.#eventsPath, "r");
      let position = Math.min(fs.fstatSync(descriptor).size, endOffset ?? Number.MAX_SAFE_INTEGER);
      let carry = Buffer.alloc(0);
      const entries: MeshArchiveEntry[] = [];
      while (position > 0) {
        const start = Math.max(0, position - EVENT_READ_CHUNK_BYTES);
        const chunk = Buffer.allocUnsafe(position - start);
        fs.readSync(descriptor, chunk, 0, chunk.length, start);
        position = start;
        const text = Buffer.concat([chunk, carry]);
        // Before the first newline, a line may continue in the earlier chunk.
        const split = position === 0 ? 0 : text.indexOf(0x0a) + 1;
        if (split === 0 && position > 0) {
          carry = text;
          continue;
        }
        carry = text.subarray(0, split);
        const lines = text.subarray(split).toString("utf8").split("\n");
        for (let index = lines.length - 1; index >= 0; index--) {
          const line = lines[index];
          if (!line) continue;
          let event: MeshEvent;
          try {
            event = JSON.parse(line) as MeshEvent;
          } catch {
            continue;
          }
          if (typeof event.sequence !== "number") continue;
          if (event.sequence <= after) return entries.reverse();
          entries.push({ event, line });
        }
      }
      return entries.reverse();
    } catch (error) {
      if (errorCode(error) === "ENOENT") return [];
      throw error;
    } finally {
      if (descriptor !== undefined) fs.closeSync(descriptor);
    }
  }

  /**
   * The sequence of the oldest event still in the log, read from its first line. Undefined when
   * it cannot tell (no log, or no readable event near its start): callers must then keep
   * anything that depends on an event still being replayable.
   */
  oldestSequence(): number | undefined {
    let descriptor: number | undefined;
    try {
      descriptor = fs.openSync(this.#eventsPath, "r");
      const readBytes = Math.min(fs.fstatSync(descriptor).size, this.maxEventBytes + 1);
      const head = Buffer.allocUnsafe(readBytes);
      const bytesRead = fs.readSync(descriptor, head, 0, readBytes, 0);
      const text = head.subarray(0, bytesRead).toString("utf8");
      for (const line of text.slice(0, text.lastIndexOf("\n") + 1).split("\n")) {
        try {
          const parsed = JSON.parse(line) as { sequence?: unknown };
          if (typeof parsed.sequence === "number" && Number.isSafeInteger(parsed.sequence)) return parsed.sequence;
        } catch { /* skip a malformed line */ }
      }
      return undefined;
    } catch (error) {
      if (errorCode(error) === "ENOENT") return undefined;
      throw error;
    } finally {
      if (descriptor !== undefined) fs.closeSync(descriptor);
    }
  }

  latestSequence(): number {
    return Math.max(this.#readSequence(), this.#readLastEventSequence());
  }

  latestOffset(): number {
    return this.latestCursor().cursor;
  }

  /** Tail offset and sequence boundary from the same file handle, never the reservation/archive head. */
  latestCursor(): { cursor: number; last?: { sequence: number; id: string } } {
    const generation = this.#readGeneration();
    let descriptor: number | undefined;
    let completeOffset = 0;
    let last: { sequence: number; id: string } | undefined;
    try {
      descriptor = fs.openSync(this.#eventsPath, "r");
      const size = fs.fstatSync(descriptor).size;
      if (size > 0) {
        const lastByte = Buffer.allocUnsafe(1);
        fs.readSync(descriptor, lastByte, 0, 1, size - 1);
        if (lastByte[0] === 0x0a) {
          completeOffset = size;
        } else {
          const readBytes = Math.min(size, this.maxEventBytes + 1);
          const tail = Buffer.allocUnsafe(readBytes);
          fs.readSync(descriptor, tail, 0, readBytes, size - readBytes);
          const newline = tail.lastIndexOf(0x0a);
          completeOffset = newline >= 0 ? size - readBytes + newline + 1 : 0;
        }
      }
      if (completeOffset > 0) {
        // Bounded startup work: just the last complete line, using the captured offset even
        // if another publisher has since appended. A partial append is never an anchor.
        const readBytes = Math.min(completeOffset, this.maxEventBytes + 2);
        const tail = Buffer.allocUnsafe(readBytes);
        const bytesRead = fs.readSync(descriptor, tail, 0, readBytes, completeOffset - readBytes);
        if (bytesRead === readBytes) {
          const lineStart = tail.lastIndexOf(0x0a, tail.length - 2) + 1;
          if (lineStart > 0 || readBytes === completeOffset) {
            try {
              const event = JSON.parse(tail.subarray(lineStart, tail.length - 1).toString("utf8")) as MeshEvent;
              if (Number.isSafeInteger(event.sequence) && event.sequence > 0 && typeof event.id === "string") {
                last = { sequence: event.sequence, id: event.id };
              }
            } catch { /* unreadable boundary: a saved lastless cursor reconciles conservatively */ }
          }
        }
      }
    } catch (error) {
      if (errorCode(error) !== "ENOENT") throw error;
    } finally {
      if (descriptor !== undefined) fs.closeSync(descriptor);
    }
    // Sequence zero is a start boundary, not a later reservation that could skip unread work.
    if (completeOffset === 0) last = { sequence: 0, id: "" };
    return { cursor: this.#encodeCursor(generation, completeOffset), ...(last ? { last } : {}) };
  }

  tail(cursor: number, limit = 100): MeshTailResult {
    const boundedLimit = Math.max(1, Math.min(Math.floor(limit), this.maxReadEvents));
    const generation = this.#readGeneration();
    const decoded = this.#decodeCursor(cursor);
    let descriptor: number | undefined;
    try {
      descriptor = fs.openSync(this.#eventsPath, "r");
      const size = fs.fstatSync(descriptor).size;
      let position = decoded.generation === generation ? Math.min(decoded.offset, size) : 0;
      if (position > 0) {
        const previousByte = Buffer.allocUnsafe(1);
        fs.readSync(descriptor, previousByte, 0, 1, position - 1);
        if (previousByte[0] !== 0x0a) position = 0;
      }
      if (position >= size) {
        return { events: [], nextOffset: this.#encodeCursor(generation, position) };
      }
      const chunkBytes = Math.min(
        size - position,
        Math.max(this.maxEventBytes + 1, EVENT_READ_PAGE_BYTES),
      );
      const buffer = Buffer.allocUnsafe(chunkBytes);
      const bytesRead = fs.readSync(descriptor, buffer, 0, chunkBytes, position);
      const events: MeshEvent[] = [];
      const cursors: number[] = [];
      let lineStart = 0;
      let consumed = 0;
      for (let index = 0; index < bytesRead; index++) {
        if (buffer[index] !== 0x0a) continue;
        const line = buffer.subarray(lineStart, index).toString("utf8").trim();
        lineStart = index + 1;
        consumed = lineStart;
        if (line) {
          try {
            const event = JSON.parse(line) as MeshEvent;
            if (typeof event.sequence === "number") {
              events.push(event);
              cursors.push(this.#encodeCursor(generation, position + consumed));
            }
          } catch { /* skip malformed mesh log line */ }
        }
        if (events.length >= boundedLimit) break;
      }
      return {
        events: events.map((event) => jsonClone(event)),
        nextOffset: this.#encodeCursor(generation, position + consumed),
        cursors,
      };
    } catch (error) {
      if (errorCode(error) === "ENOENT") {
        return { events: [], nextOffset: this.#encodeCursor(generation, 0) };
      }
      throw error;
    } finally {
      if (descriptor !== undefined) fs.closeSync(descriptor);
    }
  }

  #readRecentEvents(
    input: { topic?: string; to?: string },
    limit: number,
  ): MeshEvent[] {
    let events: MeshEvent[] = [];
    let before: number | undefined;
    while (events.length < limit) {
      const page = readJsonlPage(
        this.#eventsPath,
        this.maxReadEvents,
        before,
        Math.max(this.maxEventBytes + 1, EVENT_READ_PAGE_BYTES),
      );
      const pageEvents: MeshEvent[] = [];
      for (const line of page.lines) {
        const parsed = line.parsed;
        if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) continue;
        const event = parsed as MeshEvent;
        if (typeof event.sequence !== "number" || !this.#eventMatches(event, input)) continue;
        pageEvents.push(event);
      }
      events = [...pageEvents, ...events].slice(-limit);
      if (!page.hasMore || page.before === undefined || page.before === before) break;
      before = page.before;
    }
    return events;
  }

  #readEventsAfter(
    after: number,
    input: { topic?: string; to?: string },
    limit: number,
  ): MeshEvent[] {
    let descriptor: number | undefined;
    try {
      descriptor = fs.openSync(this.#eventsPath, "r");
      const stat = fs.fstatSync(descriptor);
      const size = stat.size;
      const events: MeshEvent[] = [];
      // Sequences rise in log order (appends run under the lock), so every line before the
      // hint has a sequence at or below it and cannot match a read after it. Without this,
      // each read scanned the whole log (tens of MB on the fleet) from the start
      // (smarty-dev#557). A rotated log is a new file and generation, and the hint must end a line.
      const generation = this.#readGeneration();
      const cached = this.#readHints?.generation === generation && this.#readHints.inode === stat.ino
        ? this.#readHints : undefined;
      const hints = cached?.lines ?? [];
      const anchors = cached?.anchors ?? new Map<number, { sequence: number; offset: number }>();
      let boundary: { sequence: number; offset: number } | undefined;
      for (const hint of [...hints, ...anchors.values()]) {
        if (hint.sequence <= after && (!boundary || hint.sequence > boundary.sequence)) boundary = hint;
      }
      let position = 0;
      if (boundary && boundary.offset <= size && this.#endsLine(descriptor, boundary.offset)) position = boundary.offset;
      else boundary = undefined;
      const scanned: Array<{ sequence: number; offset: number }> = [];
      let lineChunks: Buffer[] = [];
      let lineBytes = 0;
      let skippingOversizedLine = false;
      let reachedLimit = false;

      const emitLine = (lineEnd?: number): void => {
        if (!skippingOversizedLine && lineBytes > 0) {
          const decoded = Buffer.concat(lineChunks, lineBytes).toString("utf8");
          const line = decoded.endsWith(String.fromCharCode(13)) ? decoded.slice(0, -1) : decoded;
          try {
            const event = JSON.parse(line) as MeshEvent;
            if (typeof event.sequence === "number" && lineEnd !== undefined) {
              const hint = { sequence: event.sequence, offset: lineEnd };
              scanned.push(hint);
              if (event.sequence <= after) boundary = hint;
              if (scanned.length > 2 * READ_HINT_LINES) scanned.splice(0, scanned.length - READ_HINT_LINES);
            }
            if (
              typeof event.sequence === "number" &&
              event.sequence > after &&
              this.#eventMatches(event, input)
            ) {
              events.push(event);
              reachedLimit = events.length >= limit;
            }
          } catch { /* skip malformed mesh log line */ }
        }
        lineChunks = [];
        lineBytes = 0;
        skippingOversizedLine = false;
      };

      while (position < size && !reachedLimit) {
        const readLength = Math.min(EVENT_READ_CHUNK_BYTES, size - position);
        const chunk = Buffer.allocUnsafe(readLength);
        const bytesRead = fs.readSync(descriptor, chunk, 0, readLength, position);
        if (bytesRead <= 0) break;
        const chunkStart = position;
        position += bytesRead;
        const captured = chunk.subarray(0, bytesRead);
        let segmentStart = 0;
        while (segmentStart < captured.length && !reachedLimit) {
          const newline = captured.indexOf(0x0a, segmentStart);
          const segmentEnd = newline < 0 ? captured.length : newline;
          const segment = captured.subarray(segmentStart, segmentEnd);
          if (!skippingOversizedLine) {
            if (lineBytes + segment.length <= this.maxEventBytes) {
              if (segment.length > 0) lineChunks.push(segment);
              lineBytes += segment.length;
            } else {
              lineChunks = [];
              lineBytes = 0;
              skippingOversizedLine = true;
            }
          }
          if (newline < 0) break;
          emitLine(chunkStart + newline + 1);
          segmentStart = newline + 1;
        }
      }
      if (!reachedLimit && (lineBytes > 0 || skippingOversizedLine)) emitLine();
      // A page can read hundreds of events beyond `after` while its caller waits at a grace
      // boundary. Keeping only the last 128 lines evicted that boundary on every poll, making
      // idle wakes scan the entire fleet log twice. Retain bounded reader anchors separately;
      // they use the same generation/inode/line-end checks, never delivery authority (#2039).
      if (boundary) {
        anchors.delete(after);
        anchors.set(after, boundary);
        if (anchors.size > READ_HINT_LINES) anchors.delete(anchors.keys().next().value!);
      }
      if (scanned.length > 0 || boundary) {
        const lines = new Map(hints.map((hint) => [hint.sequence, hint.offset]));
        for (const line of scanned) lines.set(line.sequence, line.offset);
        this.#readHints = {
          generation,
          inode: stat.ino,
          anchors,
          lines: [...lines].sort((left, right) => left[0] - right[0]).slice(-READ_HINT_LINES)
            .map(([sequence, offset]) => ({ sequence, offset })),
        };
      }
      return events;
    } catch (error) {
      if (errorCode(error) === "ENOENT") return [];
      throw error;
    } finally {
      if (descriptor !== undefined) fs.closeSync(descriptor);
    }
  }

  #endsLine(descriptor: number, offset: number): boolean {
    if (offset === 0) return true;
    const byte = Buffer.allocUnsafe(1);
    return fs.readSync(descriptor, byte, 0, 1, offset - 1) === 1 && byte[0] === 0x0a;
  }

  #eventMatches(event: MeshEvent, input: { topic?: string; to?: string }): boolean {
    if (input.topic !== undefined && event.topic !== input.topic) return false;
    if (input.to !== undefined && event.to !== input.to) return false;
    return true;
  }

  #readGeneration(): number {
    try {
      const parsed: unknown = JSON.parse(fs.readFileSync(this.#generationPath, "utf8"));
      return typeof parsed === "number" && Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : 0;
    } catch {
      return 0;
    }
  }

  #encodeCursor(generation: number, offset: number): number {
    const cursor = generation * CURSOR_OFFSET_BASE + offset;
    if (!Number.isSafeInteger(cursor) || cursor < 0) {
      throw new Error("Fabric mesh cursor exhausted its safe integer range");
    }
    return cursor;
  }

  #decodeCursor(cursor: number): { generation: number; offset: number } {
    if (!Number.isSafeInteger(cursor) || cursor < 0) return { generation: -1, offset: 0 };
    return {
      generation: Math.floor(cursor / CURSOR_OFFSET_BASE),
      offset: cursor % CURSOR_OFFSET_BASE,
    };
  }

  #compactEventLog(): void {
    // Never rewrite away an event named by a durable intent. Resolve every intent while
    // the publish lock is held, before taking the retained tail snapshot.
    let descriptor: number | undefined;
    try {
      descriptor = fs.openSync(this.#eventsPath, "r");
      const size = fs.fstatSync(descriptor).size;
      if (size <= this.#maxEventLogBytes) return;
      this.#settleDedupeIntents(MeshArchive.fromRoot(this.root));
      const readBytes = Math.min(
        size,
        this.#retainedEventLogBytes + this.maxEventBytes + 1,
      );
      const buffer = Buffer.allocUnsafe(readBytes);
      const bytesRead = fs.readSync(descriptor, buffer, 0, readBytes, size - readBytes);
      const captured = buffer.subarray(0, bytesRead);
      const retentionBoundary = Math.max(0, captured.length - this.#retainedEventLogBytes);
      const newline = retentionBoundary === 0 ? -1 : captured.indexOf(0x0a, retentionBoundary);
      const retainedStart = retentionBoundary === 0 ? 0 : newline >= 0 ? newline + 1 : captured.length;
      const retained = captured.subarray(retainedStart);
      fs.closeSync(descriptor);
      descriptor = undefined;
      // Persist both the retained bytes and the rename. Later intents may name offsets in
      // this generation; a reboot must not resurrect its unsynced predecessor or lose bytes.
      writeFileAtomic(this.#eventsPath, retained, { durable: true });
      atomicWrite(this.#generationPath, this.#readGeneration() + 1);
    } finally {
      if (descriptor !== undefined) fs.closeSync(descriptor);
    }
  }

  #repairEventLog(): void {
    let descriptor: number | undefined;
    try {
      descriptor = fs.openSync(this.#eventsPath, "r+");
      const size = fs.fstatSync(descriptor).size;
      if (size === 0) return;
      const lastByte = Buffer.allocUnsafe(1);
      fs.readSync(descriptor, lastByte, 0, 1, size - 1);
      if (lastByte[0] === 0x0a) return;
      const readBytes = Math.min(size, this.maxEventBytes + 1);
      const tail = Buffer.allocUnsafe(readBytes);
      fs.readSync(descriptor, tail, 0, readBytes, size - readBytes);
      const newline = tail.lastIndexOf(0x0a);
      fs.ftruncateSync(descriptor, newline >= 0 ? size - readBytes + newline + 1 : 0);
    } catch (error) {
      if (errorCode(error) !== "ENOENT") throw error;
    } finally {
      if (descriptor !== undefined) fs.closeSync(descriptor);
    }
  }

  #readLastEventSequence(): number {
    let descriptor: number | undefined;
    try {
      descriptor = fs.openSync(this.#eventsPath, "r");
      const size = fs.fstatSync(descriptor).size;
      if (size === 0) return 0;
      const readBytes = Math.min(size, this.maxEventBytes + 1);
      const tail = Buffer.allocUnsafe(readBytes);
      fs.readSync(descriptor, tail, 0, readBytes, size - readBytes);
      const lines = tail.toString("utf8").trim().split("\n");
      for (let index = lines.length - 1; index >= 0; index--) {
        const line = lines[index];
        if (!line) continue;
        try {
          const parsed = JSON.parse(line) as { sequence?: unknown };
          if (typeof parsed.sequence === "number" && Number.isSafeInteger(parsed.sequence)) {
            return parsed.sequence;
          }
        } catch { /* skip malformed sequence line */ }
      }
      return 0;
    } catch (error) {
      if (errorCode(error) === "ENOENT") return 0;
      throw error;
    } finally {
      if (descriptor !== undefined) fs.closeSync(descriptor);
    }
  }

  #readSequence(): number {
    try {
      const parsed: unknown = JSON.parse(fs.readFileSync(this.#counterPath, "utf8"));
      return typeof parsed === "number" && Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : 0;
    } catch (error) {
      if (errorCode(error) === "ENOENT") return 0;
      return 0;
    }
  }

  #validateTopic(topic: string): void {
    if (!TOPIC_PATTERN.test(topic)) throw new Error(`Invalid Fabric mesh topic: ${topic}`);
  }
}
