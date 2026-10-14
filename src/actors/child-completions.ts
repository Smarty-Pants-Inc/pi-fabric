import { createHash, randomUUID } from "node:crypto";
import fs, { type FSWatcher } from "node:fs";
import path from "node:path";
import { syncDirectoryChain, withExclusiveFileLock, writeJsonAtomic } from "../core/atomic-write.js";
import { ChildCompletionClaimLostError } from "../result-consumption.js";
export { ChildCompletionClaimLostError } from "../result-consumption.js";
import { ARCHIVE_PENDING_FILE, commitRunArchive, readPendingRunArchives } from "../agents/archive-custody.js";
import { ownedStat, processAlive } from "../storage/scratch.js";
import { verifiedRemove } from "../storage/verified-remove.js";
import type { AgentRunResult, AgentSpawner } from "../agents/types.js";

export type ActorChildResult = Pick<AgentRunResult, "id" | "name" | "status" | "text" | "error" | "startedAt" | "finishedAt">;
export interface ActorChildCompletion {
  format: 1;
  spawner: AgentSpawner;
  result: ActorChildResult;
}
const ID = /^[a-f0-9]{32}$/;
const STORED_FILE = /^([a-f0-9]{32})(?:\.result\.json|\.json|\.receipt|\.live-receipt|\.consumed|\.abandon)$/;

// Writer-visible pending work: an envelope, cancellation intent or archive source.
const WAKE_FILE = /^[a-f0-9]{32}\.(?:json|abandon|source)$/;
const WATCH_REARM_MS = 60_000;
const WATCH_REARM_MAX_MS = 5 * 60_000;
const WATCH_FAILURE_LIMIT = 6;
// A closed native handle normally reports `close`; an errored one may never do so.
const WATCH_CLOSE_GRACE_MS = 1_000;
// Envelopes/control records are small metadata, not the retained full result or session.
const MAX_SPOOL_JSON_BYTES = 1024 * 1024;
const MAX_WARNING_ENTRIES = 256;
class UnsafeCompletionEntry extends Error {}

/**
 * Edge-only wake for completion writers in other processes. It carries no authority:
 * the callback only schedules the owner's existing fresh ownership/claim reconcile.
 * Arm it BEFORE the first empty preflight so a later write cannot fall between them.
 * A failed watch gets one immediate rescan/re-arm, then 60/120/240/300 s one-shots.
 * Six consecutive failures enter observation-degraded health and warn once. Only a
 * delivered event resets the budget. Degraded recovery scans/re-arms once per minute;
 * a successful re-arm stops that fallback and restores the native event path.
 */
export class ActorChildCompletionWatch {
  #watcher: FSWatcher | undefined;
  #timer: NodeJS.Timeout | undefined;
  #failures = 0;
  #unavailable = false;
  #warnedUnavailable = false;
  #closed = false;
  // Watchers closed after an error whose native close may still be in flight.
  readonly #closing = new Set<Promise<void>>();
  constructor(readonly directory: string, readonly wake: () => void) { this.#arm(); }

  /** Native observation is unavailable; the degraded durable-source fallback remains active. */
  get unavailable(): boolean { return this.#unavailable; }
  get health(): "observing" | "recovering" | "observation-degraded" | "closed" {
    return this.#closed ? "closed" : this.#unavailable ? "observation-degraded" : this.#watcher ? "observing" : "recovering";
  }

  #arm(): void {
    this.#timer = undefined;
    if (this.#closed || this.#watcher) return;
    try {
      // Create the empty spool once (never recursively: a removed actor stays removed).
      try { fs.mkdirSync(this.directory, { mode: 0o700 }); }
      catch (error) { if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error; }
      const watcher = fs.watch(this.directory, { persistent: false }, (_event, filename) => {
        if (this.#closed || this.#watcher !== watcher) return;
        this.#failures = 0; // Only a delivered event, never arming, resets the failure budget.
        this.#warnedUnavailable = false;
        const name = filename == null ? undefined : path.basename(filename.toString());
        // A nameless event is a wake, not evidence that nothing changed.
        if (name === undefined || WAKE_FILE.test(name)) this.wake();
      });
      watcher.on("error", () => this.#failed(watcher));
      const degraded = this.#unavailable;
      this.#watcher = watcher;
      this.#unavailable = false;
      // A degraded tick already rescanned. Other re-arms cover their missed edges once.
      if (this.#failures && !degraded) this.wake();
    } catch { this.#failed(undefined); }
  }

  #failed(watcher: FSWatcher | undefined): void {
    if (this.#closed || (watcher && this.#watcher !== watcher)) return;
    if (watcher) {
      this.#watcher = undefined;
      this.#closing.add(this.#closeHandle(watcher));
    }
    if (this.#timer || this.#unavailable) return;
    this.#failures++;
    if (this.#failures === 1) {
      this.wake();
      this.#arm(); // Single immediate rescan/re-arm, not a fast retry timer.
      return;
    }
    if (this.#failures >= WATCH_FAILURE_LIMIT) {
      this.#unavailable = true;
      if (!this.#warnedUnavailable) {
        this.#warnedUnavailable = true;
        console.warn(`[pi-fabric] observation-degraded: child completion watch unavailable for ${this.directory}; using bounded 60 s durable-source scan/re-arm until native observation recovers`);
      }
      this.#scheduleDegraded();
      return;
    }
    this.#timer = setTimeout(() => this.#arm(), Math.min(WATCH_REARM_MAX_MS, WATCH_REARM_MS * 2 ** (this.#failures - 2)));
    this.#timer.unref();
  }

  #scheduleDegraded(): void {
    if (this.#closed || !this.#unavailable || this.#timer) return;
    // ponytail: named R-no-polling exception (#1047/#11172), exhausted observer only.
    // One bounded durable-source wake plus re-arm each minute; never abandon late work,
    // and never retain a safety poll when native observation has recovered.
    this.#timer = setTimeout(() => {
      this.#timer = undefined;
      if (this.#closed || !this.#unavailable) return;
      this.wake();
      this.#arm();
      this.#scheduleDegraded();
    }, WATCH_REARM_MS);
    this.#timer.unref();
  }

  /** Joins the native close; an errored handle that never reports it is released after a bounded grace. */
  #closeHandle(watcher: FSWatcher): Promise<void> {
    let settle!: () => void;
    const closed = new Promise<void>(resolve => { settle = resolve; });
    const grace = setTimeout(() => settle(), WATCH_CLOSE_GRACE_MS);
    grace.unref();
    watcher.once("close", () => { clearTimeout(grace); settle(); });
    try { watcher.close(); } catch { clearTimeout(grace); settle(); }
    const done: Promise<void> = closed.then(() => { this.#closing.delete(done); });
    return done;
  }

  /** Resolves once every owned native handle has closed (or its bounded grace expired), so custody is not released early. */
  close(): Promise<void> {
    this.#closed = true;
    if (this.#timer) clearTimeout(this.#timer);
    this.#timer = undefined;
    const watcher = this.#watcher;
    this.#watcher = undefined;
    if (watcher) this.#closing.add(this.#closeHandle(watcher));
    return Promise.all([...this.#closing]).then(() => undefined);
  }
}

/** Offline recovery retries exact actor-addressed archives before a dead root is collected. */
export const recoverActorRunArchives = (directory: string, expired: () => boolean = () => false, depth = 0): void => {
  if (expired() || depth > 32 || !ownedStat(directory)?.isDirectory()) return;
  const file = path.join(directory, ARCHIVE_PENDING_FILE);
  try {
    if (ownedStat(file)?.isFile()) {
      const archive = JSON.parse(fs.readFileSync(file, "utf8"));
      if (archive.awaitingResult && Number.isSafeInteger(archive.ownerPid) && !processAlive(archive.ownerPid)) {
        const status = path.join(directory, "status.json");
        if (ownedStat(status)?.isFile()) archive.result = { ...JSON.parse(fs.readFileSync(status, "utf8")), spawner: archive.spawner };
      }
      const archives = archive.awaitingResult ? [archive] : readPendingRunArchives(directory);
      for (const pending of archives) {
        const result = pending.result as AgentRunResult | undefined;
        if (pending.format === 1 && !pending.routePending && pending.actorOnly && typeof pending.actorSessionFile === "string" && result?.id === path.basename(directory) &&
            result.spawner?.kind === "actor" && ["completed", "failed", "stopped", "timed_out"].includes(result.status)) {
          const store = new ActorChildCompletionStore(pending.actorSessionFile);
          store.enqueue(result, result.spawner, pending.notify);
          commitRunArchive(directory, pending.kind);
          if (!fs.existsSync(file)) store.releaseArchiveSource(result.id);
        }
      }
    }
  } catch { /* Offline recovery cannot authorize deletion on failure. */ }
  const nested = path.join(directory, "nested");
  try {
    if (ownedStat(nested)?.isDirectory()) for (const name of fs.readdirSync(nested)) {
      if (expired()) break;
      recoverActorRunArchives(path.join(nested, name), expired, depth + 1);
    }
  } catch { /* Unknown contents remain guarded by the retention tree check. */ }
};

/**
 * Write-ahead inbox shared by an actor activation and its authoritative owner.
 * Only unread outcomes retain a full result. A handoff receipt transfers ownership
 * to the serial mailbox; that result lives until an activation consumes its context.
 */
export class ActorChildCompletionStore {
  readonly directory: string;
  // A completed spawning activation cannot add native live receipts later. Check
  // each eligible envelope once, even when mailbox/receipt I/O needs many retries.
  readonly #checked = new Map<string, boolean>();
  readonly #consumed = new Set<string>();
  readonly #publicationId = randomUUID();
  readonly #uncertainReceipts = new Set<string>();
  // Retain listed paths until absent, even if their contents/type become readable.
  readonly #warnedEntries = new Set<string>();
  #overflowSignature: string | undefined;
  #overflowScanned = false;
  #claimDepth = 0;
  constructor(readonly sessionFile: string) {
    this.directory = path.join(path.dirname(sessionFile), "child-completions");
  }

  #scanFiles(): string[] {
    let files: string[];
    try { files = fs.readdirSync(this.directory); }
    catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") {
        this.#warnedEntries.clear();
        this.#overflowSignature = undefined;
        this.#overflowScanned = false;
      }
      throw error;
    }
    if (this.#warnedEntries.size) {
      const present = new Set(files);
      for (const file of this.#warnedEntries) if (!present.has(path.basename(file))) this.#warnedEntries.delete(file);
    }
    this.#overflowScanned = false;
    // Inventory before callers short-circuit on a legitimate envelope/source. Only
    // saturated warning caches (or a still-active overflow) pay for this metadata-
    // only pass; no spool is opened. Safe-file churn cannot reset the summary.
    if (this.#warnedEntries.size === MAX_WARNING_ENTRIES || this.#overflowSignature !== undefined) this.#warnOverflow(files);
    return files;
  }

  #safeSpoolStat(stat: fs.Stats): boolean {
    return stat.isFile() && stat.nlink === 1 &&
      (!process.getuid || stat.uid === process.getuid()) && stat.size <= MAX_SPOOL_JSON_BYTES;
  }

  #warnOverflow(files?: string[], refused?: string): void {
    // A direct receipt/control read may precede any scan. Inventory at most once
    // between directory scans, never once per overflowing read. Later direct-read
    // changes remain refused; the next scan refreshes their aggregate visibility.
    if (this.#overflowScanned) return;
    this.#overflowScanned = true;
    try { files ??= fs.readdirSync(this.directory); } catch { files = []; }
    // XOR of path digests is order-independent and retains no unlisted path keys.
    const digest = Buffer.alloc(32);
    let unsafe = 0, additional = 0, includedRefused = false;
    const include = (file: string): void => {
      unsafe++;
      if (!this.#warnedEntries.has(file)) additional++;
      const entry = createHash("sha256").update(file).digest();
      for (let index = 0; index < digest.length; index++) digest[index] = digest[index]! ^ entry[index]!;
    };
    for (const name of files) {
      if (!WAKE_FILE.test(name) && !STORED_FILE.test(name)) continue;
      const file = path.join(this.directory, name);
      if (file === refused) { include(file); includedRefused = true; continue; }
      try { if (!this.#safeSpoolStat(fs.lstatSync(file))) include(file); }
      catch { /* Unreadable metadata is not proof of an unsafe entry. */ }
    }
    // Preserve visibility for a raced refusal even if its current metadata is safe
    // or its directory entry disappeared before this inventory.
    if (refused && !includedRefused) include(refused);
    const signature = `${unsafe}:${digest.toString("hex")}`;
    if (additional && signature !== this.#overflowSignature) {
      console.warn(`[pi-fabric] ${additional} additional unsafe child completion entries retained, not read; individual warning limit ${MAX_WARNING_ENTRIES} reached in ${this.directory}`);
    }
    this.#overflowSignature = unsafe ? signature : undefined;
  }

  /** A task can write spool names. Refuse unsafe entries before open and bound every
   * read; POSIX no-follow/nonblocking open also protects the final-component swap. */
  #readSpoolJson(file: string) {
    let fd: number | undefined;
    try {
      const expected = fs.lstatSync(file);
      if (expected.isSymbolicLink() || !this.#safeSpoolStat(expected)) throw new UnsafeCompletionEntry("not an owned bounded regular file");
      // ponytail: Windows has no task confinement, so a same-user task can already
      // end the host directly. Refuse Node-reported symlinks/junctions BEFORE open
      // and retain fstat checks; the remaining Windows lstat/open swap is an accepted
      // security gap, not a claim that these unavailable POSIX flags protect it.
      fd = fs.openSync(file, fs.constants.O_RDONLY | (fs.constants.O_NOFOLLOW ?? 0) | (fs.constants.O_NONBLOCK ?? 0));
      const stat = fs.fstatSync(fd);
      if (!this.#safeSpoolStat(stat) || stat.dev !== expected.dev || stat.ino !== expected.ino) {
        throw new UnsafeCompletionEntry("file changed before read");
      }
      // The sentinel catches growth, without allocating/reading beyond the bound.
      const bytes = Buffer.alloc(stat.size + 1);
      let length = 0;
      while (length < bytes.length) {
        const read = fs.readSync(fd, bytes, length, bytes.length - length, null);
        if (read === 0) break;
        length += read;
      }
      const after = fs.fstatSync(fd);
      if (length !== stat.size || !this.#safeSpoolStat(after) || after.size !== stat.size || after.mtimeMs !== stat.mtimeMs) {
        throw new UnsafeCompletionEntry("file changed during read");
      }
      return JSON.parse(bytes.subarray(0, length).toString("utf8"));
    } catch (error) {
      if (error instanceof UnsafeCompletionEntry || (error as NodeJS.ErrnoException).code === "ELOOP") {
        if (!this.#warnedEntries.has(file)) {
          if (this.#warnedEntries.size < MAX_WARNING_ENTRIES) {
            this.#warnedEntries.add(file);
            console.warn(`[pi-fabric] unsafe child completion entry retained, not read: ${file}`);
          } else this.#warnOverflow(undefined, file);
        }
        throw error instanceof UnsafeCompletionEntry ? error : new UnsafeCompletionEntry("no-follow open refused");
      }
      throw error;
    } finally { if (fd !== undefined) fs.closeSync(fd); }
  }

  enqueue(result: AgentRunResult, spawner: AgentSpawner, notify = true): void {
    if (!ID.test(result.id) || spawner.kind !== "actor") throw new Error("Invalid actor child completion identity");
    this.#withClaim(() => {
      // Cancellation may precede both archive files. A failed rollback must keep
      // the source's archive obligation, not let its prepared receipt discharge it.
      this.#rollbackAbandoned(result.id);
      const received = this.received(result.id);
      const publication = received ? this.#readSpoolJson(this.#receipt(result.id)).publication : undefined;
      if (this.#consumed.has(result.id) || (received && !publication)) {
        // A finalized observation/claim still prevents replay. Fence a receipt
        // whose rename was visible but whose durability barrier previously failed.
        syncDirectoryChain(this.directory);
        return;
      }
      // A publication fence is not delivery: retain a full recoverable archive
      // even while other observers must continue to regard it as claimed.
      if (fs.existsSync(this.#file(result.id)) && fs.existsSync(this.resultFile(result.id))) {
        syncDirectoryChain(this.directory); // Retry an archive/envelope post-rename barrier too.
        return;
      }
      writeJsonAtomic(this.resultFile(result.id), { ...result, spawner }, { durable: true });
      if (!notify) return; // Unread, but no automatic mailbox activation was requested.
      writeJsonAtomic(this.#file(result.id), {
        format: 1, spawner,
        result: {
          id: result.id, name: result.name, status: result.status,
          text: result.text.slice(0, 4000), startedAt: result.startedAt,
          ...(result.finishedAt !== undefined ? { finishedAt: result.finishedAt } : {}),
          ...(result.error !== undefined ? { error: result.error.slice(0, 4000) } : {}),
        },
      } satisfies ActorChildCompletion, { durable: true });
    });
  }

  acknowledge(id: string, options: { handoff?: boolean } = {}): void {
    if (!ID.test(id)) return;
    this.consume(id, { ...options, ...(options.handoff ? { mailbox: true } : {}) });
    fs.rmSync(this.#file(id), { force: true });
    this.#checked.delete(id);
  }

  /** Shared live/mailbox receipt, durable before an activation can observe it. */
  consume(id: string, options: { handoff?: boolean; publication?: boolean; mailbox?: boolean } = {}): void {
    if (!ID.test(id)) return;
    this.#withClaim(() => {
      const received = this.received(id);
      if (received && options.mailbox && !this.mailboxClaimed(id)) throw new ChildCompletionClaimLostError([id]);
      const publication = received ? this.#readSpoolJson(this.#receipt(id)).publication : undefined;
      if (!received || this.#uncertainReceipts.has(id) || (!options.publication && publication === this.#publicationId)) {
        this.#uncertainReceipts.add(id);
        writeJsonAtomic(this.#receipt(id), { id, kind: options.mailbox ? "mailbox" : "foreground", acknowledgedAt: Date.now(),
          ...(options.publication ? { publication: this.#publicationId } : {}),
        }, { durable: true });
        this.#uncertainReceipts.delete(id);
      } else syncDirectoryChain(this.directory); // Visibility must not discharge a failed post-rename fence.
      if (!options.handoff) this.releaseResult(id);
    });
  }

  /** Roll back only our unpublished fence, never a finalized/delivered receipt. */
  abandonForeground(id: string): void {
    if (!ID.test(id) || this.#consumed.has(id)) return;
    // Persist cancellation independently of the contested claim lock. A later
    // owner can retry this exact unpublished fence, but never a finalized receipt.
    writeJsonAtomic(path.join(this.directory, `${id}.abandon`), { id, publication: this.#publicationId }, { durable: true });
    this.#withClaim(() => this.#rollbackAbandoned(id));
  }

  #rollbackAbandoned(id: string): void {
    const file = path.join(this.directory, `${id}.abandon`);
    if (!fs.existsSync(file)) return;
    const abandoned = this.#readSpoolJson(file);
    let receipt;
    try { receipt = this.#readSpoolJson(this.#receipt(id)); }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
    if (abandoned.id === id && typeof abandoned.publication === "string" && receipt?.publication === abandoned.publication) {
      writeJsonAtomic(this.#receipt(id), { id, unread: true }, { durable: true });
      this.#checked.delete(id);
    }
    fs.rmSync(file, { force: true });
  }

  /** Preparation checks recoverability, but must not claim or remove an outcome. */
  prepareLive(id: string): void {
    if (!this.received(id)) fs.accessSync(this.resultFile(id), fs.constants.R_OK);
  }

  /** One atomic claim for the entire prepared batch, immediately before sending. */
  consumeLiveBatch(ids: string[]): void {
    if (ids.some((id) => !ID.test(id))) throw new Error("Invalid actor child completion id");
    const batchId = ids[0];
    if (!batchId) return;
    this.#withClaim(() => {
      const lost = ids.filter((id) => this.received(id));
      if (lost.length) throw new ChildCompletionClaimLostError(lost);
      // Staged receipts remain unread until the entire batch marker commits.
      for (const id of ids) writeJsonAtomic(this.#receipt(id), { id, kind: "live", batchId, acknowledgedAt: Date.now() }, { durable: true });
      try {
        writeJsonAtomic(this.#batchReceipt(batchId), { ids }, { durable: true });
      } catch (error) {
        // Rename can precede a failed barrier; nothing was sent. Withdraw the
        // marker under the same exclusive claim before another owner can inspect it.
        fs.rmSync(this.#batchReceipt(batchId), { force: true });
        syncDirectoryChain(this.directory); // Fence the withdrawal when storage permits it.
        throw error;
      }
    });
  }

  #withClaim<T>(operation: () => T): T {
    if (this.#claimDepth) return operation();
    return withExclusiveFileLock({ directory: this.directory, lockName: ".claim.lock",
      timeoutMessage: "Actor child completion claim is busy" }, () => {
      this.#claimDepth++;
      try { return operation(); } finally { this.#claimDepth--; }
    });
  }

  mailboxClaimed(id: string): boolean {
    return this.#withClaim(() => {
      if (!this.received(id)) return false;
      const receipt = this.#readSpoolJson(this.#receipt(id));
      // Legacy standalone receipts also gated persisted mailbox records.
      return receipt.kind === "mailbox" || (!receipt.kind && !receipt.batchId && !receipt.publication);
    });
  }

  #batchReceipt(id: string): string { return path.join(this.directory, `${id}.live-receipt`); }

  /** A foreground result was durably consumed: cleanup cannot undo its receipt. */
  discard(id: string): void {
    if (!ID.test(id)) return;
    this.consume(id, { handoff: true });
    // status.json may become terminal before the manager receives its settle event.
    this.#consumed.add(id);
    try {
      fs.rmSync(this.#file(id), { force: true });
      this.releaseResult(id);
      this.#checked.delete(id);
    } catch { /* Receipt is durable; the owner/retention sweep can retry cleanup. */ }
  }

  /** Inference consumption is distinct from transfer into a persisted mailbox. */
  markHandoffConsumed(id: string): void {
    if (!ID.test(id)) throw new Error("Invalid actor child completion id");
    writeJsonAtomic(path.join(this.directory, `${id}.consumed`), { id, consumedAt: Date.now() }, { durable: true });
  }

  handoffConsumed(id: string): boolean {
    if (!ID.test(id)) return false;
    try { return this.#readSpoolJson(path.join(this.directory, `${id}.consumed`)).id === id; }
    catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") return false;
      throw error;
    }
  }

  releaseResult(id: string): void {
    if (ID.test(id)) fs.rmSync(this.resultFile(id), { force: true });
  }

  #committedIds(): Set<string> {
    const ids = new Set<string>();
    // A committed native completion also proves consumption, including older workers
    // whose post-send receipt failed. Do not infer unread from a surviving envelope.
    let text: string;
    try { text = fs.readFileSync(this.sessionFile, "utf8"); } catch { return ids; }
    for (const line of text.split("\n")) {
      try {
        const entry = JSON.parse(line);
        const message = entry.type === "custom_message" ? entry : entry.type === "message" ? entry.message : undefined;
        if (message?.customType === "pi-fabric-agent-complete") {
          for (const id of message.details?.ids ?? []) if (typeof id === "string" && ID.test(id)) ids.add(id);
        }
      } catch { /* Ignore an interrupted last session entry. */ }
    }
    return ids;
  }

  releaseArchiveSource(id: string): void {
    if (ID.test(id)) fs.rmSync(path.join(this.directory, `${id}.source`), { force: true });
  }

  /** Durable address of the retained source, installed before attempting its archive. */
  trackArchiveSource(id: string, directory: string): void {
    if (!ID.test(id)) throw new Error("Invalid actor child archive source");
    writeJsonAtomic(path.join(this.directory, `${id}.source`), { id, directory }, { durable: true });
  }

  /** An owner/restart retries sources even when the original activation has exited. */
  recoverArchives(options: { actorId?: string; inFlightRunId?: string } = {}): void {
    let files: string[];
    try { files = this.#scanFiles(); } catch { return; }
    for (const name of files.filter(file => file.endsWith(".source") && ID.test(file.slice(0, -7))).slice(0, 100)) {
      try {
        const source = this.#readSpoolJson(path.join(this.directory, name));
        if (source.id !== name.slice(0, -7) || typeof source.directory !== "string" || path.basename(source.directory) !== source.id || !ownedStat(source.directory)?.isDirectory()) continue;
        const pending = path.join(source.directory, ARCHIVE_PENDING_FILE);
        if (!fs.existsSync(pending)) { fs.rmSync(path.join(this.directory, name), { force: true }); continue; }
        if (!ownedStat(pending)?.isFile()) continue;
        for (const archive of readPendingRunArchives(source.directory)) {
          const spawner = archive.result?.spawner;
          if (archive.format !== 1 || archive.result?.id !== source.id || archive.actorSessionFile !== this.sessionFile || archive.routePending || !archive.actorOnly || spawner?.kind !== "actor" ||
              (options.actorId && spawner.id !== options.actorId) || (options.inFlightRunId && spawner.runId === options.inFlightRunId)) continue;
          this.enqueue(archive.result, spawner, archive.notify);
          commitRunArchive(source.directory, archive.kind);
        }
        if (!fs.existsSync(pending)) fs.rmSync(path.join(this.directory, name), { force: true });
      } catch { /* Unknown/uncommitted custody stays fenced and retryable. */ }
    }
  }

  /** Unsettled child custody is a pending reply, even before a result envelope exists. */
  hasPendingReply(): boolean {
    try {
      return this.#scanFiles().some(file => {
        if (file.endsWith(".source") || file.endsWith(".abandon")) return true;
        return /^[a-f0-9]{32}\.json$/.test(file) && !this.received(file.slice(0, -5));
      });
    } catch (error) {
      return (error as NodeJS.ErrnoException).code !== "ENOENT"; // unknown custody cannot authorize sleep
    }
  }

  pending(options: { actorId?: string; inFlightRunId?: string } = {}): ActorChildCompletion[] {
    // Most resident polls find an empty spool. Readdir is the observation; do not take the
    // claim lock until an envelope, cancellation intent, or recoverable archive source exists.
    // The owner arms ActorChildCompletionWatch before this preflight; a later writer wakes
    // the existing actor settlement/mesh path, so an empty scan never waits on a slow cadence.
    if (!this.#hasPendingWork(options)) return [];
    this.recoverArchives(options);
    if (!this.#hasPendingWork(options)) return [];
    try { return this.#withClaim(() => this.#pending(options)); }
    catch { return []; } // Busy/unreadable claims retry on the next owner poll.
  }

  #hasPendingWork(options: { actorId?: string; inFlightRunId?: string }): boolean {
    let files: string[];
    try { files = this.#scanFiles(); } catch (error) {
      return (error as NodeJS.ErrnoException).code !== "ENOENT";
    }
    return files.some(file => {
      if (!WAKE_FILE.test(file)) return false;
      try {
        const value = this.#readSpoolJson(path.join(this.directory, file)) as ActorChildCompletion;
        if (!file.endsWith(".json") || (!options.actorId && !options.inFlightRunId)) return true;
        return value.format === 1 && value.spawner?.kind === "actor" &&
          (!options.actorId || value.spawner.id === options.actorId) &&
          (!options.inFlightRunId || value.spawner.runId !== options.inFlightRunId);
      } catch (error) {
        // Unsafe entries remain on disk/custody-visible, but never enter the claim/read path.
        return !(error instanceof UnsafeCompletionEntry);
      }
    });
  }

  #pending(options: { actorId?: string; inFlightRunId?: string }): ActorChildCompletion[] {
    let files: string[];
    try { files = this.#scanFiles(); } catch { return []; }
    // Intent is independent of archival: neither an envelope nor even a result
    // file need exist when a foreground publication is cancelled. Reconcile these
    // under the claim lock on restart, including muted outcomes with no envelope.
    for (const file of files) {
      if (!file.endsWith(".abandon") || !ID.test(file.slice(0, -8))) continue;
      try { this.#rollbackAbandoned(file.slice(0, -8)); }
      catch { /* Keep the exact intent and publication fence for the next poll. */ }
    }
    const envelopes = files.filter((file) => file.endsWith(".json") && ID.test(file.slice(0, -5)));
    const ids = new Set(envelopes.map((file) => file.slice(0, -5)));
    for (const id of this.#checked.keys()) if (!ids.has(id)) this.#checked.delete(id);
    const eligible: ActorChildCompletion[] = [];
    for (const file of envelopes) {
      const id = file.slice(0, -5);
      try {
        this.#rollbackAbandoned(id);
        if (this.received(id)) {
          // An unpublished foreground fence can still be abandoned. Retain its envelope.
          if (this.#readSpoolJson(this.#receipt(id)).publication) continue;
          fs.rmSync(this.#file(id), { force: true });
          this.#checked.delete(id);
          continue;
        }
        const value = this.#readSpoolJson(this.#file(id)) as ActorChildCompletion;
        if (value.format !== 1 || value.result?.id !== id || value.spawner?.kind !== "actor") continue;
        if (options.actorId && value.spawner.id !== options.actorId) continue;
        // Crucially this precedes the session read, not merely mailbox enqueue.
        if (options.inFlightRunId && value.spawner.runId === options.inFlightRunId) continue;
        eligible.push(value);
        if (eligible.length === 100) break;
      } catch { /* Retry unread envelopes on the next owner poll. */ }
    }
    const unchecked = eligible.filter(({ result }) => !this.#checked.has(result.id));
    if (unchecked.length) {
      const committed = this.#committedIds();
      for (const { result } of unchecked) this.#checked.set(result.id, committed.has(result.id));
    }
    return eligible.filter(({ result }) => {
      if (this.#checked.get(result.id)) {
        try { this.acknowledge(result.id); } catch { /* Retry cleanup without rescanning the session. */ }
        return false;
      }
      return !this.received(result.id);
    });
  }

  /** On the existing retention sweep, bound unread archives and receipt tombstones. */
  prune(retentionMs: number, now = Date.now(), keepIds: ReadonlySet<string> = new Set()): void {
    // smarty-dev#7916: each file's owned stat is taken at judgment; only that inode is removed.
    const groups = new Map<string, { files: { file: string; stat: fs.Stats | undefined }[]; newest: number }>();
    try {
      for (const name of this.#scanFiles()) {
        const id = STORED_FILE.exec(name)?.[1];
        if (!id || keepIds.has(id)) continue;
        const file = path.join(this.directory, name);
        const group = groups.get(id) ?? { files: [], newest: 0 };
        // One stat for both the age and the removal (pi-fabric#850 round 1 SEC P1): a second stat could describe a
        // swapped-in older inode and make a fresh group look expired. Without an owned stat (a link, hard-linked or
        // not ours) Linux removes nothing; off Linux main's age stat and removal stay (smarty-dev#7858).
        const stat = ownedStat(file);
        group.files.push({ file, stat });
        group.newest = Math.max(group.newest, stat?.mtimeMs ?? (process.platform === "linux" ? Infinity : fs.statSync(file).mtimeMs));
        groups.set(id, group);
      }
    } catch { return; }
    for (const [id, group] of groups) {
      if (now - group.newest < retentionMs) continue;
      try {
        let all = true;
        for (const { file, stat } of group.files) {
          // No owned stat: refused on Linux; off Linux main's removal runs unchanged (smarty-dev#7858).
          if (!verifiedRemove(file, stat, { deleter: "childCompletionsPrune",
            fallback: () => { fs.rmSync(file, { force: true }); return true; } }).removed) all = false;
        }
        if (all) this.#checked.delete(id);
      } catch { /* Best effort, like actor run retention. */ }
    }
  }

  /** Admission uses the same newest-file TTL as pruning, without deleting active context. */
  retained(id: string, retentionMs: number, now = Date.now()): boolean {
    if (!ID.test(id)) return false;
    try {
      // A handoff must still have its full outcome. Missing/unreadable evidence
      // cannot authorize restored context while asynchronous maintenance is pending.
      let newest = fs.statSync(this.resultFile(id)).mtimeMs;
      for (const suffix of [".json", ".receipt", ".live-receipt"]) {
        try { newest = Math.max(newest, fs.statSync(path.join(this.directory, `${id}${suffix}`)).mtimeMs); }
        catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") return false; }
      }
      return now - newest < retentionMs;
    } catch { return false; }
  }

  resultFile(id: string): string {
    if (!ID.test(id)) throw new Error("Invalid actor child completion id");
    return path.join(this.directory, `${id}.result.json`);
  }

  received(id: string): boolean {
    if (!ID.test(id)) return false;
    return this.#withClaim(() => this.#received(id));
  }

  #received(id: string): boolean {
    let receipt: { batchId?: string; unread?: boolean };
    try { receipt = this.#readSpoolJson(this.#receipt(id)); } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") return false;
      throw error; // Unreadable consumption evidence is not evidence of non-consumption.
    }
    return !receipt.unread && (!receipt.batchId || (ID.test(receipt.batchId) && fs.existsSync(this.#batchReceipt(receipt.batchId))));
  }
  #file(id: string): string { return path.join(this.directory, `${id}.json`); }
  #receipt(id: string): string { return path.join(this.directory, `${id}.receipt`); }
}
