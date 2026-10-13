import { randomUUID } from "node:crypto";
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
const WATCH_REARM_MS = 500;
const WATCH_REARM_MAX_MS = 30_000;
// A closed native handle normally reports `close`; an errored one may never do so.
const WATCH_CLOSE_GRACE_MS = 1_000;

/**
 * Edge-only wake for completion writers in other processes. It carries no authority:
 * the callback only schedules the owner's existing fresh ownership/claim reconcile.
 * Arm it BEFORE the first empty preflight so a later write cannot fall between them.
 * A failed watch triggers exactly one rescan and one re-arm on a single one-shot timer
 * (500 ms doubling, capped at 30 s). There is no standing poll and no attempt cap: a
 * delivered event proves the watch healthy and resets the backoff, so failures never
 * accumulate over the owner's lifetime into permanent loss of the wake.
 */
export class ActorChildCompletionWatch {
  #watcher: FSWatcher | undefined;
  #timer: NodeJS.Timeout | undefined;
  #failures = 0;
  #closed = false;
  // Watchers closed after an error whose native close may still be in flight.
  readonly #closing = new Set<Promise<void>>();
  constructor(readonly directory: string, readonly wake: () => void) { this.#arm(); }

  #arm(): void {
    this.#timer = undefined;
    if (this.#closed || this.#watcher) return;
    try {
      // Create the empty spool once (never recursively: a removed actor stays removed).
      try { fs.mkdirSync(this.directory, { mode: 0o700 }); }
      catch (error) { if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error; }
      const watcher = fs.watch(this.directory, { persistent: false }, (_event, filename) => {
        if (this.#closed || this.#watcher !== watcher) return;
        this.#failures = 0; // A delivering watch is healthy: the backoff never exhausts over a lifetime.
        const name = filename == null ? undefined : path.basename(filename.toString());
        // A nameless event is a wake, not evidence that nothing changed.
        if (name === undefined || WAKE_FILE.test(name)) this.wake();
      });
      watcher.on("error", () => this.#failed(watcher));
      this.#watcher = watcher;
      // Edges may have been missed while unarmed: rescan once after a successful re-arm.
      if (this.#failures) this.wake();
    } catch { this.#failed(undefined); }
  }

  #failed(watcher: FSWatcher | undefined): void {
    if (this.#closed || (watcher && this.#watcher !== watcher)) return;
    if (watcher) {
      this.#watcher = undefined;
      this.#closing.add(this.#closeHandle(watcher));
    }
    this.wake(); // Exactly one rescan per failure.
    if (this.#timer) return; // One one-shot re-arm at a time.
    this.#timer = setTimeout(() => this.#arm(), Math.min(WATCH_REARM_MAX_MS, WATCH_REARM_MS * 2 ** Math.min(this.#failures++, 16)));
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
  #claimDepth = 0;
  constructor(readonly sessionFile: string) {
    this.directory = path.join(path.dirname(sessionFile), "child-completions");
  }

  enqueue(result: AgentRunResult, spawner: AgentSpawner, notify = true): void {
    if (!ID.test(result.id) || spawner.kind !== "actor") throw new Error("Invalid actor child completion identity");
    this.#withClaim(() => {
      // Cancellation may precede both archive files. A failed rollback must keep
      // the source's archive obligation, not let its prepared receipt discharge it.
      this.#rollbackAbandoned(result.id);
      const received = this.received(result.id);
      const publication = received ? JSON.parse(fs.readFileSync(this.#receipt(result.id), "utf8")).publication : undefined;
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
      const publication = received ? JSON.parse(fs.readFileSync(this.#receipt(id), "utf8")).publication : undefined;
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
    const abandoned = JSON.parse(fs.readFileSync(file, "utf8"));
    let receipt;
    try { receipt = JSON.parse(fs.readFileSync(this.#receipt(id), "utf8")); }
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
      const receipt = JSON.parse(fs.readFileSync(this.#receipt(id), "utf8"));
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
    try { return JSON.parse(fs.readFileSync(path.join(this.directory, `${id}.consumed`), "utf8")).id === id; }
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
    try { files = fs.readdirSync(this.directory); } catch { return; }
    for (const name of files.filter(file => file.endsWith(".source") && ID.test(file.slice(0, -7))).slice(0, 100)) {
      try {
        const source = JSON.parse(fs.readFileSync(path.join(this.directory, name), "utf8"));
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
      return fs.readdirSync(this.directory).some(file => {
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
    try { files = fs.readdirSync(this.directory); } catch (error) {
      return (error as NodeJS.ErrnoException).code !== "ENOENT";
    }
    return files.some(file => {
      if (file.endsWith(".abandon")) return ID.test(file.slice(0, -".abandon".length));
      if (file.endsWith(".source")) return ID.test(file.slice(0, -".source".length));
      if (!file.endsWith(".json") || !ID.test(file.slice(0, -5))) return false;
      if (!options.actorId && !options.inFlightRunId) return true;
      try {
        const value = JSON.parse(fs.readFileSync(path.join(this.directory, file), "utf8")) as ActorChildCompletion;
        return value.format === 1 && value.spawner?.kind === "actor" &&
          (!options.actorId || value.spawner.id === options.actorId) &&
          (!options.inFlightRunId || value.spawner.runId !== options.inFlightRunId);
      } catch { return true; }
    });
  }

  #pending(options: { actorId?: string; inFlightRunId?: string }): ActorChildCompletion[] {
    let files: string[];
    try { files = fs.readdirSync(this.directory); } catch { return []; }
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
          if (JSON.parse(fs.readFileSync(this.#receipt(id), "utf8")).publication) continue;
          fs.rmSync(this.#file(id), { force: true });
          this.#checked.delete(id);
          continue;
        }
        const value = JSON.parse(fs.readFileSync(this.#file(id), "utf8")) as ActorChildCompletion;
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
      for (const name of fs.readdirSync(this.directory)) {
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
    try { receipt = JSON.parse(fs.readFileSync(this.#receipt(id), "utf8")); } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") return false;
      throw error; // Unreadable consumption evidence is not evidence of non-consumption.
    }
    return !receipt.unread && (!receipt.batchId || (ID.test(receipt.batchId) && fs.existsSync(this.#batchReceipt(receipt.batchId))));
  }
  #file(id: string): string { return path.join(this.directory, `${id}.json`); }
  #receipt(id: string): string { return path.join(this.directory, `${id}.receipt`); }
}
