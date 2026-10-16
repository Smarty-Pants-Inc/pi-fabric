import { createLockEvents, createLockStats, lockHeldEventThreshold, type LockEventStore, type MeshLockClass } from "./commit-stats.js";
import { meshLockQueueDirectory, MeshLockTicket } from "./lock-queue.js";
import { AsyncLocalStorage } from "node:async_hooks";
import { retryDelayMs } from "../core/retry-backoff.js";
import type { MeshLockProtocol } from "../config.js";
import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import {
  alarmForeignHolder, MeshIncarnationPendingError, MeshLockForeignHolderError, MeshLockTimeoutError, ownProcessIncarnation, processIncarnation, validProcessIncarnation, type StaleLockNote,
} from "../core/atomic-write.js";
import { FOREIGN_LOCK, pinFenceLock, releaseFenceLock } from "./fence-lock.js";
import { verifiedRemove } from "../storage/verified-remove.js";

// The mesh `.lock` domain (smarty-dev#6477 L0): acquisition (withLock, withTryLock), lock tickets,
// timeouts, the L8 timing hook and recovery of stale owners. State (state-file.ts) and events
// (event-log.ts) take this lock; neither owns it.
// Lock order, one direction only (smarty-dev#6477 R20): actor registries (sorted path) first, then
// the state transaction. On the file backend the state transaction is this mesh `.lock`: the second
// and innermost lock, whose critical sections are synchronous. On SQLite, state has its own
// transaction: `.lock` may then be held around it (the bridge's R20 write fence, a single
// synchronous `BEGIN IMMEDIATE` attempt inside an event append), never taken inside it, except by
// migration tools. A SQLite transaction is one synchronous segment and `.lock` is only acquired
// asynchronously, so the reverse order cannot occur in this process.

// Mesh-lock wait/hold by caller class under <root>/lock-stats (smarty-dev#6477 L8). On unless
// PI_FABRIC_LOCK_STATS=0; no extra lock, no fsync, and no timer or file before an acquisition.
export const lockStats = createLockStats();
// Held-time and wait-timeout events under <root>/lock-stats/events.jsonl (smarty-dev#9290), with the
// recorder: rare appends only, nothing on an ordinary acquisition.
export const lockEvents = createLockEvents();
const timed = lockStats !== undefined || lockEvents !== undefined;

/** The pid a `.lock` owner record names (its second line), when readable. */
export const lockOwnerPid = (ownerPath: string): number | undefined => {
  try {
    const pid = Number(fs.readFileSync(ownerPath, "utf8").split("\n")[1]);
    return Number.isSafeInteger(pid) && pid > 0 ? pid : undefined;
  } catch { return undefined; }
};

const LOCK_TIMEOUT_MS = 10_000;
const STALE_LOCK_MS = 30_000;

/** A wait budget exhausted before FIFO admission, not a failed acquisition of the canonical lock. */
export class MeshLockQueueExpiredError extends MeshLockTimeoutError {
  readonly preLockCode = "FABRIC_MESH_LOCK_QUEUE_EXPIRED";
  readonly kind = "pre-lock";
  readonly retryable = true;
  constructor(readonly lockPath: string, readonly waitersAhead: number | undefined, readonly waitedMs: number) {
    super("", 0, 0);
    this.name = "MeshLockQueueExpiredError";
    this.message = `${this.preLockCode} (code ${this.code}): Fabric mesh lock ${lockPath} expired in the FIFO queue before its turn, ` +
      `${waitersAhead === undefined ? "waiter count unavailable" : `${waitersAhead} waiters ahead`}, waited ${waitedMs} ms`;
  }
}

// Diagnostic snapshot only: admission remains MeshLockTicket's decision. Count matching receipts before our exact
// token; a concurrent removal or an unreadable directory makes the count unavailable, never an invented number.
const queueWaitersAhead = (root: string, token: string): number | undefined => {
  try {
    const receipts = fs.readdirSync(meshLockQueueDirectory(root)).filter(name => /^\d{24}-\d+-[a-f0-9-]+$/.test(name)).sort();
    const index = receipts.findIndex(name => name.endsWith(`-${process.pid}-${token}`));
    return index < 0 ? undefined : index;
  } catch { return undefined; }
};

/** Both canonical mesh and SQLite writers start relative waits after ticket setup. Expiry before any acquisition
 * stays a FIFO refusal: close admission before emitting its separate event, never a contention counter/event. */
export const createMeshLockAdmission = (root: string, token: string, budgetMs: number,
  options: { passStalledHead?: boolean; stalledHeadBypassLock?: string } = {}, now: () => number = Date.now) => {
  const ticket = new MeshLockTicket(root, token, budgetMs, options);
  const startedAt = now();
  return {
    ticket, startedAt, deadline: startedAt + Math.max(0, budgetMs),
    expire(lockPath: string, eventRoot: string, store: LockEventStore, lockClass: MeshLockClass): MeshLockQueueExpiredError {
      const refusal = new MeshLockQueueExpiredError(lockPath, queueWaitersAhead(root, token), Math.max(0, now() - startedAt));
      ticket.close();
      lockEvents?.queueExpired(eventRoot, store, lockClass, refusal.waitedMs, refusal.waitersAhead);
      return refusal;
    },
  };
};

// Keep the normal global timer seam (including diagnostic/test clocks). An
// aborted lifetime clears its referenced retry timer instead of awaiting the lock.
export const delay = (milliseconds: number, signal?: AbortSignal): Promise<void> =>
  new Promise((resolve, reject) => {
    signal?.throwIfAborted();
    const onAbort = (): void => {
      clearTimeout(timer);
      signal?.removeEventListener("abort", onAbort);
      reject(signal!.reason);
    };
    const timer = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, milliseconds);
    signal?.addEventListener("abort", onAbort, { once: true });
    if (signal?.aborted) onAbort();
  });

export const errorCode = (error: unknown): string | undefined =>
  error instanceof Error && "code" in error && typeof error.code === "string"
    ? error.code
    : undefined;

const PROCESS_STATES: Record<string, string> = {
  R: "running", S: "sleeping", D: "uninterruptible I/O wait", T: "stopped", t: "stopped by tracer",
  Z: "zombie", X: "dead", I: "idle",
};


// Names the holder in a lock timeout. A live holder is never taken over (a resumed
// holder could commit stale state), so a stuck one must be found and restarted from
// outside; tonight's stopped holder (smarty-dev#266) only showed up as expired sessions.
// ponytail: the process state comes from Linux /proc; other platforms report the PID only.
export const describeLockHolder = (ownerPath: string): string => {
  let owner: string;
  try {
    owner = fs.readFileSync(ownerPath, "utf8");
  } catch {
    return " (lock directory has no owner record)";
  }
  const [, pidText, createdText] = owner.trim().split("\n");
  const pid = Number(pidText);
  if (!Number.isSafeInteger(pid) || pid <= 0) return " (lock owner record is unreadable)";
  const createdAt = Number(createdText);
  const held = Number.isFinite(createdAt) ? ` for ${Math.max(0, Math.round((Date.now() - createdAt) / 1000))} s` : "";
  if (!processAlive(pid)) return ` held by pid ${pid} (not running)${held}`;
  let state = "";
  try {
    const stat = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
    const code = stat.slice(stat.lastIndexOf(")") + 2).charAt(0);
    if (code) state = `, state ${code}${PROCESS_STATES[code] ? ` ${PROCESS_STATES[code]}` : ""}`;
  } catch {
    // No /proc: report the PID only.
  }
  return ` held by pid ${pid} (alive${state})${held}`;
};

const processAlive = (pid: number): boolean => {
  if (!Number.isSafeInteger(pid) || pid <= 0) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    // Only the native no-such-process result proves death. Permission denial and
    // unexpected/unknown probe failures must not authorize detaching a live holder.
    return errorCode(error) !== "ESRCH";
  }
};

/** Shared by the MeshStore facade with its domains: the lock and the store's fixed bounds. */
export interface MeshStoreContext {
  readonly root: string;
  readonly maxEventBytes: number;
  readonly maxReadEvents: number;
  readonly lock: MeshLock;
}

export interface MeshLockOptions {
  lockProtocol?: MeshLockProtocol;
  lockTimeoutMs?: number;
  writeSignal?: AbortSignal;
  staleLockMs?: number;
  /** A hold longer than this emits a `held` lock event (smarty-dev#9290). Default 1,000 ms; 0 disables. */
  lockHeldEventMs?: number;
}

export class MeshLock {
  readonly #lockPath: string;
  readonly #lockProtocol: MeshLockProtocol;
  readonly #ownIncarnation: Promise<string | undefined> | undefined;
  #ownIncarnationReady = false;
  #ownStartTime: string | undefined;
  #ownNamespace: string | undefined;
  readonly #tryLockScope = new AsyncLocalStorage<{ active: boolean; timeoutMs: number }>();
  readonly #lockTimeoutMs: number;
  readonly #writeAbortSignal: AbortSignal | undefined;
  readonly #staleLockMs: number;
  readonly #lockHeldEventMs: number;
  readonly #onOperationError: () => void;

  constructor(readonly root: string, options: MeshLockOptions, onOperationError: () => void) {
    this.#onOperationError = onOperationError;
    // ponytail: keep this tiny validation local; importing config's runtime adds eager graph edges.
    const lockProtocol = options.lockProtocol === undefined ? 1 : options.lockProtocol;
    if (lockProtocol !== 1 && lockProtocol !== 2 && lockProtocol !== 3) throw new Error("mesh.lockProtocol must be 1, 2 or 3");
    this.#lockProtocol = lockProtocol;
    // Prepare this immutable process identity once, at store construction rather
    // than inside a registry-fenced acquisition. A cold bounded try fails closed
    // until preparation finishes; the ordinary outside-custody recovery lane can
    // wait for it. UNKNOWN still publishes the conservative three-line receipt.
    // Protocol 3 also prepares this process's PID namespace (smarty-dev#9569).
    if (lockProtocol >= 2) {
      // The namespace reader is a lazy module (startup graph budget): loaded only by a protocol-3 store.
      const namespace = lockProtocol === 3 ? import("../core/pid-namespace.js").then(module => module.ownPidNamespace()) : undefined;
      this.#ownIncarnation = Promise.all([ownProcessIncarnation(), namespace]).then(([start, ns]) => {
        this.#ownStartTime = start;
        this.#ownNamespace = ns;
        this.#ownIncarnationReady = true;
        return start;
      }, () => { this.#ownIncarnationReady = true; return undefined; });
    }
    this.#writeAbortSignal = options.writeSignal;
    this.#lockPath = path.join(root, ".lock");
    this.#lockTimeoutMs = Math.max(100, Math.floor(options.lockTimeoutMs ?? LOCK_TIMEOUT_MS));
    this.#staleLockMs = Math.max(100, Math.floor(options.staleLockMs ?? STALE_LOCK_MS));
    this.#lockHeldEventMs = lockHeldEventThreshold(options.lockHeldEventMs);
  }

  get lockPath(): string { return this.#lockPath; }
  get lockProtocol(): MeshLockProtocol { return this.#lockProtocol; }
  get lockTimeoutMs(): number { return this.#lockTimeoutMs; }
  get writeAbortSignal(): AbortSignal | undefined { return this.#writeAbortSignal; }
  get tryLockScope(): AsyncLocalStorage<{ active: boolean; timeoutMs: number }> { return this.#tryLockScope; }
  get ownIncarnation(): Promise<string | undefined> | undefined { return this.#ownIncarnation; }
  get ownIncarnationReady(): boolean { return this.#ownIncarnationReady; }

  /** Bound every mesh acquisition in this async step via the existing FIFO/try path.
   * Registry -> mesh publication retains custody through source selection and copies,
   * but a busy or unprepared mesh throws its typed refusal after a short try so the caller can
   * release registry fences and retry the whole step. This acquires NO lock itself.
   * Reset the shared scope receipt on exit: escaped async work must not inherit it. */
  async withTryLock<T>(operation: () => Promise<T>, timeoutMs = 0): Promise<T> {
    // Nested helpers retain the surrounding try budget and its lifetime.
    const inherited = this.#tryLockScope.getStore();
    if (inherited?.active) return operation();
    const scope = { active: true, timeoutMs: Math.max(0, timeoutMs) };
    return this.#tryLockScope.run(scope, async () => {
      try { return await operation(); }
      finally { scope.active = false; }
    });
  }

  // Global actor-custody order: actor registries (sorted path), then mesh.
  // Mesh critical sections are synchronous: never await a registry mutation
  // here or wrap resident async controls in this second/innermost lock.
  // `bounded` marks an explicit try (preparation refusal and lock stats); a scoped withTryLock is one too. A
  // reduced remaining budget from an ordinary caller is never a try.
  withLock<T>(operation: () => T, lockTimeoutMs = this.#lockTimeoutMs, lockClass: MeshLockClass = "other",
    bounded = false): Promise<T> {
    return this.#withLock(operation, lockTimeoutMs, lockClass, bounded, false);
  }

  /**
   * Holds `.lock` until the asynchronous operation settles. Only for a dedicated migration process
   * (the backend cutover holds it across its writer census, smarty-dev#6477 R1); never in a session,
   * whose mesh critical sections stay synchronous.
   */
  withLockAcrossAwait<T>(operation: () => Promise<T>, lockTimeoutMs = this.#lockTimeoutMs,
    lockClass: MeshLockClass = "other"): Promise<T> {
    return this.#withLock(operation, lockTimeoutMs, lockClass, false, true);
  }

  async #withLock<T>(operation: () => T | Promise<T>, lockTimeoutMs: number, lockClass: MeshLockClass,
    bounded: boolean, acrossAwait: boolean): Promise<T> {
    const lockWaitStart = timed ? performance.now() : 0;
    let lockHeldAt = -1;
    let holdMs = -1;
    let failedWaitMs = -1;
    this.#writeAbortSignal?.throwIfAborted();
    // A registry-fenced publisher gets only a short try, never the ordinary wait.
    // Async-local scope leaves concurrent ordinary callers on their own budget.
    const scope = this.#tryLockScope.getStore();
    const budget = scope?.active ? Math.min(scope.timeoutMs, lockTimeoutMs) : lockTimeoutMs;
    if (this.#lockProtocol >= 2 && !this.#ownIncarnationReady &&
      (bounded || scope?.active || lockTimeoutMs < this.#lockTimeoutMs)) {
      // Fail closed without awaiting native preparation under custody. This never tried .lock:
      // no owner probe, failed-acquisition stats, or wait-timeout event belongs to it.
      throw new MeshIncarnationPendingError();
    }
    fs.mkdirSync(this.root, { recursive: true, mode: 0o700 });
    const ownerPath = path.join(this.#lockPath, "owner");
    const startTime = this.#lockProtocol >= 2
      ? this.#ownIncarnationReady ? this.#ownStartTime : await this.#ownIncarnation
      : undefined;
    const waitMs = Math.min(this.#lockTimeoutMs, Math.max(0, budget));
    const token = randomUUID();
    // Protocol 3 (smarty-dev#9569): the start line is always present (empty: unknown) and the PID namespace follows
    // on its own line (empty: unknown, which no reclaimer may treat as its own). Six fields: a reader from before
    // protocol 3 accepts only 4 or 5, so it can never reclaim this record.
    const ownerRecord = this.#lockProtocol === 3
      ? `${token}\n${process.pid}\n${Date.now()}\n${startTime ?? ""}\n${this.#ownNamespace ?? ""}\n`
      : `${token}\n${process.pid}\n${Date.now()}\n${startTime ? `${startTime}\n` : ""}`;
    const blocked: StaleLockNote = {};
    // smarty-dev#8777: release through the directory this holder pinned at acquisition (the publication
    // fence's fd-pinned release). A check-then-rename by name let a holder that stalled past staleMs, and was
    // reclaimed, remove its successor's lock. Before a pin exists, and off Linux, the release is main's below.
    let pin: number | undefined;
    const releaseOwned = (): void => {
      const held = pin;
      pin = undefined;
      if (held !== undefined) { releaseFenceLock(this.#lockPath, ownerRecord, {}, held); return; }
      // Non-Linux keeps main's release; the stalled-holder race there is tracked in smarty-dev#9569.
      try {
        // smarty-dev#8907: the directory judged by its owner record, stat captured before that read; only that inode
        // is removed (detached into verifiedRemove's graveyard first). A swapped-in successor is refused, never removed.
        const judged = fs.lstatSync(this.#lockPath);
        if (fs.readFileSync(ownerPath, "utf8") === ownerRecord) {
          verifiedRemove(this.#lockPath, judged, { deleter: "meshLockRelease", fallback: () => {
            // Off Linux, main's release: detach the complete owned directory before unlinking anything inside it.
            // Interrupted/resumed recursive cleanup must never follow the canonical name.
            const released = `${this.#lockPath}.released.${token}`;
            fs.renameSync(this.#lockPath, released);
            fs.rmSync(released, { recursive: true, force: true });
            return true;
          } });
        }
      } catch {
        // Already replaced/removed, unreadable, or cleanup failed: never delete canonical.
      }
    };
    // smarty-dev#9805: only the canonical mesh lock opts into the early free-lock bypass of a stalled head.
    const admission = createMeshLockAdmission(this.root, token, waitMs, { stalledHeadBypassLock: this.#lockPath });
    const { ticket, deadline } = admission;
    let budgetSpent = false;
    let budgetTimer: ReturnType<typeof setTimeout> | undefined;
    // The shared admission helper fixes the relative budget only after setup; expiry cannot grant admission.
    try {
      // Attempts and the largest gap between two of them: a large gap means this waiter stalled
      // (no CPU); many attempts with small gaps mean it kept losing the race (smarty-dev#816).
      let attempts = 0;
      let maxGapMs = 0;
      let lastAttemptAt = Date.now();
      let retryAttempt = 0;
      let waited = false;
      let waitSetupExpired = false;
      const wait = async (ms: number): Promise<void> => {
        // Arm once, before watch setup, on the same relative timer clock as both wait paths.
        // Date is lease time (it may jump); performance may be a separate diagnostic clock.
        // Lazy arming keeps uncontended acquisitions free of another clock read or timer.
        // Spent means strictly past the budget (+1 ms), so an attempt due exactly at the deadline still runs.
        budgetTimer ??= setTimeout(() => { budgetSpent = true; }, Math.max(0, deadline - Date.now()) + 1);
        const pending = ticket.queued
          ? ticket.wait(ms, this.#writeAbortSignal, deadline)
          : delay(ms, this.#writeAbortSignal);
        // Invocation includes synchronous watch setup: retain its fixed-deadline refusal.
        // After yielding, wall-clock lease time can jump independently of the relative timer.
        waitSetupExpired = Date.now() > deadline;
        await pending;
        waited = true;
      };
      const timeout = (): MeshLockTimeoutError => {
        // Keep a foreign-holder refusal typed even when the deadline is crossed during a wait.
        if (blocked.verdict) {
          const refused = new MeshLockForeignHolderError(describeLockHolder(ownerPath), attempts, maxGapMs, blocked.verdict,
            blocked.namespace ?? "", this.#lockPath, blocked.token ?? "");
          alarmForeignHolder(refused);
          return refused;
        }
        return new MeshLockTimeoutError(describeLockHolder(ownerPath), attempts, maxGapMs);
      };
      while (true) {
        this.#writeAbortSignal?.throwIfAborted();
        if (!ticket.mayContend()) {
          if (!budgetSpent && Date.now() < deadline) {
            // Woken when the predecessor's receipt goes; the short poll is only a safety net.
            await wait(Math.min(20, Math.max(0, deadline - Date.now())));
            continue;
          }
          // Expiry never grants FIFO admission: only an eligible head may probe at its deadline. Keep the legacy
          // retry code, but report this zero-acquisition refusal separately from real lock contention.
          throw admission.expire(this.#lockPath, this.root, "mesh-lock", lockClass);
        }
        const attemptAt = Date.now();
        // Both waits can spend the budget in setup or resume late, even on a release signal.
        // Keep the initial eligible probe; once we have waited, timer expiry is not admission.
        if (waited && (waitSetupExpired || budgetSpent)) {
          if (attempts === 0 && ticket.queued) throw admission.expire(this.#lockPath, this.root, "mesh-lock", lockClass);
          throw timeout();
        }
        if (attempts > 0) maxGapMs = Math.max(maxGapMs, attemptAt - lastAttemptAt);
        attempts += 1;
        lastAttemptAt = attemptAt;
        try {
          if (this.#lockProtocol === 1) {
            // Keep the B68 three-line wire, but never overwrite an owner published by a
            // successor while this initializer was stopped after canonical mkdir.
            fs.mkdirSync(this.#lockPath, { mode: 0o700 });
            const ownershipLost = () => Object.assign(new Error("Fabric mesh lock ownership lost during acquisition"), {
              code: "FABRIC_MESH_LOCK_OWNERSHIP_LOST",
            });
            try {
              const directory = fs.lstatSync(this.#lockPath);
              fs.writeFileSync(ownerPath, ownerRecord, {
                encoding: "utf8", flag: "wx", mode: 0o600,
              });
              // The exclusive create may itself have paused with an open descriptor to a
              // recovered directory. Prove publication still belongs to the canonical lock
              // before entering the critical section; never clean a successor on failure.
              const current = fs.lstatSync(this.#lockPath);
              if (!current.isDirectory() || current.dev !== directory.dev || current.ino !== directory.ino ||
                fs.readFileSync(ownerPath, "utf8") !== ownerRecord) throw ownershipLost();
            } catch (error) {
              // A resumed initializer may have published into an empty replacement before
              // rejecting its directory identity. Remove only that attempt's exact receipt.
              releaseOwned();
              if (errorCode(error) === "EEXIST" || errorCode(error) === "ENOENT") throw ownershipLost();
              throw error;
            }
          } else {
            // Never expose an ownerless canonical directory: a stalled initializer must not
            // resume its owner write through a name that legacy recovery gave to a successor.
            const staging = fs.mkdtempSync(`${this.#lockPath}.pending.${token}.`);
            try {
              fs.writeFileSync(path.join(staging, "owner"), ownerRecord, {
                encoding: "utf8", flag: "wx", mode: 0o600,
              });
              // POSIX rename can replace an EMPTY directory, but a fresh ownerless legacy
              // lock may be an in-flight creator. Route every observed canonical path through
              // the original owner/stale checks instead of publishing over it.
              try {
                fs.lstatSync(this.#lockPath);
                throw Object.assign(new Error("Fabric mesh lock already exists"), { code: "EEXIST" });
              } catch (error) {
                if (errorCode(error) !== "ENOENT") throw error;
              }
              // New-format competitors publish nonempty owners atomically. This does not fence
              // old-format writers that create an empty canonical after the absence check.
              fs.renameSync(staging, this.#lockPath);
            } finally {
              // ponytail: no judgment precedes this. `staging` is this attempt's own unpublished mkdtemp name (random
              // suffix, never published under it, never a recovery or rename target); after a successful publish the
              // name is empty and `force` makes this a no-op. smarty-dev#8907 inventory.
              fs.rmSync(staging, { recursive: true, force: true });
            }
          }
          break;
        } catch (error) {
          const code = errorCode(error);
          if (code !== "EEXIST" && (this.#lockProtocol === 1 ||
            (code !== "ENOTEMPTY" && code !== "EPERM" && code !== "EACCES"))) throw error;
          if (await this.clearStaleLock(ownerPath, deadline, blocked)) continue;
          if (Date.now() >= deadline) throw timeout();
          // Only the FIFO head probes promptly, woken when .lock is released. After the bounded
          // admission fallback, full jitter spreads plain contenders; the deadline bounds every sleep.
          await wait(ticket.queued ? Math.min(10, Math.max(0, deadline - Date.now()))
            : retryDelayMs(retryAttempt++, 20, 250, deadline - Date.now()));
        }
      }
      pin = pinFenceLock(this.#lockPath, ownerRecord);
      // ponytail: fence-lock.ts stays main's code, so its verdicts are split here. On Linux the pin must hold:
      // FOREIGN_LOCK also comes from a failed open (EMFILE, EACCES...) and undefined from an unreadable
      // /proc/self/fd/<fd>/owner. If `<lock>/owner` is still OUR record we hold the lock we just created (no
      // successor can acquire while it is there): release it by name (main's release) and fail retryably; never
      // run the section unpinned on Linux. Not ours: reclaimed before entry, so release nothing.
      if (pin === FOREIGN_LOCK || (pin === undefined && process.platform === "linux")) {
        pin = undefined;
        let ours = false;
        try { ours = fs.readFileSync(ownerPath, "utf8") === ownerRecord; } catch { /* unreadable: not provably ours */ }
        if (ours) releaseOwned();
        throw new MeshLockTimeoutError(describeLockHolder(ownerPath), attempts, maxGapMs);
      }
      if (timed) lockHeldAt = performance.now();
      try {
        this.#writeAbortSignal?.throwIfAborted();
        return acrossAwait ? await operation() : operation();
      } catch (error) {
        // A failed write (a version conflict above all) means this store's view is behind: the
        // next read parses the file again instead of reusing a recent parse.
        this.#onOperationError();
        throw error;
      } finally {
        releaseOwned();
        if (timed) holdMs = performance.now() - lockHeldAt;
      }
    } catch (error) {
      if (timed && lockHeldAt < 0 && error instanceof MeshLockTimeoutError && !(error instanceof MeshLockQueueExpiredError)) {
        failedWaitMs = performance.now() - lockWaitStart;
      }
      throw error;
    } finally {
      clearTimeout(budgetTimer);
      ticket.close();
      // Recorded after the ticket closes, so bookkeeping never delays a FIFO follower.
      if (holdMs >= 0) {
        lockStats?.acquired(this.root, lockClass, lockHeldAt - lockWaitStart, holdMs);
        if (this.#lockHeldEventMs > 0 && holdMs > this.#lockHeldEventMs) lockEvents?.held(this.root, "mesh-lock", lockClass, holdMs);
      } else if (failedWaitMs >= 0) {
        const tried = Boolean(scope?.active) || bounded;
        lockStats?.failed(this.root, lockClass, failedWaitMs, tried);
        // The holder as its owner record names it now (it may have released since the timeout).
        if (!tried) lockEvents?.waitTimeout(this.root, "mesh-lock", lockClass, failedWaitMs, lockOwnerPid(ownerPath));
      }
    }
  }

  // A dead/other-incarnation complete receipt recovers at once, an empty ownerless directory after the grace (atomic
  // rmdir: a late owner fails it closed); torn receipts and nonempty unrecorded directories stay. smarty-dev#9569: pid
  // evidence only for OUR PID namespace (foreign/unprovable: `note`). smarty-dev#9501: `expected` = exact owner bytes
  // after the read; its namespace must come from the owner, never a mutable receipt.
  // `beforeRename` is synchronous, restartable retirement of that exact owner's dependent receipt, not another death
  // proof. It runs only AFTER the sole death/namespace/root proof and BEFORE detaching the canonical owner. A throw
  // keeps that owner available to reprove on rerun; a kill after the native rename cannot strand its FIFO ticket.
  async clearStaleLock(ownerPath: string, deadline: number, note: StaleLockNote = {},
    expected?: { owner: string; namespace: string; beforeRename?: () => void }): Promise<boolean> {
    delete note.verdict; delete note.namespace; delete note.token; // describes the CURRENT holder only
    let pin: { unchanged(): boolean; close(): void } | undefined;
    try {
      const stat = fs.lstatSync(this.#lockPath);
      if (!stat.isDirectory()) return false;
      const readOwner = (): string | undefined => {
        try { return fs.readFileSync(ownerPath, "utf8"); }
        catch (error) { if (errorCode(error) === "ENOENT") return undefined; throw error; }
      };
      const owner = readOwner();
      if (expected && owner !== expected.owner) return false;
      if (owner === undefined) {
        if (Date.now() - stat.mtimeMs <= this.#staleLockMs) return false;
        const current = fs.lstatSync(this.#lockPath);
        if (!current.isDirectory() || current.dev !== stat.dev || current.ino !== stat.ino ||
          current.mtimeMs !== stat.mtimeMs || readOwner() !== undefined) return false;
        // Native emptiness is the final fence, including against an initializer that
        // publishes and enters while this recoverer is paused at the removal syscall.
        // ponytail (smarty-dev#8907): atomic rmdir, not verifiedRemove. It removes only an EMPTY directory, so a
        // directory swapped in after this judgment loses no data and no custody: an ownerless directory grants none,
        // and a protocol-1 initializer enters only after its `wx` owner write and its lstat identity recheck of its own
        // mkdir, which fail (ENOENT / identity) if this rmdir took its directory. Any published owner makes it ENOTEMPTY.
        fs.rmdirSync(this.#lockPath);
        // Retain a nonempty recovery receipt, without ever renaming a live canonical.
        // Inode reuse may revisit the same receipt; that must not undo successful recovery.
        const fence = `${this.#lockPath}.dead.${createHash("sha256").update(`${stat.dev}:${stat.ino}:`).digest("hex")}`;
        fs.mkdirSync(fence, { recursive: true, mode: 0o700 });
        try { fs.writeFileSync(path.join(fence, ".recovery-fence"), "1\n", { flag: "wx", mode: 0o600 }); }
        catch (error) { if (errorCode(error) !== "EEXIST") throw error; }
        return true;
      }
      const fields = owner.split("\n");
      const [token, pidText, createdText, startText] = fields;
      // An in-flight/torn fourth line is not evidence of PID reuse. Protocol 3's start line may be empty (unknown).
      const recordedStart = owner?.endsWith("\n") && startText ? startText : undefined;
      const pid = Number(pidText);
      const validPid = Number.isSafeInteger(pid) && pid > 0;
      const validOwner = owner?.endsWith("\n") && (fields.length === 4 || fields.length === 5 || fields.length === 6) &&
        !!token && validPid && createdText !== undefined &&
        createdText.trim() !== "" && Number.isFinite(Number(createdText));
      if (!validOwner) return false;
      const recordNamespace = fields.length === 6 ? fields[4]! : null;
      // pi-fabric#923: under protocol 3 a record without a namespace (v1/v2 holder) is UNPROVEN (its pid may be alive in
      // another namespace); no previous-boot inference (VM clones share a machine id): operator recovery only.
      // A forged receipt can make a LIVE foreign legacy holder look dead locally; never upgrade its namespace.
      if (recordNamespace === null && (this.#lockProtocol === 3 || expected)) {
        note.verdict = "unproven"; note.namespace = ""; note.token = token!;
        return false;
      }
      const ns = recordNamespace;
      if (expected && ns !== expected.namespace) { note.verdict = "unproven"; note.namespace = ns ?? ""; note.token = token!; return false; }
      if (ns !== null) {
        const namespaces = await import("../core/pid-namespace.js");
        const own = await namespaces.ownNamespaceForReclaim(deadline, Boolean(this.#tryLockScope.getStore()?.active));
        // pid evidence only for our own namespace AND a host-local root (pi-fabric#923 round 3).
        const result = namespaces.reclaimVerdict(ns, own, this.root);
        pin = result.pin;
        if (result.verdict !== "pid") { note.verdict = result.verdict; note.namespace = ns; note.token = token!; return false; }
      }
      if (processAlive(pid)) {
        if (!validProcessIncarnation(recordedStart)) return false;
        // Registry-fenced publication cannot start even a bounded native read.
        // Fail closed; the ordinary outside-custody admission lane obtains fresh
        // holder evidence/recovery, then publication selects under fresh fences.
        if (this.#tryLockScope.getStore()?.active) return false;
        const remaining = deadline - Date.now();
        if (remaining <= 0) return false;
        let timer: ReturnType<typeof setTimeout> | undefined;
        let actualStart: string | undefined;
        try {
          // Race only the evidence read, NEVER the recovery operation. A reader
          // ignoring its native timeout cannot leave an escaped rename behind.
          // The native reader also receives this budget so its child is aborted.
          actualStart = await Promise.race([
            processIncarnation(pid, remaining),
            new Promise<undefined>(resolve => { timer = setTimeout(() => resolve(undefined), remaining); }),
          ]);
        } finally { clearTimeout(timer); }
        if (Date.now() >= deadline || !actualStart || actualStart === recordedStart) return false;
      }
      const unchanged = (): boolean => {
        const current = fs.lstatSync(this.#lockPath);
        return current.isDirectory() && current.dev === stat.dev && current.ino === stat.ino && readOwner() === owner;
      };
      if (!unchanged()) return false;
      // pi-fabric#923 round 5: the root checked as host-local must still be the root this path names, right now.
      if (pin && !pin.unchanged()) { note.verdict = "unproven"; note.namespace = ns ?? ""; note.token = token!; return false; }
      expected?.beforeRename?.();
      // The callback may have stalled at a native syscall: recheck the exact owner, directory and host-local root
      // immediately before detaching. This is the same proof's identity fence, never a second death decision.
      if (expected?.beforeRename) {
        if (!unchanged()) return false;
        if (pin && !pin.unchanged()) { note.verdict = "unproven"; note.namespace = ns ?? ""; note.token = token!; return false; }
      }
      const fence = `${this.#lockPath}.dead.${createHash("sha256").update(`${stat.dev}:${stat.ino}:${owner}`).digest("hex")}`;
      // ponytail: retain this tiny nonempty directory permanently. A paused old cleaner
      // cannot rename a successor over the same fence (native EEXIST/ENOTEMPTY). Deleting
      // it, or recursively deleting the canonical name after a re-read, reopens that race.
      fs.renameSync(this.#lockPath, fence);
      return true;
    } catch {
      return false;
    } finally { pin?.close(); }
  }
}
