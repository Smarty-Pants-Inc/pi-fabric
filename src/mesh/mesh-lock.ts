import { createLockStats, type MeshLockClass } from "./commit-stats.js";
import { MeshLockTicket } from "./lock-queue.js";
import { AsyncLocalStorage } from "node:async_hooks";
import { retryDelayMs } from "../core/retry-backoff.js";
import type { MeshLockProtocol } from "../config.js";
import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import {
  alarmForeignHolder, MeshLockForeignHolderError, MeshLockTimeoutError, ownProcessIncarnation, processIncarnation, validProcessIncarnation, type StaleLockNote,
} from "../core/atomic-write.js";
import { FOREIGN_LOCK, pinFenceLock, releaseFenceLock } from "./fence-lock.js";

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

const LOCK_TIMEOUT_MS = 10_000;
const STALE_LOCK_MS = 30_000;

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
   * but a busy mesh throws its typed timeout after a short try so the caller can
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
  // `bounded` marks an explicit try (lock stats only); a scoped withTryLock is one too. A
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
    const lockWaitStart = lockStats ? performance.now() : 0;
    let lockHeldAt = -1;
    let holdMs = -1;
    let failedWaitMs = -1;
    this.#writeAbortSignal?.throwIfAborted();
    fs.mkdirSync(this.root, { recursive: true, mode: 0o700 });
    // A registry-fenced publisher gets only a short try, never the ordinary wait.
    // Async-local scope leaves concurrent ordinary callers on their own budget.
    const scope = this.#tryLockScope.getStore();
    const budget = scope?.active ? Math.min(scope.timeoutMs, lockTimeoutMs) : lockTimeoutMs;
    const ownerPath = path.join(this.#lockPath, "owner");
    if (this.#lockProtocol >= 2 && !this.#ownIncarnationReady &&
      (scope?.active || lockTimeoutMs < this.#lockTimeoutMs)) {
      // Fails closed before the common accounting below: count it once here.
      lockStats?.failed(this.root, lockClass, performance.now() - lockWaitStart, Boolean(scope?.active) || bounded);
      throw new MeshLockTimeoutError(describeLockHolder(ownerPath), 0, 0);
    }
    const startTime = this.#lockProtocol >= 2
      ? this.#ownIncarnationReady ? this.#ownStartTime : await this.#ownIncarnation
      : undefined;
    const deadline = Date.now() + Math.min(this.#lockTimeoutMs, Math.max(0, budget));
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
        if (fs.readFileSync(ownerPath, "utf8") === ownerRecord) {
          // Detach the complete owned directory before unlinking anything inside it.
          // Interrupted/resumed recursive cleanup must never follow the canonical name.
          const released = `${this.#lockPath}.released.${token}`;
          fs.renameSync(this.#lockPath, released);
          fs.rmSync(released, { recursive: true, force: true });
        }
      } catch {
        // Already replaced/removed, unreadable, or cleanup failed: never delete canonical.
      }
    };
    const ticket = new MeshLockTicket(this.root, token, deadline - Date.now());
    try {
      // Attempts and the largest gap between two of them: a large gap means this waiter stalled
      // (no CPU); many attempts with small gaps mean it kept losing the race (smarty-dev#816).
      let attempts = 0;
      let maxGapMs = 0;
      let lastAttemptAt = Date.now();
      let retryAttempt = 0;
      while (true) {
        this.#writeAbortSignal?.throwIfAborted();
        if (!ticket.mayContend()) {
          if (Date.now() >= deadline) throw new MeshLockTimeoutError(describeLockHolder(ownerPath), attempts, maxGapMs);
          // Woken when the predecessor's receipt goes; the short poll is only a safety net.
          await ticket.wait(Math.min(20, Math.max(0, deadline - Date.now())), this.#writeAbortSignal);
          continue;
        }
        const attemptAt = Date.now();
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
              fs.rmSync(staging, { recursive: true, force: true });
            }
          }
          break;
        } catch (error) {
          const code = errorCode(error);
          if (code !== "EEXIST" && (this.#lockProtocol === 1 ||
            (code !== "ENOTEMPTY" && code !== "EPERM" && code !== "EACCES"))) throw error;
          if (await this.#clearStaleLock(ownerPath, deadline, blocked)) continue;
          if (Date.now() >= deadline) {
            // The holder was not reclaimed because its namespace is not ours (or unprovable): say so, typed.
            if (blocked.verdict) {
              const refused = new MeshLockForeignHolderError(describeLockHolder(ownerPath), attempts, maxGapMs, blocked.verdict,
                blocked.namespace ?? "", this.#lockPath, blocked.token ?? "");
              alarmForeignHolder(refused);
              throw refused;
            }
            throw new MeshLockTimeoutError(describeLockHolder(ownerPath), attempts, maxGapMs);
          }
          // Only the FIFO head probes promptly, woken when .lock is released. After the bounded
          // admission fallback, full jitter spreads plain contenders; the deadline bounds every sleep.
          if (ticket.queued) await ticket.wait(Math.min(10, Math.max(0, deadline - Date.now())), this.#writeAbortSignal);
          else await delay(retryDelayMs(retryAttempt++, 20, 250, deadline - Date.now()), this.#writeAbortSignal);
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
      if (lockStats) lockHeldAt = performance.now();
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
        if (lockStats) holdMs = performance.now() - lockHeldAt;
      }
    } catch (error) {
      if (lockStats && lockHeldAt < 0 && error instanceof MeshLockTimeoutError) failedWaitMs = performance.now() - lockWaitStart;
      throw error;
    } finally {
      ticket.close();
      // Recorded after the ticket closes, so bookkeeping never delays a FIFO follower.
      if (holdMs >= 0) lockStats?.acquired(this.root, lockClass, lockHeldAt - lockWaitStart, holdMs);
      else if (failedWaitMs >= 0) lockStats?.failed(this.root, lockClass, failedWaitMs, Boolean(scope?.active) || bounded);
    }
  }

  // Complete dead/different-incarnation receipts recover immediately. Empty ownerless
  // directories recover only after the grace, using atomic rmdir (never recursive removal
  // or rename): an owner published after our last comparison makes rmdir fail closed.
  // Torn/corrupt receipts and nonempty unrecorded directories remain protected.
  // smarty-dev#9569: a protocol-3 record names its holder's PID namespace; pid evidence (ESRCH, another incarnation)
  // is used only for a holder of OUR namespace. A foreign or unprovable one is never reclaimed here (`note` says why).
  async #clearStaleLock(ownerPath: string, deadline: number, note: StaleLockNote = {}): Promise<boolean> {
    delete note.verdict; delete note.namespace; delete note.token; // describes the CURRENT holder only
    try {
      const stat = fs.lstatSync(this.#lockPath);
      if (!stat.isDirectory()) return false;
      const readOwner = (): string | undefined => {
        try { return fs.readFileSync(ownerPath, "utf8"); }
        catch (error) { if (errorCode(error) === "ENOENT") return undefined; throw error; }
      };
      const owner = readOwner();
      if (owner === undefined) {
        if (Date.now() - stat.mtimeMs <= this.#staleLockMs) return false;
        const current = fs.lstatSync(this.#lockPath);
        if (!current.isDirectory() || current.dev !== stat.dev || current.ino !== stat.ino ||
          current.mtimeMs !== stat.mtimeMs || readOwner() !== undefined) return false;
        // Native emptiness is the final fence, including against an initializer that
        // publishes and enters while this recoverer is paused at the removal syscall.
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
      // pi-fabric#923 (fabric-store decisions): under protocol 3 a record without a namespace (a v1/v2 holder) is
      // UNPROVEN: its pid may name nothing here although it is alive in another namespace. There is no previous-boot
      // inference either (VM clones share a machine id): such holders are recovered only by the operator.
      if (recordNamespace === null && this.#lockProtocol === 3) {
        note.verdict = "unproven"; note.namespace = ""; note.token = token!;
        return false;
      }
      if (recordNamespace !== null) {
        const namespaces = await import("../core/pid-namespace.js");
        const own = await namespaces.ownNamespaceForReclaim(deadline, Boolean(this.#tryLockScope.getStore()?.active));
        const verdict = namespaces.pidNamespaceVerdict(recordNamespace, own);
        if (verdict !== "pid") { note.verdict = verdict; note.namespace = recordNamespace; note.token = token!; return false; }
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
      const fence = `${this.#lockPath}.dead.${createHash("sha256").update(`${stat.dev}:${stat.ino}:${owner}`).digest("hex")}`;
      // ponytail: retain this tiny nonempty directory permanently. A paused old cleaner
      // cannot rename a successor over the same fence (native EEXIST/ENOTEMPTY). Deleting
      // it, or recursively deleting the canonical name after a re-read, reopens that race.
      fs.renameSync(this.#lockPath, fence);
      return true;
    } catch {
      return false;
    }
  }
}
