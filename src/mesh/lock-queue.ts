import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { verifiedRemove } from "../storage/verified-remove.js";

/** Advisory receipts live beside, never inside, the mesh's data namespace.
 * Canonical paths make symlink aliases share admission; the mesh's parent (not
 * process-specific TMPDIR) keeps FIFO shared by all contenders for the same root.
 */
export function meshLockQueueDirectory(root: string): string {
  const canonical = fs.realpathSync(root);
  const key = createHash("sha256").update(canonical).digest("hex");
  return path.join(path.dirname(canonical), `.pi-fabric-mesh-lock-${process.getuid?.() ?? "user"}`, key);
}

const TICKET = /^\d{24}-\d+-[a-f0-9-]+$/;
// The head refreshes its receipt this often; the stall bound is at least four heartbeats.
const HEARTBEAT_MS = 50;

const pidAlive = (name: string): boolean => {
  try { process.kill(Number(name.split("-")[1]), 0); return true; }
  catch (error) { return (error as NodeJS.ErrnoException).code !== "ESRCH"; }
};

/** Advisory admission only: the unchanged .lock directory is the sole exclusion fence.
 * Fair admission (smarty-dev#6477 L0b): each waiter watches only its predecessor's receipt and
 * is woken (inotify/FSEvents via fs.watch, short poll as the safety net) the moment it goes; the
 * head is woken when .lock is released. The holder keeps its receipt until it releases, so the
 * receipt order is the admission order. A waiter falls back to the plain contest after
 * 80% of its budget only when the head has stopped making progress (its receipt heartbeat is
 * older than the stall bound), so live queued waiters do not barge the head. Old releases ignore
 * this queue and still barge. The caller's acquisition deadline is never extended, and a live
 * lock is never removed.
 */
export class MeshLockTicket {
  readonly #directory: string;
  readonly #root: string;
  readonly #name: string;
  readonly #file: string;
  readonly #fallbackAt: number;
  readonly #staleMs: number;
  readonly #stallMs: number;
  readonly #passStalledHead: boolean;
  readonly #bypassLock: string | undefined;
  #nextAfterHead = false;
  #queueHead: string | undefined;
  #predecessorMtimeMs = 0;
  #predecessorStat: fs.Stats | undefined;
  #observedBeat: string | undefined;
  #beatSeenAt = 0;
  #headCheckedAt = 0;
  #closed = false;
  #head = false;
  #predecessor: string | undefined;
  #touchedAt = 0;
  #observedHead: string | undefined;
  #observedAt = 0;
  #rootWatcher: fs.FSWatcher | undefined;
  #predecessorWatcher: fs.FSWatcher | undefined;
  #watched: string | undefined;
  #wake: (() => void) | undefined;
  #signalled = false;

  /** `passStalledHead` (smarty-dev#10516): pass a stalled head at any time, not only past 80% of the budget. Only for
   * a queue whose ticket closes when the lock is taken (SQLite state writes): there a stale head never holds the lock.
   * A `.lock` holder keeps its receipt through its critical section, so a long hold would look stalled.
   * `stalledHeadBypassLock` (smarty-dev#9805): the canonical `.lock` directory path. Only the waiter directly behind
   * the head may reap its receipt early, once its own observation of the head's name and mtime is older than the stall
   * bound and that path is absent. Reaping makes the follower the new FIFO head; a resumed head sees its missing ticket
   * and falls back to the plain contest, while mkdir/rename stays the only exclusion fence. */
  constructor(root: string, token: string, budgetMs: number,
    options: { passStalledHead?: boolean; stalledHeadBypassLock?: string } = {}) {
    this.#directory = meshLockQueueDirectory(root);
    this.#root = root;
    // hrtime is a host-wide monotonic clock, unlike a wall clock that can step backwards.
    this.#name = `${process.hrtime.bigint().toString().padStart(24, "0")}-${process.pid}-${token}`;
    this.#file = path.join(this.#directory, this.#name);
    this.#fallbackAt = Date.now() + Math.min(10_000, Math.max(0, budgetMs * 0.8));
    this.#staleMs = Math.max(30_000, budgetMs * 2);
    this.#stallMs = Math.min(2000, Math.max(4 * HEARTBEAT_MS, budgetMs * 0.1));
    this.#passStalledHead = options.passStalledHead === true;
    this.#bypassLock = options.stalledHeadBypassLock;
    const namespace = path.dirname(this.#directory);
    // No external sibling exists for a filesystem root. Admission is optional: a
    // read-only parent or unsafe namespace must not break otherwise writable meshes.
    const canonical = fs.realpathSync(root);
    if (path.dirname(canonical) === canonical) { this.#closed = true; return; }
    try {
      fs.mkdirSync(namespace, { recursive: true, mode: 0o700 });
      const stat = fs.lstatSync(namespace);
      if (!stat.isDirectory() || (process.getuid && stat.uid !== process.getuid()) ||
        (process.platform !== "win32" && (stat.mode & 0o077) !== 0)) { this.#closed = true; return; }
      fs.mkdirSync(this.#directory, { recursive: true, mode: 0o700 });
      fs.writeFileSync(this.#file, "", { flag: "wx", mode: 0o600 });
    } catch (error) {
      if (!["EACCES", "EPERM", "EROFS", "EEXIST", "ENOTDIR"].includes((error as NodeJS.ErrnoException).code ?? "")) throw error;
      this.#closed = true;
    }
  }

  /** True when this waiter may probe .lock: it is the head, or admission is closed/fell back.
   * Non-heads stat only their predecessor's receipt (never .lock/owner) and list the queue only
   * when that receipt goes, or once past the fallback point (with `passStalledHead`, at most once
   * per heartbeat interval at any time) to judge the head's progress. */
  mayContend(): boolean {
    if (this.#closed) return true;
    if (this.#head) { this.#heartbeat(); return true; }
    // An earlier head can disappear while our immediate predecessor stays. Rediscover its promotion instead of
    // caching index > 1 until that predecessor leaves; it may itself now be the stalled head (#9805).
    if (this.#bypassLock !== undefined && !this.#nextAfterHead && this.#queueHead !== undefined &&
      !fs.existsSync(path.join(this.#directory, this.#queueHead))) this.#predecessor = undefined;
    if (this.#predecessor !== undefined && this.#waitingOn(this.#predecessor)) {
      const held = this.#holdBack();
      if (held !== undefined) return held;
      // The stalled head was reaped. Rescan so this ticket can become the new FIFO head.
      this.#predecessor = undefined;
    }
    for (;;) {
      let names: string[];
      try { names = fs.readdirSync(this.#directory).filter(name => TICKET.test(name)).sort(); }
      catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; names = []; }
      const index = names.indexOf(this.#name);
      // A stale reaper may have removed our receipt while we were descheduled. Admission
      // is advisory, so continue the bounded plain contest rather than wait on a lost ticket.
      if (index < 0) { this.close(); return true; }
      if (index === 0) { this.#head = true; this.#predecessor = undefined; this.#heartbeat(true); return true; }
      this.#predecessor = names[index - 1]!;
      // Receipt names are host-monotonic, so a predecessor at index 0 stays head until its receipt goes (watched).
      this.#nextAfterHead = index === 1;
      this.#queueHead = names[0];
      if (this.#waitingOn(this.#predecessor)) {
        const held = this.#holdBack();
        if (held !== undefined) return held;
        // The stalled head was reaped. Rescan so this ticket can become the new FIFO head.
        this.#predecessor = undefined;
        continue;
      }
      // The predecessor was dead, stale, or reaped and is now unlinked: look again.
    }
  }

  /** False when the receipt is gone; a dead or stale one is reaped. Names are unique and never
   * reused, so unlink cannot remove a successor. smarty-dev#8907: only the receipt inode judged here, with its judged
   * mtime, is removed: one refreshed (a head's heartbeat) or replaced since stays, and is still waited on. */
  #waitingOn(name: string): boolean {
    const file = path.join(this.#directory, name);
    try {
      const stat = fs.lstatSync(file);
      if (pidAlive(name) && Date.now() - stat.mtimeMs <= this.#staleMs) {
        this.#predecessorMtimeMs = stat.mtimeMs;
        this.#predecessorStat = stat;
        return true;
      }
      this.#predecessorStat = undefined;
      // A canonical holder retains its advisory receipt even if its event loop has been stalled past staleMs. The
      // receipt can be reconsidered once the sole exclusion fence is gone; until then, fail closed and wait.
      if (this.#bypassLock !== undefined && !this.#canonicalLockAbsent()) return true;
      // A receipt is a regular file (main unlinked it; an unlink never removes a directory): anything else is kept and
      // waited on, never handed to a recursive removal. verifiedRemove's `.retention-graveyard-*` never matches TICKET.
      if (!stat.isFile()) return true;
      if (!verifiedRemove(file, stat, { deleter: "meshLockQueueReceipt", fallback: () => {
        if (this.#bypassLock !== undefined && !this.#canonicalLockAbsent()) return false;
        fs.unlinkSync(file);
        return true;
      } }).removed) {
        return fs.lstatSync(file, { throwIfNoEntry: false }) !== undefined;
      }
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    }
    return false;
  }

  /** Past 80% of the budget (with `passStalledHead`, at any time), join the plain contest only if the head is
   * stalled: neither its heartbeat nor our first sight of it as head is within the stall bound. The second term
   * keeps a head that has only just taken over from being judged by its old creation time. Returns undefined when the
   * canonical-lock follower reaped its stalled head and the caller must rescan. */
  #holdBack(): boolean | undefined {
    if (this.#reapStalledHead()) return undefined;
    const now = Date.now();
    if (now < this.#fallbackAt) {
      // smarty-dev#10516: a stalled head otherwise holds every waiter to 80% of its budget, and most then time out in
      // the last 20%. One queue listing per heartbeat interval at most, not one per poll.
      if (!this.#passStalledHead || now - this.#headCheckedAt < HEARTBEAT_MS) return false;
      this.#headCheckedAt = now;
    }
    const head = fs.readdirSync(this.#directory).filter(name => TICKET.test(name)).sort()[0];
    if (head === undefined || head === this.#name) return false; // the next scan makes us head
    if (head !== this.#observedHead) { this.#observedHead = head; this.#observedAt = now; }
    try {
      const heartbeat = Math.max(fs.statSync(path.join(this.#directory, head)).mtimeMs, this.#observedAt);
      if (now - heartbeat <= this.#stallMs) return false;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") return false;
      throw error;
    }
    this.close();
    return true;
  }

  /** smarty-dev#9805: reap the receipt of a stalled head only from the waiter directly behind it. This waiter must
   * have observed the same head name and mtime for #stallMs, and the canonical lock must be absent at the judgment.
   * The judged stat goes through verifiedRemove, exactly like ordinary dead/stale receipt cleanup. A failed verification
   * keeps the receipt and holds back; a successful removal asks mayContend() to rescan. */
  #reapStalledHead(): boolean {
    if (this.#bypassLock === undefined || !this.#nextAfterHead || this.#predecessor === undefined) return false;
    const now = Date.now();
    const beat = `${this.#predecessor}:${this.#predecessorMtimeMs}`;
    if (beat !== this.#observedBeat) {
      this.#observedBeat = beat;
      this.#beatSeenAt = now;
      return false;
    }
    if (now - this.#beatSeenAt <= this.#stallMs) return false;
    if (!this.#canonicalLockAbsent()) return false;
    const judged = this.#predecessorStat;
    if (judged === undefined || !judged.isFile()) return false;
    const file = path.join(this.#directory, this.#predecessor);
    try {
      const removed = verifiedRemove(file, judged, { deleter: "meshLockQueueReceipt", proceed: () => this.#canonicalLockAbsent(), fallback: () => {
        if (!this.#canonicalLockAbsent()) return false;
        fs.unlinkSync(file);
        return true;
      } }).removed;
      if (!removed) {
        // Do not spin on a permission or identity refusal. The next observation gets another full stall bound.
        this.#beatSeenAt = now;
        return false;
      }
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    }
    this.#predecessorStat = undefined;
    this.#observedBeat = undefined;
    return true;
  }

  /** The canonical guard is optional; when configured, only ENOENT proves absence and every other result holds back cleanup. */
  #canonicalLockAbsent(): boolean {
    if (this.#bypassLock === undefined) return true;
    try { fs.lstatSync(this.#bypassLock); return false; }
    catch (error) { return (error as NodeJS.ErrnoException).code === "ENOENT"; }
  }

  /** The head (and the holder, until it releases) proves progress by its receipt's mtime. */
  #heartbeat(force = false): void {
    const now = Date.now();
    if (!force && now - this.#touchedAt < HEARTBEAT_MS) return;
    this.#touchedAt = now;
    try { fs.utimesSync(this.#file, now / 1000, now / 1000); }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
  }

  get queued(): boolean { return !this.#closed; }

  /** Sleep at most `ms`, waking early when the receipt this waiter depends on goes (non-head)
   * or when .lock is released (head). A wake that arrived since the last wait returns at once.
   * An optional fixed acquisition deadline bounds the sleep after synchronous watch setup. */
  wait(ms: number, signal?: AbortSignal, deadline?: number): Promise<void> {
    signal?.throwIfAborted();
    if (!this.#closed) {
      if (this.#head) this.#watchRelease();
      else if (this.#predecessor !== undefined) this.#watchPredecessor(this.#predecessor);
    }
    signal?.throwIfAborted();
    if (this.#signalled) { this.#signalled = false; return Promise.resolve(); }
    const sleepMs = deadline === undefined ? ms : Math.min(ms, deadline - Date.now());
    if (deadline !== undefined && sleepMs <= 0) return Promise.resolve();
    return new Promise((resolve, reject) => {
      const finish = (): void => {
        clearTimeout(timer);
        signal?.removeEventListener("abort", onAbort);
        this.#wake = undefined;
        this.#signalled = false;
        resolve();
      };
      const onAbort = (): void => {
        clearTimeout(timer);
        signal?.removeEventListener("abort", onAbort);
        this.#wake = undefined;
        reject(signal!.reason);
      };
      const timer = setTimeout(finish, sleepMs);
      this.#wake = finish;
      signal?.addEventListener("abort", onAbort, { once: true });
    });
  }

  #signal(): void {
    if (this.#wake) this.#wake();
    else this.#signalled = true;
  }

  /** Not persistent: the waiter's own timer keeps the process alive. Any failure (no inotify
   * instances, unsupported filesystem) leaves the poll, the previous behaviour. */
  #open(target: string, onEvent: (name: string | null) => void): fs.FSWatcher | undefined {
    try {
      const watcher = fs.watch(target, { persistent: false }, (_event, name) => onEvent(name === null ? null : String(name)));
      watcher.on("error", () => {
        watcher.close();
        if (this.#predecessorWatcher === watcher) {
          this.#predecessorWatcher = undefined;
          this.#watched = undefined;
        } else if (this.#rootWatcher === watcher) this.#rootWatcher = undefined;
        else return; // A late error from a replaced/closed handle cannot disturb its successor.
        this.#signal();
      });
      return watcher;
    } catch (error) {
      // The receipt went between our stat and the watch: that is the wake itself.
      if ((error as NodeJS.ErrnoException).code === "ENOENT") this.#signal();
      return undefined;
    }
  }

  /** Watch the predecessor's receipt only (one event per handoff, not one per queue change). */
  #watchPredecessor(name: string): void {
    if (this.#watched === name) return;
    this.#predecessorWatcher?.close();
    this.#predecessorWatcher = this.#open(path.join(this.#directory, name), () => this.#signal());
    this.#watched = this.#predecessorWatcher ? name : undefined;
  }

  /** The head is the only waiter that watches the mesh root, for .lock going away. */
  #watchRelease(): void {
    if (this.#rootWatcher) return;
    this.#predecessorWatcher?.close();
    this.#predecessorWatcher = undefined;
    this.#watched = undefined;
    this.#rootWatcher = this.#open(this.#root, name => { if (name === null || name === ".lock") this.#signal(); });
  }

  close(): void {
    this.#predecessorWatcher?.close();
    this.#rootWatcher?.close();
    this.#predecessorWatcher = this.#rootWatcher = undefined;
    this.#wake = undefined;
    if (this.#closed) return;
    this.#closed = true;
    // Never throws (pi-fabric#936 round 3): a writer's cleanup must always finish; an unremovable receipt goes stale.
    // ponytail (smarty-dev#8907): no judgment precedes this. `#file` is this ticket's own receipt (hrtime-pid-uuid,
    // created `wx` by this ticket, never reused or renamed); no other writer can put another inode at that name.
    try { fs.rmSync(this.#file, { force: true }); } catch { /* left to stale reaping */ }
    // Keep the empty directory OUTSIDE the mesh: removing/recreating it races enqueue.
  }
}
