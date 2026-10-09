import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";

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

/** The same native release/receipt events used by FIFO waiters, but no ticket or
 * periodic probe. A timed-out publisher keeps only this passive wake subscription. */
export const onMeshLockAdmission = (root: string, wake: () => void): (() => void) => {
  const watchers: fs.FSWatcher[] = [];
  let closed = false;
  const open = (directory: string, event: (name: string | null) => void): void => {
    try {
      const watcher = fs.watch(directory, { persistent: false }, (kind, name) => {
        if (!closed && kind === "rename") event(name === null ? null : String(name));
      });
      watcher.on("error", () => watcher.close());
      watchers.push(watcher);
    } catch { /* R-no-polling's once-per-minute heartbeat fallback remains. */ }
  };
  open(root, name => {
    if ((name === null || name === ".lock" || name.startsWith(".lock.released.")) &&
      !fs.existsSync(path.join(root, ".lock"))) wake();
  });
  try {
    const queue = meshLockQueueDirectory(root);
    // Our cancelled receipt is NOT an admission event. Only receipts already
    // belonging to other waiters when we subscribed can hand admission to us.
    const predecessors = new Set(fs.readdirSync(queue).filter(name => TICKET.test(name) && Number(name.split("-")[1]) !== process.pid));
    open(queue, name => {
      if (name !== null && predecessors.has(name) && !fs.existsSync(path.join(queue, name))) { predecessors.delete(name); wake(); }
    });
  } catch { /* A queue not created yet contributes no event. */ }
  return () => { closed = true; for (const watcher of watchers) watcher.close(); };
};
// Budgets below this are short try-acquires (registry-fenced publication); they keep the
// unconditional 80% fallback rather than waiting behind a long queue they cannot clear.
const GATED_BUDGET_MS = 2000;
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
 * receipt order is the admission order. An ordinary waiter falls back to the plain contest after
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
  readonly #gated: boolean;
  readonly #stallMs: number;
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

  constructor(root: string, token: string, budgetMs: number) {
    this.#directory = meshLockQueueDirectory(root);
    this.#root = root;
    // hrtime is a host-wide monotonic clock, unlike a wall clock that can step backwards.
    this.#name = `${process.hrtime.bigint().toString().padStart(24, "0")}-${process.pid}-${token}`;
    this.#file = path.join(this.#directory, this.#name);
    this.#fallbackAt = Date.now() + Math.min(10_000, Math.max(0, budgetMs * 0.8));
    this.#staleMs = Math.max(30_000, budgetMs * 2);
    this.#gated = budgetMs >= GATED_BUDGET_MS;
    this.#stallMs = Math.min(2000, Math.max(4 * HEARTBEAT_MS, budgetMs * 0.1));
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
   * when that receipt goes, or once past the fallback point to judge the head's progress. */
  mayContend(): boolean {
    if (this.#closed) return true;
    // Short (try) budgets keep the previous unconditional 80% fallback, head included.
    if (!this.#gated && Date.now() >= this.#fallbackAt) { this.close(); return true; }
    if (this.#head) { this.#heartbeat(); return true; }
    if (this.#predecessor !== undefined && this.#waitingOn(this.#predecessor)) return this.#holdBack();
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
      if (this.#waitingOn(this.#predecessor)) return this.#holdBack();
      // The predecessor was dead or stale and is now unlinked: look again.
    }
  }

  /** False when the receipt is gone; a dead or stale one is reaped. Names are unique and never
   * reused, so unlink cannot remove a successor. */
  #waitingOn(name: string): boolean {
    const file = path.join(this.#directory, name);
    try {
      const stat = fs.statSync(file);
      if (pidAlive(name) && Date.now() - stat.mtimeMs <= this.#staleMs) return true;
      fs.unlinkSync(file);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    }
    return false;
  }

  /** Past 80% of an ordinary budget, join the plain contest only if the head is stalled: neither
   * its heartbeat nor our first sight of it as head is within the stall bound. The second term
   * keeps a head that has only just taken over from being judged by its old creation time. */
  #holdBack(): boolean {
    const now = Date.now();
    if (now < this.#fallbackAt) return false;
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
   * or when .lock is released (head). A wake that arrived since the last wait returns at once. */
  wait(ms: number, signal?: AbortSignal): Promise<void> {
    signal?.throwIfAborted();
    if (!this.#closed) {
      if (this.#head) this.#watchRelease();
      else if (this.#predecessor !== undefined) this.#watchPredecessor(this.#predecessor);
    }
    if (this.#signalled) { this.#signalled = false; return Promise.resolve(); }
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
        this.#wake = undefined;
        reject(signal!.reason);
      };
      const timer = setTimeout(finish, ms);
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
      watcher.on("error", () => watcher.close());
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
    this.#watched = name;
    this.#predecessorWatcher = this.#open(path.join(this.#directory, name), () => this.#signal());
  }

  /** The head is the only waiter that watches the mesh root, for .lock going away. */
  #watchRelease(): void {
    if (this.#rootWatcher) return;
    this.#predecessorWatcher?.close();
    this.#predecessorWatcher = undefined;
    this.#rootWatcher = this.#open(this.#root, name => { if (name === null || name === ".lock") this.#signal(); });
  }

  close(): void {
    this.#predecessorWatcher?.close();
    this.#rootWatcher?.close();
    this.#predecessorWatcher = this.#rootWatcher = undefined;
    this.#wake = undefined;
    if (this.#closed) return;
    fs.rmSync(this.#file, { force: true });
    this.#closed = true;
    // Keep the empty directory OUTSIDE the mesh: removing/recreating it races enqueue.
  }
}
