import fs from "node:fs";

/** setTimeout fires at once for delays above 2^31-1 ms; longer deadlines are reached in chunks. */
const MAX_TIMER_MS = 2 ** 31 - 1;

/**
 * Event-driven idle wake for a local Main's root inbox (smarty-dev#4313; replaces the 15 s
 * interval). A non-persistent directory watch on the mesh root (atomic renames replace the file
 * inode, the directory stays) reports `events.jsonl`/`generation` changes; one one-shot timer
 * waits for the exact next deadline the inbox reported (steer grace, cooldown, lock backoff).
 * There is no polling loop: `wake` reports a deadline only from a read it actually made (a read
 * never reports a deadline at or before its own clock), so a busy or disarmed Main that skips
 * the read arms nothing and cannot spin; its settle requests the next read. Notifications are hints only: the wake
 * callback owns every idle, preflight, cancellation, liveness and receipt gate.
 * A notification during an async read is retained and runs once after it.
 */
export class RootInboxWatch {
  #watcher: fs.FSWatcher | undefined;
  #timer: ReturnType<typeof setTimeout> | undefined;
  #timerAt = Number.POSITIVE_INFINITY;
  #running = false;
  #requested = false;
  #closed = false;
  #failure: string | undefined;

  /** `wake` resolves to the next known deadline (epoch ms) at which held work becomes due. */
  constructor(readonly root: string, readonly wake: () => Promise<number | undefined>, readonly now: () => number = Date.now) {}

  /** Why notifications are off (watch failed); undefined while the watch is healthy. */
  get failure(): string | undefined { return this.#failure; }

  start(): void {
    if (this.#closed) return;
    this.#open();
    // Subscribe first, then read once: a publication between activation and the watch counts.
    this.request();
  }

  /** One coalesced read. A failed watch is reopened on demand (settle, activation), never on a timer. */
  request(): void {
    if (this.#closed) return;
    if (!this.#watcher) this.#open();
    this.#requested = true;
    if (this.#running) return;
    this.#running = true;
    void (async () => {
      try {
        while (this.#requested && !this.#closed) {
          this.#requested = false;
          let due: number | undefined;
          try { due = await this.wake(); } catch { /* the next notification or turn boundary retries */ }
          this.#schedule(due);
        }
      } finally {
        this.#running = false;
      }
    })();
  }

  close(): void {
    this.#closed = true;
    this.#requested = false;
    if (this.#timer) clearTimeout(this.#timer);
    this.#timer = undefined;
    this.#timerAt = Number.POSITIVE_INFINITY;
    const watcher = this.#watcher;
    this.#watcher = undefined;
    watcher?.close();
  }

  #open(): void {
    try {
      const watcher = fs.watch(this.root, { persistent: false }, (_event, name) => {
        if (this.#watcher !== watcher) return;
        const file = name === null ? null : String(name);
        if (file === null || file === "events.jsonl" || file === "generation") this.request();
      });
      watcher.on("error", (error) => {
        if (this.#watcher !== watcher) return;
        this.#watcher = undefined;
        watcher.close();
        this.#fail(error);
      });
      this.#watcher = watcher;
      this.#failure = undefined;
    } catch (error) {
      this.#fail(error);
    }
  }

  #fail(error: unknown): void {
    const message = `root inbox watch failed for ${this.root}: ${error instanceof Error ? error.message : String(error)}; ` +
      "idle wakes wait for turn boundaries and known deadlines";
    // One line per distinct failure; health reports it until a reopen succeeds.
    if (message !== this.#failure) console.warn(`[pi-fabric] ${message}`);
    this.#failure = message;
  }

  /** Keep only the earliest deadline; one that passed during the read fires at the next tick. */
  #schedule(dueAt: number | undefined): void {
    if (this.#closed || dueAt === undefined || !Number.isFinite(dueAt)) return;
    if (this.#timer && this.#timerAt <= dueAt) return;
    if (this.#timer) clearTimeout(this.#timer);
    this.#timerAt = dueAt;
    this.#arm();
  }

  /** One chunk toward #timerAt; an early chunk re-arms the remainder without reading the inbox. */
  #arm(): void {
    const delay = Math.min(MAX_TIMER_MS, Math.max(1, this.#timerAt - this.now()));
    this.#timer = setTimeout(() => {
      this.#timer = undefined;
      if (this.#closed) return;
      if (this.now() < this.#timerAt) return this.#arm();
      this.#timerAt = Number.POSITIVE_INFINITY;
      this.request();
    }, delay);
    this.#timer.unref?.();
  }
}
