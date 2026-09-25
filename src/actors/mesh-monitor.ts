import fs, { type FSWatcher } from "node:fs";
import path from "node:path";
import { writeJsonAtomic } from "../core/atomic-write.js";
import type { FabricMeshConfig } from "../config.js";
import type { MeshEvent, MeshStore } from "../mesh/store.js";

const MESH_WATCH_RECONCILE_MS = 2_000;

/** Owns observation resources and the format-1 cursor, never actor ownership or dispatch policy. */
export class ActorMeshMonitor {
  #timer: NodeJS.Timeout | undefined;
  #watcher: FSWatcher | undefined;
  #offset: number;
  #scheduled = false;
  #polling = false;
  #closed = false;
  #started = false;
  /** Events created before this are not delivered when resuming from a saved cursor. */
  #replayFloor: number | undefined;
  /** Resumed from a saved cursor and not yet at the end of the log. */
  #catchingUp = false;

  constructor(
    readonly mesh: Pick<MeshStore, "root" | "latestOffset" | "tail">,
    readonly config: Pick<FabricMeshConfig, "enabled" | "actorPollMs" | "maxReadEvents">,
    readonly callbacks: {
      cursorPath?: string | undefined;
      /**
       * On resume from a saved cursor, deliver only events newer than this many ms, so a
       * restart replays the gap it missed and not a long downtime (#37's replay storm).
       */
      maxReplayAgeMs?: number | undefined;
      beforePoll(): boolean;
      /** false: a receiver is full; while catching up, the event is offered again later. */
      onEvent(event: MeshEvent): boolean | void;
    },
  ) {
    const saved = this.#readCursor();
    this.#offset = saved ?? mesh.latestOffset();
    if (saved !== undefined && callbacks.maxReplayAgeMs !== undefined) {
      this.#replayFloor = Date.now() - callbacks.maxReplayAgeMs;
      this.#catchingUp = true;
    }
  }

  start(): void {
    if (this.#started || this.#closed || !this.config.enabled) return;
    this.#started = true;
    if (process.platform === "win32") {
      this.#startTimer(this.config.actorPollMs);
      this.schedule();
      return;
    }
    try {
      const watcher = fs.watch(this.mesh.root, { persistent: false }, (_event, filename) => {
        if (filename !== null && path.basename(filename.toString()) !== "events.jsonl") return;
        this.schedule();
      });
      this.#watcher = watcher;
      watcher.on("error", () => this.#fallback(watcher));
      this.#startTimer(Math.max(MESH_WATCH_RECONCILE_MS, this.config.actorPollMs));
    } catch {
      this.#startTimer(this.config.actorPollMs);
    }
    this.schedule();
  }

  close(): void {
    this.#closed = true;
    if (this.#timer) clearInterval(this.#timer);
    this.#timer = undefined;
    this.#watcher?.close();
    this.#watcher = undefined;
  }

  schedule(): void {
    if (this.#scheduled || this.#closed || !this.config.enabled) return;
    this.#scheduled = true;
    queueMicrotask(() => {
      this.#scheduled = false;
      if (this.#closed) return;
      void this.#poll().catch(() => undefined);
    });
  }

  #fallback(watcher: FSWatcher): void {
    if (this.#closed || this.#watcher !== watcher) return;
    watcher.close();
    this.#watcher = undefined;
    this.#startTimer(this.config.actorPollMs);
    this.schedule();
  }

  #startTimer(delay: number): void {
    if (this.#timer) clearInterval(this.#timer);
    this.#timer = setInterval(() => this.schedule(), delay);
    this.#timer.unref();
  }

  async #poll(): Promise<void> {
    if (this.#polling || this.#closed || !this.config.enabled) return;
    if (!this.callbacks.beforePoll()) return;
    this.#polling = true;
    try {
      // Live and catch-up both read whole pages. Live advances first, so a failing dispatch
      // never blocks the stream; the cursor file is committed after the page.
      const start = this.#offset;
      const tail = this.mesh.tail(start, this.config.maxReadEvents);
      if (this.#catchingUp && tail.events.length === 0) this.#catchingUp = false;
      const catchingUp = this.#catchingUp;
      if (!catchingUp) this.#offset = tail.nextOffset;
      for (const [index, event] of tail.events.entries()) {
        if (this.#replayFloor !== undefined && event.createdAt < this.#replayFloor) continue;
        if (this.callbacks.onEvent(event) === false && catchingUp) {
          // A full actor queue rejected this event while catching up (smarty-dev#472): keep
          // the cursor on it and offer it again later; earlier events are already delivered.
          // The boundary comes from this same read, so a compaction since cannot move it.
          this.#offset = index === 0 ? start : tail.cursors?.[index - 1] ?? start;
          this.#writeCursor();
          return;
        }
      }
      if (catchingUp) this.#offset = tail.nextOffset;
      this.#writeCursor();
      // Yield to the event loop between catch-up pages, so timers such as the lease
      // heartbeat keep running through a long backlog.
      if (catchingUp) setImmediate(() => this.schedule());
    } finally {
      this.#polling = false;
    }
  }

  #readCursor(): number | undefined {
    if (!this.callbacks.cursorPath) return undefined;
    try {
      const value = JSON.parse(fs.readFileSync(this.callbacks.cursorPath, "utf8")) as {
        format?: unknown;
        cursor?: unknown;
      };
      return value.format === 1 && typeof value.cursor === "number" && value.cursor >= 0
        ? value.cursor
        : undefined;
    } catch {
      return undefined;
    }
  }

  #writeCursor(): void {
    if (!this.callbacks.cursorPath) return;
    try {
      writeJsonAtomic(this.callbacks.cursorPath, { format: 1, cursor: this.#offset }, { space: 2 });
    } catch {
      // Cursor persistence is best-effort; replay resumes from the latest safe cursor.
    }
  }
}
