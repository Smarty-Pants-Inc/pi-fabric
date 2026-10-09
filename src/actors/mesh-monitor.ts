import fs, { type FSWatcher } from "node:fs";
import { MeshBackgroundRetry } from "../core/atomic-write.js";
import path from "node:path";
import { writeJsonAtomic } from "../core/atomic-write.js";
import type { FabricMeshConfig } from "../config.js";
import { meshCursorAtStart, meshCursorGeneration, type MeshEvent, type MeshStore } from "../mesh/store.js";

const MESH_WATCH_RECONCILE_MS = 60_000;

/** Observation hint only: never a receipt, cursor, or consumption authority. */
export function meshObserverStamp(root: string, files: readonly string[]): string | undefined {
  try {
    return files.map(file => {
      try {
        const stat = fs.statSync(path.join(root, file), { bigint: true });
        return `${stat.dev}:${stat.ino}:${stat.size}:${stat.mtimeNs}:${stat.ctimeNs}`;
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code === "ENOENT") return "missing";
        throw error;
      }
    }).join(";");
  } catch { return undefined; } // An unreadable witness must request a trusted drain.
}
/** Native watches follow inodes, not paths. Check attachment races and idle replacements. */
const watchIdentities = new WeakMap<FSWatcher, string>();
function directoryIdentity(directory: string): string | undefined {
  try {
    const stat = fs.statSync(directory, { bigint: true });
    return stat.isDirectory() ? `${stat.dev}:${stat.ino}` : undefined;
  } catch { return undefined; }
}
export function meshObserverWatchCurrent(watcher: FSWatcher, directory: string): boolean {
  const identity = directoryIdentity(directory);
  return identity !== undefined && identity === watchIdentities.get(watcher);
}
export function meshObserverWatch(
  directory: string,
  options: { persistent: boolean; recursive?: boolean },
  notify: (event: string, filename: string | Buffer | null) => void,
): FSWatcher | undefined {
  const identity = directoryIdentity(directory);
  if (identity === undefined) return undefined;
  const watcher = fs.watch(directory, options, notify);
  if (directoryIdentity(directory) !== identity) { watcher.close(); return undefined; }
  watchIdentities.set(watcher, identity);
  return watcher;
}
const OBSERVED_FILES = ["events.jsonl", "generation", "state.json", "participants", "host-leases"];
const OBSERVED_DIRECTORIES = ["actors", "participants", "host-leases"];
const MESH_BACKGROUND_POLL_MS = 1_000;
const CURSOR_CHECKPOINT_MS = 10_000;
type MonitorCursor = { cursor: number; last?: { sequence: number; id: string } };
/**
 * Work topics (smarty-dev#754 §3.2 step 3): agent-to-agent acks, asks, handoffs. They are
 * durable work, so they skip the replay window and are read back from the mesh archive.
 */
const WORK_TOPIC_PREFIX = "fleet.";
const isWork = (event: MeshEvent): boolean => event.topic.startsWith(WORK_TOPIC_PREFIX);

/** Owns observation resources and the format-1 cursor, never actor ownership or dispatch policy. */
export class ActorMeshMonitor {
  readonly #backgroundPoll = new MeshBackgroundRetry("actor mesh monitor");
  #timer: NodeJS.Timeout | undefined;
  #watcher: FSWatcher | undefined;
  readonly #directoryWatchers = new Map<string, FSWatcher>();
  #retryTimer: NodeJS.Timeout | undefined;
  #stamp: string | undefined;
  #eventStamp: string | undefined;
  #retryNeeded = false;
  #watchTimer: NodeJS.Timeout | undefined;
  #lastWatchAt = Number.NEGATIVE_INFINITY;
  #watchPending = false;
  #watchTailPending = false;
  #offset: number;
  #scheduled = false;
  #running = false;
  #dirty = false;
  #tailRequested = false;
  #polling = false;
  #closed = false;
  #started = false;
  /** Events created before this are not delivered when resuming from a saved cursor. */
  #replayFloor: number | undefined;
  /** Resumed from a saved cursor and not yet at the end of the log. */
  #catchingUp = false;
  /** Last event passed (delivered or skipped), so resets and rereads do not deliver it twice. */
  #last: { sequence: number; id: string } | undefined;
  /** On resume: the sequence to read work events after, from the archive, before the live log. */
  #archiveAfter: number | undefined;
  /** Last safe page boundary; live dispatch can throw after advancing the in-memory offset. */
  #safeCursor: MonitorCursor;
  #persistedCursor: string | undefined;
  #persistedEventAnchor = false;
  #lastCheckpointAt = Date.now();

  constructor(
    readonly mesh: Pick<MeshStore, "root" | "latestOffset" | "tail"> & Partial<Pick<MeshStore, "read" | "oldestSequence" | "nextEventAfter" | "latestCursor">>,
    readonly config: Pick<FabricMeshConfig, "enabled" | "actorPollMs" | "maxReadEvents">,
    readonly callbacks: {
      cursorPath?: string | undefined;
      /**
       * On resume from a saved cursor, deliver only events newer than this many ms, so a
       * restart replays the gap it missed and not a long downtime (#37's replay storm).
       */
      maxReplayAgeMs?: number | undefined;
      beforePoll(): boolean;
      /** Rechecked per event: a page can outlast the resident host lease. */
      canConsumeMesh?: (() => boolean) | undefined;
      /** false: full (retry unchanged); "ignored": no local delivery; true/void: handed on. */
      onEvent(event: MeshEvent): boolean | void | "ignored";
    },
  ) {
    const saved = this.#readCursor();
    const initial: MonitorCursor = saved ?? mesh.latestCursor?.() ?? { cursor: mesh.latestOffset() };
    this.#offset = initial.cursor;
    this.#last = initial.last;
    this.#persistedCursor = saved ? JSON.stringify(saved) : undefined;
    this.#persistedEventAnchor = (saved?.last?.sequence ?? 0) > 0;
    if (saved && !saved.last && mesh.read) {
      // A legacy/crash seed has no sequence boundary. Its bytes may now name a different
      // file, even before the generation bump. Replay conservatively once, not on idle polls.
      this.#last = { sequence: 0, id: "" };
      this.#offset = meshCursorAtStart(meshCursorGeneration(saved.cursor));
    }
    this.#safeCursor = { cursor: this.#offset, ...(this.#last ? { last: this.#last } : {}) };
    if (saved !== undefined && callbacks.maxReplayAgeMs !== undefined) {
      this.#replayFloor = Date.now() - callbacks.maxReplayAgeMs;
      this.#catchingUp = true;
    }
    if (saved && this.#last) this.#archiveAfter = this.#last.sequence;
  }

  start(): void {
    if (this.#started || this.#closed || !this.config.enabled) return;
    this.#started = true;
    this.#attachWatcher();
    this.#startTimer(Math.max(MESH_WATCH_RECONCILE_MS, this.config.actorPollMs));
    this.schedule();
  }

  /** Checked release receipt; normal best-effort writes are NOT a handover barrier. */
  checkpointForRelease(): void {
    if (this.#polling) throw new Error("Actor mesh cursor still polling");
    if (this.callbacks.cursorPath) {
      writeJsonAtomic(this.callbacks.cursorPath, { format: 1, ...this.#safeCursor }, { durable: true, space: 2 });
      this.#persistedCursor = JSON.stringify(this.#safeCursor);
    }
  }

  close(): void {
    this.#closed = true;
    if (this.#timer) clearInterval(this.#timer);
    this.#timer = undefined;
    if (this.#watchTimer) clearTimeout(this.#watchTimer);
    this.#watchTimer = undefined;
    if (this.#retryTimer) clearTimeout(this.#retryTimer);
    this.#retryTimer = undefined;
    this.#watcher?.close();
    this.#watcher = undefined;
    for (const watcher of this.#directoryWatchers.values()) watcher.close();
    this.#directoryWatchers.clear();
    if (this.#started) this.#persistCursor(true);
  }

  schedule(readTail = true): void {
    if (this.#closed || !this.config.enabled) return;
    this.#tailRequested ||= readTail;
    this.#dirty = true;
    if (this.#scheduled || this.#running) return;
    this.#scheduled = true;
    queueMicrotask(() => {
      this.#scheduled = false;
      if (this.#closed) return;
      this.#running = true;
      this.#dirty = false;
      void this.#backgroundPoll.run(() => this.#poll()).then(result => {
        this.#running = false;
        if (result !== "done" || this.#retryNeeded) this.#retry();
        else if (this.#dirty) this.schedule(false);
      });
    });
  }

  // Lead after a quiet actor cadence, then retain one trailing wake for a burst.
  // The window belongs to watch notifications, not startup/explicit/idle polls:
  // none of those may delay the first new event. Continuous notifications never
  // slide the trailing deadline; isolated events retain their original latency.
  #scheduleBackground(readTail = true): void {
    if (this.#closed || !this.config.enabled) return;
    this.#tailRequested ||= readTail;
    const now = Date.now();
    const quiet = now - this.#lastWatchAt >= this.config.actorPollMs;
    this.#lastWatchAt = now;
    if (this.#watchTimer && !quiet) {
      this.#watchPending = true;
      this.#watchTailPending ||= readTail;
      return;
    }
    if (this.#watchTimer) clearTimeout(this.#watchTimer);
    this.#watchPending = false;
    this.#watchTailPending = false;
    this.schedule(false);
    this.#watchTimer = setTimeout(() => {
      this.#watchTimer = undefined;
      if (this.#watchPending) {
        this.#watchPending = false;
        const readTail = this.#watchTailPending;
        this.#watchTailPending = false;
        this.schedule(readTail);
      }
    }, Math.max(MESH_BACKGROUND_POLL_MS, this.config.actorPollMs));
    this.#watchTimer.unref();
  }

  #fallback(watcher: FSWatcher): void {
    if (this.#closed || this.#watcher !== watcher) return;
    watcher.close();
    this.#watcher = undefined;
    this.schedule();
  }

  #attachWatcher(reconcile = false): void {
    if (this.#closed) return;
    // These changes are not necessarily mesh appends. Notifications only request
    // the existing manager-owned registry/completion/ownership checks.
    for (const directory of OBSERVED_DIRECTORIES) {
      const watchedPath = path.join(this.mesh.root, directory);
      const previous = this.#directoryWatchers.get(directory);
      if (previous && reconcile && !meshObserverWatchCurrent(previous, watchedPath)) {
        this.#directoryWatchers.delete(directory); previous.close();
      }
      if (this.#directoryWatchers.has(directory)) continue;
      try {
        const watcher = meshObserverWatch(watchedPath, { persistent: false, recursive: directory === "actors" }, (_event, filename) => {
          if (this.#closed || this.#directoryWatchers.get(directory) !== watcher) return;
          if (filename !== null && path.basename(filename.toString()) === "mesh-cursor.json") return;
          this.#scheduleBackground(false);
        });
        if (!watcher) continue;
        this.#directoryWatchers.set(directory, watcher);
        watcher.on("error", () => {
          if (this.#closed || this.#directoryWatchers.get(directory) !== watcher) return;
          watcher.close(); this.#directoryWatchers.delete(directory); this.schedule(false);
        });
      } catch { /* Platforms without recursive watches retain bounded reconciliation. */ }
    }
    if (this.#watcher && reconcile && !meshObserverWatchCurrent(this.#watcher, this.mesh.root)) {
      const previous = this.#watcher; this.#watcher = undefined; previous.close();
    }
    if (this.#watcher) return;
    try {
      const watcher = meshObserverWatch(this.mesh.root, { persistent: false }, (_event, filename) => {
        if (this.#closed || this.#watcher !== watcher) return;
        if (filename !== null) {
          const file = path.basename(filename.toString());
          if (OBSERVED_DIRECTORIES.includes(file)) this.#attachWatcher(true);
          else if (!OBSERVED_FILES.includes(file)) return;
        }
        const file = filename === null ? undefined : path.basename(filename.toString());
        this.#scheduleBackground(file === undefined || file === "events.jsonl" || file === "generation");
      });
      if (!watcher) return;
      this.#watcher = watcher;
      watcher.on("error", () => this.#fallback(watcher));
    } catch { /* Retry attachment at the bounded safety check, never fast idle polling. */ }
  }

  #startTimer(delay: number): void {
    if (this.#timer) clearInterval(this.#timer);
    this.#timer = setInterval(() => {
      this.#attachWatcher(true);
      const stamp = meshObserverStamp(this.mesh.root, OBSERVED_FILES);
      if (stamp === undefined || stamp !== this.#stamp) this.schedule(false);
      else if (!this.#closed) void this.#backgroundPoll.run(() => this.callbacks.beforePoll()).then(result => {
        if (result !== "done") this.#retry();
      }); // Time-based ownership/filter expiry, without rereading the unchanged tail.
    }, delay);
    this.#timer.unref();
  }

  #retry(): void {
    if (this.#closed || this.#retryTimer) return;
    this.#retryTimer = setTimeout(() => {
      this.#retryTimer = undefined;
      this.schedule();
    }, this.#backgroundPoll.waitMs || Math.max(20, this.config.actorPollMs));
    this.#retryTimer.unref();
  }

  async #poll(): Promise<void> {
    if (this.#polling || this.#closed || !this.config.enabled) return;
    const readTail = this.#tailRequested;
    this.#tailRequested = false;
    if (!this.callbacks.beforePoll() || this.callbacks.canConsumeMesh?.() === false) return;
    this.#stamp = meshObserverStamp(this.mesh.root, OBSERVED_FILES);
    const eventStamp = meshObserverStamp(this.mesh.root, ["events.jsonl", "generation"]);
    if (!readTail && eventStamp !== undefined && eventStamp === this.#eventStamp && !this.#retryNeeded && this.#archiveAfter === undefined && !this.#catchingUp) return;
    this.#eventStamp = eventStamp;
    this.#retryNeeded = true; // Only an admitted read can acquire a blocked-page obligation.
    this.#polling = true;
    try {
      if (this.#archiveAfter !== undefined && !this.#catchUpArchive()) return;
      // Live and catch-up both read whole pages. A throwing dispatch restores the boundary
      // before its event, so an empty later poll cannot checkpoint past failed work.
      const start = this.#offset;
      const tail = this.mesh.tail(start, this.config.maxReadEvents);
      if (this.callbacks.canConsumeMesh?.() === false) return;
      // A rewrite restarts the stream at the retained log; the events it cut are in the archive
      // (it holds each event before it goes live). The generation and the events file are two
      // reads, and a rewrite renames the file before it bumps the generation, so a page can come
      // from a new file with an old generation, or from a stale byte offset into it. So trust
      // the events too: a generation change, or a page that starts past the event after the last
      // one handed on, re-arms the archive from that event and re-reads the current file from
      // its start. Nothing is committed past the gap first; events already handed on are skipped.
      const generation = meshCursorGeneration(tail.nextOffset);
      const first = tail.events[0];
      // A gap is unread only if the store holds an event inside it. `nextEventAfter` asks the
      // archive alone when it holds the range: every event is there before it goes live, so no
      // rewrite can hide one. A hole the store does not hold (a failed publish's reserved
      // sequence, or events a store without the archive cut) is passed at once, so it cannot
      // stall the stream.
      const gap = first !== undefined && this.#last !== undefined && this.mesh.nextEventAfter !== undefined &&
        typeof first.sequence === "number" && first.sequence > this.#last.sequence + 1 &&
        (this.mesh.nextEventAfter(this.#last.sequence)?.sequence ?? Infinity) < first.sequence;
      if (this.#last && this.mesh.read && (generation !== meshCursorGeneration(start) || gap)) {
        this.#archiveAfter = this.#last.sequence;
        this.#offset = meshCursorAtStart(generation);
        this.#writeCursor();
        setImmediate(() => this.schedule());
        return;
      }
      if (this.#catchingUp && tail.events.length === 0) this.#catchingUp = false;
      const catchingUp = this.#catchingUp;
      let handedOn = false;
      if (!catchingUp) this.#offset = tail.nextOffset;
      for (const [index, event] of tail.events.entries()) {
        if (this.callbacks.canConsumeMesh?.() === false) {
          this.#offset = index === 0 ? start : tail.cursors?.[index - 1] ?? start;
          this.#safeCursor = { cursor: this.#offset, ...(this.#last ? { last: this.#last } : {}) };
          return;
        }
        if (this.#delivered(event)) continue;
        if (this.#replayFloor !== undefined && event.createdAt < this.#replayFloor && !isWork(event)) {
          if (typeof event.sequence === "number" && typeof event.id === "string") this.#last = { sequence: event.sequence, id: event.id };
          continue;
        }
        let accepted: boolean | void | "ignored";
        try {
          accepted = this.callbacks.onEvent(event);
        } catch (error) {
          // Keep only this page's consumed prefix. No second read can move the retry
          // boundary; #last describes that prefix and suppresses duplicates on reread.
          this.#offset = index === 0 ? start : tail.cursors?.[index - 1] ?? start;
          this.#safeCursor = { cursor: this.#offset, ...(this.#last ? { last: this.#last } : {}) };
          throw error;
        }
        if (this.callbacks.canConsumeMesh?.() === false) {
          this.#offset = index === 0 ? start : tail.cursors?.[index - 1] ?? start;
          this.#safeCursor = { cursor: this.#offset, ...(this.#last ? { last: this.#last } : {}) };
          return;
        }
        // A receiver that is full holds the event: while catching up, and for work events always
        // (smarty-dev#754), so work waits for room instead of being dropped.
        if (accepted === false && (catchingUp || isWork(event))) {
          // A full actor queue rejected this event while catching up (smarty-dev#472): keep
          // the cursor on it and offer it again later; earlier events are already delivered.
          // The boundary comes from this same read, so a compaction since cannot move it.
          this.#offset = index === 0 ? start : tail.cursors?.[index - 1] ?? start;
          this.#writeCursor(handedOn);
          return;
        }
        if (accepted !== false && accepted !== "ignored") handedOn = true;
        if (typeof event.sequence === "number" && typeof event.id === "string") this.#last = { sequence: event.sequence, id: event.id };
      }
      if (catchingUp) this.#offset = tail.nextOffset;
      this.#writeCursor(handedOn);
      this.#retryNeeded = false;
      if (!catchingUp && tail.events.length === this.config.maxReadEvents) setImmediate(() => this.schedule());
      // Yield to the event loop between catch-up pages, so timers such as the lease
      // heartbeat keep running through a long backlog.
      if (catchingUp) setImmediate(() => this.schedule());
    } finally {
      this.#polling = false;
    }
  }

  // Events the stream already passed: sequences only rise in log order, and a sequence two
  // events share is told apart by id (at least once, never lost).
  #delivered(event: MeshEvent): boolean {
    const last = this.#last;
    if (!last || typeof event.sequence !== "number") return false;
    return event.sequence < last.sequence || (event.sequence === last.sequence && event.id === last.id);
  }

  // Work events that left the live log while this host was away, or during a rewrite, come
  // from the archive, oldest first, one page per poll: it yields between pages as the live
  // catch-up does. Returns true when the archive is caught up to the live log's oldest event;
  // false while a page remains, or while a full receiver holds a work event back.
  #catchUpArchive(): boolean {
    const oldest = this.mesh.oldestSequence?.();
    if (!this.mesh.read || oldest === undefined || this.#archiveAfter === undefined || this.#archiveAfter + 1 >= oldest) {
      this.#archiveAfter = undefined;
      return true;
    }
    const page = this.mesh.read({ after: this.#archiveAfter, limit: this.config.maxReadEvents });
    const older = page.filter((event) => event.sequence < oldest);
    let handedOn = false;
    for (const event of older) {
      if (this.callbacks.canConsumeMesh?.() === false) return false;
      if (isWork(event) && !this.#delivered(event)) {
        const accepted = this.callbacks.onEvent(event);
        if (this.callbacks.canConsumeMesh?.() === false) return false;
        if (accepted === false) {
          this.#writeCursor(handedOn);
          return false;
        }
        if (accepted !== "ignored") handedOn = true;
      }
      // Every event older than the live log counts as passed, work or not, so the live log's
      // first event follows it with no gap. Only a relevant delivery checkpoints immediately.
      this.#archiveAfter = event.sequence;
      this.#last = { sequence: event.sequence, id: event.id };
    }
    if (older.length === page.length && page.length === this.config.maxReadEvents) {
      this.#writeCursor(handedOn);
      setImmediate(() => this.schedule());
      return false;
    }
    this.#archiveAfter = undefined;
    this.#writeCursor(handedOn);
    return true;
  }

  #readCursor(): { cursor: number; last?: { sequence: number; id: string } } | undefined {
    if (!this.callbacks.cursorPath) return undefined;
    try {
      const value = JSON.parse(fs.readFileSync(this.callbacks.cursorPath, "utf8")) as {
        format?: unknown;
        cursor?: unknown;
        last?: { sequence?: unknown; id?: unknown };
      };
      if (value.format !== 1 || typeof value.cursor !== "number" || value.cursor < 0) return undefined;
      const last = value.last && typeof value.last.sequence === "number" && typeof value.last.id === "string"
        ? { sequence: value.last.sequence, id: value.last.id }
        : undefined;
      return { cursor: value.cursor, ...(last ? { last } : {}) };
    } catch {
      return undefined;
    }
  }

  #writeCursor(immediate = false): void {
    this.#safeCursor = { cursor: this.#offset, ...(this.#last ? { last: this.#last } : {}) };
    this.#persistCursor(immediate);
  }

  #persistCursor(immediate: boolean): void {
    if (!this.callbacks.cursorPath) return;
    const serialized = JSON.stringify(this.#safeCursor);
    if (serialized === this.#persistedCursor) return;
    // Seed once; never batch away the first actual sequence anchor. Later ignored-only
    // progress can wait ten seconds because restart already has a safe archive boundary.
    const firstAnchor = !this.#persistedEventAnchor && (this.#safeCursor.last?.sequence ?? 0) > 0;
    if (!immediate && !firstAnchor && this.#persistedCursor !== undefined && Date.now() - this.#lastCheckpointAt < CURSOR_CHECKPOINT_MS) return;
    try {
      writeJsonAtomic(this.callbacks.cursorPath, { format: 1, ...this.#safeCursor }, { space: 2 });
      this.#persistedCursor = serialized;
      this.#persistedEventAnchor = (this.#safeCursor.last?.sequence ?? 0) > 0;
      this.#lastCheckpointAt = Date.now();
    } catch {
      // Cursor persistence is best-effort; replay resumes from the latest safe cursor.
    }
  }
}
