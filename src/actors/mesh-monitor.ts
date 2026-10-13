import fs, { type FSWatcher } from "node:fs";
import { MeshBackgroundRetry, type MeshOutageAlarm } from "../core/atomic-write.js";
import path from "node:path";
import { writeJsonAtomic } from "../core/atomic-write.js";
import { isWatchLimitError } from "../core/watch-fallback.js";
import type { FabricMeshConfig } from "../config.js";
import { meshCursorAtStart, meshCursorGeneration, type MeshEvent, type MeshStore } from "../mesh/store.js";

// No standing poll (#11172): a watch error rescans and re-arms; an unsupported
// watch retries the re-arm on a bounded one-shot deadline; a failed drain
// retries on a bounded one-shot backoff. A healthy idle monitor owns no timer.
const WATCH_REARM_MAX_MS = 5_000;
const FAILED_DRAIN_MIN_MS = 100;
const FAILED_DRAIN_MAX_MS = 5_000;
const CURSOR_CHECKPOINT_MS = 10_000;
const watchWarnings = new Set<string>();

/** One line naming the real cause and the degraded mode; there is no fallback poll. */
export function meshMonitorWatchMessage(target: string, error: unknown, retryMs: number): string {
  const first = retryMs <= 0 ? "first retry at once" : `first retry in ${Math.max(1, Math.round(retryMs))} ms`;
  const retry = `${first}, backing off to at most ${WATCH_REARM_MAX_MS} ms`;
  const mode = `Rescanning once and re-arming the watch on a bounded one-shot deadline (${retry}); there is no standing poll`;
  if (isWatchLimitError(error)) {
    return `[pi-fabric] actor mesh monitor: fs.watch(${target}) failed with ENOSPC: inotify watch limit, not disk space ` +
      `(fs.inotify.max_user_watches is exhausted for this user). ${mode}. To restore instant wakeups, raise the limit, e.g. ` +
      `\`sudo sysctl fs.inotify.max_user_watches=524288\` (persist it under /etc/sysctl.d).`;
  }
  const detail = error instanceof Error ? error.message : String(error);
  return `[pi-fabric] actor mesh monitor: fs.watch(${target}) failed (${detail}). ${mode}.`;
}

/** Warn once per process and cause; never throws. */
function warnMeshWatch(target: string, error: unknown, retryMs: number): void {
  const key = isWatchLimitError(error) ? "limit" : "other";
  if (watchWarnings.has(key)) return;
  watchWarnings.add(key);
  try { console.warn(meshMonitorWatchMessage(target, error, retryMs)); } catch {}
}

/** Test seam: forget which monitor watch warnings were printed. */
export function resetMeshMonitorWatchWarnings(): void { watchWarnings.clear(); }
type MonitorCursor = { cursor: number; last?: { sequence: number; id: string } };
/**
 * Work topics (smarty-dev#754 §3.2 step 3): agent-to-agent acks, asks, handoffs. They are
 * durable work, so they skip the replay window and are read back from the mesh archive.
 */
const WORK_TOPIC_PREFIX = "fleet.";
const isWork = (event: MeshEvent): boolean => event.topic.startsWith(WORK_TOPIC_PREFIX);

/** Owns observation resources and the format-1 cursor, never actor ownership or dispatch policy. */
export class ActorMeshMonitor {
  // The monitor has no identity or publish of its own: its owner supplies the outage sink (smarty-dev#8825).
  readonly #backgroundPoll = new MeshBackgroundRetry("actor mesh monitor")
    .alarmsTo((alarm) => this.callbacks.outageAlarm?.(alarm));
  #retryTimer: NodeJS.Timeout | undefined;
  #rearmTimer: NodeJS.Timeout | undefined;
  #checkpointTimer: NodeJS.Timeout | undefined;
  #rearmFailures = 0;
  #failedDelay = 0;
  #dueTimer: NodeJS.Timeout | undefined;
  #dueAt: number | undefined;
  #watcher: FSWatcher | undefined;
  #stamp: string | undefined;
  #needsPoll = true;
  #offset: number;
  #scheduled = false;
  #wakePending = false;
  #explicitWake = false;
  #maintenanceWake = false;
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
    readonly mesh: Pick<MeshStore, "root" | "latestOffset" | "tail"> & Partial<Pick<MeshStore, "read" | "oldestSequence" | "nextEventAfter" | "latestCursor" | "nextScheduleDueAt" | "releaseDueSchedules">>,
    readonly config: Pick<FabricMeshConfig, "enabled" | "actorPollMs" | "maxReadEvents">,
    readonly callbacks: {
      cursorPath?: string | undefined;
      /**
       * On resume from a saved cursor, deliver only events newer than this many ms, so a
       * restart replays the gap it missed and not a long downtime (#37's replay storm).
       */
      maxReplayAgeMs?: number | undefined;
      /** Sleeping residents replay subscribed events after live-log compaction. */
      replayAll?: boolean;
      beforePoll(): boolean;
      /** Rechecked per event: a page can outlast the resident host lease. */
      canConsumeMesh?: (() => boolean) | undefined;
      /** false: full (retry unchanged); "ignored": no local delivery; true/void: handed on. */
      onEvent(event: MeshEvent): boolean | void | "ignored";
      /** The owner's MeshBackgroundRetry outage sink (smarty-dev#8825). */
      outageAlarm?: ((alarm: MeshOutageAlarm) => Promise<unknown> | void) | undefined;
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
    this.#armWatch();
    this.schedule();
  }

  #armWatch(): void {
    if (this.#closed || this.#watcher) return;
    try {
      // Watch the existing parent directory, not files replaced by atomic rename.
      // Non-recursive directory watches work on Windows too; a nameless rename
      // is a wake, not evidence that nothing changed. The stamp filters duplicates.
      const watcher = fs.watch(this.mesh.root, { persistent: false }, (_event, filename) => {
        if (this.#watcher !== watcher) return;
        this.#rearmFailures = 0; // a delivering watch is healthy again
        const name = filename == null ? undefined : path.basename(filename.toString());
        // schedules.json re-arms the due timer when another process schedules.
        if (name !== undefined && name !== "events.jsonl" && name !== "generation" && name !== "schedules.json") return;
        this.#schedule();
      });
      this.#watcher = watcher;
      watcher.on("error", (error) => this.#watchFailed(watcher, error));
    } catch (error) {
      // An unsupported watch (including a missing directory) retries the re-arm on a
      // bounded one-shot deadline, rescanning each attempt; never a standing interval.
      // ENOSPC here is the inotify watch limit (smarty-dev#5247), not a full disk: say so
      // once, so nobody hunts for free space and the session never crashes.
      this.#rearmFailures++;
      warnMeshWatch(this.mesh.root, error, this.#rearmDelay());
      this.#armRearm();
    }
  }

  #rearmDelay(): number {
    const base = Math.max(1, this.config.actorPollMs);
    return Math.min(base * 2 ** Math.max(0, this.#rearmFailures - 1), Math.max(base, WATCH_REARM_MAX_MS));
  }

  #armRearm(): void {
    if (this.#closed || this.#watcher || this.#rearmTimer) return;
    this.#rearmTimer = setTimeout(() => {
      this.#rearmTimer = undefined;
      if (this.#closed || this.#watcher) return;
      this.#armWatch();
      // Rescan after (re-)arming so nothing published before the watch is missed.
      this.schedule();
    }, this.#rearmDelay());
    this.#rearmTimer.unref();
  }

  caughtUp(): boolean {
    return !this.#polling && this.#archiveAfter === undefined && this.#safeCursor.cursor === this.mesh.latestOffset();
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
    for (const timer of [this.#retryTimer, this.#rearmTimer, this.#checkpointTimer]) if (timer) clearTimeout(timer);
    this.#retryTimer = this.#rearmTimer = this.#checkpointTimer = undefined;
    if (this.#dueTimer) clearTimeout(this.#dueTimer);
    this.#dueTimer = undefined;
    this.#watcher?.close();
    this.#watcher = undefined;
    if (this.#started) this.#persistCursor(true);
  }

  /** Explicit delivery/resume/queue-capacity wake; never delayed by watch traffic. */
  schedule(): void { this.#schedule(true); }

  #schedule(explicit = false, maintenance = false): void {
    if (this.#closed || !this.config.enabled) return;
    this.#wakePending = true;
    this.#explicitWake ||= explicit;
    this.#maintenanceWake ||= maintenance;
    if (this.#scheduled) return;
    this.#scheduled = true;
    queueMicrotask(async () => {
      const force = this.#explicitWake;
      const maintain = this.#maintenanceWake;
      this.#wakePending = this.#explicitWake = this.#maintenanceWake = false;
      try {
        if (!this.#closed) this.#afterRun(await this.#backgroundPoll.run(() => this.#poll(force, maintain)));
      } finally {
        this.#scheduled = false;
        // A callback can free a receiver slot or append another page while dispatching.
        // Retain that wake until the background operation releases its running fence.
        if (this.#wakePending) this.#schedule();
      }
    });
  }

  // A failed or backed-off drain owes one bounded retry; a successful one owes none.
  #afterRun(result: "done" | "retry" | "failed" | "skipped"): void {
    if (this.#closed) return;
    if (result === "done") {
      this.#failedDelay = 0;
      if (this.#retryTimer) clearTimeout(this.#retryTimer);
      this.#retryTimer = undefined;
      return;
    }
    let delay = this.#backgroundPoll.waitMs;
    if (result === "failed") {
      this.#failedDelay = Math.min(FAILED_DRAIN_MAX_MS, Math.max(FAILED_DRAIN_MIN_MS, this.#failedDelay * 2));
      delay = Math.max(delay, this.#failedDelay);
    } else if (result === "skipped" && delay <= 0) return;
    if (this.#retryTimer) return;
    this.#retryTimer = setTimeout(() => {
      this.#retryTimer = undefined;
      this.schedule();
    }, Math.max(1, delay));
    this.#retryTimer.unref();
  }

  #watchFailed(watcher: FSWatcher, error: unknown): void {
    if (this.#closed || this.#watcher !== watcher) return;
    warnMeshWatch(this.mesh.root, error, this.#rearmFailures === 0 ? 0 : this.#rearmDelay());
    watcher.close();
    this.#watcher = undefined;
    // The first error re-arms at once; consecutive errors without a delivered
    // notification back off on one bounded one-shot deadline, never a hot loop.
    if (this.#rearmFailures++ === 0) this.#armWatch();
    else this.#armRearm();
    // One rescan covers notifications the failed watch may have missed.
    this.schedule();
  }

  // A schedule falling due changes no file, so wake at its due time instead of
  // waiting for the reconcile interval.
  #armDue(dueAt: number | undefined): void {
    if (dueAt === this.#dueAt) return;
    if (this.#dueTimer) clearTimeout(this.#dueTimer);
    this.#dueTimer = undefined;
    this.#dueAt = dueAt;
    if (dueAt === undefined || this.#closed) return;
    this.#dueTimer = setTimeout(() => {
      this.#dueTimer = undefined;
      this.#dueAt = undefined;
      this.schedule();
    }, Math.min(Math.max(0, dueAt - Date.now()), 2_147_000_000));
    this.#dueTimer.unref();
  }


  #meshStamp(): string | undefined {
    try {
      return ["events.jsonl", "generation"].map(name => {
        const stat = fs.statSync(path.join(this.mesh.root, name), { bigint: true, throwIfNoEntry: false });
        return stat ? `${stat.dev}:${stat.ino}:${stat.size}:${stat.mtimeNs}:${stat.ctimeNs}` : "missing";
      }).join("/");
    } catch {
      // Unknown is not unchanged: unreadable metadata must not hide recovery work.
      return undefined;
    }
  }

  async #poll(explicit: boolean, maintenance: boolean): Promise<void> {
    if (this.#polling || this.#closed || !this.config.enabled) return;
    const stamp = this.#meshStamp();
    const unchanged = !this.#needsPoll && stamp !== undefined && stamp === this.#stamp;
    if (!explicit && !maintenance && unchanged) return;
    // A maintenance (deferred-checkpoint) wake still runs the manager's registry/ownership/
    // filter/child-result recovery obligations, even with a quiet log. It does not reread it.
    if (!this.callbacks.beforePoll() || this.callbacks.canConsumeMesh?.() === false) {
      this.#needsPoll = true;
      return;
    }
    if (!explicit && unchanged) {
      // Ignored-only progress can still owe its ten-second checkpoint, even
      // though an unchanged log needs no reread.
      this.#persistCursor(false);
      return;
    }
    this.#needsPoll = true;
    this.#polling = true;
    try {
      // Whichever process polls first releases due schedules; the store lock
      // makes the release exactly-once, and this poll's tail then sees them.
      // The lockless due check keeps an idle poll synchronous.
      let dueAt: number | undefined;
      try { dueAt = this.mesh.nextScheduleDueAt?.(); } catch { dueAt = undefined; }
      if (dueAt !== undefined && dueAt <= Date.now()) {
        await this.mesh.releaseDueSchedules?.().catch(() => undefined);
        if (this.#closed) return;
        try { dueAt = this.mesh.nextScheduleDueAt?.(); } catch { dueAt = undefined; }
      }
      this.#armDue(dueAt);
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
        if (this.#replayFloor !== undefined && event.createdAt < this.#replayFloor && !isWork(event) && !this.callbacks.replayAll) {
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
        if (accepted === false && (catchingUp || isWork(event) || this.callbacks.replayAll)) {
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
      this.#stamp = stamp;
      this.#needsPoll = false;
      // A watch is an edge, not one notification per page. Drain a full live page too;
      // yield between pages so the lease heartbeat can run through a large backlog.
      if (catchingUp || tail.events.length >= this.config.maxReadEvents) {
        this.#needsPoll = true;
        setImmediate(() => this.schedule());
      }
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
      if ((isWork(event) || this.callbacks.replayAll) && !this.#delivered(event)) {
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
    if (!immediate && !firstAnchor && this.#persistedCursor !== undefined && Date.now() - this.#lastCheckpointAt < CURSOR_CHECKPOINT_MS) {
      // Deferred ignored-only progress owes one checkpoint; a one-shot maintenance
      // wake replays it through the same beforePoll/canConsumeMesh gates.
      if (!this.#checkpointTimer && !this.#closed) {
        this.#checkpointTimer = setTimeout(() => {
          this.#checkpointTimer = undefined;
          this.#schedule(false, true);
        }, Math.max(1, CURSOR_CHECKPOINT_MS - (Date.now() - this.#lastCheckpointAt)));
        this.#checkpointTimer.unref();
      }
      return;
    }
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
