import fs, { type FSWatcher } from "node:fs";
import { MeshBackgroundRetry, type MeshOutageAlarm } from "../core/atomic-write.js";
import path from "node:path";
import { writeJsonAtomic } from "../core/atomic-write.js";
import { warnWatchFallback } from "../core/watch-fallback.js";
import type { FabricMeshConfig } from "../config.js";
import { meshCursorAtStart, meshCursorGeneration, type MeshEvent, type MeshStore } from "../mesh/store.js";

const CURSOR_CHECKPOINT_MS = 10_000;
// One-shot retry after a failed/deferred poll (lock timeout, dispatch throw); never armed while idle.
const MESH_MONITOR_RETRY_MS = 5_000;
// No notification carrier: bounded recovery only, never a fast idle loop.
const MESH_MONITOR_FALLBACK_MS = 60_000;
type MonitorCursor = { cursor: number; last?: { sequence: number; id: string } };
/**
 * Work topics (smarty-dev#754 §3.2 step 3): agent-to-agent acks, asks, handoffs. They are
 * durable work, so they skip the replay window and are read back from the mesh archive.
 */
const WORK_TOPIC_PREFIX = "fleet.";

/** Owns observation resources and the format-1 cursor, never actor ownership or dispatch policy. */
export class ActorMeshMonitor {
  // The monitor has no identity or publish of its own: its owner supplies the outage sink (smarty-dev#8825).
  readonly #backgroundPoll = new MeshBackgroundRetry("actor mesh monitor")
    .alarmsTo((alarm) => this.callbacks.outageAlarm?.(alarm));
  #safetyNetTimer: NodeJS.Timeout | undefined;
  #dueTimer: NodeJS.Timeout | undefined;
  #dueAt: number | undefined;
  #retryTimer: NodeJS.Timeout | undefined;
  /** One-shot ignored-only cursor checkpoint, armed only while the cursor is dirty. */
  #checkpointTimer: NodeJS.Timeout | undefined;
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
      /** New admission only; receipt work for an already consumed prefix remains allowed. */
      canAdmit?: (() => boolean) | undefined;
      /** Owner-defined durable input also survives replay expiry and archive rewrites. */
      isDurableWork?: ((event: MeshEvent) => boolean) | undefined;
      /** "deferred": hold on every topic; false: ordinary queue backpressure; "ignored": no local delivery. */
      onEvent(event: MeshEvent): boolean | void | "ignored" | "deferred";
      /** smarty-dev#11172: a log read finished; the safe cursor is updated and polling is clear. */
      onPollComplete?: (() => void) | undefined;
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
    try {
      // Watch the existing parent directory, not files replaced by atomic rename.
      // Non-recursive directory watches work on Windows too; a nameless rename
      // is a wake, not evidence that nothing changed. The stamp filters duplicates.
      const watcher = fs.watch(this.mesh.root, { persistent: false }, (_event, filename) => {
        const name = filename == null ? undefined : path.basename(filename.toString());
        // schedules.json re-arms the due timer when another process schedules.
        if (name !== undefined && name !== "events.jsonl" && name !== "generation" && name !== "schedules.json") return;
        this.#schedule();
      });
      this.#watcher = watcher;
      watcher.on("error", (error) => this.#fallback(watcher, error));
      // A watch that stops on its own (for example a removed directory) is unhealthy too.
      watcher.on("close", () => this.#fallback(watcher, new Error("watch closed")));
    } catch (error) {
      // An unsupported watch (including a missing directory) takes the fallback.
      // ENOSPC here is the inotify watch limit (smarty-dev#5247), not a full disk: poll instead
      // and say so once, so nobody hunts for free space and the session never crashes.
      warnWatchFallback("actor mesh monitor", this.mesh.root, error, Math.max(MESH_MONITOR_FALLBACK_MS, this.config.actorPollMs));
    }
    // smarty-dev#11172: a healthy watch has no idle timer; only a failed watch polls.
    if (!this.#watcher) this.#startSafetyNet();
    this.schedule();
  }

  caughtUp(): boolean {
    return !this.#polling && this.#archiveAfter === undefined && this.#safeCursor.cursor === this.mesh.latestOffset();
  }

  /** Checked release receipt; normal best-effort writes are NOT a handover barrier. */
  checkpointForRelease(assertHeld?: () => void): void {
    assertHeld?.();
    if (this.#polling) throw new Error("Actor mesh cursor still polling");
    if (this.callbacks.cursorPath) {
      writeJsonAtomic(this.callbacks.cursorPath, { format: 1, ...this.#safeCursor }, { durable: true, space: 2 });
      assertHeld?.();
      this.#persistedCursor = JSON.stringify(this.#safeCursor);
    }
    this.releaseCheckpointReceipt();
  }

  releaseCheckpointReceipt(): unknown {
    if (this.#polling) throw new Error("Actor mesh cursor still polling");
    if (this.callbacks.cursorPath && JSON.stringify(this.#readCursor()) !== JSON.stringify(this.#safeCursor)) {
      throw new Error("Actor mesh cursor release readback differs");
    }
    return { format: 1, ...this.#safeCursor, persisted: !!this.callbacks.cursorPath };
  }

  #isWork(event: MeshEvent): boolean {
    return event.topic.startsWith(WORK_TOPIC_PREFIX) || this.callbacks.isDurableWork?.(event) === true;
  }

  #mayAdmit(): boolean {
    return !this.#closed && this.callbacks.canConsumeMesh?.() !== false && this.callbacks.canAdmit?.() !== false;
  }

  close(): void {
    this.#closed = true;
    if (this.#safetyNetTimer) clearInterval(this.#safetyNetTimer);
    this.#safetyNetTimer = undefined;
    if (this.#dueTimer) clearTimeout(this.#dueTimer);
    this.#dueTimer = undefined;
    if (this.#retryTimer) clearTimeout(this.#retryTimer);
    this.#retryTimer = undefined;
    if (this.#checkpointTimer) clearTimeout(this.#checkpointTimer);
    this.#checkpointTimer = undefined;
    this.#watcher?.close();
    this.#watcher = undefined;
    if (this.#started) this.#persistCursor(true);
  }

  /** Explicit delivery/resume/queue-capacity wake; never delayed by watch traffic. */
  schedule(): void { this.#schedule(true); }

  /** Ownership/registry hints must run beforePoll even when the event log is unchanged. */
  maintain(): void { this.#schedule(false, true); }

  /** A required notification carrier failed; recover at most once a minute. */
  watchUnavailable(): void { this.#startSafetyNet(); this.maintain(); }

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
        const result = this.#closed ? "done" : await this.#backgroundPoll.run(() => this.#poll(force, maintain));
        // A healthy watch has no safety net (smarty-dev#11172): an unfinished poll, or one a lapsed
        // mesh lease refused (regain has no event), retries once, later.
        const deferred = result !== "done" || (this.#needsPoll && this.callbacks.canConsumeMesh?.() === false);
        if (deferred && this.#watcher && !this.#retryTimer && !this.#closed) {
          this.#retryTimer = setTimeout(() => { this.#retryTimer = undefined; this.#schedule(false, true); },
            Math.max(MESH_MONITOR_RETRY_MS, this.#backgroundPoll.waitMs));
          this.#retryTimer.unref();
        }
      } finally {
        this.#scheduled = false;
        // A callback can free a receiver slot or append another page while dispatching.
        // Retain that wake until the background operation releases its running fence.
        if (this.#wakePending) this.#schedule();
      }
    });
  }

  #fallback(watcher: FSWatcher, error: unknown): void {
    if (this.#closed || this.#watcher !== watcher) return;
    warnWatchFallback("actor mesh monitor", this.mesh.root, error, Math.max(MESH_MONITOR_FALLBACK_MS, this.config.actorPollMs));
    watcher.close();
    this.#watcher = undefined;
    this.#startSafetyNet();
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

  #startSafetyNet(): void {
    if (this.#safetyNetTimer) clearInterval(this.#safetyNetTimer);
    // ponytail: keep one bounded timer, not a watcher-retry subsystem. ONLY a
    // failed/unsupported/closed watch runs it: without a notification carrier,
    // one-minute recovery bounds missed work without restoring fast idle polling.
    // Healthy watches rely on edges, explicit wakes and owed-work deadlines.
    this.#safetyNetTimer = setInterval(() => this.#schedule(true, true), Math.max(MESH_MONITOR_FALLBACK_MS, this.config.actorPollMs));
    this.#safetyNetTimer.unref();
  }

  #meshStamp(): string | undefined {
    try {
      // schedules.json is stamped so a schedule another process adds re-arms the due timer.
      return ["events.jsonl", "generation", "schedules.json"].map(name => {
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
    // Ownership/registry notifications (or degraded-watch recovery) retain the
    // manager's maintenance obligations even with a quiet, unchanged event log.
    if (!this.callbacks.beforePoll() || !this.#mayAdmit()) {
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
        if (!this.#mayAdmit()) return;
        try { dueAt = this.mesh.nextScheduleDueAt?.(); } catch { dueAt = undefined; }
      }
      if (!this.#mayAdmit()) return;
      this.#armDue(dueAt);
      if (this.#archiveAfter !== undefined && !this.#catchUpArchive()) return;
      // Live and catch-up both read whole pages. A throwing dispatch restores the boundary
      // before its event, so an empty later poll cannot checkpoint past failed work.
      const start = this.#offset;
      const tail = this.mesh.tail(start, this.config.maxReadEvents);
      if (!this.#mayAdmit()) return;
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
        if (!this.#mayAdmit()) {
          this.#offset = index === 0 ? start : tail.cursors?.[index - 1] ?? start;
          this.#safeCursor = { cursor: this.#offset, ...(this.#last ? { last: this.#last } : {}) };
          return;
        }
        if (this.#delivered(event)) continue;
        if (this.#replayFloor !== undefined && event.createdAt < this.#replayFloor && !this.#isWork(event) && !this.callbacks.replayAll) {
          if (typeof event.sequence === "number" && typeof event.id === "string") this.#last = { sequence: event.sequence, id: event.id };
          continue;
        }
        let accepted: boolean | void | "ignored" | "deferred";
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
        // A cut inside the synchronous callback must never turn false into a live drop.
        if (accepted === false && this.callbacks.canAdmit?.() === false) {
          this.#offset = index === 0 ? start : tail.cursors?.[index - 1] ?? start;
          this.#safeCursor = { cursor: this.#offset, ...(this.#last ? { last: this.#last } : {}) };
          return;
        }
        // Explicit deferral always holds input (including live non-work). Ordinary full
        // receivers still hold only catch-up/durable work (smarty-dev#472, smarty-dev#754).
        if (accepted === "deferred" || (accepted === false && (catchingUp || this.#isWork(event) || this.callbacks.replayAll))) {
          // Keep the cursor on this event and offer it again; earlier events are delivered.
          // The boundary comes from this same read, so a compaction since cannot move it.
          this.#offset = index === 0 ? start : tail.cursors?.[index - 1] ?? start;
          this.#writeCursor(handedOn);
          return;
        }
        if (accepted !== false && accepted !== "ignored") handedOn = true;
        if (typeof event.sequence === "number" && typeof event.id === "string") this.#last = { sequence: event.sequence, id: event.id };
        if (this.callbacks.canAdmit?.() === false) {
          // Finish the accepted prefix's receipt; never move over the next held input.
          this.#offset = tail.cursors?.[index] ?? start;
          this.#writeCursor(handedOn);
          return;
        }
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
      // An event, not a timer: a consumer blocked on caughtUp() re-checks only when a read ends.
      if (!this.#closed) {
        try { this.callbacks.onPollComplete?.(); } catch { /* observers must not break the poll */ }
      }
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
    if (!this.#mayAdmit()) return false;
    const oldest = this.mesh.oldestSequence?.();
    if (!this.mesh.read || oldest === undefined || this.#archiveAfter === undefined || this.#archiveAfter + 1 >= oldest) {
      this.#archiveAfter = undefined;
      return true;
    }
    const page = this.mesh.read({ after: this.#archiveAfter, limit: this.config.maxReadEvents });
    const older = page.filter((event) => event.sequence < oldest);
    let handedOn = false;
    for (const event of older) {
      if (!this.#mayAdmit()) return false;
      if ((this.#isWork(event) || this.callbacks.replayAll) && !this.#delivered(event)) {
        const accepted = this.callbacks.onEvent(event);
        if (this.callbacks.canConsumeMesh?.() === false) return false;
        if (accepted === "deferred" || accepted === false) {
          this.#writeCursor(handedOn);
          return false;
        }
        if (accepted !== "ignored") handedOn = true;
      }
      // Every event older than the live log counts as passed, work or not, so the live log's
      // first event follows it with no gap. Only a relevant delivery checkpoints immediately.
      this.#archiveAfter = event.sequence;
      this.#last = { sequence: event.sequence, id: event.id };
      if (this.callbacks.canAdmit?.() === false) {
        this.#writeCursor(handedOn);
        return false;
      }
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
    const wait = CURSOR_CHECKPOINT_MS - (Date.now() - this.#lastCheckpointAt);
    if (!immediate && !firstAnchor && this.#persistedCursor !== undefined && wait > 0) {
      // smarty-dev#11172: the owed checkpoint gets its own deadline, not the next unrelated event.
      if (!this.#checkpointTimer && !this.#closed) {
        this.#checkpointTimer = setTimeout(() => { this.#checkpointTimer = undefined; this.#persistCursor(false); }, wait);
        this.#checkpointTimer.unref();
      }
      return;
    }
    if (this.#checkpointTimer) clearTimeout(this.#checkpointTimer);
    this.#checkpointTimer = undefined;
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
