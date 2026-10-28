import { randomUUID } from "node:crypto";
import { diagnostics } from "../core/diagnostics.js";
import { MeshBackgroundQueue, MeshBackgroundRetry } from "../core/atomic-write.js";
import { rethrowMeshLockTimeout } from "../core/atomic-write.js";
import { isWatchLimitError } from "../core/watch-fallback.js";
import { DegradedObservation } from "../core/observation-watch.js";
import { watchBridgeStore } from "../mesh/change-notifier.js";
import { MeshStore, meshOutageAlarmSink, type MeshIdentity, type MeshStateEntry } from "../mesh/store.js";
import type { FabricParticipantInfo, FabricParticipantSource } from "../topology/types.js";
import {
  FABRIC_LIFECYCLE_SUBSCRIPTION_PREFIX,
  FABRIC_PARTICIPANT_LIFECYCLE_TOPIC,
  lifecycleEventFromMesh,
  lifecycleSourceIdentity,
  lifecycleSubscriptionFromValue,
  type FabricLifecycleEvent,
  type FabricLifecyclePublishRequest,
  type FabricLifecycleSubscription,
  type FabricLifecycleSubscriptionRequest,
} from "./types.js";

export interface LifecycleBrokerOptions {
  enabled: boolean;
  pollMs: number;
  maxReadEvents: number;
  canConsumeMesh?: () => boolean;
}

export type FabricLifecycleDeliveryHandler = (
  subscription: FabricLifecycleSubscription,
  event: FabricLifecycleEvent,
) => Promise<void> | void;

const subscriptionKey = (id: string): string =>
  FABRIC_LIFECYCLE_SUBSCRIPTION_PREFIX + id;

/** Failed-drain retry ceiling: unresolved work is retried by one-shot timers, never an interval. */
const DRAIN_RETRY_MAX_MS = 30_000;
/** Watcher re-arm backoff: bounded attempts, then a degraded-only minute carrier. */
const WATCH_REARM_BASE_MS = 1_000;
const WATCH_REARM_MAX_MS = 60_000;
const WATCH_REARM_ATTEMPTS = 6;

export class LifecycleBroker {
  readonly #backgroundPoll = new MeshBackgroundRetry("lifecycle cursor poll")
    .alarmsTo(meshOutageAlarmSink(() => ({ mesh: this.mesh, from: this.identity })));
  readonly #backgroundPublish = new MeshBackgroundQueue("participant lifecycle event");

  publishBackground(request: FabricLifecyclePublishRequest): Promise<void> {
    return this.#backgroundPublish.enqueue(() => this.publish(request));
  }
  readonly #pollMs: number;
  readonly #maxReadEvents: number;
  #started = false;
  #stopWatch: (() => void) | undefined;
  /** Unsubscribes the state-projection wake (a NATS projector applying a change no watched file shows). */
  #stopStateChanges: (() => void) | undefined;
  #rearmTimer: NodeJS.Timeout | undefined;
  #rearmAttempts = 0;
  #watchWarned = false;
  readonly #observation = new DegradedObservation("lifecycle", () => this.#wake(), () => {
    this.#armWatch();
    if (!this.#stopWatch) return false;
    this.#rearmAttempts = 0;
    return true;
  });
  observationHealth(): string[] { return this.#observation.health ? [this.#observation.health] : []; }
  #retryTimer: NodeJS.Timeout | undefined;
  #retryDelay = 0;
  #polling: Promise<void> | undefined;
  #publishTail: Promise<void> = Promise.resolve();
  #pollScheduled = false;
  /** A wake arrived that no completed drain has served yet (notification, failed or gated drain). */
  #dirty = false;
  /** The drain loop is active; a wake during it only sets #dirty and the loop runs again. */
  #draining = false;
  /** The current drain swallowed a failure (delivery, cursor or receipt write) and left work behind. */
  #drainFailed = false;
  #closed = false;
  #paused = false;
  #strictPause = false;
  #releaseEpoch = 0;
  #releaseCheckpoint: { epoch: number; state: string } | undefined;
  readonly #onceReceipts = new Map<string, { key: string; version: number; eventId?: string; afterSequence: number }>();
  /** Cursors past events that matched nothing, not yet saved, by subscription id. */
  readonly #unsaved = new Map<string, number>();
  /** Accepted delivery is never replayed to recover a storage receipt. */
  readonly #delivered = new Map<string, { entry: MeshStateEntry; subscription: FabricLifecycleSubscription }>();

  constructor(
    readonly mesh: MeshStore,
    readonly identity: MeshIdentity,
    readonly participants: FabricParticipantSource,
    readonly options: LifecycleBrokerOptions,
    readonly deliver: FabricLifecycleDeliveryHandler,
  ) {
    this.#pollMs = Math.max(20, options.pollMs);
    this.#maxReadEvents = Math.max(1, options.maxReadEvents);
  }

  /** Drains at once, then on mesh notifications. Healthy idle has no timer;
   * failed work retries, and exhausted watches use the degraded-only minute carrier. */
  start(): void {
    if (!this.options.enabled || this.#started) return;
    this.#started = true;
    this.#closed = false;
    this.#observation.open();
    this.#armWatch();
    // A subscription may become visible here only when the state projector catches up, after its event's
    // file wake was already drained against the stale view: that projection wakes a fresh drain.
    this.#stopStateChanges ??= this.mesh.subscribeStateChanges(() => this.#wake());
    this.#wake();
  }

  #armWatch(): void {
    if (this.#closed || this.#stopWatch) return;
    try {
      this.#stopWatch = watchBridgeStore(this.mesh.root, () => {
        this.#rearmAttempts = 0; // a delivered notification proves the watch works
        this.#wake();
      }, (error) => this.#watchFailed(error));
    } catch (error) {
      this.#watchFailed(error);
    }
  }

  // One rescan plus bounded re-arms; exhaustion retains the shared degraded-only carrier.
  #watchFailed(error: unknown): void {
    this.#stopWatch?.();
    this.#stopWatch = undefined;
    if (this.#closed || this.#observation.degraded) return;
    const delay = Math.min(WATCH_REARM_MAX_MS, WATCH_REARM_BASE_MS * 2 ** this.#rearmAttempts);
    if (!this.#watchWarned) {
      this.#watchWarned = true;
      const cause = isWatchLimitError(error)
        ? "ENOSPC: inotify watch limit, not disk space (raise fs.inotify.max_user_watches)"
        : error instanceof Error ? error.message : String(error);
      try {
        diagnostics.warn("src/lifecycle/broker.ts", `[pi-fabric] lifecycle broker: mesh notifications failed for ${this.mesh.root} (${cause}); ` +
          `rescanned once, re-arming in ${delay} ms with bounded retries. Normal operation has no polling; ` +
          "exhausted watches scan and re-arm once per minute.");
      } catch {}
    }
    this.#wake();
    if (this.#rearmTimer) return;
    if (this.#rearmAttempts >= WATCH_REARM_ATTEMPTS) { this.#observation.enter(); return; }
    this.#rearmAttempts++;
    this.#rearmTimer = setTimeout(() => {
      this.#rearmTimer = undefined;
      if (this.#closed) return;
      this.#armWatch();
      if (this.#stopWatch) this.#wake(); // notifications may have been missed while unarmed
    }, delay);
    this.#rearmTimer.unref?.();
  }

  #wake(): void {
    this.#dirty = true;
    this.#schedulePoll();
  }

  publish(
    request: FabricLifecyclePublishRequest,
  ): Promise<FabricLifecycleEvent | undefined> {
    if (
      !this.options.enabled ||
      this.#closed ||
      !this.#isObserved(request.source.id, request.event)
    ) return Promise.resolve(undefined);
    const operation = this.#publishTail.then(async () => {
      const occurredAt = request.occurredAt ?? Date.now();
      const event = await this.mesh.publish({
        topic: FABRIC_PARTICIPANT_LIFECYCLE_TOPIC,
        kind: request.event,
        from: lifecycleSourceIdentity(request.source),
        data: {
          version: 1,
          event: request.event,
          source: request.source,
          occurredAt,
          ...(request.runId ? { runId: request.runId } : {}),
          ...(request.status ? { status: request.status } : {}),
          ...(request.data === undefined ? {} : { payload: request.data }),
        },
      });
      this.#wake();
      return lifecycleEventFromMesh(event);
    });
    this.#publishTail = operation.then(
      () => undefined,
      () => undefined,
    );
    return operation;
  }

  async subscribe(
    request: FabricLifecycleSubscriptionRequest,
  ): Promise<FabricLifecycleSubscription> {
    if (!this.options.enabled) {
      throw new Error("Fabric mesh is disabled; lifecycle subscriptions are unavailable");
    }
    const from = request.from.trim();
    const to = request.to.trim();
    if (!from) throw new Error("Lifecycle subscription source is empty");
    if (!to) throw new Error("Lifecycle subscription target is empty");
    if (from === to) {
      throw new Error("Lifecycle subscriptions cannot target their own source");
    }
    const events = [...new Set(request.events)];
    if (events.length === 0) throw new Error("Lifecycle subscription requires at least one event");

    this.#assertSubscriptionAdmission();
    await this.participants.refresh();
    this.#assertSubscriptionAdmission();
    const source = this.participants.get(from);
    if (!source || source.stale) throw new Error("Unknown or stale lifecycle source: " + from);
    const target = this.participants.get(to);
    if (!target || target.stale) throw new Error("Unknown or stale lifecycle target: " + to);
    if (!target.capabilities.includes(request.delivery)) {
      throw new Error(
        "Fabric participant " + to + " does not support " + request.delivery + " delivery",
      );
    }

    const now = Date.now();
    const subscription: FabricLifecycleSubscription = {
      format: 1,
      id: randomUUID().replaceAll("-", ""),
      from,
      events,
      to,
      delivery: request.delivery,
      triggerTurn: request.triggerTurn,
      once: request.once === true,
      afterSequence: this.mesh.latestSequence(),
      createdAt: now,
      updatedAt: now,
      createdBy: structuredClone(this.identity),
    };
    await this.mesh.writeBatch({ identity: this.identity, lockTiming: { operation: "lifecycle-subscribe" },
      ops: [{ kind: "put", key: subscriptionKey(subscription.id), value: subscription, ifVersion: 0 }],
      beforeCommit: () => this.#assertSubscriptionAdmission() });
    this.#wake();
    return structuredClone(subscription);
  }

  list(input: { from?: string; to?: string } = {}): FabricLifecycleSubscription[] {
    return this.mesh
      .listAll(FABRIC_LIFECYCLE_SUBSCRIPTION_PREFIX)
      .flatMap((entry) => {
        const subscription = lifecycleSubscriptionFromValue(entry.value);
        if (!subscription || entry.key !== subscriptionKey(subscription.id)) return [];
        if (input.from && subscription.from !== input.from) return [];
        if (input.to && subscription.to !== input.to) return [];
        return [structuredClone(subscription)];
      });
  }

  #assertSubscriptionAdmission(): void {
    if (this.#paused && this.#strictPause) throw new Error("Lifecycle release subscription admission is held");
  }

  async unsubscribe(id: string): Promise<{ removed: boolean }> {
    this.#assertSubscriptionAdmission();
    const key = subscriptionKey(id.trim());
    const entry = this.mesh.get(key, { fresh: true });
    if (!entry || !lifecycleSubscriptionFromValue(entry.value)) return { removed: false };
    const [result] = await this.mesh.writeBatch({ identity: this.identity, lockTiming: { operation: "lifecycle-unsubscribe" },
      ops: [{ kind: "delete", key, ifVersion: entry.version }],
      beforeCommit: () => this.#assertSubscriptionAdmission() });
    return { removed: result?.applied === true };
  }

  pause(strict = false): void {
    if (!this.#paused) this.#releaseEpoch++;
    this.#paused = true;
    this.#strictPause ||= strict;
  }
  resume(): void {
    if (this.#paused) {
      this.#paused = false; this.#strictPause = false; this.#releaseEpoch++;
    }
    this.#wake();
  }
  async checkpointForRelease(valid?: () => boolean): Promise<void> {
    const epoch = this.#releaseEpoch;
    const check = () => {
      if (!this.#paused || epoch !== this.#releaseEpoch || valid?.() === false) throw new Error("Lifecycle release checkpoint is stale or not paused");
    };
    check();
    await this.#backgroundPublish.checkpointForRelease();
    check();
    await this.#publishTail;
    check();
    await this.#polling;
    check();
    for (const id of this.#delivered.keys()) {
      await this.#confirmDelivered(id, valid ? check : undefined);
      check();
    }
    if (this.#delivered.size) throw new Error("Lifecycle release has unconfirmed delivery receipts");
    const state = JSON.stringify(this.#readReleaseState());
    check();
    this.#releaseCheckpoint = { epoch, state };
  }

  #readReleaseState(): unknown {
    const subscriptions = this.mesh.listAll(FABRIC_LIFECYCLE_SUBSCRIPTION_PREFIX, { fresh: true })
      .sort((a, b) => a.key.localeCompare(b.key));
    for (const entry of subscriptions) {
      const subscription = lifecycleSubscriptionFromValue(entry.value);
      if (!subscription || entry.key !== subscriptionKey(subscription.id)) throw new Error("Lifecycle subscription disk readback is invalid");
    }
    for (const receipt of this.#onceReceipts.values()) {
      if (this.mesh.get(receipt.key, { fresh: true })) throw new Error("Lifecycle confirmed once-delete disk readback differs");
    }
    return { subscriptions, confirmedOnceDeletes: [...this.#onceReceipts.entries()],
      uncommittedSkips: [...this.#unsaved.entries()] };
  }

  releaseCheckpointReceipt(): unknown {
    const saved = this.#releaseCheckpoint;
    if (!this.#paused || !saved || saved.epoch !== this.#releaseEpoch || this.#delivered.size || this.#polling) {
      throw new Error("Lifecycle release checkpoint receipt is unavailable or unsettled");
    }
    const state = this.#readReleaseState();
    if (JSON.stringify(state) !== saved.state) throw new Error("Lifecycle release checkpoint disk readback changed");
    return { admissionHeld: true, state, receiptScope: "persisted subscriptions and exact confirmed delivery/once receipts, not UI quiet" };
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    this.#started = false;
    this.#observation.close();
    this.#stopWatch?.();
    this.#stopWatch = undefined;
    this.#stopStateChanges?.();
    this.#stopStateChanges = undefined;
    clearTimeout(this.#rearmTimer);
    this.#rearmTimer = undefined;
    clearTimeout(this.#retryTimer);
    this.#retryTimer = undefined;
    await this.#backgroundPublish.close();
    await this.#publishTail;
    await this.#polling?.catch(() => undefined);
  }

  #schedulePoll(): void {
    if (
      this.#pollScheduled ||
      this.#closed ||
      !this.options.enabled
    ) return;
    this.#pollScheduled = true;
    queueMicrotask(() => {
      this.#pollScheduled = false;
      if (this.#closed) return;
      void this.#runDrains();
    });
  }

  #gated(): boolean {
    return this.#closed || this.#paused || !this.options.enabled || this.options.canConsumeMesh?.() === false;
  }

  // Serial drain loop. A wake while a drain runs sets #dirty, so the loop drains again rather
  // than dropping it. A gated drain keeps #dirty for resume() or the next notification.
  async #runDrains(): Promise<void> {
    if (this.#draining) return;
    this.#draining = true;
    try {
      while (this.#dirty && !this.#gated()) {
        if (this.#retryTimer) { clearTimeout(this.#retryTimer); this.#retryTimer = undefined; }
        this.#dirty = false;
        this.#drainFailed = false;
        const outcome = await this.#backgroundPoll.run(() => this.#poll());
        if (outcome === "done" && !this.#drainFailed && this.#delivered.size === 0) {
          this.#retryDelay = 0;
          continue;
        }
        // Failed, deferred or left an unconfirmed receipt: retry once after backoff, no event needed.
        this.#dirty = true;
        this.#scheduleRetry(outcome);
        return;
      }
    } finally {
      this.#draining = false;
    }
  }

  #scheduleRetry(outcome: "done" | "retry" | "failed" | "skipped"): void {
    if (this.#closed) return;
    let delay: number;
    if (outcome === "retry" || outcome === "skipped") {
      delay = Math.max(1, this.#backgroundPoll.waitMs);
    } else {
      this.#retryDelay = Math.min(DRAIN_RETRY_MAX_MS, this.#retryDelay ? this.#retryDelay * 2 : this.#pollMs);
      delay = this.#retryDelay;
    }
    clearTimeout(this.#retryTimer);
    this.#retryTimer = setTimeout(() => {
      this.#retryTimer = undefined;
      this.#wake();
    }, delay);
    this.#retryTimer.unref?.();
  }

  /** Contains a swallowed drain-step failure but records it, so the drain is retried. */
  readonly #contain = (error: unknown): undefined => {
    this.#drainFailed = true;
    return rethrowMeshLockTimeout(error);
  };

  async #poll(): Promise<void> {
    if (this.#gated()) { this.#dirty = true; return; }
    if (this.#polling) return this.#polling;
    const operation = this.#drain();
    this.#polling = operation;
    try {
      await operation;
    } finally {
      if (this.#polling === operation) this.#polling = undefined;
    }
  }

  async #drain(): Promise<void> {
    const entries = this.mesh.listAll(FABRIC_LIFECYCLE_SUBSCRIPTION_PREFIX);
    const listed = new Set<string>();
    let latestSequence: number | undefined;
    // One directory rebuild serves every subscription in this cycle that still
    // needs a target lookup (upstream ee2b27b85).
    let directory: Map<string, FabricParticipantInfo> | undefined;
    for (const entry of entries) {
      if (this.options.canConsumeMesh?.() === false) { this.#dirty = true; return; }
      const subscription = lifecycleSubscriptionFromValue(entry.value);
      if (!subscription || entry.key !== subscriptionKey(subscription.id)) continue;
      listed.add(subscription.id);
      if (this.#delivered.has(subscription.id)) {
        // Retry only the cursor/delete receipt; use a fresh poll after success.
        await this.#confirmDelivered(subscription.id).catch(this.#contain);
        continue;
      }
      // Only the target's host drains a subscription. Pass over other hosts' targets (from
      // memory) and caught-up subscriptions before the directory read: that read parses every
      // participant and host record, and ran for every subscription on every poll of every
      // host, about a quarter of a core per idle Pi on the fleet mesh (smarty-dev#557).
      if (this.participants.publishes?.(subscription.to) === false) continue;
      latestSequence ??= this.mesh.latestSequence();
      if (latestSequence <= this.#cursor(subscription)) continue;
      directory ??= new Map(
        this.participants.list({ scope: "project" }).map((record) => [record.id, record]),
      );
      const target = directory.get(subscription.to) ?? this.participants.get(subscription.to);
      if (!target || target.stale || !target.local) continue;
      if (this.#paused) return;
      await this.#drainSubscription(entry, subscription);
      if (this.#paused) return;
    }
    if (this.#paused) return;
    for (const id of this.#unsaved.keys()) if (!listed.has(id)) this.#unsaved.delete(id);
  }

  #cursor(subscription: FabricLifecycleSubscription): number {
    return Math.max(subscription.afterSequence, this.#unsaved.get(subscription.id) ?? 0);
  }

  // Each save rewrites the whole shared state file, and the owner saved after every new mesh
  // event: most of the fleet's state writes (smarty-dev#557). A cursor that passed only events
  // matching nothing stays in memory until it leads the saved one by a read page. A restart
  // scans those events again and skips them again. A delivery, a matching event skipped for
  // good, and an error to set or clear are saved at once.
  #keepUnsaved(subscription: FabricLifecycleSubscription, cursor: number): boolean {
    if (this.options.canConsumeMesh?.() === false || subscription.lastError !== undefined ||
        cursor - subscription.afterSequence >= this.#maxReadEvents) return false;
    this.#unsaved.set(subscription.id, cursor);
    return true;
  }

  async #drainSubscription(
    initialEntry: MeshStateEntry,
    initial: FabricLifecycleSubscription,
  ): Promise<void> {
    let entry = initialEntry;
    let subscription = initial;
    while (!this.#gated()) {
      const latestSequence = this.mesh.latestSequence();
      const from = this.#cursor(subscription);
      if (latestSequence <= from) return;
      const events = this.mesh.read({
        after: from,
        limit: this.#maxReadEvents,
      });
      if (events.length === 0) {
        if (this.#keepUnsaved(subscription, latestSequence)) return;
        await this.#replace(entry, {
          ...subscription,
          afterSequence: latestSequence,
          updatedAt: Date.now(),
        }, () => { if (this.#paused) throw new Error("Lifecycle input cursor is held for release"); }).then(() => this.#unsaved.delete(subscription.id), this.#contain);
        return;
      }

      let cursor = from;
      let decided = false;
      let lastDeliveredAt = subscription.lastDeliveredAt;
      let lastEventId = subscription.lastEventId;
      for (const meshEvent of events) {
        if (this.#paused || this.options.canConsumeMesh?.() === false) { this.#dirty = true; return; }
        const lifecycle = lifecycleEventFromMesh(meshEvent);
        if (!lifecycle) {
          cursor = Math.max(cursor, meshEvent.sequence);
          continue;
        }
        const candidate =
          lifecycle.source.id === subscription.from &&
          subscription.events.includes(lifecycle.event);
        if (!candidate || !this.#sourceIsCurrentOwner(lifecycle)) {
          decided ||= candidate;
          cursor = lifecycle.sequence;
          continue;
        }
        try {
          await this.deliver(subscription, lifecycle);
        } catch (error) {
          // The handler failed: the event stays unconsumed and must be retried without another event.
          this.#drainFailed = true;
          if (this.options.canConsumeMesh?.() === false) return;
          const failed: FabricLifecycleSubscription = {
            ...subscription,
            afterSequence: cursor,
            updatedAt: Date.now(),
            lastError: error instanceof Error ? error.message : String(error),
          };
          await this.#replace(entry, failed).then(() => this.#unsaved.delete(subscription.id), this.#contain);
          return;
        }
        cursor = lifecycle.sequence;
        decided = true;
        lastDeliveredAt = Date.now();
        lastEventId = lifecycle.id;
        const delivered = { ...subscription, afterSequence: cursor, updatedAt: Date.now(), lastDeliveredAt, lastEventId };
        delete delivered.lastError;
        this.#delivered.set(subscription.id, { entry, subscription: delivered });
        // Preserve the delivery receipt, not the cursor, across lease loss.
        if (this.options.canConsumeMesh?.() === false) { this.#dirty = true; return; }
        const confirmed = await this.#confirmDelivered(subscription.id).catch(this.#contain);
        if (subscription.once || !confirmed) return;
        entry = confirmed; subscription = delivered;
        if (this.#paused) return;
      }

      if (this.#paused) return;
      if (!decided && this.#keepUnsaved(subscription, cursor)) {
        if (events.length < this.#maxReadEvents) return;
        continue;
      }
      const updated: FabricLifecycleSubscription = {
        ...subscription,
        afterSequence: cursor,
        updatedAt: Date.now(),
        ...(lastDeliveredAt !== undefined ? { lastDeliveredAt } : {}),
        ...(lastEventId !== undefined ? { lastEventId } : {}),
      };
      delete updated.lastError;
      if (cursor === subscription.afterSequence && subscription.lastError === undefined) {
        if (events.length < this.#maxReadEvents) return;
        continue;
      }
      const next = await this.#replace(entry, updated, () => {
        if (this.#paused) throw new Error("Lifecycle input cursor is held for release");
      }).catch(this.#contain);
      if (!next) return;
      this.#unsaved.delete(subscription.id);
      entry = next;
      subscription = updated;
      if (events.length < this.#maxReadEvents) return;
    }
    this.#dirty = true; // a gate closed mid-drain: resume() or the next notification finishes it
  }

  // Both answers are final: a skipped event is skipped for good (the cursor moves past it), and
  // a delivered one can end a once subscription. So ownership is read from the current mesh
  // state, not a recent cached parse. This runs only for events that already match a
  // subscription's source and event type, so it costs about one check per delivery.
  #sourceIsCurrentOwner(event: FabricLifecycleEvent): boolean {
    return this.#ownsSource(event, this.participants.get(event.source.id, undefined, { fresh: true }));
  }

  #ownsSource(event: FabricLifecycleEvent, participant: FabricParticipantInfo | undefined): boolean {
    return Boolean(
      participant &&
      !participant.stale &&
      event.source.ownerHostId &&
      event.source.ownerIdentityId &&
      participant.kind === event.source.kind &&
      participant.rootId === event.source.rootId &&
      participant.runner === event.source.runner &&
      participant.ownerHostId === event.source.ownerHostId &&
      participant.ownerIdentityId === event.source.ownerIdentityId
    );
  }

  // Fresh: this decides whether the event is published at all, so a subscription another host
  // created a moment ago must count (a cached listing would drop the event for good).
  #isObserved(sourceId: string, event: FabricLifecyclePublishRequest["event"]): boolean {
    return this.mesh
      .listAll(FABRIC_LIFECYCLE_SUBSCRIPTION_PREFIX, { fresh: true })
      .some((entry) => {
        const subscription = lifecycleSubscriptionFromValue(entry.value);
        return (
          subscription !== undefined &&
          entry.key === subscriptionKey(subscription.id) &&
          subscription.from === sourceId &&
          subscription.events.includes(event)
        );
      });
  }

  async #confirmDelivered(id: string, check?: () => void): Promise<MeshStateEntry | undefined> {
    check?.();
    const obligation = this.#delivered.get(id);
    if (!obligation) return undefined;
    const { entry, subscription } = obligation;
    // Re-persist a visible cursor whose put threw, never invoke deliver. An
    // absent once entry after a failed delete has no successful receipt: stay
    // fail-closed rather than infer durability from a no-op delete.
    const current = this.mesh.get(entry.key, { fresh: true });
    if (subscription.once) {
      if (!current || current.version !== entry.version) throw new Error("Lifecycle once deletion receipt is unconfirmed");
      const deleted = this.options.canConsumeMesh || check
        ? (await this.mesh.writeBatch({ identity: this.identity, lockTiming: { operation: "lifecycle-once-retire" },
            ops: [{ kind: "delete", key: entry.key,
            ifVersion: current.version, condition: () => { check?.(); return this.options.canConsumeMesh?.() !== false; } }] }))[0]?.applied
        : (await this.mesh.delete({ key: entry.key, ifVersion: current.version })).deleted;
      if (!deleted) throw new Error("Lifecycle once deletion is unconfirmed");
      check?.();
      this.#onceReceipts.set(id, { key: entry.key, version: current.version,
        ...(subscription.lastEventId ? { eventId: subscription.lastEventId } : {}), afterSequence: subscription.afterSequence });
      this.#delivered.delete(id); this.#unsaved.delete(id);
      return undefined;
    }
    const observed = lifecycleSubscriptionFromValue(current?.value);
    if (!current || !observed || (current.version !== entry.version &&
        JSON.stringify(observed) !== JSON.stringify(subscription))) {
      throw new Error("Lifecycle delivered cursor receipt ownership is unconfirmed");
    }
    const next = await this.#replace(current, subscription, check);
    check?.();
    this.#delivered.delete(id); this.#unsaved.delete(id);
    return next;
  }

  async #replace(
    entry: MeshStateEntry,
    subscription: FabricLifecycleSubscription,
    check?: () => void,
  ): Promise<MeshStateEntry> {
    check?.();
    if (!this.options.canConsumeMesh && !check) return this.mesh.put({
      key: entry.key, value: subscription, identity: this.identity, ifVersion: entry.version,
    });
    let committedAt = 0;
    const [result] = await this.mesh.writeBatch({ identity: this.identity, lockTiming: { operation: "lifecycle-cursor" }, ops: [{
      kind: "put", key: entry.key, ifVersion: entry.version,
      // The lock wait may outlast renewal. Fence the actual commit, not just polling.
      value: (now: number) => {
        check?.();
        if (this.options.canConsumeMesh?.() === false) throw new Error("Resident host lease unavailable for lifecycle cursor commit");
        committedAt = now;
        return subscription;
      },
    }] });
    if (!result?.applied) throw new Error("Lifecycle cursor commit is unconfirmed");
    return { key: entry.key, value: subscription, version: result.version,
      updatedAt: committedAt, updatedBy: this.identity };
  }
}
