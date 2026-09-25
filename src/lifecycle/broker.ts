import { randomUUID } from "node:crypto";
import { MeshStore, type MeshIdentity, type MeshStateEntry } from "../mesh/store.js";
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
}

export type FabricLifecycleDeliveryHandler = (
  subscription: FabricLifecycleSubscription,
  event: FabricLifecycleEvent,
) => Promise<void> | void;

const subscriptionKey = (id: string): string =>
  FABRIC_LIFECYCLE_SUBSCRIPTION_PREFIX + id;

export class LifecycleBroker {
  readonly #pollMs: number;
  readonly #maxReadEvents: number;
  #timer: NodeJS.Timeout | undefined;
  #polling: Promise<void> | undefined;
  #publishTail: Promise<void> = Promise.resolve();
  #pollScheduled = false;
  #closed = false;
  /** Cursors past events that matched nothing, not yet saved, by subscription id. */
  readonly #unsaved = new Map<string, number>();

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

  start(): void {
    if (!this.options.enabled || this.#timer) return;
    this.#closed = false;
    this.#timer = setInterval(() => this.#schedulePoll(), this.#pollMs);
    this.#timer.unref();
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
      this.#schedulePoll();
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

    await this.participants.refresh();
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
    await this.mesh.put({
      key: subscriptionKey(subscription.id),
      value: subscription,
      identity: this.identity,
      ifVersion: 0,
    });
    this.#schedulePoll();
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

  async unsubscribe(id: string): Promise<{ removed: boolean }> {
    const key = subscriptionKey(id.trim());
    const entry = this.mesh.get(key, { fresh: true });
    if (!entry || !lifecycleSubscriptionFromValue(entry.value)) return { removed: false };
    const result = await this.mesh.delete({ key, ifVersion: entry.version });
    return { removed: result.deleted };
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    if (this.#timer) clearInterval(this.#timer);
    this.#timer = undefined;
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
      void this.#poll().catch(() => undefined);
    });
  }

  async #poll(): Promise<void> {
    if (this.#closed || !this.options.enabled) return;
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
    for (const entry of entries) {
      const subscription = lifecycleSubscriptionFromValue(entry.value);
      if (!subscription || entry.key !== subscriptionKey(subscription.id)) continue;
      listed.add(subscription.id);
      // Only the target's host drains a subscription. Pass over other hosts' targets (from
      // memory) and caught-up subscriptions before the directory read: that read parses every
      // participant and host record, and ran for every subscription on every poll of every
      // host, about a quarter of a core per idle Pi on the fleet mesh (smarty-dev#557).
      if (this.participants.publishes?.(subscription.to) === false) continue;
      latestSequence ??= this.mesh.latestSequence();
      if (latestSequence <= this.#cursor(subscription)) continue;
      const target = this.participants.get(subscription.to);
      if (!target || target.stale || !target.local) continue;
      await this.#drainSubscription(entry, subscription);
    }
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
    if (subscription.lastError !== undefined || cursor - subscription.afterSequence >= this.#maxReadEvents) return false;
    this.#unsaved.set(subscription.id, cursor);
    return true;
  }

  async #drainSubscription(
    initialEntry: MeshStateEntry,
    initial: FabricLifecycleSubscription,
  ): Promise<void> {
    let entry = initialEntry;
    let subscription = initial;
    while (!this.#closed) {
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
        }).then(() => this.#unsaved.delete(subscription.id), () => undefined);
        return;
      }

      let cursor = from;
      let decided = false;
      let lastDeliveredAt = subscription.lastDeliveredAt;
      let lastEventId = subscription.lastEventId;
      for (const meshEvent of events) {
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
          const failed: FabricLifecycleSubscription = {
            ...subscription,
            afterSequence: cursor,
            updatedAt: Date.now(),
            lastError: error instanceof Error ? error.message : String(error),
          };
          await this.#replace(entry, failed).then(() => this.#unsaved.delete(subscription.id), () => undefined);
          return;
        }
        cursor = lifecycle.sequence;
        decided = true;
        lastDeliveredAt = Date.now();
        lastEventId = lifecycle.id;
        if (subscription.once) {
          await this.mesh
            .delete({ key: entry.key, ifVersion: entry.version })
            .catch(() => ({ deleted: false }));
          return;
        }
      }

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
      const next = await this.#replace(entry, updated).catch(() => undefined);
      if (!next) return;
      this.#unsaved.delete(subscription.id);
      entry = next;
      subscription = updated;
      if (events.length < this.#maxReadEvents) return;
    }
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

  async #replace(
    entry: MeshStateEntry,
    subscription: FabricLifecycleSubscription,
  ): Promise<MeshStateEntry> {
    return this.mesh.put({
      key: entry.key,
      value: subscription,
      identity: this.identity,
      ifVersion: entry.version,
    });
  }
}
