import { performance } from "node:perf_hooks";
import type { MeshEvent } from "./store.js";

export type BridgeDeliveryDirection = "toRemote" | "toLocal";
export type BridgeDrainPhase = "readMs" | "resolveMs" | "membershipWaitMs" | "filterMs" | "publishMs" | "checkpointMs" | "idleMs";
const drainTotals = () => ({ pages: 0, events: 0, readMs: 0, resolveMs: 0, membershipWaitMs: 0,
  membershipWaitCount: 0, membershipWaitMaxMs: 0, filterMs: 0, publishMs: 0, checkpointMs: 0, idleMs: 0 });
export type BridgeDeliveryAgeReason = "delivery-age" | "ack-age" | "command-deadline";
export interface BridgeDeliveryAgeAlarm {
  v: 1;
  state: "outage" | "recovered";
  host: string;
  peer: string;
  direction: BridgeDeliveryDirection;
  episodeId: string;
  since: string;
  at: string;
  /** Null until the destination receipt supplies its append timestamp. */
  maxDeliveryAgeMs: number | null;
  maxAckLatencyMs: number;
  reason: BridgeDeliveryAgeReason;
  durationMs?: number;
}

export const BRIDGE_DELIVERY_AGE_ALERT_MS = 30_000;
export const BRIDGE_DELIVERY_AGE_WINDOW_MS = 60_000;

type DeliveryAgeEvent = Pick<MeshEvent, "topic" | "data" | "createdAt">;
type AlarmSink = (alarm: BridgeDeliveryAgeAlarm) => void | Promise<void>;
type Pending = {
  id: number;
  direction: BridgeDeliveryDirection;
  sourceCreatedAt: number;
  cursorLagSequences: number;
  cursorLagSeconds: number;
  deadlineAt?: number;
  startedAt: number;
  alarmed: boolean;
  timer: ReturnType<typeof setTimeout>;
};
type Episode = {
  id: string;
  since: number;
  maxDeliveryAgeMs: number | null;
  maxAckLatencyMs: number;
  reason: BridgeDeliveryAgeReason;
  latePending: Set<number>;
};

export interface BridgeDeliveryAgeOptions {
  host: string;
  peer: string;
  log?: (line: string) => void;
  alarm?: AlarmSink;
  clock?: () => number;
  monotonic?: () => number;
  thresholdMs?: number;
  windowMs?: number;
}

export interface DeliveryAgeReceipt {
  complete(destinationCreatedAt?: number): void;
  cancel(): void;
}

/** Interpolated p50, kept pure so the telemetry contract is directly testable. */
export const percentile50 = (samples: readonly number[]): number | undefined => {
  if (samples.length === 0) return undefined;
  const sorted = [...samples].sort((left, right) => left - right);
  const position = (sorted.length - 1) * 0.5;
  const lower = Math.floor(position), upper = Math.ceil(position);
  return Math.round(sorted[lower]! + (sorted[upper]! - sorted[lower]!) * (position - lower));
};

/**
 * Tracks real command/ACK crossings and the time until the source receives the destination's
 * durable publish receipt. Until that receipt arrives, only its latency is known. Alerts are
 * observational: this class never changes the bridge cursor, transport, or systemd watchdog state.
 */
export class BridgeDeliveryAgeMonitor {
  readonly #clock: () => number;
  readonly #monotonic: () => number;
  readonly #thresholdMs: number;
  readonly #windowMs: number;
  readonly #pending = new Map<number, Pending>();
  readonly #episodes = new Map<BridgeDeliveryDirection, Episode>();
  readonly #samples: Record<BridgeDeliveryDirection, Array<{ deliveryMs: number; ackMs: number; lagSequences: number; lagSeconds: number }>> = {
    toRemote: [], toLocal: [],
  };
  readonly #drain = new Map<BridgeDeliveryDirection, {
    phase: BridgeDrainPhase; since: number; totals: ReturnType<typeof drainTotals>; membershipSince?: number;
  }>();
  #nextPendingId = 0;
  #bucketStart: number | undefined;
  #flushTimer: ReturnType<typeof setTimeout> | undefined;
  #closed = false;

  constructor(readonly options: BridgeDeliveryAgeOptions) {
    this.#clock = options.clock ?? Date.now;
    this.#monotonic = options.monotonic ?? (() => performance.now());
    this.#thresholdMs = options.thresholdMs ?? BRIDGE_DELIVERY_AGE_ALERT_MS;
    this.#windowMs = options.windowMs ?? BRIDGE_DELIVERY_AGE_WINDOW_MS;
    if (!(Number.isSafeInteger(this.#thresholdMs) && this.#thresholdMs > 0)) throw new Error("delivery-age threshold must be a positive integer");
    if (!(Number.isSafeInteger(this.#windowMs) && this.#windowMs > 0)) throw new Error("delivery-age window must be a positive integer");
  }

  /** Exclusive phases; membership count records starts, max includes in-flight waits across windows. */
  drainPhase(direction: BridgeDeliveryDirection, phase: BridgeDrainPhase): () => void {
    this.#openWindow();
    const now = this.#monotonic();
    let state = this.#drain.get(direction);
    if (!state) {
      state = { phase: "idleMs", since: now, totals: drainTotals() };
      this.#drain.set(direction, state);
    }
    const previous = state.phase;
    this.#accrueDrain(direction);
    state.phase = phase;
    if (phase === "membershipWaitMs") {
      state.totals.membershipWaitCount++;
      state.membershipSince = now;
    }
    return () => {
      this.#accrueDrain(direction);
      state.phase = previous;
      if (phase === "membershipWaitMs") delete state.membershipSince;
    };
  }

  async drainWait<T>(direction: BridgeDeliveryDirection, phase: BridgeDrainPhase, action: () => Promise<T>): Promise<T> {
    const end = this.drainPhase(direction, phase);
    try { return await action(); } finally { end(); }
  }

  drainPage(direction: BridgeDeliveryDirection, events: number): void {
    const totals = this.#drain.get(direction)!.totals;
    totals.pages++;
    totals.events += events;
  }

  #accrueDrain(direction: BridgeDeliveryDirection): void {
    const state = this.#drain.get(direction)!;
    const now = this.#monotonic();
    state.totals[state.phase] += Math.max(0, now - state.since);
    if (state.membershipSince !== undefined) state.totals.membershipWaitMaxMs =
      Math.max(state.totals.membershipWaitMaxMs, now - state.membershipSince);
    state.since = now;
  }

  /** Start before the destination append; the returned receipt must be completed or canceled. */
  begin(direction: BridgeDeliveryDirection, event: DeliveryAgeEvent, cursorLagSequences = 0): DeliveryAgeReceipt | undefined {
    if (this.#closed || (event.topic !== "fabric.control.command" && event.topic !== "fabric.control.ack") ||
        !Number.isFinite(event.createdAt) || event.createdAt < 0) return undefined;
    const data = event.data && typeof event.data === "object" && !Array.isArray(event.data)
      ? event.data as Record<string, unknown> : undefined;
    const deadlineAt = event.topic === "fabric.control.command" && typeof data?.deadlineAt === "number" &&
      Number.isFinite(data.deadlineAt) ? data.deadlineAt : undefined;
    const id = ++this.#nextPendingId;
    const now = this.#clock();
    if (!Number.isSafeInteger(cursorLagSequences) || cursorLagSequences < 0) cursorLagSequences = 0;
    const pending = {
      id, direction, sourceCreatedAt: event.createdAt, cursorLagSequences,
      cursorLagSeconds: Math.max(0, Math.floor((now - event.createdAt) / 1000)),
      ...(deadlineAt !== undefined ? { deadlineAt } : {}),
      startedAt: this.#monotonic(), alarmed: false, timer: undefined as unknown as ReturnType<typeof setTimeout>,
    };
    this.#pending.set(id, pending);
    // A long publish with no receipt is an ACK-age alarm. Delivery age/deadline are checked
    // against the destination append timestamp when the receipt arrives, not guessed here.
    pending.timer = setTimeout(() => this.#alarmPending(id), this.#thresholdMs + 1);
    pending.timer.unref?.();
    let settled = false;
    const settle = (): Pending | undefined => {
      if (settled) return undefined;
      settled = true;
      const current = this.#pending.get(id);
      if (!current) return undefined;
      clearTimeout(current.timer);
      this.#pending.delete(id);
      return current;
    };
    return {
      complete: destinationCreatedAt => {
        const current = this.#pending.get(id);
        if (!current || settled) return;
        clearTimeout(current.timer);
        const at = Number.isFinite(destinationCreatedAt) && (destinationCreatedAt ?? 0) >= 0
          ? destinationCreatedAt! : this.#clock();
        const deliveryMs = Math.max(0, Math.round(at - current.sourceCreatedAt));
        const ackMs = Math.max(0, Math.round(this.#monotonic() - current.startedAt));
        const lateDeadline = current.deadlineAt !== undefined && at > current.deadlineAt;
        const deliverySlow = deliveryMs > this.#thresholdMs;
        const ackSlow = ackMs > this.#thresholdMs;
        const tooSlow = deliverySlow || ackSlow;
        if (tooSlow || lateDeadline || current.alarmed) {
          const reason = lateDeadline ? "command-deadline" : deliverySlow ? "delivery-age" : "ack-age";
          this.#raise(current.direction, deliveryMs, ackMs, reason, current);
        }
        this.#record(current.direction, deliveryMs, ackMs, current);
        const completed = settle();
        if (completed) this.#episodes.get(completed.direction)?.latePending.delete(id);
        if (completed && !tooSlow && !lateDeadline && !completed.alarmed) this.#recoverIfClear(completed.direction);
      },
      cancel: () => {
        const canceled = settle();
        if (canceled?.alarmed) this.#episodes.get(canceled.direction)?.latePending.delete(id);
      },
    };
  }

  /** Track one destination append without making telemetry part of the publish result path. */
  async track<T extends { sequence: number; createdAt?: number }>(direction: BridgeDeliveryDirection, event: DeliveryAgeEvent,
    publish: () => Promise<T>, cursorLagSequences = 0): Promise<T> {
    const receipt = this.begin(direction, event, cursorLagSequences);
    try {
      const result = await publish();
      receipt?.complete(result.createdAt);
      return result;
    } catch (error) {
      receipt?.cancel();
      throw error;
    }
  }

  /** Track a committed batch prefix; uncommitted suffixes remain unread and are canceled. */
  async trackBatch<T extends { sequence: number; createdAt?: number }>(direction: BridgeDeliveryDirection, events: readonly DeliveryAgeEvent[],
    publish: () => Promise<readonly T[]>, cursorLagSequences = 0): Promise<readonly T[]> {
    const receipts = events.map(event => this.begin(direction, event, cursorLagSequences));
    try {
      const results = await publish();
      for (let index = 0; index < Math.min(results.length, receipts.length); index++) {
        receipts[index]?.complete(results[index]?.createdAt);
      }
      return results;
    } finally {
      for (const receipt of receipts) receipt?.cancel();
    }
  }

  /** Flush one final nonempty minute on close; the monitor has no recurring idle timer. */
  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    if (this.#flushTimer !== undefined) clearTimeout(this.#flushTimer);
    this.#flushTimer = undefined;
    this.#flush();
    for (const pending of this.#pending.values()) clearTimeout(pending.timer);
    this.#pending.clear();
  }

  #alarmPending(id: number): void {
    const pending = this.#pending.get(id);
    if (!pending || this.#closed) return;
    const ackMs = Math.max(0, Math.round(this.#monotonic() - pending.startedAt));
    pending.alarmed = true;
    this.#raise(pending.direction, undefined, ackMs, "ack-age", pending);
  }

  #raise(direction: BridgeDeliveryDirection, deliveryMs: number | undefined, ackMs: number,
    reason: BridgeDeliveryAgeReason, pending: Pending): void {
    const atMs = this.#clock();
    let episode = this.#episodes.get(direction);
    if (!episode) {
      episode = { id: process.hrtime.bigint().toString(), since: atMs, maxDeliveryAgeMs: deliveryMs ?? null,
        maxAckLatencyMs: ackMs, reason, latePending: new Set() };
      this.#episodes.set(direction, episode);
      this.#emit({ v: 1, state: "outage", host: this.options.host, peer: this.options.peer, direction,
        episodeId: episode.id, since: new Date(atMs).toISOString(), at: new Date(atMs).toISOString(),
        maxDeliveryAgeMs: deliveryMs ?? null, maxAckLatencyMs: ackMs, reason });
    }
    if (deliveryMs !== undefined) episode.maxDeliveryAgeMs = Math.max(episode.maxDeliveryAgeMs ?? 0, deliveryMs);
    episode.maxAckLatencyMs = Math.max(episode.maxAckLatencyMs, ackMs);
    if (reason === "command-deadline") episode.reason = reason;
    if (pending.alarmed || (deliveryMs !== undefined && deliveryMs > this.#thresholdMs) || ackMs > this.#thresholdMs || reason === "command-deadline") {
      episode.latePending.add(pending.id);
    }
  }

  #recoverIfClear(direction: BridgeDeliveryDirection): void {
    const episode = this.#episodes.get(direction);
    if (!episode || episode.latePending.size > 0) return;
    const atMs = this.#clock();
    this.#episodes.delete(direction);
    this.#emit({ v: 1, state: "recovered", host: this.options.host, peer: this.options.peer, direction,
      episodeId: episode.id, since: new Date(episode.since).toISOString(), at: new Date(atMs).toISOString(),
      maxDeliveryAgeMs: episode.maxDeliveryAgeMs, maxAckLatencyMs: episode.maxAckLatencyMs,
      reason: episode.reason, durationMs: Math.max(0, atMs - episode.since) });
  }

  #record(direction: BridgeDeliveryDirection, deliveryMs: number, ackMs: number, pending: Pending): void {
    this.#openWindow();
    this.#samples[direction].push({ deliveryMs, ackMs, lagSequences: pending.cursorLagSequences, lagSeconds: pending.cursorLagSeconds });
  }

  #openWindow(): void {
    if (this.#closed) return;
    const now = this.#clock();
    const bucket = Math.floor(now / this.#windowMs) * this.#windowMs;
    if (this.#bucketStart !== undefined && this.#bucketStart !== bucket) this.#flush();
    if (this.#bucketStart === undefined) {
      this.#bucketStart = bucket;
      for (const state of this.#drain.values()) if (state.phase === "idleMs") state.since = this.#monotonic();
    }
    if (this.#flushTimer === undefined) {
      this.#flushTimer = setTimeout(() => {
        this.#flushTimer = undefined;
        this.#flush();
        if ([...this.#drain.values()].some(state => state.phase !== "idleMs")) this.#openWindow();
      }, Math.max(0, this.#bucketStart + this.#windowMs - now));
      this.#flushTimer.unref?.();
    }
  }

  #flush(): void {
    if (this.#bucketStart === undefined) return;
    const bucketStart = this.#bucketStart;
    const describe = (direction: BridgeDeliveryDirection): string => {
      const samples = this.#samples[direction];
      const delivery = samples.map(sample => sample.deliveryMs);
      const acknowledgements = samples.map(sample => sample.ackMs);
      const maxDelivery = delivery.reduce((value, sample) => Math.max(value, sample), Number.NEGATIVE_INFINITY);
      const maxAck = acknowledgements.reduce((value, sample) => Math.max(value, sample), Number.NEGATIVE_INFINITY);
      const maxLagSequences = samples.reduce((value, sample) => Math.max(value, sample.lagSequences), 0);
      const maxLagSeconds = samples.reduce((value, sample) => Math.max(value, sample.lagSeconds), 0);
      return `${direction} count=${samples.length} deliveryP50Ms=${percentile50(delivery) ?? "none"} ` +
        `deliveryMaxMs=${samples.length ? maxDelivery : "none"} ackP50Ms=${percentile50(acknowledgements) ?? "none"} ` +
        `ackMaxMs=${samples.length ? maxAck : "none"} cursorLagSequences=${maxLagSequences} cursorLagSeconds=${maxLagSeconds}`;
    };
    if (this.#samples.toRemote.length || this.#samples.toLocal.length) {
      this.options.log?.(`bridge delivery-age v1 peer=${this.options.peer} minute=${new Date(bucketStart).toISOString()} ` +
        `${describe("toRemote")} ${describe("toLocal")}`);
    }
    for (const [direction, state] of this.#drain) {
      this.#accrueDrain(direction);
      try { this.options.log?.(`bridge drain-time v1 peer=${this.options.peer} dir=${direction} minute=${new Date(bucketStart).toISOString()} ` +
        Object.entries(state.totals).map(([key, value]) => `${key}=${Math.round(value)}`).join(" ")); } catch { /* Diagnostics only. */ }
      state.totals = drainTotals();
    }
    this.#samples.toRemote.length = 0;
    this.#samples.toLocal.length = 0;
    this.#bucketStart = undefined;
    if (this.#flushTimer !== undefined) clearTimeout(this.#flushTimer);
    this.#flushTimer = undefined;
  }

  #emit(alarm: BridgeDeliveryAgeAlarm): void {
    if (!this.options.alarm) {
      this.options.log?.(`bridge delivery-age ${alarm.state} peer=${alarm.peer} direction=${alarm.direction} ` +
        `deliveryMs=${alarm.maxDeliveryAgeMs ?? "unknown"} ackMs=${alarm.maxAckLatencyMs} reason=${alarm.reason}`);
      return;
    }
    try {
      void Promise.resolve(this.options.alarm(alarm)).catch(() => {
        this.options.log?.(`bridge delivery-age ${alarm.state} alarm publish failed peer=${alarm.peer} direction=${alarm.direction}`);
      });
    } catch {
      this.options.log?.(`bridge delivery-age ${alarm.state} alarm publish failed peer=${this.options.peer} direction=${alarm.direction}`);
    }
  }
}
