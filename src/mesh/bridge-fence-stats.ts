import { performance } from "node:perf_hooks";

/** Bridge callbacks measured while state writer custody is held, including mirror batch effects. */
export type BridgeFenceSite = "subscribeChanges.capture" | "heldFence" | "mirror.prepare" | "mirror.afterCommit";

/** State-fence callback through rollback, or only the named synchronous writeBatch callback. */
export type BridgeFenceScope = "state-write-fence" | "writeBatch.prepare" | "writeBatch.afterCommit";

/** One bounded, opt-in raw observation of a bridge fence/scan callback. */
export interface BridgeFenceObservation {
  v: 1;
  peer: string;
  site: BridgeFenceSite;
  scope: BridgeFenceScope;
  /** Monotonic milliseconds: state-write-fence includes rollback, not acquisition; mirror.prepare
   * and mirror.afterCommit measure only their complete callbacks, not the enclosing transaction. */
  holdMs: number;
  /** The attempt number within the current bridge retry loop. */
  attempt: number;
  /** True for a revision/stamp proof rather than a full scan; false (not applicable) for afterCommit. */
  optimized: boolean;
  /** True for a full-scan fallback; false (not applicable) for afterCommit. */
  fallback: boolean;
  /** True when this was a retry after an earlier attempt in the same loop. */
  retry: boolean;
}

export type BridgeFenceObservationCallback = (observation: BridgeFenceObservation) => void;

/** Maximum number of raw observations retained by a side when no callback drains them. */
export const BRIDGE_FENCE_RAW_BUFFER_MAX = 4_096;

/** A bounded, oldest-first handoff for local reproduction probes. */
export class BridgeFenceObservationBuffer {
  #observations: BridgeFenceObservation[] = [];
  #next = 0;
  #dropped = 0;

  push(observation: BridgeFenceObservation): void {
    if (this.#observations.length < BRIDGE_FENCE_RAW_BUFFER_MAX) this.#observations.push(observation);
    else {
      this.#observations[this.#next] = observation;
      this.#next = (this.#next + 1) % BRIDGE_FENCE_RAW_BUFFER_MAX;
      this.#dropped += 1;
    }
  }

  drain(): BridgeFenceObservation[] {
    const observations = this.#next === 0 ? this.#observations
      : [...this.#observations.slice(this.#next), ...this.#observations.slice(0, this.#next)];
    this.#observations = [];
    this.#next = 0;
    return observations;
  }

  get dropped(): number {
    return this.#dropped;
  }
}

export interface BridgeFenceStatsOptions {
  log?: (line: string) => void;
  /** Delivers each raw sample; observer failures are deliberately ignored. */
  observe?: BridgeFenceObservationCallback;
  clock?: () => number;
  windowMs?: number;
}

type FenceBucket = {
  count: number;
  fallbackCount: number;
  optimizedCount: number;
  retryCount: number;
  holdMaxMs: number;
  holds: number[];
  seen: number;
};

// A bounded rolling sample keeps percentile reporting bounded even on a busy bridge. Until the cap
// is reached, percentiles are exact. There is intentionally no per-sample log or timer.
const HOLD_SAMPLE_MAX = 512;
const percentile = (holds: readonly number[], fraction: number): number | undefined => {
  if (holds.length === 0) return undefined;
  const sorted = [...holds].sort((left, right) => left - right);
  const position = Math.min(sorted.length - 1, Math.max(0, Math.ceil(sorted.length * fraction) - 1));
  return sorted[position];
};

const bucketKey = (peer: string, site: BridgeFenceSite): string => `${peer}\u0000${site}`;

/**
 * Windowed bridge fence telemetry. It is intentionally passive: it only records and reports
 * observations and never participates in admission, retry, or append decisions.
 */
export class BridgeFenceStats {
  readonly #clock: () => number;
  readonly #windowMs: number;
  readonly #buckets = new Map<string, { peer: string; site: BridgeFenceSite; bucket: FenceBucket }>();
  #bucketStart: number | undefined;
  #closed = false;

  constructor(readonly options: BridgeFenceStatsOptions = {}) {
    this.#clock = options.clock ?? Date.now;
    this.#windowMs = options.windowMs ?? 60_000;
    if (!(Number.isSafeInteger(this.#windowMs) && this.#windowMs > 0)) throw new Error("write-fence window must be a positive integer");
  }

  record(observation: BridgeFenceObservation): void {
    if (this.#closed) return;
    try {
      this.options.observe?.(observation);
    } catch {
      // Raw reproduction hooks are advisory and must never alter bridge behavior.
    }
    const now = this.#clock();
    const bucketStart = Math.floor(now / this.#windowMs) * this.#windowMs;
    // Rotation is deliberately observed by flushIfDue(), outside measured callbacks. In particular,
    // record() must never synchronously log from mirror.prepare/afterCommit under a write transaction.
    if (this.#bucketStart === undefined) this.#bucketStart = bucketStart;
    const key = bucketKey(observation.peer, observation.site);
    let entry = this.#buckets.get(key);
    if (!entry) {
      entry = { peer: observation.peer, site: observation.site, bucket: {
        count: 0, fallbackCount: 0, optimizedCount: 0, retryCount: 0, holdMaxMs: 0, holds: [], seen: 0,
      } };
      this.#buckets.set(key, entry);
    }
    const holdMs = Math.max(0, Number.isFinite(observation.holdMs) ? observation.holdMs : 0);
    const bucket = entry.bucket;
    bucket.count += 1;
    bucket.fallbackCount += observation.fallback ? 1 : 0;
    bucket.optimizedCount += observation.optimized ? 1 : 0;
    bucket.retryCount += observation.retry ? 1 : 0;
    bucket.holdMaxMs = Math.max(bucket.holdMaxMs, holdMs);
    if (bucket.holds.length < HOLD_SAMPLE_MAX) bucket.holds.push(holdMs);
    else bucket.holds[bucket.seen % HOLD_SAMPLE_MAX] = holdMs;
    bucket.seen += 1;
  }

  /** Emit due windows without arming a timer. Existing bridge monitor lifecycle calls this. */
  flushIfDue(now = this.#clock()): void {
    if (this.#bucketStart !== undefined && now >= this.#bucketStart + this.#windowMs) this.flush();
  }

  /** Emit the current nonempty window, one line per peer/site. */
  flush(): void {
    if (this.#buckets.size === 0) {
      this.#bucketStart = undefined;
      return;
    }
    for (const { peer, site, bucket } of this.#buckets.values()) {
      const line = `bridge write-fence v1 peer=${peer} site=${site} count=${bucket.count} ` +
        `holdP50Ms=${percentile(bucket.holds, 0.5) ?? "none"} ` +
        `holdP99Ms=${percentile(bucket.holds, 0.99) ?? "none"} holdMaxMs=${bucket.holdMaxMs} ` +
        `fallbackCount=${bucket.fallbackCount} optimizedCount=${bucket.optimizedCount} retryCount=${bucket.retryCount}`;
      try { this.options.log?.(line); } catch { /* telemetry must not affect the bridge */ }
    }
    this.#buckets.clear();
    this.#bucketStart = undefined;
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.flush();
  }
}

interface TimedFenceContext {
  peer: string;
  site: BridgeFenceSite;
  scope: BridgeFenceScope;
  attempt: number;
  optimized: boolean;
  fallback: boolean;
  retry: boolean;
}

const observationOf = (context: TimedFenceContext, startedAt: number): BridgeFenceObservation => ({
  v: 1, ...context, holdMs: Math.max(0, performance.now() - startedAt),
});

/**
 * Measure the actual synchronous state-fence callback. The timer starts immediately before the
 * operation and ends after withWriteFence returns, including its rollback but excluding acquisition
 * and other fence preamble. A busy failure before the callback starts is not reported as a hold.
 */
export const measureBridgeFence = <T>(
  fence: (operation: () => T) => T,
  operation: () => T,
  context: TimedFenceContext,
  observe: (observation: BridgeFenceObservation) => void,
): T => {
  let startedAt: number | undefined;
  let entered = false;
  try {
    return fence(() => {
      entered = true;
      startedAt = performance.now();
      return operation();
    });
  } finally {
    if (entered && startedAt !== undefined) {
      try { observe(observationOf(context, startedAt)); } catch { /* observation is advisory */ }
    }
  }
};

/** Measure a complete synchronous writeBatch.prepare/afterCommit callback, not its transaction. */
export const measureBridgeScan = <T>(
  operation: () => T,
  context: TimedFenceContext,
  observe: (observation: BridgeFenceObservation) => void,
): T => {
  const startedAt = performance.now();
  try {
    return operation();
  } finally {
    try { observe(observationOf(context, startedAt)); } catch { /* observation is advisory */ }
  }
};
