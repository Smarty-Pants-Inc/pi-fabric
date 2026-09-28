import type { PublicationRelay } from "./relay.js";
import type { ConsumerLag, RecordsOps } from "./store.js";

export type { ConsumerLag } from "./store.js";

/**
 * The idle watchdog (C4, revision 7's): a nudge that was published but missed (the receiver was
 * idle, offline or backpressured) is caught by comparing each consumer's processing cursor with
 * the committed records addressed to it. A consumer that lags past `lagMs` is woken when it is
 * this process's own root, and otherwise gets an alarm, once per window across all processes.
 * Each tick also republishes unpublished nudges (a crash between commit and publish) and runs
 * the optional `check` (the records service refreshes C2's archive frontier there).
 */
export class RecordsWatchdog {
  #timer: ReturnType<typeof setInterval> | undefined;
  #ticking: Promise<unknown> | undefined;

  constructor(readonly options: {
    store: RecordsOps;
    relay?: PublicationRelay;
    /** Extra work each tick, before the lag scan (the service's archive check). */
    check?: (signal: AbortSignal | undefined) => Promise<void>;
    /** This process's own consumer id, if it has a root inbox. */
    self?: string;
    /** Deliver this root's pending records now (a turn it was not going to take). */
    wake?: () => Promise<void>;
    alarm?: (lag: ConsumerLag) => Promise<void>;
    lagMs?: number;
    intervalMs?: number;
    /** Minimum time between two alarms for one consumer. */
    realarmMs?: number;
    now?: () => number;
    /** The owner's lifetime: aborting it stops a running tick's work. */
    signal?: AbortSignal;
  }) {}

  start(): void {
    if (this.#timer) return;
    this.#timer = setInterval(() => { void this.tick().catch(() => undefined); }, this.options.intervalMs ?? 60_000);
    this.#timer.unref?.();
  }

  stop(): void {
    if (this.#timer) clearInterval(this.#timer);
    this.#timer = undefined;
  }

  /** Wait for a running tick to end (it ends promptly once the lifetime signal aborts). */
  async idle(): Promise<void> {
    await this.#ticking?.catch(() => undefined);
  }

  async tick(): Promise<{ lagging: ConsumerLag[]; woke: boolean; alarmed: string[] }> {
    if (this.#ticking) await this.#ticking.catch(() => undefined);
    const run = this.#tick();
    this.#ticking = run;
    try { return await run; } finally { if (this.#ticking === run) this.#ticking = undefined; }
  }

  async #tick(): Promise<{ lagging: ConsumerLag[]; woke: boolean; alarmed: string[] }> {
    const { store, relay, signal } = this.options;
    signal?.throwIfAborted();
    await relay?.flush(signal).catch(() => undefined);
    await this.options.check?.(signal);
    signal?.throwIfAborted();
    const now = this.options.now?.() ?? Date.now();
    const lagging = await store.lagging(this.options.lagMs ?? 120_000, now, signal);
    let woke = false;
    const alarmed: string[] = [];
    for (const lag of lagging) {
      if (lag.consumer === this.options.self && this.options.wake) {
        await this.options.wake();
        woke = true;
        continue;
      }
      if (!this.options.alarm) continue;
      if (!await store.claimAlarm(`consumer-lag:${lag.consumer}`, now, this.options.realarmMs ?? 10 * 60_000, signal)) continue;
      await this.options.alarm(lag).catch(() => undefined);
      alarmed.push(lag.consumer);
    }
    return { lagging, woke, alarmed };
  }

  /** Consumers with an addressed record past their cursor older than the lag bound. */
  lagging(): Promise<ConsumerLag[]> {
    return this.options.store.lagging(this.options.lagMs ?? 120_000, this.options.now?.() ?? Date.now(), this.options.signal);
  }
}
