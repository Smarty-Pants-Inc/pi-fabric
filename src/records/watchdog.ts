import type { AdmissionGate } from "./admission.js";
import type { PublicationRelay } from "./relay.js";
import type { RecordStore } from "./store.js";

export interface ConsumerLag { consumer: string; after: number; oldestAt: number; count: number }

/**
 * The idle watchdog (C4, revision 7's): a nudge that was published but missed (the receiver was
 * idle, offline or backpressured) is caught by comparing each consumer's processing cursor with
 * the committed records addressed to it. A consumer that lags past `lagMs` is woken when it is
 * this process's own root, and otherwise gets an alarm. Each tick also republishes unpublished
 * nudges (a crash between commit and publish) and refreshes C2's archive frontier.
 */
export class RecordsWatchdog {
  #timer: ReturnType<typeof setInterval> | undefined;
  #ticking: Promise<unknown> | undefined;

  constructor(readonly options: {
    store: RecordStore;
    relay?: PublicationRelay;
    gate?: AdmissionGate;
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

  async tick(): Promise<{ lagging: ConsumerLag[]; woke: boolean; alarmed: string[] }> {
    if (this.#ticking) await this.#ticking.catch(() => undefined);
    const run = this.#tick();
    this.#ticking = run;
    try { return await run; } finally { if (this.#ticking === run) this.#ticking = undefined; }
  }

  async #tick(): Promise<{ lagging: ConsumerLag[]; woke: boolean; alarmed: string[] }> {
    const { store, relay, gate } = this.options;
    await relay?.flush().catch(() => undefined);
    if (gate?.enabled) {
      await gate.refresh();
      const input = await store.transaction((client) => store.admissionInput(client, gate.frontier()));
      gate.evaluate(input);
    }
    const lagging = await this.lagging();
    let woke = false;
    const alarmed: string[] = [];
    const now = this.options.now?.() ?? Date.now();
    for (const lag of lagging) {
      if (lag.consumer === this.options.self && this.options.wake) {
        await this.options.wake();
        woke = true;
        continue;
      }
      if (!this.options.alarm || !await this.#claimAlarm(lag.consumer, now)) continue;
      await this.options.alarm(lag).catch(() => undefined);
      alarmed.push(lag.consumer);
    }
    return { lagging, woke, alarmed };
  }

  /** One alarm per consumer per re-alarm window across every process on the database. */
  async #claimAlarm(consumer: string, now: number): Promise<boolean> {
    const realarmMs = this.options.realarmMs ?? 10 * 60_000;
    return this.options.store.transaction(async (client) => {
      const { rowCount } = await client.query(
        "UPDATE consumers SET alarmed_at = to_timestamp($2) WHERE consumer = $1 AND (alarmed_at IS NULL OR alarmed_at <= to_timestamp($3))",
        [consumer, now / 1000, (now - realarmMs) / 1000],
      );
      return rowCount === 1;
    });
  }

  /** Consumers with an addressed record past their cursor older than the lag bound. */
  async lagging(): Promise<ConsumerLag[]> {
    const { store } = this.options;
    const lagMs = this.options.lagMs ?? 120_000;
    const now = this.options.now?.() ?? Date.now();
    return store.transaction(async (client) => {
      const { rows } = await client.query<{ consumer: string; after: string; oldest: Date; n: string }>(
        `SELECT c.consumer, c.after, min(r.created_at) AS oldest, count(*) AS n
         FROM consumers c JOIN records r ON r.origin = c.origin AND r.seq > c.after AND r.author <> c.consumer
           AND r.data->>'to' IN (SELECT jsonb_array_elements_text(c.names))
         WHERE c.origin = $1
         GROUP BY c.consumer, c.after
         HAVING min(r.created_at) <= to_timestamp($2)`,
        [store.origin, (now - lagMs) / 1000],
      );
      return rows.map((row) => ({ consumer: row.consumer, after: Number(row.after), oldestAt: row.oldest.getTime(), count: Number(row.n) }));
    });
  }
}
