import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { writeJsonAtomicAsync } from "../core/atomic-write.js";
import { AdmissionGate, WalGFrontierProvider, type AdmissionStatus } from "./admission.js";
import type { FabricRecordsConfig } from "./config.js";
import { RecordsInbox } from "./inbox.js";
import { SharedFrontierProvider } from "./shared-frontier.js";
import { PublicationRelay, type NudgePublisher } from "./relay.js";
import { migrate } from "./schema.js";
import { RecordStore, type ClientPool, type RecordsPrincipal } from "./store.js";
import { RecordsWatchdog, type ConsumerLag } from "./watchdog.js";

export const RECORDS_ALARM_TOPIC = "ops.records";
const REALARM_MS = 10 * 60_000;

export interface RecordsServiceOptions {
  config: FabricRecordsConfig;
  /** The mesh root: the default place of the status file. */
  meshRoot: string;
  /** Publishes nudges and alarms on the mesh as this process's participant. */
  publisher: NudgePublisher;
  /** This process's authenticated participant. */
  identity: { id: string; name?: string };
  /** A root session's names; with `wake`, the service keeps its records inbox. */
  names?: () => readonly string[];
  wake?: () => Promise<void>;
  /** A test or a caller may supply the pool; otherwise `pg` is loaded at first use. */
  pool?: ClientPool;
  now?: () => number;
}

/** The pg pool, loaded at first use so an idle session never imports the driver. */
const openPool = async (config: FabricRecordsConfig): Promise<ClientPool> => {
  const { default: pg } = await import("pg");
  const pool = new pg.Pool({ ...config.connection, max: 2, idleTimeoutMillis: 30_000, application_name: "pi-fabric-records" });
  // An idle client's error (a server restart) must not crash the host; the next query reconnects.
  pool.on("error", () => undefined);
  return pool as unknown as ClientPool;
};

/** The record layer of one Fabric process: store, relay, admission, inbox and watchdog. */
export class RecordsService {
  readonly store: RecordStore;
  readonly relay: PublicationRelay;
  readonly gate: AdmissionGate;
  readonly inbox: RecordsInbox | undefined;
  readonly watchdog: RecordsWatchdog;
  readonly statusFile: string;
  #lastAlarm = 0;
  /** The service's lifetime: close() aborts it, which stops archive checks, relay runs and queries. */
  readonly #life = new AbortController();

  /** The service's lifetime; aborted by close(). */
  get signal(): AbortSignal { return this.#life.signal; }
  #statusWrite: Promise<void> = Promise.resolve();

  private constructor(readonly options: RecordsServiceOptions, pool: ClientPool) {
    const { config } = options;
    const org = config.org!;
    this.statusFile = config.statusFile ?? path.join(options.meshRoot, "records", `${org}.status.json`);
    this.gate = new AdmissionGate({
      providers: config.admission.targets.map((target) => new SharedFrontierProvider(new WalGFrontierProvider(target.name, target.command, {
        ...(target.env ? { env: target.env } : {}), ...(target.timeoutMs ? { timeoutMs: target.timeoutMs } : {}),
        ...(config.admission.segmentSize ? { segmentSize: config.admission.segmentSize } : {}),
      }), () => this.store, config.admission.refreshMs)),
      alarmSeconds: config.admission.alarmSeconds,
      refuseSeconds: config.admission.refuseSeconds,
      ...(options.now ? { now: options.now } : {}),
      onStatus: (status, previous) => this.#onAdmission(status, previous),
    });
    let relay: PublicationRelay | undefined;
    this.store = new RecordStore(pool, {
      org,
      origin: config.origin ?? os.hostname().split(".")[0]!,
      mirror: config.mirror,
      admission: this.gate,
      onCommitted: () => { if (!this.#life.signal.aborted) void relay?.flush(this.#life.signal).catch(() => undefined); },
    });
    this.relay = relay = new PublicationRelay(this.store, options.publisher);
    this.inbox = options.names ? new RecordsInbox(this.store, options.identity.id, options.names, { signal: this.#life.signal }) : undefined;
    this.watchdog = new RecordsWatchdog({
      store: this.store, relay: this.relay, gate: this.gate,
      ...(this.inbox ? { self: options.identity.id } : {}),
      ...(options.wake ? { wake: options.wake } : {}),
      alarm: (lag) => this.#consumerAlarm(lag),
      lagMs: config.consumerLagSeconds * 1000,
      intervalMs: Math.min(config.watchdogMs, config.admission.refreshMs),
      ...(options.now ? { now: options.now } : {}),
      signal: this.#life.signal,
    });
  }

  static async open(options: RecordsServiceOptions): Promise<RecordsService> {
    const { config } = options;
    if (!config.enabled) throw new Error("records are off: set records.enabled, records.org and records.connection in .pi/fabric.json");
    if (!config.org) throw new Error("records.org is required: the org this database belongs to");
    const pool = options.pool ?? await openPool(config);
    try {
      if (config.migrate) {
        const client = await pool.connect();
        try { await migrate(client); } finally { client.release(); }
      }
    } catch (error) {
      await pool.end?.().catch(() => undefined);
      throw error;
    }
    const service = new RecordsService(options, pool);
    // A crash between a commit and its recovery bound leaves one unbounded record: bound it now.
    await service.store.transaction((client) => service.store.fillBounds(client, "all"));
    // Republish what a crash left unpublished, then keep the watchdog running.
    void service.relay.flush(service.#life.signal).catch(() => undefined);
    service.watchdog.start();
    return service;
  }

  /** The caller's principal (C13): its authenticated id; roles come from configuration only. */
  principal(identity: { id: string; name?: string } = this.options.identity): RecordsPrincipal {
    const { config } = this.options;
    return {
      id: identity.id,
      ...(identity.name ? { name: identity.name } : {}),
      importer: config.importers.includes(identity.id),
      mirror: config.mirrors.includes(identity.id),
    };
  }

  /** Records and archive state for status readers (the factory check reads the status file). */
  async status(): Promise<{ org: string; origin: string; frontier: number; unpublished: number; admission: AdmissionStatus | { state: "disabled" }; statusFile: string }> {
    const frontier = await this.store.page({ after: Number.MAX_SAFE_INTEGER - 1, limit: 1, origin: this.store.origin });
    return {
      org: this.store.org, origin: this.store.origin, frontier: frontier.frontier, unpublished: await this.relay.unpublished(),
      admission: this.gate.status() ?? { state: "disabled" }, statusFile: this.statusFile,
    };
  }

  #onAdmission(status: AdmissionStatus, previous: AdmissionStatus | undefined): void {
    const record = { org: this.store.org, origin: this.store.origin, updatedAt: new Date(status.checkedAt).toISOString(), admission: status };
    this.#statusWrite = this.#statusWrite.then(async () => {
      await fs.promises.mkdir(path.dirname(this.statusFile), { recursive: true, mode: 0o700 });
      await writeJsonAtomicAsync(this.statusFile, record, { space: 2, newline: true });
    }).catch(() => undefined);
    const raised = status.state === "alarm" || status.state === "refuse";
    const changed = previous?.state !== status.state;
    const now = status.checkedAt;
    if (!raised || (!changed && now - this.#lastAlarm < REALARM_MS)) return;
    this.#lastAlarm = now;
    const text = status.state === "refuse"
      ? `records archive lagging ${status.lagSeconds} s: records.append refuses new records (C2, over ${status.refuseSeconds} s). Restore WAL archiving to a target (${status.targets.map((target) => target.name).join(", ")}).`
      : `records archive lagging ${status.lagSeconds} s (alarm at ${status.alarmSeconds} s; appends are refused past ${status.refuseSeconds} s). Check WAL archiving (${status.targets.map((target) => target.name).join(", ")}).`;
    void this.options.publisher.publish({
      topic: RECORDS_ALARM_TOPIC, kind: "records.archive-lag", ...(this.options.config.alarmTo ? { to: this.options.config.alarmTo } : {}), text,
      data: { org: this.store.org, origin: this.store.origin, state: status.state, lagSeconds: status.lagSeconds, frontier: status.frontier, insertLsn: status.insertLsn, key: `records-archive-lag:${this.store.org}:${status.state}` },
    }).catch(() => undefined);
  }

  async #consumerAlarm(lag: ConsumerLag): Promise<void> {
    await this.options.publisher.publish({
      topic: RECORDS_ALARM_TOPIC, kind: "records.consumer-lag", to: lag.consumer,
      text: `${lag.count} record(s) addressed to ${lag.consumer} wait past its processing cursor ${lag.after}, the oldest since ${new Date(lag.oldestAt).toISOString()}: its session reconciles them at its next turn start (records inbox).`,
      data: { org: this.store.org, origin: this.store.origin, consumer: lag.consumer, after: lag.after, count: lag.count, oldestAt: lag.oldestAt, key: `records-consumer-lag:${lag.consumer}:${lag.after}` },
    });
  }

  /** Wait for the pending status write (tests and shutdown). */
  async settled(): Promise<void> { await this.#statusWrite; }

  /**
   * Stop the watchdog, abort running work (a wal-g child, relay runs, queries: their connections
   * are destroyed, so the server rolls back), and end the pool. Bounded: close never hangs a
   * shutdown on a stuck archive check or connection.
   */
  async close(timeoutMs = 5_000): Promise<void> {
    this.watchdog.stop();
    this.#life.abort(new Error("records service closed"));
    const bounded = (work: Promise<unknown>) => Promise.race([work.catch(() => undefined), new Promise((resolve) => setTimeout(resolve, timeoutMs).unref?.())]);
    await bounded(this.watchdog.idle());
    await bounded(this.#statusWrite);
    await bounded(this.store.close());
  }
}
