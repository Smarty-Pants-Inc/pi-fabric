import { RemoteRecords } from "./client.js";
import type { FabricRecordsConfig } from "./config.js";
import { RecordsInbox } from "./inbox.js";
import { PublicationRelay, type NudgePublisher } from "./relay.js";
import type { RecordsBackend, RecordsPrincipal } from "./store.js";
import { RecordsWatchdog, type ConsumerLag } from "./watchdog.js";

export const RECORDS_ALARM_TOPIC = "ops.records";
const REALARM_MS = 10 * 60_000;

export interface RecordsServiceOptions {
  config: FabricRecordsConfig;
  /** Publishes nudges and alarms on the mesh as this process's participant. */
  publisher: NudgePublisher;
  /** This process's participant: the principal it registers with the records service. */
  identity: { id: string; name?: string };
  /** Where this process keeps its registered credential. */
  credentialDir: string;
  /** A root session's names; with `wake`, the service keeps its records inbox. */
  names?: () => readonly string[];
  wake?: () => Promise<void>;
}

/**
 * A Fabric process's side of the record layer (C10): the records service's client, the
 * publication relay (nudges go on this org's mesh, which the service cannot write), the root's
 * records inbox, and the watchdog (lag wakes and alarms, and the archive-lag alarm the service
 * reports). Database authority stays in the records service.
 */
export class RecordsService {
  readonly store: RemoteRecords;
  /** The relay's client (the reserved relay principal), when this process holds its credential. */
  readonly relayClient: RemoteRecords | undefined;
  readonly relay: PublicationRelay | undefined;
  readonly inbox: RecordsInbox | undefined;
  readonly watchdog: RecordsWatchdog;
  readonly #life = new AbortController();

  private constructor(readonly options: RecordsServiceOptions, client: RemoteRecords, relayClient: RemoteRecords | undefined) {
    const { config } = options;
    this.store = client;
    this.relayClient = relayClient;
    // Only the reserved relay principal claims and completes nudges and alarms (F9); a process
    // without its credential still delivers by cursor (the records inbox) and wakes its own root.
    this.relay = relayClient ? new PublicationRelay(relayClient, options.publisher) : undefined;
    this.inbox = options.names ? new RecordsInbox(client, client.principalId, options.names, { signal: this.#life.signal }) : undefined;
    this.watchdog = new RecordsWatchdog({
      store: relayClient ?? client,
      ...(this.relay ? { relay: this.relay } : {}),
      ...(relayClient ? { check: (signal: AbortSignal | undefined) => this.#archiveAlarm(signal) } : {}),
      ...(this.inbox ? { self: client.principalId } : {}),
      ...(options.wake ? { wake: options.wake } : {}),
      ...(relayClient ? { alarm: (lag: ConsumerLag) => this.#consumerAlarm(lag) } : {}),
      lagMs: config.consumerLagSeconds * 1000,
      intervalMs: config.watchdogMs,
      signal: this.#life.signal,
    });
  }

  static async open(options: RecordsServiceOptions): Promise<RecordsService> {
    const { config } = options;
    if (!config.enabled) throw new Error("records are off: set records.enabled and records.socket in .pi/fabric.json");
    if (!config.socket) throw new Error("records.socket is required: the org's records service socket");
    const client = new RemoteRecords({
      socket: config.socket, identity: options.identity, credentialDir: options.credentialDir,
      ...(config.credentialFile ? { credentialFile: config.credentialFile } : {}),
    });
    const relayClient = config.relayCredentialFile ? new RemoteRecords({
      socket: config.socket, identity: options.identity, credentialDir: options.credentialDir, credentialFile: config.relayCredentialFile,
    }) : undefined;
    try {
      await client.open();
      await relayClient?.open();
    } catch (error) {
      client.close();
      relayClient?.close();
      throw error;
    }
    const service = new RecordsService(options, client, relayClient);
    void service.relay?.flush(service.#life.signal).catch(() => undefined);
    service.watchdog.start();
    return service;
  }

  /** The service's lifetime; aborted by close(). */
  get signal(): AbortSignal { return this.#life.signal; }

  /** The caller's principal is the service's to decide; this is only for the provider's call shape. */
  principal(): RecordsPrincipal { return { id: this.store.principalId }; }

  /** The backend the provider calls; a commit nudges the relay. */
  get backend(): RecordsBackend {
    const client = this.store;
    return {
      append: async (principal, args, options) => {
        const receipt = await client.append(principal, args, options);
        if (!this.#life.signal.aborted) void this.relay?.flush(this.#life.signal).catch(() => undefined);
        return receipt;
      },
      read: (principal, args, options) => client.read(principal, args, options),
      get: (principal, args, options) => client.get(principal, args, options),
      fold: (principal, args, options) => client.fold(principal, args, options),
      list: (principal, args, options) => client.list(principal, args, options),
    };
  }

  status(): Promise<Record<string, unknown>> { return this.store.status(this.#life.signal); }

  async #archiveAlarm(signal: AbortSignal | undefined): Promise<void> {
    const status = await this.store.status(signal);
    const admission = status.admission as { state?: string; lagSeconds?: number; alarmSeconds?: number; refuseSeconds?: number; frontier?: string; insertLsn?: string } | undefined;
    const state = admission?.state;
    if (state !== "alarm" && state !== "refuse") return;
    if (!this.relayClient || !await this.relayClient.claimAlarm(`archive-lag:${state}`, Date.now(), REALARM_MS, signal)) return;
    const text = state === "refuse"
      ? `records archive lagging ${admission!.lagSeconds} s: records.append refuses new records (C2, over ${admission!.refuseSeconds} s). Restore WAL archiving.`
      : `records archive lagging ${admission!.lagSeconds} s (alarm at ${admission!.alarmSeconds} s; appends are refused past ${admission!.refuseSeconds} s). Check WAL archiving.`;
    await this.options.publisher.publish({
      topic: RECORDS_ALARM_TOPIC, kind: "records.archive-lag", ...(this.options.config.alarmTo ? { to: this.options.config.alarmTo } : {}), text,
      data: { org: this.store.org, origin: this.store.origin, state, lagSeconds: admission!.lagSeconds, frontier: admission!.frontier, insertLsn: admission!.insertLsn, key: `records-archive-lag:${this.store.org}:${state}` },
    }).catch(() => undefined);
  }

  async #consumerAlarm(lag: ConsumerLag): Promise<void> {
    await this.options.publisher.publish({
      topic: RECORDS_ALARM_TOPIC, kind: "records.consumer-lag", to: lag.consumer,
      text: `${lag.count} record(s) addressed to ${lag.consumer} wait past its processing cursor ${lag.after}, the oldest since ${new Date(lag.oldestAt).toISOString()}: its session reconciles them at its next turn start (records inbox).`,
      data: { org: this.store.org, origin: this.store.origin, consumer: lag.consumer, after: lag.after, count: lag.count, oldestAt: lag.oldestAt, key: `records-consumer-lag:${lag.consumer}:${lag.after}` },
    });
  }

  /** Cancel calls in flight (the records service rolls them back), stop the watchdog, disconnect. Bounded. */
  async close(timeoutMs = 5_000): Promise<void> {
    this.watchdog.stop();
    this.#life.abort(new Error("records service closed"));
    this.store.close();
    this.relayClient?.close();
    await Promise.race([this.watchdog.idle(), new Promise((resolve) => setTimeout(resolve, timeoutMs).unref?.())]);
  }
}
