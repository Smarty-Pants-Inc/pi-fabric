import { createHash } from "node:crypto";
import path from "node:path";
import type { JetStreamClient, KV, KvEntry, NatsConnection, QueuedIterator, Status } from "nats";
import { jsonClone, MeshBatchConflictError, type MeshStateEntry } from "./state-file.js";
import type { StateBackendDeleteInput, StateBackendPutInput } from "./state-backend.js";
import type { AsyncMeshStateStore } from "./state-async.js";

/** Experimental, single-key state; deliberately NOT the synchronous/transactional StateBackend. */
export const NATS_KV_MIN_SERVER_VERSION = "2.14.7";
export const NATS_KV_MAX_VALUE_BYTES = 256 * 1024;
export const NATS_KV_MAX_KEYS = 100_000;
const CODEC_VERSION = "1";
const MAX_BUCKET_BYTES = 32 * 1024 * 1024;
const KEY_PATTERN = /^[a-zA-Z0-9][a-zA-Z0-9._:/-]{0,255}$/;

const validateKey = (key: string, prefix = false): void => {
  if (prefix && key === "") return;
  const unsafe = key.split(/[/:]/).some(part => ["__proto__", "prototype", "constructor"].includes(part));
  if (!KEY_PATTERN.test(key) || unsafe) throw new Error(`Invalid Fabric mesh key: ${key}`);
};
const validateRevision = (version: number): void => {
  if (!Number.isSafeInteger(version) || version < 0) throw new Error("KV revision must be a non-negative safe integer");
};
const segment = (value: string): string => "s" + value.replace(/[.:]/g, char => `=${char.charCodeAt(0).toString(16)}`);

/** Slash -> subject boundary; s prefixes preserve empty segments, =hh escapes dots and colons. */
export const encodeNatsKvKey = (key: string): string => {
  validateKey(key);
  return "k." + key.split("/").map(segment).join(".");
};
export const decodeNatsKvKey = (encoded: string): string => {
  if (!encoded.startsWith("k.")) throw new Error("Invalid Fabric KV key encoding");
  const key = encoded.slice(2).split(".").map(part => {
    if (!/^s(?:[a-zA-Z0-9_-]|=2e|=3a)*$/.test(part)) throw new Error("Invalid Fabric KV key encoding");
    return part.slice(1).replace(/=([0-9a-f]{2})/g, (_, hex: string) => String.fromCharCode(parseInt(hex, 16)));
  }).join("/");
  if (encodeNatsKvKey(key) !== encoded) throw new Error("Non-canonical Fabric KV key encoding");
  return key;
};
/** NATS cannot match a partial token. Select the complete parent, then startsWith on decoded keys. */
export const natsKvPrefixFilter = (prefix: string): string => {
  validateKey(prefix, true);
  const complete = prefix.split("/").slice(0, -1);
  return complete.length ? "k." + complete.map(segment).join(".") + ".>" : "k.>";
};
export const natsKvBucketForRoot = (root: string): string => {
  if (!root.trim()) throw new Error("Mesh root must not be empty");
  return "FABRIC_STATE_" + createHash("sha256").update(path.resolve(root)).digest("hex");
};
export const isSupportedNatsKvServer = (version: string): boolean => {
  const match = /^v?(\d+)\.(\d+)\.(\d+)$/.exec(version);
  if (!match) return false; // prerelease/dev builds are not qualified releases
  const [major, minor, patch] = match.slice(1).map(Number) as [number, number, number];
  return major > 2 || major === 2 && (minor > 14 || minor === 14 && patch >= 7);
};

export interface NatsKvStateStoreOptions {
  servers: string | string[];
  /** Must be true. Runtime MeshStore selection is intentionally not wired to this experiment. */
  experimentalNatsKv: boolean;
  timeoutMs?: number;
  maxValueBytes?: number;
  maxBucketBytes?: number;
  /** Stream-wide message ceiling; history=1 counts live keys AND retained delete markers. */
  maxKeys?: number;
}
export interface NatsKvStateChange {
  key: string;
  /** The JetStream stream sequence, not a process-local/per-key counter. */
  version: number;
  operation: "put" | "delete";
  entry?: MeshStateEntry;
}
export interface NatsKvStateWatch extends AsyncIterableIterator<NatsKvStateChange> { stop(): void }
export interface NatsKvStateWatchOptions {
  /** By default, only changes after subscription establishment. */
  includeCurrent?: boolean;
  resumeFromRevision?: number;
  signal?: AbortSignal;
}
interface Envelope { format: 1; value: unknown; updatedAt: number; updatedBy: MeshStateEntry["updatedBy"] }
const errorNumber = (error: unknown): number | undefined =>
  (error as { api_error?: { err_code?: number } } | null)?.api_error?.err_code;
// 10071 is the traditional expected-sequence failure; 2.14.7 can return 10164
// ("wrong last sequence") for a conflicting publish under concurrent proposals.
// Classify only these numeric API errors, never generic 400/network/capacity failures.
const isSequenceConflict = (error: unknown): boolean => {
  const code = errorNumber(error);
  return code === 10071 || code === 10164;
};
const positiveLimit = (value: number, name: string): number => {
  if (!Number.isSafeInteger(value) || value <= 0) throw new Error(`${name} must be a positive safe integer`);
  return value;
};

export class NatsKvStateStore implements AsyncMeshStateStore {
  readonly kind = "nats-kv" as const;
  readonly bucket: string;
  readonly maxValueBytes: number;
  readonly maxKeys: number;
  readonly #nc: NatsConnection;
  readonly #kv: KV;
  readonly #js: JetStreamClient;
  readonly #timeoutMs: number;
  readonly #watches = new Map<QueuedIterator<KvEntry>, () => void>();
  #closed = false;
  #closing: Promise<void> | undefined;

  private constructor(bucket: string, nc: NatsConnection, kv: KV, js: JetStreamClient, maxValueBytes: number, maxKeys: number, timeoutMs: number) {
    this.bucket = bucket; this.#nc = nc; this.#kv = kv; this.#js = js;
    this.maxValueBytes = maxValueBytes; this.maxKeys = maxKeys; this.#timeoutMs = timeoutMs;
  }

  static async open(root: string, options: NatsKvStateStoreOptions): Promise<NatsKvStateStore> {
    if (options.experimentalNatsKv !== true) throw new Error("NATS KV state requires experimentalNatsKv: true");
    const bucket = natsKvBucketForRoot(root);
    const maxValueBytes = positiveLimit(options.maxValueBytes ?? NATS_KV_MAX_VALUE_BYTES, "maxValueBytes");
    const maxKeys = positiveLimit(options.maxKeys ?? NATS_KV_MAX_KEYS, "maxKeys");
    const maxBucketBytes = positiveLimit(options.maxBucketBytes ?? MAX_BUCKET_BYTES, "maxBucketBytes");
    const timeout = positiveLimit(options.timeoutMs ?? 5_000, "timeoutMs");
    // Load the official client only on explicit first use; no startup connection/import side effect.
    const { connect, StorageType, RetentionPolicy, DiscardPolicy } = await import("nats");
    const nc = await connect({ servers: options.servers, timeout, name: "fabric-state-kv", maxReconnectAttempts: 3 });
    try {
      if (!isSupportedNatsKvServer(nc.info?.version ?? "")) {
        throw new Error(`Fabric KV state requires nats-server ${NATS_KV_MIN_SERVER_VERSION}+ (connected ${nc.info?.version ?? "unknown"})`);
      }
      if ((nc.info?.max_payload ?? 0) < maxValueBytes + 512) throw new Error("NATS max_payload must allow maxValueBytes plus 512 bytes of headers");
      const js = nc.jetstream({ timeout });
      const jsm = await nc.jetstreamManager({ timeout });
      const stream = `KV_${bucket}`;
      const metadata = { fabric_state_codec: CODEC_VERSION, fabric_mesh_root: bucket.slice("FABRIC_STATE_".length) };
      let info;
      try { info = await jsm.streams.info(stream); }
      catch (error) {
        if (errorNumber(error) !== 10059) throw error;
        try {
          info = await jsm.streams.add({ name: stream, subjects: [`$KV.${bucket}.>`], storage: StorageType.File,
            retention: RetentionPolicy.Limits, num_replicas: 3, max_msgs_per_subject: 1, max_msgs: maxKeys,
            max_bytes: maxBucketBytes, max_msg_size: maxValueBytes + 512, max_age: 0,
            discard: DiscardPolicy.New, allow_direct: false, deny_delete: true, deny_purge: true, metadata });
        } catch (creationError) {
          // Another opener may have provisioned it. Validate the winner, never modify it.
          if (errorNumber(creationError) !== 10058) throw creationError;
          info = await jsm.streams.info(stream);
        }
      }
      const c = info.config;
      if (c.storage !== StorageType.File || c.retention !== RetentionPolicy.Limits || c.num_replicas !== 3 ||
          c.max_msgs_per_subject !== 1 || c.max_msgs !== maxKeys || c.max_bytes !== maxBucketBytes ||
          c.max_msg_size !== maxValueBytes + 512 || c.max_age !== 0 || c.discard !== DiscardPolicy.New ||
          c.allow_direct === true || c.deny_delete !== true || c.deny_purge !== true || c.mirror || c.sources?.length ||
          c.subjects?.length !== 1 || c.subjects[0] !== `$KV.${bucket}.>` ||
          c.metadata?.fabric_state_codec !== metadata.fabric_state_codec || c.metadata?.fabric_mesh_root !== metadata.fabric_mesh_root) {
        throw new Error(`Incompatible Fabric KV bucket ${bucket}; refusing to change existing state configuration`);
      }
      // bindOnly does not alter an existing stream. allow_direct=false forces STREAM.MSG.GET at the leader.
      const kv = await js.views.kv(bucket, { bindOnly: true, allow_direct: false, timeout });
      return new NatsKvStateStore(bucket, nc, kv, js, maxValueBytes, maxKeys, timeout);
    } catch (error) { await nc.close(); throw error; }
  }

  /** Transport diagnostics only: reconnect events do not grant read/write or lease authority. */
  connectionStatus(): AsyncIterable<Status> {
    this.#assertOpen();
    return this.#nc.status();
  }

  #assertOpen(): void {
    if (this.#closed) throw new Error("NatsKvStateStore is closed");
    if (!isSupportedNatsKvServer(this.#nc.info?.version ?? "")) throw new Error("Connected NATS server no longer satisfies the Fabric KV minimum version");
  }
  #entry(raw: KvEntry): MeshStateEntry {
    validateRevision(raw.revision);
    const key = decodeNatsKvKey(raw.key);
    const data = raw.json<Envelope>();
    if (!data || typeof data !== "object" || raw.length > this.maxValueBytes || data.format !== 1 || !Number.isFinite(data.updatedAt) ||
        !data.updatedBy || typeof data.updatedBy.id !== "string" || typeof data.updatedBy.name !== "string" ||
        !["main", "agent", "actor"].includes(data.updatedBy.kind) || !Object.hasOwn(data, "value")) {
      throw new Error(`Invalid Fabric KV value for ${key}`);
    }
    return { key, value: data.value, version: raw.revision, updatedAt: data.updatedAt, updatedBy: data.updatedBy };
  }
  async get(key: string): Promise<MeshStateEntry | undefined> {
    this.#assertOpen();
    const raw = await this.#kv.get(encodeNatsKvKey(key));
    this.#assertOpen();
    return raw?.operation === "PUT" ? this.#entry(raw) : undefined;
  }
  /** Leader-read revision, INCLUDING a retained delete marker. Missing virgin keys have revision 0. */
  async version(key: string): Promise<number> {
    this.#assertOpen();
    const revision = (await this.#kv.get(encodeNatsKvKey(key)))?.revision ?? 0;
    this.#assertOpen();
    validateRevision(revision);
    return revision;
  }
  async #conflict(error: unknown, key: string, expected: number): Promise<never> {
    if (!isSequenceConflict(error)) throw error;
    throw new MeshBatchConflictError(key, expected, await this.version(key));
  }
  async put(input: StateBackendPutInput): Promise<MeshStateEntry> {
    this.#assertOpen();
    const encoded = encodeNatsKvKey(input.key);
    if (input.ifVersion !== undefined) validateRevision(input.ifVersion);
    const data: Envelope = { format: 1, value: jsonClone(input.value), updatedAt: Date.now(), updatedBy: jsonClone(input.identity) };
    const bytes = Buffer.from(JSON.stringify(data));
    if (bytes.byteLength > this.maxValueBytes) throw new Error(`Fabric KV value exceeds ${this.maxValueBytes} bytes`);
    let revision: number;
    try {
      revision = input.ifVersion === undefined ? await this.#kv.put(encoded, bytes) : input.ifVersion === 0
        // kv.create automatically resurrects tombstones. That would incorrectly accept stale version 0.
        ? await this.#kv.put(encoded, bytes, { previousSeq: 0 })
        : await this.#kv.update(encoded, bytes, input.ifVersion);
    } catch (error) { return this.#conflict(error, input.key, input.ifVersion ?? 0); }
    this.#assertOpen();
    validateRevision(revision);
    return { key: input.key, value: data.value, version: revision, updatedAt: data.updatedAt, updatedBy: data.updatedBy };
  }
  async compareAndSwap(input: StateBackendPutInput & { ifVersion: number }): Promise<MeshStateEntry> {
    return this.put(input);
  }
  async delete(input: StateBackendDeleteInput): Promise<{ deleted: boolean; version?: number }> {
    this.#assertOpen();
    const encoded = encodeNatsKvKey(input.key);
    if (input.ifVersion !== undefined) validateRevision(input.ifVersion);
    // Conditional deletion is one publish with the same expected-subject-sequence header kv.delete uses.
    // Unlike kv.delete (void), retaining its PubAck returns THIS deletion's exact revision, even if raced.
    for (let attempt = 0; attempt < 8; attempt++) {
      const raw = await this.#kv.get(encoded);
      this.#assertOpen();
      const found = raw?.revision ?? 0;
      validateRevision(found);
      if (input.ifVersion !== undefined && input.ifVersion !== found) throw new MeshBatchConflictError(input.key, input.ifVersion, found);
      if (raw?.operation !== "PUT") return { deleted: false };
      const { headers } = await import("nats");
      const h = headers();
      h.set("KV-Operation", "DEL");
      h.set("Nats-Expected-Last-Subject-Sequence", String(found));
      try {
        const ack = await this.#js.publish(`$KV.${this.bucket}.${encoded}`, new Uint8Array(), { headers: h });
        this.#assertOpen();
        validateRevision(ack.seq);
        return { deleted: true, version: ack.seq };
      } catch (error) {
        if (!isSequenceConflict(error)) throw error;
        if (input.ifVersion !== undefined || attempt === 7) return this.#conflict(error, input.key, input.ifVersion ?? found);
      }
    }
    throw new Error("Unreachable KV delete retry");
  }
  async listAll(prefix = ""): Promise<MeshStateEntry[]> {
    this.#assertOpen();
    const keys = await this.#kv.keys(natsKvPrefixFilter(prefix));
    const selected: string[] = [];
    try {
      for await (const encoded of keys) {
        const key = decodeNatsKvKey(encoded);
        if (key.startsWith(prefix)) selected.push(key);
        if (selected.length > this.maxKeys) throw new Error("Fabric KV key limit exceeded");
      }
    } finally { keys.stop(); }
    this.#assertOpen();
    const entries: MeshStateEntry[] = [];
    // Each get is authoritative. This scan is NOT an atomic multi-key snapshot.
    for (const key of selected.sort((a, b) => a.localeCompare(b))) {
      const entry = await this.get(key);
      if (entry) entries.push(entry);
    }
    this.#assertOpen();
    return entries;
  }
  /** First live matches in stream order, locale-sorted within this bounded page. */
  async list(prefix = "", limit = 100): Promise<MeshStateEntry[]> {
    const boundedLimit = Math.min(positiveLimit(limit, "limit"), this.maxKeys);
    this.#assertOpen();
    const filter = `$KV.${this.bucket}.${natsKvPrefixFilter(prefix)}`;
    // nats 2.29.3's KV.keys().stop() does not cancel its background push subscription.
    // A finite headers-only pull bounds actual server delivery, not just local iteration.
    const { DeliverPolicy } = await import("nats");
    const consumer = await this.#js.consumers.get(`KV_${this.bucket}`, {
      filterSubjects: filter, deliver_policy: DeliverPolicy.LastPerSubject, headers_only: true, inactive_threshold: 5_000,
    });
    const entries: MeshStateEntry[] = [];
    let failed = false;
    try {
      let pending = (await consumer.info()).num_pending;
      while (pending > 0 && entries.length < boundedLimit) {
        const page = await consumer.fetch({ max_messages: Math.min(boundedLimit - entries.length, pending), expires: Math.max(1_000, this.#timeoutMs) });
        let received = 0;
        try {
          for await (const message of page) {
            received++; pending = message.info.pending;
            const operation = message.headers?.get("KV-Operation");
            if (operation === "DEL" || operation === "PURGE") continue;
            const key = decodeNatsKvKey(message.subject.slice(`$KV.${this.bucket}.`.length));
            if (!key.startsWith(prefix)) continue;
            // A concurrent delete can remove a selected value; fill the page with live entries.
            const entry = await this.get(key);
            if (entry) entries.push(entry);
          }
        } finally { await page.close(); }
        if (!received) throw new Error("Fabric KV listing page timed out before pending keys were read");
      }
      this.#assertOpen();
      // Neither this page nor listAll is an atomic snapshot. Global ordering requires listAll.
      return entries.sort((a, b) => a.key.localeCompare(b.key));
    } catch (error) { failed = true; throw error; }
    finally {
      try { await consumer.delete(); }
      catch (error) { if (!failed) throw error; } // preserve the read failure; inactive_threshold bounds abandoned consumers
    }
  }
  async watch(prefix = "", options: NatsKvStateWatchOptions = {}): Promise<NatsKvStateWatch> {
    this.#assertOpen();
    if (options.signal?.aborted) throw new Error("Fabric KV watch aborted");
    if (options.resumeFromRevision !== undefined) {
      validateRevision(options.resumeFromRevision);
      if (options.resumeFromRevision === 0) throw new Error("Watch resume revision must be positive");
    }
    const { KvWatchInclude } = await import("nats");
    const source = await this.#kv.watch({ key: natsKvPrefixFilter(prefix), ignoreDeletes: false,
      include: options.includeCurrent ? KvWatchInclude.LastValue : KvWatchInclude.UpdatesOnly,
      ...(options.resumeFromRevision !== undefined ? { resumeFromRevision: options.resumeFromRevision } : {}) });
    if (this.#closed || options.signal?.aborted) { source.stop(); throw new Error("Fabric KV watch closed or aborted"); }
    const signal = options.signal;
    const stop = () => { source.stop(); this.#watches.delete(source); signal?.removeEventListener("abort", stop); };
    this.#watches.set(source, stop);
    signal?.addEventListener("abort", stop, { once: true });
    const store = this;
    const iterator = (async function* (): AsyncGenerator<NatsKvStateChange> {
      try {
        for await (const raw of source) {
          const key = decodeNatsKvKey(raw.key);
          if (!key.startsWith(prefix)) continue;
          validateRevision(raw.revision);
          yield raw.operation === "PUT"
            ? { key, version: raw.revision, operation: "put", entry: store.#entry(raw) }
            : { key, version: raw.revision, operation: "delete" };
        }
      } finally { stop(); }
    })();
    const finish = iterator.return.bind(iterator);
    const fail = iterator.throw.bind(iterator);
    return Object.assign(iterator, { stop,
      return: async (value?: unknown) => { stop(); return finish(value); },
      throw: async (error?: unknown) => { stop(); return fail(error); },
    });
  }
  close(): Promise<void> {
    if (this.#closing) return this.#closing;
    this.#closed = true;
    for (const stop of this.#watches.values()) stop();
    this.#watches.clear();
    this.#closing = this.#nc.close();
    return this.#closing;
  }
}
