// The NATS JetStream mesh state backend, single host, phase 1 (smarty-dev#7504; design: docs/mesh-backends.md
// option (c)). One JetStream message per committed batch, published with the stream's expected last sequence (a
// compare-and-swap over the whole stream) and a message id (dedupe of a retried publish). A projector applies the
// stream, in order, to a local read model; every read of the StateBackend contract is served synchronously from it.
//
// This module never imports a NATS client: the real port (state-backend-nats-client.ts) is loaded by
// `openNatsStateBackend` at first use, so the startup graph is unchanged. The selector's factory is synchronous, so a
// host prepares the backend first (`openNatsStateBackend`, async: connect, refuse, replay) and the factory then
// returns the prepared backend for that root, or refuses. `nats` never falls back to `file`.
import { createHash, randomUUID } from "node:crypto";
import dns from "node:dns";
import fs from "node:fs";
import net from "node:net";
import path from "node:path";
import { gte as semverGte, valid as semverValid } from "semver";
import { captureStorageDelete, captureStoragePut, storageRevision } from "../verified/storage.js";
import type { MeshIdentity } from "./event-log.js";
import type {
  MeshCommitEffects, StateBackend, StateBackendBatchInput, StateBackendDeleteInput, StateBackendDiagnostics,
  StateBackendPutInput,
} from "./state-backend.js";
import {
  bracketFileRead, jsonClone, MeshBatchConflictError, MeshStateFileReadChangedError,
  type MeshBatchResult, type MeshBatchView, type MeshReadOptions, type MeshStateEntry,
} from "./state-file.js";
import { validateMeshStateKey } from "./state-sqlite.js";
import { MeshLockTimeoutError } from "../core/atomic-write.js";

// ------------------------------------------------------------------ the JetStream port

/** A JetStream publish refused because the stream's last sequence was not the expected one (NATS err_code 10071). */
export class NatsWrongLastSequenceError extends Error {
  readonly code = "FABRIC_MESH_NATS_WRONG_LAST_SEQUENCE";
  /** Set by the backend: an earlier attempt of this same publish got no answer (it may be stored). */
  afterLostAck = false;
  constructor(readonly expected: number, detail = "") {
    super(`JetStream wrong last sequence (expected ${expected})${detail ? `: ${detail}` : ""}`);
    this.name = "NatsWrongLastSequenceError";
  }
}

/** One stored stream message. */
export interface NatsStreamMessage { seq: number; data: Uint8Array }

/**
 * The JetStream operations this backend uses. The real implementation wraps the official nats.js v3 client
 * (state-backend-nats-client.ts); tests use an in-process fake with the same semantics.
 */
export interface JetStreamPort {
  /** Server facts for the refusals: its version and whether it fsyncs every write (`/varz` sync_always). */
  server(): Promise<{ version: string; syncAlways: boolean | undefined; maxPayload?: number; authRequired?: boolean }>;
  /** Creates the stream if absent (file storage, these subjects; an existing stream gains missing ones); returns its last sequence. */
  ensureStream(stream: string, subjects: string[], options: { maxMsgSize: number }): Promise<{ lastSeq: number; maxMsgSize?: number }>;
  /** The stream's last sequence now (leader). */
  lastSequence(stream: string): Promise<number>;
  /** The stream's first stored sequence now (above 1 after a purge). */
  firstSequence(stream: string): Promise<number>;
  /** Whether a stream of this name exists (never creates it). */
  streamExists(stream: string): Promise<boolean>;
  /** One stored message by sequence, or undefined when it is gone (purged) or never existed. */
  getMessage(stream: string, seq: number): Promise<NatsStreamMessage | undefined>;
  /** The last stored message on one subject, or undefined. */
  lastBySubject(stream: string, subject: string): Promise<NatsStreamMessage | undefined>;
  /** Removes every message below `beforeSeq`. */
  purge(stream: string, beforeSeq: number): Promise<void>;
  /**
   * Publishes with `expect.lastSequence` and `msgID`. A wrong last sequence throws `NatsWrongLastSequenceError`; no
   * answer (a timeout, no responders, a dropped connection: the ack may be lost) throws an error with `transient: true`;
   * anything else (an oversized payload, a permission) is final.
   */
  publish(subject: string, data: Uint8Array, options: { expectLastSequence: number; msgID: string }): Promise<{ seq: number; duplicate: boolean }>;
  /** Delivers every message after `afterSeq`, in order, then each new one as it is stored; a broken delivery calls `onError`. */
  consume(stream: string, afterSeq: number, onMessage: (message: NatsStreamMessage) => void, onError?: (error: unknown) => void): Promise<{ stop(): Promise<void> }>;
  close(): Promise<void>;
}

// ------------------------------------------------------------------ the stream records

/** One committed change: the entry after the batch, or a delete (the key's tombstone version). */
type NatsChange =
  | { key: string; put: { value: unknown; version: number; updatedAt: number; updatedBy: MeshIdentity } }
  | { key: string; deleted: { version: number } };

/**
 * A stream message. `batch` is a committed writeBatch; `retire` ends the stream's authority (rollback).
 * `snapshot-chunk` and `snapshot-end` are a snapshot of the model at sequence `base`: its chunks are published as one
 * CAS chain right after `base` (chunk i expects the sequence before it), then the end record. Any interleaved commit
 * breaks the chain and the snapshot is abandoned (its chunks are inert). Only a complete chain (an end record) is ever
 * read; the stream is then purged below its first chunk, and a host opening later replays from it (bounded replay).
 */
type NatsRecord =
  | { format: 1; type: "batch"; id: string; base: number; highWater: number; changes: NatsChange[] }
  | { format: 1; type: "retire"; epoch: number }
  | { format: 1; type: "import-begin"; epoch: number }
  | { format: 1; type: "import-commit"; epoch: number }
  | { format: 1; type: "snapshot-chunk"; snapshot: string; index: number; part: string }
  | { format: 1; type: "snapshot-end"; snapshot: string; base: number; first: number; chunks: number; header: SnapshotHeader };

/**
 * What a snapshot's end record commits to (pi-fabric#907 P1-1). `digest` is the sha256 of the canonical body (the
 * joined chunks); `prev` is the digest of the snapshot before it on this stream ("" for the first); `bytes` and
 * `entries` bound the body before it is joined; `status` and `epoch` bind the lifecycle. The loader recomputes and
 * compares every field, and every live projector re-serializes its OWN model when it applies an end record and
 * compares the digest, so an altered, torn or swapped chunk, or a snapshot that differs from the stream it covers,
 * fails closed. ponytail: this is integrity, not authentication; a writer that may publish to the stream can also
 * publish batches, so forging a whole consistent snapshot is the same trust as forging a batch.
 */
interface SnapshotHeader { digest: string; prev: string; bytes: number; entries: number; status: NatsStatus; epoch: number }

const RECORD_TYPES = ["batch", "retire", "import-begin", "import-commit", "snapshot-chunk", "snapshot-end"];
const encode = (record: NatsRecord): Uint8Array => Buffer.from(JSON.stringify(record), "utf8");
const decode = (data: Uint8Array): NatsRecord => {
  const record = JSON.parse(Buffer.from(data).toString("utf8")) as NatsRecord;
  if (record?.format !== 1 || !RECORD_TYPES.includes(record.type)) {
    throw new Error("Unknown Fabric mesh NATS record");
  }
  return record;
};

// ------------------------------------------------------------------ the read model

/** One immutable model snapshot (copy on write per applied message), so a `stateToken` reads exactly that state. */
interface NatsSnapshot {
  readonly seq: number;
  readonly entries: ReadonlyMap<string, MeshStateEntry>;
  /** Every key's latest version, tombstones included (the CAS base of an absent key). */
  readonly versions: ReadonlyMap<string, number>;
  /** Absent keys whose tombstone is retained, oldest first (bounded). */
  readonly tombstones: readonly string[];
  readonly highWater: number;
  readonly status: "ready" | "importing" | "retired";
  /** The epoch of the last lifecycle record (import-begin, import-commit, retire); 0 before any. */
  readonly epoch: number;
  /** The sequence of the last complete snapshot's end record (0: none). */
  readonly snapshotEnd: number;
  /** That snapshot's digest ("": none): the `prev` the next snapshot must name. */
  readonly snapshotDigest: string;
}

const EMPTY: NatsSnapshot = { seq: 0, entries: new Map(), versions: new Map(), tombstones: [], highWater: 0, status: "ready", epoch: 0,
  snapshotEnd: 0, snapshotDigest: "" };

/**
 * The model as a snapshot body (JSON), canonical: entries and versions sorted by key (code-unit order), so every
 * replica serializes the same state to the same bytes whatever its map insertion order. `parseModel` is its inverse.
 */
const byKey = (left: string, right: string): number => (left < right ? -1 : left > right ? 1 : 0);
/**
 * The canonical body, built piece by piece and counted as it grows (pi-fabric#907 round 2): past `maxBytes` it stops
 * and returns undefined, so an over-bound state is refused without ever building its whole string. The bytes equal
 * `JSON.stringify({ entries, versions, tombstones, highWater, status, epoch })` exactly.
 */
const serializeModelBounded = (snapshot: NatsSnapshot, maxBytes: number): string | undefined => {
  const pieces: string[] = [];
  let bytes = 0;
  const push = (piece: string): boolean => {
    bytes += Buffer.byteLength(piece, "utf8");
    if (bytes > maxBytes) return false;
    pieces.push(piece);
    return true;
  };
  if (!push('{"entries":[')) return undefined;
  let first = true;
  for (const key of [...snapshot.entries.keys()].sort(byKey)) {
    if (!push(`${first ? "" : ","}${JSON.stringify(snapshot.entries.get(key))}`)) return undefined;
    first = false;
  }
  if (!push('],"versions":[')) return undefined;
  first = true;
  for (const key of [...snapshot.versions.keys()].sort(byKey)) {
    if (!push(`${first ? "" : ","}${JSON.stringify([key, snapshot.versions.get(key)])}`)) return undefined;
    first = false;
  }
  if (!push(`],"tombstones":${JSON.stringify(snapshot.tombstones)},"highWater":${JSON.stringify(snapshot.highWater)},` +
    `"status":${JSON.stringify(snapshot.status)},"epoch":${JSON.stringify(snapshot.epoch)}}`)) return undefined;
  return pieces.join("");
};
const serializeModel = (snapshot: NatsSnapshot): string => serializeModelBounded(snapshot, Number.POSITIVE_INFINITY)!;

/**
 * The snapshot digest (pi-fabric#907 round 2): ONE canonical encoding of everything the end record states (the
 * snapshot id, its base, its end sequence, its chunk count, the previous digest, the lifecycle, the size) followed by
 * the body. Nothing in the header can be altered without the digest changing, so a first-time opener uses `base` and
 * `prev` only after this verifies.
 */
const snapshotDigest = (fields: { snapshot: string; base: number; end: number; chunks: number; prev: string; status: string;
  epoch: number; bytes: number; entries: number }, body: string): string =>
  createHash("sha256").update(JSON.stringify(["fabric-mesh-nats-snapshot", 1, fields.snapshot, fields.base, fields.end, fields.chunks,
    fields.prev, fields.status, fields.epoch, fields.bytes, fields.entries]), "utf8").update("\n").update(body, "utf8").digest("hex");
const sha256 = (text: string): string => createHash("sha256").update(text, "utf8").digest("hex");
const parseModel = (body: string, seq: number): NatsSnapshot => {
  const value = JSON.parse(body) as { entries: MeshStateEntry[]; versions: [string, number][]; tombstones: string[]; highWater: number;
    status: NatsSnapshot["status"]; epoch: number };
  if (!Array.isArray(value.entries) || !Array.isArray(value.versions) || !Array.isArray(value.tombstones) ||
      !["ready", "importing", "retired"].includes(value.status) || !Number.isSafeInteger(value.epoch)) {
    throw new MeshStateNatsRefusedError("the snapshot body is not a model");
  }
  const entries = new Map<string, MeshStateEntry>();
  for (const entry of value.entries) { validateMeshStateKey(entry.key); storageRevision(entry.version); entries.set(entry.key, entry); }
  const versions = new Map<string, number>();
  for (const [key, version] of value.versions) { validateMeshStateKey(key); versions.set(key, storageRevision(version)); }
  return { seq, entries, versions, tombstones: value.tombstones.map(String), highWater: storageRevision(value.highWater), status: value.status,
    epoch: value.epoch, snapshotEnd: seq, snapshotDigest: "" };
};

/** A snapshot's chain or contents do not match what its end record commits to: fail closed, never load it. */
export class MeshStateNatsSnapshotIntegrityError extends Error {
  readonly code = "FABRIC_MESH_STATE_NATS_SNAPSHOT_INTEGRITY";
  constructor(detail: string) {
    super(`FABRIC_MESH_STATE_NATS_SNAPSHOT_INTEGRITY: ${detail}`);
    this.name = "MeshStateNatsSnapshotIntegrityError";
  }
}

/** A snapshot or a replay is larger than this host's bounds: fail closed, never truncate. */
export class MeshStateNatsLimitError extends Error {
  readonly code = "FABRIC_MESH_STATE_NATS_LIMIT";
  constructor(detail: string) {
    super(`FABRIC_MESH_STATE_NATS_LIMIT: ${detail}`);
    this.name = "MeshStateNatsLimitError";
  }
}

/** The projector could not get past repeated purges: reads and writes fail until the backend is reopened. */
export class MeshStateNatsUnavailableError extends Error {
  readonly code = "FABRIC_MESH_STATE_NATS_UNAVAILABLE";
  constructor(detail: string) {
    super(`FABRIC_MESH_STATE_NATS_UNAVAILABLE: ${detail}`);
    this.name = "MeshStateNatsUnavailableError";
  }
}

/** The bounds a snapshot load and an open replay are checked against BEFORE anything is fetched or joined. */
export interface NatsSnapshotLimits { maxBytes: number; maxEntries: number; maxChunks: number; maxReplay: number; maxMessageBytes: number }

/** A model's header: what a snapshot of it at `snapshot.seq`, in `chunks` chunks, commits to. */
const headerOf = (snapshot: NatsSnapshot, body: string, id: string, chunks: number): SnapshotHeader => {
  const fields = { prev: snapshot.snapshotDigest, bytes: Buffer.byteLength(body, "utf8"), entries: snapshot.entries.size,
    status: snapshot.status, epoch: snapshot.epoch };
  return { ...fields, digest: snapshotDigest({ ...fields, snapshot: id, base: snapshot.seq, end: snapshot.seq + chunks + 1, chunks }, body) };
};
/** The digest an end record at `end` must carry for this body. */
const expectedDigest = (record: { snapshot: string; base: number; chunks: number; header: SnapshotHeader }, end: number, body: string): string =>
  snapshotDigest({ snapshot: record.snapshot, base: record.base, end, chunks: record.chunks, prev: record.header.prev,
    status: record.header.status, epoch: record.header.epoch, bytes: record.header.bytes, entries: record.header.entries }, body);

const isHeader = (value: unknown): value is SnapshotHeader => {
  const header = value as Partial<SnapshotHeader> | undefined;
  return typeof header?.digest === "string" && /^[0-9a-f]{64}$/.test(header.digest) && typeof header.prev === "string" &&
    (header.prev === "" || /^[0-9a-f]{64}$/.test(header.prev)) && Number.isSafeInteger(header.bytes) && header.bytes! >= 0 &&
    Number.isSafeInteger(header.entries) && header.entries! >= 0 && ["ready", "importing", "retired"].includes(header.status ?? "") &&
    Number.isSafeInteger(header.epoch) && header.epoch! >= 0;
};

type NatsStatus = NatsSnapshot["status"];
type LifecycleRecord = Extract<NatsRecord, { epoch: number }>;
/**
 * The stream's lifecycle, the only legal transitions (docs/mesh-backends.md, Migration). A batch is legal while
 * ready or importing (the importer writes). Retired is terminal. Any other record in any state is a protocol
 * violation: the projector fails closed.
 */
const LIFECYCLE: Record<LifecycleRecord["type"], { from: readonly NatsStatus[]; epoch: "newer" | "same"; to: NatsStatus }> = {
  "import-begin": { from: ["ready"], epoch: "newer", to: "importing" },
  "import-commit": { from: ["importing"], epoch: "same", to: "ready" },
  retire: { from: ["ready", "importing"], epoch: "newer", to: "retired" },
};
const lifecycle = (snapshot: NatsSnapshot, record: LifecycleRecord, seq: number): NatsSnapshot => {
  const rule = LIFECYCLE[record.type];
  const epochOk = Number.isSafeInteger(record.epoch) &&
    (rule.epoch === "same" ? record.epoch === snapshot.epoch : record.epoch > snapshot.epoch);
  if (!rule.from.includes(snapshot.status) || !epochOk) {
    throw new MeshStateNatsRefusedError(`stream message ${seq} (${record.type}, epoch ${String(record.epoch)}) is illegal while ${snapshot.status} at epoch ${snapshot.epoch}`);
  }
  return { ...snapshot, seq, status: rule.to, epoch: record.epoch };
};

/** Thrown by reads and writes once the stream is retired (rolled back to sqlite or file), or while it is importing. */
export class MeshStateNatsRefusedError extends Error {
  readonly code = "FABRIC_MESH_STATE_NATS_REFUSED";
  constructor(message: string) {
    super(`FABRIC_MESH_STATE_NATS_REFUSED: ${message}`);
    this.name = "MeshStateNatsRefusedError";
  }
}

/** A `fresh` read while the projector lags the stream, or a write that ran out of its retry budget: retryable. */
export class MeshStateNatsBusyError extends MeshLockTimeoutError {
  readonly busyCode = "FABRIC_MESH_STATE_BUSY";
  constructor(detail: string) {
    super(` (NATS state: ${detail})`, 1, 0);
    this.message = `FABRIC_MESH_STATE_BUSY: ${detail}`;
    this.name = "MeshStateNatsBusyError";
  }
}

/** A publish got no answer within the write budget: the batch may or may not have committed. Not retryable as is. */
export class MeshStateNatsOutcomeUnknownError extends Error {
  readonly code = "FABRIC_MESH_STATE_NATS_OUTCOME_UNKNOWN";
  constructor(readonly batchId: string, detail: string) {
    super(`FABRIC_MESH_STATE_NATS_OUTCOME_UNKNOWN: batch ${batchId} got no acknowledgement (${detail}); re-read before writing again`);
    this.name = "MeshStateNatsOutcomeUnknownError";
  }
}

/**
 * Applies one stream message to a snapshot. Deterministic and idempotent: a message at or below the snapshot's
 * sequence is ignored; a batch whose `base` is not the snapshot's sequence cannot have passed the publish CAS, so
 * it is a protocol violation and refuses (the model never guesses).
 */
export const applyNatsMessage = (snapshot: NatsSnapshot, message: NatsStreamMessage, maxTombstones: number,
  limits: Pick<NatsSnapshotLimits, "maxBytes" | "maxEntries" | "maxChunks"> = natsSnapshotLimits({})): NatsSnapshot => {
  if (message.seq <= snapshot.seq) return snapshot;
  const record = decode(message.data);
  if (record.type === "retire" || record.type === "import-begin" || record.type === "import-commit") return lifecycle(snapshot, record, message.seq);
  if (snapshot.status === "retired") throw new MeshStateNatsRefusedError(`stream message ${message.seq} (${record.type}) is after the stream was retired`);
  // A snapshot's records change no state; a complete one (its end) marks where a later host may start.
  if (record.type === "snapshot-chunk") return { ...snapshot, seq: message.seq };
  if (record.type === "snapshot-end") {
    // Every replica checks a published snapshot against its OWN model (the chunks between change no state), and
    // that it continues this stream's snapshot chain: a snapshot that misstates the stream fails closed here too.
    const header = record.header;
    const mismatch = () => new MeshStateNatsSnapshotIntegrityError(`snapshot ${record.snapshot} (end record ${message.seq}) does not match the stream it covers`);
    if (!isHeader(header)) throw mismatch();
    // pi-fabric#907 round 4: the CONFIGURED bounds first (the loader's checks), before anything is serialized, so an
    // untrusted `header.bytes` never sizes the work.
    if (header.bytes > limits.maxBytes || header.entries > limits.maxEntries || !Number.isSafeInteger(record.chunks) || record.chunks > limits.maxChunks) {
      throw new MeshStateNatsLimitError(`snapshot ${record.snapshot} (end record ${message.seq}) claims ${header.bytes} bytes, ${header.entries} entries, ${String(record.chunks)} chunks (limits ${limits.maxBytes}, ${limits.maxEntries}, ${limits.maxChunks})`);
    }
    // The cheap fields next; the sequence fields against where this end record actually is (they are in the digest too).
    if (record.chunks < 1 || !Number.isSafeInteger(record.base) || record.first !== record.base + 1 || record.base + record.chunks + 1 !== message.seq ||
        header.prev !== snapshot.snapshotDigest || header.status !== snapshot.status || header.epoch !== snapshot.epoch ||
        header.entries !== snapshot.entries.size) throw mismatch();
    // Our own body, capped at the smaller of the header's size and the configured bound: a larger model cannot match.
    const body = serializeModelBounded(snapshot, Math.min(header.bytes, limits.maxBytes));
    if (body === undefined || header.digest !== expectedDigest(record, message.seq, body)) throw mismatch();
    return { ...snapshot, seq: message.seq, snapshotEnd: message.seq, snapshotDigest: header.digest };
  }
  if (record.base !== snapshot.seq) {
    throw new MeshStateNatsRefusedError(`stream message ${message.seq} was committed on sequence ${record.base}, not ${snapshot.seq}`);
  }
  const entries = new Map(snapshot.entries);
  const versions = new Map(snapshot.versions);
  let tombstones = snapshot.tombstones.slice();
  for (const change of record.changes) {
    validateMeshStateKey(change.key);
    if ("put" in change) {
      entries.set(change.key, { key: change.key, ...change.put });
      versions.set(change.key, storageRevision(change.put.version));
      tombstones = tombstones.filter(key => key !== change.key);
    } else {
      entries.delete(change.key);
      versions.set(change.key, storageRevision(change.deleted.version));
      tombstones = [...tombstones.filter(key => key !== change.key), change.key];
    }
  }
  // The same bound as the file backend: forget the oldest absent keys' tombstones (their versions) past the limit.
  while (tombstones.length > maxTombstones) {
    const forgotten = tombstones.shift()!;
    if (!entries.has(forgotten)) versions.delete(forgotten);
  }
  return { seq: message.seq, entries, versions, tombstones, highWater: Math.max(snapshot.highWater, storageRevision(record.highWater)),
    status: snapshot.status, epoch: snapshot.epoch, snapshotEnd: snapshot.snapshotEnd, snapshotDigest: snapshot.snapshotDigest };
};

const sortedKeys = (keys: Iterable<string>): string[] => [...keys].sort((left, right) => left.localeCompare(right));
const viewOf = (snapshot: NatsSnapshot): MeshBatchView => ({
  get: key => { const entry = snapshot.entries.get(key); return entry ? jsonClone(entry) : undefined; },
  listAll: prefix => sortedKeys([...snapshot.entries.keys()].filter(key => key.startsWith(prefix))).map(key => jsonClone(snapshot.entries.get(key)!)),
  version: key => snapshot.versions.get(key) ?? 0,
});

// ------------------------------------------------------------------ the backend

export interface NatsStateBackendOptions {
  port: JetStreamPort;
  /** The stream (one per mesh root). */
  stream: string;
  /** The subject of the batches. The CAS is on the whole stream's last sequence, whatever the subject. */
  subject: string;
  /** The subject of the snapshot records (the latest end record is looked up by subject). */
  snapshotSubject: string;
  maxEventBytes: number;
  maxStateTombstones?: number;
  /** The budget of one write's CAS retries, then `MeshStateNatsBusyError`. Default 5 s. */
  lockTimeoutMs?: number;
  /** Take a snapshot (and purge below it) once this many messages follow the last one; 0 never. Default 10 000. */
  snapshotEvery?: number;
  /** The state bound (the file backend's `maxStateBytes`, default 32 MiB): a snapshot body larger than it refuses. */
  maxStateBytes?: number;
  /** At most this many messages are replayed after the snapshot (or from 1) at open. Default 100 000. */
  maxReplay?: number;
}

const DEFAULT_TOMBSTONES = 2_048;
const DEFAULT_SNAPSHOT_EVERY = 10_000;
const DEFAULT_MAX_STATE_BYTES = 32 * 1024 * 1024;
const DEFAULT_MAX_REPLAY = 100_000;
/** Re-syncs after a purge gap before the backend gives up (pi-fabric#907 P1-3). */
const RESYNC_ATTEMPTS = 3;
/** Characters of the model's JSON per snapshot chunk: at most 6 bytes each once escaped, far under the 1 MiB payload. */
const SNAPSHOT_CHUNK = 64 * 1024;
/** The largest stream message (pi-fabric#907): the stream's max_msg_size and the server's max_payload must not exceed it. */
export const MAX_MESSAGE_BYTES = 1024 * 1024;
/** A chunk record's raw bound: 64 Ki characters at most 6 bytes each once JSON-escaped, plus its envelope. */
const MAX_CHUNK_RECORD_BYTES = SNAPSHOT_CHUNK * 6 + 4_096;
/** An end record's raw bound: ids, integers and two digests. */
const MAX_END_RECORD_BYTES = 4_096;
/** The smallest serialized entry (`{"key":"a","value":0,...}` with its version pair) is well over 32 bytes. */
const MIN_ENTRY_BYTES = 32;

/** The bounds of a backend: the state bound, and what it implies for entries and chunks (each chunk is ≥ 1 byte per character). */
export const natsSnapshotLimits = (options: Pick<NatsStateBackendOptions, "maxStateBytes" | "maxReplay">): NatsSnapshotLimits => {
  const maxBytes = Math.max(1, Math.floor(options.maxStateBytes ?? DEFAULT_MAX_STATE_BYTES));
  return { maxBytes, maxEntries: Math.ceil(maxBytes / MIN_ENTRY_BYTES), maxChunks: Math.ceil(maxBytes / SNAPSHOT_CHUNK),
    maxReplay: Math.max(1, Math.floor(options.maxReplay ?? DEFAULT_MAX_REPLAY)),
    // One stream message (a batch, a snapshot chunk, an end record): NATS's default max_payload. A chunk record is at
    // most MAX_CHUNK_RECORD_BYTES (~390 KiB), well under it.
    maxMessageBytes: MAX_MESSAGE_BYTES };
};

/**
 * A single put or delete fails its CAS with the verified storage kernel's plain Error, as on file and sqlite
 * (verified/storage.ts); only writeBatch throws MeshBatchConflictError. Same message on every backend.
 */
const singleWriteContract = <T>(write: Promise<T>): Promise<T> => write.catch((error: unknown) => {
  throw error instanceof MeshBatchConflictError ? new Error(error.message) : error;
});

export class NatsStateBackend implements StateBackend {
  readonly kind = "nats" as const;
  readonly readCacheMs = 0;
  readonly backgroundReadCacheMs = 0;
  readonly readCacheRemainingMs = 0;
  readonly #options: NatsStateBackendOptions;
  readonly #streamId = randomUUID();
  #snapshot: NatsSnapshot = EMPTY;
  /** The highest sequence this process knows the stream holds (acks and deliveries). */
  #knownSeq = 0;
  #waiters: { seq: number; resolve: () => void; reject: (error: unknown) => void }[] = [];
  #failure: unknown;
  #consumer: { stop(): Promise<void> } | undefined;
  #closed = false;
  #casRetries = 0;
  /** Bumped per projector start: deliveries of a stopped consumer are ignored. */
  #generation = 0;
  #resyncing = false;
  /** A purge gap seen while a re-sync was running: the re-sync loops (bounded) instead of dropping it. */
  #gapPending = false;
  readonly #limits: NatsSnapshotLimits;
  #compacting: Promise<boolean> | undefined;
  #snapshots = 0;
  #resyncs = 0;

  private constructor(options: NatsStateBackendOptions) { this.#options = options; this.#limits = natsSnapshotLimits(options); }

  /** Loads the latest snapshot, connects the projector after it and replays up to the stream's current last sequence. */
  static async open(options: NatsStateBackendOptions): Promise<NatsStateBackend> {
    const backend = new NatsStateBackend(options);
    const { lastSeq, maxMsgSize } = await options.port.ensureStream(options.stream, [options.subject, options.snapshotSubject],
      { maxMsgSize: backend.#limits.maxMessageBytes });
    // The server enforces the message cap too (pi-fabric#907): a stream with no limit, or a larger one, is refused.
    if (maxMsgSize === undefined || maxMsgSize <= 0 || maxMsgSize > backend.#limits.maxMessageBytes) {
      throw new MeshStateNatsLimitError(`the stream ${options.stream} allows messages of ${maxMsgSize === undefined || maxMsgSize <= 0 ? "any size" : `${maxMsgSize} bytes`} (limit ${backend.#limits.maxMessageBytes}); set its max_msg_size`);
    }
    backend.#knownSeq = lastSeq;
    await backend.#startProjector();
    await backend.#appliedThrough(backend.#knownSeq);
    return backend;
  }

  /**
   * (Re)starts the projector: from the latest complete snapshot when it is ahead of the model, else from the model.
   * The replay after it is bounded (pi-fabric#907): the stream's last sequence is read AFTER the snapshot load (writes
   * during the load count), and a backlog over `maxReplay` loads a newer snapshot once, then refuses (LIMIT).
   */
  async #startProjector(): Promise<void> {
    for (let attempt = 1; ; attempt++) {
      const loaded = await this.#loadSnapshot();
      if (loaded && loaded.seq > this.#snapshot.seq) { this.#snapshot = loaded; this.#settle(); }
      const last = await this.#options.port.lastSequence(this.#options.stream);
      this.#knownSeq = Math.max(this.#knownSeq, last);
      const backlog = last - this.#snapshot.seq;
      if (backlog <= this.#limits.maxReplay) break;
      if (attempt >= 2) {
        throw new MeshStateNatsLimitError(`the projector would replay ${backlog} messages after sequence ${this.#snapshot.seq} (limit ${this.#limits.maxReplay})`);
      }
    }
    const generation = ++this.#generation;
    this.#consumer = await this.#options.port.consume(this.#options.stream, this.#snapshot.seq,
      message => { if (generation === this.#generation) this.#apply(message); },
      error => { if (generation === this.#generation) this.#fail(error); });
  }

  /**
   * The latest complete snapshot as a model at its base sequence, or undefined when the stream has none. A stream
   * purged without one cannot be replayed: refuse. A newer snapshot's purge may remove the chunks being read: retry.
   */
  async #loadSnapshot(): Promise<NatsSnapshot | undefined> {
    const { port, stream, snapshotSubject } = this.#options;
    for (let attempt = 1; attempt <= 5; attempt++) {
      const end = await port.lastBySubject(stream, snapshotSubject);
      if (!end) {
        if (await port.firstSequence(stream) > 1) throw new MeshStateNatsRefusedError(`the stream ${stream} was purged without a snapshot`);
        return undefined;
      }
      const verified = await this.#verifiedSnapshot(end);
      if (verified !== "gone") return verified;
      // A newer snapshot's purge took the chunks: retry with it. Still the latest end record: a chunk was dropped.
      const latest = await port.lastBySubject(stream, snapshotSubject);
      if (latest?.seq === end.seq) throw new MeshStateNatsSnapshotIntegrityError(`a chunk of the snapshot ending at ${end.seq} is missing`);
    }
    throw new MeshStateNatsBusyError(`newer snapshots kept replacing the one being read`);
  }

  /**
   * Reads and verifies the snapshot whose end record is `end`: the record's shape, the bounds (before any chunk is
   * fetched), the chunk chain, then the digest over the canonical header encoding plus the body, then the model's
   * own lifecycle and size. Returns the model at its base (BEFORE the end record), or "gone" when a chunk is no
   * longer stored. Used by the loader and by `compact()` before it purges (pi-fabric#907 round 2).
   */
  async #verifiedSnapshot(end: NatsStreamMessage): Promise<NatsSnapshot | "gone"> {
    const { port, stream } = this.#options;
    {
      // The RAW size is bounded before anything is decoded (pi-fabric#907 round 2).
      if (end.data.byteLength > MAX_END_RECORD_BYTES) throw new MeshStateNatsLimitError(`the snapshot end record ${end.seq} is ${end.data.byteLength} bytes (limit ${MAX_END_RECORD_BYTES})`);
      const record = decode(end.data);
      if (record.type !== "snapshot-end" || !Number.isSafeInteger(record.base) || record.base < 0 || !Number.isSafeInteger(record.chunks) ||
          record.chunks < 1 || record.first !== record.base + 1 || end.seq !== record.first + record.chunks || !isHeader(record.header)) {
        throw new MeshStateNatsSnapshotIntegrityError(`stream message ${end.seq} is not a complete snapshot end record`);
      }
      const { header } = record;
      // P1-2: every bound is checked from the header BEFORE a chunk is fetched; the joined size again while joining.
      if (record.chunks > this.#limits.maxChunks) throw new MeshStateNatsLimitError(`snapshot ${record.snapshot} has ${record.chunks} chunks (limit ${this.#limits.maxChunks})`);
      if (header.bytes > this.#limits.maxBytes) throw new MeshStateNatsLimitError(`snapshot ${record.snapshot} is ${header.bytes} bytes (limit ${this.#limits.maxBytes})`);
      if (header.entries > this.#limits.maxEntries) throw new MeshStateNatsLimitError(`snapshot ${record.snapshot} has ${header.entries} entries (limit ${this.#limits.maxEntries})`);
      const parts: string[] = [];
      let bytes = 0;
      for (let index = 0; index < record.chunks; index++) {
        const message = await port.getMessage(stream, record.first + index);
        if (!message) break; // purged by a newer snapshot meanwhile
        if (message.data.byteLength > MAX_CHUNK_RECORD_BYTES) {
          throw new MeshStateNatsLimitError(`chunk ${index} of snapshot ${record.snapshot} is ${message.data.byteLength} bytes before decoding (limit ${MAX_CHUNK_RECORD_BYTES})`);
        }
        const chunk = decode(message.data);
        if (chunk.type !== "snapshot-chunk" || chunk.snapshot !== record.snapshot || chunk.index !== index || typeof chunk.part !== "string") {
          throw new MeshStateNatsSnapshotIntegrityError(`stream message ${message.seq} is not chunk ${index} of snapshot ${record.snapshot}`);
        }
        if (chunk.part.length > SNAPSHOT_CHUNK) throw new MeshStateNatsLimitError(`chunk ${index} of snapshot ${record.snapshot} is ${chunk.part.length} characters (limit ${SNAPSHOT_CHUNK})`);
        bytes += Buffer.byteLength(chunk.part, "utf8");
        if (bytes > header.bytes) throw new MeshStateNatsSnapshotIntegrityError(`snapshot ${record.snapshot} is longer than its header's ${header.bytes} bytes`);
        parts.push(chunk.part);
      }
      if (parts.length !== record.chunks) return "gone";
      const body = parts.join("");
      // The digest binds the whole header (base, end sequence, chunks, prev, lifecycle, size) to the body: nothing in
      // the end record is trusted before this verifies.
      if (bytes !== header.bytes || expectedDigest(record, end.seq, body) !== header.digest) {
        throw new MeshStateNatsSnapshotIntegrityError(`snapshot ${record.snapshot} does not match its digest (a chunk is altered, missing or foreign)`);
      }
      let model: NatsSnapshot;
      try { model = parseModel(body, record.base); }
      catch (error) { throw new MeshStateNatsSnapshotIntegrityError(`snapshot ${record.snapshot} is not a model: ${(error as Error).message}`); }
      // The lifecycle and the size it commits to must be the model's own (and the body re-serializes to itself).
      if (model.status !== header.status || model.epoch !== header.epoch || model.entries.size !== header.entries ||
          serializeModelBounded(model, header.bytes) !== body) {
        throw new MeshStateNatsSnapshotIntegrityError(`snapshot ${record.snapshot} does not match its header (status, epoch or entries)`);
      }
      // The model stands at `base`, BEFORE the end record: the consumer starts after `base` and applies the chunks and
      // the end record, which re-checks the digest and then sets `snapshotDigest` (so the chain carries `prev` here).
      return { ...model, snapshotDigest: header.prev };
    }
  }

  /**
   * A delivery skipped sequences: a purge removed messages this projector had not applied yet (it lagged behind a
   * snapshot, or opened before one). Reload from the latest snapshot and continue after it. Reads meanwhile serve
   * the old, consistent model; `fresh` reads are busy.
   */
  #resync(): void {
    if (this.#closed) return;
    // P1-3: a gap during a running re-sync (a second purge) is recorded, never dropped.
    if (this.#resyncing) { this.#gapPending = true; return; }
    this.#resyncing = true;
    void (async () => {
      for (let attempt = 1; attempt <= RESYNC_ATTEMPTS; attempt++) {
        this.#resyncs++;
        this.#gapPending = false;
        this.#generation++;
        const consumer = this.#consumer;
        this.#consumer = undefined;
        await consumer?.stop();
        await this.#startProjector();
        // A purge between the snapshot load and the consumer start leaves the model below the stream's first message.
        const first = await this.#options.port.firstSequence(this.#options.stream);
        if (!this.#gapPending && first <= this.#snapshot.seq + 1) return;
      }
      throw new MeshStateNatsUnavailableError(`the stream kept being purged past this projector (${RESYNC_ATTEMPTS} re-syncs); reopen the backend`);
    })().catch(error => this.#fail(error)).finally(() => { this.#resyncing = false; });
  }

  /**
   * Publishes a snapshot of the model, then purges the stream below it. The chunks and the end record are one CAS
   * chain right after the model's sequence: a commit by any writer in between breaks it, and the snapshot is abandoned
   * (false; its stored chunks are inert, no end record names them). One at a time per backend.
   * ponytail: a writer that commits continuously can starve snapshots; the next due write tries again.
   */
  compact(): Promise<boolean> {
    this.#compacting ??= this.#compact().finally(() => { this.#compacting = undefined; });
    return this.#compacting;
  }

  async #compact(): Promise<boolean> {
    const model = this.#readable();
    if (model.status !== "ready" || model.seq === 0) return false;
    const { port, stream, snapshotSubject } = this.#options;
    // Never publish a snapshot no host could load (P1-2): an over-bound state is refused while it is serialized,
    // before its whole string exists (round 2).
    if (model.entries.size > this.#limits.maxEntries) return false;
    const body = serializeModelBounded(model, this.#limits.maxBytes);
    if (body === undefined) return false;
    const parts: string[] = [];
    // pi-fabric#907 round 5: never split a surrogate pair. A boundary right after a high surrogate moves back one code
    // unit, so every chunk is well-formed text and the per-chunk UTF-8 byte counts sum to the body's.
    for (let offset = 0; offset < body.length;) {
      let end = Math.min(offset + SNAPSHOT_CHUNK, body.length);
      const last = body.charCodeAt(end - 1);
      if (end < body.length && last >= 0xd800 && last <= 0xdbff) end -= 1;
      parts.push(body.slice(offset, end));
      offset = end;
    }
    const snapshot = randomUUID();
    const header = headerOf(model, body, snapshot, parts.length);
    const records: NatsRecord[] = [
      ...parts.map((part, index): NatsRecord => ({ format: 1, type: "snapshot-chunk", snapshot, index, part })),
      { format: 1, type: "snapshot-end", snapshot, base: model.seq, first: model.seq + 1, chunks: parts.length, header },
    ];
    // The chunks go on the batch subject: only end records are on the snapshot subject, so its last message is always
    // the latest complete snapshot, never a chunk of an abandoned one.
    for (const [index, record] of records.entries()) {
      const subject = record.type === "snapshot-end" ? snapshotSubject : this.#options.subject;
      try { await port.publish(subject, encode(record), { expectLastSequence: model.seq + index, msgID: `${snapshot}:${index}` }); }
      catch { return false; } // another commit came first, or no answer: abandon (nothing names these chunks)
    }
    // Verify what the stream now STORES before anything is purged (pi-fabric#907 round 2): re-read the end record and
    // its chunks and check the digest, the chain position and the model. A corrupt publish leaves the stream unpurged,
    // so the history it would have replaced stays.
    const endSeq = model.seq + parts.length + 1;
    try {
      const stored = await port.getMessage(stream, endSeq);
      const verified = stored ? await this.#verifiedSnapshot(stored) : "gone";
      if (verified === "gone" || verified.seq !== model.seq || (decode(stored!.data) as { header?: SnapshotHeader }).header?.digest !== header.digest ||
          header.prev !== model.snapshotDigest) return false;
    } catch { return false; }
    await port.purge(stream, model.seq + 1);
    this.#snapshots++;
    return true;
  }

  /** After a write: take a snapshot when enough messages follow the last one (in the background, never failing the write). */
  #maybeCompact(): void {
    const every = this.#options.snapshotEvery ?? DEFAULT_SNAPSHOT_EVERY;
    if (every <= 0 || this.#snapshot.seq - this.#snapshot.snapshotEnd < every) return;
    void this.compact().catch(() => undefined);
  }

  #settle(): void {
    const ready = this.#waiters.filter(waiter => waiter.seq <= this.#snapshot.seq);
    this.#waiters = this.#waiters.filter(waiter => waiter.seq > this.#snapshot.seq);
    for (const waiter of ready) waiter.resolve();
  }

  /** The projector stopped (a protocol violation, a broken delivery): every later read and write fails closed. */
  #fail(error: unknown): void {
    if (this.#failure !== undefined || this.#closed) return;
    this.#failure = redactNatsError(error);
    for (const waiter of this.#waiters.splice(0)) waiter.reject(this.#failure);
  }

  #apply(message: NatsStreamMessage): void {
    if (this.#failure !== undefined) return;
    this.#knownSeq = Math.max(this.#knownSeq, message.seq);
    // Stream sequences are contiguous until a purge: a gap means a snapshot purged what this model still needed.
    if (message.seq > this.#snapshot.seq + 1) { this.#resync(); return; }
    try { this.#snapshot = applyNatsMessage(this.#snapshot, message, this.#options.maxStateTombstones ?? DEFAULT_TOMBSTONES, this.#limits); }
    catch (error) { this.#fail(error); return; }
    this.#settle();
  }

  /** Resolves once the model has applied `seq`; a projector that does not get there within the write budget is busy. */
  #appliedThrough(seq: number, budgetMs = this.#options.lockTimeoutMs ?? 5_000): Promise<void> {
    if (this.#failure !== undefined) return Promise.reject(this.#failure);
    if (this.#snapshot.seq >= seq) return Promise.resolve();
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.#waiters = this.#waiters.filter(waiter => waiter !== entry);
        reject(new MeshStateNatsBusyError(`the projector did not reach sequence ${seq} within ${budgetMs} ms (at ${this.#snapshot.seq})`));
      }, budgetMs);
      timer.unref?.();
      const entry = { seq, resolve: () => { clearTimeout(timer); resolve(); }, reject: (error: unknown) => { clearTimeout(timer); reject(error); } };
      this.#waiters.push(entry);
    });
  }

  #readable(options: MeshReadOptions = {}): NatsSnapshot {
    if (this.#closed) throw new Error("Fabric mesh NATS state backend is closed");
    if (this.#failure !== undefined) throw this.#failure;
    const snapshot = (options.snapshot as NatsSnapshot | undefined) ?? this.#snapshot;
    if (snapshot.status === "retired") throw new MeshStateNatsRefusedError("the stream is retired (rolled back); reopen on the active backend");
    if (options.fresh === true && this.#knownSeq > this.#snapshot.seq) {
      throw new MeshStateNatsBusyError(`the projector is at ${this.#snapshot.seq} of ${this.#knownSeq}`);
    }
    return snapshot;
  }

  get(key: string, options?: MeshReadOptions): MeshStateEntry | undefined {
    validateMeshStateKey(key);
    const entry = this.#readable(options).entries.get(key);
    return entry ? jsonClone(entry) : undefined;
  }
  list(prefix = "", limit?: number, options?: MeshReadOptions): MeshStateEntry[] {
    const entries = this.listAll(prefix, options);
    return limit === undefined ? entries : entries.slice(0, Math.max(0, limit));
  }
  listAll(prefix = "", options?: MeshReadOptions): MeshStateEntry[] {
    return this.listAllShared(prefix, options).map(entry => jsonClone(entry as MeshStateEntry));
  }
  listAllShared(prefix = "", options?: MeshReadOptions): readonly Readonly<MeshStateEntry>[] {
    if (prefix) validateMeshStateKey(prefix);
    const snapshot = this.#readable(options);
    return sortedKeys([...snapshot.entries.keys()].filter(key => key.startsWith(prefix))).map(key => snapshot.entries.get(key)!);
  }
  stateToken(options?: MeshReadOptions): object { return this.#readable(options); }
  stateStamp(): string | undefined { return `nats:${this.#streamId}:${this.#snapshot.seq}`; }
  cachedStateStamp(): string | undefined { return this.stateStamp(); }

  async put(input: StateBackendPutInput): Promise<MeshStateEntry> {
    await singleWriteContract(this.writeBatch({ identity: input.identity, ops: [{ kind: "put", key: input.key, value: input.value,
      ...(input.ifVersion === undefined ? {} : { ifVersion: input.ifVersion }) }] }));
    const entry = this.get(input.key);
    if (!entry) throw new Error(`Fabric mesh NATS put of ${input.key} did not apply`);
    return entry;
  }

  async delete(input: StateBackendDeleteInput): Promise<{ deleted: boolean; version?: number }> {
    const [result] = await singleWriteContract(this.writeBatch({ identity: { id: "fabric", name: "fabric", kind: "agent" },
      ops: [{ kind: "delete", key: input.key, ...(input.ifVersion === undefined ? {} : { ifVersion: input.ifVersion }) }] }));
    return result?.applied ? { deleted: true, version: result.version } : { deleted: false };
  }

  /**
   * The R11 loop of the design: `prepare` runs on the model at sequence S, the batch is published with
   * `expect.lastSequence: S`; another writer's commit fails it (wrong last sequence), the model catches up and the
   * batch runs again, within `lockTimeoutMs`. The promise resolves once this host's model has applied the commit.
   */
  async writeBatch(input: StateBackendBatchInput): Promise<MeshBatchResult[]> {
    const deadline = performance.now() + (this.#options.lockTimeoutMs ?? 5_000);
    for (let attempt = 1; ; attempt++) {
      let fileRead: { value: unknown; stamp: string | undefined } | undefined;
      if (input.fileRead) {
        fileRead = bracketFileRead(input.fileRead);
        if (!fileRead) { if (attempt > 8) throw new MeshStateFileReadChangedError(attempt); continue; }
      }
      const snapshot = this.#readable();
      if (snapshot.status !== "ready") throw new MeshStateNatsRefusedError(`the stream is ${snapshot.status}; writes refuse`);
      if (input.fileRead && input.fileRead.stamp() !== fileRead!.stamp) { if (attempt > 8) throw new MeshStateFileReadChangedError(attempt); continue; }
      const planned = this.#plan(snapshot, input, fileRead?.value);
      if (planned.changes.length === 0) {
        input.beforeCommit?.();
        input.afterCommit?.(viewOf(snapshot));
        this.#outbox(input, planned.results, [], snapshot);
        return planned.results;
      }
      input.beforeCommit?.();
      const id = randomUUID();
      const record: NatsRecord = { format: 1, type: "batch", id, base: snapshot.seq, highWater: planned.highWater, changes: planned.changes };
      const data = encode(record);
      if (data.byteLength > this.#options.maxEventBytes * Math.max(1, planned.changes.length) + 64 * 1024) {
        throw new Error("Fabric mesh NATS batch exceeds its size bound");
      }
      if (data.byteLength > this.#limits.maxMessageBytes) {
        throw new MeshStateNatsLimitError(`a batch of ${data.byteLength} bytes is over the stream's message limit (${this.#limits.maxMessageBytes})`);
      }
      let seq: number;
      try {
        seq = await this.#publish(data, snapshot.seq, id, deadline);
      } catch (error) {
        if (!(error instanceof NatsWrongLastSequenceError)) throw error;
        // The server checks the expected sequence before the duplicate window, so a retry of MY stored batch (its ack
        // was lost) also fails this way. The CAS stored it, if at all, at exactly base + 1: read that one message.
        const committed = error.afterLostAck ? await this.#committedAt(snapshot.seq + 1, id) : undefined;
        const last = await this.#options.port.lastSequence(this.#options.stream);
        this.#knownSeq = Math.max(this.#knownSeq, last);
        await this.#appliedThrough(last);
        if (committed === undefined) {
          // Another writer won: run the batch again on the new model.
          this.#casRetries++;
          if (performance.now() >= deadline) throw new MeshStateNatsBusyError(`another writer kept committing (${attempt} attempts)`);
          continue;
        }
        seq = committed;
      }
      this.#knownSeq = Math.max(this.#knownSeq, seq);
      await this.#appliedThrough(seq);
      const after = this.#snapshot;
      input.afterCommit?.(viewOf(after));
      this.#outbox(input, planned.results, planned.changes.map(change => change.key), after);
      this.#maybeCompact();
      return planned.results;
    }
  }

  /**
   * One publish of one batch. A lost ack (a timeout, a dropped connection) is retried with the SAME message id and
   * expected sequence. If the first copy was stored, the stream moved past `base` and the retry fails the expected
   * sequence: the caller then finds the batch in the stream by its id (never commits it twice). If it was not stored
   * and nothing else was, the retry stores it (or the duplicate window returns it). Past the deadline with no answer,
   * the outcome is unknown: that is not a retryable busy error, the caller must re-read.
   */
  async #publish(data: Uint8Array, base: number, msgID: string, deadline: number): Promise<number> {
    let lostAck = false;
    for (;;) {
      try { return (await this.#options.port.publish(this.#options.subject, data, { expectLastSequence: base, msgID })).seq; }
      catch (error) {
        if (error instanceof NatsWrongLastSequenceError) { error.afterLostAck = lostAck; throw error; }
        lostAck = true;
        if (this.#closed || (error as { transient?: unknown })?.transient !== true) throw error;
        if (performance.now() >= deadline) throw new MeshStateNatsOutcomeUnknownError(msgID, (error as Error).message);
      }
    }
  }

  /**
   * Did batch `id` commit at `seq`? Exact, from the stream itself (no bounded memory of ids): the message at `seq` is
   * that batch (its sequence) or not (undefined). A message that is gone cannot tell: the outcome is unknown.
   */
  async #committedAt(seq: number, id: string): Promise<number | undefined> {
    const message = await this.#options.port.getMessage(this.#options.stream, seq);
    if (!message) throw new MeshStateNatsOutcomeUnknownError(id, `stream message ${seq} is gone`);
    const record = decode(message.data);
    return record.type === "batch" && record.id === id ? seq : undefined;
  }

  #outbox(input: StateBackendBatchInput, results: MeshBatchResult[], changed: string[], snapshot: NatsSnapshot): void {
    if (!input.commitOutbox) return;
    const effects: MeshCommitEffects = { backend: "nats", results: results.map(result => ({ ...result })), changed,
      stamp: `nats:${this.#streamId}:${snapshot.seq}`, view: viewOf(snapshot) } as MeshCommitEffects;
    input.commitOutbox(effects);
  }

  /** The batch's verified transitions on the snapshot, exactly as the file and sqlite backends apply them. */
  #plan(snapshot: NatsSnapshot, input: StateBackendBatchInput, fileValue: unknown): { changes: NatsChange[]; results: MeshBatchResult[]; highWater: number } {
    const entries = new Map(snapshot.entries);
    const versions = new Map(snapshot.versions);
    let highWater = snapshot.highWater;
    const view: MeshBatchView = {
      get: key => { const entry = entries.get(key); return entry ? jsonClone(entry) : undefined; },
      listAll: prefix => sortedKeys([...entries.keys()].filter(key => key.startsWith(prefix))).map(key => jsonClone(entries.get(key)!)),
      version: key => versions.get(key) ?? 0,
    };
    const ops = [...input.ops, ...(input.prepare?.(view, fileValue) ?? [])];
    for (const op of ops) validateMeshStateKey(op.key);
    const results: MeshBatchResult[] = [];
    const changes = new Map<string, NatsChange>();
    const now = Date.now();
    for (const op of ops) {
      const present = entries.has(op.key);
      const version = versions.get(op.key) ?? 0;
      if (op.kind === "delete" && op.condition && !op.condition(view.get)) { results.push({ key: op.key, applied: false, version }); continue; }
      if (op.ifVersion !== undefined && op.ifVersion !== version) {
        const policy = typeof op.onConflict === "function" ? op.onConflict(view.get(op.key)) : op.onConflict ?? "abort";
        if (policy === "abort") throw new MeshBatchConflictError(op.key, op.ifVersion, version);
        results.push({ key: op.key, applied: false, version });
        continue;
      }
      const request = op.kind === "delete"
        ? captureStorageDelete({ key: op.key, ifVersion: op.ifVersion })
        : captureStoragePut({ key: op.key, ifVersion: op.ifVersion,
          value: typeof op.value === "function" ? (op.value as (now: number) => unknown)(now) : op.value,
          identity: op.identity ?? input.identity }, this.#options.maxEventBytes);
      const plan = request.transition(present, version, highWater);
      if (plan.kind === "unchanged") { results.push({ key: op.key, applied: false, version }); continue; }
      if (plan.kind === "delete") {
        entries.delete(plan.key);
        changes.set(plan.key, { key: plan.key, deleted: { version: plan.version } });
      } else {
        const entry = { key: plan.key, value: plan.value, version: plan.version, updatedAt: now, updatedBy: plan.identity };
        entries.set(plan.key, entry);
        changes.set(plan.key, { key: plan.key, put: { value: plan.value, version: plan.version, updatedAt: now, updatedBy: plan.identity } });
      }
      versions.set(plan.key, plan.version);
      highWater = plan.highWater;
      results.push({ key: op.key, applied: true, version: plan.version });
    }
    return { changes: [...changes.values()], results, highWater };
  }

  async confirmWritable(onAcquired?: (at: number) => void): Promise<void> {
    const snapshot = this.#readable();
    if (snapshot.status !== "ready") throw new MeshStateNatsRefusedError(`the stream is ${snapshot.status}; writes refuse`);
    this.#knownSeq = Math.max(this.#knownSeq, await this.#options.port.lastSequence(this.#options.stream));
    onAcquired?.(Date.now());
  }

  /**
   * ponytail (phase 1, single host): runs `operation` on the model as applied now. Unlike file and sqlite, a
   * commit by another writer cannot be held off synchronously (the stream is the authority, and Node cannot block
   * on it), so the fence is exactly as strong as the caller's own `.lock` plus the stream CAS of its next publish.
   * Phase 2 (smarty-dev#7504) decides the cross-writer fence (the event log on the same stream).
   */
  withWriteFence<T>(operation: () => T): T {
    this.#readable();
    return operation();
  }

  dropCache(): void { /* the model is the stream's projection: nothing to drop */ }

  diagnostics(): StateBackendDiagnostics {
    return { kind: "nats", nats: { stream: this.#options.stream, appliedSeq: this.#snapshot.seq, knownSeq: this.#knownSeq,
      status: this.#snapshot.status, casRetries: this.#casRetries, resyncing: this.#resyncing, snapshotSeq: this.#snapshot.snapshotEnd, snapshots: this.#snapshots,
      resyncs: this.#resyncs, ...(this.#failure ? { failure: redactNatsUrls(String(this.#failure)) } : {}) } } as StateBackendDiagnostics;
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#generation++;
    const consumer = this.#consumer;
    for (const waiter of this.#waiters.splice(0)) waiter.reject(new Error("Fabric mesh NATS state backend is closed"));
    void (async () => { await consumer?.stop(); await this.#options.port.close(); })().catch(() => undefined);
  }
}

// ------------------------------------------------------------------ the prepared-backend registry and the refusals

const prepared = new Map<string, NatsStateBackend>();

/** The prepared backend for a mesh root, or undefined. Read by the selector's synchronous `nats` factory. */
export const preparedNatsStateBackend = (root: string): NatsStateBackend | undefined => prepared.get(path.resolve(root));

/** Minimum server: 2.14.7, as smarty-dev#7504 requires (per-message TTL and delete markers since 2.11; the KV leases of phase 2). */
export const MIN_SERVER = [2, 14, 7];
// Strict semver (pi-fabric#937 round 1): a prerelease (2.14.7-rc.1) is below its release; an unparseable version refuses.
const versionAtLeast = (version: string, minimum: number[]): boolean => {
  const have = semverValid(version.replace(/^v/, ""));
  return have !== null && semverGte(have, minimum.join("."));
};

export interface OpenNatsStateBackendOptions {
  /** The mesh root this backend serves. Its stream is always derived from it (natsStreamName): no override, so two
   * roots never share one stream (smarty-dev#9663 item 3). */
  root: string;
  /** NATS server URLs (the real client); ignored when `port` is given (tests). */
  servers?: string[];
  /** Host-only: the server's monitoring endpoint (`http://host:8222`), read for `/varz` sync_always. */
  monitoringUrl?: string;
  port?: JetStreamPort;
  /** Tests: the resolver for the loopback check (default dns.promises.lookup). */
  lookup?: NatsLookup;
  maxEventBytes?: number;
  maxStateTombstones?: number;
  lockTimeoutMs?: number;
  /** Messages between snapshots (each followed by a purge below it); 0 never. Default 10 000. */
  snapshotEvery?: number;
  /** The state bound (default 32 MiB, as the file backend): bounds a snapshot's bytes, entries and chunks. */
  maxStateBytes?: number;
  /** At most this many messages replayed at open after the snapshot. Default 100 000. */
  maxReplay?: number;
  /** Host-only: the operator accepts a server that does not fsync every write (`sync_always: false`). */
  acceptPeriodicSync?: boolean;
  /** Host-only: the client's TLS files (PEM paths), for a `tls://` server. */
  tls?: NatsTlsFiles;
}

export interface NatsTlsFiles { caFile?: string; certFile?: string; keyFile?: string }

/** A non-loopback server or monitoring URL without TLS: state (and the sync-policy check) would cross the network in clear. */
export class MeshStateNatsInsecureError extends Error {
  readonly code = "FABRIC_MESH_STATE_NATS_INSECURE";
  constructor(url: string, detail = "is not loopback and not TLS") {
    const shown = redactNatsUrl(url);
    super(`FABRIC_MESH_STATE_NATS_INSECURE: ${shown} ${detail}; use tls:// (servers) or https:// (monitoring)`);
    this.url = shown;
    this.name = "MeshStateNatsInsecureError";
  }
  readonly url: string;
}

const withScheme = (url: string): string => /^[a-z][a-z0-9+.-]*:\/\//i.test(url) ? url : `nats://${url}`;

/**
 * A URL fit for an error or a log line (smarty-dev#9663 item 2): its userinfo and every query value become `***`.
 * Unparseable input keeps only what a credential cannot hide in.
 */
export const redactNatsUrl = (url: string): string => {
  try {
    const parsed = new URL(withScheme(url));
    if (parsed.username || parsed.password) { parsed.username = "***"; parsed.password = ""; }
    for (const key of [...parsed.searchParams.keys()]) parsed.searchParams.set(key, "***");
    if (parsed.hash) parsed.hash = "***"; // pi-fabric#937 round 1: a fragment can carry a token too
    const shown = parsed.toString();
    return withScheme(url) === url ? shown : shown.replace(/^nats:\/\//, "");
  } catch {
    // Unparseable: nothing after the scheme is shown (any part of it may be a credential).
    return `${/^[a-z][a-z0-9+.-]*:\/\//i.exec(url)?.[0] ?? ""}*** (unparseable URL)`;
  }
};

/**
 * Bare credential text (pi-fabric#937 round 2): a .creds block (`-----BEGIN NATS USER JWT-----` ... `-----END ...`,
 * or to the end when cut off), a `key=value` / `key: value` / `"key":"value"` for token, auth_token, pass(word),
 * nkey, jwt, seed and credential(s), an nkey seed or public key (base32, `SU...`, `UA...`), and a JWT (`eyJ...`).
 */
export const redactSecrets = (text: string): string => text
  .replace(/-----BEGIN [A-Z0-9 ]+-----[\s\S]*?(?:-----END [A-Z0-9 ]+-----|$)/g, "-----*** (credentials block)-----")
  .replace(/(["']?)\b(auth_token|token|password|pass|nkey|jwt|seed|credentials?)\1(\s*[=:]\s*)("[^"]*"|'[^']*'|[^\s&,;"'}\]]+)/gi, "$1$2$1$3***")
  .replace(/\bS[UANOCXP][A-Z2-7]{50,}|\b[UANOCXP][A-Z2-7]{50,}/g, "***")
  .replace(/\beyJ[A-Za-z0-9_\-.=]{8,}/g, "***");

/** Every URL inside a message (a client or fetch error), redacted as redactNatsUrl does, then redactSecrets. */
export const redactNatsUrls = (text: string): string => redactSecrets(text
  .replace(/[a-z][a-z0-9+.-]*:\/\/[^\s'"<>]+/gi, url => redactNatsUrl(url)).replace(/\b[^\s/@:'"]+:[^\s/@'"]+@/g, "***@"));

/**
 * A NEW error fit to leave the NATS client (pi-fabric#937 round 2). It keeps the prototype (so `instanceof`, e.g.
 * NatsWrongLastSequenceError, still works), a redacted name, message, stack and `code`, the number and boolean own
 * fields (`transient`, `expected`, `afterLostAck`: no text), a sanitized `cause` and AggregateError `errors` (depth
 * cap 4), and a sanitized `apiError()` result. Every other field is dropped. A non-Error throw becomes an Error with
 * its redacted String(). Total: no coercion can throw out of it (a hostile toString, getter or proxy gives
 * "[unprintable error]").
 */
export const redactNatsError = (error: unknown, depth = 0): Error => {
  try { return sanitizeNatsError(error, depth); }
  catch { return new Error("[unprintable error]"); }
};

/** Redacted text of any value, never throwing: JSON for a plain object, else String(), else "[unprintable error]". */
const printable = (value: unknown): string => {
  try {
    if (typeof value === "object" && value !== null && !(value instanceof Error)) {
      try { const json = JSON.stringify(value); if (json !== undefined) return redactNatsUrls(json); } catch { /* String() next */ }
    }
    return redactNatsUrls(String(value));
  } catch { return "[unprintable error]"; }
};

const sanitizeNatsError = (error: unknown, depth: number): Error => {
  if (!(error instanceof Error)) return new Error(printable(error));
  const read = (key: string): unknown => { try { return (error as unknown as Record<string, unknown>)[key]; } catch { return undefined; } };
  const safe = Object.create(Object.getPrototypeOf(error) as object) as Error & Record<string, unknown>;
  const define = (key: string, value: unknown) => Object.defineProperty(safe, key, { value, writable: true, configurable: true, enumerable: false });
  define("name", printable(read("name") ?? "Error"));
  define("message", printable(read("message") ?? ""));
  define("stack", printable(read("stack") ?? ""));
  for (const key of Object.keys(error)) {
    const value = read(key);
    if (typeof value === "number" || typeof value === "boolean") safe[key] = value;
  }
  const code = read("code");
  if (code !== undefined) safe.code = typeof code === "number" ? code : printable(code);
  if ("cause" in error && read("cause") !== undefined) define("cause", depth < 4 ? redactNatsError(read("cause"), depth + 1) : undefined);
  const errors = read("errors");
  if (Array.isArray(errors)) define("errors", depth < 4 ? errors.map(inner => redactNatsError(inner, depth + 1)) : []);
  const apiError = read("apiError");
  if (typeof apiError === "function") {
    let kept: { code: number; err_code: number; description: string } | undefined;
    try {
      const api = (apiError as () => { code?: unknown; err_code?: unknown; description?: unknown } | undefined).call(error);
      if (api !== undefined && api !== null) {
        kept = { code: typeof api.code === "number" ? api.code : NaN, err_code: typeof api.err_code === "number" ? api.err_code : NaN,
          description: printable(api.description ?? "") };
      }
    } catch { kept = undefined; }
    define("apiError", () => kept);
  }
  return safe;
};

/**
 * The one choke point of the real client (pi-fabric#937 round 1): every port method's thrown error, the errors
 * `consume` hands to `onError`, and the `stop` it returns pass through redactNatsError.
 */
export const guardNatsPort = (port: JetStreamPort): JetStreamPort => {
  const guard = <A extends unknown[], R>(method: (...args: A) => Promise<R>) => async (...args: A): Promise<R> => {
    let result: R;
    try { result = await method(...args); } catch (error) { throw redactNatsError(error); }
    const stop = (result as { stop?: unknown } | undefined)?.stop;
    return typeof stop === "function" ? { ...result, stop: guard(stop as () => Promise<unknown>) } as R : result;
  };
  const guarded = Object.fromEntries(Object.entries(port).map(([key, method]) =>
    [key, typeof method === "function" ? guard(method as (...args: unknown[]) => Promise<unknown>) : method])) as unknown as JetStreamPort;
  const consume = guarded.consume;
  guarded.consume = (stream, afterSeq, onMessage, onError) =>
    consume(stream, afterSeq, onMessage, onError && (error => onError(redactNatsError(error))));
  return guarded;
};

export type NatsLookup = (host: string) => Promise<{ address: string; family: number }[]>;
const systemLookup: NatsLookup = host => dns.promises.lookup(host, { all: true, verbatim: true });

const loopbackAddress = (address: string): boolean => {
  const bare = address.replace(/^\[|\]$/g, "").toLowerCase();
  if (net.isIPv4(bare)) return bare.split(".")[0] === "127";
  if (!net.isIPv6(bare)) return false;
  if (bare === "::1") return true;
  const mapped = /^::ffff:(\d+\.\d+\.\d+\.\d+)$/.exec(bare);
  return mapped !== null && mapped[1]!.split(".")[0] === "127";
};

/**
 * Refuses a plaintext URL to a non-loopback host, before any connection, and returns the URL to connect to. A TLS URL
 * is returned as given (its name is checked by the certificate). A plaintext URL passes only when its host is a
 * loopback address (127.0.0.0/8, ::1) or a name whose EVERY resolved address is one (smarty-dev#9663 item 1: never
 * by the name `localhost`; an unresolvable name refuses). The returned plaintext URL names the resolved address, so
 * a later re-resolution cannot move the connection off the host. A server without a scheme is `nats://`.
 */
export const assertNatsTransportSecure = async (url: string, secureScheme: "tls:" | "https:",
  lookup: NatsLookup = systemLookup): Promise<string> => {
  let parsed: URL;
  try { parsed = new URL(withScheme(url)); }
  catch { throw new MeshStateNatsInsecureError(url); }
  if (parsed.protocol === secureScheme) return url;
  const host = parsed.hostname.replace(/^\[|\]$/g, "");
  if (net.isIP(host)) {
    if (!loopbackAddress(host)) throw new MeshStateNatsInsecureError(url);
    return url;
  }
  let addresses: { address: string }[];
  try { addresses = await lookup(host); }
  catch { throw new MeshStateNatsInsecureError(url, "does not resolve, so it cannot be shown to be loopback"); }
  if (addresses.length === 0 || !addresses.every(entry => loopbackAddress(entry.address))) {
    throw new MeshStateNatsInsecureError(url, "does not resolve only to loopback addresses and is not TLS");
  }
  const pinned = addresses[0]!.address;
  parsed.hostname = net.isIPv6(pinned) ? `[${pinned}]` : pinned;
  return parsed.toString();
};

/** A URL whose host is a loopback IP literal (no lookup). */
const loopbackUrl = (url: string): boolean => {
  try { const host = new URL(withScheme(url)).hostname.replace(/^\[|\]$/g, ""); return net.isIP(host) !== 0 && loopbackAddress(host); }
  catch { return false; }
};

/** The c03c829f stream name (before the hashed one): an existing stream under it is never replaced silently. */
const legacyNatsStreamName = (root: string): string =>
  `FABRIC_MESH_${Buffer.from(path.resolve(root)).toString("base64url").replace(/[^A-Za-z0-9_]/g, "_").slice(-48)}`;

/**
 * The stream name for a mesh root: a fixed prefix and 32 hex characters (128 bits) of the SHA-256 of the full
 * canonical root (its realpath when it exists), so distinct roots never share a stream.
 */
export const natsStreamName = (root: string): string => {
  let canonical = path.resolve(root);
  try { canonical = fs.realpathSync.native(canonical); } catch { /* not created yet: the resolved path */ }
  return `FABRIC_MESH_${createHash("sha256").update(canonical, "utf8").digest("hex").slice(0, 32)}`;
};

/**
 * Opens (and registers for the selector) the NATS state backend of a mesh root. Refuses, never falls back:
 * a server older than MIN_SERVER (2.14.7); a server that does not fsync every write unless the operator accepts it; a
 * stream that is retired; a root whose state is still in a stream under the old (c03c829f) name. Loads the official client only here.
 */
export const openNatsStateBackend = async (options: OpenNatsStateBackendOptions): Promise<NatsStateBackend> => {
  const root = path.resolve(options.root);
  const lookup = options.lookup ?? systemLookup;
  const servers: string[] = [];
  for (const server of options.servers ?? []) servers.push(await assertNatsTransportSecure(server, "tls:", lookup));
  const monitoringUrl = options.monitoringUrl === undefined ? undefined
    : await assertNatsTransportSecure(options.monitoringUrl, "https:", lookup);
  // A tls:// server makes the client REQUIRE TLS on every connection (a server not offering it fails the connect),
  // whatever the client does with the scheme itself.
  const tls = options.tls ?? (servers.some(server => /^tls:\/\//i.test(server)) ? {} : undefined);
  const port = options.port ?? await (await import("./state-backend-nats-client.js")).connectJetStream(servers, {
    ...(monitoringUrl === undefined ? {} : { monitoringUrl }),
    ...(tls === undefined ? {} : { tls }) });
  try {
    const server = await port.server();
    if (!versionAtLeast(server.version, MIN_SERVER)) throw new MeshStateNatsRefusedError(`nats-server ${server.version} is older than ${MIN_SERVER.join(".")}`);
    if (server.syncAlways !== true && options.acceptPeriodicSync !== true) {
      throw new MeshStateNatsRefusedError(`nats-server does not report sync_always (got ${String(server.syncAlways)}): an acked write may not survive a power loss; the operator may accept it explicitly`);
    }
    // pi-fabric#907: the server's own payload cap must not exceed ours (it then refuses oversized messages itself).
    if (server.maxPayload !== undefined && server.maxPayload > MAX_MESSAGE_BYTES) {
      throw new MeshStateNatsRefusedError(`nats-server max_payload is ${server.maxPayload} bytes (limit ${MAX_MESSAGE_BYTES}); set max_payload (docs/mesh-backends.md)`);
    }
    // What a client CAN see of the server's access control is whether it requires authentication at all (INFO
    // auth_required). A non-loopback server that does not lets anyone on the network publish to or purge the stream:
    // refuse. Per-user publish/purge permissions are not visible to a client; docs/mesh-backends.md states the
    // required server configuration.
    // Decided once, before the connection, never by a second lookup (pi-fabric#937 round 2: DNS rebinding). A
    // plaintext URL is already pinned to the loopback address its one resolution gave; anything else whose host is not
    // a loopback IP literal (a TLS hostname, even `localhost`) is remote. ponytail: local TLS without auth names
    // tls://127.0.0.1, not a hostname.
    const remote = servers.some(url => !loopbackUrl(url));
    if (remote && server.authRequired !== true) {
      throw new MeshStateNatsRefusedError("a non-loopback nats-server that does not require authentication: anyone who can reach it could write or purge the state stream (docs/mesh-backends.md)");
    }
    const stream = natsStreamName(root);
    // smarty-dev#9663 item 4: a stream under the c03c829f name holds this root's state; the hashed stream would
    // silently start empty beside it. Refuse until the operator migrates it.
    const legacy = legacyNatsStreamName(root);
    if (legacy !== stream && await port.streamExists(legacy)) {
      throw new MeshStateNatsRefusedError(`the stream ${legacy} holds this root's state under the old (c03c829f) name; ` +
        `migrate it to ${stream} (or remove it after an export), then open again: it is never replaced silently`);
    }
    const backend = await NatsStateBackend.open({ port, stream, subject: `${stream}.state`, snapshotSubject: `${stream}.snapshot`,
      maxEventBytes: options.maxEventBytes ?? 1024 * 1024,
      ...(options.snapshotEvery === undefined ? {} : { snapshotEvery: options.snapshotEvery }),
      ...(options.maxStateBytes === undefined ? {} : { maxStateBytes: options.maxStateBytes }),
      ...(options.maxReplay === undefined ? {} : { maxReplay: options.maxReplay }),
      ...(options.maxStateTombstones === undefined ? {} : { maxStateTombstones: options.maxStateTombstones }),
      ...(options.lockTimeoutMs === undefined ? {} : { lockTimeoutMs: options.lockTimeoutMs }) });
    if (backend.diagnostics().nats?.status === "retired") { backend.close(); throw new MeshStateNatsRefusedError(`the stream ${stream} is retired`); }
    prepared.get(root)?.close();
    prepared.set(root, backend);
    return backend;
  } catch (error) {
    if (!options.port) await port.close().catch(() => undefined);
    throw error;
  }
};

/** Forgets a root's prepared backend (closing it). */
export const closeNatsStateBackend = (root: string): void => {
  const resolved = path.resolve(root);
  prepared.get(resolved)?.close();
  prepared.delete(resolved);
};
