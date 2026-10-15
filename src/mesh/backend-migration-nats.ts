/**
 * Mesh state migration file <-> nats (smarty-dev#9670; docs/mesh-backends.md, Migration). The sibling of the
 * file <-> sqlite import and rollback in backend-migration.ts, with the same fence (`custody.lock`, then `.lock`,
 * for the whole section), the same digest (`meshSnapshotDigest`) checked both ways, and the same order: roll forward
 * installs the marker BEFORE the stream says `import-commit`; roll back retires the stream BEFORE the marker goes.
 *
 * Import (a file root: no state.db; a sqlite root rolls back to file first), under the fence:
 *   1. Read state.json (G0: generation and digest). Publish `import-begin{E+1, digest, generation}` to the empty
 *      stream (expected last sequence 0: a second migrator fails). nats writers refuse while it is importing.
 *   2. Publish the snapshot as deterministic batch chunks (entries in key order, then tombstones in eviction
 *      order), each with the expected last sequence and a fixed message id.
 *   3. Project the stream with the backend's own projector and check its digest against G0; re-read state.json
 *      against G0 (a file-mode writer that ignored `.lock` fails the import).
 *   4. Keep state.json at `state.json.nats-cutover-<E+1>`, install the NATS moved marker over state.json, then
 *      publish `import-commit{E+1}`.
 * Rerun after a crash: without the marker, the same source (G0 equals the `import-begin` record) resumes: chunks
 * already stored must match byte for byte, the rest are published; a changed source refuses (a leftover import may be deleted on the server; a stream with committed state never).
 * With the marker, the stream is verified against the kept copy and committed (or found committed).
 *
 * Rollback (nats -> file), under the fence: publish `retire{E+1}` with the expected last sequence (every later nats
 * write fails its CAS and projectors refuse), project the stream at `retire`, export it through the normal
 * state.json encoder into `state.json.nats-rollback-<E+1>-<uuid>.tmp` (fsync, read back, digest), and rename it over the
 * marker. A rerun with a retired stream redoes the export and the rename.
 *
 * The local locks fence this host only; other hosts are fenced by the stream itself (every nats write carries an
 * expected sequence). This module loads the NATS client only when no port is given.
 */
import { randomBytes, randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { renameAtomic } from "../core/atomic-write.js";
import { encodeMeshStateNatsMarker, meshStateMalformedMarkerError, readMeshStateMovedMarker, readMeshStateNatsMarker,
  stateFileIsMalformedNatsMarker } from "./backend-fence.js";
import { MeshBackendRefusedError, meshSnapshotDigest, normalizeSnapshot, type MeshBackendStep, type MeshStateSnapshot } from "./backend-migration.js";
import { holdMeshFence } from "./fence-lock.js";
import { applyNatsMessage, assertNatsMaxPayload, assertNatsServerSupported, connectNatsEndpoint, isNatsStreamForRoot, MAX_MESSAGE_BYTES,
  NATS_SWITCH_ID, NatsStateBackend, natsSnapshotLimits, natsStreamName, natsSwitchStreamName, natsTargetOf, NatsWrongLastSequenceError,
  type JetStreamPort, type NatsLookup, type NatsTlsFiles } from "./state-backend-nats.js";
import { decodeMeshStateFile, encodeMeshStateFile, type MeshStateFile } from "./state-file.js";

const STATE_JSON = "state.json";
const DEFAULT_MAX_STATE_BYTES = 32 * 1024 * 1024;
const DEFAULT_LOCK_MS = 60_000;
/** One chunk's changes stay well under the server's default max_payload (1 MiB). */
const CHUNK_BYTES = 512 * 1024;
/** The backend's own tombstone bound (state-backend-nats.ts DEFAULT_TOMBSTONES): the projection forgets as it does. */
const DEFAULT_MAX_TOMBSTONES = 2_048;
/** The remedy for a partial import left in the root's stream (still `importing`, and no state.json NATS marker, so no
 * Fabric store serves it): abandon it (smarty-dev#9690: deletes exactly this switch's own unfinished stream and intent). */
const LEFTOVER = (stream: string): string => `the stream is a leftover of a failed import (still importing; state.json has no NATS marker). `
  + `Abandon it (import --to nats --abandon deletes stream ${stream} only when it is this switch's unfinished import) and rerun (an import never purges)`;
/** At most this many stream messages are replayed by one projection (a stream that long needs a snapshot first). */
const DEFAULT_MAX_REPLAY = 1_000_000;

/** A bound of the migration was exceeded: nothing was retired, renamed or committed. */
export class MeshNatsMigrationLimitError extends MeshBackendRefusedError {
  readonly limitCode = "FABRIC_MESH_NATS_MIGRATION_LIMIT";
  constructor(detail: string) {
    super(`LIMIT: FABRIC_MESH_NATS_MIGRATION_LIMIT: ${detail}`);
    this.name = "MeshNatsMigrationLimitError";
  }
}

export interface MeshNatsMigrationOptions {
  /** A JetStream port (tests); otherwise `servers` (and `monitoringUrl`, `tls`) connect the official client. */
  port?: JetStreamPort;
  servers?: string[];
  monitoringUrl?: string;
  tls?: NatsTlsFiles;
  /** Tests: the resolver for the loopback check (as openNatsStateBackend). */
  lookup?: NatsLookup;
  /** Host-only: accept a server that does not report `sync_always` (as openNatsStateBackend). */
  acceptPeriodicSync?: boolean;
  /** The state size bound (as both stores). Default 32 MiB. */
  maxStateBytes?: number;
  /** The most stream messages one projection replays. Default 1,000,000. */
  maxReplay?: number;
  /** The tombstone bound (as the nats backend's `maxStateTombstones`). Default 2,048. */
  maxStateTombstones?: number;
  /** The server's `max_payload` (one message). Default 1 MiB, the nats-server default. */
  maxPayloadBytes?: number;
  lockTimeoutMs?: number;
  lockProtocol?: 1 | 2 | 3;
  /** As openNatsStateBackend: create with these replicas; refuse an existing stream with other replicas.
   * Omitted: the real client reads the agent-layer fabric.json mesh.nats.replicas; an injected port decides. */
  replicas?: number;
  /** Real-client placement budget in milliseconds (default 30 000); exhausted: typed unavailable, never fewer replicas. */
  placementTimeoutMs?: number;
  /**
   * Rollback only (smarty-dev#9690): the 32-hex switch id of a per-switch stream, to verify a FINISHED rollback (state.json
   * no longer names the stream). The stream is always `natsStreamName(root)_<switchId>`, root-bound by construction:
   * never an arbitrary stream (pi-fabric#937). Omitted: the root's fixed stream (the earlier layout). With the marker
   * present it must agree with the marker's stream.
   */
  switchId?: string;
  /** Synchronous crash-point hook (tests stop the tool here). */
  onStep?: (step: MeshBackendStep) => void;
  /**
   * smarty-dev#9690: runs synchronously under the fence right before the marker replaces state.json and right before
   * `import-commit` (a crash rerun too), after every server read. The reader-readiness gate goes here; a throw stops
   * that step (state.json unchanged before the marker; the stream stays importing before the commit).
   */
  beforeCommit?: (commit: MeshNatsCommitCheck) => void;
}

/** What the import is about to commit: the binding a `nats` reader proof must match (reader-proof.ts NatsProofBind). */
export interface MeshNatsCommitCheck {
  phase: "nats-marker" | "nats-import-commit";
  switchId: string;
  stream: string;
  /** The stream's creation time, as the server reports it. */
  streamCreated: string;
  /** The target identity (natsTargetOf); undefined when the server does not report the account. */
  target: string | undefined;
  /** The switch's challenge nonce from the stream's `import-begin` (undefined: an import begun without one; the gate refuses). */
  nonce: string | undefined;
  head: number;
  digest: string;
  fromEpoch: number;
  toEpoch: number;
}

/**
 * The import intent (smarty-dev#9690): written under the fence BEFORE the stream exists, so the per-switch stream S
 * (never derivable from the root alone) is known only to this root's migration until the marker names it. S is always
 * `natsStreamName(root)_<switchId>` (pi-fabric#937: root-bound, never an override); any other intent refuses.
 */
const INTENT = `${STATE_JSON}.nats-intent`;
interface NatsIntent { switchId: string; stream: string }
const readIntent = (root: string): NatsIntent | undefined => {
  const file = path.join(root, INTENT);
  const stat = fs.lstatSync(file, { throwIfNoEntry: false });
  if (stat === undefined) return undefined;
  assertOwnRegular(file, stat);
  const value = JSON.parse(fs.readFileSync(file, "utf8")) as Partial<NatsIntent>;
  if (typeof value.switchId !== "string" || !NATS_SWITCH_ID.test(value.switchId) || typeof value.stream !== "string"
    || value.stream !== natsSwitchStreamName(root, value.switchId)) {
    throw new MeshBackendRefusedError(`Fabric mesh NATS import refused: ${file} is not a valid intent`);
  }
  return { switchId: value.switchId, stream: value.stream };
};
const removeIntent = (root: string): void => {
  fs.rmSync(path.join(root, INTENT), { force: true });
  fsyncDirectory(root);
};
/**
 * A rollback's cleanup (LD-02): removes the intent only when it (re-read and validated, under the fence) names `stream`,
 * the switch this rollback retired; an intent of another (newer, staged) switch is never this rollback's and stays.
 */
const removeOwnIntent = (root: string, stream: string): void => {
  if (readIntent(root)?.stream === stream) removeIntent(root);
};

export interface MeshNatsMigrationResult {
  root: string;
  stream: string;
  /** The stream's lifecycle epoch after the operation: E+1 after an import, E+2 after a rollback. */
  epoch: number;
  digest: string;
  entries: number;
  tombstones: number;
  /** True when a rerun found the operation already done and verified it only. */
  converged: boolean;
}

type Snapshot = Parameters<typeof applyNatsMessage>[0];
const EMPTY: Snapshot = { seq: 0, entries: new Map(), versions: new Map(), tombstones: [], highWater: 0, status: "ready", epoch: 0,
  snapshotEnd: 0, snapshotDigest: "" };
const record = (value: object): Uint8Array => Buffer.from(JSON.stringify(value), "utf8");

const fsyncDirectory = (directory: string): void => {
  if (process.platform === "win32") return;
  const descriptor = fs.openSync(directory, "r");
  try { fs.fsyncSync(descriptor); } finally { fs.closeSync(descriptor); }
};

/** Our own regular file: never a symbolic link, a device or another user's file (pi-fabric#918 security P1). */
const assertOwnRegular = (file: string, stat: fs.Stats): void => {
  const uid = typeof process.getuid === "function" ? process.getuid() : undefined;
  if (!stat.isFile() || (uid !== undefined && stat.uid !== uid)) {
    throw new MeshBackendRefusedError(`Fabric mesh NATS migration refused: ${file} is not a regular file of this user`);
  }
};

/**
 * Creates `file` (it must not exist: O_CREAT|O_EXCL|O_NOFOLLOW, mode 0600), checks the descriptor is our regular file,
 * writes `data` and fsyncs. A pre-created path (a symbolic link to a target outside the root) refuses; nothing is truncated.
 */
const writeDurably = (file: string, data: Uint8Array | string): void => {
  const flags = fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | (fs.constants.O_NOFOLLOW ?? 0);
  let descriptor: number;
  try { descriptor = fs.openSync(file, flags, 0o600); }
  catch (error) {
    const code = (error as NodeJS.ErrnoException).code;
    if (code === "EEXIST" || code === "ELOOP") throw new MeshBackendRefusedError(`Fabric mesh NATS migration refused: ${file} already exists (${code})`);
    throw error;
  }
  try {
    assertOwnRegular(file, fs.fstatSync(descriptor));
    const bytes = typeof data === "string" ? Buffer.from(data, "utf8") : data;
    let offset = 0;
    while (offset < bytes.byteLength) offset += fs.writeSync(descriptor, bytes, offset, bytes.byteLength - offset);
    fs.fsyncSync(descriptor);
  } finally { fs.closeSync(descriptor); }
};

const replaceDurably = (root: string, target: string, data: Uint8Array | string): void => {
  const temporary = path.join(root, `${STATE_JSON}.mesh-backend-${process.pid}-${randomUUID()}.tmp`);
  writeDurably(temporary, data);
  renameAtomic(temporary, target);
  fsyncDirectory(root);
};

/** The projection's state in the file representation (what `meshSnapshotDigest` reads). */
export const snapshotOf = (projection: Snapshot): MeshStateSnapshot & { versions: Record<string, number>; tombstoneOrder: string[]; highWater: number } => ({
  entries: Object.fromEntries([...projection.entries].map(([key, entry]) => [key, { ...entry }])),
  versions: Object.fromEntries(projection.versions),
  tombstoneOrder: [...projection.tombstones],
  highWater: projection.highWater,
});

interface Limits { maxStateBytes: number; maxReplay: number; maxStateTombstones: number; maxPayloadBytes: number }
const limitsOf = (options: MeshNatsMigrationOptions): Limits => ({
  maxStateBytes: Math.max(1, Math.floor(options.maxStateBytes ?? DEFAULT_MAX_STATE_BYTES)),
  maxReplay: Math.max(1, Math.floor(options.maxReplay ?? DEFAULT_MAX_REPLAY)),
  maxStateTombstones: Math.max(0, Math.floor(options.maxStateTombstones ?? DEFAULT_MAX_TOMBSTONES)),
  maxPayloadBytes: Math.max(1, Math.min(MAX_MESSAGE_BYTES, Math.floor(options.maxPayloadBytes ?? MAX_MESSAGE_BYTES))),
});
const entryBytes = (key: string, entry: unknown): number => Buffer.byteLength(key, "utf8") + Buffer.byteLength(JSON.stringify(entry) ?? "", "utf8");

/**
 * Replays the stream through the backend's own projector (fails closed on any protocol violation), within the
 * limits: at most `maxReplay` messages, tombstones bounded as the backend bounds them, and the live state (keys and
 * entries) under `maxStateBytes`. A bound fails with LIMIT before the caller retires, renames or commits anything.
 */
const project = async (port: JetStreamPort, stream: string, limits: Limits): Promise<{ projection: Snapshot; head: number }> => {
  // pi-fabric#907: a stream may be compacted (a snapshot, then a purge below it). Start from its latest verified snapshot
  // through the backend's own loader (bounds, chunk chain, digest), then apply every later message here.
  const loaded = await NatsStateBackend.loadLatestSnapshot({ port, stream, subject: `${stream}.state`, snapshotSubject: `${stream}.snapshot`,
    maxEventBytes: MAX_MESSAGE_BYTES, maxStateTombstones: limits.maxStateTombstones, maxStateBytes: limits.maxStateBytes, maxReplay: limits.maxReplay });
  const head = await port.lastSequence(stream);
  let projection = loaded ?? EMPTY;
  if (head - projection.seq > limits.maxReplay) {
    throw new MeshNatsMigrationLimitError(`stream ${stream} holds ${head - projection.seq} messages after its snapshot, more than maxReplay ${limits.maxReplay}`);
  }
  let bytes = [...projection.entries].reduce((sum, [key, entry]) => sum + entryBytes(key, entry), 0);
  if (bytes > limits.maxStateBytes) throw new MeshNatsMigrationLimitError(`stream ${stream}'s snapshot holds more than maxStateBytes ${limits.maxStateBytes}`);
  for (let seq = projection.seq + 1; seq <= head; seq++) {
    const message = await port.getMessage(stream, seq);
    if (!message) throw new MeshBackendRefusedError(`Fabric mesh NATS stream ${stream} has no message ${seq} (purged?): refusing to migrate`);
    const before = projection;
    projection = applyNatsMessage(projection, message, limits.maxStateTombstones, natsSnapshotLimits({ maxStateBytes: limits.maxStateBytes, maxReplay: limits.maxReplay }));
    if (projection === before || projection.entries === before.entries) continue;
    // Only the keys this batch changed move the running size (the projector already validated the record).
    const changed = (JSON.parse(Buffer.from(message.data).toString("utf8")) as { changes?: { key: string }[] }).changes ?? [];
    for (const { key } of changed) {
      const old = before.entries.get(key);
      const now = projection.entries.get(key);
      bytes += (now === undefined ? 0 : entryBytes(key, now)) - (old === undefined ? 0 : entryBytes(key, old));
    }
    if (bytes > limits.maxStateBytes) throw new MeshNatsMigrationLimitError(`stream ${stream} projects more than maxStateBytes ${limits.maxStateBytes} at message ${seq}`);
  }
  return { projection, head };
};

/** The snapshot as batch records, deterministic for one source and epoch: the import's sequence 2, 3, ... */
const importChunks = (state: MeshStateSnapshot, epoch: number): Uint8Array[] => {
  const normalized = normalizeSnapshot(state);
  const changes: object[] = [
    ...normalized.entries.map(entry => ({ key: entry.key, put: { value: entry.value, version: entry.version, updatedAt: entry.updatedAt, updatedBy: entry.updatedBy } })),
    ...normalized.tombstones.map(tombstone => ({ key: tombstone.key, deleted: { version: tombstone.version } })),
  ];
  const groups: object[][] = [];
  let current: object[] = [];
  let size = 0;
  for (const change of changes) {
    const bytes = Buffer.byteLength(JSON.stringify(change), "utf8");
    if (current.length > 0 && size + bytes > CHUNK_BYTES) { groups.push(current); current = []; size = 0; }
    current.push(change);
    size += bytes;
  }
  if (current.length > 0 || groups.length === 0) groups.push(current);
  return groups.map((group, index) => record({ format: 1, type: "batch", id: `import-${epoch}-${index}`, base: 1 + index,
    highWater: normalized.highWater, changes: group }));
};

/** Connects, refuses an unsafe server, ensures the stream, then runs `body` under the migration fence. */
// `forImport`: an import is gated on the server version and creates its (intent's) stream; a rollback is neither.
const run = async <T>(root: string, options: MeshNatsMigrationOptions, forImport: boolean,
  pickStream: (root: string, port: JetStreamPort) => string | Promise<string>,
  body: (port: JetStreamPort, stream: string, subject: string, target: { created: string; target: string | undefined }) => Promise<T>): Promise<T> => {
  // (pickStream may await the port: the import's fixed-root preflight below.)
  root = path.resolve(root);
  if (!fs.statSync(root, { throwIfNoEntry: false })?.isDirectory()) throw new MeshBackendRefusedError(`No mesh root at ${root}`);
  // The backend's own endpoint path (pi-fabric#918 round 8): pinned URLs, refusals before any connect, and a remote
  // server without authentication refused before anything is read or written; a rollback too.
  const { port, owned, server } = await connectNatsEndpoint(options);
  try {
    // Before an IMPORT writes anything: a server the backend refuses to open would strand a migrated root (pi-fabric#918).
    // A rollback is the escape hatch from such a server, so it is never gated on the version or max_payload (round 4).
    if (forImport) { assertNatsServerSupported(server.version); assertNatsMaxPayload(server); }
    if (server.syncAlways !== true && options.acceptPeriodicSync !== true) {
      throw new MeshBackendRefusedError(`nats-server does not report sync_always (got ${String(server.syncAlways)}): refusing to migrate state onto it`);
    }
    return await holdMeshFence(root, { lockProtocol: options.lockProtocol ?? 1, lockTimeoutMs: options.lockTimeoutMs ?? DEFAULT_LOCK_MS }, async () => {
      const stream = await pickStream(root, port);
      // pi-fabric#937: whatever picked it, the stream is this root's (fixed or per-switch); two roots never share one.
      if (!isNatsStreamForRoot(root, stream)) throw new MeshBackendRefusedError(`Fabric mesh NATS stream ${stream} is not this root's`);
      const subject = `${stream}.state`;
      const subjects = [subject, `${stream}.snapshot`];
      const replicas = options.replicas === undefined ? {} : { replicas: options.replicas };
      // Only an import creates its stream (named by its intent first); a rollback needs the stream to exist. The stream
      // gets the backend's subjects and message cap (pi-fabric#907): the root's open refuses a stream without max_msg_size.
      // createStream delegates to ensureStream's create (replicas, placement, readiness, smarty-dev#11373); an existing
      // stream gets ensureStream's replica check and readiness wait, never a create or a change.
      if (await port.streamInfo(stream) === undefined) {
        if (!forImport) throw new MeshBackendRefusedError(`Fabric mesh NATS stream ${stream} does not exist`);
        await port.createStream(stream, subjects, { maxMsgSize: MAX_MESSAGE_BYTES, ...replicas });
      } else {
        await port.ensureStream(stream, subjects, { maxMsgSize: MAX_MESSAGE_BYTES, ...replicas, create: false });
      }
      const info = await port.streamInfo(stream);
      if (!info) throw new MeshBackendRefusedError(`Fabric mesh NATS stream ${stream} does not exist`);
      return body(port, stream, subject, { created: info.created, target: natsTargetOf(server) });
    });
  } finally { if (owned) await port.close().catch(() => undefined); }
};

const MIGRATION_PUBLISH_ATTEMPTS = 3;
const publish = async (port: JetStreamPort, subject: string, data: Uint8Array, expectLastSequence: number, msgID: string): Promise<number> => {
  let outcomeUnknown = false;
  for (let attempt = 1; attempt <= MIGRATION_PUBLISH_ATTEMPTS; attempt++) {
    try {
      const ack = await port.publish(subject, data, { expectLastSequence, msgID });
      if (outcomeUnknown && ack.duplicate) return await reconcile();
      return ack.seq;
    } catch (error) {
      if (error instanceof NatsWrongLastSequenceError) {
        if (outcomeUnknown) return await reconcile();
        throw new MeshBackendRefusedError(`Fabric mesh NATS stream moved during the migration (expected last sequence ${expectLastSequence}): another writer; rerun`);
      }
      if ((error as { transient?: boolean } | null)?.transient !== true) throw error;
      outcomeUnknown = true;
      if (attempt === MIGRATION_PUBLISH_ATTEMPTS) throw error;
    }
  }
  throw new Error("unreachable migration publish retry state");

  async function reconcile(): Promise<number> {
    const sequence = expectLastSequence + 1;
    const stored = await port.getMessage(subject.slice(0, subject.lastIndexOf(".")), sequence);
    if (!stored || stored.seq !== sequence || stored.msgID !== msgID || Buffer.compare(Buffer.from(stored.data), Buffer.from(data)) !== 0) {
      throw new MeshBackendRefusedError(`Fabric mesh NATS stream moved during the migration (expected last sequence ${expectLastSequence}): ambiguous publish at sequence ${sequence} does not exactly match; refusing`);
    }
    return sequence;
  }
};

const readSource = (file: string, options: MeshNatsMigrationOptions): MeshStateFile =>
  decodeMeshStateFile(file, options.maxStateBytes ?? DEFAULT_MAX_STATE_BYTES, false);

/** The kept source copy (a fixed name): our own regular file, never followed through a link; undefined when absent. */
const readKeptCopy = (copy: string, options: MeshNatsMigrationOptions): MeshStateFile | undefined => {
  const stat = fs.lstatSync(copy, { throwIfNoEntry: false });
  if (stat === undefined) return undefined;
  assertOwnRegular(copy, stat);
  return readSource(copy, options);
};

/** Whether a kept copy of this state is already at `copy`; a link or another user's file there (readKeptCopy), or a
 * copy of another state, refuses. */
const keptCopyPresent = (copy: string, digest: string, options: MeshNatsMigrationOptions): boolean => {
  const existing = readKeptCopy(copy, options);
  if (existing === undefined) return false;
  if (meshSnapshotDigest(existing) !== digest) throw new MeshBackendRefusedError(`Fabric mesh NATS migration refused: ${copy} exists and holds another state`);
  return true;
};
/** Keeps the source at `copy`: an existing copy must be our regular file with the source's digest, else refuse. */
const keepCopy = (root: string, copy: string, source: MeshStateFile, digest: string, options: MeshNatsMigrationOptions): void => {
  if (keptCopyPresent(copy, digest, options)) return;
  writeDurably(copy, encodeMeshStateFile(source).serialized);
  fsyncDirectory(root);
  // Read the new copy back BEFORE state.json becomes the marker: a torn or corrupted write never stands for the source.
  const written = readKeptCopy(copy, options);
  if (written === undefined || meshSnapshotDigest(written) !== digest) {
    throw new MeshBackendRefusedError(`Fabric mesh NATS migration refused: ${copy} does not read back as the source (digest mismatch); state.json is unchanged`);
  }
};

/** The export of a projection, within `maxStateBytes` (checked BEFORE the stream is retired). */
const encodeExport = (state: ReturnType<typeof snapshotOf>, limits: Limits): Buffer => {
  const serialized = encodeMeshStateFile({ readGeneration: randomUUID(), format: 1, revisionFormat: 2, ...state } as MeshStateFile).serialized;
  if (serialized.byteLength > limits.maxStateBytes) {
    throw new MeshNatsMigrationLimitError(`the export is ${serialized.byteLength} bytes, more than maxStateBytes ${limits.maxStateBytes}`);
  }
  return serialized;
};

/**
 * The stream a marker names (pi-fabric#937: it must be this root's, fixed or per-switch; a marker of the earlier
 * fixed-root layout is accepted). A rollback's `switchId` must agree with it.
 */
const markerStream = (root: string, options: MeshNatsMigrationOptions): string | undefined => {
  const marker = readMeshStateNatsMarker(root);
  if (!marker) return undefined;
  if (!isNatsStreamForRoot(root, marker.natsStream)) {
    throw new MeshBackendRefusedError(`Fabric mesh state.json says the state is on NATS stream ${marker.natsStream}, which is not this root's (${natsStreamName(root)})`);
  }
  if (options.switchId !== undefined && natsSwitchStreamName(root, options.switchId) !== marker.natsStream) {
    throw new MeshBackendRefusedError(`Fabric mesh state.json says the state is on NATS stream ${marker.natsStream}, not switch ${options.switchId}'s`);
  }
  return marker.natsStream;
};

/**
 * The import's stream: the marker's (a rerun after it), else the intent's (a rerun before it), else a NEW per-switch
 * stream `natsStreamName(root)_<switch id>`, recorded in a new intent before it is created (never an override).
 */
const importStream = async (root: string, options: MeshNatsMigrationOptions, port: JetStreamPort): Promise<string> => {
  const marked = markerStream(root, options);
  const intent = readIntent(root);
  if (marked !== undefined) {
    if (intent && intent.stream !== marked) throw new MeshBackendRefusedError(`Fabric mesh NATS import refused: ${INTENT} names ${intent.stream}, the marker ${marked}`);
    return marked;
  }
  if (intent) return intent.stream;
  // Decision B preflight, BEFORE the intent or any new stream: a root that bootstrapped (or still serves) its fixed-root
  // stream natsStreamName(root) without a marker has live NATS state there; a new per-switch import would silently fork
  // it. Only an absent or retired fixed-root stream lets a new switch begin; anything else (or an unreadable one) refuses.
  const fixed = natsStreamName(root);
  if (await port.streamExists(fixed)) {
    const { projection } = await project(port, fixed, limitsOf(options));
    if (projection.status !== "retired") {
      throw new MeshBackendRefusedError(`Fabric mesh NATS import refused for ${root}: its fixed-root stream ${fixed} exists and is ${projection.status}, not retired (live NATS state); a new switch would fork it. Roll it back or retire it first`);
    }
  }
  const switchId = randomUUID().replaceAll("-", "");
  const stream = natsSwitchStreamName(root, switchId);
  writeDurably(path.join(root, INTENT), JSON.stringify({ switchId, stream } satisfies NatsIntent));
  fsyncDirectory(root);
  if (readIntent(root)?.stream !== stream) throw new MeshBackendRefusedError(`Fabric mesh NATS import refused: ${INTENT} does not read back`);
  options.onStep?.("nats-intent");
  return stream;
};

/** The switch id the stream's `import-begin` records (undefined before it). */
type BeginRecord = { type?: string; digest?: string; generation?: string; switch?: string; nonce?: string };
const beginSwitch = async (port: JetStreamPort, stream: string): Promise<BeginRecord | undefined> => {
  const begin = await port.getMessage(stream, 1);
  return begin ? JSON.parse(Buffer.from(begin.data).toString("utf8")) as BeginRecord : undefined;
};

/** Imports a file root's state into its NATS stream and moves state.json to the NATS marker. */
export const importMeshStateToNats = (root: string, options: MeshNatsMigrationOptions = {}): Promise<MeshNatsMigrationResult> =>
  run(root, options, true, (at, port) => {
    if (fs.existsSync(path.join(at, "state.db")) || readMeshStateMovedMarker(at)) {
      throw new MeshBackendRefusedError(`Fabric mesh NATS import refused for ${at}: it has state.db; roll it back to file first (rollbackMeshState)`);
    }
    return importStream(at, options, port);
  }, async (port, stream, subject, { created, target }) => {
    root = path.resolve(root);
    const limits = limitsOf(options);
    const marker = readMeshStateNatsMarker(root);
    const intent = readIntent(root);
    let { projection, head } = await project(port, stream, limits);
    const recordedBegin = head > 0 ? await beginSwitch(port, stream) : undefined;
    // The switch id: the stream's own `import-begin` once it exists, else the intent's. Undefined only for a rerun of an
    // import of the earlier fixed-root layout (its marker names natsStreamName(root)): it converges or commits ungated.
    const switchId = typeof recordedBegin?.switch === "string" ? recordedBegin.switch : intent?.switchId;
    if (switchId === undefined && !marker) throw new MeshBackendRefusedError(`Fabric mesh NATS import refused: no intent and no import-begin name the switch of ${stream}`);
    // pi-fabric#927 round 1: the switch's challenge nonce lives ONLY in the stream (`import-begin`): a reader learns it
    // only by reading NATS. Never written to the mesh directory, a switch record or the output.
    let nonce = typeof recordedBegin?.nonce === "string" ? recordedBegin.nonce : undefined;
    const check = (phase: MeshNatsCommitCheck["phase"], digest: string): void => {
      if (options.beforeCommit === undefined) return;
      if (switchId === undefined) throw new MeshBackendRefusedError(`Fabric mesh NATS import refused: no import-begin names the switch of ${stream} (an import of the earlier layout); the readiness gate cannot bind it`);
      options.beforeCommit({ phase, switchId, stream, streamCreated: created, target, head, digest, nonce, fromEpoch: 0, toEpoch: 1 });
    };
    if (marker) {
      // Rerun after the marker: the stream must hold the kept copy's state; commit it if it is still importing.
      const copy = path.join(root, `${STATE_JSON}.nats-cutover-${marker.epoch}`);
      const kept = readKeptCopy(copy, options);
      if (kept === undefined) throw new MeshBackendRefusedError(`Fabric mesh NATS import rerun refused: ${copy} is missing`);
      const digest = meshSnapshotDigest(kept);
      if (projection.status === "ready" && projection.epoch === marker.epoch) {
        if (intent) removeIntent(root);
        return { root, stream, epoch: marker.epoch, digest, entries: projection.entries.size, tombstones: projection.tombstones.length, converged: true };
      }
      if (projection.status !== "importing" || projection.epoch !== marker.epoch) {
        throw new MeshBackendRefusedError(`Fabric mesh NATS stream ${stream} is ${projection.status} at epoch ${projection.epoch}, but state.json is its marker at epoch ${marker.epoch}`);
      }
      if (meshSnapshotDigest(snapshotOf(projection)) !== digest) throw new MeshBackendRefusedError(`Fabric mesh NATS stream ${stream} does not hold ${copy}: refusing to commit`);
      check("nats-import-commit", digest);
      await publish(port, subject, record({ format: 1, type: "import-commit", epoch: marker.epoch }), head, `${stream}-import-commit-${marker.epoch}`);
      options.onStep?.("nats-import-commit");
      if (intent) removeIntent(root);
      return { root, stream, epoch: marker.epoch, digest, entries: projection.entries.size, tombstones: projection.tombstones.length, converged: false };
    }

    const statePath = path.join(root, STATE_JSON);
    const source = readSource(statePath, options);
    const generation = typeof source.readGeneration === "string" ? source.readGeneration : "";
    const digest = meshSnapshotDigest(source);
    // A file root without state.db is at epoch 0 (an epoch-stamped state.json needs its state.db), so E+1 = 1; every
    // import uses its own empty stream, whose lifecycle starts at 0.
    const epoch = 1;
    // pi-fabric#918 round 8: the kept-copy path is checked BEFORE anything is written to the stream, so a hostile link
    // there (or a copy of another state) refuses with the stream untouched, whatever the stream already holds. keepCopy
    // checks it again right before it writes (a link created in between still refuses, never followed).
    keptCopyPresent(path.join(root, `${STATE_JSON}.nats-cutover-${epoch}`), digest, options);
    const tombstones = normalizeSnapshot(source).tombstones.length;
    if (tombstones > limits.maxStateTombstones) {
      throw new MeshNatsMigrationLimitError(`state.json holds ${tombstones} tombstones, more than maxStateTombstones ${limits.maxStateTombstones}`);
    }
    const chunks = importChunks(source, epoch);
    // A single change larger than one message (a chunk holds at least one change) refuses here, BEFORE import-begin:
    // the stream never enters importing for a state it cannot hold.
    const largest = chunks.reduce((max, chunk) => Math.max(max, chunk.byteLength), 0);
    if (largest > limits.maxPayloadBytes) {
      throw new MeshNatsMigrationLimitError(`one import chunk is ${largest} bytes, more than the server's max payload ${limits.maxPayloadBytes} (a single entry is too large)`);
    }
    if (head === 0) {
      nonce = randomBytes(16).toString("hex");
      head = await publish(port, subject, record({ format: 1, type: "import-begin", epoch, digest, generation, switch: switchId, nonce }), 0, `${stream}-import-begin-${epoch}`);
      options.onStep?.("nats-import-begin");
    } else {
      const recorded = recordedBegin;
      // A committed, live or retired projection may be this root's live NATS mesh: never advise deleting it.
      if (projection.status !== "importing") {
        throw new MeshBackendRefusedError(`Fabric mesh: this root already has NATS state in stream ${stream} `
          + `(${projection.status} at epoch ${projection.epoch}, ${head} messages); an import never overwrites it`);
      }
      if (projection.epoch !== epoch || recorded?.type !== "import-begin" || recorded.digest !== digest
        || recorded.generation !== generation || recorded.switch !== intent?.switchId || head > 1 + chunks.length) {
        throw new MeshBackendRefusedError(`Fabric mesh NATS stream ${stream} holds an unfinished import of another state.json `
          + `(epoch ${projection.epoch}, ${head} messages): ${LEFTOVER(stream)}`);
      }
    }
    for (const [index, chunk] of chunks.entries()) {
      const seq = 2 + index;
      if (seq <= head) {
        const stored = await port.getMessage(stream, seq);
        if (!stored || Buffer.compare(Buffer.from(stored.data), Buffer.from(chunk)) !== 0) {
          throw new MeshBackendRefusedError(`Fabric mesh NATS stream ${stream} message ${seq} is not chunk ${index} of this import: ${LEFTOVER(stream)}`);
        }
        continue;
      }
      head = await publish(port, subject, chunk, seq - 1, `${stream}-import-${epoch}-${index}`);
      options.onStep?.("nats-import-chunk");
    }
    ({ projection, head } = await project(port, stream, limits));
    const projected = meshSnapshotDigest(snapshotOf(projection));
    if (projection.status !== "importing" || projected !== digest) {
      throw new MeshBackendRefusedError(`Fabric mesh NATS import verification failed: the stream projects ${projected}, state.json is ${digest}`);
    }
    options.onStep?.("nats-import-verify");
    const again = readSource(statePath, options);
    if (meshSnapshotDigest(again) !== digest || (typeof again.readGeneration === "string" ? again.readGeneration : "") !== generation) {
      throw new MeshBackendRefusedError(`Fabric mesh state.json changed during the NATS import (a writer ignored .lock): ${LEFTOVER(stream)}`);
    }
    // Keep the source (the rerun's and the operator's reference), then the marker, then the commit.
    keepCopy(root, path.join(root, `${STATE_JSON}.nats-cutover-${epoch}`), source, digest, options);
    options.onStep?.("nats-copy");
    check("nats-marker", digest);
    replaceDurably(root, statePath, encodeMeshStateNatsMarker(stream, epoch));
    options.onStep?.("nats-marker");
    check("nats-import-commit", digest);
    await publish(port, subject, record({ format: 1, type: "import-commit", epoch }), head, `${stream}-import-commit-${epoch}`);
    options.onStep?.("nats-import-commit");
    removeIntent(root);
    return { root, stream, epoch, digest, entries: projection.entries.size, tombstones: projection.tombstones.length, converged: false };
  });

/** Rolls a NATS root back to file: retires the stream, exports its state to state.json over the marker. */
export const rollbackMeshStateFromNats = (root: string, options: MeshNatsMigrationOptions = {}): Promise<MeshNatsMigrationResult> =>
  run(root, options, false, (at) => {
    // The marker names the stream; after the rename (a converged rerun) the root-bound stream of the given switch id,
    // else the root's fixed stream (the earlier layout). Never an arbitrary stream (pi-fabric#937).
    return markerStream(at, options) ?? (options.switchId === undefined ? natsStreamName(at) : natsSwitchStreamName(at, options.switchId));
  }, async (port, stream, subject) => {
    root = path.resolve(root);
    const limits = limitsOf(options);
    const marker = readMeshStateNatsMarker(root);
    // LD-02: an intent present is validated BEFORE anything is retired or renamed (an invalid one refuses here).
    readIntent(root);
    let { projection, head } = await project(port, stream, limits);
    if (!marker) {
      // A rerun after the rename: done when the stream is retired and state.json holds its state.
      const digest = meshSnapshotDigest(snapshotOf(projection));
      if (projection.status === "retired" && meshSnapshotDigest(readSource(path.join(root, STATE_JSON), options)) === digest) {
        // A crash after the rename left this switch's intent: remove it (only if it is this stream's).
        removeOwnIntent(root, stream);
        return { root, stream, epoch: projection.epoch, digest, entries: projection.entries.size, tombstones: projection.tombstones.length, converged: true };
      }
      throw new MeshBackendRefusedError(`Fabric mesh NATS rollback refused for ${root}: state.json is not the NATS marker of ${stream}`);
    }
    if (projection.status !== "retired") {
      if (projection.epoch !== marker.epoch) {
        throw new MeshBackendRefusedError(`Fabric mesh NATS stream ${stream} is at epoch ${projection.epoch}, the marker at ${marker.epoch}`);
      }
      // LIMIT before the stream is retired: the export size, and the replay that will include the retire record.
      if (head + 1 > limits.maxReplay) {
        throw new MeshNatsMigrationLimitError(`stream ${stream} holds ${head} messages; with the retire record it exceeds maxReplay ${limits.maxReplay}`);
      }
      encodeExport(snapshotOf(projection), limits);
      head = await publish(port, subject, record({ format: 1, type: "retire", epoch: marker.epoch + 1 }), head, `${stream}-retire-${marker.epoch + 1}`);
      options.onStep?.("nats-retire");
      ({ projection, head } = await project(port, stream, limits));
    }
    const state = snapshotOf(projection);
    const digest = meshSnapshotDigest(state);
    // A random name, created exclusively: a pre-created path (a link) can never make the export write elsewhere.
    const temporary = path.join(root, `${STATE_JSON}.nats-rollback-${projection.epoch}-${randomUUID()}.tmp`);
    writeDurably(temporary, encodeExport(state, limits));
    fsyncDirectory(root);
    if (meshSnapshotDigest(readSource(temporary, options)) !== digest) {
      throw new MeshBackendRefusedError(`Fabric mesh NATS export verification of ${temporary} failed`);
    }
    options.onStep?.("nats-export-temp");
    renameAtomic(temporary, path.join(root, STATE_JSON));
    fsyncDirectory(root);
    options.onStep?.("nats-replace");
    if (meshSnapshotDigest(readSource(path.join(root, STATE_JSON), options)) !== digest) {
      throw new MeshBackendRefusedError("Fabric mesh NATS rollback verification: state.json does not hold the stream's state");
    }
    // LD-02: a rollback after the marker but before import-commit retires the intent's stream: its intent goes too, so the
    // next import allocates a NEW switch instead of reselecting the retired stream.
    removeOwnIntent(root, stream);
    return { root, stream, epoch: projection.epoch, digest, entries: projection.entries.size, tombstones: projection.tombstones.length, converged: false };
  });

/**
 * `import --to nats --abandon` (smarty-dev#9690, R5): under the fence, with no marker, deletes exactly the intent's
 * stream when it is absent, empty, or this switch's unfinished import (importing at epoch 1 with the intent's switch
 * id), then the intent. Anything else refuses: an abandon never deletes a stream that a marker names or that holds
 * another switch's records.
 */
export const abandonMeshNatsImport = async (root: string, options: MeshNatsMigrationOptions = {}): Promise<{ root: string; stream: string; deleted: boolean }> => {
  root = path.resolve(root);
  // The same secured endpoint as the migration (pi-fabric#918 round 8): refusals before any connect or delete.
  const { port, owned } = await connectNatsEndpoint(options);
  try {
    return await holdMeshFence(root, { lockProtocol: options.lockProtocol ?? 1, lockTimeoutMs: options.lockTimeoutMs ?? DEFAULT_LOCK_MS }, async () => {
      // LD-03: a malformed marker is never "no marker" (as the import and rollback preflights): refuse before any stream call.
      const statePath = path.join(root, STATE_JSON);
      if (stateFileIsMalformedNatsMarker(root)) throw meshStateMalformedMarkerError(statePath);
      if (readMeshStateNatsMarker(root)) throw new MeshBackendRefusedError(`Fabric mesh NATS abandon refused for ${root}: the marker names the stream (roll back instead)`);
      // LD-03: a present state.json must be a strict, readable file state (a failed import keeps it so); truncated,
      // invalid, another marker or unreadable (EACCES, ...) refuses. Absent: a crash before the source was read.
      if (fs.lstatSync(statePath, { throwIfNoEntry: false }) !== undefined) {
        try { readSource(statePath, options); }
        catch (error) {
          throw new MeshBackendRefusedError(`Fabric mesh NATS abandon refused for ${root}: ${statePath} is not a readable file state `
            + `(${error instanceof Error ? error.message : String(error)}); repair it first`);
        }
      }
      const intent = readIntent(root);
      if (!intent) throw new MeshBackendRefusedError(`Fabric mesh NATS abandon refused for ${root}: no ${INTENT} (nothing to abandon)`);
      // readIntent already bound the stream to this root and switch; checked again right before a delete.
      if (!isNatsStreamForRoot(root, intent.stream) || intent.stream === natsStreamName(root)) {
        throw new MeshBackendRefusedError(`Fabric mesh NATS abandon refused: ${intent.stream} is not this root's per-switch stream`);
      }
      const info = await port.streamInfo(intent.stream);
      let deleted = false;
      if (info) {
        if (info.lastSeq > 0) {
          const { projection } = await project(port, intent.stream, limitsOf(options));
          const begin = await beginSwitch(port, intent.stream);
          if (projection.status !== "importing" || projection.epoch !== 1 || begin?.switch !== intent.switchId) {
            throw new MeshBackendRefusedError(`Fabric mesh NATS abandon refused: ${intent.stream} is ${projection.status} at epoch ${projection.epoch}, not this switch's unfinished import`);
          }
        }
        deleted = await port.deleteStream(intent.stream);
      }
      removeIntent(root);
      return { root, stream: intent.stream, deleted };
    });
  } finally { if (owned) await port.close().catch(() => undefined); }
};
