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
 * already stored must match byte for byte, the rest are published; a changed source refuses (use a fresh stream).
 * With the marker, the stream is verified against the kept copy and committed (or found committed).
 *
 * Rollback (nats -> file), under the fence: publish `retire{E+1}` with the expected last sequence (every later nats
 * write fails its CAS and projectors refuse), project the stream at `retire`, export it through the normal
 * state.json encoder into `state.json.nats-rollback-<E+1>.tmp` (fsync, read back, digest), and rename it over the
 * marker. A rerun with a retired stream redoes the export and the rename.
 *
 * The local locks fence this host only; other hosts are fenced by the stream itself (every nats write carries an
 * expected sequence). This module loads the NATS client only when no port is given.
 */
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { renameAtomic } from "../core/atomic-write.js";
import { encodeMeshStateNatsMarker, readMeshStateMovedMarker, readMeshStateNatsMarker } from "./backend-fence.js";
import { MeshBackendRefusedError, meshSnapshotDigest, normalizeSnapshot, type MeshBackendStep, type MeshStateSnapshot } from "./backend-migration.js";
import { holdMeshFence } from "./fence-lock.js";
import { applyNatsMessage, assertNatsTransportSecure, natsStreamName, NatsWrongLastSequenceError, type JetStreamPort, type NatsTlsFiles } from "./state-backend-nats.js";
import { decodeMeshStateFile, encodeMeshStateFile, type MeshStateFile } from "./state-file.js";

const STATE_JSON = "state.json";
const DEFAULT_MAX_STATE_BYTES = 32 * 1024 * 1024;
const DEFAULT_LOCK_MS = 60_000;
/** One chunk's changes stay well under the server's default max_payload (1 MiB). */
const CHUNK_BYTES = 512 * 1024;
const MAX_TOMBSTONES = Number.MAX_SAFE_INTEGER; // the import carries every tombstone; the backend bounds them after

export interface MeshNatsMigrationOptions {
  /** A JetStream port (tests); otherwise `servers` (and `monitoringUrl`, `tls`) connect the official client. */
  port?: JetStreamPort;
  servers?: string[];
  monitoringUrl?: string;
  tls?: NatsTlsFiles;
  /** Host-only: accept a server that does not report `sync_always` (as openNatsStateBackend). */
  acceptPeriodicSync?: boolean;
  /** The stream; default `natsStreamName(root)`. A rerun must name the same one (the marker records it). */
  stream?: string;
  maxStateBytes?: number;
  lockTimeoutMs?: number;
  lockProtocol?: 1 | 2;
  /** Synchronous crash-point hook (tests stop the tool here). */
  onStep?: (step: MeshBackendStep) => void;
}

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
const EMPTY: Snapshot = { seq: 0, entries: new Map(), versions: new Map(), tombstones: [], highWater: 0, status: "ready", epoch: 0 };
const record = (value: object): Uint8Array => Buffer.from(JSON.stringify(value), "utf8");

const fsyncDirectory = (directory: string): void => {
  if (process.platform === "win32") return;
  const descriptor = fs.openSync(directory, "r");
  try { fs.fsyncSync(descriptor); } finally { fs.closeSync(descriptor); }
};

const writeDurably = (file: string, data: Uint8Array | string): void => {
  const descriptor = fs.openSync(file, "w", 0o600);
  try {
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
const snapshotOf = (projection: Snapshot): MeshStateSnapshot & { versions: Record<string, number>; tombstoneOrder: string[]; highWater: number } => ({
  entries: Object.fromEntries([...projection.entries].map(([key, entry]) => [key, { ...entry }])),
  versions: Object.fromEntries(projection.versions),
  tombstoneOrder: [...projection.tombstones],
  highWater: projection.highWater,
});

/** Replays the whole stream through the backend's own projector (fails closed on any protocol violation). */
const project = async (port: JetStreamPort, stream: string): Promise<{ projection: Snapshot; head: number }> => {
  const head = await port.lastSequence(stream);
  let projection = EMPTY;
  for (let seq = 1; seq <= head; seq++) {
    const message = await port.getMessage(stream, seq);
    if (!message) throw new MeshBackendRefusedError(`Fabric mesh NATS stream ${stream} has no message ${seq} (purged?): refusing to migrate`);
    projection = applyNatsMessage(projection, message, MAX_TOMBSTONES);
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

const connect = async (options: MeshNatsMigrationOptions): Promise<{ port: JetStreamPort; owned: boolean }> => {
  if (options.port) return { port: options.port, owned: false };
  for (const server of options.servers ?? []) assertNatsTransportSecure(server, "tls:");
  if (options.monitoringUrl !== undefined) assertNatsTransportSecure(options.monitoringUrl, "https:");
  const tls = options.tls ?? ((options.servers ?? []).some(server => /^tls:\/\//i.test(server)) ? {} : undefined);
  const port = await (await import("./state-backend-nats-client.js")).connectJetStream(options.servers ?? [], {
    ...(options.monitoringUrl === undefined ? {} : { monitoringUrl: options.monitoringUrl }), ...(tls === undefined ? {} : { tls }) });
  return { port, owned: true };
};

/** Connects, refuses an unsafe server, ensures the stream, then runs `body` under the migration fence. */
const run = async <T>(root: string, options: MeshNatsMigrationOptions,
  body: (port: JetStreamPort, stream: string, subject: string) => Promise<T>): Promise<T> => {
  root = path.resolve(root);
  if (!fs.statSync(root, { throwIfNoEntry: false })?.isDirectory()) throw new MeshBackendRefusedError(`No mesh root at ${root}`);
  const { port, owned } = await connect(options);
  try {
    const server = await port.server();
    if (server.syncAlways !== true && options.acceptPeriodicSync !== true) {
      throw new MeshBackendRefusedError(`nats-server does not report sync_always (got ${String(server.syncAlways)}): refusing to migrate state onto it`);
    }
    return await holdMeshFence(root, { lockProtocol: options.lockProtocol ?? 1, lockTimeoutMs: options.lockTimeoutMs ?? DEFAULT_LOCK_MS }, async () => {
      const marker = readMeshStateNatsMarker(root);
      const stream = marker?.stream ?? options.stream ?? natsStreamName(root);
      if (marker && options.stream !== undefined && options.stream !== marker.stream) {
        throw new MeshBackendRefusedError(`Fabric mesh state.json says the state is on NATS stream ${marker.stream}, not ${options.stream}`);
      }
      const subject = `${stream}.state`;
      await port.ensureStream(stream, subject);
      return body(port, stream, subject);
    });
  } finally { if (owned) await port.close().catch(() => undefined); }
};

const publish = async (port: JetStreamPort, subject: string, data: Uint8Array, expectLastSequence: number, msgID: string): Promise<number> => {
  try { return (await port.publish(subject, data, { expectLastSequence, msgID })).seq; }
  catch (error) {
    if (error instanceof NatsWrongLastSequenceError) {
      throw new MeshBackendRefusedError(`Fabric mesh NATS stream moved during the migration (expected last sequence ${expectLastSequence}): another writer; rerun`);
    }
    throw error;
  }
};

const readSource = (file: string, options: MeshNatsMigrationOptions): MeshStateFile =>
  decodeMeshStateFile(file, options.maxStateBytes ?? DEFAULT_MAX_STATE_BYTES, false);

/** Imports a file root's state into its NATS stream and moves state.json to the NATS marker. */
export const importMeshStateToNats = (root: string, options: MeshNatsMigrationOptions = {}): Promise<MeshNatsMigrationResult> =>
  run(root, options, async (port, stream, subject) => {
    root = path.resolve(root);
    if (fs.existsSync(path.join(root, "state.db")) || readMeshStateMovedMarker(root)) {
      throw new MeshBackendRefusedError(`Fabric mesh NATS import refused for ${root}: it has state.db; roll it back to file first (rollbackMeshState)`);
    }
    const marker = readMeshStateNatsMarker(root);
    let { projection, head } = await project(port, stream);
    if (marker) {
      // Rerun after the marker: the stream must hold the kept copy's state; commit it if it is still importing.
      const copy = path.join(root, `${STATE_JSON}.nats-cutover-${marker.epoch}`);
      const digest = meshSnapshotDigest(readSource(copy, options));
      if (projection.status === "ready" && projection.epoch === marker.epoch) {
        return { root, stream, epoch: marker.epoch, digest, entries: projection.entries.size, tombstones: projection.tombstones.length, converged: true };
      }
      if (projection.status !== "importing" || projection.epoch !== marker.epoch) {
        throw new MeshBackendRefusedError(`Fabric mesh NATS stream ${stream} is ${projection.status} at epoch ${projection.epoch}, but state.json is its marker at epoch ${marker.epoch}`);
      }
      if (meshSnapshotDigest(snapshotOf(projection)) !== digest) throw new MeshBackendRefusedError(`Fabric mesh NATS stream ${stream} does not hold ${copy}: refusing to commit`);
      await publish(port, subject, record({ format: 1, type: "import-commit", epoch: marker.epoch }), head, `${stream}-import-commit-${marker.epoch}`);
      options.onStep?.("nats-import-commit");
      return { root, stream, epoch: marker.epoch, digest, entries: projection.entries.size, tombstones: projection.tombstones.length, converged: false };
    }

    const statePath = path.join(root, STATE_JSON);
    const source = readSource(statePath, options);
    const generation = typeof source.readGeneration === "string" ? source.readGeneration : "";
    const digest = meshSnapshotDigest(source);
    // A file root without state.db is at epoch 0 (an epoch-stamped state.json needs its state.db), so E+1 = 1; every
    // import uses its own empty stream, whose lifecycle starts at 0.
    const epoch = 1;
    const chunks = importChunks(source, epoch);
    if (head === 0) {
      head = await publish(port, subject, record({ format: 1, type: "import-begin", epoch, digest, generation }), 0, `${stream}-import-begin-${epoch}`);
      options.onStep?.("nats-import-begin");
    } else {
      const begin = await port.getMessage(stream, 1);
      const recorded = begin ? JSON.parse(Buffer.from(begin.data).toString("utf8")) as { type?: string; digest?: string; generation?: string } : undefined;
      if (projection.status !== "importing" || projection.epoch !== epoch || recorded?.type !== "import-begin"
        || recorded.digest !== digest || recorded.generation !== generation || head > 1 + chunks.length) {
        throw new MeshBackendRefusedError(`Fabric mesh NATS stream ${stream} is not empty and holds no unfinished import of this state.json `
          + `(${projection.status} at epoch ${projection.epoch}, ${head} messages): name a fresh stream`);
      }
    }
    for (const [index, chunk] of chunks.entries()) {
      const seq = 2 + index;
      if (seq <= head) {
        const stored = await port.getMessage(stream, seq);
        if (!stored || Buffer.compare(Buffer.from(stored.data), Buffer.from(chunk)) !== 0) {
          throw new MeshBackendRefusedError(`Fabric mesh NATS stream ${stream} message ${seq} is not chunk ${index} of this import: name a fresh stream`);
        }
        continue;
      }
      head = await publish(port, subject, chunk, seq - 1, `${stream}-import-${epoch}-${index}`);
      options.onStep?.("nats-import-chunk");
    }
    ({ projection, head } = await project(port, stream));
    const projected = meshSnapshotDigest(snapshotOf(projection));
    if (projection.status !== "importing" || projected !== digest) {
      throw new MeshBackendRefusedError(`Fabric mesh NATS import verification failed: the stream projects ${projected}, state.json is ${digest}`);
    }
    options.onStep?.("nats-import-verify");
    const again = readSource(statePath, options);
    if (meshSnapshotDigest(again) !== digest || (typeof again.readGeneration === "string" ? again.readGeneration : "") !== generation) {
      throw new MeshBackendRefusedError("Fabric mesh state.json changed during the NATS import (a writer ignored .lock); rerun with a fresh stream");
    }
    // Keep the source (the rerun's and the operator's reference), then the marker, then the commit.
    const copy = path.join(root, `${STATE_JSON}.nats-cutover-${epoch}`);
    writeDurably(copy, encodeMeshStateFile(source).serialized);
    fsyncDirectory(root);
    options.onStep?.("nats-copy");
    replaceDurably(root, statePath, encodeMeshStateNatsMarker(stream, epoch));
    options.onStep?.("nats-marker");
    await publish(port, subject, record({ format: 1, type: "import-commit", epoch }), head, `${stream}-import-commit-${epoch}`);
    options.onStep?.("nats-import-commit");
    return { root, stream, epoch, digest, entries: projection.entries.size, tombstones: projection.tombstones.length, converged: false };
  });

/** Rolls a NATS root back to file: retires the stream, exports its state to state.json over the marker. */
export const rollbackMeshStateFromNats = (root: string, options: MeshNatsMigrationOptions = {}): Promise<MeshNatsMigrationResult> =>
  run(root, options, async (port, stream, subject) => {
    root = path.resolve(root);
    const marker = readMeshStateNatsMarker(root);
    let { projection, head } = await project(port, stream);
    if (!marker) {
      // A rerun after the rename: done when the stream is retired and state.json holds its state.
      const digest = meshSnapshotDigest(snapshotOf(projection));
      if (projection.status === "retired" && meshSnapshotDigest(readSource(path.join(root, STATE_JSON), options)) === digest) {
        return { root, stream, epoch: projection.epoch, digest, entries: projection.entries.size, tombstones: projection.tombstones.length, converged: true };
      }
      throw new MeshBackendRefusedError(`Fabric mesh NATS rollback refused for ${root}: state.json is not the NATS marker of ${stream}`);
    }
    if (projection.status !== "retired") {
      if (projection.epoch !== marker.epoch) {
        throw new MeshBackendRefusedError(`Fabric mesh NATS stream ${stream} is at epoch ${projection.epoch}, the marker at ${marker.epoch}`);
      }
      head = await publish(port, subject, record({ format: 1, type: "retire", epoch: marker.epoch + 1 }), head, `${stream}-retire-${marker.epoch + 1}`);
      options.onStep?.("nats-retire");
      ({ projection, head } = await project(port, stream));
    }
    const state = snapshotOf(projection);
    const digest = meshSnapshotDigest(state);
    const temporary = path.join(root, `${STATE_JSON}.nats-rollback-${projection.epoch}.tmp`);
    writeDurably(temporary, encodeMeshStateFile({ readGeneration: randomUUID(), format: 1, revisionFormat: 2, ...state } as MeshStateFile).serialized);
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
    return { root, stream, epoch: projection.epoch, digest, entries: projection.entries.size, tombstones: projection.tombstones.length, converged: false };
  });
