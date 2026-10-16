import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createRequire } from "node:module";
import { isMeshStateMovedMarker } from "../mesh/backend-fence.js";
import { openPrivateStateDb } from "../mesh/state-gate.js";
import type { SqliteConnection } from "../mesh/state-sqlite.js";
import { ownProcessIncarnation, syncDirectoryChain } from "../core/atomic-write.js";
import { ActorRegistryStore } from "../actors/registry-store.js";
import { sessionActorRootGone } from "../actors/session-orphans.js";
import { acquireMeshCustodyLock } from "../mesh/custody-lock.js";
import { MeshLock } from "../mesh/mesh-lock.js";
import { assertMeshStateReadable, censusRecordAlive, type MeshStateEntry, type MeshStore } from "../mesh/store.js";
import { hostEntryLiveness, hostLeasePath, readHostLeaseCurrent, type FabricHostLease } from "./host-leases.js";
import { withParticipantFileTryLock } from "./participant-files.js";

/** Operator-only archival. Nothing imports this module from startup or idle hooks. */
export const STALE_MESH_GRACE_MS = 6 * 60 * 60 * 1000;
const dirs = ["host-leases", "participants", "main-followups", "actors"] as const;
type Directory = typeof dirs[number];
const object = (value: unknown): Record<string, unknown> | undefined =>
  value !== null && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : undefined;
const finite = (value: unknown): value is number => typeof value === "number" && Number.isFinite(value) && value > 0;
const hash = (value: string | Buffer): string => createHash("sha256").update(value).digest("hex");
const participantKey = (id: string): string => `topology/participants/${hash(id)}`;
const missing = (error: unknown): boolean => (error as NodeJS.ErrnoException).code === "ENOENT";

/** All clocks, not just host TTL. Malformed optional evidence is never expiry evidence.
 * cutoff is an absolute deadline (normally now - six hours), reusable by host-reaper. */
export const hostLeasePastGrace = (lease: FabricHostLease, cutoff: number): boolean => {
  if (!Number.isFinite(cutoff) || !finite(lease.updatedAt) || !finite(lease.expiresAt)) return false;
  const deadlines = [lease.updatedAt, lease.expiresAt];
  if (lease.reloadUntil !== undefined) {
    if (!finite(lease.reloadUntil)) return false;
    deadlines.push(lease.reloadUntil);
  }
  if (lease.session !== undefined) {
    if (!object(lease.session) || typeof lease.session.id !== "string" ||
        !finite(lease.session.startedAt) || !finite(lease.session.updatedAt) || !finite(lease.session.expiresAt)) return false;
    deadlines.push(lease.session.startedAt, lease.session.updatedAt, lease.session.expiresAt);
  }
  // Source-clock observations are not native, independent local death evidence.
  if (lease.mirror !== undefined || lease.mirrorLease !== undefined) return false;
  return deadlines.every(deadline => deadline <= cutoff);
};

interface FileCopy { relative: string; bytes: Buffer; generation: string; }
interface Candidate { directory: Directory; owner: string; files: FileCopy[]; reason: string; evidence?: string; stateKeys?: string[]; participant?: string; registry?: string; }
interface ArchivedFile { relative: string; sha256: string; size: number; generation: string; }
interface Intent { type: "intent"; id: string; directory: Directory; owner: string; files: ArchivedFile[]; reason: string; }
type Receipt = Intent | { type: "done" | "rolled-back" | "restoring"; id: string } | { type: "header"; format: 1; batch: string; mesh: string };
export interface ArchiveReport {
  mode: "dry-run" | "apply" | "restore";
  batch?: string;
  counts: Record<Directory, number>;
  examples: string[];
  retained: string[];
}
export interface ArchiveOptions {
  mesh: string;
  mode: "dry-run" | "apply" | "restore";
  batch?: string;
  /** Deterministic clock only; production CLI uses wall time. */
  now?: number;
  /** Test seam: outside every custody fence, after selection. Never executable CLI input. */
  afterSelection?: () => void | Promise<void>;
}
const counts = (): ArchiveReport["counts"] => ({ "host-leases": 0, participants: 0, "main-followups": 0, actors: 0 });
const batchName = (value: string): string => {
  if (!/^[A-Za-z0-9][A-Za-z0-9_-]{0,95}$/.test(value)) throw new Error("Invalid archive batch (one plain path component required)");
  return value;
};
const relativeName = (value: string): string => {
  if (typeof value !== "string" || value.includes("\\") || value.includes("\0") || path.isAbsolute(value) ||
      value.split("/").some(part => !part || part === "." || part === "..")) throw new Error("Unsafe archive path");
  return value;
};
/** Walk every ancestor with lstat; neither source nor archive may redirect outside ROOT. */
const noLinks = (file: string): void => {
  const absolute = path.resolve(file), parsed = path.parse(absolute);
  let current = parsed.root;
  for (const part of absolute.slice(parsed.root.length).split(path.sep).filter(Boolean)) {
    current = path.join(current, part);
    try { if (fs.lstatSync(current).isSymbolicLink()) throw new Error(`Symlink refused: ${current}`); }
    catch (error) { if (missing(error)) return; throw error; }
  }
};
const inside = (root: string, relative: string): string => {
  const file = path.join(root, ...relativeName(relative).split("/"));
  noLinks(file);
  return file;
};
const mkdir = (directory: string): void => { noLinks(directory); fs.mkdirSync(directory, { recursive: true, mode: 0o700 }); noLinks(directory); };
const generation = (stat: fs.BigIntStats): string => `${stat.dev}:${stat.ino}:${stat.size}:${stat.mtimeNs}:${stat.ctimeNs}`;
const readCopy = (root: string, relative: string): FileCopy | undefined => {
  const file = inside(root, relative);
  let fd: number;
  try { fd = fs.openSync(file, fs.constants.O_RDONLY | (fs.constants.O_NOFOLLOW ?? 0)); }
  catch (error) { if (missing(error)) return undefined; throw error; }
  try {
    const before = fs.fstatSync(fd, { bigint: true });
    if (!before.isFile()) throw new Error(`Not a regular file: ${file}`);
    const bytes = fs.readFileSync(fd);
    const after = fs.fstatSync(fd, { bigint: true });
    if (generation(before) !== generation(after) || generation(fs.lstatSync(file, { bigint: true })) !== generation(after)) {
      throw new Error(`File changed while reading: ${file}`);
    }
    return { relative, bytes, generation: generation(after) };
  } finally { fs.closeSync(fd); }
};
const names = (root: string, relative: string): string[] => {
  const directory = inside(root, relative);
  try { return fs.readdirSync(directory).sort(); }
  catch (error) { if (missing(error)) return []; throw error; }
};
const json = (copy: FileCopy | undefined): Record<string, unknown> | undefined => copy && object(JSON.parse(copy.bytes.toString("utf8")));
const exact = (before: FileCopy, after: FileCopy | undefined): boolean => !!after && before.generation === after.generation && before.bytes.equals(after.bytes);
const sameBytes = (copy: FileCopy | undefined, expected: ArchivedFile): boolean =>
  !!copy && copy.bytes.length === expected.size && hash(copy.bytes) === expected.sha256;
const archivedPath = (batch: string, file: ArchivedFile): string => {
  const [directory, ...tail] = relativeName(file.relative).split("/");
  if (!dirs.includes(directory as Directory) || !tail.length) throw new Error("Archive file outside allowed directories");
  return `archive/${directory}/${batch}/${tail.join("/")}`;
};
const sameInode = (root: string, left: string, right: string): boolean => {
  const a = fs.lstatSync(inside(root, left), { bigint: true });
  const b = fs.lstatSync(inside(root, right), { bigint: true });
  return a.isFile() && b.isFile() && a.dev === b.dev && a.ino === b.ino;
};

/** Gate refuses rather than tightening permissions: a dry-run must not chmod state. */
const openSqlite = (root: string): SqliteConnection => {
  for (const name of ["state.db", "state.db-wal", "state.db-shm"]) {
    const file = inside(root, name), stat = fs.lstatSync(file, { throwIfNoEntry: false });
    if (stat && (!stat.isFile() || (process.platform !== "win32" && ((stat.mode & 0o077) !== 0 ||
        (typeof process.getuid === "function" && stat.uid !== process.getuid()))))) throw new Error(`SQLite path is not private: ${name}`);
  }
  const { DatabaseSync } = createRequire(import.meta.url)("node:sqlite") as typeof import("node:sqlite");
  return openPrivateStateDb(inside(root, "state.db"), file => new DatabaseSync(file, { readOnly: true }) as unknown as SqliteConnection);
};

interface StateReadCache { copy?: FileCopy | undefined; raw?: Record<string, unknown> | undefined; }
/** Fresh read-only adapter, using canonical kv/meta decoding and the canonical
 * pinned private database gate, WITHOUT SqliteStateStore initialization or census.
 * A file-store snapshot is reused only after a fresh full generation check under
 * custody. Unchanged files must not be reread/parsed once per archived group. */
const stateView = (root: string, keys?: readonly string[], cache?: StateReadCache): { mesh: MeshStore; stamp: string } => {
  noLinks(path.join(root, "state.json"));
  const stat = fs.lstatSync(inside(root, "state.json"), { bigint: true, throwIfNoEntry: false });
  const unchanged = !!cache?.copy && !!stat && generation(stat) === cache.copy.generation;
  if (!unchanged) assertMeshStateReadable(root);
  const copy = unchanged ? cache!.copy : readCopy(root, "state.json"), raw = unchanged ? cache!.raw : json(copy);
  if (cache && !unchanged) { cache.copy = copy; cache.raw = raw; }
  const rootStat = fs.lstatSync(root, { bigint: true });
  let rows = object(raw?.entries), stamp = `file:${rootStat.dev}:${rootStat.ino}:${raw?.backendEpoch ?? 0}`;
  if (isMeshStateMovedMarker(raw)) {
    const db = openSqlite(root);
    try {
      db.exec("BEGIN");
      const meta = new Map(db.prepare("SELECT name, value FROM meta").all().map(row => [String(row.name), row.value]));
      if (Number(meta.get("schema")) !== 1 || meta.get("backend") !== "sqlite" || Number(meta.get("epoch")) !== raw.epoch ||
          typeof meta.get("store_id") !== "string" || !String(meta.get("store_id")) ||
          !Number.isSafeInteger(Number(meta.get("commit_no"))) || Number(meta.get("commit_no")) < 0) throw new Error("SQLite backend/epoch/schema identity mismatch");
      const stat = fs.lstatSync(inside(root, "state.db"), { bigint: true });
      // Identity/epoch, NOT global commit_no: unrelated commits are not owner renewals.
      stamp = `sqlite:${stat.dev}:${stat.ino}:${String(meta.get("store_id"))}:${raw.epoch}`;
      rows = {};
      const selectedRows = keys === undefined
        ? db.prepare("SELECT key, value, version, updated_at, updated_by FROM kv").all()
        : keys.flatMap(key => {
          const row = db.prepare("SELECT key, value, version, updated_at, updated_by FROM kv WHERE key = ?").get(key);
          return row ? [row] : [];
        });
      for (const row of selectedRows) {
        const key = String(row.key);
        Object.defineProperty(rows, key, { enumerable: true, configurable: true, value: { key,
          value: JSON.parse(String(row.value)), version: Number(row.version), updatedAt: Number(row.updated_at), updatedBy: JSON.parse(String(row.updated_by)) } });
      }
      if (!exact(copy!, readCopy(root, "state.json"))) throw new Error("SQLite moved marker changed during snapshot");
    } finally { try { db.exec("ROLLBACK"); } finally { db.close(); } }
  } else if (copy && (!raw || (raw.format !== 1 && raw.format !== 2) || !rows)) {
    throw new Error("Unknown/NATS state backend; archival refused");
  } else if (fs.existsSync(inside(root, "state.db"))) throw new Error("File/database coexistence requires a canonical moved marker");
  // Only selection enumerates the store. Commit rechecks decode direct canonical keys.
  const selectedEntries: Array<[string, unknown]> = keys === undefined ? Object.entries(rows ?? {})
    : keys.filter(key => Object.hasOwn(rows ?? {}, key)).map((key): [string, unknown] => [key, rows![key]]);
  const entries = selectedEntries.map(([key, entry]) => {
    const row = object(entry);
    if (!row || row.key !== key || !Number.isSafeInteger(row.version) || !finite(row.updatedAt) || typeof object(row.updatedBy)?.id !== "string") {
      throw new Error("Unknown mesh state entry; archival refused");
    }
    return row as unknown as MeshStateEntry;
  });
  const byKey = new Map(entries.map(entry => [entry.key, entry]));
  const mesh = { root, get: (key: string) => byKey.get(key) } as unknown as MeshStore;
  return { mesh, stamp };
};

type StateView = ReturnType<typeof stateView>;
const ownerKeys = (owner: string): string[] => [
  `topology/hosts/${hash(owner)}`, participantKey(owner),
  ...(owner.startsWith("session:") ? [`sessions/${owner.slice(8)}`, `topology/lineage-closures/${hash(owner)}`] : []),
];
const currentLease = (root: string, owner: string): { lease?: FabricHostLease; copy?: FileCopy } => {
  const copy = readCopy(root, `host-leases/${path.basename(hostLeasePath(root, owner))}`);
  if (!copy) return {};
  const raw = json(copy), lease = readHostLeaseCurrent(root, owner);
  if (!raw || raw.id !== owner || !lease || !exact(copy, readCopy(root, copy.relative)) ||
      ["session", "writer", "reloadUntil", "mirror", "mirrorLease"].some(key => raw[key] !== undefined &&
        (lease as unknown as Record<string, unknown>)[key] === undefined)) throw new Error("Malformed optional lease evidence; retain all");
  return { lease, copy };
};
const ownerEvidence = (state: StateView, keys: readonly string[], leaseCopy?: FileCopy): string => hash(JSON.stringify([
  state.stamp, keys.map(key => [key, state.mesh.get(key)]),
  leaseCopy && [leaseCopy.relative, leaseCopy.generation, hash(leaseCopy.bytes)],
]));

/** Exact owner only, like host-reaper and sessionActorRootGone. A live X whose
 * rootId happens to be S is not authority over S's independently owned entries. */
const ownerDead = (root: string, owner: string, now: number, state: StateView, lease?: FabricHostLease): boolean => {
  const cutoff = now - STALE_MESH_GRACE_MS;
  if (lease && !hostLeasePastGrace(lease, cutoff)) return false;
  const entry = state.mesh.get(`topology/hosts/${hash(owner)}`);
  if (entry) {
    const value = object(entry.value), session = object(value?.session);
    if (!value || value.id !== owner || !finite(value.expiresAt) || !finite(entry.updatedAt) || entry.updatedAt > cutoff ||
        hostEntryLiveness(entry, new Map<string, FabricHostLease>(lease ? [[owner, lease]] : [])).expiresAt > cutoff ||
        (value.session !== undefined && (!session || !finite(session.expiresAt) || !finite(session.updatedAt))) ||
        (value.remoteHost !== undefined && value.remoteHost !== os.hostname())) return false;
    for (const deadline of [value.reloadUntil, session?.expiresAt, session?.updatedAt]) {
      if (deadline !== undefined && (!finite(deadline) || deadline > cutoff)) return false;
    }
  }
  if (owner.startsWith("session:")) {
    const lineageAlive = (): boolean => {
      if (readCopy(root, `participants/${hash(owner)}.json`) || state.mesh.get(participantKey(owner)) ||
          state.mesh.get(`sessions/${owner.slice(8)}`)) return true;
      const closure = state.mesh.get(`topology/lineage-closures/${hash(owner)}`), receipt = object(closure?.value);
      return !(closure && receipt?.format === 1 && receipt.rootId === owner && receipt.ownerHostId === owner &&
        receipt.ownerIdentityId === owner && closure.updatedBy.id === owner && closure.updatedBy.kind === "main" && finite(receipt.closedAt));
    };
    noLinks(inside(root, `participants/${hash(owner)}.json`));
    noLinks(inside(root, `main-followups/${encodeURIComponent(owner.slice(8))}.owner.json`));
    return sessionActorRootGone(state.mesh, owner, lineageAlive, now) !== undefined;
  }
  const writer = lease?.writer;
  return !!lease && writer?.host === os.hostname() && Number.isSafeInteger(writer.pid) && writer.pid > 0 &&
    finite(writer.startedAt) && !censusRecordAlive(writer.pid, writer);
};

/** SQLite facts cannot supply positive death authority: generic same-uid state
 * writes are not lease-gated. Only a native token's independently dead LOCAL
 * writer qualifies; canonical claims/renewals share the held target lease gate. */
const nativeSqliteDeath = (lease: FabricHostLease | undefined): boolean => {
  const writer = lease?.writer;
  return !!lease && typeof lease.incarnationToken === "string" && lease.incarnationToken.length > 0 &&
    lease.mirror === undefined && lease.mirrorLease === undefined && writer?.lockProtocol === 3 && writer.host === os.hostname() &&
    Number.isSafeInteger(writer.pid) && writer.pid > 0 && finite(writer.startedAt) && !censusRecordAlive(writer.pid, writer);
};
const sqliteAuthorityReason = "SQLite candidate lacks token-bearing local protocol-3 lease with independently proven writer death";

const select = (root: string, now: number, retained: string[], cache: StateReadCache): Candidate[] => {
  const cutoff = now - STALE_MESH_GRACE_MS;
  const state = stateView(root, undefined, cache);
  const leases = new Map<string, FabricHostLease>();
  const leaseCopies = new Map<string, FileCopy>();
  // An unreadable/invalid lease might be a live replacement. It vetoes this entire scan.
  for (const name of names(root, "host-leases")) {
    if (!name.endsWith(".json")) continue;
    const copy = readCopy(root, `host-leases/${name}`), raw = json(copy);
    if (!copy || !raw || typeof raw.id !== "string" || path.basename(hostLeasePath(root, raw.id)) !== name) throw new Error("Unknown lease evidence; retain all");
    const current = currentLease(root, raw.id), lease = current.lease;
    if (!lease || !exact(copy, current.copy)) throw new Error("Lease changed during selection; retain all");
    leases.set(lease.id, lease); leaseCopies.set(lease.id, copy);
  }
  // Validate paths used internally by the existing death predicate before it reads them.
  for (const directory of ["participants", "main-followups"] as const) {
    for (const name of names(root, directory)) noLinks(inside(root, `${directory}/${name}`));
  }
  const pinned = new Set<string>(), candidates: Candidate[] = [];
  const registries: Array<{ session: string; copy: FileCopy }> = [];
  for (const session of names(root, "actors")) {
    const actorPath = inside(root, `actors/${session}`);
    if (!fs.lstatSync(actorPath).isDirectory()) continue;
    const copy = readCopy(root, `actors/${session}/actors.json`);
    if (!copy) continue;
    const raw = json(copy);
    // Nonempty private registries authenticate terminal directory rows (session-orphans.ts
    // validateTombstone/directorySettlement.{by,at,settledAt,retiredAt,retiredBy,persisted}).
    // Moving even a retired row would invalidate that proof. Pin its raw death evidence too.
    if (raw?.format !== 1 || !Array.isArray(raw.actors) || raw.actors.length !== 0) {
      pinned.add(`session:${session}`); retained.push(`actors/${session}: nonempty/unknown private registry retained with owner evidence`); continue;
    }
    if (names(root, `actors/${session}`).some(name => name !== "actors.json" && name !== "actors.json.lock")) {
      pinned.add(`session:${session}`); retained.push(`actors/${session}: traces/sidecars retained`); continue;
    }
    registries.push({ session, copy });
  }
  const deadOwners = new Map<string, boolean>();
  const dead = (owner: string): boolean => {
    if (!deadOwners.has(owner)) deadOwners.set(owner, !pinned.has(owner) && ownerDead(root, owner, now, state, leases.get(owner)));
    return deadOwners.get(owner)!;
  };
  for (const { session, copy } of registries) {
    if (dead(`session:${session}`)) candidates.push({ directory: "actors", owner: `session:${session}`, files: [copy],
      registry: session, reason: "empty ended-session registry (canonical directory shell retained for lock custody)" });
  }
  for (const name of names(root, "participants")) {
    if (!/^[a-f0-9]{64}\.json$/.test(name)) continue;
    const copy = readCopy(root, `participants/${name}`), raw = json(copy), value = object(raw?.value), by = object(raw?.updatedBy);
    if (!copy || raw?.format !== 1 || typeof value?.id !== "string" || raw.key !== participantKey(value.id) ||
        name !== `${hash(value.id)}.json` || !Number.isSafeInteger(raw.version) || !finite(raw.updatedAt) || raw.updatedAt > cutoff ||
        typeof value.ownerHostId !== "string" || value.ownerIdentityId !== value.ownerHostId || value.rootId !== value.ownerHostId ||
        by?.id !== value.ownerHostId || (value.remoteHost !== undefined && value.remoteHost !== os.hostname()) ||
        (value.reloadUntil !== undefined && (!finite(value.reloadUntil) || value.reloadUntil > cutoff)) ||
        (value.expiresAt !== undefined && (!finite(value.expiresAt) || value.expiresAt > cutoff)) || !dead(value.ownerHostId)) continue;
    const shared = state.mesh.get(String(raw.key));
    if (shared && (shared.updatedAt > cutoff || JSON.stringify(shared.value) !== JSON.stringify(raw.value))) continue;
    candidates.push({ directory: "participants", owner: value.ownerHostId, files: [copy], participant: String(raw.key), reason: "aged owner-linked participant; owner independently gone" });
  }
  for (const name of names(root, "main-followups")) {
    if (!name.endsWith(".owner.json")) continue;
    const copy = readCopy(root, `main-followups/${name}`), owner = json(copy);
    if (!copy || typeof owner?.sessionId !== "string" || name !== `${encodeURIComponent(owner.sessionId)}.owner.json` ||
        owner.rootId !== `session:${owner.sessionId}` || owner.ownerIdentityId !== owner.rootId || !dead(String(owner.rootId))) continue;
    const journal = readCopy(root, `main-followups/${encodeURIComponent(owner.sessionId)}.json`), raw = json(journal);
    // Closed shape: no unknown pending/nextTurn field may be discarded. Routes, successors,
    // rotations and delivered indices are NEVER selected, even for an empty journal.
    if (!journal || raw?.version !== 1 || !Array.isArray(raw.items) || raw.items.length ||
        Object.keys(raw).some(key => key !== "version" && key !== "items")) continue;
    if (fs.statSync(inside(root, copy.relative)).mtimeMs > cutoff || fs.statSync(inside(root, journal.relative)).mtimeMs > cutoff) continue;
    candidates.push({ directory: "main-followups", owner: String(owner.rootId), files: [copy, journal], reason: "dead Main and empty journal, paired" });
  }
  // Lease last: retain death evidence until dependent candidates have been revalidated.
  for (const [owner, lease] of leases) if (hostLeasePastGrace(lease, cutoff) && dead(owner)) {
    candidates.push({ directory: "host-leases", owner, files: [leaseCopies.get(owner)!], reason: "aged deadlines plus independently proven local writer/session death" });
  }
  // Pin exact owner facts/lease generation, not the store's global commit clock.
  // A previously absent S lease is pinned as absence and re-read under S's gate.
  return candidates.flatMap(candidate => {
    if (state.stamp.startsWith("sqlite:") && !nativeSqliteDeath(leases.get(candidate.owner))) {
      retained.push(`${candidate.files[0]!.relative}: ${sqliteAuthorityReason}`);
      return [];
    }
    const stateKeys = [...new Set([...ownerKeys(candidate.owner), ...(candidate.participant ? [candidate.participant] : [])])];
    return [{ ...candidate, stateKeys, evidence: ownerEvidence(state, stateKeys, leaseCopies.get(candidate.owner)) }];
  });
};

const manifestPath = (batch: string): string => `archive/manifests/${batch}.jsonl`;
/** writeSync may legally make partial progress. Neither a torn intent nor a failed
 * durability barrier authorizes a move. Offsets count bytes, not UTF-16 characters. */
const writeAll = (fd: number, bytes: Buffer): void => {
  for (let offset = 0; offset < bytes.byteLength;) {
    const written = fs.writeSync(fd, bytes, offset, bytes.byteLength - offset);
    if (!Number.isSafeInteger(written) || written <= 0 || written > bytes.byteLength - offset) {
      throw new Error("Archive receipt write made invalid/zero progress");
    }
    offset += written;
  }
};
const append = (root: string, batch: string, receipt: Receipt, exclusive = false): void => {
  // All nonexclusive appends run under existing file custody. Reading intents is
  // side-effect free; ONLY a custodial append may preserve/repair a torn tail.
  const file = inside(root, manifestPath(batch));
  mkdir(path.dirname(file));
  const fd = fs.openSync(file, exclusive ? "wx" : "a+", 0o600);
  try {
    if (!exclusive) {
      // Constant-size check, not a whole-manifest reread for every receipt.
      const size = fs.fstatSync(fd).size, tail = Buffer.alloc(1);
      if (!Number.isSafeInteger(size) || size <= 0 || fs.readSync(fd, tail, 0, 1, size - 1) !== 1) throw new Error("Invalid archive manifest tail");
      if (tail[0] !== 0x0a) intents(root, batch, true);
    }
    writeAll(fd, Buffer.from(`${JSON.stringify(receipt)}\n`, "utf8"));
    fs.fsyncSync(fd);
  } finally { fs.closeSync(fd); }
  syncDirectoryChain(path.dirname(file));
};
const intents = (root: string, batch: string, repairTailUnderCustody = false): { intents: Intent[]; completed: Set<string>; rolledBack: Set<string>; restoring: Set<string> } => {
  const copy = readCopy(root, manifestPath(batch));
  if (!copy) throw new Error(`Archive batch not found: ${batch}`);
  // Only newline-terminated records authorize work. Parse/validate EVERY complete
  // row before repairing a tail: interior corruption must remain a hard refusal.
  const completeEnd = copy.bytes.lastIndexOf(0x0a) + 1;
  const text = copy.bytes.subarray(0, completeEnd).toString("utf8");
  const rows = text.slice(0, -1).split("\n").map(line => JSON.parse(line) as Receipt);
  const header = rows.shift();
  if (header?.type !== "header" || header.format !== 1 || header.batch !== batch || header.mesh !== root) throw new Error("Manifest mesh/batch identity mismatch");
  const result: Intent[] = [], ids = new Set<string>(), completed = new Set<string>(), rolledBack = new Set<string>(), restoring = new Set<string>();
  for (const row of rows) {
    if (row.type === "intent") {
      if (!/^[a-f0-9-]{36}$/.test(row.id) || ids.has(row.id) || !dirs.includes(row.directory) || typeof row.owner !== "string" || !Array.isArray(row.files) || !row.files.length) throw new Error("Invalid archive intent");
      for (const file of row.files) {
        archivedPath(batch, file);
        if (!file.relative.startsWith(`${row.directory}/`) || !/^[a-f0-9]{64}$/.test(file.sha256) ||
            !Number.isSafeInteger(file.size) || file.size < 0 || typeof file.generation !== "string") throw new Error("Invalid archive file receipt");
      }
      // Manifests cannot turn this tool into an arbitrary mesh-file mover.
      if ((row.directory === "host-leases" && (row.files.length !== 1 || !/^host-leases\/[a-f0-9]{32}\.json$/.test(row.files[0]!.relative))) ||
          (row.directory === "participants" && (row.files.length !== 1 || !/^participants\/[a-f0-9]{64}\.json$/.test(row.files[0]!.relative))) ||
          (row.directory === "actors" && (row.files.length !== 1 || !/^actors\/[^/]+\/actors\.json$/.test(row.files[0]!.relative))) ||
          (row.directory === "main-followups" && (row.files.length !== 2 || !row.files[0]!.relative.endsWith(".owner.json") ||
            row.files[1]!.relative !== row.files[0]!.relative.replace(/\.owner\.json$/, ".json")))) throw new Error("Invalid archive group layout");
      ids.add(row.id); result.push(row);
    } else if ((row.type === "done" || row.type === "rolled-back" || row.type === "restoring") && ids.has(row.id)) {
      (row.type === "done" ? completed : row.type === "restoring" ? restoring : rolledBack).add(row.id);
    } else throw new Error("Invalid archive manifest record");
  }
  if (repairTailUnderCustody && completeEnd < copy.bytes.byteLength) {
    // Retain the ENTIRE original, including exact torn bytes and its complete
    // prefix, before shortening our own manifest. A failed backup/fsync leaves
    // the original untouched; retry may retain another copy, never delete one.
    const retained = inside(root, `archive/manifests/${batch}.retained-tail-${randomUUID()}`);
    const backup = fs.openSync(retained, "wx", 0o600);
    try { writeAll(backup, copy.bytes); fs.fsyncSync(backup); } finally { fs.closeSync(backup); }
    syncDirectoryChain(path.dirname(retained));
    const file = inside(root, manifestPath(batch)), fd = fs.openSync(file, fs.constants.O_RDWR | (fs.constants.O_NOFOLLOW ?? 0));
    try {
      if (generation(fs.fstatSync(fd, { bigint: true })) !== copy.generation ||
          !exact(copy, readCopy(root, manifestPath(batch)))) throw new Error("Manifest changed during tail recovery");
      fs.ftruncateSync(fd, completeEnd);
      fs.fsyncSync(fd);
    } finally { fs.closeSync(fd); }
    syncDirectoryChain(path.dirname(file));
  }
  return { intents: result, completed, rolledBack, restoring };
};

/** Same-filesystem no-replace move. link is an atomic EEXIST refusal; unlink removes
 * only a second name, NEVER the last copy of any bytes. No rm, recursive deletion,
 * serialization or trace rewriting. Custody excludes canonical publishers. */
const move = (root: string, source: string, destination: string, expected: ArchivedFile): void => {
  const from = inside(root, source), to = inside(root, destination);
  const copy = readCopy(root, source);
  if (!sameBytes(copy, expected)) throw new Error(`Byte identity changed: ${source}`);
  mkdir(path.dirname(to));
  fs.linkSync(from, to); // No overwrite, including a dangling symlink at destination.
  syncDirectoryChain(path.dirname(to));
  const current = readCopy(root, source), linked = readCopy(root, destination);
  const fromStat = fs.lstatSync(from, { bigint: true }), toStat = fs.lstatSync(to, { bigint: true });
  if (!current || !linked || !sameBytes(current, expected) || !sameBytes(linked, expected) ||
      fromStat.ino !== toStat.ino || fromStat.dev !== toStat.dev ||
      current.generation.split(":").slice(0, 2).join(":") !== copy!.generation.split(":").slice(0, 2).join(":")) throw new Error("Move generation changed; both copies retained");
  fs.unlinkSync(from);
  syncDirectoryChain(path.dirname(from));
};

/** Recover unfinished groups by rolling back, not by inventing death evidence from
 * archived files. Both-name crashes are resolved only when both names are the SAME
 * verified inode. A different canonical generation is always a collision. */
const rollback = (root: string, batch: string, intent: Intent): void => {
  for (const file of intent.files) {
    const destination = archivedPath(batch, file), source = readCopy(root, file.relative), archived = readCopy(root, destination);
    if (!archived) {
      if (!sameBytes(source, file) || source!.generation.split(":").slice(0, 2).join(":") !== file.generation.split(":").slice(0, 2).join(":")) throw new Error(`Recovery source changed: ${file.relative}`);
      continue;
    }
    if (!sameBytes(archived, file)) throw new Error(`Archive bytes changed: ${destination}`);
    if (!source) move(root, destination, file.relative, file);
    else {
      const a = fs.lstatSync(inside(root, file.relative), { bigint: true }), b = fs.lstatSync(inside(root, destination), { bigint: true });
      if (!sameBytes(source, file) || a.ino !== b.ino || a.dev !== b.dev) throw new Error(`Recovery collision: ${file.relative}`);
      fs.unlinkSync(inside(root, destination)); // duplicate name only; canonical bytes retained
      syncDirectoryChain(path.dirname(inside(root, destination)));
    }
  }
  append(root, batch, { type: "rolled-back", id: intent.id });
};

/** Canonical order: registry -> target lease custody -> file custody -> shared
 * mesh -> zero-wait participant key. Never prepare native identity under custody.
 * No MeshStore construction: no census registration, backend migration or janitor. */
const fences = async <T>(root: string, candidate: Pick<Candidate, "owner" | "participant" | "registry">,
  operation: () => T): Promise<T> => {
  const incarnation = await ownProcessIncarnation().catch(() => undefined);
  const meshLock = new MeshLock(root, { lockProtocol: 3 }, () => undefined);
  await meshLock.ownIncarnation;
  const leaseFile = hostLeasePath(root, candidate.owner), leaseHash = hash(path.basename(leaseFile));
  // Keep exactly MeshStore.leaseCustody's legacy-domain preference and short domain.
  const legacy = inside(root, `host-lease-commits/${leaseHash}`);
  const domain = fs.existsSync(legacy) ? legacy : inside(root, `host-lease-commits/${leaseHash.slice(0, 16)}`);
  for (const relative of ["custody.lock", ".lock", "host-lease-commits", "participants/.locks"]) noLinks(inside(root, relative));
  const lease = async (): Promise<T> => {
    noLinks(path.join(domain, "custody.lock"));
    const releaseLease = await acquireMeshCustodyLock(domain, 0, { ownIncarnation: incarnation, hostQualified: true, lockProtocol: 3 });
    try {
      const releaseCustody = await acquireMeshCustodyLock(root, 0, { ownIncarnation: incarnation, lockProtocol: 3 });
      try {
        return await meshLock.withLock(() => {
          // File custody only. SQLite writers do NOT share .lock; owner publications
          // are excluded by their lease gate, not by any SQLite write reservation.
          // In particular restore/recovery never opens the database.
          return candidate.participant
            ? withParticipantFileTryLock({ root, custody: async callback => callback() }, candidate.participant, incarnation, operation)
            : operation();
        }, 0, "custody", true);
      } finally { releaseCustody(); }
    } finally { releaseLease(); }
  };
  if (!candidate.registry) return lease();
  const actorRoot = inside(root, `actors/${candidate.registry}`);
  noLinks(path.join(actorRoot, "actors.json.lock"));
  return ActorRegistryStore.withLocks([new ActorRegistryStore(actorRoot)], lease, "archive-stale-mesh");
};
const groupFence = (intent: Intent): Pick<Candidate, "owner" | "participant" | "registry"> => ({ owner: intent.owner,
  ...(intent.directory === "participants" ? { participant: `topology/participants/${path.basename(intent.files[0]!.relative, ".json")}` } : {}),
  ...(intent.directory === "actors" ? { registry: intent.files[0]!.relative.split("/")[1]! } : {}),
});

export const archiveStaleMeshEntries = async (options: ArchiveOptions): Promise<ArchiveReport> => {
  if (!options.mesh || options.mesh.split(/[\\/]/).includes("..")) throw new Error("Explicit --mesh ROOT without traversal is required");
  const root = path.resolve(options.mesh); noLinks(root);
  if (!fs.lstatSync(root).isDirectory()) throw new Error("Mesh root must be an existing real directory");
  const report: ArchiveReport = { mode: options.mode, counts: counts(), examples: [], retained: [] };
  if (options.mode === "restore") {
    const batch = batchName(options.batch ?? ""); report.batch = batch;
    const manifest = intents(root, batch);
    // Preflight the ENTIRE batch before the first restoration. Repeat under custody.
    const preflight = (): void => {
      for (const intent of manifest.intents.filter(item => !manifest.rolledBack.has(item.id))) for (const file of intent.files) {
        const source = readCopy(root, file.relative), archived = readCopy(root, archivedPath(batch, file));
        const restoring = manifest.restoring.has(intent.id);
        const restoredCanonical = restoring && !!source && !archived && sameBytes(source, file) &&
          source.generation.split(":").slice(0, 2).join(":") === file.generation.split(":").slice(0, 2).join(":");
        const crashAfterLink = restoring && !!source && !!archived && sameBytes(source, file) && sameBytes(archived, file) &&
          sameInode(root, file.relative, archivedPath(batch, file));
        if (source && !restoredCanonical && !crashAfterLink) throw new Error(`Restore collision: ${file.relative}`);
        if (!source && !sameBytes(archived, file)) throw new Error(`Restore byte identity mismatch: ${file.relative}`);
      }
    };
    preflight();
    // Leases first, then dependent records (reverse of apply).
    for (const intent of [...manifest.intents].reverse()) {
      if (manifest.rolledBack.has(intent.id)) continue;
      await fences(root, groupFence(intent), () => {
        for (const file of intent.files) {
          const source = readCopy(root, file.relative), archived = readCopy(root, archivedPath(batch, file));
          if (source && !manifest.restoring.has(intent.id)) throw new Error(`Restore collision: ${file.relative}`);
          if (!source && !sameBytes(archived, file)) throw new Error(`Restore byte identity mismatch: ${file.relative}`);
        }
        append(root, batch, { type: "restoring", id: intent.id });
        rollback(root, batch, intent);
        report.counts[intent.directory] += intent.files.length;
      });
    }
    return report;
  }
  if (options.mode !== "apply" && options.mode !== "dry-run") throw new Error("Explicit dry-run, apply or restore mode required");
  if (options.mode === "apply") {
    // Recover before selecting anything. Completed groups stay archived; partial
    // groups return byte-for-byte to canonical names, then qualify afresh.
    for (const name of names(root, "archive/manifests")) {
      if (!name.endsWith(".jsonl")) continue;
      const batch = batchName(name.slice(0, -6)), manifest = intents(root, batch);
      for (const intent of manifest.intents) if ((!manifest.completed.has(intent.id) || manifest.restoring.has(intent.id)) && !manifest.rolledBack.has(intent.id)) {
        await fences(root, groupFence(intent), () => rollback(root, batch, intent));
      }
    }
  }
  const now = options.now ?? Date.now();
  if (!finite(now)) throw new Error("Invalid archival clock");
  const stateCache: StateReadCache = {};
  const selected = select(root, now, report.retained, stateCache);
  await options.afterSelection?.();
  if (options.mode === "dry-run") {
    for (const candidate of selected) {
      report.counts[candidate.directory] += candidate.files.length;
      report.examples.push(...candidate.files.map(file => `${file.relative}: ${candidate.reason}`));
    }
    report.examples = report.examples.slice(0, 20);
    return report;
  }
  if (!selected.length) return report;
  const batch = batchName(options.batch ?? `${new Date(now).toISOString().replace(/[^0-9TZ]/g, "")}_${randomUUID()}`);
  report.batch = batch;
  // Exclusive batch manifest: an explicit existing batch never gets overwritten.
  append(root, batch, { type: "header", format: 1, mesh: root, batch }, true);
  for (const candidate of selected) {
    await fences(root, candidate, () => {
      // Native claim/renew/publication holds this SAME owner lease gate through
      // its awaited state write (#withLeaseFence -> withOwnedHostLease). Generic
      // SQLite writers do not share the gate and remain free to commit. Their
      // facts are fresh vetoes, NOT positive process-death authority.
      const moved = new Set<string>();
      const files: ArchivedFile[] = candidate.files.map(file => ({ relative: file.relative, sha256: hash(file.bytes), size: file.bytes.length, generation: file.generation }));
      const still = (): boolean => {
        // Short read-only snapshot; ROLLBACK/close precede every receipt or move.
        const state = stateView(root, candidate.stateKeys!, stateCache), current = currentLease(root, candidate.owner);
        const registry = candidate.owner.startsWith("session:")
          ? readCopy(root, `actors/${candidate.owner.slice(8)}/actors.json`) : undefined;
        const actors = json(registry);
        const pinned = !!registry && (actors?.format !== 1 || !Array.isArray(actors.actors) || actors.actors.length !== 0);
        return (!state.stamp.startsWith("sqlite:") || nativeSqliteDeath(current.lease)) && !pinned &&
          ownerEvidence(state, candidate.stateKeys!, current.copy) === candidate.evidence &&
          candidate.files.every((file, index) => {
            if (!moved.has(file.relative)) return exact(file, readCopy(root, file.relative));
            const archived = readCopy(root, archivedPath(batch, files[index]!));
            return !readCopy(root, file.relative) && sameBytes(archived, files[index]!) &&
              archived!.generation.split(":").slice(0, 2).join(":") === file.generation.split(":").slice(0, 2).join(":");
          }) &&
          ownerDead(root, candidate.owner, options.now ?? Date.now(), state, current.lease) &&
          (!candidate.registry || !names(root, `actors/${candidate.registry}`).some(name => name !== "actors.json" && name !== "actors.json.lock"));
      };
      if (!still()) { report.retained.push(`${candidate.files[0]!.relative}: generation/death evidence changed before commit`); return; }
      const intent: Intent = { type: "intent", id: randomUUID(), directory: candidate.directory, owner: candidate.owner,
        reason: candidate.reason, files };
      for (const file of intent.files) if (readCopy(root, archivedPath(batch, file))) throw new Error("Archive destination collision");
      append(root, batch, intent);
      // Manifest durability can take time. Fresh veto/death reads AFTER its fsync
      // and before EACH move, with the SQLite reader already closed. Keep the
      // paired inbox owner (positive file-backend death evidence) canonical until
      // its journal has moved. Own prior moves are verified in archive, not
      // mistaken for a vanished/replaced source or dropped death evidence.
      const order = candidate.directory === "main-followups" ? [...intent.files].reverse() : intent.files;
      for (const file of order) {
        if (!still()) {
          rollback(root, batch, intent);
          report.retained.push(`${candidate.files[0]!.relative}: generation/death evidence changed after intent fsync/before move`);
          return;
        }
        move(root, file.relative, archivedPath(batch, file), file);
        moved.add(file.relative);
      }
      append(root, batch, { type: "done", id: intent.id });
      report.counts[candidate.directory] += candidate.files.length;
      if (report.examples.length < 20) report.examples.push(...candidate.files.map(file => file.relative));
    });
  }
  return report;
};

export const archiveStaleMeshEntriesCli = async (argv: readonly string[]): Promise<ArchiveReport> => {
  let mesh: string | undefined, mode: ArchiveOptions["mode"] | undefined, batch: string | undefined;
  for (let index = 0; index < argv.length; index++) {
    const arg = argv[index];
    if (arg === "--mesh" && mesh === undefined) mesh = argv[++index];
    else if ((arg === "--dry-run" || arg === "--apply") && mode === undefined) mode = arg === "--apply" ? "apply" : "dry-run";
    else if (arg === "--restore" && mode === undefined) { mode = "restore"; batch = argv[++index]; }
    else throw new Error(`Unknown/duplicate argument: ${arg}`);
  }
  if (!mesh || mesh.startsWith("--") || !mode || (mode === "restore" && !batch)) {
    throw new Error("Usage: archive-stale-mesh-entries --mesh ROOT (--dry-run | --apply | --restore BATCH)");
  }
  return archiveStaleMeshEntries({ mesh, mode, ...(batch ? { batch } : {}) });
};
