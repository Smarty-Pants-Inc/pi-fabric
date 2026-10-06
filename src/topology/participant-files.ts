import { randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { ownProcessIncarnation, processIncarnation, validProcessIncarnation, readFileRetrying, renameAtomic, AtomicFileWriter } from "../core/atomic-write.js";
import type { MeshStateEntry } from "../mesh/store.js";

// Participant records outside the shared state (smarty-dev#2004). Each record lived in the one
// shared state.json, so every participant change rewrote the whole file under the mesh lock, and
// every process re-read and parsed all of it. Each record now also lives in a file of its own
// under participants/, replaced by an atomic rename without the lock; only its owner host writes
// it (a reaper removes the files of hosts gone for hours). A reader re-reads only the files whose
// metadata changed, and none while the directory is unchanged.

/**
 * Under the liveness policy (LIVENESS_POLICY_KEY) with `participants: "files"`, set by the fleet
 * owner once every runtime reads these files, records are written only here and hosts remove
 * their records from the shared state. Before that, hosts write both (older runtimes read only
 * the shared state).
 * ponytail: until that switch every committed record is written twice, and the lock and state
 * size are unchanged; the gain comes with the switch, as for host leases (#68).
 */
export const participantFilesOnly = (policy: unknown): boolean =>
  typeof policy === "object" && policy !== null &&
  (policy as { version?: unknown }).version === 1 &&
  (policy as { participants?: unknown }).participants === "files";

const DIR = "participants";
const PREFIX = "topology/participants/";
const NAME = /^[0-9a-f]{64}\.json$/;

// A record's file is named for its shared-state key: the key's hash part plus ".json".
const fileOf = (meshRoot: string, key: string): string | undefined => {
  const name = key.startsWith(PREFIX) ? `${key.slice(PREFIX.length)}.json` : "";
  return NAME.test(name) ? path.join(meshRoot, DIR, name) : undefined;
};

/**
 * Replaces a record's file if `decide`, given the file's current entry (read fresh), returns the
 * entry to write. Decisions on one key are serialized by a per-key lock, so a takeover or removal
 * judged on an earlier read cannot overwrite a newer owner (review/astra F1 on #142). The lock is
 * per record, not the mesh lock: routine owner updates never wait on other keys.
 */
/** The store whose lock serializes the rare recovery of a dead holder's per-key lock. */
export interface ParticipantFileMesh {
  readonly root: string;
  exclusive<T>(operation: () => T): Promise<T>;
}

export const writeParticipantFileIf = async (
  mesh: ParticipantFileMesh,
  key: string,
  decide: (current: MeshStateEntry | undefined) => MeshStateEntry | undefined,
  options: { durable?: boolean } = {},
): Promise<boolean> => {
  const file = fileOf(mesh.root, key);
  if (!file) throw new Error(`Not a participant key: ${key}`);
  return withKeyLock(mesh, file, () => {
    const entry = decide(readFresh(file));
    if (!entry) return false;
    if (entry.key !== key) throw new Error(`Participant entry key mismatch: ${entry.key}`);
    // Presence lapses/rebuilds after a crash; migration callers still request barriers.
    new AtomicFileWriter(file).write(JSON.stringify({ format: 1, ...entry }), options);
    if (options.durable && JSON.stringify(readFresh(file)) !== JSON.stringify(entry)) {
      throw new Error(`Participant migration verification failed: ${key}`);
    }
    rescan(path.dirname(file));
    return true;
  });
};

/** Removes a record's file if `decide` accepts its current entry; under the same per-key lock. */
export const removeParticipantFileIf = async (
  mesh: ParticipantFileMesh,
  key: string,
  decide: (current: MeshStateEntry) => boolean,
): Promise<boolean> => {
  const file = fileOf(mesh.root, key);
  if (!file) return false;
  return withKeyLock(mesh, file, () => {
    const current = readFresh(file);
    if (!current || !decide(current)) return false;
    fs.rmSync(file, { force: true });
    rescan(path.dirname(file));
    return true;
  });
};

/** Writes a changed record (tests and tools; runtimes use writeParticipantFileIf). */
export const writeParticipantFile = (meshRoot: string, entry: MeshStateEntry): void => {
  const file = fileOf(meshRoot, entry.key);
  if (!file) throw new Error(`Not a participant key: ${entry.key}`);
  new AtomicFileWriter(file).write(JSON.stringify({ format: 1, ...entry }));
  rescan(path.dirname(file));
};

// This process sees its own writes at once: the next read scans again (parsed files are kept).
const rescan = (dir: string): void => {
  const known = cache.get(dir);
  if (known) known.scannedAt = Number.NEGATIVE_INFINITY;
};

const readFresh = (file: string): MeshStateEntry | undefined => {
  let text: string;
  try {
    text = readFileRetrying(file);
  } catch (error) {
    if ((error as { code?: unknown }).code === "ENOENT") return undefined;
    throw error;                                            // unknown, not absent: decide nothing
  }
  return entryOf(text, path.basename(file));
};

const LOCK_WAIT_MS = 5_000;

const holderAlive = async (owner: string): Promise<boolean> => {
  const [pidText, startTime, token] = owner.split("\n");
  const pid = Number(pidText);
  if (!Number.isSafeInteger(pid) || pid <= 0) return false;
  try {
    process.kill(pid, 0);
  } catch (error) {
    // Only ESRCH proves absence. Other probe failures still need native reuse evidence.
    if ((error as { code?: unknown }).code === "ESRCH") return false;
  }
  // Read once: an unreadable or foreign/torn identity proves nothing about PID reuse.
  if (!owner.endsWith("\n") || !token || !validProcessIncarnation(startTime)) return true;
  const actual = await processIncarnation(pid);
  return actual === undefined || actual === startTime;
};

// A per-key lock: participants/.locks/<hash>, a directory created with its owner record inside by
// one rename, so a lock never exists without its owner. A live holder is never taken over, however
// long it holds the lock (a resumed holder would write on an earlier decision); a waiter then times
// out and its refresh retries. A dead holder's lock is removed under the mesh lock, after reading
// its owner again, so concurrent recoveries cannot remove a successor's lock (review/astra round 3
// on #142). Recovery is rare; routine writes never take the mesh lock.
const withKeyLock = async <T>(mesh: ParticipantFileMesh, file: string, operation: () => T): Promise<T> => {
  const locks = path.join(mesh.root, DIR, ".locks");
  const lock = path.join(locks, path.basename(file, ".json"));
  fs.mkdirSync(locks, { recursive: true, mode: 0o700 });
  const token = `${process.pid}\n${await ownProcessIncarnation() ?? ""}\n${randomUUID()}\n`;
  const deadline = Date.now() + LOCK_WAIT_MS;
  for (;;) {
    const staging = `${lock}.${process.pid}.${randomUUID()}.tmp`;
    fs.mkdirSync(staging, { mode: 0o700 });
    fs.writeFileSync(path.join(staging, "owner"), token, { mode: 0o600 });
    try {
      fs.renameSync(staging, lock);                         // fails while another lock holds the name
      break;
    } catch (error) {
      fs.rmSync(staging, { recursive: true, force: true });
      const code = (error as { code?: unknown }).code;
      if (code !== "EEXIST" && code !== "ENOTEMPTY" && code !== "EPERM" && code !== "EACCES") throw error;
      await recoverDeadKeyLock(mesh, lock);
      if (Date.now() >= deadline) throw new Error(`Timed out waiting for the participant file lock ${lock}`);
      await new Promise((resolve) => setTimeout(resolve, 5));
    }
  }
  try {
    return operation();
  } finally {
    // Unlinking the owner before rmdir leaves an empty canonical directory that a successor
    // can replace on POSIX. Detach our whole lock first; recursive cleanup touches only it.
    const tombstone = `${lock}.${randomUUID()}.dead`;
    // A sibling read/scanner can briefly deny a Windows directory rename. Keep
    // the unique target and bounded retry; never fall back to deleting the lock.
    renameAtomic(lock, tombstone);
    fs.rmSync(tombstone, { recursive: true, force: true });
  }
};

const readOwner = (lock: string): string | undefined => {
  try {
    return fs.readFileSync(path.join(lock, "owner"), "utf8");
  } catch {
    return undefined;
  }
};

const recoverDeadKeyLock = async (mesh: ParticipantFileMesh, lock: string): Promise<void> => {
  const seen = readOwner(lock);
  if (seen === undefined || await holderAlive(seen)) return;
  await mesh.exclusive(() => {
    // Compare, then delete by a rename to a unique name and a second compare: a lock that is not
    // the one judged dead goes back (it cannot be, while recoveries share the mesh lock).
    if (readOwner(lock) !== seen) return;
    const tombstone = `${lock}.${randomUUID()}.dead`;
    // A sibling read/scanner can briefly deny a Windows directory rename. Keep
    // the unique target and bounded retry; never fall back to deleting the lock.
    renameAtomic(lock, tombstone);
    if (readOwner(tombstone) === seen) fs.rmSync(tombstone, { recursive: true, force: true });
    else renameAtomic(tombstone, lock);
  }); // A mesh timeout unwinds the publication fence; retry on the next refresh.
};

const deepFreeze = <T>(value: T): T => {
  if (typeof value === "object" && value !== null && !Object.isFrozen(value)) {
    Object.freeze(value);
    for (const child of Object.values(value)) deepFreeze(child);
  }
  return value;
};

const entryOf = (text: string, name: string): MeshStateEntry | undefined => {
  try {
    const value = JSON.parse(text) as Record<string, unknown>;
    const updatedBy = value?.updatedBy as Record<string, unknown> | undefined;
    if (
      value?.format !== 1 ||
      value.key !== `${PREFIX}${name.slice(0, -5)}` ||
      typeof value.updatedAt !== "number" ||
      typeof value.version !== "number" ||
      typeof updatedBy !== "object" || updatedBy === null || typeof updatedBy.id !== "string"
    ) return undefined;
    const { format: _format, ...entry } = value;
    return deepFreeze(entry) as unknown as MeshStateEntry;           // shared with every reader
  } catch {
    return undefined;
  }
};

interface Slot { ino: number; mtimeMs: number; size: number; entry: MeshStateEntry | undefined }
// complete: every file in the listing was read. An incomplete scan is never reused as current: a
// file that failed to read is retried by the next read (review/astra F2 on #142).
interface DirCache {
  slots: Map<string, Slot>; mtimeMs: number; ino: number; scannedAt: number; entries: MeshStateEntry[]; complete: boolean;
}

// Parsed files by directory, reused while their metadata is unchanged.
const cache = new Map<string, DirCache>();
// A directory whose mtime is at least this old when scanned cannot change again within the same
// timestamp tick, so an unchanged mtime later means an unchanged listing. Windows does not update
// a directory's mtime reliably on a replace; it always scans.
const DIR_TICK_MS = 100;

const readSlot = (
  dir: string, name: string, stat: fs.Stats, slot: Slot | undefined,
): { slot: Slot | undefined; read: boolean } => {
  if (slot && slot.ino === stat.ino && slot.mtimeMs === stat.mtimeMs && slot.size === stat.size) return { slot, read: true };
  let text: string;
  try {
    text = readFileRetrying(path.join(dir, name));
  } catch {
    return { slot, read: false };                           // unreadable for now: keep the last answer
  }
  return { slot: { ino: stat.ino, mtimeMs: stat.mtimeMs, size: stat.size, entry: entryOf(text, name) }, read: true };
};

/**
 * Every participant file's entry. Callers must not mutate the entries: they are shared with the
 * cache (the shared-state reads return clones; these are read far more often).
 */
export const readParticipantFiles = (
  meshRoot: string,
  options: { maxAgeMs?: number } = {},
): readonly MeshStateEntry[] => {
  const dir = path.join(meshRoot, DIR);
  // A listing may reuse a scan this recent even if files changed since, like the shared state's
  // read cache (RUNTIME_MESH_READ_CACHE_MS): a busy fleet changes some record several times a second.
  const recent = cache.get(dir);
  if (recent?.complete && options.maxAgeMs && Date.now() - recent.scannedAt < options.maxAgeMs) return recent.entries;
  let dirStat: fs.Stats;
  try {
    dirStat = fs.statSync(dir);
  } catch {
    cache.delete(dir);
    return [];
  }
  const known = cache.get(dir);
  if (
    known?.complete && process.platform !== "win32" && known.ino === dirStat.ino && known.mtimeMs === dirStat.mtimeMs &&
    known.scannedAt - known.mtimeMs >= DIR_TICK_MS
  ) return known.entries;
  const scannedAt = Date.now();
  let names: string[];
  try {
    names = fs.readdirSync(dir);
  } catch {
    return known?.entries ?? [];
  }
  const slots = new Map<string, Slot>();
  const entries: MeshStateEntry[] = [];
  let complete = true;
  for (const name of names) {
    if (!NAME.test(name)) continue;
    let stat: fs.Stats;
    try {
      stat = fs.statSync(path.join(dir, name));
    } catch (error) {
      if ((error as { code?: unknown }).code !== "ENOENT") complete = false;   // else removed since the listing
      continue;
    }
    const { slot, read } = readSlot(dir, name, stat, known?.slots.get(name));
    complete &&= read;
    if (!slot) continue;
    slots.set(name, slot);
    if (slot.entry) entries.push(slot.entry);
  }
  // An unchanged listing keeps its array, so callers can key their own caches on it.
  const same = known !== undefined && known.entries.length === entries.length &&
    known.entries.every((entry, index) => entry === entries[index]);
  const listed = same ? known.entries : entries;
  cache.set(dir, { slots, mtimeMs: dirStat.mtimeMs, ino: dirStat.ino, scannedAt, entries: listed, complete });
  return listed;
};

/** One record's file entry, from the same cache; for a single-participant lookup. */
export const readParticipantFile = (meshRoot: string, key: string): MeshStateEntry | undefined => {
  const file = fileOf(meshRoot, key);
  if (!file) return undefined;
  let stat: fs.Stats;
  try {
    stat = fs.statSync(file);
  } catch {
    return undefined;
  }
  const dir = path.dirname(file);
  const name = path.basename(file);
  const known = cache.get(dir);
  const { slot, read } = readSlot(dir, name, stat, known?.slots.get(name));
  // Only refresh the slot; the directory listing is rebuilt by the next full read.
  if (slot && read && known) known.slots.set(name, slot);
  return slot?.entry;
};

/**
 * Whether anything may be at a record's file: true unless it is known absent (ENOENT). For
 * guards that must fail closed, such as the bridge's native checks (security pass S3 on #142):
 * an unreadable or invalid file still counts as a native's.
 */
export const participantFilePresent = (meshRoot: string, key: string): boolean => {
  const file = fileOf(meshRoot, key);
  if (!file) return false;
  try {
    fs.statSync(file);
    return true;
  } catch (error) {
    return (error as { code?: unknown }).code !== "ENOENT";
  }
};

/**
 * Removes staging and tombstone directories of the per-key locks older than `olderThanMs`: a
 * process that died between creating one and renaming or removing it leaves it (security pass S5).
 */
export const sweepParticipantLockLeftovers = (
  mesh: ParticipantFileMesh, olderThanMs: number, now = Date.now(),
): Promise<void> => mesh.exclusive(() => {
  // Share recovery's lock for the entire scan/removal: never unlink the owner of
  // a detached lock between recovery's rename and its compare/restore decision.
  const locks = path.join(mesh.root, DIR, ".locks");
  let names: string[];
  try {
    names = fs.readdirSync(locks);
  } catch {
    return;
  }
  for (const name of names) {
    if (!name.endsWith(".tmp") && !name.endsWith(".dead")) continue;
    try {
      if (now - fs.statSync(path.join(locks, name)).mtimeMs > olderThanMs) {
        fs.rmSync(path.join(locks, name), { recursive: true, force: true });
      }
    } catch {
      // Removed meanwhile.
    }
  }
});

/** Changes whenever a participant file is added, replaced or removed (not on Windows: see above). */
export const participantFilesStamp = (meshRoot: string): string | undefined => {
  try {
    const stat = fs.statSync(path.join(meshRoot, DIR));
    return `${stat.ino}:${stat.mtimeMs}`;
  } catch {
    return undefined;
  }
};

/**
 * The directory stamp of the listing that reads now return: undefined unless the last scan was
 * complete. A reader records what it consumed, not what is on disk (as cachedStateStamp, #84).
 */
export const participantFilesCachedStamp = (meshRoot: string): string | undefined => {
  const known = cache.get(path.join(meshRoot, DIR));
  // A scan within the directory's timestamp tick may have missed a change in that same tick, which
  // would not move the stamp: report no stamp, so the reader's gate stays open until a later scan.
  return known?.complete && known.scannedAt - known.mtimeMs >= DIR_TICK_MS ? `${known.ino}:${known.mtimeMs}` : undefined;
};
