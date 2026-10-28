import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { ownProcessIncarnation, processIncarnation, validProcessIncarnation, readFileRetrying, renameAtomic, AtomicFileWriter, MeshLockTimeoutError,
  namespaceBarrierAvailable } from "../core/atomic-write.js";
import type { MeshStateEntry } from "../mesh/store.js";

// Participant records outside the shared state (smarty-dev#2004). Each record lived in the one
// shared state.json, so every participant change rewrote the whole file under the mesh lock, and
// every process re-read and parsed all of it. Native records now live in per-record files.

/**
 * Under the liveness policy (LIVENESS_POLICY_KEY) with `participants: "files"`, set by the fleet
 * owner once every runtime reads these files, records are written only here and hosts remove their
 * records from the shared state. Before that, hosts write both (older runtimes read only shared state).
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
 * entry to write. Decisions on one key are serialized by a per-key lock. Additions also take the
 * namespace gate; updates release it before waiting on the key so one contended file cannot stall
 * healthy heartbeats or unrelated participant writers.
 */
/** The store whose file custody lock serializes the rare recovery of a dead holder's per-key
 * lock and the sweep (smarty-dev#6477 L5: MeshStore.custody, not the shared mesh lock alone). */
export interface ParticipantFileMesh {
  readonly root: string;
  custody<T>(operation: () => T, lockTimeoutMs?: number): Promise<T>;
}

/** Prepared unknown is distinct from an absent (lazy) identity. */
export interface ParticipantFileLockOptions {
  ownIncarnation?: string | undefined;
  /** No native holder read or ordinary key wait: registry custody and the independent zero-wait
   * renewal lane both require this prepared mode. */
  registryFenced?: boolean;
  /** One-shot acquisition used while the namespace gate is held; contention releases the gate. */
  tryOnly?: boolean;
  /** Runs the actual decision/write under the publisher's lease custody. */
  withCommitFence?: <T>(operation: () => T | Promise<T>) => Promise<T>;
}

export class ParticipantFileLockBusyError extends MeshLockTimeoutError {
  constructor(lock: string) { super(` participant file lock ${lock}`, 1, 0); }
}

/** A bounded (LOCK_WAIT_MS) key or namespace wait that ran out. A plain Error, so participant
 * writers keep their existing handling; directory custody converts it (below). */
class ParticipantFileLockWaitTimeout extends Error {
  constructor(readonly lock: string, readonly attempts: number, readonly maxGapMs: number, readonly lockWaitMs: number) {
    super(`Timed out waiting for the participant file lock ${lock}`);
  }
}

/** smarty-dev#13645 (re-audit P1-1): a mirror's directory custody that waited out LOCK_WAIT_MS behind
 * another holder (another link's state-changing pass holds the namespace across its batches). That
 * is contention, not a permanent refusal: a MeshLockTimeoutError, so the bridge backs off and retries
 * instead of stopping the link. */
export class ParticipantFileCustodyTimeoutError extends MeshLockTimeoutError {
  constructor(readonly lock: string, attempts: number, maxGapMs: number, lockWaitMs?: number) {
    super(` participant file lock ${lock}`, attempts, maxGapMs, lockWaitMs);
    if (typeof lockWaitMs === "number" && Number.isFinite(lockWaitMs) && lockWaitMs >= 0) {
      this.message += `; lockWaitMs=${Math.round(lockWaitMs)}`;
    }
    this.name = "ParticipantFileCustodyTimeoutError";
  }
}

export const writeParticipantFileIf = async (
  mesh: ParticipantFileMesh,
  key: string,
  decide: (current: MeshStateEntry | undefined) => MeshStateEntry | undefined,
  options: ParticipantFileLockOptions & { durable?: boolean } = {},
): Promise<boolean> => {
  const file = fileOf(mesh.root, key);
  if (!file) throw new Error(`Not a participant key: ${key}`);
  // Every write decides under the hold that writes it. `decide` reads authority outside the file
  // (publication fences, registry lineage, lease liveness) that must be judged in the same atomic step
  // as the write (#7817), so a decision made under an earlier, released hold is never reused, even
  // when the file's bytes are unchanged.
  const write = (requireSameIdentity = false): boolean | typeof RETRY_ADDITION => {
    const current = readFresh(file);
    const entry = decide(current);
    if (!entry) return false;
    if (entry.key !== key) throw new Error(`Participant entry key mismatch: ${entry.key}`);
    if (requireSameIdentity && (identityOf(current) === undefined || identityOf(current) !== identityOf(entry))) return RETRY_ADDITION;
    new AtomicFileWriter(file).write(JSON.stringify({ format: 1, ...entry }),
      options.durable === undefined ? {} : { durable: options.durable });
    if (options.durable && JSON.stringify(readFresh(file)) !== JSON.stringify(entry)) {
      throw new Error(`Participant migration verification failed: ${key}`);
    }
    rescan(path.dirname(file));
    return true;
  };
  // smarty-dev#13645: an update of an existing record whose identity fields stay the same needs
  // only its own key receipt. A directory-custody holder (the mesh bridge) holds the receipts of
  // every record that can alias its incoming ids, and additions and identity changes still take
  // the namespace gate below, so unrelated heartbeats never queue behind a mirror's custody.
  if (fs.existsSync(file)) {
    const result = await withKeyLock(mesh, file, () => fs.existsSync(file) ? write(true) : RETRY_ADDITION, options);
    if (result !== RETRY_ADDITION) return result;
  }
  for (;;) {
    // Additions and identity changes take the namespace gate before their per-key receipt. Existing
    // entries release that short check before waiting on their key.
    const { withCommitFence: _withCommitFence, ...namespaceOptions } = options;
    const namespace = await withParticipantNamespaceLock(mesh, async () => {
      if (fs.existsSync(file)) return { present: true as const };
      try {
        return { present: false as const, result: await withKeyLock(mesh, file, () => write() as boolean, { ...options, tryOnly: true }) };
      } catch (error) {
        if (!(error instanceof ParticipantFileLockBusyError)) throw error;
        return { present: false as const, contended: true as const };
      }
    }, namespaceOptions);
    if (!namespace.present && !("contended" in namespace)) return namespace.result;
    const result = await withKeyLock(mesh, file, () => fs.existsSync(file) ? write() : RETRY_ADDITION, options);
    if (result !== RETRY_ADDITION) return result;
  }
};

const RETRY_ADDITION = Symbol("participant-file-addition-raced-removal");

/** The identity fields a mirror's participant snapshot keys admission and collisions on. */
const identityOf = (entry: MeshStateEntry | undefined): string | undefined => {
  const value = entry?.value;
  if (typeof value !== "object" || value === null || Array.isArray(value)) return undefined;
  const record = value as Record<string, unknown>;
  return JSON.stringify([record.id, record.rootId, record.ownerHostId, record.ownerIdentityId, record.name, record.sessionId]);
};

/** Removes a record under its per-key receipt; the namespace gate is only for additions. */
export const removeParticipantFileIf = async (
  mesh: ParticipantFileMesh,
  key: string,
  decide: (current: MeshStateEntry) => boolean,
  options: ParticipantFileLockOptions = {},
): Promise<boolean> => {
  const file = fileOf(mesh.root, key);
  if (!file) return false;
  return withKeyLock(mesh, file, () => {
    const current = readFresh(file);
    if (!current || !decide(current)) return false;
    fs.rmSync(file, { force: true });
    rescan(path.dirname(file));
    return true;
  }, options);
};

/** Writes a changed record (tests and tools; runtimes use writeParticipantFileIf, which takes the
 * key receipt, and the namespace gate for additions, that a mirror's directory custody holds). As on
 * main this has no receipt of its own: one atomic replacement, none for byte-identical bytes. */
export const writeParticipantFile = (meshRoot: string, entry: MeshStateEntry): void => {
  writeParticipantFileUnderCustody(meshRoot, entry);
};

/** Synchronous file replacement for a caller already holding the namespace and key receipts. */
export const writeParticipantFileUnderCustody = (meshRoot: string, entry: MeshStateEntry): void => {
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

// A per-key lock: participants/.locks/<hash>, a directory created with its owner record inside by
// one rename, so a lock never exists without its owner. A live holder is never taken over, however
// long it holds the lock (a resumed holder would write on an earlier decision); a waiter then times
// out and its refresh retries. A dead holder's lock is removed under the mesh lock, after reading
// its owner again, so concurrent recoveries cannot remove a successor's lock (review/astra round 3
// on #142). Recovery is rare; routine writes never take the mesh lock.
const holderAlive = async (owner: string, lock: string, registryFenced = false): Promise<boolean> => {
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
  if (registryFenced) throw new ParticipantFileLockBusyError(lock);
  const actual = await processIncarnation(pid);
  return actual === undefined || actual === startTime;
};

/** One directory fsync, no chain walk: the lock directories already exist. Skipped where there is no directory fsync
 * (namespaceBarrierAvailable: Windows; NTFS journals the names). */
const syncDirectoryEntries = (directory: string): void => {
  if (!namespaceBarrierAvailable()) return;
  const fd = fs.openSync(directory, fs.constants.O_RDONLY);
  try { fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
};

// A per-key lock receipt is atomically renamed into place with its owner record.
const keyReceipt = (mesh: ParticipantFileMesh, file: string, incarnation: string | undefined) => {
  const locks = file === namespaceReceiptFile(mesh) ? namespaceLockDirectory(mesh) : path.join(mesh.root, DIR, ".locks");
  const lock = path.join(locks, path.basename(file, ".json"));
  fs.mkdirSync(locks, { recursive: true, mode: 0o700 });
  const token = `${process.pid}\n${incarnation ?? ""}\n${randomUUID()}\n`;
  const release = (): void => {
    const tombstone = `${lock}.${randomUUID()}.dead`;
    renameAtomic(lock, tombstone);
    fs.rmSync(tombstone, { recursive: true, force: true });
  };
  return {
    lock,
    acquire: (): void => {
      // A held lock is refused before any staging or fsync, as its rename would be: waiters retry every 5 ms (withKeyLock),
      // and three fsyncs per refused attempt made contention cost real disk time (pi-fabric#1562: 5 s waits under fake
      // timers ran ~1,000 attempts and timed out on ZFS). Only a fast path: the rename below still decides.
      if (fs.existsSync(lock)) throw Object.assign(new Error(`Participant lock is held: ${lock}`), { code: "EEXIST" });
      const staging = `${lock}.${process.pid}.${randomUUID()}.tmp`;
      fs.mkdirSync(staging, { mode: 0o700 });
      try {
        // Durable before it becomes visible (pi-fabric#1562, fabric-store P2): a host crash must never leave a published lock
        // with an empty or torn owner, which recovery cannot judge and so never clears. The owner's bytes, then its name in
        // the staging directory, reach the disk before the rename publishes them.
        const fd = fs.openSync(path.join(staging, "owner"), "wx", 0o600);
        try { fs.writeFileSync(fd, token); fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
        syncDirectoryEntries(staging);
        fs.renameSync(staging, lock);
      } catch (error) {
        fs.rmSync(staging, { recursive: true, force: true });
        throw error;
      }
      // Then the published name. If that sync fails, give the lock back rather than hold it while reporting an error.
      try { syncDirectoryEntries(locks); }
      catch (error) {
        try { release(); } catch { /* left to recovery: our pid ends, and a dead holder is cleared */ }
        throw error;
      }
    },
    release,
  };
};

/** Synchronous zero-wait key custody for registry lineage commits. Adoption shares this receipt
 * with independent renewal, closing the token-check/rename race. */
export const withParticipantFileTryLock = <T>(
  mesh: ParticipantFileMesh, key: string, ownIncarnation: string | undefined, operation: () => T,
): T => {
  const file = fileOf(mesh.root, key);
  if (!file) throw new Error(`Not a participant key: ${key}`);
  const receipt = keyReceipt(mesh, file, ownIncarnation);
  try { receipt.acquire(); }
  catch (error) {
    const code = (error as NodeJS.ErrnoException).code;
    if (["EEXIST", "ENOTEMPTY", "EPERM", "EACCES"].includes(code ?? "")) throw new ParticipantFileLockBusyError(receipt.lock);
    throw error;
  }
  try { return operation(); }
  finally { receipt.release(); }
};

const namespaceReceiptFile = (mesh: ParticipantFileMesh): string => path.join(mesh.root, DIR, ".namespace.json");
/** smarty-dev#13645: the namespace receipt lives OUTSIDE participants/. A mirror takes it on every
 * state-changing pass, and keeping it under participants/.locks created participants/ on a mesh that
 * has no participant file yet, so readers watching for that directory's creation never saw it. The
 * per-key receipts stay in participants/.locks, which only exists once a participant file does. */
const NAMESPACE_LOCK_DIR = "participant-namespace";
const namespaceLockDirectory = (mesh: ParticipantFileMesh): string => path.join(mesh.root, NAMESPACE_LOCK_DIR);

/** Additions take the namespace receipt before per-key custody. */
const withParticipantNamespaceLock = <T>(mesh: ParticipantFileMesh, operation: () => T | Promise<T>,
  options: ParticipantFileLockOptions = {}): Promise<T> => withKeyLock(mesh, namespaceReceiptFile(mesh), operation, options);

/** Stabilize additions and the selected existing records in key order. The ID filter excludes
 * unrelated native heartbeat files while still locking every entry that can alias this mirror. */
export const withParticipantFileDirectoryCustody = async <T>(mesh: ParticipantFileMesh,
  operation: () => T | Promise<T>, relevantIds?: ReadonlySet<string>): Promise<T> => {
  // Only this custody's own acquisitions are converted; the operation's errors pass through unchanged.
  let entered = false;
  try {
    return await participantFileDirectoryCustody(mesh, () => { entered = true; return operation(); }, relevantIds);
  } catch (error) {
    if (!entered && error instanceof ParticipantFileLockWaitTimeout) {
      throw new ParticipantFileCustodyTimeoutError(error.lock, error.attempts, error.maxGapMs, error.lockWaitMs);
    }
    throw error;
  }
};

const participantFileDirectoryCustody = <T>(mesh: ParticipantFileMesh,
  operation: () => T | Promise<T>, relevantIds?: ReadonlySet<string>): Promise<T> => withParticipantNamespaceLock(mesh, async () => {
    const directory = path.join(mesh.root, DIR);
    let names: string[];
    try { names = fs.readdirSync(directory).filter(name => NAME.test(name)).sort(); }
    catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") names = []; else throw error; }
    const relevantFiles = relevantIds === undefined ? undefined : new Set<string>();
    if (relevantFiles && relevantIds) {
      for (const id of relevantIds) {
        const key = `${PREFIX}${createHash("sha256").update(id).digest("hex")}`;
        const file = fileOf(mesh.root, key);
        if (file) relevantFiles.add(path.basename(file));
      }
      for (const name of names) {
        const entry = readFresh(path.join(directory, name));
        if (entry && typeof entry.value === "object" && entry.value !== null) {
          const value = entry.value as Record<string, unknown>;
          if ([value.id, value.rootId, value.ownerHostId, value.ownerIdentityId].some(id => typeof id === "string" && relevantIds.has(id))) {
            relevantFiles.add(name);
          }
        }
      }
    }
    const files = names.filter(name => relevantFiles === undefined || relevantFiles.has(name)).map(name => path.join(directory, name));
    const acquire = (index: number): Promise<T> => index === files.length
      ? Promise.resolve().then(operation)
      : withKeyLock(mesh, files[index]!, () => acquire(index + 1));
    return acquire(0);
  });

/** Synchronous form for already-prepared publication fences; namespace first, then sorted keys. */
export const withParticipantFilesTryLock = <T>(mesh: ParticipantFileMesh, keys: readonly string[],
  ownIncarnation: string | undefined, operation: () => T): T => {
  const files = [...new Set(keys.map(key => fileOf(mesh.root, key)))];
  if (files.some(file => !file)) throw new Error("Not a participant key");
  const receipts = [keyReceipt(mesh, namespaceReceiptFile(mesh), ownIncarnation),
    ...files.map(file => keyReceipt(mesh, file!, ownIncarnation)).sort((a, b) => a.lock.localeCompare(b.lock))];
  const acquired: ReturnType<typeof keyReceipt>[] = [];
  try {
    for (const receipt of receipts) {
      try { receipt.acquire(); }
      catch (error) {
        const code = (error as NodeJS.ErrnoException).code;
        if (["EEXIST", "ENOTEMPTY", "EPERM", "EACCES"].includes(code ?? "")) throw new ParticipantFileLockBusyError(receipt.lock);
        throw error;
      }
      acquired.push(receipt);
    }
    return operation();
  } finally { for (const receipt of acquired.reverse()) receipt.release(); }
};

const withKeyLock = async <T>(
  mesh: ParticipantFileMesh,
  file: string,
  operation: () => T | Promise<T>,
  options: ParticipantFileLockOptions = {},
): Promise<T> => {
  const prepared = Object.prototype.hasOwnProperty.call(options, "ownIncarnation");
  if (options.registryFenced && !prepared) throw new ParticipantFileLockBusyError(file);
  const incarnation = prepared ? options.ownIncarnation : await ownProcessIncarnation();
  const receipt = keyReceipt(mesh, file, incarnation);
  const waitStarted = performance.now();
  const deadline = Date.now() + LOCK_WAIT_MS;
  let attempts = 0;
  let maxGapMs = 0;
  let lastAttemptAt = Date.now();
  for (;;) {
    const attemptAt = Date.now();
    maxGapMs = Math.max(maxGapMs, attemptAt - lastAttemptAt);
    lastAttemptAt = attemptAt;
    attempts += 1;
    try { receipt.acquire(); break; }
    catch (error) {
      const code = (error as NodeJS.ErrnoException).code;
      if (code !== "EEXIST" && code !== "ENOTEMPTY" && code !== "EPERM" && code !== "EACCES") throw error;
      if (options.tryOnly) throw new ParticipantFileLockBusyError(receipt.lock);
      await recoverDeadKeyLock(mesh, receipt.lock, options.registryFenced);
      if (options.registryFenced && fs.existsSync(receipt.lock)) throw new ParticipantFileLockBusyError(receipt.lock);
      if (Date.now() >= deadline) throw new ParticipantFileLockWaitTimeout(receipt.lock, attempts, maxGapMs, Math.max(0, performance.now() - waitStarted));
      await new Promise(resolve => setTimeout(resolve, 5));
    }
  }
  try { return options.withCommitFence ? await options.withCommitFence(operation) : await operation(); }
  finally { receipt.release(); }
};

const readOwner = (lock: string): string | undefined => {
  try {
    const value = fs.readFileSync(path.join(lock, "owner"), "utf8");
    return value.endsWith("\n") && value.split("\n").length === 4 ? value : undefined;
  } catch { return undefined; }
};

/** An owner that cannot be read (empty or torn: only a host crash on a build without the owner fsync can leave one) is
 * never judged dead here. The operator step (docs/participants-cli.md): confirm no process holds the lock, then rename
 * the lock directory aside to `<lock>.<UTC stamp>.operator-dead` (never delete it). */
const recoverDeadKeyLock = async (mesh: ParticipantFileMesh, lock: string, registryFenced = false): Promise<void> => {
  let owner = readOwner(lock);
  if (!owner || await holderAlive(owner, lock, registryFenced)) return;
  await mesh.custody(async () => {
    const current = readOwner(lock);
    if (current !== owner || await holderAlive(current, lock, registryFenced)) return;
    const tombstone = `${lock}.${randomUUID()}.dead`;
    try {
      renameAtomic(lock, tombstone);
      fs.rmSync(tombstone, { recursive: true, force: true });
    } catch (error) {
      // Windows can briefly deny the directory rename; leave it for the next retry.
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    }
  }, 0);
};

/** Recover only this adoption key before taking registry/mesh custody. */
export const prepareParticipantFileLock = async (mesh: ParticipantFileMesh, key: string): Promise<void> => {
  const file = fileOf(mesh.root, key);
  if (!file) throw new Error(`Not a participant key: ${key}`);
  await recoverDeadKeyLock(mesh, path.join(mesh.root, DIR, ".locks", path.basename(file, ".json")));
};

/** Recover a dead holder's namespace receipt (participant-namespace/.namespace) before taking
 * registry/mesh custody. Every fenced addition and every withParticipantFilesTryLock takes this receipt
 * first, and a fenced holder probe reports any holder whose PID is live or unprobeable (reused PID,
 * EPERM) as busy, so only an unfenced native check like this one can clear it (fabric-store review of
 * pi-fabric#1562, P1). Same receipt comparison under file custody as the per-key recovery. */
export const prepareParticipantNamespaceLock = (mesh: ParticipantFileMesh): Promise<void> =>
  // keyReceipt's own lock path, without its mkdir: preparation creates nothing.
  recoverDeadKeyLock(mesh, path.join(namespaceLockDirectory(mesh), path.basename(namespaceReceiptFile(mesh), ".json")));

/** Retry preparation only: fresh native key-holder evidence/recovery occurs outside
 * registry custody. Recovery compares the exact receipt under file custody;
 * no participant/source selection crosses this wait. The namespace receipt comes first, in
 * acquisition order; it lives outside participants/, so it is recovered even when no
 * participant file (and so no participants/.locks) exists yet. */
export const prepareParticipantFileLocks = async (mesh: ParticipantFileMesh): Promise<void> => {
  await prepareParticipantNamespaceLock(mesh);
  const locks = path.join(mesh.root, DIR, ".locks");
  let names: fs.Dirent[];
  try { names = fs.readdirSync(locks, { withFileTypes: true }); }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return; throw error; }
  for (const name of names) {
    if (name.isDirectory() && /^[0-9a-f]{64}$/.test(name.name)) await recoverDeadKeyLock(mesh, path.join(locks, name.name));
  }
};

const deepFreeze = <T>(value: T): T => {
  if (typeof value === "object" && value !== null && !Object.isFrozen(value)) {
    Object.freeze(value);
    for (const child of Object.values(value)) deepFreeze(child);
  }
  return value;
};

/**
 * Removes staging and tombstone directories of the per-key locks older than `olderThanMs`: a
 * process that died between creating one and renaming or removing it leaves it (security pass S5).
 */
export const sweepParticipantLockLeftovers = (
  mesh: ParticipantFileMesh, olderThanMs: number, now = Date.now(),
): Promise<void> => mesh.custody(() => {
  // Share recovery's lock for the entire scan/removal: never unlink the owner of
  // a detached lock between recovery's rename and its compare/restore decision.
  for (const locks of [path.join(mesh.root, DIR, ".locks"), namespaceLockDirectory(mesh)]) {
    let names: string[];
    try { names = fs.readdirSync(locks); }
    catch { continue; }
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
  }
});

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

/** Stable directory/file metadata used by external snapshot revalidation. */
export const participantFileSnapshot = (meshRoot: string): { stamp: string; entries: readonly MeshStateEntry[] } => {
  const entries = readParticipantFiles(meshRoot, { maxAgeMs: 0 });
  return { stamp: participantFilesStamp(meshRoot) ?? "missing", entries };
};
