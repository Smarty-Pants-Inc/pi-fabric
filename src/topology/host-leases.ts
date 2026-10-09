import { createHash, randomUUID } from "node:crypto";
import os from "node:os";
import fs from "node:fs";
import path from "node:path";
import { MeshLockTimeoutError, ownProcessIncarnation, processIncarnation, validProcessIncarnation, readFileRetrying, writeJsonAtomic } from "../core/atomic-write.js";
import { effectiveLiveness, type Liveness } from "./liveness.js";
import type { MeshStateEntry } from "../mesh/store.js";

// Host lease renewals outside the shared state (smarty-dev#816). Every heartbeat rewrote the
// whole shared state under the one mesh lock, and heartbeats were 78% of all locked writes. Each
// host renews its own file without the mesh lock. A tiny per-lease publish mutex
// serializes ALL writers (including takeover/removal), not unrelated hosts.

/**
 * Host-reserved historical policy key (also used for participant-file migration).
 * Directory liveness now negotiates file-only renewals through livenessLeaseFiles: 1;
 * a live older peer restores only the legacy session cadence; the host record keeps this policy cadence.
 */
export const LIVENESS_POLICY_KEY = "topology/liveness";
/**
 * Historical compatibility interval, still used by the bridge for mirrored state records.
 * Native all-capable directories need no periodic state renewal: reapers read file liveness.
 */
export const STATE_LEASE_RENEW_MS = 10 * 60 * 1000;

const LEASE_DIR = "host-leases";

export const DEFAULT_PARTICIPANT_LEASE_GRACE_MS = 45_000;
export const PARTICIPANT_LEASE_WAIT_MS = 10_000;

/** Routing grace only: this never changes a writer's TTL or consumer admission. */
export const participantLeaseGraceMs = (override?: number): number => {
  const value = override ?? (process.env.PI_FABRIC_PARTICIPANT_LEASE_GRACE_MS === undefined
    ? DEFAULT_PARTICIPANT_LEASE_GRACE_MS : Number(process.env.PI_FABRIC_PARTICIPANT_LEASE_GRACE_MS));
  return Number.isFinite(value) && value >= 0 ? Math.min(300_000, Math.floor(value)) : DEFAULT_PARTICIPANT_LEASE_GRACE_MS;
};

export class FabricParticipantStaleError extends Error {
  override readonly name = "FabricParticipantStaleError";
  readonly code = "FABRIC_PARTICIPANT_STALE";
  readonly retryable = true;
  constructor(readonly targetId: string, readonly lapsedMs: number, readonly idempotencyKey?: string) {
    super(`Fabric participant ${targetId}: lease late by ${Math.ceil(Math.max(0, lapsedMs) / 1000)} s; retry` +
      (idempotencyKey ? ` once with the same idempotencyKey (${idempotencyKey}); delivery outcome is not yet known.` : " once; the session is not proven ended."));
  }
}

export interface RoutingLeaseWaitOptions {
  graceMs?: number;
  waitMs?: number;
  pollMs?: number;
  now?: () => number;
  sleep?: (ms: number) => Promise<void>;
  /** Testable lock-wait signal; production checks the mesh lock without taking it. */
  lockWaiting?: () => boolean;
}

/** Only file observations while waiting; callers capture/fence shared ownership once. */
export const waitForHostLeaseRenewal = async (
  id: string,
  readExpiry: () => number,
  options: RoutingLeaseWaitOptions = {},
): Promise<void> => {
  const now = options.now ?? Date.now;
  const sleep = options.sleep ?? (ms => new Promise(resolve => setTimeout(resolve, ms)));
  const configuredWait = options.waitMs ?? PARTICIPANT_LEASE_WAIT_MS;
  let remaining = Number.isFinite(configuredWait) ? Math.min(PARTICIPANT_LEASE_WAIT_MS, Math.max(0, configuredWait)) : PARTICIPANT_LEASE_WAIT_MS;
  const deadline = now() + remaining;
  const configuredPoll = options.pollMs ?? 100;
  const pollMs = Number.isFinite(configuredPoll) ? Math.max(1, configuredPoll) : 100;
  for (;;) {
    const at = now(), expiresAt = readExpiry();
    if (expiresAt >= at) return;
    if (at >= deadline || remaining <= 0) throw new FabricParticipantStaleError(id, at - expiresAt);
    const delay = Math.min(pollMs, deadline - at, remaining);
    await sleep(delay);
    remaining -= delay; // A backward wall-clock adjustment must not make the wait unbounded.
  }
};

export interface MeshWriterRecord {
  pid: number;
  host: string;
  /** Build/release commit SHA; the advisory census reports "unknown" as an unknown writer. */
  releaseSha: string;
  lockProtocol: number;
  stateBackend: "file" | "shadow" | "sqlite" | string;
  startedAt: number;
}

export const meshWriterLeaseRecord = (lockProtocol: number, stateBackend: string, startedAt = Math.floor(Date.now() - process.uptime() * 1000)): MeshWriterRecord => ({
  pid: process.pid, host: os.hostname(),
  releaseSha: process.env.PI_FABRIC_RELEASE_SHA ?? process.env.PI_FABRIC_BUILD_SHA ?? process.env.GITHUB_SHA ?? "unknown",
  lockProtocol, stateBackend, startedAt,
});

export interface FabricHostLease {
  id: string;
  rootId: string;
  identityId: string;
  updatedAt: number;
  expiresAt: number;
  /** Incarnation fence; absent on pre-lease-capability writers. */
  startedAt?: number;
  /** Root Main's explicit bounded reload handoff; absent on ordinary heartbeats and older writers. */
  reloadUntil?: number;
  /** Main session has a fixed 15 s TTL, independent of the host TTL. */
  session?: Liveness & { id: string; startedAt: number };
  /** Writer census metadata, absent on leases from pre-census releases. */
  writer?: MeshWriterRecord;
}

const fileName = (hostId: string): string =>
  createHash("sha256").update(hostId).digest("hex").slice(0, 32) + ".json";

/** Canonical own-host path, shared by peers and the residency launcher watchdog. */
export const hostLeasePath = (meshRoot: string, hostId: string): string =>
  path.join(meshRoot, LEASE_DIR, fileName(hostId));

export const hostLeasePublishLockPath = (meshRoot: string, hostId: string): string =>
  `${hostLeasePath(meshRoot, hostId)}.publish.lock`;

export class HostLeasePublishLockBusyError extends MeshLockTimeoutError {
  constructor(readonly lock: string) { super(` host lease publish lock ${lock}`, 1, 0); }
}

interface PublishOwner { pid: number; startTime: string | null; token: string }
const readPublishOwner = (lock: string): PublishOwner | undefined => {
  try {
    const stat = fs.lstatSync(lock);
    if (!stat.isFile() || stat.nlink !== 1) return undefined;
    const value = JSON.parse(fs.readFileSync(lock, "utf8")) as PublishOwner;
    return Number.isSafeInteger(value.pid) && value.pid > 0 && typeof value.token === "string" && value.token.length > 0 &&
      (value.startTime === null || typeof value.startTime === "string") ? value : undefined;
  } catch { return undefined; }
};

// Linux's native start identity is a cheap file read. Other platforms use the
// caller's prepared native identity; UNKNOWN can never prove reuse of a live pid.
let ownStartTime: string | undefined;
const publishStartTime = (): string | undefined => {
  if (ownStartTime !== undefined) return ownStartTime;
  if (process.platform === "linux") {
    try {
      const stat = fs.readFileSync(`/proc/${process.pid}/stat`, "utf8");
      const value = stat.slice(stat.lastIndexOf(")") + 2).trim().split(/\s+/)[19];
      if (validProcessIncarnation(value)) ownStartTime = value;
    } catch { /* Unknown is conservative. */ }
  }
  return ownStartTime;
};

/** Zero-wait exclusive publication. No await, sleep, or age-based recovery here.
 * All lease publishers/removers share this fence; callers unwind custody before admission. */
export const acquireHostLeasePublishLock = (meshRoot: string, hostId: string): (() => void) => {
  const lock = hostLeasePublishLockPath(meshRoot, hostId);
  fs.mkdirSync(path.dirname(lock), { recursive: true, mode: 0o700 });
  const owner: PublishOwner = { pid: process.pid, startTime: publishStartTime() ?? null, token: randomUUID() };
  let fd: number;
  try { fd = fs.openSync(lock, "wx", 0o600); }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code === "EEXIST") throw new HostLeasePublishLockBusyError(lock);
    throw error;
  }
  try { fs.writeFileSync(fd, JSON.stringify(owner)); }
  catch (error) {
    fs.closeSync(fd);
    // A torn receipt is UNKNOWN, not reclaimable by age or unconditional unlink.
    if (readPublishOwner(lock)?.token === owner.token) fs.unlinkSync(lock);
    throw error;
  }
  fs.closeSync(fd);
  return () => {
    // A delayed release is never permission to unlink a successor's receipt.
    try { if (readPublishOwner(lock)?.token === owner.token) fs.unlinkSync(lock); }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
  };
};

export const withHostLeasePublishLock = <T>(meshRoot: string, hostId: string, operation: () => T): T => {
  const release = acquireHostLeasePublishLock(meshRoot, hostId);
  try { return operation(); } finally { release(); }
};

/** Rare crash recovery is serialized by existing file custody, OUTSIDE registry
 * custody. Two reclaimers cannot compare the dead token then unlink a fresh holder.
 * Only ESRCH or a different native process start identity is proof; never age. */
type LeasePublishCustody = { root: string; custody<T>(operation: () => T): Promise<T> };
const preparePublishLock = async (mesh: LeasePublishCustody, lock: string): Promise<void> => {
  const seen = readPublishOwner(lock);
  ownStartTime ??= publishStartTime() ?? await ownProcessIncarnation();
  if (!seen) return;
  let dead = false;
  try { process.kill(seen.pid, 0); }
  catch (error) { dead = (error as NodeJS.ErrnoException).code === "ESRCH"; }
  if (!dead && validProcessIncarnation(seen.startTime ?? undefined)) {
    const actual = await processIncarnation(seen.pid);
    dead = actual !== undefined && actual !== seen.startTime;
  }
  if (!dead) return;
  await mesh.custody(() => {
    const current = readPublishOwner(lock);
    if (current?.token === seen.token && current.pid === seen.pid && current.startTime === seen.startTime) fs.unlinkSync(lock);
  });
};

export const prepareHostLeasePublishLock = (mesh: LeasePublishCustody, hostId: string): Promise<void> =>
  preparePublishLock(mesh, hostLeasePublishLockPath(mesh.root, hostId));

/** Bridge removal/outbox replay can have no live presence id. Prepare only
 * actual publish-lock receipts, not another shared-state authority snapshot. */
export const prepareHostLeasePublishLocks = async (mesh: LeasePublishCustody): Promise<void> => {
  const directory = path.join(mesh.root, LEASE_DIR);
  let names: string[];
  try { names = fs.readdirSync(directory); }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return; throw error; }
  await Promise.all(names.filter(name => /^[a-f0-9]{32}\.json\.publish\.lock$/.test(name))
    .map(name => preparePublishLock(mesh, path.join(directory, name))));
};

/** Subscribe to a real lease-publisher release after an admission timeout.
 * No immediate absence probe: already-absent is not a NEW admission event. */
export const onHostLeasePublication = (meshRoot: string, hostId: string, wake: () => void): (() => void) => {
  const lock = hostLeasePublishLockPath(meshRoot, hostId);
  try {
    let expected = readPublishOwner(lock)?.token;
    const watcher = fs.watch(path.dirname(lock), { persistent: false }, (event, name) => {
      if (event !== "rename" || name !== null && String(name) !== path.basename(lock)) return;
      const current = readPublishOwner(lock);
      if (current && current.pid !== process.pid) expected = current.token;
      // Our own ordinary renewal release is NOT an admission event.
      if (!fs.existsSync(lock) && expected !== undefined) { expected = undefined; wake(); }
    });
    watcher.on("error", () => watcher.close());
    return () => watcher.close();
  } catch { return () => {}; /* Once-per-minute regular-tick fallback only. */ }
};

/** Passive lock-release admission, bounded by the caller's existing abort budget.
 * Watch first, then check absence to cover release during watch setup. No timer/poll. */
export const waitForHostLeasePublication = (meshRoot: string, hostId: string, signal: AbortSignal): Promise<void> => {
  const lock = hostLeasePublishLockPath(meshRoot, hostId);
  return new Promise<void>((resolve, reject) => {
    let watcher: fs.FSWatcher | undefined;
    const finish = (error?: unknown): void => {
      watcher?.close(); signal.removeEventListener("abort", abort);
      if (error !== undefined) reject(error); else resolve();
    };
    const abort = (): void => finish(signal.reason);
    const check = (): void => { if (!fs.existsSync(lock)) finish(); };
    try {
      signal.throwIfAborted();
      watcher = fs.watch(path.dirname(lock), { persistent: false }, (_event, name) => {
        if (name === null || String(name) === path.basename(lock)) check();
      });
      watcher.on("error", finish);
      signal.addEventListener("abort", abort, { once: true });
      check();
    } catch (error) { finish(error); }
  });
};

/** Unconditional successor/mirror publication still takes the SAME exclusive lock.
 * Busy is a typed admission failure, never a spin or a clobber. */
export const writeHostLease = (meshRoot: string, lease: FabricHostLease): void =>
  withHostLeasePublishLock(meshRoot, lease.id, () => writeJsonAtomic(hostLeasePath(meshRoot, lease.id), { format: 1, ...lease }));

/** Renew only the current incarnation; initial absence is a no-clobber create.
 * False means superseded. "skipped" means another publisher holds the lock: next tick retries. */
export const writeHostLeaseIfCurrent = (meshRoot: string, lease: FabricHostLease, allowMissing = false): boolean | "skipped" => {
  let release: () => void;
  try { release = acquireHostLeasePublishLock(meshRoot, lease.id); }
  catch (error) { if (error instanceof HostLeasePublishLockBusyError) return "skipped"; throw error; }
  const file = hostLeasePath(meshRoot, lease.id);
  const temporary = path.join(path.dirname(file), `.renew-${randomUUID()}.tmp`);
  const ours = (current: FabricHostLease | undefined): boolean => current !== undefined &&
    current.id === lease.id && current.rootId === lease.rootId &&
    current.identityId === lease.identityId && current.startedAt === lease.startedAt;
  let fd: number | undefined;
  try {
    try { fd = fs.openSync(file, fs.constants.O_RDONLY | (fs.constants.O_NOFOLLOW ?? 0)); }
    catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
      if (!allowMissing) return false;
    }
    if (fd !== undefined) {
      const opened = fs.fstatSync(fd, { bigint: true });
      if (!opened.isFile() || !ours(leaseOf(fs.readFileSync(fd, "utf8"), fileName(lease.id)))) return false;
      let current: fs.BigIntStats;
      try { current = fs.lstatSync(file, { bigint: true }); }
      catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return false; throw error; }
      if (!current.isFile() || current.dev !== opened.dev || current.ino !== opened.ino ||
        current.size !== opened.size || current.mtimeNs !== opened.mtimeNs || current.ctimeNs !== opened.ctimeNs) return false;
    }
    // Check + stage + rename are indivisible to ALL cooperating lease publishers.
    writeJsonAtomic(temporary, { format: 1, ...lease });
    if (fd === undefined) {
      try { fs.linkSync(temporary, file); }
      catch (error) { if ((error as NodeJS.ErrnoException).code === "EEXIST") return false; throw error; }
    } else fs.renameSync(temporary, file);
    // Even an out-of-protocol replacement after rename supersedes us; never restore.
    return ours(readHostLeaseCurrent(meshRoot, lease.id));
  } finally {
    if (fd !== undefined) fs.closeSync(fd);
    fs.rmSync(temporary, { force: true });
    release();
  }
};

export const removeHostLease = (meshRoot: string, hostId: string): void =>
  withHostLeasePublishLock(meshRoot, hostId, () => fs.rmSync(hostLeasePath(meshRoot, hostId), { force: true }));

/** Exact ownership is re-read under the SAME publish lock immediately before unlink.
 * Invalid/absent files are kept. A busy removal must unwind into existing admission. */
export const removeHostLeaseIf = (meshRoot: string, hostId: string, owned: (lease: FabricHostLease) => boolean): boolean =>
  withHostLeasePublishLock(meshRoot, hostId, () => {
    const current = readHostLeaseCurrent(meshRoot, hostId);
    if (!current || !owned(current)) return false;
    fs.unlinkSync(hostLeasePath(meshRoot, hostId));
    return true;
  });

// A file that could not be read gives no answer to cache (`read: false`): the next lookup reads it
// again. Only a file that was read, valid or not, is cached by its filesystem identity.
const parseLease = (file: string, name: string): { read: boolean; lease?: FabricHostLease | undefined; code?: string } => {
  let text: string;
  try {
    text = readFileRetrying(file);
  } catch (error) {
    return { read: false, code: (error as NodeJS.ErrnoException)?.code ?? "EUNKNOWN" };
  }
  return { read: true, lease: leaseOf(text, name) };
};

const leaseOf = (text: string, name: string): FabricHostLease | undefined => {
  try {
    const value = JSON.parse(text) as Record<string, unknown>;
    if (
      value?.format !== 1 ||
      typeof value.id !== "string" ||
      fileName(value.id) !== name ||
      typeof value.rootId !== "string" ||
      typeof value.identityId !== "string" ||
      typeof value.updatedAt !== "number" || !Number.isFinite(value.updatedAt) ||
      typeof value.expiresAt !== "number" || !Number.isFinite(value.expiresAt) ||
      (value.startedAt !== undefined && (typeof value.startedAt !== "number" || !Number.isFinite(value.startedAt))) ||
      (value.reloadUntil !== undefined && (typeof value.reloadUntil !== "number" || !Number.isFinite(value.reloadUntil)))
    ) return undefined;
    return {
      id: value.id, rootId: value.rootId, identityId: value.identityId,
      updatedAt: value.updatedAt, expiresAt: value.expiresAt,
      ...(typeof value.startedAt === "number" && Number.isFinite(value.startedAt) ? { startedAt: value.startedAt } : {}),
      ...(typeof value.reloadUntil === "number" && Number.isFinite(value.reloadUntil) ? { reloadUntil: value.reloadUntil } : {}),
      ...(validSession(value.session) ? { session: value.session } : {}),
      ...(validWriter(value.writer) ? { writer: value.writer } : {}),
    };
  } catch {
    return undefined;
  }
};

// Parsed files by directory and name, reused while a file's identity and timestamps are unchanged.
const cache = new Map<string, LeaseSlots>();

/** Every host's file lease, by host id. Unreadable or misnamed files are skipped. */
/**
 * `problems`, when given, collects every lease file or directory that exists but could not be
 * read, or a lease file that is not a valid lease, as "path: errno|invalid" (the advisory writer census
 * reports them as unknown, smarty-dev#6477). A file or directory that is absent (ENOENT) is no problem.
 */
export const readHostLeases = (meshRoot: string, problems?: string[]): Map<string, FabricHostLease> => {
  const dir = path.join(meshRoot, LEASE_DIR);
  const failed = (file: string, error: unknown): void => {
    const code = (error as NodeJS.ErrnoException)?.code ?? "EUNKNOWN";
    if (code !== "ENOENT") problems?.push(`${file}: ${code}`);
  };
  let names: string[];
  try {
    names = fs.readdirSync(dir);
  } catch (error) {
    failed(dir, error);
    return new Map();
  }
  const known = cache.get(dir) ?? new Map();
  cache.set(dir, known);
  const leases = new Map<string, FabricHostLease>();
  const present = new Set<string>();
  for (const name of names) {
    if (!name.endsWith(".json")) continue;
    present.add(name);
    let stat: fs.BigIntStats;
    try {
      stat = fs.statSync(path.join(dir, name), { bigint: true });
    } catch (error) {
      failed(path.join(dir, name), error);
      continue;
    }
    const lease = cachedLease(known, dir, name, stat, problems);
    if (lease) leases.set(lease.id, lease);
  }
  for (const name of known.keys()) if (!present.has(name)) known.delete(name);
  return leases;
};

/** Cheap topology-cache invalidation; freshness itself always uses effectiveLiveness. */
export const hostLeasesStamp = (meshRoot: string): string | undefined => {
  try {
    // Windows directory mtimes need not move on replacement: inspect the small leases there.
    if (process.platform === "win32") return [...readHostLeases(meshRoot).values()]
      .map(lease => `${lease.id}:${lease.updatedAt}:${lease.expiresAt}`).sort().join("|");
    const stat = fs.statSync(path.join(meshRoot, LEASE_DIR), { bigint: true });
    return `${stat.ino}:${stat.mtimeNs}:${stat.ctimeNs}`;
  } catch {
    return undefined;
  }
};

/** One host's file lease, from the same cache; for a single-participant lookup. */
export const readHostLease = (meshRoot: string, hostId: string): FabricHostLease | undefined =>
  readHostLeaseSnapshot(meshRoot, hostId)?.lease;

/** Strict current read for destructive operator checks: never consult/update the
 * peer cache or return its last answer when current bytes are unreadable/invalid.
 * Callers must distinguish an absent file from an existing but invalid one. */
export const readHostLeaseCurrent = (meshRoot: string, hostId: string): FabricHostLease | undefined => {
  try {
    return leaseOf(fs.readFileSync(hostLeasePath(meshRoot, hostId), "utf8"), fileName(hostId));
  } catch {
    return undefined;
  }
};

/** Stat and parsed lease from one filesystem observation, never the shared state. */
export const readHostLeaseSnapshot = (meshRoot: string, hostId: string): {
  lease: FabricHostLease | undefined; mtimeMs: number;
} | undefined => {
  const dir = path.join(meshRoot, LEASE_DIR);
  const name = fileName(hostId);
  let stat: fs.BigIntStats;
  try {
    stat = fs.statSync(path.join(dir, name), { bigint: true });
  } catch {
    return undefined;
  }
  const known = cache.get(dir) ?? new Map();
  cache.set(dir, known);
  // Recovery compares this timestamp with numeric owner.updatedAt; keep cache identities bigint.
  return { lease: cachedLease(known, dir, name, stat), mtimeMs: Number(stat.mtimeMs) };
};

type LeaseSlots = Map<string, Pick<fs.BigIntStats, "dev" | "ino" | "size" | "mtimeNs" | "ctimeNs"> & {
  lease: FabricHostLease | undefined;
}>;

const cachedLease = (
  known: LeaseSlots, dir: string, name: string, stat: fs.BigIntStats, problems?: string[],
): FabricHostLease | undefined => {
  const slot = known.get(name);
  const invalid = (): undefined => { problems?.push(`${path.join(dir, name)}: invalid`); return undefined; };
  // Atomic replacement can preserve size and timestamps. Keep the exact file identity:
  // NTFS IDs can exceed Number.MAX_SAFE_INTEGER, so distinct replacements can have the
  // same numeric ino. Nanosecond timestamps also avoid rounding away a change when a
  // filesystem lacks useful dev/ino values.
  if (slot && slot.dev === stat.dev && slot.ino === stat.ino && slot.size === stat.size &&
    slot.mtimeNs === stat.mtimeNs && slot.ctimeNs === stat.ctimeNs) return slot.lease ?? invalid();
  const parsed = parseLease(path.join(dir, name), name);
  if (!parsed.read) {                                       // unreadable for now: keep the last answer
    if (parsed.code !== "ENOENT") problems?.push(`${path.join(dir, name)}: ${parsed.code}`);
    return slot?.lease;
  }
  known.set(name, {
    dev: stat.dev, ino: stat.ino, size: stat.size, mtimeNs: stat.mtimeNs, ctimeNs: stat.ctimeNs,
    lease: parsed.lease,
  });
  return parsed.lease ?? invalid();
};

// Census writer metadata validation (smarty-dev#6477 L4a), shared by host leases, MeshStore
// process records and the census. Here, in the eager host-lease module, so it adds no startup chunk.

/** A positive safe-integer process id. */
export const validWriterPid = (value: unknown): value is number => Number.isSafeInteger(value) && (value as number) > 0;

/** A census writer must name its host; "" cannot be matched to a machine, so it is no evidence. */
export const validWriterHost = (value: unknown): value is string => typeof value === "string" && value.length > 0;

/** A census start time is a positive epoch-ms integer; 0 or a fraction is not a real incarnation. */
export const validWriterStartedAt = (value: unknown): value is number => Number.isSafeInteger(value) && (value as number) > 0;

/** The mesh lock protocols this release understands. */
export const validWriterLockProtocol = (value: unknown): value is 1 | 2 => value === 1 || value === 2;

/** The keyed-state backends this release understands. */
export const validWriterStateBackend = (value: unknown): value is "file" | "shadow" | "sqlite" =>
  value === "file" || value === "shadow" || value === "sqlite";

/** A build commit; "unknown" (no build SHA in the environment) is reported unknown by the census. */
export const validWriterReleaseSha = (value: unknown): value is string =>
  typeof value === "string" && value.length > 0 && value !== "unknown";

/** Invalid writer metadata is dropped from the lease, so the census reports that writer unknown. */
const validWriter = (value: unknown): value is MeshWriterRecord => {
  if (typeof value !== "object" || value === null) return false;
  const record = value as Record<string, unknown>;
  return validWriterPid(record.pid) && validWriterHost(record.host) && typeof record.releaseSha === "string" &&
    validWriterLockProtocol(record.lockProtocol) && validWriterStateBackend(record.stateBackend) &&
    validWriterStartedAt(record.startedAt);
};

const validSession = (value: unknown): value is NonNullable<FabricHostLease["session"]> => {
  if (typeof value !== "object" || value === null) return false;
  const session = value as Record<string, unknown>;
  return typeof session.id === "string" &&
    [session.startedAt, session.updatedAt, session.expiresAt].every(n => typeof n === "number" && Number.isFinite(n));
};

/** Matching identity and incarnation only; an old file cannot revive a takeover. */
export const hostLiveness = (
  leases: ReadonlyMap<string, FabricHostLease>,
  host: { id: string; rootId: string; identity: { id: string }; startedAt?: number; updatedAt?: number; expiresAt: number },
): Liveness => {
  const lease = leases.get(host.id);
  const matching = lease && lease.rootId === host.rootId && lease.identityId === host.identity.id &&
    (lease.startedAt === undefined || host.startedAt === undefined || lease.startedAt === host.startedAt);
  return effectiveLiveness({ updatedAt: host.updatedAt ?? 0, expiresAt: host.expiresAt }, matching ? lease : undefined);
};

/** Reapers also accept incomplete historical records, retaining them conservatively. */
export const hostEntryLiveness = (entry: MeshStateEntry, leases: ReadonlyMap<string, FabricHostLease>): Liveness => {
  const value = typeof entry.value === "object" && entry.value !== null ? entry.value as Record<string, unknown> : {};
  const identity = typeof value.identity === "object" && value.identity !== null ? value.identity as Record<string, unknown> : {};
  const stored = { updatedAt: entry.updatedAt, expiresAt: typeof value.expiresAt === "number" ? value.expiresAt : entry.updatedAt };
  if (typeof value.id !== "string") return stored;
  if (typeof value.rootId === "string" && typeof identity.id === "string") return hostLiveness(leases, {
    id: value.id, rootId: value.rootId, identity: { id: identity.id },
    ...(typeof value.startedAt === "number" ? { startedAt: value.startedAt } : {}), ...stored,
  });
  return effectiveLiveness(stored, leases.get(value.id));
};

/** Compatibility alias: every host reader uses the same effective-liveness rule. */
export const hostLeaseExpiry = (
  leases: ReadonlyMap<string, FabricHostLease>,
  host: { id: string; rootId: string; identity: { id: string }; startedAt?: number; updatedAt?: number; expiresAt: number },
): number => hostLiveness(leases, host).expiresAt;

/** Whether the fleet owner has moved lease renewals to files (see LIVENESS_POLICY_KEY). */
export const fileLeasesOnly = (policy: unknown): boolean =>
  typeof policy === "object" && policy !== null &&
  (policy as { version?: unknown }).version === 1 &&
  (policy as { hostLeases?: unknown }).hostLeases === "files";
