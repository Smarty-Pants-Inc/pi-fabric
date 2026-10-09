import { createHash, randomUUID } from "node:crypto";
import os from "node:os";
import fs from "node:fs";
import path from "node:path";
import { isMeshLockTimeout, readFileRetrying, writeJsonAtomic } from "../core/atomic-write.js";
import { effectiveLiveness, type Liveness } from "./liveness.js";
import { preparedUuidHostClaim, prepareFirstUuidHostClaim } from "./host-lease-upgrade-gate.js";
import type { MeshStateEntry, MeshStore } from "../mesh/store.js";
import { HostLeaseLockBusyError, HostLeaseLockLostError, withHostLeaseLock, type HostLeaseLockOptions, type HostLeaseMesh } from "./host-lease-lock.js";

// Host lease renewals outside the shared state (smarty-dev#816). Every heartbeat rewrote the
// whole shared state under the one mesh lock, and heartbeats were 78% of all locked writes. Each
// host now also renews its lease in a file of its own, replaced by an atomic rename without
// the shared state lock. Claim/renew/remove and recovery serialize in a per-host commit domain (#7313).

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
  /** Start-time metadata only; clocks can repeat. */
  startedAt?: number;
  /** Unique incarnation fence; absent only on historical writers. */
  incarnationToken?: string;
  /** Root Main's bounded reload lease; absent on ordinary heartbeats and older writers. */
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

export const writeHostLease = (meshRoot: string, lease: FabricHostLease): void =>
  writeJsonAtomic(hostLeasePath(meshRoot, lease.id), { format: 1, ...lease });

export class FabricHostLeaseSupersededError extends Error {
  override readonly name: string = "FabricHostLeaseSupersededError";
  readonly code: string = "FABRIC_HOST_LEASE_SUPERSEDED";
  readonly retryable = false;
  constructor(readonly hostId: string, readonly identityId: string, readonly startedAt: number | undefined) {
    super(`Fabric host ${hostId}: lease incarnation ${identityId}/${startedAt} no longer owns the lease`);
  }
}

export class FabricHostLeaseContestedError extends FabricHostLeaseSupersededError {
  override readonly name = "FabricHostLeaseContestedError";
  override readonly code = "FABRIC_HOST_LEASE_CONTESTED";
}

export class FabricHostLeaseLegacyBusyError extends Error {
  override readonly name = "FabricHostLeaseLegacyBusyError";
  readonly code = "FABRIC_HOST_LEASE_LEGACY_BUSY";
  readonly retryable = true;
  constructor(readonly hostId: string, readonly expiresAt: number) {
    super(`Fabric host ${hostId}: a fresh legacy writer owns the lease until ${expiresAt}; drain it before upgrade`);
  }
}

const waitForLegacyExpiry = (ms: number, signal?: AbortSignal): Promise<void> => new Promise((resolve, reject) => {
  signal?.throwIfAborted();
  const finish = (error?: unknown) => {
    clearTimeout(timer); signal?.removeEventListener("abort", abort);
    if (error) reject(error); else resolve();
  };
  const abort = () => finish(signal?.reason ?? new DOMException("Aborted", "AbortError"));
  const timer = setTimeout(() => finish(), ms);
  signal?.addEventListener("abort", abort, { once: true });
});

/** Fresh uncached ownership check. A legacy overwrite after UUID admission is contested,
 * not a migration opportunity. Callers retain this terminal typed error. */
export const assertHostLeaseOwner = (meshRoot: string, owner: FabricHostLease): void => {
  const current = hostLeasePredecessor(meshRoot, owner.id);
  if (current && current.incarnationToken === undefined) {
    throw new FabricHostLeaseContestedError(owner.id, owner.identityId, owner.startedAt);
  }
  if (!current || !sameHostLeaseOwner(current, owner)) {
    throw new FabricHostLeaseSupersededError(owner.id, owner.identityId, owner.startedAt);
  }
};

/** Claims/renewals and every owned mutation share this physical-host-qualified commit gate.
 * Hold it THROUGH an awaited state transaction; a paused writer cannot cross a new token CAS.
 * Lock order: participant key (if any), lease commit gate, then shared state. */
export const withOwnedHostLease = async <T>(mesh: HostLeaseMesh & Pick<MeshStore, "withBatchCommitFence">, owner: FabricHostLease,
  operation: () => T | Promise<T>, options: { ownIncarnation?: string | undefined; ownerAfter?: () => FabricHostLease } = {}): Promise<T> => {
  const file = hostLeasePath(mesh.root, owner.id);
  let entered = false;
  try {
    return await mesh.leaseCustody(file, async () => {
      entered = true;
      assertHostLeaseOwner(mesh.root, owner);
      const result = await mesh.withBatchCommitFence(() => assertHostLeaseOwner(mesh.root, owner), operation);
      assertHostLeaseOwner(mesh.root, options.ownerAfter?.() ?? owner);
      return result;
    }, 0, options);
  } catch (error) {
    if (!entered && isMeshLockTimeout(error)) throw new HostLeaseLockBusyError(file);
    throw error;
  }
};

export const sameHostLeaseOwner = (left: FabricHostLease, right: FabricHostLease): boolean =>
  left.id === right.id && left.rootId === right.rootId && left.identityId === right.identityId &&
  left.incarnationToken !== undefined && left.incarnationToken === right.incarnationToken;

/** Capture once, before any acquisition wait. Unknown bytes never authorize a claim. */
export const hostLeasePredecessor = (meshRoot: string, hostId: string): FabricHostLease | undefined => {
  const file = hostLeasePath(meshRoot, hostId);
  try {
    const lease = leaseOf(readFileRetrying(file), fileName(hostId));
    if (!lease) throw new Error(`Invalid host lease: ${file}`);
    return lease;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    return undefined;
  }
};

/** Historical predecessors have no UUID; their metadata may be compared for the one-time
 * migration claim, but can never authorize a renewal/release by a token-bearing writer. */
const samePredecessor = (left: FabricHostLease | undefined, right: FabricHostLease | undefined): boolean =>
  left === undefined || right === undefined ? left === right :
    left.incarnationToken !== undefined || right.incarnationToken !== undefined ? sameHostLeaseOwner(left, right) :
      left.id === right.id && left.rootId === right.rootId && left.identityId === right.identityId && left.startedAt === right.startedAt;

/** Synchronous lease CAS; caller MUST hold this host's leaseCustody gate through the write.
 * Reload successors use it in the successful shared commit hook, never before a timed-out batch. */
export const commitHostLeaseUnderCustody = (meshRoot: string, lease: FabricHostLease,
  options: { claim?: boolean; expected?: FabricHostLease | undefined; uuidAdmission?: (() => void) | undefined } = {}): void => {
  if (!lease.incarnationToken) throw new TypeError("Host lease mutation requires an incarnation token");
  const expected = options.expected;
  const current = hostLeasePredecessor(meshRoot, lease.id);
  const markUuidClaim = options.claim ? options.uuidAdmission
    ? preparedUuidHostClaim(options.uuidAdmission, meshRoot, lease.id, lease.incarnationToken)
    : prepareFirstUuidHostClaim(meshRoot, lease.id, lease.incarnationToken) : undefined;
  if (current?.incarnationToken === undefined && current !== undefined) {
    if (!options.claim) throw new FabricHostLeaseContestedError(lease.id, lease.identityId, lease.startedAt);
    if (current.expiresAt > Date.now()) throw new FabricHostLeaseLegacyBusyError(lease.id, current.expiresAt);
  }
  const owned = current !== undefined && sameHostLeaseOwner(current, lease);
  if (!owned && (!options.claim || !samePredecessor(current, expected))) {
    throw new FabricHostLeaseSupersededError(lease.id, lease.identityId, lease.startedAt);
  }
  writeHostLease(meshRoot, { ...lease,
    ...(current && owned && lease.reloadUntil === undefined ? {
      updatedAt: Math.max(lease.updatedAt, current.updatedAt),
      expiresAt: Math.max(lease.expiresAt, current.expiresAt),
      ...(lease.session && current.session?.id === lease.session.id && current.session.startedAt === lease.session.startedAt ? {
        session: { ...lease.session, updatedAt: Math.max(lease.session.updatedAt, current.session.updatedAt),
          expiresAt: Math.max(lease.session.expiresAt, current.session.expiresAt) },
      } : {}),
    } : {}),
  });
  markUuidClaim?.();
};

/** Initial claim compares the captured predecessor, not clocks; subsequent renewal never
 * recreates a missing/different lease. Legacy admission has one expiry/deadline wake only. */
export const renewHostLease = async (mesh: HostLeaseMesh, lease: FabricHostLease,
  options: HostLeaseLockOptions & { claim?: boolean; expected?: FabricHostLease | undefined } = {}): Promise<void> => {
  if (!lease.incarnationToken) throw new TypeError("Host lease mutation requires an incarnation token");
  const expected = options.claim
    ? Object.hasOwn(options, "expected") ? options.expected : hostLeasePredecessor(mesh.root, lease.id)
    : undefined;
  if (expected && expected.incarnationToken === undefined && expected.expiresAt > Date.now()) {
    // Refuse live/unknown old processes BEFORE waiting for a legacy TTL. A successful
    // preflight is revalidated under custody after the single expiry wake, never cached.
    prepareFirstUuidHostClaim(mesh.root, lease.id, lease.incarnationToken);
    const waitMs = Math.min(expected.expiresAt - Date.now(), options.timeoutMs ?? Number.POSITIVE_INFINITY);
    if (waitMs <= 0) throw new FabricHostLeaseLegacyBusyError(lease.id, expected.expiresAt);
    await waitForLegacyExpiry(waitMs, options.signal); // ONE wake, never a renewal/poll loop.
  }
  try {
    await withHostLeaseLock(mesh, hostLeasePath(mesh.root, lease.id), () => {
      commitHostLeaseUnderCustody(mesh.root, lease, { claim: options.claim ?? false, expected });
    }, { ...options, lease: { incarnationToken: lease.incarnationToken, expiresAt: lease.expiresAt } });
  } catch (error) {
    if (error instanceof HostLeaseLockLostError) throw new FabricHostLeaseSupersededError(lease.id, lease.identityId, lease.startedAt);
    throw error;
  }
};

/** Directory close uses the same claim/renew receipt, so it cannot remove a successor. */
export const removeOwnedHostLease = async (mesh: HostLeaseMesh, owner: FabricHostLease): Promise<boolean> => {
  const captured = readHostLeaseCurrent(mesh.root, owner.id);
  if (captured && captured.incarnationToken === undefined) {
    throw new FabricHostLeaseContestedError(owner.id, owner.identityId, owner.startedAt);
  }
  if (!captured || !sameHostLeaseOwner(captured, owner)) return false;
  try {
    return await withHostLeaseLock(mesh, hostLeasePath(mesh.root, owner.id), () => {
      const current = readHostLeaseCurrent(mesh.root, owner.id);
      if (current && current.incarnationToken === undefined) {
        throw new FabricHostLeaseContestedError(owner.id, owner.identityId, owner.startedAt);
      }
      if (!current || !sameHostLeaseOwner(current, owner)) return false;
      fs.rmSync(hostLeasePath(mesh.root, owner.id), { force: true });
      return true;
    }, { lease: { incarnationToken: captured.incarnationToken!, expiresAt: captured.expiresAt } });
  } catch (error) { if (error instanceof HostLeaseLockLostError) return false; throw error; }
};

/** The exact observed renewal version, including deadlines/session metadata, not only its UUID. */
export const sameHostLeaseVersion = (left: FabricHostLease | undefined, right: FabricHostLease | undefined): boolean =>
  JSON.stringify(left) === JSON.stringify(right);

/** Reaper compare-and-delete. Caller holds the TARGET's leaseCustody gate, as renew/claim do.
 * The detached-file check also keeps an unfenced historical replacement rather than unlinking it. */
export const removeHostLeaseVersionUnderCustody = (meshRoot: string, expected: FabricHostLease): boolean =>
  removeHostLeaseIf(meshRoot, expected.id, current => sameHostLeaseVersion(current, expected));

export const removeHostLease = (meshRoot: string, hostId: string): void =>
  fs.rmSync(path.join(meshRoot, LEASE_DIR, fileName(hostId)), { force: true });

/**
 * Removes a host's file lease only when `owned` accepts it (smarty-dev#6477 L2b owner review): a
 * lease another owner wrote (a replacement that wrote its file before its state record) is kept.
 * An absent, unreadable or invalid file is never removed. The file is renamed aside before the
 * final check, so the lease judged is the lease removed; a lease that changed owner in between is
 * linked back, unless a newer lease already took the name (the newer one wins). Returns whether
 * a lease was removed.
 */
export const removeHostLeaseIf = (meshRoot: string, hostId: string, owned: (lease: FabricHostLease) => boolean): boolean => {
  const current = readHostLeaseCurrent(meshRoot, hostId);
  if (!current || !owned(current)) return false;
  const file = hostLeasePath(meshRoot, hostId);
  // Not a .json name: lease scans never read the aside copy.
  const aside = path.join(path.dirname(file), `.unlease-${randomUUID()}.tmp`);
  try {
    fs.renameSync(file, aside);
  } catch {
    return false; // Gone already, or busy (Windows): keep; an unremoved mirror lease lapses.
  }
  try {
    let taken: FabricHostLease | undefined;
    try { taken = leaseOf(fs.readFileSync(aside, "utf8"), fileName(hostId)); } catch { taken = undefined; }
    if (taken && owned(taken)) return true;
    try { fs.linkSync(aside, file); } catch { /* EEXIST: a newer lease holds the name */ }
    return false;
  } finally {
    fs.rmSync(aside, { force: true });
  }
};

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
      (value.incarnationToken !== undefined && (typeof value.incarnationToken !== "string" || value.incarnationToken.length === 0)) ||
      (value.reloadUntil !== undefined && (typeof value.reloadUntil !== "number" || !Number.isFinite(value.reloadUntil)))
    ) return undefined;
    return {
      id: value.id, rootId: value.rootId, identityId: value.identityId,
      updatedAt: value.updatedAt, expiresAt: value.expiresAt,
      ...(typeof value.startedAt === "number" && Number.isFinite(value.startedAt) ? { startedAt: value.startedAt } : {}),
      ...(typeof value.incarnationToken === "string" ? { incarnationToken: value.incarnationToken } : {}),
      ...(typeof value.reloadUntil === "number" ? { reloadUntil: value.reloadUntil } : {}),
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
  host: { id: string; rootId: string; identity: { id: string }; incarnationToken?: string; startedAt?: number; updatedAt?: number; expiresAt: number },
): Liveness => {
  const lease = leases.get(host.id);
  const matching = lease && lease.rootId === host.rootId && lease.identityId === host.identity.id &&
    (host.incarnationToken !== undefined ? lease.incarnationToken === host.incarnationToken :
      (lease.startedAt === undefined || host.startedAt === undefined || lease.startedAt === host.startedAt));
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
    ...(typeof value.startedAt === "number" ? { startedAt: value.startedAt } : {}),
    ...(typeof value.incarnationToken === "string" ? { incarnationToken: value.incarnationToken } : {}), ...stored,
  });
  return effectiveLiveness(stored, leases.get(value.id));
};

/** Compatibility alias: every host reader uses the same effective-liveness rule. */
export const hostLeaseExpiry = (
  leases: ReadonlyMap<string, FabricHostLease>,
  host: { id: string; rootId: string; identity: { id: string }; incarnationToken?: string; startedAt?: number; updatedAt?: number; expiresAt: number },
): number => hostLiveness(leases, host).expiresAt;

/** Whether the fleet owner has moved lease renewals to files (see LIVENESS_POLICY_KEY). */
export const fileLeasesOnly = (policy: unknown): boolean =>
  typeof policy === "object" && policy !== null &&
  (policy as { version?: unknown }).version === 1 &&
  (policy as { hostLeases?: unknown }).hostLeases === "files";
