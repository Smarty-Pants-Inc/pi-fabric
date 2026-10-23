import { createHash, randomUUID } from "node:crypto";
import os from "node:os";
import fs from "node:fs";
import path from "node:path";
import { isMeshLockTimeout, readFileRetrying, writeJsonAtomic, type PhysicalHostIdentity } from "../core/atomic-write.js";
import { effectiveLiveness, type Liveness } from "./liveness.js";
import { watchBridgeStore } from "../mesh/change-notifier.js";
import { FabricPeerLeaseStaleError, PEER_LEASE_WAIT_MS } from "../agents/peer-lease-stale.js";
import type { MeshStateEntry } from "../mesh/store.js";
import { TransportUnavailableError, type TransportKv, type TransportKvEntry, type TransportOwner } from "../transport/types.js";
import { HostLeaseLockBusyError, HostLeaseLockLostError, hostLeaseAdditionsGatePath, withHostLeaseFileCustody, withHostLeaseLock, type HostLeaseLockOptions, type HostLeaseMesh } from "./host-lease-lock.js";

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

/** Local writer evidence only: generated by the receiving bridge, never copied from its peer. */
export interface BridgeLeaseProcess extends PhysicalHostIdentity {
  link: string;
  pid: number;
  incarnation: string;
}

/** A source-clock lease observation, attributed to one physical bridge peer. */
export interface FabricMirrorLease {
  remoteHost: string;
  /** Receiving clock before the mirror's dispatch/write wait, never the delayed commit time. */
  observedAt: number;
  sourceObservedAt: number;
  sourceUpdatedAt: number;
  sourceExpiresAt: number;
  /** Advisory stall detection only; never routing or ownership authority. */
  bridgeProcess?: BridgeLeaseProcess;
}

export const MIRROR_FRESH_MS = 15_000;
export type MirroredLeaseStatus = "live" | "lease-stale" | "bridge-stale";

const validMirrorLease = (value: unknown): value is FabricMirrorLease => {
  if (typeof value !== "object" || value === null) return false;
  const mirror = value as Record<string, unknown>;
  return typeof mirror.remoteHost === "string" && mirror.remoteHost.length > 0 &&
    [mirror.observedAt, mirror.sourceObservedAt, mirror.sourceUpdatedAt, mirror.sourceExpiresAt]
      .every(n => typeof n === "number" && Number.isFinite(n));
};

/** Transport staleness is not evidence that the remote Main's own lease lapsed. */
export const mirroredLeaseStatus = (
  remoteHost: string,
  mirror: FabricMirrorLease | undefined,
  now = Date.now(),
): MirroredLeaseStatus => {
  if (!validMirrorLease(mirror) || mirror.remoteHost !== remoteHost ||
    mirror.observedAt > now || now - mirror.observedAt >= MIRROR_FRESH_MS) return "bridge-stale";
  return mirror.sourceExpiresAt <= mirror.sourceObservedAt ? "lease-stale" : "live";
};

/** Routing needs both source evidence and unexpired translated authority. A translated
 * deadline passing alone cannot prove source death; only a fresh expired sample can. */
export const mirroredOwnerRoutingStatus = (
  owner: { remoteHost: string; expiresAt: number; mirror?: FabricMirrorLease; mirrorLease?: 1; bridgeStale?: true },
  now = Date.now(),
): MirroredLeaseStatus => {
  if (owner.bridgeStale) return "bridge-stale";
  // Mixed-release legacy exception: an older bridge proves only its unexpired projection,
  // never source death. Remove ONLY after ALL bridges run a release with mirrorLease.
  const status = owner.mirror || owner.mirrorLease === 1
    ? mirroredLeaseStatus(owner.remoteHost, owner.mirror, now) : "live";
  return status === "live" && !(owner.expiresAt > now) ? "bridge-stale" : status;
};

/** Shared by directory recovery and commit-time control admission: never cascade two waits. */
export interface MirrorLeaseWaitBudget { waited?: boolean; }

/** Resolution only. Arm before re-reading; notifications drive renewal, one timer bounds silence.
 * A fresh source lapse returns to the caller immediately for its ordinary root-gone refusal. */
export const waitForMirroredLeaseRenewal = async <T extends {
  remoteHost: string; expiresAt: number; mirror?: FabricMirrorLease; mirrorLease?: 1; bridgeStale?: true;
}>(meshRoot: string, targetId: string, initial: T, read: () => T | undefined,
  sameOwner: (current: T) => boolean,
  options: { signal?: AbortSignal | undefined; now?: (() => number) | undefined; waitMs?: number | undefined; budget?: MirrorLeaseWaitBudget | undefined } = {},
): Promise<T | undefined> => {
  const { signal, budget } = options;
  signal?.throwIfAborted();
  const now = options.now ?? Date.now;
  const stale = (current: T): never => {
    throw new FabricPeerLeaseStaleError(targetId, initial.remoteHost, Math.max(0, now() - current.expiresAt));
  };
  if (budget?.waited) return stale(initial);
  if (budget) budget.waited = true;
  const configured = options.waitMs ?? PEER_LEASE_WAIT_MS;
  const waitMs = Number.isFinite(configured) ? Math.min(PEER_LEASE_WAIT_MS, Math.max(0, configured)) : PEER_LEASE_WAIT_MS;
  return await new Promise<T | undefined>((resolve, reject) => {
    let settled = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let unwatch: (() => void) | undefined;
    const settle = (outcome: () => T | undefined): void => {
      if (settled) return;
      settled = true;
      signal?.removeEventListener("abort", abort);
      if (timer !== undefined) clearTimeout(timer);
      timer = undefined;
      try { unwatch?.(); } catch { /* best effort */ }
      unwatch = undefined;
      try { resolve(outcome()); } catch (error) { reject(error); }
    };
    const abort = (): void => settle(() => { throw signal!.reason; });
    const decide = (final: boolean): { done: boolean; owner?: T } => {
      signal?.throwIfAborted();
      const current = read();
      if (!current) return { done: true };
      if (!sameOwner(current)) return stale(initial);
      if (mirroredOwnerRoutingStatus(current, now()) !== "bridge-stale") return { done: true, owner: current };
      if (final) return stale(current);
      return { done: false };
    };
    const check = (): void => {
      if (settled) return;
      try {
        const result = decide(false);
        if (result.done) settle(() => result.owner);
      } catch (error) { settle(() => { throw error; }); }
    };
    const timeout = (): void => settle(() => decide(true).owner);
    signal?.addEventListener("abort", abort, { once: true });
    if (signal?.aborted) abort();
    if (settled) return;
    try {
      const stop = watchBridgeStore(meshRoot, check, timeout);
      // Watch setup can synchronously abort or fail before its cleanup handle exists.
      if (settled) { stop(); return; }
      unwatch = stop;
    } catch { timeout(); return; }
    const armedTimer = setTimeout(timeout, waitMs);
    if (settled) { clearTimeout(armedTimer); return; }
    timer = armedTimer;
    check();
  });
};

export class FabricBridgeStaleError extends Error {
  override readonly name = "FabricBridgeStaleError";
  readonly code = "FABRIC_BRIDGE_STALE";
  readonly retryable = true;
  constructor(readonly remoteHost: string, readonly targetId: string, published = true) {
    super(`Fabric bridge-stale for remote host ${remoteHost}, target ${targetId}; ` +
      "no fresh mirror from that host; the remote owner's lease is unconfirmed; " +
      (published ? "the outcome is unknown and it may still be delivered, so a retry can deliver it twice." :
        "this attempt was not published."));
  }
}

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
  /** Locally observed tokens superseded by this bridge authority; absent on legacy leases. */
  bridgeSupersededTokens?: string[];
  /** Root-local first-seen ordinals for retained superseded tokens; absent on legacy leases. */
  bridgeSupersededTokenSeen?: Record<string, number>;
  /** Root Main's bounded reload lease; absent on ordinary heartbeats and older writers. */
  reloadUntil?: number;
  /** Main session has a fixed 15 s TTL, independent of the host TTL. */
  session?: Liveness & { id: string; startedAt: number };
  /** Bridge-written observation of this root's native lease on its physical remote host. */
  mirror?: FabricMirrorLease;
  /** Missing/malformed required observation is doubt, not native owner-death evidence. */
  mirrorLease?: 1;
  /** Writer census metadata, absent on leases from pre-census releases. */
  writer?: MeshWriterRecord;
}

const fileName = (hostId: string): string =>
  createHash("sha256").update(hostId).digest("hex").slice(0, 32) + ".json";

/** Canonical own-host path, shared by peers and the residency launcher watchdog. */
export const hostLeasePath = (meshRoot: string, hostId: string): string =>
  path.join(meshRoot, LEASE_DIR, fileName(hostId));

/** Holds the additions namespace and every existing per-host lease gate in path order.
 * Call before reading lease JSON; additions are blocked by the namespace gate and replacements
 * are blocked by their target gate until the caller releases custody after COMMIT. */
export const withHostLeaseDirectoryCustody = async <T>(mesh: HostLeaseMesh,
  operation: () => T | Promise<T>): Promise<T> => mesh.leaseCustody(hostLeaseAdditionsGatePath(mesh.root), async () => {
  const directory = path.join(mesh.root, LEASE_DIR);
  let names: string[];
  try { names = fs.readdirSync(directory).filter(name => /^[0-9a-f]{32}\.json$/.test(name)).sort(); }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") names = []; else throw error; }
  const files = names.map(name => path.join(directory, name));
  const acquire = (index: number): Promise<T> => index === files.length
    ? Promise.resolve().then(operation)
    : mesh.leaseCustody(files[index]!, () => acquire(index + 1));
  return acquire(0);
});

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
export const withOwnedHostLease = async <T>(mesh: HostLeaseMesh, owner: FabricHostLease,
  operation: () => T | Promise<T>, options: { ownIncarnation?: string | undefined; ownerAfter?: () => FabricHostLease } = {}): Promise<T> => {
  const file = hostLeasePath(mesh.root, owner.id);
  let entered = false;
  try {
    return await mesh.leaseCustody(file, async () => {
      entered = true;
      assertHostLeaseOwner(mesh.root, owner);
      const result = await operation();
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
  options: { claim?: boolean; expected?: FabricHostLease | undefined } = {}): void => {
  if (!lease.incarnationToken) throw new TypeError("Host lease mutation requires an incarnation token");
  const expected = options.expected;
  const current = hostLeasePredecessor(meshRoot, lease.id);
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
    const deadline = Date.now() + (options.timeoutMs ?? Number.POSITIVE_INFINITY);
    const waitMs = Math.min(expected.expiresAt - Date.now(), options.timeoutMs ?? Number.POSITIVE_INFINITY);
    if (waitMs <= 0) throw new FabricHostLeaseLegacyBusyError(lease.id, expected.expiresAt);
    await waitForLegacyExpiry(waitMs, options.signal); // ONE wake, never a renewal/poll loop.
    // The timer runs on libuv's monotonic loop clock (started from the iteration's cached time); Date.now() is the
    // truncated wall clock. So the wake can land ~1 ms (more after synchronous work) before expiresAt, and the commit
    // would then refuse a lease that is about to expire. One bounded catch-up wake for the remainder, never a loop.
    // The +1 ms covers a sub-millisecond gap between the two clocks on the catch-up wake itself.
    if (expected.expiresAt > Date.now()) {
      const remainder = Math.min(expected.expiresAt - Date.now() + 1, deadline - Date.now());
      if (remainder > 0) await waitForLegacyExpiry(remainder, options.signal);
    }
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

/** Exact-version atomic replacement. Caller holds the target lease-custody gate through this check/write. */
export const writeHostLeaseVersionIf = (meshRoot: string, expected: FabricHostLease | undefined,
  replacement: FabricHostLease): boolean => {
  if (expected && expected.id !== replacement.id) return false;
  const file = hostLeasePath(meshRoot, replacement.id);
  const current = readHostLeaseCurrent(meshRoot, replacement.id);
  if (!sameHostLeaseVersion(current, expected) || (!current && fs.existsSync(file))) return false;
  writeHostLease(meshRoot, replacement);
  return true;
};

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
      ...(Array.isArray(value.bridgeSupersededTokens) ? { bridgeSupersededTokens: value.bridgeSupersededTokens.filter((token): token is string => typeof token === "string" && token.length > 0).slice(-16) } : {}),
      ...(typeof value.bridgeSupersededTokenSeen === "object" && value.bridgeSupersededTokenSeen !== null && !Array.isArray(value.bridgeSupersededTokenSeen)
        ? { bridgeSupersededTokenSeen: Object.fromEntries(Object.entries(value.bridgeSupersededTokenSeen)
          .filter(([token, sequence]) => typeof token === "string" && token.length > 0 && Number.isSafeInteger(sequence) && (sequence as number) >= 0)) } : {}),
      ...(typeof value.reloadUntil === "number" ? { reloadUntil: value.reloadUntil } : {}),
      ...(validSession(value.session) ? { session: value.session } : {}),
      ...(validMirrorLease(value.mirror) ? { mirror: value.mirror } : {}),
      ...(value.mirrorLease === 1 || value.mirror !== undefined ? { mirrorLease: 1 as const } : {}),
      ...(validWriter(value.writer) ? { writer: value.writer } : {}),
    };
  } catch {
    return undefined;
  }
};

// Parsed files by directory and name, reused while a file's identity and timestamps are unchanged.
const cache = new Map<string, LeaseSlots>();

// smarty-dev#7978: the directory listing, reused while the directory's stamp is unchanged. Every
// writer adds, replaces or removes a lease by a directory entry (writeJsonAtomic's rename, the
// unlease rename, rmSync), which moves the directory's mtime/ctime; an in-place rewrite does not,
// so every listed file is still stat'ed and re-read on its own change. A directory whose mtime is
// at least DIR_TICK_MS old when listed cannot change again within the same timestamp tick, so an
// unchanged stamp later means an unchanged listing (as participant-files.ts). Windows does not move
// a directory's mtime reliably on a replace: it always lists.
const DIR_TICK_MS = 100;
interface LeaseListing { dev: bigint; ino: bigint; mtimeNs: bigint; ctimeNs: bigint; listedAt: number; names: readonly string[] }
const listings = new Map<string, LeaseListing>();

const leaseNames = (dir: string, uncached: boolean): readonly string[] => {
  if (uncached || process.platform === "win32") {
    listings.delete(dir);
    return fs.readdirSync(dir);
  }
  const stat = fs.statSync(dir, { bigint: true });
  const known = listings.get(dir);
  if (known && known.dev === stat.dev && known.ino === stat.ino && known.mtimeNs === stat.mtimeNs &&
    known.ctimeNs === stat.ctimeNs && known.listedAt - Number(known.mtimeNs / 1_000_000n) >= DIR_TICK_MS) return known.names;
  const listedAt = Date.now();
  const names = fs.readdirSync(dir);
  listings.set(dir, { dev: stat.dev, ino: stat.ino, mtimeNs: stat.mtimeNs, ctimeNs: stat.ctimeNs, listedAt, names });
  return names;
};

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
  let names: readonly string[];
  try {
    names = leaseNames(dir, problems !== undefined);
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

/** Cheap mutation stamp for a mirror plan: no lease JSON is opened or parsed. Every supported
 * lease writer replaces or removes a directory entry atomically. POSIX directory metadata is one
 * stat; Windows requires an entry listing and metadata-only stat of each lease file. */
export const hostLeaseDirectoryStamp = (meshRoot: string): string | undefined => {
  const dir = path.join(meshRoot, LEASE_DIR);
  try {
    if (process.platform !== "win32") {
      const stat = fs.statSync(dir, { bigint: true });
      return `${stat.dev}:${stat.ino}:${stat.mtimeNs}:${stat.ctimeNs}`;
    }
    return fs.readdirSync(dir).filter(name => name.endsWith(".json")).sort().map(name => {
      const stat = fs.statSync(path.join(dir, name), { bigint: true });
      return `${name}:${stat.dev}:${stat.ino}:${stat.size}:${stat.mtimeNs}:${stat.ctimeNs}`;
    }).join("|");
  } catch (error) {
    if ((error as NodeJS.ErrnoException)?.code === "ENOENT") return "absent";
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
export const validWriterLockProtocol = (value: unknown): value is 1 | 2 | 3 => value === 1 || value === 2 || value === 3;

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

type HostLeaseOwner = { id: string; rootId: string; identity: { id: string }; incarnationToken?: string;
  startedAt?: number; updatedAt?: number; expiresAt: number; remoteHost?: string; mirrorLease?: 1 };

const matchingHostLease = (leases: ReadonlyMap<string, FabricHostLease>, host: HostLeaseOwner): FabricHostLease | undefined => {
  const lease = leases.get(host.id);
  return lease && lease.rootId === host.rootId && lease.identityId === host.identity.id &&
    (host.incarnationToken !== undefined ? lease.incarnationToken === host.incarnationToken :
      (lease.startedAt === undefined || host.startedAt === undefined || lease.startedAt === host.startedAt)) ? lease : undefined;
};

/** Mirror provenance is accepted only for this host's current identity and incarnation. */
export const hostMirrorLease = (leases: ReadonlyMap<string, FabricHostLease>, host: HostLeaseOwner): FabricMirrorLease | undefined => {
  const mirror = matchingHostLease(leases, host)?.mirror;
  return host.remoteHost && mirror?.remoteHost === host.remoteHost ? mirror : undefined;
};

/** Matching identity and incarnation only; an old file cannot revive a takeover. */
export const hostLiveness = (
  leases: ReadonlyMap<string, FabricHostLease>,
  host: HostLeaseOwner,
): Liveness => {
  const lease = matchingHostLease(leases, host);
  if (lease && host.remoteHost && lease.mirror?.remoteHost === host.remoteHost) {
    // mirroredOwnerRoutingStatus requires a fresh source sample AND this translated TTL,
    // so short-lease origins are reobserved before forwarding. Expired translated authority
    // is not proof of a dead Main; mirroredLeaseStatus alone judges source-death evidence.
    return { updatedAt: lease.mirror.observedAt, expiresAt: lease.mirror.sourceExpiresAt > lease.mirror.sourceObservedAt
      ? Math.min(lease.expiresAt, lease.mirror.observedAt + MIRROR_FRESH_MS) : lease.mirror.observedAt - 1 };
  }
  // No observation grants no authority, but retain the last checkpoint's age for reapers:
  // a missing/corrupt file must not turn a recent projection into an epoch-old record.
  if (host.remoteHost && host.mirrorLease === 1) return { updatedAt: host.updatedAt ?? 0, expiresAt: (host.updatedAt ?? 0) - 1 };
  return effectiveLiveness({ updatedAt: host.updatedAt ?? 0, expiresAt: host.expiresAt }, lease);
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
    ...(typeof value.incarnationToken === "string" ? { incarnationToken: value.incarnationToken } : {}),
    ...(typeof value.remoteHost === "string" ? { remoteHost: value.remoteHost } : {}),
    ...(value.mirrorLease === 1 ? { mirrorLease: 1 as const } : {}), ...stored,
  });
  return effectiveLiveness(stored, leases.get(value.id));
};

/** Compatibility alias: every host reader uses the same effective-liveness rule. */
export const hostLeaseExpiry = (
  leases: ReadonlyMap<string, FabricHostLease>,
  host: HostLeaseOwner,
): number => hostLiveness(leases, host).expiresAt;

/** Whether the fleet owner has moved lease renewals to files (see LIVENESS_POLICY_KEY). */
export const fileLeasesOnly = (policy: unknown): boolean =>
  typeof policy === "object" && policy !== null &&
  (policy as { version?: unknown }).version === 1 &&
  (policy as { hostLeases?: unknown }).hostLeases === "files";

// The transport path is deliberately separate from all file-lease functions above. It never
// reads/writes shared mesh lease files or acquires a mesh/shared-state lock. Its only local
// file is the native successor-proof receipt approved by the D9 ruling, in localStateDir.
export const TRANSPORT_HOST_LEASE_TTL_MS = 15_000;
export const TRANSPORT_HOST_LEASE_RENEW_MS = 5_000;
export const TRANSPORT_HOST_LEASE_SAFETY_MS = 2_000;
export const TRANSPORT_HOST_LEASE_CUSTODY_TIMEOUT_MS = 2_000;
const TRANSPORT_HOST_LEASE_RECEIPT_MAX_BYTES = 4_096;

export interface TransportHostLeaseProcessProof {
  readonly pid: number;
  /** Linux /proc/<pid>/stat field 22, in kernel ticks, not an epoch timestamp. */
  readonly pidStartTime: string;
  readonly bootId: string;
}

export interface TransportHostLeaseValue extends FabricHostLease, TransportHostLeaseProcessProof {
  readonly incarnation: string;
  incarnationToken: string;
}

export class TransportHostLeaseProofError extends Error {
  override readonly name = "TransportHostLeaseProofError";
  readonly code = "FABRIC_HOST_LEASE_PROOF_REQUIRED";
  constructor(readonly reason: "missing" | "invalid" | "unreadable" | "alive" | "timeout", options?: ErrorOptions) {
    super(`Fabric transport host lease predecessor proof refused: ${reason}`, options);
  }
}

export type TransportHostLeaseLossReason = "conflict" | "timeout" | "clock" | "invalid-revision" | "acquire-failed" | "released";

export class TransportHostLeaseLostError extends Error {
  override readonly name = "TransportHostLeaseLostError";
  readonly code = "FABRIC_HOST_LEASE_LOST";
  readonly retryable: boolean;
  constructor(readonly hostId: string, readonly owner: string, readonly reason: TransportHostLeaseLossReason, options?: ErrorOptions) {
    super(`Fabric transport host ${hostId}: lease ${owner} lost (${reason})`, options);
    this.retryable = reason === "timeout";
  }
}

/** Native successor evidence only; KV remains the sole routing authority. Exact D9 schema. */
export interface TransportHostLeaseReceipt {
  readonly ownerHostId: string;
  readonly incarnation: string;
  readonly pid: number;
  readonly processStartTime: string;
  readonly bootId: string;
  readonly acquiredAt: number;
  readonly releasedCleanly: boolean;
  readonly releasedAt?: number;
}

export interface TransportHostLeaseOptions {
  readonly kv: TransportKv;
  readonly self: TransportOwner;
  readonly rootId: string;
  readonly identityId: string;
  readonly incarnation: string;
  /** EXISTING, private, machine-local runtime state directory, NEVER the shared mesh root.
   * Every incarnation/parallel successor of this hostId MUST use the same stable directory.
   * Composition owns this placement; deleting/moving the receipt loses successor evidence. */
  readonly localStateDir: string;
  /** Synchronous notification; called at most once for a terminal ownership loss. */
  readonly onLost?: (error: TransportHostLeaseLostError) => void;
  /** Synchronous notification; called once per recoverable local TTL lapse. */
  readonly onSuspended?: (error: TransportHostLeaseLostError) => void;
  readonly monotonic?: () => number;
}

const transportBootId = (): string => {
  if (process.platform !== "linux") throw new TransportHostLeaseProofError("unreadable");
  try {
    const bootId = fs.readFileSync("/proc/sys/kernel/random/boot_id", "utf8").trim().toLowerCase();
    if (!/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(bootId)) {
      throw new TransportHostLeaseProofError("invalid");
    }
    return bootId;
  } catch (cause) {
    if (cause instanceof TransportHostLeaseProofError) throw cause;
    throw new TransportHostLeaseProofError("unreadable", { cause });
  }
};

const transportProcessStartTime = (pid: number): string => {
  // The command name may itself contain spaces and ')'. Fields after the LAST ')' begin at 3.
  const stat = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
  const close = stat.lastIndexOf(")");
  const fields = stat.slice(close + 1).trim().split(/\s+/);
  const start = fields[19];
  if (close < 0 || !stat.startsWith(`${pid} (`) || !/^[A-Za-z]$/.test(fields[0] ?? "") ||
    !start || !/^[1-9][0-9]*$/.test(start)) throw new TransportHostLeaseProofError("invalid");
  return start;
};

const validTransportProcessProof = (proof: TransportHostLeaseProcessProof): boolean =>
  validWriterPid(proof.pid) && typeof proof.pidStartTime === "string" && /^[1-9][0-9]*$/.test(proof.pidStartTime) &&
  typeof proof.bootId === "string" && /^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(proof.bootId);

/** No kill(pid, 0) fallback: an unreadable/malformed /proc identity is not exit evidence. */
export const readLocalHostLeaseProcessProof = (): TransportHostLeaseProcessProof => {
  const bootId = transportBootId();
  try {
    return Object.freeze({ pid: process.pid, pidStartTime: transportProcessStartTime(process.pid), bootId });
  } catch (cause) {
    if (cause instanceof TransportHostLeaseProofError) throw cause;
    throw new TransportHostLeaseProofError("unreadable", { cause });
  }
};

/** Only a changed boot, a reused PID, or ENOENT on a readable local /proc proves exit.
 * A stopped process (including SIGSTOP), a zombie not yet reaped, and permission errors fail closed. */
export const transportHostLeasePredecessorExited = (proof: TransportHostLeaseProcessProof): boolean => {
  if (!proof || !validTransportProcessProof(proof)) throw new TransportHostLeaseProofError("invalid");
  const local = readLocalHostLeaseProcessProof();
  if (proof.bootId !== local.bootId) return true;
  try {
    return transportProcessStartTime(proof.pid) !== proof.pidStartTime;
  } catch (cause) {
    if ((cause as NodeJS.ErrnoException)?.code === "ENOENT") return true;
    if (cause instanceof TransportHostLeaseProofError) throw cause;
    throw new TransportHostLeaseProofError("unreadable", { cause });
  }
};

const transportLeaseConflict = (error: unknown): boolean =>
  typeof error === "object" && error !== null && "code" in error && error.code === "FABRIC_KV_CONFLICT";

/** One activation incarnation. The directory drives renew() every RENEW_MS; there is no heartbeat
 * loop here. The unreferenced cutoff alarm suspends admission even if nobody calls live(). Every
 * custody/claim fence must still call live(): a paused event loop can delay that alarm. Successful
 * writes are dated conservatively at request START, not delayed PubAck receipt. A suspended lease
 * may recover only by verifying the same local receipt and CASing the broker's current revision;
 * a conflicting owner or invalid local fence permanently loses this object. */
export class TransportHostLease {
  readonly key: string;
  readonly owner: string;
  readonly self: TransportOwner;
  readonly #kv: TransportKv;
  readonly #rootId: string;
  readonly #identityId: string;
  readonly #incarnation: string;
  readonly #localStateDir: string;
  readonly #receiptFile: string;
  readonly #monotonic: () => number;
  readonly #onLost: TransportHostLeaseOptions["onLost"];
  readonly #onSuspended: TransportHostLeaseOptions["onSuspended"];
  readonly #abort = new AbortController();
  #state: "idle" | "acquiring" | "held" | "suspended" | "lost" | "released" = "idle";
  #loss: TransportHostLeaseLostError | undefined;
  #suspension: TransportHostLeaseLostError | undefined;
  #revision = 0;
  #deadline: number | undefined;
  #lastClock: number | undefined;
  #timer: ReturnType<typeof setTimeout> | undefined;
  #processProof: TransportHostLeaseProcessProof | undefined;
  #receipt: TransportHostLeaseReceipt | undefined;
  #pending: Promise<unknown> | undefined;
  #renewing: Promise<TransportHostLeaseValue> | undefined;
  #releasing: Promise<boolean> | undefined;

  constructor(options: TransportHostLeaseOptions) {
    if (options.kv.ttlMs !== TRANSPORT_HOST_LEASE_TTL_MS) throw new TypeError("Transport host leases require a 15000 ms TTL bucket");
    if (!/^[a-z0-9][a-z0-9-]{0,31}$/.test(options.self.machine) ||
      [options.self.hostId, options.rootId, options.identityId, options.incarnation, options.localStateDir]
        .some(value => typeof value !== "string" || value.length === 0) || !path.isAbsolute(options.localStateDir)) {
      throw new TypeError("Invalid transport host lease identity or localStateDir");
    }
    this.#kv = options.kv;
    this.self = Object.freeze({ machine: options.self.machine, hostId: options.self.hostId });
    this.key = `${this.self.machine}.h.${createHash("sha256").update(this.self.hostId).digest("hex").slice(0, 32)}`;
    this.owner = `${this.self.hostId}#${options.incarnation}`;
    this.#rootId = options.rootId;
    this.#identityId = options.identityId;
    this.#incarnation = options.incarnation;
    this.#localStateDir = options.localStateDir;
    this.#receiptFile = path.join(options.localStateDir, `transport-host-lease-${fileName(this.self.hostId)}`);
    this.#monotonic = options.monotonic ?? (() => performance.now());
    this.#onLost = options.onLost;
    this.#onSuspended = options.onSuspended;
  }

  get signal(): AbortSignal { return this.#abort.signal; }

  live(): boolean {
    if (this.#state !== "held") return false;
    try { this.#checkCutoff(); return this.#state === "held"; } catch { return false; }
  }

  /** A TTL lapse stops admission but does not prove a different owner took the lease. */
  recovering(): boolean { return this.#state === "suspended"; }

  acquire(signal?: AbortSignal): Promise<TransportHostLeaseValue> {
    if (this.#state !== "idle") throw this.#loss ?? new Error("Transport host lease acquisition is single-use");
    this.#state = "acquiring";
    const pending = this.#acquire(signal);
    this.#pending = pending;
    return pending;
  }

  /** Concurrent renew calls join one operation; stale revisions are re-read before recovery. */
  renew(signal?: AbortSignal): Promise<TransportHostLeaseValue> {
    signal?.throwIfAborted();
    if (this.#renewing) return this.#renewing;
    if (this.#state !== "suspended") this.#requireLive();
    const pending = this.#state === "suspended" ? this.#reacquire(signal) : this.#renew(signal);
    this.#pending = this.#renewing = pending;
    const clear = (): void => { if (this.#renewing === pending) this.#renewing = undefined; };
    void pending.then(clear, clear);
    return pending;
  }

  /** Close local admission, settle the in-flight CAS, then durably mark clean BEFORE KV delete
   * under the same per-receipt custody as acquisition. A crash/uncertain delete then leaves clean
   * successor evidence, but a successor still cannot create until KV is absent. Even after loss,
   * cleanup is CAS-only and cannot delete a successor. Callers quiesce handlers before release. */
  release(signal?: AbortSignal): Promise<boolean> {
    if (this.#releasing) return this.#releasing;
    this.#state = "released";
    this.#clearTimer();
    this.#abort.abort(this.#loss ?? this.#suspension ?? new TransportHostLeaseLostError(this.self.hostId, this.owner, "released"));
    const pending = this.#pending;
    this.#releasing = (async () => {
      try { await pending; } catch { /* Cleanup still uses any confirmed revision from that write. */ }
      signal?.throwIfAborted();
      if (!this.#receipt) return false;
      return await this.#withReceiptCustody(async () => {
        signal?.throwIfAborted();
        const current = this.#readReceipt();
        if (!current || !this.#sameReceiptOwner(current, this.#receipt!)) return false;
        this.#writeReceipt({ ...current, releasedCleanly: true, releasedAt: Date.now() });
        if (this.#revision === 0) return false;
        return await this.#kv.delete(this.key, this.#revision, signal);
      });
    })();
    return this.#releasing;
  }

  async #acquire(signal?: AbortSignal): Promise<TransportHostLeaseValue> {
    const operationSignal = signal ? AbortSignal.any([signal, this.signal]) : this.signal;
    try {
      operationSignal.throwIfAborted();
      this.#processProof = readLocalHostLeaseProcessProof();
      return await this.#withReceiptCustody(async () => {
        operationSignal.throwIfAborted();
        const predecessor = this.#readReceipt();
        if (predecessor?.incarnation === this.#incarnation) throw new TransportHostLeaseProofError("invalid");
        if (predecessor && !predecessor.releasedCleanly && !transportHostLeasePredecessorExited({
          pid: predecessor.pid, pidStartTime: predecessor.processStartTime, bootId: predecessor.bootId,
        })) throw new TransportHostLeaseProofError("alive");
        // Reserve durably BEFORE the async create: death/uncertainty between these steps must
        // never look like first acquisition. Hold custody through PubAck and local admission.
        const processProof = this.#processProof!;
        const receipt: TransportHostLeaseReceipt = Object.freeze({ ownerHostId: this.self.hostId,
          incarnation: this.#incarnation, pid: processProof.pid, processStartTime: processProof.pidStartTime,
          bootId: processProof.bootId, acquiredAt: Date.now(), releasedCleanly: false });
        this.#writeReceipt(receipt);
        this.#receipt = receipt;
        operationSignal.throwIfAborted();
        const started = this.#clock();
        this.#deadline = started + TRANSPORT_HOST_LEASE_TTL_MS - TRANSPORT_HOST_LEASE_SAFETY_MS;
        this.#armCutoff();
        const value = this.#value();
        const revision = await this.#kv.put(this.key, this.owner, value, 0, operationSignal);
        this.#confirmRevision(revision);
        this.#checkCutoff();
        operationSignal.throwIfAborted();
        this.#state = "held";
        return value;
      });
    } catch (cause) {
      if (this.#state !== "released") this.#lose(transportLeaseConflict(cause) ? "conflict" : "acquire-failed", cause);
      throw this.#loss?.reason === "acquire-failed" ? cause : this.#loss ?? cause;
    }
  }

  async #renew(signal?: AbortSignal): Promise<TransportHostLeaseValue> {
    const operationSignal = signal ? AbortSignal.any([signal, this.signal]) : this.signal;
    try {
      operationSignal.throwIfAborted();
      const started = this.#checkCutoff();
      const value = this.#value();
      let revision: number;
      try {
        revision = await this.#kv.put(this.key, this.owner, value, this.#revision, operationSignal);
      } catch (cause) {
        if (!transportLeaseConflict(cause)) throw cause;
        const current = await this.#kv.get<unknown>(this.key, operationSignal);
        if (current && !this.#isOwnLeaseEntry(current)) throw this.#lose("conflict", cause);
        this.#suspend(cause);
        return await this.#reacquire(signal);
      }
      this.#confirmRevision(revision);
      // Check the OLD deadline first, even if the deadline alarm has not had an event-loop turn.
      this.#requireLive();
      operationSignal.throwIfAborted();
      this.#deadline = started + TRANSPORT_HOST_LEASE_TTL_MS - TRANSPORT_HOST_LEASE_SAFETY_MS;
      this.#armCutoff();
      return value;
    } catch (cause) {
      if (this.#state !== "suspended") this.live(); // Latch a cutoff that elapsed while the request was in flight.
      if (this.#state === "suspended" && cause === this.#suspension) return await this.#reacquire(signal);
      throw this.#loss ?? this.#suspension ?? cause;
    }
  }

  async #reacquire(signal?: AbortSignal): Promise<TransportHostLeaseValue> {
    const operationSignal = signal ? AbortSignal.any([signal, this.signal]) : this.signal;
    try {
      operationSignal.throwIfAborted();
      const processProof = this.#processProof;
      const receipt = this.#receipt;
      if (!processProof || !receipt) throw this.#lose("acquire-failed", new TransportHostLeaseProofError("missing"));
      return await this.#withReceiptCustody(async () => {
        operationSignal.throwIfAborted();
        const currentReceipt = this.#readReceipt();
        if (!currentReceipt || currentReceipt.releasedCleanly || !this.#sameReceiptOwner(currentReceipt, receipt) ||
          currentReceipt.pid !== processProof.pid || currentReceipt.processStartTime !== processProof.pidStartTime ||
          currentReceipt.bootId !== processProof.bootId) {
          throw this.#lose("acquire-failed", new TransportHostLeaseProofError("invalid"));
        }
        for (let attempt = 0; attempt < 3; attempt++) {
          operationSignal.throwIfAborted();
          const started = this.#clock();
          const deadline = started + TRANSPORT_HOST_LEASE_TTL_MS - TRANSPORT_HOST_LEASE_SAFETY_MS;
          const current = await this.#kv.get<unknown>(this.key, operationSignal);
          if (current && !this.#isOwnLeaseEntry(current)) throw this.#lose("conflict");
          const value = this.#value();
          try {
            const revision = await this.#kv.put(this.key, this.owner, value, current?.revision ?? 0, operationSignal);
            this.#confirmRevision(revision);
            if (this.#clock() >= deadline) throw this.#suspend();
            operationSignal.throwIfAborted();
            this.#deadline = deadline;
            this.#suspension = undefined;
            this.#state = "held";
            this.#armCutoff();
            return value;
          } catch (cause) {
            if (this.#loss) throw this.#loss;
            if (!transportLeaseConflict(cause)) throw cause;
            const latest = await this.#kv.get<unknown>(this.key, operationSignal);
            if (latest && !this.#isOwnLeaseEntry(latest)) throw this.#lose("conflict", cause);
          }
        }
        throw new TransportUnavailableError("Transport host lease recovery is contended");
      });
    } catch (cause) {
      if (this.#loss) throw this.#loss;
      if (this.#state === "released") throw this.signal.reason ?? cause;
      throw this.#suspension ?? cause;
    }
  }

  #isOwnLeaseEntry(entry: TransportKvEntry<unknown>): boolean {
    const value = entry.value;
    const proof = this.#processProof;
    if (!proof || typeof value !== "object" || value === null || Array.isArray(value)) return false;
    const lease = value as Record<string, unknown>;
    return entry.key === this.key && entry.machine === this.self.machine && entry.owner === this.owner &&
      lease.id === this.self.hostId && lease.rootId === this.#rootId && lease.identityId === this.#identityId &&
      lease.incarnation === this.#incarnation && lease.incarnationToken === this.#incarnation &&
      lease.pid === proof.pid && lease.pidStartTime === proof.pidStartTime && lease.bootId === proof.bootId;
  }

  #value(): TransportHostLeaseValue {
    if (!this.#processProof) throw new TransportHostLeaseProofError("missing");
    const updatedAt = Date.now();
    return Object.freeze({ id: this.self.hostId, rootId: this.#rootId, identityId: this.#identityId,
      incarnation: this.#incarnation, incarnationToken: this.#incarnation, ...this.#processProof,
      updatedAt, expiresAt: updatedAt + TRANSPORT_HOST_LEASE_TTL_MS });
  }

  #confirmRevision(revision: number): void {
    if (!Number.isSafeInteger(revision) || revision <= this.#revision) throw this.#lose("invalid-revision");
    // Retain confirmation for CAS cleanup even if a release/cutoff won while awaiting PubAck.
    this.#revision = revision;
  }

  #requireLive(): void {
    if (!this.live()) throw this.#loss ?? this.signal.reason ?? new Error("Transport host lease is not acquired");
  }

  #clock(): number {
    let now: number;
    try { now = this.#monotonic(); } catch (cause) { throw this.#lose("clock", cause); }
    if (!Number.isFinite(now) || (this.#lastClock !== undefined && now < this.#lastClock)) throw this.#lose("clock");
    this.#lastClock = now;
    return now;
  }

  #checkCutoff(): number {
    this.signal.throwIfAborted();
    const now = this.#clock();
    if (this.#deadline !== undefined && now >= this.#deadline) throw this.#suspend();
    return now;
  }

  #clearTimer(): void {
    if (this.#timer !== undefined) clearTimeout(this.#timer);
    this.#timer = undefined;
  }

  #armCutoff(): void {
    this.#clearTimer();
    const remaining = this.#deadline! - this.#checkCutoff();
    this.#timer = setTimeout(() => {
      this.#timer = undefined;
      if (this.#state !== "held" && this.#state !== "acquiring") return;
      try { this.#armCutoff(); } catch { /* checkCutoff has already suspended admission. */ }
    }, Math.ceil(remaining));
    this.#timer.unref?.();
  }

  #suspend(cause?: unknown): TransportHostLeaseLostError {
    if (this.#suspension) return this.#suspension;
    this.#suspension = new TransportHostLeaseLostError(this.self.hostId, this.owner, "timeout", { cause });
    this.#state = "suspended";
    this.#clearTimer();
    try { this.#onSuspended?.(this.#suspension); } catch { /* A callback cannot grant authority or mask suspension. */ }
    return this.#suspension;
  }

  #lose(reason: TransportHostLeaseLossReason, cause?: unknown): TransportHostLeaseLostError {
    if (this.#loss) return this.#loss;
    this.#loss = new TransportHostLeaseLostError(this.self.hostId, this.owner, reason, { cause });
    this.#state = "lost";
    this.#clearTimer();
    this.#abort.abort(this.#loss);
    try { this.#onLost?.(this.#loss); } catch { /* A callback cannot mask loss or reopen admission. */ }
    return this.#loss;
  }

  async #withReceiptCustody<T>(operation: () => Promise<T>): Promise<T> {
    if (!this.#processProof) throw new TransportHostLeaseProofError("missing");
    // The runtime directory must already exist: never silently substitute a shared mesh root.
    try {
      if (!fs.statSync(this.#localStateDir).isDirectory()) throw new TransportHostLeaseProofError("invalid");
    } catch (cause) {
      if (cause instanceof TransportHostLeaseProofError) throw cause;
      throw new TransportHostLeaseProofError("unreadable", { cause });
    }
    // First-use only; no new static startup edge. This is the SAME primitive used by
    // MeshStore.leaseCustody, scoped here to a private receipt, never MeshStore or its locks.
    const { acquireMeshCustodyLock } = await import("../mesh/custody-lock.js");
    const unlock = await acquireMeshCustodyLock(`${this.#receiptFile}.custody`, TRANSPORT_HOST_LEASE_CUSTODY_TIMEOUT_MS,
      { hostQualified: true, ownIncarnation: this.#processProof.pidStartTime, lockProtocol: 2 });
    try { return await operation(); } finally { unlock(); }
  }

  #writeReceipt(receipt: TransportHostLeaseReceipt): void {
    if (Buffer.byteLength(JSON.stringify(receipt)) > TRANSPORT_HOST_LEASE_RECEIPT_MAX_BYTES) {
      throw new TransportHostLeaseProofError("invalid");
    }
    writeJsonAtomic(this.#receiptFile, receipt, { durable: true });
  }

  #readReceipt(): TransportHostLeaseReceipt | undefined {
    let fd: number;
    try { fd = fs.openSync(this.#receiptFile, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW | fs.constants.O_NONBLOCK); }
    catch (cause) {
      if ((cause as NodeJS.ErrnoException)?.code === "ENOENT") return undefined;
      throw new TransportHostLeaseProofError("unreadable", { cause });
    }
    try {
      const stat = fs.fstatSync(fd);
      if (!stat.isFile() || stat.size > TRANSPORT_HOST_LEASE_RECEIPT_MAX_BYTES) throw new TransportHostLeaseProofError("invalid");
      const text = fs.readFileSync(fd, "utf8");
      if (Buffer.byteLength(text) > TRANSPORT_HOST_LEASE_RECEIPT_MAX_BYTES) throw new TransportHostLeaseProofError("invalid");
      const value: unknown = JSON.parse(text);
      if (typeof value !== "object" || value === null || Array.isArray(value)) throw new TransportHostLeaseProofError("invalid");
      const receipt = value as Record<string, unknown>;
      const keys = ["ownerHostId", "incarnation", "pid", "processStartTime", "bootId", "acquiredAt", "releasedCleanly", "releasedAt"];
      if (Object.keys(receipt).some(key => !keys.includes(key)) || receipt.ownerHostId !== this.self.hostId ||
        typeof receipt.incarnation !== "string" || !receipt.incarnation || !validWriterPid(receipt.pid) ||
        typeof receipt.processStartTime !== "string" || !/^[1-9][0-9]*$/.test(receipt.processStartTime) ||
        typeof receipt.bootId !== "string" || !/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(receipt.bootId) ||
        !Number.isSafeInteger(receipt.acquiredAt) || (receipt.acquiredAt as number) <= 0 ||
        typeof receipt.releasedCleanly !== "boolean" ||
        (receipt.releasedAt !== undefined && (!receipt.releasedCleanly || !Number.isSafeInteger(receipt.releasedAt) || (receipt.releasedAt as number) <= 0))) {
        throw new TransportHostLeaseProofError("invalid");
      }
      return value as TransportHostLeaseReceipt;
    } catch (cause) {
      if (cause instanceof TransportHostLeaseProofError) throw cause;
      throw new TransportHostLeaseProofError(cause instanceof SyntaxError ? "invalid" : "unreadable", { cause });
    } finally { fs.closeSync(fd); }
  }

  #sameReceiptOwner(left: TransportHostLeaseReceipt, right: TransportHostLeaseReceipt): boolean {
    return left.ownerHostId === right.ownerHostId && left.incarnation === right.incarnation &&
      left.pid === right.pid && left.processStartTime === right.processStartTime && left.bootId === right.bootId &&
      left.acquiredAt === right.acquiredAt;
  }
}
