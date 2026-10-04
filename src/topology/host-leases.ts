import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { readFileRetrying, writeJsonAtomic } from "../core/atomic-write.js";
import { effectiveLiveness, type Liveness } from "./liveness.js";
import type { MeshStateEntry } from "../mesh/store.js";

// Host lease renewals outside the shared state (smarty-dev#816). Every heartbeat rewrote the
// whole shared state under the one mesh lock, and heartbeats were 78% of all locked writes. Each
// host now also renews its lease in a file of its own, replaced by an atomic rename without
// the lock; one host writes each file.

/**
 * Host-reserved historical policy key (also used for participant-file migration).
 * Directory liveness now negotiates file-only renewals through livenessLeaseFiles: 1;
 * a live older peer always restores half-life state renewal, without an operator switch.
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

export interface FabricHostLease {
  id: string;
  rootId: string;
  identityId: string;
  updatedAt: number;
  expiresAt: number;
  /** Incarnation fence; absent on pre-lease-capability writers. */
  startedAt?: number;
  /** Main session has a fixed 15 s TTL, independent of the host TTL. */
  session?: Liveness & { id: string; startedAt: number };
}

const fileName = (hostId: string): string =>
  createHash("sha256").update(hostId).digest("hex").slice(0, 32) + ".json";

export const writeHostLease = (meshRoot: string, lease: FabricHostLease): void =>
  writeJsonAtomic(path.join(meshRoot, LEASE_DIR, fileName(lease.id)), { format: 1, ...lease });

export const removeHostLease = (meshRoot: string, hostId: string): void =>
  fs.rmSync(path.join(meshRoot, LEASE_DIR, fileName(hostId)), { force: true });

// A file that could not be read gives no answer to cache (`read: false`): the next lookup reads it
// again. Only a file that was read, valid or not, is cached by its filesystem identity.
const parseLease = (file: string, name: string): { read: boolean; lease?: FabricHostLease | undefined } => {
  let text: string;
  try {
    text = readFileRetrying(file);
  } catch {
    return { read: false };
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
      (value.startedAt !== undefined && (typeof value.startedAt !== "number" || !Number.isFinite(value.startedAt)))
    ) return undefined;
    return {
      id: value.id, rootId: value.rootId, identityId: value.identityId,
      updatedAt: value.updatedAt, expiresAt: value.expiresAt,
      ...(typeof value.startedAt === "number" && Number.isFinite(value.startedAt) ? { startedAt: value.startedAt } : {}),
      ...(validSession(value.session) ? { session: value.session } : {}),
    };
  } catch {
    return undefined;
  }
};

// Parsed files by directory and name, reused while a file's identity and timestamps are unchanged.
const cache = new Map<string, LeaseSlots>();

/** Every host's file lease, by host id. Unreadable or misnamed files are skipped. */
export const readHostLeases = (meshRoot: string): Map<string, FabricHostLease> => {
  const dir = path.join(meshRoot, LEASE_DIR);
  let names: string[];
  try {
    names = fs.readdirSync(dir);
  } catch {
    return new Map();
  }
  const known = cache.get(dir) ?? new Map();
  cache.set(dir, known);
  const leases = new Map<string, FabricHostLease>();
  const present = new Set<string>();
  for (const name of names) {
    if (!name.endsWith(".json")) continue;
    present.add(name);
    let stat: fs.Stats;
    try {
      stat = fs.statSync(path.join(dir, name));
    } catch {
      continue;
    }
    const lease = cachedLease(known, dir, name, stat);
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

/** Stat and parsed lease from one filesystem observation, never the shared state. */
export const readHostLeaseSnapshot = (meshRoot: string, hostId: string): {
  lease: FabricHostLease | undefined; mtimeMs: number;
} | undefined => {
  const dir = path.join(meshRoot, LEASE_DIR);
  const name = fileName(hostId);
  let stat: fs.Stats;
  try {
    stat = fs.statSync(path.join(dir, name));
  } catch {
    return undefined;
  }
  const known = cache.get(dir) ?? new Map();
  cache.set(dir, known);
  return { lease: cachedLease(known, dir, name, stat), mtimeMs: stat.mtimeMs };
};

type LeaseSlots = Map<string, Pick<fs.Stats, "dev" | "ino" | "size" | "mtimeMs" | "ctimeMs"> & {
  lease: FabricHostLease | undefined;
}>;

const cachedLease = (known: LeaseSlots, dir: string, name: string, stat: fs.Stats): FabricHostLease | undefined => {
  const slot = known.get(name);
  // Atomic replacement can preserve size and mtime. Compare file identity too; timestamps
  // remain the fallback on filesystems (including Windows) without useful dev/ino values.
  if (slot && slot.dev === stat.dev && slot.ino === stat.ino && slot.size === stat.size &&
    slot.mtimeMs === stat.mtimeMs && slot.ctimeMs === stat.ctimeMs) return slot.lease;
  const parsed = parseLease(path.join(dir, name), name);
  if (!parsed.read) return slot?.lease;                     // unreadable for now: keep the last answer
  known.set(name, {
    dev: stat.dev, ino: stat.ino, size: stat.size, mtimeMs: stat.mtimeMs, ctimeMs: stat.ctimeMs,
    lease: parsed.lease,
  });
  return parsed.lease;
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
