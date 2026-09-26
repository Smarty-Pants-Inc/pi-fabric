import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { writeJsonAtomic } from "../core/atomic-write.js";

// Host lease renewals outside the shared state (smarty-dev#816). Every heartbeat rewrote the
// whole shared state under the one mesh lock, and heartbeats were 78% of all locked writes. Each
// host now also renews its lease in a file of its own, replaced by an atomic rename without
// the lock; one host writes each file.

/**
 * Host-reserved policy key. With { version: 1, hostLeases: "files" }, set by the fleet owner once
 * every runtime reads file leases, a renewal that changes nothing writes only the file.
 */
export const LIVENESS_POLICY_KEY = "topology/liveness";
/**
 * Under that policy a host still renews its shared-state record this often, so no reaper of any
 * version (they remove hosts gone for hours) takes a live host for a dead one.
 */
export const STATE_LEASE_RENEW_MS = 10 * 60 * 1000;

const LEASE_DIR = "host-leases";

export interface FabricHostLease {
  id: string;
  rootId: string;
  identityId: string;
  updatedAt: number;
  expiresAt: number;
}

const fileName = (hostId: string): string =>
  createHash("sha256").update(hostId).digest("hex").slice(0, 32) + ".json";

export const writeHostLease = (meshRoot: string, lease: FabricHostLease): void =>
  writeJsonAtomic(path.join(meshRoot, LEASE_DIR, fileName(lease.id)), { format: 1, ...lease });

export const removeHostLease = (meshRoot: string, hostId: string): void =>
  fs.rmSync(path.join(meshRoot, LEASE_DIR, fileName(hostId)), { force: true });

const parseLease = (file: string, name: string): FabricHostLease | undefined => {
  try {
    const value = JSON.parse(fs.readFileSync(file, "utf8")) as Record<string, unknown>;
    if (
      value?.format !== 1 ||
      typeof value.id !== "string" ||
      fileName(value.id) !== name ||
      typeof value.rootId !== "string" ||
      typeof value.identityId !== "string" ||
      typeof value.updatedAt !== "number" ||
      typeof value.expiresAt !== "number"
    ) return undefined;
    return {
      id: value.id, rootId: value.rootId, identityId: value.identityId,
      updatedAt: value.updatedAt, expiresAt: value.expiresAt,
    };
  } catch {
    return undefined;
  }
};

// Parsed files by directory and name, reused while a file's mtime is unchanged.
const cache = new Map<string, Map<string, { mtimeMs: number; size: number; lease: FabricHostLease | undefined }>>();

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
    let slot = known.get(name);
    if (!slot || slot.mtimeMs !== stat.mtimeMs || slot.size !== stat.size) {
      slot = { mtimeMs: stat.mtimeMs, size: stat.size, lease: parseLease(path.join(dir, name), name) };
      known.set(name, slot);
    }
    if (slot.lease) leases.set(slot.lease.id, slot.lease);
  }
  for (const name of known.keys()) if (!present.has(name)) known.delete(name);
  return leases;
};

/** One host's file lease, from the same cache; for a single-participant lookup. */
export const readHostLease = (meshRoot: string, hostId: string): FabricHostLease | undefined => {
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
  let slot = known.get(name);
  if (!slot || slot.mtimeMs !== stat.mtimeMs || slot.size !== stat.size) {
    slot = { mtimeMs: stat.mtimeMs, size: stat.size, lease: parseLease(path.join(dir, name), name) };
    known.set(name, slot);
  }
  return slot.lease;
};

/** A host lease's effective expiry: the later of its shared-state lease and its file lease. */
export const hostLeaseExpiry = (
  leases: ReadonlyMap<string, FabricHostLease>,
  host: { id: string; rootId: string; identity: { id: string }; expiresAt: number },
): number => {
  const lease = leases.get(host.id);
  return lease && lease.rootId === host.rootId && lease.identityId === host.identity.id
    ? Math.max(host.expiresAt, lease.expiresAt)
    : host.expiresAt;
};

/** Whether the fleet owner has moved lease renewals to files (see LIVENESS_POLICY_KEY). */
export const fileLeasesOnly = (policy: unknown): boolean =>
  typeof policy === "object" && policy !== null &&
  (policy as { version?: unknown }).version === 1 &&
  (policy as { hostLeases?: unknown }).hostLeases === "files";
