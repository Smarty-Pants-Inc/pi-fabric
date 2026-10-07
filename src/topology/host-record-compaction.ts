import { createHash } from "node:crypto";
import type { MeshBatchOperation, MeshBatchView, MeshStateEntry } from "../mesh/store.js";
import { hostEntryLiveness, readHostLease } from "./host-leases.js";

/** Native Mains and resident brokers both advertise topology host records. Never expire their
 * participants, deliveries, completion claims, result journals or explicit lineage receipts here. */
export const HOST_RECORD_RETENTION_MS = 6 * 60 * 60 * 1000;
export const HOST_RECORD_COMPACTION_BATCH = 64;
const PREFIX = "topology/hosts/";
const object = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);
const expired = (entry: MeshStateEntry, root: string, ownHostId: string, cutoff: number): boolean => {
  const host = entry.value;
  if (!object(host) || host.format !== 1 || typeof host.id !== "string" || host.id === ownHostId ||
    entry.key !== PREFIX + createHash("sha256").update(host.id).digest("hex") ||
    !object(host.identity) || host.identity.id !== entry.updatedBy.id ||
    typeof host.rootId !== "string" || typeof host.startedAt !== "number" || !Number.isFinite(host.startedAt) ||
    typeof host.expiresAt !== "number" || !Number.isFinite(host.expiresAt) ||
    !Number.isFinite(entry.updatedAt) || entry.updatedAt > cutoff) return false;
  // readHostLease is an exact, fresh metadata-gated file read. Incarnation/root/identity checks
  // in hostEntryLiveness prevent an unrelated file from extending or shortening this record.
  const lease = readHostLease(root, host.id);
  return hostEntryLiveness(entry, lease ? new Map([[host.id, lease]]) : new Map()).expiresAt < cutoff;
};

/** Prepare only from the authoritative view of an ALREADY-needed write. No separate sweep,
 * no timer and no extra state read/lock acquisition. Revalidate file liveness just before delete. */
export const compactExpiredHostRecords = (view: MeshBatchView, root: string, ownHostId: string,
  now = Date.now()): MeshBatchOperation[] => {
  const cutoff = now - HOST_RECORD_RETENTION_MS;
  const ops: MeshBatchOperation[] = [];
  for (const entry of view.listAll(PREFIX)) {
    if (!expired(entry, root, ownHostId, cutoff)) continue;
    ops.push({ kind: "delete", key: entry.key, ifVersion: entry.version, onConflict: "skip",
      condition: current => {
        const latest = current(entry.key);
        return latest !== undefined && expired(latest, root, ownHostId, cutoff);
      } });
    if (ops.length >= HOST_RECORD_COMPACTION_BATCH) break;
  }
  return ops;
};
