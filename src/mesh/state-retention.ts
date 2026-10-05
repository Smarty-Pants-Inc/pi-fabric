import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import type { MeshStateEntry } from "./store.js";
import type { FabricHostLease } from "../topology/host-leases.js";

const RETENTION_MS = 6 * 60 * 60 * 1000;
const MAX_EXPIRED_PER_COMMIT = 500;
const PARTICIPANTS = "topology/participants/";
const HOSTS = "topology/hosts/";
const DELIVERIES = "residency/deliveries/";
const terminal = new Set(["completed", "failed", "stopped", "timed_out", "cancelled"]);
const hash = (id: string): string => createHash("sha256").update(id).digest("hex");
const record = (value: unknown): Record<string, unknown> | undefined =>
  typeof value === "object" && value !== null && !Array.isArray(value) ? value as Record<string, unknown> : undefined;
const time = (value: unknown): value is number => typeof value === "number" && Number.isFinite(value) && value > 0;

// The canonical host-leases.ts filename/schema contract, like residency/launcher.ts.
// Keep this raw, uncached reader lazy: importing the live lease cache here extracts
// another eager shared chunk. Failed reads must not reuse an old cached expiry.
const readHostLeaseForExpiry = (root: string, id: string): { uncertain: boolean; lease?: FabricHostLease } => {
  try {
    const lease = record(JSON.parse(fs.readFileSync(path.join(root, "host-leases", `${hash(id).slice(0, 32)}.json`), "utf8")));
    if (lease?.format !== 1 || lease.id !== id || typeof lease.rootId !== "string" || typeof lease.identityId !== "string" ||
      !time(lease.updatedAt) || !time(lease.expiresAt) ||
      (lease.startedAt !== undefined && !time(lease.startedAt))) return { uncertain: true };
    return { uncertain: false, lease: lease as unknown as FabricHostLease };
  } catch (error) {
    // Transient Windows open failures are uncertainty too. Only ENOENT is absence.
    return { uncertain: !(error instanceof Error && "code" in error && error.code === "ENOENT") };
  }
};

// A terminal RESULT in data is still pending delivery. Only envelope disposition or a
// validated durable consumption receipt proves that custody has ended. Receipt layout is
// the completion-journal.ts contract; do not import its replay engine into the mesh graph.
const deliverySettledAt = (root: string, value: Record<string, unknown>): number | undefined => {
  if ((value.completedAt !== undefined && !time(value.completedAt)) ||
    (value.acknowledgedAt !== undefined && !time(value.acknowledgedAt))) return undefined;
  const dispositionAt = Math.max(time(value.completedAt) ? value.completedAt : 0, time(value.acknowledgedAt) ? value.acknowledgedAt : 0);
  if (typeof value.status === "string" && terminal.has(value.status)) return dispositionAt;
  if (value.acknowledged === true || value.status === "acknowledged" || time(value.acknowledgedAt)) return dispositionAt;
  const from = record(value.from);
  const id = value.agentCompletionId;
  if (typeof id !== "string" || from?.kind !== "agent" || from.id !== id) return undefined;
  try {
    const receipt = record(JSON.parse(fs.readFileSync(path.join(root, "agent-completions", "receipts", `${hash(id)}.json`), "utf8")));
    if (receipt?.id === id && typeof receipt.sessionId === "string" && receipt.sessionId && time(receipt.consumedAt)) return receipt.consumedAt;
  } catch { /* absent, unreadable or malformed receipt is not acknowledgement */ }
  return undefined;
};

/** Called only inside an existing state commit. No timer or separate locked write. */
export const expiredStateKeys = (entries: Record<string, MeshStateEntry>, root: string, now: number): string[] => {
  const cutoff = now - RETENTION_MS;
  const expired: string[] = [];
  for (const [key, entry] of Object.entries(entries)) {
    if (expired.length >= MAX_EXPIRED_PER_COMMIT) break;
    if (!time(entry.updatedAt) || entry.updatedAt >= cutoff) continue;
    const value = record(entry.value);
    if (!value || value.format !== 1) continue;
    if (key.startsWith(DELIVERIES)) {
      if (typeof value.id !== "string" || typeof value.rootId !== "string" ||
        key !== `${DELIVERIES}${hash(value.rootId).slice(0, 32)}/${value.id}` ||
        !time(value.createdAt) || value.createdAt >= cutoff) continue;
      const settledAt = deliverySettledAt(root, value);
      if (settledAt !== undefined && settledAt < cutoff) expired.push(key);
    } else if (key.startsWith(PARTICIPANTS)) {
      if (typeof value.id !== "string" || key !== PARTICIPANTS + hash(value.id) ||
        typeof value.ownerHostId !== "string" || typeof value.ownerIdentityId !== "string" ||
        typeof value.rootId !== "string") continue;
      const hostEntry = entries[HOSTS + hash(value.ownerHostId)];
      const host = record(hostEntry?.value);
      const identity = record(host?.identity);
      // Absence, malformed host, takeover or publication gap is uncertainty, not death.
      if (!host || host.format !== 1 || host.id !== value.ownerHostId || host.rootId !== value.rootId ||
        identity?.id !== value.ownerIdentityId || !time(host.expiresAt) || host.expiresAt >= cutoff ||
        !time(host.updatedAt) || host.updatedAt >= cutoff ||
        !time(hostEntry?.updatedAt) || hostEntry.updatedAt >= cutoff) continue;
      // An uncached read immediately before selection: failed reads cannot reuse stale expiry.
      const observation = readHostLeaseForExpiry(root, value.ownerHostId);
      if (observation.uncertain) continue;
      const lease = observation.lease;
      // Even a mismatched/incarnation file is retained conservatively; no takeover inference.
      if (lease && (lease.rootId !== value.rootId || lease.identityId !== value.ownerIdentityId ||
        (lease.startedAt !== undefined && lease.startedAt !== host.startedAt) ||
        lease.expiresAt >= cutoff || lease.updatedAt >= cutoff)) continue;
      expired.push(key);
    }
  }
  return expired;
};
