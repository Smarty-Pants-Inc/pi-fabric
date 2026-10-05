import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { syncPathNamespace } from "../core/atomic-write.js";
import { hostFromEntry, participantFromEntry } from "../topology/record-validation.js";
import type { MeshStateEntry } from "./store.js";
import type { FabricHostLease } from "../topology/host-leases.js";

const RETENTION_MS = 6 * 60 * 60 * 1000;
const MAX_EXPIRED_PER_COMMIT = 500;
const PARTICIPANTS = "topology/participants/";
const HOSTS = "topology/hosts/";
const DELIVERIES = "residency/deliveries/";
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

const fingerprint = (stat: fs.Stats): string => JSON.stringify([stat.dev, stat.ino, stat.size, stat.mtimeMs, stat.ctimeMs]);
// A terminal result or a status flag is not an acknowledgement. Like confirmReceipt
// in completion-journal.ts, every destructive cleanup confirms the opened receipt
// AND its complete reopenable namespace afresh. A visible failed rename is retained.
const deliverySettledAt = (root: string, value: Record<string, unknown>, cutoff: number): number | undefined => {
  const from = record(value.from);
  const id = value.agentCompletionId;
  if (typeof id !== "string" || from?.kind !== "agent" || from.id !== id) return undefined;
  const file = path.join(root, "agent-completions", "receipts", `${hash(id)}.json`);
  let fd: number | undefined;
  try {
    fd = fs.openSync(file, process.platform === "win32" ? "r+" : "r");
    const stat = fs.fstatSync(fd);
    if (!stat.isFile()) return undefined;
    const receipt = record(JSON.parse(fs.readFileSync(fd, "utf8")));
    if (receipt?.id !== id || typeof receipt.sessionId !== "string" || !receipt.sessionId ||
      !time(receipt.consumedAt) || receipt.consumedAt >= cutoff) return undefined;
    fs.fsyncSync(fd);
    syncPathNamespace(file, stat);
    if (fingerprint(fs.fstatSync(fd)) !== fingerprint(stat)) return undefined;
    return receipt.consumedAt;
  } catch { /* absent, malformed or unconfirmed durability never authorizes expiry */ }
  finally { if (fd !== undefined) fs.closeSync(fd); }
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
      const settledAt = deliverySettledAt(root, value, cutoff);
      if (settledAt !== undefined && settledAt < cutoff) expired.push(key);
    } else if (key.startsWith(PARTICIPANTS)) {
      const participant = participantFromEntry(entry);
      if (!participant) continue;
      const hostEntry = entries[HOSTS + hash(participant.ownerHostId)];
      const host = hostEntry && hostFromEntry(hostEntry);
      // Absence, malformed host, wrong writer, takeover or publication gap is uncertainty.
      if (!host || host.id !== participant.ownerHostId || host.rootId !== participant.rootId ||
        host.identity.id !== participant.ownerIdentityId || !time(host.startedAt) ||
        !time(host.expiresAt) || host.expiresAt >= cutoff ||
        !time(host.updatedAt) || host.updatedAt >= cutoff ||
        !time(hostEntry.updatedAt) || hostEntry.updatedAt >= cutoff) continue;
      // An uncached read immediately before selection: failed reads cannot reuse stale expiry.
      const observation = readHostLeaseForExpiry(root, participant.ownerHostId);
      if (observation.uncertain) continue;
      const lease = observation.lease;
      // Even a mismatched/incarnation file is retained conservatively; no takeover inference.
      if (lease && (lease.rootId !== participant.rootId || lease.identityId !== participant.ownerIdentityId ||
        (lease.startedAt !== undefined && lease.startedAt !== host.startedAt) ||
        lease.expiresAt >= cutoff || lease.updatedAt >= cutoff)) continue;
      expired.push(key);
    }
  }
  return expired;
};
