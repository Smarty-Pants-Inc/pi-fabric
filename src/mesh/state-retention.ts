import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { syncPathNamespaceAsync } from "../core/atomic-write.js";
import { hostFromEntry, participantFromEntry } from "../topology/record-validation.js";
import type { MeshStateEntry } from "./store.js";
import type { FabricHostLease } from "../topology/host-leases.js";

const RETENTION_MS = 6 * 60 * 60 * 1000;
const MAX_EXPIRED_PER_COMMIT = 500;
// Count attempts (including failed confirmations), not successful deletions.
const MAX_RECEIPTS_PER_PASS = 16;
const MAX_SCANNED_PER_PASS = 500;
export const RECEIPT_PASS_BUDGET_MS = 50;
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
export interface ConfirmedReceiptCandidate {
  entry: MeshStateEntry;
  file: string;
  fingerprint: string;
}

// A terminal result or a status flag is not an acknowledgement. Keep F1's fresh
// opened-file, complete namespace and post-barrier fingerprint checks, but perform
// the slow barriers asynchronously, outside BOTH the mesh lock and foreground writes.
const confirmDeliveryReceipt = async (
  root: string, entry: MeshStateEntry, cutoff: number,
): Promise<ConfirmedReceiptCandidate | undefined> => {
  const value = record(entry.value)!;
  const id = value.agentCompletionId as string;
  const file = path.join(root, "agent-completions", "receipts", `${hash(id)}.json`);
  let handle: Awaited<ReturnType<typeof fs.promises.open>> | undefined;
  try {
    handle = await fs.promises.open(file, process.platform === "win32" ? "r+" : "r");
    const stat = await handle.stat();
    if (!stat.isFile()) return undefined;
    const receipt = record(JSON.parse(await handle.readFile("utf8")));
    if (receipt?.id !== id || typeof receipt.sessionId !== "string" || !receipt.sessionId ||
      !time(receipt.consumedAt) || receipt.consumedAt >= cutoff) return undefined;
    await handle.sync();
    await syncPathNamespaceAsync(file, stat);
    if (fingerprint(await handle.stat()) !== fingerprint(stat)) return undefined;
    return { entry, file, fingerprint: fingerprint(stat) };
  } catch { /* absent, malformed or unconfirmed durability never authorizes expiry */ }
  finally { await handle?.close().catch(() => undefined); }
  return undefined;
};

/** One background pass. Its cursor advances over failed confirmations too, so an
 * unreadable prefix cannot starve the rest. Never start another receipt after the
 * deadline; an already-started asynchronous barrier can finish without blocking Main.
 * Unprocessed work is revisited on the next heartbeat, not in an immediate drain. */
export const prepareReceiptCompaction = async (
  entries: MeshStateEntry[], root: string, now: number, after: string | undefined, deadline: number,
): Promise<{ confirmed: ConfirmedReceiptCandidate[]; after: string | undefined }> => {
  const cutoff = now - RETENTION_MS;
  const confirmed: ConfirmedReceiptCandidate[] = [];
  let scanned = 0, attempts = 0;
  let cursor = after;
  for (const entry of entries) {
    if (after !== undefined && entry.key <= after) continue;
    if (scanned >= MAX_SCANNED_PER_PASS || attempts >= MAX_RECEIPTS_PER_PASS || performance.now() >= deadline) {
      return { confirmed, after: cursor };
    }
    scanned++;
    cursor = entry.key;
    const value = record(entry.value);
    if (!time(entry.updatedAt) || entry.updatedAt >= cutoff || value?.format !== 1 ||
      typeof value.id !== "string" || typeof value.rootId !== "string" ||
      entry.key !== `${DELIVERIES}${hash(value.rootId).slice(0, 32)}/${value.id}` ||
      !time(value.createdAt) || value.createdAt >= cutoff ||
      typeof value.agentCompletionId !== "string" || record(value.from)?.kind !== "agent" ||
      record(value.from)?.id !== value.agentCompletionId) continue;
    attempts++;
    const candidate = await confirmDeliveryReceipt(root, entry, cutoff);
    if (candidate) confirmed.push(candidate);
  }
  return { confirmed, after: undefined }; // wrap only on the next pass
};

/** Only cheap checks under the mutation fence; no receipt reads or barriers here.
 * Compare complete entries as well as metadata: legacy writers can copy CAS labels. */
export const receiptCandidateUnchanged = (
  candidate: ConfirmedReceiptCandidate, current: MeshStateEntry | undefined,
): boolean => {
  if (!current || JSON.stringify(current) !== JSON.stringify(candidate.entry)) return false;
  try { return fingerprint(fs.statSync(candidate.file)) === candidate.fingerprint; }
  catch { return false; }
};

/** Participant expiry only; receipt barriers never run in an ordinary commit. */
export const expiredStateKeys = (entries: Record<string, MeshStateEntry>, root: string, now: number): string[] => {
  const cutoff = now - RETENTION_MS;
  const expired: string[] = [];
  for (const [key, entry] of Object.entries(entries)) {
    if (expired.length >= MAX_EXPIRED_PER_COMMIT) break;
    if (!time(entry.updatedAt) || entry.updatedAt >= cutoff) continue;
    const value = record(entry.value);
    if (!value || value.format !== 1) continue;
    if (key.startsWith(PARTICIPANTS)) {
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
