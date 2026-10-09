import { createHash } from "node:crypto";
import fs from "node:fs";
import { ResidentActorAuthorizationError, residentHostId, type ResidentHostConfig } from "./protocol.js";
import type { MeshStateEntry, MeshStore } from "../mesh/store.js";
import { participantFilePresent, readParticipantFile } from "../topology/participant-files.js";
import { hostEntryLiveness, hostLeasePath, readHostLeaseCurrent } from "../topology/host-leases.js";

export interface ResidentOperatorEvidence {
  rootId: string;
  mainSessionId: string;
  lastLeaseTime: number | null;
  leaseExpiresAt: number | null;
  liveLease: boolean;
  /** A live root lease that this root's own resident host writes, with no live Main session in it (smarty-dev#7817). */
  residentRenewedLease?: true;
  operatorCheck: string;
}

/** The process that may renew a Main-less root's lease itself: the resident host of that root. */
export interface ResidentLeaseWriter { pid: number; host: string; startedAt: number }

const operatorCheck = "Check the root's Main session and last lease time; run herdr agent list / ps to confirm that no live Main serves this root. A wrong confirmation can interrupt a live Main's actor.";
const refuse = (evidence: ResidentOperatorEvidence, reason: string): never => {
  throw new ResidentActorAuthorizationError(`${reason}; ${JSON.stringify(evidence)}; only after the manual check use --confirm-dead-root ${evidence.rootId}`);
};

/** Report lease facts only. An absent/expired lease never proves that Main is dead. */
export function readResidentOperatorEvidence(config: ResidentHostConfig, mesh: Pick<MeshStore, "get">,
  resident?: ResidentLeaseWriter): ResidentOperatorEvidence {
  const evidence: ResidentOperatorEvidence = { rootId: config.rootId, mainSessionId: config.sessionId,
    lastLeaseTime: null, leaseExpiresAt: null, liveLease: false, operatorCheck };
  // smarty-dev#7817: only a live lease proven to be the resident's own heartbeat (its exact writer
  // incarnation, no live Main session) is set aside; any other live lease, or doubt, is a live Main.
  let mainLive = false;
  let selfIncarnation: number | undefined;
  let selfWindow = 0;
  const record = (updatedAt: number, expiresAt: number, self = false) => {
    evidence.lastLeaseTime = Math.max(evidence.lastLeaseTime ?? updatedAt, updatedAt);
    evidence.leaseExpiresAt = Math.max(evidence.leaseExpiresAt ?? expiresAt, expiresAt);
    if (expiresAt < Date.now()) return;
    evidence.liveLease = true;
    if (self) evidence.residentRenewedLease = true; else mainLive = true;
  };
  const file = hostLeasePath(config.meshRoot, config.rootId);
  let present = false;
  try { fs.lstatSync(file); present = true; }
  catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") refuse(evidence, "root lease is unreadable"); }
  if (present) {
    const lease = readHostLeaseCurrent(config.meshRoot, config.rootId);
    if (!lease || lease.rootId !== config.rootId || lease.identityId !== config.rootId) {
      refuse(evidence, "root lease is unreadable or invalid");
    }
    const writer = lease!.writer;
    const self = resident !== undefined && writer !== undefined && writer.pid === resident.pid &&
      writer.host === resident.host && writer.startedAt === resident.startedAt &&
      (lease!.session === undefined || lease!.session.expiresAt < Date.now());
    if (self) { selfIncarnation = lease!.startedAt; selfWindow = Math.max(0, lease!.expiresAt - lease!.updatedAt); }
    record(lease!.updatedAt, lease!.expiresAt, self);
  }
  const shared = mesh.get("topology/hosts/" + createHash("sha256").update(config.rootId).digest("hex"), { fresh: true });
  if (shared) {
    const value = shared.value as { id?: string; rootId?: string; expiresAt?: number; identity?: { id?: string } };
    if (!value || value.id !== config.rootId || value.rootId !== config.rootId ||
        value.identity?.id !== config.rootId || !Number.isFinite(value.expiresAt)) refuse(evidence, "shared root lease is invalid");
    const lease = hostEntryLiveness(shared, new Map());
    // The shared record of the same incarnation as the resident's own lease is that same heartbeat.
    record(lease.updatedAt, lease.expiresAt, selfIncarnation !== undefined &&
      (value as { startedAt?: number }).startedAt === selfIncarnation);
  }
  // A resident-renewed lease is set aside only when no Main presence for the root is live in that
  // lease window: the root's participant record, unless the resident itself owns it (smarty-dev#7817).
  if (!mainLive && evidence.residentRenewedLease) {
    const key = "topology/participants/" + createHash("sha256").update(config.rootId).digest("hex");
    const now = Date.now();
    let entries: Array<MeshStateEntry | undefined>;
    try { entries = [readParticipantFile(config.meshRoot, key), mesh.get(key, { fresh: true })]; }
    catch { entries = []; mainLive = true; }
    for (const entry of entries) {
      if (!entry) continue;
      const value = entry.value as { id?: unknown; ownerHostId?: unknown; status?: unknown; reloadUntil?: unknown } | null;
      if (!value || value.id !== config.rootId || typeof value.ownerHostId !== "string") { mainLive = true; continue; }
      if (value.ownerHostId === residentHostId(config.rootId)) continue;
      const owner = value.ownerHostId === config.rootId ? undefined : readHostLeaseCurrent(config.meshRoot, value.ownerHostId);
      if (entry.updatedAt + selfWindow >= now || (owner !== undefined && owner.expiresAt >= now) ||
          (value.status === "reloading" && typeof value.reloadUntil === "number" && value.reloadUntil >= now)) mainLive = true;
    }
    if (!entries[0] && participantFilePresent(config.meshRoot, key)) mainLive = true; // present but unreadable: doubt
    if (mainLive) evidence.liveLease = true;
  }
  if (!mainLive && evidence.liveLease) evidence.liveLease = false;
  if (mainLive) delete evidence.residentRenewedLease;
  return evidence;
}

export function assertResidentOperatorConfirmed(evidence: ResidentOperatorEvidence, confirmation?: string, dryRun = false): void {
  if (dryRun && confirmation === undefined) return;
  if (confirmation !== evidence.rootId) refuse(evidence, confirmation === undefined
    ? "Missing --confirm-dead-root: operator confirmation is required"
    : "Mismatched --confirm-dead-root: value must equal the selected resident's root id exactly");
  if (!dryRun && evidence.liveLease) refuse(evidence, "Main has a live root lease; confirmation cannot override a live owner lease");
}
