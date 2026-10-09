import { createHash } from "node:crypto";
import fs from "node:fs";
import { ResidentActorAuthorizationError, type ResidentHostConfig } from "./protocol.js";
import type { MeshStateEntry, MeshStore } from "../mesh/store.js";
import { participantFilePresent, readParticipantFile } from "../topology/participant-files.js";
import { hostEntryLiveness, hostLeasePath, readHostLeaseCurrent, STATE_LEASE_RENEW_MS, type FabricHostLease } from "../topology/host-leases.js";

export interface ResidentOperatorEvidence {
  rootId: string;
  mainSessionId: string;
  lastLeaseTime: number | null;
  leaseExpiresAt: number | null;
  liveLease: boolean;
  /** A live root lease that this root's own resident host writes, with no live Main session in it (smarty-dev#7817). */
  residentRenewedLease?: true;
  /** Why a resident-renewed lease was or was not set aside. */
  mainLiveness?: string;
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
  let rootLease: FabricHostLease | undefined;
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
    rootLease = lease!;
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
  // A resident-renewed lease is set aside only on a positive proof that no Main serves the root
  // (smarty-dev#7817). Absent within the grace, unreadable, invalid or any doubt is unknown: refuse.
  if (!mainLive && evidence.residentRenewedLease) {
    const verdict = mainDeadProof(config, mesh, rootLease!, selfWindow);
    evidence.mainLiveness = verdict;
    if (verdict !== "dead: stale root participant, owner lease expired" && verdict !== "dead: no root participant after the grace") mainLive = true;
    if (mainLive) evidence.liveLease = true;
  }
  if (!mainLive && evidence.liveLease) evidence.liveLease = false;
  if (mainLive) delete evidence.residentRenewedLease;
  return evidence;
}

/** A root participant record is republished at least every STATE_LEASE_RENEW_MS by a live Main; a
 * restarted Main publishes within it. Twice that, and never less than the lease window, is the grace. */
export const MAIN_PUBLISH_GRACE_MS = 2 * STATE_LEASE_RENEW_MS;

/** Positive dead proof of the root's Main for a resident-renewed lease; any other answer is unknown. */
function mainDeadProof(config: ResidentHostConfig, mesh: Pick<MeshStore, "get">, rootLease: FabricHostLease, window: number): string {
  const now = Date.now();
  const grace = Math.max(MAIN_PUBLISH_GRACE_MS, window);
  const key = "topology/participants/" + createHash("sha256").update(config.rootId).digest("hex");
  let entries: MeshStateEntry[];
  try {
    const file = readParticipantFile(config.meshRoot, key);
    if (!file && participantFilePresent(config.meshRoot, key)) return "unknown: root participant record is unreadable";
    entries = [file, mesh.get(key, { fresh: true })].filter((entry): entry is MeshStateEntry => entry !== undefined);
  } catch (error) { return `unknown: root participant record is unreadable (${error instanceof Error ? error.message : String(error)})`; }
  if (!entries.length) {
    // (b) A restarted Main publishes its record within the grace; this incarnation has run longer.
    // The live executor re-reads this before every mutation, after its registry and fence checks.
    if (rootLease.startedAt === undefined || !Number.isFinite(rootLease.startedAt)) return "unknown: resident incarnation start is unrecorded";
    if (now - rootLease.startedAt <= grace) return `unknown: no root participant yet, resident started ${Math.round((now - rootLease.startedAt) / 1000)} s ago (grace ${grace / 1000} s)`;
    return "dead: no root participant after the grace";
  }
  for (const entry of entries) {
    // (a) Readable, valid, not reloading, stale, and its owner's lease expired.
    const value = entry.value as { id?: unknown; rootId?: unknown; ownerHostId?: unknown; status?: unknown } | null;
    if (!value || typeof value !== "object" || value.id !== config.rootId || typeof value.ownerHostId !== "string" ||
        !value.ownerHostId || !Number.isFinite(entry.updatedAt)) return "unknown: root participant record is invalid";
    if (value.status === "reloading") return "unknown: root participant is reloading";
    if (now - entry.updatedAt <= grace) return "live: root participant published within the grace";
    // The root lease itself is the resident's heartbeat; the Main's own liveness is its session in it.
    const ownerUntil = value.ownerHostId === config.rootId
      ? rootLease.session?.expiresAt
      : readHostLeaseCurrent(config.meshRoot, value.ownerHostId)?.expiresAt;
    if (value.ownerHostId !== config.rootId && ownerUntil === undefined) return "unknown: participant owner lease is unreadable";
    if (ownerUntil !== undefined && ownerUntil >= now) return "live: participant owner lease is live";
  }
  return "dead: stale root participant, owner lease expired";
}

export function assertResidentOperatorConfirmed(evidence: ResidentOperatorEvidence, confirmation?: string, dryRun = false): void {
  if (dryRun && confirmation === undefined) return;
  if (confirmation !== evidence.rootId) refuse(evidence, confirmation === undefined
    ? "Missing --confirm-dead-root: operator confirmation is required"
    : "Mismatched --confirm-dead-root: value must equal the selected resident's root id exactly");
  if (!dryRun && evidence.liveLease) refuse(evidence, "Main has a live root lease; confirmation cannot override a live owner lease" +
    (evidence.mainLiveness ? ` (${evidence.mainLiveness})` : ""));
}
