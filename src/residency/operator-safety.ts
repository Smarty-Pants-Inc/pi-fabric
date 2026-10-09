import { createHash } from "node:crypto";
import fs from "node:fs";
import { writeJsonAtomic } from "../core/atomic-write.js";
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
  resident?: ResidentLeaseWriter, options: MainAbsenceOptions = {}): ResidentOperatorEvidence {
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
    const verdict = mainDeadProof(config, mesh, rootLease, selfWindow, options);
    evidence.mainLiveness = verdict;
    if (!verdict.startsWith("dead:")) mainLive = true;
    if (mainLive) evidence.liveLease = true;
  } else if (!mainLive && options.offline) {
    // Offline (dead resident): no live lease. A Main that is starting may publish its participant
    // first; any fresh, reloading or doubtful root participant is a live Main (smarty-dev#7817).
    const verdict = mainDeadProof(config, mesh, rootLease, 0, options);
    evidence.mainLiveness = verdict;
    if (!verdict.startsWith("dead:")) { mainLive = true; evidence.liveLease = true; }
  }
  if (!mainLive && evidence.liveLease) evidence.liveLease = false;
  if (mainLive) delete evidence.residentRenewedLease;
  return evidence;
}

/** A root participant record is republished at least every STATE_LEASE_RENEW_MS by a live Main; a
 * restarted Main publishes within it. Twice that, and never less than the lease window, is the grace. */
export const MAIN_PUBLISH_GRACE_MS = 2 * STATE_LEASE_RENEW_MS;

export interface MainAbsenceOptions {
  /** Durable record of when the root participant was first seen absent (live executor only). */
  absenceFile?: string;
  /** Record a first absence (a dry run only reads). */
  recordAbsence?: boolean;
  /** The resident is dead: an absent participant with no live lease is no Main. */
  offline?: boolean;
}

interface MainAbsence { format: 1; rootId: string; absentSince: number; lease: string }

/** Positive dead proof of the root's Main; any other answer is unknown or live. */
function mainDeadProof(config: ResidentHostConfig, mesh: Pick<MeshStore, "get">, rootLease: FabricHostLease | undefined,
  window: number, options: MainAbsenceOptions): string {
  const now = Date.now();
  const grace = Math.max(MAIN_PUBLISH_GRACE_MS, window);
  const key = "topology/participants/" + createHash("sha256").update(config.rootId).digest("hex");
  const resetAbsence = (): void => {
    if (options.absenceFile && options.recordAbsence) fs.rmSync(options.absenceFile, { force: true });
  };
  let entries: MeshStateEntry[];
  try {
    const file = readParticipantFile(config.meshRoot, key);
    if (!file && participantFilePresent(config.meshRoot, key)) { resetAbsence(); return "unknown: root participant record is unreadable"; }
    entries = [file, mesh.get(key, { fresh: true })].filter((entry): entry is MeshStateEntry => entry !== undefined);
  } catch (error) { resetAbsence(); return `unknown: root participant record is unreadable (${error instanceof Error ? error.message : String(error)})`; }
  if (!entries.length) {
    if (options.offline) return "dead: no root participant and no live lease";
    // (b) Absence itself must hold for the whole grace, observed durably: a restarting Main has no
    // record for a moment, whatever the lease age. A lease incarnation change restarts the window.
    if (!options.absenceFile) return "unknown: root participant absence is not established";
    const lease = JSON.stringify([rootLease?.startedAt ?? null, rootLease?.writer?.pid ?? null, rootLease?.writer?.host ?? null, rootLease?.writer?.startedAt ?? null]);
    let seen: MainAbsence | undefined;
    try { seen = JSON.parse(fs.readFileSync(options.absenceFile, "utf8")) as MainAbsence; }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") return "unknown: root participant absence record is unreadable"; }
    if (!seen || seen.format !== 1 || seen.rootId !== config.rootId || seen.lease !== lease ||
        !Number.isFinite(seen.absentSince) || seen.absentSince > now) {
      seen = { format: 1, rootId: config.rootId, absentSince: now, lease };
      if (options.recordAbsence) writeJsonAtomic(options.absenceFile, seen, { durable: true });
    }
    if (now - seen.absentSince < grace) {
      return `unknown: root participant absence not yet established (absent since ${new Date(seen.absentSince).toISOString()}); retry after ${new Date(seen.absentSince + grace).toISOString()}`;
    }
    return "dead: no root participant for the whole grace";
  }
  // Any participant observation restarts the absence window.
  resetAbsence();
  for (const entry of entries) {
    // (a) Readable, valid, not reloading, stale, and its owner's lease expired.
    const value = entry.value as { id?: unknown; rootId?: unknown; ownerHostId?: unknown; status?: unknown } | null;
    if (!value || typeof value !== "object" || value.id !== config.rootId || typeof value.ownerHostId !== "string" ||
        !value.ownerHostId || !Number.isFinite(entry.updatedAt)) return "unknown: root participant record is invalid";
    if (value.status === "reloading") return "unknown: root participant is reloading";
    if (now - entry.updatedAt <= grace) return "live: root participant published within the grace";
    // The root lease itself is the resident's heartbeat; the Main's own liveness is its session in it.
    const ownerUntil = value.ownerHostId === config.rootId
      ? rootLease?.session?.expiresAt
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
