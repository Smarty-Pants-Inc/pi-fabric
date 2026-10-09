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
  /** With --main-stopped: what the root participant shows. */
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
  resident?: ResidentLeaseWriter, options: MainStoppedOptions = {}): ResidentOperatorEvidence {
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
  // smarty-dev#7817: no automatic Main-dead proof (that is smarty-dev#7956). A resident-renewed lease
  // is set aside only on the operator's --main-stopped assertion, and the assertion never overrides a
  // live observation: any fresh, reloading or doubtful root participant, or a live owner lease, refuses.
  if (!mainLive && options.mainStopped) {
    const verdict = rootParticipantVerdict(config, mesh, rootLease, selfWindow);
    evidence.mainLiveness = verdict;
    if (!verdict.startsWith("absent:") && !verdict.startsWith("stale:")) mainLive = true;
  }
  if (!options.mainStopped && evidence.residentRenewedLease) mainLive = true;
  evidence.liveLease = mainLive;
  if (mainLive) delete evidence.residentRenewedLease;
  return evidence;
}

/** A live Main republishes its root participant at least every STATE_LEASE_RENEW_MS; a record newer
 * than twice that (never less than the lease window) is fresh, a live observation. */
export const ROOT_PARTICIPANT_FRESH_MS = 2 * STATE_LEASE_RENEW_MS;

export interface MainStoppedOptions {
  /** The operator asserts the root's Main process is gone (--main-stopped). */
  mainStopped?: boolean;
}

/** What the root participant shows; "absent:" and "stale:" are the only answers that do not refuse. */
function rootParticipantVerdict(config: ResidentHostConfig, mesh: Pick<MeshStore, "get">,
  rootLease: FabricHostLease | undefined, window: number): string {
  const now = Date.now();
  const fresh = Math.max(ROOT_PARTICIPANT_FRESH_MS, window);
  const key = "topology/participants/" + createHash("sha256").update(config.rootId).digest("hex");
  let entries: MeshStateEntry[];
  try {
    const file = readParticipantFile(config.meshRoot, key);
    if (!file && participantFilePresent(config.meshRoot, key)) return "unknown: root participant record is unreadable";
    entries = [file, mesh.get(key, { fresh: true })].filter((entry): entry is MeshStateEntry => entry !== undefined);
  } catch (error) { return `unknown: root participant record is unreadable (${error instanceof Error ? error.message : String(error)})`; }
  if (!entries.length) return "absent: no root participant";
  for (const entry of entries) {
    const value = entry.value as { id?: unknown; ownerHostId?: unknown; status?: unknown } | null;
    if (!value || typeof value !== "object" || value.id !== config.rootId || typeof value.ownerHostId !== "string" ||
        !value.ownerHostId || !Number.isFinite(entry.updatedAt)) return "unknown: root participant record is invalid";
    if (value.status === "reloading") return "unknown: root participant is reloading";
    if (now - entry.updatedAt <= fresh) return "live: root participant is fresh";
    // The root lease may be the resident's heartbeat; the Main's own liveness is its session in it.
    const ownerUntil = value.ownerHostId === config.rootId
      ? rootLease?.session?.expiresAt
      : readHostLeaseCurrent(config.meshRoot, value.ownerHostId)?.expiresAt;
    if (value.ownerHostId !== config.rootId && ownerUntil === undefined) return "unknown: participant owner lease is unreadable";
    if (ownerUntil !== undefined && ownerUntil >= now) return "live: participant owner lease is live";
  }
  return "stale: root participant is stale and its owner lease expired";
}

export const MAIN_STOPPED_REQUIRED = "the root's Main may be running; confirm it is stopped and pass --main-stopped (automatic proof: smarty-dev#7956)";

/** `mainStopped` undefined: the action needs no Main-stopped assertion (stop); false/true: it does (remove). */
export function assertResidentOperatorConfirmed(evidence: ResidentOperatorEvidence, confirmation?: string, dryRun = false,
  mainStopped?: boolean): void {
  if (dryRun && confirmation === undefined) return;
  if (confirmation !== evidence.rootId) refuse(evidence, confirmation === undefined
    ? "Missing --confirm-dead-root: operator confirmation is required"
    : "Mismatched --confirm-dead-root: value must equal the selected resident's root id exactly");
  if (!dryRun && mainStopped === false) refuse(evidence, MAIN_STOPPED_REQUIRED);
  if (!dryRun && evidence.liveLease) refuse(evidence, "Main has a live root lease; confirmation cannot override a live owner lease" +
    (evidence.mainLiveness ? ` (${evidence.mainLiveness})` : ""));
}
