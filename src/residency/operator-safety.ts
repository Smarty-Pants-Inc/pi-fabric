import { createHash } from "node:crypto";
import fs from "node:fs";
import { ResidentActorAuthorizationError, type ResidentHostConfig } from "./protocol.js";
import type { MeshStore } from "../mesh/store.js";
import { hostEntryLiveness, hostLeasePath, readHostLeaseCurrent } from "../topology/host-leases.js";

export interface ResidentOperatorEvidence {
  rootId: string;
  mainSessionId: string;
  lastLeaseTime: number | null;
  leaseExpiresAt: number | null;
  liveLease: boolean;
  operatorCheck: string;
}

const operatorCheck = "Check the root's Main session and last lease time; run herdr agent list / ps to confirm that no live Main serves this root. A wrong confirmation can interrupt a live Main's actor.";
const refuse = (evidence: ResidentOperatorEvidence, reason: string): never => {
  throw new ResidentActorAuthorizationError(`${reason}; ${JSON.stringify(evidence)}; only after the manual check use --confirm-dead-root ${evidence.rootId}`);
};

/** Report lease facts only. An absent/expired lease never proves that Main is dead. */
export function readResidentOperatorEvidence(config: ResidentHostConfig, mesh: Pick<MeshStore, "get">): ResidentOperatorEvidence {
  const evidence: ResidentOperatorEvidence = { rootId: config.rootId, mainSessionId: config.sessionId,
    lastLeaseTime: null, leaseExpiresAt: null, liveLease: false, operatorCheck };
  const record = (updatedAt: number, expiresAt: number) => {
    evidence.lastLeaseTime = Math.max(evidence.lastLeaseTime ?? updatedAt, updatedAt);
    evidence.leaseExpiresAt = Math.max(evidence.leaseExpiresAt ?? expiresAt, expiresAt);
    if (expiresAt >= Date.now()) evidence.liveLease = true;
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
    record(lease!.updatedAt, lease!.expiresAt);
  }
  const shared = mesh.get("topology/hosts/" + createHash("sha256").update(config.rootId).digest("hex"), { fresh: true });
  if (shared) {
    const value = shared.value as { id?: string; rootId?: string; expiresAt?: number; identity?: { id?: string } };
    if (!value || value.id !== config.rootId || value.rootId !== config.rootId ||
        value.identity?.id !== config.rootId || !Number.isFinite(value.expiresAt)) refuse(evidence, "shared root lease is invalid");
    const lease = hostEntryLiveness(shared, new Map());
    record(lease.updatedAt, lease.expiresAt);
  }
  return evidence;
}

export function assertResidentOperatorConfirmed(evidence: ResidentOperatorEvidence, confirmation?: string, dryRun = false): void {
  if (dryRun && confirmation === undefined) return;
  if (confirmation !== evidence.rootId) refuse(evidence, confirmation === undefined
    ? "Missing --confirm-dead-root: operator confirmation is required"
    : "Mismatched --confirm-dead-root: value must equal the selected resident's root id exactly");
  if (!dryRun && evidence.liveLease) refuse(evidence, "Main has a live root lease; confirmation cannot override a live owner lease");
}
