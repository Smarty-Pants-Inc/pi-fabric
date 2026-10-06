import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { mainGenerationPath, readHandoverJson, type ResidentMainGeneration } from "./handover.js";
import { residentProcessAlive } from "./process-identity.js";
import { ResidentActorAuthorizationError, type ResidentHostConfig } from "./protocol.js";
import type { FabricParticipantSource } from "../topology/types.js";
import type { MeshStore } from "../mesh/store.js";
import { hostEntryLiveness, hostLeasePath, readHostLease } from "../topology/host-leases.js";

/** A lapsed lease alone is NOT a dead Main. Missing/invalid process evidence fails closed. */
export function assertDeadResidentMain(config: ResidentHostConfig, participants: Pick<FabricParticipantSource, "get">, mesh: Pick<MeshStore, "get">): void {
  const refuse = (reason: string): never => {
    throw new ResidentActorAuthorizationError(`Resident root ${config.rootId}: ${reason}; use --force-live only for an intentional override`);
  };
  const root = participants.get(config.rootId, Date.now(), { fresh: true });
  if (root && !root.stale) refuse("Main has a live root lease");
  const key = "topology/hosts/" + createHash("sha256").update(config.rootId).digest("hex");
  const shared = mesh.get(key, { fresh: true });
  if (shared) {
    const value = shared.value as { id?: string; rootId?: string; expiresAt?: number; identity?: { id?: string } };
    if (!value || value.id !== config.rootId || value.rootId !== config.rootId ||
        value.identity?.id !== config.rootId || !Number.isFinite(value.expiresAt)) refuse("shared root lease is invalid");
    if (hostEntryLiveness(shared, new Map()).expiresAt >= Date.now()) refuse("Main has a live root lease");
  }
  const leaseFile = hostLeasePath(config.meshRoot, config.rootId);
  let leasePresent = false;
  try { fs.lstatSync(leaseFile); leasePresent = true; }
  catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
  if (leasePresent) {
    const lease = readHostLease(config.meshRoot, config.rootId);
    if (!lease || lease.rootId !== config.rootId || lease.identityId !== config.rootId) refuse("root lease is unreadable or invalid");
    if (lease!.expiresAt >= Date.now()) refuse("Main has a live root lease");
  }
  const generation = readHandoverJson<ResidentMainGeneration>(mainGenerationPath(config.residencyRoot));
  const inbox = readHandoverJson<{ rootId: string; sessionId: string; pid: number; processStartedAt?: string }>(
    path.join(config.meshRoot, "main-followups", `${encodeURIComponent(config.sessionId)}.owner.json`));
  let known = false;
  for (const owner of [generation && { ...generation, started: generation.processStartTime },
    inbox && { ...inbox, started: inbox.processStartedAt }]) {
    if (!owner) continue;
    if (owner.rootId !== config.rootId || owner.sessionId !== config.sessionId ||
        !Number.isSafeInteger(owner.pid) || owner.pid <= 0 ||
        (owner.started !== undefined && typeof owner.started !== "string")) refuse("Main process identity is invalid");
    known = true;
    if (residentProcessAlive(owner.pid, owner.started || undefined)) refuse("Main process is still alive");
  }
  if (!known) refuse("Main process death cannot be established (no recorded identity)");
}
