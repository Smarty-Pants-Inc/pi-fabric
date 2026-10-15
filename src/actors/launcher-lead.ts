import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { MeshIdentity } from "../mesh/store.js";
import { processStartTime, residentProcessAlive } from "../residency/process-identity.js";
import { readHostLeaseCurrent } from "../topology/host-leases.js";
import { registeredMainActivation, registeredMainLauncherIdentity, registeredMainPredecessors,
  type NativeLauncherIdentity } from "../topology/stall-alarms.js";

const object = (value: unknown): Record<string, unknown> | undefined =>
  value !== null && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : undefined;
const launcherIdentity = (value: unknown): NativeLauncherIdentity | undefined => {
  const identity = object(value);
  if (!identity || typeof identity.rootId !== "string" || !identity.rootId.startsWith("session:") ||
      typeof identity.activationId !== "string" || !identity.activationId || !Number.isSafeInteger(identity.pid) ||
      (identity.pid as number) <= 0 || identity.host !== os.hostname() ||
      (identity.processStartedAt !== undefined && (typeof identity.processStartedAt !== "string" || !/^\d+$/.test(identity.processStartedAt))) ||
      (process.platform === "linux" && identity.processStartedAt === undefined)) return undefined;
  return { rootId: identity.rootId, activationId: identity.activationId, pid: identity.pid as number,
    host: identity.host, ...(identity.processStartedAt === undefined ? {} : { processStartedAt: identity.processStartedAt as string }) };
};

/** Receiver-owned native inbox state; public mesh and mutable display names never supply a grant. */
export function nativeRootGrant(meshRoot: string, rootId: string): Record<string, unknown> | undefined {
  if (!rootId.startsWith("session:")) return undefined;
  try {
    const owner = object(JSON.parse(fs.readFileSync(path.join(meshRoot, "main-followups",
      `${encodeURIComponent(rootId.slice(8))}.owner.json`), "utf8")));
    return owner?.rootId === rootId && owner.sessionId === rootId.slice(8) && owner.ownerIdentityId === rootId &&
      owner.host === os.hostname() &&
      (owner.nativeProject === undefined || typeof owner.nativeProject === "string" && owner.nativeProject.length > 0) ? owner : undefined;
  } catch { return undefined; }
}

/** Checked again under commit custody. Project bindings come from private actor/native state.
 * Retirement additionally requires the original receiver's exact native launcher identity.
 * Historical missing grants are not invented from a project, role or inherited mainAgentId. */
export function assertNativeObserver(meshRoot: string, identity: MeshIdentity, ownSessionId: string | undefined,
  row: Record<string, unknown>, retire: boolean): string {
  const current = nativeRootGrant(meshRoot, identity.id);
  const lease = readHostLeaseCurrent(meshRoot, identity.id);
  const old = typeof row.rootId === "string" ? nativeRootGrant(meshRoot, row.rootId) : undefined;
  const sameProject = typeof row.project === "string" && row.project.length > 0 && current?.nativeProject === row.project;
  const originalLauncher = launcherIdentity(old?.nativeLauncherIdentity);
  const currentLauncher = launcherIdentity(current?.nativeLauncherIdentity);
  const registeredLauncher = launcherIdentity(registeredMainLauncherIdentity(meshRoot, identity.id));
  const predecessors = registeredMainPredecessors(meshRoot, identity.id).map(launcherIdentity).filter(value => value !== undefined);
  const sameLauncher = !!originalLauncher && originalLauncher.rootId === row.rootId && originalLauncher.activationId === old?.activationId &&
    !!currentLauncher && !!registeredLauncher && JSON.stringify(currentLauncher) === JSON.stringify(registeredLauncher) &&
    predecessors.some(predecessor => JSON.stringify(predecessor) === JSON.stringify(originalLauncher));
  const registrationToken = registeredMainActivation(meshRoot, identity.id);
  const processIdentityValid = current !== undefined && (current.processStartedAt === undefined && process.platform !== "linux"
    ? typeof registrationToken === "string" && current.activationId === registrationToken && residentProcessAlive(process.pid)
    : typeof current.processStartedAt === "string" && /^\d+$/.test(current.processStartedAt) &&
      (process.platform === "linux" ? processStartTime(process.pid) === current.processStartedAt
        : residentProcessAlive(process.pid, current.processStartedAt)));
  if (identity.kind !== "main" || !ownSessionId || identity.id !== `session:${ownSessionId}` || identity.sessionId !== ownSessionId ||
      identity.id === row.rootId || !current || current.retired || current.pid !== process.pid ||
      typeof current.activationId !== "string" || current.activationId !== registrationToken || !processIdentityValid ||
      typeof current.nativeProject !== "string" || !current.nativeProject ||
      !lease || lease.rootId !== identity.id || lease.identityId !== identity.id ||
      lease.expiresAt < Date.now() || lease.writer?.pid !== process.pid || lease.writer.host !== os.hostname() ||
      (retire ? !sameLauncher : !sameProject)) {
    throw new Error("Orphan settlement requires a live native project root; retirement requires the same trusted native launcher (not a display name)");
  }
  return JSON.stringify([current.activationId, current.nativeProject, current.pid, current.processStartedAt,
    lease.incarnationToken, originalLauncher, currentLauncher, registeredLauncher, predecessors]);
}
