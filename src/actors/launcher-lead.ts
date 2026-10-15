import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { MeshIdentity } from "../mesh/store.js";
import { residentProcessAlive } from "../residency/process-identity.js";
import { readHostLeaseCurrent } from "../topology/host-leases.js";

const object = (value: unknown): Record<string, unknown> | undefined =>
  value !== null && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : undefined;
const launchName = (value: unknown): value is string => typeof value === "string" && value.length > 0 && value.length <= 200 &&
  value === value.trim() && !/[\u0000-\u001f\u007f-\u009f]/.test(value);

/** Receiver-owned native inbox state; public mesh and mutable display names never supply a grant. */
export function nativeRootGrant(meshRoot: string, rootId: string): Record<string, unknown> | undefined {
  if (!rootId.startsWith("session:")) return undefined;
  try {
    const owner = object(JSON.parse(fs.readFileSync(path.join(meshRoot, "main-followups",
      `${encodeURIComponent(rootId.slice(8))}.owner.json`), "utf8")));
    return owner?.rootId === rootId && owner.sessionId === rootId.slice(8) && owner.ownerIdentityId === rootId &&
      owner.host === os.hostname() && (owner.nativeLauncher === undefined || launchName(owner.nativeLauncher)) &&
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
  const sameLauncher = !!old && launchName(old.nativeLauncher) && old.nativeLauncher === current?.nativeLauncher;
  if (identity.kind !== "main" || !ownSessionId || identity.id !== `session:${ownSessionId}` || identity.sessionId !== ownSessionId ||
      identity.id === row.rootId || !current || current.retired || current.pid !== process.pid ||
      typeof current.activationId !== "string" || typeof current.processStartedAt !== "string" || !/^\d+$/.test(current.processStartedAt) ||
      !residentProcessAlive(process.pid, current.processStartedAt) || !lease || lease.rootId !== identity.id || lease.identityId !== identity.id ||
      lease.expiresAt < Date.now() || lease.writer?.pid !== process.pid || lease.writer.host !== os.hostname() ||
      (retire ? !sameLauncher : !sameProject && !sameLauncher)) {
    throw new Error("Orphan settlement requires a live native project root; retirement requires the same trusted native launcher (not a display name)");
  }
  return JSON.stringify([current.activationId, current.nativeLauncher, current.nativeProject, current.processStartedAt,
    lease.incarnationToken, old?.nativeLauncher]);
}
