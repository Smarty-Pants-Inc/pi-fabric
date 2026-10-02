import type { MeshStore } from "../mesh/store.js";
import type { FabricParticipantSource } from "./types.js";
import { hostLeaseExpiry, readHostLeases } from "./host-leases.js";

/** The same fresh directory/owner-lease evidence gates adoption and explicit lineage pruning. */
export const lineageAlive = (mesh: MeshStore, participants: FabricParticipantSource, root: string): boolean => {
  const stalled = participants.writeStalled?.();
  if (stalled) throw stalled; // Unknown visibility is not evidence of death.
  const now = Date.now();
  if (participants.get(root, now, { fresh: true }) ||
      participants.list({ scope: "project", fresh: true }, now).some(p => p.rootId === root && !p.stale)) return true;
  const leases = readHostLeases(mesh.root);
  if ([...leases.values()].some(lease => lease.rootId === root && lease.expiresAt >= now)) return true;
  return mesh.listAll("topology/hosts/", { fresh: true }).some(entry => {
    const host = entry.value as { id?: string; rootId?: string; identity?: { id: string }; expiresAt?: number } | null;
    return host?.rootId === root && typeof host.id === "string" && typeof host.identity?.id === "string" &&
      typeof host.expiresAt === "number" && hostLeaseExpiry(leases, {
        id: host.id, rootId: root, identity: host.identity, expiresAt: host.expiresAt,
      }) >= now;
  });
};
