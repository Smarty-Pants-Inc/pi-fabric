import type { FabricParticipantInfo, FabricParticipantRecord } from "./types.js";

/**
 * smarty-dev#16357 follow-up: how long after its owner host lease lapsed the bound root's own
 * last-known record still names its lane (four 15 s lease TTLs). A busy Main renews late, so its
 * lease lapses for seconds at a time; the lane is only the root's cwd, which a late renewal does
 * not change. A longer lapse, or an ended root, stays unresolved.
 */
export const ROOT_LANE_LEASE_LAPSE_GRACE_MS = 60_000;

/** The directory reads the bound-root cwd lookup needs (ParticipantDirectory implements them). */
export interface RootSessionCwdSource {
  get(id: string, now?: number, options?: { fresh?: boolean }): FabricParticipantInfo | undefined;
  lastKnown?(id: string, now?: number): { participant: FabricParticipantInfo; lapsedMs: number } | undefined;
  routeEnded?(participant: FabricParticipantRecord): boolean;
}

const ownRootCwd = (record: FabricParticipantInfo | undefined, rootId: string): string | undefined =>
  record?.kind === "root" && record.id === rootId && typeof record.cwd === "string" && record.cwd.trim().length > 0
    ? record.cwd
    : undefined;

/**
 * The cwd of the bound root `rootId`, the source of an actor's exception lane. A lease-fresh record
 * answers first, unchanged. Only when the fresh lookup finds nothing, the SAME root's last-known
 * record answers if its owner lease lapsed at most ROOT_LANE_LEASE_LAPSE_GRACE_MS ago and its
 * route is not ended (the directory's own closure check, as get() applies). No other
 * participant's record is ever read; a failed fallback read is undefined, as without it.
 */
export function boundRootSessionCwd(participants: RootSessionCwdSource | undefined, rootId: string): string | undefined {
  if (!participants) return undefined;
  const now = Date.now();
  const live = participants.get(rootId, now, { fresh: true });
  if (live !== undefined) return ownRootCwd(live, rootId);
  if (!participants.lastKnown || !participants.routeEnded) return undefined;
  try {
    const known = participants.lastKnown(rootId, now);
    if (!known || !(known.lapsedMs >= 0 && known.lapsedMs <= ROOT_LANE_LEASE_LAPSE_GRACE_MS)) return undefined;
    const { participant } = known;
    if (participant.kind !== "root" || participant.id !== rootId || participants.routeEnded(participant)) return undefined;
    return ownRootCwd(participant, rootId);
  } catch {
    return undefined;
  }
}
