import type { FabricParticipantSource } from "../topology/types.js";
import { residentHostId } from "./protocol.js";

/** A fresh directory lookup verifies the publishing identity, root and live host lease. */
export const isOwnResidentActor = (
  participants: Pick<FabricParticipantSource, "get">,
  id: string,
  rootId: string,
): boolean => {
  const actor = participants.get(id, Date.now(), { fresh: true });
  const hostId = residentHostId(rootId);
  return actor !== undefined && !actor.stale && actor.remoteHost === undefined &&
    actor.kind === "actor" && actor.residency === "durable" && actor.rootId === rootId &&
    actor.ownerHostId === hostId && actor.ownerIdentityId === hostId;
};
