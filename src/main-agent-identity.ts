import type { MeshIdentity } from "./mesh/store.js";
import { rootParticipantName } from "./topology/participant-name.js";

// Startup needs only identity. Keep the journal/replay controller behind FabricState's
// first-use runtime import instead of pulling it into a shared eager chunk.
export interface FabricIdentityResolution {
  identity: MeshIdentity;
  mainAgentId: string;
}

export const resolveFabricIdentity = (
  sessionId: string,
  environment: NodeJS.ProcessEnv = process.env,
): FabricIdentityResolution => {
  const actorId = environment.PI_FABRIC_ACTOR_ID?.trim();
  const parentAgentId = environment.PI_FABRIC_PARENT_RUN?.trim();
  const identity: MeshIdentity = actorId
    ? {
        id: actorId,
        name: environment.PI_FABRIC_ACTOR_NAME?.trim() || actorId.slice(0, 8),
        kind: "actor",
        sessionId,
      }
    : parentAgentId
      ? {
          id: parentAgentId,
          name: environment.PI_FABRIC_AGENT_NAME?.trim() || parentAgentId.slice(0, 8),
          kind: "agent",
          sessionId,
        }
      : { id: `session:${sessionId}`, name: rootParticipantName(undefined, environment), kind: "main", sessionId };
  const inheritedMainAgentId = environment.PI_FABRIC_MAIN_AGENT_ID?.trim();
  return {
    identity,
    mainAgentId:
      inheritedMainAgentId || (identity.kind === "main" ? identity.id : `session:${sessionId}`),
  };
};
