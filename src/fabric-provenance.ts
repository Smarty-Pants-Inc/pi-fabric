// Stable metadata facade. Co-located with host compatibility so eager and lazy consumers
// share one existing bundle chunk, with no Main journal engine or optional runtime import.
export {
  fabricHostIdentity,
  fabricProvenanceOptions,
  fabricProvenanceSupported,
  fabricTurnProvenance,
  resolveFabricIdentity,
  sendFabricMessage,
  sendFabricUserMessage,
  type FabricIdentityResolution,
  type FabricTurnProvenance,
} from "./host-compatibility.js";
