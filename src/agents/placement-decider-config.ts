// Stable lazy-adapter facade; the shared synchronous host validator lives in the
// already-eager placement-config module to keep Pi's startup file graph bounded.
export { DEFAULT_PLACEMENT_DECIDER_MODE, normalizePlacementDeciderConfig } from "./placement-config.js";
export type { FabricPlacementDeciderConfig, PlacementDeciderConfig, PlacementDeciderMode } from "./placement-config.js";
