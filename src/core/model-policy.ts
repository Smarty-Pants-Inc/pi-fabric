// Stable policy facade. Co-located with host compatibility so eager and lazy
// consumers share its existing chunk instead of adding a startup-only split.
export {
  FabricModelDeniedError,
  assertFabricModelAllowed,
  type FabricModelPolicy,
} from "../host-compatibility.js";
