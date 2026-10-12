/** Lightweight public guards for the opt-in Fabric assessment projection. */
export {
  FABRIC_ASSESSMENT_TRACE_KIND,
  FABRIC_ASSESSMENT_TRACE_MAX_BYTES,
  FABRIC_ASSESSMENT_TRACE_VERSION,
  isFabricAssessmentTraceV1,
  readFabricAssessmentTraceV1,
  type FabricAssessmentOperationV1,
  type FabricAssessmentSourceV1,
  type FabricAssessmentTraceV1,
  type FabricAssessmentUsageV1,
} from "./audit/assessment.js";
