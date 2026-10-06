/**
 * Runner adapters: register a worker runner (Fabric spawns its process) or a
 * hosted runner (the adapter owns execution). Off the extension startup graph.
 */
export {
  getAgentRunner,
  listAgentRunners,
  registerAgentRunner,
} from "./agents/runner-registry.js";
export type {
  BuiltInFabricAgentRunner,
  FabricHostedLiveness,
  FabricHostedProgress,
  FabricHostedReporter,
  FabricHostedRunContext,
  FabricHostedRunner,
  FabricRunStopReason,
  FabricRunnerAdapter,
  FabricRunnerAnswer,
  FabricRunnerCapabilities,
  FabricRunnerLaunchContext,
  FabricRunnerModelContext,
  FabricRunnerModelInfo,
  FabricRunnerQuestion,
  FabricTranscriptEvent,
  FabricWorkerLaunch,
  FabricWorkerLaunchContext,
  FabricWorkerRunner,
} from "./agents/runner-registry.js";
export {
  AgentRunRecordSchema,
  FABRIC_WORKER_PROTOCOL_VERSION,
  LifecycleLineSchema,
  SteerCommandSchema,
  TranscriptEventSchema,
  type AgentRunRecordWire,
  type LifecycleLine,
  type SteerCommand,
  type TranscriptEvent,
} from "./agents/runner-protocol.js";
export type { AgentUsage, FabricRunOutcome } from "./agents/types.js";
