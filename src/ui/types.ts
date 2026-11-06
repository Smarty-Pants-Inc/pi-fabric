import type { FabricActorInfo, FabricActorMessage, GlobalActorDefinition } from "../actors/types.js";
import type { FabricActivityRun } from "../activity/types.js";
import type { FabricComponentGraph } from "../components/types.js";
import type { MeshEvent } from "../mesh/store.js";
import type { FabricMainAgentInfo } from "../main-agent.js";
import type {
  FabricParticipantCapability,
  FabricParticipantInfo,
  FabricParticipantKind,
  FabricParticipantResidency,
  FabricPeerInfo,
} from "../topology/types.js";
import type { AgentUsage } from "../agents/types.js";
import type { FabricAgentRunner } from "../config.js";

export type FabricUiMain = FabricMainAgentInfo;
export type FabricUiPeer = FabricPeerInfo;

export interface FabricUiAgent {
  id: string;
  name: string;
  status: string;
  runner?: FabricAgentRunner;
  residency?: FabricParticipantResidency;
  transport: string;
  cwd: string;
  task?: string;
  model?: string;
  thinking?: string;
  currentTool?: string;
  startedAt?: number;
  updatedAt?: number;
  finishedAt?: number;
  turns?: number;
  toolCalls?: number;
  usage?: AgentUsage;
  text?: string;
  value?: unknown;
  error?: string;
  logFile?: string;
  attachCommand?: string;
  branch?: string;
  worktree?: string;
  actorId?: string;
  actorName?: string;
  runId?: string;
  phaseId?: string;
  parentId?: string;
  nestingDepth?: number;
  rootId?: string;
  ownerHostId?: string;
  local?: boolean;
  stale?: boolean;
  participantKind?: FabricParticipantKind;
  capabilities?: FabricParticipantCapability[];
}

export interface FabricUiActor extends FabricActorInfo {
  /** The actor's default instruction (persona text); shown and edited in the dashboard. */
  instructions: string;
  recentMessages: FabricActorMessage[];
  ownerHostId?: string;
  local?: boolean;
  worker?: FabricUiAgent;
}

export interface FabricUiStateEntry {
  key: string;
  label: string;
  status: string;
  owner?: string;
  detail?: string;
  value: unknown;
  version: number;
  updatedAt: number;
}

export interface FabricDashboardSnapshot {
  shells?: import("../core/shell-jobs.js").FabricShellJobInfo[];
  now: number;
  widgetDismissedAt?: number;
  runs: FabricActivityRun[];
  main: FabricUiMain;
  peers: FabricUiPeer[];
  participants?: FabricParticipantInfo[];
  agents: FabricUiAgent[];
  actors: FabricUiActor[];
  componentGraph: FabricComponentGraph;
  /** Project-independent actor templates from the global registry. */
  globalActors: GlobalActorDefinition[];
  state: FabricUiStateEntry[];
  events: MeshEvent[];
}

export { activeStatuses, isActiveStatus, orderAgentsByCreation } from "./format.js";
