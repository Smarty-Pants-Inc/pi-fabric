import type { ImageContent } from "@earendil-works/pi-ai";
import type {
  SessionEntry,
  SessionMessageEntry,
} from "@earendil-works/pi-coding-agent";
import type { FabricAgentRunner, FabricAgentTransport, FabricPythonRuntime } from "../config.js";
import type { FabricKernel } from "../runtime/kernel.js";
import type { ThinkingTransferInput } from "./thinking-transfer.js";
import type { FabricThinking } from "../thinking.js";
import type { FabricParticipantResidency } from "../topology/types.js";
import type { InheritedSessionPin } from "./session-pins.js";

export type AgentRunStatus =
  | "queued"
  | "running"
  | "completed"
  | "failed"
  | "stopped"
  | "timed_out";

export type AgentToolResultMessage = Extract<
  SessionMessageEntry["message"],
  { role: "toolResult" }
>;

/** Deterministic Fabric compaction applied to the inherited handoff trajectory. */
export interface HandoffCompactionRequest {
  instructions?: string;
  preserve?: string[];
}

export interface AgentSessionSeed {
  sourceSessionId: string;
  sourceSessionFile?: string;
  sourceBranchLeafId: string;
  /** Present only when the source session is in memory and must be materialized. */
  sourceBranch?: SessionEntry[];
  sourceModel?: { provider: string; modelId: string };
  sourceThinkingLevel?: string;
  outerToolResult: AgentToolResultMessage;
}

export interface AgentSpawner {
  id: string;
  kind: "main" | "agent" | "actor";
  /** The activation that spawned the child; actor identity survives that run ending. */
  runId?: string;
}

export interface AgentRunRequest {
  task: string;
  images?: ImageContent[];
  name?: string;
  runner?: FabricAgentRunner;
  /** Omitted/inherit uses the caller kernel; concrete kernels require Pi with Fabric extensions. */
  kernel?: FabricKernel | "inherit";
  /** Host-only snapshot for resident/trajectory forwarding; not a provider argument. */
  pythonRuntime?: FabricPythonRuntime;
  transport?: FabricAgentTransport;
  model?: string;
  /** Veda persona name; only used when runner is "veda". */
  persona?: string;
  thinking?: FabricThinking;
  tools?: string[];
  timeoutMs?: number;
  extensions?: boolean;
  recursive?: boolean;
  /** Leaf or recursive execution cwd; relative to the immediate caller, independent of project/mesh lineage. */
  cwd?: string;
  worktree?: boolean;
  residency?: FabricParticipantResidency;
  schema?: Record<string, unknown>;
  /** With a schema on the Pi runner: the result is one fabric_reply tool call (smarty-dev#967). */
  replyTool?: boolean;
  systemPrompt?: string;
  sessionFile?: string;
  /** Host-owned actor activation policy, not a one-shot provider argument. */
  inferenceContext?: "full-history" | "activation";
  actorId?: string;
  actorName?: string;
  capabilityRequirements?: string[];
  capabilityDigest?: string;
  meshRoot?: string;
  runnerSessionId?: string;
  /** Host-created Pi branch seed ending with the native outer fabric_exec result. */
  sessionSeed?: AgentSessionSeed;
  /** Source/executor reasoning channels for trajectory thinking transfer. */
  thinkingTransfer?: ThinkingTransferInput | undefined;
  /** Compact the inherited trajectory with Fabric's deterministic compactor before the executor resumes. */
  handoffCompact?: HandoffCompactionRequest;
  /** Host-only parent /switch-account pins; not a model argument. */
  inheritedSessionPins?: InheritedSessionPin[];
  /** Unix niceness 0-19; only raises agents.nice, never lowers it. */
  nice?: number;
  /** Actor runs: default bash timeout (s), exported as PI_FABRIC_ACTOR_BASH_TIMEOUT_S; 0 = none. */
  bashTimeoutSeconds?: number;
}

export interface AgentUsage {
  input: number;
  output: number;
  cacheRead: number;
  cacheWrite: number;
  cost: number;
}

export interface FabricBudgetSummary {
  limit: number;
  spent: number;
  remaining: number;
  tokens: number;
}

export interface AgentCompactionStatus {
  status: "queued" | "in_flight" | "completed" | "failed";
  requestedAt: number;
  updatedAt: number;
  startedAt?: number;
  finishedAt?: number;
  attempts: number;
  coalescedRequests: number;
  queued?: boolean;
  error?: string;
}

export interface AgentRunRecord {
  /** Immediate caller, distinct from the lineage Main. */
  spawner?: AgentSpawner;
  /** Requested launch model; model below follows verified state/assistant attribution. */
  requestedModel?: string;
  id: string;
  name: string;
  task: string;
  status: AgentRunStatus;
  runner: FabricAgentRunner;
  /** Resolved Fabric kernel; absent for runners without Fabric. */
  kernel?: FabricKernel;
  transport: FabricAgentTransport;
  cwd: string;
  model?: string;
  thinking?: FabricThinking;
  actorId?: string;
  actorName?: string;
  capabilityRequirements?: string[];
  capabilityDigest?: string;
  recursive?: boolean;
  residency?: FabricParticipantResidency;
  startedAt: number;
  updatedAt: number;
  finishedAt?: number;
  currentTool?: string;
  turns: number;
  /** Actual model output/tool execution, not worker startup or an error-only turn. */
  inferenceStarted?: boolean;
  toolCalls: number;
  text: string;
  /** How a structured reply arrived: its fabric_reply tool call (smarty-dev#967). */
  replyVia?: "tool";
  value?: unknown;
  error?: string;
  /** Non-fatal run problems, e.g. a dropped oversized child event (smarty-dev#1907). */
  warnings?: string[];
  stderr?: string;
  exitCode?: number | null;
  usage: AgentUsage;
  budget?: FabricBudgetSummary;
  sessionId?: string;
  runnerSessionId?: string;
  attachCommand?: string;
  branch?: string;
  worktree?: string;
  logFile?: string;
  nestedAgents?: AgentRunRecord[];
  pendingMessages?: { steering: string[]; followUp: string[] };
  compaction?: AgentCompactionStatus;
}

export interface AgentRunResult extends AgentRunRecord {
  status: "completed" | "failed" | "stopped" | "timed_out";
}

export interface AgentHandleInfo {
  spawner?: AgentSpawner;
  id: string;
  name: string;
  status: AgentRunStatus;
  runner: FabricAgentRunner;
  /** Resolved Fabric kernel; absent for runners without Fabric. */
  kernel?: FabricKernel;
  transport: FabricAgentTransport;
  cwd: string;
  model?: string;
  thinking?: FabricThinking;
  actorId?: string;
  actorName?: string;
  capabilityRequirements?: string[];
  capabilityDigest?: string;
  recursive?: boolean;
  residency?: FabricParticipantResidency;
  sessionId?: string;
  runnerSessionId?: string;
  attachCommand?: string;
  branch?: string;
  worktree?: string;
}

export interface AgentWorkerOptions {
  id: string;
  runner: FabricAgentRunner;
  kernel?: FabricKernel;
  pythonRuntime?: FabricPythonRuntime;
  name: string;
  taskFile: string;
  imagesFile?: string;
  statusFile: string;
  lifecycleFile: string;
  logFile: string;
  schemaFile?: string;
  /** Take the structured result from one fabric_reply tool call, not the final text (smarty-dev#967). */
  replyTool?: boolean;
  cwd: string;
  piBinary: string;
  claudeBinary: string;
  vedaBinary: string;
  vedaBackend: string;
  vedaPersona: string;
  timeoutMs: number;
  depth: number;
  fullCodeMode: boolean;
  mainAgentId?: string;
  spawner?: AgentSpawner;
  fabricSessionId?: string;
  extensions: boolean;
  tools: string[];
  grantedRisks: string[];
  maxTokens?: number;
  /** Niceness applied to the spawned child (and IO priority on Linux). */
  nice?: number;
  bashTimeoutSeconds?: number;
  fabricExtensionPath?: string;
  model?: string;
  thinking?: string;
  systemPrompt?: string;
  sessionFile?: string;
  inferenceContext?: "full-history" | "activation";
  sessionExportFile?: string;
  actorId?: string;
  actorName?: string;
  capabilityRequirements?: string[];
  capabilityDigest?: string;
  meshRoot?: string;
  projectRoot?: string;
  ownerHostId?: string;
  ownerIdentityId?: string;
  runnerSessionId?: string;
  runRoot?: string;
  steerFile?: string;
  transport: FabricAgentTransport;
  sessionId?: string;
  attachCommand?: string;
  branch?: string;
  worktree?: string;
  inheritedSessionPins?: InheritedSessionPin[];
  carryOver?: AgentRunCarryOver;
}

/**
 * Cumulative totals a relaunched worker seeds its fresh run record with. The
 * worker owns the run record, so passing the prefix here (instead of adding it
 * host-side) keeps every reader — status file, live UI rows, settled result,
 * and the budget ledger's settle residual — on one cumulative number.
 */
export interface AgentRunCarryOver {
  turns: number;
  toolCalls: number;
  usage: AgentUsage;
}

export interface AgentTransportLaunch {
  id: string;
  name: string;
  cwd: string;
  workerPath: string;
  workerArguments: string[];
  /** Aborted when the agent manager closes; a transport may stop waiting to launch. */
  signal?: AbortSignal;
}

export interface AgentTransportHandle {
  kind: FabricAgentTransport;
  sessionId?: string;
  attachCommand?: string;
  livenessPollIntervalMs?: number;
  /**
   * False when a lost worker must never be launched again automatically: the transport
   * cannot prove the previous one is gone (Herdr, smarty-dev#266). Default true.
   */
  relaunchable?: boolean;
  /**
   * Why liveness gave up without proof that the worker exited (a Herdr server that stayed
   * unreachable). The run then fails as "lost track of the worker", and Fabric neither
   * relaunches it nor deletes its files. Undefined while contact holds or after a proven exit.
   */
  lostContact?(): string | undefined;
  isAlive(): Promise<boolean>;
  stop(): Promise<void>;
}

export interface AgentTransportAdapter {
  kind: FabricAgentTransport;
  available(): Promise<boolean>;
  launch(request: AgentTransportLaunch): Promise<AgentTransportHandle>;
}

export interface FabricLogLine {
  /** Legacy absolute line index; newer paged readers expose byte offset instead. */
  index?: number;
  offset: number;
  raw: string;
  parsed?: unknown;
}

export interface FabricAgentLog {
  id: string;
  runDirectory: string;
  logFile: string;
  status?: AgentRunRecord;
  events: FabricLogLine[];
  hasMore: boolean;
  before?: number;
}

export type FabricSteeringMode = "all" | "one-at-a-time";

export interface AgentSteerEntry {
  type: "steer" | "follow_up" | "set_steering_mode" | "set_follow_up_mode" | "compact";
  id: string;
  message?: string;
  mode?: FabricSteeringMode;
  instructions?: string;
  data?: unknown;
  ts: number;
}

export interface AgentSteerResult {
  queued: true;
  messageId: string;
}
