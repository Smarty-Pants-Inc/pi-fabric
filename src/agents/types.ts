import type { ModelRouteDecision } from "./model-route.js";
import type { FabricTurnProvenance } from "../fabric-provenance.js";
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
  | "finishing"
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

/** Record-only host classification; it never grants permission to route. */
export interface AgentRunRouteMetadata {
  routeClass: string;
  routeClassSource: "explicit" | "derived";
  /** Trusted caller protection snapshot; absent means unknown, never known-clear. */
  protected?: boolean;
}

export interface AgentSpawner {
  id: string;
  kind: "main" | "agent" | "actor";
  /** The activation that spawned the child; actor identity survives that run ending. */
  runId?: string;
}

export interface AgentRunRequest {
  /** Explicit history class; routing still requires a separately prepared decision. */
  routeClass?: string;
  protected?: boolean;
  /** Resident-host create deduplication key; reuse on retry (host-local, bounded retention). */
  idempotencyKey?: string;
  /** Host-created shadow decision; never accepted from external argument normalization. */
  routeDecision?: ModelRouteDecision;
  /** Host-only judgment join; never normalized from public agent arguments. */
  routeRecord?: { ledger: string; decisionRecorded: boolean };
  /** Host-only resident startup probe: model/extension admission, no prompt or tools. */
  residentStartupProbe?: boolean;
  /** Host-only admission snapshot. Never accepted by normalizeAgentRunRequest. */
  provenance?: FabricTurnProvenance | undefined;
  task: string;
  /** Required target capabilities. Unknown needs force configured placement local. */
  needs?: string[];
  images?: ImageContent[];
  name?: string;
  runner?: FabricAgentRunner;
  /** Omitted/inherit uses the caller kernel; concrete kernels require Pi with Fabric extensions. */
  kernel?: FabricKernel | "inherit";
  /** Host-only snapshot for resident/trajectory forwarding; not a provider argument. */
  pythonRuntime?: FabricPythonRuntime;
  transport?: FabricAgentTransport;
  model?: string;
  /** Caller-supplied justification for an explicit model selection. */
  modelReason?: string;
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
  /** Canonical Fabric package root selected for the process worker at spawn time. */
  fabricRelease?: string;
  /** Always populated for new runs; optional for legacy records. */
  routeClass?: string;
  routeClassSource?: AgentRunRouteMetadata["routeClassSource"];
  protected?: boolean;
  /** Immediate caller, distinct from the lineage Main. */
  spawner?: AgentSpawner;
  /** Requested launch model; model below follows verified state/assistant attribution. */
  requestedModel?: string;
  /** Shadow-route children: model/effort verified at the pre-prompt admission boundary. */
  admittedModel?: string;
  admittedThinking?: FabricThinking;
  id: string;
  name: string;
  task: string;
  status: AgentRunStatus;
  /** One-based FIFO admission position; present only while queued. */
  queuePosition?: number;
  runner: FabricAgentRunner;
  /** Resolved Fabric kernel; absent for runners without Fabric. */
  kernel?: FabricKernel;
  transport: FabricAgentTransport;
  cwd: string;
  model?: string;
  modelReason?: string;
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
  currentToolStartedAt?: number;
  followUpDeliveries?: AgentFollowUpDelivery[];
  turns: number;
  /** Actual model output/tool execution, not worker startup or an error-only turn. */
  inferenceStarted?: boolean;
  toolCalls: number;
  text: string;
  /** How a structured reply arrived: its fabric_reply tool call (smarty-dev#967). */
  replyVia?: "tool";
  value?: unknown;
  error?: string;
  /** Machine-readable terminal cause for a whitespace-only tool-call runaway. */
  errorCode?: "RUNAWAY_TOOL_CALL_STREAM";
  /** Non-fatal run problems, e.g. a dropped oversized child event (smarty-dev#1907). */
  warnings?: string[];
  stderr?: string;
  exitCode?: number | null;
  usage: AgentUsage;
  budget?: FabricBudgetSummary;
  /** Transport identity (e.g. process PID), not the native Pi session. */
  sessionId?: string;
  /** Linux process birth identity, persisted by the worker to detect PID reuse. */
  processStartTime?: string;
  /** Latest native runner session; joins Pi gateway session_id to this run. */
  runnerSessionId?: string;
  /** Distinct native Pi sessions observed during this run, in first-seen order. */
  runnerSessionIds?: string[];
  /** Parent Main participant and its Pi/Fabric session, not the child session. */
  mainAgentId?: string;
  fabricSessionId?: string;
  attachCommand?: string;
  branch?: string;
  worktree?: string;
  logFile?: string;
  nestedAgents?: AgentRunRecord[];
  pendingMessages?: { steering: string[]; followUp: string[] };
  compaction?: AgentCompactionStatus;
  /** Unconsumed outcome retained for its exact bound Main root/session. */
  completionDelivery?: { status: "undelivered"; addressedTo: string; redeliveredFrom?: string };
  /** Terminal event-log optimization was skipped; the full original log remains. */
  compactionSkipped?: string;
}

export interface AgentRunResult extends AgentRunRecord {
  /** Resolution marker in agents.run results; does not replace the observed model. */
  via?: string;
  /** Canonical launch selection when via is present; may differ from the observed model. */
  selectedModel?: string;
  /** Failed admission only: model/auth timed out before any transport launch was attempted.
   * The receipt remains terminal; an actor may separately retry its unlaunched activation. */
  launchPreparationTimeoutMs?: number;
  status: "completed" | "failed" | "stopped" | "timed_out";
}

export interface AgentHandleInfo {
  /** Canonical Fabric package root selected for the process worker at spawn time. */
  fabricRelease?: string;
  followUpDeliveries?: AgentFollowUpDelivery[];
  routeClass?: string;
  routeClassSource?: AgentRunRouteMetadata["routeClassSource"];
  protected?: boolean;
  spawner?: AgentSpawner;
  /** Present on terminal status snapshots when the full log was retained. */
  compactionSkipped?: string;
  id: string;
  name: string;
  status: AgentRunStatus;
  /** One-based FIFO admission position; present only while queued. */
  queuePosition?: number;
  runner: FabricAgentRunner;
  /** Resolved Fabric kernel; absent for runners without Fabric. */
  kernel?: FabricKernel;
  transport: FabricAgentTransport;
  cwd: string;
  model?: string;
  modelReason?: string;
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
  /** Spawn-selected Fabric package root, retained in the durable run record. */
  fabricRelease?: string;
  /** Host-created record metadata, independent of the route header/decision. */
  routeClass?: string;
  routeClassSource?: AgentRunRouteMetadata["routeClassSource"];
  protected?: boolean;
  residentStartupProbe?: boolean;
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
  routeHeader?: string;
  /** Host-only bounded judge: no ambient resources, compaction or retry. */
  judgment?: boolean;
  model?: string;
  modelReason?: string;
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
  /** Observed native Pi session history carried across a same-run worker relaunch. Not a resume target. */
  runnerSessionIds?: string[];
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
  needs?: string[];
  /** Host-derived incompatibility, never accepted from guest arguments. */
  placementLocalReason?: string | undefined;
  /** Manager close or explicit run/actor revocation, never a returned queued receipt's guest deadline. */
  signal?: AbortSignal | undefined;
  /** Host activation generation check. Recheck after preparation, immediately before worker creation. */
  authorize?: () => boolean;
  /** Persist unknown tree/native close before stop returns or a parent-only fallback reports exit. */
  onUnconfirmedExit?: (reason: string) => void;
}

export interface AgentTransportObservationOptions {
  signal?: AbortSignal;
  /** Absolute wall-clock deadline in milliseconds; transports may impose a shorter bound. */
  deadline?: number;
}

/** Session membership only: neither a terminal UI record nor session absence
 * proves that its worker (or descendants) exited. Unknown must veto custody. */
export type AgentTransportObservation =
  | { state: "alive" | "absent" }
  | { state: "unknown"; reason: string };

export interface AgentTransportHandle {
  /** Actual worker release, not the parent manager's loaded generation. */
  fabricRelease?: string;
  kind: FabricAgentTransport;
  sessionId?: string;
  attachCommand?: string;
  livenessPollIntervalMs?: number;
  /** Natural-exit grace after a native terminal candidate, before custody cleanup. */
  finishingGraceMs?: number;
  /** Birth-checked descendants signalled during custody cleanup, counted once. */
  forcedCleanupCount?(): number;
  /**
   * False when a lost worker must never be launched again automatically: the transport
   * cannot prove the previous one is gone (Herdr, smarty-dev#266). Default true.
   */
  relaunchable?: boolean;
  /** One-shot placement adapters have no steering/compaction channel. */
  controls?: boolean;
  /**
   * Why worker/tree exit could not be confirmed (lost contact or uncertain teardown).
   * Primary-worker exit alone does not clear process-tree debt. Fabric neither
   * relaunches it nor deletes its files. Undefined while contact holds or after a proven exit.
   */
  lostContact?(): string | undefined;
  /** Optional checked session observation; absence alone is NOT a worker exit receipt. */
  observe?(options?: AgentTransportObservationOptions): Promise<AgentTransportObservation>;
  /** Immutable debt from this captured native stop/close deadline, not a generic
   * liveness failure. Bounded host shutdown may finish with custody retained,
   * but task settlement and admission still require confirmed execution exit. */
  stopDebt?(): string | undefined;
  /** Bounded join of the captured process worker's native close (not PID absence). */
  waitForClose?(): Promise<void>;
  /** Passive native close notification; wakes monitoring, never itself grants collection. */
  closed?: Promise<void>;
  isAlive(options?: AgentTransportObservationOptions): Promise<boolean>;
  stop(options?: AgentTransportObservationOptions): Promise<void>;
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
  /** Exclusive byte offset; pair with generation as beforeGeneration on the next request. */
  before?: number;
  generation?: string;
}

export type FabricSteeringMode = "all" | "one-at-a-time";

export interface AgentFollowUpAlarm {
  code: "FABRIC_FOLLOW_UP_DEADLINE";
  messageId: string;
  targetId: string;
  targetName: string;
  deadlineAt: number;
  status: AgentRunStatus;
  currentTool?: string;
  currentToolStartedAt?: number;
  options: ["wait", "steer", "cancel"];
  message: string;
}

export interface AgentFollowUpDelivery {
  messageId: string;
  deadlineAt: number;
  state: "queued" | "settling" | "delivered" | "cancelled";
  alarm?: AgentFollowUpAlarm;
}

export interface AgentSteerEntry {
  provenance?: FabricTurnProvenance | undefined;
  followUpId?: string;
  deadlineAt?: number;
  type: "steer" | "follow_up" | "set_steering_mode" | "set_follow_up_mode" | "compact";
  id: string;
  message?: string;
  mode?: FabricSteeringMode;
  instructions?: string;
  data?: unknown;
  ts: number;
}

export const FOLLOW_UP_RUNNING_TASK_MESSAGE = "followUp to a running task waits until its current run finishes; use agents.steer for a correction needed before completion.";

/** Sender-only, receiver-time advisory; it does not change followUp delivery. */
export interface AgentFollowUpRunningWarning {
  code: "FABRIC_FOLLOW_UP_RUNNING_TASK";
  targetId: string;
  kind: "agent";
  status: "running";
  message: string;
}

export interface AgentSteerResult {
  deadlineAt?: number;
  warning?: AgentFollowUpRunningWarning;
  queued: true;
  messageId: string;
}
