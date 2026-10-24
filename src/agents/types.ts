import type { ModelRouteDecision } from "./model-route.js";
import type { FabricTurnProvenance } from "../fabric-provenance.js";
import type { ImageContent } from "@earendil-works/pi-ai";
import type {
  SessionEntry,
  SessionMessageEntry,
} from "@earendil-works/pi-coding-agent";
import type { FabricAgentRunner, FabricAgentTransport, FabricPythonRuntime } from "../config.js";
import type { FabricKernel } from "../runtime/kernel.js";
import type { FabricScope, FabricScopeGrant } from "../protocol.js";
import type { ThinkingTransferInput } from "./thinking-transfer.js";
import type { FabricThinking, FabricThinkingBounds } from "../thinking.js";
import type { FabricParticipantResidency } from "../topology/types.js";
import type { AgentWriteReceipt } from "./write-receipt.js";
import type { InheritedSessionPin } from "./session-pins.js";
import type { AgentWorktreeResult } from "./worktree-manager.js";
import type { AgentUndeliveredMessage } from "./terminal-target.js";
import type { FabricActorActivationIdentity } from "../actors/activation-identity.js";

/** Fabric run transports plus adapter-owned ("hosted") runs. */
export type AgentRunTransport = FabricAgentTransport | "hosted";

export type AgentPlacementKind = "process" | "remote";

/** Trusted admission snapshot shared with transport and task-presence publication. */
export interface AgentPlacementAdmission {
  placementRunId: string;
  placementKind: AgentPlacementKind;
  placementHost: string;
  expectedSeconds: number;
  executionDeadline: string;
}

/**
 * A terminal outcome Fabric cannot vouch for: the run may or may not have done
 * its work (an interrupted claimed request, an unreachable hosted run). Fabric
 * never replays such work.
 */
export type FabricRunOutcome = "indeterminate";

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

export type AgentRunStopPath = "agents.stop" | "actors.stop" | "actors.remove" | "session-reset" | "residency-teardown" | "deadline";
export type AgentRunStopCaller = { by: Pick<AgentSpawner, "kind" | "id">; path: AgentRunStopPath };
export interface AgentRunStopIntent {
  reason: "explicit" | "deadline";
  requestedAt: number;
  /** Optional only for legacy records written before stop-caller provenance was added. */
  by?: Pick<AgentSpawner, "kind" | "id">;
  /** Optional only for legacy records written before stop-caller provenance was added. */
  path?: AgentRunStopPath;
  recoveryRecord?: Record<string, unknown>;
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
  /** Host-only: the actor queue item this run serves (smarty-dev#8935 recovery of an ENODEV-unconfirmed run). */
  activationId?: string;
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
  /** Caller classification passed to the optional external spawn router. */
  complexity?: "simple" | "normal" | "complex" | "delicate";
  /** Required target capabilities. Reserved local always pins to Main; unknown needs stay local. */
  needs?: string[];
  /** Absolute inputs that must exist on the selected execution host before starting. */
  requires?: string[];
  images?: ImageContent[];
  name?: string;
  runner?: FabricAgentRunner;
  /** Omitted/inherit uses the caller kernel; concrete kernels require Pi with Fabric extensions. */
  kernel?: FabricKernel | "inherit";
  /** Host-only snapshot for resident/trajectory forwarding; not a provider argument. */
  pythonRuntime?: FabricPythonRuntime;
  transport?: FabricAgentTransport;
  model?: string;
  /** Host-only selection provenance; never normalized from public arguments. */
  modelSource?: "explicit" | "default" | "inherited" | "route";
  /** Host-only original target retained across pre-admission fallback/forwarding. */
  modelPolicyOriginalModel?: string;
  /** Caller-supplied justification for a protected model selection. */
  modelReason?: string;
  /** Compatibility alias; modelReason wins when both are supplied. */
  opusReason?: string;
  /** Veda persona name; only used when runner is "veda". */
  persona?: string;
  thinking?: FabricThinking;
  /** Child thinking bounds; must lie inside the caller's effective bounds. */
  thinkingBounds?: FabricThinkingBounds;
  tools?: string[];
  timeoutMs?: number;
  extensions?: boolean;
  recursive?: boolean;
  /** Leaf or recursive execution cwd; relative to the immediate caller, independent of project/mesh lineage. */
  cwd?: string;
  worktree?: boolean;
  /** Shell command run in a new worktree before launch; overrides agents.worktree.setup. */
  worktreeSetup?: string;
  /** Explicit opt-out of the process task write default, audited at launch. */
  writePolicy?: "unrestricted";
  /** Write confinement enforced in the Pi child; see child-env.ts. */
  readOnly?: boolean;
  writableRoots?: string[];
  shell?: "deny" | "unconfined";
  /** Narrow the host-issued scope for the child; omitted inherits it unchanged. See src/scope.ts. */
  scope?: { grants: FabricScopeGrant[] };
  /**
   * Host-only full parent scope forwarded to a host without this session's
   * scope (resident host, actor turns); never a provider argument.
   */
  inheritedScope?: FabricScope;
  residency?: FabricParticipantResidency;
  schema?: Record<string, unknown>;
  /** With a schema on the Pi runner: the result is one fabric_reply tool call (smarty-dev#967). */
  replyTool?: boolean;
  systemPrompt?: string;
  /** Opt in to Claude Code transcript persistence for hook-based observability. */
  persistSession?: boolean;
  sessionFile?: string;
  /** Host-owned actor activation policy, not a one-shot provider argument. */
  inferenceContext?: "full-history" | "activation";
  actorId?: string;
  actorName?: string;
  /** Host-only immutable identity for this actor activation; never a provider argument. */
  actorSelf?: FabricActorActivationIdentity;
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
  /** Pi runs: seconds without output before a bash call without a timeout is killed (default 180); 0 = none. */
  bashIdleSeconds?: number;
  /** Host-created fork of the caller branch ending at its last completed turn (seed: "branch"). */
  forkSeed?: AgentForkSeed;
}

export interface AgentForkSeed {
  sourceSessionId: string;
  sourceSessionFile?: string;
  sourceBranch: SessionEntry[];
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

export interface AgentTimeoutIgnoredNote {
  code: "AGENT_TIMEOUT_BELOW_SHORTENING_FLOOR";
  requestedTimeoutMs: number;
  effectiveTimeoutMs: number;
  minimumTimeoutMs: number;
}

export interface AgentRunRecord {
  /** A tiny explicit shortening was ignored to protect real work. */
  timeoutNote?: AgentTimeoutIgnoredNote;
  /** Manager-owned durable intent distinguishing operator/deadline stops from worker checkpoints.
   * by/path are absent only on legacy records written before stop-caller provenance was added. */
  stopIntent?: AgentRunStopIntent;
  /** Canonical Fabric package root selected for the process worker at spawn time. */
  fabricRelease?: string;
  /** Admitted memory-capped task cgroup (smarty-dev#8257); absent when uncapped. */
  cgroup?: string;
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
  /** Exact original spawn brief, retained separately from mutable continuation task text. */
  promptSha256?: string;
  promptBytes?: number;
  promptPath?: string;
  /** Current task.txt bytes; changes when a same-run continuation rewrites the input. */
  inputSha256?: string;
  inputBytes?: number;
  /** True when close/retention dropped the current input file. */
  promptRetentionDropped?: boolean;
  /** True when the current input failed stable digest verification. */
  promptRetentionMismatch?: boolean;
  status: AgentRunStatus;
  /** Native execution contact or exit is unconfirmed; logical status is unchanged. */
  lostContact?: string;
  /** One-based FIFO admission position; present only while queued. */
  queuePosition?: number;
  runner: FabricAgentRunner;
  /** Resolved Fabric kernel; absent for runners without Fabric. */
  kernel?: FabricKernel;
  transport: AgentRunTransport;
  cwd: string;
  model?: string;
  modelReason?: string;
  thinking?: FabricThinking;
  /** Set only when the requested level was clamped into thinking bounds. */
  requestedThinking?: FabricThinking;
  actorId?: string;
  actorName?: string;
  capabilityRequirements?: string[];
  capabilityDigest?: string;
  recursive?: boolean;
  residency?: FabricParticipantResidency;
  /** smarty-dev#15878: the run's short private TMPDIR outside its run directory; cleanup follows the run. */
  temporaryDirectory?: string;
  startedAt: number;
  updatedAt: number;
  finishedAt?: number;
  /** Native final settlement; raw worker status stays running until drain/validation. */
  finishingAt?: number;
  currentTool?: string;
  currentToolStartedAt?: number;
  /** smarty-dev#8758: tool-generated receipt of the paths written and shell commands run (write-receipt.ts). */
  writeReceipt?: AgentWriteReceipt;
  followUpDeliveries?: AgentFollowUpDelivery[];
  turns: number;
  /** Actual model output/tool execution, not worker startup or an error-only turn. */
  inferenceStarted?: boolean;
  toolCalls: number;
  /** Durable ordinary-task answer boundary; native execution custody is separate. */
  finalAnswerReceipt?: { id: string; recordedAt: number };
  /** Assistant output; a non-completed run may contain partial, non-authoritative prose. */
  text: string;
  /** Last nonempty, fully streamed Pi assistant message, including tool-turn prose. */
  lastCompleteText?: string;
  /** Streaming text; on an interrupted failed task, retained partial output (possibly the preceding message), never a successful report. */
  partialText?: string;
  /** How a structured reply arrived: its fabric_reply tool call (smarty-dev#967). */
  replyVia?: "tool";
  value?: unknown;
  error?: string;
  /** Machine-readable terminal cause; failure publication is not an exit receipt.
   * CONTEXT_OVERFLOW is set only from a structured overflow signal, never free error text. */
  errorCode?: "RUNAWAY_TOOL_CALL_STREAM" | "PROCESS_LIVENESS_WATCH_FAILED" | "PROCESS_TREE_CUSTODY_UNCONFIRMED" | "CONTEXT_OVERFLOW" | "CUSTODY_UNCONFIRMED";
  executionCustody?: { state: "unconfirmed"; descendantsMayRemain: number; reason: string };
  /** Run diagnostics, including partial-report retention; warnings never override status. */
  warnings?: string[];
  stderr?: string;
  exitCode?: number | null;
  usage: AgentUsage;
  /** Actor runs: the last assistant turn's context tokens and the child model's window (smarty-dev#3238). */
  contextUsage?: { tokens: number; contextWindow: number };
  budget?: FabricBudgetSummary;
  /** Transport identity (e.g. process PID), not the native Pi session. */
  sessionId?: string;
  /** Linux process birth identity, persisted by the worker to detect PID reuse. */
  processStartTime?: string;
  /** Latest native runner session; joins Pi gateway session_id to this run. */
  runnerSessionId?: string;
  /** Placement-only presence metadata; absent when Placement API is disabled. */
  placementRunId?: string;
  placementKind?: AgentPlacementKind;
  placementHost?: string;
  expectedSeconds?: number;
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
  /** Set while a routed child dialog waits for an answer (status detail waiting_for_answer). */
  blockedOn?: { decisionId?: string; since: number };
  compaction?: AgentCompactionStatus;
  /** Unconsumed outcome retained for its exact bound Main root/session. */
  completionDelivery?: { status: "undelivered"; addressedTo: string; redeliveredFrom?: string };
  /** Terminal event-log optimization was skipped; the full original log remains. */
  compactionSkipped?: string;
  /** Settlement diff summary of a worktree: true run; `worktree` stays the path. */
  worktreeResult?: AgentWorktreeResult;
  /** Hosted runs: the adapter locator, persisted before the run is submitted. */
  hosted?: { locator: unknown };
  /** A hosted run the adapter reports parked; still running, not dead. */
  sleeping?: true;
  /** Set on terminal records whose effect Fabric cannot confirm. */
  outcome?: FabricRunOutcome;
  /** A hosted runner's failure hint; Fabric itself never retries. */
  retryable?: boolean;
}

export interface AgentRunResult extends AgentRunRecord {
  /** Resolution marker in agents.run results; does not replace the observed model. */
  via?: string;
  /** smarty-dev#8935: an ENODEV-unconfirmed run's saved answer, not yet settled; the actor commits it after delivery. */
  enodevRecovered?: boolean;
  /** Its delivery receipt already exists: a crash came after a delivery attempt; the actor replays idempotently. */
  enodevReplay?: boolean;
  /** Canonical launch selection when via is present; may differ from the observed model. */
  selectedModel?: string;
  /** Failed admission only: model/auth timed out before any transport launch was attempted.
   * The receipt remains terminal; an actor may separately retry its unlaunched activation. */
  launchPreparationTimeoutMs?: number;
  /** Failed admission only (smarty-dev#16357): the bound root lane was unresolved, so no worker started.
   * The receipt remains terminal; an actor may separately retry its unlaunched activation. */
  launchDenial?: "lane-unresolved";
  status: "completed" | "failed" | "stopped" | "timed_out";
}

export interface AgentHandleInfo {
  /** A tiny explicit shortening was ignored to protect real work. */
  timeoutNote?: AgentTimeoutIgnoredNote;
  /** Policy switch/audit diagnostics are present on receipts and final results. */
  warnings?: string[];
  /** Spawn admission: unavailable discovery never blocks launch, but is explicit in the receipt. */
  modelVerification?: "verified" | "unverified";
  finalAnswerReceipt?: { id: string; recordedAt: number };
  /** smarty-dev#9855: on a waiting spawn, the admission permits unknown-outcome launches still hold. */
  heldPermits?: number;
  /** Canonical Fabric package root selected for the process worker at spawn time. */
  fabricRelease?: string;
  /** Admitted memory-capped task cgroup (smarty-dev#8257); absent when uncapped. */
  cgroup?: string;
  followUpDeliveries?: AgentFollowUpDelivery[];
  routeClass?: string;
  routeClassSource?: AgentRunRouteMetadata["routeClassSource"];
  protected?: boolean;
  spawner?: AgentSpawner;
  /** Present on terminal status snapshots when the full log was retained. */
  compactionSkipped?: string;
  id: string;
  name: string;
  /** Exact original spawn brief metadata; never contains prompt text. */
  promptSha256?: string;
  promptBytes?: number;
  promptPath?: string;
  /** Current task.txt bytes, updated for a same-run continuation. */
  inputSha256?: string;
  inputBytes?: number;
  promptRetentionDropped?: boolean;
  promptRetentionMismatch?: boolean;
  status: AgentRunStatus;
  /** One-based FIFO admission position; present only while queued. */
  queuePosition?: number;
  runner: FabricAgentRunner;
  /** Resolved Fabric kernel; absent for runners without Fabric. */
  kernel?: FabricKernel;
  transport: AgentRunTransport;
  cwd: string;
  model?: string;
  modelReason?: string;
  thinking?: FabricThinking;
  /** Set only when the requested level was clamped into thinking bounds. */
  requestedThinking?: FabricThinking;
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
  /** Host-issued policy notices retained in every worker status record. */
  modelPolicyWarnings?: string[];
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
  /** Exact original spawn brief metadata. The task file may later contain a continuation. */
  promptSha256?: string;
  promptBytes?: number;
  promptPath?: string;
  inputSha256?: string;
  inputBytes?: number;
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
  /** The spawner's own sender name (smarty-dev#6207), sent as X-Smarty-Spawner. */
  spawnerName?: string;
  fabricSessionId?: string;
  extensions: boolean;
  tools: string[];
  grantedRisks: string[];
  maxTokens?: number;
  /** Niceness applied to the spawned child (and IO priority on Linux). */
  nice?: number;
  bashTimeoutSeconds?: number;
  bashIdleSeconds?: number;
  fabricExtensionPath?: string;
  routeHeader?: string;
  /** Host-only bounded judge: no ambient resources, compaction or retry. */
  judgment?: boolean;
  model?: string;
  modelReason?: string;
  thinking?: string;
  /** Serialized effective bounds forwarded as PI_FABRIC_THINKING_BOUNDS. */
  thinkingBounds?: string;
  systemPrompt?: string;
  persistSession?: boolean;
  modelAdmission?: "strict" | "permissive";
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
  /** Present when agents.childQuestions is "route": child dialogs go to the parent with this default deadline. */
  childQuestionTimeoutMs?: number;
  transport: FabricAgentTransport;
  sessionId?: string;
  attachCommand?: string;
  branch?: string;
  worktree?: string;
  inheritedSessionPins?: InheritedSessionPin[];
  /** Observed native Pi session history carried across a same-run worker relaunch. Not a resume target. */
  runnerSessionIds?: string[];
  carryOver?: AgentRunCarryOver;
  /** Default process task's own private scratch; exported as TMPDIR. */
  tmpdir?: string;
  /** Serialized PI_FABRIC_WRITE_POLICY; Pi children also load the write guard. */
  writePolicy?: string;
  /** Serialized PI_FABRIC_LINEAGE. */
  lineage?: string;
  /** Serialized PI_FABRIC_SCOPE; absent clears any inherited scope variables. */
  scope?: string;
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
  requires?: string[];
  /** Host-derived incompatibility, never accepted from guest arguments. */
  placementLocalReason?: string | undefined;
  /** Placement API admission; absent means the exact legacy transport path. */
  placementAdmission?: AgentPlacementAdmission;
  /** Report the default policy's actual local/remote launch boundary once; host-owned audit only. */
  onWritePolicyDisposition?: (disposition: "local" | "remote") => void;
  /** Trusted spawn facts needed by the island broker intent. */
  parentSession?: string;
  complexity?: AgentRunRequest["complexity"];
  /** Host-only carrier inheritance, after the worker environment is assembled. */
  childEnvironment?: (environment: NodeJS.ProcessEnv) => NodeJS.ProcessEnv;
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
  kind: AgentRunTransport;
  sessionId?: string;
  attachCommand?: string;
  /** Event custody is opt-in only after a verified, owned cgroup-v2 receipt.
   * Missing/failed admission keeps the legacy checked-query contract. */
  liveness?: "events" | "poll";
  livenessPollIntervalMs?: number;
  /** Natural-exit grace after a native terminal candidate, before custody cleanup. */
  finishingGraceMs?: number;
  /** Birth-checked descendants signalled during custody cleanup, counted once. */
  forcedCleanupCount?(): number;
  /** Conservative count of descendants whose exit is not confirmed. */
  remainingDescendantCount?(): number;
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
  /** Captured primary-worker exit notification, NOT native close or tree-exit proof.
   * Windows uses this only to begin its bounded native-close join after natural exit. */
  primaryExit?: Promise<void>;
  /** Passive native close notification; wakes monitoring, never itself grants collection. */
  closed?: Promise<void>;
  /** Scoped-only tree-empty receipt: the live PID and admission record agree
   * on our UID-owned v2 scope, with cgroup.events opened and watched before
   * publication. Populated-0 plus retained execution custody proves exit;
   * observation failure rejects, never authorizes collection/replacement.
   * Unverified scopes and unscoped workers omit this field entirely. */
  treeClosed?: Promise<void>;
  /** Admitted, memory-capped task cgroup path (smarty-dev#8257). Present only after the
   * launcher verified the capped scope receipt; uncapped/fallback launches omit it. */
  cgroup?: string;
  /** Bounded diagnostic tail for workers that fail before writing a status record. */
  readStderr?(): string;
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

/** smarty-dev#9855: held admission permits reached half of maxConcurrent. */
export interface AgentAdmissionAlarm {
  heldPermits: number;
  maxConcurrent: number;
  at: number;
  message: string;
  /** pi-fabric#939 round 4: one owner alarm when a held run's recovered answer cannot be saved (names the run).
   * pi-fabric#1554 P2-4: one alarm when a child launches without a short TMPDIR that fits the fleet bash guard
   * (names the first such run; at most once per caller TMPDIR per 6 h). */
  kind?: "recovered-answer-save-failed" | "short-tmpdir-unavailable";
  runId?: string;
}

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

/** A routed child dialog (agents.childQuestions "route"); `question` is the raw worker payload. */
export interface AgentChildQuestionRequest {
  runId: string;
  name: string;
  actorId?: string;
  question: Record<string, unknown>;
  /** Aborted when the run settles; the router must stop asking. */
  signal: AbortSignal;
  /** Report the durable decision backing a headless question. */
  onDecision(decisionId: string): void;
}

export type AgentChildQuestionResponse =
  | { value: string }
  | { confirmed: boolean }
  | { cancelled: true };

export interface AgentSteerResult {
  /** Explicit receipt when the final answer won the admission/append race. */
  undelivered?: AgentUndeliveredMessage;
  deadlineAt?: number;
  warning?: AgentFollowUpRunningWarning;
  queued: true;
  messageId: string;
}
