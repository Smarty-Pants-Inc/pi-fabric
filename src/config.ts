import fs from "node:fs";
import { isIP } from "node:net";
import { diagnostics } from "./core/diagnostics.js";
import { readHostPolicy, warnAgentLandlockDisabled, type HostPolicy } from "./host-policy.js";
export { readHostPolicy } from "./host-policy.js";
import type { ModelRoutingConfig } from "./agents/model-route.js";
import { parsePerCallRouting } from "./agents/per-call-config.js";
import { normalizeAgentRouterConfig, type FabricAgentRouterConfig } from "./agents/router-config.js";
export type { FabricAgentRouterConfig } from "./agents/router-config.js";
import { normalizeAgentPlacement, type AgentPlacementConfig } from "./agents/placement-config.js";
import { isLoopbackPlacementUrl, normalizePlacementApiConfig, type PlacementApiConfig } from "./placement/config.js";
export type { PlacementApiConfig } from "./placement/config.js";
import type { SandboxTransportConfig } from "./transport/sandbox/types.js";
import { normalizeWakeTextConfig, type FabricWakeTextConfig } from "./actors/wake-text.js";
import type { LandlockSettings } from "./core/landlock.js";
import { DEFAULT_JEV_CONFIG, normalizeJevConfig, type FabricJevConfig } from "./jev/config.js";
import { DEFAULT_RECORDS_CONFIG, normalizeRecordsConfig, type FabricRecordsConfig } from "./records/config.js";
import { normalizeJevApprovalModel } from "./jev/model-key.js";
export type { FabricJevConfig } from "./jev/config.js";
import os from "node:os";
import path from "node:path";
import { renameAtomic } from "./core/atomic-write.js";
import { approvalActionOverridesValue, type FabricActionApprovalMode } from "./core/approval-overrides.js";
import { foregroundConfigValue, type FabricForegroundConfig } from "./core/foreground-tools.js";
import { quarantineDamagedFile } from "./core/damaged-file.js";
import { normalizeModelAliases, type FabricModelAliases } from "./core/model-resolution.js";
import { PI_CORE_TOOL_NAME_SET } from "./core/pi-tools.js";
import { DEFAULT_SHELL_HANG_MS, SHELL_HANG_MAX_MS } from "./core/shell-limits.js";
import {
  CURRENT_FABRIC_CONFIG_VERSION,
  migrateFabricConfigDocument,
} from "./config-migrations.js";
import type { FabricComponentEntry } from "./components/types.js";
import type { FabricRisk } from "./protocol.js";
import type { FabricKernel } from "./runtime/kernel.js";
import {
  DEFAULT_FABRIC_THINKING,
  FABRIC_THINKING_BOUNDS_ENV,
  inheritedThinkingBounds,
  intersectThinkingBounds,
  isFabricThinking,
  normalizeThinkingBounds,
  type FabricThinking,
  type FabricThinkingBounds,
} from "./thinking.js";
import { MESH_STATE_BACKEND_KIND_LIST, MESH_STATE_BACKEND_KINDS, type MeshStateBackendKind } from "./mesh/state-backend-kinds.js";
import {
  defaultCodePreviewSettings,
  normalizeCodePreviewSettings,
  type CodePreviewSettings,
} from "./ui/code-preview.js";

type FabricApprovalMode = "allow" | "ask" | "auto" | "deny";
export type FabricAgentTransport =
  | "auto"
  | "process"
  | "sandbox"
  | "tmux"
  | "screen"
  | "localterm"
  | "herdr";
/** Built-in runner ids, or a runner registered through pi-fabric/runners. */
export type FabricAgentRunner = "pi-durable" | "pi" | "claude" | "veda" | (string & {});

/** How a child run's reported model is checked against the requested key.
 * Strict fails the run on a mismatch. Permissive records the reported
 * attribution instead, so virtual provider keys can resolve to a concrete
 * backend at stream time. */
export type FabricModelAdmission = "strict" | "permissive";
export type FabricUiWidgetMode = "auto" | "always" | "hidden";
type FabricToolDisplayMode = "full" | "compact";
export type FabricIncomingMessageMode = "auto" | "collapsed" | "expanded";
export type FabricPrincipalViewMode = "auto" | "on" | "off";

/** Read the old incoming-only preference until a principal-view preference is saved. */
const principalViewModeValue = (ui: Record<string, unknown>): FabricPrincipalViewMode =>
  ui.principalView === "on" || ui.principalView === "off" || ui.principalView === "auto"
    ? ui.principalView
    : ui.incomingMessages === "collapsed" ? "on" : ui.incomingMessages === "expanded" ? "off" : "auto";
export type FabricResultFormat = "auto" | "yaml" | "json" | "text";
export type FabricPrewalkMode = "in-place" | "trajectory";
export type FabricExecutorRuntime = "quickjs" | "node-process" | "bun-process";
export type FabricConfigScope = "global" | "project";
type FabricCompactionEngine = "pi" | "fabric";
type FabricActorScope = "project" | "session";

export type FabricPythonRuntime = "cpython" | "monty";

/** Fixed root-owned grant source; no config, environment, workspace, or agent directory may redirect it. */
export const MODEL_EXCEPTIONS_PATH = "/etc/smarty/model-exceptions.json";

/** Whether pi.powershell and extension powershell overrides may run (smarty-dev#9826). */
export type FabricPowerShellPolicy = "refuse" | "allow";

interface FabricExecutorConfig {
  kernel: FabricKernel;
  pythonRuntime: FabricPythonRuntime;
  cpython: { binary: string };
  /** Optional jev-fabric backend for durable tasks and sessions (macOS/Linux).
   * `binary` empty or "auto" picks the user's compatible install outside the
   * workspace, then the bundled package; an explicit path never falls back.
   * `home` empty means JEV_FABRIC_HOME, else `<cwd>/.jev-fabric-native`, the
   * same store other harnesses share. `timeoutMs` is the default job lifetime. */
  jevFabric: { binary: string; home: string; timeoutMs: number };
  /** TypeScript backend only; ignored by the Python kernel. */
  runtime: FabricExecutorRuntime;
  /** Add native tool aliases, or opt into Pi's script-level naming/result ergonomics. */
  codemodeProfile: "additive" | "native";
  timeoutMs: number;
  /** Policy maximum for any executor deadline, including per-invocation
   * requests and per-ref floors. Values above this are visibly normalized. */
  maxTimeoutMs: number;
  /** Fixed whole-program ceiling for interactive Main only; overrides every deadline floor. */
  mainMaxTimeoutMs: number;
  /** Exact-ref deadline floors (ms) for known long-running host calls, e.g.
   * "extensions.subagent". Keys are exact refs; no wildcard matching. */
  hostCallTimeouts: Record<string, number>;
  /** Exact host-call refs that wait for a person, e.g. "extensions.ask".
   * While such a call is in flight the program deadline is paused, so a
   * foreground question can wait indefinitely. Cancellation still applies. */
  humanWaitRefs: string[];
  /** Wait budget for nested pi.bash / pi.powershell (default 2m, max 10m).
   * 0 disables auto-spill. After this, the await settles successfully with a
   * live output path while the process keeps running. `background: true`
   * detaches immediately. Explicit shell timeout remains a hard cap. */
  shellHangMs: number;
  /** Linux filesystem confinement for local pi.bash; no audit-only kernel mode. */
  landlock: LandlockSettings;
  /** pi.powershell and every captured or extension powershell override are
   * refused unless the host profile sets "allow" (smarty-dev#9826). No guard
   * judges PowerShell grammar, so "allow" accepts that gap. Host-only: the
   * project layer cannot opt in. */
  powershell: FabricPowerShellPolicy;
  memoryLimitBytes: number;
  maxOutputChars: number;
  maxNestedResultChars: number;
  resultFormat: FabricResultFormat;
}

export interface FabricApprovalConfig {
  read: FabricApprovalMode;
  write: FabricApprovalMode;
  execute: FabricApprovalMode;
  network: FabricApprovalMode;
  agent: FabricApprovalMode;
  model?: string;
  /** Exact `provider.action` or `provider.*` → allow | ask | deny; exact beats wildcard beats risk mode. */
  actions?: Record<string, FabricActionApprovalMode>;
  /** No-UI approvals: "deny" (absent, fail closed) or "decision" (await a durable user decision). */
  headless?: "deny" | "decision";
  /** Headless approval decision deadline; absent means 5 minutes. */
  headlessTimeoutMs?: number;
}

/** Session-start background revalidation scope for the MCP descriptor cache:
 * "changed" lists only added/reconfigured servers, "all" re-lists every known
 * server, "off" never spawns servers in the background. */
type FabricMcpRevalidatePolicy = "changed" | "all" | "off";

interface FabricMcpCacheConfig {
  /** Serve MCP tool metadata from the on-disk descriptor cache instead of
   * connecting to every configured server on first discovery each session. */
  enabled: boolean;
  revalidate: FabricMcpRevalidatePolicy;
  /** Wall-clock budget for one session-start background revalidation pass. */
  revalidateBudgetMs: number;
}

export interface FabricMcpJevConfig {
  /** Opt-in Jev ranking for tools.search({ searchMode: "semantic" }). */
  semanticSearch: boolean;
  /** MCP servers whose tool metadata must not be sent to Jev. Empty allows every server, including ones not yet cached. */
  blockedServers: string[];
  semanticCandidateLimit: number;
  semanticMinProbability: number;
}

const nativeMcpServersValue = (value: unknown): string[] => {
  if (value === undefined) return [];
  if (!Array.isArray(value) || value.length > 256 || value.some(name =>
    typeof name !== "string" || !/^[A-Za-z0-9_-]+$/.test(name.trim()))) {
    throw new Error("mcp.nativeServers must be an array of at most 256 exact Pi MCP server names (letters, digits, _ and -)");
  }
  return [...new Set(value.map((name: string) => name.trim()))];
};

export interface FabricMcpConfig {
  enabled: boolean;
  /** Opt-in exact server names owned by Pi, never mcporter fallback targets. */
  nativeServers?: string[];
  configPath?: string;
  disableOAuth: boolean;
  allowDynamicServers: boolean;
  callTimeoutMs: number;
  cache: FabricMcpCacheConfig;
  jev: FabricMcpJevConfig;
}

interface FabricClaudeRunnerConfig {
  binary: string;
  model?: string;
}

/** Veda CLI options for the `veda` agent runner. The Veda CLI wraps external
 * backends (agy, codex, claude-code, droid, pi, and any backend registered by
 * the installed Veda build); the backend is selected here and the persona
 * controls read-only vs write-capable behavior. */
interface FabricVedaRunnerConfig {
  binary: string;
  /** Veda backend to drive. Defaults to the Antigravity CLI (agy). */
  backend: string;
  /** Optional backend-specific model or Veda model alias. */
  model?: string;
  /** Veda persona: navigator-plan, navigator-chat, reviewer, or worker. */
  persona: string;
}

interface FabricPrewalkConfig {
  // Master switch, persisted by /fabric prewalk --disable|--enable or the
  // settings UI (absent means enabled). Manual arming, session auto-arm, and
  // boundary claims all gate on it.
  enabled?: boolean;
  mode: FabricPrewalkMode;
  model?: string;
  alwaysRearm: boolean;
  // Compact with the configured engine just before restoring Main's boundary
  // model after an in-place continuation settles.
  compactOnReturn: boolean;
  // Frontier-first planning: the mutation boundary that would hand off instead
  // asks Main to record its plan, and the handoff fires at the next mutation with
  // that plan in the transcript. Upstream prewalk nudges the plan on turn one,
  // before any discovery; this lands on the boundary, after it.
  requirePlan: boolean;
  // Filesystem fallback trigger: when an armed boundary ran a successful
  // pi.bash or pi.powershell without an audited mutation, claim on stat-manifest drift so
  // shell heredocs / sed -i / formatter writes also hand off.
  detectShellWrites: boolean;
  // Reasoning effort for the trajectory executor; unset inherits agents.thinking.
  thinking?: FabricThinking;
}

export interface FabricAgentConfig {
  /** Optional host-only external spawn/actor model router; off unless enabled. */
  router?: FabricAgentRouterConfig;
  /** Opt-in shadow routing pins and finite candidates, never live routing. */
  modelRouting?: ModelRoutingConfig;
  enabled: boolean;
  runner: FabricAgentRunner;
  transport: FabricAgentTransport;
  /** Root-policy Linux user scope slice; agent-dir values cannot relax this. */
  processSlice?: string;
  /** Root-policy memory.max (bytes) for capped process task cgroups, with swap 0
   * (smarty-dev#8257). Default 24 GiB; agent-dir/workspace values cannot relax it. */
  processMemoryMaxBytes?: number;
  /** Host-only opt-in process task placement; workspace files cannot override it. */
  placement?: AgentPlacementConfig;
  sandbox: SandboxTransportConfig;
  /** Host-only Placement API client; absent means the legacy path is unchanged. */
  placementApi?: PlacementApiConfig;
  /** User/workspace Pi default; package defaults intentionally leave this unset. */
  model?: string;
  /** Root deny baseline unioned with agent-dir additions; workspace lists are ignored. */
  deniedModels: string[];
  /** Root/default reason baseline unioned with agent-dir additions; only root may relax. */
  modelPolicy: {
    requireReason: string[];
    /** Model used for indirect/default resolution when the configured model is unavailable or denied. */
    fallbackModel?: string;
    /** Thinking level paired with the indirect fallback model. */
    fallbackThinking?: FabricThinking;
  };
  deniedModelReplacement?: string;
  /**
   * Internal trusted host path of `model-exceptions.json` (root-owned host source).
   * Never read from any config file: normalization always uses MODEL_EXCEPTIONS_PATH.
   * Direct embedders may set it, but every read validates the trusted source.
   */
  modelExceptionsPath?: string;
  /** Host-only file instructions root. Unset = ~/.local/share/smarty-dev/factory/current/. */
  instructionsRoot?: string;
  claude: FabricClaudeRunnerConfig;
  veda: FabricVedaRunnerConfig;
  thinking: FabricThinking;
  /** Configured effort only; excludes the built-in thinking fallback. */
  configuredThinking?: FabricThinking | undefined;
  maxConcurrent: number;
  maxPerExecution: number;
  maxDepth: number;
  timeoutMs: number;
  extensions: boolean;
  defaultTools: string[];
  retainRuns: boolean;
  notifyOnComplete: boolean;
  budgetUsd: number;
  maxTokensPerChild: number;
  /** Write usage-only pi-format session files per agent run for external trackers. */
  sessionExport: boolean;
  /** Export store root override; PI_FABRIC_AGENT_DIR wins. Empty = ~/.pi-fabric/agent. */
  sessionExportDir: string;
  /** Unix niceness 0-19 for every child agent; 0 leaves priority unchanged. */
  nice: number;
  /** Host-only, default off: hydrate projected GitHub webhook text from the local ingress receipt (smarty-dev#6144). */
  wakeText?: FabricWakeTextConfig;
  /** Host-only: skip durable-actor activations whose owning root is dead (smarty-dev#6062). */
  deadRootFilter: FabricDeadRootFilterConfig;
  /** Host-only: tell the root's Main once an activation stays in preparing this long (smarty-dev#6337). */
  stuckPreparingMs: number;
  /** Admission policy for the model a child process actually reports. */
  modelAdmission: FabricModelAdmission;
  /** Shell command run in each new agent worktree before the child starts. */
  worktree?: { setup?: string };
  /** Child UI dialogs: "cancel" (absent) or "route" to the parent UI or a decision. */
  childQuestions?: "cancel" | "route";
  /** Routed child question deadline; absent means 10 minutes. */
  childQuestionTimeoutMs?: number;
}

export type FabricDeadRootFilterMode = "off" | "on";

export interface FabricDeadRootFilterConfig {
  /** "off" (default) never skips; "on" skips activations of non-exempt actors under a dead root. */
  mode: FabricDeadRootFilterMode;
  /** Actor ids, id prefixes or exact actor names that always run, even under a dead root. */
  exempt: string[];
}

const MAX_DEAD_ROOT_EXEMPT = 512;

const MAX_DEAD_ROOT_EXEMPT_LENGTH = 200;
const deadRootExemptWarnings = new Set<string>();

/** An absent list is []; anything but an array of non-blank strings (each <= 200 chars, <= 512) is invalid. */
const deadRootExemptList = (value: unknown): string[] | undefined => {
  if (value === undefined) return [];
  if (!Array.isArray(value) || value.length > MAX_DEAD_ROOT_EXEMPT) return undefined;
  const entries: string[] = [];
  for (const entry of value) {
    if (typeof entry !== "string") return undefined;
    const trimmed = entry.trim();
    if (!trimmed || trimmed.length > MAX_DEAD_ROOT_EXEMPT_LENGTH) return undefined;
    entries.push(trimmed);
  }
  return [...new Set(entries)];
};

/**
 * Unknown or malformed input means "off". An invalid `exempt` shape disables the filter (mode off)
 * with one config warning: dropping a malformed exemption while staying on could skip a keep-actor.
 */
export const normalizeDeadRootFilterConfig = (value: unknown): FabricDeadRootFilterConfig => {
  const input = typeof value === "object" && value !== null && !Array.isArray(value) ? value as Record<string, unknown> : {};
  const exempt = deadRootExemptList(input.exempt);
  if (input.mode !== "on") return { mode: "off", exempt: exempt ?? [] };
  if (exempt) return { mode: "on", exempt };
  let shape: string;
  try {
    shape = String(JSON.stringify(input.exempt)).slice(0, 200);
  } catch {
    shape = typeof input.exempt;
  }
  if (!deadRootExemptWarnings.has(shape)) {
    if (deadRootExemptWarnings.size >= 64) deadRootExemptWarnings.clear();
    deadRootExemptWarnings.add(shape);
    diagnostics.warn("src/config.ts", `[pi-fabric] agents.deadRootFilter.exempt must be an array of non-empty strings (got ${shape}); the dead-root filter is disabled.`);
  }
  return { mode: "off", exempt: [] };
};

export interface FabricToolCaptureConfig {
  enabled: boolean;
  hideFromModel: boolean;
  keepVisible: string[];
  defaultRisk: FabricRisk;
  risks: Record<string, FabricRisk>;
}

export type FabricSchemaMode = "off" | "audit" | "enforce";

export interface FabricSchemaTrustedCommand {
  command: string;
  args: string[];
  shell: boolean;
  timeoutMs: number;
}

export interface FabricSchemaConfig {
  mode: FabricSchemaMode;
  certificateTtlMs: number;
  maxFiles: number;
  maxBytes: number;
  trustedCommands: Record<string, FabricSchemaTrustedCommand>;
}

interface FabricUiConfig {
  enabled: boolean;
  widget: FabricUiWidgetMode;
  maxRows: number;
  refreshMs: number;
  eventHistory: number;
  haltOnEscape: boolean;
  showAgentToolPreview: boolean;
  toolDisplay: FabricToolDisplayMode;
  /** @deprecated Use principalView; retained for incoming-only config compatibility. */
  incomingMessages: FabricIncomingMessageMode;
  principalView: FabricPrincipalViewMode;
  updateDebounceMs: number;
}

export interface FabricCompactionPressureBands {
  warn: number;
  urgent: number;
}

interface FabricCompactionConfig {
  engine: FabricCompactionEngine;
  targetContextRatio: number;
  thresholds: Record<string, number>;
  tokenThresholds: Record<string, number>;
  /** Occupancy fractions reported by `compact.pressure()`; 0 < warn < urgent < 1. */
  pressureBands: FabricCompactionPressureBands;
  /** Compact at a settled boundary when window headroom drops below this; 0 disables. */
  outputReserveTokens: number;
  /** Drop orphaned tool results and fill missing ones in outgoing context. */
  repairOrphans: boolean;
}

export const MIN_COMPACTION_TOKEN_THRESHOLD = 1_000;
export const MAX_COMPACTION_TOKEN_THRESHOLD = 100_000_000;
export const MIN_COMPACTION_RATIO_THRESHOLD = 0.25;
export const MAX_COMPACTION_RATIO_THRESHOLD = 0.95;

export const clampCompactionTokenThreshold = (value: number): number =>
  Math.min(
    MAX_COMPACTION_TOKEN_THRESHOLD,
    Math.max(MIN_COMPACTION_TOKEN_THRESHOLD, Math.round(value)),
  );

export const clampCompactionRatioThreshold = (value: number): number =>
  Math.min(
    MAX_COMPACTION_RATIO_THRESHOLD,
    Math.max(MIN_COMPACTION_RATIO_THRESHOLD, value),
  );

export interface FabricRetentionConfig {
  orphanedTempRunMs: number;
  oneShotRunMs: number;
  actorRunArchiveMs: number;
  /** Compact safe terminal run event logs after this age; latest actor runs are exempt. */
  terminalRunEventsAgeMs: number;
  /** Total retained events.jsonl bytes, including the truncation marker. */
  terminalRunEventsMaxBytes: number;
  /** Move eligible identity-less legacy runs into runs-retired/; never delete their bytes. */
  legacyRunArchiveEnabled?: boolean;
  /** Minimum terminal finishedAt age before legacy retirement (default 48 hours). */
  legacyRunArchiveAgeMs?: number;
}

/** Aggregate serialized event/image budget for an opt-in actor batch; single oversized events run alone. */
export const DEFAULT_ACTOR_BATCH_MAX_BYTES = 256 * 1024;

export interface FabricActorsConfig {
  /** Reset an actor's Pi session at a run boundary once its file exceeds this many bytes; 0 disables. */
  maxSessionBytes: number;
  /** How long a transient activation block (provider stream error, timeout, preparation or lock
   * timeout) gates routing before the actor gets a probe run (default 10 min; smarty-dev#6509). */
  transientBlockCooldownMs?: number;
  /** Cap of the doubling backoff after failed probes (default 60 min). */
  transientBlockMaxMs?: number;
}

/**
 * 1: the B68 wire; 2: a complete owner published by rename, with the holder's process incarnation; 3: protocol 2 plus
 * the holder's PID namespace on its own line (smarty-dev#9569), so a reclaimer uses pid evidence only in its own
 * namespace, and processes from before protocol 3 cannot parse (so never reclaim) its records. Switch a root to 3 once
 * its writers run a release that knows it.
 */
export type MeshLockProtocol = 1 | 2 | 3;

const meshLockProtocol = (value: unknown): MeshLockProtocol => {
  if (value === undefined) return 1;
  if (value === 1 || value === 2 || value === 3) return value;
  throw new Error("mesh.lockProtocol must be 1, 2 or 3");
};

/** Keyed mesh state backend (smarty-dev#6477 L2a): see src/mesh/state-backend.ts. */
export type MeshStateBackend = MeshStateBackendKind;

// The import-free kinds leaf on purpose: importing state-backend.ts here would put SQLite in the config graph.
const MESH_STATE_BACKENDS: readonly MeshStateBackend[] = MESH_STATE_BACKEND_KINDS;
const meshStateBackend = (value: unknown, env = process.env.PI_FABRIC_MESH_STATE_BACKEND): MeshStateBackend => {
  // The environment overrides the file setting. An unknown non-empty value is refused, never ignored:
  // a typo would silently open a different store than its peers and split the mesh (pi-fabric#796).
  const override = env?.trim().toLowerCase();
  if (override) {
    if ((MESH_STATE_BACKENDS as readonly string[]).includes(override)) return override as MeshStateBackend;
    throw new Error(`PI_FABRIC_MESH_STATE_BACKEND must be ${MESH_STATE_BACKEND_KIND_LIST} (got ${JSON.stringify(env)})`);
  }
  if (value === undefined) return "file";
  if (typeof value === "string" && (MESH_STATE_BACKENDS as readonly string[]).includes(value)) return value as MeshStateBackend;
  throw new Error(`mesh.stateBackend must be ${MESH_STATE_BACKEND_KIND_LIST}`);
};

/** Control carrier, independent of the keyed mesh state backend. */
export type FabricMeshTransport = "file" | "nats";

export class FabricMeshTransportConfigError extends Error {
  readonly code = "FABRIC_MESH_TRANSPORT_CONFIG";
  constructor(readonly requested: FabricMeshTransport, readonly hostTransport: FabricMeshTransport) {
    super(`mesh.transport (${requested}) differs from mesh.hostTransport (${hostTransport}); refusing mixed carriers on this host`);
    this.name = "FabricMeshTransportConfigError";
  }
}

const meshTransport = (value: unknown, env = process.env.PI_FABRIC_MESH_TRANSPORT): FabricMeshTransport => {
  // Match stateBackend: only the environment is trimmed/case-folded; a blank override is absent.
  const override = env?.trim().toLowerCase();
  if (override) {
    if (override === "file" || override === "nats") return override;
    throw new Error(`PI_FABRIC_MESH_TRANSPORT must be file or nats (got ${JSON.stringify(env)})`);
  }
  if (value === undefined) return "file";
  if (value === "file" || value === "nats") return value;
  throw new Error("mesh.transport must be file or nats");
};

const meshHostTransport = (value: unknown): FabricMeshTransport | undefined => {
  if (value !== undefined && value !== "file" && value !== "nats") {
    throw new Error("mesh.hostTransport must be file or nats");
  }
  // This is an inherited stamp, not an override of the host's own stamp.
  const inherited = process.env.PI_FABRIC_MESH_HOST_TRANSPORT;
  if (inherited !== undefined && inherited !== "file" && inherited !== "nats") {
    throw new Error("PI_FABRIC_MESH_HOST_TRANSPORT must be file or nats");
  }
  if (value !== undefined && inherited !== undefined && value !== inherited) {
    throw new FabricMeshTransportConfigError(value, inherited);
  }
  return value ?? inherited;
};

/** Host rollout policy for controls whose sender omits the destination epoch. */
export type MeshControlIncarnationFence = "warn" | "enforce";
const meshControlIncarnationFence = (value: unknown): MeshControlIncarnationFence => {
  if (value === undefined) return "warn";
  if (value === "warn" || value === "enforce") return value;
  throw new Error("mesh.controlIncarnationFence must be warn or enforce");
};

/** JetStream stream num_replicas for the NATS state backend; an explicit invalid value is refused, never defaulted. */
const meshNatsReplicas = (value: unknown): number => {
  if (value === undefined) return 1;
  if (typeof value === "number" && Number.isSafeInteger(value) && value >= 1) return value;
  throw new Error("mesh.nats.replicas must be a positive integer");
};

const meshNatsEphemeralHost = (value: unknown): boolean => {
  if (value === undefined) return false;
  if (typeof value === "boolean") return value;
  throw new FabricMeshNatsConfigError("ephemeralHost", "must be a boolean");
};

const meshNatsEphemeralInactiveSeconds = (value: unknown): number => {
  if (value === undefined) return 3_600;
  if (typeof value === "number" && Number.isSafeInteger(value) && value >= 300 && value <= 86_400) return value;
  throw new FabricMeshNatsConfigError("ephemeralInactiveSeconds", "must be an integer from 300 through 86400");
};

const DEFAULT_INBOX_MAX_AGE_MS = 7 * 24 * 60 * 60 * 1_000;
export const DEFAULT_NATS_EVENT_RECEIPT_LIMIT = 4_096;
const MIN_NATS_EVENT_RECEIPT_LIMIT = 1;
const MAX_NATS_EVENT_RECEIPT_LIMIT = 65_536;
const MIN_INBOX_MAX_AGE_MS = 60 * 1_000;
const MAX_INBOX_MAX_AGE_MS = 90 * 24 * 60 * 60 * 1_000;
const isValidInboxMaxAgeMs = (value: unknown): value is number =>
  typeof value === "number" && Number.isSafeInteger(value) && value >= MIN_INBOX_MAX_AGE_MS &&
  value <= MAX_INBOX_MAX_AGE_MS && Number.isSafeInteger(value * 1_000_000);

const meshNatsInboxMaxAgeMs = (value: unknown): number => {
  if (value === undefined) return DEFAULT_INBOX_MAX_AGE_MS;
  if (isValidInboxMaxAgeMs(value)) return value;
  throw new FabricMeshNatsConfigError("inboxMaxAgeMs",
    `must be an integer from ${MIN_INBOX_MAX_AGE_MS} through ${MAX_INBOX_MAX_AGE_MS} milliseconds`);
};

const meshNatsEventReceiptLimit = (value: unknown): number => {
  if (value === undefined) return DEFAULT_NATS_EVENT_RECEIPT_LIMIT;
  if (typeof value === "number" && Number.isSafeInteger(value) && value >= MIN_NATS_EVENT_RECEIPT_LIMIT &&
      value <= MAX_NATS_EVENT_RECEIPT_LIMIT) return value;
  throw new FabricMeshNatsConfigError("eventReceiptLimit",
    `must be an integer from ${MIN_NATS_EVENT_RECEIPT_LIMIT} through ${MAX_NATS_EVENT_RECEIPT_LIMIT}`);
};

/** Messaging/rollback settings fail visibly in the same typed config-error family as E9. */
export class FabricMeshNatsConfigError extends Error {
  readonly code = "FABRIC_MESH_TRANSPORT_CONFIG";
  constructor(readonly field: "operatorMachine" | "rollbackHoldUntil" | "servers" | "custodyStream" | "livenessStream" | "pathBMachines" | "tls" | "org" | "eventOrgs" | "allowInsecureLoopback" | "verifyMachine" | "ephemeralHost" | "ephemeralInactiveSeconds" | "events" | "provision" | "inboxMaxAgeMs" | "eventReceiptLimit", requirement: string) {
    super(`mesh.nats.${field} ${requirement}`);
    this.name = "FabricMeshNatsConfigError";
  }
}

export interface FabricMeshNatsConfig {
  /** Stream num_replicas on create; an existing stream with a different count is refused (default 1). */
  replicas: number;
  /** Opt-in NATS event log; normalized configs supply false. */
  events?: boolean;
  /** Org slug for event and service subjects; required when events is true or a session channel is opened. */
  org?: string;
  /** Host-owned event grant scopes; defaults to [org] and never comes from a workspace layer. */
  eventOrgs?: string[];
  /** Explicit event-stream provisioning gate; normalized configs supply false. Does not grant admin rights. */
  provision?: boolean;
  /** Messaging connection settings. No credential is read while loading config. */
  servers?: string | string[];
  /** Required literal policy mapping in NATS mode; full permission verification stays in the lazy adapter. */
  custodyStream?: string;
  livenessStream?: string;
  /** Explicit runtime inventory (1..64 unique, non-operator machine literals). */
  pathBMachines?: string[];
  /** Required trust anchor in NATS mode. Config never reads the CA or permits verification overrides. */
  tls?: { ca?: string; caFile?: string };
  /** Host credential file path, consumed only by the NATS adapter. */
  credentials?: string;
  /** Literal machine id; the adapter must also verify it against the credential. */
  machine?: string;
  /** Reserved fabric-ops[-<lowercase alphanumeric>] relay source token; required for NATS, never this runtime's machine. */
  operatorMachine?: string;
  /** UTC ISO-8601 withdrawal hold, also honored after rollback to file. Composition roots convert to epoch ms. */
  rollbackHoldUntil?: string;
  /** Parallel inbox lanes; normalized configs supply 8, with a minimum of 1. */
  inboxLanes?: number;
  /** Host-only: allow server-side expiration and clean-shutdown deletion of this host's inbox durable (default false). */
  ephemeralHost?: boolean;
  /** Server-side durable idle expiration in seconds when ephemeralHost is enabled (300..86400, default 3600). */
  ephemeralInactiveSeconds?: number;
  /** Inbox stream message age limit in milliseconds (60 s..90 d, default 7 d); existing max_age:0 streams remain compatible. */
  inboxMaxAgeMs?: number;
  /** Maximum persisted NATS actor event message-ID receipts per actor (1..65,536, default 4,096). */
  eventReceiptLimit?: number;
}

const meshNatsFlag = (value: unknown, field: "events" | "provision"): boolean => {
  if (value === undefined) return false;
  if (typeof value === "boolean") return value;
  throw new FabricMeshNatsConfigError(field, "must be a boolean");
};

const isMeshNatsOrgSlug = (value: unknown): value is string =>
  typeof value === "string" && /^[a-z0-9][a-z0-9-]{0,62}$/.exec(value)?.[0] === value && value !== "sys";

const meshNatsOrg = (value: unknown, events = false): string | undefined => {
  if (value === undefined && !events) return undefined;
  if (isMeshNatsOrgSlug(value)) return value;
  throw new FabricMeshNatsConfigError("org", "must be a non-sys org slug (1..63 lowercase letters, digits or hyphens, starting with a letter or digit); required when events is true");
};

const meshNatsEventOrgs = (value: unknown, org?: string): string[] => {
  const configured = value === undefined ? [] : value;
  if (!Array.isArray(configured) || configured.length > 64 || !Array.from(configured).every(isMeshNatsOrgSlug) ||
    new Set(configured).size !== configured.length) {
    throw new FabricMeshNatsConfigError("eventOrgs", "must be an array of up to 64 unique non-sys org slugs");
  }
  return [...new Set([...(org === undefined ? [] : [org]), ...configured])];
};

const meshNatsString = (value: unknown, field: "credentials" | "machine"): string | undefined => {
  if (value === undefined) return undefined;
  if (typeof value !== "string") throw new Error(`mesh.nats.${field} must be a string`);
  if (field === "machine" && /^[a-z0-9][a-z0-9-]{0,31}$/.exec(value)?.[0] !== value) {
    throw new Error("mesh.nats.machine must match /^[a-z0-9][a-z0-9-]{0,31}$/");
  }
  return value;
};

/** Shared E16b guard: roots validate before NATS open and again against transport.self.machine after open. */
export const validateFabricNatsOperatorMachine = (value: unknown, machine?: string): string => {
  // Require the whole reserved literal: JavaScript's $ also matches just before a final newline.
  if (typeof value !== "string" || value.length > 32 || /^fabric-ops(-[a-z0-9]+)?$/.exec(value)?.[0] !== value) {
    throw new FabricMeshNatsConfigError("operatorMachine", "is required in NATS mode and must match /^fabric-ops(-[a-z0-9]+)?$/");
  }
  if (value === machine) {
    throw new FabricMeshNatsConfigError("operatorMachine", "must differ from mesh.nats.machine");
  }
  return value;
};

const meshNatsRollbackHoldUntil = (value: unknown): string | undefined => {
  if (value === undefined) return undefined;
  // Minimal eager shape/calendar check only. Date.parse normalizes impossible dates;
  // compare the UTC date, and retain the literal precision for lazy root conversion.
  if (typeof value === "string" &&
    /^\d{4}-\d{2}-\d{2}T(?:[01]\d|2[0-3]):[0-5]\d(?::[0-5]\d(?:\.\d+)?)?(?:Z|\+00:00)$/.exec(value)?.[0] === value) {
    const instant = Date.parse(value);
    if (Number.isFinite(instant) && new Date(instant).toISOString().slice(0, 10) === value.slice(0, 10)) return value;
  }
  throw new FabricMeshNatsConfigError("rollbackHoldUntil", "must be a valid UTC ISO-8601 timestamp");
};

// ponytail: minimal eager mirrors of streams()/streamName() in mesh/nats-auth/permissions.ts
// and pathBMachines() in mesh/nats-auth/mint.ts. Do not import their permission catalog or NATS
// dependency graph into config. connection.ts performs canonical signed-scope verification lazily.
// Compare the full regex match, not only test(): `$` can otherwise admit a final line terminator.
const meshNatsStream = (value: unknown, field: "custodyStream" | "livenessStream"): string => {
  if (typeof value !== "string" || /^[A-Za-z0-9_-]{1,255}$/.exec(value)?.[0] !== value ||
    /^(?:FABRIC_MESH_|_INBOX)/.test(value.toUpperCase())) {
    throw new FabricMeshNatsConfigError(field, "is required in NATS mode and must be a non-reserved literal stream name");
  }
  return value;
};

const meshNatsPathBMachines = (value: unknown): string[] => {
  if (!Array.isArray(value) || value.length < 1 || value.length > 64 ||
    [...value].some(machine => typeof machine !== "string" || /^[a-z0-9][a-z0-9-]{0,31}$/.exec(machine)?.[0] !== machine ||
      /^fabric-ops(-[a-z0-9]+)?$/.exec(machine)?.[0] === machine) || new Set(value).size !== value.length) {
    throw new FabricMeshNatsConfigError("pathBMachines", "is required in NATS mode and must contain 1..64 unique non-operator machine literals");
  }
  return [...value] as string[];
};

const meshNatsTls = (value: unknown): NonNullable<FabricMeshNatsConfig["tls"]> => {
  if (!isJsonObject(value) || ![Object.prototype, null].includes(Object.getPrototypeOf(value)) ||
    Reflect.ownKeys(value).some(key => key !== "ca" && key !== "caFile")) {
    throw new FabricMeshNatsConfigError("tls", "is required in NATS mode and supports only ca and caFile");
  }
  const tls: NonNullable<FabricMeshNatsConfig["tls"]> = {};
  for (const key of ["ca", "caFile"] as const) {
    if (!Object.hasOwn(value, key)) continue;
    const entry = value[key];
    if (typeof entry !== "string" || !entry.trim()) {
      throw new FabricMeshNatsConfigError("tls", "ca and caFile must be nonempty strings");
    }
    if (key === "caFile" && !path.isAbsolute(entry)) {
      throw new FabricMeshNatsConfigError("tls", "caFile must be an absolute path");
    }
    tls[key] = entry;
  }
  if (tls.ca === undefined && tls.caFile === undefined) {
    throw new FabricMeshNatsConfigError("tls", "requires at least one of ca or caFile");
  }
  return tls;
};

const meshNatsServers = (value: unknown): string[] => {
  const validServer = (server: unknown): boolean => {
    if (typeof server !== "string") return false;
    const match = /^tls:\/\/([A-Za-z0-9.-]+):([0-9]{1,5})$/.exec(server);
    if (!match || match[0] !== server) return false;
    const host = match[1]!;
    if (host.length > 253) return false;
    // ponytail: a tls: URL is not WHATWG-special, so mixed-hex IPv4 survives its parser.
    // Use the standard HTTP hostname rules before the DNS check, not another numeric-host grammar.
    try { if (isIP(new URL(`http://${host}`).hostname) !== 0) return false; }
    catch { return false; }
    const labels = (host.endsWith(".") ? host.slice(0, -1) : host).split(".");
    return labels.every(label => /^[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?$/.exec(label)?.[0] === label) &&
      Number(match[2]) >= 1 && Number(match[2]) <= 65535;
  };
  const servers = typeof value === "string" ? [value] : value;
  if (!Array.isArray(servers) || servers.length === 0 || ![...servers].every(validServer)) {
    throw new FabricMeshNatsConfigError("servers", "must be a tls://<DNS>:port URL or an explicit nonempty array of those URLs");
  }
  return [...servers] as string[];
};

const meshNatsConfig = (value: unknown, transport: FabricMeshTransport): FabricMeshNatsConfig => {
  const nats = objectValue(value);
  // FILE ignores messaging settings and inherited snapshots. Only the existing machine and hold
  // remain rollback authority. Invalid authority refuses.
  if (transport === "file") {
    const machine = meshNatsString(nats.machine, "machine");
    const rollbackHoldUntil = meshNatsRollbackHoldUntil(nats.rollbackHoldUntil);
    return {
      replicas: meshNatsReplicas(nats.replicas), inboxLanes: 8, events: false, provision: false,
      ephemeralHost: meshNatsEphemeralHost(nats.ephemeralHost),
      ephemeralInactiveSeconds: meshNatsEphemeralInactiveSeconds(nats.ephemeralInactiveSeconds),
      inboxMaxAgeMs: DEFAULT_INBOX_MAX_AGE_MS,
      eventReceiptLimit: DEFAULT_NATS_EVENT_RECEIPT_LIMIT,
      ...(machine !== undefined ? { machine } : {}),
      ...(rollbackHoldUntil !== undefined ? { rollbackHoldUntil } : {}),
    };
  }
  let messaging = nats;
  const raw = process.env.PI_FABRIC_MESH_NATS;
  if (transport === "nats" && raw !== undefined) {
    let parsed: unknown;
    try { parsed = JSON.parse(raw); }
    catch { throw new Error("PI_FABRIC_MESH_NATS must be a JSON object"); }
    if (!isJsonObject(parsed)) throw new Error("PI_FABRIC_MESH_NATS must be a JSON object");
    messaging = parsed;
  }
  // Inheritance replaces messaging settings, including absent optional fields, but not state-backend replicas.
  for (const field of ["allowInsecureLoopback", "verifyMachine"] as const) {
    if (field in nats || field in messaging) throw new FabricMeshNatsConfigError(field, "is not supported in NATS config");
  }
  const events = meshNatsFlag(messaging.events, "events");
  const org = meshNatsOrg(messaging.org, events);
  const eventOrgs = meshNatsEventOrgs(messaging.eventOrgs, org);
  const provision = meshNatsFlag(messaging.provision, "provision");
  const servers = meshNatsServers(messaging.servers);
  const custodyStream = meshNatsStream(messaging.custodyStream, "custodyStream");
  const livenessStream = meshNatsStream(messaging.livenessStream, "livenessStream");
  if (custodyStream.toUpperCase() === livenessStream.toUpperCase()) {
    throw new FabricMeshNatsConfigError("livenessStream", "must differ from custodyStream (case-insensitive)");
  }
  const pathBMachines = meshNatsPathBMachines(messaging.pathBMachines);
  const tls = meshNatsTls(messaging.tls);
  const ephemeralHost = meshNatsEphemeralHost(messaging.ephemeralHost);
  const ephemeralInactiveSeconds = meshNatsEphemeralInactiveSeconds(messaging.ephemeralInactiveSeconds);
  const inboxMaxAgeMs = meshNatsInboxMaxAgeMs(messaging.inboxMaxAgeMs);
  const credentials = meshNatsString(messaging.credentials, "credentials");
  const machine = meshNatsString(messaging.machine, "machine");
  const operatorMachine = validateFabricNatsOperatorMachine(messaging.operatorMachine, machine);
  const rollbackHoldUntil = meshNatsRollbackHoldUntil(messaging.rollbackHoldUntil);
  return {
    replicas: meshNatsReplicas(nats.replicas),
    servers, custodyStream, livenessStream, pathBMachines, tls,
    events, provision, eventOrgs,
    ...(org !== undefined ? { org } : {}),
    ...(credentials !== undefined ? { credentials } : {}),
    ...(machine !== undefined ? { machine } : {}),
    operatorMachine,
    ...(rollbackHoldUntil !== undefined ? { rollbackHoldUntil } : {}),
    inboxLanes: boundedInteger(messaging.inboxLanes, 8, 1, Number.MAX_SAFE_INTEGER),
    ephemeralHost,
    ephemeralInactiveSeconds,
    inboxMaxAgeMs,
    eventReceiptLimit: meshNatsEventReceiptLimit(messaging.eventReceiptLimit),
  };
};

export const RESIDENT_IDLE_EXIT_MS = 10 * 60_000;

export interface FabricMeshConfig {
  /** Host-only; warn admits legacy senders, enforce requires a captured destination epoch. */
  controlIncarnationFence: MeshControlIncarnationFence;
  /** Startup-only wire protocol; 1 preserves compatibility with B68 writers. */
  lockProtocol: MeshLockProtocol;
  /** Keyed-state backend: "file" (default), "shadow", "sqlite" or "nats" (refused until built); env PI_FABRIC_MESH_STATE_BACKEND overrides. */
  stateBackend: MeshStateBackend;
  /** Control carrier; default file, env PI_FABRIC_MESH_TRANSPORT overrides. */
  transport: FabricMeshTransport;
  /** Explicit per-host rollout stamp. A differing runtime carrier is refused. */
  hostTransport?: FabricMeshTransport;
  /** NATS state-backend and messaging settings; selecting either remains independent. */
  nats: FabricMeshNatsConfig;
  enabled: boolean;
  root?: string;
  /** Publish the Main participant at session start instead of on first Fabric use. */
  announce: boolean;
  actorScope: FabricActorScope;
  maxEventBytes: number;
  maxReadEvents: number;
  actorPollMs: number;
  /** Exit a dead-root resident after this long with no actors or requests; 0 disables. */
  residentIdleExitMs: number;
  /** Explicit background observational age; 1 s runtime floor, never ordinary authority reads. */
  idleReadCoalesceMs: number;
  /** Admission window for commands routed over a mesh bridge, minimum 30 s. */
  bridgeControlTimeoutMs: number;
  actorQueueLimit: number;
  eventContextChars: number;
  actorContextEntries: number;
  /** A followUp to a busy Main that waited this long goes in at the next tool boundary; 0 disables. */
  followUpFlushMs: number;
  /** A Main idle with a held followUp this old tells its senders the queue is stalled; 0 disables. */
  followUpStallSeconds: number;
  /** Owning-host root absence alarm, independent of actor status (default 15 min). */
  rootPresenceAlarmMs: number;
  /** Undelivered Main message alarm to sender and target owner (default 30 min). */
  undeliveredAlarmMs: number;
  /** Explicit undeliverable receipt after root absence without a successor (default 2 h). */
  rootGoneTtlMs: number;
}

interface FabricRepairsConfig {
  enabled: boolean;
}

interface FabricEntropyConfig {
  compile: boolean;
}

interface FabricTraceConfig {
  /** Persist the opt-in FabricAssessmentTraceV1 (timings, usage) beside the execution trace. */
  assessment: boolean;
}

/** One configured portable memory source (see docs/memory-recall.md). */
export interface FabricMemorySourceConfig {
  /** Registry id used as `args.source` in source-qualified memory calls. */
  id: string;
  /** Adapter kind; `"fs"` walks a local directory of session JSONL files. */
  kind: "fs";
  /** Absolute directory the `fs` adapter enumerates recursively. */
  root: string;
}

export interface FabricExtractiveConfig {
  enabled: boolean;
  provider: string;
  model: string;
  maxViewBytes: number;
  maxCandidates: number;
  maxSourceChars: number;
  /** Zero selects deterministic-only operation. V1 issues at most one batch. */
  maxEvaluationsPerTurn: number;
  timeoutMs: number;
}

export const DEFAULT_EXTRACTIVE_CONFIG: FabricExtractiveConfig = {
  enabled: false, provider: "typesafe", model: "jev-latest",
  maxViewBytes: 8192, maxCandidates: 128, maxSourceChars: 24000,
  maxEvaluationsPerTurn: 1, timeoutMs: 3000,
};

export interface FabricMemoryConfig {
  extractive?: FabricExtractiveConfig;
  enabled: boolean;
  indexDir?: string;
  maxSessions: number;
  maxEntryChars: number;
  indexThinking: boolean;
  indexToolOutput: boolean;
  hotSessions?: number;
  digestTerms?: number;
  maxColdVocabularyBytes?: number;
  maxColdCacheBytes?: number;
  maxSyncSessions?: number;
  maxSyncSourceBytes?: number;
  maxCacheCleanupFiles?: number;
  regexMaxPatternBytes?: number;
  regexMaxHaystackTerms?: number;
  regexMaxHaystackBytes?: number;
  regexTimeoutMs?: number;
  /** Configured portable sources; present enables source-qualified calls. */
  sources?: FabricMemorySourceConfig[];
}

export interface FabricSpeculationConfig {
  /** Master switch for speculative programmatic tool calling during streaming. */
  enabled: boolean;
  /** Maximum simultaneously in-flight speculative calls; excess candidates are dropped. */
  maxConcurrent: number;
  /** Maximum retained unserved speculation entries per turn. */
  maxEntries: number;
  /** Per-stream cap on buffered partial tool-call arguments while extracting the `code` field. */
  maxBufferBytes: number;
  /** Unserved speculation entries older than this are aborted and discarded. */
  entryTtlMs: number;
  /**
   * Tier B: MCP tools that may be speculated despite risk "network". Entries
   * are `server.tool` or `server.*` and match the ref after the `mcp.` prefix.
   * Only enable for tools the operator knows are read-only; cached MCP
   * annotations with destructiveHint=true always refuse.
   */
  mcpAllowlist: string[];
}


export interface FabricModelsConfig {
  /** Alias name → ordered provider/model fallback chain plus an optional default thinking level. */
  aliases: FabricModelAliases;
}

export interface FabricThinkingConfig {
  /** Inclusive host/child thinking bounds; empty means the model's levels. */
  bounds: FabricThinkingBounds;
}

export interface FabricConfig {
  fullCodeMode: boolean;
  /** A Main reloads itself onto a newer active Fabric release at a safe run end (smarty-dev#2160). */
  autoReload: boolean;
  /** Automatic reload slots shared per host/user; 0 disables admission and jitter. */
  selfReloadConcurrency: number;
  executor: FabricExecutorConfig;
  approvals: FabricApprovalConfig;
  mcp: FabricMcpConfig;
  prewalk: FabricPrewalkConfig;
  agents: FabricAgentConfig;
  models: FabricModelsConfig;
  components: FabricComponentEntry[];
  capture: FabricToolCaptureConfig;
  /** Registered tools full code mode keeps declared beside fabric_exec. */
  foreground: FabricForegroundConfig;
  ui: FabricUiConfig;
  compaction: FabricCompactionConfig;
  retention: FabricRetentionConfig;
  actors: FabricActorsConfig;
  mesh: FabricMeshConfig;
  memory: FabricMemoryConfig;
  jev: FabricJevConfig;
  records: FabricRecordsConfig;
  entropy: FabricEntropyConfig;
  repairs: FabricRepairsConfig;
  trace: FabricTraceConfig;
  schema: FabricSchemaConfig;
  speculation: FabricSpeculationConfig;
  thinking: FabricThinkingConfig;
  codePreview: CodePreviewSettings;
}

/** Hard implementation maximum for any executor deadline. Policy ceilings
 * (executor.maxTimeoutMs) may be raised up to this value by administrators. */
export const MAX_EXECUTOR_TIMEOUT_MS = 24 * 3_600_000;

export const MIN_AGENT_TIMEOUT_MS = 1_000;
export const MAX_AGENT_TIMEOUT_MS = 24 * 3_600_000;
/** Default per-run wall-clock budget. It equals the policy ceiling on purpose:
 *  outside interactive Main, an orchestration program inherits agents.timeoutMs
 *  as its whole-program deadline floor, so any lower default truncates participants at a
 *  fraction of the maximum the policy already allows. Narrow one run by setting
 *  agents.timeoutMs explicitly; per-call values can only raise it. */
const DEFAULT_AGENT_TIMEOUT_MS = MAX_AGENT_TIMEOUT_MS;
export const QUICKJS_MAX_MEMORY_LIMIT_BYTES = 0xffff_ffff;
export const MAX_EXECUTOR_MEMORY_LIMIT_BYTES = Math.max(
  8 * 1024 * 1024,
  Math.min(Number.MAX_SAFE_INTEGER, Math.floor(os.totalmem())),
);

export const maxExecutorMemoryLimitBytes = (
  runtime: FabricExecutorRuntime,
  kernel: FabricKernel = "typescript",
): number =>
  kernel === "typescript" && runtime === "quickjs"
    ? Math.min(QUICKJS_MAX_MEMORY_LIMIT_BYTES, MAX_EXECUTOR_MEMORY_LIMIT_BYTES)
    : MAX_EXECUTOR_MEMORY_LIMIT_BYTES;

/** Default per-task cgroup memory.max (smarty-dev#8257): 24 GiB, swap capped at 0. */
export const DEFAULT_PROCESS_MEMORY_MAX_BYTES = 24 * 1024 ** 3;

/** A positive safe integer byte count, or the 24 GiB default for absent/invalid values. */
export const processMemoryMaxBytesValue = (value: unknown): number =>
  typeof value === "number" && Number.isSafeInteger(value) && value > 0 ? value : DEFAULT_PROCESS_MEMORY_MAX_BYTES;

export const DEFAULT_FABRIC_CONFIG: FabricConfig = {
  fullCodeMode: true,
  autoReload: true,
  selfReloadConcurrency: 6,
  executor: {
    kernel: "typescript",
    pythonRuntime: "monty",
    cpython: { binary: "python3" },
    jevFabric: { binary: "", home: "", timeoutMs: 3_600_000 },
    runtime: "quickjs",
    codemodeProfile: "additive",
    timeoutMs: 120_000,
    maxTimeoutMs: 900_000,
    mainMaxTimeoutMs: 600_000,
    hostCallTimeouts: {},
    humanWaitRefs: ["extensions.ask"],
    shellHangMs: DEFAULT_SHELL_HANG_MS,
    landlock: { mode: "off", disabled: false },
    powershell: "refuse",
    memoryLimitBytes: 64 * 1024 * 1024,
    maxOutputChars: 50_000,
    maxNestedResultChars: 2_000_000,
    resultFormat: "auto",
  },
  approvals: {
    read: "allow",
    write: "allow",
    execute: "allow",
    network: "allow",
    agent: "allow",
  },
  mcp: {
    enabled: true,
    nativeServers: [],
    disableOAuth: true,
    allowDynamicServers: true,
    callTimeoutMs: 120_000,
    cache: {
      enabled: true,
      revalidate: "changed",
      revalidateBudgetMs: 60_000,
    },
    jev: {
      semanticSearch: false,
      blockedServers: [],
      semanticCandidateLimit: 127,
      semanticMinProbability: 0.2,
    },
  },
  prewalk: {
    mode: "in-place",
    alwaysRearm: false,
    compactOnReturn: true,
    detectShellWrites: true,
    requirePlan: true,
  },
  agents: {
    enabled: true,
    // Fork policy (smarty-dev#5776): our route pins, release pinning, placement and residency
    // behaviour key on the "pi" runner; upstream's pi-durable default stays opt-in here.
    runner: "pi",
    transport: "process",
    processMemoryMaxBytes: DEFAULT_PROCESS_MEMORY_MAX_BYTES,
    sandbox: { enabled: false, template: "smarty-proof-agent", readyTimeoutMs: 90_000 },
    deniedModels: [],
    modelExceptionsPath: MODEL_EXCEPTIONS_PATH,
    modelPolicy: {
      requireReason: ["gpt-6-astra", "opus", "claude"],
      fallbackThinking: "high",
    },
    claude: { binary: "claude" },
    veda: { binary: "veda", backend: "agy", persona: "navigator-chat" },
    thinking: DEFAULT_FABRIC_THINKING,
    maxConcurrent: 4,
    maxPerExecution: 100,
    maxDepth: 2,
    timeoutMs: DEFAULT_AGENT_TIMEOUT_MS,
    extensions: true,
    defaultTools: ["read", "bash", "edit", "write", "grep", "find", "ls"],
    retainRuns: false,
    notifyOnComplete: true,
    budgetUsd: 0,
    maxTokensPerChild: 0,
    sessionExport: true,
    sessionExportDir: "",
    nice: 0,
    deadRootFilter: { mode: "off", exempt: [] },
    stuckPreparingMs: 600_000,
    modelAdmission: "strict",
  },
  jev: { ...DEFAULT_JEV_CONFIG, credentialCommand: [] },
  records: structuredClone(DEFAULT_RECORDS_CONFIG),
  components: [],
  capture: {
    enabled: true,
    hideFromModel: true,
    keepVisible: ["fabric_exec"],
    defaultRisk: "execute",
    risks: {
      read: "read",
      grep: "read",
      find: "read",
      ls: "read",
      edit: "write",
      write: "write",
      bash: "execute",
      fovea_sketch: "read",
      fovea_focus: "read",
      fovea_dwell: "read",
      fovea_impact: "read",
    },
  },
  ui: {
    enabled: true,
    widget: "auto",
    maxRows: 6,
    refreshMs: 500,
    eventHistory: 80,
    haltOnEscape: true,
    showAgentToolPreview: true,
    toolDisplay: "compact",
    incomingMessages: "auto",
    principalView: "auto",
    updateDebounceMs: 100,
  },
  compaction: {
    engine: "fabric",
    targetContextRatio: 0.65,
    thresholds: {},
    tokenThresholds: {},
    pressureBands: { warn: 0.6, urgent: 0.8 },
    outputReserveTokens: 0,
    repairOrphans: true,
  },
  retention: {
    orphanedTempRunMs: 6 * 60 * 60 * 1_000,
    oneShotRunMs: 24 * 60 * 60 * 1_000,
    actorRunArchiveMs: 7 * 24 * 60 * 60 * 1_000,
    terminalRunEventsAgeMs: 6 * 60 * 60 * 1_000,
    terminalRunEventsMaxBytes: 256 * 1024,
    legacyRunArchiveEnabled: true,
    legacyRunArchiveAgeMs: 48 * 60 * 60 * 1_000,
  },
  actors: {
    maxSessionBytes: 20 * 1024 * 1024,
    transientBlockCooldownMs: 10 * 60_000,
    transientBlockMaxMs: 60 * 60_000,
  },
  mesh: {
    controlIncarnationFence: "warn",
    lockProtocol: 1,
    stateBackend: "file",
    transport: "file",
    nats: { replicas: 1, inboxLanes: 8, events: false, provision: false, ephemeralHost: false, ephemeralInactiveSeconds: 3_600, inboxMaxAgeMs: DEFAULT_INBOX_MAX_AGE_MS, eventReceiptLimit: DEFAULT_NATS_EVENT_RECEIPT_LIMIT },
    enabled: true,
    announce: false,
    actorScope: "project",
    maxEventBytes: 256 * 1024,
    maxReadEvents: 500,
    actorPollMs: 250,
    residentIdleExitMs: RESIDENT_IDLE_EXIT_MS,
    idleReadCoalesceMs: 5_000,
    bridgeControlTimeoutMs: 30_000,
    actorQueueLimit: 32,
    eventContextChars: 40_000,
    actorContextEntries: 14,
    followUpFlushMs: 120_000,
    followUpStallSeconds: 600,
    rootPresenceAlarmMs: 15 * 60_000,
    undeliveredAlarmMs: 30 * 60_000,
    rootGoneTtlMs: 2 * 60 * 60_000,
  },
  models: {
    aliases: {},
  },
  memory: {
    extractive: { ...DEFAULT_EXTRACTIVE_CONFIG },
    enabled: true,
    maxSessions: 500,
    maxEntryChars: 2_000,
    indexThinking: false,
    indexToolOutput: true,
    hotSessions: 50,
    digestTerms: 200,
    maxColdVocabularyBytes: 512 * 1024,
    maxColdCacheBytes: 1024 * 1024,
    maxSyncSessions: 10_000,
    maxSyncSourceBytes: 512 * 1024 * 1024,
    maxCacheCleanupFiles: 100_000,
    regexMaxPatternBytes: 1_024,
    regexMaxHaystackTerms: 20_000,
    regexMaxHaystackBytes: 2 * 1024 * 1024,
    regexTimeoutMs: 250,
  },
  entropy: {
    compile: true,
  },
  repairs: {
    enabled: true,
  },
  trace: {
    assessment: false,
  },
  schema: {
    mode: "off",
    certificateTtlMs: 30_000,
    maxFiles: 100,
    maxBytes: 10 * 1024 * 1024,
    trustedCommands: {},
  },
  speculation: {
    enabled: true,
    maxConcurrent: 4,
    maxEntries: 64,
    maxBufferBytes: 2 * 1024 * 1024,
    entryTtlMs: 180_000,
    mcpAllowlist: [],
  },
  thinking: { bounds: {} },
  foreground: { tools: [], maxTools: 4 },
  codePreview: defaultCodePreviewSettings(),
};

interface JsonObjectFile {
  document: Record<string, unknown>;
  source: string;
}

const readJsonObjectFile = (filePath: string): JsonObjectFile | undefined => {
  let source: string;
  try {
    source = fs.readFileSync(filePath, "utf8");
  } catch (error) {
    if (error instanceof Error && "code" in error && error.code === "ENOENT") return undefined;
    // Unreadable settings must not take down extension load.
    return undefined;
  }
  if (!source.trim()) return { document: {}, source };
  try {
    const parsed: unknown = JSON.parse(source);
    if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
      quarantineDamagedFile(filePath);
      return undefined;
    }
    return { document: parsed as Record<string, unknown>, source };
  } catch {
    quarantineDamagedFile(filePath);
    return undefined;
  }
};

const readJsonObject = (filePath: string): Record<string, unknown> | undefined =>
  readJsonObjectFile(filePath)?.document;

const isJsonObject = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

/**
 * smarty-dev#11373: the NATS stream replicas from the agent layer only (`<agentDir>/fabric.json` mesh.nats.replicas),
 * validated by meshNatsReplicas (default 1). A missing file or an absent value is 1. Read-only: no project layer,
 * host policy, migration, quarantine, warning or write. An unreadable file, malformed JSON, a non-object document or
 * mesh/nats section, or an invalid value is refused (throws), never defaulted to a lower replica count.
 */
export const loadAgentNatsReplicas = (agentDir: string): number => {
  const filePath = path.join(agentDir, "fabric.json");
  let source: string;
  try {
    source = fs.readFileSync(filePath, "utf8");
  } catch (error) {
    if (error instanceof Error && "code" in error && error.code === "ENOENT") return meshNatsReplicas(undefined);
    throw new Error(`mesh.nats.replicas: cannot read ${filePath}`, { cause: error });
  }
  let document: unknown;
  try {
    document = JSON.parse(source);
  } catch {
    // Parser diagnostics can quote config contents; expose only the path, never the raw cause.
    throw new Error(`mesh.nats.replicas: ${filePath} is not valid JSON`);
  }
  if (!isJsonObject(document)) throw new Error(`mesh.nats.replicas: ${filePath} must be a JSON object`);
  const { mesh } = document;
  if (mesh === undefined) return meshNatsReplicas(undefined);
  if (!isJsonObject(mesh)) throw new Error("mesh must be an object");
  const { nats } = mesh;
  if (nats === undefined) return meshNatsReplicas(undefined);
  if (!isJsonObject(nats)) throw new Error("mesh.nats must be an object");
  return meshNatsReplicas(nats.replicas);
};

const mergeObjects = (
  base: Record<string, unknown>,
  override: Record<string, unknown>,
): Record<string, unknown> => {
  const merged = { ...base };
  for (const [key, value] of Object.entries(override)) {
    const baseValue = merged[key];
    // Routing documents replace atomically: never splice one provider's route/options
    // into another provider across trusted global/project layers or UI updates.
    if (key === "decisionProfiles") {
      merged[key] = structuredClone(value);
    } else if (
      typeof baseValue === "object" &&
      baseValue !== null &&
      !Array.isArray(baseValue) &&
      typeof value === "object" &&
      value !== null &&
      !Array.isArray(value)
    ) {
      merged[key] = mergeObjects(
        baseValue as Record<string, unknown>,
        value as Record<string, unknown>,
      );
    } else {
      merged[key] = value;
    }
  }
  return merged;
};

const approvalMode = (value: unknown, fallback: FabricApprovalMode): FabricApprovalMode =>
  value === "allow" || value === "ask" || value === "auto" || value === "deny"
    ? value
    : fallback;

const booleanValue = (value: unknown, fallback: boolean): boolean =>
  typeof value === "boolean" ? value : fallback;

const boundedInteger = (value: unknown, fallback: number, min: number, max: number): number =>
  typeof value === "number" && Number.isFinite(value)
    ? Math.max(min, Math.min(max, Math.floor(value)))
    : fallback;

const boundedFloat = (value: unknown, fallback: number, min: number, max: number): number =>
  typeof value === "number" && Number.isFinite(value)
    ? Math.max(min, Math.min(max, value))
    : fallback;

const stringValue = (value: unknown): string | undefined =>
  typeof value === "string" && value.trim() ? value : undefined;

// Inlined copy of RUNNER_ID_PATTERN (src/agents/runner-registry.ts) so config
// never loads the runner registry; tests/runner-registry-graph.test.ts keeps it in sync.
const RUNNER_ID_PATTERN = /^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$/;

// A registered runner may load after config; launch fails closed if it never does.
const runnerValue = (value: unknown, fallback: FabricAgentRunner): FabricAgentRunner =>
  typeof value === "string" && value.length <= 64 && RUNNER_ID_PATTERN.test(value) ? value : fallback;

const modelAdmissionValue = (
  value: unknown,
  fallback: FabricModelAdmission,
): FabricModelAdmission =>
  value === "strict" || value === "permissive" ? value : fallback;

const prewalkModeValue = (
  value: unknown,
  fallback: FabricPrewalkMode,
): FabricPrewalkMode =>
  value === "in-place" || value === "trajectory" ? value : fallback;

const sandboxPlacementUrlValue = (value: unknown): string => {
  const candidate = value === undefined ? "http://smarty-placement.smarty-island.svc:8080" : value;
  if (typeof candidate !== "string" || candidate.length > 512) throw new Error("Invalid agents.sandbox.placementUrl");
  let url: URL;
  try { url = new URL(candidate); } catch { throw new Error("Invalid agents.sandbox.placementUrl"); }
  const octets = url.hostname.split(".");
  const tailnetIp = octets.length === 4 && octets.every(part => /^\d{1,3}$/u.test(part) && Number(part) <= 255) &&
    Number(octets[0]) === 100 && Number(octets[1]) >= 64 && Number(octets[1]) <= 127;
  const service = url.hostname === "smarty-placement.smarty-island.svc" && url.port === "8080";
  if (url.protocol !== "http:" || url.username || url.password || url.pathname !== "/" || url.search || url.hash ||
    !(service || tailnetIp && url.port === "30880" || isLoopbackPlacementUrl(candidate, url))) throw new Error("agents.sandbox.placementUrl must use the in-cluster HTTP endpoint or a tailnet IPv4 NodePort");
  return url.origin;
};

const transportValue = (
  value: unknown,
  fallback: FabricAgentTransport,
): FabricAgentTransport =>
  value === "auto" ||
  value === "process" ||
  value === "sandbox" ||
  value === "tmux" ||
  value === "screen" ||
  value === "localterm" ||
  value === "herdr"
    ? value
    : fallback;

const thinkingValue = (value: unknown, fallback: FabricThinking): FabricThinking =>
  isFabricThinking(value) ? value : fallback;

const humanWaitRefsValue = (value: unknown): string[] =>
  Array.isArray(value)
    ? [...new Set(value
        .filter((ref): ref is string => typeof ref === "string")
        .map((ref) => ref.trim())
        .filter((ref) => ref.length > 0 && ref.length <= 512))].slice(0, 256)
    : [...DEFAULT_FABRIC_CONFIG.executor.humanWaitRefs];

const objectValue = (value: unknown): Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};

const widgetModeValue = (value: unknown, fallback: FabricUiWidgetMode): FabricUiWidgetMode =>
  value === "auto" || value === "always" || value === "hidden" ? value : fallback;

const toolDisplayModeValue = (
  value: unknown,
  fallback: FabricToolDisplayMode,
): FabricToolDisplayMode => value === "full" || value === "compact" ? value : fallback;

const executorKernelValue = (value: unknown, fallback: FabricKernel): FabricKernel =>
  value === "typescript" || value === "python" ? value : fallback;

const executorRuntimeValue = (
  value: unknown,
  fallback: FabricExecutorRuntime,
): FabricExecutorRuntime =>
  value === "quickjs" || value === "node-process" || value === "bun-process" ? value : fallback;

const resultFormatValue = (
  value: unknown,
  fallback: FabricResultFormat,
): FabricResultFormat =>
  value === "auto" || value === "yaml" || value === "json" || value === "text"
    ? value
    : fallback;

const compactionEngineValue = (
  value: unknown,
  fallback: FabricCompactionEngine,
): FabricCompactionEngine =>
  value === "pi" || value === "fabric" ? value : fallback;

// Bands are validated as a pair: any non-finite value or an ordering other
// than 0 < warn < urgent < 1 falls back to the defaults as a whole.
const compactionPressureBandsValue = (
  value: unknown,
  fallback: FabricCompactionPressureBands,
): FabricCompactionPressureBands => {
  const bands = objectValue(value);
  const warn = bands.warn === undefined ? fallback.warn : bands.warn;
  const urgent = bands.urgent === undefined ? fallback.urgent : bands.urgent;
  return typeof warn === "number"
    && typeof urgent === "number"
    && Number.isFinite(warn)
    && Number.isFinite(urgent)
    && warn > 0
    && warn < urgent
    && urgent < 1
    ? { warn, urgent }
    : { ...fallback };
};

const actorScopeValue = (value: unknown, fallback: FabricActorScope): FabricActorScope =>
  value === "project" || value === "session" ? value : fallback;

const schemaModeValue = (value: unknown, fallback: FabricSchemaMode): FabricSchemaMode =>
  value === "off" || value === "audit" || value === "enforce" ? value : fallback;

const mcpRevalidatePolicyValue = (
  value: unknown,
  fallback: FabricMcpRevalidatePolicy,
): FabricMcpRevalidatePolicy =>
  value === "changed" || value === "all" || value === "off" ? value : fallback;

const riskValue = (value: unknown, fallback: FabricRisk): FabricRisk =>
  value === "read" ||
  value === "write" ||
  value === "execute" ||
  value === "network" ||
  value === "agent"
    ? value
    : fallback;

const MEMORY_SOURCE_ID_PATTERN = /^[a-z0-9][a-z0-9_.-]{0,127}$/;

/**
 * Parse `memory.sources`. A wrong-typed value stays absent like every other
 * memory key, but a present array with malformed entries is a hard config
 * error: a silently dropped source would surface only as a confusing
 * `source_not_found` at call time. The id charset is the portable registry's
 * SOURCE_ID_PATTERN restricted to lowercase.
 */
const memorySourcesValue = (value: unknown): FabricMemorySourceConfig[] | undefined => {
  if (!Array.isArray(value)) return undefined;
  const sources: FabricMemorySourceConfig[] = [];
  const seen = new Set<string>();
  value.forEach((raw, index) => {
    const entry = objectValue(raw);
    const where = `memory.sources[${index}]`;
    const id = stringValue(entry.id);
    if (!id || !MEMORY_SOURCE_ID_PATTERN.test(id)) {
      throw new Error(
        `Invalid ${where}: id must be 1-128 chars of lowercase letters, digits, dot, underscore, or dash, starting with a letter or digit`,
      );
    }
    if (entry.kind !== "fs") {
      throw new Error(
        `Invalid ${where}: unknown kind ${JSON.stringify(entry.kind ?? null)}; expected "fs"`,
      );
    }
    const root = stringValue(entry.root);
    if (!root || !path.isAbsolute(root)) {
      throw new Error(`Invalid ${where}: root must be an absolute directory path`);
    }
    if (seen.has(id)) {
      throw new Error(`Invalid ${where}: duplicate source id ${JSON.stringify(id)}`);
    }
    seen.add(id);
    sources.push({ id, kind: "fs", root });
  });
  return sources.length > 0 ? sources : undefined;
};

export const normalizeFabricConfig = (input: Record<string, unknown>): FabricConfig => {
  const executor = objectValue(input.executor);
  const cpython = objectValue(executor.cpython);
  const landlock = objectValue(executor.landlock);
  if (landlock.mode !== undefined && landlock.mode !== "off" && landlock.mode !== "enforce") {
    throw new Error("executor.landlock.mode must be off or enforce. Landlock has no honest warn/audit mode on kernel 6.8; use a one-lane enforce trial with root-granted, logged PI_FABRIC_LANDLOCK_ESCAPE=1 commands.");
  }
  const jevFabric = objectValue(executor.jevFabric);
  const executorKernel = executorKernelValue(executor.kernel, DEFAULT_FABRIC_CONFIG.executor.kernel);
  const executorMaxTimeoutMs = boundedInteger(
    executor.maxTimeoutMs,
    DEFAULT_FABRIC_CONFIG.executor.maxTimeoutMs,
    1_000,
    MAX_EXECUTOR_TIMEOUT_MS,
  );
  const approvals = objectValue(input.approvals);
  const mcp = objectValue(input.mcp);
  const mcpCache = objectValue(mcp.cache);
  const mcpJev = objectValue(mcp.jev);
  const prewalk = objectValue(input.prewalk);
  const agents = objectValue(input.agents);
  const placement = normalizeAgentPlacement(agents.placement);
  const sandbox = objectValue(agents.sandbox);
  const sandboxTemplate = sandbox.template === undefined ? DEFAULT_FABRIC_CONFIG.agents.sandbox.template
    : typeof sandbox.template === "string" && /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/u.test(sandbox.template)
      ? sandbox.template : (() => { throw new Error("Invalid agents.sandbox.template"); })();
  const sandboxConfig: SandboxTransportConfig = {
    enabled: booleanValue(sandbox.enabled, DEFAULT_FABRIC_CONFIG.agents.sandbox.enabled),
    template: sandboxTemplate,
    ...(sandbox.placementUrl !== undefined ? { placementUrl: sandboxPlacementUrlValue(sandbox.placementUrl) } : {}),
    readyTimeoutMs: boundedInteger(sandbox.readyTimeoutMs, DEFAULT_FABRIC_CONFIG.agents.sandbox.readyTimeoutMs, 1_000, 90_000),
  };
  const placementApi = normalizePlacementApiConfig(agents.placementApi);
  const claude = objectValue(agents.claude);
  const veda = objectValue(agents.veda);
  const capture = objectValue(input.capture);
  const ui = objectValue(input.ui);
  const compaction = objectValue(input.compaction);
  const retention = objectValue(input.retention);
  const actors = objectValue(input.actors);
  const mesh = objectValue(input.mesh);
  const carrier = meshTransport(mesh.transport);
  const hostTransport = meshHostTransport(mesh.hostTransport);
  // An unstamped host is on today's file carrier; selecting NATS never silently flips the host.
  if (carrier !== (hostTransport ?? "file")) {
    throw new FabricMeshTransportConfigError(carrier, hostTransport ?? "file");
  }
  const memory = objectValue(input.memory);
  const extractive = objectValue(memory.extractive);
  const memorySources = memorySourcesValue(memory.sources);
  const entropy = objectValue(input.entropy);
  const repairs = objectValue(input.repairs);
  const trace = objectValue(input.trace);
  const modelsSection = objectValue(input.models);
  const schema = objectValue(input.schema);
  const schemaMode = schemaModeValue(schema.mode, DEFAULT_FABRIC_CONFIG.schema.mode);
  const speculation = objectValue(input.speculation);
  const configuredExecutorRuntime = executorRuntimeValue(
    executor.runtime,
    DEFAULT_FABRIC_CONFIG.executor.runtime,
  );
  const executorRuntime = schemaMode === "enforce" && executorKernel === "typescript"
    ? "quickjs"
    : configuredExecutorRuntime;
  const configuredTools = Array.isArray(agents.defaultTools)
    ? agents.defaultTools.filter(
        (tool): tool is string => typeof tool === "string" && Boolean(tool),
      )
    : DEFAULT_FABRIC_CONFIG.agents.defaultTools;
  const approvalModel = normalizeJevApprovalModel(stringValue(approvals.model));
  const approvalActions = approvalActionOverridesValue(approvals.actions);
  const configPath = stringValue(mcp.configPath);
  const meshRoot = stringValue(mesh.root);
  const memoryIndexDir = stringValue(memory.indexDir);
  const compactionThresholds = Object.fromEntries(
    Object.entries(objectValue(compaction.thresholds))
      .filter(([model, threshold]) =>
        model.includes("/")
        && typeof threshold === "number"
        && Number.isFinite(threshold),
      )
      .map(([model, threshold]) => [
        model,
        clampCompactionRatioThreshold(threshold as number),
      ]),
  );
  const compactionTokenThresholds = Object.fromEntries(
    Object.entries(objectValue(compaction.tokenThresholds))
      .filter(([model, tokens]) =>
        model.includes("/")
        && typeof tokens === "number"
        && Number.isFinite(tokens),
      )
      .map(([model, tokens]) => [
        model,
        clampCompactionTokenThreshold(tokens as number),
      ]),
  );
  const prewalkModel = stringValue(prewalk.model);
  const prewalkThinking = isFabricThinking(prewalk.thinking) ? prewalk.thinking : undefined;
  const agentModel = stringValue(agents.model);
  const modelPolicy = objectValue(agents.modelPolicy);
  const fallbackModel = stringValue(modelPolicy.fallbackModel)?.trim();
  const fallbackThinking = isFabricThinking(modelPolicy.fallbackThinking)
    ? modelPolicy.fallbackThinking
    : undefined;
  const wakeText = normalizeWakeTextConfig(agents.wakeText);
  const router = normalizeAgentRouterConfig(agents.router);
  const deniedModelReplacement = stringValue(agents.deniedModelReplacement)?.trim();
  const claudeBinary = stringValue(claude.binary);
  const claudeModel = stringValue(claude.model);
  const vedaBinary = stringValue(veda.binary);
  const vedaBackend = stringValue(veda.backend);
  const vedaModel = stringValue(veda.model);
  const vedaPersona = stringValue(veda.persona);
  const agentThinking = thinkingValue(agents.thinking, DEFAULT_FABRIC_CONFIG.agents.thinking);
  const configuredComponents: FabricComponentEntry[] = Array.isArray(input.components)
    ? input.components.flatMap((raw) => {
        const componentEntry = objectValue(raw);
        const id = stringValue(componentEntry.id);
        const component = stringValue(componentEntry.component);
        if (!id || !component) return [];
        return [{
          id,
          component,
          ...(Object.prototype.hasOwnProperty.call(componentEntry, "config")
            ? { config: componentEntry.config }
            : {}),
          ...(typeof componentEntry.disabled === "boolean"
            ? { disabled: componentEntry.disabled }
            : {}),
        }];
      }).slice(0, 256)
    : DEFAULT_FABRIC_CONFIG.components;
  const configuredVisible = Array.isArray(capture.keepVisible)
    ? capture.keepVisible.filter(
        (name): name is string => typeof name === "string" && Boolean(name.trim()),
      )
    : DEFAULT_FABRIC_CONFIG.capture.keepVisible;
  const configuredRisks = {
    ...DEFAULT_FABRIC_CONFIG.capture.risks,
    ...objectValue(capture.risks),
  };
  const trustedCommands = Object.fromEntries(
    Object.entries(objectValue(schema.trustedCommands)).flatMap(([name, raw]) => {
      if (!/^[a-zA-Z0-9][a-zA-Z0-9._-]{0,63}$/.test(name)) return [];
      const command = objectValue(raw);
      const executable = stringValue(command.command);
      if (!executable) return [];
      const shell = booleanValue(command.shell, false);
      const args = !shell && Array.isArray(command.args)
        ? command.args.filter((arg): arg is string => typeof arg === "string").slice(0, 64)
        : [];
      return [[name, {
        command: executable,
        args,
        shell,
        timeoutMs: boundedInteger(command.timeoutMs, 30_000, 1, 300_000),
      } satisfies FabricSchemaTrustedCommand]];
    }),
  );
  const risks = Object.fromEntries(
    Object.entries(configuredRisks)
      .filter(([name]) => Boolean(name.trim()))
      .map(([name, risk]) => [name, riskValue(risk, DEFAULT_FABRIC_CONFIG.capture.defaultRisk)]),
  );

  return {
    fullCodeMode: booleanValue(input.fullCodeMode, DEFAULT_FABRIC_CONFIG.fullCodeMode),
    autoReload: booleanValue(input.autoReload, DEFAULT_FABRIC_CONFIG.autoReload),
    selfReloadConcurrency: typeof input.selfReloadConcurrency === "number"
      && Number.isSafeInteger(input.selfReloadConcurrency) && input.selfReloadConcurrency >= 0
      ? input.selfReloadConcurrency : DEFAULT_FABRIC_CONFIG.selfReloadConcurrency,
    executor: {
      kernel: executorKernel,
      pythonRuntime: executor.pythonRuntime === "cpython" ? "cpython" : "monty",
      cpython: {
        binary: stringValue(cpython.binary)?.trim() ?? DEFAULT_FABRIC_CONFIG.executor.cpython.binary,
      },
      jevFabric: {
        binary: stringValue(jevFabric.binary)?.trim() ?? DEFAULT_FABRIC_CONFIG.executor.jevFabric.binary,
        home: stringValue(jevFabric.home)?.trim() ?? "",
        timeoutMs: boundedInteger(jevFabric.timeoutMs, DEFAULT_FABRIC_CONFIG.executor.jevFabric.timeoutMs, 1_000, MAX_EXECUTOR_TIMEOUT_MS),
      },
      runtime: executorRuntime,
      codemodeProfile: executor.codemodeProfile === "native" ? "native" : "additive",
      maxTimeoutMs: boundedInteger(
        executor.maxTimeoutMs,
        DEFAULT_FABRIC_CONFIG.executor.maxTimeoutMs,
        1_000,
        MAX_EXECUTOR_TIMEOUT_MS,
      ),
      mainMaxTimeoutMs: boundedInteger(
        executor.mainMaxTimeoutMs,
        Math.min(DEFAULT_FABRIC_CONFIG.executor.mainMaxTimeoutMs, executorMaxTimeoutMs),
        Math.min(60_000, executorMaxTimeoutMs),
        executorMaxTimeoutMs,
      ),
      hostCallTimeouts: Object.fromEntries(
        Object.entries(objectValue(executor.hostCallTimeouts))
          .filter(([ref, value]) =>
            typeof ref === "string" &&
            Boolean(ref.trim()) &&
            typeof value === "number" &&
            Number.isFinite(value) &&
            value >= 1,
          )
          .map(([ref, value]) => [ref, boundedInteger(value, 1_000, 1_000, executorMaxTimeoutMs)]),
      ),
      humanWaitRefs: humanWaitRefsValue(executor.humanWaitRefs),
      timeoutMs: boundedInteger(
        executor.timeoutMs,
        DEFAULT_FABRIC_CONFIG.executor.timeoutMs,
        1_000,
        executorMaxTimeoutMs,
      ),
      shellHangMs: boundedInteger(
        executor.shellHangMs,
        DEFAULT_FABRIC_CONFIG.executor.shellHangMs,
        0,
        Math.min(executorMaxTimeoutMs, SHELL_HANG_MAX_MS),
      ),
      landlock: {
        mode: landlock.mode === "enforce" ? "enforce" : "off",
        disabled: landlock.disabled === true,
        ...(landlock.allowEscape === true ? { allowEscape: true } : {}),
      },
      // Fail closed: anything but the exact opt-in refuses PowerShell (smarty-dev#9826).
      powershell: executor.powershell === "allow" ? "allow" : "refuse",
      memoryLimitBytes: boundedInteger(
        executor.memoryLimitBytes,
        DEFAULT_FABRIC_CONFIG.executor.memoryLimitBytes,
        8 * 1024 * 1024,
        maxExecutorMemoryLimitBytes(executorRuntime, executorKernel),
      ),
      maxOutputChars: boundedInteger(
        executor.maxOutputChars,
        DEFAULT_FABRIC_CONFIG.executor.maxOutputChars,
        1_000,
        1_000_000,
      ),
      maxNestedResultChars: boundedInteger(
        executor.maxNestedResultChars,
        DEFAULT_FABRIC_CONFIG.executor.maxNestedResultChars,
        10_000,
        20_000_000,
      ),
      resultFormat: resultFormatValue(
        executor.resultFormat,
        DEFAULT_FABRIC_CONFIG.executor.resultFormat,
      ),
    },
    approvals: {
      read: approvalMode(approvals.read, DEFAULT_FABRIC_CONFIG.approvals.read),
      write: approvalMode(approvals.write, DEFAULT_FABRIC_CONFIG.approvals.write),
      execute: approvalMode(approvals.execute, DEFAULT_FABRIC_CONFIG.approvals.execute),
      network: approvalMode(approvals.network, DEFAULT_FABRIC_CONFIG.approvals.network),
      agent: approvalMode(approvals.agent, DEFAULT_FABRIC_CONFIG.approvals.agent),
      ...(approvalModel ? { model: approvalModel } : {}),
      ...(approvalActions ? { actions: approvalActions } : {}),
      ...(approvals.headless === "decision" ? { headless: "decision" as const } : {}),
      ...(approvals.headlessTimeoutMs === undefined ? {} : {
        headlessTimeoutMs: boundedInteger(approvals.headlessTimeoutMs, 300_000, 1_000, 86_400_000),
      }),
    },
    mcp: {
      enabled: booleanValue(mcp.enabled, DEFAULT_FABRIC_CONFIG.mcp.enabled),
      nativeServers: nativeMcpServersValue(mcp.nativeServers),
      ...(configPath ? { configPath } : {}),
      disableOAuth: booleanValue(mcp.disableOAuth, DEFAULT_FABRIC_CONFIG.mcp.disableOAuth),
      allowDynamicServers: booleanValue(
        mcp.allowDynamicServers,
        DEFAULT_FABRIC_CONFIG.mcp.allowDynamicServers,
      ),
      callTimeoutMs: boundedInteger(
        mcp.callTimeoutMs,
        DEFAULT_FABRIC_CONFIG.mcp.callTimeoutMs,
        1_000,
        900_000,
      ),
      cache: {
        enabled: booleanValue(mcpCache.enabled, DEFAULT_FABRIC_CONFIG.mcp.cache.enabled),
        revalidate: mcpRevalidatePolicyValue(
          mcpCache.revalidate,
          DEFAULT_FABRIC_CONFIG.mcp.cache.revalidate,
        ),
        revalidateBudgetMs: boundedInteger(
          mcpCache.revalidateBudgetMs,
          DEFAULT_FABRIC_CONFIG.mcp.cache.revalidateBudgetMs,
          1_000,
          600_000,
        ),
      },
      jev: {
        semanticSearch: booleanValue(
          mcpJev.semanticSearch,
          DEFAULT_FABRIC_CONFIG.mcp.jev.semanticSearch,
        ),
        blockedServers: Array.isArray(mcpJev.blockedServers)
          ? [...new Set(
              mcpJev.blockedServers.flatMap((name) => {
                if (typeof name !== "string") return [];
                const trimmed = name.trim();
                return trimmed.length > 0 && trimmed.length <= 128 ? [trimmed] : [];
              }),
            )].slice(0, 256)
          : [...DEFAULT_FABRIC_CONFIG.mcp.jev.blockedServers],
        semanticCandidateLimit: boundedInteger(
          mcpJev.semanticCandidateLimit,
          DEFAULT_FABRIC_CONFIG.mcp.jev.semanticCandidateLimit,
          2,
          127,
        ),
        semanticMinProbability:
          typeof mcpJev.semanticMinProbability === "number" &&
          Number.isFinite(mcpJev.semanticMinProbability) &&
          mcpJev.semanticMinProbability >= 0 &&
          mcpJev.semanticMinProbability <= 1
            ? mcpJev.semanticMinProbability
            : DEFAULT_FABRIC_CONFIG.mcp.jev.semanticMinProbability,
      },
    },
    prewalk: {
      ...(prewalk.enabled === false ? { enabled: false } : {}),
      mode: prewalkModeValue(prewalk.mode, DEFAULT_FABRIC_CONFIG.prewalk.mode),
      ...(prewalkModel ? { model: prewalkModel } : {}),
      ...(prewalkThinking ? { thinking: prewalkThinking } : {}),
      alwaysRearm: booleanValue(
        prewalk.alwaysRearm,
        DEFAULT_FABRIC_CONFIG.prewalk.alwaysRearm,
      ),
      compactOnReturn: booleanValue(
        prewalk.compactOnReturn,
        DEFAULT_FABRIC_CONFIG.prewalk.compactOnReturn,
      ),
      detectShellWrites: booleanValue(
        prewalk.detectShellWrites,
        DEFAULT_FABRIC_CONFIG.prewalk.detectShellWrites,
      ),
      requirePlan: booleanValue(
        prewalk.requirePlan,
        DEFAULT_FABRIC_CONFIG.prewalk.requirePlan,
      ),
    },
    agents: {
      enabled: booleanValue(agents.enabled, DEFAULT_FABRIC_CONFIG.agents.enabled),
      runner: runnerValue(agents.runner, DEFAULT_FABRIC_CONFIG.agents.runner),
      transport: transportValue(agents.transport, DEFAULT_FABRIC_CONFIG.agents.transport),
      ...(typeof agents.processSlice === "string" && /^[a-zA-Z0-9_.-]+\.slice$/.test(agents.processSlice)
        ? { processSlice: agents.processSlice } : {}),
      processMemoryMaxBytes: processMemoryMaxBytesValue(agents.processMemoryMaxBytes),
      ...(placement ? { placement } : {}),
      sandbox: sandboxConfig,
      ...(placementApi ? { placementApi } : {}),
      ...(router ? { router } : {}),
      ...(agentModel ? { model: agentModel } : {}),
      ...(typeof agents.modelRouting === "object" && agents.modelRouting !== null && !Array.isArray(agents.modelRouting)
        ? { modelRouting: (() => {
            const routing = agents.modelRouting as Record<string, unknown>;
            if (routing.live !== undefined && routing.live !== false) throw new Error("Live model routing requires measured parity and Paul's floor approval (#2236); unavailable in shadow mode");
            return {
              live: false as const,
              liveClasses: (() => {
                if (routing.liveClasses === undefined) return [];
                if (!Array.isArray(routing.liveClasses) || !routing.liveClasses.every(value => typeof value === "string" && /^[a-z][a-z0-9:-]{0,63}$/.test(value))) {
                  throw new Error("Invalid agents.modelRouting.liveClasses");
                }
                return [...new Set(routing.liveClasses as string[])];
              })(),
              revertReset: (() => {
                if (routing.revertReset === undefined) return {};
                if (!routing.revertReset || typeof routing.revertReset !== "object" || Array.isArray(routing.revertReset) ||
                  !Object.values(routing.revertReset).every(value => typeof value === "string" && value.length <= 128)) {
                  throw new Error("Invalid agents.modelRouting.revertReset");
                }
                return { ...routing.revertReset as Record<string, string> };
              })(),
              ...(routing.perCall !== undefined ? { perCall: parsePerCallRouting(routing.perCall) } : {}),
              ...(typeof routing.pinModel === "string" ? { pinModel: routing.pinModel } : {}),
              ...(isFabricThinking(routing.pinThinking) ? { pinThinking: routing.pinThinking } : {}),
              shadowCandidates: Array.isArray(routing.shadowCandidates)
                ? routing.shadowCandidates.map((entry: unknown) => {
                    if (typeof entry !== "object" || entry === null || Array.isArray(entry) ||
                        typeof (entry as Record<string, unknown>).model !== "string" || !isFabricThinking((entry as Record<string, unknown>).effort)) {
                      throw new Error("Invalid agents.modelRouting.shadowCandidates entry");
                    }
                    return { model: (entry as { model: string }).model, effort: (entry as { effort: FabricThinking }).effort };
                  })
                : [],
            };
          })() }
        : {}),
      modelExceptionsPath: MODEL_EXCEPTIONS_PATH,
      modelPolicy: {
        requireReason: [...new Set((Array.isArray(modelPolicy.requireReason)
          ? modelPolicy.requireReason as unknown[]
          : DEFAULT_FABRIC_CONFIG.agents.modelPolicy.requireReason)
          .filter((model): model is string => typeof model === "string" && !!model.trim())
          .map(model => model.trim().toLowerCase()))],
        ...(fallbackModel ? { fallbackModel } : {}),
        fallbackThinking: fallbackThinking ?? "high",
      },
      deniedModels: [...new Set((Array.isArray(agents.deniedModels) ? agents.deniedModels : [])
        .filter((model): model is string => typeof model === "string" && !!model.trim())
        .map((model) => model.trim().toLowerCase()))],
      ...(deniedModelReplacement ? { deniedModelReplacement } : {}),
      claude: {
        binary: claudeBinary ?? DEFAULT_FABRIC_CONFIG.agents.claude.binary,
        ...(claudeModel ? { model: claudeModel } : {}),
      },
      veda: {
        binary: vedaBinary ?? DEFAULT_FABRIC_CONFIG.agents.veda.binary,
        backend: vedaBackend ?? DEFAULT_FABRIC_CONFIG.agents.veda.backend,
        ...(vedaModel ? { model: vedaModel } : {}),
        persona: vedaPersona ?? DEFAULT_FABRIC_CONFIG.agents.veda.persona,
      },
      thinking: agentThinking,
      // Idempotent: a normalized config carries the key (possibly undefined), so re-normalizing keeps it
      // instead of promoting the filled-in thinking fallback to a configured value.
      configuredThinking: Object.hasOwn(agents, "configuredThinking")
        ? (isFabricThinking(agents.configuredThinking) ? agents.configuredThinking : undefined)
        : (isFabricThinking(agents.thinking) ? agents.thinking : undefined),
      maxConcurrent: boundedInteger(
        agents.maxConcurrent,
        DEFAULT_FABRIC_CONFIG.agents.maxConcurrent,
        1,
        32,
      ),
      maxPerExecution: boundedInteger(
        agents.maxPerExecution,
        DEFAULT_FABRIC_CONFIG.agents.maxPerExecution,
        1,
        1_000,
      ),
      maxDepth: boundedInteger(
        agents.maxDepth,
        DEFAULT_FABRIC_CONFIG.agents.maxDepth,
        0,
        Number.MAX_SAFE_INTEGER,
      ),
      timeoutMs: boundedInteger(
        agents.timeoutMs,
        DEFAULT_FABRIC_CONFIG.agents.timeoutMs,
        MIN_AGENT_TIMEOUT_MS,
        MAX_AGENT_TIMEOUT_MS,
      ),
      extensions: booleanValue(agents.extensions, DEFAULT_FABRIC_CONFIG.agents.extensions),
      defaultTools: configuredTools,
      retainRuns: booleanValue(agents.retainRuns, DEFAULT_FABRIC_CONFIG.agents.retainRuns),
      notifyOnComplete: booleanValue(
        agents.notifyOnComplete,
        DEFAULT_FABRIC_CONFIG.agents.notifyOnComplete,
      ),
      budgetUsd: boundedFloat(
        agents.budgetUsd,
        DEFAULT_FABRIC_CONFIG.agents.budgetUsd,
        0,
        1_000_000,
      ),
      maxTokensPerChild: boundedInteger(
        agents.maxTokensPerChild,
        DEFAULT_FABRIC_CONFIG.agents.maxTokensPerChild,
        0,
        100_000_000,
      ),
      sessionExport: booleanValue(
        agents.sessionExport,
        DEFAULT_FABRIC_CONFIG.agents.sessionExport,
      ),
      sessionExportDir:
        typeof agents.sessionExportDir === "string"
          ? agents.sessionExportDir
          : DEFAULT_FABRIC_CONFIG.agents.sessionExportDir,
      nice: boundedInteger(agents.nice, DEFAULT_FABRIC_CONFIG.agents.nice, 0, 19),
      deadRootFilter: normalizeDeadRootFilterConfig(agents.deadRootFilter),
      stuckPreparingMs: boundedInteger(agents.stuckPreparingMs, DEFAULT_FABRIC_CONFIG.agents.stuckPreparingMs, 1_000, 7 * 24 * 60 * 60_000),
      ...(stringValue(agents.instructionsRoot)?.trim() ? { instructionsRoot: stringValue(agents.instructionsRoot)!.trim() } : {}),
      ...(wakeText ? { wakeText } : {}),
      modelAdmission: modelAdmissionValue(
        agents.modelAdmission,
        DEFAULT_FABRIC_CONFIG.agents.modelAdmission,
      ),
      ...(stringValue(objectValue(agents.worktree).setup)
        ? { worktree: { setup: String(objectValue(agents.worktree).setup).trim() } }
        : {}),
      ...(agents.childQuestions === "route" ? { childQuestions: "route" as const } : {}),
      ...(agents.childQuestionTimeoutMs === undefined ? {} : {
        childQuestionTimeoutMs: boundedInteger(agents.childQuestionTimeoutMs, 600_000, 1_000, 86_400_000),
      }),
    },
    jev: normalizeJevConfig(input.jev),
    records: normalizeRecordsConfig(input.records),
    components: configuredComponents.map((entry) => structuredClone(entry)),
    capture: {
      enabled: booleanValue(capture.enabled, DEFAULT_FABRIC_CONFIG.capture.enabled),
      hideFromModel: booleanValue(
        capture.hideFromModel,
        DEFAULT_FABRIC_CONFIG.capture.hideFromModel,
      ),
      keepVisible: [...new Set(configuredVisible)],
      defaultRisk: riskValue(capture.defaultRisk, DEFAULT_FABRIC_CONFIG.capture.defaultRisk),
      risks,
    },
    ui: {
      enabled: booleanValue(ui.enabled, DEFAULT_FABRIC_CONFIG.ui.enabled),
      widget: widgetModeValue(ui.widget, DEFAULT_FABRIC_CONFIG.ui.widget),
      maxRows: boundedInteger(ui.maxRows, DEFAULT_FABRIC_CONFIG.ui.maxRows, 1, 20),
      refreshMs: boundedInteger(ui.refreshMs, DEFAULT_FABRIC_CONFIG.ui.refreshMs, 100, 10_000),
      eventHistory: boundedInteger(
        ui.eventHistory,
        DEFAULT_FABRIC_CONFIG.ui.eventHistory,
        1,
        500,
      ),
      haltOnEscape: booleanValue(ui.haltOnEscape, DEFAULT_FABRIC_CONFIG.ui.haltOnEscape),
      // Renamed from ui.showNestedToolCalls; the v2 migration rewrites persisted
      // files, and this fallback covers configs normalized without migration.
      showAgentToolPreview: booleanValue(
        ui.showAgentToolPreview ?? ui.showNestedToolCalls,
        DEFAULT_FABRIC_CONFIG.ui.showAgentToolPreview,
      ),
      incomingMessages: ui.incomingMessages === "collapsed" || ui.incomingMessages === "expanded"
        ? ui.incomingMessages : "auto",
      principalView: principalViewModeValue(ui),
      toolDisplay: toolDisplayModeValue(
        ui.toolDisplay,
        DEFAULT_FABRIC_CONFIG.ui.toolDisplay,
      ),
      // Renamed from ui.nestedToolDebounceMs (v3): the window coalesces every
      // live fabric_exec card update — nested calls, progress, agent previews.
      updateDebounceMs: boundedInteger(
        ui.updateDebounceMs ?? ui.nestedToolDebounceMs,
        DEFAULT_FABRIC_CONFIG.ui.updateDebounceMs,
        0,
        2_000,
      ),
    },
    compaction: {
      engine: compactionEngineValue(compaction.engine, DEFAULT_FABRIC_CONFIG.compaction.engine),
      targetContextRatio: boundedFloat(
        compaction.targetContextRatio,
        DEFAULT_FABRIC_CONFIG.compaction.targetContextRatio,
        0.25,
        0.85,
      ),
      thresholds: compactionThresholds,
      tokenThresholds: compactionTokenThresholds,
      pressureBands: compactionPressureBandsValue(
        compaction.pressureBands,
        DEFAULT_FABRIC_CONFIG.compaction.pressureBands,
      ),
      outputReserveTokens: boundedInteger(
        compaction.outputReserveTokens,
        DEFAULT_FABRIC_CONFIG.compaction.outputReserveTokens,
        0,
        MAX_COMPACTION_TOKEN_THRESHOLD,
      ),
      repairOrphans: booleanValue(
        compaction.repairOrphans,
        DEFAULT_FABRIC_CONFIG.compaction.repairOrphans,
      ),
    },
    retention: {
      legacyRunArchiveEnabled: booleanValue(retention.legacyRunArchiveEnabled, DEFAULT_FABRIC_CONFIG.retention.legacyRunArchiveEnabled ?? true),
      legacyRunArchiveAgeMs: boundedInteger(retention.legacyRunArchiveAgeMs,
        DEFAULT_FABRIC_CONFIG.retention.legacyRunArchiveAgeMs ?? 48 * 60 * 60 * 1_000,
        60 * 60 * 1_000, 365 * 24 * 60 * 60 * 1_000),
      orphanedTempRunMs: boundedInteger(
        retention.orphanedTempRunMs,
        DEFAULT_FABRIC_CONFIG.retention.orphanedTempRunMs,
        60 * 60 * 1_000,
        365 * 24 * 60 * 60 * 1_000,
      ),
      oneShotRunMs: boundedInteger(
        retention.oneShotRunMs,
        DEFAULT_FABRIC_CONFIG.retention.oneShotRunMs,
        60 * 60 * 1_000,
        365 * 24 * 60 * 60 * 1_000,
      ),
      actorRunArchiveMs: boundedInteger(
        retention.actorRunArchiveMs,
        DEFAULT_FABRIC_CONFIG.retention.actorRunArchiveMs,
        60 * 60 * 1_000,
        365 * 24 * 60 * 60 * 1_000,
      ),
      terminalRunEventsAgeMs: boundedInteger(
        retention.terminalRunEventsAgeMs,
        DEFAULT_FABRIC_CONFIG.retention.terminalRunEventsAgeMs,
        60 * 60 * 1_000,
        365 * 24 * 60 * 60 * 1_000,
      ),
      terminalRunEventsMaxBytes: boundedInteger(
        retention.terminalRunEventsMaxBytes,
        DEFAULT_FABRIC_CONFIG.retention.terminalRunEventsMaxBytes,
        1024,
        16 * 1024 * 1024,
      ),
    },
    actors: {
      maxSessionBytes: boundedInteger(
        actors.maxSessionBytes,
        DEFAULT_FABRIC_CONFIG.actors.maxSessionBytes,
        0,
        Number.MAX_SAFE_INTEGER,
      ),
      transientBlockCooldownMs: boundedInteger(
        actors.transientBlockCooldownMs,
        DEFAULT_FABRIC_CONFIG.actors.transientBlockCooldownMs!,
        1_000,
        24 * 60 * 60_000,
      ),
      transientBlockMaxMs: boundedInteger(
        actors.transientBlockMaxMs,
        DEFAULT_FABRIC_CONFIG.actors.transientBlockMaxMs!,
        1_000,
        7 * 24 * 60 * 60_000,
      ),
    },
    mesh: {
      controlIncarnationFence: meshControlIncarnationFence(mesh.controlIncarnationFence),
      lockProtocol: meshLockProtocol(mesh.lockProtocol),
      stateBackend: meshStateBackend(mesh.stateBackend),
      transport: carrier,
      ...(hostTransport !== undefined ? { hostTransport } : {}),
      nats: meshNatsConfig(mesh.nats, carrier),
      enabled: booleanValue(mesh.enabled, DEFAULT_FABRIC_CONFIG.mesh.enabled),
      ...(meshRoot ? { root: meshRoot } : {}),
      announce: booleanValue(mesh.announce, DEFAULT_FABRIC_CONFIG.mesh.announce),
      actorScope: actorScopeValue(mesh.actorScope, DEFAULT_FABRIC_CONFIG.mesh.actorScope),
      maxEventBytes: boundedInteger(
        mesh.maxEventBytes,
        DEFAULT_FABRIC_CONFIG.mesh.maxEventBytes,
        1_024,
        4 * 1024 * 1024,
      ),
      maxReadEvents: boundedInteger(
        mesh.maxReadEvents,
        DEFAULT_FABRIC_CONFIG.mesh.maxReadEvents,
        1,
        10_000,
      ),
      actorPollMs: boundedInteger(
        mesh.actorPollMs,
        DEFAULT_FABRIC_CONFIG.mesh.actorPollMs,
        50,
        10_000,
      ),
      residentIdleExitMs: boundedInteger(
        mesh.residentIdleExitMs, DEFAULT_FABRIC_CONFIG.mesh.residentIdleExitMs, 0, 7 * 24 * 60 * 60_000,
      ),
      idleReadCoalesceMs: boundedInteger(
        mesh.idleReadCoalesceMs,
        DEFAULT_FABRIC_CONFIG.mesh.idleReadCoalesceMs,
        0,
        10_000,
      ),
      bridgeControlTimeoutMs: boundedInteger(
        mesh.bridgeControlTimeoutMs,
        DEFAULT_FABRIC_CONFIG.mesh.bridgeControlTimeoutMs,
        30_000,
        300_000,
      ),
      actorQueueLimit: boundedInteger(
        mesh.actorQueueLimit,
        DEFAULT_FABRIC_CONFIG.mesh.actorQueueLimit,
        1,
        1_000,
      ),
      eventContextChars: boundedInteger(
        mesh.eventContextChars,
        DEFAULT_FABRIC_CONFIG.mesh.eventContextChars,
        1_000,
        1_000_000,
      ),
      actorContextEntries: boundedInteger(
        mesh.actorContextEntries,
        DEFAULT_FABRIC_CONFIG.mesh.actorContextEntries,
        1,
        100,
      ),
      followUpFlushMs: boundedInteger(
        mesh.followUpFlushMs,
        DEFAULT_FABRIC_CONFIG.mesh.followUpFlushMs,
        0,
        24 * 60 * 60_000,
      ),
      followUpStallSeconds: boundedInteger(
        mesh.followUpStallSeconds,
        DEFAULT_FABRIC_CONFIG.mesh.followUpStallSeconds,
        0,
        7 * 24 * 60 * 60,
      ),
      rootPresenceAlarmMs: boundedInteger(mesh.rootPresenceAlarmMs, DEFAULT_FABRIC_CONFIG.mesh.rootPresenceAlarmMs, 1, 7 * 24 * 60 * 60_000),
      undeliveredAlarmMs: boundedInteger(mesh.undeliveredAlarmMs, DEFAULT_FABRIC_CONFIG.mesh.undeliveredAlarmMs, 1, 7 * 24 * 60 * 60_000),
      rootGoneTtlMs: boundedInteger(mesh.rootGoneTtlMs, DEFAULT_FABRIC_CONFIG.mesh.rootGoneTtlMs, 1, 7 * 24 * 60 * 60_000),
    },
    models: {
      aliases: normalizeModelAliases(modelsSection.aliases),
    },
    memory: {
      extractive: {
        enabled: booleanValue(extractive.enabled, false),
        provider: stringValue(extractive.provider) || DEFAULT_EXTRACTIVE_CONFIG.provider,
        model: stringValue(extractive.model) || DEFAULT_EXTRACTIVE_CONFIG.model,
        maxViewBytes: boundedInteger(extractive.maxViewBytes, 8192, 1024, 32768),
        maxCandidates: boundedInteger(extractive.maxCandidates, 128, 1, 256),
        maxSourceChars: boundedInteger(extractive.maxSourceChars, 24000, 256, 100000),
        maxEvaluationsPerTurn: boundedInteger(extractive.maxEvaluationsPerTurn, 1, 0, 1),
        timeoutMs: boundedInteger(extractive.timeoutMs, 3000, 100, 10000),
      },
      enabled: booleanValue(memory.enabled, DEFAULT_FABRIC_CONFIG.memory.enabled),
      ...(memoryIndexDir ? { indexDir: memoryIndexDir } : {}),
      ...(memorySources ? { sources: memorySources } : {}),
      maxSessions: boundedInteger(
        memory.maxSessions,
        DEFAULT_FABRIC_CONFIG.memory.maxSessions,
        1,
        100_000,
      ),
      maxEntryChars: boundedInteger(
        memory.maxEntryChars,
        DEFAULT_FABRIC_CONFIG.memory.maxEntryChars,
        100,
        1_000_000,
      ),
      indexThinking: booleanValue(
        memory.indexThinking,
        DEFAULT_FABRIC_CONFIG.memory.indexThinking ?? false,
      ),
      indexToolOutput: booleanValue(
        memory.indexToolOutput,
        DEFAULT_FABRIC_CONFIG.memory.indexToolOutput ?? true,
      ),
      hotSessions: boundedInteger(
        memory.hotSessions,
        DEFAULT_FABRIC_CONFIG.memory.hotSessions ?? 50,
        0,
        100_000,
      ),
      digestTerms: boundedInteger(
        memory.digestTerms,
        DEFAULT_FABRIC_CONFIG.memory.digestTerms ?? 200,
        1,
        10_000,
      ),
      maxColdVocabularyBytes: boundedInteger(
        memory.maxColdVocabularyBytes,
        DEFAULT_FABRIC_CONFIG.memory.maxColdVocabularyBytes ?? 512 * 1024,
        2,
        64 * 1024 * 1024,
      ),
      maxColdCacheBytes: boundedInteger(
        memory.maxColdCacheBytes,
        DEFAULT_FABRIC_CONFIG.memory.maxColdCacheBytes ?? 1024 * 1024,
        512,
        128 * 1024 * 1024,
      ),
      maxSyncSessions: boundedInteger(
        memory.maxSyncSessions,
        DEFAULT_FABRIC_CONFIG.memory.maxSyncSessions ?? 10_000,
        1,
        1_000_000,
      ),
      maxSyncSourceBytes: boundedInteger(
        memory.maxSyncSourceBytes,
        DEFAULT_FABRIC_CONFIG.memory.maxSyncSourceBytes ?? 512 * 1024 * 1024,
        1_024,
        8 * 1024 * 1024 * 1024,
      ),
      maxCacheCleanupFiles: boundedInteger(
        memory.maxCacheCleanupFiles,
        DEFAULT_FABRIC_CONFIG.memory.maxCacheCleanupFiles ?? 100_000,
        1,
        1_000_000,
      ),
      regexMaxPatternBytes: boundedInteger(
        memory.regexMaxPatternBytes,
        DEFAULT_FABRIC_CONFIG.memory.regexMaxPatternBytes ?? 1_024,
        1,
        64 * 1024,
      ),
      regexMaxHaystackTerms: boundedInteger(
        memory.regexMaxHaystackTerms,
        DEFAULT_FABRIC_CONFIG.memory.regexMaxHaystackTerms ?? 20_000,
        1,
        1_000_000,
      ),
      regexMaxHaystackBytes: boundedInteger(
        memory.regexMaxHaystackBytes,
        DEFAULT_FABRIC_CONFIG.memory.regexMaxHaystackBytes ?? 2 * 1024 * 1024,
        1_024,
        128 * 1024 * 1024,
      ),
      regexTimeoutMs: boundedInteger(
        memory.regexTimeoutMs,
        DEFAULT_FABRIC_CONFIG.memory.regexTimeoutMs ?? 250,
        10,
        10_000,
      ),
    },
    entropy: {
      compile: booleanValue(entropy.compile, DEFAULT_FABRIC_CONFIG.entropy.compile),
    },
    repairs: {
      enabled: booleanValue(repairs.enabled, DEFAULT_FABRIC_CONFIG.repairs.enabled),
    },
    trace: {
      assessment: booleanValue(trace.assessment, DEFAULT_FABRIC_CONFIG.trace.assessment),
    },
    schema: {
      mode: schemaMode,
      certificateTtlMs: boundedInteger(
        schema.certificateTtlMs,
        DEFAULT_FABRIC_CONFIG.schema.certificateTtlMs,
        1_000,
        10 * 60_000,
      ),
      maxFiles: boundedInteger(
        schema.maxFiles,
        DEFAULT_FABRIC_CONFIG.schema.maxFiles,
        1,
        1_000,
      ),
      maxBytes: boundedInteger(
        schema.maxBytes,
        DEFAULT_FABRIC_CONFIG.schema.maxBytes,
        1_024,
        100 * 1024 * 1024,
      ),
      trustedCommands,
    },
    speculation: {
      enabled: booleanValue(
        speculation.enabled,
        DEFAULT_FABRIC_CONFIG.speculation.enabled,
      ),
      maxConcurrent: boundedInteger(
        speculation.maxConcurrent,
        DEFAULT_FABRIC_CONFIG.speculation.maxConcurrent,
        1,
        32,
      ),
      maxEntries: boundedInteger(
        speculation.maxEntries,
        DEFAULT_FABRIC_CONFIG.speculation.maxEntries,
        1,
        1_024,
      ),
      maxBufferBytes: boundedInteger(
        speculation.maxBufferBytes,
        DEFAULT_FABRIC_CONFIG.speculation.maxBufferBytes,
        64 * 1024,
        64 * 1024 * 1024,
      ),
      entryTtlMs: boundedInteger(
        speculation.entryTtlMs,
        DEFAULT_FABRIC_CONFIG.speculation.entryTtlMs,
        5_000,
        30 * 60_000,
      ),
      mcpAllowlist: [
        ...new Set(
          (Array.isArray(speculation.mcpAllowlist) ? speculation.mcpAllowlist : [])
            .filter(
            (entry): entry is string =>
              typeof entry === "string" && entry.trim().length > 0,
          )
            .map((entry) => entry.trim().slice(0, 256)),
        ),
      ].slice(0, 256),
    },
    thinking: {
      // Bounds are a ceiling policy: malformed values fail closed.
      bounds: normalizeThinkingBounds(objectValue(input.thinking).bounds, "thinking.bounds"),
    },
    // Malformed entries fail config loading; semantic refusals report at runtime.
    foreground: foregroundConfigValue(input.foreground),
    codePreview: normalizeCodePreviewSettings(input.codePreview),
  };
};

export const effectiveToolCaptureConfig = (
  config: Pick<FabricConfig, "fullCodeMode" | "capture"> & Partial<Pick<FabricConfig, "schema">>,
): FabricToolCaptureConfig =>
  config.schema?.mode === "enforce"
    ? {
        ...config.capture,
        enabled: true,
        hideFromModel: true,
        keepVisible: ["fabric_exec"],
        risks: { ...config.capture.risks },
      }
    : config.fullCodeMode
      ? {
          ...config.capture,
          keepVisible: config.capture.keepVisible.filter(
            (name) => !PI_CORE_TOOL_NAME_SET.has(name),
          ),
          risks: { ...config.capture.risks },
        }
      : {
          ...config.capture,
          enabled: false,
          hideFromModel: false,
          keepVisible: [...config.capture.keepVisible],
          risks: { ...config.capture.risks },
        };

interface FabricConfigFilePlan {
  path: string;
  document: Record<string, unknown>;
  source: string;
  changed: boolean;
}

const planConfigFile = (filePath: string): FabricConfigFilePlan | undefined => {
  const input = readJsonObjectFile(filePath);
  if (!input) return undefined;
  const migration = migrateFabricConfigDocument(input.document);
  return {
    path: filePath,
    document: migration.document,
    source: input.source,
    changed: migration.changed,
  };
};

export const writeJsonAtomic = (
  filePath: string,
  document: Record<string, unknown>,
  expectedSource: string | null,
): void => {
  const resolvedPath = fs.existsSync(filePath) ? fs.realpathSync(filePath) : filePath;
  const directory = path.dirname(resolvedPath);
  if (!fs.existsSync(directory)) fs.mkdirSync(directory, { recursive: true });
  const mode = fs.existsSync(resolvedPath) ? fs.statSync(resolvedPath).mode & 0o777 : 0o600;
  const temporaryPath = path.join(
    directory,
    `.${path.basename(resolvedPath)}.${process.pid}.${Date.now()}.${Math.random().toString(16).slice(2)}.tmp`,
  );
  let descriptor: number | undefined;
  try {
    descriptor = fs.openSync(temporaryPath, "wx", mode);
    fs.writeFileSync(descriptor, `${JSON.stringify(document, null, 2)}\n`, "utf8");
    fs.fsyncSync(descriptor);
    fs.closeSync(descriptor);
    descriptor = undefined;
    if (expectedSource === null) {
      if (fs.existsSync(resolvedPath)) {
        throw new Error(`Fabric configuration changed while updating ${filePath}`);
      }
    } else {
      let currentSource: string;
      try {
        currentSource = fs.readFileSync(resolvedPath, "utf8");
      } catch (error) {
        throw new Error(`Fabric configuration changed while updating ${filePath}`, { cause: error });
      }
      if (currentSource !== expectedSource) {
        throw new Error(`Fabric configuration changed while updating ${filePath}`);
      }
    }
    renameAtomic(temporaryPath, resolvedPath);
    try {
      const directoryDescriptor = fs.openSync(directory, "r");
      try {
        fs.fsyncSync(directoryDescriptor);
      } finally {
        fs.closeSync(directoryDescriptor);
      }
    } catch (error) {
      const code = error instanceof Error && "code" in error ? error.code : undefined;
      if (code !== "EINVAL" && code !== "ENOTSUP" && code !== "EISDIR" && code !== "EPERM") throw error;
    }
  } catch (error) {
    if (descriptor !== undefined) fs.closeSync(descriptor);
    fs.rmSync(temporaryPath, { force: true });
    throw error;
  }
};

// Missing policy preserves rollout defaults; damaged/unprovable policy must fail closed.
const hostLandlockBaseline = (policy: HostPolicy): LandlockSettings => {
  const defaults = DEFAULT_FABRIC_CONFIG.executor.landlock;
  if (policy.status === "missing") return { ...defaults };
  if (policy.status === "invalid") return { mode: "enforce", disabled: false };
  const landlock = objectValue(objectValue(policy.document.executor).landlock);
  return {
    mode: landlock.mode === "off" || landlock.mode === "enforce" ? landlock.mode : defaults.mode,
    disabled: typeof landlock.disabled === "boolean" ? landlock.disabled : defaults.disabled,
    ...(landlock.allowEscape === true ? { allowEscape: true } : {}),
  };
};

const resolveFabricConfig = (
  options: {
    cwd: string;
    agentDir: string;
  },
  includeProject: boolean,
  applyEnvironmentOverrides: boolean,
): FabricConfig => {
  let merged = structuredClone(DEFAULT_FABRIC_CONFIG) as unknown as Record<string, unknown>;
  // Normalize supplies the fallback; retain whether a persisted layer sets effort.
  delete objectValue(merged.agents).thinking;
  const policy = readHostPolicy();
  const rootPolicyValid = policy.status === "valid";
  // Invalid/unprovable policy must tighten even when there is no writable config.
  merged = mergeObjects(merged, { executor: { landlock: hostLandlockBaseline(policy) } });
  if (rootPolicyValid) {
    // Whitelist authority keys: root policy is not a third general settings layer.
    const agents = objectValue(policy.document.agents);
    const requireReason = objectValue(agents.modelPolicy).requireReason;
    merged = mergeObjects(merged, {
      agents: {
        ...(Object.hasOwn(agents, "processSlice") ? { processSlice: agents.processSlice } : {}),
        ...(Object.hasOwn(agents, "processMemoryMaxBytes") ? { processMemoryMaxBytes: agents.processMemoryMaxBytes } : {}),
        ...(Object.hasOwn(agents, "sandbox") ? { sandbox: agents.sandbox } : {}),
        ...(Array.isArray(agents.deniedModels) ? { deniedModels: agents.deniedModels } : {}),
        ...(Array.isArray(requireReason) ? { modelPolicy: { requireReason } } : {}),
      },
    });
  }
  const hostPlan = planConfigFile(path.join(options.agentDir, "fabric.json"));
  const projectPlan = includeProject ? planConfigFile(path.join(options.cwd, ".pi", "fabric.json")) : undefined;
  for (const plan of [hostPlan, projectPlan]) {
    if (!plan) continue;
    if (plan.changed) writeJsonAtomic(plan.path, plan.document, plan.source);
    const document = { ...plan.document };
    const ui = objectValue(document.ui);
    // Translate each persisted layer before merging with defaults, so legacy
    // project preferences still override global ones without rewriting files.
    if (ui.principalView === undefined && ui.incomingMessages !== undefined) {
      document.ui = { ...ui, principalView: principalViewModeValue(ui) };
    }
    if (plan === hostPlan) {
      const executor = { ...objectValue(document.executor) };
      const landlock = { ...objectValue(executor.landlock) };
      if (landlock.disabled === true && rootPolicyValid) warnAgentLandlockDisabled();
      // Agent settings may only tighten the root/default baseline. Ignore
      // off/unknown modes even when no root policy is provisioned.
      if (landlock.mode !== "enforce") delete landlock.mode;
      delete landlock.disabled;
      delete landlock.allowEscape;
      executor.landlock = landlock;
      document.executor = executor;
      const agents = { ...objectValue(document.agents) };
      delete agents.processSlice;
      delete agents.processMemoryMaxBytes; // Root-policy-only, like processSlice.
      delete agents.sandbox; // Root-policy-only: only the host policy can enable island execution.
      const inheritedAgents = objectValue(merged.agents);
      // Agent-writable lists can add restrictions, never remove a root/default gate.
      const union = (base: unknown, extra: unknown): unknown[] => [
        ...(Array.isArray(base) ? base : []), ...(Array.isArray(extra) ? extra : []),
      ];
      agents.deniedModels = union(inheritedAgents.deniedModels, agents.deniedModels);
      agents.modelPolicy = {
        ...objectValue(agents.modelPolicy),
        requireReason: union(objectValue(inheritedAgents.modelPolicy).requireReason,
          objectValue(agents.modelPolicy).requireReason),
      };
      document.agents = agents;
    }
    if (plan === projectPlan) {
      const agents = { ...objectValue(document.agents) };
      delete agents.router; // Host-only argv execution and task disclosure policy.
      delete agents.modelPolicy;
      delete agents.deniedModels;
      delete agents.deniedModelReplacement;
      delete agents.instructionsRoot;
      delete agents.wakeText;
      delete agents.processSlice;
      delete agents.processMemoryMaxBytes;
      delete agents.placement;
      delete agents.sandbox; // Host-only: a lane cannot enable island execution.
      delete agents.placementApi; // Host-only Placement endpoint and timeouts.
      delete agents.deadRootFilter; // Host-only: a lane cannot drop the fleet's exemptions.
      delete agents.stuckPreparingMs; // Host-only: a lane cannot silence the stuck-preparing owner notice.
      document.agents = agents;
      const mesh = { ...objectValue(document.mesh) };
      delete mesh.controlIncarnationFence; // Host-only: a workspace cannot weaken the fence.
      delete mesh.hostTransport; // A workspace cannot replace or clear the host's rollout stamp.
      const nats = { ...objectValue(mesh.nats) };
      delete nats.ephemeralHost; // Host-only: a workspace cannot retain its own inbox durable.
      delete nats.ephemeralInactiveSeconds;
      delete nats.inboxMaxAgeMs; // Host-only: a workspace cannot alter shared inbox stream retention.
      delete nats.eventOrgs; // Host-only: a workspace cannot widen event credential scopes.
      delete nats.eventReceiptLimit; // Host-only: a workspace cannot raise the actor's durable receipt-state bound.
      mesh.nats = nats;
      document.mesh = mesh;
      const executor = { ...objectValue(document.executor) };
      const landlock = { ...objectValue(executor.landlock) };
      delete landlock.disabled; // Host-only fleet kill switch wins over lane config.
      delete landlock.allowEscape; // Only root policy can grant a per-command escape.
      if (landlock.mode !== "enforce") delete landlock.mode;
      executor.landlock = landlock;
      // Host-only: a workspace cannot opt into unguarded PowerShell (smarty-dev#9826).
      delete executor.powershell;
      document.executor = executor;
      if (Object.hasOwn(document, "jev")) {
        // Host-only: a lane cannot redirect Jev questions to another socket.
        const jev = { ...objectValue(document.jev) };
        delete jev.gatewaySocket;
        document.jev = jev;
      }
    }
    merged = mergeObjects(merged, document);
  }
  const inheritedKernel = process.env.PI_FABRIC_KERNEL;
  if (applyEnvironmentOverrides && inheritedKernel !== undefined) {
    // This can select trusted native execution: never repair an unsafe selector.
    if (inheritedKernel !== "typescript" && inheritedKernel !== "python") {
      throw new Error(`Invalid PI_FABRIC_KERNEL: ${inheritedKernel}; expected typescript or python`);
    }
    merged.executor = { ...objectValue(merged.executor), kernel: inheritedKernel };
  }
  const inheritedPythonRuntime = process.env.PI_FABRIC_PYTHON_RUNTIME;
  if (applyEnvironmentOverrides && inheritedPythonRuntime !== undefined) {
    if (inheritedPythonRuntime !== "cpython" && inheritedPythonRuntime !== "monty") {
      throw new Error(`Invalid PI_FABRIC_PYTHON_RUNTIME: ${inheritedPythonRuntime}; expected cpython or monty`);
    }
    merged.executor = { ...objectValue(merged.executor), pythonRuntime: inheritedPythonRuntime };
  }
  const inheritedFullCodeMode = process.env.PI_FABRIC_FULL_CODE_MODE;
  if (
    applyEnvironmentOverrides &&
    (inheritedFullCodeMode === "true" || inheritedFullCodeMode === "false")
  ) {
    merged.fullCodeMode = inheritedFullCodeMode === "true";
  }
  const normalized = normalizeFabricConfig(merged);
  // Host-owned exceptions: no host/project config layer can supply or redirect them.
  normalized.agents.modelExceptionsPath = MODEL_EXCEPTIONS_PATH;
  if (applyEnvironmentOverrides && process.env[FABRIC_THINKING_BOUNDS_ENV] !== undefined) {
    // A child only narrows its own bounds by the parent's; never widens them.
    const inherited = inheritedThinkingBounds();
    if (inherited) {
      normalized.thinking.bounds = intersectThinkingBounds(inherited, normalized.thinking.bounds);
    }
  }
  return normalized;
};

export const loadFabricConfigForScope = (
  options: {
    cwd: string;
    agentDir: string;
    projectTrusted: boolean;
  },
  scope: FabricConfigScope,
): FabricConfig => {
  if (scope === "project" && !options.projectTrusted) {
    throw new Error("Cannot load project Fabric configuration for an untrusted project");
  }
  return resolveFabricConfig(options, scope === "project", false);
};

export const loadFabricConfig = (options: {
  cwd: string;
  agentDir: string;
  projectTrusted: boolean;
}): FabricConfig => {
  const config = resolveFabricConfig(options, options.projectTrusted, true);
  if (config.compaction.engine === "fabric") {
    process.env.PI_FABRIC_COMPACTION_ENGINE = "fabric";
  } else {
    delete process.env.PI_FABRIC_COMPACTION_ENGINE;
  }
  return config;
};

// The global configuration plus environment overrides, readable at extension load before a
// session context exists (smarty-dev#459). Project configuration needs the trust decision
// that only bootstrap has, so it is left out here.
/** Live root-owned kill switch. No valid policy fails closed. */
export const readHostLandlockDisabled = (_agentDir: string): boolean =>
  hostLandlockBaseline(readHostPolicy()).disabled;

/** Session settings with live root-only grants applied (F2: no cached value). */
export const liveLandlockSettings = (settings: LandlockSettings, _agentDir: string): LandlockSettings => {
  const baseline = hostLandlockBaseline(readHostPolicy());
  return {
    // Preserve session tightening and honor live enforcement. Only genuinely
    // missing policy uses the product default; unprovable policy enforces.
    mode: settings.mode === "enforce" ? "enforce" : baseline.mode,
    disabled: baseline.disabled,
    ...(baseline.allowEscape === true ? { allowEscape: true } : {}),
  };
};

export const loadGlobalFabricConfig = (agentDir: string): FabricConfig =>
  resolveFabricConfig({ cwd: agentDir, agentDir }, false, true);

export const saveFabricConfig = (
  options: {
    cwd: string;
    agentDir: string;
    projectTrusted: boolean;
    scope?: FabricConfigScope;
  },
  partial: Record<string, unknown>,
): { scope: FabricConfigScope; path: string } => {
  const scope = options.scope ?? (options.projectTrusted ? "project" : "global");
  if (scope === "project" && !options.projectTrusted) {
    throw new Error("Cannot save project Fabric configuration for an untrusted project");
  }
  const targetPath = scope === "project"
    ? path.join(options.cwd, ".pi", "fabric.json")
    : path.join(options.agentDir, "fabric.json");
  if (Object.hasOwn(partial, "configVersion") || Object.hasOwn(partial, "subagents")) {
    throw new Error("Fabric configuration updates must use the current schema");
  }
  const input = readJsonObjectFile(targetPath);
  const existing = migrateFabricConfigDocument(input?.document ?? {}).document;
  const merged = mergeObjects(existing, partial) as Record<string, unknown>;
  // Reject invalid ownership before the settings UI replaces a working file.
  // Do not normalize the whole document: saved layers must remain sparse.
  nativeMcpServersValue(objectValue(merged.mcp).nativeServers);
  approvalActionOverridesValue(objectValue(merged.approvals).actions);
  foregroundConfigValue(merged.foreground);
  // Never stamp down: preserve version markers written by newer builds.
  merged.configVersion = Math.max(
    typeof merged.configVersion === "number" ? merged.configVersion : 0,
    CURRENT_FABRIC_CONFIG_VERSION,
  );
  writeJsonAtomic(targetPath, merged, input?.source ?? null);
  return { scope, path: targetPath };
};
