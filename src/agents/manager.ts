import { copyFabricPrincipal, type FabricPrincipal, type FabricTurnProvenance } from "../fabric-provenance.js";
import { randomUUID } from "node:crypto";
import { taskReturnAddressArguments, type TaskReturnAddress } from "./task-return-address.js";
import { AgentWaitBoundError, describeWaitBound } from "./wait-bound.js";
import type { FabricKernel } from "../runtime/kernel.js";
import fs from "node:fs";
import { spawn } from "node:child_process";
import os from "node:os";
import { fabricDataRoot } from "../storage/temp-root.js";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { assertFabricModelAllowed, FabricModelDeniedError } from "../core/model-policy.js";
import { readChildToolAllowlist } from "../core/child-tool-allowlist.js";
import { writeJsonAtomic } from "../core/atomic-write.js";
import { discardWorkerCompletion, type CompletionRecipient } from "./completion-journal.js";
import { processStartTime } from "../residency/process-identity.js";
import {
  DEFAULT_FABRIC_CONFIG,
  MAX_AGENT_TIMEOUT_MS,
  MIN_AGENT_TIMEOUT_MS,
  type FabricAgentRunner,
  type FabricAgentConfig,
  type FabricAgentTransport,
  type FabricRetentionConfig,
  type FabricPythonRuntime,
} from "../config.js";
import {
  discoverClaudeModels,
  mapClaudeTools,
  normalizeClaudeModel,
  type ClaudeModelInfo,
} from "./claude-cli.js";
import { mapVedaTools, normalizeVedaModel } from "./veda-cli.js";
import { resolvePiBinary } from "./pi-binary.js";
import {
  inheritedSessionPinsFromEnv,
  serializeInheritedSessionPins,
  type InheritedSessionPin,
} from "./session-pins.js";
import { tokenUsagePayloadFromValue } from "../lifecycle/types.js";
import type { FabricTokenUsagePayload } from "../lifecycle/types.js";
import { AgentAdmission, assertAgentTask, beginAgentSettlement, createAgentLifecycle, finishAgentSettlement, terminalAgentStatuses, type AgentLifecycleState } from "./lifecycle.js";
import { removeTree } from "./rm.js";
import { HerdrTransport } from "./transports/herdr-transport.js";
import { LocaltermTransport } from "./transports/localterm-transport.js";
import { ProcessTransport } from "./transports/process-transport.js";
import { scriptSpawnArgs } from "./transports/process-utils.js";
import { ScreenTransport } from "./transports/screen-transport.js";
import { TmuxTransport } from "./transports/tmux-transport.js";
import { resolveAgentSpawner } from "./spawner.js";
import type {
  AgentSpawner,
  FabricBudgetSummary,
  FabricSteeringMode,
  FabricAgentLog,
  AgentHandleInfo,
  AgentRunCarryOver,
  AgentRunRecord,
  AgentRunRequest,
  AgentRunResult,
  AgentSteerEntry,
  AgentSteerResult,
  AgentTransportAdapter,
  AgentTransportHandle,
  AgentTransportLaunch,
  AgentUsage,
} from "./types.js";
import { FOLLOW_UP_RUNNING_TASK_MESSAGE, type AgentFollowUpAlarm, type AgentFollowUpDelivery } from "./types.js";
import { followUpFile, followUpState, settleFollowUp, releaseFollowUpPayload } from "./follow-up-delivery.js";
import { createRunRouteMetadata } from "../worker/run-record.js";
import type { AgentRunRouteMetadata } from "./types.js";
import { WorktreeManager } from "./worktree-manager.js";
import { writeHandoffSession } from "./handoff.js";
import type { FabricCompactionBudget } from "../compaction/hook.js";
import {
  activeBudgetState,
  appendBudgetLedger,
  clearOwnedBudgetEnv,
  initBudgetLedger,
  readBudgetLedger,
  readBudgetLedgerDetailed,
} from "./budget-ledger.js";
import type { BudgetLedgerDetail } from "./budget-ledger.js";
import type { BudgetLedgerState } from "./budget-ledger.js";
import { readJsonlPage } from "../log-tail.js";
import { ownedStat, processAlive } from "../storage/scratch.js";
import {
  canRemoveManagedRunRoot,
  canRemoveTerminalRun,
  hasUnresolvedWorker,
  runTreeExitVeto,
  markUnresolvedWorker,
  heartbeatRunRoot,
  markRunRootActive,
  markRunRootClosed,
  claimTempRunSweep,
  removeEmptyRunRoot,
  type TempRunSweepRequest,
} from "../storage/retention.js";
import { resolveSessionExportDir, sessionExportFileFor } from "./session-export.js";
import { effectiveAgentNice, parseAgentNice } from "./priority.js";
import {
  isFabricLifecycleEventType,
  type FabricLifecycleEventType,
  type FabricLifecyclePublishRequest,
} from "../lifecycle/types.js";
import {
  AGENT_RESUME_MAX_ATTEMPTS,
  AGENT_RESUME_RETRY_BASE_DELAY_MS,
  AGENT_STARTUP_MAX_ATTEMPTS,
  AGENT_STARTUP_RETRY_BASE_DELAY_MS,
  AGENT_STATUS_POLL_INTERVAL_MS,
} from "./constants.js";
const NESTED_SNAPSHOT_POLL_MS = 500;
const TRANSPORT_EXIT_GRACE_MS = 1_000;
// These adapters conflate CLI/socket errors with absent sessions and have no
// checked exit receipt. Scope-cut release/collection rather than trust false.
const uncheckedExternalExit = (transport: Pick<AgentTransportHandle, "kind">): boolean =>
  transport.kind === "tmux" || transport.kind === "screen";
const MAX_NAME_LENGTH = 60;
const MAX_UI_TEXT_CHARS = 16_000;
const MAX_UI_ERROR_CHARS = 8_000;
const MAX_UI_VALUE_CHARS = 64_000;
const MAX_RETAINED_UI_RUNS = 240;
const MAX_RETAINED_RUN_HANDLES = 1_000;
const MAX_LOG_SUMMARY_CHARS = 7_000;
const MAX_LOG_DETAIL_CHARS = 900;
const RETENTION_SWEEP_INTERVAL_MS = 15 * 60 * 1_000;

export const effectiveAgentTimeoutMs = (
  configuredTimeoutMs: number,
  requestedTimeoutMs?: number,
): number => {
  const configured = Math.max(
    MIN_AGENT_TIMEOUT_MS,
    Math.min(Math.floor(configuredTimeoutMs), MAX_AGENT_TIMEOUT_MS),
  );
  if (requestedTimeoutMs === undefined || !Number.isFinite(requestedTimeoutMs)) {
    return configured;
  }
  return Math.max(
    configured,
    Math.min(Math.floor(requestedTimeoutMs), MAX_AGENT_TIMEOUT_MS),
  );
};

/** Host-only actor setup budget. Starts after admission and ends before transport launch. */
export interface AgentLaunchPreparationOptions {
  timeoutMs: number;
  onPreparing?: () => void;
}

export class AgentLaunchPreparationTimeoutError extends Error {
  /** This deadline races model/auth only, never transport launch or worker execution. */
  readonly launchOutcome = "unlaunched";
  readonly code = "FABRIC_AGENT_LAUNCH_PREPARATION_TIMEOUT";
  constructor(readonly timeoutMs: number) {
    super(`Agent launch preparation (model/auth) timed out after ${timeoutMs} ms`);
    this.name = "AgentLaunchPreparationTimeoutError";
  }
}

interface AgentParticipantGuidanceRequest {
  model?: string;
  runner: FabricAgentRunner;
}

type AgentParticipantGuidanceResolver = (
  request: AgentParticipantGuidanceRequest,
) => string | undefined;

/** Resolve and validate a one-shot agent's filesystem execution directory. */
export const resolveAgentCwd = (parentCwd: string, requestedCwd?: string): string => {
  if (requestedCwd === undefined) return parentCwd;
  const requested = requestedCwd;
  if (typeof requested !== "string" || requested.trim().length === 0) {
    throw new Error(`Invalid Fabric agent cwd ${JSON.stringify(requested)}: path must not be empty`);
  }
  const candidate = path.isAbsolute(requested)
    ? requested
    : path.resolve(parentCwd, requested);
  try {
    const canonical = fs.realpathSync(candidate);
    fs.accessSync(canonical, fs.constants.R_OK | fs.constants.X_OK);
    if (!fs.statSync(canonical).isDirectory()) {
      throw new Error("path is not a directory");
    }
    return canonical;
  } catch (error) {
    const reason = error instanceof Error ? error.message : String(error);
    const missing = (error as NodeJS.ErrnoException).code === "ENOENT";
    throw new Error(
      `Invalid Fabric agent cwd ${JSON.stringify(requested)}: ${reason}` +
        (missing ? ". The cwd does not exist; if another tool call in the same message creates it, call spawn in the next message" : ""),
      { cause: error },
    );
  }
};

const AGENT_CWD_SETTLE_MS = 3_000;
const AGENT_WORKTREE_SETTLE_MS = 30_000;

const readText = (file: string): string | undefined => {
  try {
    return fs.readFileSync(file, "utf8");
  } catch {
    return undefined;
  }
};

/**
 * True while `git worktree add` is still creating the linked worktree `dir`. Git writes
 * `<gitdir>/locked` ("initializing") before it creates `dir`, holds `<gitdir>/index.lock`
 * during the checkout, and removes the lock when it finishes; a failed add removes `dir`.
 * Only git's own add-time reason counts: a `git worktree lock` with no reason (an empty file)
 * or another reason is a finished worktree. git writes "initializing" before W exists, so a
 * half-written lock is never seen beside W/.git.
 * ponytail: a localized git writes a translated reason; index.lock still covers its checkout.
 */
const gitWorktreeInitializing = (dir: string): boolean => {
  const dotGit = readText(path.join(dir, ".git"));
  const match = dotGit?.match(/^gitdir:\s*(.+?)\s*$/m);
  if (!match) return false;
  const gitdir = path.resolve(dir, match[1]!);
  if (fs.existsSync(path.join(gitdir, "index.lock"))) return true;
  return readText(path.join(gitdir, "locked"))?.trim() === "initializing";
};

const isEmptyDirectory = (dir: string): boolean => {
  try {
    return fs.readdirSync(dir).length === 0;
  } catch {
    return false;
  }
};

/**
 * resolveAgentCwd, but it waits for a sibling tool call that is still creating the cwd.
 * ponytail: Pi runs the tool calls of one message in parallel, so `git worktree add W` and a
 * spawn with cwd W race (smarty-dev#1668). A missing cwd is re-checked for up to
 * AGENT_CWD_SETTLE_MS. A linked worktree whose checkout is still running is awaited for up to
 * AGENT_WORKTREE_SETTLE_MS, so the agent never starts on a partial tree; a failed add removes
 * the directory and the spawn fails. A plain directory spawns as soon as it exists.
 */
export const awaitAgentCwd = async (
  parentCwd: string,
  requestedCwd?: string,
  signal?: AbortSignal,
): Promise<string> => {
  const started = Date.now();
  let appeared = false;
  for (let wait = 50; ; wait = Math.min(wait * 2, 500)) {
    let resolved: string | undefined;
    try {
      resolved = resolveAgentCwd(parentCwd, requestedCwd);
    } catch (error) {
      const cause = (error as Error).cause as NodeJS.ErrnoException | undefined;
      if (cause?.code !== "ENOENT" || signal?.aborted || Date.now() + wait > started + AGENT_CWD_SETTLE_MS) {
        throw error;
      }
      appeared = true;
    }
    if (resolved !== undefined) {
      // Git creates W and writes W/.git a moment apart: an empty directory that just appeared
      // gets one more look before it counts as a plain directory.
      const pending = gitWorktreeInitializing(resolved) || (appeared && isEmptyDirectory(resolved));
      if (!pending) return resolved;
      appeared = false;
      if (signal?.aborted || Date.now() + wait > started + AGENT_WORKTREE_SETTLE_MS) {
        throw new Error(
          `Invalid Fabric agent cwd ${JSON.stringify(requestedCwd)}: a git worktree is still being created there` +
            "; call spawn after the git worktree add finishes",
        );
      }
    }
    await new Promise((resolve) => setTimeout(resolve, wait));
  }
};
interface ManagedAgent extends AgentLifecycleState<AgentRunResult> {
  runRoute: AgentRunRouteMetadata;
  id: string;
  name: string;
  task: string;
  /** Conservative activation lineage: foreign/UNKNOWN admitted input clears it forever. */
  outputPrincipal: FabricPrincipal | undefined;
  /** Host-only activation persistence fence, invoked before conflicting input is admitted. */
  onOutputPrincipalDowngrade: (() => void) | undefined;
  runner: FabricAgentRunner;
  kernel?: FabricKernel;
  recursive: boolean;
  residency: "session" | "durable";
  cwd: string;
  statusFile: string;
  lifecycleFile: string;
  lifecycleOffset: number;
  lifecycleRemainder: Buffer;
  runDirectory: string;
  transport: AgentTransportHandle;
  adapter: AgentTransportAdapter;
  launch: AgentTransportLaunch;
  startupAttempts: number;
  /** Mid-run resumes already spent on this run (see AGENT_RESUME_MAX_ATTEMPTS). */
  resumeAttempts: number;
  /** Set by an explicit stop — tool, dashboard, or session shutdown. A requested
   *  stop is terminal and must never be resumed behind the operator's back. */
  stopRequested: boolean;
  /** Process teardown is an ownership obligation, even after logical settlement. */
  processStop?: Promise<void>;
  processStopPending?: boolean;
  /** Windows settlement retains native admission until captured close and any stop join. */
  nativeReleasePending?: Promise<void>;
  /**
   * Its owner gave it up (a stopped or removed actor): nobody wants its result, so a worker that
   * dies is not relaunched and the run ends failed (smarty-dev#2184 item 8b). A caller that only
   * stopped waiting does not set this: its detached run is still resumed (review on pi-fabric#160).
   */
  abandoned?: boolean;
  /** Monotonic progress maxima seen for this run across attempts. The worker's
   *  own terminal record keeps its counters, but a host-synthesized stop or
   *  transport-death record resets them to zero, so recovery reads this. */
  observedProgress: { turns: number; toolCalls: number; usage: AgentUsage };
  // The dead-transport failure we are retrying past; preferred over a bare
  // timed_out verdict if the run deadline lands mid-retry.
  lastRetriedTransportFailure?: AgentRunResult;
  /** Set when a relaunch failed; the run settles with it, not the attempt it replaced. */
  relaunchFailure?: AgentRunRecord;
  /** Set when the run failed because its transport lost contact: its worker may still run. */
  lostContact?: string;
  model?: string;
  modelReason?: string;
  thinking?: AgentRunRequest["thinking"];
  routeOutcome?: (result: AgentRunResult) => void;
  /** Original authority for every attempt, never a prepared/observed replacement label. */
  routePin?: Readonly<NonNullable<AgentRunRequest["routeDecision"]>["pin"]>;
  actorId?: string;
  actorName?: string;
  spawner?: AgentSpawner;
  capabilityRequirements?: string[];
  capabilityDigest?: string;
  runnerSessionId?: string;
  mainAgentId?: string;
  fabricSessionId?: string;
  branch?: string;
  worktree?: string;
  nestedSnapshot?: AgentRunRecord[];
  nestedSnapshotAt?: number;
  latestRecord?: AgentRunRecord;
  latestUiRecord?: AgentRunRecord;
  /** Keep the full completion only while its terminal-result save needs retrying. */
  settlementSaveFailure?: { result: AgentRunResult; warning: string };
  background: boolean;
  completionNotified?: boolean;
  lastLivenessCheckAt: number;
  /** Sum of tokens.usage deltas drained from the worker so far. Settle closes
   *  the gap against the status file's cumulative snapshot so the ledger total
   *  is identical whether the stream arrived live or only at settle. */
  usageEmitted: { input: number; output: number; cacheRead: number; cacheWrite: number; cost: number };
}

interface QueuedAgent {
  /** Host-owned admission address, including terminal outcomes with no launch manifest. */
  completionRecipient?: CompletionRecipient;
  routeSaveFailure?: string;
  routeOutcome?: (result: AgentRunResult) => void;
  /** Also guard cleanup if writing the persistent unresolved marker failed. */
  cleanupPending?: string;
  info: AgentHandleInfo;
  task: string;
  enqueuedAt: number;
  abort: AbortController;
  result: Promise<AgentRunResult>;
  resolve(result: AgentRunResult): void;
  pending?: Promise<void>;
  preparing?: boolean;
  terminal?: AgentRunResult;
  background: boolean;
  completionNotified?: boolean;
}

const terminalStatuses = terminalAgentStatuses;

const delay = (milliseconds: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, milliseconds));

const TRANSPORT_EXITED_WITHOUT_RESULT_PREFIX = "Agent transport exited without a result";

const transportExitedWithoutResult = (error: string | undefined): boolean =>
  typeof error === "string" && error.startsWith(TRANSPORT_EXITED_WITHOUT_RESULT_PREFIX);

/**
 * A stop worth resuming: the worker caught a signal mid-run, or its transport
 * died while the run still had work in flight. Timeouts and token-limit kills are
 * policy verdicts, and an explicit stop is operator intent — none of them resume.
 */
const recoverableStop = (record: AgentRunRecord): boolean =>
  record.status === "stopped" ||
  (record.status === "failed" && transportExitedWithoutResult(record.error));

const MAX_RESUME_NOTE_CHARS = 2_000;

const setWorkerArgument = (args: string[], name: string, value: string): void => {
  const index = args.indexOf(`--${name}`);
  if (index >= 0) args[index + 1] = value;
  else args.push(`--${name}`, value);
};

/**
 * Task text a resumed attempt receives: the original task plus a bounded note
 * naming the interruption, so a Pi child without a session file picks up where
 * the stopped attempt left off instead of restarting blind.
 */
const resumeTask = (
  task: string,
  record: AgentRunRecord,
  progress: { turns: number; toolCalls: number },
  runDirectory: string,
): string => {
  const summary = summarizeRunLog(runDirectory, 6);
  const turns = Math.max(record.turns, progress.turns);
  const toolCalls = Math.max(record.toolCalls, progress.toolCalls);
  const note = [
    "[Fabric continuation] A previous attempt at this exact task was interrupted before it finished.",
    `It ended with status "${record.status}"${record.error ? ` (${record.error})` : ""} after ${turns} turns and ${toolCalls} tool calls, so the working tree already contains what that attempt completed.`,
    "Inspect the current state first, do not redo work that is already done, and carry the task through to completion.",
    ...(summary ? [`Last observed run activity: ${summary}`] : []),
  ].join(" ");
  return `${note.slice(0, MAX_RESUME_NOTE_CHARS)}\n\n${task}`;
};

const retryablePiStartupError = (error: string | undefined): boolean =>
  typeof error === "string" &&
  /\b(?:no|missing)\s+(?:api key|credentials?)\b|\b(?:api key|credentials?)\s+(?:was\s+)?not found\b/i.test(
    error,
  );

const safeName = (value: string): string =>
  value
    .replace(/[\r\n\t]+/g, " ")
    .trim()
    .slice(0, MAX_NAME_LENGTH) || "Fabric agent";

const readRecord = (filePath: string): AgentRunRecord | undefined => {
  try {
    const parsed: unknown = JSON.parse(fs.readFileSync(filePath, "utf8"));
    if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) return undefined;
    const record = parsed as AgentRunRecord;
    return {
      ...record,
      runner:
        record.runner === "claude"
          ? "claude"
          : record.runner === "veda"
            ? "veda"
            : "pi",
    };
  } catch {
    return undefined;
  }
};

const boundedUiValue = (value: unknown): unknown => {
  if (value === undefined) return undefined;
  try {
    const serialized = JSON.stringify(value);
    if (serialized.length <= MAX_UI_VALUE_CHARS) return JSON.parse(serialized) as unknown;
    return {
      fabricTruncated: true,
      originalChars: serialized.length,
      preview: serialized.slice(0, MAX_UI_VALUE_CHARS - 100),
    };
  } catch {
    return String(value).slice(0, MAX_UI_VALUE_CHARS);
  }
};

const compactUiRecord = (record: AgentRunRecord): AgentRunRecord => {
  const { task, text, error, value, nestedAgents, ...rest } = record;
  return {
    ...rest,
    task:
      task.length <= MAX_UI_TEXT_CHARS
        ? task
        : `${task.slice(0, MAX_UI_TEXT_CHARS)}…`,
    text: text.length <= MAX_UI_TEXT_CHARS ? text : `${text.slice(0, MAX_UI_TEXT_CHARS)}…`,
    ...(error
      ? {
          error:
            error.length <= MAX_UI_ERROR_CHARS
              ? error
              : `${error.slice(0, MAX_UI_ERROR_CHARS)}…`,
        }
      : {}),
    ...(value !== undefined ? { value: boundedUiValue(value) } : {}),
    ...(nestedAgents && nestedAgents.length > 0
      ? { nestedAgents: nestedAgents.map((nested) => compactUiRecord(nested)) }
      : {}),
  };
};

const readNestedAgents = (runDirectory: string, depth = 0): AgentRunRecord[] => {
  if (depth >= 8) return [];
  const nestedRoot = path.join(runDirectory, "nested");
  let entries: string[];
  try {
    entries = fs.readdirSync(nestedRoot);
  } catch {
    return [];
  }
  const agents: AgentRunRecord[] = [];
  for (const entry of entries.slice(0, 200)) {
    const runDirectory = path.join(nestedRoot, entry);
    const record = readRecord(path.join(runDirectory, "status.json"));
    if (!record) continue;
    const nestedAgents = readNestedAgents(runDirectory, depth + 1);
    const { logFile: _logFile, nestedAgents: _nestedAgents, ...safeRecord } = record;
    agents.push(
      compactUiRecord({
        ...safeRecord,
        logFile: path.join(runDirectory, "events.jsonl"),
        ...(nestedAgents.length > 0 ? { nestedAgents } : {}),
      }),
    );
  }
  return agents;
};

const summarizeRunLog = (runDirectory: string, lines: number): string => {
  const page = readJsonlPage(path.join(runDirectory, "events.jsonl"), lines);
  const summary: string[] = [];
  for (const entry of page.lines) {
    const parsed = entry.parsed as Record<string, unknown> | undefined;
    if (!parsed || typeof parsed.type !== "string") continue;
    const rawDetail =
      typeof parsed.error === "string"
        ? parsed.error
        : typeof parsed.message === "string"
          ? parsed.message
          : typeof parsed.toolName === "string"
            ? parsed.toolName
            : typeof parsed.text === "string"
              ? parsed.text
              : "";
    const type = parsed.type.replace(/\s+/g, " ").trim().slice(0, 80);
    const detail = rawDetail
      .replace(/\s+/g, " ")
      .trim()
      .slice(0, MAX_LOG_DETAIL_CHARS);
    summary.push(detail ? `${type}: ${detail}` : type);
  }
  return summary.join(" | ").slice(-MAX_LOG_SUMMARY_CHARS);
};

const writeRecord = (filePath: string, record: AgentRunRecord): void => {
  writeJsonAtomic(filePath, record, { space: 2 });
};

const failedRecord = (
  managed: Omit<
    ManagedAgent,
    "result" | "resolve" | "release" | "abortSignal" | "abortHandler" | "settled"
  >,
  status: "failed" | "stopped" | "timed_out",
  error: string,
): AgentRunResult => {
  const now = Date.now();
  const previous = readRecord(managed.statusFile) ?? managed.latestRecord;
  const progress = managed.observedProgress;
  const usage = { ...progress.usage };
  for (const key of ["input", "output", "cacheRead", "cacheWrite", "cost"] as const) {
    usage[key] = Math.max(usage[key], previous?.usage[key] ?? 0);
  }
  return {
    ...managed.runRoute,
    id: managed.id,
    name: managed.name,
    task: managed.task,
    status,
    runner: managed.runner,
    ...(managed.mainAgentId ? { mainAgentId: managed.mainAgentId } : {}),
    ...(managed.fabricSessionId ? { fabricSessionId: managed.fabricSessionId } : {}),
    ...(managed.latestRecord?.runnerSessionIds ? { runnerSessionIds: [...managed.latestRecord.runnerSessionIds] } : {}),
    ...(managed.kernel ? { kernel: managed.kernel } : {}),
    transport: managed.transport.kind,
    cwd: managed.cwd,
    ...(managed.residency === "durable" ? { residency: "durable" as const } : {}),
    startedAt: now,
    updatedAt: now,
    finishedAt: now,
    turns: Math.max(progress.turns, previous?.turns ?? 0),
    toolCalls: Math.max(progress.toolCalls, previous?.toolCalls ?? 0),
    text: "",
    error,
    usage,
    ...(managed.model ? { model: managed.model } : {}),
    ...(managed.modelReason !== undefined ? { modelReason: managed.modelReason } : {}),
    ...(managed.thinking ? { thinking: managed.thinking } : {}),
    ...(managed.latestRecord?.admittedModel ? { admittedModel: managed.latestRecord.admittedModel } : {}),
    ...(managed.latestRecord?.admittedThinking ? { admittedThinking: managed.latestRecord.admittedThinking } : {}),
    ...(managed.actorId ? { actorId: managed.actorId } : {}),
    ...(managed.actorName ? { actorName: managed.actorName } : {}),
    ...(managed.spawner ? { spawner: managed.spawner } : {}),
    ...(managed.runnerSessionId ? { runnerSessionId: managed.runnerSessionId } : {}),
    ...(managed.transport.sessionId ? { sessionId: managed.transport.sessionId } : {}),
    ...(managed.transport.attachCommand ? { attachCommand: managed.transport.attachCommand } : {}),
    ...(managed.branch ? { branch: managed.branch } : {}),
    ...(managed.worktree ? { worktree: managed.worktree } : {}),
  };
};

export const HOST_STOP_REASON = "stopped by host reload/shutdown";

const lastEventTime = (managed: ManagedAgent): number | undefined => {
  try {
    return fs.statSync(path.join(managed.runDirectory, "events.jsonl")).mtimeMs;
  } catch {
    return managed.latestRecord?.updatedAt;
  }
};

/** The result a spawner gets for a run the host stopped at close: the reason, last error, last event. */
const hostStoppedResult = (result: AgentRunResult, lastEventAt: number | undefined): AgentRunResult => {
  if (result.status !== "stopped") return result;
  const { logFile: _logFile, nestedAgents: _nestedAgents, budget: _budget, ...rest } = result;
  const lastEvent = lastEventAt === undefined ? "none" : new Date(lastEventAt).toISOString();
  return { ...rest, error: `${HOST_STOP_REASON}; last error: ${result.error ?? "none"}; last event: ${lastEvent}` };
};

// Settled handles can be evicted while their descendants still use the shared
// budget. Inspect persisted trees too; marker absence is not checked child exit.
const runRootHasExitVeto = (root: string): boolean => {
  try {
    return fs.readdirSync(root, { withFileTypes: true })
      .some((entry) => entry.isDirectory() && !!runTreeExitVeto(path.join(root, entry.name), 0, undefined, true));
  } catch (error) {
    return (error as NodeJS.ErrnoException).code !== "ENOENT";
  }
};

export class AgentManager {
  readonly #runs = new Map<string, ManagedAgent>();
  readonly #queued = new Map<string, QueuedAgent>();
  readonly #queuedStarts = new Set<Promise<void>>();
  readonly #semaphore: AgentAdmission;
  readonly #worktrees = new WorktreeManager();
  readonly #runRoot: string;
  readonly #managedTempRoot: boolean;
  readonly #retention: FabricRetentionConfig;
  readonly #workerPath: string;
  readonly #sweepPath: string;
  readonly #fabricExtensionPath: string;
  readonly #piBinary: string;
  readonly #claudeBinary: string;
  readonly #vedaBinary: string;
  readonly #currentDepth: number;
  readonly #fullCodeMode: boolean;
  readonly #kernel: () => FabricKernel;
  readonly #pythonRuntime: () => FabricPythonRuntime;
  readonly #mainAgentId: string | undefined;
  readonly #spawner: AgentSpawner | undefined;
  readonly #fabricSessionId: string | undefined;
  readonly #meshRoot: string | undefined;
  readonly #projectRoot: string;
  readonly #completionRecipient: CompletionRecipient | (() => CompletionRecipient) | undefined;
  readonly #hostId: string | undefined;
  readonly #identityId: string | undefined;
  readonly #taskReturnAddressArguments: string[];
  readonly #transports: Map<FabricAgentTransport, AgentTransportAdapter>;
  readonly #onBackgroundComplete: ((result: AgentRunResult, admittedRecipient?: CompletionRecipient) => void) | undefined;
  readonly #onResultConsumed: ((id: string) => void) | undefined;
  readonly #onBeforeResultReturned: ((id: string) => void) | undefined;
  readonly #onStoppedAtClose: ((results: AgentRunResult[]) => void) | undefined;
  readonly #onSettled: ((result: AgentRunResult, admittedRecipient?: CompletionRecipient) => void) | undefined;
  /** Results of runs a previous runtime of this session stopped at reload/shutdown. */
  readonly #previousRuns = new Map<string, AgentRunResult>();
  readonly #onLifecycle: ((event: FabricLifecyclePublishRequest) => void) | undefined;
  readonly #onFollowUpAlarm: ((alarm: AgentFollowUpAlarm) => void) | undefined;
  readonly #followUps = new Map<string, Map<string, AgentFollowUpDelivery>>();
  readonly #followUpTimers = new Map<string, NodeJS.Timeout>();
  readonly #preparePiModel:
    | ((model: string | undefined, requiredPin?: boolean) => Promise<string | void>)
    | undefined;
  readonly #resolveHandoffCompactionBudget:
    | ((model: string | undefined, cwd: string) => Promise<FabricCompactionBudget>)
    | undefined;
  readonly #resolveParticipantGuidance: AgentParticipantGuidanceResolver | undefined;
  readonly #resolveInheritedSessionPins: (() => InheritedSessionPin[] | undefined) | undefined;
  readonly #piModelPreparations = new Map<string, Promise<string | undefined>>();
  readonly #budget: BudgetLedgerState | undefined;
  readonly #budgetOwned: boolean;
  readonly #uiListeners = new Set<() => void>();
  #retentionTimer: NodeJS.Timeout | undefined;
  #retentionSweep: Promise<void> | undefined;
  #budgetSummaryCache: { at: number; value: FabricBudgetSummary } | undefined;
  #claudeModelsCache: { at: number; value: ClaudeModelInfo[] } | undefined;
  #uiListRevision = 0;
  #uiListCache:
    | { revision: number; value: Array<AgentRunRecord | AgentHandleInfo> }
    | undefined;
  #closing = false;
  #closePromise: Promise<void> | undefined;
  readonly #closeAbort = new AbortController();
  readonly #spawns = new Set<Promise<AgentHandleInfo>>();
  readonly #unregisteredTransports = new Set<AgentTransportHandle>();
  readonly #launches = new Set<Promise<AgentTransportHandle>>();

  constructor(
    readonly cwd: string,
    readonly config: FabricAgentConfig,
    options: {
      workerPath?: string;
      /** The detached temp-root sweep entry (dist/storage/sweep-main.js). */
      sweepPath?: string;
      fabricExtensionPath?: string;
      piBinary?: string;
      claudeBinary?: string;
      vedaBinary?: string;
      runRoot?: string;
      fullCodeMode?: boolean;
      kernel?: () => FabricKernel;
      pythonRuntime?: () => FabricPythonRuntime;
      mainAgentId?: string;
      fabricSessionId?: string;
      meshRoot?: string;
      projectRoot?: string;
      /** Root-owned return address; never inferred from a child request or inherited by nested agents. */
      completionRecipient?: CompletionRecipient | (() => CompletionRecipient);
      hostId?: string;
      identityId?: string;
      /** Immediate caller's Pi session, distinct from the inherited root Fabric session. */
      spawnerSessionId?: string;
      retention?: FabricRetentionConfig;
      onBackgroundComplete?: (result: AgentRunResult, admittedRecipient?: CompletionRecipient) => void;
      onResultConsumed?: (id: string) => void;
      /** Fail-closed durable fence before a foreground value reaches its caller. */
      onBeforeResultReturned?: (id: string) => void;
      onStoppedAtClose?: (results: AgentRunResult[]) => void;
      /** Every terminal result, foreground or background, before its run directory can be removed. */
      onSettled?: (result: AgentRunResult, admittedRecipient?: CompletionRecipient) => void;
      onLifecycle?: (event: FabricLifecyclePublishRequest) => void;
      onFollowUpAlarm?: (alarm: AgentFollowUpAlarm) => void;
      preparePiModel?: (model: string | undefined, requiredPin?: boolean) => Promise<string | void>;
      resolveHandoffCompactionBudget?: (model: string | undefined, cwd: string) => Promise<FabricCompactionBudget>;
      resolveParticipantGuidance?: AgentParticipantGuidanceResolver;
      resolveInheritedSessionPins?: () => InheritedSessionPin[] | undefined;
    } = {},
  ) {
    this.#semaphore = new AgentAdmission(config.maxConcurrent, Infinity, config.maxDepth);
    this.#managedTempRoot = options.runRoot === undefined && process.env.PI_FABRIC_RUN_ROOT === undefined;
    this.#runRoot =
      options.runRoot ?? process.env.PI_FABRIC_RUN_ROOT ?? fs.mkdtempSync(path.join(fabricDataRoot(), "pi-fabric-runs-"));
    this.#retention = options.retention ?? DEFAULT_FABRIC_CONFIG.retention;
    this.#workerPath =
      options.workerPath ?? fileURLToPath(new URL("../worker.js", import.meta.url));
    this.#sweepPath =
      options.sweepPath ?? fileURLToPath(new URL("../storage/sweep-main.js", import.meta.url));
    this.#fabricExtensionPath =
      options.fabricExtensionPath ?? fileURLToPath(new URL("../index.js", import.meta.url));
    this.#piBinary = resolvePiBinary(options.piBinary);
    this.#claudeBinary =
      options.claudeBinary ?? process.env.PI_FABRIC_CLAUDE_BINARY ?? config.claude.binary;
    this.#vedaBinary =
      options.vedaBinary ?? process.env.PI_FABRIC_VEDA_BINARY ?? config.veda.binary;
    this.#onBackgroundComplete = options.onBackgroundComplete;
    this.#onResultConsumed = options.onResultConsumed;
    this.#onBeforeResultReturned = options.onBeforeResultReturned;
    this.#onStoppedAtClose = options.onStoppedAtClose;
    this.#onSettled = options.onSettled;
    this.#onLifecycle = options.onLifecycle;
    this.#onFollowUpAlarm = options.onFollowUpAlarm;
    this.#preparePiModel = options.preparePiModel;
    this.#resolveHandoffCompactionBudget = options.resolveHandoffCompactionBudget;
    this.#resolveParticipantGuidance = options.resolveParticipantGuidance;
    this.#resolveInheritedSessionPins = options.resolveInheritedSessionPins;
    this.#currentDepth = Math.max(0, Number(process.env.PI_FABRIC_DEPTH ?? "0") || 0);
    this.#fullCodeMode = options.fullCodeMode ?? true;
    this.#kernel = options.kernel ?? (() => "typescript");
    this.#pythonRuntime = options.pythonRuntime ?? (() => "monty");
    this.#mainAgentId =
      options.mainAgentId ?? process.env.PI_FABRIC_MAIN_AGENT_ID;
    this.#fabricSessionId = options.fabricSessionId ?? process.env.PI_FABRIC_SESSION_ID;
    this.#meshRoot = options.meshRoot ?? process.env.PI_FABRIC_MESH_ROOT;
    this.#projectRoot =
      options.projectRoot ?? process.env.PI_FABRIC_PROJECT_ROOT ?? cwd;
    this.#completionRecipient = options.completionRecipient;
    this.#hostId = options.hostId ?? process.env.PI_FABRIC_HOST_ID;
    this.#identityId = options.identityId ?? process.env.PI_FABRIC_IDENTITY_ID;
    this.#taskReturnAddressArguments = taskReturnAddressArguments(
      this.#identityId ?? process.env.PI_FABRIC_ACTOR_ID ?? process.env.PI_FABRIC_PARENT_RUN,
      options.spawnerSessionId ?? process.env.PI_SESSION_ID ?? this.#fabricSessionId,
      this.#mainAgentId,
    );
    this.#spawner = resolveAgentSpawner(this.#identityId, this.#mainAgentId);
    const inheritedBudget = activeBudgetState();
    this.#budget =
      inheritedBudget ??
      (this.#currentDepth === 0 && config.budgetUsd > 0
        ? initBudgetLedger(config.budgetUsd)
        : undefined);
    this.#budgetOwned =
      !inheritedBudget && this.#currentDepth === 0 && config.budgetUsd > 0;
    const adapters: AgentTransportAdapter[] = [
      new ProcessTransport(),
      new TmuxTransport(),
      new ScreenTransport(),
      new LocaltermTransport(),
      new HerdrTransport(),
    ];
    this.#transports = new Map(adapters.map((adapter) => [adapter.kind, adapter]));
    if (this.#managedTempRoot) {
      markRunRootActive(this.#runRoot);
      // Allocate ownership now; scan only on actual agent use or close.
    }
  }

  /** This runtime's creation host, not a child participant's upstream owner. */
  get runtimeHostId(): string | undefined { return this.#hostId; }

  defaultModel(runner: FabricAgentRunner = this.config.runner): string | undefined {
    return runner === "claude" ? this.config.claude.model
      : runner === "veda" ? this.config.veda.model : this.config.model;
  }

  assertModelAllowed(model: string | undefined, runner?: FabricAgentRunner): void {
    // Veda owns its backend default. With an active deny policy, absence cannot
    // prove admission; require a selector before queueing or durable dispatch.
    if (runner === "veda" && !model?.trim() && this.config.deniedModels.length > 0) {
      const error = new FabricModelDeniedError("veda/<unresolved-backend-default>", this.config.deniedModelReplacement);
      error.message = "Fabric cannot admit the unresolved Veda backend default; set agents.veda.model or pass an explicit model. " + error.message;
      throw error;
    }
    assertFabricModelAllowed(model, this.config);
    // Admit the exact backend selector sent by each runner's argv builder too.
    if (model && runner === "claude") assertFabricModelAllowed(normalizeClaudeModel(model), this.config);
    if (model && runner === "veda") assertFabricModelAllowed(normalizeVedaModel(model), this.config);
  }

  /** Resolve the actual backend target without committing a run or binding. */
  async prepareModelForAdmission(
    model: string | undefined,
    runner: FabricAgentRunner,
    resolvePi?: (model: string) => Promise<string>,
    requiredPin = false,
    signal: AbortSignal = this.#closeAbort.signal,
    timeoutMs?: number,
  ): Promise<string | undefined> {
    this.assertModelAllowed(model, runner);
    if (runner === "pi") {
      const prepared = await this.#prepareModel(model, requiredPin, signal, timeoutMs);
      this.assertModelAllowed(prepared, runner);
      return prepared;
    }
    // Without host policy, preserve backend-owned aliases and defaults verbatim.
    if (this.config.deniedModels.length === 0) return model;
    const unresolved = (): never => {
      const error = new FabricModelDeniedError(`${runner}/<unresolved-backend-model>`, this.config.deniedModelReplacement);
      error.message = `Fabric cannot establish the ${runner} backend model under the active host policy; select a known concrete model. ` + error.message;
      throw error;
    };
    let prepared: string;
    try {
      if (runner === "veda") {
        const selector = model ? normalizeVedaModel(model) : "";
        // Only Pi's registry can establish this backend's concrete provider/model.
        // Do not reinterpret Veda aliases, bare IDs, other backends or fuzzy misses.
        const prepare = resolvePi ?? this.#preparePiModel;
        if (this.config.veda.backend !== "pi" || !/^[^\s/]+\/[^\s]+$/.test(selector) || !prepare) return unresolved();
        const resolved = await prepare(selector);
        if (typeof resolved !== "string" || resolved.trim().toLowerCase() !== selector.toLowerCase()) return unresolved();
        prepared = resolved.trim();
      } else {
        const selector = model ? normalizeClaudeModel(model) : "default";
        const catalog = await this.claudeModels();
        const selected = catalog.find(entry => normalizeClaudeModel(entry.value) === selector || normalizeClaudeModel(entry.resolvedModel) === selector);
        if (!selected?.resolvedModel || selected.resolvedModelKnown === false) return unresolved();
        this.assertModelAllowed(selected.resolvedModel, runner);
        prepared = normalizeClaudeModel(selected.resolvedModel);
        // The native catalog names Claude IDs; host policies can name their Pi key.
        assertFabricModelAllowed(`anthropic/${prepared}`, this.config);
      }
    } catch (error) {
      if (error instanceof FabricModelDeniedError) throw error;
      return unresolved();
    }
    this.assertModelAllowed(prepared, runner);
    return prepared;
  }

  async #prepareModel(model: string | undefined, requiredPin = false, signal: AbortSignal = this.#closeAbort.signal, timeoutMs?: number): Promise<string | undefined> {
    if (!this.#preparePiModel) return model;
    const key = `${requiredPin ? "route-pin:" : "participant:"}${model?.trim() || "<session-default>"}`;
    // Ordinary participant admission keeps the host's original one-argument contract.
    // Only required route pins opt in to strict preparation with the second argument.
    // Each waiter races its own abort/deadline, even when sharing model preparation.
    const preparation = this.#piModelPreparations.get(key) ?? Promise.resolve().then(() =>
      requiredPin ? this.#preparePiModel!(model, true) : this.#preparePiModel!(model),
    ).then((prepared) => {
      const effective = typeof prepared === "string" ? prepared.trim() || model : model;
      if (requiredPin && effective !== model) {
        throw Object.assign(new Error(`MODEL_ROUTE_PIN_MISMATCH: required ${model}, prepared ${effective}; task was not sent`), {
          name: "ModelRoutePinMismatchError", code: "MODEL_ROUTE_PIN_MISMATCH",
        });
      }
      return effective;
    });
    this.#piModelPreparations.set(key, preparation);
    let timer: NodeJS.Timeout | undefined;
    let onAbort: (() => void) | undefined;
    try {
      return await Promise.race([preparation, new Promise<never>((_resolve, reject) => {
        onAbort = () => reject(new Error(this.#closing ? "Fabric agent manager is closing" : "Agent launch preparation aborted"));
        if (signal.aborted) { onAbort(); return; }
        signal.addEventListener("abort", onAbort, { once: true });
        if (timeoutMs !== undefined) {
          timer = setTimeout(() => reject(new AgentLaunchPreparationTimeoutError(timeoutMs)), timeoutMs);
        }
      })]);
    } finally {
      if (timer) clearTimeout(timer);
      if (onAbort) signal.removeEventListener("abort", onAbort);
      // An abandoned shared promise must not poison the next admission. Its late
      // completion is observed by the race but has no path to worker creation.
      if (this.#piModelPreparations.get(key) === preparation) {
        this.#piModelPreparations.delete(key);
      }
    }
  }

  subscribeUi(listener: () => void): () => void {
    this.#uiListeners.add(listener);
    return () => this.#uiListeners.delete(listener);
  }

  resolveCwd(requestedCwd?: string, signal?: AbortSignal): Promise<string> {
    return awaitAgentCwd(this.cwd, requestedCwd, signal);
  }

  #inheritedSessionPins(request: AgentRunRequest): InheritedSessionPin[] | undefined {
    const explicit = request.inheritedSessionPins;
    if (explicit && explicit.length > 0) return explicit;
    const resolved = this.#resolveInheritedSessionPins?.();
    if (resolved && resolved.length > 0) return resolved;
    if (this.#currentDepth > 0) {
      const fromEnv = inheritedSessionPinsFromEnv();
      if (fromEnv.length > 0) return fromEnv;
    }
    return undefined;
  }

  /** Resolve once at the caller boundary, before launch or resident/trajectory handoff. */
  resolveKernel(
    request: Pick<AgentRunRequest, "kernel" | "runner" | "extensions">,
  ): FabricKernel | undefined {
    const choice = request.kernel;
    if (choice !== undefined && choice !== "inherit" && choice !== "typescript" && choice !== "python") {
      throw new Error(`Invalid Fabric agent kernel: ${String(choice)}`);
    }
    const runner = request.runner ?? this.config.runner;
    if (runner !== "pi" && runner !== "claude" && runner !== "veda") {
      throw new Error(`Unsupported Fabric agent runner: ${String(runner)}`);
    }
    if (runner !== "pi" || !(request.extensions ?? this.config.extensions)) {
      if (choice === "typescript" || choice === "python") {
        throw new Error("Explicit agent kernel requires the Pi runner with Fabric extensions enabled");
      }
      return undefined;
    }
    const kernel = choice === undefined || choice === "inherit" ? this.#kernel() : choice;
    if (kernel !== "typescript" && kernel !== "python") {
      throw new Error(`Invalid inherited Fabric agent kernel: ${String(kernel)}`);
    }
    return kernel;
  }

  /** Internal backend policy snapshot; public agent calls select a language, not a backend. */
  resolvePythonRuntime(inherited?: FabricPythonRuntime): FabricPythonRuntime {
    const runtime = inherited === undefined ? this.#pythonRuntime() : inherited;
    if (runtime !== "cpython" && runtime !== "monty") {
      throw new Error(`Invalid inherited Fabric Python runtime: ${String(runtime)}`);
    }
    return runtime;
  }

  /** authorize is host-only activation authority; unlike a guest deadline it survives queuing.
   * beforeCommit is a separate resident-host mutation fence, checked after model preparation.
   * callerReturnAddress is a validated resident caller snapshot, never a task request field.
   */
  spawn(request: AgentRunRequest, signal?: AbortSignal, authorize?: () => boolean, beforeCommit?: (id: string) => void, onOutputPrincipalDowngrade?: () => void, onLaunched?: (handle: AgentHandleInfo) => void, preparation?: AgentLaunchPreparationOptions, callerReturnAddress?: TaskReturnAddress): Promise<AgentHandleInfo> {
    if (this.#closing) return Promise.reject(new Error("Fabric agent manager is closing"));
    const pending = this.#spawn(request, signal, authorize, beforeCommit, onOutputPrincipalDowngrade, onLaunched, preparation, callerReturnAddress && structuredClone(callerReturnAddress));
    this.#spawns.add(pending);
    void pending.then(() => this.#spawns.delete(pending), () => this.#spawns.delete(pending));
    return pending;
  }

  async #launchTransport(adapter: AgentTransportAdapter, request: AgentTransportLaunch): Promise<AgentTransportHandle> {
    if (this.#closing) throw new Error("Fabric agent manager is closing");
    const signal = request.signal ? AbortSignal.any([request.signal, this.#closeAbort.signal]) : this.#closeAbort.signal;
    const pending = adapter.launch({ ...request, signal });
    this.#launches.add(pending);
    try {
      const transport = await pending;
      // Includes launches that race close, before a ManagedAgent can own them.
      this.#unregisteredTransports.add(transport);
      return transport;
    } finally {
      this.#launches.delete(pending);
    }
  }

  async #spawn(request: AgentRunRequest, signal?: AbortSignal, authorize?: () => boolean, beforeCommit?: (id: string) => void, onOutputPrincipalDowngrade?: () => void, onLaunched?: (handle: AgentHandleInfo) => void, preparation?: AgentLaunchPreparationOptions, callerReturnAddress?: TaskReturnAddress): Promise<AgentHandleInfo> {
    if (!this.config.enabled) throw new Error("Agents are disabled in Fabric configuration");
    if (this.#currentDepth >= this.config.maxDepth) {
      throw new Error(`Fabric agent depth limit reached (${this.config.maxDepth})`);
    }
    assertAgentTask(request);
    // Snapshot trusted classification inputs before asynchronous preparation/queueing.
    const explicitRouteClass = request.routeClass ?? request.routeDecision?.routeClass;
    const routeFacts = {
      ...(explicitRouteClass !== undefined ? { routeClass: explicitRouteClass } : {}),
      ...(typeof request.protected === "boolean" ? { protected: request.protected } : {}),
      ...(request.actorId !== undefined ? { actorId: request.actorId } : {}),
      ...(request.actorName !== undefined ? { actorName: request.actorName } : {}),
      handoff: Boolean(request.sessionSeed),
    };
    // Snapshot host-owned identity before any await/queue: later /name changes
    // affect new spawns, never an already-admitted run or its retries.
    const completionRecipient = this.#completionRecipient
      ? { ...(typeof this.#completionRecipient === "function" ? this.#completionRecipient() : this.#completionRecipient) }
      : undefined;
    if (request.model === "auto") throw new Error('Unresolved model: "auto" must go through agents.spawn routing');
    const routedActor = request.routeDecision?.mode === "shadow" && Boolean(request.actorId) &&
      request.routeDecision.actorId === request.actorId && Boolean(request.routeDecision.activationId) && Boolean(request.sessionFile);
    if (request.routeDecision && ((request.runner ?? this.config.runner) !== "pi" ||
      (request.transport ?? this.config.transport) !== "process" || (request.residency ?? "session") !== "session" ||
      request.sessionSeed || (!routedActor && (request.actorId || request.actorName || request.sessionFile)))) {
      throw new Error("Shadow routing requires a new process/Pi task session or a host-prepared actor activation");
    }
    const kernel = this.resolveKernel({
      ...request,
      ...(request.recursive === true ? { extensions: true } : {}),
    });
    const pythonRuntime = kernel ? this.resolvePythonRuntime(request.pythonRuntime) : undefined;
    // Validate explicit execution targets before any model preparation or budget side effects.
    // With no override this deliberately preserves the manager cwd without canonicalizing it.
    const selectedCwd = await this.resolveCwd(request.cwd, signal);
    const residency = request.residency ?? "session";
    if (residency !== "session" && residency !== "durable") {
      throw new Error(`Invalid Fabric agent residency: ${String(request.residency)}`);
    }
    const runner = request.runner ?? this.config.runner;
    if (runner !== "pi" && runner !== "claude" && runner !== "veda") {
      throw new Error(`Unsupported Fabric agent runner: ${String(runner)}`);
    }
    if (request.inferenceContext !== undefined && request.inferenceContext !== "full-history" && request.inferenceContext !== "activation") {
      throw new Error("Invalid actor inference context");
    }
    if (request.inferenceContext === "activation" && (runner !== "pi" || !request.sessionFile || !request.actorId || request.sessionSeed)) {
      throw new Error("Activation inference context requires a persistent Pi actor session");
    }
    parseAgentNice(request.nice);
    if (request.persona && runner !== "veda") {
      throw new Error(`The persona option is only supported by the Veda runner, not ${runner}`);
    }
    if (runner === "claude" && request.recursive) {
      throw new Error(
        "Claude runner does not support recursive Fabric. Use a Pi runner for recursive: true, or omit recursive for Claude Code tools.",
      );
    }
    if (runner === "veda" && request.recursive) {
      throw new Error(
        "Veda runner does not support recursive Fabric. Use a Pi runner for recursive: true — Veda executes one headless prompt per invocation.",
      );
    }
    if (request.sessionSeed && runner !== "pi") {
      throw new Error("Trajectory handoff sessions are only supported by the Pi runner");
    }
    if (request.sessionSeed && request.sessionFile) {
      throw new Error("A agent request cannot combine sessionSeed with sessionFile");
    }
    const requiresFabricKernel = kernel === "python" || request.kernel === "typescript";
    const tools = this.#childTools(request, runner, requiresFabricKernel);
    if (runner === "claude") mapClaudeTools(tools);
    if (runner === "veda") mapVedaTools(tools);
    const routePin = request.routeDecision ? Object.freeze({ ...request.routeDecision.pin }) : undefined;
    let model = routePin?.model ?? (request.model?.trim() || this.defaultModel(runner));
    this.assertModelAllowed(model, runner);
    // Alternate backend targets must be known before even accepting a queue receipt.
    if (runner !== "pi" && this.config.deniedModels.length > 0) {
      model = await this.prepareModelForAdmission(model, runner);
    }
    if (this.#budget) {
      const spent = readBudgetLedger(this.#budget.file).cost;
      if (spent >= this.#budget.budget) {
        throw new Error(
          `Fabric recursion budget exceeded: spent $${spent.toFixed(6)} of $${this.#budget.budget.toFixed(6)}. Increase agents.budgetUsd or simplify the task.`,
        );
      }
    }
    const admissionSignal = signal ? AbortSignal.any([signal, this.#closeAbort.signal]) : this.#closeAbort.signal;
    if (admissionSignal.aborted) throw new Error("Operation aborted");
    // Count accepted requests, including queued ones, exactly once.
    this.#semaphore.admit(this.#currentDepth + 1);
    const id = randomUUID().replaceAll("-", "");
    const name = safeName(request.name ?? request.task.split("\n", 1)[0] ?? "Fabric agent");
    const callerSignal = signal;
    const assertAuthorized = (): void => {
      if (authorize && !authorize()) {
        // Mark revocation before throwing so a queued receipt settles as stopped,
        // not a launch failure, even if only the generation/state check changed.
        this.#queued.get(id)?.abort.abort();
        throw new Error("Agent activation no longer authorized");
      }
    };
    let routeDispatch: ReturnType<typeof import("./model-route.js")["prepareRouteDispatch"]> | undefined;
    if (request.routeDecision) {
      const { prepareRouteDispatch } = await import("./model-route.js");
      routeDispatch = prepareRouteDispatch(request.routeDecision, undefined, path.join(this.#runRoot, id), id, request.routeRecord);
    }
    const startPrepared = async (release: () => void, signal = admissionSignal): Promise<AgentHandleInfo> => {
      try {
        // A permit is no longer a queue wait. Only model/auth preparation is raced;
        // transport launch and its unknown-worker obligations must never be retried
        // just because a setup timer expired.
        const queued = this.#queued.get(id);
        if (queued) { queued.preparing = true; this.#invalidateUiList(); }
        preparation?.onPreparing?.();
        model = await this.prepareModelForAdmission(routePin?.model ?? model, runner, undefined, Boolean(routePin), signal, preparation?.timeoutMs);
        if (this.#closing) throw new Error("Fabric agent manager is closing");
        if (signal?.aborted) throw new Error("Agent launch aborted");
        assertAuthorized();
        // Internal resident-host fence: preparation may outlive the caller's deadline.
        // Activation authority and durable request commit are independent obligations.
        beforeCommit?.(id);
      } catch (error) {
        try { routeDispatch?.outcome({ status: signal?.aborted ? "stopped" : "failed" }); } catch { /* pinned work is never blocked by routing storage */ }
        release();
        throw error;
      }
      const runDirectory = path.join(this.#runRoot, id);
      fs.mkdirSync(runDirectory, { recursive: true });
      if (this.#managedTempRoot && !this.#retentionTimer) {
        this.#retentionTimer = setInterval(() => this.#scheduleRetentionSweep(), RETENTION_SWEEP_INTERVAL_MS);
        this.#retentionTimer.unref();
        this.#scheduleRetentionSweep();
      }
      if (completionRecipient && this.#meshRoot && !request.actorId) {
        writeJsonAtomic(path.join(runDirectory, "completion-recipient.json"),
          { meshRoot: this.#meshRoot, recipient: completionRecipient,
            supervisor: { pid: process.pid, processStartedAt: processStartTime(process.pid) } }, { durable: true });
      }
      const taskFile = path.join(runDirectory, "task.txt");
      const statusFile = path.join(runDirectory, "status.json");
      const lifecycleFile = path.join(runDirectory, "lifecycle.jsonl");
      const logFile = path.join(runDirectory, "events.jsonl");
      const steerFile = path.join(runDirectory, "steer.jsonl");
      const schemaFile = request.schema ? path.join(runDirectory, "schema.json") : undefined;
      const imagesFile = request.images && request.images.length > 0
        ? path.join(runDirectory, "images.json")
        : undefined;
      fs.writeFileSync(taskFile, request.task, { encoding: "utf8", mode: 0o600 });
      if (request.provenance) fs.writeFileSync(taskFile + ".provenance.json", JSON.stringify(request.provenance), { mode: 0o600 });
      if (imagesFile) {
        fs.writeFileSync(imagesFile, JSON.stringify(request.images), {
          encoding: "utf8",
          mode: 0o600,
        });
      }
      if (schemaFile) {
        fs.writeFileSync(schemaFile, JSON.stringify(request.schema, null, 2), {
          encoding: "utf8",
          mode: 0o600,
        });
      }

      let agentCwd = selectedCwd;
      let branch: string | undefined;
      let worktree: string | undefined;
      if (request.worktree) {
        try {
          const lease = await this.#worktrees.create(id, selectedCwd, name, request.cwd !== undefined);
          agentCwd = lease.cwd;
          branch = lease.branch;
          worktree = lease.path;
        } catch (error) {
          release();
          throw error;
        }
      }

      try {
        const sessionFile = request.sessionSeed
          ? writeHandoffSession(
              request.sessionSeed,
              agentCwd,
              path.join(runDirectory, "handoff-session"),
              request.thinkingTransfer,
              request.handoffCompact,
              request.handoffCompact ? await this.#resolveHandoffCompactionBudget?.(model, agentCwd) : undefined,
            )
          : routedActor ? request.sessionFile : routeDispatch?.bindSession(agentCwd) ?? request.sessionFile;
        const adapter = await this.#resolveTransport(request.transport ?? this.config.transport);
        const runRoute = createRunRouteMetadata({ ...routeFacts, runner, transport: adapter.kind });
        const timeoutMs = effectiveAgentTimeoutMs(
          this.config.timeoutMs,
          request.timeoutMs,
        );
        const thinking = routePin?.effort ?? request.thinking ?? this.config.thinking;
        const nice = effectiveAgentNice(this.config.nice ?? 0, request.nice);
        const recursive = runner === "pi" && request.recursive === true;
        const extensions = recursive ? true : (request.extensions ?? this.config.extensions);
        const inheritedSessionPins = runner === "pi" && extensions
          ? this.#inheritedSessionPins(request)
          : undefined;
        // In a full-code parent every extension-enabled Pi child runs Fabric
        // through fabric_exec — not only recursively spawned agents. An explicit
        // extensions: false request opts the child back out to the native tool
        // surface, and a non-full-code parent keeps the historical behavior.
        // Recursive children additionally keep their recursive permission
        // surface (the "agent" granted risk) below.
        const inheritedFullCodeMode = runner === "pi" && this.#fullCodeMode && extensions;
        const componentGuidance = recursive
          ? undefined
          : this.#resolveParticipantGuidance?.({ ...(model ? { model } : {}), runner })?.trim();
        const spawnerGuidance = runner === "pi" && extensions && this.#spawner && this.#spawner.kind !== "main"
          ? `Your immediate Fabric spawner is ${this.#spawner.kind} ${this.#spawner.id}${this.#spawner.runId ? ` (run ${this.#spawner.runId})` : ""}. ` +
            "Your final result is returned to that spawner automatically. For addressed updates use agents.followUp({id:'spawner',message:'...'}); " +
            "agents.spawner() discovers this binding. The 'main' target is the lineage root, NOT an actor spawner."
          : undefined;
        const systemPrompt = [request.systemPrompt?.trim(), componentGuidance, spawnerGuidance]
          .filter((section): section is string => Boolean(section))
          .join("\n\n") || undefined;
        const sessionExportDir = resolveSessionExportDir(this.config);
        const sessionExportFile = sessionExportDir
          ? sessionExportFileFor(sessionExportDir, agentCwd, id, new Date())
          : undefined;
        const workerArguments = [
          ...(request.residentStartupProbe ? ["--resident-startup-probe", "true"] : []),
          "--id",
          id,
          "--name",
          name,
          "--runner",
          runner,
          ...(kernel ? ["--kernel", kernel] : []),
          ...(pythonRuntime ? ["--python-runtime", pythonRuntime] : []),
          "--task-file",
          taskFile,
          ...(imagesFile ? ["--images-file", imagesFile] : []),
          "--status-file",
          statusFile,
          "--lifecycle-file",
          lifecycleFile,
          "--log-file",
          logFile,
          "--cwd",
          agentCwd,
          "--pi-binary",
          this.#piBinary,
          "--claude-binary",
          this.#claudeBinary,
          "--veda-binary",
          this.#vedaBinary,
          "--veda-backend",
          this.config.veda.backend,
          "--veda-persona",
          request.persona?.trim() || this.config.veda.persona,
          "--timeout-ms",
          String(timeoutMs),
          "--depth",
          String(this.#currentDepth + 1),
          "--full-code-mode",
          String(inheritedFullCodeMode),
          ...(this.#mainAgentId ? ["--main-agent-id", this.#mainAgentId] : []),
          ...(this.#spawner ? ["--spawner-id", this.#spawner.id, "--spawner-kind", this.#spawner.kind,
            ...(this.#spawner.runId ? ["--spawner-run", this.#spawner.runId] : [])] : []),
          ...(this.#fabricSessionId ? ["--fabric-session-id", this.#fabricSessionId] : []),
          ...(adapter.kind === "process" && !request.actorId
            ? callerReturnAddress ? ["--task-return-address", JSON.stringify(callerReturnAddress)] : this.#taskReturnAddressArguments
            : []),
          "--extensions",
          String(extensions),
          "--tools",
          JSON.stringify(tools),
          "--granted-risks",
          JSON.stringify(recursive ? ["agent"] : []),
          ...(this.config.maxTokensPerChild > 0
            ? ["--max-tokens", String(this.config.maxTokensPerChild)]
            : []),
          ...(nice > 0 ? ["--nice", String(nice)] : []),
          "--transport",
          adapter.kind,
          "--route-class", runRoute.routeClass,
          "--route-class-source", runRoute.routeClassSource,
          ...(runRoute.protected !== undefined ? ["--protected", String(runRoute.protected)] : []),
          ...(recursive || inheritedFullCodeMode || requiresFabricKernel
            ? ["--fabric-extension", this.#fabricExtensionPath]
            : []),
          ...(model ? ["--model", model] : []),
          ...(request.modelReason !== undefined ? ["--model-reason", request.modelReason] : []),
          ...(thinking ? ["--thinking", thinking] : []),
          ...(routeDispatch ? ["--route-header", routeDispatch.header] : []),
          ...(request.routeDecision?.mode === "judgment" ? ["--judgment", "true"] : []),
          ...(systemPrompt ? ["--system-prompt", systemPrompt] : []),
          ...(sessionFile ? ["--session-file", sessionFile] : []),
          ...(request.inferenceContext ? ["--inference-context", request.inferenceContext] : []),
          ...(sessionExportFile ? ["--session-export-file", sessionExportFile] : []),
          ...(inheritedSessionPins && inheritedSessionPins.length > 0
            ? ["--inherited-session-pins", serializeInheritedSessionPins(inheritedSessionPins)]
            : []),
          ...(request.actorId ? ["--actor-id", request.actorId] : []),
          ...(request.actorName ? ["--actor-name", request.actorName] : []),
          ...(request.bashTimeoutSeconds !== undefined ? ["--actor-bash-timeout", String(request.bashTimeoutSeconds)] : []),
          ...(request.capabilityRequirements
            ? ["--capability-requirements", JSON.stringify(request.capabilityRequirements)]
            : []),
          ...(request.capabilityDigest
            ? ["--capability-digest", request.capabilityDigest]
            : []),
          ...(request.meshRoot ?? this.#meshRoot
            ? ["--mesh-root", request.meshRoot ?? this.#meshRoot!]
            : []),
          "--project-root",
          this.#projectRoot,
          ...(this.#hostId ? ["--owner-host-id", this.#hostId] : []),
          ...(this.#identityId ? ["--owner-identity-id", this.#identityId] : []),
          ...(request.runnerSessionId
            ? ["--runner-session-id", request.runnerSessionId]
            : []),
          "--run-root",
          path.join(runDirectory, "nested"),
          "--steer-file",
          steerFile,
          ...(schemaFile ? ["--schema-file", schemaFile] : []),
          ...(schemaFile && request.replyTool && runner === "pi" ? ["--reply-tool", "true"] : []),
          ...(branch ? ["--branch", branch] : []),
          ...(worktree ? ["--worktree", worktree] : []),
        ];
        const launch: AgentTransportLaunch = {
          id,
          name,
          cwd: agentCwd,
          workerPath: this.#workerPath,
          workerArguments,
          signal,
          onUnconfirmedExit: (reason) => {
            const managed = this.#runs.get(id);
            if (managed) this.#markLost(managed, reason);
            else {
              // Cancelled/in-flight launches have no registered owner yet.
              try { markUnresolvedWorker(runDirectory, reason, { runId: id, transport: adapter.kind }); } catch { /* launch cleanup remains pending */ }
            }
          },
          ...(authorize ? { authorize: () => { assertAuthorized(); return true; } } : {}),
        };
        if (this.#closing) throw new Error("Fabric agent manager is closing");
        if (signal?.aborted) throw new Error("Agent launch aborted");
        // Preparation/transport resolution may have yielded since admission. Check the
        // current owner and activation generation at the final launch boundary too.
        assertAuthorized();
        const transport = await this.#launchTransport(adapter, launch);
        const queued = this.#queued.get(id);
        const lifecycle = createAgentLifecycle<AgentRunResult>(release);
        if (queued) {
          lifecycle.result = queued.result;
          lifecycle.resolve = queued.resolve;
        }
        if (signal?.aborted || this.#closing || (authorize && !authorize())) {
          queued?.abort.abort();
          if (await this.#stopUnregisteredTransport(transport)) {
            this.#unregisteredTransports.delete(transport);
            throw new Error("Agent launch aborted");
          }
          // A stop acknowledgment alone is not proof of exit. Retain both sets of
          // working files and expose the obligation through the stopped receipt.
          throw Object.assign(new Error("Agent launch aborted; cleanup pending: worker exit unconfirmed"), {
            launchOutcome: "unknown", cleanupPending: true, transport: transport.kind, sessionId: transport.sessionId,
          });
        }
        const managed: ManagedAgent = {
          runRoute,
          id,
          name,
          task: request.task,
          ...(this.#mainAgentId ? { mainAgentId: this.#mainAgentId } : {}),
          ...(this.#fabricSessionId ? { fabricSessionId: this.#fabricSessionId } : {}),
          outputPrincipal: copyFabricPrincipal(request.provenance?.principal),
          onOutputPrincipalDowngrade,
          runner,
          ...(kernel ? { kernel } : {}),
          recursive,
          residency,
          cwd: agentCwd,
          statusFile,
          lifecycleFile,
          lifecycleOffset: 0,
          lifecycleRemainder: Buffer.alloc(0),
          runDirectory,
          transport,
          adapter,
          // Ordinary caller abort detaches a managed worker; it must not veto
          // that worker's later retries. Actor authority stays attached.
          launch: { ...launch, signal: authorize ? signal : undefined },
          startupAttempts: 1,
          ...lifecycle,
          abortSignal: queued && !authorize ? undefined : signal,
          abortHandler: undefined,
          ...(model ? { model } : {}),
          ...(request.modelReason !== undefined ? { modelReason: request.modelReason } : {}),
          ...(thinking ? { thinking } : {}),
          ...(routeDispatch ? { routeOutcome: routeDispatch.outcome } : {}),
          ...(routePin ? { routePin } : {}),
          ...(request.actorId ? { actorId: request.actorId } : {}),
          ...(request.actorName ? { actorName: request.actorName } : {}),
          ...(this.#spawner ? { spawner: this.#spawner } : {}),
          ...(request.capabilityRequirements
            ? { capabilityRequirements: [...request.capabilityRequirements] }
            : {}),
          ...(request.capabilityDigest ? { capabilityDigest: request.capabilityDigest } : {}),
          ...(request.runnerSessionId ? { runnerSessionId: request.runnerSessionId } : {}),
          ...(branch ? { branch } : {}),
          ...(worktree ? { worktree } : {}),
          settled: false,
          background: queued?.background ?? false,
          lastLivenessCheckAt: 0,
          resumeAttempts: 0,
          stopRequested: false,
          observedProgress: {
            turns: 0,
            toolCalls: 0,
            usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
          },
          usageEmitted: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
        };
        if (signal && (!queued || authorize)) {
          managed.abortHandler = () => this.#handleCallerAbort(id);
          signal.addEventListener("abort", managed.abortHandler, { once: true });
        }
        this.#runs.set(id, managed);
        this.#queued.delete(id);
        this.#unregisteredTransports.delete(transport);
        this.#invalidateUiList();
        void this.#monitor(managed, timeoutMs);
        const handle = this.#handleInfo(managed, "running");
        // A queued receipt is not a worker. Notify only after launch and registration.
        try { onLaunched?.(handle); } catch { /* observers must not undo a launched worker */ }
        return handle;
      } catch (error) {
        release();
        // An unconfirmed launch may have started a worker that already uses the worktree
        // and run files: keep both, marked, and neither retry nor adopt it.
        if ((error as { launchOutcome?: string } | undefined)?.launchOutcome === "unknown") {
          const obligation = error as Error & { cleanupPending?: boolean; transport?: string; sessionId?: string };
          const queued = this.#queued.get(id);
          if (queued) {
            queued.cleanupPending = obligation.message;
            queued.info = { ...queued.info, ...(worktree ? { worktree } : {}), ...(branch ? { branch } : {}) };
          }
          try {
            markUnresolvedWorker(runDirectory, obligation.message, {
              runId: id, ...(worktree ? { worktree } : {}),
              ...(obligation.cleanupPending ? { cleanupPending: true, transport: obligation.transport, sessionId: obligation.sessionId } : {}),
            });
          } catch { /* best effort: the worktree is kept either way */ }
          throw error;
        }
        try { routeDispatch?.outcome({ status: signal?.aborted ? "stopped" : "failed" }); } catch { /* pinned work is never blocked by routing storage */ }
        // A launch rejected before publishing a worker record is rollback, not
        // admitted-run collection. Any persisted record still needs saved exit proof.
        if (worktree && !runTreeExitVeto(runDirectory, 0, undefined, fs.existsSync(path.join(runDirectory, "status.json")))) {
          await this.#worktrees.cleanup(id, true).catch(() => false);
        }
        throw error;
      }
    };
    const start = async (release: () => void, signal = admissionSignal): Promise<AgentHandleInfo> => {
      try { return await startPrepared(release, signal); }
      catch (error) {
        // Cover preparation writes and worktree creation as well as transport failures.
        release();
        if ((error as { launchOutcome?: string } | undefined)?.launchOutcome !== "unknown" && !this.#queued.has(id)) {
          try { routeDispatch?.outcome({ status: signal?.aborted ? "stopped" : "failed" }); }
          catch (saveError) { console.warn(`[pi-fabric] Pre-worker route outcome save failed; files retained at ${path.join(this.#runRoot, id)}: ${String(saveError)}`); }
        }
        throw error;
      }
    };
    let release: (() => void) | undefined;
    try { release = this.#semaphore.tryAcquire("native", admissionSignal); }
    catch (error) {
      try { routeDispatch?.outcome({ status: admissionSignal.aborted ? "stopped" : "failed" }); } catch { /* storage does not block cancellation */ }
      throw error;
    }
    if (release) return start(release).catch((error) => { release(); throw error; });
    return this.#enqueue({
      ...createRunRouteMetadata({ ...routeFacts, runner, transport: request.transport ?? this.config.transport }),
      id, name, status: "queued", runner, transport: request.transport ?? this.config.transport,
      cwd: selectedCwd, residency, recursive: request.recursive === true,
      ...(kernel ? { kernel } : {}),
      ...(model ? { model } : {}),
      ...(request.modelReason !== undefined ? { modelReason: request.modelReason } : {}),
      ...(request.routeDecision?.pin.effort ?? request.thinking ?? this.config.thinking
        ? { thinking: request.routeDecision?.pin.effort ?? request.thinking ?? this.config.thinking } : {}),
      ...(request.actorId ? { actorId: request.actorId } : {}),
      ...(request.actorName ? { actorName: request.actorName } : {}),
      ...(this.#spawner ? { spawner: this.#spawner } : {}),
      ...(request.capabilityRequirements ? { capabilityRequirements: [...request.capabilityRequirements] } : {}),
      ...(request.capabilityDigest ? { capabilityDigest: request.capabilityDigest } : {}),
      ...(request.runnerSessionId ? { runnerSessionId: request.runnerSessionId } : {}),
    }, request.task, start, authorize ? callerSignal : undefined, routeDispatch?.outcome, completionRecipient);
  }

  #enqueue(
    info: AgentHandleInfo,
    task: string,
    start: (release: () => void, signal: AbortSignal) => Promise<AgentHandleInfo>,
    ownerSignal?: AbortSignal,
    routeOutcome?: (result: AgentRunResult) => void,
    completionRecipient?: CompletionRecipient,
  ): AgentHandleInfo {
    const abort = new AbortController();
    // Guest receipts belong to the session, not a program deadline. Host-owned
    // actor activations retain their explicit stop/interrupt authority.
    const signal = AbortSignal.any([abort.signal, this.#closeAbort.signal, ...(ownerSignal ? [ownerSignal] : [])]);
    let resolve!: (result: AgentRunResult) => void;
    const result = new Promise<AgentRunResult>((done) => { resolve = done; });
    const queued: QueuedAgent = { info, task, enqueuedAt: Date.now(), abort, result, resolve, background: false,
      ...(routeOutcome ? { routeOutcome } : {}), ...(completionRecipient ? { completionRecipient } : {}) };
    this.#queued.set(info.id, queued);
    const admission = this.#semaphore.acquire("native", signal);
    const pending = (async () => {
      let release: (() => void) | undefined;
      try {
        release = await admission;
        if (signal.aborted) throw new Error("Agent launch aborted");
        await start(release, signal);
      } catch (error) {
        release?.();
        this.#settleQueued(queued, signal.aborted ? "stopped" : "failed", error);
      }
    })();
    queued.pending = pending;
    this.#queuedStarts.add(pending);
    void pending.then(() => this.#queuedStarts.delete(pending));
    this.#invalidateUiList();
    return this.#queuedInfo(queued);
  }

  #queuedInfo(queued: QueuedAgent): AgentHandleInfo {
    const waiting = [...this.#queued.values()].filter((run) => !run.terminal && !run.preparing);
    return structuredClone({ ...queued.info,
      ...(!queued.preparing ? { queuePosition: waiting.indexOf(queued) + 1 } : {}) });
  }

  #settleQueued(queued: QueuedAgent, status: "stopped" | "failed", error: unknown): void {
    if (queued.terminal) return;
    const now = Date.now();
    const record: AgentRunResult = {
      ...queued.info, task: queued.task, status,
      error: error instanceof Error ? error.message : String(error),
      ...(status === "failed" && error instanceof AgentLaunchPreparationTimeoutError && error.launchOutcome === "unlaunched"
        ? { launchPreparationTimeoutMs: error.timeoutMs } : {}),
      startedAt: queued.enqueuedAt, updatedAt: now, finishedAt: now,
      turns: 0, toolCalls: 0, text: "",
      usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
    };
    queued.terminal = record;
    this.#saveQueuedRouteOutcome(queued);
    queued.resolve(record);
    this.#emitLifecycle(queued.info, `run.${status}`, now, { status });
    this.#invalidateUiList();
    try { this.#onSettled?.(record, queued.completionRecipient); } catch { /* retain queued terminal result on publication failure */ }
    this.#notifyQueuedComplete(queued);
  }

  #saveQueuedRouteOutcome(queued: QueuedAgent): boolean {
    if (!queued.terminal || queued.cleanupPending) return !queued.routeSaveFailure;
    let failure: unknown;
    // Finite retries only; persistent failure keeps the full terminal receipt and run files.
    for (let attempt = 0; attempt < 3; attempt++) {
      try {
        queued.routeOutcome?.(queued.terminal);
        if (queued.routeSaveFailure) queued.terminal.warnings = (queued.terminal.warnings ?? []).filter(warning => warning !== queued.routeSaveFailure);
        delete queued.routeSaveFailure;
        return true;
      } catch (error) { failure = error; }
    }
    const warning = `Routing outcome save failed; queued run retained: ${String(failure)}`;
    if (!queued.routeSaveFailure) console.warn(`[pi-fabric] ${warning}`);
    queued.terminal.warnings = [...(queued.terminal.warnings ?? []).filter(item => item !== queued.routeSaveFailure), warning];
    queued.routeSaveFailure = warning;
    this.#invalidateUiList();
    return false;
  }

  #notifyQueuedComplete(queued: QueuedAgent): void {
    if (queued.terminal && queued.background && !queued.completionNotified && !this.#closing && this.config.notifyOnComplete) {
      queued.completionNotified = true;
      try { this.#onBackgroundComplete?.(queued.terminal, queued.completionRecipient); } catch { /* must not break settlement */ }
    }
  }

  async run(
    request: AgentRunRequest,
    signal?: AbortSignal,
    onSpawned?: (handle: AgentHandleInfo) => void,
    authorize?: () => boolean,
    onOutputPrincipalDowngrade?: () => void,
    onQueued?: (handle: AgentHandleInfo) => void,
    preparation?: AgentLaunchPreparationOptions,
  ): Promise<AgentRunResult> {
    const handle = await this.spawn(request, signal, authorize, undefined, onOutputPrincipalDowngrade, onSpawned, preparation);
    const queued = this.#queued.get(handle.id);
    if (queued && !queued.terminal && !queued.preparing) onQueued?.(this.#queuedInfo(queued));
    return this.wait(handle.id);
  }

  /** Side-effect-free settlement join for preparation before a durable mutation fence. */
  async join(id: string): Promise<void> {
    if (this.#previousRun(id)) return;
    const managed = this.#requireRun(id);
    if (!managed.settled) {
      if (!managed.result) throw new Error(`Agent ${id} has no pending result`);
      await managed.result;
    }
  }

  /**
   * Waits for a run's result and consumes it. With timeoutMs, a run still going at the bound is
   * detached instead (smarty-dev#854): it continues, nothing is consumed, and its result arrives
   * as a completion message. An optional signal cancels only this observation (Main's
   * program deadline / Escape), detaches the run, and leaves its result unconsumed.
   */
  async wait(id: string, options: { timeoutMs?: number; signal?: AbortSignal; deferConsumption?: (consume: () => void, abandon?: () => void) => void } = {}): Promise<AgentRunResult> {
    const previous = this.#previousRun(id);
    if (previous) {
      this.prepareForeground(id);
      if (options.deferConsumption) options.deferConsumption(() => this.markForeground(id));
      else this.#onResultConsumed?.(id);
      return previous;
    }
    const consumed = (): void => {
      this.prepareForeground(id);
      if (options.deferConsumption) options.deferConsumption(() => this.markForeground(id), () => this.detachSignal(id));
      else this.#onResultConsumed?.(id);
    };
    const queued = this.#queued.get(id);
    if (queued) {
      if (!options.deferConsumption) queued.background = false;
      const result = options.timeoutMs === undefined && options.signal === undefined
        ? await queued.result
        : await this.#boundedResult(queued, options);
      consumed();
      return structuredClone(result);
    }
    const managed = this.#requireRun(id);
    if (!options.deferConsumption) managed.background = false;
    if (!managed.settled) {
      if (!managed.result) throw new Error(`Agent ${id} has no pending result`);
      const result = options.timeoutMs === undefined && options.signal === undefined
        ? await managed.result
        : await this.#boundedResult(managed, options);
      consumed();
      return result;
    }
    const record = readRecord(managed.statusFile) ?? managed.latestRecord;
    if (!record || !terminalStatuses.has(record.status)) {
      throw new Error(`Agent ${id} settled without a result`);
    }
    consumed();
    return this.#withTransportMetadata(record, managed) as AgentRunResult;
  }

  #boundedResult(managed: ManagedAgent | QueuedAgent, options: { timeoutMs?: number; signal?: AbortSignal }): Promise<AgentRunResult> {
    const { timeoutMs, signal } = options;
    let timer: NodeJS.Timeout | undefined;
    let abort: (() => void) | undefined;
    const bound = new Promise<never>((_resolve, reject) => {
      // Interactive Main owns the observation, not the child. Ending its program
      // must also end the wait without consuming a later detached completion.
      abort = () => {
        this.#detach(managed, "caller stopped waiting; the run continues");
        reject(signal?.reason ?? new Error("Agent wait aborted; the run continues"));
      };
      if (signal?.aborted) { abort(); return; }
      signal?.addEventListener("abort", abort, { once: true });
      if (timeoutMs !== undefined) {
        timer = setTimeout(() => {
          this.#detach(managed, "agents.wait reached its bound; the run continues");
          reject(new AgentWaitBoundError(
            `agents.wait: ${"info" in managed ? managed.info.name : managed.name} is still running after ${describeWaitBound(timeoutMs)}. It continues, and its result ` +
              "arrives as a completion message after this turn: end the turn now.",
          ));
        }, timeoutMs);
        timer.unref?.();
      }
    });
    return Promise.race([managed.result!, bound]).finally(() => {
      clearTimeout(timer);
      if (abort) signal?.removeEventListener("abort", abort);
    });
  }

  /** A terminal worker status is provisional until its retry supervisor settles the run. */
  isSettled(id: string): boolean {
    if (this.#previousRun(id)) return true;
    const queued = this.#queued.get(id);
    return queued ? queued.terminal !== undefined : this.#requireRun(id).settled;
  }

  prepareForeground(id: string): void {
    if (!this.isSettled(id)) return;
    this.#onBeforeResultReturned?.(id);
  }

  markForeground(id: string): void {
    if (!this.isSettled(id)) return;
    this.prepareForeground(id);
    if (!this.#previousRun(id)) {
      const queued = this.#queued.get(id);
      if (queued) queued.background = false;
      else this.#requireRun(id).background = false;
    }
    this.#onResultConsumed?.(id);
  }

  detachSignal(id: string): void {
    this.#detach(this.#queued.get(id) ?? this.#requireRun(id), "caller detached; the run continues");
  }

  /**
   * An abort belongs to the caller, not to the run it started: a returned guest
   * program, a cancelled tool call, or a spent sandbox deadline must not discard
   * hours of participant work. A run that already produced progress is detached
   * and keeps going to a real terminal state; a run that never started working is
   * still stopped, because releasing it loses nothing.
   */
  /**
   * Its owner no longer wants this run (an actor stopped or removed while the run finishes): it
   * may end on its own, but a worker that dies is not relaunched (smarty-dev#2184 item 8b).
   */
  abandon(id: string): void {
    // A queued activation has no worker to finish: revoke its permit request.
    this.#queued.get(id)?.abort.abort();
    const managed = this.#runs.get(id);
    if (managed && !managed.settled) managed.abandoned = true;
  }

  #handleCallerAbort(id: string): void {
    const managed = this.#runs.get(id);
    if (!managed || managed.settled || this.#closing) return;
    if (!this.#observedWork(managed)) {
      void this.stop(id);
      return;
    }
    this.#detach(managed, "caller aborted; the run continues");
  }

  #detach(managed: ManagedAgent | QueuedAgent, reason: string): void {
    if ("info" in managed) {
      const running = this.#runs.get(managed.info.id);
      if (running) {
        this.#detach(running, reason);
        return;
      }
      managed.background = true;
      this.#notifyQueuedComplete(managed);
      return;
    }
    const attached = managed.abortSignal !== undefined || managed.abortHandler !== undefined;
    if (managed.abortSignal && managed.abortHandler) {
      managed.abortSignal.removeEventListener("abort", managed.abortHandler);
    }
    managed.abortSignal = undefined;
    managed.abortHandler = undefined;
    if (managed.background) return;
    managed.background = true;
    // A fast worker may settle before agents.spawn returns and detaches it.
    if (managed.settled) {
      const record = readRecord(managed.statusFile) ?? managed.latestRecord;
      if (record && terminalStatuses.has(record.status)) {
        this.#notifyBackgroundComplete(managed, this.#withTransportMetadata(record, managed) as AgentRunResult);
      }
    }
    if (attached) {
      this.#emitLifecycle(managed, "run.detached", Date.now(), { data: { reason } });
    }
  }

  #observedWork(managed: ManagedAgent): boolean {
    const { turns, toolCalls } = managed.observedProgress;
    if (turns > 0 || toolCalls > 0) return true;
    const record = managed.latestRecord ?? readRecord(managed.statusFile);
    return record !== undefined && (record.turns > 0 || record.toolCalls > 0);
  }

  /**
   * Track element-wise maxima of the run's counters. Usage components only grow
   * within a run, so a per-field maximum stays the cumulative total even when a
   * host-synthesized stop or transport-death record resets the counters to zero.
   */
  #observeProgress(managed: ManagedAgent, record: AgentRunRecord): void {
    const seen = managed.observedProgress;
    if (record.turns > seen.turns) seen.turns = record.turns;
    if (record.toolCalls > seen.toolCalls) seen.toolCalls = record.toolCalls;
    for (const key of ["input", "output", "cacheRead", "cacheWrite", "cost"] as const) {
      if (record.usage[key] > seen.usage[key]) seen.usage[key] = record.usage[key];
    }
  }

  status(id: string): AgentRunRecord | AgentHandleInfo {
    const queued = this.#queued.get(id);
    if (queued) return queued.terminal ? structuredClone(queued.terminal) : this.#queuedInfo(queued);
    const previous = this.#previousRuns.get(id);
    if (previous && !this.#runs.has(id)) return structuredClone(previous);
    const managed = this.#requireRun(id);
    const record = managed.settled
      ? readRecord(managed.statusFile) ?? managed.latestRecord
      : managed.latestRecord ?? readRecord(managed.statusFile);
    if (!record) {
      const info = this.#handleInfo(managed, "running");
      const deliveries = this.#checkFollowUps(managed);
      if (deliveries.length) info.followUpDeliveries = deliveries;
      return info;
    }
    managed.latestRecord = record;
    if (!managed.latestUiRecord) {
      managed.latestUiRecord = compactUiRecord(record);
      this.#invalidateUiList();
    }
    const result = structuredClone(this.#withTransportMetadata(record, managed));
    this.#pruneRetainedUiRecords();
    const deliveries = this.#checkFollowUps(managed);
    if (deliveries.length) result.followUpDeliveries = deliveries;
    return result;
  }

  list(): Array<AgentRunRecord | AgentHandleInfo> {
    return [...this.#runs.keys(), ...this.#queued.keys()].map((id) => this.status(id));
  }

  listForUi(): Array<AgentRunRecord | AgentHandleInfo> {
    if (this.#uiListCache?.revision === this.#uiListRevision) {
      return this.#uiListCache.value;
    }
    const runs = [...this.#runs.values()];
    const active = runs.filter((managed) => !managed.settled);
    const settled = runs.filter((managed) => managed.settled);
    const retainedSettledCount = Math.max(0, MAX_RETAINED_UI_RUNS - active.length);
    const retainedSettled =
      retainedSettledCount > 0 ? settled.slice(-retainedSettledCount) : [];
    const visible = new Set([...active, ...retainedSettled]);
    const value = runs
      .filter((managed) => visible.has(managed))
      .map((managed) => {
        let record = managed.latestUiRecord;
        if (!record) {
          const latest = managed.latestRecord ?? readRecord(managed.statusFile);
          if (!latest) return this.#handleInfo(managed, "running");
          managed.latestRecord = latest;
          record = compactUiRecord(latest);
          managed.latestUiRecord = record;
        }
        return structuredClone(
          compactUiRecord(this.#withTransportMetadata(record, managed)),
        );
      });
    value.push(...[...this.#queued.keys()].map((id) => this.status(id)));
    this.#uiListCache = { revision: this.#uiListRevision, value };
    return value;
  }

  /** Full ownership references for retention, never the bounded UI/status snapshot.
   * A terminal status or abandonment is not worker-exit evidence. "*" vetoes
   * stopped-actor exit proofs when untracked ownership cannot be determined. */
  retentionReferences(): Set<string> {
    const refs = new Set<string>();
    const protect = (id: string, actorId?: string) => { refs.add(id); if (actorId) refs.add(actorId); };
    for (const queued of this.#queued.values()) {
      if (!queued.terminal || queued.cleanupPending || hasUnresolvedWorker(path.join(this.#runRoot, queued.info.id))) {
        protect(queued.info.id, queued.info.actorId);
      }
    }
    for (const managed of this.#runs.values()) {
      const pid = managed.transport.kind === "process" ? Number(managed.transport.sessionId) : undefined;
      const unconfirmedProcess = pid !== undefined && (!Number.isSafeInteger(pid) || pid <= 0 || processAlive(pid));
      if (!managed.settled || managed.processStopPending || managed.nativeReleasePending || managed.lostContact || managed.settlementSaveFailure || uncheckedExternalExit(managed.transport) ||
          // Settlement and primary exit do not prove descendant exit. The
          // persistent tree veto checks every descendant's worker identity too.
          unconfirmedProcess || runTreeExitVeto(managed.runDirectory, 0, undefined, true)) protect(managed.id, managed.actorId);
    }
    // A restarted host does not own handles for the previous host's actor workers.
    // Reuse offline retention's exit/ownership predicate; a truncated or unknown
    // tree vetoes all stopped-actor proofs, rather than guessing its association.
    const started = performance.now();
    const expired = () => performance.now() - started >= 5;
    let directory: fs.Dir | undefined;
    try {
      try { fs.lstatSync(this.#runRoot); }
      catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return refs; throw error; }
      if (!ownedStat(this.#runRoot)?.isDirectory()) { refs.add("*"); return refs; }
      directory = fs.opendirSync(this.#runRoot);
      let entry: fs.Dirent | null;
      while (!expired() && (entry = directory.readSync())) {
        if (this.#managedTempRoot && entry.name === ".fabric-owner.json") continue;
        if (this.#runs.has(entry.name) || this.#queued.has(entry.name)) continue;
        const run = path.join(this.#runRoot, entry.name);
        if (!entry.isDirectory()) { refs.add("*"); continue; }
        const status = ownedStat(path.join(run, "status.json"));
        const record = status?.isFile() && status.size <= 1024 * 1024 ? readRecord(path.join(run, "status.json")) : undefined;
        // Without a surviving handle, only a checked process identity can
        // establish exit; a terminal record with no PID is still uncertain.
        const pid = record?.transport === "process" && typeof record.sessionId === "string" && /^\d+$/.test(record.sessionId)
          ? Number(record.sessionId) : undefined;
        if (pid !== undefined && !processAlive(pid) && !runTreeExitVeto(run, 0, expired, true) && canRemoveTerminalRun(run, expired)) continue;
        const actorId = record?.actorId;
        if (typeof actorId === "string" && /^[A-Za-z0-9_-]+$/.test(actorId)) protect(entry.name, actorId);
        else refs.add("*");
      }
      if (expired()) refs.add("*");
    } catch { refs.add("*"); }
    finally { try { directory?.closeSync(); } catch { refs.add("*"); } }
    return refs;
  }

  runDirectory(id: string): string | undefined {
    return this.#runs.get(id)?.runDirectory;
  }

  worktreeGitRoot(id: string): string | undefined {
    return this.#worktrees.get(id)?.gitRoot;
  }

  async claudeModels(refresh = false): Promise<ClaudeModelInfo[]> {
    const now = Date.now();
    if (!refresh && this.#claudeModelsCache && now - this.#claudeModelsCache.at < 60_000) {
      return structuredClone(this.#claudeModelsCache.value);
    }
    const value = await discoverClaudeModels(this.#claudeBinary, this.cwd);
    this.#claudeModelsCache = { at: now, value };
    return structuredClone(value);
  }

  /** Runs a previous runtime of this session stopped; wait/status return them, not Unknown. */
  restorePreviousRuns(results: AgentRunResult[]): void {
    for (const result of results) {
      if (!this.#runs.has(result.id)) this.#previousRuns.set(result.id, structuredClone(result));
    }
  }

  /** Running (not settled) task agents: what a reload or shutdown would stop. */
  runningCount(): number {
    return [...this.#runs.values()].filter((managed) => !managed.settled).length +
      [...this.#queued.values()].filter((queued) => !queued.terminal).length;
  }

  #previousRun(id: string): AgentRunResult | undefined {
    const previous = this.#runs.has(id) ? undefined : this.#previousRuns.get(id);
    if (!previous) return undefined;
    return structuredClone(previous);
  }

  async stop(id: string): Promise<AgentRunResult> {
    const queued = this.#queued.get(id);
    if (queued) {
      queued.background = false;
      queued.abort.abort();
      await queued.pending;
      return queued.result;
    }
    const previous = this.#previousRuns.get(id);
    if (previous && !this.#runs.has(id)) return structuredClone(previous);
    const managed = this.#requireRun(id);
    // A requested stop is terminal: record the intent before any transport work
    // so recovery never restarts a run the operator, a tool, or shutdown ended.
    managed.stopRequested = true;
    if (managed.settled) {
      // A terminal result can be published just before native worker close.
      // Explicit process stop still owes its caller that exit join.
      if (managed.transport.kind === "process") await this.#stopManagedTransport(managed);
      return this.wait(id);
    }
    managed.background = false;
    const existing = readRecord(managed.statusFile);
    if (existing && terminalStatuses.has(existing.status)) {
      if (managed.transport.kind === "process") await this.#stopManagedTransport(managed);
      const result = this.#withTransportMetadata(existing, managed) as AgentRunResult;
      this.#settle(managed, result);
      return result;
    }
    await this.#stopManagedTransport(managed);
    await this.#waitForTransportExit(managed);
    await this.#noteUnconfirmedExit(managed);
    const terminal = readRecord(managed.statusFile);
    // A force-killed worker (notably on Windows) may leave only running status.
    // Its session telemetry can be newer than the monitor's last poll. Preserve
    // that snapshot, or the pre-stop one if stopping removed the status file,
    // before synthesizing a terminal result.
    const observed = terminal ?? existing;
    if (observed) {
      managed.latestRecord = observed;
      if (observed.runnerSessionId) managed.runnerSessionId = observed.runnerSessionId;
    }
    // Stop intent and tree custody are separate: uncertainty keeps the worker's
    // files and native admission fenced, but does not turn an explicit stop into
    // a worker failure. Preserve a genuine terminal result when one exists.
    const record =
      terminal && terminalStatuses.has(terminal.status)
        ? (this.#withTransportMetadata(terminal, managed) as AgentRunResult)
        : failedRecord(managed, "stopped", "Agent stopped");
    if (!terminal || !terminalStatuses.has(terminal.status)) writeRecord(managed.statusFile, record);
    this.#settle(managed, record);
    return record;
  }

  async cleanup(id: string, deleteBranch = false): Promise<{ cleaned: boolean }> {
    const queued = this.#queued.get(id);
    if (queued) {
      if (!queued.terminal) throw new Error("Cannot clean up a queued agent");
      const runDirectory = path.join(this.#runRoot, id);
      if (queued.cleanupPending || hasUnresolvedWorker(runDirectory)) {
        throw new Error(`Cannot clean up agent ${id}: Fabric lost track of its worker; check ${runDirectory} before removing its files`);
      }
      if (!this.#saveQueuedRouteOutcome(queued)) throw new Error(`Cannot clean up agent ${id}: ${queued.routeSaveFailure}`);
      const exitVeto = runTreeExitVeto(runDirectory);
      if (exitVeto) throw new Error(`Cannot clean up agent ${id}: ${exitVeto}`);
      this.#onResultConsumed?.(id);
      const cleaned = await this.#worktrees.cleanup(id, deleteBranch);
      if (!this.config.retainRuns) await removeTree(runDirectory);
      this.#queued.delete(id);
      this.#invalidateUiList();
      return { cleaned: cleaned || !fs.existsSync(runDirectory) };
    }
    const managed = this.#requireRun(id);
    if (!managed.settled) throw new Error("Cannot clean up a running agent");
    if (managed.processStopPending) {
      throw new Error(`Cannot clean up agent ${id}: process teardown is pending`);
    }
    // A normal Windows terminal result can precede captured native close.
    // Join its existing bounded obligation instead of exposing that incidental
    // ordering as a cleanup failure. Expiry records uncertainty, not exit proof.
    if (managed.nativeReleasePending) await managed.nativeReleasePending;
    // A stop may have acquired tree custody during the join. Recheck every
    // pending/uncertain fence before authorizing worktree or run collection.
    if (managed.processStopPending || managed.nativeReleasePending) {
      throw new Error(`Cannot clean up agent ${id}: process teardown is pending`);
    }
    if (managed.lostContact || hasUnresolvedWorker(managed.runDirectory)) {
      throw new Error(
        `Cannot clean up agent ${id}: Fabric lost track of its worker (${managed.lostContact ?? "see its run directory"}), ` +
        `which may still use ${managed.runDirectory}. Check the worker, then remove its files by hand.`,
      );
    }
    // On POSIX too, the worker can publish terminal status before exiting.
    // Explicit cleanup must join that bounded obligation, not treat the result
    // as exit proof or expose the incidental file/close ordering as a failure.
    if (managed.transport.kind === "process") {
      await this.#waitForTransportExit(managed);
      await this.#noteUnconfirmedExit(managed);
      if (managed.lostContact) {
        throw new Error(`Cannot clean up agent ${id}: worker exit is unconfirmed (${managed.lostContact})`);
      }
    }
    const exitVeto = runTreeExitVeto(managed.runDirectory, 0, undefined, true);
    if (exitVeto) throw new Error(`Cannot clean up agent ${id}: ${exitVeto}`);
    if (!this.#canCollect(managed)) {
      throw new Error(`Cannot clean up agent ${id}: ${uncheckedExternalExit(managed.transport) ? "external transport has no checked worker exit receipt" : managed.settlementSaveFailure?.warning ?? "terminal result is not durably preserved"}`);
    }
    this.markForeground(id);
    const cleaned = await this.#worktrees.cleanup(id, deleteBranch);
    if (!this.config.retainRuns) {
      await removeTree(managed.runDirectory);
    }
    this.#runs.delete(id);
    for (const messageId of this.#followUps.get(id)?.keys() ?? []) this.#clearFollowUpTimer(messageId);
    this.#followUps.delete(id);
    this.#pruneRetainedUiRecords();
    this.#invalidateUiList();
    return { cleaned: cleaned || !fs.existsSync(managed.runDirectory) };
  }

  readLog(id: string, opts: { lines?: number; before?: number; beforeGeneration?: string } = {}): FabricAgentLog {
    const managed = this.#requireRun(id);
    const runDirectory = managed.runDirectory;
    const logFile = path.join(runDirectory, "events.jsonl");
    const lines = Math.max(1, Math.min(opts.lines ?? 200, 5000));
    const page = readJsonlPage(logFile, lines, opts.before, undefined, opts.beforeGeneration);
    const statusRecord = readRecord(path.join(runDirectory, "status.json"));
    return {
      id,
      runDirectory,
      logFile,
      events: page.lines,
      hasMore: page.hasMore,
      ...(page.before !== undefined ? { before: page.before } : {}),
      ...(page.generation !== undefined ? { generation: page.generation } : {}),
      ...(statusRecord ? { status: { ...statusRecord, cwd: managed.cwd } } : {}),
    };
  }

  /** Automatic outputs must use admitted task lineage, never the original mailbox item. */
  outputPrincipal(id: string): FabricPrincipal | undefined {
    return copyFabricPrincipal(this.#runs.get(id)?.outputPrincipal);
  }

  steer(id: string, message: string, data?: unknown, provenance?: FabricTurnProvenance): AgentSteerResult {
    this.#requireSteerable(id);
    return this.#appendSteer(id, { type: "steer", message, data, provenance });
  }

  followUp(id: string, message: string, data?: unknown, provenance?: FabricTurnProvenance,
    options?: { deadlineMs: number }): AgentSteerResult {
    this.#requireSteerable(id);
    if (!options) return this.#appendSteer(id, { type: "follow_up", message, data, provenance });
    const managed = this.#requireRun(id);
    if (managed.runner !== "pi") throw new Error("Delivery deadlines require a local Pi task agent");
    if (!Number.isSafeInteger(options.deadlineMs) || options.deadlineMs < 1) throw new Error("deadlineMs must be a positive safe integer");
    const messageId = randomUUID();
    const deadlineAt = Date.now() + options.deadlineMs;
    if (!Number.isSafeInteger(deadlineAt)) throw new Error("deadlineMs exceeds the safe timestamp range");
    const file = followUpFile(managed.runDirectory, messageId);
    fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
    fs.writeFileSync(file, JSON.stringify({ messageId, deadlineAt }), { mode: 0o600 });
    let receipt: AgentSteerResult;
    try { receipt = this.#appendSteer(id, { type: "follow_up", message, data, provenance, followUpId: messageId, deadlineAt }); }
    catch (error) { fs.unlinkSync(file); throw error; }
    const entries = this.#followUps.get(id) ?? new Map<string, AgentFollowUpDelivery>();
    entries.set(messageId, { messageId, deadlineAt, state: "queued" });
    this.#followUps.set(id, entries);
    this.#armFollowUpDeadline(managed, entries.get(messageId)!);
    return { ...receipt, messageId, deadlineAt };
  }

  cancelFollowUp(id: string, messageId: string): AgentFollowUpDelivery {
    const managed = this.#requireRun(id);
    const delivery = this.#followUps.get(id)?.get(messageId);
    if (!delivery) throw new Error(`Unknown follow-up: ${messageId}`);
    delivery.state = settleFollowUp(followUpFile(managed.runDirectory, messageId), "cancelled");
    releaseFollowUpPayload(followUpFile(managed.runDirectory, messageId));
    if (delivery.state === "delivered" || delivery.state === "cancelled") this.#clearFollowUpTimer(messageId);
    return structuredClone(delivery);
  }

  #clearFollowUpTimer(messageId: string): void {
    clearTimeout(this.#followUpTimers.get(messageId));
    this.#followUpTimers.delete(messageId);
  }

  #armFollowUpDeadline(managed: ManagedAgent, delivery: AgentFollowUpDelivery): void {
    // Only message admission starts a timer. Keep it after receiver settlement,
    // but never past sender teardown; long deadlines must not overflow setTimeout.
    const timer = setTimeout(() => {
      this.#followUpTimers.delete(delivery.messageId);
      if (Date.now() < delivery.deadlineAt) this.#armFollowUpDeadline(managed, delivery);
      else this.#checkFollowUps(managed);
    }, Math.max(0, Math.min(delivery.deadlineAt - Date.now(), 2_147_483_647)));
    timer.unref();
    this.#followUpTimers.set(delivery.messageId, timer);
  }

  #checkFollowUps(managed: ManagedAgent, record = readRecord(managed.statusFile)): AgentFollowUpDelivery[] {
    const entries = this.#followUps.get(managed.id);
    if (!entries) return [];
    for (const delivery of entries.values()) {
      delivery.state = followUpState(followUpFile(managed.runDirectory, delivery.messageId));
      if (delivery.state === "delivered" || delivery.state === "cancelled" || delivery.alarm) { this.#clearFollowUpTimer(delivery.messageId); continue; }
      if (Date.now() < delivery.deadlineAt) continue;
      const alarm: AgentFollowUpAlarm = {
        code: "FABRIC_FOLLOW_UP_DEADLINE", messageId: delivery.messageId,
        targetId: managed.id, targetName: managed.name, deadlineAt: delivery.deadlineAt,
        status: record?.status ?? "running",
        ...(record?.currentTool ? { currentTool: record.currentTool, currentToolStartedAt: record.currentToolStartedAt } : {}),
        options: ["wait", "steer", "cancel"],
        message: `Follow-up ${delivery.messageId} to ${managed.name} (${managed.id}) missed its delivery deadline; ` +
          `${record?.currentTool ? `busy in tool ${record.currentTool} since ${new Date(record.currentToolStartedAt ?? record.updatedAt).toISOString()}` : record?.status ?? "running"}. ` +
          `${delivery.state === "settling" ? "Delivery remains uncertain and fenced." : "It remains queued."} Wait, use agents.steer, or agents.cancelFollowUp({ id: '${managed.id}', messageId: '${delivery.messageId}' }).`,
      };
      delivery.alarm = alarm; // Mark before calling observers: one alarm, even under reentrant status.
      this.#clearFollowUpTimer(delivery.messageId);
      try { this.#onFollowUpAlarm?.(structuredClone(alarm)); } catch { /* Status retains the sender alarm. */ }
    }
    return structuredClone([...entries.values()]);
  }

  // Veda children run one headless prompt per invocation; there is no stdin
  // turn channel to steer into. Reject steer/follow-up here so callers learn
  // at call time instead of the command being silently dropped by the worker.
  #requireSteerable(id: string): void {
    if (this.#requireRun(id).runner === "veda") {
      throw new Error(
        "The Veda runner does not support steering or follow-ups: Veda executes one headless prompt per invocation. Start a new run instead.",
      );
    }
  }

  setSteeringMode(id: string, mode: FabricSteeringMode): AgentSteerResult {
    return this.#appendSteer(id, { type: "set_steering_mode", mode });
  }

  setFollowUpMode(id: string, mode: FabricSteeringMode): AgentSteerResult {
    return this.#appendSteer(id, { type: "set_follow_up_mode", mode });
  }

  // Request an advisory compaction of a running Pi-runner child's context.
  // Appended to the same steer.jsonl channel as steer(); the worker queues it
  // until child agent_settled, then correlates Pi's compact response and
  // compaction_end before closing the one-shot RPC channel. Rejected for
  // Claude-runner children — the official Claude Code CLI exposes no compact
  // RPC; a fresh run is the only way to reset a Claude child's context.
  compact(id: string, instructions?: string): AgentSteerResult {
    const managed = this.#requireRun(id);
    if (managed.runner === "claude" || managed.runner === "veda") {
      throw new Error(
        "Fabric agent compaction is only supported for Pi-runner children; Claude Code and Veda sessions cannot be compacted through Fabric.",
      );
    }
    return this.#appendSteer(id, {
      type: "compact",
      ...(typeof instructions === "string" && instructions ? { instructions } : {}),
    });
  }

  #appendSteer(id: string, entry: Omit<AgentSteerEntry, "id" | "ts">): AgentSteerResult {
    const managed = this.#requireRun(id);
    const record = readRecord(managed.statusFile);
    if (record && terminalStatuses.has(record.status)) {
      throw new Error(
        `Fabric agent ${id} already finished (${record.status}); steering has no target`,
      );
    }
    const steerFile = path.join(managed.runDirectory, "steer.jsonl");
    const messageId = entry.followUpId ?? randomUUID();
    const line = JSON.stringify({ ...entry, id: messageId, ts: Date.now() }) + "\n";
    if (entry.type === "steer" || entry.type === "follow_up") {
      const incoming = copyFabricPrincipal(entry.provenance?.principal);
      if (!incoming || incoming.id !== managed.outputPrincipal?.id || incoming.binding !== managed.outputPrincipal?.binding) {
        managed.outputPrincipal = undefined;
        // A failed fence rejects ingress. Retry it even after an in-memory downgrade:
        // an earlier persistence failure must not let the next input bypass the fence.
        managed.onOutputPrincipalDowngrade?.();
      }
    }
    fs.appendFileSync(steerFile, line, { encoding: "utf8", mode: 0o600 });
    return {
      queued: true,
      messageId,
      ...(entry.type === "follow_up" && record?.status === "running" ? { warning: {
        code: "FABRIC_FOLLOW_UP_RUNNING_TASK" as const,
        targetId: managed.id,
        kind: "agent" as const,
        status: "running" as const,
        message: FOLLOW_UP_RUNNING_TASK_MESSAGE,
      } } : {}),
    };
  }

  /** Non-destructive receipt for a paused resident release boundary. A terminal
   * UI record is not a worker exit. Unknown launches or saved trees veto custody.
   */
  async checkpointForRelease(deadline = Date.now() + TRANSPORT_EXIT_GRACE_MS * 7): Promise<void> {
    const obligations = (): boolean => this.#closing || this.#spawns.size > 0 || this.#launches.size > 0 ||
      this.#queuedStarts.size > 0 || [...this.#queued.values()].some(q => !q.terminal || q.cleanupPending !== undefined) ||
      [...this.#runs.values()].some(run => run.processStopPending || run.nativeReleasePending);
    if (obligations()) throw new Error("Agent release quiescence has pending launch/cleanup obligations");
    const runs = [...this.#runs.values()];
    if (runs.some(run => !run.settled || run.lostContact || run.settlementSaveFailure || hasUnresolvedWorker(run.runDirectory))) {
      throw new Error("Agent release quiescence has an unresolved worker/result");
    }
    const transports = [...runs.map(run => run.transport), ...this.#unregisteredTransports];
    if (transports.some(uncheckedExternalExit)) {
      throw new Error("Agent release quiescence has no checked tmux/screen worker exit contract; retain A and worker files");
    }
    // Bound the observation itself, not just the interval between polls.
    await Promise.all(runs.map(run => this.#waitForTransportExit(run, deadline)));
    if (runs.some(run => run.lostContact)) throw new Error("Agent release quiescence has an unconfirmed worker exit observation");
    for (const transport of transports) {
      if (await this.#transportAliveUntil(transport, deadline) || transport.lostContact?.() !== undefined) {
        throw new Error("Agent release quiescence cannot confirm worker exit");
      }
    }
    // Check preserved trees from previous hosts too. Reuse the conservative
    // retention predicate without removing anything. Unknown external transport
    // identities have no surviving handle, so are deliberately out of scope.
    const started = performance.now();
    const expired = () => performance.now() - started > 100;
    const inspect = (directory: string, tracked: boolean, depth = 0): void => {
      if (expired() || depth > 32 || !canRemoveTerminalRun(directory, expired)) {
        throw new Error(`Agent release quiescence has an unresolved run tree: ${directory}`);
      }
      if (!tracked) {
        const record = readRecord(path.join(directory, "status.json"));
        if (record?.transport !== "process" || typeof record.sessionId !== "string" || !/^\d+$/.test(record.sessionId) || Number(record.sessionId) <= 0) {
          throw new Error(`Agent release quiescence has unknown worker identity: ${directory}`);
        }
      }
      const nested = path.join(directory, "nested");
      if (fs.existsSync(nested)) for (const name of fs.readdirSync(nested)) inspect(path.join(nested, name), false, depth + 1);
    };
    let entries: fs.Dirent[];
    try { entries = fs.readdirSync(this.#runRoot, { withFileTypes: true }); }
    catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT" || runs.length) throw error;
      entries = [];
    }
    for (const entry of entries) {
      if (this.#managedTempRoot && entry.name === ".fabric-owner.json") continue;
      if (!entry.isDirectory()) throw new Error("Agent release quiescence has unknown run-root contents");
      inspect(path.join(this.#runRoot, entry.name), this.#runs.has(entry.name));
    }
    if (obligations()) throw new Error("Agent release quiescence changed while checking workers");
  }

  close(): Promise<void> {
    this.#closing = true;
    for (const timer of this.#followUpTimers.values()) clearTimeout(timer);
    this.#followUpTimers.clear();
    this.#closeAbort.abort(new Error("Fabric agent manager is closing"));
    return this.#closePromise ??= this.#close();
  }

  async #close(): Promise<void> {
    const queuedAtClose = [...this.#queued.values()].filter((queued) => !queued.terminal);
    this.#uiListeners.clear();
    if (this.#retentionTimer) clearInterval(this.#retentionTimer);
    this.#retentionTimer = undefined;
    await this.#retentionSweep?.catch(() => undefined);
    const running = [...this.#runs.values()].filter((managed) => !managed.settled);
    const lastEventAt = new Map(running.map((managed) => [managed.id, lastEventTime(managed)]));
    const stopped = await Promise.allSettled([
      ...running.map((managed) => this.stop(managed.id)),
      ...queuedAtClose.map((queued) => this.stop(queued.info.id)),
    ]);
    // A reload or shutdown ends these runs; tell the spawner's session (smarty-dev#1602).
    const results = stopped.flatMap((outcome) =>
      outcome.status === "fulfilled" ? [hostStoppedResult(outcome.value, lastEventAt.get(outcome.value.id))] : []);
    if (results.length > 0) {
      try { this.#onStoppedAtClose?.(results); } catch { /* must not block close */ }
    }
    await Promise.allSettled([...this.#spawns]);
    await Promise.allSettled([...this.#queuedStarts]);
    await Promise.allSettled([...this.#launches]);
    for (const queued of this.#queued.values()) this.#saveQueuedRouteOutcome(queued);
    const all = [...this.#runs.values()];
    await Promise.allSettled(all.flatMap(managed => [managed.processStop, managed.nativeReleasePending]));
    await Promise.allSettled(all.map((managed) => this.#waitForTransportExit(managed)));
    const transports = [...all.map((managed) => managed.transport), ...this.#unregisteredTransports];
    // Lost contact is not an exit: such a worker may still use its files.
    const observationDeadline = Date.now() + TRANSPORT_EXIT_GRACE_MS * 7;
    const alive = await Promise.all(transports.map((transport) =>
      uncheckedExternalExit(transport) ? true :
        this.#transportAliveUntil(transport, observationDeadline).then((alive) => alive || transport.lostContact?.() !== undefined).catch(() => true)));
    // Primary transport exit cannot release a surviving descendant's files or
    // shared budget. Keep the persistent run tree (and its owning actor ID) for
    // the next fenced owner whenever tree-wide exit evidence is incomplete.
    const unresolved = all.some((managed) => managed.processStopPending || managed.nativeReleasePending || managed.lostContact || runTreeExitVeto(managed.runDirectory, 0, undefined, true)) ||
      [...this.#queued.values()].some((queued) => queued.cleanupPending || queued.routeSaveFailure) || runRootHasExitVeto(this.#runRoot);
    // A failed stop is not authority to delete a child's working files.
    if (!alive.some(Boolean) && !unresolved) {
      this.#unregisteredTransports.clear();
      const storageSafe = !this.#managedTempRoot || canRemoveManagedRunRoot(this.#runRoot);
      if (!this.config.retainRuns) {
        if (storageSafe) {
          // A recovered manager does not own workers left by an earlier host.
          // All tracked transports are confirmed exited above; untracked runs stay put.
          await Promise.all(all.filter((managed) => this.#canCollect(managed))
            .map((managed) => removeTree(managed.runDirectory).catch(() => undefined)));
          try {
            if (this.#managedTempRoot && fs.readdirSync(this.#runRoot).every((name) => name === ".fabric-owner.json")) {
              fs.unlinkSync(path.join(this.#runRoot, ".fabric-owner.json"));
            }
            fs.rmdirSync(this.#runRoot); // Never recursively remove an untracked directory.
          } catch { /* nonempty, missing, or unsafe roots are retained */ }
        }
      } else if (this.#managedTempRoot) {
        try { markRunRootClosed(this.#runRoot, Date.now(), true); } catch {}
        removeEmptyRunRoot(this.#runRoot);
      }
      if (storageSafe && this.#budgetOwned && this.#budget) {
        await removeTree(path.dirname(this.#budget.file)).catch(() => undefined);
      }
    }
    if (this.#budgetOwned) clearOwnedBudgetEnv();
    if (this.#managedTempRoot) await this.#startTempRunSweep();
  }

  /**
   * Walking every retained run on the host is slow (about 60 runs/s at load 130 on Dev1), so it
   * runs in a detached, niced process: neither this exit nor a live event loop waits for it, and
   * one unbounded sweep per interval per host keeps up with creation (smarty-dev#2010).
   */
  async #startTempRunSweep(): Promise<void> {
    const request: TempRunSweepRequest = {
      tempRoot: path.dirname(this.#runRoot),
      currentRoot: this.#runRoot,
      orphanedTempRunRetentionMs: this.#retention.orphanedTempRunMs,
      oneShotRunRetentionMs: this.#retention.oneShotRunMs,
      terminalRunEventsAgeMs: this.#retention.terminalRunEventsAgeMs,
      terminalRunEventsMaxBytes: this.#retention.terminalRunEventsMaxBytes,
    };
    try {
      // A Bun-compiled Pi's execPath is Pi itself: resolve a real node/bun (the override, then PATH)
      // as every other detached launch does. Resolve before the claim, so a host that cannot run
      // the sweep does not suppress the next attempt for a whole interval.
      const [runtime, ...args] = await scriptSpawnArgs(this.#sweepPath, [JSON.stringify(request)]);
      if (!claimTempRunSweep(request.tempRoot, RETENTION_SWEEP_INTERVAL_MS)) return;
      const child = spawn(runtime!, args, {
        detached: true, stdio: "ignore", windowsHide: true,
      });
      child.on("error", () => undefined);
      if (child.pid) {
        try { os.setPriority(child.pid, 19); } catch { /* best effort */ }
      }
      child.unref();
    } catch {
      // The next close or interval sweeps.
    }
  }

  #scheduleRetentionSweep(): void {
    if (this.#closing || this.#retentionSweep) return;
    this.#retentionSweep = new Promise<void>((resolve) => setImmediate(resolve))
      .then(() => this.#closing ? undefined : this.#runRetentionSweep()).catch(() => undefined).finally(() => {
      this.#retentionSweep = undefined;
    });
  }

  async #runRetentionSweep(now = Date.now()): Promise<void> {
    if (this.#managedTempRoot) {
      heartbeatRunRoot(this.#runRoot, now);
      await this.#startTempRunSweep();
    }
    const expired = [...this.#runs.values()].filter((managed) => {
      if (!managed.settled || managed.actorId || managed.processStopPending || managed.nativeReleasePending || managed.lostContact || hasUnresolvedWorker(managed.runDirectory)) return false;
      const record = readRecord(managed.statusFile) ?? managed.latestRecord;
      const finishedAt = record?.finishedAt ?? record?.updatedAt;
      return typeof finishedAt === "number" && now - finishedAt >= this.#retention.oneShotRunMs;
    });
    for (const managed of expired) {
      if (!this.#canCollect(managed)) continue;
      // Retry settlement saves first; pending deliveries and unsafe contents still veto expiry.
      if (!canRemoveTerminalRun(managed.runDirectory)) continue;
      await removeTree(managed.runDirectory).catch(() => undefined);
      if (!fs.existsSync(managed.runDirectory)) this.#runs.delete(managed.id);
    }
    if (expired.length > 0) {
      this.#pruneRetainedUiRecords();
      this.#invalidateUiList();
    }
  }

  /** A cancelled launch has no ManagedAgent yet. Bound stop AND liveness calls,
   * including hung RPCs; false/lost-contact and failed probes are not proven exits. */
  async #stopUnregisteredTransport(transport: AgentTransportHandle): Promise<boolean> {
    const deadline = Date.now() + TRANSPORT_EXIT_GRACE_MS * 7;
    const bounded = async <T>(operation: () => Promise<T>): Promise<T> => {
      const remaining = deadline - Date.now();
      if (remaining <= 0) throw new Error("Worker exit confirmation timed out");
      let timer: ReturnType<typeof setTimeout> | undefined;
      try {
        return await Promise.race([
          operation(),
          new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new Error("Worker exit confirmation timed out")), remaining); }),
        ]);
      } finally { if (timer) clearTimeout(timer); }
    };
    // A failed close request can still be followed by a proven exit.
    await bounded(() => transport.stop()).catch(() => undefined);
    if (uncheckedExternalExit(transport)) return false;
    try {
      while (Date.now() < deadline) {
        const alive = await bounded(() => transport.isAlive());
        if (transport.lostContact?.() !== undefined) return false;
        if (!alive) return true;
        await delay(Math.min(transport.livenessPollIntervalMs ?? AGENT_STATUS_POLL_INTERVAL_MS, deadline - Date.now()));
      }
    } catch { /* failed or hung liveness never authorizes deletion */ }
    return false;
  }

  /** Publish the join before calling stop: a settled result and worker absence
   * cannot erase the outstanding Windows tree-helper obligation. */
  #stopManagedTransport(managed: ManagedAgent): Promise<void> {
    if (managed.transport.kind !== "process") return managed.transport.stop();
    if (managed.processStop) return managed.processStop;
    managed.processStopPending = true;
    let resolve!: () => void;
    let reject!: (error: unknown) => void;
    managed.processStop = new Promise<void>((done, fail) => { resolve = done; reject = fail; });
    // Install the promise before stop can synchronously emit native events.
    void (async () => {
      try {
        await managed.transport.stop();
        await this.#noteUnconfirmedExit(managed);
        managed.processStopPending = false;
        resolve();
      } catch (error) {
        this.#markLost(managed, error instanceof Error ? error.message : String(error));
        managed.processStopPending = false;
        reject(error);
      }
    })();
    return managed.processStop;
  }

  // After a stop: a worker whose exit is not confirmed (lost contact, or still reported
  // alive) may keep using its files, so the run is marked unresolved (never cleaned up).
  async #noteUnconfirmedExit(managed: ManagedAgent): Promise<void> {
    if (managed.lostContact) return;
    const lost = uncheckedExternalExit(managed.transport) ? "external transport has no checked exit contract" : managed.transport.lostContact?.();
    const alive = lost === undefined && await this.#transportAliveUntil(managed.transport, Date.now() + TRANSPORT_EXIT_GRACE_MS * 7).catch(() => true);
    if (lost === undefined && !alive) return;
    this.#markLost(managed, lost ?? "the worker did not confirm its exit after it was stopped");
  }

  #markLost(managed: ManagedAgent, reason: string): void {
    managed.lostContact = reason;
    try {
      markUnresolvedWorker(managed.runDirectory, reason, {
        runId: managed.id,
        transport: managed.transport.kind,
        ...(managed.transport.sessionId ? { sessionId: managed.transport.sessionId } : {}),
      });
    } catch { /* the in-memory mark still guards this manager */ }
  }

  async #transportAliveUntil(transport: AgentTransportHandle, deadline: number): Promise<boolean> {
    const remaining = deadline - Date.now();
    if (remaining <= 0) throw new Error("Worker exit observation deadline expired");
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      return await Promise.race([transport.isAlive(), new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error("Worker exit observation timed out")), remaining);
      })]);
    } finally { if (timer) clearTimeout(timer); }
  }

  async #waitForTransportExit(managed: ManagedAgent, outerDeadline = Infinity): Promise<void> {
    // Never issue a potentially hung query that cannot supply exit proof.
    if (uncheckedExternalExit(managed.transport)) return;
    const deadline = Math.min(outerDeadline, Date.now() + TRANSPORT_EXIT_GRACE_MS * 7);
    const pollIntervalMs = managed.transport.livenessPollIntervalMs ?? AGENT_STATUS_POLL_INTERVAL_MS;
    try {
      while (Date.now() < deadline && await this.#transportAliveUntil(managed.transport, deadline)) {
        await delay(Math.min(pollIntervalMs, Math.max(0, deadline - Date.now())));
      }
    } catch (error) {
      this.#markLost(managed, error instanceof Error ? error.message : String(error));
    }
  }

  async #retryStartup(
    managed: ManagedAgent,
    record: AgentRunRecord,
    deadline: number,
  ): Promise<boolean> {
    if (
      managed.launch.workerArguments.includes("--judgment") ||
      managed.transport.relaunchable === false ||
      managed.startupAttempts >= AGENT_STARTUP_MAX_ATTEMPTS ||
      managed.settled ||
      this.#closing ||
      managed.abortSignal?.aborted ||
      managed.abandoned ||
      record.status !== "failed" ||
      // Window admission is terminal even when stderr also contains an auth
      // miss or transport-death diagnostic. Never launch a second Pi for it.
      /Context exceeds window:/i.test(record.error ?? "") ||
      !(
        (managed.runner === "pi" && retryablePiStartupError(record.error)) ||
        transportExitedWithoutResult(record.error)
      ) ||
      record.turns !== 0 ||
      record.toolCalls !== 0 ||
      record.usage.input !== 0 ||
      record.usage.output !== 0 ||
      record.usage.cacheRead !== 0 ||
      record.usage.cacheWrite !== 0
    ) {
      return false;
    }
    const retryDelayMs =
      AGENT_STARTUP_RETRY_BASE_DELAY_MS * 2 ** (managed.startupAttempts - 1);
    if (Date.now() + retryDelayMs >= deadline) return false;
    await this.#waitForTransportExit(managed);
    await delay(retryDelayMs);
    if (managed.settled || this.#closing || managed.abortSignal?.aborted || managed.abandoned) return false;
    managed.startupAttempts++;
    return this.#relaunch(managed, record);
  }

  /**
   * An unexpected mid-run stop — the worker caught a signal, or its transport
   * died with work already done — is recoverable. Relaunch the same run in the
   * same directory, seeded with the cumulative prefix of the attempt it lost, so
   * a long participant finishes instead of reporting a terminal stop that throws
   * away hours of work. Explicit stops, timeouts, and spent deadlines stay
   * terminal; AGENT_RESUME_MAX_ATTEMPTS bounds the retries.
   */
  async #resumeStopped(
    managed: ManagedAgent,
    record: AgentRunRecord,
    deadline: number,
  ): Promise<boolean> {
    if (
      managed.launch.workerArguments.includes("--judgment") ||
      managed.transport.relaunchable === false ||
      managed.settled ||
      this.#closing ||
      managed.stopRequested ||
      managed.abandoned ||
      managed.resumeAttempts >= AGENT_RESUME_MAX_ATTEMPTS ||
      !this.#observedWork(managed) ||
      /Context exceeds window:/i.test(record.error ?? "") ||
      !recoverableStop(record)
    ) {
      return false;
    }
    const retryDelayMs = AGENT_RESUME_RETRY_BASE_DELAY_MS * 2 ** managed.resumeAttempts;
    if (Date.now() + retryDelayMs >= deadline) return false;
    await this.#waitForTransportExit(managed);
    await delay(retryDelayMs);
    if (managed.settled || this.#closing || managed.stopRequested || managed.abandoned) return false;
    managed.resumeAttempts += 1;
    const { turns, toolCalls, usage } = managed.observedProgress;
    return this.#relaunch(managed, record, {
      task: resumeTask(managed.task, record, { turns, toolCalls }, managed.runDirectory),
      carryOver: { turns, toolCalls, usage: { ...usage } },
    });
  }

  /**
   * Relaunch the same worker run in place. Shared by the startup retry (a child
   * that died before producing a result) and the mid-run resume, which differ
   * only in the task the child is handed and the cumulative prefix its fresh
   * record starts from.
   */
  async #relaunch(
    managed: ManagedAgent,
    record: AgentRunRecord,
    resume?: { task: string; carryOver: AgentRunCarryOver },
  ): Promise<boolean> {
    try {
      if (managed.runner === "pi") {
        const model = await this.prepareModelForAdmission(managed.routePin?.model ?? managed.model, managed.runner, undefined, Boolean(managed.routePin));
        if (managed.routePin) setWorkerArgument(managed.launch.workerArguments, "thinking", managed.routePin.effort);
        const modelIndex = managed.launch.workerArguments.indexOf("--model");
        if (model) {
          if (modelIndex >= 0) managed.launch.workerArguments[modelIndex + 1] = model;
          else managed.launch.workerArguments.push("--model", model);
          managed.model = model;
        } else if (modelIndex >= 0) {
          managed.launch.workerArguments.splice(modelIndex, 2);
          delete managed.model;
        }
      }
      if (resume) {
        fs.writeFileSync(path.join(managed.runDirectory, "task.txt"), resume.task, {
          encoding: "utf8",
          mode: 0o600,
        });
        setWorkerArgument(
          managed.launch.workerArguments,
          "carry-over",
          JSON.stringify(resume.carryOver),
        );
      }
      // Never run two workers for one run (smarty-dev#347): stop the previous
      // transport and wait for it to exit before starting the next. After a real
      // exit this is a no-op; if liveness was misjudged (a dropped transport call),
      // it ends the old worker instead of racing it on the same task.
      // ponytail: the stop is unconditional because a misjudged liveness check is the
      // case it exists for. A process worker that really exited is not signalled: its
      // transport saw the exit and never signals a numeric id that may be reused.
      const previousSession = managed.transport.sessionId;
      await managed.transport.stop().catch(() => undefined);
      await this.#waitForTransportExit(managed);
      if (managed.settled || this.#closing || managed.stopRequested || managed.abandoned) return false;
      // Relaunch only when the previous worker is gone for certain. A worker that did
      // not stop, or whose transport cannot say, fails the run instead of running twice.
      if (managed.lostContact || managed.transport.lostContact?.() !== undefined || await managed.transport.isAlive().catch(() => true)) {
        const reason = `the previous worker ${previousSession ? `(${previousSession}) ` : ""}did not stop, so it was not relaunched`;
        this.#markLost(managed, reason);                       // it may still use its files
        throw new Error(reason);
      }
      // Keep an append-only record of every relaunch; the status and lifecycle
      // files below are replaced by the new attempt.
      fs.appendFileSync(
        path.join(managed.runDirectory, "relaunches.jsonl"),
        `${JSON.stringify({
          at: Date.now(),
          kind: resume ? "resume" : "startup-retry",
          startupAttempts: managed.startupAttempts,
          resumeAttempts: managed.resumeAttempts,
          previousStatus: record.status,
          ...(record.error ? { previousError: record.error.slice(0, 500) } : {}),
          ...(previousSession ? { previousTransportSession: previousSession } : {}),
        })}\n`,
        { encoding: "utf8", mode: 0o600 },
      );
      // The relaunched child owns a fresh status/lifecycle pair, so drain what
      // the previous attempt published (token usage above all) before discarding
      // the journal it landed in.
      this.#drainLifecycle(managed);
      // Once this attempt is superseded, a supervisor crash must not promote its old failure
      // while the already-launched retry is still running. The previous worker has exited.
      if (this.#meshRoot) discardWorkerCompletion(this.#meshRoot, managed.id);

      if (managed.runner === "pi") {
        // status.json must be removed to fence the new attempt from the old
        // terminal verdict. Hand off its native-session joins separately, after
        // confirmed exit so the previous worker's final observations are included.
        const ids = [record, managed.latestRecord, readRecord(managed.statusFile)]
          .filter((prior): prior is AgentRunRecord => prior?.id === managed.id)
          .flatMap(prior => [...(Array.isArray(prior.runnerSessionIds) ? prior.runnerSessionIds : []), prior.runnerSessionId])
          .filter((id): id is string => typeof id === "string" && Boolean(id.trim()));
        if (ids.length) {
          setWorkerArgument(managed.launch.workerArguments, "runner-session-ids", JSON.stringify([...new Set(ids)]));
        }
      }

      fs.rmSync(managed.statusFile, { force: true });
      if (managed.settled || this.#closing || managed.stopRequested || managed.abandoned) return false;
      managed.transport = await this.#launchTransport(managed.adapter, managed.launch);
      this.#unregisteredTransports.delete(managed.transport);
      // A later launch succeeded: an earlier relaunch failure no longer describes this run.
      delete managed.relaunchFailure;
      if (managed.settled || this.#closing || managed.stopRequested || managed.abandoned) {
        // A stop (or an abandonment, #2184 8b) landed while the relaunch was in flight.
        // Release the child we just started so it cannot outlive the monitor and the
        // stop path can publish its terminal record.
        await managed.transport.stop().catch(() => undefined);
        return false;
      }
      delete managed.latestRecord;
      delete managed.latestUiRecord;
      managed.lastLivenessCheckAt = 0;
      managed.lifecycleOffset = 0;
      managed.lifecycleRemainder = Buffer.alloc(0);
      fs.rmSync(managed.lifecycleFile, { force: true });
      if (resume) {
        this.#emitLifecycle(managed, "run.resumed", Date.now(), {
          data: {
            attempt: managed.resumeAttempts,
            attemptsAllowed: AGENT_RESUME_MAX_ATTEMPTS,
            previousStatus: record.status,
            ...(record.error ? { previousError: record.error } : {}),
            carriedTurns: resume.carryOver.turns,
            carriedToolCalls: resume.carryOver.toolCalls,
          },
        });
      }
      this.#invalidateUiList();
      return true;
    } catch (error) {
      const retryError = error instanceof Error ? error.message : String(error);
      try {
        fs.appendFileSync(
          path.join(managed.runDirectory, "relaunches.jsonl"),
          `${JSON.stringify({ at: Date.now(), kind: "relaunch-failed", error: retryError.slice(0, 500) })}\n`,
          { encoding: "utf8", mode: 0o600 },
        );
      } catch {
        // The status record below still carries the failure.
      }
      const failed = {
        ...record,
        // Keep the run's real progress: the relaunch failed, not the attempt.
        turns: Math.max(record.turns, managed.observedProgress.turns),
        toolCalls: Math.max(record.toolCalls, managed.observedProgress.toolCalls),
        error: `${record.error ?? "Agent run failed"} · relaunch failed: ${retryError}`,
      };
      Object.assign(failed, managed.runRoute);
      writeRecord(managed.statusFile, failed);
      managed.latestRecord = failed;
      managed.relaunchFailure = failed;
      return false;
    }
  }

  async #monitor(managed: ManagedAgent, timeoutMs: number): Promise<void> {
    const deadline = Date.now() + timeoutMs + TRANSPORT_EXIT_GRACE_MS;
    let firstObservedDeadAt: number | undefined;
    while (!managed.settled) {
      this.#drainLifecycle(managed);
      const record = readRecord(managed.statusFile);
      this.#checkFollowUps(managed, record);
      if (record) {
        this.#observeProgress(managed, record);
        const previous = managed.latestRecord;
        managed.latestRecord = record;
        if (
          !previous ||
          previous.updatedAt !== record.updatedAt ||
          previous.status !== record.status ||
          previous.runnerSessionId !== record.runnerSessionId ||
          previous.currentTool !== record.currentTool
        ) {
          managed.latestUiRecord = compactUiRecord(record);
          this.#invalidateUiList();
        }
      }
      if (managed.recursive) this.#nestedAgents(managed);
      if (record?.runnerSessionId) {
        managed.runnerSessionId = record.runnerSessionId;
      }
      if (record && terminalStatuses.has(record.status)) {
        // A terminal file can race an explicit stop or its tree-helper outcome.
        // Join the bounded stop before settlement can transfer native admission.
        if (managed.stopRequested && managed.transport.kind === "process") await this.#stopManagedTransport(managed);
        if (await this.#resumeStopped(managed, record, deadline)) continue;
        // A relaunch that failed is terminal: no fallback launch may run after it.
        if (!managed.relaunchFailure && await this.#retryStartup(managed, record, deadline)) continue;
        this.#settle(managed, this.#withTransportMetadata(managed.relaunchFailure ?? record, managed) as AgentRunResult);
        return;
      }
      if (Date.now() >= deadline) {
        managed.stopRequested = true;
        await this.#stopManagedTransport(managed);
        await this.#waitForTransportExit(managed);
        await this.#noteUnconfirmedExit(managed);
        const completed = readRecord(managed.statusFile);
        if (
          completed &&
          terminalStatuses.has(completed.status) &&
          completed.status !== "stopped"
        ) {
          this.#settle(
            managed,
            this.#withTransportMetadata(completed, managed) as AgentRunResult,
          );
          return;
        }
        if (managed.lastRetriedTransportFailure) {
          // The deadline fired mid-retry: the root cause is the dead transport
          // we were recovering from, not runaway wall time. Report that failure.
          this.#settle(
            managed,
            this.#withTransportMetadata(
              managed.lastRetriedTransportFailure,
              managed,
            ) as AgentRunResult,
          );
          return;
        }
        const timedOut = failedRecord(
          managed,
          "timed_out",
          `Agent timed out after ${timeoutMs}ms`,
        );
        writeRecord(managed.statusFile, timedOut);
        this.#settle(managed, timedOut);
        return;
      }
      const livenessPollIntervalMs =
        managed.transport.livenessPollIntervalMs ?? AGENT_STATUS_POLL_INTERVAL_MS;
      const livenessCheckedAt = Date.now();
      if (livenessCheckedAt - managed.lastLivenessCheckAt >= livenessPollIntervalMs) {
        managed.lastLivenessCheckAt = livenessCheckedAt;
        const alive = await managed.transport.isAlive();
        if (!alive) {
          firstObservedDeadAt ??= livenessCheckedAt;
          if (livenessCheckedAt - firstObservedDeadAt >= TRANSPORT_EXIT_GRACE_MS) {
            // Worker exit can precede the tree-helper/native-close join. The
            // explicit stop owns the no-result terminal status; joining it is
            // still mandatory, but absence during teardown is not run failure.
            if (managed.stopRequested && managed.transport.kind === "process") {
              await this.#stopManagedTransport(managed);
              return;
            }
            const lost = managed.transport.lostContact?.();
            if (lost) {
              // Not an exit: never relaunched, retried or cleaned up automatically.
              this.#markLost(managed, lost);
              const failed = failedRecord(
                managed,
                "failed",
                `Lost track of the worker: ${lost}. Fabric does not relaunch it or delete its files.`,
              );
              writeRecord(managed.statusFile, failed);
              this.#settle(managed, failed);
              return;
            }
            const logSummary = summarizeRunLog(managed.runDirectory, 8);
            const failed = failedRecord(
              managed,
              "failed",
              logSummary
                ? `Agent transport exited without a result; last run log: ${logSummary}`
                : "Agent transport exited without a result",
            );
            if (await this.#resumeStopped(managed, failed, deadline)) continue;
            if (!managed.relaunchFailure && await this.#retryStartup(managed, failed, deadline)) {
              managed.lastRetriedTransportFailure = failed;
              continue;
            }
            const settled = (managed.relaunchFailure ?? failed) as AgentRunResult;
            writeRecord(managed.statusFile, settled);
            this.#settle(managed, settled);
            return;
          }
        } else {
          firstObservedDeadAt = undefined;
        }
      }
      await delay(AGENT_STATUS_POLL_INTERVAL_MS);
    }
  }

  #settle(managed: ManagedAgent, result: AgentRunResult): void {
    if (managed.settled) return;
    this.#drainLifecycle(managed);
    const lost = managed.transport.lostContact?.();
    if (lost) this.#markLost(managed, lost);
    if (managed.transport.kind === "process" && managed.lostContact) {
      // Reporting a terminal result is allowed; releasing native admission is not.
      // An uncertain Windows tree can still contain untracked native descendants.
      managed.release = () => {};
    }
    if (managed.transport.kind === "process" && process.platform === "win32" &&
        managed.transport.waitForClose && !managed.lostContact && !managed.processStop) {
      // The logical result can precede native close. Keep its permit while a
      // later explicit stop may still acquire a Windows tree-helper obligation.
      const release = managed.release;
      managed.release = () => {
        managed.nativeReleasePending = Promise.resolve().then(async () => {
          await managed.transport.waitForClose!();
          // Stop may have begun after settlement, while native close was pending.
          await managed.processStop;
          await this.#noteUnconfirmedExit(managed);
          if (!managed.lostContact) release();
        }).catch(error => {
          this.#markLost(managed, error instanceof Error ? error.message : String(error));
        }).finally(() => { delete managed.nativeReleasePending; });
      };
    }
    if (!beginAgentSettlement(managed)) return;
    // Images are transport inputs, not retained run artifacts. Startup retries
    // have finished by settlement, so remove the owner-only handoff file for
    // every terminal outcome even when retainRuns keeps the rest of the run.
    fs.rmSync(path.join(managed.runDirectory, "images.json"), { force: true });
    this.#emitLifecycle(managed, `run.${result.status}`, result.finishedAt ?? Date.now(), {
      status: result.status,
      data: {
        ...(result.runnerSessionId ? { runnerSessionId: result.runnerSessionId } : {}),
        ...(this.#fabricSessionId ? { fabricSessionId: this.#fabricSessionId } : {}),
      },
    });

    if (this.#budget) {
      this.#settleBudgetGap(managed, result);
      const summary = this.#budgetSummary();
      if (summary) result.budget = summary;
    }
    const compactResult = compactUiRecord(result);
    managed.latestRecord = compactResult;
    managed.latestUiRecord = compactResult;
    if (managed.nestedSnapshot) {
      managed.nestedSnapshot = managed.nestedSnapshot.map((record) =>
        compactUiRecord(record),
      );
    }
    this.#saveSettledResult(managed, result);
    this.#pruneRetainedUiRecords();
    this.#invalidateUiList();
    const reported = this.#withTransportMetadata(result, managed) as AgentRunResult;
    finishAgentSettlement(managed, reported);
    managed.task = "";
    this.#notifyBackgroundComplete(managed, reported);
  }

  #saveSettledResult(managed: ManagedAgent, result: AgentRunResult): boolean {
    try {
      managed.routeOutcome?.(result);
      this.#onSettled?.(this.#withTransportMetadata(result, managed, false) as AgentRunResult);
      delete managed.settlementSaveFailure;
      return true;
    } catch (error) {
      managed.settlementSaveFailure = {
        result: structuredClone(result),
        warning: `Terminal result save failed; run retained: ${error instanceof Error ? error.message : String(error)}`,
      };
      return false;
    } finally {
      this.#invalidateUiList();
    }
  }

  #canCollect(managed: ManagedAgent): boolean {
    if (managed.processStopPending || managed.nativeReleasePending || managed.lostContact ||
        uncheckedExternalExit(managed.transport) || runTreeExitVeto(managed.runDirectory, 0, undefined, true)) return false;
    // Settlement compacts UI caches. Retry only the original full result, never those caches.
    if (managed.settlementSaveFailure &&
        !this.#saveSettledResult(managed, managed.settlementSaveFailure.result)) return false;
    return true;
  }

  #notifyBackgroundComplete(managed: ManagedAgent, result: AgentRunResult): void {
    if (
      managed.background &&
      !managed.completionNotified &&
      !this.#closing &&
      this.config.notifyOnComplete &&
      this.#onBackgroundComplete
    ) {
      managed.completionNotified = true;
      try {
        this.#onBackgroundComplete(result);
      } catch { /* completion callback must not break the manager */ }
    }
  }

  #drainLifecycle(managed: ManagedAgent): void {
    let content: Buffer;
    try {
      content = fs.readFileSync(managed.lifecycleFile);
    } catch {
      return;
    }
    if (content.length < managed.lifecycleOffset) {
      managed.lifecycleOffset = 0;
      managed.lifecycleRemainder = Buffer.alloc(0);
    }
    if (content.length === managed.lifecycleOffset) return;
    const appended = content.subarray(managed.lifecycleOffset);
    managed.lifecycleOffset = content.length;
    const combined = Buffer.concat([managed.lifecycleRemainder, appended]);
    const finalNewline = combined.lastIndexOf(0x0a);
    if (finalNewline < 0) {
      managed.lifecycleRemainder = combined.length <= 64 * 1024 ? combined : Buffer.alloc(0);
      return;
    }
    managed.lifecycleRemainder = combined.subarray(finalNewline + 1);
    const complete = combined.subarray(0, finalNewline).toString("utf8");
    for (const line of complete.split("\n")) {
      if (!line.trim()) continue;
      try {
        const parsed = JSON.parse(line) as Record<string, unknown>;
        if (parsed.version !== 1 || typeof parsed.occurredAt !== "number") continue;
        if (parsed.event === "tokens.usage") {
          if (!Object.prototype.hasOwnProperty.call(parsed, "data")) continue;
          const usage = tokenUsagePayloadFromValue(parsed.data);
          if (usage) this.#onTokenUsage(managed, usage, parsed.occurredAt);
          continue;
        }
        if (
          !isFabricLifecycleEventType(parsed.event) ||
          !parsed.event.startsWith("pi.")
        ) continue;
        this.#emitLifecycle(
          managed,
          parsed.event,
          parsed.occurredAt,
          Object.prototype.hasOwnProperty.call(parsed, "data") ? { data: parsed.data } : {},
        );
      } catch {
        // Ignore malformed worker lifecycle records; status monitoring remains authoritative.
      }
    }
  }

  #appendAttributedBudgetLedger(
    managed: ManagedAgent,
    tokens: number,
    cost: number,
  ): void {
    if (!this.#budget || (tokens <= 0 && cost <= 0)) return;
    appendBudgetLedger(this.#budget.file, {
      id: managed.id,
      depth: this.#currentDepth + 1,
      runner: managed.runner,
      ...(managed.actorId ? { actorId: managed.actorId } : {}),
      ...(managed.actorName ? { actorName: managed.actorName } : {}),
      cost,
      tokens,
      ts: Date.now(),
    });
    this.#budgetSummaryCache = undefined;
  }

  #onTokenUsage(
    managed: ManagedAgent,
    usage: FabricTokenUsagePayload,
    occurredAt: number,
  ): void {
    managed.usageEmitted.input += usage.input;
    managed.usageEmitted.output += usage.output;
    managed.usageEmitted.cacheRead += usage.cacheRead;
    managed.usageEmitted.cacheWrite += usage.cacheWrite;
    managed.usageEmitted.cost += usage.cost;
    this.#appendAttributedBudgetLedger(managed, usage.input + usage.output + usage.cacheRead + usage.cacheWrite, usage.cost);
    this.#emitLifecycle(managed, "tokens.usage", occurredAt, { data: usage });
  }

  #settleBudgetGap(managed: ManagedAgent, result: AgentRunResult): void {
    const total = result.usage;
    const residual = {
      input: Math.max(0, total.input - managed.usageEmitted.input),
      output: Math.max(0, total.output - managed.usageEmitted.output),
      cacheRead: Math.max(0, total.cacheRead - managed.usageEmitted.cacheRead),
      cacheWrite: Math.max(0, total.cacheWrite - managed.usageEmitted.cacheWrite),
      cost: Math.max(0, total.cost - managed.usageEmitted.cost),
    };
    const residualTokens =
      residual.input + residual.output + residual.cacheRead + residual.cacheWrite;
    this.#appendAttributedBudgetLedger(managed, residualTokens, residual.cost);
  }

  #emitLifecycle(
    managed: Pick<ManagedAgent, "id" | "name" | "actorId" | "actorName" | "runner">,
    event: FabricLifecycleEventType,
    occurredAt: number,
    options: { status?: string; data?: unknown } = {},
  ): void {
    if (!this.#onLifecycle) return;
    try {
      this.#onLifecycle({
        source: {
          id: managed.actorId ?? managed.id,
          name: managed.actorName ?? managed.name,
          kind: managed.actorId ? "actor" : "agent",
          rootId: this.#mainAgentId ?? managed.id,
          runner: managed.runner,
          ...(this.#hostId ? { ownerHostId: this.#hostId } : {}),
          ...(this.#identityId ? { ownerIdentityId: this.#identityId } : {}),
        },
        event,
        occurredAt,
        runId: managed.id,
        ...(options.status ? { status: options.status } : {}),
        ...(options.data === undefined ? {} : { data: options.data }),
      });
    } catch {
      // Lifecycle observers must not interrupt child execution or settlement.
    }
  }

  readonly #inheritedToolAllowlist = readChildToolAllowlist();

  #childTools(request: AgentRunRequest, runner: FabricAgentRunner, requiresFabricKernel = false): string[] {
    const tools = [...(request.tools ?? this.config.defaultTools)].filter(
      (tool) => tool !== "fabric_exec" &&
        (this.#inheritedToolAllowlist === undefined || this.#inheritedToolAllowlist.has(tool)),
    );
    const extensions = request.recursive === true
      ? true
      : (request.extensions ?? this.config.extensions);
    if (runner === "pi" && (request.recursive || ((this.#fullCodeMode || requiresFabricKernel) && extensions))) {
      tools.push("fabric_exec");
    }
    return [...new Set(tools)];
  }

  #budgetSummary(): FabricBudgetSummary | undefined {
    if (!this.#budget) return undefined;
    const now = Date.now();
    if (this.#budgetSummaryCache && now - this.#budgetSummaryCache.at < AGENT_STATUS_POLL_INTERVAL_MS) {
      return this.#budgetSummaryCache.value;
    }
    const { cost, tokens } = readBudgetLedger(this.#budget.file);
    const value = {
      limit: this.#budget.budget,
      spent: cost,
      remaining: Math.max(0, this.#budget.budget - cost),
      tokens,
    };
    this.#budgetSummaryCache = { at: now, value };
    return value;
  }

  async #resolveTransport(requested: FabricAgentTransport): Promise<AgentTransportAdapter> {
    if (requested !== "auto") {
      const adapter = this.#transports.get(requested);
      if (!adapter || !(await adapter.available())) {
        throw new Error(`Fabric agent transport is unavailable: ${requested}`);
      }
      return adapter;
    }
    for (const kind of ["herdr", "localterm", "tmux", "screen", "process"] as const) {
      const adapter = this.#transports.get(kind);
      if (adapter && (await adapter.available())) return adapter;
    }
    throw new Error("No Fabric agent transport is available");
  }

  #pruneRetainedUiRecords(): void {
    const settled = [...this.#runs.values()].filter((managed) => managed.settled);
    const evicted = settled.slice(0, -MAX_RETAINED_RUN_HANDLES);
    for (const managed of evicted) {
      if (!managed.settlementSaveFailure && !managed.processStopPending && !managed.nativeReleasePending &&
          !managed.lostContact && !hasUnresolvedWorker(managed.runDirectory)) this.#runs.delete(managed.id);
    }
    const retained = evicted.length > 0 ? settled.slice(evicted.length) : settled;
    if (retained.length <= MAX_RETAINED_UI_RUNS) return;
    for (const managed of retained.slice(0, -MAX_RETAINED_UI_RUNS)) {
      delete managed.latestRecord;
      delete managed.latestUiRecord;
      delete managed.nestedSnapshot;
      delete managed.nestedSnapshotAt;
    }
  }

  #invalidateUiList(): void {
    this.#uiListRevision++;
    this.#uiListCache = undefined;
    for (const listener of this.#uiListeners) {
      try {
        listener();
      } catch {
        // UI observers must not interrupt agent state transitions.
      }
    }
  }

  #requireRun(id: string): ManagedAgent {
    const managed = this.#runs.get(id);
    if (!managed) {
      const previous = this.#previousRuns.get(id);
      if (previous) {
        throw new Error(`Fabric agent ${previous.name} (${id}) is ${previous.status}: ${previous.error ?? "no error"}; it has no running target`);
      }
      throw new Error(`Unknown Fabric agent: ${id}`);
    }
    return managed;
  }

  #handleInfo(managed: ManagedAgent, status: AgentHandleInfo["status"]): AgentHandleInfo {
    const model = managed.latestRecord?.model ?? managed.model;
    const thinking = managed.latestRecord?.thinking ?? managed.thinking;
    return {
      ...managed.runRoute,
      id: managed.id,
      name: managed.name,
      status,
      runner: managed.runner,
      ...(managed.kernel ? { kernel: managed.kernel } : {}),
      transport: managed.transport.kind,
      cwd: managed.cwd,
      ...(managed.residency === "durable" ? { residency: "durable" as const } : {}),
      ...(model ? { model } : {}),
      ...(thinking ? { thinking } : {}),
      ...(managed.actorId ? { actorId: managed.actorId } : {}),
      ...(managed.actorName ? { actorName: managed.actorName } : {}),
      ...(managed.spawner ? { spawner: managed.spawner } : {}),
      ...(managed.capabilityRequirements
        ? { capabilityRequirements: [...managed.capabilityRequirements] }
        : {}),
      ...(managed.capabilityDigest ? { capabilityDigest: managed.capabilityDigest } : {}),
      ...(managed.recursive ? { recursive: true } : {}),
      ...(managed.runnerSessionId ? { runnerSessionId: managed.runnerSessionId } : {}),
      ...(managed.transport.sessionId ? { sessionId: managed.transport.sessionId } : {}),
      ...(managed.transport.attachCommand
        ? { attachCommand: managed.transport.attachCommand }
        : {}),
      ...(managed.branch ? { branch: managed.branch } : {}),
      ...(managed.worktree ? { worktree: managed.worktree } : {}),
    };
  }

  // Recursive child processes remove their nested run directories on shutdown.
  // Preserve the last bounded status tree so completed leaves remain visible
  // in the parent run until that parent is explicitly cleaned up.
  #nestedAgents(managed: ManagedAgent, force = false): AgentRunRecord[] {
    const now = Date.now();
    const needsInitialDiscovery =
      managed.nestedSnapshot === undefined &&
      fs.existsSync(path.join(managed.runDirectory, "nested"));
    if (
      !force &&
      !needsInitialDiscovery &&
      managed.nestedSnapshotAt !== undefined &&
      now - managed.nestedSnapshotAt < NESTED_SNAPSHOT_POLL_MS
    ) {
      return managed.nestedSnapshot ? structuredClone(managed.nestedSnapshot) : [];
    }
    managed.nestedSnapshotAt = now;
    const discovered = readNestedAgents(managed.runDirectory);
    if (discovered.length > 0) {
      managed.nestedSnapshot = discovered;
      this.#invalidateUiList();
    }
    return managed.nestedSnapshot ? structuredClone(managed.nestedSnapshot) : [];
  }

  #withTransportMetadata(record: AgentRunRecord, managed: ManagedAgent, includeSaveFailure = true): AgentRunRecord {
    const nestedAgents = this.#nestedAgents(
      managed,
      terminalStatuses.has(record.status) && !managed.settled,
    );
    const budget = this.#budgetSummary();
    const { logFile: _logFile, nestedAgents: _nestedAgents, ...safeRecord } = record;
    const model = record.model ?? managed.model;
    const thinking = record.thinking ?? managed.thinking;
    const runnerSessionId = record.runnerSessionId ?? managed.runnerSessionId;
    return {
      ...safeRecord,
      ...managed.runRoute,
      ...(includeSaveFailure && managed.settlementSaveFailure
        ? { warnings: [...(record.warnings ?? []), managed.settlementSaveFailure.warning] }
        : {}),
      cwd: managed.cwd,
      runner: managed.runner,
      ...(managed.kernel ? { kernel: managed.kernel } : {}),
      ...(managed.residency === "durable" ? { residency: "durable" as const } : {}),
      logFile: path.join(managed.runDirectory, "events.jsonl"),
      ...(nestedAgents.length > 0 ? { nestedAgents } : {}),
      ...(budget ? { budget } : {}),
      ...(model ? { model } : {}),
      ...(managed.modelReason !== undefined ? { modelReason: managed.modelReason } : {}),
      ...(thinking ? { thinking } : {}),
      ...(managed.actorId ? { actorId: managed.actorId } : {}),
      ...(managed.actorName ? { actorName: managed.actorName } : {}),
      ...(managed.spawner ? { spawner: managed.spawner } : {}),
      ...(managed.capabilityRequirements
        ? { capabilityRequirements: [...managed.capabilityRequirements] }
        : {}),
      ...(managed.capabilityDigest ? { capabilityDigest: managed.capabilityDigest } : {}),
      ...(managed.recursive ? { recursive: true } : {}),
      ...(runnerSessionId ? { runnerSessionId } : {}),
      ...(this.#mainAgentId ? { mainAgentId: this.#mainAgentId } : {}),
      ...(this.#fabricSessionId ? { fabricSessionId: this.#fabricSessionId } : {}),
      ...(managed.transport.sessionId ? { sessionId: managed.transport.sessionId } : {}),
      ...(managed.transport.attachCommand
        ? { attachCommand: managed.transport.attachCommand }
        : {}),
      ...(managed.branch ? { branch: managed.branch } : {}),
      ...(managed.worktree ? { worktree: managed.worktree } : {}),
    };
  }
}
