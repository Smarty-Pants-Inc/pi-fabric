import type { FabricPrincipal } from "../fabric-provenance.js";
import { createHash, randomUUID } from "node:crypto";
import type { ResidentReleaseIntent, ResidentLauncherIdentity } from "./handover.js";
import { recordResidentOutcome, registerCancellationEffect } from "../async-settlement.js";
import { readFileRetrying } from "../core/atomic-write.js";
import fs from "node:fs";
import path from "node:path";
import type { FabricOwnedModelGuidance } from "../components/model-guidance.js";
import type { FabricModelAliases, FabricModelCandidate } from "../core/model-resolution.js";
import type { FabricActorsConfig, FabricAgentConfig, FabricMeshConfig, FabricRetentionConfig } from "../config.js";
import type { FabricActorInfo, FabricActorRequest, FabricActorBindingScope, FabricActorActivationFilter } from "../actors/types.js";
import type { FabricThinking } from "../thinking.js";
import type { AgentHandleInfo, AgentRunRequest } from "../agents/types.js";
import type { FabricKernel, FabricResidentOutcomeReceipt } from "../runtime/kernel.js";
import type { MeshIdentity } from "../mesh/store.js";
export const sleepUnlessAborted = (ms: number, signal?: AbortSignal): Promise<void> =>
  // Executor form: the configured lib is ES2022, which has no
  // Promise.withResolvers, and an abort listener plus a timer need shared
  // completion control.
  new Promise<void>((resolve, reject) => {
    const aborted = (): Error =>
      signal?.reason instanceof Error ? signal.reason : new Error("Fabric residency request was aborted");
    if (signal?.aborted) {
      reject(aborted());
      return;
    }
    const timer = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, Math.max(0, ms));
    timer.unref?.();
    const onAbort = (): void => {
      clearTimeout(timer);
      reject(aborted());
    };
    signal?.addEventListener("abort", onAbort, { once: true });
  });

export interface ResidentRequestDecision {
  requestId: string;
  state: "abandoned" | "committed";
  /** Allocated before the first mutation, even if publication is still pending. */
  id?: string;
  operation?: ResidentCommand["operation"];
  ownerHostId?: string;
  /** Originating requester metadata; never grants mutation authority. */
  principal?: FabricPrincipal | undefined;
}

const residentDecisionPath = (residencyRoot: string, requestId: string): string =>
  path.join(residencyRoot, "decisions", `${requestId}.json`);

export const readResidentRequestDecision = (
  residencyRoot: string,
  requestId: string,
): ResidentRequestDecision | undefined => {
  const file = residentDecisionPath(residencyRoot, requestId);
  try {
    const decision = JSON.parse(readFileRetrying(file)) as ResidentRequestDecision;
    if (decision.requestId !== requestId || !["abandoned", "committed"].includes(decision.state) ||
      (decision.state === "committed" && (typeof decision.id !== "string" ||
        typeof decision.ownerHostId !== "string" ||
        typeof decision.operation !== "string"))) {
      throw new Error(`Invalid Fabric residency decision for ${requestId}`);
    }
    return decision;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
    throw error; // Corruption is not permission to commit.
  }
};

/**
 * The hard-link is a filesystem compare-and-set: publish a complete, immutable
 * record ONLY if the shared fence is absent. Unlike rename (overwrite) or an
 * open("wx") followed by write (an empty-file window), readers always see the
 * winner's complete record. Both commit and abandonment use this same fence.
 * Keep decisions across host restarts: an old picked-up request must never replay.
 */
const decideResidentRequest = (residencyRoot: string, decision: ResidentRequestDecision): boolean => {
  const file = residentDecisionPath(residencyRoot, decision.requestId);
  fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
  const temporary = `${file}.${process.pid}.${randomUUID()}.tmp`;
  try {
    fs.writeFileSync(temporary, JSON.stringify(decision), { mode: 0o600, flag: "wx" });
    try {
      fs.linkSync(temporary, file);
      return true;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "EEXIST") return false;
      throw error; // Fail closed if the filesystem cannot provide the fence.
    }
  } finally {
    fs.rmSync(temporary, { force: true });
  }
};

/** Called at the first mutation, after asynchronous preparation, never at pickup. */
export const commitResidentRequest = (
  residencyRoot: string,
  command: ResidentCommand,
  id: string,
  ownerHostId: string,
): void => {
  if (decideResidentRequest(residencyRoot, {
    requestId: command.requestId, state: "committed", operation: command.operation, id, ownerHostId,
    ...(("caller" in command && command.caller?.principal) ? { principal: command.caller.principal } : {}),
  })) return;
  const decision = readResidentRequestDecision(residencyRoot, command.requestId);
  throw new Error(decision?.state === "abandoned"
    ? `Fabric residency request ${command.requestId} was abandoned before commit`
    : `Fabric residency request ${command.requestId} was already committed; do not replay`);
};

/**
 * Publish the abandonment tombstone BEFORE removing exchange files. If commit
 * won, retain those files and return its known ID: cancellation cannot undo a
 * mutation, and the caller must not mistake this for a rejected spawn/create.
 */
export const abandonResidentRequest = (
  requestsPath: string,
  responsesPath: string,
  requestId: string,
): ResidentRequestDecision => {
  const root = path.dirname(requestsPath);
  decideResidentRequest(root, { requestId, state: "abandoned" });
  const decision = readResidentRequestDecision(root, requestId)!;
  if (decision.state === "abandoned") {
    for (const directory of [requestsPath, responsesPath]) {
      try {
        fs.rmSync(path.join(directory, `${requestId}.json`), { force: true });
      } catch { /* The durable tombstone, not deletion, prevents a late commit. */ }
    }
  }
  return decision;
};

/** Existing handle/actor return contracts cannot represent an unconfirmed launch. */
export class ResidentOutcomeUnknownError extends Error {
  readonly residentOutcome: FabricResidentOutcomeReceipt;
  readonly requestId: string;
  readonly id: string | undefined;
  readonly operation: ResidentCommand["operation"];
  readonly ownerHostId: string | undefined;

  constructor(command: ResidentCommand, decision: ResidentRequestDecision | undefined, cause: unknown, signal?: AbortSignal) {
    const id = decision?.id ?? ("id" in command ? command.id : undefined);
    const kind = ["spawn", "foreground", "cleanup"].includes(command.operation) ? "agent" : "actor";
    // Guest runtimes may preserve only message, so the classification and IDs live there too.
    super(`ResidentOutcomeUnknownError: Fabric residency ${command.operation} outcome unknown: requestId=${command.requestId}` +
      `, ${kind}Id=${id ?? "not yet known"}` +
      `${decision?.ownerHostId ? `, ownerHostId=${decision.ownerHostId}` : ""}. ` +
      `Do not retry or reassign this work. Check agents.${kind === "actor" ? "actorStatus" : "status"}` +
      ` / agents.list${id ? ` for ${id}` : ` and request ${command.requestId}`}; ` +
      `publication may still be pending. Use agents.stop with the known ID once registered to cancel. ` +
      `Cause: ${cause instanceof Error ? cause.message : String(cause)}`, { cause });
    this.name = "ResidentOutcomeUnknownError";
    this.requestId = command.requestId;
    this.id = id;
    this.operation = command.operation;
    this.ownerHostId = decision?.ownerHostId;
    this.residentOutcome = Object.freeze({
      requestId: command.requestId,
      state: decision?.state === "committed" ? "committed" : "unknown",
      operation: command.operation,
      entityKind: kind,
      ...(id ? { id } : {}),
      ...(decision?.ownerHostId ? { ownerHostId: decision.ownerHostId } : {}),
    });
    recordResidentOutcome(signal, this.residentOutcome);
  }
}

/** Install before request publication; outer abort races can now settle the same fence. */
export const registerResidentCancellation = (
  signal: AbortSignal | undefined,
  residencyRoot: string,
  command: ResidentCommand,
): void => {
  // A committed receipt is immutable. Reuse its first error instead of nesting
  // already-formatted uncertainty again at each enclosing cancellation gate.
  let committedOutcome: ResidentOutcomeUnknownError | undefined;
  registerCancellationEffect(signal, (reason) => {
    if (committedOutcome) return committedOutcome;
    let decision: ResidentRequestDecision | undefined;
    try {
      decision = abandonResidentRequest(path.join(residencyRoot, "requests"), path.join(residencyRoot, "responses"), command.requestId);
    } catch (error) {
      try { decision = readResidentRequestDecision(residencyRoot, command.requestId); } catch { /* unreadable fence */ }
      return new ResidentOutcomeUnknownError(command, decision, error, signal);
    }
    if (decision.state === "committed") return committedOutcome = new ResidentOutcomeUnknownError(command, decision, reason, signal);
    return undefined;
  });
};

/** A short age such as "42s", "3m12s" or "1h05m". */
export const formatAge = (ms: number): string => {
  const s = Math.max(0, Math.round(ms / 1_000));
  if (s < 60) return `${s}s`;
  if (s < 3_600) return `${Math.floor(s / 60)}m${String(s % 60).padStart(2, "0")}s`;
  return `${Math.floor(s / 3_600)}h${String(Math.floor((s % 3_600) / 60)).padStart(2, "0")}m`;
};

/** The resident host's pending actor removals (smarty-dev#2184 item 8). */
export const residentRemovalsPath = (residencyRoot: string): string =>
  path.join(residencyRoot, "removals.json");

/**
 * Why a residency request may wait or fail, for its error: the removals pending behind in-flight
 * runs, and a request the host has been processing for a while (smarty-dev#2184 item 8).
 * Best effort: never throws.
 */
export const residentHostStateNote = (residencyRoot: string, now = Date.now()): string => {
  const notes: string[] = [];
  try {
    const value = JSON.parse(fs.readFileSync(residentRemovalsPath(residencyRoot), "utf8")) as {
      removals?: Array<{ name?: unknown; id?: unknown; runId?: unknown; runStartedAt?: unknown; requestedAt?: unknown }>;
    };
    for (const removal of value.removals ?? []) {
      if (typeof removal.id !== "string") continue;
      const label = typeof removal.name === "string" ? `${removal.name} (${removal.id})` : removal.id;
      const since = typeof removal.runStartedAt === "number" ? removal.runStartedAt
        : typeof removal.requestedAt === "number" ? removal.requestedAt : now;
      notes.push(typeof removal.runId === "string"
        ? `removal of ${label} is pending behind its in-flight run ${removal.runId} (${formatAge(now - since)})`
        : `removal of ${label} is pending (${formatAge(now - since)})`);
    }
  } catch { /* no pending removals */ }
  try {
    const processing = path.join(residencyRoot, "processing");
    for (const entry of fs.readdirSync(processing).filter((name) => name.endsWith(".json"))) {
      const file = path.join(processing, entry);
      const command = JSON.parse(fs.readFileSync(file, "utf8")) as { operation?: unknown; id?: unknown };
      const age = now - fs.statSync(file).mtimeMs;
      if (age < 2_000) continue;
      notes.push(`the host has been processing ${String(command.operation)}` +
        `${typeof command.id === "string" ? ` of ${command.id}` : ""} for ${formatAge(age)}; this request waits behind it`);
    }
  } catch { /* nothing in process */ }
  return notes.join("; ");
};

export const RESIDENT_HOST_FORMAT = 1 as const;
// Only command envelopes for post-B70 registry operations use format 2.
// B70 validates format before dispatch and otherwise treats unknown operations
// as removeActor: an unclaimed request MUST remain safe across crash/rollback.
// Keep host config, owner records, responses and the five legacy commands at 1.
export const RESIDENT_ACTOR_COMMAND_FORMAT = 2 as const;
const RESIDENT_DELIVERY_PREFIX = "residency/deliveries/";

const digest = (value: string): string =>
  createHash("sha256").update(value).digest("hex");

export const residentHostId = (rootId: string): string =>
  `resident:${digest(rootId).slice(0, 24)}`;

export const isResidentHostId = (id: string): boolean => /^resident:[0-9a-f]{24}$/.test(id);

export const residentRoot = (meshRoot: string, rootId: string): string =>
  path.join(meshRoot, "residency", digest(rootId));

/**
 * A durable run's terminal record. It outlives the run directory, which the resident host
 * removes when it goes idle, so status after a restart still reads the result (smarty-dev#1882).
 */
export const residentResultPath = (residencyRoot: string, id: string): string =>
  path.join(residencyRoot, "results", `${id}.json`);

export const residentDeliveryPrefix = (rootId: string): string =>
  `${RESIDENT_DELIVERY_PREFIX}${digest(rootId).slice(0, 32)}/`;

export interface ResidentPiModelState {
  available: FabricModelCandidate[];
  aliases: FabricModelAliases;
  defaultModel?: string;
}

export interface ResidentHostConfig {
  format: typeof RESIDENT_HOST_FORMAT;
  rootId: string;
  sessionId: string;
  cwd: string;
  projectRoot: string;
  /** The root's fleet role; only a project agent's hosts adopt a project's orphans (smarty-dev#878). */
  role?: string;
  /** The root's project (participantProject), which can differ from its cwd's (smarty-dev#977). */
  project?: string;
  meshRoot: string;
  actorRoot: string;
  sessionActorRoot?: string;
  residencyRoot: string;
  fullCodeMode: boolean;
  kernel?: FabricKernel;
  pythonRuntime?: "cpython" | "monty";
  agents: FabricAgentConfig;
  mesh: FabricMeshConfig;
  retention: FabricRetentionConfig;
  /** Absent in a config an older release wrote: the defaults apply. */
  actors?: FabricActorsConfig;
  workerPath: string;
  fabricExtensionPath: string;
  piBinary: string;
  claudeBinary: string;
  vedaBinary: string;
  piModels?: ResidentPiModelState;
  modelGuidance?: FabricOwnedModelGuidance[];
}

export interface ResidentHostOwner {
  format: typeof RESIDENT_HOST_FORMAT;
  hostId: string;
  pid: number;
  /** Linux /proc start ticks; absent for older hosts and on other platforms. */
  processStartTime?: string | undefined;
  token: string;
  startedAt: number;
  readyAt: number;
  /** Commands supported by this running binary; absent on pre-negotiation hosts. */
  commands?: readonly string[];
  /** New clients must not dispatch mutations to an already-running pre-fence host. */
  requestFence?: 1;
  /** Attestation from the loaded host, never desired config.json. */
  releaseRoot?: string;
  configDigest?: string;
  handover?: { abi: "fabric-resident-1"; launcher: ResidentLauncherIdentity };
  /** A staged successor has proved worker startup but admits no business work yet. */
  attempt?: { id: string; kind: "target" | "fallback" };
}

interface ResidentSpawnCommand {
  format: typeof RESIDENT_HOST_FORMAT;
  operation: "spawn";
  requestId: string;
  rootId: string;
  request: AgentRunRequest;
  createdAt: number;
}

interface ResidentCleanupCommand {
  format: typeof RESIDENT_HOST_FORMAT;
  operation: "cleanup";
  requestId: string;
  rootId: string;
  id: string;
  deleteBranch: boolean;
  createdAt: number;
}

interface ResidentForegroundCommand {
  format: typeof RESIDENT_HOST_FORMAT;
  operation: "foreground";
  requestId: string;
  rootId: string;
  id: string;
  createdAt: number;
}

interface ResidentRemoveActorCommand {
  format: typeof RESIDENT_HOST_FORMAT;
  operation: "removeActor";
  requestId: string;
  rootId: string;
  id: string;
  createdAt: number;
}

interface ResidentCreateActorCommand {
  format: typeof RESIDENT_HOST_FORMAT;
  operation: "createActor";
  requestId: string;
  rootId: string;
  request: FabricActorRequest;
  createdAt: number;
}

/** Existing Pi runtime control identity, captured by the provider, never from action args. */
export interface ResidentActorCaller {
  identity: MeshIdentity;
  hostId: string;
  /** Captured host turn provenance, independent of owning-Main authorization. */
  principal?: FabricPrincipal | undefined;
  /** Frozen optional-tool authority; absence means an unrestricted Main. */
  toolCeiling?: string[];
}

export class ResidentActorAuthorizationError extends Error {
  readonly code = "RESIDENT_ACTOR_FORBIDDEN" as const;
  constructor(message = "Only the actual owning Main can mutate a resident actor") {
    super(message);
    this.name = "ResidentActorAuthorizationError";
  }
}

export const assertResidentActorMain = (caller: ResidentActorCaller | undefined, rootId: string): void => {
  if (!caller || caller.identity?.kind !== "main" || caller.identity.id !== rootId) {
    throw new ResidentActorAuthorizationError();
  }
};

export const assertResidentActorToolCeiling = (tools: string[], ceiling: readonly string[] | undefined): void => {
  if (ceiling === undefined) return;
  if (!Array.isArray(ceiling) || !ceiling.every((tool) => typeof tool === "string") ||
    !Array.isArray(tools) || !tools.every((tool) => typeof tool === "string" &&
      (tool.trim() === "fabric_exec" || ceiling.includes(tool.trim())))) {
    throw new ResidentActorAuthorizationError("Actor tools cannot exceed the caller's tool ceiling");
  }
};

/** Root-owned registry operations; these never start a resident host. */
export type ResidentActorMutation =
  | { operation: "setInstructions"; id: string; instructions: string }
  | { operation: "setTools"; id: string; tools: string[] }
  | { operation: "setModel"; id: string; model?: string; scope: FabricActorBindingScope }
  | { operation: "setThinking"; id: string; thinking?: FabricThinking; scope: FabricActorBindingScope }
  | { operation: "setActivationFilter"; id: string; activationFilter: FabricActorActivationFilter | null };

type ResidentActorMutationCommand = ResidentActorMutation & {
  caller?: ResidentActorCaller;
  format: typeof RESIDENT_ACTOR_COMMAND_FORMAT;
  requestId: string;
  rootId: string;
  createdAt: number;
};

interface ResidentActorStatusCommand {
  format: typeof RESIDENT_ACTOR_COMMAND_FORMAT;
  operation: "actorStatus" | "actors";
  requestId: string;
  rootId: string;
  id?: string;
  createdAt: number;
}

export type ResidentCommand =
  | ResidentSpawnCommand
  | ResidentCleanupCommand
  | ResidentForegroundCommand
  | ResidentRemoveActorCommand
  | ResidentCreateActorCommand
  | ResidentActorMutationCommand
  | ResidentActorStatusCommand
  | (ResidentReleaseIntent & { format: typeof RESIDENT_ACTOR_COMMAND_FORMAT; operation: "releaseChange";
      requestId: string; rootId: string; createdAt: number });

// The only operations every format-1 host predating command negotiation understood.
const LEGACY_RESIDENT_COMMANDS = ["spawn", "foreground", "cleanup", "createActor", "removeActor"] as const;
export const RESIDENT_COMMANDS = [
  ...LEGACY_RESIDENT_COMMANDS, "actors", "actorStatus", "setInstructions", "setModel",
  "setThinking", "setTools", "setActivationFilter", "releaseChange",
] as const satisfies readonly ResidentCommand["operation"][];

export const isResidentCommandOperation = (operation: unknown): operation is ResidentCommand["operation"] =>
  typeof operation === "string" && (RESIDENT_COMMANDS as readonly string[]).includes(operation);

export class ResidentCommandUnsupportedError extends Error {
  readonly code = "RESIDENT_COMMAND_UNSUPPORTED" as const;
  constructor(message = "The owning resident host runs an older release; release following requires a handover-capable host and launcher; retry after activation") {
    super(message);
    this.name = "ResidentCommandUnsupportedError";
  }
}

/** Check the running owner's publication, never the caller's release/config. */
export const assertResidentCommandSupported = (owner: ResidentHostOwner, operation: ResidentCommand["operation"]): void => {
  const supported = owner.commands === undefined ? LEGACY_RESIDENT_COMMANDS : owner.commands;
  if (!isResidentCommandOperation(operation) || !Array.isArray(supported) ||
      !(supported as readonly string[]).includes(operation)) {
    throw new ResidentCommandUnsupportedError();
  }
};

export interface ResidentCommandResponse {
  format: typeof RESIDENT_HOST_FORMAT;
  requestId: string;
  ok: boolean;
  handle?: AgentHandleInfo;
  actor?: FabricActorInfo;
  actors?: FabricActorInfo[];
  /** A removeActor that returned before the actor's in-flight run ended: the pending state. */
  pending?: string;
  cleaned?: boolean;
  error?: string;
  errorCode?: "RESIDENT_ACTOR_FORBIDDEN" | "RESIDENT_COMMAND_UNSUPPORTED";
  completedAt: number;
}

export interface ResidentAgentMetadata {
  format: typeof RESIDENT_HOST_FORMAT;
  rootId: string;
  id: string;
  runDirectory: string;
  handle: AgentHandleInfo;
  worktreeGitRoot?: string;
  /** Main consumed this terminal result; suppress queued delivery across reconnects. */
  completionConsumedAt?: number;
  createdAt: number;
  updatedAt: number;
}

export interface ResidentDeliveryRecord {
  principal?: FabricPrincipal | undefined;
  format: typeof RESIDENT_HOST_FORMAT;
  /** Survives payload truncation; lets Main read the authoritative terminal result. */
  agentCompletionId?: string;
  id: string;
  rootId: string;
  from: MeshIdentity;
  delivery: "steer" | "followUp";
  triggerTurn: boolean;
  message: string;
  data?: unknown;
  createdAt: number;
}
