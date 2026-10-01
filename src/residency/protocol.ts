import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import type { FabricOwnedModelGuidance } from "../components/model-guidance.js";
import type { FabricModelAliases, FabricModelCandidate } from "../core/model-resolution.js";
import type { FabricActorsConfig, FabricAgentConfig, FabricMeshConfig, FabricRetentionConfig } from "../config.js";
import type { FabricActorInfo, FabricActorRequest, FabricActorBindingScope, FabricActorActivationFilter } from "../actors/types.js";
import type { FabricThinking } from "../thinking.js";
import type { AgentHandleInfo, AgentRunRequest } from "../agents/types.js";
import type { FabricKernel } from "../runtime/kernel.js";
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

/**
 * Remove an abandoned file-exchange request. The resident host renames
 * unstarted requests out of `requests/` before running them, so deleting our
 * file cancels work the host has not picked up yet; a late host response is
 * removed as well. Deleting a request the host already moved is a no-op, and
 * in-flight work still runs to completion. Best effort: never throws.
 */
export const abandonResidentRequest = (
  requestsPath: string,
  responsesPath: string,
  requestId: string,
): void => {
  for (const directory of [requestsPath, responsesPath]) {
    try {
      fs.rmSync(path.join(directory, `${requestId}.json`), { force: true });
    } catch { /* best effort: an abandoned request must not raise a second error */ }
  }
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
  | ResidentActorStatusCommand;

// The only operations every format-1 host predating command negotiation understood.
const LEGACY_RESIDENT_COMMANDS = ["spawn", "foreground", "cleanup", "createActor", "removeActor"] as const;
export const RESIDENT_COMMANDS = [
  ...LEGACY_RESIDENT_COMMANDS, "actors", "actorStatus", "setInstructions", "setModel",
  "setThinking", "setTools", "setActivationFilter",
] as const satisfies readonly ResidentCommand["operation"][];

export const isResidentCommandOperation = (operation: unknown): operation is ResidentCommand["operation"] =>
  typeof operation === "string" && (RESIDENT_COMMANDS as readonly string[]).includes(operation);

export class ResidentCommandUnsupportedError extends Error {
  readonly code = "RESIDENT_COMMAND_UNSUPPORTED" as const;
  constructor(message = "The owning resident host runs an older release; it is relaunched on the current release at its next idle point; retry then") {
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
