import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import type { FabricOwnedModelGuidance } from "../components/model-guidance.js";
import type { FabricModelAliases, FabricModelCandidate } from "../core/model-resolution.js";
import type { FabricActorsConfig, FabricAgentConfig, FabricMeshConfig, FabricRetentionConfig } from "../config.js";
import type { FabricActorInfo, FabricActorRequest } from "../actors/types.js";
import type { AgentHandleInfo, AgentRunRequest } from "../agents/types.js";
import type { FabricKernel } from "../runtime/kernel.js";
import type { MeshIdentity } from "../mesh/store.js";
import type { ProcessIdentity } from "../core/process-identity.js";
import type { FabricParticipantRecord } from "../topology/types.js";
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
  /** Native Main identity at host launch; kept even after its participant record is removed. */
  rootOwner?: FabricParticipantRecord;
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
  processIdentity?: ProcessIdentity;
  token: string;
  startedAt: number;
  readyAt: number;
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

export type ResidentCommand =
  | ResidentSpawnCommand
  | ResidentCleanupCommand
  | ResidentForegroundCommand
  | ResidentRemoveActorCommand
  | ResidentCreateActorCommand;

export interface ResidentCommandResponse {
  format: typeof RESIDENT_HOST_FORMAT;
  requestId: string;
  ok: boolean;
  handle?: AgentHandleInfo;
  actor?: FabricActorInfo;
  /** A removeActor that returned before the actor's in-flight run ended: the pending state. */
  pending?: string;
  cleaned?: boolean;
  error?: string;
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
