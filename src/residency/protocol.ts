import type { TaskReturnAddress } from "../agents/task-return-address.js";
import type { FabricParticipantInfo } from "../topology/types.js";
import type { FabricPrincipal } from "../fabric-provenance.js";
import { createHash, randomUUID } from "node:crypto";
import type { ResidentReleaseIntent, ResidentLauncherIdentity } from "./handover.js";
import { recordResidentOutcome, registerCancellationEffect } from "../async-settlement.js";
import { readFileRetrying } from "../core/atomic-write.js";
import fs from "node:fs";
import { writeJsonAtomic } from "../core/atomic-write.js";
import { assertResidentRequestNotExpired, newResidentRequestId, residentRequestGeneration, ResidentRequestExpiredError, RESIDENT_EXPIRING_COMMAND_FORMAT } from "./request-expiry.js";
import path from "node:path";
import type { FabricOwnedModelGuidance } from "../components/model-guidance.js";
import type { FabricModelAliases, FabricModelCandidate } from "../core/model-resolution.js";
import type { FabricActorsConfig, FabricAgentConfig, FabricMeshConfig, FabricRetentionConfig } from "../config.js";
import type { FabricActorInfo, FabricActorCreateRequest, FabricActorBindingScope, FabricActorActivationFilter } from "../actors/types.js";
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
  /** Only format-3 requests are eligible for generation collection. */
  requestFormat?: 3;
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
  assertResidentRequestNotExpired(residencyRoot, command.requestId, command.format);
  if (decideResidentRequest(residencyRoot, {
    requestId: command.requestId, state: "committed", operation: command.operation, id, ownerHostId,
    ...(command.format === RESIDENT_EXPIRING_COMMAND_FORMAT ? { requestFormat: RESIDENT_EXPIRING_COMMAND_FORMAT } : {}),
    ...(("caller" in command && command.caller && "principal" in command.caller && command.caller.principal) ? { principal: command.caller.principal } : {}),
  })) {
    // A collector may advance expiry between the precheck and hard-link CAS.
    // Once it deletes a fence the watermark is already durable: recheck before mutation.
    assertResidentRequestNotExpired(residencyRoot, command.requestId, command.format);
    return;
  }
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
  requestFormat?: number,
): ResidentRequestDecision => {
  const root = path.dirname(requestsPath);
  assertResidentRequestNotExpired(root, requestId);
  decideResidentRequest(root, { requestId, state: "abandoned",
    ...(requestFormat === RESIDENT_EXPIRING_COMMAND_FORMAT ? { requestFormat: RESIDENT_EXPIRING_COMMAND_FORMAT } : {}),
  });
  // Collection can durably expire this generation and remove a committed fence
  // between our precheck and CAS. A replacement abandonment is not proof that
  // the original work never committed: fail before acknowledgement or unlink.
  assertResidentRequestNotExpired(root, requestId);
  const decision = readResidentRequestDecision(root, requestId)!;
  if (decision.state === "abandoned") {
    acknowledgeResidentResponse(root, { format: 1, requestId, ok: false, completedAt: Date.now() }, Date.now(), requestFormat);
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
    const kind = ["spawn", "spawnBound", "foreground", "cleanup"].includes(command.operation) ? "agent" : "actor";
    // Guest runtimes may preserve only message, so the classification and IDs live there too.
    super(`ResidentOutcomeUnknownError: Fabric residency ${command.operation} outcome unknown: requestId=${command.requestId}` +
      `, ${kind}Id=${id ?? "not yet known"}` +
      `${decision?.ownerHostId ? `, ownerHostId=${decision.ownerHostId}` : ""}` +
      // smarty-dev#6829: the request journal committed; the registry save may still be retrying.
      `${decision?.state === "committed" ? `, state=accepted; ${kind === "actor" ? "registry save" : "publication"} pending` : ""}. ` +
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

/** Expiry forbids replay but never proves rejection. Read retained live fences
 * even below the watermark; collection eligibility and outcome are independent.
 * A collected (or unreadable) fence preserves expiry without inventing an ID. */
export const residentRequestExpiredOutcome = (
  residencyRoot: string, command: ResidentCommand, signal?: AbortSignal,
): ResidentRequestExpiredError => {
  let decision: ResidentRequestDecision | undefined;
  try { decision = readResidentRequestDecision(residencyRoot, command.requestId); } catch { /* unknown fence */ }
  const committed = decision?.state === "committed" ? decision : undefined;
  const operation = committed?.operation ?? command.operation;
  return new ResidentRequestExpiredError(command.requestId, {
    requestId: command.requestId, state: committed ? "committed" : "expired", expired: true,
    operation,
    entityKind: ["spawn", "foreground", "cleanup"].includes(operation) ? "agent" : "actor",
    ...(committed?.id ? { id: committed.id } : {}),
    ...(committed?.ownerHostId ? { ownerHostId: committed.ownerHostId } : {}),
  }, signal);
};

/** Install before request publication; outer abort races can now settle the same fence. */
export const registerResidentCancellation = (
  signal: AbortSignal | undefined,
  residencyRoot: string,
  command: ResidentCommand,
): void => {
  // A committed receipt is immutable. Reuse its first error instead of nesting
  // already-formatted uncertainty again at each enclosing cancellation gate.
  let committedOutcome: ResidentOutcomeUnknownError | ResidentRequestExpiredError | undefined;
  registerCancellationEffect(signal, (reason) => {
    if (committedOutcome) return committedOutcome;
    let decision: ResidentRequestDecision | undefined;
    try {
      decision = abandonResidentRequest(path.join(residencyRoot, "requests"), path.join(residencyRoot, "responses"), command.requestId, command.format);
    } catch (error) {
      if (error instanceof ResidentRequestExpiredError) return committedOutcome = residentRequestExpiredOutcome(residencyRoot, command, signal);
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
        `${typeof command.id === "string" ? ` of ${command.id}` : ""} for ${formatAge(age)}; its response may still be settling`);
    }
  } catch { /* nothing in process */ }
  try {
    const health = JSON.parse(fs.readFileSync(path.join(residencyRoot, "request-retention.json"), "utf8")) as {
      entries?: number; bytes?: number; unknown?: number; legacy?: number; error?: string;
    };
    if (typeof health.entries === "number" && typeof health.bytes === "number") {
      notes.push(`residency retention: ${health.entries} entries, ${health.bytes} bytes, unknown=${health.unknown ?? 0}, legacy=${health.legacy ?? 0}${health.error ? `; ${health.error}` : ""}`);
    }
  } catch { /* no completed capacity sample yet */ }
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

/** The resident's project and session actor registry roots (shared by the host and the offline remove). */
export const residentActorRoots = (config: ResidentHostConfig): { project: string; session: string } =>
  config.sessionActorRoot
    ? { project: config.actorRoot, session: config.sessionActorRoot }
    : config.mesh.actorScope === "session"
      ? { project: path.dirname(config.actorRoot), session: config.actorRoot }
      : { project: config.actorRoot, session: path.join(config.actorRoot, config.sessionId) };

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
  /** Current host-owned Main name; each admitted run snapshots it in its launch manifest. */
  mainName?: string;
  mainStartedAt?: number;
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
  /** Launcher-owned recovery for a live child whose file lease stopped renewing. Unsupported (always disabled) on win32. */
  watchdog?: {
    enabled?: boolean;
    /** Lease age that identifies a wedge. Default 180000 ms. */
    stallMs?: number;
    /** Do not inspect for a wedge until the first lease write plus this allowance. Default 900000 ms. */
    coldStartMs?: number;
    /** Sampling interval. Default 30000 ms. */
    intervalMs?: number;
    /** Reserved restart ceiling (default/hard cap 3). Automatic respawn is currently fail-closed without whole-attempt containment. */
    maxRestartsPerHour?: number;
  };
  /** Absent in a config an older release wrote: the defaults apply. */
  actors?: FabricActorsConfig;
  /** Host-only shadow gates; old snapshots refuse optional inference. */
  shadowRouting?: import("../agents/model-route-owner.js").ShadowRoutePolicy;
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
  /** Binds ready custody transfer to the exact client-owned launch attempt. */
  launchToken?: string;
  startedAt: number;
  readyAt: number;
  /** With v1, usable readiness additionally requires a same-token post-lease maintenance receipt. */
  maintenanceReady?: 1;
  /** The immutable entry path this owner actually loaded, not the mutable config selector. */
  fabricExtensionPath?: string;
  /** Commands supported by this running binary; absent on pre-negotiation hosts. */
  commands?: readonly string[];
  /** New clients must not dispatch mutations to an already-running pre-fence host. */
  requestFence?: 1;
  /** Loaded executor validates and applies a trusted per-launch caller return address. */
  callerBoundSpawn?: 1;
  /** Generation format 3 with durable expiry; absent on older fenced hosts. */
  requestExpiry?: 1;
  /** Operation-scoped retry keys implemented by this loaded host, not desired config. */
  creationIdempotency?: 1;
  /** Attestation from the loaded host, never desired config.json. */
  releaseRoot?: string;
  configDigest?: string;
  handover?: { abi: "fabric-resident-1"; launcher: ResidentLauncherIdentity };
  /** A staged successor has proved worker startup but admits no business work yet. */
  attempt?: { id: string; kind: "target" | "fallback" };
}

/** Runtime caller binding on the trusted residency envelope, never AgentRunRequest. */
export interface ResidentTaskCaller {
  id: string;
  rootId: string;
  sessionId: string;
  ownerHostId: string;
  ownerIdentityId: string;
  kind: FabricParticipantInfo["kind"];
  returnAddress: TaskReturnAddress;
}

/** Verify the envelope against the same live owner directory used by native control. */
export const assertResidentTaskCaller = (
  caller: ResidentTaskCaller | undefined,
  participant: FabricParticipantInfo | undefined,
  rootId: string,
): TaskReturnAddress => {
  const address = caller?.returnAddress;
  if (!caller || !participant || participant.stale || participant.remoteHost !== undefined ||
      participant.id !== caller.id || participant.rootId !== rootId || caller.rootId !== rootId ||
      participant.sessionId !== caller.sessionId || !caller.sessionId ||
      participant.ownerHostId !== caller.ownerHostId || participant.ownerIdentityId !== caller.ownerIdentityId ||
      participant.kind !== caller.kind || !address || address.spawnerId !== caller.id ||
      address.spawnerSessionId !== caller.sessionId || !Array.isArray(address.ancestors) ||
      !address.ancestors.includes(rootId) || address.ancestors.some(id => typeof id !== "string" || !id.trim()) ||
      !Array.isArray(address.escalationTargets) || address.escalationTargets.some(id =>
        typeof id !== "string" || !id.startsWith("session:") || !id.slice(8).trim())) {
    throw new Error("Durable agents.spawn requires a trusted live caller return-address binding; absent or forged binding refused. Nothing was launched.");
  }
  return structuredClone(address);
};

interface ResidentSpawnCommand {
  format: typeof RESIDENT_HOST_FORMAT;
  // A distinct wire operation prevents rollback hosts from ignoring caller.
  operation: "spawnBound";
  idempotencyKey?: string;
  requestId: string;
  rootId: string;
  request: AgentRunRequest;
  /** Host-captured runtime binding, separate from all task-supplied run settings. */
  caller: ResidentTaskCaller;
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
  idempotencyKey?: string;
  requestId: string;
  rootId: string;
  request: FabricActorCreateRequest;
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
  | ({ operation: "setInstructions"; id: string; replace?: boolean } & import("../actors/instructions-file.js").FabricActorInstructionsSource)
  | { operation: "resetSession"; id: string }
  | { operation: "stop"; id: string }
  | { operation: "setTools"; id: string; tools: string[] }
  | { operation: "setModel"; id: string; model?: string; modelReason?: string; scope: FabricActorBindingScope }
  | { operation: "setThinking"; id: string; thinking?: FabricThinking; scope: FabricActorBindingScope }
  | { operation: "setActivationFilter"; id: string; activationFilter: FabricActorActivationFilter | null; expiresAt?: number };

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

export interface ResidentOperatorActorCommand {
  format: typeof RESIDENT_ACTOR_COMMAND_FORMAT;
  operation: "operatorActor";
  action: "stop" | "remove";
  id: string;
  dryRun?: boolean;
  confirmDeadRoot?: string;
  /** remove: the operator's audited assertion that the root's Main process is gone (smarty-dev#7817). */
  mainStoppedAudit?: unknown;
  requestId: string;
  rootId: string;
  createdAt: number;
}

type LegacyResidentCommand =
  | ResidentSpawnCommand
  | ResidentCleanupCommand
  | ResidentForegroundCommand
  | ResidentRemoveActorCommand
  | ResidentCreateActorCommand
  | ResidentActorMutationCommand
  | ResidentActorStatusCommand
  | ResidentOperatorActorCommand
  | (ResidentReleaseIntent & { format: typeof RESIDENT_ACTOR_COMMAND_FORMAT; operation: "releaseChange";
      requestId: string; rootId: string; createdAt: number });

type ExpiringResidentCommand<T> = T extends unknown ? Omit<T, "format"> & { format: typeof RESIDENT_EXPIRING_COMMAND_FORMAT } : never;
export type ResidentCommand = LegacyResidentCommand | ExpiringResidentCommand<LegacyResidentCommand>;

/** Preserve older host compatibility without collecting its legacy requests. */
export const residentCommandForOwner = (command: ResidentCommand, owner: ResidentHostOwner): ResidentCommand =>
  owner.requestExpiry === 1 && command.format !== RESIDENT_EXPIRING_COMMAND_FORMAT
    ? { ...command, format: RESIDENT_EXPIRING_COMMAND_FORMAT, requestId: newResidentRequestId() }
    : command;

// The only operations every format-1 host predating command negotiation understood.
const LEGACY_RESIDENT_COMMANDS = ["spawn", "foreground", "cleanup", "createActor", "removeActor"] as const;
export const RESIDENT_COMMANDS = [
  "spawnBound", "foreground", "cleanup", "createActor", "removeActor", "actors", "actorStatus", "setInstructions", "setModel",
  "setThinking", "setTools", "setActivationFilter", "resetSession", "stop", "releaseChange", "operatorActor",
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
  // Negotiation and the new wire discriminant are independent fences: this
  // check prevents dispatch to an old live owner; spawnBound prevents a queued
  // request being run unbound if a rollback host later acquires the directory.
  if (operation === "spawnBound" && (owner.callerBoundSpawn !== 1 ||
      !Array.isArray(owner.commands) || !owner.commands.includes(operation))) {
    throw new ResidentCommandUnsupportedError(
      `The owning resident host ${owner.hostId} (pid ${owner.pid}${owner.releaseRoot ? `, release ${owner.releaseRoot}` : ""}) ` +
      "lacks caller-bound spawn support. Reload Main and complete resident host handover to the current release before retrying. No request was dispatched.",
    );
  }
  const supported = owner.commands === undefined ? LEGACY_RESIDENT_COMMANDS : owner.commands;
  if (!isResidentCommandOperation(operation) || !Array.isArray(supported) ||
      !(supported as readonly string[]).includes(operation)) {
    throw new ResidentCommandUnsupportedError();
  }
};

/** Fence explicit retry keys before publication; keep unkeyed calls compatible with older hosts. */
export const prepareResidentCreationCommand = (owner: ResidentHostOwner, command: ResidentCommand): ResidentCommand => {
  if (command.operation !== "spawnBound" && command.operation !== "createActor") return command;
  if (owner.creationIdempotency !== 1) {
    if (command.idempotencyKey !== undefined) {
      throw new ResidentCommandUnsupportedError(
        "The loaded resident host lacks creation idempotency-key support; activate a compatible resident host before retrying with the same key. No request was dispatched.",
      );
    }
    return command;
  }
  // One key per call, held in the envelope for any transport replay. Generate it
  // only after negotiation, so an unkeyed call can still use a pre-key host.
  return { ...command, idempotencyKey: command.idempotencyKey ?? randomUUID() };
};

export interface ResidentCommandResponse {
  format: typeof RESIDENT_HOST_FORMAT;
  requestId: string;
  ok: boolean;
  handle?: AgentHandleInfo;
  actor?: FabricActorInfo;
  actors?: FabricActorInfo[];
  operatorEvidence?: import("./operator-safety.js").ResidentOperatorEvidence;
  /** remove --dry-run: where the archive would go and the audit record it would keep (smarty-dev#7817). */
  plan?: { archiveRoot: string; audit: unknown };
  /** A removeActor that returned before the actor's in-flight run ended: the pending state. */
  pending?: string;
  cleaned?: boolean;
  error?: string;
  errorCode?: "RESIDENT_ACTOR_FORBIDDEN" | "RESIDENT_COMMAND_UNSUPPORTED" | "RESIDENT_REQUEST_EXPIRED" | "FABRIC_MODEL_DENIED" | "ACTOR_SESSION_RESET_CANCELLED";
  /** Allowlisted policy-refusal payload, never arbitrary host Error properties. */
  modelDenied?: { model: string; replacement?: string };
  completedAt: number;
}

export interface ResidentResponseAcknowledgement {
  format: 1;
  requestFormat: 3;
  requestId: string;
  completedAt: number;
  acknowledgedAt: number;
  pending?: string;
}

/** Persist consumption BEFORE unlinking a response; absence alone proves nothing. */
export const acknowledgeResidentResponse = (root: string, response: ResidentCommandResponse, now = Date.now(), requestFormat?: number): boolean => {
  if (requestFormat !== RESIDENT_EXPIRING_COMMAND_FORMAT || residentRequestGeneration(response.requestId) === undefined || response.errorCode === "RESIDENT_REQUEST_EXPIRED") return true;
  try {
    const acknowledgement: ResidentResponseAcknowledgement = {
      format: 1, requestFormat: RESIDENT_EXPIRING_COMMAND_FORMAT, requestId: response.requestId, completedAt: response.completedAt, acknowledgedAt: now,
      ...(response.pending ? { pending: response.pending } : {}),
    };
    writeJsonAtomic(path.join(root, "acknowledgements", `${response.requestId}.json`), acknowledgement);
    return true;
  } catch { return false; } // Success remains success; retain the unacknowledged exchange.
};

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
  /** Producer-owned classification. Only actor-output admits an actor sender; absent or
   * unknown classifications (including older/retained records) keep the label but no claim. */
  source?: "actor-output" | "fabric-host";
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
