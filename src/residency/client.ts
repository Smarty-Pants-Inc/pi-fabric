import { randomUUID } from "node:crypto";
import { FabricModelDeniedError } from "../core/model-policy.js";
import { throwIfAborted } from "../async-settlement.js";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { writeJsonAtomic } from "../core/atomic-write.js";
import type { FabricActorInfo, FabricActorRequest } from "../actors/types.js";
import type { FabricAgentLog, AgentHandleInfo, AgentRunRecord, AgentRunRequest, AgentRunResult } from "../agents/types.js";
import { readChildToolAllowlist } from "../core/child-tool-allowlist.js";
import { awaitAgentCwd } from "../agents/manager.js";
import { isFabricWorktreePath } from "../agents/worktree-paths.js";
import { executeFile, spawnDetached, resolveScriptRuntime } from "../agents/transports/process-utils.js";
import { readJsonlPage } from "../log-tail.js";
import { processStartTime, residentProcessAlive } from "./process-identity.js";
import {
  RESIDENT_HANDOVER_ABI, residentLaunchSpec, handoverActive, handoverPath, handoverOutcomePath,
  mainGenerationPath, readHandoverJson, type ResidentMainGeneration, type ResidentHandoverState,
} from "./handover.js";
import { kernelFenceAvailable } from "./file-lock.js";
import { runTreeExitVeto } from "../storage/retention.js";
import type { FabricOwnedModelGuidance } from "../components/model-guidance.js";
import type { FabricMainAgentTarget } from "../main-agent.js";
import { MeshStore, type MeshStateEntry } from "../mesh/store.js";
import { MeshBackgroundRetry } from "../core/atomic-write.js";
import { isMeshLockTimeout } from "../core/atomic-write.js";
import type { FabricParticipantSource } from "../topology/types.js";
import {
  abandonResidentRequest,
  ResidentActorAuthorizationError,
  ResidentCommandUnsupportedError,
  assertResidentCommandSupported,
  assertResidentActorMain,
  assertResidentActorToolCeiling,
  type ResidentActorCaller,
  ResidentOutcomeUnknownError,
  readResidentRequestDecision,
  registerResidentCancellation,
  commitResidentRequest,
  RESIDENT_HOST_FORMAT,
  RESIDENT_ACTOR_COMMAND_FORMAT,
  isResidentHostId,
  residentDeliveryPrefix,
  residentHostId,
  residentHostStateNote,
  residentResultPath,
  sleepUnlessAborted,
  type ResidentAgentMetadata,
  type ResidentCommand,
  type ResidentCommandResponse,
  type ResidentActorMutation,
  type ResidentDeliveryRecord,
  type ResidentHostConfig,
  type ResidentHostOwner,
  type ResidentPiModelState,
} from "./protocol.js";

// One-time cost per resident root: cold-starting the bundled pi binary plus
// extension loading can exceed 10s on slow runners (e.g. CI Windows), so give
// startup a generous budget. Idle exit still reclaims the processes.
const STARTUP_TIMEOUT_MS = 30_000;
// smarty-dev#883: the start is CPU-bound process boot, so its wall time grows
// with contention (a 1 s boot took 16 s at load 5 per core). Scale the budget
// by the 1-minute load per core, capped. Windows reports no load average (0).
// ponytail: load is a coarse proxy, but it needs no progress protocol.
const startupBudgetMs = (base: number): number => {
  const loadPerCore = os.loadavg()[0]! / Math.max(1, os.availableParallelism());
  return Math.round(base * Math.min(4, Math.max(1, loadPerCore)));
};
const COMMAND_TIMEOUT_MS = 30_000;
const HANDOVER_WAIT_MS = 180_000;
const STATUS_POLL_MS = 100;
const WATCHDOG_INTERVAL_MS = 5_000;
const WATCHDOG_MAX_BACKOFF_MS = 60_000;
const AGENT_ID_PATTERN = /^[a-f0-9]{32}$/;

const delay = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

const atomicWrite = (filePath: string, value: unknown): void => {
  writeJsonAtomic(filePath, value, { space: 2 });
};

const readJson = <T>(filePath: string): T | undefined => {
  try {
    return JSON.parse(fs.readFileSync(filePath, "utf8")) as T;
  } catch {
    return undefined;
  }
};

const terminal = (status: string): status is AgentRunResult["status"] =>
  status === "completed" || status === "failed" || status === "stopped" || status === "timed_out";

const samePath = (left: string, right: string): boolean => {
  try {
    return path.relative(fs.realpathSync.native(left), fs.realpathSync.native(right)) === "";
  } catch {
    return false;
  }
};

/** Refuse cleanup unless the selected repository still owns this worktree. */
const registeredWorktree = async (gitRoot: string, worktreePath: string): Promise<string> => {
  let output: string;
  try {
    output = (await executeFile("git", ["worktree", "list", "--porcelain"], {
      cwd: gitRoot,
      timeoutMs: 30_000,
    })).stdout;
  } catch (error) {
    const reason = error instanceof Error ? error.message : String(error);
    throw new Error(`Cannot validate durable worktree ${JSON.stringify(worktreePath)}: ${reason}`);
  }
  const registered = output
    .split(/\r?\n/)
    .filter((line) => line.startsWith("worktree "))
    .map((line) => line.slice("worktree ".length));
  const match = registered.find((candidate) => samePath(candidate, worktreePath));
  if (!match) {
    throw new Error(
      `Refusing durable worktree cleanup: ${JSON.stringify(worktreePath)} is not registered by ${JSON.stringify(gitRoot)}`,
    );
  }
  return match;
};

export interface ResidencyClientOptions {
  config: ResidentHostConfig;
  mesh: MeshStore;
  participants: FabricParticipantSource;
  mainAgent: FabricMainAgentTarget;
  piModelState?: () => ResidentPiModelState;
  onBackgroundComplete?: (result: AgentRunResult, delivered: () => void) => void;
  onResultConsumed?: (id: string) => void;
  hostPath?: string;
  /** Start budget before load scaling; tests shorten it. */
  startupTimeoutMs?: number;
  /** File-exchange deadline; tests shorten it without faking host execution. */
  commandTimeoutMs?: number;
}

export class ResidencyClient {
  readonly hostId: string;
  readonly #configPath: string;
  readonly #ownerPath: string;
  readonly #errorPath: string;
  readonly #requestsPath: string;
  readonly #responsesPath: string;
  readonly #agentsPath: string;
  readonly #inheritedToolAllowlist = readChildToolAllowlist();
  readonly #deliveryPrefix: string;
  readonly #hostPath: string;
  #deliveryTimer: NodeJS.Timeout | undefined;
  #modelGuidanceJson: string | undefined;
  #drainingDeliveries = false;
  readonly #backgroundDelivery = new MeshBackgroundRetry("resident delivery cleanup");
  #closed = false;
  #startingHost: Promise<ResidentHostOwner> | undefined;
  #startupLauncher: Awaited<ReturnType<typeof spawnDetached>> | undefined;
  #nextWatchdogAt = 0;
  #watchdogFailures = 0;
  #watchdogWork: string | undefined;
  readonly #runtimeNonce = randomUUID();
  #generationRecorded = false;
  readonly #releaseAbort = new AbortController();
  readonly #releaseOwners = new Set<string>();

  constructor(readonly options: ResidencyClientOptions) {
    this.hostId = residentHostId(options.config.rootId);
    this.#configPath = path.join(options.config.residencyRoot, "config.json");
    this.#ownerPath = path.join(options.config.residencyRoot, "owner.json");
    this.#errorPath = path.join(options.config.residencyRoot, "error.json");
    this.#requestsPath = path.join(options.config.residencyRoot, "requests");
    this.#responsesPath = path.join(options.config.residencyRoot, "responses");
    this.#agentsPath = path.join(options.config.residencyRoot, "agents");
    this.#deliveryPrefix = residentDeliveryPrefix(options.config.rootId);
    this.#hostPath = options.hostPath ?? fileURLToPath(new URL("./launcher.js", import.meta.url));
  }

  start(): void {
    if (this.#deliveryTimer || this.#closed || !this.options.mainAgent.local) return;
    this.syncPiModels();
    this.#deliveryTimer = setInterval(
      () => {
        void this.#backgroundDelivery.run(() => this.#drainDeliveries());
        void this.#watchdog().catch(() => undefined);
      },
      Math.max(20, this.options.config.mesh.actorPollMs),
    );
    this.#deliveryTimer.unref();
    void this.#backgroundDelivery.run(() => this.#drainDeliveries());
    // Every runtime activation, including manual native /reload, reconciles
    // a live owner. This never starts an empty root or loads an optional engine.
    void this.reconcileRelease().catch((error) => this.#deferRelease(error));
  }

  async close(): Promise<void> {
    this.#closed = true;
    // Disposing a runtime during native reload/shutdown ends its authority even
    // while the OS process remains alive. Never invalidate a newer runtime.
    if (this.#generationRecorded) {
      try {
        const recorded = readHandoverJson<ResidentMainGeneration>(mainGenerationPath(this.options.config.residencyRoot));
        if (recorded?.nonce === this.#runtimeNonce) writeJsonAtomic(mainGenerationPath(this.options.config.residencyRoot),
          { ...recorded, nonce: `closed:${this.#runtimeNonce}` }, { durable: true });
      } catch (error) { this.#deferRelease(error); }
    }
    this.#releaseAbort.abort();
    if (this.#deliveryTimer) clearInterval(this.#deliveryTimer);
    this.#deliveryTimer = undefined;
    while (this.#drainingDeliveries) await delay(10);
    await this.#startingHost?.catch(() => undefined);
    // A failed verification retains its owned handle so close can retry, or surface
    // the cleanup failure instead of silently reporting an aborted start as settled.
    await this.#stopStartingLauncher();
  }

  syncPiModels(): void {
    this.#refreshPiModels();
    if (fs.existsSync(this.options.config.residencyRoot)) {
      atomicWrite(this.#configPath, this.options.config);
    }
  }

  updateModelGuidance(guidance: readonly FabricOwnedModelGuidance[]): void {
    const snapshot: FabricOwnedModelGuidance[] = structuredClone([...guidance]);
    const serialized = JSON.stringify(snapshot);
    if (serialized === this.#modelGuidanceJson) return;
    this.#modelGuidanceJson = serialized;
    this.options.config.modelGuidance = snapshot;
    this.#refreshPiModels();
    if (fs.existsSync(this.options.config.residencyRoot)) {
      atomicWrite(this.#configPath, this.options.config);
    }
  }

  async ensureHost(): Promise<ResidentHostOwner> {
    if (this.#startingHost) return this.#startingHost;
    const starting = this.#startHost();
    this.#startingHost = starting;
    try { return await starting; }
    finally { this.#startingHost = undefined; }
  }

  async #stopStartingLauncher(): Promise<void> {
    const launcher = this.#startupLauncher;
    if (!launcher) return;
    await launcher.stop();
    if (this.#startupLauncher === launcher) this.#startupLauncher = undefined;
  }

  async #startHost(): Promise<ResidentHostOwner> {
    if (this.#closed) throw new Error("Fabric residency client is closed");
    await this.#stopStartingLauncher();
    this.#refreshPiModels();
    atomicWrite(this.#configPath, this.options.config);
    const existing = this.#liveOwner();
    if (existing) return existing;
    // The detached launcher already owns the exact attempt/fallback. Neither
    // ensureHost nor the watchdog may become a competing handover executor.
    const releaseDeadline = Date.now() + HANDOVER_WAIT_MS;
    while (handoverActive(readHandoverJson<ResidentHandoverState>(handoverPath(this.options.config.residencyRoot)))) {
      const state = readHandoverJson<ResidentHandoverState>(handoverPath(this.options.config.residencyRoot));
      if (this.#closed || state?.phase === "blocked" || Date.now() >= releaseDeadline) {
        throw new Error("Resident release transaction is in launcher custody; do not start a duplicate");
      }
      await delay(STATUS_POLL_MS);
    }
    const followed = this.#liveOwner();
    if (followed) return followed;
    fs.rmSync(this.#errorPath, { force: true });
    const launchToken = randomUUID();
    const launcher = await spawnDetached(
      this.#hostPath,
      ["--config", this.#configPath, "--launch-token", launchToken],
      this.options.config.cwd,
    );
    this.#startupLauncher = launcher;
    // The budget counts from the launcher's first sign of life (its
    // launcher-started trace), so its own boot does not consume it.
    const launcherBirth = processStartTime(launcher.pid);
    const budget = startupBudgetMs(this.options.startupTimeoutMs ?? STARTUP_TIMEOUT_MS);
    let deadline = Date.now() + budget;
    let started = false;
    let launcherExited = false;
    while (true) {
      if (this.#closed) {
        await this.#stopStartingLauncher();
        throw new Error("Fabric residency client is closed");
      }
      const owner = this.#liveOwner();
      if (owner) {
        // Only our attempt may transfer custody to durable residency. Another
        // winner does not prove our losing launcher (or restored work) exited.
        if (owner.launchToken !== launchToken) await this.#stopStartingLauncher();
        else this.#startupLauncher = undefined;
        return owner;
      }
      const failure = readJson<{ error?: unknown; launcherPid?: number; launcherBirth?: string }>(this.#errorPath);
      // An exiting prior launcher can race this start after error.json was
      // cleared. Its root diagnostic is not evidence about our owned attempt.
      if (typeof failure?.error === "string" && (failure.launcherPid === undefined ||
          (failure.launcherPid === launcher.pid && failure.launcherBirth === launcherBirth))) {
        await this.#stopStartingLauncher();
        throw new Error(`Fabric resident host failed to start: ${failure.error}`);
      }
      // An exited launcher cannot publish a new owner. Its last owner/error
      // states were read above; retained group cleanup still runs below.
      if (launcherExited) break;
      launcherExited = !(await launcher.isAlive());
      if (launcherExited) continue;
      if (!started && this.#launcherStarted(launcher.pid)) {
        started = true;
        deadline = Date.now() + budget;
      }
      if (Date.now() >= deadline) break;
      await delay(STATUS_POLL_MS);
    }
    // Work must not outlive its owner: end the launcher this call spawned (its
    // own process group, which holds its Pi child) before reporting the timeout.
    await this.#stopStartingLauncher();
    // Surface any launcher-recorded child output so a silent slow start (or a
    // quiet child crash) is diagnosable from the error alone.
    const readIfPresent = (name: string): string => {
      try { return fs.readFileSync(path.join(this.options.config.residencyRoot, name), "utf8").trim(); } catch { return ""; }
    };
    const childOutput = readIfPresent("child-stderr.log");
    const launcherLog = readIfPresent("launcher.log").split("\n").slice(-6).join("\n");
    const ownerState = readIfPresent("owner.json").slice(0, 300);
    const diagnostics = [
      childOutput ? `Child output: ${childOutput.slice(-500)}` : "",
      launcherLog ? `Launcher log: ${launcherLog}` : "",
      ownerState ? `Owner state: ${ownerState}` : "Owner state: absent",
    ].filter(Boolean).join(" | ");
    throw new Error(`${launcherExited ? "Launcher exited while starting" : `Timed out after ${budget}ms starting`} Fabric resident host ${this.hostId}. ${diagnostics}`);
  }

  /** Host-only Main lifecycle hook; not a guest-selected release path. */
  async reconcileRelease(): Promise<void> {
    if (this.#closed || !this.options.mainAgent.local || this.options.mainAgent.id !== this.options.config.rootId) return;
    const owner = this.#liveOwner();
    const state = readHandoverJson<ResidentHandoverState>(handoverPath(this.options.config.residencyRoot));
    // A new runtime must invalidate the old nonce even after A exited or while
    // B is staged. Business readiness is not the Main generation boundary.
    if (!owner && !handoverActive(state)) return;
    const self = this.options.participants.self();
    if (self.kind !== "root" || self.id !== this.options.config.rootId || self.sessionId !== this.options.config.sessionId) throw new ResidentActorAuthorizationError();
    const releaseRoot = path.resolve(path.dirname(this.#hostPath), "../..");
    const main: ResidentMainGeneration = { nonce: this.#runtimeNonce, pid: process.pid,
      processStartTime: processStartTime(process.pid) ?? "", rootId: this.options.config.rootId,
      sessionId: this.options.config.sessionId, releaseRoot };
    if (!this.#generationRecorded) {
      writeJsonAtomic(mainGenerationPath(this.options.config.residencyRoot), main, { durable: true });
      this.#generationRecorded = true;
    }
    if (!owner || handoverActive(state) || this.#releaseOwners.has(owner.token)) return;
    this.#releaseOwners.add(owner.token);
    if (owner.releaseRoot === releaseRoot) return;
    if (owner.handover?.abi !== RESIDENT_HANDOVER_ABI || !owner.commands?.includes("releaseChange")) {
      this.#deferRelease(new Error("Legacy resident host/launcher has no release custody protocol; installer drain required")); return;
    }
    this.#refreshPiModels();
    // A bundled Main's execPath is Pi, not a JavaScript interpreter. Resolve
    // on this initiating generation and freeze the same runtime workers use.
    const runtime = await resolveScriptRuntime({ execPath: process.execPath });
    const target = residentLaunchSpec(this.options.config, path.join(path.dirname(this.#hostPath), "pi-entry.js"), runtime);
    if (readHandoverJson(handoverOutcomePath(this.options.config.residencyRoot, target))) return;
    await this.#command({ format: RESIDENT_ACTOR_COMMAND_FORMAT, operation: "releaseChange",
      requestId: randomUUID(), rootId: main.rootId, createdAt: Date.now(), main, target,
      caller: { identity: { id: self.id, name: self.name, kind: "main", sessionId: main.sessionId }, hostId: self.ownerHostId },
    }, this.#releaseAbort.signal);
  }

  #deferRelease(error: unknown): void {
    try { writeJsonAtomic(path.join(this.options.config.residencyRoot, "handover-deferred.json"), {
      at: Date.now(), release: this.options.config.fabricExtensionPath,
      reason: error instanceof Error ? error.message : String(error),
    }); } catch { /* Deferred is diagnostic; never permission to exit A. */ }
  }

  #refreshPiModels(): void {
    const state = this.options.piModelState?.();
    if (state) this.options.config.piModels = structuredClone(state);
  }

  async ensureActor(id: string): Promise<void> {
    await this.ensureHost();
    await this.#waitForParticipant(id, "actor");
  }

  async createActor(request: FabricActorRequest, signal?: AbortSignal): Promise<FabricActorInfo> {
    await this.ensureHost();
    const response = await this.#command({
      format: RESIDENT_HOST_FORMAT,
      operation: "createActor",
      requestId: randomUUID(),
      rootId: this.options.config.rootId,
      request,
      createdAt: Date.now(),
    }, signal);
    if (!response.actor) throw new Error("Fabric resident host returned no actor");
    await this.#waitForParticipant(response.actor.id, "actor", signal).catch((error) => {
      throw new ResidentOutcomeUnknownError({
        format: RESIDENT_HOST_FORMAT, operation: "createActor", requestId: response.requestId,
        rootId: this.options.config.rootId, request, createdAt: Date.now(),
      }, { requestId: response.requestId, state: "committed", id: response.actor!.id, ownerHostId: this.hostId }, error, signal);
    });
    return response.actor;
  }

  /** Setters/status use only an existing owner, never create hidden residency. */
  async setActor(mutation: ResidentActorMutation, signal?: AbortSignal, caller?: ResidentActorCaller): Promise<FabricActorInfo> {
    const self = this.options.participants.self();
    if (!this.options.mainAgent.local || this.options.mainAgent.id !== this.options.config.rootId ||
      self.kind !== "root" || self.id !== this.options.config.rootId || !self.sessionId) {
      throw new ResidentActorAuthorizationError();
    }
    caller ??= { identity: { id: self.id, name: self.name, kind: "main", sessionId: self.sessionId }, hostId: self.ownerHostId };
    assertResidentActorMain(caller, this.options.config.rootId);
    if (this.#inheritedToolAllowlist !== undefined) caller = { ...caller, toolCeiling: [...this.#inheritedToolAllowlist] };
    if (mutation.operation === "setTools") assertResidentActorToolCeiling(mutation.tools, caller.toolCeiling);
    if (!this.#liveOwner()) throw new Error("Root resident host is not live");
    const response = await this.#command({
      ...mutation, caller, format: RESIDENT_ACTOR_COMMAND_FORMAT, requestId: randomUUID(),
      rootId: this.options.config.rootId, createdAt: Date.now(),
    }, signal);
    if (!response.actor) throw new Error("Resident host returned no actor from setter");
    return response.actor;
  }

  async actorStatus(id: string, signal?: AbortSignal): Promise<FabricActorInfo> {
    if (!this.#liveOwner()) throw new Error("Root resident host is not live");
    const response = await this.#command({
      format: RESIDENT_ACTOR_COMMAND_FORMAT, operation: "actorStatus", id,
      requestId: randomUUID(), rootId: this.options.config.rootId, createdAt: Date.now(),
    }, signal);
    if (!response.actor) throw new Error("Resident host returned no actor status");
    return response.actor;
  }

  async actors(signal?: AbortSignal): Promise<FabricActorInfo[]> {
    if (!this.#liveOwner()) throw new Error("Root resident host is not live");
    const response = await this.#command({
      format: RESIDENT_ACTOR_COMMAND_FORMAT, operation: "actors", requestId: randomUUID(),
      rootId: this.options.config.rootId, createdAt: Date.now(),
    }, signal);
    if (!response.actors) throw new Error("Resident host returned no actors");
    return response.actors;
  }

  async spawnAgent(request: AgentRunRequest, signal?: AbortSignal): Promise<AgentHandleInfo> {
    const resolvedRequest = request.cwd === undefined
      ? request
      : { ...request, cwd: await awaitAgentCwd(this.options.config.cwd, request.cwd, signal) };
    // Freeze inherited optional-tool authority before transferring to an existing host.
    const allowedTools = this.#inheritedToolAllowlist;
    const tools = allowedTools === undefined ? undefined
      : (request.tools ?? this.options.config.agents.defaultTools).filter((tool) => allowedTools.has(tool));
    await this.ensureHost();
    const response = await this.#command(
      {
        format: RESIDENT_HOST_FORMAT,
        operation: "spawn",
        requestId: randomUUID(),
        rootId: this.options.config.rootId,
        request: { ...resolvedRequest, ...(tools ? { tools } : {}), residency: "durable" },
        createdAt: Date.now(),
      },
      signal,
    );
    if (!response.handle) throw new Error("Fabric resident host returned no agent handle");
    await this.#waitForParticipant(response.handle.id, "agent", signal).catch((error) => {
      throw new ResidentOutcomeUnknownError({
        format: RESIDENT_HOST_FORMAT, operation: "spawn", requestId: response.requestId,
        rootId: this.options.config.rootId, request, createdAt: Date.now(),
      }, { requestId: response.requestId, state: "committed", id: response.handle!.id, ownerHostId: this.hostId }, error, signal);
    });
    return response.handle;
  }

  hasAgent(id: string): boolean {
    return AGENT_ID_PATTERN.test(id) && fs.existsSync(this.#metadataPath(id));
  }

  statusAgent(id: string): AgentRunRecord | AgentHandleInfo {
    const metadata = this.#metadata(id);
    if (!metadata) throw new Error(`Unknown durable Fabric agent: ${id}`);
    const record = this.#record(metadata);
    if (!record) return structuredClone(metadata.handle);
    return {
      ...record,
      cwd: metadata.handle.cwd,
      ...(metadata.handle.kernel ? { kernel: metadata.handle.kernel } : {}),
      ...(metadata.handle.recursive ? { recursive: true } : {}),
      residency: "durable",
      logFile: path.join(metadata.runDirectory, "events.jsonl"),
      ...(metadata.handle.sessionId ? { sessionId: metadata.handle.sessionId } : {}),
      ...(metadata.handle.attachCommand ? { attachCommand: metadata.handle.attachCommand } : {}),
    };
  }

  /**
   * The result of a durable run known to be settled: the host's saved terminal record, or any
   * terminal status once no host owns the run. A worker's terminal status.json alone is not
   * settlement while a host lives: a stopped or failed attempt can still resume or retry, so a
   * stop must go to the host (review/astra on pi-fabric#136).
   */
  settledAgent(id: string): AgentRunResult | undefined {
    const metadata = this.#metadata(id);
    if (!metadata) return undefined;
    const saved = readJson<AgentRunRecord>(residentResultPath(this.options.config.residencyRoot, id));
    if (!(saved?.id === id && terminal(saved.status)) && this.#liveOwner()) return undefined;
    const status = this.statusAgent(id);
    return terminal(status.status) && "startedAt" in status ? status as AgentRunResult : undefined;
  }

  listAgents(): Array<AgentRunRecord | AgentHandleInfo> {
    let entries: string[];
    try {
      entries = fs.readdirSync(this.#agentsPath);
    } catch {
      return [];
    }
    return entries
      .filter((entry) => entry.endsWith(".json"))
      .flatMap((entry) => {
        try {
          return [this.statusAgent(entry.slice(0, -5))];
        } catch {
          return [];
        }
      });
  }

  acknowledgeCompletion(id: string): void {
    const metadata = this.#metadata(id);
    if (!metadata) return;
    if (!metadata.completionConsumedAt) {
      atomicWrite(this.#metadataPath(id), { ...metadata, completionConsumedAt: Date.now() });
    }
    this.options.onResultConsumed?.(id);
  }

  async waitAgent(id: string, signal?: AbortSignal, deferConsumption?: (consume: () => void, abandon?: () => void) => void): Promise<AgentRunResult> {
    while (true) {
      if (signal?.aborted) throw new Error(`Waiting for durable Fabric agent ${id} was aborted`);
      const status = this.statusAgent(id);
      if (terminal(status.status) && "startedAt" in status) {
        if (deferConsumption) deferConsumption(() => this.acknowledgeCompletion(id));
        else this.acknowledgeCompletion(id);
        return status as AgentRunResult;
      }
      await sleepUnlessAborted(STATUS_POLL_MS, signal).catch(() => undefined);
    }
  }

  readAgentLog(id: string, options: { lines?: number; before?: number; beforeGeneration?: string } = {}): FabricAgentLog {
    const metadata = this.#metadata(id);
    if (!metadata) throw new Error(`Unknown durable Fabric agent: ${id}`);
    const logFile = path.join(metadata.runDirectory, "events.jsonl");
    const page = readJsonlPage(logFile, Math.max(1, Math.min(options.lines ?? 200, 5_000)), options.before, undefined, options.beforeGeneration);
    const status = this.#record(metadata);
    return {
      id,
      runDirectory: metadata.runDirectory,
      logFile,
      ...(status ? { status: { ...status, cwd: metadata.handle.cwd, residency: "durable" } } : {}),
      events: page.lines,
      hasMore: page.hasMore,
      ...(page.before !== undefined ? { before: page.before } : {}),
      ...(page.generation !== undefined ? { generation: page.generation } : {}),
    };
  }

  async removeActor(id: string, signal?: AbortSignal): Promise<{ removed: boolean; pending?: string; cleaned?: boolean }> {
    await this.ensureHost();
    const response = await this.#command({
      format: RESIDENT_HOST_FORMAT,
      operation: "removeActor",
      requestId: randomUUID(),
      rootId: this.options.config.rootId,
      id,
      createdAt: Date.now(),
    }, signal);
    return { removed: true, ...(response.pending === undefined ? {} : { pending: response.pending }),
      ...(response.cleaned === undefined ? {} : { cleaned: response.cleaned }) };
  }

  /** Pending removals and a long request on the host, for error messages (smarty-dev#2184). */
  hostStateNote(): string {
    return residentHostStateNote(this.options.config.residencyRoot);
  }

  async cleanupAgent(id: string, deleteBranch = false, signal?: AbortSignal): Promise<{ cleaned: boolean }> {
    throwIfAborted(signal);
    const metadata = this.#metadata(id);
    if (!metadata) throw new Error(`Unknown durable Fabric agent: ${id}`);
    if (!this.#liveOwner()) return this.#cleanupTerminalFiles(metadata, deleteBranch, signal);
    let response: ResidentCommandResponse;
    try {
      response = await this.#command({
        format: RESIDENT_HOST_FORMAT,
        operation: "cleanup",
        requestId: randomUUID(),
        rootId: this.options.config.rootId,
        id,
        deleteBranch,
        createdAt: Date.now(),
      }, signal);
    } catch (error) {
      if (!(error instanceof ResidentOutcomeUnknownError) && error instanceof Error && /Unknown Fabric agent/.test(error.message)) {
        return this.#cleanupTerminalFiles(metadata, deleteBranch, signal);
      }
      throw error;
    }
    if (!response.ok) throw new Error(response.error ?? `Failed to clean durable Fabric agent ${id}`);
    this.options.onResultConsumed?.(id);
    return { cleaned: true };
  }

  async #cleanupTerminalFiles(
    metadata: ResidentAgentMetadata,
    deleteBranch: boolean,
    signal?: AbortSignal,
  ): Promise<{ cleaned: boolean }> {
    throwIfAborted(signal);
    const status = this.statusAgent(metadata.id);
    if (!("startedAt" in status) || !terminal(status.status)) {
      throw new Error(`Cannot clean up running durable Fabric agent ${metadata.id}`);
    }
    const exitVeto = runTreeExitVeto(metadata.runDirectory);
    if (exitVeto) {
      throw new Error(
        `Cannot clean up durable Fabric agent ${metadata.id}: ${exitVeto} ` +
        `(see ${metadata.runDirectory}). Check the worker, then remove its files by hand.`,
      );
    }
    const command: ResidentCommand = { format: RESIDENT_HOST_FORMAT, operation: "cleanup", requestId: randomUUID(),
      rootId: this.options.config.rootId, id: metadata.id, deleteBranch, createdAt: Date.now() };
    let commitAttempted = false;
    const commit = (): void => {
      throwIfAborted(signal);
      registerResidentCancellation(signal, this.options.config.residencyRoot, command);
      commitAttempted = true;
      commitResidentRequest(this.options.config.residencyRoot, command, metadata.id, this.hostId);
    };
    try {
      if (metadata.handle.worktree) {
        const gitRoot = metadata.worktreeGitRoot ?? this.options.config.projectRoot;
        const worktree = await registeredWorktree(gitRoot, metadata.handle.worktree);
        commit();
        await executeFile(
          "git",
          ["worktree", "remove", "--force", worktree],
          { cwd: gitRoot, timeoutMs: 60_000 },
        );
        if (deleteBranch && metadata.handle.branch) {
          throwIfAborted(signal);
          await executeFile(
            "git",
            ["branch", "-D", metadata.handle.branch],
            { cwd: gitRoot, timeoutMs: 30_000 },
          );
        }
      } else if (deleteBranch) {
        throw new Error(`Durable Fabric agent ${metadata.id} has no worktree branch to delete`);
      } else {
        commit();
      }
      throwIfAborted(signal);
      fs.rmSync(metadata.runDirectory, { recursive: true, force: true });
      fs.rmSync(this.#metadataPath(metadata.id), { force: true });
      fs.rmSync(residentResultPath(this.options.config.residencyRoot, metadata.id), { force: true });
      this.options.onResultConsumed?.(metadata.id);
      return { cleaned: true };
    } catch (error) {
      if (!commitAttempted) throw error;
      let decision;
      try { decision = readResidentRequestDecision(this.options.config.residencyRoot, command.requestId); } catch { /* unknown fence */ }
      if (decision?.state === "abandoned") throw error;
      // Partial cleanup or an unreadable fence is not a proved rejection either.
      throw new ResidentOutcomeUnknownError(command, decision, error, signal);
    }
  }

  async #command(command: ResidentCommand, signal?: AbortSignal): Promise<ResidentCommandResponse> {
    const owner = this.#liveOwner();
    if (!owner) throw new Error("Root resident host is not live");
    assertResidentCommandSupported(owner, command.operation);
    if (owner.requestFence !== 1) {
      throw new Error("Fabric resident host lacks the abandonment fence; restart the resident host before retrying. No request was dispatched.");
    }
    throwIfAborted(signal);
    registerResidentCancellation(signal, this.options.config.residencyRoot, command);
    const responsePath = path.join(this.#responsesPath, `${command.requestId}.json`);
    try {
      atomicWrite(path.join(this.#requestsPath, `${command.requestId}.json`), command);
      const deadline = Date.now() + (this.options.commandTimeoutMs ?? COMMAND_TIMEOUT_MS);
      while (Date.now() < deadline) {
        if (signal?.aborted) throw new Error("Fabric residency request was aborted");
        const response = readJson<ResidentCommandResponse>(responsePath);
        if (response?.format === RESIDENT_HOST_FORMAT && response.requestId === command.requestId) {
          fs.rmSync(responsePath, { force: true });
          if (!response.ok) {
            if (response.errorCode === "RESIDENT_ACTOR_FORBIDDEN") throw new ResidentActorAuthorizationError(response.error);
            if (response.errorCode === "RESIDENT_COMMAND_UNSUPPORTED") throw new ResidentCommandUnsupportedError(response.error);
            if (response.errorCode === "FABRIC_MODEL_DENIED" && typeof response.modelDenied?.model === "string") {
              throw new FabricModelDeniedError(response.modelDenied.model,
                typeof response.modelDenied.replacement === "string" ? response.modelDenied.replacement : undefined);
            }
            throw new Error(response.error ?? "Fabric resident host rejected request");
          }
          if ((command.operation === "spawn" && !response.handle) || (command.operation === "createActor" && !response.actor)) {
            throw new Error("Fabric resident host returned no created entity");
          }
          return response;
        }
        const owner = this.#liveOwner();
        if (!owner) throw new Error("Fabric resident host exited while processing a request");
        await sleepUnlessAborted(STATUS_POLL_MS, signal).catch(() => undefined);
      }
      const note = this.hostStateNote();
      throw new Error(`Timed out waiting for Fabric residency request ${command.requestId}` +
        ` (${command.operation})${note ? `: ${note}` : ""}`);
    } catch (error) {
      let decision;
      try {
        decision = abandonResidentRequest(this.#requestsPath, this.#responsesPath, command.requestId);
      } catch (fenceError) {
        // No proven abandonment: never report a safe-to-retry rejection.
        let known;
        try { known = readResidentRequestDecision(this.options.config.residencyRoot, command.requestId); } catch { /* unreadable fence */ }
        throw new ResidentOutcomeUnknownError(command, known, fenceError, signal);
      }
      if (decision.state === "committed") throw new ResidentOutcomeUnknownError(command, decision, error, signal);
      throw error;
    }
  }

  async #waitForParticipant(id: string, kind: "actor" | "agent", signal?: AbortSignal): Promise<void> {
    const deadline = Date.now() + STARTUP_TIMEOUT_MS;
    while (Date.now() < deadline) {
      if (signal?.aborted) throw new Error("Fabric residency publication wait was aborted");
      const participant = this.options.participants.get(id);
      if (
        participant?.kind === kind &&
        participant.ownerHostId === this.hostId &&
        participant.residency === "durable" &&
        !participant.stale
      ) {
        return;
      }
      await sleepUnlessAborted(STATUS_POLL_MS, signal);
    }
    throw new Error(`Timed out publishing durable Fabric ${kind} ${id} from ${this.hostId}`);
  }

  /**
   * The run's live status, else the terminal record the host saved before an idle exit removed
   * the run directory. With neither, no run directory and no live host, the run cannot still be
   * running: report it failed rather than the stale spawn handle (smarty-dev#1882).
   */
  #record(metadata: ResidentAgentMetadata): AgentRunRecord | undefined {
    const saved = readJson<AgentRunRecord>(residentResultPath(this.options.config.residencyRoot, metadata.id));
    if (saved?.id === metadata.id && terminal(saved.status)) return saved;
    const live = readJson<AgentRunRecord>(path.join(metadata.runDirectory, "status.json"));
    if (live?.id === metadata.id) return live;
    if (fs.existsSync(metadata.runDirectory) || this.#liveOwner()) return undefined;
    const { handle } = metadata;
    return {
      id: handle.id,
      name: handle.name,
      task: "",
      status: "failed",
      runner: handle.runner,
      transport: handle.transport,
      cwd: handle.cwd,
      ...(handle.model ? { model: handle.model } : {}),
      startedAt: metadata.createdAt,
      updatedAt: metadata.updatedAt,
      turns: 0,
      toolCalls: 0,
      usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
      error: "Durable run record lost: its resident host exited and removed the run directory " +
        "before this Fabric version kept terminal results; the outcome is unknown.",
    } as AgentRunRecord;
  }

  #metadataPath(id: string): string {
    return path.join(this.#agentsPath, `${id}.json`);
  }

  #metadata(id: string): ResidentAgentMetadata | undefined {
    if (!AGENT_ID_PATTERN.test(id)) return undefined;
    const metadata = readJson<ResidentAgentMetadata>(this.#metadataPath(id));
    if (
      metadata?.format !== RESIDENT_HOST_FORMAT ||
      metadata.rootId !== this.options.config.rootId ||
      metadata.id !== id ||
      metadata.handle.id !== id ||
      (metadata.worktreeGitRoot !== undefined && typeof metadata.worktreeGitRoot !== "string") ||
      path.resolve(metadata.runDirectory) !==
        path.resolve(this.options.config.residencyRoot, "runs", id)
    ) {
      return undefined;
    }
    if (metadata.handle.worktree && !isFabricWorktreePath(metadata.handle.worktree, id)) {
      return undefined;
    }
    if (
      metadata.handle.branch &&
      (!metadata.handle.branch.startsWith("pi-fabric/") ||
        !metadata.handle.branch.endsWith(`-${id.slice(0, 8)}`))
    ) {
      return undefined;
    }
    return metadata;
  }

  #launcherStarted(pid: number): boolean {
    let log: string;
    try { log = fs.readFileSync(path.join(this.options.config.residencyRoot, "launcher.log"), "utf8"); } catch { return false; }
    return log.split("\n").some((line) => {
      try {
        const entry = JSON.parse(line) as { event?: unknown; pid?: unknown };
        return entry.event === "launcher-started" && entry.pid === pid;
      } catch { return false; }
    });
  }

  #liveOwner(): ResidentHostOwner | undefined {
    const owner = readJson<ResidentHostOwner>(this.#ownerPath);
    if (
      owner?.format !== RESIDENT_HOST_FORMAT ||
      owner.hostId !== this.hostId ||
      !Number.isSafeInteger(owner.pid) ||
      !residentProcessAlive(owner.pid, owner.processStartTime)
    ) {
      return undefined;
    }
    if (owner.attempt) {
      const state = readHandoverJson<ResidentHandoverState>(handoverPath(this.options.config.residencyRoot));
      if (state?.plan.id !== owner.attempt.id || state.phase !== (owner.attempt.kind === "target" ? "complete" : "fallback")) return undefined;
    }
    return owner;
  }

  async #watchdog(): Promise<void> {
    const now = Date.now();
    if (this.#closed || this.#startingHost || now < this.#nextWatchdogAt || !kernelFenceAvailable()) return;
    this.#nextWatchdogAt = now + WATCHDOG_INTERVAL_MS;
    if (this.#liveOwner()) {
      void this.reconcileRelease().catch((error) => this.#deferRelease(error));
      return;
    }
    if (handoverActive(readHandoverJson<ResidentHandoverState>(handoverPath(this.options.config.residencyRoot)))) return;
    const work = this.#durableWork();
    if (!work) { this.#watchdogWork = undefined; this.#watchdogFailures = 0; return; }
    const noProgress = work === this.#watchdogWork;
    this.#watchdogWork = work;
    try {
      await this.ensureHost();
      // A ready owner is not progress: it may exit idle with the same disk work.
      this.#watchdogFailures = noProgress ? this.#watchdogFailures + 1 : 0;
      this.#nextWatchdogAt = Date.now() + Math.min(WATCHDOG_MAX_BACKOFF_MS, WATCHDOG_INTERVAL_MS * 2 ** this.#watchdogFailures);
    } catch {
      this.#nextWatchdogAt = Date.now() + Math.min(WATCHDOG_MAX_BACKOFF_MS, WATCHDOG_INTERVAL_MS * 2 ** ++this.#watchdogFailures);
    }
  }

  #durableWork(): string | undefined {
    const work: string[] = [];
    const config = this.options.config;
    const actorRoots = [config.actorRoot, config.sessionActorRoot ??
      (config.mesh.actorScope === "session" ? path.dirname(config.actorRoot) : path.join(config.actorRoot, config.sessionId))];
    for (const root of actorRoots) {
      const registry = readJson<{ actors?: Array<{ id?: string; rootId?: string; residency?: string; status?: string }> }>(path.join(root, "actors.json"));
      for (const actor of registry?.actors ?? []) {
        if (actor.rootId === config.rootId && actor.residency === "durable" && actor.status !== "stopped") {
          work.push(`actor:${root}:${actor.id}:${actor.status}`);
        }
      }
    }
    // A terminal task can leave its producer outbox behind at idle exit. The live
    // client owns recovery even with no durable actors; the fenced host replays
    // the stable envelope id, not the completed agent.
    const outbox = path.join(config.residencyRoot, "delivery-outbox");
    let entries: string[];
    try { entries = fs.readdirSync(outbox); } catch { entries = []; }
    for (const entry of entries.filter(entry => entry.endsWith(".json"))) {
      const record = readJson<ResidentDeliveryRecord>(path.join(outbox, entry));
      if (record?.format === RESIDENT_HOST_FORMAT && typeof record.id === "string" && `${record.id}.json` === entry &&
        typeof record.rootId === "string" && typeof record.message === "string" && typeof record.triggerTurn === "boolean" &&
        (record.delivery === "steer" || record.delivery === "followUp") && record.from && typeof record.from.id === "string") {
        work.push(`delivery:${record.rootId}:${record.id}`);
      }
    }
    return work.length ? JSON.stringify(work.sort()) : undefined;
  }

  async #drainDeliveries(): Promise<void> {
    if (this.#drainingDeliveries || this.#closed || !this.options.mainAgent.local) return;
    this.#drainingDeliveries = true;
    try {
      const entries = this.options.mesh.listAll(this.#deliveryPrefix);
      for (const entry of entries) {
        try { await this.#deliver(entry); } catch (error) {
          // The record remains durable. Back off a locked mesh; retain ordinary failed senders
          // without blocking the other entries in this pass.
          if (isMeshLockTimeout(error)) throw error;
        }
      }
    } finally {
      this.#drainingDeliveries = false;
    }
  }

  async #deliver(entry: MeshStateEntry): Promise<void> {
    if (typeof entry.value !== "object" || entry.value === null || Array.isArray(entry.value)) return;
    const value = entry.value as Partial<ResidentDeliveryRecord>;
    if (
      value.format !== RESIDENT_HOST_FORMAT ||
      value.rootId !== this.options.config.rootId ||
      typeof value.id !== "string" ||
      typeof value.message !== "string" ||
      (value.delivery !== "steer" && value.delivery !== "followUp") ||
      typeof value.triggerTurn !== "boolean" ||
      typeof value.from !== "object" ||
      value.from === null ||
      // This root's resident host, or for an actor's message another root's resident host whose
      // root is gone: its actors' messages go to the project agent (smarty-dev#878).
      (entry.updatedBy.id !== this.hostId &&
        !(value.from.kind === "actor" && isResidentHostId(entry.updatedBy.id)))
    ) {
      return;
    }
    const data = value.data as Partial<AgentRunResult> | undefined;
    // Also recognize envelopes written by an older resident host.
    const completionId = value.agentCompletionId ?? (data && typeof data.status === "string" &&
      terminal(data.status) && typeof data.startedAt === "number" ? data.id : undefined);
    if (value.from.kind === "agent" && typeof completionId === "string" && completionId === value.from.id) {
      const metadata = this.#metadata(completionId);
      if (!metadata || metadata.completionConsumedAt || !this.options.config.agents.notifyOnComplete) {
        await this.options.mesh.delete({ key: entry.key, ifVersion: entry.version });
        return;
      }
      if (this.options.onBackgroundComplete) {
        // Keep the durable envelope until Main actually consumes it, not merely
        // until the TUI copies it into its retractable in-memory inbox.
        const result = this.statusAgent(completionId);
        if (terminal(result.status) && "startedAt" in result) {
          this.options.onBackgroundComplete(result as AgentRunResult, () => this.acknowledgeCompletion(completionId));
        }
        return;
      }
    }
    // smarty-dev#2236: one record reached Main twice (a failed delete, or a second drainer that
    // listed it through the 2 s read cache before the delete). The record stays the durable copy
    // until Main durably admitted it: deliverAgent journals it under the record's stable id before
    // it returns, keeps it until the session holds it, and admits one id once across restarts and
    // release reloads (review round 2 on pi-fabric#160). Only then is the record deleted.
    this.options.mainAgent.deliverAgent({
      from: value.from,
      // The authenticated resident writer alone does not prove actor authorship: older
      // hosts also write alarms under the actor label. Require positive classification;
      // unclassified/unknown actor records retain routing/display and durable receipts only.
      ...(value.source === "fabric-host" || (value.from.kind === "actor" && value.source !== "actor-output")
        ? {} : { verification: "mesh" as const, principal: value.principal }),
      message: value.message,
      delivery: value.delivery,
      triggerTurn: value.triggerTurn,
      ...(value.data === undefined ? {} : { data: value.data }),
      deliveryId: `resident:${this.options.config.rootId}:${value.id}`,
    });
    await this.options.mesh.delete({ key: entry.key, ifVersion: entry.version });
  }
}
