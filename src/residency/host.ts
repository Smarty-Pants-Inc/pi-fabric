#!/usr/bin/env node

import { randomUUID } from "node:crypto";
import { lockFile, FileLockBusy } from "./file-lock.js";
import { closeWithActors } from "../actors/close-order.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { writeJsonAtomic } from "../core/atomic-write.js";
import { normalizeModelAliases, type FabricModelCandidate } from "../core/model-resolution.js";
import { resolvePiModel, type PiModelRegistryView } from "../core/model-refresh.js";
import {
  parseFabricOwnedModelGuidance,
  resolveFabricModelGuidance,
} from "../components/model-guidance.js";
import { ActorDirectory } from "../actors/directory.js";
import type { FabricActorInfo } from "../actors/types.js";
import { AgentManager } from "../agents/manager.js";
import type { AgentRunRecord } from "../agents/types.js";
import { useBudgetLedger } from "../agents/budget-ledger.js";
import { LifecycleBroker } from "../lifecycle/broker.js";
import { lifecycleSourceIdentity, type FabricLifecycleEvent, type FabricLifecycleSubscription } from "../lifecycle/types.js";
import { MeshStore, RUNTIME_MESH_READ_CACHE_MS, type MeshIdentity } from "../mesh/store.js";
import { MeshBackgroundQueue, MeshBackgroundRetry } from "../core/atomic-write.js";
import { isMeshLockTimeout } from "../core/atomic-write.js";
import { FabricControlPlane, controlActorBindingOptions, type FabricControlAcceptance, type FabricControlCommand } from "../topology/control-plane.js";
import { ParticipantDirectory } from "../topology/participant-directory.js";
import { actorParticipantRecord, agentParticipantRecords } from "../topology/records.js";
import {
  RESIDENT_HOST_FORMAT,
  RESIDENT_ACTOR_COMMAND_FORMAT,
  ResidentActorAuthorizationError,
  ResidentCommandUnsupportedError,
  RESIDENT_COMMANDS,
  isResidentCommandOperation,
  assertResidentActorMain,
  assertResidentActorToolCeiling,
  type ResidentActorCaller,
  commitResidentRequest,
  readResidentRequestDecision,
  residentDeliveryPrefix,
  residentHostId,
  residentRemovalsPath,
  residentResultPath,
  type ResidentAgentMetadata,
  type ResidentCommand,
  type ResidentCommandResponse,
  type ResidentDeliveryRecord,
  type ResidentHostConfig,
  type ResidentHostOwner,
} from "./protocol.js";
import { deliveryRoot, projectOf } from "../topology/project-identity.js";
import { processStartTime, residentProcessAlive } from "./process-identity.js";
import { canRemoveTerminalRun } from "../storage/retention.js";
import { ownedStat } from "../storage/scratch.js";

export const RESIDENT_RUN_RETENTION_MS = 24 * 60 * 60 * 1_000;

/** A worker can finish after its host dies, leaving status.json as the only completion copy. */
const hasPreservedResidentResult = (runsRoot: string, id: string): boolean => {
  const residencyRoot = path.dirname(runsRoot);
  const metadataPath = path.join(residencyRoot, "agents", `${id}.json`);
  // Only proven absence permits ordinary actor/untracked collection. Unreadable or unsafe
  // metadata may still describe a public task, so uncertainty keeps its run directory.
  try { fs.lstatSync(metadataPath); }
  catch (error) { return (error as NodeJS.ErrnoException).code === "ENOENT"; }
  const readOwnedJson = <T>(file: string): T | undefined => {
    const stat = ownedStat(file);
    if (!stat?.isFile() || stat.size > 1024 * 1024) return undefined;
    return readJson<T>(file);
  };
  const time = (value: unknown): value is number =>
    typeof value === "number" && Number.isFinite(value) && value >= 0;
  const metadata = readOwnedJson<ResidentAgentMetadata>(metadataPath);
  if (metadata?.format !== RESIDENT_HOST_FORMAT || metadata.id !== id ||
      typeof metadata.rootId !== "string" || metadata.handle?.id !== id ||
      metadata.handle.residency !== "durable" || metadata.handle.actorId !== undefined ||
      typeof metadata.handle.name !== "string" || typeof metadata.handle.cwd !== "string" ||
      !["pi", "claude", "veda"].includes(metadata.handle.runner) ||
      !["process", "tmux", "screen", "localterm", "herdr"].includes(metadata.handle.transport) ||
      !["queued", "running", "completed", "failed", "stopped", "timed_out"].includes(metadata.handle.status) ||
      !time(metadata.createdAt) || !time(metadata.updatedAt) ||
      typeof metadata.runDirectory !== "string" ||
      path.resolve(metadata.runDirectory) !== path.resolve(runsRoot, id)) return false;
  const saved = readOwnedJson<AgentRunRecord>(residentResultPath(residencyRoot, id));
  // Validate the terminal record, not just a matching id/status stub: deleting the run must
  // leave a usable result (including its text) for client status/wait across restarts.
  return !!saved && saved.id === id && saved.actorId === undefined &&
    ["completed", "failed", "stopped", "timed_out"].includes(saved.status) &&
    typeof saved.name === "string" && typeof saved.task === "string" &&
    typeof saved.cwd === "string" && typeof saved.text === "string" &&
    ["pi", "claude", "veda"].includes(saved.runner) &&
    ["process", "tmux", "screen", "localterm", "herdr"].includes(saved.transport) &&
    time(saved.startedAt) && time(saved.updatedAt) &&
    (saved.finishedAt === undefined || time(saved.finishedAt)) &&
    time(saved.turns) && time(saved.toolCalls) &&
    (saved.error === undefined || typeof saved.error === "string") &&
    !!saved.usage && [saved.usage.input, saved.usage.output, saved.usage.cacheRead,
      saved.usage.cacheWrite, saved.usage.cost].every(time);
};

/** Called under the host fence, before constructing the manager: every existing run is untracked. */
export const sweepResidentRuns = (runsRoot: string, now = Date.now(), budgetMs = 100): string[] => {
  const removed: string[] = [];
  if (!ownedStat(runsRoot)?.isDirectory()) return removed;
  const started = performance.now();
  const expired = () => performance.now() - started >= budgetMs;
  let directory: fs.Dir;
  try { directory = fs.opendirSync(runsRoot); } catch { return removed; }
  try {
    let entry: fs.Dirent | null;
    while (!expired() && (entry = directory.readSync())) {
      if (!entry.isDirectory()) continue;
      const run = path.join(runsRoot, entry.name);
      const stat = ownedStat(run);
      if (!stat?.isDirectory() || now - stat.mtimeMs <= RESIDENT_RUN_RETENTION_MS) continue;
      if (!canRemoveTerminalRun(run, expired) ||
          !hasPreservedResidentResult(runsRoot, entry.name) || expired()) continue;
      try { fs.rmSync(run, { recursive: true, force: true }); removed.push(run); } catch {}
    }
  } finally { directory.closeSync(); }
  return removed;
};

const REQUEST_POLL_MS = 50;
const IDLE_EXIT_MS = 30_000;
const COMPLETION_MAX_CHARS = 8_000;
const HOST_CLOSING_RETRY = "Fabric resident host is closing; retry";

const delay = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

/** Test-only native-process proof seam. Unset/invalid values are inert; never a startup hook. */
const testResidentRequestDelay = async (stage: "before_commit" | "after_commit"): Promise<void> => {
  if (process.env.PI_FABRIC_TEST_RESIDENT_DELAY_STAGE !== stage) return;
  const ms = Number(process.env.PI_FABRIC_TEST_RESIDENT_DELAY_MS);
  if (Number.isInteger(ms) && ms > 0 && ms <= 10_000) await delay(ms);
};

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

const errorMessage = (error: unknown): string =>
  error instanceof Error ? error.message : String(error);

export class ResidentHostAlreadyRunning extends Error {}

const parseResidentHostConfigPath = (argv: readonly string[]): string => {
  const index = argv.indexOf("--config");
  const value = index >= 0 ? argv[index + 1] : undefined;
  if (!value) throw new Error("Missing resident host argument: --config");
  return path.resolve(value);
};

const validateResidentHostConfig = (value: unknown, configPath: string): ResidentHostConfig => {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error("Invalid Fabric resident host config");
  }
  const config = value as Partial<ResidentHostConfig>;
  if (
    config.format !== RESIDENT_HOST_FORMAT ||
    typeof config.rootId !== "string" ||
    typeof config.sessionId !== "string" ||
    typeof config.cwd !== "string" ||
    typeof config.projectRoot !== "string" ||
    typeof config.meshRoot !== "string" ||
    typeof config.actorRoot !== "string" ||
    (config.sessionActorRoot !== undefined && typeof config.sessionActorRoot !== "string") ||
    typeof config.residencyRoot !== "string" ||
    typeof config.fullCodeMode !== "boolean" ||
    (config.kernel !== undefined && config.kernel !== "typescript" && config.kernel !== "python") ||
    (config.pythonRuntime !== undefined && config.pythonRuntime !== "cpython" && config.pythonRuntime !== "monty") ||
    typeof config.agents !== "object" ||
    config.agents === null ||
    typeof config.mesh !== "object" ||
    config.mesh === null ||
    typeof config.retention !== "object" ||
    config.retention === null ||
    typeof config.workerPath !== "string" ||
    typeof config.fabricExtensionPath !== "string" ||
    typeof config.piBinary !== "string" ||
    typeof config.claudeBinary !== "string" ||
    typeof config.vedaBinary !== "string"
  ) {
    throw new Error("Fabric resident host config is incomplete");
  }
  if (path.resolve(config.residencyRoot) !== path.dirname(configPath)) {
    throw new Error("Fabric resident host config is outside its residency root");
  }
  if (!config.mesh.enabled) throw new Error("Durable residency requires the Fabric mesh");
  return config as ResidentHostConfig;
};

export class ResidentHost {
  readonly hostId: string;
  readonly identity: MeshIdentity;
  mesh!: MeshStore;
  participants!: ParticipantDirectory;
  control!: FabricControlPlane;
  agents!: AgentManager;
  actors!: ActorDirectory;
  lifecycle!: LifecycleBroker;
  #lockFd: number | undefined;
  #fallbackLock = false;
  readonly #ownerPath: string;
  readonly #lockPath: string;
  readonly #errorPath: string;
  readonly #requestsPath: string;
  readonly #processingPath: string;
  readonly #responsesPath: string;
  readonly #agentsPath: string;
  readonly #removalsPath: string;
  readonly #deliveryOutboxPath: string;
  readonly #deliveryRetry = new MeshBackgroundRetry("resident completion/actor delivery");
  #flushingDeliveries: Promise<unknown> | undefined;
  readonly #token = randomUUID();
  #requestTimer: NodeJS.Timeout | undefined;
  #pollingRequests = false;
  #closed = false;
  readonly #backgroundRequests = new MeshBackgroundRetry("resident request poll");
  readonly #backgroundDeliveries = new MeshBackgroundQueue("resident completion/actor delivery");
  #started = false;
  #idleSince = Date.now();
  #admissions = 0;

  constructor(
    readonly config: ResidentHostConfig,
    readonly onIdle: () => void = () => {},
    private readonly modelRegistry?: PiModelRegistryView,
  ) {
    this.hostId = residentHostId(config.rootId);
    this.identity = { id: this.hostId, name: "Fabric resident host", kind: "agent" };
    this.#ownerPath = path.join(config.residencyRoot, "owner.json");
    this.#lockPath = path.join(config.residencyRoot, "host.lock");
    this.#errorPath = path.join(config.residencyRoot, "error.json");
    this.#requestsPath = path.join(config.residencyRoot, "requests");
    this.#processingPath = path.join(config.residencyRoot, "processing");
    this.#responsesPath = path.join(config.residencyRoot, "responses");
    this.#agentsPath = path.join(config.residencyRoot, "agents");
    this.#removalsPath = residentRemovalsPath(config.residencyRoot);
    this.#deliveryOutboxPath = path.join(config.residencyRoot, "delivery-outbox");
  }

  #initialize(): void {
    const { config, modelRegistry } = this;
    this.mesh = new MeshStore(config.meshRoot, config.mesh.maxEventBytes, config.mesh.maxReadEvents,
      { readCacheMs: RUNTIME_MESH_READ_CACHE_MS, lockProtocol: config.mesh.lockProtocol });
    this.participants = new ParticipantDirectory(this.mesh, {
      enabled: true,
      hostId: this.hostId,
      rootId: config.rootId,
      identity: this.identity,
      reapDeadHosts: false,                                    // its session's runtime sweeps
    });
    this.control = new FabricControlPlane(this.mesh, this.identity, {
      enabled: true,
      hostId: this.hostId,
      pollMs: config.mesh.actorPollMs,
      readMirroredOwner: (ownerHostId, ownerIdentityId, targetId) =>
        this.participants.mirroredControlOwner(ownerHostId, ownerIdentityId, targetId),
    });
    if (config.agents.budgetUsd > 0) {
      const budgetFile = path.join(config.residencyRoot, "budget.jsonl");
      fs.mkdirSync(path.dirname(budgetFile), { recursive: true, mode: 0o700 });
      if (!fs.existsSync(budgetFile)) fs.writeFileSync(budgetFile, "", { mode: 0o600 });
      useBudgetLedger({
        budget: config.agents.budgetUsd,
        file: budgetFile,
        id: this.hostId,
      });
    }
    const guidanceConfigPath = path.join(config.residencyRoot, "config.json");
    const currentConfig = (): Partial<ResidentHostConfig> =>
      readJson<Partial<ResidentHostConfig>>(guidanceConfigPath) ?? config;
    const currentModelGuidance = () =>
      parseFabricOwnedModelGuidance(currentConfig().modelGuidance ?? config.modelGuidance);
    // The session's visible models (synced at each ensureHost) plus, after a miss, this host's
    // own refreshed Pi registry: the one shared resolver, so an already-running host resolves a
    // model added to models.json after it started (pi-fabric#138).
    const resolveResidentPiModel = async (selector?: string): Promise<string> => {
      const state = currentConfig().piModels ?? config.piModels;
      const snapshot: FabricModelCandidate[] = Array.isArray(state?.available)
        ? state.available.flatMap((candidate) =>
            typeof candidate?.provider === "string" && typeof candidate.id === "string"
              ? [{
                  provider: candidate.provider,
                  id: candidate.id,
                  ...(typeof candidate.name === "string" ? { name: candidate.name } : {}),
                }]
              : [],
          )
        : [];
      const resolved = await resolvePiModel({
        selector,
        registry: modelRegistry,
        aliases: normalizeModelAliases(state?.aliases),
        defaultModel: state?.defaultModel,
        snapshot,
      });
      return `${resolved.provider}/${resolved.id}`;
    };
    this.agents = new AgentManager(config.cwd, config.agents, {
      workerPath: config.workerPath,
      fabricExtensionPath: config.fabricExtensionPath,
      piBinary: config.piBinary,
      claudeBinary: config.claudeBinary,
      vedaBinary: config.vedaBinary,
      runRoot: path.join(config.residencyRoot, "runs"),
      fullCodeMode: config.fullCodeMode,
      kernel: () => currentConfig().kernel ?? config.kernel ?? "typescript",
      pythonRuntime: () => currentConfig().pythonRuntime ?? config.pythonRuntime ?? "monty",
      mainAgentId: config.rootId,
      fabricSessionId: config.sessionId,
      meshRoot: config.meshRoot,
      projectRoot: config.projectRoot,
      hostId: this.hostId,
      identityId: this.identity.id,
      retention: config.retention,
      preparePiModel: async (model) => resolveResidentPiModel(model),
      resolveParticipantGuidance: ({ model }) => {
        if (!model) return undefined;
        return resolveFabricModelGuidance(currentModelGuidance(), {
          model,
          target: "participant",
          includeSlots: false,
        }).appendText || undefined;
      },
      onLifecycle: (event) => void this.lifecycle?.publishBackground(event),
      onSettled: (result) => {
        // Only public durable task runs: actor activations are cleaned by their actor (review/astra on #136).
        if (result.actorId) return;
        const file = residentResultPath(config.residencyRoot, result.id);
        fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
        atomicWrite(file, result);
      },
      onBackgroundComplete: (result) => {
        if (!config.agents.notifyOnComplete) return;
        const durationMs = Math.max(0, (result.finishedAt ?? Date.now()) - result.startedAt);
        const summary = (result.text || result.error || "no result").slice(0, COMPLETION_MAX_CHARS);
        void this.#queueDelivery(
          { id: result.id, name: result.name, kind: "agent" },
          `Fabric agent ${result.id.slice(0, 8)} ${result.status} after ${Math.round(durationMs / 1_000)}s: ${summary}`,
          "followUp",
          true,
          result,
          result.id,
        ).catch(() => undefined);
      },
    });
    const canManageActor = (id: string): boolean | undefined => {
      const participant = this.participants.get(id);
      return participant ? participant.ownerHostId === this.hostId : undefined;
    };
    const lineageAlive = (rootId: string): boolean =>
      this.participants.get(rootId) !== undefined;
    const actorRoots = config.sessionActorRoot
      ? { project: config.actorRoot, session: config.sessionActorRoot }
      : config.mesh.actorScope === "session"
        ? { project: path.dirname(config.actorRoot), session: config.actorRoot }
        : { project: config.actorRoot, session: path.join(config.actorRoot, config.sessionId) };
    this.actors = new ActorDirectory([
      config.sessionId,
      this.identity,
      this.mesh,
      config.mesh,
      this.agents,
      ({ actor, message, delivery, triggerTurn }) => {
        if (!message.text) return;
        const mode = delivery === "steer" ? "steer" : "followUp";
        const triggers = delivery === "nextTurn" ? false : triggerTurn;
        void this.#queueDelivery(
          { id: actor.id, name: actor.name, kind: "actor" },
          message.text,
          mode,
          triggers,
          message.data,
          undefined,
          // smarty-dev#878: once the root is gone, to the project's live project agent.
          deliveryRoot(
            config.rootId,
            this.participants.list({ scope: "project", kinds: ["root"] }),
            actor.project ?? (typeof config.project === "string" ? config.project : projectOf(config.cwd)),
          ),
        ).catch(() => undefined);
      },
      {
        persistent: true,
        canManageActor,
        lineageAlive,
        claimResidency: "durable",
        rootId: config.rootId,
        // Recorded on every actor it creates, and the only project whose orphans it adopts, and
        // then only as a project agent's host.
        project: (typeof config.project === "string" ? config.project : projectOf(config.cwd)),
        role: typeof config.role === "string" ? config.role : undefined,
        meshCursorPath: path.join(config.residencyRoot, "actor-mesh-cursor.json"),
        retention: config.retention,
        ...(typeof config.actors?.maxSessionBytes === "number" ? { maxSessionBytes: config.actors.maxSessionBytes } : {}),
        resolvePiModel: resolveResidentPiModel,
      },
    ], actorRoots, config.mesh.actorScope);
    this.lifecycle = new LifecycleBroker(
      this.mesh,
      this.identity,
      this.participants,
      {
        enabled: true,
        pollMs: config.mesh.actorPollMs,
        maxReadEvents: config.mesh.maxReadEvents,
      },
      (subscription, event) => this.#deliverLifecycle(subscription, event),
    );
  }

  async start(): Promise<void> {
    if (this.#started) return;
    await this.#acquireLock();
    this.#started = true;
    try {
      if (!this.config.agents.retainRuns) sweepResidentRuns(path.join(this.config.residencyRoot, "runs"));
      this.#initialize();
      fs.mkdirSync(this.#requestsPath, { recursive: true, mode: 0o700 });
      fs.mkdirSync(this.#processingPath, { recursive: true, mode: 0o700 });
      fs.mkdirSync(this.#responsesPath, { recursive: true, mode: 0o700 });
      fs.mkdirSync(this.#agentsPath, { recursive: true, mode: 0o700 });
      this.#recoverInterruptedRequests();
      const firstSeenAgents = new Map<string, number>();
      this.participants.registerSource(() =>
        agentParticipantRecords(
          this.agents.listForUi(),
          this.config.rootId,
          this.hostId,
          this.identity.id,
          this.config.rootId,
          firstSeenAgents,
        ),
      );
      this.participants.registerSource(() =>
        this.actors.listOwned().map((actor) =>
          actorParticipantRecord(
            actor,
            this.config.rootId,
            this.hostId,
            this.identity.id,
            this.config.rootId,
          ),
        ),
      );
      this.agents.subscribeUi(() => this.participants.scheduleRefresh());
      this.actors.subscribe(() => this.participants.scheduleRefresh());
      this.control.start((command, from, signal) =>
        this.#acceptControl(command, from, signal));
      await this.participants.start().catch(() => undefined);
      this.lifecycle.start();
      this.#requestTimer = setInterval(
        () => {
          void this.#backgroundRequests.run(() => this.#pollRequests());
          void this.#retryDeliveries();
        },
        REQUEST_POLL_MS,
      );
      const now = Date.now();
      const owner: ResidentHostOwner = {
        format: RESIDENT_HOST_FORMAT,
        hostId: this.hostId,
        pid: process.pid,
        processStartTime: processStartTime(process.pid),
        token: this.#token,
        startedAt: now,
        readyAt: now,
        commands: RESIDENT_COMMANDS,
        requestFence: 1,
      };
      atomicWrite(this.#ownerPath, owner);
      fs.rmSync(this.#errorPath, { force: true });
      // Removals a previous host accepted: their runs ended with it.
      void this.#backgroundDeliveries.enqueue(async () => {
        await this.actors.finishPendingRemovals();
        this.#writeRemovals();
      });
      void this.#retryDeliveries();
      await this.#pollRequests();
    } catch (error) {
      await this.close();
      throw error;
    }
  }

  async close(): Promise<void> {
    if (this.#closed || !this.#started) return;
    this.#closed = true;
    if (this.#requestTimer) clearInterval(this.#requestTimer);
    this.#requestTimer = undefined;
    // Stop drains first so an in-flight ask can settle within the actor shutdown grace.
    const actorsClosed = this.actors?.close();
    while (this.#pollingRequests || this.#admissions) await delay(10);
    await this.participants?.quiesce().catch(() => undefined);
    await this.lifecycle?.close().catch(() => undefined);
    try {
      await closeWithActors({ close: () => actorsClosed }, () => this.control?.close().catch(() => undefined));
    } finally {
      try {
        try {
          await this.agents?.close();
        } finally {
          await this.#backgroundDeliveries.close();
          await this.#flushingDeliveries;
          await this.participants?.close().catch(() => undefined);
        }
      } finally { this.#releaseLock(); }
    }
  }

  async #acceptControl(
    command: FabricControlCommand,
    from: MeshIdentity,
    signal?: AbortSignal,
  ): Promise<FabricControlAcceptance> {
    if (this.#closed) {
      return { accepted: false, error: HOST_CLOSING_RETRY };
    }
    this.#admissions++;
    try { return await this.#handleControl(command, from, signal); }
    finally { this.#admissions--; }
  }

  async #handleControl(command: FabricControlCommand, from: MeshIdentity, signal?: AbortSignal): Promise<FabricControlAcceptance> {
    if (command.operation === "cancel") {
      return { accepted: false, error: "Cancel commands are handled by the control plane" };
    }
    if (command.operation === "stop") {
      try {
        await this.agents.stop(command.targetId);
        this.participants.scheduleRefresh();
        return { accepted: true, messageId: command.commandId };
      } catch (error) {
        if (!(error instanceof Error && /Unknown Fabric agent/.test(error.message))) {
          return { accepted: false, error: errorMessage(error) };
        }
      }
      try {
        if (this.#closed) return { accepted: false, error: HOST_CLOSING_RETRY };
        if (!this.actors.owns(command.targetId)) {
          return { accepted: false, error: `Resident host does not own ${command.targetId}` };
        }
        await this.actors.stop(command.targetId);
        this.participants.scheduleRefresh();
        return { accepted: true, messageId: command.commandId };
      } catch (error) {
        return { accepted: false, error: errorMessage(error) };
      }
    }
    const message = command.message?.trim();
    if (!message) return { accepted: false, error: "Fabric control message must not be empty" };
    if (command.operation === "ask") {
      try {
        if (!this.actors.owns(command.targetId)) {
          return { accepted: false, error: `Resident host does not own ${command.targetId}` };
        }
        const result = await this.actors.ask(
          command.targetId,
          message,
          command.data,
          signal,
          controlActorBindingOptions(command, from, this.actors.status(command.targetId).rootId,
            this.participants.get(from.id)?.rootId),
        );
        return { accepted: true, messageId: result.id, result };
      } catch (error) {
        return { accepted: false, error: errorMessage(error) };
      }
    }
    try {
      this.agents.status(command.targetId);
      const result = command.operation === "steer"
        ? this.agents.steer(command.targetId, message, command.data)
        : this.agents.followUp(command.targetId, message, command.data);
      return { accepted: true, messageId: result.messageId };
    } catch (error) {
      if (!(error instanceof Error && /Unknown Fabric agent/.test(error.message))) {
        return { accepted: false, error: errorMessage(error) };
      }
    }
    try {
      if (!this.actors.owns(command.targetId)) {
        return { accepted: false, error: `Resident host does not own ${command.targetId}` };
      }
      const options = controlActorBindingOptions(command, from, this.actors.status(command.targetId).rootId,
        this.participants.get(from.id)?.rootId);
      // Validate now without turning the resolved owner defaults into per-call overrides.
      await this.actors.resolveActivationBinding(command.targetId, options);
      if (this.#closed) return { accepted: false, error: HOST_CLOSING_RETRY };
      const result = this.actors.tell(command.targetId, message, command.data, options);
      return { accepted: true, messageId: result.messageId };
    } catch (error) {
      return { accepted: false, error: errorMessage(error) };
    }
  }

  async #deliverLifecycle(
    subscription: FabricLifecycleSubscription,
    event: FabricLifecycleEvent,
  ): Promise<void> {
    if (this.#closed) throw new Error(HOST_CLOSING_RETRY);
    this.#admissions++;
    try { await this.#handleLifecycle(subscription, event); }
    finally { this.#admissions--; }
  }

  async #handleLifecycle(subscription: FabricLifecycleSubscription, event: FabricLifecycleEvent): Promise<void> {
    const message = `Fabric lifecycle ${event.event} from ${event.source.name} (${event.source.id})${event.status ? ` with status ${event.status}` : ""}.`;
    if (subscription.to === this.config.rootId) {
      await this.#queueDelivery(
        lifecycleSourceIdentity(event.source),
        message,
        subscription.delivery,
        subscription.triggerTurn,
        event,
      );
      return;
    }
    try {
      this.agents.status(subscription.to);
      if (subscription.delivery === "steer") this.agents.steer(subscription.to, message, event);
      else this.agents.followUp(subscription.to, message, event);
      return;
    } catch (error) {
      if (!(error instanceof Error && /Unknown Fabric agent/.test(error.message))) throw error;
    }
    try {
      if (this.actors.owns(subscription.to)) {
        this.actors.tell(subscription.to, message, event);
        return;
      }
    } catch {
      // Route through the current remote owner below.
    }
    const target = this.participants.get(subscription.to);
    if (!target) throw new Error(`Unknown Fabric lifecycle target: ${subscription.to}`);
    await this.control.request(
      target.ownerHostId,
      target.id,
      subscription.delivery,
      { message, data: event, triggerTurn: subscription.triggerTurn },
      target.ownerIdentityId,
      { routedRemoteHost: target.remoteHost ?? null },
    );
  }

  async #queueDelivery(
    from: MeshIdentity,
    message: string,
    delivery: "steer" | "followUp",
    triggerTurn: boolean,
    data?: unknown,
    agentCompletionId?: string,
    rootId = this.config.rootId,
  ): Promise<void> {
    const id = randomUUID();
    const record: ResidentDeliveryRecord = {
      format: RESIDENT_HOST_FORMAT,
      id,
      rootId,
      from,
      delivery,
      triggerTurn,
      message,
      ...(data === undefined ? {} : { data }),
      ...(agentCompletionId ? { agentCompletionId } : {}),
      createdAt: Date.now(),
    };
    // Persist before yielding: AgentManager has already marked this notification sent.
    // Idle exit/queue pressure must not drop the host's ownership of the handoff.
    writeJsonAtomic(path.join(this.#deliveryOutboxPath, `${id}.json`), record, { durable: true });
    await this.#retryDeliveries();
  }

  #retryDeliveries(): Promise<unknown> {
    if (this.#closed) return Promise.resolve();
    if (this.#flushingDeliveries) return this.#flushingDeliveries;
    const flushing = this.#deliveryRetry.run(() => this.#flushDeliveries());
    this.#flushingDeliveries = flushing;
    void flushing.finally(() => {
      if (this.#flushingDeliveries === flushing) this.#flushingDeliveries = undefined;
    }).catch(() => undefined);
    return flushing;
  }

  async #flushDeliveries(): Promise<void> {
    if (!fs.existsSync(this.#deliveryOutboxPath)) return;
    for (const entry of fs.readdirSync(this.#deliveryOutboxPath).filter(entry => entry.endsWith(".json")).slice(0, 32)) {
      const file = path.join(this.#deliveryOutboxPath, entry);
      const record = readJson<ResidentDeliveryRecord>(file);
      if (!record || record.format !== RESIDENT_HOST_FORMAT || `${record.id}.json` !== entry) {
        throw new Error(`Invalid resident delivery outbox item: ${file}`);
      }
      const key = `${residentDeliveryPrefix(record.rootId)}${record.id}`;
      try {
        await this.mesh.put({ key, value: record, identity: this.identity, ifVersion: 0 });
      } catch (error) {
        // A restart after put but before unlink replays the SAME private UUID. A CAS
        // conflict (including a consumed tombstone) means it was handed off already.
        if (!(error instanceof Error && error.message.startsWith(`Mesh compare-and-swap failed for ${key}: expected version 0,`))) {
          if (isMeshLockTimeout(error)) throw error;
          await this.mesh.put({
            key, value: { ...record, message: record.message.slice(0, Math.max(1, this.config.mesh.eventContextChars)), data: { fabricTruncated: true } },
            identity: this.identity, ifVersion: 0,
          });
        }
      }
      // Only a durable mesh handoff releases ownership. Main journals the stable id.
      fs.rmSync(file);
    }
  }

  async #pollRequests(): Promise<void> {
    if (this.#pollingRequests || this.#closed) return;
    this.#pollingRequests = true;
    try {
      let entries: string[];
      try {
        entries = fs.readdirSync(this.#requestsPath).filter((entry) => entry.endsWith(".json"));
      } catch {
        return;
      }
      for (const entry of entries.slice(0, 32)) {
        const source = path.join(this.#requestsPath, entry);
        const processing = path.join(this.#processingPath, entry);
        try {
          fs.renameSync(source, processing);
        } catch {
          continue;
        }
        await this.#processRequest(processing);
      }
    } finally {
      this.#pollingRequests = false;
      this.#checkIdle();
    }
  }

  #checkIdle(): void {
    if (this.#closed) return;
    const ownedActors = this.actors.listOwned();
    const activeActor = ownedActors.some((actor) => actor.residency === "durable" && actor.status !== "stopped");
    const activeAgent = this.agents
      .listForUi()
      .some((agent) => agent.status === "queued" || agent.status === "running");
    const pendingRequest = [this.#requestsPath, this.#processingPath].some((directory) => {
      try { return fs.readdirSync(directory).some((entry) => entry.endsWith(".json")); }
      catch { return false; }
    });
    if (activeActor || activeAgent || pendingRequest || this.#admissions) {
      this.#idleSince = Date.now();
      return;
    }
    if (Date.now() - this.#idleSince >= IDLE_EXIT_MS) this.onIdle();
  }

  #authorizeResidentSetter(caller: ResidentActorCaller | undefined): void {
    assertResidentActorMain(caller, this.config.rootId);
    // Verify the actual root session's existing control identity, not merely
    // inherited mainAgent.id. get(fresh) validates the record's writer and host
    // lease through the same directory used by native Fabric control routing.
    const root = this.participants.get(this.config.rootId, Date.now(), { fresh: true });
    if (!caller || caller.identity.sessionId !== this.config.sessionId ||
      !root || root.stale || root.remoteHost !== undefined || root.kind !== "root" ||
      root.rootId !== this.config.rootId || root.sessionId !== this.config.sessionId ||
      root.ownerIdentityId !== caller.identity.id || root.ownerHostId !== caller.hostId) {
      throw new ResidentActorAuthorizationError();
    }
  }

  async #processRequest(filePath: string): Promise<void> {
    const command = readJson<ResidentCommand>(filePath);
    const requestId = path.basename(filePath, ".json");
    let response: ResidentCommandResponse;
    try {
      if (
        (command?.format !== RESIDENT_HOST_FORMAT && command?.format !== RESIDENT_ACTOR_COMMAND_FORMAT) ||
        command.rootId !== this.config.rootId ||
        command.requestId !== requestId
      ) {
        throw new Error("Invalid Fabric residency request");
      }
      // Validate the runtime JSON discriminant before any actor lookup/mutation.
      if (!isResidentCommandOperation(command.operation)) {
        throw new ResidentCommandUnsupportedError(`Unsupported Fabric residency command: ${String(command.operation)}`);
      }
      if (readResidentRequestDecision(this.config.residencyRoot, requestId)?.state === "abandoned") {
        throw new Error(`Fabric residency request ${requestId} was abandoned before commit`);
      }
      await testResidentRequestDelay("before_commit");
      const commit = (id: string): void => commitResidentRequest(this.config.residencyRoot, command, id, this.hostId);
      if (command.operation === "spawn") {
        if (
          command.request.sessionSeed ||
          command.request.sessionFile ||
          command.request.actorId ||
          command.request.actorName ||
          command.request.meshRoot ||
          command.request.runnerSessionId ||
          command.request.systemPrompt ||
          command.request.images
        ) {
          throw new Error("Durable agents.spawn accepts only its public task and run settings");
        }
        const handle = await this.agents.spawn({ ...command.request, residency: "durable" }, undefined, undefined, commit);
        const runDirectory = this.agents.runDirectory(handle.id);
        if (!runDirectory) {
          // Durable metadata currently requires an admitted run directory. Never
          // report a failed spawn while leaving its accepted queue entry alive:
          // stop revokes admission synchronously and joins any admission race.
          await this.agents.stop(handle.id);
          await this.agents.cleanup(handle.id);
          throw new Error("Resident host cannot queue durable spawns while all permits are occupied; the queued request was stopped. Retry after capacity is available.");
        }
        const worktreeGitRoot = this.agents.worktreeGitRoot(handle.id);
        const metadata: ResidentAgentMetadata = {
          format: RESIDENT_HOST_FORMAT,
          rootId: this.config.rootId,
          id: handle.id,
          runDirectory,
          handle: { ...handle, residency: "durable" },
          ...(worktreeGitRoot ? { worktreeGitRoot } : {}),
          createdAt: Date.now(),
          updatedAt: Date.now(),
        };
        atomicWrite(path.join(this.#agentsPath, `${handle.id}.json`), metadata);
        // Publish ownership before a fast, already-settled spawn can notify Main.
        this.agents.detachSignal(handle.id);
        response = {
          format: RESIDENT_HOST_FORMAT,
          requestId,
          ok: true,
          handle: metadata.handle,
          completedAt: Date.now(),
        };
      } else if (command.operation === "foreground") {
        commit(command.id);
        this.agents.markForeground(command.id);
        response = {
          format: RESIDENT_HOST_FORMAT,
          requestId,
          ok: true,
          completedAt: Date.now(),
        };
      } else if (command.operation === "cleanup") {
        // Joining is preparation, not foregrounding or consumption: abandonment
        // must preserve the background completion notification as well as files.
        await this.agents.join(command.id);
        commit(command.id);
        await this.agents.cleanup(command.id, command.deleteBranch);
        fs.rmSync(path.join(this.#agentsPath, `${command.id}.json`), { force: true });
        fs.rmSync(residentResultPath(this.config.residencyRoot, command.id), { force: true });
        response = {
          format: RESIDENT_HOST_FORMAT,
          requestId,
          ok: true,
          completedAt: Date.now(),
        };
      } else if (command.operation === "createActor") {
        if (command.request.residency !== "durable") {
          throw new Error("Resident host createActor only supports durable residency");
        }
        // This handler already runs inside the authoritative durable host.
        // Keep the new actor locally owned; ceding it here created a needless
        // self-transfer window that blocked the next recruitment request.
        const actor = await this.actors.create(command.request, { asRegistryOwner: true, beforeCommit: commit });
        response = {
          format: RESIDENT_HOST_FORMAT,
          requestId,
          ok: true,
          actor: actor as FabricActorInfo,
          completedAt: Date.now(),
        };
      } else if (command.operation === "actors") {
        response = {
          format: RESIDENT_HOST_FORMAT, requestId, ok: true,
          actors: this.actors.listOwned().filter((actor) => actor.rootId === this.config.rootId),
          completedAt: Date.now(),
        };
      } else if (command.operation !== "removeActor") {
        if (command.operation === "setInstructions" || command.operation === "setModel" ||
          command.operation === "setThinking" || command.operation === "setActivationFilter" || command.operation === "setTools") {
          this.#authorizeResidentSetter(command.caller);
          if (command.operation === "setTools") assertResidentActorToolCeiling(command.tools, command.caller?.toolCeiling);
        }
        const actor = this.actors.status(String(command.id));
        if (actor.rootId !== this.config.rootId || !this.actors.owns(actor.id)) {
          throw new Error(`Resident host does not own root actor ${actor.id}`);
        }
        let updated: FabricActorInfo;
        switch (command.operation) {
          case "actorStatus": updated = actor; break;
          case "setInstructions": updated = await this.actors.setInstructions(actor.id, command.instructions, commit); break;
          case "setModel": updated = await this.actors.setModel(actor.id, command.model, command.scope, commit); break;
          case "setThinking": updated = await this.actors.setThinking(actor.id, command.thinking, command.scope, commit); break;
          case "setActivationFilter": updated = await this.actors.setActivationFilter(actor.id, command.activationFilter, commit); break;
          case "setTools": updated = await this.actors.setTools(actor.id, command.tools, commit); break;
          default: throw new Error("Unknown resident actor operation");
        }
        response = { format: RESIDENT_HOST_FORMAT, requestId, ok: true, actor: updated, completedAt: Date.now() };
      } else {
        const cleanup = this.actors.cleanupObligation(command.id);
        if (!this.actors.owns(command.id) || (cleanup && cleanup.residency !== "durable")) {
          throw new Error(`Resident host does not own ${command.id}`);
        }
        // smarty-dev#2184 item 8: stop now and return; the removal finishes behind its run.
        commit(command.id);
        const removed = await this.actors.remove(command.id, { wait: false });
        this.#writeRemovals();
        if (removed.pending) {
          void this.actors.removalSettled(command.id)?.finally(() => {
            this.#writeRemovals();
            this.participants.scheduleRefresh();
          });
        }
        response = {
          format: RESIDENT_HOST_FORMAT,
          requestId,
          ok: true,
          ...(removed.pending ? { pending: removed.pending } : {}),
          ...(removed.cleaned !== undefined ? { cleaned: removed.cleaned } : {}),
          completedAt: Date.now(),
        };
      }
    } catch (error) {
      response = {
        format: RESIDENT_HOST_FORMAT,
        requestId,
        ok: false,
        error: errorMessage(error),
        ...(error instanceof ResidentActorAuthorizationError || error instanceof ResidentCommandUnsupportedError
          ? { errorCode: error.code } : {}),
        completedAt: Date.now(),
      };
    }
    if (response.ok) await testResidentRequestDelay("after_commit");
    const responsePath = path.join(this.#responsesPath, `${requestId}.json`);
    atomicWrite(responsePath, response);
    // An abandoned caller already left; clean late responses as well as processing files.
    if (readResidentRequestDecision(this.config.residencyRoot, requestId)?.state === "abandoned") {
      fs.rmSync(responsePath, { force: true });
    }
    fs.rmSync(filePath, { force: true });
    this.participants.scheduleRefresh();
  }

  /** Pending removals for clients' error messages (smarty-dev#2184 item 8). */
  #writeRemovals(): void {
    const removals = this.actors.pendingRemovals();
    if (removals.length === 0) fs.rmSync(this.#removalsPath, { force: true });
    else atomicWrite(this.#removalsPath, { format: RESIDENT_HOST_FORMAT, removals });
  }

  #recoverInterruptedRequests(): void {
    let entries: string[];
    try {
      entries = fs.readdirSync(this.#processingPath).filter((entry) => entry.endsWith(".json"));
    } catch {
      return;
    }
    for (const entry of entries) {
      const requestId = path.basename(entry, ".json");
      if (readResidentRequestDecision(this.config.residencyRoot, requestId)?.state === "abandoned") {
        fs.rmSync(path.join(this.#processingPath, entry), { force: true });
        fs.rmSync(path.join(this.#responsesPath, entry), { force: true });
        continue;
      }
      const response: ResidentCommandResponse = {
        format: RESIDENT_HOST_FORMAT,
        requestId,
        ok: false,
        error: "Fabric residency outcome is indeterminate after resident host restart",
        completedAt: Date.now(),
      };
      atomicWrite(path.join(this.#responsesPath, entry), response);
      fs.rmSync(path.join(this.#processingPath, entry), { force: true });
    }
  }

  async #acquireLock(): Promise<void> {
    fs.mkdirSync(this.config.residencyRoot, { recursive: true, mode: 0o700 });
    if (process.platform === "linux") {
      try { this.#lockFd = await lockFile(this.#lockPath, 0, true); }
      catch (error) {
        if (error instanceof FileLockBusy) throw new ResidentHostAlreadyRunning("Fabric resident host is already running");
        throw error; // Never weaken Linux ownership when util-linux is missing/broken.
      }
    } else {
      // ponytail: without Linux flock/setpriv, retain PID + start-time staleness.
      // Windows durable residency is unsupported; non-Linux identity is #2566.
      const existing = readJson<ResidentHostOwner>(this.#ownerPath);
      const locked = readJson<ResidentHostOwner>(this.#lockPath);
      if ((existing && residentProcessAlive(existing.pid, existing.processStartTime)) ||
          (locked && residentProcessAlive(locked.pid, locked.processStartTime))) {
        throw new ResidentHostAlreadyRunning("Fabric resident host is already running");
      }
      fs.rmSync(this.#lockPath, { force: true });
      try { this.#lockFd = fs.openSync(this.#lockPath, "wx", 0o600); }
      catch (error) {
        if ((error as NodeJS.ErrnoException).code === "EEXIST") throw new ResidentHostAlreadyRunning("Fabric resident host is starting");
        throw error;
      }
      this.#fallbackLock = true;
    }
    // A pre-flock host may own these diagnostic records without holding our fence.
    // Read BEFORE overwriting; unknown birth identity is not authority to displace it.
    const records = [readJson<ResidentHostOwner>(this.#lockPath), readJson<ResidentHostOwner>(this.#ownerPath)];
    if (records.some((owner) => owner && owner.pid !== process.pid && residentProcessAlive(owner.pid, owner.processStartTime))) {
      this.#releaseLock();
      throw new ResidentHostAlreadyRunning("Fabric resident host is already running (legacy owner)");
    }
    try {
      fs.ftruncateSync(this.#lockFd, 0);
      fs.writeFileSync(this.#lockFd, JSON.stringify({ token: this.#token, pid: process.pid, processStartTime: processStartTime(process.pid) }));
    } catch (error) { this.#releaseLock(); throw error; }
  }

  #releaseLock(): void {
    if (this.#lockFd === undefined) return;
    // Remove our publication while still holding the fence; never unlink the Linux inode.
    const owner = readJson<ResidentHostOwner>(this.#ownerPath);
    if (owner?.token === this.#token) fs.rmSync(this.#ownerPath, { force: true });
    if (this.#fallbackLock && readJson<{ token?: string }>(this.#lockPath)?.token === this.#token) {
      fs.rmSync(this.#lockPath, { force: true });
    }
    fs.closeSync(this.#lockFd);
    this.#lockFd = undefined;
  }
}

const runResidentHost = async (
  config: ResidentHostConfig,
  signal?: AbortSignal,
  modelRegistry?: PiModelRegistryView,
): Promise<void> => {
  let finishIdle: (() => void) | undefined;
  const idle = new Promise<void>((resolve) => {
    finishIdle = resolve;
  });
  const host = new ResidentHost(config, () => finishIdle?.(), modelRegistry);
  await host.start();
  if (signal?.aborted) {
    await host.close();
    return;
  }
  await Promise.race([
    idle,
    new Promise<void>((resolve) => {
      const finish = (): void => resolve();
      signal?.addEventListener("abort", finish, { once: true });
      process.once("SIGTERM", finish);
      process.once("SIGINT", finish);
    }),
  ]);
  await host.close();
};

export const runResidentHostFromConfigPath = async (
  configPath: string,
  signal?: AbortSignal,
  modelRegistry?: PiModelRegistryView,
): Promise<void> => {
  let config: ResidentHostConfig | undefined;
  try {
    config = validateResidentHostConfig(readJson<unknown>(configPath), configPath);
    await runResidentHost(config, signal, modelRegistry);
  } catch (error) {
    if (error instanceof ResidentHostAlreadyRunning) return;
    const residencyRoot = config?.residencyRoot ?? path.dirname(configPath);
    try {
      atomicWrite(path.join(residencyRoot, "error.json"), {
        error: errorMessage(error),
        occurredAt: Date.now(),
      });
    } catch {
      // Startup diagnostics are best-effort.
    }
    throw error;
  }
};

const isMain = process.argv[1] && path.resolve(process.argv[1]) === path.resolve(fileURLToPath(import.meta.url));
if (isMain) {
  const configPath = parseResidentHostConfigPath(process.argv);
  try {
    await runResidentHostFromConfigPath(configPath);
  } catch {
    process.exitCode = 1;
  }
}
