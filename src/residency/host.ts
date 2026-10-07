#!/usr/bin/env node

import { fabricTurnProvenance, type FabricPrincipal } from "../fabric-provenance.js";
import { randomUUID } from "node:crypto";
import { resolveActorInstructions, assertActorInstructionReplacement } from "../actors/instructions-file.js";
import {
  RESIDENT_HANDOVER_ABI, HANDOVER_DRAIN_MS, exactResidentProcess, assertAutomaticReleaseRecovery,
  residentLaunchSpec, validateLaunchSpec, assertHandoverTopology, assertPreviousLaunchSpec,
  handoverPath, handoverCustodyPath, handoverOutcomePath, handoverActive,
  readHandoverJson, writeHandoverState, writeLaunchSnapshot, mainGenerationCurrent, decideHandover,
  type ResidentLaunchSpec, type ResidentLauncherIdentity, type ResidentHandoverPlan, type ResidentHandoverState,
} from "./handover.js";

interface ResidentHostLaunchContext {
  launcher: ResidentLauncherIdentity;
  spec: ResidentLaunchSpec;
  attempt?: { id: string; kind: "target" | "fallback" };
}
import { lockFile, FileLockBusy } from "./file-lock.js";
import { assertNoWatchdogCustody } from "./watchdog-custody.js";
import { readResidentOperatorEvidence, assertResidentOperatorConfirmed } from "./operator-safety.js";
import { closeWithActors } from "../actors/close-order.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { writeJsonAtomic } from "../core/atomic-write.js";
import { FabricModelDeniedError } from "../core/model-policy.js";
import { normalizeModelAliases, type FabricModelCandidate } from "../core/model-resolution.js";
import { resolvePiModel, resolvePiRoutePin, type PiModelRegistryView } from "../core/model-refresh.js";
import { ShadowRouteOwner } from "../agents/model-route-owner.js";
import {
  parseFabricOwnedModelGuidance,
  resolveFabricModelGuidance,
} from "../components/model-guidance.js";
import { ActorDirectory } from "../actors/directory.js";
import { ActorRegistryStore } from "../actors/registry-store.js";
import { ActorSessionResetCancelledError } from "../actors/session-reset-error.js";
import type { FabricActorInfo } from "../actors/types.js";
import { AgentManager } from "../agents/manager.js";
import { useBudgetLedger } from "../agents/budget-ledger.js";
import { LifecycleBroker } from "../lifecycle/broker.js";
import { lifecycleSourceIdentity, type FabricLifecycleEvent, type FabricLifecycleSubscription } from "../lifecycle/types.js";
import { MeshStore, RUNTIME_MESH_READ_CACHE_MS, type MeshIdentity } from "../mesh/store.js";
import { MeshBackgroundQueue, MeshBackgroundRetry } from "../core/atomic-write.js";
import { isMeshLockTimeout } from "../core/atomic-write.js";
import { FabricControlPlane, controlActorBindingOptions, type FabricControlAcceptance, type FabricControlCommand } from "../topology/control-plane.js";
import { MeshConsumptionPausedError, assertMeshConsumption } from "../topology/mesh-consumption.js";
import { ParticipantDirectory } from "../topology/participant-directory.js";
import { rootPresenceAlarms } from "../topology/stall-alarms.js";
import { actorParticipantRecord, agentParticipantRecords } from "../topology/records.js";
import {
  RESIDENT_HOST_FORMAT,
  RESIDENT_ACTOR_COMMAND_FORMAT,
  ResidentActorAuthorizationError,
  ResidentCommandUnsupportedError,
  RESIDENT_COMMANDS,
  isResidentCommandOperation,
  assertResidentActorMain,
  assertResidentTaskCaller,
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
import { completionRecipientFromRun, saveCompletion } from "../agents/completion-journal.js";
import { projectOf } from "../topology/project-identity.js";
import { processStartTime, residentProcessAlive } from "./process-identity.js";
import { canRemoveTerminalRun, compactTerminalRunEvents, retainedActorRunIds, runTreeExitVeto, type TerminalRunEventsRetention } from "../storage/retention.js";
import { ownedStat } from "../storage/scratch.js";
import { ResidentRequestRetention } from "./retention.js";
import { retentionV2Enabled } from "../storage/retention-platform.js";
import { ResidentLegacyRunArchive } from "./legacy-run-archive.js";
import { hasPreservedResidentResult } from "./preserved-result.js";
import { assertResidentRequestNotExpired, residentRequestGeneration, ResidentRequestExpiredError, RESIDENT_EXPIRING_COMMAND_FORMAT } from "./request-expiry.js";

export const RESIDENT_RUN_RETENTION_MS = 24 * 60 * 60 * 1_000;

/** Called under the host fence, before constructing the manager: every existing run is untracked. */
export const sweepResidentRuns = (
  runsRoot: string, now = Date.now(), budgetMs = 100,
  options: TerminalRunEventsRetention & { actorRoots?: readonly string[]; retainRuns?: boolean } = {},
): string[] => {
  const removed: string[] = [];
  if (!ownedStat(runsRoot)?.isDirectory()) return removed;
  const started = performance.now();
  const expired = () => performance.now() - started >= budgetMs;
  const retained = retainedActorRunIds(options.actorRoots ?? []);
  if (retained.has("*")) return removed;
  let directory: fs.Dir;
  try { directory = fs.opendirSync(runsRoot); } catch { return removed; }
  try {
    let entry: fs.Dirent | null;
    while (!expired() && (entry = directory.readSync())) {
      if (!entry.isDirectory() || retained.has(entry.name)) continue;
      const run = path.join(runsRoot, entry.name);
      const stat = ownedStat(run);
      if (!stat?.isDirectory()) continue;
      if (!options.retainRuns && now - stat.mtimeMs > RESIDENT_RUN_RETENTION_MS &&
          !runTreeExitVeto(run, 0, expired, true) && canRemoveTerminalRun(run, expired) &&
          hasPreservedResidentResult(runsRoot, entry.name) && !expired()) {
        try { fs.rmSync(run, { recursive: true, force: true }); removed.push(run); } catch {}
      } else {
        compactTerminalRunEvents(run, { ...options, now, expired });
      }
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

const residentActorRoots = (config: ResidentHostConfig): { project: string; session: string } =>
  config.sessionActorRoot
    ? { project: config.actorRoot, session: config.sessionActorRoot }
    : config.mesh.actorScope === "session"
      ? { project: path.dirname(config.actorRoot), session: config.actorRoot }
      : { project: config.actorRoot, session: path.join(config.actorRoot, config.sessionId) };

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
  #exclusiveCreateLock = false;
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
  #maintenanceTimer: NodeJS.Timeout | undefined;
  #legacyArchive: ResidentLegacyRunArchive | undefined;
  #pollingRequests = false;
  // Boundary commands retain response custody without occupying serial admission.
  readonly #boundaryRequests = new Map<string, Promise<void>>();
  // Host-local only: retain pending promises and at most 256 completed creates for 10 minutes.
  readonly #creations = new Map<string, { result: Promise<ResidentCommandResponse>; completedAt?: number }>();
  #closed = false;
  #routeOwner?: ShadowRouteOwner;
  readonly #backgroundRequests = new MeshBackgroundRetry("resident request poll");
  readonly #backgroundDeliveries = new MeshBackgroundQueue("resident completion/actor delivery");
  #started = false;
  #ready = false;
  #idleSince = Date.now();
  #admissions = 0;
  readonly #requestRetention: ResidentRequestRetention;
  #handover: ResidentHandoverPlan | undefined;
  #advancingRelease = false;
  #staged = false;
  #publicationFailed = false;
  #reloadEvent: Promise<unknown> | undefined;
  readonly #publications = new Set<Promise<unknown>>();
  #effectiveConfig: (() => ResidentHostConfig) | undefined;
  readonly #retention: ResidentHostConfig["retention"] & { retainRuns: boolean };

  constructor(
    readonly config: ResidentHostConfig,
    readonly onIdle: () => void = () => {},
    private readonly modelRegistry?: PiModelRegistryView,
    readonly launch?: ResidentHostLaunchContext,
  ) {
    this.#staged = !!launch?.attempt;
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
    // All resident collectors share one mutable policy, not the constructor's
    // config snapshot (nor the process-wide default object).
    this.#retention = { ...config.retention, retainRuns: config.agents.retainRuns };
    this.#requestRetention = new ResidentRequestRetention(config.residencyRoot,
      [...new Set(Object.values(residentActorRoots(config)))], this.#retention,
      (directory, expired = () => false) => {
        if (!retentionV2Enabled()) { this.agents.recoverPendingArchives(directory); return; }
        if (!this.agents.hasRunCustody(path.basename(directory)) && this.agents.recoverPendingArchives(directory, expired)) this.#requestRetention.resample();
      }, retentionV2Enabled() ? {
        // A checked exit clears exchange debt, not the manager's run-directory
        // custody. Deleting a still-managed tree destroys the next fresh exit
        // proof and can pin a deferred stopped-actor exchange indefinitely.
        run: id => this.agents.hasRunCustody(id),
        reference: id => this.agents.retentionCustodyVeto(id),
      } : undefined);
  }

  #initialize(): void {
    const { config, modelRegistry } = this;
    this.mesh = new MeshStore(config.meshRoot, config.mesh.maxEventBytes, config.mesh.maxReadEvents,
      { backgroundReadCacheMs: config.mesh.idleReadCoalesceMs ?? RUNTIME_MESH_READ_CACHE_MS, lockProtocol: config.mesh.lockProtocol });
    // Global order remains registry -> mesh, with the #535 50 ms mesh try.
    // Prepare actor/presence observations BEFORE acquisition, then validate exact
    // atomic registry generations under custody and retain custody through publication.
    // An invalid preparation is discarded and re-selected outside every fence.
    // Independent liveness may only renew existing keys with matching lineage tokens;
    // it never claims, creates, removes, or certifies a shared heartbeat.
    const registries = Object.values(residentActorRoots(config)).map((root) => new ActorRegistryStore(root));
    const publishFenced = <T>(publish: () => Promise<T>): Promise<T> =>
      ActorRegistryStore.withLocks(registries, () => this.mesh.withTryLock(publish, 50));
    const knownRegistries = registries.map(store => ({ store, snapshot: store.snapshot(), byId: new Map(store.snapshot().actors.map(row => [row.id, row])) }));
    const actorRenewalAllowed = (record: import("../topology/types.js").FabricParticipantRecord): boolean => {
      for (const known of knownRegistries) {
        const snapshot = known.store.snapshot(); // cached last-known view unless the atomic generation moved
        if (snapshot !== known.snapshot) {
          known.snapshot = snapshot;
          known.byId = new Map(snapshot.actors.map(row => [row.id, row]));
        }
        const row = known.byId.get(record.id);
        if (row) return row.rootId === config.rootId && (row.residency ?? "session") === "durable" &&
          record.actorOwnershipToken === JSON.stringify([row.rootId, row.adoptedAt ?? null, row.adoptedFrom ?? []]);
      }
      return false;
    };
    this.participants = new ParticipantDirectory(this.mesh, {
      enabled: true,
      renewActorParticipants: true,                            // host fence outlives its Main
      actorRenewalAllowed,
      preparePublicationFence: () => {
        const generations = registries.map(store => store.fingerprint());
        return () => registries.every((store, index) => store.fingerprint() === generations[index]);
      },
      // Legacy list observations may lag; authority snapshots explicitly request fresh.
      listReadCacheMs: config.mesh.idleReadCoalesceMs ?? RUNTIME_MESH_READ_CACHE_MS,
      withPublicationFence: publishFenced,
      // Acquire/release only: never carry a selected snapshot or mesh custody into
      // registry acquisition. FIFO waiting gets us into periodic free windows.
      waitForPublicationRetry: () => this.mesh.exclusive(() => undefined),
      publicationBatch: full => this.actors.presenceBatch(full),
      hostId: this.hostId,
      rootId: config.rootId,
      identity: this.identity,
      reapDeadHosts: false,                                    // its session's runtime sweeps
      presencePass: () => rootPresenceAlarms(this.mesh, this.identity, this.hostId,
        this.participants.list({ scope: "project", includeStale: true, fresh: true }), config.mesh.rootPresenceAlarmMs),
    });
    this.control = new FabricControlPlane(this.mesh, this.identity, {
      enabled: true,
      hostId: this.hostId,
      pollMs: config.mesh.actorPollMs,
      bridgeTimeoutMs: config.mesh.bridgeControlTimeoutMs,
      canConsumeMesh: () => this.#ready && this.participants.canConsumeMesh(),
      captureOwnerLease: (ownerHostId, ownerIdentityId, targetId) =>
        this.participants.captureControlOwnerLease(ownerHostId, ownerIdentityId, targetId),
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
    const currentConfig = (): Partial<ResidentHostConfig> => {
      const desired = readJson<Partial<ResidentHostConfig>>(guidanceConfigPath);
      // Desired B/C is NOT an effective A overlay or an A rollback snapshot.
      // Once accepted, however, the snapshot is authoritative: omission of an
      // optional policy is an explicit revocation, not permission to fall back
      // to the constructor's startup policy.
      return desired?.fabricExtensionPath === config.fabricExtensionPath && desired.workerPath === config.workerPath &&
        desired.rootId === config.rootId && desired.sessionId === config.sessionId ? desired : config;
    };
    const currentModelRouting = (): ResidentHostConfig["agents"]["modelRouting"] => {
      const overlay = currentConfig();
      return overlay === config ? config.agents.modelRouting : overlay.agents?.modelRouting;
    };
    this.#effectiveConfig = () => {
      const overlay = currentConfig();
      const acceptedAgents = { ...config.agents };
      if (overlay.agents?.modelRouting) acceptedAgents.modelRouting = overlay.agents.modelRouting;
      else delete acceptedAgents.modelRouting;
      return { ...config,
        // An accepted snapshot that omits modelRouting must clear the startup
        // value. Invalid/unavailable snapshots still use the startup config.
        ...(overlay === config ? {} : { agents: acceptedAgents }),
        ...(overlay.piModels ? { piModels: overlay.piModels } : {}),
        ...(overlay.modelGuidance ? { modelGuidance: overlay.modelGuidance } : {}),
        ...(overlay.kernel ? { kernel: overlay.kernel } : {}),
        ...(overlay.pythonRuntime ? { pythonRuntime: overlay.pythonRuntime } : {}),
        ...(overlay.retention ? { retention: overlay.retention } : {}) };
    };
    const currentModelGuidance = () =>
      parseFabricOwnedModelGuidance(currentConfig().modelGuidance ?? config.modelGuidance);
    // The session's visible models (synced at each ensureHost) plus, after a miss, this host's
    // own refreshed Pi registry: the one shared resolver, so an already-running host resolves a
    // model added to models.json after it started (pi-fabric#138).
    // A bare resident registry does not load Main's provider extensions. Workers do:
    // retain the trusted, auth-filtered catalog synced by Main for exact route pins
    // and candidates, just as ordinary resident model selection does below.
    // Keep one view so concurrent exact misses share the bounded registry refresh.
    const routeRegistry: PiModelRegistryView = {
      getAvailable: () => {
        const live = modelRegistry?.getAvailable() ?? [];
        const snapshot = (currentConfig().piModels ?? config.piModels)?.available ?? [];
        return [...live, ...snapshot.filter(candidate => !live.some(model =>
          model.provider === candidate.provider && model.id === candidate.id))];
      },
      ...(modelRegistry?.refresh ? { refresh: () => modelRegistry.refresh!() } : {}),
    };
    const residentRouteRegistry = (): PiModelRegistryView => routeRegistry;
    const resolveResidentPiModel = async (selector?: string, options: { requiredPin?: boolean; closest?: boolean } = {}): Promise<string> => {
      if (options.requiredPin) {
        const exact = await resolvePiRoutePin({ selector: selector ?? "", registry: residentRouteRegistry(),
          aliases: normalizeModelAliases((currentConfig().piModels ?? config.piModels)?.aliases) });
        return `${exact.provider}/${exact.id}`;
      }
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
        policy: config.agents,
        closest: options.closest ?? true,
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
      completionRecipient: () => ({ rootId: config.rootId, sessionId: config.sessionId, cwd: config.cwd,
        projectRoot: config.projectRoot, name: currentConfig().mainName ?? config.mainName ?? "main", role: config.role,
        startedAt: config.mainStartedAt ?? this.participants.lastKnown?.(config.rootId)?.participant.startedAt ?? 0 }),
      hostId: this.hostId,
      identityId: this.identity.id,
      retention: this.#retention,
      preparePiModel: async (model, requiredPin) => resolveResidentPiModel(model, { requiredPin: requiredPin ?? false }),
      resolveParticipantGuidance: ({ model }) => {
        if (!model) return undefined;
        return resolveFabricModelGuidance(currentModelGuidance(), {
          model,
          target: "participant",
          includeSlots: false,
        }).appendText || undefined;
      },
      onLifecycle: (event) => { if (this.lifecycle) this.#trackPublication(this.lifecycle.publishBackground(event)); },
      onSettled: (result) => {
        // Only public durable task runs: actor activations are cleaned by their actor (review/astra on #136).
        if (result.actorId) return;
        const file = residentResultPath(config.residencyRoot, result.id);
        fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
        try {
          writeJsonAtomic(file, result, { durable: true });
          const trackedRun = this.agents.runDirectory(result.id);
          const runDirectory = trackedRun ?? path.join(config.residencyRoot, "runs", result.id);
          // Rejected queued spawns have no worker source; recovered admitted
          // runs do, even though this manager no longer has their transport.
          if (!trackedRun && !fs.existsSync(path.join(runDirectory, "status.json"))) return;
          // Retain/retry full sources on faults; logical settlement is recoverable
          // even when inbox notifications are disabled.
          const recipient = completionRecipientFromRun(config.meshRoot, runDirectory);
          if (!recipient) throw new Error(`Missing admitted completion recipient for ${result.id}`);
          saveCompletion(config.meshRoot, recipient, result);
        } catch (error) { this.#publicationFailed = true; throw error; }
      },
      onBackgroundComplete: (result) => {
        if (!config.agents.notifyOnComplete) return;
        const durationMs = Math.max(0, (result.finishedAt ?? Date.now()) - result.startedAt);
        const summary = (result.text || result.error || "no result").slice(0, COMPLETION_MAX_CHARS);
        this.#trackPublication(this.#queueDelivery(
          { id: result.id, name: result.name, kind: "agent" },
          `Fabric agent ${result.id.slice(0, 8)} ${result.status} after ${Math.round(durationMs / 1_000)}s: ${summary}`,
          "followUp",
          true,
          result,
          result.id,
        ));
      },
    });
    const canManageActor = (id: string, fresh = true): boolean | undefined => {
      const participant = this.participants.get(id, undefined, { fresh });
      return participant ? participant.ownerHostId === this.hostId : undefined;
    };
    const snapshotActorOwnership = (fresh = true): ReadonlyMap<string, boolean> => new Map(
      this.participants.list({ scope: "project", fresh })
        .map((participant) => [participant.id, participant.ownerHostId === this.hostId]),
    );
    const lineageAlive = (rootId: string): boolean =>
      this.participants.lineageAlive(rootId);
    const actorRoots = residentActorRoots(config);
    this.#routeOwner = new ShadowRouteOwner(() => currentConfig().shadowRouting ?? config.shadowRouting);
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
        this.#trackPublication(this.#queueDelivery(
          { id: actor.id, name: actor.name, kind: "actor" },
          message.text,
          mode,
          triggers,
          message.data,
          undefined,
          // #471: actor output is bound to the exact owning root; a dead root is retained,
          // never re-homed to another Main selected by cwd, project, or launch metadata.
          config.rootId,
          message.source === "fabric-host" ? undefined : message.principal,
          message.source === "fabric-host" ? "fabric-host" : "actor-output",
        ));
      },
      {
        // Restoration must not launch queued work until owner and readiness publication commit.
        releasePaused: true,
        canConsumeMesh: () => this.#ready && this.participants.canConsumeMesh(),
        presencePublisher: { refresh: () => this.participants.refreshPresence(), schedule: () => this.participants.scheduleRefresh() },
        persistent: true,
        canManageActor,
        snapshotActorOwnership,
        lineageAlive,
        // smarty-dev#6062: read per activation, so turning mode "off" in config takes effect live.
        deadRootFilter: () => (currentConfig().agents ?? config.agents)?.deadRootFilter,
        claimResidency: "durable",
        rootId: config.rootId,
        // Recorded on every actor it creates, and the only project whose orphans it adopts, and
        // then only as a project agent's host.
        project: (typeof config.project === "string" ? config.project : projectOf(config.cwd)),
        role: typeof config.role === "string" ? config.role : undefined,
        meshCursorPath: path.join(config.residencyRoot, "actor-mesh-cursor.json"),
        retention: this.#retention,
        ...(typeof config.actors?.maxSessionBytes === "number" ? { maxSessionBytes: config.actors.maxSessionBytes } : {}),
        resolvePiModel: (model, requiredPin) => resolveResidentPiModel(model, { requiredPin: requiredPin ?? false, closest: false }),
        prepareModelRoute: async (input, signal) => {
          const { prepareModelRoute } = await import("../agents/model-route-prepare.js");
          const overlay = currentConfig();
          return prepareModelRoute({ ...input, signal, config: currentModelRouting(),
            registry: residentRouteRegistry(), aliases: normalizeModelAliases((overlay.piModels ?? config.piModels)?.aliases),
            assertModelAllowed: model => this.agents.assertModelAllowed(model, "pi"),
            evaluate: (request, routeSignal) => this.#routeOwner!.evaluate(request, routeSignal) });
        },
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
        canConsumeMesh: () => this.#ready && this.participants.canConsumeMesh(),
      },
      (subscription, event) => this.#deliverLifecycle(subscription, event),
    );
  }

  async start(): Promise<void> {
    if (this.#started) return;
    await this.#acquireLock();
    this.#started = true;
    try {
      // The launcher's preflight cannot admit this host: its native child may
      // arrive after the watchdog stopped the previous owner. Check only AFTER
      // taking the host fence and serialize with custody publication using the
      // watchdog's root transaction lock. A busy/broken transaction fails closed.
      // Once admitted, our host fence excludes the old watchdog's live owner;
      // its under-transaction revalidation cannot publish for that dead owner.
      let admissionFd: number | undefined;
      try {
        if (process.platform !== "win32") {
          admissionFd = await lockFile(path.join(this.config.residencyRoot, "handover.lock"), 0, process.platform === "linux");
        }
        assertNoWatchdogCustody(this.config.residencyRoot);
      } finally { if (admissionFd !== undefined) fs.closeSync(admissionFd); }
      // Archived runs are read on demand, never walked before the host lease is up.
      // The streaming request collector replays pending full archives before
      // terminal retention after readiness. Failed sinks retain their sources.
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
      this.control.start((command, from, signal, verification) =>
        this.#acceptControl(command, from, signal, verification));
      if (this.#staged) this.control.pause();
      await this.participants.start().catch(() => undefined);
      // A publication failure is not readiness. Keep this same start pending,
      // with requests/events untouched, until a real locked renewal confirms it.
      while (!this.participants.canConsumeMesh()) {
        if (this.#closed) throw new Error(HOST_CLOSING_RETRY);
        await delay(20);
      }
      this.lifecycle.start();
      if (this.#staged) {
        this.lifecycle.pause();
        await this.#probeWorkerStartup();
      }
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
        fabricExtensionPath: this.config.fabricExtensionPath,
        token: this.#token,
        ...(process.env.PI_FABRIC_RESIDENT_LAUNCH_TOKEN ? { launchToken: process.env.PI_FABRIC_RESIDENT_LAUNCH_TOKEN } : {}),
        startedAt: now,
        readyAt: now,
        maintenanceReady: 1, // client requires the same-token startup receipt before business admission
        commands: RESIDENT_COMMANDS,
        requestFence: 1,
        callerBoundSpawn: 1,
        requestExpiry: 1,
        creationIdempotency: 1,
        ...(this.launch ? { releaseRoot: this.launch.spec.releaseRoot, configDigest: this.launch.spec.digest,
          handover: { abi: RESIDENT_HANDOVER_ABI, launcher: this.launch.launcher },
          ...(this.launch.attempt ? { attempt: this.launch.attempt } : {}) } : {}),
      };
      atomicWrite(this.#ownerPath, owner);
      fs.rmSync(this.#errorPath, { force: true });
      // The originating client may cancel this owned attempt until it sees the
      // required receipt. Commit it BEFORE opening any business gate or resuming
      // restored queues: publication failure/timeout must remain a non-serving
      // start, not shutdown of work that may already have escaped the attempt.
      atomicWrite(path.join(this.config.residencyRoot, "maintenance-ready.json"), { token: this.#token, readyAt: now });
      // No fallible/awaited startup work remains. Accepted backlog is untouched
      // on failure; maintenance/collection stays on normal post-readiness ticks.
      this.#ready = true;
      // Retention is not part of request admission/heartbeat/claim. A bounded
      // preparation cursor progresses even between request-retention samples.
      if (retentionV2Enabled()) {
        this.#maintenanceTimer = setInterval(() => this.#maintainRequests(), 100);
        this.#legacyArchive = new ResidentLegacyRunArchive(this.config.residencyRoot, this.#retention, {
          actorRoots: [...new Set(Object.values(residentActorRoots(this.config)))],
          isRetained: id => this.#closed || !!this.#handover || !this.participants.canConsumeMesh() || this.agents.hasRunCustody(id),
        });
        this.#legacyArchive.start();
      }
      // Removals a previous host accepted: their runs ended with it.
      if (!this.#staged) {
        this.actors.resumeAfterRelease();
        void this.#backgroundDeliveries.enqueue(async () => {
          await this.actors.finishPendingRemovals();
          this.#writeRemovals();
        });
        void this.#retryDeliveries();
      }
    } catch (error) {
      await this.close();
      throw error;
    }
  }

  async close(): Promise<void> {
    if (this.#closed || !this.#started) return;
    this.#closed = true;
    const routeClosed = this.#routeOwner?.close();
    if (this.#requestTimer) clearInterval(this.#requestTimer);
    this.#requestTimer = undefined;
    if (this.#maintenanceTimer) clearInterval(this.#maintenanceTimer);
    this.#maintenanceTimer = undefined;
    this.#requestRetention.close();
    await this.#legacyArchive?.close();
    // Stop drains first so an in-flight ask can settle within the actor shutdown grace.
    const actorsClosed = this.actors?.close();
    // Observed below by closeWithActors, but only after further awaits: a rejection
    // before then must not become an unhandled rejection that kills the host (pi-fabric#577).
    void actorsClosed?.catch(() => undefined);
    while (this.#pollingRequests || this.#admissions) await delay(10);
    await this.participants?.quiesce().catch(() => undefined);
    await this.lifecycle?.close().catch(() => undefined);
    try {
      await closeWithActors({ close: () => actorsClosed }, () => this.control?.close().catch(() => undefined));
    } finally {
      try {
        try {
          await this.agents?.close();
          await routeClosed;
        } finally {
          // Fenced actor deliveries may still be acquiring custody. Join them
          // before releasing the host; closed hosts retain their durable outbox.
          await Promise.allSettled([...this.#publications]);
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
    verification?: "mesh" | "bridge",
  ): Promise<FabricControlAcceptance> {
    if (this.#closed || this.#staged || this.#handover) {
      return { accepted: false, error: HOST_CLOSING_RETRY };
    }
    assertMeshConsumption(() => this.participants.canConsumeMesh());
    this.#admissions++;
    try { return await this.#handleControl(command, from, signal, verification); }
    finally { this.#admissions--; }
  }

  async #handleControl(command: FabricControlCommand, from: MeshIdentity, signal?: AbortSignal, verification?: "mesh" | "bridge"): Promise<FabricControlAcceptance> {
    if (command.operation === "setModel" || command.operation === "setThinking") {
      return { accepted: false, error: "remote Main model changes are not supported yet; see smarty-dev#4153" };
    }
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
        // The resident command path remains owner-only. Legacy mesh stop also
        // serves a detached actor after its originating Main withdraws: a
        // verified Main may stop it, without acquiring setter/reset authority.
        // While the root is addressable (including reload), retain its fence.
        const now = Date.now();
        // One snapshot includes stale/reloading roots: a lease lapse is not
        // withdrawal, and separate live/stale reads could race a renewal.
        const root = this.participants.list({ scope: "project", kinds: ["root"], includeStale: true, fresh: true }, now)
          .find(candidate => candidate.id === this.config.rootId);
        const detachedMain = !root && from.kind === "main" && (verification === "mesh" || verification === "bridge");
        if (!detachedMain) {
          const caller = this.participants.get(from.id, now, { fresh: true });
          this.#authorizeResidentSetter({ identity: from, hostId: caller?.ownerHostId ?? "" });
        }
        await this.actors.stop(command.targetId);
        this.participants.scheduleRefresh();
        return { accepted: true, messageId: command.commandId };
      } catch (error) {
        return { accepted: false, error: errorMessage(error) };
      }
    }
    const provenance = from && (verification === "mesh" || verification === "bridge")
      ? fabricTurnProvenance(from, command.operation === "steer" ? "steer" : "followUp", verification, command.principal) : undefined;
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
          { provenance, ...controlActorBindingOptions(command, from, this.actors.status(command.targetId).rootId,
            this.participants.get(from.id, undefined, { fresh: true })?.rootId) },
        );
        return { accepted: true, messageId: result.id, result };
      } catch (error) {
        return { accepted: false, error: errorMessage(error) };
      }
    }
    try {
      this.agents.status(command.targetId);
      const result = command.operation === "steer"
        ? this.agents.steer(command.targetId, message, command.data, provenance)
        : this.agents.followUp(command.targetId, message, command.data, provenance);
      return { accepted: true, messageId: result.messageId,
        ...(result.warning ? { warning: result.warning } : {}) };
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
        this.participants.get(from.id, undefined, { fresh: true })?.rootId);
      // Validate now without turning the resolved owner defaults into per-call overrides.
      await this.actors.resolveActivationBinding(command.targetId, options);
      if (this.#closed) return { accepted: false, error: HOST_CLOSING_RETRY };
      assertMeshConsumption(() => this.participants.canConsumeMesh());
      const result = this.actors.tell(command.targetId, message, command.data, { provenance, ...options });
      return { accepted: true, messageId: result.messageId };
    } catch (error) {
      if (error instanceof MeshConsumptionPausedError) throw error;
      return { accepted: false, error: errorMessage(error) };
    }
  }

  async #deliverLifecycle(
    subscription: FabricLifecycleSubscription,
    event: FabricLifecycleEvent,
  ): Promise<void> {
    if (this.#closed || this.#staged || this.#handover) throw new Error(HOST_CLOSING_RETRY);
    assertMeshConsumption(() => this.participants.canConsumeMesh());
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
    const target = this.participants.get(subscription.to, undefined, { fresh: true });
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
    rootId: string | (() => string) = this.config.rootId,
    principal?: FabricPrincipal,
    source?: ResidentDeliveryRecord["source"],
  ): Promise<void> {
    const id = randomUUID();
    const persist = (target: string): void => {
      const record: ResidentDeliveryRecord = {
        format: RESIDENT_HOST_FORMAT,
        id,
        rootId: target,
        from,
        ...(source ? { source } : {}),
        ...(principal ? { principal } : {}),
        delivery,
        triggerTurn,
        message,
        ...(data === undefined ? {} : { data }),
        ...(agentCompletionId ? { agentCompletionId } : {}),
        createdAt: Date.now(),
      };
      // Persist before handing off: idle exit/queue pressure must not drop custody.
      // Agent completions use the fixed creating root and still persist before yielding.
      writeJsonAtomic(path.join(this.#deliveryOutboxPath, `${id}.json`), record, { durable: true });
    };
    if (typeof rootId === "function") {
      // Serialize proof+absence, target selection and the irreversible outbox
      // write with resumed-root proof invalidation. Never persist a stale choice.
      // This stays on the mesh lock, not file custody (smarty-dev#6477 L5):
      // resumeLineage() invalidates the proof with a shared-state delete.
      try { await this.mesh.exclusive(() => persist(rootId())); }
      catch { persist(this.config.rootId); } // Unknown custody keeps the original mailbox.
    } else persist(rootId);
    await this.#retryDeliveries();
  }

  #retryDeliveries(): Promise<unknown> {
    if (this.#closed || this.#staged) return Promise.resolve();
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
      // Actor output is owned by the host that produced it. Older hosts could have
      // persisted an inferred successor root; never replay that stale target.
      const ownerRoot = record.from.kind === "actor" ? this.config.rootId : record.rootId;
      const publish = ownerRoot === record.rootId ? record : { ...record, rootId: ownerRoot };
      const key = `${residentDeliveryPrefix(ownerRoot)}${record.id}`;
      try {
        await this.mesh.put({ key, value: publish, identity: this.identity, ifVersion: 0 });
      } catch (error) {
        // A restart after put but before unlink replays the SAME private UUID. A CAS
        // conflict (including a consumed tombstone) means it was handed off already.
        if (!(error instanceof Error && error.message.startsWith(`Mesh compare-and-swap failed for ${key}: expected version 0,`))) {
          if (isMeshLockTimeout(error)) throw error;
          await this.mesh.put({
            key, value: { ...publish, message: publish.message.slice(0, Math.max(1, this.config.mesh.eventContextChars)), data: { fabricTruncated: true } },
            identity: this.identity, ifVersion: 0,
          });
        }
      }
      // Only a durable mesh handoff releases ownership. Main journals the stable id.
      fs.rmSync(file);
    }
  }

  async #pollRequests(): Promise<void> {
    if (!this.#ready || this.#pollingRequests || this.#closed) return;
    if (this.#staged || this.#handover) { await this.#advanceRelease(); return; }
    this.#pollingRequests = true;
    try {
      let entries: string[];
      try {
        entries = fs.readdirSync(this.#requestsPath).filter((entry) => entry.endsWith(".json"));
      } catch {
        return;
      }
      for (const entry of entries.slice(0, 32)) {
        if (this.#handover || this.#closed) break;
        const source = path.join(this.#requestsPath, entry);
        const processing = path.join(this.#processingPath, entry);
        // Never overwrite an admitted exchange while its response is still pending.
        if (this.#boundaryRequests.has(processing)) continue;
        try {
          fs.renameSync(source, processing);
        } catch {
          continue;
        }
        let releaseAdmission!: () => void;
        let boundary = false;
        const admitted = new Promise<void>(resolve => { releaseAdmission = resolve; });
        const response = this.#processRequest(processing, () => {
          // ActorManager installed its commit fence and boundary waiter (or stop
          // intent) synchronously. Only settlement/publication may now run aside.
          boundary = true;
          releaseAdmission();
        });
        await Promise.race([response, admitted]);
        if (boundary) {
          this.#boundaryRequests.set(processing, response);
          // Shutdown/handover retains custody until the terminal response is durable.
          this.#trackPublication(response);
          void response.finally(() => this.#boundaryRequests.delete(processing)).catch(() => undefined);
        }
      }
    } finally {
      this.#pollingRequests = false;
      if (!retentionV2Enabled()) this.#maintainRequests();
      this.#checkIdle();
    }
  }

  #maintainRequests(): void {
    const now = Date.now();
    if (!this.#ready || this.#closed || !this.participants.canConsumeMesh() ||
        (!retentionV2Enabled() && !this.#requestRetention.due(now))) return;
    // ensureHost/syncPiModels already publishes reloads to config.json. Apply
    // only the same-release/root/session overlay at the next existing sweep;
    // actor archives and agent collectors hold this same policy object.
    Object.assign(this.#retention, this.#effectiveConfig?.().retention ?? this.config.retention);
    const live = retentionV2Enabled() ? this.agents.retentionReferences({ now }) : this.agents.retentionReferences();
    if (retentionV2Enabled() && !this.#requestRetention.due(now)) return;
    for (const id of this.actors.inFlightActorIds()) live.add(id);
    const stoppedWritersGone = new Set<string>();
    for (const actor of this.actors.listOwned()) {
      if (actor.lastRunId) live.add(actor.lastRunId);
      if (actor.status !== "stopped" || actor.inFlightRun) live.add(actor.id);
      else if (!live.has(actor.id) && !live.has("*")) stoppedWritersGone.add(actor.id);
    }
    for (const removal of this.actors.pendingRemovals()) {
      live.add(removal.id);
      if (removal.runId) live.add(removal.runId);
    }
    this.#requestRetention.sweep(now, live, 5, stoppedWritersGone);
  }

  #checkIdle(): void {
    if (this.#closed || this.#staged || this.#handover) return;
    const activeActor = this.actors.hasActiveDurableActor();
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

  #trackPublication(promise: Promise<unknown>): void {
    this.#publications.add(promise);
    void promise.then(() => this.#publications.delete(promise), () => {
      this.#publicationFailed = true;
      this.#publications.delete(promise);
    });
  }

  async #probeWorkerStartup(): Promise<void> {
    const probe = await this.agents.spawn({ task: "Resident release startup probe; no inference", name: "resident-startup-probe",
      runner: "pi", transport: "process", tools: [], extensions: true, recursive: true,
      residentStartupProbe: true, timeoutMs: 20_000, thinking: "off" });
    const result = await this.agents.wait(probe.id, { timeoutMs: 25_000 });
    if (result.status !== "completed" || result.text !== "resident worker startup verified") {
      throw new Error(`Resident worker startup probe failed: ${result.error ?? result.status}`);
    }
    await this.agents.cleanup(probe.id);
  }

  #prepareRelease(command: Extract<ResidentCommand, { operation: "releaseChange" }>): void {
    this.#authorizeResidentSetter(command.caller);
    const launch = this.launch;
    if (!launch || process.platform !== "linux" || !exactResidentProcess(launch.launcher)) {
      throw new Error("Resident launcher has no attested handover custody capability; stay on current release");
    }
    if (!mainGenerationCurrent(this.config.residencyRoot, command.main) ||
        command.main.rootId !== this.config.rootId || command.main.sessionId !== this.config.sessionId ||
        command.main.releaseRoot !== command.target.releaseRoot) throw new ResidentActorAuthorizationError("Obsolete Main release intent");
    validateLaunchSpec(command.target);
    const previous = residentLaunchSpec(this.#effectiveConfig?.() ?? this.config, launch.spec.entry, launch.spec.runtime);
    if (previous.releaseRoot === command.target.releaseRoot) return;
    assertPreviousLaunchSpec(launch.spec, previous);
    assertHandoverTopology(previous, command.target);
    const active = readHandoverJson<ResidentHandoverState>(handoverPath(this.config.residencyRoot));
    if (handoverActive(active)) throw new Error("Another resident release transaction is in custody");
    if (readHandoverJson(handoverOutcomePath(this.config.residencyRoot, command.target))) {
      throw new Error("Resident target was already attempted; release retry is suppressed");
    }
    const owner = readHandoverJson<ResidentHostOwner>(this.#ownerPath);
    if (owner?.token !== this.#token || owner.pid !== process.pid || !owner.processStartTime) throw new Error("Resident owner fence is uncertain");
    const plan: ResidentHandoverPlan = { id: randomUUID(), rootId: this.config.rootId, old: {
      pid: owner.pid, processStartTime: owner.processStartTime, token: owner.token, hostId: owner.hostId },
      launcher: launch.launcher, previous, target: command.target, caller: command.caller, main: command.main, createdAt: Date.now() };
    writeLaunchSnapshot(this.config.residencyRoot, previous);
    writeLaunchSnapshot(this.config.residencyRoot, command.target);
    commitResidentRequest(this.config.residencyRoot, command, plan.id, this.hostId);
    // Check before publishing an active transaction: a crash between preparing
    // and cancellation would otherwise strand dead-host recovery without custody.
    // A deferred diagnostic is terminal from its very first publication.
    try { assertAutomaticReleaseRecovery(); }
    catch (error) {
      writeHandoverState(this.config.residencyRoot, plan, "cancelled", errorMessage(error));
      return;
    }
    writeHandoverState(this.config.residencyRoot, plan, "preparing");
    this.#handover = plan;
    this.actors.pauseForRelease(); this.control.pause(); this.lifecycle.pause();
  }

  #cancelRelease(plan: ResidentHandoverPlan, reason: string): void {
    if (decideHandover(this.config.residencyRoot, { id: plan.id, state: "cancelled" }).state === "custody") return;
    writeHandoverState(this.config.residencyRoot, plan, "cancelled", reason);
    this.#handover = undefined;
    this.actors.resumeAfterRelease(); this.control.resume(); this.lifecycle.resume();
  }

  async #advanceRelease(): Promise<void> {
    if (this.#advancingRelease || this.#closed) return;
    this.#advancingRelease = true;
    try {
      if (this.#staged) {
        const state = readHandoverJson<ResidentHandoverState>(handoverPath(this.config.residencyRoot));
        const attempt = this.launch?.attempt;
        if (!state || state.plan.id !== attempt?.id) throw new Error("Staged resident transaction identity changed");
        const terminal = attempt.kind === "target" ? "complete" : "fallback";
        if (state.phase !== terminal) return;
        if (attempt.kind === "target") {
          this.#reloadEvent ??= this.mesh.publish({ topic: "host.reloaded", from: this.identity,
            data: { old: state.plan.previous.releaseRoot, new: state.plan.target.releaseRoot, transaction: state.plan.id } });
          await this.#reloadEvent;
        }
        const owner = readHandoverJson<ResidentHostOwner>(this.#ownerPath);
        if (owner?.token !== this.#token || owner.attempt?.id !== attempt.id) throw new Error("Staged resident owner changed before service commitment");
        // Attempt identity gates staging only. Retire it before business service
        // so a later C prepare/cancellation cannot hide this healthy owner.
        const { attempt: _completedAttempt, ...servingOwner } = owner;
        writeJsonAtomic(this.#ownerPath, servingOwner, { durable: true });
        this.#staged = false;
        this.actors.resumeAfterRelease(); this.control.resume(); this.lifecycle.resume();
        this.#trackPublication(this.#backgroundDeliveries.enqueue(async () => {
          await this.actors.finishPendingRemovals();
          this.#writeRemovals();
        }));
        return;
      }
      const plan = this.#handover;
      if (!plan || this.#pollingRequests) return;
      const custody = readHandoverJson<{ id: string; launcher: ResidentLauncherIdentity }>(handoverCustodyPath(this.config.residencyRoot, plan.id));
      if (custody) {
        assertAutomaticReleaseRecovery();
        if (custody.id !== plan.id || JSON.stringify(custody.launcher) !== JSON.stringify(plan.launcher) ||
            !exactResidentProcess(plan.launcher)) throw new Error("Resident launcher custody is uncertain");
        // Receipt precedes release of A's stable flock. The Main is no longer the executor.
        writeHandoverState(this.config.residencyRoot, plan, "released");
        this.onIdle();
        return;
      }
      if (!mainGenerationCurrent(this.config.residencyRoot, plan.main) || !exactResidentProcess(plan.launcher)) {
        this.#cancelRelease(plan, "Main/launcher lost before custody"); return;
      }
      if (Date.now() - plan.createdAt > HANDOVER_DRAIN_MS) {
        this.#cancelRelease(plan, "Release drain timed out without cutting a run"); return;
      }
      if (fs.readdirSync(this.#processingPath).some((entry) => entry.endsWith(".json"))) return;
      if (this.#admissions || this.actors.inFlightCount() || this.actors.pendingRemovals().length ||
          this.agents.listForUi().some((agent) => agent.status === "queued" || agent.status === "running")) return;
      const state = readHandoverJson<ResidentHandoverState>(handoverPath(this.config.residencyRoot));
      if (state?.plan.id !== plan.id) throw new Error("Resident release transaction changed");
      if (state.phase === "cancelled") { this.#cancelRelease(plan, state.error ?? "Launcher deferred release before custody"); return; }
      if (state.phase === "custody") return;
      await this.control.checkpointForRelease();
      await this.lifecycle.checkpointForRelease();
      await Promise.all([...this.#publications]);
      await this.#backgroundDeliveries.checkpointForRelease();
      await this.#flushingDeliveries;
      await this.#flushDeliveries();
      if (fs.existsSync(this.#deliveryOutboxPath) && fs.readdirSync(this.#deliveryOutboxPath).some(entry => entry.endsWith(".json"))) {
        throw new Error("Resident release has pending delivery outbox obligations");
      }
      if (this.#publicationFailed) throw new Error("Resident release has unconfirmed public results/deliveries");
      await this.actors.checkpointForRelease();
      await this.agents.checkpointForRelease(plan.createdAt + HANDOVER_DRAIN_MS);
      assertAutomaticReleaseRecovery();
      validateLaunchSpec(plan.previous); validateLaunchSpec(plan.target);
      writeHandoverState(this.config.residencyRoot, plan, "custody");
    } catch (error) {
      // Preparation is reversible. After custody an independent launcher owns recovery.
      if (this.#handover && !readHandoverJson(handoverCustodyPath(this.config.residencyRoot, this.#handover.id))) {
        this.#cancelRelease(this.#handover, errorMessage(error));
      } else if (this.#staged) {
        // Never turn a failed event/commit check into ungated business work.
        this.#publicationFailed = true;
      }
    } finally { this.#advancingRelease = false; }
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

  async #processRequest(filePath: string, boundaryAdmitted?: () => void): Promise<void> {
    const command = readJson<ResidentCommand>(filePath);
    const requestId = path.basename(filePath, ".json");
    let response: ResidentCommandResponse;
    try {
      if (
        (command?.format !== RESIDENT_HOST_FORMAT && command?.format !== RESIDENT_ACTOR_COMMAND_FORMAT && command?.format !== RESIDENT_EXPIRING_COMMAND_FORMAT) ||
        command.rootId !== this.config.rootId ||
        command.requestId !== requestId
      ) {
        throw new Error("Invalid Fabric residency request");
      }
      assertResidentRequestNotExpired(this.config.residencyRoot, requestId, command.format);
      // Validate the runtime JSON discriminant before any actor lookup/mutation.
      if (!isResidentCommandOperation(command.operation)) {
        throw new ResidentCommandUnsupportedError(`Unsupported Fabric residency command: ${String(command.operation)}`);
      }
      if (readResidentRequestDecision(this.config.residencyRoot, requestId)?.state === "abandoned") {
        throw new Error(`Fabric residency request ${requestId} was abandoned before commit`);
      }
      // Global order: actor registries (sorted path), then mesh. Dispatch controls
      // OUTSIDE mesh.exclusive: create/remove/setters acquire their own registry
      // mutation fence and only afterwards publish presence on the mesh. Holding
      // mesh custody here would invert the heartbeat/adoption publication fence.
      response = await this.#executeOnce(command, boundaryAdmitted);
    } catch (error) {
      response = { format: RESIDENT_HOST_FORMAT, requestId, ok: false, error: errorMessage(error),
        ...(error instanceof ResidentCommandUnsupportedError || error instanceof ResidentRequestExpiredError
          ? { errorCode: error.code } : {}), completedAt: Date.now() };
    }
    if (response.ok) await testResidentRequestDelay("after_commit");
    const responsePath = path.join(this.#responsesPath, `${requestId}.json`);
    writeJsonAtomic(responsePath, response, { durable: true });
    // An abandoned caller already left; clean late responses as well as processing files.
    if (readResidentRequestDecision(this.config.residencyRoot, requestId)?.state === "abandoned") {
      fs.rmSync(responsePath, { force: true });
    }
    fs.rmSync(filePath, { force: true });
    this.participants.scheduleRefresh();
  }

  #pruneCreations(): void {
    const cutoff = Date.now() - 10 * 60_000;
    for (const [key, entry] of this.#creations) {
      if (entry.completedAt !== undefined && entry.completedAt <= cutoff) this.#creations.delete(key);
    }
    const completed = [...this.#creations].filter(([, entry]) => entry.completedAt !== undefined);
    for (const [key] of completed.slice(0, Math.max(0, completed.length - 256))) this.#creations.delete(key);
    // Never evict an in-flight creation: retries must join the same promise.
  }

  async #executeOnce(command: ResidentCommand, boundaryAdmitted?: () => void): Promise<ResidentCommandResponse> {
    if ((command.operation !== "spawnBound" && command.operation !== "createActor") || command.idempotencyKey === undefined) {
      return this.#executeRequest(command, boundaryAdmitted);
    }
    if (typeof command.idempotencyKey !== "string" || !command.idempotencyKey.length || command.idempotencyKey.length > 256) {
      throw new Error("Resident idempotencyKey must be a string of 1 to 256 characters");
    }
    // Retry receipts must not bypass the trusted live-caller fence, even on cache hits.
    if (command.operation === "spawnBound") {
      const caller = command.caller;
      assertResidentTaskCaller(caller,
        caller && this.participants.get(caller.id, Date.now(), { fresh: true }), this.config.rootId);
    }
    this.#pruneCreations();
    // Operation-scoped; this host already validates its one root before dispatch.
    const key = JSON.stringify([command.operation, command.idempotencyKey]);
    let entry = this.#creations.get(key);
    if (!entry) {
      entry = { result: Promise.resolve().then(() => this.#executeRequest(command)) };
      this.#creations.set(key, entry);
      const tracked = entry;
      // The caller awaits the result; this bookkeeping branch must not leak its rejection.
      void tracked.result.then(() => { tracked.completedAt = Date.now(); this.#pruneCreations(); }, () => undefined);
    }
    const response = await entry.result;
    if (response.requestId !== command.requestId) {
      // Cache hits still participate in the caller's existing cancellation fence,
      // including failures after commit. Replays of a retry receipt are safe too.
      const original = readResidentRequestDecision(this.config.residencyRoot, response.requestId);
      if (response.ok || original?.state === "committed") {
        const id = response.handle?.id ?? response.actor?.id ?? original?.id;
        if (!id) throw new Error("Resident creation result has no entity ID");
        const decision = readResidentRequestDecision(this.config.residencyRoot, command.requestId);
        if (decision?.state !== "committed" || decision.id !== id ||
          decision.operation !== command.operation || decision.ownerHostId !== this.hostId) {
          commitResidentRequest(this.config.residencyRoot, command, id, this.hostId);
        }
      }
    }
    // Cache the entity/outcome, not the retry exchange's completion time. A
    // later generation must be acknowledgeable without relaxing validAck.
    return { ...response, requestId: command.requestId,
      completedAt: response.requestId === command.requestId ? response.completedAt
        : Math.max(Date.now(), residentRequestGeneration(command.requestId) ?? 0) };
  }

  async #executeRequest(command: ResidentCommand, boundaryAdmitted?: () => void): Promise<ResidentCommandResponse> {
    const requestId = command.requestId;
    let response: ResidentCommandResponse;
    try {
      await testResidentRequestDelay("before_commit");
      const commit = (id: string): void => commitResidentRequest(this.config.residencyRoot, command, id, this.hostId);
      if (command.operation === "releaseChange") {
        this.#prepareRelease(command);
        response = { format: RESIDENT_HOST_FORMAT, requestId, ok: true, completedAt: Date.now() };
      } else if (command.operation === "spawnBound") {
        if (
          command.request.residentStartupProbe ||
          command.request.sessionSeed ||
          command.request.sessionFile ||
          command.request.actorId ||
          command.request.actorName ||
          command.request.meshRoot ||
          command.request.runnerSessionId ||
          command.request.images
        ) {
          throw new Error("Durable agents.spawn accepts only its public task and run settings");
        }
        // No executor fallback: revalidate the captured caller before the mutation fence.
        const caller = command.caller;
        const returnAddress = assertResidentTaskCaller(caller,
          caller && this.participants.get(caller.id, Date.now(), { fresh: true }), this.config.rootId);
        const handle = await this.agents.spawn({ ...command.request, residency: "durable" }, undefined, undefined, commit, undefined, undefined, undefined, returnAddress);
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
        const { instructionsFile: _file, sha256: _digest, ...base } = command.request;
        const instructions = resolveActorInstructions(command.request, this.config.agents.instructionsRoot);
        const actor = await this.actors.create({ ...base, instructions }, { asRegistryOwner: true, beforeCommit: commit });
        response = {
          format: RESIDENT_HOST_FORMAT,
          requestId,
          ok: true,
          actor: actor as FabricActorInfo,
          completedAt: Date.now(),
        };
      } else if (command.operation === "operatorActor") {
        if ((command.action !== "stop" && command.action !== "remove") ||
            typeof command.id !== "string" || !command.id.trim() ||
            (command.confirmDeadRoot !== undefined && typeof command.confirmDeadRoot !== "string") ||
            (command.dryRun !== undefined && typeof command.dryRun !== "boolean")) {
          throw new Error("Invalid resident operator actor request");
        }
        const evidence = readResidentOperatorEvidence(this.config, this.mesh);
        const check = () => assertResidentOperatorConfirmed(
          readResidentOperatorEvidence(this.config, this.mesh), command.confirmDeadRoot);
        assertResidentOperatorConfirmed(evidence, command.confirmDeadRoot, command.dryRun === true);
        // Exact id/name within this executor's root only; never resolve via the caller's root.
        const candidates = this.actors.listOwned().filter(actor => actor.rootId === this.config.rootId &&
          actor.residency === "durable" && (actor.id === command.id || actor.name === command.id));
        if (candidates.length !== 1) throw new Error(candidates.length
          ? `Ambiguous resident actor: ${command.id}` : `Unknown Fabric actor: ${command.id}`);
        const actor = candidates[0]!;
        if (command.dryRun === true) {
          response = { format: RESIDENT_HOST_FORMAT, requestId, ok: true, actor, operatorEvidence: evidence, completedAt: Date.now() };
        } else {
          // Re-read the uncached current lease immediately before each mutation.
          // This is a lease veto, not a proof that no Main exists or can restart.
          const pending = this.actors.stop(actor.id, id => { check(); commit(id); }, true);
          boundaryAdmitted?.();
          const stopped = await pending;
          response = command.action === "stop"
            ? { format: RESIDENT_HOST_FORMAT, requestId, ok: true, actor: stopped, completedAt: Date.now() }
            : await this.#removeResidentActor(actor.id, requestId, () => check());
        }
      } else if (command.operation === "actors") {
        response = {
          format: RESIDENT_HOST_FORMAT, requestId, ok: true,
          actors: this.actors.listOwned().filter((actor) => actor.rootId === this.config.rootId),
          completedAt: Date.now(),
        };
      } else if (command.operation !== "removeActor") {
        if (command.operation === "setInstructions" || command.operation === "setModel" ||
          command.operation === "setThinking" || command.operation === "setActivationFilter" || command.operation === "setTools" ||
          command.operation === "resetSession" || command.operation === "stop") {
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
          case "setInstructions": {
            const instructions = resolveActorInstructions(command, this.config.agents.instructionsRoot);
            assertActorInstructionReplacement(this.actors.instructions(actor.id), instructions, command.replace);
            updated = await this.actors.setInstructions(actor.id, instructions, commit);
            break;
          }
          // Repair is a boundary request, not terminal stop: the admitted run settles,
          // then queued deliveries resume on the fresh session under the same actor.
          case "resetSession": {
            const pending = this.actors.resetSession(actor.id, { beforeCommit: commit });
            boundaryAdmitted?.();
            updated = await pending;
            break;
          }
          case "stop": {
            const pending = this.actors.stop(actor.id, commit, true);
            boundaryAdmitted?.();
            updated = await pending;
            break;
          }
          case "setModel": updated = await this.actors.setModel(actor.id, command.model, command.scope, commit, command.modelReason); break;
          case "setThinking": updated = await this.actors.setThinking(actor.id, command.thinking, command.scope, commit); break;
          case "setActivationFilter": updated = await this.actors.setActivationFilter(actor.id, command.activationFilter, commit, command.expiresAt, command.reservation, command.observation, command.reservationToken); break;
          case "setTools": updated = await this.actors.setTools(actor.id, command.tools, commit); break;
          default: throw new Error("Unknown resident actor operation");
        }
        response = { format: RESIDENT_HOST_FORMAT, requestId, ok: true, actor: updated, completedAt: Date.now() };
      } else {
        response = await this.#removeResidentActor(command.id, requestId, commit);
      }
    } catch (error) {
      response = {
        format: RESIDENT_HOST_FORMAT,
        requestId,
        ok: false,
        error: errorMessage(error),
        ...(error instanceof ResidentActorAuthorizationError || error instanceof ResidentCommandUnsupportedError || error instanceof ResidentRequestExpiredError
          ? { errorCode: error.code } : {}),
        ...(error instanceof ActorSessionResetCancelledError ? { errorCode: error.code } : {}),
        ...(error instanceof FabricModelDeniedError ? {
          errorCode: error.code, modelDenied: { model: error.model, ...(error.replacement ? { replacement: error.replacement } : {}) },
        } : {}),
        completedAt: Date.now(),
      };
    }
    return response;
  }

  async #removeResidentActor(id: string, requestId: string, commit: (id: string) => void): Promise<ResidentCommandResponse> {
    const cleanup = this.actors.cleanupObligation(id);
    if (!this.actors.owns(id) || (cleanup && cleanup.residency !== "durable")) {
      throw new Error(`Resident host does not own ${id}`);
    }
    // smarty-dev#2184 item 8: stop now and return; the removal finishes behind its run.
    commit(id);
    const removed = await this.actors.remove(id, { wait: false });
    this.#writeRemovals();
    if (removed.pending) {
      void this.actors.removalSettled(id)?.finally(() => {
        this.#writeRemovals();
        this.participants.scheduleRefresh();
      });
    }
    return { format: RESIDENT_HOST_FORMAT, requestId, ok: true,
      ...(removed.pending ? { pending: removed.pending } : {}),
      ...(removed.cleaned !== undefined ? { cleaned: removed.cleaned } : {}), completedAt: Date.now() };
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
    // Windows has no flock helper. Retain main's exclusive-create host claim,
    // without PID-based stale unlinking or pretending it is a kernel fence.
    if (process.platform === "win32") { this.#acquireWindowsLock(); return; }
    // Only an inode we created, or one previously established by this protocol,
    // is safe to adopt. A dead legacy PID cannot exclude a reclaimer that already
    // committed to unlinking that inode. Empty/torn legacy startup records prove even less.
    // Serialize creation through provenance publication. A noncreator must
    // never acquire host.lock ahead of its creator and strand a fresh root.
    // This guard is immutable too; arbitrary empty legacy host.lock stays refused.
    let establishmentFd: number;
    try {
      establishmentFd = await lockFile(path.join(this.config.residencyRoot, "host-fence-establish.lock"), 0, process.platform === "linux");
    } catch (error) {
      if (error instanceof FileLockBusy) throw new ResidentHostAlreadyRunning("Fabric resident host startup claim is busy");
      throw error;
    }
    try { await this.#establishLockInode(); }
    finally { fs.closeSync(establishmentFd); }
  }

  /** Legacy Windows primitive: an atomic claim, not automatic crash recovery. */
  #acquireWindowsLock(): void {
    const existing = readJson<ResidentHostOwner>(this.#ownerPath);
    if (existing && residentProcessAlive(existing.pid, existing.processStartTime)) {
      throw new ResidentHostAlreadyRunning("Fabric resident host is already running");
    }
    try { this.#lockFd = fs.openSync(this.#lockPath, "wx", 0o600); }
    catch (error) {
      if ((error as NodeJS.ErrnoException).code === "EEXIST") {
        // Never unlink a stale/empty claim: a delayed starter or legacy
        // reclaimer may still own it. Windows recovery requires a verified drain.
        throw new ResidentHostAlreadyRunning("Fabric resident host is already running or has a legacy/uncertain startup record; verify drain before removing it");
      }
      throw error;
    }
    this.#exclusiveCreateLock = true;
    try {
      fs.writeFileSync(this.#lockFd, JSON.stringify({ token: this.#token, pid: process.pid, processStartTime: processStartTime(process.pid) }));
    } catch (error) { this.#releaseLock(); throw error; }
  }

  /** Called only while holding the immutable first-claim establishment guard. */
  async #establishLockInode(): Promise<void> {
    let created: fs.BigIntStats | undefined;
    try {
      const fd = fs.openSync(this.#lockPath, fs.constants.O_RDWR | fs.constants.O_CREAT |
        fs.constants.O_EXCL | fs.constants.O_NOFOLLOW, 0o600);
      try { created = fs.fstatSync(fd, { bigint: true }); } finally { fs.closeSync(fd); }
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
    }
    try { this.#lockFd = await lockFile(this.#lockPath, 0, process.platform === "linux"); }
    catch (error) {
      if (error instanceof FileLockBusy) throw new ResidentHostAlreadyRunning("Fabric resident host is already running");
      throw error; // Missing/broken flock fails closed, including explicit non-Linux starts.
    }
    try {
      const locked = fs.fstatSync(this.#lockFd, { bigint: true });
      const current = fs.lstatSync(this.#lockPath, { bigint: true });
      const sameInode = (stat: { dev: bigint; ino: bigint }): boolean =>
        stat.dev === locked.dev && stat.ino === locked.ino;
      if (!sameInode(current)) throw new Error("Fabric resident host startup inode was replaced; verify legacy drain");
      // Read before overwriting: an unknown birth identity is not permission to displace a live owner.
      const records = [readJson<ResidentHostOwner>(this.#lockPath), readJson<ResidentHostOwner>(this.#ownerPath)];
      if (records.some((owner) => owner && owner.pid !== process.pid && residentProcessAlive(owner.pid, owner.processStartTime))) {
        throw new ResidentHostAlreadyRunning("Fabric resident host is already running (legacy owner)");
      }
      const provenancePath = path.join(this.config.residencyRoot, "host-fence.json");
      const provenance = readJson<{ format: number; dev: string; ino: string }>(provenancePath);
      const established = provenance?.format === 1 && provenance.dev === String(locked.dev) && provenance.ino === String(locked.ino);
      if (!established) {
        if (!created || !sameInode(created) || fs.existsSync(provenancePath) || fs.existsSync(this.#ownerPath)) {
          throw new Error("Fabric resident host has a legacy or uncertain startup record; verify rollout/rollback drain before removing it");
        }
        // Bind provenance to the immutable flock inode, not the mutable PID diagnostic.
        // After a crash, corrupt diagnostic bytes must not disable kernel-fenced recovery.
        atomicWrite(provenancePath, { format: 1, dev: String(locked.dev), ino: String(locked.ino) });
      }
      fs.ftruncateSync(this.#lockFd, 0);
      fs.writeFileSync(this.#lockFd, JSON.stringify({ token: this.#token, pid: process.pid, processStartTime: processStartTime(process.pid) }));
    } catch (error) { this.#releaseLock(); throw error; }
  }

  #releaseLock(): void {
    if (this.#lockFd === undefined) return;
    // Remove our publication while holding the claim. POSIX never unlinks its
    // immutable kernel-fence inode; Windows removes only its token-owned claim.
    const owner = readJson<ResidentHostOwner>(this.#ownerPath);
    if (owner?.token === this.#token) fs.rmSync(this.#ownerPath, { force: true });
    const removeClaim = this.#exclusiveCreateLock && readJson<{ token?: string }>(this.#lockPath)?.token === this.#token;
    fs.closeSync(this.#lockFd);
    this.#lockFd = undefined;
    // Close before unlinking for Windows file-sharing semantics. Contenders
    // still cannot claim the existing path between close and this synchronous rm.
    if (removeClaim) fs.rmSync(this.#lockPath, { force: true });
  }
}

function residentHostLaunchContext(config: ResidentHostConfig): ResidentHostLaunchContext | undefined {
  const raw = process.env.PI_FABRIC_RESIDENT_LAUNCHER;
  if (!raw) return undefined;
  const launcher = JSON.parse(raw) as ResidentLauncherIdentity;
  if (launcher.pid !== process.ppid || !exactResidentProcess(launcher)) throw new Error("Resident launcher identity is uncertain");
  // host.ts and its built shared chunk both live one directory below dist.
  const loadedRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
  // A bundled Pi executable is not a generic JS runtime. Reconstruct with
  // the birth-validated parent launcher runtime, checked against its kernel exe.
  if (fs.realpathSync(`/proc/${launcher.pid}/exe`) !== launcher.runtime) {
    throw new Error("Resident launcher runtime identity is uncertain");
  }
  const attempt = process.env.PI_FABRIC_RESIDENT_ATTEMPT ? JSON.parse(process.env.PI_FABRIC_RESIDENT_ATTEMPT) as ResidentHostLaunchContext["attempt"] : undefined;
  let pinned: ResidentLaunchSpec | undefined;
  if (attempt) {
    const state = readHandoverJson<ResidentHandoverState>(handoverPath(config.residencyRoot));
    pinned = attempt.kind === "target" ? state?.plan.target : state?.plan.previous;
    if (!["target", "fallback"].includes(attempt.kind) || state?.plan.id !== attempt.id || !pinned || state.plan.launcher.token !== launcher.token) {
      throw new Error("Resident successor is not the launcher's owned attempt");
    }
  }
  // The successor's retained script runtime can differ from launcher A's.
  // Its identity is already bound to the immutable custody plan.
  const spec = residentLaunchSpec(config, path.join(loadedRoot, "dist/residency/pi-entry.js"), pinned?.runtime ?? launcher.runtime);
  if (spec.digest !== process.env.PI_FABRIC_RESIDENT_SPEC_DIGEST || (pinned && pinned.digest !== spec.digest)) {
    throw new Error("Resident loaded release does not match launcher snapshot");
  }
  return { launcher, spec, ...(attempt ? { attempt } : {}) };
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
  const host = new ResidentHost(config, () => finishIdle?.(), modelRegistry, residentHostLaunchContext(config));
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
        launcherPid: process.env.PI_FABRIC_RESIDENT_LAUNCHER ? process.ppid : undefined,
        launcherBirth: process.env.PI_FABRIC_RESIDENT_LAUNCHER ? processStartTime(process.ppid) : undefined,
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
