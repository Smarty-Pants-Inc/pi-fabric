#!/usr/bin/env node

import { fabricTurnProvenance, fabricWakeCause, copyFabricWakeCause, type FabricWakeCause, type FabricPrincipal } from "../fabric-provenance.js";
import { diagnostics } from "../core/diagnostics.js";
import { randomUUID } from "node:crypto";
import { detachedPreview } from "../core/preview.js";
import { resolveActorInstructions, assertActorInstructionReplacement } from "../actors/instructions-file.js";
import {
  RESIDENT_HANDOVER_ABI, HANDOVER_DRAIN_MS, exactResidentProcess, assertAutomaticReleaseRecovery,
  residentLaunchSpec, validateLaunchSpec, assertHandoverTopology, assertPreviousLaunchSpec,
  handoverPath, handoverCustodyPath, handoverOutcomePath, handoverActive,
  readHandoverJson, writeHandoverState, writeLaunchSnapshot, mainGenerationCurrent, decideHandover,
  writeResidentStagedReadyReceipt, assertResidentStagedReady, specDigest, type ResidentStagedReadyReceipt,
  type ResidentLaunchSpec, type ResidentLauncherIdentity, type ResidentHandoverPlan, type ResidentHandoverState,
} from "./handover.js";
import {
  RESIDENT_DRAIN_ABI, ResidentDrainRefusedError, newResidentDrainReceipt, residentDrainDeadline, sameDrainOwner, sameDrainLauncher,
  writeResidentDrainReceipt, readResidentDrainReceipt, type ResidentDrainReceipt,
} from "./drain.js";
import { failClosedContainment, ContainmentUnavailableError, type AttemptContainment, type AttemptLaunchReceipt } from "./attempt-containment-contract.js";
import { readResidentLaunchAttestation } from "./launch-attestation.js";

/** Internal checkpoint capability; absence refuses before holding admissions. */
interface ResidentDrainActors {
  pauseForRelease(options?: { strict?: boolean }): void;
  resumeAfterRelease(): void;
  checkpointForRelease(assertHeld?: () => void): Promise<void>;
  releaseCheckpointReceipt(): unknown;
  withReleaseAdmission<T>(admit: () => T): T;
}
interface ResidentDrainAgents {
  pauseForRelease(strict?: boolean): void;
  resumeAfterRelease(): void;
  checkpointForRelease(deadlineAt?: number, valid?: () => boolean): Promise<void>;
}
interface ResidentDrainIngress {
  pause(strict?: boolean): void;
  checkpointForRelease(valid?: () => boolean): Promise<void>;
}
interface ResidentHeldDrain {
  receipt: ResidentDrainReceipt;
  caller: ResidentActorCaller;
  checkpointAbort: AbortController;
  timer: NodeJS.Timeout;
  resumed: Promise<void>;
  resolveResumed(): void;
}

interface ResidentHostLaunchContext {
  launcher: ResidentLauncherIdentity;
  spec: ResidentLaunchSpec;
  /** Trusted launcher-channel launch receipt; never reconstructed by opening a scope path. */
  launchAttestation?: AttemptLaunchReceipt;
  attempt?: { id: string; kind: "target" | "fallback" };
}
import { lockFile, FileLockBusy } from "./file-lock.js";
import { assertNoWatchdogCustody } from "./watchdog-custody.js";
import { readResidentOperatorEvidence, assertResidentOperatorConfirmed, assertRootParticipantQuiet } from "./operator-safety.js";
import { closeWithActors } from "../actors/close-order.js";
import { residentProcessWorkPending, subscribeResidentProcessWork } from "./process-work.js";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
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
import { slotAdmissionFor } from "../actors/slot-admission.js";
import { ActorSlotRecoveryParkedError } from "../actors/slot-recovery.js";
import { wakeResidentActors, assertResidentWakeWatch, ensureResidentWakeArchive, readWakeJson, residentWakeRequestPath, residentSleepingPath, ResidentWakeWatchError, subscribeResidentWakeWatchErrors, type ResidentWakeRoutes } from "./wake.js";
import { DEFAULT_STUCK_PREPARING_MS, STUCK_PREPARING_CHECK_MS } from "../actors/stuck-preparing.js";
import { ActorRegistryStore } from "../actors/registry-store.js";
import { canonicalResidentWakeConfig, residentWakeConfigMatches, residentWakeCapacityAvailable, residentWakeIndexPath, RESIDENT_WAKE_INDEX_MAX_ROOTS, ResidentWakeRecoveryReader, type ResidentWakeRecoveryBatch } from "./wake-index.js";
import { ActorSessionResetCancelledError } from "../actors/session-reset-error.js";
import type { FabricActorInfo } from "../actors/types.js";
import type { AgentRunRecord } from "../agents/types.js";
import type { StateProjector } from "../mesh/state-projector.js";
import { AgentManager } from "../agents/manager.js";
import type { SpawnRouterAdmission } from "../agents/spawn-router.js";
import { FabricTargetTerminalError, terminalRejectionFields, type AgentTerminalNotice } from "../agents/terminal-target.js";
import { launchScope } from "../scope.js";
import { useBudgetLedger } from "../agents/budget-ledger.js";
import { LifecycleBroker } from "../lifecycle/broker.js";
import { lifecycleSourceIdentity, type FabricLifecycleEvent, type FabricLifecycleSubscription } from "../lifecycle/types.js";
import { meshStateBackendFromMarker } from "../mesh/backend-fence.js";
import { MeshStore, meshOutageAlarmSink, RUNTIME_MESH_READ_CACHE_MS, type MeshBatchView, type MeshIdentity } from "../mesh/store.js";
import { CommitOutbox, withStateFence } from "../mesh/commit-outbox.js";
import { MeshBackgroundQueue, MeshBackgroundRetry } from "../core/atomic-write.js";
import { isMeshWriteDeferred } from "../core/atomic-write.js";
import { FabricControlPlane, controlOwnerIncarnation, controlActorBindingOptions, type FabricControlAcceptance, type FabricControlCommand } from "../topology/control-plane.js";
import { MeshConsumptionPausedError, assertMeshConsumption } from "../topology/mesh-consumption.js";
import { ParticipantDirectory } from "../topology/participant-directory.js";
import type { FabricTransport } from "../transport/types.js";
import { hostLeasePath, readHostLeaseCurrent } from "../topology/host-leases.js";
import { RESIDENT_IDLE_EXIT_MS, FabricMeshTransportConfigError, normalizeFabricConfig, validateFabricNatsOperatorMachine } from "../config.js";
import { residentIdleExitPath, type ResidentIdleExit } from "./idle-exit.js";
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
  residentSlotErrorFields,
  residentCreateMayBeProtected,
  type ResidentDeliveryRecord,
  type ResidentHostConfig,
  type ResidentHostOwner,
} from "./protocol.js";
import { completionRecipientFromRun, saveCompletion } from "../agents/completion-journal.js";
import { projectOf } from "../topology/project-identity.js";
import { processStartTime, residentProcessAlive } from "./process-identity.js";
import { ensureResidentGenerations, recordResidentGenerationError, recordResidentGenerationExit } from "./generation-exit.js";
import { ResidentRequestRetention } from "./retention.js";
import { retentionV2Enabled } from "../storage/retention-platform.js";
import { ResidentLegacyRunArchive } from "./legacy-run-archive.js";
import { assertResidentRequestNotExpired, residentRequestGeneration, ResidentRequestExpiredError, RESIDENT_EXPIRING_COMMAND_FORMAT } from "./request-expiry.js";

// Moved to ./retention.js so the mesh-wide sweep (storage/retention-cli.ts) can run it under the
// dead host's fence without loading the host (smarty-dev#3252).
export { RESIDENT_RUN_RETENTION_MS, sweepResidentRuns } from "./retention.js";
// smarty-dev#11172: the requests/ directory watch is primary; this is only the base of
// the capped, coalesced backoff for a failed rename (pending failed work), never an idle poll.
const REQUEST_POLL_MS = 50;
// A failed (non-transient) request poll or delivery flush is retried at this deadline.
const REQUEST_FAILURE_RETRY_MS = 1_000;
// A running retention scan continues in bounded slices at the former maintenance cadence.
const MAINTENANCE_SLICE_MS = 100;
// A staged/handover release re-observes launcher liveness (no event) at most once a minute; the
// handover itself is bounded by its HANDOVER_DRAIN_MS deadline.
const RELEASE_RESAMPLE_MS = 60_000;
// ponytail: 30s only amortizes deliveries racing clean close; no worker/session is kept warm.
const IDLE_EXIT_MS = 30_000;
// Reuse active-actor observations on the existing request poll, not dormancy checks.
const ACTIVE_ACTOR_CACHE_MS = 1_000;
// Keep fleet-wide ownership/root observations off the 20 Hz request poll (smarty-dev#6729).
// Requests/admissions/agents remain exact on every tick; exit revalidates all observations.
// Actor and request activity resets the window even when it starts/ends between samples.
const IDLE_ACTOR_CHECK_MS = 1_000;
// A stat stamp of config.json proves it unchanged only once the file is older than the
// coarsest timestamp granularity a volume may have (FAT: 2 s). Until then a same-size
// replacement inside one timestamp tick can keep size and times, and its file id too where
// the volume reports ino 0 or reuses the freed inode; such a file is re-read on every tick.
// Any later replacement is then written after the cached mtime plus a tick, so its mtime
// differs (the racily-clean rule of git's index).
const CONFIG_STAMP_SETTLE_MS = 3_000;
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

// The resident host runs Pi with --no-extensions, so a custom runner is only
// registered here when its residentModule is imported (once per process).
const runnerModules = new Map<string, Promise<void>>();
const loadRunnerModule = (specifier: string): Promise<void> => {
  if (!/^(?:\/|[A-Za-z]:[\\/]|file:)/.test(specifier)) {
    return Promise.reject(new Error(`Fabric runner module must be absolute: ${specifier}`));
  }
  const url = specifier.startsWith("file:") ? specifier : pathToFileURL(specifier).href;
  let pending = runnerModules.get(url);
  if (!pending) {
    pending = import(url).then(() => undefined);
    runnerModules.set(url, pending);
    pending.catch(() => runnerModules.delete(url));
  }
  return pending;
};

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

/**
 * The resident host's idle rule. Live durable participants, queued work or a
 * pending request keep it alive; pending mesh schedules do too while at least
 * one durable participant could receive them, and `wakeAt` names the next due
 * time so the host releases it on time. Without durable participants a
 * schedule waits for the next Fabric process that touches the mesh.
 */
export const residentIdleDecision = (input: {
  now: number;
  idleSince: number;
  activeActor: boolean;
  activeAgent: boolean;
  pendingRequest: boolean;
  durableParticipants: boolean;
  nextScheduleDueAt: number | undefined;
  idleExitMs?: number;
}): { busy: boolean; exit: boolean; wakeAt?: number } => {
  const holdsSchedules = input.nextScheduleDueAt !== undefined && input.durableParticipants;
  const wakeAt = holdsSchedules ? input.nextScheduleDueAt : undefined;
  const busy = input.activeActor || input.activeAgent || input.pendingRequest || holdsSchedules;
  const exit = !busy && input.now - input.idleSince >= (input.idleExitMs ?? RESIDENT_IDLE_EXIT_MS);
  return { busy, exit, ...(wakeAt !== undefined ? { wakeAt } : {}) };
};

export class ResidentHost {
  readonly hostId: string;
  readonly identity: MeshIdentity;
  mesh!: MeshStore;
  participants!: ParticipantDirectory;
  control!: FabricControlPlane;
  #transport: FabricTransport | undefined;
  #transportAbort: AbortController | undefined;
  #controlInboxBound = false;
  agents!: AgentManager;
  actors!: ActorDirectory;
  lifecycle!: LifecycleBroker;
  #lockFd: number | undefined;
  #exclusiveCreateLock = false;
  readonly #ownerPath: string;
  #failure: unknown;
  /**
   * Last act just before this host releases owner.json (smarty-dev#7770).
   * `failure` is the startup error, if any. `owned()` revalidates the root's
   * fence and owner.json; call it immediately before a root write, and write
   * nothing there when it is false.
   */
  beforeRelease: ((failure: unknown, owned: () => boolean) => void) | undefined;
  readonly #lockPath: string;
  readonly #requestsPath: string;
  readonly #processingPath: string;
  readonly #responsesPath: string;
  readonly #agentsPath: string;
  readonly #removalsPath: string;
  readonly #deliveryOutboxPath: string;
  readonly #deliveryRetry = new MeshBackgroundRetry("resident completion/actor delivery")
    .alarmsTo(meshOutageAlarmSink(() => ({ mesh: this.mesh, from: this.identity })));
  // A delivery whose root is chosen on shared state is written after the commit that chose it
  // (smarty-dev#6477 R11); a crash in between leaves its row for the next flush to replay.
  #deliveryCommits!: CommitOutbox;
  #deliveryCommitsRecovered = false;
  #flushingDeliveries: Promise<unknown> | undefined;
  readonly #token = residentGenerationToken();
  /** The owner token this host publishes in owner.json: its generation (smarty-dev#7950). */
  get generation(): string { return this.#token; }
  #requestWatcher: fs.FSWatcher | undefined;
  #requestWatchRetried = false;
  #rootWatchRetried = false;
  #meshWatchRetried = false;
  #meshWatcher: fs.FSWatcher | undefined;
  /** Watchers this host closed itself; any other `close` is a native self-close (failure contract). */
  readonly #ownClosedWatchers = new WeakSet<fs.FSWatcher>();
  /** Outbox items a flush handed off; only measurable progress buys an immediate continuation. */
  #deliveryFlushProgress = 0;
  #releasedPlan: string | undefined;
  #watchFailed = false;
  #requestFailureStreak = 0;
  #requestRetryTimer: NodeJS.Timeout | undefined;
  #requestRetryAt: number | undefined;
  #requestKick = false;
  #requestRetry = false;
  #requestDrain: Promise<void> | undefined;
  #rootWatcher: fs.FSWatcher | undefined;
  #maintenanceTimer: NodeJS.Timeout | undefined;
  #maintenanceAt: number | undefined;
  #maintenanceSoon = false;
  #maintenanceArmed = false;
  #configSettleAt: number | undefined;
  #idleCheckQueued = false;
  #idleCheckRequested = false;
  #idleCheckPending: Promise<void> | undefined;
  #wakeWatchReprobeUsed = false;
  #wakeWatchErrorUnsubscribe: (() => Promise<void>) | undefined;
  #processWorkUnsubscribe: (() => void) | undefined;
  #wakeWatchNeedsReprobe = false;
  #stuckPreparingCheckedAt = 0;
  #stuckPreparingRetryAt: number | undefined;
  // The known root lapse (its host lease expiry / reload lease), a real dead-root sample deadline.
  #rootLapseAt: number | undefined;
  // The idle-window deadline already sampled once: a past deadline never re-arms at 0 ms.
  #idleDeadlineSampled: number | undefined;
  #idleBlockedOnCatchup = false;
  #legacyArchive: ResidentLegacyRunArchive | undefined;
  #scheduleTimer: NodeJS.Timeout | undefined;
  #scheduleWakeAt: number | undefined;
  #pollingRequests = false;
  // Boundary commands retain response custody without occupying serial admission.
  readonly #boundaryRequests = new Map<string, Promise<void>>();
  // Host-local only: retain pending promises and at most 256 completed creates for 10 minutes.
  readonly #creations = new Map<string, { result: Promise<ResidentCommandResponse>; completedAt?: number }>();
  #closed = false;
  #closePending: Promise<void> | undefined;
  #routeOwner?: ShadowRouteOwner;
  readonly #backgroundRequests = new MeshBackgroundRetry("resident request poll")
    .alarmsTo(meshOutageAlarmSink(() => ({ mesh: this.mesh, from: this.identity })));
  readonly #backgroundDeliveries = new MeshBackgroundQueue("resident completion/actor delivery");
  #started = false;
  #ready = false;
  #idleSince: number | undefined;
  #idleExitRequested = false;
  #residentIdleExitMs = RESIDENT_IDLE_EXIT_MS;
  #idleObservation = { at: Number.NEGATIVE_INFINITY, blocked: true, wakeable: false };
  #idleWakeable: boolean | undefined;
  #sleeping = false;
  #wakeRoutesJson: string | undefined;
  #wakeRecovery: ResidentWakeRecoveryReader | undefined;
  #wakeRecoveryPending: Promise<void> | undefined;
  #wakeRecoveryIgnored = 0;
  #wakeCapacityWarned = false;
  #wakeWatchSupported: boolean | undefined;
  #activeActor = { at: Number.NEGATIVE_INFINITY, active: true };
  // Accepted policy overlay of config.json, keyed by file identity (smarty-dev#6729).
  #retentionOverlay: { stamp: string; retention: ResidentHostConfig["retention"]; idleExitMs: number } | undefined;
  #admissions = 0;
  #businessAdmissions = 0;
  #drain: ResidentHeldDrain | undefined;
  #drainCheckpointPending: Promise<void> | undefined;
  readonly #checkpointJoins = new Set<Promise<void>>();
  #stagedReady: ResidentStagedReadyReceipt | undefined;
  #trustedLaunchAttestation: AttemptLaunchReceipt | undefined;
  #containmentClient: (AttemptContainment & { dispose(): void }) | undefined;
  readonly #requestRetention: ResidentRequestRetention;
  #handover: ResidentHandoverPlan | undefined;
  #advancingRelease = false;
  #staged = false;
  #publicationFailed = false;
  #reloadEvent: Promise<unknown> | undefined;
  #stateProjector: Promise<StateProjector | undefined> | undefined;
  readonly #publications = new Set<Promise<unknown>>();
  #effectiveConfig: (() => ResidentHostConfig) | undefined;
  readonly #retention: ResidentHostConfig["retention"] & { retainRuns: boolean };

  constructor(
    readonly config: ResidentHostConfig,
    readonly onIdle: (reason?: ResidentExitReason) => void = () => {},
    private readonly modelRegistry?: PiModelRegistryView,
    readonly launch?: ResidentHostLaunchContext,
    private readonly processWorkSessionId = config.sessionId,
    /** Launcher-custody channel only: read-only membership seam, never scope-path reopening or launch/exit. */
    readonly containment: AttemptContainment = failClosedContainment,
  ) {
    // NATS owns its snapshot; ordinary FILE constructor callers retain their behavior.
    if (config.mesh.transport === "nats") this.config = structuredClone(config);
    this.#staged = !!launch?.attempt;
    this.#residentIdleExitMs = config.mesh.residentIdleExitMs ?? RESIDENT_IDLE_EXIT_MS;
    this.hostId = residentHostId(config.rootId);
    this.identity = { id: this.hostId, name: "Fabric resident host", kind: "agent" };
    this.#ownerPath = path.join(config.residencyRoot, "owner.json");
    this.#lockPath = path.join(config.residencyRoot, "host.lock");
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
      }, {
        // A checked exit clears exchange debt, not the manager's run-directory
        // custody. Deleting a still-managed tree destroys the next fresh exit
        // proof and can pin a deferred stopped-actor exchange indefinitely.
        run: id => this.agents.hasRunCustody(id),
        reference: id => this.agents.retentionCustodyVeto(id),
      });
  }

  #createMesh(): MeshStore {
    const { config } = this;
    // Follow moved-marker/meta authority even after a stale backend config re-apply.
    const stateBackend = meshStateBackendFromMarker(config.meshRoot, config.mesh.stateBackend);
    if (stateBackend === "sqlite" && stateBackend !== config.mesh.stateBackend) {
      diagnostics.warn("src/residency/host.ts", `[pi-fabric] resident mesh ${config.meshRoot}: SQLite moved marker overrides mesh.stateBackend=${config.mesh.stateBackend}; opening sqlite`);
    }
    return new MeshStore(config.meshRoot, config.mesh.maxEventBytes, config.mesh.maxReadEvents,
      { backgroundReadCacheMs: config.mesh.idleReadCoalesceMs ?? RUNTIME_MESH_READ_CACHE_MS, lockProtocol: config.mesh.lockProtocol,
        stateBackend,
        canWakeResidents: () => this.#ready && !this.#closed && !this.#staged && !this.#handover && !this.#drain });
  }

  async #prepareNatsTransport(): Promise<string> {
    // Direct ResidentHost callers do not necessarily pass through config normalization.
    if (this.config.mesh.hostTransport !== "nats") throw new FabricMeshTransportConfigError("nats", this.config.mesh.hostTransport ?? "file");
    const normalized = normalizeFabricConfig({ mesh: this.config.mesh }).mesh;
    // Environment resolution must never turn this NATS activation into FILE policy.
    if (normalized.transport !== "nats") throw new FabricMeshTransportConfigError("nats", normalized.transport);
    // Only messaging settings are normalized; retain every other raw host mesh setting.
    this.config.mesh.nats = normalized.nats;
    validateFabricNatsOperatorMachine(this.config.mesh.nats.operatorMachine, this.config.mesh.nats.machine);
    this.mesh = this.#createMesh();
    const { resolveAgentDir } = await import("../core/agent-dir.js");
    const transportStateDir = path.resolve(path.join(resolveAgentDir(), "fabric"));
    const relative = path.relative(path.resolve(this.mesh.root), transportStateDir);
    if (relative === "" || (relative !== ".." && !relative.startsWith(`..${path.sep}`) && !path.isAbsolute(relative))) {
      throw new Error("Fabric transport local state directory must be outside the shared mesh root");
    }
    const { mkdirPrivate } = await import("../storage/private-dir.js");
    mkdirPrivate(transportStateDir);
    const transportAbort = new AbortController();
    this.#transportAbort = transportAbort;
    const { openFabricTransport } = await import("../transport/index.js");
    // NatsTransport.open awaits ensureTransportConsumer before returning (inbox.ts).
    const opened = await openFabricTransport({ config: structuredClone(this.config.mesh), hostId: this.hostId, mesh: this.mesh, signal: transportAbort.signal });
    // A close while open was pending must not publish a late carrier.
    if (transportAbort.signal.aborted) {
      await opened.close();
      transportAbort.signal.throwIfAborted();
    }
    this.#transport = opened;
    if (this.#transport.kind !== "nats" || this.#transport.self.hostId !== this.hostId) {
      throw new Error("Fabric NATS transport factory returned a different carrier or owner");
    }
    // Recheck against the actual factory/environment-resolved machine before use.
    validateFabricNatsOperatorMachine(this.config.mesh.nats.operatorMachine, this.#transport.self.machine);
    return transportStateDir;
  }

  #initialize(transportStateDir?: string): void | Promise<void> {
    const { config, modelRegistry } = this;
    // Do not make this method async: FILE setup throws must reach start()'s catch
    // synchronously, and protected recovery must enter in the constructor tick.
    if (!this.#transport) this.mesh = this.#createMesh();
    const transport = this.#transport;
    const firstSeenAgents = new Map<string, number>();
    this.#deliveryCommits = new CommitOutbox(this.mesh, `residency/${this.hostId}/deliveries`, this.identity, {
      delivery: (record: ResidentDeliveryRecord, view, replay) => this.#writeDelivery(record, view, replay),
    });
    // Global order remains registry -> mesh, with the #535 50 ms mesh try.
    // Prepare actor/presence observations BEFORE acquisition, then validate exact
    // atomic registry generations under custody and retain custody through publication.
    // An invalid preparation is discarded and re-selected outside every fence.
    // Independent liveness may only renew existing keys with matching lineage tokens;
    // it never claims, creates, removes, or certifies a shared heartbeat.
    const registryRoots = Object.values(residentActorRoots(config));
    const registries = registryRoots.map((root) => new ActorRegistryStore(root));
    // smarty-dev#8526: lock only the registries holding the rows of the actors this round writes.
    // The project registry is ONE lock shared by every resident host of the project; a round that
    // publishes none of its rows no longer serializes behind (or blocks) other hosts on it.
    // An exact empty set proves no actor effects and needs no registry. Any id held by no
    // registry, or a round without an exact actor set, still locks every registry.
    // validPublication still checks every registry's generation before committing.
    const publishFenced = <T>(publish: () => Promise<T>, actorIds?: ReadonlySet<string>): Promise<T> => {
      let scoped: ActorRegistryStore[] | undefined = actorIds?.size === 0 ? [] : undefined;
      if (actorIds && actorIds.size > 0) {
        // ponytail: an unreadable snapshot only loses the scoping; the full fence (and its own reads) decide.
        try {
          const holders = [...actorIds].map(id => knownRegistries.find(known => current(known).byId.has(id))?.store);
          if (holders.every(store => store !== undefined)) scoped = registries.filter(store => holders.includes(store));
        } catch { scoped = undefined; }
      }
      return ActorRegistryStore.withLocks(scoped ?? registries, () => this.mesh.withTryLock(publish, 50), "presence-fence");
    };
    const knownRegistries = registries.map((store, index) => ({ root: registryRoots[index]!, store, snapshot: store.snapshot(), byId: new Map(store.snapshot().actors.map(row => [row.id, row])) }));
    const current = (known: typeof knownRegistries[number]): typeof knownRegistries[number] => {
      const snapshot = known.store.snapshot(); // cached last-known view unless the atomic generation moved
      if (snapshot !== known.snapshot) {
        known.snapshot = snapshot;
        known.byId = new Map(snapshot.actors.map(row => [row.id, row]));
      }
      return known;
    };
    const actorRenewalAllowed = (record: import("../topology/types.js").FabricParticipantRecord): boolean => {
      for (const known of knownRegistries) {
        const row = current(known).byId.get(record.id);
        if (row) {
          const topics = Array.isArray(row.topics) ? row.topics.filter((topic): topic is string => typeof topic === "string") : [];
          const slots = slotAdmissionFor(this.mesh);
          if (slots.requiresEnforcement(topics, known.root, config.rootId) &&
              (!this.actors.slotRecoveryStatus().ready || !slots.isCurrent(record.id, topics, known.root))) return false;
          return row.rootId === config.rootId && (row.residency ?? "session") === "durable" &&
            record.actorOwnershipToken === JSON.stringify([row.rootId, row.adoptedAt ?? null, row.adoptedFrom ?? []]);
        }
      }
      return false;
    };
    this.participants = new ParticipantDirectory(this.mesh, {
      ...(transport ? { transport } : {}),
      ...(transportStateDir ? { transportStateDir } : {}),
      enabled: true,
      actorRenewalAllowed,
      preparePublicationFence: () => {
        const generations = registries.map(store => store.fingerprint());
        return () => registries.every((store, index) => store.fingerprint() === generations[index]);
      },
      // Legacy list observations may lag; authority snapshots explicitly request fresh.
      listReadCacheMs: config.mesh.idleReadCoalesceMs ?? RUNTIME_MESH_READ_CACHE_MS,
      withPublicationFence: publishFenced,
      // Acquire/release only: never carry a selected snapshot or mesh custody into
      // registry acquisition. FIFO waiting gets us into periodic free windows. A state
      // fence, so it waits on whichever lock the state backend commits under (R3).
      waitForPublicationRetry: () => withStateFence(this.mesh, this.identity, () => undefined),
      publicationBatch: full => this.actors.presenceBatch(full),
      hostId: this.hostId,
      rootId: config.rootId,
      identity: this.identity,
      reapDeadHosts: false,                                    // its session's runtime sweeps
      presencePass: async () => {
        await rootPresenceAlarms(this.mesh, this.identity, this.hostId,
          this.participants.list({ scope: "project", includeStale: true, fresh: true }), config.mesh.rootPresenceAlarmMs);
        await this.actors?.reconcileSessionOrphans();
        // smarty-dev#11172: root liveness has no event; the existing presence pass re-samples it.
        this.#sampleIdle();
      },
    });
    const participants = this.participants;
    const initializeNative = (): void => {
    const participantView = transport ? participants.participantView : undefined;
    if (transport && !participantView) throw new Error("Fabric NATS participant view unavailable");
    this.control = new FabricControlPlane(this.mesh, this.identity, {
      ...(transport && participantView ? { transport, participants: participantView } : {}),
      ownerIncarnation: participants.ownerIncarnation,
      controlIncarnationFence: transport ? "enforce" : config.mesh.controlIncarnationFence,
      enabled: true,
      hostId: this.hostId,
      pollMs: config.mesh.actorPollMs,
      bridgeTimeoutMs: config.mesh.bridgeControlTimeoutMs,
      ...(transport ? { inboxLanes: config.mesh.nats.inboxLanes ?? 8,
        relayOperatorMachine: config.mesh.nats.operatorMachine } : {}),
      ...(config.mesh.nats.machine !== undefined
        ? { expectedNatsInboxStream: `FABRIC_IN_${config.mesh.nats.machine}` } : {}),
      ...(config.mesh.nats.rollbackHoldUntil !== undefined
        ? { withdrawHoldUntil: Date.parse(config.mesh.nats.rollbackHoldUntil) } : {}),
      canConsumeMesh: () => this.#ready && (transport ? participants : this.participants).canConsumeMesh(),
      captureOwnerLease: (ownerHostId, ownerIdentityId, targetId) =>
        (transport ? participants : this.participants).captureControlOwnerLease(ownerHostId, ownerIdentityId, targetId),
      readMirroredOwner: (ownerHostId, ownerIdentityId, targetId) =>
        (transport ? participants : this.participants).mirroredControlOwner(ownerHostId, ownerIdentityId, targetId),
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
        ...(overlay.retention ? { retention: overlay.retention } : {}),
        mesh: { ...config.mesh, residentIdleExitMs: overlay.mesh?.residentIdleExitMs ?? RESIDENT_IDLE_EXIT_MS } };
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
      admissionSnapshotPath: path.join(residentActorRoots(config).session, "admission.json"),
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
      ...(transport ? {
        meshTransport: structuredClone({ transport: config.mesh.transport,
          ...(config.mesh.hostTransport ? { hostTransport: config.mesh.hostTransport } : {}), nats: config.mesh.nats }),
        taskPresence: {
          publishBeforeLaunch: async (run: AgentRunRecord) => {
            if (!this.#controlInboxBound || this.#closed || !participants.canConsumeMesh()) {
              throw new Error("Fabric NATS task admission requires the bound inbox and live owner lease");
            }
            // Actor activations retain actor-native publication, never a synthetic task row.
            for (const record of agentParticipantRecords([run], config.rootId, this.hostId, this.identity.id, config.rootId, firstSeenAgents)) {
              await participants.publishBeforeLaunch({ ...record, ownerIncarnation: participants.ownerIncarnation });
            }
          },
          withdraw: (id: string) => participants.withdrawBeforeLaunch(id),
        },
      } : {}),
      // Launches on Main's behalf: Main's published name, not this host's ("main" is only the unnamed default).
      spawnerName: () => { const name = currentConfig().mainName ?? config.mainName; return name && name !== "main" ? name : undefined; },
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
      onTerminalNotice: notice => this.#deliverTerminalNotice(notice),
      onSettled: (result) => {
        this.#requestRetention.markDirty();
        this.#scheduleIdleCheck();
        this.#noteReleaseProgress();
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
        const summary = detachedPreview(result.text || result.error || "no result", COMPLETION_MAX_CHARS);
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
      const participant = (transport ? participants : this.participants).get(id, undefined, { fresh });
      return participant ? participant.ownerHostId === this.hostId : undefined;
    };
    // Polls may reuse validated ownership inputs; authority decisions still read exactly.
    const snapshotActorOwnership = (fresh = true, actorIds?: readonly string[]): ReadonlyMap<string, boolean> =>
      (transport ? participants : this.participants).actorOwnership(this.hostId, { fresh, ...(fresh ? {} : { actorIds }) });
    const lineageAlive = (rootId: string): boolean =>
      (transport ? participants : this.participants).lineageAlive(rootId);
    const actorRoots = residentActorRoots(config);
    // Host-only agents.wakeText (smarty-dev#6144), read at every activation from the accepted config.json
    // snapshot that Main rewrites on a live reload, so enabling applies and removal revokes at once. Fail
    // closed: an unreadable, invalid or other-generation snapshot (currentConfig() fell back to the startup
    // config) gives no text, never the startup policy. ActorManager revalidates whatever this returns.
    const currentWakeText = (): ResidentHostConfig["agents"]["wakeText"] => {
      const overlay = currentConfig();
      return overlay === config ? undefined : overlay.agents?.wakeText;
    };
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
          // Alarms retain the actor's display label, but their producer is this host.
          fabricWakeCause(message.source === "fabric-host" ? this.identity : { id: actor.id, name: actor.name, kind: "actor" },
            message.source === "fabric-host" ? "host-event" : "actor",
            message.source.startsWith("mesh:") || message.source.startsWith("host:") ? message.source.slice(5) : undefined, message.id),
        ));
      },
      {
        // Restoration must not launch queued work until owner and readiness publication commit.
        releasePaused: true,
        canConsumeMesh: () => this.#ready && (transport ? participants : this.participants).canConsumeMesh(),
        presencePublisher: { refresh: async () => {
          try {
            await (transport ? participants : this.participants).refreshPresence();
          } finally {
            try {
              this.#scheduleIdleCheck();
            } catch (error) {
              // Scheduling must not replace the original presence rejection.
              diagnostics.warn("src/residency/host.ts", `[pi-fabric] resident idle check scheduling failed: ${String(error)}`);
            }
          }
        }, schedule: () => (transport ? participants : this.participants).scheduleRefresh() },
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
        // smarty-dev#6509: the transient activation block cooldown and backoff cap.
        ...(typeof config.actors?.transientBlockCooldownMs === "number" ? { transientBlockCooldownMs: config.actors.transientBlockCooldownMs } : {}),
        ...(typeof config.actors?.transientBlockMaxMs === "number" ? { transientBlockMaxMs: config.actors.transientBlockMaxMs } : {}),
        wakeText: currentWakeText,
        // smarty-dev#8090: an operator removal's tree goes only after this check, run under the root's Main
        // publication fence (held by the caller), the state transaction and the root participant key lock.
        beforeOperatorCleanup: async (rootId) => {
          if (rootId !== config.rootId) throw new Error(`Operator cleanup for foreign root ${rootId}; tree kept`);
          const { fencedOperatorCheck } = await import("../actors/remove-offline.js");
          await fencedOperatorCheck(this.mesh, this.identity as { id: string; name: string; kind: "agent" }, rootId, get => {
            // pi-fabric#1031: the same Main-identity verdict as the offline removal. A stale record needs its Main
            // pid+start identity independently gone and no live lease file; no record or no identity is unknown.
            try {
              assertResidentOperatorConfirmed(readResidentOperatorEvidence(config, { get }, undefined, { mainStopped: true }),
                rootId, false, true);
              assertRootParticipantQuiet(config, { get });
            } catch (error) {
              if (!(error instanceof ResidentActorAuthorizationError)) throw error;
              throw new ResidentActorAuthorizationError(`${error.message}; the actor tree is kept: stop the Main, or let a host start finish it`);
            }
          });
        },
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
        canConsumeMesh: () => this.#ready && (transport ? participants : this.participants).canConsumeMesh(),
      },
      (subscription, event) => this.#deliverLifecycle(subscription, event),
    );
    };
    if (transport) {
      // Await the lease and both gap-safe watches BEFORE any ActorManager snapshot.
      return participants.start().then(() => {
        if (!participants.canConsumeMesh()) throw new Error("Fabric NATS directory did not acquire a live owner lease and view");
        initializeNative();
      });
    }
    initializeNative();
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
      if (this.launch?.launchAttestation) {
        try { this.#trustedLaunchAttestation = readResidentLaunchAttestation(this.config.residencyRoot, this.launch.launchAttestation); }
        catch (error) { if (!(error instanceof ContainmentUnavailableError)) throw error; }
      }
      if (this.#trustedLaunchAttestation && this.launch && process.connected && this.containment === failClosedContainment) {
        const birth = processStartTime(process.pid);
        if (!birth || this.launch.launcher.pid !== process.ppid || !exactResidentProcess(this.launch.launcher)) {
          throw new Error("Resident containment bootstrap parent/child birth is uncertain");
        }
        const { createLauncherContainmentClient } = await import("./launcher-containment-control.js");
        this.#containmentClient = createLauncherContainmentClient({ channel: process,
          binding: { launchAttestation: this.#trustedLaunchAttestation, launcher: this.launch.launcher,
            child: { pid: process.pid, processStartTime: birth }, generation: this.#token } });
      }
      if (this.config.mesh.transport === "nats") {
        const transportStateDir = await this.#prepareNatsTransport();
        await this.#initialize(transportStateDir);
      } else this.#initialize();
      // Both actor scopes must reconcile durable intents before any actor source,
      // control ingress, readiness receipt, or release resume can become visible.
      try { await this.actors.recoverProtectedActors(); }
      catch (error) {
        if (!(error instanceof ActorSlotRecoveryParkedError)) throw error;
        diagnostics.warn("src/residency/host.ts", `[pi-fabric] ${this.actors.slotRecoveryStatus().error?.message}`);
      }
      if (this.#staged) { this.#drainActors().pauseForRelease({ strict: true }); this.#drainAgents().pauseForRelease(true); }
      fs.mkdirSync(this.#requestsPath, { recursive: true, mode: 0o700 });
      fs.mkdirSync(this.#processingPath, { recursive: true, mode: 0o700 });
      fs.mkdirSync(this.#responsesPath, { recursive: true, mode: 0o700 });
      fs.mkdirSync(this.#agentsPath, { recursive: true, mode: 0o700 });
      this.#recoverInterruptedRequests();
      // Reattachment is business admission: AgentManager deliberately waits on
      // its hold even for terminal saved entries. A staged start must instead
      // prove the retained trees quiet through the real checkpoint below, not
      // await recovery whose gate only opens after that receipt is committed.
      // Live/unknown hosted trees still veto staged readiness in that checkpoint.
      if (!this.#staged && !this.#transport) await this.#recoverHostedRuns();
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
        this.actors.listOwned(true).map((actor) =>
          actorParticipantRecord(
            actor,
            this.config.rootId,
            this.hostId,
            this.identity.id,
            this.config.rootId,
          ),
        ),
      );
      this.agents.subscribeUi(() => { this.participants.scheduleRefresh(); this.#scheduleIdleCheck(); this.#noteReleaseProgress(); });
      this.actors.subscribe(() => this.participants.scheduleRefresh());
      this.actors.subscribe(() => this.#noteActorActivity());
      // smarty-dev#11172: an elapsed idle window held only by mesh catch-up finishes when the
      // monitor's read completes and it is caught up; no idle poll or timer.
      this.actors.subscribeMeshPoll(() => {
        if (!this.#idleBlockedOnCatchup || this.#closed || !this.actors.meshCaughtUp()) return;
        this.#idleBlockedOnCatchup = false;
        this.#sampleIdle();
      });
      this.#processWorkUnsubscribe = subscribeResidentProcessWork(this.processWorkSessionId, () => this.#noteActorActivity());
      this.#wakeWatchErrorUnsubscribe = subscribeResidentWakeWatchErrors(this.config.residencyRoot, () => {
        if (this.#wakeWatchSupported !== true) return; // probe-time errors settle through the promise
        this.#wakeWatchSupported = undefined;
        this.#wakeWatchReprobeUsed = true;
        this.#wakeWatchNeedsReprobe = true;
        this.#scheduleIdleCheck();
      });
      if (!this.#transport) this.control.start((command, from, signal, verification) =>
        this.#acceptControl(command, from, signal, verification));
      if (this.#staged) (this.control as unknown as ResidentDrainIngress).pause(true);
      if (this.#transport) {
        // Managers, native actor recovery, and sources exist before publication.
        if (!this.participants.canConsumeMesh()) throw new Error("Fabric NATS owner lease retired before publication");
        await this.participants.refreshPresence();
        if (!this.participants.canConsumeMesh()) throw new Error("Fabric NATS owner lease retired during publication");
        // Recover native custody while ingress is closed; publish recovered rows too.
        if (!this.#staged) {
          await this.#recoverHostedRuns();
          await this.participants.refreshPresence();
          if (!this.participants.canConsumeMesh()) throw new Error("Fabric NATS owner lease retired during native recovery");
        }
        const control = this.control;
        const leaseSignal = this.participants.transportLeaseSignal;
        if (!leaseSignal) throw new Error("Fabric NATS lease signal unavailable");
        const retireInbox = (): void => {
          this.#controlInboxBound = false;
          control.pause(leaseSignal.reason);
        };
        // Fence loss BEFORE consumers; only a new acquisition may reopen the inbox.
        leaseSignal.addEventListener("abort", retireInbox, { once: true });
        if (leaseSignal.aborted) { retireInbox(); leaseSignal.throwIfAborted(); }
        // First binding waits for #ready below. Starting while its consumption
        // gate is closed permanently pauses a NATS control plane.
      } else {
        // A failed join watch leaves no heartbeat or retry driver: fail startup closed.
        await this.participants.start().catch(error => { if (!this.participants.heartbeatArmed) throw error; });
      }
      // A publication failure is not readiness. Keep this same start pending,
      // with requests/events untouched, until a real locked renewal confirms it.
      while (!this.participants.canConsumeMesh()) {
        if (this.#closed) throw new Error(HOST_CLOSING_RETRY);
        if (this.#transport) throw new Error("Fabric NATS owner lease retired during startup");
        await delay(20);
      }
      this.lifecycle.start();
      if (this.#staged) {
        (this.lifecycle as unknown as ResidentDrainIngress).pause(true);
        await this.#probeWorkerStartup();
      }
      // Registered before the readiness drain below, so a request written meanwhile is not lost.
      // Both fail closed here (smarty-dev#11172): no watch, no readiness, and no polling fallback.
      this.#watchRequests(true);
      this.#watchRoot(true);
      this.#watchMesh(true);
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
          ...(this.#drainSupported() ? { drain: { abi: RESIDENT_DRAIN_ABI } } : {}),
          ...(this.#trustedLaunchAttestation ? { launchAttestation: this.#trustedLaunchAttestation } : {}),
          ...(this.launch.attempt ? { attempt: this.launch.attempt } : {}) } : {}),
      };
      // Where this generation's exit record goes (smarty-dev#7950), made while this host owns the root.
      ensureResidentGenerations(this.config.residencyRoot);
      atomicWrite(this.#ownerPath, owner);
      // A publisher can die after commit but before writing any root-local wake request.
      // Before readiness, stream at most 256 entries / 4 MiB. Own-root recovery
      // cannot launch another host; restored actor queues remain paused here.
      // Presence publications above do not perform an unscoped pending drain.
      if (!this.#staged && (fs.existsSync(residentWakeIndexPath(this.mesh.root)) || fs.existsSync(path.join(this.mesh.root, "wake-overflow")))) {
        try {
          this.#wakeRecovery = await this.mesh.exclusive(() => new ResidentWakeRecoveryReader(this.mesh.root));
          await this.#recoverWakeBatch(this.#wakeRecovery.next());
        } catch (error) {
          this.#wakeRecovery?.close(); this.#wakeRecovery = undefined;
          diagnostics.warn("src/residency/host.ts", `[pi-fabric] resident startup wake recovery deferred: ${String(error)}`);
        }
      }
      // The originating client may cancel this owned attempt until it sees the
      // required receipt. Commit it BEFORE opening any business gate or resuming
      // restored queues: publication failure/timeout must remain a non-serving
      // start, not shutdown of work that may already have escaped the attempt.
      if (this.#staged) await this.#publishStagedReady(owner);
      if (this.#transport && !this.participants.canConsumeMesh()) throw new Error("Fabric NATS owner lease retired before readiness");
      atomicWrite(path.join(this.config.residencyRoot, "maintenance-ready.json"), { token: this.#token, readyAt: now });
      // No fallible/awaited startup work remains. Accepted backlog is untouched
      // on failure; maintenance/collection stays on normal post-readiness ticks.
      this.#ready = true;
      if (this.#transport) {
        const leaseSignal = this.participants.transportLeaseSignal!;
        leaseSignal.throwIfAborted();
        if (!this.participants.canConsumeMesh()) throw new Error("Fabric NATS owner lease retired before inbox binding");
        // One initial start, not a resume after lease loss. The loss listener was
        // installed before readiness; a staged host retains its existing pause.
        this.control.start((command, from, signal, verification) => this.#acceptControl(command, from, signal, verification));
        this.#controlInboxBound = !leaseSignal.aborted;
        leaseSignal.throwIfAborted();
      }
      // The initial bounded drain (smarty-dev#11172): requests written before readiness.
      this.#kickRequests();
      fs.rmSync(residentSleepingPath(this.config.residencyRoot), { force: true });
      this.#scheduleIdleCheck();
      this.#writeWakeRoutes();
      if (this.#wakeRecovery) {
        this.#wakeRecoveryPending = this.#continueWakeRecovery().finally(() => {
          this.#wakeRecoveryPending = undefined;
          this.#scheduleIdleCheck();
        });
      }
      this.#startStateProjector();
      // Retention is not part of request admission/heartbeat/claim. A bounded
      // preparation cursor progresses even between request-retention samples.
      if (retentionV2Enabled()) {
        this.#legacyArchive = new ResidentLegacyRunArchive(this.config.residencyRoot, this.#retention, {
          actorRoots: [...new Set(Object.values(residentActorRoots(this.config)))],
          isRetained: id => this.#closed || !!this.#handover || !!this.#drain || !this.participants.canConsumeMesh() || this.agents.hasRunCustody(id),
        });
        this.#legacyArchive.start();
      }
      // smarty-dev#11172: one deadline timer at the nearest real maintenance deadline (both
      // retention modes); config.json replacements arrive as root directory events.
      this.#maintenanceArmed = true;
      this.#scheduleMaintenance();
      // Removals a previous host accepted: their runs ended with it.
      if (!this.#staged) {
        this.actors.resumeAfterRelease();
        void this.#backgroundDeliveries.enqueue(async () => {
          await this.actors.finishPendingRemovals();
          this.#writeRemovals();
        });
        void this.#retryDeliveries();
      }
      // The spawned process, not a watched file, owns the startup outcome.
      if (process.connected && process.send) process.send({ event: "resident-ready",
        root: this.config.residencyRoot, token: this.#token }, () => {});
    } catch (error) {
      this.#failure ??= error;
      await this.close();
      throw error;
    }
  }

  async #recoverWakeBatch(batch: ResidentWakeRecoveryBatch): Promise<void> {
    if (this.#drain || this.#staged) throw new Error(HOST_CLOSING_RETRY);
    this.#admissions++;
    try {
      const recovery = { config: this.config, ownedActors: this.actors.listOwned(true), batch, ignored: 0 };
      await wakeResidentActors(this.mesh, [], undefined, recovery);
      this.#wakeRecoveryIgnored += recovery.ignored;
    } finally { this.#admissions--; this.#scheduleIdleCheck(); this.#noteReleaseProgress(); }
  }

  async #continueWakeRecovery(): Promise<void> {
    const reader = this.#wakeRecovery!;
    let deferred = 0;
    try {
      while (!reader.done && !this.#closed) {
        // Yield AFTER the readiness receipt and between every bounded batch.
        await new Promise<void>(resolve => setImmediate(resolve));
        if (this.#closed) break;
        if (this.#drain) await this.#drain.resumed;
        if (this.#closed || this.#staged) break;
        const batch = reader.next();
        deferred += batch.entries;
        await this.#recoverWakeBatch(batch);
      }
    } catch (error) {
      diagnostics.warn("src/residency/host.ts", `[pi-fabric] resident startup wake recovery deferred: ${String(error)}`);
    } finally {
      reader.close(); this.#wakeRecovery = undefined;
      diagnostics.warn("src/residency/host.ts", `[pi-fabric] resident startup wake recovery: ${deferred} deferred entries processed after readiness; ${this.#wakeRecoveryIgnored} ignored (foreign, unmatched or not pending)`);
    }
  }

  /**
   * smarty-dev#6477 W1: with mesh.stateBackend=shadow (the effective backend, after the env override
   * and the R16/R19 fallback) this host runs the L3 projector for its mesh root. The lease elects one
   * active projector per root; another host's stays standby. Loaded on first use; a projector that
   * cannot open never fails the host (shadow state is not the authority). Its database is the
   * shadow default <mesh>/state-projector/state.db (projectorDatabaseRoot), never <mesh>/state.db.
   */
  #startStateProjector(): void {
    if (this.mesh.stateBackend !== "shadow" || this.#stateProjector) return;
    this.#stateProjector = import("../mesh/state-projector.js").then(async ({ StateProjector }) => {
      const projector = await StateProjector.open({ root: this.config.meshRoot, mode: "shadow" });
      if (!this.#closed) return projector.run();
      await projector.stop();
      return undefined;
    }).catch(() => undefined);
  }

  /** The running L3 projector (shadow mode); undefined in file or sqlite mode or when it failed to open. */
  get stateProjector(): Promise<StateProjector | undefined> {
    return this.#stateProjector ?? Promise.resolve(undefined);
  }

  async close(): Promise<void> {
    if (!this.#started || (this.#closed && !this.#closePending)) return;
    this.#closePending ??= this.#closeWithTransport().finally(() => { this.#closePending = undefined; });
    return this.#closePending;
  }

  async #closeWithTransport(): Promise<void> {
    const transport = this.#transport;
    const transportAbort = this.#transportAbort;
    const control = this.control;
    const participants = this.participants;
    if (transport || transportAbort) {
      this.#controlInboxBound = false;
      control?.pause();
    }
    try { await this.#close(); }
    finally {
      if (transport || transportAbort) {
        // Partial startup (including rejected open) preserves withdrawal before abort.
        try {
          await control?.close().catch(() => undefined);
          await participants?.close().catch(() => undefined);
        } finally {
          transportAbort?.abort();
          if (this.#transportAbort === transportAbort) this.#transportAbort = undefined;
          if (this.#transport === transport) this.#transport = undefined;
          try { await transport?.close(); }
          finally { try { this.mesh?.closeState(); } finally { this.#releaseLock(); } }
        }
      }
    }
  }

  async #close(): Promise<void> {
    this.#closed = true;
    if (this.#drain) this.#finishDrain(this.#drain, "needs-operator", "HOST_CLOSED", "Resident closed during cooperative drain");
    this.#containmentClient?.dispose(); this.#containmentClient = undefined;
    // Stops with the host (shutdown and the release handover's exit alike): the lease is released
    // so the successor host's projector takes over at once.
    const projectorStopped = this.#stateProjector?.then(projector => projector?.stop()).catch(() => undefined);
    const routeClosed = this.#routeOwner?.close();
    const watchersClosed = Promise.all([this.#closeWatcher(this.#requestWatcher), this.#closeWatcher(this.#rootWatcher),
      this.#closeWatcher(this.#meshWatcher)]);
    this.#requestWatcher = undefined;
    this.#rootWatcher = undefined;
    this.#meshWatcher = undefined;
    if (this.#requestRetryTimer) clearTimeout(this.#requestRetryTimer);
    this.#requestRetryTimer = undefined;
    if (this.#scheduleTimer) clearTimeout(this.#scheduleTimer);
    this.#scheduleTimer = undefined;
    if (this.#maintenanceTimer) clearTimeout(this.#maintenanceTimer);
    this.#maintenanceTimer = undefined;
    this.#maintenanceAt = undefined;
    const wakeWatchClosed = this.#wakeWatchErrorUnsubscribe?.();
    this.#wakeWatchErrorUnsubscribe = undefined;
    this.#processWorkUnsubscribe?.();
    this.#processWorkUnsubscribe = undefined;
    // FSWatcher.close() schedules native handle closure. Join its close event before
    // owner release or callers removing the residency directory on Windows.
    await wakeWatchClosed;
    await watchersClosed;
    await this.#requestDrain?.catch(() => undefined);
    await this.#wakeRecoveryPending;
    this.#wakeRecovery?.close(); this.#wakeRecovery = undefined;
    this.#requestRetention.close();
    await this.#legacyArchive?.close();
    // Stop drains first so an in-flight ask can settle within the actor shutdown grace.
    const actorsClosed = this.actors?.close();
    // Observed below by closeWithActors, but only after further awaits: a rejection
    // before then must not become an unhandled rejection that kills the host (pi-fabric#577).
    void actorsClosed?.catch(() => undefined);
    // Retire an eligibility/probe already in flight before releasing host custody.
    await this.#idleCheckPending?.catch(() => undefined);
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
          await projectorStopped;
          // The host owns its MeshStore: release the state database handle before the host fence,
          // so Windows can remove or migrate state.db once the host is gone (pi-fabric#640).
          try { this.mesh?.closeState(); } catch { /* best effort at teardown */ }
        }
      } finally { if (!this.#transport && !this.#transportAbort) this.#releaseLock(); }
    }
  }

  async #acceptControl(
    command: FabricControlCommand,
    from: MeshIdentity,
    signal?: AbortSignal,
    verification?: "mesh" | "bridge",
  ): Promise<FabricControlAcceptance> {
    if (this.#closed || this.#idleExitRequested || this.#staged || this.#handover || this.#drain) {
      return { accepted: false, error: HOST_CLOSING_RETRY };
    }
    assertMeshConsumption(() => this.participants.canConsumeMesh());
    this.#idleSince = undefined;
    this.#admissions++;
    this.#businessAdmissions++;
    try { return await this.#handleControl(command, from, signal, verification); }
    finally { this.#admissions--; this.#scheduleIdleCheck(); this.#noteReleaseProgress(); }
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
            this.participants.get(from.id, undefined, { fresh: true })?.rootId), sender: command.sender ?? null },
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
      if (result.undelivered) throw new FabricTargetTerminalError(command.targetId, result.undelivered.finalAnswerReceiptId, result.undelivered.runState);
      return { accepted: true, messageId: result.messageId,
        ...(result.warning ? { warning: result.warning } : {}) };
    } catch (error) {
      if (!(error instanceof Error && /Unknown Fabric agent/.test(error.message))) {
        return { accepted: false, error: errorMessage(error), ...terminalRejectionFields(error) };
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
      const result = this.#drain
        ? this.#drainActors().withReleaseAdmission(() => this.actors.tell(command.targetId, message, command.data, { provenance, ...options, sender: command.sender ?? null }))
        : this.actors.tell(command.targetId, message, command.data, { provenance, ...options, sender: command.sender ?? null });
      return { accepted: true, messageId: result.messageId };
    } catch (error) {
      if (error instanceof MeshConsumptionPausedError) throw error;
      return { accepted: false, error: errorMessage(error) };
    }
  }

  async #deliverTerminalNotice(notice: AgentTerminalNotice): Promise<void> {
    if (!notice.sender?.id) throw new Error("Terminal refusal has no attributed sender; retain native receipt");
    if (!this.#ready || this.#closed || this.#staged || this.#handover) throw new Error(HOST_CLOSING_RETRY);
    assertMeshConsumption(() => this.participants.canConsumeMesh());
    const message = `FABRIC_TARGET_TERMINAL: undelivered: task already finished; ${notice.delivery} ${notice.messageId} was not delivered to task ${notice.targetId}; ` +
      `its final answer receipt is ${notice.finalAnswerReceiptId}. Start a new task; this input cannot revive the old one.`;
    const from: MeshIdentity = { id: notice.targetId, name: `Task ${notice.targetId}`, kind: "agent" };
    if (this.#drain) {
      // Completion obligations may publish, but must not enqueue a new local/remote turn while held.
      await this.#queueDelivery(from, message, "steer", false, notice, undefined, notice.sender.id);
      return;
    }
    if (notice.sender.id === this.config.rootId) {
      // Existing resident durable delivery outbox, addressed to the exact sender
      // root. The route receipt means retained handoff, not sender consumption.
      await this.#queueDelivery(from, message, "steer", false, notice, undefined, notice.sender.id);
      return;
    }
    const provenance = fabricTurnProvenance(from, "steer", "mesh");
    try {
      this.agents.status(notice.sender.id);
      this.agents.steer(notice.sender.id, message, notice, provenance);
      return;
    } catch (error) {
      if (!(error instanceof Error && /Unknown Fabric agent/.test(error.message))) throw error;
    }
    if (this.actors.owns(notice.sender.id)) {
      this.actors.tell(notice.sender.id, message, notice, { provenance });
      return;
    }
    const sender = this.participants.get(notice.sender.id, undefined, { fresh: true });
    if (!sender || sender.stale) throw new Error(`Terminal refusal sender unavailable: ${notice.sender.id}`);
    await this.control.request(sender.ownerHostId, sender.id, "steer", { message, data: notice, triggerTurn: false }, sender.ownerIdentityId,
      { routedRemoteHost: sender.remoteHost ?? null, idempotencyKey: `terminal-refusal:${notice.targetId}:${notice.messageId}` });
  }

  async #deliverLifecycle(
    subscription: FabricLifecycleSubscription,
    event: FabricLifecycleEvent,
  ): Promise<void> {
    if (this.#closed || this.#staged || this.#handover || this.#drain) throw new Error(HOST_CLOSING_RETRY);
    assertMeshConsumption(() => this.participants.canConsumeMesh());
    this.#admissions++;
    this.#businessAdmissions++;
    try { await this.#handleLifecycle(subscription, event); }
    finally { this.#admissions--; this.#scheduleIdleCheck(); this.#noteReleaseProgress(); }
  }

  async #handleLifecycle(subscription: FabricLifecycleSubscription, event: FabricLifecycleEvent): Promise<void> {
    const message = `Fabric lifecycle ${event.event} from ${event.source.name} (${event.source.id})${event.status ? ` with status ${event.status}` : ""}.`;
    const wakeCause = fabricWakeCause(lifecycleSourceIdentity(event.source), "host-event", event.event, event.id);
    if (subscription.to === this.config.rootId) {
      await this.#queueDelivery(
        lifecycleSourceIdentity(event.source),
        message,
        subscription.delivery,
        subscription.triggerTurn,
        event,
        undefined,
        this.config.rootId,
        undefined,
        undefined,
        wakeCause,
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
      { message, data: event, triggerTurn: subscription.triggerTurn, ownerIncarnation: controlOwnerIncarnation(target), wakeCause },
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
    wakeCause?: FabricWakeCause,
  ): Promise<void> {
    const id = randomUUID();
    const diagnostic = copyFabricWakeCause(wakeCause);
    const record = (target: string): ResidentDeliveryRecord => ({
        format: RESIDENT_HOST_FORMAT,
        id,
        rootId: target,
        from,
        ...(source ? { source } : {}),
        ...(diagnostic ? { wakeCause: diagnostic } : {}),
        ...(principal ? { principal } : {}),
        delivery,
        triggerTurn,
        message,
        ...(data === undefined ? {} : { data }),
        ...(agentCompletionId ? { agentCompletionId } : {}),
        createdAt: Date.now(),
      });
    // Persist before handing off: idle exit/queue pressure must not drop custody.
    // Agent completions use the fixed creating root and still persist before yielding.
    const persist = (target: string): void =>
      writeJsonAtomic(path.join(this.#deliveryOutboxPath, `${id}.json`), record(target), { durable: true });
    if (typeof rootId === "function") {
      // Serialize proof+absence and target selection with resumed-root proof
      // invalidation: resumeLineage() invalidates the proof with a shared-state delete,
      // so the choice is a state commit (smarty-dev#6477 L5, R11). The irreversible
      // outbox write is that commit's effect: recorded in it, written after it, and
      // replayed after a crash. Never persist a stale choice.
      // The plan is this call's own: concurrent deliveries never run each other's effect or
      // read each other's result (on SQLite another batch can commit before this afterCommit).
      let chosen = false;
      const plan = this.#deliveryCommits.plan();
      try {
        await this.mesh.writeBatch({ identity: this.identity, ops: [],
          prepare: () => plan.stage([], [{ kind: "delivery", key: id, payload: record(rootId()) }], { durable: true }),
          afterCommit: view => { chosen = plan.run(view) === 1; } });
        await this.#deliveryCommits.retire().catch(() => undefined);
      } catch { /* no commit: the original mailbox below */ }
      if (!chosen) persist(this.config.rootId); // Unknown custody keeps the original mailbox.
    } else persist(rootId);
    this.#scheduleIdleCheck();
    await this.#retryDeliveries();
  }

  #retryDeliveries(): Promise<unknown> {
    if (this.#closed || this.#staged) return Promise.resolve();
    if (this.#flushingDeliveries) return this.#flushingDeliveries;
    // An empty maintenance retry is not delivery custody. Avoid creating a pending
    // promise on every request tick which can falsely restart the host idle window.
    if (this.#deliveryCommitsRecovered && (!fs.existsSync(this.#deliveryOutboxPath) ||
        !fs.readdirSync(this.#deliveryOutboxPath).some(entry => entry.endsWith(".json")))) return Promise.resolve();
    this.#deliveryFlushProgress = 0;
    const flushing = this.#deliveryRetry.run(() => this.#flushDeliveries());
    this.#flushingDeliveries = flushing;
    void flushing.then(status => {
      if (this.#flushingDeliveries === flushing) this.#flushingDeliveries = undefined;
      // smarty-dev#11172: no request tick retries the durable outbox. A flush that left items
      // (failure, backoff skip or the 32-item bound) owns its own retry deadline.
      // A failed CommitOutbox recovery is owed work even without a local outbox file.
      // A failed flush, or an outbox that cannot be listed, is possibly non-empty: still owed.
      let owed = status === "failed" || !this.#deliveryCommitsRecovered;
      if (!owed && !this.#closed) {
        try {
          owed = fs.existsSync(this.#deliveryOutboxPath) &&
            fs.readdirSync(this.#deliveryOutboxPath).some(entry => entry.endsWith(".json"));
        } catch { owed = true; }
      }
      if (!this.#closed && owed) {
        // Backoff wins; an immediate continuation only after measurable progress (the 32-item
        // bound). A no-progress pass with outstanding custody waits the 1 s failure default.
        const waitMs = this.#deliveryRetry.waitMs;
        this.#armRequestRetry(status === "failed" ? REQUEST_FAILURE_RETRY_MS : waitMs > 0 ? waitMs
          : status === "done" && this.#deliveryFlushProgress > 0 ? 1 : REQUEST_FAILURE_RETRY_MS);
      }
    }, () => {
      if (this.#flushingDeliveries === flushing) this.#flushingDeliveries = undefined;
      this.#armRequestRetry(REQUEST_FAILURE_RETRY_MS);
    }).finally(() => {
      this.#scheduleIdleCheck(); // Delivery custody ended; do not wait for a timed nudge.
      this.#noteReleaseProgress();
    }).catch(() => undefined);
    return flushing;
  }

  // The commit effect of a state-chosen delivery (see #queueDelivery). A replay never
  // recreates a delivery that a flush already handed to the mesh under either root.
  #writeDelivery(record: ResidentDeliveryRecord, view: MeshBatchView, replay: boolean): void {
    const file = path.join(this.#deliveryOutboxPath, `${record.id}.json`);
    if (replay && (fs.existsSync(file) || [record.rootId, this.config.rootId].some(root =>
      view.version(`${residentDeliveryPrefix(root)}${record.id}`) > 0))) return;
    writeJsonAtomic(file, record, { durable: true });
  }

  async #flushDeliveries(): Promise<void> {
    if (!this.#deliveryCommitsRecovered) {
      try {
        await this.#deliveryCommits.recover();
        this.#deliveryCommitsRecovered = true;
      } catch { /* retried on the next flush; the files below still flush */ }
    }
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
          if (isMeshWriteDeferred(error)) throw error; // no truncated rewrite for a refused write
          await this.mesh.put({
            key, value: { ...publish, message: detachedPreview(publish.message, Math.max(1, this.config.mesh.eventContextChars)), data: { fabricTruncated: true } },
            identity: this.identity, ifVersion: 0,
          });
        }
      }
      // Only a durable mesh handoff releases ownership. Main journals the stable id.
      fs.rmSync(file);
      this.#deliveryFlushProgress++;
    }
  }

  async #pollRequests(): Promise<void> {
    if (!this.#ready || this.#pollingRequests || this.#closed || this.#idleExitRequested) return;
    if (this.#staged || this.#handover || this.#drain) { await this.#advanceRelease(); return; }
    this.#pollingRequests = true;
    try {
      let entries: string[];
      try {
        entries = fs.readdirSync(this.#requestsPath).filter((entry) => entry.endsWith(".json"));
      } catch {
        this.#requestRetry = true; // a failed scan is failed work, not an empty queue
        return;
      }
      if (entries.length) { this.#idleSince = undefined; this.#requestRetention.markDirty(); }
      // Never overwrite an admitted exchange while its response is still pending: such a same-named
      // request is held for that exchange's settlement kick, and never counts toward a continuation.
      // Select the first 32 ADMISSIBLE entries, so blocked names cannot hide an eligible 33rd.
      const eligible = entries.filter(entry => !this.#boundaryRequests.has(path.join(this.#processingPath, entry)));
      // A bounded pass: an eligible remainder is drained by the next coalesced pass, not a timer.
      // Continue immediately only after real progress; a pass whose renames all failed waits for
      // the capped retry backoff instead of rescanning at once (no spin on persistent failure).
      let progressed = false;
      for (const entry of eligible.slice(0, 32)) {
        if (this.#staged || this.#handover || this.#drain || this.#closed) break;
        const source = path.join(this.#requestsPath, entry);
        const processing = path.join(this.#processingPath, entry);
        if (this.#boundaryRequests.has(processing)) continue; // admitted earlier in this same pass
        try {
          fs.renameSync(source, processing);
        } catch {
          this.#requestRetry = true; // e.g. a transient Windows sharing violation
          continue;
        }
        progressed = true;
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
          // A same-named request skipped above is drained once this exchange settles.
          void response.finally(() => {
            this.#boundaryRequests.delete(processing);
            this.#scheduleIdleCheck();
            this.#kickRequests();
          }).catch(() => undefined);
        }
      }
      if (eligible.length > 32 && progressed) this.#requestKick = true;
    } finally {
      this.#pollingRequests = false;
      if (!retentionV2Enabled()) this.#maintainRequests();
      await this.#checkIdle(false);
    }
  }

  #maintainRequests(): void {
    const now = Date.now();
    if (!this.#ready || this.#closed || this.#staged || this.#handover || this.#drain || !this.participants.canConsumeMesh()) return;
    // Keep the stuck-preparing cadence independent from retention, as in origin/main.
    // smarty-dev#11172: report at the exact episode deadline (no 10 s gate between two episodes).
    // reportStuckPreparing notifies the due episodes, so nextStuckPreparingAt moves past them. An
    // episode still owed right after a report (failure/unnotified) retries at one bounded deadline.
    const stuckPreparingMs = this.config.agents?.stuckPreparingMs ?? DEFAULT_STUCK_PREPARING_MS;
    const stuckAt = stuckPreparingMs > 0 ? this.actors.nextStuckPreparingAt(stuckPreparingMs, now) : undefined;
    if (stuckAt !== undefined && stuckAt <= now && !(this.#stuckPreparingRetryAt !== undefined && now < this.#stuckPreparingRetryAt)) {
      this.#stuckPreparingCheckedAt = now;
      let failed = false;
      try { this.actors.reportStuckPreparing(stuckPreparingMs, now); } catch { failed = true; }
      const still = this.actors.nextStuckPreparingAt(stuckPreparingMs, now);
      this.#stuckPreparingRetryAt = failed || (still !== undefined && still <= now) ? now + STUCK_PREPARING_CHECK_MS : undefined;
    } else if (stuckAt === undefined || stuckAt > now) this.#stuckPreparingRetryAt = undefined;
    // Preserve main's policy-reload observation cadence; this is one cached config stamp,
    // not the retained-run reference scan. B gates the expensive work on due() in both modes.
    Object.assign(this.#retention, this.#currentRetention(now));
    if (!this.#requestRetention.due(now)) return;
    const live = retentionV2Enabled() ? this.agents.retentionReferences({ now }) : this.agents.retentionReferences();
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

  /** smarty-dev#11172: drains requests on directory events, startup and known failed-work retries.
   * Concurrent kicks coalesce into one more bounded pass after the running one. */
  #kickRequests(): void {
    this.#requestKick = true;
    if (this.#requestDrain || this.#closed) return;
    this.#requestDrain = (async () => {
      try {
        while (this.#requestKick && !this.#closed) {
          this.#requestKick = false;
          this.#requestRetry = false;
          const polled = await this.#backgroundRequests.run(() => this.#pollRequests());
          // The former tick also retried the outbox; it arms its own deadline when items remain.
          await this.#retryDeliveries();
          if (polled === "retry" || (polled === "skipped" && this.#backgroundRequests.waitMs > 0)) {
            this.#armRequestRetry(Math.max(1, this.#backgroundRequests.waitMs));
          } else if (polled === "failed") this.#armRequestRetry(REQUEST_FAILURE_RETRY_MS);
          // A failed rename is pending failed work: retried on one coalesced timer with a capped
          // backoff (50 ms doubling to 1 s), reset by the first clean pass.
          if (this.#requestRetry) {
            this.#armRequestRetry(Math.min(REQUEST_FAILURE_RETRY_MS, REQUEST_POLL_MS * 2 ** this.#requestFailureStreak));
            this.#requestFailureStreak = Math.min(this.#requestFailureStreak + 1, 5);
          } else this.#requestFailureStreak = 0;
          // A release progresses on root-directory events (handover/custody/main-generation files) and
          // host-internal settle events (#noteReleaseProgress). Launcher liveness has no event: it is
          // re-observed at the real drain deadline, and at most once a minute before it.
          if (this.#staged || this.#handover) {
            const now = Date.now();
            const deadline = this.#handover ? this.#handover.createdAt + HANDOVER_DRAIN_MS + 1 : Number.POSITIVE_INFINITY;
            this.#armRequestRetry(Math.max(1, Math.min(deadline - now, RELEASE_RESAMPLE_MS)));
          }
        }
      } catch { this.#armRequestRetry(REQUEST_FAILURE_RETRY_MS); }
      finally { this.#requestDrain = undefined; }
      this.#scheduleMaintenance();
    })();
  }

  #armRequestRetry(ms: number): void {
    if (this.#closed) return;
    const at = performance.now() + ms;
    if (this.#requestRetryTimer && this.#requestRetryAt! <= at) return;
    if (this.#requestRetryTimer) clearTimeout(this.#requestRetryTimer);
    this.#requestRetryAt = at;
    this.#requestRetryTimer = setTimeout(() => {
      this.#requestRetryTimer = undefined;
      this.#requestRetryAt = undefined;
      this.#kickRequests();
    }, ms);
  }

  /** smarty-dev#11172: the request queue's primary wake source. Unsupported at startup fails closed
   * (start() throws before readiness). A watcher error buys one re-registration plus one bounded
   * rescan; a repeated error makes the host unavailable (#failWatch). A healthy empty queue has
   * no request timer; failed scans and renames retain their own bounded retry deadline. */
  #watchRequests(startup = false): void {
    if (this.#closed) return;
    let watcher: fs.FSWatcher;
    try {
      watcher = fs.watch(this.#requestsPath, (_event, filename) => {
        // A null filename is an overflow/unknown change: one bounded rescan.
        if (!filename || String(filename).endsWith(".json")) this.#kickRequests();
      });
    } catch (error) {
      if (startup) throw new Error(`Resident request watch unavailable for ${this.#requestsPath}: ${errorMessage(error)}`);
      this.#failWatch("request", error);
      return;
    }
    this.#onWatchLoss(watcher, (error) => {
      if (this.#requestWatcher === watcher) this.#requestWatcher = undefined;
      void this.#closeWatcher(watcher);
      if (this.#closed) return;
      if (this.#requestWatchRetried) { this.#failWatch("request", error); return; }
      // One re-registration, then one bounded rescan for anything written meanwhile.
      this.#requestWatchRetried = true;
      this.#watchRequests();
      this.#kickRequests();
    });
    this.#requestWatcher = watcher;
  }

  /** One residency-root watch serves config.json replacement (maintenance) and the release files
   * (handover.json, handover-<id>.custody.json, main-generation.json). Same failure contract. */
  #watchRoot(startup = false): void {
    if (this.#closed) return;
    let watcher: fs.FSWatcher;
    try {
      watcher = fs.watch(this.config.residencyRoot, (_event, filename) => this.#onRootEvent(filename ? String(filename) : undefined));
    } catch (error) {
      if (startup) throw new Error(`Resident root watch unavailable for ${this.config.residencyRoot}: ${errorMessage(error)}`);
      this.#failWatch("root", error);
      return;
    }
    this.#onWatchLoss(watcher, (error) => {
      if (this.#rootWatcher === watcher) this.#rootWatcher = undefined;
      void this.#closeWatcher(watcher);
      if (this.#closed) return;
      if (this.#rootWatchRetried) { this.#failWatch("root", error); return; }
      this.#rootWatchRetried = true;
      this.#watchRoot();
      this.#onRootEvent(undefined); // one bounded re-read of config and release state
    });
    this.#rootWatcher = watcher;
  }

  /** smarty-dev#11172: a cross-process mesh.schedule() (including one moving a due time EARLIER than
   * the armed timer) rewrites schedules.json. Its event re-evaluates the exact due time through the
   * idle check; there is no periodic deadline check. Same failure contract as the other watches. */
  #watchMesh(startup = false): void {
    if (this.#closed) return;
    let watcher: fs.FSWatcher;
    try {
      watcher = fs.watch(this.config.meshRoot, (_event, filename) => {
        if (!filename || String(filename) === "schedules.json") this.#scheduleIdleCheck();
      });
    } catch (error) {
      if (startup) throw new Error(`Resident mesh schedule watch unavailable for ${this.config.meshRoot}: ${errorMessage(error)}`);
      this.#failWatch("mesh", error);
      return;
    }
    this.#onWatchLoss(watcher, (error) => {
      if (this.#meshWatcher === watcher) this.#meshWatcher = undefined;
      void this.#closeWatcher(watcher);
      if (this.#closed) return;
      if (this.#meshWatchRetried) { this.#failWatch("mesh", error); return; }
      this.#meshWatchRetried = true;
      this.#watchMesh();
      this.#scheduleIdleCheck(); // one bounded re-read of the schedule due time
    });
    this.#meshWatcher = watcher;
  }

  /** A watcher is lost on `error` or on a native `close` this host did not request; the handler runs once. */
  #onWatchLoss(watcher: fs.FSWatcher, handler: (error: unknown) => void): void {
    let lost = false;
    const lose = (error: unknown) => { if (lost) return; lost = true; handler(error); };
    watcher.on("error", lose);
    watcher.on("close", () => {
      if (!this.#ownClosedWatchers.has(watcher)) lose(new Error("native watch closed unexpectedly"));
    });
  }

  #onRootEvent(filename: string | undefined): void {
    if (this.#closed) return;
    // An undefined filename is an overflow/unknown change: re-read both.
    if (filename === undefined || filename === "config.json") {
      // Main replaced (or touched) config.json: invalidate the cached stamp, so even a same-size
      // replacement with an identical coarse stamp is re-read at this maintenance pass.
      this.#retentionOverlay = undefined;
      this.#runMaintenanceSoon();
    }
    if ((this.#staged || this.#handover || this.#drain) && (filename === undefined || filename === "main-generation.json" ||
        filename === "handover.json" || (filename.startsWith("handover-") && filename.endsWith(".custody.json")))) {
      this.#kickRequests();
    }
  }

  /** A host-internal settle event (admission, run, actor, request completion) during a release. */
  #noteReleaseProgress(): void {
    // A pass's own checkpoint events end in custody, cancellation or a throw; never re-kick from them.
    // A drain waiting for accepted work still needs its settle edge, even if an early-returning
    // release check has not yet unwound its await. Only the active checkpoint suppresses it.
    if ((this.#drain && !this.#drainCheckpointPending) ||
        ((this.#staged || this.#handover) && !this.#advancingRelease)) this.#kickRequests();
  }

  /** A repeated watch failure: surface a terminal, unavailable host instead of a silent strand or a
   * recurring poll. Accepted requests stay durable on disk; the exit lets a successor serve them. */
  #failWatch(kind: "request" | "root" | "mesh", error: unknown): void {
    if (this.#closed || this.#watchFailed) return;
    this.#watchFailed = true;
    console.error(`[pi-fabric] resident ${kind} watch failed after its one re-registration; host unavailable, exiting: ${errorMessage(error)}`);
    this.#kickRequests(); // the bounded final rescan before exit
    this.onIdle("error");
  }

  #closeWatcher(watcher: fs.FSWatcher | undefined): Promise<void> {
    if (!watcher) return Promise.resolve();
    this.#ownClosedWatchers.add(watcher);
    const closed = new Promise<void>(resolve => watcher.once("close", () => resolve()));
    try { watcher.close(); } catch { return Promise.resolve(); }
    return closed;
  }

  #runMaintenanceSoon(): void {
    if (!this.#maintenanceArmed || this.#closed || this.#staged || this.#handover || this.#drain) return;
    if (this.#maintenanceTimer) clearTimeout(this.#maintenanceTimer);
    this.#maintenanceAt = Date.now();
    this.#maintenanceSoon = true;
    this.#maintenanceTimer = setTimeout(() => this.#runMaintenance(), 0);
  }

  #runMaintenance(): void {
    this.#maintenanceTimer = undefined;
    this.#maintenanceAt = undefined;
    this.#maintenanceSoon = false;
    if (this.#idleSince !== undefined) {
      const at = this.#idleSince + (this.#idleWakeable ? IDLE_EXIT_MS : this.#residentIdleExitMs);
      if (at <= Date.now()) this.#idleDeadlineSampled = at;
    }
    try {
      this.#maintainRequests();
    } finally {
      // The dead-root/dormancy idle sample the request tick used to take.
      this.#sampleIdle();
    }
  }

  #sampleIdle(): void {
    if (this.#closed) return;
    void this.#checkIdle(false).catch(() => undefined).finally(() => this.#scheduleMaintenance());
  }

  /** smarty-dev#11172: one timer at the nearest real maintenance deadline; none without one. */
  #scheduleMaintenance(): void {
    if (!this.#maintenanceArmed || this.#closed) return;
    const now = Date.now();
    const at = this.#nextMaintenanceAt(now);
    // Config events still require a pass when no deadline was previously known. Only a held
    // admission boundary cancels that event-owned pass; resume explicitly requests it again.
    if (this.#maintenanceSoon && !this.#staged && !this.#handover && !this.#drain) return;
    if (at === undefined) {
      // No deadline remains, or admissions are held: cancel even an event-requested pass.
      if (this.#maintenanceTimer) clearTimeout(this.#maintenanceTimer);
      this.#maintenanceTimer = undefined;
      this.#maintenanceAt = undefined;
      this.#maintenanceSoon = false;
      return;
    }
    // Re-arm at the exact nearest deadline: an earlier deadline that disappeared must not leave a stale wake.
    // Within one slice earlier is kept, so frequent re-asks cannot starve a running scan's slice.
    if (this.#maintenanceTimer && this.#maintenanceAt! <= at && at - this.#maintenanceAt! <= MAINTENANCE_SLICE_MS) return;
    if (this.#maintenanceTimer) clearTimeout(this.#maintenanceTimer);
    this.#maintenanceAt = at;
    this.#maintenanceTimer = setTimeout(() => this.#runMaintenance(), Math.min(Math.max(0, at - now), 2_147_000_000));
  }

  #nextMaintenanceAt(now: number): number | undefined {
    if (!this.#ready || this.#closed || this.#staged || this.#handover || this.#drain || this.#sleeping || this.#idleExitRequested) return undefined;
    const due: number[] = [];
    // Retention: a running scan continues in slices; an empty root has no sample until new activity.
    const retention = this.#requestRetention.nextDueAt();
    if (retention !== undefined) {
      due.push(retention > now ? retention : now + (this.participants.canConsumeMesh() ? MAINTENANCE_SLICE_MS : IDLE_ACTOR_CHECK_MS));
    }
    // Stuck preparation: the exact earliest un-notified active episode's threshold (permit waits pause
    // it; none owed is undefined). The existing check rate limit stays a floor, never a cadence.
    const stuckPreparingMs = this.config.agents?.stuckPreparingMs ?? DEFAULT_STUCK_PREPARING_MS;
    const stuckAt = stuckPreparingMs > 0 ? this.actors.nextStuckPreparingAt(stuckPreparingMs, now) : undefined;
    if (stuckAt !== undefined) {
      if (stuckAt > now) due.push(stuckAt);
      // Owed now: report at once, unless a just-failed report holds its bounded retry deadline, or a
      // lapsed lease skips the pass (bounded failed-work deadline, never a 0 ms re-arm).
      else if (this.#stuckPreparingRetryAt !== undefined && this.#stuckPreparingRetryAt > now) due.push(this.#stuckPreparingRetryAt);
      else due.push(this.participants.canConsumeMesh() ? now : now + STUCK_PREPARING_CHECK_MS);
    }
    // An unsettled config.json stamp is trusted (cached) once it settles.
    if (this.#configSettleAt !== undefined) due.push(this.#configSettleAt);
    // The running idle window's expiry: dormancy (durable) or dead-root retirement.
    if (!this.#staged && !this.#handover && this.#idleSince !== undefined) {
      const exitMs = this.#idleWakeable ? IDLE_EXIT_MS : this.#residentIdleExitMs;
      const at = this.#idleSince + exitMs;
      // Sampled once at its deadline; a blocked exit (e.g. mesh catching up) then waits for an event.
      if (exitMs > 0 && (at > now || this.#idleDeadlineSampled !== at)) due.push(at);
    }
    // Dead-root retirement: re-observe the root exactly when its known lease lapses (renewal re-arms
    // at the new expiry). Unknown/unreadable expiry stays fail-closed: presence pass and events only.
    if (!this.#staged && !this.#handover && this.#idleSince === undefined && this.#residentIdleExitMs > 0 &&
        this.#rootLapseAt !== undefined && this.#rootLapseAt > now) due.push(this.#rootLapseAt);
    return due.length ? Math.min(...due) : undefined;
  }

  /** The accepted retention overlay, re-read only when config.json was replaced or changed:
   * this runs on every maintenance pass, and Main rewrites the file atomically. */
  #currentRetention(now = Date.now()): ResidentHostConfig["retention"] {
    let stamp: string | undefined;
    let settled = false;
    this.#configSettleAt = undefined;
    try {
      const stat = fs.statSync(path.join(this.config.residencyRoot, "config.json"), { bigint: true });
      stamp = `${stat.dev}:${stat.ino}:${stat.size}:${stat.mtimeNs}:${stat.ctimeNs}`;
      settled = BigInt(Math.floor(now)) * 1_000_000n - stat.mtimeNs >= BigInt(CONFIG_STAMP_SETTLE_MS) * 1_000_000n;
      if (!settled) this.#configSettleAt = Math.max(now + 1, Number(stat.mtimeNs / 1_000_000n) + CONFIG_STAMP_SETTLE_MS);
    } catch { /* Absent or unreadable: re-evaluated on the next config event or maintenance pass. */ }
    if (stamp !== undefined && this.#retentionOverlay?.stamp === stamp) {
      this.#residentIdleExitMs = this.#retentionOverlay.idleExitMs;
      return this.#retentionOverlay.retention;
    }
    const config = this.#effectiveConfig?.() ?? this.config;
    const retention = config.retention;
    const configured = config.mesh.residentIdleExitMs;
    const idleExitMs = typeof configured === "number" && Number.isFinite(configured) && configured >= 0
      ? configured : RESIDENT_IDLE_EXIT_MS;
    if (idleExitMs !== this.#residentIdleExitMs) this.#idleSince = undefined;
    this.#residentIdleExitMs = idleExitMs;
    // Cache only a settled stamp: a file younger than the settle window is read on every tick.
    this.#retentionOverlay = stamp !== undefined && settled ? { stamp, retention, idleExitMs } : undefined;
    return retention;
  }

  /** Actor activity restarts the idle window and requests an immediate event-driven check. */
  #noteActorActivity(): void {
    this.#idleSince = undefined;
    this.#activeActor = { at: Number.NEGATIVE_INFINITY, active: true };
    this.#idleObservation = { at: Number.NEGATIVE_INFINITY, blocked: true, wakeable: false };
    this.#requestRetention.markDirty();
    this.#scheduleIdleCheck();
    this.#scheduleMaintenance(); // e.g. a newly in-flight item's stuck-preparing deadline
    this.#noteReleaseProgress();
  }

  #hasActiveActor(now: number, current = false): boolean {
    if (current || now - this.#activeActor.at >= ACTIVE_ACTOR_CACHE_MS) {
      this.#activeActor = { at: now, active: this.actors.hasActiveDurableActor() };
    }
    return this.#activeActor.active;
  }

  #hasIdleBlocker(now: number, current = false): boolean {
    if (current || now - this.#idleObservation.at >= IDLE_ACTOR_CHECK_MS) {
      let blocked = true, wakeable = false;
      try {
        // get() honours Main's published reloadUntil and matching host-lease incarnation.
        // A live Main host lease also vetoes exit if its participant publication is absent.
        const root = this.participants.get(this.config.rootId, now, { fresh: true });
        const lease = readHostLeaseCurrent(this.config.meshRoot, this.config.rootId);
        const leaseValid = !!(lease && lease.rootId === this.config.rootId && lease.identityId === this.config.rootId);
        let rootLive = !!root || !!(leaseValid && lease!.expiresAt >= now);
        // The actual known lapse: lease expiry, or a reloading Main's reload lease if later.
        const lapse = Math.max(leaseValid ? lease!.expiresAt + 1 : Number.NEGATIVE_INFINITY,
          root?.status === "reloading" && root.reloadUntil !== undefined ? root.reloadUntil + 1 : Number.NEGATIVE_INFINITY);
        this.#rootLapseAt = rootLive && Number.isFinite(lapse) && lapse > now ? lapse : undefined;
        if (!lease) {
          try { fs.lstatSync(hostLeasePath(this.config.meshRoot, this.config.rootId)); rootLive = true; }
          catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") rootLive = true; }
        }
        const owned = this.actors.hasOwnedActors();
        blocked = rootLive || owned;
        // Owned durable actors (dormant included) leave by dormant release, never by dead-root retirement.
        wakeable = owned && this.actors.hasDurableActor();
      } catch { /* Unknown ownership/liveness is not permission to exit. */ }
      this.#idleObservation = { at: now, blocked, wakeable };
    }
    return this.#idleObservation.blocked;
  }

  #hasPendingRequest(): boolean {
    return !!(this.#pollingRequests || this.#admissions || this.#boundaryRequests.size) ||
      [this.#requestsPath, this.#processingPath].some(directory => {
        try { return fs.readdirSync(directory).some(entry => entry.endsWith(".json")); }
        catch { return true; } // Unreadable queues cannot certify emptiness.
      });
  }

  #scheduleIdleCheck(): void {
    this.#idleCheckRequested = true;
    if (this.#closed || this.#staged || this.#handover || this.#drain || this.#sleeping || this.#idleCheckQueued || this.#idleCheckPending) return;
    this.#idleCheckQueued = true;
    try {
      queueMicrotask(() => {
        this.#idleCheckQueued = false;
        if (this.#closed || this.#staged || this.#handover || this.#drain || this.#sleeping || this.#idleCheckPending || !this.#idleCheckRequested) return;
        this.#idleCheckRequested = false;
        const pending = this.#checkIdle();
        this.#idleCheckPending = pending;
        void pending.finally(() => {
          if (this.#idleCheckPending === pending) this.#idleCheckPending = undefined;
          if (this.#idleCheckRequested) this.#scheduleIdleCheck();
          // smarty-dev#11172: event-driven checks serve dormancy; dead-root retirement starts its
          // window on a sample. Take that sample on the event instead of the old request tick.
          else if (!this.#idleWakeable && this.#idleSince === undefined) this.#sampleIdle();
          else this.#scheduleMaintenance();
        }).catch(() => undefined);
      });
    } catch (error) {
      this.#idleCheckQueued = false; // a later eligibility event must still be able to schedule
      throw error;
    }
  }

  async #ensureWakeWatch(): Promise<boolean> {
    if (this.#wakeWatchSupported !== undefined) return this.#wakeWatchSupported;
    try {
      await assertResidentWakeWatch(this.config.residencyRoot);
      this.#wakeWatchSupported = true;
      this.#wakeWatchReprobeUsed = false;
      this.#wakeWatchNeedsReprobe = false;
      return true;
    } catch (error) {
      if (this.#closed) return false; // host close retired its pending native proof
      const watcherError = error instanceof ResidentWakeWatchError;
      if (watcherError && !this.#wakeWatchReprobeUsed) {
        this.#wakeWatchReprobeUsed = true;
        this.#wakeWatchSupported = undefined;
        diagnostics.warn("src/residency/host.ts", `[pi-fabric] resident dormancy watcher failed for ${this.config.residencyRoot}: ${String(error)}`);
        // An error event authorizes exactly one re-probe. Silent and synchronous
        // failures remain cached as unsupported for this process and root.
        this.#scheduleIdleCheck();
        return false;
      }
      this.#wakeWatchSupported = false;
      diagnostics.warn("src/residency/host.ts", `[pi-fabric] resident dormancy disabled for ${this.config.residencyRoot}: ${String(error)}`);
      return false;
    }
  }

  /** Dormancy may release the owner only when delivery can relaunch this exact generation. */
  #hasWakeConfig(): boolean {
    const saved = readWakeJson<ResidentHostConfig>(path.join(this.config.residencyRoot, "config.json"));
    if (residentWakeConfigMatches(saved, this.config)) return true;
    diagnostics.warn("src/residency/host.ts", `[pi-fabric] resident dormancy disabled: restart config differs or is unreadable for ${this.config.residencyRoot}`);
    return false;
  }

  /** Capacity and its sleeping reservation share the publication lock across hosts. */
  async #admitWakeDormancy(reserve = false): Promise<boolean> {
    let available = false;
    try {
      available = await this.mesh.exclusive(() => {
        if (!residentWakeCapacityAvailable(this.mesh.root, this.config.residencyRoot)) return false;
        if (reserve) writeJsonAtomic(residentSleepingPath(this.config.residencyRoot), { token: this.#token,
          request: readWakeJson<unknown>(residentWakeRequestPath(this.config.residencyRoot)) }, { durable: true });
        return true;
      });
    } catch { /* Unreadable capacity is not permission to release the resident owner. */ }
    if (available) this.#wakeCapacityWarned = false;
    else if (!this.#wakeCapacityWarned) {
      this.#wakeCapacityWarned = true;
      diagnostics.warn("src/residency/host.ts", `[pi-fabric] resident dormancy deferred: wake capacity (${RESIDENT_WAKE_INDEX_MAX_ROOTS}) full or unavailable; staying resident for ${this.config.residencyRoot}`);
    }
    return available;
  }

  #writeWakeRoutes(): void {
    const routes: ResidentWakeRoutes = { format: 1, rootId: this.config.rootId, hostId: this.hostId,
      configJson: canonicalResidentWakeConfig(this.config),
      actors: this.actors.listOwned().filter(actor => actor.residency === "durable" && actor.status !== "stopped")
        .map(actor => {
          // Preserve the host's actual published owner/capability/ACK advertisement for
          // dormant command admission after close withdraws its live directory records.
          const participant = this.participants.get(actor.id) ??
            readWakeJson<ResidentWakeRoutes>(path.join(this.config.residencyRoot, "wake-routes.json"))?.actors
              .find(entry => entry.id === actor.id)?.participant;
          return { id: actor.id, name: actor.name, topics: actor.topics, ...(participant ? { participant } : {}) };
        }) };
    const serialized = JSON.stringify(routes);
    if (serialized === this.#wakeRoutesJson) return;
    writeJsonAtomic(path.join(this.config.residencyRoot, "wake-routes.json"), routes, { durable: true });
    this.#wakeRoutesJson = serialized;
  }

  #expectedActors(): Set<string> {
    const protectedIds = new Set<string>();
    const actors = this.actors.listOwned();
    try {
      const main = this.participants.get(this.config.rootId, undefined, { fresh: true });
      for (const actor of actors) {
        // A live Main's supervisor is expected, even between its activations.
        if (main && (actor.events.length > 0 || actor.delivery !== "mailbox" || actor.topics.includes("fabric.participant.lifecycle"))) {
          protectedIds.add(actor.id);
        }
      }
      // A subscribed live participant can still owe this actor a reply.
      for (const subscription of this.lifecycle.list()) {
        if (actors.some(actor => actor.id === subscription.to) && this.participants.get(subscription.from, undefined, { fresh: true })) {
          protectedIds.add(subscription.to);
        }
      }
    } catch { for (const actor of actors) protectedIds.add(actor.id); } // uncertainty is not idle
    return protectedIds;
  }

  // Creation/adoption/reconciliation retain release custody, but must not consume
  // an actor's eligibility event. Ordinary dormancy revalidates each actor's
  // activation work; every idle-exit boundary still uses the full custody count.
  #hasHostWork(includeActorCustody = true): boolean {
    return this.#wakeRecoveryPending !== undefined || residentProcessWorkPending(this.processWorkSessionId) || this.#admissions > 0 || this.#boundaryRequests.size > 0 ||
      this.#publicationFailed || (includeActorCustody ? this.actors.releaseCustodyCount() > 0 : this.actors.inFlightActorIds().length > 0) ||
      this.agents.listForUi().some(agent => agent.status === "queued" || agent.status === "running") ||
      [this.#requestsPath, this.#processingPath].some(directory => {
        try { return fs.readdirSync(directory).some(entry => entry.endsWith(".json")); }
        catch { return false; }
      }) || this.#publications.size > 0 || this.#flushingDeliveries !== undefined ||
      (fs.existsSync(this.#deliveryOutboxPath) && fs.readdirSync(this.#deliveryOutboxPath).some(entry => entry.endsWith(".json")));
  }

  async #checkIdle(includeDormancy = true): Promise<void> {
    if (this.#closed || this.#idleExitRequested || this.#staged || this.#handover || this.#drain || this.#sleeping) {
      this.#idleSince = undefined;
      return;
    }
    const now = Date.now();
    // Startup/reload and open process calls are work even when all actors look idle.
    // Dormant release restarts its window now; dead-root retirement at its next idle sample.
    // Only event-owned eligibility distinguishes actor activation from release
    // custody. Request polling and all final boundaries retain the exact veto.
    if (!this.#ready || this.#hasHostWork(!includeDormancy)) { this.#idleSince = this.#idleWakeable ? now : undefined; return; }
    if (!includeDormancy && this.#idleCheckPending) return;
    if (includeDormancy) {
      if (this.#wakeWatchNeedsReprobe && !await this.#ensureWakeWatch()) return;
      const owned = this.actors.listOwned();
      const hasIdleActor = owned.some(actor => actor.residency === "durable" && actor.status === "idle");
      const protectedIds = hasIdleActor ? this.#expectedActors() : new Set<string>();
      const eligible = hasIdleActor && this.#hasWakeConfig() && this.actors.hasDormantIdleActor(protectedIds);
      if (eligible) {
        if (!await this.#admitWakeDormancy()) { this.#idleSince = now; return; }
        if (!await this.#ensureWakeWatch()) {
          this.#idleSince = now;
          return;
        }
        if (this.#closed || !this.#ready || this.#staged || this.#handover || this.#drain || this.#sleeping ||
            this.#hasHostWork(false) || !this.#hasWakeConfig()) { this.#idleSince = Date.now(); return; }
        try {
          // The watcher proof may yield to delivery or presence changes. Refresh
          // expected participants; dormantIdleActors rechecks current work custody.
          const currentProtectedIds = this.#expectedActors();
          this.#writeWakeRoutes(); // Arm routing before an actor can become dormant.
          await this.actors.dormantIdleActors(currentProtectedIds);
        } catch {
          // Keep the actor warm. Only a new actor/delivery/presence event retries.
          this.#idleSince = now;
          return;
        }
      }
    }
    // A previously dormant actor still needs a working watcher before this host
    // can release its final consumer gate after a failed re-probe.
    if (this.#wakeWatchSupported === false && this.actors.listOwned().some(actor =>
      actor.residency === "durable" && actor.status !== "stopped")) {
      this.#idleSince = now;
      return;
    }
    const activeActor = this.#hasActiveActor(now);
    const activeAgent = this.agents
      .listForUi()
      .some((agent) => agent.status === "queued" || agent.status === "running");
    // Host work (admissions, requests, publications, outbox, process custody, wake recovery)
    // is the dormant-wake superset of main's pending-request check.
    const pendingRequest = this.#hasHostWork() || this.#hasPendingRequest();
    // Owned durable actors (dormant included) leave by dormant release after IDLE_EXIT_MS. Without one,
    // dead-root retirement holds the host while its root Main lives or it owns any actor.
    const blocked = this.#hasIdleBlocker(now);
    const durableActor = this.#idleObservation.wakeable;
    // Each exit path counts its own continuous window.
    if (durableActor !== this.#idleWakeable) {
      this.#idleWakeable = durableActor;
      this.#idleSince = undefined;
    }
    const retirementBlocked = !durableActor && blocked;
    let nextScheduleDueAt: number | undefined;
    try {
      nextScheduleDueAt = this.mesh.nextScheduleDueAt();
    } catch {
      // Unreadable schedules neither keep the host alive nor stop it.
    }
    // Main's rule holds pending schedules for durable participants. A dormant durable actor is
    // one too: only this host's due timer releases its schedule, and the delivery wakes it.
    let durableParticipants = activeActor || activeAgent;
    if (!durableParticipants && nextScheduleDueAt !== undefined) {
      try { durableParticipants = this.actors.hasDurableActor(); }
      catch { durableParticipants = true; } // Unknown ownership is not permission to drop a schedule.
    }
    const decision = residentIdleDecision({
      now,
      idleSince: this.#idleSince ?? now,
      activeActor: activeActor || retirementBlocked,
      activeAgent,
      pendingRequest,
      durableParticipants,
      nextScheduleDueAt,
      idleExitMs: durableActor ? IDLE_EXIT_MS : this.#residentIdleExitMs,
    });
    // Due schedules are released on time even while idle exit is disabled.
    this.#armScheduleWake(decision.wakeAt);
    if (decision.busy || (!durableActor && !this.#residentIdleExitMs)) {
      this.#idleSince = undefined;
      return;
    }
    // Dead-root retirement starts its window on request-poll samples; event-driven checks serve dormancy.
    if (!durableActor && includeDormancy) return;
    this.#idleSince ??= now;
    const idleMs = now - this.#idleSince;
    if (!durableActor) {
      if (idleMs < this.#residentIdleExitMs) return;
      // A disable/rollback may have arrived between maintenance and this deadline tick.
      // Revalidate the accepted policy before committing retirement, not just on maintenance.
      this.#currentRetention(now);
      if (!this.#residentIdleExitMs || this.#idleSince === undefined || now - this.#idleSince < this.#residentIdleExitMs) return;
      // Never retire from a reused observation, or after a request or host work appeared during the fresh read.
      if (this.#hasIdleBlocker(now, true) || this.#hasPendingRequest() || this.#hasHostWork()) {
        this.#idleSince = undefined;
        return;
      }
      const marker: ResidentIdleExit = { format: 1, reason: "root-dead-idle", rootId: this.config.rootId,
        pid: process.pid, token: this.#token, at: now, idleMs };
      atomicWrite(residentIdleExitPath(this.config.residencyRoot), marker);
      this.#idleExitRequested = true;
      diagnostics.error("src/residency/host.ts", `resident exiting: root dead, no actors, idle ${Math.round(idleMs / 6_000) / 10} min`);
      this.onIdle("idle-exit"); // runResidentHost joins close(): withdraw lease/owner, then release the host fence.
      return;
    }
    if (idleMs < IDLE_EXIT_MS) return;
    // Never exit on a reused actor observation: confirm with a current actor check.
    if (this.#hasActiveActor(now, true)) {
      this.#idleSince = now;
      return;
    }
    // A bare/in-process host or an invalid generation has no delivery-owned restart.
    // Only check this at an actual exit boundary, not on every request-poll tick.
    if (this.actors.listOwned().some(actor =>
      actor.residency === "durable" && actor.status !== "stopped") && !this.#hasWakeConfig()) {
      this.#idleSince = now;
      return;
    }
    // Native watch delivery is asynchronous (especially on Windows). Ignored presence
    // events can leave the cursor behind after the actor is already dormant. Keep the
    // consumer gate open until it catches up; that is progress, not new host work and
    // must not restart the idle window. The final checkpoint still fences racing work.
    if (!this.actors.meshCaughtUp()) { this.#idleBlockedOnCatchup = true; return; }
    this.#idleBlockedOnCatchup = false;
    // Publish the closing boundary before shutting any consumer gate: a racing publisher
    // now queues a wake whose one-shot launcher waits for this exact owner to exit.
    this.#sleeping = true;
    try {
      // A bounded live log alone cannot retain a long sleeping actor's deliveries.
      // Enable the existing file archive under the same publication lock before sleeping.
      if (this.actors.listOwned().some(actor => actor.residency === "durable" && actor.status !== "stopped")) {
        await ensureResidentWakeArchive(this.mesh);
        if (!await this.#admitWakeDormancy(true)) {
          this.#sleeping = false;
          this.#idleSince = Date.now();
          return;
        }
      } else {
        writeJsonAtomic(residentSleepingPath(this.config.residencyRoot), { token: this.#token,
          request: readWakeJson<unknown>(residentWakeRequestPath(this.config.residencyRoot)) }, { durable: true });
      }
      this.#ready = false;
      this.actors.pauseForRelease();
      this.control.pause();
      this.lifecycle.pause();
      await this.control.checkpointForRelease();
      await this.lifecycle.checkpointForRelease();
      await this.#backgroundDeliveries.checkpointForRelease();
      await this.actors.checkpointForRelease();
      if (this.#closed || this.#hasHostWork() || this.actors.hasActiveDurableActor() ||
          !this.actors.meshCaughtUp() || (this.actors.listOwned().some(actor =>
            actor.residency === "durable" && actor.status !== "stopped") && !this.#hasWakeConfig()) ||
          [this.#requestsPath, this.#processingPath].some(directory =>
            fs.readdirSync(directory).some(entry => entry.endsWith(".json")))) throw new Error("Resident acquired work before dormancy");
      this.onIdle(this.actors.listOwned().some(actor =>
        actor.residency === "durable" && actor.status !== "stopped") ? "dormant" : "idle-exit");
    } catch {
      this.#sleeping = false;
      fs.rmSync(residentSleepingPath(this.config.residencyRoot), { force: true });
      if (!this.#closed) {
        this.#ready = true;
        this.actors.resumeAfterRelease();
        this.control.resume();
        this.lifecycle.resume();
        this.#idleSince = Date.now();
        this.#scheduleIdleCheck();
      }
    }
  }

  // Re-armed on every idle check: the due time of the earliest pending
  // schedule, released here even if no actor monitor polls in time.
  #armScheduleWake(wakeAt: number | undefined): void {
    if (this.#closed || this.#drain || this.#staged || this.#handover) wakeAt = undefined;
    if (wakeAt === this.#scheduleWakeAt) return;
    if (this.#scheduleTimer) clearTimeout(this.#scheduleTimer);
    this.#scheduleTimer = undefined;
    this.#scheduleWakeAt = wakeAt;
    if (wakeAt === undefined || this.#closed || this.#drain || this.#staged || this.#handover) return;
    this.#scheduleTimer = setTimeout(() => {
      this.#scheduleTimer = undefined;
      this.#scheduleWakeAt = undefined;
      if (this.#closed || this.#drain || this.#staged || this.#handover) return;
      this.#admissions++;
      const released = this.mesh.releaseDueSchedules().finally(() => { this.#admissions--; });
      this.#trackPublication(released);
      void released.catch(() => undefined).finally(() => { if (!this.#closed) void this.#checkIdle(); });
    }, Math.min(Math.max(0, wakeAt - Date.now()), 2_147_000_000));
    this.#scheduleTimer.unref();
  }

  #trackPublication(promise: Promise<unknown>): void {
    this.#publications.add(promise);
    this.#scheduleIdleCheck();
    void promise.then(() => {
      this.#publications.delete(promise);
      this.#scheduleIdleCheck();
      this.#noteReleaseProgress();
    }, () => {
      this.#publicationFailed = true;
      this.#publications.delete(promise);
      this.#scheduleIdleCheck();
      this.#noteReleaseProgress();
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

  #drainActors(): ResidentDrainActors {
    const actors = this.actors as unknown as ResidentDrainActors;
    if (typeof actors.releaseCheckpointReceipt !== "function") {
      throw new ResidentDrainRefusedError("CHECKPOINT_RECEIPT_UNAVAILABLE", "Resident actor checkpoint readback capability is unavailable; retain A");
    }
    return actors;
  }

  #drainAgents(): ResidentDrainAgents {
    const agents = this.agents as unknown as ResidentDrainAgents;
    if (typeof agents.pauseForRelease !== "function" || typeof agents.resumeAfterRelease !== "function") {
      throw new ResidentDrainRefusedError("DRAIN_ADMISSION_PROTOCOL_UNAVAILABLE", "Resident agent admission hold capability is unavailable; retain A");
    }
    return agents;
  }

  #drainSupported(): boolean {
    const launch = this.launch;
    if (!launch || process.platform !== "linux" || !exactResidentProcess(launch.launcher) ||
        launch.spec.config.rootId !== this.config.rootId || launch.spec.config.sessionId !== this.config.sessionId ||
        launch.spec.config.residencyRoot !== this.config.residencyRoot ||
        launch.spec.config.fabricExtensionPath !== this.config.fabricExtensionPath ||
        launch.spec.config.workerPath !== this.config.workerPath || !launch.launcher.token) return false;
    const { digest, ...value } = launch.spec;
    if (specDigest(value) !== digest) return false;
    try { this.#drainActors(); this.#drainAgents(); return true; } catch { return false; }
  }

  async #publishStagedReady(owner: ResidentHostOwner): Promise<void> {
    const launch = this.launch!;
    const deadline = Date.now() + HANDOVER_DRAIN_MS;
    const checkpoint = new AbortController();
    const timer = setTimeout(() => checkpoint.abort(new Error("Staged resident checkpoint timed out")), HANDOVER_DRAIN_MS);
    try {
      // Startup probe is maintenance, never business admission. All four gates stay held.
      const assertHeld = (): void => {
        if (checkpoint.signal.aborted || Date.now() >= deadline || !this.#staged || this.#closed) throw new Error("Staged checkpoint invalidated");
      };
      const valid = (): boolean => { try { assertHeld(); return true; } catch { return false; } };
      await this.#boundedCheckpoint(() => (this.control as unknown as ResidentDrainIngress).checkpointForRelease(valid), deadline, checkpoint.signal);
      await this.#boundedCheckpoint(() => (this.lifecycle as unknown as ResidentDrainIngress).checkpointForRelease(valid), deadline, checkpoint.signal);
      await this.#boundedCheckpoint(() => this.#drainActors().checkpointForRelease(assertHeld), deadline, checkpoint.signal);
      await this.#boundedCheckpoint(() => this.#drainAgents().checkpointForRelease(deadline, valid), deadline, checkpoint.signal);
      assertHeld();
      if (this.#businessAdmissions !== 0 || !owner.processStartTime || checkpoint.signal.aborted) {
        throw new Error("Staged resident cannot prove zero business admission");
      }
      const receipt: ResidentStagedReadyReceipt = { abi: RESIDENT_HANDOVER_ABI, id: launch.attempt!.id,
        rootId: this.config.rootId, owner: { pid: owner.pid, processStartTime: owner.processStartTime,
          token: owner.token, hostId: owner.hostId }, launcher: launch.launcher, kind: launch.attempt!.kind,
        digest: launch.spec.digest, businessAdmissions: 0, admissionHeld: true, checkpointReadback: true,
        checkpoint: this.#releaseCheckpointReadback(), at: Date.now() };
      writeResidentStagedReadyReceipt(this.config.residencyRoot, receipt);
      this.#stagedReady = receipt;
    } finally { clearTimeout(timer); checkpoint.abort(new Error("Staged checkpoint complete")); }
  }

  #assertDrainChannelOwned(command: Extract<ResidentCommand, { operation: "releaseDrain" }>): void {
    const uid = process.getuid?.();
    if (process.platform !== "linux" || uid === undefined) throw new ResidentDrainRefusedError("RESIDENT_CHANNEL_UNOWNED", "Resident cannot verify same-user drain channel ownership");
    const directories = [...new Set([this.config.meshRoot, path.dirname(this.config.meshRoot), this.config.residencyRoot,
      path.dirname(this.config.residencyRoot), this.#requestsPath, this.#processingPath, this.#responsesPath,
      path.join(this.config.meshRoot, "participants"), path.dirname(hostLeasePath(this.config.meshRoot, command.caller?.hostId ?? ""))])];
    const files = [this.#ownerPath, path.join(this.config.residencyRoot, "config.json"),
      path.join(this.config.residencyRoot, "main-generation.json"), path.join(this.#processingPath, `${command.requestId}.json`),
      hostLeasePath(this.config.meshRoot, command.caller?.hostId ?? "")];
    for (const file of [...directories, ...files]) {
      const stat = fs.lstatSync(file);
      if (stat.isSymbolicLink() || stat.uid !== uid || (stat.mode & 0o022) !== 0 ||
          (directories.includes(file) ? !stat.isDirectory() : !stat.isFile()) || fs.realpathSync(file) !== path.resolve(file)) {
        throw new ResidentDrainRefusedError("RESIDENT_CHANNEL_UNOWNED", `Resident drain channel is not exclusively owned by this OS user: ${file}`);
      }
    }
  }

  #prepareDrain(command: Extract<ResidentCommand, { operation: "releaseDrain" }>): ResidentDrainReceipt {
    this.#authorizeResidentSetter(command.caller);
    this.#assertDrainChannelOwned(command);
    const launch = this.launch;
    const owner = readHandoverJson<ResidentHostOwner>(this.#ownerPath);
    const refuse = (code: string, reason: string): never => { throw new ResidentDrainRefusedError(code, reason); };
    // The expected loaded release is checked BEFORE commit, snapshots, pause or any checkpoint.
    if (!command.expected || !launch || owner?.releaseRoot !== command.expected.releaseRoot ||
        launch.spec.releaseRoot !== command.expected.releaseRoot || owner.fabricExtensionPath !== launch.spec.config.fabricExtensionPath) {
      return refuse("EXPECTED_RELEASE_MISMATCH", "Expected release does not match the loaded resident owner; nothing was held");
    }
    if (!owner.processStartTime || !sameDrainOwner(owner as ResidentDrainReceipt["owner"], command.expected.owner) ||
        owner.pid !== process.pid || owner.token !== this.#token || owner.hostId !== this.hostId || !exactResidentProcess({ pid: owner.pid, processStartTime: owner.processStartTime })) {
      return refuse("RESIDENT_OWNER_MISMATCH", "Resident owner birth/token fence is uncertain");
    }
    if (!owner.handover || !sameDrainLauncher(owner.handover.launcher, command.expected.launcher) ||
        !sameDrainLauncher(launch.launcher, command.expected.launcher) || !exactResidentProcess(launch.launcher)) {
      return refuse("LAUNCHER_IDENTITY_MISMATCH", "Resident launcher birth/token fence changed");
    }
    if (owner.configDigest !== command.expected.configDigest || owner.configDigest !== launch.spec.digest) {
      return refuse("CONFIG_DIGEST_MISMATCH", "Expected resident config digest changed");
    }
    if (!this.#drainSupported() || owner.drain?.abi !== RESIDENT_DRAIN_ABI) {
      return refuse("DRAIN_ADMISSION_PROTOCOL_UNAVAILABLE", "Loaded resident cannot attest a coherent cooperative drain boundary");
    }
    if (this.#closed || this.#staged || this.#handover || this.#drain || this.#drainCheckpointPending || this.#checkpointJoins.size || this.#sleeping || this.#idleExitRequested) {
      return refuse("DRAIN_BUSY", "Another resident boundary is unsettled; retain A");
    }
    const root = this.participants.get(this.config.rootId, Date.now(), { fresh: true });
    if (!mainGenerationCurrent(this.config.residencyRoot, command.main) || command.main.rootId !== this.config.rootId ||
        command.main.sessionId !== this.config.sessionId || ![launch.spec.releaseRoot, command.target.releaseRoot].includes(command.main.releaseRoot) ||
        root?.mainProcess?.pid !== command.main.pid || root.mainProcess.startTime !== command.main.processStartTime ||
        root.mainProcess.host !== os.hostname()) {
      throw new ResidentActorAuthorizationError("Obsolete owning Main drain intent");
    }
    const deadlineAt = residentDrainDeadline(command.timeoutMs);
    validateLaunchSpec(command.target);
    const previous = residentLaunchSpec(this.#effectiveConfig?.() ?? this.config, launch.spec.entry, launch.spec.runtime);
    assertPreviousLaunchSpec(launch.spec, previous);
    if (previous.digest !== command.expected.configDigest) return refuse("CONFIG_DIGEST_MISMATCH", "Loaded resident policy/config snapshot changed");
    assertHandoverTopology(previous, command.target);
    if (previous.releaseRoot === command.target.releaseRoot) return refuse("TARGET_IS_CURRENT_RELEASE", "Drain target must be a different retained release");
    if (handoverActive(readHandoverJson<ResidentHandoverState>(handoverPath(this.config.residencyRoot)))) {
      return refuse("DRAIN_BUSY", "A resident handover is in custody");
    }
    if (Date.now() >= deadlineAt) return refuse("DRAIN_TIMEOUT", "Drain deadline elapsed during read-only validation; nothing was held");
    const receipt = newResidentDrainReceipt({ rootId: this.config.rootId, owner: command.expected.owner,
      launcher: launch.launcher, previous, target: command.target, main: command.main, phase: "held", outcome: "held",
      code: "DRAIN_HELD", reason: "Resident admissions held; accepted work may settle naturally", at: Date.now(),
      deadlineAt, admissionHeld: true, terminal: false });
    commitResidentRequest(this.config.residencyRoot, command, receipt.id, this.hostId);
    let resolveResumed!: () => void;
    const resumed = new Promise<void>(resolve => { resolveResumed = resolve; });
    const checkpointAbort = new AbortController();
    const timer = setTimeout(() => {
      if (this.#drain === held) this.#finishDrainTimeout(held);
    }, Math.max(0, deadlineAt - Date.now()));
    const held: ResidentHeldDrain = { receipt, caller: structuredClone(command.caller), checkpointAbort, timer, resumed, resolveResumed };
    // Single synchronous boundary: no await between resident state and manager gates.
    this.#drain = held;
    try {
      this.#drainActors().pauseForRelease({ strict: true }); this.#drainAgents().pauseForRelease(true);
      (this.control as unknown as ResidentDrainIngress).pause(true); (this.lifecycle as unknown as ResidentDrainIngress).pause(true);
      this.#armScheduleWake(undefined);
      this.#scheduleMaintenance();
      writeResidentDrainReceipt(this.config.residencyRoot, receipt);
      return receipt;
    } catch (error) {
      this.#finishDrain(held, "needs-operator", "DRAIN_HOLD_FAILED", errorMessage(error));
      throw error;
    }
  }

  #drainStillCurrent(held: ResidentHeldDrain): void {
    if (this.#drain !== held || held.checkpointAbort.signal.aborted || Date.now() >= held.receipt.deadlineAt) throw new Error("Resident drain checkpoint expired");
    this.#authorizeResidentSetter(held.caller);
    const root = this.participants.get(this.config.rootId, Date.now(), { fresh: true });
    if (root?.mainProcess?.pid !== held.receipt.main.pid || root.mainProcess.startTime !== held.receipt.main.processStartTime ||
        root.mainProcess.host !== os.hostname()) throw new ResidentActorAuthorizationError("Owning Main process identity changed during drain");
    if (!mainGenerationCurrent(this.config.residencyRoot, held.receipt.main) || !exactResidentProcess(held.receipt.launcher)) {
      throw new Error("Owning Main/launcher lost during cooperative drain");
    }
    const owner = readHandoverJson<ResidentHostOwner>(this.#ownerPath);
    if (!owner || !sameDrainOwner(owner as ResidentDrainReceipt["owner"], held.receipt.owner) ||
        owner.releaseRoot !== held.receipt.previous.releaseRoot || owner.configDigest !== held.receipt.previous.digest ||
        !owner.handover || !sameDrainLauncher(owner.handover.launcher, held.receipt.launcher)) throw new Error("Resident drain owner binding changed");
    if (JSON.stringify(readResidentDrainReceipt(this.config.residencyRoot, held.receipt.id, held.receipt)) !== JSON.stringify(held.receipt)) {
      throw new Error("Resident held drain receipt changed");
    }
  }

  #finishDrainTimeout(held: ResidentHeldDrain): void {
    this.#finishDrain(held, "resumed", "DRAIN_TIMEOUT", "Drain deadline elapsed; A resumed without aborting accepted work");
  }

  #finishDrain(held: ResidentHeldDrain, outcome: "resumed" | "containment-unavailable" | "needs-operator", code: string, reason: string): void {
    if (this.#drain !== held) return;
    clearTimeout(held.timer);
    // Cancels checkpoint validity ONLY. Never pass this signal to a worker/accepted handler.
    held.checkpointAbort.abort(new Error(reason));
    this.#drain = undefined;
    let resumeFailed = false;
    if (!this.#closed) {
      for (const resume of [() => this.actors.resumeAfterRelease(), () => this.#drainAgents().resumeAfterRelease(),
        () => this.control.resume(), () => this.lifecycle.resume()]) {
        try { resume(); }
        catch (error) { resumeFailed = true; reason += `; resume failed: ${errorMessage(error)}`; }
      }
    }
    if (resumeFailed) { outcome = "needs-operator"; code = "DRAIN_RESUME_UNCERTAIN"; }
    held.resolveResumed();
    const terminalAt = Date.now();
    held.receipt = { ...held.receipt, phase: outcome === "needs-operator" ? "needs-operator" : held.receipt.phase,
      outcome, code, reason, at: terminalAt, ...(!this.#closed && !resumeFailed ? { resumedAt: terminalAt } : {}),
      admissionHeld: this.#closed || resumeFailed, terminal: true };
    try { writeResidentDrainReceipt(this.config.residencyRoot, held.receipt); }
    catch (error) { diagnostics.error("src/residency/host.ts", `[pi-fabric] resident drain terminal receipt unavailable: ${errorMessage(error)}`); }
    this.#idleSince = Date.now();
    if (!this.#closed) {
      try { this.#armScheduleWake(this.mesh.nextScheduleDueAt()); } catch { /* durable schedule remains upstream */ }
      this.#runMaintenanceSoon();
      this.#kickRequests(); // Held request events were consumed; no interval remains to rediscover them.
    }
    this.#scheduleIdleCheck();
  }

  async #boundedCheckpoint(checkpoint: () => Promise<void>, deadlineAt: number, signal: AbortSignal): Promise<void> {
    if (signal.aborted || Date.now() >= deadlineAt) throw new Error("Resident checkpoint deadline elapsed");
    let onAbort!: () => void;
    const aborted = new Promise<never>((_, reject) => {
      onAbort = () => reject(signal.reason ?? new Error("Resident checkpoint invalidated"));
      signal.addEventListener("abort", onAbort, { once: true });
    });
    // Observe the losing promise too; manager epoch checks prevent late persistence after resume.
    const pending = Promise.resolve().then(() => {
      if (signal.aborted || Date.now() >= deadlineAt) throw new Error("Resident checkpoint invalidated before entry");
      return checkpoint();
    });
    this.#checkpointJoins.add(pending);
    void pending.finally(() => this.#checkpointJoins.delete(pending)).catch(() => undefined);
    try { await Promise.race([pending, aborted]); }
    finally { signal.removeEventListener("abort", onAbort); }
  }

  async #advanceDrain(held: ResidentHeldDrain): Promise<void> {
    if (this.#pollingRequests || this.#drainCheckpointPending) return;
    try {
      this.#drainStillCurrent(held);
      if (this.#admissions || this.#boundaryRequests.size || this.actors.releaseCustodyCount() || this.actors.pendingRemovals().length ||
          residentProcessWorkPending(this.processWorkSessionId) || this.#publications.size || this.#flushingDeliveries ||
          this.agents.listForUi().some(agent => agent.status === "queued" || agent.status === "running") ||
          fs.readdirSync(this.#processingPath).some(entry => entry.endsWith(".json"))) return;
      const pending = this.#checkpointDrain(held);
      this.#drainCheckpointPending = pending;
      try { await pending; }
      finally { if (this.#drainCheckpointPending === pending) this.#drainCheckpointPending = undefined; }
    } catch (error) {
      if (this.#drain === held) {
        // The wall deadline owns terminal timeout classification even when this
        // continuation wins the event edge just before the setTimeout callback.
        if (Date.now() >= held.receipt.deadlineAt) this.#finishDrainTimeout(held);
        else this.#finishDrain(held, "resumed", error instanceof ResidentDrainRefusedError ? error.code : "DRAIN_UNCERTAIN", errorMessage(error));
      }
    }
  }

  async #checkpointDrain(held: ResidentHeldDrain): Promise<void> {
    const { deadlineAt } = held.receipt;
    const signal = held.checkpointAbort.signal;
    const assertHeld = (): void => this.#drainStillCurrent(held);
    const valid = (): boolean => { try { assertHeld(); return true; } catch { return false; } };
    const step = async (callback: () => Promise<void>): Promise<void> => {
      this.#drainStillCurrent(held);
      await this.#boundedCheckpoint(callback, deadlineAt, signal);
      this.#drainStillCurrent(held);
    };
    await step(() => (this.control as unknown as ResidentDrainIngress).checkpointForRelease(valid));
    await step(() => (this.lifecycle as unknown as ResidentDrainIngress).checkpointForRelease(valid));
    await step(() => this.#backgroundDeliveries.checkpointForRelease());
    await step(async () => {
      await Promise.all([...this.#publications]); assertHeld();
      await this.#flushingDeliveries; assertHeld();
      // Normal producer replay owns its own durable handoff; do not leave an
      // unguarded checkpoint flusher running across a timed-out resume.
      if (fs.existsSync(this.#deliveryOutboxPath) && fs.readdirSync(this.#deliveryOutboxPath).some(entry => entry.endsWith(".json"))) {
        throw new Error("Resident producer outbox has pending handoff obligations");
      }
    });
    if (this.#publicationFailed || (fs.existsSync(this.#deliveryOutboxPath) && fs.readdirSync(this.#deliveryOutboxPath).some(entry => entry.endsWith(".json")))) {
      throw new Error("Resident drain has unconfirmed producer/publication obligations");
    }
    await step(() => this.#drainActors().checkpointForRelease(assertHeld));
    await step(() => this.#drainAgents().checkpointForRelease(deadlineAt, valid));
    const checkpoint = this.#releaseCheckpointReadback();
    this.#drainStillCurrent(held);
    validateLaunchSpec(held.receipt.previous); validateLaunchSpec(held.receipt.target);
    this.#drainStillCurrent(held);
    // Quiet is evidence after real synchronous gates and joins, never a replacement admission boundary.
    if (this.#admissions || this.#boundaryRequests.size || this.#publications.size || this.#flushingDeliveries ||
        this.actors.releaseCustodyCount() || this.actors.pendingRemovals().length ||
        this.agents.listForUi().some(agent => agent.status === "queued" || agent.status === "running")) throw new Error("Resident drain quiet changed during checkpoint");
    held.receipt = { ...held.receipt, phase: "checkpointed", outcome: "checkpointed", code: "DRAIN_CHECKPOINTED",
      reason: "Held queues, registry and receipt obligations checkpointed and read back", at: Date.now(),
      checkpoint, checkpointReadback: true, quiet: { admissions: 0, actors: 0, agents: 0, removals: 0,
        publications: 0, producerOutbox: 0, boundaryRequests: 0, businessAdmissions: this.#businessAdmissions } };
    writeResidentDrainReceipt(this.config.residencyRoot, held.receipt);
    // Drain operation IDs are NOT launch attempt IDs. Only a trusted launch
    // attestation may select the launcher-custody channel; no scope path is opened.
    await step(async () => {
      try {
        const attestation = this.#trustedLaunchAttestation;
        if (!attestation?.attemptId || attestation.launchedInScope !== true) {
          throw new ContainmentUnavailableError("Current resident has no launcher-issued launch attestation; retain A");
        }
        // ENV alone and mutable owner.json are not launch authority. Recheck the
        // launcher's immutable record; IPC independently binds every receipt field.
        readResidentLaunchAttestation(this.config.residencyRoot, attestation);
        const owner = readHandoverJson<ResidentHostOwner>(this.#ownerPath);
        if (!owner || !sameDrainOwner(owner as ResidentDrainReceipt["owner"], held.receipt.owner) ||
            JSON.stringify(owner.launchAttestation) !== JSON.stringify(attestation)) {
          throw new ContainmentUnavailableError("Current owner generation has no matching launcher launch attestation; retain A");
        }
        const membership = await (this.#containmentClient ?? this.containment).attemptMembership(attestation.attemptId);
        if (membership.attemptId !== attestation.attemptId || membership.scope !== attestation.scope) {
          throw new Error("Launcher membership reply is not bound to the current launch attempt");
        }
      } catch (error) { if (!(error instanceof ContainmentUnavailableError)) throw error; }
    });
    // Spec 5/7 are still absent. NEVER publish custody, exit A, or start B here.
    try { assertAutomaticReleaseRecovery(); }
    catch (error) { this.#finishDrain(held, "containment-unavailable", "CONTAINMENT_UNAVAILABLE", errorMessage(error)); return; }
    this.#finishDrain(held, "containment-unavailable", "CONTAINMENT_UNAVAILABLE", "Cooperative endpoint does not own containment or release movement");
  }

  #releaseCheckpointReadback(): unknown {
    const receipts: Record<string, unknown> = { actors: this.#drainActors().releaseCheckpointReceipt() };
    for (const [name, manager] of [["agents", this.agents], ["control", this.control], ["lifecycle", this.lifecycle]] as const) {
      const readback = (manager as unknown as { releaseCheckpointReceipt?: () => unknown }).releaseCheckpointReceipt;
      if (typeof readback !== "function") throw new ResidentDrainRefusedError("CHECKPOINT_RECEIPT_UNAVAILABLE", `${name} durable checkpoint readback is unavailable; no complete quiet claim; retain A`);
      receipts[name] = readback.call(manager);
    }
    return receipts;
  }

  #prepareRelease(command: Extract<ResidentCommand, { operation: "releaseChange" }>): void {
    this.#authorizeResidentSetter(command.caller);
    if (this.#drain) throw new Error("Cooperative resident drain already holds admissions");
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
    this.#armScheduleWake(undefined);
    this.#scheduleMaintenance();
  }

  #cancelRelease(plan: ResidentHandoverPlan, reason: string): void {
    if (decideHandover(this.config.residencyRoot, { id: plan.id, state: "cancelled" }).state === "custody") return;
    writeHandoverState(this.config.residencyRoot, plan, "cancelled", reason);
    this.#handover = undefined;
    this.actors.resumeAfterRelease(); this.control.resume(); this.lifecycle.resume();
    this.#runMaintenanceSoon();
    this.#scheduleIdleCheck();
    this.#kickRequests();
  }

  async #advanceRelease(): Promise<void> {
    if (this.#advancingRelease || this.#closed) return;
    this.#advancingRelease = true;
    try {
      if (this.#drain) { await this.#advanceDrain(this.#drain); return; }
      if (this.#staged) {
        const state = readHandoverJson<ResidentHandoverState>(handoverPath(this.config.residencyRoot));
        const attempt = this.launch?.attempt;
        if (!state || state.plan.id !== attempt?.id) throw new Error("Staged resident transaction identity changed");
        const terminal = attempt.kind === "target" ? "complete" : "fallback";
        if (state.phase !== terminal) return;
        // Staged readiness is not containment. No forged terminal state opens B.
        assertAutomaticReleaseRecovery();
        if (!this.#stagedReady || this.#businessAdmissions !== 0) throw new Error("Staged resident admitted business work");
        assertResidentStagedReady(this.config.residencyRoot, this.#stagedReady);
        if (JSON.stringify(this.#releaseCheckpointReadback()) !== JSON.stringify(this.#stagedReady.checkpoint)) {
          throw new Error("Staged resident checkpoint readback changed before admission");
        }
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
        this.actors.resumeAfterRelease(); this.#drainAgents().resumeAfterRelease(); this.control.resume(); this.lifecycle.resume();
        this.#runMaintenanceSoon();
        this.#scheduleIdleCheck();
        this.#kickRequests();
        this.#trackPublication(this.#backgroundDeliveries.enqueue(async () => {
          await this.actors.finishPendingRemovals();
          this.#writeRemovals();
        }));
        return;
      }
      const plan = this.#handover;
      // Released once: our own handover.json write must not feed back into another pass (smarty-dev#11172).
      if (!plan || this.#pollingRequests || this.#releasedPlan === plan.id) return;
      const custody = readHandoverJson<{ id: string; launcher: ResidentLauncherIdentity }>(handoverCustodyPath(this.config.residencyRoot, plan.id));
      if (custody) {
        assertAutomaticReleaseRecovery();
        if (custody.id !== plan.id || JSON.stringify(custody.launcher) !== JSON.stringify(plan.launcher) ||
            !exactResidentProcess(plan.launcher)) throw new Error("Resident launcher custody is uncertain");
        // Receipt precedes release of A's stable flock. The Main is no longer the executor.
        writeHandoverState(this.config.residencyRoot, plan, "released");
        this.#releasedPlan = plan.id;
        this.onIdle("handover-release");
        return;
      }
      if (!mainGenerationCurrent(this.config.residencyRoot, plan.main) || !exactResidentProcess(plan.launcher)) {
        this.#cancelRelease(plan, "Main/launcher lost before custody"); return;
      }
      if (Date.now() - plan.createdAt > HANDOVER_DRAIN_MS) {
        this.#cancelRelease(plan, "Release drain timed out without cutting a run"); return;
      }
      if (fs.readdirSync(this.#processingPath).some((entry) => entry.endsWith(".json"))) return;
      if (this.#admissions || this.actors.releaseCustodyCount() || this.actors.pendingRemovals().length ||
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
          ? { errorCode: error.code } : {}), ...residentSlotErrorFields(error), completedAt: Date.now() };
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
    // Actor events during create/setters preceded the durable response and
    // processing-file release. Recheck when this exchange actually settles.
    this.#scheduleIdleCheck();
    this.#noteReleaseProgress(); // processing/ drained: a pending release re-checks at once
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
    // The durable native ledger, not this bounded host-local receipt cache, owns
    // protected retries and immutable-spec conflicts across resident generations.
    if (command.operation === "createActor") {
      const slots = slotAdmissionFor(this.mesh);
      if (slots.requiresEnforcement(command.request.topics ?? [], residentActorRoots(this.config)[command.request.scope ?? this.config.mesh.actorScope], this.config.rootId)) {
        return this.#executeRequest(command, boundaryAdmitted);
      }
    }
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
    if (command.operation === "createActor" && command.caller) this.#authorizeResidentSetter(command.caller);
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

  async #routerAdmission(command: ResidentCommand): Promise<SpawnRouterAdmission | undefined> {
    if (command.operation !== "spawnBound" && command.operation !== "createActor") return undefined;
    const decisionId = (command.request as { spawnRouterDecisionId?: unknown }).spawnRouterDecisionId;
    if (typeof decisionId !== "string" || !decisionId) return undefined;
    try {
      const caller = command.operation === "spawnBound" ? command.caller : command.routerCaller;
      const participant = caller && this.participants.get(caller.id, Date.now(), { fresh: true });
      const { authenticatedSpawnRouterAdmission } = await import("../agents/spawn-router.js");
      return authenticatedSpawnRouterAdmission(this.mesh, caller, participant, this.config.rootId, this.hostId,
        command.operation === "spawnBound" ? "spawn" : "actor");
    } catch { return undefined; } // Attribution never supplies launch authority or a new launch veto.
  }

  async #executeRequest(command: ResidentCommand, boundaryAdmitted?: () => void): Promise<ResidentCommandResponse> {
    const requestId = command.requestId;
    let response: ResidentCommandResponse;
    const routerAdmission = await this.#routerAdmission(command);
    let routerHandedOff = false;
    try {
      await testResidentRequestDelay("before_commit");
      if (command.operation !== "releaseDrain") this.#businessAdmissions++;
      const commit = (id: string): void => commitResidentRequest(this.config.residencyRoot, command, id, this.hostId);
      if (command.operation === "releaseDrain") {
        const drain = this.#prepareDrain(command);
        response = { format: RESIDENT_HOST_FORMAT, requestId, ok: true, drain, completedAt: Date.now() };
      } else if (command.operation === "releaseChange") {
        this.#prepareRelease(command);
        response = { format: RESIDENT_HOST_FORMAT, requestId, ok: true, completedAt: Date.now() };
      } else if (command.operation === "spawnBound") {
        if (
          command.request.residentStartupProbe ||
          command.request.sessionSeed ||
          command.request.forkSeed ||
          command.request.sessionFile ||
          command.request.actorId ||
          command.request.actorName ||
          command.request.meshRoot ||
          command.request.runnerSessionId ||
          command.request.images
        ) {
          throw new Error("Durable agents.spawn accepts only its public task and run settings");
        }
        if (command.runnerModule) await loadRunnerModule(command.runnerModule);
        // No executor fallback: revalidate the captured caller before the mutation fence.
        const caller = command.caller;
        const returnAddress = assertResidentTaskCaller(caller,
          caller && this.participants.get(caller.id, Date.now(), { fresh: true }), this.config.rootId);
        routerHandedOff = true;
        const handle = await this.agents.spawn({ ...command.request, residency: "durable" }, undefined, undefined, commit, undefined, undefined, undefined, returnAddress, routerAdmission);
        // spawn already validated the forwarded scope; record what the child received.
        const scope = launchScope(command.request.scope, command.request.inheritedScope);
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
          ...(command.runnerModule ? { runnerModule: command.runnerModule } : {}),
          ...(scope ? { scope } : {}),
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
        const slots = slotAdmissionFor(this.mesh);
        const protectedCreate = slots.requiresEnforcement(command.request.topics ?? [], residentActorRoots(this.config)[command.request.scope ?? this.config.mesh.actorScope], this.config.rootId);
        if (protectedCreate) this.#authorizeResidentSetter(command.caller);
        const { instructionsFile: _file, sha256: _digest, idempotencyKey: _requestKey, ...base } = command.request;
        const instructions = resolveActorInstructions(command.request, this.config.agents.instructionsRoot);
        routerHandedOff = true;
        const actor = await this.actors.create({ ...base, instructions,
          ...(command.idempotencyKey === undefined ? {} : { idempotencyKey: command.idempotencyKey }),
        }, {
          asRegistryOwner: true,
          ...(routerAdmission ? { routerAdmission } : {}),
          ...(command.caller ? { supervisorOwner: command.caller.identity } : {}),
          beforeCommit: id => {
            if (protectedCreate || command.caller) this.#authorizeResidentSetter(command.caller);
            commit(id);
          },
        });
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
        // smarty-dev#11105: an exact-id remove of this root's durable actor whose commit is in flight joins
        // that commit (no preliminary stop: its run already ended). Never for stop, dry-run or an alias.
        const finishing = candidates.length === 0 && command.action === "remove" && command.dryRun !== true
          ? this.actors.finishingRemoval(command.id) : undefined;
        // #10189: exact-id operator remove may finish this root's cleanup of any residency.
        // The row is revoked: no stop, alias resolution or dry-run mutation.
        const legacy = candidates.length === 0 && command.action === "remove" && command.dryRun !== true && !finishing
          ? this.actors.cleanupObligation(command.id) : undefined;
        if (finishing && finishing.rootId === this.config.rootId && finishing.residency === "durable") {
          // Same lease veto, then this request's commit, as the stop-first path's callback.
          const pending = this.#removeResidentActor(finishing.id, requestId, id => { check(); commit(id); }, true);
          boundaryAdmitted?.();
          response = await pending;
        } else if (legacy && legacy.id === command.id && legacy.rootId === this.config.rootId) {
          const pending = this.#removeResidentActor(legacy.id, requestId, id => { check(); commit(id); }, true);
          boundaryAdmitted?.();
          response = await pending;
        } else if (candidates.length !== 1) {
          throw new Error(candidates.length ? `Ambiguous resident actor: ${command.id}` : `Unknown Fabric actor: ${command.id}`);
        } else if (command.dryRun === true) {
          const actor = candidates[0]!;
          response = { format: RESIDENT_HOST_FORMAT, requestId, ok: true, actor, operatorEvidence: evidence, completedAt: Date.now() };
        } else {
          // Re-read the uncached current lease immediately before each mutation.
          // This is a lease veto, not a proof that no Main exists or can restart.
          const actor = candidates[0]!;
          const pending = this.actors.stop(actor.id, id => { check(); commit(id); }, true);
          boundaryAdmitted?.();
          const stopped = await pending;
          response = command.action === "stop"
            ? { format: RESIDENT_HOST_FORMAT, requestId, ok: true, actor: stopped, completedAt: Date.now() }
            : await this.#removeResidentActor(actor.id, requestId, () => check(), true);
        }
      } else if (command.operation === "slotStatus") {
        response = { format: RESIDENT_HOST_FORMAT, requestId, ok: true,
          slotStatus: slotAdmissionFor(this.mesh).status(), completedAt: Date.now() };
      } else if (command.operation === "actors") {
        response = {
          format: RESIDENT_HOST_FORMAT, requestId, ok: true,
          actors: this.actors.listOwned().filter((actor) => actor.rootId === this.config.rootId),
          completedAt: Date.now(),
        };
      } else if (command.operation !== "removeActor") {
        if (command.operation === "setInstructions" || command.operation === "setModel" ||
          command.operation === "setThinking" || command.operation === "setBatching" || command.operation === "setActivationFilter" || command.operation === "setTools" ||
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
          case "setBatching": updated = await this.actors.setBatching(actor.id, command, commit); break;
          case "setActivationFilter": updated = await this.actors.setActivationFilter(actor.id, command.activationFilter, commit, command.expiresAt); break;
          case "setTools": updated = await this.actors.setTools(actor.id, command.tools, commit); break;
          default: throw new Error("Unknown resident actor operation");
        }
        response = { format: RESIDENT_HOST_FORMAT, requestId, ok: true, actor: updated, completedAt: Date.now() };
      } else {
        const pending = this.#removeResidentActor(command.id, requestId, commit);
        boundaryAdmitted?.();
        response = await pending;
      }
    } catch (error) {
      if (!routerHandedOff && (command.operation === "spawnBound" || command.operation === "createActor")) {
        const decisionId = (command.request as { spawnRouterDecisionId?: unknown }).spawnRouterDecisionId;
        if (typeof decisionId === "string" && decisionId) {
          try {
            const { recordSpawnRouterStartFailed } = await import("../agents/spawn-router.js");
            await recordSpawnRouterStartFailed({ meshRoot: this.mesh.root, decisionId, admission: routerAdmission,
              spawnKind: command.operation === "spawnBound" ? "spawn" : "actor", request: command.request, cwd: this.config.cwd });
          } catch { /* Attribution is advisory, not a replacement for the native error. */ }
        }
      }
      const drain = command.operation === "releaseDrain" ? this.#drainRefusal(command, error) : undefined;
      let slotFailure = residentSlotErrorFields(error);
      if (command.operation === "createActor" && (residentCreateMayBeProtected(command) ||
          slotAdmissionFor(this.mesh).isProtected(command.request.topics ?? []))) {
        // Acceptance is not durable birth. Once its CAS wins, SQL/sync/projection
        // failures are uncertainty even when the intent or response is missing.
        let decision;
        try { decision = readResidentRequestDecision(this.config.residencyRoot, requestId); } catch { /* retain native error */ }
        if (decision?.state === "committed") {
          slotFailure = { errorCode: "FABRIC_ACTOR_SLOT_CREATE_UNCERTAIN", slotError: {
            ...slotFailure.slotError,
            ...((slotFailure.slotError?.actorId ?? decision.id) ? { actorId: (slotFailure.slotError?.actorId ?? decision.id)! } : {}),
            ...(command.request.slotClaim ? { claimKey: command.request.slotClaim.key,
              claimVersion: command.request.slotClaim.ifVersion } : {}),
            ...(command.idempotencyKey ? { requestKey: command.idempotencyKey } : {}),
          } };
        }
      }
      response = {
        format: RESIDENT_HOST_FORMAT,
        requestId,
        ok: false,
        error: errorMessage(error),
        ...(drain ? { drain } : {}),
        ...(error instanceof ResidentActorAuthorizationError || error instanceof ResidentCommandUnsupportedError || error instanceof ResidentRequestExpiredError
          ? { errorCode: error.code } : {}),
        ...(error instanceof ActorSessionResetCancelledError ? { errorCode: error.code } : {}),
        ...(error instanceof FabricModelDeniedError ? {
          errorCode: error.code, modelDenied: { model: error.model, ...(error.replacement ? { replacement: error.replacement } : {}) },
        } : {}),
        ...slotFailure,
        completedAt: Date.now(),
      };
    }
    return response;
  }

  #drainRefusal(command: Extract<ResidentCommand, { operation: "releaseDrain" }>, error: unknown): ResidentDrainReceipt | undefined {
    const owner = readJson<ResidentHostOwner>(this.#ownerPath);
    const launch = this.launch;
    if (!owner?.processStartTime || !launch || !command.target || !command.main) return undefined;
    return newResidentDrainReceipt({ rootId: this.config.rootId,
      owner: { pid: owner.pid, processStartTime: owner.processStartTime, token: owner.token, hostId: owner.hostId },
      launcher: launch.launcher, previous: launch.spec, target: command.target, main: command.main,
      phase: "refused", outcome: "refused", code: error instanceof ResidentDrainRefusedError || error instanceof ResidentActorAuthorizationError
        ? error.code : "DRAIN_REFUSED", reason: errorMessage(error), at: Date.now(), deadlineAt: Date.now(), admissionHeld: false, terminal: true });
  }

  async #removeResidentActor(id: string, requestId: string, commit: (id: string) => void, operator = false): Promise<ResidentCommandResponse> {
    const cleanup = this.actors.cleanupObligation(id);
    // Only an explicit operator request can waive durable ownership for this root's cleanup (#10189).
    // The manager rechecks eligibility and requires the host's fenced final check.
    const operatorCleanup = operator === true && cleanup !== undefined && cleanup.rootId === this.config.rootId;
    if (!operatorCleanup && (!this.actors.owns(id) || (cleanup && cleanup.residency !== "durable"))) {
      throw new Error(`Resident host does not own ${id}`);
    }
    // smarty-dev#2184 item 8: stop now and return; the removal finishes behind its run.
    commit(id);
    const removed = await this.actors.remove(id, { wait: false, ...(operator ? { operator: true as const } : {}) });
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

  /**
   * Re-import the runner modules recorded for this host's durable runs, then
   * re-attach their hosted runs from the persisted locators. A run whose
   * runner cannot be loaded settles indeterminate; nothing is re-submitted.
   */
  async #recoverHostedRuns(): Promise<void> {
    let entries: string[];
    try {
      entries = fs.readdirSync(this.#agentsPath).filter((entry) => entry.endsWith(".json"));
    } catch {
      entries = [];
    }
    const modules = new Set<string>();
    for (const entry of entries) {
      const metadata = readJson<ResidentAgentMetadata>(path.join(this.#agentsPath, entry));
      if (metadata?.rootId === this.config.rootId && typeof metadata.runnerModule === "string") {
        modules.add(metadata.runnerModule);
      }
    }
    for (const module of modules) await loadRunnerModule(module).catch(() => undefined);
    await this.agents.recoverHostedRuns().catch(() => []);
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
        outcome: "indeterminate",
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
    } catch (error) { this.#releaseLock(error); throw error; }
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
    } catch (error) { this.#releaseLock(error); throw error; }
  }

  /**
   * True only while this host provably owns its root: host.lock at its path is
   * still the inode this host locked (the establishment check), and owner.json
   * names this host (the owner fence check) or, before this host published it,
   * names no live process (a dead predecessor's record). A replaced lock or a
   * live other generation's owner.json means the root is not ours to write.
   */
  #holdsRoot(): boolean {
    if (this.#lockFd === undefined) return false;
    try {
      const locked = fs.fstatSync(this.#lockFd, { bigint: true });
      const current = fs.lstatSync(this.#lockPath, { bigint: true });
      if (current.dev !== locked.dev || current.ino !== locked.ino) return false;
    } catch { return false; }
    const owner = readJson<ResidentHostOwner>(this.#ownerPath);
    if (owner?.token === this.#token) return owner.pid === process.pid;
    return !owner || !residentProcessAlive(owner.pid, owner.processStartTime);
  }

  #releaseLock(failure = this.#failure): void {
    if (this.#lockFd === undefined) return;
    if (!(failure instanceof ResidentHostAlreadyRunning)) {
      try { this.beforeRelease?.(failure, () => this.#holdsRoot()); } catch { /* diagnostics never block release */ }
    }
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
  const attestationRaw = process.env.PI_FABRIC_RESIDENT_LAUNCH_ATTESTATION;
  delete process.env.PI_FABRIC_RESIDENT_LAUNCH_ATTESTATION;
  let launchAttestation: AttemptLaunchReceipt | undefined;
  if (attestationRaw) {
    const value = JSON.parse(attestationRaw) as AttemptLaunchReceipt;
    const fields = ["attemptId", "scope", "cgroupPath", "launchedInScope", "at"];
    if (!value || Object.keys(value).length !== fields.length || Object.keys(value).some(key => !fields.includes(key)) ||
        typeof value.attemptId !== "string" || !value.attemptId.trim() || typeof value.scope !== "string" || !value.scope.trim() ||
        typeof value.cgroupPath !== "string" || !path.isAbsolute(value.cgroupPath) || value.launchedInScope !== true ||
        !Number.isFinite(value.at) || value.at <= 0 || value.at > Date.now() || !process.env.PI_FABRIC_RESIDENT_GENERATION) {
      throw new Error("Resident launcher launch attestation is malformed or has no chosen generation");
    }
    // An unavailable/mismatched record removes containment authority, not A's
    // ability to serve or to cooperatively checkpoint/resume without custody.
    try { launchAttestation = readResidentLaunchAttestation(config.residencyRoot, value); }
    catch (error) { if (!(error instanceof ContainmentUnavailableError)) throw error; }
  }
  return { launcher, spec, ...(attempt ? { attempt } : {}), ...(launchAttestation ? { launchAttestation } : {}) };
}

/**
 * This host's generation (owner token). A launcher chooses it before the spawn and passes it in
 * PI_FABRIC_RESIDENT_GENERATION, so the launcher knows the generation of an exit it did not see the host publish
 * (smarty-dev#7950); a malformed one refuses the start. Consumed once (removed from the environment), so no child
 * inherits it. Absent (an in-process host, or a launcher from before this change during a handover): a random one.
 */
const residentGenerationToken = (): string => {
  const chosen = process.env.PI_FABRIC_RESIDENT_GENERATION;
  delete process.env.PI_FABRIC_RESIDENT_GENERATION;
  if (!chosen) return randomUUID();
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(chosen)) {
    throw new Error("Resident generation from the launcher is malformed; refusing to start");
  }
  return chosen;
};

/** Why a resident host exits (smarty-dev#7770). */
export type ResidentExitReason = "idle-exit" | "dormant" | "handover-release" | "stopped" | "error";

/**
 * Names this host's exit in the root's launcher.log, in the launcher's trace
 * shape (smarty-dev#7770). Writes only when `owned()` holds, checked right
 * before the open: the launcher writes nothing after a clean owned exit
 * (#2010), and nothing may write the root without its fence.
 * ponytail: synchronous on purpose. The release itself does synchronous I/O on
 * this filesystem (owner.json unlink), so this adds no new stall class; an
 * async or timed write could land after the release (#2010). The residual
 * window between owned() and the open is a few syscalls, reachable only by a
 * same-uid process. This line is the human-readable copy; the durable record is
 * the generation's exit.json (generation-exit.ts), which has no such window
 * because its path is private to this generation (smarty-dev#7950).
 */
export const reportResidentExit = (residencyRoot: string, reason: ResidentExitReason,
  owned: () => boolean = () => true, constants: Partial<typeof fs.constants> = fs.constants): void => {
  // Everything slow happens before the ownership check.
  const line = `${JSON.stringify({ event: "resident-exit", at: Date.now(), reason, pid: process.pid })}\n`;
  const file = path.join(residencyRoot, "launcher.log");
  const { O_WRONLY = 0, O_APPEND = 0, O_CREAT = 0, O_NOFOLLOW, O_NONBLOCK } = constants;
  // Without O_NOFOLLOW a planted symlink would redirect the write; without
  // O_NONBLOCK a planted FIFO with no reader would block the open: fail closed.
  if (O_NOFOLLOW === undefined || O_NONBLOCK === undefined) return;
  const flags = O_WRONLY | O_APPEND | O_CREAT | O_NOFOLLOW | O_NONBLOCK;
  const uid = process.getuid?.();
  let fd: number | undefined;
  try {
    if (!owned()) {
      // No proven fence: the root may be another generation's. stderr only.
      try { process.stderr.write(`Fabric resident host exit (${reason}) without a proven root fence\n`); } catch { /* best effort */ }
      return;
    }
    // A FIFO without a reader fails at once (ENXIO); one with a reader is
    // rejected by the regular-file check. A regular file ignores O_NONBLOCK.
    fd = fs.openSync(file, flags, 0o600);
    const stat = fs.fstatSync(fd);
    if (!stat.isFile() || stat.uid !== uid) return;
    if (stat.mode & 0o177) fs.fchmodSync(fd, 0o600);
    // A short line in one write(2) call, appended atomically by O_APPEND.
    fs.writeSync(fd, line);
  } catch { /* Diagnostics never block the release. */ }
  finally { if (fd !== undefined) { try { fs.closeSync(fd); } catch { /* closed */ } } }
};

const runResidentHost = async (
  config: ResidentHostConfig,
  signal?: AbortSignal,
  modelRegistry?: PiModelRegistryView,
  processWorkSessionId = config.sessionId,
): Promise<void> => {
  let finishIdle: ((reason: ResidentExitReason) => void) | undefined;
  const idle = new Promise<ResidentExitReason>((resolve) => {
    finishIdle = resolve;
  });
  let reason: ResidentExitReason | undefined;
  const host = new ResidentHost(config, (idleReason = "idle-exit") => finishIdle?.(idleReason), modelRegistry,
    residentHostLaunchContext(config), processWorkSessionId);
  // The last act before close releases owner.json (smarty-dev#1882).
  host.beforeRelease = (failure, owned) => {
    const exitReason = failure === undefined ? reason ?? "error" : "error";
    reportResidentExit(config.residencyRoot, exitReason, owned);
    // The durable record: generation-private, created once, so the launcher writes none for this exit. The
    // path is never another generation's, so owned() only keeps a host that never owned the root from writing.
    if (owned()) recordResidentGenerationExit(config.residencyRoot,
      { format: 1, generation: host.generation, writer: "host", reason: exitReason, pid: process.pid, at: Date.now() });
  };
  await host.start();
  if (signal?.aborted) {
    reason = "stopped";
    await host.close();
    return;
  }
  reason = await Promise.race([
    idle,
    new Promise<ResidentExitReason>((resolve) => {
      const finish = (): void => resolve("stopped");
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
  processWorkSessionId?: string,
): Promise<void> => {
  let config: ResidentHostConfig | undefined;
  // Capture before ResidentHost's constructor consumes the launcher env; the
  // startup catch still reports against the exact generation that was chosen.
  const generation = process.env.PI_FABRIC_RESIDENT_GENERATION;
  try {
    config = validateResidentHostConfig(readJson<unknown>(configPath), configPath);
    await runResidentHost(config, signal, modelRegistry, processWorkSessionId);
  } catch (error) {
    if (error instanceof ResidentHostAlreadyRunning) return;
    const residencyRoot = config?.residencyRoot ?? path.dirname(configPath);
    // The launcher prepares generations/ before spawning us. Startup diagnostics are generation-private,
    // so ownership of owner.json is irrelevant; direct hosts never create the directory on failure.
    if (generation && /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(generation)) {
      const launcherBirth = process.env.PI_FABRIC_RESIDENT_LAUNCHER ? processStartTime(process.ppid) : undefined;
      recordResidentGenerationError(residencyRoot, { format: 1, generation, writer: "host", error: errorMessage(error),
        occurredAt: Date.now(), pid: process.pid, ...(launcherBirth ? { launcherBirth } : {}) });
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
