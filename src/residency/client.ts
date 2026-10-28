import { snapshotTaskReturnAddress } from "../agents/task-return-address.js";
import { diagnostics } from "../core/diagnostics.js";
import { withFabricWakeAdmission, fabricWakeCause } from "../fabric-provenance.js";
import { randomUUID } from "node:crypto";
import { CompletionJournal, completionRecipientFromRun, completionConsumed, consumeCompletion, legacyCompletionConsumed, saveCompletion, type CompletionRecipient, type CompletionSummary } from "../agents/completion-journal.js";
import { newResidentRequestId, ResidentRequestExpiredError, RESIDENT_EXPIRING_COMMAND_FORMAT } from "./request-expiry.js";
import { FabricModelDeniedError } from "../core/model-policy.js";
import { ActorSessionResetCancelledError } from "../actors/session-reset-error.js";

import { throwIfAborted } from "../async-settlement.js";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { readResidentIdleExit, residentIdleExitPath } from "./idle-exit.js";
import { readResidentGenerationError } from "./generation-exit.js";
import { readFileRetrying, writeJsonAtomic } from "../core/atomic-write.js";
import type { FabricActorInfo, FabricActorCreateRequest, FabricActorRequeueResult } from "../actors/types.js";
import { FabricActorQueuedWorkError } from "../actors/types.js";
import type { FabricAgentLog, AgentHandleInfo, AgentRunRecord, AgentRunRequest, AgentRunResult } from "../agents/types.js";
import { readChildToolAllowlist } from "../core/child-tool-allowlist.js";
import { awaitAgentCwd } from "../agents/manager.js";
import { BUILT_IN_RUNNER_IDS, requireAgentRunner, requireActorRunner } from "../agents/runner-registry.js";
import { isFabricWorktreePath } from "../agents/worktree-paths.js";
import { retainCleanupRoot, type CleanupRootCustody } from "../agents/worktree-manager.js";
import { ownedShortTmpdir, recordedShortTmpdir } from "../agents/short-tmpdir.js";
import { executeFile, spawnDetached, resolveScriptRuntime } from "../agents/transports/process-utils.js";
import { readJsonlPage } from "../log-tail.js";
import { processStartTime, residentProcessAlive } from "./process-identity.js";
import {
  RESIDENT_HANDOVER_ABI, residentLaunchSpec, handoverActive, handoverPath, handoverOutcomePath,
  mainGenerationPath, readHandoverJson, type ResidentMainGeneration, type ResidentHandoverState,
} from "./handover.js";
import { kernelFenceAvailable } from "./file-lock.js";
import { assertNoWatchdogCustody } from "./watchdog-custody.js";
import { runTreeExitVeto, runTreeResourceVeto } from "../storage/retention.js";
import type { FabricOwnedModelGuidance } from "../components/model-guidance.js";
import type { FabricMainAgentTarget } from "../main-agent.js";
import { MeshStore, meshOutageAlarmSink, type MeshStateEntry } from "../mesh/store.js";
import { MeshBackgroundRetry } from "../core/atomic-write.js";
import { DegradedObservation } from "../core/observation-watch.js";
import { isMeshWriteDeferred } from "../core/atomic-write.js";
import type { FabricParticipantSource } from "../topology/types.js";
import {
  abandonResidentRequest,
  acknowledgeResidentResponse,
  residentCommandForOwner,
  ResidentActorAuthorizationError,
  assertResidentTaskCaller,
  type ResidentTaskCaller,
  ResidentCommandUnsupportedError,
  assertResidentCommandSupported,
  assertResidentDropQueuedSupported,
  prepareResidentCreationCommand,
  assertResidentActorMain,
  assertResidentActorToolCeiling,
  type ResidentActorCaller,
  ResidentOutcomeUnknownError,
  readResidentRequestDecision,
  registerResidentCancellation,
  residentRequestExpiredOutcome,
  residentCreateMayBeProtected,
  residentSlotResponseError,
  residentCleanupErrorFields,
  ResidentSlotError,
  type ResidentSlotStatus,
  commitResidentRequest,
  RESIDENT_HOST_FORMAT,
  RESIDENT_ACTOR_COMMAND_FORMAT,
  residentDeliveryPrefix,
  residentHostId,
  residentHostStateNote,
  residentResultPath,
  residentRoot,
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
// Usable readiness now requires confirmed publication, which cannot recover an
// empty legacy mesh lock before its mandatory 30 s grace. Include bounded boot
// and post-grace acquisition time; explicit caller startup budgets stay exact.
const HOST_READY_TIMEOUT_MS = 45_000;
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
// An adopted owner (a launcher this client did not spawn) has no exit event we
// can observe: its crash is detected by owner.json/lease notifications or this
// one-shot liveness deadline, i.e. within 60 s. Never a sub-minute standing poll.
const ADOPTED_OWNER_DEADLINE_MS = 60_000;
// Failed delivery: one bounded one-shot backoff (1 s floor, 60 s cap), no interval.
const DELIVERY_RETRY_MIN_MS = 1_000;
const DELIVERY_RETRY_MAX_MS = 60_000;
// fs.watch may report one atomic journal rename more than once (notably on Windows).
// One short, non-recurring burst deadline coalesces those hints before reading state.
const DELIVERY_EVENT_COALESCE_MS = 25;
// Watcher failure: one rescan, then a bounded number of one-shot re-arms.
const WATCH_REARM_BASE_MS = 1_000;
const WATCH_REARM_MAX_MS = 60_000;
const WATCH_REARM_ATTEMPTS = 6;
const MESH_STATE_FILES = new Set(["state.json", "state.db", "state.db-wal", "state.db-journal"]);
const OWNER_FILES = new Set(["owner.json", "maintenance-ready.json", "handover.json", "idle-exit.json", "delivery-outbox"]);
const AGENT_ID_PATTERN = /^[a-f0-9]{32}$/;
const ROOT_ANCESTOR_KEY = "\0residency-root-ancestor";
const isDirectory = (directory: string): boolean => {
  try { return fs.statSync(directory).isDirectory(); } catch { return false; }
};

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
  /** Current normalized host-owned Main name; never supplied by a task request. */
  mainName?: () => string;
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
  readonly #requestsPath: string;
  readonly #responsesPath: string;
  readonly #agentsPath: string;
  readonly #inheritedToolAllowlist = readChildToolAllowlist();
  readonly #spawnPolicy = snapshotTaskReturnAddress(undefined, undefined, undefined);
  readonly #deliveryPrefix: string;
  readonly #hostPath: string;
  readonly #completions: CompletionJournal;
  #started = false;
  #deliveryRunning = false;
  #deliveryDirty = false;
  #deliveryRetry: NodeJS.Timeout | undefined;
  #deliveryFailures = 0;
  #deliveryEventTimer: NodeJS.Timeout | undefined;
  #stopStateChanges: (() => void) | undefined;
  readonly #watchers = new Map<string, fs.FSWatcher>();
  // Nearest existing ancestor of a not-yet-created residencyRoot (own key: it may equal meshRoot).
  #rootAncestor: string | undefined;
  #watchRearm: NodeJS.Timeout | undefined;
  #watchRearmAttempts = 0;
  #watchWarned = false;
  readonly #observation = new DegradedObservation("residency", () => {
    this.#wakeDelivery();
    this.#wakeWatchdog();
  }, () => {
    this.#armWatches();
    if (!this.#watchers.size) return false;
    this.#watchRearmAttempts = 0;
    return true;
  });
  observationHealth(): string[] { return this.#observation.health ? [this.#observation.health] : []; }
  #ownedLauncher: { launcher: Awaited<ReturnType<typeof spawnDetached>>; token: string } | undefined;
  #watchdogDeadline: number | undefined;
  #watchdogAgain = false;
  #watchdogTimer: NodeJS.Timeout | undefined;
  #modelGuidanceJson: string | undefined;
  #deliveryPass: Promise<void> | undefined;
  #completionFault: string | undefined;
  readonly #backgroundDelivery = new MeshBackgroundRetry("resident delivery cleanup")
    .alarmsTo(meshOutageAlarmSink(() => {
      const self = this.options.participants.self(); // this Main, as the client's other requests name it
      return { mesh: this.options.mesh, from: { id: self.id, name: self.name, kind: "main",
        ...(self.sessionId ? { sessionId: self.sessionId } : {}) } };
    }));
  #closed = false;
  #startingHost: Promise<ResidentHostOwner> | undefined;
  #startupLauncher: Awaited<ReturnType<typeof spawnDetached>> | undefined;
  #nextWatchdogAt = 0;
  #watchdogFailures = 0;
  #watchdogWork: string | undefined;
  #watchdogRunning = false;
  #watchdogStopped = false;
  readonly #runtimeNonce = randomUUID();
  #generationRecorded = false;
  readonly #releaseAbort = new AbortController();
  readonly #releaseOwners = new Set<string>();
  // A committed rename ends ownership of that source pathname, even when a
  // later move/receipt fails. Keep the original TMPDIR binding before moving run.
  readonly #terminalCleanup = new Map<string, {
    temporary?: { directory: string; dev: number; ino: number };
    completed: Set<string>;
    custody: CleanupRootCustody[];
  }>();

  constructor(readonly options: ResidencyClientOptions) {
    this.hostId = residentHostId(options.config.rootId);
    this.#configPath = path.join(options.config.residencyRoot, "config.json");
    this.#ownerPath = path.join(options.config.residencyRoot, "owner.json");
    this.#requestsPath = path.join(options.config.residencyRoot, "requests");
    this.#responsesPath = path.join(options.config.residencyRoot, "responses");
    this.#agentsPath = path.join(options.config.residencyRoot, "agents");
    this.#deliveryPrefix = residentDeliveryPrefix(options.config.rootId);
    this.#hostPath = options.hostPath ?? fileURLToPath(new URL("./launcher.js", import.meta.url));
    this.#completions = new CompletionJournal(options.config.meshRoot,
      options.mainName ? () => this.#recipient(options.config) : this.#recipient(options.config),
      options.participants, options.mesh, (result, delivered) => {
        const acknowledge = () => {
          // Publish resident metadata before the journal receipt can retire the source.
          // A refused Windows rename must leave both fences available for retry.
          this.acknowledgeCompletion(result.id);
          delivered();
        };
        if (options.onBackgroundComplete) options.onBackgroundComplete(result, acknowledge);
        else {
          options.mainAgent.deliverAgent(withFabricWakeAdmission({ from: { id: result.id, name: result.name, kind: "agent" },
            verification: "mesh", message: `Fabric agent ${result.name} ${result.status}` +
              (result.completionDelivery?.redeliveredFrom ? ` [re-delivered from dead Main session ${result.completionDelivery.redeliveredFrom}]` : "") +
              `: ${result.error ?? result.text}`, delivery: "followUp", triggerTurn: true, data: result,
            deliveryId: `agent-completion:${result.id}` }, [fabricWakeCause(
              { id: result.id, name: result.name, kind: "agent" }, "inbox", "agent-completion", result.id)]));
          acknowledge();
        }
      });
  }

  start(): void {
    if (this.#started || this.#closed || !this.options.mainAgent.local) return;
    this.syncPiModels();
    this.#started = true;
    // Event-driven: journal/mesh/owner notifications wake delivery and recovery.
    // Arm before the startup catch-up so a write racing it is never lost.
    this.#armWatches();
    this.#stopStateChanges = this.options.mesh.subscribeStateChanges(() => this.#wakeDelivery());
    // Startup catch-up: one delivery pass and one recovery pass now.
    this.#wakeDelivery();
    this.#wakeWatchdog();
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
    this.#observation.close();
    this.#stopStateChanges?.();
    this.#stopStateChanges = undefined;
    if (this.#deliveryEventTimer) clearTimeout(this.#deliveryEventTimer);
    this.#deliveryEventTimer = undefined;
    if (this.#deliveryRetry) clearTimeout(this.#deliveryRetry);
    this.#deliveryRetry = undefined;
    if (this.#watchdogTimer) clearTimeout(this.#watchdogTimer);
    this.#watchdogTimer = undefined;
    if (this.#watchRearm) clearTimeout(this.#watchRearm);
    this.#watchRearm = undefined;
    this.#closeWatches();
    // The durable host outlives this client; only stop observing its launcher.
    this.#ownedLauncher = undefined;
    // Await the owned pass itself, not a timer polling its state. Real async file
    // I/O can settle while the caller's clock (e.g. fake timers) is stopped.
    await this.#deliveryPass?.catch(() => undefined);
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

  /**
   * Host-only agents.wakeText after a live Main config reload (smarty-dev#6144 review round 3). The
   * resident host reads it from config.json at every activation, so enabling applies and removing
   * revokes at once, with no host restart.
   */
  updateWakeText(wakeText: ResidentHostConfig["agents"]["wakeText"]): void {
    const agents = this.options.config.agents;
    if (JSON.stringify(agents.wakeText) === JSON.stringify(wakeText)) return;
    if (wakeText) agents.wakeText = structuredClone(wakeText);
    else delete agents.wakeText;
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
    finally {
      this.#startingHost = undefined;
      // Startup settlement is an owner event: reclassify (own child / adopted / backoff).
      this.#armWatches();
      this.#wakeWatchdog();
    }
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
    const existing = this.#readyOwner();
    if (existing) return existing;
    assertNoWatchdogCustody(this.options.config.residencyRoot);
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
    // Owner publication is not maintenance readiness. Attach to its exact
    // live generation; never launch a competing startup while its fence holds.
    const attachDeadline = Date.now() + startupBudgetMs(this.options.startupTimeoutMs ?? HOST_READY_TIMEOUT_MS);
    while (this.#liveOwner()) {
      if (this.#closed) throw new Error("Fabric residency client is closed");
      const followed = this.#readyOwner();
      if (followed) return followed;
      if (Date.now() >= attachDeadline) throw new Error("Timed out waiting for Fabric resident host maintenance readiness");
      await delay(STATUS_POLL_MS);
    }
    // Recheck after the attachment wait: the old launcher may have entered
    // destructive watchdog custody while we still saw its live owner.
    assertNoWatchdogCustody(this.options.config.residencyRoot);
    // This explicit business startup, not a launcher recovery loop, revokes the idle stop.
    fs.rmSync(residentIdleExitPath(this.options.config.residencyRoot), { force: true });
    const launchToken = randomUUID();
    const launcher = await spawnDetached(
      this.#hostPath,
      ["--config", this.#configPath, "--launch-token", launchToken],
      this.options.config.cwd,
    );
    this.#startupLauncher = launcher;
    // The budget counts from the launcher's first sign of life (its
    // launcher-started trace), so its own boot does not consume it.
    const budget = startupBudgetMs(this.options.startupTimeoutMs ?? HOST_READY_TIMEOUT_MS);
    let deadline = Date.now() + budget;
    let started = false;
    let launcherExited = false;
    while (true) {
      if (this.#closed) {
        await this.#stopStartingLauncher();
        throw new Error("Fabric residency client is closed");
      }
      const owner = this.#readyOwner();
      if (owner) {
        // Only our attempt may transfer custody to durable residency. Another
        // winner does not prove our losing launcher (or restored work) exited.
        if (owner.launchToken !== launchToken) await this.#stopStartingLauncher();
        else {
          this.#startupLauncher = undefined;
          this.#observeLauncher(launcher, launchToken);
        }
        return owner;
      }
      // Select only the error record whose filename is this call's launch token;
      // a predecessor or earlier start cannot provide stale startup diagnostics.
      const failure = readResidentGenerationError(this.options.config.residencyRoot, launchToken);
      if (failure) {
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
    if (this.options.mainName) this.options.config.mainName = this.options.mainName();
    const state = this.options.piModelState?.();
    if (state) this.options.config.piModels = structuredClone(state);
  }

  async ensureActor(id: string): Promise<void> {
    await this.ensureHost();
    await this.#waitForParticipant(id, "actor");
  }

  /** Read the selected live resident's actual native store; do not start a host. */
  async slotStatus(signal?: AbortSignal): Promise<ResidentSlotStatus> {
    if (!this.#liveOwner()) throw new Error("Root resident host is not live");
    const response = await this.#command({
      format: RESIDENT_ACTOR_COMMAND_FORMAT, operation: "slotStatus", requestId: randomUUID(),
      rootId: this.options.config.rootId, createdAt: Date.now(),
    }, signal);
    if (!response.slotStatus) throw new Error("Resident host returned no native slot status");
    return response.slotStatus;
  }

  async createActor(request: FabricActorCreateRequest, signal?: AbortSignal, onDispatched?: () => void): Promise<FabricActorInfo> {
    const { idempotencyKey, ...creationRequest } = request;
    const runner = request.runner ?? this.options.config.agents.runner;
    const runnerModule = requireActorRunner(runner, "durable").residentModule;
    await this.ensureHost();
    const self = this.options.participants.self();
    const caller: ResidentActorCaller | undefined = this.options.mainAgent.local && self.kind === "root" &&
      self.id === this.options.config.rootId && self.sessionId
      ? { identity: { id: self.id, name: self.name, kind: "main", sessionId: self.sessionId }, hostId: self.ownerHostId }
      : undefined;
    const response = await this.#command({
      format: RESIDENT_HOST_FORMAT,
      operation: "createActor",
      ...(typeof (request as { spawnRouterDecisionId?: unknown }).spawnRouterDecisionId === "string"
        ? { routerCaller: { id: self.id, rootId: self.rootId, sessionId: self.sessionId ?? "",
          ownerHostId: self.ownerHostId, ownerIdentityId: self.ownerIdentityId, kind: self.kind } } : {}),
      ...(idempotencyKey === undefined ? {} : { idempotencyKey }),
      requestId: randomUUID(),
      rootId: this.options.config.rootId,
      request: { ...creationRequest, runner },
      ...(runnerModule ? { runnerModule } : {}),
      ...(caller ? { caller } : {}),
      createdAt: Date.now(),
    }, signal, onDispatched);
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
    const response = await this.#mutateActor(mutation, signal, caller);
    if (!response.actor) throw new Error("Resident host returned no actor from setter");
    return response.actor;
  }

  /** smarty-dev#16416: feed a stop archive to a resident-owned actor. */
  async requeueArchived(mutation: Extract<ResidentActorMutation, { operation: "requeueArchived" }>, signal?: AbortSignal,
    caller?: ResidentActorCaller): Promise<FabricActorRequeueResult> {
    const response = await this.#mutateActor(mutation, signal, caller);
    if (!response.requeued) throw new Error("Resident host returned no requeueArchived result");
    return response.requeued;
  }

  async #mutateActor(mutation: ResidentActorMutation, signal?: AbortSignal, caller?: ResidentActorCaller): Promise<ResidentCommandResponse> {
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
    return this.#command({
      ...mutation, caller, format: RESIDENT_ACTOR_COMMAND_FORMAT, requestId: randomUUID(),
      rootId: this.options.config.rootId, createdAt: Date.now(),
    }, signal);
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

  async spawnAgent(request: AgentRunRequest, signal?: AbortSignal, onDispatched?: () => void): Promise<AgentHandleInfo> {
    // Capture at the public call from the host-owned participant, never from request fields.
    // Snapshot policy at runtime construction, before a task can change ambient state.
    const self = this.options.participants.self();
    const caller: ResidentTaskCaller = {
      id: self.id, rootId: self.rootId, sessionId: self.sessionId ?? "",
      ownerHostId: self.ownerHostId, ownerIdentityId: self.ownerIdentityId, kind: self.kind,
      returnAddress: {
        spawnerId: self.id, spawnerSessionId: self.sessionId ?? "",
        ancestors: [...new Set([...this.#spawnPolicy.ancestors, this.options.config.rootId])],
        escalationTargets: [...this.#spawnPolicy.escalationTargets],
      },
    };
    assertResidentTaskCaller(caller, self, this.options.config.rootId);
    // Explicit keys are checked against the loaded owner before dispatch.
    const { idempotencyKey, ...spawnRequest } = request;
    const resolvedRequest = spawnRequest.cwd === undefined
      ? spawnRequest
      : { ...spawnRequest, cwd: await awaitAgentCwd(this.options.config.cwd, spawnRequest.cwd, signal) };
    // Registration is process-local: the resident host must import the runner too.
    const runner = spawnRequest.runner ?? this.options.config.agents.runner;
    const runnerModule = BUILT_IN_RUNNER_IDS.has(runner) ? undefined : requireAgentRunner(runner).residentModule;
    if (!BUILT_IN_RUNNER_IDS.has(runner) && !runnerModule) {
      throw new Error(`Fabric runner ${runner} declares no residentModule; durable residency needs one`);
    }
    // Freeze inherited optional-tool authority before transferring to an existing host.
    const allowedTools = this.#inheritedToolAllowlist;
    const tools = allowedTools === undefined ? undefined
      : (request.tools ?? this.options.config.agents.defaultTools).filter((tool) => allowedTools.has(tool));
    await this.ensureHost();
    // Durable settlement can synchronously occupy the caller's event loop past
    // its last heartbeat. Publish a fresh trusted owner lease before dispatch;
    // the host still checks the captured binding and rejects absent/stale peers.
    await this.options.participants.refresh?.();
    const response = await this.#command(
      {
        format: RESIDENT_HOST_FORMAT,
        operation: "spawnBound",
        ...(idempotencyKey === undefined ? {} : { idempotencyKey }),
        requestId: randomUUID(),
        rootId: this.options.config.rootId,
        request: { ...resolvedRequest, ...(tools ? { tools } : {}), residency: "durable" },
        caller,
        ...(runnerModule ? { runnerModule } : {}),
        createdAt: Date.now(),
      },
      signal,
      onDispatched,
    );
    if (!response.handle) throw new Error("Fabric resident host returned no agent handle");
    await this.#waitForParticipant(response.handle.id, "agent", signal).catch((error) => {
      throw new ResidentOutcomeUnknownError({
        format: RESIDENT_HOST_FORMAT, operation: "spawnBound", requestId: response.requestId,
        rootId: this.options.config.rootId, request, caller, createdAt: Date.now(),
      }, { requestId: response.requestId, state: "committed", id: response.handle!.id, ownerHostId: this.hostId }, error, signal);
    });
    return response.handle;
  }

  /** Persist session-scoped background outcomes before their retractable inbox admission. */
  enqueueCompletion(result: AgentRunResult, admittedRecipient?: CompletionRecipient): void {
    // No manifest and no host binding means no authorized recipient. This is a
    // deliberate prelaunch refusal, not a transient storage failure to retry.
    if (!result.logFile && !admittedRecipient && typeof this.#completions.recipientSource === "function") return;
    this.#completions.save(result, admittedRecipient);
    this.#wakeDelivery();
  }

  hasAgent(id: string): boolean {
    return AGENT_ID_PATTERN.test(id) && (fs.existsSync(this.#metadataPath(id)) || this.#completions.result(id) !== undefined);
  }

  /** Operational ownership excludes recovered/journal-only ordinary runs. */
  ownsAgent(id: string): boolean {
    return AGENT_ID_PATTERN.test(id) && fs.existsSync(this.#metadataPath(id));
  }

  statusAgent(id: string): AgentRunRecord | AgentHandleInfo | CompletionSummary {
    const metadata = this.#metadata(id);
    if (!metadata) {
      const completion = this.#completions.result(id);
      if (completion) return completion;
      throw new Error(`Unknown durable Fabric agent: ${id}`);
    }
    const record = this.#record(metadata);
    if (!record) return structuredClone(metadata.handle);
    return {
      ...record,
      ...(terminal(record.status) && !metadata.completionConsumedAt && !completionConsumed(this.options.config.meshRoot, id)
        ? { completionDelivery: { status: "undelivered" as const, addressedTo: this.options.config.sessionId } } : {}),
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
      entries = [];
    }
    const records = entries
      .filter((entry) => entry.endsWith(".json"))
      .flatMap((entry) => {
        try {
          return [this.statusAgent(entry.slice(0, -5))];
        } catch {
          return [];
        }
      });
    const seen = new Set(records.map(record => record.id));
    return [...records, ...this.#completions.pending().filter(value => !seen.has(value.result.id))
      .flatMap(value => { const result = this.#completions.result(value.result.id); return result ? [result] : []; })];
  }

  /** Status may be observed during retry backoff; only logical settlement permits consumption. */
  completionSettled(id: string): boolean {
    if (!this.hasAgent(id) || this.#attemptMayRetry(id)) return false;
    const status = this.statusAgent(id);
    return terminal(status.status) && "startedAt" in status;
  }

  /** localRunSettled is certified by the owning manager, including a failed journal save. */
  acknowledgeCompletion(id: string, localRunSettled = false): void {
    const metadata = this.#metadata(id, true);
    // The local-manager fallback is for its ordinary runs, never a resident worker attempt.
    if ((metadata || !localRunSettled) && !this.completionSettled(id)) return;
    if (metadata && !metadata.completionConsumedAt) {
      writeJsonAtomic(this.#metadataPath(id), { ...metadata, completionConsumedAt: Date.now() }, { space: 2, durable: true });
    }
    const journalConsumed = this.#completions.acknowledge(id, localRunSettled);
    // Journal-only ordinary outcomes have no durable metadata, but their wait still
    // retracts an already admitted completion from this session's inbox.
    if (metadata || journalConsumed) this.options.onResultConsumed?.(id);
  }

  async waitAgent(id: string, signal?: AbortSignal, deferConsumption?: (consume: () => void, abandon?: () => void) => void): Promise<AgentRunResult | CompletionSummary> {
    while (true) {
      if (signal?.aborted) throw new Error(`Waiting for durable Fabric agent ${id} was aborted`);
      const status = this.statusAgent(id);
      if (terminal(status.status) && "startedAt" in status && !this.#attemptMayRetry(id)) {
        if (deferConsumption) deferConsumption(() => this.acknowledgeCompletion(id));
        else this.acknowledgeCompletion(id);
        return status as AgentRunResult | CompletionSummary;
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

  async removeActor(id: string, signal?: AbortSignal, options: { dropQueued?: true } = {}): Promise<{ removed: boolean; pending?: string; cleaned?: boolean }> {
    await this.ensureHost();
    const response = await this.#command({
      format: RESIDENT_HOST_FORMAT,
      operation: "removeActor",
      requestId: randomUUID(),
      rootId: this.options.config.rootId,
      id,
      ...(options.dropQueued === true ? { dropQueued: true as const } : {}),
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
    this.#completions.forget(id);
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
    // A proven absent tree has no descendant files left to remove (saved-result
    // cleanup). Other stat errors fail closed; every existing tree needs strict exit evidence.
    const runDirectoryPresent = fs.lstatSync(metadata.runDirectory, { throwIfNoEntry: false }) !== undefined;
    const exitVeto = runDirectoryPresent ? runTreeExitVeto(metadata.runDirectory, 0, undefined, true) : undefined;
    if (exitVeto) {
      throw new Error(
        `Cannot clean up durable Fabric agent ${metadata.id}: ${exitVeto} ` +
        `(see ${metadata.runDirectory}). Check the worker, then remove its files by hand.`,
      );
    }
    const command: ResidentCommand = { format: RESIDENT_EXPIRING_COMMAND_FORMAT, operation: "cleanup", requestId: newResidentRequestId(),
      rootId: this.options.config.rootId, id: metadata.id, deleteBranch, createdAt: Date.now() };
    const prior = this.#terminalCleanup.get(metadata.id);
    const temporaryDirectory = prior ? undefined : recordedShortTmpdir(metadata.runDirectory, metadata.id);
    const temporaryStat = temporaryDirectory ? ownedShortTmpdir(temporaryDirectory) : undefined;
    const cleanup = prior ?? {
      ...(temporaryDirectory && temporaryStat ? { temporary: { directory: temporaryDirectory, dev: temporaryStat.dev, ino: temporaryStat.ino } } : {}),
      completed: new Set<string>(), custody: [] as CleanupRootCustody[],
    };
    const custody = cleanup.custody;
    const deferred: string[] = [];
    let commitAttempted = false;
    const commit = (): void => {
      throwIfAborted(signal);
      registerResidentCancellation(signal, this.options.config.residencyRoot, command);
      commitAttempted = true;
      commitResidentRequest(this.options.config.residencyRoot, command, metadata.id, this.hostId);
    };
    try {
      const gitRoot = metadata.worktreeGitRoot ?? this.options.config.projectRoot;
      const worktree = metadata.handle.worktree ? await registeredWorktree(gitRoot, metadata.handle.worktree) : undefined;
      if (!worktree && deleteBranch) {
        throw new Error(`Durable Fabric agent ${metadata.id} has no worktree branch to delete`);
      }
      if (worktree) {
        deferred.push(`Git worktree registration cleanup deferred: ${JSON.stringify(gitRoot)} still registers ${JSON.stringify(worktree)}; no remove or prune was attempted.`);
        if (deleteBranch && metadata.handle.branch) deferred.push(`Git branch deletion deferred: ${metadata.handle.branch} was not deleted.`);
      }
      // The helper durably receipts and renames whole roots, including files.
      // Commit before its first effect; EXDEV is visible, never copy-and-delete.
      // Keep prior held roots on every partial failure, without rollback over a
      // replacement at a canonical source path. Git registration remains debt.
      commit();
      this.#terminalCleanup.set(metadata.id, cleanup);
      const archive = `${this.options.config.residencyRoot}.trace-archive`;
      const retain = (source: string, kind: string): void => {
        throwIfAborted(signal);
        if (cleanup.completed.has(source)) return;
        try {
          const move = retainCleanupRoot(source, archive, metadata.id, `durable agent cleanup whole-root custody: ${kind}`);
          if (move) custody.push(move);
          cleanup.completed.add(source); // Proven absence is not authority to adopt a later replacement.
        } catch (error) {
          const moved = error instanceof Error && "retainedRoot" in error ? error.retainedRoot as CleanupRootCustody | undefined : undefined;
          if (moved?.source === source) { custody.push(moved); cleanup.completed.add(source); }
          throw error;
        }
      };
      if (runDirectoryPresent) retain(metadata.runDirectory, "run");
      if (cleanup.temporary && !cleanup.completed.has(cleanup.temporary.directory)) {
        const current = ownedShortTmpdir(cleanup.temporary.directory);
        if (current && (current.dev !== cleanup.temporary.dev || current.ino !== cleanup.temporary.ino)) {
          throw new Error(`Recorded private TMPDIR changed identity: ${cleanup.temporary.directory}`);
        }
        if (current) retain(cleanup.temporary.directory, "private TMPDIR");
        else cleanup.completed.add(cleanup.temporary.directory);
      }
      if (worktree) retain(worktree, "worktree");
      retain(this.#metadataPath(metadata.id), "agent metadata");
      retain(residentResultPath(this.options.config.residencyRoot, metadata.id), "resident result");
      if (deferred.length) diagnostics.warn("src/residency/client.ts", `[pi-fabric] resident cleanup ${metadata.id}: ${deferred.join(" ")}`);
      // Consumption is last, including cancellation that arrived during the
      // final move. A failed/cancelled move never acknowledges partial cleanup.
      throwIfAborted(signal);
      this.#completions.forget(metadata.id);
      this.options.onResultConsumed?.(metadata.id);
      acknowledgeResidentResponse(this.options.config.residencyRoot, { format: 1, requestId: command.requestId, ok: true, completedAt: Date.now() }, Date.now(), command.format);
      return { cleaned: true };
    } catch (error) {
      const detail = new Error(`Resident cleanup refused for ${metadata.id}: ${error instanceof Error ? error.message : String(error)}. ` +
        `Custody paths: ${JSON.stringify(custody)}. ${deferred.join(" ")} No root rollback or source-path deletion was attempted.`, { cause: error });
      const deferredFailure = residentCleanupErrorFields(error);
      if (deferredFailure.cleanupStatus) Object.assign(detail, { code: deferredFailure.errorCode, cleanupStatus: deferredFailure.cleanupStatus });
      diagnostics.warn("src/residency/client.ts", `[pi-fabric] ${detail.message}`);
      if (!commitAttempted) throw detail;
      let decision;
      try { decision = readResidentRequestDecision(this.options.config.residencyRoot, command.requestId); } catch { /* unknown fence */ }
      if (decision?.state === "abandoned") throw detail;
      // Partial cleanup or an unreadable fence is not a proved rejection either.
      throw new ResidentOutcomeUnknownError(command, decision, detail, signal);
    }
  }

  async #command(command: ResidentCommand, signal?: AbortSignal, onDispatched?: () => void): Promise<ResidentCommandResponse> {
    const owner = this.#liveOwner();
    if (!owner) throw new Error("Root resident host is not live");
    assertResidentCommandSupported(owner, command.operation);
    assertResidentDropQueuedSupported(owner, command);
    if (owner.requestFence !== 1) {
      throw new Error("Fabric resident host lacks the abandonment fence; restart the resident host before retrying. No request was dispatched.");
    }
    command = prepareResidentCreationCommand(owner, command);
    throwIfAborted(signal);
    command = residentCommandForOwner(command, owner);
    let dispatched = false;
    registerResidentCancellation(signal, this.options.config.residencyRoot, command, () => dispatched);
    const responsePath = path.join(this.#responsesPath, `${command.requestId}.json`);
    try {
      throwIfAborted(signal);
      dispatched = true; // A failed publication can still have reached the selected host.
      // Host-only receipt custody follows this same fence, never earlier preparation.
      try { onDispatched?.(); } catch { /* Observers cannot change dispatch. */ }
      atomicWrite(path.join(this.#requestsPath, `${command.requestId}.json`), command);
      const deadline = Date.now() + (this.options.commandTimeoutMs ?? COMMAND_TIMEOUT_MS);
      while (Date.now() < deadline) {
        if (signal?.aborted) throw new Error("Fabric residency request was aborted");
        const response = readJson<ResidentCommandResponse>(responsePath);
        if (response?.format === RESIDENT_HOST_FORMAT && response.requestId === command.requestId) {
          if (acknowledgeResidentResponse(this.options.config.residencyRoot, response, Date.now(), command.format)) fs.rmSync(responsePath, { force: true });
          if (!response.ok) {
            const slotError = residentSlotResponseError(response, signal);
            if (slotError) {
              if (slotError.code === "FABRIC_ACTOR_SLOT_CLAIM_REFUSED") dispatched = false;
              throw slotError;
            }
            if (command.operation === "resetSession" && response.errorCode === "ACTOR_SESSION_RESET_CANCELLED") {
              throw new ActorSessionResetCancelledError(command.id, response.error, command.requestId);
            }
            if (response.errorCode === "FABRIC_ACTOR_QUEUED_WORK" && response.queuedWork) {
              throw new FabricActorQueuedWorkError(response.queuedWork.operation, response.queuedWork.actorId,
                { ...response.queuedWork.counts }, response.error ?? "Fabric actor holds queued work");
            }
            if (response.errorCode === "RESIDENT_REQUEST_EXPIRED") throw new ResidentRequestExpiredError(command.requestId);
            if (response.errorCode === "RESIDENT_ACTOR_FORBIDDEN") throw new ResidentActorAuthorizationError(response.error);
            if (response.errorCode === "RESIDENT_COMMAND_UNSUPPORTED") throw new ResidentCommandUnsupportedError(response.error);
            if (response.errorCode === "FABRIC_MODEL_DENIED" && typeof response.modelDenied?.model === "string") {
              throw new FabricModelDeniedError(response.modelDenied.model,
                typeof response.modelDenied.replacement === "string" ? response.modelDenied.replacement : undefined);
            }
            const cleanupFailure = command.operation === "cleanup"
              ? residentCleanupErrorFields({ code: response.errorCode, cleanupStatus: response.cleanupStatus }) : {};
            throw Object.assign(new Error(response.error ?? "Fabric resident host rejected request"),
              cleanupFailure.cleanupStatus ? { code: cleanupFailure.errorCode, cleanupStatus: cleanupFailure.cleanupStatus } : {});
          }
          if ((command.operation === "spawnBound" && !response.handle) || (command.operation === "createActor" && !response.actor)) {
            throw new Error("Fabric resident host returned no created entity");
          }
          // A resident response can carry new deliveries/completions: drain now.
          this.#wakeDelivery();
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
      // This acknowledged terminal response is known, unlike a lost post-commit reply.
      if (error instanceof ActorSessionResetCancelledError || error instanceof ResidentSlotError) throw error;
      // The protected receiver upgrades every post-accept failure to distinct
      // slot uncertainty, so these terminal native refusals retain their codes.
      if (error instanceof ResidentActorAuthorizationError || error instanceof ResidentCommandUnsupportedError ||
          error instanceof FabricModelDeniedError || error instanceof FabricActorQueuedWorkError) { dispatched = false; throw error; }
      if (error instanceof ResidentRequestExpiredError) throw residentRequestExpiredOutcome(this.options.config.residencyRoot, command, signal);
      let decision;
      try {
        decision = abandonResidentRequest(this.#requestsPath, this.#responsesPath, command.requestId, command.format);
      } catch (fenceError) {
        if (fenceError instanceof ResidentRequestExpiredError) throw residentRequestExpiredOutcome(this.options.config.residencyRoot, command, signal);
        // No proven abandonment: never report a safe-to-retry rejection.
        let known;
        try { known = readResidentRequestDecision(this.options.config.residencyRoot, command.requestId); } catch { /* unreadable fence */ }
        // Fence failure must not erase an authenticated custody failure/status.
        const failure = residentCleanupErrorFields(error).cleanupStatus
          ? Object.assign(new Error(`${error instanceof Error ? error.message : String(error)}; fence read failed: ${String(fenceError)}`, { cause: error }),
            { code: "CLEANUP_CUSTODY_DEFERRED", cleanupStatus: residentCleanupErrorFields(error).cleanupStatus }) : fenceError;
        throw new ResidentOutcomeUnknownError(command, known, failure, signal);
      }
      if (decision.state === "committed" || (dispatched && residentCreateMayBeProtected(command))) {
        throw new ResidentOutcomeUnknownError(command, decision, error, signal);
      }
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

  /** A replacement resident host does not supervise/retry the predecessor's old runs. */
  #attemptMayRetry(id: string): boolean {
    const metadata = this.#metadata(id);
    if (!metadata) return false;
    const saved = readJson<AgentRunRecord>(residentResultPath(this.options.config.residencyRoot, id));
    if (saved?.id === id && terminal(saved.status)) return false;
    const committed = this.#completions.result(id);
    if (committed && "text" in committed) return false;
    const manifest = readJson<{ supervisor?: { pid: number; processStartedAt?: string } }>(
      path.join(metadata.runDirectory, "completion-recipient.json"));
    // Legacy/missing pins do not prove settlement while a resident supervisor still owns work.
    return manifest?.supervisor !== undefined
      ? residentProcessAlive(manifest.supervisor.pid, manifest.supervisor.processStartedAt)
      : this.#liveOwner() !== undefined;
  }

  /**
   * The run's live status, else the terminal record the host saved before an idle exit removed
   * the run directory. With neither, no run directory and no live host, the run cannot still be
   * running: report it failed rather than the stale spawn handle (smarty-dev#1882).
   */
  #record(metadata: ResidentAgentMetadata): AgentRunRecord | undefined {
    const saved = readJson<AgentRunRecord>(residentResultPath(this.options.config.residencyRoot, metadata.id));
    if (saved?.id === metadata.id && terminal(saved.status)) return saved;
    // A supervisor-published exact-owner journal outcome is also authoritative.
    // Do not re-mask it using a provisional/missing worker identity: the same
    // journal already discharges #attemptMayRetry and its consumption fence.
    const committed = this.#completions.result(metadata.id);
    if (committed && "text" in committed && "startedAt" in committed && terminal(committed.status)) return committed;
    const live = readJson<AgentRunRecord>(path.join(metadata.runDirectory, "status.json"));
    if (live?.id === metadata.id) {
      if (live.transport === "process" && ((live.status === "running" && live.finishingAt !== undefined) ||
          (terminal(live.status) && (this.#attemptMayRetry(metadata.id) || runTreeResourceVeto(metadata.runDirectory, 0, undefined, true, true))))) {
        // Result visibility needs checked tree exit, not successful archival.
        // A failed archive still vetoes deletion, never delivery after exit.
        // A worker candidate is not the resident supervisor's tree-exit outcome.
        // Preserve its final text, but keep status and wait/join non-terminal.
        const { finishedAt: _finishedAt, ...pending } = live;
        return { ...pending, status: "finishing" };
      }
      return live;
    }
    if (fs.existsSync(metadata.runDirectory) || this.#liveOwner()) return undefined;
    const { handle } = metadata;
    return {
      id: handle.id,
      name: handle.name,
      task: "",
      status: "failed",
      ...(handle.routeClass !== undefined ? { routeClass: handle.routeClass } : {}),
      ...(handle.routeClassSource !== undefined ? { routeClassSource: handle.routeClassSource } : {}),
      ...(handle.protected !== undefined ? { protected: handle.protected } : {}),
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

  #metadata(id: string, strict = false): ResidentAgentMetadata | undefined {
    if (!AGENT_ID_PATTERN.test(id)) return undefined;
    const file = this.#metadataPath(id);
    let metadata: ResidentAgentMetadata | undefined;
    try { metadata = JSON.parse(readFileRetrying(file)) as ResidentAgentMetadata; }
    catch (error) {
      if (!strict) return undefined;
      // Unknown metadata is not an ordinary journal-only run. Retry consumption
      // rather than silently dropping its resident receipt on a transient open.
      if ((error as NodeJS.ErrnoException).code === "ENOENT") {
        try { fs.lstatSync(file); } catch (absence) {
          if ((absence as NodeJS.ErrnoException).code === "ENOENT") return undefined;
        }
      }
      throw error;
    }
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

  #readyOwner(): ResidentHostOwner | undefined {
    const owner = this.#liveOwner();
    if (owner?.maintenanceReady === 1 &&
        readJson<{ token?: string }>(path.join(this.options.config.residencyRoot, "maintenance-ready.json"))?.token !== owner.token) return undefined;
    return owner;
  }

  /** Own launcher child: its actual ChildProcess close is the crash/exit event. No timer. */
  #observeLauncher(launcher: Awaited<ReturnType<typeof spawnDetached>>, token: string): void {
    if (this.#closed) return;
    this.#ownedLauncher = { launcher, token };
    void launcher.closed.then(() => {
      if (this.#ownedLauncher?.launcher !== launcher) return;
      this.#ownedLauncher = undefined;
      this.#wakeWatchdog();
    }, () => undefined);
  }

  /** Run one recovery pass now (coalesced); events arriving mid-pass rerun it once. */
  #wakeWatchdog(): void {
    if (this.#closed || !this.#started || this.#watchdogStopped) return;
    if (this.#watchdogRunning) { this.#watchdogAgain = true; return; }
    // An in-flight startup settles through ensureHost's finally, which wakes again.
    if (this.#startingHost) return;
    if (this.#watchdogTimer) clearTimeout(this.#watchdogTimer);
    this.#watchdogTimer = undefined;
    this.#watchdogRunning = true;
    this.#watchdogAgain = false;
    void this.#watchdog().catch(() => undefined).finally(() => {
      this.#watchdogRunning = false;
      if (this.#closed) return;
      // Without a kernel fence the watchdog can never act: stop after the startup check.
      if (!kernelFenceAvailable()) { this.#watchdogStopped = true; return; }
      if (this.#watchdogAgain && !this.#startingHost) { this.#wakeWatchdog(); return; }
      this.#armWatchdog();
    });
  }

  /** Arm the single one-shot deadline the last pass chose (none for idle/own child). */
  #armWatchdog(): void {
    if (this.#watchdogTimer) clearTimeout(this.#watchdogTimer);
    this.#watchdogTimer = undefined;
    if (this.#closed || this.#watchdogStopped || this.#watchdogRunning || !this.#started) return;
    const deadline = this.#watchdogDeadline;
    if (deadline === undefined) return;
    this.#watchdogTimer = setTimeout(() => {
      this.#watchdogTimer = undefined;
      this.#wakeWatchdog();
    }, Math.max(1, deadline - Date.now()));
    this.#watchdogTimer.unref();
  }

  /** Deadline for a live owner: none when it is our own observed launcher child. */
  #ownerDeadline(owner: ResidentHostOwner): number | undefined {
    const owned = this.#ownedLauncher;
    if (owned && owner.launchToken === owned.token) return undefined;
    return Date.now() + ADOPTED_OWNER_DEADLINE_MS;
  }

  async #watchdog(): Promise<void> {
    this.#watchdogDeadline = undefined;
    if (this.#closed || this.#startingHost || !kernelFenceAvailable()) return;
    const live = this.#liveOwner();
    if (live) {
      this.#watchdogDeadline = this.#ownerDeadline(live);
      if (!this.#readyOwner()) return;
      void this.reconcileRelease().catch((error) => this.#deferRelease(error));
      return;
    }
    // Launcher custody: handover.json/owner.json notifications or the 60 s deadline.
    if (handoverActive(readHandoverJson<ResidentHandoverState>(handoverPath(this.options.config.residencyRoot)))) {
      this.#watchdogDeadline = Date.now() + ADOPTED_OWNER_DEADLINE_MS;
      return;
    }
    // Intentional dead-root retirement is not a crash, even if retained delivery debt remains.
    if (readResidentIdleExit(this.options.config.residencyRoot, this.options.config.rootId)) return;
    const work = this.#durableWork();
    // Idle: no deadline at all; owner/outbox notifications and ensureHost wake us.
    if (!work) { this.#watchdogWork = undefined; this.#watchdogFailures = 0; this.#nextWatchdogAt = 0; return; }
    // Failed/unprogressing recovery: one bounded one-shot backoff while work exists.
    if (Date.now() < this.#nextWatchdogAt) { this.#watchdogDeadline = this.#nextWatchdogAt; return; }
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
    if (this.#closed) return;
    const owner = this.#liveOwner();
    this.#watchdogDeadline = owner ? this.#ownerDeadline(owner) : this.#nextWatchdogAt;
  }

  /** Coalesce a filesystem notification burst, not recurring delivery work. */
  #notifyDelivery(): void {
    if (this.#closed || this.#deliveryEventTimer) return;
    this.#deliveryEventTimer = setTimeout(() => {
      this.#deliveryEventTimer = undefined;
      this.#wakeDelivery();
    }, DELIVERY_EVENT_COALESCE_MS);
    this.#deliveryEventTimer.unref();
  }

  /** Delivery wake: coalesced; a wake during an in-flight drain reruns it once. */
  #wakeDelivery(): void {
    if (this.#closed || !this.#started || !this.options.mainAgent.local) return;
    if (this.#deliveryRunning) { this.#deliveryDirty = true; return; }
    // A pending failure backoff owns the next attempt; it drains everything then.
    if (this.#deliveryRetry) { this.#deliveryDirty = true; return; }
    this.#deliveryRunning = true;
    this.#deliveryDirty = false;
    void this.#backgroundDelivery.run(() => this.#drainDeliveries()).then(outcome => {
      this.#deliveryRunning = false;
      if (this.#closed) return;
      const meshWait = this.#backgroundDelivery.waitMs;
      const failed = outcome === "retry" || outcome === "failed" || this.#completionFault !== undefined ||
        (outcome === "skipped" && meshWait > 0);
      if (!failed) {
        this.#deliveryFailures = 0;
        if (this.#deliveryDirty) this.#wakeDelivery();
        return;
      }
      const own = Math.min(DELIVERY_RETRY_MAX_MS, DELIVERY_RETRY_MIN_MS * 2 ** this.#deliveryFailures++);
      const delayMs = Math.min(DELIVERY_RETRY_MAX_MS, Math.max(own, meshWait));
      this.#deliveryRetry = setTimeout(() => {
        this.#deliveryRetry = undefined;
        this.#wakeDelivery();
      }, delayMs);
      this.#deliveryRetry.unref();
    }, () => { this.#deliveryRunning = false; });
  }

  #closeWatches(): void {
    for (const watcher of this.#watchers.values()) { try { watcher.close(); } catch { /* already closed */ } }
    this.#watchers.clear();
    this.#rootAncestor = undefined;
  }

  /**
   * A missing residencyRoot (e.g. idle start before any client materialized it) is observed
   * through its nearest existing ancestor; each next-component creation deepens the watch until
   * the root binds, then recovery rescans. No timer: idle stays event-only.
   */
  #armRootAncestor(): void {
    const target = this.options.config.residencyRoot;
    const close = (): void => {
      const watcher = this.#watchers.get(ROOT_ANCESTOR_KEY);
      if (watcher) { try { watcher.close(); } catch { /* already closed */ } }
      this.#watchers.delete(ROOT_ANCESTOR_KEY);
      this.#rootAncestor = undefined;
    };
    if (this.#watchers.has(target)) { close(); return; }
    let ancestor = path.dirname(target);
    while (!isDirectory(ancestor)) {
      const parent = path.dirname(ancestor);
      if (parent === ancestor) return;
      ancestor = parent;
    }
    if (this.#rootAncestor === ancestor && this.#watchers.has(ROOT_ANCESTOR_KEY)) return;
    close();
    const next = path.relative(ancestor, target).split(path.sep)[0]!;
    let watcher: fs.FSWatcher;
    try {
      watcher = fs.watch(ancestor, (_event, name) => {
        if (this.#closed || this.#watchers.get(ROOT_ANCESTOR_KEY) !== watcher) return;
        this.#watchRearmAttempts = 0;
        if (name === null || String(name) === next) this.#rootAppeared();
      });
    } catch (error) {
      this.#watchFailed(error);
      return;
    }
    watcher.on("error", (error) => this.#watchFailed(error));
    watcher.unref?.();
    this.#watchers.set(ROOT_ANCESTOR_KEY, watcher);
    this.#rootAncestor = ancestor;
    // A component created between the scan and binding is not lost.
    if (fs.existsSync(path.join(ancestor, next))) this.#rootAppeared();
  }

  #rootAppeared(): void {
    if (this.#closed) return;
    this.#armWatches();
    // Root now bound: rescan owner/outbox/durable work it may already hold.
    if (this.#watchers.has(this.options.config.residencyRoot)) this.#wakeWatchdog();
  }

  /** Bind directory watches that exist now; missing ones bind on a later event/settlement. */
  #armWatches(): void {
    if (this.#closed || !this.#started || this.#watchRearm) return;
    const meshRoot = this.options.config.meshRoot;
    const completions = path.join(meshRoot, "agent-completions");
    const residency = this.options.config.residencyRoot;
    const bindings: Array<[string, (name: string | null) => void]> = [
      [meshRoot, (name) => {
        if (name === "agent-completions") this.#armWatches();
        if (name === null || name === "agent-completions" || MESH_STATE_FILES.has(name)) this.#notifyDelivery();
      }],
      [completions, (name) => {
        if (name === "attempts") this.#armWatches();
        if (name === null || name === "attempts" || name.endsWith(".json")) this.#notifyDelivery();
      }],
      [path.join(completions, "attempts"), (name) => { if (name === null || name.endsWith(".json")) this.#notifyDelivery(); }],
      [residency, (name) => {
        if (name === "delivery-outbox") this.#armWatches();
        if (name === null || OWNER_FILES.has(name)) this.#wakeWatchdog();
      }],
      [path.join(residency, "delivery-outbox"), (name) => { if (name === null || name.endsWith(".json")) this.#wakeWatchdog(); }],
    ];
    for (const [directory, callback] of bindings) {
      if (this.#watchers.has(directory)) continue;
      let watcher: fs.FSWatcher;
      try {
        watcher = fs.watch(directory, (_event, name) => {
          if (this.#closed) return;
          this.#watchRearmAttempts = 0; // a delivered notification proves the watch works
          callback(name === null ? null : String(name));
        });
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code === "ENOENT") continue; // bound when it appears
        this.#watchFailed(error);
        return;
      }
      watcher.on("error", (error) => this.#watchFailed(error));
      watcher.unref?.();
      this.#watchers.set(directory, watcher);
    }
    this.#armRootAncestor();
  }

  /** Close all, rescan and re-arm; exhaustion keeps the shared degraded-only minute carrier. */
  #watchFailed(error: unknown): void {
    this.#closeWatches();
    if (this.#closed || this.#observation.degraded) return;
    const attempt = this.#watchRearmAttempts++;
    const giveUp = attempt >= WATCH_REARM_ATTEMPTS;
    if (!this.#watchWarned) {
      this.#watchWarned = true;
      const cause = (error as NodeJS.ErrnoException | undefined)?.code === "ENOSPC"
        ? "ENOSPC: inotify watch limit, not disk space (raise fs.inotify.max_user_watches)"
        : error instanceof Error ? error.message : String(error);
      diagnostics.warn("src/residency/client.ts", `[pi-fabric] resident delivery: filesystem notifications failed for ${this.options.config.residencyRoot} (${cause}); ` +
        `rescanned now and re-arming at most ${WATCH_REARM_ATTEMPTS} times with backoff. Normal operation has no polling; ` +
        "exhausted watches scan and re-arm once per minute.");
    }
    this.#wakeDelivery();
    this.#wakeWatchdog();
    if (giveUp) { this.#observation.enter(); return; }
    this.#watchRearm = setTimeout(() => {
      this.#watchRearm = undefined;
      this.#armWatches();
      // Rescan after re-arming: a change between failure and re-arm is not lost.
      this.#wakeDelivery();
      this.#wakeWatchdog();
    }, Math.min(WATCH_REARM_MAX_MS, WATCH_REARM_BASE_MS * 2 ** attempt));
    this.#watchRearm.unref();
  }

  #durableWork(): string | undefined {
    const work: string[] = [];
    const config = this.options.config;
    const actorRoots = [config.actorRoot, config.sessionActorRoot ??
      (config.mesh.actorScope === "session" ? path.dirname(config.actorRoot) : path.join(config.actorRoot, config.sessionId))];
    for (const root of actorRoots) {
      const registry = readJson<{ actors?: Array<{ id?: string; rootId?: string; residency?: string; status?: string }> }>(path.join(root, "actors.json"));
      for (const actor of registry?.actors ?? []) {
        if (actor.rootId === config.rootId && actor.residency === "durable" && actor.status !== "stopped" && actor.status !== "dormant") {
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

  #recipient(config: ResidentHostConfig): CompletionRecipient {
    const name = (config === this.options.config ? this.options.mainName?.() : undefined) ?? config.mainName;
    // lastKnown performs a fresh full-fleet scan. Current Main configs already supply
    // both fields; idle completion polls need that scan only for legacy metadata.
    // Keep the live name callback uncached so renames still bind new admissions.
    const original = name === undefined || config.mainStartedAt === undefined
      ? this.options.participants?.lastKnown?.(config.rootId)?.participant : undefined;
    return { rootId: config.rootId, sessionId: config.sessionId, cwd: config.cwd, projectRoot: config.projectRoot,
      name: name ?? original?.name ?? "main", role: config.role,
      startedAt: config.mainStartedAt ?? original?.startedAt ??
        (/^[a-f0-9]{8}-[a-f0-9]{4}-7[a-f0-9]{3}-/.test(config.sessionId)
          ? Number.parseInt(config.sessionId.replaceAll("-", "").slice(0, 12), 16) : 0) };
  }

  /** Import authenticated legacy resident envelopes too: upgrading must not strand B72 work. */
  async #adoptCompletion(entry: MeshStateEntry): Promise<void> {
    const value = entry.value as Partial<ResidentDeliveryRecord> | undefined;
    if (!value || value.format !== RESIDENT_HOST_FORMAT || typeof value.rootId !== "string" ||
      !value.from || value.from.kind !== "agent" || !AGENT_ID_PATTERN.test(value.from.id) ||
      entry.updatedBy.id !== residentHostId(value.rootId) ||
      !entry.key.startsWith(residentDeliveryPrefix(value.rootId))) return;
    const id = value.agentCompletionId ?? value.from.id;
    if (id !== value.from.id) return;
    if (completionConsumed(this.options.config.meshRoot, id)) {
      await this.options.mesh.delete({ key: entry.key, ifVersion: entry.version });
      return;
    }
    const root = residentRoot(this.options.config.meshRoot, value.rootId);
    const config = readJson<ResidentHostConfig>(path.join(root, "config.json"));
    if (!config || config.rootId !== value.rootId || path.resolve(config.residencyRoot) !== root ||
      typeof config.projectRoot !== "string" || typeof config.cwd !== "string" ||
      !samePath(config.projectRoot, this.options.config.projectRoot)) return;
    if (legacyCompletionConsumed(this.options.config.meshRoot, value.rootId, id)) {
      consumeCompletion(this.options.config.meshRoot, id, config.sessionId);
      return;
    }
    const result = readJson<AgentRunResult>(residentResultPath(root, id)) ??
      readJson<AgentRunResult>(path.join(root, "runs", id, "status.json"));
    if (!result || result.id !== id || !terminal(result.status)) return;
    const admitted = completionRecipientFromRun(this.options.config.meshRoot, path.join(root, "runs", id));
    saveCompletion(this.options.config.meshRoot, admitted ?? this.#recipient(config), result);
  }

  #drainDeliveries(): Promise<void> {
    if (this.#deliveryPass) return this.#deliveryPass;
    if (this.#closed || !this.options.mainAgent.local) return Promise.resolve();
    return this.#deliveryPass = this.#drainDeliveryPass().finally(() => { this.#deliveryPass = undefined; });
  }

  async #drainDeliveryPass(): Promise<void> {
    try {
      const entries = this.options.mesh.listAll("residency/deliveries/");
      let fault: unknown;
      for (const entry of entries) {
        try {
          // Exact-owner actor recovery also scans a stale foreign mailbox key: an
          // upgraded host may have written it before the exact-root contract.
          const value = entry.value as Partial<ResidentDeliveryRecord> | undefined;
          const ownerActorRecord = value?.format === RESIDENT_HOST_FORMAT && value.from?.kind === "actor" &&
            entry.updatedBy.id === this.hostId;
          if (entry.key.startsWith(this.#deliveryPrefix) || ownerActorRecord) await this.#deliver(entry);
          else await this.#adoptCompletion(entry);
        } catch (error) {
          // The record remains durable. Back off a locked mesh; retain ordinary failed senders
          // without blocking the other entries in this pass.
          if (isMeshWriteDeferred(error)) throw error;
          fault ??= error;
        }
      }
      await this.#completions.drain(this.options.config.agents.notifyOnComplete);
      if (fault !== undefined) throw fault; // Legacy-import faults need the same deduplicated diagnostic.
      this.#completionFault = undefined;
    } catch (error) {
      if (isMeshWriteDeferred(error)) throw error; // Let the owned background retry back off the outage.
      const diagnostic = `Fabric completion remains pending: ${String(error).slice(0, 1000)}`;
      if (diagnostic !== this.#completionFault) diagnostics.warn("src/residency/client.ts", diagnostic);
      this.#completionFault = diagnostic;
    }
  }

  async #deliver(entry: MeshStateEntry): Promise<void> {
    if (typeof entry.value !== "object" || entry.value === null || Array.isArray(entry.value)) return;
    const value = entry.value as Partial<ResidentDeliveryRecord>;
    if (
      value.format !== RESIDENT_HOST_FORMAT ||
      (value.rootId !== this.options.config.rootId &&
        !(value.from && value.from.kind === "actor" && entry.updatedBy.id === this.hostId)) ||
      typeof value.id !== "string" ||
      typeof value.message !== "string" ||
      (value.delivery !== "steer" && value.delivery !== "followUp") ||
      typeof value.triggerTurn !== "boolean" ||
      typeof value.from !== "object" ||
      value.from === null ||
      // Only the exact owner's resident host may admit a delivery. In particular,
      // retained actor output from an older inferred route stays durable until the
      // producing root returns; a foreign Main never consumes it.
      entry.updatedBy.id !== this.hostId
    ) {
      return;
    }
    const data = value.data as Partial<AgentRunResult> | undefined;
    // Also recognize envelopes written by an older resident host.
    const completionId = value.agentCompletionId ?? (data && typeof data.status === "string" &&
      terminal(data.status) && typeof data.startedAt === "number" ? data.id : undefined);
    if (value.from.kind === "agent" && typeof completionId === "string" && completionId === value.from.id) {
      const metadata = this.#metadata(completionId);
      if (metadata?.completionConsumedAt) this.#completions.acknowledge(completionId);
      if (metadata?.completionConsumedAt || completionConsumed(this.options.config.meshRoot, completionId)) {
        await this.options.mesh.delete({ key: entry.key, ifVersion: entry.version });
        return;
      }
      // No result/notifications disabled is not a receipt. Keep the source pending, never drop it.
      if (!metadata || !this.options.config.agents.notifyOnComplete) return;
      // One logical completion key across resident envelopes and the session inbox. Keep
      // the source until Main consumes it, not just until its in-memory inbox accepts it.
      const result = this.statusAgent(completionId);
      if (terminal(result.status) && "startedAt" in result) this.#completions.save(result as AgentRunResult);
      return;
    }
    // smarty-dev#2236: one record reached Main twice (a failed delete, or a second drainer that
    // listed it through the 2 s read cache before the delete). The record stays the durable copy
    // until Main durably admitted it: deliverAgent journals it under the record's stable id before
    // it returns, keeps it until the session holds it, and admits one id once across restarts and
    // release reloads (review round 2 on pi-fabric#160). Only then is the record deleted.
    // Ignore the resident payload's optional wakeCause. The owner-authenticated
    // writer emits host notices; positively classified output delegates the actor
    // identity under the existing resident admission policy. Topic/key are the
    // receiving storage route and envelope key, never producer diagnostic fields.
    const wakeCause = fabricWakeCause(value.source === "actor-output" ? value.from : entry.updatedBy,
      value.source === "fabric-host" ? "host-event" : value.source === "actor-output" ? "actor" : value.delivery,
      "fabric.resident.delivery", entry.key);
    this.options.mainAgent.deliverAgent(withFabricWakeAdmission({
      from: value.from,
      source: value.source,
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
    }, [wakeCause]));
    await this.options.mesh.delete({ key: entry.key, ifVersion: entry.version });
  }
}
