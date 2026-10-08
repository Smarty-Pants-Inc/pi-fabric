import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { RootInbox, type RootInboxBatch, type RootInboxSession } from "./topology/root-inbox.js";
import { MainInboxMaintenance, registerMainInbox, recordMainSuccessor, mainInboxOwns, mainInboxActive, rootPresenceAlarms, stageMainSuccessor, confirmMainSuccessor } from "./topology/stall-alarms.js";
import type { RecordsService } from "./records/service.js";
import { recordsInboxMessage, recordsInboxSession, type RecordsInboxBatch, type RecordsInboxSession } from "./records/inbox.js";
import { RECORDS_DISABLED_HINT } from "./records/config.js";
import { RecordsProvider } from "./providers/records-provider.js";
import { closeWithActors } from "./actors/close-order.js";
import { OutputArtifactStore } from "./output-budget.js";
import { resolveAgentDir } from "./core/agent-dir.js";
import { recordMainRelease } from "./lifecycle/release-process.js";
import { loadedFabricRoot } from "./core/agent-dir.js";
import type { FabricModelCandidate } from "./core/model-resolution.js";
import { resolvePiModel, resolvePiRoutePin } from "./core/model-refresh.js";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { FabricActivityStore } from "./activity/store.js";
import { ActorDirectory } from "./actors/directory.js";
import type { ActorModelRouteInput } from "./actors/manager.js";
import { resolvePiBinary } from "./agents/pi-binary.js";
import { isPiShellRef } from "./core/pi-tools.js";
import { DEFAULT_SHELL_HANG_MS, FabricShellJobStore } from "./core/shell-jobs.js";
import { GlobalActorRegistry } from "./actors/global-registry.js";
import { buildActorContext } from "./actors/context.js";
import { actorDeliveryNotice } from "./actors/delivery-policy.js";
import { prepareFabricActorHostPayload } from "./actors/host-event-payload.js";
import type { JevObservationHost } from "./jev/observation.js";
import type { JevProgramManager } from "./jev/manager.js";
import { resolveJevModelRoute } from "./jev/routes.js";
import type { FabricActorHostEvent } from "./actors/types.js";
import { CapturedToolCatalog, type CapturedToolEntry } from "./capture/catalog.js";
import { FabricComponentCatalog } from "./components/catalog.js";
import { FabricComponentLoader } from "./components/loader.js";
import { FabricComponentControl } from "./components/control.js";
import { FabricComponentConfiguration, watchComponentConfiguration } from "./components/configuration.js";
import {
  resolveFabricModelGuidance,
  type FabricOwnedModelGuidance,
} from "./components/model-guidance.js";
import { FabricComponentSupervisor } from "./components/supervisor.js";
import {
  createProviderComponent,
  FABRIC_COMPONENT_PROVIDER_NAMES,
  FABRIC_PROVIDER_COMPONENT_PREFIX,
  FabricProviderComponentManifest,
} from "./components/provider-component.js";
import type {
  FabricComponentDefinition,
  FabricComponentGraph,
  FabricComponentInfo,
} from "./components/types.js";
import {
  DEFAULT_FABRIC_CONFIG,
  loadFabricConfig,
  liveLandlockSettings,
  type FabricConfig,
  type FabricResultFormat,
  type FabricSchemaMode,
} from "./config.js";
import {
  ActionRegistry,
  type FabricCapabilityViewLease,
} from "./core/action-registry.js";
import { ApprovalController, FabricSessionApprovals } from "./core/approval-controller.js";
import { runAbortable } from "./async-settlement.js";
import { CompactController, type CompactLastCommit, type CompactPendingIntent } from "./core/compact-controller.js";
import { FabricToolResultProxy } from "./core/tool-result-proxy.js";
import { FabricExecutionService, type FabricExecutionResult } from "./execution-service.js";
import { RepairCompiler } from "./repairs/compiler.js";
import {
  clearActiveRepairCompiler,
  setActiveRepairCompiler,
} from "./repairs/active.js";
import { loadCompiledSurface } from "./entropy/compiled-store.js";
import {
  clearActiveCompiledSurface,
  setActiveCompiledSurface,
} from "./entropy/active.js";
import { RuntimeStateSpeculation } from "./runtime-state-speculation.js";
import { schemaRefAllowedInEnforce } from "./schema/policy.js";
import type { FabricSpeculationStreamTap } from "./speculation/stream-tap.js";
import { MeshStore, type MeshIdentity } from "./mesh/store.js";
import { MeshBackgroundQueue, MeshBackgroundRetry } from "./core/atomic-write.js";
import { LifecycleBroker } from "./lifecycle/broker.js";
import type { FabricLifecycleEventType } from "./lifecycle/types.js";
import { FabricControlPlane } from "./topology/control-plane.js";
import { ParticipantDirectory } from "./topology/participant-directory.js";
import { rootParticipantName } from "./topology/participant-name.js";
import type {
  FabricParticipantInfo,
  FabricParticipantListOptions,
  FabricPeerInfo,
} from "./topology/types.js";
import { actorParticipantRecord, agentParticipantRecords } from "./topology/records.js";
import {
  PrewalkController,
  type FabricPrewalkPlanCheckpoint,
} from "./prewalk/controller.js";
import { PrewalkDriftTracker } from "./prewalk/fs-drift.js";
import {
  deliverPrewalkPlanCheckpoint,
} from "./prewalk/messages.js";
import {
  claimFabricFsDriftHandoff,
  claimFabricHandoff,
  runFabricHandoffAtBoundary,
  type PendingFabricHandoff,
} from "./prewalk/handoff.js";
import type { AgentToolResultMessage } from "./agents/types.js";
import {
  MainAgentController,
  resolveFabricIdentity,
  type FabricAgentMessageDelivery,
  type FabricAgentMessageResult,
  type FabricMainAgentInfo,
} from "./main-agent.js";
import { followUpDrainSupported } from "./host-compatibility.js";
import { deliverActorToMain } from "./actors/main-delivery.js";
import { sendFabricMessage } from "./fabric-provenance.js";
import { AgentsProvider } from "./providers/agents-provider.js";
import { CompactProvider } from "./providers/compact-provider.js";
import { CacheProvider } from "./providers/cache-provider.js";
import { PrewalkProvider } from "./providers/prewalk-provider.js";
import { ComponentsProvider } from "./providers/components-provider.js";
import type { McpProviderHooks } from "./providers/mcp-provider.js";
import { RuntimeStateBuiltins } from "./runtime-state-builtins.js";
import { SchemaProvider } from "./providers/schema-provider.js";
import { SchemaController } from "./schema/controller.js";
import { StateStore } from "./state/store.js";
import {
  FABRIC_COMPONENT_DISCOVER_EVENT,
  FABRIC_PROVIDER_DISCOVER_EVENT,
  type FabricActionDescriptor,
  type FabricComponentDiscovery,
  type FabricProvider,
  type FabricProviderDiscovery,
} from "./protocol.js";
import { participantProject, ParticipantRoleGrant } from "./topology/project-identity.js";
import { AgentManager } from "./agents/manager.js";
import { AgentCompletionInbox } from "./agents/completion-inbox.js";
import { ActorChildCompletionStore } from "./actors/child-completions.js";
import { resolveAgentSpawner } from "./agents/spawner.js";
import { rememberStoppedAtClose, restoreStoppedRuns, STOPPED_AGENTS_ENTRY, type StoppedAgentsEntryData } from "./agents/stopped-runs.js";
import { ShellEventInbox } from "./core/shell-inbox.js";
import { resolveInheritedSessionPins } from "./agents/session-pins.js";
import { ResidencyClient } from "./residency/client.js";
import { isOwnResidentActor } from "./residency/actor-ownership.js";
import { RESIDENT_HOST_FORMAT, residentRoot } from "./residency/protocol.js";
import type { FabricRuntimePaths } from "./runtime-paths.js";

const inheritedCapabilityRequirements = (): string[] => {
  const source = process.env.PI_FABRIC_CAPABILITY_REQUIREMENTS;
  if (!source) return [];
  const parsed: unknown = JSON.parse(source);
  if (!Array.isArray(parsed) || parsed.length > 128) {
    throw new Error("PI_FABRIC_CAPABILITY_REQUIREMENTS must be an array of at most 128 refs");
  }
  const refs = parsed.filter((value): value is string => typeof value === "string");
  if (refs.length !== parsed.length || refs.some((ref) => ref.length > 256 || !ref.includes("."))) {
    throw new Error("PI_FABRIC_CAPABILITY_REQUIREMENTS contains an invalid provider.action ref");
  }
  return [...new Set(refs)];
};

const escapeXmlText = (value: string): string =>
  value.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");


import type { FabricManagedHost } from "./managed-host.js";
import { captureLoadedFileIdentity, type FabricLoadedFileIdentity } from "./build-identity.js";

// Loaded-code identity of this lazy runtime module. Stable-path lazy imports
// keep their first evaluation for the life of the host process, so a hash
// captured at activation is the ground truth a reload-freshness check compares
// the current disk file against.
const FABRIC_RUNTIME_MODULE_IDENTITY = captureLoadedFileIdentity(import.meta.url);

export interface FabricRuntimeStateOptions {
  roleGrant?: ParticipantRoleGrant;
  managedHost?: FabricManagedHost;
  activity?: FabricActivityStore;
  prewalk?: PrewalkController;
  prewalkDrift?: PrewalkDriftTracker;
  sessionApprovals?: FabricSessionApprovals;
  paths?: FabricRuntimePaths;
  entryIdentity?: FabricLoadedFileIdentity;
}

// ponytail: 10 min covers a reload wave's gap; a longer downtime is future-only by design.
const MAIN_ACTOR_MESH_REPLAY_MS = 10 * 60_000;

// A detached callback can outlive its ctx (reload, session replacement). Reading a stale
// ctx throws, and a throw there is uncaught and exits Pi, so the notice is dropped.
const notifyDetached = (context: ExtensionContext, message: string): void => {
  try {
    if (context.hasUI) context.ui.notify(message, "error");
  } catch {
    // Stale ctx: the runtime that owned this callback is closing.
  }
};

export class FabricRuntimeState {
  readonly #roleGrant: ParticipantRoleGrant;
  #registry: ActionRegistry | undefined;
  #config: FabricConfig | undefined;
  #execution: FabricExecutionService | undefined;
  #repairs: RepairCompiler | undefined;
  #speculation: RuntimeStateSpeculation | undefined;
  #agents: AgentManager | undefined;
  #completionInbox: AgentCompletionInbox | undefined;
  #shellInbox: ShellEventInbox | undefined;
  #actors: ActorDirectory | undefined;
  #jevObservationHost: JevObservationHost | undefined;
  #jevPrograms: JevProgramManager | undefined;
  #globalActors: GlobalActorRegistry | undefined;
  #rootInbox: RootInbox | undefined;
  #records: Promise<RecordsService> | undefined;
  #openRecords: (() => Promise<RecordsService>) | undefined;
  #mesh: MeshStore | undefined;
  #disposableMeshWrites: AbortController | undefined;
  #backgroundMesh = new MeshBackgroundQueue("runtime lifecycle/compaction");
  readonly #inboxRetry = new MeshBackgroundRetry("root inbox cursor");
  #identity: MeshIdentity | undefined;
  #mainAgent: MainAgentController | undefined;
  #participants: ParticipantDirectory | undefined;
  #control: FabricControlPlane | undefined;
  #lifecycle: LifecycleBroker | undefined;
  #residency: ResidencyClient | undefined;
  #agentsProvider: AgentsProvider | undefined;
  #compact: CompactController | undefined;
  #schema: SchemaController | undefined;
  #componentSupervisor: FabricComponentSupervisor | undefined;
  #componentLoader: FabricComponentLoader | undefined;
  #componentControl: FabricComponentControl | undefined;
  #componentConfiguration: FabricComponentConfiguration | undefined;
  #stopComponentWatch: (() => void) | undefined;
  readonly #componentTransitionSignatures = new Map<string, string>();
  readonly #componentTransitionPublications = new Set<Promise<void>>();
  #sessionCapabilityLease: FabricCapabilityViewLease | undefined;
  #unsubscribeCapturedCatalog: (() => void) | undefined;
  #cwd: string | undefined;
  readonly #externalProviders = new Map<string, FabricProvider>();
  readonly #builtinComponentNames = new Set<string>();
  readonly componentCatalog = new FabricComponentCatalog();
  readonly activity: FabricActivityStore;
  #outputArtifacts = new OutputArtifactStore();
  get outputArtifactWriter(): OutputArtifactStore["write"] { return this.#outputArtifacts.write; }
  #shellJobs = new FabricShellJobStore();
  get shellJobs(): FabricShellJobStore { return this.#shellJobs; }
  readonly prewalk: PrewalkController;
  readonly prewalkDrift: PrewalkDriftTracker;
  readonly sessionApprovals: FabricSessionApprovals;
  readonly #paths: FabricRuntimePaths | undefined;
  readonly #managedHost: FabricManagedHost | undefined;
  readonly #entryIdentity: FabricLoadedFileIdentity | undefined;
  #widgetDismissedAt = 0;
  #suppressResidentGuidanceSync = false;
  // smarty-dev#5962: background timers (participant heartbeat/change refresh, mesh read
  // pacing, root inbox naming) outlive a /reload or session replacement. They never hold
  // a ctx: each tick reads the ctx bound by the latest activation/ensure, and only while
  // that lifecycle lease is current. A retired lease skips the tick quietly.
  #binding: { context: ExtensionContext; current: () => boolean } | undefined;
  // Each initialize() starts a new epoch. Sources and closures built by an older epoch never
  // read the live ctx again, even after the runtime is rebound to a successor session.
  #epoch = 0;

  constructor(
    readonly pi: ExtensionAPI,
    readonly capturedTools: CapturedToolCatalog,
    options: FabricRuntimeStateOptions = {},
  ) {
    this.#roleGrant = options.roleGrant ?? new ParticipantRoleGrant();
    this.activity = options.activity ?? new FabricActivityStore();
    this.prewalk = options.prewalk ?? new PrewalkController();
    this.prewalkDrift = options.prewalkDrift ?? new PrewalkDriftTracker();
    this.sessionApprovals = options.sessionApprovals ?? new FabricSessionApprovals();
    this.#paths = options.paths;
    this.#managedHost = options.managedHost;
    this.#entryIdentity = options.entryIdentity;
  }

  get initialized(): boolean {
    return Boolean(this.#execution);
  }

  get widgetDismissedAt(): number {
    return this.#widgetDismissedAt;
  }

  set widgetDismissedAt(value: number) {
    this.#widgetDismissedAt = value;
  }

  get cwd(): string | undefined {
    return this.#cwd;
  }

  get config(): FabricConfig {
    if (!this.#config) throw new Error("Pi Fabric has not initialized");
    return this.#config;
  }

  /** Stream tap for speculative PTC; undefined when speculation is disabled. */
  get speculationTap(): FabricSpeculationStreamTap | undefined {
    return this.#speculation?.tap;
  }

  /** Turn-boundary backstop: tap state and unserved entries never outlive a turn. */
  resetSpeculation(): void {
    this.#speculation?.reset();
  }

  get registry(): ActionRegistry {
    if (!this.#registry) throw new Error("Pi Fabric has not initialized");
    return this.#registry;
  }

  get components(): FabricComponentLoader {
    if (!this.#componentLoader) throw new Error("Pi Fabric has not initialized");
    return this.#componentLoader;
  }

  get execution(): FabricExecutionService {
    if (!this.#execution) throw new Error("Pi Fabric has not initialized");
    return this.#execution;
  }

  get agents(): AgentManager {
    if (!this.#agents) throw new Error("Pi Fabric has not initialized");
    return this.#agents;
  }

  get actors(): ActorDirectory {
    if (!this.#actors) throw new Error("Pi Fabric has not initialized");
    return this.#actors;
  }

  get globalActors(): GlobalActorRegistry {
    if (!this.#globalActors) throw new Error("Pi Fabric has not initialized");
    return this.#globalActors;
  }

  get mesh(): MeshStore {
    if (!this.#mesh) throw new Error("Pi Fabric has not initialized");
    return this.#mesh;
  }

  mainAgentInfo(context?: ExtensionContext): FabricMainAgentInfo {
    if (!this.#mainAgent) throw new Error("Pi Fabric has not initialized");
    return this.#mainAgent.info(context);
  }

  peerInfos(options: FabricParticipantListOptions = {}): FabricPeerInfo[] {
    return this.#participants?.peers(undefined, options) ?? [];
  }

  /**
   * The inbox batch this Main should see now (smarty-dev#754); undefined when it has no inbox.
   * With `idle`, the batch an idle Main wakes for (smarty-dev#1595), under the wake cooldown.
   */
  async nextRootInbox(session: RootInboxSession, idle?: () => boolean): Promise<RootInboxBatch | undefined> {
    let batch: RootInboxBatch | undefined;
    await this.#inboxRetry.run(async () => {
      batch = await (idle ? this.#rootInbox?.wake(session, idle) : this.#rootInbox?.next(session));
    });
    return batch;
  }

  /** The host's gated idle wake for records (F21); unset, the watchdog starts no turn. */
  recordsWake: (() => Promise<void>) | undefined;

  /** The records addressed to this root past its processing cursor (smarty-dev#754 C4). */
  async nextRecordsInbox(session: RecordsInboxSession): Promise<RecordsInboxBatch | undefined> {
    if (!this.#openRecords) return undefined;
    const service = await this.#openRecords();
    return service.inbox?.next(session);
  }

  /** The same, as the one message that brings the batch into the session; undefined when empty. */
  async nextRecordsInboxMessage(entries: readonly unknown[]): Promise<ReturnType<typeof recordsInboxMessage> | undefined> {
    const batch = await this.nextRecordsInbox(recordsInboxSession(entries));
    return batch?.records.length ? recordsInboxMessage(batch.records) : undefined;
  }

  async #closeRecords(): Promise<void> {
    const opening = this.#records;
    this.#records = undefined;
    this.#openRecords = undefined;
    await opening?.then((service) => service.close()).catch(() => undefined);
  }

  /** Why peer visibility is unknown (a stalled mesh writer), or undefined when healthy. */
  writeStalled(): Error | undefined {
    return this.#participants?.writeStalled();
  }

  /** When this host last committed its mesh heartbeat, if it has a directory. */
  participantsConfirmedAt(): number | undefined {
    return this.#participants?.confirmedAt();
  }

  componentGraph(): FabricComponentGraph {
    return this.#componentLoader?.graph() ?? { components: [], edges: [], cycles: [] };
  }

  modelGuidance(): FabricOwnedModelGuidance[] {
    return this.#componentSupervisor?.guidance() ?? [];
  }

  participantInfos(options: FabricParticipantListOptions = {}): FabricParticipantInfo[] {
    return this.#participants?.list(options) ?? [];
  }

  async queueUserMessage(
    targetId: string,
    message: string,
    delivery: FabricAgentMessageDelivery,
  ): Promise<FabricAgentMessageResult> {
    if (!this.#mainAgent || !this.#agentsProvider) {
      throw new Error("Pi Fabric has not initialized");
    }
    if (this.#mainAgent.matches(targetId) && this.#mainAgent.local) {
      return this.#mainAgent.deliverUser(message, delivery);
    }
    return this.#agentsProvider.routeMessage(targetId, message, undefined, delivery);
  }

  async reportMainProviderError(message: string): Promise<unknown> {
    return this.#agentsProvider?.reportMainProviderError(message);
  }

  async stopParticipant(targetId: string): Promise<unknown> {
    if (!this.#agentsProvider) throw new Error("Pi Fabric has not initialized");
    return this.#agentsProvider.stopParticipant(targetId);
  }

  get compact(): CompactController {
    if (!this.#compact) throw new Error("Pi Fabric has not initialized");
    return this.#compact;
  }

  get repairs(): RepairCompiler {
    if (!this.#repairs) throw new Error("Pi Fabric has not initialized");
    return this.#repairs;
  }

  /** Rebinds session-bound background reads to this ctx while `current()` holds (smarty-dev#5962). */
  bindLifecycle(context: ExtensionContext, current: () => boolean): void {
    this.#binding = { context, current };
  }

  /** False once the bound lifecycle retired: background ticks must skip, not read a stale ctx. */
  get lifecycleCurrent(): boolean {
    const binding = this.#binding;
    if (!binding) return false;
    try { return binding.current(); } catch { return false; }
  }

  /**
   * Session-bound reads for one initialize() epoch. Reads go through the live ctx/pi only while
   * the epoch and the bound lease are current; a retired epoch, a retired lease, or a stale read
   * yields undefined, and the session name falls back to this epoch's own last live value.
   */
  #liveReads(epoch: number): {
    current: () => boolean;
    read: <T>(read: (context: ExtensionContext) => T) => T | undefined;
    sessionName: () => string | undefined;
  } {
    const current = (): boolean => epoch === this.#epoch && this.lifecycleCurrent;
    const read = <T>(read: (context: ExtensionContext) => T): T | undefined => {
      const context = current() ? this.#binding?.context : undefined;
      if (!context) return undefined;
      // The token cannot observe every host invalidation; a stale read is a quiet skip too.
      try { return read(context); } catch { return undefined; }
    };
    let sessionName: string | undefined;
    return {
      current,
      read,
      sessionName: () => {
        const live = read(() => ({ name: this.pi.getSessionName?.() }));
        if (live) sessionName = live.name;
        return sessionName;
      },
    };
  }

  async initialize(
    context: ExtensionContext,
    bootstrapConfig?: FabricConfig,
    options: { lifecycle?: () => boolean } = {},
  ): Promise<void> {
    // FabricState passes its activation lease; direct callers (tests, standalone hosts) keep a
    // lease bound to this same ctx, else bind for the runtime's life.
    const successor = {
      context,
      current: options.lifecycle
        ?? (this.#binding?.context === context ? this.#binding.current : () => true),
    };
    // Session replacement (smarty-dev#5962): retire the predecessor binding and epoch BEFORE
    // teardown. Quiesce, close and in-flight old sources then read no ctx at all, never the
    // successor's; the successor lease is installed only once the old runtime is torn down.
    this.#epoch += 1;
    const epoch = this.#epoch;
    this.#binding = undefined;
    const live = this.#liveReads(epoch);
    const predecessor = this.#mainAgent?.local && this.#mainAgent.sessionId && this.#mesh
      ? { id: this.#mainAgent.id, sessionId: this.#mainAgent.sessionId, meshRoot: this.#mesh.root, cwd: this.#mainAgent.cwd } : undefined;
    this.#suppressResidentGuidanceSync = true;
    try {
      await this.#closeInternal();
      this.#shellJobs = new FabricShellJobStore();
      this.#outputArtifacts = new OutputArtifactStore();
    } finally {
      this.#suppressResidentGuidanceSync = false;
    }
    // A newer initialize() owns the binding now; this superseded one must not install its ctx.
    if (epoch === this.#epoch) this.#binding = successor;
    for (const name of this.#builtinComponentNames) this.componentCatalog.unregister(name);
    this.#builtinComponentNames.clear();
    this.prewalk.cancel();
    this.prewalkDrift.clear();
    context.ui.setStatus("fabric-prewalk", undefined);
    this.#speculation?.reset();
    this.#speculation = undefined;
    this.activity.reset();
    this.sessionApprovals.reset();
    this.#cwd = context.cwd;
    const projectTrusted = this.#managedHost ? false : context.isProjectTrusted();
    this.#managedHost?.seal();
    this.#config = this.#managedHost?.config() ?? bootstrapConfig ?? loadFabricConfig({
      cwd: context.cwd,
      agentDir: resolveAgentDir(),
      projectTrusted,
    });
    this.#registry = new ActionRegistry(
      new FabricToolResultProxy(() => this.capturedTools.runner),
    );
    this.#configureSpeculation();
    this.#unsubscribeCapturedCatalog?.();
    this.#unsubscribeCapturedCatalog = this.capturedTools.subscribe(() => {
      this.#registry?.notifyCatalogChanged("extensions");
      this.#refreshRepairCatalog();
    });
    this.#componentSupervisor = new FabricComponentSupervisor(this.#registry, {
      invocationContext: () => ({
        cwd: context.cwd,
        signal: undefined,
        parentToolCallId: "fabric-component",
        nestedToolCallId: "fabric-component",
        extensionContext: context,
        update() {},
      }),
      maxResultChars: this.#config.executor.maxNestedResultChars,
      acquire: async (ref, args, invocation) => {
        const action = await this.#registry!.describe(ref, invocation);
        await this.#schema?.authorize(action.ref, invocation.parentToolCallId);
        return this.#registry!.acquireScoped(ref, args, invocation);
      },
      invoke: (ref, args, invocation) => this.#registry!.invoke(ref, args, {
        ...invocation,
        ...(this.#schema
          ? { authorize: (action) => this.#schema!.authorize(action.ref, invocation.parentToolCallId) }
          : {}),
        approve: async () => {},
        audits: [],
        maxResultChars: this.#config!.executor.maxNestedResultChars,
      }),
    });
    this.#componentSupervisor.subscribe((componentId) =>
      this.#observeComponentTransitions(componentId),
    );
    this.#componentLoader = new FabricComponentLoader(
      this.componentCatalog,
      this.#componentSupervisor,
    );
    if (!this.#managedHost && this.#config.schema.mode !== "enforce") {
      this.#componentConfiguration = new FabricComponentConfiguration({
        cwd: context.cwd, agentDir: resolveAgentDir(), projectTrusted: () => context.isProjectTrusted(),
      });
    }
    this.#componentControl = new FabricComponentControl(this.#componentLoader, {
      ...(this.#componentConfiguration ? { store: this.#componentConfiguration } : {}),
      initialEntries: this.#config.schema.mode === "enforce" ? [] : this.#config.components,
      assertMutable: () => {
        if (this.#managedHost || this.#config?.schema.mode === "enforce") throw new Error("Live component configuration is unavailable in managed hosts and Schema enforce mode");
      },
      applied: entries => { if (this.#config) this.#config.components = entries; },
    });
    this.#registry.setUnavailableResolver(name => this.#componentLoader?.unavailableProviderMessage(name));
    this.#registry.register(this.#managedHost?.provider("components") ?? new ComponentsProvider(this.#componentLoader, this.#componentControl));
    const builtinManifest = new FabricProviderComponentManifest(
      this.componentCatalog,
      this.#componentLoader,
    );
    const builtins = new RuntimeStateBuiltins(
      builtinManifest,
      this.#registry,
      (name) => this.#builtinComponentNames.add(name),
      this.#managedHost,
    );
    const enforceSchema = this.#config.schema.mode === "enforce";
    await builtins.tools(context.cwd, this.#config, this.capturedTools, {
      jobs: this.shellJobs,
      getHangMs: () => this.#config?.executor.shellHangMs ?? DEFAULT_SHELL_HANG_MS,
      // F2: the host kill switch is re-read per call, never a cached value.
      getLandlockSettings: () => liveLandlockSettings(
        this.#config?.executor.landlock ?? { mode: "off", disabled: false }, resolveAgentDir()),
    });
    if (!this.#managedHost && (this.#config.fullCodeMode || enforceSchema)) {
      this.#shellInbox = new ShellEventInbox(this.pi, context, this.shellJobs);
    }
    // One definition for both host modes: the controller belongs to this
    // runtime state, so managed and normal sessions share a single wiring site.
    await builtins.install(createProviderComponent({
      provider: "prewalk",
      description: "Frontier-first handoff readiness and plan record",
      create: () => new PrewalkProvider(this.prewalk, {
        buildIdentity: () => ({
          entry: this.#entryIdentity ?? null,
          lazyRuntime: FABRIC_RUNTIME_MODULE_IDENTITY,
        }),
      }),
    }));
    if (this.#managedHost) {
      this.#registry.markUnavailable("jev", "Jev programs are unavailable in managed hosts");
      this.#registry.markUnavailable("cache", "Native prompt-cache access is unavailable in managed hosts");
      // Closed-world hosts must never construct unused native managers, stores or model history.
      for (const name of ["agents", "schema", "compact", "memory", "mesh", "state"]) {
        if (["agents", "schema", "compact"].includes(name) || this.#managedHost.has(name)) {
          await builtins.install(createProviderComponent({
            provider: name, description: "Managed host provider",
            create: () => this.#managedHost!.provider(name),
          }));
        } else this.#registry.markUnavailable(name, "unavailable in managed host");
      }
      builtins.assertActive(this.#config);
      await this.#mountExecution(context, false);
      return;
    }
    const sessionId = context.sessionManager.getSessionId();
    const role = this.#roleGrant.roleFor(sessionId, context.cwd);
    const { identity, mainAgentId } = resolveFabricIdentity(sessionId);
    await builtins.install(createProviderComponent({
      provider: "cache",
      description: "Local prompt-cache observations and scoped native warming",
      create: () => new CacheProvider(this.pi, context, identity.kind === "main"),
    }));
    const fabricSessionId = process.env.PI_FABRIC_SESSION_ID?.trim() || sessionId;
    const ownsPersistentActorRegistry =
      identity.kind === "main" &&
      !enforceSchema &&
      projectTrusted &&
      this.#config.mesh.enabled;
    const mainAgent = new MainAgentController(
      this.pi,
      mainAgentId,
      identity.kind === "main" && identity.id === mainAgentId,
      context.cwd,
      identity.kind === "main" ? sessionId : undefined,
      context.mode !== "print" && context.mode !== "json",
      (event) => { void this.publishOpsEvent("fabric.main.wake", "provider-backoff-released", event); },
    );
    this.#mainAgent = mainAgent;
    const projectRoot = process.env.PI_FABRIC_PROJECT_ROOT ?? context.cwd;
    const configuredMeshRoot = this.#config.mesh.root;
    const meshRoot =
      process.env.PI_FABRIC_MESH_ROOT ??
      (configuredMeshRoot
        ? path.resolve(projectRoot, configuredMeshRoot)
        : path.join(projectRoot, ".pi", "fabric", "mesh"));
    this.#backgroundMesh = new MeshBackgroundQueue("runtime lifecycle/compaction");
    this.#disposableMeshWrites = identity.kind === "actor" || identity.kind === "agent"
      ? new AbortController() : undefined;
    this.#mesh = new MeshStore(
      meshRoot,
      this.#config.mesh.maxEventBytes,
      this.#config.mesh.maxReadEvents,
      {
        backgroundReadCacheMs: this.#config.mesh.idleReadCoalesceMs,
        readActive: () => live.read(ctx => !ctx.isIdle() || ctx.hasPendingMessages()) === true ||
          (this.#agents?.runningCount() ?? 0) > 0 || (this.#actors?.inFlightCount() ?? 0) > 0,
        lockProtocol: this.#config.mesh.lockProtocol,
        ...(this.#disposableMeshWrites ? { writeSignal: this.#disposableMeshWrites.signal } : {}),
      },
    );
    // A Main on the shared mesh reconciles the work events a steer missed (smarty-dev#754).
    this.#rootInbox = identity.kind === "main" && mainAgent.local && this.#config.mesh.enabled
      ? new RootInbox(this.#mesh, identity, () => [mainAgentId, live.sessionName() ?? ""])
      : undefined;
    const hostId = identity.kind === "main" ? mainAgentId : `runtime:${sessionId}`;
    let inboxMaintenance: MainInboxMaintenance | undefined;
    this.#participants = new ParticipantDirectory(this.#mesh, {
      presencePass: async () => {
        await rootPresenceAlarms(this.#mesh!, identity, hostId,
          this.#participants!.list({ scope: "project", includeStale: true, fresh: true }), this.#config!.mesh.rootPresenceAlarmMs);
        await inboxMaintenance?.run();
      },
      enabled: this.#config.mesh.enabled,
      hostId,
      rootId: mainAgentId,
      identity,
      onRootCollision: collision => {
        const warning = `Duplicate live Fabric root (${collision.reason}): ${collision.name}; ${collision.ids.join(", ")}. Fixture forks must use PI_FABRIC_FIXTURE=1.`;
        console.warn(`[pi-fabric] ${warning}`);
        live.read(ctx => { if (ctx.hasUI) ctx.ui.notify(warning, "warning"); });
      },
      live: live.current,
      // No live ctx to rebind to (smarty-dev#5962/#4313): the lease lapses, so say it on the fleet
      // ops topic instead of vanishing from the directory in silence. The UI ctx is retired too.
      // Non-Main runtimes (actors, agents) heartbeat their own host lease too, so they report
      // their own participant and session, never the Main they belong to (pi-fabric#660).
      onLifecycleLost: ticks => {
        void this.publishOpsEvent("ops.fabric.presence", "fabric.presence.degraded", {
          participantId: identity.kind === "main" ? mainAgentId : identity.id, sessionId, hostId, ticks,
          reason: "no live session ctx (session replaced or reloaded)",
        }).catch(() => undefined);
      },
      ...(process.env.PI_FABRIC_OWNER_HOST_ID
        ? { selfOwnerHostId: process.env.PI_FABRIC_OWNER_HOST_ID }
        : {}),
      ...(process.env.PI_FABRIC_OWNER_IDENTITY_ID
        ? { selfOwnerIdentityId: process.env.PI_FABRIC_OWNER_IDENTITY_ID }
        : {}),
    });
    // Resumption must invalidate an earlier terminal proof before actors/control
    // can activate, not merely as part of the later participant publication batch.
    await this.#participants.resumeLineage();
    // Install this exact activation under the custody lock BEFORE either succession
    // path publishes it. A competing drainer must never see B -> resumed C while
    // C still carries a historical retired owner/successor (for example C -> D).
    // Registration resets root activation only; per-carrier replay fences survive.
    const inboxActivation = this.#config.mesh.enabled && mainAgent.local ? await this.#mesh.custody(() =>
      registerMainInbox(meshRoot, identity, sessionId, context.sessionManager.getSessionFile?.())) : undefined;
    if (this.#config.mesh.enabled && mainAgent.local && predecessor && predecessor.id !== mainAgentId &&
      predecessor.meshRoot === meshRoot && predecessor.cwd === context.cwd) {
      await recordMainSuccessor(this.#mesh, predecessor.id, predecessor.sessionId, mainAgentId);
    }
    const recordedRotation = this.#config.mesh.enabled && mainAgent.local
      ? await confirmMainSuccessor(this.#mesh, mainAgentId, context.sessionManager.getSessionFile?.()) : false;
    // No Main admission/drain starts while a prior-generation death proof survives.
    mainAgent.attachFollowUpDrain(
      context,
      followUpDrainSupported() ? this.#config.mesh.followUpFlushMs : 0,
      path.join(meshRoot, "main-followups", `${encodeURIComponent(sessionId)}.json`),
      this.#config.mesh.followUpStallSeconds,
      this.#config.mesh.enabled && mainAgent.local ? {
        owns: id => mainInboxOwns(meshRoot, mainAgentId, id),
        active: () => mainInboxActive(meshRoot, mainAgentId, inboxActivation),
      } : undefined,
    );
    if (this.#config.mesh.enabled && mainAgent.local) {
      inboxMaintenance = new MainInboxMaintenance(this.#mesh, identity, this.#participants, mainAgent, this.#config.mesh);
      if (recordedRotation || (predecessor && predecessor.id !== mainAgentId)) await inboxMaintenance.run();
    }
    this.#rootInbox?.start();
    this.#control = new FabricControlPlane(this.#mesh, identity, {
      enabled: this.#config.mesh.enabled,
      hostId,
      pollMs: this.#config.mesh.actorPollMs,
      bridgeTimeoutMs: this.#config.mesh.bridgeControlTimeoutMs,
      captureOwnerLease: (ownerHostId, ownerIdentityId, targetId) =>
        this.#participants?.captureControlOwnerLease(ownerHostId, ownerIdentityId, targetId),
      readMirroredOwner: (ownerHostId, ownerIdentityId, targetId) =>
        this.#participants?.mirroredControlOwner(ownerHostId, ownerIdentityId, targetId),
    });
    await builtins.mesh(this.#config, this.#mesh, identity, this.#participants);
    this.#schema = new SchemaController(
      context.cwd,
      this.#config.schema,
      this.#mesh,
      identity,
      new StateStore(this.#mesh),
    );
    await builtins.install(createProviderComponent({
      provider: "schema",
      description: "Schema verification and workspace transactions",
      create: () => new SchemaProvider(this.#schema!),
    }));
    this.#identity = identity;
    this.#compact = new CompactController({
      onRequest: (intent) => void this.#publishCompactEvent("requested", intent),
      onCommit: (info) => void this.#publishCompactEvent(info.status, info),
    });
    await builtins.install(createProviderComponent({
      provider: "compact",
      description: "Host context compaction controller",
      create: () => new CompactProvider(this.#compact!),
    }));
    const agentConfig = enforceSchema
      ? { ...this.#config.agents, enabled: false }
      : this.#config.agents;
    const modelsConfig = this.#config.models;
    const visiblePiModels = () => {
      try {
        return context.modelRegistry.getAvailable();
      } catch {
        return [];
      }
    };
    const piModelState = (models = visiblePiModels()) => {
      const available: FabricModelCandidate[] = models.map((model) => ({
        provider: String(model.provider),
        id: String(model.id),
        ...(typeof model.name === "string" ? { name: model.name } : {}),
      }));
      const defaultModel = context.model
        ? `${context.model.provider}/${context.model.id}`
        : undefined;
      return {
        available,
        aliases: structuredClone(modelsConfig.aliases),
        ...(defaultModel ? { defaultModel } : {}),
      };
    };
    // Task agents and actors share one single-flight refresh per registry (smarty-dev#1830).
    const resolveParticipantPiModel = async (selector?: string, options: { requiredPin?: boolean; closest?: boolean } = {}) => {
      const defaultModel = context.model ? `${context.model.provider}/${context.model.id}` : undefined;
      const resolved = options.requiredPin
        ? await resolvePiRoutePin({ selector: selector!, registry: context.modelRegistry, aliases: {} })
        : await resolvePiModel({
            selector,
            registry: context.modelRegistry,
            aliases: modelsConfig.aliases,
            defaultModel,
            policy: agentConfig,
            closest: options.closest ?? true,
          });
      const model = visiblePiModels().find(
        (candidate) =>
          String(candidate.provider).toLowerCase() === resolved.provider.toLowerCase() &&
          String(candidate.id).toLowerCase() === resolved.id.toLowerCase(),
      );
      if (!model) {
        throw new Error(
          `Model ${JSON.stringify(selector?.trim() || defaultModel || "")} is not available to this Pi session. ` +
            'Use agents.models({ runner: "pi" }) to list the models visible to this session.',
        );
      }
      return { key: `${resolved.provider}/${resolved.id}`, model };
    };
    const actorSpawner = identity.kind === "actor" ? resolveAgentSpawner(identity.id, mainAgentId) : undefined;
    const actorSessionFile = process.env.PI_FABRIC_ACTOR_SESSION_FILE?.trim() || context.sessionManager.getSessionFile?.();
    const actorChildStore = actorSpawner && actorSessionFile ? new ActorChildCompletionStore(actorSessionFile) : undefined;
    const completionInbox = new AgentCompletionInbox(this.pi, context,
      actorChildStore ? (ids) => actorChildStore.consumeLiveBatch(ids) : undefined);
    this.#completionInbox = completionInbox;
    let markStoppedDelivered = (_id: string): void => {};
    recordMainRelease(sessionId, loadedFabricRoot(import.meta.url));
    this.#agents = new AgentManager(context.cwd, agentConfig, {
      ...(!this.#managedHost ? { placementConfigPath: path.join(resolveAgentDir(), "fabric.json") } : {}),
      fullCodeMode: this.#config.fullCodeMode,
      kernel: () => this.#config?.executor.kernel ?? "typescript",
      pythonRuntime: () => this.#config?.executor.pythonRuntime ?? "monty",
      mainAgentId,
      fabricSessionId,
      meshRoot,
      projectRoot,
      hostId,
      identityId: identity.id,
      ...(ownsPersistentActorRegistry ? { completionRecipient: () => ({
        rootId: mainAgentId, sessionId, cwd: context.cwd, projectRoot, name: rootParticipantName(this.pi.getSessionName?.()), role,
        startedAt: mainAgent.info(context).startedAt ?? Date.now(),
      }) } : {}),
      spawnerSessionId: sessionId,
      retention: this.#config.retention,
      ...(this.#paths
        ? {
            workerPath: this.#paths.worker,
            fabricExtensionPath: this.#paths.extension,
          }
        : {}),
      resolveInheritedSessionPins: () =>
        resolveInheritedSessionPins(context.sessionManager?.getEntries?.() ?? []),
      resolveParticipantGuidance: ({ model, runner }) => {
        const targetModel = model ?? (runner === "pi" && context.model
          ? `${context.model.provider}/${context.model.id}`
          : undefined);
        if (!targetModel) return undefined;
        return resolveFabricModelGuidance(this.modelGuidance(), {
          model: targetModel,
          target: "participant",
          includeSlots: false,
        }).appendText || undefined;
      },
      resolveHandoffCompactionBudget: async (modelKey, cwd) => {
        const { model } = await resolveParticipantPiModel(modelKey);
        // Load host settings only for an actual compacted handoff. Project
        // trust does not transfer implicitly to a different working directory.
        const { SettingsManager } = await import("@earendil-works/pi-coding-agent");
        const settings = SettingsManager.create(cwd, resolveAgentDir(), {
          projectTrusted: !this.#managedHost && cwd === context.cwd && context.isProjectTrusted(),
        }).getCompactionSettings(model);
        return {
          contextWindow: model.contextWindow,
          targetContextRatio: this.#config?.compaction.targetContextRatio ?? DEFAULT_FABRIC_CONFIG.compaction.targetContextRatio,
          reserveTokens: settings.reserveTokens,
          keepRecentTokens: settings.keepRecentTokens,
        };
      },
      preparePiModel: async (modelKey, requiredPin) => {
        const resolved = await resolveParticipantPiModel(modelKey, { requiredPin: requiredPin ?? false });
        const auth = await context.modelRegistry.getApiKeyAndHeaders(resolved.model);
        if (!auth.ok) throw new Error(auth.error);
        return resolved.key;
      },
      onFollowUpAlarm: (alarm) => {
        this.pi.sendMessage({ customType: "pi-fabric-follow-up-alarm", content: alarm.message, display: true, details: alarm },
          { deliverAs: "steer", triggerTurn: false });
        this.pi.events.emit("fabric.followUp.deadline", alarm);
      },
      onLifecycle: (event) => {
        const lifecycle = this.#lifecycle;
        if (lifecycle) void lifecycle.publishBackground(event);
      },
      // Retain terminal results until consumption, for both Main residency and actor children.
      onSettled: (result, admittedRecipient) => {
        this.#residency?.enqueueCompletion(result, admittedRecipient);
        if (actorChildStore && actorSpawner) actorChildStore.enqueue(result, actorSpawner, agentConfig.notifyOnComplete);
      },
      onBackgroundComplete: (result, admittedRecipient) => {
        if (this.#residency) this.#residency.enqueueCompletion(result, admittedRecipient);
        else completionInbox.enqueue(result,
          actorChildStore ? () => actorChildStore.acknowledge(result.id) : undefined,
          actorChildStore ? () => actorChildStore.prepareLive(result.id) : undefined);
      },
      onBeforeResultReturned: (id) => {
        // ponytail: commit BEFORE returning to the actor program, not in the
        // deferred post-delivery callback. Retry a transient receipt failure once;
        // persistent failure rejects the observation, making returned-but-unrecorded impossible.
        if (actorChildStore) {
          try { actorChildStore.consume(id, { handoff: true, publication: true }); } catch {
            actorChildStore.consume(id, { handoff: true, publication: true });
          }
        }
      },
      onResultAbandoned: (id) => actorChildStore?.abandonForeground(id),
      onResultConsumed: (id) => {
        completionInbox.acknowledge(id);
        // The manager certifies logical settlement: fence even a temporarily failed journal save.
        this.#residency?.acknowledgeCompletion(id, true);
        try { actorChildStore?.discard(id); } catch { /* Cleanup must not turn a returned outcome into a wait failure. */ }
        markStoppedDelivered(id);
      },
      onStoppedAtClose: (results) => {
        if (actorChildStore && actorSpawner) {
          const failures: unknown[] = [];
          for (const result of results) {
            for (let attempt = 0; attempt < 3; attempt++) {
              try { actorChildStore.enqueue(result, actorSpawner, agentConfig.notifyOnComplete); break; }
              catch (error) { if (attempt === 2) failures.push(error); }
            }
          }
          if (failures.length) throw new AggregateError(failures, "Actor child shutdown archives remain uncommitted");
          return;
        }
        rememberStoppedAtClose(sessionId, results);
        this.pi.appendEntry<StoppedAgentsEntryData>(STOPPED_AGENTS_ENTRY, { stopped: results });
      },
    });
    // Runs a previous runtime of this session stopped at reload/shutdown (smarty-dev#1602):
    // wait/status answer from the record, and with notices on each result reaches the spawner once.
    const agents = this.#agents;
    markStoppedDelivered = restoreStoppedRuns({
      entries: context.sessionManager?.getEntries?.() ?? [],
      notifyOnComplete: agentConfig.notifyOnComplete,
      restore: (runs) => agents.restorePreviousRuns(runs),
      enqueue: (run, delivered) => completionInbox.enqueue(run, delivered),
      appendEntry: (data) => this.pi.appendEntry<StoppedAgentsEntryData>(STOPPED_AGENTS_ENTRY, data),
    });
    const canManageActor = (actorId: string, fresh = true): boolean | undefined => {
      const participant = this.#participants?.get(actorId, undefined, { fresh });
      return participant ? participant.ownerHostId === hostId : undefined;
    };
    const snapshotActorOwnership = (fresh = true): ReadonlyMap<string, boolean> => new Map(
      (this.#participants?.list({ scope: "project", fresh }) ?? [])
        .map((participant) => [participant.id, participant.ownerHostId === hostId]),
    );
    // Capture this generation's directory: replacement/quiesce must veto old
    // deferred slices, and new actors cannot maintain archives before publication.
    const actorParticipants = this.#participants;
    const canConsumeActorMesh = () => actorParticipants.canConsumeMesh();
    const lineageAlive = (rootId: string): boolean =>
      this.#participants?.lineageAlive(rootId) ?? true;
    const actorRoots = {
      project: path.join(meshRoot, "actors"),
      session: path.join(meshRoot, "actors", fabricSessionId),
    };
    const acquireActorCapabilityView = (
      requirements: Parameters<ActionRegistry["acquireCapabilityView"]>[0],
      signal: AbortSignal,
    ) => this.#registry!.acquireCapabilityView(requirements, {
      cwd: context.cwd,
      signal,
      parentToolCallId: "fabric-actor-capability",
      nestedToolCallId: "fabric-actor-capability",
      extensionContext: context,
      update() {},
    });
    const prepareActorModelRoute = async (input: ActorModelRouteInput, signal: AbortSignal) => {
      const { prepareModelRoute } = await import("./agents/model-route-prepare.js");
      return prepareModelRoute({ ...input, signal, config: this.#config!.agents.modelRouting,
        registry: context.modelRegistry, aliases: this.#config!.models.aliases,
        assertModelAllowed: model => this.#agents!.assertModelAllowed(model, "pi"),
        evaluate: (request, routeSignal) => {
          if (!this.#agentsProvider) throw new Error("Jev routing unavailable");
          return this.#agentsProvider.routeEvaluate(request, routeSignal, { cwd: context.cwd,
            signal: routeSignal, parentToolCallId: "fabric-actor-route", nestedToolCallId: "fabric-actor-route",
            extensionContext: context, update() {} });
        } });
    };
    this.#actors = new ActorDirectory([
      fabricSessionId,
      identity,
      this.#mesh,
      enforceSchema ? { ...this.#config.mesh, enabled: false } : this.#config.mesh,
      this.#agents,
      request => deliverActorToMain(this.pi, identity, request),
      ownsPersistentActorRegistry
        ? {
            persistent: true,
            mainAgent,
            canManageActor,
            snapshotActorOwnership,
            canConsumeMesh: canConsumeActorMesh,
            isOwnResidentActor: (id) => isOwnResidentActor(this.#participants!, id, mainAgentId),
            lineageAlive,
            claimResidency: "session",
            rootId: mainAgentId,
            project: participantProject(context.cwd),
            role,
            retention: this.#config.retention,
            maxSessionBytes: this.#config.actors.maxSessionBytes,
            // Read live at each drain: reloadConfig deep-assigns this.#config, so removal revokes (smarty-dev#6144).
            wakeText: () => this.#config?.agents.wakeText,
            resolvePiModel: async (model, requiredPin) => (await resolveParticipantPiModel(model, { requiredPin: requiredPin ?? false, closest: false })).key,
            prepareModelRoute: prepareActorModelRoute,
            acquireCapabilityView: acquireActorCapabilityView,
            // A /reload or restart of this session resumes its actors' mesh stream where the
            // last runtime stopped, so events published in between still reach them
            // (smarty-dev#472). A longer downtime replays only its last minutes.
            meshCursorPath: path.join(actorRoots.session, "mesh-cursor.json"),
            meshReplayAgeMs: MAIN_ACTOR_MESH_REPLAY_MS,
          }
        : {
            persistent: false,
            mainAgent,
            canManageActor,
            snapshotActorOwnership,
            canConsumeMesh: canConsumeActorMesh,
            lineageAlive,
            claimResidency: "session",
            rootId: mainAgentId,
            project: participantProject(context.cwd),
            role,
            retention: this.#config.retention,
            maxSessionBytes: this.#config.actors.maxSessionBytes,
            // Read live at each drain: reloadConfig deep-assigns this.#config, so removal revokes (smarty-dev#6144).
            wakeText: () => this.#config?.agents.wakeText,
            resolvePiModel: async (model, requiredPin) => (await resolveParticipantPiModel(model, { requiredPin: requiredPin ?? false, closest: false })).key,
            prepareModelRoute: prepareActorModelRoute,
            acquireCapabilityView: acquireActorCapabilityView,
          },
    ], actorRoots, this.#config.mesh.actorScope);
    // A removal this Main accepted behind a run that its restart ended (a same-name create) is
    // finished here, as a resident host does at start; ownership limits it to this Main's actors.
    if (ownsPersistentActorRegistry) void this.#actors.finishPendingRemovals().catch(() => undefined);
    this.#registry.subscribeProviderChanges(() => {
      this.#actors?.retryCapabilityWaiters();
      this.#refreshRepairCatalog();
    });
    this.#lifecycle = new LifecycleBroker(
      this.#mesh,
      identity,
      this.#participants,
      {
        enabled: this.#config.mesh.enabled && !enforceSchema,
        pollMs: this.#config.mesh.actorPollMs,
        maxReadEvents: this.#config.mesh.maxReadEvents,
      },
      async (subscription, event) => {
        if (!this.#agentsProvider) throw new Error("Fabric agents provider is unavailable");
        await this.#agentsProvider.deliverLifecycle(subscription, event);
      },
    );
    this.#globalActors = new GlobalActorRegistry(resolveAgentDir(), this.#config.mesh.maxEventBytes);
    this.#residency = ownsPersistentActorRegistry
      ? new ResidencyClient({
          config: {
            format: RESIDENT_HOST_FORMAT,
            rootId: mainAgentId,
            sessionId,
            cwd: context.cwd,
            projectRoot,
            mainName: rootParticipantName(this.pi.getSessionName?.()),
            mainStartedAt: mainAgent.info(context).startedAt ?? Date.now(),
            ...(role ? { role } : {}),
            project: participantProject(context.cwd),
            meshRoot,
            actorRoot: actorRoots.project,
            sessionActorRoot: actorRoots.session,
            residencyRoot: residentRoot(meshRoot, mainAgentId),
            fullCodeMode: this.#config.fullCodeMode,
            kernel: this.#config.executor.kernel,
            pythonRuntime: this.#config.executor.pythonRuntime,
            agents: structuredClone(this.#config.agents),
            mesh: structuredClone(this.#config.mesh),
            retention: structuredClone(this.#config.retention),
            actors: structuredClone(this.#config.actors),
            shadowRouting: { jev: structuredClone(this.#config.jev),
              networkAllowed: this.#config.approvals.network === "allow", schemaEnforced: enforceSchema },
            workerPath: this.#paths?.worker ?? fileURLToPath(new URL("./worker.js", import.meta.url)),
            fabricExtensionPath: this.#paths?.extension ?? fileURLToPath(new URL("./index.js", import.meta.url)),
            piBinary: resolvePiBinary(),
            claudeBinary:
              process.env.PI_FABRIC_CLAUDE_BINARY ?? this.#config.agents.claude.binary,
            vedaBinary:
              process.env.PI_FABRIC_VEDA_BINARY ?? this.#config.agents.veda.binary,
            piModels: piModelState(),
            modelGuidance: [],
          },
          mesh: this.#mesh,
          participants: this.#participants,
          mainAgent,
          onBackgroundComplete: (result, delivered) => completionInbox.enqueue(result, delivered),
          onResultConsumed: (id) => completionInbox.acknowledge(id),
          piModelState,
          mainName: () => rootParticipantName(live.sessionName()),
          ...(this.#paths ? { hostPath: this.#paths.residentHost } : {}),
        })
      : undefined;
    const firstSeenAgents = new Map<string, number>();
    if (mainAgent.local) {
      // The presence heartbeat rereads the Pi name (renames, clearing) and model through the
      // live binding only. A retired ctx keeps the last live snapshot (smarty-dev#5962).
      let rootInfo = mainAgent.info(context);
      live.sessionName();
      const participants = this.#participants;
      participants.registerSource(() => {
        const current = live.read(ctx => mainAgent.info(ctx));
        rootInfo = current ?? { ...rootInfo, updatedAt: Date.now() };
        return [participants.root(rootInfo, mainAgent.interactive, live.sessionName(), { role })];
      });
    }
    this.#participants.registerSource(() =>
      agentParticipantRecords(
        this.#agents!.listForUi(),
        mainAgentId,
        hostId,
        identity.id,
        identity.id,
        firstSeenAgents,
      ),
    );
    this.#participants.registerSource(() =>
      this.#actors!.listOwned().map((actor) =>
        actorParticipantRecord(actor, mainAgentId, hostId, identity.id, identity.id),
      ),
    );
    this.#agents.subscribeUi(() => this.#participants?.scheduleRefresh());
    this.#actors.subscribe(() => this.#participants?.scheduleRefresh());
    let routeOwner: { client: import("./jev/client.js").JevClient; signal: AbortSignal; pending: Set<Promise<unknown>> } | undefined;
    const agentsProvider = new AgentsProvider(
      this.#agents,
      this.#actors,
      this.#globalActors,
      mainAgent,
      this.#participants,
      this.#control,
      this.#lifecycle,
      () => this.#config?.ui.showAgentToolPreview ?? true,
      this.#residency,
      false,
      () => this.#config?.models ?? DEFAULT_FABRIC_CONFIG.models,
      () => this.pi.getThinkingLevel(),
      (request, signal, invocation) => {
        const owner = routeOwner;
        if (!owner || owner.signal.aborted) throw new Error("Jev routing unavailable");
        const routeSignal = AbortSignal.any([signal, owner.signal]);
        const pending = (async () => {
          // agents.spawn approval grants agent work, not Jev network access. Use
          // the current ordinary jev.evaluate policy before touching credentials.
          // Ungranted `auto`/`ask` is refused before the approval queue (SR-8/9):
          // neither classifier work nor a host dialog can be owned by routeSignal.
          // Record pinned fallback instead; no approval cleanup debt is created.
          await runAbortable(routeSignal, async () => {
            const action = await this.#registry!.describe("jev.evaluate", { ...invocation, signal: routeSignal });
            routeSignal.throwIfAborted();
            await this.#schema!.authorize(action.ref, invocation.parentToolCallId);
            routeSignal.throwIfAborted();
            const approval = new ApprovalController(
              this.#config!.approvals, invocation.extensionContext, this.sessionApprovals,
              this.execution.autoApprovalClassifier, undefined, this.execution.brokeredNetwork, true,
            );
            await approval.approve(action, request as unknown as Record<string, unknown>);
          });
          routeSignal.throwIfAborted();
          return owner.client.evaluate(request, routeSignal);
        })().catch(error => {
          if (owner.signal.aborted && !signal.aborted) throw new Error("Jev routing owner retired");
          throw error;
        });
        owner.pending.add(pending);
        void pending.then(() => owner.pending.delete(pending), () => owner.pending.delete(pending));
        return pending;
      },
    );
    this.#agentsProvider = agentsProvider;
    this.#control.start((command, from, signal, verification) =>
      agentsProvider.acceptControl(command, from, signal, verification));
    try {
      await this.#participants.start();
    } catch (error) {
      const detail = error instanceof Error ? error.message : String(error);
      console.warn(
        `[pi-fabric] Initial mesh publish failed (${detail}); the participant heartbeat will keep retrying.`,
      );
      if (context.hasUI) {
        context.ui.notify(
          `Pi Fabric could not reach the mesh (${detail}); retrying in the background.`,
          "warning",
        );
      }
    }
    this.#lifecycle.start();
    this.#residency?.start();
    await builtins.install(createProviderComponent({
      provider: "agents",
      description: "Agents, actors, lifecycle delivery, and residency control",
      create: () => agentsProvider,
    }));
    if (this.#config.jev.enabled && !enforceSchema) {
      const { JevProvider } = await import("./providers/jev-provider.js");
      const { JevObservationHost } = await import("./jev/observation.js");
      await builtins.install(createProviderComponent({
        provider: "jev",
        description: "Shell orchestration and explicit typed Jev decisions",
        create: (component) => {
          component.guide({
            label: "jev-programs", models: ["*/*"], targets: ["main", "participant"],
            content: "Jev supplies typed Choice, Noul, and Score judgments, not generated text. Prefer shell-first orchestration: granted pi.bash runs existing CLIs; tasks.wait/watch await bounded receipts/monitor batches without polling or inference. Use UI-only monitors to avoid Main wakeups. Browser/macOS tools need no Fabric bridge. Code owns commands; never execute a model answer as shell source. Omit jev.evaluate and set maxEvaluations:0 for deterministic programs (host auto approvals remain independent). Use jev.evaluate only for explicit authorized batched questions; jev.run/spawn for isolated TypeScript programs that may loop using input, program.sleep, program.emit, and exact requires capabilities. run/wait return terminal envelopes (join aliases wait for both agents and Jev); inspect state and result/error. Programs and detached tasks are session-owned, not restart-durable. jev.status/stop control programs; tasks.stop separately stops their detached tasks. Observation timeout/cancellation never cancels the task; keep task IDs and finite process deadlines. For Main-turn advisors, spawn with observe, await program.nextEvent without polling, and opt into bounded context fields. program.advise requires jev.advise and explicit delivery; default is record-only. Return the observer ID without waiting in Main; Escape/Main abort cancels observers. Use /login jev, TYPESAFE_API_KEY, /login openrouter, OPENROUTER_API_KEY, /login vercel-ai-gateway, AI_GATEWAY_API_KEY, or a trusted credentialCommand. Credentials stay host-side; status never retrieves a key. See docs/jev.md for schemas, budgets, and shell/CLI composition.",
          });
          const observationHost = identity.kind === "main" ? new JevObservationHost(context.sessionManager.getSessionId(), advice => {
            sendFabricMessage(this.pi, {
              customType: "pi-fabric-jev",
              content: [`<fabric-jev name=${JSON.stringify(escapeXmlText(advice.name))} id=${JSON.stringify(advice.runId)}>\n${escapeXmlText(advice.message)}\n</fabric-jev>`, actorDeliveryNotice(advice.delivery, advice.triggerTurn)].filter(Boolean).join("\n"),
              display: true,
              details: { runId: advice.runId, eventId: advice.eventId, delivery: { mode: advice.delivery, triggerTurn: advice.triggerTurn } },
            }, { deliverAs: advice.delivery, triggerTurn: advice.triggerTurn }, identity, "actor", "mesh");
          }) : undefined;
          this.#jevObservationHost = observationHost;
          // A bare `jev.model` alias stays on TypeSafe; `typesafe/...` / `~typesafe/...` uses OpenRouter decisions, and `typesafe-ai/...` uses Vercel AI Gateway.
          const jevRoute = resolveJevModelRoute(this.#config!.jev.model).route;
          const provider = new JevProvider({
            registry: this.#registry!, config: this.#config!, observationHost,
            credentialSource: {
              configured: () => context.modelRegistry.getProviderAuthStatus?.(jevRoute.providerId)?.configured ?? false,
              resolve: async (signal) => {
                signal.throwIfAborted();
                return context.modelRegistry.getApiKeyForProvider?.(jevRoute.providerId);
              },
            },
            authorize: (ref, parentToolCallId) => this.#schema!.authorize(ref, parentToolCallId),
          });
          // A program may pin jev.evaluate itself. Cancel at owner retirement,
          // not only at provider.close(), which waits for those pins to drain.
          const owner = { client: provider.client, signal: component.signal, pending: new Set<Promise<unknown>>() };
          routeOwner = owner;
          this.#jevPrograms = provider.manager;
          const stop = () => {
            if (routeOwner === owner) routeOwner = undefined;
            observationHost?.close(); provider.manager.stopAll();
          };
          component.signal.addEventListener("abort", stop, { once: true });
          component.defer(async () => {
            component.signal.removeEventListener("abort", stop);
            observationHost?.close();
            if (this.#jevObservationHost === observationHost) this.#jevObservationHost = undefined;
            if (this.#jevPrograms === provider.manager) this.#jevPrograms = undefined;
            await Promise.allSettled([...owner.pending]);
            await owner.client.drainCredentials();
            await provider.manager.close();
          }, { label: "jev-program-owner", kind: "transactional", resources: ["jev:programs"], ordering: "ordered" });
          return provider;
        },
      }));
    } else {
      this.#registry.markUnavailable("jev", enforceSchema ? "Jev network programs are unavailable in Schema enforce mode" : "disabled by configuration (jev.enabled=false)");
    }
    if (!this.#managedHost && this.#config.records.enabled && this.#config.mesh.enabled && !enforceSchema) {
      // The org's record (smarty-dev#754): the driver loads and the database connects at first use.
      const recordsConfig = this.#config.records;
      const mesh = this.#mesh;
      const recordsIdentity = identity;
      const root = identity.kind === "main" && mainAgent.local;
      const recordsNames = () => [mainAgentId, this.pi.getSessionName?.() ?? ""];
      this.#openRecords = () => {
        this.#records ??= import("./records/service.js").then(({ RecordsService }) => RecordsService.open({
          config: recordsConfig,
          identity: { id: recordsIdentity.id, name: recordsIdentity.name },
          credentialDir: path.join(resolveAgentDir(), "fabric", "records-credentials"),
          publisher: { publish: (input) => mesh.publish({ ...input, from: recordsIdentity }) },
          ...(root ? {
            names: recordsNames,
            // The watchdog never starts a turn itself (F21): it asks the host's session-owned idle
            // gate (#107), which delivers only to an idle, armed Main with no prompt preflight.
            // Without that gate, records wait for the next turn (before_agent_start delivers them).
            wake: async () => { await this.recordsWake?.(); },
          } : {}),
        })).catch((error: unknown) => {
          // A failed open (the database is down) is retried at the next use, not cached.
          this.#records = undefined;
          throw error;
        });
        return this.#records;
      };
      const openRecords = this.#openRecords;
      await builtins.install(createProviderComponent({
        provider: "records",
        description: "The org's durable record on its Node (PostgreSQL)",
        create: () => new RecordsProvider(openRecords),
      }));
    } else {
      this.#registry.markUnavailable("records", this.#config.records.enabled
        ? "records need the mesh and a local host (not Schema enforce mode or a managed host)"
        : RECORDS_DISABLED_HINT);
    }
    await builtins.memory(context, this.#config, sessionId);
    builtins.assertActive(this.#config);
    await this.#mountExecution(context, enforceSchema);
    // Reload restores accepted activations before provider/directory startup finishes. Re-admit
    // both actor scopes now, retaining a wake if an early drain is still finalizing (#3167).
    this.#actors.resumeQueued();
    const inheritedRequirements = inheritedCapabilityRequirements();
    const inheritedDigest = process.env.PI_FABRIC_CAPABILITY_DIGEST;
    const hasInheritedCommit =
      process.env.PI_FABRIC_CAPABILITY_REQUIREMENTS !== undefined && Boolean(inheritedDigest);
    if (inheritedRequirements.length > 0 || hasInheritedCommit) {
      const lease = await this.#registry.acquireCapabilityView(inheritedRequirements, {
        cwd: context.cwd,
        signal: undefined,
        parentToolCallId: "fabric-capability-commit",
        nestedToolCallId: "fabric-capability-commit",
        extensionContext: context,
        update() {},
      });
      if (!lease.satisfied || !lease.view) {
        await lease.release();
        throw new Error(
          `Required Fabric capabilities are unavailable: ${lease.missing.join(", ")}`,
        );
      }
      const expectedDigest = inheritedDigest;
      if (expectedDigest && lease.view.semanticDigest !== expectedDigest) {
        await lease.release();
        throw new Error(
          `Fabric capability commitment mismatch: expected ${expectedDigest}, resolved ${lease.view.semanticDigest}`,
        );
      }
      this.#sessionCapabilityLease = lease;
      this.execution.setCapabilityView(lease.view);
    }
    this.#repairs = new RepairCompiler({
      agentDir: resolveAgentDir(),
      enabled: this.#config.repairs.enabled,
    });
    // Commit the stable catalog surface before promotion can reach the
    // active compiler: an active compiler whose surface has not been
    // committed yet must never persist under a mid-reconcile catalog.
    this.#refreshRepairCatalog();
    setActiveRepairCompiler(this.#repairs);
    // Static compatibility is enabled on first use, even without a corpus.
    // Loaded plans re-prove their rule language and live schema at consult.
    // A damaged artifact disables normalization and surfaces on demand.
    const compiled = this.#config.entropy.compile ? loadCompiledSurface(resolveAgentDir()) : {};
    setActiveCompiledSurface(compiled.file, this.#config.entropy.compile && !compiled.error);
  }

  async #mountExecution(context: ExtensionContext, enforceSchema: boolean): Promise<void> {
    for (const provider of this.#externalProviders.values()) {
      this.registry.register(provider);
    }
    this.#execution = new FabricExecutionService(
      this.registry,
      this.config,
      this.activity,
      this.#schema,
      undefined,
      this.sessionApprovals,
      this.capturedTools,
      this.#managedHost ? (name) => this.#managedHost!.ownsProvider(name) : undefined,
    );
    const discovery: FabricProviderDiscovery = {
      version: 1,
      register: (provider, options) => this.registerExternal(provider, options),
    };
    this.pi.events.emit(FABRIC_PROVIDER_DISCOVER_EVENT, discovery);
    const componentDiscovery: FabricComponentDiscovery = {
      version: 1,
      register: (component, options) => this.registerExternalComponent(component, options),
    };
    this.pi.events.emit(FABRIC_COMPONENT_DISCOVER_EVENT, componentDiscovery);
    await this.components.reconcile(enforceSchema ? [] : this.config.components);
    const configuration = this.#componentConfiguration;
    const control = this.#componentControl;
    if (configuration && control) {
      this.#stopComponentWatch = watchComponentConfiguration(configuration.paths, () => {
        void control.reconcile().catch(error => {
          notifyDetached(context, `Pi Fabric component configuration not applied: ${error instanceof Error ? error.message : String(error)}`);
        });
      });
      for (const warning of control.configuration().warnings) {
        if (context.hasUI) context.ui.notify(warning, "warning");
      }
    }
  }

  async ensure(context: ExtensionContext): Promise<void> {
    if (!this.initialized || this.#cwd !== context.cwd) await this.initialize(context);
  }

  // Accepts the config FabricState just loaded so a /fabric settings save
  // costs one loadFabricConfig instead of two. The runtime still stamps
  // schema.mode from its own previous config, preserving the existing
  // in-memory override chain (state and runtime share the same preserved
  // mode by construction: the runtime's config originates from FabricState).
  // Ephemeral schema mode override: mutates the live config in place so the
  // per-call readers (SchemaController.authorize, the top-level tool gate,
  // ExecutionService's runtime selection) see the change immediately without
  // a provider-topology rebuild. FabricState owns the coupling rules.
  setSchemaMode(mode: FabricSchemaMode, executorRuntime: FabricConfig["executor"]["runtime"]): void {
    if (this.#managedHost) throw new Error("Managed host policy is immutable");
    if (!this.#config) return;
    this.#config.schema.mode = mode;
    this.#config.executor.runtime = executorRuntime;
    this.#configureSpeculation();
  }

  reloadConfig(context: ExtensionContext, next: FabricConfig): void {
    if (this.#managedHost) next = this.#managedHost.config();
    if (!this.#config || !this.#cwd) return;
    next.schema.mode = this.#config.schema.mode;
    this.#speculation?.reset();
    const previousComponents = structuredClone(this.#config.components);
    deepAssign(this.#config as unknown as Record<string, unknown>, next as unknown as Record<string, unknown>);
    // Host-only agents.wakeText: this Main's actors read this.#config at each drain; publish it to the
    // resident host's config.json, which its actors read at each drain too, so removal revokes there at once.
    this.#residency?.updateWakeText(this.#config.agents.wakeText);
    this.#configureSpeculation();
    // The persisted master switch wins over any live arm: disabling prewalk
    // via /fabric settings (or an external config edit followed by a reload)
    // cancels the arm so no later boundary can claim behind the user's back.
    if (next.prewalk.enabled === false && this.prewalk.status().state !== "idle") {
      this.prewalk.cancel();
      this.prewalkDrift.drop(context.sessionManager.getSessionId());
      if (context.hasUI) context.ui.setStatus("fabric-prewalk", undefined);
    }
    void (this.#componentControl?.reconcile(next.components) ?? this.#componentLoader?.reconcile(next.components))?.catch((error) => {
      if (this.#config) this.#config.components = previousComponents;
      const detail = error instanceof Error ? error.message : String(error);
      notifyDetached(context, `Pi Fabric component reload failed: ${detail}`);
    });
  }

  #configureSpeculation(): void {
    this.#speculation?.reset();
    this.#speculation = undefined;
    this.#registry?.setSpeculation(undefined);
    const config = this.#config;
    // Native runtimes can mutate outside registry epochs; only isolated backends speculate.
    // Recreate the tap/store for every policy change so limits, epochs and
    // pending asynchronous scans cannot leak across an execution boundary.
    const eligible = (): boolean => {
      const current = this.#config;
      if (!current?.speculation.enabled) return false;
      return current.executor.kernel === "python"
        ? current.executor.pythonRuntime === "monty" || current.schema.mode === "enforce"
        : current.schema.mode === "enforce" || current.executor.runtime === "quickjs";
    };
    if (!config || !this.#registry || !eligible()) return;
    this.#speculation = new RuntimeStateSpeculation(
      this.#registry,
      () => eligible() ? this.#config?.speculation : undefined,
      () => this.#sessionCapabilityLease?.view,
      (ref) => {
        const current = this.#config!;
        if (current.approvals[ref.startsWith("mcp.") ? "network" : "read"] !== "allow") return false;
        if (ref.startsWith("pi.") && !current.fullCodeMode && current.schema.mode !== "enforce") return false;
        return current.schema.mode !== "enforce" || schemaRefAllowedInEnforce(ref);
      },
      config.executor.kernel,
    );
  }

  async claimHandoff(
    execution: FabricExecutionResult,
    sessionId: string,
    resultFormat: FabricResultFormat,
    outerToolCallId: string,
  ): Promise<PendingFabricHandoff | undefined> {
    // Defense in depth behind cancel-at-disable: an arm that predates a
    // config edit must never claim once the master switch is off.
    if (this.#config?.prewalk.enabled === false) return undefined;
    let pending = claimFabricHandoff(this.prewalk, execution, sessionId, resultFormat);
    if (pending && pending.kind !== "explicit" && this.#config?.prewalk.detectShellWrites && this.#cwd) {
      // This audited outer boundary consumed all mutations in its window, even
      // when the plan gate withholds the handoff. Do not rediscover those edits
      // as shell drift on a later read. The fs-only path advances in evaluate().
      await this.prewalkDrift.captureBaseline(sessionId, this.#cwd);
    }
    if (!pending && this.#config?.prewalk.detectShellWrites) {
      pending = await this.#claimShellWriteHandoff(execution, sessionId, resultFormat);
    }
    if (pending?.kind === "prewalk-plan") {
      // Nothing hands off yet: the frontier model owes a plan checkpoint at this
      // boundary, and the arm stays armed for the mutation that follows it.
      if (!deliverPrewalkPlanCheckpoint(this.pi, pending)) {
        this.prewalk.reopenPlanCheckpoint();
      }
      return undefined;
    }
    if (pending) {
      this.activity.resume(outerToolCallId);
      this.activity.beginCall(outerToolCallId, {
        callId: pending.audit.nestedToolCallId,
        ref: pending.audit.ref,
        args: pending.args,
      });
    }
    return pending;
  }

  // Filesystem fallback for writes audits cannot attribute (shell heredocs,
  // sed -i, formatter binaries). Gated on a successful Pi shell call in the program
  // so read-only scans never pay the stat walk, and external saves can only
  // mis-fire inside a bash-running window. The tracker refreshes its baseline
  // on every evaluation, claimed or not, so one change never fires twice.
  async #claimShellWriteHandoff(
    execution: FabricExecutionResult,
    sessionId: string,
    resultFormat: FabricResultFormat,
  ): Promise<PendingFabricHandoff | FabricPrewalkPlanCheckpoint | undefined> {
    if (!this.prewalk.isArmed(sessionId) || !this.#cwd) return undefined;
    if (!execution.audits.some((audit) => isPiShellRef(audit.ref) && audit.success === true)) {
      return undefined;
    }
    const drift = await this.prewalkDrift.evaluate(sessionId, this.#cwd);
    if (!drift || drift.files.length === 0) return undefined;
    return claimFabricFsDriftHandoff(this.prewalk, execution, sessionId, drift, resultFormat);
  }

  async runHandoffAtBoundary(
    pending: PendingFabricHandoff,
    outerToolResult: AgentToolResultMessage,
    context: ExtensionContext,
  ): Promise<Record<string, unknown>> {
    if (!this.#agentsProvider) throw new Error("Pi Fabric has not initialized");
    const runId = outerToolResult.toolCallId;
    const callId = pending.audit.nestedToolCallId;
    const result = await runFabricHandoffAtBoundary(
      this.prewalk,
      this.#agentsProvider,
      this.pi,
      pending,
      outerToolResult,
      context,
      (update) => this.activity.updateCall(runId, callId, update),
      () => this.config.agents,
    );
    const succeeded = result.completed === true || result.continued === true;
    const error = typeof result.error === "string" ? result.error : undefined;
    this.activity.finishCall(runId, callId, {
      success: succeeded,
      result,
      ...(pending.audit.preview !== undefined ? { preview: pending.audit.preview } : {}),
      ...(error ? { error } : {}),
    });
    this.activity.finish(runId, succeeded, error);
    return result;
  }

  get advisorsHalted(): boolean {
    return (!this.#config?.mesh.enabled || Boolean(this.#actors?.halted)) && (!this.#jevObservationHost || this.#jevObservationHost.halted);
  }

  /**
   * Escape's stop-the-world halt is in force: actors or the Jev observation host are halted.
   * Both latches outlive the cancelled runs and lift only on the user's next input, so the
   * self-reload gate holds with the mesh off too (review/astra on pi-fabric#160).
   */
  get escapeHalted(): boolean {
    return Boolean(this.#actors?.halted) || Boolean(this.#jevObservationHost?.halted);
  }

  haltMain(): void { this.#mainAgent?.halt(); }

  haltAdvisors(): number {
    const actors = this.#config?.mesh.enabled ? this.#actors?.haltAll().halted ?? 0 : 0;
    return actors + (this.#jevObservationHost?.halt() ?? 0);
  }

  noteMainActivity(context: ExtensionContext): void {
    this.#actors?.noteMainActivity(context.isIdle());
    this.#participants?.scheduleRefresh();
  }

  dispatchHostEvent(
    event: FabricActorHostEvent,
    payload: unknown,
    context: ExtensionContext,
  ): number {
    const observed = this.#jevObservationHost?.observe(event, payload, {
      sessionId: context.sessionManager.getSessionId(), signal: context.signal,
    }) ?? 0;
    if (
      !this.#actors ||
      !this.#config?.mesh.enabled ||
      this.#config.schema.mode === "enforce"
    ) return observed;
    const idle = context.isIdle();
    const source = event === "input" && isPlainObject(payload) && typeof payload.source === "string" ? payload.source : undefined;
    if (!this.#actors.observeHostEvent(event, idle, source)) return observed;
    const branch = context.sessionManager.getBranch();
    const { digest, transcript } = buildActorContext(
      branch as unknown[],
      this.#config.mesh.actorContextEntries,
      this.#config.mesh.eventContextChars,
    );
    const prepared = prepareFabricActorHostPayload(
      payload,
      this.#config.mesh.eventContextChars,
    );
    const preparedContext = prepareFabricActorHostPayload(
      { digest, transcript },
      this.#config.mesh.eventContextChars,
    ).payload;
    const safeContext = isPlainObject(preparedContext)
      ? preparedContext
      : { digest: {}, transcript: [String(preparedContext)] };
    return observed + this.#actors.dispatchObservedHostEvent(
      event,
      {
        event,
        session: { id: context.sessionManager.getSessionId(), cwd: context.cwd },
        digest: safeContext.digest ?? {},
        transcript: safeContext.transcript ?? [],
        signal: {
          payload: prepared.payload,
          ...(prepared.media.length > 0 ? { media: prepared.media } : {}),
          idle,
          observedAt: Date.now(),
        },
      },
      prepared.images,
    );
  }

  #observeComponentTransitions(componentId?: string): void {
    if (!this.#suppressResidentGuidanceSync) {
      this.#residency?.updateModelGuidance(this.modelGuidance());
    }
    let components: FabricComponentInfo[];
    if (componentId) {
      try {
        components = this.#componentSupervisor
          ? [this.#componentSupervisor.status(componentId)]
          : [];
      } catch {
        this.#componentTransitionSignatures.delete(componentId);
        return;
      }
    } else {
      components = this.#componentSupervisor?.list() ?? [];
    }
    const visible = componentId ? undefined : new Set<string>();
    for (const component of components) {
      visible?.add(component.id);
      const signature = [
        component.state,
        component.revision,
        component.targetDigest ?? "",
        component.missing.join("\u0000"),
        component.optionalMissing.join("\u0000"),
        component.error ?? "",
        component.cleanupErrors?.join("\u0000") ?? "",
        JSON.stringify(component.guidance ?? []),
      ].join("\u0001");
      if (this.#componentTransitionSignatures.get(component.id) === signature) continue;
      this.#componentTransitionSignatures.set(component.id, signature);
      const publication = this.publishHostLifecycle("component.state", component)
        .catch(() => undefined);
      this.#componentTransitionPublications.add(publication);
      void publication.finally(() => this.#componentTransitionPublications.delete(publication));
    }
    if (visible) {
      for (const id of this.#componentTransitionSignatures.keys()) {
        if (!visible.has(id)) this.#componentTransitionSignatures.delete(id);
      }
    }
  }

  async publishHostLifecycle(
    event: FabricLifecycleEventType,
    payload: unknown,
  ): Promise<void> {
    if (
      !this.#lifecycle ||
      !this.#identity ||
      this.#identity.kind !== "main" ||
      !this.#participants
    ) return;
    const self = this.#participants.self();
    const metadata = lifecycleMetadata(event, payload);
    const lifecycle = this.#lifecycle;
    await this.#backgroundMesh.enqueue(() => lifecycle.publish({
      source: {
        id: self.id,
        name: self.name,
        kind: self.kind,
        rootId: self.rootId,
        runner: self.runner,
        ownerHostId: self.ownerHostId,
        ownerIdentityId: self.ownerIdentityId,
      },
      event,
      occurredAt: lifecycleObservedAt(payload),
      ...(metadata !== undefined ? { data: metadata } : {}),
    }));
  }

  registerExternal(provider: FabricProvider, options: { overwrite?: boolean } = {}): void {
    if (this.#managedHost) {this.#managedHost.register(provider, options.overwrite); return;}
    if (
      provider.name === "fabric" ||
      provider.name === "components" ||
      FABRIC_COMPONENT_PROVIDER_NAMES.some((name) => name === provider.name)
    ) {
      throw new Error(`Reserved Fabric provider name: ${provider.name}`);
    }
    if (this.#externalProviders.has(provider.name) && !options.overwrite) {
      throw new Error(`Fabric provider already registered: ${provider.name}`);
    }
    this.#externalProviders.set(provider.name, provider);
    if (this.#registry) this.#registry.register(provider, options);
  }

  registerExternalComponent(
    component: FabricComponentDefinition,
    options: { overwrite?: boolean } = {},
  ): void {
    if (this.#managedHost) throw new Error("Managed host component registration is disabled");
    if (component.name.startsWith(FABRIC_PROVIDER_COMPONENT_PREFIX)) {
      throw new Error(`Reserved Fabric component name: ${component.name}`);
    }
    this.componentCatalog.register(component, options);
  }

  async settleComponents(): Promise<void> {
    await this.#componentControl?.settle();
    await this.#componentLoader?.settle();
  }

  async shutdown(reason?: string, targetSessionFile?: string): Promise<void> {
    try {
      await this.#shutdownSteps(reason, targetSessionFile);
    } catch (error) {
      // A failed teardown step must not leave the heartbeat timer running past this
      // session's ctx (smarty-dev#5962): stop presence, then surface the failure.
      await this.#participants?.close().catch(() => undefined);
      throw error;
    }
  }

  async #shutdownSteps(reason?: string, targetSessionFile?: string): Promise<void> {
    this.sessionApprovals.reset();
    // Disposable actor/task results and journals belong to their worker, not
    // to this runtime's advisory presence/heartbeat writes. EOF must not convoy
    // behind the shared mesh lock before local teardown stops its timers (#5256).
    // Main/resident-root custody and reload checkpoints retain their normal joins.
    if (reason === "exit" || reason === "quit") this.#disposableMeshWrites?.abort();
    if (reason === "reload") {
      // Stop admission synchronously, before the first await. In-flight handlers may only journal.
      this.#mainAgent?.prepareReload();
      this.#control?.pause();
      await this.#participants?.quiesce("reload").catch(() => undefined);
    }
    this.#completionInbox?.close();
    this.#completionInbox = undefined;
    this.#shellInbox?.close();
    this.#shellInbox = undefined;
    // Stop the resident drainer (and await its drain) before the Main journal closes: a delivery
    // must never reach a Main that can no longer journal it (review round 3 on pi-fabric#160).
    await this.#residency?.close().catch(() => undefined);
    if (reason !== "reload") this.#mainAgent?.closeFollowUpDrain();
    this.#suppressResidentGuidanceSync = true;
    await this.#deactivateRepairs();
    clearActiveCompiledSurface();
    if (reason !== "reload") await this.#participants?.quiesce().catch(() => undefined);
    this.#stopComponentWatch?.();
    this.#stopComponentWatch = undefined;
    await this.#componentControl?.close();
    this.#componentControl = undefined;
    this.#componentConfiguration = undefined;
    await this.#componentLoader?.close();
    await Promise.allSettled([...this.#componentTransitionPublications]);
    await this.#sessionCapabilityLease?.release().catch(() => undefined);
    this.#sessionCapabilityLease = undefined;
    await this.#backgroundMesh.close();
    await this.#rootInbox?.close();
    await this.#lifecycle?.close();
    await closeWithActors(this.#actors, () => this.#control?.close(), () => this.#residency?.close());
    // Actor shutdown starts before control drains (an in-flight ask may await an actor).
    // Only now can admitted Main handlers no longer write the reload journal.
    if (reason === "reload") this.#mainAgent?.closeFollowUpDrain();
    await this.#closeRecords();
    await this.#agents?.close();
    await this.shellJobs.close();
    await this.#outputArtifacts.close();
    if ((reason === "new" || reason === "resume") && targetSessionFile && this.#mainAgent?.local &&
      this.#mainAgent.sessionId && this.#mesh && this.#config?.mesh.enabled) {
      await stageMainSuccessor(this.#mesh, this.#mainAgent.id, this.#mainAgent.sessionId, targetSessionFile);
    }
    try {
      await this.#registry?.close();
    } finally {
      if (reason === "exit") await this.#participants?.closeLineage();
      else await this.#participants?.close();
    }
    this.#registry = undefined;
    this.#config = undefined;
    this.#execution = undefined;
    this.#agents = undefined;
    this.#actors = undefined;
    this.#globalActors = undefined;
    this.#mesh = undefined;
    this.#identity = undefined;
    this.#mainAgent = undefined;
    this.#participants = undefined;
    this.#control = undefined;
    this.#lifecycle = undefined;
    this.#residency = undefined;
    this.#agentsProvider = undefined;
    this.#compact = undefined;
    this.#schema = undefined;
    this.#componentSupervisor = undefined;
    this.#componentLoader = undefined;
    this.#componentTransitionSignatures.clear();
    this.#componentTransitionPublications.clear();
    this.#sessionCapabilityLease = undefined;
    this.#unsubscribeCapturedCatalog?.();
    this.#unsubscribeCapturedCatalog = undefined;
    this.componentCatalog.clear();
    this.#builtinComponentNames.clear();
    this.#cwd = undefined;
    this.activity.reset();
    this.#widgetDismissedAt = 0;
    this.#externalProviders.clear();
    this.prewalk.cancel();
    this.prewalkDrift.clear();
  }

  /**
   * Session-owned work a reload would cancel besides task agents and actor runs: live shell jobs
   * (background, monitors, auto-detached) and running Jev programs, observers included (smarty-dev#2160).
   * A finished shell job counts until its notice is sent and delivered; a background Jev run until
   * its result is read (smarty-dev#2216).
   */
  backgroundWorkCount(): number {
    return this.#shellJobs.unannounced() + (this.#shellInbox?.pendingCount() ?? 0) + (this.#jevPrograms?.runningCount() ?? 0);
  }

  /** Best-effort ops event on the mesh, e.g. ops.fabric.reloaded (smarty-dev#2160). */
  publishOpsEvent(topic: string, kind: string, data: Record<string, unknown>): Promise<void> {
    if (!this.#mesh || !this.#identity || !this.#config?.mesh.enabled) return Promise.resolve();
    const mesh = this.#mesh, identity = this.#identity;
    return this.#backgroundMesh.enqueue(() => mesh.publish({ topic, kind, from: identity, data }));
  }

  // Publish a best-effort mesh event to the durable `fabric.compact` topic so
  // other roots, agents, and actors can observe compaction transitions.
  // Activity-only sessions (mesh disabled) silently skip this.
  #publishCompactEvent(kind: string, data: CompactPendingIntent | CompactLastCommit): void {
    if (!this.#mesh || !this.#identity || !this.#config?.mesh.enabled) return;
    const mesh = this.#mesh, identity = this.#identity;
    // publish is async: a synchronous try/catch cannot contain a lock rejection.
    void this.#backgroundMesh.enqueue(() => mesh.publish({
      topic: "fabric.compact", kind, from: identity, data,
    }));
  }

  #refreshRepairCatalog(): void {
    if (!this.#repairs || !this.#registry) return;
    // Capture suspension (session_start, /fabric reload) clears the catalog
    // only transiently: the same tools refill on re-arm, so recomputing here
    // would flip the digest to an empty-catalog value that promotion could
    // then persist, destroying the stable catalog's table. Freeze the surface
    // instead; the refill re-commits it (or legitimately starts a new one).
    if (this.capturedTools.suspended) return;
    this.#repairs.setCatalogSurface({
      providers: this.#registry.providers().map((provider) => provider.name),
      capturedTools: this.capturedTools.list().map((entry) => entry.name),
    });
  }

  async #deactivateRepairs(): Promise<void> {
    const repairs = this.#repairs;
    this.#repairs = undefined;
    clearActiveRepairCompiler(repairs);
    await repairs?.flush();
  }

  async #closeInternal(): Promise<void> {
    // /fabric reload and bootstrap replacement rebuild Fabric, not the creating
    // Main's lineage. Preserve its mailbox/address even if replacement fails.
    this.#mainAgent?.prepareReload();
    this.#control?.pause();
    await this.#participants?.quiesce("reload").catch(() => undefined);
    this.#completionInbox?.close();
    this.#completionInbox = undefined;
    this.#shellInbox?.close();
    this.#shellInbox = undefined;
    // Stop the resident drainer (and await its drain) before the Main journal closes: a delivery
    // must never reach a Main that can no longer journal it (review round 3 on pi-fabric#160).
    await this.#residency?.close().catch(() => undefined);
    this.#mainAgent?.closeFollowUpDrain();
    await this.shellJobs.close();
    await this.#deactivateRepairs();
    if (!this.#registry) {
      await this.#outputArtifacts.close();
      return;
    }
    this.#stopComponentWatch?.();
    this.#stopComponentWatch = undefined;
    await this.#componentControl?.close();
    this.#componentControl = undefined;
    this.#componentConfiguration = undefined;
    await this.#componentLoader?.close();
    await Promise.allSettled([...this.#componentTransitionPublications]);
    await this.#sessionCapabilityLease?.release().catch(() => undefined);
    this.#sessionCapabilityLease = undefined;
    await this.#backgroundMesh.close();
    await this.#rootInbox?.close();
    await this.#lifecycle?.close();
    await closeWithActors(this.#actors, () => this.#control?.close(), () => this.#residency?.close());
    await this.#closeRecords();
    await this.#agents?.close();
    // Reinitialization, like shutdown, must drain workers before releasing output artifacts.
    await this.#outputArtifacts.close();
    const externalNames = new Set(this.#externalProviders.keys());
    try {
      await this.#registry.close(externalNames);
    } finally {
      await this.#participants?.close();
    }
    this.#registry = undefined;
    this.#execution = undefined;
    this.#agents = undefined;
    this.#actors = undefined;
    this.#mesh = undefined;
    this.#identity = undefined;
    this.#mainAgent = undefined;
    this.#participants = undefined;
    this.#control = undefined;
    this.#lifecycle = undefined;
    this.#residency = undefined;
    this.#agentsProvider = undefined;
    this.#compact = undefined;
    this.#schema = undefined;
    this.#componentSupervisor = undefined;
    this.#componentLoader = undefined;
    this.#componentTransitionSignatures.clear();
    this.#componentTransitionPublications.clear();
    this.#sessionCapabilityLease = undefined;
    this.#unsubscribeCapturedCatalog?.();
    this.#unsubscribeCapturedCatalog = undefined;
  }
}

const scalarMetadata = (
  value: unknown,
  keys: readonly string[],
): Record<string, string | number | boolean | null> | undefined => {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return undefined;
  const source = value as Record<string, unknown>;
  const metadata: Record<string, string | number | boolean | null> = {};
  for (const key of keys) {
    const nested = source[key];
    if (
      typeof nested === "string" ||
      typeof nested === "number" ||
      typeof nested === "boolean" ||
      nested === null
    ) metadata[key] = nested;
  }
  return Object.keys(metadata).length > 0 ? metadata : undefined;
};

const lifecycleMetadata = (
  event: FabricLifecycleEventType,
  payload: unknown,
): Record<string, string | number | boolean | null> | undefined => {
  switch (event) {
    case "pi.input":
      return scalarMetadata(payload, ["source", "streamingBehavior"]);
    case "pi.agent_end":
      return scalarMetadata(payload, ["willRetry"]);
    case "pi.turn_end":
      return scalarMetadata(payload, ["turnIndex", "timestamp"]);
    case "pi.tool_error":
      return scalarMetadata(payload, ["toolCallId", "toolName"]);
    case "pi.session_compact":
      return scalarMetadata(payload, ["reason", "willRetry"]);
    case "component.state":
      return scalarMetadata(payload, [
        "id",
        "component",
        "parentId",
        "state",
        "guarantee",
        "revision",
        "targetDigest",
      ]);
    default:
      return undefined;
  }
};

const lifecycleObservedAt = (payload: unknown): number => {
  if (typeof payload !== "object" || payload === null || Array.isArray(payload)) return Date.now();
  const timestamp = (payload as Record<string, unknown>).timestamp;
  return typeof timestamp === "number" && Number.isFinite(timestamp) ? timestamp : Date.now();
};

const isPlainObject = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const deepAssign = (
  target: Record<string, unknown>,
  source: Record<string, unknown>,
): void => {
  for (const key of Object.keys(target)) {
    if (!(key in source)) delete target[key];
  }
  for (const [key, value] of Object.entries(source)) {
    const targetValue = target[key];
    if (isPlainObject(value) && isPlainObject(targetValue)) {
      deepAssign(targetValue, value);
    } else {
      target[key] = value;
    }
  }
};
