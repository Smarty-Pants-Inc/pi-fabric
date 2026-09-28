import type { Usage } from "@earendil-works/pi-ai";
import { rootInboxMessage, rootInboxSession } from "./topology/root-inbox.js";
import { foregroundWaitRefusal } from "./guards/foreground-wait.js";
import { killsByPattern, PATTERN_KILL_REASON } from "./core/pattern-kill.js";
import { registerJevAuth } from "./jev/auth.js";
import { yieldsToExplicitFabric } from "./core/explicit-fabric.js";
import type {
  ExtensionAPI,
  ExtensionContext,
} from "@earendil-works/pi-coding-agent";
import { defaultCodePreviewSettings } from "./ui/code-preview.js";
import {
  type FabricToolShellDecorator,
  withCodePreviewShell,
} from "./ui/code-preview-shell.js";
import { registerFabricActorHostEventObservers } from "./actors/host-event-observer.js";
import { CapturedToolCatalog } from "./capture/catalog.js";
import { installRegisteredToolCapture } from "./capture/interceptor.js";
import { registerFabricCommand } from "./commands/fabric.js";
import { resolveAgentDir } from "./core/agent-dir.js";
import {
  comparableCompiledSurfaceScore,
  BackgroundEntropyCompiler,
  BackgroundSessionSelector,
  SessionObservationCache,
  entropyRepairRows,
  formatEntropyCompileNotice,
  liveSurfaceSnapshot,
  loadCompiledSurfaceAsync,
  loadObservationPoolAsync,
  machineSessionFilesAsync,
  saveCompiledSurfaceAsync,
  saveObservationPoolAsync,
  sessionWindowEvidenceAsync,
} from "./entropy/index.js";
import { setActiveCompiledSurface } from "./entropy/active.js";
import {
  filterPrewalkContinuationMessages,
  filterPrewalkPlanningDirectives,
  withTrajectoryRearmDirective,
} from "./prewalk/messages.js";
import {
  restoreBorrowedInPlaceMain,
  settleInPlacePrewalk,
} from "./prewalk/return.js";
import type { PendingFabricHandoff } from "./prewalk/handoff.js";
import { autoArmFabricPrewalk } from "./prewalk/arm.js";
import {
  DEFAULT_FABRIC_CONFIG,
  effectiveToolCaptureConfig,
} from "./config.js";
import { registerCompactionHook } from "./compaction/hook.js";
import { compactAtConfiguredThreshold } from "./compaction/threshold.js";
import {
  createToolOwnershipReassertion,
  FabricToolLifecycle,
  FabricToolOwnership,
  ownsFabricToolSource,
} from "./core/tool-ownership.js";
import {
  expandSkillDirMarkersForRead,
  expandSkillDirMarkersInSkillBlock,
} from "./core/skill-dir.js";
import { coreOverridePromptGuidance } from "./core/core-override-guidance.js";
import { PI_CORE_TOOL_NAMES } from "./core/pi-tools.js";
import {
  fabricExecutionKernelGuidance,
  defaultFabricExecutionGuidance,
  fabricSchemaGuidance,
  extensionToolRosterGuidance,
} from "./core/system-guidance.js";
import {
  FABRIC_EXECUTION_GUIDANCE_SLOT,
  resolveFabricModelGuidance,
} from "./components/model-guidance.js";
import { restoreSkillsForFullCodePrompt } from "./core/skill-prompt.js";
import { fabricSkillPaths } from "./core/kernel-skills.js";
import {
  formatProxyContractReminder,
  PROXY_CONTRACT_CUSTOM_TYPE,
  ProxyContractLedger,
  proxyContractMentionsInSkills,
  rewritableHiddenCapturedToolNames,
} from "./core/proxy-contract.js";
import {
  FabricDirectToolApproval,
  mergeFabricApprovalUsage,
} from "./core/direct-tool-approval.js";
import { buildSkillReferenceGuidance } from "./core/skill-references.js";
import { createFabricExecTool } from "./fabric-exec-tool.js";
import { FabricState } from "./fabric-state.js";
import { classifyToolResult } from "./repairs/classify.js";
import { getActiveRepairCompiler } from "./repairs/active.js";
import { piHostCompatibilityWarning } from "./host-compatibility.js";
import {
  FABRIC_COMPONENT_REGISTER_EVENT,
  FABRIC_PROVIDER_REGISTER_EVENT,
  type FabricComponentRegistration,
  type FabricProviderRegistration,
} from "./protocol.js";
import type { AgentToolResultMessage } from "./agents/types.js";
import { FabricUiController } from "./ui/controller.js";
import { installFabricEscapeHalt } from "./ui/escape-halt.js";
import { installFabricShellHangKeys } from "./ui/shell-hang-keys.js";
import { FabricToolDisplayController } from "./ui/tool-display.js";
import { configureHighlighting } from "./ui/highlight.js";
import { registerHandoffCompletionRenderer } from "./ui/handoff-completion.js";
import { formatFabricValue } from "./ui/structured.js";
import { truncateMiddle } from "./util.js";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { captureLoadedFileIdentity } from "./build-identity.js";
import { ownsRunReplyTool } from "./core/reply-tool-identity.js";

// Absolute path to the Fabric skills bundled with this extension. Resolved
// relative to the extension entry so it works both in development (src/) and
// in an installed package (dist/). Contributed via resources_discover so child
// Pi processes that load Fabric with -e (agents and actors) discover the
// same kernel-specific tree as Main. The package manifest exposes no skills;
// selecting exactly one tree avoids canonical-name collisions.
const FABRIC_EXTENSION_ENTRY_PATH = path.resolve(fileURLToPath(import.meta.url));
const FABRIC_ENTRY_DIR = path.dirname(FABRIC_EXTENSION_ENTRY_PATH);
const FABRIC_RUNTIME_PATHS = {
  extension: FABRIC_EXTENSION_ENTRY_PATH,
  worker: path.join(FABRIC_ENTRY_DIR, "worker.js"),
  residentHost: path.join(FABRIC_ENTRY_DIR, "residency", "launcher.js"),
  skills: path.resolve(FABRIC_ENTRY_DIR, "..", "skillsets"),
};
const FABRIC_SKILLS_DIR = FABRIC_RUNTIME_PATHS.skills;

// Loaded-code identity of this extension entry, captured while the module bytes
// on disk are still the bytes this process evaluated. prewalk.status compares
// it against the current file to expose stale-runtime reloads.
const FABRIC_ENTRY_IDENTITY = captureLoadedFileIdentity(import.meta.url);

const componentRegistrationFrom = (
  value: unknown,
): FabricComponentRegistration | undefined => {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return undefined;
  const registration = value as Partial<FabricComponentRegistration>;
  const component = registration.component;
  if (
    registration.version !== 1 ||
    typeof component !== "object" ||
    component === null ||
    typeof component.name !== "string" ||
    typeof component.activate !== "function"
  ) {
    return undefined;
  }
  return registration as FabricComponentRegistration;
};

const registrationFrom = (value: unknown): FabricProviderRegistration | undefined => {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return undefined;
  const registration = value as Partial<FabricProviderRegistration>;
  const provider = registration.provider;
  if (
    registration.version !== 1 ||
    typeof provider !== "object" ||
    provider === null ||
    typeof provider.name !== "string" ||
    typeof provider.description !== "string" ||
    typeof provider.list !== "function" ||
    typeof provider.describe !== "function" ||
    typeof provider.invoke !== "function"
  ) {
    return undefined;
  }
  return registration as FabricProviderRegistration;
};

const SKILL_REFERENCE_CUSTOM_TYPE = "pi-fabric-skill-reference";

export const FABRIC_MANAGED_HOST_VERSION = 1;
export type { FabricManagedHostOptions } from "./managed-host.js";
import type { FabricManagedHostOptions } from "./managed-host.js";

// A run the user aborted, or one that failed, must not start another turn by itself. Newer Pi
// names the outcome; for older Pi, the last assistant message's stop reason says it.
const settledCompleted = (event: unknown, context: ExtensionContext): boolean => {
  const outcome = (event as { outcome?: unknown }).outcome;
  if (typeof outcome === "string") return outcome === "completed";
  if (context.signal?.aborted) return false;
  const entries = context.sessionManager.getEntries();
  for (let index = entries.length - 1; index >= Math.max(0, entries.length - 50); index--) {
    const entry = entries[index] as { type?: string; message?: { role?: string; stopReason?: string } };
    if (entry.type === "message" && entry.message?.role === "assistant") {
      return entry.message.stopReason !== "aborted" && entry.message.stopReason !== "error";
    }
  }
  return true;
};

// Whether the session already holds an inbox batch: its cursor moves only then (smarty-dev#754).
const inboxHeldBy = (context: ExtensionContext) => rootInboxSession(context.sessionManager.getEntries());

// An idle Main reads its inbox this often (smarty-dev#1595). With the 60 s steer grace, an event
// published to an idle Main starts a turn about 60-75 s later. PI_FABRIC_INBOX_WAKE_MS overrides it.
// The idle wake needs a Pi that queues a triggered message behind a live prompt preflight;
// otherwise a wake can start a run that makes a prompt in its preflight fail (#107 review F2).
// Pi declares it on the extension API (pi.hostCapabilities, Smarty-Pants-Inc/pi#74 and #76), not through
// a module export: Fabric ships its own copy of the Pi package, whose export describes that copy.
// An older Pi lacks it, and the wake stays off. Tests inject the capability under the global
// symbol below, since their Pi predates it.
type HostCapabilities = { triggeredMessageQueuesBehindPreflight?: unknown; promptPendingVisible?: unknown };
const TEST_HOST_CAPABILITIES = Symbol.for("pi-fabric.test.hostCapabilities");
const hostQueuesTriggeredBehindPreflight = (pi: ExtensionAPI): boolean => {
  const injected = (globalThis as Record<symbol, HostCapabilities | undefined>)[TEST_HOST_CAPABILITIES];
  const declared = (pi as { hostCapabilities?: HostCapabilities }).hostCapabilities;
  const capabilities = injected ?? declared;
  return capabilities?.triggeredMessageQueuesBehindPreflight === true && capabilities.promptPendingVisible === true;
};

const inboxWakeMs = (): number => {
  const value = Number(process.env.PI_FABRIC_INBOX_WAKE_MS);
  return Number.isFinite(value) && value > 0 ? value : 15_000;
};

export default async function piFabric(pi: ExtensionAPI, options: { managedHost?: FabricManagedHostOptions } = {}): Promise<void> {
  // A different Fabric requested explicitly with -e (a worker's parent Fabric) wins over
  // this discovered copy; registering both makes Pi refuse to start (fabric_exec conflict).
  if (!options.managedHost && yieldsToExplicitFabric(FABRIC_EXTENSION_ENTRY_PATH)) return;
  if (!options.managedHost) registerJevAuth(pi);
  const codePreviewSettings = defaultCodePreviewSettings();
  const decorateShell: FabricToolShellDecorator = withCodePreviewShell;
  let compatibilityWarningShown = false;
  configureHighlighting(
    codePreviewSettings.shikiTheme,
    codePreviewSettings.syntaxHighlighting,
  );
  const capturedTools = new CapturedToolCatalog();
  const proxyContract = new ProxyContractLedger();
  const state = new FabricState(pi, capturedTools, {
    paths: FABRIC_RUNTIME_PATHS,
    ...(FABRIC_ENTRY_IDENTITY ? { entryIdentity: FABRIC_ENTRY_IDENTITY } : {}),
    ...(options.managedHost ? {managedHost: options.managedHost} : {}),
  });
  const directToolApproval = new FabricDirectToolApproval(
    pi,
    () => state.config,
    state.sessionApprovals,
  );
  const pendingHandoffs = new Map<string, PendingFabricHandoff>();
  const toolOwnership = new FabricToolOwnership(pi);
  const fabricUi = new FabricUiController(state, codePreviewSettings, {
    getToolDefinition: (name) => name === "fabric_exec" ? fabricTool : capturedTools.get(name)?.definition,
    get markdownTransformers() { return capturedTools.runner?.getMarkdownTransformers(); },
    getMessageRenderer: (type) => capturedTools.runner?.getMessageRenderer(type),
  });
  const toolDisplay = new FabricToolDisplayController();

  const capturePolicy = () => effectiveToolCaptureConfig(state.config);
  const fabricOwnsModelTools = (): boolean =>
    state.config.fullCodeMode || state.config.schema.mode === "enforce";
  // Captured tools that must stay out of the model's active set in full code
  // mode: every captured extension tool minus the capture.keepVisible names.
  const hiddenCapturedToolNames = (): Set<string> => {
    const visible = new Set(capturePolicy().keepVisible);
    return new Set(
      capturedTools.list().map((entry) => entry.name).filter((name) => !visible.has(name)),
    );
  };
  // Pi auto-activates tools that newly appear in the registry on every tool
  // refresh; re-assert ownership afterwards so captured tools stay hidden from
  // the model even when a late-loading extension triggers a refresh. Refresh
  // callbacks arrive before session initialization too, so reassertion waits
  // for state to be ready rather than reading an uninitialized config.
  const { reassert: reassertToolOwnership, schedule: scheduleOwnershipReassert } =
    createToolOwnershipReassertion({
      ready: () => state.cwd !== undefined,
      active: () => {
        const policy = capturePolicy();
        return policy.enabled && policy.hideFromModel && fabricOwnsModelTools();
      },
      hiddenNames: hiddenCapturedToolNames,
      apply: (hidden) => toolOwnership.apply(true, hidden),
    });

  const unsubscribeComponentRegistration = pi.events.on(
    FABRIC_COMPONENT_REGISTER_EVENT,
    (value: unknown) => {
      const registration = componentRegistrationFrom(value);
      if (!registration) throw new Error("Invalid Pi Fabric component registration");
      state.registerExternalComponent(
        registration.component,
        registration.overwrite === undefined ? {} : { overwrite: registration.overwrite },
      );
    },
  );

  const unsubscribeProviderRegistration = pi.events.on(
    FABRIC_PROVIDER_REGISTER_EVENT,
    (value: unknown) => {
      const registration = registrationFrom(value);
      if (!registration) throw new Error("Invalid Pi Fabric provider registration");
      state.registerExternal(
        registration.provider,
        registration.overwrite === undefined ? {} : { overwrite: registration.overwrite },
      );
    },
  );

  pi.on("resources_discover", async (_event, context) => {
    if (!state.bootstrapped) await state.bootstrap(context);
    return { skillPaths: fabricSkillPaths(FABRIC_SKILLS_DIR, state.config.executor.kernel) };
  });

  const fabricTool = createFabricExecTool(
    state,
    codePreviewSettings,
    pendingHandoffs,
    decorateShell,
    toolDisplay,
  );
  const refreshCodePreviewSettings = (): void => {
    Object.assign(codePreviewSettings, state.config.codePreview);
    configureHighlighting(
      codePreviewSettings.shikiTheme,
      codePreviewSettings.syntaxHighlighting,
    );
  };
  const fabricToolLifecycle = new FabricToolLifecycle(
    () => ownsFabricToolSource(pi.getAllTools(), FABRIC_EXTENSION_ENTRY_PATH),
    () => state.initialized ? state.execution.authorizer : undefined,
    () => state.initialized ? directToolApproval : undefined,
    () => ownsRunReplyTool(pi.getAllTools()),
  );

  const inactiveCapturePolicy = {
    ...structuredClone(DEFAULT_FABRIC_CONFIG.capture),
    enabled: false,
    hideFromModel: false,
  };
  const toolCapture = await installRegisteredToolCapture({
    anchorDefinition: fabricTool,
    catalog: capturedTools,
    initialPolicy: inactiveCapturePolicy,
    onCatalogRefresh: () => {
      scheduleOwnershipReassert();
    },
  });
  registerHandoffCompletionRenderer(pi);
  pi.registerTool(fabricTool);

  const applyFabricMode = (): void => {
    // Re-applying the persistent policy ends any suspension window; do it
    // before setPolicy so a config-disabled policy recomputes derived
    // surfaces against the (now stable) empty catalog.
    capturedTools.markResumed();
    toolCapture.setPolicy(capturePolicy());
    Object.assign(
      fabricTool,
      createFabricExecTool(state, codePreviewSettings, pendingHandoffs, decorateShell, toolDisplay),
    );
    pi.registerTool(fabricTool);
    toolOwnership.apply(
      fabricOwnsModelTools(),
      fabricOwnsModelTools() ? hiddenCapturedToolNames() : undefined,
    );
    capturedTools.refresh();
  };
  const suspendToolCapture = (): void => {
    // Mark the suspension before the policy flip: setPolicy clears the
    // catalog, and the freeze must already be in effect when that clear
    // reaches derived-surface listeners.
    capturedTools.markSuspended();
    toolCapture.setPolicy(inactiveCapturePolicy);
  };

  // ESC stop-the-world: a lone Escape (debounced to ignore escape sequences
  // such as arrow keys) halts every persistent actor — aborting in-flight runs
  // and cancelling queued work — and arms a stop-the-world gate that freezes
  // host-event and mesh dispatch so the interrupted actors are not re-armed by
  // the interrupt's own turn_end / agent_settled events. The gate lifts when the
  // user resumes by sending a new message (the "input" host event). Escape is
  // observed but not consumed, so Pi's native cancel-streaming still fires;
  // single ESC therefore stops the current turn and the advisor/supervisor
  // actors and event-driven Jev observers at once. Jev observers are cancelled,
  // not automatically restarted. Also works without mesh. ui.haltOnEscape opts out.
  let haltOnEscapeUnsubscribe: (() => void) | undefined;
  let shellHangKeysUnsubscribe: (() => void) | undefined;
  const uninstallHaltOnEscape = (): void => {
    haltOnEscapeUnsubscribe?.();
    haltOnEscapeUnsubscribe = undefined;
  };
  const uninstallShellHangKeys = (): void => {
    shellHangKeysUnsubscribe?.();
    shellHangKeysUnsubscribe = undefined;
  };
  const installHaltOnEscape = (context: ExtensionContext): void => {
    uninstallHaltOnEscape();
    if (!state.config.ui.haltOnEscape || (!state.config.mesh.enabled && !state.config.jev.enabled)) return;
    haltOnEscapeUnsubscribe = installFabricEscapeHalt(context, {
      enabled: () => state.initialized && (state.config.mesh.enabled || state.config.jev.enabled) && state.config.ui.haltOnEscape,
      ownsInput: () => fabricUi.ownsInput,
      halted: () => state.advisorsHalted,
      halt: () => state.haltAdvisors(),
    });
  };
  const installShellHangKeys = (context: ExtensionContext): void => {
    uninstallShellHangKeys();
    shellHangKeysUnsubscribe = installFabricShellHangKeys(context, {
      enabled: () => state.initialized,
      ownsInput: () => fabricUi.ownsInput,
      jobs: () => state.shellJobs,
    });
  };

  const refreshProxyLedger = (context: ExtensionContext): void => {
    proxyContract.restoreFromEntries(context.sessionManager?.getBranch?.() ?? []);
  };

  // alwaysRearm means always armed: every session opens with prewalk armed.
  // Config-health skips (no prewalk.model, gated modes) warn once per process
  // rather than on every session switch.
  let prewalkAutoArmNoticeShown = false;
  const autoArmPrewalk = async (context: ExtensionContext): Promise<void> => {
    const skipReason = await autoArmFabricPrewalk(state, context, pi);
    if (!skipReason || prewalkAutoArmNoticeShown || !context.hasUI) return;
    prewalkAutoArmNoticeShown = true;
    context.ui.notify(skipReason, "warning");
  };

  const cleanupActivationSideEffects = (): void => {
    uninstallHaltOnEscape();
    uninstallShellHangKeys();
    fabricUi.stop();
  };
  state.setActivationHook(async (context) => {
    refreshCodePreviewSettings();
    await autoArmPrewalk(context);
    applyFabricMode();
    fabricUi.start(context);
    installHaltOnEscape(context);
    installShellHangKeys(context);
  }, cleanupActivationSideEffects);

  // Continual entropy reduction runs off the interaction path. Session-tree
  // discovery and JSONL ingestion use async I/O, scoring yields in fixed trace
  // chunks, and durable stores acquire locks cooperatively. Turn hooks only
  // enqueue work; a pending turn coalesces to the newest context.
  let entropyEvidenceThisTurn = false;
  let entropyCompileInFlight: Promise<void> | undefined;
  let entropyCompilePending: EntropyCompileRequest | undefined;
  let entropyLifecycleEpoch = 0;
  const createEntropyCaches = () => ({
    compiler: new BackgroundEntropyCompiler(),
    observations: new SessionObservationCache(),
    sessions: new BackgroundSessionSelector(machineSessionFilesAsync),
  });
  let entropyCaches = createEntropyCaches();

  interface EntropyCompileRequest {
    context: ExtensionContext;
    delayMs: number;
    epoch: number;
  }

  const compileEntropyNow = async (
    context: ExtensionContext,
    epoch: number,
  ): Promise<void> => {
    const current = (): boolean =>
      epoch === entropyLifecycleEpoch && state.initialized && state.config.entropy.compile;
    if (!current()) return;
    const agentDir = resolveAgentDir();
    const cwd = state.cwd ?? context.cwd;
    const repairs = entropyRepairRows(state.repairs.repairs);
    const caches = entropyCaches;
    const [files, loaded, poolLoaded, snapshot] = await Promise.all([
      caches.sessions.select(agentDir, cwd, context.sessionManager.getSessionFile?.()),
      loadCompiledSurfaceAsync(agentDir),
      loadObservationPoolAsync(agentDir),
      liveSurfaceSnapshot({ registry: state.registry, extensionContext: context, cwd }),
    ]);
    if (!current() || loaded.error) return;
    const evidence = await sessionWindowEvidenceAsync(files, { windowsOnly: true });
    if (!current()) return;
    const mergedPool = await caches.observations.merge(
      poolLoaded.file,
      evidence.observationWindows,
    );
    if (!poolLoaded.error && (mergedPool.mergedSessions > 0 || !poolLoaded.file)) {
      await saveObservationPoolAsync(agentDir, mergedPool.file);
    }
    if (!current()) return;
    const outcome = await caches.compiler.compile({
      windows: evidence.traceWindows,
      surface: snapshot,
      repairs,
      ...(loaded.file ? { artifact: loaded.file } : {}),
    });
    if (!current()) return;
    if (outcome.status === "compiled" && outcome.artifact) {
      const saved = await saveCompiledSurfaceAsync(agentDir, outcome.artifact);
      if (current()) {
        // Activate even when another process already persisted identical bytes.
        setActiveCompiledSurface(saved.file);
        // The notice is progress evidence, not a heartbeat: show it only when
        // the fresh score measurably lowered the previously persisted surface.
        const previousScore = comparableCompiledSurfaceScore(loaded.file, saved.file.metricVersion);
        if (previousScore !== undefined && outcome.report.score < previousScore && context.hasUI) {
          context.ui.notify(formatEntropyCompileNotice({
            beforeScore: previousScore,
            afterScore: outcome.report.score,
            normalizations: saved.file.normalizations?.length ?? 0,
          }), "info");
        }
      }
    }
    // Advisory abstraction suggestions remain available on demand. Static
    // normalization never asks the user to approve a compatibility repair.
  };

  const launchEntropyCompile = (request: EntropyCompileRequest): void => {
    const task = (async () => {
      try {
        if (request.delayMs > 0) {
          await new Promise((resolve) => setTimeout(resolve, request.delayMs));
        }
        await compileEntropyNow(request.context, request.epoch);
      } catch (error) {
        console.warn(
          `[pi-fabric] entropy compile failed: ${
            error instanceof Error ? error.message : String(error)
          }`,
        );
      }
    })();
    entropyCompileInFlight = task;
    void task.finally(() => {
      if (entropyCompileInFlight !== task) return;
      entropyCompileInFlight = undefined;
      const pending = entropyCompilePending;
      entropyCompilePending = undefined;
      if (pending) launchEntropyCompile(pending);
    });
  };

  const scheduleEntropyCompile = (
    context: ExtensionContext,
    delayMs = 250,
  ): void => {
    const request = { context, delayMs, epoch: entropyLifecycleEpoch };
    if (entropyCompileInFlight) {
      entropyCompilePending = request;
      return;
    }
    launchEntropyCompile(request);
  };

  const settleEntropyCompiles = async (): Promise<void> => {
    while (entropyCompileInFlight) await entropyCompileInFlight;
  };

  // smarty-dev#1595: an idle Main takes the work events addressed to it without waiting for a
  // turn, with the same call as a completed settle. The timer keeps the latest handler's context.
  // `settling` covers the settle handler: its own read and follow-up win, so a batch goes once.
  // A prompt in preflight (ctx.isPromptPending(), #111 review) takes the batch at its own turn
  // start, so the timer never sends one then: each batch has one owner, the turn or the timer.
  const inboxWake: {
    timer?: ReturnType<typeof setInterval> | undefined; context?: ExtensionContext | undefined;
    armed: boolean; reading: boolean; settling: boolean;
  } = { armed: true, reading: false, settling: false };
  // A host that declares promptPendingVisible has isPromptPending(); only a test's injected
  // capability on an older Pi lacks it.
  const promptPending = (context: ExtensionContext): boolean =>
    (context as { isPromptPending?: () => boolean }).isPromptPending?.() ?? false;
  const stopInboxWake = (): void => {
    if (inboxWake.timer) clearInterval(inboxWake.timer);
    inboxWake.timer = undefined;
    inboxWake.context = undefined;
  };
  const wakeIdleMain = async (): Promise<void> => {
    const context = inboxWake.context;
    if (!context || !inboxWake.armed || inboxWake.reading || !state.initialized) return;
    try {
      context.isIdle();
    } catch {
      // A stale context (Pi disposed or replaced the session): this timer has no session left.
      if (inboxWake.context === context) stopInboxWake();
      return;
    }
    const idle = () => inboxWake.context === context && inboxWake.armed && !inboxWake.settling &&
      context.isIdle() && !promptPending(context) && !context.hasPendingMessages();
    inboxWake.reading = true;
    try {
      if (!idle()) return;
      const inbox = await state.nextRootInbox(inboxHeldBy(context), idle);
      // A turn that started meanwhile takes the pending batch at its own start: never a second run.
      if (inbox?.events.length && idle()) pi.sendMessage(rootInboxMessage(inbox.events), { deliverAs: "followUp", triggerTurn: true });
    } catch {
      // A stale context (reload, session replacement) or a mesh error: the next tick or turn retries.
    } finally {
      inboxWake.reading = false;
    }
  };

  pi.on("session_start", async (_event, context) => {
    stopInboxWake();
    inboxWake.context = context;
    inboxWake.armed = true;
    entropyLifecycleEpoch += 1;
    entropyCaches = createEntropyCaches();
    entropyEvidenceThisTurn = false;
    entropyCompilePending = undefined;
    pendingHandoffs.clear();
    directToolApproval.clear();
    toolDisplay.clear();
    uninstallHaltOnEscape();
    uninstallShellHangKeys();
    fabricUi.stop();
    suspendToolCapture();
    proxyContract.reset();
    refreshProxyLedger(context);
    if (!compatibilityWarningShown) {
      compatibilityWarningShown = true;
      const warning = piHostCompatibilityWarning();
      if (warning) {
        console.warn(`[pi-fabric] ${warning}`);
        if (context.hasUI) context.ui.notify(warning, "warning");
      }
    }
    await state.bootstrap(context);
    // Inert until Pi queues a triggered message behind a live prompt preflight.
    if (hostQueuesTriggeredBehindPreflight(pi)) {
      inboxWake.timer = setInterval(() => void wakeIdleMain(), inboxWakeMs());
      inboxWake.timer.unref?.();
    }
    // bootstrap() cancels any live arm; the borrowed Main model survives so a
    // new session that inherited the in-place executor can snap back.
    await restoreBorrowedInPlaceMain(state.prewalk, pi, context);
    refreshCodePreviewSettings();
    applyFabricMode();
    if (state.shouldEagerlyActivate(context)) await state.ensure(context);
  });

  // Branch changes move the leaf: emitted echoes and spent reminder budget
  // must track it exactly. Rewind removes abandoned-branch residue.
  pi.on("session_tree", async (_event, context) => {
    proxyContract.reset();
    refreshProxyLedger(context);
    // Pi emits session_tree before it clears and rebuilds the transcript:
    // drop card invalidators from abandoned branches so a later display-mode
    // switch only refreshes cards registered by the rebuilt active branch.
    toolDisplay.clear();
    return undefined;
  });

  pi.on("input", async (event, context) => {
    if (!state.initialized) return;
    state.prewalk.observeTask(
      context.sessionManager.getSessionId(),
      event.text,
    );
    await state.publishHostLifecycle("pi.input", event);
  });

  pi.on("agent_start", async (event) => {
    if (state.initialized) await state.publishHostLifecycle("pi.agent_start", event);
  });

  pi.on("agent_end", async (event) => {
    if (state.initialized) await state.publishHostLifecycle("pi.agent_end", event);
  });

  pi.on("turn_end", async (event, context) => {
    // Speculation never crosses a turn boundary; registry.endInvocation already
    // dropped entries for completed fabric_exec runs, this catches turns where
    // the program never executed (type errors, aborts).
    if (state.initialized) state.resetSpeculation();
    if (state.initialized) await state.publishHostLifecycle("pi.turn_end", event);
    // A turn with new action evidence only enqueues the background compiler;
    // the hook returns without scanning session files or waiting on a lock.
    if (entropyEvidenceThisTurn) {
      entropyEvidenceThisTurn = false;
      scheduleEntropyCompile(context);
    }
  });

  pi.on("agent_settled", async (event, context) => {
    inboxWake.settling = true;
    try {
      await settle(event, context);
    } finally {
      inboxWake.settling = false;
    }
  });
  const settle = async (event: unknown, context: ExtensionContext): Promise<void> => {
    // A user's cancel (or a failed run) keeps the idle wake off until the next turn starts.
    inboxWake.context = context;
    inboxWake.armed = settledCompleted(event, context);
    if (!state.initialized) {
      await compactAtConfiguredThreshold(context, state.config);
      return;
    }
    const sessionId = context.sessionManager.getSessionId();
    const settledInPlace = await settleInPlacePrewalk(state.prewalk, pi, context, {
      compactOnReturn: state.config.prewalk.compactOnReturn,
      compact: state.compact,
    });
    if (!settledInPlace && state.prewalk.settleTask(sessionId)) {
      const status = state.prewalk.status();
      context.ui.setStatus(
        "fabric-prewalk",
        status.state === "armed" ? `armed → ${status.model}` : undefined,
      );
    }
    // Drift baselines track armed windows: re-anchor when still armed (a
    // re-arm starts each new window from the just-settled tree state), drop
    // once prewalk is no longer armed for this session.
    if (state.prewalk.status().state === "armed") {
      void state.prewalkDrift.captureBaseline(sessionId, context.cwd);
    } else {
      state.prewalkDrift.drop(sessionId);
    }
    // Keep the completed widget mounted until a newer Fabric run replaces it.
    // Removing rows at settle would pull the editor and latest chat content upward.
    // Pi's compact API is callback-based. Await the controller's Promise here
    // so ExtensionRunner does not finish this handler (and Pi does not publish
    // its public agent_settled event) before compaction settles.
    await state.compact.maybeCommit(context);
    await compactAtConfiguredThreshold(context, state.config);
    await state.publishHostLifecycle("pi.agent_settled", event);
    // A Main whose run completed takes the work events a steer missed as its next turn
    // (smarty-dev#754). An aborted or failed run starts nothing: the batch waits for a turn.
    if (settledCompleted(event, context)) {
      const inbox = await state.nextRootInbox(inboxHeldBy(context)).catch(() => undefined);
      if (inbox?.events.length) pi.sendMessage(rootInboxMessage(inbox.events), { deliverAs: "followUp", triggerTurn: true });
    }
  };


  // Speculative PTC: follow fabric_exec argument streaming and pre-launch
  // literal-argument read calls so their latency hides behind generation.
  pi.on("message_start", () => {
    state.speculationTap?.reset();
  });

  pi.on("message_update", (event, context) => {
    if (!state.initialized) return;
    state.speculationTap?.handleMessageUpdate(event, context);
  });

  pi.on("tool_call", (event, context) =>
    fabricToolLifecycle.toolCall(event, context));

  // smarty-dev#854: a foreground wait over the limit blocks this session from steers and asks.
  // smarty-dev#774: a kill by name pattern kills other owners' processes on a shared host. Every
  // session that loads Fabric (Mains, task agents, actors) runs this, and fabric_exec's pi.bash
  // emits the same tool_call.
  pi.on("tool_call", (event) => {
    if (event.toolName !== "bash") return undefined;
    const { command, timeout } = event.input as { command?: unknown; timeout?: unknown };
    if (typeof command !== "string") return undefined;
    if (killsByPattern(command)) return { block: true, reason: PATTERN_KILL_REASON };
    const reason = foregroundWaitRefusal(command, typeof timeout === "number" ? timeout : undefined);
    return reason ? { block: true, reason } : undefined;
  });

  // Pi 0.80.6 intentionally ignores `isError` returned by custom-tool
  // execute(). Repair the finalized outer result through official middleware.
  pi.on("tool_result", (event) => fabricToolLifecycle.toolResult(event));

  pi.on("tool_result", (event, context) => {
    if (event.toolName !== "read" || event.isError) return undefined;
    let changed = false;
    const content = event.content.map((part) => {
      if (part.type !== "text") return part;
      const text = expandSkillDirMarkersForRead(
        part.text,
        event.input,
        context.cwd,
      );
      if (text === part.text) return part;
      changed = true;
      return { ...part, text };
    });
    return changed ? { content } : undefined;
  });

  pi.on("message_end", (event) => {
    if (event.message.role !== "toolResult") return undefined;
    const message = event.message as AgentToolResultMessage & { usage?: Usage };
    const usage = directToolApproval.takeUsage(message.toolCallId);
    if (!usage) return undefined;
    return {
      message: {
        ...message,
        usage: mergeFabricApprovalUsage(message.usage, usage),
      },
    };
  });

  // message_end runs after all tool-result middleware and tool_execution_end but
  // before Pi persists the native toolResult or starts another model turn. That
  // is the complete outer fabric_exec boundary: fork the exact message, wait for
  // the child, then replace what Main sees while terminate prevents inference.
  pi.on("message_end", async (event, context) => {
    if (event.message.role !== "toolResult") return undefined;
    const pending = pendingHandoffs.get(event.message.toolCallId);
    if (!pending || event.message.toolName !== "fabric_exec") return undefined;
    pendingHandoffs.delete(event.message.toolCallId);

    const outerToolResult = event.message as AgentToolResultMessage;
    const handoff = await state.runHandoffAtBoundary(
      pending,
      outerToolResult,
      context,
    );
    const formatted = formatFabricValue(
      handoff,
      pending.resultFormat,
      state.config.executor.maxOutputChars,
    );
    const output = truncateMiddle(
      formatted.text || "(no output)",
      state.config.executor.maxOutputChars,
    );
    // Directive lands after truncation so it survives maxOutputChars, and
    // gates on "still armed" so one-shot trajectory handoffs stay silent.
    const text = withTrajectoryRearmDirective(
      output,
      pending,
      handoff,
      state.prewalk,
      context.sessionManager.getSessionId(),
    );
    const boundarySucceeded = handoff.completed === true || handoff.continued === true;
    const details =
      typeof event.message.details === "object" &&
      event.message.details !== null &&
      !Array.isArray(event.message.details) &&
      "success" in event.message.details
        ? { ...event.message.details, success: boundarySucceeded }
        : event.message.details;
    // `details` is optional on ToolResultMessage; under exactOptionalPropertyTypes
    // an explicitly `undefined` property is rejected, so omit the key instead.
    return {
      message: {
        ...event.message,
        content: [{ type: "text", text }],
        isError: !boundarySucceeded,
        ...(details === undefined ? {} : { details }),
      },
    };
  });

  pi.on("tool_execution_end", async (event, context) => {
    if (!state.initialized) return;
    if (event.toolName === "fabric_exec") entropyEvidenceThisTurn = true;
    state.noteMainActivity(context);
    if (event.isError) {
      const classified = classifyToolResult({
        toolName: event.toolName,
        isError: true,
        content: event.result,
      });
      const registryObserved =
        event.toolName === "fabric_exec" &&
        (classified?.stage === "invocation_args" ||
          classified?.stage === "invocation_unknown_action");
      if (classified && !registryObserved) {
        getActiveRepairCompiler()?.observe(classified);
      }
      state.dispatchHostEvent("tool_error", event, context);
      await state.publishHostLifecycle("pi.tool_error", event);
    }
  });

  pi.on("session_compact", async (event, context) => {
    if (!state.initialized) return;
    await state.publishHostLifecycle("pi.session_compact", event);
  });

  // Deterministic, LLM-free compaction is registered unconditionally and is
  // active by default. The documented "pi" escape hatch returns early so
  // pi-core's own summarization proceeds normally.
  registerCompactionHook(pi, {
    getEngine: () =>
      state.cwd
        ? state.config.compaction.engine
        : DEFAULT_FABRIC_CONFIG.compaction.engine,
    getTargetContextRatio: () =>
      state.cwd
        ? state.config.compaction.targetContextRatio
        : DEFAULT_FABRIC_CONFIG.compaction.targetContextRatio,
    getThresholdContextRatio: (modelKey) =>
      state.cwd
        ? state.config.compaction.thresholds[modelKey]
        : DEFAULT_FABRIC_CONFIG.compaction.thresholds[modelKey],
    getThresholdTokens: (modelKey) =>
      state.cwd
        ? state.config.compaction.tokenThresholds[modelKey]
        : DEFAULT_FABRIC_CONFIG.compaction.tokenThresholds[modelKey],
  });

  pi.on("context", (event, context) => {
    const sessionId = context.sessionManager.getSessionId();
    const pendingContinuation = state.initialized
      ? state.prewalk.pendingContinuationMessage(sessionId)
      : undefined;
    const continuation = filterPrewalkContinuationMessages(
      event.messages,
      (continuationId) => state.initialized &&
        state.prewalk.acceptContinuation(sessionId, continuationId),
      pendingContinuation,
    );
    // Planning directives are phase-scoped: visible only while this session's
    // arm is live (Main still owes its plan). A claimed handoff or an off arm
    // must not project stale planning instructions into later requests.
    const planning = filterPrewalkPlanningDirectives(
      continuation.messages,
      state.initialized && state.prewalk.isArmed(sessionId),
    );
    let changed = continuation.changed || planning.changed;
    const messages = planning.messages.map((message) => {
      if (message.role !== "user") return message;
      if (typeof message.content === "string") {
        const content = expandSkillDirMarkersInSkillBlock(message.content);
        if (content === message.content) return message;
        changed = true;
        return { ...message, content };
      }
      let messageChanged = false;
      const content = message.content.map((part) => {
        if (part.type !== "text") return part;
        const text = expandSkillDirMarkersInSkillBlock(part.text);
        if (text === part.text) return part;
        changed = true;
        messageChanged = true;
        return { ...part, text };
      });
      return messageChanged ? { ...message, content } : message;
    });
    return changed ? { messages } : undefined;
  });

  pi.on("before_agent_start", async (event, context) => {
    const config = state.bootstrapped ? state.config : DEFAULT_FABRIC_CONFIG;
    const fullCodeMode = config.fullCodeMode;
    const schemaMode = config.schema.mode;
    const effectiveFullCodeMode = fullCodeMode || schemaMode === "enforce";
    if (!pi.getActiveTools().includes("fabric_exec")) return;
    const skills = event.systemPromptOptions.skills ?? [];
    const captureSnapshot = state.bootstrapped ? capturePolicy() : undefined;
    // Pi omits its entire skill catalog when the active tool set lacks a tool
    // named read. Restore Pi's discovered catalog (already bound to one skill
    // tree); full code mode adapts its loader to Fabric's nested pi.read path.
    const systemPrompt = restoreSkillsForFullCodePrompt(event.systemPrompt, skills, effectiveFullCodeMode);
    // Pi expands the invoked skill into the user message, but wrappers may
    // delegate by name. Resolve only explicit invocation lines so full code
    // mode preserves Pi's progressive skill loading without exposing read.
    // Turn-derived: delivered via the message channel (below), never the
    // system prompt, so the cached system prefix stays byte-stable.
    const skillReferenceGuidance = effectiveFullCodeMode
      ? buildSkillReferenceGuidance(event.prompt, skills)
      : undefined;
    const currentModel = context.model
      ? `${context.model.provider}/${context.model.id}`
      : undefined;
    const resolvedGuidance = resolveFabricModelGuidance(state.modelGuidance(), {
      ...(currentModel ? { model: currentModel } : {}),
      target: process.env.PI_FABRIC_PARENT_RUN ? "participant" : "main",
      defaults: [{
        slot: FABRIC_EXECUTION_GUIDANCE_SLOT,
        content: defaultFabricExecutionGuidance(effectiveFullCodeMode, config.executor.kernel, config.executor.pythonRuntime),
      }],
    });
    const overrideGuidance = effectiveFullCodeMode
      ? coreOverridePromptGuidance(capturedTools).trim()
      : undefined;
    const extensionRoster = effectiveFullCodeMode
      ? extensionToolRosterGuidance(capturedTools.list(), new Set(PI_CORE_TOOL_NAMES))
      : undefined;
    // Only turn-stable sections go into the system prompt. Anything derived
    // from the current prompt (skill references) rides
    // the message channel so provider prefix caches never cold-prefill.
    const guidance = [
      fabricExecutionKernelGuidance(effectiveFullCodeMode, config.executor.kernel, config.executor.pythonRuntime),
      resolvedGuidance.slotText,
      fabricSchemaGuidance(schemaMode),
      overrideGuidance,
      extensionRoster,
      resolvedGuidance.appendText,
    ].filter((section): section is string => Boolean(section)).join("\n\n");
    // Turn-varying content (skill reference guidance) is delivered here as a
    // persistent message, not appended to the system prompt. Keeping the
    // system prompt byte-identical across turns is what lets provider prefix
    // caches (e.g. DeepSeek) stay warm.
    if (!skillReferenceGuidance) return {
      systemPrompt: `${systemPrompt}\n\n${guidance}`,
    };
    return {
      systemPrompt: `${systemPrompt}\n\n${guidance}`,
      message: {
        customType: SKILL_REFERENCE_CUSTOM_TYPE,
        content: skillReferenceGuidance,
        display: false,
        details: {},
      },
    };
  });

  // Ambient skill prose that names hidden captured tools is not user intent,
  // so the furnace strips it. This sidecar retargets the call site without
  // spending hint budget, echoing tokens, or burning ash.
  pi.on("before_agent_start", (event) => {
    if (!pi.getActiveTools().includes("fabric_exec")) return;
    const captureSnapshot = state.cwd ? capturePolicy() : undefined;
    if (
      !captureSnapshot?.enabled ||
      !captureSnapshot.hideFromModel ||
      !fabricOwnsModelTools()
    ) {
      return;
    }
    const names = rewritableHiddenCapturedToolNames(hiddenCapturedToolNames());
    if (names.length === 0) return;
    const mentioned = proxyContractMentionsInSkills(
      event.prompt,
      event.systemPrompt,
      names,
    );
    const fresh = proxyContract.take(mentioned);
    if (fresh.length === 0) return;
    return {
      message: {
        customType: PROXY_CONTRACT_CUSTOM_TYPE,
        content: formatProxyContractReminder(fresh),
        display: false,
        details: { names: fresh, origin: "skill" },
      },
    };
  });

  // Work events a steer missed reach the Main with its next turn (smarty-dev#754).
  pi.on("before_agent_start", async (_event, context) => {
    inboxWake.context = context;
    inboxWake.armed = true;
    if (!state.initialized) return;
    const inbox = await state.nextRootInbox(inboxHeldBy(context)).catch(() => undefined);
    if (!inbox?.events.length) return;
    return { message: rootInboxMessage(inbox.events) };
  });

  registerFabricActorHostEventObservers(pi, (eventName, event, context) => {
    if (!state.initialized) return;
    state.dispatchHostEvent(eventName, event, context);
  });

  pi.on("session_shutdown", async (_event, context) => {
    stopInboxWake();
    // Queue the richest final window and let async I/O/cooperative scoring
    // finish before teardown; the TUI event loop remains responsive.
    if (entropyEvidenceThisTurn) {
      entropyEvidenceThisTurn = false;
      scheduleEntropyCompile(context, 0);
    }
    await settleEntropyCompiles();
    entropyLifecycleEpoch += 1;
    entropyCaches = createEntropyCaches();
    entropyCompilePending = undefined;
    unsubscribeComponentRegistration();
    unsubscribeProviderRegistration();
    pendingHandoffs.clear();
    directToolApproval.clear();
    toolDisplay.clear();
    try {
      await state.shutdown();
    } finally {
      uninstallHaltOnEscape();
      uninstallShellHangKeys();
      fabricUi.stop();
      suspendToolCapture();
      toolOwnership.release();
      fabricToolLifecycle.clear();
      toolCapture.dispose();
    }
  });

  // Turn-scoped invariant: even if another extension rewrote the active tool
  // set (e.g. a permission system filtering its allowlist at before_agent_start,
  // or a refresh that ran before Fabric's policy was active), captured tools
  // must not leak into the model's next turn.
  pi.on("before_agent_start", () => {
    reassertToolOwnership();
  });

  registerFabricCommand(pi, {
    state,
    fabricUi,
    capturedTools,
    applyFabricMode,
    suspendToolCapture,
    refreshCodePreviewSettings,
    refreshToolDisplay: () => toolDisplay.refresh(),
  });
}

export * from "./audit/index.js";
export * from "./entropy/index.js";
export * from "./protocol.js";
