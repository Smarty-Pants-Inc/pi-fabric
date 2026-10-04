import type { Usage } from "@earendil-works/pi-ai";
import type { RootInboxBatch } from "./topology/root-inbox.js";
import { registerFabricPrincipalCapture, fabricHostIdentity, fabricProvenanceSupported, sendFabricMessage } from "./fabric-provenance.js";
import { actorBashTimeout } from "./guards/actor-bash-timeout.js";
import { registerFabricFixture } from "./guards/fixture-mode.js";
import { registerJevAuth } from "./jev/auth.js";
import { claimFabricRegistration, yieldsToExplicitFabric } from "./core/explicit-fabric.js";
import type {
  ExtensionAPI,
  ExtensionContext,
  MessageUpdateEvent,
} from "@earendil-works/pi-coding-agent";
import { defaultCodePreviewSettings } from "./ui/code-preview.js";
import {
  type FabricToolShellDecorator,
  withCodePreviewShell,
} from "./ui/code-preview-shell.js";
import { registerFabricActorHostEventObservers } from "./actors/host-event-observer.js";
import { CapturedToolCatalog } from "./capture/catalog.js";
import { isSelectedNativeMcpTool, nativeMcpIdentity } from "./core/native-mcp-identity.js";
import { installRegisteredToolCapture } from "./capture/interceptor.js";
import { registerFabricCommand } from "./commands/fabric.js";
import { resolveAgentDir } from "./core/agent-dir.js";
import { FileLockTimeoutError } from "./core/file-lock.js";
import { setActiveCompiledSurface } from "./entropy/active-state.js";
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
import { registerLazyCompactionHook } from "./compaction/lazy-hook.js";
import { COMPACTION_FAILED_ALARM, registerCompactionRecovery } from "./compaction/recovery.js";
import { compactAtConfiguredThreshold, type AutoCompactionTrigger } from "./compaction/threshold.js";
import type { CompactionOwnerObserver } from "./compaction/owner.js";
import { unregisteredRunnerNotice } from "./agents/runner-notice.js";
import {
  createToolOwnershipReassertion,
  fabricModelContext,
  FabricToolLifecycle,
  FabricToolOwnership,
  fabricToolPlacement,
  ownsFabricToolSource,
} from "./core/tool-ownership.js";
import { readChildToolAllowlist } from "./core/child-tool-allowlist.js";
import { formatForeground } from "./core/foreground-tools.js";
import {
  expandSkillDirMarkersForRead,
  expandSkillDirMarkersInSkillBlock,
} from "./core/skill-dir.js";
import { coreOverridePromptGuidance } from "./core/core-override-guidance.js";
import { PI_CORE_TOOL_NAMES, PI_CORE_TOOL_NAME_SET } from "./core/pi-tools.js";
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
import { FABRIC_THINKING_BOUNDS_ENV } from "./thinking.js";
import { classifyToolResult } from "./repairs/classify.js";
import { getActiveRepairCompiler } from "./repairs/active.js";
import { piHostCompatibilityWarning } from "./host-compatibility.js";
import {
  FABRIC_COMPONENT_REGISTER_EVENT,
  FABRIC_PROGRAM_RUN_EVENT,
  FABRIC_PROVIDER_REGISTER_EVENT,
  FABRIC_PROVIDER_WITHDRAW_EVENT,
  FABRIC_TOOL_PLACEMENT_EVENT,
  readFabricProviderWithdrawalV1,
  readFabricToolPlacementRequestV1,
  type FabricComponentRegistration,
  type FabricProviderRegistration,
  type FabricToolPlacementMode,
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
import { writeSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { captureLoadedFileIdentity } from "./build-identity.js";
import { ownsRunReplyTool, REPLY_TOOL_NAME } from "./core/reply-tool-identity.js";
import { readStoppedRuns, takeReloadStoppedNotice } from "./agents/stopped-runs.js";
import { installSelfReload, reloadTargetUiHold, RELOAD_HELD_TOPIC, RELOADED_TOPIC, SELF_RELOAD_STATUS } from "./lifecycle/self-reload.js";


export const FABRIC_MANAGED_HOST_VERSION = 1;
export type { FabricManagedHostOptions } from "./managed-host.js";
import type { FabricManagedHostOptions } from "./managed-host.js";

export function createFabricExtension(entryUrl: string) {
// Absolute path to the Fabric skills bundled with this extension. Resolved
// relative to the extension entry so it works both in development (src/) and
// in an installed package (dist/). Contributed via resources_discover so child
// Pi processes that load Fabric with -e (agents and actors) discover the
// same kernel-specific tree as Main. The package manifest exposes no skills;
// selecting exactly one tree avoids canonical-name collisions.
const FABRIC_EXTENSION_ENTRY_PATH = path.resolve(fileURLToPath(entryUrl));
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
const FABRIC_ENTRY_IDENTITY = captureLoadedFileIdentity(entryUrl);

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


// Newer Pi names the terminal outcome; older hosts expose the last assistant's stop reason.
// Keep failure separate from owner cancellation: future mailbox input may wake an errored Main.
const settledOutcome = (event: unknown, context: ExtensionContext): string => {
  const outcome = (event as { outcome?: unknown }).outcome;
  if (typeof outcome === "string") return outcome;
  if (context.signal?.aborted) return "aborted";
  const entries = context.sessionManager.getEntries();
  for (let index = entries.length - 1; index >= Math.max(0, entries.length - 50); index--) {
    const entry = entries[index] as { type?: string; message?: { role?: string; stopReason?: string } };
    if (entry.type === "message" && entry.message?.role === "assistant") {
      return entry.message.stopReason === "aborted" || entry.message.stopReason === "error"
        ? entry.message.stopReason : "completed";
    }
  }
  return "completed";
};

const settledCompleted = (event: unknown, context: ExtensionContext): boolean =>
  settledOutcome(event, context) === "completed";

// Whether the session already holds an inbox batch: its cursor moves only then (smarty-dev#754).
const inboxHeldBy = async (context: ExtensionContext) =>
  (await import("./topology/root-inbox.js")).confirmedRootInboxSession(context.sessionManager);
const deliverRootInbox = async (...args: Parameters<typeof import("./topology/root-inbox-delivery.js")["deliverRootInbox"]>): Promise<void> =>
  (await import("./topology/root-inbox-delivery.js")).deliverRootInbox(...args);

/** Expiry is observational: one line, never a triggered continuation. */
const reportInboxExpiry = async (pi: ExtensionAPI, inbox: RootInboxBatch | undefined): Promise<void> => {
  if (!inbox?.skippedStale) return;
  const { rootInboxSummary } = await import("./topology/root-inbox.js");
  sendFabricMessage(pi, rootInboxSummary(inbox), { deliverAs: "followUp", triggerTurn: false });
};

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

return async function piFabric(pi: ExtensionAPI, options: { managedHost?: FabricManagedHostOptions } = {}): Promise<void> {
  // A fixture must never construct Fabric state, capture auth or join an inherited mailbox.
  if (registerFabricFixture(pi)) return;
  // A different Fabric requested explicitly with -e (a worker's parent Fabric) wins over
  // this discovered copy; registering both makes Pi refuse to start (fabric_exec conflict).
  if (!options.managedHost && yieldsToExplicitFabric(FABRIC_EXTENSION_ENTRY_PATH)) return;
  if (!options.managedHost && !claimFabricRegistration(FABRIC_EXTENSION_ENTRY_PATH, pi)) return;
  registerFabricPrincipalCapture(pi);
  if (!options.managedHost) registerJevAuth(pi);
  const codePreviewSettings = defaultCodePreviewSettings();
  const decorateShell: FabricToolShellDecorator = withCodePreviewShell;
  let compatibilityWarningShown = false;
  // Host scope issuance is read once here; its parser loads only when present (src/scope.ts).
  const scopeEnv = { json: process.env.PI_FABRIC_SCOPE, file: process.env.PI_FABRIC_SCOPE_FILE };
  const sealScope = async (): Promise<void> => {
    const issued = ((globalThis as Record<symbol, { sealed?: boolean; error?: string } | undefined>)[
      Symbol.for("pi-fabric:scope:v1")] ??= {});
    if (issued.sealed) return;
    if (!scopeEnv.json && !scopeEnv.file) {
      issued.sealed = true;
      return;
    }
    try {
      (await import("./scope.js")).sealRootScope(scopeEnv);
    } catch (error) {
      issued.sealed = true;
      issued.error ??= error instanceof Error ? error.message : String(error);
    }
  };
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
    undefined,
    undefined,
    (toolName, config) => {
      const definition = pi.getAllTools().find((tool) => tool.name === toolName);
      const namespace = definition?.namespace?.name;
      const selected = namespace?.startsWith("mcp__") && (config.mcp.nativeServers ?? []).includes(namespace.slice(5));
      if (!selected) return undefined;
      const registered = capturedTools.registeredTools().find((entry) => entry.definition.name === toolName);
      const identity = nativeMcpIdentity(registered?.definition);
      if (!identity) throw new Error(`Unsupported Pi MCP identity metadata for ${toolName}; refusing native dispatch`);
      return identity;
    },
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
  const modelDeclaredTools = (): readonly string[] => [
    ...state.foregroundTools().tools,
    // Only the exact worker reply hook can bypass full-code/enforce hiding.
    ...(ownsRunReplyTool(pi.getAllTools()) ? [REPLY_TOOL_NAME] : []),
  ];
  // Legacy capture preferences still describe the catalog, but native loadout
  // hiding is unconditional in full-code/enforce mode, including keepVisible.
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
        return fabricOwnsModelTools();
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

  // Direct registrations only: component-owned providers withdraw through
  // their component lease. Unknown names and generation mismatches are no-ops.
  const unsubscribeProviderWithdrawal = pi.events.on(
    FABRIC_PROVIDER_WITHDRAW_EVENT,
    (value: unknown) => {
      const withdrawal = readFabricProviderWithdrawalV1(value);
      if (!withdrawal) throw new Error("Invalid Pi Fabric provider withdrawal");
      if (!state.withdrawExternal(withdrawal.name, withdrawal.generation) && process.env.PI_FABRIC_DEBUG) {
        console.debug(`[pi-fabric] ignored provider withdrawal: ${withdrawal.name}`);
      }
    },
  );

  // Synchronous reachability answer so extensions stop guessing whether the
  // model or a fabric_exec program can reach a tool this turn.
  const programReachable = (registered: ReadonlySet<string>): ((name: string) => boolean) => {
    if (!state.initialized) return () => false;
    const allowlist = readChildToolAllowlist();
    const registry = state.registry;
    return (name) => {
      if (allowlist && !allowlist.has(name)) return false;
      const captured = capturedTools.get(name) !== undefined;
      if (PI_CORE_TOOL_NAME_SET.has(name)) {
        if (!registry.has("pi")) return false;
        if (options.managedHost) return captured;
        return name !== "powershell" || captured || registered.has(name);
      }
      return captured && registry.has("extensions");
    };
  };
  const unsubscribeToolPlacement = pi.events.on(
    FABRIC_TOOL_PLACEMENT_EVENT,
    (value: unknown) => {
      const request = readFabricToolPlacementRequestV1(value);
      if (!request) throw new Error("Invalid Pi Fabric tool placement query");
      const config = state.bootstrapped ? state.config : DEFAULT_FABRIC_CONFIG;
      const mode: FabricToolPlacementMode = config.schema.mode === "enforce"
        ? "enforce"
        : config.fullCodeMode ? "full-code" : "orchestration";
      const registered = pi.getAllTools().map((tool) => tool.name);
      request.reply(fabricToolPlacement({
        mode,
        registered,
        active: pi.getActiveTools(),
        program: programReachable(new Set(registered)),
        foreground: modelDeclaredTools(),
        ...(request.tools ? { tools: request.tools } : {}),
      }));
    },
  );

  // Host program runs (daemons, embedders) use the live session's context.
  let programRunContext: ExtensionContext | undefined;
  const unsubscribeProgramRun = pi.events.on(FABRIC_PROGRAM_RUN_EVENT, (value: unknown) => {
    const reply = (value as { reply?: unknown } | null)?.reply;
    if (typeof reply !== "function") throw new Error("Invalid Pi Fabric program run request");
    void import("./programs/host.js").then(
      ({ handleFabricProgramRunEvent }) => handleFabricProgramRunEvent(value, { state, pi, context: programRunContext }),
      (error: unknown) => reply({ ok: false, error: `Fabric program host unavailable: ${String(error)}` }),
    );
  });

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
      // Called only for a recognized lone Escape: latch it even when nothing was left to halt.
      halted: () => { escapeLatched = true; state.haltMain(); return state.advisorsHalted; },
      halt: () => state.haltAdvisors(),
    });
  };
  // The user's Escape stop-the-world, held for the self-reload gate independently of the actor
  // and Jev halts and of any settle outcome; only a non-extension input lifts it (pi-fabric#160).
  let escapeLatched = false;
  pi.on("input", (event) => {
    if ((event as { source?: string }).source !== "extension") escapeLatched = false;
  });
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
  }, cleanupActivationSideEffects, cleanupActivationSideEffects);

  // Continual entropy reduction runs off the interaction path. Session-tree
  // discovery and JSONL ingestion use async I/O, scoring yields in fixed trace
  // chunks, and durable stores acquire locks cooperatively. Turn hooks only
  // enqueue work; a pending turn coalesces to the newest context.
  let entropyEvidenceThisTurn = false;
  let entropyCompileInFlight: Promise<void> | undefined;
  let entropyCompilePending: EntropyCompileRequest | undefined;
  let entropyLifecycleEpoch = 0;
  // Aborted when the lifecycle epoch ends: reads, stats and scoring stop at once (smarty-dev#2010).
  let entropyAbort = new AbortController();
  let entropyStopping = false;
  let entropyRetryTimer: ReturnType<typeof setTimeout> | undefined;
  let entropyRetryDelayMs = 1_000;
  const clearEntropyRetry = (reset = true): void => {
    if (entropyRetryTimer) clearTimeout(entropyRetryTimer);
    entropyRetryTimer = undefined;
    if (reset) entropyRetryDelayMs = 1_000;
  };
  const endEntropyLifecycle = (): void => {
    entropyLifecycleEpoch += 1;
    entropyAbort.abort();
    entropyAbort = new AbortController();
    entropyCaches = undefined;
    entropyEvidenceThisTurn = false;
    entropyCompilePending = undefined;
  };
  const createEntropyCaches = (entropy: typeof import("./entropy/index.js")) => ({
    compiler: new entropy.BackgroundEntropyCompiler(),
    observations: new entropy.SessionObservationCache(),
  });
  let entropyCaches: ReturnType<typeof createEntropyCaches> | undefined;

  interface EntropyCompileRequest {
    context: ExtensionContext;
    delayMs: number;
    epoch: number;
    signal: AbortSignal;
  }

  const compileEntropyNow = async (
    context: ExtensionContext,
    epoch: number,
    signal: AbortSignal,
  ): Promise<boolean> => {
    const current = (): boolean =>
      !signal.aborted && epoch === entropyLifecycleEpoch && state.initialized &&
      state.config.entropy.compile;
    if (!current()) return false;
    const entropy = await import("./entropy/index.js");
    if (!current()) return false;
    const { comparableCompiledSurfaceScore, entropyRepairRows, formatEntropyCompileNotice,
      liveSurfaceSnapshot, loadCompiledSurfaceAsync, saveCompiledSurfaceAsync,
      updateObservationPoolAsync, sessionWindowEvidenceAsync } = entropy;
    let retry = false;
    const agentDir = resolveAgentDir();
    const cwd = state.cwd ?? context.cwd;
    const repairs = entropyRepairRows(state.repairs.repairs);
    const caches = entropyCaches ??= createEntropyCaches(entropy);
    // Only this session's append cursor; never scan other agents' large session files.
    const sessionFile = context.sessionManager.getSessionFile?.();
    const files = sessionFile ? [sessionFile] : [];
    const [loaded, snapshot] = await Promise.all([
      loadCompiledSurfaceAsync(agentDir),
      liveSurfaceSnapshot({ registry: state.registry, extensionContext: context, cwd }),
    ]);
    if (!current() || loaded.error) return false;
    const evidence = await sessionWindowEvidenceAsync(files, { windowsOnly: true, signal });
    if (!current()) return false;
    try {
      // Upstream rebases under the pool lock. Carry our abort fence into both
      // merges, including the merge after lock admission, before any pool write.
      const observations = new class extends entropy.SessionObservationCache {
        override async merge(...args: Parameters<typeof caches.observations.merge>) {
          signal.throwIfAborted();
          const merged = await caches.observations.merge(args[0], args[1], signal);
          signal.throwIfAborted();
          return merged;
        }
      }();
      // Avoid a write/lock attempt for unchanged evidence as in the fork, but
      // rebase every actual update under upstream's cross-process pool lock.
      const pooled = await entropy.loadObservationPoolAsync(agentDir);
      if (pooled.error) throw new Error(pooled.error);
      const planned = await observations.merge(pooled.file, evidence.observationWindows, signal);
      if (planned.mergedSessions > 0 || !pooled.file) {
        await updateObservationPoolAsync(agentDir, evidence.observationWindows, observations);
      }
    } catch (error) {
      if (signal.aborted) return false;
      if (error instanceof FileLockTimeoutError) {
        retry = true;
      } else {
        // Advisory evidence cannot gate the normal-form compiler.
        console.warn(`[pi-fabric] observation pool update failed: ${error instanceof Error ? error.message : String(error)}`);
      }
    }
    if (!current()) return false;
    const outcome = await caches.compiler.compile({
      windows: evidence.traceWindows,
      surface: snapshot,
      repairs,
      ...(loaded.file ? { artifact: loaded.file } : {}),
      signal,
    });
    if (!current()) return false;
    if (outcome.status === "compiled" && outcome.artifact) {
      try {
        const saved = await saveCompiledSurfaceAsync(agentDir, outcome.artifact);
        if (current()) {
          // Activate even when another process already persisted identical bytes.
          setActiveCompiledSurface(saved.file);
          const previousScore = comparableCompiledSurfaceScore(loaded.file, saved.file.metricVersion);
          if (previousScore !== undefined && outcome.report.score < previousScore && context.hasUI) {
            context.ui.notify(formatEntropyCompileNotice({
              beforeScore: previousScore,
              afterScore: outcome.report.score,
              normalizations: saved.file.normalizations?.length ?? 0,
            }), "info");
          }
        }
      } catch (error) {
        if (!(error instanceof FileLockTimeoutError)) throw error;
        // Proof-checked plans are usable now; durable persistence can catch up
        // later without blocking this session on another process's writer.
        if (current()) setActiveCompiledSurface(outcome.artifact);
        retry = true;
      }
    }
    return retry && current();
  };

  const launchEntropyCompile = (request: EntropyCompileRequest): void => {
    let retry = false;
    const task = (async () => {
      try {
        if (request.delayMs > 0) {
          await new Promise<void>((resolve) => {
            const finish = (): void => {
              clearTimeout(timer);
              request.signal.removeEventListener("abort", finish);
              resolve();
            };
            const timer = setTimeout(finish, request.delayMs);
            timer.unref();
            request.signal.addEventListener("abort", finish, { once: true });
            if (request.signal.aborted) finish();
          });
        }
        retry = await compileEntropyNow(request.context, request.epoch, request.signal);
      } catch (error) {
        if (request.signal.aborted) return;
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
      if (!retry && request.epoch === entropyLifecycleEpoch) entropyRetryDelayMs = 1_000;
      if (pending) {
        launchEntropyCompile(pending);
      } else if (retry && !entropyStopping && request.epoch === entropyLifecycleEpoch) {
        // One unref'ed timer, not an in-flight sleep: idle retries never hold
        // shutdown open. Equal jitter prevents sibling Pi processes retrying
        // in lockstep, and the delay caps at 30 seconds without a tight loop.
        const delay = entropyRetryDelayMs * (0.5 + Math.random() * 0.5);
        entropyRetryDelayMs = Math.min(30_000, entropyRetryDelayMs * 2);
        entropyRetryTimer = setTimeout(() => {
          entropyRetryTimer = undefined;
          launchEntropyCompile({ ...request, delayMs: 0 });
        }, delay);
        entropyRetryTimer.unref();
      }
    });
  };

  const scheduleEntropyCompile = (
    context: ExtensionContext,
    delayMs = 250,
  ): void => {
    clearEntropyRetry(false);
    const request = { context, delayMs, epoch: entropyLifecycleEpoch, signal: entropyAbort.signal };
    if (entropyCompileInFlight) {
      entropyCompilePending = request;
      return;
    }
    launchEntropyCompile(request);
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
  const hostSettling = (context: ExtensionContext): boolean =>
    (context as { isSettling?: () => boolean }).isSettling?.() ?? false;
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
    // The host's whole settle counts, not only Fabric's handler (#111 review F4): a turn requested
    // then is deferred past every agent_settled handler, where neither the transcript nor
    // hasPendingMessages() shows it, and an earlier handler may still precede Fabric's disarm.
    const idle = () => inboxWake.context === context && inboxWake.armed && !inboxWake.settling &&
      context.isIdle() && !hostSettling(context) && !promptPending(context) && !context.hasPendingMessages();
    inboxWake.reading = true;
    try {
      if (!idle()) return;
      const inbox = await state.nextRootInbox(await inboxHeldBy(context), idle);
      await reportInboxExpiry(pi, inbox);
      // A turn that started meanwhile takes the pending batch at its own start: never a second run.
      if (inbox?.events.length && idle()) {
        await deliverRootInbox(pi, inbox.events);
        return;
      }
      // Records: the same gate, re-checked after the read (F21).
      if (!idle()) return;
      const records = await state.nextRecordsInboxMessage(context.sessionManager.getEntries()).catch(() => undefined);
      if (records && idle()) sendFabricMessage(pi, records, { deliverAs: "followUp", triggerTurn: true });
    } catch {
      // A stale context (reload, session replacement) or a mesh error: the next tick or turn retries.
    } finally {
      inboxWake.reading = false;
    }
  };

  pi.on("session_start", async (event, context) => {
    await sealScope();
    clearEntropyRetry();
    entropyStopping = false;
    fabricPrewarm = undefined;
    stopInboxWake();
    // The pre-reload warning leaves with the redraw; say it again where the user can read it (smarty-dev#1882).
    const reloadNotice = takeReloadStoppedNotice(context.sessionManager?.getSessionId?.() ?? "", event?.reason ?? "");
    if (reloadNotice && context.hasUI) context.ui.notify(reloadNotice, "warning");
    inboxWake.context = context;
    inboxWake.armed = true;
    endEntropyLifecycle();
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
    state.thinking.invalidate();
    programRunContext = context;
    await state.bootstrap(context);
    // Inert until Pi queues a triggered message behind a live prompt preflight (also for records, F21).
    state.setRecordsWake(hostQueuesTriggeredBehindPreflight(pi) ? () => wakeIdleMain() : undefined);
    if (hostQueuesTriggeredBehindPreflight(pi)) {
      inboxWake.timer = setInterval(() => void wakeIdleMain(), inboxWakeMs());
      inboxWake.timer.unref?.();
    }
    // A Fabric child narrows its level into the parent's inherited bounds.
    if (process.env[FABRIC_THINKING_BOUNDS_ENV] !== undefined && state.bootstrapped) {
      try {
        await state.thinking.enforceBounds(context);
      } catch (error) {
        console.warn(`[pi-fabric] thinking bounds: ${error instanceof Error ? error.message : String(error)}`);
      }
    }
    // bootstrap() cancels any live arm; the borrowed Main model survives so a
    // new session that inherited the in-place executor can snap back.
    await restoreBorrowedInPlaceMain(state.prewalk, pi, context, () => state.config.agents);
    refreshCodePreviewSettings();
    applyFabricMode();
    // Results of task agents the last reload/shutdown stopped reach the spawner now (smarty-dev#1602).
    const stoppedUndelivered = readStoppedRuns(context.sessionManager?.getEntries?.() ?? []).undelivered.length > 0;
    // A self-reload (smarty-dev#2160) re-arms the actors this Main hosts and reports on the mesh.
    const selfReloaded = selfReload.sessionStart(event?.reason ?? "", context);
    const { releaseSlot, ...reloadReport } = selfReloaded ?? {};
    try {
      if (selfReloaded && context.hasUI) {
        const notice = `${selfReloaded.owner ?? "Fabric"} reloaded: ${selfReloaded.old} → ${selfReloaded.new}`;
        context.ui.notify(notice, "info");
        // Keep the notice after the TUI's own reload status line replaces it.
        context.ui.setStatus(SELF_RELOAD_STATUS, notice);
      }
      const probeNonce = process.env.PI_FABRIC_RESIDENT_PROBE_NONCE;
      const residentProbe = Boolean(probeNonce && process.env.PI_FABRIC_RESIDENT_PROBE_WORKER_PID === String(process.ppid));
      if (residentProbe && (context.mode !== "rpc" || !process.env.PI_FABRIC_PARENT_RUN)) {
        throw new Error("resident startup probe requires a bound RPC worker");
      }
      if (residentProbe || stoppedUndelivered || selfReloaded || state.shouldEagerlyActivate(context)) await state.ensure(context);
      if (residentProbe) {
        // Positive native ACK is published only after this generation activated.
        writeSync(1, `${JSON.stringify({ type: "fabric_resident_extension_ready", protocol: 1,
          runId: process.env.PI_FABRIC_PARENT_RUN, nonce: probeNonce, extension: FABRIC_EXTENSION_ENTRY_PATH })}\n`);
      }
      if (selfReloaded) {
        await state.publishOpsEvent(RELOADED_TOPIC, "fabric.reloaded", {
          ...reloadReport,
          sessionId: context.sessionManager.getSessionId(),
        });
      }
    } finally {
      // Hold the lease through activation, actor re-arm and reporting, even on failure.
      releaseSlot?.();
    }
  });

  // Branch changes move the leaf: emitted echoes and spent reminder budget
  // must track it exactly. Rewind removes abandoned-branch residue.
  pi.on("session_tree", async (_event, context) => {
    state.thinking.invalidate();
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

  pi.on("agent_end", async (event, context) => {
    try {
      // Turn-scoped thinking overrides revert here, even before activation.
      await state.thinking.agentEnded(context);
    } catch (error) {
      console.warn(`[pi-fabric] thinking revert failed: ${error instanceof Error ? error.message : String(error)}`);
    }
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
    // Only an owner cancel disarms future mailbox work. A provider error must not leave
    // addressed followUps waiting forever for a boundary that will never come (#4012).
    // This arms the existing idle reader, not a retry: no pending work means no new turn.
    // Error settlement itself still does not drain below; the inbox's grace/cooldown apply.
    inboxWake.context = context;
    inboxWake.armed = !context.signal?.aborted &&
      ["completed", "error"].includes(settledOutcome(event, context));
    if (!state.initialized) {
      await compactAtConfiguredThreshold(context, state.config);
      return;
    }
    const sessionId = context.sessionManager.getSessionId();
    const settledInPlace = await settleInPlacePrewalk(state.prewalk, pi, context, {
      policy: () => state.config.agents,
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
    await compactAtConfiguredThreshold(
      context,
      state.config,
      (trigger: AutoCompactionTrigger, committed: boolean) =>
        state.compact.noteAutoCompaction(trigger, committed),
    );
    await state.publishHostLifecycle("pi.agent_settled", event);
    // A Main whose run completed takes the work events a steer missed as its next turn
    // (smarty-dev#754). An aborted run waits for owner input; a failed run leaves its
    // mailbox to the idle reader above, rather than retrying at the error boundary.
    if (settledCompleted(event, context)) {
      const inbox = await state.nextRootInbox(await inboxHeldBy(context)).catch(() => undefined);
      await reportInboxExpiry(pi, inbox);
      if (inbox?.events.length) await deliverRootInbox(pi, inbox.events);
      // Records addressed to this root past its processing cursor (smarty-dev#754 C4), same hook.
      const records = await state.nextRecordsInboxMessage(context.sessionManager.getEntries()).catch(() => undefined);
      if (records) sendFabricMessage(pi, records, { deliverAs: "followUp", triggerTurn: true });
    }
  };


  // The first fabric_exec of a session paid Fabric's initialization and the TypeScript checker
  // (5-10 s on a loaded host) after the model finished streaming it. Start both when the model
  // starts streaming that call, the first sign of actual use (smarty-dev#2010).
  let fabricPrewarm: Promise<void> | undefined;
  const prewarmOnFabricExecStream = (event: MessageUpdateEvent, context: ExtensionContext): void => {
    if (fabricPrewarm) return;
    const update = event.assistantMessageEvent;
    if (update.type !== "toolcall_start" && update.type !== "toolcall_delta") return;
    const block = update.partial?.content?.[update.contentIndex];
    if (block?.type !== "toolCall" || block.name !== "fabric_exec") return;
    fabricPrewarm = (async () => {
      await state.ensure(context);
      await state.execution.prewarm(context);
    })().catch(() => undefined);
  };

  // Speculative PTC: follow fabric_exec argument streaming and pre-launch
  // literal-argument read calls so their latency hides behind generation.
  pi.on("message_start", () => {
    state.speculationTap?.reset();
  });

  pi.on("message_update", (event, context) => {
    prewarmOnFabricExecStream(event, context);
    if (!state.initialized) return;
    state.speculationTap?.handleMessageUpdate(event, context);
  });

  pi.on("tool_call", (event, context) =>
    fabricToolLifecycle.toolCall(event, context));

  // smarty-dev#854: a foreground wait over the limit blocks this session from steers and asks.
  // smarty-dev#774: a kill by name pattern kills other owners' processes on a shared host. Every
  // session that loads Fabric (Mains, task agents, actors) runs this, and fabric_exec's pi.bash
  // emits the same tool_call.
  let shellGuards: Promise<[typeof import("./core/pattern-kill.js"), typeof import("./guards/foreground-wait.js")]> | undefined;
  pi.on("tool_call", async (event) => {
    if (event.toolName !== "bash") return undefined;
    const { command, timeout } = event.input as { command?: unknown; timeout?: unknown };
    if (typeof command !== "string") return undefined;
    const [{ killsByPattern, PATTERN_KILL_REASON, TMP_WIPE_REASON, wipesTmp }, { foregroundWaitRefusal }] =
      await (shellGuards ??= Promise.all([import("./core/pattern-kill.js"), import("./guards/foreground-wait.js")]));
    if (killsByPattern(command)) return { block: true, reason: PATTERN_KILL_REASON };
    if (wipesTmp(command)) return { block: true, reason: TMP_WIPE_REASON };
    const reason = foregroundWaitRefusal(command, typeof timeout === "number" ? timeout : undefined);
    if (reason) return { block: true, reason };
    // smarty-dev#2184: judged on the caller's own timeout above, so the injected default never unblocks a wait.
    const injected = actorBashTimeout(process.env, timeout);
    if (injected !== undefined) (event.input as { timeout?: number }).timeout = injected;
    return undefined;
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
    const outputArtifactWriter = state.outputArtifactWriter;
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
    const text = output;
    const executionOutcome = pending.executionOutcome;
    const boundarySucceeded = (handoff.completed === true || handoff.continued === true) &&
      executionOutcome?.success !== false && !executionOutcome?.residentOutcomes.length && !outerToolResult.isError;
    const { boundModelOutput, formatResidentOutcomePriority, modelOutputBudget } = await import("./output-budget.js");
    const residentPriority = executionOutcome?.residentOutcomes.length
      ? formatResidentOutcomePriority(executionOutcome.residentOutcomes)
      : undefined;
    const sections = [
      ...(executionOutcome?.error ? [`Original execution failed: ${executionOutcome.error}`] : []),
      text,
    ];
    const fullOutput = [...(residentPriority ? [residentPriority] : []), ...sections].join("\n\n");
    // Apply the same non-truncating receipt priority as execute(), at the final
    // persisted/model-visible boundary. Transition success cannot cure an
    // execution failure or authorize retrying its committed resident work.
    const protectedOutput = await boundModelOutput(
      fullOutput,
      modelOutputBudget(state.config.executor.maxOutputChars, boundarySucceeded),
      fullOutput,
      outputArtifactWriter,
      residentPriority ? { text: residentPriority, sections } : undefined,
    );
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
        content: [{ type: "text", text: withTrajectoryRearmDirective(
          protectedOutput.text, pending, handoff, state.prewalk, context.sessionManager.getSessionId(),
        ) }],
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

  // Ownership is observed, never contested: the committed entry says who
  // produced it, and a foreign owner under the Fabric engine earns one
  // load-order explanation per session.
  // The observer loads at the first committed compaction; a deliberate yield
  // noted before then is handed over when it does.
  let compactionOwners: Promise<CompactionOwnerObserver> | undefined;
  let compactionYieldPending = false;
  pi.on("session_compact", async (event, context) => {
    const fabricEngine = (state.bootstrapped ? state.config : DEFAULT_FABRIC_CONFIG).compaction.engine === "fabric";
    const observer = await (compactionOwners ??= import("./compaction/owner.js").then(
      (module) => new module.CompactionOwnerObserver(),
    ));
    if (compactionYieldPending) {
      compactionYieldPending = false;
      observer.noteDeliberateYield();
    }
    const { warning } = observer.observe(
      context.sessionManager.getSessionId(),
      event.compactionEntry,
      fabricEngine,
    );
    if (warning) {
      if (context.hasUI) context.ui.notify(warning, "warning");
      else console.warn(`[pi-fabric] ${warning}`);
    }
    if (!state.initialized) return;
    await state.publishHostLifecycle("pi.session_compact", event);
  });

  registerCompactionRecovery(pi, {
    enabled: () => true, // Leaf task agents may not have needed a Fabric tool yet.
    alarm: async (data, context) => {
      await state.ensure(context); // Failure is first use, never an eager idle import.
      await state.publishOpsEvent(COMPACTION_FAILED_ALARM, COMPACTION_FAILED_ALARM, data);
    },
  });

  // Deterministic, LLM-free compaction is registered unconditionally and is
  // active by default. The documented "pi" escape hatch returns early so
  // pi-core's own summarization proceeds normally.
  registerLazyCompactionHook(pi, {
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
    getOutputReserveTokens: () =>
      state.bootstrapped
        ? state.config.compaction.outputReserveTokens
        : DEFAULT_FABRIC_CONFIG.compaction.outputReserveTokens,
    onYield: () => {
      compactionYieldPending = true;
    },
  });

  pi.on("context", async (event, context) => {
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
    // Retire requests to plan as soon as the plan is recorded, not just when
    // handoff claims the arm. Ungated arm advisories remain while armed.
    const planning = filterPrewalkPlanningDirectives(
      continuation.messages,
      state.initialized && state.prewalk.isArmed(sessionId),
      state.initialized && state.prewalk.planRequired(sessionId),
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
    const repairOrphans = (state.bootstrapped ? state.config : DEFAULT_FABRIC_CONFIG).compaction.repairOrphans;
    if (!repairOrphans) return changed ? { messages } : undefined;
    // Identity-preserving: a defect-free list comes back unchanged.
    const { repairToolResultPairing } = await import("./compaction/orphan-repair.js");
    const repaired = repairToolResultPairing(messages);
    if (repaired.messages !== messages) return { messages: repaired.messages };
    return changed ? { messages } : undefined;
  });

  pi.on("before_agent_start", async (event, context) => {
    const config = state.provisionalConfig();
    const fullCodeMode = config.fullCodeMode;
    const schemaMode = config.schema.mode;
    const effectiveFullCodeMode = fullCodeMode || schemaMode === "enforce";
    if (!pi.getActiveTools().includes("fabric_exec")) return;
    const skills = event.systemPromptOptions.skills ?? [];
    const captureSnapshot = state.bootstrapped ? capturePolicy() : undefined;
    // Pi omits its entire skill catalog when the active tool set lacks a tool
    // named read. Restore Pi's discovered catalog (already bound to one skill
    // tree); full code mode adapts its loader to Fabric's nested pi.read path.
    if (skills.length) {
      event.systemPromptOptions.sections.skills = restoreSkillsForFullCodePrompt("", skills, effectiveFullCodeMode).trim();
    }
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
      ? extensionToolRosterGuidance(capturedTools.list().filter(entry =>
          !state.config.mcp.enabled || !isSelectedNativeMcpTool(entry.definition, state.config.mcp.nativeServers),
        ), new Set(PI_CORE_TOOL_NAMES))
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
    // Native prompt options produce journaled section deltas. Returning systemPrompt
    // instead creates an unjournaled forced projection after context_with_system;
    // activation dispatch then (correctly) rejects its mismatch with the witness.
    event.systemPromptOptions.sections.fabric_execution = guidance;
    if (!skillReferenceGuidance) return;
    const message = {
      customType: SKILL_REFERENCE_CUSTOM_TYPE,
      content: skillReferenceGuidance,
      display: false,
      details: {},
    };
    if (!fabricProvenanceSupported(pi)) return { message };
    sendFabricMessage(pi, message, { deliverAs: "nextTurn", triggerTurn: false });
  });

  // Ambient skill prose that names hidden captured tools is not user intent,
  // so the furnace strips it. This sidecar retargets the call site without
  // spending hint budget, echoing tokens, or burning ash.
  pi.on("before_agent_start", (event, context) => {
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
    const message = {
      customType: PROXY_CONTRACT_CUSTOM_TYPE,
      content: formatProxyContractReminder(fresh),
      display: false,
      details: { names: fresh, origin: "skill" },
    };
    if (!fabricProvenanceSupported(pi)) return { message };
    sendFabricMessage(pi, message, { deliverAs: "nextTurn", triggerTurn: false });
  });

  // Work events a steer missed reach the Main with its next turn (smarty-dev#754).
  pi.on("before_agent_start", async (_event, context) => {
    inboxWake.context = context;
    inboxWake.armed = true;
    if (!state.initialized) return;
    const inbox = await state.nextRootInbox(await inboxHeldBy(context)).catch(() => undefined);
    await reportInboxExpiry(pi, inbox);
    if (!inbox?.events.length) return;
    // Only capable Pi consumes nextTurn after hooks; legacy Pi needs the hook result.
    if (!fabricProvenanceSupported(pi)) return { message: (await import("./topology/root-inbox.js")).rootInboxMessage(inbox.events) };
    await deliverRootInbox(pi, inbox.events, { deliverAs: "nextTurn", triggerTurn: false });
  });

  // Records addressed to this root reach it with its next turn (smarty-dev#754 C4).
  pi.on("before_agent_start", async (_event, context) => {
    if (!state.initialized) return;
    const message = await state.nextRecordsInboxMessage(context.sessionManager.getEntries()).catch(() => undefined);
    if (!message) return;
    // Records have authors, not authenticated admission envelopes. Never claim this Main.
    if (!fabricProvenanceSupported(pi)) return { message };
    sendFabricMessage(pi, message, { deliverAs: "nextTurn", triggerTurn: false });
  });

  registerFabricActorHostEventObservers(pi, (eventName, event, context) => {
    if (!state.initialized) return;
    state.dispatchHostEvent(eventName, event, context);
  });

  pi.on("session_shutdown", async (event, context) => {
    stopInboxWake();
    const reason = event?.reason ?? "exit";
    let stopping = 0;
    try { stopping = state.initialized ? state.agents.runningCount() : 0; } catch { /* not initialized */ }
    if (stopping > 0 && context.hasUI) {
      context.ui.notify(
        `${reason === "reload" ? "Reload" : "Shutdown"} stops ${stopping} running task agent${stopping === 1 ? "" : "s"}; ` +
          'each spawner gets a stopped result. Spawn with residency: "durable" to keep an agent across reloads.',
        "warning",
      );
    }
    // ponytail: the entropy compile is advisory and machine-wide; session JSONL stays the source
    // of truth, so the next compile on this machine reads this session's final window. Waiting
    // for it here kept `pi -p` alive 14-98 s re-reading other agents' multi-hundred-MB sessions
    // (smarty-dev#2010). Abort it instead; every write is gated on current().
    entropyStopping = true;
    clearEntropyRetry();
    endEntropyLifecycle();
    unsubscribeComponentRegistration();
    unsubscribeProviderRegistration();
    unsubscribeProviderWithdrawal();
    unsubscribeToolPlacement();
    unsubscribeProgramRun();
    programRunContext = undefined;
    pendingHandoffs.clear();
    directToolApproval.clear();
    toolDisplay.clear();
    try {
      await state.shutdown(reason);
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
  // Foreground refusals are never silent: one notice per session, plus status.
  let foregroundNoticeSession: string | undefined;
  pi.on("before_agent_start", (_event, context) => {
    reassertToolOwnership();
    const foreground = state.foregroundTools();
    if (foreground.refused.length === 0) return;
    const sessionId = context.sessionManager?.getSessionId?.() ?? "";
    if (foregroundNoticeSession === sessionId) return;
    foregroundNoticeSession = sessionId;
    const notice = `Fabric foreground policy ${formatForeground(foreground)}`;
    if (context.hasUI) context.ui.notify(notice, "warning");
    else console.warn(`[pi-fabric] ${notice}`);
  });

  // A configured custom runner is kept even before its extension registers it
  // (load order), so launches fail closed. Say so before the first turn instead
  // of at the first agents.run. Registrations live on a globalThis map, so this
  // check never loads the runner module.
  let runnerNoticeKey: string | undefined;
  pi.on("before_agent_start", (_event, context) => {
    if (!state.bootstrapped) return;
    const runner = state.config.agents.runner;
    const notice = unregisteredRunnerNotice(
      runner,
      (globalThis as Record<symbol, Map<string, unknown> | undefined>)[
        Symbol.for("pi-fabric.runnerRegistry.v1")
      ]?.keys() ?? [],
    );
    if (!notice) return;
    const key = `${context.sessionManager?.getSessionId?.() ?? ""}\0${runner}`;
    if (runnerNoticeKey === key) return;
    runnerNoticeKey = key;
    if (context.hasUI) context.ui.notify(notice, "warning");
    else console.warn(`[pi-fabric] ${notice}`);
  });

  pi.on("context_with_system", (event) => {
    if (!fabricOwnsModelTools()) return;
    reassertToolOwnership();
    const foreground = modelDeclaredTools();
    const registered = foreground.length === 0 ? [] : pi.getAllTools();
    const declared = foreground.flatMap((name) => registered.filter((tool) => tool.name === name)
      .map(({ description, parameters }) => ({ name, description, parameters })));
    return { messages: fabricModelContext(event.messages, [{
      name: fabricTool.name, description: fabricTool.description, parameters: fabricTool.parameters,
    }, ...declared]) };
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

  // Registered after Fabric's own agent_settled handler, so the inbox follow-up goes first.
  const selfReload = installSelfReload(pi, {
    busy: () => state.initialized
      ? state.agents.runningCount() + state.actors.inFlightCount() + state.backgroundWorkCount()
      : 0,
    autoReloadConfigured: () => state.provisionalConfig().autoReload,
    selfReloadConcurrency: () => state.provisionalConfig().selfReloadConcurrency,
    moduleUrl: entryUrl,
    publishHeld: data => { void state.publishOpsEvent(RELOAD_HELD_TOPIC, "fabric.reload_held", data); },
    // Escape's stop-the-world halt of actors or Jev observers (mesh off too); the user's next
    // input lifts it (review/astra on pi-fabric#158, #160).
    halted: () => escapeLatched || state.escapeHalted,
    // Pi's host-wide hold query covers native/extension dialogs, custom UI and editors.
    // Old hosts remain fail-closed for resources and retain legacy Fabric-only behavior.
    reloadTargetUiHold,
  });
};
}
