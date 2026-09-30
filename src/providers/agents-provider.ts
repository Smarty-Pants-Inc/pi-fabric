import { createHash } from "node:crypto";
import { formatAge } from "../residency/protocol.js";
import { ActorManager, ActorRegistryOwnershipError, parseBashTimeoutSeconds } from "../actors/manager.js";
import { participantProject, resolveProjectAgent } from "../topology/project-identity.js";
import { GlobalActorRegistry } from "../actors/global-registry.js";
import { isFabricActorHostEvent, validateActorCoalesceKey, validateActorInferenceContext } from "../actors/types.js";
import { normalizeActorActivationFilter } from "../actors/activation-filter.js";
import type {
  FabricActorDelivery,
  FabricActorHostEvent,
  FabricActorInfo,
  FabricActorMessage,
  FabricActorRequest,
  FabricActorRunBinding,
} from "../actors/types.js";
import type {
  FabricAgentMessageResult,
  FabricMainAgentTarget,
} from "../main-agent.js";
import type { MeshIdentity } from "../mesh/store.js";
import { LifecycleBroker } from "../lifecycle/broker.js";
import {
  DEFAULT_LIFECYCLE_COALESCE_MS,
  LifecycleDeliveryScheduler,
  type PendingLifecycleDelivery,
} from "../lifecycle/delivery-scheduler.js";
import {
  isFabricLifecycleEventType,
  lifecycleSourceIdentity,
  type FabricLifecycleEvent,
  type FabricLifecycleSubscription,
} from "../lifecycle/types.js";
import {
  FabricControlPlane,
  type FabricControlCommand,
  type FabricControlAcceptance,
} from "../topology/control-plane.js";
import type {
  FabricParticipantInfo,
  FabricParticipantScope,
  FabricParticipantSource,
} from "../topology/types.js";
import type {
  FabricActionDescriptor,
  FabricCapabilityRequirement,
  FabricInvocationContext,
  FabricProvider,
  FabricProviderListRequest,
} from "../protocol.js";
import {
  effectiveAgentTimeoutMs,
  AgentManager,
} from "../agents/manager.js";
import { checkedHandoffCompaction } from "../agents/handoff.js";
import { withInheritedSessionPins } from "../agents/session-pins.js";
import type {
  AgentHandleInfo,
  AgentRunRecord,
  AgentRunRequest,
  AgentRunResult,
  AgentSessionSeed,
} from "../agents/types.js";
import type { ThinkingTransferInput } from "../agents/thinking-transfer.js";
import {
  DEFAULT_FABRIC_CONFIG,
  type FabricAgentRunner,
  type FabricModelsConfig,
} from "../config.js";
import {
  FUZZY_RESOLUTION_MARKERS,
  resolveAvailablePiModel,
  resolveFabricModel,
  type FabricModelCandidate,
} from "../core/model-resolution.js";
import { resolvePiModel } from "../core/model-refresh.js";
import { loadModelUsage } from "../core/model-usage.js";
import { AGENTS_ACTION_DESCRIPTORS } from "./agents-actions.js";
import { mainExecutionCeilingAbortReason, withoutMainExecutionCeiling } from "../async-settlement.js";
import {
  AGENT_WAIT_MAX_MS,
  AgentWaitBoundError,
  mainAgentWaitBound,
  agentWaitBound,
  describeWaitBound,
  isInteractiveMain,
} from "../agents/wait-bound.js";
import { actionArgNormalizer } from "./arg-normalization.js";
import { isFabricThinking } from "../thinking.js";
import { normalizeAgentRunRequest } from "../agents/request.js";
import { parseAgentNice } from "../agents/priority.js";
import { ResidencyClient } from "../residency/client.js";
import { ResidentActorClient } from "../residency/actor-client.js";
import { AgentTranscriptReader } from "../ui/transcript.js";
import { waitWithProgress, waitWithActorProgress } from "./agents-progress.js";
import { AgentMessageRouter, unknownParticipant } from "./agents-message-router.js";
import { terminalAgentStatuses } from "../agents/lifecycle.js";
import { deliverWithMessageNotice, outgoingMessageNotice } from "./message-id-notice.js";

export { collectAgentToolPreviewNodes, type AgentToolPreviewTreeOptions } from "./agents-progress.js";

const REMOTE_ASK_ACK_GRACE_MS = 30_000;
const MAX_ACTIVITY_CWD_CHARS = 240;

const displaySafeCwd = (cwd: string): string => {
  const safe = cwd.replace(/[\u0000-\u001f\u007f]/g, (character) =>
    `\\u${character.codePointAt(0)!.toString(16).padStart(4, "0")}`,
  );
  if (safe.length <= MAX_ACTIVITY_CWD_CHARS) return safe;
  return `…${safe.slice(-(MAX_ACTIVITY_CWD_CHARS - 1))}`;
};

const agentStartedMessage = (handle: AgentHandleInfo): string =>
  `Agent ${handle.name} started via ${handle.runner}/${handle.transport}${handle.attachCommand ? ` · ${handle.attachCommand}` : ""} · cwd ${displaySafeCwd(handle.cwd)}`;

// Resolve source and executor reasoning channels for the trajectory handoff
// boundary. The executor model must be registered to transfer at all; an
// unresolvable source model simply yields no family comparison, so the
// transfer falls back to the target-driven policy.
const resolveThinkingTransfer = (
  extensionContext: FabricInvocationContext["extensionContext"],
  targetKey: string,
  sourceModel?: { provider: string; modelId: string },
): ThinkingTransferInput | undefined => {
  const separator = targetKey.indexOf("/");
  if (separator <= 0 || separator === targetKey.length - 1) return undefined;
  // Invocation contexts don't always thread the extension host (tests, nested
  // runners); without a registry no family comparison is possible.
  const registry = extensionContext?.modelRegistry;
  if (!registry) return undefined;
  const target = registry.find(targetKey.slice(0, separator), targetKey.slice(separator + 1));
  if (!target) return undefined;
  const source = sourceModel
    ? {
        provider: sourceModel.provider,
        modelId: sourceModel.modelId,
        api: registry.find(sourceModel.provider, sourceModel.modelId)?.api,
      }
    : undefined;
  return {
    ...(source ? { source } : {}),
    target: {
      provider: target.provider,
      modelId: target.id,
      api: target.api,
      reasoning: target.reasoning,
      ...((target.compat as { requiresThinkingAsText?: boolean } | undefined)
        ?.requiresThinkingAsText !== undefined
        ? {
            requiresThinkingAsText: (target.compat as { requiresThinkingAsText?: boolean })
              .requiresThinkingAsText,
          }
        : {}),
    },
  };
};

const stringArray = (value: unknown): string[] | undefined =>
  Array.isArray(value)
    ? value.filter((entry): entry is string => typeof entry === "string")
    : undefined;

const actorRunBinding = (args: Record<string, unknown>): FabricActorRunBinding => ({
  ...(typeof args.model === "string" && args.model.trim()
    ? { model: args.model.trim() }
    : {}),
  ...(isFabricThinking(args.thinking) ? { thinking: args.thinking } : {}),
});

const longerTimeoutOverride = (
  value: unknown,
  manager: AgentManager,
): number | undefined => {
  if (typeof value !== "number" || !Number.isFinite(value)) return undefined;
  const effective = effectiveAgentTimeoutMs(manager.config.timeoutMs, value);
  return effective > manager.config.timeoutMs ? effective : undefined;
};

const checkedKernel = (value: unknown): AgentRunRequest["kernel"] => {
  if (value === undefined || value === "inherit" || value === "typescript" || value === "python") return value;
  throw new Error(`Invalid Fabric agent kernel: ${String(value)}`);
};

const runRequest = (args: Record<string, unknown>, context: FabricInvocationContext, manager: AgentManager, options: {allowCwd?: boolean} = {}): AgentRunRequest => normalizeAgentRunRequest({...args, timeoutMs: longerTimeoutOverride(args.timeoutMs, manager)}, { ...manager.config, ...(context.extensionContext.model ? {inheritedModel: context.extensionContext.model} : {}) }, options);

const handoffTask = (args: Record<string, unknown>): string => {
  const task = typeof args.task === "string" ? args.task.trim() : "";
  const lines = [
    "Continue and complete the current user task from the inherited conversation trajectory and current workspace.",
    "The caller is blocked awaiting this run. Finish what the user actually requested. If the request is read-only — a plan, review, or investigation — deliver the answer without changing files. For implementation requests, complete the remaining changes and verification; do not merely restate the plan.",
    "Treat the inherited conversation, completed outer Fabric result, and current workspace as grounded context. Inspect again only where the workspace or a failed check makes it necessary.",
    "Keep the work scoped, run the relevant test module or equivalent verification, and report the result plus checks honestly.",
    "End with a concise conclusion for the caller: what you completed, which checks passed or failed, and any unfinished or blocked work. Include links, PR and issue numbers, commit hashes, and artifact paths verbatim.",
  ];
  if (task) lines.push("Additional continuation task:", task);
  return lines.join("\n\n");
};

const compactHandoffResult = (
  result: AgentRunResult,
): Record<string, unknown> => ({
  handedOff: true,
  completed: result.status === "completed",
  status: result.status,
  agent: {
    id: result.id,
    name: result.name,
    runner: result.runner,
    ...(result.kernel ? { kernel: result.kernel } : {}),
    transport: result.transport,
    ...(result.model ? { model: result.model } : {}),
    ...(result.thinking ? { thinking: result.thinking } : {}),
    turns: result.turns,
    toolCalls: result.toolCalls,
    usage: result.usage,
  },
  implementation: result.value ?? result.text,
  ...(result.error ? { error: result.error } : {}),
});

const actorRequest = (
  args: Record<string, unknown>,
  context: FabricInvocationContext,
  manager: AgentManager,
  inheritModel = true,
): FabricActorRequest => {
  const events = Array.isArray(args.events)
    ? args.events.filter(
        (event): event is FabricActorHostEvent => isFabricActorHostEvent(event),
      )
    : undefined;
  const topics = stringArray(args.topics);
  const tools = stringArray(args.tools);
  const requires = Array.isArray(args.requires)
    ? args.requires.reduce<Array<string | FabricCapabilityRequirement>>(
        (result, requirement) => {
          if (typeof requirement === "string") result.push(requirement);
          else if (
            typeof requirement === "object" &&
            requirement !== null &&
            !Array.isArray(requirement) &&
            typeof (requirement as { ref?: unknown }).ref === "string"
          ) {
            result.push({
              ref: (requirement as { ref: string }).ref,
              ...((requirement as { optional?: unknown }).optional === true
                ? { optional: true }
                : {}),
            });
          }
          return result;
        },
        [],
      )
    : undefined;
  const timeoutMs = longerTimeoutOverride(args.timeoutMs, manager);
  const validWhile = typeof args.validWhile === "object" && args.validWhile !== null &&
    !Array.isArray(args.validWhile) &&
    (args.validWhile as { version?: unknown }).version === 1 &&
    typeof (args.validWhile as { source?: unknown }).source === "string"
    ? { version: 1 as const, source: (args.validWhile as { source: string }).source }
    : undefined;
  const runner =
    args.runner === "pi" || args.runner === "claude" || args.runner === "veda"
      ? args.runner
      : manager.config.runner;
  if (runner === "veda") {
    throw new Error(
      'The Veda runner does not support persistent actors: Veda executes one headless prompt per invocation. Use a Pi or Claude actor, or agents.run({ runner: "veda" }).',
    );
  }
  validateActorInferenceContext(args.inferenceContext, runner);
  validateActorCoalesceKey(args.coalesceKey);
  const activationFilter = args.activationFilter === undefined ? undefined : normalizeActorActivationFilter(args.activationFilter);
  const requestedKernel = checkedKernel(args.kernel);
  const kernelRequest = {
    runner,
    ...(requestedKernel !== undefined ? { kernel: requestedKernel } : {}),
    extensions: typeof args.extensions === "boolean" ? args.extensions : true,
  };
  // Templates retain inheritance until import; live actors freeze at creation.
  const kernel = inheritModel ? manager.resolveKernel(kernelRequest) : requestedKernel;
  if (!inheritModel && kernel !== undefined && kernel !== "inherit") manager.resolveKernel(kernelRequest);
  const inheritedModel =
    inheritModel && runner === "pi" && !manager.config.model && context.extensionContext.model
      ? `${context.extensionContext.model.provider}/${context.extensionContext.model.id}`
      : undefined;
  return {
    ...(args.scope === "session" || args.scope === "project" ? { scope: args.scope } : {}),
    name: String(args.name),
    instructions: String(args.instructions),
    runner,
    ...(kernel !== undefined ? { kernel } : {}),
    ...(events ? { events } : {}),
    ...(topics ? { topics } : {}),
    ...(args.delivery === "mailbox" ||
    args.delivery === "steer" ||
    args.delivery === "followUp" ||
    args.delivery === "nextTurn"
      ? { delivery: args.delivery }
      : {}),
    ...(args.responseMode === "text" || args.responseMode === "directive"
      ? { responseMode: args.responseMode }
      : {}),
    ...(typeof args.triggerTurn === "boolean" ? { triggerTurn: args.triggerTurn } : {}),
    ...(typeof args.coalesce === "boolean" ? { coalesce: args.coalesce } : {}),
    ...(typeof args.coalesceKey === "string" ? { coalesceKey: args.coalesceKey } : {}),
    ...(activationFilter ? { activationFilter } : {}),
    ...(args.residency === "session" || args.residency === "durable"
      ? { residency: args.residency }
      : {}),
    ...(typeof args.model === "string"
      ? { model: args.model }
      : inheritedModel
        ? { model: inheritedModel }
        : {}),
    ...(isFabricThinking(args.thinking) ? { thinking: args.thinking } : {}),
    ...(tools ? { tools } : {}),
    ...(args.transport === "auto" ||
    args.transport === "process" ||
    args.transport === "tmux" ||
    args.transport === "screen" ||
    args.transport === "localterm" ||
    args.transport === "herdr"
      ? { transport: args.transport }
      : {}),
    ...(timeoutMs !== undefined ? { timeoutMs } : {}),
    ...(args.nice !== undefined ? { nice: parseAgentNice(args.nice as number) } : {}), // non-numbers throw at runtime
    ...(args.bashTimeoutSeconds !== undefined ? { bashTimeoutSeconds: parseBashTimeoutSeconds(args.bashTimeoutSeconds) } : {}),
    ...(typeof args.extensions === "boolean" ? { extensions: args.extensions } : {}),
    ...(args.inferenceContext !== undefined ? { inferenceContext: args.inferenceContext } : {}),
    ...(requires ? { requires } : {}),
    ...(validWhile ? { validWhile } : {}),
  };
};

// Argument repair derives from the action schemas plus the shared synonym
// lexicon; no agents-specific table remains.
export const normalizeAgentsArgs = actionArgNormalizer(() => AGENTS_ACTION_DESCRIPTORS);

const MESSAGE_ACTIONS = new Set(["ask", "tell", "steer", "followUp"]);

// Messaging calls accept "to" (the mesh.publish spelling) as an alias of "id".
// sessionId is rejected with the exact fix: the generic synonym repair would
// otherwise map it onto id without the "session:" prefix the directory needs.
export const messageTargetArgs = (
  actionName: string,
  args: Record<string, unknown>,
): Record<string, unknown> => {
  if (!MESSAGE_ACTIONS.has(actionName)) return args;
  if (Object.hasOwn(args, "sessionId")) {
    const raw = String(args.sessionId).replace(/^session:/, "");
    throw new Error(
      `agents.${actionName} has no sessionId field: use id: 'session:${raw}'`,
    );
  }
  if (!Object.hasOwn(args, "to")) return args;
  const { to, ...rest } = args;
  if (rest.id !== undefined && to !== undefined && rest.id !== to) {
    throw new Error(
      `agents.${actionName} got two targets, id ${JSON.stringify(rest.id)} and to ${JSON.stringify(to)}: give one (to is an alias of id)`,
    );
  }
  return rest.id === undefined ? { ...rest, id: to } : rest;
};

export class AgentsProvider implements FabricProvider {
  readonly #transcripts = new AgentTranscriptReader();
  readonly #router: AgentMessageRouter;
  readonly name = "agents";
  readonly description =
    "The user-facing Main target, one-shot Pi or Claude Code agents, and persistent mailbox actors over process, tmux, screen, LocalTerm, or Herdr";

  constructor(
    readonly manager: AgentManager,
    readonly actorManager: ActorManager,
    readonly globalActors: GlobalActorRegistry,
    readonly mainAgent: FabricMainAgentTarget,
    readonly participants: FabricParticipantSource,
    readonly control: FabricControlPlane | undefined,
    readonly lifecycle: LifecycleBroker,
    readonly agentToolPreviewEnabled: () => boolean = () => true,
    readonly residency?: ResidencyClient,
    readonly ownsRuntime = true,
    readonly modelsConfig: () => FabricModelsConfig = () => DEFAULT_FABRIC_CONFIG.models,
  ) {
    this.#router = new AgentMessageRouter(
      manager, actorManager, mainAgent, participants, control,
      (binding, runner, context) => this.#resolvePiRunBinding(binding, runner, context),
    );
    this.#lifecycleScheduler = new LifecycleDeliveryScheduler(
      DEFAULT_LIFECYCLE_COALESCE_MS,
      (target, batch) => this.#routeLifecycleBatch(target, batch),
      (target, batch, error) => {
        console.warn(
          `[pi-fabric] lifecycle delivery to ${target} failed for ${batch.length} event(s): ` +
            (error instanceof Error ? error.message : String(error)),
        );
      },
    );
  }

  readonly #lifecycleScheduler: LifecycleDeliveryScheduler;

  #lifecycleMessage(event: FabricLifecycleEvent): string {
    const status = event.status ? ` with status ${event.status}` : "";
    const run = event.runId ? ` (run ${event.runId.slice(0, 8)})` : "";
    return `Fabric lifecycle ${event.event} from ${event.source.name} (${event.source.id})${run}${status}.`;
  }

  async #routeLifecycleBatch(
    target: string,
    batch: PendingLifecycleDelivery[],
  ): Promise<void> {
    const first = batch[0]!;
    const single = batch.length === 1;
    const message = single
      ? this.#lifecycleMessage(first.event)
      : `Fabric lifecycle events (${batch.length}):\n` +
        batch.map((delivery) => `- ${this.#lifecycleMessage(delivery.event)}`).join("\n");
    const data = single ? first.event : batch.map((delivery) => delivery.event);
    const last = batch[batch.length - 1]!;
    await this.routeMessage(
      target,
      message,
      data,
      first.subscription.delivery,
      undefined,
      {
        from: single
          ? lifecycleSourceIdentity(first.event.source)
          : lifecycleSourceIdentity(last.event.source),
        triggerTurn: batch.some((delivery) => delivery.subscription.triggerTurn),
      },
    );
  }

  /** Resolve a Pi participant selector only within this session's visible registry. */
  async #resolvePiModel(
    model: string,
    context: FabricInvocationContext,
  ): Promise<string> {
    // A model added to models.json after startup resolves after one shared refresh (smarty-dev#1830).
    const resolved = await resolvePiModel({
      selector: model,
      registry: context.extensionContext.modelRegistry,
      aliases: this.modelsConfig().aliases,
    });
    return `${resolved.provider}/${resolved.id}`;
  }

  async #resolvePiModelArgs(
    args: Record<string, unknown>,
    context: FabricInvocationContext,
    runnerOverride?: FabricAgentRunner,
  ): Promise<Record<string, unknown>> {
    const runner = runnerOverride ??
      (args.runner === "pi" || args.runner === "claude" || args.runner === "veda"
        ? args.runner
        : this.manager.config.runner);
    if (runner !== "pi") return args;
    const model = typeof args.model === "string" ? args.model.trim() : "";
    if (!model) return args;
    const resolved = await this.#resolvePiModel(model, context);
    return resolved === model ? args : { ...args, model: resolved };
  }

  async #resolvePiRunBinding(
    binding: FabricActorRunBinding,
    runner: FabricAgentRunner,
    context: FabricInvocationContext,
  ): Promise<FabricActorRunBinding> {
    if (runner !== "pi" || !binding.model) return binding;
    return { ...binding, model: await this.#resolvePiModel(binding.model, context) };
  }

  async list(
    request: FabricProviderListRequest,
    _context: FabricInvocationContext,
  ): Promise<FabricActionDescriptor[]> {
    const query = request.query?.toLowerCase();
    return query
      ? AGENTS_ACTION_DESCRIPTORS.filter((descriptor) =>
          `${descriptor.name} ${descriptor.description}`.toLowerCase().includes(query),
        )
      : AGENTS_ACTION_DESCRIPTORS;
  }

  async describe(
    actionName: string,
    _context: FabricInvocationContext,
  ): Promise<FabricActionDescriptor | undefined> {
    return AGENTS_ACTION_DESCRIPTORS.find((descriptor) => descriptor.name === actionName);
  }

  prepareArguments(
    actionName: string,
    args: Record<string, unknown>,
  ): Record<string, unknown> {
    return normalizeAgentsArgs(actionName, args);
  }

  // Before generic repair: a learned sessionId -> id row would otherwise drop
  // the "session:" prefix, and a to/id conflict must be seen as the caller wrote it.
  guardArguments(actionName: string, args: Record<string, unknown>): Record<string, unknown> {
    return messageTargetArgs(actionName, args);
  }

  async handoff(
    args: Record<string, unknown>,
    context: FabricInvocationContext,
  ): Promise<Record<string, unknown>> {
    const model = typeof args.model === "string" ? args.model.trim() : "";
    if (!model) throw new Error("agents.handoff requires an explicit Pi target model");
    checkedHandoffCompaction(args.compact);
    if (!context.deferHandoff) {
      throw new Error(
        "agents.handoff must be scheduled from inside fabric_exec and completed at its outer result boundary",
      );
    }
    const handoffArgs = await this.#resolvePiModelArgs(
      { ...args, model },
      context,
      "pi",
    );
    delete handoffArgs.cwd;
    const request = runRequest({ ...handoffArgs, runner: "pi" }, context, this.manager);
    const kernel = this.manager.resolveKernel(request);
    delete handoffArgs.kernel;
    if (kernel) handoffArgs.kernel = kernel;
    handoffArgs.extensions = request.extensions ?? this.manager.config.extensions;
    if (kernel) handoffArgs.pythonRuntime = this.manager.resolvePythonRuntime();
    return context.deferHandoff(handoffArgs);
  }

  async executeHandoff(
    args: Record<string, unknown>,
    context: FabricInvocationContext,
    sessionSeed: AgentSessionSeed,
  ): Promise<Record<string, unknown>> {
    const model = typeof args.model === "string" ? args.model.trim() : "";
    if (!model) throw new Error("agents.handoff requires an explicit Pi target model");
    const request = runRequest(
      await this.#resolvePiModelArgs(
        {
          ...args,
          task: handoffTask(args),
          name:
            typeof args.name === "string" && args.name.trim()
              ? args.name
              : "Trajectory handoff",
          runner: "pi",
          model,
        },
        context,
      ),
      context,
      this.manager,
      { allowCwd: false },
    );
    request.runner = "pi";
    if (args.pythonRuntime !== undefined) {
      // Only host-created deferred handoffs carry this internal policy snapshot.
      request.pythonRuntime = this.manager.resolvePythonRuntime(args.pythonRuntime as AgentRunRequest["pythonRuntime"]);
    }
    request.sessionSeed = sessionSeed;
    const targetModel = request.model ?? model;
    const handoffCompaction = checkedHandoffCompaction(args.compact);
    if (handoffCompaction) request.handoffCompact = handoffCompaction;
    request.thinkingTransfer = resolveThinkingTransfer(
      context.extensionContext,
      targetModel,
      sessionSeed.sourceModel,
    );
    const handle = await this.manager.spawn(request, context.signal);
    context.activity?.({
      type: "entity",
      id: handle.id,
      kind: "agent",
      name: handle.name,
    });
    context.update(
      `Trajectory handed off to ${handle.name} (${targetModel}); caller is waiting for implementation`,
    );
    const completed = await waitWithProgress(
      this.manager,
      this.#transcripts,
      handle.id,
      context,
      this.agentToolPreviewEnabled,
      { ...(context.deferResultConsumption ? { deferConsumption: context.deferResultConsumption } : {}) },
    );
    context.update(
      completed.status === "completed"
        ? `Handoff ${handle.name} completed implementation`
        : `Handoff ${handle.name} ended with ${completed.status}`,
    );
    return compactHandoffResult(completed);
  }

  async invoke(
    actionName: string,
    args: Record<string, unknown>,
    context: FabricInvocationContext,
  ): Promise<unknown> {
    try {
      return await this.#invoke(actionName, args, context);
    } catch (error) {
      // smarty-dev#2184 item 8: name a pending removal (or a long host request) that these
      // errors come from, so a caller does not read them as a lost or foreign host.
      if (!(error instanceof Error) || !/owned by another host/.test(error.message)) throw error;
      const note = this.residency?.hostStateNote?.();
      if (note) error.message = `${error.message}; ${note}`;
      throw error;
    }
  }

  #mainWaitAtBound(record: object, timeoutMs: number): Record<string, unknown> {
    // The wait ended so Main can see news: held followUps land at this tool boundary.
    this.mainAgent.flushHeldAtNextBoundary?.();
    return {
      ...record,
      waitTimedOut: true,
      note: `Still running after ${describeWaitBound(timeoutMs)}; Main waits are capped at 60 s. It continues, and its ` +
        "result arrives as a completion message: end the turn or do other work, then check agents.status.",
    };
  }

  async #invoke(
    actionName: string,
    args: Record<string, unknown>,
    context: FabricInvocationContext,
  ): Promise<unknown> {
    switch (actionName) {
      case "run": {
        const main = isInteractiveMain(context.extensionContext);
        const handle = await this.manager.spawn(
          runRequest(await this.#resolvePiModelArgs(args, context), context, this.manager),
          // Only the branded Main ceiling is observation-only, including during launch.
          // Escape and ordinary deadlines retain zero-progress child cancellation.
          main ? withoutMainExecutionCeiling(context.signal) : context.signal,
        );
        const timeoutMs = mainAgentWaitBound(args.timeoutMs, context.mainDeadlineAt);
        try {
          this.participants.scheduleRefresh();
          context.activity?.({
            type: "entity",
            id: handle.id,
            kind: "agent",
            name: handle.name,
          });
          context.update(agentStartedMessage(handle));
          return await waitWithProgress(
            this.manager,
            this.#transcripts,
            handle.id,
            context,
            this.agentToolPreviewEnabled,
            { ...(main ? { timeoutMs, ...(context.signal ? { signal: context.signal } : {}) } : {}), ...(context.deferResultConsumption ? { deferConsumption: context.deferResultConsumption } : {}) },
          );
        } catch (error) {
          // A launch can spend the remaining budget before its first update/wait.
          // Accepted work still needs a detached completion owner in that case.
          if (main && mainExecutionCeilingAbortReason(context.signal)) this.manager.detachSignal(handle.id);
          if (!main || !(error instanceof AgentWaitBoundError)) throw error;
          return this.#mainWaitAtBound(this.manager.status(handle.id), timeoutMs);
        }
      }
      case "handoff":
        return this.handoff(args, context);
      case "spawn": {
        const request = runRequest(await this.#resolvePiModelArgs(args, context), context, this.manager);
        const kernel = this.manager.resolveKernel(request);
        const { kernel: _requestedKernel, ...baseRequest } = request;
        const durableCwd = request.residency === "durable" && request.cwd !== undefined
          ? await this.manager.resolveCwd(request.cwd, context.signal)
          : undefined;
        const durableRequest = withInheritedSessionPins({
          ...baseRequest,
          ...(kernel ? { kernel, pythonRuntime: this.manager.resolvePythonRuntime() } : {}),
          extensions: request.extensions ?? this.manager.config.extensions,
          ...(durableCwd !== undefined ? { cwd: durableCwd } : {}),
        }, context.extensionContext.sessionManager?.getEntries?.() ?? []);
        const handle = durableRequest.residency === "durable"
          ? await this.#resident().spawnAgent(durableRequest, context.signal)
          : await this.manager.spawn(durableRequest, isInteractiveMain(context.extensionContext) ? withoutMainExecutionCeiling(context.signal) : context.signal);
        if (request.residency !== "durable") this.manager.detachSignal(handle.id);
        this.participants.scheduleRefresh();
        context.activity?.({
          type: "entity",
          id: handle.id,
          kind: "agent",
          name: handle.name,
        });
        context.update(agentStartedMessage(handle));
        return handle;
      }
      case "join":
      case "wait": {
        const id = String(args.id);
        // smarty-dev#2119: an interactive Main waits at most 60 s, and the bound is a normal result.
        const main = isInteractiveMain(context.extensionContext);
        const timeoutMs = main
          ? mainAgentWaitBound(args.timeoutMs, context.mainDeadlineAt)
          : agentWaitBound(args.timeoutMs, AGENT_WAIT_MAX_MS);
        const atBound = (record: object) => this.#mainWaitAtBound(record, timeoutMs);
        if (this.residency?.hasAgent(id)) {
          const status = this.residency.statusAgent(id);
          context.activity?.({ type: "entity", id, kind: "agent", name: status.name });
          context.update(`Waiting for durable agent ${status.name}`);
          // Stopping the wait leaves the durable run and its unread result alone (smarty-dev#854).
          const bound = AbortSignal.timeout(timeoutMs);
          const signal = context.signal ? AbortSignal.any([context.signal, bound]) : bound;
          try {
            return await this.residency.waitAgent(id, signal, context.deferResultConsumption);
          } catch (error) {
            if (!bound.aborted || context.signal?.aborted) throw error;
            if (main) return atBound(this.residency.statusAgent(id));
            throw new AgentWaitBoundError(
              `agents.wait: durable agent ${status.name} is still running after ${describeWaitBound(timeoutMs)}. ` +
                "It continues, and its result arrives as a completion message: end the turn now.",
            );
          }
        }
        const status = this.manager.status(id);
        context.activity?.({ type: "entity", id, kind: "agent", name: status.name });
        try {
          return await waitWithProgress(
            this.manager,
            this.#transcripts,
            id,
            context,
            this.agentToolPreviewEnabled,
            { timeoutMs, ...(main && context.signal ? { signal: context.signal } : {}), ...(context.deferResultConsumption ? { deferConsumption: context.deferResultConsumption } : {}) },
          );
        } catch (error) {
          if (!main || !(error instanceof AgentWaitBoundError)) throw error;
          return atBound(this.manager.status(id));
        }
      }
      case "status": {
        const id = String(args.id);
        if (this.mainAgent.matches(id)) {
          if (this.mainAgent.local) return this.mainAgent.info(context.extensionContext);
          const root = this.participants.get(this.mainAgent.id);
          if (!root) throw this.participants.writeStalled?.() ?? unknownParticipant(this.participants, this.mainAgent.id, "Fabric Main participant");
          return root;
        }
        try {
          const result = this.manager.status(id);
          // Model-facing terminal status returns the result; UI polling must not acknowledge it.
          if (terminalAgentStatuses.has(result.status)) {
            if (context.deferResultConsumption) context.deferResultConsumption(() => this.manager.markForeground(id), () => this.manager.detachSignal(id));
            else this.manager.markForeground(id);
          }
          return result;
        } catch (error) {
          if (!(error instanceof Error && /Unknown Fabric agent/.test(error.message))) throw error;
        }
        if (this.residency?.hasAgent(id)) {
          const result = this.residency.statusAgent(id);
          if (terminalAgentStatuses.has(result.status)) {
            if (context.deferResultConsumption) context.deferResultConsumption(() => this.residency!.acknowledgeCompletion(id));
            else this.residency.acknowledgeCompletion(id);
          }
          return result;
        }
        const known = this.participants.get(id);
        if (known && !known.local) return known;
        try {
          return this.actorManager.status(id);
        } catch (error) {
          if (!(error instanceof Error && /Unknown Fabric actor/.test(error.message))) throw error;
        }
        const participant = this.participants.get(id);
        if (!participant) throw this.participants.writeStalled?.() ?? unknownParticipant(this.participants, id);
        return participant;
      }
      case "list":
        return this.#listAgents(args.scope);
      case "members": {
        const kinds = Array.isArray(args.kinds)
          ? args.kinds.filter(
              (kind): kind is "root" | "agent" | "actor" =>
                kind === "root" || kind === "agent" || kind === "actor",
            )
          : undefined;
        const scope = this.#participantScope(args.scope, "project");
        // Every scope but local reads the shared directory (lineage includes descendants
        // in other runtimes), so each is unknown, not short, during a write stall.
        const stalled = scope !== "local" && args.includeStale !== true
          ? this.participants.writeStalled?.()
          : undefined;
        if (stalled) throw stalled;
        return this.participants.list({
          scope,
          ...(kinds ? { kinds } : {}),
          ...(args.includeStale === true ? { includeStale: true } : {}),
        });
      }
      case "self":
        return this.participants.self();
      case "main":
        return this.mainAgent.info(context.extensionContext);
      case "sessions": {
        const stalled = this.participants.writeStalled?.();
        if (stalled) throw stalled;
        return this.participants.sessions?.() ??
          this.participants.list({ scope: "project", kinds: ["root"] });
      }
      case "peers": {
        const stalled = this.participants.writeStalled?.();
        if (stalled) throw stalled;
        return this.participants.peers();
      }
      case "projectAgent": {
        const stalled = this.participants.writeStalled?.();
        if (stalled) throw stalled;
        const roots = this.participants.sessions?.() ??
          this.participants.list({ scope: "project", kinds: ["root"] });
        return resolveProjectAgent(roots, participantProject(context.cwd));
      }
      case "subscribe": {
        const events = Array.isArray(args.events)
          ? args.events.filter(isFabricLifecycleEventType)
          : [];
        if (typeof args.triggerTurn !== "boolean") {
          throw new Error("Lifecycle subscriptions require explicit triggerTurn: true or false");
        }
        const delivery = args.delivery === "steer" || args.delivery === "followUp"
          ? args.delivery
          : undefined;
        if (!delivery) throw new Error("Invalid lifecycle subscription delivery");
        const subscription = await this.lifecycle.subscribe({
          from: this.#participantAlias(String(args.from)),
          events,
          to: this.#participantAlias(typeof args.to === "string" ? args.to : "main"),
          delivery,
          triggerTurn: args.triggerTurn,
          ...(args.once === true ? { once: true } : {}),
        });
        context.update(
          `Subscribed ${subscription.to.slice(0, 8)} to ${subscription.events.join(", ")} from ${subscription.from.slice(0, 8)}`,
        );
        return subscription;
      }
      case "subscriptions":
        return this.lifecycle.list({
          ...(typeof args.from === "string"
            ? { from: this.#participantAlias(args.from) }
            : {}),
          ...(typeof args.to === "string"
            ? { to: this.#participantAlias(args.to) }
            : {}),
        });
      case "unsubscribe":
        return this.lifecycle.unsubscribe(String(args.id));
      case "models": {
        const runner =
          args.runner === "pi" || args.runner === "claude" || args.runner === "veda"
            ? args.runner
            : this.manager.config.runner;
        if (runner === "veda") {
          // Veda forwards any -m value to the configured backend; model
          // discovery would require parsing `veda models <backend>`. Return an
          // empty advisory list so callers can still pass model strings
          // directly to agents.run({ runner: "veda", model }).
          return [];
        }
        if (runner === "claude") {
          const models = await this.manager.claudeModels(args.refresh === true);
          return models.map((model) => ({
            runner: "claude",
            provider: "claude",
            id: model.value,
            name: model.displayName,
            key: `claude/${model.value}`,
            ...model,
          }));
        }
        try {
          const available = context.extensionContext.modelRegistry.getAvailable();
          return available.map((model) => ({
            runner: "pi",
            provider: String(model.provider),
            id: String(model.id),
            name: String(model.name ?? model.id),
            key: `${model.provider}/${model.id}`,
          }));
        } catch {
          return [];
        }
      }
      case "switchModel": {
        const query = typeof args.model === "string" ? args.model.trim() : "";
        if (!query) {
          throw new Error(
            "agents.switchModel requires a model selector: provider/id, models.aliases name, or search term",
          );
        }
        const registry = context.extensionContext.modelRegistry;
        let available: FabricModelCandidate[] = [];
        try {
          available = registry.getAvailable().map((model) => ({
            provider: String(model.provider),
            id: String(model.id),
            ...(typeof model.name === "string" ? { name: model.name } : {}),
          }));
        } catch {
          available = [];
        }
        if (available.length === 0) {
          throw new Error(
            "agents.switchModel found no authenticated models; configure a provider key or check agents.models()",
          );
        }
        const currentModel = context.extensionContext.model ?? undefined;
        const resolution = resolveFabricModel(query, {
          aliases: this.modelsConfig().aliases,
          available,
          lastUsed: loadModelUsage(),
          ...(currentModel
            ? { current: { provider: currentModel.provider, id: currentModel.id } }
            : {}),
          ...(typeof args.provider === "string" && args.provider.trim()
            ? { provider: args.provider.trim() }
            : {}),
        });
        if (resolution.kind === "already-active") {
          return {
            switched: false,
            reason: "already-active",
            model: `${resolution.model.provider}/${resolution.model.id}`,
            ...(resolution.model.name ? { name: resolution.model.name } : {}),
          };
        }
        if (resolution.kind === "ambiguous") {
          throw new Error(
            `agents.switchModel: "${query}" matches multiple models: ${resolution.candidates
              .map((candidate) => `${candidate.provider}/${candidate.id}`)
              .join(", ")}. Pass an exact provider/id.`,
          );
        }
        if (resolution.kind === "not-found") {
          throw new Error(
            resolution.tried !== undefined
              ? `agents.switchModel: alias "${query}" has no available target. Tried: ${resolution.tried.join(", ")}`
              : `agents.switchModel: no available model matching "${query}"`,
          );
        }
        if (typeof this.mainAgent.switchModel !== "function") {
          throw new Error("agents.switchModel requires a local Main session");
        }
        const previous = currentModel
          ? `${currentModel.provider}/${currentModel.id}`
          : undefined;
        const outcome = await this.mainAgent.switchModel(
          { provider: resolution.model.provider, id: resolution.model.id },
          context.extensionContext,
        );
        if (!outcome.ok) {
          throw new Error(`agents.switchModel: ${outcome.error ?? "switch failed"}`);
        }
        try {
          this.residency?.syncPiModels();
        } catch {
          // The next durable command retries synchronization before execution.
        }
        context.activity?.({
          type: "progress",
          message: `Main model ${previous ? `${previous} → ` : ""}${resolution.model.provider}/${resolution.model.id}`,
        });
        return {
          switched: true,
          model: `${resolution.model.provider}/${resolution.model.id}`,
          ...(resolution.model.name ? { name: resolution.model.name } : {}),
          ...(previous ? { previous } : {}),
          ...(resolution.via !== undefined ? { via: resolution.via } : {}),
          ...(resolution.via !== undefined &&
          !(FUZZY_RESOLUTION_MARKERS as readonly string[]).includes(resolution.via)
            ? { alias: resolution.via }
            : {}),
        };
      }
      case "stop":
        return this.stopParticipant(String(args.id));
      case "cleanup": {
        const id = String(args.id);
        return this.residency?.hasAgent(id)
          ? this.residency.cleanupAgent(id, args.deleteBranch === true)
          : this.manager.cleanup(id, args.deleteBranch === true);
      }
      case "create": {
        const createArgs = await this.#resolvePiModelArgs(args, context);
        if (createArgs.scope === "global") {
          return this.globalActors.create(actorRequest(createArgs, context, this.manager, false));
        }
        const request = actorRequest(createArgs, context, this.manager);
        const actor = await this.#createActor(request);
        this.participants.scheduleRefresh();
        context.activity?.({ type: "entity", id: actor.id, kind: "actor", name: actor.name });
        return actor;
      }
      case "ask": {
        const id = String(args.id);
        const message = String(args.message);
        this.actorManager.validateDirectMessage(message, args.data);
        const { actor, participant } = this.#resolveActorTarget(id);
        const ownsActor = actor ? this.actorManager.owns(actor.id) : false;
        const requestedOverrides = actorRunBinding(args);
        const overrides = ownsActor
          ? await this.#resolvePiRunBinding(requestedOverrides, actor!.runner, context)
          : requestedOverrides;
        context.activity?.({
          type: "entity",
          id: actor?.id ?? participant!.id,
          kind: "actor",
          name: actor?.name ?? participant!.name,
        });
        if (actor && ownsActor) {
          return waitWithActorProgress(
            this.manager,
            this.#transcripts,
            actor.id,
            actor.name,
            this.actorManager.ask(actor.id, message, args.data, context.signal, {
              overrides,
              detachOnMainCeiling: isInteractiveMain(context.extensionContext),
            }),
            context,
            this.agentToolPreviewEnabled,
          );
        }
        if (!participant) throw new Error(`Fabric actor ${actor!.id} has no live execution owner`);
        if (!participant.capabilities.includes("ask")) {
          throw new Error(`Fabric actor owner ${participant.ownerHostId} does not support remote ask`);
        }
        const binding = actor ? this.actorManager.resolveBinding(actor.id, overrides) : overrides;
        const needsBinding = Boolean(binding.model || binding.thinking);
        if (needsBinding && !participant.capabilities.includes("actor-bindings")) {
          throw new Error(`Fabric actor owner ${participant.ownerHostId} does not support session bindings`);
        }
        if (!this.control || participant.controlProtocol === "legacy") {
          throw new Error(`Fabric actor owner ${participant.ownerHostId} has no result control channel`);
        }
        return this.control.requestResult<FabricActorMessage>(
          participant.ownerHostId,
          participant.id,
          "ask",
          {
            message,
            ...(args.data === undefined ? {} : { data: args.data }),
            ...(needsBinding ? { binding } : {}),
          },
          participant.ownerIdentityId,
          {
            timeoutMs: (actor?.timeoutMs ?? this.manager.config.timeoutMs) +
              REMOTE_ASK_ACK_GRACE_MS,
            ...(context.signal ? { signal: context.signal } : {}),
            routedRemoteHost: participant.remoteHost ?? null,
            detachOnMainCeiling: isInteractiveMain(context.extensionContext),
          },
        );
      }
      case "tell":
        return this.routeMessage(
          String(args.id),
          String(args.message),
          args.data,
          "followUp",
          context,
          { binding: actorRunBinding(args) },
        );
      case "steer":
        return this.routeMessage(
          String(args.id),
          String(args.message),
          args.data,
          "steer",
          context,
        );
      case "followUp":
        return this.routeMessage(
          String(args.id),
          String(args.message),
          args.data,
          "followUp",
          context,
        );
      case "setSteeringMode":
        return this.manager.setSteeringMode(String(args.id), this.#steeringMode(args.mode));
      case "setFollowUpMode":
        return this.manager.setFollowUpMode(String(args.id), this.#steeringMode(args.mode));
      case "compact": {
        const id = String(args.id);
        let status: ReturnType<typeof this.manager.status>;
        try {
          status = this.manager.status(id);
        } catch (error) {
          // An actor is not a task agent (smarty-dev#1439): point at the call that helps.
          if (!(error instanceof Error && /Unknown Fabric agent/.test(error.message))) throw error;
          let actor: FabricActorInfo | undefined;
          try { actor = this.actorManager.status(id); } catch { /* not an actor either */ }
          if (!actor) throw error;
          throw new Error(
            `${actor.name} (${actor.id}) is a Fabric actor, not a task agent: agents.compact compacts a running task agent. ` +
            "To start an actor on a fresh session, use agents.resetSession({ id }).",
          );
        }
        context.activity?.({ type: "entity", id, kind: "agent", name: status.name });
        const instructions = typeof args.instructions === "string" ? args.instructions : undefined;
        const result = this.manager.compact(id, instructions);
        context.activity?.({
          type: "progress",
          message: `Compaction enqueued for agent ${id.slice(0, 8)} (advisory; commits at the child's next turn boundary)`,
        });
        return result;
      }
      case "actorStatus":
        return this.actorManager.status(String(args.id));
      case "instructions": {
        const actor = this.actorManager.status(String(args.id));
        const { instructions } = this.actorManager.definition(actor.id);
        return {
          id: actor.id,
          name: actor.name,
          instructions,
          instructionsDigest: createHash("sha256").update(instructions).digest("hex"),
          instructionsLength: instructions.length,
        };
      }
      case "actors":
        return args.scope === "global" ? this.globalActors.list() : this.#actorsWithLiveState();
      case "messages": {
        const actor = this.actorManager.status(String(args.id));
        return this.actorManager.messages(
          actor.id,
          typeof args.limit === "number" ? args.limit : 50,
        );
      }
      case "setModel": {
        const id = String(args.id);
        const model = typeof args.model === "string" ? args.model.trim() : "";
        const target = this.#resolveActorTarget(id);
        const ownsActor = target.actor ? this.actorManager.owns(target.actor.id) : false;
        const runner = target.actor?.runner ?? target.participant!.runner;
        const resolvedModel = model && ownsActor
          ? (await this.#resolvePiModelArgs({ model }, context, runner)).model as string
          : model || undefined;
        return this.actorManager.setModel(
          id,
          resolvedModel,
          args.scope === "project" ? "project" : "session",
        );
      }
      case "setThinking":
        return this.actorManager.setThinking(
          String(args.id),
          typeof args.thinking === "string" ? args.thinking : undefined,
          args.scope === "project" ? "project" : "session",
        );
      case "setTools": {
        const tools = stringArray(args.tools) ?? [];
        if (args.scope === "global") {
          return this.globalActors.update(String(args.id), { tools });
        }
        return this.actorManager.setTools(String(args.id), tools);
      }
      case "setNice": {
        const nice = parseAgentNice(args.nice);
        if (nice === undefined) throw new Error("nice is required (an integer 0-19)");
        if (args.scope === "global") return this.globalActors.update(String(args.id), { nice });
        return this.actorManager.setNice(String(args.id), nice);
      }
      case "setInferenceContext": {
        validateActorInferenceContext(args.inferenceContext);
        if (args.inferenceContext === undefined) throw new Error("inferenceContext is required");
        if (args.scope === "global") {
          return this.globalActors.update(String(args.id), { inferenceContext: args.inferenceContext });
        }
        return this.actorManager.setInferenceContext(String(args.id), args.inferenceContext);
      }
      case "setCoalesceKey": {
        const coalesceKey = args.coalesceKey === null ? null : args.coalesceKey;
        if (coalesceKey !== null) {
          validateActorCoalesceKey(coalesceKey);
          if (coalesceKey === undefined) throw new Error("coalesceKey is required (a dotted path, or null to clear)");
        }
        if (args.scope === "global") return this.globalActors.update(String(args.id), { coalesceKey });
        return this.actorManager.setCoalesceKey(String(args.id), coalesceKey);
      }
      case "setActivationFilter": {
        if (args.activationFilter === undefined) throw new Error("activationFilter is required (a list of presets or rules, or null to clear)");
        const activationFilter = args.activationFilter === null ? null : normalizeActorActivationFilter(args.activationFilter);
        if (args.scope === "global") return this.globalActors.update(String(args.id), { activationFilter });
        return this.actorManager.setActivationFilter(String(args.id), activationFilter);
      }
      case "setEvents": {
        const events = Array.isArray(args.events)
          ? args.events.filter(
              (event): event is FabricActorHostEvent => isFabricActorHostEvent(event),
            )
          : [];
        return this.actorManager.setEvents(String(args.id), events);
      }
      case "setDeliveryPolicy": {
        const delivery = args.delivery as FabricActorDelivery;
        if (typeof args.triggerTurn !== "boolean") {
          throw new Error("setDeliveryPolicy requires explicit triggerTurn: true or false");
        }
        const triggerTurn = args.triggerTurn;
        if (args.scope === "global") {
          return this.globalActors.update(String(args.id), { delivery, triggerTurn });
        }
        return this.actorManager.setDeliveryPolicy(String(args.id), delivery, triggerTurn);
      }
      case "clearMessages":
        return this.actorManager.clearMessages(String(args.id));
      case "resetSession":
        return this.actorManager.resetSession(String(args.id));
      case "remove": {
        if (args.scope === "global") return this.globalActors.remove(String(args.id));
        const cleanup = this.actorManager.cleanupObligation(String(args.id));
        if (cleanup) {
          if (this.actorManager.owns(cleanup.id)) return this.actorManager.remove(cleanup.id);
          if (cleanup.residency !== "durable") throw new Error("Only the owning host can remove this actor");
          return this.residency
            ? this.residency.removeActor(cleanup.id)
            : this.#residentActorClient().removeActor(cleanup.id);
        }
        let target: { actor?: FabricActorInfo; participant?: FabricParticipantInfo };
        try {
          target = this.#resolveActorTarget(String(args.id));
        } catch (error) {
          // A template is not a live actor (smarty-dev#918): say how to remove it.
          const template = this.globalActors.resolve(String(args.id));
          if (!template) throw error;
          throw new Error(`${error instanceof Error ? error.message : String(error)}. ` +
            `${template.name} (${template.id}) is a global template: remove it with agents.remove({ id, scope: "global" })`);
        }
        const { actor, participant } = target;
        if (actor && this.actorManager.owns(actor.id)) return this.actorManager.remove(actor.id);
        const residency = actor?.residency ?? participant?.residency;
        if (residency !== "durable") throw new Error("Only the owning host can remove this actor");
        const id = actor?.id ?? participant!.id;
        return this.residency
          ? this.residency.removeActor(id)
          : this.#residentActorClient().removeActor(id);
      }
      case "setInstructions": {
        const id = String(args.id);
        const instructions = String(args.instructions);
        const global = args.scope === "global";
        // smarty-dev#2340: refuse a >80% shrink unless the caller opts into replace: true.
        const current = global ? this.globalActors.resolve(id)?.instructions : this.actorManager.instructions(id);
        if (args.replace !== true && current !== undefined && instructions.length * 5 < current.length) {
          throw new Error(
            `Refusing setInstructions: new instructions (${instructions.length} chars) are more than 80% shorter than the current ${current.length} chars; pass replace: true to replace them`,
          );
        }
        if (global) return this.globalActors.update(id, { instructions });
        return this.actorManager.setInstructions(id, instructions);
      }
      case "import": {
        const key =
          typeof args.id === "string" && args.id.trim()
            ? args.id.trim()
            : typeof args.name === "string" && args.name.trim()
              ? args.name.trim()
              : "";
        if (!key) throw new Error("Import requires a template id or name");
        const def = this.globalActors.resolve(key);
        if (!def) throw new Error(`Unknown global actor: ${key}`);
        const as =
          typeof args.as === "string" && args.as.trim() ? args.as.trim() : undefined;
        const request = this.globalActors.toRequest(def, as);
        const resolvedRequest = request.model
          ? {
              ...request,
              model: (await this.#resolvePiModelArgs(
                { model: request.model },
                context,
                request.runner ?? this.manager.config.runner,
              )).model as string,
            }
          : request;
        const actor = await this.#createActor(resolvedRequest);
        this.participants.scheduleRefresh();
        context.activity?.({ type: "entity", id: actor.id, kind: "actor", name: actor.name });
        return actor;
      }
      case "export": {
        // Agents called export to read an actor and left stray global templates (smarty-dev#918).
        if (args.write !== true) {
          throw new Error(
            "agents.export writes a global template; pass write: true to confirm. To read an actor's instructions, use agents.instructions({ id }).",
          );
        }
        const actor = this.actorManager.status(String(args.id));
        const overwrite = args.overwrite === true;
        const def = this.actorManager.definition(actor.id);
        return this.globalActors.create(def, overwrite);
      }
      case "log": {
        const id = String(args.id);
        const type = args.type === "run" || args.type === "all" ? args.type : "session";
        const lines = typeof args.lines === "number" ? args.lines : 200;
        const runId = typeof args.runId === "string" ? args.runId : undefined;
        const before = typeof args.before === "number" ? args.before : undefined;
        try {
          const actor = this.actorManager.status(id);
          return this.actorManager.readLog(actor.id, {
            type,
            lines,
            ...(runId ? { runId } : {}),
            ...(before !== undefined ? { before } : {}),
          });
        } catch (error) {
          if (!(error instanceof Error && /Unknown Fabric actor/.test(error.message))) throw error;
          /* not an actor — fall through to agent */
        }
        if (this.residency?.hasAgent(id)) {
          return this.residency.readAgentLog(id, {
            lines,
            ...(before !== undefined ? { before } : {}),
          });
        }
        return this.manager.readLog(id, { lines, ...(before !== undefined ? { before } : {}) });
      }
      default:
        throw new Error(`Unknown agents action: ${actionName}`);
    }
  }

  async routeMessage(
    id: string,
    message: string,
    data: unknown,
    kind: "steer" | "followUp",
    context?: FabricInvocationContext,
    options: {
      from?: MeshIdentity;
      triggerTurn?: boolean;
      binding?: FabricActorRunBinding;
    } = {},
  ): Promise<FabricAgentMessageResult> {
    // Host-authored lifecycle routing has no sender invocation/history. Check
    // only model sends, before *all* local/actor/remote routing branches.
    if (!context) return this.#router.routeMessage(id, message, data, kind, context, options);
    const checked = await outgoingMessageNotice(message, context, this.actorManager.identity.id);
    const result = await deliverWithMessageNotice(message, checked,
      text => this.#router.routeMessage(id, text, data, kind, context, options));
    return checked.notice ? { ...result, notice: checked.notice } : result;
  }

  /** Flush pending coalesced lifecycle deliveries; used by tests and shutdown. */
  flushLifecycleDeliveries(): Promise<void> {
    return this.#lifecycleScheduler.flushAll();
  }

  async deliverLifecycle(
    subscription: FabricLifecycleSubscription,
    event: FabricLifecycleEvent,
  ): Promise<void> {
    // Buffered per target: bursts of run completions coalesce into a single
    // orchestrator wake turn instead of one full run per event (#85). steer
    // deliveries bypass the buffer inside the scheduler.
    this.#lifecycleScheduler.schedule(subscription.to, { subscription, event });
  }

  async acceptControl(
    command: FabricControlCommand,
    from: MeshIdentity,
    signal?: AbortSignal,
  ): Promise<FabricControlAcceptance> {
    return this.#router.acceptControl(command, from, signal);
  }

  #resolveActorTarget(id: string): {
    actor?: FabricActorInfo;
    participant?: FabricParticipantInfo;
  } {
    return this.#router.resolveActorTarget(id);
  }

  async #createActor(request: FabricActorRequest): Promise<FabricActorInfo> {
    // Also freeze imported templates before any resident host sees the request.
    const extensions = request.extensions ?? true;
    const kernel = this.manager.resolveKernel({ ...request, extensions });
    const { kernel: _requestedKernel, ...baseRequest } = request;
    request = {
      ...baseRequest,
      runner: request.runner ?? this.manager.config.runner,
      extensions,
      ...(kernel ? { kernel, pythonRuntime: this.manager.resolvePythonRuntime(request.pythonRuntime) } : {}),
    };
    if (request.residency !== "durable") return this.actorManager.create(request);
    if (!this.residency) return this.#residentActorClient().createActor(request);

    await this.residency.ensureHost();
    let actor: FabricActorInfo;
    try {
      actor = await this.actorManager.create(request);
    } catch (error) {
      if (!(error instanceof ActorRegistryOwnershipError)) throw error;
      return this.residency.createActor(request);
    }
    await this.#activateDurableActor(actor);
    return actor;
  }

  /**
   * Actors, with the owner's live state for those another host runs: the registry says only
   * idle or stopped, so a stopped actor still ending a run looked finished (smarty-dev#2184 item 8).
   */
  #actorsWithLiveState(): FabricActorInfo[] {
    return this.actorManager.list().map((actor) => {
      if (this.actorManager.owns(actor.id)) return actor;
      const live = this.participants.get(actor.id);
      if (!live || live.stale || live.kind !== "actor") return actor;
      const now = Date.now();
      const removal = live.actorRemoval ?? actor.removal;
      const run = live.actorRun;
      const runId = removal?.runId ?? run?.id;
      const runAge = formatAge(now - (removal?.runStartedAt ?? run?.startedAt ?? removal?.requestedAt ?? now));
      return {
        ...actor,
        status: live.status as FabricActorInfo["status"],
        ...(run ? { inFlightRun: { ...run, ageS: Math.max(0, Math.round((now - run.startedAt) / 1_000)) } } : {}),
        ...(removal
          ? {
              removal: {
                ...removal,
                state: runId
                  ? `removal of ${actor.name} (${actor.id}) is pending behind its in-flight run ${runId} (${runAge})`
                  : `removal of ${actor.name} (${actor.id}) is pending (${runAge})`,
              },
            }
          : {}),
      };
    });
  }

  #residentActorClient(): ResidentActorClient {
    const client = ResidentActorClient.fromEnv();
    if (client) return client;
    throw new Error(
      "Durable residency requires a trusted project with Fabric mesh persistence enabled",
    );
  }

  #resident(): ResidencyClient {
    if (!this.residency) {
      throw new Error(
        "Durable residency requires a trusted project with Fabric mesh persistence enabled",
      );
    }
    return this.residency;
  }

  async #activateDurableActor(actor: FabricActorInfo): Promise<void> {
    const residency = this.#resident();
    await this.actorManager.cede(actor.id);
    await this.participants.refresh();
    try {
      await residency.ensureActor(actor.id);
    } catch (error) {
      try {
        await residency.removeActor(actor.id);
      } catch {
        this.actorManager.reclaim(actor.id);
      }
      await this.participants.refresh().catch(() => undefined);
      throw error;
    }
  }

  #listAgents(scopeValue: unknown): Array<AgentRunRecord | AgentHandleInfo | ReturnType<FabricParticipantSource["self"]>> {
    const scope = this.#participantScope(scopeValue, "local");
    // An actor's activation run is the actor at work, not an agent: listed, it read as a new
    // root-less agent named after the actor with its run id (smarty-dev#2184). agents.actors lists
    // the actor; the shared directory already omits these runs (agentParticipantRecords).
    if (scope === "local") return this.manager.list().filter((record) => !record.actorId);
    // Like agents.members: a mesh-dependent listing (project or lineage) during a write
    // stall is unknown, not short.
    const stalled = this.participants.writeStalled?.();
    if (stalled) throw stalled;
    const local = new Map<string, AgentRunRecord | AgentHandleInfo>();
    const append = (record: AgentRunRecord | AgentHandleInfo): void => {
      local.set(record.id, record);
      if ("nestedAgents" in record) {
        for (const nested of record.nestedAgents ?? []) append(nested);
      }
    };
    for (const record of this.manager.list()) append(record);
    for (const record of this.residency?.listAgents() ?? []) append(record);
    const listed = this.participants
      .list({ scope, kinds: ["agent"] })
      .map((participant) => local.get(participant.id) ?? participant);
    const seen = new Set(listed.map((record) => record.id));
    for (const record of this.residency?.listAgents() ?? []) {
      if (!seen.has(record.id)) listed.push(record);
    }
    return listed;
  }

  #participantAlias(value: string): string {
    const id = value.trim();
    return id === "main" ? this.mainAgent.id : id;
  }

  #participantScope(
    value: unknown,
    fallback: FabricParticipantScope,
  ): FabricParticipantScope {
    return value === "local" || value === "lineage" || value === "project" ? value : fallback;
  }

  async stopParticipant(id: string): Promise<unknown> {
    try {
      const result = await this.manager.stop(id);
      this.participants.scheduleRefresh();
      return result;
    } catch (error) {
      if (!(error instanceof Error && /Unknown Fabric agent/.test(error.message))) throw error;
    }
    try {
      const actor = this.actorManager.status(id);
      const ownership = this.participants.get(actor.id);
      if (!ownership || ownership.local) {
        const result = await this.actorManager.stop(actor.id);
        this.participants.scheduleRefresh();
        return result;
      }
    } catch (error) {
      if (!(error instanceof Error && /Unknown Fabric actor/.test(error.message))) throw error;
    }
    // A finished durable run has no participant after its host exits: return its result (smarty-dev#1882).
    // Only a settled run: a live host may still resume or retry a stopped attempt.
    const settled = this.residency?.settledAgent(id);
    if (settled) {
      this.residency!.acknowledgeCompletion(id);
      return settled;
    }
    const participant = this.participants.get(id);
    if (!participant) throw this.participants.writeStalled?.() ?? unknownParticipant(this.participants, id);
    if (!participant.capabilities.includes("stop")) {
      throw new Error(`Fabric participant ${id} cannot be stopped`);
    }
    if (!this.control) throw new Error("Fabric control plane is unavailable");
    const result = await this.control.request(
      participant.ownerHostId,
      participant.id,
      "stop",
      {},
      participant.ownerIdentityId,
      { routedRemoteHost: participant.remoteHost ?? null },
    );
    if (this.residency?.hasAgent(id)) this.residency.acknowledgeCompletion(id);
    return result;
  }

  #steeringMode(mode: unknown): "all" | "one-at-a-time" {
    if (mode === "all" || mode === "one-at-a-time") return mode;
    throw new Error(
      `Invalid steering mode: ${String(mode)} (expected "all" or "one-at-a-time")`,
    );
  }

  async close(): Promise<void> {
    this.#transcripts.clear();
    if (!this.ownsRuntime) return;
    await this.lifecycle.close();
    try {
      await this.actorManager.close();
    } finally {
      await this.manager.close();
    }
  }
}
