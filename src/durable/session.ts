// Worker-only: keep the SDK and durable engine out of Fabric's registration graph.
import { homedir } from "node:os";
import { join, resolve } from "node:path";
import type { AgentOptions, ThinkingLevel } from "@earendil-works/pi-agent-core";
import { clampThinkingLevel } from "@earendil-works/pi-ai/compat";
import type { Storage } from "@earendil-works/pi-durable";
import {
  AgentSession, convertToLlm, DefaultResourceLoader, getAgentDir, ModelRuntime,
  SessionManager, SettingsManager,
  type CreateAgentSessionOptions, type ExtensionRunner,
} from "@earendil-works/pi-coding-agent";
import { DurableAgent } from "./agent.js";

const absolute = (path: string) => resolve(path === "~" ? homedir() : path.startsWith("~/") ? join(homedir(), path.slice(2)) : path);

/**
 * Public-SDK host for a durable agent. AgentSession remains responsible for native
 * tool provenance/loadouts, extension tool hooks, request routing, and history.
 * Like createAgentSession, this does not bind extensions: the host calls
 * session.bindExtensions() with its mode/UI/error bindings before prompting.
 * Storage is borrowed; close settles this host but does not close its storage.
 */
export async function createDurableAgentSession(
  options: CreateAgentSessionOptions & { storage: Storage; runId: string },
): Promise<{ session: AgentSession; agent: DurableAgent; close(): Promise<void> }> {
  if (!options.runId.trim()) throw new Error("A durable session requires a non-empty runId");
  const cwd = absolute(options.cwd ?? options.sessionManager?.getCwd() ?? process.cwd());
  const agentDir = absolute(options.agentDir ?? getAgentDir());
  const modelRuntime = options.modelRuntime ?? await ModelRuntime.create({
    authPath: join(agentDir, "auth.json"), modelsPath: join(agentDir, "models.json"),
  });
  const settingsManager = options.settingsManager ?? SettingsManager.create(cwd, agentDir);
  const sessionManager = options.sessionManager ?? SessionManager.create(
    cwd, join(agentDir, "sessions", `--${cwd.replace(/^[/\\]/, "").replace(/[/\\:]/g, "-")}--`),
  );
  let resourceLoader = options.resourceLoader;
  if (!resourceLoader) {
    resourceLoader = new DefaultResourceLoader({ cwd, agentDir, settingsManager });
    await resourceLoader.reload();
  }

  const existing = sessionManager.buildSessionContext();
  const branch = sessionManager.getBranch();
  const hasHistory = existing.messages.length > 0;
  const hasThinking = branch.some(entry => entry.type === "thinking_level_change");
  // A virtual selection is recorded in model_change, not the physical response.
  const lastSelection = branch.slice().reverse().find(entry => entry.type === "model_change");
  const selected = lastSelection?.type === "model_change"
    ? modelRuntime.getModel(lastSelection.provider, lastSelection.modelId) : undefined;
  const saved = selected?.api === "pi-virtual" ? selected : existing.model
    ? modelRuntime.getModel(existing.model.provider, existing.model.modelId) : undefined;
  let model = options.model;
  if (!model && hasHistory && (lastSelection || existing.model)) {
    if (!saved || (saved.api !== "pi-virtual" && !modelRuntime.hasConfiguredAuth(saved.provider))) {
      throw new Error("Cannot restore durable session model; explicitly select an available model");
    }
    model = saved;
  }
  if (!model) {
    const available = await modelRuntime.getAvailable();
    const provider = settingsManager.getDefaultProvider();
    const modelId = settingsManager.getDefaultModel();
    model = provider || modelId
      ? available.find(candidate => (!provider || candidate.provider === provider) && (!modelId || candidate.id === modelId))
      : available[0];
    if (!model && (provider || modelId)) throw new Error(`Configured durable model is unavailable: ${provider ?? "*"}/${modelId ?? "*"}`);
  }
  if (!model) throw new Error("No available model for durable session; supply model and modelRuntime");
  const savedThinking = ["off", "minimal", "low", "medium", "high", "xhigh", "max"].includes(existing.thinkingLevel)
    ? existing.thinkingLevel as ThinkingLevel : "medium";
  const thinkingLevel = clampThinkingLevel(model, options.thinkingLevel ??
    (hasHistory ? (hasThinking ? savedThinking : settingsManager.getDefaultThinkingLevel() ?? "medium") : undefined) ??
    settingsManager.getModelThinkingLevel(model.provider, model.id) ?? settingsManager.getDefaultThinkingLevel() ?? "medium");

  const extensionRunnerRef: { current?: ExtensionRunner } = {};
  const agentOptions: AgentOptions = {
    initialState: { systemPrompt: "", model, thinkingLevel, tools: [], messages: existing.messages },
    convertToLlm: messages => {
      const converted = convertToLlm(messages);
      if (!settingsManager.getBlockImages()) return converted;
      return converted.map(message => {
        if ((message.role !== "user" && message.role !== "toolResult") || !Array.isArray(message.content)) return message;
        return { ...message, content: message.content.map(part => part.type === "image"
          ? { type: "text" as const, text: "Image reading is disabled." } : part) };
      });
    },
    transformContext: async messages => extensionRunnerRef.current?.emitContext(messages) ?? messages,
    streamFn: (requestModel, context, requestOptions = {}) => {
      const retry = settingsManager.getProviderRetrySettings();
      const idleTimeout = settingsManager.getHttpIdleTimeoutMs();
      const websocketConnectTimeoutMs = requestOptions.websocketConnectTimeoutMs ?? settingsManager.getWebSocketConnectTimeoutMs();
      const maxRetries = requestOptions.maxRetries ?? retry.maxRetries;
      return modelRuntime.streamSimple(requestModel, context, {
        ...requestOptions,
        timeoutMs: requestOptions.timeoutMs ?? retry.timeoutMs ?? (idleTimeout === 0 ? 2147483647 : idleTimeout),
        ...(websocketConnectTimeoutMs !== undefined ? { websocketConnectTimeoutMs } : {}),
        ...(maxRetries !== undefined ? { maxRetries } : {}),
        maxRetryDelayMs: requestOptions.maxRetryDelayMs ?? retry.maxRetryDelayMs,
        transformHeaders: async headers => {
          const transformed = headers;
          const runner = extensionRunnerRef.current;
          return runner?.hasHandlers("before_provider_headers")
            ? runner.emitBeforeProviderHeaders(transformed ?? {}) : transformed ?? {};
        },
      });
    },
    onPayload: async payload => {
      const runner = extensionRunnerRef.current;
      return runner?.hasHandlers("before_provider_request") ? runner.emitBeforeProviderRequest(payload) : payload;
    },
    onResponse: async response => {
      const runner = extensionRunnerRef.current;
      if (runner?.hasHandlers("after_provider_response")) {
        await runner.emit({ type: "after_provider_response", status: response.status, headers: response.headers });
      }
    },
    onProviderStreamEvent: async (data, requestModel) => {
      const runner = extensionRunnerRef.current;
      if (runner?.hasHandlers("provider_stream_event")) {
        await runner.emit({ type: "provider_stream_event", data, provider: requestModel.provider, api: requestModel.api, model: requestModel.id });
      }
    },
    sessionId: sessionManager.getSessionId(),
    steeringMode: settingsManager.getSteeringMode(), followUpMode: settingsManager.getFollowUpMode(),
    transport: settingsManager.getTransport(),
    ...(settingsManager.getThinkingBudgets() ? { thinkingBudgets: settingsManager.getThinkingBudgets()! } : {}),
    ...(settingsManager.getProviderRetrySettings().maxRetryDelayMs !== undefined
      ? { maxRetryDelayMs: settingsManager.getProviderRetrySettings().maxRetryDelayMs! } : {}),
  };
  // Harness owns its adapter lifecycle, not the worker's lease/storage lifetime.
  // Bind methods to the original instance (storage adapters may use private fields).
  const borrowedStorage = new Proxy(options.storage, {
    get(target, property) {
      if (property === "close") return async () => {};
      const value = Reflect.get(target, property, target);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
  const agent = new DurableAgent({ ...agentOptions, models: modelRuntime, storage: borrowedStorage, runId: options.runId });
  try {
    if (!hasHistory) sessionManager.appendModelChange(model.provider, model.id);
    if (!hasHistory || !hasThinking) sessionManager.appendThinkingLevelChange(thinkingLevel);
    const excluded = new Set(options.excludeTools);
    const session = new AgentSession({
      agent, cwd, sessionManager, settingsManager, resourceLoader, modelRuntime,
      extensionRunnerRef,
      ...(options.customTools ? { customTools: options.customTools } : {}),
      ...(options.scopedModels ? { scopedModels: options.scopedModels } : {}),
      ...(options.sessionStartEvent ? { sessionStartEvent: options.sessionStartEvent } : {}),
      initialActiveToolNames: (options.tools ?? (options.noTools ? [] : settingsManager.getDefaultTools() ?? ["read", "bash", "edit", "write"]))
        .filter(name => !excluded.has(name)),
      usesDefaultTools: options.tools === undefined && !options.noTools,
      ...(options.tools || options.noTools === "all" ? { allowedToolNames: options.tools ?? [] } : {}),
      ...(options.excludeTools ? { excludedToolNames: options.excludeTools } : {}),
    });
    agent.recoverHistory = messages => {
      // Do not redeliver message_end: it runs extension hooks and accounts usage.
      // Match against source entries as well as the projection: compaction and
      // context edits can hide a committed message without making it missing.
      const key = (message: unknown) => JSON.stringify(message);
      const projected = sessionManager.buildSessionContext().messages;
      const branch = sessionManager.getBranch();
      const counts = (values: string[]) => {
        const result = new Map<string, number>();
        for (const value of values) result.set(value, (result.get(value) ?? 0) + 1);
        return result;
      };
      const present = counts(projected.map(key));
      for (const [value, count] of counts(branch.flatMap(entry => entry.type === "message" ? [key(entry.message)] : []))) {
        present.set(value, Math.max(present.get(value) ?? 0, count));
      }
      // Count occurrences, not just values: repeated identical messages are valid.
      const seen = new Map<string, number>();
      let end = 0;
      messages.forEach((message, index) => {
        const value = key(message); const count = (seen.get(value) ?? 0) + 1;
        seen.set(value, count);
        if ((present.get(value) ?? 0) >= count) end = index + 1;
      });
      const missing = messages.slice(end);
      // A deliberate branch switch must not resurrect the abandoned branch.
      const branchIds = new Set(branch.map(entry => entry.id));
      const elsewhere = new Set(sessionManager.getEntries().flatMap(entry =>
        entry.type === "message" && !branchIds.has(entry.id) ? [key(entry.message)] : []));
      if (!missing.some(message => elsewhere.has(key(message)))) {
        for (const message of missing) {
          if (message.role === "compactionSummary") {
            sessionManager.appendCompaction(message.summary, null, message.tokensBefore);
          } else if (message.role === "branchSummary") {
            sessionManager.branchWithSummary(sessionManager.getLeafId(), message.summary);
          } else {
            sessionManager.appendMessage(message);
          }
        }
      }
      // Keep the native projection (including context edits/compaction) canonical.
      agent.state.messages = sessionManager.buildSessionContext().messages;
    };
    const steer = session.steer.bind(session);
    session.steer = async (...args) => { const result = await steer(...args); await agent.awaitControls(); return result; };
    const followUp = session.followUp.bind(session);
    session.followUp = async (...args) => { const result = await followUp(...args); await agent.awaitControls(); return result; };
    let closing: Promise<void> | undefined;
    return { session, agent, close: () => closing ??= (async () => {
      try {
        await session.abort();
        await session.extensionRunner.emit({ type: "session_shutdown", reason: "quit" });
      } finally {
        try { session.dispose(); } finally { await agent.close(); }
      }
    })() };
  } catch (error) {
    await agent.close();
    throw error;
  }
}
