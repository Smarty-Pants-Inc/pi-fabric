#!/usr/bin/env node
/** Native Pi RPC, with task-local in-memory settings and canonical auth storage.
 * The worker passes the SDK directory of its pinned Pi binary, not Fabric's peers.
 */
import path from "node:path";
import { pathToFileURL } from "node:url";
import type { CreateAgentSessionRuntimeFactory } from "@earendil-works/pi-coding-agent";
import { applyTaskRetryDefaults } from "./retry-profile.js";
import { installFabricResourcePin } from "./resource-pin.js";
import { installTerminalAnswerBoundary } from "./terminal-boundary.js";

const [sdkDirectory, scale, ...args] = process.argv.slice(2);
if (!sdkDirectory) throw new Error("Native Pi SDK directory is required");
const load = (file: string) => import(pathToFileURL(path.join(sdkDirectory, file)).href);
const sdk = await load("index.js") as typeof import("@earendil-works/pi-coding-agent");
const { parseArgs } = await load("cli/args.js") as typeof import("../../node_modules/@earendil-works/pi-coding-agent/dist/cli/args.js");
const { createSessionManager } = await load("main.js") as typeof import("../../node_modules/@earendil-works/pi-coding-agent/dist/main.js");
const { resolveProjectTrusted } = await load("core/project-trust.js") as typeof import("../../node_modules/@earendil-works/pi-coding-agent/dist/core/project-trust.js");
const { createProjectTrustContext } = await load("cli/project-trust.js") as typeof import("../../node_modules/@earendil-works/pi-coding-agent/dist/cli/project-trust.js");
const { builtInExtensions } = await load("extensions/index.js") as typeof import("../../node_modules/@earendil-works/pi-coding-agent/dist/extensions/index.js");
const { applyHttpProxySettings, configureHttpDispatcher } = await load("core/http-dispatcher.js") as typeof import("../../node_modules/@earendil-works/pi-coding-agent/dist/core/http-dispatcher.js");
const pinnedExtension = process.env.PI_FABRIC_PINNED_EXTENSION;
if (pinnedExtension) installFabricResourcePin(sdk.DefaultPackageManager, pinnedExtension);
const parsed = parseArgs(args);
if (parsed.mode !== "rpc") throw new Error("Task entry supports only native RPC mode");
const cwd = process.cwd();
const agentDir = sdk.getAgentDir(); // Unchanged PI_CODING_AGENT_DIR: same auth pathname and lock as Main.
const startupSettings = sdk.SettingsManager.create(cwd, agentDir, { projectTrusted: false });
const sessionManager = await createSessionManager(parsed, cwd, undefined, startupSettings);
const trustStore = new sdk.ProjectTrustStore(agentDir);
const trustByCwd = new Map<string, boolean>();
const extensionPaths = parsed.extensions?.map(file => path.resolve(cwd, file));
const factory: CreateAgentSessionRuntimeFactory = async ({ cwd, agentDir, sessionManager, sessionStartEvent, projectTrustContext }) => {
  const cachedTrust = trustByCwd.get(cwd);
  const resolveTrust = cachedTrust === undefined && sdk.hasTrustRequiringProjectResources(cwd);
  const settingsManager = sdk.SettingsManager.create(cwd, agentDir, { projectTrusted: cachedTrust ?? !resolveTrust });
  const services = await sdk.createAgentSessionServices({
    cwd, agentDir, settingsManager,
    modelRuntimeSignal: AbortSignal.timeout(15_000),
    extensionFlagValues: parsed.unknownFlags,
    resourceLoaderOptions: {
      ...(extensionPaths ? { additionalExtensionPaths: extensionPaths } : {}),
      ...(parsed.noExtensions !== undefined ? { noExtensions: parsed.noExtensions } : {}),
      ...(parsed.noSkills !== undefined ? { noSkills: parsed.noSkills } : {}),
      ...(parsed.noPromptTemplates !== undefined ? { noPromptTemplates: parsed.noPromptTemplates } : {}),
      ...(parsed.noThemes !== undefined ? { noThemes: parsed.noThemes } : {}),
      ...(parsed.noContextFiles !== undefined ? { noContextFiles: parsed.noContextFiles } : {}),
      ...(parsed.systemPrompt !== undefined ? { systemPrompt: parsed.systemPrompt } : {}),
      ...(parsed.appendSystemPrompt ? { appendSystemPrompt: parsed.appendSystemPrompt } : {}),
      extensionFactories: builtInExtensions,
    },
    ...(resolveTrust ? { resourceLoaderReloadOptions: {
      resolveProjectTrust: async ({ extensionsResult }) => {
        const trusted = await resolveProjectTrusted({ cwd, trustStore, extensionsResult,
          defaultProjectTrust: startupSettings.getDefaultProjectTrust(),
          projectTrustContext: projectTrustContext ?? createProjectTrustContext({ cwd, mode: "rpc", settingsManager: startupSettings, hasUI: false }),
        });
        trustByCwd.set(cwd, trusted);
        return trusted;
      },
    } } : {}),
  });
  // Resource reload resolves trust. Keep task defaults at the retry read boundary:
  // native queue-mode setters save/rebuild settings and discard applyOverrides.
  // Explicit settings still win; the fallback never enters the shared profile.
  applyTaskRetryDefaults(settingsManager, Number(scale));
  const model = sdk.resolveCliModel({ ...(parsed.model ? { cliModel: parsed.model } : {}), ...(parsed.thinking ? { cliThinking: parsed.thinking } : {}), modelRuntime: services.modelRuntime });
  if (model.error) throw new Error(model.error);
  const patterns = settingsManager.getEnabledModels();
  const scoped = patterns?.length ? await sdk.resolveModelScopeWithDiagnostics(patterns, services.modelRuntime, { signal: AbortSignal.timeout(15_000) }) : undefined;
  const created = await sdk.createAgentSessionFromServices({
    services, sessionManager,
    ...(sessionStartEvent ? { sessionStartEvent } : {}),
    ...(model.model ? { model: model.model } : {}),
    ...((parsed.thinking ?? model.thinkingLevel) ? { thinkingLevel: (parsed.thinking ?? model.thinkingLevel)! } : {}),
    ...(scoped ? { scopedModels: scoped.scopedModels } : {}),
    ...(parsed.tools ? { tools: parsed.tools } : {}),
    ...(parsed.noTools ? { noTools: "all" as const } : {}),
  });
  const diagnostics = [...services.diagnostics, ...(scoped?.diagnostics ?? []),
    ...services.resourceLoader.getExtensions().errors.map(({ path, error }) => ({ type: "error" as const, message: `Failed to load extension "${path}": ${error}` })),
    ...settingsManager.drainErrors().map(({ error }) => ({ type: "error" as const, message: error.message })),
  ];
  return { ...created, services, diagnostics };
};
process.env.PI_CODING_AGENT = "true";
process.env.AI_AGENT = "pi";
const runtime = await sdk.createAgentSessionRuntime(factory, { cwd: sessionManager.getCwd(), agentDir, sessionManager });
const failures = runtime.diagnostics.filter(diagnostic => diagnostic.type === "error");
if (failures.length) {
  await runtime.dispose();
  throw new Error(failures.map(diagnostic => diagnostic.message).join("\n"));
}
// The SDK entry also owns ordinary tasks with explicit retry settings. Preserve
// native launch controls and settings; the terminal fence is independent of retry.
if (args.includes("--no-auto-compaction")) {
  const session = runtime.session as typeof runtime.session & { disableAutoCompactionForProcess?: () => void };
  if (session.disableAutoCompactionForProcess) session.disableAutoCompactionForProcess();
  else {
    // Older SDKs lack the process-local switch. Never persist the CLI override
    // into the canonical shared profile.
    const settings = runtime.services.settingsManager;
    const compaction = settings.getCompactionSettings.bind(settings);
    settings.getCompactionSettings = () => ({ ...compaction(), enabled: false });
  }
}
if (process.env.PI_FABRIC_TERMINAL_TASK === "1") {
  const directory = process.env.PI_FABRIC_AGENT_RUN_DIR;
  const runId = process.env.PI_FABRIC_PARENT_RUN;
  if (!directory || !runId) throw new Error("Terminal task has no owned run identity");
  const totals = { turns: 0, toolCalls: 0, usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 } };
  // IPC can overtake stdout during a large final answer. Keep native attempt
  // accounting ahead of the terminal listener so its immutable status remains
  // complete even when the worker has not consumed earlier stream frames yet.
  runtime.session.agent.subscribe(event => {
    if (event.type === "turn_end") totals.turns++;
    if (event.type === "tool_execution_start") totals.toolCalls++;
    if (event.type === "message_end" && event.message.role === "assistant") {
      const usage = event.message.usage;
      for (const key of ["input", "output", "cacheRead", "cacheWrite"] as const) totals.usage[key] += usage[key];
      totals.usage.cost += usage.cost.total;
    }
  });
  installTerminalAnswerBoundary(runtime.session, directory, runId, receipt => {
    const message = [...runtime.session.agent.state.messages].reverse().find(message => message.role === "assistant");
    // Do not write around native RPC's serialized stdout queue: a large answer
    // may still be flushing. IPC preserves this receipt frame independently.
    const assistant = message?.role === "assistant" ? { usage: message.usage, model: message.model,
      provider: message.provider, timestamp: message.timestamp } : undefined;
    process.send?.({ type: "fabric_final_answer", runId, receiptId: receipt.id, assistant,
      totals: { ...totals, turns: totals.turns + 1 } });
  }, process.env.PI_FABRIC_REPLY_FILE, process.env.PI_FABRIC_TASK_RESUMING === "1");
  process.channel?.unref();
}
applyHttpProxySettings(runtime.services.settingsManager.getGlobalSettings().httpProxy);
configureHttpDispatcher(runtime.services.settingsManager.getHttpIdleTimeoutMs());
await sdk.runRpcMode(runtime);
