#!/usr/bin/env node
/** Native Pi RPC, with task-local in-memory settings and canonical auth storage.
 * The worker passes the SDK directory of its pinned Pi binary, not Fabric's peers.
 */
import path from "node:path";
import { pathToFileURL } from "node:url";
import type { CreateAgentSessionRuntimeFactory } from "@earendil-works/pi-coding-agent";
import { applyTaskRetryDefaults } from "./retry-profile.js";

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
applyHttpProxySettings(runtime.services.settingsManager.getGlobalSettings().httpProxy);
configureHttpDispatcher(runtime.services.settingsManager.getHttpIdleTimeoutMs());
await sdk.runRpcMode(runtime);
