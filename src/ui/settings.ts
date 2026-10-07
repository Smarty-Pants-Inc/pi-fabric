import type { FabricState } from "../fabric-state.js";
import type { CapturedToolCatalog } from "../capture/catalog.js";
import type { ExtensionContext, Theme } from "@earendil-works/pi-coding-agent";
import { resolveAgentDir } from "../core/agent-dir.js";
import {
  type FabricConfigScope,
  loadFabricConfigForScope,
  saveFabricConfig,
} from "../config.js";
import { listCachedMcpServerNames, mcpDescriptorCachePath } from "../providers/mcp-descriptor-cache.js";
import { FabricSettingsComponent } from "./settings-component.js";
import {
  modelKey,
  buildModelSource,
  type ModelSource,
} from "./model-picker.js";
import {
  COMPACTION_THRESHOLD_SETTING_ID,
  compactionThresholdPartial,
  type CompactionThresholdSelection,
  buildPartial,
  summaryFor,
  coerceValue,
  unique,
} from "./settings-values.js";
import {
  populateClaudeModelSource,
  buildFabricSettingsItems,
} from "./settings-sections.js";
import type { SettingItem } from "@earendil-works/pi-tui";
import { openRpcFabricSettings } from "./settings-rpc.js";
import { imageSafeCustom } from "./image-overlays.js";

const ROOT_ITEM_IDS = [
  "fullCodeMode",
  "executor",
  "schema",
  "approvals",
  "mcp",
  "prewalk",
  "agents",
  "capture",
  "ui",
  "compaction",
  "memory",
  "retention",
  "mesh",
  "codePreview",
] as const;

const RELOAD_SECTIONS = new Set(["mesh", "agents", "mcp", "retention"]);

export interface FabricSettingsDeps {
  state: FabricState;
  applyFabricMode: () => void;
  capturedTools: CapturedToolCatalog;
  onConfigApplied?: (id: string) => void;
  reloadResources?: () => Promise<void>;
}

export async function openFabricSettings(
  context: ExtensionContext,
  deps: FabricSettingsDeps,
): Promise<void> {
  await deps.state.ensure(context);

  const agentDir = resolveAgentDir();
  const projectTrusted = context.isProjectTrusted();
  const configLocation = { cwd: context.cwd, agentDir, projectTrusted };
  let saveScope: FabricConfigScope = projectTrusted ? "project" : "global";
  let settingsConfig = loadFabricConfigForScope(configLocation, saveScope);
  let rootComponent: FabricSettingsComponent | undefined;
  const changedSections = new Set<string>();
  let dirty = false;

  const activeModelKey = context.model
    ? modelKey(context.model.provider, context.model.id)
    : undefined;

  const apply = (id: string, value: unknown): void => {
    const partial = id === COMPACTION_THRESHOLD_SETTING_ID && activeModelKey
      ? compactionThresholdPartial(activeModelKey, value as CompactionThresholdSelection)
      : buildPartial(id, value);
    try {
      saveFabricConfig(
        { cwd: context.cwd, agentDir, projectTrusted, scope: saveScope },
        partial,
      );
    } catch (error) {
      context.ui.notify(
        `Failed to save Fabric settings: ${error instanceof Error ? error.message : String(error)}`,
        "error",
      );
      return;
    }
    deps.state.reloadConfig(context);
    if (id.startsWith("memory.extractive.")) deps.state.pi.events.emit("pi-fabric:extractive-config-changed", {});
    // Render the persisted layers, not the live config: runtime-only
    // environment and session overrides must not change what this editor saves.
    Object.assign(
      settingsConfig,
      loadFabricConfigForScope(configLocation, saveScope),
    );
    deps.onConfigApplied?.(id);
    dirty = true;
    changedSections.add(id.split(".")[0] ?? id);
    const list = rootComponent?.settingsList;
    if (list) {
      for (const rootId of ROOT_ITEM_IDS) {
        list.updateValue(rootId, summaryFor(rootId, settingsConfig));
      }
    }
  };

  const persist = (id: string, newValue: string): void =>
    apply(id, coerceValue(id, newValue, settingsConfig));

  const keepVisibleCandidates = unique([
    "fabric_exec",
    ...deps.capturedTools.list().map((tool) => tool.name),
  ]);
  const modelSource = buildModelSource(context.modelRegistry, resolveAgentDir());
  // Native classifier catalog only; never reuse the chat model picker. No inference.
  const classifierModels = typeof context.modelRegistry.getModelsOfType === "function"
    ? context.modelRegistry.getModelsOfType("classifier").map((m) => `${m.provider}/${m.id}`) : [];
  let availableClassifierModels: string[] = [];
  try {
    if (typeof context.modelRegistry.getAvailableOfType === "function") {
      const available = await context.modelRegistry.getAvailableOfType("classifier", undefined, { signal: AbortSignal.timeout(3000) });
      availableClassifierModels = available.map((m) => `${m.provider}/${m.id}`);
    }
  } catch { /* Catalog remains selectable; missing auth uses explicit deterministic fallback. */ }
  const configuredClaudeModel = deps.state.config.agents.claude.model;
  const claudeModelSource: ModelSource = {
    models: configuredClaudeModel
      ? [{ provider: "claude", id: configuredClaudeModel.replace(/^claude\//, "") }]
      : [],
    lastUsed: {},
  };
  void populateClaudeModelSource(
    claudeModelSource,
    () => deps.state.agents.claudeModels(),
  ).catch((error: unknown) => {
    if (deps.state.config.agents.runner === "claude") {
      context.ui.notify(
        `Claude model discovery failed: ${error instanceof Error ? error.message : String(error)}`,
        "warning",
      );
    }
  });

  const itemsForScope = (scope: FabricConfigScope, theme: Theme): SettingItem[] => {
    settingsConfig = loadFabricConfigForScope(configLocation, scope);
    return buildFabricSettingsItems(theme, settingsConfig, apply, {
      keepVisibleCandidates,
      modelSource,
      claudeModelSource,
      classifierModels,
      availableClassifierModels,
      cachedMcpServers: listCachedMcpServerNames(mcpDescriptorCachePath(context.cwd)),
      ...(activeModelKey ? { activeModelKey } : {}),
    });
  };

  if (context.mode === "rpc") {
    await openRpcFabricSettings(context, {
      projectScopeAvailable: projectTrusted,
      getScope: () => saveScope,
      setScope: (scope) => {
        saveScope = scope;
      },
      itemsForScope: (scope) => itemsForScope(scope, context.ui.theme),
      persist,
    });
  } else if (context.mode !== "tui") {
    context.ui.notify("Fabric settings require an interactive UI", "warning");
    return;
  } else {
    await imageSafeCustom<void>(context.ui,
      (tui, theme, _keybindings, done) => {
        const component = new FabricSettingsComponent(
          theme,
          itemsForScope(saveScope, theme),
          persist,
          () => done(),
          {
            initialSaveScope: saveScope,
            projectScopeAvailable: projectTrusted,
            onSaveScopeChange: (scope) => {
              saveScope = scope;
              tui.requestRender();
            },
            itemsForSaveScope: (scope) => itemsForScope(scope, theme),
          },
        );
        rootComponent = component;
        return component;
      },
      {
        // Use the same image-safe overlay lifecycle as dashboard and chat.
        overlay: true,
        overlayOptions: {
          width: "94%",
          minWidth: 40,
          maxHeight: "90%",
          anchor: "center",
          margin: 1,
        },
      },
    );
  }

  if (dirty) {
    if (deps.state.kernelReloadRequired) {
      if (deps.reloadResources) {
        context.ui.notify("Kernel saved. Reloading Pi to switch execution and skill resources together.", "info");
        await deps.reloadResources();
        return;
      }
      context.ui.notify("Run /reload to apply the kernel change; the current kernel remains active.", "warning");
    }
    deps.applyFabricMode();
    const needsReload = [...changedSections].some((section) => RELOAD_SECTIONS.has(section));
    if (needsReload) {
      context.ui.notify(
        "Fabric settings saved. Run /fabric reload to apply mesh, agent, and MCP changes.",
        "info",
      );
    } else if (changedSections.has("schema")) {
      context.ui.notify(
        "Fabric settings saved. Schema changes take effect in the next session.",
        "info",
      );
    } else {
      context.ui.notify("Fabric settings saved.", "info");
    }
  }
}

// Preserve the public settings entrypoint while implementations stay cohesive.
export { executorMemoryLimitOptions } from "./settings-values.js";
export type { CompactionThresholdSelection } from "./settings-values.js";
export { compactionThresholdPartial } from "./settings-values.js";
export { parseBudgetValue } from "./settings-values.js";
export { parseFormattedNumericValue } from "./settings-values.js";
export type { FabricSettingsComponentOptions } from "./settings-component.js";
export { FabricSettingsComponent } from "./settings-component.js";
export { populateClaudeModelSource } from "./settings-sections.js";
export { buildFabricSettingsItems } from "./settings-sections.js";
