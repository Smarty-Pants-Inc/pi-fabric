import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { saveFabricConfig, type FabricIncomingMessageMode, type FabricPrincipalViewMode } from "../config.js";
import { resolveAgentDir } from "../core/agent-dir.js";
import type { FabricState } from "../fabric-state.js";
import { incomingMessagesCollapsed } from "./incoming-messages.js";
import { safeText } from "./format.js";

export const PRINCIPAL_VIEW_SHORTCUT = "ctrl+alt+p";

export const principalViewEnabled = (
  mode: FabricPrincipalViewMode,
  environment: NodeJS.ProcessEnv = process.env,
): boolean => mode === "on" || (mode === "auto" && incomingMessagesCollapsed("auto", environment));

export const principalViewIncomingMode = (mode: FabricPrincipalViewMode): FabricIncomingMessageMode =>
  principalViewEnabled(mode) ? "collapsed" : "expanded";

/** A display-only controller. Never edits prompts, messages, model thinking, or active tools. */
export const registerPrincipalView = (pi: ExtensionAPI, state: FabricState) => {
  let previousToolsExpanded: boolean | undefined;
  const enabled = () => principalViewEnabled(state.provisionalConfig().ui.principalView);
  const status = (context: ExtensionContext): void => {
    if (context.mode === "tui") context.ui.setStatus("fabric-principal-view", enabled() ? "principal view · Ctrl+Alt+P" : undefined);
  };
  const apply = (context: ExtensionContext): void => {
    if (context.mode !== "tui") return;
    const current = context.ui.getToolsExpanded();
    let expanded: boolean;
    if (enabled()) {
      previousToolsExpanded ??= current;
      expanded = false;
    } else {
      expanded = previousToolsExpanded ?? current;
      previousToolsExpanded = undefined;
    }
    // Native custom-message components rebuild only on an expansion change.
    // Round-trip through the public API, never poke Pi's transcript internals.
    if (current === expanded) context.ui.setToolsExpanded(!expanded);
    context.ui.setToolsExpanded(expanded);
    status(context);
  };
  const start = (context: ExtensionContext): void => {
    previousToolsExpanded = undefined; // A new session has its own native display state.
    if (enabled()) apply(context);
    else status(context);
  };
  const toggle = async (argument: string, context: ExtensionContext): Promise<void> => {
    const requested = argument.trim().toLowerCase();
    if (requested && requested !== "on" && requested !== "off" && requested !== "auto") {
      context.ui.notify("Usage: /principal-view [on|off|auto] (no argument toggles)", "warning");
      return;
    }
    try {
      // Config bootstrap only: changing transcript presentation must not start
      // the optional runtime, mesh, actors, or provider discovery.
      if (!state.bootstrapped) await state.bootstrap(context);
      const mode: FabricPrincipalViewMode = requested === "auto" ? "auto" : requested === "on" ? "on" : requested === "off" ? "off" : enabled() ? "off" : "on";
      const projectTrusted = context.isProjectTrusted();
      const saved = saveFabricConfig({ cwd: context.cwd, agentDir: resolveAgentDir(), projectTrusted,
        scope: projectTrusted ? "project" : "global" }, { ui: { principalView: mode } });
      state.reloadConfig(context);
      apply(context);
      context.ui.notify(`Principal view ${enabled() ? "on" : "off"}${mode === "auto" ? " (auto)" : ""} (${saved.scope}: ${saved.path}). Display only; Ctrl+O expands incoming/tool output.`, "info");
    } catch (error) {
      context.ui.notify(`Failed to save principal view: ${safeText(error instanceof Error ? error.message : String(error))}`, "error");
    }
  };
  pi.registerCommand("principal-view", {
    description: "Toggle principal view: collapse Fabric chatter and tool output (display only)",
    getArgumentCompletions: prefix => {
      const modes = ["on", "off", "auto"];
      // A complete argument (or no argument for toggle) should submit on Enter,
      // not require another Enter to accept an identical completion first.
      if (!prefix || modes.includes(prefix.trim())) return null;
      const matches = modes.filter(value => value.startsWith(prefix)).map(value => ({ value, label: value }));
      return matches.length ? matches : null;
    },
    handler: toggle,
  });
  pi.registerShortcut?.(PRINCIPAL_VIEW_SHORTCUT, {
    description: "Toggle principal view",
    handler: async context => { if (context.mode === "tui") await toggle("", context); },
  });
  return { apply, start };
};
