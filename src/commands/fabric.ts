import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import type { AutocompleteItem } from "@earendil-works/pi-tui";
import type { CapturedToolCatalog } from "../capture/catalog.js";
import type { FabricState } from "../fabric-state.js";
import { resolveAgentDir } from "../core/agent-dir.js";
import { armFabricPrewalkSession } from "../prewalk/arm.js";
import { restoreBorrowedInPlaceMain } from "../prewalk/return.js";
import { truncateMiddle } from "../util.js";
import { setActiveCompiledSurface } from "../entropy/active.js";
import type { FabricUiController } from "../ui/controller.js";
import { FABRIC_CONVERSATION_SHORTCUT } from "../ui/conversation-shortcut.js";
import { safeText } from "../ui/format.js";
import {
  FABRIC_PEER_AWAIT_SETTLE_EVENT,
  FABRIC_PEER_CARDS_EVENT,
  FABRIC_PREWALK_REQUEST_EVENT,
  readFabricPeerAwaitSettleRequestV1,
  readFabricPeerCardsRequestV1,
  readFabricPrewalkRequestV1,
  type FabricPrewalkRequestResultV1,
} from "../protocol.js";
import { awaitPeerSettle, buildPeerCards } from "../topology/peer-settle.js";

export interface FabricCommandDeps {
  state: FabricState;
  fabricUi: FabricUiController;
  capturedTools: CapturedToolCatalog;
  applyFabricMode: () => void;
  suspendToolCapture: () => void;
  autoArmPrewalk?: (context: ExtensionContext) => Promise<void>;
  refreshCodePreviewSettings?: () => void;
  refreshToolDisplay?: () => void;
  refreshPrincipalView?: (context: ExtensionContext) => void;
}

const resolvePrewalkModel = async (
  state: FabricState,
  context: ExtensionContext,
): Promise<string | undefined> => {
  const configured = state.config.prewalk.model?.trim();
  if (configured) {
    if (configured.includes("/")) return configured;
    context.ui.notify(
      "prewalk.model must use provider/model form.",
      "error",
    );
    return undefined;
  }
  let models: Array<{ provider: string; id: string; name?: string }> = [];
  try {
    models = context.modelRegistry.getAvailable();
  } catch {
    models = [];
  }
  const keys = models
    .map((model) => `${model.provider}/${model.id}`)
    .sort((left, right) => left.localeCompare(right));
  if (keys.length === 0) {
    context.ui.notify(
      "Prewalk needs an explicit Pi executor model. Configure prewalk.model in /fabric settings.",
      "error",
    );
    return undefined;
  }
  if (!context.hasUI) {
    context.ui.notify(
      "Prewalk needs prewalk.model in non-interactive mode.",
      "error",
    );
    return undefined;
  }
  if (typeof context.ui.custom === "function") {
    try {
      // First use only: the picker UI stays out of the startup graph (45-file budget).
      const [{ FabricModelSelector }, { buildModelSource }] = await Promise.all([
        import("../ui/fabric-model-selector.js"), import("../ui/model-picker.js"),
      ]);
      // undefined = host can't show the dialog; { model: undefined } = user cancelled.
      const picked = await context.ui.custom<{ model?: string | undefined } | undefined>(
        (_tui, theme, _keybindings, done) =>
          new FabricModelSelector({
            theme,
            source: buildModelSource(context.modelRegistry, resolveAgentDir()),
            currentValue: "",
            headerText:
              "Prewalk executor model. Fabric hands off at the next matching mutation boundary; Main continues on the picked model.",
            inheritRow: false,
            onSelect: (value) => done({ model: value }),
            onCancel: () => done({ model: undefined }),
          }),
      );
      if (picked !== undefined) return picked.model;
    } catch {}
  }
  return context.ui.select("Prewalk executor model", keys);
};

const armPrewalk = async (
  state: FabricState,
  context: ExtensionContext,
  pi: ExtensionAPI,
  task = "",
): Promise<FabricPrewalkRequestResultV1> => {
  if (state.config.prewalk.enabled === false) {
    const error = "Fabric prewalk is disabled; re-enable with /fabric prewalk --enable or /fabric settings.";
    context.ui.notify(error, "error");
    return { ok: false, error };
  }
  if (!state.config.fullCodeMode || state.config.schema.mode === "enforce") {
    const error = "Fabric prewalk requires full code mode and Schema enforce mode disabled.";
    context.ui.notify(error, "error");
    return { ok: false, error };
  }
  if (state.config.prewalk.mode === "trajectory" && !state.config.agents.enabled) {
    const error = "Trajectory prewalk requires enabled agents. Choose in-place mode or enable agents.";
    context.ui.notify(error, "error");
    return { ok: false, error };
  }
  const model = await resolvePrewalkModel(state, context);
  if (!model) return { ok: false, error: "Fabric prewalk was not armed." };

  await armFabricPrewalkSession(state, context, pi, {
    model,
    ...(task ? { task } : {}),
  });
  const modeLabel =
    state.config.prewalk.mode === "in-place"
      ? "Main will continue in place"
      : "the trajectory will move to a visible child executor";
  context.ui.notify(
    task
      ? `Fabric prewalk armed for the next matching Fabric boundary; ${modeLabel} with ${model}${state.config.prewalk.alwaysRearm ? "; always re-arm enabled" : ""}`
      : `Fabric prewalk armed for the next task; ${modeLabel} with ${model}${state.config.prewalk.alwaysRearm ? "; always re-arm enabled" : ""}`,
    "info",
  );
  // This is the principal's typed task, not a host-generated Fabric directive.
  if (task) pi.sendUserMessage(task);
  return { ok: true };
};

export function registerFabricCommand(pi: ExtensionAPI, deps: FabricCommandDeps): void {
  const { state, fabricUi } = deps;
  const unsubscribePrewalkRequests = pi.events?.on?.(FABRIC_PREWALK_REQUEST_EVENT, (value) => {
    const request = readFabricPrewalkRequestV1(value);
    if (!request || !request.claim()) return;
    void (async () => {
      try {
        await state.ensure(request.context);
        request.respond(await armPrewalk(state, request.context, pi));
      } catch (error) {
        request.respond({
          ok: false,
          error: error instanceof Error ? error.message : String(error),
        });
      }
    })();
  });
  if (unsubscribePrewalkRequests) {
    pi.on("session_shutdown", () => unsubscribePrewalkRequests());
  }

  // Peer queuing protocol (used by pi-queue-steer): enumerate live peer root
  // sessions and hold dispatch until they settle on the project mesh.
  const unsubscribePeerCards = pi.events?.on?.(FABRIC_PEER_CARDS_EVENT, (value) => {
    const request = readFabricPeerCardsRequestV1(value);
    if (!request || !request.claim()) return;
    void (async () => {
      try {
        await state.ensure(request.context);
        const stalled = state.writeStalled?.();
        if (stalled) {
          request.respond({ ok: false, error: stalled.message });
          return;
        }
        request.respond({ ok: true, cards: buildPeerCards(state.peerInfos()) });
      } catch (error) {
        request.respond({
          ok: false,
          error: error instanceof Error ? error.message : String(error),
        });
      }
    })();
  });
  const unsubscribePeerAwait = pi.events?.on?.(FABRIC_PEER_AWAIT_SETTLE_EVENT, (value) => {
    const request = readFabricPeerAwaitSettleRequestV1(value);
    if (!request || !request.claim()) return;
    void (async () => {
      try {
        await state.ensure(request.context);
        if (!state.config.mesh.enabled) {
          request.respond({ ok: false, error: "Fabric mesh is disabled; peers cannot be observed" });
          return;
        }
        request.respond(await awaitPeerSettle({
          poll: () => state.peerInfos(),
          stalled: () => state.writeStalled?.(),
          confirmedAt: () => state.participantsConfirmedAt?.(),
          ...(request.selector !== undefined ? { selector: request.selector } : {}),
          ...(request.settledForMs !== undefined ? { settledForMs: request.settledForMs } : {}),
          ...(request.signal ? { signal: request.signal } : {}),
          ...(request.update ? { onUpdate: request.update } : {}),
        }));
      } catch (error) {
        request.respond({
          ok: false,
          error: error instanceof Error ? error.message : String(error),
        });
      }
    })();
  });
  if (unsubscribePeerCards || unsubscribePeerAwait) {
    pi.on("session_shutdown", () => {
      unsubscribePeerCards?.();
      unsubscribePeerAwait?.();
    });
  }

  pi.registerShortcut?.(FABRIC_CONVERSATION_SHORTCUT, {
    description: "Open Fabric conversation or return to Main",
    handler: async (context) => {
      if (context.mode !== "tui") return;
      try {
        await state.ensure(context);
        await fabricUi.openConversation(context);
      } catch (error) {
        context.ui.notify(safeText(error instanceof Error ? error.message : String(error)), "error");
      }
    },
  });

  pi.registerShortcut?.("ctrl+alt+t", {
    description: "Inspect Fabric background shell tasks",
    handler: async (context) => {
      if (context.mode !== "tui") return;
      try { await state.ensure(context); await fabricUi.openTasks(context); }
      catch (error) { context.ui.notify(safeText(error instanceof Error ? error.message : String(error)), "error"); }
    },
  });

  pi.registerCommand("fabric", {
    description: "Open Fabric dashboard, chat or tasks, arm prewalk, reload, or manage agents and actors",
    getArgumentCompletions: (argumentPrefix: string): AutocompleteItem[] | null => {
      const subcommands = [
        "status",
        "dashboard",
        "tasks",
        "chat",
        "settings",
        "schema",
        "prewalk",
        "reload",
        "providers",
        "agents",
        "actors",
        "messages",
        "clear-messages",
        "events",
        "log",
        "export-log",
        "attach",
        "stop",
        "remove",
        "global",
        "import",
        "export",
        "kill",
        "repairs",
        "entropy",
        "decisions",
        "programs",
        "run",
      ];
      const idCommands = new Set([
        "messages",
        "clear-messages",
        "events",
        "log",
        "export-log",
        "attach",
        "stop",
        "remove",
        "kill",
      ]);
      const firstSpace = argumentPrefix.indexOf(" ");
      if (firstSpace < 0) {
        const matches = subcommands.filter((name) => name.startsWith(argumentPrefix));
        return matches.length > 0 ? matches.map((name) => ({ value: name, label: name })) : null;
      }
      const subcommand = argumentPrefix.slice(0, firstSpace);
      const idPrefix = argumentPrefix.slice(firstSpace + 1);
      if (!state.initialized) return null;
      if (subcommand === "tasks") {
        const matches = state.shellJobs?.list().filter(job => job.id.startsWith(idPrefix)) ?? [];
        return matches.map(job => ({ value: `tasks ${job.id}`, label: job.id.slice(0, 8), description: safeText(job.description ?? job.command).slice(0, 120) }));
      }
      if (subcommand === "chat") {
        const snapshot = fabricUi.snapshot();
        const seen = new Set<string>();
        const targets = [snapshot.main, ...snapshot.peers, ...snapshot.actors, ...snapshot.agents, ...(snapshot.participants ?? [])];
        const matches = targets.filter((target) => {
          if (seen.has(target.id)) return false;
          seen.add(target.id);
          return `${target.id} ${target.name}`.toLowerCase().includes(idPrefix.toLowerCase());
        });
        return matches.length > 0 ? matches.map((target) => ({
          value: target.id,
          label: safeText(target.name),
          description: safeText(`${target.status} · ${target.id.slice(0, 8)}`),
        })) : null;
      }
      if (subcommand === "schema") {
        const modes = ["off", "audit", "enforce"].filter((mode) => mode.startsWith(idPrefix));
        return modes.length > 0 ? modes.map((value) => ({ value, label: value })) : null;
      }
      if (subcommand === "import") {
        const items: AutocompleteItem[] = [];
        try {
          for (const template of state.globalActors.list()) {
            items.push({
              value: template.name,
              label: template.name,
              description: `global ${template.runner} template · ${template.id.slice(0, 8)}`,
            });
          }
        } catch {
          /* global registry not initialized */
        }
        const filtered = items.filter((item) => item.value.startsWith(idPrefix));
        return filtered.length > 0 ? filtered : null;
      }
      if (!idCommands.has(subcommand)) {
        if (subcommand === "export") {
          const items: AutocompleteItem[] = [];
          try {
            for (const actor of state.actors.list()) {
              items.push({
                value: actor.name,
                label: actor.name,
                description: `${actor.status} ${actor.runner} actor · ${actor.id.slice(0, 8)}`,
              });
            }
          } catch {
            /* actors not initialized */
          }
          const filtered = items.filter((item) => item.value.startsWith(idPrefix));
          return filtered.length > 0 ? filtered : null;
        }
        return null;
      }
      const items: AutocompleteItem[] = [];
      try {
        for (const actor of state.actors.list()) {
          items.push({
            value: actor.name,
            label: actor.name,
            description: `${actor.status} ${actor.runner} actor · ${actor.id.slice(0, 8)}`,
          });
        }
      } catch {
        /* actors not initialized */
      }
      try {
        for (const agent of state.agents.list()) {
          const short = agent.id.slice(0, 8);
          items.push({
            value: short,
            label: short,
            description: `${agent.status} ${agent.runner} agent · ${agent.name}`,
          });
        }
      } catch {
        /* agents not initialized */
      }
      const filtered = items.filter((item) => item.value.startsWith(idPrefix));
      return filtered.length > 0 ? filtered : null;
    },
    async handler(argumentsText, context) {
      let runFabricCommand: typeof import("./fabric-handler.js").runFabricCommand;
      try {
        ({ runFabricCommand } = await import("./fabric-handler.js"));
      } catch (error) {
        context.ui.notify(
          `Failed to load /fabric command: ${safeText(error instanceof Error ? error.message : String(error))}`,
          "error",
        );
        return;
      }
      // Only import failures are handled here; command errors retain their
      // existing behavior. Do not cache a rejected import: later calls may retry.
      // ponytail: keep existing eager helpers in their current chunks; passing
      // their references avoids four extra startup chunks from the lazy command edge.
      await runFabricCommand(pi, deps, argumentsText, context,
        (task) => armPrewalk(state, context, pi, task),
        { restoreBorrowedInPlaceMain, truncateMiddle, setActiveCompiledSurface });
    },
  });
}
