import type { ExtensionAPI, ExtensionCommandContext } from "@earendil-works/pi-coding-agent";
import type { FabricCommandDeps } from "./fabric.js";
import type { FabricActorHostEvent } from "../actors/types.js";
import type { RepairStatus } from "../repairs/types.js";
import { resolveAgentDir } from "../core/agent-dir.js";
import { saveFabricConfig } from "../config.js";
import { formatForeground } from "../core/foreground-tools.js";
import fs from "node:fs";
import path from "node:path";

type CommandHelpers = {
  restoreBorrowedInPlaceMain: typeof import("../prewalk/return.js").restoreBorrowedInPlaceMain;
  truncateMiddle: typeof import("../util.js").truncateMiddle;
  setActiveCompiledSurface: typeof import("../entropy/active.js").setActiveCompiledSurface;
};

const extractContentText = (content: unknown): string => {
  if (typeof content === "string") return content;
  if (Array.isArray(content)) {
    return content
      .map((part) => {
        if (typeof part !== "object" || part === null) return "";
        const p = part as Record<string, unknown>;
        return typeof p.text === "string" ? p.text : typeof p.type === "string" ? p.type : "";
      })
      .filter(Boolean)
      .join(" ");
  }
  return "";
};

const summarizeLogLine = (entry: unknown, truncateMiddle: CommandHelpers["truncateMiddle"]): string => {
  if (typeof entry !== "object" || entry === null) return truncateMiddle(String(entry), 200);
  const record = entry as Record<string, unknown>;
  const type = typeof record.type === "string" ? record.type : undefined;
  const tool = typeof record.toolName === "string" ? record.toolName : undefined;
  // Pi session lines and worker message_end both wrap a { role, content } message.
  const msg = record.message;
  if (typeof msg === "object" && msg !== null && !Array.isArray(msg)) {
    const m = msg as Record<string, unknown>;
    const role = typeof m.role === "string" ? m.role : "message";
    const model = typeof m.model === "string" ? m.model : undefined;
    const text = extractContentText(m.content);
    const body = (text || JSON.stringify(m)).replace(/\s+/g, " ");
    return `${role}${model ? ` [${model}]` : ""}: ${truncateMiddle(body, 160)}`;
  }
  if (type) {
    const bits = [type];
    if (tool) bits.push(tool);
    const model = typeof record.modelId === "string" ? record.modelId : undefined;
    const provider = typeof record.provider === "string" && !model ? record.provider : undefined;
    if (provider) bits.push(provider);
    if (model) bits.push(model);
    return bits.join(" ");
  }
  return truncateMiddle(JSON.stringify(record), 160);
};

const formatRepairStatus = (status: RepairStatus): string => {
  const top = status.fingerprints
    .slice(0, 8)
    .map((entry) => `  ${entry.count}× ${entry.fingerprint}`)
    .join("\n");
  const rows = status.repairs
    .slice(0, 12)
    .map((repair) =>
      repair.kind === "keyAlias"
        ? `  ${repair.ref}: ${repair.from} → ${repair.to}`
        : `  ${repair.provider}.${repair.from} → ${repair.to}`,
    )
    .join("\n");
  return [
    `repairs: ${status.enabled ? "on" : "off"} · digest ${status.catalogDigest.slice(0, 12) || "none"}`,
    `table: ${status.repairCount} · apply hits ${status.applyHits} · invocation ${status.invocationErrors} · effect dropped ${status.effectDropped}`,
    status.repairs.length > 0 ? `current:\n${rows}` : "current: (empty)",
    ...(status.storeError ? [`store: ${status.storeError}`] : []),
    top ? `fingerprints:\n${top}` : "fingerprints: (none this session)",
  ].join("\n");
};

export async function runFabricCommand(
  pi: ExtensionAPI,
  deps: FabricCommandDeps,
  argumentsText: string,
  context: ExtensionCommandContext,
  armPrewalk: (task: string) => Promise<unknown>,
  { restoreBorrowedInPlaceMain, truncateMiddle, setActiveCompiledSurface }: CommandHelpers,
): Promise<void> {
  const { state, fabricUi, capturedTools, applyFabricMode, suspendToolCapture } = deps;
  await state.ensure(context);
  const [command = "dashboard", ...argumentsList] = argumentsText
    .trim()
    .split(/\s+/)
    .filter(Boolean);
  if (command === "reload") {
    fabricUi.stop();
    suspendToolCapture();
    try {
      await state.initialize(context);
    } catch (error) {
      fabricUi.stop();
      suspendToolCapture();
      throw error;
    }
    if (state.kernelReloadRequired) {
      await context.reload();
      return;
    }
    await restoreBorrowedInPlaceMain(state.prewalk, pi, context, () => state.config.agents, { principalVisible: true });
    context.ui.notify("Pi Fabric reloaded", "info");
    // initialize() reloads configuration, so an externally edited
    // ui.toolDisplay must re-render existing transcript cards too.
    deps.refreshToolDisplay?.();
    deps.refreshPrincipalView?.(context);
    return;
  }
  if (command === "settings") {
    const { openFabricSettings } = await import("../ui/settings.js");
    await openFabricSettings(context, {
      reloadResources: () => context.reload(),
      state,
      applyFabricMode,
      capturedTools,
      // Only card-affecting preferences pay for a transcript refresh:
      // refreshToolDisplay re-renders every fabric_exec card, so gating it
      // on the display sections keeps unrelated saves off the transcript.
      onConfigApplied: (id) => {
        if (id === "ui.principalView") {
          deps.refreshPrincipalView?.(context);
        } else if (id === "ui.incomingMessages" && context.hasUI) {
          // Pi rebuilds custom-message components only when expansion changes.
          // Refresh through the host API and restore the tool state unchanged.
          const expanded = context.ui.getToolsExpanded();
          context.ui.setToolsExpanded(!expanded);
          context.ui.setToolsExpanded(expanded);
        } else if (id.startsWith("codePreview.")) {
          deps.refreshCodePreviewSettings?.();
          deps.refreshToolDisplay?.();
        } else if (id === "ui.toolDisplay" || id === "ui.showAgentToolPreview") {
          deps.refreshToolDisplay?.();
        }
      },
    });
    return;
  }
  if (command === "schema") {
    const requested = argumentsList[0];
    if (requested === undefined) {
      const status = state.schemaStatus(context);
      context.ui.notify(
        `Schema mode: ${status.mode} (${status.source}) · executor: ${status.executorRuntime} · /fabric schema [off|audit|enforce] overrides this session only`,
        "info",
      );
      return;
    }
    if (requested !== "off" && requested !== "audit" && requested !== "enforce") {
      context.ui.notify("Usage: /fabric schema [off|audit|enforce]", "warning");
      return;
    }
    try {
      state.setSchemaMode(context, requested);
    } catch (error) {
      context.ui.notify(
        error instanceof Error ? error.message : String(error),
        "error",
      );
      return;
    }
    context.ui.notify(
      `Schema mode set to ${requested} for this session (config file unchanged; the next session starts from the configured mode)`,
      "info",
    );
    return;
  }
  if (command === "prewalk") {
    const option = argumentsList[0];
    if (option === "--disable" || option === "--enable") {
      // Persistent master switch: saves prewalk.enabled to the same scope
      // the settings UI writes (project when trusted), reloads config so
      // the rest of the session agrees, and when disabling also cancels
      // the live arm so nothing claims mid-change.
      const enabled = option === "--enable";
      try {
        const projectTrusted = context.isProjectTrusted();
        const saved = saveFabricConfig(
          {
            cwd: context.cwd,
            agentDir: resolveAgentDir(),
            projectTrusted,
            scope: projectTrusted ? "project" : "global",
          },
          { prewalk: { enabled } },
        );
        state.reloadConfig(context);
        if (!enabled) {
          state.prewalk.cancel();
          state.prewalkDrift.drop(context.sessionManager.getSessionId());
          context.ui.setStatus("fabric-prewalk", undefined);
          await restoreBorrowedInPlaceMain(state.prewalk, pi, context, () => state.config.agents, { principalVisible: true });
        }
        context.ui.notify(
          `Fabric prewalk ${enabled ? "enabled" : "disabled"} (${saved.scope}: ${saved.path})`,
          "info",
        );
      } catch (error) {
        context.ui.notify(
          `Failed to update Fabric prewalk setting: ${error instanceof Error ? error.message : String(error)}`,
          "error",
        );
      }
      return;
    }
    if (option === "--off" || option === "--cancel") {
      state.prewalk.cancel();
      state.prewalkDrift.drop(context.sessionManager.getSessionId());
      context.ui.setStatus("fabric-prewalk", undefined);
      await restoreBorrowedInPlaceMain(state.prewalk, pi, context, () => state.config.agents, { principalVisible: true });
      context.ui.notify("Fabric prewalk cancelled", "info");
      return;
    }
    if (option === "--status") {
      const status = state.prewalk.status();
      context.ui.notify(
        status.state === "idle"
          ? "Fabric prewalk is idle"
          : `Fabric prewalk ${status.state} (${status.mode}) → ${status.model}${status.task ? `\nTask: ${status.task}` : ""}`,
        "info",
      );
      return;
    }
    const task = argumentsText.trim().slice(command.length).trim();
    await armPrewalk(task);
    return;
  }
  if (command === "tasks") {
    await fabricUi.openTasks(context, argumentsList[0]);
    return;
  }
  if (command === "chat") {
    await fabricUi.openConversation(context, argumentsList.join(" ") || undefined);
    return;
  }
  if (command === "decisions") {
    const { openFabricDecisions } = await import("../decisions/command.js");
    await openFabricDecisions(state.mesh, context, argumentsList[0]);
    return;
  }
  if (command === "programs" || command === "run") {
    const { runFabricProgramsCommand } = await import("../programs/host.js");
    await runFabricProgramsCommand({ state, pi }, context, command, argumentsText.trim().slice(command.length));
    return;
  }
  if (command === "dashboard" || command === "ui") {
    await fabricUi.openDashboard(context);
    return;
  }
  if (command === "providers") {
    const providers = state.registry.providers();
    context.ui.notify(
      providers.map((provider) => `${provider.name} — ${provider.description}`).join("\n"),
      "info",
    );
    return;
  }
  if (command === "captured") {
    const query = argumentsList.join(" ").toLowerCase();
    const tools = capturedTools
      .list()
      .filter(
        (tool) =>
          !query ||
          `${tool.name} ${tool.definition.description} ${tool.sourceInfo.path}`
            .toLowerCase()
            .includes(query),
      );
    const shown = tools.slice(0, 100);
    context.ui.notify(
      shown.length > 0
        ? [
            ...shown.map((tool) => `${tool.name} [${tool.risk}] — ${tool.sourceInfo.path}`),
            ...(tools.length > shown.length
              ? [`… ${tools.length - shown.length} more captured tools`]
              : []),
          ].join("\n")
        : query
          ? `No captured extension tools matching ${JSON.stringify(query)}`
          : "No extension tools captured",
      "info",
    );
    return;
  }
  if (command === "agents") {
    const agents = state.agents.list();
    context.ui.notify(
      agents.length > 0
        ? agents
            .map(
              (agent) =>
                `${agent.id.slice(0, 8)} ${agent.status} ${agent.runner}/${agent.transport} — ${agent.name}`,
            )
            .join("\n")
        : "No Fabric agents",
      "info",
    );
    return;
  }
  if (command === "actors") {
    const actors = state.actors.list();
    context.ui.notify(
      actors.length > 0
        ? actors
            .map(
              (actor) =>
                `${actor.id.slice(0, 8)} ${actor.status} ${actor.runner} q:${actor.queued} — ${actor.name}`,
            )
            .join("\n")
        : "No Fabric actors",
      "info",
    );
    return;
  }
  if (command === "messages") {
    const id = argumentsList[0];
    if (!id) {
      context.ui.notify("Usage: /fabric messages <actor-id>", "warning");
      return;
    }
    try {
      const actor = state.actors.status(id);
      const messages = state.actors.messages(actor.id, 20);
      const shortId = actor.id.slice(0, 8);
      const body =
        messages.length > 0
          ? messages
              .map((message) => {
                const value = message.text ?? message.error ?? message.action ?? "data";
                const summary = truncateMiddle(value.replace(/\s+/g, " "), 500);
                const runTag = message.runId ? ` [${message.runId.slice(0, 8)}]` : "";
                const usageTag = message.usage
                  ? ` · ${message.usage.input + message.usage.output} tok`
                  : "";
                return `${message.direction === "in" ? "→" : "←"} ${message.source}${runTag}: ${summary}${usageTag}`;
              })
              .join("\n")
          : `No messages for ${actor.name}`;
      const footer = `\nInspect LLM I/O: /fabric log ${shortId} · Export: /fabric export-log ${actor.name}`;
      context.ui.notify(`${body}${footer}`, "info");
    } catch (error) {
      context.ui.notify(error instanceof Error ? error.message : String(error), "error");
    }
    return;
  }
  if (command === "log") {
    const id = argumentsList[0];
    if (!id) {
      context.ui.notify(
        "Usage: /fabric log <id> [session|run|all] [--lines N] [--run <runId>]",
        "warning",
      );
      return;
    }
    let type: "session" | "run" | "all" = "session";
    let lines = 40;
    let runId: string | undefined;
    for (let i = 1; i < argumentsList.length; i++) {
      const arg = argumentsList[i]!;
      if (arg === "session" || arg === "run" || arg === "all") type = arg;
      else if ((arg === "--lines" || arg === "-n") && i + 1 < argumentsList.length) {
        const n = Number(argumentsList[++i]);
        if (n > 0) lines = Math.min(n, 5000);
      } else if (arg === "--run" && i + 1 < argumentsList.length) {
        runId = argumentsList[++i];
      }
    }
    try {
      const actor = state.actors.status(id);
      const log = state.actors.readLog(actor.id, { type, lines, ...(runId ? { runId } : {}) });
      const parts: string[] = [`Actor ${actor.name} · ${log.sessionFile}`];
      if (log.session.length > 0) {
        parts.push(`── session (last ${log.session.length} lines) ──`);
        for (const line of log.session) parts.push(summarizeLogLine(line.parsed ?? line.raw, truncateMiddle));
      }
      if (log.run) {
        parts.push(
          `── run ${log.run.runId.slice(0, 8)} (${log.run.status?.status ?? "?"}) ──`,
        );
        for (const line of log.run.events) parts.push(summarizeLogLine(line.parsed ?? line.raw, truncateMiddle));
      }
      if (log.retainedRuns.length > 0) {
        parts.push(
          `retained runs: ${log.retainedRuns.map((r) => r.slice(0, 8)).join(" ")}`,
        );
      }
      context.ui.notify(
        parts.length > 1 ? truncateMiddle(parts.join("\n"), 8000) : `No log found for ${actor.name}`,
        "info",
      );
    } catch (error) {
      context.ui.notify(error instanceof Error ? error.message : String(error), "error");
    }
    return;
  }
  if (command === "export-log") {
    const id = argumentsList[0];
    const destArg = argumentsList.slice(1).join(" ");
    if (!id) {
      context.ui.notify("Usage: /fabric export-log <id> [path]", "warning");
      return;
    }
    try {
      const dest = path.resolve(
        destArg || path.join("fabric-logs", `export-${Date.now()}`),
      );
      fs.mkdirSync(dest, { recursive: true });
      const actor = state.actors
        .list()
        .find((candidate) => candidate.id.startsWith(id) || candidate.name === id);
      let label: string;
      let copied: string[] = [];
      if (actor) {
        const full = state.actors.status(actor.id);
        label = actor.name;
        if (full.sessionFile && fs.existsSync(full.sessionFile)) {
          fs.copyFileSync(full.sessionFile, path.join(dest, "session.jsonl"));
          copied.push("session.jsonl");
        }
        if (full.logDir && fs.existsSync(full.logDir)) {
          fs.cpSync(full.logDir, path.join(dest, "runs"), { recursive: true });
          copied.push("runs/");
        }
      } else {
        const runDir = state.agents.runDirectory(id);
        const status = state.agents.status(id);
        label = status.name;
        if (runDir && fs.existsSync(runDir)) {
          fs.cpSync(runDir, dest, { recursive: true });
          copied.push("run/");
        }
      }
      if (copied.length === 0) {
        context.ui.notify(`No log files found for ${label}`, "warning");
        return;
      }
      context.ui.notify(`Exported ${label} log → ${dest} (${copied.join(", ")})`, "info");
    } catch (error) {
      context.ui.notify(error instanceof Error ? error.message : String(error), "error");
    }
    return;
  }
  if (command === "clear-messages") {
    const id = argumentsList[0];
    if (!id) {
      context.ui.notify("Usage: /fabric clear-messages <actor-id>", "warning");
      return;
    }
    try {
      const actor = state.actors.status(id);
      await state.actors.clearMessages(actor.id);
      context.ui.notify(`Cleared message history for ${actor.name}`, "info");
    } catch (error) {
      context.ui.notify(error instanceof Error ? error.message : String(error), "error");
    }
    return;
  }
  if (command === "events") {
    const id = argumentsList[0];
    if (!id) {
      context.ui.notify("Usage: /fabric events <actor-id> [event...]", "warning");
      return;
    }
    try {
      const actor = state.actors.status(id);
      const events = argumentsList.slice(1) as FabricActorHostEvent[];
      await state.actors.setEvents(actor.id, events);
      context.ui.notify(
        `Set ${actor.name} events: ${events.join(", ") || "(none)"}`,
        "info",
      );
    } catch (error) {
      context.ui.notify(error instanceof Error ? error.message : String(error), "error");
    }
    return;
  }
  if (command === "stop") {
    const id = argumentsList[0];
    if (!id) {
      context.ui.notify("Usage: /fabric stop <id>", "warning");
      return;
    }
    const actor = state.actors
      .list()
      .find((candidate) => candidate.id.startsWith(id) || candidate.name === id);
    if (actor) {
      await state.actors.stop(actor.id);
      context.ui.notify(`Stopped Fabric actor ${actor.id.slice(0, 8)}`, "info");
      return;
    }
    const agent = state.agents.list().find((candidate) => candidate.id.startsWith(id));
    if (!agent) {
      context.ui.notify(`Unknown Fabric actor or agent: ${id}`, "error");
      return;
    }
    await state.agents.stop(agent.id);
    context.ui.notify(`Stopped Fabric agent ${agent.id.slice(0, 8)}`, "info");
    return;
  }
  if (command === "remove" || command === "kill") {
    const id = argumentsList[0];
    if (!id) {
      context.ui.notify("Usage: /fabric remove <id>", "warning");
      return;
    }
    const actor = state.actors
      .list()
      .find((candidate) => candidate.id.startsWith(id) || candidate.name === id);
    if (actor) {
      await state.actors.remove(actor.id);
      context.ui.notify(`Removed Fabric actor ${actor.id.slice(0, 8)} (${actor.name})`, "info");
      return;
    }
    const agent = state.agents.list().find((candidate) => candidate.id.startsWith(id));
    if (!agent) {
      context.ui.notify(`Unknown Fabric actor or agent: ${id}`, "error");
      return;
    }
    await state.agents.stop(agent.id);
    await state.agents.cleanup(agent.id);
    context.ui.notify(`Removed Fabric agent ${agent.id.slice(0, 8)}`, "info");
    return;
  }
  if (command === "attach") {
    const id = argumentsList[0];
    const agent = id
      ? state.agents.list().find((candidate) => candidate.id.startsWith(id))
      : undefined;
    if (!agent?.attachCommand) {
      context.ui.notify("No attachable Fabric agent found", "warning");
      return;
    }
    context.ui.notify(agent.attachCommand, "info");
    return;
  }
  if (command === "global") {
    const templates = state.globalActors.list();
    context.ui.notify(
      templates.length > 0
        ? templates
            .map((template) => `${template.id.slice(0, 8)} global ${template.runner} — ${template.name}`)
            .join("\n")
        : "No global Fabric actor templates",
      "info",
    );
    return;
  }
  if (command === "import") {
    try {
      // Keep optional import parsing off the idle registration graph.
      const { parseSlotImport, SLOT_IMPORT_USAGE } = await import("./slot-import.js");
      const { key, as, ...materialization } = parseSlotImport(argumentsList);
      if (!key) {
        context.ui.notify(SLOT_IMPORT_USAGE, "warning");
        return;
      }
      const def = state.globalActors.resolve(key);
      if (!def) {
        context.ui.notify(`Unknown global actor: ${key}`, "error");
        return;
      }
      const actor = await state.actors.create({ ...state.globalActors.toRequest(def, as), ...materialization });
      context.ui.notify(`Imported global actor "${def.name}" as ${actor.name}`, "info");
    } catch (error) {
      context.ui.notify(error instanceof Error ? error.message : String(error), "error");
    }
    return;
  }
  if (command === "export") {
    const id = argumentsList[0];
    const overwrite = argumentsList.includes("--overwrite") || argumentsList.includes("-f");
    if (!id) {
      context.ui.notify("Usage: /fabric export <actor-id> [--overwrite]", "warning");
      return;
    }
    try {
      const actor = state.actors
        .list()
        .find((candidate) => candidate.id.startsWith(id) || candidate.name === id);
      if (!actor) {
        context.ui.notify(`Unknown Fabric actor: ${id}`, "error");
        return;
      }
      const def = state.actors.definition(actor.id);
      const template = state.globalActors.create(def, overwrite);
      context.ui.notify(`Exported "${template.name}" to global actors`, "info");
    } catch (error) {
      context.ui.notify(error instanceof Error ? error.message : String(error), "error");
    }
    return;
  }
  if (command === "repairs") {
    if (argumentsList[0] !== undefined) {
      context.ui.notify("Usage: /fabric repairs", "warning");
      return;
    }
    context.ui.notify(formatRepairStatus(state.repairs.status()), "info");
    return;
  }
  if (command === "entropy") {
    const {
      ENTROPY_METRIC_VERSION, entropySurfaceHash, liveSurfaceSnapshot, surfaceFreedomReport,
      machineSessionFilesAsync, measureSessionCorpusAsync, projectSessionFilesAsync,
      sessionWindowEvidenceAsync, measureEntropyAsync, entropyRepairRows, entropyReviewSignals,
      formatEntropyReviewSignal, loadObservationPoolAsync, mergeObservationWindowAsync,
      poolToValueObservations, applyCompiledSurface, normalFormEvidenceSummary,
      loadCompiledSurfaceAsync, parseCompiledSurfaceArtifact, saveCompiledSurfaceAsync,
      formatEntropyCommandHints, formatEntropyMetric, mergeCompiledSurfaces,
    } = await import("../entropy/index.js");
    const exportArtifactIndex = argumentsList.indexOf("export-artifact");
    if (exportArtifactIndex >= 0) {
      const target = argumentsList[exportArtifactIndex + 1];
      try {
        const loaded = await loadCompiledSurfaceAsync(resolveAgentDir());
        if (loaded.error) {
          context.ui.notify(loaded.error, "error");
          return;
        }
        if (!loaded.file) {
          context.ui.notify(
            "No compiled surface to export; wait for a compile to pass its gate",
            "warning",
          );
          return;
        }
        const dest = path.resolve(
          target ?? path.join(resolveAgentDir(), "fabric", "entropy", "artifact.json"),
        );
        await fs.promises.mkdir(path.dirname(dest), { recursive: true });
        await fs.promises.writeFile(dest, `${JSON.stringify(loaded.file, null, 2)}\n`, {
          encoding: "utf-8",
          mode: 0o600,
        });
        context.ui.notify(
          `Exported compiled surface (${loaded.file.normalizations?.length ?? 0} normal-form plans; legacy restrictions inert) → ${dest}`,
          "info",
        );
      } catch (error) {
        context.ui.notify(
          error instanceof Error ? error.message : String(error),
          "error",
        );
      }
      return;
    }
    const importIndex = argumentsList.indexOf("import");
    if (importIndex >= 0) {
      const source = argumentsList[importIndex + 1];
      if (!source) {
        context.ui.notify("Usage: /fabric entropy import <artifact.json>", "warning");
        return;
      }
      try {
        const raw: unknown = JSON.parse(
          await fs.promises.readFile(path.resolve(source), "utf-8"),
        );
        const incoming = parseCompiledSurfaceArtifact(raw);
        if (!incoming) {
          context.ui.notify(`Artifact is invalid: ${source}`, "error");
          return;
        }
        const agentDir = resolveAgentDir();
        const localLoaded = await loadCompiledSurfaceAsync(agentDir);
        if (localLoaded.error) {
          context.ui.notify(localLoaded.error, "error");
          return;
        }
        const live = await liveSurfaceSnapshot({
          registry: state.registry,
          extensionContext: context,
          cwd: state.cwd ?? context.cwd,
        });
        const merged = mergeCompiledSurfaces(localLoaded.file, incoming, live);
        const saved = await saveCompiledSurfaceAsync(agentDir, merged.file);
        if (state.config.entropy.compile) setActiveCompiledSurface(saved.file);
        context.ui.notify(
          `Imported compiled surface: ${merged.file.normalizations?.length ?? 0} normal-form plans · ${merged.droppedOverlays + merged.droppedQuarantines + merged.droppedNormalizations} dropped (unproved rules or legacy restrictions)`,
          "info",
        );
      } catch (error) {
        context.ui.notify(
          error instanceof Error ? error.message : String(error),
          "error",
        );
      }
      return;
    }
    const exportIndex = argumentsList.indexOf("export");
    if (exportIndex >= 0) {
      const target = argumentsList[exportIndex + 1];
      try {
        const snapshot = await liveSurfaceSnapshot({
          registry: state.registry,
          extensionContext: context,
          cwd: state.cwd ?? context.cwd,
        });
        const dest = path.resolve(
          target ?? path.join(resolveAgentDir(), "fabric", "entropy", "surface.json"),
        );
        await fs.promises.mkdir(path.dirname(dest), { recursive: true });
        await fs.promises.writeFile(dest, `${JSON.stringify(snapshot, null, 2)}\n`, {
          encoding: "utf8",
          mode: 0o600,
        });
        context.ui.notify(
          `Exported ${snapshot.actions.length} surface actions → ${dest}`,
          "info",
        );
      } catch (error) {
        context.ui.notify(
          error instanceof Error ? error.message : String(error),
          "error",
        );
      }
      return;
    }
    const projectScope = argumentsList.includes("--project");
    const scopedArguments = argumentsList.filter((argument) => argument !== "--project");
    if (scopedArguments[0] !== undefined) {
      context.ui.notify(
        "Usage: /fabric entropy [--project] [export [path] | export-artifact [path] | import <path>]",
        "warning",
      );
      return;
    }
    try {
      const status = state.repairs.status();
      const agentDir = resolveAgentDir();
      const scopeCwd = state.cwd ?? context.cwd;
      const [loadedCompiled, live, files] = await Promise.all([
        loadCompiledSurfaceAsync(agentDir),
        liveSurfaceSnapshot({
          registry: state.registry,
          extensionContext: context,
          cwd: scopeCwd,
        }),
        projectScope
          ? projectSessionFilesAsync(agentDir, scopeCwd)
          : machineSessionFilesAsync(agentDir, scopeCwd),
      ]);
      const snapshot = loadedCompiled.file
        ? applyCompiledSurface(live, loadedCompiled.file)
        : live;
      const freedom = surfaceFreedomReport(snapshot);
      const surfaceDigest = entropySurfaceHash(snapshot);
      const corpus = await measureSessionCorpusAsync({
        files,
        surface: snapshot,
        catalogDigest: surfaceDigest,
      });
      const formatFreedom = (value: number): string =>
        String(Math.round(value * 100) / 100);
      const worst = freedom.actions
        .slice(0, 4)
        .map((action) => `${action.ref} ${formatFreedom(action.freedom)}`)
        .join(" · ");
      const latest = corpus.latest;
      const sessionsLine =
        corpus.sessions.length === 0 || latest === undefined
          ? `session entropy (observed): no fabric_exec traces in the latest ${files.length} ${projectScope ? "project" : "machine"} sessions`
          : `session entropy (observed): ${corpus.sessions.length} sessions · latest ${latest.totals.operations} ops · invocation rejection rate ${formatEntropyMetric(latest.score)} · ${formatEntropyMetric(latest.totals.invocationRejectionsPer1k)}/1k calls · lower is better · slope ${formatEntropyMetric(corpus.trend.slopePerStep)}/session · ${
              latest.totals.invocationRejectionsPer1k === 0 && corpus.trend.slopePerStep <= 0
                ? "no invocation rejections"
                : "invocation friction remains"
            }`;
      const modelsLine =
        corpus.models.length > 1
          ? `models: ${corpus.models
              .map(
                (model) =>
                  `${model.model} behavioral ${formatEntropyMetric(model.latestBehavioralScore)} · slope ${formatEntropyMetric(model.slopePerSession)}`,
              )
              .join(" · ")}`
          : undefined;
      const compiledLine = loadedCompiled.error
        ? `compiled: unavailable — ${loadedCompiled.error}`
        : loadedCompiled.file
          ? `compiled: artifact v${loadedCompiled.file.version} · ${loadedCompiled.file.normalizations?.length ?? 0} normal-form plans · canonical capabilities preserved${loadedCompiled.file.version === 1 ? " · legacy restrictions inert (migration pending)" : ""}`
          : "compiled: none";
      // Review listing: the signals the compiler declined to apply,
      // derived read-only from the current window plus the pool (the
      // merge is in memory; the listing never writes the pool).
      let reviewLine: string;
      let normalizationLine = "normal forms: no witness evidence";
      try {
        const [windowEvidence, poolLoaded] = await Promise.all([
          sessionWindowEvidenceAsync(files),
          loadObservationPoolAsync(agentDir),
        ]);
        const normal = normalFormEvidenceSummary(windowEvidence.traces);
        normalizationLine = `normal forms: ${normal.normalizedCalls} calls normalized · ${normal.rulesApplied} rules applied · ${normal.succeeded} succeeded · ${normal.subsequentFailures} subsequent failures (not masked)`;
        if (poolLoaded.error) {
          reviewLine = `review: unavailable — ${poolLoaded.error}`;
        } else if (windowEvidence.traces.length === 0) {
          reviewLine = "review: no window evidence";
        } else {
          const mergedPool = await mergeObservationWindowAsync(
            poolLoaded.file,
            windowEvidence.observationWindows,
          );
          const signals = entropyReviewSignals({
            report: await measureEntropyAsync({
              traces: windowEvidence.traces,
              surface: snapshot,
              catalogDigest: surfaceDigest,
            }),
            traces: windowEvidence.traces,
            surface: snapshot,
            repairs: entropyRepairRows(state.repairs.repairs),
            valueObservations: poolToValueObservations(mergedPool.file),
          });
          reviewLine =
            signals.length === 0
              ? "review: none"
              : `review: ${signals.length} suggestion${signals.length === 1 ? "" : "s"} · ${signals
                  .slice(0, 4)
                  .map(formatEntropyReviewSignal)
                  .join(" · ")}${signals.length > 4 ? ` · +${signals.length - 4} more` : ""}`;
        }
      } catch {
        reviewLine = "review: unavailable";
      }
      context.ui.notify(
        [
          `entropy: metric v${ENTROPY_METRIC_VERSION} · surface ${surfaceDigest.slice(0, 12)} · ${freedom.actions.length} actions`,
          `live: invocation errors ${status.invocationErrors} · effect dropped ${status.effectDropped} · aliases ${status.repairCount} · alias hits ${status.applyHits}`,
          compiledLine,
          normalizationLine,
          reviewLine,
          `surface freedom (diagnostic, not objective): mean ${formatFreedom(freedom.mean)}${worst ? ` · worst ${worst}` : ""}`,
          sessionsLine,
          ...(modelsLine ? [modelsLine] : []),
          ...formatEntropyCommandHints(),
        ].join("\n"),
        "info",
      );
    } catch (error) {
      context.ui.notify(
        error instanceof Error ? error.message : String(error),
        "error",
      );
    }
    return;
  }
  if (command !== "status") {
    context.ui.notify(
      "Usage: /fabric [status|dashboard|chat [id-or-name]|prewalk [task]|prewalk --off|--disable|--enable|reload|providers|agents|actors|global|import <name> [as <new>]|export <id> [--overwrite]|messages <id>|clear-messages <id>|events <id> [event...]|log <id>|export-log <id>|attach <id>|stop <id>|remove <id>|kill <id>|repairs|entropy|decisions [id]|programs [promote|retire <ref>]|run <ref> [json]]",
      "warning",
    );
    return;
  }
  const config = state.config;
  context.ui.notify(
    [
      `cwd: ${state.cwd}`,
      `mode: ${config.fullCodeMode ? "full code (Fabric-owned core tools)" : "orchestration-only (native Pi tools)"}`,
      `providers: ${state.registry
        .providers()
        .map((provider) => provider.name)
        .join(", ")}`,
      `runner: ${config.agents.runner} · transport: ${config.agents.transport} · model: ${
        config.agents.runner === "claude"
          ? config.agents.claude.model || "Claude default"
          : config.agents.runner === "veda"
            ? `${config.agents.veda.model || "Veda default"} · backend ${config.agents.veda.backend} · persona ${config.agents.veda.persona}`
            : config.agents.model || "inherit"
      }`,
      `agent limits: concurrency ${config.agents.maxConcurrent}, per execution ${config.agents.maxPerExecution}, depth ${config.agents.maxDepth}`,
      (() => {
        const prewalk = state.prewalk.status();
        return prewalk.state === "idle"
          ? `prewalk: idle · ${config.prewalk.mode} · model ${config.prewalk.model || "Ask each time"} · auto-arm & re-arm ${config.prewalk.alwaysRearm ? "on" : "off"}`
          : `prewalk: ${prewalk.state} · ${prewalk.mode} → ${prewalk.model}${prewalk.alwaysRearm ? " · always re-arm" : ""}`;
      })(),
      config.fullCodeMode && config.capture.enabled
        ? `captured tools: ${capturedTools.size} · model visibility: ${config.capture.hideFromModel ? "hidden" : "visible"}`
        : "captured tools: disabled (native registry preserved)",
      ...(config.foreground.tools.length > 0 ? [`foreground: ${formatForeground(state.foregroundTools())}`] : []),
      `actors: ${state.actors.list().length} · mesh: ${config.mesh.enabled ? state.mesh.root : "disabled"}`,
      `MCP: ${config.mcp.enabled ? "enabled" : "disabled"}`,
      `UI: ${config.ui.enabled ? `${config.ui.widget} widget above chat` : "disabled"}`,
      ...(state.observationHealth?.() ?? []),
    ].join("\n"),
    "info",
  );
}
