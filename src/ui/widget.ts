import type { Component, Focusable, TuiMouseEvent, TuiMouseEventResult } from "@earendil-works/pi-tui";
import { matchesKey, truncateToWidth, visibleWidth } from "@earendil-works/pi-tui";
import type { Theme } from "@earendil-works/pi-coding-agent";
import type { FabricUiWidgetMode } from "../config.js";
import { spinnerFrame } from "./spinner.js";
import { FabricWidgetRetention, isRecentWidgetCompletion } from "./retention.js";
import { FABRIC_CONVERSATION_HINT } from "./conversation-shortcut.js";
import type { AgentTranscriptReader, FabricAgentTranscript } from "./transcript.js";
import type { WidgetInteraction } from "./widget-interaction.js";

export const FABRIC_WIDGET_FOCUS_SHORTCUT = "ctrl+shift+e";
export const FABRIC_WIDGET_FOCUS_HINT = "ctrl+shift+e expand";
export const WIDGET_ACTIVITY_ROWS = 12;
export const WIDGET_ACTIVITY_LOG_LINES = 600;

let interactionModule: typeof import("./widget-interaction.js") | undefined;
let interactionLoad: Promise<void> | undefined;
/** Explicit first-use preload; standalone callers may await this before constructing a widget. */
export const loadWidgetInteraction = (): Promise<void> => interactionLoad ??= import("./widget-interaction.js")
  .then((module) => { interactionModule = module; })
  .catch((error) => { interactionLoad = undefined; throw error; });
import type {
  FabricActivityRun,
  FabricActivityStatus,
} from "../activity/types.js";
import { formatCost, formatDuration, formatTokens, safeText } from "./format.js";
import {
  isActiveStatus,
  orderAgentsByCreation,
  type FabricDashboardSnapshot,
  type FabricUiAgent,
} from "./types.js";

const statusGlyph = (status: string): string => {
  if (status === "completed" || status === "done" || status === "exited") return "✓";
  if (status === "failed" || status === "timed_out") return "✗";
  if (status === "blocked") return "!";
  if (status === "stopped" || status === "cancelled" || status === "killed") return "■";
  if (status === "queued" || status === "pending" || status === "ready") return "○";
  if (status === "idle" || status === "state") return "·";
  return spinnerFrame();
};

const colorStatus = (theme: Theme, status: string, value: string): string => {
  if (status === "completed" || status === "done" || status === "exited") return theme.fg("success", value);
  if (status === "failed" || status === "timed_out") return theme.fg("error", value);
  if (status === "blocked" || status === "stopping") return theme.fg("warning", value);
  if (status === "running" || status === "in_progress" || status === "spilled") return theme.fg("accent", value);
  return theme.fg("dim", value);
};

const phaseProgress = (
  run: FabricActivityRun,
  phaseId: string,
): { completed: number; total: number } => {
  const phase = run.phases.find((candidate) => candidate.id === phaseId);
  const statuses: FabricActivityStatus[] = [
    ...run.calls.filter((call) => call.phaseId === phaseId).map((call) => call.status),
    ...run.items.filter((item) => item.phaseId === phaseId).map((item) => item.status),
  ];
  const completed = statuses.filter((status) => status === "completed").length;
  return { completed, total: Math.max(phase?.total ?? 0, statuses.length) };
};

// smarty-dev#1089: the widget is this session's status. Every pane showed the same "5 running" for
// other sessions' runs (from the project-wide participant list), which misled agents about their own
// state. Only runs rooted in this session count here; the dashboard still shows every root's runs.
const ownAgents = (snapshot: FabricDashboardSnapshot): FabricDashboardSnapshot["agents"] =>
  snapshot.agents.filter((agent) => agent.rootId === undefined || agent.rootId === snapshot.main.id);

const totalTokens = (
  snapshot: FabricDashboardSnapshot,
  run: FabricActivityRun | undefined,
): number =>
  ownAgents(snapshot)
    .filter((agent) => (run ? agent.runId === run.id : isActiveStatus(agent.status)))
    .reduce(
      (sum, agent) => sum + (agent.usage ? agent.usage.input + agent.usage.output : 0),
      0,
    );

const totalCost = (
  snapshot: FabricDashboardSnapshot,
  run: FabricActivityRun | undefined,
): number =>
  ownAgents(snapshot)
    .filter((agent) => (run ? agent.runId === run.id : isActiveStatus(agent.status)))
    .reduce((sum, agent) => sum + (agent.usage?.cost ?? 0), 0);

/** A widget row that can be selected and expanded to show its live activity. */
export interface FabricWidgetRow {
  /** Agent id, or actor id for an actor's worker row. */
  id: string;
  kind: "agent" | "actor";
  name: string;
}

/** Optional interaction wiring; without it the widget stays a passive status box. */
export interface FabricWidgetInteraction {
  /** Live activity for an expanded row, read from the participant's event log. */
  activity: (row: FabricWidgetRow, reader?: AgentTranscriptReader) => FabricAgentTranscript | undefined;
  /**
   * Return focus to the editor. `forward` is a key the widget does not use; the
   * editor receives it, because Pi handles Ctrl+C and extension shortcuts there.
   */
  release: (forward?: string) => void;
  /** Pi's hideThinkingBlock: the pane keeps the thinking marker and drops its text. */
  hideThinking?: () => boolean;
  /** Ask the host to redraw (selection and expansion change without a snapshot change). */
  requestRender: () => void;
}

const agentLines = (
  theme: Theme,
  agent: FabricUiAgent,
): string[] => {
  const status = colorStatus(theme, agent.status, statusGlyph(agent.status));
  const activity =
    agent.currentTool ??
    (agent.error
      ? `error: ${truncateToWidth(safeText(agent.error), 48)}`
      : agent.text && !isActiveStatus(agent.status)
        ? `result: ${truncateToWidth(safeText(agent.text), 48)}`
        : agent.status === "running"
          ? "thinking"
          : agent.status);
  const metrics = [
    agent.toolCalls !== undefined ? `${agent.toolCalls} calls` : undefined,
    agent.usage ? `${formatTokens(agent.usage.input + agent.usage.output)} tok` : undefined,
    agent.startedAt !== undefined && agent.finishedAt !== undefined
      ? formatDuration(agent.finishedAt - agent.startedAt)
      : undefined,
  ].filter((value): value is string => Boolean(value));
  const indent = "  ".repeat(1 + Math.max(0, agent.nestingDepth ?? 0));
  return [
    `${indent}${status} ${theme.fg("muted", safeText(agent.name))}  ${theme.fg("muted", safeText(activity))}${
      metrics.length > 0 ? theme.fg("dim", ` · ${metrics.join(" · ")}`) : ""
    }`,
  ];
};

// Only the host reply needs a stable above-editor height. Children can outlive
// that reply, but must not keep its reservation alive once Main has ended.
export const isFabricWidgetStreaming = (snapshot: FabricDashboardSnapshot): boolean =>
  snapshot.main.status === "running";

export const shouldShowFabricWidget = (
  snapshot: FabricDashboardSnapshot,
  mode: FabricUiWidgetMode,
  retention = new FabricWidgetRetention(),
): boolean => {
  if (mode === "hidden") return false;
  if (mode === "always") return true;
  retention.sync(snapshot);
  if (snapshot.shells?.some(job => job.finishedAt === undefined || isRecentWidgetCompletion(job.finishedAt, snapshot.now))) return true;
  if (ownAgents(snapshot).some((agent) => retention.visible(agent, snapshot))) return true;
  if (snapshot.actors.some((actor) => actor.status !== "stopped")) return true;
  const run = snapshot.runs[0];
  if (!run) return false;
  if (run.status === "running") return true;
  const finishedAt = run.finishedAt ?? run.updatedAt;
  return finishedAt > (snapshot.widgetDismissedAt ?? 0);
};

/**
 * Share of the terminal height the animated box may claim. pi's main-screen
 * renderer scrolls the pane once the rendered content is taller than the
 * terminal, so a box that fills the viewport keeps that scroll region moving on
 * every animation frame — which reads as flicker inside tmux. Half the pane
 * keeps the transcript and editor visible and bounds the box on short panes.
 */
export const WIDGET_TERMINAL_ROW_SHARE = 0.5;

/** Effective row budget: the configured maximum, bounded by the live pane. */
export const widgetRowLimit = (maxRows: number, terminalRows?: number): number => {
  const configured = Math.max(1, maxRows);
  if (terminalRows === undefined || !Number.isFinite(terminalRows) || terminalRows <= 0) {
    return configured;
  }
  const paneShare = Math.floor(terminalRows * WIDGET_TERMINAL_ROW_SHARE);
  return Math.max(1, Math.min(configured, paneShare));
};

/**
 * Row budget while an activity pane is open. Expanding is an explicit request
 * for more room, so the widget may use its whole terminal share instead of
 * `maxRows`, but never more than that share.
 */
export const widgetExpandedRowLimit = (maxRows: number, terminalRows?: number): number => {
  const limit = widgetRowLimit(maxRows, terminalRows);
  if (terminalRows === undefined || !Number.isFinite(terminalRows) || terminalRows <= 0) {
    return limit + 2 + WIDGET_ACTIVITY_ROWS;
  }
  return Math.max(limit, Math.floor(terminalRows * WIDGET_TERMINAL_ROW_SHARE));
};

export class FabricWidget implements Component, Focusable {
  constructor(
    readonly theme: Theme,
    readonly snapshot: () => FabricDashboardSnapshot,
    readonly maxRows: number,
    // Live terminal height. pi re-renders the widget on resize, so reading the
    // pane per render bounds the box without a resize subscription.
    readonly terminalRows?: () => number | undefined,
    // Controller-owned above-editor widgets reserve rows for streamed turns.
    // Standalone renderers keep the historical compact behavior.
    readonly stableWhileStreaming = false,
    readonly retention = new FabricWidgetRetention(),
    readonly interaction?: FabricWidgetInteraction,
  ) {
    if (interactionModule && interaction) this.#installInteraction();
  }

  /** Set by the TUI when the widget gains or loses keyboard focus. */
  focused = false;
  #engine: WidgetInteraction | undefined;
  #renderedFocus = false;
  #renderedExpansion: string | undefined;
  #interactionKey(): string { return this.#engine?.key ?? String(this.focused); }

  #installInteraction(): void {
    if (this.#engine || !interactionModule) return;
    this.#engine = new interactionModule.WidgetInteraction({
      widget: this, content: () => this.#content(this.snapshot()), rowLimit: () => this.#rowLimit(),
      bound: (lines, width, limit) => this.#boundContent(lines, width, limit),
      hint: (lines, hasRows) => this.#withFocusHint(lines, hasRows),
    });
    this.invalidate();
  }

  /** Controller awaits this at keyboard focus; mouse focus queues its first click. */
  #pendingActions: Array<(engine: WidgetInteraction) => void> = [];
  #passiveRows: Array<FabricWidgetRow & { line: number }> = [];
  async prepareInteraction(): Promise<void> {
    await loadWidgetInteraction();
    this.#installInteraction();
    if (this.focused) {
      this.#engine!.selectDefault();
      while (this.focused && this.#pendingActions.length > 0) this.#pendingActions.shift()!(this.#engine!);
    }
    this.#pendingActions = [];
  }

  /** Id of the row whose activity pane is open, if any. */
  get expandedId(): string | undefined {
    return this.#engine?.expandedId;
  }

  /** Select the first participant row; used when focus arrives from the keyboard. */
  selectDefault(): void {
    this.#engine?.selectDefault();
  }

  handleInput(data: string): void {
    if (this.#engine) { this.#engine.handleInput(data); return; }
    // Escape and unrelated keys must still return to the editor immediately
    // while the first-use import is pending. Preserve ordered widget keys.
    if (matchesKey(data, FABRIC_WIDGET_FOCUS_SHORTCUT) || (matchesKey(data, "escape") && this.#pendingActions.length === 0)) {
      this.#pendingActions = [];
      this.interaction?.release();
    } else if ((["up", "down", "enter", "escape", "pageUp", "pageDown", "end"] as const).some((key) => matchesKey(data, key)) || ["k", "j", " ", "K", "J", "G"].includes(data)) {
      this.#pendingActions.push((engine) => engine.handleInput(data));
      void this.prepareInteraction().then(() => this.interaction?.requestRender()).catch(() => {
        this.#pendingActions = [];
        if (this.focused) this.interaction?.release();
      });
    } else {
      this.#pendingActions = [];
      this.interaction?.release(data);
    }
  }

  handleMouse(event: TuiMouseEvent): TuiMouseEventResult | undefined {
    if (!this.interaction) return undefined;
    if (this.#engine) return this.#engine.handleMouse(event);
    if (event.type === "press") return { handled: true };
    if (event.type !== "click" || event.button !== "left") return undefined;
    const row = this.#passiveRows.find((candidate) => candidate.line === event.y);
    if (!row) return undefined;
    this.#pendingActions.push((engine) => engine.activateRow(row.id));
    // The host grants focus synchronously from the result below. A late load
    // must not steal it back from the editor or another view.
    void this.prepareInteraction().then(() => this.interaction?.requestRender()).catch(() => {
      this.#pendingActions = [];
      if (this.focused) this.interaction?.release();
    });
    return { focus: true, render: true };
  }

  #rowLimit(): number {
    return widgetRowLimit(this.maxRows, this.terminalRows?.());
  }

  #lastWidth: number | undefined;
  #lastLimit: number | undefined;
  #lastSnapshot: FabricDashboardSnapshot | undefined;
  #lastLines: string[] | undefined;
  // Above-editor components must not change height while a streamed turn is in
  // flight: pi-tui may have to redraw the whole screen when their line count
  // changes, and terminals such as Herdr can leave that redraw in scrollback.
  #turnRowLimit: number | undefined;
  #pending:
    | { width: number; limit: number; snapshot: FabricDashboardSnapshot; lines: string[] }
    | undefined;

  render(width: number): string[] {
    if (width <= 0) return [];
    const snapshot = this.snapshot();
    // The pane can shrink between renders, so the row budget is part of the
    // cache key: a box measured against a taller terminal must not be reused.
    const limit = this.#rowLimit();
    const streaming = this.stableWhileStreaming && isFabricWidgetStreaming(snapshot);
    if (streaming) this.#turnRowLimit ??= limit;
    else this.#turnRowLimit = undefined;
    // A stable turn budget must still fit the live pane after a resize.
    // Keep the smaller reservation for the rest of this turn, avoiding growth
    // when the pane is expanded again.
    if (streaming) this.#turnRowLimit = Math.min(this.#turnRowLimit!, limit);
    const renderLimit = streaming ? this.#turnRowLimit! : limit;
    const interactionChanged =
      this.#renderedFocus !== this.focused ||
      this.#renderedExpansion !== this.#interactionKey();
    if (interactionChanged) this.#pending = undefined;
    // An open pane reads a live log: reuse only the lines hasChanged() just
    // built, never the last frame, whose snapshot key says nothing about the log.
    const lines =
      !interactionChanged &&
      this.#pending?.width === width &&
      this.#pending.limit === renderLimit &&
      this.#pending.snapshot === snapshot
        ? this.#pending.lines
        : !interactionChanged && !this.expandedId &&
            this.#lastWidth === width &&
            this.#lastLimit === renderLimit &&
            this.#lastSnapshot === snapshot &&
            this.#lastLines
          ? this.#lastLines
          : this.#renderLines(snapshot, width, renderLimit, streaming);
    this.#pending = undefined;
    this.#renderedFocus = this.focused;
    this.#renderedExpansion = this.#interactionKey();
    this.#lastWidth = width;
    this.#lastLimit = renderLimit;
    this.#lastSnapshot = snapshot;
    this.#lastLines = lines;
    return lines;
  }

  hasChanged(): boolean {
    if (this.#lastWidth === undefined || this.#lastLines === undefined) return true;
    const snapshot = this.snapshot();
    const limit = this.#rowLimit();
    const streaming = this.stableWhileStreaming && isFabricWidgetStreaming(snapshot);
    if (streaming) this.#turnRowLimit ??= this.#lastLimit ?? limit;
    else this.#turnRowLimit = undefined;
    // A stable turn budget must still fit the live pane after a resize.
    // Keep the smaller reservation for the rest of this turn, avoiding growth
    // when the pane is expanded again.
    if (streaming) this.#turnRowLimit = Math.min(this.#turnRowLimit!, limit);
    const renderLimit = streaming ? this.#turnRowLimit! : limit;
    const interactionChanged =
      this.#renderedFocus !== this.focused ||
      this.#renderedExpansion !== this.#interactionKey();
    const lines = this.#renderLines(snapshot, this.#lastWidth, renderLimit, streaming);
    this.#pending = { width: this.#lastWidth, limit: renderLimit, snapshot, lines };
    if (lines.length === this.#lastLines.length &&
        lines.every((line, index) => line === this.#lastLines?.[index])) return interactionChanged;
    // Spinner-only differences are allowed to ride on the next Pi render.
    // Calling requestRender for every animation tick defeats Pi's render
    // coalescing and is especially noisy for above-editor widgets.
    const normalizeSpinner = (line: string): string => line.replace(/[◐◓◑◒]/g, "◐");
    if (!this.stableWhileStreaming) return true;
    return lines.length !== this.#lastLines.length ||
      lines.some((line, index) => normalizeSpinner(line) !== normalizeSpinner(this.#lastLines?.[index] ?? ""));
  }

  invalidate(): void {
    this.#pending = undefined;
    this.#lastWidth = undefined;
    this.#lastLimit = undefined;
    this.#lastSnapshot = undefined;
    this.#lastLines = undefined;
  }

  #renderLines(snapshot: FabricDashboardSnapshot, width: number, limit: number, streaming: boolean): string[] {
    const { lines, rows } = this.#content(snapshot);
    this.#passiveRows = rows.filter((row) => row.line < limit);
    const rendered = this.#engine
      ? this.#engine.render(lines, rows, width, limit)
      : this.#boundContent(this.#withFocusHint(lines, rows.length > 0), width, limit);
    if (streaming) while (rendered.length < limit) rendered.push("");
    return rendered;
  }

  /** Advertise the expand shortcut on the header while participant rows exist. */
  #withFocusHint(lines: string[], hasRows: boolean): string[] {
    if (!this.interaction || lines.length === 0 || !hasRows) return lines;
    const hint = this.focused ? "↑↓ select · ⏎ expand · esc back" : FABRIC_WIDGET_FOCUS_HINT;
    return [`${lines[0]} · ${this.theme.fg("dim", hint)}`, ...lines.slice(1)];
  }

  #content(snapshot: FabricDashboardSnapshot): { lines: string[]; rows: Array<FabricWidgetRow & { line: number }> } {
    const rows: Array<FabricWidgetRow & { line: number }> = [];
    const lines = this.#buildContent(snapshot, rows);
    return { lines, rows };
  }

  #buildContent(snapshot: FabricDashboardSnapshot, rows: Array<FabricWidgetRow & { line: number }> = []): string[] {
    const candidateRun = snapshot.runs[0];
    const candidateFinishedAt = candidateRun?.finishedAt ?? candidateRun?.updatedAt ?? 0;
    const run =
      candidateRun &&
      (candidateRun.status === "running" ||
        candidateFinishedAt > (snapshot.widgetDismissedAt ?? 0))
        ? candidateRun
        : undefined;
    this.retention.sync(snapshot);
    const orderedAgents = orderAgentsByCreation(ownAgents(snapshot));
    const activeAgents = orderedAgents.filter((agent) => isActiveStatus(agent.status));
    const activeAgentIds = new Set(activeAgents.map((agent) => agent.id));
    // Keep settle-time rows stable, then retire each completion independently.
    // Bound only current content: expiry must not leave a high-water blank row.
    const terminalAgents = orderedAgents
      .filter((agent) =>
        !activeAgentIds.has(agent.id) &&
        !isActiveStatus(agent.status) &&
        this.retention.visible(agent, snapshot))
      .sort((left, right) =>
        this.retention.finishedAt(right, snapshot.now) - this.retention.finishedAt(left, snapshot.now));
    const visibleActors = snapshot.actors.filter((actor) => actor.status !== "stopped");
    const activeActorWorkers = visibleActors
      .filter((actor) => actor.worker && isActiveStatus(actor.worker.status))
      .map((actor) => ({ ...actor.worker!, name: actor.name, actorId: actor.id }));
    const terminalActorWorkers = visibleActors
      .filter((actor) => actor.worker && !isActiveStatus(actor.worker.status))
      .map((actor) => ({ ...actor.worker!, name: actor.name, actorId: actor.id }));
    const nestedCalls =
      run?.calls.filter((call) => call.kind !== "agent" && call.kind !== "actor") ?? [];
    const title = run?.name ?? "Fabric session";
    const shells = snapshot.shells ?? [];
    const liveShells = shells.filter(job => job.finishedAt === undefined);
    const recentShells = shells.filter(job => job.finishedAt !== undefined && isRecentWidgetCompletion(job.finishedAt, snapshot.now));
    const headerStatus =
      run?.status ??
      (activeAgents.length > 0 || activeActorWorkers.length > 0 || liveShells.length > 0 ? "running" : "idle");
    const parts: string[] = [];

    const callTotal = nestedCalls.length;
    if (callTotal > 1) {
      const callDone = nestedCalls.filter(
        (call) => call.status === "completed" || call.status === "failed",
      ).length;
      parts.push(`${callDone}/${callTotal} calls`);
    }
    if (run?.currentPhaseId) {
      const phaseIndex = run.phases.findIndex((phase) => phase.id === run.currentPhaseId);
      const phase = run.phases[phaseIndex];
      if (phase) {
        const progress = phaseProgress(run, phase.id);
        parts.push(
          `${phaseIndex + 1}/${run.phases.length} ${safeText(phase.name)}${
            progress.total > 0 ? ` ${progress.completed}/${progress.total}` : ""
          }`,
        );
      }
    }
    if (liveShells.length > 0) parts.push(`${liveShells.length} shell${liveShells.length === 1 ? "" : "s"}`);
    if (activeAgents.length > 0) parts.push(`${activeAgents.length} running`);
    if (visibleActors.length > 0) parts.push(`${visibleActors.length} actor${visibleActors.length === 1 ? "" : "s"}`);
    const tokens = totalTokens(snapshot, run);
    if (tokens > 0) parts.push(`${formatTokens(tokens)} tok`);
    const cost = totalCost(snapshot, run);
    if (cost > 0) parts.push(formatCost(cost));
    // Live durations would imply a ticking clock. Only completed work gets a
    // duration; progress and status redraw exclusively on observed events.
    const elapsed = run?.finishedAt !== undefined && formatDuration(run.finishedAt - run.startedAt);
    if (elapsed) parts.push(elapsed);

    const glyph = colorStatus(this.theme, headerStatus, statusGlyph(headerStatus));
    const header = `${glyph} ${this.theme.fg("accent", "Fabric")} ${this.theme.fg(
      "muted",
      safeText(title),
    )}${parts.length > 0 ? this.theme.fg("dim", ` · ${parts.join(" · ")}`) : ""}`;
    const hasActiveConversations = activeAgents.some((agent) => !agent.stale) ||
      activeActorWorkers.length > 0 || visibleActors.some((actor) => isActiveStatus(actor.status));
    const taskHint = liveShells.length || recentShells.length ? this.theme.fg("dim", " · /fabric tasks · ctrl+alt+t") : "";
    const taskHeader = taskHint ? `${glyph} ${this.theme.fg("accent", "Fabric")}${taskHint}${parts.length ? this.theme.fg("dim", ` · ${parts.join(" · ")}`) : ""}` : header;
    const lines = [hasActiveConversations ? `${taskHeader} · ${this.theme.fg("dim", FABRIC_CONVERSATION_HINT)}` : taskHeader];
    for (const job of [...liveShells, ...recentShells].slice(0, 3)) {
      const elapsed = job.finishedAt !== undefined ? formatDuration(job.finishedAt - job.startedAt) : "";
      const status = job.stopping ? "stopping" : job.status;
      const label = job.monitor && job.finishedAt === undefined && !job.stopping ? `monitor:${job.monitor.delivery}` : status;
      const glyph = colorStatus(this.theme, status, statusGlyph(status));
      lines.push(
        `  ${glyph} ${this.theme.fg("muted", job.id.slice(0, 8))} ${this.theme.fg("muted", label)}` +
        `${this.theme.fg("dim", `${elapsed ? ` · ${elapsed}` : ""} · `)}${this.theme.fg("muted", safeText(job.description ?? job.command))}`,
      );
    }
    if (liveShells.length + recentShells.length > 3) lines.push(this.theme.fg("dim", `  +${liveShells.length + recentShells.length - 3} more shell tasks`));

    const pushParticipant = (agent: FabricUiAgent, kind: "agent" | "actor"): void => {
      const id = kind === "actor" ? agent.actorId ?? agent.id : agent.id;
      rows.push({ id, kind, name: agent.name, line: lines.length });
      lines.push(...agentLines(this.theme, agent));
    };
    for (const agent of activeAgents) pushParticipant(agent, "agent");
    for (const agent of activeActorWorkers) pushParticipant(agent, "actor");
    for (const agent of terminalActorWorkers) pushParticipant(agent, "actor");
    for (const agent of terminalAgents) pushParticipant(agent, "agent");
    return lines;
  }

  #boundContent(content: string[], width: number, limit: number): string[] {
    const bounded = content.slice(0, limit);
    if (content.length > bounded.length && bounded.length > 0) {
      const marker = this.theme.fg("dim", `+${content.length - bounded.length}`);
      const available = Math.max(0, width - visibleWidth(marker) - 1);
      const last = truncateToWidth(bounded[bounded.length - 1] ?? "", available, "");
      bounded[bounded.length - 1] = `${last} ${marker}`;
    }
    return bounded.map((line) => truncateToWidth(line, width));
  }
}
