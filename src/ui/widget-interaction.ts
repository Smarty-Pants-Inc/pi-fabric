import type { TuiMouseEvent, TuiMouseEventResult } from "@earendil-works/pi-tui";
import { matchesKey, truncateToWidth } from "@earendil-works/pi-tui";
import type { FabricWidget, FabricWidgetRow } from "./widget.js";
import { FABRIC_WIDGET_FOCUS_SHORTCUT, WIDGET_ACTIVITY_ROWS, WIDGET_ACTIVITY_LOG_LINES, widgetExpandedRowLimit } from "./widget.js";
import { widgetActivityLines, widgetDivider } from "./widget-activity.js";
import { safeText } from "./format.js";
import { AgentTranscriptReader } from "./transcript.js";
export { AgentTranscriptReader } from "./transcript.js";

type Row = FabricWidgetRow & { line: number };
export interface WidgetInteractionHost {
  widget: FabricWidget;
  content: () => { lines: string[]; rows: Row[] };
  rowLimit: () => number;
  bound: (lines: string[], width: number, limit: number) => string[];
  hint: (lines: string[], hasRows: boolean) => string[];
}

/** Loaded only by an explicit focus/click, never by rendering the passive widget. */
export class WidgetInteraction {
  #selectedId: string | undefined;
  #expandedId: string | undefined;
  #activityScroll = 0;
  #activityMaxScroll = 0;
  #paneRows = WIDGET_ACTIVITY_ROWS;
  #rows: Row[] = [];
  readonly #transcripts = new AgentTranscriptReader(WIDGET_ACTIVITY_LOG_LINES);
  constructor(readonly host: WidgetInteractionHost) {}
  get expandedId(): string | undefined { return this.#expandedId; }
  get key(): string {
    return `${this.host.widget.focused}|${this.#selectedId ?? ""}|${this.#expandedId ?? ""}|${this.#activityScroll}`;
  }
  selectDefault(): void {
    const rows = this.#selectableRows();
    if (!rows.some((row) => row.id === this.#selectedId)) this.#selectedId = this.#expandedId ?? rows[0]?.id;
  }
  handleInput(data: string): void {
    const { widget } = this.host;
    const rows = this.#selectableRows();
    const index = Math.max(0, rows.findIndex((row) => row.id === this.#selectedId));
    const select = (next: number): void => {
      const row = rows[Math.max(0, Math.min(rows.length - 1, next))];
      if (!row) return;
      this.#selectedId = row.id;
      if (this.#expandedId && this.#expandedId !== row.id) this.#expand(row.id);
    };
    if (matchesKey(data, "escape") || matchesKey(data, FABRIC_WIDGET_FOCUS_SHORTCUT)) {
      if (this.#expandedId && matchesKey(data, "escape")) this.#expandedId = undefined;
      else widget.interaction?.release();
    } else if (matchesKey(data, "up") || data === "k") {
      select(index - 1);
    } else if (matchesKey(data, "down") || data === "j") {
      select(index + 1);
    } else if (matchesKey(data, "enter") || data === " ") {
      if (this.#selectedId) this.#toggle(this.#selectedId);
    } else if (this.#expandedId && (matchesKey(data, "pageUp") || data === "K")) {
      this.#activityScroll = Math.min(this.#activityMaxScroll, this.#activityScroll + Math.max(1, this.#paneRows - 1));
    } else if (this.#expandedId && (matchesKey(data, "pageDown") || data === "J")) {
      this.#activityScroll = Math.max(0, this.#activityScroll - Math.max(1, this.#paneRows - 1));
    } else if (this.#expandedId && (matchesKey(data, "end") || data === "G")) {
      this.#activityScroll = 0;
    } else {
      // Unused keys still belong to the editor (including Ctrl+C and shortcuts).
      widget.interaction?.release(data);
      return;
    }
    widget.interaction?.requestRender();
  }
  handleMouse(event: TuiMouseEvent): TuiMouseEventResult | undefined {
    if (!this.host.widget.interaction) return undefined;
    if (event.type === "wheel" && this.#expandedId) {
      const delta = Math.sign(event.wheelDelta ?? 0);
      this.#activityScroll = Math.max(0, Math.min(this.#activityMaxScroll, this.#activityScroll - delta * 3));
      return { handled: true, render: true };
    }
    if (event.type === "press") return { handled: true };
    if (event.type !== "click" || event.button !== "left") return undefined;
    const row = this.#rows.find((candidate) => candidate.line === event.y);
    if (!row) return undefined;
    this.#selectedId = row.id;
    this.#toggle(row.id);
    return { focus: true, render: true };
  }
  activateRow(id: string): void {
    if (!this.#selectableRows().some((row) => row.id === id)) return;
    this.#selectedId = id;
    this.#toggle(id);
  }
  #toggle(id: string): void {
    if (this.#expandedId === id) this.#expandedId = undefined;
    else this.#expand(id);
  }
  #expand(id: string): void { this.#expandedId = id; this.#activityScroll = 0; }
  #selectableRows(): Row[] {
    const { lines, rows } = this.host.content();
    const { listLimit } = this.#layout(lines.length, this.#expandedId !== undefined, this.host.rowLimit());
    return rows.filter((row) => row.line < listLimit);
  }
  #layout(listLength: number, expanded: boolean, limit: number): { listLimit: number; paneRows: number } {
    if (!expanded) return { listLimit: limit, paneRows: 0 };
    const { widget } = this.host;
    const budget = widgetExpandedRowLimit(widget.maxRows, widget.terminalRows?.());
    // Two dividers frame the pane. Keep at least two list rows and one pane row.
    const listLimit = Math.max(2, Math.min(listLength, limit, budget - 5));
    const paneRows = Math.max(1, Math.min(WIDGET_ACTIVITY_ROWS, budget - Math.min(listLength, listLimit) - 2));
    return { listLimit, paneRows };
  }
  render(lines: string[], rows: Row[], width: number, limit: number): string[] {
    const { widget, bound, hint } = this.host;
    if (this.#selectedId && !rows.some((row) => row.id === this.#selectedId)) this.#selectedId = rows[0]?.id;
    const expanded = rows.find((row) => row.id === this.#expandedId);
    if (this.#expandedId && !expanded) this.#expandedId = undefined;
    const marked = lines.map((line, index) => {
      const row = rows.find((candidate) => candidate.line === index);
      if (!row || !widget.focused || row.id !== this.#selectedId) return line;
      return `${widget.theme.fg("accent", "▸")}${line.replace(/^ /, "")}`;
    });
    const { listLimit, paneRows } = this.#layout(lines.length, expanded !== undefined, limit);
    this.#rows = rows.filter((row) => row.line < listLimit);
    if (!expanded) return bound(hint(marked, rows.length > 0), width, limit);
    const list = bound(hint(marked, true), width, listLimit);
    this.#paneRows = paneRows;
    const activity = widgetActivityLines(widget.theme, widget.interaction?.activity(expanded, this.#transcripts), width,
      paneRows, this.#activityScroll, widget.interaction?.hideThinking?.() ?? false);
    this.#activityMaxScroll = activity.maxScroll;
    this.#activityScroll = Math.min(this.#activityScroll, activity.maxScroll);
    const position = this.#activityScroll > 0 ? ` · ↑${this.#activityScroll}` : " · live";
    const help = widget.focused ? "↑↓ switch · PgUp/PgDn scroll · ⏎/click collapse · esc close"
      : `click or ${FABRIC_WIDGET_FOCUS_SHORTCUT} to focus`;
    return [...list,
      widgetDivider(widget.theme, width, widget.theme.fg("accent", `${safeText(expanded.name)} activity${position}`)),
      ...activity.lines, widgetDivider(widget.theme, width, widget.theme.fg("dim", help)),
    ].map((line) => truncateToWidth(line, width));
  }
}
