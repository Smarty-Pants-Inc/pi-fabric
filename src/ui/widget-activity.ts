import type { Theme } from "@earendil-works/pi-coding-agent";
import { truncateToWidth, visibleWidth, wrapTextWithAnsi } from "@earendil-works/pi-tui";
import type { FabricAgentTranscript, FabricTranscriptEntry } from "./transcript.js";
import { safeText } from "./format.js";

// Preserve the public constants without making the renderer an eager dependency.
export { FABRIC_WIDGET_FOCUS_SHORTCUT, FABRIC_WIDGET_FOCUS_HINT,
  WIDGET_ACTIVITY_ROWS, WIDGET_ACTIVITY_LOG_LINES } from "./widget.js";

const ENTRY_TEXT_LINES = 4;

const toolGlyph = (theme: Theme, status: string | undefined): string =>
  status === "running"
    ? theme.fg("accent", "◐")
    : status === "failed"
      ? theme.fg("error", "✗")
      : theme.fg("success", "✓");

const oneLine = (value: string | undefined): string =>
  safeText(value ?? "").replace(/\s+/g, " ").trim();

/** Last `count` wrapped lines of `text`: the newest reasoning is the useful part. */
const tailLines = (text: string, width: number, count: number): string[] => {
  const wrapped = text
    .split("\n")
    .flatMap((paragraph) => wrapTextWithAnsi(paragraph, Math.max(1, width)))
    .filter((line) => line.trim().length > 0);
  return wrapped.slice(-count);
};

const entryLines = (theme: Theme, entry: FabricTranscriptEntry, width: number, hideThinking: boolean): string[] => {
  const inner = Math.max(1, width - 4);
  const body = (text: string | undefined, color: "dim" | "muted"): string[] =>
    text ? tailLines(safeText(text), inner, ENTRY_TEXT_LINES).map((line) => `    ${theme.fg(color, line)}`) : [];
  switch (entry.kind) {
    case "thinking":
      return [
        `  ${theme.fg("dim", entry.status === "running" ? "∴ thinking…" : "∴ thinking")}`,
        // Pi's hideThinkingBlock keeps the marker and drops the reasoning text, as in its own chat.
        ...(hideThinking ? [] : body(entry.text, "dim")),
      ];
    case "tool": {
      const args = oneLine(entry.text);
      return [
        `  ${toolGlyph(theme, entry.status)} ${theme.fg("toolTitle", theme.bold(safeText(entry.toolName ?? entry.label)))}${
          args ? theme.fg("dim", ` ${args}`) : ""
        }`,
      ];
    }
    case "assistant":
      return [`  ${theme.fg("accent", "◆")} ${theme.fg("accent", safeText(entry.label))}`, ...body(entry.text, "muted")];
    case "user":
      return [`  ${theme.fg("muted", "›")} ${theme.fg("muted", oneLine(entry.text) || safeText(entry.label))}`];
    case "error":
      return [`  ${theme.fg("error", "✗")} ${theme.fg("error", safeText(entry.label))}`, ...body(entry.text, "muted")];
    default:
      return [`  ${theme.fg("dim", "·")} ${theme.fg("dim", `${safeText(entry.label)}${entry.text ? ` ${oneLine(entry.text)}` : ""}`)}`];
  }
};

/**
 * Activity lines for an expanded widget participant, newest at the bottom.
 * `scroll` counts lines back from the newest; the caller clamps it with the
 * returned `maxScroll`. `hideThinking` mirrors Pi's hideThinkingBlock setting.
 */
export const widgetActivityLines = (
  theme: Theme,
  transcript: FabricAgentTranscript | undefined,
  width: number,
  rows: number,
  scroll: number,
  hideThinking = false,
): { lines: string[]; maxScroll: number } => {
  if (!transcript || transcript.entries.length === 0) {
    return { lines: [`  ${theme.fg("dim", "Waiting for activity…")}`], maxScroll: 0 };
  }
  const all = transcript.entries.flatMap((entry) => entryLines(theme, entry, width, hideThinking));
  const maxScroll = Math.max(0, all.length - rows);
  const offset = Math.max(0, Math.min(scroll, maxScroll));
  const end = all.length - offset;
  const lines = all.slice(Math.max(0, end - rows), end).map((line) => truncateToWidth(line, width, "…"));
  return { lines, maxScroll };
};

/** A full-width divider used above and below the expanded pane. */
export const widgetDivider = (theme: Theme, width: number, label = ""): string => {
  const text = label ? ` ${label} ` : "";
  return theme.fg("border", `──${text}${"─".repeat(Math.max(0, width - 2 - visibleWidth(text)))}`);
};
