import { initTheme } from "@earendil-works/pi-coding-agent";
import type { EditorTheme, TUI } from "@earendil-works/pi-tui";
import { CURSOR_MARKER, stripTerminalSequences, truncateToWidth, visibleWidth } from "@earendil-works/pi-tui";
import { describe, expect, it } from "vitest";
import { CustomEditor } from "../node_modules/@earendil-works/pi-coding-agent/dist/modes/interactive/components/custom-editor.js";
import { WorkingStatusIndicator } from "../node_modules/@earendil-works/pi-coding-agent/dist/modes/interactive/components/status-indicator.js";
import { BorderStatusEditor, BorderWorkingIndicator } from "../src/ui/border-status-editor.js";

initTheme("dark", false);

const theme: EditorTheme = {
  borderColor: (text) => text,
  selectList: {
    selectedPrefix: (text) => text,
    selectedText: (text) => text,
    description: (text) => text,
    scrollInfo: (text) => text,
    noMatch: (text) => text,
  },
};

// Only rows/requestRender are touched while rendering and constructing loaders.
const tui = () => ({
  terminal: { rows: 40, columns: 100 },
  requestRender: () => {},
  mode: "regular",
}) as unknown as TUI;

/** Top border Pi renders for the default editor with an embedded indicator. */
const piTopBorder = (width: number, text: string): string => {
  const editor = new CustomEditor(tui(), theme, {} as never, { embedWorkingStatus: true });
  const indicator = new WorkingStatusIndicator(tui(), "Working");
  editor.setWorkingStatusIndicator(indicator);
  editor.setText(text);
  const line = editor.render(width)[0] ?? "";
  indicator.dispose();
  return stripTerminalSequences(line);
};

/** Top border the Fabric preview renders for the same editor state. */
const fabricTopBorder = (width: number, text: string, color = (value: string): string => value): string => {
  const editor = new BorderStatusEditor(tui(), theme, { paddingX: 0 });
  const indicator = new BorderWorkingIndicator(tui(), color, "Working");
  editor.setWorkingStatusIndicator(indicator);
  editor.setText(text);
  const line = editor.render(width)[0] ?? "";
  indicator.stop();
  return stripTerminalSequences(line);
};

const scrolledDraft = Array.from({ length: 200 }, (_, index) => `line ${index}`).join("\n");

describe("BorderStatusEditor inherits Pi's embedded working status", () => {
  it.each([100, 60, 40, 24, 20, 16, 14, 12, 8, 5, 3, 2, 1])(
    "matches Pi's editor border at width %i while working", (width) => {
      expect(fabricTopBorder(width, "draft")).toBe(piTopBorder(width, "draft"));
    },
  );

  it.each([100, 60, 40, 24, 20, 16, 14, 12, 8, 5, 3, 2, 1])(
    "matches Pi's scrolled border at width %i", (width) => {
      expect(fabricTopBorder(width, scrolledDraft)).toBe(piTopBorder(width, scrolledDraft));
      // Pi keeps the ↑ N more label only when it fits beside the status.
      if (width >= 24) expect(fabricTopBorder(width, scrolledDraft)).toContain("more");
    },
  );

  it("leaves the border alone without an indicator and after it clears", () => {
    const piPlain = (): string => {
      const piEditor = new CustomEditor(tui(), theme, {} as never, { embedWorkingStatus: true });
      piEditor.setText("draft");
      return stripTerminalSequences(piEditor.render(40)[0] ?? "");
    };
    const editor = new BorderStatusEditor(tui(), theme, { paddingX: 0 });
    editor.setText("draft");
    expect(stripTerminalSequences(editor.render(40)[0] ?? "")).toBe(piPlain());
    const indicator = new BorderWorkingIndicator(tui(), (text) => text, "Working");
    editor.setWorkingStatusIndicator(indicator);
    expect(stripTerminalSequences(editor.render(40)[0] ?? "")).toContain("Working");
    editor.setWorkingStatusIndicator(undefined);
    indicator.stop();
    expect(stripTerminalSequences(editor.render(40)[0] ?? "")).toBe(piPlain());
  });

  it.each(["regular", "fullscreen"])("keeps native content, cursor and status labels with a border badge in %s", (mode) => {
    const ui = { ...tui(), mode } as TUI;
    const editor = new BorderStatusEditor(ui, theme, { paddingX: 0 });
    editor.focused = true;
    const indicator = new BorderWorkingIndicator(ui, (text) => text, "Working");
    editor.setWorkingStatusIndicator(indicator);
    editor.setText(scrolledDraft);
    try {
      for (const width of [1, 2, 8, 16, 24, 40, 80, 120]) {
        editor.topBorderBadge = undefined;
        const native = editor.render(width);
        editor.topBorderBadge = (budget) => ` ${truncateToWidth("worker", budget - 2, "…")} `;
        const decorated = editor.render(width);
        expect(decorated.slice(1)).toEqual(native.slice(1));
        expect(decorated.join("\n")).toContain(CURSOR_MARKER);
        // Native content may exceed pathological one-cell widths; only the
        // decorated border is ours, and all remaining rows match Pi exactly.
        expect(visibleWidth(decorated[0]!)).toBeLessThanOrEqual(width);
        if (width >= 40) {
          // Pi collapses to spinner-only when the centered overflow label
          // and the full status cannot both fit beside the target.
          expect(decorated[0]).toContain("⠋");
          if (width >= 80) expect(decorated[0]).toContain("Working");
          expect(decorated[0]).toContain("more");
          expect(decorated[0]).toMatch(/ worker ──$/);
        }
      }
    } finally { indicator.stop(); }
  });

  it("paints the spinner and the label with the editor border color", () => {
    const statusColors: string[] = [];
    const frameColors: string[] = [];
    const editor = new BorderStatusEditor(tui(), {
      ...theme,
      borderColor: (text) => { frameColors.push(text); return text; },
    }, { paddingX: 0 });
    const indicator = new BorderWorkingIndicator(tui(), (value) => {
      statusColors.push(value);
      return value;
    }, "Working");
    editor.setWorkingStatusIndicator(indicator);
    editor.render(60);
    indicator.stop();
    // Pi's embedded indicator paints spinner and label with the border color
    // (not accent/muted), and the frame keeps that same border color.
    expect(statusColors).toContain("⠋");
    expect(statusColors).toContain("Working");
    expect(frameColors).toContain("── ");
  });
});
