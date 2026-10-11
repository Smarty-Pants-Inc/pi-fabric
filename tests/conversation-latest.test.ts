import "./fixtures/conversation-host.js";
import { initTheme, type Theme } from "@earendil-works/pi-coding-agent";
import { ScrollView, getKeybindings, setKeybindings, KeybindingsManager, stripTerminalSequences, visibleWidth, type Terminal, type TUI, type TuiMouseEvent } from "@earendil-works/pi-tui";
import { renderLayoutFrame } from "../node_modules/@earendil-works/pi-tui/dist/layout.js";
import { createInteractiveTui } from "../node_modules/@earendil-works/pi-coding-agent/dist/modes/interactive/tui-renderer.js";
import { theme as nativeTheme } from "../node_modules/@earendil-works/pi-coding-agent/dist/modes/interactive/theme/theme.js";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ConversationLatest } from "../src/ui/conversation-latest.js";
import { FabricConversationState, FabricConversationView } from "../src/ui/conversation.js";
import { FabricConversationTranscriptRenderer } from "../src/ui/conversation-render.js";
import { nativeTranscript } from "./fixtures/native-conversation.js";

const mouse = (type: TuiMouseEvent["type"], x: number, y: number): TuiMouseEvent => ({
  type, x, y, screenX: x, screenY: y, width: 80, height: 24,
  button: "left", shift: false, alt: false, ctrl: false,
});
const theme = {
  fg: (_color: string, text: string) => text, bg: (_color: string, text: string) => text,
  bold: (text: string) => text, italic: (text: string) => text,
  underline: (text: string) => text, strikethrough: (text: string) => text,
} as unknown as Theme;
afterEach(() => { vi.restoreAllMocks(); vi.useRealTimers(); });

describe("native jump-to-latest parity", () => {
  it("matches the actual Pi fullscreen renderer's label, style and placement", () => {
    initTheme("dark", false);
    const native = createInteractiveTui({ tuiMode: "fullscreen", showHardwareCursor: false, logDirectory: "/tmp",
      terminal: { columns: 80, rows: 24 } as Terminal }) as unknown as {
        compositeScrollToEndIndicator(lines: string[], layout: ReturnType<typeof renderLayoutFrame>, width: number): string[];
      };
    const originalKeys = getKeybindings();
    try {
      for (const keys of [["end"], ["ctrl+end", "alt+down"], []] as const) {
        setKeybindings(new KeybindingsManager({ "tui.altScreen.bottom": { defaultKeys: [...keys] } }));
        expect(getKeybindings().getKeys("tui.altScreen.bottom")).toEqual([...keys]);
        for (const width of [1, 2, 8, 30, 34, 35, 80]) for (const scrollbar of ["hidden", "always"] as const) {
          const scroll = new ScrollView({ render: () => Array(30).fill("transcript"), invalidate: () => {} }, { follow: "end", scrollbar });
          renderLayoutFrame(scroll, width, 5, () => {});
          scroll.scrollTo(0);
          const layout = renderLayoutFrame(scroll, width, 5, () => {});
          const expected = native.compositeScrollToEndIndicator(layout.lines, layout, width);
          const actual = [...layout.lines];
          const latest = new ConversationLatest();
          latest.paint(actual, width, 0, true, scroll.isScrollbarVisible, nativeTheme);
          expect(actual, JSON.stringify({ keys, width, scrollbar })).toEqual(expected);
        }
      }
    } finally { setKeybindings(originalKeys); }
  });

  it("bounds left-press hitboxes, hides stale bounds and skips image rows", () => {
    const latest = new ConversationLatest();
    const lines = ["history", "history"];
    latest.paint(lines, 80, 3, true, true, theme);
    const label = " ↓ Jump to latest message · End ";
    const column = Math.floor((80 - label.length) / 2);
    expect(latest.hit(mouse("press", column, 4))).toBe(true);
    expect(latest.hit(mouse("press", column + label.length - 1, 4))).toBe(true);
    for (const x of [column - 1, column + label.length, 79]) expect(latest.hit(mouse("press", x, 4))).toBe(false);
    expect(latest.hit(mouse("press", column, 3))).toBe(false);
    for (const type of ["drag", "release", "click", "wheel"] as const) expect(latest.hit(mouse(type, column, 4))).toBe(false);
    expect(latest.hit({ ...mouse("press", column, 4), button: "right" })).toBe(false);
    latest.paint(lines, 80, 3, false, false, theme);
    expect(latest.hit(mouse("press", column, 4))).toBe(false);
    const image = ["\x1b_Ga=T;AAAA\x1b\\"];
    latest.paint(image, 80, 4, true, false, theme);
    expect(image).toEqual(["\x1b_Ga=T;AAAA\x1b\\"]);
    expect(latest.hit(mouse("press", column, 4))).toBe(false);
  });
});

function fixture() {
  initTheme("dark", false);
  vi.useFakeTimers();
  let history = Array.from({ length: 80 }, (_, i) => `row ${i}`);
  vi.spyOn(FabricConversationTranscriptRenderer.prototype, "render").mockImplementation(() => history);
  let transcript = nativeTranscript();
  const loadLatest = vi.fn(() => { transcript = nativeTranscript(); return true; });
  const terminal = { columns: 80, rows: 22 };
  const state = new FabricConversationState();
  state.view("child").draft = "draft stays here";
  const view = new FabricConversationView({ terminal, requestRender: vi.fn() } as unknown as TUI, theme, {
    state, initialTargetId: "child", targets: () => [{ id: "child", name: "Child", kind: "agent", status: "idle",
      canSteer: true, canFollowUp: true, canStop: true }],
    transcript: () => transcript, loadOlder: () => false, loadNewer: () => false, loadLatest,
    send: vi.fn(), stop: vi.fn(), close: vi.fn(),
  });
  return { view, state, terminal, loadLatest,
    frame: () => view.render(terminal.columns).map(stripTerminalSequences),
    newer: () => { transcript = nativeTranscript([], { hasNewer: true }); },
    append: () => { history = [...history, "new output while pinned"]; },
  };
}

describe("participant latest-history action", () => {
  it("stays visible during pinned output, loads newest history on press and preserves draft/focus", () => {
    const f = fixture();
    try {
      expect(f.frame().join("\n")).not.toContain("Jump to latest");
      f.view.handleInput("\x1b[5~");
      const pinned = f.state.view("child").scroll;
      f.append(); f.newer();
      const frame = f.frame();
      expect(f.state.view("child").scroll).toBe(pinned);
      const row = frame.findIndex((line) => line.includes("Jump to latest message · End"));
      const x = frame[row]!.indexOf("↓");
      expect(row).toBeGreaterThan(0);
      expect(f.view.focused).toBe(true);
      f.view.handleMouse(mouse("press", x, row));
      f.view.handleMouse(mouse("release", x, row));
      expect(f.loadLatest).toHaveBeenCalledTimes(1);
      expect(f.state.view("child").following).toBe(true);
      expect(f.state.view("child").draft).toBe("draft stays here");
      expect(f.view.focused).toBe(true);
      expect(f.frame().join("\n")).not.toContain("Jump to latest");
      f.view.handleInput("!");
      expect(f.state.view("child").draft).toBe("draft stays here!");
      f.view.handleInput("\x1b[5~");
      f.frame();
      f.view.handleInput("\x1b[F");
      expect(f.loadLatest).toHaveBeenCalledTimes(2);
      expect(f.frame().join("\n")).not.toContain("Jump to latest");
    } finally { f.view.dispose(); f.state.clear(); }
  });

  it("offers latest for unloaded newer history even at the loaded window's bottom", () => {
    const f = fixture();
    try {
      f.newer();
      expect(f.state.view("child").following).toBe(true);
      expect(f.frame().join("\n")).toContain("Jump to latest message · End");
      f.view.handleInput("\x1b[F");
      expect(f.loadLatest).toHaveBeenCalledTimes(1);
      expect(f.frame().join("\n")).not.toContain("Jump to latest");
    } finally { f.view.dispose(); f.state.clear(); }
  });

  it("does not activate from a selection drag and recomputes bounds after resize/picker", () => {
    const f = fixture();
    try {
      f.frame(); f.view.handleInput("\x1b[5~");
      let frame = f.frame();
      let row = frame.findIndex((line) => line.includes("Jump to latest"));
      const x = frame[row]!.indexOf("↓");
      f.view.handleMouse(mouse("press", 0, 1));
      f.view.handleMouse(mouse("drag", x, row));
      f.view.handleMouse(mouse("release", x, row));
      expect(f.loadLatest).not.toHaveBeenCalled();
      f.terminal.columns = 18;
      frame = f.frame();
      expect(frame.every((line) => visibleWidth(line) <= 18)).toBe(true);
      row = frame.findIndex((line) => line.includes("↓ Jump"));
      expect(row).toBeGreaterThan(0);
      f.view.handleMouse(mouse("press", 1, row));
      expect(f.loadLatest).toHaveBeenCalledTimes(1);
      f.view.handleInput("\x1b[5~"); f.frame();
      f.view.handleInput("\x0e");
      expect(f.frame().join("\n")).not.toContain("↓ Jump");
      f.view.handleMouse(mouse("press", 1, row));
      expect(f.loadLatest).toHaveBeenCalledTimes(1);
      f.terminal.rows = 1;
      expect(f.frame()).toHaveLength(1);
    } finally { f.view.dispose(); f.state.clear(); }
  });
});
