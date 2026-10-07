import "./fixtures/conversation-host.js";
import { initTheme, type Theme } from "@earendil-works/pi-coding-agent";
import { ScrollView, stripTerminalSequences, visibleWidth, type ScrollViewScrollbar, type TUI, type TuiMouseEvent } from "@earendil-works/pi-tui";
import { renderLayoutFrame } from "../node_modules/@earendil-works/pi-tui/dist/layout.js";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ConversationScrollbar } from "../src/ui/conversation-scrollbar.js";
import { FabricConversationState, FabricConversationView } from "../src/ui/conversation.js";
import { FabricConversationTranscriptRenderer } from "../src/ui/conversation-render.js";
import { nativeTranscript } from "./fixtures/native-conversation.js";

const theme = {
  fg: (color: string, text: string) => color === "scrollbarTrack" ? `\x1b[90m${text}\x1b[39m` : color === "scrollbarThumb" ? `\x1b[37m${text}\x1b[39m` : text,
  bg: (_color: string, text: string) => text,
  bold: (text: string) => text, italic: (text: string) => text,
  underline: (text: string) => text, strikethrough: (text: string) => text,
} as unknown as Theme;
const mouse = (type: TuiMouseEvent["type"], x: number, y: number): TuiMouseEvent => ({
  type, x, y, screenX: x, screenY: y, width: 40, height: 24,
  button: type === "move" ? "none" : "left", shift: false, alt: false, ctrl: false,
});
afterEach(() => { vi.restoreAllMocks(); vi.useRealTimers(); });

describe("Pi 1.0 participant scrollbar parity", () => {
  it.each(["auto", "always", "hidden"] as const)("matches native layout bytes and geometry (%s)", (mode) => {
    vi.useFakeTimers();
    for (const width of [1, 2, 9, 40]) for (const height of [1, 2, 7]) for (const count of [0, 1, 7, 51]) {
      const bar = new ConversationScrollbar(theme, mode);
      const contentWidth = bar.contentWidth(width);
      const samples = ["", "x".repeat(contentWidth), "界".repeat(Math.ceil(contentWidth / 2)),
        `\x1b[1;38;2;40;49;0;48;2;30;40;50m${"z".repeat(contentWidth)}\x1b[0m`,
        `\x1b[48;5;123m\x1b]8;;https://example.com\x07${"a".repeat(contentWidth)}\x1b]8;;\x07\x1b[0m`,
        `\x1b[104m${"b".repeat(contentWidth)}\x1b[49m`];
      const lines = Array.from({ length: count }, (_, i) => samples[i % samples.length]!);
      const native = new ScrollView({ render: () => lines, invalidate: () => {} }, {
        follow: "end", scrollbar: mode,
        scrollbarTrackStyle: (text) => theme.fg("scrollbarTrack", text),
        scrollbarThumbStyle: (text) => theme.fg("scrollbarThumb", text),
      });
      expect(native.getContentWidth(width)).toBe(contentWidth);
      renderLayoutFrame(native, width, height, () => {});
      for (const scroll of [0, Math.floor(Math.max(0, count - height) / 2), Math.max(0, count - height)]) {
        native.scrollTo(scroll, { disableFollow: true });
        bar.sync(width, 0, height, count, scroll, false, () => {});
        const actual = lines.slice(scroll, scroll + height);
        while (actual.length < height) actual.push("");
        bar.paint(actual);
        const expected = renderLayoutFrame(native, width, height, () => {}).lines;
        expect(actual, JSON.stringify({ mode, width, height, count, scroll })).toEqual(expected);
      }
      bar.reset();
      native.setScrollbar("hidden");
    }
  });

  it("uses native auto timing, hover glyphs, drag geometry and cleanup", () => {
    vi.useFakeTimers();
    const requestRender = vi.fn();
    const bar = new ConversationScrollbar(theme);
    const frame = () => { const lines = Array(10).fill(""); bar.paint(lines); return lines.map(stripTerminalSequences); };
    bar.sync(40, 2, 10, 100, 90, true, requestRender);
    expect(frame().every((line) => line === "")).toBe(true);
    bar.sync(40, 2, 10, 100, 40, false, requestRender);
    expect(frame().filter((line) => line.endsWith("┃"))).toHaveLength(2);
    vi.advanceTimersByTime(999);
    expect(frame()[0]).toContain("│");
    vi.advanceTimersByTime(1);
    expect(frame().every((line) => line === "")).toBe(true);
    const scrollTo = vi.fn();
    bar.handleMouse(mouse("move", 39, 6), scrollTo);
    expect(frame().filter((line) => line.endsWith("█"))).toHaveLength(2);
    vi.advanceTimersByTime(2000);
    expect(frame()[0]).toContain("│");
    expect(bar.handleMouse(mouse("press", 39, 6), scrollTo)).toBe(true);
    expect(bar.handleMouse(mouse("drag", 10, 11), scrollTo)).toBe(true);
    expect(scrollTo).toHaveBeenLastCalledWith(90);
    bar.handleMouse(mouse("release", 10, 11), scrollTo);
    vi.advanceTimersByTime(1000);
    expect(frame().every((line) => line === "")).toBe(true);
    bar.sync(40, 2, 10, 100, 0, false, requestRender);
    bar.handleMouse(mouse("press", 39, 7), scrollTo);
    expect(scrollTo).toHaveBeenLastCalledWith(45);
    bar.reset();
    requestRender.mockClear();
    vi.advanceTimersByTime(2000);
    expect(requestRender).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
  });

  it("never paints image control payloads", () => {
    const bar = new ConversationScrollbar(theme, "always");
    bar.sync(40, 0, 3, 3, 0, true, () => {});
    const lines = ["\x1b_Ga=T;AAAA\x1b\\", "\x1b]1337;File=AAAA\x07", "\x1bPqAAAA\x1b\\"];
    const original = [...lines];
    bar.paint(lines);
    expect(lines).toEqual(original);
    bar.reset();
  });
});

function fixture(mode: ScrollViewScrollbar = "auto") {
  initTheme("dark", false);
  let lines = Array.from({ length: 80 }, (_, i) => `row ${i}`);
  const renderer = vi.spyOn(FabricConversationTranscriptRenderer.prototype, "render").mockImplementation(() => lines);
  const terminal = { rows: 22, columns: 40 };
  const requestRender = vi.fn();
  const state = new FabricConversationState();
  const view = new FabricConversationView({ terminal, requestRender } as unknown as TUI, theme, {
    state, initialTargetId: "child", targets: () => [{ id: "child", name: "Child", kind: "agent", status: "idle",
      canSteer: true, canFollowUp: true, canStop: true }],
    appearance: { editorPaddingX: 0, outputPad: 0, scrollbar: mode },
    transcript: () => nativeTranscript(), loadOlder: () => false, loadNewer: () => false, loadLatest: () => false,
    send: vi.fn(), stop: vi.fn(), close: vi.fn(),
  });
  return { view, state, terminal, renderer, requestRender,
    append: () => { lines = [...lines, "new activity"]; },
    frame: () => view.render(terminal.columns).map(stripTerminalSequences) };
}

describe("participant scrollbar integration", () => {
  it("keeps keyboard/wheel focus, pinned history, live tail and resize behavior", () => {
    vi.useFakeTimers();
    const f = fixture();
    try {
      const initial = f.frame();
      expect(initial.some((line) => /[┃│]$/.test(line))).toBe(false);
      f.view.handleInput("\x1b[5~");
      const scrolled = f.frame();
      expect(f.state.view("child").following).toBe(false);
      expect(scrolled.some((line) => line.endsWith("┃"))).toBe(true);
      const position = f.state.view("child").scroll;
      f.append();
      f.frame();
      expect(f.state.view("child").scroll).toBe(position);
      const barRows = scrolled.map((line, row) => ({ line, row })).filter(({ line }) => /[┃│]$/.test(line));
      expect(barRows[0]!.row).toBe(0);
      expect(scrolled.slice(barRows.at(-1)!.row + 1).every((line) => !/[┃│]$/.test(line))).toBe(true);
      f.view.handleMouse({ ...mouse("wheel", 0, 2), wheelDelta: -3 });
      f.frame();
      expect(f.state.view("child").scroll).toBe(position - 3);
      const bottom = barRows.at(-1)!.row;
      f.view.handleMouse(mouse("press", 39, bottom));
      f.view.handleMouse(mouse("drag", 39, bottom + 10));
      f.view.handleMouse(mouse("release", 39, bottom + 10));
      f.frame();
      expect(f.state.view("child").following).toBe(true);
      f.append();
      expect(f.frame().join("\n")).toContain("new activity");
      for (const width of [1, 2, 8, 40]) for (const height of [1, 6, 22]) {
        f.terminal.columns = width; f.terminal.rows = height;
        const frame = f.frame();
        expect(frame).toHaveLength(height);
        expect(frame.every((line) => visibleWidth(line) <= width)).toBe(true);
        expect(f.state.view("child").following).toBe(true);
      }
    } finally { f.view.dispose(); f.state.clear(); }
  });

  it.each(["always", "hidden"] as const)("honors %s without narrowing editor/dock", (mode) => {
    const f = fixture(mode);
    try {
      const lines = f.frame();
      expect(f.renderer.mock.calls.at(-1)?.[1]).toBe(mode === "always" ? 39 : 40);
      expect(lines.some((line) => line.endsWith("┃"))).toBe(mode === "always");
      expect(lines.filter((line) => /^─+$/.test(line)).every((line) => line.length === 40)).toBe(true);
      f.view.handleInput("\x0e");
      expect(f.frame().some((line) => /[┃│█]$/.test(line))).toBe(false);
    } finally { f.view.dispose(); f.state.clear(); }
  });
});
