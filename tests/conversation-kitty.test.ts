import "./fixtures/conversation-host.js";
import { initTheme, type Theme } from "@earendil-works/pi-coding-agent";
import { getCapabilities, Image, setCapabilities, TuiAltScreen, TuiMainScreen } from "@earendil-works/pi-tui";
import { createInteractiveTuiReference } from "../node_modules/@earendil-works/pi-coding-agent/dist/modes/interactive/tui-renderer.js";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { FabricConversationState, FabricConversationView } from "../src/ui/conversation.js";
import { FabricConversationTranscriptRenderer } from "../src/ui/conversation-render.js";
import { retainImageSafeOverlays } from "../src/ui/image-overlays.js";
import { nativeTranscript, userMessage } from "./fixtures/native-conversation.js";
import { KittyTerminal, PNG } from "./fixtures/kitty-terminal.js";

const theme = {
  fg: (_color: string, text: string) => text, bg: (_color: string, text: string) => text,
  bold: (text: string) => text, italic: (text: string) => text,
  underline: (text: string) => text, strikethrough: (text: string) => text,
} as unknown as Theme;
let capabilities: ReturnType<typeof getCapabilities>;
beforeEach(() => {
  initTheme("dark", false);
  capabilities = getCapabilities();
  setCapabilities({ images: "kitty", trueColor: true, hyperlinks: true });
});
afterEach(() => { setCapabilities(capabilities); vi.restoreAllMocks(); });
const header = (line: string) => Object.fromEntries(/\x1b_G([^;]*);/.exec(line)![1]!.split(",").map((part) => part.split("=")));

describe("native tool images inside ctrl+shift+a", () => {
  for (const Renderer of [TuiMainScreen, TuiAltScreen]) for (const scrollbar of ["always", "auto", "hidden"] as const) {
    it(`scrolls, resizes, switches targets and restores Main (${Renderer.name}, ${scrollbar})`, () => {
      const terminal = new KittyTerminal();
      const tui = new Renderer(terminal);
      tui.addChild(new Image(PNG, "image/png", { fallbackColor: (text) => text }, { imageId: 909, maxWidthCells: 8 }));
      const proxy = createInteractiveTuiReference(() => tui);
      const release = retainImageSafeOverlays(proxy);
      const state = new FabricConversationState();
      state.view("child").toolsExpanded = true;
      const transcript = nativeTranscript([userMessage(Array.from({ length: 40 }, (_, i) => `history ${i}`).join("\n"))], {
        streaming: { active: false, tools: [{ toolCallId: "read-image", toolName: "read", status: "completed", args: { path: "image.png" },
          result: { content: [{ type: "image", data: PNG, mimeType: "image/png" }] } },
          { toolCallId: "after", toolName: "read", status: "completed", args: { path: "after.txt" },
            result: { content: [{ type: "text", text: Array.from({ length: 30 }, (_, i) => `after ${i}`).join("\n") }] } }] },
      });
      const render = vi.spyOn(FabricConversationTranscriptRenderer.prototype, "render");
      const view = new FabricConversationView(proxy, theme, {
        state, initialTargetId: "child", appearance: { scrollbar, imageWidthCells: 20, editorPaddingX: 0, outputPad: 1, copyOnSelect: false },
        targets: () => ["child", "other"].map((id) => ({ id, name: id, kind: "agent" as const, status: "completed",
          canSteer: false, canFollowUp: false, canStop: false })),
        transcript: (id) => id === "child" ? transcript : nativeTranscript([userMessage("Other target")]),
        loadOlder: () => false, loadNewer: () => false, loadLatest: () => false,
        send: vi.fn(), stop: vi.fn(), close: vi.fn(),
      });
      tui.start();
      const overlay = tui.showOverlay(view, { width: "100%", maxHeight: "100%", anchor: "top-left", margin: 0 });
      try {
        tui.renderNow();
        const raw = render.mock.results.at(-1)!.value as string[];
        const anchor = raw.findIndex((line) => line.includes("\x1b_G"));
        expect(anchor).toBeGreaterThan(30);
        const original = header(raw[anchor]!);
        const id = Number(original.i);
        const rows = Number(original.r);
        expect(rows).toBeGreaterThan(3);
        expect(terminal.placements.size).toBe(0);
        const entry = state.view("child");
        entry.following = false;
        entry.scroll = anchor + 2;
        terminal.output = "";
        tui.renderNow();
        const clipped = view.render(terminal.columns);
        const imageRow = clipped.findIndex((line) => line.includes("\x1b_G"));
        expect(imageRow).toBe(0); // no standalone breadcrumb above the transcript
        expect(header(clipped[imageRow]!)).toMatchObject({ i: String(id), r: String(rows - 2), y: "0", h: "1" });
        expect(clipped.slice(imageRow + 1, imageRow + rows - 2)).toEqual(Array(rows - 3).fill(""));
        expect(terminal.placements).toEqual(new Set([id]));
        if (Renderer === TuiMainScreen) {
          // The blank reservations let Main clear them BEFORE drawing, not
          // erase image cells with later EL sequences (notably on WezTerm).
          expect(terminal.output).toContain(`\x1b[${rows - 3}A`);
        }
        const retained = render.mock.results.at(-1)!.value;
        terminal.output = "";
        for (let i = 0; i < 10; i++) tui.renderNow();
        expect(render.mock.results.at(-1)!.value).toBe(retained);
        expect(terminal.output).not.toContain("\x1b_G"); // no idle deletes, placements or reuploads
        for (const [type, y] of [["press", 1], ["drag", 3], ["release", 3]] as const) {
          view.handleMouse({ type, button: "left", x: 4, y, screenX: 4, screenY: y,
            width: terminal.columns, height: terminal.rows, shift: false, alt: false, ctrl: false });
          tui.renderNow();
        }
        expect(view.render(terminal.columns).slice(imageRow + 1, imageRow + rows - 2)).toEqual(Array(rows - 3).fill(""));
        expect(terminal.placements).toEqual(new Set([id]));
        view.handleMouse({ type: "press", button: "left", x: 4, y: 0, screenX: 4, screenY: 0,
          width: terminal.columns, height: terminal.rows, shift: false, alt: false, ctrl: false });
        // Keyboard and wheel paths move the crop while retaining its image ID.
        terminal.input("\x1b[5~");
        tui.renderNow();
        expect(entry.scroll).toBeLessThan(anchor + 2);
        entry.scroll = anchor + 1;
        tui.renderNow();
        terminal.output = "";
        view.handleMouse({ type: "wheel", button: "none", x: 10, y: 4, screenX: 10, screenY: 4,
          width: terminal.columns, height: terminal.rows, wheelDelta: 2, shift: false, alt: false, ctrl: false });
        tui.renderNow();
        expect(entry.scroll).toBe(anchor + 3);
        expect(terminal.placements).toEqual(new Set([id]));
        if (Renderer === TuiAltScreen) {
          expect(terminal.output).toContain("\x1b_Ga=p,");
          expect(terminal.output).not.toContain("\x1b_Ga=T,"); // native upload cache survives scrolling
        }
        entry.scroll = 0;
        tui.renderNow();
        expect(terminal.placements.size).toBe(0);
        entry.scroll = anchor;
        tui.renderNow();
        expect(terminal.placements).toEqual(new Set([id]));
        terminal.input("\x0e");
        tui.renderNow();
        expect(terminal.placements.size).toBe(0);
        terminal.input("\x1b");
        tui.renderNow();
        expect(terminal.placements).toEqual(new Set([id]));
        terminal.columns = 14;
        terminal.rows = 12;
        tui.renderNow();
        const resizedRaw = render.mock.results.at(-1)!.value as string[];
        entry.scroll = resizedRaw.findIndex((line) => line.includes("\x1b_G")) + 1;
        tui.renderNow();
        const resized = view.render(terminal.columns);
        expect(resized).toHaveLength(12);
        const resizedAnchor = resized.findIndex((line) => line.includes("\x1b_G"));
        expect(resizedAnchor).toBeGreaterThanOrEqual(0);
        expect(resizedAnchor + Number(header(resized[resizedAnchor]!).r)).toBeLessThan(terminal.rows);
        view.selectTarget("other");
        tui.renderNow();
        expect(terminal.placements.size).toBe(0);
        view.selectTarget("child");
        tui.renderNow();
        expect(terminal.placements.size).toBe(1);
        overlay.hide();
        view.dispose();
        release();
        tui.renderNow();
        expect(terminal.placements).toEqual(new Set([909]));
      } finally { overlay.hide(); view.dispose(); state.clear(); release(); tui.stop(); }
    });
  }
});
