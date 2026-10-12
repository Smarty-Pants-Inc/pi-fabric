import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { getCapabilities, Image, setCapabilities, Text, TuiAltScreen, TuiMainScreen, type TUI } from "@earendil-works/pi-tui";
import { createInteractiveTuiReference } from "../node_modules/@earendil-works/pi-coding-agent/dist/modes/interactive/tui-renderer.js";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { imageSafeCustom, retainImageSafeOverlays } from "../src/ui/image-overlays.js";
import { KittyTerminal, PNG } from "./fixtures/kitty-terminal.js";

let capabilities: ReturnType<typeof getCapabilities>;
beforeEach(() => { capabilities = getCapabilities(); setCapabilities({ images: "kitty", trueColor: true, hyperlinks: true }); });
afterEach(() => { setCapabilities(capabilities); vi.restoreAllMocks(); });
const image = (id: number) => new Image(PNG, "image/png", { fallbackColor: (text) => text }, { imageId: id, maxWidthCells: 12 });
const compositor = (tui: TUI) => (tui as unknown as { compositeOverlays(lines: string[], width: number, height: number): string[] }).compositeOverlays;

describe("image-safe native overlays", () => {
  it.each([TuiMainScreen, TuiAltScreen])("removes background placements while open and restores on hide/close (%s)", (Renderer) => {
    const terminal = new KittyTerminal();
    const tui = new Renderer(terminal);
    tui.addChild(image(101));
    tui.addChild(new Text("Main remains"));
    const original = compositor(tui);
    const release = retainImageSafeOverlays(createInteractiveTuiReference(() => tui));
    tui.start();
    const handles: ReturnType<TUI["showOverlay"]>[] = [];
    try {
      tui.renderNow();
      expect(terminal.placements).toEqual(new Set([101]));
      const panel = tui.showOverlay(new Text("DASHBOARD"), { row: 0, col: 1, width: 30 });
      handles.push(panel);
      terminal.output = "";
      tui.renderNow();
      expect(terminal.output).toContain("DASHBOARD");
      expect(terminal.placements.size).toBe(0);
      panel.setHidden(true);
      tui.renderNow();
      expect(terminal.placements).toEqual(new Set([101]));
      panel.setHidden(false);
      tui.renderNow();
      expect(terminal.placements.size).toBe(0);
      panel.hide();
      release();
      tui.renderNow();
      expect(terminal.placements).toEqual(new Set([101]));
      expect(compositor(tui)).toBe(original);
    } finally { for (const handle of handles) handle.hide(); release(); tui.stop(); }
  });

  it.each([TuiMainScreen, TuiAltScreen])("preserves full-width reservations and suppresses lower overlay images (%s)", (Renderer) => {
    const terminal = new KittyTerminal();
    const tui = new Renderer(terminal);
    const child = image(202);
    const render = child.render;
    const release = retainImageSafeOverlays(tui);
    tui.start();
    const chat = tui.showOverlay(child, { width: "100%", anchor: "top-left" });
    let panel: ReturnType<TUI["showOverlay"]> | undefined;
    try {
      const frame = compositor(tui).call(tui, [], terminal.columns, terminal.rows);
      expect(frame[0]).toContain("i=202");
      expect(frame.slice(1, 6)).toEqual(Array(5).fill(""));
      tui.renderNow();
      expect(terminal.placements).toEqual(new Set([202]));
      // The image anchor is above the panel, but its placement crosses it.
      panel = tui.showOverlay(new Text("TOP PANEL"), { row: 3, col: 2, width: 30 });
      terminal.output = "";
      tui.renderNow();
      expect(terminal.output).toContain("TOP PANEL");
      expect(terminal.placements.size).toBe(0);
      expect(child.render).toBe(render);
      panel.setHidden(true);
      tui.renderNow();
      expect(terminal.placements).toEqual(new Set([202]));
      panel.setHidden(false);
      panel.focus();
      tui.renderNow();
      expect(terminal.placements.size).toBe(0);
      panel.hide();
      tui.renderNow();
      expect(terminal.placements).toEqual(new Set([202]));
    } finally { panel?.hide(); chat.hide(); release(); tui.stop(); }
  });

  it("reference-counts the real host across live proxy mode changes and releases idempotently", () => {
    const terminal = new KittyTerminal();
    let tui: TUI = new TuiMainScreen(terminal);
    const proxy = createInteractiveTuiReference(() => tui);
    const original = compositor(tui);
    const first = retainImageSafeOverlays(proxy);
    const patched = compositor(tui);
    tui = new TuiAltScreen(terminal);
    const second = retainImageSafeOverlays(proxy);
    expect(compositor(tui)).toBe(patched);
    first(); first();
    expect(compositor(tui)).toBe(patched);
    second();
    expect(compositor(tui)).toBe(original);
  });

  it("restores component methods after render errors and leases after factory errors", async () => {
    const tui = new TuiMainScreen(new KittyTerminal());
    const original = compositor(tui);
    const release = retainImageSafeOverlays(tui);
    const lower = Object.freeze({ render: (width: number) => image(303).render(width), invalidate() {} });
    const descriptor = Object.getOwnPropertyDescriptor(lower, "render");
    const a = tui.showOverlay(lower);
    const b = tui.showOverlay({ render: () => { throw new Error("render failed"); }, invalidate() {} });
    try {
      expect(() => compositor(tui).call(tui, [], 80, 24)).toThrow("render failed");
      expect(Object.getOwnPropertyDescriptor(lower, "render")).toEqual(descriptor);
    } finally { b.hide(); a.hide(); release(); }
    const ui = { custom: async (factory: (...args: unknown[]) => unknown) => factory(tui, {}, {}, vi.fn()) } as unknown as ExtensionContext["ui"];
    await expect(imageSafeCustom(ui, async () => { throw new Error("factory failed"); })).rejects.toThrow("factory failed");
    expect(compositor(tui)).toBe(original);
    tui.stop();
  });
});
