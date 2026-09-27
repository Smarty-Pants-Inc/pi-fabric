import type { ExtensionContext, Theme } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { FabricState } from "../src/fabric-state.js";
import { FabricShellJobStore } from "../src/core/shell-jobs.js";
const loads = vi.hoisted(() => vi.fn());
vi.mock("../src/ui/shell-tasks.js", async original => { loads(); return original<typeof import("../src/ui/shell-tasks.js")>(); });
afterEach(() => vi.useRealTimers());

describe("shell inspector first use", () => {
  it("stays unloaded and timer-free at import/idle, then opens lazily and releases on stop", async () => {
    vi.resetModules(); loads.mockClear(); vi.useFakeTimers();
    const { FabricUiController } = await import("../src/ui/controller.js");
    expect(loads).not.toHaveBeenCalled();
    const jobs = new FabricShellJobStore();
    const state = { shellJobs: jobs, config: { ui: { enabled: true, widget: "auto", refreshMs: 1000, maxRows: 5 }, mesh: { enabled: false } },
      activity: { subscribe: () => () => {}, runs: () => [] }, agents: { list: () => [], subscribeUi: () => () => {} }, actors: { list: () => [], subscribe: () => () => {} },
      globalActors: { list: () => [] }, mainAgentInfo: () => ({ status: "idle" }),
    } as unknown as FabricState;
    const theme = { fg: (_: string, s: string) => s } as unknown as Theme;
    let view: any;
    const custom = vi.fn((factory: any) => new Promise<void>(resolve => { view = factory({ requestRender: vi.fn(), terminal: { rows: 24 } }, theme, {}, resolve); }));
    const context = { mode: "tui", ui: { setWidget: vi.fn(), notify: vi.fn(), custom } } as unknown as ExtensionContext;
    const controller = new FabricUiController(state);
    try {
      controller.start(context);
      await vi.advanceTimersByTimeAsync(5000);
      expect(loads).not.toHaveBeenCalled(); expect(vi.getTimerCount()).toBe(0);
      const pending = controller.openTasks(context);
      expect(controller.ownsInput).toBe(true);
      await vi.waitFor(() => expect(custom).toHaveBeenCalledOnce());
      expect(loads).toHaveBeenCalledOnce();
      expect(view.render(80).join("\n")).toContain("No background shell tasks");
      controller.stop(); await pending;
      expect(controller.ownsInput).toBe(false); expect(vi.getTimerCount()).toBe(0);
    } finally { controller.stop(); await jobs.close(); }
  });
});
