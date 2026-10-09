import type { ExtensionContext, Theme } from "@earendil-works/pi-coding-agent";
import type { TUI } from "@earendil-works/pi-tui";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { FabricState } from "../src/fabric-state.js";
import { FabricUiController } from "../src/ui/controller.js";

const watches = vi.hoisted(() => ({ changed: () => {}, close: vi.fn() }));
vi.mock("../src/ui/watch-files.js", () => ({
  watchUiFiles: vi.fn((_root, _names, changed) => { watches.changed = changed; return watches.close; }),
}));
const controllers: FabricUiController[] = [];
const completions: (() => void)[] = [];
const openings: Promise<void>[] = [];
afterEach(async () => {
  for (const controller of controllers.splice(0)) controller.stop();
  for (const complete of completions.splice(0)) complete();
  await Promise.all(openings.splice(0));
  vi.useRealTimers();
  vi.clearAllMocks();
});
const harness = () => {
  let activity = () => {};
  let actor = () => {};
  let agent = () => {};
  let shell = () => {};
  let input = (_data: string): undefined => undefined;
  const unsubInput = vi.fn();
  const mainAgentInfo = vi.fn(() => ({ id: "main", name: "Main", kind: "main", status: "idle",
    runner: "pi", transport: "host", cwd: "/test", startedAt: 1, updatedAt: 1, pendingMessages: false, local: true }));
  const refresh = vi.fn(() => ({ events: [], nextOffset: 0 }));
  const state = {
    initialized: true, widgetDismissedAt: 0,
    config: { ui: { enabled: true, refreshMs: 500, eventHistory: 80, widget: "hidden" }, mesh: { enabled: true } },
    mesh: { root: "/not-a-mesh", read: () => [], latestOffset: () => 0, tail: refresh,
      list: () => [], stateStamp: () => "stationary", cachedStateStamp: () => "stationary", readCacheRemainingMs: 1 },
    activity: { revision: () => 0, runs: () => [], subscribe: (f: () => void) => { activity = f; return () => {}; } },
    actors: { list: () => [], subscribe: (f: () => void) => { actor = f; return () => {}; } },
    agents: { list: () => [], subscribeUi: (f: () => void) => { agent = f; return () => {}; } },
    shellJobs: { list: () => [], subscribe: (f: () => void) => { shell = f; return () => {}; } },
    globalActors: { list: () => [], stamp: () => "stationary" },
    mainAgentInfo, peerInfos: () => [{ id: "peer", name: "peer", kind: "main", status: "idle" }],
  } as unknown as FabricState;
  const requestRender = vi.fn();
  const tui = { requestRender } as unknown as TUI;
  const theme = { fg: (_c: string, s: string) => s, bg: (_c: string, s: string) => s, bold: (s: string) => s } as Theme;
  const custom = vi.fn((factory: (t: TUI, theme: Theme, keys: unknown, done: () => void) => unknown) =>
    new Promise<void>(resolve => { completions.push(resolve); factory(tui, theme, {}, resolve); }));
  const context = { mode: "tui", ui: { custom, setWidget: vi.fn(), notify: vi.fn(),
    onTerminalInput: (f: typeof input) => { input = f; return unsubInput; } } } as unknown as ExtensionContext;
  const controller = new FabricUiController(state);
  controllers.push(controller);
  return { controller, state, context, mainAgentInfo, refresh, requestRender, unsubInput,
    activity: () => activity(), actor: () => actor(), agent: () => agent(), shell: () => shell(), input: () => input("x"),
    async open() {
      controller.start(context);
      const pending = controller.openDashboard(context);
      await vi.waitFor(() => expect(custom).toHaveBeenCalled());
      openings.push(pending);
      mainAgentInfo.mockClear(); refresh.mockClear(); requestRender.mockClear();
    },
  };
};

describe("event-driven dashboard refresh", () => {
  it("does not refresh an idle open Main dashboard with peers for 10 seconds", async () => {
    vi.useFakeTimers();
    const h = harness(); await h.open();
    await vi.advanceTimersByTimeAsync(10_000);
    expect(h.refresh).not.toHaveBeenCalled();
    expect(h.requestRender).not.toHaveBeenCalled();
  });

  it("coalesces a mesh change burst into one refresh within 100 ms", async () => {
    vi.useFakeTimers();
    const h = harness(); await h.open();
    for (let i = 0; i < 20; i++) watches.changed();
    await vi.advanceTimersByTimeAsync(100);
    expect(h.refresh).toHaveBeenCalledTimes(1);
    expect(h.requestRender).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(100);
    expect(h.refresh).toHaveBeenCalledTimes(1);
  });

  it.each(["activity", "actor", "agent", "shell", "input"] as const)("wakes on %s, without consuming input", async event => {
    vi.useFakeTimers();
    const h = harness(); await h.open();
    expect(h[event]()).toBeUndefined();
    await vi.advanceTimersByTimeAsync(100);
    expect(h.refresh).toHaveBeenCalledTimes(1);
  });

  it("does zero hidden refreshes, cancels queued work, and refreshes exactly once on show", async () => {
    vi.useFakeTimers();
    const h = harness(); await h.open();
    watches.changed(); // Cancel a refresh that was already queued before hiding.
    h.controller.setPaneVisible(false);
    for (let i = 0; i < 10; i++) {
      watches.changed(); h.activity(); h.actor(); h.agent(); h.shell(); h.input();
      h.controller.setHostStreaming(true);
    }
    await vi.advanceTimersByTimeAsync(10_000);
    expect(h.refresh).not.toHaveBeenCalled();
    expect(h.requestRender).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
    h.controller.setPaneVisible(true);
    h.controller.setPaneVisible(true);
    expect(h.refresh).toHaveBeenCalledTimes(1);
    expect(h.requestRender).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(100);
    expect(h.refresh).toHaveBeenCalledTimes(1);
  });

  it.each([100, 5000, 12000])("preserves the >=5s fallback floor at refreshMs=%s even with a 1ms cache deadline", async ms => {
    vi.useFakeTimers();
    const h = harness(); h.state.config.ui.refreshMs = ms;
    h.controller.start(h.context); h.controller.setHostStreaming(true); h.refresh.mockClear();
    const floor = Math.max(5000, ms);
    await vi.advanceTimersByTimeAsync(floor - 1);
    expect(h.refresh).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1);
    expect(h.refresh).toHaveBeenCalledTimes(1);
  });

  it("unsubscribes watches/input on stop and ignores stale mesh callbacks across restart", async () => {
    vi.useFakeTimers();
    const h = harness(); h.controller.start(h.context);
    const oldMeshEvent = watches.changed;
    h.controller.stop(); h.refresh.mockClear();
    oldMeshEvent(); h.input();
    await vi.advanceTimersByTimeAsync(100);
    expect(h.refresh).not.toHaveBeenCalled();
    expect(watches.close).toHaveBeenCalledTimes(1);
    expect(h.unsubInput).toHaveBeenCalledTimes(1);
    h.controller.start(h.context); h.refresh.mockClear();
    oldMeshEvent();
    await vi.advanceTimersByTimeAsync(100);
    expect(h.refresh).not.toHaveBeenCalled();
    watches.changed(); await vi.advanceTimersByTimeAsync(100);
    expect(h.refresh).toHaveBeenCalledTimes(1);
  });
});
