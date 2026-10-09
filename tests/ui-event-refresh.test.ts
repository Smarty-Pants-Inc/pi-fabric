import type { ExtensionContext, Theme } from "@earendil-works/pi-coding-agent";
import type { TUI } from "@earendil-works/pi-tui";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { FabricState } from "../src/fabric-state.js";
import type { FabricShellJobInfo } from "../src/core/shell-jobs.js";
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
  const snapshots = vi.spyOn(state.shellJobs!, "list");
  const requestRender = vi.fn();
  const tui = { requestRender } as unknown as TUI;
  const theme = { fg: (_c: string, s: string) => s, bg: (_c: string, s: string) => s, bold: (s: string) => s } as Theme;
  const custom = vi.fn((factory: (t: TUI, theme: Theme, keys: unknown, done: () => void) => unknown) =>
    new Promise<void>(resolve => { completions.push(resolve); factory(tui, theme, {}, resolve); }));
  const context = { mode: "tui", ui: { custom, setWidget: vi.fn(), notify: vi.fn(),
    onTerminalInput: (f: typeof input) => { input = f; return unsubInput; } } } as unknown as ExtensionContext;
  const controller = new FabricUiController(state);
  controllers.push(controller);
  return { controller, state, context, mainAgentInfo, snapshots, refresh, requestRender, unsubInput,
    activity: () => activity(), actor: () => actor(), agent: () => agent(), shell: () => shell(), input: () => input("x"),
    async open() {
      controller.start(context);
      const pending = controller.openDashboard(context);
      await vi.waitFor(() => expect(custom).toHaveBeenCalled());
      openings.push(pending);
      mainAgentInfo.mockClear(); snapshots.mockClear(); refresh.mockClear(); requestRender.mockClear();
    },
  };
};

describe("event-driven dashboard refresh", () => {
  it.each([
    { open: false, peers: false }, { open: false, peers: true },
    { open: true, peers: false }, { open: true, peers: true },
  ])("has at most five fallback wakes and zero unchanged idle refreshes in five minutes: $open/$peers", async ({ open, peers }) => {
    vi.useFakeTimers();
    const h = harness();
    if (!peers) h.state.peerInfos = () => [];
    if (open) await h.open();
    else { h.controller.start(h.context); h.refresh.mockClear(); h.mainAgentInfo.mockClear(); h.snapshots.mockClear(); }
    const wakes = vi.spyOn(h.state.mesh, "latestOffset");
    await vi.advanceTimersByTimeAsync(300_000);
    expect(wakes).toHaveBeenCalledTimes(5);
    expect(h.refresh).not.toHaveBeenCalled();
    expect(h.mainAgentInfo).not.toHaveBeenCalled();
    expect(h.snapshots).not.toHaveBeenCalled();
    expect(h.requestRender).not.toHaveBeenCalled();
    wakes.mockRestore();
  });

  it.each([true, false])("expires a completed shell at 30 seconds with one extra refresh (mesh=%s)", async mesh => {
    vi.useFakeTimers({ now: 1_000_000 });
    const h = harness();
    h.state.config.mesh.enabled = mesh;
    h.state.peerInfos = () => [];
    const job: FabricShellJobInfo = {
      id: "shell", tool: "bash", command: "build", startedAt: Date.now(), spilledAt: Date.now(),
      status: "spilled", eventCount: 0, unread: false, stopping: false,
    };
    h.snapshots.mockImplementation(() => [job]);
    h.controller.start(h.context);
    await vi.advanceTimersByTimeAsync(1_000);
    Object.assign(job, { status: "exited", finishedAt: Date.now() });
    h.snapshots.mockClear();
    h.shell();
    await vi.advanceTimersByTimeAsync(100);
    expect(h.snapshots).toHaveBeenCalledTimes(1); // completion event, not an expiry wake
    expect(h.controller.snapshot().shells).toHaveLength(1);
    expect(vi.getTimerCount()).toBe(mesh ? 2 : 1); // only the expiry plus the mesh recovery timer
    h.snapshots.mockClear();
    await vi.advanceTimersByTimeAsync(job.finishedAt! + 30_000 - Date.now() - 1);
    expect(h.snapshots).not.toHaveBeenCalled();
    expect(h.controller.snapshot().shells).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(h.controller.snapshot().shells).toEqual([]);
    expect(h.snapshots).toHaveBeenCalledTimes(1);
    expect(vi.getTimerCount()).toBe(mesh ? 1 : 0);
    await vi.advanceTimersByTimeAsync(300_000);
    expect(h.snapshots).toHaveBeenCalledTimes(1); // no expiry polling after the last deadline
  });

  it.each([true, false])("arms no expiry timer without expiring items (mesh=%s)", async mesh => {
    vi.useFakeTimers();
    const h = harness();
    h.state.config.mesh.enabled = mesh;
    h.state.peerInfos = () => [];
    const timers = vi.spyOn(globalThis, "setTimeout");
    try {
      h.controller.start(h.context);
      expect(vi.getTimerCount()).toBe(mesh ? 1 : 0);
      expect(timers.mock.calls.map(call => call[1])).toEqual(mesh ? [60_000] : []);
    } finally { timers.mockRestore(); }
  });

  it("re-arms one expiry timer only for the next completed shell deadline", async () => {
    vi.useFakeTimers({ now: 1_000_000 });
    const h = harness();
    h.state.config.mesh.enabled = false;
    h.state.peerInfos = () => [];
    const jobs: FabricShellJobInfo[] = [10_000, 0].map((age, index) => ({
      id: `shell-${index}`, tool: "bash", command: "build", startedAt: Date.now() - 20_000,
      spilledAt: Date.now() - 20_000, finishedAt: Date.now() - age, status: "exited",
      eventCount: 0, unread: false, stopping: false,
    }));
    h.snapshots.mockReturnValue(jobs);
    h.controller.start(h.context);
    h.snapshots.mockClear();
    expect(vi.getTimerCount()).toBe(1);
    await vi.advanceTimersByTimeAsync(20_000);
    expect(h.controller.snapshot().shells?.map(job => job.id)).toEqual(["shell-1"]);
    expect(h.snapshots).toHaveBeenCalledTimes(1);
    expect(vi.getTimerCount()).toBe(1);
    await vi.advanceTimersByTimeAsync(10_000);
    expect(h.controller.snapshot().shells).toEqual([]);
    expect(h.snapshots).toHaveBeenCalledTimes(2);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("honors explicit snapshot item expiries without polling", async () => {
    vi.useFakeTimers({ now: 1_000_000 });
    const h = harness();
    h.state.config.mesh.enabled = false;
    const peers = [5_000, 9_000].map((ttl, index) => ({
      id: `peer-${index}`, name: "peer", kind: "peer" as const, status: "idle" as const,
      runner: "pi" as const, transport: "host" as const, cwd: "/test", sessionId: "peer",
      startedAt: Date.now(), updatedAt: Date.now(), pendingMessages: false, local: false as const,
      expiresAt: Date.now() + ttl,
    }));
    h.state.peerInfos = () => peers.filter(peer => peer.expiresAt > Date.now());
    h.controller.start(h.context);
    h.snapshots.mockClear();
    expect(vi.getTimerCount()).toBe(2); // one deadline, one slow recovery
    await vi.advanceTimersByTimeAsync(5_000);
    expect(h.controller.snapshot().peers.map(peer => peer.id)).toEqual(["peer-1"]);
    expect(h.snapshots).toHaveBeenCalledTimes(1);
    expect(vi.getTimerCount()).toBe(2);
    await vi.advanceTimersByTimeAsync(4_000);
    expect(h.controller.snapshot().peers).toEqual([]);
    expect(h.snapshots).toHaveBeenCalledTimes(2);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("cancels the one-shot expiry timer on stop", async () => {
    vi.useFakeTimers({ now: 1_000_000 });
    const h = harness();
    h.state.config.mesh.enabled = false;
    h.state.peerInfos = () => [];
    h.snapshots.mockReturnValue([{
      id: "shell", tool: "bash", command: "build", startedAt: Date.now(), spilledAt: Date.now(),
      finishedAt: Date.now(), status: "exited", eventCount: 0, unread: false, stopping: false,
    }]);
    h.controller.start(h.context);
    expect(vi.getTimerCount()).toBe(1);
    h.controller.stop();
    h.snapshots.mockClear();
    expect(vi.getTimerCount()).toBe(0);
    await vi.advanceTimersByTimeAsync(30_000);
    expect(h.snapshots).not.toHaveBeenCalled();
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

  it.each([100, 5000, 12000, 70000])("preserves the >=60s fallback floor at refreshMs=%s even with a 1ms cache deadline", async ms => {
    vi.useFakeTimers();
    const h = harness(); h.state.config.ui.refreshMs = ms;
    h.controller.start(h.context); h.controller.setHostStreaming(true); h.refresh.mockClear();
    const floor = Math.max(60000, ms);
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
