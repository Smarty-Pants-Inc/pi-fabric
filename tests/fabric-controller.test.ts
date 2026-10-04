import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionContext, Theme } from "@earendil-works/pi-coding-agent";
import type { TUI } from "@earendil-works/pi-tui";
import { describe, expect, it, vi } from "vitest";
import type { FabricActivityRun } from "../src/activity/types.js";
import { FabricActivityStore } from "../src/activity/store.js";
import type { FabricState } from "../src/fabric-state.js";
import { FabricUiController } from "../src/ui/controller.js";
import { MeshStore } from "../src/mesh/store.js";
import { readParticipantFiles } from "../src/topology/participant-files.js";
import { hostLiveness, readHostLeases, writeHostLease } from "../src/topology/host-leases.js";
import type { FabricDashboard } from "../src/ui/dashboard.js";
import { FabricWidget } from "../src/ui/widget.js";
import "../src/ui/dashboard.js";
import "../src/ui/model-picker.js";

const theme = {
  fg: (_c: string, t: string) => t,
  bg: (_c: string, t: string) => t,
  bold: (t: string) => t,
} as unknown as Theme;

const stubActor = {
  id: "actor-1",
  name: "advisor",
  status: "idle",
  events: ["turn_end"],
  topics: [],
  delivery: "mailbox",
  responseMode: "text",
  triggerTurn: false,
  coalesce: true,
  queued: 0,
  messages: 0,
  createdAt: 0,
  updatedAt: 0,
};

const stubState = () =>
  ({
    initialized: true,
    config: {
      ui: { enabled: true, refreshMs: 60_000, eventHistory: 80, widget: "hidden" },
      mesh: { enabled: false },
    },
    activity: { subscribe: vi.fn(() => () => {}), runs: vi.fn(() => []), reset: vi.fn() },
    mainAgentInfo: vi.fn(() => ({
      id: "session:test",
      name: "Main",
      kind: "main",
      status: "idle",
      runner: "pi",
      transport: "host",
      cwd: "/tmp/project",
      sessionId: "test",
      startedAt: 1,
      updatedAt: 1,
      pendingMessages: false,
      local: true,
    })),
    queueUserMessage: vi.fn().mockResolvedValue({
      queued: true,
      messageId: "message-1",
      routed: "main",
    }),
    agents: { list: vi.fn(() => []), subscribeUi: vi.fn(() => () => {}) },
    actors: {
      list: vi.fn(() => [stubActor]),
      messages: vi.fn(() => []),
      instructions: vi.fn(() => "Advise only when useful."),
      setModel: vi.fn().mockResolvedValue(undefined),
      setThinking: vi.fn().mockResolvedValue(undefined),
      setEvents: vi.fn().mockResolvedValue(undefined),
      setInstructions: vi.fn().mockResolvedValue(undefined),
      clearMessages: vi.fn().mockResolvedValue(undefined),
      subscribe: vi.fn(() => () => {}),
    },
    globalActors: {
      list: vi.fn(() => []),
      resolve: vi.fn(() => undefined),
      create: vi.fn(() => ({ id: "g1", name: "x", createdAt: 0, updatedAt: 0 })),
      update: vi.fn(() => ({ id: "g1", name: "x", createdAt: 0, updatedAt: 0 })),
      remove: vi.fn(() => ({ removed: true })),
      toRequest: vi.fn(() => ({ name: "x", instructions: "y" })),
    },
    mesh: { read: vi.fn(() => []), latestOffset: vi.fn(() => 0), list: vi.fn(() => []) },
    widgetDismissedAt: 0,
  }) as unknown as FabricState;

describe("FabricUiController dashboard wiring", () => {
  it("uses incremental views, skips duplicate progress refreshes, and releases readers on stop", async () => {
    vi.useFakeTimers();
    const state = stubState();
    const activity = new FabricActivityStore();
    Object.assign(state, { activity });
    activity.start("history");
    activity.finish("history", true);
    activity.start("live");
    activity.beginCall("live", { callId: "call", ref: "pi.read", args: { path: "file.ts" } });
    const createView = vi.spyOn(activity, "createRunView");
    const fullReads = vi.spyOn(activity, "runs");
    const legacySummaries = vi.spyOn(activity, "runSummaries");
    const tui = { requestRender: vi.fn() } as unknown as TUI;
    const controller = new FabricUiController(state);
    const context = {
      mode: "tui", modelRegistry: { getAvailable: () => [] }, ui: {
        custom: vi.fn(async (factory: (t: TUI, theme: Theme, keys: unknown, done: () => void) => FabricDashboard) => {
          factory(tui, theme, {}, () => {});
          expect(controller.snapshot().runs[0]!.calls[0]!.args).toEqual({ path: "file.ts" });
        }),
        notify: vi.fn(), setWidget: vi.fn(),
      },
    } as unknown as ExtensionContext;
    try {
      controller.start(context);
      const history = controller.snapshot().runs[1];
      expect(controller.snapshot().runs[0]!.calls[0]).not.toHaveProperty("args");
      activity.updateCall("live", "call", { type: "progress", message: "working" });
      await vi.advanceTimersByTimeAsync(110);
      // The public snapshot() API deliberately returns isolated copies.
      expect(controller.snapshot().runs[1]).toEqual(history);
      expect(controller.snapshot().runs[0]!.calls[0]!.progress).toBe("working");
      vi.mocked(state.mainAgentInfo).mockClear();
      for (let i = 0; i < 100; i++) activity.updateCall("live", "call", { type: "progress", message: "working" });
      await vi.advanceTimersByTimeAsync(110);
      expect(state.mainAgentInfo).not.toHaveBeenCalled();
      await controller.openDashboard(context);
      expect(controller.snapshot().runs[0]!.calls[0]).not.toHaveProperty("args");
      expect(fullReads).not.toHaveBeenCalled();
      expect(legacySummaries).not.toHaveBeenCalled();
      expect(createView).toHaveBeenCalledTimes(1);
      controller.stop();
      controller.start(context);
      expect(createView).toHaveBeenCalledTimes(2);
      expect(context.ui.notify).not.toHaveBeenCalled();
    } finally {
      controller.stop();
      vi.restoreAllMocks();
      vi.useRealTimers();
    }
  });
  it("passes every actor callback to the dashboard so all pickers are available", async () => {
    const state = stubState();
    const controller = new FabricUiController(state);
    const tui = { requestRender: vi.fn() } as unknown as TUI;
    let dashboard: FabricDashboard | undefined;
    const context = {
      mode: "tui",
      modelRegistry: { getAvailable: () => [] },
      ui: {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        custom: vi.fn(async (factory: any) => {
          dashboard = factory(tui, theme, {}, () => {}) as FabricDashboard;
        }),
        notify: vi.fn(),
        setWidget: vi.fn(),
      },
    } as unknown as ExtensionContext;

    try {
      await controller.openDashboard(context);
      expect(dashboard).toBeDefined();
      // Enter the entities pane and open the actor detail.
      dashboard!.handleInput("l");
      dashboard!.handleInput("\r");
      const detail = dashboard!.render(120).join("\n");
      expect(detail).toContain("advisor");
      // Each hint is gated on its callback being wired by the controller;
      // this guards against regressions like the thinking picker being omitted.
      expect(detail).toContain("m session model");
      expect(detail).toContain("M pin model");
      expect(detail).toContain("e session thinking");
      expect(detail).toContain("E pin thinking");
      expect(detail).toContain("v events");
      expect(detail).toContain("c clear");
    } finally {
      dashboard?.dispose();
      controller.stop();
    }
  });

  it("routes Main dashboard messages through FabricState", async () => {
    const state = stubState();
    const controller = new FabricUiController(state);
    const tui = { requestRender: vi.fn() } as unknown as TUI;
    let dashboard: FabricDashboard | undefined;
    const context = {
      mode: "tui",
      modelRegistry: { getAvailable: () => [] },
      ui: {
        custom: vi.fn(async (factory: (
          tui: TUI,
          theme: Theme,
          keybindings: unknown,
          done: () => void,
        ) => FabricDashboard) => {
          dashboard = factory(tui, theme, {}, () => {});
        }),
        notify: vi.fn(),
        setWidget: vi.fn(),
      },
    } as unknown as ExtensionContext;

    try {
      await controller.openDashboard(context);
      dashboard!.handleInput("l");
      dashboard!.handleInput("g");
      dashboard!.handleInput("s");
      dashboard!.handleInput("focus on the failing test");
      dashboard!.handleInput("\r");
      expect(state.queueUserMessage).toHaveBeenCalledWith(
        "session:test",
        "focus on the failing test",
        "steer",
      );
    } finally {
      dashboard?.dispose();
      controller.stop();
    }
  });

  it("does not refresh settled sessions on an idle timer", async () => {
    vi.useFakeTimers();
    const state = stubState();
    state.config.ui.refreshMs = 100;
    vi.mocked(state.actors.list).mockReturnValue([]);
    const settledRun: FabricActivityRun = {
      id: "settled-run",
      name: "Large settled run",
      status: "completed",
      phases: [],
      calls: Array.from({ length: 1_000 }, (_, index) => ({
        id: `call-${index}`,
        ref: "pi.read",
        label: "pi.read",
        kind: "tool",
        status: "completed",
        startedAt: index,
        updatedAt: index,
        finishedAt: index,
      })),
      items: [],
      events: [],
      startedAt: 0,
      updatedAt: 1_000,
      finishedAt: 1_000,
    };
    vi.mocked(state.activity.runs).mockReturnValue([settledRun]);
    const context = {
      mode: "tui",
      ui: { setWidget: vi.fn(), notify: vi.fn() },
    } as unknown as ExtensionContext;
    const controller = new FabricUiController(state);
    try {
      controller.start(context);
      expect(state.activity.runs).toHaveBeenCalledTimes(1);
      await vi.advanceTimersByTimeAsync(60_000);
      expect(state.activity.runs).toHaveBeenCalledTimes(1);
    } finally {
      controller.stop();
      vi.useRealTimers();
    }
  });

  // smarty-dev#251: on a shared mesh some peer is always present; polling remote records at
  // refreshMs kept every idle Pi rebuilding its snapshot and re-rendering twice a second.
  it("polls remote-only activity at the heartbeat interval and local activity at refreshMs", async () => {
    vi.useFakeTimers();
    const state = stubState();
    state.config.ui.refreshMs = 500;
    vi.mocked(state.actors.list).mockReturnValue([]);
    const remoteAgent = {
      format: 1, id: "agent:remote", kind: "agent", rootId: "session:other", ownerHostId: "session:other",
      ownerIdentityId: "session:other", name: "remote", status: "running", runner: "pi", transport: "host",
      capabilities: [], startedAt: 1, updatedAt: 1, local: false, stale: false,
    };
    Object.assign(state, {
      peerInfos: vi.fn(() => [{ id: "session:other", name: "other", kind: "main", status: "idle" }]),
      participantInfos: vi.fn(() => [remoteAgent]),
    });
    const context = {
      mode: "tui",
      ui: { setWidget: vi.fn(), notify: vi.fn() },
    } as unknown as ExtensionContext;
    const controller = new FabricUiController(state);
    try {
      controller.start(context);
      expect(controller.snapshot().agents.map((agent) => agent.id)).toContain("agent:remote");
      expect(state.activity.runs).toHaveBeenCalledTimes(1);
      await vi.advanceTimersByTimeAsync(4_999);
      expect(state.activity.runs).toHaveBeenCalledTimes(1);          // no 500 ms poll for peers
      await vi.advanceTimersByTimeAsync(1);
      expect(state.activity.runs).toHaveBeenCalledTimes(2);          // one per heartbeat
      vi.mocked(state.activity.runs).mockReturnValue([{
        id: "local-run", name: "Local", status: "running", phases: [], calls: [], items: [], events: [],
        startedAt: 0, updatedAt: 0,
      } as FabricActivityRun]);
      await vi.advanceTimersByTimeAsync(5_000);                      // picks up the local run
      expect(state.activity.runs).toHaveBeenCalledTimes(3);
      await vi.advanceTimersByTimeAsync(500);
      expect(state.activity.runs).toHaveBeenCalledTimes(4);          // local activity: refreshMs
    } finally {
      controller.stop();
      vi.useRealTimers();
    }
  });

  it("wakes settled UI state when actors or detached agents change", async () => {
    vi.useFakeTimers();
    const state = stubState();
    state.config.ui.refreshMs = 500;
    let onActor = (): void => {};
    let onAgent = (): void => {};
    vi.mocked(state.actors.subscribe).mockImplementation((listener) => {
      onActor = listener;
      return () => {};
    });
    vi.mocked(state.agents.subscribeUi).mockImplementation((listener) => {
      onAgent = listener;
      return () => {};
    });
    const context = {
      mode: "tui",
      ui: { setWidget: vi.fn(), notify: vi.fn() },
    } as unknown as ExtensionContext;
    const controller = new FabricUiController(state);
    try {
      controller.start(context);
      expect(state.activity.runs).toHaveBeenCalledTimes(1);
      onActor();
      await vi.advanceTimersByTimeAsync(100);
      expect(state.activity.runs).toHaveBeenCalledTimes(2);
      onAgent();
      await vi.advanceTimersByTimeAsync(100);
      expect(state.activity.runs).toHaveBeenCalledTimes(3);
    } finally {
      controller.stop();
      vi.useRealTimers();
    }
  });

  it("coalesces bursty activity updates into a 10 Hz refresh", async () => {
    vi.useFakeTimers();
    const state = stubState();
    state.config.ui.refreshMs = 500;
    let onActivity = (): void => {};
    vi.mocked(state.activity.subscribe).mockImplementation((listener) => {
      onActivity = listener;
      return () => {};
    });
    const context = {
      mode: "tui",
      ui: { setWidget: vi.fn(), notify: vi.fn() },
    } as unknown as ExtensionContext;
    const controller = new FabricUiController(state);
    try {
      controller.start(context);
      expect(state.activity.runs).toHaveBeenCalledTimes(1);
      for (let index = 0; index < 25; index++) onActivity();
      await vi.advanceTimersByTimeAsync(99);
      expect(state.activity.runs).toHaveBeenCalledTimes(1);
      await vi.advanceTimersByTimeAsync(1);
      expect(state.activity.runs).toHaveBeenCalledTimes(2);
    } finally {
      controller.stop();
      vi.useRealTimers();
    }
  });

  it("surfaces dashboard refresh failures while retaining the last snapshot", async () => {
    const state = stubState();
    vi.mocked(state.activity.runs).mockImplementation(() => {
      throw new Error("corrupt activity state");
    });
    const notify = vi.fn();
    const context = {
      mode: "tui",
      modelRegistry: { getAvailable: () => [] },
      ui: {
        custom: vi.fn(async () => undefined),
        notify,
        setWidget: vi.fn(),
      },
    } as unknown as ExtensionContext;
    const controller = new FabricUiController(state);
    try {
      await controller.openDashboard(context);
      expect(notify).toHaveBeenCalledWith(
        "Fabric dashboard refresh failed: corrupt activity state",
        "warning",
      );
    } finally {
      controller.stop();
    }
  });

  it("refreshes streaming activity from payload-free summaries unless the dashboard is open", async () => {
    vi.useFakeTimers();
    const state = stubState();
    state.config.ui.refreshMs = 500;
    const baseRun = {
      id: "run-1",
      status: "running",
      phases: [],
      calls: [],
      items: [],
      events: [],
      startedAt: 0,
      updatedAt: 1,
    } as Omit<FabricActivityRun, "name">;
    const activityStubs = state.activity as unknown as Record<string, unknown>;
    activityStubs.runSummaries = vi.fn(() => [
      { ...baseRun, name: "summary view" } as FabricActivityRun,
    ]);
    let revision = 1;
    activityStubs.revision = vi.fn(() => revision);
    const runSummaries = vi.mocked(
      activityStubs.runSummaries as () => FabricActivityRun[],
    );
    let onActivity = (): void => {};
    vi.mocked(state.activity.subscribe).mockImplementation((listener) => {
      onActivity = listener;
      return () => {};
    });
    const tui = { requestRender: vi.fn() } as unknown as TUI;
    const context = {
      mode: "tui",
      modelRegistry: { getAvailable: () => [] },
      ui: {
        custom: vi.fn(async (factory: (
          t: TUI,
          theme: Theme,
          keybindings: unknown,
          done: () => void,
        ) => FabricDashboard) => {
          factory(tui, theme, {}, () => {});
        }),
        notify: vi.fn(),
        setWidget: vi.fn(),
      },
    } as unknown as ExtensionContext;
    const controller = new FabricUiController(state);
    try {
      controller.start(context);
      expect(runSummaries).toHaveBeenCalledTimes(1);
      expect(state.activity.runs).not.toHaveBeenCalled();
      expect(controller.snapshot().runs[0]?.name).toBe("summary view");

      // Streaming updates stay on the cheap path at the 10 Hz cadence.
      revision = 2;
      onActivity();
      await vi.advanceTimersByTimeAsync(110);
      expect(runSummaries).toHaveBeenCalledTimes(2);
      expect(state.activity.runs).not.toHaveBeenCalled();

      // Opening the dashboard switches to full detail even at the same
      // revision; closing returns to summaries and downgrades promptly.
      await controller.openDashboard(context);
      expect(state.activity.runs).toHaveBeenCalledTimes(1);
      expect(runSummaries).toHaveBeenCalledTimes(3);

      // Idle polling does not churn copies at an unchanged revision.
      await vi.advanceTimersByTimeAsync(10_000);
      expect(state.activity.runs).toHaveBeenCalledTimes(1);
      expect(runSummaries).toHaveBeenCalledTimes(3);
    } finally {
      controller.stop();
      vi.useRealTimers();
    }
  });

  // smarty-dev#1043: each 500 ms poll gathered and deep-compared every input while local work
  // was active (about 11% of a busy Main). A poll now rebuilds only when a cheap stamp moved.
  it("rebuilds a polled snapshot only when a stamp moved: Main at once, remote state at most every 5 s", async () => {
    vi.useFakeTimers();
    const state = stubState();
    state.config.ui.refreshMs = 500;
    vi.mocked(state.actors.list).mockReturnValue([]);
    const activity = new FabricActivityStore();
    let stamp = "state-1";
    const participantInfos = vi.fn(() => []);
    Object.assign(state, {
      activity,
      participantInfos,
      config: { ...state.config, mesh: { enabled: true } },
      mesh: {
        ...state.mesh, tail: vi.fn(() => ({ events: [], nextOffset: 0 })),
        stateStamp: vi.fn(() => stamp), cachedStateStamp: vi.fn(() => stamp),
      },
    });
    const context = { mode: "tui", ui: { setWidget: vi.fn(), notify: vi.fn() } } as unknown as ExtensionContext;
    const controller = new FabricUiController(state);
    const gathered = () => participantInfos.mock.calls.length;
    try {
      controller.start(context);
      activity.start("live", { name: "local work" });            // keeps the poll at refreshMs
      await vi.advanceTimersByTimeAsync(200);                     // its own event-driven rebuild
      const settled = gathered();
      await vi.advanceTimersByTimeAsync(4_000);                   // 8 polls, nothing moved
      expect(gathered()).toBe(settled);
      expect(controller.snapshot().now).toBeGreaterThanOrEqual(Date.now() - 500);   // ages still move
      stamp = "state-2";                                          // remote state changed
      await vi.advanceTimersByTimeAsync(500);
      expect(gathered()).toBe(settled);                           // less than 5 s since the last build
      await vi.advanceTimersByTimeAsync(1_000);
      expect(gathered()).toBe(settled + 1);                       // then once
      const afterRemote = gathered();
      vi.mocked(state.mainAgentInfo).mockReturnValue({
        ...vi.mocked(state.mainAgentInfo)(), status: "running",
      } as ReturnType<FabricState["mainAgentInfo"]>);
      await vi.advanceTimersByTimeAsync(500);
      expect(gathered()).toBe(afterRemote + 1);                   // Main's own state: at once
      const afterMain = gathered();
      await vi.advanceTimersByTimeAsync(14_000);
      expect(gathered()).toBe(afterMain);
      await vi.advanceTimersByTimeAsync(1_500);
      expect(gathered()).toBe(afterMain + 1);                     // a lapsing lease: every 15 s
    } finally {
      controller.stop();
      vi.useRealTimers();
    }
  });

  // review/astra F2 on #84: a rebuild must show the state it records as built, even when the mesh
  // read cache was warmed just before a remote write; and a rebuild that consumed an older cached
  // payload must leave the gate open, not wait for the 15 s ceiling.
  it.each(["a poll's remote rebuild", "an event-driven rebuild"] as const)(
    "shows the remote state after %s with a freshly warmed mesh read cache",
    async (order) => {
      vi.useFakeTimers();
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-dashboard-"));
      const identity = { id: "session:writer", name: "writer", kind: "main" as const, sessionId: "writer" };
      const mesh = new MeshStore(root, 64 * 1024, 100, { readCacheMs: 2_000 });
      const writer = new MeshStore(root, 64 * 1024, 100);
      await writer.put({ key: "status", value: "A0", identity });
      const stateFile = path.join(root, "state.json");
      const reads = vi.spyOn(fs, "readFileSync");
      const readCount = () => reads.mock.calls.filter(([file]) => String(file) === stateFile).length;
      const state = stubState();
      state.config.ui.refreshMs = 500;
      vi.mocked(state.actors.list).mockReturnValue([]);
      const activity = new FabricActivityStore();
      Object.assign(state, { activity, config: { ...state.config, mesh: { enabled: true } }, mesh });
      const context = { mode: "tui", ui: { setWidget: vi.fn(), notify: vi.fn() } } as unknown as ExtensionContext;
      const controller = new FabricUiController(state);
      const shown = () => controller.snapshot().state.find((entry) => entry.key === "status")?.value;
      try {
        controller.start(context);
        activity.start("live", { name: "local work" });          // polls at refreshMs
        await vi.advanceTimersByTimeAsync(4_700);
        expect(shown()).toBe("A0");
        await writer.put({ key: "status", value: "A", identity });
        expect(mesh.get("status")?.value).toBe("A");             // another reader parses A: a warm cache
        await writer.put({ key: "status", value: "B", identity });
        const afterWrite = readCount(); // Exclude the writer's own canonical read under its lock.
        if (order === "an event-driven rebuild") {
          activity.beginCall("live", { callId: "c1", ref: "pi.read", args: {} });   // consumes the cached A
          await vi.advanceTimersByTimeAsync(150);
          expect(shown()).toBe("A");
          expect(readCount()).toBe(afterWrite); // Local events retain ordinary warm-cache semantics.
        }
        await vi.advanceTimersByTimeAsync(order === "an event-driven rebuild" ? 5_500 : 2_000);
        expect(shown()).toBe("B");
        expect(readCount()).toBe(afterWrite + 1); // New generation needs exactly ONE canonical parse.
      } finally {
        controller.stop();
        vi.restoreAllMocks();
        vi.useRealTimers();
        fs.rmSync(root, { recursive: true, force: true });
      }
    },
  );

  it("idle UI never bypasses the window and sees a change within 5 s of a warmed cache", async () => {
    vi.useFakeTimers({ now: 1_000_000 });
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "dashboard-idle-coalesce-"));
    const mesh = new MeshStore(root, 64 * 1024, 100, { readCacheMs: 5_000 });
    const writer = new MeshStore(root, 64 * 1024, 100);
    const identity = { id: "writer", name: "writer", kind: "main" as const };
    await writer.put({ key: "status", value: "before", identity });
    const state = stubState();
    vi.mocked(state.actors.list).mockReturnValue([]);
    Object.assign(state, { config: { ...state.config, mesh: { enabled: true } }, mesh,
      peerInfos: () => [{ id: "peer", name: "peer", status: "idle" }],
    });
    const context = { mode: "tui", ui: { setWidget: vi.fn(), notify: vi.fn() } } as unknown as ExtensionContext;
    const controller = new FabricUiController(state);
    const reads = vi.spyOn(fs, "readFileSync");
    const count = () => reads.mock.calls.filter(([file]) => String(file) === path.join(root, "state.json")).length;
    const shown = () => controller.snapshot().state.find(entry => entry.key === "status")?.value;
    const observe = vi.spyOn(mesh, "cachedStateStamp");
    try {
      state.config.ui.refreshMs = 500;
      controller.start(context);
      await vi.advanceTimersByTimeAsync(2_000);
      await writer.put({ key: "status", value: "warm", identity });
      // Another correctness consumer warms canonical state just before a remote change.
      expect(mesh.get("status", { fresh: true })?.value).toBe("warm");
      await writer.put({ key: "status", value: "after", identity });
      const before = count();
      await vi.advanceTimersByTimeAsync(3_000); // First UI poll consumes the warm, older snapshot.
      expect(shown()).toBe("warm");
      expect(count()).toBe(before); // Generation change must NOT force an idle parse.
      await vi.advanceTimersByTimeAsync(2_000); // Fixed deadline, not another full 5 s poll.
      expect(shown()).toBe("after");
      expect(count()).toBe(before + 1);
      // Idle metadata observation must not independently parse at expiry before snapshot
      // consumers use the shared reader (#4383); active demand has separate coverage below.
      expect(observe).toHaveBeenCalledWith(false, false);
      expect(observe.mock.calls.some(([fresh]) => fresh === true)).toBe(false);
      expect(context.ui.notify).not.toHaveBeenCalled();
    } finally {
      controller.stop(); vi.restoreAllMocks(); vi.useRealTimers();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it.each(["running", "pending"] as const)("revalidates a warm remote view immediately for %s Main demand", async demand => {
    vi.useFakeTimers({ now: 1_000_000 });
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "dashboard-active-coalesce-"));
    const mesh = new MeshStore(root, 64 * 1024, 100, { readCacheMs: 5_000 });
    const writer = new MeshStore(root, 64 * 1024, 100);
    const identity = { id: "writer", name: "writer", kind: "main" as const };
    await writer.put({ key: "status", value: "before", identity });
    const state = stubState();
    state.config.ui.refreshMs = 100;
    vi.mocked(state.actors.list).mockReturnValue([]);
    const activity = new FabricActivityStore();
    Object.assign(state, { activity, config: { ...state.config, mesh: { enabled: true } }, mesh });
    const context = { mode: "tui", ui: { setWidget: vi.fn(), notify: vi.fn() } } as unknown as ExtensionContext;
    const controller = new FabricUiController(state);
    try {
      controller.start(context);
      activity.start("work");
      await vi.advanceTimersByTimeAsync(200);
      await writer.put({ key: "status", value: "after", identity });
      vi.mocked(state.mainAgentInfo).mockReturnValue({ ...vi.mocked(state.mainAgentInfo)(),
        status: demand === "running" ? "running" : "idle", pendingMessages: demand === "pending",
      } as ReturnType<FabricState["mainAgentInfo"]>);
      await vi.advanceTimersByTimeAsync(100);
      expect(controller.snapshot().state.find(entry => entry.key === "status")?.value).toBe("after");
    } finally {
      controller.stop(); vi.restoreAllMocks(); vi.useRealTimers();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it("keeps stationary mesh state at zero extra canonical reads across idle UI polls", async () => {
    vi.useFakeTimers();
    const scratch = path.resolve(".local/check-temp");
    fs.mkdirSync(scratch, { recursive: true });
    const root = fs.mkdtempSync(path.join(scratch, "dashboard-stationary-"));
    const mesh = new MeshStore(root, 64 * 1024, 100, { readCacheMs: 2_000 });
    await mesh.put({ key: "status", value: "A", identity: { id: "session:writer", name: "writer", kind: "main" } });
    const stateFile = path.join(root, "state.json");
    const reads = vi.spyOn(fs, "readFileSync");
    const readCount = () => reads.mock.calls.filter(([file]) => String(file) === stateFile).length;
    const state = stubState();
    state.config.ui.refreshMs = 500;
    vi.mocked(state.actors.list).mockReturnValue([]);
    const activity = new FabricActivityStore();
    Object.assign(state, { activity, config: { ...state.config, mesh: { enabled: true } }, mesh });
    const context = { mode: "tui", ui: { setWidget: vi.fn(), notify: vi.fn() } } as unknown as ExtensionContext;
    const controller = new FabricUiController(state);
    try {
      controller.start(context);
      activity.start("live", { name: "local work" }); // Keep the UI polling while mesh state is idle.
      await vi.advanceTimersByTimeAsync(200);
      const before = readCount();
      const stamp = mesh.cachedStateStamp();
      for (let index = 0; index < 6; index++) {
        await vi.advanceTimersByTimeAsync(5_000);
        expect(mesh.cachedStateStamp(true)).toBe(stamp);
        expect(controller.snapshot().state.find((entry) => entry.key === "status")?.value).toBe("A");
        expect(readCount()).toBe(before); // Includes observer calls and the 15 s ceiling rebuilds.
      }
      expect(context.ui.notify).not.toHaveBeenCalled();
    } finally {
      controller.stop();
      vi.restoreAllMocks();
      vi.useRealTimers();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it.each(["ordinary poll consumed latest", "legacy unrelated writer copied marker"] as const)(
    "remote UI rebuild adds zero canonical reads when %s",
    async (order) => {
      vi.useFakeTimers();
      const scratch = path.resolve(".local/check-temp");
      fs.mkdirSync(scratch, { recursive: true });
      const root = fs.mkdtempSync(path.join(scratch, "dashboard-coalesced-"));
      const identity = { id: "session:writer", name: "writer", kind: "main" as const };
      const writer = new MeshStore(root, 64 * 1024, 100);
      const mesh = new MeshStore(root, 64 * 1024, 100, { readCacheMs: 2_000 });
      await writer.put({ key: "status", value: "A0", identity });
      const stateFile = path.join(root, "state.json");
      const realRead = fs.readFileSync;
      const reads = vi.spyOn(fs, "readFileSync");
      const readCount = () => reads.mock.calls.filter(([file]) => String(file) === stateFile).length;
      const state = stubState();
      state.config.ui.refreshMs = 500;
      vi.mocked(state.actors.list).mockReturnValue([]);
      const activity = new FabricActivityStore();
      Object.assign(state, { activity, config: { ...state.config, mesh: { enabled: true } }, mesh });
      const context = { mode: "tui", ui: { setWidget: vi.fn(), notify: vi.fn() } } as unknown as ExtensionContext;
      const controller = new FabricUiController(state);
      const shown = () => controller.snapshot().state.find((entry) => entry.key === "status")?.value;
      const gathered = vi.spyOn(mesh, "list");
      try {
        controller.start(context);
        activity.start("live", { name: "local work" });
        await vi.advanceTimersByTimeAsync(4_700);
        expect(shown()).toBe("A0");
        await writer.put({ key: "status", value: "A", identity });
        const afterWrite = readCount();
        const consumedToken = mesh.stateToken(); // Ordinary topology/poll reader already paid for A.
        expect(mesh.get("status")?.value).toBe("A");
        expect(readCount()).toBe(afterWrite + 1);
        const consumedStamp = mesh.cachedStateStamp();
        if (order === "legacy unrelated writer copied marker") {
          // A pre-generation writer replaces real canonical bytes under the real lock, retaining UUID.
          // This is not evidence of a new minted generation; do not restore old F1's forced full read.
          await writer.exclusive(() => {
            const disk = JSON.parse(String(realRead(stateFile, "utf8")));
            disk.entries.unrelated = { ...disk.entries.status, key: "unrelated", value: "legacy unrelated metadata change" };
            fs.writeFileSync(`${stateFile}.legacy-tmp`, JSON.stringify(disk));
            fs.renameSync(`${stateFile}.legacy-tmp`, stateFile);
          });
          expect(mesh.stateStamp()).not.toBe(consumedStamp);
        }
        const beforeUi = readCount();
        const beforeGather = gathered.mock.calls.length;
        await vi.advanceTimersByTimeAsync(1_000);
        expect(gathered.mock.calls.length).toBe(beforeGather + 1); // Exercise the real remote rebuild.
        expect(shown()).toBe("A");
        expect(mesh.stateToken()).toBe(consumedToken);
        expect(mesh.cachedStateStamp(true)).toBe(consumedStamp);
        expect(readCount()).toBe(beforeUi); // Observer + snapshot + ordinary reader share ONE parse.
        expect(context.ui.notify).not.toHaveBeenCalled();
      } finally {
        controller.stop();
        vi.restoreAllMocks();
        vi.useRealTimers();
        fs.rmSync(root, { recursive: true, force: true });
      }
    },
  );

  // review/astra F3 on #142: participant records live in files too (smarty-dev#2004). A rebuild for
  // a file-only change must show the listing it records as built, even when another reader warmed
  // the participant file cache just before the change.
  it("shows a file-only participant change after another reader warmed the participant cache", async () => {
    vi.useFakeTimers();
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-dashboard-"));
    const key = "topology/participants/" + "a".repeat(64);
    const writeExternally = (status: string) => {           // another process: no in-process cache hint
      const file = path.join(root, "participants", `${"a".repeat(64)}.json`);
      fs.mkdirSync(path.dirname(file), { recursive: true });
      fs.writeFileSync(`${file}.tmp`, JSON.stringify({
        format: 1, key, version: 1, updatedAt: 1, updatedBy: { id: "session:peer" }, value: { id: "peer", status },
      }));
      fs.renameSync(`${file}.tmp`, file);
    };
    writeExternally("A0");
    const mesh = new MeshStore(root, 64 * 1024, 100, { readCacheMs: 2_000 });
    await mesh.put({ key: "status", value: "unchanged", identity: { id: "session:w", name: "w", kind: "main", sessionId: "w" } });
    const state = stubState();
    state.config.ui.refreshMs = 500;
    vi.mocked(state.actors.list).mockReturnValue([]);
    const activity = new FabricActivityStore();
    const participantInfos = () => readParticipantFiles(root, { maxAgeMs: 2_000 }).map((entry) => ({
      ...(entry.value as object), kind: "agent", name: "peer", rootId: "peer", ownerHostId: "h", startedAt: 1, updatedAt: 1,
      runner: "pi", transport: "process", capabilities: [], local: false, stale: false,
    }));
    Object.assign(state, { activity, participantInfos, config: { ...state.config, mesh: { enabled: true } }, mesh });
    const context = { mode: "tui", ui: { setWidget: vi.fn(), notify: vi.fn() } } as unknown as ExtensionContext;
    const controller = new FabricUiController(state);
    const shown = () => controller.snapshot().agents.find((agent) => agent.id === "peer")?.status;
    try {
      controller.start(context);
      activity.start("live", { name: "local work" });            // polls at refreshMs
      await vi.advanceTimersByTimeAsync(4_700);
      expect(shown()).toBe("A0");
      writeExternally("A");
      expect(readParticipantFiles(root, { maxAgeMs: 0 })).toHaveLength(1);   // another reader: a warm cache holds A
      Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 30);      // B lands in a later timestamp tick
      writeExternally("B");
      await vi.advanceTimersByTimeAsync(2_500);
      expect(shown()).toBe("B");
    } finally {
      controller.stop();
      vi.useRealTimers();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it("rebuilds topology on lease-only re-acquisition without a state or participant record change", async () => {
    vi.useFakeTimers();
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-dashboard-leases-"));
    const mesh = new MeshStore(root, 64 * 1024, 100, { readCacheMs: 60_000 });
    const host = { id: "session:peer", rootId: "session:peer", identity: { id: "session:peer" },
      startedAt: 1, updatedAt: 1, expiresAt: 2 };
    const renew = (updatedAt: number) => writeHostLease(root, { id: host.id, rootId: host.rootId,
      identityId: host.identity.id, startedAt: host.startedAt, updatedAt, expiresAt: updatedAt + 15_000 });
    renew(Date.now() - 20_000); // a previously advertised owner is stale
    await mesh.put({ key: "status", value: "unchanged", identity: { id: "observer", name: "main", kind: "main" } });
    const before = fs.readFileSync(path.join(root, "state.json"), "utf8");
    const state = stubState();
    state.config.ui.refreshMs = 500;
    vi.mocked(state.actors.list).mockReturnValue([]);
    const activity = new FabricActivityStore();
    const participantInfos = () => hostLiveness(readHostLeases(root), host).expiresAt >= Date.now() ? [{
      format: 1, id: "peer-agent", kind: "agent", name: "peer", status: "idle", rootId: host.rootId,
      ownerHostId: host.id, ownerIdentityId: host.identity.id, startedAt: 1, updatedAt: 1,
      runner: "pi", transport: "process", capabilities: [], local: false, stale: false,
    }] : [];
    Object.assign(state, { activity, participantInfos, config: { ...state.config, mesh: { enabled: true } }, mesh });
    const context = { mode: "tui", ui: { setWidget: vi.fn(), notify: vi.fn() } } as unknown as ExtensionContext;
    const controller = new FabricUiController(state);
    try {
      controller.start(context);
      activity.start("live", { name: "local work" });
      await vi.advanceTimersByTimeAsync(4_700);
      expect(controller.snapshot().agents).toEqual([]);
      renew(Date.now());
      await vi.advanceTimersByTimeAsync(1_000); // next 5 s remote refresh, not the 15 s ceiling
      expect(controller.snapshot().agents.map(agent => agent.id)).toEqual(["peer-agent"]);
      expect(fs.readFileSync(path.join(root, "state.json"), "utf8")).toBe(before);
    } finally {
      controller.stop();
      vi.useRealTimers();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it("ticks the activity widget elapsed clock while nested calls are idle", async () => {
    vi.useFakeTimers();
    const state = stubState();
    state.config.ui.widget = "auto";
    state.config.ui.refreshMs = 500;
    state.config.ui.maxRows = 6;
    vi.mocked(state.actors.list).mockReturnValue([]);
    const activity = new FabricActivityStore();
    Object.assign(state, { activity });
    let widget: FabricWidget | undefined;
    const requestRender = vi.fn();
    const tui = { requestRender } as unknown as TUI;
    const context = {
      mode: "tui",
      ui: {
        notify: vi.fn(),
        setWidget: vi.fn((_key: string, content: unknown) => {
          if (typeof content === "function") {
            widget = (content as (t: TUI, theme: Theme) => FabricWidget)(tui, theme);
          }
        }),
      },
    } as unknown as ExtensionContext;
    const controller = new FabricUiController(state);
    try {
      controller.start(context);
      activity.start("live", { name: "T06 slice A" });
      for (let index = 0; index < 6; index++) {
        const callId = `c${index}`;
        activity.beginCall("live", { callId, ref: "pi.read", args: { path: `${index}.ts` } });
        activity.finishCall("live", callId, { success: true, result: "ok" });
      }
      await vi.advanceTimersByTimeAsync(1_100);
      expect(widget).toBeDefined();
      const first = widget!.render(80).join("\n");
      expect(first).toContain("T06 slice A");
      expect(first).toContain("6/6 calls");
      // Sub-second elapsed stays hidden, so the clock shows its first real tick.
      expect(first).toMatch(/1s/);
      expect(controller.snapshot().runs[0]?.status).toBe("running");
      requestRender.mockClear();
      await vi.advanceTimersByTimeAsync(5_000);
      const elapsedMs =
        controller.snapshot().now - controller.snapshot().runs[0]!.startedAt;
      const second = widget!.render(80).join("\n");
      expect(elapsedMs).toBeGreaterThanOrEqual(5_000);
      expect(second).toMatch(/6s/);
      expect(requestRender).toHaveBeenCalled();
    } finally {
      controller.stop();
      vi.useRealTimers();
    }
  });
});
