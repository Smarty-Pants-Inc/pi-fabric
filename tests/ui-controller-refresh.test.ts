import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionContext, Theme } from "@earendil-works/pi-coding-agent";
import type { TUI } from "@earendil-works/pi-tui";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { FabricActivityStore } from "../src/activity/store.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import type { FabricActorInfo } from "../src/actors/types.js";
import type { AgentRunRecord } from "../src/agents/types.js";
import type { FabricState } from "../src/fabric-state.js";
import type { MeshEvent } from "../src/mesh/store.js";
import { FabricUiController } from "../src/ui/controller.js";
import type { FabricDashboard } from "../src/ui/dashboard.js";
import * as snapshots from "../src/ui/snapshot.js";
import type { FabricWidget } from "../src/ui/widget.js";

const theme = {
  fg: (_color: string, text: string) => text,
  bg: (_color: string, text: string) => text,
  bold: (text: string) => text,
} as Theme;
const actor = (id = "actor-1"): FabricActorInfo => ({
  id, name: id, scope: "project", status: "idle", runner: "pi", events: [], topics: [],
  delivery: "mailbox", responseMode: "text", triggerTurn: false, coalesce: true,
  queued: 0, messages: 0, createdAt: 1, updatedAt: 1,
  filterSkipped: { count: 0, lastKey: null, lastTopic: null, lastAt: null },
});
const agent = (): AgentRunRecord => ({
  id: "agent-1", name: "reader", status: "running", runner: "pi", transport: "process",
  cwd: process.cwd(), task: "read", text: "", startedAt: 1, updatedAt: 1, turns: 0, toolCalls: 0,
  currentTool: "pi.read", usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
});
type Domain = "mesh" | "actor" | "agent" | "activity";

// Real controller, snapshot builder, activity revisions, mounted widget and dashboard.
// Only the host UI and manager/mesh I/O boundaries are fixtures. In particular, no
// replacement scheduler is installed: listeners captured by start() enter the
// controller's actual #scheduleRefresh -> setTimeout -> #refresh path.
const harness = (refreshMs = 5_000) => {
  const listeners = new Map<Domain, () => void>();
  const subscribe = (domain: Domain) => vi.fn((listener: () => void) => {
    listeners.set(domain, listener);
    return () => { listeners.delete(domain); };
  });
  const activity = new FabricActivityStore();
  const subscribeActivity = activity.subscribe.bind(activity);
  vi.spyOn(activity, "subscribe").mockImplementation(listener => {
    listeners.set("activity", listener);
    const unsubscribe = subscribeActivity(listener);
    return () => { listeners.delete("activity"); unsubscribe(); };
  });
  const records = [agent()];
  const actors = [actor()];
  const log: MeshEvent[] = [];
  const state = {
    initialized: true, cwd: process.cwd(), widgetDismissedAt: 0,
    config: {
      ui: { enabled: true, refreshMs, eventHistory: 80, widget: "auto", maxRows: 8 },
      mesh: { enabled: true },
    },
    activity,
    agents: { listForUi: vi.fn(() => records), subscribeUi: subscribe("agent") },
    actors: {
      list: vi.fn(() => actors), instructions: vi.fn(() => "Observe only."),
      messages: vi.fn(() => []), subscribe: subscribe("actor"), subscribeMesh: subscribe("mesh"),
    },
    globalActors: { list: vi.fn(() => []), stamp: vi.fn(() => "global-stable") },
    mainAgentInfo: vi.fn(() => ({
      id: "main", name: "Main", kind: "main", status: "idle", runner: "pi", transport: "host",
      cwd: process.cwd(), sessionId: "main", startedAt: 1, updatedAt: 1,
      pendingMessages: false, local: true,
    })),
    participantInfos: vi.fn(() => []), peerInfos: vi.fn(() => []),
    componentGraph: vi.fn(() => ({ components: [], edges: [], cycles: [] })),
    mesh: {
      read: vi.fn(() => []), latestOffset: vi.fn(() => 0),
      tail: vi.fn((offset: number) => ({ events: log.slice(offset), nextOffset: log.length })),
      list: vi.fn(() => []), cachedStateStamp: vi.fn(() => "mesh-stable"),
    },
  } as unknown as FabricState;
  let widget: FabricWidget | undefined;
  const widgetRender = vi.fn();
  const widgetTui = {
    terminal: { rows: 40 }, requestRender: () => { widgetRender(); widget?.render(160); },
  } as unknown as TUI;
  const dashboardRender = vi.fn();
  const dashboardTui = { terminal: { rows: 40 }, requestRender: dashboardRender } as unknown as TUI;
  let dashboard: FabricDashboard | undefined;
  let closeDashboard = () => {};
  let attached = () => {};
  const dashboardReady = new Promise<void>(resolve => { attached = resolve; });
  let pendingDashboard: Promise<void> | undefined;
  const notify = vi.fn();
  const context = {
    mode: "tui", cwd: process.cwd(), modelRegistry: { getAvailable: () => [] }, ui: {
      notify,
      setWidget: vi.fn((_key: string, content: unknown) => {
        if (typeof content === "function") {
          widget = (content as (tui: TUI, theme: Theme) => FabricWidget)(widgetTui, theme);
          widget.render(160); // A real mounted/painted widget, not just setWidget being recorded.
        } else widget = undefined;
      }),
      custom: (factory: (tui: TUI, theme: Theme, keys: unknown, done: () => void) => FabricDashboard) =>
        new Promise<void>(resolve => {
          closeDashboard = resolve;
          dashboard = factory(dashboardTui, theme, {}, resolve);
          attached();
        }),
    },
  } as unknown as ExtensionContext;
  const controller = new FabricUiController(state);
  const emit = (domain: Domain) => {
    const listener = listeners.get(domain);
    expect(listener, `${domain} subscription must be attached`).toBeTypeOf("function");
    listener!();
  };
  const meshEvent = () => {
    const sequence = log.length + 1;
    log.push({ id: `event-${sequence}`, sequence, topic: "telemetry", kind: "heartbeat",
      from: { id: "remote", name: "remote", kind: "main" }, createdAt: Date.now() });
    emit("mesh");
  };
  const h = {
    controller, state, context, activity, records, actors, emit, meshEvent, notify,
    widgetRender, dashboardRender,
    widgetText: () => {
      expect(widget, "start() must attach the widget factory").toBeDefined();
      return widget!.render(160).join("\n");
    },
    async openDashboard() {
      pendingDashboard = controller.openDashboard(context);
      await Promise.race([dashboardReady, pendingDashboard.then(() => {
        throw new Error("dashboard closed before its host factory attached");
      })]);
      expect(controller.ownsInput).toBe(true);
      expect(dashboard).toBeDefined();
    },
    async dispose() {
      closeDashboard();
      if (pendingDashboard) await pendingDashboard;
      dashboard?.dispose();
      controller.stop();
    },
  };
  harnesses.push(h);
  return h;
};
const harnesses: Array<{ dispose(): Promise<void>; notify: ReturnType<typeof vi.fn> }> = [];
let buildSnapshot: import("vitest").MockInstance<typeof snapshots.createDashboardSnapshot>;
beforeEach(() => {
  vi.useFakeTimers({ now: 10_000 });
  vi.stubEnv("SMARTY_ROLE", "project-agent@ui-refresh-red");
  // Call-through spy: these counts include the actual projection and expensive manager reads.
  buildSnapshot = vi.spyOn(snapshots, "createDashboardSnapshot");
});
afterEach(async () => {
  for (const h of harnesses.splice(0)) {
    await h.dispose();
    expect(h.notify).not.toHaveBeenCalled();
  }
  expect(vi.getTimerCount()).toBe(0);
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  vi.useRealTimers();
});

describe("closed mounted widget refresh budget (test-first RED)", () => {
  it("includes mount in a <=1 snapshot budget for 20 mesh events spread over 1000ms at refreshMs=5000", async () => {
    const h = harness();
    h.controller.start(h.context);
    expect(h.widgetText()).toContain("reader");
    expect(buildSnapshot).toHaveBeenCalledTimes(1);
    expect(h.controller.ownsInput).toBe(false);
    const startedAt = performance.now();
    // Genuine widget changes prevent an unchanged-data fast path from masking
    // a broken minimum interval. Events occur at 0, 50, ..., 950 ms.
    for (let event = 0; event < 20; event++) {
      h.records[0] = { ...h.records[0]!, currentTool: `visible-tool-${event}`, updatedAt: Date.now() };
      h.meshEvent();
      await vi.advanceTimersByTimeAsync(50);
    }
    expect(performance.now() - startedAt).toBe(1_000);
    expect(buildSnapshot.mock.calls.length, "20 events / 1000ms; initial mount IS included").toBeLessThanOrEqual(1);
  });

  it("skips expensive snapshot rebuilds at successive allowed slots when only dashboard-only events changed", async () => {
    const h = harness();
    h.controller.start(h.context);
    const before = h.widgetText();
    expect(buildSnapshot).toHaveBeenCalledTimes(1);
    const readsAtMount = vi.mocked(h.state.actors.instructions).mock.calls.length;
    h.widgetRender.mockClear();
    for (let slot = 1; slot <= 2; slot++) {
      h.meshEvent(); // Real #pollMesh will see a new event, but widget-visible data is unchanged.
      await vi.advanceTimersByTimeAsync(5_000);
      expect.soft(buildSnapshot.mock.calls.length, `unchanged widget after allowed slot ${slot}`).toBe(1);
      expect.soft(vi.mocked(h.state.actors.instructions).mock.calls.length,
        `no repeated expensive actor payload reads after slot ${slot}`).toBe(readsAtMount);
      expect(h.widgetText()).toBe(before);
      expect(h.widgetRender).not.toHaveBeenCalled();
    }
  });

  it("shows a real activity revision at the next allowed slot without another notification", async () => {
    const h = harness();
    h.controller.start(h.context);
    expect(h.widgetText()).not.toContain("new activity");
    h.widgetRender.mockClear();
    h.activity.start("run-1", { name: "new activity" }); // Real store emits its own subscriber notification.
    await vi.advanceTimersByTimeAsync(5_000);
    expect(h.controller.snapshot().runs[0]?.name).toBe("new activity");
    expect(h.widgetText()).toContain("new activity");
    expect(h.widgetRender).toHaveBeenCalledTimes(1);
    expect(buildSnapshot).toHaveBeenCalledTimes(2);
  });

  it("does not starve changed agent data after an unchanged slot, behind a pending burst or a stable mesh stamp", async () => {
    const h = harness();
    h.controller.start(h.context);
    h.meshEvent();
    await vi.advanceTimersByTimeAsync(5_000); // An unchanged slot precedes the relevant change.
    expect(h.widgetText()).toContain("pi.read");
    h.meshEvent(); // Arm the actual coalescer before the relevant change arrives.
    await vi.advanceTimersByTimeAsync(50);
    h.records[0] = { ...h.records[0]!, currentTool: "pi.write", toolCalls: 9, updatedAt: Date.now() };
    h.emit("agent");
    h.widgetRender.mockClear();
    for (let event = 0; event < 20; event++) {
      h.meshEvent();
      await vi.advanceTimersByTimeAsync(50);
    }
    await vi.advanceTimersByTimeAsync(3_950); // Exactly 10000 ms from mount; no trailing notification needed.
    expect(h.controller.snapshot().agents[0]).toMatchObject({ currentTool: "pi.write", toolCalls: 9 });
    expect(h.widgetText()).toContain("pi.write");
    expect(h.widgetText()).toContain("9 calls");
    expect(h.widgetRender).toHaveBeenCalledTimes(1);
  });

  it("observes a replaced actor registry generation on a mesh notification with no actor/activity change signal", async () => {
    // Exercise the existing registry generation/cache, not a new UI cache API.
    // Only this exact file's stat/read are virtualized; there are no disk writes.
    const root = path.join(os.tmpdir(), "ui-refresh-red-registry");
    const file = path.join(root, "actors.json");
    let inode = 1n;
    let bytes = JSON.stringify({ format: 1, actors: [actor()] });
    const nativeStat = fs.statSync;
    const nativeRead = fs.readFileSync;
    vi.spyOn(fs, "statSync").mockImplementation(((target: fs.PathLike, options: unknown) => {
      if (String(target) === file) return {
        dev: 1n, ino: inode, size: BigInt(bytes.length), mtimeNs: inode, ctimeNs: inode,
      } as fs.BigIntStats;
      return Reflect.apply(nativeStat, fs, [target, options]);
    }) as typeof fs.statSync);
    vi.spyOn(fs, "readFileSync").mockImplementation(((target: fs.PathOrFileDescriptor, options: unknown) =>
      String(target) === file ? bytes : Reflect.apply(nativeRead, fs, [target, options])) as typeof fs.readFileSync);
    const registry = new ActorRegistryStore(root);
    const h = harness();
    vi.mocked(h.state.actors.list).mockImplementation(() =>
      registry.snapshot().actors as unknown as FabricActorInfo[]);
    const initial = registry.snapshot();
    h.controller.start(h.context);
    expect(h.widgetText()).toContain("1 actor");
    h.widgetRender.mockClear();
    bytes = JSON.stringify({ format: 1, actors: [actor(), actor("actor-2")] });
    inode++;
    expect(registry.fingerprint()).not.toBe(initial.generation);
    h.meshEvent(); // No actors.subscribe emission; mesh/activity stamps remain unchanged.
    await vi.advanceTimersByTimeAsync(5_000);
    expect(registry.snapshot()).not.toBe(initial);
    expect(h.controller.snapshot().actors.map(row => row.id)).toEqual(["actor-1", "actor-2"]);
    expect(h.widgetText()).toContain("2 actors");
    expect(h.widgetRender).toHaveBeenCalledTimes(1);
    expect(buildSnapshot).toHaveBeenCalledTimes(2);
    registry.releaseReadCache();
  });
});

describe("open dashboard retains existing event behavior", () => {
  it.each(["mesh", "actor", "agent", "activity"] as const)(
    "keeps %s notifications responsive at the existing 1000ms minimum, even for unchanged widget data",
    async domain => {
      const h = harness(10);
      h.controller.start(h.context);
      await h.openDashboard();
      // Open/attach snapshots are deliberately excluded only in open-dashboard controls.
      buildSnapshot.mockClear();
      h.dashboardRender.mockClear();
      for (let event = 0; event < 20; event++) h.emit(domain);
      expect(vi.getTimerCount()).toBe(1);
      await vi.advanceTimersByTimeAsync(999);
      expect(buildSnapshot).not.toHaveBeenCalled();
      expect(h.dashboardRender).not.toHaveBeenCalled();
      await vi.advanceTimersByTimeAsync(1);
      expect(buildSnapshot).toHaveBeenCalledTimes(1);
      expect(h.dashboardRender).toHaveBeenCalledTimes(1);
      h.emit(domain);
      await vi.advanceTimersByTimeAsync(1_000);
      expect(buildSnapshot).toHaveBeenCalledTimes(2);
      expect(h.dashboardRender).toHaveBeenCalledTimes(2);
      await vi.advanceTimersByTimeAsync(5_000);
      expect(buildSnapshot).toHaveBeenCalledTimes(2); // Still event driven, no idle polling.
    },
  );

  it("keeps the configured 5000ms open-dashboard interval and displays event-only changes at that slot", async () => {
    const h = harness();
    h.controller.start(h.context);
    await h.openDashboard();
    buildSnapshot.mockClear();
    h.dashboardRender.mockClear();
    h.meshEvent();
    await vi.advanceTimersByTimeAsync(4_999);
    expect(buildSnapshot).not.toHaveBeenCalled();
    expect(h.controller.snapshot().events).toEqual([]);
    await vi.advanceTimersByTimeAsync(1);
    expect(buildSnapshot).toHaveBeenCalledTimes(1);
    expect(h.dashboardRender).toHaveBeenCalledTimes(1);
    expect(h.controller.snapshot().events.map(event => event.id)).toEqual(["event-1"]);
  });
});

// Cache-sensitive controls appended after the frozen original ten RED cases.
describe("closed widget cache freshness boundaries", () => {
  it("ignores volatile Main/participant heartbeat timestamps, skips dashboard payloads, and clears on a new epoch", async () => {
    const h = harness();
    const main = h.state.mainAgentInfo(h.context);
    vi.mocked(h.state.mainAgentInfo).mockImplementation(() => ({ ...main, updatedAt: Date.now() }));
    vi.mocked(h.state.participantInfos).mockImplementation(() => [{
      id: "agent-1", kind: "agent", rootId: "main", ownerHostId: "remote", local: false,
      stale: false, capabilities: [], updatedAt: Date.now(),
    } as never]);
    h.controller.start(h.context);
    const before = h.widgetText();
    h.widgetRender.mockClear();
    for (let slot = 0; slot < 2; slot++) {
      h.meshEvent();
      await vi.advanceTimersByTimeAsync(5_000);
      expect(h.controller.snapshot().now).toBe(Date.now());
      expect(h.widgetText()).toBe(before);
    }
    expect(buildSnapshot).toHaveBeenCalledTimes(1);
    expect(h.widgetRender).not.toHaveBeenCalled();
    for (const read of [h.state.actors.instructions, h.state.actors.messages, h.state.globalActors.list,
      h.state.peerInfos, h.state.componentGraph, h.state.mesh.list]) expect(read).toHaveBeenCalledTimes(1);
    expect(h.state.actors.list).toHaveBeenCalledTimes(3);
    expect(h.state.participantInfos).toHaveBeenCalledTimes(3);
    expect(h.state.mesh.tail).toHaveBeenCalledTimes(3);
    h.controller.start(h.context);
    expect(buildSnapshot).toHaveBeenCalledTimes(2);
    expect(h.widgetText()).toBe(before);
  });

  it("observes participant lease liveness at an admitted slot despite a stable mesh stamp", async () => {
    const h = harness();
    vi.mocked(h.state.participantInfos).mockImplementation(() => [{
      id: "agent-1", kind: "agent", rootId: "main", ownerHostId: "remote", local: false,
      stale: Date.now() >= 15_000, capabilities: [], updatedAt: 1,
    } as never]);
    h.controller.start(h.context);
    const before = h.widgetText();
    expect(h.controller.snapshot().agents[0]?.stale).toBe(false);
    h.widgetRender.mockClear();
    h.meshEvent();
    await vi.advanceTimersByTimeAsync(5_000);
    expect(h.controller.snapshot().agents[0]?.stale).toBe(true);
    expect(h.widgetText()).not.toBe(before); // The live-conversation hint disappears.
    expect(h.widgetRender).toHaveBeenCalledTimes(1);
    expect(buildSnapshot).toHaveBeenCalledTimes(2);
  });

  it("retires cached completion rows at the first 30s event with refreshMs=60000 without moving pending dirty data", async () => {
    const h = harness(60_000);
    h.actors.splice(0);
    h.records[0] = { ...h.records[0]!, status: "completed", finishedAt: 10_000 };
    h.records.push({ ...agent(), id: "live-agent", name: "worker" });
    const collectInputs = vi.spyOn(snapshots, "collectDashboardSnapshotInputs");
    const activityRevision = vi.spyOn(h.activity, "revision");
    h.controller.start(h.context);
    expect(h.widgetText()).toContain("reader");
    expect(vi.getTimerCount()).toBe(0);
    await vi.advanceTimersByTimeAsync(1_000);
    h.records[1] = { ...h.records[1]!, currentTool: "pending-dirty-tool", updatedAt: Date.now() };
    h.emit("agent"); // Dirty data already owns the full refresh at 60000ms from mount.
    expect(vi.getTimerCount()).toBe(1);
    const observations = [collectInputs, buildSnapshot, activityRevision, h.state.mainAgentInfo,
      h.state.actors.list, h.state.agents.listForUi, h.state.participantInfos,
      h.state.mesh.tail, h.state.mesh.cachedStateStamp, h.state.globalActors.stamp].map(read => vi.mocked(read!));
    const counts = observations.map(read => read.mock.calls.length);
    const stat = vi.spyOn(fs, "statSync");
    const read = vi.spyOn(fs, "readFileSync");
    const watch = vi.spyOn(fs, "watch");
    await vi.advanceTimersByTimeAsync(29_000);
    expect(h.widgetText()).toContain("reader"); // No independent expiry clock.
    h.widgetRender.mockClear();
    h.meshEvent(); // First event at the exact 30s completion boundary, with the full timeout pending.
    expect(h.widgetText()).not.toContain("reader");
    expect(h.widgetText()).toContain("worker");
    expect(h.widgetText()).not.toContain("pending-dirty-tool");
    expect(h.widgetRender).toHaveBeenCalledTimes(1);
    expect(h.controller.snapshot().now).toBe(40_000);
    expect(h.controller.snapshot().agents.find(row => row.id === "agent-1")?.status).toBe("completed");
    expect(observations.map(observation => observation.mock.calls.length)).toEqual(counts);
    for (const io of [stat, read, watch]) expect(io).not.toHaveBeenCalled();
    expect(buildSnapshot).toHaveBeenCalledTimes(1);
    expect(vi.getTimerCount()).toBe(1);
    await vi.advanceTimersByTimeAsync(29_999);
    expect(h.widgetText()).not.toContain("pending-dirty-tool");
    expect(observations.map(observation => observation.mock.calls.length)).toEqual(counts);
    await vi.advanceTimersByTimeAsync(1); // Original data slot, not 60000ms after the retention paint.
    expect(h.widgetText()).toContain("pending-dirty-tool");
    expect(h.widgetText()).not.toContain("reader");
    expect(buildSnapshot).toHaveBeenCalledTimes(2);
    expect(collectInputs).toHaveBeenCalledTimes(2);
    expect(h.controller.snapshot().now).toBe(70_000);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("loads full activity and dashboard-only inputs on first open after closed cache hits", async () => {
    const h = harness();
    h.activity.start("detailed", { name: "detailed activity" });
    h.activity.beginCall("detailed", { callId: "call", ref: "pi.read", args: { path: "detail.txt" } });
    h.controller.start(h.context);
    expect(h.controller.snapshot().runs[0]?.calls[0]?.args).toBeUndefined();
    vi.mocked(h.state.actors.instructions).mockReturnValue("Fresh dashboard-only instructions");
    h.meshEvent();
    await vi.advanceTimersByTimeAsync(5_000);
    expect(buildSnapshot).toHaveBeenCalledTimes(1);
    expect(h.controller.snapshot().events).toEqual([]);
    await h.openDashboard();
    expect(h.controller.snapshot().runs[0]?.calls[0]?.args).toEqual({ path: "detail.txt" });
    expect(h.controller.snapshot().actors[0]?.instructions).toBe("Fresh dashboard-only instructions");
    expect(h.controller.snapshot().events.map(event => event.id)).toEqual(["event-1"]);
  });

  it("releases the streamed height immediately at an explicit host boundary, not at the event minimum", () => {
    const h = harness();
    h.controller.start(h.context);
    h.controller.setHostStreaming(true);
    expect(h.widgetText().split("\n")).toHaveLength(8);
    h.meshEvent(); // A pending event must not defer the explicit boundary.
    h.controller.setHostStreaming(false);
    expect(h.controller.snapshot().main.status).toBe("idle");
    expect(h.widgetText().split("\n").length).toBeLessThan(8);
    expect(performance.now()).toBe(0);
  });

  it("rebases pending event work after an explicit refresh without dropping later dirty data", async () => {
    const h = harness();
    h.controller.start(h.context);
    h.meshEvent(); // Initial deadline: 5000ms from mount.
    await vi.advanceTimersByTimeAsync(3_000);
    h.controller.setHostStreaming(true); // Explicit lifecycle refresh is immediate.
    expect(buildSnapshot).toHaveBeenCalledTimes(2);
    h.records[0] = { ...h.records[0]!, currentTool: "dirty-after-host-refresh", updatedAt: Date.now() };
    h.emit("agent"); // The already-pending timeout must retain this work.
    await vi.advanceTimersByTimeAsync(2_000);
    expect(buildSnapshot).toHaveBeenCalledTimes(2); // Old 5000ms deadline is now too early.
    expect(h.widgetText()).not.toContain("dirty-after-host-refresh");
    expect(vi.getTimerCount()).toBe(1);
    await vi.advanceTimersByTimeAsync(2_999);
    expect(buildSnapshot).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(1); // 5000ms since the explicit refresh, no new notification.
    expect(buildSnapshot).toHaveBeenCalledTimes(3);
    expect(h.widgetText()).toContain("dirty-after-host-refresh");
    expect(vi.getTimerCount()).toBe(0);
  });

  it("fully refreshes explicit closed snapshots with stable widget inputs and seeds the next scheduled cache hit", async () => {
    const h = harness();
    h.controller.start(h.context);
    const before = h.widgetText();
    vi.mocked(h.state.actors.instructions).mockReturnValue("Fresh explicit-only instructions");
    h.meshEvent();
    await vi.advanceTimersByTimeAsync(5_000);
    expect(buildSnapshot).toHaveBeenCalledTimes(1);
    expect(h.controller.snapshot().actors[0]?.instructions).toBe("Observe only.");
    expect(h.controller.snapshot().events).toEqual([]);

    h.controller.setHostStreaming(false); // Main remains idle: widget inputs are identical.
    expect(buildSnapshot).toHaveBeenCalledTimes(2);
    expect(h.controller.snapshot().actors[0]?.instructions).toBe("Fresh explicit-only instructions");
    expect(h.controller.snapshot().events.map(event => event.id)).toEqual(["event-1"]);
    expect(h.state.mesh.list).toHaveBeenCalledTimes(2);
    expect(h.widgetText()).toBe(before);
    const payloadReads = vi.mocked(h.state.actors.instructions).mock.calls.length;

    h.meshEvent();
    await vi.advanceTimersByTimeAsync(5_000);
    expect(buildSnapshot).toHaveBeenCalledTimes(2);
    expect(h.state.actors.instructions).toHaveBeenCalledTimes(payloadReads);
    expect(h.state.mesh.list).toHaveBeenCalledTimes(2);
    expect(h.controller.snapshot().actors[0]?.instructions).toBe("Fresh explicit-only instructions");
    expect(h.controller.snapshot().events.map(event => event.id)).toEqual(["event-1"]);
    expect(h.widgetText()).toBe(before);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("does not cache an uncertain actor that disappeared between listing and details", async () => {
    const h = harness();
    h.controller.start(h.context);
    h.actors[0] = { ...h.actors[0]!, name: "renamed actor" };
    vi.mocked(h.state.actors.instructions).mockImplementationOnce(() => { throw new Error("Unknown Fabric actor: actor-1"); });
    h.meshEvent();
    await vi.advanceTimersByTimeAsync(5_000);
    expect(h.controller.snapshot().actors).toEqual([]);
    h.meshEvent();
    await vi.advanceTimersByTimeAsync(5_000);
    expect(h.controller.snapshot().actors[0]?.name).toBe("renamed actor");
    expect(buildSnapshot).toHaveBeenCalledTimes(3);
  });

  it("bypasses the closed gate before conversation attachment and keeps its source watcher on the existing 100ms cadence", async () => {
    const { initTheme } = await import("@earendil-works/pi-coding-agent");
    const { FabricConversationView } = await import("../src/ui/conversation.js");
    initTheme("dark", false);
    const h = harness();
    h.records[0] = { ...h.records[0]!, status: "completed", finishedAt: 10_000,
      logFile: path.join(os.tmpdir(), "ui-refresh-conversation", "events.jsonl") };
    h.controller.start(h.context);
    h.meshEvent();
    await vi.advanceTimersByTimeAsync(5_000);
    expect(buildSnapshot).toHaveBeenCalledTimes(1);
    vi.mocked(h.state.actors.instructions).mockReturnValue("Fresh conversation inputs");
    const watch = vi.spyOn(fs, "watch").mockImplementation(() => ({
      on: vi.fn().mockReturnThis(), close: vi.fn(),
    }) as unknown as fs.FSWatcher);
    let view: InstanceType<typeof FabricConversationView> | undefined;
    let close = () => {};
    let attached = () => {};
    const ready = new Promise<void>(resolve => { attached = resolve; });
    const tui = { terminal: { rows: 40 }, requestRender: vi.fn() } as unknown as TUI;
    vi.spyOn(h.context.ui, "custom").mockImplementation(((factory: (
      tui: TUI, theme: Theme, keys: unknown, done: () => void,
    ) => InstanceType<typeof FabricConversationView>) => new Promise<void>(resolve => {
      close = resolve;
      // openConversation's force-fresh snapshot precedes attachment AND target resolution.
      expect(h.controller.snapshot().actors[0]?.instructions).toBe("Fresh conversation inputs");
      view = factory(tui, theme, { matches: () => false, getKeys: () => [] }, resolve);
      attached();
    })) as ExtensionContext["ui"]["custom"]);
    const pending = h.controller.openConversation(h.context, "agent-1");
    try {
      await Promise.race([ready, pending.then(() => { throw new Error("conversation did not attach"); })]);
      expect(view).toBeInstanceOf(FabricConversationView);
      expect(buildSnapshot).toHaveBeenCalledTimes(2);
      expect(view!.render(160).join("\n")).toContain("reader");
      const refresh = vi.spyOn(view!, "refresh");
      expect(watch).toHaveBeenCalled();
      const callback = watch.mock.calls[0]!.at(-1) as (event: string, filename: string) => void;
      callback("change", "events.jsonl");
      await vi.advanceTimersByTimeAsync(99);
      expect(buildSnapshot).toHaveBeenCalledTimes(2);
      await vi.advanceTimersByTimeAsync(1);
      expect(buildSnapshot).toHaveBeenCalledTimes(3);
      expect(refresh).toHaveBeenCalledTimes(1);
      callback("change", "events.jsonl");
      await vi.advanceTimersByTimeAsync(100);
      expect(buildSnapshot).toHaveBeenCalledTimes(4);
    } finally {
      close();
      await pending;
    }
  });
});

// F1 controls appended after the complete, byte-preserved original 19 cases.
// Keep reason: independent audit F1 caught normal shell output invalidating the
// closed-widget cache despite unchanged rows. Only public shell I/O is a fixture;
// start() installs the real receiver, and collector/builder spies call through.
type ShellInfo = import("../src/core/shell-jobs.js").FabricShellJobInfo;
type ShellEvent = import("../src/core/shell-jobs.js").FabricShellJobEvent;
type ShellChange = { [Key in keyof ShellInfo]?: ShellInfo[Key] | undefined };
const monitoredShell = (): ShellInfo => ({
  id: "shell-01-monitored", tool: "bash", command: "tail -f build.log", description: "Build monitor",
  startedAt: 1_000, spilledAt: 2_000, status: "spilled", stopping: false,
  monitor: { delivery: "ui", intervalMs: 1_000, timeoutMs: 300_000 },
  eventCount: 0, unread: false,
});
const attachShells = (h: ReturnType<typeof harness>, jobs = [monitoredShell()]) => {
  let receiver: ((event: ShellEvent) => void) | undefined;
  const info = (job: ShellInfo): ShellInfo => structuredClone(job);
  const list = vi.fn(() => jobs.map(info)); // Public info()/list() return fresh metadata copies.
  const subscribe = vi.fn((listener: (event: ShellEvent) => void) => {
    receiver = listener;
    return () => { receiver = undefined; };
  });
  Object.assign(h.state, { shellJobs: { list, subscribe } }); // Before controller.start().
  const emit = (type: ShellEvent["type"], job = jobs[0]!) => {
    expect(receiver, "start() must install the actual shell subscriber").toBeTypeOf("function");
    receiver!({ type, job: info(job) });
  };
  return {
    jobs, list, subscribe, emit,
    output() { jobs[0]!.lastOutputAt = Date.now(); }, // append(nonempty data), BEFORE batching.
    batch(line: string) {
      const job = jobs[0]!;
      job.lastEvent = { lines: [line], omitted: 0, at: Date.now() };
      job.eventCount++;
      job.unread = true;
      emit("monitor"); // ShellMonitor flush -> replay.record -> onChange("monitor").
    },
  };
};

describe("F1 shell monitor closed-cache comparison", () => {
  it("reuses successive output/monitor slots but keeps explicit and dashboard metadata fresh", async () => {
    const h = harness();
    const shell = attachShells(h);
    const collect = vi.spyOn(snapshots, "collectDashboardSnapshotInputs");
    h.controller.start(h.context);
    expect(shell.subscribe).toHaveBeenCalledTimes(1);
    const before = h.widgetText();
    expect(before).toContain("monitor:ui");
    expect(before).toContain("Build monitor");
    const expensive = [h.state.actors.instructions, h.state.actors.messages, h.state.globalActors.list,
      h.state.peerInfos, h.state.componentGraph, h.state.mesh.list].map(read => vi.mocked(read));
    const counts = expensive.map(read => read.mock.calls.length);
    h.widgetRender.mockClear();
    for (let slot = 1; slot <= 2; slot++) {
      await vi.advanceTimersByTimeAsync(10);
      shell.output(); // Real output changes lastOutputAt even before the monitor notification.
      await vi.advanceTimersByTimeAsync(1_000);
      shell.batch(`progress ${slot}`);
      expect(shell.jobs[0]!.lastOutputAt).toBeLessThan(shell.jobs[0]!.lastEvent!.at);
      expect(vi.getTimerCount()).toBe(1);
      await vi.advanceTimersByTimeAsync(3_989);
      expect(collect).toHaveBeenCalledTimes(slot); // No early observation.
      await vi.advanceTimersByTimeAsync(1); // Actual configured slot, not a replacement scheduler.
      expect.soft(buildSnapshot, `unchanged output slot ${slot}`).toHaveBeenCalledTimes(1);
      expect.soft(expensive.map(read => read.mock.calls.length)).toEqual(counts);
      expect(collect).toHaveBeenCalledTimes(slot + 1);
      expect(shell.list).toHaveBeenCalledTimes(slot + 1);
      expect(h.widgetText()).toBe(before);
      expect(h.widgetRender).not.toHaveBeenCalled();
      expect(vi.getTimerCount()).toBe(0);
    }
    // Acknowledgement still reaches the receiver but does not change widget rows.
    shell.jobs[0]!.unread = false;
    shell.emit("acknowledged");
    await vi.advanceTimersByTimeAsync(5_000);
    expect(buildSnapshot).toHaveBeenCalledTimes(1);
    expect(shell.list()[0]).toMatchObject({ eventCount: 2, unread: false, lastOutputAt: 15_010,
      lastEvent: { at: 16_010, lines: ["progress 2"] } });
    h.controller.setHostStreaming(false); // Explicit, identical widget key: full projection promised.
    expect(buildSnapshot).toHaveBeenCalledTimes(2);
    expect(h.controller.snapshot().shells).toEqual(shell.list());
    expect(h.widgetText()).toBe(before);
    shell.output();
    shell.batch("progress 3");
    await vi.advanceTimersByTimeAsync(5_000);
    expect(buildSnapshot).toHaveBeenCalledTimes(2); // Explicit refresh seeded the existing cache.
    expect(h.controller.snapshot().shells?.[0]?.eventCount).toBe(2);
    await h.openDashboard();
    expect(h.controller.snapshot().shells).toEqual(shell.list());
    const buildsAtOpen = buildSnapshot.mock.calls.length;
    shell.output();
    shell.batch("progress 4");
    await vi.advanceTimersByTimeAsync(5_000);
    expect(buildSnapshot).toHaveBeenCalledTimes(buildsAtOpen + 1);
    expect(h.controller.snapshot().shells).toEqual(shell.list());
    expect(h.controller.snapshot().shells?.[0]).toMatchObject({ eventCount: 4, unread: true });
    expect(vi.getTimerCount()).toBe(0);
  });

  it.each<[string, ShellChange]>([
    ["status", { status: "failed" }],
    ["stopping", { stopping: true }],
    ["monitor delivery", { monitor: { delivery: "wake", intervalMs: 1_000, timeoutMs: 300_000 } }],
    ["description", { description: "Renamed monitor" }],
    ["command fallback", { description: undefined, command: "tail -f other.log" }],
    ["completion", { status: "exited", finishedAt: 11_000, exitCode: 0 }],
    ["identity", { id: "shell-02-replaced" }],
    ["start identity", { startedAt: 2_000 }],
  ])(
    "delivers changed shell %s at the next admitted slot", async (_name, change) => {
      const h = harness();
      const shell = attachShells(h);
      const collect = vi.spyOn(snapshots, "collectDashboardSnapshotInputs");
      h.controller.start(h.context);
      const before = h.widgetText();
      Object.assign(shell.jobs[0]!, change);
      shell.emit(change.finishedAt !== undefined ? "finished" : change.stopping ? "stopping" : "monitor");
      await vi.advanceTimersByTimeAsync(4_999);
      expect(buildSnapshot).toHaveBeenCalledTimes(1);
      expect(h.widgetText()).toBe(before);
      await vi.advanceTimersByTimeAsync(1);
      expect(buildSnapshot).toHaveBeenCalledTimes(2);
      expect(collect).toHaveBeenCalledTimes(2);
      expect(h.controller.snapshot().shells).toEqual(shell.list());
      // Start identity is needed for completion duration/retention, not a live clock.
      if (change.startedAt === undefined) expect(h.widgetText()).not.toBe(before);
      expect(vi.getTimerCount()).toBe(0);
    },
  );

  it("preserves source order, visibility and first-event 30s shell retention without polling", async () => {
    const h = harness(60_000);
    const shell = attachShells(h, [monitoredShell(), { ...monitoredShell(), id: "shell-02-second",
      description: "Second monitor" }]);
    const collect = vi.spyOn(snapshots, "collectDashboardSnapshotInputs");
    h.controller.start(h.context);
    const before = h.widgetText();
    shell.jobs.reverse();
    shell.emit("monitor");
    await vi.advanceTimersByTimeAsync(60_000);
    expect(buildSnapshot).toHaveBeenCalledTimes(2);
    expect(h.widgetText()).not.toBe(before);
    expect(h.controller.snapshot().shells?.map(job => job.id)).toEqual(shell.jobs.map(job => job.id));
    // A non-monitored, never-spilled job is absent from the collector's widget inputs.
    Object.assign(shell.jobs[0]!, { monitor: undefined, spilledAt: undefined });
    shell.emit("monitor");
    await vi.advanceTimersByTimeAsync(60_000);
    expect(buildSnapshot).toHaveBeenCalledTimes(3);
    expect(h.widgetText()).not.toContain("Second monitor");
    const completed = shell.jobs[1]!;
    Object.assign(completed, { status: "exited", finishedAt: Date.now(), exitCode: 0 });
    shell.emit("finished", completed);
    h.controller.setHostStreaming(false); // Mount the real completion before its 30s boundary.
    expect(h.widgetText()).toContain("Build monitor");
    const counts = [collect.mock.calls.length, buildSnapshot.mock.calls.length];
    await vi.advanceTimersByTimeAsync(30_000);
    expect(h.widgetText()).toContain("Build monitor"); // No independent retention timer.
    const stat = vi.spyOn(fs, "statSync");
    const read = vi.spyOn(fs, "readFileSync");
    const watch = vi.spyOn(fs, "watch");
    shell.emit("acknowledged", completed); // Actual receiver's first expiry notification.
    expect(h.widgetText()).not.toContain("Build monitor");
    expect([collect.mock.calls.length, buildSnapshot.mock.calls.length]).toEqual(counts);
    for (const io of [stat, read, watch]) expect(io).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(1); // Original dirty data deadline survives expiry paint.
    await vi.advanceTimersByTimeAsync(30_000);
    expect(h.widgetText()).not.toContain("Build monitor");
    expect(buildSnapshot).toHaveBeenCalledTimes(counts[1]!);
    expect(collect).toHaveBeenCalledTimes(counts[0]! + 1);
    expect(vi.getTimerCount()).toBe(0);
  });
});
