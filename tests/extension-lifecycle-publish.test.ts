import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import piFabric from "../src/index.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { FabricState } from "../src/fabric-state.js";
import { MeshBackgroundQueue } from "../src/core/atomic-write.js";
import { diagnostics } from "../src/core/diagnostics.js";

type Handler = (event: unknown, context: ExtensionContext) => unknown;

afterEach(() => {
  vi.restoreAllMocks();
  vi.useRealTimers();
});

// Incident: Pi's serial input preflight waited forever behind a stalled mesh
// publish. Exercise the registered hooks with that exact failure, not just a helper.
const fixture = async () => {
  const handlers = new Map<string, Handler[]>();
  const pi = {
    events: { emit: vi.fn(), on: vi.fn(() => () => {}) },
    getActiveTools: () => [], getAllTools: () => [],
    registerCommand: vi.fn(), registerMessageRenderer: vi.fn(), registerTool: vi.fn(), setActiveTools: vi.fn(),
    on: (name: string, handler: Handler) => handlers.set(name, [...(handlers.get(name) ?? []), handler]),
  } as unknown as ExtensionAPI;
  const context = {
    cwd: process.cwd(), hasUI: false, isIdle: () => false, hasPendingMessages: () => false,
    getContextUsage: () => undefined,
    sessionManager: { getSessionId: () => "lifecycle-test", getSessionFile: () => undefined,
      getEntries: () => [], getBranch: () => [], getLeafId: () => null },
    ui: { notify: vi.fn(), setStatus: vi.fn() },
  } as unknown as ExtensionContext;
  vi.spyOn(FabricState.prototype, "initialized", "get").mockReturnValue(true);
  vi.spyOn(FabricState.prototype, "config", "get").mockReturnValue(DEFAULT_FABRIC_CONFIG);
  vi.spyOn(FabricState.prototype, "provisionalConfig").mockReturnValue(DEFAULT_FABRIC_CONFIG);
  vi.spyOn(FabricState.prototype, "compact", "get").mockReturnValue({ maybeCommit: async () => {} } as unknown as FabricState["compact"]);
  vi.spyOn(FabricState.prototype, "nextRootInbox").mockResolvedValue(undefined);
  vi.spyOn(FabricState.prototype, "nextRecordsInboxMessage").mockResolvedValue(undefined);
  const publish = vi.spyOn(FabricState.prototype, "publishHostLifecycle").mockResolvedValue(undefined);
  const warn = vi.spyOn(diagnostics, "warn").mockImplementation(() => {});
  await piFabric(pi);
  const emit = async (name: string, event: unknown = {}) => {
    for (const handler of handlers.get(name) ?? []) await handler(event, context);
  };
  // Load the optional compaction-owner observer before measuring a mesh wait.
  await emit("session_compact");
  publish.mockClear();
  warn.mockClear();
  return { handlers, context, emit, publish, warn };
};

const ordinaryEvents = [
  ["input", "pi.input", { text: "start the turn", source: "extension" }],
  ["agent_start", "pi.agent_start", {}],
  ["turn_end", "pi.turn_end", {}],
  ["tool_execution_end", "pi.tool_error", { toolName: "bash", isError: true, result: [] }],
  ["session_compact", "pi.session_compact", {}],
] as const;

describe("host lifecycle publication cannot stall Pi", () => {
  it.each(ordinaryEvents)("returns from %s even when publication never settles", async (hook, lifecycle, event) => {
    const h = await fixture();
    h.publish.mockReturnValue(new Promise<void>(() => {}));
    // A separate real event-loop boundary bounds the regression check without
    // allowing a hung hook to hang the test itself.
    const result = await Promise.race([
      h.emit(hook, event).then(() => "returned"),
      new Promise<string>(resolve => setImmediate(() => resolve("blocked"))),
    ]);
    expect(result).toBe("returned");
    expect(h.publish).toHaveBeenCalledWith(lifecycle, event);
    expect(h.warn).not.toHaveBeenCalled();
    if (hook === "input") {
      // Also guard the earlier Escape-latch input handler: neither handler
      // needs to be async or expose the publication promise to Pi.
      for (const handler of h.handlers.get("input") ?? []) {
        expect(handler(event, h.context)).toBeUndefined();
      }
    }
  });

  it.each(["agent_end", "agent_settled"])("bounds a hung %s flush to five seconds", async hook => {
    const h = await fixture();
    h.publish.mockReturnValue(new Promise<void>(() => {}));
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    let returned = false;
    const pending = h.emit(hook, { outcome: "completed" }).then(() => { returned = true; });
    await vi.advanceTimersByTimeAsync(0);
    expect(h.publish).toHaveBeenCalledWith(`pi.${hook}`, { outcome: "completed" });
    await vi.advanceTimersByTimeAsync(4_999);
    expect(returned).toBe(false);
    await vi.advanceTimersByTimeAsync(1);
    expect(returned).toBe(true);
    await pending;
    expect(h.warn).toHaveBeenCalledExactlyOnceWith("src/extension.ts",
      `[pi-fabric] host lifecycle publish failed: pi.${hook} publication timed out after 5000 ms`);
    expect(vi.getTimerCount()).toBe(0);
  });

  it.each(["agent_end", "agent_settled"])("waits for a normal %s flush and cancels its deadline", async hook => {
    const h = await fixture();
    let release!: () => void;
    h.publish.mockReturnValue(new Promise<void>(resolve => { release = resolve; }));
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    let returned = false;
    const pending = h.emit(hook).then(() => { returned = true; });
    await vi.advanceTimersByTimeAsync(0);
    expect(returned).toBe(false);
    expect(vi.getTimerCount()).toBe(1);
    release();
    await pending;
    expect(returned).toBe(true);
    expect(vi.getTimerCount()).toBe(0);
    expect(h.warn).not.toHaveBeenCalled();
  });

  it("handles rejected background and flush publications with one diagnostic", async () => {
    const h = await fixture();
    h.publish.mockRejectedValue(new Error("mesh unavailable"));
    for (const [hook, , event] of ordinaryEvents) await h.emit(hook, event);
    await h.emit("agent_end");
    await h.emit("agent_settled");
    expect(h.warn).toHaveBeenCalledExactlyOnceWith("src/extension.ts",
      "[pi-fabric] host lifecycle publish failed: mesh unavailable");
  });

  it("preserves serial queue order when the blocked write eventually finishes", async () => {
    const h = await fixture();
    const queue = new MeshBackgroundQueue("test lifecycle");
    const published: string[] = [];
    let release!: () => void;
    const blocked = new Promise<void>(resolve => { release = resolve; });
    void queue.enqueue(() => blocked);
    h.publish.mockImplementation((event) => queue.enqueue(async () => { published.push(event); }));
    for (const [hook, , event] of ordinaryEvents) await h.emit(hook, event);
    await h.emit("agent_end");
    await h.emit("agent_settled");
    expect(published).toEqual([]);
    release();
    await queue.checkpointForRelease();
    await queue.close();
    expect(published).toEqual([...ordinaryEvents.map(([, lifecycle]) => lifecycle), "pi.agent_end", "pi.agent_settled"]);
    expect(h.warn).not.toHaveBeenCalled();
  });
});
