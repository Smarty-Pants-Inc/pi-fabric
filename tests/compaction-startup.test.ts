import type {
  ExtensionAPI, ExtensionContext, SessionBeforeCompactEvent, SessionBeforeTreeEvent, SessionEntry,
} from "@earendil-works/pi-coding-agent";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { CompactionHookOptions } from "../src/compaction/hook.js";

const loaded = vi.hoisted(() => vi.fn());
beforeEach(() => {
  vi.resetModules();
  loaded.mockClear();
  // Reinstall the factory: resetModules alone retains Vitest's evaluated mock cache.
  vi.doMock("../src/compaction/hook.js", async () => {
    loaded();
    return vi.importActual<typeof import("../src/compaction/hook.js")>("../src/compaction/hook.js");
  });
});

const entries: SessionEntry[] = [{
  type: "message", id: "user-1", parentId: null, timestamp: "2025-01-01T00:00:00Z",
  message: { role: "user", content: "Implement the startup boundary", timestamp: 1 },
}];
const compactEvent = (extra = {}): SessionBeforeCompactEvent => ({
  type: "session_before_compact", branchEntries: entries,
  preparation: { tokensBefore: 1000 }, ...extra,
}) as SessionBeforeCompactEvent;
const treeEvent = (extra = {}): SessionBeforeTreeEvent => ({
  type: "session_before_tree", preparation: {
    userWantsSummary: true, entriesToSummarize: entries, oldLeafId: "user-1", ...extra,
  },
}) as SessionBeforeTreeEvent;
const harness = async (options: CompactionHookOptions = { getEngine: () => "fabric" }) => {
  type Handler = (event: unknown, context: ExtensionContext) => unknown;
  const handlers = new Map<string, Handler>();
  const pi = { on: (name: string, handler: Handler) => handlers.set(name, handler) } as unknown as ExtensionAPI;
  const { registerLazyCompactionHook } = await import("../src/compaction/lazy-hook.js");
  registerLazyCompactionHook(pi, options);
  const context = { hasUI: true, ui: { notify: vi.fn() }, model: { provider: "test", id: "model", contextWindow: 100_000 } } as unknown as ExtensionContext;
  return { context, handlers, emit: (name: string, event: unknown) => handlers.get(name)!(event, context) };
};

describe("lazy compaction registration", () => {
  it("registers both hooks without loading the engine, including after an idle turn", async () => {
    const h = await harness();
    expect([...h.handlers.keys()]).toEqual(["session_before_compact", "session_before_tree"]);
    await new Promise<void>(resolve => setImmediate(resolve));
    expect(loaded).not.toHaveBeenCalled();
  });

  it("does not load for the pi-vcc sentinel or trees that do not need Fabric summaries", async () => {
    const h = await harness();
    expect(await h.emit("session_before_compact", compactEvent({ customInstructions: "__pi_vcc__" }))).toBeUndefined();
    expect(await h.emit("session_before_tree", treeEvent({ userWantsSummary: false }))).toBeUndefined();
    expect(await h.emit("session_before_tree", treeEvent({ replaceInstructions: true }))).toBeUndefined();
    const pi = await harness({ getEngine: () => "pi" });
    expect(await pi.emit("session_before_tree", treeEvent())).toBeUndefined();
    expect(loaded).not.toHaveBeenCalled();
  });

  it("loads once on first compaction and preserves the summary and mutable interop marker", async () => {
    const h = await harness();
    const event = compactEvent();
    const result = await h.emit("session_before_compact", event);
    expect(result).toHaveProperty("compaction");
    expect(event).toHaveProperty("_fabricCompaction", true);
    const { handleFabricBeforeCompact } = await import("../src/compaction/hook.js");
    expect(result).toEqual(handleFabricBeforeCompact(compactEvent(), h.context, { getEngine: () => "fabric" }));
    expect(await h.emit("session_before_compact", compactEvent())).toEqual(result);
    expect(loaded).toHaveBeenCalledTimes(1);
  });

  it("loads on tree-first use and returns the identical deterministic branch summary", async () => {
    const h = await harness();
    const result = await h.emit("session_before_tree", treeEvent());
    expect(result).toHaveProperty("summary");
    const { handleFabricBeforeTree } = await import("../src/compaction/hook.js");
    expect(result).toEqual(handleFabricBeforeTree(treeEvent(), h.context, { getEngine: () => "fabric" }));
    await h.emit("session_before_compact", compactEvent());
    expect(loaded).toHaveBeenCalledTimes(1);
  });

  it("preserves threshold cancellation even when Pi is the selected engine", async () => {
    const h = await harness({ getEngine: () => "pi", getThresholdTokens: () => 2000 });
    expect(await h.emit("session_before_compact", compactEvent({ reason: "threshold" }))).toEqual({ cancel: true });
    expect(await h.emit("session_before_compact", compactEvent({ reason: "manual" }))).toBeUndefined();
    expect(loaded).toHaveBeenCalledTimes(1);
  });

  it("preserves instruction rejection notifications and pi-vcc fallback", async () => {
    const h = await harness();
    const invalid = await h.emit("session_before_compact", compactEvent({ customInstructions: "__pi_fabric_compact_request_v1__:not-json" }));
    expect(invalid).toEqual({ cancel: true });
    expect(h.context.ui.notify).toHaveBeenCalledWith(expect.stringContaining("Fabric compaction rejected"), "error");
    expect(await h.emit("session_before_compact", compactEvent({ branchEntries: [], _piVccOverriding: true }))).toBeUndefined();
    expect(await h.emit("session_before_compact", compactEvent({ branchEntries: [] }))).toEqual({ cancel: true });
  });
});
