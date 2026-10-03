import { afterEach, describe, expect, it, vi } from "vitest";
import type { CompactOptions, ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { COMPACTION_FAILED_ALARM, registerCompactionRecovery } from "../src/compaction/recovery.js";

afterEach(() => vi.useRealTimers());
const harness = (idle = true) => {
  vi.useFakeTimers();
  const handlers = new Map<string, (event: any, ctx: ExtensionContext) => unknown>();
  const alarm = vi.fn(async (_data: Record<string, unknown>) => {});
  let isIdle = idle; let session = "Main-A";
  const controller = new AbortController();
  const compact = vi.fn((_options?: CompactOptions) => {});
  const context = { isIdle: () => isIdle, signal: controller.signal, compact,
    sessionManager: { getSessionId: () => session }, getContextUsage: () => ({ tokens: 259000, contextWindow: 272000, percent: 95.22 }) } as unknown as ExtensionContext;
  registerCompactionRecovery({ on: (name: string, fn: any) => handlers.set(name, fn) } as unknown as ExtensionAPI,
    { enabled: () => true, alarm });
  const emit = async (event: string, value = {}) => handlers.get(event)?.({ type: event, ...value }, context);
  const failure = (extra = {}) => emit("session_compact_failed", { reason: "threshold", aborted: false, willRetry: false,
    fromExtension: false, errorMessage: "Auto-compaction failed: stream disconnected", ...extra });
  return { alarm, compact, controller, context, emit, failure, idle: (value: boolean) => { isIdle = value; }, session: (value: string) => { session = value; } };
};

describe("Fabric-hosted compaction recovery", () => {
  it.each(["Main-A", "task-A"])("%s retries once and emits one named alarm on second failure", async session => {
    const h = harness(); h.session(session);
    await h.emit("session_before_compact", { customInstructions: "preserve task handoff" }); await h.failure();
    expect(h.compact).not.toHaveBeenCalled(); expect(h.alarm).not.toHaveBeenCalled();
    await vi.runOnlyPendingTimersAsync(); expect(h.compact).toHaveBeenCalledOnce();
    expect(h.compact.mock.calls[0]![0]?.customInstructions).toBe("preserve task handoff");
    await h.failure({ reason: "manual", errorMessage: "Compaction failed: stream disconnected again" });
    h.compact.mock.calls[0]![0]?.onError?.(new Error("stream disconnected again"));
    await h.failure(); await vi.runOnlyPendingTimersAsync();
    expect(h.compact).toHaveBeenCalledOnce(); expect(h.alarm).toHaveBeenCalledOnce();
    expect(COMPACTION_FAILED_ALARM).toBe("fabric.session.compaction_failed");
    expect(h.alarm.mock.calls[0]![0]).toMatchObject({ session, windowPercent: 95.22, attempts: 2,
      reason: "threshold", error: "Compaction failed: stream disconnected again" });
  });

  it("defers an automatic failure until settled/idle without aborting the active run", async () => {
    const h = harness(false); await h.failure(); await vi.runOnlyPendingTimersAsync();
    expect(h.compact).not.toHaveBeenCalled(); h.idle(true);
    await h.emit("agent_settled", { outcome: "error" }); await vi.runOnlyPendingTimersAsync();
    expect(h.compact).toHaveBeenCalledOnce(); expect(h.alarm).not.toHaveBeenCalled();
  });

  it("successful retry clears the incident and allows one retry for a later independent failure", async () => {
    const h = harness(); await h.failure(); await vi.runOnlyPendingTimersAsync();
    await h.emit("session_compact"); h.compact.mock.calls[0]![0]?.onComplete?.({} as never);
    expect(h.alarm).not.toHaveBeenCalled(); await h.failure(); await vi.runOnlyPendingTimersAsync();
    expect(h.compact).toHaveBeenCalledTimes(2);
  });

  it.each([{ aborted: true }, { errorMessage: "Compaction failed: Already compacted" },
    { errorMessage: "Nothing to compact (session too small)" }, { errorMessage: "Compaction cancelled" }])("does not retry/alarm owner cancellation, extension veto or benign no-op: %j", async extra => {
    const h = harness(); await h.failure(extra); await vi.runOnlyPendingTimersAsync();
    expect(h.compact).not.toHaveBeenCalled(); expect(h.alarm).not.toHaveBeenCalled();
  });

  it.each(["session_shutdown", "signal", "replace"])("cancels deferred work on %s", async reason => {
    const h = harness(); await h.failure();
    if (reason === "signal") h.controller.abort(); else if (reason === "replace") h.session("replacement"); else await h.emit(reason);
    await vi.runOnlyPendingTimersAsync(); expect(h.compact).not.toHaveBeenCalled(); expect(h.alarm).not.toHaveBeenCalled();
  });

  it("alarms synchronous public retry exceptions and null usage without looping", async () => {
    const h = harness(); h.context.getContextUsage = () => undefined;
    h.compact.mockImplementation(() => { throw new Error("retry dispatch failed"); });
    await h.failure(); await vi.runOnlyPendingTimersAsync();
    expect(h.alarm).toHaveBeenCalledWith(expect.objectContaining({ windowPercent: null, error: "retry dispatch failed" }), h.context);
    await h.emit("agent_settled"); await vi.runOnlyPendingTimersAsync(); expect(h.compact).toHaveBeenCalledOnce();
  });
});
