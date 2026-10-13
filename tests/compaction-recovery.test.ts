import { afterEach, describe, expect, it, vi } from "vitest";
import type { CompactOptions, ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { COMPACTION_FAILED_ALARM, registerCompactionRecovery } from "../src/compaction/recovery.js";

afterEach(() => vi.useRealTimers());
const harness = (idle = true, retryEnded?: () => void) => {
  vi.useFakeTimers();
  const handlers = new Map<string, (event: any, ctx: ExtensionContext) => unknown>();
  const alarm = vi.fn(async (_data: Record<string, unknown>) => {});
  let isIdle = idle; let session = "Main-A";
  const controller = new AbortController();
  const compact = vi.fn((_options?: CompactOptions) => {});
  const context = { isIdle: () => isIdle, signal: controller.signal, compact,
    sessionManager: { getSessionId: () => session }, getContextUsage: () => ({ tokens: 259000, contextWindow: 272000, percent: 95.22 }) } as unknown as ExtensionContext;
  registerCompactionRecovery({ on: (name: string, fn: any) => handlers.set(name, fn) } as unknown as ExtensionAPI,
    { enabled: () => true, alarm, ...(retryEnded ? { retryEnded } : {}) });
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

  // smarty-dev#9271: no agent_settled follows a retry on an idle Main; this callback wakes it for a
  // child completion that landed during the retry. Pi calls onComplete/onError after its compaction state is gone.
  it.each(["onComplete", "onError"] as const)("signals the end of its retry from Pi's %s callback", async end => {
    const retryEnded = vi.fn();
    const h = harness(true, retryEnded); await h.failure(); await vi.runOnlyPendingTimersAsync();
    expect(h.compact).toHaveBeenCalledOnce(); expect(retryEnded).not.toHaveBeenCalled();
    const options = h.compact.mock.calls[0]![0]!;
    if (end === "onComplete") options.onComplete?.({} as never); else options.onError?.(new Error("stream disconnected again"));
    expect(retryEnded).toHaveBeenCalledOnce();
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

// smarty-dev#8881: AgentSession.dispose() and session replacement invalidate the ctx without a
// session_shutdown; every getter then throws Pi's stale-ctx error. The zero-delay retry timer read
// ctx.sessionManager unguarded, so a dispose inside that window threw an uncaught exception.
const STALE = "This extension ctx is stale after session replacement or reload.";
const disposable = (id: string) => {
  let stale = false;
  const compact = vi.fn((_options?: CompactOptions) => {});
  const owner = new AbortController();
  const target = { get hasUI() { return false; }, get signal() { return owner.signal; }, isIdle: () => true, compact,
    sessionManager: { getSessionId: () => id }, getContextUsage: () => ({ tokens: 1, contextWindow: 2, percent: 50 }) };
  const context = new Proxy(target, { get(object, key, receiver) {
    if (stale) throw new Error(STALE);
    return Reflect.get(object, key, receiver);
  } }) as unknown as ExtensionContext;
  return { context, compact, dispose: () => { stale = true; } };
};
const lifecycle = () => {
  vi.useFakeTimers();
  const handlers = new Map<string, (event: any, ctx: ExtensionContext) => unknown>();
  const alarm = vi.fn(async (_data: Record<string, unknown>, _ctx: ExtensionContext) => {});
  const retryEnded = vi.fn();
  registerCompactionRecovery({ on: (name: string, fn: any) => handlers.set(name, fn) } as unknown as ExtensionAPI,
    { enabled: () => true, alarm, retryEnded });
  const emit = async (event: string, ctx: ExtensionContext, value = {}) => handlers.get(event)?.({ type: event, ...value }, ctx);
  const failure = (ctx: ExtensionContext) => emit("session_compact_failed", ctx, { reason: "threshold", aborted: false,
    willRetry: false, fromExtension: false, errorMessage: "Auto-compaction failed: stream disconnected" });
  return { alarm, retryEnded, emit, failure };
};

describe("compaction recovery after session disposal (smarty-dev#8881)", () => {
  it("a dispose inside the zero-delay retry window retires the timer without touching the stale ctx", async () => {
    const h = lifecycle(); const a = disposable("Main-A");
    await h.failure(a.context); expect(vi.getTimerCount()).toBe(1);
    a.dispose(); // no session_shutdown: Pi's dispose emits none
    await expect(vi.runOnlyPendingTimersAsync()).resolves.toBe(vi); // a throwing timer rejects here
    expect(a.compact).not.toHaveBeenCalled(); expect(h.alarm).not.toHaveBeenCalled(); expect(h.retryEnded).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
    // Teardown stays idempotent: a late shutdown after the quiet retirement does nothing.
    await h.emit("session_shutdown", a.context); await h.emit("session_shutdown", a.context);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("a replacement's live ctx still retries after the stale incident retired", async () => {
    const h = lifecycle(); const a = disposable("Main-A"); const b = disposable("Main-B");
    await h.failure(a.context); a.dispose();
    await vi.runOnlyPendingTimersAsync();
    await h.failure(b.context); await vi.runOnlyPendingTimersAsync(); // no session_start: same instance, live ctx
    expect(b.compact).toHaveBeenCalledOnce(); expect(a.compact).not.toHaveBeenCalled();
    await h.emit("session_start", b.context); await h.failure(b.context); await vi.runOnlyPendingTimersAsync();
    expect(b.compact).toHaveBeenCalledTimes(2); expect(h.alarm).not.toHaveBeenCalled();
  });

  it("an incident whose ctx goes stale before agent_settled is replaced by the live ctx", async () => {
    const h = lifecycle(); const a = disposable("Main-A"); const b = disposable("Main-A");
    await h.failure(a.context); a.dispose();
    await h.emit("agent_settled", b.context); await vi.runOnlyPendingTimersAsync();
    expect(a.compact).not.toHaveBeenCalled();
    await h.failure(b.context); await vi.runOnlyPendingTimersAsync();
    expect(b.compact).toHaveBeenCalledOnce();
  });

  it.each(["onComplete", "onError"] as const)("a retry whose session is disposed before %s fires has no late effect", async end => {
    const h = lifecycle(); const a = disposable("Main-A");
    await h.failure(a.context); await vi.runOnlyPendingTimersAsync();
    expect(a.compact).toHaveBeenCalledOnce();
    const options = a.compact.mock.calls[0]![0]!; a.dispose();
    if (end === "onComplete") options.onComplete?.({} as never); else options.onError?.(new Error("stream disconnected again"));
    await vi.runOnlyPendingTimersAsync();
    expect(h.alarm).not.toHaveBeenCalled(); expect(h.retryEnded).not.toHaveBeenCalled();
    const b = disposable("Main-B"); await h.failure(b.context); await vi.runOnlyPendingTimersAsync();
    expect(b.compact).toHaveBeenCalledOnce(); // the stale incident did not wedge recovery
  });

  it("a retry failure reported while the session is live still alarms and ends once", async () => {
    const h = lifecycle(); const a = disposable("Main-A");
    await h.failure(a.context); await vi.runOnlyPendingTimersAsync();
    a.compact.mock.calls[0]![0]!.onError?.(new Error("again"));
    await vi.runOnlyPendingTimersAsync();
    expect(h.alarm).toHaveBeenCalledOnce(); expect(h.alarm.mock.calls[0]![1]).toBe(a.context); expect(h.retryEnded).toHaveBeenCalledOnce();
  });
});
