import { afterEach, describe, expect, it, vi } from "vitest";
import type { CompactOptions, ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { COMPACTION_FAILED_ALARM, registerCompactionRecovery } from "../src/compaction/recovery.js";

afterEach(() => vi.useRealTimers());
type Handler = (event: Record<string, unknown>, context: ExtensionContext) => unknown;
const harness = (idle = true) => {
  vi.useFakeTimers();
  const handlers = new Map<string, Handler>();
  const alarm = vi.fn(async (_data: Record<string, unknown>) => {});
  const retryEnded = vi.fn();
  let isIdle = idle; let session = "Main-A"; let enabled = true;
  const controller = new AbortController();
  const compact = vi.fn((_options?: CompactOptions) => {});
  const context = { isIdle: () => isIdle, signal: controller.signal, compact,
    sessionManager: { getSessionId: () => session }, getContextUsage: () => ({ tokens: 259000, contextWindow: 272000, percent: 95.22 }) } as unknown as ExtensionContext;
  registerCompactionRecovery({ on: (name: string, fn: Handler) => handlers.set(name, fn) } as unknown as ExtensionAPI,
    { enabled: () => enabled, alarm, retryEnded });
  const emit = async (event: string, value = {}, ctx = context) => handlers.get(event)?.({ type: event, ...value }, ctx);
  const failure = (extra = {}, ctx = context) => emit("session_compact_failed", { reason: "threshold", aborted: false, willRetry: false,
    fromExtension: false, errorMessage: "Auto-compaction failed: stream disconnected", ...extra }, ctx);
  return { alarm, compact, retryEnded, controller, context, emit, failure, idle: (value: boolean) => { isIdle = value; },
    session: (value: string) => { session = value; }, enabled: (value: boolean) => { enabled = value; } };
};

describe("Fabric-hosted compaction recovery", () => {
  // smarty-dev#17478: native Pi already exhausted summary retry/fallback; another full compact hides failure.
  it.each(["Main-A", "task-A"])("%s alarms immediately while busy, without a second summary or idle signal", async session => {
    const h = harness(false); h.session(session);
    h.context.isIdle = vi.fn(() => false);
    await h.emit("session_before_compact", { customInstructions: "preserve task handoff" });
    const reported = h.failure();
    expect(h.alarm).toHaveBeenCalledOnce(); // no timer, idle edge or second event needed
    expect(h.alarm).toHaveBeenCalledWith({ session, windowPercent: 95.22, reason: "threshold",
      error: "Auto-compaction failed: stream disconnected", fabricAttempts: 0 }, h.context);
    await reported;
    await vi.advanceTimersByTimeAsync(60_000);
    h.idle(true); await h.emit("agent_settled", { outcome: "error" });
    await vi.runOnlyPendingTimersAsync();
    expect(COMPACTION_FAILED_ALARM).toBe("fabric.session.compaction_failed");
    expect(h.compact).not.toHaveBeenCalled(); expect(h.context.isIdle).not.toHaveBeenCalled();
    expect(h.retryEnded).not.toHaveBeenCalled(); expect(h.alarm).toHaveBeenCalledOnce();
    expect(vi.getTimerCount()).toBe(0);
  });

  it.each(["manual", "threshold", "overflow"])("reports terminal %s failures, including extension content", async reason => {
    const h = harness(false);
    await h.failure({ reason, fromExtension: true });
    expect(h.alarm).toHaveBeenCalledWith(expect.objectContaining({ reason, fabricAttempts: 0 }), h.context);
    await vi.runOnlyPendingTimersAsync();
    expect(h.compact).not.toHaveBeenCalled(); expect(h.retryEnded).not.toHaveBeenCalled();
  });

  it.each(["session_compact", "session_start"])("%s resets dedupe for a later independent failure", async event => {
    const h = harness(); await h.failure(); await h.failure();
    expect(h.alarm).toHaveBeenCalledOnce();
    await h.emit(event); await h.failure();
    expect(h.alarm).toHaveBeenCalledTimes(2);
    expect(h.compact).not.toHaveBeenCalled(); expect(h.retryEnded).not.toHaveBeenCalled();
  });

  // smarty-dev#17478/#9271: alarm publication is not a retry completion or an idle edge.
  it("deduplicates concurrent failures before publication finishes without signaling recovery", async () => {
    const h = harness(false);
    let finish!: () => void;
    h.alarm.mockImplementationOnce(() => new Promise<void>(resolve => { finish = resolve; }));
    const pending = h.failure();
    await h.failure({ errorMessage: "another failure" });
    expect(h.alarm).toHaveBeenCalledOnce();
    expect(h.retryEnded).not.toHaveBeenCalled();
    finish(); await pending; await h.failure();
    await vi.runOnlyPendingTimersAsync();
    expect(h.alarm).toHaveBeenCalledOnce(); expect(h.retryEnded).not.toHaveBeenCalled();
    expect(h.compact).not.toHaveBeenCalled(); expect(vi.getTimerCount()).toBe(0);
  });

  it.each([{ aborted: true }, { errorMessage: "Compaction failed: Already compacted" },
    { errorMessage: "Nothing to compact (session too small)" }, { errorMessage: "Compaction cancelled" },
    { errorMessage: "Auto-compaction failed: Compaction cancelled" },
    { errorMessage: "Context overflow recovery failed: Already compacted" }])("ignores owner cancellation, extension veto or benign no-op: %j", async extra => {
    const h = harness(); await h.failure(extra); await vi.runOnlyPendingTimersAsync();
    expect(h.compact).not.toHaveBeenCalled(); expect(h.alarm).not.toHaveBeenCalled();
    expect(h.retryEnded).not.toHaveBeenCalled(); expect(vi.getTimerCount()).toBe(0);
    await h.failure(); expect(h.alarm).toHaveBeenCalledOnce();
  });

  it("does nothing while disabled, then reports a fresh enabled failure", async () => {
    const h = harness(); h.enabled(false); await h.failure();
    expect(h.alarm).not.toHaveBeenCalled();
    h.enabled(true); await h.failure();
    expect(h.alarm).toHaveBeenCalledOnce(); expect(h.compact).not.toHaveBeenCalled();
    expect(h.retryEnded).not.toHaveBeenCalled(); expect(vi.getTimerCount()).toBe(0);
  });

  it("ignores failures after the owner's abort signal", async () => {
    const h = harness(); h.controller.abort(); await h.failure();
    await h.emit("agent_settled"); await vi.runOnlyPendingTimersAsync();
    expect(h.compact).not.toHaveBeenCalled(); expect(h.alarm).not.toHaveBeenCalled();
    expect(h.retryEnded).not.toHaveBeenCalled(); expect(vi.getTimerCount()).toBe(0);
  });

  it("keeps an existing incident deduplicated across benign or disabled notifications", async () => {
    const h = harness(); await h.failure();
    await h.failure({ aborted: true }); await h.failure({ errorMessage: "Compaction cancelled" });
    h.enabled(false); await h.failure(); h.enabled(true); await h.failure();
    expect(h.alarm).toHaveBeenCalledOnce(); expect(h.compact).not.toHaveBeenCalled();
    expect(h.retryEnded).not.toHaveBeenCalled();
  });

  it("reports null usage and a missing error without invoking public compact", async () => {
    const h = harness(); h.context.getContextUsage = () => undefined;
    h.compact.mockImplementation(() => { throw new Error("must not dispatch another summary"); });
    await h.failure({ errorMessage: undefined });
    expect(h.alarm).toHaveBeenCalledWith(expect.objectContaining({ windowPercent: null,
      error: "Compaction failed", fabricAttempts: 0 }), h.context);
    await h.emit("agent_settled"); await vi.runOnlyPendingTimersAsync();
    expect(h.compact).not.toHaveBeenCalled(); expect(h.retryEnded).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
  });

  it.each(["throw", "reject"])("contains a publication %s without retrying or signaling idle", async mode => {
    const h = harness();
    h.alarm.mockImplementationOnce(() => {
      if (mode === "throw") throw new Error("mesh unavailable");
      return Promise.reject(new Error("mesh unavailable"));
    });
    await expect(h.failure()).resolves.toBeUndefined();
    await h.failure(); await h.emit("agent_settled"); await vi.runOnlyPendingTimersAsync();
    expect(h.alarm).toHaveBeenCalledOnce(); expect(h.compact).not.toHaveBeenCalled();
    expect(h.retryEnded).not.toHaveBeenCalled(); expect(vi.getTimerCount()).toBe(0);
    await h.emit("session_compact"); await h.failure();
    expect(h.alarm).toHaveBeenCalledTimes(2);
  });
});

// smarty-dev#8881: AgentSession.dispose() and session replacement invalidate the ctx without a
// session_shutdown; every getter then throws Pi's stale-ctx error. Terminal failure reporting
// must reject stale contexts and schedule no work that could access them after disposal.
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
  const handlers = new Map<string, Handler>();
  const alarm = vi.fn(async (_data: Record<string, unknown>, _ctx: ExtensionContext) => {});
  const retryEnded = vi.fn();
  registerCompactionRecovery({ on: (name: string, fn: Handler) => handlers.set(name, fn) } as unknown as ExtensionAPI,
    { enabled: () => true, alarm, retryEnded });
  const emit = async (event: string, ctx: ExtensionContext, value = {}) => handlers.get(event)?.({ type: event, ...value }, ctx);
  const failure = (ctx: ExtensionContext) => emit("session_compact_failed", ctx, { reason: "threshold", aborted: false,
    willRetry: false, fromExtension: false, errorMessage: "Auto-compaction failed: stream disconnected" });
  return { alarm, retryEnded, emit, failure };
};

describe("compaction recovery after session disposal (smarty-dev#8881)", () => {
  it("ignores a failure on an already disposed ctx without accessing stale getters", async () => {
    const h = lifecycle(); const a = disposable("Main-A");
    a.dispose(); // no session_shutdown: Pi's dispose emits none
    await expect(h.failure(a.context)).resolves.toBeUndefined();
    await vi.runOnlyPendingTimersAsync();
    expect(a.compact).not.toHaveBeenCalled(); expect(h.alarm).not.toHaveBeenCalled();
    expect(h.retryEnded).not.toHaveBeenCalled(); expect(vi.getTimerCount()).toBe(0);
    await h.emit("session_shutdown", a.context); await h.emit("session_shutdown", a.context);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("reports a different session without needing session_start", async () => {
    const h = lifecycle(); const a = disposable("Main-A"); const b = disposable("Main-B");
    await h.failure(a.context); await h.failure(b.context); await h.failure(b.context);
    expect(h.alarm).toHaveBeenCalledTimes(2);
    expect(h.alarm.mock.calls[1]![0]).toMatchObject({ session: "Main-B", fabricAttempts: 0 });
    expect(h.alarm.mock.calls[1]![1]).toBe(b.context);
    expect(b.compact).not.toHaveBeenCalled(); expect(a.compact).not.toHaveBeenCalled();
    expect(h.retryEnded).not.toHaveBeenCalled(); expect(vi.getTimerCount()).toBe(0);
  });

  it("reports a live replacement of a stale incident even with the same session id", async () => {
    const h = lifecycle(); const a = disposable("Main-A"); const b = disposable("Main-A");
    await h.failure(a.context); a.dispose();
    await h.emit("agent_settled", b.context); await h.failure(b.context); await h.failure(a.context);
    await vi.runOnlyPendingTimersAsync();
    expect(h.alarm).toHaveBeenCalledTimes(2); expect(h.alarm.mock.calls[1]![1]).toBe(b.context);
    expect(a.compact).not.toHaveBeenCalled(); expect(b.compact).not.toHaveBeenCalled();
    expect(h.retryEnded).not.toHaveBeenCalled(); expect(vi.getTimerCount()).toBe(0);
  });

  it.each(["resolve", "reject"])("a publication that finishes after disposal (%s) has no late idle effect", async end => {
    const h = lifecycle(); const a = disposable("Main-A"); const b = disposable("Main-A");
    let finish!: () => void; let fail!: (error: Error) => void;
    h.alarm.mockImplementationOnce(() => new Promise<void>((resolve, reject) => { finish = resolve; fail = reject; }));
    const pending = h.failure(a.context); a.dispose();
    await h.failure(b.context); // old publication must not clear the live replacement's dedupe
    if (end === "resolve") finish(); else fail(new Error("ctx disposed during publication"));
    await expect(pending).resolves.toBeUndefined(); await h.failure(b.context);
    await vi.runOnlyPendingTimersAsync();
    expect(h.alarm).toHaveBeenCalledTimes(2); expect(h.retryEnded).not.toHaveBeenCalled();
    expect(a.compact).not.toHaveBeenCalled(); expect(b.compact).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
  });

  it("suppresses late failure after shutdown until a new session starts", async () => {
    const h = lifecycle(); const a = disposable("Main-A"); const b = disposable("Main-B");
    await h.failure(a.context);
    await h.emit("session_shutdown", a.context); a.dispose();
    await h.emit("session_shutdown", a.context);
    await h.failure(a.context); await h.failure(b.context); await h.emit("agent_settled", b.context);
    await h.emit("session_compact", b.context); await h.failure(b.context);
    expect(h.alarm).toHaveBeenCalledOnce();
    await h.emit("session_start", b.context); await h.failure(b.context);
    expect(h.alarm).toHaveBeenCalledTimes(2); expect(h.alarm.mock.calls[1]![1]).toBe(b.context);
    await vi.runOnlyPendingTimersAsync();
    expect(a.compact).not.toHaveBeenCalled(); expect(b.compact).not.toHaveBeenCalled();
    expect(h.retryEnded).not.toHaveBeenCalled(); expect(vi.getTimerCount()).toBe(0);
  });
});
