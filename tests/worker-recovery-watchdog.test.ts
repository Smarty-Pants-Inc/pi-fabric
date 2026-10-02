import { afterEach, describe, expect, it, vi } from "vitest";
import { PI_RECOVERY_MAX_MS, PI_RECOVERY_TIMEOUT_MS, PI_PROVIDER_RESUME_DELAYS_MS, PiRecoveryWatchdog } from "../src/worker/recovery-watchdog.js";
import { ToolCallStreamGuard } from "../src/worker/tool-call-stream-guard.js";
import { assistantStreamEvent } from "../src/worker/assistant-stream-event.js";

afterEach(() => vi.useRealTimers());

const setup = () => {
  vi.useFakeTimers();
  const fail = vi.fn();
  return { fail, watchdog: new PiRecoveryWatchdog(fail) };
};

describe("assistantStreamEvent", () => {
  it("normalizes both envelopes without changing whitespace bytes", () => {
    const stream = { type: "toolcall_delta", contentIndex: 0, delta: " \t\n" };
    for (const field of ["assistantMessageEvent", "event"]) {
      expect(assistantStreamEvent({ type: "message_update", [field]: stream })).toBe(stream);
    }
  });

  it("prefers a valid legacy envelope and falls back to native for malformed legacy values", () => {
    const legacy = { type: "text_delta", delta: "legacy" };
    const native = { type: "text_delta", delta: "native" };
    expect(assistantStreamEvent({ type: "message_update", assistantMessageEvent: legacy, event: native })).toBe(legacy);
    for (const malformed of [undefined, null, "invalid", [], false]) {
      expect(assistantStreamEvent({ type: "message_update", assistantMessageEvent: malformed, event: native })).toBe(native);
      expect(assistantStreamEvent({ type: "message_update", event: malformed })).toBeUndefined();
    }
    expect(assistantStreamEvent({ type: "message_start", event: native })).toBeUndefined();
  });
});

describe("PiRecoveryWatchdog", () => {
  it("honours the native announced delay plus slack, without counting chatter as progress", () => {
    const { fail, watchdog } = setup();
    watchdog.arm("503 server_is_overloaded");
    watchdog.observe({ type: "auto_retry_start", attempt: 5, maxAttempts: 6, delayMs: 80_000, errorMessage: "503 server_is_overloaded" });
    vi.advanceTimersByTime(90_000);
    expect(fail).not.toHaveBeenCalled();
    watchdog.observe({ type: "agent_start" });
    vi.advanceTimersByTime(49_999);
    expect(fail).not.toHaveBeenCalled();
    vi.advanceTimersByTime(1);
    expect(fail).toHaveBeenCalledOnce();
  });

  it("bounds repeated native retry delays and resumed attempts at ten minutes", () => {
    const { fail, watchdog } = setup();
    expect(PI_RECOVERY_MAX_MS).toBe(600_000);
    expect(PI_PROVIDER_RESUME_DELAYS_MS).toEqual([30_000, 60_000, 120_000]);
    watchdog.arm("overloaded");
    for (let i = 0; i < 5; i++) {
      watchdog.observe({ type: "auto_retry_start", delayMs: 160_000 });
      vi.advanceTimersByTime(100_000);
      watchdog.observe({ type: "message_start", message: { role: "assistant", content: [] } });
      watchdog.arm("503 overloaded");
    }
    watchdog.observe({ type: "auto_retry_start", delayMs: 160_000 });
    vi.advanceTimersByTime(99_999);
    expect(fail).not.toHaveBeenCalled();
    vi.advanceTimersByTime(1);
    expect(fail.mock.calls[0]?.[0]).toContain("10-minute bound (600000ms)");
  });

  it.each([-1, NaN, Infinity, "80000", undefined])("ignores malformed retry delay %s", delayMs => {
    const { fail, watchdog } = setup();
    watchdog.arm("overloaded");
    watchdog.observe({ type: "auto_retry_start", delayMs });
    vi.advanceTimersByTime(60_000);
    expect(fail).toHaveBeenCalledOnce();
  });

  it("keeps the absolute incident cap through accepted response starts and process restarts", () => {
    const { fail, watchdog } = setup();
    watchdog.arm("503 server_is_overloaded");
    watchdog.observe({ type: "message_start", message: { role: "assistant", content: [] } });
    vi.advanceTimersByTime(PI_RECOVERY_MAX_MS - 1);
    expect(fail).not.toHaveBeenCalled();
    vi.advanceTimersByTime(1);
    expect(fail.mock.calls[0]?.[0]).toContain("503 server_is_overloaded; Pi provider recovery exceeded the 10-minute bound");
  });

  it("clears the incident budget only when recovery completes", () => {
    const { watchdog } = setup();
    watchdog.arm("overloaded");
    watchdog.observe({ type: "auto_retry_start", delayMs: 500_000 });
    vi.advanceTimersByTime(500_000);
    watchdog.suspend();
    expect(watchdog.remainingMs).toBe(100_000);
    watchdog.clear();
    watchdog.arm("new incident");
    expect(watchdog.remainingMs).toBe(600_000);
    watchdog.dispose();
  });
  it.each(["assistantMessageEvent", "event"])("disarms when the retry starts before 70 seconds of silent reasoning (%s)", (field) => {
    const { fail, watchdog } = setup();
    watchdog.arm("cliproxyapi/gpt-6.1-sol: terminated");
    vi.advanceTimersByTime(2_000);
    watchdog.observe({ type: "message_start", message: { role: "assistant", content: [], stopReason: "stop" } });
    expect(vi.getTimerCount()).toBe(1); // Only the absolute incident cap remains.
    vi.advanceTimersByTime(70_000);
    watchdog.observe({ type: "message_update", [field]: { type: "text_delta", delta: "recovered" } });
    expect(fail).not.toHaveBeenCalled();
    watchdog.clear();
    // Acceptance does not dispose the watchdog: a later failure is still bounded.
    watchdog.arm("later independent error");
    vi.advanceTimersByTime(60_000);
    expect(fail).toHaveBeenCalledExactlyOnceWith(
      "later independent error; no model stream or tool event for 60000ms; terminating child",
    );
  });

  it.each(["assistantMessageEvent", "event"])("keeps runaway whitespace bounds after accepted recovery (%s)", (field) => {
    for (const bound of ["time", "bytes"]) {
      const { fail, watchdog } = setup();
      const runaway = vi.fn();
      const guard = new ToolCallStreamGuard(runaway, () => ({ model: "cliproxyapi/gpt-6.1-sol", effort: "high" }));
      watchdog.arm("Error: Terminated");
      const start = { type: "message_start", message: { role: "assistant", content: [] } };
      guard.observe(start);
      watchdog.observe(start);
      const delta = { type: "message_update", [field]: {
        type: "toolcall_delta", contentIndex: 0, delta: bound === "bytes" ? " ".repeat(65_536) : " \t\n",
      } };
      guard.observe(delta);
      watchdog.observe(delta);
      if (bound === "time") vi.advanceTimersByTime(60_000);
      expect(fail).not.toHaveBeenCalled();
      expect(runaway).toHaveBeenCalledTimes(1);
      expect(runaway.mock.calls[0]![0]).toMatchObject(bound === "time"
        ? { elapsedMs: 60_000, bytes: 3 } : { elapsedMs: 0, bytes: 65_536 });
      watchdog.dispose();
      expect(vi.getTimerCount()).toBe(0);
    }
  });
  it.each(["assistantMessageEvent", "event"])("bounds whitespace-only tool arguments every 5s after recovery is armed (%s)", (field) => {
    const { fail, watchdog } = setup();
    const runaway = vi.fn();
    const guard = new ToolCallStreamGuard(runaway, () => ({ model: "cliproxyapi/gpt-6.1-sol", effort: "high" }));
    watchdog.arm("Error: Terminated");
    const observe = () => {
      const event = { type: "message_update", [field]: { type: "toolcall_delta", contentIndex: 0, delta: " \t\n" },
        message: { role: "assistant", content: "old output" } };
      // Same ordering and same unnormalized event as worker.ts.
      guard.observe(event);
      watchdog.observe(event);
    };
    observe();
    for (let i = 0; i < 11; i++) {
      vi.advanceTimersByTime(5_000);
      observe();
    }
    vi.advanceTimersByTime(4_999);
    expect(fail).not.toHaveBeenCalled();
    expect(runaway).not.toHaveBeenCalled();
    vi.advanceTimersByTime(1);
    expect(fail).toHaveBeenCalledTimes(1);
    expect(runaway).toHaveBeenCalledTimes(1);
    expect(runaway.mock.calls[0]![0]).toMatchObject({ elapsedMs: 60_000, bytes: 36 });
    expect(vi.getTimerCount()).toBe(0);
  });

  it.each(["assistantMessageEvent", "event"])("does not count whitespace-only model deltas as recovery progress (%s)", (field) => {
    for (const type of ["text_delta", "thinking_delta", "toolcall_delta"]) {
      const { fail, watchdog } = setup();
      watchdog.arm("Error: Terminated");
      for (let i = 0; i < 11; i++) {
        vi.advanceTimersByTime(5_000);
        watchdog.observe({ type: "message_update", [field]: { type, delta: " \t\n" } });
      }
      vi.advanceTimersByTime(5_000);
      expect(fail).toHaveBeenCalledTimes(1);
    }
  });
  it("bounds a terminated response and reports the original cause exactly once", () => {
    const { fail, watchdog } = setup();
    expect(PI_RECOVERY_TIMEOUT_MS).toBe(60_000);
    watchdog.arm("Error: Terminated");
    vi.advanceTimersByTime(59_999);
    expect(fail).not.toHaveBeenCalled();
    vi.advanceTimersByTime(1);
    expect(fail).toHaveBeenCalledExactlyOnceWith(
      "Error: Terminated; no model stream or tool event for 60000ms; terminating child",
    );
    watchdog.arm("another retry");
    watchdog.progress();
    vi.advanceTimersByTime(120_000);
    expect(fail).toHaveBeenCalledTimes(1);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("does not extend the deadline for repeated errors without a delay announcement", () => {
    const { fail, watchdog } = setup();
    watchdog.arm("Error: Terminated");
    for (let i = 0; i < 5; i++) {
      vi.advanceTimersByTime(10_000);
      watchdog.arm("Error: Terminated");
    }
    vi.advanceTimersByTime(10_000);
    expect(fail).toHaveBeenCalledTimes(1);
  });

  it("refreshes on real streaming progress but still bounds a retry that stalls again", () => {
    const { fail, watchdog } = setup();
    watchdog.arm("Error: Terminated");
    vi.advanceTimersByTime(50_000);
    watchdog.progress();
    vi.advanceTimersByTime(50_000);
    expect(fail).not.toHaveBeenCalled();
    vi.advanceTimersByTime(10_000);
    expect(fail).toHaveBeenCalledTimes(1);
  });

  it.each(["assistantMessageEvent", "event"])("survives more than 60 seconds of stream activity in %s", (field) => {
    for (const type of ["text_delta", "thinking_delta", "toolcall_delta"]) {
      const { fail, watchdog } = setup();
      watchdog.arm("cliproxyapi/gpt-6.1-sol: terminated");
      for (let i = 0; i < 12; i++) {
        vi.advanceTimersByTime(10_000);
        watchdog.observe({ type: "message_update", [field]: { type, delta: "real output" } });
      }
      expect(fail).not.toHaveBeenCalled();
      vi.advanceTimersByTime(59_999);
      expect(fail).not.toHaveBeenCalled();
      vi.advanceTimersByTime(1);
      expect(fail).toHaveBeenCalledExactlyOnceWith(
        "cliproxyapi/gpt-6.1-sol: terminated; no model stream or tool event for 60000ms; terminating child",
      );
    }
  });

  it.each(["assistantMessageEvent", "event"])("counts model stream block boundaries in %s", (field) => {
    for (const type of ["text_start", "text_end", "thinking_start", "thinking_end", "toolcall_start", "toolcall_end"]) {
      const { fail, watchdog } = setup();
      watchdog.arm("Error: Terminated");
      vi.advanceTimersByTime(50_000);
      watchdog.observe({ type: "message_update", [field]: { type } });
      vi.advanceTimersByTime(50_000);
      expect(fail).not.toHaveBeenCalled();
      watchdog.dispose();
    }
  });

  it.each(["message_start", "message_update"])("counts assistant output in %s without a delta envelope", (type) => {
    for (const content of ["working", [{ type: "text", text: "working" }],
      [{ type: "thinking", thinking: "working" }], [{ type: "toolCall", name: "edit", arguments: {} }]]) {
      const { fail, watchdog } = setup();
      watchdog.arm("Error: Terminated");
      for (let i = 0; i < 12; i++) {
        vi.advanceTimersByTime(10_000);
        watchdog.observe({ type, message: { role: "assistant", content } });
      }
      expect(fail).not.toHaveBeenCalled();
      watchdog.dispose();
    }
  });

  it.each(["tool_execution_start", "tool_execution_update", "tool_execution_end"])("counts a %s tool event as recovery progress", (type) => {
    const { fail, watchdog } = setup();
    watchdog.arm("Error: Terminated");
    vi.advanceTimersByTime(50_000);
    watchdog.observe({ type, toolCallId: "edit-1" });
    vi.advanceTimersByTime(59_999);
    expect(fail).not.toHaveBeenCalled();
    vi.advanceTimersByTime(1);
    expect(fail).toHaveBeenCalledTimes(1);
  });

  it("does not extend recovery for errors, retry announcements or other chatter", () => {
    const { fail, watchdog } = setup();
    watchdog.arm("Error: Terminated");
    for (let i = 0; i < 5; i++) {
      vi.advanceTimersByTime(10_000);
      watchdog.arm("Error: Terminated");
      for (const event of [
        { type: "agent_start" }, { type: "agent_end", willRetry: true },
        { type: "auto_retry_start" }, { type: "auto_retry_end", success: false },
        { type: "queue_update" }, { type: "extension_error" },
        { type: "message_start", message: { role: "user", content: "retry prompt" } },
        { type: "message_start", message: { role: "toolResult", content: [] } },
        { type: "message_start", message: { role: "assistant", content: [], stopReason: "error" } },
        { type: "message_start", message: { role: "assistant", content: [], stopReason: "aborted" } },
        { type: "message_start" },
        { type: "message_update", assistantMessageEvent: { type: "error", error: "terminated" } },
        { type: "message_update", event: { type: "error", error: "terminated" } },
        { type: "message_update", assistantMessageEvent: { type: "text_delta", delta: "" }, message: { role: "assistant", content: "old output" } },
        { type: "message_update", message: { role: "assistant", content: "old output", stopReason: "aborted" } },
        { type: "message_update", message: { role: "user", content: "not model output" } },
        { type: "message_update", message: { role: "assistant", content: "old output", stopReason: "error" } },
        { type: "message_end", message: { role: "assistant", content: [], stopReason: "error" } },
      ]) watchdog.observe(event);
    }
    vi.advanceTimersByTime(9_999);
    expect(fail).not.toHaveBeenCalled();
    vi.advanceTimersByTime(1);
    expect(fail).toHaveBeenCalledTimes(1);
  });

  it("does not time out healthy work and permits a later independent failure", () => {
    const { fail, watchdog } = setup();
    watchdog.progress();
    vi.advanceTimersByTime(120_000);
    watchdog.arm("first error");
    vi.advanceTimersByTime(30_000);
    watchdog.clear();
    vi.advanceTimersByTime(120_000);
    expect(fail).not.toHaveBeenCalled();
    watchdog.arm("later error");
    vi.advanceTimersByTime(60_000);
    expect(fail.mock.calls[0]?.[0]).toContain("later error");
  });

  it("cancels pending recovery on stop, timeout or process exit", () => {
    const { fail, watchdog } = setup();
    watchdog.arm("Error: Terminated");
    watchdog.dispose();
    watchdog.progress();
    watchdog.arm("late event");
    vi.advanceTimersByTime(120_000);
    expect(fail).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
  });
});
