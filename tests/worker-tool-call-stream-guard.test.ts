import { afterEach, describe, expect, it, vi } from "vitest";
import {
  RunawayToolCallStreamError,
  TOOL_CALL_WHITESPACE_MAX_BYTES,
  TOOL_CALL_WHITESPACE_TIMEOUT_MS,
  ToolCallStreamGuard,
} from "../src/worker/tool-call-stream-guard.js";

afterEach(() => {
  vi.restoreAllMocks();
  vi.useRealTimers();
});
const update = (type: string, contentIndex = 0, delta?: string) => ({
  type: "message_update", assistantMessageEvent: { type, contentIndex, delta },
});
const setup = () => {
  vi.useFakeTimers();
  const fail = vi.fn();
  const guard = new ToolCallStreamGuard(fail, () => ({ model: "openai-codex/gpt-5.6-sol", effort: "high" }));
  return { guard, fail, delta: (text: string, index = 0) => guard.observe(update("toolcall_delta", index, text)) };
};

describe("ToolCallStreamGuard", () => {
  it("aborts exactly at 64 KiB with a typed and attributed error, once", () => {
    const { guard, fail, delta } = setup();
    expect(TOOL_CALL_WHITESPACE_MAX_BYTES).toBe(65536);
    delta(" ".repeat(65535));
    expect(fail).not.toHaveBeenCalled();
    delta("\t");
    expect(fail).toHaveBeenCalledTimes(1);
    const error = fail.mock.calls[0]![0];
    expect(error).toBeInstanceOf(RunawayToolCallStreamError);
    expect(error).toMatchObject({ name: "RunawayToolCallStreamError", code: "RUNAWAY_TOOL_CALL_STREAM",
      bytes: 65536, elapsedMs: 0, contentIndex: 0, model: "openai-codex/gpt-5.6-sol", effort: "high" });
    expect(error.message).toBe("runaway: whitespace-only tool-call stream for 0s / 65536 bytes (openai-codex/gpt-5.6-sol, high)");
    delta(" ");
    guard.observe(update("toolcall_start"));
    vi.advanceTimersByTime(120_000);
    expect(fail).toHaveBeenCalledTimes(1);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("aborts at 60 seconds despite continuous whitespace progress and unrelated events", () => {
    const { guard, fail, delta } = setup();
    expect(TOOL_CALL_WHITESPACE_TIMEOUT_MS).toBe(60_000);
    delta(" ");
    for (let i = 0; i < 59; i++) {
      vi.advanceTimersByTime(1000);
      delta("\n");
      guard.observe(update("text_delta", 1, "thinking isn't argument progress"));
    }
    vi.advanceTimersByTime(999);
    expect(fail).not.toHaveBeenCalled();
    vi.advanceTimersByTime(1);
    expect(fail).toHaveBeenCalledTimes(1);
    expect(fail.mock.calls[0]![0]).toMatchObject({ elapsedMs: 60_000, bytes: 60 });
  });

  it("bounds a whitespace stream that goes silent without requiring another delta", () => {
    const { fail, delta } = setup();
    delta("\t");
    vi.advanceTimersByTime(60_000);
    expect(fail.mock.calls[0]![0]).toMatchObject({ elapsedMs: 60_000, bytes: 1 });
  });

  it("re-arms for the remainder when the timer wakes 1 ms before the monotonic deadline", () => {
    const { fail, delta } = setup();
    let now = 1000;
    vi.spyOn(performance, "now").mockImplementation(() => now);
    const timer = vi.spyOn(globalThis, "setTimeout");
    delta(" ");
    now += 59_999;
    // The timer clock has reached its deadline, but the elapsed clock is 1 ms behind.
    vi.advanceTimersByTime(60_000);
    expect(fail).not.toHaveBeenCalled();
    expect(timer).toHaveBeenLastCalledWith(expect.any(Function), 1);
    expect(vi.getTimerCount()).toBe(1);
    now += 1;
    vi.advanceTimersByTime(1);
    expect(fail).toHaveBeenCalledTimes(1);
    expect(fail.mock.calls[0]![0]).toMatchObject({ elapsedMs: 60_000, bytes: 1 });
    expect(vi.getTimerCount()).toBe(0);
  });

  it.each([-86_400_000, 86_400_000])("ignores a wall-clock adjustment of %i ms", adjustment => {
    const { fail, delta } = setup();
    delta(" ");
    vi.setSystemTime(Date.now() + adjustment);
    vi.advanceTimersByTime(59_999);
    delta("\t");
    expect(fail).not.toHaveBeenCalled();
    vi.advanceTimersByTime(1);
    expect(fail).toHaveBeenCalledTimes(1);
    expect(fail.mock.calls[0]![0]).toMatchObject({ elapsedMs: 60_000, bytes: 2 });
  });

  it("reports monotonic elapsed time for the byte limit too", () => {
    const { fail, delta } = setup();
    delta(" ");
    vi.advanceTimersByTime(125);
    vi.setSystemTime(Date.now() - 86_400_000);
    delta(" ".repeat(65535));
    expect(fail).toHaveBeenCalledTimes(1);
    expect(fail.mock.calls[0]![0]).toMatchObject({ elapsedMs: 125, bytes: 65536 });
  });

  it("counts UTF-8 bytes rather than characters for Unicode whitespace", () => {
    const { fail, delta } = setup();
    delta("\u2003".repeat(21845)); // Three UTF-8 bytes each = 65535.
    expect(fail).not.toHaveBeenCalled();
    delta(" ");
    expect(fail.mock.calls[0]![0].bytes).toBe(65536);
  });

  it.each(["{", "\u200b", "x"])("permanently exempts a normal long call after non-whitespace %j", meaningful => {
    const { fail, delta } = setup();
    delta(" ");
    vi.advanceTimersByTime(59_999);
    delta(meaningful);
    delta(" ".repeat(100_000));
    vi.advanceTimersByTime(600_000);
    expect(fail).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
  });

  it("tracks each parallel call independently and ignores tool-result message boundaries", () => {
    const { guard, fail, delta } = setup();
    delta('{"code":', 0);
    delta(" ".repeat(32_768), 1);
    delta(" ".repeat(100_000), 0);
    guard.observe({ type: "message_start", message: { role: "toolResult" } });
    delta(" ".repeat(32_767), 1);
    expect(fail).not.toHaveBeenCalled();
    delta(" ", 1);
    expect(fail.mock.calls[0]![0]).toMatchObject({ contentIndex: 1, bytes: 65536 });
  });

  it("does not pool whitespace byte counts across parallel calls", () => {
    const { guard, fail, delta } = setup();
    delta(" ".repeat(40_000), 0);
    delta(" ".repeat(40_000), 1);
    expect(fail).not.toHaveBeenCalled();
    guard.dispose();
    expect(vi.getTimerCount()).toBe(0);
  });

  it.each(["toolcall_end", "message_end", "message_start", "agent_start", "dispose"])("cleans up on %s and permits independent later calls unless disposed", boundary => {
    const { guard, fail, delta } = setup();
    delta(" ");
    vi.advanceTimersByTime(30_000);
    if (boundary === "dispose") guard.dispose();
    else if (boundary === "toolcall_end") guard.observe(update(boundary));
    else guard.observe({ type: boundary, message: { role: "assistant" } });
    vi.advanceTimersByTime(120_000);
    expect(fail).not.toHaveBeenCalled();
    delta(" ".repeat(65536));
    expect(fail).toHaveBeenCalledTimes(boundary === "dispose" ? 0 : 1);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("does not bound a toolcall_start before any argument delta arrives", () => {
    const { guard, fail } = setup();
    guard.observe({ type: "message_update", assistantMessageEvent: {
      type: "toolcall_start", contentIndex: 0, partial: { content: [{ partialJson: "", arguments: {} }] },
    } });
    vi.advanceTimersByTime(120_000);
    expect(fail).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
    guard.dispose();
  });

  it("also bounds empty-only argument deltas at 60 seconds", () => {
    const { fail, delta } = setup();
    delta("");
    vi.advanceTimersByTime(59_999);
    delta("");
    expect(fail).not.toHaveBeenCalled();
    vi.advanceTimersByTime(1);
    expect(fail.mock.calls[0]![0]).toMatchObject({ elapsedMs: 60_000, bytes: 0 });
  });

  it.each([
    { partialJson: '{"code":', arguments: {} },
    { partialJson: "", arguments: { code: "already supplied" } },
  ])("respects meaningful provider-supplied initial arguments: %j", block => {
    const { guard, fail, delta } = setup();
    guard.observe({ type: "message_update", assistantMessageEvent: {
      type: "toolcall_start", contentIndex: 0, partial: { content: [{ type: "toolCall", ...block }] },
    } });
    delta(" ".repeat(100_000));
    vi.advanceTimersByTime(120_000);
    expect(fail).not.toHaveBeenCalled();
    guard.dispose();
  });

  it("does not abort legitimate calls when an oversized event hides meaningful arguments", () => {
    const { guard, fail, delta } = setup();
    delta(" ");
    guard.discardedEvent();
    delta(" ".repeat(100_000));
    vi.advanceTimersByTime(120_000);
    expect(fail).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
    guard.observe({ type: "message_start", message: { role: "assistant" } });
    delta(" ".repeat(65536));
    expect(fail).toHaveBeenCalledTimes(1);
  });

  it("counts initial raw whitespace arguments without counting cumulative partials twice", () => {
    const { guard, fail } = setup();
    guard.observe({ type: "message_update", assistantMessageEvent: {
      type: "toolcall_start", contentIndex: 0, partial: { content: [{ partialJson: " ".repeat(32_768), arguments: {} }] },
    } });
    guard.observe({ type: "message_update", assistantMessageEvent: {
      type: "toolcall_delta", contentIndex: 0, delta: " ".repeat(32_768),
      partial: { content: [{ partialJson: " ".repeat(65536), arguments: {} }] },
    } });
    expect(fail.mock.calls[0]![0].bytes).toBe(65536);
  });
});
