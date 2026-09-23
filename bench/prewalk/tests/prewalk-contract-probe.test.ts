import { afterEach, describe, expect, it, vi } from "vitest";
import { estimateTokens, type ExtensionAPI, type ProviderConfig } from "@earendil-works/pi-coding-agent";
import type { Model, SimpleStreamOptions } from "@earendil-works/pi-ai";
import { normalizeContext } from "@earendil-works/pi-ai/utils/transcript";
import probe from "../fixtures/prewalk-contract-probe.js";

const executor = { api: "prewalk-probe-api", provider: "prewalk-probe", id: "executor" } as Model<any>;
const context = normalizeContext({ messages: [{ role: "user", content: "x".repeat(40000), timestamp: 1 }] });
const load = () => {
  let config: ProviderConfig | undefined;
  const on = vi.fn();
  probe({ on, registerProvider: (_name: string, value: ProviderConfig) => { config = value; } } as unknown as ExtensionAPI);
  return { on, stream: (options?: SimpleStreamOptions) => config!.streamSimple!(executor, context, options) };
};
afterEach(() => { vi.unstubAllEnvs(); vi.useRealTimers(); });

describe("offline request-contract probe", () => {
  it("estimates synthetic usage from the request and response, without resetting the model on reload", async () => {
    const { on, stream } = load();
    const events = [];
    const result = stream();
    for await (const event of result) events.push(event.type);
    const message = await result.result();
    expect(message.usage.input).toBe(context.messages.reduce((sum, item) => sum + estimateTokens(item), 0));
    expect(message.usage.output).toBe(estimateTokens(message));
    expect(message.usage.totalTokens).toBe(message.usage.input + message.usage.output);
    expect(events).toEqual(["start", "done"]);
    expect(on.mock.calls.some(([type]) => type === "session_start")).toBe(false);
  });

  it.each([false, true])("honors executor cancellation (already aborted: %s) with one terminal event", async (alreadyAborted) => {
    vi.useFakeTimers();
    vi.stubEnv("PREWALK_PROBE_EXECUTOR_DELAY_MS", "60000");
    const abort = new AbortController();
    if (alreadyAborted) abort.abort();
    const result = load().stream({ signal: abort.signal });
    abort.abort();
    await vi.runAllTimersAsync();
    const events = [];
    for await (const event of result) events.push(event.type);
    const message = await result.result();
    expect(events).toEqual(["start", "error"]);
    expect(message.stopReason).toBe("aborted");
    expect(message.content).toEqual([]);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("finishes a delayed response once and removes its abort listener", async () => {
    vi.useFakeTimers();
    vi.stubEnv("PREWALK_PROBE_EXECUTOR_DELAY_MS", "100");
    const abort = new AbortController();
    const remove = vi.spyOn(abort.signal, "removeEventListener");
    const result = load().stream({ signal: abort.signal });
    await vi.advanceTimersByTimeAsync(100);
    abort.abort();
    const events = [];
    for await (const event of result) events.push(event.type);
    expect(events).toEqual(["start", "done"]);
    expect((await result.result()).stopReason).toBe("stop");
    expect(remove).toHaveBeenCalledWith("abort", expect.any(Function));
  });
});
