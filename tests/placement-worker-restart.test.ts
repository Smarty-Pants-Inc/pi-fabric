import { EventEmitter } from "node:events";
import { afterEach, describe, expect, it, vi } from "vitest";
import { requestPlacementRestart, type PlacementRestartRequest } from "../src/agents/worker-protocol.js";

// Keep reason: private IPC trust boundary and failure paths that should never
// grant a restart. Real worker/process coverage lives in the preflight suite.
const request: PlacementRestartRequest = { type: "placement.restart", launchId: "launch", generation: 8 };
const fixture = () => {
  const events = Object.assign(new EventEmitter(), {
    connected: true,
    send: vi.fn((_message: unknown, callback: (error: Error | null) => void) => callback(null)),
  });
  const channel = events as unknown as Parameters<typeof requestPlacementRestart>[0];
  const answer = (extra: Record<string, unknown> = {}) => events.emit("message", {
    type: "placement.restart.decision", launchId: request.launchId, generation: request.generation, decision: "BASELINE", ...extra,
  });
  return { events, channel, answer };
};
afterEach(() => { vi.useRealTimers(); vi.restoreAllMocks(); });

describe("worker placement restart response boundary", () => {
  it("sends only the launch identity and consumes a correlated baseline decision", async () => {
    const f = fixture();
    const restarting = requestPlacementRestart(f.channel, request);
    expect(f.events.send.mock.calls[0]?.[0]).toEqual(request);
    f.answer();
    expect(await restarting).toBe(true);
    expect(f.events.listenerCount("message")).toBe(0);
    expect(f.events.listenerCount("disconnect")).toBe(0);
  });

  it.each([
    { decision: "REFUSE" }, { decision: "PLACE" }, { decision: undefined },
    { hypothetical: { decision: "PLACE", host: "other" } }, { grant: { host: "other" } },
    { task: "not-an-allowed-response-field" },
  ])("refuses a malformed or non-baseline answer: %j", async response => {
    const f = fixture();
    const restarting = requestPlacementRestart(f.channel, request);
    f.answer(response);
    expect(await restarting).toBe(false);
    f.answer(); // A later valid-looking frame cannot revise the refusal.
    expect(f.events.listenerCount("message")).toBe(0);
  });

  it.each([{ launchId: "other" }, { generation: 7 }, { generation: 9 }])("does not consume another request's answer: %j", async response => {
    vi.useFakeTimers();
    const f = fixture();
    let settled = false;
    const restarting = requestPlacementRestart(f.channel, request).then(value => { settled = true; return value; });
    f.answer(response);
    await Promise.resolve();
    expect(settled).toBe(false);
    await vi.advanceTimersByTimeAsync(2_000);
    expect(await restarting).toBe(false);
    f.answer();
    expect(f.events.listenerCount("message")).toBe(0);
  });

  it("ignores a late timed-out answer while a fresh generation is pending", async () => {
    vi.useFakeTimers();
    const f = fixture();
    const previous = requestPlacementRestart(f.channel, request);
    await vi.advanceTimersByTimeAsync(2_000);
    expect(await previous).toBe(false);
    let settled = false;
    const next = requestPlacementRestart(f.channel, { ...request, generation: 9 }).then(value => { settled = true; return value; });
    f.answer();
    await Promise.resolve();
    expect(settled).toBe(false);
    f.answer({ generation: 9, decision: "REFUSE" });
    expect(await next).toBe(false);
  });

  it.each(["disconnect", "abort", "send-error"])("fails closed on %s and removes pending listeners", async cause => {
    const f = fixture();
    const abort = new AbortController();
    if (cause === "send-error") f.events.send.mockImplementation((_message, callback) => callback(new Error("closed")));
    const restarting = requestPlacementRestart(f.channel, request, abort.signal);
    if (cause === "disconnect") { f.events.connected = false; f.events.emit("disconnect"); }
    if (cause === "abort") abort.abort();
    expect(await restarting).toBe(false);
    f.answer();
    expect(f.events.listenerCount("message")).toBe(0);
    expect(f.events.listenerCount("disconnect")).toBe(0);
  });

  it("never treats BASELINE as enforcing authority (no full native grant path is implemented)", async () => {
    const f = fixture();
    const restarting = requestPlacementRestart(f.channel, request, undefined, true);
    f.answer();
    expect(await restarting).toBe(false);
  });

  it("refuses missing/legacy IPC and malformed outbound identities", async () => {
    const f = fixture();
    f.events.connected = false;
    expect(await requestPlacementRestart(f.channel, request)).toBe(false);
    f.events.connected = true;
    expect(await requestPlacementRestart(f.channel, { ...request, generation: Number.MAX_SAFE_INTEGER + 1 })).toBe(false);
    expect(f.events.send).not.toHaveBeenCalled();
  });
});
