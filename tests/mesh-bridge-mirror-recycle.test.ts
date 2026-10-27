import { PassThrough } from "node:stream";
import { afterEach, describe, expect, it, vi } from "vitest";
import { BridgeMirrorDeferredError, RemoteBridgeSide } from "../src/mesh/bridge.js";

// KEEP: a real RPC channel must retain one late-reply slot, then release a silent
// transport to the existing reconnect owner; virtual time reaches the safety bound.
const remotes: RemoteBridgeSide[] = [];
afterEach(() => {
  for (const remote of remotes.splice(0)) remote.close();
  vi.useRealTimers();
});
const fixture = async () => {
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
  const input = new PassThrough(), output = new PassThrough();
  const requests: Array<{ id: number; op: string }> = [];
  output.on("data", chunk => {
    const request = JSON.parse(String(chunk)) as { id: number; op: string };
    requests.push(request);
    if (request.op === "hello") input.write(JSON.stringify({ id: request.id, ok: true,
      result: { version: 2, changes: true, independentRpc: true, partialPresence: true } }) + "\n");
  });
  const remote = new RemoteBridgeSide(input, output, 1_000);
  remotes.push(remote);
  await remote.hello();
  let failure: unknown;
  const mirror = remote.mirror({ hosts: [], participants: [] }).catch(error => { failure = error; });
  await vi.advanceTimersByTimeAsync(1_000);
  await mirror;
  expect(failure).toBeInstanceOf(BridgeMirrorDeferredError);
  expect(failure).toHaveProperty("timedOut", true);
  return { remote, input, output, requests, failure };
};

describe("bounded timed-out mirror gate", () => {
  it("closes the RPC channel after the pending grace bound, instead of rejecting topology forever", async () => {
    const f = await fixture();
    let closed: Error | undefined;
    void f.remote.closed.then(error => { closed = error; });
    await expect(f.remote.presence()).rejects.toBe(f.failure);
    await vi.advanceTimersByTimeAsync(149_999);
    expect(closed).toBeUndefined();
    expect(f.requests.filter(request => request.op === "mirror")).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(closed?.message).toMatch(/mirror.*150000 ms.*recycl/i);
    expect(f.input.destroyed).toBe(true);
    expect(f.output.writableEnded).toBe(true);
    await expect(f.remote.presence()).rejects.toBe(closed);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("accepts a late ACK before the bound, cancels recycling, and permits a fresh mirror", async () => {
    const f = await fixture();
    await vi.advanceTimersByTimeAsync(149_999);
    const old = f.requests.find(request => request.op === "mirror")!;
    f.input.write(JSON.stringify({ id: old.id, ok: true }) + "\n");
    await vi.advanceTimersByTimeAsync(1);
    expect(f.input.destroyed).toBe(false);
    expect(vi.getTimerCount()).toBe(0);
    const next = f.remote.mirror({ hosts: [], participants: [] });
    await Promise.resolve();
    const request = f.requests.at(-1)!;
    expect(request.op).toBe("mirror");
    expect(request.id).not.toBe(old.id);
    f.input.write(JSON.stringify({ id: request.id, ok: true }) + "\n");
    await next;
    expect(vi.getTimerCount()).toBe(0);
  });

  it.each(["close", "closeGracefully"] as const)("%s cancels the pending mirror recycling timer", async method => {
    const f = await fixture();
    const reason = new Error("test stop");
    f.remote[method](reason);
    await expect(f.remote.closed).resolves.toBe(reason);
    expect(vi.getTimerCount()).toBe(0);
    await vi.advanceTimersByTimeAsync(150_000);
    await expect(f.remote.closed).resolves.toBe(reason);
  });
});
