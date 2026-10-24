import * as transport from "@nats-io/transport-node";
import * as js from "@nats-io/jetstream";
import type { StreamInfo } from "@nats-io/jetstream";
import { afterEach, describe, expect, it, vi } from "vitest";
import { captureNatsConsumer, waitForNatsConsumerReady, waitForNatsStreamReady } from "./helpers/nats-consumer-ready.js";
import { startNatsElectionCluster, stopNatsElectionCluster, type NatsElectionCluster } from "./helpers/nats-election-cluster.js";

// Keep: #13112/#13851. A pending NEXT and a leader are independent of full replica recovery. These
// controlled INFO snapshots prove the gate itself (including missing membership and leader flips),
// which a live cluster cannot cheaply hold at every transition. The paused-follower case below also
// exercises real NATS INFO and broker recovery; no mutation retries are used in either kind of test.
const readyInfo = (): StreamInfo => ({
  config: { num_replicas: 3 }, cluster: { leader: "n1", replicas: [
    { name: "n2", current: true, offline: false, lag: 0 },
    { name: "n3", current: true, offline: false, lag: 0 },
  ] },
} as StreamInfo);
const controlledInfo = () => {
  vi.useFakeTimers();
  const state = { stream: readyInfo(), waiting: 1, targetPresent: true };
  const info = vi.fn(async () => state.stream);
  const consumer = vi.fn(async (_stream: string, name: string) => {
    if (!state.targetPresent) throw new js.JetStreamApiError({ code: 404, err_code: 10014, description: "consumer not found" });
    return { stream_name: _stream, name, num_waiting: state.waiting };
  });
  // LIST deliberately offers unrelated ready evidence even when the exact target is absent/not waiting.
  const list = vi.fn(() => ({ next: async () => [{ name: "unrelated", num_waiting: 1 }] }));
  const close = vi.fn(async () => undefined);
  vi.spyOn(transport, "connect").mockResolvedValue({ close } as unknown as Awaited<ReturnType<typeof transport.connect>>);
  const manager = { streams: { info }, consumers: { info: consumer, list } } as unknown as Awaited<ReturnType<typeof js.jetstreamManager>>;
  const getManager = vi.spyOn(js, "jetstreamManager").mockResolvedValue(manager);
  return { state, info, consumer, list, close, manager, getManager };
};
afterEach(() => { vi.useRealTimers(); vi.restoreAllMocks(); });

const controlledReceiver = (name: string) => {
  const cached = { stream_name: "STATE", name };
  const receiver = { name, stream: "STATE", info: vi.fn(async () => ({ ...cached })), consume: vi.fn(async () => ({})) } as unknown as js.Consumer & { name: string; stream: string };
  return { cached, receiver };
};
const controlledGets = (receivers: js.Consumer[]) => {
  class FixtureConsumers {
    async get(_stream: string) { return receivers.shift()!; }
  }
  const consumers = new FixtureConsumers();
  const originalGet = FixtureConsumers.prototype.get;
  vi.spyOn(js, "jetstream").mockReturnValue({ consumers } as unknown as ReturnType<typeof js.jetstream>);
  return { consumers, prototype: FixtureConsumers.prototype, originalGet };
};

describe("NATS harness readiness conditions", () => {
  it("does not accept pending NEXT or a leader flip while any R3 follower is not current", async () => {
    const { state, close } = controlledInfo();
    state.stream.cluster!.replicas![0]!.current = false;
    let ready = false;
    const gate = waitForNatsConsumerReady("nats://fixture", "STATE", "receiver").then(() => { ready = true; });
    await vi.advanceTimersByTimeAsync(0);
    expect(ready).toBe(false);
    state.stream.cluster = { leader: "n2", replicas: [
      { name: "n1", current: true, offline: false, active: 0, lag: 0 },
      { name: "n3", current: false, offline: false, active: 0, lag: 1 },
    ] };
    await vi.advanceTimersByTimeAsync(50);
    expect(ready).toBe(false);
    state.stream = readyInfo();
    await vi.advanceTimersByTimeAsync(50);
    await gate;
    expect(ready).toBe(true);
    expect(close).toHaveBeenCalledOnce();
  });

  it.each(["missing", "short", "duplicate", "offline", "lag", "leader"] as const)("rejects %s replica evidence until the full condition holds", async condition => {
    const { state } = controlledInfo();
    if (condition === "missing") delete state.stream.cluster!.replicas;
    if (condition === "short") state.stream.cluster!.replicas!.pop();
    if (condition === "duplicate") state.stream.cluster!.replicas![1]!.name = "n2";
    if (condition === "offline") state.stream.cluster!.replicas![0]!.offline = true;
    if (condition === "lag") state.stream.cluster!.replicas![0]!.lag = 1;
    if (condition === "leader") delete state.stream.cluster!.leader;
    let ready = false;
    const gate = waitForNatsStreamReady("nats://fixture", "STATE").then(() => { ready = true; });
    await vi.advanceTimersByTimeAsync(0);
    expect(ready).toBe(false);
    state.stream = readyInfo();
    await vi.advanceTimersByTimeAsync(50);
    await gate;
  });

  it("waits for a real pending NEXT after all replicas become current", async () => {
    const { state } = controlledInfo();
    state.waiting = 0;
    let ready = false;
    const gate = waitForNatsConsumerReady("nats://fixture", "STATE", "receiver").then(() => { ready = true; });
    await vi.advanceTimersByTimeAsync(0);
    expect(ready).toBe(false);
    state.waiting = 1;
    await vi.advanceTimersByTimeAsync(50);
    await gate;
  });

  it.each(["absent", "not waiting"])("does not borrow an unrelated ready receiver when its exact target is %s", async condition => {
    const { state, consumer, list } = controlledInfo();
    state.targetPresent = condition !== "absent";
    state.waiting = 0;
    let ready = false;
    const gate = waitForNatsConsumerReady("nats://fixture", "STATE", "target").then(() => { ready = true; });
    await vi.advanceTimersByTimeAsync(100);
    expect(ready).toBe(false);
    expect(consumer).toHaveBeenCalledWith("STATE", "target");
    expect(list).not.toHaveBeenCalled();
    state.targetPresent = true;
    state.waiting = 1;
    await vi.advanceTimersByTimeAsync(50);
    await gate;
    expect(ready).toBe(true);
  });

  it("follows the same captured handle from absent _1 to pending SDK successor _2", async () => {
    const { state, consumer, list } = controlledInfo();
    const { cached, receiver } = controlledReceiver("ordered_1");
    const { consumers, prototype, originalGet } = controlledGets([receiver]);
    const originalConsume = receiver.consume;
    const captured = await captureNatsConsumer("nats://fixture", "STATE", async () => {
      const selected = await consumers.get("STATE");
      await selected.consume();
      return 7;
    });
    expect(captured).toEqual({ value: 7, consumer: receiver });
    expect(prototype.get).toBe(originalGet);
    expect(receiver.consume).toBe(originalConsume);
    state.targetPresent = false;
    let ready = false;
    const gate = waitForNatsConsumerReady("nats://fixture", "STATE", captured.consumer, { timeoutMs: 150 }).then(() => { ready = true; });
    await vi.advanceTimersByTimeAsync(50);
    expect(ready).toBe(false);
    expect(consumer).toHaveBeenCalledWith("STATE", "ordered_1");
    receiver.name = cached.name = "ordered_2"; // Atomic settled state; split live/cache transition is tested below.
    state.targetPresent = true;
    await vi.advanceTimersByTimeAsync(50);
    await gate;
    expect(consumer).toHaveBeenLastCalledWith("STATE", "ordered_2");
    expect(list).not.toHaveBeenCalled();
  });

  it("rejects a predecessor ready ACK when the handle advances during asynchronous INFO", async () => {
    const { consumer } = controlledInfo();
    const { cached, receiver } = controlledReceiver("ordered_1");
    consumer.mockImplementationOnce(async (_stream, name) => {
      receiver.name = cached.name = "ordered_2";
      return { stream_name: _stream, name, num_waiting: 1 };
    }).mockImplementation(async (_stream, name) => ({ stream_name: _stream, name, num_waiting: 0 }));
    let ready = false;
    const gate = waitForNatsConsumerReady("nats://fixture", "STATE", receiver).then(() => { ready = true; });
    await vi.advanceTimersByTimeAsync(50);
    expect(ready).toBe(false);
    expect(consumer).toHaveBeenLastCalledWith("STATE", "ordered_2");
    consumer.mockImplementation(async (_stream, name) => ({ stream_name: _stream, name, num_waiting: 1 }));
    await vi.advanceTimersByTimeAsync(50);
    await gate;
  });

  it.each(["absent", "not waiting"])("keeps live _2/cache _1 closed despite predecessor pending, then rejects current _2 %s", async condition => {
    const { consumer, list, close } = controlledInfo();
    const { cached, receiver } = controlledReceiver("ordered_1");
    let predecessorWaiting = 0;
    let successorPresent = condition !== "absent";
    let successorWaiting = 0;
    consumer.mockImplementation(async (stream, name) => {
      if (name === "ordered_1") return { stream_name: stream, name, num_waiting: predecessorWaiting };
      if (!successorPresent) throw new js.JetStreamApiError({ code: 404, err_code: 10014, description: "consumer not found" });
      return { stream_name: stream, name, num_waiting: successorWaiting };
    });
    let ready = false;
    const gate = waitForNatsConsumerReady("nats://fixture", "STATE", receiver, { timeoutMs: 200 }).then(() => { ready = true; });
    await vi.advanceTimersByTimeAsync(0);
    expect(ready).toBe(false);
    expect(consumer.mock.calls.map(args => args[1])).toEqual(["ordered_1"]);
    receiver.name = "ordered_2"; // Native reset advances LIVE name before the add ACK updates cached INFO.
    predecessorWaiting = 1;
    await vi.advanceTimersByTimeAsync(50);
    expect(ready).toBe(false);
    expect(consumer.mock.calls.map(args => args[1])).toEqual(["ordered_1"]); // Never certify old pending.
    cached.name = "ordered_2";
    await vi.advanceTimersByTimeAsync(50);
    expect(ready).toBe(false);
    expect(consumer).toHaveBeenLastCalledWith("STATE", "ordered_2");
    successorPresent = true;
    successorWaiting = 1;
    await vi.advanceTimersByTimeAsync(50);
    await gate;
    expect(ready).toBe(true);
    expect(list).not.toHaveBeenCalled();
    expect(close).toHaveBeenCalledOnce();
  });

  it("rejects mid-INFO live advance while cache still names the ready predecessor", async () => {
    const { consumer } = controlledInfo();
    const { cached, receiver } = controlledReceiver("ordered_1");
    consumer.mockImplementationOnce(async (stream, name) => {
      receiver.name = "ordered_2";
      return { stream_name: stream, name, num_waiting: 1 };
    });
    let ready = false;
    const gate = waitForNatsConsumerReady("nats://fixture", "STATE", receiver, { timeoutMs: 150 }).then(() => { ready = true; });
    await vi.advanceTimersByTimeAsync(50);
    expect(ready).toBe(false);
    expect(consumer).toHaveBeenCalledTimes(1);
    cached.name = "ordered_2";
    await vi.advanceTimersByTimeAsync(50);
    await gate;
    expect(consumer).toHaveBeenLastCalledWith("STATE", "ordered_2");
  });

  it("accepts a healthy live receiver with matching cache and its own pending pull without delay", async () => {
    const { consumer } = controlledInfo();
    const { receiver } = controlledReceiver("ordered_2");
    let ready = false;
    const gate = waitForNatsConsumerReady("nats://fixture", "STATE", receiver).then(() => { ready = true; });
    await vi.advanceTimersByTimeAsync(0);
    expect(ready).toBe(true);
    await gate;
    expect(consumer).toHaveBeenCalledExactlyOnceWith("STATE", "ordered_2");
  });

  it("bounds a persistent live/cache transition by the original deadline", async () => {
    const { consumer, close } = controlledInfo();
    const { receiver } = controlledReceiver("ordered_1");
    receiver.name = "ordered_2";
    const outcome = waitForNatsConsumerReady("nats://fixture", "STATE", receiver, { timeoutMs: 75 }).catch(error => error as Error);
    await vi.advanceTimersByTimeAsync(75);
    expect((await outcome)?.message).toContain("readiness deadline");
    expect(consumer).not.toHaveBeenCalled();
    expect(close).toHaveBeenCalledOnce();
    expect(vi.getTimerCount()).toBe(0);
  });

  it.each([["stream", "OTHER"], ["stream", undefined], ["name", ""], ["name", undefined], ["name", 7]])(
    "fails invalid LIVE %s=%s rather than falling back to valid cached identity", async (field, value) => {
      const { consumer, close } = controlledInfo();
      const { receiver } = controlledReceiver("ordered_1");
      Object.assign(receiver, { [field as string]: value });
      const outcome = waitForNatsConsumerReady("nats://fixture", "STATE", receiver).catch(error => error as Error);
      await vi.advanceTimersByTimeAsync(0);
      expect((await outcome)?.message).toContain("identity mismatch");
      expect(consumer).not.toHaveBeenCalled();
      expect(close).toHaveBeenCalledOnce();
    });

  it.each(["stream", "name"])("fails invalid cached %s identity without querying another receiver", async field => {
    const { consumer, close } = controlledInfo();
    const { cached, receiver } = controlledReceiver("ordered_1");
    if (field === "stream") cached.stream_name = "OTHER";
    else cached.name = "";
    const outcome = waitForNatsConsumerReady("nats://fixture", "STATE", receiver).catch(error => error as Error);
    await vi.advanceTimersByTimeAsync(0);
    expect((await outcome)?.message).toContain("identity mismatch");
    expect(consumer).not.toHaveBeenCalled();
    expect(close).toHaveBeenCalledOnce();
  });

  it("never treats a missing consumer handle as a stream-only gate", async () => {
    const { consumer } = controlledInfo();
    await expect(waitForNatsConsumerReady("nats://fixture", "STATE", undefined as unknown as js.Consumer))
      .rejects.toThrow("actual consumer identity required");
    expect(consumer).not.toHaveBeenCalled();
  });

  it("bounds a stalled cached identity under the same absolute deadline", async () => {
    const { close } = controlledInfo();
    const { receiver } = controlledReceiver("ordered_1");
    vi.mocked(receiver.info).mockImplementation(() => new Promise(() => undefined));
    const outcome = waitForNatsConsumerReady("nats://fixture", "STATE", receiver, { timeoutMs: 75 }).catch(error => error as Error);
    await vi.advanceTimersByTimeAsync(75);
    expect((await outcome)?.message).toContain("readiness deadline");
    expect(close).toHaveBeenCalledOnce();
    expect(vi.getTimerCount()).toBe(0);
  });

  it("isolates simultaneous same-stream captures and ignores late abandoned get ACKs", async () => {
    controlledInfo();
    const a = controlledReceiver("a").receiver;
    const b = controlledReceiver("b").receiver;
    const late = controlledReceiver("late").receiver;
    const { consumers, prototype, originalGet } = controlledGets([a, b]);
    let resolveLate!: (receiver: js.Consumer) => void;
    const get = vi.spyOn(prototype, "get").mockImplementationOnce(() => new Promise(resolve => { resolveLate = resolve; }))
      .mockImplementation(originalGet);
    const before = prototype.get;
    const lateConsume = late.consume;
    const [first, second] = await Promise.all([captureNatsConsumer("nats://fixture", "STATE", async () => {
      void consumers.get("STATE"); // Abandoned bootstrap get ACK arrives after this scope closes.
      const receiver = await consumers.get("STATE");
      await receiver.consume();
    }), captureNatsConsumer("nats://fixture", "STATE", async () => {
      const receiver = await consumers.get("STATE");
      await receiver.consume();
    })]);
    expect(new Set([first.consumer, second.consumer])).toEqual(new Set([a, b]));
    resolveLate(late);
    await vi.advanceTimersByTimeAsync(0);
    expect(late.consume).toBe(lateConsume);
    expect(prototype.get).toBe(before);
    get.mockRestore();
  });

  it("restores get and unconsumed receiver hooks when the operation fails", async () => {
    controlledInfo();
    const { receiver } = controlledReceiver("unused");
    const { consumers, prototype, originalGet } = controlledGets([receiver]);
    const originalConsume = receiver.consume;
    const failure = new Error("open failed");
    await expect(captureNatsConsumer("nats://fixture", "STATE", async () => {
      await consumers.get("STATE");
      throw failure;
    })).rejects.toBe(failure);
    expect(prototype.get).toBe(originalGet);
    expect(receiver.consume).toBe(originalConsume);
  });

  it("observes temporarily unavailable manager INFO then ready state without a new deadline", async () => {
    const { getManager, info, close } = controlledInfo();
    getManager.mockRejectedValueOnce(new js.JetStreamApiError({ code: 503, err_code: 10008, description: "JetStream system temporarily unavailable" }));
    let ready = false;
    const gate = waitForNatsConsumerReady("nats://fixture", "STATE", "receiver", { timeoutMs: 75 }).then(() => { ready = true; });
    await vi.advanceTimersByTimeAsync(49);
    expect(ready).toBe(false);
    expect(info).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1);
    await gate;
    expect(ready).toBe(true);
    expect(close).toHaveBeenCalledOnce();
    expect(vi.getTimerCount()).toBe(0);
  });

  it("persistent temporarily unavailable manager expires at the original absolute deadline and closes", async () => {
    const { getManager, close, info } = controlledInfo();
    getManager.mockRejectedValue(new js.JetStreamApiError({ code: 503, err_code: 10008, description: "JetStream system temporarily unavailable" }));
    const outcome = waitForNatsStreamReady("nats://fixture", "STATE", { timeoutMs: 75 }).then(() => undefined, error => error as Error);
    await vi.advanceTimersByTimeAsync(75);
    expect((await outcome)?.message).toMatch(/readiness deadline.*JetStream system temporarily unavailable/);
    expect(info).not.toHaveBeenCalled();
    expect(close).toHaveBeenCalledOnce();
    expect(vi.getTimerCount()).toBe(0);
  });

  it.each([
    new js.JetStreamApiError({ code: 403, err_code: 10000, description: "permission denied" }),
    new js.JetStreamApiError({ code: 503, err_code: 10039, description: "JetStream not enabled for account" }),
    new js.JetStreamApiError({ code: 400, err_code: 10005, description: "no suitable peers" }),
    Object.assign(new Error("permission violation"), { name: "PermissionViolationError" }),
    Object.assign(new Error("request failed", { cause: Object.assign(new Error("permission violation"), { name: "PermissionViolationError" }) }), { name: "RequestError" }),
    new Error("unknown failure"),
  ])("surfaces manager permission/refusal/unknown error unchanged: %s", async failure => {
    const { getManager, close } = controlledInfo();
    getManager.mockRejectedValue(failure);
    const outcome = waitForNatsStreamReady("nats://fixture", "STATE").then(() => undefined, error => error);
    await vi.advanceTimersByTimeAsync(0);
    expect(await outcome).toBe(failure);
    expect(close).toHaveBeenCalledOnce();
    expect(vi.getTimerCount()).toBe(0);
  });

  it("surfaces a refused connection without attempting manager observation", async () => {
    const { getManager } = controlledInfo();
    const failure = Object.assign(new Error("connect ECONNREFUSED"), { name: "ConnectionError" });
    vi.mocked(transport.connect).mockRejectedValue(failure);
    await expect(waitForNatsStreamReady("nats://fixture", "STATE")).rejects.toBe(failure);
    expect(getManager).not.toHaveBeenCalled();
  });

  it.each(["manager", "stream"])("bounds slow first %s INFO without requiring an unavailable snapshot", async phase => {
    const { info, getManager, close } = controlledInfo();
    if (phase === "manager") getManager.mockImplementation(() => new Promise(() => undefined));
    else info.mockImplementation(() => new Promise(() => undefined));
    const outcome = waitForNatsStreamReady("nats://fixture", "STATE", { timeoutMs: 75 }).then(() => undefined, error => error as Error);
    await vi.advanceTimersByTimeAsync(75);
    expect((await outcome)?.message).toContain("readiness deadline for STATE");
    expect((await outcome)?.message).toContain("no stream INFO yet");
    expect(close).toHaveBeenCalledOnce();
    expect(vi.getTimerCount()).toBe(0);
  });

  it.each([1, 3])("already-ready R%s setup completes without a poll delay", async replicas => {
    const { state } = controlledInfo();
    if (replicas === 1) state.stream = { config: { num_replicas: 1 } } as StreamInfo;
    let done = false;
    const gates = Promise.all([waitForNatsStreamReady("nats://fixture", "STATE"),
      waitForNatsConsumerReady("nats://fixture", "STATE", "receiver")]).then(() => { done = true; });
    await vi.advanceTimersByTimeAsync(0); // Frozen time: advancing by 50ms would conceal an unconditional poll delay.
    expect(done).toBe(true);
    await gates;
    expect(vi.getTimerCount()).toBe(0);
  });

  it("bounds a stalled INFO by the absolute deadline and includes the last replica snapshot", async () => {
    const { state, info, close } = controlledInfo();
    state.stream.cluster!.replicas![0]!.current = false;
    info.mockImplementationOnce(async () => state.stream).mockImplementation(() => new Promise<StreamInfo>(() => undefined));
    const outcome = waitForNatsStreamReady("nats://fixture", "STATE", { timeoutMs: 75 }).then(() => undefined, error => error as Error);
    await vi.advanceTimersByTimeAsync(50);
    await vi.advanceTimersByTimeAsync(25);
    const error = await outcome;
    expect(error?.message).toContain("readiness deadline for STATE");
    expect(error?.message).toContain('"current":false');
    expect(close).toHaveBeenCalledOnce();
    expect(vi.getTimerCount()).toBe(0);
  });
});

// Opt-in premerge integration/diagnostic probes: NATS_BIN enables these locally. This new file is NOT
// listed in the dedicated CI job; that job retains the actual product four-child and election paths.
const binary = process.env.NATS_BIN;
// Owned raw-SDK gate-isolation fixture ONLY. A short native heartbeat makes a lost initial pull recover
// without test-level mutation retries, and explicit inactivity keeps the durable fixture alive. These
// are NOT product consume options and do not claim to fix the product default-pull behavior.
const isolationPull = { expires: 2_000, idle_heartbeat: 500 };
let cluster: NatsElectionCluster | undefined;
afterEach(async () => {
  if (cluster) {
    const owned = cluster;
    cluster = undefined;
    await stopNatsElectionCluster(owned);
    console.log(`NATS_READINESS_CLEANUP ${JSON.stringify(owned.cleanupReceipt())}`);
  }
});
it.skipIf(!binary)("real two-receiver boundary names each actual SDK get and never borrows unrelated pending pulls", async () => {
  cluster = await startNatsElectionCluster(binary!);
  const seed = cluster.seedUrl;
  const stream = "HARNESS_RECEIVERS";
  const nc = await transport.connect({ servers: [seed], ignoreClusterUpdates: true });
  const subscriptions: Awaited<ReturnType<js.Consumer["consume"]>>[] = [];
  const readers: Promise<void>[] = [];
  const receiveErrors: unknown[] = [];
  const read = (messages: Awaited<ReturnType<js.Consumer["consume"]>>) => {
    // The SDK's iterator drives its queued NEXT callback, just as the product adapter's for-await does.
    readers.push((async () => { try { for await (const message of messages) void message; }
      catch (error) { receiveErrors.push(error); } })());
  };
  let targetMessages: Awaited<ReturnType<js.Consumer["consume"]>> | undefined;
  try {
    const manager = await js.jetstreamManager(nc, { timeout: 1_000 });
    await manager.streams.add({ name: stream, subjects: [`${stream}.state`], num_replicas: 3 });
    await waitForNatsStreamReady(seed, stream);
    // This gate-isolation fixture owns durable receivers, independent of the separately unresolved product
    // consumer[] outcome. Product receiver arguments remain untouched in the four-child flow.
    for (const name of ["unrelated_a", "unrelated_b", "target"]) {
      await manager.consumers.add(stream, { durable_name: name, ack_policy: js.AckPolicy.None,
        inactive_threshold: 120_000 * 1e6 });
    }
    // Concurrent real SDK gets on the same stream: async-local capture must not cross identities.
    const receivers = await Promise.all(["unrelated_a", "unrelated_b"].map(name => captureNatsConsumer(seed, stream, async () => {
      const receiver = await js.jetstream(nc).consumers.get(stream, name);
      const messages = await receiver.consume(isolationPull);
      subscriptions.push(messages);
      read(messages);
      return messages;
    })));
    const names = await Promise.all(receivers.map(async receiver => (await receiver.consumer.info(true)).name));
    expect(names).toEqual(["unrelated_a", "unrelated_b"]);
    await Promise.all(receivers.map(receiver => waitForNatsConsumerReady(seed, stream, receiver.consumer)));
    const unrelated = await manager.consumers.list(stream).next();
    expect(unrelated.filter(info => info.num_waiting > 0).map(info => info.name).sort()).toEqual([...names].sort());
    await expect(waitForNatsConsumerReady(seed, stream, "absent_target", { timeoutMs: 500 })).rejects.toThrow(/readiness deadline/);
    const target = await js.jetstream(nc).consumers.get(stream, "target");
    const targetInfo = await target.info(true);
    expect(targetInfo.num_waiting).toBe(0);
    await expect(waitForNatsConsumerReady(seed, stream, target, { timeoutMs: 500 })).rejects.toThrow(/readiness deadline/);
    targetMessages = await target.consume(isolationPull); // Owned raw fixture options, no reset/delete or mutation retry.
    read(targetMessages);
    await waitForNatsConsumerReady(seed, stream, target);
    expect((await manager.consumers.info(stream, targetInfo.name)).num_waiting).toBeGreaterThan(0);
    expect(receiveErrors).toEqual([]);
    console.log(`NATS_READINESS_RECEIVERS ${JSON.stringify({ unrelated: names,
      target: targetInfo.name, absentRejected: true, targetWithoutPullRejected: true, originalTargetPullReady: true })}`);
  } finally {
    await targetMessages?.close();
    await Promise.all(subscriptions.map(subscription => subscription.close()));
    await nc.close();
    await Promise.all(readers);
  }
}, 60_000);

// Additional opt-in diagnostic, NOT a stable gate-isolation fixture or a default-product recovery promise.
// Observed: two native _1 -> _2 recoveries near 30s, but also a 45s expiry with cached _1 unchanged.
// Keep that failure visible in diagnostics; the controlled tests above deterministically test the gate's
// successor/racing-INFO contract without requiring the SDK itself to recover in every seeded run.
it.skipIf(!binary || process.env.NATS_ORDERED_RECOVERY_PROBE !== "1")("real ordered SDK recovery advances the SAME captured handle after a dropped first NEXT", async () => {
  cluster = await startNatsElectionCluster(binary!);
  const seed = cluster.seedUrl;
  const stream = "HARNESS_ORDERED_RECOVERY";
  const nc = await transport.connect({ servers: [seed], ignoreClusterUpdates: true });
  let messages: Awaited<ReturnType<js.Consumer["consume"]>> | undefined;
  let reading: Promise<void> | undefined;
  const errors: unknown[] = [];
  const publish = nc.publish;
  let dropped = 0;
  const started = performance.now();
  try {
    const manager = await js.jetstreamManager(nc, { timeout: 1_000 });
    await manager.streams.add({ name: stream, subjects: [`${stream}.state`], num_replicas: 3 });
    await waitForNatsStreamReady(seed, stream);
    nc.publish = function (...args: Parameters<typeof nc.publish>) {
      if (dropped === 0 && args[0].includes(`.CONSUMER.MSG.NEXT.${stream}.`)) {
        dropped++;
        return; // Explicit owned seed: no handcrafted ACK and no reset/replacement/delete by the test.
      }
      return publish.apply(this, args);
    };
    let predecessor = "";
    const captured = await captureNatsConsumer(seed, stream, async () => {
      // Owned diagnostic's short inactivity lets the broker naturally remove the unused _1.
      // consume() keeps the SDK default 30s expiry/heartbeat: native recovery must discover and recreate it.
      const receiver = await js.jetstream(nc).consumers.get(stream, { name_prefix: "native_recovery", inactive_threshold: 1_000 });
      predecessor = (await receiver.info(true)).name;
      messages = await receiver.consume();
      reading = (async () => { try { for await (const message of messages) void message; }
        catch (error) { errors.push(error); } })();
    });
    expect(dropped).toBe(1);
    await waitForNatsConsumerReady(seed, stream, captured.consumer);
    const successor = await captured.consumer.info(true);
    expect(predecessor).toBe("native_recovery_1");
    expect(successor.name).toBe("native_recovery_2");
    expect((await manager.consumers.info(stream, successor.name)).num_waiting).toBeGreaterThan(0);
    expect(errors).toEqual([]);
    console.log(`NATS_READINESS_NATIVE_RECOVERY ${JSON.stringify({ predecessor, successor: successor.name,
      sameConsumedHandle: true, droppedFirstNext: dropped, elapsedMs: performance.now() - started })}`);
  } finally {
    nc.publish = publish;
    await messages?.close();
    await nc.close();
    await reading;
  }
}, 60_000);

it.skipIf(process.platform !== "linux" || !binary)("real R3 readiness stays closed for a paused follower and opens after replica recovery", async () => {
  cluster = await startNatsElectionCluster(binary!);
  const nc = await transport.connect({ servers: [cluster.seedUrl], ignoreClusterUpdates: true });
  let follower: NatsElectionCluster["nodes"][number] | undefined;
  let gate: Promise<void> | undefined;
  try {
    const manager = await js.jetstreamManager(nc, { timeout: 1_000 });
    const stream = "HARNESS_READINESS";
    await manager.streams.add({ name: stream, subjects: [`${stream}.state`], num_replicas: 3 });
    await waitForNatsStreamReady(cluster.seedUrl, stream);
    const info = await manager.streams.info(stream);
    const seed = cluster.nodes.find(node => node.clientUrl === cluster!.seedUrl)!;
    const followers = info.cluster!.replicas!.map(peer => peer.name);
    follower = cluster.nodes.find(node => node !== seed && followers.includes(node.name));
    expect(seed, "connected seed must map to an owned node").toBeDefined();
    expect(followers).toHaveLength(2);
    expect(follower, "one of two followers must differ from the healthy connected seed").toBeDefined();
    if (!follower) throw new Error("no owned follower distinct from connected seed");
    expect(follower).not.toBe(seed);
    expect(follower!.name).not.toBe(info.cluster!.leader);
    console.log(`NATS_READINESS_TOPOLOGY ${JSON.stringify({ seed: seed.name, leader: info.cluster!.leader,
      followers, paused: follower!.name, probe: cluster.seedUrl })}`);
    expect(follower.child.kill("SIGSTOP")).toBe(true);
    await expect.poll(async () => (await manager.streams.info(stream)).cluster?.replicas?.find(peer => peer.name === follower!.name)?.current,
      { timeout: 10_000, interval: 50 }).toBe(false);
    // The helper must reject the same live broker condition even though the leader remains elected.
    // A healthy seed remains available. 1.5s includes a full 1s SDK request plus observation margin;
    // slow first INFO may still leave no snapshot, so rejection must not depend on diagnostic contents.
    await expect(waitForNatsStreamReady(cluster.seedUrl, stream, { timeoutMs: 1_500 })).rejects.toThrow(/readiness deadline/);
    gate = waitForNatsStreamReady(cluster.seedUrl, stream);
    expect(follower.child.kill("SIGCONT")).toBe(true);
    await gate;
    const recovered = await manager.streams.info(stream);
    expect(recovered.cluster?.replicas).toHaveLength(2);
    expect(recovered.cluster?.replicas?.every(peer => peer.current && !peer.offline && (peer.lag ?? 0) === 0)).toBe(true);
  } finally {
    follower?.child.kill("SIGCONT");
    await gate?.catch(() => undefined);
    await nc.close();
  }
}, 60_000);
