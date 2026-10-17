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
    return { name, num_waiting: state.waiting };
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

const binary = process.env.NATS_BIN;
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
      await manager.consumers.add(stream, { durable_name: name, ack_policy: js.AckPolicy.None });
    }
    // Concurrent real SDK gets on the same stream: async-local capture must not cross identities.
    const receivers = await Promise.all(["unrelated_a", "unrelated_b"].map(name => captureNatsConsumer(seed, stream, async () => {
      const receiver = await js.jetstream(nc).consumers.get(stream, name);
      const messages = await receiver.consume();
      subscriptions.push(messages);
      read(messages);
      return messages;
    })));
    expect(receivers.map(receiver => receiver.name)).toEqual(["unrelated_a", "unrelated_b"]);
    await Promise.all(receivers.map(receiver => waitForNatsConsumerReady(seed, stream, receiver.name)));
    const unrelated = await manager.consumers.list(stream).next();
    expect(unrelated.filter(info => info.num_waiting > 0).map(info => info.name).sort()).toEqual(receivers.map(receiver => receiver.name).sort());
    await expect(waitForNatsConsumerReady(seed, stream, "absent_target", { timeoutMs: 500 })).rejects.toThrow(/readiness deadline/);
    const target = await js.jetstream(nc).consumers.get(stream, "target");
    const targetInfo = await target.info(true);
    expect(targetInfo.num_waiting).toBe(0);
    await expect(waitForNatsConsumerReady(seed, stream, targetInfo.name, { timeoutMs: 500 })).rejects.toThrow(/readiness deadline/);
    targetMessages = await target.consume(); // Original default SDK pull; no reset/delete or mutation retry.
    read(targetMessages);
    await waitForNatsConsumerReady(seed, stream, targetInfo.name);
    expect((await manager.consumers.info(stream, targetInfo.name)).num_waiting).toBeGreaterThan(0);
    expect(receiveErrors).toEqual([]);
    console.log(`NATS_READINESS_RECEIVERS ${JSON.stringify({ unrelated: receivers.map(receiver => receiver.name),
      target: targetInfo.name, absentRejected: true, targetWithoutPullRejected: true, originalTargetPullReady: true })}`);
  } finally {
    await targetMessages?.close();
    await Promise.all(subscriptions.map(subscription => subscription.close()));
    await nc.close();
    await Promise.all(readers);
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
