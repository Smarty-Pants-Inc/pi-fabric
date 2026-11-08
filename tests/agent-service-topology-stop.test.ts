import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AgentService } from "../src/agents/service.js";
import { createAgentServiceClient, createAgentServiceHandler, createAgentsProvider } from "../src/agents/service-provider.js";
import type { AgentExecutionRequest, AgentExecutionResponse, AgentServiceOptions, AgentSessionRecord, AgentTopologyStopRequest } from "../src/agents/service-types.js";
import type { FabricActorInfo } from "../src/actors/types.js";
import type { FabricInvocationContext } from "../src/protocol.js";

const deferred = <T>() => {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(yes => { resolve = yes; });
  return { promise, resolve };
};
const services: AgentService[] = [];
const releases: Array<() => void> = [];
const context = (signal?: AbortSignal) => ({ signal }) as FabricInvocationContext;
const peer: AgentSessionRecord = { id: "remote:actor", name: "remote", kind: "actor", status: "running", capabilities: ["stop"] };
const nativeReceipt = (): FabricActorInfo => ({
  id: peer.id, name: peer.name, scope: "session", rootId: "session:owner", status: "stopped", runner: "pi",
  events: ["agent_settled"], topics: ["work.ready"], delivery: "mailbox", responseMode: "text", triggerTurn: false, coalesce: false,
  principal: { id: "session:owner", digest: "native-principal" },
  requirements: [{ ref: "mesh.publish" }, { ref: "agents.ask", optional: true }],
  capabilityDigest: "native-capabilities", missingCapabilities: ["agents.ask"],
  queued: 0, messages: 3, createdAt: 1, updatedAt: 20,
});
const harness = (options: Partial<AgentServiceOptions> = {}, callerId = "root") => {
  const service = new AgentService({ rootId: "root", port: { execute: async () => ({ status: "completed" }) }, ...options });
  services.push(service);
  const client = createAgentServiceClient(createAgentServiceHandler(service, callerId), service.capabilities);
  return { service, client, provider: createAgentsProvider(client) };
};
const topology = (stop?: NonNullable<AgentServiceOptions["topology"]>["stop"]): NonNullable<AgentServiceOptions["topology"]> => ({
  self: () => peer, sessions: () => [peer], peers: () => [peer], deliver: async () => peer,
  ...(stop ? { stop } : {}),
});
const tracked = <T>(promise: Promise<T>) => {
  let settled = false;
  const result = promise.then(value => { settled = true; return { value }; }, error => { settled = true; return { error }; });
  return { result, settled: () => settled };
};

beforeEach(() => {
  // Exercise an authenticated hosted Main, not the test process's task launch identity.
  for (const key of ["PI_FABRIC_PARENT_RUN", "PI_FABRIC_ACTOR_ID", "PI_FABRIC_LINEAGE", "PI_FABRIC_DEPTH", "PI_FABRIC_TASK_PROCESS_CHILD", "PI_FABRIC_AGENT_RUN_DIR", "PI_FABRIC_AGENT_NAME", "PI_FABRIC_SPAWNER_KIND", "SMARTY_ROLE"]) vi.stubEnv(key, undefined);
});
afterEach(async () => {
  vi.unstubAllEnvs();
  for (const release of releases.splice(0)) release();
  await Promise.allSettled(services.splice(0).map(service => service.close()));
});

// Keep reasons: authenticated host trust boundary, native receipt isolation, and deferred
// host acknowledgment. The provider path crosses the actual client -> handler -> service seam.
describe("hosted topology STOP", () => {
  it("waits for native acknowledgment and deep-clones the unchanged actor receipt", async () => {
    const ack = deferred<FabricActorInfo>();
    const stop = vi.fn(async (_request: AgentTopologyStopRequest) => ack.promise);
    const { service, provider } = harness({ topology: topology(stop) });
    const receipt = nativeReceipt();
    const expected = structuredClone(receipt);
    const pending = tracked(provider.invoke("stop", { id: peer.id }, context()));
    await vi.waitFor(() => expect(stop).toHaveBeenCalledOnce());
    expect(pending.settled()).toBe(false);
    expect(service.snapshot().records).toEqual([]);
    ack.resolve(receipt);
    const outcome = await pending.result;
    expect(outcome).toEqual({ value: expected });
    const result = (outcome as { value: FabricActorInfo }).value;
    expect(result).not.toBe(receipt);
    expect(result).toStrictEqual(expected);
    expect(Object.keys(result).sort()).toEqual(Object.keys(expected).sort());
    expect(result.topics).not.toBe(receipt.topics);
    expect(result.principal).not.toBe(receipt.principal);
    expect(result.requirements).not.toBe(receipt.requirements);
    expect(result.requirements![0]).not.toBe(receipt.requirements![0]);
    expect(result.missingCapabilities).not.toBe(receipt.missingCapabilities);
    receipt.topics.push("host-mutated");
    receipt.principal!.digest = "host-mutated";
    receipt.requirements![0]!.optional = true;
    receipt.missingCapabilities!.push("mesh.publish");
    expect(result).toStrictEqual(expected);
    const hostAfterMutation = structuredClone(receipt);
    result.topics.push("guest-mutated");
    result.principal!.id = "guest-mutated";
    result.requirements![0]!.ref = "guest-mutated";
    result.missingCapabilities!.push("guest-mutated");
    expect(receipt).toStrictEqual(hostAfterMutation);
    expect(result).not.toHaveProperty("generation");
    expect(service.snapshot().records).toEqual([]);
  });

  it("passes a native session stop acknowledgment through the hosted path unchanged and cloned", async () => {
    const ack = deferred<AgentSessionRecord>();
    const native: AgentSessionRecord = {
      id: "session:native-main", name: "Native Pi DurableConversation", kind: "agent", status: "stopping", capabilities: ["stop", "ask"],
    };
    const expected = structuredClone(native);
    const stop = vi.fn((_request: AgentTopologyStopRequest) => ack.promise);
    const { service, provider } = harness({ topology: topology(stop) });
    const pending = tracked(provider.invoke("stop", { id: native.id }, context()));
    await vi.waitFor(() => expect(stop).toHaveBeenCalledOnce());
    expect(stop.mock.calls).toEqual([[{ callerId: "root", id: native.id }]]);
    expect(pending.settled()).toBe(false);
    expect(service.snapshot().records).toEqual([]);

    ack.resolve(native);
    const outcome = await pending.result;
    expect(outcome).toEqual({ value: expected });
    const result = (outcome as { value: AgentSessionRecord }).value;
    expect(result).not.toBe(native);
    expect(result).toStrictEqual(expected);
    expect(Object.keys(result).sort()).toEqual(Object.keys(expected).sort());
    expect(result).not.toHaveProperty("rootId");
    expect(result).not.toHaveProperty("generation");
    expect(result.capabilities).not.toBe(native.capabilities);

    native.name = "mutated by host";
    native.capabilities.push("followUp");
    expect(result).toEqual(expected);
    result.status = "stopped";
    result.capabilities.push("steer");
    expect(native.name).toBe("mutated by host");
    expect(native.status).toBe(expected.status);
    expect(native.capabilities).toEqual([...expected.capabilities, "followUp"]);
    expect(service.snapshot().records).toEqual([]);
  });

  it("preserves the native topology receiver through the hosted provider", async () => {
    const receipt = nativeReceipt();
    const hostTopology = topology();
    hostTopology.stop = function (this: NonNullable<AgentServiceOptions["topology"]>, _request: AgentTopologyStopRequest) {
      expect(this).toBe(hostTopology);
      return Promise.resolve(receipt);
    };
    const { provider } = harness({ topology: hostTopology });
    await expect(provider.invoke("stop", { id: peer.id }, context())).resolves.toEqual(receipt);
  });

  describe.each(["provider", "client"] as const)("%s forwarding", path => {
    it("forwards authenticated identity, exact id and the invocation signal", async () => {
      const receipt = nativeReceipt();
      const stop = vi.fn(async (_request: AgentTopologyStopRequest) => receipt);
      const { client, provider } = harness({ topology: topology(stop) });
      const controller = new AbortController();
      const id = " remote:exact-id ";
      const result = path === "provider"
        ? await provider.invoke("stop", { id }, context(controller.signal))
        : await client.stop(id, controller.signal);
      expect(result).toEqual(receipt);
      expect(stop.mock.calls).toEqual([[{ callerId: "root", id, signal: controller.signal }]]);
      expect(stop.mock.calls[0]![0].signal).toBe(controller.signal);
      expect(stop.mock.calls[0]![0]).not.toHaveProperty("explicit");
    });
  });

  it("leaves the optional invocation signal absent", async () => {
    const stop = vi.fn(async (_request: AgentTopologyStopRequest) => nativeReceipt());
    const { provider } = harness({ topology: topology(stop) });
    await provider.invoke("stop", { id: peer.id }, context());
    expect(stop.mock.calls).toEqual([[{ callerId: "root", id: peer.id }]]);
  });

  it("preserves the host's refusal object unchanged without retry or execution effects", async () => {
    const refusal = new Error("native stop refused");
    const stop = vi.fn(async (_request: AgentTopologyStopRequest): Promise<FabricActorInfo> => { throw refusal; });
    const execute = vi.fn(async () => ({ status: "completed" as const }));
    const { provider } = harness({ topology: topology(stop), port: { execute } });
    await expect(provider.invoke("stop", { id: peer.id }, context())).rejects.toBe(refusal);
    expect(stop).toHaveBeenCalledOnce();
    expect(execute).not.toHaveBeenCalled();
  });

  it.each([false, true])("retains unknown-direct-child refusal without an optional callback (directory=%s)", async hasTopology => {
    const { provider } = harness(hasTopology ? { topology: topology() } : {});
    await expect(provider.invoke("stop", { id: peer.id }, context())).rejects.toThrow(`Unknown direct child agent: ${peer.id}`);
  });

  it("refuses an unknown authenticated caller before any host effect", async () => {
    const stop = vi.fn(async (_request: AgentTopologyStopRequest) => nativeReceipt());
    const { provider } = harness({ topology: topology(stop) }, "unknown-caller");
    await expect(provider.invoke("stop", { id: peer.id }, context())).rejects.toThrow("Unknown agent caller");
    expect(stop).not.toHaveBeenCalled();
  });

  it("checks authority before the callback and again after its deferred receipt", async () => {
    const ack = deferred<FabricActorInfo>();
    let authorized = false;
    const assertAuthority = vi.fn(() => { if (!authorized) throw new Error("lease revoked"); });
    const stop = vi.fn(async (_request: AgentTopologyStopRequest) => ack.promise);
    const { provider } = harness({ topology: topology(stop), assertAuthority });
    await expect(provider.invoke("stop", { id: peer.id }, context())).rejects.toThrow("lease revoked");
    expect(stop).not.toHaveBeenCalled();
    authorized = true;
    const pending = tracked(provider.invoke("stop", { id: peer.id }, context()));
    await vi.waitFor(() => expect(stop).toHaveBeenCalledOnce());
    expect(pending.settled()).toBe(false);
    authorized = false;
    ack.resolve(nativeReceipt());
    expect(await pending.result).toMatchObject({ error: new Error("lease revoked") });
    expect(assertAuthority).toHaveBeenCalledTimes(3);
  });

  it("retains pause fencing before host effect and after an in-flight receipt", async () => {
    const ack = deferred<FabricActorInfo>();
    const stop = vi.fn(async (_request: AgentTopologyStopRequest) => ack.promise);
    const { service, provider } = harness({ topology: topology(stop) });
    const pending = tracked(provider.invoke("stop", { id: peer.id }, context()));
    await vi.waitFor(() => expect(stop).toHaveBeenCalledOnce());
    await service.suspend();
    ack.resolve(nativeReceipt());
    expect(await pending.result).toMatchObject({ error: new Error("Agent service is paused") });
    await expect(provider.invoke("stop", { id: peer.id }, context())).rejects.toThrow("Agent service is paused");
    expect(stop).toHaveBeenCalledOnce();
  });

  it("lets the host own signal cancellation/refusal without publishing early success", async () => {
    const refusal = new Error("native stop observation cancelled");
    const stop = vi.fn((request: AgentTopologyStopRequest) => new Promise<FabricActorInfo>((_resolve, reject) => {
      request.signal!.addEventListener("abort", () => reject(refusal), { once: true });
    }));
    const { provider } = harness({ topology: topology(stop) });
    const controller = new AbortController();
    const pending = tracked(provider.invoke("stop", { id: peer.id }, context(controller.signal)));
    await vi.waitFor(() => expect(stop).toHaveBeenCalledOnce());
    expect(pending.settled()).toBe(false);
    controller.abort();
    expect(await pending.result).toEqual({ error: refusal });
    expect(stop).toHaveBeenCalledOnce();
  });

  it.each([
    { callerId: "forged" }, { explicit: true }, { explicit: false }, { routeId: "forged" }, { generation: 9 },
    { signal: "forged" }, { options: { explicit: true } },
    { wait: false }, { wait: true }, { wait: "invalid" },
    { dropQueued: false }, { dropQueued: true }, { dropQueued: "invalid" },
  ])("rejects forged provider fields, including removed stop options, before effect: %j", async forged => {
    const stop = vi.fn(async (_request: AgentTopologyStopRequest) => nativeReceipt());
    const { client, provider } = harness({ topology: topology(stop) });
    const args = { id: peer.id, ...forged };
    await expect(provider.invoke("stop", args, context())).rejects.toThrow("Invalid hosted agents.stop arguments");
    await expect(client.dispatch("stop", args)).rejects.toThrow("Invalid hosted agents.stop arguments");
    expect(stop).not.toHaveBeenCalled();
  });

  it("keeps direct-child stop on its exact generation/route and waits for stop acknowledgment plus cleanup despite caller cancellation", async () => {
    const entered = deferred<AgentExecutionRequest>();
    const finished = deferred<AgentExecutionResponse>();
    const stopAck = deferred<void>();
    const cleanupAck = deferred<void>();
    // Even a seeded schema regression must leave no held child/cleanup gate behind.
    releases.push(() => { stopAck.resolve(); cleanupAck.resolve(); finished.resolve({ status: "stopped" }); });
    const remoteStop = vi.fn(async (_request: AgentTopologyStopRequest) => nativeReceipt());
    const localStop = vi.fn(async () => stopAck.promise);
    const cleanup = vi.fn(async () => cleanupAck.promise);
    const execute = vi.fn(async (request: AgentExecutionRequest) => {
      entered.resolve(request);
      request.signal.addEventListener("abort", () => finished.resolve({ status: "stopped" }), { once: true });
      return finished.promise;
    });
    const { client, provider } = harness({
      stickyRoutes: ["topology:pair"], topology: topology(remoteStop), port: { execute, stop: localStop, cleanup },
    });
    const child = await client.spawn({ task: "owned child", routingKey: "stable" });
    const execution = await entered.promise;
    const controller = new AbortController();
    const pending = tracked(provider.invoke("stop", { id: child.id }, context(controller.signal)));
    await vi.waitFor(() => expect(localStop).toHaveBeenCalledOnce());
    const identity = { rootId: "root", parentId: "root", id: child.id, generation: 1, routeId: "topology:pair" };
    expect(localStop.mock.calls).toEqual([[identity]]);
    expect(execution.signal.aborted).toBe(true);
    controller.abort();
    expect(pending.settled()).toBe(false);
    expect(cleanup).not.toHaveBeenCalled();
    stopAck.resolve();
    await vi.waitFor(() => expect(cleanup).toHaveBeenCalledOnce());
    expect(cleanup.mock.calls).toEqual([[identity]]);
    expect(pending.settled()).toBe(false);
    cleanupAck.resolve();
    expect(await pending.result).toMatchObject({ value: { id: child.id, status: "stopped", generation: 1, routeId: "topology:pair" } });
    expect(remoteStop).not.toHaveBeenCalled();
  });

  it("never falls back to topology when its own-child stop fails, and still cleans up", async () => {
    const entered = deferred<AgentExecutionRequest>();
    const finished = deferred<AgentExecutionResponse>();
    const remoteStop = vi.fn(async (_request: AgentTopologyStopRequest) => nativeReceipt());
    const cleanup = vi.fn(async () => {});
    const { client, provider } = harness({ topology: topology(remoteStop), port: {
      execute: async request => {
        entered.resolve(request);
        request.signal.addEventListener("abort", () => finished.resolve({ status: "stopped" }), { once: true });
        return finished.promise;
      },
      stop: async () => { throw new Error("own-child stop refused"); }, cleanup,
    } });
    const child = await client.spawn({ task: "owned child" });
    await entered.promise;
    await expect(provider.invoke("stop", { id: child.id }, context())).rejects.toThrow("Agent stop failed");
    expect(cleanup).toHaveBeenCalledOnce();
    expect(remoteStop).not.toHaveBeenCalled();
    await expect(client.status(child.id)).resolves.toMatchObject({ status: "stopped" });
  });

  it("preserves legacy two-argument direct stop and paused-child semantics without remote fallback", async () => {
    const remoteStop = vi.fn(async (_request: AgentTopologyStopRequest) => nativeReceipt());
    const { service, client } = harness({ topology: topology(remoteStop), port: { execute: async () => ({ status: "paused", checkpoint: { private: true } }) } });
    const child = await client.spawn({ task: "pausable" });
    await client.wait(child.id);
    await expect(service.stop("root", child.id)).resolves.toMatchObject({ id: child.id, status: "stopped", generation: 1 });
    expect(remoteStop).not.toHaveBeenCalled();
  });
});
