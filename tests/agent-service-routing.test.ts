import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AgentService } from "../src/agents/service.js";
import { createAgentServiceClient, createAgentServiceHandler, createAgentsProvider } from "../src/agents/service-provider.js";
import type { AgentControlRequest, AgentExecutionPort, AgentExecutionRequest, AgentExecutionResponse, AgentPrepareRequest, AgentPublicRecord, AgentServiceEvent, AgentServiceOptions } from "../src/agents/service-types.js";
import type { FabricInvocationContext } from "../src/protocol.js";

const deferred = <T>() => {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(yes => { resolve = yes; });
  return { promise, resolve };
};
const services: AgentService[] = [];
const routes = ["topology:designer-reviewer", "topology:builder-checker", "topology:triage-owner"];
const harness = (port: AgentExecutionPort, options: Partial<AgentServiceOptions> = {}) => {
  const service = new AgentService({ rootId: "root", port, ...options });
  services.push(service);
  const forCaller = (callerId: string) => {
    const client = createAgentServiceClient(createAgentServiceHandler(service, callerId), service.capabilities);
    const provider = createAgentsProvider(client);
    return (action: string, args: Record<string, unknown>, signal?: AbortSignal) =>
      Promise.resolve().then(() => provider.invoke(action, args, { signal } as FabricInvocationContext)) as Promise<AgentPublicRecord>;
  };
  return { service, invoke: forCaller("root"), forCaller };
};
// One host port resolves opaque topology IDs to existing pairs. It really uses the
// selected pair for execution and controls; there is no pool of raw execution ports.
const pairHost = () => {
  const trace: Array<{ operation: string; id: string; generation: number; routeId?: string }> = [];
  const pending = new Map<string, ReturnType<typeof deferred<AgentExecutionResponse>>>();
  const requests = new Map<string, AgentExecutionRequest>();
  const prepared = new Map<string, AgentPrepareRequest>();
  const pairFor = (routeId?: string) => routeId === undefined ? "legacy" : routes.includes(routeId) ? `pair:${routeId}` : (() => { throw new Error(`Unknown pair ${routeId}`); })();
  const note = (operation: string, request: AgentControlRequest) => {
    pairFor(request.routeId);
    trace.push({ operation, id: request.id, generation: request.generation, ...(request.routeId !== undefined ? { routeId: request.routeId } : {}) });
  };
  const execute = async (request: AgentExecutionRequest) => {
    note("execute", request);
    expect(request.binding).toEqual({ pair: pairFor(request.routeId) });
    requests.set(request.id, request);
    const result = deferred<AgentExecutionResponse>();
    pending.set(request.id, result);
    request.signal.addEventListener("abort", () => result.resolve({ status: "stopped" }), { once: true });
    if (request.signal.aborted) result.resolve({ status: "stopped" });
    return result.promise;
  };
  const port: AgentExecutionPort = {
    prepare: async request => { note("prepare", request); prepared.set(request.id, request); return { pair: pairFor(request.routeId) }; },
    execute,
    resume: async request => { note("resume", request); return execute(request); },
    steer: async request => { note("steer", request); },
    followUp: async request => { note("followUp", request); },
    compact: async request => { note("compact", request); },
    pause: async request => { note("pause", request); pending.get(request.id)!.resolve({ status: "paused", checkpoint: { pair: pairFor(request.routeId) } }); },
    stop: async request => { note("stop", request); pending.get(request.id)?.resolve({ status: "stopped" }); },
    cleanup: async request => { note("cleanup", request); },
  };
  const entered = async (id: string, generation = 1) => {
    await vi.waitFor(() => expect(requests.get(id)?.generation).toBe(generation), { timeout: 1_000, interval: 5 });
    return requests.get(id)!;
  };
  return { port, trace, pending, prepared, entered, requests };
};
const immediatePort = (): AgentExecutionPort => ({
  prepare: async request => ({ pair: request.routeId ?? "legacy" }),
  execute: async request => ({ status: "completed", text: `${(request.binding as { pair: string }).pair}: ${request.request.task}`, checkpoint: { private: true } }),
  resume: async request => ({ status: "completed", text: `${(request.binding as { pair: string }).pair}: ${request.request.task}` }),
});

beforeEach(() => {
  // Model the authenticated hosted Main, not the task process running Vitest.
  for (const key of ["PI_FABRIC_PARENT_RUN", "PI_FABRIC_ACTOR_ID", "PI_FABRIC_LINEAGE", "PI_FABRIC_DEPTH", "PI_FABRIC_TASK_PROCESS_CHILD", "PI_FABRIC_AGENT_RUN_DIR", "PI_FABRIC_AGENT_NAME", "PI_FABRIC_SPAWNER_KIND", "SMARTY_ROLE"]) vi.stubEnv(key, undefined);
});
afterEach(async () => {
  await Promise.allSettled(services.splice(0).map(service => service.close()));
  vi.unstubAllEnvs();
});

// Keep reasons: host/guest trust boundary, checkpoint data safety, cancellation
// and recursive admission failure paths. All requests traverse the real hosted
// provider -> transport client -> handler -> service -> execution port.
describe("hosted sticky routes", () => {
  it("keeps a PR key stable across successive heads, route reorder and restart/resume", async () => {
    const first = harness(immediatePort(), { stickyRoutes: routes });
    const one = await first.invoke("run", { task: "review PR head a1", routingKey: "org/repo/pull/71" });
    const two = await first.invoke("run", { task: "review PR head b2", routingKey: "org/repo/pull/71" });
    expect(routes).toContain(one.routeId);
    expect(two.routeId).toBe(one.routeId);
    expect(two.text).toBe(`${one.routeId}: review PR head b2`);
    const snapshot = first.service.snapshot();
    const next = harness(immediatePort(), { stickyRoutes: [...routes].reverse(), snapshot });
    const resumed = await next.invoke("resume", { id: one.id, task: "review PR head c3" });
    expect(resumed).toMatchObject({ id: one.id, routeId: one.routeId, generation: 2, text: `${one.routeId}: review PR head c3` });
    expect(await next.invoke("run", { task: "review PR head d4", routingKey: "org/repo/pull/71" })).toMatchObject({ routeId: one.routeId });
    expect(next.service.snapshot().records[0]!.record.routeId).toBe(one.routeId);
    expect(snapshot.records[0]!.record.generation).toBe(1);
    // Existing children keep their host pin even if current config no longer lists it.
    const rebased = harness(immediatePort(), { rootId: "next-root", stickyRoutes: [], snapshot, restorePolicy: "new-root" });
    expect(await rebased.forCaller("next-root")("resume", { id: one.id })).toMatchObject({ routeId: one.routeId, generation: 2, parentId: "next-root" });
  });

  it("distributes different keys, independent of route order", async () => {
    const left = harness(immediatePort(), { stickyRoutes: routes, maxStarts: 32 });
    const right = harness(immediatePort(), { stickyRoutes: [...routes].reverse(), maxStarts: 32 });
    const chosen = new Set<string>();
    for (let index = 0; index < 24; index++) {
      const args = { task: "review", routingKey: `org/repo/pull/${index}` };
      const a = await left.invoke("run", args);
      const b = await right.invoke("run", args);
      expect(routes).toContain(a.routeId);
      expect(b.routeId).toBe(a.routeId);
      chosen.add(a.routeId!);
    }
    expect(chosen.size).toBe(3);
  });

  it("selects before prepare and carries the same pin through controls, pause, cleanup, events and resume", async () => {
    const host = pairHost();
    const events: AgentServiceEvent[] = [];
    const current = harness(host.port, { stickyRoutes: routes, onEvent: event => { events.push(event); } });
    const child = await current.invoke("spawn", { task: "review", routing_key: "org/repo/pull/71", PRIORITY: "P0" });
    const execution = await host.entered(child.id);
    expect(routes).toContain(child.routeId);
    expect(host.prepared.get(child.id)).toMatchObject({ routeId: child.routeId, request: { routingKey: "org/repo/pull/71", priority: "p0" } });
    await execution.emit({ type: "progress", text: "reviewing", routeId: "guest-forgery" } as Parameters<typeof execution.emit>[0]);
    await execution.emit({ type: "checkpoint", checkpoint: { routeId: "nested-guest-data" } });
    for (const action of ["steer", "followUp", "compact"]) await current.invoke(action, { id: child.id, ...(action === "compact" ? { instructions: "keep pin" } : { message: "next head" }) });
    await current.service.suspend();
    expect(execution.signal.aborted).toBe(false);
    const saved = current.service.snapshot();
    expect(saved.records[0]!.record).toMatchObject({ routeId: child.routeId, status: "paused", checkpoint: { pair: `pair:${child.routeId}` } });
    const next = harness(host.port, { snapshot: saved, stickyRoutes: [...routes].reverse(), onEvent: event => { events.push(event); } });
    const continuation = next.invoke("resume", { id: child.id, task: "new head" });
    const continued = await host.entered(child.id, 2);
    expect(continued).toMatchObject({ routeId: child.routeId, checkpoint: { pair: `pair:${child.routeId}` } });
    await next.invoke("stop", { id: child.id });
    expect(await continuation).toMatchObject({ id: child.id, routeId: child.routeId, generation: 2, status: "stopped" });
    expect(host.trace.map(entry => entry.operation)).toEqual(expect.arrayContaining(["prepare", "execute", "steer", "followUp", "compact", "pause", "resume", "stop", "cleanup"]));
    for (const entry of host.trace) expect(entry.routeId).toBe(child.routeId);
    for (const event of events) expect(event.record.routeId).toBe(child.routeId);
    expect(events.map(event => event.sequence)).toEqual([...events].map((_, index) => index + 1));
    expect(await next.invoke("status", { id: child.id })).not.toHaveProperty("checkpoint");
  });

  it("copies config/request once and keeps host metadata safe from port output and snapshot mutation", async () => {
    const config = [...routes];
    const preparing = deferred<AgentPrepareRequest>();
    const gate = deferred<void>();
    const current = harness({
      prepare: async request => { const routeId = request.routeId; preparing.resolve(request); await gate.promise; return routeId; },
      execute: async request => {
        expect(request.routeId).toBe(request.binding);
        expect(request.request).toMatchObject({ routingKey: "original-key", priority: "p0" });
        return { status: "completed", text: "actual pair ran", routeId: "guest-forgery", generation: 999 } as AgentExecutionResponse;
      },
    }, { stickyRoutes: config });
    config.splice(0, config.length, "unconfigured-pair");
    const input = { task: "review", routingKey: "original-key", priority: "p0" };
    const running = current.invoke("run", input);
    const prepared = await Promise.race([preparing.promise, running.then(() => { throw new Error("Execution finished before preparation"); })]);
    expect(routes).toContain(prepared.routeId);
    input.routingKey = "mutated-key"; input.priority = "normal";
    prepared.routeId = "port-local-mutation";
    prepared.request.routingKey = "port-local-mutation";
    gate.resolve();
    const result = await running;
    expect(result).toMatchObject({ status: "completed", text: "actual pair ran", generation: 1 });
    expect(routes).toContain(result.routeId);
    const saved = current.service.snapshot();
    saved.records[0]!.record.routeId = "snapshot-local-mutation";
    expect(await current.invoke("status", { id: result.id })).toMatchObject({ routeId: result.routeId });
    expect(current.service.snapshot().records[0]!.request).toMatchObject({ routingKey: "original-key", priority: "p0" });
    // A successful response's extra fields also cannot overwrite host identity.
    const safe = harness({ execute: async () => ({ status: "completed", routeId: "guest-forgery", generation: 999 } as AgentExecutionResponse) }, { stickyRoutes: routes });
    const completed = await safe.invoke("run", { task: "review", routingKey: "original-key" });
    expect(completed).toMatchObject({ routeId: result.routeId, generation: 1, status: "completed" });
  });

  it("rejects invalid configuration and guest route IDs; requires configured routes for keyed children", async () => {
    for (const stickyRoutes of [[""], [" "], ["same", "same"], [3], "pair", null]) {
      expect(() => harness(immediatePort(), { stickyRoutes } as Partial<AgentServiceOptions>)).toThrow(/stickyRoutes|route/i);
    }
    for (const reservedP0 of [-1, 0.5, 3, NaN, Infinity, null]) {
      expect(() => harness(immediatePort(), { maxConcurrent: 2, reservedP0 } as Partial<AgentServiceOptions>)).toThrow(/reservedP0/);
    }
    const legacy = harness(immediatePort());
    await expect(legacy.invoke("run", { task: "review", routingKey: "key" })).rejects.toThrow(/routingKey.*stickyRoutes|routes.*configured/i);
    const current = harness(immediatePort(), { stickyRoutes: routes });
    for (const extra of [{ routeId: routes[0] }, { priority: "urgent" }, { routingKey: 42 }, { routingKey: "" }, { routingKey: "  " }]) {
      await expect(current.invoke("run", { task: "review", ...extra })).rejects.toThrow(/Invalid hosted|routingKey/);
    }
    expect(current.service.snapshot().starts).toBe(0);
  });
});

describe("hosted explicit P0 reservation", () => {
  it("leaves P0 spare capacity, bypasses a queued normal and preserves the total ceiling", async () => {
    const host = pairHost();
    const { service, invoke } = harness(host.port, { maxConcurrent: 2, reservedP0: 1, stickyRoutes: routes });
    const normal = await invoke("spawn", { task: "normal", routingKey: "key" });
    await host.entered(normal.id);
    const normalQueued = invoke("spawn", { task: "queued normal", priority: "normal" });
    const p0 = await invoke("spawn", { task: "P0", priority: "p0", routingKey: "key" });
    await host.entered(p0.id);
    expect(service.snapshot().starts).toBe(2);
    expect([...host.requests.values()].map(request => request.request.task)).toEqual(["normal", "P0"]);
    const p0Queued = invoke("spawn", { task: "queued P0", priority: "p0" });
    await vi.waitFor(() => expect(host.prepared.size).toBe(4), { timeout: 1_000, interval: 5 });
    expect(service.snapshot().starts).toBe(2);
    host.pending.get(p0.id)!.resolve({ status: "completed" });
    const secondP0 = await p0Queued;
    await host.entered(secondP0.id);
    expect([...host.requests.values()].map(request => request.request.task)).not.toContain("queued normal");
    host.pending.get(normal.id)!.resolve({ status: "completed" });
    const secondNormal = await normalQueued;
    await host.entered(secondNormal.id);
    expect(service.snapshot().starts).toBe(4);
    expect((await invoke("status", { id: secondP0.id })).status).toBe("running");
  });

  it("allows P0 to use shared slots and supports an all-reserved P0-only service", async () => {
    const host = pairHost();
    const current = harness(host.port, { maxConcurrent: 2, reservedP0: 1 });
    const a = await current.invoke("spawn", { task: "a", priority: "p0" });
    const b = await current.invoke("spawn", { task: "b", priority: "p0" });
    await host.entered(a.id); await host.entered(b.id);
    expect(current.service.snapshot().starts).toBe(2);
    const onlyP0 = harness(host.port, { maxConcurrent: 1, reservedP0: 1 });
    const cancel = new AbortController();
    const normal = onlyP0.invoke("spawn", { task: "normal" }, cancel.signal);
    const cancelled = expect(normal).rejects.toThrow(/abort/i);
    const urgent = await onlyP0.invoke("spawn", { task: "urgent", priority: "p0" });
    await host.entered(urgent.id);
    expect(onlyP0.service.snapshot().starts).toBe(1);
    cancel.abort(); await cancelled;
  });

  it.each(["close", "suspend"] as const)("cancels pending normal and P0 admissions on %s without charging starts", async operation => {
    const host = pairHost();
    const { service, invoke } = harness(host.port, { maxConcurrent: 1, reservedP0: 1, stickyRoutes: routes });
    const running = await invoke("spawn", { task: "active", priority: "p0", routingKey: "key" });
    await host.entered(running.id);
    const normal = invoke("spawn", { task: "waiting normal" });
    const urgent = invoke("spawn", { task: "waiting P0", priority: "p0" });
    const checked = Promise.all([expect(normal).rejects.toThrow(/abort/i), expect(urgent).rejects.toThrow(/abort/i)]);
    await vi.waitFor(() => expect(host.prepared.size).toBe(3), { timeout: 1_000, interval: 5 });
    await service[operation]();
    await checked;
    expect(service.snapshot().starts).toBe(1);
    expect(service.snapshot().records).toHaveLength(1);
    expect(service.snapshot().records[0]!.record).toMatchObject({ status: "paused", routeId: running.routeId });
  });

  it("retains recursive per-parent permits and the start/depth fences for P0", async () => {
    let current: ReturnType<typeof harness>;
    current = harness({ execute: async request => {
      const invoke = current.forCaller(request.id);
      if (request.depth === 2) {
        await expect(invoke("run", { task: "too deep", priority: "p0" })).rejects.toThrow(/depth limit/);
        return { status: "completed", text: "leaf" };
      }
      const child = await invoke("run", { task: "recursive", priority: "p0", routingKey: "key" });
      return { status: "completed", text: child.text };
    } }, { maxConcurrent: 1, reservedP0: 1, maxDepth: 2, maxStarts: 2, stickyRoutes: routes });
    expect(await current.invoke("run", { task: "parent", priority: "p0", routingKey: "key" })).toMatchObject({ text: "leaf", status: "completed" });
    await expect(current.invoke("run", { task: "over starts", priority: "p0" })).rejects.toThrow(/start limit/);
    expect(current.service.snapshot().starts).toBe(2);
  });

  it("preserves default unkeyed/no-priority behavior without inferring P0 from task text", async () => {
    const legacy = harness(immediatePort());
    const result = await legacy.invoke("run", { task: "P0 urgent critical review" });
    expect(result).toMatchObject({ status: "completed", text: "legacy: P0 urgent critical review" });
    expect(result).not.toHaveProperty("routeId");
    expect(legacy.service.snapshot().records[0]!.request).not.toHaveProperty("priority");
    const host = pairHost();
    const reserved = harness(host.port, { maxConcurrent: 1, reservedP0: 1 });
    const abort = new AbortController();
    const waiting = reserved.invoke("spawn", { task: "P0 urgent critical review" }, abort.signal);
    const outcome = waiting.then(value => ({ value }), error => ({ error }));
    await vi.waitFor(() => expect(host.prepared.size).toBe(1), { timeout: 1_000, interval: 5 });
    const starts = reserved.service.snapshot().starts;
    abort.abort();
    expect(await outcome).toMatchObject({ error: expect.objectContaining({ message: expect.stringMatching(/abort/i) }) });
    expect(starts).toBe(0);
  });

  it("does not apply session reservation to topology durable dispatch", async () => {
    const host = pairHost();
    const peer = { id: "durable-child", name: "durable", kind: "agent" as const, status: "running", capabilities: [] };
    const current = harness(host.port, { maxConcurrent: 1, reservedP0: 1, topology: {
      self: () => peer, sessions: () => [peer], peers: () => [], deliver: async () => peer,
      dispatch: async request => {
        expect(request.request).toMatchObject({ routingKey: "key", priority: "normal", residency: "durable" });
        return peer;
      },
    } });
    expect(await current.invoke("spawn", { task: "durable", residency: "durable", routingKey: "key", priority: "normal" })).toEqual(peer);
    expect(current.service.snapshot().starts).toBe(0);
    expect(host.trace).toEqual([]);
  });
});
