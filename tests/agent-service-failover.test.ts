import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AgentService } from "../src/agents/service.js";
import { createAgentServiceClient, createAgentServiceHandler, createAgentsProvider } from "../src/agents/service-provider.js";
import type { AgentExecutionPort, AgentExecutionRequest, AgentExecutionResponse, AgentPrepareRequest, AgentPublicRecord, AgentServiceOptions } from "../src/agents/service-types.js";
import type { FabricInvocationContext } from "../src/protocol.js";

const services: AgentService[] = [];
const routes = ["worker-a", "worker-b", "worker-c"];

const hosted = (port: AgentExecutionPort, options: Partial<AgentServiceOptions> = {}) => {
  const service = new AgentService({ rootId: "root", port, ...options });
  services.push(service);
  const client = createAgentServiceClient(createAgentServiceHandler(service, "root"), service.capabilities);
  const provider = createAgentsProvider(client);
  const invoke = (action: string, args: Record<string, unknown>) =>
    provider.invoke(action, args, {} as FabricInvocationContext) as Promise<AgentPublicRecord>;
  return { service, invoke };
};

// A minimal host adapter at the production provider -> client -> handler ->
// service -> execution-port boundary. The returned binding is the selected
// opaque host route, and every prepare/execute/resume call is recorded.
const hostPort = (
  readLiveRoutes: () => readonly string[] | Promise<readonly string[]>,
  failPrepareForRoute?: string,
) => {
  const prepared: AgentPrepareRequest[] = [];
  const executed: AgentExecutionRequest[] = [];
  const resumed: AgentExecutionRequest[] = [];
  const prepare = async (request: AgentPrepareRequest) => {
    prepared.push(request);
    if (failPrepareForRoute !== undefined && request.routeId === failPrepareForRoute) throw new Error(`worker died: ${failPrepareForRoute}`);
    return { routeId: request.routeId };
  };
  const response = (request: AgentExecutionRequest): AgentExecutionResponse => {
    const routeId = (request.binding as { routeId?: string }).routeId;
    return { status: "completed", text: `${routeId ?? "legacy"}: ${request.request.task}` };
  };
  const port: AgentExecutionPort = {
    liveRoutes: readLiveRoutes,
    prepare,
    execute: async (request: AgentExecutionRequest) => {
      executed.push(request);
      return response(request);
    },
    resume: async (request: AgentExecutionRequest) => {
      resumed.push(request);
      return response(request);
    },
  };
  return { port, prepared, executed, resumed };
};

beforeEach(() => {
  // Model the authenticated hosted Main, not the task process running Vitest.
  for (const key of ["PI_FABRIC_PARENT_RUN", "PI_FABRIC_ACTOR_ID", "PI_FABRIC_LINEAGE", "PI_FABRIC_DEPTH", "PI_FABRIC_TASK_PROCESS_CHILD", "PI_FABRIC_AGENT_RUN_DIR", "PI_FABRIC_AGENT_NAME", "PI_FABRIC_SPAWNER_KIND", "SMARTY_ROLE"]) vi.stubEnv(key, undefined);
});
afterEach(async () => {
  await Promise.allSettled(services.splice(0).map(service => service.close()));
  vi.unstubAllEnvs();
});

describe("hosted sticky-route failover", () => {
  it("moves a dead worker-a pin for pr/0 to worker-b and persists the new pin through resume", async () => {
    let liveRoutes: readonly string[] = ["worker-a", "worker-b"];
    const host = hostPort(async () => liveRoutes);
    const first = hosted(host.port, { stickyRoutes: ["worker-a", "worker-b"] });
    const initial = await first.invoke("run", { task: "review head one", routingKey: "pr/0" });

    expect(initial.routeId).toBe("worker-a");
    expect(host.prepared).toHaveLength(1);
    expect(host.prepared[0]).toMatchObject({ routeId: "worker-a", request: { routingKey: "pr/0" } });
    expect(host.executed).toHaveLength(1);
    expect(await first.invoke("status", { id: initial.id })).toMatchObject({ routeId: "worker-a" });
    const saved = first.service.snapshot();
    expect(saved.records[0]!.record.routeId).toBe("worker-a");

    liveRoutes = ["worker-b"];
    const restored = hosted(host.port, { snapshot: saved, stickyRoutes: ["worker-a", "worker-b"] });
    const result = await restored.invoke("resume", { id: initial.id, task: "review head two" });

    expect(result).toMatchObject({ id: initial.id, routeId: "worker-b", generation: 2, status: "completed" });
    expect(await restored.invoke("status", { id: initial.id })).toMatchObject({ routeId: "worker-b", generation: 2 });
    expect(restored.service.snapshot().records[0]!.record).toMatchObject({ routeId: "worker-b", generation: 2 });
    // Failover happens before prepare; there is one execution on the replacement,
    // never an attempted execution/replay on the dead worker.
    expect(host.prepared.filter(request => request.generation === 2)).toHaveLength(1);
    expect(host.prepared.filter(request => request.generation === 2)[0]!.routeId).toBe("worker-b");
    expect(host.resumed).toHaveLength(1);
    expect(host.resumed[0]).toMatchObject({ generation: 2, routeId: "worker-b", binding: { routeId: "worker-b" } });
    expect(host.executed).toHaveLength(1);
    expect(saved.records[0]!.record.routeId).toBe("worker-a");
    const persisted = hosted(host.port, { snapshot: restored.service.snapshot(), stickyRoutes: ["worker-a", "worker-b"] });
    expect(await persisted.invoke("status", { id: initial.id })).toMatchObject({ routeId: "worker-b", generation: 2 });
    expect(persisted.service.snapshot().records[0]!.record.routeId).toBe("worker-b");

    const resumedAgain = await persisted.invoke("resume", { id: initial.id, task: "review head three" });
    expect(resumedAgain).toMatchObject({ id: initial.id, routeId: "worker-b", generation: 3, status: "completed" });
    expect(await persisted.invoke("status", { id: initial.id })).toMatchObject({ routeId: "worker-b", generation: 3 });
    expect(persisted.service.snapshot().records[0]!.record).toMatchObject({ routeId: "worker-b", generation: 3 });
    expect(host.prepared.filter(request => request.generation === 3)).toHaveLength(1);
    expect(host.prepared.filter(request => request.generation === 3)[0]!.routeId).toBe("worker-b");
    expect(host.resumed.at(-1)).toMatchObject({ generation: 3, routeId: "worker-b", binding: { routeId: "worker-b" } });
  });

  it("intersects host liveness with configured routes for a fresh keyed selection", async () => {
    const host = hostPort(() => ["worker-a", "worker-b", "worker-c"]);
    const current = hosted(host.port, { stickyRoutes: ["worker-a", "worker-b"] });

    // pr/0 rendezvous-selects worker-a from a/b. worker-c has a higher score,
    // so selecting it would prove the host callback was not intersected with config.
    const result = await current.invoke("run", { task: "review", routingKey: "pr/0" });

    expect(result.routeId).toBe("worker-a");
    expect(host.prepared[0]).toMatchObject({ routeId: "worker-a" });
    expect(host.executed[0]).toMatchObject({ routeId: "worker-a", binding: { routeId: "worker-a" } });
  });

  it("routes fresh keyed work away from worker-a when only worker-b is live", async () => {
    const host = hostPort(() => ["worker-b"], "worker-a");
    const current = hosted(host.port, { stickyRoutes: ["worker-a", "worker-b"] });

    // pr/0 normally selects worker-a; preparing it would reproduce the host's
    // original dead-worker failure instead of reaching the live replacement.
    const result = await current.invoke("run", { task: "review", routingKey: "pr/0" });

    expect(result.routeId).toBe("worker-b");
    expect(host.prepared).toHaveLength(1);
    expect(host.prepared[0]).toMatchObject({ routeId: "worker-b", request: { routingKey: "pr/0" } });
    expect(host.executed).toHaveLength(1);
    expect(host.executed[0]).toMatchObject({ routeId: "worker-b", binding: { routeId: "worker-b" } });
  });

  it("keeps a prior pin that the host still reports live even when config no longer lists it", async () => {
    let liveRoutes: readonly string[] = ["worker-a", "worker-b"];
    const host = hostPort(() => liveRoutes);
    const first = hosted(host.port, { stickyRoutes: ["worker-a", "worker-b"] });
    const initial = await first.invoke("run", { task: "review", routingKey: "pr/0" });
    expect(initial.routeId).toBe("worker-a");

    // worker-a is absent from current config, but host liveness is authoritative
    // for an existing pin and must prevent needless remapping.
    liveRoutes = ["worker-a", "worker-b"];
    const restored = hosted(host.port, { snapshot: first.service.snapshot(), stickyRoutes: ["worker-b"] });
    const result = await restored.invoke("resume", { id: initial.id, task: "continue review" });

    expect(result).toMatchObject({ routeId: "worker-a", generation: 2, status: "completed" });
    expect(host.prepared.at(-1)).toMatchObject({ generation: 2, routeId: "worker-a" });
    expect(host.resumed.at(-1)).toMatchObject({ generation: 2, routeId: "worker-a", binding: { routeId: "worker-a" } });
  });

  it("does not move a key whose rendezvous winner remains live after another route dies", async () => {
    let liveRoutes: readonly string[] = routes;
    const host = hostPort(async () => liveRoutes);
    const current = hosted(host.port, { stickyRoutes: [...routes] });
    const before = await current.invoke("run", { task: "review before removal", routingKey: "pr/10" });

    expect(before.routeId).toBe("worker-b");
    liveRoutes = ["worker-b", "worker-c"];
    const after = await current.invoke("run", { task: "review after removal", routingKey: "pr/10" });

    expect(after.routeId).toBe("worker-b");
    expect(after.id).not.toBe(before.id);
    expect(host.prepared.map(request => request.routeId)).toEqual(["worker-b", "worker-b"]);
    expect(host.executed.map(request => request.routeId)).toEqual(["worker-b", "worker-b"]);
  });

  it("refuses keyed work clearly when no configured route is live, before prepare or admission", async () => {
    const host = hostPort(() => []);
    const current = hosted(host.port, { stickyRoutes: ["worker-a", "worker-b"] });

    await expect(current.invoke("spawn", { task: "review", routingKey: "pr/0" })).rejects.toThrow(/live.*route/i);

    expect(host.prepared).toHaveLength(0);
    expect(host.executed).toHaveLength(0);
    expect(host.resumed).toHaveLength(0);
    expect(current.service.snapshot()).toMatchObject({ starts: 0, records: [] });
  });

  it("preserves legacy unkeyed execution without assigning a route when liveRoutes is empty", async () => {
    const host = hostPort(() => []);
    const current = hosted(host.port);

    const result = await current.invoke("run", { task: "legacy unkeyed work" });

    expect(result).toMatchObject({ status: "completed", text: "legacy: legacy unkeyed work" });
    expect(result).not.toHaveProperty("routeId");
    expect(host.prepared).toHaveLength(1);
    expect(host.prepared[0]).not.toHaveProperty("routeId");
    expect(host.executed).toHaveLength(1);
    expect(host.executed[0]).not.toHaveProperty("routeId");
    expect(current.service.snapshot().starts).toBe(1);
  });
});
