import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { waitForNatsCluster } from "../scripts/nats-cluster-readiness.mjs";

beforeEach(() => { vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "performance", "Date"] }); });
afterEach(() => { vi.useRealTimers(); });

const observe: typeof waitForNatsCluster = async (targets, options) => {
  const result = waitForNatsCluster(targets, options).then(value => ({ value }), error => ({ error }));
  await vi.runAllTimersAsync();
  const outcome = await result;
  if ("error" in outcome) throw outcome.error;
  return outcome.value;
};

type Replica = { name: string; current?: boolean; offline?: boolean };
type NodeFixture = {
  name: string;
  id: string;
  serverName?: string;
  leader?: string | undefined;
  syncAlways?: boolean;
  health?: { full?: string; js?: string };
  routes?: string[];
  routeCount?: number;
  clusterSize?: number;
  replicas?: Replica[] | undefined;
};

const names = ["n1", "n2", "n3"] as const;
const nodes = names.map((name, index) => ({ name, monitorUrl: `http://${name}.test/` }));
const response = (body: unknown, status = 200): Response => ({
  ok: status >= 200 && status < 300,
  status,
  json: async () => body,
} as Response);
const endpoint = (input: RequestInfo | URL): { node: string; path: string } => {
  const url = new URL(String(input));
  return { node: url.hostname.split(".")[0]!, path: `${url.pathname}${url.search}` };
};
const baseCluster = (changes: Partial<NodeFixture>[] = []): NodeFixture[] => {
  const ids = ["id-1", "id-2", "id-3"];
  return names.map((name, index) => {
    const leader = "n1";
    const memberReplicas = names.filter(member => member !== leader).map(member => ({ name: member, current: true, offline: false }));
    return {
      name,
      id: ids[index]!,
      leader,
      syncAlways: true,
      health: { full: "ok", js: "ok" },
      routes: ids.filter((_, peer) => peer !== index),
      routeCount: 2,
      clusterSize: 3,
      // Follower views intentionally omit replicas. The gate uses replicas
      // only from the mapped metadata leader's jsz view.
      replicas: name === leader ? memberReplicas : undefined,
      ...changes[index],
    };
  });
};
const recordedFetch = (fixtures: NodeFixture[], read: (node: NodeFixture, path: string, signal: AbortSignal) => Response | Promise<Response>): typeof fetch =>
  async (input, init) => {
    const { node: name, path } = endpoint(input);
    const node = fixtures.find(candidate => candidate.name === name);
    if (!node) return response({ error: `unknown node ${name}` }, 404);
    return await read(node, path, init?.signal as AbortSignal);
  };
const fetchFor = (fixtures: NodeFixture[]): typeof fetch => recordedFetch(fixtures, (node, path) => {
    if (path === "/healthz") return response({ status: node.health?.full ?? "ok" });
    if (path === "/healthz?js-enabled-only=true") return response({ status: node.health?.js ?? "ok" });
    if (path === "/varz") return response({ server_id: node.id, server_name: node.serverName ?? node.name,
      jetstream: { config: { sync_always: node.syncAlways === true } } });
    if (path === "/routez") return response({ num_routes: node.routeCount ?? node.routes?.length ?? 0,
      routes: (node.routes ?? []).map(remote_id => ({ remote_id })) });
    if (path === "/jsz?raft=true") return response({ meta_cluster: { leader: node.leader,
      cluster_size: node.clusterSize, ...(node.replicas === undefined ? {} : { replicas: node.replicas }) } });
    return response({ error: `unexpected endpoint ${path}` }, 404);
  });
const failure = async (fixtures: NodeFixture[], timeoutMs = 40, fetch: typeof globalThis.fetch = fetchFor(fixtures)): Promise<string> => {
  try {
    await observe(nodes, { fetch, timeoutMs });
    throw new Error("expected readiness failure");
  } catch (error) {
    return error instanceof Error ? error.message : String(error);
  }
};

describe("shared NATS cluster readiness gate", () => {
  it("waits through staggered state, checks both health endpoints, and accepts follower jsz without replicas", async () => {
    let attempt = 0;
    const fixtures = baseCluster();
    const fetch = recordedFetch(fixtures, (node, path) => {
      if (path === "/healthz") {
        if (node.name === "n1") attempt++;
        return response({ status: "ok" });
      }
      if (path === "/healthz?js-enabled-only=true") return response({ status: "ok" });
      const ready = attempt >= 3;
      const state = { ...node, syncAlways: ready || node.name !== "n2" };
      if (path === "/varz") return response({ server_id: state.id, server_name: state.name,
        jetstream: { config: { sync_always: state.syncAlways === true } } });
      if (path === "/routez") return response({ num_routes: 2, routes: state.routes!.map(remote_id => ({ remote_id })) });
      if (path === "/jsz?raft=true") return response({ meta_cluster: { leader: state.leader, cluster_size: 3,
        ...(state.replicas === undefined ? {} : { replicas: state.replicas }) } });
      return response({ error: path }, 404);
    });
    const result = await observe(nodes, { fetch, timeoutMs: 1_000 });
    expect(result.metaLeader).toBe("n1");
    expect(attempt).toBeGreaterThanOrEqual(3);
  });

  it("rejects health without JS while retaining the full health probe", async () => {
    const fixtures = baseCluster();
    const calls: string[] = [];
    const fetch = recordedFetch(fixtures, (node, path) => {
      calls.push(`${node.name}${path}`);
      if (path === "/healthz") return response({ status: "ok" });
      if (path === "/healthz?js-enabled-only=true") return response({ status: "not ok" });
      return fetchFor(fixtures)(`http://${node.name}.test${path}`);
    });
    const error = await failure(fixtures, 35, fetch);
    expect(error).toContain("/healthz?js-enabled-only=true");
    expect(calls).toContain("n1/healthz");
    expect(calls).toContain("n1/healthz?js-enabled-only=true");
  });

  it("rejects a missing metadata leader", async () => {
    const error = await failure(baseCluster(names.map(() => ({ leader: undefined }))));
    expect(error).toContain("metadata leader is missing");
  });

  it("rejects a route pool whose remote IDs collapse to one peer", async () => {
    const error = await failure(baseCluster(names.map(() => ({ routes: ["id-2", "id-2"], routeCount: 2 }))));
    expect(error).toContain("missing distinct peer routes");
    expect(error).toContain("id-3");
  });

  it("rejects disagreeing metadata leaders", async () => {
    const error = await failure(baseCluster([{ leader: "n1" }, { leader: "n2" }, { leader: "n1" }]));
    expect(error).toContain("metadata leaders disagree");
  });

  it("rejects bad server identity", async () => {
    const error = await failure(baseCluster([{}, { serverName: "wrong-name" }, {}]));
    expect(error).toContain("server_name is not n2");
  });

  it.each([
    ["stale", [{ name: "n2", current: false, offline: false }, { name: "n3", current: true, offline: false }]],
    ["offline", [{ name: "n2", current: true, offline: true }, { name: "n3", current: true, offline: false }]],
    ["missing", [{ name: "n2", current: true, offline: false }]],
  ] as const)("rejects %s current-replica state", (_label, replicas) => {
    return failure(baseCluster([{}, { replicas: undefined }, { replicas: undefined }]).map((node, index) =>
      index === 0 ? { ...node, replicas: replicas as unknown as Replica[] } : node), 40).then(error => {
      expect(error).toContain("metadata leader replica");
      expect(error).toContain("diagnostics");
    });
  });

  it("rejects a metadata leader cluster size that cannot place across all nodes", async () => {
    const error = await failure(baseCluster([{ clusterSize: 2 }, {}, {}]));
    expect(error).toContain("cluster_size is not 3");
  });

  it("includes every node response when a monitor request fails", async () => {
    const fixtures = baseCluster();
    const fetch = recordedFetch(fixtures, (node, path) => {
      if (node.name === "n2" && path === "/varz") return response({ error: "server unavailable" }, 503);
      return fetchFor(fixtures)(`http://${node.name}.test${path}`);
    });
    let error = "";
    try { await observe(nodes, { fetch, timeoutMs: 35 }); }
    catch (caught) { error = caught instanceof Error ? caught.message : String(caught); }
    expect(error).toContain('"name":"n1"');
    expect(error).toContain('"name":"n2"');
    expect(error).toContain('"name":"n3"');
    expect(error).toContain('"server":{"error":"HTTP 503"}');
  });

  it("bounds a hung response body by the absolute deadline", async () => {
    const hungFetch: typeof globalThis.fetch = async () => Object.assign(new Response("{}"), {
      json: () => new Promise<never>(() => {}),
    });
    const started = Date.now();
    const error = await failure(baseCluster(), 45, hungFetch).catch(value => String(value));
    expect(Date.now() - started).toBe(45);
    expect(vi.getTimerCount()).toBe(0);
    expect(error).toContain("NATS cluster not formed");
  });

  it("does not require leader, routes, or jsz for a single node", async () => {
    const single = [{ name: "solo", monitorUrl: "http://solo.test/" }];
    const fixture = [{ name: "solo", id: "solo-id", syncAlways: true, health: { full: "ok", js: "ok" } }];
    const result = await observe(single, { fetch: fetchFor(fixture), timeoutMs: 100 });
    expect(result.nodes).toHaveLength(1);
    expect(result.metaLeader).toBeUndefined();
  });
});
