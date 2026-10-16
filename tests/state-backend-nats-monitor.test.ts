import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";

// #13112: the product adapter's real server() monitor path. Only the NATS connection and JetStream manager are
// replaced; `/varz` and `/jsz?raft=true` are recorded monitor responses served through a stubbed global fetch.
// Shape source: .local/natsflake-hosted-varz-cause.md (NATS 2.14.7 /varz.jetstream without `config` while the
// same node's /jsz has top-level config.sync_always: true).
vi.mock("@nats-io/transport-node", () => ({
  connect: async () => ({ info: { version: "2.14.7", auth_required: false, max_payload: 1024 * 1024 }, drain: async () => undefined }),
}));
vi.mock("@nats-io/jetstream", () => ({
  DeliverPolicy: {}, JetStreamApiError: class extends Error {}, StorageType: {}, jetstream: () => ({}), jetstreamManager: async () => ({}),
}));

const { connectNatsEndpoint, openNatsStateBackend } = await import("../src/mesh/state-backend-nats.js");

const MONITOR = "http://127.0.0.1:8222";
const endpoint = { servers: ["nats://127.0.0.1:4222"], monitoringUrl: MONITOR, replicas: 1 };
const VARZ_WITHOUT_CONFIG = { server_id: "id-2", server_name: "election-2", jetstream: { stats: {}, meta: {}, limits: {} } };
const varzWith = (syncAlways: unknown) => ({ ...VARZ_WITHOUT_CONFIG,
  jetstream: { ...VARZ_WITHOUT_CONFIG.jetstream, config: { sync_always: syncAlways } } });
const jszWith = (syncAlways?: boolean) => ({ server_id: "id-2", meta_cluster: { leader: "election-1", cluster_size: 3 },
  ...(syncAlways === undefined ? {} : { config: { sync_always: syncAlways } }) });
const json = (body: unknown, status = 200) => new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });

type Reply = Response | ((signal: AbortSignal | undefined) => Response | Promise<Response>);
const requests: { path: string; signal: AbortSignal | undefined }[] = [];
const serve = (replies: Record<string, Reply>) => {
  vi.stubGlobal("fetch", async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = new URL(String(input));
    const key = `${url.pathname}${url.search}`;
    const signal = init?.signal ?? undefined;
    requests.push({ path: key, signal });
    const reply = replies[key];
    if (reply === undefined) return json({ error: `unexpected ${key}` }, 404);
    return typeof reply === "function" ? await reply(signal) : reply;
  });
};
const observe = async () => (await connectNatsEndpoint(endpoint)).server.syncAlways;
const paths = () => requests.map(request => request.path);

afterEach(() => { requests.length = 0; vi.unstubAllGlobals(); });

describe("NATS adapter monitor sync_always observation (#13112)", () => {
  it("falls back to /jsz top-level config when /varz omits jetstream.config", async () => {
    serve({ "/varz": json(VARZ_WITHOUT_CONFIG), "/jsz?raft=true": json(jszWith(true)) });
    expect(await observe()).toBe(true);
    expect(paths()).toEqual(["/varz", "/jsz?raft=true"]);
  });

  it("opens past the durability refusal for the hosted varz-missing/jsz-true shape", async () => {
    serve({ "/varz": json(VARZ_WITHOUT_CONFIG), "/jsz?raft=true": json(jszWith(true)) });
    // Any later failure (the fake JetStream manager has no streams API) is fine; the sync_always refusal is not.
    const error = await openNatsStateBackend({ ...endpoint, root: path.join(os.tmpdir(), "nats-monitor-never-created") })
      .then(() => undefined, (caught: unknown) => caught);
    expect(String(error instanceof Error ? error.message : error)).not.toContain("sync_always");
  });

  it("keeps an explicit /varz false authoritative and never requests /jsz", async () => {
    serve({ "/varz": json(varzWith(false)), "/jsz?raft=true": json(jszWith(true)) });
    expect(await observe()).toBe(false);
    expect(paths()).toEqual(["/varz"]);
  });

  it("keeps an explicit /varz true authoritative and never requests /jsz", async () => {
    serve({ "/varz": json(varzWith(true)), "/jsz?raft=true": json(jszWith(false)) });
    expect(await observe()).toBe(true);
    expect(paths()).toEqual(["/varz"]);
  });

  it("reports undefined when both omit the policy, and the open still refuses", async () => {
    serve({ "/varz": json(VARZ_WITHOUT_CONFIG), "/jsz?raft=true": json(jszWith(undefined)) });
    expect(await observe()).toBeUndefined();
    serve({ "/varz": json(VARZ_WITHOUT_CONFIG), "/jsz?raft=true": json(jszWith(undefined)) });
    await expect(openNatsStateBackend({ ...endpoint, root: path.join(os.tmpdir(), "nats-monitor-never-created") }))
      .rejects.toThrow(/sync_always \(got undefined\)/);
  });

  it("does not read camelCase or other guessed paths", async () => {
    serve({ "/varz": json({ jetstream: { config: { syncAlways: true } } }), "/jsz?raft=true": json({ syncAlways: true, jetstream: { config: { sync_always: true } } }) });
    expect(await observe()).toBeUndefined();
  });

  it("rejects a non-OK /varz even when its body looks durable", async () => {
    serve({ "/varz": json(varzWith(true), 503) });
    await expect(observe()).rejects.toThrow(/503/);
    expect(paths()).toEqual(["/varz"]);
  });

  it("rejects a non-OK /jsz fallback instead of reporting an absent policy", async () => {
    serve({ "/varz": json(VARZ_WITHOUT_CONFIG), "/jsz?raft=true": json(jszWith(true), 500) });
    await expect(observe()).rejects.toThrow(/500/);
  });

  it.each([
    ["/varz", { "/varz": new Response("{not json", { status: 200 }) }],
    ["/jsz", { "/varz": json(VARZ_WITHOUT_CONFIG), "/jsz?raft=true": new Response("{not json", { status: 200 }) }],
    ["/varz array", { "/varz": json([]) }],
    ["/jsz non-object", { "/varz": json(VARZ_WITHOUT_CONFIG), "/jsz?raft=true": json("ok") }],
  ] as const)("rejects an invalid JSON body from %s", async (_label, replies) => {
    serve(replies as unknown as Record<string, Reply>);
    await expect(observe()).rejects.toThrow();
  });

  it("shares one 5000 ms monitor budget across /varz and the /jsz fallback", async () => {
    const timeout = vi.spyOn(AbortSignal, "timeout");
    serve({ "/varz": json(VARZ_WITHOUT_CONFIG), "/jsz?raft=true": json(jszWith(true)) });
    expect(await observe()).toBe(true);
    expect(timeout.mock.calls).toEqual([[5_000]]);
    expect(requests).toHaveLength(2);
    expect(requests[0]!.signal).toBeInstanceOf(AbortSignal);
    expect(requests[1]!.signal).toBe(requests[0]!.signal);
  });

  it("fails closed when the shared budget aborts a pending /jsz request", async () => {
    const budget = new AbortController();
    vi.spyOn(AbortSignal, "timeout").mockImplementation(() => budget.signal);
    serve({
      "/varz": json(VARZ_WITHOUT_CONFIG),
      "/jsz?raft=true": signal => new Promise<Response>((_, reject) => {
        signal?.addEventListener("abort", () => reject(signal.reason), { once: true });
        queueMicrotask(() => budget.abort(new DOMException("The operation was aborted due to timeout", "TimeoutError")));
      }),
    });
    await expect(observe()).rejects.toThrow();
    expect(paths()).toEqual(["/varz", "/jsz?raft=true"]);
  });

  it("bounds a /jsz body that ignores the abort signal by the shared budget", async () => {
    const budget = new AbortController();
    vi.spyOn(AbortSignal, "timeout").mockImplementation(() => budget.signal);
    serve({
      "/varz": json(VARZ_WITHOUT_CONFIG),
      "/jsz?raft=true": () => {
        setTimeout(() => budget.abort(new DOMException("The operation was aborted due to timeout", "TimeoutError")), 10);
        return Object.assign(new Response("{}"), { json: () => new Promise<never>(() => {}) });
      },
    });
    const outcome = await Promise.race([
      observe().then(() => "resolved", () => "rejected"),
      new Promise(resolve => setTimeout(() => resolve("hung"), 1_000)),
    ]);
    expect(outcome).toBe("rejected");
  });
});
