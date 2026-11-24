import { createServer, type Server } from "node:http";
import { once } from "node:events";
import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { loadFabricConfig } from "../src/config.js";
import { JevClient, JevCredentials } from "../src/jev/client.js";
import { DEFAULT_JEV_CONFIG, normalizeJevConfig } from "../src/jev/config.js";
import { JEV_OPENROUTER_ROUTE, JEV_TYPESAFE_ROUTE, JEV_VERCEL_ROUTE, resolveJevModelRoute } from "../src/jev/routes.js";
import type { JevRequest } from "../src/jev/types.js";

const request: JevRequest = {
  state: { text: "Synthetic billing request" },
  questions: {
    route: { type: "choice", instructions: "Which team?", criteria: { billing: null, other: null } },
    yes: { type: "noul", instructions: "Is this billing?" },
    score: { type: "score", instructions: "How urgent?", criteria: ["Routine", "Immediate"] },
  },
};
const response = {
  model: "jev-latest",
  answers: {
    route: { type: "choice", choice: "billing", confidence: 1, probabilities: { billing: 1, other: 0 } },
    yes: { type: "noul", noul: 0.9 },
    score: { type: "score", score: 0.1, confidence: 0.9, probabilities: { "0": 0.9, "1": 0.1 }, legend: { "0": "Routine", "1": "Immediate" } },
  },
  usage: { input_tokens: 30, output_tokens: 10 },
};
const signal = () => new AbortController().signal;
const credentials = () => new JevCredentials([], { TYPESAFE_API_KEY: "synthetic-org-key" });

describe("trusted Jev base URL at the HTTP boundary", () => {
  let root: string;
  let server: Server;
  let baseUrl: string;
  let received: { method: string | undefined; url: string | undefined; authorization: string | undefined; body: unknown }[];
  let loopbackFetch: typeof fetch;

  beforeEach(async () => {
    vi.stubEnv("PI_FABRIC_JEV_BASE_URL", "");
    vi.stubEnv("TYPESAFE_API_KEY", "synthetic-org-key");
    root = mkdtempSync(path.join(tmpdir(), "fabric-jev-base-url-"));
    received = [];
    server = createServer((req, res) => {
      const chunks: Buffer[] = [];
      req.on("data", chunk => chunks.push(Buffer.from(chunk)));
      req.on("end", () => {
        received.push({ method: req.method, url: req.url, authorization: req.headers.authorization, body: JSON.parse(Buffer.concat(chunks).toString("utf8")) });
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify(response));
      });
    });
    server.listen(0, "127.0.0.1");
    await once(server, "listening");
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("Missing loopback address");
    baseUrl = `http://127.0.0.1:${address.port}/v1`;
    // Never rewrite the destination. In RED, refuse the old upstream before any
    // real network call; in GREEN, exercise Node fetch and the real HTTP server.
    loopbackFetch = (input, init) => {
      if (!String(input).startsWith(`${baseUrl}/`)) throw new Error("Test refuses non-loopback upstream");
      return fetch(input, init);
    };
  });

  afterEach(async () => {
    vi.unstubAllEnvs();
    server?.closeAllConnections();
    if (server?.listening) await new Promise<void>((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
    if (root) rmSync(root, { recursive: true, force: true });
  });

  function load(host: Record<string, unknown> = {}, project?: Record<string, unknown>, projectTrusted = false) {
    const agentDir = path.join(root, "agent");
    const cwd = path.join(root, "project");
    mkdirSync(agentDir, { recursive: true });
    mkdirSync(path.join(cwd, ".pi"), { recursive: true });
    writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({ jev: host }));
    if (project) writeFileSync(path.join(cwd, ".pi", "fabric.json"), JSON.stringify({ jev: project }));
    return loadFabricConfig({ agentDir, cwd, projectTrusted }).jev;
  }

  async function expectTypedHttp(client: JevClient) {
    expect(await client.evaluate(request, signal())).toEqual(response);
    expect(received).toEqual([{
      method: "POST", url: "/v1/systemone", authorization: "Bearer synthetic-org-key",
      body: { ...request, model: "jev-latest" },
    }]);
  }

  it("retains the TypeSafe default without configuration", async () => {
    const fetched = vi.fn(async () => new Response(JSON.stringify(response))) as unknown as typeof fetch;
    const config = load();
    const client = new JevClient(config, fetched, credentials(), resolveJevModelRoute(config.model).route);
    expect(await client.evaluate(request, signal())).toEqual(response);
    expect(vi.mocked(fetched).mock.calls[0]).toEqual(["https://api.typesafe.ai/v1/systemone", expect.objectContaining({
      method: "POST", redirect: "error", headers: { "Content-Type": "application/json", Authorization: "Bearer synthetic-org-key" },
      body: JSON.stringify({ ...request, model: "jev-latest" }),
    })]);
    expect(JEV_TYPESAFE_ROUTE.endpoint).toBe("https://api.typesafe.ai/v1/systemone");
  });

  it.each(["", "/", "///"])("loads persisted Fabric jev.baseUrl and appends /systemone (suffix %j)", async suffix => {
    const config = load({ baseUrl: `${baseUrl}${suffix}` });
    await expectTypedHttp(new JevClient(config, loopbackFetch, undefined, resolveJevModelRoute(config.model).route));
  });

  it("uses trusted project configuration over global config, but ignores an untrusted project", async () => {
    const config = load({ baseUrl: "http://global.invalid/v1" }, { baseUrl }, true);
    await expectTypedHttp(new JevClient(config, loopbackFetch, undefined, resolveJevModelRoute(config.model).route));
    expect(load({ baseUrl }, { baseUrl: "http://untrusted.invalid/v1" }, false)).toHaveProperty("baseUrl", baseUrl);
  });

  it.each(["normalized", "defaults"])("uses PI_FABRIC_JEV_BASE_URL over config on %s production clients", async mode => {
    vi.stubEnv("PI_FABRIC_JEV_BASE_URL", ` ${baseUrl}/ `);
    const config = mode === "normalized" ? load({ baseUrl: "http://config.invalid/v1" }) : DEFAULT_JEV_CONFIG;
    await expectTypedHttp(new JevClient(config, loopbackFetch, undefined, resolveJevModelRoute(config.model).route));
  });

  it("treats a blank environment override as unset", async () => {
    vi.stubEnv("PI_FABRIC_JEV_BASE_URL", "   ");
    await expectTypedHttp(new JevClient(normalizeJevConfig({ baseUrl }), loopbackFetch, credentials()));
  });

  it.each([JEV_OPENROUTER_ROUTE, JEV_VERCEL_ROUTE])("does not redirect the $id route", async route => {
    vi.stubEnv("PI_FABRIC_JEV_BASE_URL", baseUrl);
    const fetched = vi.fn(async () => new Response(JSON.stringify(response))) as unknown as typeof fetch;
    const client = new JevClient(normalizeJevConfig({ baseUrl: "http://config.invalid/v1" }), fetched, credentials(), route);
    await client.evaluate(request, signal());
    expect(vi.mocked(fetched).mock.calls[0]![0]).toBe(route.endpoint);
  });

  it("preserves an explicitly injected TypeSafe endpoint", async () => {
    vi.stubEnv("PI_FABRIC_JEV_BASE_URL", "http://environment.invalid/v1");
    const config = normalizeJevConfig({ baseUrl: "http://config.invalid/v1" });
    await expectTypedHttp(new JevClient(config, loopbackFetch, credentials(), { ...JEV_TYPESAFE_ROUTE, endpoint: `${baseUrl}/systemone` }));
  });

  it("leaves explicit gateway and dispatch requests unchanged", async () => {
    vi.stubEnv("PI_FABRIC_JEV_BASE_URL", baseUrl);
    const config = normalizeJevConfig({ baseUrl: "http://config.invalid/v1" });
    const fetched = vi.fn() as unknown as typeof fetch;
    const dispatch = vi.fn(async () => response);
    const direct = new JevClient(config, fetched, credentials());
    expect(await direct.evaluate(request, signal(), { dispatch })).toEqual(response);
    expect(dispatch).toHaveBeenCalledWith({ ...request, model: "jev-latest" }, "synthetic-org-key", expect.any(AbortSignal));
    const systemone = vi.fn(async (_use: string, _body: string, _signal: AbortSignal, _expiresAt: number) => ({ status: 200, body: JSON.stringify({ model: "jev-latest", answers: { yes: { noul: 0.9 } } }) }));
    const gateway = new JevClient(config, fetched, new JevCredentials([], {}), undefined, { use: "percall_route", transport: { systemone } });
    const noulRequest = { state: request.state, questions: { yes: request.questions.yes! } };
    expect(await gateway.evaluate(noulRequest, signal(), { dispatch })).toMatchObject({ answers: { yes: { noul: 0.9 } } });
    expect(JSON.parse(systemone.mock.calls[0]![1] as string)).toEqual({ model: "jev-latest", ...noulRequest });
    expect(dispatch).toHaveBeenCalledTimes(1);
    expect(fetched).not.toHaveBeenCalled();
    expect(received).toEqual([]);
  });
});
