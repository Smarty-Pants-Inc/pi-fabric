import fs from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { JevClient, JevCredentials, JEV_GATEWAY_USES, type JevGatewayUse } from "../src/jev/client.js";
import { JevProvider } from "../src/providers/jev-provider.js";
import { ActionRegistry } from "../src/core/action-registry.js";
import { normalizeFabricConfig } from "../src/config.js";
import { DEFAULT_JEV_CONFIG, normalizeJevConfig } from "../src/jev/config.js";
import { checkGatewaySocket, createGatewaySocketTransport, type JevGatewayTransport } from "../src/jev/gateway-transport.js";
import type { JevRequest } from "../src/jev/types.js";

const request: JevRequest = { state: { ops: ["bash"] }, questions: { simple: { type: "noul", instructions: "Is it simple?" } } };
const signal = () => new AbortController().signal;
/** A credential source that fails the test if the gateway path ever reads a key. */
const noKey = () => new JevCredentials([], {}, { configured: () => true, resolve: async () => { throw new Error("credential read"); } });
const bound = (transport?: JevGatewayTransport) => ({ use: "percall_route" as const, ...(transport ? { transport } : {}) });
/** A direct-route TypeSafe reply for `request`. */
const directReply = () => new Response(JSON.stringify({ model: "jev-1.13.0", answers: { simple: { type: "noul", noul: 0.42 } }, usage: { input_tokens: 3, output_tokens: 1 } }));

describe("Jev gateway transport (test double)", () => {
  it("sends the exact typed body to the gateway's systemone op and reads its Noul projection; no credential", async () => {
    const calls: Array<{ use: string; body: string }> = [];
    const double: JevGatewayTransport = { systemone: async (use, body) => {
      calls.push({ use, body });
      return { status: 200, body: JSON.stringify({ model: "jev-1.13.0", answers: { simple: { noul: 0.93 } } }) };
    } };
    const fetcher = vi.fn() as unknown as typeof fetch;
    const client = new JevClient({ ...DEFAULT_JEV_CONFIG, model: "jev-1.13.0" }, fetcher, noKey(), undefined, bound(double));
    expect(client.viaGateway).toBe(true);
    expect(await client.evaluate(request, signal(), { use: "percall_route" })).toEqual({
      model: "jev-1.13.0", answers: { simple: { type: "noul", noul: 0.93 } }, usage: { input_tokens: 0, output_tokens: 0 } });
    expect(calls).toHaveLength(1);
    expect(calls[0]!.use).toBe("percall_route");
    expect(JSON.parse(calls[0]!.body)).toEqual({ model: "jev-1.13.0", state: request.state, questions: request.questions });
    expect(fetcher).not.toHaveBeenCalled();
  });

  it("refuses non-Noul questions before sending, and rejects HTTP errors, unpinned models and bad answers", async () => {
    const reply = vi.fn(async () => ({ status: 200, body: "" }));
    const client = new JevClient(DEFAULT_JEV_CONFIG, undefined, noKey(), undefined, bound({ systemone: reply }));
    await expect(client.evaluate({ state: "x", questions: { c: { type: "choice", instructions: "which", criteria: { a: null, b: null } } } }, signal()))
      .rejects.toThrow(/Noul questions only/);
    expect(reply).not.toHaveBeenCalled();
    reply.mockResolvedValueOnce({ status: 429, body: "" });
    await expect(client.evaluate(request, signal())).rejects.toThrow(/^Jev gateway HTTP 429: rate limited/);
    reply.mockResolvedValueOnce({ status: 200, body: JSON.stringify({ model: "other", answers: { simple: { noul: 0.5 } } }) });
    await expect(client.evaluate(request, signal())).rejects.toThrow(/unpinned model/);
    reply.mockResolvedValueOnce({ status: 200, body: JSON.stringify({ model: "jev-1.13.0", answers: {} }) });
    await expect(client.evaluate(request, signal())).rejects.toThrow(/invalid typed response/);
    // A bound client spends only its own use; a caller cannot name another one per request.
    await expect(client.evaluate(request, signal(), { use: "fabric" })).rejects.toThrow(/use mismatch: this client is bound to percall_route/);
    await expect(client.evaluate(request, signal(), { use: "../x" })).rejects.toThrow(/use mismatch/);
    expect(reply).toHaveBeenCalledTimes(3);
  });

  it("a caller deadline caps the gateway's expiresAt and the local timeout; without one, jev.requestTimeoutMs applies", async () => {
    const expires: number[] = [];
    const double: JevGatewayTransport = { systemone: async (_use, _body, _signal, expiresAt) => {
      expires.push(expiresAt);
      return { status: 200, body: JSON.stringify({ model: "jev-1.13.0", answers: { simple: { noul: 0.5 } } }) };
    } };
    const client = new JevClient({ ...DEFAULT_JEV_CONFIG, model: "jev-1.13.0", requestTimeoutMs: 120_000 }, undefined, noKey(), undefined, bound(double));
    const before = Date.now();
    await client.evaluate(request, signal(), { deadline: before + 2_500 });
    await client.evaluate(request, signal());
    expect(expires[0]).toBe(before + 2_500);
    expect(expires[1]! - before).toBeGreaterThanOrEqual(120_000);
    // A deadline later than the request timeout never extends it.
    await client.evaluate(request, signal(), { deadline: Date.now() + 10 * 60_000 });
    expect(expires[2]! - Date.now()).toBeLessThanOrEqual(120_000);
  });

  it("jev.gatewaySocket is an optional absolute path", () => {
    expect(normalizeJevConfig({}).gatewaySocket).toBeUndefined();
    expect(normalizeJevConfig({ gatewaySocket: "/srv/org/state/jev-gateway.sock" }).gatewaySocket).toBe("/srv/org/state/jev-gateway.sock");
    expect(normalizeJevConfig({ gatewaySocket: "state/jev.sock" }).gatewaySocket).toBeUndefined();
    expect(new JevClient(DEFAULT_JEV_CONFIG).viaGateway).toBe(false);
    // The socket is opt-in per use: configuring it alone reroutes no client.
    expect(new JevClient({ ...DEFAULT_JEV_CONFIG, gatewaySocket: "/nonexistent/jev.sock" }).viaGateway).toBe(false);
    expect(new JevClient({ ...DEFAULT_JEV_CONFIG, gatewaySocket: "/nonexistent/jev.sock" }, undefined, undefined, undefined, bound()).viaGateway).toBe(true);
  });

  it("only registered gateway uses may bind; other uses and a missing socket are refused explicitly", () => {
    expect([...JEV_GATEWAY_USES]).toEqual(["percall_route"]);
    const config = { ...DEFAULT_JEV_CONFIG, gatewaySocket: "/nonexistent/jev.sock" };
    for (const use of ["fabric", "approvals", "jev_provider", "../x"]) {
      expect(() => new JevClient(config, undefined, undefined, undefined, { use: use as JevGatewayUse }))
        .toThrow(/is refused: jev.gatewaySocket serves only percall_route/);
    }
    expect(() => new JevClient(DEFAULT_JEV_CONFIG, undefined, undefined, undefined, bound())).toThrow(/jev.gatewaySocket is unset/);
  });

  it("with jev.gatewaySocket configured, the Jev provider (programs, jev.evaluate) keeps its direct client", async () => {
    const config = normalizeFabricConfig({ jev: { gatewaySocket: "/srv/org/state/jev-gateway.sock" } });
    expect(config.jev.gatewaySocket).toBe("/srv/org/state/jev-gateway.sock");
    const provider = new JevProvider({ registry: new ActionRegistry(), config });
    expect(provider.client.viaGateway).toBe(false);
  });
});

describe.skipIf(process.platform === "win32")("Jev gateway Unix socket (a local test-double server)", () => {
  let dir: string;
  let socket: string;
  let server: net.Server;
  let lines: string[];
  let answer: (line: string) => string;
  beforeEach(async () => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), "jev-gw-"));
    socket = path.join(dir, "jev-gateway.sock");
    lines = [];
    answer = () => JSON.stringify({ ok: true, status: 200, body: JSON.stringify({ model: "jev-1.13.0", answers: { simple: { noul: 0.97 } } }) });
    server = net.createServer(conn => {
      let buffer = "";
      conn.setEncoding("utf8");
      conn.on("data", chunk => {
        buffer += chunk;
        const at = buffer.indexOf("\n");
        if (at < 0) return;
        const line = buffer.slice(0, at);
        lines.push(line);
        conn.end(`${answer(line)}\n`);
      });
    });
    await new Promise<void>(resolve => server.listen(socket, resolve));
    fs.chmodSync(socket, 0o660);
  });
  afterEach(async () => {
    await new Promise<void>(resolve => server.close(() => resolve()));
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it("with jev.gatewaySocket configured, a non-router Jev client keeps the direct route and never touches the socket", async () => {
    const fetcher = vi.fn(async () => directReply()) as unknown as typeof fetch;
    const client = new JevClient({ ...DEFAULT_JEV_CONFIG, model: "jev-1.13.0", gatewaySocket: socket }, fetcher,
      new JevCredentials([], { TYPESAFE_API_KEY: "test-only-never-a-real-key" }));
    expect(client.viaGateway).toBe(false);
    const response = await client.evaluate(request, signal(), { use: "percall_route" });
    expect(response.answers.simple).toEqual({ type: "noul", noul: 0.42 });
    expect(vi.mocked(fetcher).mock.calls[0]![0]).toBe("https://api.typesafe.ai/v1/systemone");
    expect(lines).toHaveLength(0);
  });

  it("speaks the gateway's line protocol through jev.gatewaySocket", async () => {
    const client = new JevClient({ ...DEFAULT_JEV_CONFIG, model: "jev-1.13.0", gatewaySocket: socket }, undefined, noKey(), undefined, bound());
    const response = await client.evaluate(request, signal(), { use: "percall_route" });
    expect(response.answers.simple).toEqual({ type: "noul", noul: 0.97 });
    expect(lines).toHaveLength(1);
    const sent = JSON.parse(lines[0]!) as Record<string, unknown>;
    expect(Object.keys(sent).sort()).toEqual(["body", "expiresAt", "op", "use"]);
    expect(sent).toMatchObject({ op: "systemone", use: "percall_route" });
    expect(typeof sent.expiresAt).toBe("number");
    expect(lines[0]).not.toMatch(/Bearer|API_KEY/i);
  });

  it("surfaces a gateway refusal and does not retry", async () => {
    answer = () => JSON.stringify({ ok: false, error: "use does not allow raw requests" });
    const transport = createGatewaySocketTransport(socket);
    await expect(transport.systemone("percall_route", "{}", signal(), Date.now() + 1000)).rejects.toThrow(/refused: use does not allow raw requests/);
    expect(lines).toHaveLength(1);
  });

  it("a gateway that never answers: the deadline destroys the socket (connect, write and read are all bounded)", async () => {
    let closed = 0;
    server.on("connection", conn => conn.on("close", () => { closed++; }));
    answer = () => "";
    server.removeAllListeners("connection");
    server.on("connection", conn => {
      conn.on("data", chunk => lines.push(String(chunk)));
      conn.on("close", () => { closed++; });
    });
    const started = Date.now();
    const transport = createGatewaySocketTransport(socket);
    const error = await transport.systemone("percall_route", "{}", signal(), started + 300).catch((e: unknown) => e);
    expect(error).toMatchObject({ name: "JevGatewayError", unsettled: true });
    expect(String(error)).toMatch(/timed out/);
    expect(Date.now() - started).toBeLessThan(1_500);
    await new Promise<void>(resolve => setTimeout(resolve, 50));
    expect(closed).toBe(1);
    expect(JSON.parse(lines[0]!.trim())).toMatchObject({ op: "systemone", expiresAt: started + 300 });
    // A connect that never completes is bounded by the same deadline and never writes.
    let stuck: net.Socket | undefined;
    const never = createGatewaySocketTransport(socket, () => (stuck = new net.Socket()));
    const late = await never.systemone("percall_route", "{}", signal(), Date.now() + 200).catch((e: unknown) => e);
    expect(late).toMatchObject({ name: "JevGatewayError", unsettled: false });
    expect(stuck!.destroyed).toBe(true);
    // An expired deadline never connects.
    const connect = vi.fn(() => new net.Socket());
    await expect(createGatewaySocketTransport(socket, connect).systemone("percall_route", "{}", signal(), Date.now() - 1)).rejects.toThrow(/timed out before sending/);
    expect(connect).not.toHaveBeenCalled();
  });

  it("checks the socket before connecting: mode, type and symlinks", () => {
    expect(checkGatewaySocket(socket)).toBe(socket);
    fs.chmodSync(socket, 0o666);
    expect(() => checkGatewaySocket(socket)).toThrow(/mode 0600 or 0660/);
    fs.chmodSync(socket, 0o600);
    const link = path.join(dir, "link.sock");
    fs.symlinkSync(socket, link);
    expect(() => checkGatewaySocket(link)).toThrow(/not a socket/);
    const file = path.join(dir, "plain");
    fs.writeFileSync(file, "");
    expect(() => checkGatewaySocket(file)).toThrow(/not a socket/);
    expect(() => checkGatewaySocket(path.join(dir, "missing.sock"))).toThrow(/does not exist/);
    expect(() => checkGatewaySocket("relative.sock")).toThrow(/absolute/);
  });
});
