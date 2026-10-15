import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import http from "node:http";
import { spawnSync } from "node:child_process";
import type { ExtensionAPI, ExtensionContext, ModelRegistry } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import type { AgentRunRequest, AgentRunResult } from "../src/agents/types.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import { FabricModelNotServedError } from "../src/core/model-served-error.js";
import { verifyPiSpawnModel } from "../src/core/model-served.js";
import { QuickJsRuntime } from "../src/runtime/quickjs-runtime.js";
import { AgentCompletionInbox } from "../src/agents/completion-inbox.js";
import { typeCheckFabricCode } from "../src/runtime/type-checker.js";
import { GUEST_TYPE_DECLARATIONS } from "../src/runtime/guest-types.js";
import { rmTempSync } from "./fixtures/temp-cleanup.js";

const roots: string[] = [];
const servers: http.Server[] = [];
const policyManagers: AgentManager[] = [];
afterEach(async () => {
  vi.useRealTimers();
  vi.unstubAllEnvs();
  await Promise.all(policyManagers.splice(0).map(manager => manager.close()));
  await Promise.all(servers.splice(0).map(server => new Promise<void>(resolve => server.close(() => resolve()))));
  for (const root of roots.splice(0)) rmTempSync(root);
});

const gateway = async (response: unknown = { data: [{ id: "a" }, { id: "b" }] }, status = 200) => {
  const requests: { url: string | undefined; auth: string | undefined }[] = [];
  const server = http.createServer((req, res) => {
    requests.push({ url: req.url, auth: req.headers.authorization });
    res.writeHead(status, { "content-type": "application/json" }); res.end(JSON.stringify(response));
  });
  await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
  servers.push(server);
  const address = server.address() as { port: number };
  return { baseUrl: `http://127.0.0.1:${address.port}/v1`, requests };
};

const harness = (baseUrl?: string, ids = ["a", "b", "c"]) => {
  const models = ids.map(id => ({ provider: "fake", id, api: "openai-completions", baseUrl }));
  const registry = {
    getAvailable: () => models, find: (provider: string, id: string) => models.find(model => model.provider === provider && model.id === id),
    refresh: vi.fn(async () => ({ aborted: false, errors: new Map() })),
    getApiKeyAndHeaders: vi.fn(async () => ({ ok: true, apiKey: "offline-fixture-only" })),
  } as unknown as ModelRegistry;
  const requests: AgentRunRequest[] = [];
  const spawn = vi.fn(async (request: AgentRunRequest) => {
    requests.push(request);
    return { id: "child", name: "child", status: "running", runner: "pi", transport: "process", cwd: "." };
  });
  const config: AgentManager["config"] = { ...DEFAULT_FABRIC_CONFIG.agents, transport: "process", deniedModels: [], model: undefined };
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-model-policy-")); roots.push(root);
  // Keep private-brand policy methods on a real instance; only the double spawns.
  const policyManager = new AgentManager(".", config, { runRoot: root, fullCodeMode: false });
  policyManagers.push(policyManager);
  const manager = { config,
    resolveModelReasonPolicy: policyManager.resolveModelReasonPolicy.bind(policyManager),
    assertModelAllowed() {}, defaultModel: () => undefined, resolveKernel: () => undefined,
    resolvePythonRuntime: () => "monty", detachSignal() {}, spawn,
  } as unknown as AgentManager;
  const provider = new AgentsProvider(manager, {} as never, {} as never, {} as never,
    { self: () => ({ id: "session:parent" }), scheduleRefresh() {} } as never, undefined, {} as never);
  const update = vi.fn();
  const context = { cwd: ".", update, extensionContext: { model: models[1], modelRegistry: registry } } as unknown as FabricInvocationContext;
  return { provider, registry, requests, spawn, update, context };
};

const spawnModel = (h: ReturnType<typeof harness>, model: string) =>
  h.provider.invoke("spawn", { task: "offline probe", model, transport: "process", tools: [] }, h.context);

describe("agents.spawn served-model admission", () => {
  it("launches b but refuses stale configured c before any process or placement", async () => {
    const g = await gateway(); const h = harness(g.baseUrl);
    expect(await spawnModel(h, "fake/b")).toMatchObject({ id: "child", modelVerification: "verified" });
    await expect(spawnModel(h, "fake/c")).rejects.toMatchObject({ name: "FabricModelNotServedError", code: "FABRIC_MODEL_NOT_SERVED", provider: "fake", model: "c" });
    expect(h.spawn).toHaveBeenCalledOnce();
    expect(h.requests[0]?.model).toBe("fake/b");
    expect(g.requests).toEqual([{ url: "/v1/models", auth: "Bearer offline-fixture-only" }]);
  });

  it("walks provider to manager to native process: c starts nothing and b runs", async () => {
    const g = await gateway(); const h = harness(g.baseUrl);
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-model-spawn-")); roots.push(root);
    const agentDir = path.join(root, "agent"); fs.mkdirSync(agentDir);
    vi.stubEnv("PI_CODING_AGENT_DIR", agentDir);
    vi.stubEnv("MODEL_NOT_FOUND_ERROR", '429 {"error":{"type":"rate_limit_error","message":"Busy"}}');
    vi.stubEnv("MODEL_NOT_FOUND_ATTEMPTS", path.join(root, "attempts"));
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 8_000, budgetUsd: 0, nice: 19 }, {
      workerPath: path.resolve("dist/worker.js"), piBinary: path.resolve("tests/fixtures/model-not-found-pi.mjs"),
      runRoot: path.join(root, "runs"), fullCodeMode: false,
    });
    const provider = new AgentsProvider(manager, {} as never, {} as never, {} as never,
      { self: () => ({ id: "session:parent" }), scheduleRefresh() {} } as never, undefined, {} as never);
    try {
      await expect(provider.invoke("spawn", { task: "offline task", model: "fake/c", transport: "process", extensions: false, tools: [] }, h.context))
        .rejects.toMatchObject({ code: "FABRIC_MODEL_NOT_SERVED" });
      expect(launch).not.toHaveBeenCalled();
      expect(manager.list()).toEqual([]);
      const receipt = await provider.invoke("spawn", { task: "offline task", model: "fake/b", transport: "process", extensions: false, tools: [] }, h.context) as { id: string; modelVerification: string };
      expect(receipt.modelVerification).toBe("verified");
      expect(launch).toHaveBeenCalledOnce();
      expect(await manager.wait(receipt.id)).toMatchObject({ status: "completed", text: "recovered" });
      expect(fs.readFileSync(path.join(root, "attempts"), "utf8")).toBe("first-turn\nretry\n");
    } finally { await manager.close(); }
  }, 30_000);

  it("returns the same typed refusal for a model absent from Pi's registry", async () => {
    const h = harness(undefined, ["a", "b"]);
    await expect(spawnModel(h, "fake/c")).rejects.toMatchObject({ code: "FABRIC_MODEL_NOT_SERVED", provider: "fake", model: "c" });
    expect(h.spawn).not.toHaveBeenCalled();
  });

  it("preserves Pi's one-refresh admission of a just-added models.json entry", async () => {
    const h = harness(undefined, ["a", "b"]);
    const fresh = { provider: "fake", id: "fresh", api: "fixture", baseUrl: "offline" };
    const models = [...h.registry.getAvailable()];
    Object.assign(h.registry, { getAvailable: () => models,
      find: (provider: string, id: string) => models.find(model => model.provider === provider && model.id === id),
      refresh: vi.fn(async () => { models.push(fresh as never); }) });
    expect(await spawnModel(h, "fake/fresh")).toMatchObject({ modelVerification: "verified" });
    expect(h.registry.refresh).toHaveBeenCalledOnce();
    expect(h.requests[0]?.model).toBe("fake/fresh");
  });

  it("leaves the receipt and progress explicitly unverified when discovery is unreachable", async () => {
    const h = harness("http://127.0.0.1:0/v1");
    expect(await spawnModel(h, "fake/b")).toMatchObject({ id: "child", modelVerification: "unverified" });
    expect(h.spawn).toHaveBeenCalledOnce();
    expect(h.update).toHaveBeenCalledWith(expect.stringContaining("model unverified"));
  });

  it.each([{ error: "outage" }, { data: [{ wrong: "id" }] }, { data: [null] }])("does not turn malformed discovery into a refusal: %j", async body => {
    const g = await gateway(body); const h = harness(g.baseUrl);
    expect(await spawnModel(h, "fake/c")).toMatchObject({ modelVerification: "unverified" });
    expect(h.spawn).toHaveBeenCalledOnce();
  });

  it.each([401, 503])("launches unverified when the model endpoint returns HTTP %s", async status => {
    const g = await gateway({ error: "unavailable" }, status); const h = harness(g.baseUrl);
    expect(await spawnModel(h, "fake/b")).toMatchObject({ modelVerification: "unverified" });
    expect(await spawnModel(h, "fake/c")).toMatchObject({ modelVerification: "unverified" });
    expect(h.spawn).toHaveBeenCalledTimes(2);
    expect(g.requests).toHaveLength(1);
  });

  it("treats an authoritative empty list as no served models, not an outage", async () => {
    const g = await gateway({ data: [] }); const h = harness(g.baseUrl);
    await expect(spawnModel(h, "fake/b")).rejects.toMatchObject({ code: "FABRIC_MODEL_NOT_SERVED" });
    expect(h.spawn).not.toHaveBeenCalled();
  });

  it("coalesces requests and expires the provider/base-URL catalog after ten minutes", async () => {
    const g = await gateway(); const h = harness(g.baseUrl);
    await Promise.all([verifyPiSpawnModel("fake/a", h.registry), verifyPiSpawnModel("fake/b", h.registry)]);
    expect(g.requests).toHaveLength(1);
    const clock = vi.spyOn(Date, "now").mockReturnValue(Date.now() + 10 * 60_000 + 1);
    await verifyPiSpawnModel("fake/b", h.registry);
    expect(g.requests).toHaveLength(2); clock.mockRestore();
  });

  it("does not reuse a provider catalog after its base URL changes", async () => {
    const first = await gateway(); const second = await gateway({ data: [{ id: "c" }] });
    const h = harness(first.baseUrl);
    expect(await verifyPiSpawnModel("fake/b", h.registry)).toBe("verified");
    for (const model of h.registry.getAvailable()) model.baseUrl = second.baseUrl;
    await expect(verifyPiSpawnModel("fake/b", h.registry)).rejects.toMatchObject({ code: "FABRIC_MODEL_NOT_SERVED" });
    expect(first.requests).toHaveLength(1); expect(second.requests).toHaveLength(1);
  });

  it("reuses Pi's provider-scoped refresh when native discovery already exists", async () => {
    const h = harness();
    Object.assign(h.registry, { getProvider: () => ({ refreshModels() {}, getModels: () => [{ id: "a" }, { id: "b" }] }) });
    expect(await verifyPiSpawnModel("fake/b", h.registry)).toBe("verified");
    await expect(verifyPiSpawnModel("fake/c", h.registry)).rejects.toMatchObject({ code: "FABRIC_MODEL_NOT_SERVED" });
    expect(h.registry.refresh).toHaveBeenCalledOnce();
    expect(h.registry.refresh).toHaveBeenCalledWith(expect.objectContaining({ providers: ["fake"], allowNetwork: true }));
    expect(h.registry.getApiKeyAndHeaders).not.toHaveBeenCalled();
  });

  it("launches unverified if Pi's native catalog refresh reports an outage", async () => {
    const h = harness();
    Object.assign(h.registry, { getProvider: () => ({ refreshModels() {}, getModels: () => [] }),
      refresh: async () => ({ aborted: false, errors: new Map([["fake", new Error("offline")]]) }) });
    expect(await spawnModel(h, "fake/b")).toMatchObject({ modelVerification: "unverified" });
    expect(h.spawn).toHaveBeenCalledOnce();
  });

  it("marks discovery unverified when the provider-list facade throws", async () => {
    const h = harness(); Object.assign(h.registry, { getProvider() { throw new Error("offline"); } });
    expect(await spawnModel(h, "fake/b")).toMatchObject({ modelVerification: "unverified" });
    expect(h.spawn).toHaveBeenCalledOnce();
  });

  it("bounds hung auth discovery and caches the unverified result", async () => {
    vi.useFakeTimers(); const h = harness("http://127.0.0.1:0/v1");
    Object.assign(h.registry, { getApiKeyAndHeaders: vi.fn(() => new Promise(() => {})) });
    // Node's AbortSignal.timeout uses real timers; a supplied abort timeout seam records its bound.
    const timeout = vi.spyOn(AbortSignal, "timeout").mockImplementation(ms => {
      const controller = new AbortController(); setTimeout(() => controller.abort(), ms); return controller.signal;
    });
    const pending = verifyPiSpawnModel("fake/b", h.registry);
    await vi.advanceTimersByTimeAsync(3_000);
    expect(await pending).toBe("unverified");
    expect(await verifyPiSpawnModel("fake/c", h.registry)).toBe("unverified");
    expect(h.registry.getApiKeyAndHeaders).toHaveBeenCalledOnce(); timeout.mockRestore();
  });

  it("preserves the typed spawn refusal through the actual QuickJS host bridge", async () => {
    const g = await gateway(); const h = harness(g.baseUrl);
    const result = await new QuickJsRuntime().execute(`
      try { await agents.spawn({task: "offline probe", model: "fake/c", transport: "process"}); }
      catch (error) { return {name: error.name, code: error.code, message: error.message}; }
    `, async (ref, args) => {
      expect(ref).toBe("agents.spawn"); return h.provider.invoke("spawn", args, h.context);
    }, { timeoutMs: 5_000, memoryLimitBytes: 32 * 1024 * 1024 });
    expect(result.error).toBeUndefined();
    expect(result.value).toMatchObject({ name: "FabricModelNotServedError", code: "FABRIC_MODEL_NOT_SERVED", message: expect.stringMatching(/fake.*nothing launched/) });
    expect(h.spawn).not.toHaveBeenCalled();
  });

  it("does not transfer arbitrary host error fields under the model error name", async () => {
    const forged = Object.assign(new Error("spoof"), { name: "FabricModelNotServedError", code: "FABRIC_MODEL_NOT_SERVED", secret: "host-only" });
    const result = await new QuickJsRuntime().execute('try { await agents.spawn({task:"x"}); } catch (error) { return {code: typeof error.code, secret: typeof error.secret}; }', async () => { throw forged; },
      { timeoutMs: 5_000, memoryLimitBytes: 32 * 1024 * 1024 });
    expect(result.value).toEqual({ code: "undefined", secret: "undefined" });
    expect(new FabricModelNotServedError("fake/c").message).toContain('provider "fake"');
  });

  it("exposes the model verification field in guest spawn receipts", () => {
    expect(typeCheckFabricCode('const h = await agents.spawn({task:"x"}); const v: "verified" | "unverified" | undefined = h.modelVerification; return v;', GUEST_TYPE_DECLARATIONS).errors).toEqual([]);
  });
});

describe("real worker first-turn model_not_found", () => {
  const run = (error: string) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-model-served-")); roots.push(root);
    const runDir = path.join(root, "run"); fs.mkdirSync(runDir);
    const agentDir = path.join(root, "agent"); fs.mkdirSync(agentDir);
    const taskFile = path.join(runDir, "task.txt"); fs.writeFileSync(taskFile, "offline task");
    const attempts = path.join(root, "attempts");
    const statusFile = path.join(runDir, "status.json");
    const args = { id: "offline-child", name: "offline-child", runner: "pi", transport: "process", cwd: root,
      "task-file": taskFile, "status-file": statusFile, "log-file": path.join(runDir, "events.jsonl"),
      "lifecycle-file": path.join(runDir, "lifecycle.jsonl"), "pi-binary": path.resolve("tests/fixtures/model-not-found-pi.mjs"),
      "claude-binary": "unused", "veda-binary": "unused", "veda-backend": "unused", "veda-persona": "unused",
      "timeout-ms": "8000", depth: "1", "full-code-mode": "false", extensions: "false", tools: "[]", "granted-risks": "[]", model: "fake/b", nice: "19" };
    const child = spawnSync(process.execPath, [path.resolve("dist/worker.js"), ...Object.entries(args).flatMap(([key, value]) => [`--${key}`, value])], {
      encoding: "utf8", timeout: 15_000, env: { ...process.env, PI_CODING_AGENT_DIR: agentDir,
        MODEL_NOT_FOUND_ERROR: error, MODEL_NOT_FOUND_ATTEMPTS: attempts },
    });
    expect(fs.existsSync(statusFile), child.stderr).toBe(true);
    const result = JSON.parse(fs.readFileSync(statusFile, "utf8")) as AgentRunResult;
    return { child, result, attempts: fs.readFileSync(attempts, "utf8") };
  };

  it.each([
    { status: 400, type: "invalid_request_error", code: "model_not_found" },
    { status: 404, type: "invalid_request_error", code: "model_not_found" },
    { status: 400, type: "model_not_found", code: "not_served" },
    { status: 404, type: "model_not_found", code: "not_served" },
  ])("fails on the first $status $type/$code and tells its spawner", ({ status, type, code }) => {
    const error = `${status} ${JSON.stringify({ error: { type, code, message: "Model b is not served" } })}`;
    const { child, result, attempts } = run(error);
    expect(child.status, child.stderr).toBe(1);
    expect(result).toMatchObject({ status: "failed", error: expect.stringContaining(error), model: "fake/b" });
    expect(attempts).toBe("first-turn\n");
    const handlers = new Map<string, (event: unknown, context: ExtensionContext) => unknown>();
    const sendMessage = vi.fn();
    const pi = { on: (name: string, handler: (event: unknown, context: ExtensionContext) => unknown) => {
      handlers.set(name, handler); return () => handlers.delete(name);
    }, sendMessage } as unknown as ExtensionAPI;
    const context = { hasUI: false, isIdle: () => false, hasPendingMessages: () => false } as unknown as ExtensionContext;
    const inbox = new AgentCompletionInbox(pi, context, vi.fn());
    try {
      inbox.enqueue(result);
      handlers.get("turn_end")!({ message: { role: "assistant", stopReason: "stop" } }, context);
      expect(sendMessage).toHaveBeenCalledOnce();
      expect(sendMessage.mock.calls[0]![0].content).toContain("failed after");
      expect(sendMessage.mock.calls[0]![0].content).toContain(error);
    } finally { inbox.close(); }
  }, 20_000);

  it.each(['429 {"error":{"type":"rate_limit_error","message":"Busy"}}', '400 {"error":{"type":"invalid_request_error","message":"unknown provider for model b"}}'])
    ("still permits native recovery for a transient counterexample: %s", error => {
      const { child, result, attempts } = run(error);
      expect(child.status, child.stderr).toBe(0);
      expect(result).toMatchObject({ status: "completed", text: "recovered" });
      expect(attempts).toBe("first-turn\nretry\n");
    }, 20_000);
});
