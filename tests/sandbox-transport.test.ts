import { afterEach, describe, expect, it } from "vitest";
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { DEFAULT_FABRIC_CONFIG, normalizeFabricConfig } from "../src/config.js";
import { AgentManager } from "../src/agents/manager.js";
import { placementRunIdForParticipant } from "../src/topology/placement-run-id.js";
import { buildSandboxIntent, DEFAULT_SANDBOX_PLACEMENT_URL, SandboxPlacementHttp, SandboxReadyTimeoutError, SandboxSpawnTimeoutError, SandboxTransport, SandboxUnsupportedError, type SandboxBinding, type SandboxMesh, type SandboxPlacement, type SandboxSpawnIntent, type SandboxSpawnResult, type SandboxTaskResult, type SandboxTerminalRecord, type SandboxTransportConfig } from "../src/transport/sandbox/index.js";

const BASE_NOW = Date.parse("2026-02-01T00:00:00Z");
const RUN_ID = "sandbox-run-1";
const PARENT = "session:parent-1";
const CHILD = "session:child-1";
const MODEL = "cliproxyapi/gpt-6.1-sol";
const dirs: string[] = [];
const servers: Server[] = [];
const managers: AgentManager[] = [];

afterEach(async () => {
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  for (const server of servers.splice(0)) {
    server.closeAllConnections();
    await new Promise<void>(resolve => server.close(() => resolve()));
  }
  for (const directory of dirs.splice(0)) fs.rmSync(directory, { recursive: true, force: true });
});

const scratch = (): string => {
  const directory = fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "fv2-sandbox-test-"));
  dirs.push(directory);
  return directory;
};

const result = (runId: string, status: SandboxSpawnResult["status"], overrides: Partial<SandboxSpawnResult> = {}): SandboxSpawnResult => {
  const admitted = status === "admitted";
  const claimRef = admitted ? { namespace: "smarty-island", name: `spawn-${runId}`, uid: "claim-uid-1" } : null;
  return {
    runId,
    status,
    code: status === "admitted" ? "PLACED" : status === "refused" ? "POLICY_REFUSED" : "PLACEMENT_UNAVAILABLE",
    detail: status === "admitted" ? "sandbox admitted" : status === "refused" ? "sandbox refused by policy" : "placement capacity is delayed",
    retryAfterSeconds: status === "delayed" ? 15 : null,
    host: admitted ? "island-node-1" : null,
    claimRef,
    claimUID: claimRef?.uid ?? null,
    podUID: admitted ? "pod-uid-1" : null,
    ...overrides,
  };
};

const bindingFor = (intent: SandboxSpawnIntent): SandboxBinding => {
  const admittedAt = new Date(BASE_NOW).toISOString().replace(".000Z", "Z");
  const rawDeadline = intent.deadline ? Date.parse(intent.deadline) : Number.POSITIVE_INFINITY;
  const executionDeadline = new Date(Math.min(BASE_NOW + intent.expectedSeconds * 1_000, rawDeadline)).toISOString().replace(".000Z", "Z");
  return {
    runId: intent.runId,
    status: "admitted",
    claimRef: { namespace: "smarty-island", name: `spawn-${intent.runId}`, uid: "claim-uid-1" },
    claimUID: "claim-uid-1",
    podUID: "pod-uid-1",
    podName: "sandbox-pod-1",
    host: "island-node-1",
    org: intent.org,
    role: intent.role,
    model: intent.model,
    provider: "cliproxyapi",
    thinking: intent.thinking,
    expectedSeconds: intent.expectedSeconds,
    deadline: intent.deadline,
    executionDeadline,
    admittedAt,
  };
};

interface FakePlacementOptions {
  post: (intent: SandboxSpawnIntent, index: number) => SandboxSpawnResult;
  watch: (runId: string, index: number) => SandboxSpawnResult[];
  rejectProfile?: boolean;
  profileErrorDetail?: string;
  allowAnyRunId?: boolean;
}

const isObject = (value: unknown): value is Record<string, unknown> => typeof value === "object" && value !== null && !Array.isArray(value);
const intentKeys = ["v", "runId", "kind", "org", "parentSession", "role", "profile", "complexity", "model", "thinking", "needs", "repository", "cwd", "expectedSeconds", "timeoutSeconds", "deadline"];
const spawnResultKeys = ["runId", "status", "code", "detail", "retryAfterSeconds", "host", "claimRef", "claimUID", "podUID"];
const bindingKeys = ["runId", "status", "claimRef", "claimUID", "podUID", "podName", "host", "org", "role", "model", "provider", "thinking", "expectedSeconds", "deadline", "executionDeadline", "admittedAt"];

const fakePlacementServer = async (options: FakePlacementOptions): Promise<{ placement: SandboxPlacement; intents: SandboxSpawnIntent[]; requestBodies: Record<string, unknown>[]; watches: string[]; posts: number; gets: number; base: string }> => {
  const intents: SandboxSpawnIntent[] = [];
  const requestBodies: Record<string, unknown>[] = [];
  const watches: string[] = [];
  const current = new Map<string, SandboxSpawnResult>();
  let posts = 0, gets = 0;
  const respond = (res: import("node:http").ServerResponse, status: number, body: unknown, contentType = "application/json") => {
    res.writeHead(status, { "content-type": contentType });
    res.end(typeof body === "string" ? body : JSON.stringify(body));
  };
  const server = createServer(async (req, res) => {
    const url = new URL(req.url ?? "/", "http://127.0.0.1");
    if (req.headers.authorization !== undefined) { respond(res, 401, { code: "UNAUTHENTICATED", detail: "this contract uses no authorization header" }); return; }
    if (req.method === "POST" && url.pathname === "/v1/spawns") {
      let raw = "";
      for await (const chunk of req) raw += chunk.toString();
      const body: unknown = JSON.parse(raw);
      if (!isObject(body)) { respond(res, 400, { code: "INVALID_INTENT", detail: "request does not match SpawnIntent" }); return; }
      requestBodies.push(body);
      const expectedKeys = Object.hasOwn(body, "profile") ? intentKeys : intentKeys.filter(key => key !== "profile");
      if (Object.keys(body).length !== expectedKeys.length || expectedKeys.some(key => !Object.hasOwn(body, key)) ||
        body.v !== 1 || body.kind !== "sandbox" || typeof body.runId !== "string" ||
        !/^[a-z0-9](?:[a-z0-9-]{0,55}[a-z0-9])?$/u.test(body.runId) ||
        !options.allowAnyRunId && body.runId !== RUN_ID ||
        body.profile !== undefined && !["lead", "lane", "task", "org", "actor"].includes(String(body.profile)) ||
        Object.hasOwn(body, "labels") || !Array.isArray(body.needs) || new Set(body.needs).size !== body.needs.length) {
        respond(res, 400, { code: "INVALID_INTENT", detail: "request does not match frozen SpawnIntent" }); return;
      }
      if (options.rejectProfile && body.profile !== undefined) {
        respond(res, 400, { code: "INVALID_INTENT", detail: options.profileErrorDetail ?? "Unknown field profile" }); return;
      }
      const intent = body as unknown as SandboxSpawnIntent;
      intents.push(intent);
      const spawnResult = options.post(intent, posts++);
      if (Object.keys(spawnResult).length !== spawnResultKeys.length) throw new Error("fake result is not the nine-field SpawnResult");
      current.set(intent.runId, spawnResult);
      respond(res, 200, spawnResult);
      return;
    }
    const getMatch = /^\/v1\/spawns\/([^/]+)$/u.exec(url.pathname);
    if (req.method === "GET" && getMatch) {
      gets += 1;
      const snapshot = current.get(decodeURIComponent(getMatch[1]!));
      if (!snapshot) { respond(res, 404, { code: "NOT_FOUND", detail: "unknown run" }); return; }
      respond(res, 200, snapshot);
      return;
    }
    const watchMatch = /^\/v1\/spawns\/([^/]+)\/watch$/u.exec(url.pathname);
    if (req.method === "GET" && watchMatch) {
      const runId = decodeURIComponent(watchMatch[1]!);
      watches.push(runId);
      const events = options.watch(runId, watches.length - 1);
      if (events.length) current.set(runId, events[events.length - 1]!);
      respond(res, 200, events.map(event => JSON.stringify(event)).join("\n") + (events.length ? "\n" : ""), "application/x-ndjson");
      return;
    }
    const bindingMatch = /^\/v1\/spawns\/([^/]+)\/binding$/u.exec(url.pathname);
    if (req.method === "GET" && bindingMatch) {
      const intent = [...intents].reverse().find(value => value.runId === decodeURIComponent(bindingMatch[1]!));
      if (!intent) { respond(res, 404, { code: "NOT_FOUND", detail: "unknown run" }); return; }
      const binding = bindingFor(intent);
      if (Object.keys(binding).length !== bindingKeys.length) throw new Error("fake binding is not the exact sixteen-field projection");
      respond(res, 200, binding);
      return;
    }
    respond(res, 404, { code: "NOT_FOUND", detail: "unknown endpoint" });
  });
  servers.push(server);
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => resolve());
  });
  const address = server.address() as AddressInfo;
  const base = `http://127.0.0.1:${address.port}`;
  return { placement: new SandboxPlacementHttp(base), intents, requestBodies, watches, get posts() { return posts; }, get gets() { return gets; }, base };
};

class FakeTerminal {
  readonly records = new Map<string, SandboxTerminalRecord>();
  async put(key: string, value: SandboxTerminalRecord): Promise<void> { this.records.set(key, structuredClone(value)); }
}

const taskResult = (status: SandboxTaskResult["status"] = "completed", text = "sandbox-result", runId = RUN_ID, participantId = CHILD): SandboxTaskResult => ({
  runId,
  participantId,
  status,
  text,
  exitCode: status === "completed" ? 0 : 1,
});

const config = (enabled = true, readyTimeoutMs = 90_000, placementUrl?: string): SandboxTransportConfig => ({ enabled, template: "smarty-proof-agent", ...(placementUrl ? { placementUrl } : {}), readyTimeoutMs });

const launchRequest = (directory: string, overrides: Partial<Parameters<SandboxTransport["launch"]>[0]> = {}): Parameters<SandboxTransport["launch"]>[0] => {
  const taskFile = path.join(directory, "task.txt");
  const statusFile = path.join(directory, "status.json");
  fs.writeFileSync(taskFile, "Return the exact marker sandbox-result.", { mode: 0o600 });
  return {
    id: RUN_ID,
    name: "sandbox test",
    cwd: process.cwd(),
    workerPath: "not-used-by-sandbox",
    workerArguments: ["--task-file", taskFile, "--status-file", statusFile, "--timeout-ms", "60000", "--model", MODEL, "--thinking", "high"],
    parentSession: PARENT,
    complexity: "normal",
    ...overrides,
  };
};

const meshFor = (resultValue = taskResult()): SandboxMesh & { delivered: Array<{ childSession: string; text: string }>; presenceRunIds: string[]; awaited: Array<{ childSession: string; deadline: number }>; readyTimeouts: number[] } => {
  const delivered: Array<{ childSession: string; text: string }> = [];
  const presenceRunIds: string[] = [];
  const awaited: Array<{ childSession: string; deadline: number }> = [];
  const readyTimeouts: number[] = [];
  return {
    delivered,
    presenceRunIds,
    awaited,
    readyTimeouts,
    waitForPresence: async (runId, options) => { presenceRunIds.push(runId); readyTimeouts.push(options.timeoutMs); return { id: CHILD, placementRunId: runId }; },
    deliverTask: async (childSession, text) => { delivered.push({ childSession, text }); },
    awaitResult: async (childSession, _runId, deadline) => { awaited.push({ childSession, deadline }); return resultValue; },
    stop: async () => ({ confirmed: true }),
  };
};

describe("sandbox agent transport", () => {
  it("admits, binds by run id, delivers via the mesh, returns the final answer, and writes finished KV", async () => {
    const directory = scratch();
    const api = await fakePlacementServer({ post: intent => result(intent.runId, "admitted"), watch: () => [] });
    const mesh = meshFor(taskResult("completed", "exact sandbox-result"));
    const terminal = new FakeTerminal();
    const transport = new SandboxTransport(config(), {
      placement: api.placement, mesh, terminal, meshTransport: "nats", org: "smarty-dev", now: () => BASE_NOW,
      resolveRepository: () => ({ repository: "https://github.com/Smarty-Pants-Inc/smarty-dev", cwd: "src/agents" }),
    });

    const handle = await transport.launch(launchRequest(directory));
    await handle.closed;

    expect(api.intents).toHaveLength(1);
    expect(Object.keys(api.intents[0]!)).toEqual(intentKeys);
    expect(api.intents[0]).toMatchObject({ runId: RUN_ID, kind: "sandbox", org: "smarty-dev", parentSession: PARENT, role: "task-agent", profile: "task", model: MODEL, thinking: "high", repository: "https://github.com/Smarty-Pants-Inc/smarty-dev", cwd: "src/agents", expectedSeconds: 60, timeoutSeconds: 60 });
    expect(api.intents[0]).not.toHaveProperty("labels");
    expect(api.intents[0]).not.toHaveProperty("task");
    expect(mesh.presenceRunIds).toEqual([RUN_ID]);
    expect(mesh.delivered).toHaveLength(1);
    expect(mesh.delivered[0]).toMatchObject({ childSession: CHILD });
    expect(mesh.delivered[0]!.text).toContain("Return the exact marker sandbox-result.");
    expect(mesh.delivered[0]!.text).toContain(`agents.followUp({ id: ${JSON.stringify(PARENT)}`);
    expect(mesh.delivered[0]!.text).toContain(`FABRIC-RESULT ${RUN_ID}\\n`);
    expect(mesh.awaited).toEqual([{ childSession: CHILD, deadline: Date.parse("2026-02-01T00:01:00Z") }]);
    expect(handle.sessionId).toBe(CHILD);
    expect(JSON.parse(fs.readFileSync(path.join(directory, "status.json"), "utf8"))).toMatchObject({ status: "completed", text: "exact sandbox-result", exitCode: 0, transport: "sandbox" });
    expect(terminal.records.get(RUN_ID)).toMatchObject({ placementRunId: RUN_ID, state: "finished", exitCode: 0, at: "2026-02-01T00:00:00Z" });
  });

  it("returns a typed refusal and records never-started", async () => {
    const directory = scratch();
    const api = await fakePlacementServer({ post: intent => result(intent.runId, "refused"), watch: () => [] });
    const terminal = new FakeTerminal();
    const transport = new SandboxTransport(config(), { placement: api.placement, mesh: meshFor(), terminal, meshTransport: "nats", org: "smarty-dev", now: () => BASE_NOW, resolveRepository: () => ({ repository: "https://github.com/Smarty-Pants-Inc/smarty-dev", cwd: "." }) });

    await expect(transport.launch(launchRequest(directory))).rejects.toMatchObject({ name: "SandboxPlacementRefusedError", code: "sandbox.placement.refused", placementCode: "POLICY_REFUSED" });
    expect(terminal.records.get(RUN_ID)).toMatchObject({ placementRunId: RUN_ID, state: "never-started", reason: "placement refused (POLICY_REFUSED)" });
  });

  it("watches a delayed placement until it is admitted, then binds and completes", async () => {
    const directory = scratch();
    const api = await fakePlacementServer({
      post: intent => result(intent.runId, "delayed"),
      watch: runId => [result(runId, "delayed"), result(runId, "admitted")],
    });
    const mesh = meshFor();
    const terminal = new FakeTerminal();
    const transport = new SandboxTransport(config(), { placement: api.placement, mesh, terminal, meshTransport: "nats", org: "smarty-dev", now: () => BASE_NOW, resolveRepository: () => ({ repository: "https://github.com/Smarty-Pants-Inc/smarty-dev", cwd: "." }) });

    const handle = await transport.launch(launchRequest(directory));
    await handle.closed;
    expect(api.watches).toEqual([RUN_ID]);
    expect(mesh.delivered).toHaveLength(1);
    expect(terminal.records.get(RUN_ID)?.state).toBe("finished");
  });

  it("falls back once without profile only when the server names that field, then polls with GET", async () => {
    const directory = scratch();
    let watchCount = 0;
    const api = await fakePlacementServer({
      rejectProfile: true,
      post: intent => result(intent.runId, "delayed"),
      watch: runId => watchCount++ === 0 ? [] : [result(runId, "admitted")],
    });
    const fallbacks: string[] = [];
    const placement = new SandboxPlacementHttp(api.base, runId => fallbacks.push(runId));
    const mesh = meshFor();
    const terminal = new FakeTerminal();
    const transport = new SandboxTransport(config(true, 90_000, api.base), {
      placement, mesh, terminal, meshTransport: "nats", org: "smarty-dev", now: () => BASE_NOW,
      sleep: async () => {},
      resolveRepository: () => ({ repository: "https://github.com/Smarty-Pants-Inc/smarty-dev", cwd: "." }),
    });

    const handle = await transport.launch(launchRequest(directory));
    await handle.closed;
    expect(api.requestBodies).toHaveLength(2);
    expect(api.requestBodies[0]).toHaveProperty("profile", "task");
    expect(api.requestBodies[1]).not.toHaveProperty("profile");
    expect(api.posts).toBe(1);
    expect(api.gets).toBe(1);
    expect(fallbacks).toEqual([RUN_ID]);
    expect(terminal.records.get(RUN_ID)?.state).toBe("finished");
  });

  it("does not retry profile for an unrelated Placement validation error", async () => {
    const api = await fakePlacementServer({ rejectProfile: true, profileErrorDetail: "Invalid repository", post: intent => result(intent.runId, "admitted"), watch: () => [] });
    const fallbackLog: string[] = [];
    const request = launchRequest(scratch());
    const intent = buildSandboxIntent({ runId: RUN_ID, cwd: process.cwd(), needs: [], parentSession: PARENT, org: "smarty-dev", timeoutMs: 60_000, now: BASE_NOW,
      workerArguments: request.workerArguments, resolveRepository: () => ({ repository: "https://github.com/Smarty-Pants-Inc/smarty-dev", cwd: "." }) });
    await expect(new SandboxPlacementHttp(api.base, runId => fallbackLog.push(runId)).requestSpawn(intent)).rejects.toMatchObject({ status: 400 });
    expect(api.requestBodies).toHaveLength(1);
    expect(api.requestBodies[0]).toHaveProperty("profile", "task");
    expect(fallbackLog).toEqual([]);
  });

  it("runs a manager sandbox spawn through the HTTP adapter, NATS mesh, and terminal result", async () => {
    const directory = scratch();
    const api = await fakePlacementServer({ allowAnyRunId: true, post: intent => result(intent.runId, "admitted"), watch: () => [] });
    const terminal = new FakeTerminal();
    const mesh = meshFor();
    mesh.awaitResult = async (childSession, runId, deadline) => {
      mesh.awaited.push({ childSession, deadline });
      return taskResult("completed", "manager round-trip result", runId, childSession);
    };
    const manager = new AgentManager(process.cwd(), {
      ...DEFAULT_FABRIC_CONFIG.agents,
      budgetUsd: 0,
      sandbox: { enabled: true, template: "smarty-proof-agent", placementUrl: api.base, readyTimeoutMs: 90_000 },
    }, {
      runRoot: directory,
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
      projectRoot: process.cwd(),
      spawnerSessionId: PARENT,
      preparePiModel: async model => model ?? MODEL,
      sandbox: { mesh, terminal, meshTransport: "nats", org: "smarty-dev", now: () => BASE_NOW,
        resolveRepository: () => ({ repository: "https://github.com/Smarty-Pants-Inc/smarty-dev", cwd: "." }) },
    });
    managers.push(manager);

    const handle = await manager.spawn({ task: "Return the exact marker from a real manager spawn.", model: MODEL, thinking: "high", transport: "sandbox", extensions: false });
    const final = await manager.wait(handle.id);
    expect(final).toMatchObject({ id: handle.id, status: "completed", text: "manager round-trip result", transport: "sandbox" });
    expect(api.intents[0]).toMatchObject({ runId: handle.id, profile: "task" });
    expect(mesh.presenceRunIds).toEqual([handle.id]);
    expect(mesh.delivered[0]?.text).toContain(`FABRIC-RESULT ${handle.id}\\n`);
    expect(terminal.records.get(handle.id)).toMatchObject({ placementRunId: handle.id, state: "finished", exitCode: 0 });
  });

  it("stops retrying delayed admission at 120 seconds and records never-started", async () => {
    const directory = scratch();
    const api = await fakePlacementServer({ post: intent => result(intent.runId, "delayed"), watch: runId => [result(runId, "delayed")] });
    let now = BASE_NOW;
    const terminal = new FakeTerminal();
    const transport = new SandboxTransport(config(), {
      placement: api.placement, mesh: meshFor(), terminal, meshTransport: "nats", org: "smarty-dev", now: () => now,
      sleep: async milliseconds => { now += milliseconds; },
      resolveRepository: () => ({ repository: "https://github.com/Smarty-Pants-Inc/smarty-dev", cwd: "." }),
    });

    await expect(transport.launch(launchRequest(directory))).rejects.toBeInstanceOf(SandboxSpawnTimeoutError);
    expect(now - BASE_NOW).toBe(120_000);
    expect(api.posts).toBe(1);
    expect(api.gets).toBeGreaterThan(1);
    expect(terminal.records.get(RUN_ID)?.state).toBe("never-started");
  });

  it("returns a typed ready timeout without delivering the task", async () => {
    const directory = scratch();
    const api = await fakePlacementServer({ post: intent => result(intent.runId, "admitted"), watch: () => [] });
    const terminal = new FakeTerminal();
    const mesh = meshFor();
    mesh.waitForPresence = async (runId, options) => { mesh.presenceRunIds.push(runId); mesh.readyTimeouts.push(options.timeoutMs); throw new SandboxReadyTimeoutError(runId, options.timeoutMs); };
    const transport = new SandboxTransport(config(true, 90_000), { placement: api.placement, mesh, terminal, meshTransport: "nats", org: "smarty-dev", now: () => BASE_NOW, resolveRepository: () => ({ repository: "https://github.com/Smarty-Pants-Inc/smarty-dev", cwd: "." }) });

    const readyRequest = launchRequest(directory);
    readyRequest.workerArguments = ["--task-file", path.join(directory, "task.txt"), "--status-file", path.join(directory, "status.json"), "--timeout-ms", "180000", "--model", MODEL, "--thinking", "high"];
    await expect(transport.launch(readyRequest)).rejects.toMatchObject({ name: "SandboxReadyTimeoutError", code: "sandbox.ready.timeout" });
    expect(mesh.readyTimeouts).toEqual([90_000]);
    expect(mesh.delivered).toHaveLength(0);
    expect(terminal.records.get(RUN_ID)?.state).toBe("never-started");
  });

  it("writes failed when the admitted child returns a failed result", async () => {
    const directory = scratch();
    const api = await fakePlacementServer({ post: intent => result(intent.runId, "admitted"), watch: () => [] });
    const terminal = new FakeTerminal();
    const mesh = meshFor({ ...taskResult("failed", "partial answer"), error: "private child diagnostic" });
    const transport = new SandboxTransport(config(), { placement: api.placement, mesh, terminal, meshTransport: "nats", org: "smarty-dev", now: () => BASE_NOW, resolveRepository: () => ({ repository: "https://github.com/Smarty-Pants-Inc/smarty-dev", cwd: "." }) });

    const handle = await transport.launch(launchRequest(directory));
    await handle.closed;
    expect(JSON.parse(fs.readFileSync(path.join(directory, "status.json"), "utf8"))).toMatchObject({ status: "failed", text: "partial answer", error: "Sandbox child reported a failed task", exitCode: 1 });
    expect(terminal.records.get(RUN_ID)).toMatchObject({ placementRunId: RUN_ID, state: "failed", exitCode: 1, reason: "sandbox task did not complete successfully" });
    expect(terminal.records.get(RUN_ID)?.reason).not.toContain("private child diagnostic");
  });

  it("maps a completed response with a nonzero exit code to a failed run", async () => {
    const directory = scratch();
    const api = await fakePlacementServer({ post: intent => result(intent.runId, "admitted"), watch: () => [] });
    const terminal = new FakeTerminal();
    const mesh = meshFor({ ...taskResult("completed"), exitCode: 7 });
    const transport = new SandboxTransport(config(), { placement: api.placement, mesh, terminal, meshTransport: "nats", org: "smarty-dev", now: () => BASE_NOW, resolveRepository: () => ({ repository: "https://github.com/Smarty-Pants-Inc/smarty-dev", cwd: "." }) });

    const handle = await transport.launch(launchRequest(directory));
    await handle.closed;
    expect(JSON.parse(fs.readFileSync(path.join(directory, "status.json"), "utf8"))).toMatchObject({ status: "failed", exitCode: 7 });
    expect(terminal.records.get(RUN_ID)).toMatchObject({ state: "failed", exitCode: 7 });
  });

  it("records a failed terminal outcome without claiming an unconfirmed child has stopped", async () => {
    const directory = scratch();
    const api = await fakePlacementServer({ post: intent => result(intent.runId, "admitted"), watch: () => [] });
    const terminal = new FakeTerminal();
    const mesh = meshFor();
    mesh.awaitResult = async () => { throw new Error("result channel unavailable"); };
    mesh.stop = async () => ({ confirmed: false, reason: "child still present" });
    const transport = new SandboxTransport(config(), { placement: api.placement, mesh, terminal, meshTransport: "nats", org: "smarty-dev", now: () => BASE_NOW, resolveRepository: () => ({ repository: "https://github.com/Smarty-Pants-Inc/smarty-dev", cwd: "." }) });

    const handle = await transport.launch(launchRequest(directory));
    await handle.closed;
    expect(terminal.records.get(RUN_ID)).toMatchObject({ placementRunId: RUN_ID, state: "failed", reason: "sandbox result path failed; remote exit is unconfirmed" });
    expect(JSON.parse(fs.readFileSync(path.join(directory, "status.json"), "utf8"))).toMatchObject({ status: "failed", outcome: "indeterminate" });
  });

  it("refuses sandbox when disabled with a typed unsupported error and terminal record", async () => {
    const directory = scratch();
    const terminal = new FakeTerminal();
    const transport = new SandboxTransport(config(false), { terminal, now: () => BASE_NOW });

    await expect(transport.launch(launchRequest(directory))).rejects.toBeInstanceOf(SandboxUnsupportedError);
    expect(terminal.records.get(RUN_ID)?.state).toBe("never-started");
  });

  it("routes an explicit manager spawn to sandbox and returns typed unsupported while disabled", async () => {
    const directory = scratch();
    const manager = new AgentManager(process.cwd(), {
      ...DEFAULT_FABRIC_CONFIG.agents,
      budgetUsd: 0,
      sandbox: { enabled: false, template: "smarty-proof-agent", readyTimeoutMs: 90_000 },
    }, {
      runRoot: directory,
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
      projectRoot: process.cwd(),
      spawnerSessionId: PARENT,
      preparePiModel: async model => model ?? MODEL,
    });
    managers.push(manager);
    await expect(manager.spawn({ task: "must not run locally", model: MODEL, thinking: "high", transport: "sandbox", extensions: false }))
      .rejects.toMatchObject({ name: "SandboxUnsupportedError", code: "sandbox.unsupported" });
  });

  it("binds only a root presence record to the exact valid Pod-injected run id", () => {
    expect(placementRunIdForParticipant("root", { PI_FABRIC_RUN_ID: RUN_ID })).toBe(RUN_ID);
    expect(placementRunIdForParticipant("agent", { PI_FABRIC_RUN_ID: RUN_ID })).toBeUndefined();
    expect(placementRunIdForParticipant("root", { PI_FABRIC_RUN_ID: "../not-a-run" })).toBeUndefined();
  });

  it("rejects credential-bearing repository URLs before sending an intent", () => {
    const request = launchRequest(scratch());
    expect(() => buildSandboxIntent({
      runId: RUN_ID, cwd: process.cwd(), needs: [], parentSession: PARENT, org: "smarty-dev", timeoutMs: 60_000, now: BASE_NOW,
      workerArguments: request.workerArguments,
      resolveRepository: () => ({ repository: "https://user:secret-token@github.com/Smarty-Pants-Inc/smarty-dev", cwd: "." }),
    })).toThrow(/credential-free HTTPS/);
  });

  it("keeps sandbox disabled by default and validates its host-only placement URL", () => {
    expect(normalizeFabricConfig({}).agents.transport).toBe("process");
    expect(normalizeFabricConfig({ agents: { transport: "sandbox" } }).agents.transport).toBe("sandbox");
    expect(normalizeFabricConfig({}).agents.sandbox).toEqual({ enabled: false, template: "smarty-proof-agent", readyTimeoutMs: 90_000 });
    expect(DEFAULT_SANDBOX_PLACEMENT_URL).toBe("http://smarty-placement.smarty-island.svc:8080");
    expect(normalizeFabricConfig({ agents: { sandbox: { enabled: true, template: "smarty-proof-agent", readyTimeoutMs: 30_000, placementUrl: "http://100.125.29.10:30880" } } }).agents.sandbox)
      .toEqual({ enabled: true, template: "smarty-proof-agent", readyTimeoutMs: 30_000, placementUrl: "http://100.125.29.10:30880" });
    expect(() => normalizeFabricConfig({ agents: { sandbox: { placementUrl: "https://smarty-placement.smarty-island.svc:8443" } } })).toThrow(/plain HTTP|NodePort/);
    expect(() => normalizeFabricConfig({ agents: { sandbox: { placementUrl: "http://user:secret@100.125.29.10:30880" } } })).toThrow(/plain HTTP|NodePort/);
  });
});
