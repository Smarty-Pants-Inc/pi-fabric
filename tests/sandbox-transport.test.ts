import { afterEach, describe, expect, it, vi } from "vitest";
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { DEFAULT_FABRIC_CONFIG, normalizeFabricConfig } from "../src/config.js";
import { diagnostics } from "../src/core/diagnostics.js";
import { AgentManager } from "../src/agents/manager.js";
import { normalizeAgentRunRequest } from "../src/agents/request.js";
import { SandboxPlacementHttpError } from "../src/transport/sandbox/placement-http.js";
import { placementRunIdForParticipant } from "../src/topology/placement-run-id.js";
import { captureSandboxFinalAnswer, waitForSandboxFinalAnswer } from "../src/agents/completion-inbox.js";
import { createSandboxFabricMesh } from "../src/transport/sandbox/fabric-mesh.js";
import { buildSandboxIntent, DEFAULT_SANDBOX_PLACEMENT_URL, SandboxPlacementHttp, SandboxSessionImportUnsupportedError, SandboxPodPendingTimeoutError, SandboxReadyTimeoutError, SandboxSpawnTimeoutError, SandboxTransport, SandboxUnsupportedError, type SandboxBinding, type SandboxMesh, type SandboxPlacement, type SandboxSpawnIntent, type SandboxSpawnResult, type SandboxTaskResult, type SandboxTerminalRecord, type SandboxTransportConfig } from "../src/transport/sandbox/index.js";

const BASE_NOW = Date.parse("2026-02-01T00:00:00Z");
const RUN_ID = "sandbox-run-1";
const PARENT = "session:parent-1";
const CHILD = "session:child-1";
const MODEL = "cliproxyapi/gpt-6.1-sol";
const dirs: string[] = [];
const servers: Server[] = [];
const managers: AgentManager[] = [];

afterEach(async () => {
  vi.restoreAllMocks();
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

const bindingFor = (intent: SandboxSpawnIntent, namespace = "smarty-island"): SandboxBinding => {
  const admittedAt = new Date(BASE_NOW).toISOString().replace(".000Z", "Z");
  const rawDeadline = intent.deadline ? Date.parse(intent.deadline) : Number.POSITIVE_INFINITY;
  const executionDeadline = new Date(Math.min(BASE_NOW + intent.expectedSeconds * 1_000, rawDeadline)).toISOString().replace(".000Z", "Z");
  return {
    runId: intent.runId,
    status: "admitted",
    claimRef: { namespace, name: `spawn-${intent.runId}`, uid: "claim-uid-1" },
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
  binding?: (intent: SandboxSpawnIntent, index: number) => SandboxBinding;
  rejectProfile?: boolean;
  profileErrorDetail?: string;
  spawnError?: { code: string; detail: string };
  allowAnyRunId?: boolean;
}

const isObject = (value: unknown): value is Record<string, unknown> => typeof value === "object" && value !== null && !Array.isArray(value);
const intentKeys = ["v", "runId", "kind", "org", "parentSession", "role", "profile", "complexity", "model", "thinking", "needs", "repository", "cwd", "expectedSeconds", "timeoutSeconds", "deadline"];
const spawnResultKeys = ["runId", "status", "code", "detail", "retryAfterSeconds", "host", "claimRef", "claimUID", "podUID"];
const bindingKeys = ["runId", "status", "claimRef", "claimUID", "podUID", "podName", "host", "org", "role", "model", "provider", "thinking", "expectedSeconds", "deadline", "executionDeadline", "admittedAt"];

const fakePlacementServer = async (options: FakePlacementOptions): Promise<{ placement: SandboxPlacement; intents: SandboxSpawnIntent[]; requestBodies: Record<string, unknown>[]; watches: string[]; posts: number; gets: number; bindingReads: number; base: string }> => {
  const intents: SandboxSpawnIntent[] = [];
  const requestBodies: Record<string, unknown>[] = [];
  const watches: string[] = [];
  const current = new Map<string, SandboxSpawnResult>();
  let posts = 0, gets = 0, bindingReads = 0;
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
      const expectedKeys = [...(Object.hasOwn(body, "profile") ? intentKeys : intentKeys.filter(key => key !== "profile")), ...(Object.hasOwn(body, "branch") ? ["branch"] : []), ...(Object.hasOwn(body, "sessionImport") ? ["sessionImport"] : [])];
      if (Object.keys(body).length !== expectedKeys.length || expectedKeys.some(key => !Object.hasOwn(body, key)) ||
        body.v !== 1 || body.kind !== "sandbox" || typeof body.runId !== "string" ||
        !/^[a-z0-9](?:[a-z0-9-]{0,55}[a-z0-9])?$/u.test(body.runId) ||
        !options.allowAnyRunId && body.runId !== RUN_ID ||
        body.profile !== undefined && !["lead", "lane", "task", "org", "actor"].includes(String(body.profile)) ||
        Object.hasOwn(body, "labels") || !Array.isArray(body.needs) || new Set(body.needs).size !== body.needs.length) {
        respond(res, 400, { code: "INVALID_INTENT", detail: "request does not match frozen SpawnIntent" }); return;
      }
      if (options.spawnError) { respond(res, 400, options.spawnError); return; }
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
      const binding = options.binding?.(intent, bindingReads) ?? bindingFor(intent);
      bindingReads += 1;
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
  return { placement: new SandboxPlacementHttp(base), intents, requestBodies, watches, get posts() { return posts; }, get gets() { return gets; }, get bindingReads() { return bindingReads; }, base };
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

const config = (enabled = true, readyTimeoutMs = 90_000, placementUrl?: string, namespace?: string): SandboxTransportConfig => ({ enabled, template: "smarty-proof-agent", ...(placementUrl ? { placementUrl } : {}), ...(namespace !== undefined ? { namespace } : {}), readyTimeoutMs });

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

const transportForNamespace = (api: Awaited<ReturnType<typeof fakePlacementServer>>, namespace?: string) => {
  const mesh = meshFor();
  const terminal = new FakeTerminal();
  const transport = new SandboxTransport(config(true, 90_000, undefined, namespace), {
    placement: api.placement, mesh, terminal, meshTransport: "nats", org: "smarty-dev", now: () => BASE_NOW,
    resolveRepository: () => ({ repository: "https://github.com/Smarty-Pants-Inc/smarty-dev", cwd: "." }),
  });
  return { transport, mesh, terminal };
};

describe("sandbox agent transport", () => {
  it("accepts the default smarty-island claim namespace and completes the sandbox round-trip", async () => {
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
    expect(api.bindingReads).toBe(1);
    expect(mesh.presenceRunIds).toEqual([RUN_ID]);
    expect(mesh.delivered).toHaveLength(1);
    expect(mesh.delivered[0]).toMatchObject({ childSession: CHILD });
    expect(mesh.delivered[0]!.text).toContain(`FABRIC-SANDBOX-TASK ${RUN_ID}\n`);
    expect(mesh.delivered[0]!.text).toContain("Return the exact marker sandbox-result.");
    expect(mesh.delivered[0]!.text).toContain("automatically returns your final assistant text");
    expect(mesh.delivered[0]!.text).toContain(`agents.followUp({ id: ${JSON.stringify(PARENT)}`);
    expect(mesh.delivered[0]!.text).toContain(`FABRIC-RESULT ${RUN_ID}\\n`);
    expect(mesh.awaited).toEqual([{ childSession: CHILD, deadline: Date.parse("2026-02-01T00:01:00Z") }]);
    expect(handle.sessionId).toBe(CHILD);
    expect(JSON.parse(fs.readFileSync(path.join(directory, "status.json"), "utf8"))).toMatchObject({ status: "completed", text: "exact sandbox-result", exitCode: 0, transport: "sandbox" });
    expect(terminal.records.get(RUN_ID)).toMatchObject({ placementRunId: RUN_ID, state: "finished", exitCode: 0, at: "2026-02-01T00:00:00Z" });
  });

  // Keep: #17551 records the namespace trust-boundary failure; check both Placement projections against the requested namespace.
  it("accepts the configured org-ex2r1 claim namespace returned by spawn and binding", async () => {
    const directory = scratch();
    const namespace = "org-ex2r1";
    const api = await fakePlacementServer({
      post: intent => result(intent.runId, "admitted", { claimRef: { namespace, name: `spawn-${intent.runId}`, uid: "claim-uid-1" } }),
      watch: () => [],
      binding: intent => bindingFor(intent, namespace),
    });
    const { transport, mesh } = transportForNamespace(api, namespace);

    const handle = await transport.launch(launchRequest(directory));
    await handle.closed;
    expect(api.bindingReads).toBe(1);
    expect(mesh.delivered).toHaveLength(1);
  });

  it("rejects an unsolicited org namespace when the default smarty-island namespace was requested", async () => {
    const api = await fakePlacementServer({
      post: intent => result(intent.runId, "admitted", { claimRef: { namespace: "org-ex2r1", name: `spawn-${intent.runId}`, uid: "claim-uid-1" } }),
      watch: () => [],
    });
    const { transport, mesh, terminal } = transportForNamespace(api);

    await expect(transport.launch(launchRequest(scratch()))).rejects.toMatchObject({
      name: "SandboxProtocolError", code: "sandbox.protocol.invalid", message: expect.stringContaining("spawn result field claimRef.namespace"),
    });
    expect(api.bindingReads).toBe(0);
    expect(mesh.delivered).toHaveLength(0);
    expect(terminal.records.get(RUN_ID)?.state).toBe("never-started");
  });

  it("rejects a spawn namespace that differs from the configured org namespace", async () => {
    const api = await fakePlacementServer({ post: intent => result(intent.runId, "admitted"), watch: () => [] });
    const { transport, mesh, terminal } = transportForNamespace(api, "org-ex2r1");

    await expect(transport.launch(launchRequest(scratch()))).rejects.toMatchObject({
      name: "SandboxProtocolError", code: "sandbox.protocol.invalid", message: expect.stringContaining("spawn result field claimRef.namespace"),
    });
    expect(api.bindingReads).toBe(0);
    expect(mesh.delivered).toHaveLength(0);
    expect(terminal.records.get(RUN_ID)?.state).toBe("never-started");
  });

  it("rejects a mismatched binding namespace even when the admitted spawn claimRef is null", async () => {
    const api = await fakePlacementServer({
      post: intent => result(intent.runId, "admitted", { claimRef: null, claimUID: null, podUID: null }),
      watch: () => [],
      binding: intent => bindingFor(intent, "smarty-island"),
    });
    const { transport, mesh, terminal } = transportForNamespace(api, "org-ex2r1");

    await expect(transport.launch(launchRequest(scratch()))).rejects.toMatchObject({
      name: "SandboxProtocolError", code: "sandbox.protocol.invalid", message: expect.stringContaining("binding field claimRef.namespace"),
    });
    expect(api.bindingReads).toBe(1);
    expect(mesh.delivered).toHaveLength(0);
    expect(terminal.records.get(RUN_ID)?.state).toBe("never-started");
  });

  it("waits for the admitted Pod UID to appear before spawning and delivering", async () => {
    const directory = scratch();
    const api = await fakePlacementServer({
      post: intent => result(intent.runId, "admitted", { podUID: null }),
      watch: () => [],
      binding: (intent, index) => index === 0 ? { ...bindingFor(intent), podUID: null, podName: null } : bindingFor(intent),
    });
    let now = BASE_NOW;
    const mesh = meshFor(taskResult("completed", "spawned after Pod binding"));
    const terminal = new FakeTerminal();
    const transport = new SandboxTransport(config(), {
      placement: api.placement, mesh, terminal, meshTransport: "nats", org: "smarty-dev", now: () => now,
      sleep: async milliseconds => { now += milliseconds; },
      resolveRepository: () => ({ repository: "https://github.com/Smarty-Pants-Inc/smarty-dev", cwd: "." }),
    });

    const handle = await transport.launch(launchRequest(directory));
    await handle.closed;
    expect(api.bindingReads).toBe(2);
    expect(now).toBe(BASE_NOW + 250);
    expect(mesh.delivered).toHaveLength(1);
    expect(JSON.parse(fs.readFileSync(path.join(directory, "status.json"), "utf8")))
      .toMatchObject({ status: "completed", text: "spawned after Pod binding" });
    expect(terminal.records.get(RUN_ID)?.state).toBe("finished");
  });

  it("allows a replacement Pod UID for the same admitted run and Claim", async () => {
    const directory = scratch();
    const api = await fakePlacementServer({
      post: intent => result(intent.runId, "admitted", { podUID: "pod-uid-before-replacement" }),
      watch: () => [],
      binding: intent => ({ ...bindingFor(intent), podUID: "pod-uid-after-replacement" }),
    });
    const mesh = meshFor(taskResult("completed", "replacement Pod result"));
    const terminal = new FakeTerminal();
    const transport = new SandboxTransport(config(), {
      placement: api.placement, mesh, terminal, meshTransport: "nats", org: "smarty-dev", now: () => BASE_NOW,
      resolveRepository: () => ({ repository: "https://github.com/Smarty-Pants-Inc/smarty-dev", cwd: "." }),
    });

    const handle = await transport.launch(launchRequest(directory));
    await handle.closed;
    expect(api.bindingReads).toBe(1);
    expect(mesh.delivered).toHaveLength(1);
    expect(JSON.parse(fs.readFileSync(path.join(directory, "status.json"), "utf8")))
      .toMatchObject({ status: "completed", text: "replacement Pod result" });
  });

  it("times out an unlinked Claim with the sandbox Pod-pending error code", async () => {
    const directory = scratch();
    const api = await fakePlacementServer({
      post: intent => result(intent.runId, "admitted"),
      watch: () => [],
      binding: intent => ({ ...bindingFor(intent), claimRef: null, claimUID: null, podUID: null, podName: null }),
    });
    let now = BASE_NOW;
    const mesh = meshFor();
    const terminal = new FakeTerminal();
    const transport = new SandboxTransport(config(true, 500), {
      placement: api.placement, mesh, terminal, meshTransport: "nats", org: "smarty-dev", now: () => now,
      sleep: async milliseconds => { now += milliseconds; },
      resolveRepository: () => ({ repository: "https://github.com/Smarty-Pants-Inc/smarty-dev", cwd: "." }),
    });

    await expect(transport.launch(launchRequest(directory))).rejects.toMatchObject({
      name: "SandboxPodPendingTimeoutError", code: "FABRIC_SANDBOX_POD_PENDING_TIMEOUT", field: "claimRef",
    });
    expect(api.bindingReads).toBe(2);
    expect(mesh.delivered).toHaveLength(0);
    expect(terminal.records.get(RUN_ID)).toMatchObject({ state: "never-started", reason: "sandbox Pod or Claim binding remained pending" });
  });

  it("fails a hard binding mismatch and names the mismatched field", async () => {
    const directory = scratch();
    const api = await fakePlacementServer({
      post: intent => result(intent.runId, "admitted"),
      watch: () => [],
      binding: intent => ({ ...bindingFor(intent), role: "wrong-role" }),
    });
    const mesh = meshFor();
    const terminal = new FakeTerminal();
    const transport = new SandboxTransport(config(), {
      placement: api.placement, mesh, terminal, meshTransport: "nats", org: "smarty-dev", now: () => BASE_NOW,
      resolveRepository: () => ({ repository: "https://github.com/Smarty-Pants-Inc/smarty-dev", cwd: "." }),
    });

    await expect(transport.launch(launchRequest(directory))).rejects.toMatchObject({
      name: "SandboxProtocolError", code: "sandbox.protocol.invalid", message: expect.stringContaining("binding field role"),
    });
    expect(api.bindingReads).toBe(1);
    expect(mesh.delivered).toHaveLength(0);
    expect(terminal.records.get(RUN_ID)?.state).toBe("never-started");
  });

  it("publishes failed terminal status when the child's result return fails", async () => {
    const directory = scratch();
    const api = await fakePlacementServer({ post: intent => result(intent.runId, "admitted"), watch: () => [] });
    const mesh = meshFor();
    mesh.awaitResult = async () => { throw new Error("child result followUp was rejected"); };
    const terminal = new FakeTerminal();
    const transport = new SandboxTransport(config(), {
      placement: api.placement, mesh, terminal, meshTransport: "nats", org: "smarty-dev", now: () => BASE_NOW,
      resolveRepository: () => ({ repository: "https://github.com/Smarty-Pants-Inc/smarty-dev", cwd: "." }),
    });

    const handle = await transport.launch(launchRequest(directory));
    await handle.closed;

    expect(JSON.parse(fs.readFileSync(path.join(directory, "status.json"), "utf8")))
      .toMatchObject({ status: "failed", error: "Sandbox result could not be retrieved" });
    expect(terminal.records.get(RUN_ID)).toMatchObject({ state: "failed", reason: "sandbox result path failed" });
  });

  it("keeps the captured final answer when the terminal KV write fails", async () => {
    const directory = scratch();
    const api = await fakePlacementServer({ post: intent => result(intent.runId, "admitted"), watch: () => [] });
    const warning = vi.spyOn(diagnostics, "warn").mockImplementation(() => {});
    const terminal = { put: async () => { throw Object.assign(new Error("KV write denied"), { code: "FABRIC_PLACEMENT_TERMINAL_WRITE_FAILED" }); } };
    const transport = new SandboxTransport(config(), {
      placement: api.placement, mesh: meshFor(taskResult("completed", "captured answer")), terminal,
      meshTransport: "nats", org: "smarty-dev", now: () => BASE_NOW,
      resolveRepository: () => ({ repository: "https://github.com/Smarty-Pants-Inc/smarty-dev", cwd: "." }),
    });

    const handle = await transport.launch(launchRequest(directory));
    await handle.closed;
    expect(JSON.parse(fs.readFileSync(path.join(directory, "status.json"), "utf8")))
      .toMatchObject({ status: "completed", text: "captured answer", exitCode: 0, transport: "sandbox" });
    expect(handle.lostContact?.()).toBeUndefined();
    expect(warning).toHaveBeenCalledWith("src/transport/sandbox/transport.ts",
      expect.stringContaining("FABRIC_PLACEMENT_TERMINAL_WRITE_FAILED"));
  });

  it("returns a typed refusal and records never-started", async () => {
    const directory = scratch();
    const api = await fakePlacementServer({ post: intent => result(intent.runId, "refused"), watch: () => [] });
    const terminal = new FakeTerminal();
    const transport = new SandboxTransport(config(), { placement: api.placement, mesh: meshFor(), terminal, meshTransport: "nats", org: "smarty-dev", now: () => BASE_NOW, resolveRepository: () => ({ repository: "https://github.com/Smarty-Pants-Inc/smarty-dev", cwd: "." }) });

    await expect(transport.launch(launchRequest(directory))).rejects.toMatchObject({ name: "SandboxPlacementRefusedError", code: "sandbox.placement.refused", placementCode: "POLICY_REFUSED" });
    expect(terminal.records.get(RUN_ID)).toMatchObject({ placementRunId: RUN_ID, state: "never-started", reason: "placement refused (POLICY_REFUSED)" });
  });

  it("stops a running sandbox task and resolves its handle only after the child owner confirms stop", async () => {
    const directory = scratch();
    const api = await fakePlacementServer({ post: intent => result(intent.runId, "admitted"), watch: () => [] });
    const mesh = meshFor();
    const stopCalls: Array<[string, string]> = [];
    mesh.awaitResult = async () => new Promise<SandboxTaskResult>(() => {});
    mesh.stop = async (childSession, runId) => { stopCalls.push([childSession, runId]); return { confirmed: true }; };
    const terminal = new FakeTerminal();
    const transport = new SandboxTransport(config(), { placement: api.placement, mesh, terminal, meshTransport: "nats", org: "smarty-dev", now: () => BASE_NOW,
      resolveRepository: () => ({ repository: "https://github.com/Smarty-Pants-Inc/smarty-dev", cwd: "." }) });

    const handle = await transport.launch(launchRequest(directory));
    expect(await handle.isAlive()).toBe(true);
    await handle.stop();
    await handle.closed;

    expect(stopCalls).toEqual([[CHILD, RUN_ID]]);
    expect(await handle.isAlive()).toBe(false);
    expect(JSON.parse(fs.readFileSync(path.join(directory, "status.json"), "utf8"))).toMatchObject({ status: "stopped", error: "Sandbox task stopped" });
    expect(terminal.records.get(RUN_ID)).toMatchObject({ placementRunId: RUN_ID, state: "failed", reason: "child stop was confirmed" });
  });

  it("refuses an operator stop when the bound child owner cannot confirm exit", async () => {
    const directory = scratch();
    const api = await fakePlacementServer({ post: intent => result(intent.runId, "admitted"), watch: () => [] });
    const mesh = meshFor();
    let rejectResult!: (error: Error) => void;
    mesh.awaitResult = async () => new Promise<SandboxTaskResult>((_resolve, reject) => { rejectResult = reject; });
    mesh.stop = async () => ({ confirmed: false, reason: "bound child route changed" });
    const terminal = new FakeTerminal();
    const transport = new SandboxTransport(config(), { placement: api.placement, mesh, terminal, meshTransport: "nats", org: "smarty-dev", now: () => BASE_NOW,
      resolveRepository: () => ({ repository: "https://github.com/Smarty-Pants-Inc/smarty-dev", cwd: "." }) });

    const handle = await transport.launch(launchRequest(directory));
    await expect(handle.stop()).rejects.toThrow("Sandbox child stop was not confirmed: bound child route changed");
    expect(handle.lostContact?.()).toBe("bound child route changed");
    rejectResult(new Error("sandbox result unavailable"));
    await handle.closed;
    expect(terminal.records.get(RUN_ID)).toMatchObject({ state: "failed", reason: "sandbox result path failed; remote exit is unconfirmed" });
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

  it("binds NATS presence, delivers through control followUp, and awaits the run-scoped result", async () => {
    const route = { machine: "island", ownerHostId: "runtime:child", ownerIdentityId: CHILD, ownerIncarnation: "incarnation-1", capabilities: ["followUp"], live: true };
    const participant = { id: CHILD, kind: "root", placementRunId: RUN_ID, ownerHostId: route.ownerHostId,
      ownerIdentityId: route.ownerIdentityId, ownerIncarnation: route.ownerIncarnation, interactive: true };
    const view = { resolve: () => route, whenChanged: async () => {}, leaseLive: () => true };
    const requests: unknown[][] = [];
    const mesh = createSandboxFabricMesh({
      transport: { kind: "nats" } as never,
      participants: { get: () => participant, list: () => [participant] } as never,
      participantView: view as never,
      control: { request: async (...args: unknown[]) => { requests.push(args); return { queued: true }; } } as never,
      canSend: () => true,
    });
    const child = await mesh.waitForPresence(RUN_ID, { timeoutMs: 5_000 });
    await mesh.deliverTask(child.id, "actual task text");
    expect(requests[0]?.slice(0, 3)).toEqual([route.ownerHostId, CHILD, "followUp"]);
    expect(requests[0]?.[3]).toMatchObject({ ownerIncarnation: route.ownerIncarnation, message: "actual task text", triggerTurn: true,
      data: { sandboxControl: { protocol: "pi-fabric-sandbox-stop-v1", runId: RUN_ID } } });
    const pending = mesh.awaitResult(child.id, RUN_ID, Date.now() + 10_000);
    expect(captureSandboxFinalAnswer({ id: CHILD, kind: "main" }, "mesh", `FABRIC-RESULT ${RUN_ID}\nresult from the bound child`)).toBe(true);
    await expect(pending).resolves.toMatchObject({ runId: RUN_ID, participantId: CHILD, text: "result from the bound child" });
  });

  it("refuses a stop for another run id and sends a bound stop only over the current child route", async () => {
    const route = { machine: "island", ownerHostId: "runtime:child", ownerIdentityId: CHILD, ownerIncarnation: "incarnation-1", capabilities: ["followUp"], live: true };
    const participant = { id: CHILD, kind: "root", placementRunId: RUN_ID, ownerHostId: route.ownerHostId,
      ownerIdentityId: route.ownerIdentityId, ownerIncarnation: route.ownerIncarnation, interactive: true };
    const requests: unknown[][] = [];
    const mesh = createSandboxFabricMesh({
      transport: { kind: "nats" } as never,
      participants: { get: () => participant, list: () => [participant] } as never,
      participantView: { resolve: () => route, whenChanged: async () => {}, leaseLive: () => true } as never,
      control: { request: async (...args: unknown[]) => { requests.push(args); return { queued: true, acknowledged: true }; } } as never,
      canSend: () => true,
    });
    const child = await mesh.waitForPresence(RUN_ID, { timeoutMs: 5_000 });
    const stop = mesh.stop;
    if (!stop) throw new Error("sandbox mesh does not expose stop");

    await expect(stop(child.id, `${RUN_ID}-other`)).resolves.toMatchObject({ confirmed: false });
    expect(requests).toHaveLength(0);
    await expect(stop(child.id, RUN_ID)).resolves.toEqual({ confirmed: true });
    expect(requests).toHaveLength(1);
    expect(requests[0]?.slice(0, 3)).toEqual([route.ownerHostId, CHILD, "stop"]);
    expect(requests[0]?.[3]).toMatchObject({ ownerIncarnation: route.ownerIncarnation,
      data: { sandboxControl: { protocol: "pi-fabric-sandbox-stop-v1", runId: RUN_ID } } });
  });

  it("refuses delivery if the run-bound child's owner incarnation changes", async () => {
    let incarnation = "incarnation-1";
    const participant = { id: CHILD, kind: "root", placementRunId: RUN_ID, ownerHostId: "runtime:child",
      ownerIdentityId: CHILD, get ownerIncarnation() { return incarnation; }, interactive: true };
    const mesh = createSandboxFabricMesh({
      transport: { kind: "nats" } as never,
      participants: { get: () => participant, list: () => [participant] } as never,
      participantView: { resolve: () => ({ machine: "island", ownerHostId: "runtime:child", ownerIdentityId: CHILD,
        get ownerIncarnation() { return incarnation; }, capabilities: ["followUp"], live: true }), whenChanged: async () => {}, leaseLive: () => true } as never,
      control: { request: async () => ({ queued: true }) } as never,
      canSend: () => true,
    });
    const child = await mesh.waitForPresence(RUN_ID, { timeoutMs: 5_000 });
    incarnation = "incarnation-2";
    await expect(mesh.deliverTask(child.id, "must not cross owner change")).rejects.toThrow(/owner changed/);
  });

  it("captures only a verified final answer from the exact child session", async () => {
    const runId = "sandbox-result-capture-1";
    const pending = waitForSandboxFinalAnswer(CHILD, runId, Date.now() + 10_000);
    const text = `FABRIC-RESULT ${runId}\nexact return value`;
    expect(captureSandboxFinalAnswer({ id: CHILD, kind: "main" }, undefined, text)).toBe(false);
    expect(captureSandboxFinalAnswer({ id: "session:attacker", kind: "main" }, "mesh", text)).toBe(false);
    expect(captureSandboxFinalAnswer({ id: CHILD, kind: "agent" }, "mesh", text)).toBe(false);
    expect(captureSandboxFinalAnswer({ id: CHILD, kind: "main" }, "mesh", text)).toBe(true);
    await expect(pending).resolves.toMatchObject({ runId, participantId: CHILD, status: "completed", text: "exact return value", exitCode: 0 });
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

  it("passes the configured namespace through a manager sandbox spawn to Placement", async () => {
    const directory = scratch();
    const namespace = "org-ex2r1";
    const api = await fakePlacementServer({
      allowAnyRunId: true,
      post: intent => result(intent.runId, "admitted", { claimRef: { namespace, name: `spawn-${intent.runId}`, uid: "claim-uid-1" } }),
      watch: () => [],
      binding: intent => bindingFor(intent, namespace),
    });
    const terminal = new FakeTerminal();
    const mesh = meshFor();
    mesh.awaitResult = async (childSession, runId, deadline) => {
      mesh.awaited.push({ childSession, deadline });
      return taskResult("completed", "manager round-trip result", runId, childSession);
    };
    const manager = new AgentManager(process.cwd(), {
      ...DEFAULT_FABRIC_CONFIG.agents,
      budgetUsd: 0,
      sandbox: config(true, 90_000, api.base, namespace),
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

    const imported = { session: "01234567-89ab-cdef-0123-456789abcdef", manifestSha256: "c".repeat(64) };
    const handle = await manager.spawn({ task: "Return the exact marker from a real manager spawn.", model: MODEL, thinking: "high", transport: "sandbox", profile: "lead", branch: "feature/manager-import", sessionImport: imported, extensions: false });
    const final = await manager.wait(handle.id);
    expect(final).toMatchObject({ id: handle.id, status: "completed", text: "manager round-trip result", transport: "sandbox" });
    expect(api.intents[0]).toMatchObject({ runId: handle.id, profile: "lead", branch: "feature/manager-import", sessionImport: imported });
    expect(api.requestBodies[0]).not.toHaveProperty("namespace");
    expect(api.bindingReads).toBe(1);
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

  it("keeps legacy canonical intent bytes and hash unchanged, validates optional import fields, and counts them toward the byte cap", () => {
    const request = launchRequest(scratch());
    const base = { runId: RUN_ID, cwd: process.cwd(), needs: [], parentSession: PARENT, org: "smarty-dev", timeoutMs: 60_000, now: BASE_NOW, workerArguments: request.workerArguments, resolveRepository: () => ({ repository: "https://github.com/Smarty-Pants-Inc/smarty-dev", cwd: "src/agents" }) };
    const intent = buildSandboxIntent(base);
    const bytes = JSON.stringify(intent);
    expect(Object.hasOwn(intent, "branch")).toBe(false);
    expect(Object.hasOwn(intent, "sessionImport")).toBe(false);
    expect(bytes).toBe("{\"v\":1,\"runId\":\"sandbox-run-1\",\"kind\":\"sandbox\",\"org\":\"smarty-dev\",\"parentSession\":\"session:parent-1\",\"role\":\"task-agent\",\"profile\":\"task\",\"complexity\":\"normal\",\"model\":\"cliproxyapi/gpt-6.1-sol\",\"thinking\":\"high\",\"needs\":[],\"repository\":\"https://github.com/Smarty-Pants-Inc/smarty-dev\",\"cwd\":\"src/agents\",\"expectedSeconds\":60,\"timeoutSeconds\":60,\"deadline\":\"2026-02-01T00:01:00Z\"}");
    expect(createHash("sha256").update(bytes).digest("hex")).toBe("3cbe7003f426813b11abf6d1ef1ab0a48492e19bf5af8ba72d03b0f0f622f013");

    const imported = { session: "01234567-89ab-cdef-0123-456789abcdef", manifestSha256: "a".repeat(64) };
    const withFields = buildSandboxIntent({ ...base, profile: "lead", branch: "feature/session-import", sessionImport: imported });
    expect(withFields).toMatchObject({ branch: "feature/session-import", sessionImport: imported });
    expect(() => buildSandboxIntent({ ...base, branch: "refs/heads/main" })).toThrow();
    expect(() => buildSandboxIntent({ ...base, branch: "a..b" })).toThrow();
    expect(() => buildSandboxIntent({ ...base, branch: "has space" })).toThrow();
    expect(() => buildSandboxIntent({ ...base, branch: "a".repeat(256) })).toThrow();
    expect(() => buildSandboxIntent({ ...base, branch: "ghp_" + "A".repeat(40) })).toThrow(/refused by policy/);
    expect(() => buildSandboxIntent({ ...base, branch: "https://example.test/branch" })).toThrow();
    expect(() => buildSandboxIntent({ ...base, branch: "name@example.test" })).toThrow();
    // Token policy matches whole values, not substrings or path components.
    for (const branch of ["feature/sk-calc", "feature/ghp_foo", "feature/xoxb-demo", "encoded-" + "A".repeat(90), "A".repeat(79), "AKIA" + "A".repeat(15), "AKIA" + "A".repeat(17)]) {
      expect(buildSandboxIntent({ ...base, branch }).branch).toBe(branch);
    }
    expect(() => buildSandboxIntent({ ...base, profile: "lead", branch: "main", sessionImport: { ...imported, extra: true } as never })).toThrow();
    expect(() => buildSandboxIntent({ ...base, profile: "lead", branch: "main", sessionImport: { session: imported.session } as never })).toThrow();
    expect(() => buildSandboxIntent({ ...base, profile: "lead", branch: "main", sessionImport: { ...imported, manifestSha256: "A".repeat(64) } })).toThrow();
    expect(() => buildSandboxIntent({ ...base, profile: "lead", sessionImport: imported })).toThrow();
    expect(() => buildSandboxIntent({ ...base, profile: "task", branch: "main", sessionImport: imported })).toThrow();
    expect(() => buildSandboxIntent({ ...base, profile: "org", branch: "main", sessionImport: imported })).toThrow();
    expect(() => buildSandboxIntent({ ...base, profile: "lane", branch: "main", sessionImport: imported, needs: Array.from({ length: 64 }, (_, index) => "n".repeat(250) + index) })).toThrow();
  });

  // Keep: Placement trust-boundary checks prevent a malformed move from becoming a fresh session.
  const importPin = { session: "01234567-89ab-cdef-0123-456789abcdef", manifestSha256: "a".repeat(64) };
  const wireIntent = (overrides: Partial<Parameters<typeof buildSandboxIntent>[0]> = {}) => buildSandboxIntent({
    runId: RUN_ID, cwd: process.cwd(), needs: [], parentSession: PARENT, org: "smarty-dev", timeoutMs: 60_000, now: BASE_NOW,
    workerArguments: ["--model", MODEL, "--thinking", "high"],
    resolveRepository: () => ({ repository: "https://github.com/Smarty-Pants-Inc/smarty-dev", cwd: "." }), ...overrides,
  });

  it.each([
    "01234567-89AB-CDEF-0123-456789ABCDEF", "not-a-uuid", "session:01234567-89ab-cdef-0123-456789abcdef",
    "0123456789abcdef0123456789abcdef", "01234567-89ab-cdef-0123-456789abcdeg", "01234567-89ab-cdef-0123-456789abcdef\n",
  ])("refuses non-lowercase-UUID import session %j", session => {
    expect(() => wireIntent({ profile: "lane", branch: "main", sessionImport: { ...importPin, session } })).toThrow(/sessionImport/);
  });

  it.each(["branch", "sessionImport"] as const)("refuses explicit null %s", field => {
    const values = { [field]: null };
    expect(() => normalizeAgentRunRequest({ task: "test", transport: "sandbox", profile: "lane", branch: "main", ...values }, { runner: "pi", timeoutMs: 60_000 })).toThrow();
    expect(() => wireIntent({ profile: "lane", branch: "main", ...values } as never)).toThrow();
  });

  it.each([
    "", "a".repeat(256), "éclair", "feature/éclair", "-main", "_main", ".main", "/main", "refs/heads/main",
    "a//b", "a..b", "a/.b", "a.lock", "a.lock/b", "a/b.lock", "a/", "a.", "a b", "a@b", "a:b", "a+b", "a\\b", "a\nb", "main\n",
  ])("refuses invalid wire branch %j", branch => {
    expect(() => wireIntent({ branch })).toThrow(/branch/);
  });

  it.each(["A_1/topic.v2-fix", "a".repeat(127) + "-" + "b".repeat(127), "a.locked/topic", "0", "a./b"])("accepts valid ASCII wire branch %j", branch => {
    expect(wireIntent({ branch }).branch).toBe(branch);
  });

  // Keep: credential policy and Git failures must not leak values across the Placement trust boundary.
  it.each([
    ["GitHub classic", ["ghp_foo", ...["ghp_", "gho_", "ghu_", "ghs_", "ghr_"].map(prefix => prefix + "A".repeat(40))]],
    ["GitHub fine-grained", ["github_pat_" + "A".repeat(40)]],
    ["OpenAI", ["sk-" + "A".repeat(40)]],
    ["Anthropic", ["sk-ant-api03-" + "A".repeat(40)]],
    ["Slack xox family", ["xoxa-", "xoxb-", "xoxp-", "xoxr-", "xoxs-"].map(prefix => prefix + "1234567890-ABCdef")],
    ["AWS access key", ["AKIA0123456789ABCDEF"]],
    ["GitLab", ["glpat-" + "A".repeat(20)]],
    ["long base64", ["A".repeat(80), "Ab01".repeat(20), "A".repeat(78) + "+/", "A".repeat(80) + "==", "a".repeat(255)]],
  ] as const)("refuses token-shaped %s values in branch and sessionImport.session by policy", (_family, values) => {
    for (const value of values) {
      for (const field of ["branch", "sessionImport.session"] as const) {
        const overrides = field === "branch" ? { branch: value } : { profile: "lane" as const, branch: "main", sessionImport: { ...importPin, session: value } };
        expect(() => wireIntent(overrides)).toThrowError(expect.objectContaining({
          code: "sandbox.intent.credential_refused",
          message: `Sandbox intent ${field} refused by policy: credential-looking values are not allowed`,
        }));
      }
    }
  });

  it("wraps a real Git branch refusal without returning Git's message or raw value", () => {
    // HEAD passes the ASCII prechecks but Git refuses it as a branch name.
    let caught: unknown;
    try { wireIntent({ branch: "HEAD" }); } catch (error) { caught = error; }
    expect(caught).toBeInstanceOf(Error);
    expect(caught).toMatchObject({ code: "sandbox.intent.branch_invalid", message: "Invalid sandbox intent branch" });
    expect(String(caught)).not.toContain("HEAD");
    expect(String(caught)).not.toContain("fatal:");
    expect(Object.keys(caught as Error)).toEqual(["code"]);
    expect(caught).not.toHaveProperty("cause");
  });

  it("defaults an explicitly requested import to lane without changing the legacy task default", () => {
    const normalized = normalizeAgentRunRequest({ task: "test", transport: "sandbox", branch: "main", sessionImport: importPin }, { runner: "pi", timeoutMs: 60_000 });
    expect(wireIntent({
      ...(normalized.branch !== undefined ? { branch: normalized.branch } : {}),
      ...(normalized.sessionImport !== undefined ? { sessionImport: normalized.sessionImport } : {}),
    })).toMatchObject({ profile: "lane", sessionImport: importPin });
    expect(wireIntent().profile).toBe("task");
  });

  it.each(["branch", "sessionImport"] as const)("reports old-broker INTENT_INVALID with %s after exactly one POST", async field => {
    // Also exercise a detail that would trigger legacy profile fallback.
    for (const detail of ["request must contain only documented v1 fields", "Unknown field profile"]) {
      const api = await fakePlacementServer({ spawnError: { code: "INTENT_INVALID", detail }, post: intent => result(intent.runId, "admitted"), watch: () => [] });
      const intent = wireIntent({ profile: "lane", branch: "main", ...(field === "sessionImport" ? { sessionImport: importPin } : {}) });
      const fallbackLog: string[] = [];
      const error = await new SandboxPlacementHttp(api.base, runId => fallbackLog.push(runId)).requestSpawn(intent).catch((error: unknown) => error);
      expect(error).toBeInstanceOf(SandboxSessionImportUnsupportedError);
      expect(error).toMatchObject({ name: "SandboxSessionImportUnsupportedError", code: "PLACEMENT_SESSION_IMPORT_UNSUPPORTED", status: 400,
        cause: { code: "INTENT_INVALID", status: 400 } });
      expect(api.requestBodies).toEqual([intent]);
      expect(fallbackLog).toEqual([]);
    }
  });

  it.each([
    ["request must contain only documented v1 fields", 1], ["Unknown field profile", 2],
  ] as const)("keeps legacy INTENT_INVALID behaviour without fields: %s", async (detail, posts) => {
    const api = await fakePlacementServer({ spawnError: { code: "INTENT_INVALID", detail }, post: intent => result(intent.runId, "admitted"), watch: () => [] });
    const intent = wireIntent();
    const pending = api.placement.requestSpawn(intent);
    await expect(pending).rejects.toBeInstanceOf(SandboxPlacementHttpError);
    await expect(pending).rejects.toMatchObject({ code: "INTENT_INVALID", status: 400, body: { code: "INTENT_INVALID", detail } });
    expect(api.requestBodies).toHaveLength(posts);
    expect(api.requestBodies[0]).toEqual(intent);
    if (posts === 2) expect(api.requestBodies[1]).not.toHaveProperty("profile");
  });

  it.each(["branch", "sessionImport"] as const)("does not retry or start a child when transport receives old-broker refusal with %s", async field => {
    const directory = scratch();
    const api = await fakePlacementServer({ spawnError: { code: "INTENT_INVALID", detail: "request must contain only documented v1 fields" }, post: intent => result(intent.runId, "admitted"), watch: () => [] });
    const terminal = new FakeTerminal();
    const mesh = meshFor();
    let slept = 0;
    const transport = new SandboxTransport(config(), { placement: api.placement, mesh, terminal, meshTransport: "nats", org: "smarty-dev", now: () => BASE_NOW,
      sleep: async () => { slept += 1; throw new Error("must not retry a refused move"); },
      resolveRepository: () => ({ repository: "https://github.com/Smarty-Pants-Inc/smarty-dev", cwd: "." }) });
    await expect(transport.launch(launchRequest(directory, { profile: "lane", branch: "main", ...(field === "sessionImport" ? { sessionImport: importPin } : {}) })))
      .rejects.toBeInstanceOf(SandboxSessionImportUnsupportedError);
    expect(api.requestBodies).toHaveLength(1);
    expect(api.requestBodies[0]).toMatchObject({ profile: "lane", branch: "main", ...(field === "sessionImport" ? { sessionImport: importPin } : {}) });
    expect(slept).toBe(0);
    expect(api.gets).toBe(0);
    expect(mesh.presenceRunIds).toEqual([]);
    expect(mesh.delivered).toEqual([]);
    expect(terminal.records.get(RUN_ID)?.state).toBe("never-started");
  });

  it("bypasses cached legacy profile fallback when move fields are later supplied", async () => {
    const api = await fakePlacementServer({ rejectProfile: true, post: intent => result(intent.runId, "admitted"), watch: () => [] });
    const fallbackLog: string[] = [];
    const client = new SandboxPlacementHttp(api.base, runId => fallbackLog.push(runId));
    await client.requestSpawn(wireIntent());
    expect(api.requestBodies).toHaveLength(2);
    const intent = wireIntent({ profile: "lane", branch: "main", sessionImport: importPin });
    await expect(client.requestSpawn(intent)).rejects.toMatchObject({ code: "INVALID_INTENT", status: 400 });
    expect(api.requestBodies).toHaveLength(3);
    expect(api.requestBodies[2]).toEqual(intent);
    expect(fallbackLog).toEqual([RUN_ID]);
  });

  it.each(["A".repeat(64), "a".repeat(63), "a".repeat(65), "g".repeat(64), "a".repeat(64) + "\n"])("refuses malformed manifest SHA-256 %j", manifestSha256 => {
    expect(() => wireIntent({ profile: "lane", branch: "main", sessionImport: { ...importPin, manifestSha256 } })).toThrow(/sessionImport/);
  });

  it("refuses sessionImport outside sandbox transport before any placement request", () => {
    expect(() => normalizeAgentRunRequest({ task: "test", transport: "process", profile: "lead", branch: "main", sessionImport: { session: "01234567-89ab-cdef-0123-456789abcdef", manifestSha256: "d".repeat(64) } }, { runner: "pi", timeoutMs: 60_000 })).toThrow(/requires sandbox transport/);
  });

  it("passes explicitly requested branch and sessionImport unchanged to the Placement POST", async () => {
    const directory = scratch();
    const api = await fakePlacementServer({ post: intent => result(intent.runId, "admitted"), watch: () => [] });
    const transport = new SandboxTransport(config(), { placement: api.placement, mesh: meshFor(), terminal: new FakeTerminal(), meshTransport: "nats", org: "smarty-dev", now: () => BASE_NOW, resolveRepository: () => ({ repository: "https://github.com/Smarty-Pants-Inc/smarty-dev", cwd: "." }) });
    const sessionImport = { session: "01234567-89ab-cdef-0123-456789abcdef", manifestSha256: "b".repeat(64) };
    const handle = await transport.launch(launchRequest(directory, { profile: "lane", branch: "feature/import", sessionImport }));
    await handle.closed;
    expect(api.requestBodies[0]).toMatchObject({ branch: "feature/import", sessionImport });
  });

  it("keeps sandbox disabled by default and validates its host-only placement URL", () => {
    expect(normalizeFabricConfig({}).agents.transport).toBe("process");
    expect(normalizeFabricConfig({ agents: { transport: "sandbox" } }).agents.transport).toBe("sandbox");
    expect(normalizeFabricConfig({}).agents.sandbox).toEqual({ enabled: false, template: "smarty-proof-agent", readyTimeoutMs: 90_000 });
    expect(DEFAULT_SANDBOX_PLACEMENT_URL).toBe("http://smarty-placement.smarty-island.svc:8080");

    for (const placementUrl of [
      "http://smarty-placement.smarty-island.svc:8080",
      "http://100.125.29.10:30880",
      "http://127.0.0.1:1024",
      "http://127.0.0.1:18080",
      "http://127.0.0.1:65535",
    ]) {
      expect(normalizeFabricConfig({ agents: { sandbox: { enabled: true, template: "smarty-proof-agent", readyTimeoutMs: 30_000, placementUrl } } }).agents.sandbox)
        .toEqual({ enabled: true, template: "smarty-proof-agent", readyTimeoutMs: 30_000, placementUrl });
    }

    for (const placementUrl of [
      "https://smarty-placement.smarty-island.svc:8443",
      "http://user:secret@100.125.29.10:30880",
      "http://localhost:18080",
      "http://127.0.0.2:18080",
      "http://127.1:18080",
      "http://2130706433:18080",
      "http://0x7f000001:18080",
      "http://0.0.0.0:18080",
      "http://[::1]:18080",
      "http://127.0.0.1",
      "http://127.0.0.1:80",
      "http://127.0.0.1:1023",
      "http://127.0.0.1:65536",
      "https://127.0.0.1:18080",
      "http://u:p@127.0.0.1:18080",
      "http://127.0.0.1:18080/x",
    ]) {
      const expectedError = placementUrl === "http://127.0.0.1:65536"
        ? "Invalid agents.sandbox.placementUrl"
        : "agents.sandbox.placementUrl must use the in-cluster HTTP endpoint or a tailnet IPv4 NodePort";
      expect(() => normalizeFabricConfig({ agents: { sandbox: { placementUrl } } })).toThrow(expectedError);
    }
  });

  it("normalizes explicitly configured one- and 63-character DNS-1123 sandbox namespaces", () => {
    for (const namespace of ["a", "a".repeat(63)]) {
      const normalized = normalizeFabricConfig({ agents: { sandbox: { namespace, placementUrl: "http://127.0.0.1:18080" } } }).agents.sandbox;
      expect(normalized).toEqual({ enabled: false, template: "smarty-proof-agent", placementUrl: "http://127.0.0.1:18080", namespace, readyTimeoutMs: 90_000 });
    }
  });

  it.each(["A", "org.example", "org_example", "-org", "org-", "a".repeat(64), "", "\n", 7, null])(
    "refuses malformed sandbox namespace %j",
    namespace => expect(() => normalizeFabricConfig({ agents: { sandbox: { namespace } } })).toThrow(/agents\.sandbox\.namespace/),
  );
});
