import fs from "node:fs";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import type { IncomingMessage, Server, ServerResponse } from "node:http";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG, type FabricAgentConfig } from "../src/config.js";
import { AgentManager, type AgentTaskPresence } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { normalizeAgentPlacement, type AgentPlacementConfig } from "../src/agents/placement-config.js";
import type { AgentRunRecord } from "../src/agents/types.js";
import type { PlacementBinding, PlacementIntent } from "../src/placement/client.js";
import type { PlacementTerminalRecord } from "../src/placement/terminal.js";

// Keep reason: these tests cross the Placement admission/binding boundary into NATS
// task-presence custody and the native process launch boundary, including two prelaunch failures.
const MODEL = "openai/gpt-6.1-sol";
const HOST = os.hostname().toLowerCase();
const PARENT_SESSION = "placement-spawn-parent";
const BINDING_KEYS = [
  "runId", "status", "claimRef", "claimUID", "podUID", "podName", "host", "org", "role", "model",
  "provider", "thinking", "expectedSeconds", "deadline", "executionDeadline", "admittedAt",
].sort();
const RESULT_KEYS = ["runId", "status", "code", "detail", "retryAfterSeconds", "host", "claimRef", "claimUID", "podUID"].sort();

type AdmittedPlacementBinding = Extract<PlacementBinding, { status: "admitted" }>;

interface RequestObservation {
  method: string | undefined;
  path: string | undefined;
  contentType: string | undefined;
  authorization: string | undefined;
}

interface PlacementApiFixture {
  root: string;
  runRoot: string;
  url: string;
  admittedHost: string;
  requests: RequestObservation[];
  intents: PlacementIntent[];
  outcomes: Record<string, unknown>[];
  bindings: AdmittedPlacementBinding[];
  errors: string[];
  close(): Promise<void>;
}

const sendJson = (response: ServerResponse, body: unknown, status = 200): void => {
  response.writeHead(status, { "Content-Type": "application/json", "Cache-Control": "no-store" });
  response.end(JSON.stringify(body));
};

const requestBody = async (request: IncomingMessage): Promise<string> => {
  const chunks: Buffer[] = [];
  for await (const chunk of request) chunks.push(Buffer.from(chunk));
  return Buffer.concat(chunks).toString("utf8");
};

const timestamp = (milliseconds: number): string => new Date(milliseconds).toISOString().replace(/\.\d{3}Z$/u, "Z");

const providerOf = (model: string): string | null => {
  const provider = model.slice(0, model.indexOf("/"));
  return ["anthropic", "azure", "bedrock", "cohere", "deepseek", "google", "gemini", "groq", "mistral", "ollama", "openai", "openrouter", "qwen", "vertex", "xai", "cliproxyapi"].includes(provider)
    ? provider
    : null;
};

const makeBinding = (intent: PlacementIntent, host: string): AdmittedPlacementBinding => {
  const admittedAtMs = Math.floor(Date.now() / 1_000) * 1_000;
  const deadlineMs = intent.deadline === null ? Number.POSITIVE_INFINITY : Date.parse(intent.deadline);
  const executionDeadlineMs = Math.min(admittedAtMs + intent.expectedSeconds * 1_000, deadlineMs);
  return {
    runId: intent.runId,
    status: "admitted",
    claimRef: null,
    claimUID: null,
    podUID: null,
    podName: null,
    host,
    org: intent.org,
    role: intent.role,
    model: intent.model,
    provider: providerOf(intent.model),
    thinking: intent.thinking,
    expectedSeconds: intent.expectedSeconds,
    deadline: intent.deadline,
    executionDeadline: timestamp(executionDeadlineMs),
    admittedAt: timestamp(admittedAtMs),
  };
};

const admittedResult = (intent: PlacementIntent, host: string): Record<string, unknown> => ({
  runId: intent.runId,
  status: "admitted",
  code: "ADMITTED",
  detail: "Local process placement admitted",
  retryAfterSeconds: null,
  host,
  claimRef: null,
  claimUID: null,
  podUID: null,
});

const refusedResult = (intent: PlacementIntent): Record<string, unknown> => ({
  runId: intent.runId,
  status: "refused",
  code: "POLICY_DENIED",
  detail: "The local fixture policy refused this spawn",
  retryAfterSeconds: null,
  host: null,
  claimRef: null,
  claimUID: null,
  podUID: null,
});

async function createPlacementApiFixture(mode: "admit" | "refuse", admittedHost = HOST): Promise<PlacementApiFixture> {
  const root = await mkdtemp(path.join(os.tmpdir(), "placement-spawn-boundary-"));
  const runRoot = path.join(root, "runs");
  await mkdir(runRoot, { mode: 0o700 });

  const requests: RequestObservation[] = [];
  const intents: PlacementIntent[] = [];
  const outcomes: Record<string, unknown>[] = [];
  const bindings: AdmittedPlacementBinding[] = [];
  const errors: string[] = [];
  let submittedIntent: PlacementIntent | undefined;

  const server: Server = createServer((request, response) => {
    void (async () => {
      requests.push({
        method: request.method,
        path: request.url,
        contentType: request.headers["content-type"],
        authorization: request.headers.authorization,
      });

      if (request.method === "POST" && request.url === "/v1/spawns") {
        const intent = JSON.parse(await requestBody(request)) as PlacementIntent;
        submittedIntent = intent;
        intents.push(intent);
        const outcome = mode === "admit" ? admittedResult(intent, admittedHost) : refusedResult(intent);
        outcomes.push(outcome);
        sendJson(response, outcome);
        return;
      }

      const bindingPath = request.url?.match(/^\/v1\/spawns\/([^/]+)\/binding$/u);
      const currentIntent = submittedIntent;
      if (request.method === "GET" && bindingPath && currentIntent && currentIntent.runId === bindingPath[1]) {
        const binding = makeBinding(currentIntent, admittedHost);
        bindings.push(binding);
        sendJson(response, binding);
        return;
      }

      sendJson(response, { code: "NOT_FOUND", detail: "Fixture route not found" }, 404);
    })().catch(error => {
      errors.push(error instanceof Error ? error.message : String(error));
      if (!response.headersSent) sendJson(response, { code: "PLACEMENT_UNAVAILABLE", detail: "Fixture handler failed" }, 503);
      else response.destroy();
    });
  });

  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      server.off("error", reject);
      resolve();
    });
  });
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("Placement loopback fixture did not bind");

  return {
    root,
    runRoot,
    url: `http://127.0.0.1:${address.port}`,
    admittedHost,
    requests,
    intents,
    outcomes,
    bindings,
    errors,
    async close() {
      if (!server.listening) return;
      await new Promise<void>((resolve, reject) => {
        server.close(error => error ? reject(error) : resolve());
        server.closeAllConnections();
      });
    },
  };
}

const deferred = <T>() => {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(done => { resolve = done; });
  return { promise, resolve };
};

const isolatedEnvironmentKeys = [
  "PI_FABRIC_DEPTH", "PI_FABRIC_BUDGET", "PI_FABRIC_BUDGET_FILE", "PI_FABRIC_BUDGET_ID",
  "PI_FABRIC_PARENT_RUN", "PI_FABRIC_ACTOR_ID", "PI_FABRIC_ACTOR_SESSION_FILE",
  "PI_FABRIC_IDENTITY_ID", "PI_FABRIC_MAIN_AGENT_ID", "PI_FABRIC_SESSION_ID",
  "PI_FABRIC_SPAWNER_ID", "PI_FABRIC_SPAWNER_KIND", "PI_FABRIC_SPAWNER_RUN",
  "PI_FABRIC_PROJECT_ROOT", "PI_FABRIC_RUN_ROOT", "PI_FABRIC_TMPDIR", "PI_FABRIC_MESH_ROOT",
  "PI_FABRIC_WRITE_POLICY", "PI_FABRIC_LINEAGE", "SMARTY_ORG",
] as const;

const isolatePlacementEnvironment = (): (() => void) => {
  const previous = new Map(isolatedEnvironmentKeys.map(key => [key, process.env[key]]));
  for (const key of isolatedEnvironmentKeys) delete process.env[key];
  return () => {
    for (const [key, value] of previous) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
  };
};

const makeManager = (
  fixture: PlacementApiFixture,
  taskPresence: AgentTaskPresence | undefined,
  placementTerminalWriter: (record: PlacementTerminalRecord) => Promise<void>,
  placement?: AgentPlacementConfig,
  nats = true,
): AgentManager => {
  const placementApi = {
    enabled: true as const,
    // PlacementClient explicitly supports loopback HTTP for an integration fixture;
    // host config restricts deployment URLs at normalization.
    url: fixture.url,
    requestTimeoutMs: 2_000,
    watchTimeoutMs: 2_000,
  };
  const config: FabricAgentConfig = {
    ...DEFAULT_FABRIC_CONFIG.agents,
    runner: "pi",
    transport: "process",
    model: MODEL,
    thinking: "high",
    timeoutMs: 120_000,
    budgetUsd: 0,
    maxTokensPerChild: 0,
    retainRuns: false,
    sessionExport: false,
    placementApi,
    ...(placement ? { placement } : {}),
  };
  return new AgentManager(process.cwd(), config, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
    runRoot: fixture.runRoot,
    projectRoot: process.cwd(),
    rootSessionCwd: () => process.cwd(),
    sessionId: () => PARENT_SESSION,
    mainAgentId: "placement-spawn-test-main",
    preparePiModel: async model => model,
    ...(nats ? { meshTransport: { transport: "nats" as const, hostTransport: "nats" as const, nats: { replicas: 1 } } } : {}),
    ...(taskPresence ? { taskPresence } : {}),
    placementTerminalWriter,
  });
};

const cleanup = async (
  fixture: PlacementApiFixture,
  manager: AgentManager | undefined,
  restoreEnvironment: () => void,
): Promise<void> => {
  try {
    await manager?.close();
  } finally {
    try {
      await fixture.close();
    } finally {
      try {
        await rm(fixture.root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
      } finally {
        restoreEnvironment();
      }
    }
  }
};

const expectFixtureRequests = (fixture: PlacementApiFixture, runId: string, bindingRequested: boolean): void => {
  expect(fixture.errors).toEqual([]);
  expect(fixture.requests.map(({ method, path: requestPath, contentType, authorization }) => ({
    method, path: requestPath, contentType, authorization,
  }))).toEqual([
    { method: "POST", path: "/v1/spawns", contentType: "application/json", authorization: undefined },
    ...(bindingRequested ? [{
      method: "GET", path: `/v1/spawns/${runId}/binding`, contentType: undefined, authorization: undefined,
    }] : []),
  ]);
};

afterEach(() => {
  vi.restoreAllMocks();
});

describe.skipIf(process.platform === "win32" || typeof process.getuid !== "function" || !fs.constants.O_NOFOLLOW)("Placement spawn boundary", () => {
  it("publishes the admitted local binding before a real process worker and does not await its terminal writer", async () => {
    const restoreEnvironment = isolatePlacementEnvironment();
    const fixture = await createPlacementApiFixture("admit");
    let manager: AgentManager | undefined;
    const terminalGate = deferred<void>();
    const terminalStarted = deferred<PlacementTerminalRecord>();
    const terminalWrites: PlacementTerminalRecord[] = [];
    const ordering: string[] = [];
    let published: AgentRunRecord | undefined;
    const taskPresence: AgentTaskPresence = {
      publishBeforeLaunch: async record => {
        ordering.push("taskPresence.publishBeforeLaunch");
        published = record;
      },
      withdraw: async () => undefined,
    };
    const realLaunch = ProcessTransport.prototype.launch;
    const launch = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(function (this: ProcessTransport, request) {
      ordering.push("ProcessTransport.launch");
      return realLaunch.call(this, request);
    });

    try {
      manager = makeManager(fixture, taskPresence, async record => {
        terminalWrites.push(record);
        terminalStarted.resolve(record);
        return terminalGate.promise;
      });
      let timer: ReturnType<typeof setTimeout> | undefined;
      const timedRun = new Promise<never>((_resolve, reject) => {
        timer = setTimeout(() => reject(new Error("manager waited for the best-effort terminal writer")), 5_000);
      });
      let result;
      try {
        result = await Promise.race([
          manager.run({ task: "placement spawn boundary integration", model: MODEL, complexity: "simple", transport: "process", readOnly: true }),
          timedRun,
        ]);
      } finally {
        if (timer) clearTimeout(timer);
      }

      expect(result).toMatchObject({ status: "completed", model: MODEL, thinking: "high", text: "fake worker complete", exitCode: 0 });
      expect(ordering).toEqual(["taskPresence.publishBeforeLaunch", "ProcessTransport.launch"]);
      expect(launch).toHaveBeenCalledOnce();
      const intent = fixture.intents[0]!;
      const binding = fixture.bindings[0]!;
      expect(Object.keys(fixture.outcomes[0]!).sort()).toEqual(RESULT_KEYS);
      expect(fixture.outcomes[0]).toEqual(admittedResult(intent, fixture.admittedHost));
      expect(Object.keys(binding).sort()).toEqual(BINDING_KEYS);
      expect(binding).toMatchObject({
        host: os.hostname().toLowerCase(),
        org: intent.org,
        role: intent.role,
        model: MODEL,
        provider: "openai",
        thinking: "high",
        expectedSeconds: intent.expectedSeconds,
        deadline: intent.deadline,
      });
      expect(binding.executionDeadline).toBe(timestamp(Date.parse(binding.admittedAt) + intent.expectedSeconds * 1_000));
      expect(intent).toMatchObject({ kind: "process", model: MODEL, thinking: "high", parentSession: PARENT_SESSION, complexity: "simple" });
      expect(published).toMatchObject({
        id: result.id,
        status: "queued",
        transport: "process",
        model: MODEL,
        placementRunId: result.id,
        placementKind: "process",
        placementHost: os.hostname().toLowerCase(),
        expectedSeconds: intent.expectedSeconds,
      });
      expect(launch.mock.calls[0]![0].placementAdmission).toEqual({
        placementRunId: result.id,
        placementKind: "process",
        placementHost: os.hostname().toLowerCase(),
        expectedSeconds: intent.expectedSeconds,
        executionDeadline: binding.executionDeadline,
      });
      expectFixtureRequests(fixture, result.id, true);
      const terminalRecord = await terminalStarted.promise;
      expect(terminalWrites).toHaveLength(1);
      expect(terminalRecord).toMatchObject({ placementRunId: result.id, state: "finished", exitCode: 0 });
    } finally {
      terminalGate.resolve();
      launch.mockRestore();
      await cleanup(fixture, manager, restoreEnvironment);
    }
  }, 15_000);

  it("refuses a remote write-policy binding before launching without confinement", async () => {
    const restoreEnvironment = isolatePlacementEnvironment();
    const fixture = await createPlacementApiFixture("admit", "placement-node");
    let manager: AgentManager | undefined;
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");

    try {
      manager = makeManager(fixture, undefined, async () => undefined);
      await expect(manager.run({ task: "remote write policy must fail closed", model: MODEL, transport: "process", readOnly: true }))
        .rejects.toMatchObject({ name: "PlacementAdmissionError", code: "LOCAL_TRANSPORT_UNSUPPORTED" });
      expect(fixture.bindings[0]).toMatchObject({ host: "placement-node" });
      expect(launch).not.toHaveBeenCalled();
      expectFixtureRequests(fixture, fixture.intents[0]!.runId, true);
    } finally {
      launch.mockRestore();
      await cleanup(fixture, manager, restoreEnvironment);
    }
  }, 10_000);

  it("does not fall back to a local worker when Placement refuses admission", async () => {
    const restoreEnvironment = isolatePlacementEnvironment();
    const fixture = await createPlacementApiFixture("refuse");
    let manager: AgentManager | undefined;
    let presenceCalls = 0;
    let terminalCalls = 0;
    const taskPresence: AgentTaskPresence = {
      publishBeforeLaunch: async () => { presenceCalls++; },
      withdraw: async () => undefined,
    };
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");

    try {
      manager = makeManager(fixture, taskPresence, async () => { terminalCalls++; });
      await expect(manager.run({ task: "Placement refusal must not launch", model: MODEL, transport: "process" }))
        .rejects.toMatchObject({ name: "PlacementAdmissionError", code: "POLICY_DENIED" });

      expect(launch).not.toHaveBeenCalled();
      expect(presenceCalls).toBe(0);
      expect(terminalCalls).toBe(0);
      expect(fixture.intents).toHaveLength(1);
      expect(Object.keys(fixture.outcomes[0]!).sort()).toEqual(RESULT_KEYS);
      expect(fixture.outcomes[0]).toEqual(refusedResult(fixture.intents[0]!));
      expectFixtureRequests(fixture, fixture.intents[0]!.runId, false);
    } finally {
      launch.mockRestore();
      await cleanup(fixture, manager, restoreEnvironment);
    }
  }, 10_000);

  // #1423 contract: the default task write policy keeps remote placement (logged as write_policy.remote_unconfined);
  // only an explicit or inherited policy refuses a remote binding (above).
  it.each([["no", { writePolicy: "unrestricted" as const }], ["the default", {}]] as const)(
    "routes a remote admission with %s write policy through its bound-host launcher without local fallback", async (_kind, policy) => {
    const restoreKeys = isolatePlacementEnvironment();
    const previousPath = process.env.PATH;
    const restoreEnvironment = () => {
      restoreKeys();
      if (previousPath === undefined) delete process.env.PATH;
      else process.env.PATH = previousPath;
    };
    const fixture = await createPlacementApiFixture("admit", "placement-node");
    const bin = path.join(fixture.root, "bin");
    const sshLog = path.join(fixture.root, "ssh.jsonl");
    const launcher = path.join(fixture.root, "remote-launcher.mjs");
    await mkdir(bin, { mode: 0o700 });
    await writeFile(path.join(bin, "ssh"), `#!${process.execPath}\nconst fs = require("node:fs"); fs.appendFileSync(${JSON.stringify(sshLog)}, JSON.stringify(process.argv.slice(2)) + "\\n");\n`, { mode: 0o700 });
    await writeFile(launcher, [
      'const [operation, runId, ...args] = process.argv.slice(2);',
      'if (operation === "launch") { const host = args[args.indexOf("--host") + 1]; console.log(`RYZEN2_TASK_ACCEPTED ${runId} on ${host} (fixture)`); }',
      'else if (operation === "result") console.log(JSON.stringify({ rc: 0, text: "remote worker complete" }));',
      'else if (operation === "cancel") console.log(JSON.stringify({ ok: true }));',
    ].join("\n"), { mode: 0o600 });
    process.env.PATH = `${bin}${path.delimiter}${previousPath ?? ""}`;
    const placement = normalizeAgentPlacement({
      default: "remote",
      command: [process.execPath, launcher, "launch", "{id}", "--host", "auto", "--minutes", "{minutes}", "--src", "{cwd}", "--model", "{model}", "--thinking", "{thinking}", "--", "{task}"],
      pickerCommand: [process.execPath, launcher, "picker"],
      resultCommand: [process.execPath, launcher, "result"],
      cancelCommand: [process.execPath, launcher, "cancel"],
      capabilities: [],
      sshAliases: { "placement-node": "placement-alias" },
      pollIntervalMs: 10,
      commandTimeoutMs: 5_000,
    })!;
    let manager: AgentManager | undefined;
    const terminalStarted = deferred<PlacementTerminalRecord>();

    try {
      manager = makeManager(fixture, undefined, async record => { terminalStarted.resolve(record); }, placement, false);
      const result = await manager.run({ task: "Placement must launch on the admitted remote host", model: MODEL, complexity: "simple", transport: "process", ...policy });
      expect(result).toMatchObject({ status: "completed", model: MODEL, text: "remote worker complete", exitCode: 0 });
      expect(result.text).not.toBe("fake worker complete");
      expect(fixture.intents[0]).toMatchObject({ kind: "remote", model: MODEL, complexity: "simple" });
      expect(fixture.bindings[0]).toMatchObject({ host: "placement-node" });
      expectFixtureRequests(fixture, result.id, true);
      expect(await readFile(sshLog, "utf8")).toContain("placement-alias");
      expect(await terminalStarted.promise).toMatchObject({ placementRunId: result.id, state: "finished", exitCode: 0 });
    } finally {
      await cleanup(fixture, manager, restoreEnvironment);
    }
  }, 15_000);

  it("keeps NATS task-presence failure unlaunched and records a best-effort never-started receipt", async () => {
    const restoreEnvironment = isolatePlacementEnvironment();
    const fixture = await createPlacementApiFixture("admit");
    let manager: AgentManager | undefined;
    const ordering: string[] = [];
    const withdrawn: string[] = [];
    const terminalStarted = deferred<PlacementTerminalRecord>();
    let published: AgentRunRecord | undefined;
    const taskPresence: AgentTaskPresence = {
      publishBeforeLaunch: async record => {
        ordering.push("taskPresence.publishBeforeLaunch");
        published = record;
        throw new Error("injected NATS task-presence publish failure");
      },
      withdraw: async id => {
        ordering.push("taskPresence.withdraw");
        withdrawn.push(id);
      },
    };
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");

    try {
      manager = makeManager(fixture, taskPresence, async record => {
        terminalStarted.resolve(record);
        throw new Error("injected terminal writer failure");
      });
      let runId: string | undefined;
      await expect(manager.run({ task: "NATS presence failure must prevent launch", model: MODEL, transport: "process" }))
        .rejects.toMatchObject({ code: "FABRIC_AGENT_TASK_PRESENCE_FAILED", launchOutcome: "unlaunched" });

      const terminalRecord = await terminalStarted.promise;
      runId = fixture.intents[0]?.runId;
      expect(runId).toBeTypeOf("string");
      expect(launch).not.toHaveBeenCalled();
      expect(ordering).toEqual(["taskPresence.publishBeforeLaunch", "taskPresence.withdraw"]);
      expect(withdrawn).toEqual([runId]);
      expect(published).toMatchObject({
        id: runId,
        status: "queued",
        transport: "process",
        model: MODEL,
        placementRunId: runId,
        placementKind: "process",
        placementHost: os.hostname().toLowerCase(),
        expectedSeconds: fixture.intents[0]?.expectedSeconds,
      });
      expect(terminalRecord).toMatchObject({
        placementRunId: runId,
        state: "never-started",
        reason: "spawn-aborted-before-worker-launch",
      });
      expectFixtureRequests(fixture, runId!, true);
      expect(Object.keys(fixture.bindings[0]!).sort()).toEqual(BINDING_KEYS);
      expect(fixture.bindings[0]).toMatchObject({ host: os.hostname().toLowerCase(), model: MODEL, provider: "openai" });
    } finally {
      launch.mockRestore();
      await cleanup(fixture, manager, restoreEnvironment);
    }
  }, 10_000);
});
