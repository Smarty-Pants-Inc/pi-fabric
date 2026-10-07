import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import senderHeadersHook, { resolveSenderHeaders, senderHeaderValue, smartyTaskLane } from "../src/guards/sender-headers.js";
import piFabric from "../src/index.js";

// Every sender-relevant variable, cleared so the ambient lane (this test may run inside one) cannot leak.
const SENDER_ENV = ["SMARTY_ROLE", "SMARTY_LANE", "TASK_OUT", "PI_FABRIC_ACTOR_ID", "PI_FABRIC_ACTOR_NAME",
  "PI_FABRIC_PARENT_RUN", "PI_FABRIC_AGENT_NAME", "PI_FABRIC_TASK_PROCESS_CHILD", "PI_FABRIC_SPAWNER_ID",
  "PI_FABRIC_SPAWNER_KIND", "PI_FABRIC_SPAWNER_RUN", "PI_FABRIC_SPAWNER_NAME", "PI_FABRIC_SPAWNER_SESSION_ID"];
const clean = (env: Record<string, string> = {}): void => {
  for (const key of SENDER_ENV) vi.stubEnv(key, undefined);
  for (const [key, value] of Object.entries(env)) vi.stubEnv(key, value);
};
const log = (kind: string, headers: unknown): void => { console.log(`[sender-headers] ${kind} ${JSON.stringify(headers)}`); };
const REV = "0123456789abcdef0123456789abcdef01234567";

afterEach(() => vi.unstubAllEnvs());

describe("sender headers (smarty-dev#6207)", () => {
  it("Main: SMARTY_ROLE without @rev, its mesh participant (session) name, no spawner", () => {
    clean({ SMARTY_ROLE: `project-agent@${REV}`, SMARTY_LANE: "lane-fallback", PI_FABRIC_SPAWNER_NAME: "leaked" });
    const headers = resolveSenderHeaders(process.env, "herdr-main-7");
    log("main", headers);
    expect(headers).toEqual({ "X-Smarty-Role": "project-agent", "X-Smarty-Agent": "herdr-main-7" });
    // No session name: SMARTY_LANE; nothing known: every header omitted, never guessed.
    expect(resolveSenderHeaders(process.env)).toEqual({ "X-Smarty-Role": "project-agent", "X-Smarty-Agent": "lane-fallback" });
    clean();
    log("main-unknown", resolveSenderHeaders(process.env));
    expect(resolveSenderHeaders(process.env)).toEqual({});
  });

  it("smarty-task lane Main: the lane from the launcher's TASK_OUT=<tasks>/<lane>/artifacts", () => {
    clean({ TASK_OUT: "/srv/scratch/paul/tasks/direct/fv2-senderhdr/artifacts", SMARTY_LANE: "other" });
    const headers = resolveSenderHeaders(process.env, "session-name");
    log("smarty-task-lane", headers);
    expect(headers).toEqual({ "X-Smarty-Agent": "fv2-senderhdr" });
    expect(smartyTaskLane({ TASK_OUT: "/tmp/out" })).toBeUndefined();
  });

  it("process task child: role task, its Fabric agent name, the spawner's name else session id", () => {
    clean({ SMARTY_ROLE: `task-agent@${REV}`, PI_FABRIC_TASK_PROCESS_CHILD: "1", PI_FABRIC_PARENT_RUN: "run-1",
      PI_FABRIC_AGENT_NAME: "fix-tests", PI_FABRIC_SPAWNER_NAME: "herdr-main-7", PI_FABRIC_SPAWNER_SESSION_ID: "01a0cd9c-7c24",
      TASK_OUT: "/srv/x/lane/artifacts" });
    const headers = resolveSenderHeaders(process.env, "ignored");
    log("process-task", headers);
    expect(headers).toEqual({ "X-Smarty-Role": "task", "X-Smarty-Agent": "fix-tests", "X-Smarty-Spawner": "herdr-main-7" });
    vi.stubEnv("PI_FABRIC_SPAWNER_NAME", undefined);
    expect(resolveSenderHeaders(process.env)["X-Smarty-Spawner"]).toBe("01a0cd9c-7c24");
  });

  it("actor worker: actor role with its name class, the actor name, the spawning Main", () => {
    clean({ SMARTY_ROLE: "project-agent", PI_FABRIC_ACTOR_ID: "actor-uuid", PI_FABRIC_ACTOR_NAME: "fleet-review-astra",
      PI_FABRIC_PARENT_RUN: "run-2", PI_FABRIC_SPAWNER_ID: "session:abc", PI_FABRIC_SPAWNER_KIND: "main", PI_FABRIC_SPAWNER_NAME: "herdr-main-7" });
    const headers = resolveSenderHeaders(process.env);
    log("actor", headers);
    expect(headers).toEqual({ "X-Smarty-Role": "actor:review", "X-Smarty-Agent": "fleet-review-astra", "X-Smarty-Spawner": "herdr-main-7" });
    // Unnamed actor: no guessed id slice; a session-id spawner only without a name.
    vi.stubEnv("PI_FABRIC_ACTOR_NAME", undefined); vi.stubEnv("PI_FABRIC_SPAWNER_NAME", undefined);
    expect(resolveSenderHeaders(process.env)).toEqual({ "X-Smarty-Role": "actor", "X-Smarty-Spawner": "session:abc" });
  });

  it.each(["evil\r\nX-Injected: yes", "evil\n", "a".repeat(129), "has space", "tok=secret", ""])("rejects CR/LF, over-long and non-name values: %j", value => {
    expect(senderHeaderValue(value)).toBeUndefined();
    clean({ SMARTY_ROLE: value, SMARTY_LANE: value, PI_FABRIC_PARENT_RUN: "r", PI_FABRIC_AGENT_NAME: value, PI_FABRIC_SPAWNER_NAME: value });
    expect(resolveSenderHeaders(process.env, value)).toEqual({});
  });
  it("accepts exactly 128 bounded ASCII characters", () => {
    const value = `a:b@c/d.e_f-${"x".repeat(116)}`;
    expect(value).toHaveLength(128);
    expect(senderHeaderValue(value)).toBe(value);
  });

  it("the hook mutates Pi's header map in place, per request, keeping other headers", () => {
    clean({ SMARTY_ROLE: "project-agent@abc" });
    let hook: ((event: { headers: Record<string, string> }) => unknown) | undefined;
    let name = "first";
    senderHeadersHook({ on: (_event: string, handler: typeof hook) => { hook = handler; }, getSessionName: () => name } as unknown as ExtensionAPI);
    const headers: Record<string, string> = { Authorization: "unmodified" };
    expect(hook?.({ headers })).toBeUndefined();
    expect(headers).toEqual({ Authorization: "unmodified", "X-Smarty-Role": "project-agent", "X-Smarty-Agent": "first" });
    name = "renamed"; hook?.({ headers });
    expect(headers["X-Smarty-Agent"]).toBe("renamed");
  });

  const fabricHeaderHandlers = async (): Promise<Array<(event: { headers: Record<string, string> }) => unknown>> => {
    const handlers = new Map<string, Array<(...args: unknown[]) => unknown>>();
    const pi = {
      events: { emit: vi.fn(), on: vi.fn(() => () => undefined) },
      getActiveTools: vi.fn(() => []), getAllTools: vi.fn(() => []), getSessionName: () => "herdr-main-7",
      on: vi.fn((event: string, handler: (...args: unknown[]) => unknown) => {
        handlers.set(event, [...handlers.get(event) ?? [], handler]);
      }),
      registerCommand: vi.fn(), registerMessageRenderer: vi.fn(), registerTool: vi.fn(), setActiveTools: vi.fn(),
    } as unknown as ExtensionAPI;
    await piFabric(pi);
    const result = handlers.get("before_provider_headers") ?? [];
    for (const shutdown of handlers.get("session_shutdown") ?? []) await shutdown();
    return result as Array<(event: { headers: Record<string, string> }) => unknown>;
  };

  it("Fabric's entry registers the hook in a Main, and leaves worker children to their -e hook", async () => {
    clean({ SMARTY_ROLE: "project-agent@abc" });
    const main = await fabricHeaderHandlers();
    const headers: Record<string, string> = {};
    for (const handler of main) await handler({ headers });
    log("main-via-fabric-entry", headers);
    expect(headers).toEqual({ "X-Smarty-Role": "project-agent", "X-Smarty-Agent": "herdr-main-7" });
    clean({ PI_FABRIC_PARENT_RUN: "run-3" });
    const child = await fabricHeaderHandlers();
    const childHeaders: Record<string, string> = {};
    for (const handler of child) await handler({ headers: childHeaders });
    expect(childHeaders).toEqual({});
  });
});

const workerPath = path.resolve("dist/worker.js");
describe.skipIf(!fs.existsSync(workerPath))("sender headers in real built worker children (smarty-dev#6207)", () => {
  const roots: string[] = [];
  const managers: AgentManager[] = [];
  afterEach(async () => {
    await Promise.all(managers.splice(0).map(manager => manager.close()));
    for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  });
  const run = async (facts: Record<string, unknown>, spawnerName?: () => string | undefined) => {
    clean({ SMARTY_ROLE: `project-agent@${REV}` });
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-sender-worker-")); roots.push(root);
    const scenario = path.join(root, "scenario"); fs.writeFileSync(scenario, "success");
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(root, "agent"));
    vi.stubEnv("FAKE_MODEL_SCENARIO", scenario);
    const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 10_000, retainRuns: true }, {
      workerPath, piBinary: path.resolve("tests/fixtures/fake-pi-sender.mjs"), runRoot: path.join(root, "runs"),
      mainAgentId: "session:main-1", identityId: "session:main-1", spawnerSessionId: "main-session-1",
      ...(spawnerName ? { spawnerName } : {}),
    }); managers.push(manager);
    const result = await manager.run({ task: "harmless", model: "openai-codex/gpt-5.6-sol", transport: "process", extensions: false, ...facts });
    const events = fs.readFileSync(path.join(root, "runs", result.id, "events.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line));
    return { result, launch: events.find(event => event.type === "fake_sender_headers") };
  };

  it("process task child: loads the -e hook with --no-extensions and sends role task, its name and the Main's name", async () => {
    const { result, launch } = await run({ name: "fix-tests" }, () => "herdr-main-7");
    log("worker process-task", launch.headers);
    expect(result.status).toBe("completed");
    expect(launch).toMatchObject({ hookLoaded: true });
    expect(launch.headers).toEqual({ "X-Session-Id": "unmodified", "X-Smarty-Role": "task", "X-Smarty-Agent": "fix-tests", "X-Smarty-Spawner": "herdr-main-7" });
  });

  it("process task child of an unnamed Main: the spawner's session id, never a guessed name", async () => {
    const { launch } = await run({ name: "fix-tests" }, () => undefined);
    log("worker process-task-unnamed-main", launch.headers);
    expect(launch.headers["X-Smarty-Spawner"]).toBe("main-session-1");
  });

  it("actor worker: actor role with name class, the actor name and the spawning Main's name", async () => {
    const { result, launch } = await run({ actorId: "trusted-actor", actorName: "fleet-review-astra" }, () => "herdr-main-7");
    log("worker actor", launch.headers);
    expect(result.status).toBe("completed");
    expect(launch.headers).toEqual({ "X-Session-Id": "unmodified", "X-Smarty-Role": "actor:review", "X-Smarty-Agent": "fleet-review-astra", "X-Smarty-Spawner": "herdr-main-7" });
  });
});
