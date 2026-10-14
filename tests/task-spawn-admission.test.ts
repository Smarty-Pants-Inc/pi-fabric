import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import type { AgentHandleInfo, AgentRunResult } from "../src/agents/types.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import {
  TASK_SPAWN_DENIED_CODE,
  TaskSpawnDeniedError,
  assertTaskSpawnAdmitted,
  captureTaskSpawnAdmission,
} from "../src/agents/spawn-admission.js";

describe("task spawn admission (smarty-dev#12187)", () => {
  it("lets Main spawn", () => {
    const admission = captureTaskSpawnAdmission({});
    expect(admission).toMatchObject({ kind: "main", spawnAllowed: true });
    expect(() => assertTaskSpawnAdmitted(admission, "spawn")).not.toThrow();
  });

  it("gives actor identity priority over PI_FABRIC_PARENT_RUN", () => {
    const admission = captureTaskSpawnAdmission({ PI_FABRIC_ACTOR_ID: "actor-1", PI_FABRIC_PARENT_RUN: "run-1", PI_FABRIC_GRANTED_RISKS: "" });
    expect(admission).toMatchObject({ kind: "actor", spawnAllowed: true });
  });

  it("refuses a non-recursive task with the fixed typed error", () => {
    const admission = captureTaskSpawnAdmission({ PI_FABRIC_PARENT_RUN: "run-1", PI_FABRIC_GRANTED_RISKS: "" });
    expect(admission).toMatchObject({ kind: "agent", id: "run-1", spawnAllowed: false });
    let caught: unknown;
    try { assertTaskSpawnAdmitted(admission, "run"); } catch (error) { caught = error; }
    expect(caught).toBeInstanceOf(TaskSpawnDeniedError);
    const error = caught as TaskSpawnDeniedError;
    expect(error.name).toBe("TaskSpawnDeniedError");
    expect(error.code).toBe(TASK_SPAWN_DENIED_CODE);
    expect(error.message.startsWith("task agents do not spawn; return your result to your spawner")).toBe(true);
    // The QuickJS bridge reads own data properties only.
    expect(Object.getOwnPropertyDescriptor(error, "name")?.value).toBe("TaskSpawnDeniedError");
    expect(Object.getOwnPropertyDescriptor(error, "code")?.value).toBe("FABRIC_TASK_SPAWN_DENIED");
  });

  it("admits a task launched recursive: true (granted agent risk)", () => {
    const admission = captureTaskSpawnAdmission({ PI_FABRIC_PARENT_RUN: "run-1", PI_FABRIC_GRANTED_RISKS: "agent" });
    expect(admission).toMatchObject({ kind: "agent", spawnAllowed: true });
  });

  it("is an immutable snapshot: later environment mutation grants nothing", () => {
    const environment: NodeJS.ProcessEnv = { PI_FABRIC_PARENT_RUN: "run-1", PI_FABRIC_GRANTED_RISKS: "read,write" };
    const admission = captureTaskSpawnAdmission(environment);
    environment.PI_FABRIC_GRANTED_RISKS = "agent";
    delete environment.PI_FABRIC_PARENT_RUN;
    expect(Object.isFrozen(admission)).toBe(true);
    expect(() => assertTaskSpawnAdmitted(admission, "spawn")).toThrow(TaskSpawnDeniedError);
  });
});

// Decision C (see .local/provider-tests-result.md): the real-process tests prove a launched task
// child is refused end to end; this deterministic supplement pins the public provider trust
// boundary itself: refusal happens before model policy, routing, cwd or residency work, the
// snapshot is taken at construction, and actor/grant/Main counterexamples still launch.
// The manager and resident host are mocked only so no process is launched.
describe("AgentsProvider launch admission (smarty-dev#12187)", () => {
  const roots: string[] = [];
  const managers: AgentManager[] = [];
  const handle: AgentHandleInfo = { id: "child", name: "child", status: "running", runner: "pi", transport: "process", cwd: process.cwd() };
  const completed: AgentRunResult = {
    ...handle, status: "completed", task: "t", text: "done", turns: 1, toolCalls: 0, startedAt: 1, updatedAt: 2,
    usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
  };
  type ProviderArgs = ConstructorParameters<typeof AgentsProvider>;
  const setup = () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-task-spawn-admission-"));
    roots.push(root);
    const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, { runRoot: root });
    managers.push(manager);
    const spies = {
      spawn: vi.spyOn(manager, "spawn").mockResolvedValue(handle),
      wait: vi.spyOn(manager, "wait").mockResolvedValue(completed),
      status: vi.spyOn(manager, "status").mockReturnValue(completed),
      detach: vi.spyOn(manager, "detachSignal").mockImplementation(() => {}),
      modelReason: vi.spyOn(manager, "assertExplicitModelReason"),
      resolveCwd: vi.spyOn(manager, "resolveCwd"),
      getAvailable: vi.fn(() => [{ provider: "test", id: "model" }]),
      find: vi.fn(() => undefined),
      refresh: vi.fn(),
    };
    const residency = {
      spawnAgent: vi.fn(async () => handle), createActor: vi.fn(), ensureHost: vi.fn(async () => {}), ensureActor: vi.fn(async () => {}),
    };
    const provider = new AgentsProvider(
      manager, {} as ProviderArgs[1], {} as ProviderArgs[2], {} as ProviderArgs[3],
      { scheduleRefresh: spies.refresh, async refresh() {} } as unknown as ProviderArgs[4],
      undefined, {} as ProviderArgs[6], () => false, residency as unknown as ProviderArgs[8], false,
    );
    const context = {
      cwd: process.cwd(), signal: undefined, parentToolCallId: "outer", nestedToolCallId: "nested",
      extensionContext: { modelRegistry: { getAvailable: spies.getAvailable, find: spies.find } },
      update() {}, activity() {},
    } as unknown as FabricInvocationContext;
    return { provider, context, spies, residency };
  };
  const stubCaller = (env: { parent?: string; actor?: string; risks?: string }) => {
    vi.stubEnv("PI_FABRIC_PARENT_RUN", env.parent);
    vi.stubEnv("PI_FABRIC_ACTOR_ID", env.actor);
    vi.stubEnv("PI_FABRIC_GRANTED_RISKS", env.risks);
  };
  const attempts: ["run" | "spawn", Record<string, unknown>][] = [
    ["run", { task: "t" }],
    ["spawn", { task: "t" }],
    ["run", { task: "t", model: "auto", routeClass: "task:exact-checks" }],
    ["spawn", { task: "t", model: "auto" }],
    ["spawn", { task: "t", residency: "durable" }],
    // Attempted self-upgrade: recursive describes the child, never the caller.
    ["run", { task: "t", recursive: true }],
    ["spawn", { task: "t", recursive: true, residency: "durable", model: "auto" }],
  ];
  const expectRefused = async (setupResult: ReturnType<typeof setup>, action: string, args: Record<string, unknown>) => {
    const { provider, context, spies, residency } = setupResult;
    let caught: unknown;
    try { await provider.invoke(action, args, context); } catch (error) { caught = error; }
    expect(caught).toBeInstanceOf(TaskSpawnDeniedError);
    expect(caught).toMatchObject({ name: "TaskSpawnDeniedError", code: TASK_SPAWN_DENIED_CODE });
    expect((caught as Error).message).toBe("task agents do not spawn; return your result to your spawner "
      + `(agents.${action} refused for task task-run-1; launch the task with recursive: true to grant spawning).`);
    for (const spy of Object.values(spies)) expect(spy).not.toHaveBeenCalled();
    for (const spy of Object.values(residency)) expect(spy).not.toHaveBeenCalled();
  };

  beforeEach(() => {
    vi.stubEnv("PI_FABRIC_DEPTH", undefined);
    vi.stubEnv("PI_FABRIC_BUDGET_FILE", undefined);
    vi.stubEnv("PI_FABRIC_BUDGET", undefined);
  });
  afterEach(async () => {
    await Promise.all(managers.splice(0).map((manager) => manager.close()));
    vi.restoreAllMocks();
    vi.unstubAllEnvs();
    for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  });

  it.each(attempts)("refuses a non-recursive task's %s %j before any side effect", async (action, args) => {
    stubCaller({ parent: "task-run-1" });
    await expectRefused(setup(), action, args);
  });

  it("keeps refusing after post-construction environment mutation", async () => {
    stubCaller({ parent: "task-run-1", risks: "read" });
    const result = setup();
    stubCaller({ actor: "actor-1", risks: "agent" });
    for (const [action, args] of attempts) await expectRefused(result, action, args);
  });

  it.each([
    ["Main", {}],
    ["recursive task (agent grant)", { parent: "task-run-1", risks: "read, agent" }],
    ["actor (identity wins over PI_FABRIC_PARENT_RUN)", { parent: "task-run-1", actor: "actor-1" }],
  ] as const)("admits %s through run and spawn", async (_label, env) => {
    stubCaller(env);
    const { provider, context, spies } = setup();
    // Mutating towards a task after construction cannot revoke a launch-time grant either.
    stubCaller({ parent: "task-run-2" });
    await expect(provider.invoke("spawn", { task: "t" }, context)).resolves.toMatchObject({ id: "child" });
    await expect(provider.invoke("run", { task: "t" }, context)).resolves.toMatchObject({ id: "child", status: "completed", text: "done" });
    expect(spies.spawn).toHaveBeenCalledTimes(2);
    expect(spies.modelReason).toHaveBeenCalledTimes(2);
  });
});
