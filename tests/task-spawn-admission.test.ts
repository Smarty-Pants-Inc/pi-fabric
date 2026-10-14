import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import type { AgentHandleInfo, AgentRunResult } from "../src/agents/types.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import { AgentMessageRouter } from "../src/providers/agents-message-router.js";
import * as wake from "../src/residency/wake.js";
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
    // Actor and global-template plumbing: every side-effecting entry point is a spy so a refusal
    // can prove nothing behind the gate ran. Identity is Main-like; the gate uses its own snapshot.
    const actor = { id: "actor-x", name: "actor-x", runner: "pi", status: "idle" };
    const template = { id: "tpl-1", name: "tpl", instructions: "be helpful", model: "test/model" };
    const actors = {
      create: vi.fn(async () => actor),
      ask: vi.fn(async () => ({ id: "m1", text: "ok" })),
      validateDirectMessage: vi.fn(),
      owns: vi.fn(() => true),
      resolveBinding: vi.fn(() => ({})),
      status: vi.fn(() => actor),
    };
    const actorManager = { identity: { kind: "main", id: "main-1" }, mesh: { root: root }, ...actors };
    const templates = {
      create: vi.fn(() => template),
      resolve: vi.fn(() => template),
      toRequest: vi.fn(() => ({ name: template.name, instructions: template.instructions, model: template.model })),
    };
    const deferHandoff = vi.fn(async (args: Record<string, unknown>) => ({ deferred: true, model: args.model }));
    const provider = new AgentsProvider(
      manager, actorManager as unknown as ProviderArgs[1], templates as unknown as ProviderArgs[2], {} as ProviderArgs[3],
      { get: () => undefined, scheduleRefresh: spies.refresh, async refresh() {} } as unknown as ProviderArgs[4],
      undefined, {} as ProviderArgs[6], () => false, residency as unknown as ProviderArgs[8], false,
    );
    const context = {
      cwd: process.cwd(), signal: undefined, parentToolCallId: "outer", nestedToolCallId: "nested",
      extensionContext: { modelRegistry: { getAvailable: spies.getAvailable, find: spies.find } },
      update() {}, activity() {},
    } as unknown as FabricInvocationContext;
    // Same context with the active-fabric_exec handoff deferral callback supplied.
    const handoffContext = { ...context, deferHandoff } as unknown as FabricInvocationContext;
    return { provider, context, handoffContext, spies, residency, actors, templates, deferHandoff };
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

  // SEC-1/SEC-2 (.local/code-security-audit.md): actor creation/import/ask and handoff are
  // delegation routes too. A leaf must get the same typed refusal before templates, resident
  // host, actor create/ask, registry/model preparation, deferHandoff or manager launch.
  const delegationAttempts: [string, Record<string, unknown>][] = [
    ["create", { name: "a", instructions: "i" }],
    ["createActor", { name: "a", instructions: "i" }],
    ["create", { name: "a", instructions: "i", residency: "durable" }],
    ["createActor", { name: "a", instructions: "i", scope: "global" }],
    ["create", { name: "a", instructions: "i", model: "auto" }],
    ["import", { id: "tpl-1" }],
    ["import", { name: "tpl", as: "copy" }],
    ["ask", { id: "actor-x", message: "go" }],
    ["handoff", { model: "test/model" }],
    // A leaf may not grant its handoff child recursion either.
    ["handoff", { model: "test/model", recursive: true }],
  ];
  const expectDelegationRefused = async (
    result: ReturnType<typeof setup>, run: () => Promise<unknown>, action?: string,
  ) => {
    let caught: unknown;
    try { await run(); } catch (error) { caught = error; }
    expect(caught).toBeInstanceOf(TaskSpawnDeniedError);
    expect(caught).toMatchObject({ name: "TaskSpawnDeniedError", code: TASK_SPAWN_DENIED_CODE });
    const message = (caught as Error).message;
    expect(message).toContain(action && ["create", "createActor", "import", "ask"].includes(action)
      ? "Task-to-actor two-hop rule" : "task agents do not spawn; return your result to your spawner");
    expect(message).toContain("task-run-1");
    if (action) expect(message).toContain(`agents.${action} refused`);
    for (const group of [result.spies, result.residency, result.actors, result.templates]) {
      for (const [name, spy] of Object.entries(group)) {
        // Actor lookup/validation precede resolved-target admission; neither can activate or publish.
        if (action === "ask" && group === result.actors && ["status", "validateDirectMessage"].includes(name)) continue;
        expect(spy).not.toHaveBeenCalled();
      }
    }
    expect(result.deferHandoff).not.toHaveBeenCalled();
    // Keep subsequent probes independent; read-only actor resolution is not a side effect.
    result.actors.status.mockClear();
    result.actors.validateDirectMessage.mockClear();
  };
  const seed = { sourceModel: "test/model" } as unknown as Parameters<AgentsProvider["executeHandoff"]>[2];

  it.each(delegationAttempts)("refuses a non-recursive task's %s %j before any side effect", async (action, args) => {
    stubCaller({ parent: "task-run-1" });
    const result = setup();
    await expectDelegationRefused(result, () => result.provider.invoke(action, args, result.handoffContext), action);
  });

  it("refuses direct public handoff() and executeHandoff() before deferral or manager launch", async () => {
    stubCaller({ parent: "task-run-1" });
    const result = setup();
    await expectDelegationRefused(result, () => result.provider.handoff({ model: "test/model" }, result.handoffContext));
    await expectDelegationRefused(result, () => result.provider.handoff({ model: "test/model", recursive: true }, result.handoffContext));
    await expectDelegationRefused(result, () => result.provider.executeHandoff({ model: "test/model", task: "t" }, result.context, seed));
    await expectDelegationRefused(result, () => result.provider.executeHandoff({ model: "test/model", task: "t", recursive: true }, result.context, seed));
  });

  it("keeps refusing delegation routes after post-construction environment mutation", async () => {
    stubCaller({ parent: "task-run-1", risks: "read" });
    const result = setup();
    stubCaller({ actor: "actor-1", risks: "agent" });
    for (const [action, args] of delegationAttempts) {
      await expectDelegationRefused(result, () => result.provider.invoke(action, args, result.handoffContext), action);
    }
    await expectDelegationRefused(result, () => result.provider.executeHandoff({ model: "test/model", task: "t" }, result.context, seed));
  });

  // Allowed counterexamples: the gate must not be what stops Main, actors or recursive tasks.
  // Plumbing is mocked, so these assert only that the call passes the admission gate and reaches
  // its first mocked collaborator; they do not claim a full actor lifecycle or real handoff.
  const notRefused = async (run: () => Promise<unknown>): Promise<void> => {
    let caught: unknown;
    try { await run(); } catch (error) { caught = error; }
    expect(caught).not.toBeInstanceOf(TaskSpawnDeniedError);
  };
  it.each([
    ["Main", {}],
    ["actor (identity wins over PI_FABRIC_PARENT_RUN)", { parent: "task-run-1", actor: "actor-1" }],
  ] as const)("admits %s through actor creation, import, ask and handoff", async (_label, env) => {
    stubCaller(env);
    const result = setup();
    stubCaller({ parent: "task-run-2" });
    const { provider, context, handoffContext, templates, actors, spies, deferHandoff } = result;
    spies.find.mockImplementation(((providerName: string, id: string) =>
      providerName === "test" && id === "model" ? { provider: "test", id: "model" } : undefined) as never);
    await notRefused(() => provider.invoke("createActor", { name: "g", instructions: "i", scope: "global" }, context));
    expect(templates.create).toHaveBeenCalledTimes(1);
    await notRefused(() => provider.invoke("create", { name: "a", instructions: "i" }, context));
    await notRefused(() => provider.invoke("import", { id: "tpl-1" }, context));
    expect(templates.resolve).toHaveBeenCalled();
    await notRefused(() => provider.invoke("ask", { id: "actor-x", message: "go" }, context));
    expect(actors.validateDirectMessage).toHaveBeenCalled();
    await notRefused(() => provider.invoke("handoff", { model: "test/model" }, handoffContext));
    await notRefused(() => provider.handoff({ model: "test/model", recursive: true }, handoffContext));
    expect(deferHandoff.mock.calls.length + spies.find.mock.calls.length + spies.getAvailable.mock.calls.length).toBeGreaterThan(0);
    await notRefused(() => provider.executeHandoff({ model: "test/model", task: "t" }, context, seed));
  });

  it.each(delegationAttempts.filter(([action]) => action !== "handoff"))(
    "refuses a recursively granted task's %s %j at the actor boundary", async (action, args) => {
      stubCaller({ parent: "task-run-1", risks: "agent" });
      const result = setup();
      stubCaller({ actor: "forged-actor", risks: "agent" });
      await expectDelegationRefused(result, () => result.provider.invoke(action, args, result.context), action);
    });

  it("forces recursive:false at handoff scheduling and execution despite guest recursive:true", async () => {
    stubCaller({});
    const { provider, handoffContext, context, spies, deferHandoff } = setup();
    spies.find.mockImplementation(((providerName: string, id: string) =>
      providerName === "test" && id === "model" ? { provider: "test", id: "model" } : undefined) as never);
    await provider.handoff({ model: "test/model", recursive: true }, handoffContext);
    expect(deferHandoff.mock.calls[0]![0].recursive).toBe(false);
    await provider.executeHandoff({ model: "test/model", task: "t", recursive: true }, context, seed);
    expect(spies.spawn.mock.calls[0]![0].recursive).toBe(false);
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

// Trust-boundary supplement for Astra P1: real-process tests cover actual ActorManager state;
// these two route shapes pin refusal before the dormant wake lookup or residency activation.
describe("task actor routing admission", () => {
  afterEach(() => { vi.restoreAllMocks(); vi.unstubAllEnvs(); });
  it.each(["dormant", "remote"] as const)("refuses a recursive task's %s actor route before wake/ensure/publication after env tampering", async mode => {
    vi.stubEnv("PI_FABRIC_PARENT_RUN", "task-run-1");
    vi.stubEnv("PI_FABRIC_ACTOR_ID", undefined);
    vi.stubEnv("PI_FABRIC_GRANTED_RISKS", "agent");
    vi.stubEnv("PI_FABRIC_TASK_PROCESS_CHILD", "1");
    vi.stubEnv("PI_FABRIC_SPAWNER_ID", "actor-own-spawner");
    vi.stubEnv("PI_FABRIC_SPAWNER_CHAIN", undefined);
    vi.stubEnv("PI_FABRIC_TASK_ESCALATION_TARGETS", undefined);
    const actor = { id: "actor-other", name: "other", rootId: "main-1", residency: "durable", status: "dormant", runner: "pi" };
    const participant = mode === "remote" ? { ...actor, kind: "actor", local: false, ownerHostId: "host-1",
      ownerIdentityId: "owner-1", capabilities: ["ask", "followUp", "steer"], remoteHost: "peer-1" } : undefined;
    const dormant = vi.spyOn(wake, "dormantActorRoute");
    const ensureActor = vi.fn();
    const tell = vi.fn();
    const ask = vi.fn();
    const publish = vi.fn();
    const router = new AgentMessageRouter(
      { status: () => { throw new Error("Unknown Fabric agent"); } } as any,
      { identity: { kind: "agent", id: "task-run-1" }, mesh: { root: "/unused-mesh" },
        status: () => actor, owns: () => false, validateDirectMessage() {}, tell, ask, steerRemote: publish } as any,
      { id: "main-1", matches: () => false } as any,
      { get: () => participant, lastKnown: () => undefined } as any,
      { request: publish } as any, binding => binding,
      { ensureActor, hostId: "host-1", options: { config: { rootId: "main-1", meshRoot: "/unused-mesh" } } } as any,
    );
    vi.stubEnv("PI_FABRIC_ACTOR_ID", "forged-actor");
    vi.stubEnv("PI_FABRIC_PARENT_RUN", undefined);
    vi.stubEnv("PI_FABRIC_SPAWNER_ID", actor.id);
    for (const run of [
      () => router.resolveActorMessageTarget("other"),
      () => router.routeMessage("other", "report", undefined, "followUp"),
      () => router.routeMessage("other", "report", undefined, "steer"),
    ]) {
      await expect(run()).rejects.toMatchObject({ name: "TaskSpawnDeniedError", code: TASK_SPAWN_DENIED_CODE });
    }
    for (const sideEffect of [dormant, ensureActor, tell, ask, publish]) expect(sideEffect).not.toHaveBeenCalled();
  });
});
