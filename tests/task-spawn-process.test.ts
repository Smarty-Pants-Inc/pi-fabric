// smarty-dev#12187: leaf-by-default task spawn admission through the real process transport.
// Every task below is launched by a real AgentManager through the source worker.ts process
// transport into a deterministic Pi RPC stand-in (tests/fixtures/task-spawn-admission-pi.ts).
// The stand-in runs the task's "turn" as a QuickJS guest against the real AgentsProvider and a
// real nested AgentManager, so refusals are observed as guest-visible name/code, and nested
// children are observed as real handles with terminal statuses.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { build } from "esbuild";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ActorManager } from "../src/actors/manager.js";
import { GlobalActorRegistry } from "../src/actors/global-registry.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import { MainAgentController } from "../src/main-agent.js";
import { QuickJsRuntime } from "../src/runtime/quickjs-runtime.js";

const MAIN = "session:aaaaaaaa-1212-4187-8187-121871218712";
const DENIED_NAME = "TaskSpawnDeniedError";
const DENIED_CODE = "FABRIC_TASK_SPAWN_DENIED";
const DENIED_RULE = "task agents do not spawn; return your result to your spawner";
const MARKER = "ADMISSION-TASK:";
// The Main's identity must not be inherited from whatever process runs vitest.
const AMBIENT = ["PI_FABRIC_PARENT_RUN", "PI_FABRIC_ACTOR_ID", "PI_FABRIC_ACTOR_NAME", "PI_FABRIC_GRANTED_RISKS",
  "PI_FABRIC_TASK_PROCESS_CHILD", "PI_FABRIC_SPAWNER_ID", "PI_FABRIC_DEPTH", "PI_FABRIC_RUN_ROOT", "PI_FABRIC_MAIN_AGENT_ID",
  "PI_FABRIC_TEST_ADMISSION_WORKER", "PI_FABRIC_TEST_ADMISSION_EXTENSION"];

const saved = new Map<string, string | undefined>();
const closers: Array<() => void | Promise<void>> = [];
beforeEach(() => {
  for (const key of AMBIENT) { saved.set(key, process.env[key]); delete process.env[key]; }
  process.env.PI_FABRIC_TEST_ADMISSION_WORKER = path.resolve("src/worker.ts");
  process.env.PI_FABRIC_TEST_ADMISSION_EXTENSION = path.resolve("src/index.ts");
});
afterEach(async () => {
  for (const close of closers.splice(0).reverse()) await close();
  for (const [key, value] of saved) { if (value === undefined) delete process.env[key]; else process.env[key] = value; }
  saved.clear();
});

/** A task whose deterministic turn is the given QuickJS guest program. */
const taskText = (guest: string, label: string, extra: Record<string, unknown> = {}) =>
  `${MARKER}${JSON.stringify({ guest, label, ...extra })}`;
/** Guest-side source of a task literal (for a child spawned by a guest). */
const guestTask = (guest: string, label: string) => JSON.stringify(taskText(guest, label));

// Guest probe: try one creation call, report only the guest-visible refusal fields.
const probe = (call: string) => `(async () => { try { const value = await ${call}; return { ok: true, value }; }
  catch (error) { return { ok: false, name: error.name, code: error.code, message: error.message }; } })()`;
const leafChild = (label: string) => `{ task: ${guestTask("return { leaf: true };", label)}, transport: "process", extensions: false }`;

const fixture = async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-task-spawn-"));
  closers.push(() => fs.rmSync(root, { recursive: true, force: true }));
  const fakePi = path.join(root, "pi.mjs");
  await build({ entryPoints: [path.resolve("tests/fixtures/task-spawn-admission-pi.ts")], outfile: fakePi,
    bundle: true, platform: "node", format: "esm", packages: "external", banner: { js: "#!/usr/bin/env node" } });
  fs.chmodSync(fakePi, 0o755);
  fs.symlinkSync(path.resolve("node_modules"), path.join(root, "node_modules"), "junction");
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const runRoot = path.join(root, "runs");
  const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, deniedModels: [],
    maxDepth: 10, nice: 0, timeoutMs: 30_000 }, {
    workerPath: path.resolve("src/worker.ts"), fabricExtensionPath: path.resolve("src/index.ts"), piBinary: fakePi, runRoot,
    meshRoot: mesh.root, projectRoot: root, mainAgentId: MAIN, fabricSessionId: MAIN.slice(8),
    identityId: MAIN, spawnerSessionId: MAIN.slice(8),
  });
  closers.push(() => manager.close());
  /** Main launches a task (real worker process) and waits for its own final result. */
  const launch = async (guest: string, label: string, options: Record<string, unknown> = {}, extra: Record<string, unknown> = {}) => {
    const handle = await manager.spawn({ task: taskText(guest, label, extra), transport: "process", extensions: false, ...options });
    const result = await manager.wait(handle.id);
    expect(result.status, result.error ?? result.text).toBe("completed");
    const report = JSON.parse(result.text);
    expect(report.error, JSON.stringify(report)).toBeUndefined();
    expect(report.guestError, JSON.stringify(report)).toBeUndefined();
    expect(report.label).toBe(label);
    return { handle, result, report };
  };
  /** Runs nested under this task's run directory; empty when the task created nothing. */
  const nestedRuns = (id: string) => {
    const nested = path.join(runRoot, id, "nested");
    return fs.existsSync(nested) ? fs.readdirSync(nested).filter(name => /^[0-9a-f]{16,}$/.test(name)) : [];
  };
  return { root, mesh, manager, launch, nestedRuns, fakePi };
};

const expectDenied = (outcome: any) => {
  expect(outcome, JSON.stringify(outcome)).toMatchObject({ ok: false, name: DENIED_NAME, code: DENIED_CODE });
  expect(outcome.message.startsWith(DENIED_RULE)).toBe(true);
  expect(outcome.message).toContain("recursive: true");
};

const expectActorDenied = (outcome: any) => {
  expect(outcome, JSON.stringify(outcome)).toMatchObject({ ok: false, name: DENIED_NAME, code: DENIED_CODE });
  expect(outcome.message).toContain("Task-to-actor two-hop rule");
  expect(outcome.message).toContain("even with recursive: true");
};

describe("task spawn admission through the real worker process transport (#12187)", () => {
  it.each([
    ["default (recursive omitted)", {}],
    ["explicit recursive: false", { recursive: false }],
  ])("%s: a leaf task is refused agents.spawn and agents.run in its guest, yet its own result reaches Main", async (_label, launchOptions) => {
    const f = await fixture();
    const guest = `return {
      spawn: await ${probe(`agents.spawn(${leafChild("must-not-launch-spawn")})`)},
      run: await ${probe(`agents.run(${leafChild("must-not-launch-run")})`)},
      // Requesting recursion for the child grants the caller nothing.
      spawnRecursiveChild: await ${probe(`agents.spawn({ ...${leafChild("must-not-launch-recursive")}, recursive: true })`)},
      runRecursiveChild: await ${probe(`agents.run({ ...${leafChild("must-not-launch-recursive-run")}, recursive: true })`)},
      final: "leaf-final-answer",
    };`;
    const { handle, report } = await f.launch(guest, "leaf", launchOptions);
    expect(report.identity).toEqual({ id: handle.id, kind: "agent" });
    expect(report.parentRun).toBe(handle.id);
    // worker.ts exported the empty launch grant; the post-start env tamper to "agent" did not help.
    expect(report.grantedRisks).toBe("");
    for (const key of ["spawn", "run", "spawnRecursiveChild", "runRecursiveChild"]) expectDenied(report.guest[key]);
    expect(report.guest.spawn.message).toContain(handle.id);
    expect(report.guest.final).toBe("leaf-final-answer");
    // No child handle was created host-side, and no nested run directory exists.
    expect(report.children).toEqual([]);
    expect(f.nestedRuns(handle.id)).toEqual([]);
    expect(f.manager.list().map(record => record.id)).toEqual([handle.id]);
  }, 60_000);

  it("a task launched recursive: true spawns one child, receives its result, and that child is again a leaf", async () => {
    const f = await fixture();
    const grandchildGuest = `return { nested: await ${probe(`agents.spawn(${leafChild("grandchild-must-not-launch")})`)},
      final: "child-final-answer" };`;
    const guest = `const handle = await agents.spawn({ task: ${guestTask(grandchildGuest, "child")}, transport: "process", extensions: false });
      const waited = await agents.wait({ id: handle.id });
      return { handle: { id: handle.id, status: handle.status }, waited: { status: waited.status, text: waited.text } };`;
    const { handle, report } = await f.launch(guest, "recursive-parent", { recursive: true, extensions: true });
    expect(report.identity).toEqual({ id: handle.id, kind: "agent" });
    expect(report.grantedRisks.split(",")).toContain("agent");
    // The child handle is real, observed by the recursive task's own manager, and terminal.
    const childId = report.guest.handle.id;
    expect(typeof childId).toBe("string");
    expect(childId).not.toBe(handle.id);
    expect(report.guest.waited.status).toBe("completed");
    expect(report.children).toEqual([{ id: childId, status: "completed" }]);
    // The child's own final result reached the recursive task.
    const child = JSON.parse(report.guest.waited.text);
    expect(child.label).toBe("child");
    expect(child.identity).toEqual({ id: childId, kind: "agent" });
    expect(child.parentRun).toBe(childId);
    // A child launched without recursive: true is a leaf even under a recursive parent.
    expect(child.grantedRisks).toBe("");
    expectDenied(child.guest.nested);
    expect(child.guest.final).toBe("child-final-answer");
    expect(child.children).toEqual([]);
  }, 90_000);

  it("actor counterexample: an explicit actor activation still spawns a leaf task and receives its result", async () => {
    const f = await fixture();
    const guest = `const handle = await agents.spawn(${leafChild("actor-child")});
      const waited = await agents.wait({ id: handle.id });
      return { id: handle.id, status: waited.status, text: waited.text };`;
    const { report } = await f.launch(guest, "actor", { actorId: "actor-12187", actorName: "admission-actor" });
    expect(report.identity.kind).toBe("actor");
    expect(report.guest.status).toBe("completed");
    expect(report.children).toEqual([{ id: report.guest.id, status: "completed" }]);
    const child = JSON.parse(report.guest.text);
    expect(child).toMatchObject({ label: "actor-child", identity: { id: report.guest.id, kind: "agent" }, grantedRisks: "" });
    expect(child.guest).toEqual({ leaf: true });
  }, 90_000);

  // SEC-1/SEC-2: every public creation/delegation action is refused for a leaf before any actor
  // state, deferred handoff, or child launch; the direct executeHandoff boundary is refused too.
  const delegationProbes = `{
      create: await ${probe(`agents.create({ name: "leaf-actor", instructions: "x" })`)},
      createActor: await ${probe(`agents.createActor({ name: "leaf-actor-2", instructions: "x" })`)},
      importActor: await ${probe(`agents.import({ id: "global-template" })`)},
      ask: await ${probe(`agents.ask({ id: "actor-existing-12187", message: "hi" })`)},
      handoff: await ${probe(`agents.handoff({ model: "fake/fake" })`)},
      handoffRecursive: await ${probe(`agents.handoff({ model: "fake/fake", recursive: true })`)},
    }`;
  const DELEGATION_KEYS = ["create", "createActor", "importActor", "ask", "handoff", "handoffRecursive"];

  it.each([
    ["default (recursive omitted)", {}],
    ["explicit recursive: false", { recursive: false }],
  ])("%s: a leaf task is refused create/createActor/import/ask/handoff and direct executeHandoff (SEC-1/SEC-2)", async (_l, launchOptions) => {
    const f = await fixture();
    const guest = `return { probes: ${delegationProbes}, final: "leaf-final-answer" };`;
    const { handle, report } = await f.launch(guest, "leaf-delegation", launchOptions,
      { directHandoff: true, realActorFlow: true, existingActors: true });
    expect(report.identity).toEqual({ id: handle.id, kind: "agent" });
    expect(report.grantedRisks).toBe("");
    for (const key of ["create", "createActor", "importActor", "ask"]) expectActorDenied(report.guest.probes[key]);
    for (const key of ["handoff", "handoffRecursive"]) expectDenied(report.guest.probes[key]);
    expect(report.guest.final).toBe("leaf-final-answer");
    // Direct host executeHandoff at the modeled outer boundary: refused with the same typed error.
    expect(report.handoff).toMatchObject({ ok: false, name: DENIED_NAME, code: DENIED_CODE });
    // Nothing reached actor state, no deferred handoff request, no child.
    expect(report.actorCalls).toEqual([]);
    expect(report.deferredHandoffs).toBe(0);
    expect(report.children).toEqual([]);
    expect(f.nestedRuns(handle.id)).toEqual([]);
    expect(f.manager.list().map(record => record.id)).toEqual([handle.id]);
  }, 60_000);

  it.each([
    ["recursive: true task", { recursive: true, extensions: true }],
    ["explicit actor", { actorId: "actor-12187-h", actorName: "admission-actor-h" }],
  ])("%s: delegation actions pass admission, and a deferred handoff launches a real child to a terminal result", async (_l, launchOptions) => {
    const f = await fixture();
    const handoffChild = `return { spawn: await ${probe(`agents.spawn(${leafChild("handoff-grandchild-must-not-launch")})`)} };`;
    const guest = `return { handoff: await ${probe(`agents.handoff({ model: "fake/fake", recursive: true,
      task: ${guestTask(handoffChild, "handoff-leaf")} })`)} };`;
    const { handle, report } = await f.launch(guest, "permitted-delegation", launchOptions, { runDeferredHandoff: true });
    expect(report.guest.handoff).toMatchObject({ ok: true, value: { scheduled: true } });
    expect(report.deferredHandoffs).toBe(1);
    // Real handoff child: launched by the task's real manager, terminal, own final result.
    expect(report.handoff, JSON.stringify(report.handoff)).toMatchObject({ ok: true, status: "completed" });
    expect(report.handoff.id).not.toBe(handle.id);
    expect(report.children).toContainEqual({ id: report.handoff.id, status: "completed" });
    const child = JSON.parse(report.handoff.text);
    expect(child.identity).toEqual({ id: report.handoff.id, kind: "agent" });
    // Both the guest scheduling request and direct boundary execution supplied recursive: true.
    expect(child.grantedRisks).toBe("");
    expectDenied(child.guest.spawn);
    expect(child.children).toEqual([]);
  }, 90_000);

  it("recursive task cannot create, ask, or message real existing/dormant actors, including generic calls and aliases", async () => {
    const f = await fixture();
    const actorTurn = `return { escaped: await agents.spawn(${leafChild("two-hop-grandchild-must-not-launch")}) };`;
    const message = guestTask(actorTurn, "must-not-activate-actor");
    const guest = `const outcomes = {
      create: await ${probe(`agents.create({ name: "escape-actor", instructions: "x", recursive: true })`)},
      createActor: await ${probe(`agents.createActor({ name: "escape-actor-2", instructions: "x" })`)},
      importActor: await ${probe(`agents.import({ id: "missing-template" })`)},
    };
    for (const id of ["actor-existing-12187", "actor-dormant-12187"]) {
      for (const verb of ["ask", "tell", "followUp", "steer", "send"]) {
        outcomes[id + ":" + verb] = await ${probe(`tools.call({ ref: "agents." + verb, args: { to: id, message: ${message} } })`)};
      }
    }
    return outcomes;`;
    const { handle, report } = await f.launch(guest, "recursive-actor-refusal", { recursive: true, extensions: true },
      { realActorFlow: true, existingActors: true, tamperIdentity: true });
    expect(report.grantedRisks.split(",")).toContain("agent");
    expect(Object.keys(report.guest)).toHaveLength(13);
    for (const outcome of Object.values(report.guest)) expectActorDenied(outcome);
    expect(report.actorsBefore).toHaveLength(2);
    expect(report.actorsBefore.find((actor: any) => actor.name === "actor-dormant-12187").status).toBe("dormant");
    expect(report.actorsAfter).toEqual(report.actorsBefore);
    expect(report.children).toEqual([]);
    expect(f.nestedRuns(handle.id)).toEqual([]);
  }, 60_000);

  // Real actor lifecycle (Decision C limits modeling to registry/session/context inference only):
  // the task's fixture builds an actual ActorManager + GlobalActorRegistry, so create -> ask -> actor
  // worker bootstrap -> actor-turn spawn -> leaf child terminal runs through real code end to end.
  it("explicit actor: real actor create -> ask -> actor turn spawns a leaf child that reports to its bound actor", async () => {
    const f = await fixture();
    const launchOptions = { actorId: "actor-12187-r", actorName: "admission-actor-r" };
    const reportGuest = `const spawner = await agents.main();
      const delivered = await agents.followUp({ id: "spawner", message: "bound-spawner-report" });
      return { spawnerId: spawner.id, delivered, spawn: await ${probe(`agents.spawn(${leafChild("actor-grandchild-must-not-launch")})`)} };`;
    const actorTurn = `const handle = await agents.spawn({ task: ${guestTask(reportGuest, "actor-turn-child")}, transport: "process", extensions: false });
      const waited = await agents.wait({ id: handle.id });
      return { id: handle.id, status: waited.status, text: waited.text };`;
    const guest = `const actor = await agents.create({ name: "delegation-proof", instructions: "Do deterministic task",
        residency: "session", extensions: false });
      const reply = await agents.ask({ id: actor.id, message: ${guestTask(actorTurn, "actor-turn")} });
      return { actorId: actor.id, reply };`;
    const { report } = await f.launch(guest, "real-actor-flow", launchOptions, { realActorFlow: true });
    expect(typeof report.guest.actorId).toBe("string");
    // ask resolves to a FabricActorMessage whose text is the actor turn's own final report.
    const turn = JSON.parse(report.guest.reply.text);
    expect(turn.label).toBe("actor-turn");
    expect(turn.identity.kind).toBe("actor");
    // A completed actor worker can still report a failed guest. Keep its actual
    // refusal/timeout visible instead of hiding it behind an undefined-property error.
    expect(turn.error, JSON.stringify(turn)).toBeUndefined();
    expect(turn.guestError, JSON.stringify(turn)).toBeUndefined();
    expect(turn.guest, JSON.stringify(turn)).toBeDefined();
    expect(turn.guest.status).toBe("completed");
    expect(turn.children).toEqual([{ id: turn.guest.id, status: "completed" }]);
    const child = JSON.parse(turn.guest.text);
    expect(child).toMatchObject({ label: "actor-turn-child", identity: { id: turn.guest.id, kind: "agent" }, grantedRisks: "" });
    expectDenied(child.guest.spawn);
    expect(child.children).toEqual([]);
    expect(child.guest.spawnerId).toBe(report.guest.actorId);
    expect(child.guest.delivered).toMatchObject({ queued: true });
    // The real owner acknowledged publication into the actual ActorManager mailbox.
    expect(report.routedReports).toHaveLength(1);
    expect(report.routedReports[0]).toMatchObject({ targetId: report.guest.actorId, accepted: true,
      mailbox: { id: report.guest.actorId } });
    expect(report.routedReports[0].mailbox.queued).toBeGreaterThan(0);
  }, 120_000);

  it("Main still spawns through its real AgentsProvider from a QuickJS guest", async () => {
    const f = await fixture();
    const identity: MeshIdentity = { id: MAIN, name: "Main", kind: "main" };
    const directory = new ParticipantDirectory(f.mesh, { enabled: true, hostId: MAIN, rootId: MAIN, identity });
    closers.push(() => directory.close());
    await directory.start();
    const control = new FabricControlPlane(f.mesh, identity, { enabled: true, hostId: MAIN, pollMs: 10 });
    closers.push(() => control.close()); control.start(() => ({ accepted: false }));
    const main = new MainAgentController({} as any, MAIN, true, f.root);
    closers.push(() => main.closeFollowUpDrain());
    const actors = new ActorManager(MAIN.slice(8), identity, f.mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, f.manager,
      () => {}, { actorRoot: path.join(f.root, "main-actors"), persistent: true, mainAgent: main, rootId: MAIN });
    closers.push(() => actors.close());
    const provider = new AgentsProvider(f.manager, actors, new GlobalActorRegistry(path.join(f.root, "global-actors"), 64 * 1024),
      main, directory, control, {} as any);
    const context = { cwd: f.root, update() {}, activity() {},
      extensionContext: { cwd: f.root, model: undefined, modelRegistry: { getAvailable: () => [], getAll: () => [], find: () => undefined },
        sessionManager: { getEntries: () => [], getSessionId: () => MAIN.slice(8) } } } as any;
    const result = await new QuickJsRuntime().execute(`const handle = await agents.spawn(${leafChild("main-child")});
      return { id: handle.id };`, async (ref, args) => provider.invoke(ref.slice("agents.".length), args, context),
      { timeoutMs: 30_000, memoryLimitBytes: 64 * 1024 * 1024 });
    expect(result.error).toBeUndefined();
    const id = (result.value as { id: string }).id;
    const waited = await f.manager.wait(id);
    expect(waited.status, waited.error ?? waited.text).toBe("completed");
    expect(JSON.parse(waited.text)).toMatchObject({ label: "main-child", identity: { id, kind: "agent" }, guest: { leaf: true } });

    // Main counterexample for SEC-1/SEC-2: create is admitted (downstream modeled-actor failure only),
    // handoff defers, and the modeled outer boundary launches a real child to a terminal result.
    const deferred: Array<Record<string, unknown>> = [];
    const fake = { provider: "fake", id: "fake", api: "openai-completions", name: "fake", reasoning: false, input: ["text"],
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 8192, maxTokens: 1024 };
    const handoffContext = { ...context, deferHandoff: async (args: Record<string, unknown>) => { deferred.push(args); return { scheduled: true }; },
      extensionContext: { ...context.extensionContext, model: fake,
        modelRegistry: { getAvailable: () => [fake], getAll: () => [fake], find: (p: string, m: string) => p === "fake" && m === "fake" ? fake : undefined } } };
    const delegated = await new QuickJsRuntime().execute(`return {
      create: await ${probe(`agents.create({ name: "main-actor", instructions: "x", residency: "session", extensions: false })`)},
      handoff: await ${probe(`agents.handoff({ model: "fake/fake", recursive: true,
        task: ${guestTask(`return { spawn: await ${probe(`agents.spawn(${leafChild("main-handoff-grandchild-must-not-launch")})`)} };`, "main-handoff-leaf")} })`)} };`,
      async (ref, args) => provider.invoke(ref.slice("agents.".length), args, handoffContext),
      { timeoutMs: 30_000, memoryLimitBytes: 64 * 1024 * 1024 });
    const probes = delegated.value as any;
    expect(probes.create).toMatchObject({ ok: true });
    const actorReply = await provider.invoke("ask", { id: probes.create.value.id,
      message: taskText("return { actorTurn: true };", "main-actor-turn") }, handoffContext) as any;
    expect(JSON.parse(actorReply.text)).toMatchObject({ label: "main-actor-turn", identity: { kind: "actor" }, guest: { actorTurn: true } });
    expect(probes.handoff).toMatchObject({ ok: true, value: { scheduled: true } });
    expect(deferred).toHaveLength(1);
    const seed = { sourceSessionId: MAIN.slice(8), sourceBranchLeafId: "seed-leaf",
      sourceBranch: [{ type: "message", id: "seed-leaf", parentId: null, timestamp: new Date(0).toISOString(),
        message: { role: "user", content: "handoff seed", timestamp: 0 } }],
      sourceModel: { provider: "fake", modelId: "fake" },
      outerToolResult: { role: "toolResult", toolCallId: "seed-call", toolName: "fabric_exec",
        content: [{ type: "text", text: "handoff scheduled" }], isError: false, timestamp: 0 } } as any;
    const before = new Set(f.manager.list().map(record => record.id));
    await provider.executeHandoff({ ...deferred[0]!, recursive: true }, handoffContext, seed);
    const handoff = { id: f.manager.list().map(record => record.id).find(candidate => !before.has(candidate))! };
    expect(handoff.id).toBeDefined();
    const handoffResult = await f.manager.wait(handoff.id);
    expect(handoffResult.status, handoffResult.error ?? handoffResult.text).toBe("completed");
    const handoffChild = JSON.parse(handoffResult.text);
    expect(handoffChild).toMatchObject({ identity: { id: handoff.id, kind: "agent" }, grantedRisks: "", children: [] });
    expectDenied(handoffChild.guest.spawn);
  }, 90_000);
});
