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
const taskText = (guest: string, label: string) => `${MARKER}${JSON.stringify({ guest, label })}`;
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
  const launch = async (guest: string, label: string, options: Record<string, unknown> = {}) => {
    const handle = await manager.spawn({ task: taskText(guest, label), transport: "process", extensions: false, ...options });
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
    const provider = new AgentsProvider(f.manager, { identity, mesh: f.mesh } as any, {} as any, main, directory, control, {} as any);
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
  }, 60_000);
});
