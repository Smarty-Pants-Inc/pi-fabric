import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { build } from "esbuild";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { AgentMessageRouter } from "../src/providers/agents-message-router.js";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import { MainAgentController } from "../src/main-agent.js";
import { assertTaskMainTarget, applyTaskReturnAddress, readTaskReturnAddress, taskReturnAddressArguments } from "../src/agents/task-return-address.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";

const SPAWNER = "session:aaaaaaaa-1111-4111-8111-111111111111";
const ORG = "session:bbbbbbbb-2222-4222-8222-222222222222";
const KATE = "session:cccccccc-3333-4333-8333-333333333333";
const closers: Array<() => void | Promise<void>> = [];
afterEach(async () => {
  for (const close of closers.splice(0).reverse()) await close();
  vi.unstubAllEnvs();
});

const fixture = async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-task-return-"));
  closers.push(() => fs.rmSync(root, { recursive: true, force: true }));
  const fakePi = path.join(root, "pi.mjs");
  await build({ entryPoints: [path.resolve("tests/fixtures/task-return-address-pi.ts")], outfile: fakePi,
    bundle: true, platform: "node", format: "esm", packages: "external", banner: { js: "#!/usr/bin/env node" } });
  fs.chmodSync(fakePi, 0o755);
  // External package imports must resolve from this owned temporary executable too.
  fs.symlinkSync(path.resolve("node_modules"), path.join(root, "node_modules"), "junction");
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const delivered: Array<{ id: string; message: string; from: string; operation: string }> = [];
  const receiver = async (id: string, kind: "root" | "agent" = "root") => {
    const identity: MeshIdentity = { id, name: id === KATE ? "org-kate" : id, kind: kind === "root" ? "main" : "agent" };
    const record: FabricParticipantRecord = { format: 1, id, rootId: kind === "root" ? id : SPAWNER,
      ownerHostId: id, ownerIdentityId: id, kind, name: identity.name, status: "idle",
      runner: "pi", transport: "host", capabilities: ["steer", "followUp"], controlProtocol: "v1",
      cwd: root, startedAt: 1, updatedAt: Date.now() };
    const directory = new ParticipantDirectory(mesh, { enabled: true, hostId: id, rootId: record.rootId, identity });
    directory.registerSource(() => [record]);
    closers.push(() => directory.close());
    await directory.start();
    const control = new FabricControlPlane(mesh, identity, { enabled: true, hostId: id, pollMs: 10 });
    closers.push(() => control.close());
    const entries: any[] = [];
    const main = new MainAgentController({ on: () => () => {}, sendMessage: (message: any) => {
      delivered.push({ id, message: message.content, from: message.details.from.id, operation: message.details.delivery });
      entries.push({ type: "custom_message", customType: message.customType, details: message.details });
    } } as any, id, true, root);
    main.attachFollowUpDrain({ isIdle: () => true, hasPendingMessages: () => false,
      sessionManager: { getEntries: () => entries } } as any, 60_000,
      path.join(root, `journal-${encodeURIComponent(id)}.json`));
    closers.push(() => main.closeFollowUpDrain());
    const router = new AgentMessageRouter({} as any, { identity } as any, main, directory, control, binding => binding);
    control.start((command, from, signal) => {
      if (kind === "agent") {
        delivered.push({ id, message: command.message!, from: from.id, operation: command.operation });
        return { accepted: true, messageId: `agent-${delivered.length}` };
      }
      return router.acceptControl(command, from, signal);
    });
  };
  await receiver(SPAWNER); await receiver(ORG); await receiver(KATE);
  const spawn = async (targets: unknown[], options: { identityId?: string; actorId?: string; spawnerSessionId?: string; workerPath?: string } = {}) => {
    const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, deniedModels: [],
      maxDepth: 10, nice: 0, timeoutMs: 10_000 }, {
      workerPath: path.resolve(options.workerPath ?? "src/worker.ts"), piBinary: fakePi, runRoot: path.join(root, `runs-${Math.random()}`),
      meshRoot: mesh.root, projectRoot: root, mainAgentId: SPAWNER, fabricSessionId: SPAWNER.slice(8),
      identityId: options.identityId ?? SPAWNER, spawnerSessionId: options.spawnerSessionId ?? SPAWNER.slice(8),
    });
    closers.push(() => manager.close());
    const handle = await manager.spawn({ task: JSON.stringify({ targets }), transport: "process", extensions: false,
      ...(options.actorId ? { actorId: options.actorId, actorName: "durable-review" } : {}) });
    const result = await manager.wait(handle.id);
    expect(result.status, result.error ?? result.text).toBe("completed");
    const report = JSON.parse(result.text);
    expect(report.error).toBeUndefined();
    return { report, id: handle.id };
  };
  return { mesh, root, delivered, receiver, spawn };
};

const targets = (id: string, prefix: string) => ["steer", "followUp", "tell"].map(action => ({ action, id, message: `${prefix}-${action}` }));

describe("spawn-bound task return address through real process transport (#2950)", () => {
  // Source is always tested; a fresh build additionally exercises the published worker.
  it.each(["src/worker.ts", ...(fs.existsSync(path.resolve("dist/worker.js")) ? ["dist/worker.js"] : [])])("%s: delivers spawner and explicit org sends; refuses org-kate before publication for all three actions", async workerPath => {
    vi.stubEnv("PI_FABRIC_TASK_ESCALATION_TARGETS", JSON.stringify([ORG]));
    const f = await fixture();
    const { report, id } = await f.spawn([...targets("main", "report"), ...targets(ORG, "P0"), ...targets(KATE, "wrong-P0")], { workerPath });
    expect(report.env).toMatchObject({ spawnerId: SPAWNER, spawnerSessionId: SPAWNER.slice(8), marker: "1", parentRun: id, root: SPAWNER });
    expect(report.main).toMatchObject({ id: SPAWNER, sessionId: SPAWNER.slice(8) });
    expect(report.mainAgain).toMatchObject({ id: report.main.id, sessionId: report.main.sessionId });
    expect(report.sends.slice(0, 6).map((send: any) => send.ok)).toEqual(Array(6).fill(true));
    for (const send of report.sends.slice(6)) {
      expect(send.ok).toBe(false);
      expect(send.code).toBe("FABRIC_TASK_ESCALATION_TARGET_DENIED");
      expect(send.error).toContain(KATE); expect(send.error).toContain(SPAWNER); expect(send.error).toContain(ORG);
      expect(send.error).toContain("Nothing was delivered");
    }
    expect(f.delivered.filter(send => send.id === SPAWNER)).toHaveLength(3);
    expect(f.delivered.filter(send => send.id === ORG)).toHaveLength(3);
    expect(f.delivered.filter(send => send.id === KATE)).toHaveLength(0);
    const commands = f.mesh.read({ topic: "fabric.control.command" });
    expect(commands.filter(event => (event.data as any)?.targetId === KATE)).toHaveLength(0);
    expect(f.delivered.every(send => send.from === id)).toBe(true);
  }, 25_000);

  it("does not let bare session UUID aliases bypass the Main guard", async () => {
    vi.stubEnv("PI_FABRIC_TASK_ESCALATION_TARGETS", JSON.stringify([ORG]));
    const f = await fixture();
    const { report } = await f.spawn([
      { action: "followUp", id: ORG.slice(8), message: "bare-allowed-P0" },
      { action: "steer", id: KATE.slice(8), message: "bare-wrong-P0" },
    ]);
    expect(report.sends[0].ok).toBe(true);
    expect(report.sends[1]).toMatchObject({ ok: false, code: "FABRIC_TASK_ESCALATION_TARGET_DENIED" });
    expect(f.delivered.filter(send => send.id === ORG)).toHaveLength(1);
    expect(f.delivered.filter(send => send.id === KATE)).toHaveLength(0);
  }, 25_000);

  it("binds nested tasks to their immediate spawner; keeps ancestor and sibling sends unchanged", async () => {
    const f = await fixture();
    const parent = "d".repeat(32), sibling = "e".repeat(32);
    await f.receiver(parent, "agent"); await f.receiver(sibling, "agent");
    vi.stubEnv("PI_FABRIC_PARENT_RUN", parent);
    vi.stubEnv("PI_FABRIC_TASK_PROCESS_CHILD", "1");
    vi.stubEnv("PI_FABRIC_SPAWNER_ID", SPAWNER);
    vi.stubEnv("PI_FABRIC_SPAWNER_CHAIN", JSON.stringify([SPAWNER]));
    const { report } = await f.spawn([...targets("main", "nested"), ...targets(SPAWNER, "ancestor"), ...targets(sibling, "peer")],
      { identityId: parent, spawnerSessionId: "parent-native-session" });
    expect(report.main).toMatchObject({ id: parent, sessionId: "parent-native-session" });
    expect(report.mainAgain.id).toBe(parent);
    expect(report.sends.every((send: any) => send.ok), JSON.stringify(report.sends)).toBe(true);
    expect(f.delivered.filter(send => send.id === parent)).toHaveLength(3);
    expect(f.delivered.filter(send => send.id === SPAWNER)).toHaveLength(3);
    expect(f.delivered.filter(send => send.id === sibling)).toHaveLength(3);
  }, 25_000);

  it("does not mistake an actor-spawned task for a durable actor", async () => {
    const f = await fixture();
    const parent = "f".repeat(32);
    vi.stubEnv("PI_FABRIC_ACTOR_ID", parent); vi.stubEnv("PI_FABRIC_ACTOR_NAME", "parent-actor");
    const { report } = await f.spawn(targets(KATE, "actor-child-P0"), { identityId: parent });
    expect(report.env.actorId).toBeUndefined();
    expect(report.env.actorName).toBe("parent-actor"); // Write attribution remains compatible.
    expect(report.main.id).toBe(parent);
    for (const send of report.sends) {
      expect(send.ok).toBe(false);
      expect(send.code).toBe("FABRIC_TASK_ESCALATION_TARGET_DENIED");
    }
    expect(f.delivered).toHaveLength(0);
  }, 25_000);

  it("keeps explicit actor sends unrestricted even when launched by a task process", async () => {
    const f = await fixture();
    vi.stubEnv("PI_FABRIC_PARENT_RUN", "task-parent"); vi.stubEnv("PI_FABRIC_TASK_PROCESS_CHILD", "1");
    vi.stubEnv("PI_FABRIC_SPAWNER_ID", SPAWNER);
    const { report } = await f.spawn(targets(KATE, "actor"), { actorId: "actor-review" });
    expect(report.env.marker).toBeUndefined();
    expect(report.sends.every((send: any) => send.ok), JSON.stringify(report.sends)).toBe(true);
    expect(f.delivered.filter(send => send.id === KATE)).toHaveLength(3);
  }, 25_000);

  it("leaves a Main's public send path unrestricted", async () => {
    const f = await fixture();
    const identity: MeshIdentity = { id: SPAWNER, name: "Main", kind: "main" };
    const directory = new ParticipantDirectory(f.mesh, { enabled: true, hostId: SPAWNER, rootId: SPAWNER, identity });
    const control = new FabricControlPlane(f.mesh, identity, { enabled: true, hostId: SPAWNER, pollMs: 10 });
    closers.push(() => control.close()); control.start(() => ({ accepted: false }));
    const main = new MainAgentController({} as any, SPAWNER, true, f.root);
    const provider = new AgentsProvider({ cwd: f.root } as any, { identity } as any, {} as any,
      main, directory, control, {} as any);
    for (const target of targets(KATE, "Main-can-send")) {
      await provider.invoke(target.action, { id: target.id, message: target.message },
        { cwd: f.root, update() {}, activity() {} } as any);
    }
    expect(f.delivered.filter(send => send.id === KATE)).toHaveLength(3);
  }, 25_000);
});

describe("exact task return-address policy", () => {
  it("freezes JSON allowlists in launch flags, validates malformed input, and exempts non-process children", () => {
    const env = { PI_FABRIC_TASK_ESCALATION_TARGETS: JSON.stringify([ORG]) };
    const args = taskReturnAddressArguments(SPAWNER, "native-session", SPAWNER, env);
    env.PI_FABRIC_TASK_ESCALATION_TARGETS = JSON.stringify([KATE]);
    const child = applyTaskReturnAddress({ ...env, PI_FABRIC_PARENT_RUN: "run" }, args);
    const address = readTaskReturnAddress(child)!;
    expect(address.escalationTargets).toEqual([ORG]);
    expect(() => assertTaskMainTarget(address, KATE)).toThrow("allowed targets:");
    expect(() => assertTaskMainTarget(address, ORG)).not.toThrow();
    expect(readTaskReturnAddress({ PI_FABRIC_PARENT_RUN: "tmux-run" })).toBeUndefined();
    expect(() => taskReturnAddressArguments(SPAWNER, undefined, SPAWNER, { PI_FABRIC_TASK_ESCALATION_TARGETS: '["org-kate"]' })).toThrow("session:<id>");
    expect(() => taskReturnAddressArguments(SPAWNER, undefined, SPAWNER, { PI_FABRIC_TASK_ESCALATION_TARGETS: 'broken' })).toThrow("JSON array");
  });
});
