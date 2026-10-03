import { randomUUID } from "node:crypto";
import { ResidentHost } from "../src/residency/host.js";
import { ResidencyClient } from "../src/residency/client.js";
import { RESIDENT_HOST_FORMAT, residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
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
      cwd: root, sessionId: kind === "root" ? id.slice(8) : `native-${id}`, startedAt: 1, updatedAt: Date.now() };
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
    return { identity, directory, main, control };
  };
  const requester = await receiver(SPAWNER); await receiver(ORG); await receiver(KATE);
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
  return { mesh, root, delivered, receiver, spawn, requester, fakePi };
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

  it("does not let published root names bypass the Main guard (#321 integration)", async () => {
    vi.stubEnv("PI_FABRIC_TASK_ESCALATION_TARGETS", JSON.stringify([ORG]));
    const f = await fixture();
    const { report } = await f.spawn(targets("org-kate", "named-wrong-P0"));
    for (const send of report.sends) {
      expect(send).toMatchObject({ ok: false, code: "FABRIC_TASK_ESCALATION_TARGET_DENIED" });
      expect(send.error).toContain(KATE);
      expect(send.error).toContain("Nothing was delivered");
    }
    expect(f.delivered.filter(send => send.id === KATE)).toHaveLength(0);
    expect(f.mesh.read({ topic: "fabric.control.command" }).filter(event => (event.data as any)?.targetId === KATE)).toHaveLength(0);
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
    expect(report.env.actorName).toBeUndefined(); // Main #2643 isolates task identity from its actor parent.
    expect(report.spawner).toMatchObject({ id: parent, kind: "actor" });
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

describe.skipIf(process.platform === "win32")("durable public spawn return address", () => {
  const durableFixture = async () => {
    const f = await fixture();
    const config: ResidentHostConfig = {
      format: RESIDENT_HOST_FORMAT, rootId: SPAWNER, sessionId: SPAWNER.slice(8), cwd: f.root,
      projectRoot: f.root, meshRoot: f.mesh.root, actorRoot: path.join(f.root, "actors"),
      residencyRoot: residentRoot(f.mesh.root, SPAWNER), fullCodeMode: true,
      agents: { ...DEFAULT_FABRIC_CONFIG.agents, model: "", deniedModels: [], budgetUsd: 0, nice: 0, timeoutMs: 10_000 },
      mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 10 }, retention: DEFAULT_FABRIC_CONFIG.retention,
      workerPath: path.resolve("src/worker.ts"), fabricExtensionPath: path.resolve("dist/index.js"),
      piBinary: f.fakePi, claudeBinary: "claude", vedaBinary: "veda",
      piModels: { available: [{ provider: "fixture", id: "model" }], aliases: {}, defaultModel: "fixture/model" },
    };
    fs.mkdirSync(config.residencyRoot, { recursive: true, mode: 0o700 });
    fs.writeFileSync(path.join(config.residencyRoot, "config.json"), JSON.stringify(config));
    const host = new ResidentHost(config);
    closers.push(() => host.close());
    await host.start();
    const client = new ResidencyClient({ config, mesh: f.mesh, participants: f.requester.directory, mainAgent: f.requester.main });
    closers.push(() => client.close());
    const manager = new AgentManager(f.root, config.agents, { runRoot: path.join(f.root, "caller-runs"), mainAgentId: SPAWNER, identityId: SPAWNER });
    closers.push(() => manager.close());
    const provider = new AgentsProvider(manager, { identity: f.requester.identity } as any, {} as any,
      f.requester.main, f.requester.directory, f.requester.control, {} as any, undefined, client);
    const context = { cwd: f.root, extensionContext: { sessionManager: { getEntries: () => [], getSessionId: () => SPAWNER.slice(8) } }, update() {}, activity() {} } as any;
    return { ...f, host, client, config, provider, context };
  };

  it("binds agents.main and all main sends to the requesting Main, not the hidden resident executor", async () => {
    vi.stubEnv("PI_FABRIC_TASK_ESCALATION_TARGETS", JSON.stringify([ORG]));
    const f = await durableFixture();
    // The host already exists; neither its identity nor a later ambient edit grants a return target.
    vi.stubEnv("PI_FABRIC_TASK_ESCALATION_TARGETS", JSON.stringify([KATE]));
    for (let i = 0; i < 2; i++) {
      const handle = await f.provider.invoke("spawn", {
        task: JSON.stringify({ targets: [...targets("main", `durable-${i}`), ...targets(ORG, "allowed"), ...targets(KATE, "denied")] }),
        residency: "durable", transport: "process", extensions: false,
        // Unrecognized task-supplied identity fields must never become the trusted envelope.
        caller: { id: KATE }, returnAddress: { spawnerId: KATE },
      }, f.context) as { id: string };
      const result = await f.client.waitAgent(handle.id);
      if (!("text" in result)) throw new Error("Expected the requesting Main's full result, not a completion summary");
      expect(result.status, result.error).toBe("completed");
      const report = JSON.parse(result.text);
      expect(report.main).toMatchObject({ id: SPAWNER, sessionId: SPAWNER.slice(8) });
      expect(report.main.id).not.toBe(f.host.identity.id);
      expect(report.mainAgain.id).toBe(SPAWNER);
      expect(report.sends.slice(0, 6).every((send: any) => send.ok)).toBe(true);
      expect(report.sends.slice(6).every((send: any) => send.code === "FABRIC_TASK_ESCALATION_TARGET_DENIED")).toBe(true);
    }
    expect(f.delivered.filter(send => send.id === SPAWNER)).toHaveLength(6);
    expect(f.delivered.filter(send => send.id === ORG)).toHaveLength(6);
    expect(f.delivered.filter(send => send.id === KATE || send.id === f.host.identity.id)).toHaveLength(0);
    expect(f.mesh.read({ topic: "fabric.control.command" }).filter(event => (event.data as any)?.targetId === KATE)).toHaveLength(0);
  }, 30_000);

  it.each([false, true])("refuses absent and forged launch envelopes before commit or worker launch (cached retry: %s)", async cachedRetry => {
    const f = await durableFixture();
    const idempotencyKey = "caller-bound-retry";
    if (cachedRetry) {
      const handle = await f.client.spawnAgent({ task: JSON.stringify({ targets: [] }), residency: "durable",
        transport: "process", extensions: false, idempotencyKey });
      expect((await f.client.waitAgent(handle.id)).status).toBe("completed");
    }
    const self = f.requester.directory.self();
    const caller = { id: self.id, rootId: self.rootId, sessionId: self.sessionId,
      ownerHostId: self.ownerHostId, ownerIdentityId: self.ownerIdentityId, kind: self.kind,
      returnAddress: { spawnerId: self.id, spawnerSessionId: self.sessionId, ancestors: [SPAWNER], escalationTargets: [] } };
    const forged = [undefined, { ...caller, ownerHostId: f.host.hostId }, { ...caller, sessionId: "forged" },
      { ...caller, rootId: KATE }, { ...caller, returnAddress: { ...caller.returnAddress, spawnerId: f.host.identity.id } }];
    for (const value of forged) {
      const requestId = randomUUID();
      fs.writeFileSync(path.join(f.config.residencyRoot, "requests", `${requestId}.json`), JSON.stringify({
        format: RESIDENT_HOST_FORMAT, operation: "spawnBound", requestId, rootId: SPAWNER,
        ...(cachedRetry ? { idempotencyKey } : {}),
        request: { task: "must not launch", residency: "durable", transport: "process" }, caller: value, createdAt: Date.now(),
      }));
      const responseFile = path.join(f.config.residencyRoot, "responses", `${requestId}.json`);
      await vi.waitFor(() => expect(fs.existsSync(responseFile)).toBe(true), { timeout: 5_000, interval: 20 });
      expect(JSON.parse(fs.readFileSync(responseFile, "utf8"))).toMatchObject({ ok: false, error: expect.stringContaining("absent or forged binding refused") });
      expect(fs.existsSync(path.join(f.config.residencyRoot, "decisions", `${requestId}.json`))).toBe(false);
    }
    expect(f.host.agents.list()).toHaveLength(cachedRetry ? 1 : 0);
    expect(f.client.listAgents()).toHaveLength(cachedRetry ? 1 : 0);
  }, 30_000);

  it("fails closed at public dispatch when no native caller session binding exists", async () => {
    const f = await durableFixture();
    const client = new ResidencyClient({ config: f.config, mesh: f.mesh, mainAgent: f.requester.main,
      participants: { self: () => ({ ...f.requester.directory.self(), sessionId: undefined }) } as any });
    closers.push(() => client.close());
    await expect(client.spawnAgent({ task: "must not launch", transport: "process" })).rejects.toThrow("trusted live caller");
    expect(f.host.agents.list()).toHaveLength(0);
  }, 30_000);
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
