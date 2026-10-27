import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import { pathToFileURL } from "node:url";
import { afterEach, describe, expect, it } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { ResidencyClient } from "../src/residency/client.js";
import { ResidentActorClient } from "../src/residency/actor-client.js";
import { RESIDENT_HOST_FORMAT, residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import { getAgentRunner, registerAgentRunner, type FabricWorkerRunner } from "../src/runners.js";
import type { FabricActorInfo, FabricActorMessage } from "../src/actors/types.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { liveControlOwnerIncarnation } from "./helpers/live-control-owner.js";

// Keep reason: smarty-dev#16188(a), real process-local registration / resident ownership boundary.
// No model calls: the existing file-protocol runner pattern supplies an offline persistent worker.
const cleanups: Array<() => void | Promise<void>> = [];
afterEach(async () => { for (const cleanup of cleanups.splice(0).reverse()) await cleanup(); });
const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-custom-actor-"));
  cleanups.push(() => fs.rmSync(root, { recursive: true, force: true }));
  const identity = { id: "session:custom-main", name: "main", kind: "main" as const, sessionId: "custom-main" };
  const meshConfig = { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 };
  const mesh = new MeshStore(path.join(root, "mesh"), meshConfig.maxEventBytes, meshConfig.maxReadEvents);
  return { root, identity, mesh, meshConfig };
};
const adapter = (residentModule?: string): FabricWorkerRunner => ({
  id: "custom-actor", kind: "worker", label: "Custom actor",
  capabilities: { ...getAgentRunner("veda")!.capabilities, imageInput: false, persistentSessions: true },
  ...(residentModule ? { residentModule } : {}),
  launch(context) {
    const file = path.join(context.runDirectory, "custom-context.json");
    fs.writeFileSync(file, JSON.stringify(context));
    return { workerPath: path.resolve("tests/fixtures/persistent-custom-runner-worker.mjs"), workerArguments: [file] };
  },
});
const register = (runner: FabricWorkerRunner) => { cleanups.push(registerAgentRunner(runner)); };
const session = (runner: FabricWorkerRunner) => {
  const s = fixture();
  register(runner);
  const agents = new AgentManager(s.root, { ...DEFAULT_FABRIC_CONFIG.agents, runner: runner.id, transport: "process" }, {
    runRoot: path.join(s.root, "runs"), fullCodeMode: false,
  });
  cleanups.push(() => agents.close());
  const reopen = () => {
    const actors = new ActorManager(s.identity.sessionId, s.identity, s.mesh, s.meshConfig, agents, () => {}, {
      actorRoot: path.join(s.root, "actors"), persistent: true,
    });
    cleanups.push(() => actors.close());
    return actors;
  };
  return { ...s, agents, reopen, actors: reopen() };
};

interface Reply { id?: number; ready?: boolean; error?: string; result?: unknown }
const startResident = async (config: ResidentHostConfig) => {
  const configFile = path.join(config.residencyRoot, "config.json");
  fs.mkdirSync(config.residencyRoot, { recursive: true });
  fs.writeFileSync(configFile, JSON.stringify(config));
  const child = spawn("bun", [path.resolve("tests/fixtures/custom-actor-resident.ts"), configFile], {
    stdio: ["ignore", "pipe", "pipe", "ipc"],
  });
  let output = "";
  child.stdout!.on("data", data => { output += String(data); });
  child.stderr!.on("data", data => { output += String(data); });
  const exited = new Promise<void>(resolve => child.once("exit", () => resolve()));
  const receive = (matches: (reply: Reply) => boolean): Promise<Reply> => new Promise((resolve, reject) => {
    const timer = setTimeout(() => finish(new Error(`Resident reply timed out: ${output}`)), 20_000);
    const onExit = () => finish(new Error(`Resident exited: ${output}`));
    const onMessage = (reply: Reply) => { if (matches(reply)) finish(reply.error ? new Error(`${reply.error}\n${output}`) : undefined, reply); };
    const finish = (error?: Error, reply?: Reply) => {
      clearTimeout(timer); child.off("message", onMessage); child.off("exit", onExit);
      if (error) reject(error); else resolve(reply!);
    };
    child.on("message", onMessage); child.once("exit", onExit);
  });
  let sequence = 0;
  const request = async <T>(operation: string, actorId?: string): Promise<T> => {
    const id = ++sequence;
    const pending = receive(reply => reply.id === id);
    child.send({ id, operation, actorId });
    return (await pending).result as T;
  };
  let closed = false;
  const close = async () => {
    if (closed) return;
    closed = true;
    if (child.exitCode === null && child.signalCode === null) {
      try { await request("close"); }
      finally { if (child.connected) child.disconnect(); }
    }
    // Never delete fixture state while its owned host can still write.
    await Promise.race([exited, new Promise<never>((_, reject) => {
      const timer = setTimeout(() => { child.kill("SIGKILL"); reject(new Error(`Resident did not close: ${output}`)); }, 10_000);
      timer.unref(); exited.then(() => clearTimeout(timer));
    })]);
  };
  cleanups.push(close);
  await receive(reply => reply.ready === true || reply.error !== undefined);
  return { request, close, pid: child.pid! };
};
const durable = async () => {
  const s = fixture();
  const participants = new ParticipantDirectory(s.mesh, { enabled: true, hostId: s.identity.id, rootId: s.identity.id, identity: s.identity });
  participants.registerSource(() => [{
    format: 1, id: s.identity.id, kind: "root", rootId: s.identity.id, ownerHostId: s.identity.id,
    ownerIdentityId: s.identity.id, name: "main", status: "idle", residency: "session", runner: "pi", transport: "host",
    capabilities: ["steer", "followUp", "fabric"], cwd: s.root, sessionId: s.identity.sessionId,
    startedAt: Date.now(), updatedAt: Date.now(), controlProtocol: "v1",
  }]);
  await participants.start();
  cleanups.push(() => participants.close());
  const config: ResidentHostConfig = {
    format: RESIDENT_HOST_FORMAT, rootId: s.identity.id, sessionId: s.identity.sessionId,
    cwd: s.root, projectRoot: s.root, meshRoot: s.mesh.root,
    actorRoot: path.join(s.root, "actors"), sessionActorRoot: path.join(s.root, "session-actors"),
    residencyRoot: residentRoot(s.mesh.root, s.identity.id), fullCodeMode: false,
    agents: { ...DEFAULT_FABRIC_CONFIG.agents, runner: "custom-actor", transport: "process" }, mesh: s.meshConfig,
    retention: DEFAULT_FABRIC_CONFIG.retention, workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
    fabricExtensionPath: path.resolve("src/index.ts"), piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
  };
  const client = new ResidencyClient({ config, mesh: s.mesh, participants, mainAgent: {
    id: s.identity.id, local: true, matches: id => id === s.identity.id,
    info: () => { throw new Error("unused"); }, deliverAgent: () => ({ queued: true, messageId: "unused", routed: "main" }),
  } });
  cleanups.push(() => client.close());
  const control = new FabricControlPlane(s.mesh, s.identity, {
    enabled: true, hostId: s.identity.id, pollMs: 20, acknowledgementTimeoutMs: 5_000,
  });
  control.start(() => ({ accepted: false }));
  cleanups.push(() => control.close());
  const ask = async (id: string) => control.requestResult<FabricActorMessage>(client.hostId, id, "ask", {
    message: "Check persistent state", ownerIncarnation: await liveControlOwnerIncarnation(participants, id),
  }, client.hostId, { timeoutMs: 10_000 });
  return { ...s, config, client, ask };
};

// This module is generated only in the test's own fresh directory. Importing it in a new
// host appends the PID, proving registration was loaded there, not inherited from Vitest.
const residentModule = (root: string, persistentSessions = true) => {
  const file = path.join(root, "custom-runner.mjs");
  fs.writeFileSync(file, `
import fs from 'node:fs';
import path from 'node:path';
import { registerAgentRunner, getAgentRunner } from ${JSON.stringify(pathToFileURL(path.resolve("src/agents/runner-registry.ts")).href)};
fs.appendFileSync(${JSON.stringify(path.join(root, "imports.jsonl"))}, JSON.stringify(process.pid) + '\\n');
registerAgentRunner({ id: 'custom-actor', kind: 'worker', label: 'Custom actor', residentModule: ${JSON.stringify(file)},
  capabilities: { ...getAgentRunner('veda').capabilities, imageInput: false, persistentSessions: ${persistentSessions} },
  launch(context) {
    const file = path.join(context.runDirectory, 'custom-context.json');
    fs.writeFileSync(file, JSON.stringify(context));
    return { workerPath: ${JSON.stringify(path.resolve("tests/fixtures/persistent-custom-runner-worker.mjs"))}, workerArguments: [file] };
  }
});
`);
  return file;
};

describe("custom persistent actor runners (L8 a)", () => {
  it("creates a session actor using the configured registered runner and actually asks twice", async () => {
    const { actors } = session(adapter());
    const actor = await actors.create({ name: "session-custom", instructions: "Acknowledge" });
    expect(actor).toMatchObject({ runner: "custom-actor", residency: "session" });
    expect((await actors.ask(actor.id, "first")).text).toBe("ACK 1: session-custom");
    expect((await actors.ask(actor.id, "second")).text).toBe("ACK 2: session-custom");
  });

  it("preserves the custom runner and session across manager reload, without inventing a Pi kernel", async () => {
    const { actors, reopen } = session(adapter());
    const actor = await actors.create({ name: "reload-custom", instructions: "Acknowledge", runner: "custom-actor" });
    expect((await actors.ask(actor.id, "before reload")).text).toBe("ACK 1: reload-custom");
    await actors.close();
    const restored = reopen();
    expect(restored.status(actor.id)).toMatchObject({ runner: "custom-actor" });
    expect(restored.status(actor.id).kernel).toBeUndefined();
    expect((await restored.ask(actor.id, "after reload")).text).toBe("ACK 2: reload-custom");
  });

  it.each(["session", "durable"] as const)("rejects unknown and nonpersistent %s runners before creating actors", async residency => {
    const runner = { ...adapter(), capabilities: { ...adapter().capabilities, persistentSessions: false } };
    const { actors, root } = session(runner);
    await expect(actors.create({ name: "unknown", instructions: "no", runner: "unknown", residency }))
      .rejects.toThrow(/Unsupported Fabric agent runner/);
    for (const id of [runner.id, "veda"]) {
      await expect(actors.create({ name: "not-persistent", instructions: "no", runner: id, residency }))
        .rejects.toThrow(/persistentSessions/);
    }
    expect(actors.list()).toEqual([]);
    expect(fs.existsSync(path.join(root, "actors", "actors.json"))).toBe(false);
  });

  it("rejects a durable custom actor without residentModule at both the manager and client boundaries", async () => {
    const { actors } = session(adapter());
    await expect(actors.create({ name: "no-module", instructions: "no", residency: "durable" }))
      .rejects.toThrow(/declares no residentModule/);
    const s = await durable();
    await expect(s.client.createActor({ name: "no-module", instructions: "no", residency: "durable" }))
      .rejects.toThrow(/declares no residentModule/);
    expect(fs.existsSync(path.join(s.config.residencyRoot, "config.json"))).toBe(false);
  });

  it("rejects nonpersistent and unknown runners in the durable client before starting a host", async () => {
    const s = await durable();
    register({ ...adapter(), capabilities: { ...adapter().capabilities, persistentSessions: false } });
    for (const [runner, error] of [["custom-actor", /persistentSessions/], ["unknown", /Unsupported Fabric agent runner/]] as const) {
      await expect(s.client.createActor({ name: "reject", instructions: "no", residency: "durable", runner })).rejects.toThrow(error);
    }
    expect(fs.existsSync(path.join(s.config.residencyRoot, "config.json"))).toBe(false);
  });

  it("transfers registration to a fresh durable owner, reloads its module after restart and actually asks with the same session", async () => {
    const s = await durable();
    register(adapter(residentModule(s.root)));
    const first = await startResident(s.config);
    const actor = await s.client.createActor({ name: "durable-custom", instructions: "Acknowledge", residency: "durable" });
    expect(actor.runner).toBe("custom-actor");
    expect((await s.ask(actor.id)).text).toBe("ACK 1: durable-custom");
    await first.close();
    const second = await startResident(s.config);
    expect(second.pid).not.toBe(first.pid);
    expect(await second.request<FabricActorInfo>("status", actor.id)).toMatchObject({ id: actor.id, runner: "custom-actor", residency: "durable" });
    expect((await s.ask(actor.id)).text).toBe("ACK 2: durable-custom");
    const imports = fs.readFileSync(path.join(s.root, "imports.jsonl"), "utf8").trim().split("\n").map(Number);
    expect(imports).toEqual([first.pid, second.pid]);
    const rows = JSON.parse(fs.readFileSync(path.join(s.config.actorRoot, "actors.json"), "utf8"));
    expect(JSON.stringify(rows)).toContain("custom-runner.mjs");
  }, 60_000);

  it("retains a durable actor's custom identity but never launches Pi when its module loses registration on restart", async () => {
    const s = await durable();
    const module = residentModule(s.root);
    register(adapter(module));
    const first = await startResident(s.config);
    const actor = await s.client.createActor({ name: "unavailable-custom", instructions: "Acknowledge", residency: "durable" });
    expect((await s.ask(actor.id)).text).toBe("ACK 1: unavailable-custom");
    await first.close();
    fs.writeFileSync(module, "export {};\n");
    const second = await startResident(s.config);
    const restored = await second.request<FabricActorInfo>("status", actor.id);
    expect(restored.runner).toBe("custom-actor");
    expect(restored.kernel).toBeUndefined();
    await expect(s.ask(actor.id)).rejects.toThrow(/Unsupported Fabric agent runner/);
    expect(fs.readFileSync(actor.sessionFile!, "utf8")).toBe("1");
  }, 60_000);

  it.each(["missing", "unregistered", "nonpersistent"] as const)("refuses a %s resident module at the actual durable owner", async mode => {
    const s = await durable();
    const module = mode === "missing" ? path.join(s.root, "missing.mjs") : residentModule(s.root, mode !== "nonpersistent");
    if (mode === "unregistered") fs.writeFileSync(module, "export {};\n");
    register(adapter(module));
    await startResident(s.config);
    const error = mode === "nonpersistent" ? /persistentSessions/ : mode === "unregistered" ? /Unsupported Fabric agent runner/ : /Cannot find|ModuleNotFound/;
    await expect(s.client.createActor({ name: "refused", instructions: "No", residency: "durable" })).rejects.toThrow(error);
    expect(await new ResidentActorClient(s.mesh.root, s.identity.id).actors()).toEqual([]);
  }, 60_000);

  it("forwards the module through the nested ResidentActorClient as well", async () => {
    const s = await durable();
    register(adapter(residentModule(s.root)));
    await startResident(s.config);
    const proxy = new ResidentActorClient(s.mesh.root, s.identity.id);
    const actor = await proxy.createActor({ name: "nested-custom", instructions: "Acknowledge", runner: "custom-actor", residency: "durable" });
    expect((await s.ask(actor.id)).text).toBe("ACK 1: nested-custom");
  }, 60_000);
});
