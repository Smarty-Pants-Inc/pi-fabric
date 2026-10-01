import { fork, type ChildProcess } from "node:child_process";
import { once } from "node:events";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import type { FabricActorInfo } from "../src/actors/types.js";
import { MeshStore } from "../src/mesh/store.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import { ResidentActorClient } from "../src/residency/actor-client.js";
import { ResidencyClient } from "../src/residency/client.js";
import { ResidentHost } from "../src/residency/host.js";
import { RESIDENT_HOST_FORMAT, residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";

// CI can supply a pinned prior release; local rollout probes use installed B70.
const legacyRelease = process.env.PI_FABRIC_LEGACY_RELEASE ?? path.join(os.homedir(), ".local/share/smarty-dev/fabric/releases/b243bc926beec5da8717135893dd6b578af73f7b");
const legacyHost = path.join(legacyRelease, "dist/residency/host.js");
const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-mixed-release-"));
  const config: ResidentHostConfig = {
    format: RESIDENT_HOST_FORMAT, rootId: "session:mixed", sessionId: "mixed",
    cwd: process.cwd(), projectRoot: process.cwd(), meshRoot: path.join(root, "mesh"),
    actorRoot: path.join(root, "actors"), residencyRoot: residentRoot(path.join(root, "mesh"), "session:mixed"),
    fullCodeMode: true, agents: { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 60_000, budgetUsd: 0 },
    mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, retention: DEFAULT_FABRIC_CONFIG.retention,
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fabricExtensionPath: path.resolve("dist/index.js"),
    piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
    piModels: { available: [{ provider: "fixture", id: "visible" }], aliases: {}, defaultModel: "fixture/visible" },
  };
  fs.mkdirSync(config.residencyRoot, { recursive: true });
  const configPath = path.join(config.residencyRoot, "config.json");
  fs.writeFileSync(configPath, JSON.stringify(config));
  return { root, config, configPath };
};
const stop = async (child: ChildProcess, output: () => string) => {
  if (child.exitCode !== null || child.signalCode !== null) return;
  const exit = once(child, "exit");
  const timer = setTimeout(() => child.kill("SIGKILL"), 10_000);
  try {
    if (child.connected) child.send("close"); else child.kill("SIGTERM");
    const [code, signal] = await exit;
    expect({ code, signal }, `Legacy host and its worker must close cleanly: ${output()}`).toEqual({ code: 0, signal: null });
  }
  finally { clearTimeout(timer); }
};
const operations = ["actorStatus", "setInstructions", "setModel", "setThinking", "setTools"] as const;

describe.skipIf(!fs.existsSync(legacyHost))("real B70 mixed-release resident compatibility", () => {
  it.each((["Main", "proxy"] as const).flatMap(clientKind => operations.map(operation => [clientKind, operation] as const)))("%s refuses %s before dispatch, preserving identity, registry and queued mailbox", async (clientKind, operation) => {
    const { root, config, configPath } = fixture();
    const identity = { id: config.rootId, name: "Main", kind: "main" as const, sessionId: config.sessionId };
    const mesh = new MeshStore(config.meshRoot, config.mesh.maxEventBytes, config.mesh.maxReadEvents);
    const participants = new ParticipantDirectory(mesh, { enabled: true, hostId: identity.id, rootId: identity.id, identity, heartbeatMs: 100, leaseMs: 1_000 });
    participants.registerSource(() => [{ format: 1, id: identity.id, kind: "root", rootId: identity.id, ownerHostId: identity.id, ownerIdentityId: identity.id,
      name: "Main", status: "idle", residency: "session", runner: "pi", transport: "host", capabilities: ["fabric"], cwd: config.cwd, sessionId: config.sessionId,
      startedAt: Date.now(), updatedAt: Date.now(), controlProtocol: "v1" }]);
    await participants.start();
    const main = new ResidencyClient({ config, mesh, participants, mainAgent: { id: identity.id, local: true } as FabricMainAgentTarget });
    const client = clientKind === "Main" ? main : new ResidentActorClient(config.meshRoot, config.rootId);
    const child = fork(path.resolve("tests/fixtures/legacy-resident-host.mjs"), [legacyHost, configPath], { stdio: ["ignore", "pipe", "pipe", "ipc"] });
    let output = "";
    child.stdout?.on("data", data => { output += String(data); }); child.stderr?.on("data", data => { output += String(data); });
    try {
      const actor = await new Promise<FabricActorInfo>((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error(`Legacy startup timeout: ${output}`)), 15_000);
        child.once("message", (message: { actor: FabricActorInfo }) => { clearTimeout(timer); resolve(message.actor); });
        child.once("exit", code => { clearTimeout(timer); reject(new Error(`Legacy host exited ${code}: ${output}`)); });
      });
      const ownerPath = path.join(config.residencyRoot, "owner.json");
      const owner = fs.readFileSync(ownerPath, "utf8");
      expect(JSON.parse(owner).pid).toBe(child.pid);
      expect(JSON.parse(owner).commands).toBeUndefined();
      const registryPath = path.join(config.actorRoot, "actors.json");
      const actorDir = path.join(config.actorRoot, actor.id);
      const queueFiles = fs.readdirSync(actorDir).filter(file => file.startsWith("queue-"));
      expect(queueFiles.length).toBeGreaterThan(0);
      const queues = () => queueFiles.map(file => fs.readFileSync(path.join(actorDir, file), "utf8"));
      const before = { registry: fs.readFileSync(registryPath, "utf8"), queues: queues() };
      expect(before.queues.join()).toContain("queued-mailbox-one");
      expect(before.queues.join()).toContain("queued-mailbox-two");
      const requests = path.join(config.residencyRoot, "requests");
      const requestTime = fs.statSync(requests).mtimeMs;
      {
        const caller = { identity, hostId: identity.id };
        const result = await (operation === "actorStatus" ? client.actorStatus(actor.id) : client.setActor(
          operation === "setInstructions" ? { operation, id: actor.id, instructions: "Changed" } :
          operation === "setModel" ? { operation, id: actor.id, model: "fixture/visible", scope: "project" } :
          operation === "setThinking" ? { operation, id: actor.id, thinking: "high", scope: "project" } :
          { operation, id: actor.id, tools: ["read"] }, undefined, caller)).catch(error => error);
        // Assert preservation even on the destructive base, before checking the refusal.
        await delay(150);
        expect(fs.readFileSync(ownerPath, "utf8"), operation).toBe(owner);
        expect(fs.readFileSync(registryPath, "utf8"), operation).toBe(before.registry);
        expect(queues(), operation).toEqual(before.queues);
        expect(fs.statSync(requests).mtimeMs, operation).toBe(requestTime);
        expect(result, operation).toMatchObject({ name: "ResidentCommandUnsupportedError", code: "RESIDENT_COMMAND_UNSUPPORTED" });
        expect(result.message, operation).toMatch(/older release.*next idle point.*retry/i);
      }
    } finally {
      await main.close(); await stop(child, () => output); await participants.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  }, 30_000);
});

describe("strict resident command parser", () => {
  it.each(["futureSetter", "__proto__", "", null])("refuses unknown operation %s before actor lookup or removal", async operation => {
    const { root, config } = fixture();
    const host = new ResidentHost(config);
    try {
      await host.start();
      const lookup = vi.spyOn(host.actors, "status");
      const remove = vi.spyOn(host.actors, "remove");
      const requestId = "unknown-operation";
      fs.writeFileSync(path.join(config.residencyRoot, "requests", `${requestId}.json`), JSON.stringify({ format: RESIDENT_HOST_FORMAT, rootId: config.rootId, requestId, operation, id: "actor-do-not-remove", createdAt: Date.now() }));
      const response = path.join(config.residencyRoot, "responses", `${requestId}.json`);
      const deadline = Date.now() + 5_000;
      while (!fs.existsSync(response)) { if (Date.now() > deadline) throw new Error("No unknown-command response"); await delay(20); }
      expect(JSON.parse(fs.readFileSync(response, "utf8"))).toMatchObject({ ok: false, errorCode: "RESIDENT_COMMAND_UNSUPPORTED", error: expect.stringContaining("Unsupported Fabric residency command") });
      expect(lookup).not.toHaveBeenCalled(); expect(remove).not.toHaveBeenCalled();
    } finally { await host.close(); fs.rmSync(root, { recursive: true, force: true }); }
  });
});
