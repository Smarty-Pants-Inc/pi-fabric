import { fork, type ChildProcess } from "node:child_process";
import { once } from "node:events";
import { beforeEach } from "vitest";
import { installInProcessResidentFence } from "./helpers/in-process-resident-fence.js";
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
import { RESIDENT_COMMANDS, RESIDENT_HOST_FORMAT, residentHostId, residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import { launchLog, stopAllOwned } from "./helpers/owned-processes.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";

// CI can supply a pinned prior release; local rollout probes use installed B70.
const legacyRelease = process.env.PI_FABRIC_LEGACY_RELEASE ?? path.join(os.homedir(), ".local/share/smarty-dev/fabric/releases/b243bc926beec5da8717135893dd6b578af73f7b");
const legacyHost = path.join(legacyRelease, "dist/residency/host.js");
beforeEach(() => installInProcessResidentFence());

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
        // A legacy binary cannot be taught launcher custody by desired config.
        // Refusal must not promise an automatic next-idle-point upgrade.
        expect(result.message, operation).toMatch(/older release.*handover-capable host and launcher.*retry after activation/i);
      }
    } finally {
      await main.close(); await stop(child, () => output); await participants.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  }, 30_000);
});

describe("rollback-safe resident actor command envelopes", () => {
  it.each((["Main", "proxy"] as const).flatMap(clientKind =>
    [...operations, "actors", "setActivationFilter"].map(operation => [clientKind, operation] as const)))
  ("%s persists %s with a format B70 rejects", async (clientKind, operation) => {
    const { root, config } = fixture();
    const identity = { id: config.rootId, name: "Main", kind: "main" as const, sessionId: config.sessionId };
    const mesh = new MeshStore(config.meshRoot, config.mesh.maxEventBytes, config.mesh.maxReadEvents);
    const participants = new ParticipantDirectory(mesh, { enabled: true, hostId: identity.id, rootId: identity.id, identity, heartbeatMs: 100, leaseMs: 1_000 });
    participants.registerSource(() => [{ format: 1, id: identity.id, kind: "root", rootId: identity.id, ownerHostId: identity.id, ownerIdentityId: identity.id,
      name: "Main", status: "idle", residency: "session", runner: "pi", transport: "host", capabilities: ["fabric"], cwd: config.cwd, sessionId: config.sessionId,
      startedAt: Date.now(), updatedAt: Date.now(), controlProtocol: "v1" }]);
    await participants.start();
    const main = new ResidencyClient({ config, mesh, participants, mainAgent: { id: identity.id, local: true } as FabricMainAgentTarget });
    const client = clientKind === "Main" ? main : new ResidentActorClient(config.meshRoot, config.rootId);
    const abort = new AbortController();
    let result: Promise<unknown> | undefined;
    try {
      // Publish a live owner that can write but deliberately never consumes a
      // request. This isolates each real writer's wire format from host parsing.
      fs.mkdirSync(path.join(config.residencyRoot, "requests"), { recursive: true });
      fs.writeFileSync(path.join(config.residencyRoot, "owner.json"), JSON.stringify({
        format: 1, rootId: config.rootId, hostId: residentHostId(config.rootId), pid: process.pid,
        startedAt: Date.now(), readyAt: Date.now(), commands: RESIDENT_COMMANDS, requestFence: 1,
      }));
      const id = "actor-wire-format";
      const caller = { identity, hostId: identity.id };
      result = (operation === "actorStatus" ? client.actorStatus(id, abort.signal) :
        operation === "actors" ? client.actors(abort.signal) : client.setActor(
          operation === "setInstructions" ? { operation, id, instructions: "Changed" } :
          operation === "setModel" ? { operation, id, model: "fixture/visible", scope: "project" } :
          operation === "setThinking" ? { operation, id, thinking: "high", scope: "project" } :
          operation === "setActivationFilter" ? { operation, id, activationFilter: null } :
          { operation: "setTools", id, tools: ["read"] }, abort.signal, caller)).catch(error => error);
      const requests = path.join(config.residencyRoot, "requests");
      expect(fs.readdirSync(requests)).toHaveLength(1);
      const envelope = JSON.parse(fs.readFileSync(path.join(requests, fs.readdirSync(requests)[0]!), "utf8"));
      expect(envelope).toMatchObject({ format: 2, operation, rootId: config.rootId });
      expect(config.format).toBe(1);
      expect(JSON.parse(fs.readFileSync(path.join(config.residencyRoot, "owner.json"), "utf8")).format).toBe(1);
      abort.abort();
      expect(await result).toMatchObject({ message: expect.stringContaining("aborted") });
      expect(fs.readdirSync(requests)).toEqual([]);
    } finally {
      abort.abort();
      await result;
      await main.close(); await participants.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
});

describe.skipIf(!fs.existsSync(legacyHost) || !fs.existsSync(path.resolve("dist/residency/host.js")))("real B70 rollback recovery of unclaimed current-release commands", () => {
  it("restores an untouched current actor before previous-release read/write without losing settings", async () => {
    const { root, config, configPath } = fixture();
    let legacy: ChildProcess | undefined;
    let output = "";
    const current = fork(path.resolve("tests/fixtures/untouched-rollback-host.mjs"), [path.resolve("dist/residency/host.js"), configPath], { stdio: ["ignore", "pipe", "pipe", "ipc"] });
    current.stdout?.on("data", data => { output += String(data); });
    current.stderr?.on("data", data => { output += String(data); });
    try {
      const exited = await Promise.race([once(current, "exit"), delay(15_000).then(() => { throw new Error("Current reload timed out: " + output); })]);
      expect(exited, output).toEqual([0, null]);
      const registryPath = path.join(config.actorRoot, "actors.json");
      const before = JSON.parse(fs.readFileSync(registryPath, "utf8"));
      expect(before.actors).toHaveLength(1);
      expect(before.actors[0]).not.toHaveProperty("filterSkipped");
      expect(before.actors[0]).not.toHaveProperty("activationFilterExpiresAt");
      legacy = fork(path.resolve("tests/fixtures/legacy-resident-host.mjs"), [legacyHost, configPath, "recover"], { stdio: ["ignore", "pipe", "pipe", "ipc"] });
      legacy.stdout?.on("data", data => { output += String(data); });
      legacy.stderr?.on("data", data => { output += String(data); });
      const [recovered] = await Promise.race([once(legacy, "message"), delay(15_000).then(() => { throw new Error("Legacy reload timed out: " + output); })]);
      expect(recovered.actor).toMatchObject({ id: before.actors[0].id, name: "untouched-rollback" });
      await stop(legacy, () => output);
      const after = JSON.parse(fs.readFileSync(registryPath, "utf8"));
      const settings = ({ status: _status, updatedAt: _updatedAt, lastRunId: _lastRunId, messages: _messages, runnerSessionId: _runnerSessionId, ...entry }: Record<string, unknown>) => entry;
      expect(after.actors.map(settings)).toEqual(before.actors.map(settings));
    } finally {
      if (current.exitCode === null && current.signalCode === null) { current.kill("SIGKILL"); await once(current, "exit"); }
      if (legacy) await stop(legacy, () => output);
      fs.rmSync(root, { recursive: true, force: true });
    }
  }, 40_000);
  it.each([...operations, "actors", "setActivationFilter"] as const)("rejects persisted %s after SIGKILL without removing identity, registry or queued mailbox", async operation => {
    const { root, config, configPath } = fixture();
    const launches = launchLog(root);
    let output = "";
    let legacy: ChildProcess | undefined;
    const current = fork(path.resolve("tests/fixtures/crash-resident-host.mjs"), [path.resolve("dist/residency/host.js"), configPath, operation], {
      env: { ...process.env, ...launches.env }, stdio: ["ignore", "pipe", "pipe", "ipc"],
    });
    current.stdout?.on("data", data => { output += String(data); }); current.stderr?.on("data", data => { output += String(data); });
    try {
      const exit = await Promise.race([once(current, "exit"), delay(15_000).then(() => { throw new Error(`Current host crash timeout: ${output}`); })]);
      expect(exit, output).toEqual([null, "SIGKILL"]);
      const requestDir = path.join(config.residencyRoot, "requests");
      const requestFiles = fs.readdirSync(requestDir);
      expect(requestFiles).toHaveLength(1);
      expect(fs.readdirSync(path.join(config.residencyRoot, "processing"))).toEqual([]);
      const envelope = JSON.parse(fs.readFileSync(path.join(requestDir, requestFiles[0]!), "utf8"));
      expect(envelope.operation).toBe(operation);
      const registryPath = path.join(config.actorRoot, "actors.json");
      const beforeRegistry = JSON.parse(fs.readFileSync(registryPath, "utf8"));
      const actor = beforeRegistry.actors[0];
      const actorDir = path.join(config.actorRoot, actor.id);
      const headFile = path.join(actorDir, "registry", "messages-head.json");
      const historyHead = () => fs.existsSync(headFile) ? fs.readFileSync(headFile, "utf8") : undefined;
      const beforeHistoryHead = historyHead();
      const queues = () => !fs.existsSync(actorDir) ? [] : fs.readdirSync(actorDir).filter(file => file.startsWith("queue-")).flatMap(file =>
        JSON.parse(fs.readFileSync(path.join(actorDir, file), "utf8")).items);
      const beforeItems = queues();
      const mailbox = beforeItems.filter((item: { payload: { message: string } }) => item.payload.message.startsWith("queued-mailbox-"));
      expect(mailbox.map((item: { payload: { message: string } }) => item.payload.message)).toEqual(["queued-mailbox-one", "queued-mailbox-two"]);
      // The host crashed, not its detached worker. Stop only recorded fixture
      // descendants before rollback; normal actor recovery must restore the queue.
      await stopAllOwned(launches.owned(), 2_000, 2_000);
      legacy = fork(path.resolve("tests/fixtures/legacy-resident-host.mjs"), [legacyHost, configPath, "recover"], {
        env: { ...process.env, ...launches.env }, stdio: ["ignore", "pipe", "pipe", "ipc"],
      });
      legacy.stdout?.on("data", data => { output += String(data); }); legacy.stderr?.on("data", data => { output += String(data); });
      const recovered = await Promise.race([
        once(legacy, "message").then(([message]) => message as { actor?: FabricActorInfo }),
        once(legacy, "exit").then(([code]) => { throw new Error(`B70 exited ${code}: ${output}`); }),
        delay(15_000).then(() => { throw new Error(`B70 recovery timeout: ${output}`); }),
      ]);
      const response = JSON.parse(fs.readFileSync(path.join(config.residencyRoot, "responses", `${envelope.requestId}.json`), "utf8"));
      expect.soft(response, `${operation}: ${output}`).toMatchObject({ ok: false, error: "Invalid Fabric residency request" });
      expect.soft(recovered.actor).toMatchObject({ id: actor.id, name: actor.name, rootId: actor.rootId, residency: "durable" });
      const registry = JSON.parse(fs.readFileSync(registryPath, "utf8"));
      expect.soft(registry.actors).toHaveLength(beforeRegistry.actors.length);
      // Recovery legitimately updates running status, timestamps and run history,
      // but must not rewrite identity or any of the actor's persistent settings.
      // B70 predates the external message journal: its rewrite drops the registry's
      // messageHistory reference, which the current release re-reads from the
      // untouched messages-head.json (a legacy rewrite never clears history).
      const settings = ({ status: _status, updatedAt: _updatedAt, lastRunId: _lastRunId, messages: _messages,
        messageHistory: _messageHistory, runnerSessionId: _runnerSessionId, ...entry }: Record<string, unknown>) => entry;
      expect.soft(registry.actors.map(settings)).toEqual(beforeRegistry.actors.map(settings));
      if (beforeRegistry.actors[0]?.messageHistory) expect.soft(beforeHistoryHead).toBe(JSON.stringify(beforeRegistry.actors[0].messageHistory));
      expect.soft(historyHead()).toBe(beforeHistoryHead);
      expect.soft(registry.actors[0]?.removal).toBeUndefined();
      const afterItems = queues();
      // B70 retries recovered deliveries and predates bindingVersion. Preserve
      // every queued message's identity/content/order, not retry bookkeeping.
      const queuedMessages = (items: typeof beforeItems) => items
        .filter((item: { payload: { message: string } }) => item.payload.message.startsWith("queued-mailbox-"))
        .map((item: { id: string; payload: unknown }) => ({ id: item.id, payload: item.payload }));
      expect.soft(queuedMessages(afterItems)).toEqual(queuedMessages(mailbox));
      expect(fs.readdirSync(requestDir)).toEqual([]);
      expect(fs.readdirSync(path.join(config.residencyRoot, "processing"))).toEqual([]);
    } finally {
      try { if (legacy) await stop(legacy, () => output); }
      finally {
        await stopAllOwned(launches.owned(), 2_000, 2_000);
        fs.rmSync(root, { recursive: true, force: true });
      }
    }
  }, 40_000);
});

describe("strict resident command parser", () => {
  it.each(["spawn", "futureSetter", "__proto__", "", null])("refuses unknown operation %s before actor lookup or removal", async operation => {
    const { root, config } = fixture();
    const host = new ResidentHost(config);
    try {
      await host.start();
      const lookup = vi.spyOn(host.actors, "status");
      const remove = vi.spyOn(host.actors, "remove");
      const spawn = vi.spyOn(host.agents, "spawn");
      const requestId = "unknown-operation";
      fs.writeFileSync(path.join(config.residencyRoot, "requests", `${requestId}.json`), JSON.stringify({ format: RESIDENT_HOST_FORMAT, rootId: config.rootId, requestId, operation, id: "actor-do-not-remove", createdAt: Date.now() }));
      const response = path.join(config.residencyRoot, "responses", `${requestId}.json`);
      const deadline = Date.now() + 5_000;
      while (!fs.existsSync(response)) { if (Date.now() > deadline) throw new Error("No unknown-command response"); await delay(20); }
      expect(JSON.parse(fs.readFileSync(response, "utf8"))).toMatchObject({ ok: false, errorCode: "RESIDENT_COMMAND_UNSUPPORTED", error: expect.stringContaining("Unsupported Fabric residency command") });
      expect(lookup).not.toHaveBeenCalled(); expect(remove).not.toHaveBeenCalled();
      expect(spawn).not.toHaveBeenCalled();
    } finally { await host.close(); fs.rmSync(root, { recursive: true, force: true }); }
  });
});
