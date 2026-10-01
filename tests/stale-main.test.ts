import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import net from "node:net";
import { randomUUID } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { AgentManager } from "../src/agents/manager.js";
import { ActorManager } from "../src/actors/manager.js";
import { MeshStore } from "../src/mesh/store.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { HerdrTransport } from "../src/agents/transports/herdr-transport.js";
import * as processUtils from "../src/agents/transports/process-utils.js";

const roots: string[] = [];
const managers: AgentManager[] = [];
const actorManagers: ActorManager[] = [];
const herdrServers: net.Server[] = [];
afterEach(async () => {
  await Promise.all(actorManagers.splice(0).map(manager => manager.close()));
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  await Promise.all(herdrServers.splice(0).map(server => new Promise<void>(resolve => server.close(() => resolve()))));
  roots.splice(0).forEach(root => fs.rmSync(root, { recursive: true, force: true }));
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
});

const setup = (loaded: string, active = "active", extra: ConstructorParameters<typeof AgentManager>[2] = {},
  transport = DEFAULT_FABRIC_CONFIG.agents.transport) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-stale-main-"));
  roots.push(root);
  const base = path.join(root, "fabric");
  const releases = path.join(base, "releases");
  const sequence = ["B65", "B66", "B68", "after-critical", "active", "next"];
  for (const [index, release] of sequence.entries()) {
    const directory = path.join(releases, release);
    fs.mkdirSync(directory, { recursive: true });
    fs.writeFileSync(path.join(directory, "package.json"), JSON.stringify({ name: "pi-fabric" }));
    fs.writeFileSync(path.join(base, `${release}.receipt.json`), JSON.stringify({
      commit: release, installedAt: new Date(Date.UTC(2026, 8, 30, index)).toISOString(),
    }));
  }
  fs.writeFileSync(path.join(base, "releases-safety.json"), JSON.stringify({
    releases: { B66: { safetyCritical: true }, B68: { safetyCritical: true } },
  }));
  const settingsPath = path.join(root, "settings.json");
  const activate = (name: string) => fs.writeFileSync(settingsPath, JSON.stringify({
    packages: [{ source: path.join(releases, name) }],
  }));
  activate(active);
  const publishStaleMain = vi.fn();
  const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent: 2, transport }, {
    runRoot: path.join(root, "runs"),
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
    fabricExtensionPath: path.join(releases, loaded, "dist/index.js"),
    fullCodeMode: false,
    mainAgentId: randomUUID(),
    releaseSettingsPath: settingsPath,
    publishStaleMain,
    ...extra,
  });
  managers.push(manager);
  return { manager, publishStaleMain, activate, base, releases, settingsPath };
};

const notice = "This Main runs after-critical; the fleet runs active; it self-reloads at its next safe settle";

// PR #190 R6: admission must survive waits inside the transport, not just its permit queue.
const herdrReleaseWait = async (boundary: "spawn slot" | "command preparation" | "connection establishment") => {
  const fixture = setup("active", "active", {}, "herdr");
  const { base, activate } = fixture;
  fs.writeFileSync(path.join(base, "releases-safety.json"), JSON.stringify({ releases: { next: { safetyCritical: true } } }));
  const socketPath = path.join(path.dirname(base), "herdr.sock");
  const requests: string[] = [];
  const createWorker = vi.fn();
  const server = net.createServer(socket => {
    let input = "";
    socket.setEncoding("utf8");
    socket.on("data", (chunk: string) => {
      input += chunk;
      if (!input.includes("\n")) return;
      const request = JSON.parse(input.slice(0, input.indexOf("\n"))) as { id: string; method: string };
      requests.push(request.method);
      if (request.method === "layout.apply") createWorker();
      const response = request.method === "layout.apply"
        ? { result: { type: "layout_apply", layout: { root: { pane_id: "worker-pane" } } } }
        : request.method === "pane.get"
          ? { error: { code: "pane_not_found", message: "worker exited" } }
          : { result: { type: "pong" } };
      socket.end(`${JSON.stringify({ id: request.id, ...response })}\n`);
    });
  });
  herdrServers.push(server);
  await new Promise<void>((resolve, reject) => { server.once("error", reject); server.listen(socketPath, resolve); });
  vi.stubEnv("HERDR_ENV", "1");
  vi.stubEnv("HERDR_SOCKET_PATH", socketPath);
  vi.stubEnv("HERDR_WORKSPACE_ID", "w1");
  const switchRelease = vi.fn(() => activate("next"));
  const spawnLedgerDir = path.join(path.dirname(base), "spawns");
  fs.mkdirSync(spawnLedgerDir, { mode: 0o700 });
  if (boundary === "spawn slot") fs.writeFileSync(path.join(spawnLedgerDir, "0-0"), "");
  let now = 0;
  let inLaunch = false;
  const originalLaunch = HerdrTransport.prototype.launch;
  vi.spyOn(HerdrTransport.prototype, "launch").mockImplementation(function (request) {
    inLaunch = true;
    const transport = new HerdrTransport(process.env, {
      spawnLedgerDir, spawnsPerMinute: 1, now: () => now, monotonicNow: () => now, random: () => 0,
      sleep: async ms => { await Promise.resolve(); switchRelease(); now += ms; },
    });
    return originalLaunch.call(transport, request).finally(() => { inLaunch = false; });
  });
  const originalArgs = processUtils.scriptSpawnArgs;
  vi.spyOn(processUtils, "scriptSpawnArgs").mockImplementation(async (...args) => {
    const command = await originalArgs(...args);
    if (inLaunch && boundary === "command preparation") switchRelease();
    return command;
  });
  const originalConnect = net.createConnection;
  vi.spyOn(net, "createConnection").mockImplementation((...args: Parameters<typeof net.createConnection>) => {
    const socket = originalConnect(...args);
    if (inLaunch && boundary === "connection establishment") socket.prependOnceListener("connect", switchRelease);
    return socket;
  });
  return { ...fixture, requests, createWorker, switchRelease };
};

describe.skipIf(process.platform === "win32")("stale Main Herdr dispatch admission", () => {
  it.each(["spawn slot", "command preparation", "connection establishment"] as const)(
    "refuses a public spawn when a critical release activates during %s", async boundary => {
      const { manager, requests, createWorker, switchRelease } = await herdrReleaseWait(boundary);
      const refusal = await manager.spawn({ task: "Do not dispatch an old-release worker", transport: "herdr" }).catch(error => error);
      expect(switchRelease).toHaveBeenCalledTimes(1);
      expect(requests.filter(method => method === "layout.apply")).toHaveLength(0);
      expect(createWorker).not.toHaveBeenCalled();
      expect(refusal).toMatchObject({ code: "FABRIC_STALE_MAIN", message: expect.stringMatching(/safetyCritical.*next/) });
      expect(manager.runningCount()).toBe(0);
      expect(manager.list()).toHaveLength(0);
    },
  );

  it.each(["spawn slot", "command preparation", "connection establishment"] as const)(
    "parks an actor event across reload when a critical release activates during %s", async boundary => {
      const { manager, base, releases, settingsPath, requests, createWorker, switchRelease } = await herdrReleaseWait(boundary);
      const identity = { id: "session:transport-release", name: "Main", kind: "main" as const, sessionId: "transport-release" };
      const mesh = new MeshStore(path.join(base, "mesh"), 64 * 1024, 100);
      const meshConfig = { ...DEFAULT_FABRIC_CONFIG.mesh, enabled: true, actorPollMs: 20 };
      const options = { actorRoot: path.join(base, "actors"), persistent: true, rootId: identity.id };
      const actors = new ActorManager(identity.sessionId, identity, mesh, meshConfig, manager, () => {}, options);
      actorManagers.push(actors);
      const actor = await actors.create({ name: "Transport release event", instructions: "Reply", responseMode: "text" });
      const oldRun = vi.spyOn(manager, "run");
      actors.tell(actor.id, "preserve-transport-event");
      await vi.waitFor(() => expect(actors.status(actor.id).lastError || createWorker.mock.calls.length).toBeTruthy());
      expect(switchRelease).toHaveBeenCalledTimes(1);
      expect(requests.filter(method => method === "layout.apply")).toHaveLength(0);
      expect(createWorker).not.toHaveBeenCalled();
      expect(actors.status(actor.id).lastError).toMatch(/safetyCritical.*next/);
      expect(manager.runningCount()).toBe(0);
      expect(manager.list()).toHaveLength(0);
      const queueFile = () => fs.readdirSync(path.join(options.actorRoot, actor.id)).find(name => name.startsWith("queue-"));
      expect(queueFile()).toBeDefined();
      const saved = JSON.parse(fs.readFileSync(path.join(options.actorRoot, actor.id, queueFile()!), "utf8"));
      expect(saved.items).toHaveLength(1);
      expect(saved.items[0]).toMatchObject({ admissionRefused: true, payload: { message: "preserve-transport-event" } });
      expect(actors.messages(actor.id).filter(message => message.direction === "out")).toHaveLength(0);
      await actors.close();
      const current = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents }, {
        runRoot: path.join(base, "reloaded-runs"), workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
        fabricExtensionPath: path.join(releases, "next", "dist/index.js"), releaseSettingsPath: settingsPath,
      });
      managers.push(current);
      const resumedRun = vi.spyOn(current, "run");
      const resumed = new ActorManager(identity.sessionId, identity, mesh, meshConfig, current, () => {}, options);
      actorManagers.push(resumed);
      await vi.waitFor(() => {
        expect(resumed.messages(actor.id).filter(message => message.direction === "out" && message.runId)).toHaveLength(1);
        expect(queueFile()).toBeUndefined();
        expect(resumed.status(actor.id).status).toBe("idle");
      });
      expect(oldRun).toHaveBeenCalledTimes(1);
      expect(resumedRun).toHaveBeenCalledTimes(1);
    },
  );
});

describe("stale Main task admission", () => {
  it.each(["spawn", "run"] as const)("refuses %s from B65 across B66 before any worker launch", async method => {
    const { manager, publishStaleMain } = setup("B65");
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    await expect(manager[method]({ task: "Do not launch", transport: "process" }))
      .rejects.toThrow(/safetyCritical.*B66.*B68.*\/fabric-release-reload/);
    expect(launch).not.toHaveBeenCalled();
    expect(publishStaleMain).toHaveBeenCalledTimes(1);
  });

  it("refuses actor task activations through the same boundary", async () => {
    const { manager } = setup("B65");
    await expect(manager.run({ task: "Actor activation", actorId: "actor-test", actorName: "Test", transport: "process" }))
      .rejects.toThrow(/safetyCritical/);
  });

  it.each(["B65", "after-critical"])("real actor ask on %s obeys admission and preserves its notice", async loaded => {
    const { manager, base } = setup(loaded);
    const actors = new ActorManager("stale-session", {
      id: "main:stale-session", name: "Main", kind: "main", sessionId: "stale-session",
    }, new MeshStore(path.join(base, "mesh"), 64 * 1024, 100), {
      ...DEFAULT_FABRIC_CONFIG.mesh, enabled: true, actorPollMs: 20,
    }, manager, () => {}, { actorRoot: path.join(base, "actors") });
    actorManagers.push(actors);
    const actor = await actors.create({ name: "Guarded actor", instructions: "Reply", responseMode: "text" });
    const result = actors.ask(actor.id, "Test activation");
    if (loaded === "B65") await expect(result).rejects.toThrow(/safetyCritical/);
    else expect(await result).toHaveProperty("notice", notice);
  });
  it("parks refused actor events and runs them once after a release reload", async () => {
    const { manager, base, releases, settingsPath } = setup("B65");
    const identity = { id: "session:retry", name: "Main", kind: "main" as const, sessionId: "retry" };
    const mesh = new MeshStore(path.join(base, "mesh"), 64 * 1024, 100);
    const meshConfig = { ...DEFAULT_FABRIC_CONFIG.mesh, enabled: true, actorPollMs: 20 };
    const options = { actorRoot: path.join(base, "actors"), persistent: true, rootId: identity.id };
    const actors = new ActorManager("retry", identity, mesh, meshConfig, manager, () => {}, options);
    actorManagers.push(actors);
    const actor = await actors.create({ name: "Preserved event", instructions: "Reply", responseMode: "text" });
    const run = vi.spyOn(manager, "run");
    actors.tell(actor.id, "preserve-this-event", { label: "retry" });
    await vi.waitFor(() => expect(actors.status(actor.id).lastError).toMatch(/safetyCritical/));
    const queueFile = () => fs.readdirSync(path.join(options.actorRoot, actor.id)).find(name => name.startsWith("queue-"));
    expect(queueFile()).toBeDefined();
    const saved = JSON.parse(fs.readFileSync(path.join(options.actorRoot, actor.id, queueFile()!), "utf8"));
    expect(saved.items).toHaveLength(1);
    expect(saved.items[0].payload).toMatchObject({ message: "preserve-this-event" });
    await new Promise(resolve => setTimeout(resolve, 100));
    expect(run).toHaveBeenCalledTimes(1); // no hot retry against a still-stale runtime
    await actors.close();
    // Multiple reloads that remain stale are NOT interrupted executions and cannot discard work.
    for (let index = 0; index < 4; index += 1) {
      const stillOld = new ActorManager("retry", identity, mesh, meshConfig, manager, () => {}, options);
      actorManagers.push(stillOld);
      await vi.waitFor(() => expect(stillOld.status(actor.id).lastError).toMatch(/safetyCritical/));
      await new Promise(resolve => setTimeout(resolve, 25));
      expect(queueFile()).toBeDefined();
      expect(JSON.parse(fs.readFileSync(path.join(options.actorRoot, actor.id, queueFile()!), "utf8")).items).toHaveLength(1);
      await stillOld.close();
    }
    const current = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents }, {
      runRoot: path.join(base, "new-runs"), workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
      fabricExtensionPath: path.join(releases, "active", "dist/index.js"), releaseSettingsPath: settingsPath,
    });
    managers.push(current);
    const resumedRun = vi.spyOn(current, "run");
    // Force the Windows interleaving: recording an out message is not settlement.
    let releasePublication!: () => void;
    const publication = new Promise<void>(resolve => { releasePublication = resolve; });
    const publish = mesh.publish.bind(mesh);
    vi.spyOn(mesh, "publish").mockImplementation(async event => {
      if (event.topic === "fabric.actor.output") await publication;
      return publish(event);
    });
    const resumed = new ActorManager("retry", identity, mesh, meshConfig, current, () => {}, options);
    actorManagers.push(resumed);
    await vi.waitFor(() => expect(resumed.messages(actor.id).some(message => message.direction === "out" && message.runId)).toBe(true));
    expect(queueFile()).toBeDefined(); // held until mesh publication + cleanup finish
    releasePublication();
    await vi.waitFor(() => {
      expect(queueFile()).toBeUndefined();
      expect(resumed.status(actor.id).status).toBe("idle");
    });
    expect(resumedRun).toHaveBeenCalledTimes(1);
    expect(resumed.messages(actor.id).filter(message => message.direction === "out" && message.runId)).toHaveLength(1);
  });

  it("adds a notice, not a refusal, after the last critical release; reports once per active", async () => {
    const { manager, publishStaleMain, activate } = setup("after-critical");
    const handle = await manager.spawn({ task: "Allowed task", transport: "process" });
    expect(handle).toHaveProperty("notice", notice);
    const result = await manager.wait(handle.id);
    expect(result).toHaveProperty("notice", notice);
    expect(manager.status(handle.id)).toHaveProperty("notice", notice);
    expect(await manager.run({ task: "Allowed again", transport: "process" })).toHaveProperty("notice", notice);
    expect(publishStaleMain).toHaveBeenCalledTimes(1);
    activate("next");
    expect(await manager.run({ task: "New activation", transport: "process" }))
      .toHaveProperty("notice", expect.stringContaining("fleet runs next"));
    expect(publishStaleMain).toHaveBeenCalledTimes(2);
  });

  it("rechecks a safety activation while awaited model preparation is in flight", async () => {
    let enter!: () => void;
    let resume!: () => void;
    const entered = new Promise<void>(resolve => { enter = resolve; });
    const gate = new Promise<void>(resolve => { resume = resolve; });
    const { manager, activate, base } = setup("after-critical", "active", {
      preparePiModel: async () => { enter(); await gate; },
    });
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    const spawning = manager.spawn({ task: "Queued at activation", transport: "process" });
    const refused = expect(spawning).rejects.toThrow(/safetyCritical.*next/);
    await entered;
    fs.writeFileSync(path.join(base, "releases-safety.json"), JSON.stringify({ releases: { next: { safetyCritical: true } } }));
    activate("next");
    resume();
    await refused;
    expect(launch).not.toHaveBeenCalled();
    expect(manager.runningCount()).toBe(0);
  });
  it.each([true, false])("rechecks safety and owner authority after a real permit queue (authorized=%s)", async authorized => {
    const preparePiModel = vi.fn(async () => undefined);
    const { manager, activate, base } = setup("after-critical", "active", { preparePiModel });
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    const blockers = await Promise.all([0, 1].map(index => manager.spawn({ task: "HANG", model: `test/blocker-${index}`, transport: "process" })));
    let currentOwner = true;
    const queued = await manager.spawn({ task: "Queued release intersection", model: "test/queued", transport: "process" }, undefined, () => currentOwner);
    expect(queued).toMatchObject({ status: "queued", notice });
    expect(manager.runDirectory(queued.id)).toBeUndefined();
    fs.writeFileSync(path.join(base, "releases-safety.json"), JSON.stringify({ releases: { next: { safetyCritical: true } } }));
    activate("next");
    currentOwner = authorized;
    await manager.stop(blockers[0]!.id);
    const result = await manager.wait(queued.id);
    expect(result).toMatchObject(authorized
      ? { status: "failed", error: expect.stringMatching(/safetyCritical.*next/) }
      : { status: "stopped", error: "Agent activation no longer authorized" });
    expect(manager.runDirectory(queued.id)).toBeUndefined();
    expect(preparePiModel).toHaveBeenCalledTimes(2);
    expect(launch).toHaveBeenCalledTimes(2);
  });

  it("updates a noncritical queued notice at launch without changing the loaded release", async () => {
    const { manager, activate, releases } = setup("after-critical");
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    const blockers = await Promise.all(["HANG", "HANG"].map(task => manager.spawn({ task, transport: "process" })));
    const caller = new AbortController();
    const queued = await manager.spawn({ task: "Queued notice", transport: "process", recursive: true }, caller.signal);
    expect(queued).toMatchObject({ status: "queued", notice });
    caller.abort();
    activate("next");
    await manager.stop(blockers[0]!.id);
    expect(await manager.wait(queued.id)).toMatchObject({ status: "completed", notice: expect.stringContaining("fleet runs next") });
    expect(launch).toHaveBeenCalledTimes(3);
    expect(launch.mock.calls[2]![0].workerArguments).toContain(path.join(releases, "after-critical", "dist/index.js"));
  });

  it("parks an actor event when its accepted queued receipt becomes safety-stale before launch", async () => {
    const { manager, base, activate } = setup("after-critical");
    const identity = { id: "session:queued-release", name: "Main", kind: "main" as const, sessionId: "queued-release" };
    const mesh = new MeshStore(path.join(base, "mesh"), 64 * 1024, 100);
    const actorRoot = path.join(base, "actors");
    const actors = new ActorManager(identity.sessionId, identity, mesh, {
      ...DEFAULT_FABRIC_CONFIG.mesh, enabled: true, actorPollMs: 20,
    }, manager, () => {}, { actorRoot, persistent: true, rootId: identity.id });
    actorManagers.push(actors);
    const blockers = await Promise.all(["HANG", "HANG"].map(task => manager.spawn({ task, transport: "process" })));
    const actor = await actors.create({ name: "Queued release event", instructions: "Reply", responseMode: "text" });
    actors.tell(actor.id, "preserve-queued-event");
    await vi.waitFor(() => expect(manager.list().some(run => run.actorId === actor.id && run.status === "queued")).toBe(true));
    fs.writeFileSync(path.join(base, "releases-safety.json"), JSON.stringify({ releases: { next: { safetyCritical: true } } }));
    activate("next");
    await manager.stop(blockers[0]!.id);
    await vi.waitFor(() => expect(actors.status(actor.id).lastError).toMatch(/safetyCritical/));
    const queue = fs.readdirSync(path.join(actorRoot, actor.id)).find(name => name.startsWith("queue-"));
    expect(queue).toBeDefined();
    const saved = JSON.parse(fs.readFileSync(path.join(actorRoot, actor.id, queue!), "utf8"));
    expect(saved.items).toHaveLength(1);
    expect(saved.items[0]).toMatchObject({ admissionRefused: true, payload: { message: "preserve-queued-event" } });
    expect(actors.messages(actor.id).filter(message => message.direction === "out")).toHaveLength(0);
  });

  it("loaded equals active: no notice or stale event", async () => {
    const { manager, publishStaleMain } = setup("active");
    const handle = await manager.spawn({ task: "Current task", transport: "process" });
    expect(handle).not.toHaveProperty("notice");
    expect(await manager.wait(handle.id)).not.toHaveProperty("notice");
    expect(publishStaleMain).not.toHaveBeenCalled();
  });

  it("the loaded release is exclusive (B68 itself is allowed after the last critical release)", async () => {
    const { manager } = setup("B68");
    expect(await manager.run({ task: "Already has B68", transport: "process" }))
      .toHaveProperty("notice", expect.stringContaining("Main runs B68"));
  });

  it("the active release is inclusive even if intermediate release trees were pruned", async () => {
    const { manager, releases } = setup("B65", "B66");
    fs.rmSync(path.join(releases, "B68"), { recursive: true });
    await expect(manager.spawn({ task: "Stop at B66", transport: "process" }))
      .rejects.toThrow(/safetyCritical.*B66/);
  });
});
