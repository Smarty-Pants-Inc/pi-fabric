import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import runnerSession from "../src/worker/session-id.js";
import { parseWorkerOptions } from "../src/worker/options.js";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { ActorManager } from "../src/actors/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import { ResidentHost } from "../src/residency/host.js";
import { RESIDENT_HOST_FORMAT, type ResidentHostConfig } from "../src/residency/protocol.js";
import type { FabricLifecyclePublishRequest } from "../src/lifecycle/types.js";

const first = "01900000-0000-7000-8000-000000000001";
const latest = "01900000-0000-7000-8000-000000000002";
const parent = "parent-main-session";
// Source worker, not a dist-dependent skip: these regressions fail on main
// even before a build, and cover the same spawn/monitor path as dist/worker.js.
const workerPath = path.resolve("src/worker.ts");
const piBinary = path.resolve("tests/fixtures/fake-pi-session-id.mjs");
const roots: string[] = [];
const close: (() => Promise<unknown>)[] = [];
const root = () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-native-session-"));
  roots.push(dir);
  return dir;
};
const until = async (predicate: () => boolean) => {
  const deadline = Date.now() + 10_000;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("native session observation timed out");
    await new Promise(resolve => setTimeout(resolve, 20));
  }
};
afterEach(async () => {
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  for (const fn of close.splice(0).reverse()) await fn();
  for (const dir of roots.splice(0)) {
    fs.rmSync(dir, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
  }
});
const config = { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 10_000, retainRuns: true, budgetUsd: 0 };
const setup = () => {
  const dir = root();
  const events: FabricLifecyclePublishRequest[] = [];
  const agents = new AgentManager(dir, config, {
    workerPath, piBinary, runRoot: path.join(dir, "runs"),
    mainAgentId: "session:" + parent, fabricSessionId: parent,
    onLifecycle: event => events.push(event),
  });
  close.push(() => agents.close());
  return { dir, agents, events };
};
const readRecord = (file: string) => JSON.parse(fs.readFileSync(file, "utf8"));

describe("native Pi session hook", () => {
  it("observes native state on startup, replacement, request, compaction and shutdown without duplicate IDs", () => {
    vi.stubEnv("PI_FABRIC_PARENT_RUN", "run");
    const handlers = new Map<string, (event: unknown, ctx: ExtensionContext) => void>();
    runnerSession({
      on: (name: string, handler: (event: unknown, ctx: ExtensionContext) => void) => handlers.set(name, handler),
    } as unknown as ExtensionAPI);
    const write = vi.spyOn(fs, "writeSync").mockReturnValue(1);
    let sessionId = first;
    const ctx = { mode: "rpc", sessionManager: { getSessionId: () => sessionId } } as unknown as ExtensionContext;
    for (const name of ["session_start", "agent_start", "before_provider_request", "session_compact", "agent_settled"]) {
      handlers.get(name)!({}, ctx);
    }
    sessionId = latest;
    handlers.get("session_start")!({ reason: "new" }, ctx);
    handlers.get("session_compact")!({}, ctx);
    sessionId = "replacement-after-startup";
    handlers.get("before_provider_request")!({}, ctx);
    sessionId = "shutdown-session";
    handlers.get("session_shutdown")!({}, ctx);
    expect(write.mock.calls.map(call => JSON.parse(String(call[1])))).toEqual(
      [first, latest, "replacement-after-startup", "shutdown-session"].map(id => ({
        type: "fabric_runner_session", runId: "run", sessionId: id,
      })),
    );
    expect(write.mock.calls.every(call => call[0] === 1)).toBe(true);
    handlers.get("session_start")!({}, { ...ctx, mode: "tui" } as ExtensionContext);
    expect(write).toHaveBeenCalledTimes(4);
  });

  it("does not register without a disposable worker run binding", () => {
    vi.stubEnv("PI_FABRIC_PARENT_RUN", "");
    const on = vi.fn();
    runnerSession({ on } as unknown as ExtensionAPI);
    expect(on).not.toHaveBeenCalled();
  });
});

describe("native Pi runner session attribution", () => {
  const seededRecord = (id: string) => ({
    id, name: "seed", task: "work", status: "running", runner: "pi", transport: "process", cwd: process.cwd(),
    startedAt: Date.now(), updatedAt: Date.now(), turns: 0, toolCalls: 0, text: "",
    runnerSessionId: first, runnerSessionIds: [first],
    usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
  });

  it.each([true, false])("reads only matching prior status identities (matching ID: %s)", async matching => {
    const { agents } = setup();
    const launch = ProcessTransport.prototype.launch;
    const earlier = "01900000-0000-7000-8000-000000000000";
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function (this: ProcessTransport, request) {
      const file = request.workerArguments[request.workerArguments.indexOf("--status-file") + 1]!;
      fs.writeFileSync(file, JSON.stringify({
        ...seededRecord(matching ? request.id : "unrelated"),
        runnerSessionId: earlier, runnerSessionIds: [earlier, earlier, null],
      }));
      return launch.call(this, request);
    });
    const result = await agents.run({ task: "work", transport: "process", extensions: false });
    expect(result).toMatchObject({ status: "completed", runnerSessionId: first });
    expect(result.runnerSessionIds).toEqual(matching ? [earlier, first] : [first]);
  }, 15_000);

  it.each(["startup-retry", "resume"])("carries every native identity through the actual manager %s boundary", async kind => {
    const dir = root();
    const recoveryPi = path.resolve("tests/fixtures/fake-pi-session-recovery.mjs");
    const manager = new AgentManager(dir, config, {
      workerPath, piBinary: recoveryPi, runRoot: path.join(dir, "recovery-runs"),
      mainAgentId: "session:" + parent, fabricSessionId: parent,
    });
    close.push(() => manager.close());
    const launch = ProcessTransport.prototype.launch;
    const attempts: { id: string; freshStatus: boolean }[] = [];
    const workerArguments: string[][] = [];
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function (this: ProcessTransport, request) {
      const file = request.workerArguments[request.workerArguments.indexOf("--status-file") + 1]!;
      attempts.push({ id: request.id, freshStatus: !fs.existsSync(file) });
      workerArguments.push([...request.workerArguments]);
      return launch.call(this, request);
    });
    const handle = await manager.spawn({ task: kind, name: "recover-native-sessions", transport: "process", extensions: false });
    if (kind === "resume") {
      await until(() => {
        const status = manager.status(handle.id);
        return status.runnerSessionId === latest && "turns" in status && status.turns > 0;
      });
      // An unexpected real worker stop, NOT manager.stop (which forbids recovery).
      process.kill(Number(handle.sessionId), "SIGTERM");
    }
    const result = await manager.wait(handle.id, { timeoutMs: 10_000 });
    const expectedIds = [first, latest, "01900000-0000-7000-8000-000000000003", "01900000-0000-7000-8000-000000000004"];
    expect(result.status).toBe("completed");
    expect(attempts).toEqual([{ id: handle.id, freshStatus: true }, { id: handle.id, freshStatus: true }]);
    expect(fs.readFileSync(path.join(dir, "native-session-attempts"), "utf8")).toBe("2");
    const runDirectory = manager.runDirectory(handle.id)!;
    const relaunches = fs.readFileSync(path.join(runDirectory, "relaunches.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line));
    expect(relaunches).toEqual([expect.objectContaining({ kind })]);
    expect(result).toMatchObject({ runnerSessionId: expectedIds[3], runnerSessionIds: expectedIds, fabricSessionId: parent });
    expect(manager.status(handle.id)).toMatchObject({ runnerSessionId: expectedIds[3], runnerSessionIds: expectedIds });
    expect(readRecord(path.join(runDirectory, "status.json"))).toMatchObject({ runnerSessionId: expectedIds[3], runnerSessionIds: expectedIds });
    const argv = ["node", workerPath, ...workerArguments[1]!];
    expect(parseWorkerOptions(argv)).toMatchObject({ runnerSessionIds: [first, latest] });
    expect(parseWorkerOptions(argv).runnerSessionId).toBeUndefined(); // history is not a resume target
    expect(parseWorkerOptions([...argv, "--runner-session-ids", JSON.stringify([first, first])]).runnerSessionIds).toEqual([first]);
    for (const malformed of ["{", "null", "{}", JSON.stringify([first, null]), JSON.stringify([""])]) {
      expect(() => parseWorkerOptions([...argv, "--runner-session-ids", malformed])).toThrow("Invalid worker runner session IDs");
    }
    if (kind === "resume") expect(result.turns).toBe(2);
  }, 20_000);

  it("preserves native identity, history and parent Main on a host-synthesized stop record", async () => {
    const { agents, events } = setup();
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
      const file = request.workerArguments[request.workerArguments.indexOf("--status-file") + 1]!;
      fs.writeFileSync(file, JSON.stringify(seededRecord(request.id)));
      let alive = true;
      return {
        kind: "process", sessionId: "transport-not-native", isAlive: async () => alive,
        stop: async () => { alive = false; fs.rmSync(file, { force: true }); },
      };
    });
    const handle = await agents.spawn({ task: "work", transport: "process", extensions: false });
    await until(() => agents.status(handle.id).runnerSessionId === first);
    const result = await agents.stop(handle.id);
    expect(result).toMatchObject({
      status: "stopped", runnerSessionId: first, runnerSessionIds: [first],
      mainAgentId: "session:" + parent, fabricSessionId: parent,
    });
    expect(readRecord(path.join(agents.runDirectory(handle.id)!, "status.json"))).toMatchObject(result);
    expect(events.find(event => event.event === "run.stopped")).toMatchObject({
      data: { runnerSessionId: first, fabricSessionId: parent },
    });
  }, 15_000);

  it("records task identity live and the latest identity after compaction in status, listing, and terminal events", async () => {
    const { dir, agents, events } = setup();
    const handle = await agents.spawn({
      task: "rotate native session", name: "metered-task", transport: "process", extensions: false,
    });
    await until(() => agents.status(handle.id).runnerSessionId === first);
    expect(agents.list().find(run => run.id === handle.id)).toMatchObject({ runnerSessionId: first });
    expect(agents.listForUi().find(run => run.id === handle.id)).toMatchObject({ runnerSessionId: first });
    const result = await agents.wait(handle.id, { timeoutMs: 10_000 });
    expect(result).toMatchObject({
      status: "completed", name: "metered-task", runnerSessionId: latest,
      runnerSessionIds: [first, latest], fabricSessionId: parent, mainAgentId: "session:" + parent,
    });
    expect(agents.status(handle.id).runnerSessionId).toBe(latest);
    expect(agents.listForUi().find(run => run.id === handle.id)?.runnerSessionId).toBe(latest);
    const runDirectory = path.join(dir, "runs", handle.id);
    expect(readRecord(path.join(runDirectory, "status.json"))).toMatchObject({
      runnerSessionId: latest, fabricSessionId: parent,
    });
    expect(events.find(event => event.event === "pi.agent_start")).toMatchObject({ data: { runnerSessionId: first } });
    expect(events.find(event => event.event === "run.completed")).toMatchObject({
      source: { rootId: "session:" + parent }, data: { runnerSessionId: latest, fabricSessionId: parent },
    });
    const frames = fs.readFileSync(path.join(runDirectory, "events.jsonl"), "utf8").trim().split("\n");
    const argv = JSON.parse(frames.find(line => line.includes("fake_session_argv"))!).argv as string[];
    expect(argv).not.toContain("--no-session");
    expect(argv[argv.indexOf("--session") + 1]).toBe(path.join(runDirectory, "session.jsonl"));
    expect(argv.some(arg => arg.endsWith(path.join("worker", "session-id.ts")))).toBe(true);
  }, 15_000);

  it("retains a durable actor run native identity alongside actor name and parent Main", async () => {
    const { dir, agents, events } = setup();
    const mesh = new MeshStore(path.join(dir, "mesh"), 64 * 1024, 100);
    const actors = new ActorManager(
      parent, { id: "session:" + parent, name: "Main", kind: "main", sessionId: parent }, mesh,
      { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {},
      { persistent: true, actorRoot: path.join(dir, "actors"), rootId: "session:" + parent },
    );
    close.push(() => actors.close());
    const actor = await actors.create({
      name: "metered-actor", instructions: "Reply.", runner: "pi", extensions: false,
      responseMode: "text", transport: "process",
    });
    const reply = await actors.ask(actor.id, "work", { timeoutMs: 10_000 });
    const file = path.join(dir, "actors", actor.id, "runs", reply.runId!, "status.json");
    await until(() => fs.existsSync(file));
    expect(readRecord(file)).toMatchObject({
      runnerSessionId: first, actorId: actor.id, actorName: "metered-actor",
      fabricSessionId: parent, mainAgentId: "session:" + parent,
    });
    expect(events.find(event => event.event === "run.completed")).toMatchObject({
      source: { id: actor.id, name: "metered-actor", rootId: "session:" + parent }, data: { runnerSessionId: first },
    });
    const frames = fs.readFileSync(path.join(path.dirname(file), "events.jsonl"), "utf8").trim().split("\n");
    expect(JSON.parse(frames.find(line => line.includes("fake_session_argv"))!).argv).toContain("--session");
  }, 15_000);

  it("records resident-host actor native identity in its retained run and lifecycle emission", async () => {
    const dir = root();
    const resident: ResidentHostConfig = {
      format: RESIDENT_HOST_FORMAT, rootId: "session:" + parent, sessionId: parent,
      cwd: dir, projectRoot: dir, meshRoot: path.join(dir, "mesh"), actorRoot: path.join(dir, "actors"),
      residencyRoot: path.join(dir, "resident"), fullCodeMode: false, agents: config,
      mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, retention: DEFAULT_FABRIC_CONFIG.retention,
      workerPath, piBinary, fabricExtensionPath: path.resolve("dist/index.js"), claudeBinary: "claude", vedaBinary: "veda",
      piModels: { available: [{ provider: "session-test", id: "offline" }], aliases: {}, defaultModel: "session-test/offline" },
    };
    const host = new ResidentHost(resident);
    close.push(() => host.close());
    await host.start();
    const lifecycle = vi.spyOn(host.lifecycle, "publish");
    const actor = await host.actors.create({
      name: "resident-metered-actor", instructions: "Reply.", residency: "durable", runner: "pi",
      extensions: false, responseMode: "text", transport: "process",
    });
    const reply = await host.actors.ask(actor.id, "resident work", { timeoutMs: 10_000 });
    const file = path.join(path.dirname(host.actors.status(actor.id).sessionFile!), "runs", reply.runId!, "status.json");
    await until(() => fs.existsSync(file));
    expect(readRecord(file)).toMatchObject({
      runnerSessionId: first, actorId: actor.id, actorName: "resident-metered-actor",
      fabricSessionId: parent, mainAgentId: resident.rootId,
    });
    // The broker elides events with no subscribers; inspect the host's emission
    // boundary, not an unobserved mesh stream.
    expect(lifecycle.mock.calls.map(call => call[0])).toContainEqual(expect.objectContaining({
      event: "run.completed", data: { runnerSessionId: first, fabricSessionId: parent },
    }));
  }, 15_000);
});
