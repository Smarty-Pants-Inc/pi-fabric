import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { readRunStopIntent } from "../src/agents/archive-custody.js";
import type { AgentHandleInfo, AgentRunRequest, AgentRunResult } from "../src/agents/types.js";
import { ActorManager } from "../src/actors/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { ResidentHost } from "../src/residency/host.js";
import { RESIDENT_HOST_FORMAT, residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";

const identity: MeshIdentity = { id: "session:redrive-test", name: "main", kind: "main", sessionId: "redrive-test" };
const roots: string[] = [];
const closers: Array<() => Promise<void>> = [];

const waitFor = async (predicate: () => boolean, timeoutMs = 10_000): Promise<void> => {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error("Timed out waiting for unstarted actor run");
    await new Promise(resolve => setTimeout(resolve, 10));
  }
};

const fixture = (persistent = true, closeGraceMs = 0) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-unstarted-redrive-"));
  roots.push(root);
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const agents = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: true, sessionExport: false }, {
    runRoot: path.join(root, "runs"), workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
    identityId: identity.id, mainAgentId: identity.id,
  });
  closers.push(() => agents.close());
  const actors = new ActorManager("redrive-test", identity, mesh,
    { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20, actorQueueLimit: 16 }, agents, () => {}, {
      actorRoot: path.join(root, "actors"), persistent, closeGraceMs, meshRetentionSweepPath: false,
    });
  closers.push(() => actors.close());
  return { root, mesh, agents, actors };
};

const resultFor = (request: AgentRunRequest, id: string, status: AgentRunResult["status"], inferenceStarted: boolean): AgentRunResult => ({
  id, name: request.name ?? "test-actor", task: request.task, status,
  runner: request.runner ?? "pi", transport: request.transport ?? "process", cwd: request.cwd ?? process.cwd(),
  startedAt: Date.now(), updatedAt: Date.now(), finishedAt: Date.now(),
  turns: inferenceStarted ? 1 : 0, toolCalls: 0, inferenceStarted,
  text: status === "completed" ? "review complete" : "", usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
});

const handleFor = (request: AgentRunRequest, id: string): AgentHandleInfo => ({
  id, name: request.name ?? "test-actor", status: "running", runner: request.runner ?? "pi",
  transport: request.transport ?? "process", cwd: request.cwd ?? process.cwd(),
  ...(request.actorId ? { actorId: request.actorId } : {}), ...(request.actorName ? { actorName: request.actorName } : {}),
});

const actorRequest = (manager: ActorManager, name = "reviewer") => manager.create({
  name, instructions: "Review each delivery.", topics: ["review.requests"], coalesce: false,
});

const publishReplay = (mesh: MeshStore, replayId = "ci-complete:abc123@2") =>
  mesh.publish({ topic: "review.requests", from: identity, kind: "ci-complete", data: { replayId }, text: "Review this change." });

const alarms = (mesh: MeshStore) => mesh.read({ topic: "ops.owner", limit: 100 })
  .filter(event => event.kind === "actor.run.unstarted");

describe("actor runs that end before inference (#15195)", () => {
  it("re-drives the same replay once, then alarms and consumes it after a second unstarted end", async () => {
    const h = fixture();
    const actor = await actorRequest(h.actors);
    const requests: AgentRunRequest[] = [];
    vi.spyOn(h.agents, "run").mockImplementation(async (request, _signal, onSpawned) => {
      requests.push(request);
      const id = randomUUID().replace(/-/g, "");
      onSpawned?.(handleFor(request, id));
      return resultFor(request, id, "stopped", false);
    });
    const replay = await publishReplay(h.mesh);
    await waitFor(() => alarms(h.mesh).length === 1 && requests.length === 2);

    expect(requests).toHaveLength(2);
    expect(requests[0]!.activationId).toBe(requests[1]!.activationId);
    expect(requests.every(request => request.task.includes(replay.id))).toBe(true);
    expect(alarms(h.mesh)).toHaveLength(1);
    expect(alarms(h.mesh)[0]).toMatchObject({
      to: identity.id,
      data: {
        actorId: actor.id,
        replayId: "ci-complete:abc123@2",
        runIds: [expect.any(String), expect.any(String)],
      },
    });
    expect(h.actors.status(actor.id)).toMatchObject({ status: "idle", queued: 0 });
    await new Promise(resolve => setTimeout(resolve, 60));
    expect(requests).toHaveLength(2);
    expect(alarms(h.mesh)).toHaveLength(1);
  });

  it("does not re-drive an ordinary mesh item without an explicit replay or delivery identity", async () => {
    const h = fixture();
    const actor = await actorRequest(h.actors, "ordinary-reviewer");
    const requests: AgentRunRequest[] = [];
    vi.spyOn(h.agents, "run").mockImplementation(async (request, _signal, onSpawned) => {
      requests.push(request);
      const id = randomUUID().replace(/-/g, "");
      onSpawned?.(handleFor(request, id));
      return resultFor(request, id, "stopped", false);
    });
    await h.mesh.publish({ topic: "review.requests", from: identity, kind: "notification", text: "ordinary delivery" });
    await waitFor(() => requests.length === 1 && h.actors.status(actor.id).status === "idle" && h.actors.status(actor.id).queued === 0);
    await new Promise(resolve => setTimeout(resolve, 60));
    expect(requests).toHaveLength(1);
    expect(alarms(h.mesh)).toHaveLength(0);
  });

  it("does not re-drive a run that started inference", async () => {
    const h = fixture(false);
    const actor = await actorRequest(h.actors);
    const requests: AgentRunRequest[] = [];
    vi.spyOn(h.agents, "run").mockImplementation(async (request, _signal, onSpawned) => {
      requests.push(request);
      const id = randomUUID().replace(/-/g, "");
      onSpawned?.(handleFor(request, id));
      return resultFor(request, id, "completed", true);
    });
    await publishReplay(h.mesh);
    await waitFor(() => requests.length === 1 && h.actors.status(actor.id).status === "idle");
    expect(requests).toHaveLength(1);
    expect(alarms(h.mesh)).toHaveLength(0);
  });

  it("does not fence a persistent session run when close suspends it for reload", async () => {
    const h = fixture(true, 5_000);
    const actor = await actorRequest(h.actors, "reload-reviewer");
    await h.mesh.publish({ topic: "review.requests", from: identity, kind: "notification", text: "HANG during session reload" });
    await waitFor(() => h.actors.status(actor.id).inFlightRun !== undefined);
    const runId = h.actors.status(actor.id).inFlightRun!.id;
    await waitFor(() => {
      const run = h.agents.status(runId);
      return run.status === "running" && "turns" in run && run.turns === 0;
    });
    await h.actors.close();
    const result = await h.agents.wait(runId);
    expect(result.status).toBe("stopped");
    expect(result).not.toHaveProperty("stopIntent");
  }, 15_000);

  it("does not re-drive an activation being removed and records removal provenance in run status", async () => {
    const h = fixture();
    const actor = await actorRequest(h.actors, "removed-reviewer");
    const runs = vi.spyOn(h.agents, "run");
    await h.mesh.publish({ topic: "review.requests", from: identity, kind: "ci-complete",
      data: { replayId: "remove:review@1" }, text: "HANG until actor removal" });
    await waitFor(() => h.actors.status(actor.id).inFlightRun !== undefined);
    const runId = h.actors.status(actor.id).inFlightRun!.id;
    const removed = await h.actors.remove(actor.id);
    expect(removed.removed).toBe(true);
    expect(runs).toHaveBeenCalledTimes(1);
    const saved = JSON.parse(fs.readFileSync(path.join(h.agents.runDirectory(runId)!, "status.json"), "utf8"));
    expect(saved).toMatchObject({ status: "stopped", stopIntent: {
      reason: "explicit", by: { kind: identity.kind, id: identity.id }, path: "actors.remove",
    } });
    expect(alarms(h.mesh)).toHaveLength(0);
  });

  it("records explicit actors.stop provenance in terminal run status without re-driving", async () => {
    const h = fixture();
    const actor = await actorRequest(h.actors, "stopped-reviewer");
    await h.mesh.publish({ topic: "review.requests", from: identity, kind: "ci-complete",
      data: { replayId: "stop:review@1" }, text: "HANG until explicitly stopped" });
    await waitFor(() => h.actors.status(actor.id).inFlightRun !== undefined);
    const runId = h.actors.status(actor.id).inFlightRun!.id;
    const stopped = await h.actors.stop(actor.id, undefined, true);
    expect(stopped.status).toBe("stopped");
    const saved = JSON.parse(fs.readFileSync(path.join(h.agents.runDirectory(runId)!, "status.json"), "utf8"));
    expect(saved).toMatchObject({ status: "stopped", stopIntent: {
      reason: "explicit", by: { kind: identity.kind, id: identity.id }, path: "actors.stop",
    } });
    expect(alarms(h.mesh)).toHaveLength(0);
  });
});

describe("stop-intent provenance remains additive", () => {
  const managerFixture = () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "agent-stop-intent-"));
    roots.push(root);
    const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: true, sessionExport: false }, {
      runRoot: path.join(root, "runs"), workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
      identityId: identity.id, mainAgentId: identity.id,
    });
    closers.push(() => manager.close());
    return { root, manager };
  };

  it("records the direct agents.stop caller in terminal run status", async () => {
    const h = managerFixture();
    const handle = await h.manager.spawn({
      name: "direct-stop", task: "HANG", actorId: "a".repeat(32), actorName: "direct-stop", residency: "session",
      runner: "pi", transport: "process", extensions: false,
    });
    await waitFor(() => h.manager.status(handle.id)?.status === "running");
    const result = await h.manager.stop(handle.id);
    expect(result).toMatchObject({ status: "stopped", stopIntent: {
      reason: "explicit", by: { kind: "main", id: identity.id }, path: "agents.stop",
    } });
    const saved = JSON.parse(fs.readFileSync(path.join(h.manager.runDirectory(handle.id)!, "status.json"), "utf8"));
    expect(saved.stopIntent).toMatchObject({ by: { kind: "main", id: identity.id }, path: "agents.stop" });
  }, 15_000);

  it.each([
    ["session", "session-reset"],
    ["durable", "residency-teardown"],
  ] as const)("records %s teardown provenance in terminal run status", async (residency, expectedPath) => {
    const h = managerFixture();
    const handle = await h.manager.spawn({
      name: `${residency}-teardown`, task: "HANG", actorId: "b".repeat(32), actorName: `${residency}-teardown`, residency,
      runner: "pi", transport: "process", extensions: false,
    });
    await waitFor(() => h.manager.status(handle.id)?.status === "running");
    await h.manager.close();
    const saved = JSON.parse(fs.readFileSync(path.join(h.manager.runDirectory(handle.id)!, "status.json"), "utf8"));
    expect(saved).toMatchObject({ status: "stopped", stopIntent: {
      reason: "explicit", by: { kind: "main", id: identity.id }, path: expectedPath,
    } });
  }, 15_000);

  it("still parses legacy status with no stopIntent and old stopIntent fields", () => {
    const h = managerFixture();
    expect(readRunStopIntent(h.root, { id: "legacy", status: "stopped" })).toBeUndefined();
    expect(readRunStopIntent(h.root, { id: "legacy", stopIntent: { reason: "explicit", requestedAt: 42 } }))
      .toEqual({ reason: "explicit", requestedAt: 42 });
  });
});

const residentWorker = `#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
const args = new Map();
for (let i = 2; i < process.argv.length; i += 2) args.set(process.argv[i].slice(2), process.argv[i + 1]);
const statusFile = args.get("status-file");
const taskFile = args.get("task-file");
const task = fs.readFileSync(taskFile, "utf8");
const now = Date.now();
const base = { id: args.get("id"), name: args.get("name"), task, runner: "pi", transport: "process",
  cwd: args.get("cwd"), startedAt: now, updatedAt: now, turns: 0, toolCalls: 0,
  text: "", usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 } };
const write = record => {
  const temporary = statusFile + "." + process.pid + ".tmp";
  fs.writeFileSync(temporary, JSON.stringify(record));
  fs.renameSync(temporary, statusFile);
};
if (args.get("resident-startup-probe") === "true") {
  write({ ...base, status: "completed", text: "resident worker startup verified", finishedAt: Date.now() });
  process.exit(0);
}
const stateFile = path.join(process.env.FAKE_RESIDENT_STATE_DIR, "attempts");
const tasksFile = process.env.FAKE_RESIDENT_TASK_LOG;
fs.appendFileSync(tasksFile, JSON.stringify(task) + "\\n");
const attempt = fs.existsSync(stateFile) ? Number(fs.readFileSync(stateFile, "utf8")) + 1 : 1;
fs.writeFileSync(stateFile, String(attempt));
if (attempt === 1) {
  write({ ...base, status: "running", inferenceStarted: false, exitCode: null });
  const stop = () => { write({ ...base, status: "stopped", inferenceStarted: false, error: "Agent stopped", finishedAt: Date.now(), exitCode: 0 }); process.exit(0); };
  process.on("SIGTERM", stop);
  process.on("SIGINT", stop);
  setInterval(() => {}, 1000);
} else {
  write({ ...base, status: "completed", inferenceStarted: true, turns: 1, text: "Resident replay reviewed.", finishedAt: Date.now(), exitCode: 0 });
  process.exit(0);
}
`;

describe("resident replay uses the persisted unstarted retry", () => {
  it("runs the same replay in the next resident activation after teardown stops the first before inference", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "resident-unstarted-redrive-"));
    roots.push(root);
    const stateRoot = path.join(root, "provider-state");
    fs.mkdirSync(stateRoot, { recursive: true, mode: 0o700 });
    const workerPath = path.join(root, "fake-worker.mjs");
    const taskLog = path.join(root, "tasks.jsonl");
    fs.writeFileSync(workerPath, residentWorker, { mode: 0o700 });
    vi.stubEnv("FAKE_RESIDENT_STATE_DIR", stateRoot);
    vi.stubEnv("FAKE_RESIDENT_TASK_LOG", taskLog);
    const meshRoot = path.join(root, "mesh");
    const config: ResidentHostConfig = {
      format: RESIDENT_HOST_FORMAT, rootId: "session:resident-redrive", sessionId: "resident-redrive",
      cwd: root, projectRoot: root, meshRoot, actorRoot: path.join(root, "actors"),
      residencyRoot: residentRoot(meshRoot, "session:resident-redrive"), fullCodeMode: true,
      agents: { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: true, sessionExport: false, timeoutMs: 120_000 },
      mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, retention: DEFAULT_FABRIC_CONFIG.retention,
      workerPath, fabricExtensionPath: path.resolve("dist/index.js"), piBinary: process.execPath,
      claudeBinary: "claude", vedaBinary: "veda",
      piModels: { available: [{ provider: "fake", id: "fake-model" }], aliases: {}, defaultModel: "fake/fake-model" },
    };
    fs.mkdirSync(config.residencyRoot, { recursive: true });
    fs.writeFileSync(path.join(config.residencyRoot, "config.json"), JSON.stringify(config));
    const router = new MeshStore(meshRoot, 64 * 1024, 100);
    const start = async () => {
      const host = new ResidentHost(config);
      closers.push(() => host.close());
      await host.start();
      return host;
    };
    const host = await start();
    const actor = await host.actors.create({ name: "resident-reviewer", instructions: "Review each request.",
      runner: "pi", model: "fake/fake-model", transport: "process", extensions: false, tools: [],
      topics: ["review.requests"], coalesce: false, residency: "durable" });
    const replay = await router.publish({ topic: "review.requests", from: identity, kind: "ci-complete",
      data: { replayId: "resident-ci-complete:abc@3" }, text: "Review this replay." });
    await waitFor(() => Boolean(host.actors.status(actor.id).inFlightRun));
    await waitFor(() => fs.existsSync(taskLog));
    const firstRunId = host.actors.status(actor.id).inFlightRun!.id;
    const firstStatusFile = path.join(config.residencyRoot, "runs", firstRunId, "status.json");
    await waitFor(() => fs.existsSync(firstStatusFile));
    const firstStatus = JSON.parse(fs.readFileSync(firstStatusFile, "utf8")) as { inferenceStarted?: boolean };
    expect(firstStatus.inferenceStarted).not.toBe(true);

    await host.close();
    const stopped = JSON.parse(fs.readFileSync(firstStatusFile, "utf8")) as Record<string, unknown>;
    expect(stopped).toMatchObject({ status: "stopped", stopIntent: {
      reason: "explicit", requestedAt: expect.any(Number), by: { kind: expect.any(String), id: expect.any(String) }, path: "residency-teardown",
    } });

    const restarted = await start();
    await waitFor(() => {
      const status = restarted.actors.status(actor.id);
      return status.status === "idle" && status.queued === 0 && status.inFlightRun === undefined &&
        status.lastRunId !== undefined && status.lastRunId !== firstRunId;
    });
    const prompts = fs.readFileSync(taskLog, "utf8").trim().split("\n").map(line => JSON.parse(line) as string);
    expect(prompts).toHaveLength(2);
    expect(prompts[0]).toContain(replay.id);
    expect(prompts[1]).toContain(replay.id);
    expect(prompts[0]).toContain("resident-ci-complete:abc@3");
    expect(prompts[1]).toContain("resident-ci-complete:abc@3");
  }, 60_000);
});
