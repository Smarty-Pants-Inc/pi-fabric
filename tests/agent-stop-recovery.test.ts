import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { commitRunArchive, readPendingRunArchives, stageRunArchive } from "../src/agents/archive-custody.js";
import * as processIdentity from "../src/residency/process-identity.js";

const roots: string[] = [];
const managers: AgentManager[] = [];

afterEach(async () => {
  for (const manager of managers.splice(0)) await manager.close();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("durable explicit-stop recovery", () => {
  it("replays a persistently stopped activation without launching it again", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "agent-stop-recovery-"));
    roots.push(root);
    const id = "a1b2c3d4e5f60718293a4b5c6d7e8f90";
    const activationId = "activation-after-stop";
    const actorId = "actor-after-stop";
    const directory = path.join(root, id);
    fs.mkdirSync(directory, { recursive: true });
    fs.writeFileSync(path.join(directory, "task.txt"), JSON.stringify({ actorId, id: activationId }, null, 2));
    fs.writeFileSync(path.join(directory, "status.json"), JSON.stringify({
      id, name: "stopped", task: "work", status: "running", runner: "pi", transport: "process",
      actorId, cwd: process.cwd(), startedAt: Date.now() - 10_000, updatedAt: Date.now(),
      sessionId: "2147483000", turns: 2, toolCalls: 1, text: "partial",
      usage: { input: 5, output: 3, cacheRead: 0, cacheWrite: 0, cost: 0 },
    }));
    // Same existing manager receipt used for ordinary result custody. This survives
    // a worker replacing status.json with its last running checkpoint.
    fs.writeFileSync(path.join(directory, "archive-pending.json"), JSON.stringify({
      format: 1, awaitingResult: true, ownerPid: process.pid,
      stopIntent: { reason: "explicit", requestedAt: Date.now() - 1_000 },
    }));

    const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents }, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: root,
    });
    managers.push(manager);
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    try {
      const result = await manager.run({
        idempotencyKey: "recovery-stop-test", task: "must not relaunch", actorId, activationId,
        transport: "process",
      });
      expect(result).toMatchObject({ id, status: "stopped" });
      expect(launch).not.toHaveBeenCalled();
      expect(JSON.parse(fs.readFileSync(path.join(directory, "status.json"), "utf8"))).toMatchObject({
        status: "stopped", stopIntent: { reason: "explicit" },
      });

      // A second fresh owner sees the durable terminal intent even without an
      // ENODEV marker, and still does not relaunch after a queue-ack crash.
      const second = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents }, {
        workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: root,
      });
      managers.push(second);
      expect(await second.run({ task: "must remain stopped", actorId, activationId, transport: "process" }))
        .toMatchObject({ id, status: "stopped" });
      expect(launch).not.toHaveBeenCalled();
    } finally {
      launch.mockRestore();
    }
  });

  it("publishes intent before a held real manager stop and recovers without ENODEV evidence", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "agent-stop-held-"));
    roots.push(root);
    const activationId = "held-stop-activation";
    const actorId = "held-stop-actor";
    const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 30_000 }, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: root,
    });
    managers.push(manager);
    let stopCalled!: () => void;
    const called = new Promise<void>(resolve => { stopCalled = resolve; });
    let releaseStop!: () => void;
    const stopGate = new Promise<void>(resolve => { releaseStop = resolve; });
    let alive = true;
    const oldPid = process.ppid;
    const processStartTime = processIdentity.processStartTime;
    // The live parent PID is never signaled. Its controlled birth changes only
    // after the held transport closes, proving the old identity is gone/reused.
    const identity = vi.spyOn(processIdentity, "processStartTime")
      .mockImplementation(pid => pid === oldPid ? (alive ? "1" : "2") : processStartTime(pid));
    let close!: () => void;
    const closed = new Promise<void>(resolve => { close = resolve; });
    const launch = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function (_request) {
      const args = _request.workerArguments;
      const argument = (name: string): string => args[args.indexOf(name) + 1]!;
      const id = argument("--id");
      const statusFile = argument("--status-file");
      fs.writeFileSync(statusFile, JSON.stringify({
        id, name: argument("--name"), task: "held stop", status: "running", runner: "pi", transport: "process",
        actorId, sessionId: String(oldPid), processStartTime: "1", cwd: argument("--cwd"), startedAt: Date.now(), updatedAt: Date.now(), turns: 0, toolCalls: 0,
        text: "", usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
      }));
      return {
        kind: "process", sessionId: String(oldPid), isAlive: async () => alive, closed,
        stop: async () => { stopCalled(); await stopGate; alive = false; close(); },
      } as Awaited<ReturnType<ProcessTransport["launch"]>>;
    });
    try {
      const handle = await manager.spawn({
        task: JSON.stringify({ task: "HANG", actorId, id: activationId }, null, 2), actorId,
        activationId, transport: "process",
      });
      const stopping = manager.stop(handle.id);
      await called;
      const directory = path.join(root, handle.id);
      expect(JSON.parse(fs.readFileSync(path.join(directory, "archive-pending.json"), "utf8")))
        .toMatchObject({ stopIntent: { reason: "explicit" } });
      expect(fs.existsSync(path.join(directory, "unresolved-worker.json"))).toBe(false);

      const recovered = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents }, {
        workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: root,
      });
      managers.push(recovered);
      let returned = false;
      const recovery = recovered.run({ task: "same activation", actorId, activationId, transport: "process" })
        .then(result => { returned = true; return result; });
      await new Promise(resolve => setTimeout(resolve, 30));
      expect(returned).toBe(false); // ActorManager cannot finish its in-flight activation while this join is pending.
      expect(JSON.parse(fs.readFileSync(path.join(directory, "status.json"), "utf8")).status).toBe("running");
      expect(launch).toHaveBeenCalledTimes(1);
      releaseStop();
      expect(await stopping).toMatchObject({ status: "stopped" });
      expect(await recovery).toMatchObject({ id: handle.id, status: "stopped" });
      expect(launch).toHaveBeenCalledTimes(1);
    } finally {
      releaseStop();
      launch.mockRestore();
      identity.mockRestore();
    }
  });

  it("preserves a completed receipt without ENODEV and commits only after delivery", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "agent-stop-completed-"));
    roots.push(root);
    const id = "b1b2c3d4e5f60718293a4b5c6d7e8f90";
    const actorId = "completed-actor";
    const activationId = "completed-activation";
    const directory = path.join(root, id);
    fs.mkdirSync(directory);
    fs.writeFileSync(path.join(directory, "task.txt"), JSON.stringify({ actorId, id: activationId }, null, 2));
    fs.writeFileSync(path.join(directory, "status.json"), JSON.stringify({
      id, name: "completed", task: "work", status: "running", runner: "pi", transport: "process",
      actorId, sessionId: "2147483000", cwd: process.cwd(), startedAt: 1, updatedAt: 2,
      text: "partial checkpoint", turns: 0, toolCalls: 0,
      usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
    }));
    fs.writeFileSync(path.join(directory, "archive-pending.json"), JSON.stringify({
      format: 1, awaitingResult: true, stopIntent: { reason: "explicit", requestedAt: 3 },
    }));
    const receipt = JSON.stringify({ version: 1, runId: id, outcome: "completed", text: "genuine completed answer" });
    fs.writeFileSync(path.join(directory, "settlement.json"), receipt);
    const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents }, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: root,
    });
    managers.push(manager);
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    try {
      expect(await manager.run({ task: "same activation", actorId, activationId, transport: "process" }))
        .toMatchObject({ id, status: "completed", text: "genuine completed answer", enodevRecovered: true });
      expect(launch).not.toHaveBeenCalled();
      expect(JSON.parse(fs.readFileSync(path.join(directory, "status.json"), "utf8")).status).toBe("running");
      expect(manager.noteEnodevDelivered(id)).toBe(true);
      expect(manager.commitEnodevRecovery(id)).toBe(true);
      expect(JSON.parse(fs.readFileSync(path.join(directory, "status.json"), "utf8")))
        .toMatchObject({ status: "completed", text: "genuine completed answer", enodevDelivered: true });
      expect(fs.readFileSync(path.join(directory, "settlement.json"), "utf8")).toBe(receipt);
    } finally { launch.mockRestore(); }
  });

  it("preserves stop intent and an independent archive sink through stage/commit", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "agent-stop-archive-"));
    roots.push(root);
    const directory = path.join(root, "0123456789abcdef0123456789abcdef");
    fs.mkdirSync(directory);
    const result = { id: path.basename(directory), status: "stopped", text: "", usage: {} } as never;
    stageRunArchive(directory, { format: 1, kind: "shutdown", result });
    const file = path.join(directory, "archive-pending.json");
    const custody = JSON.parse(fs.readFileSync(file, "utf8"));
    fs.writeFileSync(file, JSON.stringify({ ...custody, stopIntent: { reason: "explicit", requestedAt: Date.now() } }));
    stageRunArchive(directory, { format: 1, kind: "settlement", result });
    expect(JSON.parse(fs.readFileSync(file, "utf8"))).toMatchObject({
      stopIntent: { reason: "explicit" }, pending: { shutdown: {}, settlement: {} },
    });
    commitRunArchive(directory, "settlement");
    expect(readPendingRunArchives(directory)).toHaveLength(1);
    expect(JSON.parse(fs.readFileSync(file, "utf8"))).toMatchObject({ stopIntent: { reason: "explicit" } });
  });
});
