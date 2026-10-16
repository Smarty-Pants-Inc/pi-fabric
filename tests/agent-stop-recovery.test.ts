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
  it.each(["caller abort", "host close"])("relaunches restored leading work after %s without operator stop intent", async cancellation => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "agent-lifecycle-recovery-"));
    roots.push(root);
    const actorId = "lifecycle-actor";
    const activationId = "lifecycle-activation";
    const task = JSON.stringify({ task: "restored leading work", actorId, id: activationId }, null, 2);
    const options = { workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: root };
    const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 30_000 }, options);
    managers.push(manager);
    const controller = new AbortController();
    let alive = true;
    let close!: () => void;
    const closed = new Promise<void>(resolve => { close = resolve; });
    let calls = 0;
    // This synthetic PID is known dead to the manager; only the mock transport
    // reports liveness, so draining it must never signal a real process.
    const launch = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
      const completed = ++calls === 2;
      const args = request.workerArguments;
      const argument = (name: string): string => args[args.indexOf(name) + 1]!;
      fs.writeFileSync(argument("--status-file"), JSON.stringify({
        id: argument("--id"), actorId, name: argument("--name"), task,
        status: completed ? "completed" : "running", runner: "pi", transport: "process",
        sessionId: "2147483000", cwd: argument("--cwd"), startedAt: Date.now(), updatedAt: Date.now(),
        turns: 0, toolCalls: 0, text: completed ? "restored worker answer" : "",
        usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
      }));
      return {
        kind: "process", sessionId: "2147483000", isAlive: async () => completed ? false : alive,
        closed: completed ? Promise.resolve() : closed,
        stop: async () => { alive = false; close(); },
      } as Awaited<ReturnType<ProcessTransport["launch"]>>;
    });
    let recovered: AgentManager | undefined;
    try {
      const handle = await manager.spawn({ task, actorId, activationId, transport: "process" }, controller.signal);
      expect(launch).toHaveBeenCalledTimes(1);
      if (cancellation === "caller abort") {
        controller.abort();
        await manager.wait(handle.id);
      } else {
        await manager.close();
      }
      recovered = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents }, options);
      managers.push(recovered);
      const result = await recovered.run({ task, actorId, activationId, transport: "process" });
      expect(result).toMatchObject({ status: "completed", text: "restored worker answer" });
      expect(result.id).not.toBe(handle.id);
      expect(launch).toHaveBeenCalledTimes(2);
    } finally {
      alive = false;
      close();
      try {
        await manager.close();
        await recovered?.close();
      } finally {
        launch.mockRestore();
      }
    }
  });

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
      const receipt = JSON.parse(fs.readFileSync(path.join(directory, "archive-pending.json"), "utf8"));
      expect(receipt).toMatchObject({ stopIntent: {
        reason: "explicit", recoveryRecord: {
          id: handle.id, status: "running", sessionId: String(oldPid), processStartTime: "1",
        },
      } });
      const recoveryRecord = receipt.stopIntent.recoveryRecord as Record<string, unknown>;
      expect(Object.hasOwn(recoveryRecord, "task")).toBe(false);
      expect(Object.hasOwn(recoveryRecord, "stopIntent")).toBe(false);
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

  it.each([false, true])("preserves a completed receipt without ENODEV and commits only after delivery (missing status: %s)", async missingStatus => {
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
      format: 1, awaitingResult: true, stopIntent: { reason: "explicit", requestedAt: 3,
        ...(missingStatus ? { recoveryRecord: JSON.parse(fs.readFileSync(path.join(directory, "status.json"), "utf8")) } : {}),
      },
    }));
    if (missingStatus) fs.rmSync(path.join(directory, "status.json"));
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

  it("fences a stop persisted after retry removed status, then recovers the replacement once", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "agent-stop-retry-gap-"));
    roots.push(root);
    const actorId = "retry-gap-actor";
    const activationId = "retry-gap-activation";
    const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 30_000 }, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: root,
    });
    managers.push(manager);
    let enteredLaunch!: () => void;
    const entered = new Promise<void>(resolve => { enteredLaunch = resolve; });
    let releaseLaunch!: () => void;
    const launchGate = new Promise<void>(resolve => { releaseLaunch = resolve; });
    let enteredStop!: () => void;
    const stoppingReplacement = new Promise<void>(resolve => { enteredStop = resolve; });
    let releaseStop!: () => void;
    const stopGate = new Promise<void>(resolve => { releaseStop = resolve; });
    let gone = false;
    const replacementPid = process.ppid;
    const originalIdentity = processIdentity.processStartTime;
    const identity = vi.spyOn(processIdentity, "processStartTime").mockImplementation(pid =>
      pid === replacementPid ? (gone ? "2" : "1") : originalIdentity(pid));
    let calls = 0;
    const launch = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
      calls++;
      const args = request.workerArguments;
      const argument = (name: string): string => args[args.indexOf(name) + 1]!;
      if (calls === 1) {
        fs.writeFileSync(argument("--status-file"), JSON.stringify({
          id: argument("--id"), actorId, name: argument("--name"), task: "retry gap", status: "failed",
          runner: "pi", transport: "process", cwd: argument("--cwd"), startedAt: Date.now(), updatedAt: Date.now(),
          sessionId: String(replacementPid), error: "Agent transport exited without a result", turns: 0, toolCalls: 0,
          text: "", usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
        }));
        // Main forbids retry once host identity proves an actor worker gone. A live
        // identity plus a transport failure is the counterexample that may still retry.
        // All transport stops below are mocked: this parent PID is never signaled.
        return { kind: "process", sessionId: String(replacementPid), isAlive: async () => false,
          closed: Promise.resolve(), stop: async () => {} } as Awaited<ReturnType<ProcessTransport["launch"]>>;
      }
      enteredLaunch();
      await launchGate;
      let close!: () => void;
      const closed = new Promise<void>(resolve => { close = resolve; });
      return { kind: "process", sessionId: String(replacementPid), isAlive: async () => !gone, closed,
        stop: async () => { enteredStop(); await stopGate; gone = true; close(); },
      } as Awaited<ReturnType<ProcessTransport["launch"]>>;
    });
    try {
      const handle = await manager.spawn({
        task: JSON.stringify({ actorId, id: activationId }, null, 2), actorId, activationId, transport: "process",
      });
      await entered;
      const directory = path.join(root, handle.id);
      const statusFile = path.join(directory, "status.json");
      expect(fs.existsSync(statusFile)).toBe(false);
      const stopping = manager.stop(handle.id);
      const receipt = JSON.parse(fs.readFileSync(path.join(directory, "archive-pending.json"), "utf8"));
      expect(receipt).toMatchObject({ stopIntent: { reason: "explicit" } });
      const recoveryRecord = receipt.stopIntent.recoveryRecord as Record<string, unknown>;
      expect(recoveryRecord).toMatchObject({ id: handle.id, status: "running" });
      expect(Object.hasOwn(recoveryRecord, "sessionId")).toBe(false);
      expect(Object.hasOwn(recoveryRecord, "processStartTime")).toBe(false);
      expect(Object.hasOwn(recoveryRecord, "task")).toBe(false);
      expect(Object.hasOwn(recoveryRecord, "stopIntent")).toBe(false);
      const recovered = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents }, {
        workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: root,
      });
      managers.push(recovered);
      let returned = false;
      const recovery = recovered.run({ task: "same activation", actorId, activationId, transport: "process" })
        .then(result => { returned = true; return result; });
      await new Promise(resolve => setTimeout(resolve, 30));
      expect(returned).toBe(false);
      expect(launch).toHaveBeenCalledTimes(2);
      releaseLaunch();
      await stoppingReplacement;
      await new Promise(resolve => setTimeout(resolve, 30));
      expect(returned).toBe(false); // Even resolved launch is live, and status is still missing.
      expect(fs.existsSync(statusFile)).toBe(false);
      // Prove the replacement's exact birth gone and wake the existing directory watch.
      // The old manager remains held: recovery must not depend on it publishing status.
      gone = true;
      fs.writeFileSync(path.join(directory, "identity-observed"), "gone");
      const result = await recovery;
      expect(result).toMatchObject({ id: handle.id, status: "stopped" });
      expect(launch).toHaveBeenCalledTimes(2);
      const second = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents }, {
        workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: root,
      });
      managers.push(second);
      expect(await second.run({ task: "same activation again", actorId, activationId, transport: "process" }))
        .toMatchObject({ id: handle.id, status: "stopped", finishedAt: result.finishedAt });
      expect(launch).toHaveBeenCalledTimes(2);
      releaseStop();
      expect(await stopping).toMatchObject({ status: "stopped", finishedAt: result.finishedAt });
      expect(JSON.parse(fs.readFileSync(statusFile, "utf8"))).toMatchObject({
        id: handle.id, status: "stopped", stopIntent: { reason: "explicit" }, sessionId: String(replacementPid),
      });
      // The original owner has now discharged archive intent. A fresh owner must
      // retain the replacement's exact identity from the terminal status snapshot.
      const third = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents }, {
        workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: root,
      });
      managers.push(third);
      expect(await third.run({ task: "same activation after stop", actorId, activationId, transport: "process" }))
        .toMatchObject({ id: handle.id, status: "stopped", finishedAt: result.finishedAt });
      expect(launch).toHaveBeenCalledTimes(2);
    } finally {
      releaseLaunch();
      releaseStop();
      launch.mockRestore();
      identity.mockRestore();
    }
  }, 15_000);

  it("keeps legacy missing-status stop custody without an identity snapshot", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "agent-stop-no-identity-"));
    roots.push(root);
    const directory = path.join(root, "c1b2c3d4e5f60718293a4b5c6d7e8f90");
    const actorId = "unknown-stop-actor";
    const activationId = "unknown-stop-activation";
    fs.mkdirSync(directory);
    fs.writeFileSync(path.join(directory, "task.txt"), JSON.stringify({ actorId, id: activationId }, null, 2));
    fs.writeFileSync(path.join(directory, "archive-pending.json"), JSON.stringify({
      format: 1, awaitingResult: true, stopIntent: { reason: "explicit", requestedAt: 3 },
    }));
    const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents }, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: root,
    });
    managers.push(manager);
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    try {
      let returned = false;
      void manager.run({ task: "same activation", actorId, activationId, transport: "process" }).then(() => { returned = true; });
      await new Promise(resolve => setTimeout(resolve, 30));
      expect(returned).toBe(false);
      expect(launch).not.toHaveBeenCalled();
      expect(fs.existsSync(path.join(directory, "status.json"))).toBe(false);
      await manager.close(); // Abandons observation, never manufactures worker-exit proof.
      expect(returned).toBe(false);
    } finally { launch.mockRestore(); }
  });

  it("does not fence a missing-status retry directory without stop intent", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "agent-no-stop-gap-"));
    roots.push(root);
    const oldId = "d1b2c3d4e5f60718293a4b5c6d7e8f90";
    const directory = path.join(root, oldId);
    const actorId = "no-stop-actor";
    const activationId = "no-stop-activation";
    fs.mkdirSync(directory);
    fs.writeFileSync(path.join(directory, "task.txt"), JSON.stringify({ actorId, id: activationId }, null, 2));
    fs.writeFileSync(path.join(directory, "archive-pending.json"), JSON.stringify({ format: 1, awaitingResult: true }));
    const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents }, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: root,
    });
    managers.push(manager);
    const launch = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
      const args = request.workerArguments;
      const argument = (name: string): string => args[args.indexOf(name) + 1]!;
      fs.writeFileSync(argument("--status-file"), JSON.stringify({
        id: argument("--id"), actorId, name: argument("--name"), task: "fresh launch", status: "completed",
        runner: "pi", transport: "process", cwd: argument("--cwd"), startedAt: Date.now(), updatedAt: Date.now(),
        text: "fresh answer", turns: 0, toolCalls: 0,
        usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
      }));
      return { kind: "process", sessionId: "2147483000", isAlive: async () => false,
        closed: Promise.resolve(), stop: async () => {} } as Awaited<ReturnType<ProcessTransport["launch"]>>;
    });
    try {
      const result = await manager.run({ task: "same activation", actorId, activationId, transport: "process" });
      expect(result).toMatchObject({ status: "completed", text: "fresh answer" });
      expect(result.id).not.toBe(oldId);
      expect(launch).toHaveBeenCalledTimes(1);
      expect(fs.existsSync(path.join(directory, "status.json"))).toBe(false);
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
