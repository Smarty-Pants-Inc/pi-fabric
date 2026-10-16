import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";

const managers: AgentManager[] = [];
const roots: string[] = [];

afterEach(async () => {
  for (const manager of managers.splice(0)) await manager.close();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

function makeManager(timeoutMs = 1_500, workerPath = "tests/fixtures/fake-worker.mjs"): AgentManager {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "agent-deadline-stop-"));
  roots.push(root);
  const manager = new AgentManager(process.cwd(), {
    ...DEFAULT_FABRIC_CONFIG.agents,
    timeoutMs,
  }, {
    workerPath: path.resolve(workerPath),
    runRoot: root,
  });
  managers.push(manager);
  return manager;
}

describe("owned deadline and stop retry boundaries", () => {
  it.each(["HANG_WITH_PROGRESS", "HANG"] as const)("does not resume a worker stopped at its own deadline (%s)", async (task) => {
    const manager = makeManager();
    const launch = ProcessTransport.prototype.launch;
    const transports: Array<{ stop(): Promise<void> }> = [];
    const statusAtStop: string[] = [];
    let runId = "";
    const spy = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function (this: ProcessTransport, request) {
      const transport = await launch.call(this, request);
      transports.push(transport);
      return {
        ...transport,
        stop: async () => {
          statusAtStop.push(JSON.parse(fs.readFileSync(path.join(manager.runDirectory(runId)!, "status.json"), "utf8")).status);
          await transport.stop();
        },
      };
    });
    try {
      const handle = await manager.spawn({ task, transport: "process" });
      runId = handle.id;
      await vi.waitFor(() => {
        const status = manager.status(handle.id) as { turns?: number };
        if (task === "HANG_WITH_PROGRESS") expect(status.turns).toBe(3);
        else expect(fs.existsSync(path.join(manager.runDirectory(handle.id)!, "status.json"))).toBe(true);
      }, { timeout: 5_000 });
      const result = await manager.wait(handle.id);
      expect(result.status).toBe("timed_out");
      expect(transports).toHaveLength(1);
      expect(statusAtStop.length).toBeGreaterThan(0);
      expect(statusAtStop.every(status => status === "running")).toBe(true);
      expect(fs.existsSync(path.join(manager.runDirectory(handle.id)!, "relaunches.jsonl"))).toBe(false);
    } finally {
      spy.mockRestore();
      for (const transport of transports) await transport.stop();
    }
  }, 15_000);

  it("uses the original deadline when the first transport handle arrives late", async () => {
    const timeoutMs = 2_500;
    const handleReturnDelayMs = timeoutMs + 1_000;
    const manager = makeManager(timeoutMs);
    const launchStartedAt: number[] = [];
    const statusAtStop: string[] = [];
    const liveHandles: Array<{ stop(): Promise<void> }> = [];
    const spy = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function (this: ProcessTransport, request) {
      const startedAt = Date.now();
      launchStartedAt.push(startedAt);
      const args = request.workerArguments;
      const argument = (name: string): string => args[args.indexOf(name) + 1]!;
      const statusFile = argument("--status-file");
      const id = argument("--id");
      const name = argument("--name");
      const writeStatus = (status: "running" | "completed") => fs.writeFileSync(statusFile, JSON.stringify({
        id, name, task: "HANG_WITH_PROGRESS", status, runner: "pi", transport: "process",
        cwd: argument("--cwd"), startedAt, updatedAt: Date.now(), turns: 4, toolCalls: 2,
        text: status === "completed" ? "late retry completed" : "", exitCode: null,
        usage: { input: 40, output: 20, cacheRead: 0, cacheWrite: 0, cost: 0.002 },
        ...(status === "completed" ? { finishedAt: Date.now() } : {}),
      }));
      writeStatus(launchStartedAt.length === 1 ? "running" : "completed");
      let alive = true;
      let close!: () => void;
      const closed = new Promise<void>(resolve => { close = resolve; });
      const stop = async () => {
        statusAtStop.push(JSON.parse(fs.readFileSync(statusFile, "utf8")).status);
        alive = false;
        close();
      };
      liveHandles.push({ stop });
      if (launchStartedAt.length === 1) {
        // A deterministic transport clock: the first worker exits at its admission
        // cap, while launch withholds the handle until after that original cap.
        setTimeout(() => { alive = false; close(); }, timeoutMs);
        await new Promise(resolve => setTimeout(resolve, handleReturnDelayMs));
      }
      return {
        kind: "process",
        sessionId: "simulated-worker",
        isAlive: async () => alive,
        closed,
        stop,
      } as Awaited<ReturnType<ProcessTransport["launch"]>>;
    });
    try {
      const handle = await manager.spawn({ task: "HANG_WITH_PROGRESS", transport: "process" });
      const result = await manager.wait(handle.id);
      expect(launchStartedAt).toHaveLength(1);
      expect(result.status).toBe("timed_out");
      expect(JSON.parse(fs.readFileSync(path.join(manager.runDirectory(handle.id)!, "status.json"), "utf8")))
        .toMatchObject({ status: "timed_out" });
      expect(statusAtStop.length).toBeGreaterThan(0);
      expect(statusAtStop.every(status => status === "running")).toBe(true);
    } finally {
      spy.mockRestore();
      for (const handle of liveHandles) await handle.stop();
    }
  }, 15_000);

  it("does not retry after an explicit stop races a worker exit", async () => {
    const manager = makeManager(30_000);
    const launch = ProcessTransport.prototype.launch;
    const transports: Array<{ stop(): Promise<void> }> = [];
    const statusAtStop: string[] = [];
    let runId = "";
    const spy = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function (this: ProcessTransport, request) {
      const transport = await launch.call(this, request);
      transports.push(transport);
      return {
        ...transport,
        stop: async () => {
          statusAtStop.push(JSON.parse(fs.readFileSync(path.join(manager.runDirectory(runId)!, "status.json"), "utf8")).status);
          await transport.stop();
        },
      };
    });
    try {
      const handle = await manager.spawn({ task: "HANG", transport: "process" });
      runId = handle.id;
      await vi.waitFor(() => expect(fs.existsSync(path.join(manager.runDirectory(handle.id)!, "status.json"))).toBe(true), { timeout: 5_000 });
      const result = await manager.stop(handle.id);
      expect(result.status).toBe("stopped");
      expect(transports).toHaveLength(1);
      expect(statusAtStop.length).toBeGreaterThan(0);
      expect(statusAtStop.every(status => status === "running")).toBe(true);
    } finally {
      spy.mockRestore();
      for (const transport of transports) await transport.stop();
    }
  }, 15_000);

  it.each([
    { outcome: "still running", finalStatus: "timed_out", publishCompletion: false },
    { outcome: "completed", finalStatus: "completed", publishCompletion: true },
  ] as const)("preserves a replacement result when its launch returns after the deadline (%s)", async ({ finalStatus, publishCompletion }) => {
    const timeoutMs = 2_000;
    const manager = makeManager(timeoutMs);
    const launchStartedAt: number[] = [];
    const statusAtStop: string[] = [];
    const liveHandles: Array<{ stop(): Promise<void> }> = [];
    const spy = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function (this: ProcessTransport, request) {
      const startedAt = Date.now();
      launchStartedAt.push(startedAt);
      const args = request.workerArguments;
      const argument = (name: string): string => args[args.indexOf(name) + 1]!;
      const statusFile = argument("--status-file");
      const id = argument("--id");
      const name = argument("--name");
      const writeStatus = (status: "failed" | "running" | "completed") => fs.writeFileSync(statusFile, JSON.stringify({
        id, name, task: "replacement deadline regression", status, runner: "pi", transport: "process",
        cwd: argument("--cwd"), startedAt, updatedAt: Date.now(), turns: 0, toolCalls: 0,
        text: status === "completed" ? "replacement completed" : "",
        ...(status === "failed" ? { error: "Agent transport exited without a result" } : {}),
        exitCode: status === "failed" ? 1 : null,
        usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
        ...(status !== "running" ? { finishedAt: Date.now() } : {}),
      }));
      if (launchStartedAt.length === 1) {
        writeStatus("failed");
        return {
          kind: "process", sessionId: "simulated-first-worker", isAlive: async () => false,
          closed: Promise.resolve(), stop: async () => {},
        } as Awaited<ReturnType<ProcessTransport["launch"]>>;
      }

      writeStatus("running");
      let alive = true;
      let close!: () => void;
      const closed = new Promise<void>(resolve => { close = resolve; });
      const stop = async () => {
        statusAtStop.push(JSON.parse(fs.readFileSync(statusFile, "utf8")).status);
        alive = false;
        close();
      };
      liveHandles.push({ stop });
      // The retry is admitted after its startup backoff, but its handle arrives
      // only after the original run deadline. Optionally publish terminal output
      // immediately before that handle arrives to exercise terminal preservation.
      await new Promise(resolve => setTimeout(resolve, timeoutMs + 1_000));
      if (publishCompletion) writeStatus("completed");
      return {
        kind: "process", sessionId: "simulated-replacement-worker", isAlive: async () => alive,
        closed, stop,
      } as Awaited<ReturnType<ProcessTransport["launch"]>>;
    });
    try {
      const handle = await manager.spawn({ task: "replacement deadline regression", transport: "process" });
      const result = await manager.wait(handle.id);
      expect(launchStartedAt).toHaveLength(2);
      expect(result.status).toBe(finalStatus);
      expect(JSON.parse(fs.readFileSync(path.join(manager.runDirectory(handle.id)!, "status.json"), "utf8")))
        .toMatchObject({ status: finalStatus });
      expect(statusAtStop.length).toBeGreaterThan(0);
      expect(statusAtStop.every(status => status === (publishCompletion ? "completed" : "running"))).toBe(true);
    } finally {
      spy.mockRestore();
      for (const handle of liveHandles) await handle.stop();
    }
  }, 15_000);

  it("still retries a genuine early worker crash under the cap", async () => {
    const manager = makeManager(30_000, "tests/fixtures/fake-worker-startup-retry.mjs");
    const result = await manager.run({ task: "Recoverable boot death", transport: "process" });
    expect(result.status).toBe("completed");
    expect(fs.readFileSync(path.join(manager.runDirectory(result.id)!, "startup-attempts"), "utf8")).toBe("2");
  }, 15_000);
});
