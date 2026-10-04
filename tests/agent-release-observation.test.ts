import { execFileSync, spawn, type ChildProcess } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { canRemoveTerminalRun, hasUnresolvedWorker, markRunRootActive, markRunRootClosed, sweepTempRunRoots } from "../src/storage/retention.js";

import { retainedProcessStates, retainedProcessWorker } from "./helpers/retained-process-worker.js";

const sleep = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
describe("checked agent release observations", () => {
  it.each(["tmux", "screen"])("preserves terminal %s trees in resident and orphaned retention", kind => {
    const temp = fs.mkdtempSync(path.join(os.tmpdir(), "external-run-retention-"));
    const root = path.join(temp, "pi-fabric-runs-external");
    try {
      markRunRootActive(root, 1);
      const run = path.join(root, "run"); fs.mkdirSync(run);
      fs.writeFileSync(path.join(run, "status.json"), JSON.stringify({ status: "failed", transport: kind, sessionId: "external-pane", finishedAt: 1 }));
      fs.writeFileSync(path.join(run, "task.txt"), "retained working input");
      markRunRootClosed(root, 1, true);
      expect(canRemoveTerminalRun(run)).toBe(false);
      const swept = sweepTempRunRoots({ tempRoot: temp, now: 100_000, orphanedTempRunRetentionMs: 1, oneShotRunRetentionMs: 1 });
      expect(swept.removedRuns).toEqual([]);
      expect(fs.existsSync(run)).toBe(true);
    } finally { fs.rmSync(temp, { recursive: true, force: true }); }
  });

  it.each([["tmux", "cleanup"], ["screen", "cleanup"], ["tmux", "close"], ["screen", "close"]] as const)("vetoes tracked process-parent %s tree deletion via %s with a terminal live nested worker", async (kind, action) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'external-nested-cleanup-'));
    const git = (...args: string[]) => execFileSync('git', args, { cwd: root, stdio: 'ignore' });
    git('init', '-q'); git('config', 'user.name', 'Fixture'); git('config', 'user.email', 'fixture@example.invalid');
    fs.writeFileSync(path.join(root, 'input.txt'), 'parent input'); git('add', '.'); git('commit', '-qm', 'fixture');
    const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, notifyOnComplete: false, retainRuns: false, nice: 19 },
      { workerPath: path.resolve('tests/fixtures/fake-worker.mjs'), runRoot: path.join(root, 'runs'), piBinary: process.execPath });
    let worker: ChildProcess | undefined; let exited: Promise<void> | undefined;
    try {
      const info = await manager.spawn({ task: 'complete parent', transport: 'process', worktree: true });
      await manager.wait(info.id);
      const run = manager.runDirectory(info.id)!;
      const child = path.join(run, 'nested', 'external'); fs.mkdirSync(child, { recursive: true });
      fs.writeFileSync(path.join(child, 'status.json'), JSON.stringify({ status: 'failed', transport: kind, sessionId: 'live-external-pane', finishedAt: 1 }));
      fs.writeFileSync(path.join(child, 'task.txt'), 'retained child input');
      worker = spawn(process.execPath, ["-e", "setInterval(() => {}, 1000)"], { cwd: child, stdio: "ignore" });
      exited = new Promise<void>((resolve, reject) => { worker!.once("error", reject); worker!.once("close", () => resolve()); });
      expect(hasUnresolvedWorker(run)).toBe(false);
      if (action === "cleanup") await expect(manager.cleanup(info.id)).rejects.toThrow(/checked.*exit|exit.*unconfirmed/);
      else await manager.close();
      expect(worker.exitCode).toBeNull(); expect(worker.signalCode).toBeNull();
      expect(fs.existsSync(run)).toBe(true); expect(fs.existsSync(info.worktree!)).toBe(true);
      await manager.close();
      expect(fs.existsSync(child)).toBe(true); expect(fs.existsSync(info.worktree!)).toBe(true);
    } finally {
      worker?.kill("SIGTERM"); await exited; await manager.close();
      fs.rmSync(root, { recursive: true, force: true, maxRetries: 20, retryDelay: 50 });
    }
  }, 20_000);

  // smarty-dev#3148: the parent's process exit is not its detached child's exit receipt.
  it.each(retainedProcessStates.flatMap(state => (["cleanup", "close"] as const).map(action => ({ state, action }))))(
    "vetoes nested process $state through tracked parent $action until exit is confirmed", async ({ state, action }) => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "process-nested-cleanup-"));
      const git = (...args: string[]) => execFileSync("git", args, { cwd: root, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] });
      git("init", "-q"); git("config", "user.name", "Fixture"); git("config", "user.email", "fixture@example.invalid");
      fs.writeFileSync(path.join(root, "input.txt"), "parent input"); git("add", "."); git("commit", "-qm", "fixture");
      // Explicit resident-style root, not the managed temporary root's stronger sweep predicate.
      const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, notifyOnComplete: false, retainRuns: false, nice: 19 },
        { workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"), piBinary: process.execPath });
      let child: Awaited<ReturnType<typeof retainedProcessWorker>> | undefined;
      try {
        const info = await manager.spawn({ task: "complete recursive parent", recursive: true, transport: "process", worktree: true });
        await manager.wait(info.id);
        const run = manager.runDirectory(info.id)!;
        child = await retainedProcessWorker(path.join(run, "nested", "process-child"), info.worktree!, state);
        if (state === "missing-identity") {
          const published = JSON.parse(fs.readFileSync(child.statusFile, "utf8"));
          expect(published).toMatchObject({ status: "completed", transport: "process" });
          expect(published).not.toHaveProperty("sessionId");
        }
        expect(hasUnresolvedWorker(run)).toBe(false);
        if (action === "cleanup") await expect(manager.cleanup(info.id, true)).rejects.toThrow(/exit.*unconfirmed/);
        else await manager.close();
        expect(child.worker.exitCode).toBeNull(); expect(child.worker.signalCode).toBeNull();
        for (const file of [run, path.join(run, "status.json"), child.taskFile, child.statusFile, info.worktree!]) expect(fs.existsSync(file)).toBe(true);
        expect(git("worktree", "list", "--porcelain")).toContain(`branch refs/heads/${info.branch}`);
        expect(canRemoveTerminalRun(run)).toBe(false);
        await child.stop();
        if (state !== "terminal-live") {
          // A dead PID alone does not repair a nonterminal or unusable saved identity.
          expect(canRemoveTerminalRun(run)).toBe(false);
          if (action === "cleanup") await expect(manager.cleanup(info.id, true)).rejects.toThrow(/exit.*unconfirmed/);
        }
        child.confirmExit();
        expect(canRemoveTerminalRun(run)).toBe(true);
        if (action === "cleanup") {
          expect(await manager.cleanup(info.id, true)).toEqual({ cleaned: true });
          expect(fs.existsSync(run)).toBe(false); expect(fs.existsSync(info.worktree!)).toBe(false);
          expect(git("branch", "--list", info.branch!)).toBe("");
        }
      } finally {
        await child?.stop(); await manager.close();
        fs.rmSync(root, { recursive: true, force: true, maxRetries: 20, retryDelay: 50 });
      }
    }, 20_000,
  );

  it("keeps safe pre-launch worktree rollback distinct from an admitted process record", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "process-prelaunch-rollback-"));
    const git = (...args: string[]) => execFileSync("git", args, { cwd: root, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] });
    git("init", "-q"); git("config", "user.name", "Fixture"); git("config", "user.email", "fixture@example.invalid");
    fs.writeFileSync(path.join(root, "input.txt"), "input"); git("add", "."); git("commit", "-qm", "fixture");
    const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, nice: 19 },
      { workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"), piBinary: process.execPath });
    let worktree: string | undefined;
    const launch = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
      worktree = request.cwd;
      expect(fs.existsSync(worktree)).toBe(true);
      throw new Error("rejected before worker launch");
    });
    try {
      await expect(manager.spawn({ task: "never admitted", transport: "process", worktree: true })).rejects.toThrow("rejected before worker launch");
      expect(worktree).toBeDefined(); expect(fs.existsSync(worktree!)).toBe(false);
      expect(git("worktree", "list", "--porcelain").match(/^worktree /gm)).toHaveLength(1);
      expect(git("branch", "--list", "pi-fabric/*")).toBe("");
    } finally {
      launch.mockRestore(); await manager.close();
      fs.rmSync(root, { recursive: true, force: true, maxRetries: 20, retryDelay: 50 });
    }
  });

  it("bounds a hung process-handle query by the reversible release deadline and retains the unresolved worker", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "release-query-deadline-"));
    const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, notifyOnComplete: false, retainRuns: false, nice: 19 },
      { workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"), piBinary: process.execPath });
    let worker: ChildProcess | undefined; let exited: Promise<void> | undefined;
    let handle: Awaited<ReturnType<ProcessTransport["launch"]>> | undefined;
    let hung = false; let finishQuery!: () => void;
    const query = new Promise<boolean>(resolve => { finishQuery = () => resolve(false); });
    const spy = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
      // This test mocks liveness, so own the fixture child and its close receipt too.
      // fake-worker's HANG branch launches no descendants (piBinary is unused).
      worker = spawn(process.execPath, [request.workerPath, ...request.workerArguments], { cwd: request.cwd, stdio: "ignore" });
      exited = new Promise<void>((resolve, reject) => { worker!.once("error", reject); worker!.once("close", () => resolve()); });
      handle = {
        kind: "process", sessionId: String(worker.pid),
        stop: async () => { worker!.kill("SIGTERM"); },
        isAlive: async () => worker!.exitCode === null && worker!.signalCode === null,
      };
      return { ...handle, relaunchable: false, isAlive: () => hung ? query : handle!.isAlive() };
    });
    let check: Promise<void> | undefined;
    try {
      const info = await manager.spawn({ task: "complete parent", transport: "process" });
      const run = manager.runDirectory(info.id)!;
      // A terminal UI record cannot settle a still-live worker under custody.
      // Settle its actual execution first, then inject a hung release re-observation.
      await manager.wait(info.id); hung = true;
      check = manager.checkpointForRelease(Date.now() + 80);
      const outcome = await Promise.race([check.then(() => "accepted", () => "vetoed"), sleep(400).then(() => "hung")]);
      expect(outcome).toBe("vetoed");
      expect(hasUnresolvedWorker(run)).toBe(true);
      finishQuery(); hung = false;
      // Explicit close now joins execution custody, including terminal UI runs.
      await manager.close();
      expect(fs.existsSync(run)).toBe(true);
      expect(await handle!.isAlive()).toBe(false);
    } finally {
      finishQuery(); hung = false; await check?.catch(() => undefined);
      await handle?.stop();
      // A PID liveness probe can report gone before Node has reaped/released its
      // native ChildProcess handle on Windows. Await close, not just isAlive(),
      // before removing the fixture cwd; synchronous rm retries block that callback.
      await exited;
      if (handle) expect(await handle.isAlive()).toBe(false);
      spy.mockRestore(); await manager.close();
      fs.rmSync(root, { recursive: true, force: true, maxRetries: 20, retryDelay: 50 });
    }
  }, 20_000);
});
