import { spawn, type ChildProcess } from "node:child_process";
import { EventEmitter } from "node:events";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import type { AgentRunRecord } from "../src/agents/types.js";
import { canRemoveTerminalRun, hasUnresolvedWorker } from "../src/storage/retention.js";

vi.mock("node:child_process", async importOriginal => {
  const actual = await importOriginal<typeof import("node:child_process")>();
  return { ...actual, spawn: vi.fn() };
});

const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
afterEach(() => {
  Object.defineProperty(process, "platform", platform);
  vi.useRealTimers(); vi.restoreAllMocks(); vi.clearAllMocks();
  vi.mocked(spawn).mockReset();
});

// No real workers or descendants: taskkill and the captured worker are native-event mocks.
// The unconfirmed native descendant intentionally has no Fabric status/identity file.
describe("Windows tree-stop owner custody (#360 security R1)", () => {
  it.each(["error", "terminal-before-error", "resume-error", "nonzero", "timeout", "spawn-throw", "confirmed"] as const)(
    "fences all owner-release paths until tree exit is confirmed (%s)", async outcome => {
      vi.useFakeTimers();
      vi.spyOn(process, "emitWarning").mockImplementation(() => {});
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-tree-fence-"));
      const worker = Object.assign(new EventEmitter(), { pid: 2147483647, unref: vi.fn(), kill: vi.fn() });
      const killer = Object.assign(new EventEmitter(), { pid: 2147483646, kill: vi.fn() });
      let run = "";
      let markerAtFallback = false;
      let workerExited = false;
      worker.kill.mockImplementation(() => {
        markerAtFallback = hasUnresolvedWorker(run);
        workerExited = true;
        worker.emit("exit", null); worker.emit("close", null);
        return true;
      });
      vi.mocked(spawn).mockReturnValueOnce(worker as unknown as ChildProcess);
      if (outcome === "spawn-throw") vi.mocked(spawn).mockImplementationOnce(() => { throw new Error("helper failed to start"); });
      else vi.mocked(spawn).mockReturnValueOnce(killer as unknown as ChildProcess);
      vi.spyOn(process, "kill").mockImplementation(() => {
        if (workerExited) throw Object.assign(new Error("captured worker absent"), { code: "ESRCH" });
        return true;
      });
      const manager = new AgentManager(process.cwd(), {
        ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent: 1, retainRuns: false, budgetUsd: 0,
      }, { runRoot: root, workerPath: path.join(root, "mock-worker.mjs") });
      try {
        Object.defineProperty(process, "platform", { ...platform, value: "win32" });
        const handle = await manager.spawn({ task: "work already produced", transport: "process" });
        run = manager.runDirectory(handle.id)!;
        const record: AgentRunRecord = {
          id: handle.id, name: handle.name, task: "work already produced", status: "running",
          runner: "pi", transport: "process", sessionId: String(worker.pid), cwd: process.cwd(),
          startedAt: Date.now(), updatedAt: Date.now(), turns: 1, toolCalls: 1, text: "progress",
          usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, cost: 0 },
        };
        fs.writeFileSync(path.join(run, "status.json"), JSON.stringify(record));
        let stopped = false;
        if (outcome === "resume-error") fs.writeFileSync(path.join(run, "status.json"), JSON.stringify({ ...record, status: "stopped", finishedAt: Date.now() }));
        const stopping = (outcome === "resume-error" ? manager.wait(handle.id) : manager.stop(handle.id)).then(() => { stopped = true; });
        if (outcome === "resume-error") {
          // The automatic resume path also invokes stop. Its bounded return is
          // not exit proof: an uncertain prior tree must never launch a replacement.
          await vi.advanceTimersByTimeAsync(30_000);
        } else if (outcome === "confirmed") {
          killer.emit("close", 0); workerExited = true; worker.emit("exit", 0); worker.emit("close", 0);
          await stopping;
        } else {
          if (outcome === "terminal-before-error") {
            // A terminal file may race the helper outcome. Do not release its
            // permit or publish a collectible result while stop still owes a join.
            fs.writeFileSync(path.join(run, "status.json"), JSON.stringify({ ...record, status: "completed", finishedAt: Date.now() }));
            let reported = false;
            void manager.wait(handle.id).then(() => { reported = true; });
            await vi.advanceTimersByTimeAsync(250);
            expect(reported, "terminal reporting cannot bypass an in-flight process stop").toBe(false);
          }
          if (outcome === "error" || outcome === "terminal-before-error") { killer.emit("error", new Error("helper failure")); killer.emit("close", 0); }
          if (outcome === "nonzero") killer.emit("close", 1);
          if (outcome === "timeout") { await vi.advanceTimersByTimeAsync(1_000); killer.emit("close", 0); }
          // Parallel monitoring must be able to report failure, but not transfer custody.
          await vi.advanceTimersByTimeAsync(10_000);
        }
        Object.defineProperty(process, "platform", platform);
        const result = await manager.wait(handle.id);
        expect(result.status).toBe(outcome === "terminal-before-error" ? "completed" : outcome === "confirmed" || outcome === "resume-error" ? "stopped" : "failed");
        if (outcome === "confirmed") {
          expect(hasUnresolvedWorker(run)).toBe(false);
          expect(manager.retentionReferences().has(handle.id)).toBe(false);
          await expect(manager.checkpointForRelease()).resolves.toBeUndefined();
          await expect(manager.cleanup(handle.id)).resolves.toMatchObject({ cleaned: true });
        } else {
          expect(markerAtFallback, "persist uncertainty before killing only the worker").toBe(true);
          expect(hasUnresolvedWorker(run)).toBe(true);
          expect(manager.retentionReferences().has(handle.id)).toBe(true);
          expect(canRemoveTerminalRun(run)).toBe(false);
          await expect(manager.checkpointForRelease()).rejects.toThrow(/unresolved|unconfirmed/);
          await expect(manager.cleanup(handle.id)).rejects.toThrow(/lost track/);
        }
        expect(stopped, "the bounded stop must return without transferring custody").toBe(true);
        expect(spawn, "uncertain tree exit cannot launch a replacement worker").toHaveBeenCalledTimes(2);
        if (outcome === "resume-error") expect(result.error).toMatch(/not relaunched/);
        // A terminal report must not free the uncertain tree's one native permit.
        vi.mocked(spawn).mockReturnValueOnce(worker as unknown as ChildProcess);
        const next = await manager.spawn({ task: "capacity probe", transport: "process" });
        if (outcome === "confirmed") {
          expect(next.status).toBe("running");
          const nextRun = manager.runDirectory(next.id)!;
          fs.writeFileSync(path.join(nextRun, "status.json"), JSON.stringify({ ...record, id: next.id, status: "completed", finishedAt: Date.now() }));
          await vi.advanceTimersByTimeAsync(1_000);
        } else expect(next.status).toBe("queued");
        await manager.close();
        expect(fs.existsSync(run)).toBe(outcome !== "confirmed");
      } finally {
        Object.defineProperty(process, "platform", platform);
        // Finish every mock helper's timer; never leave an owned native process running.
        killer.emit("close", 0); worker.emit("exit", null); worker.emit("close", null);
        await vi.advanceTimersByTimeAsync(10_000);
        await manager.close();
        fs.rmSync(root, { recursive: true, force: true });
      }
    });
});
