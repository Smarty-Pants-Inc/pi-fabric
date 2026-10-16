import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import type { AgentRunRecord } from "../src/agents/types.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { writeJsonAtomic } from "../src/core/atomic-write.js";

// Keep: CI's cancelled queued failed-stop incident is an admission/data-safety
// boundary. Simulated win32 only: no Windows process APIs or real workers.
// No deletion or Git hooks: removals are recorded, never delegated to fs.
// Explicit retained roots are intentionally left for their external owner.
describe("cancelled queued launch (simulated win32, recorded-call/no-delete)", () => {
  it.each(["exact-exit", "lost-contact"] as const)("keeps cancellation pending until %s", async (outcome) => {
    const nativePlatform = Object.getOwnPropertyDescriptor(process, "platform")!;
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "cancelled-launch-no-delete-"));
    const remove = vi.spyOn(fs, "rmSync").mockImplementation(() => undefined);
    const removeAsync = vi.spyOn(fs.promises, "rm").mockResolvedValue(undefined);
    // Atomic write normally only unlinks on error; record that path too.
    const unlink = vi.spyOn(fs, "unlinkSync").mockImplementation(() => undefined);
    Object.defineProperty(process, "platform", { ...nativePlatform, value: "win32" });
    let release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    let ready!: () => void;
    const creating = new Promise<void>(resolve => { ready = resolve; });
    let workerExited = false;
    let immutableDebt = false;
    let firstAlive = true;
    let launches = 0;
    const queuedStop = vi.fn(async () => { throw new Error("close acknowledgment lost"); });
    const settled = vi.fn();
    const launch = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
      const args = new Map<string, string>();
      for (let i = 0; i < request.workerArguments.length; i += 2) {
        args.set(request.workerArguments[i]!, request.workerArguments[i + 1]!);
      }
      const statusFile = args.get("--status-file")!;
      const record: AgentRunRecord = {
        id: request.id, name: request.name, task: "queued cancellation", status: "running", runner: "pi", transport: "process",
        sessionId: "simulated-worker", cwd: request.cwd, startedAt: Date.now(), updatedAt: Date.now(),
        turns: 0, toolCalls: 0, text: "", usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
      };
      writeJsonAtomic(statusFile, record);
      if (++launches === 1) {
        return { kind: "process", sessionId: "simulated-first", isAlive: async () => firstAlive,
          stop: async () => { firstAlive = false; } };
      }
      ready();
      await gate;
      // A worker candidate is not a supervisor result. It must remain finishing
      // and unjoined while this exact attempt still owns execution custody.
      writeJsonAtomic(statusFile, { ...record, status: "stopped", error: "Agent stopped", finishedAt: Date.now() });
      return { kind: "process", sessionId: "simulated-worker", stop: queuedStop,
        isAlive: async () => !workerExited,
        lostContact: () => immutableDebt ? "tree exit observation unavailable" : undefined };
    });
    const manager = new AgentManager(process.cwd(), {
      ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent: 1, budgetUsd: 0, retainRuns: true,
    }, { runRoot: root, workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), onSettled: settled });
    try {
      const first = await manager.spawn({ task: "holds admission", transport: "process" });
      const queued = await manager.spawn({ task: "cancel during worker creation", transport: "process" });
      expect(queued.status).toBe("queued");
      await manager.stop(first.id);
      await creating;
      const stopped = manager.stop(queued.id);
      const rejected = expect(stopped).rejects.toThrow(/execution exit unconfirmed/);
      release();
      await rejected;
      const directory = manager.runDirectory(queued.id)!;
      const marker = path.join(directory, "unresolved-worker.json");
      expect(manager.status(queued.id).status).toBe("finishing");
      expect(manager.isSettled(queued.id)).toBe(false);
      expect(manager.hasRunCustody(queued.id)).toBe(true);
      await expect(manager.wait(queued.id, { timeoutMs: 20 })).rejects.toThrow(/still running|timed out/i);
      await expect(manager.cleanup(queued.id)).rejects.toThrow(/running agent/);
      expect(fs.existsSync(path.join(directory, "task.txt"))).toBe(true);
      expect(fs.existsSync(marker)).toBe(true);
      expect(remove.mock.calls.some(([file]) => String(file) === marker)).toBe(false);
      expect(settled.mock.calls.filter(([result]) => result.id === queued.id)).toHaveLength(0);
      const next = await manager.spawn({ task: "must not replace live custody", transport: "process" });
      expect(next.status).toBe("queued");
      expect(launch).toHaveBeenCalledTimes(2);
      await manager.stop(next.id); // never launched; archive removals remain recorded

      workerExited = true;
      immutableDebt = outcome === "lost-contact";
      const attempts = queuedStop.mock.calls.length;
      if (immutableDebt) {
        // Counterexample: false liveness with immutable contact/tree debt is
        // not exact exit proof, even for cancellation. Do not broaden the fix.
        await expect(manager.stop(queued.id)).rejects.toThrow(/execution exit unconfirmed/);
        expect(manager.status(queued.id).status).toBe("finishing");
        expect(manager.isSettled(queued.id)).toBe(false);
        expect(manager.hasRunCustody(queued.id)).toBe(true);
        expect(remove.mock.calls.some(([file]) => String(file) === marker)).toBe(false);
        immutableDebt = false; // fixture-only exact contact recovery for close
      }
      expect(await manager.stop(queued.id)).toMatchObject({ status: "stopped", error: "Agent stopped" });
      expect(queuedStop).toHaveBeenCalledTimes(attempts + 1); // no redundant failed hook after proof
      expect(await manager.wait(queued.id)).toMatchObject({ status: "stopped" });
      expect(manager.isSettled(queued.id)).toBe(true);
      expect(manager.runningCount()).toBe(0);
      // Retained result/publication custody is distinct from active admission.
      expect(manager.hasRunCustody(queued.id)).toBe(true);
      expect(settled.mock.calls.filter(([result]) => result.id === queued.id)).toHaveLength(1);
      expect(remove.mock.calls.some(([file]) => String(file) === marker)).toBe(true);
      // The recorder did not remove the marker or worker inputs. This is not a
      // deletion/collectibility proof (in particular the fake PID is unknown).
      expect(fs.existsSync(marker)).toBe(true);
      expect(fs.existsSync(path.join(directory, "task.txt"))).toBe(true);
      expect(removeAsync).not.toHaveBeenCalled();
      expect(unlink).not.toHaveBeenCalled();
    } finally {
      release(); workerExited = true; firstAlive = false; immutableDebt = false;
      // Join while the recorder/platform seam remain installed. No root teardown.
      try { await manager.close(); }
      finally {
        launch.mockRestore();
        Object.defineProperty(process, "platform", nativePlatform);
        remove.mockRestore(); removeAsync.mockRestore(); unlink.mockRestore();
      }
    }
  });
});
