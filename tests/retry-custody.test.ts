import fs from "node:fs";

// Inject custody branches, not a foreign filesystem. This POSIX-host adapter
// supplies the private test inode; native Windows retains the real DACL policy.
vi.mock("../src/storage/windows-temp-root.js", async importOriginal => {
  const actual = await importOriginal<typeof import("../src/storage/windows-temp-root.js")>();
  return { ...actual, windowsDataRoot: process.platform === "win32" ? actual.windowsDataRoot :
    (root: string) => {
      const stat = fs.lstatSync(root);
      if (!path.isAbsolute(root) || !stat.isDirectory() || stat.isSymbolicLink() || (stat.mode & 0o777) !== 0o700) {
        throw new Error("Unsafe simulated Windows test root");
      }
      return fs.realpathSync(root);
    } };
});
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";

// Inject the manager's platform branch, not native process-tree certification.
// The real filesystem still follows the host OS: Linux injection on Windows
// would incorrectly enable POSIX directory-fsync barriers. Keep the Windows
// branch on both hosts, but exercise the Linux branch only on a POSIX host.
const platforms = (["linux", "win32"] as const).filter(platform => process.platform !== "win32" || platform === "win32");
describe.each(platforms)("retry execution custody (%s)", platform => {
  it.each([
    { retry: "startup", operation: "stop" }, { retry: "resume", operation: "stop" },
    { retry: "startup", operation: "close" }, { retry: "resume", operation: "close" },
  ] as const)("$operation joins the exact in-flight $retry replacement", async ({ retry, operation }) => {
    vi.useFakeTimers();
    vi.spyOn(process, "platform", "get").mockReturnValue(platform);
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-retry-custody-"));
    let finishLaunch!: () => void; const launchGate = new Promise<void>(resolve => { finishLaunch = resolve; });
    let finishExit!: () => void; const exitGate = new Promise<void>(resolve => { finishExit = resolve; });
    let launches = 0; let replacementAlive = false;
    const stops: number[] = [];
    const settled = vi.fn();
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
      const attempt = ++launches;
      const statusFile = request.workerArguments[request.workerArguments.indexOf("--status-file") + 1]!;
      if (attempt === 1) {
        fs.writeFileSync(statusFile, JSON.stringify({ id: request.id, name: request.name, task: "retry",
          status: retry === "startup" ? "failed" : "stopped", runner: "pi", transport: "process", cwd: root,
          startedAt: Date.now(), updatedAt: Date.now(), finishedAt: Date.now(), turns: retry === "startup" ? 0 : 2,
          toolCalls: 0, text: "PROVISIONAL", error: retry === "startup" ? "No API key found for openai-codex" : "Agent stopped",
          usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 } }));
        return { kind: "process", isAlive: async () => false, stop: async () => { stops.push(attempt); } };
      }
      if (attempt > 2) {
        // The queued run may acquire admission after the replacement exits.
        // It must not mutate the replacement's per-attempt liveness.
        return { kind: "process", isAlive: async () => false, stop: async () => {} };
      }
      await launchGate;
      replacementAlive = true;
      return { kind: "process", isAlive: async () => replacementAlive,
        stop: async () => { stops.push(attempt); await exitGate; replacementAlive = false; } };
    });
    const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent: 1,
      timeoutMs: 60_000, budgetUsd: 0, retainRuns: true, notifyOnComplete: false },
    { runRoot: path.join(root, "runs"), workerPath: "unused", onSettled: settled });
    let stopping: Promise<unknown> | undefined;
    try {
      const handle = await manager.spawn({ task: "retry", transport: "process", actorId: "same-session-writer" });
      await vi.advanceTimersByTimeAsync(3_000);
      const targetSettled = () => settled.mock.calls.filter(([result]) => result.id === handle.id);
      expect(launches).toBe(2);
      expect(stops).toEqual([1]);
      expect((await manager.spawn({ task: "must stay queued" })).status).toBe("queued");
      let stopSettled = false;
      stopping = (operation === "stop" ? manager.stop(handle.id) : manager.close()).then(() => { stopSettled = true; });
      await vi.advanceTimersByTimeAsync(0);
      expect(stopSettled, "the old attempt's exit must not settle an in-flight launch").toBe(false);
      expect(targetSettled()).toHaveLength(0);
      finishLaunch();
      await vi.advanceTimersByTimeAsync(0);
      expect(stops).toEqual([1, 2]);
      expect(replacementAlive).toBe(true);
      expect(stopSettled, "the replacement's stop join must confirm its own exit").toBe(false);
      expect(targetSettled()).toHaveLength(0);
      expect(launches).toBe(2);
      finishExit();
      await vi.advanceTimersByTimeAsync(0);
      await stopping;
      expect(replacementAlive).toBe(false);
      expect(stopSettled).toBe(true);
      expect(settled.mock.calls.some(([result]) => result.id === handle.id && result.status === "stopped")).toBe(true);
    } finally {
      finishLaunch(); finishExit();
      await vi.advanceTimersByTimeAsync(10_000);
      await stopping;
      await manager.close();
      vi.useRealTimers(); vi.restoreAllMocks();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
});
