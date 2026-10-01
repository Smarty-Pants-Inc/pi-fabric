import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { canRemoveTerminalRun, hasUnresolvedWorker, markRunRootActive, markRunRootClosed, sweepTempRunRoots } from "../src/storage/retention.js";

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

  it("bounds a hung process-handle query by the reversible release deadline and retains the unresolved worker", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "release-query-deadline-"));
    const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, notifyOnComplete: false, retainRuns: false, nice: 19 },
      { workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"), piBinary: process.execPath });
    const launch = ProcessTransport.prototype.launch;
    let handle: Awaited<ReturnType<typeof launch>> | undefined;
    let hung = false; let finishQuery!: () => void;
    const query = new Promise<boolean>(resolve => { finishQuery = () => resolve(false); });
    const spy = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function(this: ProcessTransport, request) {
      handle = await launch.call(this, request);
      return { ...handle, relaunchable: false, isAlive: () => hung ? query : handle!.isAlive() };
    });
    let check: Promise<void> | undefined;
    try {
      const info = await manager.spawn({ task: "HANG until stopped", transport: "process" });
      const run = manager.runDirectory(info.id)!; const file = path.join(run, "status.json");
      while (!fs.existsSync(file)) await sleep(10);
      const record = JSON.parse(fs.readFileSync(file, "utf8"));
      fs.writeFileSync(file, JSON.stringify({ ...record, status: "failed", error: "terminal UI failure", finishedAt: Date.now() }));
      await manager.wait(info.id); hung = true;
      check = manager.checkpointForRelease(Date.now() + 80);
      const outcome = await Promise.race([check.then(() => "accepted", () => "vetoed"), sleep(400).then(() => "hung")]);
      expect(outcome).toBe("vetoed");
      expect(hasUnresolvedWorker(run)).toBe(true);
      finishQuery(); hung = false;
      await manager.close();
      expect(fs.existsSync(run)).toBe(true);
      expect(await handle!.isAlive()).toBe(true);
    } finally {
      finishQuery(); hung = false; await check?.catch(() => undefined);
      await handle?.stop(); spy.mockRestore(); await manager.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  }, 20_000);
});
