import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { AgentManager } from "../src/agents/manager.js";
import { HerdrTransport } from "../src/agents/transports/herdr-transport.js";

// Public Herdr admission is intentionally disabled (F4). Dispatch/cancellation
// authority remains covered directly in herdr-transport.test.ts; a manager must
// never reach those dispatch boundaries until outer execution custody exists.
describe("AgentManager Herdr admission scope cut", () => {
  it.each(["immediate", "queued"] as const)("refuses %s Herdr admission before adapter launch", async mode => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-herdr-manager-"));
    const launch = vi.spyOn(HerdrTransport.prototype, "launch").mockRejectedValue(new Error("unsafe dispatch"));
    const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent: 1, retainRuns: false }, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: root,
    });
    try {
      if (mode === "immediate") {
        await expect(manager.spawn({ task: "never dispatch", transport: "herdr" })).rejects.toThrow(/disabled: detached execution custody is not confirmed/);
      } else {
        const first = await manager.spawn({ task: "HANG", transport: "process" });
        const queued = await manager.spawn({ task: "never dispatch", transport: "herdr" });
        expect(queued.status).toBe("queued");
        await manager.stop(first.id);
        expect(await manager.wait(queued.id)).toMatchObject({ status: "failed", error: expect.stringContaining("disabled: detached execution custody is not confirmed") });
        expect(await manager.cleanup(queued.id)).toEqual({ cleaned: true });
      }
      expect(launch).not.toHaveBeenCalled();
      expect(manager.runningCount()).toBe(0);
    } finally {
      launch.mockRestore(); await manager.close();
      fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
    }
  });
});
