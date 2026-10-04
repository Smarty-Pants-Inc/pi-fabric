import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";

// This drives BOTH parent and compiled worker Windows branches on Linux; it is
// not native Windows or a descendant-tree certification. Native worker-e2e's
// kill-worker case remains required and unskipped on Windows CI.
describe.skipIf(!fs.existsSync(path.resolve("dist/worker.js")))("Windows legacy custody scope", () => {
  it("settles a killed custodian without installing an unsupported execution-tree obligation", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-r4-windows-"));
    const wrapper = path.join(root, "windows-worker.mjs");
    // Cache the native spawn adapter before platform injection; only product
    // custody branches are injected, not Linux's ability to execute node.
    fs.writeFileSync(wrapper, `await import(${JSON.stringify(pathToFileURL(path.resolve("node_modules/cross-spawn/index.js")).href)});
Object.defineProperty(process, 'platform', {value: 'win32'});
await import(${JSON.stringify(pathToFileURL(path.resolve("dist/worker.js")).href)});`);
    const before = process.env.FAKE_PI_BEHAVIOR;
    process.env.FAKE_PI_BEHAVIOR = "kill-worker";
    const platform = vi.spyOn(process, "platform", "get").mockReturnValue("win32");
    const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 8000, maxConcurrent: 1 }, {
      workerPath: wrapper, piBinary: path.resolve("tests/fixtures/fake-pi.mjs"), runRoot: path.join(root, "runs"),
    });
    try {
      const handle = await manager.spawn({ task: "killed Windows custodian", transport: "process" });
      const result = await manager.wait(handle.id, { timeoutMs: 12000 });
      expect(result.status).toBe("failed");
      expect(result.error).toMatch(/exited without a result/);
      // The exact-head bug also hung close after the unsupported IPC flag stuck.
      await manager.close();
    } finally {
      await manager.close().catch(() => undefined);
      platform.mockRestore();
      if (before === undefined) delete process.env.FAKE_PI_BEHAVIOR; else process.env.FAKE_PI_BEHAVIOR = before;
      fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
    }
  }, 30000);
});
