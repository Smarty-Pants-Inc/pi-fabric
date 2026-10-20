import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";

const loads = vi.hoisted(() => vi.fn());
const sandboxLoads = vi.hoisted(() => vi.fn());
vi.mock("../src/agents/spawn-router.js", async original => {
  loads(); return original<typeof import("../src/agents/spawn-router.js")>();
});
vi.mock("../src/transport/sandbox/transport.js", async original => {
  sandboxLoads(); return original<typeof import("../src/transport/sandbox/transport.js")>();
});

describe("spawn router cold import and idle", () => {
  it("keeps the subprocess adapter out of configuration and provider imports until first use", async () => {
    vi.resetModules(); loads.mockClear(); sandboxLoads.mockClear();
    const config = await import("../src/config.js");
    config.normalizeFabricConfig({ agents: { router: { command: [process.execPath], mode: "enforce" } } });
    await import("../src/providers/agents-provider.js");
    await new Promise(resolve => setTimeout(resolve, 10));
    expect(loads).not.toHaveBeenCalled();
    expect(sandboxLoads).not.toHaveBeenCalled();
    const { AgentManager } = await import("../src/agents/manager.js");
    const runRoot = fs.mkdtempSync(path.join(os.tmpdir(), "sandbox-lazy-first-use-"));
    const manager = new AgentManager(process.cwd(), {
      ...config.DEFAULT_FABRIC_CONFIG.agents,
      budgetUsd: 0,
      sandbox: { enabled: false, template: "smarty-proof-agent", readyTimeoutMs: 90_000 },
    }, { runRoot, workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), projectRoot: process.cwd() });
    try {
      await expect(manager.spawn({ task: "lazy import probe", model: "cliproxyapi/gpt-6.1-sol", thinking: "high", transport: "sandbox", extensions: false }))
        .rejects.toMatchObject({ name: "SandboxUnsupportedError", code: "sandbox.unsupported" });
      expect(sandboxLoads).toHaveBeenCalledTimes(1);
    } finally {
      await manager.close();
      fs.rmSync(runRoot, { recursive: true, force: true });
    }
    const adapter = await import("../src/agents/spawn-router.js");
    expect(typeof adapter.routeAgentCreation).toBe("function");
    expect(loads).toHaveBeenCalledTimes(1);
  });
});
