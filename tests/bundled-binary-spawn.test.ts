import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import type { AgentRunResult } from "../src/agents/types.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";

// End-to-end proof that subagents still work when pi ships as a Bun-compiled
// single-file binary: process.execPath is the pi executable (not node/bun), so
// no transport may spawn the worker as `process.execPath worker.js`. We
// simulate that by pointing process.execPath at a fake "pi binary" and
// resolving the real Node runtime through PI_FABRIC_NODE_BINARY. If a transport
// ever reverts to spawning process.execPath, the worker (a .js module) never
// runs and the agent fails — so a green run here proves the fix holds.
const workerPath = path.resolve("dist/worker.js");
const piBinary = path.resolve("tests/fixtures/fake-pi.mjs");
const hasWorker = fs.existsSync(workerPath);

// A non-node, non-bun path that stands in for the compiled pi executable. The
// resolver never spawns it (the override below takes precedence); its only job
// is to force the resolver off the generic-execPath fast path.
const fakeBundledExecPath = "/usr/local/bin/pi";

describe.skipIf(!hasWorker)("agent worker launch under a bundled pi binary", () => {
  const roots: string[] = [];
  const managers: AgentManager[] = [];
  const originalExecPath = process.execPath;
  const originalOverride = process.env.PI_FABRIC_NODE_BINARY;
  const originalBehavior = process.env.FAKE_PI_BEHAVIOR;

  afterEach(async () => {
    process.execPath = originalExecPath;
    if (originalOverride === undefined) delete process.env.PI_FABRIC_NODE_BINARY;
    else process.env.PI_FABRIC_NODE_BINARY = originalOverride;
    if (originalBehavior === undefined) delete process.env.FAKE_PI_BEHAVIOR;
    else process.env.FAKE_PI_BEHAVIOR = originalBehavior;
    await Promise.all(managers.splice(0).map((m) => m.close()));
    for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  });

  it("resolves a JS runtime and completes a subagent when process.execPath is the pi binary", async () => {
    // Capture the real node BEFORE simulating the bundled binary, then point
    // process.execPath at the fake pi so the resolver must fall back.
    const realNode = originalExecPath;
    process.execPath = fakeBundledExecPath;
    process.env.PI_FABRIC_NODE_BINARY = realNode;
    process.env.FAKE_PI_BEHAVIOR = "success";

    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-bundled-"));
    roots.push(root);
    const config = { ...DEFAULT_FABRIC_CONFIG.agents, runner: "pi" as const, timeoutMs: 20_000, maxConcurrent: 1 };
    const manager = new AgentManager(process.cwd(), config, { workerPath, piBinary, runRoot: root });
    managers.push(manager);

    const result: AgentRunResult = await manager.run({ task: "ok", transport: "process" });
    expect(result.status).toBe("completed");
    expect(result.text).toContain("hi");
  });
});
