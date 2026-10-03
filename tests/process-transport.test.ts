import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { spawnDetached } from "../src/agents/transports/process-utils.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";

const roots: string[] = [];
const managers: AgentManager[] = [];
afterEach(async () => {
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const directory = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-process-transport-"));
  roots.push(root);
  return root;
};

describe("process transport startup diagnostics", () => {
  it("rejects a failed spawn without an unhandled child error", async () => {
    const root = directory();
    await expect(spawnDetached(path.join(root, "missing.mjs"), [], path.join(root, "missing-cwd"))).rejects.toThrow(/ENOENT/);
    await new Promise(resolve => setImmediate(resolve));
  });

  it("surfaces bounded stderr even when the worker cannot execute its bootstrap", async () => {
    const root = directory();
    const worker = path.join(root, "crash.mjs");
    fs.writeFileSync(worker, 'process.stderr.write("x".repeat(60000) + "\\nworker-bootstrap-sentinel 界面\\n"); process.exitCode = 1;');
    const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 20000, sessionExport: false }, {
      workerPath: worker, runRoot: path.join(root, "runs"), fullCodeMode: false,
    });
    managers.push(manager);
    const result = await manager.run({ task: "probe", transport: "process", extensions: false });
    expect(result.status).toBe("failed");
    expect(result.error).toContain("worker stderr:");
    expect(result.error).toContain("worker-bootstrap-sentinel 界面");
    expect(result.error).not.toContain("�");
    expect(result.error!.length).toBeLessThan(21000);
  });
});
