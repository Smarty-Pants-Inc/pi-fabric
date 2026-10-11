import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";

const roots: string[] = [];
const managers: AgentManager[] = [];
afterEach(async () => {
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("built durable Fabric host", () => {
  it("executes fabric_exec with the TypeScript kernel and successfully runs a recursive durable child", async () => {
    const cwd = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-durable-recursive-")); roots.push(cwd);
    const agentDir = path.join(cwd, "agent"); fs.mkdirSync(agentDir);
    fs.writeFileSync(path.join(agentDir, "settings.json"), JSON.stringify({
      extensions: [path.resolve("tests/fixtures/durable-pi-extension.ts")],
      defaultProvider: "durable-offline", defaultModel: "test",
      compaction: { enabled: false }, retry: { enabled: false },
    }));
    fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({
      fullCodeMode: true, mesh: { enabled: false }, mcp: { enabled: false },
      agents: { runner: "pi-durable", maxDepth: 3, timeoutMs: 30000 },
    }));
    for (const key of ["PI_FABRIC_DEPTH", "PI_FABRIC_BUDGET", "PI_FABRIC_BUDGET_FILE", "PI_FABRIC_BUDGET_ID", "PI_FABRIC_WRITE_POLICY", "PI_FABRIC_TOOL_ALLOWLIST", "PI_FABRIC_SCOPE", "PI_FABRIC_LINEAGE", "PI_FABRIC_PARENT_RUN", "PI_FABRIC_ACTOR_ID"]) vi.stubEnv(key, undefined);
    vi.stubEnv("PI_CODING_AGENT_DIR", agentDir);
    vi.stubEnv("PI_OFFLINE", "1");
    vi.stubEnv("DURABLE_TEST_FAUX_MODULE", import.meta.resolve("@earendil-works/pi-ai/providers/faux"));
    vi.stubEnv("DURABLE_TEST_FABRIC_CODE", 'const child = await agents.run({ task: "recursive leaf", tools: [], extensions: true, thinking: "off" }); return { runner: child.runner, status: child.status, text: child.text };');
    const manager = new AgentManager(cwd, { ...DEFAULT_FABRIC_CONFIG.agents, maxDepth: 3, timeoutMs: 30000 }, {
      workerPath: path.resolve("dist/worker.js"), fabricExtensionPath: path.resolve("dist/index.js"),
      fullCodeMode: true, runRoot: path.join(cwd, "runs"), piBinary: path.join(cwd, "forbidden-native-fallback"),
    }); managers.push(manager);
    const result = await manager.run({ task: "fabric fixture", model: "durable-offline/test", recursive: true, kernel: "typescript", transport: "process" });
    expect(result.status, `${result.error}\n${result.stderr}\n${result.text}`).toBe("completed");
    expect(result.runner).toBe("pi-durable");
    expect(result.stderr ?? "").not.toContain("could not resolve the persisted assistant entry ID");
    expect(result.text).toContain("recursive leaf");
    expect(result.text).toContain("pi-durable");
    expect(result.text).toContain("completed");
    const events = fs.readFileSync(result.logFile!, "utf8");
    expect(events).toContain('"toolName":"fabric_exec"');
  }, 60000);
});
