import fs from "node:fs";
import { spawn } from "node:child_process";
import { once } from "node:events";
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

// Start the durable host directly, not through AgentManager's task launcher:
// this is a real root Main with no task launch markers, not an exempted task.
async function runDurableMain(cwd: string) {
  const child = spawn(process.execPath, [path.resolve("dist/durable/worker.js"),
    "--mode", "rpc", "--durable-directory", path.join(cwd, "durable-main"),
    "--durable-run-id", "main", "-e", path.resolve("dist/index.js"),
    "--model", "durable-offline/test"], { cwd, env: process.env, stdio: ["pipe", "pipe", "pipe"] });
  let events = "", stderr = "", pending = "";
  child.stderr.on("data", chunk => { stderr += chunk.toString(); });
  try {
    await new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error(`Durable Main timed out: ${stderr}\n${events}`)), 45_000);
      const finish = (error?: Error) => { clearTimeout(timeout); error ? reject(error) : resolve(); };
      child.on("error", finish);
      child.on("exit", () => finish(new Error(`Durable Main exited early: ${stderr}\n${events}`)));
      child.stdout.on("data", chunk => {
        events += chunk.toString(); pending += chunk.toString();
        let end: number;
        while ((end = pending.indexOf("\n")) >= 0) {
          const line = pending.slice(0, end); pending = pending.slice(end + 1);
          let event: any;
          try { event = JSON.parse(line); } catch { continue; }
          if (event.type === "agent_settled") finish();
        }
      });
      child.stdin.write(`${JSON.stringify({ type: "prompt", id: "main-fixture", message: "fabric fixture" })}\n`);
    });
    const text = events.trim().split("\n").flatMap(line => {
      try {
        const event = JSON.parse(line);
        return event.type === "message_end" && event.message?.role === "assistant"
          ? event.message.content.filter((part: any) => part.type === "text").map((part: any) => part.text) : [];
      } catch { return []; }
    }).join("\n");
    return { text, events, stderr };
  } finally {
    if (child.exitCode === null && child.signalCode === null) {
      const exited = once(child, "exit");
      child.kill("SIGTERM");
      await exited;
    }
  }
}

describe("built durable Fabric host", () => {
  it.each(["main", "task"] as const)("%s executes TypeScript fabric_exec and enforces task spawn authority", async caller => {
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
    for (const key of ["PI_FABRIC_DEPTH", "PI_FABRIC_BUDGET", "PI_FABRIC_BUDGET_FILE", "PI_FABRIC_BUDGET_ID", "PI_FABRIC_WRITE_POLICY", "PI_FABRIC_TOOL_ALLOWLIST", "PI_FABRIC_SCOPE", "PI_FABRIC_LINEAGE", "PI_FABRIC_PARENT_RUN", "PI_FABRIC_ACTOR_ID", "PI_FABRIC_TASK_PROCESS_CHILD", "PI_FABRIC_AGENT_RUN_DIR", "PI_FABRIC_AGENT_NAME", "PI_FABRIC_SPAWNER_KIND", "SMARTY_ROLE"]) vi.stubEnv(key, undefined);
    vi.stubEnv("PI_CODING_AGENT_DIR", agentDir);
    vi.stubEnv("PI_OFFLINE", "1");
    vi.stubEnv("DURABLE_TEST_FAUX_MODULE", import.meta.resolve("@earendil-works/pi-ai/providers/faux"));
    // The child shares this offline provider's agent-dir profile; isolate
    // authority from the independent write-policy fence on its settings lock.
    vi.stubEnv("DURABLE_TEST_FABRIC_CODE", caller === "main"
      ? 'const child = await agents.run({ task: "recursive leaf", tools: [], extensions: true, thinking: "off", writePolicy: "unrestricted" }); return { runner: child.runner, status: child.status, text: child.text, error: child.error, stderr: child.stderr };'
      : 'try { await agents.run({ task: "forbidden descendant", tools: [], thinking: "off" }); throw new Error("unexpected task spawn"); } catch (error) { if (error.code !== "FABRIC_AGENT_SPAWN_FORBIDDEN" || error.launchOutcome !== "unlaunched") throw error; return { code: error.code, launchOutcome: error.launchOutcome }; }');
    if (caller === "main") {
      const result = await runDurableMain(cwd);
      expect(result.stderr).not.toContain("could not resolve the persisted assistant entry ID");
      expect(result.text, `${result.stderr}\n${result.events}`).toContain("recursive leaf");
      expect(result.text).toContain("pi-durable");
      expect(result.text).toContain("completed");
      expect(result.events).toContain('"toolName":"fabric_exec"');
      return;
    }
    // Fork default runner is "pi"; this probe is the durable host, so select it.
    const manager = new AgentManager(cwd, { ...DEFAULT_FABRIC_CONFIG.agents, runner: "pi-durable", maxDepth: 3, timeoutMs: 30000 }, {
      workerPath: path.resolve("dist/worker.js"), fabricExtensionPath: path.resolve("dist/index.js"),
      fullCodeMode: true, runRoot: path.join(cwd, "runs"), piBinary: path.join(cwd, "forbidden-native-fallback"),
    }); managers.push(manager);
    const result = await manager.run({ task: "fabric fixture", model: "durable-offline/test", recursive: true, kernel: "typescript", transport: "process" });
    expect(result.status, `${result.error}\n${result.stderr}\n${result.text}`).toBe("completed");
    expect(result.runner).toBe("pi-durable");
    expect(result.stderr ?? "").not.toContain("could not resolve the persisted assistant entry ID");
    expect(result.text).toContain("FABRIC_AGENT_SPAWN_FORBIDDEN");
    expect(result.text).toContain("unlaunched");
    expect(manager.list().map(run => run.id)).toEqual([result.id]);
    const events = fs.readFileSync(result.logFile!, "utf8");
    expect(events).toContain('"toolName":"fabric_exec"');
  }, 60000);
});
