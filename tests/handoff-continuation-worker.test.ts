import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { SessionManager } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { snapshotHandoffSession } from "../src/agents/handoff.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { rmTempSync } from "./fixtures/temp-cleanup.js";

const workerPath = path.resolve("dist/worker.js");

describe.skipIf(!fs.existsSync(workerPath))("real trajectory executor continuation", () => {
  let directory: string | undefined;
  let manager: AgentManager | undefined;
  afterEach(async () => {
    await manager?.close();
    vi.unstubAllEnvs();
    if (directory) rmTempSync(directory);
  });

  it.each(["pi", "pi-durable"] as const)("%s finishes its original work after a nested depth rejection instead of returning failure text (offline)", async (runner) => {
    directory = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-handoff-continuation-"));
    const agentDir = path.join(directory, "agent");
    fs.mkdirSync(agentDir);
    fs.writeFileSync(path.join(directory, "partial.txt"), "already done");
    fs.writeFileSync(path.join(agentDir, "settings.json"), JSON.stringify({
      extensions: [path.resolve("tests/fixtures/handoff-continuation-extension.ts")],
      enableInstallTelemetry: false,
    }));
    fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({
      fullCodeMode: true,
      mcp: { enabled: false },
      prewalk: { mode: "trajectory", model: "handoff-probe/executor", alwaysRearm: true },
      agents: { runner, maxDepth: 1, timeoutMs: 30_000 },
      mesh: { enabled: false },
    }));
    vi.stubEnv("PI_CODING_AGENT_DIR", agentDir);
    vi.stubEnv("PI_OFFLINE", "1");
    vi.stubEnv("PI_FABRIC_DEPTH", "0");
    vi.stubEnv("PI_FABRIC_PARENT_RUN", undefined);
    vi.stubEnv("PI_FABRIC_ACTOR_ID", undefined);
    const source = SessionManager.inMemory(directory);
    source.appendMessage({ role: "user", content: "Finish the implementation and verify it", timestamp: 1 });
    source.appendMessage({
      role: "assistant", content: [{ type: "toolCall", id: "frontier", name: "fabric_exec", arguments: { code: 'return "partial work"' } }],
      api: "handoff-probe-api", provider: "handoff-probe", model: "executor", stopReason: "toolUse", timestamp: 2,
      usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0,
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
    });
    const seed = snapshotHandoffSession(source, { provider: "handoff-probe", id: "executor" }, {
      role: "toolResult", toolCallId: "frontier", toolName: "fabric_exec", content: [{ type: "text", text: "partial work" }],
      isError: false, timestamp: 3,
    }, "frontier");
    manager = new AgentManager(directory, { ...DEFAULT_FABRIC_CONFIG.agents, runner, maxDepth: 1, timeoutMs: 30_000 }, {
      workerPath,
      piBinary: runner === "pi"
        ? path.resolve("node_modules/@earendil-works/pi-coding-agent/dist/cli.js")
        : path.join(directory, "forbidden-native-pi"),
      fullCodeMode: true, fabricExtensionPath: path.resolve("dist/index.js"), runRoot: path.join(directory, "runs"),
    });
    const result = await manager.run({
      task: "Finish the implementation and verify it", model: "handoff-probe/executor", kernel: "typescript",
      recursive: true, extensions: true, transport: "process", sessionSeed: seed,
    });
    const log = fs.readFileSync(path.join(directory, "runs", result.id, "events.jsonl"), "utf8");
    expect(result, `${result.error}\n${result.stderr}\n${log.slice(-12_000)}`).toMatchObject({
      status: "completed", turns: 3, toolCalls: 2,
      text: "Finished original assignment directly after the failed handoff; verification passed.",
    });
    expect(fs.readFileSync(path.join(directory, "continued.txt"), "utf8")).toBe("continued directly");
    expect(fs.readFileSync(path.join(directory, "partial.txt"), "utf8")).toBe("already done");
    const events = log.trim().split("\n").map(line => JSON.parse(line));
    const continuation = events.filter(event => event.type === "message_end" &&
      event.message?.role === "custom" && event.message.customType === "pi-fabric-handoff-continuation");
    expect(continuation).toHaveLength(1);
    expect(log).toContain("depth limit");
    expect(events.filter(event => event.type === "tool_execution_start" && event.toolName === "fabric_exec")).toHaveLength(2);
  }, 45_000);
});
