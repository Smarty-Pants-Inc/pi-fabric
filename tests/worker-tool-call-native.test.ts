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
// This is deliberately a compiled-worker proof, not a mock RPC launcher. The
// only synthetic part is provider inference; history never uses a sidecar.
describe("Fabric worker -> native Pi -> whitespace fixture provider", () => {
  it.each(["success", "repeat", "effects"])("recovers an unseeded native task safely (%s)", async mode => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-native-whitespace-")); roots.push(root);
    const agentDir = path.join(root, "agent"); fs.mkdirSync(path.join(agentDir, "extensions"), { recursive: true });
    fs.writeFileSync(path.join(agentDir, "settings.json"), JSON.stringify({ compaction: { enabled: false }, defaultProjectTrust: "approve" }));
    // Load the checked fixture through ordinary personal resource discovery.
    fs.writeFileSync(path.join(agentDir, "extensions", "provider.ts"), `export { default } from ${JSON.stringify(path.resolve("tests/fixtures/whitespace-native-provider.ts"))};\n`);
    vi.stubEnv("PI_CODING_AGENT_DIR", agentDir); vi.stubEnv("HOME", root); vi.stubEnv("PI_OFFLINE", "1");
    vi.stubEnv("FABRIC_NATIVE_WHITESPACE_LOG", path.join(root, "provider.jsonl"));
    vi.stubEnv("FABRIC_NATIVE_WHITESPACE_ROOT", root);
    vi.stubEnv("PI_FABRIC_TOOL_CALL_WHITESPACE_TIMEOUT_MS", "150");
    const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 30_000, nice: 19 }, {
      workerPath: path.resolve("dist/worker.js"), piBinary: path.resolve("node_modules/@earendil-works/pi-coding-agent/dist/cli.js"), runRoot: path.join(root, "runs"), fullCodeMode: false,
    }); managers.push(manager);
    const result = await manager.run({ task: `NATIVE_WHITESPACE_${mode}: retain this original context`, model: "whitespace-native/offline", thinking: "high", transport: "process", extensions: true, tools: ["bash"] });
    const directory = path.join(root, "runs", result.id);
    const rows = (file: string): Array<Record<string, any>> => fs.readFileSync(file, "utf8").trim().split("\n").map(line => JSON.parse(line));
    const provider = rows(path.join(root, "provider.jsonl"));
    const events = rows(path.join(directory, "events.jsonl"));
    const session = rows(path.join(directory, "session.jsonl"));
    const evidence = process.env.FABRIC_WHITESPACE_NATIVE_EVIDENCE_DIR;
    if (evidence) {
      const out = path.join(evidence, mode); fs.mkdirSync(out, { recursive: true });
      fs.writeFileSync(path.join(out, "result.json"), JSON.stringify(result, null, 2));
      fs.copyFileSync(path.join(root, "provider.jsonl"), path.join(out, "provider.jsonl"));
      for (const file of ["status.json", "events.jsonl", "lifecycle.jsonl", "session.jsonl"]) fs.copyFileSync(path.join(directory, file), path.join(out, file));
      if (fs.existsSync(path.join(root, "effects.txt"))) fs.copyFileSync(path.join(root, "effects.txt"), path.join(out, "effects.txt"));
    }
    const starts = provider.filter(row => row.type === "native-start");
    expect(starts).toHaveLength(2);
    expect(starts[0]).toMatchObject({ exists: false, mode: "rpc" });
    expect(starts[1]).toMatchObject({ exists: true, sessionId: starts[0]!.sessionId, sessionFile: starts[0]!.sessionFile });
    expect(starts[1]!.pid).not.toBe(starts[0]!.pid);
    expect(events.filter(row => row.type === "fabric_whitespace_toolcall_retry")).toEqual([expect.objectContaining({ attempt: 1, maxAttempts: 1, model: "whitespace-native/offline", effort: "high" })]);
    const calls = provider.filter(row => row.type === "provider-call");
    expect(calls.every(row => row.model === "whitespace-native/offline" && row.effort === "high")).toBe(true);
    const retry = calls.find(row => row.resumed)!;
    expect(retry).toBeDefined();
    expect(JSON.stringify(retry.messages)).toContain(`NATIVE_WHITESPACE_${mode}`);
    expect(session.filter(row => row.type === "message" && row.message?.role === "user" && JSON.stringify(row.message.content).includes(`NATIVE_WHITESPACE_${mode}`))).toHaveLength(1);
    expect(session.filter(row => row.type === "message" && row.message?.stopReason === "aborted").length).toBeGreaterThanOrEqual(1);
    const stalls = events.filter(row => row.type === "stall.whitespace-toolcall");
    expect(stalls).toHaveLength(mode === "repeat" ? 2 : 1);
    expect(stalls.every(row => row.elapsedMs >= 150 && row.bytes > 0)).toBe(true);
    if (mode === "repeat") {
      expect(result).toMatchObject({ status: "failed", errorCode: "RUNAWAY_TOOL_CALL_STREAM" });
      expect(result.error).toContain("repeated after one same-model retry");
      expect(provider.filter(row => row.type === "native-tool-call")).toHaveLength(0);
    } else {
      expect(result.status, result.error).toBe("completed");
      expect(result.text).toContain("NATIVE_RETRY_COMPLETED");
      expect(provider.filter(row => row.type === "native-tool-call")).toHaveLength(mode === "effects" ? 2 : 1);
      expect(fs.readFileSync(path.join(root, "effects.txt"), "utf8")).toBe(mode === "effects" ? "before\nafter\n" : "after\n");
      if (mode === "effects") expect(JSON.stringify(retry.messages)).toContain("BEFORE_STALL");
      expect(provider.filter(row => row.type === "provider-abort")).toHaveLength(1);
    }
    for (const start of starts) expect(() => process.kill(start.pid, 0)).toThrow();
  }, 60_000);
});
