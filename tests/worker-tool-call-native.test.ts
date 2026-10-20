import fs from "node:fs";
import { readRunLogText } from "../src/run-log-source.js";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { residentDrainTemp } from "./helpers/resident-drain-temp.js";

const drainFixtures: Array<ReturnType<typeof residentDrainTemp>> = [];
const managers: AgentManager[] = [];
afterEach(async () => {
  const fixtures = drainFixtures.splice(0);
  try {
    const failures: unknown[] = [];
    for (const manager of managers.splice(0).reverse()) {
      try { await manager.close(); } catch (error) { failures.push(error); }
    }
    if (failures.length) throw new AggregateError(failures, "native whitespace proof owner close failed; sandbox retained");
    for (const fixture of fixtures) fixture.assertNoOutsideDeletion();
    for (const fixture of fixtures) fs.rmSync(path.dirname(fixture.root), { recursive: true, force: true });
  } finally {
    vi.unstubAllEnvs();
  }
});
// This is deliberately a compiled-worker proof, not a mock RPC launcher. The
// only synthetic part is provider inference; history never uses a sidecar.
describe("Fabric worker -> native Pi -> whitespace fixture provider", () => {
  it.each(["success", "repeat", "effects"])("recovers an unseeded native task safely (%s)", async mode => {
    const fixture = residentDrainTemp("fabric-native-whitespace-"); drainFixtures.push(fixture);
    const root = fixture.root;
    const workspace = path.join(root, "workspace"); fs.mkdirSync(workspace, { recursive: true, mode: 0o700 });
    const agentDir = path.join(root, "agent"); fs.mkdirSync(path.join(agentDir, "extensions"), { recursive: true });
    fs.writeFileSync(path.join(agentDir, "settings.json"), JSON.stringify({ compaction: { enabled: false }, defaultProjectTrust: "approve" }));
    // Load the checked fixture through ordinary personal resource discovery.
    fs.writeFileSync(path.join(agentDir, "extensions", "provider.ts"), `export { default } from ${JSON.stringify(path.resolve("tests/fixtures/whitespace-native-provider.ts"))};\n`);
    vi.stubEnv("PI_CODING_AGENT_DIR", agentDir); vi.stubEnv("PI_OFFLINE", "1");
    vi.stubEnv("FABRIC_NATIVE_WHITESPACE_LOG", path.join(root, "provider.jsonl"));
    vi.stubEnv("FABRIC_NATIVE_WHITESPACE_ROOT", workspace);
    vi.stubEnv("PI_FABRIC_TOOL_CALL_WHITESPACE_TIMEOUT_MS", "150");
    const childEnvironment = fixture.childEnvironment(process.env.NODE_OPTIONS ?? "");
    for (const key of ["XDG_CACHE_HOME", "XDG_CONFIG_HOME", "XDG_DATA_HOME", "XDG_STATE_HOME"]) {
      const directory = childEnvironment[key];
      if (directory) fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
    }
    for (const [key, value] of Object.entries(childEnvironment)) vi.stubEnv(key, value);
    const manager = new AgentManager(workspace, { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 30_000, nice: 19 }, {
      workerPath: path.resolve("dist/worker.js"), piBinary: path.resolve("node_modules/@earendil-works/pi-coding-agent/dist/cli.js"), runRoot: path.join(root, "runs"), fullCodeMode: false,
      projectRoot: workspace,
    }); managers.push(manager);
    const result = await manager.run({ task: `NATIVE_WHITESPACE_${mode}: retain this original context`, model: "whitespace-native/offline", thinking: "high", transport: "process", extensions: true, tools: ["bash"] });
    const startupAuditFile = path.join(root, "deletion-guard-audit.jsonl");
    expect(fs.existsSync(startupAuditFile), "native worker startup safety diagnostics missing").toBe(true);
    const startupDiagnostics = fs.readFileSync(startupAuditFile, "utf8").trim().split("\n").filter(Boolean)
      .map(line => JSON.parse(line) as { event: string; root?: string });
    const installedGuards = startupDiagnostics.filter(row => row.event === "installed");
    expect(installedGuards.length, "native worker did not report guarded startup").toBeGreaterThan(0);
    expect(installedGuards.every(row => row.root === root), "native worker startup widened its deletion guard").toBe(true);
    const directory = path.join(root, "runs", result.id);
    const rows = (file: string): Array<Record<string, any>> => fs.readFileSync(file, "utf8").trim().split("\n").map(line => JSON.parse(line));
    const providerFile = path.join(root, "provider.jsonl");
    const statusFile = path.join(directory, "status.json");
    const workerStatus = fs.existsSync(statusFile)
      ? (() => {
          try {
            const status = JSON.parse(fs.readFileSync(statusFile, "utf8"));
            return { status: status.status ?? null, errorCode: status.errorCode ?? null, error: status.error ?? null, exitCode: status.exitCode ?? null };
          } catch { return { invalidJson: true }; }
        })()
      : { missing: true };
    expect(fs.existsSync(providerFile),
      `native provider log missing; result=${JSON.stringify({ status: result.status, errorCode: result.errorCode ?? null, error: result.error ?? null })}; workerStatus=${JSON.stringify(workerStatus)}`,
    ).toBe(true);
    const provider = rows(providerFile);
    const events = readRunLogText(path.join(directory, "events.jsonl")).trim().split("\n").map(line => JSON.parse(line));
    const session = rows(path.join(directory, "session.jsonl"));
    const evidence = process.env.FABRIC_WHITESPACE_NATIVE_EVIDENCE_DIR;
    if (evidence) {
      const out = path.join(evidence, mode); fs.mkdirSync(out, { recursive: true });
      fs.writeFileSync(path.join(out, "result.json"), JSON.stringify(result, null, 2));
      fs.copyFileSync(path.join(root, "provider.jsonl"), path.join(out, "provider.jsonl"));
      for (const file of ["status.json", "events.jsonl", "lifecycle.jsonl", "session.jsonl"]) {
        if (file === "events.jsonl") fs.writeFileSync(path.join(out, file), readRunLogText(path.join(directory, file)));
        else fs.copyFileSync(path.join(directory, file), path.join(out, file));
      }
      if (fs.existsSync(path.join(workspace, "effects.txt"))) fs.copyFileSync(path.join(workspace, "effects.txt"), path.join(out, "effects.txt"));
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
      expect(fs.readFileSync(path.join(workspace, "effects.txt"), "utf8")).toBe(mode === "effects" ? "before\nafter\n" : "after\n");
      if (mode === "effects") expect(JSON.stringify(retry.messages)).toContain("BEFORE_STALL");
      expect(provider.filter(row => row.type === "provider-abort")).toHaveLength(1);
    }
    for (const start of starts) expect(() => process.kill(start.pid, 0)).toThrow();
  }, 60_000);
});
