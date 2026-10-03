import fs from "node:fs";
import { execFileSync } from "node:child_process";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { decideModelRoute } from "../src/agents/model-route.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";

const workerPath = path.resolve("dist/worker.js");
const roots: string[] = [];
const managers: AgentManager[] = [];
afterEach(async () => {
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
describe.skipIf(!fs.existsSync(workerPath))("shadow routing in real built worker", () => {
  const run = async (scenario: string, route = true) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-route-worker-")); roots.push(root);
    const scenarioFile = path.join(root, "scenario"); fs.writeFileSync(scenarioFile, scenario);
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(root, "agent"));
    vi.stubEnv("FAKE_MODEL_SCENARIO", scenarioFile);
    vi.stubEnv("PI_FABRIC_ROUTE_HEADER", "parent-attribution-must-not-leak");
    const pin = { model: "openai-codex/gpt-5.6-sol", effort: "high" as const };
    const decision = await decideModelRoute({ routeClass: "bounded-lookup", protected: false, pin,
      candidates: [{ model: "test/luna", effort: "medium" }], parentSessionId: "parent" }, async () => ({ model: "jev", answers: {
      route: { type: "choice", choice: "candidate-1", confidence: .95, probabilities: { "candidate-0": .05, "candidate-1": .95 } },
    }, usage: { input_tokens: 1, output_tokens: 1 } }));
    const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 5000, retainRuns: true }, {
      workerPath, piBinary: path.resolve("tests/fixtures/fake-pi-route.mjs"), runRoot: path.join(root, "runs"),
    }); managers.push(manager);
    const result = await manager.run({ task: "harmless lookup", model: pin.model, thinking: pin.effort,
      transport: "process", extensions: false, ...(route ? { routeDecision: decision } : {}) });
    const events = fs.readFileSync(path.join(root,"runs",result.id,"events.jsonl"),"utf8").trim().split("\n").map(line => JSON.parse(line));
    const launch = events.find(event => event.type === "fake_route_launch");
    const rows = route ? fs.readFileSync(path.join(root,"agent/fabric/model-routing.jsonl"),"utf8").trim().split("\n").map(line => JSON.parse(line)) : [];
    return { result, rows, launch, events, pin };
  };
  it("R3 cleanup joins a process that publishes its terminal result before exiting", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-route-exit-")); roots.push(root);
    const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: false }, {
      workerPath: path.resolve("tests/fixtures/terminal-before-exit-worker.mjs"), runRoot: path.join(root, "runs"),
    }); managers.push(manager);
    const result = await manager.run({ task: "terminal-before-exit", transport: "process", extensions: false });
    expect(result.status).toBe("completed");
    const pid = Number(result.sessionId);
    expect(Number.isSafeInteger(pid)).toBe(true);
    expect(() => process.kill(pid, 0)).not.toThrow();
    const directory = manager.runDirectory(result.id)!;
    expect(fs.existsSync(directory)).toBe(true);
    await manager.cleanup(result.id);
    expect(() => process.kill(pid, 0)).toThrow(expect.objectContaining({ code: "ESRCH" }));
    expect(fs.existsSync(directory)).toBe(false);
  });

  it("R3 real-Pi routed worktree uses native tool cwd and committed worktree contents, not parent edits", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-route-real-cwd-")); roots.push(root);
    const git = (...args: string[]) => execFileSync("git", args, { cwd: root, stdio: "pipe" });
    git("init");
    fs.writeFileSync(path.join(root, "route-cwd.txt"), "worktree-only-content");
    git("add", "route-cwd.txt");
    git("-c", "user.name=Offline Test", "-c", "user.email=offline@example.invalid", "commit", "-m", "fixture");
    fs.writeFileSync(path.join(root, "route-cwd.txt"), "parent-only-content");
    const agentDir = path.join(root, "agent"); fs.mkdirSync(agentDir, { mode: 0o700 });
    fs.writeFileSync(path.join(agentDir, "settings.json"), JSON.stringify({ enableInstallTelemetry: false, extensions: [path.resolve("tests/fixtures/route-cwd-extension.ts")] }));
    vi.stubEnv("PI_CODING_AGENT_DIR", agentDir); vi.stubEnv("PI_OFFLINE", "1");
    vi.stubEnv("PI_FABRIC_EXTENSION_PATH", path.resolve("tests/fixtures/route-cwd-extension.ts"));
    const pin = { model: "route-cwd-probe/pinned", effort: "high" as const };
    const decision = await decideModelRoute({ routeClass: "bounded-lookup", protected: true, pin, candidates: [], parentSessionId: "parent" }, async () => { throw new Error("excluded"); });
    const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: true, timeoutMs: 30000 }, {
      workerPath, piBinary: path.resolve("node_modules/@earendil-works/pi-coding-agent/dist/cli.js"),
      fabricExtensionPath: path.resolve("tests/fixtures/route-cwd-extension.ts"), fullCodeMode: false, runRoot: path.join(root, "runs"),
    }); managers.push(manager);
    let id: string | undefined;
    try {
      const handle = await manager.spawn({ task: "Run the bounded cwd probe", worktree: true, routeDecision: decision, extensions: true, tools: ["bash"] }); id = handle.id;
      const result = await manager.wait(handle.id);
      expect(result, `${result.error}\n${result.stderr}`).toMatchObject({ status: "completed", model: pin.model, admittedThinking: pin.effort, toolCalls: 1 });
      const toolOutput = JSON.parse(result.text) as Array<{ type: string; text: string }>;
      expect(toolOutput.map(part => part.text).join("\n")).toContain(result.cwd);
      expect(result.text).toContain("worktree-only-content");
      expect(result.text).not.toContain("parent-only-content");
      expect(fs.readFileSync(path.join(root, "route-cwd.txt"), "utf8")).toBe("parent-only-content");
      const rows = fs.readFileSync(path.join(agentDir, "fabric/model-routing.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line));
      expect(rows[0].childSessionId).toBe(handle.id);
      expect(rows[1]).toMatchObject({ status: "completed", admittedModel: pin.model, admittedEffort: pin.effort });
    } finally { if (id) await manager.cleanup(id, true); }
  }, 45000);
  it.each(["native", "win32"])("loads attribution hook even when extensions are disabled and records verified admission (%s paths)", async pathStyle => {
    const { result, rows, launch, pin } = await run("success");
    // Exercise Windows argv separators on every host, using the real worker's hook path.
    const argv: string[] = pathStyle === "win32" ? launch.argv.map((arg: string) => path.win32.normalize(arg)) : launch.argv;
    expect(result).toMatchObject({ status: "completed", model: pin.model, thinking: pin.effort, admittedModel: pin.model, admittedThinking: pin.effort });
    expect(launch.argv).toContain("--no-extensions");
    expect(argv.some(arg => arg.replaceAll("\\", "/").endsWith("/guards/model-route-hook.js"))).toBe(true);
    expect(launch.header).toContain("bounded-lookup/test%2Fluna-medium/shadow-choice:");
    expect(rows[1]).toMatchObject({ admittedModel: pin.model, admittedEffort: pin.effort, status: "completed", tokens: { input: 2, output: 3 } });
  });
  it("does not claim an admitted pin when Pi rejects model selection before prompt", async () => {
    const { result, rows, events } = await run("reject");
    expect(result.status).toBe("failed");
    expect(events.filter(event => event.type === "fake_received").some(event => event.frame.type === "prompt")).toBe(false);
    expect(rows[1]).toMatchObject({ status: "failed", admittedModel: null, admittedEffort: null });
  });
  it.each(["effort-lower", "effort-off", "effort-missing", "effort-malformed"])("R3 rejects %s readback before prompt without claiming admission", async scenario => {
    const { result, rows, events } = await run(scenario);
    expect(result.status).toBe("failed");
    expect(result.error).toContain("MODEL_ROUTE_PIN_MISMATCH");
    expect(result.admittedModel).toBeUndefined();
    expect(result.admittedThinking).toBeUndefined();
    expect(events.filter(event => event.type === "fake_received").some(event => event.frame.type === "prompt")).toBe(false);
    expect(rows[1]).toMatchObject({ status: "failed", admittedModel: null, admittedEffort: null });
  });
  it("R3 preserves ordinary non-auto effort clamping", async () => {
    const { result } = await run("effort-lower", false);
    expect(result).toMatchObject({ status: "completed", thinking: "low" });
  });
  it("clears parent route metadata for unrelated explicit-model tasks", async () => {
    const { result, launch } = await run("success", false);
    expect(result.status).toBe("completed");
    expect(launch.header).toBeNull();
    expect(launch.argv.some((arg: string) => arg.replaceAll("\\", "/").endsWith("/guards/model-route-hook.js"))).toBe(false);
  });
});
