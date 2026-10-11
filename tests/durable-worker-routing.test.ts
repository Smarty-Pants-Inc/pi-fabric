import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { spawn } from "node:child_process";
import { build } from "esbuild";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { BUILT_IN_RUNNER_IDS, requireAgentRunner, registerAgentRunner } from "../src/agents/runner-registry.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { parseWorkerOptions } from "../src/worker/options.js";

const roots: string[] = [];
const managers: AgentManager[] = [];
const temporary = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-durable-routing-"));
  roots.push(root);
  return root;
};
const argv = (root: string, overrides: Record<string, string> = {}) => [
  process.execPath, "worker.js",
  ...Object.entries({
    id: "durable-probe", runner: "pi-durable", name: "routing probe",
    "task-file": path.join(root, "task.txt"), "status-file": path.join(root, "status.json"),
    "lifecycle-file": path.join(root, "lifecycle.jsonl"), "log-file": path.join(root, "events.jsonl"),
    cwd: root, "pi-binary": path.resolve("tests/fixtures/fake-pi-model.mjs"),
    "claude-binary": "unused", "veda-binary": "unused", "veda-backend": "unused", "veda-persona": "unused",
    "timeout-ms": "5000", depth: "1", "full-code-mode": "true", extensions: "true",
    tools: "[]", "granted-risks": "[]", transport: "process", model: "openai-codex/gpt-5.6-sol", thinking: "high",
    ...overrides,
  }).flatMap(([key, value]) => [`--${key}`, value]),
];
afterEach(async () => {
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  vi.unstubAllEnvs();
});
afterAll(() => {
  for (const root of roots) fs.rmSync(root, { recursive: true, force: true });
});

describe("durable Pi worker routing", () => {
  it("is a protected built-in process runner with exactly Pi capabilities", async () => {
    const durable = requireAgentRunner("pi-durable");
    expect(BUILT_IN_RUNNER_IDS.has(durable.id)).toBe(true);
    expect(durable.capabilities).toEqual(requireAgentRunner("pi").capabilities);
    expect(durable.kind).toBe("worker");
    expect(() => registerAgentRunner(durable)).toThrow("Cannot replace built-in");
    if (durable.kind !== "worker") throw new Error("expected worker");
    const fabricWorker = { workerPath: "/fabric/worker.js", workerArguments: ["--runner", "pi-durable"] };
    const launch = await durable.launch({
      fabricWorker, id: "probe", name: "probe", task: "probe", cwd: "/work", runDirectory: "/run",
      residency: "session", deadlineAt: Date.now() + 5000, depth: 1, tools: [],
      lineage: { version: 1, rootSessionId: "root", runId: "probe", depth: 1, childIndex: 0, worker: true },
      files: { taskFile: "task", statusFile: "status", lifecycleFile: "lifecycle", logFile: "log", steerFile: "steer" },
    });
    expect(launch).toEqual(fabricWorker);
    expect(launch.workerArguments).not.toBe(fabricWorker.workerArguments);
  });

  it.each(["pi", "pi-durable"])("parses Pi kernels and extension context for %s", runner => {
    const root = temporary();
    expect(parseWorkerOptions(argv(root, { runner, kernel: "python", "python-runtime": "monty", "child-questions": "1000" })))
      .toMatchObject({ runner, kernel: "python", pythonRuntime: "monty", childQuestionTimeoutMs: 1000 });
    expect(parseWorkerOptions(argv(root, { runner }))).toMatchObject({ kernel: "typescript" });
    expect(parseWorkerOptions(argv(root, { runner, extensions: "false" })).kernel).toBeUndefined();
    expect(() => parseWorkerOptions(argv(root, { runner, extensions: "false", kernel: "typescript" })))
      .toThrow("Fabric extensions enabled");
  });

  it("prepares the Pi default model and forwards recursive/actor/confinement/session context", async () => {
    const root = temporary();
    const capture = path.join(root, "capture.mjs");
    fs.writeFileSync(capture, `import fs from 'node:fs';\nfs.writeFileSync(${JSON.stringify(path.join(root, "argv.json"))}, JSON.stringify(process.argv));\nawait import(${JSON.stringify(pathToFileURL(path.resolve("tests/fixtures/fake-worker.mjs")).href)});`);
    const preparePiModel = vi.fn(async () => "provider/fallback");
    const manager = new AgentManager(root, {
      ...DEFAULT_FABRIC_CONFIG.agents, runner: "pi-durable", model: "provider/default", timeoutMs: 5000,
      childQuestions: "route", sessionExport: true, sessionExportDir: path.join(root, "exports"),
    }, {
      workerPath: capture, runRoot: path.join(root, "runs"), preparePiModel, fullCodeMode: true,
      resolveInheritedSessionPins: () => [{ pool: "parent-pool" }],
    });
    managers.push(manager);
    const result = await manager.run({ task: "probe", runner: "pi-durable", transport: "process", recursive: true,
      extensions: true, kernel: "typescript", readOnly: true, actorId: "actor-probe", actorName: "Probe",
      sessionFile: path.join(root, "actor.jsonl"),
    });
    expect(result.status, result.error).toBe("completed");
    expect(preparePiModel).toHaveBeenCalledWith("provider/default");
    const options = parseWorkerOptions(JSON.parse(fs.readFileSync(path.join(root, "argv.json"), "utf8")));
    expect(options).toMatchObject({ runner: "pi-durable", model: "provider/fallback", extensions: true,
      kernel: "typescript", fullCodeMode: true, actorId: "actor-probe", sessionFile: path.join(root, "actor.jsonl"),
      inheritedSessionPins: [{ pool: "parent-pool" }],
    });
    expect(options.childQuestionTimeoutMs).toBeGreaterThanOrEqual(1000);
    expect(options.sessionExportFile).toContain(path.join(root, "exports"));
    expect(JSON.parse(options.writePolicy!)).toMatchObject({ readOnly: true });
    expect(options.grantedRisks).toContain("agent");
    expect(options.fabricExtensionPath).toBeTruthy();
  });

  it("rejects unavailable durable Pi models before launching a worker", async () => {
    const root = temporary();
    const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, runner: "pi-durable" }, {
      runRoot: path.join(root, "runs"), preparePiModel: async () => { throw new Error("model unavailable"); },
    });
    managers.push(manager);
    await expect(manager.run({ task: "never launch", model: "provider/missing", transport: "process" }))
      .rejects.toThrow("model unavailable");
    expect(manager.listForUi()).toHaveLength(0);
  });
});

// Compile only the test subject into a temporary directory, never dist/. The
// sibling durable entry is a protocol double: this tests Fabric routing and RPC
// behavior without a provider request or coupling to the durable engine internals.
describe("real worker durable process protocol", () => {
  let fixture: string;
  let worker: string;
  beforeAll(async () => {
    fixture = temporary();
    worker = path.join(fixture, "worker.mjs");
    fs.symlinkSync(path.resolve("node_modules"), path.join(fixture, "node_modules"), process.platform === "win32" ? "junction" : "dir");
    await build({ entryPoints: [path.resolve("src/worker.ts")], outfile: worker, bundle: true, platform: "node", format: "esm", packages: "external", logLevel: "silent" });
    fs.mkdirSync(path.join(fixture, "durable"));
    fs.writeFileSync(path.join(fixture, "durable", "worker.js"), `
      process.stdout.write(JSON.stringify({type: 'launch_probe', execPath: process.execPath, argv: process.argv.slice(2), kernel: process.env.PI_FABRIC_KERNEL}) + '\\n');
      if (process.env.DURABLE_ROUTING_QUESTION === '1') {
        await import(${JSON.stringify(pathToFileURL(path.resolve("tests/fixtures/fake-pi-rpc-question.mjs")).href)});
      } else {
        await import(${JSON.stringify(pathToFileURL(path.resolve("tests/fixtures/fake-pi-model.mjs")).href)});
      }
    `);
  });

  const run = async (runner = "pi-durable", scenario = "success") => {
    const root = temporary();
    fs.writeFileSync(path.join(root, "task.txt"), "run on requested model");
    fs.writeFileSync(path.join(root, "scenario"), scenario);
    const args = argv(root, { runner, kernel: "typescript", "write-policy": JSON.stringify({ readOnly: true }),
      "session-file": path.join(root, "seed.jsonl"), "session-export-file": path.join(root, "export.jsonl") });
    const child = spawn(process.execPath, [worker, ...args.slice(2)], {
      env: { ...process.env, FAKE_MODEL_SCENARIO: path.join(root, "scenario") }, stdio: ["ignore", "pipe", "pipe"],
    });
    let stderr = "";
    child.stdout.resume();
    child.stderr.on("data", chunk => { stderr += chunk; });
    await new Promise<void>((resolve, reject) => { child.once("error", reject); child.once("close", () => resolve()); });
    expect(fs.existsSync(path.join(root, "status.json")), stderr).toBe(true);
    const result = JSON.parse(fs.readFileSync(path.join(root, "status.json"), "utf8"));
    const events = fs.existsSync(path.join(root, "events.jsonl"))
      ? fs.readFileSync(path.join(root, "events.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line)) : [];
    return { root, result, events, frames: events.filter(event => event.type === "fake_received").map(event => event.frame) };
  };

  it("launches the durable sibling through the current runtime and preserves native Pi flags and admission", async () => {
    const { root, result, events, frames } = await run();
    expect(result.status, result.error).toBe("completed");
    const launch = events.find(event => event.type === "launch_probe");
    expect(launch).toMatchObject({ execPath: process.execPath, kernel: "typescript" });
    expect(launch.argv).toEqual(expect.arrayContaining([
      "--mode", "rpc", "--durable-run-id", "durable-probe", "--durable-directory", path.join(root, "durable"),
      "--session", path.join(root, "seed.jsonl"), "--no-tools", "--model", "openai-codex/gpt-5.6-sol", "--thinking", "high",
      "-e", path.join(fs.realpathSync(fixture), "agents", "write-guard.js"),
    ]));
    expect(frames.map(frame => frame.type)).toEqual(["get_state", "set_model", "set_thinking_level", "get_state", "prompt"]);
    expect(result.text).toBe("correct model ran");
    const exported = fs.readFileSync(path.join(root, "export.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line));
    expect(exported.at(-1)).toMatchObject({ type: "message", message: {
      role: "assistant", provider: "openai-codex", model: "gpt-5.6-sol", usage: { input: 2, output: 3 },
    } });
  });

  it("routes durable child questions through manager control and back to native RPC", async () => {
    const root = temporary();
    const answerFile = path.join(root, "answers.jsonl");
    vi.stubEnv("DURABLE_ROUTING_QUESTION", "1");
    vi.stubEnv("FAKE_PI_QUESTION_LOG", answerFile);
    const onChildQuestion = vi.fn(async () => ({ value: "B" }));
    const manager = new AgentManager(root, {
      ...DEFAULT_FABRIC_CONFIG.agents, runner: "pi-durable", model: "", timeoutMs: 5000,
      childQuestions: "route", childQuestionTimeoutMs: 1000, sessionExport: false,
    }, { workerPath: worker, runRoot: path.join(root, "runs"), onChildQuestion });
    managers.push(manager);
    const result = await manager.run({ task: "ask", transport: "process" });
    expect(result.status, result.error).toBe("completed");
    expect(onChildQuestion).toHaveBeenCalledOnce();
    expect(JSON.parse(fs.readFileSync(answerFile, "utf8"))).toEqual({
      type: "extension_ui_response", id: "ui-1", value: "B",
    });
    expect(result.blockedOn).toBeUndefined();
  });

  it("keeps explicit pi on piBinary, not the durable entry", async () => {
    const { result, events, frames } = await run("pi");
    expect(result.status, result.error).toBe("completed");
    expect(events.some(event => event.type === "launch_probe")).toBe(false);
    expect(frames.at(-1).type).toBe("prompt");
  });

  it.each(["reswitch", "reject", "exit"])("never prompts before durable model admission: %s", async scenario => {
    const { result, frames } = await run("pi-durable", scenario);
    expect(result.status).toBe("failed");
    expect(result.error).toMatch(/model|admission/);
    expect(frames.some(frame => frame.type === "prompt")).toBe(false);
  });

  it("still terminates on durable assistant model drift", async () => {
    const { result } = await run("pi-durable", "drift");
    expect(result.status).toBe("failed");
    expect(result.error).toContain("terminating child");
  });
});
