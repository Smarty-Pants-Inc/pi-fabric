import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import { pathToFileURL } from "node:url";
import { build } from "esbuild";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ActorManager } from "../src/actors/manager.js";
import { MeshStore } from "../src/mesh/store.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { DEFAULT_FABRIC_CONFIG, type FabricProcessLaunchConfig } from "../src/config.js";
import { parseWorkerOptions } from "../src/worker/options.js";
import type { AgentTransportLaunch } from "../src/agents/types.js";
import type { AgentPlacementConfig } from "../src/agents/placement-config.js";
import { WORKER_PROTOCOL_VERSION } from "../src/agents/worker-protocol.js";

// Keep reason: trusted host policy crosses the native process/env and Pi code-loading
// boundaries. Only the model/RPC responder is fake; manager, transport, worker,
// native argv/environment and Pi's real argument parser/resource loader execute.
const workerPath = path.resolve(process.env.FABRIC_PROCESS_LAUNCH_TEST_WORKER ?? "src/worker.ts");
const sdkDirectory = fs.realpathSync(path.resolve("node_modules/@earendil-works/pi-coding-agent/dist"));
const managers: AgentManager[] = [];
const actorManagers: ActorManager[] = [];
let root: string;
beforeEach(() => {
  root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-process-launch-hooks-")));
  fs.mkdirSync(path.join(root, "cwd"));
  const profile = path.join(root, "profile");
  fs.mkdirSync(profile);
  fs.writeFileSync(path.join(profile, "settings.json"), JSON.stringify({ extensions: [], packages: [] }));
  vi.stubEnv("PI_CODING_AGENT_DIR", profile);
  vi.stubEnv("PI_OFFLINE", "1");
  // These tests prove the local process seam, not the machine's user-manager setup.
  vi.stubEnv("XDG_RUNTIME_DIR", root);
  vi.stubEnv("DBUS_SESSION_BUS_ADDRESS", undefined);
  vi.stubEnv("TMPDIR", root);
});
afterEach(async () => {
  let closed = false;
  try {
    await Promise.all(actorManagers.splice(0).map(manager => manager.close()))
      .finally(() => Promise.all(managers.splice(0).map(manager => manager.close())));
    closed = true;
  } finally {
    vi.restoreAllMocks();
    vi.unstubAllEnvs();
    // This exact directory was created by this test. No profile, inherited temp
    // root, run root outside it, or other session's fixture is ever removed.
    // Failed custody cleanup retains the fixture instead of deleting live files.
    if (closed) await fs.promises.rm(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
  }
});

const fakePi = () => {
  const binary = path.join(root, "pi-probe.mjs");
  fs.writeFileSync(binary, `
import fs from 'node:fs';
const { parseArgs } = await import(${JSON.stringify(pathToFileURL(path.join(sdkDirectory, "cli/args.js")).href)});
const { DefaultResourceLoader, SettingsManager } = await import(${JSON.stringify(pathToFileURL(path.join(sdkDirectory, "index.js")).href)});
const parsed = parseArgs(process.argv.slice(2));
const resources = new DefaultResourceLoader({
  cwd: process.cwd(), agentDir: process.env.PI_CODING_AGENT_DIR,
  settingsManager: SettingsManager.inMemory({ extensions: [], packages: [] }),
  additionalExtensionPaths: parsed.extensions, noExtensions: parsed.noExtensions,
  noSkills: true, noPromptTemplates: true, noThemes: true, noContextFiles: true,
});
await resources.reload();
if (resources.getExtensions().errors.length) throw new Error(JSON.stringify(resources.getExtensions().errors));
const keys = ['TMPDIR', 'TMP', 'TEMP', 'LAUNCH_HOOK_SEEN', 'LAUNCH_LITERAL', 'LAUNCH_FROM_HOOK',
  'LAUNCH_TEMP', 'LAUNCH_RAW', 'LAUNCH_EMPTY', 'LAUNCH_ORDER', 'CARRIER_VALUE', 'SMARTY_ROLE', 'PI_FABRIC_AGENT_NAME'];
const capture = { argv: process.argv.slice(2), env: Object.fromEntries(keys.map(key => [key, process.env[key] ?? null])),
  loaded: resources.getExtensions().extensions.map(extension => extension.path) };
fs.writeFileSync(${JSON.stringify(path.join(root, "pi-capture.json"))}, JSON.stringify(capture));
let model = { provider: 'launch-test', id: 'offline' };
const emit = event => process.stdout.write(JSON.stringify(event) + '\\n');
let buffer = '';
process.stdin.on('data', chunk => {
  buffer += chunk;
  let newline;
  while ((newline = buffer.indexOf('\\n')) >= 0) {
    const frame = JSON.parse(buffer.slice(0, newline)); buffer = buffer.slice(newline + 1);
    const reply = data => emit({type: 'response', id: frame.id, command: frame.type, success: true, data});
    if (frame.type === 'get_state') reply({model, thinkingLevel: 'off', isStreaming: false, isCompacting: false});
    else if (frame.type === 'set_model') { model = {provider: frame.provider, id: frame.modelId}; reply(model); }
    else if (frame.type === 'set_thinking_level') reply();
    else if (frame.type === 'prompt') {
      emit({type: 'agent_start'});
      emit({type: 'message_end', message: {role: 'assistant', provider: model.provider, model: model.id,
        content: 'launch hook observed', stopReason: 'stop'}});
      emit({type: 'agent_end'}); emit({type: 'agent_settled'});
    }
  }
});
process.stdin.on('end', () => process.exit(0));
`);
  return binary;
};
const hook = (name: string) => {
  const file = path.join(root, name + ".mjs");
  fs.writeFileSync(file, `export default function () {
    process.env.LAUNCH_HOOK_SEEN = 'loaded';
    process.env.LAUNCH_ORDER = (process.env.LAUNCH_ORDER ?? '') + ${JSON.stringify(name)};
  }`);
  return file;
};
const manager = (configuration?: FabricProcessLaunchConfig) => {
  const value = new AgentManager(path.join(root, "cwd"), {
    ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 15_000, retainRuns: true, sessionExport: false,
    ...(configuration ? { process: configuration } : {}),
  }, { workerPath, piBinary: fakePi(), runRoot: path.join(root, "runs"), fullCodeMode: false,
    mainAgentId: "session:launch-main", fabricSessionId: "launch-main" });
  managers.push(value);
  return value;
};
const capture = () => JSON.parse(fs.readFileSync(path.join(root, "pi-capture.json"), "utf8")) as {
  argv: string[]; env: Record<string, string | null>; loaded: string[];
};

// Native worker-boundary probes complement the real Pi-child flows above. The
// carrier callback remains real and composes with the configured overrides.
const direct = (runner = "pi", actor = false) => {
  const worker = path.join(root, "capture-worker.mjs");
  const output = path.join(root, "worker-capture.json");
  fs.writeFileSync(worker, `import fs from 'node:fs';
fs.writeFileSync(${JSON.stringify(output)}, JSON.stringify({argv: process.argv.slice(2),
  env: Object.fromEntries(Object.entries(process.env).sort(([a], [b]) => a.localeCompare(b)))}));`);
  const request: AgentTransportLaunch = { id: "probe", name: "probe", cwd: root,
    workerPath: worker, workerArguments: ["--runner", runner, "--extensions", "false", ...(actor ? ["--actor-id", "actor-probe"] : [])] };
  return { request, output };
};
const launch = async (request: AgentTransportLaunch, configuration?: FabricProcessLaunchConfig, placement?: AgentPlacementConfig) => {
  const handle = await new ProcessTransport(undefined, placement, undefined, undefined, configuration).launch(request);
  try { await handle.waitForClose?.(); }
  finally { await handle.stop(); }
};

describe("host-configured Pi process launch hooks", () => {
  it.each([false, true])("loads actual configured Pi -e hooks with extensions:false (actor=%s)", async actor => {
    const first = hook("first");
    const second = hook("second");
    const value = manager({ extensions: [first, second], env: { LAUNCH_LITERAL: "host policy", LAUNCH_TEMP: "${env:TMPDIR}/store" } });
    const result = await value.run({ task: "observe launch", transport: "process", extensions: false,
      ...(actor ? { actorId: "actor-probe", actorName: "Probe", sessionFile: path.join(root, "actor.jsonl") } : {}),
    });
    expect(result.status, result.error).toBe("completed");
    const observed = capture();
    expect(observed.argv).toContain("--no-extensions");
    const explicit = observed.argv.flatMap((arg, index) => arg === "-e" ? [observed.argv[index + 1]] : []);
    expect(explicit.indexOf(first)).toBeGreaterThanOrEqual(0);
    expect(explicit.indexOf(second)).toBe(explicit.indexOf(first) + 1);
    expect(observed.loaded).toEqual(expect.arrayContaining([first, second]));
    expect(observed.env).toMatchObject({ LAUNCH_HOOK_SEEN: "loaded", LAUNCH_ORDER: "firstsecond", LAUNCH_LITERAL: "host policy" });
    expect(observed.env.LAUNCH_TEMP).toBe(observed.env.TMPDIR + "/store");
    expect(observed.env.TMPDIR).toBe(actor ? root : path.join(value.runDirectory(result.id)!, "tmp"));
  }, 25_000);

  it("carries host hooks through ActorManager create/ask to its actual Pi process", async () => {
    const extension = hook("actor-manager");
    const value = manager({ extensions: [extension], env: { LAUNCH_LITERAL: "actor host hook", LAUNCH_TEMP: "${env:TMPDIR}" } });
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const actors = new ActorManager("launch-main", {
      id: "session:launch-main", name: "Main", kind: "main", sessionId: "launch-main",
    }, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, value, () => {}, {
      persistent: true, actorRoot: path.join(root, "actors"), rootId: "session:launch-main",
    });
    actorManagers.push(actors);
    const actor = await actors.create({ name: "launch-probe", instructions: "Reply.", runner: "pi",
      extensions: false, tools: [], responseMode: "text", transport: "process" });
    const reply = await actors.ask(actor.id, "observe the launch", undefined, AbortSignal.timeout(20_000));
    expect(reply.error).toBeUndefined();
    expect(reply.text).toBe("launch hook observed");
    const observed = capture();
    expect(observed.loaded).toContain(extension);
    expect(observed.argv).toContain("--no-extensions");
    expect(observed.env).toMatchObject({ LAUNCH_HOOK_SEEN: "loaded", LAUNCH_LITERAL: "actor host hook",
      LAUNCH_TEMP: root, TMPDIR: root });
  }, 25_000);

  it.each([false, true])("loads mandatory hooks in the real pi-durable SDK child (actor=%s)", async actor => {
    // The durable host requires its pinned Node SDK, so compile only this test's
    // worker entries into its own fixture; never mutate dist or shared deps.
    fs.symlinkSync(path.resolve("node_modules"), path.join(root, "node_modules"), process.platform === "win32" ? "junction" : "dir");
    fs.writeFileSync(path.join(root, "package.json"), '{"type":"module"}');
    await build({
      entryPoints: ["src/worker.ts", "src/durable/worker.ts", "src/agents/write-guard.ts", "src/guards/sender-headers.ts",
        "src/guards/actor-bash-hook.ts", "src/agents/claude-cli.ts", "src/agents/veda-cli.ts", "src/agents/compact-control.ts",
        ...fs.readdirSync("src/worker").filter(name => name.endsWith(".ts")).map(name => `src/worker/${name}`)],
      outdir: path.join(root, "build"), outbase: "src", bundle: true, packages: "external",
      platform: "node", format: "esm", splitting: true, target: "node24", logLevel: "silent",
    });
    vi.stubEnv("DURABLE_TEST_FAUX_MODULE", import.meta.resolve("@earendil-works/pi-ai/providers/faux"));
    const receipt = path.join(root, "durable-receipt.mjs");
    const output = path.join(root, "durable-capture.json");
    fs.writeFileSync(receipt, `import fs from 'node:fs';
export default function (pi) { pi.on('session_start', () => fs.writeFileSync(${JSON.stringify(output)},
  JSON.stringify({argv:process.argv.slice(2), marker:process.env.LAUNCH_LITERAL,
    template:process.env.LAUNCH_TEMP, temporary:process.env.TMPDIR}))); }`);
    const provider = path.resolve("tests/fixtures/durable-pi-extension.ts");
    const value = new AgentManager(path.join(root, "cwd"), { ...DEFAULT_FABRIC_CONFIG.agents,
      runner: "pi-durable", timeoutMs: 20_000, retainRuns: true, sessionExport: false,
      process: { extensions: [provider, receipt], env: { LAUNCH_LITERAL: "durable marker", LAUNCH_TEMP: "${env:TMPDIR}" } },
    }, { workerPath: path.join(root, "build", "worker.js"), runRoot: path.join(root, "runs"), fullCodeMode: false,
      piBinary: path.join(root, "forbidden-native-fallback") });
    managers.push(value);
    const result = await value.run({ task: "durable launch", model: "durable-offline/test", thinking: "off",
      tools: [], extensions: false, transport: "process",
      ...(actor ? { actorId: "durable-actor-probe", actorName: "Durable probe", sessionFile: path.join(root, "durable-actor.jsonl") } : {}),
    });
    expect(result.status, result.error).toBe("completed");
    expect(JSON.parse(result.text)).toMatchObject({ reply: "durable launch" });
    const observed = JSON.parse(fs.readFileSync(output, "utf8"));
    expect(observed.argv).toEqual(expect.arrayContaining(["--no-extensions", "-e", provider, "-e", receipt]));
    expect(observed.marker).toBe("durable marker");
    expect(observed.template).toBe(observed.temporary);
    expect(observed.temporary).toBe(actor ? root : path.join(value.runDirectory(result.id)!, "tmp"));
  }, 30_000);

  it("composes with the existing carrier hook and expands only once from its snapshot", async () => {
    const extension = hook("mandatory");
    const value = manager({ extensions: [extension], env: {
      CARRIER_VALUE: "configured override", LAUNCH_FROM_HOOK: "${env:CARRIER_VALUE}",
      LAUNCH_TEMP: "${env:TMPDIR}", LAUNCH_RAW: "${env:RAW_TEMPLATE}", LAUNCH_EMPTY: "${env:EMPTY_VALUE}",
      LAUNCH_LITERAL: "$(touch never-created); `echo no`; $HOME; spaces and 'quotes'",
      PI_FABRIC_AGENT_NAME: "must-not-replace-worker-identity",
    } });
    const native = ProcessTransport.prototype.launch;
    let calls = 0;
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(function (this: ProcessTransport, request) {
      return native.call(this, { ...request, childEnvironment: environment => {
        calls++;
        return { ...environment, CARRIER_VALUE: "carrier snapshot", TMPDIR: root,
          RAW_TEMPLATE: "${env:NOT_PRESENT}", EMPTY_VALUE: "" };
      } });
    });
    const result = await value.run({ task: "compose", name: "compose-probe", transport: "process", extensions: false });
    expect(result.status, result.error).toBe("completed");
    expect(calls).toBe(1);
    const observed = capture();
    expect(observed.env).toMatchObject({ CARRIER_VALUE: "configured override", LAUNCH_FROM_HOOK: "carrier snapshot",
      LAUNCH_RAW: "${env:NOT_PRESENT}", LAUNCH_EMPTY: "", LAUNCH_LITERAL: "$(touch never-created); `echo no`; $HOME; spaces and 'quotes'" });
    // The worker's --tmpdir beats the hook's TMPDIR; templates must see that
    // actual execution-child directory, not the hook's superseded value.
    expect(observed.env.LAUNCH_TEMP).toBe(observed.env.TMPDIR);
    expect(observed.env.TMPDIR).toBe(path.join(value.runDirectory(result.id)!, "tmp"));
    // These are generic launch additions, not a way to replace the worker's
    // identity/safety contract on its eventual execution child.
    expect(observed.env.PI_FABRIC_AGENT_NAME).toBe("compose-probe");
    expect(fs.existsSync(path.join(root, "cwd", "never-created"))).toBe(false);
  }, 25_000);

  it.each([false, true])("keeps absent argv/env byte-identical to the old native spawn (actor=%s)", async actor => {
    const f = direct("pi", actor);
    const environment = { PATH: process.env.PATH!, GIT_OPTIONAL_LOCKS: "0", BASELINE: "spaces\nquotes'\"", TMPDIR: root };
    const child = spawn(process.execPath, [f.request.workerPath, ...f.request.workerArguments], { cwd: root, env: environment, stdio: "ignore" });
    await new Promise<void>((resolve, reject) => { child.once("error", reject); child.once("close", () => resolve()); });
    const baseline = fs.readFileSync(f.output, "utf8");
    let calls = 0;
    const request = { ...f.request, childEnvironment: () => { calls++; return environment; } };
    await launch(request);
    expect(fs.readFileSync(f.output, "utf8")).toBe(baseline);
    await launch(request, { extensions: [], env: {} });
    expect(fs.readFileSync(f.output, "utf8")).toBe(baseline);
    expect(calls).toBe(2);
    expect(f.request.workerArguments).not.toContain("--process-extensions");
  });

  it.each(["claude", "veda"])("does not add Pi hooks or env overrides to %s", async runner => {
    const f = direct(runner);
    const environment = { PATH: process.env.PATH!, TMPDIR: root, BASELINE: "unchanged" };
    await launch({ ...f.request, childEnvironment: () => environment }, {
      extensions: [hook("not-for-this-runner")], env: { BASELINE: "changed", FAIL: "${env:MISSING}" },
    });
    const observed = JSON.parse(fs.readFileSync(f.output, "utf8"));
    expect(observed.argv).toEqual(f.request.workerArguments);
    expect(observed.env).toEqual(environment);
  });

  it.each(["pi", "pi-durable"])("forwards independent explicit hook paths for %s", async runner => {
    const f = direct(runner);
    const extension = hook("explicit");
    await launch(f.request, { extensions: [extension], env: { LAUNCH_LITERAL: "configured" } });
    const observed = JSON.parse(fs.readFileSync(f.output, "utf8"));
    expect(observed.argv).toEqual([...f.request.workerArguments, "--process-extensions", JSON.stringify([extension])]);
    expect(observed.env.LAUNCH_LITERAL).toBe("configured");
  });

  it.each([false, true])("fails closed before any process if a template source is missing (actor=%s)", async actor => {
    const f = direct("pi", actor);
    const childEnvironment = vi.fn(() => ({ PATH: process.env.PATH, TMPDIR: root }));
    await expect(launch({ ...f.request, childEnvironment }, { env: { DESTINATION: "secret/${env:ABSENT_LAUNCH_SOURCE}" } }))
      .rejects.toThrow("agents.process.env.DESTINATION: referenced environment variable ABSENT_LAUNCH_SOURCE is missing");
    expect(childEnvironment).toHaveBeenCalledOnce();
    expect(fs.existsSync(f.output)).toBe(false);
  });

  it.each(["TMPDIR", "TMP", "TEMP"])("rejects a conflicting %s override before launching", async name => {
    const f = direct();
    f.request.workerArguments.push("--tmpdir", root);
    await expect(launch({ ...f.request, childEnvironment: () => ({ PATH: process.env.PATH, TMPDIR: root }) },
      { env: { [name]: path.join(root, "different-temp") } }))
      .rejects.toThrow(`agents.process.env.${name}: cannot override the worker temporary directory`);
    expect(fs.existsSync(f.output)).toBe(false);
  });

  it("does not allow env-only actor temp relocation either", async () => {
    const f = direct("pi", true);
    await expect(launch({ ...f.request, childEnvironment: () => ({ PATH: process.env.PATH, TMPDIR: root }) },
      { env: { TMPDIR: path.join(root, "different-actor-temp") } }))
      .rejects.toThrow("agents.process.env.TMPDIR: cannot override the worker temporary directory");
    expect(fs.existsSync(f.output)).toBe(false);
  });

  it("accepts matching temp overrides without changing prepared args or template identity", async () => {
    const f = direct();
    f.request.workerArguments.push("--tmpdir", root);
    await launch({ ...f.request, childEnvironment: () => ({ PATH: process.env.PATH, TMPDIR: "superseded-by-option" }) },
      { env: { TMPDIR: "${env:TMPDIR}", TMP: "${env:TMPDIR}", TEMP: "${env:TMPDIR}", LAUNCH_TEMP: "${env:TMPDIR}" } });
    const observed = JSON.parse(fs.readFileSync(f.output, "utf8"));
    expect(observed.argv).toEqual(f.request.workerArguments);
    expect(observed.env).toMatchObject({ TMPDIR: root, TMP: root, TEMP: root, LAUNCH_TEMP: root });
  });

  it("does not hand mandatory hooks to a compatible active generation that may ignore the new flag", async () => {
    const f = direct();
    const release = (name: string) => {
      const directory = path.join(root, name);
      fs.mkdirSync(path.join(directory, "dist"), { recursive: true });
      fs.writeFileSync(path.join(directory, "package.json"), JSON.stringify({ name: "pi-fabric", type: "module" }));
      fs.copyFileSync(f.request.workerPath, path.join(directory, "dist", "worker.js"));
      fs.writeFileSync(path.join(directory, "dist", "index.js"), "export default function () {};");
      fs.writeFileSync(path.join(directory, "dist", "worker-protocol.json"), JSON.stringify({ version: WORKER_PROTOCOL_VERSION }));
      return directory;
    };
    const parent = release("loaded-parent");
    const active = release("active-with-unknown-hooks-support");
    fs.writeFileSync(path.join(root, "profile", "settings.json"), JSON.stringify({ packages: [active] }));
    const request = { ...f.request, workerPath: path.join(parent, "dist", "worker.js") };
    const unconfigured = await new ProcessTransport().launch(request);
    try { expect(unconfigured.fabricRelease).toBe(active); await unconfigured.waitForClose?.(); }
    finally { await unconfigured.stop(); }
    const configured = await new ProcessTransport(undefined, undefined, undefined, undefined,
      { extensions: [hook("known-generation")] }).launch(request);
    try { expect(configured.fabricRelease).toBe(parent); await configured.waitForClose?.(); }
    finally { await configured.stop(); }
    expect(JSON.parse(fs.readFileSync(f.output, "utf8")).argv).toContain("--process-extensions");
  });

  it("pins configured hooks local with an explicit audit reason instead of dropping them remotely", async () => {
    const f = direct();
    const log = path.join(root, "placement.jsonl");
    f.request.workerArguments.push("--log-file", log);
    const placement: AgentPlacementConfig = { default: "remote", command: ["/definitely-not-a-launcher", "{cwd}"],
      capabilities: [], sharedMounts: [], commandTimeoutMs: 1000, pollIntervalMs: 100,
      cancelCommand: ["/definitely-not-a-canceller", "{id}"], resultDirectory: path.join(root, "placed", "{id}") };
    await launch(f.request, { env: { LAUNCH_LITERAL: "mandatory" } }, placement);
    expect(JSON.parse(fs.readFileSync(f.output, "utf8")).env.LAUNCH_LITERAL).toBe("mandatory");
    expect(JSON.parse(fs.readFileSync(log, "utf8"))).toMatchObject({ type: "placement.local",
      reason: "agents.process launch hooks require the local Pi worker; one-shot placement cannot forward them" });
  });

  it("parses host hook paths independently from the boolean, and rejects malformed worker input", () => {
    const f = direct();
    const required: Record<string, string> = { id: "probe", name: "probe", "task-file": "task", "status-file": "status",
      "lifecycle-file": "lifecycle", "log-file": "log", cwd: root, "pi-binary": "pi", "claude-binary": "claude",
      "veda-binary": "veda", "veda-backend": "agy", "veda-persona": "worker", "timeout-ms": "1000", depth: "1",
      "full-code-mode": "false", tools: "[]", "granted-risks": "[]", transport: "process" };
    const argv = [process.execPath, f.request.workerPath, ...f.request.workerArguments,
      ...Object.entries(required).flatMap(([key, value]) => ["--" + key, value])];
    const extension = hook("parsed");
    expect(parseWorkerOptions([...argv, "--process-extensions", JSON.stringify([extension])]))
      .toMatchObject({ extensions: false, processExtensions: [extension] });
    expect(parseWorkerOptions(argv)).not.toHaveProperty("processExtensions");
    for (const malformed of ["{", "null", "{}", '["relative.ts"]', '[42]', JSON.stringify(["/nul\0path"])]) {
      expect(() => parseWorkerOptions([...argv, "--process-extensions", malformed])).toThrow("Invalid worker process extensions");
    }
    expect(() => parseWorkerOptions([...argv, "--runner", "claude", "--process-extensions", JSON.stringify([extension])]))
      .toThrow("Invalid worker process extensions");
  });
});
