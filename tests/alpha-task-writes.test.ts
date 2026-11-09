import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import {
  createAgentSession,
  DefaultResourceLoader,
  ExtensionRunner,
  ModelRuntime,
  SessionManager,
  SettingsManager,
  type AgentSession,
} from "@earendil-works/pi-coding-agent";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { checkWritePolicyRequest, resolveChildWritePolicy } from "../src/agents/child-env.js";
import { taskAgentEnvironment } from "../src/agents/task-environment.js";
import type { AgentRunResult, AgentTransportHandle, AgentTransportLaunch } from "../src/agents/types.js";
import fabricWriteGuard, { readWritePolicy, writePolicyDenial, type FabricWritePolicy } from "../src/agents/write-guard.js";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { childProcessEnvironment, writeJsonAtomic } from "../src/core/atomic-write.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import { PiToolsProvider } from "../src/providers/pi-tools-provider.js";
import { powerShellToolDefinitionFactory } from "../src/providers/pi-bash-cwd.js";

// Data-safety/trust-boundary proofs: real native mutations and registered hooks,
// nested provider calls, public manager admission, and real worker bootstrap.
// Everything mutable is beneath a fresh owned root; retain run traces, never purge them.
const managers: AgentManager[] = [];
const providers: PiToolsProvider[] = [];
const sessions: AgentSession[] = [];
const fixture = () => {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "alpha-task-writes-")));
  const home = path.join(root, "home"), work = path.join(root, "work");
  const allowed = path.join(work, "allowed"), scratch = path.join(root, "spawner-scratch");
  for (const directory of [home, work, allowed, scratch]) fs.mkdirSync(directory, { recursive: true });
  const disabled = path.join(home, ".config", "smarty", "bash-guard.disabled");
  vi.stubEnv("HOME", home);
  vi.stubEnv("USERPROFILE", home);
  return { root, home, work, allowed, scratch, disabled };
};
type Fixture = ReturnType<typeof fixture>;
let f: Fixture;
beforeEach(() => {
  f = fixture();
  vi.stubEnv("SMARTY_ALPHA", undefined);
  vi.stubEnv("PI_FABRIC_WRITE_POLICY", undefined);
  vi.stubEnv("PI_FABRIC_TOOL_ALLOWLIST", undefined);
  vi.stubEnv("PI_CODING_AGENT_DIR", path.join(f.home, "profile"));
  vi.stubEnv("TMPDIR", f.scratch);
  vi.stubEnv("TMP", f.scratch);
  vi.stubEnv("TEMP", f.scratch);
});
afterEach(async () => {
  try {
    await Promise.all(providers.splice(0).map(provider => provider.close()));
    await Promise.all(managers.splice(0).map(manager => manager.close()));
  } finally {
    for (const session of sessions.splice(0)) session.dispose();
    vi.restoreAllMocks();
    vi.unstubAllEnvs();
  }
});
const disable = () => {
  fs.mkdirSync(path.dirname(f.disabled), { recursive: true });
  fs.writeFileSync(f.disabled, "");
};
const rawPolicy = (kind: string): string => kind === "malformed" ? "{invalid" : JSON.stringify({
  readOnly: kind === "read-only", writableRoots: kind === "read-only" ? [] : [f.allowed], shell: "deny",
});
const inherited = (kind: string): FabricWritePolicy => readWritePolicy(rawPolicy(kind))!;

const toolHarness = async (policy: string, replay = true) => {
  vi.stubEnv("PI_FABRIC_WRITE_POLICY", policy);
  const agentDir = path.join(f.root, `native-sdk-${sessions.length}`);
  fs.mkdirSync(agentDir);
  const settingsManager = SettingsManager.inMemory({ defaultProjectTrust: "never", compaction: { enabled: false }, retry: { enabled: false } });
  const resourceLoader = new DefaultResourceLoader({
    cwd: f.work, agentDir, settingsManager, noExtensions: true, noSkills: true, noThemes: true, noPromptTemplates: true,
    agentsFilesOverride: () => ({ agentsFiles: [] }), extensionFactories: [fabricWriteGuard],
  });
  await resourceLoader.reload();
  expect(resourceLoader.getExtensions().errors).toEqual([]);
  const modelRuntime = await ModelRuntime.create({ authPath: path.join(agentDir, "auth.json"), modelsPath: null,
    allowModelNetwork: false, refreshOnCreate: false });
  const { session } = await createAgentSession({ cwd: f.work, agentDir, settingsManager, modelRuntime, resourceLoader,
    sessionManager: SessionManager.inMemory(f.work), tools: ["write", "edit"] });
  sessions.push(session);
  await session.bindExtensions({ mode: "json" });
  // The SDK registers the guard and binds native tools to real execution contexts.
  const runner = session.extensionRunner;
  expect(runner).toBeInstanceOf(ExtensionRunner);
  expect(runner.hasHandlers("tool_call")).toBe(true);
  const signal = new AbortController().signal;
  const context = runner.createToolContext("alpha-host", signal);
  expect(context.tools.map(tool => tool.name).sort()).toEqual(["edit", "write"]);
  const catalog = new CapturedToolCatalog();
  catalog.replace([], runner, DEFAULT_FABRIC_CONFIG.capture, path.join(f.root, "extension.ts"));
  const provider = replay ? new PiToolsProvider(f.work, catalog, undefined)
    : new PiToolsProvider(f.work, undefined, undefined, { allowUnhookedTools: true, powerShellToolDefinitionFactory });
  providers.push(provider);
  const invocation: FabricInvocationContext = { cwd: f.work, extensionContext: context, parentToolCallId: "alpha-parent",
    nestedToolCallId: "alpha-nested", signal, update() {} };
  const native = async (name: "write" | "edit", input: Record<string, unknown>) => {
    const refusal = await runner.emitToolCall({ type: "tool_call", toolName: name, toolCallId: "alpha-native", input });
    if (refusal?.block) throw new Error(refusal.reason);
    // AgentTools from ctx.tools wrap the real SDK definitions with createToolContext;
    // unlike ToolDefinition.execute, their execute API needs no fabricated fifth arg.
    const tool = context.tools.find(tool => tool.name === name);
    if (!tool) throw new Error(`Native SDK tool unavailable: ${name}`);
    return tool.execute("alpha-native", input, signal);
  };
  const nested = (name: string, input: Record<string, unknown>) => provider.invoke(name, input, invocation);
  const mutate = async (label: string) => {
    for (const [route, invoke] of [["native", native], ["nested", nested]] as const) {
      const target = path.join(f.scratch, `${label}-${route}.txt`);
      await invoke("write", { path: target, content: "before" });
      expect(fs.readFileSync(target, "utf8")).toBe("before");
      await invoke("edit", { path: target, edits: [{ oldText: "before", newText: "after" }] });
      expect(fs.readFileSync(target, "utf8")).toBe("after");
    }
  };
  return { native, nested, mutate };
};

describe("alpha native task writes", () => {
  it.each(["root-limited", "read-only", "malformed"])("writes and edits spawner scratch by default despite %s metadata", async kind => {
    const harness = await toolHarness(rawPolicy(kind));
    expect(writePolicyDenial(inherited(kind), "write", { path: f.scratch }, f.work)).toBeUndefined();
    await harness.mutate(kind);
    for (const shell of ["bash", "powershell"]) {
      expect(writePolicyDenial(inherited(kind), shell, { command: "echo alpha-shell-allowed" }, f.work)).toBeUndefined();
    }
    expect(JSON.stringify(await harness.nested("bash", { command: "echo alpha-shell-allowed", timeout: 5 }))).toContain("alpha-shell-allowed");
  });

  it("uses live env and disable-file changes on the same registered hook and provider", async () => {
    vi.stubEnv("SMARTY_ALPHA", "0");
    const harness = await toolHarness(rawPolicy("read-only"));
    const target = path.join(f.scratch, "blocked.txt");
    const blocked = async () => {
      await expect(harness.native("write", { path: target, content: "blocked" })).rejects.toThrow("read-only");
      await expect(harness.nested("write", { path: target, content: "blocked" })).rejects.toThrow("read-only");
      expect(fs.existsSync(target)).toBe(false);
    };
    await blocked();
    for (const [index, value] of [undefined, "1", "", "true", "00", " 0", "0 "].entries()) {
      vi.stubEnv("SMARTY_ALPHA", value);
      await harness.mutate(`mode-${index}`);
    }
    vi.stubEnv("SMARTY_ALPHA", "0");
    await blocked();
    disable();
    await harness.mutate("disable-file");
    fs.unlinkSync(f.disabled); // Only this fixture's marker, never user configuration or a trace.
    await blocked();
  });

  it.each(["root-limited", "read-only", "malformed"])("preserves strict %s refusal bytes and disk contents", async kind => {
    vi.stubEnv("SMARTY_ALPHA", "0");
    const harness = await toolHarness(rawPolicy(kind));
    const target = path.join(f.scratch, "sentinel.txt");
    fs.writeFileSync(target, "original");
    const policy = inherited(kind);
    for (const name of ["write", "edit"] as const) {
      const expected = kind === "root-limited"
        ? `Fabric write policy refuses ${name} outside writable roots; refused path: ${JSON.stringify(target)}; allowed roots: ${JSON.stringify(policy.writableRoots)}`
        : `Fabric write policy refuses ${name}: this agent is read-only; refused path: ${JSON.stringify(target)}; allowed roots: []`;
      const input = { path: target, content: "forbidden", edits: [{ oldText: "original", newText: "forbidden" }] };
      expect(writePolicyDenial(policy, name, input, f.work)).toBe(expected);
      await expect(harness.native(name, input)).rejects.toThrow(expected);
      await expect(harness.nested(name, input)).rejects.toThrow(expected);
      expect(fs.readFileSync(target, "utf8")).toBe("original");
    }
  });

  it("also applies the shared decision when nested calls have no replayed hook", async () => {
    const harness = await toolHarness(rawPolicy("read-only"), false);
    const target = path.join(f.scratch, "direct-provider.txt");
    await harness.nested("write", { path: target, content: "before" });
    await harness.nested("edit", { path: target, edits: [{ oldText: "before", newText: "after" }] });
    expect(fs.readFileSync(target, "utf8")).toBe("after");
    vi.stubEnv("SMARTY_ALPHA", "0");
    await expect(harness.nested("edit", { path: target, edits: [{ oldText: "after", newText: "forbidden" }] })).rejects.toThrow("read-only");
    expect(fs.readFileSync(target, "utf8")).toBe("after");
  });

  it("does not bypass native edit validation or the nested tool allowlist", async () => {
    const harness = await toolHarness(rawPolicy("read-only"));
    const target = path.join(f.scratch, "validation.txt");
    await harness.nested("write", { path: target, content: "original" });
    await expect(harness.nested("edit", { path: target, edits: [{ oldText: "absent", newText: "changed" }] })).rejects.toThrow();
    await expect(harness.nested("edit", { path: target, edits: [{ oldText: 42, newText: "changed" }] })).rejects.toThrow();
    vi.stubEnv("PI_FABRIC_TOOL_ALLOWLIST", JSON.stringify(["read"]));
    const allowlisted = await toolHarness(rawPolicy("read-only"));
    await expect(allowlisted.nested("write", { path: target, content: "forbidden" })).rejects.toThrow("tool allowlist");
    expect(fs.readFileSync(target, "utf8")).toBe("original");
  });
});

const managerFor = (options: ConstructorParameters<typeof AgentManager>[2] = {}) => {
  const manager = new AgentManager(f.work, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0,
    retainRuns: true, sessionExport: false, timeoutMs: 10_000 }, {
    runRoot: path.join(f.root, "runs"), workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
    fullCodeMode: false, shortTmpdirSource: () => ({ tmpdir: f.scratch, budgetBytes: 4096 }), ...options,
  });
  managers.push(manager);
  return manager;
};
const argumentsOf = (request: AgentTransportLaunch) => new Map(request.workerArguments.reduce<Array<[string, string]>>((pairs, value, index, args) => {
  if (index % 2 === 0) pairs.push([value, args[index + 1]!]);
  return pairs;
}, []));
const recordingTransport = () => {
  const launches: Array<{ request: AgentTransportLaunch; args: Map<string, string>; env: NodeJS.ProcessEnv }> = [];
  vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
    const args = argumentsOf(request);
    const env = childProcessEnvironment(request.childEnvironment?.(taskAgentEnvironment()) ?? taskAgentEnvironment());
    launches.push({ request, args, env });
    request.onWritePolicyDisposition?.("local");
    request.onWritePolicyDisposition?.("local");
    const now = Date.now();
    const record: AgentRunResult = { id: request.id, name: request.name, task: fs.readFileSync(args.get("--task-file")!, "utf8"),
      runner: args.get("--runner")!, transport: "process", cwd: request.cwd, status: "completed",
      startedAt: now, updatedAt: now, finishedAt: now, turns: 1, toolCalls: 0, text: "recorded",
      usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 }, sessionId: "2147483647" };
    writeJsonAtomic(args.get("--status-file")!, record);
    return { kind: "process", sessionId: "2147483647", closed: Promise.resolve(), waitForClose: async () => {},
      isAlive: async () => false, stop: async () => {} } satisfies AgentTransportHandle;
  });
  return launches;
};
const policyEvents = (id: string) => fs.readFileSync(path.join(f.root, "runs", id, "events.jsonl"), "utf8")
  .trim().split("\n").map(line => JSON.parse(line) as { type: string; warning?: string; policy?: FabricWritePolicy })
  .filter(event => event.type.startsWith("write_policy."));

describe("alpha explicit unrestricted task admission", () => {
  it.each(["root-limited", "read-only", "malformed"])("drops inherited %s only for the explicit opt-out, including native executor preflight", async kind => {
    vi.stubEnv("SMARTY_ALPHA", "0");
    vi.stubEnv("PI_FABRIC_WRITE_POLICY", rawPolicy(kind));
    const launches = recordingTransport();
    const manager = managerFor({ executorRuntime: () => "node-process" });
    const request = { task: "record unrestricted child", writePolicy: "unrestricted" as const, kernel: "typescript" as const, transport: "process" as const };
    expect(() => resolveChildWritePolicy(request, inherited(kind), f.work)).toThrow("cannot launch an unrestricted child");
    await expect(manager.run(request)).rejects.toThrow("cannot launch an unrestricted child");
    expect(launches).toHaveLength(0);
    // Same manager: neither constructor-time nor first-use caching is allowed.
    for (const [index, value] of [undefined, "1", "", "00"].entries()) {
      vi.stubEnv("SMARTY_ALPHA", value);
      expect(resolveChildWritePolicy(request, inherited(kind), path.join(f.root, "not-created"))).toBeUndefined();
      const result = await manager.run({ ...request, name: `unrestricted-${index}` });
      expect(result.status).toBe("completed");
      const launch = launches.at(-1)!;
      expect(launch.args.has("--write-policy")).toBe(false);
      expect(launch.env.SMARTY_ALPHA).toBe(value);
      expect(policyEvents(result.id).map(event => event.type)).toEqual(["write_policy.unrestricted"]);
    }
    expect(fs.existsSync(path.join(f.root, "not-created"))).toBe(false);
    vi.stubEnv("SMARTY_ALPHA", "0");
    await expect(manager.run(request)).rejects.toThrow("cannot launch an unrestricted child");
    disable();
    expect(resolveChildWritePolicy(request, inherited(kind), f.work)).toBeUndefined();
    expect((await manager.run(request)).status).toBe("completed");
    fs.unlinkSync(f.disabled);
    expect(() => resolveChildWritePolicy(request, inherited(kind), f.work)).toThrow("cannot launch an unrestricted child");
    await expect(manager.run(request)).rejects.toThrow("cannot launch an unrestricted child");
    // With alpha explicitly off, retained shell denial still rejects native executors.
    vi.stubEnv("SMARTY_ALPHA", "0");
    await expect(manager.run({ task: "still confined", kernel: "typescript", readOnly: true })).rejects.toThrow("native Fabric executor");
  });

  it("does not retain phantom runner-capability confinement", async () => {
    vi.stubEnv("PI_FABRIC_WRITE_POLICY", rawPolicy("read-only"));
    const launches = recordingTransport();
    const manager = managerFor();
    const result = await manager.run({ task: "record unsupported runner opt-out", runner: "veda", writePolicy: "unrestricted", extensions: false, tools: [], transport: "process" });
    expect(result.status).toBe("completed");
    expect(launches).toHaveLength(1);
    expect(launches[0]!.args.has("--write-policy")).toBe(false);
    await expect(manager.run({ task: "still inherited", runner: "veda" })).rejects.toThrow("only by the Pi runner");
  });

  it("keeps malformed request and actor launch refusals separate from alpha", async () => {
    const launches = recordingTransport();
    const manager = managerFor();
    const invalid = { writePolicy: "unrestricted" as const, readOnly: false };
    expect(() => checkWritePolicyRequest(invalid)).toThrow("cannot be combined");
    await expect(manager.run({ task: "invalid", ...invalid })).rejects.toThrow("cannot be combined");
    await expect(manager.run({ task: "invalid roots", writableRoots: "invalid" as unknown as string[] })).rejects.toThrow("writableRoots");
    await expect(manager.run({ task: "actor opt-out", actorId: "test-actor", writePolicy: "unrestricted" })).rejects.toThrow("Actors cannot use");
    expect(launches).toHaveLength(0);
  });

  it.each([false, true])("retains scope and scratch metadata with an explicit native-alpha audit (inherited: %s)", async inheritedPolicy => {
    if (inheritedPolicy) vi.stubEnv("PI_FABRIC_WRITE_POLICY", rawPolicy("read-only"));
    const launches = recordingTransport();
    const manager = managerFor();
    const result = await manager.run({ task: "record retained metadata", extensions: false, transport: "process" });
    expect(result.status).toBe("completed");
    expect(launches[0]!.args.has("--write-policy")).toBe(true);
    const policy = readWritePolicy(launches[0]!.args.get("--write-policy"))!;
    expect(policy).toBeDefined();
    if (inheritedPolicy) expect(policy).toEqual(inherited("read-only"));
    else {
      expect(policy.writableRoots).toContain(f.work);
      const scratch = launches[0]!.args.get("--tmpdir")!;
      expect(policy.writableRoots).toContain(scratch);
      if (process.platform === "win32") expect(scratch).toBe(path.join(f.root, "runs", result.id, "tmp"));
      else expect(scratch.startsWith(f.scratch + path.sep)).toBe(true);
      const identity = JSON.parse(fs.readFileSync(path.join(f.root, "runs", result.id, "run-identity.json"), "utf8"));
      expect(identity.temporaryDirectory).toBe(scratch);
    }
    const events = policyEvents(result.id);
    expect(events.filter(event => event.type === "write_policy.alpha_native_unconfined")).toEqual([
      expect.objectContaining({ policy, warning: expect.stringContaining("OS restrictions remain independent") }),
    ]);
    if (!inheritedPolicy) expect(events.filter(event => event.type === "write_policy.default")).toHaveLength(1);
  });
});

// Parent runs this only after its fresh build. The inference binary is an offline
// protocol fixture, but it uses Pi's real write/edit implementations and the guard
// actually selected by worker argv. No model, server, network, shell or deletion.
describe("alpha worker bootstrap", () => {
  it.each([false, true])("forwards exact alpha env and clears only an unrestricted child's inherited policy (unrestricted: %s)", async unrestricted => {
    const worker = path.resolve("dist/worker.js");
    expect(fs.existsSync(worker), "parent must build the candidate before bootstrap proofs").toBe(true);
    const piModule = pathToFileURL(path.resolve("node_modules/@earendil-works/pi-coding-agent/dist/index.js")).href;
    const binary = path.join(f.root, "offline-native.mjs");
    fs.writeFileSync(binary, `import fs from 'node:fs';
import path from 'node:path';
import {createInterface} from 'node:readline';
import {pathToFileURL} from 'node:url';
import {createAgentSession,DefaultResourceLoader,ModelRuntime,SessionManager,SettingsManager} from ${JSON.stringify(piModule)};
const emit = value => process.stdout.write(JSON.stringify(value)+'\\n');
const argv=process.argv.slice(2), guards=[];
for(let i=0;i<argv.length;i++) if(argv[i]==='-e' && /[\\\\/]agents[\\\\/]write-guard\\.js$/.test(argv[i+1]??'')) {
  const guard=await import(pathToFileURL(argv[i+1]).href);
  guards.push(guard.default);
}
const cwd=process.cwd(), agentDir=${JSON.stringify(path.join(f.root, "bootstrap-sdk"))};
fs.mkdirSync(agentDir);
const settingsManager=SettingsManager.inMemory({defaultProjectTrust:'never',compaction:{enabled:false},retry:{enabled:false}});
const resourceLoader=new DefaultResourceLoader({cwd,agentDir,settingsManager,noExtensions:true,noSkills:true,noThemes:true,noPromptTemplates:true,
  agentsFilesOverride:()=>({agentsFiles:[]}),extensionFactories:guards});
await resourceLoader.reload();
if(resourceLoader.getExtensions().errors.length) throw new Error(JSON.stringify(resourceLoader.getExtensions().errors));
const modelRuntime=await ModelRuntime.create({authPath:path.join(agentDir,'auth.json'),modelsPath:null,allowModelNetwork:false,refreshOnCreate:false});
const {session}=await createAgentSession({cwd,agentDir,settingsManager,modelRuntime,resourceLoader,sessionManager:SessionManager.inMemory(cwd),tools:['write','edit']});
try {
  await session.bindExtensions({mode:'json'});
  const runner=session.extensionRunner, context=runner.createToolContext('bootstrap-host',undefined);
  const hookCount=resourceLoader.getExtensions().extensions.reduce((count,extension)=>count+(extension.handlers.get('tool_call')?.length??0),0);
  for await(const line of createInterface({input:process.stdin})) {
    const frame=JSON.parse(line); if(frame.type!=='prompt') continue;
    emit({type:'agent_start'});
    const target=JSON.parse(frame.message).target;
    const report={alpha:process.env.SMARTY_ALPHA,policy:process.env.PI_FABRIC_WRITE_POLICY??null,argv,guards:hookCount,
      role:process.env.SMARTY_ROLE,gitReadOnly:process.env.SMARTY_GIT_READ_ONLY,
      identity:JSON.parse(fs.readFileSync(process.env.PI_FABRIC_RUN_IDENTITY,'utf8'))};
    for(const [name,args] of [['write',{path:target,content:'before'}],['edit',{path:target,edits:[{oldText:'before',newText:'after'}]}]]) {
      const blocked=await runner.emitToolCall({type:'tool_call',toolName:name,toolCallId:'bootstrap',input:args});
      if(blocked?.block) throw new Error(blocked.reason);
      const tool=context.tools.find(tool=>tool.name===name);
      if(!tool) throw new Error('Native SDK tool unavailable: '+name);
      await tool.execute('bootstrap',args);
    }
    report.bytes=fs.readFileSync(target,'utf8');
    emit({type:'message_end',message:{role:'assistant',content:[{type:'text',text:JSON.stringify(report)}],stopReason:'stop'}});
    emit({type:'agent_settled'});
  }
} finally {session.dispose();}
`);
    vi.stubEnv("SMARTY_ALPHA", "0");
    vi.stubEnv("PI_FABRIC_WRITE_POLICY", rawPolicy("read-only"));
    const manager = managerFor({ workerPath: worker, piBinary: binary });
    disable(); // After construction: both receiver and execution child must observe it live.
    const target = path.join(f.scratch, "bootstrap-native.txt");
    const result = await manager.run({ task: JSON.stringify({ target }), extensions: false, tools: ["write", "edit"], transport: "process",
      ...(unrestricted ? { writePolicy: "unrestricted" as const } : {}) });
    expect(result.status, `${result.error ?? ""}\n${result.stderr ?? ""}`).toBe("completed");
    const report = JSON.parse(result.text);
    expect(report.alpha).toBe("0");
    expect(report.role).toMatch(/^task-agent(?:@|$)/);
    expect(report.gitReadOnly).toBe("1");
    expect(report.policy).toBe(unrestricted ? null : rawPolicy("read-only"));
    expect(report.guards).toBe(unrestricted ? 0 : 1);
    expect(report.argv).toContain("--no-extensions");
    expect(report.identity).toMatchObject({ runId: result.id, runDirectory: path.join(f.root, "runs", result.id) });
    expect(report.bytes).toBe("after");
    expect(fs.readFileSync(target, "utf8")).toBe("after");
    expect(process.env.PI_FABRIC_WRITE_POLICY).toBe(rawPolicy("read-only"));
  }, 30_000);
});
