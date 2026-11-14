// Offline integration against the actual installed SDK and compiled extension.
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { mkdir, mkdtemp, readFile, rm, writeFile, realpath } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.resolve(fileURLToPath(new URL("..", import.meta.url)));
// smarty-dev#11145: scratch lives under the caller TMPDIR, the test temp root the deletion guard pins (under
// vitest, test-temp's private TMPDIR). Not isolatedTestTemp(): on a Windows runner it returns the shared job
// directory, which the cleanup below would remove.
const scratch = await mkdtemp(path.join(tmpdir(), "pi1-sdk-"));
for (const name of Object.keys(process.env)) if (name.startsWith("PI_FABRIC_")) delete process.env[name];
process.env.PI_CODING_AGENT_DIR = scratch;
process.env.PI_FABRIC_AGENT_DIR = path.join(scratch, "exports");
const host = await import("@earendil-works/pi-coding-agent");
const ai = await import("@earendil-works/pi-ai");
const { Type } = await import("typebox");
assert.equal(host.VERSION, "1.2.0");
assert.equal(typeof host.ExtensionRunner.prototype.getAllRegisteredTools, "function");
assert.equal(typeof host.ExtensionRunner.prototype.createToolContext, "function");
const hostEntry = await realpath(fileURLToPath(import.meta.resolve("@earendil-works/pi-coding-agent")));
const hostManifest = JSON.parse(await readFile(path.resolve(path.dirname(hostEntry), "../package.json"), "utf8"));
assert.equal(hostManifest.version, "1.2.0");
const usage = { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, totalTokens: 2, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } };
let scenarios = 0;
let mcpSkipped = 0;
let scenarioIndex = 0;
try {
  // Pi 1.2 may block MCP until the host's required review; never bypass that policy in this smoke.
  for (const [mode, fullCodeMode, schemaMode, nativeMcp = false, profile = "additive"] of [["on", true, "off"], ["only", true, "off"], ["only", false, "enforce"], ["only", true, "off", true], ["on", false, "off", true], ["on", false, "audit"], ["only", true, "off", true, "native"]]) {
    const cwd = path.join(scratch, `session-${scenarioIndex++}`);
    await mkdir(cwd, { recursive: true });
    const mcpConfig = path.join(cwd, "mcporter.json");
    await writeFile(mcpConfig, JSON.stringify({ mcpServers: {}, imports: [] }));
    await writeFile(path.join(scratch, "fabric.json"), JSON.stringify({
      fullCodeMode, executor: { codemodeProfile: profile }, schema: { mode: schemaMode }, capture: { keepVisible: ["fixture_echo"] },
      approvals: { read: "allow", write: "allow", execute: "allow" },
      mcp: { enabled: nativeMcp, nativeServers: nativeMcp ? ["fixture"] : [], configPath: mcpConfig, cache: { revalidate: "off" } }, mesh: { enabled: false }, memory: { enabled: false },
      agents: { enabled: false }, entropy: { enabled: false, compile: false }, ui: { enabled: false },
    }));
    const settingsManager = host.SettingsManager.inMemory({
      defaultTools: ["+codemode", "+tool_search"], defaultProjectTrust: "never",
      compaction: { enabled: false }, retry: { enabled: false },
    });
    const requests = [], events = [], errors = [];
    let api, toolContext, nextCode, calls = 0, preparations = 0, overrides = 0;
    const originalCaptureMethod = host.ExtensionRunner.prototype.getAllRegisteredTools;
    const fixtureTool = (name, exposure = "direct") => ({
      name, label: name, description: "Offline proxy fixture", exposure, promptGuidelines: [`guideline:${name}`],
      parameters: Type.Object({ value: Type.String() }), outputSchema: Type.Object({ value: Type.String() }),
      async execute(_id, args, _signal, onUpdate, ctx) {
        toolContext = ctx; calls++;
        if (args.value === "mutate") api.setActiveTools(["read", "codemode", "tool_search", ...(nativeMcp ? ["mcp__fixture__echo"] : [])]);
        onUpdate?.({ content: [{ type: "text", text: "progress" }], details: {} });
        return { content: [{ type: "text", text: `echo:${args.value}` }], structuredContent: { value: args.value }, details: { value: args.value } };
      },
    });
    const loader = new host.DefaultResourceLoader({
      cwd, agentDir: scratch, settingsManager, noExtensions: true, noSkills: true, noPromptTemplates: true, noThemes: true,
      additionalExtensionPaths: [path.join(root, "dist/index.js")], agentsFilesOverride: () => ({ agentsFiles: [] }),
      extensionFactories: [host.createCodemodeExtension({ mode }), host.createToolSearchExtension(), (pi) => {
        api = pi;
        pi.registerTool(fixtureTool("fixture_echo"));
        pi.registerTool(fixtureTool("fixture_deferred", "deferred"));
        pi.registerTool(fixtureTool("fixture_hidden", "hidden"));
        pi.registerTool({
          name: "read", label: "Override", description: "Read override", parameters: Type.Object({ path: Type.String() }),
          async execute(_id, args) { overrides++; return { content: [{ type: "text", text: `override:${args.path}` }], details: {} }; },
        });
        pi.registerTool({
          name: "fixture_prepare", label: "Prepared", description: "Prepare exactly once", parameters: Type.Object({ value: Type.String() }),
          prepareArguments(args) { preparations++; return { value: `${args.value}!` }; },
          async execute(_id, args, _signal, _update, ctx) {
            assert.equal(typeof ctx.executeTool, "function");
            return { content: [{ type: "text", text: args.value }], details: {} };
          },
        });
        if (nativeMcp) pi.registerMcpServer("fixture", { command: process.execPath, args: [path.join(root, "tests/fixtures/pi1-mcp.mjs")], exposure: "direct" });
        pi.on("tool_call", (event) => {
          events.push(event);
          if (event.toolName === "mcp__fixture__echo" && event.input.value === "native-block") return { block: true, reason: "native MCP denied" };
          if (event.toolName === "fixture_echo" && event.input.value === "block") return { block: true, reason: "fixture denied" };
          if (["fixture_echo", "fixture_alias"].includes(event.toolName) && event.input.value === "rewrite") event.input.value = "rewritten";
        });
        pi.on("tool_result", (event) => {
          events.push(event);
          if (event.toolName === "mcp__fixture__echo" && event.input.value === "native-redact" && !event.isError) return { content: [{ type: "text", text: "native redacted" }] };
          if (["fixture_echo", "fixture_alias"].includes(event.toolName) && !event.isError) {
            if (event.toolName === "fixture_alias") assert.equal(event.input.value, "rewritten");
            return { content: [{ type: "text", text: "redacted" }], structuredContent: { value: "redacted" } };
          }
        });
        pi.on("session_start", () => pi.registerTool(fixtureTool("fixture_start")));
        pi.registerProvider("offline-pi1", {
          api: "offline-pi1", apiKey: "offline-fixture", baseUrl: "http://invalid.local",
          models: [{ id: "fixture", name: "Offline fixture", reasoning: false, input: ["text"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 128000, maxTokens: 4096 }],
          streamSimple(model, context) {
            requests.push(context);
            const declared = ai.getCurrentTools(context.messages).map(tool => tool.name);
            if (fullCodeMode || schemaMode === "enforce") {
              assert.deepEqual(declared, ["fabric_exec"]);
              const description = ai.getCurrentTools(context.messages)[0].description;
              if (api.getActiveTools().includes("fixture_echo")) assert.match(description, /guideline:fixture_echo/);
              assert.ok(!description.includes("guideline:fixture_hidden"));
            }
            else {
              assert.ok(declared.includes("fabric_exec"), "non-exclusive mode retains Fabric");
              assert.ok(declared.includes("codemode") && declared.includes("tool_search"), "optional/audit mode retains the native loadout");
            }
            assert.ok(ai.getCurrentSystemPrompt(context.messages).includes("fabric_exec"));
            const code = nextCode; nextCode = undefined;
            const message = { role: "assistant", content: code ? [{ type: "toolCall", id: `outer-${requests.length}`, name: "fabric_exec", arguments: { code, display: { name: "Verify compatibility", description: "Preserve Fabric invocation contracts" } } }] : [{ type: "text", text: "done" }], api: model.api, provider: model.provider, model: model.id, usage, stopReason: "pending", timestamp: Date.now() };
            const stream = ai.createAssistantMessageEventStream();
            stream.push({ type: "start", partial: message });
            message.stopReason = code ? "toolUse" : "stop";
            stream.push({ type: "done", reason: message.stopReason, message }); stream.end(); return stream;
          },
        });
      }, ...(nativeMcp ? [host.createMcpExtension()] : [])],
    });
    await loader.reload();
    const extensionErrors = loader.getExtensions().errors;
    const mcpPolicyError = extensionErrors.some(({ error }) => error.includes("MCP_SECURITY_REVIEW_REQUIRED"));
    if (nativeMcp && mcpPolicyError) {
      assert.deepEqual(extensionErrors.filter(({ error }) => !error.includes("MCP_SECURITY_REVIEW_REQUIRED")), []);
    } else assert.deepEqual(extensionErrors, []);
    const modelRuntime = await host.ModelRuntime.create({ authPath: path.join(cwd, "auth.json"), modelsPath: null, modelsStorePath: path.join(cwd, "models-cache.json"), refreshOnCreate: false });
    const { session } = await host.createAgentSession({ cwd, agentDir: scratch, settingsManager, modelRuntime, resourceLoader: loader, sessionManager: host.SessionManager.inMemory(cwd) });
    try {
      // Keep a real SDK binding so reload emits session_start for the new runner.
      await session.bindExtensions({ onError: error => errors.push(error) });
      assert.equal(session.getToolDefinition("fabric_exec").exposure, "model-only");
      assert.notEqual(host.ExtensionRunner.prototype.getAllRegisteredTools, originalCaptureMethod, `capture not installed for scenario ${scenarioIndex - 1}`);
      if (nativeMcp && mcpPolicyError) {
        mcpSkipped++;
        continue;
      }
      const model = modelRuntime.getModel("offline-pi1", "fixture"); assert.ok(model);
      await session.setModel(model);
      assert.ok(api.getActiveTools().includes("codemode"), "builtin codemode is active at startup");
      assert.ok(api.getActiveTools().includes("tool_search"), "builtin tool_search is active at startup");
      const firstCaptureMethod = host.ExtensionRunner.prototype.getAllRegisteredTools;
      assert.equal(session.getAllTools().find((t) => t.name === "fixture_hidden").exposure, "hidden");
      await session.prompt("Verify initial loadout.");
      if (nativeMcp) assert.ok(session.getAllTools().some((t) => t.name === "mcp__fixture__echo"), "native MCP connected");
      api.registerTool(fixtureTool("fixture_late"));
      // smarty-dev#11350: fixture_prepare is a direct tool. Pi 1.2 calls only ACTIVE direct tools (plus registered
      // codemode/deferred ones), and Fabric's hidden-tool contract matches it, so a replacement that drops it makes the
      // later fixture_prepare call below correctly denied. Keep it active here.
      api.setActiveTools(["read", "fixture_echo", "fixture_late", "fixture_prepare", "codemode", "tool_search", ...(nativeMcp ? ["mcp__fixture__echo"] : []), ...(!fullCodeMode && schemaMode !== "enforce" ? ["fabric_exec"] : [])]);
      await session.prompt("Verify explicit replacement without fabric_exec is repaired.");
      if (fullCodeMode) {
        nextCode = `const agentList = await agents.list(); if (!Array.isArray(agentList)) throw new Error("agents.list must return an array"); const a = await extensions.fixture_echo({value:"rewrite"}); const b = await extensions.fixture_deferred({value:"deferred"}); ${nativeMcp ? 'const c = await extensions.mcp__fixture__echo({value:"native"});' : ""} const d = await extensions.tool_search({query:"fixture_deferred"}); const e = await extensions.codemode({code:"return 42;"}); return {agentList,a,b,${nativeMcp ? "c," : ""}d,e};`;
        await session.prompt("Exercise captured tools through fabric_exec.");
        const result = session.messages.findLast((m) => m.role === "toolResult" && m.toolName === "fabric_exec");
        assert.equal(result?.isError, false, JSON.stringify(result));
        assert.match(JSON.stringify(result.content), /redacted/);
        if (nativeMcp) assert.match(JSON.stringify(result.content), /mcp:native/);
        assert.match(JSON.stringify(result.content), /agentList/);
        assert.equal(calls, 2);
        assert.ok(api.getActiveTools().includes("fixture_deferred"), "native tool_search activated the deferred tool");
        assert.equal(typeof toolContext.executeTool, "function");
        assert.ok(toolContext.tools.some((t) => t.name === "fixture_echo"));
        assert.ok(events.some((e) => e.toolName === "fixture_echo" && e.parentToolCallId?.startsWith("outer-")));
        assert.ok(result.nestedCalls?.calls?.length >= (nativeMcp ? 3 : 2), JSON.stringify(result.nestedCalls));
        nextCode = 'return await extensions.fixture_prepare({value:"once"});';
        await session.prompt("Prepare captured arguments once, before approval and execution.");
        assert.equal(preparations, 1);
        assert.match(JSON.stringify(session.messages.findLast((m) => m.role === "toolResult")?.content), /once!/);
        nextCode = 'return await pi.bash({command:"printf shell-output; exit 7",settle:true});';
        await session.prompt("Settle native structured nonzero exit through Fabric.");
        const shellResult = session.messages.findLast((m) => m.role === "toolResult");
        assert.equal(shellResult?.isError, false, JSON.stringify(shellResult));
        assert.match(JSON.stringify(shellResult.content), /shell-output/);
        assert.match(JSON.stringify(shellResult.content), /exitCode[^0-9]*7/);
        nextCode = 'return await extensions.fixture_echo({value:"block"});';
        await session.prompt("Exercise native permission rejection.");
        assert.equal(calls, 2);
        assert.equal(session.messages.findLast((m) => m.role === "toolResult")?.isError, true);
        api.registerTool(fixtureTool("fixture_deferred", "hidden"));
        nextCode = 'return await extensions.fixture_deferred({value:"withdrawn"});';
        await session.prompt("Withdrawn tool must not execute.");
        assert.equal(calls, 2);
        assert.equal(session.messages.findLast((m) => m.role === "toolResult")?.isError, true);
      }
      if (fullCodeMode) {
        nextCode = 'return await extensions.fixture_echo({value:"mutate"});';
        await session.prompt("Reassert ownership after a mid-turn setActiveTools removes Fabric.");
        assert.equal(calls, 3);
        assert.equal(session.messages.findLast((m) => m.role === "toolResult")?.isError, false);
      }
      if (nativeMcp) {
        const discovery = profile === "native" ? "fabric.tools" : "tools";
        nextCode = `const descriptor = await ${discovery}.describe({ref:"mcp.fixture.echo"}); const result = await mcp.fixture.echo({value:"adapter"}); const aliases = ${fullCodeMode ? `await ${discovery}.list({provider:"extensions",query:"mcp__fixture__echo"})` : '[]'}; return {descriptor,result,aliases};`;
        await session.prompt("Verify the opt-in native MCP adapter preserves the Fabric surface.");
        const result = session.messages.findLast(m => m.role === "toolResult" && m.toolName === "fabric_exec");
        assert.equal(result?.isError, false, JSON.stringify(result));
        assert.match(JSON.stringify(result.content), /mcp:adapter/);
        assert.match(JSON.stringify(result.content), /Offline echo/);
        assert.ok(!JSON.stringify(result.content).includes("captured from"), "native aliases must not duplicate the catalog");
        assert.equal(result.nestedCalls?.calls?.filter(call => call.name === "mcp__fixture__echo").length, 1);
        const invocation = events.findLast(event => event.type === "tool_call" && event.toolName === "fabric_exec");
        assert.deepEqual(invocation.input.display, { name: "Verify compatibility", description: "Preserve Fabric invocation contracts" });
        assert.equal(result.details.trace.operations.filter(operation => operation.ref === "mcp.fixture.echo").length, 1);
        nextCode = 'return await mcp.fixture.echo({value:"native-redact"});';
        await session.prompt("Native content-only redaction must drop the original structured envelope.");
        const redacted = session.messages.findLast(m => m.role === "toolResult" && m.toolName === "fabric_exec");
        assert.equal(redacted?.isError, false, JSON.stringify(redacted));
        assert.match(JSON.stringify(redacted.content), /native redacted/);
        assert.ok(!JSON.stringify(redacted.content).includes("mcp:native-redact"));
        nextCode = 'return await mcp.fixture.echo({value:"native-block"});';
        await session.prompt("Native permission denial must fail without fallback.");
        assert.equal(session.messages.findLast(m => m.role === "toolResult")?.isError, true);
      }
      if (fullCodeMode) {
        nextCode = 'return await pi.read({path:"fixture-path"});';
        await session.prompt("Core overrides stay captured across active-set changes.");
        assert.equal(overrides, 1);
        assert.match(JSON.stringify(session.messages.findLast(m => m.role === "toolResult")?.content), /override:fixture-path/);
      }
      if (profile === "native") {
        // Use a fresh direct registration: the earlier mutation deliberately
        // evicted fixture_echo from Fabric's capture catalog as well as Pi's
        // active set. Do not undo that removal just to test alias parity.
        api.registerTool(fixtureTool("fixture_alias"));
        nextCode = 'return {native:await tools.fixture_alias({value:"rewrite"}),fabric:await extensions.fixture_alias({value:"rewrite"})};';
        await session.prompt("Verify native and additive aliases use the same host dispatch and redaction.");
        const result = session.messages.findLast(m => m.role === "toolResult");
        assert.equal(result?.isError, false, JSON.stringify(result));
        assert.match(JSON.stringify(result.content), /redacted/);
        assert.equal(result.nestedCalls.calls.filter(call => call.name === "fixture_alias").length, 2);
      }
      const oldRunner = session.extensionRunner;
      await session.reload();
      assert.notEqual(session.extensionRunner, oldRunner);
      assert.notEqual(host.ExtensionRunner.prototype.getAllRegisteredTools, originalCaptureMethod);
      assert.notEqual(host.ExtensionRunner.prototype.getAllRegisteredTools, firstCaptureMethod, "reload restored and reinstalled capture without wrapping the old layer");
      if (nativeMcp) nextCode = 'return await mcp.fixture.echo({value:"after-reload"});';
      await session.prompt("Verify reload retains the selected loadout and native ownership.");
      if (nativeMcp) {
        const result = session.messages.findLast(m => m.role === "toolResult");
        assert.equal(result?.isError, false, JSON.stringify(result));
        assert.match(JSON.stringify(result.content), /mcp:after-reload/);
      }
      assert.deepEqual(errors, []);
      assert.ok(requests.length >= 3);
      scenarios++;
    } finally {
      await session.extensionRunner.emit({ type: "session_shutdown", reason: "exit" });
      session.dispose();
      assert.equal(host.ExtensionRunner.prototype.getAllRegisteredTools, originalCaptureMethod, "shutdown restores the SDK prototype");
    }
  }
  await writeFile(path.join(scratch, "fabric.json"), JSON.stringify({ fullCodeMode: true, approvals: { read: "allow", write: "allow", execute: "allow" }, mcp: { enabled: false }, mesh: { enabled: false }, memory: { enabled: false }, agents: { enabled: false }, entropy: { compile: false }, ui: { enabled: false } }));
  const cli = path.resolve(path.dirname(hostEntry), "..", hostManifest.bin.pi);
  // smarty-dev#11350: fixture_cli is a direct tool that fabric_exec calls; the --tools allowlist must keep it, or
  // Pi filters it out and the hidden-tool contract (correctly) denies the call.

  const cliRun = promisify(execFile)(process.execPath, [cli, "--mode", "json", "--no-session", "--no-extensions", "-e", path.join(root, "dist/index.js"), "-e", path.join(root, "tests/fixtures/pi1-cli-extension.mjs"), "-e", "builtin:codemode", "-e", "builtin:tool-search", "--tools", "read,bash,fabric_exec,codemode,tool_search,fixture_cli,fixture_cli_start", "--provider", "offline-cli", "--model", "fixture", "-p", "Offline smoke"], { cwd: scratch, timeout: 30_000, maxBuffer: 4_000_000, env: process.env });
  // Print mode reads piped stdin before starting; close it (no fixture input).
  cliRun.child.stdin.end();
  const { stdout, stderr } = await cliRun;
  assert.ok(!stderr.includes("Failed to load extension"), stderr);
  const cliEvents = stdout.split("\n").filter((line) => line.startsWith("{")).map((line) => JSON.parse(line));
  const cliResult = cliEvents.find(event => event.type === "tool_execution_end" && event.toolName === "fabric_exec");
  assert.equal(cliResult?.isError, false, JSON.stringify(cliResult) + "\n" + stderr);
  assert.match(JSON.stringify(cliResult.result), /cli-capture-ok/);
  assert.match(stdout.slice(-4000), /cli-smoke-ok/);
  await writeFile(path.join(scratch, "fabric.json"), JSON.stringify({
    fullCodeMode: true,
    approvals: { read: "allow", write: "allow", execute: "allow" },
    agents: { enabled: false }, mcp: { enabled: false }, mesh: { enabled: false },
    memory: { enabled: false }, entropy: { enabled: false, compile: false }, ui: { enabled: false },
  }));
  const rpcProviderPath = path.join(scratch, "pi1-rpc-provider.mjs");
  await writeFile(rpcProviderPath, `
import { createAssistantMessageEventStream, getCurrentTools } from ${JSON.stringify(import.meta.resolve("@earendil-works/pi-ai"))};
export default function (pi) {
  let calls = 0;
  pi.registerProvider("offline-pi1-rpc", {
    api: "offline-pi1-rpc", apiKey: "offline-fixture", baseUrl: "http://invalid.local",
    models: [{ id: "fixture", name: "Offline RPC fixture", reasoning: false, input: ["text"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 128000, maxTokens: 2048 }],
    streamSimple(model, context) {
      const declared = getCurrentTools(context.messages).map(tool => tool.name);
      if (!declared.includes("fabric_exec")) throw new Error("fabric_exec is not declared in Pi RPC mode");
      const toolCall = calls++ === 0;
      const message = {
        role: "assistant",
        content: toolCall ? [{ type: "toolCall", id: "rpc-fabric-main", name: "fabric_exec", arguments: { code: "return await agents.main();", display: { name: "Pi 1.2 RPC smoke", description: "Return the Fabric Main agent through fabric_exec." } } }] : [{ type: "text", text: "rpc-smoke-ok" }],
        api: model.api, provider: model.provider, model: model.id,
        usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, totalTokens: 2, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
        stopReason: "pending", timestamp: Date.now(),
      };
      const stream = createAssistantMessageEventStream();
      stream.push({ type: "start", partial: message });
      message.stopReason = toolCall ? "toolUse" : "stop";
      stream.push({ type: "done", reason: message.stopReason, message });
      stream.end();
      return stream;
    },
  });
}
`);
  const rpcClient = new host.RpcClient({
    cliPath: cli,
    cwd: scratch,
    provider: "offline-pi1-rpc",
    model: "fixture",
    args: [
      "--no-session", "--no-extensions", "--offline",
      "-e", path.join(root, "dist/index.js"), "-e", rpcProviderPath,
      "-e", "builtin:codemode", "-e", "builtin:tool-search",
      "--no-skills", "--no-prompt-templates", "--no-themes", "--no-context-files",
      "--tools", "read,bash,fabric_exec,codemode,tool_search",
    ],
    env: { PI_CODING_AGENT_DIR: scratch, PI_FABRIC_AGENT_DIR: path.join(scratch, "exports") },
  });
  try {
    await rpcClient.start();
    const rpcState = await rpcClient.getState();
    assert.equal(rpcState.isIdle, true);
    assert.equal(rpcState.model?.provider, "offline-pi1-rpc");
    const rpcEvents = await rpcClient.promptAndWait("Run one Fabric RPC smoke call.", undefined, 30_000);
    const rpcFabricCall = rpcEvents.find(event => event.type === "tool_execution_end" && event.toolName === "fabric_exec");
    assert.ok(rpcFabricCall, JSON.stringify(rpcEvents));
    assert.equal(rpcFabricCall.isError, false, JSON.stringify(rpcFabricCall));
    const rpcResult = JSON.stringify(rpcFabricCall.result);
    assert.match(rpcResult, /\"kind\":\"main\"/);
    assert.match(rpcResult, /\"runner\":\"pi\"/);
    assert.ok(rpcEvents.some(event => event.type === "agent_settled" && event.outcome === "completed"));
    console.log(JSON.stringify({
      version: host.VERSION, hostEntry, scenarios, mcpSkipped, exclusive: true, proxy: true, reload: true, cli: true,
      rpc: { command: "pi --mode rpc", get_state: { sessionId: rpcState.sessionId, isIdle: rpcState.isIdle, model: rpcState.model }, fabric_exec: rpcFabricCall.result },
    }));
  } finally {
    await rpcClient.stop();
  }
} finally { await rm(scratch, { recursive: true, force: true }); }
