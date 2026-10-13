#!/usr/bin/env node
// Native offline proof: real Pi root -> public agents.spawn/wait -> process worker -> native write/edit/bash.
// Only model inference is deterministic. No live credentials, shared mesh, deletes, installs or service control.
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import http from "node:http";
import crypto from "node:crypto";
import { spawn } from "node:child_process";

const option = name => {
  const index = process.argv.indexOf(name);
  assert(index >= 0 && process.argv[index + 1], `Required ${name}`);
  return path.resolve(process.argv[index + 1]);
};
const fabricRoot = fs.realpathSync(option("--fabric-root"));
const workspace = fs.realpathSync(option("--workspace"));
const requestedEvidence = option("--evidence-dir");
const evidenceParent = path.dirname(requestedEvidence);
for (let component = evidenceParent; ; component = path.dirname(component)) {
  assert(!fs.lstatSync(component).isSymbolicLink(), "Evidence path must not contain symlinked components");
  if (path.dirname(component) === component) break;
}
const evidence = path.join(fs.realpathSync(evidenceParent), path.basename(requestedEvidence));
const evidenceRelative = path.relative(workspace, evidence);
assert(evidenceRelative && evidenceRelative !== ".." && !evidenceRelative.startsWith(`..${path.sep}`) && !path.isAbsolute(evidenceRelative),
  "Evidence writes must be inside the assigned worktree");
const nativePi = option("--pi-binary");
const extension = path.join(fabricRoot, "dist/index.js");
for (const file of [nativePi, extension, path.join(fabricRoot, "dist/worker.js")]) assert(fs.statSync(file).isFile(), `Missing ${file}`);
assert(process.env.TMPDIR, "Private TMPDIR required");
assert(!fs.existsSync(evidence), "Use a fresh evidence directory");
fs.mkdirSync(evidence);
assert.equal(fs.realpathSync(evidence), evidence, "Evidence directory must remain canonical");
const scratch = fs.mkdtempSync(path.join(process.env.TMPDIR, "task-write-native-"));
const agent = path.join(scratch, "agent"), rootCwd = path.join(scratch, "root"), runs = path.join(scratch, "runs");
for (const dir of [agent, rootCwd, runs, path.join(scratch, "home"), path.join(scratch, "tmp")]) fs.mkdirSync(dir);
const json = (file, value) => fs.writeFileSync(file, `${JSON.stringify(value, null, 2)}\n`);
const forbidden = "/tmp/fv2-writescope-probe"; // Explicit assignment's refused real write target; never delete it.
assert(!fs.existsSync(forbidden), "Refused target already exists; do not touch an existing fixture");
const outside = path.join(scratch, "outside-task.txt");
const shellOutside = path.join(scratch, "native-shell-outside-task.txt");
const cwdFile = path.join(evidence, "worktree-write.txt");

const toolResults = [], childRequests = [], parentEvents = [];
let child, server, parentExit, failure;
let tail = "", parentCalls = 0;
const guard = 180_000;
const bounded = (promise, label, ms = guard) => {
  let timer;
  return Promise.race([promise, new Promise((_, reject) => { timer = setTimeout(() => reject(new Error(`Timeout: ${label}`)), ms); })]).finally(() => clearTimeout(timer));
};
const sse = response => {
  response.writeHead(200, { "Content-Type": "text/event-stream" });
  const chunk = (delta, finish_reason = null) => response.write(`data: ${JSON.stringify({ id: "offline", object: "chat.completion.chunk", created: 1, model: "offline", choices: [{ index: 0, delta, finish_reason }] })}\n\n`);
  return {
    tools: calls => { chunk({ role: "assistant", tool_calls: calls.map(([name, args], index) => ({ index, id: `probe-${crypto.randomUUID()}`, type: "function", function: { name, arguments: JSON.stringify(args) } })) }); chunk({}, "tool_calls"); response.end("data: [DONE]\n\n"); },
    text: text => { chunk({ role: "assistant", content: text }); chunk({}, "stop"); response.end("data: [DONE]\n\n"); },
  };
};
const program = `const results = [];
for (const scenario of ["default", "readonly", "unrestricted"]) {
  const handle = await agents.spawn({ task: "SCOPE_PROBE:" + scenario, model: "task-write-proof/child", thinking: "off", runner: "pi", transport: "process", needs: ["local"], cwd: π.workspace, extensions: false, tools: ["write", "edit", "bash"], nice: 19,
    ...(scenario === "readonly" ? {readOnly: true} : scenario === "unrestricted" ? {writePolicy: "unrestricted"} : {}) });
  const result = await agents.wait({id: handle.id, timeoutMs: 180000});
  results.push({scenario, handle, result});
}
return results;`;
try {
  server = http.createServer((request, response) => {
    let body = "";
    request.on("data", chunk => { body += chunk; });
    request.on("end", () => {
      try {
        const payload = JSON.parse(body), stream = sse(response);
        if (payload.model === "parent") {
          if (++parentCalls === 1) stream.tools([["fabric_exec", { code: program, payloads: { workspace }, resultFormat: "json" }]]);
          else stream.text("TASK_WRITE_SCOPE_NATIVE_PROOF_COMPLETE");
          return;
        }
        assert.equal(payload.model, "child");
        const prompt = JSON.stringify(payload.messages);
        const scenario = ["default", "readonly", "unrestricted"].find(tag => prompt.includes(`SCOPE_PROBE:${tag}`));
        assert(scenario, "Child request must carry an exact scenario");
        const previous = childRequests.filter(item => item.scenario === scenario).length;
        childRequests.push({ scenario, model: payload.model, toolMessages: payload.messages.filter(message => message.role === "tool") });
        json(path.join(evidence, "child-requests.json"), childRequests);
        if (previous > 0) {
          toolResults.push({ scenario, messages: payload.messages.filter(message => message.role === "tool") });
          stream.text(`SCOPE_PROBE_COMPLETE:${scenario}`);
          return;
        }
        if (scenario === "default") {
          const runId = fs.readdirSync(runs).find(id => fs.existsSync(path.join(runs, id, "task.txt")) && fs.readFileSync(path.join(runs, id, "task.txt"), "utf8").includes("SCOPE_PROBE:default"));
          assert(runId, "Real worker run must exist before inference");
          const tmp = path.join(runs, runId, "tmp");
          stream.tools([
            ["write", { path: forbidden, content: "MUST_BE_REFUSED" }],
            ["write", { path: cwdFile, content: "WORKTREE_ALLOWED" }],
            ["edit", { path: cwdFile, edits: [{ oldText: "WORKTREE_ALLOWED", newText: "WORKTREE_EDIT_ALLOWED" }] }],
            ["write", { path: path.join(tmp, "scratch-write.txt"), content: "SCRATCH_ALLOWED" }],
            ["bash", { command: `printf 'CHILD_TMPDIR=%s\\n' "$TMPDIR"; printf 'SHELL_GAP_ALLOWED' > "$TASK_WRITE_SCOPE_SHELL_OUTSIDE"`, timeout: 10 }],
          ]);
        } else if (scenario === "readonly") stream.tools([["write", { path: path.join(evidence, "readonly-refused.txt"), content: "MUST_BE_REFUSED" }]]);
        else stream.tools([["write", { path: outside, content: "UNRESTRICTED_ALLOWED" }]]);
      } catch (error) { failure ??= error; response.destroy(error); }
    });
  });
  await new Promise(resolve => server.listen(0, "127.0.0.1", resolve));
  const port = server.address().port;
  json(path.join(agent, "models.json"), { providers: { "task-write-proof": { baseUrl: `http://127.0.0.1:${port}/v1`, apiKey: "keyless-offline-fixture", api: "openai-completions", models: ["parent", "child"].map(id => ({ id, name: id, reasoning: false, input: ["text"], contextWindow: 128000, maxTokens: 4096, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } })) } } });
  json(path.join(agent, "settings.json"), { enableInstallTelemetry: false, compaction: { enabled: false }, retry: { enabled: false }, extensions: [], packages: [] });
  json(path.join(agent, "fabric.json"), { configVersion: 4, fullCodeMode: false, autoReload: false, executor: { kernel: "typescript", timeoutMs: guard, maxTimeoutMs: guard, resultFormat: "json", landlock: { mode: "off" } }, agents: { enabled: true, runner: "pi", transport: "process", timeoutMs: guard, maxDepth: 2, maxConcurrent: 2, extensions: false, retainRuns: true, sessionExport: false, notifyOnComplete: false, deniedModels: [], modelPolicy: { requireReason: [] } }, mesh: { enabled: true, root: path.join(scratch, "mesh"), announce: true, actorScope: "session" }, capture: { enabled: false }, mcp: { enabled: false }, jev: { enabled: false }, prewalk: { enabled: false }, ui: { enabled: false }, compaction: { engine: "pi" }, approvals: { read: "allow", write: "allow", execute: "allow", network: "allow", agent: "allow" } });
  const env = Object.fromEntries(Object.entries(process.env).filter(([key]) => !/^(PI_|SMARTY_|HERDR_|ORG_|OPENAI_|ANTHROPIC_|GOOGLE_|GEMINI_|OPENROUTER_|TYPESAFE_|AWS_|AZURE_|AI_GATEWAY_|OTEL_|SENTRY_)/.test(key)));
  Object.assign(env, { HOME: path.join(scratch, "home"), XDG_CONFIG_HOME: path.join(scratch, "home/.config"), XDG_DATA_HOME: path.join(scratch, "home/.local/share"), XDG_CACHE_HOME: path.join(scratch, "home/.cache"), TMPDIR: path.join(scratch, "tmp"), TASK_WRITE_SCOPE_SHELL_OUTSIDE: shellOutside, PI_CODING_AGENT_DIR: agent, PI_OFFLINE: "1", PI_SKIP_VERSION_CHECK: "1", PI_TELEMETRY: "0", PI_FABRIC_PI_BINARY: nativePi, PI_FABRIC_PROJECT_ROOT: rootCwd, PI_FABRIC_MESH_ROOT: path.join(scratch, "mesh"), PI_FABRIC_RUN_ROOT: runs, PI_FABRIC_AGENT_DIR: path.join(scratch, "exports") });
  child = spawn(process.execPath, [nativePi, "--mode", "rpc", "--no-session", "--provider", "task-write-proof", "--model", "parent", "--thinking", "off", "--no-context-files", "--no-skills", "--no-prompt-templates", "--no-themes", "--no-extensions", "--extension", extension, "--approve"], { cwd: rootCwd, env, stdio: ["pipe", "pipe", "pipe"] });
  parentExit = new Promise(resolve => child.once("close", (code, signal) => resolve({ code, signal })));
  const settled = new Promise((resolve, reject) => {
    child.once("error", reject);
    child.stdout.on("data", chunk => {
      fs.appendFileSync(path.join(evidence, "parent-events.jsonl"), chunk);
      tail += chunk;
      while (tail.includes("\n")) {
        const index = tail.indexOf("\n"), line = tail.slice(0, index); tail = tail.slice(index + 1);
        if (!line.trim()) continue;
        try { const event = JSON.parse(line); parentEvents.push(event); if (event.type === "agent_settled") resolve(); }
        catch (error) { reject(error); }
      }
    });
  });
  child.stderr.on("data", chunk => fs.appendFileSync(path.join(evidence, "parent-stderr.log"), chunk));
  child.stdin.write(`${JSON.stringify({ id: "proof", type: "prompt", message: "Run the exact keyless native task write-scope proof." })}\n`);
  await bounded(Promise.race([settled, parentExit.then(exit => { throw new Error(`Parent exited before settle: ${JSON.stringify(exit)}`); })]), "real root settled");
  if (failure) throw failure;
  const ends = parentEvents.filter(event => event.type === "tool_execution_end" && event.toolName === "fabric_exec");
  assert.equal(ends.length, 1);
  assert(!ends[0].isError, JSON.stringify(ends[0]));
  const result = ends[0].result.details?.result ?? JSON.parse(ends[0].result.content.filter(part => part.type === "text").map(part => part.text).join("\n"));
  assert(Array.isArray(result) && result.length === 3, JSON.stringify(result));
  const records = result.map(item => ({ scenario: item.scenario, result: item.result,
    status: JSON.parse(fs.readFileSync(path.join(runs, item.handle.id, "status.json"), "utf8")),
    events: fs.readFileSync(path.join(runs, item.handle.id, "events.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line)),
  }));
  assert(records.every(item => item.result.status === "completed"), JSON.stringify(records));
  assert(!fs.existsSync(forbidden), "Default write escaped its roots");
  assert.equal(fs.readFileSync(cwdFile, "utf8"), "WORKTREE_EDIT_ALLOWED");
  const defaultRun = records.find(item => item.scenario === "default");
  const tmp = path.join(runs, defaultRun.result.id, "tmp");
  assert.equal(fs.readFileSync(path.join(tmp, "scratch-write.txt"), "utf8"), "SCRATCH_ALLOWED");
  const defaultTools = toolResults.find(item => item.scenario === "default").messages;
  assert(defaultTools.some(message => JSON.stringify(message).includes("outside writable roots")), "Native write must record its refusal, not merely skip the attempt");
  assert(defaultTools.some(message => JSON.stringify(message).includes(`CHILD_TMPDIR=${tmp}`)), "Native child must receive its own TMPDIR");
  assert(!fs.existsSync(path.join(evidence, "readonly-refused.txt")));
  assert(toolResults.find(item => item.scenario === "readonly").messages.some(message => JSON.stringify(message).includes("read-only")));
  assert.equal(fs.readFileSync(outside, "utf8"), "UNRESTRICTED_ALLOWED");
  assert.equal(fs.readFileSync(shellOutside, "utf8"), "SHELL_GAP_ALLOWED"); // Main-owned scratch only: prove the gap without a shared write.
  assert.equal(defaultRun.events.filter(event => event.type === "write_policy.default").length, 1);
  assert.equal(records.find(item => item.scenario === "unrestricted").events.filter(event => event.type === "write_policy.unrestricted").length, 1);
  json(path.join(evidence, "records.json"), records);
  json(path.join(evidence, "tool-results.json"), toolResults);
  child.stdin.end();
  assert.deepEqual(await bounded(parentExit, "root native EXITED", 15000), { code: 0, signal: null });
  child = undefined;
  const summary = { passed: true, route: "real native Pi RPC root -> public Fabric agents.spawn/wait -> compiled process worker -> real native Pi write/edit/bash", workspace, fabricRoot, nativePi, scratch,
    refused: forbidden, worktreeWriteAndEdit: "allowed", childScratchWrite: "allowed", explicitReadOnly: "refused", unrestrictedOutsideWrite: "allowed in proof-owned scratch", optOutLogCount: 1,
    nativeBashGap: "allowed outside child roots, but inside proof-owned scratch; Landlock deliberately off", parentNativeExit: 0,
    artifacts: Object.fromEntries([extension, path.join(fabricRoot, "dist/worker.js")].map(file => [file, crypto.createHash("sha256").update(fs.readFileSync(file)).digest("hex")])),
  };
  json(path.join(evidence, "summary.json"), summary);
  console.log(JSON.stringify(summary, null, 2));
} catch (error) {
  failure ??= error;
  json(path.join(evidence, "failure.json"), { error: failure.stack, scratch, toolResults });
  console.error(failure.stack);
  process.exitCode = 1;
} finally {
  if (child) {
    child.stdin.end();
    try { await bounded(parentExit, "failed proof native close", 15000); }
    catch { child.kill("SIGTERM"); await bounded(parentExit, "owned root TERM close", 15000); }
  }
  if (server) { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); }
  // Retain proof-owned scratch. The allocating Main decides cleanup after reading evidence.
}
