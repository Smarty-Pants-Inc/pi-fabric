#!/usr/bin/env node
// Real native Pi Main -> Fabric fabric_exec -> agents.spawn -> built worker -> real native Pi.
// Only inference is substituted, by a loopback OpenAI-compatible provider. No credentials.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import http from "node:http";
import assert from "node:assert/strict";
import { spawn, execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const repo = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const out = process.env.TASK_OUT;
assert(out, "TASK_OUT is required"); fs.mkdirSync(out, { recursive: true });
const root = fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "native-spawn-release-"));
const transcript = path.join(out, "native-proof.jsonl"); fs.writeFileSync(transcript, "");
const emit = record => fs.appendFileSync(transcript, JSON.stringify(record) + "\n");
const markerFile = path.join(root, "child-origin.jsonl");
const sdk = fs.realpathSync(process.env.PI_FABRIC_PROOF_PI ?? path.join(repo, "node_modules/@earendil-works/pi-coding-agent/dist/cli.js"));
const profile = path.join(root, "profile"); fs.mkdirSync(profile);
const settingsFile = path.join(profile, "settings.json");
const manifest = JSON.parse(fs.readFileSync(path.join(repo, "package.json"), "utf8"));
const release = name => {
  const dir = path.join(root, name); fs.mkdirSync(dir);
  fs.cpSync(path.join(repo, "dist"), path.join(dir, "dist"), { recursive: true });
  fs.writeFileSync(path.join(dir, "package.json"), JSON.stringify(manifest));
  fs.symlinkSync(path.join(repo, "node_modules"), path.join(dir, "node_modules"), "junction");
  fs.symlinkSync(path.join(repo, "skillsets"), path.join(dir, "skillsets"), "junction");
  for (const [file, kind] of [["worker.js", "worker"], ["index.js", "extension"]]) {
    // Instrumentation only: no factory or worker logic is replaced.
    fs.appendFileSync(path.join(dir, "dist", file), `\nimport fsProof from 'node:fs';\nconst proofOrigin = { type: 'release_origin', kind: ${JSON.stringify(kind)}, marker: ${JSON.stringify(name)}, module: import.meta.url, pid: process.pid, ppid: process.ppid, argv: process.argv, runDir: process.env.PI_FABRIC_AGENT_RUN_DIR ?? null, agentDir: process.env.PI_CODING_AGENT_DIR };\nfsProof.appendFileSync(${JSON.stringify(markerFile)}, JSON.stringify(proofOrigin) + '\\n');\nprocess.stdout.write(JSON.stringify(proofOrigin) + '\\n');\n`);
  }
  return dir;
};
const parent = release("old-main-A"); const selected = release("activated-compatible-B"); const incompatible = release("activated-incompatible-C");
const protocol = JSON.parse(fs.readFileSync(path.join(incompatible, "dist/worker-protocol.json"), "utf8"));
fs.writeFileSync(path.join(incompatible, "dist/worker-protocol.json"), JSON.stringify({ version: protocol.version + 1 }));
// Exercise actual source-checkout ancestry, not just sibling release fixtures.
const checkout = path.join(root, "source-checkout"); fs.mkdirSync(checkout);
execFileSync("tar", ["-x", "-C", checkout], { input: execFileSync("git", ["archive", "HEAD"], { cwd: repo, maxBuffer: 64 * 1024 * 1024 }) });
const cwd = path.join(checkout, "workspace"); fs.mkdirSync(cwd);
const projectHook = path.join(cwd, ".pi/extensions/caller-hook.js");
const callerHook = path.join(checkout, "explicit-caller-hook.mjs");
const rejectedCheckout = path.join(checkout, "dist/index.js");
fs.mkdirSync(path.dirname(projectHook), { recursive: true }); fs.mkdirSync(path.dirname(rejectedCheckout), { recursive: true });
for (const [file, type] of [[projectHook, "project_resource"], [callerHook, "caller_resource"], [rejectedCheckout, "rejected_checkout_fabric"]]) {
  fs.writeFileSync(file, `import fs from 'node:fs'; export default function() { const row = { type: ${JSON.stringify(type)}, module: import.meta.url, pid: process.pid, runDir: process.env.PI_FABRIC_AGENT_RUN_DIR ?? null }; fs.appendFileSync(${JSON.stringify(markerFile)}, JSON.stringify(row)+'\\n'); console.log(JSON.stringify(row)); }`);
}
const otherHook = path.join(root, "other-extension.mjs");
fs.writeFileSync(otherHook, `import fs from 'node:fs'; export default function() { const row = { type: 'other_resource', pid: process.pid, runDir: process.env.PI_FABRIC_AGENT_RUN_DIR ?? null }; fs.appendFileSync(${JSON.stringify(markerFile)}, JSON.stringify(row)+'\\n'); console.log(JSON.stringify(row)); }`);
const select = dir => fs.writeFileSync(settingsFile, JSON.stringify({ packages: [dir], extensions: [otherHook, callerHook, ...(dir === parent ? [] : [rejectedCheckout])], defaultProjectTrust: "always", enableInstallTelemetry: false, compaction: { enabled: false }, retry: { enabled: false } }));
select(parent);
fs.writeFileSync(path.join(profile, "fabric.json"), JSON.stringify({ autoReload: false, mcp: { enabled: false }, mesh: { enabled: false }, agents: { nice: 19, retainRuns: true, sessionExport: false, notifyOnComplete: false, timeoutMs: 45000 }, executor: { kernel: "typescript" } }));
let pendingCall;
const server = http.createServer((request, response) => {
  let body = ""; request.on("data", chunk => { body += chunk; }); request.on("end", () => {
    const data = JSON.parse(body);
    emit({ type: "provider_request", model: data.model, messages: data.messages.length });
    response.writeHead(200, { "Content-Type": "text/event-stream" });
    const chunk = (delta, finish_reason = null) => response.write(`data: ${JSON.stringify({ id: "offline", object: "chat.completion.chunk", created: 1, model: data.model, choices: [{ index: 0, delta, finish_reason }] })}\n\n`);
    if (data.model === "main" && pendingCall) {
      const call = pendingCall; pendingCall = undefined;
      chunk({ role: "assistant", tool_calls: [{ index: 0, id: "proof-tool", type: "function", function: { name: "fabric_exec", arguments: JSON.stringify(call) } }] }); chunk({}, "tool_calls");
    } else { chunk({ role: "assistant", content: data.model === "child" ? "real native child completed" : "Main proof completed" }); chunk({}, "stop"); }
    response.end("data: [DONE]\n\n");
  });
});
let main;
let mainClosed;
const waiters = [];
const records = [];
const waitFor = (predicate, timeout = 60000) => {
  const known = records.find(predicate); if (known) return Promise.resolve(known);
  return new Promise((resolve, reject) => { const entry = { predicate, resolve: row => { clearTimeout(timer); resolve(row); } }; const timer = setTimeout(() => { waiters.splice(waiters.indexOf(entry), 1); reject(new Error("Native Main observation timed out")); }, timeout); waiters.push(entry); });
};
try {
  await new Promise(resolve => server.listen(0, "127.0.0.1", resolve));
  const model = id => ({ id, name: id, reasoning: false, input: ["text"], contextWindow: 64000, maxTokens: 4096, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } });
  fs.writeFileSync(path.join(profile, "models.json"), JSON.stringify({ providers: { "release-proof": { baseUrl: `http://127.0.0.1:${server.address().port}/v1`, apiKey: "offline-only", api: "openai-completions", models: [model("main"), model("child")] } } }));
  emit({ type: "command", command: `PI_FABRIC_PROOF_PI=${JSON.stringify(sdk)} nice -n 19 node scripts/prove-spawn-release.mjs`, head: execFileSync("git", ["rev-parse", "HEAD"], { cwd: repo, encoding: "utf8" }).trim(), sdk, parent, selected, incompatible, checkout, cwd, checkoutSource: "git archive HEAD", instrumentation: "Exact built dist copies, appended origin-print markers only; Main autoReload disabled" });
  main = spawn(process.execPath, [sdk, "--mode", "rpc", "--no-session", "--model", "release-proof/main", "-e", path.join(parent, "dist/index.js"), "-e", callerHook], { cwd, env: { ...process.env, PI_CODING_AGENT_DIR: profile, PI_OFFLINE: "1", PI_FABRIC_PI_BINARY: sdk, PI_FABRIC_RUN_ROOT: path.join(root, "runs") }, stdio: ["pipe", "pipe", "pipe"] });
  mainClosed = new Promise(resolve => main.on("close", (code, signal) => { emit({ type: "main_closed", code, signal }); resolve(); }));
  let buffer = "";
  main.stdout.on("data", data => { buffer += data; while (buffer.includes("\n")) { const pos = buffer.indexOf("\n"); const line = buffer.slice(0, pos); buffer = buffer.slice(pos + 1); let row; try { row = JSON.parse(line); } catch { emit({ type: "main_stdout", line }); continue; } emit({ type: "native_main_event", event: row }); records.push(row); for (const waiter of [...waiters]) if (waiter.predicate(row)) { waiters.splice(waiters.indexOf(waiter), 1); waiter.resolve(row); } } });
  const warnings = [];
  main.stderr.on("data", data => { const text = data.toString(); emit({ type: "main_stderr", text }); for (const line of text.split("\n")) if (line.includes("Installed Fabric release")) warnings.push(line); });
  main.stdin.write(JSON.stringify({ id: "ready", type: "get_state" }) + "\n");
  await waitFor(row => row.type === "response" && row.id === "ready");
  const initial = fs.readFileSync(markerFile, "utf8").trim().split("\n").map(JSON.parse);
  assert(initial.some(row => row.kind === "extension" && row.marker === "old-main-A" && row.pid === main.pid));
  emit({ type: "old_main_loaded", pid: main.pid, origins: initial });
  for (const [phase, activated, extensions, expected] of [["compatible", selected, true, selected], ["incompatible", incompatible, true, parent], ["extensions-false", selected, false, selected]]) {
    select(activated); emit({ type: "activated_selector", phase, settings: JSON.parse(fs.readFileSync(settingsFile, "utf8")), stillRunningMain: main.pid });
    const resultFile = path.join(root, phase + "-result.json");
    const before = records.length; const warnsBefore = warnings.length;
    pendingCall = { code: `const handle = await agents.spawn({task: "Native release proof ${phase}", model: "release-proof/child", transport: "process", extensions: ${extensions}, tools: []}); const result = await agents.wait({id: handle.id, timeoutMs: 45000}); await pi.write({path: π.resultFile, text: JSON.stringify({handle, result})}); return {handle, result};`, payloads: { resultFile }, resultFormat: "json" };
    emit({ type: "main_tool_command", phase, ...pendingCall });
    main.stdin.write(JSON.stringify({ id: phase, type: "prompt", message: "Native Main release proof " + phase }) + "\n");
    await waitFor(row => records.indexOf(row) >= before && row.type === "agent_end");
    assert(fs.existsSync(resultFile), `Fabric tool did not produce ${phase} result`);
    const value = JSON.parse(fs.readFileSync(resultFile, "utf8"));
    assert.equal(value.result.status, "completed", value.result.error);
    assert.equal(value.handle.fabricRelease, expected); assert.equal(value.result.fabricRelease, expected);
    const run = path.dirname(value.result.logFile);
    const status = JSON.parse(fs.readFileSync(path.join(run, "status.json"), "utf8")); assert.equal(status.fabricRelease, expected);
    const origins = fs.readFileSync(markerFile, "utf8").trim().split("\n").map(JSON.parse);
    const worker = origins.find(row => row.kind === "worker" && row.pid === Number(value.result.sessionId));
    assert(worker, "No child-worker-origin evidence");
    assert.equal(fileURLToPath(worker.module), path.join(expected, "dist/worker.js"));
    assert.equal(worker.argv[1], path.join(expected, "dist/worker.js"));
    assert.equal(worker.marker, path.basename(expected));
    assert.equal(worker.argv[worker.argv.indexOf("--fabric-release") + 1], expected);
    const loaded = origins.filter(row => row.kind === "extension" && row.runDir === run);
    if (extensions) { assert(loaded.length > 0); assert(loaded.every(row => fileURLToPath(row.module) === path.join(expected, "dist/index.js"))); assert(origins.some(row => row.type === "other_resource" && row.runDir === run)); assert(origins.some(row => row.type === "project_resource" && row.runDir === run), "Authorized project hook in real checkout ancestry must load"); assert(origins.some(row => row.type === "caller_resource" && row.runDir === run), "Authorized caller hook in real checkout ancestry must load"); }
    else assert.equal(loaded.length, 0);
    fs.cpSync(run, path.join(out, "native-" + phase + "-run"), { recursive: true });
    // Native RPC redirects extension stdout to stderr before it owns stdout.
    const printed = fs.readFileSync(value.result.logFile, "utf8").trim().split("\n").map(JSON.parse).flatMap(row => {
      if (row.type === "release_origin") return [row];
      if (row.type !== "worker_stderr") return [];
      return row.text.trim().split("\n").flatMap(line => { try { const mark = JSON.parse(line); return mark.type === "release_origin" ? [mark] : []; } catch { return []; } });
    });
    if (extensions) assert(printed.some(row => row.kind === "extension" && row.marker === path.basename(expected) && fileURLToPath(row.module) === path.join(expected, "dist/index.js") && row.argv.includes(path.join(expected, "dist/index.js"))), "Child must print its actual selected extension path, argv and marker");
    assert.equal(warnings.length - warnsBefore, phase === "incompatible" ? 1 : 0);
    if (phase === "incompatible") { assert(warnings[warnsBefore].includes(activated)); assert(warnings[warnsBefore].includes(expected)); assert(warnings[warnsBefore].includes("worker protocol")); }
    assert(!origins.some(row => row.kind === "extension" && row.marker === "activated-incompatible-C"));
    assert(!origins.some(row => row.type === "rejected_checkout_fabric"), "Rejected Fabric checkout entrypoint must never execute");
    emit({ type: "phase_pass", phase, oldMainPid: main.pid, activated, expected, handle: value.handle, result: value.result, onDisk: status, workerOrigin: worker, loadedExtensions: loaded, authorizedCheckoutHooks: origins.filter(row => ["project_resource", "caller_resource"].includes(row.type) && row.runDir === run), childPrintedMarkers: printed, warnings: warnings.slice(warnsBefore) });
    const deadline = Date.now() + 10000;
    while (Date.now() < deadline) { try { process.kill(Number(value.result.sessionId), 0); } catch { break; } await new Promise(resolve => setTimeout(resolve, 50)); }
    assert.throws(() => process.kill(Number(value.result.sessionId), 0), "Worker must be joined before handing evidence back");
  }
  emit({ type: "proof_pass", phases: 3, realPiMain: true, realPiChildren: true, credentials: "none; isolated offline loopback provider" });
  console.log(JSON.stringify({ passed: true, transcript }));
} finally {
  if (main && main.exitCode === null && main.signalCode === null) main.kill("SIGTERM");
  if (mainClosed) await mainClosed;
  await new Promise(resolve => server.close(resolve));
  fs.rmSync(root, { recursive: true, force: true });
}
