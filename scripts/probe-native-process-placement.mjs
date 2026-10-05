#!/usr/bin/env node
// Opt-in integration proof: REAL fleet launcher + installed Pi + built Fabric.
// Never run in the test suite. The caller Main is offline; only the launcher
// starts one real inference task with the work host's existing native profile.
// No authentication files are read/copied. Optional host model metadata is
// symlinked into scratch only; it is never copied into the kept evidence.
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { execFileSync } from "node:child_process";
import { pathToFileURL } from "node:url";
import { assertMainProofCaller, assertWorkHostRoutes, linkProofModels } from "./lib/native-placement-proof.mjs";

const [piPackageInput, outputInput, launcherInput, host, mapInput, mode] = process.argv.slice(2);
assert(piPackageInput && outputInput && launcherInput && host && mapInput && ["local", "ssh"].includes(mode),
  "usage: probe-native-process-placement.mjs PI_PACKAGE OUTPUT_DIR REAL_LAUNCHER HOST WORK_HOSTS_JSON local|ssh");
for (const input of [piPackageInput, launcherInput, mapInput]) {
  assert(path.isAbsolute(input) && fs.existsSync(input), `BLOCKED: missing absolute local input ${input}`);
}
const piPackage = fs.realpathSync(piPackageInput), launcher = fs.realpathSync(launcherInput);
const aliases = JSON.parse(fs.readFileSync(mapInput, "utf8"));
assertWorkHostRoutes(aliases);
assert(/^[A-Za-z0-9][A-Za-z0-9._-]*$/.test(host) && /^[A-Za-z0-9][A-Za-z0-9._-]*$/.test(aliases[host] ?? ""), "host must have a safe work-hosts.json alias");
assert(mode !== "local" || os.hostname().split(".")[0] === host, "local receipts require the selected work host");
if (mode === "ssh") assertMainProofCaller(os.hostname(), aliases);
assert(fs.existsSync(path.join(piPackage, "dist/index.js")), "installed Pi SDK missing");
const extension = path.resolve("dist/index.js");
assert(fs.existsSync(extension), "fresh built extension required");
fs.accessSync(launcher, fs.constants.X_OK);
process.umask(0o077);
const output = path.resolve(outputInput);
fs.mkdirSync(output, { recursive: true, mode: 0o700 });
const hostProfile = process.env.PI_CODING_AGENT_DIR ?? path.join(os.homedir(), ".pi", "agent");
const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "native-placement-proof-"));
const profile = path.join(scratch, "profile"), cwd = path.join(scratch, "cwd");
fs.mkdirSync(profile); fs.mkdirSync(cwd);
execFileSync("git", ["init", "--quiet", cwd]);
fs.writeFileSync(path.join(cwd, "proof.txt"), "Public tiny task packet; contains no credentials or private inputs.\n");
execFileSync("git", ["-C", cwd, "add", "proof.txt"]);
const head = execFileSync("git", ["rev-parse", "HEAD"], { encoding: "utf8" }).trim();
const dirty = execFileSync("git", ["diff", "HEAD", "--", "src", "scripts", "docs", "tests"], { encoding: "utf8" }).length > 0;
const task = "Run hostname in the foreground using your bash tool, then reply with only the exact hostname. Do not spawn agents, use SSH, edit files, access credentials, or leave any work running.";
const nativeRoot = "/srv/scratch/paul/tasks/direct";
const reader = "python3 -c 'import json,pathlib,sys; p=pathlib.Path(\"/srv/scratch/paul/tasks/direct\")/sys.argv[1]; rc=p/\"rc\"; print(json.dumps({\"rc\":rc.read_text().strip(),\"text\":(p/\"result.md\").read_text(),\"stderr\":(p/\"stderr.log\").read_text() if (p/\"stderr.log\").exists() else \"\"} if rc.exists() else {\"rc\":None}))' {id}";
const placement = {
  default: "remote", capabilities: [], sshAliases: aliases,
  command: [launcher, "{id}", "--host", host, "--minutes", "{minutes}", "--src", "{cwd}", "--model", "{model}", "--thinking", "{thinking}", "--", "{task}"],
  ...(mode === "local" ? { resultDirectory: nativeRoot + "/{id}" } : { resultCommand: ["/usr/bin/ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "{sshAlias}", reader] }),
  cancelCommand: [launcher, "--host", "{host}", "--cancel", "{id}"], pollIntervalMs: 1000, commandTimeoutMs: 30000,
};
// Keep HOME unchanged: the real launcher resolves its factory installation
// through it. agentDir/settings/credentials are explicitly isolated below;
// capture the host profile before replacing PI_CODING_AGENT_DIR.
process.env.PI_CODING_AGENT_DIR = profile;
process.env.PI_FABRIC_TMPDIR = path.join(scratch, "tmp");
process.env.PI_FABRIC_RUNS_ROOT = path.join(scratch, "runs");
process.env.PI_OFFLINE = "1";
process.env.PI_FABRIC_ROLE = "main";
delete process.env.PI_MULTIPROVIDER_SESSION_PINS;
fs.writeFileSync(path.join(profile, "fabric.json"), JSON.stringify({
  executor: { kernel: "typescript", fullCodeMode: true }, mcp: { enabled: false }, ui: { enabled: false },
  mesh: { enabled: false, root: path.join(scratch, "mesh") },
  agents: { placement, timeoutMs: 180000, sessionExport: false, retainRuns: true, notifyOnComplete: false },
}));
const evidence = { head, dirty, callerHostname: os.hostname(), piPackage, extension, launcher, host, mode, aliases, isolatedAgentDir: profile, cwd, task, placement, passed: false };
fs.writeFileSync(path.join(output, "invocation.json"), JSON.stringify({ argv: process.argv, ...evidence }, null, 2));
let session;
try {
  const modelsPath = linkProofModels(profile, hostProfile, process.env.PROOF_MODELS_FROM_PROFILE === "1");
  const { createAgentSession, DefaultResourceLoader, ModelRuntime, SettingsManager, SessionManager } = await import(pathToFileURL(path.join(piPackage, "dist/index.js")).href);
  // This installed SDK does not re-export its empty in-memory storage helper.
  const { AuthStorage } = await import(pathToFileURL(path.join(piPackage, "dist/core/auth-storage.js")).href);
  const runtime = await ModelRuntime.create({ credentials: AuthStorage.inMemory(), modelsPath, refreshOnCreate: false, allowModelNetwork: false });
  // Metadata only: the public inert literal satisfies Pi availability checks.
  // It is not a credential, never persisted, and never sent over the network.
  // The launcher uses the work host's existing profile, not this caller value.
  if (!modelsPath) runtime.registerProvider("cliproxyapi", { api: "openai-completions", baseUrl: "http://invalid.invalid", apiKey: "offline-main-metadata-not-a-credential", models: [{ id: "gpt-6.1-sol", name: "Native placement metadata", reasoning: true, input: ["text"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 100000, maxTokens: 1000 }] });
  const settingsManager = SettingsManager.inMemory({ packages: [], extensions: [], skills: [], promptTemplates: [], themes: [] });
  const loader = new DefaultResourceLoader({ cwd, agentDir: profile, settingsManager, additionalExtensionPaths: [extension], noSkills: true, noPromptTemplates: true, noThemes: true });
  await loader.reload();
  assert.equal(loader.getExtensions().errors.length, 0, JSON.stringify(loader.getExtensions().errors));
  ({ session } = await createAgentSession({ cwd, agentDir: profile, modelRuntime: runtime, model: runtime.getModel("cliproxyapi", "gpt-6.1-sol"), resourceLoader: loader, settingsManager, sessionManager: SessionManager.inMemory(cwd) }));
  await session.bindExtensions({ mode: "print", onError: error => { throw new Error(JSON.stringify(error)); } });
  const tool = session.agent.state.tools.find(tool => tool.name === "fabric_exec");
  assert(tool, "installed Pi must register built fabric_exec");
  const code = `const handle = await agents.spawn({task: ${JSON.stringify(task)}, transport: "process", model: "cliproxyapi/gpt-6.1-sol", thinking: "high"}); let result; for (let i = 0; i < 4; i++) { result = await agents.wait({id: handle.id, timeoutMs: 240000}); if (!result.waitTimedOut) break; } return {handle, result};`;
  fs.writeFileSync(path.join(output, "fabric-program.ts"), code + "\n");
  const result = await tool.execute("native-placement-proof", { code, resultFormat: "json" }, new AbortController().signal);
  fs.writeFileSync(path.join(output, "fabric-exec-result.json"), JSON.stringify(result, null, 2));
  const text = result.content.filter(block => block.type === "text").map(block => block.text).join("\n");
  fs.writeFileSync(path.join(output, "fabric-exec-result.txt"), text + "\n");
  assert(!result.isError, text);
  const value = JSON.parse(text);
  evidence.handle = value.handle; evidence.result = value.result;
  // Preserve the manager's native transport receipt and full audit before
  // session_shutdown. Neither partial text nor a fake receipt counts as proof.
  const files = [];
  const walk = directory => { if (!fs.existsSync(directory)) return; for (const entry of fs.readdirSync(directory, { withFileTypes: true })) { const file = path.join(directory, entry.name); if (entry.isDirectory()) walk(file); else if (["placement.json", "events.jsonl", "status.json", "unresolved-worker.json"].includes(entry.name)) files.push(file); } };
  walk(path.join(scratch, "runs"));
  // Current Pi may select a custody run root under PI_FABRIC_TMPDIR instead
  // of the requested runs root. Follow the returned exact run, not a guess.
  if (value.result.logFile) {
    const directory = path.resolve(path.dirname(value.result.logFile));
    assert(directory.startsWith(scratch + path.sep), "proof run escaped its isolated scratch");
    walk(directory);
  }
  for (const [index, file] of files.entries()) fs.copyFileSync(file, path.join(output, `${index}-${path.basename(file)}`));
  const acceptanceFile = files.find(file => path.basename(file) === "placement.json" && JSON.parse(fs.readFileSync(file, "utf8")).id === value.handle.id);
  if (acceptanceFile) evidence.acceptance = JSON.parse(fs.readFileSync(acceptanceFile, "utf8"));
  if (mode === "local") {
    const native = path.join(nativeRoot, value.handle.id);
    for (const name of ["rc", "pi-rc", "result.md", "stderr.log", "completion.json"]) if (fs.existsSync(path.join(native, name))) fs.copyFileSync(path.join(native, name), path.join(output, "native-" + name));
  }
  assert.equal(value.result.status, "completed", "BLOCKED: real launcher did not return a completed task; inspect fabric-exec-result and native logs");
  assert.equal(value.result.exitCode, 0);
  assert(evidence.acceptance?.output.includes(`RYZEN2_TASK_ACCEPTED ${value.handle.id} on ${host}`), "real launcher acceptance missing");
  assert(value.result.text.includes(host), "task did not return selected host's hostname");
  assert(!dirty, "proof requires a clean exact candidate head");
  evidence.passed = true;
  console.log(JSON.stringify({ passed: true, head, id: value.handle.id, hostname: value.result.text, evidence: path.join(output, "evidence.json") }));
} catch (error) {
  // Provider initialization errors may quote private model configuration.
  // Never persist/print those diagnostics when host models are enabled.
  evidence.error = process.env.PROOF_MODELS_FROM_PROFILE === "1"
    ? "Native proof failed with host models enabled; private diagnostics omitted" : String(error);
  console.error(JSON.stringify({ passed: false, head, error: evidence.error, evidence: path.join(output, "evidence.json") }));
  process.exitCode = 1;
} finally {
  fs.writeFileSync(path.join(output, "evidence.json"), JSON.stringify(evidence, null, 2));
  if (session) { await session.extensionRunner.emit({ type: "session_shutdown", reason: "exit" }); session.dispose(); }
  fs.rmSync(scratch, { recursive: true, force: true });
}
