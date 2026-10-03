// Usage: nice -n 19 node scripts/prove-main-reader-compatibility.mjs dist/index.js "$TASK_OUT" /path/to/older/dist/index.js
// Only inference is faux. Discovery, public tools, owner IPC and Pi delivery are real.
import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { fileURLToPath, pathToFileURL } from "node:url";
import { RpcClient } from "@earendil-works/pi-coding-agent";

const repo = fileURLToPath(new URL("../", import.meta.url));
const candidate = path.resolve(process.argv[2] ?? "dist/index.js");
const out = path.resolve(process.argv[3] ?? process.env.TASK_OUT ?? (() => { throw new Error("Supply retained output directory"); })());
const older = path.resolve(process.argv[4] ?? (() => { throw new Error("Supply an actual pre-setter Fabric extension"); })());
fs.mkdirSync(out, { recursive: true });
const scratch = fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "main-reader-native-"));
const transcript = path.join(out, "native-proof.jsonl"); fs.writeFileSync(transcript, "");
const record = (type, data) => fs.appendFileSync(transcript, JSON.stringify({ type, ...data }) + "\n");
const receipts = path.join(out, "native-inference.jsonl"); fs.writeFileSync(receipts, "");
const hash = file => createHash("sha256").update(fs.readFileSync(file)).digest("hex");
const cli = fileURLToPath(new URL("./cli.js", import.meta.resolve("@earendil-works/pi-coding-agent")));
const provider = path.join(repo, "tests/fixtures/main-bindings-cli-provider.ts");
const clean = { PATH: process.env.PATH, TMPDIR: scratch, HOME: path.join(scratch, "home"), PI_OFFLINE: "1" };
for (const key of Object.keys(process.env)) delete process.env[key]; Object.assign(process.env, clean);
fs.mkdirSync(clean.HOME, { recursive: true });
const clients = [];
const { MeshStore } = await import(pathToFileURL(path.join(path.dirname(candidate), "mesh.js")).href);
const mesh = new MeshStore(path.join(scratch, "mesh"), 65536, 1000);
const stop = async item => {
  const child = item.client.process;
  if (!child || child.exitCode !== null || child.signalCode !== null) return;
  const exited = new Promise(resolve => child.once("exit", (code, signal) => { record("exit", { endpoint: item.name, code, signal }); resolve(); }));
  child.stdin.end();
  const term = setTimeout(() => child.kill("SIGTERM"), 10000), kill = setTimeout(() => child.kill("SIGKILL"), 15000);
  await exited; clearTimeout(term); clearTimeout(kill);
};
const make = async (name, extension, enrollment = {}) => {
  const cwd = path.join(scratch, name), profile = path.join(cwd, "profile"); fs.mkdirSync(profile, { recursive: true });
  execFileSync("git", ["init", "-q"], { cwd, env: clean });
  execFileSync("git", ["remote", "add", "origin", "https://example.invalid/team/compat.git"], { cwd, env: clean });
  fs.writeFileSync(path.join(profile, "settings.json"), JSON.stringify({ compaction: { enabled: false }, retry: { enabled: false } }));
  fs.writeFileSync(path.join(profile, "fabric.json"), JSON.stringify({ fullCodeMode: true,
    executor: { kernel: "typescript", timeoutMs: 30000 }, mesh: { enabled: true, persist: false, announce: false, actorPollMs: 20 },
    mcp: { enabled: false }, memory: { enabled: false }, jev: { enabled: false }, prewalk: { enabled: false },
    entropy: { compile: false }, speculation: { enabled: false }, compaction: { engine: "pi" },
    approvals: { read: "allow", write: "allow", exec: "allow", agent: "allow" },
  }));
  const env = { PI_CODING_AGENT_DIR: profile, PI_FABRIC_AGENT_DIR: path.join(cwd, "exports"), PI_FABRIC_MESH_ROOT: mesh.root,
    PI_FABRIC_PROJECT_ROOT: scratch, PI_FABRIC_RUN_ROOT: path.join(cwd, "runs"), SMARTY_ROLE: "project-agent",
    MAIN_BINDING_RECEIPTS: receipts, MAIN_BINDING_ENDPOINT: name, ...enrollment };
  const args = ["--offline", "--no-extensions", "--no-skills", "--no-prompt-templates", "--no-context-files", "--no-themes", "--no-builtin-tools", "--approve",
    "--thinking", "low", "--session-dir", path.join(cwd, "sessions"), "-e", extension, "-e", provider];
  record("command", { endpoint: name, executable: "node", argv: [cli, "--mode", "rpc", "--provider", "main-binding-probe", "--model", "a", ...args], cwd, env: { ...clean, ...env }, extensionSha256: hash(extension) });
  const client = new RpcClient({ cliPath: cli, cwd, env, provider: "main-binding-probe", model: "a", args });
  const item = { name, client, events: [] }; clients.push(item);
  client.onEvent(event => { item.events.push(event); record("rpc-event", { endpoint: name, event }); });
  await client.start(); return item;
};
const state = async item => {
  record("rpc-command", { endpoint: item.name, command: "get_state" });
  const data = await item.client.getState(); record("rpc-response", { endpoint: item.name, command: "get_state", data }); return data;
};
const entries = async item => {
  record("rpc-command", { endpoint: item.name, command: "get_entries" });
  const data = await item.client.getEntries(); record("rpc-response", { endpoint: item.name, command: "get_entries", data }); return data.entries;
};
const run = async (item, packet) => {
  const start = item.events.length;
  record("rpc-command", { endpoint: item.name, command: "prompt", message: "BIND " + JSON.stringify(packet) });
  await item.client.promptAndWait("BIND " + JSON.stringify(packet), undefined, 45000);
  const end = item.events.slice(start).find(event => event.type === "tool_execution_end" && event.toolName === "fabric_exec");
  assert.ok(end, "missing public fabric_exec result"); assert.equal(end.isError, false, JSON.stringify(end));
  assert.equal(end.result.details.success, true, JSON.stringify(end.result.details));
  const text = end.result.content.filter(part => part.type === "text").map(part => part.text).join("\n");
  // Older Fabric appends circuit-breaker notices after its declared JSON output range.
  const { outputFormatStartLine, outputFormatLines } = end.result.details;
  const json = Number.isInteger(outputFormatLines)
    ? text.split("\n").slice(outputFormatStartLine ?? 0, (outputFormatStartLine ?? 0) + outputFormatLines).join("\n") : text;
  const value = JSON.parse(json);
  record("guest-readback", { endpoint: item.name, packet, value }); return value;
};
const waitFor = async predicate => {
  const deadline = Date.now() + 10000;
  while (!await predicate()) { if (Date.now() > deadline) throw new Error("native receipt wait timed out"); await new Promise(resolve => setTimeout(resolve, 25)); }
};
let failure;
try {
  record("launch", { executable: "nice", argv: ["-n", "19", "node", ...process.argv.slice(1)] });
  const distFiles = fs.readdirSync(path.dirname(candidate), { recursive: true }).filter(file => fs.statSync(path.join(path.dirname(candidate), file)).isFile()).sort();
  const manifest = distFiles.map(file => `${hash(path.join(path.dirname(candidate), file))}  ${file}`).join("\n");
  record("artifact", { entry: candidate, sha256: hash(candidate), buildSha256: createHash("sha256").update(manifest).digest("hex"), older, olderSha256: hash(older), cli, cliSha256: hash(cli), offline: true });
  fs.writeFileSync(path.join(out, "build-sha256.txt"), manifest + "\n");
  await mesh.put({ key: "topology/liveness", value: { version: 1, hostLeases: "files", participants: "files" }, identity: { id: "proof-policy", name: "proof", kind: "main" } });
  const lead = await make("new-lead", candidate), leadId = `session:${(await state(lead)).sessionId}`;
  const target = await make("upgraded", candidate, { SMARTY_LEAD_SESSION: leadId }), targetId = `session:${(await state(target)).sessionId}`;
  const existing = await make("existing", older), existingId = `session:${(await state(existing)).sessionId}`;
  // announce=false keeps startup cheap; first public tool call activates each runtime.
  for (const item of [lead, target, existing]) await run(item, { tag: "activate", observe: true });
  assert.deepEqual(mesh.listAll("sessions/", { fresh: true }), [], "legacy session fallback must be absent");
  assert.deepEqual(mesh.listAll("topology/participants/", { fresh: true }), [], "participants must be files-only");
  const files = fs.readdirSync(path.join(mesh.root, "participants")).filter(file => file.endsWith(".json")).map(file => JSON.parse(fs.readFileSync(path.join(mesh.root, "participants", file), "utf8")));
  const upgradedRecord = files.find(entry => entry.value.id === targetId).value;
  assert.equal(upgradedRecord.mainBindings, true); assert.deepEqual(upgradedRecord.capabilities, ["steer", "followUp", "fabric"]);
  const found = await run(existing, { tag: "old-reader-discovery", compat: true });
  for (const collection of [found.members, found.sessions]) {
    const root = collection.find(item => item.id === targetId); assert.ok(root, "old reader lost upgraded Main");
    assert.equal(root.role, "project-agent"); assert.equal(root.repository, "example.invalid/team/compat");
    assert.equal(root.controlProtocol, "v1"); assert.equal(root.stale, false);
  }
  assert.ok(found.peers.some(item => item.id === targetId && item.role === "project-agent" && item.repository === "example.invalid/team/compat"));
  for (const delivery of ["steer", "followUp"]) {
    const receipt = await run(existing, { tag: `old-reader-${delivery}`, id: targetId, delivery, message: `mixed-generation-${delivery}` });
    assert.equal(receipt.acknowledged, true); assert.equal(receipt.routed, "mesh");
  }
  let delivered;
  await waitFor(async () => {
    delivered = (await entries(target)).filter(entry => entry.type === "custom_message" && entry.customType === "pi-fabric-agent-message")
      .flatMap(entry => (entry.details?.items ?? [entry.details]).map(item => ({ ...item, content: entry.content }))).filter(item => item?.data?.mixedGeneration === true);
    return delivered.length === 2;
  });
  assert.deepEqual(delivered.map(item => item.delivery).sort(), ["followUp", "steer"]);
  for (const item of delivered) assert.ok(item.content.includes(`\nmixed-generation-${item.delivery}\n`), "native message content differs from submitted text");
  assert.ok(delivered.every(item => item.from.id === existingId));
  const changed = await run(lead, { tag: "optional-flag-setter", id: targetId, operation: "setThinking", thinking: "high" });
  assert.equal(changed.result.thinking, "high"); assert.equal((await state(target)).thinkingLevel, "high");
  const refused = await run(lead, { tag: "older-no-flag", id: existingId, operation: "setThinking", thinking: "high" });
  assert.equal(refused.refused, true); assert.match(refused.error, /no live Main binding control path/);
  assert.equal((await state(existing)).thinkingLevel, "low");
  assert.equal(mesh.read({ topic: "fabric.control.command", limit: 100 }).filter(event => event.data.targetId === existingId && event.data.operation === "setThinking").length, 0);
  record("assertions", { passed: true, filesOnly: true, historicalRuntimeDiscovery: true, roleRepositoryRetained: true, controlProtocol: "v1", nativeMessages: delivered.length, optionalFlagSetter: true, missingFlagRefusedBeforePublication: true });
  console.log("PASS: actual old Fabric runtime discovers upgraded files-only Main; steer/followUp delivered natively once each; optional-flag setter succeeds and older no-flag setter refuses before publication.");
} catch (error) {
  failure = error; record("failure", { error: String(error), stack: error.stack }); console.error(error);
} finally {
  await Promise.all(clients.map(stop));
  for (const item of clients) fs.writeFileSync(path.join(out, `native-${item.name}-stderr.log`), item.client.getStderr());
  record("cleanup", { allPiProcessesExited: clients.every(item => item.client.process?.exitCode !== null || item.client.process?.signalCode !== null) });
  fs.rmSync(scratch, { recursive: true, force: true });
}
if (failure) process.exitCode = 1;
