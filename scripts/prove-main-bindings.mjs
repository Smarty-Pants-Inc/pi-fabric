import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { fileURLToPath } from "node:url";
import { RpcClient } from "@earendil-works/pi-coding-agent";

// Usage: nice -n 19 node scripts/prove-main-bindings.mjs dist/index.js "$TASK_OUT"
const repo = fileURLToPath(new URL("../", import.meta.url));
const candidate = path.resolve(process.argv[2] ?? "dist/index.js");
const out = path.resolve(process.argv[3] ?? process.env.TASK_OUT ?? (() => { throw new Error("Supply a retained output directory"); })());
fs.mkdirSync(out, { recursive: true });
const scratch = fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "native-main-bindings-"));
const transcript = path.join(out, "native-proof.jsonl"); fs.writeFileSync(transcript, "");
const record = (type, data) => fs.appendFileSync(transcript, JSON.stringify({ type, ...data }) + "\n");
const receipts = path.join(out, "native-inference.jsonl"); fs.writeFileSync(receipts, "");
const cli = fileURLToPath(new URL("./cli.js", import.meta.resolve("@earendil-works/pi-coding-agent")));
const provider = path.join(repo, "tests/fixtures/main-bindings-cli-provider.ts");
// RpcClient merges process.env: deliberately clear it in this standalone proof process.
// No authentication store or host credential environment is read by any proof child.
const clean = { PATH: process.env.PATH, TMPDIR: scratch, HOME: path.join(scratch, "home"), PI_OFFLINE: "1" };
for (const key of Object.keys(process.env)) delete process.env[key]; Object.assign(process.env, clean);
fs.mkdirSync(clean.HOME, { recursive: true });
const clients = [];
const stop = async (item) => {
  const child = item.client.process;
  if (!child || child.exitCode !== null || child.signalCode !== null) return;
  const exited = new Promise(resolve => child.once("exit", (code, signal) => { record("exit", { endpoint: item.name, code, signal }); resolve(); }));
  child.stdin.end();
  const term = setTimeout(() => child.kill("SIGTERM"), 10000);
  const kill = setTimeout(() => child.kill("SIGKILL"), 15000);
  await exited; clearTimeout(term); clearTimeout(kill);
};
const make = async (name, enrollment = {}) => {
  const cwd = path.join(scratch, name), profile = path.join(cwd, "profile"); fs.mkdirSync(profile, { recursive: true });
  fs.writeFileSync(path.join(profile, "settings.json"), JSON.stringify({ compaction: { enabled: false }, retry: { enabled: false } }));
  fs.writeFileSync(path.join(profile, "fabric.json"), JSON.stringify({ fullCodeMode: true,
    executor: { kernel: "typescript", timeoutMs: 30000 }, mesh: { enabled: true, persist: false, actorPollMs: 20 },
    mcp: { enabled: false }, memory: { enabled: false }, jev: { enabled: false }, prewalk: { enabled: false },
    entropy: { compile: false }, speculation: { enabled: false }, compaction: { engine: "pi" },
    approvals: { read: "allow", write: "allow", exec: "allow", agent: "allow" },
  }));
  const env = { PI_CODING_AGENT_DIR: profile, PI_FABRIC_AGENT_DIR: path.join(cwd, "exports"), PI_FABRIC_MESH_ROOT: path.join(scratch, "mesh"),
    PI_FABRIC_PROJECT_ROOT: scratch, PI_FABRIC_RUN_ROOT: path.join(cwd, "runs"), MAIN_BINDING_RECEIPTS: receipts, MAIN_BINDING_ENDPOINT: name, ...enrollment };
  const args = ["--offline", "--no-extensions", "--no-skills", "--no-prompt-templates", "--no-context-files", "--no-themes", "--no-builtin-tools", "--approve",
    "--thinking", "low", "--session-dir", path.join(cwd, "sessions"), "-e", candidate, "-e", provider];
  record("command", { endpoint: name, executable: "node", argv: [cli, "--mode", "rpc", "--provider", "main-binding-probe", "--model", "a", ...args], env });
  const client = new RpcClient({ cliPath: cli, cwd, env, provider: "main-binding-probe", model: "a", args });
  const events = []; const item = { name, client, events }; clients.push(item);
  client.onEvent(event => { events.push(event); record("rpc-event", { endpoint: name, event }); });
  await client.start(); return item;
};
const state = async item => {
  record("rpc-command", { endpoint: item.name, command: "get_state" });
  const data = await item.client.getState(); record("rpc-response", { endpoint: item.name, command: "get_state", data }); return data;
};
const entries = async item => {
  record("rpc-command", { endpoint: item.name, command: "get_entries" });
  const data = await item.client.getEntries(); record("rpc-response", { endpoint: item.name, command: "get_entries", data }); return data;
};
const run = async (item, packet) => {
  const start = item.events.length;
  record("rpc-command", { endpoint: item.name, command: "prompt", message: "BIND " + JSON.stringify(packet) });
  await item.client.promptAndWait("BIND " + JSON.stringify(packet), undefined, 45000);
  const end = item.events.slice(start).find(event => event.type === "tool_execution_end" && event.toolName === "fabric_exec");
  assert.ok(end, "missing public fabric_exec result"); assert.equal(end.isError, false, JSON.stringify(end));
  assert.equal(end.result.details.success, true, JSON.stringify(end.result.details));
  const value = JSON.parse(end.result.content.filter(part => part.type === "text").map(part => part.text).join("\n"));
  record("guest-readback", { endpoint: item.name, packet, value }); return value;
};
let failure;
try {
  record("artifact", { entry: candidate, sha256: createHash("sha256").update(fs.readFileSync(candidate)).digest("hex"), offline: true });
  const lead = await make("lead"), leadId = `session:${(await state(lead)).sessionId}`;
  const target = await make("target", { SMARTY_LEAD_SESSION: leadId });
  const targetId = `session:${(await state(target)).sessionId}`;
  const own = await run(target, { tag: "own", own: true });
  assert.equal(own.before.model, "main-binding-probe/a"); assert.equal(own.before.thinking, "low");
  assert.equal(own.effort.thinking, "high"); assert.equal(own.effort.previous.thinking, "low");
  assert.equal(own.model.model, "main-binding-probe/b"); assert.equal(own.model.previous.model, "main-binding-probe/a");
  assert.equal(own.model.caller, targetId);
  assert.equal(own.after.model, "main-binding-probe/b"); assert.equal(own.after.thinking, "high");
  const ownState = await state(target); assert.equal(ownState.model.id, "b"); assert.equal(ownState.thinkingLevel, "high");
  const { entries: beforeRemote } = await entries(target);
  for (const operation of ["setThinking", "setModel"]) {
    const refusal = await run(lead, { tag: `remote-${operation}`, id: targetId, operation, thinking: "medium", model: "main-binding-probe/a" });
    assert.equal(refusal.refused, true);
    assert.equal(refusal.error, "remote Main model changes are not supported yet; see smarty-dev#4153");
  }
  const unchanged = await state(target); assert.equal(unchanged.model.id, "b"); assert.equal(unchanged.thinkingLevel, "high");
  const { entries: nativeEntries } = await entries(target);
  // The remote attempts cannot add native model/thinking changes or Fabric audit entries.
  assert.deepEqual(nativeEntries, beforeRemote);
  const audit = nativeEntries.filter(entry => entry.type === "custom" && entry.customType === "pi-fabric.main-binding-change");
  assert.equal(audit.length, 2); assert.deepEqual(audit.map(entry => entry.data.caller), [targetId, targetId]);
  assert.ok(nativeEntries.some(entry => entry.type === "model_change" && entry.modelId === "b"));
  assert.ok(nativeEntries.some(entry => entry.type === "thinking_level_change" && entry.thinkingLevel === "high"));
  const callerState = await state(lead); assert.equal(callerState.model.id, "a"); assert.equal(callerState.thinkingLevel, "low");
  const { entries: callerEntries } = await entries(lead);
  assert.equal(callerEntries.filter(entry => entry.type === "custom" && entry.customType === "pi-fabric.main-binding-change").length, 0);
  const inference = fs.readFileSync(receipts, "utf8").trim().split("\n").map(line => JSON.parse(line));
  assert.ok(inference.some(r => r.endpoint === "target" && r.tag === "own" && r.tool && r.model === "main-binding-probe/a" && r.thinking === "low"));
  assert.ok(inference.some(r => r.endpoint === "target" && r.tag === "own" && !r.tool && r.model === "main-binding-probe/b" && r.thinking === "high"), "next inference did not use native own-Main change");
  record("assertions", { passed: true, ownMain: true, remoteModelRefused: true, remoteThinkingRefused: true,
    remoteNativeMutation: false, callerUnchanged: true, nativeAuditEntries: audit.length, nextInferenceReadback: true });
  fs.writeFileSync(path.join(out, "native-proof.txt"), `Command: nice -n 19 node scripts/prove-main-bindings.mjs dist/index.js "$TASK_OUT"\nPASS: fresh build in 2 real offline Pi RPC CLIs; public fabric_exec own-session setters; native next-inference model/effort readback; 2 caller-attributed audits; remote model/thinking refused, target journal entries unchanged and caller state unchanged. No Pi-core patch or host credential access.\n`);
  console.log("PASS: public fabric_exec own-session Main setters in real offline Pi RPC; native readback/audit; remote model/thinking refused without mutation.");
} catch (error) {
  failure = error; record("failure", { error: String(error), stack: error.stack }); console.error(error);
  fs.writeFileSync(path.join(out, "native-proof.txt"), `FAIL: ${String(error)}\nCommand: nice -n 19 node scripts/prove-main-bindings.mjs dist/index.js "$TASK_OUT"\n`);
} finally {
  await Promise.all(clients.map(stop));
  for (const item of clients) fs.writeFileSync(path.join(out, `native-${item.name}-stderr.log`), item.client.getStderr());
  record("cleanup", { allPiProcessesExited: clients.every(item => item.client.process?.exitCode !== null || item.client.process?.signalCode !== null) });
  fs.rmSync(scratch, { recursive: true, force: true });
}
if (failure) process.exitCode = 1;
