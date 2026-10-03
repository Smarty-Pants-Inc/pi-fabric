// Usage: nice -n 19 node scripts/prove-main-binding-cancellation.mjs dist/index.js "$TASK_OUT"
// Two real offline Pi Mains. Only inference and the model-resolution delay are fixtures.
import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { fileURLToPath, pathToFileURL } from "node:url";
import { RpcClient } from "@earendil-works/pi-coding-agent";

const repo = fileURLToPath(new URL("../", import.meta.url));
const candidate = path.resolve(process.argv[2] ?? "dist/index.js");
const out = path.resolve(process.argv[3] ?? process.env.TASK_OUT ?? (() => { throw new Error("Supply retained output directory"); })());
fs.mkdirSync(out, { recursive: true });
const scratch = fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "native-main-cancellation-"));
const transcript = path.join(out, "native-proof.jsonl"); fs.writeFileSync(transcript, "");
const record = (type, data) => fs.appendFileSync(transcript, JSON.stringify({ type, at: Date.now(), ...data }) + "\n");
const hash = file => createHash("sha256").update(fs.readFileSync(file)).digest("hex");
const cli = fileURLToPath(new URL("./cli.js", import.meta.resolve("@earendil-works/pi-coding-agent")));
const provider = path.join(repo, "tests/fixtures/main-bindings-cli-provider.ts");
const receipts = path.join(out, "native-inference.jsonl"); fs.writeFileSync(receipts, "");
const gate = path.join(scratch, "resolution");
const fixture = path.join(scratch, "resolution-gate.ts");
fs.writeFileSync(fixture, `import fs from "node:fs";
export default function(pi) {
  pi.on("session_start", (_event, ctx) => {
    if (process.env.MAIN_BINDING_ENDPOINT !== "target") return;
    const gate = process.env.MAIN_BINDING_RESOLUTION_GATE;
    const registry = ctx.modelRegistry;
    const available = registry.getAvailable.bind(registry), refresh = registry.refresh.bind(registry);
    registry.getAvailable = () => available().filter(model => !(fs.existsSync(gate + "-armed") && !fs.existsSync(gate + "-release") && model.provider === "main-binding-probe" && model.id === "b"));
    registry.refresh = async () => {
      if (fs.existsSync(gate + "-armed") && !fs.existsSync(gate + "-release")) {
        fs.writeFileSync(gate + "-entered", "1");
        const deadline = Date.now() + 15000;
        while (!fs.existsSync(gate + "-release")) {
          if (Date.now() > deadline) throw new Error("Resolution gate timed out");
          await new Promise(resolve => setTimeout(resolve, 10));
        }
      }
      return refresh();
    };
  });
}
`);
// RpcClient inherits process.env. Empty the proof process environment before starting children.
// Isolated empty HOME/profile, --offline, keyless provider; no host authentication or network.
const clean = { PATH: process.env.PATH, TMPDIR: scratch, HOME: path.join(scratch, "home"), PI_OFFLINE: "1" };
for (const key of Object.keys(process.env)) delete process.env[key]; Object.assign(process.env, clean);
fs.mkdirSync(clean.HOME, { recursive: true });
const { MeshStore } = await import(pathToFileURL(path.join(path.dirname(candidate), "mesh.js")).href);
const mesh = new MeshStore(path.join(scratch, "mesh"), 65536, 1000);
const clients = [];
const stop = async item => {
  const child = item.client.process;
  if (!child || child.exitCode !== null || child.signalCode !== null) return;
  const exited = new Promise(resolve => child.once("exit", (code, signal) => { record("exit", { endpoint: item.name, code, signal }); resolve(); }));
  child.stdin.end();
  const term = setTimeout(() => child.kill("SIGTERM"), 10000), kill = setTimeout(() => child.kill("SIGKILL"), 15000);
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
  const env = { PI_CODING_AGENT_DIR: profile, PI_FABRIC_AGENT_DIR: path.join(cwd, "exports"), PI_FABRIC_MESH_ROOT: mesh.root,
    PI_FABRIC_PROJECT_ROOT: scratch, PI_FABRIC_RUN_ROOT: path.join(cwd, "runs"), MAIN_BINDING_RECEIPTS: receipts,
    MAIN_BINDING_ENDPOINT: name, MAIN_BINDING_RESOLUTION_GATE: gate, ...enrollment };
  const args = ["--offline", "--no-extensions", "--no-skills", "--no-prompt-templates", "--no-context-files", "--no-themes", "--no-builtin-tools", "--approve",
    "--thinking", "low", "--session-dir", path.join(cwd, "sessions"), "-e", candidate, "-e", provider, "-e", fixture];
  record("command", { endpoint: name, executable: "node", argv: [cli, "--mode", "rpc", "--provider", "main-binding-probe", "--model", "a", ...args], cwd, env: { ...clean, ...env } });
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
const prompt = (item, packet) => {
  record("rpc-command", { endpoint: item.name, command: "prompt", message: "BIND " + JSON.stringify(packet) });
  return item.client.promptAndWait("BIND " + JSON.stringify(packet), undefined, 20000);
};
const run = async (item, packet) => {
  const start = item.events.length; await prompt(item, packet);
  const end = item.events.slice(start).find(event => event.type === "tool_execution_end" && event.toolName === "fabric_exec");
  assert.ok(end, "missing public fabric_exec result"); assert.equal(end.isError, false, JSON.stringify(end));
  assert.equal(end.result.details.success, true, JSON.stringify(end.result.details));
  const value = JSON.parse(end.result.content.filter(part => part.type === "text").map(part => part.text).join("\n"));
  record("guest-readback", { endpoint: item.name, packet, value }); return value;
};
const waitFor = async predicate => {
  const deadline = Date.now() + 10000;
  while (!await predicate()) { if (Date.now() > deadline) throw new Error("Native proof wait timed out"); await new Promise(resolve => setTimeout(resolve, 10)); }
};
const commands = () => mesh.read({ topic: "fabric.control.command", limit: 100 });
const acks = commandId => mesh.read({ topic: "fabric.control.ack", limit: 100 }).filter(event => event.data.commandId === commandId);
let failure, pending;
try {
  record("launch", { executable: "nice", argv: ["-n", "19", "node", ...process.argv.slice(1)] });
  const distFiles = fs.readdirSync(path.dirname(candidate), { recursive: true }).filter(file => fs.statSync(path.join(path.dirname(candidate), file)).isFile()).sort();
  const manifest = distFiles.map(file => `${hash(path.join(path.dirname(candidate), file))}  ${file}`).join("\n");
  record("artifact", { base: "e37392a4", entry: candidate, sha256: hash(candidate), buildSha256: createHash("sha256").update(manifest).digest("hex"), cli, cliSha256: hash(cli), offline: true });
  fs.writeFileSync(path.join(out, "build-sha256.txt"), manifest + "\n");
  const lead = await make("lead"), leadId = `session:${(await state(lead)).sessionId}`;
  const target = await make("target", { SMARTY_LEAD_SESSION: leadId }), targetId = `session:${(await state(target)).sessionId}`;
  for (const item of [lead, target]) await run(item, { tag: "activate", observe: true });
  const before = await entries(target);
  fs.writeFileSync(gate + "-armed", "1");
  pending = prompt(lead, { tag: "cancel-during-resolution", id: targetId, operation: "setModel", model: "main-binding-probe/b" });
  void pending.catch(() => {});
  await waitFor(() => fs.existsSync(gate + "-entered"));
  const command = commands().find(event => event.kind === "setModel"); assert.ok(command);
  record("commit-window-held", { command, remainingDeadlineMs: command.data.deadlineAt - Date.now(), targetState: await state(target) });
  record("rpc-command", { endpoint: "lead", command: "abort" }); await lead.client.abort(); await pending; pending = undefined;
  let cancellation;
  await waitFor(() => { cancellation = commands().find(event => event.kind === "cancel" && event.data.cancelCommandId === command.data.commandId); return cancellation; });
  record("wire-cancellation", { event: cancellation });
  // Leave time for the real receiver's independent control poll, then resume model resolution.
  await new Promise(resolve => setTimeout(resolve, 250));
  assert.ok(command.data.deadlineAt - Date.now() > 1000, "deadline, not caller cancellation, would explain refusal");
  record("resolution-release", { commandId: command.data.commandId, remainingDeadlineMs: command.data.deadlineAt - Date.now() });
  fs.writeFileSync(gate + "-release", "1");
  await waitFor(() => acks(command.data.commandId).length === 1);
  const receipt = acks(command.data.commandId)[0]; record("fenced-outcome", { receipt });
  assert.equal(receipt.data.accepted, false); assert.match(receipt.data.error, /expired or cancelled/);
  assert.ok(Date.now() < command.data.deadlineAt, "refusal must precede wire deadline");
  assert.equal((await state(target)).model.id, "a");
  const after = await entries(target);
  const changes = after.slice(before.length);
  assert.equal(changes.filter(entry => entry.type === "model_change").length, 0);
  assert.equal(changes.filter(entry => entry.type === "custom" && entry.customType === "pi-fabric.main-binding-change").length, 0);
  // A separate live request commits once, gets a read-back receipt, and replay returns that receipt.
  const changed = await run(lead, { tag: "committed-once", id: targetId, operation: "setModel", model: "main-binding-probe/b" });
  assert.equal(changed.result.model, "main-binding-probe/b"); assert.equal(changed.result.caller, leadId);
  const committed = commands().filter(event => event.kind === "setModel").at(-1);
  await mesh.publish({ topic: committed.topic, kind: committed.kind, from: committed.from, to: targetId, data: committed.data });
  await waitFor(() => acks(committed.data.commandId).length === 2);
  record("committed-replay", { receipts: acks(committed.data.commandId) });
  const final = await entries(target), finalChanges = final.slice(before.length);
  assert.equal(finalChanges.filter(entry => entry.type === "model_change" && entry.modelId === "b").length, 1);
  const audit = finalChanges.filter(entry => entry.type === "custom" && entry.customType === "pi-fabric.main-binding-change");
  assert.equal(audit.length, 1); assert.equal(audit[0].data.caller, leadId);
  assert.equal((await state(target)).model.id, "b");
  record("assertions", { passed: true, realPiMains: 2, publicRemoteSetter: true, rpcCallerAbort: true, cancelledBeforeWireDeadline: true,
    cancelledNativeModelChanges: 0, cancelledBindingAudits: 0, rejectedReceipt: true, committedNativeModelChanges: 1, committedBindingAudits: 1, replayReceipts: 2 });
  console.log("PASS: two real offline Pi Mains; mid-resolution caller abort fenced without native mutation/audit; subsequent commit once, durable receipt replayed without mutation.");
} catch (error) {
  failure = error; record("failure", { error: String(error), stack: error.stack }); console.error(error);
} finally {
  fs.writeFileSync(gate + "-release", "1");
  await Promise.all(clients.map(stop));
  await pending?.catch(() => {});
  for (const item of clients) fs.writeFileSync(path.join(out, `native-${item.name}-stderr.log`), item.client.getStderr());
  record("cleanup", { allPiProcessesExited: clients.every(item => item.client.process?.exitCode !== null || item.client.process?.signalCode !== null) });
  fs.rmSync(scratch, { recursive: true, force: true });
}
if (failure) process.exitCode = 1;
