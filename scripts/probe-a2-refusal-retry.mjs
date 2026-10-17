#!/usr/bin/env node
// A2 post-install acceptance probe (run once per work host).
// Usage: node scripts/probe-a2-refusal-retry.mjs --release /path/to/pi-fabric --release-sha <40-hex>
// Optional: --pi /path/to/pi (or PI_FABRIC_PROBE_PI), --host HOST.
// It runs the INSTALLED release in a PRIVATE scratch mesh (never live fleet roots).
// It forces refusal by making that private mesh directory unavailable.
// All routing and observation inside Pi uses public fabric_exec agents/mesh APIs.
// It names a unique absent receiver, then forces the late directory read to refuse.
// The first send must be a typed refusal, never generic/unknown.
// It checks zero control events carrying K before any receiver exists.
// It then starts the owned receiver under that exact unique probe-only name.
// It retries with unchanged target, message, and idempotency key K.
// It retries once more with K and checks one accepted receiver message, no duplicate.
// It reads command/ACK events with mesh.read; it never opens or writes mesh storage.
// It publishes one a2probe result event for the probe's own topic.
// Teardown stops only the two probe Pi processes and restores the throwaway mode.
// The release is not modified; pass --release to the installed release directory.
// The release SHA is required so the receipt cannot silently identify another build.
// Run after install with the same host/user that owns the installed Pi runtime.
// Output is exactly one operator receipt line with measured elapsed milliseconds.
// A failed stage exits nonzero and prints its diagnostic only on stderr.

import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";

const self = fileURLToPath(import.meta.url);
const repo = path.resolve(path.dirname(self), "..");
const sleep = ms => new Promise(resolve => setTimeout(resolve, Math.max(0, ms)));
const errorFields = error => Object.fromEntries(["name", "code", "retryable", "message"].map(key => [key, error?.[key]]).filter(([, value]) => value !== undefined));
const typedRefusals = new Set([
  "FABRIC_DIRECTORY_UNAVAILABLE", "FABRIC_ROUTE_AUTHORITY_CHANGED", "FABRIC_PARTICIPANT_NOT_YET_MIRRORED",
  "FABRIC_PARTICIPANT_STALE", "FABRIC_PEER_LEASE_STALE",
]);
const residuePrefix = "a2probe-";
const readResidue = root => {
  const statePath = path.join(root, "state.json");
  const state = fs.existsSync(statePath) ? JSON.parse(fs.readFileSync(statePath, "utf8")) : { entries: {} };
  const entries = Object.entries(state.entries ?? {});
  const participants = entries.filter(([key, entry]) => key.startsWith("topology/participants/") && entry?.value != null).length;
  const namedKeys = entries.filter(([key]) => key.startsWith(residuePrefix)).length;
  const eventPath = path.join(root, "events.jsonl");
  const namedEvents = fs.existsSync(eventPath)
    ? fs.readFileSync(eventPath, "utf8").split("\n").filter(Boolean).map(line => JSON.parse(line)).filter(event =>
      [event.id, event.topic, event.kind, event.name, event.data?.name].some(value => typeof value === "string" && value.startsWith(residuePrefix))).length
    : 0;
  return { participants, named: namedKeys + namedEvents };
};

const parse = argv => {
  const options = { release: repo, releaseSha: undefined, pi: process.env.PI_FABRIC_PROBE_PI ?? "pi", host: os.hostname() };
  for (let index = 0; index < argv.length; index++) {
    const arg = argv[index];
    const value = () => {
      const next = argv[++index];
      if (!next || next.startsWith("--")) throw new Error(`missing value for ${arg}`);
      return next;
    };
    if (arg === "--release") options.release = path.resolve(value());
    else if (arg === "--release-sha") options.releaseSha = value();
    else if (arg === "--pi") options.pi = path.resolve(value());
    else if (arg === "--host") options.host = value();
    else if (arg === "--help") {
      process.stdout.write("Usage: node scripts/probe-a2-refusal-retry.mjs --release DIR --release-sha SHA [--pi PI] [--host HOST]\n");
      process.exit(0);
    } else throw new Error(`unknown option ${arg}`);
  }
  if (!/^[0-9a-f]{40}$/.test(options.releaseSha ?? "")) throw new Error("--release-sha must be a full 40-hex release SHA");
  const entry = path.resolve(options.release, "dist/index.js");
  if (!fs.existsSync(entry)) throw new Error(`release entry does not exist: ${entry}`);
  const packageJson = JSON.parse(fs.readFileSync(path.join(options.release, "package.json"), "utf8"));
  if (packageJson.name !== "pi-fabric") throw new Error(`not a pi-fabric release: ${packageJson.name}`);
  return { ...options, entry };
};

/* const providerSource = `import { createAssistantMessageEventStream } from "@earendil-works/pi-ai";
const model = { provider: "a2probe", id: "offline", name: "A2 probe", api: "a2probe-api", baseUrl: "http://invalid.local", reasoning: false, input: ["text"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 200000, maxTokens: 4096 };
const textOf = message => message?.content?.filter(part => part.type === "text").map(part => part.text).join("") ?? "";
export default function(pi) {
  const stream = (selected, context) => {
    const events = createAssistantMessageEventStream();
    const prompt = textOf(context.messages.at(-1));
    const spec = JSON.parse(prompt.slice("A2PROBE:".length));
    const call = { type: "toolCall", id: "a2probe-" + Date.now(), name: "fabric_exec", arguments: { code: spec.code, resultFormat: "json", display: { name: "A2 router probe", description: "Public Fabric A2 post-install acceptance" } } };
    const message = { role: "assistant", provider: selected.provider, model: selected.id, api: selected.api, timestamp: Date.now(), content: [{ type: "text", text: "" }, { type: "toolCall", ...call }], stopReason: "toolUse", usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } } };
    events.push({ type: "start", partial: message }); events.push({ type: "toolcall_end", contentIndex: 1, toolCall: call, partial: message }); events.push({ type: "done", reason: "toolUse", message }); events.end(); return events;
  };
  pi.registerProvider({ id: "a2probe", name: "A2 probe", auth: { apiKey: { name: "Keyless", check: async () => ({ type: "api_key", source: "keyless" }), resolve: async () => ({ auth: {} }) } }, getModels: () => [model], stream, streamSimple: stream });
}
`;
*/

class Rpc {
  constructor(options, scratch, meshRoot, role, sessionFile, name) {
    this.options = options; this.scratch = scratch; this.meshRoot = meshRoot; this.role = role; this.sessionFile = sessionFile; this.name = name;
    this.events = []; this.pending = new Map(); this.serial = 0; this.buffer = ""; this.stderr = ""; this.child = undefined;
  }
  async start() {
    const profile = path.join(this.scratch, `${this.role}-profile`), cwd = path.join(this.scratch, `${this.role}-cwd`), home = path.join(this.scratch, `${this.role}-home`);
    for (const directory of [profile, cwd, home, path.join(profile, "sessions")]) fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
    fs.writeFileSync(path.join(profile, "fabric.json"), JSON.stringify({ fullCodeMode: true, prewalk: { enabled: false }, agents: { notifyOnComplete: false }, mesh: { enabled: true, announce: true, maxEventBytes: 65536, followUpFlushMs: 1 }, executor: { kernel: "typescript", timeoutMs: 120000 }, approvals: { read: "allow", write: "allow", exec: "allow", agent: "allow" } }));
    fs.writeFileSync(path.join(profile, "settings.json"), JSON.stringify({ compaction: { enabled: false }, retry: { enabled: false } }));
    const args = ["--mode", "rpc", "--offline", "--name", this.name, "--no-extensions", "--no-skills", "--no-prompt-templates", "--no-themes", "--no-context-files", "--approve", "-e", this.options.entry, "-e", path.join(repo, "tests/fixtures/a2-probe-cli-provider.ts"), "--provider", "a2probe", "--model", "offline", "--thinking", "off", "--session-dir", path.join(profile, "sessions")];
    if (this.sessionFile) args.push("--session", this.sessionFile);
    const env = { PATH: process.env.PATH, HOME: home, TMPDIR: this.scratch, PI_CODING_AGENT_DIR: profile, PI_OFFLINE: "1", PI_FABRIC_MESH_ROOT: this.meshRoot, SMARTY_ROLE_PI_BINARY: process.env.SMARTY_ROLE_PI_BINARY };
    const piArgs = this.options.pi.endsWith(".js") ? [this.options.pi, ...args] : args;
    this.child = spawn(this.options.pi.endsWith(".js") ? process.execPath : this.options.pi, piArgs, { cwd, env, stdio: ["pipe", "pipe", "pipe"] });
    this.child.stdout.setEncoding("utf8"); this.child.stderr.setEncoding("utf8");
    this.child.stdout.on("data", chunk => this.#read(chunk)); this.child.stderr.on("data", chunk => { this.stderr += chunk; });
    this.child.on("error", error => { for (const pending of this.pending.values()) { clearTimeout(pending.timer); pending.reject(error); } this.pending.clear(); });
    this.exit = new Promise(resolve => this.child.once("exit", (code, signal) => { for (const pending of this.pending.values()) { clearTimeout(pending.timer); pending.reject(new Error(`${this.role} exited (${code ?? signal}): ${this.stderr}`)); } this.pending.clear(); resolve({ code, signal }); }));
    await this.request({ type: "get_state" });
    return this.request({ type: "get_state" });
  }
  #read(chunk) {
    this.buffer += chunk;
    for (;;) {
      const end = this.buffer.indexOf("\n"); if (end < 0) return;
      const line = this.buffer.slice(0, end); this.buffer = this.buffer.slice(end + 1); if (!line.trim()) continue;
      const event = JSON.parse(line); this.events.push(event);
      if (event.type === "response" && this.pending.has(event.id)) {
        const pending = this.pending.get(event.id); this.pending.delete(event.id); clearTimeout(pending.timer);
        if (event.success) pending.resolve(event.data); else pending.reject(new Error(event.error));
      }
    }
  }
  request(data, timeout = 120000) {
    const id = `${this.role}-${++this.serial}`;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => { this.pending.delete(id); reject(new Error(`RPC timeout: ${data.type}`)); }, timeout); this.pending.set(id, { resolve, reject, timer });
      this.child.stdin.write(JSON.stringify({ id, ...data }) + "\n");
    });
  }
  async eval(code) {
    const start = this.events.length;
    await this.request({ type: "prompt", message: "A2PROBE:" + JSON.stringify({ code }) });
    const deadline = Date.now() + 120000;
    while (Date.now() < deadline) {
      const result = this.events.slice(start).find(event => event.type === "tool_execution_end" && event.toolName === "fabric_exec");
      if (result) {
        if (result.isError) throw new Error(result.result?.content?.map(part => part.text ?? "").join("\n") || "fabric_exec failed");
        const text = result.result?.content?.filter(part => part.type === "text").map(part => part.text).join("\n") ?? "";
        return JSON.parse(text);
      }
      await sleep(20);
    }
    throw new Error(`fabric_exec result timeout (${this.role})`);
  }
  async stop() {
    if (!this.child || this.child.exitCode !== null) return;
    for (const pending of this.pending.values()) { clearTimeout(pending.timer); pending.reject(new Error(`${this.role} stopping`)); }
    this.pending.clear(); this.child.stdin.end();
    const deadline = setTimeout(() => this.child.kill("SIGTERM"), 10_000); await this.exit; clearTimeout(deadline);
  }
}

const main = async () => {
  const options = parse(process.argv.slice(2));
  const started = performance.now();
  const inheritedTmp = process.env.TMPDIR;
  if (!inheritedTmp || !path.isAbsolute(inheritedTmp)) throw new Error("absolute TMPDIR is required");
  const scratch = fs.mkdtempSync(path.join(inheritedTmp, "a2probe-")); fs.chmodSync(scratch, 0o700);
  const meshRoot = path.join(scratch, "mesh");
  const host = options.host, key = `a2probe-${host}-${new Date().toISOString().replace(/[^0-9]/g, "").slice(0, 14)}`;
  const targetName = `probe-receiver-${host}-${key.slice("a2probe-".length)}`, message = `A2PROBE ${key}`, topic = `a2probe.${host}.${key}`;
  const receiver = new Rpc(options, scratch, meshRoot, "receiver", undefined, targetName);
  let sender;
  let refusal = "unknown", before = -1, deliveries = -1, duplicates = -1, residue = -1, verdict = "FAIL", failure;
  try {
    sender = new Rpc(options, scratch, meshRoot, "sender", undefined, "probe-sender");
    await sender.start();
    await sender.eval("return await agents.sessions();");
    fs.chmodSync(meshRoot, 0o000);
    const first = await sender.eval(`const errorFields = error => Object.fromEntries(["name", "code", "retryable", "message"].map(k => [k, error?.[k]]).filter(([,v]) => v !== undefined)); try { const receipt = await agents.followUp({ id: ${JSON.stringify(targetName)}, message: ${JSON.stringify(message)}, idempotencyKey: ${JSON.stringify(key)} }); return { ok: true, receipt }; } catch (error) { return { ok: false, error: errorFields(error) }; }`);
    refusal = first.error?.code ?? first.error?.name ?? "none";
    const firstEvents = await sender.eval(`return (await mesh.read({ topic: "fabric.control.command", limit: 1000 })).filter(event => event.data?.message === ${JSON.stringify(message)}).map(event => ({ id: event.id, commandId: event.data.commandId }));`);
    before = new Set(firstEvents.map(event => event.commandId)).size;
    if (first.ok || !typedRefusals.has(first.error?.code)) throw new Error(`first send was not one typed pre-publication refusal: ${JSON.stringify(first)}`);
    fs.chmodSync(meshRoot, 0o700);

    const receiverState = await receiver.start();
    const receiverId = `session:${receiverState.sessionId}`;
    const discovered = await sender.eval(`const rows = await agents.sessions(); return { rows };`);
    if (!discovered.rows.some(row => row.id === receiverId && row.name === targetName)) throw new Error("receiver did not become publicly discoverable under the probe name");
    const retryCode = `const receipt = await agents.followUp({ id: ${JSON.stringify(targetName)}, message: ${JSON.stringify(message)}, idempotencyKey: ${JSON.stringify(key)} }); return { receipt };`;
    await sender.eval(retryCode);
    await sender.eval(retryCode);
    const observed = await sender.eval(`const commands = (await mesh.read({ topic: "fabric.control.command", limit: 1000 })).filter(event => event.data?.message === ${JSON.stringify(message)}); const ids = new Set(commands.map(event => event.data.commandId)); const acks = (await mesh.read({ topic: "fabric.control.ack", limit: 1000 })).filter(event => ids.has(event.data?.commandId) && event.data?.accepted === true); const messages = new Set(acks.map(event => event.data?.messageId).filter(Boolean)); await mesh.publish({ topic: ${JSON.stringify(topic)}, kind: "a2probe.result", text: ${JSON.stringify(message)}, data: { key: ${JSON.stringify(key)}, commandIds: [...ids], messageIds: [...messages] } }); return { commandCount: ids.size, deliveryCount: messages.size };`);
    deliveries = observed.deliveryCount; duplicates = Math.max(0, observed.deliveryCount - 1);
    if (before !== 0 || deliveries !== 1 || duplicates !== 0) throw new Error(`A2 counts failed: before=${before} deliveries=${deliveries} duplicates=${duplicates}`);
    verdict = "PASS";
  } catch (error) { failure = error; }
  finally {
    try { if (fs.existsSync(meshRoot)) fs.chmodSync(meshRoot, 0o700); } catch { /* teardown is best effort */ }
    await receiver.stop(); await sender?.stop();
    try {
      const observedResidue = readResidue(meshRoot);
      residue = observedResidue.participants + observedResidue.named;
      if (residue !== 0) failure ??= new Error(`probe residue remains: participants=${observedResidue.participants} named=${observedResidue.named}`);
    } catch (error) { failure ??= error; }
    try { fs.rmSync(scratch, { recursive: true, force: true }); } catch (error) { failure ??= error; }
  }
  if (failure) verdict = "FAIL";
  const elapsed = Math.round(performance.now() - started);
  const line = `A2PROBE host=${host} release=${options.releaseSha} refusal=${refusal} events_before_retry=${before} deliveries_after_retry=${deliveries} dup_after_second_retry=${duplicates} residue=${residue} ${verdict} ${elapsed}ms`;
  process.stdout.write(line + "\n");
  if (failure) { process.stderr.write(`A2PROBE failure: ${failure.stack ?? failure}\n`); process.exitCode = 1; }
};

main().catch(error => { process.stdout.write(`A2PROBE host=unknown release=unknown refusal=unknown events_before_retry=-1 deliveries_after_retry=-1 dup_after_second_retry=-1 residue=-1 FAIL 0ms\n`); process.stderr.write(`${error.stack ?? error}\n`); process.exitCode = 1; });
