#!/usr/bin/env node
// A2 post-install acceptance probe (run once per work host).
// Usage: node scripts/probe-a2-refusal-retry.mjs --release /path/to/pi-fabric --release-sha <40-hex>
// Optional: --pi /path/to/pi (or PI_FABRIC_PROBE_PI), --host HOST.
// It runs the INSTALLED release in a PRIVATE scratch mesh (never live fleet roots).
// The keyless fixture injects one late stat EIO and restores the real reader before throwing.
// All routing and observation inside Pi uses public fabric_exec agents/mesh APIs.
// It starts a live receiver under a unique probe-only name, warms the sender directory,
// then forces one late directory read to refuse while refreshRoutingView stays readable.
// The first send must be a typed refusal, never generic/unknown, with zero command events.
// It retries with unchanged target, message, and idempotency key K.
// It retries once more with K and checks one accepted receiver message, no duplicate.
// It reads command/ACK events with mesh.read; it never opens or writes mesh storage.
// It publishes one a2probe result event for the probe's own topic.
// Teardown stops only the two probe Pi processes and removes their private scratch root.
// The release is not modified; pass --release to the installed release directory.
// Identity: prefer RELEASE.json commit+dist hashes; else realpath must be releases/<sha>.
// Run after install with the same host/user that owns the installed Pi runtime.
// Output is exactly one operator receipt line with measured elapsed milliseconds.
// A failed stage exits nonzero and prints its diagnostic only on stderr.

import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { createHash, randomUUID } from "node:crypto";

const self = fileURLToPath(import.meta.url);
const repo = path.resolve(path.dirname(self), "..");
const sleep = ms => new Promise(resolve => setTimeout(resolve, Math.max(0, ms)));
const hash = value => createHash("sha256").update(value).digest("hex");
export const finalVerdict = failure => failure ? "FAIL" : "PASS";

// Prefer commit/files metadata; legacy installs use the installer's /releases/<sha> addressing.
export const verifyRelease = (release, expectedSha) => {
  if (!release) throw new Error("--release is required (explicit installed release path)");
  if (!/^[0-9a-f]{40}$/.test(expectedSha ?? "")) throw new Error("--release-sha must be a full 40-hex release SHA");
  const root = fs.realpathSync(release);
  const metadataPath = path.join(root, "RELEASE.json");
  const metadata = fs.lstatSync(metadataPath, { throwIfNoEntry: false })
    ? JSON.parse(fs.readFileSync(metadataPath, "utf8")) : undefined;
  const identity = metadata === undefined ? "release-path" : "release-json";
  if (identity === "release-json") {
    if (metadata?.commit !== expectedSha) throw new Error(`release SHA mismatch: metadata=${metadata?.commit}, expected=${expectedSha}`);
  } else if (path.basename(path.dirname(root)) !== "releases" || path.basename(root) !== expectedSha) {
    throw new Error("release path mismatch: expected realpath .../releases/<--release-sha>");
  }
  if (JSON.parse(fs.readFileSync(path.join(root, "package.json"), "utf8")).name !== "pi-fabric") throw new Error("not a pi-fabric release");
  const files = {};
  const visit = directory => {
    for (const name of fs.readdirSync(directory).sort()) {
      const file = path.join(directory, name), stat = fs.lstatSync(file);
      if (stat.isSymbolicLink()) throw new Error(`release dist must not contain symlinks: ${file}`);
      if (stat.isDirectory()) visit(file);
      else if (/\.(?:m?js)$/.test(name)) {
        const relative = path.relative(root, file).split(path.sep).join("/");
        const digest = hash(fs.readFileSync(file));
        if (identity === "release-json" && metadata.files?.[relative] !== digest) throw new Error(`release artifact digest mismatch: ${relative}`);
        files[relative] = digest;
      }
    }
  };
  visit(path.join(root, "dist"));
  if (!files["dist/index.js"]) throw new Error("release entry does not exist: dist/index.js");
  for (const relative of Object.keys(metadata?.files ?? {})) {
    if (relative.startsWith("dist/") && /\.(?:m?js)$/.test(relative) && !files[relative]) throw new Error(`release artifact missing: ${relative}`);
  }
  return { entry: path.join(root, "dist/index.js"), releaseSha: expectedSha, identity, artifactDigest: hash(JSON.stringify(files)) };
};
const residuePrefix = "a2probe-";
const readResidue = root => {
  const statePath = path.join(root, "state.json");
  const state = fs.existsSync(statePath) ? JSON.parse(fs.readFileSync(statePath, "utf8")) : { entries: {} };
  const entries = Object.entries(state.entries ?? {});
  const participantKeys = new Set(entries.filter(([key, entry]) => key.startsWith("topology/participants/") && entry?.value != null).map(([key]) => key));
  const participantsPath = path.join(root, "participants");
  if (fs.existsSync(participantsPath)) {
    for (const name of fs.readdirSync(participantsPath).filter(name => name.endsWith(".json"))) {
      const entry = JSON.parse(fs.readFileSync(path.join(participantsPath, name), "utf8"));
      if (typeof entry.key !== "string" || !entry.key.startsWith("topology/participants/")) throw new Error(`invalid participant residue: ${name}`);
      if (entry.value != null) participantKeys.add(entry.key);
    }
  }
  const namedKeys = entries.filter(([key]) => key.startsWith(residuePrefix)).length;
  const eventPath = path.join(root, "events.jsonl");
  const namedEvents = fs.existsSync(eventPath)
    ? fs.readFileSync(eventPath, "utf8").split("\n").filter(Boolean).map(line => JSON.parse(line)).filter(event =>
      [event.id, event.topic, event.kind, event.name, event.data?.name].some(value => typeof value === "string" && value.startsWith(residuePrefix))).length
    : 0;
  return { participants: participantKeys.size, named: namedKeys + namedEvents };
};

export const parse = argv => {
  const options = { release: undefined, releaseSha: undefined, pi: process.env.PI_FABRIC_PROBE_PI ?? "pi", host: os.hostname() };
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
  return { ...options, ...verifyRelease(options.release, options.releaseSha) };
};

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
  async eval(code, lateReadFault = false) {
    const start = this.events.length;
    await this.request({ type: "prompt", message: "A2PROBE:" + JSON.stringify({ code, lateReadFault }) });
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
  const host = options.host, key = `a2probe-${host}-${randomUUID()}`;
  const targetName = `probe-receiver-${randomUUID()}`, message = `A2PROBE ${key}`, topic = `a2probe.${host}.${key}`;
  const receiver = new Rpc(options, scratch, meshRoot, "receiver", undefined, targetName);
  let sender;
  let refusal = "unknown", before = -1, deliveries = -1, duplicates = -1, residue = -1, failure;
  try {
    const receiverState = await receiver.start();
    const receiverId = `session:${receiverState.sessionId}`;
    sender = new Rpc(options, scratch, meshRoot, "sender", undefined, "probe-sender");
    await sender.start();
    const discovered = await sender.eval(`const rows = await agents.sessions(); return { rows };`);
    if (!discovered.rows.some(row => row.id === receiverId && row.name === targetName)) throw new Error("receiver did not become publicly discoverable under the probe name");
    const first = await sender.eval(`const errorFields = error => Object.fromEntries(["name", "code", "retryable", "message"].map(k => [k, error?.[k]]).filter(([,v]) => v !== undefined)); try { const receipt = await agents.followUp({ id: ${JSON.stringify(targetName)}, message: ${JSON.stringify(message)}, idempotencyKey: ${JSON.stringify(key)} }); return { ok: true, receipt }; } catch (error) { return { ok: false, error: errorFields(error) }; }`, true);
    if (!sender.stderr.includes("A2FAULT one-shot")) throw new Error("one-shot late directory fault was not reached");
    refusal = first.error?.code ?? first.error?.name ?? "none";
    const firstEvents = await sender.eval(`return (await mesh.read({ topic: "fabric.control.command", limit: 1000 })).filter(event => event.data?.message === ${JSON.stringify(message)}).map(event => ({ id: event.id, commandId: event.data.commandId }));`);
    before = new Set(firstEvents.map(event => event.commandId)).size;
    if (first.ok || first.error?.code !== "FABRIC_DIRECTORY_UNAVAILABLE" || first.error?.retryable !== true || !first.error?.message?.includes("A2PROBE one-shot late directory read")) throw new Error(`first send was not the injected typed late-read refusal: ${JSON.stringify(first)}`);

    const visible = await sender.eval(`return (await agents.sessions()).some(row => row.id === ${JSON.stringify(receiverId)} && row.name === ${JSON.stringify(targetName)});`);
    if (!visible) throw new Error("receiver is not publicly visible before explicit retry");
    const retryCode = `const receipt = await agents.followUp({ id: ${JSON.stringify(targetName)}, message: ${JSON.stringify(message)}, idempotencyKey: ${JSON.stringify(key)} }); return { receipt };`;
    const observeCode = `const commands = (await mesh.read({ topic: "fabric.control.command", limit: 1000 })).filter(event => event.data?.message === ${JSON.stringify(message)}); const ids = new Set(commands.map(event => event.data.commandId)); const acks = (await mesh.read({ topic: "fabric.control.ack", limit: 1000 })).filter(event => ids.has(event.data?.commandId) && event.data?.accepted === true); const messages = new Set(acks.map(event => event.data?.messageId).filter(Boolean));`;
    await sender.eval(retryCode);
    const afterRetry = await sender.eval(`${observeCode} return { deliveryCount: messages.size };`);
    deliveries = afterRetry.deliveryCount;
    if (deliveries !== 1) throw new Error(`first explicit retry did not deliver once: ${deliveries}`);
    await sender.eval(retryCode);
    const observed = await sender.eval(`${observeCode} await mesh.publish({ topic: ${JSON.stringify(topic)}, kind: "a2probe.result", text: ${JSON.stringify(message)}, data: { key: ${JSON.stringify(key)}, commandIds: [...ids], messageIds: [...messages] } }); return { commandCount: ids.size, deliveryCount: messages.size };`);
    duplicates = Math.max(0, observed.deliveryCount - deliveries);
    if (before !== 0 || deliveries !== 1 || duplicates !== 0) throw new Error(`A2 counts failed: before=${before} deliveries=${deliveries} duplicates=${duplicates}`);
  } catch (error) { failure = error; }
  finally {
    await receiver.stop(); await sender?.stop();
    try {
      const observedResidue = readResidue(meshRoot);
      residue = observedResidue.participants + observedResidue.named;
      if (residue !== 0) failure ??= new Error(`probe residue remains: participants=${observedResidue.participants} named=${observedResidue.named}`);
    } catch (error) { failure ??= error; }
    try { fs.rmSync(scratch, { recursive: true, force: true }); } catch (error) { failure ??= error; }
  }
  try {
    const finalRelease = verifyRelease(options.release, options.releaseSha);
    if (finalRelease.artifactDigest !== options.artifactDigest || finalRelease.identity !== options.identity) throw new Error("release artifacts or identity changed during probe");
  } catch (error) { failure ??= error; }
  const verdict = finalVerdict(failure);
  const elapsed = Math.round(performance.now() - started);
  const line = `A2PROBE host=${host} release=${options.releaseSha} identity=${options.identity} refusal=${refusal} events_before_retry=${before} deliveries_after_retry=${deliveries} dup_after_second_retry=${duplicates} residue=${residue} ${verdict} ${elapsed}ms`;
  process.stdout.write(line + "\n");
  if (failure) { process.stderr.write(`A2PROBE failure: ${failure.stack ?? failure}\n`); process.exitCode = 1; }
};

if (process.argv[1] && path.resolve(process.argv[1]) === self) main().catch(error => { process.stdout.write(`A2PROBE host=unknown release=unknown identity=unknown refusal=unknown events_before_retry=-1 deliveries_after_retry=-1 dup_after_second_retry=-1 residue=-1 FAIL 0ms\n`); process.stderr.write(`${error.stack ?? error}\n`); process.exitCode = 1; });
