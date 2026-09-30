// smarty-dev#2045: Dev1 -> actual Forge, using existing installed B65 artifacts/auth.
// Run with bun through forge-ssh-bridge.sh. Never creates a mesh, profile, key or sshd.
// REQUIRED JSON (absolute paths, existing meshes and host-owned agent profiles):
// { localMesh, remoteMesh, localFabric, remoteFabric, localAgentDir, remoteAgentDir,
//   localCwd, remoteCwd, evidenceRoot, remoteEvidenceRoot, bridgeKey, bridgeKnownHosts }
// localFabric/remoteFabric are the exact installed B65 dist/index.js paths. The
// local installation also supplies dist/mesh.js and bin/mesh-bridge. Both evidence
// roots must contain a .local path component; a unique run subdirectory is created.
// Owner/Light must preprepare each own cwd's .pi/fabric.json with mesh.root
// exactly equal to the corresponding absolute mesh path; this driver only reads it.
// OPTIONAL: localName="dev1", remoteName="forge", localPi="pi",
// remotePi="forge-pi", remoteNode="node", localNode="node",
// sshHost="forge-agent" (ordinary host-owned SSH profile, NOT the forced key),
// bridgeHost="paul@100.78.65.112", bridgePort=22,
// bridgeBin=<local install>/bin/mesh-bridge, model="cliproxyapi/gpt-6.1-sol".
// Ordinary SSH always uses BatchMode, strict host checking and a connect deadline.
// The bridge key must ALREADY force the installed remote mesh-bridge agent on
// remoteMesh with --peer <localName>. No auth file is opened, copied or linked here.
// For supplemental RYZEN2, set remoteName, sshHost, bridgeHost, remotePi and ALL
// remote paths explicitly. A supplemental PASS is not exact Forge acceptance.
// This is an intrusive opt-in proof: one fresh nice19 Pi per host, real mesh writes,
// one short-lived bridge; no existing bridge to Forge may already be running.
// Logs contain ONLY these fresh RPC sessions, never other agents' transcripts.
import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { spawn, execFile } from "node:child_process";
import { promisify } from "node:util";
import { randomUUID } from "node:crypto";

// BEGIN WORK INGRESS HELPER — extracted by .local/work-wake-probe.mjs without running this driver.
const workInboxIngress = (messages, expected) => {
  const xml = (s) => s.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");
  for (const message of messages) {
    // Only the native custom message at the RPC message's top level counts.
    if (message?.role !== "custom" || message.customType !== "pi-fabric-inbox") continue;
    const text = typeof message.content === "string" ? message.content :
      Array.isArray(message.content) && message.content.every((c) => c.type === "text" && typeof c.text === "string")
        ? message.content.map((c) => c.text).join("") : "";
    const envelope = /^<fabric-inbox count="(\d+)">\nWork events addressed to you that no steer or follow-up delivered \(a shadow copy can repeat a message you already saw\):\n([\s\S]*)\n<\/fabric-inbox>$/.exec(text);
    if (!envelope) continue;
    // Bodies from rootInboxMessage are XML-escaped. Raw nested tags are never native events.
    const events = [...envelope[2].matchAll(/(?:^|\n)<event ([^<>\n]+)>([^<]*)<\/event>(?=\n|$)/g)];
    if (events.length !== Number(envelope[1]) || events.map((m) => m[0].replace(/^\n/, "")).join("\n") !== envelope[2]) continue;
    for (const [, header, body] of events) {
      const attrs = {}; let remainder = header;
      while (remainder) {
        const attr = /^([a-z_]+)=("(?:[^"\\]|\\.)*")(?: |$)/.exec(remainder);
        if (!attr || Object.hasOwn(attrs, attr[1])) break;
        try { attrs[attr[1]] = JSON.parse(attr[2]); } catch { break; }
        remainder = remainder.slice(attr[0].length);
      }
      if (remainder || typeof attrs.id !== "string" || !attrs.id || !/^\d+$/.test(attrs.sequence ?? "") || !Number.isFinite(Date.parse(attrs.at))) continue;
      if ((expected.id !== undefined && attrs.id !== expected.id) || attrs.from_id !== expected.from.id || attrs.from_name !== expected.from.name ||
          attrs.topic !== xml(expected.topic) || attrs.kind !== xml(expected.kind) || attrs.key !== expected.key || body !== xml(expected.text)) continue;
      return { role: message.role, customType: message.customType, eventId: attrs.id, header, senderId: attrs.from_id,
        topic: attrs.topic, kind: attrs.kind, key: attrs.key, body };
    }
  }
  return undefined;
};
// END WORK INGRESS HELPER

// BEGIN FIRST SEND VALIDATOR — inert probes extract this production predicate.
const assertFirstSend = (attempts, target, host, killedAt, boundMs = 10_000) => {
  const fail = (message) => { throw new Error(`FIRST postkill send to ${target} on ${host}: ${message}`); };
  if (!Array.isArray(attempts) || !attempts.length) fail("missing first attempt");
  const first = attempts[0]; // A later named lapse can never rescue this attempt.
  if (!first || Object.hasOwn(first, "delivered") || Object.hasOwn(first, "accepted") || typeof first.error !== "string") fail("accepted/delivered or missing native failure");
  const { requestAt, nativeStartedAt, nativeCompletedAt, completedAt, nativeMs } = first;
  if (![killedAt, requestAt, nativeStartedAt, nativeCompletedAt, completedAt, nativeMs, boundMs].every(Number.isFinite) ||
      boundMs !== 10_000 || requestAt < killedAt || completedAt < requestAt || nativeCompletedAt < nativeStartedAt ||
      nativeMs !== nativeCompletedAt - nativeStartedAt || nativeMs > boundMs)
    fail("missing/invalid timing or native operation over 10000ms");
  const error = first.error;
  // Trust only the full native router clause, never a target prefix or appended diagnostic.
  const preSendPrefix = `Unknown Fabric participant: ${target} (its lease mirrored from remote host ${host} lapsed`;
  const remainder = error.slice(preSendPrefix.length);
  const clause = /^(?: (\d+) s ago)?: the mesh bridge to that host is down, or the session has ended\)$/.exec(remainder);
  const preSend = error.startsWith(preSendPrefix) && clause !== null && clause[0] === remainder &&
    (clause[1] === undefined || Number.isFinite(Number(clause[1])));
  const pending = error.startsWith(`Fabric lease mirrored from remote host ${host} lapsed for ${target};`) && error.includes("mesh bridge");
  const deadline = error.startsWith(`Fabric mesh bridge to remote host ${host} is not responding for ${target};`);
  if (!preSend && !pending && !deadline) fail("unnamed or wrong host/target bridge condition");
  if ((pending || deadline) && !error.includes("the outcome is unknown")) fail("pending/deadline failure lacks unknown-outcome warning");
  return { condition: preSend ? "pre-send-lapsed" : pending ? "pending-lapsed" : "ack-deadline",
    nativeTimestampDomain: "native-execution-host-local", nativeStartedAt, nativeCompletedAt, nativeMs,
    driverTimestampDomain: "local-driver", requestAfterKillMs: requestAt - killedAt,
    completionAfterKillMs: completedAt - killedAt, rpcModelDurationMs: completedAt - requestAt };
};
// END FIRST SEND VALIDATOR

const exec = promisify(execFile);
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const quote = (s) => `'${String(s).replaceAll("'", "'\\''")}'`;
const shell = (args) => args.map(quote).join(" ");
const assert = (value, message) => { if (!value) throw new Error(message); };
const runId = `mesh-proof-${randomUUID()}`;
const children = new Set();
const results = { runId, startedAt: new Date().toISOString(), checks: {}, status: "FAIL" };
let cfg, evidence, remoteOwned, bridge, bridgeSsh, remoteBridge, piA, piB;
let stopping = false;
const pending = new Set();
const interrupt = () => {
  stopping = true;
  for (const cancel of [...pending]) cancel(new Error("proof interrupted"));
};
const save = () => { if (evidence) fs.writeFileSync(path.join(evidence, "results.json"), `${JSON.stringify(results, null, 2)}\n`, { mode: 0o600 }); };
const check = (name, value) => { results.checks[name] = value; save(); };
const waitFor = async (what, predicate, ms = 60_000) => {
  const until = Date.now() + ms;
  while (Date.now() < until) {
    assert(!stopping, "proof interrupted");
    const value = await predicate(); // Async probes MUST be awaited.
    if (value) return value;
    await sleep(1000);
  }
  throw new Error(`deadline waiting for ${what}`);
};
const sshArgs = () => ["-T", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes", "-o", "ConnectTimeout=10",
  "-o", "ServerAliveInterval=5", "-o", "ServerAliveCountMax=2", cfg.sshHost];
const remote = async (command) => {
  const { stdout } = await exec("ssh", [...sshArgs(), command], { timeout: 20_000, killSignal: "SIGKILL", maxBuffer: 1024 * 1024 });
  return stdout.trim();
};
// Process identity is PID + Linux start ticks, not a global name pattern. Forge
// is Linux. Return only mesh-bridge agents for this configured mesh, not raw ps.
const processScript = `const fs=require('fs'); const mesh=process.argv[1]; const rows=[];
for(const pid of fs.readdirSync('/proc').filter(p=>/^\\d+$/.test(p))) { try {
 const args=fs.readFileSync('/proc/'+pid+'/cmdline','utf8').split('\\0').filter(Boolean);
 const at=args.indexOf('--mesh');
 if(args.includes('agent') && args.some(a=>/mesh-bridge(?:\\.js)?$/.test(a)) && at>=0 && args[at+1]===mesh) {
 const stat=fs.readFileSync('/proc/'+pid+'/stat','utf8').split(') ')[1].split(' ');
 rows.push({pid:Number(pid),start:stat[19]}); }
} catch {} } console.log(JSON.stringify(rows));`;
const remoteAgents = async () => JSON.parse(await remote(shell([cfg.remoteNode, "-e", processScript, cfg.remoteMesh])));
const alive = (p) => {
  try { const s = fs.readFileSync(`/proc/${p.pid}/stat`, "utf8").split(") ")[1].split(" "); return s[0] !== "Z" && s[19] === p.start; }
  catch (error) { if (error.code === "ENOENT") return false; throw error; }
};
const ownLocal = (pid) => ({ pid, start: fs.readFileSync(`/proc/${pid}/stat`, "utf8").split(") ")[1].split(" ")[19] });
const child = (exe, args, options, name) => {
  results.commands ??= [];
  results.commands.push({ name, executable: exe, args, cwd: options.cwd }); save();
  const p = spawn(exe, args, { ...options, detached: true });
  children.add(p);
  p.on("error", (error) => { p.proofError = error; });
  p.stdin?.on("error", () => {});
  p.once("spawn", () => {
    try { p.owned = ownLocal(p.pid); }
    catch (error) { p.proofError = error; results.spawnErrors ??= []; results.spawnErrors.push({ name, error: error.message }); save(); }
  });
  return p;
};
// The fresh proof client is not a second owner of its parent's fleet lane.
const cleanEnv = () => Object.fromEntries(Object.entries(process.env).filter(([key]) => !/^(PI_|HERDR|FABRIC|SMARTY_ROLE$)/.test(key)));
// ponytail: each proof call performs one explicit native provider operation, not
// general work-agent work. Keep context/profile/trust/mesh, but exclude unrelated
// skill reads: --tools allowlists extension tools too; --no-skills only disables discovery.
const piArgs = (fabric, sessionDir) => ["--mode", "rpc", "-ne", "-e", fabric, "--tools", "fabric_exec", "--no-skills", "--model", cfg.model, "--thinking", "medium", "--session-dir", sessionDir];
const rpc = (p, name) => {
  const listeners = new Set(), ingress = [], waits = new Set();
  let buffer = "", failure, promptCount = 0, runSequence = 0, activeRun;
  const fail = (error) => {
    failure ??= error;
    for (const cancel of [...waits]) cancel(failure);
  };
  const unavailable = () => failure ?? p.proofError ?? (stopping ? new Error("proof interrupted") :
    p.exitCode !== null || p.signalCode !== null ? new Error(`${name}: child exited`) : undefined);
  p.once("error", fail);
  p.once("spawn", () => { if (p.proofError) fail(p.proofError); });
  p.once("exit", (code, signal) => fail(new Error(`${name}: child exited (${signal ?? code})`)));
  const stdout = fs.createWriteStream(path.join(evidence, `${name}-rpc.jsonl`), { mode: 0o600 });
  const stderr = fs.createWriteStream(path.join(evidence, `${name}-stderr.log`), { mode: 0o600 });
  let stderrBuffer = "";
  p.stderr.on("data", (data) => {
    stderr.write(data);
    stderrBuffer = (stderrBuffer + data.toString()).slice(-8192);
    // Remote setsid leader is recorded before exec timeout; process start ticks
    // make later scoped cleanup safe even if the PID has been reused.
    const match = new RegExp(`PROOF_OWNED ${runId} (\\d+) (\\d+)`).exec(stderrBuffer);
    if (match) remoteOwned = { pid: Number(match[1]), start: match[2] };
  });
  const remember = (message) => {
    if (!message || ["assistant", "toolResult"].includes(message.role)) return;
    const text = typeof message.content === "string" ? message.content : (message.content ?? []).map((c) => c.text ?? "").join("");
    if (text.includes(runId)) ingress.push({ role: message.role, customType: message.customType, text });
  };
  p.stdout.on("data", (data) => {
    stdout.write(data); buffer += data.toString();
    let at;
    while ((at = buffer.indexOf("\n")) >= 0) {
      const line = buffer.slice(0, at); buffer = buffer.slice(at + 1);
      let record; try { record = JSON.parse(line); } catch { continue; }
      // Native RPC has no run ID: number starts locally, keeping the run alive
      // through settled (agent_end precedes awaited native settle handlers).
      if (record.type === "agent_start") activeRun = ++runSequence;
      if (record.type === "message_end") remember(record.message);
      for (const listener of [...listeners]) listener(record);
      if (record.type === "agent_settled") activeRun = undefined;
    }
  });
  p.once("close", () => { fail(new Error(`${name}: child closed`)); stdout.end(); stderr.end(); });
  const request = (type, fields = {}, ms = 15_000) => new Promise((resolve, reject) => {
    const error = unavailable();
    if (error) return reject(error);
    const id = randomUUID();
    let settled = false;
    const done = (error, value) => {
      if (settled) return;
      settled = true; clearTimeout(timer); listeners.delete(on); waits.delete(done); pending.delete(done);
      error ? reject(error) : resolve(value);
    };
    const on = (record) => {
      if (!settled && record.type === "response" && record.id === id) {
        record.success === false ? done(new Error(`${name} ${type}: ${record.error}`)) : done(null, record.data);
      }
    };
    const timer = setTimeout(() => done(new Error(`${name} RPC ${type} deadline`)), ms);
    listeners.add(on); waits.add(done); pending.add(done);
    try {
      if (type === "prompt") promptCount++;
      p.stdin.write(`${JSON.stringify({ id, type, ...fields })}\n`, (error) => { if (error) done(error); });
    }
    catch (error) { done(error); }
  });
  const idle = async () => { const state = await request("get_state"); return !state.isStreaming; };
  const execute = async (code, ms = 120_000) => {
    const started = Date.now();
    await waitFor(`${name} idle`, idle, Math.min(ms, 60_000));
    const token = randomUUID();
    const wrapped = `const value = await (async () => { ${code}\n})(); return { proofToken: ${JSON.stringify(token)}, value };`;
    const message = `Call fabric_exec exactly once with exactly the code below, byte for byte, resultFormat "json", timeoutMs ${Math.min(ms, 120_000)}. Do not call any other tool or communicate with any other root. Then reply only OK.\n${wrapped}`;
    return new Promise((resolve, reject) => {
      const error = unavailable();
      if (error) return reject(error);
      let owningRun, toolId, receipt, ended = false, settled = false;
      const done = (error, value) => {
        if (settled) return;
        settled = true; clearTimeout(timer); listeners.delete(on); waits.delete(done); pending.delete(done);
        error ? reject(error) : resolve(value);
      };
      const on = (record) => {
        if (settled) return;
        if ((record.type === "message_start" || record.type === "message_end") && record.message?.role === "user") {
          const content = record.message.content;
          const text = typeof content === "string" ? content : Array.isArray(content) && content.every((c) => c.type === "text")
            ? content.map((c) => c.text).join("") : undefined;
          if (text === message) {
            if (!activeRun || (owningRun !== undefined && owningRun !== activeRun)) return done(new Error(`${name}: prompt run identity mismatch`));
            owningRun = activeRun;
          }
        }
        // A queued prompt can follow an earlier run's settle, or a native inbox
        // reply with no tools. Neither owns this exact user prompt.
        if (owningRun === undefined) return;
        if (activeRun !== owningRun) return done(new Error(`${name}: owning run changed before settlement`));
        if (record.type === "agent_end") ended = true;
        if (record.type === "agent_settled") {
          if (record.outcome !== "completed" || !ended || !receipt) return done(new Error(`${name}: owning run settled without one completed execution`));
          results.toolExecutions ??= [];
          results.toolExecutions.push({ side: name, token, toolCallId: toolId, run: owningRun, isError: false }); save();
          return done(null, receipt.value);
        }
        if (record.type === "tool_execution_start") {
          if (ended || toolId !== undefined || record.toolName !== "fabric_exec" || record.args?.code !== wrapped ||
              typeof record.toolCallId !== "string" || !record.toolCallId) return done(new Error(`${name}: wrong or duplicate tool execution`));
          toolId = record.toolCallId;
          return;
        }
        if (record.type !== "tool_execution_end") return;
        if (ended || receipt || !toolId || record.toolCallId !== toolId || record.toolName !== "fabric_exec")
          return done(new Error(`${name}: wrong or duplicate tool execution end`));
        if (record.isError || record.result?.isError || record.result?.details?.success === false) return done(new Error(`${name}: fabric_exec failed`));
        const content = record.result?.content;
        const text = Array.isArray(content) ? content.filter((c) => c?.type === "text" && typeof c.text === "string").map((c) => c.text).join("\n") : "";
        let parsed;
        // resultFormat=json; permit host rendering around the JSON, not model prose.
        for (let a = text.indexOf("{"); a >= 0 && !parsed; a = text.indexOf("{", a + 1)) {
          for (let b = text.lastIndexOf("}"); b > a; b = text.lastIndexOf("}", b - 1)) {
            try { const v = JSON.parse(text.slice(a, b + 1)); if (v.proofToken === token) { parsed = v; break; } } catch {}
          }
        }
        if (!parsed) return done(new Error(`${name}: actual tool result lacks proof token / valid JSON`));
        receipt = parsed; // Commit exactly once only after the owning run settles.
      };
      const timer = setTimeout(() => done(new Error(`${name}: tool execution deadline`)), Math.max(1, ms - (Date.now() - started)));
      listeners.add(on); waits.add(done); pending.add(done);
      request("prompt", { message }, ms).catch((error) => done(error));
    });
  };
  const received = async (needle, delivery, senderId) => {
    // Explicit RPC reads ONLY this new session. Some native custom messages are
    // not emitted as message_end, so the stream alone is not proof of absence.
    const data = await request("get_messages");
    for (const message of data?.messages ?? []) remember(message);
    for (const item of ingress.filter((m) => m.customType === "pi-fabric-agent-message")) {
      for (const match of item.text.matchAll(/(<fabric-agent-message\b[^>]*>)([\s\S]*?)<\/fabric-agent-message>/g)) {
        const header = match[1];
        if (match[2].includes(needle) && header.includes(`delivery="${delivery}"`) && header.includes(`from_id="${senderId}"`)) {
          return { role: item.role, customType: item.customType, header, senderId, needle };
        }
      }
    }
    return undefined;
  };
  const receivedWork = async (expected) => {
    // Same authoritative RPC path as received(): no model prompt, mesh read, or embedded tool output.
    const data = await request("get_messages");
    return workInboxIngress(data?.messages ?? [], expected);
  };
  return { p, execute, idle, received, receivedWork, request, promptCount: () => promptCount };
};

const cleanup = async () => {
  interrupt();
  const errors = [];
  // First EOF, then bounded scoped TERM/KILL. Both Pis have an independent 900s
  // timeout --kill-after=10s even when the Dev1 driver / network disappears.
  for (const pi of [piA, piB]) pi?.p.stdin.end();
  await sleep(1500);
  if (remoteOwned && cfg) {
    const script = `const fs=require('fs'); const pid=${remoteOwned.pid};
const stat=p=>{try{return fs.readFileSync('/proc/'+p+'/stat','utf8').split(') ')[1].split(' ');}catch(e){if(e.code!=='ENOENT')throw e;}};
const members=()=>fs.readdirSync('/proc').filter(p=>/^\\d+$/.test(p)).flatMap(p=>{const s=stat(p);return s&&s[0]!=='Z'&&Number(s[2])===pid?[{pid:Number(p),start:s[19]}]:[];});
const leader=stat(pid); if(leader&&leader[19]===${JSON.stringify(remoteOwned.start)}&&Number(leader[2])===pid) {
 const owned=members(); process.kill(-pid,'SIGTERM');
 setTimeout(()=>{if(owned.some(p=>{const s=stat(p.pid);return s&&s[19]===p.start&&Number(s[2])===pid&&s[0]!=='Z';})) {
 try{process.kill(-pid,'SIGKILL');}catch(e){if(e.code!=='ESRCH')throw e;}}
 setTimeout(()=>{if(members().length) {console.error('owned remote process group survived cleanup');process.exitCode=1;}},500);},2000);
} else if(members().length) {console.error('remote leader lost before cleanup; scoped group cannot be authenticated');process.exitCode=1;}`;
    try { await remote(shell([cfg.remoteNode, "-e", script])); } catch (error) { errors.push(`remote Pi cleanup: ${error.message}`); }
  }
  for (const p of children) {
    if (p.owned && alive(p.owned)) { try { process.kill(-p.pid, "SIGTERM"); } catch (error) { if (error.code !== "ESRCH") errors.push(error.message); } }
  }
  await sleep(2000);
  for (const p of children) {
    if (p.owned && alive(p.owned)) { try { process.kill(-p.pid, "SIGKILL"); } catch (error) { if (error.code !== "ESRCH") errors.push(error.message); } }
  }
  // A transport left by SIGKILL of its bridge is a child in the bridge's owned
  // process group; reap only that group, never ssh by name.
  if (bridgeSsh && alive(bridgeSsh)) {
    try { process.kill(-bridge.pid, "SIGKILL"); } catch (error) { if (error.code !== "ESRCH") errors.push(error.message); }
  }
  // SIGKILL is asynchronous: require actual death, not an immediate post-signal read.
  const reapDeadline = Date.now() + 2000;
  while (Date.now() < reapDeadline && [...children].some((p) => p.owned && alive(p.owned))) await sleep(50);
  for (const p of children) if (p.owned && alive(p.owned)) errors.push(`owned local leader survived: ${p.pid}`);
  results.cleanup = { remoteOwned, errors }; save();
  assert(errors.length === 0, errors.join("; "));
};
process.once("SIGTERM", interrupt);
process.once("SIGINT", interrupt);

try {
  cfg = JSON.parse(fs.readFileSync(process.argv[2], "utf8"));
  for (const key of ["localMesh", "remoteMesh", "localFabric", "remoteFabric", "localAgentDir", "remoteAgentDir", "localCwd", "remoteCwd", "evidenceRoot", "remoteEvidenceRoot", "bridgeKey", "bridgeKnownHosts"]) {
    assert(typeof cfg[key] === "string" && path.isAbsolute(cfg[key]) && !cfg[key].includes("\n"), `required absolute path: ${key}`);
  }
  for (const key of ["evidenceRoot", "remoteEvidenceRoot"]) assert(cfg[key].split("/").includes(".local") && !cfg[key].startsWith("/tmp/"), `${key} must be under .local, not /tmp`);
  Object.assign(cfg, { localName: cfg.localName ?? "dev1", remoteName: cfg.remoteName ?? "forge",
    localPi: cfg.localPi ?? "pi", remotePi: cfg.remotePi ?? "forge-pi", localNode: cfg.localNode ?? "node", remoteNode: cfg.remoteNode ?? "node",
    sshHost: cfg.sshHost ?? "forge-agent", bridgeHost: cfg.bridgeHost ?? "paul@100.78.65.112", bridgePort: String(cfg.bridgePort ?? 22),
    model: cfg.model ?? "cliproxyapi/gpt-6.1-sol", bridgeBin: cfg.bridgeBin ?? path.resolve(path.dirname(cfg.localFabric), "../bin/mesh-bridge") });
  assert(!cfg.sshHost.startsWith("-") && !cfg.bridgeHost.startsWith("-"), "SSH host must not be an option");
  assert(/^[a-zA-Z0-9][a-zA-Z0-9._-]*$/.test(cfg.localName) && /^[a-zA-Z0-9][a-zA-Z0-9._-]*$/.test(cfg.remoteName) && cfg.localName !== cfg.remoteName, "distinct valid bridge names required");
  results.acceptanceScope = cfg.remoteName === "forge" ? "exact-forge" : `supplemental-${cfg.remoteName}`;
  evidence = path.join(cfg.evidenceRoot, runId); fs.mkdirSync(evidence, { recursive: true, mode: 0o700 });
  // Deliberately whitelist metadata; never persist arbitrary config or environment.
  results.configuration = Object.fromEntries(["localName", "remoteName", "localMesh", "remoteMesh", "localFabric", "remoteFabric", "localAgentDir", "remoteAgentDir", "localCwd", "remoteCwd", "localPi", "remotePi", "model", "sshHost", "bridgeHost", "bridgeBin", "bridgeKey", "bridgeKnownHosts"].map((k) => [k, cfg[k]])); save();
  for (const p of [cfg.localMesh, cfg.localAgentDir, cfg.localCwd]) assert(fs.statSync(p).isDirectory(), `missing existing directory: ${p}`);
  for (const p of [cfg.localFabric, cfg.bridgeBin, cfg.bridgeKey, cfg.bridgeKnownHosts]) assert(fs.statSync(p).isFile(), `missing existing file: ${p}`);
  const localMeshConfig = JSON.parse(fs.readFileSync(path.join(cfg.localCwd, ".pi/fabric.json"), "utf8"));
  assert(localMeshConfig?.mesh?.root === cfg.localMesh, "own local cwd .pi/fabric.json mesh.root must match localMesh exactly");
  check("localProjectMeshConfig", { path: path.join(cfg.localCwd, ".pi/fabric.json"), meshRoot: localMeshConfig.mesh.root });
  const remoteMeshCheck = `const fs=require('fs'),path=require('path');const file=path.join(process.argv[1],'.pi/fabric.json');const c=JSON.parse(fs.readFileSync(file,'utf8'));if(c.mesh?.root!==process.argv[2])throw Error('remote project mesh.root mismatch');console.log(JSON.stringify({path:file,meshRoot:c.mesh.root}));`;
  check("remoteProjectMeshConfig", JSON.parse(await remote(shell([cfg.remoteNode, "-e", remoteMeshCheck, cfg.remoteCwd, cfg.remoteMesh]))));
  const remoteRoot = path.join(cfg.remoteEvidenceRoot, runId);
  await remote([`test -d ${quote(cfg.remoteMesh)}`, `test -d ${quote(cfg.remoteAgentDir)}`, `test -d ${quote(cfg.remoteCwd)}`, `test -f ${quote(cfg.remoteFabric)}`, `umask 077; mkdir -p ${quote(remoteRoot)}`].join(" && "));
  const beforeRemote = await remoteAgents();
  assert(beforeRemote.length === 0, "existing bridge agent on selected remote mesh: cannot prove isolated bridge death");
  const { MeshStore } = await import(pathToFileURL(path.join(path.dirname(cfg.localFabric), "mesh.js")).href);
  const store = new MeshStore(cfg.localMesh, 256 * 1024, 500);
  piA = rpc(child("timeout", ["--signal=TERM", "--kill-after=10s", "900s", "nice", "-n", "19", cfg.localPi, ...piArgs(cfg.localFabric, path.join(evidence, "sessions"))],
    { cwd: cfg.localCwd, env: { ...cleanEnv(), PI_CODING_AGENT_DIR: cfg.localAgentDir }, stdio: ["pipe", "pipe", "pipe"] }, `${cfg.localName} Pi`), cfg.localName);
  // ponytail: SSH's command may already lead a process group; setsid then forks.
  // --wait keeps the SSH command (and RPC stdin) alive until that child finishes.
  const remoteCommand = `cd ${quote(cfg.remoteCwd)} && exec ${shell(["setsid", "--wait", "sh", "-c",
    `printf 'PROOF_OWNED ${runId} %s %s\\n' "$$" "$(awk '{print $22}' /proc/$$/stat)" >&2; exec ${shell(["timeout", "--signal=TERM", "--kill-after=10s", "900s", "nice", "-n", "19", "env", `PI_CODING_AGENT_DIR=${cfg.remoteAgentDir}`, cfg.remotePi, ...piArgs(cfg.remoteFabric, path.join(remoteRoot, "sessions"))])}`])}`;
  piB = rpc(child("ssh", [...sshArgs(), remoteCommand], { stdio: ["pipe", "pipe", "pipe"] }, `${cfg.remoteName} Pi ordinary SSH`), cfg.remoteName);
  await waitFor("owned remote timeout leader", () => {
    assert(!piB.p.proofError && piB.p.exitCode === null && piB.p.signalCode === null, "remote Pi failed before reporting its owned leader");
    return remoteOwned;
  });
  // Initial RPC commands are queued while native session_start binds extensions.
  // Allow that one cold bootstrap its own bound; later requests retain the 15s bound.
  const states = await Promise.all([piA, piB].map((pi) => pi.request("get_state", {}, 120_000)));
  assert(states.every((s) => `${s.model?.provider}/${s.model?.id}` === cfg.model && typeof s.sessionId === "string"), "native RPC must select the requested model and fresh session");
  check("nativeRpcReady", states.map((s) => ({ sessionId: s.sessionId, model: `${s.model.provider}/${s.model.id}`, thinkingLevel: s.thinkingLevel })));
  const [a, b] = await Promise.all([piA, piB].map((pi) => pi.execute("const main = await agents.main(); return { id: main.id, local: main.local };")));
  assert(a.local === true && b.local === true && a.id === `session:${states[0].sessionId}` && b.id === `session:${states[1].sessionId}` && a.id !== b.id, "native agents.main must identify the two fresh RPC roots");
  results.roots = { [cfg.localName]: a.id, [cfg.remoteName]: b.id }; check("nativeRoots", results.roots);
  const peerCode = (target) => `const peers = await agents.peers(); return { ownTarget: peers.filter(p => p.id === ${JSON.stringify(target)}).map(p => ({id:p.id,host:p.host})), unrelatedCount: peers.filter(p => p.id !== ${JSON.stringify(a.id)} && p.id !== ${JSON.stringify(b.id)}).length };`;
  const baseline = await Promise.all([piA.execute(peerCode(b.id)), piB.execute(peerCode(a.id))]);
  assert(baseline.every((v) => v.ownTarget.length === 0), "own remote root visible before proof bridge: existing link interferes");
  check("noBridgeCounterexample", baseline); // No messages to unrelated peers.
  const bridgeLog = fs.openSync(path.join(evidence, "bridge.log"), "a", 0o600);
  bridge = child(cfg.localNode, [cfg.bridgeBin, "run", "--mesh", cfg.localMesh, "--name", cfg.localName, "--remote", cfg.remoteName, "--cursor", path.join(evidence, "cursor.json"),
    "--ssh", cfg.bridgeHost, "--ssh-key", cfg.bridgeKey, "--ssh-port", cfg.bridgePort, "--ssh-known-hosts", cfg.bridgeKnownHosts], { stdio: ["ignore", bridgeLog, bridgeLog] }, "bridge forced-command SSH");
  fs.closeSync(bridgeLog);
  bridgeSsh = await waitFor("exact bridge SSH child", async () => {
    assert(!bridge.proofError && bridge.exitCode === null, "bridge failed before linking");
    // ponytail: this Linux proof already uses /proc ownership; avoid spawning ps
    // (no-child exit races and process startup time are not transport failures).
    try {
      const pids = fs.readFileSync(`/proc/${bridge.pid}/task/${bridge.pid}/children`, "utf8").trim().split(/\s+/).filter(Boolean);
      const pid = pids.find((id) => fs.readFileSync(`/proc/${id}/comm`, "utf8").trim() === "ssh");
      return pid && ownLocal(Number(pid));
    } catch (error) { if (error.code === "ENOENT") return undefined; throw error; }
  });
  remoteBridge = await waitFor("new exact remote bridge agent", async () => {
    const rows = await remoteAgents(); assert(rows.length <= 1, "multiple selected remote bridge agents interfere"); return rows[0];
  });
  check("ownedTransport", { bridge: bridge.owned, ssh: bridgeSsh, remoteAgent: remoteBridge });
  const discovered = await Promise.all([[piA, b.id, cfg.remoteName], [piB, a.id, cfg.localName]].map(async ([pi, target, host]) =>
    waitFor(`${host} peer discovery`, async () => { const v = await pi.execute(peerCode(target)); return v.ownTarget.some((p) => p.host === host) && v; })));
  check("actualHostPeers", discovered);
  for (const [sender, receiver, target, direction] of [[piA, piB, b.id, `${cfg.localName}-${cfg.remoteName}`], [piB, piA, a.id, `${cfg.remoteName}-${cfg.localName}`]]) {
    for (const operation of ["steer", "followUp"]) {
      const needle = `${runId}-${direction}-${operation}`;
      const ack = await sender.execute(`return await agents.${operation}(${JSON.stringify(target)}, ${JSON.stringify(`${needle}: reply only OK; do not use tools.`)});`);
      assert(ack?.queued === true && ack.acknowledged === true && ack.routed === "mesh" && typeof ack.messageId === "string", `${direction} ${operation} lacks native acknowledgement`);
      check(`${direction}-${operation}`, { ack, ingress: await waitFor(`${direction} ${operation} native ingress`, () => receiver.received(needle, operation, sender === piA ? a.id : b.id), 120_000) });
    }
  }
  // Complete all previous turns before publishing: no remote owner prompt may
  // trigger before_agent_start reconciliation during this independent idle wake.
  await Promise.all([piA, piB].map((pi) => waitFor("idle before work wake", pi.idle, 120_000)));
  const work = { topic: "fleet.work.smarty-dev.2045", kind: "p0", to: b.id,
    text: `${runId}-work-wake: reply only OK; do not use tools or communicate with other roots.`,
    data: { ref: "smarty-dev#2045", key: `${runId}-work-${randomUUID()}`, runId } };
  const remotePrompts = piB.promptCount();
  const workWake = await piA.execute(`return await mesh.publish(${JSON.stringify(work)});`);
  assert(typeof workWake?.id === "string" && workWake.from?.id === a.id && workWake.to === b.id && workWake.topic === work.topic &&
    workWake.kind === work.kind && workWake.text === work.text && workWake.data?.key === work.data.key && workWake.data?.runId === runId,
    "native local work publication lacks exact fresh-root attribution / run marker");
  const expectedWork = { ...work, from: workWake.from, key: work.data.key };
  // >=120s is essential: actual RootInbox steer grace is 60s, plus idle timer and bridge latency.
  // No steer/followUp carries this unique key. RPC get_messages cannot start a model turn.
  const workIngress = await waitFor("remote idle Main native work inbox ingress", () => piB.receivedWork(expectedWork), 180_000);
  assert(piB.promptCount() === remotePrompts, "remote owner prompt occurred before independent work ingress assertion");
  check("factoryWorkIdleNativeIngress", { originalEventId: workWake.id, work, ingress: workIngress,
    remotePromptsBefore: remotePrompts, remotePromptsAtIngress: piB.promptCount(), noCorrespondingSteer: true });
  // Only AFTER native ingress has been asserted may Main be prompted to read its mesh.
  const workRead = await waitFor("remote Main native bridged work read", async () => {
    const events = await piB.execute(`const self=(await agents.main()).id; const events=await mesh.read({topic:${JSON.stringify(work.topic)},to:self,limit:500}); return events.filter(e=>e.data?.key===${JSON.stringify(work.data.key)}).map(e=>({id:e.id,topic:e.topic,kind:e.kind,to:e.to,from:e.from,text:e.text,key:e.data?.key,runId:e.data?.runId,bridge:e.data?.bridge}));`);
    return events.find((e) => e.id === workIngress.eventId && e.topic === work.topic && e.kind === work.kind && e.to === b.id &&
      e.from?.id === a.id && e.from?.name === workWake.from.name && e.from?.kind === workWake.from.kind && e.text === work.text &&
      e.key === work.data.key && e.runId === runId && e.bridge?.id === workWake.id && e.bridge.from === cfg.localName);
  }, 180_000);
  const correlatedIngress = await piB.receivedWork({ ...expectedWork, id: workRead.id });
  assert(correlatedIngress?.eventId === workRead.id, "native inbox remote event ID does not correlate with native bridged read");
  check("factoryWorkBridgedRead", { ...workRead, ingress: correlatedIngress });
  const prNeedle = `${runId}-pr.wake`, laneNeedle = `${runId}-lane-followUp`;
  const prWake = await store.publish({ topic: "ops.owner", kind: "pr.wake", from: { id: "factory-host:owner", name: "factory-owner", kind: "main" }, to: b.id,
    text: prNeedle, data: { rootId: b.id, repo: "Smarty-Pants-Inc/smarty-dev", pr: 2045 } });
  const now = Date.now();
  const laneWake = await store.publish({ topic: "fabric.control.command", kind: "followUp", from: { id: "factory-host:stuck-work", name: "factory-stuck-work", kind: "main" }, to: b.id,
    text: laneNeedle, data: { version: 1, commandId: randomUUID().replaceAll("-", ""), targetId: b.id, operation: "followUp", replyTo: "factory-host:stuck-work",
      message: `${laneNeedle}: reply only OK; do not use tools.`, data: { ref: "smarty-dev#2045" }, requestedAt: now, deadlineAt: now + 120_000 } });
  check("factoryLaneNativeIngress", { eventId: laneWake.id, ingress: await waitFor("factory lane followUp native ingress", () => piB.received(laneNeedle, "followUp", "factory-host:stuck-work"), 120_000) });
  const prRead = await waitFor("remote Main reads own factory pr.wake", async () => {
    const events = await piB.execute(`const self=(await agents.main()).id; const events=await mesh.read({topic:"ops.owner",to:self,limit:50}); return events.filter(e=>e.text===${JSON.stringify(prNeedle)}).map(e=>({id:e.id,kind:e.kind,to:e.to,bridge:e.data?.bridge}));`);
    return events.find((e) => e.kind === "pr.wake" && e.to === b.id && e.bridge?.id === prWake.id && e.bridge.from === cfg.localName);
  });
  check("factoryPrWakeRead", prRead);
  await Promise.all([piA, piB].map((pi) => waitFor("idle before bridge kill", pi.idle)));
  assert(bridge.kill("SIGKILL"), "bridge SIGKILL failed");
  const killedAt = Date.now(); results.bridgeKilledAt = new Date(killedAt).toISOString(); save();
  const firstSendProbe = async (pi, target, host) => {
    const requestAt = Date.now();
    // The outer RPC/model budget is not the native send budget. No retry, no delay,
    // and no preceding peer read that could refresh away the pending-lapse path.
    const v = await pi.execute(`const nativeStartedAt = Date.now(); let outcome; try { outcome = { delivered: await agents.steer(${JSON.stringify(target)}, ${JSON.stringify(`${runId}-after-kill: reply only OK`)} ) }; } catch(error) { outcome = { error: String(error?.message ?? error) }; } const nativeCompletedAt = Date.now(); return { nativeStartedAt, nativeCompletedAt, nativeMs: nativeCompletedAt - nativeStartedAt, ...outcome };`, 120_000);
    const completedAt = Date.now();
    const attempts = [{ ...v, nativeTimestampDomain: "native-execution-host-local",
      driverTimestampDomain: "local-driver", requestAt, completedAt, requestAfterKillMs: requestAt - killedAt,
      completionAfterKillMs: completedAt - killedAt, rpcModelDurationMs: completedAt - requestAt }];
    results.afterKill ??= {}; results.afterKill[host] = attempts; save();
    const validation = assertFirstSend(attempts, target, host, killedAt);
    check(`firstPostkillSend-${host}`, validation);
    return validation;
  };
  const probes = await Promise.allSettled([firstSendProbe(piA, b.id, cfg.remoteName), firstSendProbe(piB, a.id, cfg.localName)]);
  assert(probes.every((p) => p.status === "fulfilled"), probes.filter((p) => p.status === "rejected").map((p) => p.reason.message).join("; "));
  check("bothFirstPostkillSendsNamedBounded", true);
  const afterDeath = await Promise.all([[piA, a.id, b.id], [piB, b.id, a.id]].map(async ([pi, ownId, target]) => {
    const v = await pi.execute(`const main=await agents.main(); const peers=await agents.peers(); return {main:{id:main.id,local:main.local},ownTarget:peers.filter(p=>p.id===${JSON.stringify(target)}).map(p=>({id:p.id,host:p.host})),unrelatedCount:peers.filter(p=>p.id!==${JSON.stringify(a.id)}&&p.id!==${JSON.stringify(b.id)}).length};`);
    assert(v.main.id === ownId && v.main.local === true && v.ownTarget.length === 0, "native main/peers must remain usable after bridge death without the lapsed own target");
    return v;
  }));
  check("nativeMainPeersAfterDeath", afterDeath);
  // First assert natural EOF reaping, before scoped cleanup could mask a leak.
  await waitFor("bridge SSH child exits after SIGKILL", () => !alive(bridgeSsh), 15_000);
  await waitFor("forced remote agent exits on transport EOF", async () => !(await remoteAgents()).some((p) => p.pid === remoteBridge.pid && p.start === remoteBridge.start), 20_000);
  check("transportGoneAfterKill", true);
  results.status = "PASS";
} catch (error) {
  // Even invalid configuration gets a failure receipt, without touching auth.
  if (!evidence) { evidence = path.resolve(".local/forge-bridge-proof", runId); fs.mkdirSync(evidence, { recursive: true, mode: 0o700 }); }
  results.status = "FAIL"; results.failed = String(error?.stack ?? error); save(); console.error(results.failed);
} finally {
  try { await cleanup(); } catch (error) { results.status = "FAIL"; results.cleanupFailure = String(error?.stack ?? error); }
  results.finishedAt = new Date().toISOString(); save();
  console.log(JSON.stringify({ status: results.status, acceptanceScope: results.acceptanceScope, evidence, checks: Object.keys(results.checks), failure: results.failed ?? results.cleanupFailure }));
  process.exitCode = results.status === "PASS" ? 0 : 1;
}
