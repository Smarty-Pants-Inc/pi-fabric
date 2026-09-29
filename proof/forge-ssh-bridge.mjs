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
const cleanEnv = () => Object.fromEntries(Object.entries(process.env).filter(([key]) => !/^(PI_|HERDR|FABRIC)/.test(key)));
const piArgs = (fabric, sessionDir) => ["--mode", "rpc", "-ne", "-e", fabric, "--model", cfg.model, "--thinking", "medium", "--session-dir", sessionDir];
const rpc = (p, name) => {
  const listeners = new Set(), ingress = [], waits = new Set();
  let buffer = "", failure;
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
      if (record.type === "message_end") remember(record.message);
      for (const listener of [...listeners]) listener(record);
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
    try { p.stdin.write(`${JSON.stringify({ id, type, ...fields })}\n`, (error) => { if (error) done(error); }); }
    catch (error) { done(error); }
  });
  const idle = async () => { const state = await request("get_state"); return !state.isStreaming; };
  const execute = async (code, ms = 120_000) => {
    const started = Date.now();
    await waitFor(`${name} idle`, idle, Math.min(ms, 60_000));
    const token = randomUUID();
    const wrapped = `const value = await (async () => { ${code}\n})(); return { proofToken: ${JSON.stringify(token)}, value };`;
    return new Promise((resolve, reject) => {
      const error = unavailable();
      if (error) return reject(error);
      let toolId, settled = false;
      const done = (error, value) => {
        if (settled) return;
        settled = true; clearTimeout(timer); listeners.delete(on); waits.delete(done); pending.delete(done);
        error ? reject(error) : resolve(value);
      };
      const on = (record) => {
        if (settled) return;
        if (record.type === "tool_execution_start" && record.toolName === "fabric_exec" && record.args?.code === wrapped) toolId = record.toolCallId;
        if (record.type !== "tool_execution_end" || !toolId || record.toolCallId !== toolId) return;
        if (record.isError || record.result?.isError || record.result?.details?.success === false) return done(new Error(`${name}: fabric_exec failed`));
        const text = (record.result?.content ?? []).filter((c) => c.type === "text").map((c) => c.text).join("\n");
        let parsed;
        // resultFormat=json; permit host rendering around the JSON, not model prose.
        for (let a = text.indexOf("{"); a >= 0 && !parsed; a = text.indexOf("{", a + 1)) {
          for (let b = text.lastIndexOf("}"); b > a; b = text.lastIndexOf("}", b - 1)) {
            try { const v = JSON.parse(text.slice(a, b + 1)); if (v.proofToken === token) { parsed = v; break; } } catch {}
          }
        }
        if (!parsed) return done(new Error(`${name}: actual tool result lacks proof token / valid JSON`));
        results.toolExecutions ??= [];
        results.toolExecutions.push({ side: name, token, toolCallId: toolId, isError: false }); save();
        done(null, parsed.value);
      };
      const timer = setTimeout(() => done(new Error(`${name}: tool execution deadline`)), Math.max(1, ms - (Date.now() - started)));
      listeners.add(on); waits.add(done); pending.add(done);
      request("prompt", { message: `Call fabric_exec exactly once with exactly the code below, byte for byte, resultFormat "json", timeoutMs ${Math.min(ms, 120_000)}. Do not call any other tool or communicate with any other root. Then reply only OK.\n${wrapped}` }, ms).catch((error) => done(error));
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
  return { p, execute, idle, received, request };
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
  bridge.kill("SIGKILL");
  const killedAt = Date.now(); results.bridgeKilledAt = new Date(killedAt).toISOString(); save();
  const lapseProbe = async (pi, target, host) => {
    const attempts = [], deadline = killedAt + 60_000;
    while (Date.now() < deadline) {
      const start = Date.now();
      const v = await pi.execute(`const nativeStartedAt = Date.now(); try { const delivered = await agents.steer(${JSON.stringify(target)}, ${JSON.stringify(`${runId}-after-kill: reply only OK`)}); return { nativeStartedAt, nativeMs: Date.now() - nativeStartedAt, delivered }; } catch(error) { return { nativeStartedAt, nativeMs: Date.now() - nativeStartedAt, error: String(error?.message ?? error) }; }`, Math.max(1, deadline - Date.now()));
      const completedAt = Date.now();
      attempts.push({ afterKillMs: start - killedAt, completionAfterKillMs: completedAt - killedAt, durationMs: completedAt - start, ...v });
      results.afterKill ??= {}; results.afterKill[host] = attempts; save();
      if (v.error?.includes(target) && v.error.includes(`remote host ${host}`) && /lapsed/.test(v.error) && /mesh bridge/.test(v.error)) {
        assert(completedAt <= deadline, `${host}: named lapse completed after the 60s deadline`);
        return attempts;
      }
      await sleep(1000); // One bounded attempt per second; ack timeout may occur first.
    }
    throw new Error(`${host}: named mirrored-lease lapsed error absent within 60s`);
  };
  const probes = await Promise.allSettled([lapseProbe(piA, b.id, cfg.remoteName), lapseProbe(piB, a.id, cfg.localName)]);
  assert(probes.every((p) => p.status === "fulfilled"), probes.filter((p) => p.status === "rejected").map((p) => p.reason.message).join("; "));
  check("bothNamedLapsedErrors", true);
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
