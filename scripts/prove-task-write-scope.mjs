#!/usr/bin/env node
// Native offline proof: real Pi root -> public agents.spawn/wait -> process worker -> native write/edit/bash.
// Only model inference is deterministic. No live credentials, shared mesh, deletes, installs or service control.
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import http from "node:http";
import crypto from "node:crypto";
import { spawn } from "node:child_process";
import { pathToFileURL } from "node:url";

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
const { readRunLogText } = await import(pathToFileURL(path.join(fabricRoot, "dist/run-log-source.js")));
for (const file of [nativePi, extension, path.join(fabricRoot, "dist/worker.js")]) assert(fs.statSync(file).isFile(), `Missing ${file}`);
assert(process.env.TMPDIR, "Private TMPDIR required");
if (process.platform !== "linux") {
  console.log(JSON.stringify({ passed: false, unsupported: true, reason: "Native process-tree proof requires Linux /proc exit verification", platform: process.platform }));
  process.exit(0);
}
assert(!fs.existsSync(evidence), "Use a fresh evidence directory");
fs.mkdirSync(evidence);
assert.equal(fs.realpathSync(evidence), evidence, "Evidence directory must remain canonical");
const evidenceComponents = [];
for (let component = evidence; ; component = path.dirname(component)) {
  const stat = fs.lstatSync(component);
  assert(stat.isDirectory() && !stat.isSymbolicLink(), "Evidence path must not contain symlinked components");
  evidenceComponents.push({ path: component, dev: stat.dev, ino: stat.ino });
  if (path.dirname(component) === component) break;
}
// Linux directory-relative writes keep the created inode even if an ancestor
// changes after the check. Other platforms recheck each component before writes.
const evidenceFd = process.platform === "linux"
  ? fs.openSync(evidence, fs.constants.O_RDONLY | fs.constants.O_DIRECTORY | fs.constants.O_NOFOLLOW)
  : undefined;
const verifyEvidence = () => {
  for (const component of evidenceComponents) {
    const stat = fs.lstatSync(component.path, { throwIfNoEntry: false });
    assert(stat?.isDirectory() && !stat.isSymbolicLink() && stat.dev === component.dev && stat.ino === component.ino,
      "Evidence path changed after creation");
  }
  assert.equal(fs.realpathSync(evidence), evidence, "Evidence directory must remain canonical");
  if (evidenceFd !== undefined) {
    const stat = fs.fstatSync(evidenceFd), expected = evidenceComponents[0];
    assert(stat.dev === expected.dev && stat.ino === expected.ino, "Evidence directory handle changed");
  }
};
const writeEvidence = (name, content, append = false) => {
  verifyEvidence();
  assert.equal(path.basename(name), name, "Evidence filename must be one component");
  const file = path.join(evidenceFd === undefined ? evidence : `/proc/self/fd/${evidenceFd}`, name);
  const fd = fs.openSync(file, fs.constants.O_WRONLY | fs.constants.O_CREAT |
    (append ? fs.constants.O_APPEND : fs.constants.O_TRUNC) | (fs.constants.O_NOFOLLOW ?? 0), 0o600);
  try { fs.writeFileSync(fd, content); } finally { fs.closeSync(fd); }
};
verifyEvidence();
const scratch = fs.mkdtempSync(path.join(process.env.TMPDIR, "task-write-native-"));
const agent = path.join(scratch, "agent"), rootCwd = path.join(scratch, "root"), runs = path.join(scratch, "runs");
for (const dir of [agent, rootCwd, runs, path.join(scratch, "home"), path.join(scratch, "tmp")]) fs.mkdirSync(dir);
const json = (file, value) => {
  const content = `${JSON.stringify(value, null, 2)}\n`;
  if (path.dirname(file) === evidence) writeEvidence(path.basename(file), content);
  else fs.writeFileSync(file, content);
};
const forbidden = "/tmp/fv2-writescope-probe"; // Explicit assignment's refused real write target; never delete it.
assert(!fs.existsSync(forbidden), "Refused target already exists; do not touch an existing fixture");
const outside = path.join(scratch, "outside-task.txt");
const shellOutside = path.join(scratch, "native-shell-outside-task.txt");
// Native model tools exercise cwd independently of generator-owned artifacts.
const cwdFile = path.join(workspace, `${path.basename(scratch)}-worktree-write.txt`);
assert(!fs.existsSync(cwdFile), "Use a fresh native worktree write target");
const toolResults = [], childRequests = [], parentEvents = [];
let child, server, parentExit, failure, rejectSettlement, nativePgid, nativeExitReceipt, nativeClose;
const ownedProcesses = new Map();
const spawnRecords = path.join(scratch, "native-spawns.jsonl");
fs.writeFileSync(spawnRecords, "");
// Record child births synchronously, before a custodian can exit. This is the
// stat-only fallback's custody receipt, not a scan of other processes' state.
const spawnTracker = path.join(scratch, "record-native-spawns.mjs");
fs.writeFileSync(spawnTracker, `import fs from 'node:fs'; import cp from 'node:child_process'; import {syncBuiltinESMExports} from 'node:module';
const identity=pid=>{try{const f=fs.readFileSync('/proc/'+pid+'/stat','utf8').split(') ').at(-1).split(' ');return {pid:Number(pid),ppid:Number(f[1]),pgid:Number(f[2]),sid:Number(f[3]),state:f[0],started:f[19]}}catch(e){if(e.code==='ENOENT'||e.code==='ESRCH')return;throw e}};
const original=cp.spawn;
cp.spawn=function(...args){const parent=identity(process.pid), child=original.apply(this,args), birth=child.pid&&identity(child.pid);
if(parent&&birth)fs.appendFileSync(${JSON.stringify(spawnRecords)},JSON.stringify({parent,child:birth})+'\\n');return child};
syncBuiltinESMExports();`);
let nativeCgroup;
try {
  const hierarchy = fs.readFileSync("/proc/self/cgroup", "utf8").split("\n").find(line => line.startsWith("0::"));
  if (!hierarchy) throw new Error("cgroup v2 unavailable");
  const directory = path.join("/sys/fs/cgroup", hierarchy.slice(3), `fabric-write-proof-${crypto.randomUUID()}`);
  fs.mkdirSync(directory, { mode: 0o700 });
  const stat = fs.statSync(directory);
  nativeCgroup = { directory, dev: stat.dev, ino: stat.ino };
} catch (error) {
  console.error(`NATIVE_OWNERSHIP_FALLBACK: ${error.message}`);
}
const cgroupMembers = () => {
  const stat = fs.lstatSync(nativeCgroup.directory);
  assert(stat.isDirectory() && !stat.isSymbolicLink() && stat.dev === nativeCgroup.dev && stat.ino === nativeCgroup.ino, "Native cgroup identity changed");
  const members = [];
  const visit = directory => {
    members.push(...fs.readFileSync(path.join(directory, "cgroup.procs"), "utf8").trim().split(/\s+/).filter(Boolean).map(Number));
    for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
      assert(!entry.isSymbolicLink(), "Native cgroup must not contain links");
      if (entry.isDirectory()) visit(path.join(directory, entry.name));
    }
  };
  visit(nativeCgroup.directory);
  return members;
};
const procIdentity = pid => {
  try {
    const fields = fs.readFileSync(`/proc/${pid}/stat`, "utf8").split(") ").at(-1).split(" ");
    return { pid: Number(pid), ppid: Number(fields[1]), pgid: Number(fields[2]), sid: Number(fields[3]), state: fields[0], started: fields[19] };
  } catch (error) {
    if (error.code === "ENOENT" || error.code === "ESRCH") return undefined;
    throw error;
  }
};
const ownedAlive = () => {
  const processes = (nativeCgroup ? cgroupMembers() : fs.readdirSync("/proc").filter(name => /^\d+$/.test(name)))
    .map(procIdentity).filter(Boolean);
  // Private spawn receipts retain an owned parent birth even after reparenting.
  const rows = fs.readFileSync(spawnRecords, "utf8").split("\n").filter(Boolean).map(line => JSON.parse(line));
  let admitted;
  do {
    admitted = false;
    for (const row of rows) if (ownedProcesses.get(row.parent.pid)?.started === row.parent.started) {
      const current = procIdentity(row.child.pid);
      if (current?.started === row.child.started && ownedProcesses.get(current.pid)?.started !== current.started) {
        ownedProcesses.set(current.pid, current); admitted = true;
      }
    }
  } while (admitted);
  const ownedSessions = new Set([...ownedProcesses.values()].filter(owned =>
    owned.started === procIdentity(owned.pid)?.started).map(owned => owned.sid));
  for (const identity of processes) if (nativeCgroup || ownedSessions.has(identity.sid)) {
    ownedProcesses.set(identity.pid, identity);
  }
  // Also capture descendants which intentionally start with an empty environment.
  const liveIds = new Set([...ownedProcesses.values()].filter(identity =>
    processes.some(current => current.pid === identity.pid && current.started === identity.started)).map(identity => identity.pid));
  let previous;
  do {
    previous = liveIds.size;
    for (const identity of processes) if (liveIds.has(identity.ppid)) {
      liveIds.add(identity.pid); ownedProcesses.set(identity.pid, identity);
    }
  } while (liveIds.size !== previous);
  return [...ownedProcesses.values()].filter(identity => {
    const current = procIdentity(identity.pid);
    return current && current.started === identity.started && !["Z", "X"].includes(current.state);
  });
};
const signalOwned = signal => {
  const live = ownedAlive();
  const groups = new Set(live.map(identity => identity.pgid));
  // detached:true makes the root PID its process-group ID; descendants may own other groups.
  if (live.some(identity => identity.pid === nativePgid)) groups.add(nativePgid);
  if (nativeCgroup) {
    const pids = [];
    for (const identity of live) {
      const current = procIdentity(identity.pid);
      if (current?.started !== identity.started) continue;
      try { process.kill(identity.pid, signal); pids.push(identity.pid); } catch (error) { if (error.code !== "ESRCH") throw error; }
    }
    return { signal, pids };
  }
  for (const pgid of groups) {
    assert(pgid > 1 && pgid !== procIdentity(process.pid)?.pgid, "Refuse signalling the proof's own group");
    const members = live.filter(identity => identity.pgid === pgid);
    if (!members.some(identity => {
      const current = procIdentity(identity.pid);
      return current?.started === identity.started && current.pgid === pgid;
    })) continue;
    try { process.kill(-pgid, signal); } catch (error) { if (error.code !== "ESRCH") throw error; }
  }
  return { signal, groups: [...groups] };
};
const joinNative = async () => {
  if (nativeExitReceipt || !child) return nativeExitReceipt;
  const signals = [];
  child.stdin.end();
  if (ownedAlive().length) {
    signals.push(signalOwned("SIGTERM"));
    // One bounded grace interval, not a recurring process census or retry loop.
    const termAt = Date.now();
    await bounded(parentExit, "owned native TERM close", 5000).catch(() => {});
    if (ownedAlive().length) await new Promise(resolve => setTimeout(resolve, Math.max(0, 5000 - (Date.now() - termAt))));
    if (ownedAlive().length) {
      signals.push(signalOwned("SIGKILL"));
      await bounded(parentExit, "owned native KILL close", 5000);
      // Reparented detached children have no ChildProcess close event here.
      await new Promise(resolve => setTimeout(resolve, 100));
    }
  }
  await bounded(parentExit, "owned native close", 5000);
  const remaining = ownedAlive();
  assert.equal(remaining.length, 0, `Owned native descendants remain: ${JSON.stringify(remaining)}`);
  if (nativeCgroup) {
    assert.equal(cgroupMembers().length, 0, "Native cgroup is not empty");
    assert(fs.readFileSync(path.join(nativeCgroup.directory, "cgroup.events"), "utf8").split("\n").includes("populated 0"), "Native cgroup has populated descendants");
  }
  nativeExitReceipt = { ownership: nativeCgroup ? "cgroup-v2" : "stat-and-spawn-receipts", cgroup: nativeCgroup?.directory,
    pgid: nativePgid, signals, identities: [...ownedProcesses.values()], remaining: [] };
  console.error(`OWNED_NATIVE_EXIT: ${JSON.stringify(nativeExitReceipt)}`);
  return nativeExitReceipt;
};
// A failed join is not retried in finally: the whole cleanup remains finitely bounded.
const closeNative = () => nativeClose ??= joinNative();
const protectedRecords = [];
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
    // Served-model admission uses a bodyless discovery request before spawn.
    if (request.method === "GET" && request.url === "/v1/models") {
      response.writeHead(200, { "Content-Type": "application/json" });
      response.end(JSON.stringify({ object: "list", data: ["parent", "child"].map(id => ({ id, object: "model" })) }));
      return;
    }
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
          const run = path.join(runs, runId), tmp = path.join(run, "tmp");
          const hostWrites = ["completion-recipient.json", "status.json", "events.jsonl"].flatMap(name => {
            const file = path.join(run, name), before = fs.readFileSync(file, "utf8");
            protectedRecords.push({ name, file, before });
            return [["write", { path: file, content: "HOST_RECORD_TAMPERED" }],
              ["edit", { path: file, edits: [{ oldText: before, newText: "HOST_RECORD_TAMPERED" }] }]];
          });
          stream.tools([
            ...hostWrites,
            ["write", { path: forbidden, content: "MUST_BE_REFUSED" }],
            ["write", { path: cwdFile, content: "WORKTREE_ALLOWED" }],
            ["edit", { path: cwdFile, edits: [{ oldText: "WORKTREE_ALLOWED", newText: "WORKTREE_EDIT_ALLOWED" }] }],
            ["write", { path: path.join(tmp, "scratch-write.txt"), content: "SCRATCH_ALLOWED" }],
            ["bash", { command: `printf 'CHILD_TMPDIR=%s\\n' "$TMPDIR"; printf 'SHELL_GAP_ALLOWED' > "$TASK_WRITE_SCOPE_SHELL_OUTSIDE"`, timeout: 10 }],
          ]);
        } else if (scenario === "readonly") stream.tools([["write", { path: path.join(evidence, "readonly-refused.txt"), content: "MUST_BE_REFUSED" }]]);
        else stream.tools([["write", { path: outside, content: "UNRESTRICTED_ALLOWED" }]]);
      } catch (error) { failure ??= error; rejectSettlement?.(error); response.destroy(error); }
    });
  });
  await new Promise(resolve => server.listen(0, "127.0.0.1", resolve));
  const port = server.address().port;
  json(path.join(agent, "models.json"), { providers: { "task-write-proof": { baseUrl: `http://127.0.0.1:${port}/v1`, apiKey: "keyless-offline-fixture", api: "openai-completions", models: ["parent", "child"].map(id => ({ id, name: id, reasoning: false, input: ["text"], contextWindow: 128000, maxTokens: 4096, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } })) } } });
  json(path.join(agent, "settings.json"), { enableInstallTelemetry: false, compaction: { enabled: false }, retry: { enabled: false }, extensions: [], packages: [] });
  json(path.join(agent, "fabric.json"), { configVersion: 4, fullCodeMode: false, autoReload: false, executor: { kernel: "typescript", timeoutMs: guard, maxTimeoutMs: guard, resultFormat: "json", landlock: { mode: "off" } }, agents: { enabled: true, runner: "pi", transport: "process", timeoutMs: guard, maxDepth: 2, maxConcurrent: 2, extensions: false, retainRuns: true, sessionExport: false, notifyOnComplete: false, deniedModels: [], modelPolicy: { requireReason: [] } }, mesh: { enabled: true, root: path.join(scratch, "mesh"), announce: true, actorScope: "session" }, capture: { enabled: false }, mcp: { enabled: false }, jev: { enabled: false }, prewalk: { enabled: false }, ui: { enabled: false }, compaction: { engine: "pi" }, approvals: { read: "allow", write: "allow", execute: "allow", network: "allow", agent: "allow" } });
  const env = Object.fromEntries(Object.entries(process.env).filter(([key]) => !/^(PI_|SMARTY_|HERDR_|ORG_|OPENAI_|ANTHROPIC_|GOOGLE_|GEMINI_|OPENROUTER_|TYPESAFE_|AWS_|AZURE_|AI_GATEWAY_|OTEL_|SENTRY_)/.test(key)));
  Object.assign(env, { NODE_OPTIONS: `${env.NODE_OPTIONS ?? ""} --import=${JSON.stringify(pathToFileURL(spawnTracker).href)}`, HOME: path.join(scratch, "home"), XDG_CONFIG_HOME: path.join(scratch, "home/.config"), XDG_DATA_HOME: path.join(scratch, "home/.local/share"), XDG_CACHE_HOME: path.join(scratch, "home/.cache"), TMPDIR: path.join(scratch, "tmp"), TASK_WRITE_SCOPE_SHELL_OUTSIDE: shellOutside, PI_CODING_AGENT_DIR: agent, PI_OFFLINE: "1", PI_SKIP_VERSION_CHECK: "1", PI_TELEMETRY: "0", PI_FABRIC_PI_BINARY: nativePi, PI_FABRIC_PROJECT_ROOT: rootCwd, PI_FABRIC_MESH_ROOT: path.join(scratch, "mesh"), PI_FABRIC_RUN_ROOT: runs, PI_FABRIC_AGENT_DIR: path.join(scratch, "exports") });
  let launcher = nativePi;
  if (nativeCgroup) {
    launcher = path.join(scratch, "join-native-cgroup.mjs");
    fs.writeFileSync(launcher, `import fs from 'node:fs';
fs.writeFileSync(${JSON.stringify(path.join(nativeCgroup.directory, "cgroup.procs"))},String(process.pid));
process.argv[1]=${JSON.stringify(nativePi)};
await import(${JSON.stringify(pathToFileURL(nativePi).href)});`);
  }
  child = spawn(process.execPath, [launcher, "--mode", "rpc", "--no-session", "--provider", "task-write-proof", "--model", "parent", "--thinking", "off", "--no-context-files", "--no-skills", "--no-prompt-templates", "--no-themes", "--no-extensions", "--extension", extension, "--approve"], { cwd: rootCwd, env, detached: true, stdio: ["pipe", "pipe", "pipe"] });
  nativePgid = child.pid;
  const rootIdentity = procIdentity(child.pid);
  if (rootIdentity) ownedProcesses.set(rootIdentity.pid, rootIdentity);
  parentExit = new Promise(resolve => child.once("close", (code, signal) => resolve({ code, signal })));
  json(path.join(evidence, "native-launch.json"), { pid: child.pid, pgid: nativePgid, identity: rootIdentity });
  const settled = new Promise((resolve, reject) => {
    rejectSettlement = reject;
    child.once("error", reject);
    child.stdout.on("data", chunk => {
      try {
        ownedAlive();
        writeEvidence("parent-events.jsonl", chunk, true);
        tail += chunk;
        while (tail.includes("\n")) {
          const index = tail.indexOf("\n"), line = tail.slice(0, index); tail = tail.slice(index + 1);
          if (!line.trim()) continue;
          const event = JSON.parse(line); parentEvents.push(event); if (event.type === "agent_settled") resolve();
        }
      } catch (error) { reject(error); }
    });
    child.stderr.on("data", chunk => {
      try { ownedAlive(); writeEvidence("parent-stderr.log", chunk, true); } catch (error) { reject(error); }
    });
  });
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
    events: readRunLogText(path.join(runs, item.handle.id, "events.jsonl")).trim().split("\n").map(line => JSON.parse(line)),
  }));
  assert(records.every(item => item.result.status === "completed"), JSON.stringify(records));
  assert(!fs.existsSync(forbidden), "Default write escaped its roots");
  assert.equal(fs.readFileSync(cwdFile, "utf8"), "WORKTREE_EDIT_ALLOWED");
  const defaultRun = records.find(item => item.scenario === "default");
  const tmp = path.join(runs, defaultRun.result.id, "tmp");
  assert.equal(fs.readFileSync(path.join(tmp, "scratch-write.txt"), "utf8"), "SCRATCH_ALLOWED");
  const defaultTools = toolResults.find(item => item.scenario === "default").messages;
  assert(defaultTools.some(message => JSON.stringify(message).includes("outside writable roots")), "Native write must record its refusal, not merely skip the attempt");
  for (const record of protectedRecords) {
    const refusals = defaultTools.filter(message => JSON.stringify(message).includes(record.file) && JSON.stringify(message).includes("outside writable roots"));
    assert.equal(refusals.length, 2, `Native write and edit must refuse ${record.name}`);
    const current = record.name === "events.jsonl" ? readRunLogText(record.file) : fs.readFileSync(record.file, "utf8");
    if (record.name === "completion-recipient.json") assert.equal(current, record.before);
    else if (record.name === "events.jsonl") assert(current.startsWith(record.before), "Host event history must not be overwritten");
    else assert.equal(JSON.parse(current).id, path.basename(path.dirname(record.file)), "Host status identity must survive");
  }
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
  await closeNative();
  const summary = { passed: true, nativeExitReceipt, route: "real native Pi RPC root -> public Fabric agents.spawn/wait -> compiled process worker -> real native Pi write/edit/bash", workspace, fabricRoot, nativePi, scratch,
    refused: forbidden, worktreeWriteAndEdit: "allowed", childScratchWrite: "allowed", hostRecordWriteAndEdit: "refused (completion-recipient.json, status.json, events.jsonl)", explicitReadOnly: "refused", unrestrictedOutsideWrite: "allowed in proof-owned scratch", optOutLogCount: 1,
    nativeBashGap: "allowed outside child roots, but inside proof-owned scratch; Landlock deliberately off", parentNativeExit: 0,
    artifacts: Object.fromEntries([extension, path.join(fabricRoot, "dist/worker.js")].map(file => [file, crypto.createHash("sha256").update(fs.readFileSync(file)).digest("hex")])),
  };
  json(path.join(evidence, "summary.json"), summary);
  console.log(JSON.stringify(summary, null, 2));
} catch (error) {
  failure ??= error;
  // Failure may mean the output directory is no longer trusted. Never write
  // even a failure artifact through that path; stderr is the safe receipt.
  console.error(failure.stack);
  process.exitCode = 1;
} finally {
  try { await closeNative(); }
  catch (error) {
    console.error(error.stack);
    try { console.error(`OWNED_NATIVE_EXIT_FAILED: ${JSON.stringify({ pgid: nativePgid, remaining: ownedAlive() })}`); }
    catch (inspectionError) { console.error(inspectionError.stack); }
    // Even an uninterruptible child must not keep a failed proof alive via its pipes.
    child?.stdin.destroy(); child?.stdout.destroy(); child?.stderr.destroy(); child?.unref();
    process.exitCode = 1;
  }
  finally {
    if (server) { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); }
    if (evidenceFd !== undefined) fs.closeSync(evidenceFd);
  }
  // Retain proof-owned scratch. The allocating Main decides cleanup after reading evidence.
}
