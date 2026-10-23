#!/usr/bin/env node
// Real-entry Linux idle measurement, not a timer/ResidentHost stand-in.
// No build, network, Git writes, fleet paths, synthetic peer processes, or trace deletion.
// See --help. Run baseline and candidate SEQUENTIALLY under the caller's owned reaper.
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { execFileSync, spawn } from "node:child_process";
import { fileURLToPath, pathToFileURL } from "node:url";
import { syncBuiltinESMExports } from "node:module";

const BASELINE = "0c03497f66088f9a33184bc4b2f010de45a6f9af";
const DEFAULT_PI = "/home/paul/.local/share/smarty-dev/pi-runtime/releases/eacc4e45018f670843766711e4bf304a3572b1eb/node/node_modules/@earendil-works/pi-coding-agent";
const SCRIPT = fileURLToPath(import.meta.url);
const REPO = path.resolve(path.dirname(SCRIPT), "..");
const FINDINGS = path.resolve(REPO, "../lane-input/FINDINGS.md");
const hash = bytes => createHash("sha256").update(bytes).digest("hex");
const readJson = file => JSON.parse(fs.readFileSync(file, "utf8"));
const json = (file, value) => fs.writeFileSync(file, JSON.stringify(value, null, 2) + "\n", { mode: 0o600 });
const inside = (root, file) => file.startsWith(root + path.sep);
function toolJson(result) {
  assert(!result.isError && result.details?.success === true, "native Fabric invocation failed");
  const { outputFormat, outputFormatStartLine: start, outputFormatLines: count } = result.details;
  assert(outputFormat === "json" && Number.isSafeInteger(start) && start >= 0 && Number.isSafeInteger(count) && count > 0,
    "native Fabric must identify its structured JSON output range");
  const lines = result.content.filter(block => block.type === "text").map(block => block.text).join("\n").split("\n");
  assert(start + count <= lines.length, "native Fabric JSON range exceeds output");
  // Native diagnostics (including circuit-breaker warnings) may follow the JSON.
  // Read only the range the tool identifies; retain the complete receipt in the trace.
  return JSON.parse(lines.slice(start, start + count).join("\n"));
}
const help = `Usage (Linux, Node >=24; run with native node, not bun):
  node scripts/benchmark-idle-mesh.mjs --fabric-root ROOT --label baseline|candidate --out FILE
    [--pi-package ${DEFAULT_PI}]
    [--seconds 120] [--warmup-seconds 30] [--backend file|sqlite] [--scenario renewal|static]
    [--preflight|--startup-only]
  node scripts/benchmark-idle-mesh.mjs --compare BASELINE_JSON CANDIDATE_JSON --out FILE
  node scripts/benchmark-idle-mesh.mjs --self-test

ROOT contains an UNMODIFIED built dist/, package.json and installed node_modules/.
Baseline additionally requires source-head containing exactly ${BASELINE}.
This harness never builds or imports product src/. --preflight hashes/validates inputs only.
--startup-only proves native entry/config/directory readiness and stops WITHOUT an idle window.
Each measurement starts exactly ONE actual Pi SDK session in ONE child process, loads
ROOT/dist/index.js via Pi's native resource loader, and activates its real fabric_exec.
100 participants (10 roots, 90 actors), 10 logical directory hosts (not 10 physical
machines), ~3000 keyed records, participant files and host leases are private fixtures.
Default renewal scenario: this parent (NOT the measured Pi) atomically renews the nine
fixture host leases every 5 s, without turns or inbound work. Static is a separately
labelled quiet control with long unexpired leases. Neither scenario adds keyed-state churn.
The parent reads /proc/CHILD/stat and /proc/CHILD/io at the two window boundaries.
Default: 30 s warmup AFTER readiness, then 120 s idle. Windows <120 s are refused.
All child/session traces, profiles and mesh state are KEPT in a mode-0700 mkdtemp directory.
No prompts or provider inference are sent. Network entry points are fail-closed.
No speedup or budget is assumed: comparison reports actual absolute and relative deltas.
`;

function options(argv) {
  const result = { seconds: 120, warmup: 30, backend: "file", scenario: "renewal", piPackage: DEFAULT_PI };
  const names = { "--fabric-root": "fabricRoot", "--label": "label", "--out": "out", "--pi-package": "piPackage",
    "--seconds": "seconds", "--warmup-seconds": "warmup", "--backend": "backend", "--scenario": "scenario" };
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === "--preflight") { result.preflight = true; continue; }
    if (argv[i] === "--startup-only") { result.startupOnly = true; continue; }
    if (argv[i] === "--compare") {
      assert(!result.compare && argv[i + 1] && argv[i + 2], "--compare needs two JSON files");
      result.compare = [path.resolve(argv[++i]), path.resolve(argv[++i])]; continue;
    }
    const key = names[argv[i]];
    assert(key && argv[i + 1] && !argv[i + 1].startsWith("--"), `unknown/missing option: ${argv[i]}`);
    result[key] = argv[++i];
  }
  if (result.compare) { assert(result.out, "--compare requires --out"); return result; }
  assert(["baseline", "candidate"].includes(result.label), "--label baseline|candidate required");
  assert(result.fabricRoot && result.out, "--fabric-root and --out required");
  result.seconds = Number(result.seconds); result.warmup = Number(result.warmup);
  assert(Number.isSafeInteger(result.seconds) && result.seconds >= 120 && result.seconds <= 3600, "--seconds must be 120..3600");
  assert(Number.isSafeInteger(result.warmup) && result.warmup >= 1 && result.warmup <= 300, "--warmup-seconds must be 1..300");
  assert(["file", "sqlite"].includes(result.backend), "--backend file|sqlite required");
  assert(["renewal", "static"].includes(result.scenario), "--scenario renewal|static required");
  return result;
}

// Reject symlinked dist content: both the manifest and Pi must consume this artifact,
// not a moving release/current link. node_modules links are package-manager inputs only.
function artifact(rootInput) {
  const root = fs.realpathSync(rootInput), files = {};
  const walk = relative => {
    const file = path.join(root, relative), stat = fs.lstatSync(file);
    assert(!stat.isSymbolicLink(), `artifact symlink refused: ${file}`);
    if (stat.isDirectory()) for (const name of fs.readdirSync(file).sort()) walk(path.join(relative, name));
    else { assert(stat.isFile(), `not an artifact file: ${file}`); files[relative] = hash(fs.readFileSync(file)); }
  };
  for (const relative of ["dist/index.js", "dist/mesh.js", "dist/config.js", "dist/topology/host-leases.js", "node_modules"])
    assert(fs.existsSync(path.join(root, relative)), `missing ${root}/${relative}; parent must prepare dependencies/build first`);
  for (const relative of ["dist", "package.json", "skillsets", "native", "config"]) walk(relative);
  const sourceHead = fs.existsSync(path.join(root, "source-head")) ? fs.readFileSync(path.join(root, "source-head"), "utf8").trim() : null;
  return { root, sourceHead, sha256: hash(JSON.stringify(files)), files };
}

function parseStat(text) {
  const end = text.lastIndexOf(")");
  assert(end > 0, "malformed /proc stat");
  const fields = text.slice(end + 2).trim().split(/\s+/);
  const number = i => { const n = Number(fields[i]); assert(Number.isSafeInteger(n) && n >= 0, "invalid stat counter"); return n; };
  assert(!["Z", "X", "x"].includes(fields[0]), "measured Pi is dead/zombie");
  return { utime: number(11), stime: number(12), startTicks: fields[19] };
}
function parseIo(text) {
  const counters = {};
  for (const line of text.trim().split("\n")) {
    const match = /^([a-z_]+):\s+(\d+)$/.exec(line);
    assert(match, "malformed /proc io");
    counters[match[1]] = Number(match[2]);
    assert(Number.isSafeInteger(counters[match[1]]), "io counter exceeds safe integer range");
  }
  for (const key of ["rchar", "wchar", "syscr", "syscw", "read_bytes", "write_bytes"]) assert(key in counters, `missing io ${key}`);
  return counters;
}
function sample(pid) {
  const began = process.hrtime.bigint();
  const stat = parseStat(fs.readFileSync(`/proc/${pid}/stat`, "utf8"));
  const io = parseIo(fs.readFileSync(`/proc/${pid}/io`, "utf8"));
  const ended = process.hrtime.bigint();
  return { pid, at: new Date().toISOString(), monotonicNs: String((began + ended) / 2n), readDurationNs: String(ended - began), stat, io };
}
function metrics(before, after, hz) {
  assert.equal(before.pid, after.pid); assert.equal(before.stat.startTicks, after.stat.startTicks, "PID identity changed");
  const elapsedSeconds = Number(BigInt(after.monotonicNs) - BigInt(before.monotonicNs)) / 1e9;
  assert(elapsedSeconds > 0 && hz > 0);
  const cpuTicks = after.stat.utime + after.stat.stime - before.stat.utime - before.stat.stime;
  const delta = {};
  for (const key of Object.keys(before.io)) { delta[key] = after.io[key] - before.io[key]; assert(delta[key] >= 0, `counter regressed: ${key}`); }
  assert(cpuTicks >= 0);
  return { elapsedSeconds, cpuTicks, cpuSeconds: cpuTicks / hz, cpuCores: cpuTicks / hz / elapsedSeconds,
    syscrPerSecond: delta.syscr / elapsedSeconds, rcharBytesPerSecond: delta.rchar / elapsedSeconds,
    syscwPerSecond: delta.syscw / elapsedSeconds, wcharBytesPerSecond: delta.wchar / elapsedSeconds,
    diskReadBytesPerSecond: delta.read_bytes / elapsedSeconds, diskWriteBytesPerSecond: delta.write_bytes / elapsedSeconds, ioDelta: delta };
}
function selfTest() {
  const fields = Array(22).fill("0"); fields[0] = "S"; fields[11] = "123"; fields[12] = "17"; fields[19] = "91234";
  assert.deepEqual(parseStat(`42 (pi (idle) worker) ${fields.join(" ")}`), { utime: 123, stime: 17, startTicks: "91234" });
  const io = parseIo("rchar: 1000\nwchar: 100\nsyscr: 20\nsyscw: 2\nread_bytes: 0\nwrite_bytes: 0\ncancelled_write_bytes: 0\n");
  const before = { pid: 42, monotonicNs: "1000000000", stat: { utime: 100, stime: 20, startTicks: "10" }, io };
  const after = { pid: 42, monotonicNs: "121000000000", stat: { utime: 1100, stime: 220, startTicks: "10" }, io: { ...io, rchar: 121000, syscr: 2420 } };
  const result = metrics(before, after, 100);
  assert.equal(result.cpuCores, 0.1); assert.equal(result.syscrPerSecond, 20); assert.equal(result.rcharBytesPerSecond, 1000);
  assert.throws(() => metrics(before, { ...after, stat: { ...after.stat, startTicks: "11" } }, 100));
  assert.throws(() => options(["--fabric-root", ".", "--label", "candidate", "--out", "x", "--seconds", "1"]));
  assert.deepEqual(toolJson({ content: [{ type: "text", text: '{"members":100}\n\n[circuit-breaker] diagnostic' }],
    details: { success: true, outputFormat: "json", outputFormatStartLine: 0, outputFormatLines: 1 } }), { members: 100 });
  assert.throws(() => toolJson({ content: [], details: { success: false } }));
  console.log("idle-mesh self-test PASS (parsing/math/argument checks only; NOT an idle runtime measurement)");
}

// This process is the only Pi runtime in a measurement. Parent and worker communicate
// over Node IPC; the IPC channel keeps the native session alive, with NO stand-in interval.
async function worker(specFile) {
  assert(process.send, "worker must be started by this harness over IPC");
  const spec = readJson(specFile), root = spec.root, profile = path.join(root, "profile"), cwd = path.join(root, "cwd"), meshRoot = path.join(root, "mesh");
  const trace = row => fs.appendFileSync(path.join(root, "child-events.jsonl"), JSON.stringify({ at: new Date().toISOString(), ...row }) + "\n");
  const send = row => { if (process.connected) process.send(row); };
  const fail = error => { const text = String(error?.stack ?? error); trace({ type: "fatal", error: text }); send({ type: "fatal", error: text }); };
  let session, shutdown;
  const stop = () => shutdown ??= (async () => {
    try {
      if (session) {
        await session.abort();
        await session.extensionRunner.emit({ type: "session_shutdown", reason: "exit" });
        session.dispose();
      }
      trace({ type: "shutdown", orderly: true });
      if (process.connected) await new Promise((resolve, reject) => process.send({ type: "stopped" }, error => error ? reject(error) : resolve()));
      process.exit(0);
    } catch (error) { fail(error); process.exit(1); }
  })();
  process.on("SIGTERM", () => void stop()); process.on("SIGINT", () => void stop());
  process.on("disconnect", () => void stop());
  process.on("uncaughtException", error => { fail(error); process.exit(1); });
  process.on("unhandledRejection", error => { fail(error); process.exit(1); });

  // Offline flags alone are not an egress fence. Block Node's TCP/TLS/HTTP/DNS/UDP
  // entry points before importing the SDK or product. No product polling API is mocked.
  let networkAttempts = 0, agentStarts = 0;
  const blocked = operation => function () {
    networkAttempts++;
    const error = new Error(`idle benchmark forbids network: ${operation}`);
    fail(error); throw error;
  };
  for (const [module, names] of [["node:http", ["request", "get"]], ["node:https", ["request", "get"]],
    ["node:net", ["connect", "createConnection"]], ["node:tls", ["connect"]],
    ["node:dns", ["lookup", "resolve", "resolve4", "resolve6"]]]) {
    const object = (await import(module)).default;
    for (const name of names) object[name] = blocked(`${module}.${name}`);
  }
  const net = (await import("node:net")).default;
  net.Socket.prototype.connect = blocked("Socket.connect");
  const dgram = (await import("node:dgram")).default;
  dgram.Socket.prototype.send = blocked("dgram.send");
  const dns = (await import("node:dns/promises")).default;
  for (const name of ["lookup", "resolve", "resolve4", "resolve6"]) dns[name] = blocked(`dns.promises.${name}`);
  globalThis.fetch = blocked("fetch"); syncBuiltinESMExports();

  const product = relative => pathToFileURL(path.join(spec.fabricRoot, "dist", relative)).href;
  const { MeshStore } = await import(product("mesh.js"));
  const { writeHostLease, hostLeasePath } = await import(product("topology/host-leases.js"));
  const { loadFabricConfig } = await import(product("config.js"));
  const { createAgentSession, DefaultResourceLoader, ModelRuntime, SettingsManager, SessionManager } =
    await import(pathToFileURL(path.join(spec.piPackage, "dist/index.js")).href);

  // Deterministic identities and payloads, clock-relative unexpired leases. Nine
  // fixture directory hosts each own one root and ten actors. The REAL Pi supplies
  // the tenth host and hundredth participant; none of these peers is a live process.
  const uuid = n => `00000000-0000-4000-8000-${String(n).padStart(12, "0")}`;
  const keyFor = (kind, id) => `topology/${kind}/${hash(id)}`;
  const seededIds = [], hosts = [], fixtureLeases = [], now = Date.now(), expiresAt = now + (spec.seconds + spec.warmup + 3600) * 1000;
  const store = new MeshStore(meshRoot, 256 * 1024, 500, { stateBackend: spec.backend });
  try {
    assert.equal(store.stateBackend, spec.backend);
    for (let h = 1; h <= 9; h++) {
      const id = `session:${uuid(h)}`, identity = { id, name: `fixture-host-${h}`, kind: "main" };
      hosts.push(id);
      const incarnationToken = uuid(100 + h);
      const host = { format: 1, livenessLeaseFiles: 1, id, rootId: id, identity, incarnationToken, startedAt: now, updatedAt: now, expiresAt };
      await store.put({ identity, key: keyFor("hosts", id), value: host });
      writeHostLease(meshRoot, { id, rootId: id, identityId: id, incarnationToken, startedAt: now, updatedAt: now, expiresAt });
      const leaseFile = hostLeasePath(meshRoot, id);
      fixtureLeases.push({ file: leaseFile, lease: readJson(leaseFile) });
      for (let p = 0; p <= 10; p++) {
        const participantId = p === 0 ? id : hash(`idle-mesh-actor-${h}-${p}`).slice(0, 32);
        const value = { format: 1, livenessLeaseFiles: 1, id: participantId, kind: p === 0 ? "root" : "actor", rootId: id,
          ownerHostId: id, ownerIdentityId: id, name: `fixture-${h}-${p}`, status: "idle", runner: "pi", transport: "host",
          capabilities: ["steer", "followUp", "stop"], cwd, ...(p === 0 ? { sessionId: uuid(h), interactive: true } : { parentId: id }),
          startedAt: now, updatedAt: now, controlProtocol: "v1" };
        const key = keyFor("participants", participantId);
        const entry = { format: 1, key, value, version: 1, updatedAt: now, updatedBy: identity };
        // The documented native participants/<key-hash>.json wire envelope; no
        // product source import/rewrite, and no fake directory implementation.
        json(path.join(meshRoot, "participants", `${hash(participantId)}.json`), entry);
        seededIds.push(participantId);
      }
    }
    const identity = { id: "tool:idle-mesh-seed", name: "Private benchmark seed", kind: "main" };
    const ops = [{ kind: "put", key: "topology/liveness", value: { version: 1, hostLeases: "files", participants: "files" } }];
    for (let i = 0; i < 2990; i++) ops.push({ kind: "put", key: `idle-benchmark/records/${String(i).padStart(5, "0")}`,
      value: { format: 1, index: i, status: "idle", description: `Private deterministic record ${i}`, payload: "idle-mesh-fixture-".repeat(24) } });
    await store.writeBatch({ identity, ops });
    assert.equal(store.listAll("", { fresh: true }).length, 3000, "fixture must seed exactly 3000 keyed records");
  } finally { store.closeState(); }
  trace({ type: "seeded", participants: seededIds.length, hosts: hosts.length, stateRecords: 3000, expiresAt });

  const settingsManager = SettingsManager.create(cwd, profile);
  settingsManager.setProjectTrusted(true); // Only this newly-created empty cwd is trusted.
  const modelRuntime = await ModelRuntime.create({ authPath: path.join(profile, "auth.json"), modelsPath: null,
    refreshOnCreate: false, allowModelNetwork: false });
  modelRuntime.registerProvider("idle-benchmark", { api: "openai-completions", baseUrl: "http://invalid.invalid",
    apiKey: "public-inert-offline-metadata-not-a-credential", models: [{ id: "offline", name: "Idle metadata only", reasoning: false,
      input: ["text"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 100000, maxTokens: 1000 }] });
  const loader = new DefaultResourceLoader({ cwd, agentDir: profile, settingsManager,
    additionalExtensionPaths: [path.join(spec.fabricRoot, "dist/index.js")], noExtensions: true,
    noSkills: true, noPromptTemplates: true, noThemes: true, noContextFiles: true });
  await loader.reload();
  assert.deepEqual(loader.getExtensions().errors, [], "native extension loader errors");
  const loaded = loader.getExtensions().extensions.map(extension => extension.path);
  assert.equal(loaded.length, 1, "only the supplied built Fabric extension may load");
  assert.equal(fs.realpathSync(loaded[0]), fs.realpathSync(path.join(spec.fabricRoot, "dist/index.js")));
  assert.deepEqual(loader.getAgentsFiles().agentsFiles, []);
  const sessionManager = SessionManager.create(cwd, path.join(root, "sessions"));
  ({ session } = await createAgentSession({ cwd, agentDir: profile, modelRuntime,
    model: modelRuntime.getModel("idle-benchmark", "offline"), thinkingLevel: "off", resourceLoader: loader,
    settingsManager, sessionManager, noTools: "builtin" }));
  session.subscribe(event => {
    trace({ type: "pi-session-event", event });
    if (event.type === "agent_start") { agentStarts++; fail(new Error("idle session unexpectedly started inference")); }
  });
  await session.bindExtensions({ mode: "rpc", onError: error => fail(new Error(JSON.stringify(error))) });
  const tool = session.agent.state.tools.find(tool => tool.name === "fabric_exec");
  assert(tool, "actual Pi must register built fabric_exec");
  let invocation = 0;
  const observe = async () => {
    assert.equal(networkAttempts, 0); assert.equal(agentStarts, 0); assert(session.isIdle, "native Pi must be idle");
    const code = "const main = await agents.main(); const self = await agents.self(); const members = await agents.members({scope:'project'}); return {main: {id: main.id}, self, members: members.map(m => ({id: m.id, kind: m.kind, ownerHostId: m.ownerHostId, stale: m.stale}))};";
    const result = await tool.execute(`idle-mesh-proof-${++invocation}`, { code, resultFormat: "json" }, new AbortController().signal);
    trace({ type: "native-fabric-exec", code, result });
    assert(!result.isError, "native Fabric activation/directory proof failed; see child-events.jsonl");
    const native = toolJson(result);
    assert.equal(native.members.length, 100, "actual agents.members must see 100 live participants");
    const ids = new Set(native.members.map(member => member.id));
    for (const id of seededIds) assert(ids.has(id), `native directory rejected fixture participant ${id}`);
    assert(ids.has(native.self.id), "native directory must include the actual measured Main");
    assert.equal(native.self.kind, "root"); assert.equal(native.main.id, native.self.id);
    assert.equal(native.self.mainProcess?.pid, process.pid, "presence must belong to the measured Pi PID");
    assert.equal(new Set(native.members.map(member => member.ownerHostId)).size, 10, "actual directory must span 10 logical hosts");
    const effective = loadFabricConfig({ cwd, agentDir: profile, projectTrusted: true });
    assert(effective.mesh.enabled); assert.equal(effective.mesh.root, meshRoot); assert.equal(effective.mesh.stateBackend, spec.backend);
    assert.equal(effective.mesh.transport, "file"); assert.equal(effective.autoReload, false);
    const observer = new MeshStore(meshRoot, 256 * 1024, 500, { stateBackend: spec.backend });
    let stateRecords, hostRecords, ownHost;
    try {
      assert.equal(observer.root, meshRoot); assert.equal(observer.stateBackend, spec.backend);
      const entries = observer.listAll("", { fresh: true }); stateRecords = entries.length;
      hostRecords = entries.filter(entry => entry.key.startsWith("topology/hosts/"));
      assert.equal(hostRecords.length, 10, "private native state must have 10 hosts");
      ownHost = hostRecords.find(entry => entry.value.id === native.self.ownerHostId);
      assert(ownHost && ownHost.value.identity.id === native.self.ownerIdentityId, "actual runtime did not publish in private mesh");
      assert(stateRecords >= 3000 && stateRecords <= 3020, "fixture record count drifted outside ~3000");
    } finally { observer.closeState(); }
    assert.equal(networkAttempts, 0); assert.equal(agentStarts, 0); assert(session.isIdle);
    const proof = { pid: process.pid, sessionId: sessionManager.getSessionId(), sessionFile: sessionManager.getSessionFile(),
      native, stateRecords, hostRecords, ownHost, effectiveMesh: effective.mesh, effectiveAutoReload: effective.autoReload,
      profile, cwd, meshRoot, loadedExtensions: loaded, networkAttempts, agentStarts, node: process.version,
      execPath: process.execPath, backend: spec.backend, seededLeaseExpiresAt: expiresAt,
      fixtureLeases: fixtureLeases.map(({ file }) => ({ file, lease: readJson(file) })) };
    sessionManager.appendCustomEntry("idle-mesh-benchmark-proof", proof);
    // Pi defers file creation until a conversation exists. Preserve the complete native
    // tree too, even for an entirely prompt-free session with no flushed JSONL file.
    json(path.join(root, `native-session-${invocation}.json`), { header: sessionManager.getHeader(), entries: sessionManager.getEntries() });
    return proof;
  };
  const ready = await observe();
  send({ type: "ready", proof: ready });
  process.on("message", message => {
    if (message.type === "stop") void stop();
    else if (message.type === "observe") void observe().then(proof => send({ type: "observation", id: message.id, proof })).catch(fail);
  });
}

// Intentional workload source: one parent-owned 5 s peer-heartbeat schedule,
// NOT a keepalive for a fake Pi. All nine leases remain live even if a pass is late.
// Atomic rename matches native writeHostLease's on-disk publication mechanism.
function fixtureDriver(leases, meshRoot, root, onError) {
  assert.equal(leases.length, 9);
  for (const { file, lease } of leases) {
    assert(inside(path.join(meshRoot, "host-leases"), file), "fixture lease escaped private mesh");
    assert(fs.lstatSync(file).isFile(), "fixture lease must be a regular private file");
    assert.equal(lease.format, 1); assert(lease.expiresAt > Date.now() + 120000, "fixture lease must remain live");
  }
  let passes = 0, writes = 0, lastAt = Date.now(), maxGapMs = 0, timer;
  const snapshot = () => ({ parentPid: process.pid, intervalMs: 5000, fixtureHosts: leases.length, passes, writes, lastAt, maxGapMs });
  const stop = () => { clearInterval(timer); timer = undefined; };
  timer = setInterval(() => {
    try {
      const now = Date.now(); maxGapMs = Math.max(maxGapMs, now - lastAt); lastAt = now;
      for (const { file, lease } of leases) {
        const next = `${file}.fixture-${process.pid}.next`;
        json(next, { ...lease, updatedAt: now, expiresAt: now + lease.expiresAt - lease.updatedAt }); fs.renameSync(next, file); writes++;
      }
      passes++;
      fs.appendFileSync(path.join(root, "fixture-driver.jsonl"), JSON.stringify({ at: new Date().toISOString(), ...snapshot() }) + "\n");
    } catch (error) { stop(); onError(error); }
  }, 5000);
  return { snapshot, stop };
}

function compare(o) {
  const [baseline, candidate] = o.compare.map(readJson);
  assert(baseline.status === "measured" && candidate.status === "measured", "both inputs must be successful measurements");
  assert.equal(baseline.label, "baseline"); assert.equal(candidate.label, "candidate");
  assert.equal(baseline.artifact.sourceHead, BASELINE);
  for (const key of ["backend", "scenario", "seconds", "warmup"]) assert.equal(baseline.options[key], candidate.options[key], `incomparable option: ${key}`);
  for (const key of ["hostname", "arch", "node", "clkTck"]) assert.equal(baseline.host[key], candidate.host[key], `incomparable host: ${key}`);
  for (const key of ["packageRoot", "sha256", "packageSha256"]) assert.equal(baseline.pi[key], candidate.pi[key], `Pi runtime changed: ${key}`);
  assert.equal(baseline.harnessSha256, candidate.harnessSha256, "benchmark implementation changed");
  assert(Date.parse(baseline.finishedAt) <= Date.parse(candidate.startedAt), "measurements must run sequentially, baseline first");
  const deltas = {};
  for (const key of ["cpuCores", "syscrPerSecond", "rcharBytesPerSecond", "syscwPerSecond", "wcharBytesPerSecond",
    "diskReadBytesPerSecond", "diskWriteBytesPerSecond"]) {
    const a = baseline.metrics[key], b = candidate.metrics[key];
    deltas[key] = { baseline: a, candidate: b, delta: b - a, candidateOverBaseline: a === 0 ? null : b / a,
      reductionPercent: a === 0 ? null : (1 - b / a) * 100 };
  }
  const result = { status: "compared", baseline: o.compare[0], candidate: o.compare[1],
    scenario: baseline.options.scenario, fixtureRenewalPasses: { baseline: baseline.fixtureRenewalPassesInWindow ?? 0, candidate: candidate.fixtureRenewalPassesInWindow ?? 0 }, deltas,
    note: "One real idle Pi per window; fixtures are not a live fleet. No speedup/budget assumed; repeat sequential pairs for variance." };
  fs.writeFileSync(path.resolve(o.out), JSON.stringify(result, null, 2) + "\n", { mode: 0o600, flag: "wx" });
  console.log(JSON.stringify(result, null, 2));
}

async function measure(o) {
  assert.equal(process.platform, "linux", "Linux /proc is required");
  assert(!process.versions.bun && Number(process.versions.node.split(".")[0]) >= 24, "use native Node >=24, not bun");
  process.umask(0o077);
  const out = path.resolve(o.out);
  assert(!fs.existsSync(out), `refusing to overwrite evidence ${out}`);
  const built = artifact(o.fabricRoot);
  if (o.label === "baseline") assert.equal(built.sourceHead, BASELINE, "baseline artifact must carry the parent's exact frozen source-head");
  const piPackage = fs.realpathSync(o.piPackage), sdkEntry = path.join(piPackage, "dist/index.js");
  const sdkPackage = readJson(path.join(piPackage, "package.json"));
  assert.equal(sdkPackage.name, "@earendil-works/pi-coding-agent");
  const pi = { packageRoot: piPackage, version: sdkPackage.version,
    sha256: hash(fs.readFileSync(sdkEntry)), packageSha256: hash(fs.readFileSync(path.join(piPackage, "package.json"))) };
  const findings = { path: FINDINGS, sha256: hash(fs.readFileSync(FINDINGS)) };
  const clkTck = Number(execFileSync("getconf", ["CLK_TCK"], { encoding: "utf8" }).trim());
  assert(Number.isSafeInteger(clkTck) && clkTck > 0);
  const host = { hostname: os.hostname(), arch: process.arch, node: process.version, executable: process.execPath,
    clkTck, logicalCpus: os.cpus().length, availableParallelism: os.availableParallelism() };
  if (o.preflight) {
    console.log(JSON.stringify({ status: "preflight-only", artifact: { ...built, files: undefined, fileCount: Object.keys(built.files).length }, pi, findings, host }, null, 2)); return;
  }
  fs.mkdirSync(path.dirname(out), { recursive: true, mode: 0o700 });
  const root = fs.mkdtempSync(path.join(os.tmpdir(), `idle-mesh-${o.label}-`)); fs.chmodSync(root, 0o700);
  for (const dir of ["home", "tmp", "profile", "cwd", "mesh/participants", "runs", "exports", "sessions", "runtime"])
    fs.mkdirSync(path.join(root, dir), { recursive: true, mode: 0o700 });
  const profile = path.join(root, "profile"), cwd = path.join(root, "cwd"), meshRoot = path.join(root, "mesh");
  assert(!inside(built.root, root) && !inside(piPackage, root), "scratch must not be inside an input artifact");
  const fabricConfig = { fullCodeMode: false, autoReload: false,
    executor: { kernel: "typescript", timeoutMs: 30000 },
    mesh: { enabled: true, announce: true, root: meshRoot, stateBackend: o.backend, transport: "file" },
    mcp: { enabled: false }, memory: { enabled: false }, jev: { enabled: false }, ui: { enabled: false },
    entropy: { compile: false }, speculation: { enabled: false }, compaction: { engine: "pi" },
    agents: { retainRuns: true, sessionExport: true, sessionExportDir: path.join(root, "exports") } };
  const piSettings = { packages: [], extensions: [], skills: [], prompts: [], themes: [], defaultProjectTrust: "always",
    compaction: { enabled: false }, retry: { enabled: false }, cacheWarming: "off", enableInstallTelemetry: false, enableAnalytics: false };
  json(path.join(profile, "fabric.json"), fabricConfig); json(path.join(profile, "settings.json"), piSettings);
  json(path.join(profile, "auth.json"), {});
  const specFile = path.join(root, "worker-input.json");
  json(specFile, { root, fabricRoot: built.root, piPackage, backend: o.backend, seconds: o.seconds, warmup: o.warmup });
  const env = { PATH: process.env.PATH ?? "/usr/bin:/bin", HOME: path.join(root, "home"), TMPDIR: path.join(root, "tmp"),
    XDG_CONFIG_HOME: path.join(root, "home/.config"), XDG_DATA_HOME: path.join(root, "home/.local/share"),
    XDG_CACHE_HOME: path.join(root, "home/.cache"), XDG_RUNTIME_DIR: path.join(root, "runtime"),
    PI_CODING_AGENT_DIR: profile, PI_OFFLINE: "1", PI_REMOTE_CATALOG: "0", PI_SKIP_VERSION_CHECK: "1", PI_TELEMETRY: "0",
    PI_FABRIC_MESH_ROOT: meshRoot, PI_FABRIC_PROJECT_ROOT: cwd, PI_FABRIC_RUN_ROOT: path.join(root, "runs"),
    PI_FABRIC_RUNS_ROOT: path.join(root, "runs"), PI_FABRIC_AGENT_DIR: path.join(root, "exports"), PI_FABRIC_TMPDIR: path.join(root, "tmp") };
  json(path.join(root, "child-environment.json"), env); // Whitelist only; NEVER serialize caller env.
  const report = { status: "starting", label: o.label, options: { seconds: o.seconds, warmup: o.warmup, backend: o.backend, scenario: o.scenario },
    startedAt: new Date().toISOString(), root, artifact: built, pi, findings, host,
    harnessSha256: hash(fs.readFileSync(SCRIPT)), fabricConfig, piSettings, loadBefore: os.loadavg(),
    measurement: "One actual headless Pi SDK AgentSession; native Fabric entry/hooks; /proc child PID only; no TUI rendering. Parent fixture renewal workload separately reported; no peer inference or keyed-state churn" };
  const persist = () => { json(path.join(root, "result.json"), report); json(out, report); };
  persist();
  const stdout = fs.openSync(path.join(root, "child.stdout.log"), "wx"), stderr = fs.openSync(path.join(root, "child.stderr.log"), "wx");
  let child;
  try { child = spawn(process.execPath, [SCRIPT, "--worker", specFile], { cwd, env, stdio: ["ignore", stdout, stderr, "ipc"] }); }
  finally { fs.closeSync(stdout); fs.closeSync(stderr); }
  let failure, ready, driver, request = 0;
  const waiting = new Map();
  let resolveExit;
  const exited = new Promise(resolve => { resolveExit = resolve; });
  const rejectAll = error => {
    failure ??= error;
    for (const value of waiting.values()) value.reject(error);
    waiting.clear();
  };
  child.on("error", error => { rejectAll(error); resolveExit(); });
  child.on("exit", (code, signal) => { report.childExit = { code, signal }; rejectAll(new Error(`Pi exited (${code ?? signal}); see ${root}`)); resolveExit(); });
  child.on("message", message => {
    if (message.type === "fatal") rejectAll(new Error(message.error));
    else if (message.type === "ready") { ready = message.proof; waiting.get("ready")?.resolve(ready); waiting.delete("ready"); }
    else if (message.type === "observation") { waiting.get(message.id)?.resolve(message.proof); waiting.delete(message.id); }
    else if (message.type === "stopped") report.orderlyShutdown = true;
  });
  const wait = (id, ms) => new Promise((resolve, reject) => {
    if (failure) { reject(failure); return; }
    const timer = setTimeout(() => { waiting.delete(id); reject(new Error(`Pi ${id} timeout; see ${root}`)); }, ms);
    waiting.set(id, { resolve: value => { clearTimeout(timer); resolve(value); }, reject: error => { clearTimeout(timer); reject(error); } });
  });
  const observe = async () => { const id = String(++request), pending = wait(id, 45000); child.send({ type: "observe", id }); return pending; };
  const idleFor = ms => new Promise((resolve, reject) => {
    if (failure) { reject(failure); return; }
    const id = "idle-window";
    const timer = setTimeout(() => { waiting.delete(id); resolve(); }, ms);
    waiting.set(id, { resolve: () => { clearTimeout(timer); resolve(); }, reject: error => { clearTimeout(timer); reject(error); } });
  });
  const joinFor = async ms => {
    let timer;
    try { await Promise.race([exited, new Promise(resolve => { timer = setTimeout(resolve, ms); })]); }
    finally { clearTimeout(timer); }
  };
  const interrupted = signal => { rejectAll(new Error(`benchmark interrupted by ${signal}`)); child.kill("SIGTERM"); };
  const sigint = () => interrupted("SIGINT"), sigterm = () => interrupted("SIGTERM");
  process.on("SIGINT", sigint); process.on("SIGTERM", sigterm);
  console.error(`idle-mesh ${o.label}: PID ${child.pid}; private evidence ${root}; warmup ${o.warmup}s + window ${o.seconds}s`);
  try {
    report.pid = child.pid; report.ready = ready ?? await wait("ready", 120000);
    assert.equal(report.ready.pid, child.pid);
    if (o.startupOnly) report.status = "startup-only";
    else {
    if (o.scenario === "renewal") driver = fixtureDriver(report.ready.fixtureLeases, meshRoot, root, rejectAll);
    report.status = "warmup"; persist();
    await idleFor(o.warmup * 1000);
    report.beforeProof = await observe();
    const children = fs.readFileSync(`/proc/${child.pid}/task/${child.pid}/children`, "utf8").trim();
    assert.equal(children, "", "idle measured Pi must have no child processes at window start");
    report.status = "measuring"; persist();
    report.driverBefore = driver?.snapshot();
    report.before = sample(child.pid);
    await idleFor(o.seconds * 1000);
    report.after = sample(child.pid);
    report.driverAfter = driver?.snapshot(); driver?.stop();
    if (driver) {
      report.fixtureRenewalPassesInWindow = report.driverAfter.passes - report.driverBefore.passes;
      assert(report.fixtureRenewalPassesInWindow >= Math.floor(o.seconds / 5) - 2, "fixture renewal driver did not sustain the requested workload");
    }
    assert.equal(fs.readFileSync(`/proc/${child.pid}/task/${child.pid}/children`, "utf8").trim(), "", "unexpected child process at idle window end");
    report.afterProof = await observe();
    report.metrics = metrics(report.before, report.after, clkTck);
    assert(report.metrics.elapsedSeconds >= o.seconds - 0.01, "measurement window was truncated");
    assert.equal(artifact(built.root).sha256, built.sha256, "built artifact changed during measurement");
    report.status = "measured";
    }
  } catch (error) { report.status = "failed"; report.error = String(error.stack ?? error); process.exitCode = 1; }
  finally {
    driver?.stop();
    if (child.exitCode === null && child.signalCode === null) {
      if (child.connected) child.send({ type: "stop" });
      await joinFor(10000);
      if (child.exitCode === null && child.signalCode === null) { child.kill("SIGTERM"); await joinFor(3000); }
      if (child.exitCode === null && child.signalCode === null) { child.kill("SIGKILL"); await exited; }
    }
    process.off("SIGINT", sigint); process.off("SIGTERM", sigterm);
    if (["measured", "startup-only"].includes(report.status) && (!report.orderlyShutdown || report.childExit?.code !== 0)) {
      report.status = "failed"; report.error = "Pi did not shut down cleanly; measurement retained but not accepted"; process.exitCode = 1;
    }
    report.finishedAt = new Date().toISOString(); report.loadAfter = os.loadavg(); persist();
    // Deliberately NO rm/rmdir: all traces/profile/mesh/logs remain under root.
  }
  console.log(JSON.stringify({ status: report.status, label: report.label, pid: report.pid, metrics: report.metrics, error: report.error, out, evidence: root }, null, 2));
}

try {
  const argv = process.argv.slice(2);
  if (argv[0] === "--worker") await worker(argv[1]);
  else if (argv.includes("--help") || argv.length === 0) console.log(help);
  else if (argv.length === 1 && argv[0] === "--self-test") selfTest();
  else { const o = options(argv); if (o.compare) compare(o); else await measure(o); }
} catch (error) {
  if (process.send && process.connected) process.send({ type: "fatal", error: String(error.stack ?? error) });
  console.error(String(error.stack ?? error)); process.exitCode = 1;
  if (process.send) process.disconnect();
}
