#!/usr/bin/env node
// Idle participant-refresh profile (smarty-dev#7978 S1).
//
// Starts ONE real idle Pi Main (the Pi CLI from @earendil-works/pi-coding-agent in RPC mode, with
// the built dist/index.js extension and the offline faux provider from tests/fixtures) against a
// SCRATCH mesh root seeded with --hosts peer hosts, each with --per-host participants (a root and
// agents) in the shared SQLite state and one host lease file. It profiles the Main with --cpu-prof
// for --seconds, then reports the main thread's busy share and the inclusive share of
// src/topology/participant-directory.ts (and its hot methods), mapped through dist source maps.
//
//   bun run build
//   node bench/participant-refresh-idle.mjs --out <dir> [--seconds 120] [--hosts 20] [--per-host 3] [--fleet]
//
// --fleet adds the churn a real fleet mesh has: every peer lease file is atomically replaced every
// 5 s (as each peer's heartbeat does) and one unrelated shared-state commit lands every second.
// Nothing here touches the caller's HOME, Pi settings or mesh.
import { spawn } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { createRequire } from "node:module";

const repo = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const require = createRequire(import.meta.url);
const args = process.argv.slice(2);
const flag = (name, fallback) => {
  const at = args.indexOf(name);
  return at >= 0 ? args[at + 1] : fallback;
};
const out = path.resolve(flag("--out", path.join(process.env.TMPDIR ?? "/tmp", "participant-refresh-idle")));
const seconds = Number(flag("--seconds", "120"));
const hosts = Number(flag("--hosts", "20"));
const perHost = Number(flag("--per-host", "3"));
const fleet = args.includes("--fleet");
const label = flag("--label", fleet ? "fleet" : "static");
const WARMUP_S = 10;
// SQLite state refuses a root with a group/other-writable ancestor, so the scratch mesh lives under
// --scratch (default /tmp: root-owned and sticky); only the result and the profile are kept in --out.
const scratchBase = path.resolve(flag("--scratch", "/tmp"));

// SQLite state refuses a group/other-writable ancestor: keep the scratch tree private.
process.umask(0o077);
fs.mkdirSync(out, { recursive: true, mode: 0o700 });
const root = fs.realpathSync(fs.mkdtempSync(path.join(scratchBase, `pf-7978-idle-${label}-`)));
const s = {
  home: path.join(root, "home"), cwd: path.join(root, "project"), meshRoot: path.join(root, "mesh"),
  tmp: path.join(root, "tmp"), report: path.join(root, "report"), prof: path.join(root, "prof"),
};
s.agentDir = path.join(s.home, ".pi", "agent");
for (const dir of [s.agentDir, s.cwd, s.tmp, s.report, s.prof]) fs.mkdirSync(dir, { recursive: true });
fs.writeFileSync(path.join(s.agentDir, "settings.json"), JSON.stringify({ compaction: { enabled: false }, retry: { enabled: false } }));
fs.writeFileSync(path.join(s.agentDir, "fabric.json"), JSON.stringify({
  executor: { kernel: "typescript" }, fullCodeMode: false, mesh: { enabled: true, announce: true },
  mcp: { enabled: false }, jev: { enabled: false }, memory: { enabled: false },
  compaction: { engine: "pi" }, entropy: { compile: false }, speculation: { enabled: false },
}));

// ---------------------------------------------------------------- seed the scratch mesh
process.env.PI_FABRIC_MESH_STATE_BACKEND = "sqlite";
// A fresh scratch root: create state.db in this seeding process only (the fixture seam the suite uses;
// the Main opens the created, imported database as any default open does).
globalThis[Symbol.for("pi-fabric.mesh.sqlite-initialize.test-fixtures")] = "create";
const { MeshStore } = await import(pathToFileURL(path.join(repo, "dist/mesh.js")).href);
const store = new MeshStore(s.meshRoot, 64 * 1024, 1_000, { stateBackend: "sqlite" });
const sha = (text) => createHash("sha256").update(text).digest("hex");
const keyFor = (prefix, id) => prefix + sha(id);
const leaseDir = path.join(s.meshRoot, "host-leases");
fs.mkdirSync(leaseDir, { recursive: true });
const DAY = 24 * 60 * 60 * 1_000;
const peers = [];
const writeLease = (peer, now) => {
  const file = path.join(leaseDir, sha(peer.id).slice(0, 32) + ".json");
  const staging = `${file}.${process.pid}.tmp`;
  fs.writeFileSync(staging, JSON.stringify({ format: 1, id: peer.id, rootId: peer.id, identityId: peer.id,
    updatedAt: now, expiresAt: now + DAY, startedAt: peer.startedAt, incarnationToken: peer.token }));
  fs.renameSync(staging, file);
};
const seedNow = Date.now();
for (let index = 0; index < hosts; index += 1) {
  const id = `session:peer-${index}`;
  const identity = { id, name: "main", kind: "main", sessionId: `peer-${index}` };
  const peer = { id, identity, startedAt: seedNow - 60_000, token: randomUUID() };
  peers.push(peer);
  const ops = [{ kind: "put", key: keyFor("topology/hosts/", id), value: {
    format: 1, livenessLeaseFiles: 1, id, rootId: id, identity, startedAt: peer.startedAt,
    incarnationToken: peer.token, updatedAt: seedNow, expiresAt: seedNow + DAY,
  } }];
  for (let child = 0; child < perHost; child += 1) {
    const isRoot = child === 0;
    const participantId = isRoot ? id : `agent:peer-${index}-${child}`;
    ops.push({ kind: "put", key: keyFor("topology/participants/", participantId), value: {
      format: 1, id: participantId, kind: isRoot ? "root" : "agent", rootId: id, ownerHostId: id, ownerIdentityId: id,
      ...(isRoot ? { sessionId: `peer-${index}`, pendingMessages: false } : { parentId: id }),
      name: isRoot ? `peer ${index}` : `worker ${index}.${child}`, label: isRoot ? `project-${index + 1}` : undefined,
      status: isRoot ? "idle" : "running", runner: "pi", transport: isRoot ? "host" : "process",
      capabilities: isRoot ? ["steer", "followUp", "fabric"] : ["steer", "followUp", "stop"],
      cwd: `/srv/project-${index}`, project: `project-${index}`, model: "anthropic/claude-sonnet", thinking: "medium",
      startedAt: peer.startedAt, updatedAt: seedNow, controlProtocol: "v1", livenessLeaseFiles: 1,
      ownerIncarnation: randomUUID(),
    } });
  }
  await store.writeBatch({ identity, ops });
  writeLease(peer, seedNow);
}
const seededEntries = store.listAll("", { fresh: true }).length;

// ---------------------------------------------------------------- start the idle Main
const cli = path.join(repo, "node_modules/@earendil-works/pi-coding-agent/dist/cli.js");
const entry = path.join(repo, "dist/index.js");
const fixture = path.join(repo, "tests/fixtures/exit-cleanup-8467.ts");
const child = spawn(process.execPath, ["--cpu-prof", "--cpu-prof-dir", s.prof, cli, "--mode", "rpc", "--no-extensions", "--no-skills",
  "--no-prompt-templates", "--no-themes", "--no-context-files", "--approve", "--no-builtin-tools", "--offline", "--thinking", "off",
  "--provider", "exit-8467", "--model", "faux-1", "--session-dir", path.join(root, "sessions"), "-e", entry, "-e", fixture], {
  cwd: s.cwd, stdio: ["pipe", "pipe", "pipe"],
  env: {
    PATH: process.env.PATH ?? "", LANG: "C.UTF-8", HOME: s.home, TMPDIR: s.tmp, PI_OFFLINE: "1",
    PI_CODING_AGENT_DIR: s.agentDir, PI_FABRIC_AGENT_DIR: path.join(root, "exports"), PI_FABRIC_MESH_ROOT: s.meshRoot,
    PI_FABRIC_MESH_STATE_BACKEND: "sqlite", PI_FABRIC_PROJECT_ROOT: s.cwd, PI_FABRIC_RUN_ROOT: path.join(root, "runs"),
    FABRIC_8467_REPORT_DIR: s.report, PI_FABRIC_MESH_CHECKPOINT_LOG: "off",
  },
});
let stderr = "";
child.stderr.on("data", (chunk) => { stderr = (stderr + chunk).slice(-20_000); });
child.stdout.on("data", () => undefined);
const exited = new Promise((resolve) => child.on("exit", (code, signal) => resolve({ code, signal })));
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const readyFile = path.join(s.report, "ready.json");
const readyBy = Date.now() + 90_000;
while (!fs.existsSync(readyFile)) {
  if (Date.now() > readyBy || child.exitCode !== null) {
    child.kill("SIGKILL");
    throw new Error(`the Main did not start: ${stderr}`);
  }
  await sleep(100);
}
const ready = JSON.parse(fs.readFileSync(readyFile, "utf8"));

// ---------------------------------------------------------------- idle (optionally with fleet churn)
const churn = [];
if (fleet) {
  let tick = 0;
  churn.push(setInterval(() => { const now = Date.now(); for (const peer of peers) writeLease(peer, now); }, 5_000));
  churn.push(setInterval(() => {
    void store.put({ key: "bench/churn", value: { tick: tick += 1 }, identity: peers[0].identity }).catch(() => undefined);
  }, 1_000));
}
await sleep(seconds * 1_000);
for (const timer of churn) clearInterval(timer);
const stoppedAt = performance.now();
child.stdin.end();
const exit = await Promise.race([exited, sleep(60_000).then(() => { child.kill("SIGKILL"); return { code: null, signal: "SIGKILL" }; })]);
const shutdownMs = performance.now() - stoppedAt;
const participants = store.listAll("topology/participants/", { fresh: true }).length;
store.closeState?.();

// ---------------------------------------------------------------- analyse the main-thread profile
const files = fs.readdirSync(s.prof).filter((name) => name.endsWith(".cpuprofile"));
// CPU.<date>.<time>.<pid>.<thread>.<seq>.cpuprofile: the main thread is thread 0.
const main = files.find((name) => name.split(".")[4] === "0") ?? files[0];
if (!main) throw new Error(`no profile written (exit ${JSON.stringify(exit)}): ${stderr}`);
const profile = JSON.parse(fs.readFileSync(path.join(s.prof, main), "utf8"));
const { SourceMapConsumer } = require(require.resolve("source-map-js", { paths: [repo] }));
const maps = new Map();
const mapOf = (url) => {
  if (!url.startsWith("file://")) return undefined;
  if (maps.has(url)) return maps.get(url);
  let consumer;
  try { consumer = new SourceMapConsumer(JSON.parse(fs.readFileSync(fileURLToPath(url) + ".map", "utf8"))); } catch { consumer = undefined; }
  maps.set(url, consumer);
  return consumer;
};
const byId = new Map(profile.nodes.map((node) => [node.id, node]));
const parent = new Map();
for (const node of profile.nodes) for (const id of node.children ?? []) parent.set(id, node.id);
const sourceOf = new Map();
const source = (node) => {
  if (sourceOf.has(node.id)) return sourceOf.get(node.id);
  const frame = node.callFrame;
  let file = frame.url;
  const consumer = mapOf(frame.url);
  if (consumer) {
    const original = consumer.originalPositionFor({ line: frame.lineNumber + 1, column: Math.max(0, frame.columnNumber) });
    if (original.source) file = original.source;
  }
  sourceOf.set(node.id, file);
  return file;
};
const probes = {
  "participant-directory.ts (any frame)": (node) => source(node).endsWith("src/topology/participant-directory.ts"),
  "ParticipantDirectory #refresh": (node) => node.callFrame.functionName === "#refresh" && source(node).endsWith("src/topology/participant-directory.ts"),
  "#legacyRenewalsRequired": (node) => node.callFrame.functionName === "#legacyRenewalsRequired",
  "#reportRootCollisions": (node) => node.callFrame.functionName === "#reportRootCollisions",
  "decideRenewal": (node) => node.callFrame.functionName === "decideRenewal",
  "#liveHosts": (node) => node.callFrame.functionName === "#liveHosts",
  "readHostLeases": (node) => node.callFrame.functionName === "readHostLeases",
  "readParticipantFiles": (node) => node.callFrame.functionName === "readParticipantFiles",
  "jsonClone": (node) => node.callFrame.functionName === "jsonClone",
  "toEntry": (node) => node.callFrame.functionName === "toEntry",
  "(garbage collector)": (node) => node.callFrame.functionName === "(garbage collector)",
};
const samples = profile.samples;
const deltas = profile.timeDeltas;
const times = [];
let at = profile.startTime;
for (let index = 0; index < samples.length; index += 1) { at += deltas[index] ?? 0; times.push(at); }
// Window: from WARMUP_S after start to the end of the idle period (the profile ends at exit).
const windowEnd = profile.endTime - shutdownMs * 1_000;
const windowStart = windowEnd - (seconds - WARMUP_S) * 1_000_000;
const totals = Object.fromEntries(Object.keys(probes).map((name) => [name, 0]));
let busy = 0;
let total = 0;
const stackHit = new Map();
for (let index = 0; index < samples.length; index += 1) {
  if (times[index] < windowStart || times[index] > windowEnd) continue;
  const weight = (index + 1 < samples.length ? deltas[index + 1] : 0) / 1_000;  // ms
  total += weight;
  const leaf = byId.get(samples[index]);
  if (leaf.callFrame.functionName === "(idle)") continue;
  busy += weight;
  let hits = stackHit.get(leaf.id);
  if (!hits) {
    hits = new Set();
    for (let id = leaf.id; id !== undefined; id = parent.get(id)) {
      const node = byId.get(id);
      for (const [name, probe] of Object.entries(probes)) if (probe(node)) hits.add(name);
    }
    stackHit.set(leaf.id, hits);
  }
  for (const name of hits) totals[name] += weight;
}
const pct = (value, of) => of > 0 ? Math.round((value / of) * 10_000) / 100 : 0;
const result = {
  label, fleet, seconds, windowSeconds: Math.round(total) / 1_000, hosts, perHost, seededEntries, participantsAfter: participants,
  mainPid: ready.pid, childPid: child.pid, exit, shutdownMs: Math.round(shutdownMs),
  mainThreadBusyPct: pct(busy, total), busyMs: Math.round(busy),
  inclusiveShareOfBusyPct: Object.fromEntries(Object.entries(totals).map(([name, value]) => [name, pct(value, busy)])),
  inclusiveMs: Object.fromEntries(Object.entries(totals).map(([name, value]) => [name, Math.round(value)])),
  profile: path.join(out, `profile-${label}.cpuprofile`),
};
fs.copyFileSync(path.join(s.prof, main), result.profile);
// The scratch tree is this script's own: remove it (the result and profile stay in --out).
fs.rmSync(root, { recursive: true, force: true });
fs.writeFileSync(path.join(out, `result-${label}.json`), JSON.stringify(result, null, 2) + "\n");
console.log(JSON.stringify(result, null, 2));
