#!/usr/bin/env node
// Idle wake gate (smarty-dev#7978, R-no-polling). Linux only; needs a built dist/.
//   node scripts/benchmark-idle-cpu.mjs --mode main|resident [--seconds 300] [--warmup-seconds 30]
//   node scripts/benchmark-idle-cpu.mjs --mode main --extension PATH [--extension PATH ...]
//   node scripts/benchmark-idle-cpu.mjs --mode script --script FILE   (the gate's own self-test)
// --extension adds a Pi extension to the Main, after Fabric, as `pi -e PATH` loads it (another repo's
// extension, such as smarty-inbox, runs the same gate this way).
// main: an activated native Pi RPC Main with the compiled Fabric extension (keyless offline provider;
// no network, no inference after the one activation turn). resident: an idle compiled ResidentHost.
// Each runs under scripts/idle-wake-preload.cjs, which counts Timeout and Immediate callback runs and
// fs.watchFile polls (StatWatcher, 1000/interval per second while watched) per
// creation callsite. After the warm-up it measures --seconds and FAILS (exit 1) when either rule breaks:
//   (1) non-allowlisted timer (Timeout, Immediate and StatWatcher) wakes > --budget per second (default 1.0) in total;
//   (2) one non-allowlisted callsite fires more than once a minute (more than 5 in 300 s).
// Allowlist: scripts/idle-wake-allowlist.json, [{callsite: "file function", reason, issue}].
// The Main's voluntary context switches are reported, not gated.
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import readline from "node:readline";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";

const repo = path.resolve(fileURLToPath(new URL("..", import.meta.url)));
const argv = process.argv.slice(2);
const option = (key, fallback) => { const i = argv.indexOf(key); return i < 0 ? fallback : argv[i + 1]; };
const mode = option("--mode", "main");
const seconds = Number(option("--seconds", "300"));
const warmup = Number(option("--warmup-seconds", "30"));
const budget = Number(option("--budget", "1.0"));
const extensions = argv.flatMap((arg, i) => arg === "--extension" ? ["-e", path.resolve(argv[i + 1])] : []);
const allowlist = JSON.parse(fs.readFileSync(option("--allowlist", path.join(repo, "scripts/idle-wake-allowlist.json")), "utf8"));
assert(process.platform === "linux", "the idle wake gate needs Linux /proc");
assert(["main", "resident", "script"].includes(mode), "--mode main|resident|script");
for (const entry of allowlist) assert(entry.callsite && entry.reason && entry.issue, "each allowlist entry needs callsite, reason and issue");
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const root = fs.mkdtempSync(path.join(os.tmpdir(), `idle-wakes-${mode}-`));
const wakeOut = path.join(root, "wakes.json");
const preload = path.join(repo, "scripts/idle-wake-preload.cjs");
const env = { PATH: process.env.PATH, HOME: path.join(root, "home"), TMPDIR: root, PI_CODING_AGENT_DIR: path.join(root, "profile"),
  PI_OFFLINE: "1", PI_FABRIC_MESH_ROOT: path.join(root, "mesh"), PI_FABRIC_PROJECT_ROOT: root,
  PI_FABRIC_RUN_ROOT: path.join(root, "runs"), PI_FABRIC_AGENT_DIR: path.join(root, "exports"),
  IDLE_WAKE_OUT: wakeOut, IDLE_WAKE_ROOT: repo };
const write = (file, text) => { fs.mkdirSync(path.dirname(file), { recursive: true }); fs.writeFileSync(file, text); };

function start(args) {
  const log = fs.openSync(path.join(root, "child.log"), "w");
  const child = spawn(process.execPath, ["--enable-source-maps", "--require", preload, ...args],
    { cwd: root, env, stdio: ["pipe", "pipe", log] });
  fs.closeSync(log);
  child.done = new Promise(resolve => child.once("exit", resolve));
  return child;
}

// Activated Main: a keyless provider answers the one prompt with a fabric_exec call that activates Fabric.
async function startMain() {
  const provider = path.join(root, "idle-provider.mjs");
  write(provider, `import {createAssistantMessageEventStream} from '@earendil-works/pi-ai';
export default function(pi) {
 const model={provider:'idle-probe',id:'offline',name:'Offline idle probe',api:'idle-probe',baseUrl:'http://invalid.local',reasoning:true,input:['text'],cost:{input:0,output:0,cacheRead:0,cacheWrite:0},contextWindow:200000,maxTokens:4096};
 const stream=(selected,context)=>{
  const tool=context.messages.at(-1)?.role!=='toolResult';
  const message={role:'assistant',provider:selected.provider,model:selected.id,api:selected.api,timestamp:Date.now(),
   content:tool?[{type:'toolCall',id:'idle-activate',name:'fabric_exec',arguments:{code:'return await agents.main();',resultFormat:'json'}}]:[{type:'text',text:'Idle probe activated'}],stopReason:tool?'toolUse':'stop',
   usage:{input:0,output:0,cacheRead:0,cacheWrite:0,totalTokens:0,cost:{input:0,output:0,cacheRead:0,cacheWrite:0,total:0}}};
  const events=createAssistantMessageEventStream();events.push({type:'start',partial:message});
  if(tool)events.push({type:'toolcall_end',contentIndex:0,toolCall:message.content[0],partial:message});
  events.push({type:'done',reason:message.stopReason,message});events.end();return events;
 };
 pi.registerProvider({id:model.provider,name:model.name,auth:{apiKey:{name:'Keyless probe',check:async()=>({type:'api_key',source:'keyless test'}),resolve:async()=>({auth:{}})}},getModels:()=>[model],stream,streamSimple:stream});
}
`);
  write(path.join(root, "profile/settings.json"), JSON.stringify({ compaction: { enabled: false }, retry: { enabled: false } }));
  write(path.join(root, "profile/fabric.json"), JSON.stringify({ fullCodeMode: false, executor: { kernel: "typescript", timeoutMs: 15000 },
    mesh: { enabled: true, persist: true }, mcp: { enabled: false }, memory: { enabled: false }, jev: { enabled: false },
    entropy: { compile: false }, speculation: { enabled: false }, prewalk: { enabled: false }, ui: { enabled: false },
    compaction: { engine: "pi" }, approvals: { read: "allow", write: "allow", exec: "allow", agent: "allow" } }));
  const cli = fileURLToPath(new URL("./cli.js", import.meta.resolve("@earendil-works/pi-coding-agent")));
  const main = start([cli, "--mode", "rpc", "--offline", "--provider", "idle-probe", "--model", "offline", "--no-extensions",
    "--no-skills", "--no-prompt-templates", "--no-context-files", "--no-themes", "--no-builtin-tools", "--approve",
    "--session-dir", path.join(root, "sessions"), "-e", path.join(repo, "dist/index.js"), ...extensions, "-e", provider]);
  const events = [];
  let settled;
  const settledPromise = new Promise(resolve => { settled = resolve; });
  readline.createInterface({ input: main.stdout }).on("line", line => {
    let event; try { event = JSON.parse(line); } catch { return; }
    events.push(event);
    if (event.type === "agent_settled") settled();
  });
  main.stdin.write(JSON.stringify({ type: "prompt", id: "idle-1", message: "Activate Fabric once, then idle." }) + "\n");
  await Promise.race([settledPromise, sleep(60_000).then(() => { throw new Error("Main activation timed out"); })]);
  const tool = events.find(e => e.type === "tool_execution_end" && e.toolName === "fabric_exec");
  assert(tool && !tool.isError, "Fabric activation failed; see child.log");
  return main;
}

// Idle ResidentHost: the minimal config of tests/residency-idle-poll.test.ts, idle exit after the window.
// ponytail: dist has no stable entry that exports the config defaults, so jiti (Pi's own loader) reads them
// from src/config.ts; the host itself is the compiled dist/residency/host.js.
async function startResident() {
  const script = path.join(root, "resident.mjs");
  write(script, `import fs from 'node:fs';
import path from 'node:path';
import { createJiti } from ${JSON.stringify(import.meta.resolve("jiti"))};
import { ResidentHost } from ${JSON.stringify(path.join(repo, "dist/residency/host.js"))};
const { DEFAULT_FABRIC_CONFIG: d } = await createJiti(import.meta.url).import(${JSON.stringify(path.join(repo, "src/config.ts"))});
const root = ${JSON.stringify(root)}, dist = ${JSON.stringify(path.join(repo, "dist"))}, mesh = path.join(root, 'mesh');
const config = { format: 1, rootId: 'session:idle-wakes', sessionId: 'idle-wakes', cwd: root, projectRoot: root, meshRoot: mesh,
  actorRoot: path.join(root, 'actors'), residencyRoot: path.join(mesh, 'residency', 'idle-wakes'), fullCodeMode: true,
  agents: d.agents, retention: d.retention, mesh: { ...d.mesh, residentIdleExitMs: ${Math.ceil((seconds + warmup + 120) * 1000)} },
  workerPath: path.join(dist, 'agents/worker.js'), fabricExtensionPath: path.join(dist, 'index.js'),
  piBinary: 'pi', claudeBinary: 'claude', vedaBinary: 'veda' };
fs.mkdirSync(config.residencyRoot, { recursive: true });
fs.writeFileSync(path.join(config.residencyRoot, 'config.json'), JSON.stringify(config));
const host = new ResidentHost(config, () => {});
process.on('SIGTERM', () => host.close().finally(() => process.exit(0)));
await host.start();
fs.writeFileSync(path.join(root, 'ready'), '');
`);
  const child = start([script]);
  for (let i = 0; i < 1200 && !fs.existsSync(path.join(root, "ready")); i++) {
    assert(child.exitCode === null, `resident host exited at start; see ${root}/child.log`);
    await sleep(50);
  }
  assert(fs.existsSync(path.join(root, "ready")), "resident host start timed out");
  return child;
}

function voluntarySwitches(pid) {
  let total = 0;
  for (const tid of fs.readdirSync(`/proc/${pid}/task`)) {
    try { total += Number(/^voluntary_ctxt_switches:\s*(\d+)/m.exec(fs.readFileSync(`/proc/${pid}/task/${tid}/status`, "utf8"))[1]); }
    catch (error) { if (error.code !== "ENOENT") throw error; }
  }
  return total;
}

async function stop(child) {
  if (child.exitCode !== null || child.signalCode !== null) return;
  child.stdin.end();
  child.kill("SIGTERM");
  const timer = setTimeout(() => child.kill("SIGKILL"), 14_000);
  await child.done; clearTimeout(timer);
}

let child, exitCode = 2;
try {
  if (mode !== "script") for (const name of ["index.js", "residency/host.js"]) assert(fs.existsSync(path.join(repo, "dist", name)), `dist/${name} is missing: run bun run build`);
  if (mode === "main") child = await startMain();
  else if (mode === "script") child = start([path.resolve(option("--script"))]);
  else child = await startResident();
  await sleep(warmup * 1000);
  assert(child.exitCode === null, `${mode} exited during warm-up; see ${root}/child.log`);
  child.kill("SIGUSR2");
  const switchesBefore = voluntarySwitches(child.pid), began = Date.now();
  await sleep(seconds * 1000);
  const switches = (voluntarySwitches(child.pid) - switchesBefore) / ((Date.now() - began) / 1000);
  child.kill("SIGUSR1");
  for (let i = 0; i < 200 && !fs.existsSync(wakeOut); i++) await sleep(50);
  const report = JSON.parse(fs.readFileSync(wakeOut, "utf8"));
  const allowed = row => allowlist.some(entry => entry.callsite === row.callsite.replace(/:\d+ /, " "));
  const gated = report.rows.filter(row => !allowed(row));
  const timerWakesPerSecond = gated.reduce((sum, row) => sum + row.perSecond, 0);
  const perCallsiteMax = Math.floor(report.seconds / 60);
  const pollers = gated.filter(row => row.wakes > perCallsiteMax);
  const pass = timerWakesPerSecond <= budget && pollers.length === 0;
  const result = { mode, extensions: extensions.filter((_, i) => i % 2), pass, warmupSeconds: warmup,
    windowSeconds: report.seconds, budget, timerWakesPerSecond, statWatchers: report.statWatchers,
    perCallsiteMax, pollers: pollers.length, voluntaryCtxSwitchesPerSecond: switches, top: report.rows.slice(0, 10).map(row => ({ ...row, allowlisted: allowed(row) || undefined })) };
  fs.writeFileSync(option("--out", path.join(root, "result.json")), JSON.stringify(result, null, 2) + "\n");
  console.log(`idle-wakes ${mode}: ${pass ? "PASS" : "FAIL"} ${timerWakesPerSecond.toFixed(2)} timer wakes/s (budget ${budget}),`
    + ` ${pollers.length} callsite(s) over ${perCallsiteMax} wakes in ${report.seconds.toFixed(0)} s;`
    + ` warm-up ${warmup} s; voluntary ctx switches ${switches.toFixed(1)}/s (info);`
    + ` watchFile files ${report.statWatchers.watchedFiles}, live STATWATCHER resources ${report.statWatchers.liveResources}`);
  for (const row of result.top) console.log(`  ${row.perSecond.toFixed(2).padStart(7)}/s ${String(row.wakes).padStart(6)} ${row.type.padEnd(9)} ${row.callsite}${row.allowlisted ? " (allowlisted)" : ""}`);
  if (process.env.GITHUB_STEP_SUMMARY) fs.appendFileSync(process.env.GITHUB_STEP_SUMMARY, [
    `### Idle wakes, ${mode}: ${pass ? "PASS" : "FAIL"}`, "",
    `${timerWakesPerSecond.toFixed(2)} timer wakes/s (budget ${budget}); ${pollers.length} callsite(s) over ${perCallsiteMax} wakes`
      + ` in ${report.seconds.toFixed(0)} s; warm-up ${warmup} s; voluntary context switches ${switches.toFixed(1)}/s (not gated);`
      + ` watchFile files ${report.statWatchers.watchedFiles}, live STATWATCHER resources ${report.statWatchers.liveResources}.`, "",
    "| wakes/s | wakes | type | callsite |", "|---:|---:|---|---|",
    ...result.top.map(row => `| ${row.perSecond.toFixed(2)} | ${row.wakes} | ${row.type} | \`${row.callsite}\`${row.allowlisted ? " (allowlisted)" : ""} |`), "", ""].join("\n"));
  exitCode = pass ? 0 : 1;
} catch (error) {
  console.error(`idle-wakes ${mode}: ERROR ${error.stack ?? error}`);
} finally {
  if (child) await stop(child);
  if (exitCode !== 2) fs.rmSync(root, { recursive: true, force: true });
}
process.exitCode = exitCode;
