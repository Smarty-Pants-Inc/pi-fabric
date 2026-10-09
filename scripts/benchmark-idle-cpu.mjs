#!/usr/bin/env node
// Linux idle baseline: real compiled Fabric worker + activated native Pi RPC Main.
// No compilation, source substitution, network, inference, or shared profile/mesh.
// Examples:
//   node scripts/benchmark-idle-cpu.mjs --label baseline
//   node scripts/benchmark-idle-cpu.mjs --label after --compare .local/repro-baseline-.../result.json
//   node scripts/benchmark-idle-cpu.mjs --self-test
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import readline from 'node:readline';
import { spawn, execFileSync } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { performance } from 'node:perf_hooks';

const repo = fileURLToPath(new URL('../', import.meta.url));
const argv = process.argv.slice(2);
const option = (key, fallback) => { const i = argv.indexOf(key); return i < 0 ? fallback : argv[i + 1]; };
const duration = Number(option('--seconds', '30'));
const profileSeconds = Number(option('--profile-seconds', '30'));
const warmup = Number(option('--warmup-seconds', '5'));
const label = option('--label', 'baseline');
assert(/^[a-zA-Z0-9_-]+$/.test(label), 'label must be a filename-safe token');
assert(process.platform === 'linux', 'This probe requires Linux /proc');
for (const seconds of [duration, profileSeconds, warmup]) assert(Number.isFinite(seconds) && seconds > 0 && seconds <= 120);
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const json = (file, value) => { fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 }); fs.writeFileSync(file, JSON.stringify(value, null, 2) + '\n', { mode: 0o600 }); };
const hash = text => createHash('sha256').update(text).digest('hex');
const started = new Date().toISOString();
const out = path.join(repo, '.local', `repro-${label}-${Date.now()}-${process.pid}`);
fs.mkdirSync(out, { mode: 0o700, recursive: true });
const helper = path.join(repo, '.local', 'repro-idle-preload.cjs');
const fake = path.join(repo, '.local', 'repro-idle-pi.mjs');
const provider = path.join(repo, '.local', 'repro-idle-provider.mjs');
const cleanEnv = root => ({ PATH: process.env.PATH, HOME: path.join(root, 'home'), TMPDIR: root,
  PI_CODING_AGENT_DIR: path.join(root, 'profile'), PI_OFFLINE: '1',
  PI_FABRIC_MESH_ROOT: path.join(root, 'mesh'), PI_FABRIC_PROJECT_ROOT: root,
  PI_FABRIC_RUN_ROOT: path.join(root, 'runs'), PI_FABRIC_AGENT_DIR: path.join(root, 'exports') });

// Adapted from tests/fixtures/resident-probe-pi.mjs: native RPC model handshake,
// then a pending activation whose child is sleeping, with no inference/events.
fs.writeFileSync(fake, `import fs from 'node:fs';
import readline from 'node:readline';
fs.writeFileSync(process.env.IDLE_BIRTH,JSON.stringify({pid:process.pid,birth:fs.readFileSync('/proc/self/stat','utf8').split(')')[1].trim().split(/\\s+/)[19]}));
let model={provider:'idle-probe',id:'offline',contextWindow:200000}, thinkingLevel='high';
const input=readline.createInterface({input:process.stdin});
input.on('line', line=>{const f=JSON.parse(line);let data={};
 if(f.type==='get_state') data={model,thinkingLevel,isStreaming:false,isCompacting:false};
 else if(f.type==='set_model') model={...model,provider:f.provider,id:f.modelId};
 else if(f.type==='set_thinking_level') thinkingLevel=f.level;
 else if(f.type==='get_messages') data={messages:[]};
 else if(f.type==='prompt') {fs.writeFileSync(process.env.IDLE_READY,JSON.stringify({pid:process.pid,frame:f.type,inference:false}));return;}
 process.stdout.write(JSON.stringify({type:'response',command:f.type,id:f.id,success:true,data})+'\\n');
});
input.on('close',()=>process.exit(0));
`, { mode: 0o600 });

// Same keyless provider and public fabric_exec activation path as
// tests/fixtures/main-bindings-cli-provider.ts. Only its two responses are synthetic.
fs.writeFileSync(provider, `import fs from 'node:fs';
import {createAssistantMessageEventStream} from '@earendil-works/pi-ai';
export default function(pi) {
 const model={provider:'idle-probe',id:'offline',name:'Offline idle probe',api:'idle-probe',baseUrl:'http://invalid.local',reasoning:true,input:['text'],cost:{input:0,output:0,cacheRead:0,cacheWrite:0},contextWindow:200000,maxTokens:4096};
 const stream=(selected,context)=>{
  const tool=context.messages.at(-1)?.role!=='toolResult';
  fs.appendFileSync(process.env.IDLE_INFERENCE,JSON.stringify({synthetic:true,tool})+'\\n');
  const message={role:'assistant',provider:selected.provider,model:selected.id,api:selected.api,timestamp:Date.now(),
   content:tool?[{type:'toolCall',id:'idle-activate',name:'fabric_exec',arguments:{code:'return await agents.main();',resultFormat:'json'}}]:[{type:'text',text:'Idle probe activated'}],stopReason:tool?'toolUse':'stop',
   usage:{input:0,output:0,cacheRead:0,cacheWrite:0,totalTokens:0,cost:{input:0,output:0,cacheRead:0,cacheWrite:0,total:0}}};
  const events=createAssistantMessageEventStream();events.push({type:'start',partial:message});
  if(tool)events.push({type:'toolcall_end',contentIndex:0,toolCall:message.content[0],partial:message});
  events.push({type:'done',reason:message.stopReason,message});events.end();return events;
 };
 pi.registerProvider({id:model.provider,name:model.name,auth:{apiKey:{name:'Keyless private probe',check:async()=>({type:'api_key',source:'keyless test'}),resolve:async()=>({auth:{}})}},getModels:()=>[model],stream,streamSimple:stream});
}
`, { mode: 0o600 });

// A separate instrumented pass: counts APIs and returned read bytes; captures
// bounded first-hit stack samples. SIGUSR2 begins the window, SIGUSR1 ends it.
// The hook never instruments the driver or unrelated live processes.
fs.writeFileSync(helper, `const fs=require('node:fs');const {syncBuiltinESMExports}=require('node:module');
const write=fs.writeFileSync.bind(fs);let active=false, began=0, counts=new Map(), fdPaths=new Map();
const bucket=p=>String(p).replace(/\\/proc\\/\\d+/g,'/proc/:pid').replace(/\\/agent-completions\\/[a-f0-9]{64}/g,'/agent-completions/:hash').replace(/\\/participants\\/[a-f0-9]{64}/g,'/participants/:hash');
function record(api,p,bytes=0){if(!active)return;const key=api+' '+bucket(p);let row=counts.get(key);
 if(!row){row={api,path:bucket(p),calls:0,bytes:0,samples:[]};counts.set(key,row);}row.calls++;row.bytes+=bytes;
 if(row.samples.length<3 && (row.calls===1 || row.calls===100 || row.calls===10000))row.samples.push(new Error().stack.split('\\n').slice(3,11));}
for(const api of ['readFileSync','readdirSync','statSync','lstatSync','existsSync','openSync','readSync']){const original=fs[api];fs[api]=function(...args){let p=typeof args[0]==='number'?fdPaths.get(args[0])??'fd:'+args[0]:args[0];let value;
 try{value=original.apply(this,args);return value;}finally{if(api==='openSync' && typeof value==='number')fdPaths.set(value,String(args[0]));record(api,p,api==='readSync'?value??0:api==='readFileSync'&&value!=null?Buffer.byteLength(value):0);}};}
const close=fs.closeSync;fs.closeSync=function(fd){fdPaths.delete(fd);return close.call(this,fd);};
for(const api of ['readFile','readdir','stat','lstat','open']){const original=fs.promises[api];fs.promises[api]=async function(...args){const value=await original.apply(this,args);record('promises.'+api,args[0],api==='readFile'?Buffer.byteLength(value):0);
 if(api==='open'){const p=args[0];for(const method of ['read','readFile']){const originalMethod=value[method];value[method]=async function(...a){const result=await originalMethod.apply(this,a);record('FileHandle.'+method,p,method==='read'?result.bytesRead:Buffer.byteLength(result));return result;};}}
 return value;};}
syncBuiltinESMExports();
function flush(){if(!active)return;active=false;write(process.env.IDLE_PROFILE_OUT,JSON.stringify({pid:process.pid,seconds:(Date.now()-began)/1000,rows:[...counts.values()].sort((a,b)=>b.calls-a.calls)},null,2));}
process.on('SIGUSR2',()=>{counts=new Map();began=Date.now();active=true;write(process.env.IDLE_PROFILE_OUT+'.started',String(began));});
process.on('SIGUSR1',flush);process.on('exit',flush);
`, { mode: 0o600 });

function procStat(pid) {
  const body = fs.readFileSync(`/proc/${pid}/stat`, 'utf8');
  const f = body.slice(body.lastIndexOf(')') + 2).trim().split(/\s+/);
  return { state: f[0], ppid: Number(f[1]), group: Number(f[2]), ticks: Number(f[11]) + Number(f[12]), birth: f[19] };
}
function snapshot(pid) {
  const stat = procStat(pid);
  const io = Object.fromEntries(fs.readFileSync(`/proc/${pid}/io`, 'utf8').trim().split('\n').map(line => line.split(/:\s*/)));
  const threads = {};
  for (const tid of fs.readdirSync(`/proc/${pid}/task`)) {
    try {
      const status = fs.readFileSync(`/proc/${pid}/task/${tid}/status`, 'utf8');
      const field = name => Number(new RegExp(`^${name}:\\s*(\\d+)`, 'm').exec(status)?.[1] ?? 0);
      threads[tid] = { name: /^Name:\s*(.*)$/m.exec(status)?.[1], voluntary: field('voluntary_ctxt_switches'), involuntary: field('nonvoluntary_ctxt_switches') };
    } catch (error) { if (error.code !== 'ENOENT') throw error; }
  }
  return { at: performance.now(), stat, syscr: Number(io.syscr), rchar: Number(io.rchar), threads };
}
const hz = Number(execFileSync('getconf', ['CLK_TCK'], { encoding: 'utf8' }).trim());
function delta(name, pid, a, b) {
  assert.equal(a.stat.birth, b.stat.birth, 'process birth changed');
  const seconds = (b.at - a.at) / 1000;
  const threads = Object.entries(b.threads).map(([tid, v]) => ({ tid: Number(tid), name: v.name, newThread: !a.threads[tid],
    voluntaryPerSecond: (v.voluntary - (a.threads[tid]?.voluntary ?? 0)) / seconds,
    involuntaryPerSecond: (v.involuntary - (a.threads[tid]?.involuntary ?? 0)) / seconds }));
  return { name, pid, seconds, cpuCore: (b.stat.ticks - a.stat.ticks) / hz / seconds,
    syscrPerSecond: (b.syscr - a.syscr) / seconds, rcharPerSecond: (b.rchar - a.rchar) / seconds,
    voluntaryPerSecond: threads.reduce((sum, t) => sum + t.voluntaryPerSecond, 0),
    involuntaryPerSecond: threads.reduce((sum, t) => sum + t.involuntaryPerSecond, 0),
    exitedThreads: Object.keys(a.threads).filter(tid => !b.threads[tid]), threads, before: a, after: b };
}
function seed(root) {
  fs.mkdirSync(path.join(root, 'home'), { recursive: true });
  const mesh = path.join(root, 'mesh'); fs.mkdirSync(mesh);
  const now = Date.now(), entries = {}, identity = { id: 'fixture-seeder', name: 'private fixture', kind: 'main' };
  let version = 0;
  const entry = (key, value, updatedBy = identity) => entries[key] = { key, value, version: ++version, updatedAt: now, updatedBy };
  const completions = path.join(mesh, 'agent-completions'); fs.mkdirSync(completions);
  for (let i = 0; i < 2000; i++) {
    const id = hash(`foreign-completion-${i}`).slice(0, 32);
    json(path.join(completions, hash(id) + '.json'), { format: 1,
      recipient: { rootId: `session:foreign-${i}`, sessionId: `foreign-${i}`, projectRoot: root, cwd: root, name: `foreign-${i}`, startedAt: now },
      result: { id, name: `foreign-${i}`, task: 'retained foreign completion', status: 'completed', text: 'x'.repeat(2048), runner: 'pi', transport: 'process', cwd: root, startedAt: now - 1000, updatedAt: now, finishedAt: now, turns: 0, toolCalls: 0, usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 } } });
  }
  for (let i = 0; i < 1000; i++) entry(`residency/deliveries/foreign-${i}/retained`, { format: 1, rootId: `session:foreign-${i}`, id: `delivery-${i}`,
    from: { id: hash(`foreign-actor-${i}`).slice(0, 32), name: `foreign-actor-${i}`, kind: 'actor' }, message: 'foreign retained delivery', delivery: 'followUp', triggerTurn: true });
  fs.mkdirSync(path.join(mesh, 'participants'));
  for (let i = 0; i < 500; i++) {
    const id = `session:foreign-${i}`, host = `foreign-host-${i}`, owner = { id: host, name: host, kind: 'main' };
    const value = { format: 1, id, kind: 'root', rootId: id, ownerHostId: host, ownerIdentityId: id, name: `foreign-${i}`, status: 'idle', runner: 'pi', transport: 'host', capabilities: ['fabric', 'followUp'], cwd: root, projectRoot: root, sessionId: `foreign-${i}`, startedAt: now, updatedAt: now, controlProtocol: 'v1' };
    const key = `topology/participants/${hash(id)}`; entry(key, value, owner);
    json(path.join(mesh, 'participants', hash(id) + '.json'), { format: 1, ...entries[key] });
    entry(`topology/hosts/${hash(host)}`, { format: 1, id: host, rootId: id, identity: owner, startedAt: now, updatedAt: now, expiresAt: now + 3600_000 }, owner);
  }
  for (let i = 0; i < 50; i++) {
    const id = hash(`actor-${i}`).slice(0, 32), run = hash(`run-${i}`).slice(0, 32);
    const dir = path.join(mesh, 'actors', `foreign-${i}`, id);
    json(path.join(dir, 'actor.json'), { format: 1, id, name: `foreign-${i}`, rootId: `session:foreign-${i}`, ownerSessionId: `foreign-${i}`, residency: 'durable', status: 'idle', events: [], topics: [], runner: 'pi', transport: 'process', delivery: 'mailbox', responseMode: 'text', triggerTurn: false, coalesce: true, queued: 0, messages: 0, createdAt: now, updatedAt: now });
    json(path.join(dir, 'runs', run, 'status.json'), { id: run, actorId: id, status: 'completed', runner: 'pi', transport: 'process', startedAt: now - 1000, finishedAt: now, updatedAt: now, text: 'retained' });
    fs.writeFileSync(path.join(dir, 'runs', run, 'events.jsonl'), '{}\n');
    json(path.join(root, 'runs', run, 'status.json'), { id: run, actorId: id, status: 'completed', runner: 'pi', transport: 'process', startedAt: now - 1000, finishedAt: now, updatedAt: now });
  }
  const key = 'probe/idle-padding'; entry(key, '');
  const state = { format: 1, revisionFormat: 2, readGeneration: 'private-idle-fixture', highWater: version, entries };
  entries[key].value = 'x'.repeat(4 * 1024 * 1024 - Buffer.byteLength(JSON.stringify(state)));
  fs.writeFileSync(path.join(mesh, 'state.json'), JSON.stringify(state));
  assert.equal(fs.statSync(path.join(mesh, 'state.json')).size, 4 * 1024 * 1024);
  const summary = { foreignCompletions: fs.readdirSync(completions).length, deliveryKeys: Object.keys(entries).filter(k => k.startsWith('residency/deliveries/')).length,
    memberRecords: 500, hostRecords: 500, actorDirectories: 50, actorRuns: 50, mainRunDirectories: 50, stateBytes: fs.statSync(path.join(mesh, 'state.json')).size };
  assert.equal(summary.foreignCompletions, 2000); assert.equal(summary.deliveryKeys, 1000);
  // Private throwaway settings, not a repository, shared profile, service or backend change.
  json(path.join(root, 'profile', 'settings.json'), { compaction: { enabled: false }, retry: { enabled: false } });
  json(path.join(root, 'profile', 'fabric.json'), { fullCodeMode: false,
    executor: { kernel: 'typescript', timeoutMs: 15000 }, mesh: { enabled: true, persist: true, actorPollMs: 250 },
    mcp: { enabled: false }, memory: { enabled: false }, jev: { enabled: false }, entropy: { compile: false },
    speculation: { enabled: false }, prewalk: { enabled: false }, ui: { enabled: false }, compaction: { engine: 'pi' },
    approvals: { read: 'allow', write: 'allow', exec: 'allow', agent: 'allow' } });
  return summary;
}
const owned = [], nativeBirthFiles = [], nativeBirths = [];
function birthAlive(item) {
  try { const current = procStat(item.pid); return current.birth === item.birth && !['Z', 'X'].includes(current.state); }
  catch (error) { if (['ENOENT', 'ESRCH'].includes(error.code)) return false; return 'unknown'; }
}
function captureNativeDescendants() {
  const records = [];
  for (const name of fs.readdirSync('/proc').filter(n => /^\d+$/.test(n))) {
    try { records.push({ pid: Number(name), ...procStat(Number(name)) }); } catch { /* disappearing processes */ }
  }
  const parents = new Set(owned.map(item => item.pid));
  for (let changed = true; changed;) {
    changed = false;
    for (const row of records) if (parents.has(row.ppid) && !parents.has(row.pid)) {
      parents.add(row.pid); nativeBirths.push({ pid: row.pid, birth: row.birth }); changed = true;
    }
  }
}
function startChild(name, args, env, instrumented) {
  const stdout = fs.openSync(path.join(out, name + '.stdout'), 'w');
  const stderr = fs.openSync(path.join(out, name + '.stderr'), 'w');
  const child = spawn(process.execPath, [...(instrumented ? ['--require', helper] : []), ...args], { cwd: env.PI_FABRIC_PROJECT_ROOT, env,
    stdio: ['pipe', 'pipe', stderr], detached: false });
  fs.closeSync(stderr);
  child.stdout.on('data', data => fs.writeSync(stdout, data));
  child.once('close', () => fs.closeSync(stdout));
  const done = new Promise(resolve => { child.once('exit', (code, signal) => resolve({ code, signal })); child.once('error', error => resolve({ error: String(error) })); });
  const item = { name, child, done, pid: child.pid, birth: child.pid ? procStat(child.pid).birth : undefined };
  owned.push(item); json(path.join(out, name + '.owned.json'), { name, pid: item.pid, birth: item.birth, command: [process.execPath, ...args], stdout: name + '.stdout', stderr: name + '.stderr' });
  return item;
}
async function stop(item) {
  if (item.child.exitCode !== null || item.child.signalCode !== null) return await item.done;
  item.child.stdin?.end();
  // Worker owns its detached native child's execution-group drain. Do not
  // bypass that obligation by killing the custodian before its grace expires.
  item.child.kill('SIGTERM');
  const timer = setTimeout(() => item.child.kill('SIGKILL'), 14000);
  const terminal = await item.done; clearTimeout(timer);
  return terminal;
}
async function waitFile(file, child, timeout = 45000) {
  const until = performance.now() + timeout;
  while (!fs.existsSync(file)) {
    assert(child.exitCode === null && child.signalCode === null, 'child exited before readiness');
    assert(performance.now() < until, `readiness timeout: ${file}`); await sleep(25);
  }
}
function rpc(item) {
  const pending = new Map(), events = [], listeners = new Set(); let sequence = 0;
  readline.createInterface({ input: item.child.stdout }).on('line', line => {
    let event; try { event = JSON.parse(line); } catch { return; }
    events.push(event); for (const fn of listeners) fn(event);
    if (event.type === 'response' && pending.has(event.id)) {
      const { resolve, reject, timer } = pending.get(event.id); pending.delete(event.id); clearTimeout(timer);
      event.success ? resolve(event.data) : reject(Error(event.error ?? JSON.stringify(event)));
    }
  });
  const send = (type, fields = {}) => new Promise((resolve, reject) => {
    const id = `idle-probe-${++sequence}`, timer = setTimeout(() => { pending.delete(id); reject(Error(`RPC timeout: ${type}`)); }, 45000);
    pending.set(id, { resolve, reject, timer }); item.child.stdin.write(JSON.stringify({ type, id, ...fields }) + '\n');
  });
  const settled = () => new Promise((resolve, reject) => {
    const timer = setTimeout(() => { listeners.delete(fn); reject(Error('activation settlement timeout')); }, 45000);
    const fn = event => { if (event.type === 'agent_settled') { listeners.delete(fn); clearTimeout(timer); resolve(event); } };
    listeners.add(fn);
  });
  return { send, settled, events };
}
async function runPass(root, pass, instrumented) {
  const run = path.join(root, 'runs', randomUUID().replaceAll('-', '')); fs.mkdirSync(run, { recursive: true });
  const task = path.join(run, 'task.txt'), steer = path.join(run, 'steer.jsonl'), session = path.join(run, 'session.jsonl');
  fs.writeFileSync(task, 'Private idle activation: no inference.'); fs.writeFileSync(steer, '');
  fs.writeFileSync(session, JSON.stringify({ type: 'session', version: 3, id: randomUUID(), timestamp: new Date().toISOString(), cwd: root }) + '\n');
  const ready = path.join(run, 'ready.json'), birthFile = path.join(run, 'native-birth.json');
  nativeBirthFiles.push(birthFile);
  const env = cleanEnv(root);
  const flags = { id: path.basename(run), name: 'idle-durable-probe', runner: 'pi', transport: 'process', 'task-file': task, 'status-file': path.join(run, 'status.json'), 'lifecycle-file': path.join(run, 'lifecycle.jsonl'), 'log-file': path.join(run, 'events.jsonl'), cwd: root,
    'pi-binary': fake, 'claude-binary': 'unused-claude', 'veda-binary': 'unused-veda', 'veda-backend': 'unused', 'veda-persona': 'unused', 'timeout-ms': '180000', depth: '1', 'full-code-mode': 'false', extensions: 'true', tools: '[]', 'granted-risks': '[]',
    'fabric-extension': path.join(repo, 'dist/index.js'), 'actor-id': 'a'.repeat(32), 'actor-name': 'idle-durable-probe', 'session-file': session, 'inference-context': 'full-history', 'steer-file': steer,
    'mesh-root': env.PI_FABRIC_MESH_ROOT, 'project-root': root, 'main-agent-id': 'session:probe-worker-owner', 'run-root': env.PI_FABRIC_RUN_ROOT, model: 'idle-probe/offline', thinking: 'high' };
  const worker = startChild(pass + '-worker', [path.join(repo, 'dist/worker.js'), ...Object.entries(flags).flatMap(([key, value]) => ['--' + key, value])],
    { ...env, IDLE_READY: ready, IDLE_BIRTH: birthFile, IDLE_PROFILE_OUT: path.join(out, pass + '-worker-profile.json') }, instrumented);
  await waitFile(ready, worker.child);
  const fakePid = JSON.parse(fs.readFileSync(ready, 'utf8')).pid;
  nativeBirths.push(JSON.parse(fs.readFileSync(birthFile, 'utf8')));
  // Native CLI entry resolution matches scripts/prove-main-bindings.mjs.
  const cli = fileURLToPath(new URL('./cli.js', import.meta.resolve('@earendil-works/pi-coding-agent')));
  const main = startChild(pass + '-main', [cli, '--mode', 'rpc', '--offline', '--provider', 'idle-probe', '--model', 'offline', '--thinking', 'high', '--no-extensions', '--no-skills', '--no-prompt-templates', '--no-context-files', '--no-themes', '--no-builtin-tools', '--approve', '--session-dir', path.join(root, 'sessions'), '-e', path.join(repo, 'dist/index.js'), '-e', provider],
    { ...env, IDLE_INFERENCE: path.join(out, pass + '-synthetic-inference.jsonl'), IDLE_PROFILE_OUT: path.join(out, pass + '-main-profile.json') }, instrumented);
  const client = rpc(main); await client.send('get_state');
  json(path.join(out, pass + '-commands.json'), await client.send('get_commands'));
  const boundary = client.settled(); await client.send('prompt', { message: 'Activate Fabric once, then idle.' }); await boundary;
  const tool = client.events.find(e => e.type === 'tool_execution_end' && e.toolName === 'fabric_exec');
  assert(tool && !tool.isError && tool.result?.details?.success === true, 'public Fabric activation failed; inspect Main stdout/stderr');
  const state = await client.send('get_state'); assert.equal(state.isStreaming, false); assert.equal(state.isCompacting, false);
  json(path.join(out, pass + '-activation.json'), { state, tool, workerStatus: JSON.parse(fs.readFileSync(flags['status-file'], 'utf8')), fakeChild: JSON.parse(fs.readFileSync(ready, 'utf8')) });
  await sleep(warmup * 1000);
  const subjects = [{ name: 'worker', pid: worker.pid }, { name: 'main', pid: main.pid }, { name: 'sleeping-pi-child', pid: fakePid }];
  if (instrumented) {
    for (const item of [worker, main]) item.child.kill('SIGUSR2');
    for (const item of [worker, main]) await waitFile(path.join(out, item.name + '-profile.json.started'), item.child);
  }
  const before = subjects.map(s => snapshot(s.pid));
  const inferenceBefore = fs.statSync(path.join(out, pass + '-synthetic-inference.jsonl')).size;
  await sleep((instrumented ? profileSeconds : duration) * 1000);
  const after = subjects.map(s => snapshot(s.pid));
  assert.equal(fs.statSync(path.join(out, pass + '-synthetic-inference.jsonl')).size, inferenceBefore, 'new inference during idle window');
  if (instrumented) {
    for (const item of [worker, main]) item.child.kill('SIGUSR1');
    for (const item of [worker, main]) await waitFile(path.join(out, item.name + '-profile.json'), item.child);
  }
  const rows = subjects.map((s, i) => delta(s.name, s.pid, before[i], after[i]));
  json(path.join(out, pass + '-proc.json'), rows);
  const terminal = await Promise.all([worker, main].map(stop));
  assert.throws(() => procStat(fakePid), 'sleeping native child still exists after worker cleanup');
  json(path.join(out, pass + '-cleanup.json'), { terminal, workerExited: worker.child.exitCode !== null || worker.child.signalCode !== null, mainExited: main.child.exitCode !== null || main.child.signalCode !== null, fakeChildAbsent: true });
  return rows;
}
const roots = [];
let failure;
const metadata = { version: 1, started, label, node: process.version, kernel: os.release(), host: os.hostname(), hz,
  requestedSeconds: duration, profileSeconds, warmupSeconds: warmup, driver: fileURLToPath(import.meta.url), driverSha256: hash(fs.readFileSync(fileURLToPath(import.meta.url))),
  procPopulation: fs.readdirSync('/proc').filter(name => /^\d+$/.test(name)).length, command: [process.execPath, ...process.argv.slice(1)] };
try {
  const scratchFile = path.join(repo, '.local', 'idle-scratch');
  const parent = fs.existsSync(scratchFile) ? fs.readFileSync(scratchFile, 'utf8').trim() : os.tmpdir();
  assert(fs.statSync(parent).isDirectory(), 'scratch parent does not exist');
  const fixture = () => { const root = fs.mkdtempSync(path.join(parent, 'repro-idle-')); roots.push(root); return { root, seeded: seed(root) }; };
  if (argv.includes('--self-test')) {
    const { root, seeded } = fixture();
    const ready = path.join(root, 'control-ready');
    const control = startChild('self-test-control', ['--input-type=module', '-e', `import fs from 'node:fs';fs.writeFileSync(${JSON.stringify(ready)},'ready');setInterval(()=>fs.readFileSync('/proc/self/stat','utf8'),100);`], { ...cleanEnv(root), IDLE_PROFILE_OUT: path.join(out, 'self-test-profile.json') }, true);
    await waitFile(ready, control.child); control.child.kill('SIGUSR2'); await waitFile(path.join(out, 'self-test-profile.json.started'), control.child);
    const before = snapshot(control.pid); await sleep(1000); const row = delta('control-only-not-worker', control.pid, before, snapshot(control.pid));
    control.child.kill('SIGUSR1'); await waitFile(path.join(out, 'self-test-profile.json'), control.child);
    const profile = JSON.parse(fs.readFileSync(path.join(out, 'self-test-profile.json'), 'utf8'));
    assert(profile.rows.some(r => r.api === 'readFileSync' && r.calls >= 5 && r.bytes > 0));
    assert(row.syscrPerSecond >= 5); assert(row.threads.length >= 1);
    await stop(control); json(path.join(out, 'result.json'), { ...metadata, selfTestOnly: true, seeded, row, profile });
    console.log(JSON.stringify({ selfTestOnly: true, passed: true, out, seeded }));
  } else {
    for (const name of ['worker.js', 'index.js']) assert(fs.existsSync(path.join(repo, 'dist', name)), `BLOCKED: compiled dist/${name} absent; owner must complete the native build. No source-worker fallback.`);
    metadata.artifacts = Object.fromEntries(['worker.js', 'index.js'].map(name => [name, hash(fs.readFileSync(path.join(repo, 'dist', name)))]));
    const baselineFixture = fixture(); const measured = await runPass(baselineFixture.root, 'unprofiled', false);
    let profiled;
    if (!argv.includes('--no-profile')) { const f = fixture(); profiled = await runPass(f.root, 'attribution', true); }
    const result = { ...metadata, seeded: baselineFixture.seeded, measured, profiled };
    if (option('--compare')) {
      const prior = JSON.parse(fs.readFileSync(path.resolve(option('--compare')), 'utf8'));
      assert(!prior.selfTestOnly && Array.isArray(prior.measured), 'comparison input is not a real worker/Main measurement');
      assert.equal(prior.requestedSeconds, duration); assert.equal(prior.host, metadata.host, 'before/after hosts differ');
      result.comparison = measured.map(row => { const before = prior.measured.find(r => r.name === row.name); assert(before); return { name: row.name,
        cpuCoreBefore: before.cpuCore, cpuCoreAfter: row.cpuCore, syscrPerSecondBefore: before.syscrPerSecond, syscrPerSecondAfter: row.syscrPerSecond,
        voluntaryPerSecondBefore: before.voluntaryPerSecond, voluntaryPerSecondAfter: row.voluntaryPerSecond, involuntaryPerSecondBefore: before.involuntaryPerSecond, involuntaryPerSecondAfter: row.involuntaryPerSecond }; });
    }
    assert.deepEqual(metadata.artifacts, Object.fromEntries(['worker.js', 'index.js'].map(name => [name, hash(fs.readFileSync(path.join(repo, 'dist', name)))])), 'compiled artifact changed during probe');
    json(path.join(out, 'result.json'), result);
    console.log(JSON.stringify({ out, measured: measured.map(({ name, cpuCore, syscrPerSecond, voluntaryPerSecond, involuntaryPerSecond }) => ({ name, cpuCore, syscrPerSecond, voluntaryPerSecond, involuntaryPerSecond })), comparison: result.comparison }));
  }
} catch (error) {
  failure = error; json(path.join(out, 'failure.json'), { ...metadata, error: String(error), stack: error.stack }); console.error(error);
} finally {
  captureNativeDescendants();
  const cleanup = [];
  for (const item of owned) {
    cleanup.push({ name: item.name, pid: item.pid, terminal: await stop(item) });
  }
  for (const file of nativeBirthFiles) {
    if (fs.existsSync(file)) nativeBirths.push(JSON.parse(fs.readFileSync(file, 'utf8')));
  }
  const births = [...new Map(nativeBirths.map(item => [`${item.pid}:${item.birth}`, item])).values()];
  // A forced custodian exit is not a native-tree completion receipt. Retain
  // scratch whenever any recorded native birth survives or cannot be checked.
  const unresolved = births.filter(item => birthAlive(item) !== false);
  if (unresolved.length) { failure ??= Error('Native cleanup unconfirmed; fixture roots retained'); process.exitCode = 1; }
  if (!unresolved.length) for (const root of roots) fs.rmSync(root, { recursive: true, force: true });
  json(path.join(out, 'terminal.json'), { started, ended: new Date().toISOString(), exitCode: failure ? 1 : 0, cleanup,
    nativeBirths: births, unresolved, rootsRetained: unresolved.length ? roots : [], rootsRemoved: unresolved.length === 0 });
}
if (failure) process.exitCode = 1;
