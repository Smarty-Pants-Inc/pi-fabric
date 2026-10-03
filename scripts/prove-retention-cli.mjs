#!/usr/bin/env node
// Actual Pi CLI/RPC + public fabric_exec; only inference is synthetic/keyless.
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { RpcClient } from '@earendil-works/pi-coding-agent';
const repo = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const out = path.resolve(process.argv[2] ?? process.env.TASK_OUT ?? 'retention-proof');
fs.mkdirSync(out, { recursive: true });
const scratch = fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), 'retention-native-'));
const profile = path.join(scratch, 'profile'); const cwd = path.join(scratch, 'workspace');
fs.mkdirSync(profile); fs.mkdirSync(cwd);
const transcript = path.join(out, 'native-proof.jsonl'); fs.writeFileSync(transcript, '');
const record = value => fs.appendFileSync(transcript, JSON.stringify(value) + '\n');
const entry = path.join(repo, 'dist/index.js'); const sweep = path.join(repo, 'dist/storage/retention-cli.js');
const cli = fileURLToPath(new URL('./cli.js', import.meta.resolve('@earendil-works/pi-coding-agent')));
const write = (file, data) => { fs.mkdirSync(path.dirname(file), { recursive: true }); fs.writeFileSync(file, data); };
const eventsLog = count => Array.from({ length: count }, (_, sequence) => JSON.stringify({ sequence, text: 'x'.repeat(80) }) + '\n').join('');
const seed = (root, count = 2, length = 401) => {
  const host = path.join(root, 'residency', 'host'); const registry = path.join(root, 'actors', 'project'); const actor = path.join(registry, 'actor');
  write(path.join(host, 'host.lock'), JSON.stringify({ pid: 2147483647 }));
  write(path.join(host, 'config.json'), JSON.stringify({ format: 1, rootId: 'session:retention-proof', residencyRoot: host, meshRoot: root }));
  write(path.join(registry, 'actors.json'), JSON.stringify({ actors: [{ id: 'actor', rootId: 'session:retention-proof', status: 'idle', sessionFile: path.join(actor, 'session.jsonl'), ...(count === 2 ? { lastRunId: 'latest' } : {}) }] }));
  const log = eventsLog(length);
  const run = (directory, fields = {}) => {
    write(path.join(directory, 'status.json'), JSON.stringify({ status: 'completed', transport: 'process', sessionId: '2147483647', finishedAt: Date.now() - 7 * 3600000, ...fields }));
    write(path.join(directory, 'events.jsonl'), log); write(path.join(directory, 'reply.json'), '{"text":"keep result"}');
  };
  const runs = [];
  for (let i = 0; i < count; i++) { const directory = path.join(i % 2 ? actor : host, 'runs', `run-${i}`); run(directory); runs.push(directory); }
  const protectedRuns = [];
  if (count === 2) {
    for (const [name, fields] of [['live', { status: 'running' }], ['unknown', { sessionId: 'unknown' }], ['held', { sessionId: String(process.pid) }], ['pending', { cleanupPending: true }], ['unresolved', {}]]) {
      const directory = path.join(host, 'runs', name); run(directory, fields); protectedRuns.push(directory);
      if (name === 'unresolved') write(path.join(directory, 'unresolved-worker.json'), '{}');
    }
  }
  if (count === 2) { const latest = path.join(actor, 'runs', 'latest'); run(latest); protectedRuns.push(latest); }
  write(path.join(actor, 'session.jsonl'), 'active session');
  write(path.join(actor, 'session.jsonl.20260927T150000000Z.bak'), 'old backup');
  write(path.join(actor, 'session.jsonl.20260928T150000000Z.bak'), 'new backup');
  write(path.join(actor, 'session.jsonl.20260926T150000000Z.id.orphan-noheader.bak'), 'recovery evidence');
  return { host, actor, runs, protectedRuns, log };
};
const snapshot = root => fs.readdirSync(root).sort().map(name => {
  const file = path.join(root, name); const stat = fs.lstatSync(file);
  return [name, stat.ino, stat.mtimeMs, stat.ctimeMs, stat.isDirectory() ? snapshot(file) : fs.readFileSync(file, 'utf8')];
});
const bytes = root => fs.readdirSync(root).reduce((sum, name) => {
  const file = path.join(root, name); const stat = fs.lstatSync(file);
  return sum + (stat.isDirectory() ? bytes(file) : stat.size);
}, 0);
const mesh = path.join(scratch, 'mesh'); const fixture = seed(mesh); const before = snapshot(mesh);
const quote = value => "'" + value.replaceAll("'", "'\\''") + "'";
const command = mode => [quote(process.execPath), quote(sweep), quote(mesh), mode].join(' ');
const guest = mode => `const receipt = await pi.bash({cmd: ${JSON.stringify(command(mode))}, timeout: 60}); return JSON.parse(receipt.output);`;
const provider = path.join(scratch, 'provider.ts');
write(provider, `import { createAssistantMessageEventStream } from '@earendil-works/pi-ai';
export default function(pi) {
 const model = {provider:'retention-probe',id:'offline',name:'Keyless retention proof',api:'retention-probe-api',baseUrl:'http://invalid.local',reasoning:false,input:['text'],cost:{input:0,output:0,cacheRead:0,cacheWrite:0},contextWindow:200000,maxTokens:4096};
 const stream = (selected, context) => {
  const events = createAssistantMessageEventStream(); const last = context.messages.at(-1);
  const tool = last?.role !== 'toolResult';
  const prompt = JSON.stringify(context.messages.filter(message => message.role === 'user').at(-1));
  const code = prompt.includes('APPLY') ? ${JSON.stringify(guest('--apply'))} : ${JSON.stringify(guest('--dry-run'))};
  const call = {type:'toolCall',id:'retention-'+Date.now(),name:'fabric_exec',arguments:{code,resultFormat:'json'}};
  const message = {role:'assistant',provider:selected.provider,model:selected.id,api:selected.api,timestamp:Date.now(),content:tool?[call]:[{type:'text',text:'RETENTION_PROOF_COMPLETED'}],stopReason:tool?'toolUse':'stop',usage:{input:0,output:0,cacheRead:0,cacheWrite:0,totalTokens:0,cost:{input:0,output:0,cacheRead:0,cacheWrite:0,total:0}}};
  events.push({type:'start',partial:message});
  if(tool) events.push({type:'toolcall_end',contentIndex:0,toolCall:call,partial:message});
  events.push({type:'done',reason:message.stopReason,message}); events.end(); return events;
 };
 pi.registerProvider({id:'retention-probe',name:'Keyless offline proof',auth:{apiKey:{name:'Keyless',check:async()=>({type:'api_key',source:'keyless test'}),resolve:async()=>({auth:{}})}},getModels:()=>[model],stream,streamSimple:stream});
}
`);
write(path.join(profile, 'settings.json'), JSON.stringify({ compaction: { enabled: false }, retry: { enabled: false } }));
write(path.join(profile, 'fabric.json'), JSON.stringify({ fullCodeMode: true, agents: { budgetUsd: 0 }, mesh: { enabled: false, persist: false }, prewalk: { enabled: false }, executor: { kernel: 'typescript', timeoutMs: 120000 }, approvals: { read: 'allow', write: 'allow', exec: 'allow', agent: 'allow' } }));
const args = ['--no-session', '--no-extensions', '--no-skills', '--no-prompt-templates', '--no-context-files', '--offline', '-e', provider, '-e', entry];
const env = { PI_CODING_AGENT_DIR: profile, PI_OFFLINE: '1', HOME: scratch, TMPDIR: scratch };
record({ type: 'proof-command', command: [process.execPath, cli, '--mode', 'rpc', '--provider', 'retention-probe', '--model', 'offline', ...args], env, guestDryRun: guest('--dry-run'), guestApply: guest('--apply') });
const client = new RpcClient({ cliPath: cli, cwd, env, provider: 'retention-probe', model: 'offline', args });
const events = []; client.onEvent(event => { events.push(event); record(event); });
let failure; let passed = false;
try {
  await client.start(); record({ type: 'proof-state', state: await client.getState() });
  const invoke = async prompt => {
    const start = events.length; record({ type: 'proof-rpc-command', command: { type: 'prompt', message: prompt } });
    await client.promptAndWait(prompt, undefined, 120000);
    const result = events.slice(start).find(event => event.type === 'tool_execution_end' && event.toolName === 'fabric_exec');
    assert.ok(result, 'public fabric_exec result missing'); assert.equal(result.isError, false, JSON.stringify(result));
    assert.equal(result.result.details.success, true, JSON.stringify(result));
    return JSON.parse(result.result.content.filter(part => part.type === 'text').map(part => part.text).join('\n'));
  };
  const dry = await invoke('RETENTION_DRY'); assert.equal(dry.dryRun, true); assert.equal(dry.changes.length, 3); assert.deepEqual(snapshot(mesh), before);
  const applied = await invoke('RETENTION_APPLY'); assert.deepEqual(applied.changes, dry.changes); assert.equal(applied.dryRun, false);
  for (const directory of fixture.runs) {
    const lines = fs.readFileSync(path.join(directory, 'events.jsonl'), 'utf8').trim().split('\n').map(JSON.parse);
    assert.equal(lines.length, 201); assert.equal(lines[0].fabricTruncated, true); assert.equal(lines[1].sequence, 201); assert.equal(lines.at(-1).sequence, 400);
    assert.equal(fs.readFileSync(path.join(directory, 'reply.json'), 'utf8'), '{"text":"keep result"}');
  }
  for (const directory of fixture.protectedRuns) assert.equal(fs.readFileSync(path.join(directory, 'events.jsonl'), 'utf8'), fixture.log);
  assert.deepEqual(fs.readdirSync(fixture.actor).filter(name => name.endsWith('.bak')).sort(), ['session.jsonl.20260926T150000000Z.id.orphan-noheader.bak', 'session.jsonl.20260928T150000000Z.bak']);
  record({ type: 'proof-result', passed: true, dryRunNoChange: true, compactedRuns: 2, eventsPerRun: 200, preservedCustodyCases: fixture.protectedRuns.length, newestBackupOnly: true, publicApi: 'fabric_exec -> pi.bash -> compiled retention CLI', bytesSaved: dry.bytesBefore - dry.bytesAfter });
  passed = true;
} catch (error) { failure = error; record({ type: 'proof-failure', error: String(error), stderr: client.getStderr() }); }
finally {
  if (client.process) {
    const child = client.process; const exited = child.exitCode !== null || child.signalCode !== null ? Promise.resolve() : new Promise(resolve => child.once('exit', resolve));
    child.stdin.end(); const timer = setTimeout(() => child.kill('SIGTERM'), 10000); await exited; clearTimeout(timer);
    record({ type: 'proof-process-exit', exitCode: child.exitCode, signal: child.signalCode });
  }
  fs.writeFileSync(path.join(out, 'native-stderr.log'), client.getStderr());
}
try {
  if (passed) {
    const tree = path.join(scratch, 'synthetic-2k'); seed(tree, 2000, 2500); const bytesBefore = bytes(tree);
    const start = performance.now();
    const result = spawnSync(process.execPath, [sweep, tree, '--apply'], { encoding: 'utf8', timeout: 180000, maxBuffer: 4 * 1024 * 1024 });
    const elapsedMs = performance.now() - start; assert.equal(result.status, 0, result.stderr);
    const applied = JSON.parse(result.stdout); assert.equal(applied.changes.length, 2001);
    const bytesAfter = bytes(tree);
    const measurement = { runs: 2000, runDirectories: 2000, eventsPerOriginalRun: 2500, bytesBefore, bytesAfter, bytesSaved: bytesBefore - bytesAfter, elapsedMs, compacted: 2000, command: [process.execPath, sweep, tree, '--apply'] };
    fs.writeFileSync(path.join(out, 'synthetic-2k.json'), JSON.stringify(measurement, null, 2) + '\n'); console.log(JSON.stringify(measurement));
  }
} catch (error) { failure ??= error; }
finally { fs.rmSync(scratch, { recursive: true, force: true }); }
fs.writeFileSync(path.join(out, 'native-proof.txt'), `Command: nice -n 19 node scripts/prove-retention-cli.mjs ${out}\nResult: ${failure ? 'FAIL: ' + failure : 'PASS: fresh dist in real offline Pi CLI/RPC; public fabric_exec compacts 2 runs to 200 events, preserves six custody/latest cases and results; dry run changes no bytes/inodes/timestamps; newest ordinary backup retained; CLI exited; 2k measurement in synthetic-2k.json.'}\n`);
if (failure) { console.error(failure); process.exitCode = 1; }
