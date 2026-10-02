// Real persisted Pi Main CLI proof. Only model transport is deterministic/keyless.
// Usage: PI_FABRIC_PROBE_PI=/path/to/pi node scripts/probe-inbox-shadow-dedup.mjs dist/index.js $TASK_OUT/cli
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { pathToFileURL, fileURLToPath } from 'node:url';
const lane = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const candidate = path.resolve(process.argv[2] ?? 'dist/index.js');
const out = path.resolve(process.argv[3] ?? process.env.TASK_OUT ?? (() => { throw new Error('Supply retained output directory'); })());
fs.mkdirSync(out, { recursive: true });
const scratch = fs.mkdtempSync(path.join(os.tmpdir(), 'inbox-main-cli-'));
const profile = path.join(scratch, 'profile'), cwd = path.join(scratch, 'workspace'), meshRoot = path.join(scratch, 'mesh');
for (const dir of [profile, cwd, path.join(scratch, 'home')]) fs.mkdirSync(dir);
const fixture = path.join(lane, 'tests/fixtures/inbox-dedup-cli-provider.ts');
const pi = process.env.PI_FABRIC_PROBE_PI ?? 'pi';
const inferenceFile = path.join(out, 'inferences.jsonl'), hostFile = path.join(out, 'host.jsonl');
for (const file of [inferenceFile, hostFile, path.join(out, 'commands.jsonl'), path.join(out, 'rpc.jsonl')]) fs.writeFileSync(file, '');
fs.writeFileSync(path.join(profile, 'settings.json'), JSON.stringify({ compaction: { enabled: false }, retry: { enabled: false } }));
fs.writeFileSync(path.join(profile, 'fabric.json'), JSON.stringify({ fullCodeMode: true, prewalk: { enabled: false }, components: [],
  approvals: { read: 'allow', write: 'allow', exec: 'allow', agent: 'allow' },
  executor: { kernel: 'typescript', timeoutMs: 120000 }, mesh: { enabled: true, announce: true, maxEventBytes: 65536, followUpFlushMs: 1 } }));
const env = { PATH: process.env.PATH, HOME: path.join(scratch, 'home'), TMPDIR: scratch, PI_CODING_AGENT_DIR: profile,
  PI_OFFLINE: '1', PI_FABRIC_MESH_ROOT: meshRoot, PI_FABRIC_INBOX_WAKE_MS: '100', PI_FABRIC_INBOX_WAKE_COOLDOWN_MS: '0',
  INBOX_PROOF_INFERENCES: inferenceFile, INBOX_PROOF_HOST: hostFile };
const readLines = file => fs.existsSync(file) ? fs.readFileSync(file, 'utf8').trim().split('\n').filter(Boolean).map(line => JSON.parse(line)) : [];
const inferCount = () => readLines(inferenceFile).length;
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const waitFor = async (predicate, timeout = 10000) => {
  const deadline = Date.now() + timeout;
  while (!predicate()) { if (Date.now() > deadline) throw new Error('Proof observation timed out'); await sleep(50); }
};
let sessionFile, rootId, current, requestId = 0;
const exits = [], phases = [];
const entries = () => readLines(sessionFile).filter(entry => entry.type !== 'session');
const inboxes = () => entries().filter(entry => entry.type === 'custom_message' && entry.customType === 'pi-fabric-inbox');
const summaries = () => entries().filter(entry => entry.type === 'custom_message' && entry.customType === 'pi-fabric-inbox-summary');
const { MeshStore } = await import(pathToFileURL(path.join(path.dirname(candidate), 'mesh.js')).href);
const mesh = new MeshStore(meshRoot, 65536, 500);
const wakes = () => mesh.read({ after: 0, limit: 10000 }).filter(event => event.topic === 'fabric.inbox.wake');
const launch = async (resume = false) => {
  const args = ['--mode', 'rpc', '--offline', '--no-extensions', '--no-skills', '--no-prompt-templates', '--no-themes', '--no-context-files',
    '--approve', '--provider', 'inbox-proof', '--model', 'offline', '-e', candidate, '-e', fixture,
    '--session-dir', path.join(scratch, 'sessions'), ...(resume ? ['--session', sessionFile] : [])];
  fs.appendFileSync(path.join(out, 'commands.jsonl'), JSON.stringify({ executable: pi, args, cwd, env, candidateSha256: createHash('sha256').update(fs.readFileSync(candidate)).digest('hex') }) + '\n');
  const child = spawn(pi, args, { cwd, env, stdio: ['pipe', 'pipe', 'pipe'] });
  const pending = new Map(), records = [];
  let buffer = '', stderr = '';
  const exit = new Promise(resolve => child.once('exit', (code, signal) => { exits.push({ pid: child.pid, code, signal }); resolve(); }));
  child.on('error', error => { for (const { reject } of pending.values()) reject(error); });
  child.stdout.setEncoding('utf8');
  child.stdout.on('data', chunk => {
    buffer += chunk;
    for (;;) {
      const lf = buffer.indexOf('\n'); if (lf < 0) break;
      const line = buffer.slice(0, lf); buffer = buffer.slice(lf + 1); if (!line.trim()) continue;
      const record = JSON.parse(line); records.push(record);
      fs.appendFileSync(path.join(out, 'rpc.jsonl'), JSON.stringify({ pid: child.pid, ...record }) + '\n');
      if (record.type === 'response' && pending.has(record.id)) {
        const { resolve, reject, timer } = pending.get(record.id); clearTimeout(timer); pending.delete(record.id);
        if (record.success) resolve(record.data); else reject(new Error(record.error));
      }
    }
  });
  child.stderr.on('data', chunk => { stderr += chunk; });
  const request = data => new Promise((resolve, reject) => {
    const id = 'inbox-' + ++requestId;
    const timer = setTimeout(() => { pending.delete(id); reject(new Error('RPC timeout: ' + data.type)); }, 150000);
    pending.set(id, { resolve, reject, timer });
    child.stdin.write(JSON.stringify({ id, ...data }) + '\n');
  });
  const stop = async () => {
    for (const { timer, reject } of pending.values()) { clearTimeout(timer); reject(new Error('CLI stopping')); } pending.clear();
    if (child.exitCode === null && child.signalCode === null) child.stdin.end();
    const timer = setTimeout(() => child.kill('SIGTERM'), 10000);
    await exit; clearTimeout(timer);
    fs.appendFileSync(path.join(out, 'stderr.log'), stderr);
  };
  current = { child, request, records, stop };
  const state = await request({ type: 'get_state' });
  sessionFile = state.sessionFile; rootId = 'session:' + state.sessionId;
  assert.ok(sessionFile, 'CLI must use a persisted session');
  return current;
};
const prompt = async text => {
  const before = current.records.filter(record => record.type === 'agent_settled').length;
  await current.request({ type: 'prompt', message: text });
  await waitFor(() => current.records.filter(record => record.type === 'agent_settled').length > before, 150000);
  await waitFor(() => fs.existsSync(sessionFile));
};
const snapshot = (phase, baseline) => {
  const value = { phase, inboxFrames: inboxes().length, summaries: summaries().length, extraInferences: inferCount() - baseline, wakeEvents: wakes().length };
  phases.push(value); console.log(JSON.stringify(value)); return value;
};
let failure;
try {
  await launch();
  await prompt('INBOX_SEED');
  await waitFor(() => entries().filter(entry => entry.type === 'custom_message' && entry.customType === 'pi-fabric-agent-message')
    .flatMap(entry => entry.details?.items ?? [entry.details]).filter(item => item?.data?.deliveryId?.startsWith('CLI-D')).length === 40, 10000);
  await current.request({ type: 'prompt', message: '/inbox-proof-pad' });
  const seeded = entries().filter(entry => entry.type === 'custom_message' && entry.customType === 'pi-fabric-agent-message')
    .flatMap(entry => entry.details?.items ?? [entry.details]).filter(item => item?.data?.deliveryId?.startsWith('CLI-D'));
  assert.equal(seeded.length, 40);
  const nativeIndex = entries().findIndex(entry => entry.type === 'custom_message' && entry.customType === 'pi-fabric-agent-message');
  assert.ok(entries().length - nativeIndex > 500, 'native receipt must lie beyond recent-entry window');
  const sender = seeded[0].from;
  assert.ok(seeded.every(item => item.from.id === sender.id));
  fs.writeFileSync(path.join(out, 'native-deliveries.json'), JSON.stringify(seeded, null, 2));
  await current.stop(); current = undefined;
  // Deterministic historical backlog fixture, appended ONLY while Main is stopped.
  // Native session receipts, inbox cursor, capabilities and delivery are never fabricated.
  let sequence = mesh.latestSequence();
  const appendHistorical = (data, ageMs, text) => {
    const event = { id: randomUUID(), sequence: ++sequence, topic: 'fleet.work.inbox.cli', kind: 'handoff', from: sender, to: rootId,
      data, text, createdAt: Date.now() - ageMs };
    fs.appendFileSync(path.join(meshRoot, 'events.jsonl'), JSON.stringify(event) + '\n');
  };
  for (const item of seeded) {
    appendHistorical({ ref: 'ticket:shared', deliveryId: item.data.deliveryId }, 120000, 'TRUE_DELIVERY_SHADOW');
    appendHistorical({ ref: 'ticket:shared', messageId: item.id }, 120000, 'TRUE_MESSAGE_SHADOW');
  }
  for (let index = 0; index < 45; index++) appendHistorical({ key: 'STALE-' + index }, 20 * 60 * 60_000, 'STALE_MUST_NOT_INJECT');
  fs.writeFileSync(path.join(meshRoot, 'sequence'), String(sequence));
  const baseline = inferCount();
  await launch(true);
  const host = readLines(hostFile).at(-1);
  assert.equal(host.mode, 'rpc'); assert.equal(host.testCapabilityOverride, false);
  assert.equal(host.capabilities?.triggeredMessageQueuesBehindPreflight, true);
  assert.equal(host.capabilities?.promptPendingVisible, true); assert.equal(host.isPromptPending, 'function');
  console.log('Actual host capabilities: ' + JSON.stringify(host));
  await waitFor(() => summaries().length === 1, 90000); await sleep(500);
  const stale = snapshot('reloaded idle backlog', baseline);
  assert.deepEqual(stale, { phase: 'reloaded idle backlog', inboxFrames: 0, summaries: 1, extraInferences: 0, wakeEvents: 0 });
  assert.equal(summaries()[0].content, 'Fabric inbox: skipped 45 addressed shadows older than 7200000 ms; no stale work injected.');
  console.log(summaries()[0].content);
  await prompt('INBOX_CHECK');
  const checked = snapshot('explicit check turn', baseline);
  assert.equal(checked.inboxFrames, 0); assert.equal(checked.summaries, 1); assert.equal(checked.extraInferences, 1); assert.equal(checked.wakeEvents, 0);
  await current.stop(); current = undefined;
  const reloadBaseline = inferCount();
  await launch(true); await sleep(1000);
  const reload = snapshot('second persisted reload', reloadBaseline);
  assert.equal(reload.inboxFrames, 0); assert.equal(reload.summaries, 1); assert.equal(reload.extraInferences, 0); assert.equal(reload.wakeEvents, 0);
  // Real public MeshStore publication; normal 60-second steer grace and actual idle wake.
  const fresh = await mesh.publish({ topic: 'fleet.work.inbox.cli', kind: 'handoff', from: sender, to: rootId,
    data: { ref: 'ticket:shared', deliveryId: 'CLI-NEW-DISTINCT' }, text: 'FRESH_UNSEEN_WORK' });
  await waitFor(() => inboxes().length === 1 && inferCount() === reloadBaseline + 1, 90000);
  await waitFor(() => current.records.some(record => record.type === 'agent_settled'), 10000);
  const delivered = snapshot('fresh distinct delivery on shared ref', reloadBaseline);
  assert.equal(delivered.inboxFrames, 1); assert.equal(delivered.summaries, 1); assert.equal(delivered.extraInferences, 1); assert.equal(delivered.wakeEvents, 1);
  assert.deepEqual(inboxes()[0].details.ids, [fresh.id]); assert.ok(inboxes()[0].content.includes('FRESH_UNSEEN_WORK'));
  assert.ok(readLines(inferenceFile).at(-1).inbox.some(content => content.includes('FRESH_UNSEEN_WORK')), 'fresh event must enter actual model context');
  await mesh.publish({ topic: 'fleet.work.inbox.cli', kind: 'handoff', from: sender, to: rootId,
    data: { ref: 'ticket:shared', deliveryId: 'CLI-NEW-DISTINCT' }, text: 'FRESH_TRUE_SHADOW' });
  await sleep(62000);
  const shadow = snapshot('fresh true shadow after grace', reloadBaseline);
  assert.deepEqual(shadow, { phase: 'fresh true shadow after grace', inboxFrames: 1, summaries: 1, extraInferences: 1, wakeEvents: 1 });
  assert.ok(!JSON.stringify(inboxes()).includes('TRUE_DELIVERY_SHADOW')); assert.ok(!JSON.stringify(inboxes()).includes('STALE_MUST_NOT_INJECT'));
  console.log('PASS: 40 actual native follow-ups, 80 true shadows, 600 later native history entries, 45 stale events; zero reinjection; one passive summary; fresh shared-ref delivery once; true shadow suppressed.');
} catch (error) { failure = error; console.error(error); }
finally {
  if (current) await current.stop();
  if (sessionFile && fs.existsSync(sessionFile)) fs.copyFileSync(sessionFile, path.join(out, 'session.jsonl'));
  for (const name of ['mesh', 'profile']) if (fs.existsSync(path.join(scratch, name))) fs.cpSync(path.join(scratch, name), path.join(out, name), { recursive: true });
  fs.writeFileSync(path.join(out, 'summary.json'), JSON.stringify({ passed: !failure, phases, exits, failure: failure?.message }, null, 2));
  fs.rmSync(scratch, { recursive: true, force: true });
}
assert.ok(exits.every(exit => exit.code === 0 && exit.signal === null), JSON.stringify(exits));
if (failure) process.exitCode = 1;
