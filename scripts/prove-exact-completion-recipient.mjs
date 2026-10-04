// Offline native Pi proof for smarty-dev#3178. Build first, then:
// nice -n 19 node scripts/prove-exact-completion-recipient.mjs PI_CLI TMPDIR OUTPUT
// Only this fixture's launch-recorded, start-time-verified PIDs may be signalled.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { launchLog, same, stopAllOwned } from '../tests/helpers/owned-processes.ts';

const [cli, tmpdir, output] = process.argv.slice(2);
assert(cli && tmpdir && output, 'PI_CLI TMPDIR OUTPUT required');
assert.equal(process.platform, 'linux', 'SIGSTOP/SIGCONT proof is Linux-only');
assert.equal(os.getPriority(0), 19, 'run the proof at nice -n 19');
const repo = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const scratch = fs.mkdtempSync(path.join(path.resolve(tmpdir), 'exact-recipient-'));
const out = path.resolve(output); fs.mkdirSync(out, { recursive: true });
const record = value => fs.appendFileSync(path.join(out, 'proof.jsonl'), JSON.stringify({ at: Date.now(), ...value }) + '\n');
const launches = launchLog(scratch);
const home = path.join(scratch, 'home'), agentDir = path.join(home, '.pi', 'agent');
const extensions = path.join(agentDir, 'extensions'); fs.mkdirSync(extensions, { recursive: true });
const meshRoot = path.join(scratch, 'mesh'), runRoot = path.join(scratch, 'runs');
const gate = path.join(scratch, 'release'), childReady = path.join(scratch, 'child-ready.json');
for (const lane of ['A', 'B']) fs.writeFileSync(path.join(scratch, `queue-${lane}.json`), '[]');
fs.writeFileSync(path.join(agentDir, 'fabric.json'), JSON.stringify({
  autoReload: false, fullCodeMode: true, mesh: { enabled: true, announce: true, root: meshRoot, actorPollMs: 50 },
  agents: { transport: 'process', nice: 19, sessionExport: false, retainRuns: true, notifyOnComplete: true },
  records: { enabled: false }, mcp: { enabled: false }, memory: { enabled: false }, jev: { enabled: false },
  prewalk: { enabled: false, alwaysRearm: false }, ui: { enabled: false }, executor: { timeoutMs: 60000 },
}));
fs.writeFileSync(path.join(agentDir, 'settings.json'), JSON.stringify({ retry: { enabled: false }, compaction: { enabled: false }, defaultProjectTrust: 'approve' }));
const provider = path.join(extensions, 'recipient-proof.ts');
fs.writeFileSync(provider, `
import fs from 'node:fs';
import os from 'node:os';
import { fauxProvider, fauxAssistantMessage, fauxToolCall } from ${JSON.stringify(path.join(repo, 'node_modules/@earendil-works/pi-ai/dist/index.js'))};
export default function(pi) {
  const faux = fauxProvider({ provider: 'recipient-proof', models: [{ id: 'offline' }], tokensPerSecond: 100000 });
  const respond = async (context, options) => {
    const run = process.env.PI_FABRIC_AGENT_RUN_DIR;
    if (run) {
      fs.writeFileSync(${JSON.stringify(childReady)}, JSON.stringify({ pid: process.pid, run, nice: os.getPriority(0) }));
      while (!fs.existsSync(${JSON.stringify(gate)})) {
        if (options?.signal?.aborted) throw new Error('proof child aborted');
        await new Promise(resolve => setTimeout(resolve, 25));
      }
      return fauxAssistantMessage('EXACT_RECIPIENT_RESULT_3178');
    }
    const queue = ${JSON.stringify(scratch)} + '/queue-' + process.env.PROOF_LANE + '.json';
    const specs = JSON.parse(fs.readFileSync(queue, 'utf8')); const spec = specs.shift();
    fs.writeFileSync(queue, JSON.stringify(specs));
    return spec ? fauxAssistantMessage([fauxToolCall('fabric_exec', { code: spec.code, resultFormat: 'json' }, { id: spec.id })]) : fauxAssistantMessage('proof tick');
  };
  faux.setResponses(Array.from({ length: 200 }, () => respond)); pi.registerProvider(faux.provider);
  pi.on('session_start', (_event, ctx) => {
    if (!process.env.PI_FABRIC_AGENT_RUN_DIR) fs.writeFileSync(${JSON.stringify(scratch)} + '/ready-' + process.env.PROOF_LANE + '.json', JSON.stringify({ pid: process.pid, mode: ctx.mode, nice: os.getPriority(0), sessionId: ctx.sessionManager.getSessionId() }));
  });
}
`);
fs.copyFileSync(provider, path.join(out, 'offline-provider.ts'));
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const children = [];
const wait = async (predicate, timeout = 60000) => {
  const deadline = Date.now() + timeout;
  while (!predicate()) { assert(Date.now() < deadline, 'proof observation timed out'); await sleep(25); }
};
function open(lane) {
  const args = [path.resolve(cli), '--mode', 'rpc', '--offline', '--name', 'main', '--no-extensions', '--no-skills', '--no-context-files', '--no-prompt-templates', '--no-themes', '--approve', '-e', path.join(repo, 'dist/index.js'), '-e', provider, '--provider', 'recipient-proof', '--model', 'offline', '--thinking', 'off', '--tools', 'fabric_exec', '--session-dir', path.join(scratch, 'sessions')];
  const env = { PATH: process.env.PATH, HOME: home, TMPDIR: scratch, PI_OFFLINE: '1', PI_CODING_AGENT_DIR: agentDir,
    PI_FABRIC_PI_BINARY: path.resolve(cli), PI_FABRIC_RUN_ROOT: runRoot, PI_FABRIC_MESH_ROOT: meshRoot,
    PI_FABRIC_PROJECT_ROOT: scratch, PI_FABRIC_ROLE: 'project-agent', PROOF_LANE: lane, ...launches.env };
  record({ type: 'launch', lane, executable: process.execPath, args, cwd: scratch, env });
  const child = spawn(process.execPath, args, { cwd: scratch, env, stdio: ['pipe', 'pipe', 'pipe'] });
  let buffer = '', serial = 0, exited = false;
  const events = [], pending = new Map();
  const exit = new Promise(resolve => child.once('close', (code, signal) => { exited = true; resolve({ code, signal }); }));
  child.stdout.on('data', data => {
    buffer += data.toString(); let end;
    while ((end = buffer.indexOf('\n')) >= 0) {
      const line = buffer.slice(0, end); buffer = buffer.slice(end + 1); if (!line.trim()) continue;
      let event; try { event = JSON.parse(line); } catch { record({ type: 'non-json-output', lane, line }); continue; }
      events.push(event); record({ type: 'rpc', lane, event });
      if (event.type === 'response') { const callback = pending.get(event.id); pending.delete(event.id); event.success ? callback?.resolve(event.data) : callback?.reject(new Error(event.error)); }
    }
  });
  child.stderr.on('data', data => record({ type: 'stderr', lane, text: data.toString() }));
  const request = value => {
    assert(!exited, `${lane} exited`);
    return new Promise((resolve, reject) => {
      const id = `${lane}-rpc-${++serial}`; pending.set(id, { resolve, reject });
      record({ type: 'request', lane, ...value, id }); child.stdin.write(JSON.stringify({ ...value, id }) + '\n');
      const timer = setTimeout(() => { if (pending.delete(id)) reject(new Error(`${lane} request ${value.type} timed out`)); }, 60000); timer.unref();
    });
  };
  const guest = async code => {
    const before = events.length, id = `${lane}-guest-${++serial}`;
    fs.writeFileSync(path.join(scratch, `queue-${lane}.json`), JSON.stringify([{ id, code }]));
    await request({ type: 'prompt', message: `Execute ${id}` });
    await wait(() => { assert(!exited, `${lane} exited during guest`); return events.slice(before).some(event => event.type === 'agent_settled'); });
    const event = events.slice(before).find(event => event.type === 'tool_execution_end' && event.toolCallId === id);
    assert(event && !event.isError, JSON.stringify(event ?? events.slice(before)));
    return JSON.parse(event.result.content.filter(item => item.type === 'text').map(item => item.text).join('\n'));
  };
  const tick = async () => {
    const before = events.length;
    await request({ type: 'prompt', message: 'Native lifecycle tick; no tool requested.' });
    await wait(() => { assert(!exited, `${lane} exited during tick`); return events.slice(before).some(event => event.type === 'agent_settled'); });
  };
  const value = { lane, child, events, request, guest, tick, exit, get exited() { return exited; } }; children.push(value); return value;
}
const completionBodies = (messages, id) => messages.filter(message => message.role === 'custom' && message.customType === 'pi-fabric-agent-complete' && JSON.stringify(message).includes(id));
const contents = async client => (await client.request({ type: 'get_messages' })).messages;
let a, b, stopped = false, handle, failure, summary, stoppedForMs;
try {
  a = open('A'); await wait(() => { assert(!a.exited, 'A exited before ready'); return fs.existsSync(path.join(scratch, 'ready-A.json')); });
  const rootA = await a.guest('return await agents.self();');
  b = open('B'); await wait(() => { assert(!b.exited, 'B exited before ready'); return fs.existsSync(path.join(scratch, 'ready-B.json')); });
  const rootB = await b.guest('return await agents.self();');
  for (const key of ['cwd', 'name', 'role']) assert.equal(rootA[key], rootB[key], `same ${key}`);
  assert.equal(rootA.name, 'main'); assert.equal(rootA.role, 'project-agent'); assert.notEqual(rootA.rootId, rootB.rootId);
  record({ type: 'roots', A: rootA, B: rootB });
  // A durable task has its own resident supervisor. It settles and journals while A is stopped,
  // unlike an ordinary worker attempt which still needs A's supervisor to settle after resume.
  handle = await a.guest("return await agents.spawn({ task: 'Complete the isolated exact-recipient proof task.', name: 'proof-task', transport: 'process', residency: 'durable', model: 'recipient-proof/offline', thinking: 'off', nice: 19, extensions: true });");
  assert(handle.id, JSON.stringify(handle)); await wait(() => fs.existsSync(childReady));
  const worker = JSON.parse(fs.readFileSync(childReady)); assert.equal(worker.nice, 19);
  const ownedA = launches.owned().find(value => value.pid === a.child.pid); assert(ownedA && same(ownedA), 'A PID/start ownership');
  const stoppedAt = Date.now(); process.kill(ownedA.pid, 'SIGSTOP'); stopped = true;
  record({ type: 'signal', lane: 'A', pid: ownedA.pid, started: ownedA.started, signal: 'SIGSTOP' });
  await sleep(20000);
  const membersDuringStop = await b.guest("return await agents.members({ scope: 'project', kinds: ['root'] });");
  record({ type: 'stale-owner-observation', stoppedForMs: Date.now() - stoppedAt, members: membersDuringStop });
  const members = Array.isArray(membersDuringStop) ? membersDuringStop : membersDuringStop.members;
  assert(Array.isArray(members), JSON.stringify(membersDuringStop));
  assert(!members.some(root => root.id === rootA.id && !root.stale), 'A must be absent or stale after 20 s');
  fs.writeFileSync(gate, 'release');
  const journals = () => {
    const directory = path.join(meshRoot, 'agent-completions');
    try { return fs.readdirSync(directory).filter(file => file.endsWith('.json')).map(file => ({ file: path.join(directory, file), value: JSON.parse(fs.readFileSync(path.join(directory, file))) })); } catch { return []; }
  };
  await wait(() => journals().some(entry => entry.value.result.id === handle.id));
  const entry = journals().find(entry => entry.value.result.id === handle.id);
  assert.equal(entry.value.recipient.rootId, rootA.rootId); assert.equal(entry.value.recipient.sessionId, rootA.sessionId);
  assert.equal(entry.value.result.status, 'completed'); assert.equal(entry.value.result.text, 'EXACT_RECIPIENT_RESULT_3178');
  fs.copyFileSync(entry.file, path.join(out, 'completion-before-resume.json'));
  fs.copyFileSync(path.join(worker.run, 'status.json'), path.join(out, 'native-child-status.json'));
  fs.copyFileSync(path.join(worker.run, 'session.jsonl'), path.join(out, 'native-child-session.jsonl'));
  record({ type: 'native-worker', ...worker });
  // Give B several real lifecycle/idle poll windows while A is STILL stopped.
  // This distinguishes non-delivery from merely resuming A before B could notice.
  for (let tick = 0; tick < 3; tick++) { await b.tick(); await sleep(250); }
  const observedByB = await b.guest(`return await agents.status({ id: ${JSON.stringify(handle.id)} });`);
  assert(!Object.hasOwn(observedByB, 'text'), JSON.stringify(observedByB));
  assert.equal(observedByB.completionDelivery.addressedTo, rootA.sessionId);
  assert.equal(completionBodies(await contents(b), handle.id).length, 0);
  record({ type: 'retained-while-stopped', resultId: handle.id, originalRecipient: entry.value.recipient, B: observedByB, BDeliveryCount: 0 });
  assert(same(ownedA), 'A PID/start still owned'); stoppedForMs = Date.now() - stoppedAt; process.kill(ownedA.pid, 'SIGCONT'); stopped = false;
  record({ type: 'signal', lane: 'A', pid: ownedA.pid, started: ownedA.started, signal: 'SIGCONT', stoppedForMs: Date.now() - stoppedAt });
  let deliveredA = [];
  const deadline = Date.now() + 20000;
  while (!(deliveredA = completionBodies(await contents(a), handle.id)).length) {
    assert(Date.now() < deadline, 'A did not receive its completion');
    await a.tick(); await sleep(200);
  }
  for (let tick = 0; tick < 3; tick++) { await a.tick(); await b.tick(); await sleep(250); }
  const messagesA = await contents(a), messagesB = await contents(b);
  assert.equal(completionBodies(messagesA, handle.id).length, 1); assert.equal(completionBodies(messagesB, handle.id).length, 0);
  assert(JSON.stringify(completionBodies(messagesA, handle.id)).includes('EXACT_RECIPIENT_RESULT_3178'));
  const receipts = path.join(meshRoot, 'agent-completions', 'receipts');
  const receipt = fs.readdirSync(receipts).map(file => JSON.parse(fs.readFileSync(path.join(receipts, file)))).find(value => value.id === handle.id);
  assert.equal(receipt.sessionId, rootA.sessionId);
  for (const client of [a, b]) {
    fs.writeFileSync(path.join(out, `${client.lane}-messages.json`), JSON.stringify(client === a ? messagesA : messagesB, null, 2));
    const state = await client.request({ type: 'get_state' });
    if (state.sessionFile) fs.copyFileSync(state.sessionFile, path.join(out, `${client.lane}-session.jsonl`));
  }
  summary = { result: 'PASS', sameCwdNameRole: true, rootA: rootA.rootId, rootB: rootB.rootId, resultId: handle.id,
    stoppedForMs, completedWhileAStopped: true, retainedExactRecipient: true, ADeliveryCount: 1, BDeliveryCount: 0, receipt };
  record({ type: 'assertions', ...summary });
} catch (error) { failure = error; record({ type: 'failure', error: String(error.stack ?? error) }); }
finally {
  fs.writeFileSync(gate, 'release');
  if (stopped && a) { const owner = launches.owned().find(value => value.pid === a.child.pid); if (owner && same(owner)) { process.kill(owner.pid, 'SIGCONT'); record({ type: 'cleanup-signal', pid: owner.pid, signal: 'SIGCONT' }); } }
  for (const client of children) if (!client.exited) client.child.stdin.end();
  for (const client of children) {
    const result = await Promise.race([client.exit, sleep(15000).then(() => null)]);
    record({ type: 'main-exit', lane: client.lane, result });
  }
  // Resident hosts normally close after their 30 s idle window; bounded identity-safe cleanup follows.
  const until = Date.now() + 35000;
  while (launches.owned().some(same) && Date.now() < until) await sleep(100);
  try { await stopAllOwned(launches.owned(), 5000, 5000); } catch (error) { failure ??= error; }
  const owned = launches.owned(), remaining = owned.filter(same);
  if (remaining.length) failure ??= new Error('owned proof processes still live: ' + remaining.map(value => value.pid).join(','));
  record({ type: 'cleanup', owned, remaining, scratchRemoved: true });
  if (fs.existsSync(launches.file)) fs.copyFileSync(launches.file, path.join(out, 'owned-processes.jsonl'));
  fs.rmSync(scratch, { recursive: true, force: true });
}
fs.writeFileSync(path.join(out, 'summary.json'), JSON.stringify({ ...summary, result: failure ? 'FAIL' : 'PASS', ...(failure ? { error: String(failure.stack ?? failure) } : {}) }, null, 2) + '\n');
if (failure) throw failure;
console.log(JSON.stringify(summary, null, 2));
