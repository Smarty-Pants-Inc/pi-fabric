// Real owning-Main CLI proof. Inference alone is keyless/offline; no remote or credential access.
// Build first: PI_FABRIC_PROBE_PI=/absolute/pi/cli.js node scripts/probe-resident-reset-cli.mjs dist/index.js $TASK_OUT
// Append --cancel to prove prompt lifecycle control and reset cancellation without external release.
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import assert from 'node:assert/strict';
import { spawn, execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { fileURLToPath } from 'node:url';
const lane = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const candidate = path.resolve(process.argv[2] ?? 'dist/index.js');
const cancellation = process.argv[4] === '--cancel';
const out = path.resolve(process.argv[3] ?? process.env.TASK_OUT ?? (() => { throw new Error('Retained output required'); })());
const cli = process.env.PI_FABRIC_PROBE_PI ?? fileURLToPath(new URL('./cli.js', import.meta.resolve('@earendil-works/pi-coding-agent')));
fs.mkdirSync(out, { recursive: true });
const scratch = fs.mkdtempSync(path.join(os.tmpdir(), 'resident-reset-cli-'));
const cwd = path.join(scratch, 'workspace'), profile = path.join(scratch, 'profile'), mesh = path.join(scratch, 'mesh');
for (const directory of [cwd, profile, path.join(scratch, 'home')]) fs.mkdirSync(directory);
const transcript = path.join(out, 'native-proof.jsonl'), release = path.join(scratch, 'release');
fs.writeFileSync(transcript, '');
const fixture = path.join(lane, 'tests/fixtures/resident-reset-cli-provider.ts');
fs.writeFileSync(path.join(profile, 'settings.json'), JSON.stringify({ extensions: [fixture], enableInstallTelemetry: false, compaction: { enabled: false }, retry: { enabled: false } }));
fs.writeFileSync(path.join(profile, 'fabric.json'), JSON.stringify({ fullCodeMode: true, prewalk: { enabled: false }, components: [],
  approvals: { read: 'allow', write: 'allow', exec: 'allow', agent: 'allow' }, executor: { kernel: 'typescript', timeoutMs: 120000 },
  agents: { model: 'resident-reset-proof/offline', thinking: 'off', transport: 'process', timeoutMs: 45000, budgetUsd: 0, maxConcurrent: 4, retainRuns: true },
  mesh: { enabled: true, persist: true, root: mesh, actorPollMs: 20 }, actors: { maxSessionBytes: 20971520 } }));
const env = { PATH: process.env.PATH, HOME: path.join(scratch, 'home'), TMPDIR: scratch, PI_CODING_AGENT_DIR: profile,
  PI_OFFLINE: '1', PI_FABRIC_PI_BINARY: cli, PI_FABRIC_MESH_ROOT: mesh, PI_FABRIC_RUN_ROOT: path.join(scratch, 'runs'),
  RESIDENT_RESET_TRANSCRIPT: transcript, RESIDENT_RESET_RELEASE: release, RESIDENT_RESET_CANCEL_PROOF: cancellation ? '1' : '0' };
const args = [cli, '--mode', 'rpc', '--offline', '--no-skills', '--no-prompt-templates', '--no-themes', '--no-context-files', '--approve',
  '--provider', 'resident-reset-proof', '--model', 'offline', '--thinking', 'off', '-e', candidate, '--session-dir', path.join(scratch, 'sessions')];
const record = value => fs.appendFileSync(transcript, JSON.stringify(value) + '\n');
record({ type: 'proof_command', executable: process.execPath, args, cwd, env, candidateSha256: createHash('sha256').update(fs.readFileSync(candidate)).digest('hex'),
  piVersion: execFileSync(process.execPath, [cli, '--version'], { env, encoding: 'utf8' }).trim() });
const child = spawn(process.execPath, args, { cwd, env, stdio: ['pipe', 'pipe', 'pipe'] });
const exit = new Promise(resolve => child.once('exit', (code, signal) => resolve({ pid: child.pid, code, signal })));
const pending = new Map(), events = [];
let buffer = '', stderr = '', serial = 0, failure, proof;
child.stdout.setEncoding('utf8'); child.stderr.setEncoding('utf8');
child.stderr.on('data', text => { stderr += text; });
child.stdout.on('data', text => {
  buffer += text;
  for (;;) {
    const newline = buffer.indexOf('\n'); if (newline < 0) break;
    const line = buffer.slice(0, newline); buffer = buffer.slice(newline + 1);
    let event; try { event = JSON.parse(line); } catch { record({ type: 'non_json_stdout', line }); continue; }
    events.push(event); record({ type: 'main_rpc', event });
    if (event.type === 'response' && pending.has(event.id)) {
      const waiter = pending.get(event.id); pending.delete(event.id); clearTimeout(waiter.timer);
      if (event.success) waiter.resolve(event.data); else waiter.reject(new Error(event.error));
    }
  }
});
child.on('error', error => { for (const waiter of pending.values()) { clearTimeout(waiter.timer); waiter.reject(error); } pending.clear(); });
const request = frame => new Promise((resolve, reject) => {
  const id = `proof:${++serial}`;
  const timer = setTimeout(() => { pending.delete(id); reject(new Error('RPC deadline: ' + frame.type)); }, 90000);
  pending.set(id, { resolve, reject, timer }); child.stdin.write(JSON.stringify({ ...frame, id }) + '\n');
});
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const wait = async (predicate, ms = 60000) => {
  const deadline = Date.now() + ms;
  while (!predicate()) { if (Date.now() >= deadline) throw new Error('Native proof observation deadline'); await sleep(25); }
};
const files = directory => !fs.existsSync(directory) ? [] : fs.readdirSync(directory, { withFileTypes: true }).flatMap(entry => {
  const file = path.join(directory, entry.name); return entry.isDirectory() ? files(file) : [file];
});
const json = file => { try { return JSON.parse(fs.readFileSync(file, 'utf8')); } catch { return undefined; } };
const live = pid => {
  try { const stat = fs.readFileSync(`/proc/${pid}/stat`, 'utf8'); return !['Z', 'X'].includes(stat.slice(stat.lastIndexOf(')') + 2).split(' ')[0]); }
  catch { return false; }
};
const owners = () => files(mesh).filter(file => path.basename(file) === 'owner.json').map(file => ({ file, owner: json(file) })).filter(row => row.owner?.pid);
let ownedProcesses = [];
try {
  const state = await request({ type: 'get_state' });
  assert.equal(state.model.provider + '/' + state.model.id, 'resident-reset-proof/offline');
  const start = events.length;
  await request({ type: 'prompt', message: 'RESET_MAIN' });
  let committed;
  await wait(() => {
    const decisions = files(mesh).filter(file => file.includes('/decisions/') && file.endsWith('.json'));
    committed = decisions.map(file => ({ file, decision: json(file) })).find(row => row.decision?.operation === 'resetSession' && row.decision.state === 'committed');
    return Boolean(committed) || events.slice(start).some(event => event.type === 'agent_settled');
  });
  const currentOwners = owners();
  ownedProcesses = currentOwners.flatMap(({ owner }) => [owner.pid, owner.handover?.launcher?.pid]).filter(Number.isInteger);
  assert.ok(committed, 'Actual owning Main never committed its public reset request: ' + stderr);
  const queueFiles = files(cwd).filter(file => /queue.*\.json$/.test(path.basename(file)));
  const queues = queueFiles.map(file => ({ file, value: json(file) }));
  // Include mesh-root actor storage when the configured actor roots live there.
  for (const file of files(mesh).filter(file => /queue.*\.json$/.test(path.basename(file)))) queues.push({ file, value: json(file) });
  const cursors = files(mesh).filter(file => path.basename(file).startsWith('actor-mesh-cursor.json')).map(file => ({ file, value: json(file) }));
  if (!cancellation) assert.ok(cursors.some(row => row.file.endsWith('.project') && row.value?.last?.sequence > 0), 'Durable topic cursor missing at repair fence');
  record({ type: 'repair_fence_observed', committed, owners: currentOwners, queues, cursors });
  if (!cancellation) assert.match(JSON.stringify(queues), /QUEUED_EVENT/, 'Queued topic delivery is not durable at the repair fence');
  fs.writeFileSync(path.join(out, 'native-queued-at-fence.json'), JSON.stringify({ committed, owners: currentOwners, queues, cursors }, null, 2));
  // Cancellation proof must end the activation ONLY through the owning public stop.
  if (!cancellation) fs.writeFileSync(release, 'release held native activation\n');
  await wait(() => events.slice(start).some(event => event.type === 'agent_settled'), 90000);
  const tool = events.slice(start).find(event => event.type === 'tool_execution_end' && event.toolName === 'fabric_exec');
  assert.ok(tool, 'No public Fabric tool result'); assert.equal(tool.isError, false, JSON.stringify(tool));
  assert.equal(tool.result.details.success, true, JSON.stringify(tool.result));
  proof = JSON.parse(tool.result.content.filter(part => part.type === 'text').map(part => part.text).join('\n'));
  fs.writeFileSync(path.join(out, 'native-public-result.json'), JSON.stringify(proof, null, 2));
  if (cancellation) {
    assert.equal(proof.status.id, proof.actor.id);
    assert.equal(proof.updated.id, proof.other.id);
    assert.equal(proof.stop.status, 'stopped');
    assert.equal(proof.after.status, 'stopped');
    assert.ok(!proof.after.inFlightRun, 'Public stop returned before activation joined');
    assert.equal(proof.resetOutcome.error?.name, 'ActorSessionResetCancelledError');
    assert.match(proof.resetOutcome.error?.message ?? '', /ACTOR_SESSION_RESET_CANCELLED/);
    assert.match(proof.resetOutcome.error.message, new RegExp(committed.decision.requestId));
    assert.ok(proof.elapsedMs < 5000, 'Commands blocked behind held activation');
    assert.ok(!/stop[ -]first|idle run boundary/.test(proof.discovery.description));
    assert.ok(proof.messages.some(message => message.error?.includes('stopped while messages were queued')), 'Explicit stop did not cancel queued work');
    assert.ok(!proof.messages.some(message => message.data?.sessionReset), 'Cancelled reset still rotated the session');
    const native = fs.readFileSync(transcript, 'utf8').trim().split('\n').map(line => JSON.parse(line));
    const activations = native.filter(row => row.type === 'native_activation' && row.actorId === proof.actor.id);
    assert.equal(activations.length, 1, 'Stopped actor resumed queued activation');
    assert.ok(native.some(row => row.type === 'held_provider' && row.actorId === proof.actor.id), 'Native provider never held activation');
    ownedProcesses.push(...activations.map(row => row.pid));
    assert.ok(activations.every(row => !live(row.pid)), 'Public stop left native actor worker alive');
    assert.ok(!fs.existsSync(release), 'Cancellation proof externally released activation');
    const residency = path.dirname(path.dirname(committed.file));
    assert.deepEqual(json(committed.file), committed.decision, 'Cancellation changed immutable commit fence');
    assert.ok(fs.existsSync(path.join(residency, 'acknowledgements', committed.decision.requestId + '.json')), 'Reset terminal response not acknowledged');
    assert.deepEqual(fs.readdirSync(path.join(residency, 'processing')), []);
    assert.deepEqual(fs.readdirSync(path.join(residency, 'responses')), []);
    assert.equal(files(cwd).concat(files(mesh)).filter(file => path.basename(file).endsWith('.bak')).length, 0);
    record({ type: 'proof_assertions', passed: true, owningMain: proof.actor.rootId, actorId: proof.actor.id,
      promptStatus: true, unrelatedCommand: true, publicStopJoinedWorker: true, resetCancelledWithoutRotation: true,
      noExternalRelease: true, elapsedMs: proof.elapsedMs, nativeProcesses: activations.map(row => row.pid) });
    console.log('PASS: real owning Pi Main -> built Fabric -> resident; public stop cancels/joins held native activation, reset acknowledged as cancelled, status and unrelated command promptly served.');
  } else {
    assert.ok(proof.before.queued > 0);
    for (const value of [proof.before, proof.reset, proof.after]) {
      for (const field of ['id', 'name', 'scope', 'rootId', 'ownerSessionId', 'project', 'runner', 'kernel', 'pythonRuntime', 'residency', 'instructionsDigest', 'instructionsLength', 'model', 'thinking', 'binding', 'projectDefaults', 'topics', 'events', 'tools', 'delivery', 'triggerTurn', 'responseMode', 'coalesce', 'coalesceKey', 'activationFilter', 'timeoutMs', 'nice', 'extensions', 'inferenceContext', 'requirements', 'createdAt', 'sessionFile', 'logDir']) {
        assert.deepEqual(value[field], proof.actor[field], `Repair changed ${field}`);
      }
      assert.notEqual(value.status, 'stopped');
    }
    for (const marker of ['HELD_EVENT', 'QUEUED_EVENT', 'NEW_EVENT']) assert.ok(proof.messages.some(message => message.direction === 'out' && message.text === 'HANDLED_' + marker), marker + ' not handled');
    assert.ok(!proof.messages.some(message => message.error?.includes('Dropped a queued event')));
    const native = fs.readFileSync(transcript, 'utf8').trim().split('\n').map(line => JSON.parse(line));
    const activations = native.filter(row => row.type === 'native_activation' && row.actorId === proof.actor.id);
    assert.equal(activations.length, 3, 'Exactly three real actor activations required');
    ownedProcesses.push(...activations.map(row => row.pid));
    const cursorContinuity = cursors.map(before => ({ before, after: json(before.file) }));
    for (const { before, after } of cursorContinuity) {
      assert.ok(after && after.cursor >= before.value.cursor, 'Reset rewound or removed the mailbox cursor');
      assert.ok((after.last?.sequence ?? 0) >= (before.value.last?.sequence ?? 0), 'Reset rewound the consumed-event sequence');
    }
    fs.writeFileSync(path.join(out, 'native-cursor-continuity.json'), JSON.stringify(cursorContinuity, null, 2));
    assert.match(activations[1].prompt, /QUEUED_EVENT/); assert.match(activations[2].prompt, /NEW_EVENT/);
    assert.notEqual(activations[0].sessionId, activations[1].sessionId); assert.equal(activations[1].sessionId, activations[2].sessionId);
    assert.ok(activations.every(row => row.model === 'resident-reset-proof/offline' && row.thinking === 'off'));
    const archiveFiles = files(cwd).concat(files(mesh)).filter(file => path.basename(file).endsWith('.bak'));
    assert.ok(archiveFiles.length > 0, 'Native session archive missing');
    const archived = archiveFiles.map(file => ({ file, text: fs.readFileSync(file, 'utf8') }));
    assert.ok(archived.some(row => row.text.includes('HELD_EVENT')));
    const fresh = fs.readFileSync(activations[2].sessionFile, 'utf8');
    assert.match(fresh, /QUEUED_EVENT/); assert.match(fresh, /NEW_EVENT/); assert.ok(!fresh.includes('HELD_EVENT'));
    fs.writeFileSync(path.join(out, 'native-fresh-session.jsonl'), fresh);
    fs.writeFileSync(path.join(out, 'native-archived-session.jsonl'), archived[0].text);
    record({ type: 'proof_assertions', passed: true, owningMain: proof.actor.rootId, actorId: proof.actor.id,
      retainedQueuedEventHandled: true, newEventHandled: true, sessionIds: activations.map(row => row.sessionId), nativeProcesses: activations.map(row => row.pid) });
    console.log('PASS: real owning Pi Main -> built Fabric -> native durable resident; queued + new topic events handled on fresh session under unchanged actor identity/config.');
  }
} catch (error) { failure = error; console.error(error); record({ type: 'proof_failure', error: String(error) }); }
finally {
  fs.writeFileSync(release, 'release for cleanup\n');
  const discovered = owners();
  ownedProcesses = [...new Set([...ownedProcesses, ...discovered.flatMap(({ owner }) => [owner.pid, owner.handover?.launcher?.pid]).filter(Number.isInteger)])];
  for (const { file, owner } of discovered) {
    const launcherLog = path.join(path.dirname(file), 'launcher.log');
    if (fs.existsSync(launcherLog)) {
      for (const line of fs.readFileSync(launcherLog, 'utf8').trim().split('\n')) {
        try { const row = JSON.parse(line); if (row.event === 'launcher-started' && Number.isInteger(row.pid)) ownedProcesses.push(row.pid); } catch {}
      }
    }
    const residentOut = path.join(out, 'native-resident-state');
    fs.cpSync(path.dirname(file), residentOut, { recursive: true });
  }
  ownedProcesses = [...new Set(ownedProcesses)];
  // All pids are read only from this isolated proof root; never stop any pre-existing actor.
  for (const pid of [...new Set(ownedProcesses)].reverse()) if (live(pid)) { try { process.kill(pid, 'SIGTERM'); } catch {} }
  await wait(() => ownedProcesses.every(pid => !live(pid)), 15000).catch(async error => {
    for (const pid of ownedProcesses) if (live(pid)) { try { process.kill(pid, 'SIGKILL'); } catch {} }
    await wait(() => ownedProcesses.every(pid => !live(pid)), 5000); failure ??= error;
  });
  if (child.exitCode === null && child.signalCode === null) child.stdin.end();
  const kill = setTimeout(() => child.kill('SIGTERM'), 10000);
  const mainExit = await exit; clearTimeout(kill);
  for (const waiter of pending.values()) { clearTimeout(waiter.timer); waiter.reject(new Error('Proof exited')); } pending.clear();
  fs.writeFileSync(path.join(out, 'native-stderr.log'), stderr);
  record({ type: 'proof_cleanup', mainExit, ownedProcesses, allResidentProcessesExited: ownedProcesses.every(pid => !live(pid)) });
  fs.writeFileSync(path.join(out, 'native-summary.json'), JSON.stringify({ passed: !failure, cancellation, candidate, cli, mainExit, ownedProcesses, allResidentProcessesExited: ownedProcesses.every(pid => !live(pid)), actorId: proof?.actor.id }, null, 2));
  fs.rmSync(scratch, { recursive: true, force: true });
}
if (failure) process.exitCode = 1;
