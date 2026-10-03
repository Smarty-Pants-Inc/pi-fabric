// Opt-in native proof. Build first. Usage: nice -n 19 node scripts/probe-child-completion-cli.mjs OUTPUT
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import assert from 'node:assert/strict';
import { spawn, execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { createHash } from 'node:crypto';
const repo = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const out = path.resolve(process.argv[2] ?? process.env.TASK_OUT);
fs.mkdirSync(out, { recursive: true });
const scratch = fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), 'native-handoff-'));
const profile = path.join(scratch, 'profile'), cwd = path.join(scratch, 'workspace'), log = path.join(out, 'native-proof.jsonl');
for (const dir of [profile, cwd, path.join(scratch, 'home'), path.join(scratch, 'tmp')]) fs.mkdirSync(dir, { recursive: true });
fs.writeFileSync(log, '');
const record = value => fs.appendFileSync(log, JSON.stringify({ at: Date.now(), ...value }) + '\n');
const cli = fileURLToPath(new URL('./cli.js', import.meta.resolve('@earendil-works/pi-coding-agent')));
const candidate = path.join(repo, 'dist/index.js'), fixture = path.join(repo, 'tests/fixtures/child-completion-cli-provider.ts');
const head = execFileSync('git', ['rev-parse', 'HEAD'], { cwd: repo, encoding: 'utf8' }).trim();
const diffHash = createHash('sha256').update(execFileSync('git', ['diff', 'HEAD'], { cwd: repo })).digest('hex');
record({ type: 'revision', head, diffHash, candidate, sha256: createHash('sha256').update(fs.readFileSync(candidate)).digest('hex'), cli, piVersion: execFileSync(process.execPath, [cli, '--version'], { encoding: 'utf8' }).trim(), nice: os.getPriority(0), inferenceOnlySynthetic: true });
fs.writeFileSync(path.join(profile, 'settings.json'), JSON.stringify({ extensions: [fixture], compaction: { enabled: false }, retry: { enabled: false } }));
fs.writeFileSync(path.join(profile, 'fabric.json'), JSON.stringify({ fullCodeMode: true, executor: { kernel: 'typescript', timeoutMs: 120000, maxTimeoutMs: 120000, mainMaxTimeoutMs: 120000 },
  agents: { enabled: true, model: 'handoff-proof/offline', recursive: true, transport: 'process', maxDepth: 4, maxConcurrent: 6, maxPerExecution: 40, maxTokensPerChild: 0, nice: 19, timeoutMs: 120000, budgetUsd: 0, retainRuns: true },
  mesh: { enabled: true, persist: true, root: path.join(scratch, 'mesh'), actorPollMs: 20 }, residency: { enabled: false }, prewalk: { enabled: false, alwaysRearm: false },
  approvals: { read: 'allow', write: 'allow', exec: 'allow', agent: 'allow' }, mcp: { enabled: false }, memory: { enabled: false }, jev: { enabled: false }, records: { enabled: false } }));
const env = { PATH: process.env.PATH, HOME: path.join(scratch, 'home'), TMPDIR: path.join(scratch, 'tmp'), PI_CODING_AGENT_DIR: profile, PI_OFFLINE: '1',
  PI_FABRIC_PI_BINARY: cli, PI_FABRIC_RUN_ROOT: path.join(scratch, 'runs'), HANDOFF_PROOF_LOG: log, HANDOFF_PROOF_SCRATCH: scratch };
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const wait = async (predicate, label, ms = 90000) => { const deadline = Date.now() + ms; while (!predicate()) { assert(Date.now() < deadline, label); await sleep(25); } };
const rows = () => fs.readFileSync(log, 'utf8').trim().split('\n').filter(Boolean).map(line => JSON.parse(line));
const workers = []; let generation = 0;
function launch(sessionFile) {
  const args = [cli, '--mode', 'rpc', '--offline', '--no-skills', '--no-prompt-templates', '--no-context-files', '--no-themes', '--approve', '-e', candidate,
    '--provider', 'handoff-proof', '--model', 'offline', '--thinking', 'off', '--tools', 'fabric_exec', ...(sessionFile ? ['--session', sessionFile] : ['--session-dir', path.join(profile, 'sessions')])];
  record({ type: 'command', executable: process.execPath, args, cwd, env, generation: ++generation });
  const child = spawn(process.execPath, args, { cwd, env, stdio: ['pipe', 'pipe', 'pipe'] });
  const events = [], pending = new Map(); let buffer = '', seq = 0, stderr = '';
  const exit = new Promise(resolve => child.once('exit', (code, signal) => { record({ type: 'process-exit', generation, pid: child.pid, code, signal }); resolve({ code, signal }); }));
  child.stdout.setEncoding('utf8'); child.stdout.on('data', chunk => { buffer += chunk; let pos; while ((pos = buffer.indexOf('\n')) >= 0) {
    const line = buffer.slice(0, pos); buffer = buffer.slice(pos + 1); if (!line.trim()) continue;
    let event; try { event = JSON.parse(line); } catch { record({ type: 'non-json-output', line }); continue; }
    events.push(event); record({ type: 'rpc-event', pid: child.pid, event });
    if (event.type === 'response') { const request = pending.get(event.id); pending.delete(event.id); event.success ? request?.resolve(event.data) : request?.reject(new Error(event.error)); }
  } });
  child.stderr.on('data', data => { stderr += data.toString(); record({ type: 'stderr', pid: child.pid, text: data.toString() }); });
  const request = command => new Promise((resolve, reject) => { const id = 'proof-' + (++seq); const timer = setTimeout(() => { pending.delete(id); reject(new Error('RPC timeout ' + command.type + ': ' + stderr.slice(-2000))); }, 120000);
    pending.set(id, { resolve: value => { clearTimeout(timer); resolve(value); }, reject: error => { clearTimeout(timer); reject(error); } }); record({ type: 'rpc-command', pid: child.pid, command: { id, ...command } }); child.stdin.write(JSON.stringify({ id, ...command }) + '\n'); });
  const close = async () => { if (child.exitCode === null && child.signalCode === null) child.stdin.end(); const timer = setTimeout(() => child.kill('SIGKILL'), 15000); const result = await exit; clearTimeout(timer); assert.equal(result.code, 0, stderr); };
  const worker = { child, events, request, close }; workers.push(worker); return worker;
}
let main; const actors = [], cases = [];
const guest = async code => { const before = main.events.length; await main.request({ type: 'prompt', message: 'HANDOFF_MAIN_CODE:\n' + code });
  await wait(() => main.events.slice(before).some(e => e.type === 'agent_end'), 'Main tool turn did not settle', 120000);
  const result = main.events.slice(before).find(e => e.type === 'tool_execution_end' && e.toolName === 'fabric_exec');
  assert(result && !result.isError && result.result.details?.success, JSON.stringify(result));
  return JSON.parse(result.result.content.filter(p => p.type === 'text').map(p => p.text).join('\n')); };
try {
  main = launch(); await main.request({ type: 'get_state' });
  for (const mode of ['count', 'bytes']) {
    const actor = await guest(`const actor = await agents.create({ name: "native-handoff-${mode}", instructions: "HANDOFF_ACTOR_${mode}", model: "handoff-proof/offline", responseMode: "text", delivery: "mailbox", triggerTurn: false, transport: "process", validWhile: ({activation}) => activation.source !== "child-completion" }); await agents.tell({ id: actor.id, message: "BEGIN_HANDOFF:${mode}" }); return actor;`);
    actors.push(actor);
    const marker = path.join(scratch, mode + '-fenced.json'); await wait(() => fs.existsSync(marker), 'actor did not reach unpublished stop fence');
    const fenced = JSON.parse(fs.readFileSync(marker)); assert.equal(fenced.stopped.status, 'completed', 'guest stop did not observe terminal child');
    const status = await guest(`return await agents.status({ id: ${JSON.stringify(actor.id)} });`);
    assert(status.inFlightRun?.id, JSON.stringify(status));
    // Public guest stop of the spawning activation causes real native abort/shutdown.
    const stoppedActivation = await guest(`const result = await agents.stop({ id: ${JSON.stringify(status.inFlightRun.id)} }); return { id: result.id, status: result.status, sessionId: result.sessionId };`);
    assert.equal(stoppedActivation.status, "stopped");
    const native = rows().find(r => r.type === "native-session-start" && r.runId === status.inFlightRun.id);
    assert(native && native.mode === "rpc");
    const absent = pid => { try { process.kill(Number(pid), 0); return false; } catch (error) { if (error.code === "ESRCH") return true; throw error; } };
    await wait(() => absent(stoppedActivation.sessionId) && absent(native.pid), "activation native process did not exit");
    record({ type: "activation-exit-confirmed", mode, worker: stoppedActivation, nativePiPid: native.pid });
    const directory = path.join(path.dirname(actor.sessionFile), 'child-completions');
    const unread = fenced.children.slice(1);
    await wait(() => unread.every(c => fs.existsSync(path.join(directory, c.id + '.receipt'))), 'unread child outcomes were not handed off');
    assert(unread.every(c => fs.existsSync(path.join(directory, c.id + '.result.json'))), 'unread archive lost before replacement inference');
    const firstReceipt = JSON.parse(fs.readFileSync(path.join(directory, fenced.children[0].id + '.receipt'))); assert.equal(firstReceipt.kind, 'foreground');
    assert(!fs.existsSync(path.join(directory, fenced.children[0].id + '.result.json')), 'delivered stop value became unread');
    record({ type: 'case-cancelled', mode, actor, activationRun: status.inFlightRun.id, fenced, firstReceipt, fullArchives: fenced.children.length });
    const state = await main.request({ type: 'get_state' }); assert(state.sessionFile);
    await main.close(); main = launch(state.sessionFile); await main.request({ type: 'get_state' });
    const seen = new Set(), snapshots = [];
    for (let i = 0; seen.size < unread.length && i < unread.length; i++) {
      const before = rows().length;
      await guest(`return await agents.ask({ id: ${JSON.stringify(actor.id)}, message: "RECOVER_HANDOFF_${mode}_${i}" });`);
      const provider = rows().slice(before).filter(r => r.type === 'provider-call' && r.actorId === actor.id);
      const snapshot = provider.find(r => r.handoff.length)?.handoff ?? [];
      assert(snapshot.length > 0 && snapshot.length <= 16, JSON.stringify(provider));
      const bytes = provider.find(r => r.handoff.length).handoffBytes; assert(bytes <= 32768);
      for (const item of snapshot) { assert(!seen.has(item.id), 'replayed handoff context ' + item.id); seen.add(item.id); }
      await wait(() => unread.every(c => fs.existsSync(path.join(directory, c.id + '.result.json')) === !seen.has(c.id)), 'snapshot archive consumption did not match inference');
      snapshots.push({ ids: snapshot.map(item => item.id), bytes });
    }
    assert.equal(seen.size, unread.length);
    assert(!seen.has(fenced.stopped.id), "delivered terminal stop replayed after cancellation");
    const before = rows().length;
    await guest(`return await agents.ask({ id: ${JSON.stringify(actor.id)}, message: "UNRELATED_NO_REPLAY_${mode}" });`);
    assert(rows().slice(before).filter(r => r.type === 'provider-call' && r.actorId === actor.id).every(r => r.handoff.length === 0));
    const mainMessages = await main.request({ type: 'get_messages' });
    assert(!(mainMessages.messages ?? []).some(m => m.customType === 'pi-fabric-agent-complete' && (m.details?.ids ?? []).some(id => seen.has(id))), 'child outcome delivered to Main');
    const history = await guest(`return await agents.messages({ id: ${JSON.stringify(actor.id)}, limit: 200 });`);
    for (const id of seen) assert.equal(history.filter(m => m.id === id && m.direction === 'in').length, 1, 'actor mailbox duplicate/missing ' + id);
    const summary = { mode, children: fenced.children.length, unreadChildren: unread.length, actorId: actor.id, deliveredTerminalStop: fenced.stopped, snapshots, consumedExactlyOnce: [...seen], mainDelivery: 0, noReplayOnUnrelatedActivation: true };
    cases.push(summary); record({ type: 'case-pass', ...summary });
  }
  assert.equal(cases[0].snapshots[0].ids.length, 16, 'count bound not exercised');
  assert(cases[1].snapshots.length > 1, 'UTF-8 bound not exercised');
  record({ type: 'proof-pass', cases }); console.log(JSON.stringify({ passed: true, cases }, null, 2));
} finally {
  for (const worker of workers) await worker.close();
  for (const name of ['runs', 'mesh', 'profile']) if (fs.existsSync(path.join(scratch, name))) { fs.rmSync(path.join(out, "native-" + name), { recursive: true, force: true }); fs.cpSync(path.join(scratch, name), path.join(out, "native-" + name), { recursive: true }); }
  fs.rmSync(scratch, { recursive: true, force: true });
}
