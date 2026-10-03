import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import assert from 'node:assert/strict';
import { fileURLToPath } from 'node:url';
import { RpcClient } from '@earendil-works/pi-coding-agent';

// Run after bun run build. Only inference is synthetic; CLI, extension and workers are real.
const lane = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const candidate = path.resolve(process.argv[2] ?? 'dist/index.js');
const out = path.resolve(process.argv[3] ?? process.env.TASK_OUT ?? (() => { throw new Error('Supply a retained output directory'); })());
fs.mkdirSync(out, { recursive: true });
const scratch = fs.mkdtempSync(path.join(os.tmpdir(), 'astra-cli-'));
const profile = path.join(scratch, 'profile');
const cwd = path.join(scratch, 'workspace');
const home = path.join(scratch, 'home');
for (const directory of [profile, cwd, home]) fs.mkdirSync(directory);
const cli = fileURLToPath(new URL('./cli.js', import.meta.resolve('@earendil-works/pi-coding-agent')));
const fixture = path.join(lane, 'tests/fixtures/astra-model-reason-cli-provider.ts');
const receiptsFile = path.join(out, 'native-state-receipts.jsonl');
const eventsFile = path.join(out, 'events.jsonl');
fs.writeFileSync(receiptsFile, ''); fs.writeFileSync(eventsFile, '');
fs.writeFileSync(path.join(profile, 'settings.json'), JSON.stringify({ extensions: [fixture], compaction: { enabled: false }, retry: { enabled: false } }));
const config = { fullCodeMode: true,
  agents: { model: 'cliproxyapi/gpt-6.1-sol', thinking: 'max', deniedModels: [], transport: 'process', maxDepth: 2, maxConcurrent: 2, maxPerExecution: 8, budgetUsd: 0, timeoutMs: 45000, retainRuns: true },
  prewalk: { enabled: false }, approvals: { read: 'allow', write: 'allow', exec: 'allow', agent: 'allow' }, mesh: { enabled: true, persist: false },
  executor: { timeoutMs: 120000, kernel: 'typescript' },
};
fs.writeFileSync(path.join(profile, 'fabric.json'), JSON.stringify(config));
fs.writeFileSync(path.join(out, 'fabric-config.json'), JSON.stringify(config, null, 2));
const env = { HOME: home, PI_CODING_AGENT_DIR: profile, PI_FABRIC_PI_BINARY: cli, PI_FABRIC_RUN_ROOT: path.join(scratch, 'runs'), ASTRA_PROBE_RECEIPTS: receiptsFile, PI_OFFLINE: '1' };
const args = ['--no-session', '--no-skills', '--no-prompt-templates', '--no-context-files', '--offline', '-e', candidate, '--thinking', 'max'];
const command = { executable: 'node', argv: [cli, '--mode', 'rpc', '--provider', 'cliproxyapi', '--model', 'gpt-6.1-sol', ...args], env, cwd,
  prompt: { type: 'prompt', message: 'ASTRA_CLI_MAIN' }, note: 'Isolated empty HOME/profile. Keyless faux inference only; actual Pi CLI, compiled Fabric extension and real workers. No network or credentials.' };
fs.writeFileSync(path.join(out, 'command.json'), JSON.stringify(command, null, 2));
console.log('REAL PI CLI COMMAND AND RPC INPUT\n' + JSON.stringify(command, null, 2));
const client = new RpcClient({ cliPath: cli, cwd, env, provider: 'cliproxyapi', model: 'gpt-6.1-sol', args });
const events = [];
client.onEvent(event => { events.push(event); fs.appendFileSync(eventsFile, JSON.stringify(event) + '\n'); });
let failure;
try {
  await client.start();
  await client.promptAndWait('ASTRA_CLI_MAIN', undefined, 150000);
  const toolEnd = events.find(event => event.type === 'tool_execution_end' && event.toolName === 'fabric_exec');
  fs.writeFileSync(path.join(out, 'public-tool-result.json'), JSON.stringify(toolEnd ?? null, null, 2));
  assert.ok(toolEnd, 'public Fabric tool result missing');
  assert.equal(toolEnd.isError, false, JSON.stringify(toolEnd));
  assert.equal(toolEnd.result.details.success, true, JSON.stringify(toolEnd));
  const value = JSON.parse(toolEnd.result.content.filter(part => part.type === 'text').map(part => part.text).join('\n'));
  fs.writeFileSync(path.join(out, 'guest-value.json'), JSON.stringify(value, null, 2));
  const refusal = 'named passes use cliproxyapi/gpt-6.1-sol thinking max; otherwise omit model (role default)';
  assert.deepEqual(value.refusals, ['run', 'spawn', 'create'].map(action => ({ action, message: refusal })));
  assert.deepEqual(value.before, value.after, 'refused admission created a task');
  assert.deepEqual(value.beforeActors, value.afterActors, 'refused admission created an actor');
  console.log('PUBLIC REFUSAL OUTPUT\n' + JSON.stringify(value.refusals, null, 2));
  assert.equal(value.taskResult.status, 'completed');
  assert.ok(value.activation.runId, 'actor activation did not return a run');
  const statuses = [];
  const collect = directory => {
    if (!fs.existsSync(directory)) return;
    for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
      const file = path.join(directory, entry.name);
      if (entry.isDirectory()) collect(file);
      else if (entry.name === 'status.json') statuses.push({ file, record: JSON.parse(fs.readFileSync(file, 'utf8')) });
    }
  };
  collect(path.join(scratch, 'runs'));
  assert.equal(statuses.length, 2, 'unexpected/refused worker was created');
  const receipts = fs.readFileSync(receiptsFile, 'utf8').trim().split('\n').filter(Boolean).map(line => JSON.parse(line));
  for (const [kind, id] of [['task', value.taskResult.id], ['actor activation', value.activation.runId]]) {
    const saved = statuses.find(status => status.record.id === id);
    assert.ok(saved, 'persisted run missing for ' + kind);
    const record = saved.record;
    assert.equal(record.modelReason, value.modelReason, 'persisted reason changed for ' + kind);
    assert.equal(record.model, 'cliproxyapi/gpt-6-astra');
    assert.equal(record.status, 'completed');
    assert.equal(record.exitCode, 0);
    assert.ok(!record.cleanupPending);
    const receipt = receipts.find(receipt => receipt.runId === id);
    assert.ok(receipt, 'real worker receipt missing for ' + kind);
    assert.equal(receipt.model, record.model);
    assert.equal(Boolean(receipt.actorId), kind === 'actor activation');
    const frames = fs.readFileSync(path.join(path.dirname(saved.file), 'events.jsonl'), 'utf8').trim().split('\n').map(line => JSON.parse(line));
    const state = frames.filter(frame => frame.type === 'response' && frame.command === 'get_state').at(-1);
    assert.ok(state, 'native Pi get_state missing for ' + kind);
    assert.equal(state.data.model.provider + '/' + state.data.model.id, record.model);
    console.log('PERSISTED ' + kind.toUpperCase() + ' READBACK (' + saved.file + ')\n' + JSON.stringify(record, null, 2));
    console.log('NATIVE WORKER RECEIPT\n' + JSON.stringify(receipt, null, 2));
  }
  fs.writeFileSync(path.join(out, 'summary.json'), JSON.stringify({ passed: true, refusals: value.refusals, modelReason: value.modelReason, taskId: value.taskResult.id, actorRunId: value.activation.runId, persistedRuns: statuses.map(status => status.record), noRefusedWorker: true }, null, 2));
  console.log('PASS: exact public refusal for run/spawn/create; real task and actor activation persisted modelReason verbatim, both native Pi workers completed Astra/exit 0.');
} catch (error) { failure = error; console.error(error); }
finally {
  // Await real CLI shutdown, which owns worker teardown; leave no processes running.
  if (client.process) {
    const child = client.process;
    const exited = child.exitCode !== null || child.signalCode !== null ? Promise.resolve() : new Promise(resolve => child.once('exit', resolve));
    child.stdin.end();
    const timer = setTimeout(() => child.kill('SIGTERM'), 10000);
    await exited; clearTimeout(timer);
  }
  fs.writeFileSync(path.join(out, 'stderr.log'), client.getStderr());
  const runs = path.join(scratch, 'runs');
  if (fs.existsSync(runs)) fs.cpSync(runs, path.join(out, 'runs'), { recursive: true });
  fs.rmSync(scratch, { recursive: true, force: true });
}
if (failure) process.exitCode = 1;
