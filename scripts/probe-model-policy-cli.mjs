import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import assert from 'node:assert/strict';
import { fileURLToPath } from 'node:url';
import { RpcClient } from '@earendil-works/pi-coding-agent';
const lane = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const candidate = path.resolve(process.argv[2] ?? 'dist/index.js');
const out = path.resolve(process.argv[3] ?? process.env.TASK_OUT ?? (() => { throw new Error('Supply a retained output directory'); })());
fs.mkdirSync(out, { recursive: true });
const scratch = fs.mkdtempSync(path.join(os.tmpdir(), 'policy-cli-'));
const profile = path.join(scratch, 'profile');
const cwd = path.join(scratch, 'workspace');
fs.mkdirSync(profile, { recursive: true }); fs.mkdirSync(cwd);
const cli = fileURLToPath(new URL('./cli.js', import.meta.resolve('@earendil-works/pi-coding-agent')));
const fixture = path.join(lane, 'tests/fixtures/model-policy-cli-provider.ts');
const receipts = path.join(out, 'native-state-receipts.jsonl');
fs.writeFileSync(receipts, '');
fs.writeFileSync(path.join(out, 'events.jsonl'), '');
fs.writeFileSync(path.join(profile, 'settings.json'), JSON.stringify({ extensions: [fixture], compaction: { enabled: false }, retry: { enabled: false } }));
fs.writeFileSync(path.join(profile, 'fabric.json'), JSON.stringify({ fullCodeMode: true,
  agents: { model: 'policy-probe/denied', thinking: 'low', deniedModels: ['policy-probe/denied'], deniedModelReplacement: 'policy-probe/allowed', transport: 'process', maxDepth: 3, maxConcurrent: 4, maxPerExecution: 8, budgetUsd: 0, timeoutMs: 45000, retainRuns: true },
  prewalk: { enabled: false }, approvals: { read: 'allow', write: 'allow', exec: 'allow', agent: 'allow' }, mesh: { enabled: true, persist: false },
  executor: { timeoutMs: 120000, kernel: 'typescript' },
}));
const env = { PI_CODING_AGENT_DIR: profile, PI_FABRIC_PI_BINARY: cli, PI_FABRIC_RUN_ROOT: path.join(scratch, 'runs'), POLICY_PROBE_RECEIPTS: receipts, PI_OFFLINE: '1' };
const args = ['--no-session', '--no-skills', '--no-prompt-templates', '--no-context-files', '--offline', '-e', candidate, '--thinking', 'max'];
fs.writeFileSync(path.join(out, 'command.json'), JSON.stringify({ executable: 'node', argv: [cli, '--mode', 'rpc', '--provider', 'policy-probe', '--model', 'allowed', ...args], env, note: 'Clean environment; isolated empty HOME/profile; keyless in-process synthetic model; actual Pi CLI, Fabric tool, worker processes and native model admission. No external inference/network/credentials.' }, null, 2));
const client = new RpcClient({ cliPath: cli, cwd, env, provider: 'policy-probe', model: 'allowed', args });
const events = [];
client.onEvent(event => { events.push(event); fs.appendFileSync(path.join(out, 'events.jsonl'), JSON.stringify(event) + '\n'); });
let failure;
try {
  await client.start();
  const before = await client.getState();
  fs.writeFileSync(path.join(out, 'main-get-state-before.json'), JSON.stringify(before, null, 2));
  assert.equal(before.model.provider + '/' + before.model.id, 'policy-probe/allowed');
  assert.equal(before.thinkingLevel, 'max');
  await client.promptAndWait('POLICY_MAIN', undefined, 150000);
  const after = await client.getState();
  fs.writeFileSync(path.join(out, 'main-get-state-after.json'), JSON.stringify(after, null, 2));
  const toolEnd = events.find(event => event.type === 'tool_execution_end' && event.toolName === 'fabric_exec');
  fs.writeFileSync(path.join(out, 'public-tool-result.json'), JSON.stringify(toolEnd, null, 2));
  const native = fs.readFileSync(receipts, 'utf8').trim().split('\n').filter(Boolean).map(line => JSON.parse(line));
  assert.ok(toolEnd, 'public tool result missing');
  assert.equal(toolEnd.isError, false, JSON.stringify(toolEnd));
  const detail = toolEnd.result.details;
  assert.equal(detail.success, true, 'guest execution failed');
  const value = JSON.parse(toolEnd.result.content.filter(part => part.type === 'text').map(part => part.text).join('\n'));
  fs.writeFileSync(path.join(out, 'guest-value.json'), JSON.stringify(value, null, 2));
  assert.equal(value.refusals.length, 2);
  for (const refusal of value.refusals) { assert.equal(refusal.code, 'FABRIC_MODEL_DENIED'); assert.equal(refusal.name, 'FabricModelDeniedError'); }
  assert.deepEqual(value.before, value.afterDenied, 'denied admission created a task');
  assert.deepEqual(value.beforeActors, value.afterDeniedActors, 'denied admission created an actor');
  assert.equal(value.taskResult.status, 'completed', JSON.stringify(value.taskResult));
  assert.equal(value.parent.model, 'policy-probe/allowed'); assert.equal(value.parent.thinking, 'max');
  assert.equal(value.actor.model, 'policy-probe/allowed'); assert.equal(value.actor.thinking, 'max');
  assert.ok(native.some(receipt => receipt.depth === 1 && !receipt.actorId && receipt.prompt.includes('POLICY_TASK_PARENT')), 'task parent receipt missing');
  assert.ok(native.some(receipt => receipt.depth === 1 && receipt.actorId && receipt.prompt.includes('POLICY_ACTOR_PARENT')), 'actor parent receipt missing');
  for (const kind of ['TASK', 'ACTOR']) assert.ok(native.some(receipt => receipt.depth === 2 && receipt.prompt.includes('POLICY_LEAF_' + kind)), kind + ' child receipt missing');
  for (const receipt of native) {
    assert.equal(receipt.model, 'policy-probe/allowed'); assert.equal(receipt.thinking, 'max');
    assert.ok(!receipt.prompt.includes('DENIED_TASK'));
    if (receipt.runId === 'main') continue;
    const logs = [];
    const findLog = (directory) => {
      if (!fs.existsSync(directory)) return;
      for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
        const file = path.join(directory, entry.name);
        if (entry.isDirectory()) findLog(file);
        else if (entry.name === 'events.jsonl' && path.basename(directory) === receipt.runId) logs.push(file);
      }
    };
    findLog(path.join(scratch, 'runs'));
    assert.equal(logs.length, 1, 'missing worker log for ' + receipt.runId);
    const frames = fs.readFileSync(logs[0], 'utf8').trim().split('\n').map(line => JSON.parse(line));
    const state = frames.filter(frame => frame.type === 'response' && frame.command === 'get_state').at(-1);
    assert.equal(state.data.model.provider + '/' + state.data.model.id, receipt.model);
    assert.equal(state.data.thinkingLevel, receipt.thinking);
    const status = JSON.parse(fs.readFileSync(path.join(path.dirname(logs[0]), 'status.json'), 'utf8'));
    assert.equal(status.status, 'completed');
    assert.equal(status.exitCode, 0);
    assert.ok(!status.cleanupPending);
  }
  const statuses = [];
  const collectStatuses = (directory) => {
    for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
      const file = path.join(directory, entry.name);
      if (entry.isDirectory()) collectStatuses(file);
      else if (entry.name === 'status.json') statuses.push(JSON.parse(fs.readFileSync(file, 'utf8')));
    }
  };
  collectStatuses(path.join(scratch, 'runs'));
  assert.equal(statuses.length, 4, 'unexpected/denied worker created');
  assert.ok(statuses.every(status => status.model === 'policy-probe/allowed' && status.status === 'completed'));
  assert.equal(after.model.provider + '/' + after.model.id, 'policy-probe/allowed');
  assert.equal(after.thinkingLevel, 'max');
  fs.writeFileSync(path.join(out, 'summary.json'), JSON.stringify({ passed: true, nativeReceipts: native.length, deniedCalls: value.refusals, parent: value.parent, actor: value.actor, taskResultStatus: value.taskResult.status, noDeniedWorker: true, nativeModel: 'policy-probe/allowed', nativeEffort: 'max' }, null, 2));
  console.log('PASS: actual Pi CLI; both denied public guest calls retain code/no task; real task/actor parents and depth-2 children all admitted allowed/max.');
} catch (error) { failure = error; console.error(error); }
finally {
  // Stop gracefully via stdin and await actual process exit; no proof worker survives.
  if (client.process) {
    const child = client.process;
    const exited = child.exitCode !== null || child.signalCode !== null ? Promise.resolve() : new Promise(resolve => child.once('exit', resolve));
    child.stdin.end();
    const timer = setTimeout(() => child.kill('SIGTERM'), 10000);
    await exited; clearTimeout(timer);
  }
  fs.writeFileSync(path.join(out, 'stderr.log'), client.getStderr());
  for (const name of ['runs', 'profile']) {
    const from = path.join(scratch, name);
    if (fs.existsSync(from)) fs.cpSync(from, path.join(out, name), { recursive: true });
  }
  fs.rmSync(scratch, { recursive: true, force: true });
}
if (failure) process.exitCode = 1;
