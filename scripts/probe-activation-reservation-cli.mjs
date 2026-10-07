// Real Pi CLI proof for P0 activation reservations (smarty-dev#4440, pi-fabric#548).
// Build first, then: node scripts/probe-activation-reservation-cli.mjs dist/index.js $TASK_OUT/cli-probe
// Actual Pi CLI (RPC mode) -> built Fabric -> fabric_exec -> agents.setActivationFilter/actorStatus
// -> owning ActorManager. Inference is keyless/offline; no network or credentials.
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { RpcClient } from '@earendil-works/pi-coding-agent';
const lane = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const candidate = path.resolve(process.argv[2] ?? 'dist/index.js');
const out = path.resolve(process.argv[3] ?? process.env.TASK_OUT ?? (() => { throw new Error('Supply a retained output directory'); })());
fs.mkdirSync(out, { recursive: true });
const scratch = fs.mkdtempSync(path.join(os.tmpdir(), 'p0-reservation-cli-'));
const profile = path.join(scratch, 'profile');
const cwd = path.join(scratch, 'workspace');
fs.mkdirSync(profile, { recursive: true }); fs.mkdirSync(cwd);
const cli = fileURLToPath(new URL('./cli.js', import.meta.resolve('@earendil-works/pi-coding-agent')));
const fixture = path.join(lane, 'tests/fixtures/activation-reservation-cli-provider.ts');
fs.writeFileSync(path.join(out, 'events.jsonl'), '');
fs.writeFileSync(path.join(profile, 'settings.json'), JSON.stringify({ extensions: [fixture], compaction: { enabled: false }, retry: { enabled: false } }));
fs.writeFileSync(path.join(profile, 'fabric.json'), JSON.stringify({ fullCodeMode: true,
  agents: { model: 'p0-probe/offline', thinking: 'off', transport: 'process', maxConcurrent: 2, budgetUsd: 0, timeoutMs: 45000, retainRuns: true },
  prewalk: { enabled: false }, approvals: { read: 'allow', write: 'allow', exec: 'allow', agent: 'allow' }, mesh: { enabled: true, persist: false },
  executor: { timeoutMs: 120000, kernel: 'typescript' },
}));
const env = { PI_CODING_AGENT_DIR: profile, PI_FABRIC_PI_BINARY: cli, PI_FABRIC_RUN_ROOT: path.join(scratch, 'runs'), PI_OFFLINE: '1', HOME: path.join(scratch, 'home'), TMPDIR: scratch };
fs.mkdirSync(env.HOME);
const args = ['--no-session', '--no-skills', '--no-prompt-templates', '--no-context-files', '--offline', '-e', candidate, '--thinking', 'off'];
fs.writeFileSync(path.join(out, 'command.json'), JSON.stringify({ executable: process.execPath, argv: [cli, '--mode', 'rpc', '--provider', 'p0-probe', '--model', 'offline', ...args], cwd, env,
  candidateSha256: createHash('sha256').update(fs.readFileSync(candidate)).digest('hex'),
  note: 'Clean env; isolated empty HOME/profile; keyless in-process synthetic model issues one fabric_exec call. Actual Pi CLI, built Fabric extension, public agents.* surface and owning actor manager. No network/credentials.' }, null, 2));
const client = new RpcClient({ cliPath: cli, cwd, env, provider: 'p0-probe', model: 'offline', args });
const events = [];
client.onEvent(event => { events.push(event); fs.appendFileSync(path.join(out, 'events.jsonl'), JSON.stringify(event) + '\n'); });
let failure;
try {
  await client.start();
  const state = await client.getState();
  assert.equal(state.model.provider + '/' + state.model.id, 'p0-probe/offline');
  await client.promptAndWait('P0_RESERVATION_MAIN', undefined, 150000);
  const toolEnd = events.find(event => event.type === 'tool_execution_end' && event.toolName === 'fabric_exec');
  fs.writeFileSync(path.join(out, 'public-tool-result.json'), JSON.stringify(toolEnd, null, 2));
  assert.ok(toolEnd, 'public fabric_exec result missing');
  assert.equal(toolEnd.isError, false, JSON.stringify(toolEnd));
  assert.equal(toolEnd.result.details.success, true, 'guest execution failed: ' + JSON.stringify(toolEnd.result));
  const value = JSON.parse(toolEnd.result.content.filter(part => part.type === 'text').map(part => part.text).join('\n'));
  fs.writeFileSync(path.join(out, 'guest-value.json'), JSON.stringify(value, null, 2));
  // Reservation: manager-issued generation and one-time capability, never shown by status/list.
  assert.match(value.reserved.activationFilterReservation.generation, /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/);
  assert.deepEqual({ ...value.reserved.activationFilterReservation, generation: undefined }, { ...value.request, generation: undefined });
  assert.equal(value.tokenIssued, true);
  assert.equal(value.tokenInStatusOrList, false, 'capability or digest leaked into status/list');
  // Provenance and guarded replacement through the real entry path.
  const byLabel = Object.fromEntries(value.refusals.map(row => [row.label, row]));
  for (const label of ['forged-observation', 'forged-closed']) assert.match(byLabel[label].message ?? 'ADMITTED', /not authorized/, label);
  for (const label of ['unscoped-replace', 'unscoped-clear']) assert.match(byLabel[label].message ?? 'ADMITTED', /held by P0 reservation/, label);
  assert.deepEqual(value.afterRefusals.activationFilterReservation, value.reserved.activationFilterReservation);
  assert.deepEqual(value.afterRefusals.activationFilter, [{ id: 'p0', topic: ['github.demo'] }]);
  // Terminal observation: review alone holds; review + every required security verdict releases.
  assert.equal(value.review.activationFilterReservation.reviewTerminal, true);
  assert.deepEqual(value.review.activationFilter, [{ id: 'p0', topic: ['github.demo'] }]);
  assert.equal(value.receipt.activationFilter, undefined);
  assert.equal(value.receipt.activationFilterReservation, undefined);
  assert.equal(value.receipt.activationFilterRelease.reason, 'verdicts-terminal');
  assert.deepEqual(value.receipt.activationFilterRelease.reservation, { ...value.reserved.activationFilterReservation, reviewTerminal: true, securityTerminal: ['security'] });
  // Owner readback equals the receipt.
  assert.deepEqual(value.readback.activationFilterRelease, value.receipt.activationFilterRelease);
  assert.equal(value.readback.activationFilter, undefined);
  fs.writeFileSync(path.join(out, 'summary.json'), JSON.stringify({ passed: true, generation: value.reserved.activationFilterReservation.generation,
    refusals: value.refusals, release: value.receipt.activationFilterRelease, readbackMatchesReceipt: true }, null, 2));
  console.log('PASS: actual Pi CLI -> built Fabric -> fabric_exec agents.setActivationFilter: manager-issued generation + one-time capability; forged observations and unscoped replace/clear refused; exact review+security release; owner readback equals receipt.');
} catch (error) { failure = error; console.error(error); }
finally {
  if (client.process) {
    const child = client.process;
    const exited = child.exitCode !== null || child.signalCode !== null ? Promise.resolve() : new Promise(resolve => child.once('exit', resolve));
    child.stdin.end();
    const timer = setTimeout(() => child.kill('SIGTERM'), 10000);
    await exited; clearTimeout(timer);
  }
  fs.writeFileSync(path.join(out, 'stderr.log'), client.getStderr());
  fs.rmSync(scratch, { recursive: true, force: true });
}
if (failure) process.exitCode = 1;
