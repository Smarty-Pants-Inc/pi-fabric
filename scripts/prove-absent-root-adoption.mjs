// Usage: nice -n 19 node scripts/prove-absent-root-adoption.mjs artifacts/real-pi-<head8>
// Real offline Pi RPC Mains, real resident hosts and real actor worker; inference alone is synthetic.
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { RpcClient } from '@earendil-works/pi-coding-agent';
import { buildSync } from 'esbuild';
const repo = fileURLToPath(new URL('../', import.meta.url));
const out = path.resolve(process.argv[2] ?? (() => { throw new Error('Retained proof output required'); })());
fs.mkdirSync(out, { recursive: true });
const scratch = fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), 'absent-native-'));
// Support-side reaping/diagnostics are bundled from the same source; both Pis and
// the executing resident/actor use the freshly built dist/index.js candidate.
const support = path.join(scratch, 'directory-support.mjs');
buildSync({ stdin: { contents: `export { MeshStore } from '${path.join(repo, 'src/mesh/store.ts')}';
export { reapDeadHostRecords } from '${path.join(repo, 'src/topology/host-reaper.ts')}';
export { ParticipantDirectory } from '${path.join(repo, 'src/topology/participant-directory.ts')}';
export { processStartTime, residentProcessAlive } from '${path.join(repo, 'src/residency/process-identity.ts')}';`, resolveDir: repo },
  outfile: support, bundle: true, platform: 'node', format: 'esm', target: 'node24', packages: 'external' });
const { MeshStore, reapDeadHostRecords, ParticipantDirectory, processStartTime, residentProcessAlive } = await import(support);
const cwd = path.join(scratch, 'workspace'), profile = path.join(scratch, 'profile'), meshRoot = path.join(scratch, 'mesh');
for (const dir of [cwd, profile, path.join(scratch, 'home')]) fs.mkdirSync(dir);
const transcript = path.join(out, 'native-proof.jsonl'), receipts = path.join(out, 'native-activations.jsonl');
fs.writeFileSync(transcript, ''); fs.writeFileSync(receipts, '');
const record = (type, value) => fs.appendFileSync(transcript, JSON.stringify({ type, at: Date.now(), ...value }) + '\n');
const candidate = path.join(repo, 'dist/index.js'), fixture = path.join(repo, 'tests/fixtures/absent-root-cli-provider.ts');
const cli = fileURLToPath(new URL('./cli.js', import.meta.resolve('@earendil-works/pi-coding-agent')));
// Clear the inherited environment before RpcClient merges it. No live profile or credential store.
const clean = { PATH: process.env.PATH, HOME: path.join(scratch, 'home'), TMPDIR: scratch, NODE_ENV: 'test', PI_OFFLINE: '1',
  PI_CODING_AGENT_DIR: profile, PI_FABRIC_PI_BINARY: cli, PI_FABRIC_MESH_ROOT: meshRoot, PI_FABRIC_PROJECT_ROOT: cwd, PI_FABRIC_PROJECT: cwd,
  PI_FABRIC_ROLE: 'project-agent', PI_FABRIC_TEST_LINEAGE_DEATH_GRACE_MS: '1000', ABSENT_ROOT_RECEIPTS: receipts };
for (const key of Object.keys(process.env)) delete process.env[key]; Object.assign(process.env, clean);
fs.writeFileSync(path.join(profile, 'settings.json'), JSON.stringify({ extensions: [fixture], enableInstallTelemetry: false, compaction: { enabled: false }, retry: { enabled: false } }));
fs.writeFileSync(path.join(profile, 'fabric.json'), JSON.stringify({ fullCodeMode: true, components: [], prewalk: { enabled: false },
  mcp: { enabled: false }, memory: { enabled: false }, jev: { enabled: false }, entropy: { compile: false }, speculation: { enabled: false }, compaction: { engine: 'pi' },
  approvals: { read: 'allow', write: 'allow', exec: 'allow', agent: 'allow' }, executor: { kernel: 'typescript', timeoutMs: 120000 },
  agents: { model: 'absent-root-proof/offline', thinking: 'off', transport: 'process', timeoutMs: 45000, budgetUsd: 0, maxConcurrent: 4, nice: 19, retainRuns: true },
  mesh: { enabled: true, persist: true, root: meshRoot, actorPollMs: 50 } }));
const mesh = new MeshStore(meshRoot, 256 * 1024, 500), clients = [], owned = new Map();
const files = dir => !fs.existsSync(dir) ? [] : fs.readdirSync(dir, { withFileTypes: true }).flatMap(entry => entry.isDirectory() ? files(path.join(dir, entry.name)) : [path.join(dir, entry.name)]);
const json = file => { try { return JSON.parse(fs.readFileSync(file, 'utf8')); } catch { return undefined; } };
const owners = () => files(meshRoot).filter(file => path.basename(file) === 'owner.json').map(file => ({ file, value: json(file) })).filter(row => row.value?.hostId && row.value.pid);
const capture = () => { for (const row of owners()) { owned.set(row.value.pid, row.value); const launcher = row.value.handover?.launcher; if (launcher?.pid) owned.set(launcher.pid, launcher); } };
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
const wait = async (probe, label, timeout = 120000) => { const end = Date.now() + timeout; for (;;) { const value = await probe(); if (value) return value; if (Date.now() >= end) throw new Error('Timed out: ' + label); await delay(250); } };
const killOwned = (pid, signal) => { const owner = owned.get(pid); assert.ok(owner, 'Not an isolated proof owner'); if (!residentProcessAlive(pid, owner.processStartTime)) return;
  assert.equal(processStartTime(pid), owner.processStartTime, 'PID birth changed'); process.kill(pid, signal); record('owned-signal', { pid, signal }); };
const stopHost = async row => { capture(); const launcher = row.value.handover?.launcher; if (launcher?.pid) killOwned(launcher.pid, 'SIGTERM'); else killOwned(row.value.pid, 'SIGTERM');
  await wait(() => !residentProcessAlive(row.value.pid, row.value.processStartTime), 'resident stopped', 30000);
  if (launcher?.pid) await wait(() => !residentProcessAlive(launcher.pid, launcher.processStartTime), 'launcher stopped', 30000);
  record('host-stopped', { owner: row }); };
const stopMain = async (item, signal = 'SIGTERM') => { const child = item.client.process; if (!child || child.exitCode !== null || child.signalCode !== null) return;
  const exited = new Promise(resolve => child.once('exit', (code, received) => { record('main-exit', { endpoint: item.name, code, signal: received }); resolve(); }));
  child.kill(signal); const timer = setTimeout(() => child.kill('SIGKILL'), 15000); await exited; clearTimeout(timer); };
const make = async name => {
  const args = ['--offline', '--no-skills', '--no-prompt-templates', '--no-context-files', '--no-themes', '--approve', '--thinking', 'off',
    '--session-dir', path.join(scratch, name, 'sessions'), '-e', candidate];
  const client = new RpcClient({ cliPath: cli, cwd, env: { ...clean, PI_FABRIC_RUN_ROOT: path.join(scratch, name, 'runs') }, provider: 'absent-root-proof', model: 'offline', args });
  const item = { name, client, events: [] }; clients.push(item);
  client.onEvent(event => { item.events.push(event); record('rpc-event', { endpoint: name, event }); });
  record('command', { endpoint: name, executable: process.execPath, args: [cli, '--mode', 'rpc', ...args], env: clean, candidateSha256: createHash('sha256').update(fs.readFileSync(candidate)).digest('hex') });
  await client.start(); return item;
};
const invoke = async (item, packet) => {
  const start = item.events.length; record('rpc-prompt', { endpoint: item.name, packet });
  await item.client.promptAndWait('ABSENT ' + JSON.stringify(packet), undefined, 120000);
  const end = item.events.slice(start).find(event => event.type === 'tool_execution_end' && event.toolName === 'fabric_exec');
  assert.ok(end, 'Missing public fabric_exec result: ' + item.client.getStderr()); assert.equal(end.isError, false, JSON.stringify(end)); assert.equal(end.result.details?.success, true, JSON.stringify(end.result));
  const text = end.result.content.filter(part => part.type === 'text').map(part => part.text).join('\n'), details = end.result.details;
  const value = JSON.parse(text.split('\n').slice(details.outputFormatStartLine ?? 0, (details.outputFormatStartLine ?? 0) + details.outputFormatLines).join('\n'));
  record('public-result', { endpoint: item.name, packet, value }); capture(); return value;
};
let failure;
try {
  // Match the files-only fleet: no retained legacy session advertisement can
  // legitimately keep the otherwise reaped Main present forever.
  await mesh.put({ key: 'topology/liveness', identity: { id: 'isolated-policy', name: 'proof', kind: 'main' }, value: { version: 1, hostLeases: 'files', participants: 'files' } });
  const a = await make('main-a'), created = await invoke(a, { create: true, name: 'durable-absent-proof', topics: true });
  const oldRoot = created.main.id, actorId = created.actor.id;
  const oldHost = await wait(() => owners().find(row => json(path.join(path.dirname(row.file), 'config.json'))?.rootId === oldRoot), 'Main A resident owner');
  assert.equal(created.actor.rootId, oldRoot); assert.equal(created.actor.residency, 'durable');
  // Crash exit leaves records but no terminal close receipt; the native reaper withdraws them.
  await stopMain(a, 'SIGKILL'); await stopHost(oldHost);
  const rootKey = 'topology/participants/' + createHash('sha256').update(oldRoot).digest('hex');
  await wait(async () => { await reapDeadHostRecords(mesh, { id: 'isolated-reaper', name: 'proof', kind: 'main' }, { ownHostId: 'isolated-reaper', deadAfterMs: 0 }); return mesh.get(rootKey, { fresh: true }) === undefined && !fs.existsSync(path.join(meshRoot, 'participants', rootKey.slice('topology/participants/'.length) + '.json')); }, 'native root reaping', 40000);
  assert.equal(mesh.get('topology/lineage-closures/' + createHash('sha256').update(oldRoot).digest('hex')), undefined);
  const directory = new ParticipantDirectory(mesh, { enabled: true, hostId: 'proof-observer', rootId: 'proof-observer', identity: { id: 'proof-observer', name: 'observer', kind: 'main' }, reapDeadHosts: false });
  assert.equal(directory.lineageAlive(oldRoot), true, 'Grace must still veto immediately after host stop');
  record('absent-before-grace', { oldRoot, actorId, rootAbsent: true, closureAbsent: true, stillAlive: true, legacyPresence: mesh.listAll('actors/').filter(entry => entry.value?.rootId === oldRoot) });
  const b = await make('main-b'), bootstrap = await invoke(b, { create: true, name: 'successor-host-bootstrap' }), newRoot = bootstrap.main.id;
  assert.notEqual(oldRoot, newRoot); assert.equal(directory.get(newRoot, Date.now(), { fresh: true })?.role, 'project-agent'); assert.equal(created.actor.project, bootstrap.actor.project);
  const bHost = await wait(() => owners().find(row => json(path.join(path.dirname(row.file), 'config.json'))?.rootId === newRoot), 'Main B resident owner');
  // Observe the authoritative registry/participant publication, then do ONE public
  // status readback. Repeated identical fabric_exec probes are not a wait API.
  const registryPath = files(meshRoot).find(file => path.basename(file) === 'actors.json' && json(file)?.actors?.some(row => row.id === actorId));
  assert.ok(registryPath);
  await wait(() => json(registryPath)?.actors?.find(row => row.id === actorId)?.rootId === newRoot && directory.get(actorId, Date.now(), { fresh: true })?.rootId === newRoot, 'same-project native adoption');
  const adopted = (await invoke(b, { status: true, id: actorId })).actor;
  assert.equal(directory.lineageAlive(oldRoot), false); assert.equal(adopted.id, actorId);
  await invoke(b, { event: true });
  await wait(() => files(created.actor.logDir).some(file => path.basename(file) === 'status.json' && json(file)?.status === 'completed'), 'adopted native worker completion');
  const messages = await invoke(b, { messages: true, id: actorId });
  assert.ok(messages.some(row => row.direction === 'out' && row.text === 'HANDLED_ADOPTED_EVENT'));
  const activations = fs.readFileSync(receipts, 'utf8').trim().split('\n').map(JSON.parse).filter(row => row.actorId === actorId);
  assert.equal(activations.length, 1); assert.match(activations[0].prompt, /ADOPTED_EVENT/);
  assert.equal(messages.filter(row => row.direction === 'out').length, 1);
  const registry = files(meshRoot).filter(file => path.basename(file) === 'actors.json').map(file => ({ file, value: json(file) })).find(row => row.value?.actors?.some(actor => actor.id === actorId));
  assert.ok(registry); assert.equal(registry.value.actors.find(row => row.id === actorId).rootId, newRoot);
  fs.writeFileSync(path.join(out, 'registry-after.json'), JSON.stringify(registry, null, 2));
  const result = { passed: true, oldRoot, newRoot, actorId, oldHost: oldHost.value.hostId, newHost: bHost.value.hostId, absentRootReaped: true, cleanClosureAbsent: true,
    graceFloorMs: 1000, effectiveGraceMs: 60000, graceCannotShortenTwoTtls: true, adoption: adopted, messages, nativeActivations: activations.length, api: 'real offline Pi RPC -> public fabric_exec -> real resident -> real Pi worker', isolatedMesh: meshRoot };
  fs.writeFileSync(path.join(out, 'result.json'), JSON.stringify(result, null, 2)); record('proof-result', result);
  await directory.close(); console.log('PASS: real Pi absent-root adoption and exactly one delivered-event activation', JSON.stringify({ oldRoot, newRoot, actorId }));
} catch (error) { failure = error; record('proof-failure', { error: String(error), stack: error.stack });
  fs.writeFileSync(path.join(out, 'directory-on-failure.json'), JSON.stringify({ state: mesh.listAll('', { fresh: true }), files: files(meshRoot).filter(file => file.includes('/participants/') || file.includes('/host-leases/')).map(file => ({ file, value: json(file) })) }, null, 2)); console.error(error); }
finally {
  capture();
  for (const client of clients) { await stopMain(client); fs.writeFileSync(path.join(out, client.name + '-stderr.log'), client.client.getStderr()); }
  for (const row of owners()) { if (residentProcessAlive(row.value.pid, row.value.processStartTime)) await stopHost(row); }
  for (const [pid, owner] of owned) if (residentProcessAlive(pid, owner.processStartTime)) { killOwned(pid, 'SIGKILL'); await wait(() => !residentProcessAlive(pid, owner.processStartTime), 'owned process joined', 10000); }
  record('cleanup', { allMainsExited: clients.every(item => item.client.process.exitCode !== null || item.client.process.signalCode !== null), allHostsExited: [...owned].every(([pid, owner]) => !residentProcessAlive(pid, owner.processStartTime)) });
  fs.rmSync(scratch, { recursive: true, force: true });
}
if (failure) process.exitCode = 1;
