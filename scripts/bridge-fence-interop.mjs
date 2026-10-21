#!/usr/bin/env node
// Hosted-only real built NEW CLI -> built OLD agent. No wire shim or synthetic ACK.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { createRequire } from 'node:module';
import { pathToFileURL } from 'node:url';
import { spawn } from 'node:child_process';
import { hostedOnly } from './hosted-only.mjs';

const BASE = '069f594ff99429da11ea819b4930c012ca4cdf4e';
if (!hostedOnly('old/new mesh bridge stdio interop') || process.platform !== 'linux') {
  console.error('BLOCKED: real CLI teardown requires genuine hosted Linux CI; never spoof admission.');
  process.exit(4);
}
const flags = new Map();
for (let i = 2; i < process.argv.length; i += 2) {
  assert(process.argv[i]?.startsWith('--') && process.argv[i + 1], 'key/value arguments required');
  assert(!flags.has(process.argv[i]), 'duplicate argument');
  flags.set(process.argv[i], process.argv[i + 1]);
}
assert.deepEqual([...flags.keys()].sort(), ['--new-commit', '--new-source-sha', '--new-release', '--old-release', '--private-fixture'].sort());
const absolute = name => { const p = flags.get(name); assert(path.isAbsolute(p), name); return fs.realpathSync(p); };
const root = absolute('--private-fixture');
const stat = fs.lstatSync(root);
assert(stat.isDirectory() && !stat.isSymbolicLink() && stat.uid === process.getuid() && (stat.mode & 0o777) === 0o700);
assert.deepEqual(fs.readdirSync(root), [], 'fixture must be empty, never a backup or live mesh');
const json = (name, value) => fs.writeFileSync(path.join(root, name), JSON.stringify(value, null, 2) + '\n', { mode: 0o600 });
const digest = file => createHash('sha256').update(fs.readFileSync(file)).digest('hex');
const hash = id => createHash('sha256').update(id).digest('hex');
const sides = [], expected = new Map(), admissions = [], handlerFailures = [], receipts = [];
let child, exited, closed = false, stderr = '', spawnFailure, transport, stopReceipt, failure, deadline;
let newCommit, newSourceSha, provenance;
const all = store => {
  const events = []; let offset = 0;
  for (;;) {
    const page = store.tail(offset, 500); events.push(...page.events);
    if (!page.events.length || page.nextOffset === offset) return events;
    offset = page.nextOffset;
  }
};
function treeDigests(dir, prefix = '') {
  return fs.readdirSync(dir).sort().flatMap(name => {
    const file = path.join(dir, name), relative = path.join(prefix, name), st = fs.lstatSync(file);
    assert(!st.isSymbolicLink(), `dist closure link: ${file}`);
    if (st.isDirectory()) return treeDigests(file, relative);
    assert(st.isFile()); return [{ path: relative, sha256: digest(file) }];
  });
}
function healthy() {
  if (spawnFailure) throw spawnFailure;
  assert(!closed, `CLI exited early: ${stderr}`);
  assert.deepEqual(handlerFailures, []);
  assert(!/stall v1|stopped:/.test(stderr), stderr);
}
// fs rename/append notifications and child stderr drive progress. The one-shot timer
// bounds a failed observation; it is not a polling carrier.
function observe(check, label, ms = 15000) {
  return new Promise((resolve, reject) => {
    let done = false, timer;
    const watchers = sides.map(side => fs.watch(side.store.root, inspect));
    function finish(error) {
      if (done) return; done = true; clearTimeout(timer); watchers.forEach(w => w.close());
      child.stderr.off('data', inspect); child.off('close', inspect); child.off('error', inspect);
      error ? reject(error) : resolve();
    }
    function inspect() { if (done) return; try { healthy(); if (check()) finish(); } catch (error) { finish(error); } }
    watchers.forEach(w => w.on('error', finish));
    child.stderr.on('data', inspect); child.on('close', inspect); child.on('error', inspect);
    timer = setTimeout(() => finish(new Error(`no progress within ${ms}ms: ${label}`)), ms); inspect();
  });
}
async function fixture(release, name) {
  // ONE source module graph per release, just like mesh-bridge-ingress.test.ts.
  // In particular never pass a bundled MeshStore into source control-plane internals:
  // custody/private authority brands and module-local state must have one owner.
  const require = createRequire(path.join(release, 'package.json'));
  const { createJiti } = require('jiti');
  const jiti = createJiti(pathToFileURL(path.join(release, 'package.json')).href, { tryNative: false, fsCache: false });
  const { MeshStore } = await jiti.import(path.join(release, 'src/mesh/store.ts'));
  const { writeHostLease } = await jiti.import(path.join(release, 'src/topology/host-leases.ts'));
  const { writeParticipantFile } = await jiti.import(path.join(release, 'src/topology/participant-files.ts'));
  const { FabricControlPlane } = await jiti.import(path.join(release, 'src/topology/control-plane.ts'));
  const store = new MeshStore(path.join(root, name), 256 * 1024, 500, { stateBackend: 'sqlite', lockProtocol: 3 });
  const side = { store, name }; sides.push(side);
  await store.prepareLock();
  const now = Date.now(), identity = { id: `session:interop-${name}-00000000`, name: 'main', kind: 'main', sessionId: `interop-${name}` };
  const incarnation = `fixture:${identity.id}`, key = prefix => `topology/${prefix}/${hash(identity.id)}`;
  const host = { format: 1, id: identity.id, rootId: identity.id, identity, startedAt: now, updatedAt: now, expiresAt: now + 600000 };
  await store.put({ key: key('hosts'), identity, value: host });
  writeHostLease(store.root, { id: identity.id, rootId: identity.id, identityId: identity.id,
    startedAt: now, updatedAt: now, expiresAt: host.expiresAt });
  writeParticipantFile(store.root, { key: key('participants'), version: 1, updatedAt: now, updatedBy: identity, value: {
    format: 1, id: identity.id, kind: 'root', rootId: identity.id, ownerHostId: identity.id, ownerIdentityId: identity.id,
    ownerIncarnation: incarnation, name, label: name, status: 'idle', runner: 'pi', transport: 'host',
    capabilities: ['steer', 'followUp'], sessionId: identity.sessionId, startedAt: now, updatedAt: now, controlProtocol: 'v1',
  } });
  Object.assign(side, { identity, incarnation, plane: new FabricControlPlane(store, identity, { enabled: true, hostId: identity.id,
    ownerIncarnation: incarnation, controlIncarnationFence: 'enforce', acknowledgementTimeoutMs: 15000 }) });
  return side;
}
try {
  const oldRelease = absolute('--old-release'), newRelease = absolute('--new-release');
  newCommit = flags.get('--new-commit'); newSourceSha = flags.get('--new-source-sha');
  assert.match(newCommit, /^[0-9a-f]{40}$/); assert.match(newSourceSha, /^[0-9a-f]{64}$/);
  assert.notEqual(oldRelease, newRelease);
  for (const release of [oldRelease, newRelease]) {
    assert(!root.startsWith(release + path.sep) && !release.startsWith(root + path.sep));
    for (const name of ['bin/mesh-bridge', 'dist/mesh-bridge.js', 'dist/native/manifest.json']) assert(fs.statSync(path.join(release, name)).isFile());
  }
  assert.equal(fs.readFileSync(path.join(oldRelease, 'COMMIT'), 'utf8').trim(), BASE);
  assert.equal(fs.readFileSync(path.join(newRelease, 'COMMIT'), 'utf8').trim(), newCommit);
  assert.equal(digest(path.join(newRelease, 'src/mesh/bridge.ts')), newSourceSha);
  provenance = [oldRelease, newRelease].map(release => ({ release,
    commit: fs.readFileSync(path.join(release, 'COMMIT'), 'utf8').trim(), binSha256: digest(path.join(release, 'bin/mesh-bridge')),
    bridgeSourceSha256: digest(path.join(release, 'src/mesh/bridge.ts')), dist: treeDigests(path.join(release, 'dist')) }));
  assert.notEqual(provenance[0].dist.find(f => f.path === 'mesh-bridge.js').sha256,
    provenance[1].dist.find(f => f.path === 'mesh-bridge.js').sha256, 'must exercise distinct built CLI versions');
  json('provenance.json', provenance);
  for (const name of ['home', 'runtime']) fs.mkdirSync(path.join(root, name), { mode: 0o700 });
  process.env.HOME = path.join(root, 'home'); process.env.XDG_RUNTIME_DIR = path.join(root, 'runtime');
  process.env.PI_CODING_AGENT_DIR = path.join(root, 'home', 'pi'); process.env.PI_FABRIC_MESH_STATE_BACKEND = 'sqlite';
  delete process.env.NOTIFY_SOCKET;
  const local = await fixture(newRelease, 'local'), remote = await fixture(oldRelease, 'remote');
  for (const destination of sides) destination.plane.start((command, from) => {
    try {
      const want = expected.get(command.commandId); assert(want, `unexpected admission ${command.commandId}`);
      assert.equal(destination, want.destination); assert.equal(from.id, want.source.identity.id); assert.equal(from.verified, 'bridge');
      assert.equal(command.targetId, destination.identity.id); assert.equal(command.ownerIncarnation, destination.incarnation);
      assert.equal(command.operation, 'followUp'); assert.equal(command.message, want.message); assert.deepEqual(command.data, want.payload);
      assert(!admissions.some(row => row.commandId === command.commandId), 'duplicate native admission');
      admissions.push({ receiver: destination.identity.id, from, commandId: command.commandId, eventId: command.eventId,
        operation: command.operation, message: command.message, data: command.data });
      return { accepted: true, messageId: `admitted:${command.commandId}` };
    } catch (error) { handlerFailures.push(String(error)); throw error; }
  });
  const argv = [path.join(newRelease, 'bin/mesh-bridge'), 'run', '--mesh', local.store.root, '--name', 'local', '--remote', 'remote',
    '--cursor', path.join(root, 'cursor.json'), '--lock-protocol', '3', '--call-timeout-ms', '5000', '--',
    process.execPath, path.join(oldRelease, 'bin/mesh-bridge'), 'agent', '--mesh', remote.store.root, '--peer', 'local', '--lock-protocol', '3'];
  child = spawn(process.execPath, argv, { env: { ...process.env }, stdio: ['ignore', 'ignore', 'pipe'] });
  child.stderr.setEncoding('utf8'); child.stderr.on('data', chunk => { stderr += chunk; });
  child.once('error', error => { spawnFailure = error; });
  exited = new Promise(resolve => child.once('close', (code, signal) => { closed = true; resolve({ code, signal }); }));
  json('custody.json', { pid: child.pid, processGroup: process.pid, command: [process.execPath, ...argv], deadlineMs: 60000,
    stat: child.pid ? fs.readFileSync(`/proc/${child.pid}/stat`, 'utf8') : null });
  deadline = setTimeout(() => { spawnFailure = new Error('hard CLI deadline 60000ms'); child.kill('SIGTERM'); }, 60000);
  await observe(() => stderr.includes('linked local <-> remote'), 'real hello/start/link handshake');
  const pids = fs.readFileSync(`/proc/${child.pid}/task/${child.pid}/children`, 'utf8').trim().split(/\s+/).filter(Boolean);
  assert.equal(pids.length, 1, 'NEW CLI owns exactly the OLD transport agent');
  const actualArgv = fs.readFileSync(`/proc/${pids[0]}/cmdline`, 'utf8').split('\0').filter(Boolean);
  assert.deepEqual(actualArgv.slice(0, 3), [process.execPath, path.join(oldRelease, 'bin/mesh-bridge'), 'agent']);
  transport = { cliPid: child.pid, oldAgentPid: Number(pids[0]), actualArgv, command: [process.execPath, ...argv], linked: true };
  json('transport.json', transport);
  for (let ordinal = 0; ordinal < 96; ordinal++) for (const direction of ['toLocal', 'toRemote']) {
    healthy();
    const source = direction === 'toLocal' ? remote : local, destination = direction === 'toLocal' ? local : remote;
    const commandId = `interop:${direction}:${ordinal}`, sourceId = `interop-event:${direction}:${ordinal}`;
    const message = `interop ${direction} ${ordinal}`, payload = { direction, ordinal, body: `${direction}:${ordinal}:` + 'x'.repeat(1024 + ordinal) };
    expected.set(commandId, { direction, ordinal, source, destination, message, payload, sourceId });
    const now = Date.now(), before = performance.now();
    const origin = await source.store.publish({ id: sourceId, topic: 'fabric.control.command', kind: 'followUp', from: source.identity,
      to: destination.identity.id, data: { version: 1, commandId, targetId: destination.identity.id, operation: 'followUp',
        replyTo: source.identity.id, ownerIncarnation: destination.incarnation, message, data: payload, requestedAt: now, deadlineAt: now + 15000 } });
    assert.equal(origin.id, sourceId); assert.equal(origin.verification, 'mesh');
    await observe(() => all(source.store).some(e => e.topic === 'fabric.control.ack' && e.data?.commandId === commandId && e.verification === 'bridge'), `${commandId} real return ACK`);
    receipts.push({ commandId, sourceEventId: origin.id, direction, ordinal, roundTripMs: performance.now() - before });
  }
  healthy();
} catch (error) { failure = error; }
finally {
  clearTimeout(deadline);
  if (child && !closed) child.kill('SIGTERM');
  const kill = setTimeout(() => { if (child && !closed) child.kill('SIGKILL'); }, 8000);
  try { if (exited) stopReceipt = await exited; } finally { clearTimeout(kill); }
  for (const side of sides) try { await side.plane?.close(); } catch (error) { failure ??= error; }
}
// Preserve startup failures as well as post-link evidence; never erase the owned fixture.
const logs = {};
for (const side of sides) {
  try { logs[side.name] = all(side.store); } catch (error) { failure ??= error; }
  try { side.store.closeState(); } catch (error) { failure ??= error; }
}
json('events.json', logs); json('admissions.json', admissions); json('round-trips.json', receipts);
fs.writeFileSync(path.join(root, 'bridge.stderr.log'), stderr, { mode: 0o600 });
try {
  if (failure) throw failure;
  assert.deepEqual(stopReceipt, { code: 0, signal: null }, 'normal CLI/agent teardown');
  assert(!fs.existsSync(`/proc/${transport.oldAgentPid}`), 'OLD agent must actually close before handback');
  assert.equal(expected.size, 192); assert.equal(receipts.length, 192); assert.equal(admissions.length, 192);
  assert.deepEqual(handlerFailures, []); assert(!/stall v1|stopped:/.test(stderr), stderr);
  const mapping = [];
  for (const direction of ['toLocal', 'toRemote']) {
    const source = direction === 'toLocal' ? sides[1] : sides[0], destination = direction === 'toLocal' ? sides[0] : sides[1];
    const sourceLog = logs[source.name], destLog = logs[destination.name];
    const delivered = destLog.filter(e => e.topic === 'fabric.control.command' && e.verification === 'bridge');
    assert.equal(delivered.length, 96); assert.equal(new Set(delivered.map(e => e.id)).size, 96);
    for (let ordinal = 0; ordinal < 96; ordinal++) {
      const commandId = `interop:${direction}:${ordinal}`, want = expected.get(commandId), event = delivered[ordinal];
      const origin = sourceLog.filter(e => e.topic === 'fabric.control.command' && e.id === want.sourceId);
      assert.equal(origin.length, 1); assert.equal(origin[0].verification, 'mesh');
      assert.equal(event.data.commandId, commandId, 'exact per-direction order');
      assert.equal(event.from.id, source.identity.id); assert.equal(event.from.verified, 'bridge');
      assert.equal(event.to, destination.identity.id); assert.equal(event.verification, 'bridge');
      assert.deepEqual(event.data.bridge, { from: source.name, id: want.sourceId });
      const { bridge, ...body } = event.data; assert.deepEqual(body, origin[0].data, 'complete command body');
      const admitted = admissions.filter(row => row.commandId === commandId);
      assert.equal(admitted.length, 1); assert.equal(admitted[0].eventId, event.id);
      const nativeAck = destLog.filter(e => e.topic === 'fabric.control.ack' && e.data?.commandId === commandId && e.verification === 'mesh');
      const returnedAck = sourceLog.filter(e => e.topic === 'fabric.control.ack' && e.data?.commandId === commandId && e.verification === 'bridge');
      assert.equal(nativeAck.length, 1); assert.equal(returnedAck.length, 1);
      const ack = returnedAck[0];
      assert.equal(ack.from.id, destination.identity.id); assert.equal(ack.from.verified, 'bridge'); assert.equal(ack.to, source.identity.id);
      assert.equal(ack.data.accepted, true); assert.equal(ack.data.targetId, destination.identity.id);
      assert.equal(ack.data.ownerIncarnation, destination.incarnation); assert.equal(ack.data.messageId, `admitted:${commandId}`);
      assert.deepEqual(ack.data.bridge, { from: destination.name, id: nativeAck[0].id });
      const { bridge: ackBridge, ...ackBody } = ack.data; assert.deepEqual(ackBody, nativeAck[0].data, 'complete ACK body');
      mapping.push({ commandId, direction, ordinal, sourceEventId: origin[0].id, destinationEventId: event.id,
        destinationSequence: event.sequence, nativeAckEventId: nativeAck[0].id, returnedAckEventId: ack.id });
      if (ordinal) assert(delivered[ordinal - 1].sequence < event.sequence);
    }
  }
  json('result.json', { ok: true, oldCommit: BASE, newCommit, newSourceSha, transport, stopReceipt,
    correctness: { sent: 192, delivered: 192, byDirection: { toLocal: 96, toRemote: 96 }, acks: 192, nativeAdmissions: 192,
      drops: 0, duplicates: 0, outOfOrder: 0, invalid: 0 }, mapping,
    limits: 'Real native stdio built CLI/agent interop, not SSH, performance, fault injection, rollout or live-host proof. Fixture support uses one archived source graph per release.' });
  console.log(JSON.stringify({ ok: true, result: path.join(root, 'result.json') }));
} catch (error) {
  json('result.json', { ok: false, oldCommit: BASE, newCommit, newSourceSha, transport, stopReceipt,
    error: error instanceof Error ? error.stack : String(error), handlerFailures });
  console.error(error); process.exitCode = 1;
}
