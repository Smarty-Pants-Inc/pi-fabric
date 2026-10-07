#!/usr/bin/env node
// Real-entry-point proof for pi-fabric#439 (smarty-dev#4383): two genuine resident hosts
// (built dist/residency/launcher.js -> real Pi RPC child) share one isolated mesh. No direct
// library calls, mocked clocks, manual refreshes or live fleet. Their own jittered timers renew
// host leases; the real fabric-participants CLI reads peer visibility; the real fabric-actors
// CLI performs resident operations (dry-run requests, then a confirmed stop) across renewals.
// Phase 1 (quiet, WINDOW_MS): only file reads and the read-only participants CLI, so every lease
// write comes from the hosts' own timers. Phase 2 (12 s): resident operations, which legitimately
// trigger immediate (unjittered) refreshes, while visibility and renewals must continue.
// Usage: node scripts/prove-host-lease-cadence-cli.mjs RELEASE OUT SCRATCH PI_CLI DEFAULTS_JSON [WINDOW_MS]
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { spawn, execFileSync } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';

const [releaseArg, outArg, scratchArg, piArg, defaultsArg, windowArg] = process.argv.slice(2);
if (![releaseArg, outArg, scratchArg, piArg, defaultsArg].every(Boolean)) throw Error('Required: RELEASE OUT SCRATCH PI_CLI DEFAULTS_JSON [WINDOW_MS]');
const release = fs.realpathSync(releaseArg), out = path.resolve(outArg), scratch = path.resolve(scratchArg);
const piBinary = fs.realpathSync(piArg), node = fs.realpathSync(process.execPath);
const windowMs = Number(windowArg ?? 26_000), NOMINAL = 5_000, OPS_MS = 12_000;
fs.mkdirSync(out, { recursive: true }); fs.mkdirSync(scratch, { recursive: true, mode: 0o700 });
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
const read = file => { try { return JSON.parse(fs.readFileSync(file, 'utf8')); } catch { return undefined; } };
const sha = value => createHash('sha256').update(value).digest('hex');
const log = value => { const line = JSON.stringify(value); console.log(line); fs.appendFileSync(path.join(out, 'proof.jsonl'), line + '\n'); };
// Independent re-statement of the cadence contract: SHA-256(hostId) fraction scales 0.8..1.2x nominal.
const expectedCadence = hostId => NOMINAL * (0.8 + 0.4 * (createHash('sha256').update(hostId).digest().readUInt32BE(0) / 0xffffffff));
const hostIdOf = rootId => `resident:${sha(rootId).slice(0, 24)}`;
for (const entry of ['dist/residency/launcher.js', 'dist/residency/pi-entry.js', 'dist/worker.js', 'dist/index.js', 'bin/fabric-participants', 'bin/fabric-actors']) {
  assert(fs.statSync(path.join(release, entry)).isFile(), entry);
}
fs.writeFileSync(path.join(out, 'proof.jsonl'), '');
const defaults = JSON.parse(fs.readFileSync(defaultsArg, 'utf8'));
const root = fs.mkdtempSync(path.join(scratch, 'lease-cadence-'));
const meshRoot = path.join(root, 'mesh');
fs.mkdirSync(meshRoot, { recursive: true, mode: 0o700 });
// Choose two identities whose stable cadences differ by >= 600 ms so the distinction is measurable.
let rootIds;
for (;;) {
  rootIds = [0, 1].map(() => `session:lease-proof-${randomUUID()}`);
  if (Math.abs(expectedCadence(hostIdOf(rootIds[0])) - expectedCadence(hostIdOf(rootIds[1]))) >= 600) break;
}
const env = (home) => ({ PATH: process.env.PATH, HOME: path.join(home, 'home'), TMPDIR: path.join(home, 'tmp'),
  PI_CODING_AGENT_DIR: path.join(home, 'profile'), PI_FABRIC_NODE_BINARY: node, PI_FABRIC_MESH_ROOT: meshRoot,
  PI_FABRIC_PROJECT_ROOT: root, PI_FABRIC_RUN_ROOT: path.join(home, 'runs') });
const hosts = rootIds.map((rootId, index) => {
  const label = index ? 'b' : 'a', home = path.join(root, label);
  const residencyRoot = path.join(meshRoot, 'residency', sha(rootId)), actorRoot = path.join(home, 'actors');
  for (const directory of [residencyRoot, actorRoot, path.join(home, 'home'), path.join(home, 'profile'), path.join(home, 'tmp')]) {
    fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  }
  const config = { format: 1, rootId, sessionId: rootId.slice(8), cwd: root, projectRoot: root, meshRoot, actorRoot,
    sessionActorRoot: path.join(home, 'session-actors'), residencyRoot, fullCodeMode: true,
    agents: { ...defaults.agents, budgetUsd: 0, nice: 19 }, mesh: { ...defaults.mesh, actorScope: 'project' },
    retention: defaults.retention, workerPath: path.join(release, 'dist/worker.js'),
    fabricExtensionPath: path.join(release, 'dist/index.js'), piBinary, claudeBinary: 'absent-claude', vedaBinary: 'absent-veda',
    role: 'project-agent', project: 'isolated-lease-cadence-proof', kernel: 'typescript', pythonRuntime: 'monty' };
  const at = Date.now() - 60_000;
  const actor = { id: randomUUID().replaceAll('-', ''), name: `idle-${label}`, rootId, project: config.project,
    instructions: 'Remain idle. Do not start a turn.', status: 'idle', events: [], topics: [], residency: 'durable',
    delivery: 'mailbox', runner: 'pi', responseMode: 'text', requirements: [], createdAt: at, updatedAt: at, messages: [] };
  fs.writeFileSync(path.join(actorRoot, 'actors.json'), JSON.stringify({ format: 1, actors: [actor] }));
  const configPath = path.join(residencyRoot, 'config.json'); fs.writeFileSync(configPath, JSON.stringify(config, null, 2));
  const hostId = hostIdOf(rootId);
  return { label, home, rootId, hostId, residencyRoot, actor, configPath, env: env(home), expectedMs: expectedCadence(hostId),
    leasePath: path.join(meshRoot, 'host-leases', `${sha(hostId).slice(0, 32)}.json`), renewals: [], stdout: '', stderr: '' };
});
const cli = (host, file, args) => {
  const started = Date.now();
  try {
    const stdout = execFileSync(node, [path.join(release, file), ...args], { env: host.env, encoding: 'utf8', timeout: 20_000 });
    return { ok: true, ms: Date.now() - started, stdout };
  } catch (error) { return { ok: false, ms: Date.now() - started, stdout: error.stdout, stderr: error.stderr, status: error.status }; }
};
let failure;
const head = (() => { try { return execFileSync('git', ['-C', release, 'rev-parse', 'HEAD'], { encoding: 'utf8' }).trim(); } catch { return 'unknown'; } })();
log({ type: 'candidate', release, head, piBinary, node, meshRoot, windowMs,
  hosts: hosts.map(({ label, rootId, hostId, expectedMs }) => ({ label, rootId, hostId, expectedMs: Math.round(expectedMs) })) });
try {
  for (const host of hosts) {
    const args = [path.join(release, 'dist/residency/launcher.js'), '--config', host.configPath];
    host.command = [node, ...args];
    host.child = spawn(node, args, { cwd: root, stdio: ['ignore', 'pipe', 'pipe'], env: host.env });
    host.child.stdout.on('data', chunk => { host.stdout += chunk; }); host.child.stderr.on('data', chunk => { host.stderr += chunk; });
    host.exited = new Promise(resolve => host.child.once('close', (code, signal) => { host.exit = { code, signal }; resolve(); }));
  }
  const participant = host => read(path.join(meshRoot, 'participants', `${sha(host.actor.id)}.json`));
  const deadline = Date.now() + 40_000;
  for (const host of hosts) {
    while (!(host.owner = read(path.join(host.residencyRoot, 'owner.json')))?.readyAt || !participant(host)) {
      if (host.exit || Date.now() > deadline) throw Error(`${host.label} not ready: ${host.stderr}; ${JSON.stringify(read(path.join(host.residencyRoot, 'error.json')))}`);
      await delay(25);
    }
    assert.equal(host.owner.hostId, host.hostId);
    assert.notEqual(host.owner.pid, host.child.pid, 'launcher must have a genuine Pi child');
    const snapshot = read(path.join(host.residencyRoot, `launch-${host.owner.configDigest}.json`));
    assert.equal(snapshot.piBinary, piBinary, 'launcher must pin the genuine Pi CLI');
    assert.equal(fs.realpathSync(`/proc/${host.owner.pid}/exe`), node, 'owner must run the real Node runtime');
    const cmd = fs.readFileSync(`/proc/${host.owner.pid}/cmdline`, 'utf8').split('\0').filter(Boolean);
    assert(cmd.includes(piBinary) || ['pi', 'pi-rpc'].includes(cmd[0]) || cmd.some(arg => arg.endsWith('/rpc-entry.js')), `genuine Pi RPC: ${JSON.stringify(cmd)}`);
    log({ type: 'ready', label: host.label, launcherPid: host.child.pid, piPid: host.owner.pid, piCommand: cmd, releaseRoot: host.owner.releaseRoot });
  }
  const start = Date.now(), opsAt = start + windowMs, stopAt = opsAt + OPS_MS - 6_000;
  let nextCheck = start, stopped;
  while (Date.now() - start < windowMs + OPS_MS) {
    const quiet = Date.now() < opsAt;
    for (const host of hosts) {
      assert(!host.exit, `${host.label} launcher exited`);
      const updatedAt = read(host.leasePath)?.updatedAt;
      if (updatedAt && updatedAt !== host.renewals.at(-1)) host.renewals.push(updatedAt);
    }
    if (Date.now() >= nextCheck) {
      nextCheck += 3_000;
      const view = cli(hosts[0], 'bin/fabric-participants', ['--json', '--mesh', meshRoot]);
      assert(view.ok, `fabric-participants failed: ${view.stderr}`);
      const rows = JSON.parse(view.stdout);
      const visible = hosts.map(host => rows.some(row => row.id === host.actor.id && row.ownerHostId === host.hostId && !row.stale));
      const ops = quiet ? [] : hosts.map(host => {
        const op = cli(host, 'bin/fabric-actors', ['stop', '--resident', host.residencyRoot, '--actor', host.actor.name, '--mesh-root', meshRoot, '--dry-run']);
        return { label: host.label, ok: op.ok, ms: op.ms, response: op.ok ? JSON.parse(op.stdout) : op.stderr };
      });
      log({ type: 'check', phase: quiet ? 'quiet' : 'ops', t: Date.now() - start, visible, leaseUpdatedAt: hosts.map(host => read(host.leasePath)?.updatedAt), dryRun: ops });
      assert(visible.every(Boolean), 'both hosts must stay visible to the real participants CLI');
      assert(ops.every(op => op.ok), 'resident dry-run operation must succeed');
    }
    if (!stopped && Date.now() >= stopAt) {
      const host = hosts[0];
      const op = cli(host, 'bin/fabric-actors', ['stop', '--resident', host.residencyRoot, '--actor', host.actor.name, '--mesh-root', meshRoot, '--confirm-dead-root', host.rootId]);
      stopped = { t: Date.now() - start, at: Date.now(), ok: op.ok, ms: op.ms, response: op.ok ? JSON.parse(op.stdout) : op.stderr };
      log({ type: 'resident-stop', label: host.label, ...stopped });
      assert(op.ok, `confirmed resident stop failed: ${op.stderr}`);
    }
    await delay(20);
  }
  const actorsAfter = hosts.map(host => read(path.join(host.home, 'actors', 'actors.json'))?.actors?.find(row => row.id === host.actor.id)?.status);
  const cadence = hosts.map(host => {
    // Timer cadence from the quiet phase only; resident operations trigger immediate refreshes.
    const intervals = host.renewals.slice(1).map((at, i) => [at, at - host.renewals[i]]).filter(([at]) => at < opsAt).map(([, ms]) => ms);
    const sorted = [...intervals].sort((a, b) => a - b), median = sorted[Math.floor(sorted.length / 2)];
    return { label: host.label, hostId: host.hostId, expectedMs: Math.round(host.expectedMs), medianMs: median, intervals, renewals: host.renewals.length,
      renewedDuringOps: host.renewals.filter(at => at >= opsAt).length, renewedAfterStop: host.renewals.filter(at => at > stopped.at).length };
  });
  log({ type: 'summary', head, cadence, actorsAfter, stopped });
  for (const row of cadence) {
    assert(row.intervals.length >= 3, `${row.label}: at least three timer renewals in the quiet phase`);
    assert(Math.abs(row.medianMs - row.expectedMs) <= 250, `${row.label}: cadence ${row.medianMs} vs ${row.expectedMs}`);
    assert(row.medianMs >= NOMINAL * 0.8 - 50 && row.medianMs <= NOMINAL * 1.2 + 250, `${row.label}: within the jitter band`);
  }
  assert(Math.abs(cadence[0].medianMs - cadence[1].medianMs) >= 300, 'hosts must renew at distinct cadences');
  assert(cadence.every(row => row.renewedAfterStop >= 1), 'renewals continue after the resident operation');
  assert.equal(actorsAfter[0], 'stopped', 'confirmed resident stop must take effect');
  assert.equal(actorsAfter[1], 'idle', 'peer actor untouched');
} catch (error) { failure = error; }
finally {
  for (const host of hosts) {
    if (host.child && !host.exit) host.child.kill('SIGTERM');
    const deadline = Date.now() + 30_000;
    while (host.child && !host.exit && Date.now() < deadline) await delay(50);
    if (host.child && !host.exit) {
      if (host.owner?.pid && host.owner.handover?.launcher.pid === host.child.pid && fs.existsSync(`/proc/${host.owner.pid}/stat`)) {
        const stat = fs.readFileSync(`/proc/${host.owner.pid}/stat`, 'utf8');
        if (stat.slice(stat.lastIndexOf(')') + 2).trim().split(/\s+/)[19] === host.owner.processStartTime) process.kill(host.owner.pid, 'SIGKILL');
      }
      host.child.kill('SIGKILL');
    }
    if (host.exited) await host.exited;
    let gone = !host.owner?.pid;
    for (let retry = 0; !gone && retry < 100; retry++) {
      try { process.kill(host.owner.pid, 0); await delay(50); } catch (error) { if (error.code === 'ESRCH') gone = true; else throw error; }
    }
    const keep = path.join(out, `host-${host.label}`); fs.mkdirSync(keep, { recursive: true });
    fs.writeFileSync(path.join(keep, 'launcher.stdout.log'), host.stdout); fs.writeFileSync(path.join(keep, 'launcher.stderr.log'), host.stderr);
    for (const name of ['config.json', 'launcher.log', 'child-stderr.log', 'error.json']) {
      const file = path.join(host.residencyRoot, name); if (fs.existsSync(file)) fs.copyFileSync(file, path.join(keep, name));
    }
    log({ type: 'teardown', label: host.label, exit: host.exit, ownedPiExited: gone });
    if (!gone && !failure) failure = Error(`${host.label}: owned Pi child must be gone`);
  }
  log({ type: 'outcome', head, outcome: failure ? 'FAIL' : 'PASS', failure: failure?.stack });
}
if (failure) process.exitCode = 1;
