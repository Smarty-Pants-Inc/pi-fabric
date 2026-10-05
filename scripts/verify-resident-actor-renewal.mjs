#!/usr/bin/env node
// Real launcher/Pi proof: no ResidentHost construction, clock override, or manual refresh.
// Usage: node scripts/verify-resident-actor-renewal.mjs RELEASE CONTROL_RELEASE OUT SCRATCH PI_CLI DEFAULTS_JSON
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { spawn } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';

const [candidateArg, controlArg, outputArg, scratchArg, piArg, defaultsArg, onlyCase] = process.argv.slice(2);
if (![candidateArg, controlArg, outputArg, scratchArg, piArg, defaultsArg].every(Boolean)) {
  throw Error('Required: RELEASE CONTROL_RELEASE OUT SCRATCH PI_CLI DEFAULTS_JSON (use - for no control)');
}
if (onlyCase) assert(['candidate:shared', 'candidate:files', 'main-control:shared', 'main-control:files'].includes(onlyCase), 'unknown case');
const candidate = fs.realpathSync(candidateArg), output = path.resolve(outputArg), scratch = path.resolve(scratchArg);
const piBinary = fs.realpathSync(piArg), node = fs.realpathSync(process.execPath);
fs.mkdirSync(output, { recursive: true }); fs.mkdirSync(scratch, { recursive: true, mode: 0o700 });
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
const read = file => { try { return JSON.parse(fs.readFileSync(file, 'utf8')); } catch { return undefined; } };
const sha = value => createHash('sha256').update(value).digest('hex');
const results = [];
async function run(release, label, mode, expectRenewal) {
  for (const entry of ['dist/residency/launcher.js', 'dist/residency/pi-entry.js', 'dist/worker.js', 'dist/index.js']) {
    assert(fs.statSync(path.join(release, entry)).isFile(), entry);
  }
  const defaults = JSON.parse(fs.readFileSync(defaultsArg, 'utf8'));
  const root = fs.mkdtempSync(path.join(scratch, `${label}-${mode}-`));
  const rootId = `session:absent-main-${randomUUID()}`, sessionId = rootId.slice(8);
  const actorRoot = path.join(root, 'actors'), residencyRoot = path.join(root, 'resident'), meshRoot = path.join(root, 'mesh');
  for (const directory of [actorRoot, residencyRoot, meshRoot, path.join(root, 'home'), path.join(root, 'profile'), path.join(root, 'tmp')]) {
    fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  }
  const config = { format: 1, rootId, sessionId, cwd: root, projectRoot: root, meshRoot, actorRoot,
    sessionActorRoot: path.join(root, 'session-actors'), residencyRoot, fullCodeMode: true,
    agents: { ...defaults.agents, budgetUsd: 0, nice: 19 }, mesh: { ...defaults.mesh, actorScope: 'project' },
    retention: defaults.retention, workerPath: path.join(release, 'dist/worker.js'),
    fabricExtensionPath: path.join(release, 'dist/index.js'), piBinary, claudeBinary: 'absent-claude', vedaBinary: 'absent-veda',
    role: 'project-agent', project: 'isolated-renewal-proof', kernel: 'typescript', pythonRuntime: 'monty' };
  const actorActivityAt = Date.now() - 60_000;
  const actors = ['astra-idle', 'sol-idle'].map(name => ({ id: randomUUID().replaceAll('-', ''), name, rootId,
    project: config.project, instructions: 'Remain idle. Do not start a turn.', status: 'idle', events: [], topics: [],
    residency: 'durable', delivery: 'mailbox', runner: 'pi', responseMode: 'text', requirements: [],
    createdAt: actorActivityAt, updatedAt: actorActivityAt, messages: [] }));
  fs.writeFileSync(path.join(actorRoot, 'actors.json'), JSON.stringify({ format: 1, actors }));
  const configPath = path.join(residencyRoot, 'config.json'); fs.writeFileSync(configPath, JSON.stringify(config, null, 2));
  if (mode === 'files') {
    const { MeshStore } = await import(pathToFileURL(path.join(release, 'dist/mesh.js')));
    const mesh = new MeshStore(meshRoot, config.mesh.maxEventBytes, config.mesh.maxReadEvents);
    await mesh.put({ key: 'topology/liveness', value: { version: 1, hostLeases: 'files', participants: 'files' },
      identity: { id: 'independent-proof-reader', name: 'proof reader', kind: 'agent' } });
  }
  const args = [path.join(release, 'dist/residency/launcher.js'), '--config', configPath];
  const child = spawn(node, args, { cwd: root, stdio: ['ignore', 'pipe', 'pipe'], env: {
    PATH: process.env.PATH, HOME: path.join(root, 'home'), TMPDIR: path.join(root, 'tmp'),
    PI_CODING_AGENT_DIR: path.join(root, 'profile'), PI_FABRIC_NODE_BINARY: node,
    PI_FABRIC_MESH_ROOT: meshRoot, PI_FABRIC_PROJECT_ROOT: root, PI_FABRIC_RUN_ROOT: path.join(root, 'runs'),
  } });
  let stderr = '', stdout = '';
  child.stdout.on('data', chunk => { stdout += chunk; }); child.stderr.on('data', chunk => { stderr += chunk; });
  let exitReceipt;
  const exited = new Promise((resolve, reject) => {
    child.once('error', reject); child.once('close', (code, signal) => { exitReceipt = { code, signal }; resolve(exitReceipt); });
  });
  const participant = actor => read(path.join(meshRoot, 'participants', `${sha(actor.id)}.json`));
  let owner;
  const samples = [];
  let failure;
  try {
    const deadline = Date.now() + 25_000;
    while (!(owner = read(path.join(residencyRoot, 'owner.json')))?.readyAt || actors.some(actor => !participant(actor))) {
      if (exitReceipt || Date.now() > deadline) throw Error(`Host did not become ready: ${stderr}; ${JSON.stringify(read(path.join(residencyRoot, 'error.json')))}`);
      await delay(25);
    }
    assert(owner.pid !== child.pid, 'launcher must have a genuine Pi child');
    const command = fs.readFileSync(`/proc/${owner.pid}/cmdline`, 'utf8').split('\0').filter(Boolean);
    // Genuine Pi re-execs its RPC entry and sets process.title='pi' (or 'pi-rpc'), which
    // overwrites /proc/cmdline. Attest the launch snapshot and native Node child,
    // rather than incorrectly treating the rewritten argv as the original argv.
    const snapshot = read(path.join(residencyRoot, `launch-${owner.configDigest}.json`));
    assert.equal(snapshot.piBinary, piBinary, 'launcher must pin the genuine Pi CLI');
    assert.equal(owner.releaseRoot, release);
    assert.equal(owner.fabricExtensionPath, config.fabricExtensionPath);
    assert.equal(owner.handover?.launcher.pid, child.pid);
    assert.equal(owner.handover?.launcher.entry, path.join(release, 'dist/residency/pi-entry.js'));
    assert.equal(fs.realpathSync(`/proc/${owner.pid}/exe`), node, 'owner must run the real Node runtime');
    assert(command.includes(piBinary) || ['pi', 'pi-rpc'].includes(command[0]) || command.some(arg => arg.endsWith('/rpc-entry.js')),
      `owner must be genuine Pi RPC: ${JSON.stringify(command)}`);
    const leasePath = path.join(meshRoot, 'host-leases', `${sha(owner.hostId).slice(0, 32)}.json`);
    const observe = period => {
      assert(!exitReceipt, 'launcher unexpectedly exited');
      assert(!fs.existsSync(path.join(residencyRoot, 'main-generation.json')), 'no Main generation permitted');
      const files = fs.readdirSync(path.join(meshRoot, 'participants')).filter(file => file.endsWith('.json'));
      const records = files.map(file => read(path.join(meshRoot, 'participants', file)));
      assert(!records.some(entry => entry?.value?.kind === 'root' || entry?.value?.id === rootId), 'no Main participant permitted');
      const observedAt = Date.now();
      const row = { period, observedAt, mainAbsent: true, hostLeaseUpdatedAt: read(leasePath)?.updatedAt,
        actors: actors.map(actor => {
          const entry = participant(actor); assert(entry, actor.name);
          assert.equal(entry.value.kind, 'actor'); assert.equal(entry.value.rootId, rootId);
          assert.equal(entry.value.ownerHostId, owner.hostId); assert.equal(entry.value.status, 'idle');
          assert.equal(entry.value.updatedAt, actorActivityAt, 'renewal must preserve actor activity timestamp');
          return { id: actor.id, name: actor.name, envelopeUpdatedAt: entry.updatedAt, activityUpdatedAt: entry.value.updatedAt,
            envelopeAgeMs: observedAt - entry.updatedAt, version: entry.version, status: entry.value.status };
        }) };
      const prior = samples.at(-1);
      samples.push(row); console.log(JSON.stringify({ label, mode, ...row }));
      if (prior) {
        assert(row.hostLeaseUpdatedAt > prior.hostLeaseUpdatedAt, 'automatic host lease must advance');
        row.actors.forEach((actor, index) => {
          if (expectRenewal) assert(actor.envelopeUpdatedAt > prior.actors[index].envelopeUpdatedAt, 'automatic actor renewal must advance');
          else assert.equal(actor.envelopeUpdatedAt, prior.actors[index].envelopeUpdatedAt, 'unpatched control must remain unchanged');
        });
      }
    };
    observe(0);
    for (let period = 1; period <= 3; period++) {
      await delay(5_500);
      // The independent reader may run just before a due timer on a busy host.
      // Wait only for an automatic lease, never force a host refresh.
      const priorLease = samples.at(-1).hostLeaseUpdatedAt;
      const deadline = Date.now() + 3_000;
      while ((read(leasePath)?.updatedAt ?? 0) <= priorLease && Date.now() < deadline) await delay(25);
      observe(period);
    }
    if (!expectRenewal) assert(samples.at(-1).actors.every(actor => actor.envelopeAgeMs > 15_000), 'control records must go stale');
    results.push({ label, mode, release, root, command: [node, ...args], piCommand: command, launcherPid: child.pid,
      owner, config, expected: expectRenewal ? 'automatic renewals' : 'stale control', samples });
  } catch (error) { failure = error; }
  finally {
    if (!exitReceipt) child.kill('SIGTERM');
    let stopped = false;
    const deadline = Date.now() + 30_000;
    while (!exitReceipt && Date.now() < deadline) await delay(50);
    if (!exitReceipt) {
      // Only this probe's exact launcher and its attested child, never a name/PID search.
      if (owner?.pid && owner.handover?.launcher.pid === child.pid && fs.existsSync(`/proc/${owner.pid}/stat`)) {
        const stat = fs.readFileSync(`/proc/${owner.pid}/stat`, 'utf8');
        const birth = stat.slice(stat.lastIndexOf(')') + 2).trim().split(/\s+/)[19];
        if (birth === owner.processStartTime) process.kill(owner.pid, 'SIGKILL');
      }
      child.kill('SIGKILL');
    }
    await exited;
    if (owner?.pid) {
      for (let retry = 0; retry < 100; retry++) {
        try { process.kill(owner.pid, 0); } catch (error) { if (error.code === 'ESRCH') { stopped = true; break; } throw error; }
        await delay(50);
      }
      assert(stopped, 'owned Pi child must be gone before probe returns');
    }
    const keep = path.join(output, `${label}-${mode}`); fs.mkdirSync(keep, { recursive: true });
    fs.writeFileSync(path.join(keep, 'launcher.stdout.log'), stdout); fs.writeFileSync(path.join(keep, 'launcher.stderr.log'), stderr);
    for (const name of ['config.json', `launch-${owner?.configDigest}.json`, 'launcher.log', 'child-stderr.log', 'error.json']) {
      const file = path.join(residencyRoot, name); if (fs.existsSync(file)) fs.copyFileSync(file, path.join(keep, name));
    }
    fs.writeFileSync(path.join(keep, 'samples.json'), JSON.stringify({ owner, samples, exitReceipt, ownedPiExited: stopped,
      failure: failure?.stack }, null, 2));
    console.log(JSON.stringify({ label, mode, exitReceipt, ownedPiExited: stopped, outcome: failure ? 'FAIL' : 'PASS' }));
  }
  if (failure) throw failure;
}
try {
  for (const mode of ['shared', 'files']) if (!onlyCase || onlyCase === `candidate:${mode}`) await run(candidate, 'candidate', mode, true);
  if (controlArg !== '-') for (const mode of ['shared', 'files']) if (!onlyCase || onlyCase === `main-control:${mode}`)
    await run(fs.realpathSync(controlArg), 'main-control', mode, false);
} finally {
  fs.writeFileSync(path.join(output, 'real-launcher-results.json'), JSON.stringify(results, null, 2));
}
