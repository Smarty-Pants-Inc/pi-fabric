#!/usr/bin/env node
// Full-load shadow test for a Fabric release candidate (smarty-dev#6477 stage 1). See README.md.
//   node orchestrate.mjs --release DIR [--minutes 20] [--out DIR] [thresholds/load flags, see README]
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawn, spawnSync } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { argMap, delay, entryImport, readJson, residentHostId, residentRoot, writeJson } from './candidate.mjs';

const here = path.dirname(fileURLToPath(import.meta.url));
const args = argMap(process.argv.slice(2));
if (!args.release) { process.stderr.write('usage: orchestrate.mjs --release DIR [--minutes N] [--out DIR] ...\n'); process.exit(2); }
const release = fs.realpathSync(args.release);
const num = (name, fallback) => { const value = Number(args[name] ?? fallback); if (!Number.isFinite(value) || value < 0) throw new Error(`Bad --${name}`); return value; };
const opt = {
  minutes: num('minutes', 20), mains: num('mains', 80), mainsPerProcess: Math.max(1, num('mains-per-process', 4)),
  hosts: num('hosts', 15), actors: Math.max(2, num('actors', 10)), bridges: num('bridges', 3), spokeMains: Math.max(1, num('spoke-mains', 5)),
  rate: num('rate', 3), spokeRate: num('spoke-rate', 0.5), actorSaveS: num('actor-save-s', 30),
  maxBusy: num('max-busy', 30), maxTimeouts: num('max-timeouts', 0), maxLeaseS: num('max-lease-s', 15),
  memBudgetMb: num('mem-budget-mb', 8192), heapMb: num('heap-mb', 512), semiSpaceMb: num('semi-space-mb', 8), canaryEveryS: num('canary-every-s', 120),
  restartAt: num('restart-at', 0.5), keep: Boolean(args.keep),
};
if (opt.hosts > opt.mains) throw new Error('--hosts cannot exceed --mains: each resident host belongs to a Main');
for (const entry of ['dist/mesh.js', 'dist/participants-cli.js', 'dist/residency/host.js', 'dist/residency/actor-client.js', 'dist/fabric-runtime-state.js', 'bin/mesh-bridge']) {
  if (!fs.existsSync(path.join(release, entry))) { process.stderr.write(`Not a built Fabric release: ${path.join(release, entry)} is missing\n`); process.exit(2); }
}
const gitHead = spawnSync('git', ['-C', release, 'rev-parse', 'HEAD'], { encoding: 'utf8' }).stdout?.trim();
const candidateId = gitHead || path.basename(release);
const root = fs.mkdtempSync(path.join(os.tmpdir(), 'shadow-load-'));
const dirs = Object.fromEntries(['hub', 'spoke', 'project', 'hosts', 'logs', 'stats', 'state', 'home', 'tmp', 'profile', 'canary']
  .map(name => [name, path.join(root, name)]));
for (const directory of Object.values(dirs)) fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
const out = path.resolve(args.out ?? path.join(process.cwd(), `shadow-load-${candidateId.slice(0, 12)}-${new Date().toISOString().replace(/[:.]/g, '-')}`));
fs.mkdirSync(out, { recursive: true });
const log = message => {
  const line = `[shadow ${new Date().toISOString().slice(11, 19)}] ${message}`;
  process.stdout.write(`${line}\n`);
  fs.appendFileSync(path.join(out, 'progress.log'), `${line}\n`);
};

// Every child runs candidate code with a bounded heap and the lock-timeout instrument preloaded.
const baseEnv = {
  PATH: process.env.PATH, LANG: process.env.LANG ?? 'C.UTF-8', HOME: dirs.home, TMPDIR: dirs.tmp, PI_CODING_AGENT_DIR: dirs.profile,
  // A small young generation keeps each process's RSS near its live heap: V8 otherwise grows
  // semi-spaces to 32+ MB per process before collecting the parsed mesh state it churns through.
  NODE_OPTIONS: `--max-old-space-size=${opt.heapMb} --max-semi-space-size=${opt.semiSpaceMb} --import=${pathToFileURL(path.join(here, 'instrument.mjs')).href}`,
  SHADOW_RELEASE: release, SHADOW_STATS_DIR: dirs.stats, PI_FABRIC_LOCK_STATS: '1',
};
const procs = [];
const unexpectedExits = [];
const launch = (name, argv, extraEnv = {}) => {
  const output = fs.openSync(path.join(dirs.logs, `${name}.log`), 'a');
  const child = spawn(process.execPath, argv, { cwd: dirs.project, env: { ...baseEnv, SHADOW_ROLE: name, ...extraEnv }, stdio: ['ignore', output, output] });
  fs.closeSync(output);
  const entry = { name, argv, extraEnv, child, pid: child.pid, startedAt: Date.now(), exited: null, stopping: false };
  child.on('exit', (code, signal) => {
    entry.exited = { code, signal, at: Date.now() };
    if (!entry.stopping) {
      unexpectedExits.push({ name, code, signal, at: Date.now() });
      log(`UNEXPECTED EXIT ${name} code=${code} signal=${signal}`);
    }
  });
  procs.push(entry);
  return entry;
};
const stop = async (entries, graceMs = 30_000) => {
  for (const entry of entries) { entry.stopping = true; if (!entry.exited) entry.child.kill('SIGTERM'); }
  const deadline = Date.now() + graceMs;
  while (entries.some(entry => !entry.exited) && Date.now() < deadline) await delay(100);
  for (const entry of entries) if (!entry.exited) { log(`SIGKILL ${entry.name} (pid ${entry.pid}) after ${graceMs} ms`); entry.child.kill('SIGKILL'); }
  while (entries.some(entry => !entry.exited)) await delay(50);
};
const waitUntil = async (what, timeoutMs, probe) => {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const value = probe();
    if (value) return value;
    if (Date.now() > deadline) throw new Error(`${what}: not within ${timeoutMs / 1_000} s`);
    await delay(250);
  }
};

// Memory: the summed PSS (proportional set size: shared pages such as the node binary are split
// between their users, so the sum is the RAM the run really holds) of every child and its
// descendants (the bridge agents), sampled; the plain RSS sum is reported too.
const rssMb = pid => {
  try { return Number(/VmRSS:\s+(\d+)/.exec(fs.readFileSync(`/proc/${pid}/status`, 'utf8'))?.[1] ?? 0) / 1024; } catch { return 0; }
};
const pssMb = pid => {
  try { return Number(/^Pss:\s+(\d+)/m.exec(fs.readFileSync(`/proc/${pid}/smaps_rollup`, 'utf8'))?.[1] ?? 0) / 1024; } catch { return rssMb(pid); }
};
const descendants = pid => {
  let children = [];
  try { children = fs.readFileSync(`/proc/${pid}/task/${pid}/children`, 'utf8').trim().split(/\s+/).filter(Boolean).map(Number); } catch { /* gone */ }
  return [pid, ...children.flatMap(descendants)];
};
const memory = { measure: 'PSS', peakTotalMb: 0, peakAt: 0, peakRssSumMb: 0, peakProcessMb: 0, peakProcess: '', samples: 0 };
const sampleMemory = () => {
  let total = 0, rssSum = 0;
  for (const entry of [...procs, { pid: process.pid, name: 'orchestrator' }]) {
    if (entry.exited) continue;
    const pids = entry.child ? descendants(entry.pid) : [entry.pid];
    const mb = pids.reduce((sum, pid) => sum + pssMb(pid), 0);
    rssSum += pids.reduce((sum, pid) => sum + rssMb(pid), 0);
    total += mb;
    if (mb > memory.peakProcessMb) { memory.peakProcessMb = Math.round(mb); memory.peakProcess = entry.name; }
  }
  memory.samples++;
  memory.peakRssSumMb = Math.max(memory.peakRssSumMb, Math.round(rssSum));
  if (total > memory.peakTotalMb) { memory.peakTotalMb = Math.round(total); memory.peakAt = Date.now(); }
  return total;
};

const range = count => Array.from({ length: count }, (_, index) => index);
const newMain = (index, prefix) => { const sessionId = randomUUID(); return { id: `session:${sessionId}`, sessionId, name: `${prefix}-${String(index).padStart(2, '0')}` }; };
const plan = { candidate: candidateId, release, createdAt: Date.now(), mains: range(opt.mains).map(index => newMain(index, 'shadow-main')),
  spokeMains: range(opt.spokeMains).map(index => newMain(index, 'spoke-main')) };
plan.hosts = range(opt.hosts).map(index => {
  const main = plan.mains[index];
  const actors = range(opt.actors).map(slot => ({ id: randomUUID().replaceAll('-', ''), name: `h${index}-a${slot}` }));
  return { index, rootId: main.id, sessionId: main.sessionId, mainName: main.name, hostId: residentHostId(main.id),
    residencyRoot: residentRoot(dirs.hub, main.id), actors, loadActors: actors.slice(0, -1).map(actor => actor.id), canaryActor: actors.at(-1).id };
});
const planFile = path.join(dirs.state, 'plan.json');
writeJson(planFile, plan);
const common = ['--release', release, '--plan', planFile];

const result = { candidate: candidateId, release, options: opt, root, startedAt: Date.now(), phases: {}, markers: [] };
let exitCode = 2;
const all = { mains: [], hosts: [], spoke: [], bridges: [], aux: [] };
try {
  log(`candidate ${candidateId} (${release}); throwaway root ${root}`);
  log(`load: ${opt.mains} Mains (${opt.mainsPerProcess}/process), ${opt.hosts} resident hosts x ${opt.actors} durable actors, ${opt.bridges} bridge pairs, ${opt.spokeMains} spoke Mains, ${opt.rate} events/s, ${opt.minutes} min`);

  // 1. Mains: real participant directories with the candidate's 5 s heartbeat, staggered like logins.
  const mainsStarted = Date.now();
  for (let from = 0, index = 0; from < opt.mains; from += opt.mainsPerProcess, index++) {
    all.mains.push(launch(`mains-${index}`, [path.join(here, 'mains.mjs'), ...common, '--mesh', dirs.hub, '--list', 'mains', '--from', String(from),
      '--count', String(Math.min(opt.mainsPerProcess, opt.mains - from)), '--cwd', dirs.project, '--out', path.join(dirs.state, `mains-${index}.json`)]));
    if (index % 10 === 9) await delay(1_000);
  }
  await waitUntil('Mains ready', 180_000, () => all.mains.every((entry, index) => fs.existsSync(path.join(dirs.state, `mains-${index}.json.ready`)) || entry.exited) && true);
  if (all.mains.some(entry => entry.exited)) throw new Error('a Main process exited during start; see logs');
  result.phases.mainsReadyS = Math.round((Date.now() - mainsStarted) / 100) / 10;
  log(`${opt.mains} Mains up in ${result.phases.mainsReadyS} s`);

  // 2. Resident hosts: the candidate's dist/residency/host.js, each owning its Main's durable actors.
  const defaults = await entryImport(release, 'fabric-runtime-state.js', 'DEFAULT_FABRIC_CONFIG');
  const hostsStarted = Date.now();
  for (const host of plan.hosts) {
    const hostDir = path.join(dirs.hosts, String(host.index));
    const config = { format: 1, rootId: host.rootId, sessionId: host.sessionId, cwd: dirs.project, projectRoot: dirs.project, meshRoot: dirs.hub,
      actorRoot: path.join(hostDir, 'actors'), sessionActorRoot: path.join(hostDir, 'session-actors'), residencyRoot: host.residencyRoot,
      fullCodeMode: true, agents: { ...defaults.agents, budgetUsd: 0, nice: 19 }, mesh: { ...defaults.mesh, enabled: true, actorScope: 'project' },
      retention: defaults.retention, workerPath: path.join(release, 'dist/worker.js'), fabricExtensionPath: path.join(release, 'dist/index.js'),
      piBinary: 'absent-pi', claudeBinary: 'absent-claude', vedaBinary: 'absent-veda', role: 'project-agent', project: 'shadow-load',
      kernel: 'typescript', pythonRuntime: 'monty' };
    for (const directory of ['actors', 'session-actors', 'home', 'tmp', 'profile', 'runs']) fs.mkdirSync(path.join(hostDir, directory), { recursive: true, mode: 0o700 });
    fs.mkdirSync(host.residencyRoot, { recursive: true, mode: 0o700 });
    const at = Date.now() - 60_000;
    fs.writeFileSync(path.join(config.actorRoot, 'actors.json'), JSON.stringify({ format: 1, actors: host.actors.map(actor => ({
      id: actor.id, name: actor.name, rootId: host.rootId, project: config.project, instructions: 'Remain idle. Do not start a turn.',
      status: 'idle', events: [], topics: [], residency: 'durable', delivery: 'mailbox', runner: 'pi', responseMode: 'text',
      requirements: [], createdAt: at, updatedAt: at, messages: [] })) }));
    const configPath = path.join(host.residencyRoot, 'config.json');
    fs.writeFileSync(configPath, JSON.stringify(config, null, 2));
    all.hosts.push(launch(`host-${host.index}`, [path.join(here, 'host.mjs'), '--release', release, '--config', configPath], {
      HOME: path.join(hostDir, 'home'), TMPDIR: path.join(hostDir, 'tmp'), PI_CODING_AGENT_DIR: path.join(hostDir, 'profile'),
      PI_FABRIC_NODE_BINARY: process.execPath, PI_FABRIC_MESH_ROOT: dirs.hub, PI_FABRIC_PROJECT_ROOT: dirs.project, PI_FABRIC_RUN_ROOT: path.join(hostDir, 'runs') }));
    await delay(500);
  }
  await waitUntil('resident hosts ready', 180_000, () => {
    for (const host of plan.hosts) {
      const error = readJson(path.join(host.residencyRoot, 'error.json'));
      if (error?.error) throw new Error(`host-${host.index}: ${error.error}`);
      if (all.hosts[host.index].exited) throw new Error(`host-${host.index} exited during start; see logs/host-${host.index}.log`);
    }
    return plan.hosts.every(host => readJson(path.join(host.residencyRoot, 'owner.json'))?.readyAt);
  });
  result.phases.hostsReadyS = Math.round((Date.now() - hostsStarted) / 100) / 10;
  log(`${opt.hosts} resident hosts ready in ${result.phases.hostsReadyS} s`);

  // 3. The spoke: its own Mains, linked by real mesh-bridge run/agent pairs over a local pipe.
  all.spoke.push(launch('spoke-mains', [path.join(here, 'mains.mjs'), ...common, '--mesh', dirs.spoke, '--list', 'spokeMains', '--from', '0',
    '--count', String(opt.spokeMains), '--cwd', dirs.project, '--out', path.join(dirs.state, 'spoke-mains.json')]));
  await waitUntil('spoke Mains ready', 120_000, () => fs.existsSync(path.join(dirs.state, 'spoke-mains.json.ready')));
  const bridgeArgs = pair => [path.join(release, 'bin/mesh-bridge'), 'run', '--mesh', dirs.hub, '--name', `hub${pair}`, '--remote', `spoke${pair}`,
    '--cursor', path.join(dirs.state, `bridge-${pair}.cursor`), '--', 'env', `SHADOW_ROLE=bridge-agent-${pair}`, process.execPath,
    path.join(release, 'bin/mesh-bridge'), 'agent', '--mesh', dirs.spoke, '--peer', `hub${pair}`];
  for (const pair of range(opt.bridges).map(index => index + 1)) all.bridges.push(launch(`bridge-${pair}`, bridgeArgs(pair)));

  // 4. Expected directory: every Main and every resident durable actor on the hub.
  const since = Date.now();
  const expectedFile = path.join(dirs.state, 'expected.json');
  writeJson(expectedFile, { since, participants: [
    ...plan.mains.map(main => ({ id: main.id, name: main.name, kind: 'root', hostId: main.id })),
    ...plan.hosts.flatMap(host => host.actors.map(actor => ({ id: actor.id, name: actor.name, kind: 'actor', hostId: host.hostId }))),
  ] });
  const observerOut = path.join(dirs.state, 'observer.json');
  const driverOut = path.join(dirs.state, 'driver.json');
  const observer = launch('observer', [path.join(here, 'observer.mjs'), '--release', release, '--hub', dirs.hub, '--expected', expectedFile,
    '--out', observerOut, '--max-lease-s', String(opt.maxLeaseS)]);
  const driver = launch('driver', [path.join(here, 'driver.mjs'), ...common, '--hub', dirs.hub, '--spoke', dirs.spoke, '--rate', String(opt.rate),
    '--spoke-rate', String(opt.spokeRate), '--actor-save-s', String(opt.actorSaveS), '--out', driverOut]);
  all.aux.push(observer, driver);

  // 5. Full load for N minutes; restart the bridges once; a canary round every --canary-every-s.
  const loadStart = Date.now();
  result.loadStartedAt = loadStart;
  const loadEnd = loadStart + opt.minutes * 60_000;
  const restartAt = loadStart + opt.minutes * 60_000 * opt.restartAt;
  let restarted = opt.bridges === 0;
  const canaries = [];
  let canary;
  const runCanary = () => {
    const round = canaries.length;
    const outFile = path.join(dirs.state, `canary-${round}.json`);
    const entry = launch(`canary-${round}`, [path.join(here, 'canary.mjs'), ...common, '--hub', dirs.hub, '--spoke', dirs.spoke, '--scratch', dirs.canary,
      '--round', String(round), '--out', outFile]);
    entry.stopping = true; // a canary round is expected to exit
    canaries.push({ round, outFile, entry, startedAt: Date.now() });
    return entry;
  };
  let nextCanary = loadStart + 30_000;
  let nextProgress = loadStart + 60_000;
  let memoryExceeded = false;
  while (Date.now() < loadEnd) {
    const now = Date.now();
    const total = sampleMemory();
    if (total > opt.memBudgetMb) { memoryExceeded = true; log(`ABORT: total PSS ${Math.round(total)} MB > budget ${opt.memBudgetMb} MB`); break; }
    if (!restarted && now >= restartAt) {
      restarted = true;
      const at = Date.now();
      result.markers.push({ event: 'bridge-restart', at });
      log(`restarting ${all.bridges.length} bridges`);
      await stop(all.bridges, 15_000);
      const stoppedAt = Date.now();
      await delay(2_000);
      all.bridges = range(opt.bridges).map(index => launch(`bridge-${index + 1}`, bridgeArgs(index + 1)));
      result.markers.push({ event: 'bridge-restarted', at: Date.now(), stopMs: stoppedAt - at });
    }
    if (now >= nextCanary && (!canary || canary.exited)) { canary = runCanary(); nextCanary = now + opt.canaryEveryS * 1_000; }
    if (now >= nextProgress) {
      nextProgress = now + 60_000;
      const seen = readJson(observerOut);
      const done = canaries.map(item => readJson(item.outFile)).filter(Boolean);
      log(`t+${Math.round((now - loadStart) / 60_000)} min: PSS ${Math.round(total)} MB (peak ${memory.peakTotalMb}, RSS sum peak ${memory.peakRssSumMb}), lock busy ${seen?.lock?.busyPct ?? '?'}% ` +
        `(peak min ${seen?.lock?.peakMinuteBusyPct ?? '?'}%), lease max ${seen?.lease?.maxAgeMs ?? '?'} ms, directory misses ${seen?.directory?.misses ?? '?'}, ` +
        `canary ${done.filter(item => item.pass).length}/${done.length} pass, unexpected exits ${unexpectedExits.length}`);
    }
    await delay(1_000);
  }
  result.loadEndedAt = Date.now();
  // 6. L8 lock stats, read now: the last complete minutes are all load, before any teardown write.
  const loadMinutes = Math.max(1, Math.min(60, Math.floor((result.loadEndedAt - loadStart) / 60_000)));
  let l8 = null;
  if (fs.existsSync(path.join(release, 'dist/mesh-lock-stats-cli.js'))) {
    const cli = spawnSync(process.execPath, [path.join(release, 'bin/fabric-mesh-lock-stats'), '--mesh', dirs.hub, '--minutes', String(loadMinutes), '--json'],
      { encoding: 'utf8', env: { PATH: process.env.PATH, HOME: dirs.home } });
    try { l8 = JSON.parse(cli.stdout); } catch { l8 = { error: `fabric-mesh-lock-stats exit ${cli.status}: ${cli.stderr.slice(0, 300)}` }; }
  }
  // A final canary round at full load, then wind down.
  if (canary && !canary.exited) await waitUntil('canary round', 120_000, () => canary.exited);
  if (!memoryExceeded) { canary = runCanary(); await waitUntil('final canary round', 120_000, () => canary.exited); }
  await stop(all.aux, 15_000);
  const stopStart = Date.now();
  await stop([...all.bridges, ...all.spoke, ...all.hosts, ...all.mains], 45_000);
  result.phases.stopS = Math.round((Date.now() - stopStart) / 100) / 10;

  // Lock metrics: the candidate's own L8 lock stats when it records them, else the sampler.
  const observed = readJson(observerOut) ?? {};
  const driven = readJson(driverOut) ?? {};
  const procStats = fs.readdirSync(dirs.stats).filter(name => name.endsWith('.json')).map(name => readJson(path.join(dirs.stats, name))).filter(Boolean);
  // Only the load window counts: the start ramp (every Main joining at once) and the teardown
  // (every directory closing at once) are harness artifacts, reported separately.
  const allTimes = procStats.flatMap(row => row.times ?? []);
  const instrumentTimeouts = allTimes.filter(at => at >= loadStart && at <= result.loadEndedAt).length;
  const rampTimeouts = allTimes.filter(at => at < loadStart).length;
  const teardownTimeouts = allTimes.filter(at => at > result.loadEndedAt).length;
  const instrumentTries = procStats.reduce((sum, row) => sum + row.tries, 0);
  const mainsHealth = [...all.mains.keys()].flatMap(index => Object.values(readJson(path.join(dirs.state, `mains-${index}.json`))?.health ?? {}));
  const canaryResults = canaries.map(item => readJson(item.outFile) ?? { round: item.round, pass: false, error: `no result (exit ${JSON.stringify(item.entry.exited)})` });
  const restartMarker = result.markers.find(marker => marker.event === 'bridge-restart')?.at;
  const afterRestart = list => restartMarker ? (list ?? []).filter(event => event.at >= restartMarker).length : 0;

  // 7. Verdicts.
  const hasL8 = Boolean(l8 && !l8.error);
  const lock = {
    source: hasL8 ? 'L8 lock-stats (candidate)' : 'sampler (candidate predates L8 lock stats)',
    busyPct: hasL8 ? Math.round(l8.busyPct * 10) / 10 : observed.lock?.busyPct ?? null,
    peakMinuteBusyPct: hasL8 ? Math.round(l8.peakMinuteBusyPct * 10) / 10 : observed.lock?.peakMinuteBusyPct ?? null,
    timeouts: hasL8 ? l8.timeouts : instrumentTimeouts,
    timeoutsSource: hasL8 ? `L8 lock-stats, last ${loadMinutes} complete load minutes` : 'instrumented FABRIC_MESH_LOCK_TIMEOUT rejections in the load window',
    l8: hasL8 ? { minutes: l8.minutes, processes: l8.processes, n: l8.n, timeouts: l8.timeouts, tries: l8.tries, busyPct: l8.busyPct,
      peakMinuteBusyPct: l8.peakMinuteBusyPct, waitMeanMs: l8.waitMeanMs, waitP99Ms: l8.waitP99Ms, waitMaxMs: l8.waitMaxMs,
      holdMeanMs: l8.holdMeanMs, holdP99Ms: l8.holdP99Ms, holdMaxMs: l8.holdMaxMs,
      classes: (l8.classes ?? []).map(row => ({ lockClass: row.lockClass, n: row.n, busyPct: row.busyPct, holdP99Ms: row.holdP99Ms, waitP99Ms: row.waitP99Ms, waitMaxMs: row.waitMaxMs, timeouts: row.timeouts, tries: row.tries })) } : l8,
    sampler: { busyPct: observed.lock?.busyPct ?? null, peakMinuteBusyPct: observed.lock?.peakMinuteBusyPct ?? null, samples: observed.lock?.samples ?? 0, minutes: observed.lock?.minutes ?? [] },
    instrument: { timeouts: instrumentTimeouts, rampTimeouts, teardownTimeouts, tries: instrumentTries, processes: procStats.length,
      byRole: procStats.filter(row => row.timeouts > 0).map(row => ({ role: row.role, timeouts: row.timeouts,
        inLoad: (row.times ?? []).filter(at => at >= loadStart && at <= result.loadEndedAt).length, byMethod: row.byMethod, last: row.lastTimeout })) },
  };
  const leaseCheck = {
    expected: observed.expected ?? 0, maxLeaseAgeMs: observed.lease?.maxAgeMs ?? null, maxHostLeaseGapMs: observed.lease?.maxHostGapMs ?? null,
    participantsOverLimit: observed.lease?.participantsOver ?? null, overSamples: observed.lease?.overSamples ?? null,
    overAfterBridgeRestart: afterRestart(observed.lease?.overEvents), directorySamples: observed.directory?.samples ?? 0,
    directoryMisses: observed.directory?.misses ?? null, missingParticipants: observed.directory?.missingParticipants ?? null,
    missesAfterBridgeRestart: afterRestart(observed.directory?.missEvents), remoteMirrors: observed.directory?.remote, worst: observed.lease?.worst?.slice(0, 5),
    mainMaxConfirmAgeMs: Math.max(0, ...mainsHealth.map(row => row.maxConfirmAgeMs)), mainStalledSamples: mainsHealth.reduce((sum, row) => sum + row.stalledSamples, 0),
  };
  const verdicts = {
    a: { name: 'leases renew within limit and every participant stays listed (incl. after bridge restart)',
      pass: leaseCheck.expected > 0 && leaseCheck.directorySamples > 0 && leaseCheck.maxLeaseAgeMs !== null && leaseCheck.maxLeaseAgeMs <= opt.maxLeaseS * 1_000 &&
        leaseCheck.directoryMisses === 0 && restarted && unexpectedExits.length === 0 },
    b: { name: `lock busy <= ${opt.maxBusy}% and FABRIC_MESH_LOCK_TIMEOUT <= ${opt.maxTimeouts}`,
      pass: lock.busyPct !== null && lock.busyPct <= opt.maxBusy && lock.timeouts <= opt.maxTimeouts },
    c: { name: 'canary checks 3-5 (mesh publish+read, durable actor round trip, forced preparation failure recovers)',
      pass: canaryResults.length > 0 && canaryResults.every(item => item.pass) },
    memory: { name: `total PSS <= ${opt.memBudgetMb} MB`, pass: !memoryExceeded && memory.peakTotalMb <= opt.memBudgetMb },
  };
  const pass = Object.values(verdicts).every(verdict => verdict.pass);
  Object.assign(result, { pass, verdicts, lock, lease: leaseCheck, canary: canaryResults, driver: driven, memory: { ...memory, budgetMb: opt.memBudgetMb },
    unexpectedExits, restarted, finishedAt: Date.now(), processes: procs.length });
  exitCode = pass ? 0 : 1;
} catch (error) {
  result.error = String(error?.stack ?? error);
  log(`HARNESS ERROR: ${error?.message ?? error}`);
  exitCode = 2;
} finally {
  await stop(procs.filter(entry => !entry.exited), 20_000).catch(() => undefined);
  writeJson(path.join(out, 'report.json'), result);
  fs.writeFileSync(path.join(out, 'report.md'), renderReport(result));
  fs.cpSync(dirs.logs, path.join(out, 'logs'), { recursive: true });
  fs.cpSync(dirs.state, path.join(out, 'state'), { recursive: true, filter: source => !source.endsWith('.cursor') });
  fs.cpSync(dirs.stats, path.join(out, 'stats'), { recursive: true });
  if (!opt.keep) fs.rmSync(root, { recursive: true, force: true });
  log(`${exitCode === 0 ? 'PASS' : exitCode === 1 ? 'FAIL' : 'ERROR'}: report ${path.join(out, 'report.md')}${opt.keep ? `; root kept at ${root}` : ''}`);
  process.exit(exitCode);
}

function renderReport(r) {
  if (r.error && !r.verdicts) return `# Fabric full-load shadow test: ERROR\n\nCandidate ${r.candidate}\n\n\`\`\`\n${r.error}\n\`\`\`\n`;
  const mark = ok => ok ? 'PASS' : 'FAIL';
  const minutes = ((r.loadEndedAt - r.loadStartedAt) / 60_000).toFixed(1);
  const lines = [
    `# Fabric full-load shadow test: ${mark(r.pass)}`, '',
    `Candidate \`${r.candidate}\` (${r.release}), ${minutes} min at load; ${r.options.mains} Mains, ${r.options.hosts} resident hosts x ${r.options.actors} durable actors, ` +
      `${r.options.bridges} bridge pairs (restarted once: ${r.restarted}), ${r.options.spokeMains} spoke Mains, ${r.options.rate} events/s; ${r.processes} processes.`, '',
    '| check | verdict |', '|---|---|',
    ...Object.entries(r.verdicts).map(([key, verdict]) => `| (${key}) ${verdict.name} | ${mark(verdict.pass)} |`), '',
    '## (a) Leases and directory', '',
    `- expected participants: ${r.lease.expected}; max lease age ${r.lease.maxLeaseAgeMs} ms (limit ${r.options.maxLeaseS * 1_000}); max host-lease renewal gap ${r.lease.maxHostLeaseGapMs} ms`,
    `- samples over the limit: ${r.lease.overSamples} (${r.lease.participantsOverLimit} participants; ${r.lease.overAfterBridgeRestart} after the bridge restart)`,
    `- directory samples ${r.lease.directorySamples}, misses ${r.lease.directoryMisses} (${r.lease.missingParticipants} participants; ${r.lease.missesAfterBridgeRestart} after the bridge restart); spoke mirrors listed min ${r.lease.remoteMirrors?.min} max ${r.lease.remoteMirrors?.max}`,
    `- Mains' own confirm age max ${r.lease.mainMaxConfirmAgeMs} ms; write-stalled samples ${r.lease.mainStalledSamples}; unexpected exits ${r.unexpectedExits.length}`, '',
    '## (b) Mesh lock', '',
    `- source: ${r.lock.source}; busy ${r.lock.busyPct}% (peak minute ${r.lock.peakMinuteBusyPct}%), threshold ${r.options.maxBusy}%`,
    `- FABRIC_MESH_LOCK_TIMEOUT: ${r.lock.timeouts} (${r.lock.timeoutsSource}), threshold ${r.options.maxTimeouts}; instrumented timeouts in the load window ${r.lock.instrument.timeouts} (start ramp ${r.lock.instrument.rampTimeouts}, teardown ${r.lock.instrument.teardownTimeouts}, not judged), bounded tries ${r.lock.instrument.tries}`,
    `- sampler: busy ${r.lock.sampler.busyPct}% (peak minute ${r.lock.sampler.peakMinuteBusyPct}%) over ${r.lock.sampler.samples} samples`,
    ...(r.lock.l8 && !r.lock.l8.error ? [`- L8: ${r.lock.l8.n} acquisitions by ${r.lock.l8.processes} processes; wait p99 <= ${r.lock.l8.waitP99Ms} ms, max ${r.lock.l8.waitMaxMs} ms; hold p99 <= ${r.lock.l8.holdP99Ms} ms; tries ${r.lock.l8.tries}`,
      ...r.lock.l8.classes.map(row => `  - ${row.lockClass}: ${row.n} acq, busy ${Math.round(row.busyPct * 10) / 10}%, wait p99 <= ${row.waitP99Ms} ms, timeouts ${row.timeouts}, tries ${row.tries}`)] : []), '',
    '## (c) Canary (checks 3-5)', '',
    ...r.canary.map(round => `- round ${round.round}: ${mark(round.pass)}; ` + Object.entries(round.checks ?? {}).map(([name, check]) =>
      `${name} ${mark(check.pass)} ${check.ms} ms${check.readMs !== undefined ? ` (read ${check.readMs} ms, spoke ${check.spokeMs ?? 'n/a'} ms)` : ''}${check.recoveredByHeartbeatMs !== undefined ? ` (recovered by heartbeat in ${check.recoveredByHeartbeatMs} ms)` : ''}${check.error ? ` ERROR ${check.error.split('\n')[0]}` : ''}`).join('; ')), '',
    '## Load driver and memory', '',
    `- hub events ${r.driver.publish?.hub?.ok} ok / ${r.driver.publish?.hub?.failed} failed (p99 ${r.driver.publish?.hub?.p99Ms} ms, max ${r.driver.publish?.hub?.maxMs} ms, skipped ${r.driver.publish?.hub?.skipped}); spoke events ${r.driver.publish?.spoke?.ok} ok / ${r.driver.publish?.spoke?.failed} failed`,
    `- registry saves ${r.driver.saves?.ok} ok / ${r.driver.saves?.failed} failed (p50 ${r.driver.saves?.p50Ms} ms, p99 ${r.driver.saves?.p99Ms} ms, max ${r.driver.saves?.maxMs} ms, skipped ${r.driver.saves?.skipped})`,
    `- peak total PSS ${r.memory.peakTotalMb} MB (budget ${r.memory.budgetMb}; plain RSS sum peak ${r.memory.peakRssSumMb} MB); largest process ${r.memory.peakProcess} ${r.memory.peakProcessMb} MB PSS`,
    ...(r.driver.errors?.length ? ['', 'First driver errors:', ...r.driver.errors.slice(0, 5).map(item => `- ${item.message}`)] : []),
  ];
  return `${lines.join('\n')}\n`;
}
