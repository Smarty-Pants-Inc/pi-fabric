#!/usr/bin/env node
// Full-load shadow soak for a Fabric release candidate against a baseline (smarty-dev#6477 stage 1).
// The fleet's shape: one hub and 4 spoke meshes, 5 bridge pairs (4 hub-spoke and 1 spoke-spoke),
// 80 hub Mains split between the baseline and the candidate release, fleet-sized hub state, CPU
// contention (run.sh's burner), the forwarder's bursty events, review wakes, a pin change that
// self-reloads the autoReload Mains, and a bridge restart. See README.md.
//   node orchestrate.mjs --release CANDIDATE [--baseline DIR] [--minutes 30] [--out DIR] [flags, see README]
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawn, spawnSync } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { argMap, delay, entryImport, readJson, residentHostId, residentRoot, writeJson } from './candidate.mjs';
import { gateMetrics, judge, marginsFrom, metricsOf, renderGate } from './gate.mjs';

const here = path.dirname(fileURLToPath(import.meta.url));
const args = argMap(process.argv.slice(2));
if (!args.release) { process.stderr.write('usage: orchestrate.mjs --release CANDIDATE [--baseline DIR] [--minutes N] [--out DIR] ...\n'); process.exit(2); }
const release = fs.realpathSync(args.release);
const baselineRelease = fs.realpathSync(args.baseline ?? args.release);
const releases = { baseline: baselineRelease, candidate: release };
const num = (name, fallback) => { const value = Number(args[name] ?? fallback); if (!Number.isFinite(value) || value < 0) throw new Error(`Bad --${name}`); return value; };
const opt = {
  minutes: num('minutes', 30), mains: num('mains', 80), mainsPerProcess: Math.max(1, num('mains-per-process', 8)), baselineFraction: Math.min(1, num('baseline-fraction', 0.5)),
  hosts: num('hosts', 15), actors: Math.max(2, num('actors', 10)),
  spokes: Math.max(1, num('spokes', 4)), spokeLinks: num('spoke-links', 1), spokeMains: Math.max(1, num('spoke-mains', 62)), spokeMainsPerProcess: Math.max(1, num('spoke-mains-per-process', 62)),
  rate: num('rate', 1), spokeRate: num('spoke-rate', 0.5), forwardPerMin: num('forward-per-min', 60), forwardBurst: Math.max(1, num('forward-burst', 6)),
  wakeS: num('wake-s', 5), actorSaveS: num('actor-save-s', 30), churnS: num('churn-s', 0), savesInFlight: Math.max(1, num('saves-in-flight', 1)),
  stateMb: num('state-mb', 4.8), hostLeases: num('host-leases', 600), seedParticipants: num('seed-participants', 20), actorRecords: num('actor-records', 300), actorRoots: num('actor-roots', 40),
  autoReloadFraction: Math.min(1, num('auto-reload-fraction', 0.5)), pinAt: num('pin-at', 0.4), reloadJitterS: num('reload-jitter-s', 10), restartAt: num('restart-at', 0.6),
  maxBusy: num('max-busy', 30), maxTimeouts: num('max-timeouts', 0), maxLeaseS: num('max-lease-s', 15), maxWaitP99S: num('max-wait-p99-s', 5), maxCountDriftPct: num('max-count-drift-pct', 2),
  memBudgetMb: num('mem-budget-mb', 8192), heapMb: num('heap-mb', 240), semiSpaceMb: num('semi-space-mb', 4), canaryEveryS: num('canary-every-s', 120),
  burnPct: num('burn-pct', 80), burnThreads: num('burn-threads', os.cpus().length), burnDuty: args['burn-duty'] === undefined || args['burn-duty'] === 'off' ? null : num('burn-duty', 0), profile: String(args.profile ?? 'none'), keep: Boolean(args.keep),
  // PI_FABRIC_LOCK_STATS for every child. Only releases with L8 lock stats read it, so with a
  // pre-L8 baseline this switches the candidate's lock stats (its Mains, hosts, spokes, bridges).
  lockStats: String(args['lock-stats'] ?? '1'),
};
// --reference REPORT.json: judge with the relative release gates (gate.mjs) instead of the absolute ones.
let reference = null;
const gateMargins = marginsFrom(args, true);
if (args.reference) {
  try { reference = { file: path.resolve(String(args.reference)), metrics: metricsOf(path.resolve(String(args.reference))) }; }
  catch (error) { process.stderr.write(`Bad --reference: ${error?.message ?? error}\n`); process.exit(2); }
}
if (opt.hosts > opt.mains) throw new Error('--hosts cannot exceed --mains: each resident host belongs to a Main');
for (const dir of new Set(Object.values(releases))) {
  for (const entry of ['dist/mesh.js', 'dist/participants-cli.js', 'dist/residency/host.js', 'dist/residency/actor-client.js', 'dist/fabric-runtime-state.js', 'bin/mesh-bridge']) {
    if (!fs.existsSync(path.join(dir, entry))) { process.stderr.write(`Not a built Fabric release: ${path.join(dir, entry)} is missing\n`); process.exit(2); }
  }
}
const gitHead = dir => spawnSync('git', ['-C', dir, 'rev-parse', 'HEAD'], { encoding: 'utf8' }).stdout?.trim() || path.basename(dir);
const candidateId = gitHead(release);
const baselineId = gitHead(baselineRelease);
const root = fs.mkdtempSync(path.join(os.tmpdir(), 'shadow-load-'));
const spokeNames = Array.from({ length: opt.spokes }, (_, index) => `spoke${index + 1}`);
const dirs = Object.fromEntries(['hub', ...spokeNames, 'project', 'hosts', 'logs', 'stats', 'state', 'home', 'tmp', 'profile', 'canary']
  .map(name => [name, path.join(root, name)]));
for (const directory of Object.values(dirs)) fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
const out = path.resolve(args.out ?? path.join(process.cwd(), `shadow-load-${candidateId.slice(0, 12)}-vs-${baselineId.slice(0, 8)}-${new Date().toISOString().replace(/[:.]/g, '-')}`));
fs.mkdirSync(out, { recursive: true });
const log = message => {
  const line = `[shadow ${new Date().toISOString().slice(11, 19)}] ${message}`;
  process.stdout.write(`${line}\n`);
  fs.appendFileSync(path.join(out, 'progress.log'), `${line}\n`);
};

// Every child runs candidate (or baseline) code with a bounded heap and the lock instrument preloaded.
const baseEnv = {
  PATH: process.env.PATH, LANG: process.env.LANG ?? 'C.UTF-8', HOME: dirs.home, TMPDIR: dirs.tmp, PI_CODING_AGENT_DIR: dirs.profile,
  // A small young generation keeps each process's RSS near its live heap: V8 otherwise grows
  // semi-spaces to 32+ MB per process before collecting the parsed mesh state it churns through.
  NODE_OPTIONS: `--max-old-space-size=${opt.heapMb} --max-semi-space-size=${opt.semiSpaceMb} --import=${pathToFileURL(path.join(here, 'instrument.mjs')).href}`,
  SHADOW_RELEASE: release, SHADOW_STATS_DIR: dirs.stats, SHADOW_HUB: dirs.hub, PI_FABRIC_LOCK_STATS: opt.lockStats,
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
const runToEnd = async (name, argv, timeoutMs) => {
  const entry = launch(name, argv);
  entry.stopping = true;
  await waitUntil(name, timeoutMs, () => entry.exited);
  if (entry.exited.code !== 0) throw new Error(`${name} exited ${entry.exited.code}; see logs/${name}.log`);
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

// Host CPU over a window, from run.sh's burner samples (the burner measures the whole host).
const cpuWindow = (from, to) => {
  const stats = args['burner-stats'] ? readJson(args['burner-stats']) : undefined;
  const rows = (stats?.samples ?? []).filter(row => row.at >= from && row.at <= to);
  const mean = key => { const values = rows.map(row => row[key]).filter(Number.isFinite); return values.length ? Math.round(10 * values.reduce((a, b) => a + b, 0) / values.length) / 10 : null; };
  return { burner: stats ? { targetPct: stats.targetPct, threads: stats.threads, sliceMs: stats.sliceMs, fixedDuty: stats.fixedDuty ?? null } : null, samples: rows.length,
    busyPct: mean('busyPct'), psiSome10: mean('psiSome10'), duty: mean('duty'), psiMax: Math.max(0, ...rows.map(row => row.psiSome10 ?? 0)) };
};
// Hub lock wait/hold over complete minutes [from, to], summed across every process's instrument.
const instrumentLock = (procStats, fromMs, toMs) => {
  const lo = Math.ceil(fromMs / 60_000), hi = Math.floor(toMs / 60_000) - 1;
  const bounds = procStats.find(row => row.lock)?.lock.bounds ?? [];
  const hist = bounds.map(() => 0);
  const minutes = new Map();
  let n = 0, holdMs = 0, maxWaitMs = 0, maxHoldMs = 0;
  for (const row of procStats) {
    for (const [key, bucket] of Object.entries(row.lock?.minutes ?? {})) {
      const minute = Number(key);
      if (minute < lo || minute > hi) continue;
      n += bucket.n; holdMs += bucket.holdMs;
      maxWaitMs = Math.max(maxWaitMs, bucket.maxWaitMs); maxHoldMs = Math.max(maxHoldMs, bucket.maxHoldMs);
      bucket.hist.forEach((count, index) => { hist[index] += count; });
      const m = minutes.get(minute) ?? { holdMs: 0, n: 0 };
      m.holdMs += bucket.holdMs; m.n += bucket.n;
      minutes.set(minute, m);
    }
  }
  const quantile = q => {
    let seen = 0;
    for (let index = 0; index < hist.length; index++) { seen += hist[index]; if (seen >= q * n) return bounds[index] ?? Infinity; }
    return null;
  };
  const count = Math.max(0, hi - lo + 1);
  const timeouts = procStats.flatMap(row => row.times ?? []).filter(at => at >= lo * 60_000 && at < (hi + 1) * 60_000).length;
  return { minutes: count, n, busyPct: count ? Math.round(1000 * holdMs / (count * 60_000)) / 10 : null,
    peakMinuteBusyPct: Math.max(0, ...[...minutes.values()].map(m => Math.round(1000 * m.holdMs / 60_000) / 10)),
    waitP50Ms: n ? quantile(0.5) : null, waitP99Ms: n ? quantile(0.99) : null, maxWaitMs, maxHoldMs, timeouts,
    timeoutsPerMin: count ? Math.round(10 * timeouts / count) / 10 : null, perMinute: [...minutes.entries()].sort((a, b) => a[0] - b[0])
      .map(([minute, m]) => ({ minute: new Date(minute * 60_000).toISOString().slice(11, 16), n: m.n, busyPct: Math.round(1000 * m.holdMs / 60_000) / 10 })) };
};
const readProcStats = () => fs.readdirSync(dirs.stats).filter(name => name.endsWith('.json')).map(name => readJson(path.join(dirs.stats, name))).filter(Boolean);

const range = count => Array.from({ length: count }, (_, index) => index);
const newMain = (index, prefix) => { const sessionId = randomUUID(); return { id: `session:${sessionId}`, sessionId, name: `${prefix}-${String(index).padStart(2, '0')}` }; };
const baselineCount = Math.round(opt.mains * opt.baselineFraction);
const autoReloadCount = Math.round(opt.mains * opt.autoReloadFraction);
// The baseline half comes first, so autoReload (the first --auto-reload-fraction of the Mains) is
// ON for the baseline Mains: at the pin change they follow the pin onto the candidate, as a
// rollout's Mains do. The candidate half runs with autoReload off.
const plan = { candidate: candidateId, baseline: baselineId, releases, createdAt: Date.now(),
  mains: range(opt.mains).map(index => ({ ...newMain(index, 'shadow-main'), slot: index < baselineCount ? 'baseline' : 'candidate', autoReload: index < autoReloadCount })),
  spokes: spokeNames.map((name, index) => ({ index: index + 1, name, mesh: dirs[name], list: `${name}Mains` })) };
for (const spoke of plan.spokes) plan[spoke.list] = range(opt.spokeMains).map(index => newMain(index, `${spoke.name}-main`));
plan.spokeMains = plan.spoke1Mains;
plan.hosts = range(opt.hosts).map(index => {
  // Resident hosts belong to Mains spread over both halves, so both releases run hosts.
  const main = plan.mains[Math.floor(index * opt.mains / opt.hosts)];
  const actors = range(opt.actors).map(slot => ({ id: randomUUID().replaceAll('-', ''), name: `h${index}-a${slot}` }));
  return { index, rootId: main.id, sessionId: main.sessionId, mainName: main.name, slot: main.slot, hostId: residentHostId(main.id),
    residencyRoot: residentRoot(dirs.hub, main.id), actors, loadActors: actors.slice(0, -1).map(actor => actor.id), canaryActor: actors.at(-1).id };
});
const planFile = path.join(dirs.state, 'plan.json');
const pinFile = path.join(dirs.state, 'pin.json');
writeJson(planFile, plan);
const common = ['--release', release, '--plan', planFile];

const result = { candidate: candidateId, baseline: baselineId, release, baselineRelease, options: opt, root, startedAt: Date.now(), phases: {}, markers: [] };
let exitCode = 2;
const all = { mains: [], hosts: [], spoke: [], bridges: [], aux: [] };
const mainsOuts = [];
try {
  log(`candidate ${candidateId} (${release}) vs baseline ${baselineId} (${baselineRelease}); throwaway root ${root}`);
  log(`load: ${opt.mains} hub Mains (${baselineCount} baseline, ${opt.mains - baselineCount} candidate; ${autoReloadCount} autoReload), ${opt.hosts} resident hosts x ${opt.actors} actors, ` +
    `${opt.spokes} spokes x ${opt.spokeMains} Mains, ${opt.spokes + opt.spokeLinks} bridge pairs, ${opt.rate}/s + forwarder ${opt.forwardPerMin}/min, state ${opt.stateMb} MB, ${opt.minutes} min`);

  // 0. Fleet-sized state before anyone starts: state.json padded through the release's own writes.
  const seedStarted = Date.now();
  await runToEnd('seed-state', [path.join(here, 'seed.mjs'), ...common, '--phase', 'state', '--hub', dirs.hub, '--state-mb', String(opt.stateMb),
    '--out', path.join(dirs.state, 'seed-state.json')], 600_000);
  result.seed = { state: readJson(path.join(dirs.state, 'seed-state.json')) };
  result.phases.seedStateS = Math.round((Date.now() - seedStarted) / 100) / 10;
  log(`hub state.json seeded to ${result.seed.state?.stateBytes} bytes in ${result.phases.seedStateS} s`);

  // 1. Mains: real participant directories with their release's 5 s heartbeat, staggered like logins.
  const mainsStarted = Date.now();
  const blocks = [];
  for (const slot of ['baseline', 'candidate']) {
    const indexes = plan.mains.flatMap((main, index) => main.slot === slot ? [index] : []);
    for (let k = 0; k < indexes.length; k += opt.mainsPerProcess) blocks.push({ slot, from: indexes[k], count: Math.min(opt.mainsPerProcess, indexes.length - k) });
  }
  for (const [index, block] of blocks.entries()) {
    const outFile = path.join(dirs.state, `mains-${index}.json`);
    mainsOuts.push(outFile);
    all.mains.push(launch(`mains-${index}`, [path.join(here, 'mains.mjs'), '--release', releases[block.slot], '--plan', planFile, '--mesh', dirs.hub, '--list', 'mains',
      '--from', String(block.from), '--count', String(block.count), '--cwd', dirs.project, '--out', outFile, '--slot', block.slot,
      '--pin', pinFile, '--pin-release', release, '--reload-jitter-ms', String(opt.reloadJitterS * 1_000), '--churn-s', String(opt.churnS)], { SHADOW_RELEASE: releases[block.slot] }));
    if (index % 10 === 9) await delay(1_000);
  }
  await waitUntil('Mains ready', 300_000, () => all.mains.every((entry, index) => fs.existsSync(`${mainsOuts[index]}.ready`) || entry.exited) && true);
  if (all.mains.some(entry => entry.exited)) throw new Error('a Main process exited during start; see logs');
  result.phases.mainsReadyS = Math.round((Date.now() - mainsStarted) / 100) / 10;
  log(`${opt.mains} Mains up in ${result.phases.mainsReadyS} s`);

  // 2. Resident hosts: their Main's release's dist/residency/host.js, each owning its durable actors.
  const defaultsBy = {};
  for (const slot of new Set(plan.hosts.map(host => host.slot))) defaultsBy[slot] = await entryImport(releases[slot], 'fabric-runtime-state.js', 'DEFAULT_FABRIC_CONFIG');
  const hostsStarted = Date.now();
  for (const host of plan.hosts) {
    const hostRelease = releases[host.slot];
    const defaults = defaultsBy[host.slot];
    const hostDir = path.join(dirs.hosts, String(host.index));
    const config = { format: 1, rootId: host.rootId, sessionId: host.sessionId, cwd: dirs.project, projectRoot: dirs.project, meshRoot: dirs.hub,
      actorRoot: path.join(hostDir, 'actors'), sessionActorRoot: path.join(hostDir, 'session-actors'), residencyRoot: host.residencyRoot,
      fullCodeMode: true, agents: { ...defaults.agents, budgetUsd: 0, nice: 19 }, mesh: { ...defaults.mesh, enabled: true, actorScope: 'project' },
      retention: defaults.retention, workerPath: path.join(hostRelease, 'dist/worker.js'), fabricExtensionPath: path.join(hostRelease, 'dist/index.js'),
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
    all.hosts.push(launch(`host-${host.index}`, [path.join(here, 'host.mjs'), '--release', hostRelease, '--config', configPath], {
      SHADOW_RELEASE: hostRelease, HOME: path.join(hostDir, 'home'), TMPDIR: path.join(hostDir, 'tmp'), PI_CODING_AGENT_DIR: path.join(hostDir, 'profile'),
      PI_FABRIC_NODE_BINARY: process.execPath, PI_FABRIC_MESH_ROOT: dirs.hub, PI_FABRIC_PROJECT_ROOT: dirs.project, PI_FABRIC_RUN_ROOT: path.join(hostDir, 'runs') }));
    await delay(500);
  }
  await waitUntil('resident hosts ready', 300_000, () => {
    for (const host of plan.hosts) {
      const error = readJson(path.join(host.residencyRoot, 'error.json'));
      if (error?.error) throw new Error(`host-${host.index}: ${error.error}`);
      if (all.hosts[host.index].exited) throw new Error(`host-${host.index} exited during start; see logs/host-${host.index}.log`);
    }
    return plan.hosts.every(host => readJson(path.join(host.residencyRoot, 'owner.json'))?.readyAt);
  });
  result.phases.hostsReadyS = Math.round((Date.now() - hostsStarted) / 100) / 10;
  log(`${opt.hosts} resident hosts ready in ${result.phases.hostsReadyS} s`);

  // 3. The spokes: their own Mains (the candidate's code), linked by real mesh-bridge run/agent
  // pairs over a local pipe: one per spoke to the hub, plus spoke-spoke links (Ryzen 2<->Ryzen 3).
  const spokeStarted = Date.now();
  const spokeOuts = [];
  for (const spoke of plan.spokes) {
    for (let from = 0; from < opt.spokeMains; from += opt.spokeMainsPerProcess) {
      const outFile = path.join(dirs.state, `${spoke.name}-mains-${from}.json`);
      spokeOuts.push(outFile);
      all.spoke.push(launch(`${spoke.name}-mains-${from}`, [path.join(here, 'mains.mjs'), ...common, '--mesh', spoke.mesh, '--list', spoke.list, '--from', String(from),
        '--count', String(Math.min(opt.spokeMainsPerProcess, opt.spokeMains - from)), '--cwd', dirs.project, '--out', outFile]));
    }
  }
  await waitUntil('spoke Mains ready', 300_000, () => spokeOuts.every(file => fs.existsSync(`${file}.ready`)));
  result.phases.spokesReadyS = Math.round((Date.now() - spokeStarted) / 100) / 10;
  log(`${opt.spokes * opt.spokeMains} spoke Mains up in ${result.phases.spokesReadyS} s`);
  const bridgeDefs = [
    ...plan.spokes.map(spoke => ({ id: `hub-${spoke.name}`, mesh: dirs.hub, name: `hub${spoke.index}`, remote: spoke.name, remoteMesh: spoke.mesh })),
    ...range(opt.spokeLinks).map(link => [2 + 2 * link, 3 + 2 * link]).filter(([, b]) => b <= opt.spokes)
      .map(([a, b]) => ({ id: `spoke${a}-spoke${b}`, mesh: dirs[`spoke${a}`], name: `s${a}to${b}`, remote: `s${b}to${a}`, remoteMesh: dirs[`spoke${b}`] })),
  ];
  const bridgeArgs = def => [path.join(release, 'bin/mesh-bridge'), 'run', '--mesh', def.mesh, '--name', def.name, '--remote', def.remote,
    '--cursor', path.join(dirs.state, `bridge-${def.id}.cursor`), '--', 'env', `SHADOW_ROLE=bridge-agent-${def.id}`, process.execPath,
    path.join(release, 'bin/mesh-bridge'), 'agent', '--mesh', def.remoteMesh, '--peer', def.name];
  for (const def of bridgeDefs) all.bridges.push(launch(`bridge-${def.id}`, bridgeArgs(def)));
  result.bridges = bridgeDefs.map(def => def.id);

  // 4. Seeded hosts and records (lease renewal keeps them live), then the expected directory.
  const seedOut = path.join(dirs.state, 'seed-keep.json');
  all.aux.push(launch('seed-keeper', [path.join(here, 'seed.mjs'), ...common, '--phase', 'keep', '--hub', dirs.hub, '--host-leases', String(opt.hostLeases),
    '--participants', String(opt.seedParticipants), '--actor-records', String(Math.max(0, opt.actorRecords - opt.hosts * opt.actors)),
    '--actor-roots', String(Math.max(0, opt.actorRoots - opt.hosts)), '--out', seedOut]));
  await waitUntil('seed keeper ready', 300_000, () => readJson(seedOut)?.ready);
  result.seed.keep = readJson(seedOut);
  log(`seeded ${result.seed.keep.seededLeases} host leases (${result.seed.keep.existingLeases} live), ${result.seed.keep.seededParticipants} participants, ` +
    `${result.seed.keep.actorRecords} actor records on ${result.seed.keep.actorRoots} roots`);
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
  const driver = launch('driver', [path.join(here, 'driver.mjs'), ...common, '--hub', dirs.hub, '--spokes', plan.spokes.map(spoke => spoke.mesh).join(','),
    '--rate', String(opt.rate), '--spoke-rate', String(opt.spokeRate), '--forward-per-min', String(opt.forwardPerMin), '--forward-burst', String(opt.forwardBurst),
    '--wake-s', String(opt.wakeS), '--actor-save-s', String(opt.actorSaveS), '--saves-in-flight', String(opt.savesInFlight), '--out', driverOut]);
  all.aux.push(observer, driver);

  // 5. Full load for N minutes: the pin change, the bridge restart, a canary round every --canary-every-s.
  const loadStart = Date.now();
  result.loadStartedAt = loadStart;
  const loadEnd = loadStart + opt.minutes * 60_000;
  const pinAt = loadStart + opt.minutes * 60_000 * opt.pinAt;
  const restartAt = loadStart + opt.minutes * 60_000 * opt.restartAt;
  let pinned = false;
  let restarted = bridgeDefs.length === 0;
  const canaries = [];
  let canary;
  const runCanary = () => {
    const round = canaries.length;
    const outFile = path.join(dirs.state, `canary-${round}.json`);
    const entry = launch(`canary-${round}`, [path.join(here, 'canary.mjs'), ...common, '--hub', dirs.hub, '--spoke', dirs.spoke1, '--scratch', dirs.canary,
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
    if (!pinned && now >= pinAt) {
      pinned = true;
      writeJson(pinFile, { slot: 'candidate', release, at: Date.now() });
      result.markers.push({ event: 'pin-change', at: Date.now(), to: candidateId });
      log(`pin -> candidate ${candidateId}: ${plan.mains.filter(main => main.autoReload && main.slot !== 'candidate').length} autoReload Mains self-reload`);
    }
    if (!restarted && now >= restartAt) {
      restarted = true;
      const at = Date.now();
      result.markers.push({ event: 'bridge-restart', at });
      log(`restarting ${all.bridges.length} bridges`);
      await stop(all.bridges, 15_000);
      const stoppedAt = Date.now();
      await delay(2_000);
      all.bridges = bridgeDefs.map(def => launch(`bridge-${def.id}`, bridgeArgs(def)));
      result.markers.push({ event: 'bridge-restarted', at: Date.now(), stopMs: stoppedAt - at });
    }
    if (now >= nextCanary && (!canary || canary.exited)) { canary = runCanary(); nextCanary = now + opt.canaryEveryS * 1_000; }
    if (now >= nextProgress) {
      nextProgress = now + 60_000;
      const seen = readJson(observerOut);
      const done = canaries.map(item => readJson(item.outFile)).filter(Boolean);
      const minute = instrumentLock(readProcStats(), now - 120_000, now);
      const cpu = cpuWindow(now - 60_000, now);
      const count = seen?.directory?.counts?.at(-1);
      log(`t+${Math.round((now - loadStart) / 60_000)} min: PSS ${Math.round(total)} MB (peak ${memory.peakTotalMb}), host busy ${cpu.busyPct ?? '?'}% PSI ${cpu.psiSome10 ?? '?'}, ` +
        `lock busy ${minute.busyPct ?? '?'}% (sampler ${seen?.lock?.busyPct ?? '?'}%), wait p99 <= ${minute.waitP99Ms ?? '?'} ms, timeouts ${minute.timeouts} last min, ` +
        `lease max ${seen?.lease?.maxAgeMs ?? '?'} ms, participants ${count?.live ?? '?'} (${count?.remote ?? '?'} remote), directory misses ${seen?.directory?.misses ?? '?'}, ` +
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
  const cpu = cpuWindow(loadStart, result.loadEndedAt);
  // A final canary round at full load, then wind down.
  if (canary && !canary.exited) await waitUntil('canary round', 120_000, () => canary.exited);
  if (!memoryExceeded) { canary = runCanary(); await waitUntil('final canary round', 120_000, () => canary.exited); }
  await stop(all.aux, 15_000);
  const stopStart = Date.now();
  await stop([...all.bridges, ...all.spoke, ...all.hosts, ...all.mains], 45_000);
  result.phases.stopS = Math.round((Date.now() - stopStart) / 100) / 10;

  // Lock metrics: the candidate's own L8 lock stats when it records them, else the instrument.
  const observed = readJson(observerOut) ?? {};
  const driven = readJson(driverOut) ?? {};
  const procStats = readProcStats();
  // Only the load window counts: the start ramp (every Main joining at once) and the teardown
  // (every directory closing at once) are harness artifacts, reported separately.
  const allTimes = procStats.flatMap(row => row.times ?? []);
  const instrumentTimeouts = allTimes.filter(at => at >= loadStart && at <= result.loadEndedAt).length;
  const rampTimeouts = allTimes.filter(at => at < loadStart).length;
  const teardownTimeouts = allTimes.filter(at => at > result.loadEndedAt).length;
  const instrumentTries = procStats.reduce((sum, row) => sum + row.tries, 0);
  const instrumented = instrumentLock(procStats, loadStart, result.loadEndedAt);
  const mainsHealth = mainsOuts.flatMap(file => Object.values(readJson(file)?.health ?? {}));
  const canaryResults = canaries.map(item => readJson(item.outFile) ?? { round: item.round, pass: false, error: `no result (exit ${JSON.stringify(item.entry.exited)})` });
  const restartMarker = result.markers.find(marker => marker.event === 'bridge-restart')?.at;
  const afterRestart = list => restartMarker ? (list ?? []).filter(event => event.at >= restartMarker).length : 0;

  // 7. Verdicts.
  // With PI_FABRIC_LOCK_STATS=0 the stats CLI finds no records: the instrument is the source then.
  const hasL8 = Boolean(l8 && !l8.error && l8.n > 0);
  const lock = {
    source: hasL8 ? 'L8 lock-stats (candidate)' : 'instrument (candidate predates L8 lock stats): owner-record wait, hold sum',
    busyPct: hasL8 ? Math.round(l8.busyPct * 10) / 10 : instrumented.busyPct ?? observed.lock?.busyPct ?? null,
    peakMinuteBusyPct: hasL8 ? Math.round(l8.peakMinuteBusyPct * 10) / 10 : instrumented.peakMinuteBusyPct ?? observed.lock?.peakMinuteBusyPct ?? null,
    timeouts: hasL8 ? l8.timeouts : instrumentTimeouts,
    timeoutsPerMin: Math.round(10 * (hasL8 ? l8.timeouts / Math.max(1, l8.minutes ?? loadMinutes) : instrumentTimeouts / Math.max(1 / 60, (result.loadEndedAt - loadStart) / 60_000))) / 10,
    waitP99Ms: hasL8 ? l8.waitP99Ms : instrumented.waitP99Ms,
    timeoutsSource: hasL8 ? `L8 lock-stats, last ${loadMinutes} complete load minutes` : 'instrumented FABRIC_MESH_LOCK_TIMEOUT rejections in the load window',
    l8: hasL8 ? { minutes: l8.minutes, processes: l8.processes, n: l8.n, timeouts: l8.timeouts, tries: l8.tries, busyPct: l8.busyPct,
      peakMinuteBusyPct: l8.peakMinuteBusyPct, waitMeanMs: l8.waitMeanMs, waitP99Ms: l8.waitP99Ms, waitMaxMs: l8.waitMaxMs,
      holdMeanMs: l8.holdMeanMs, holdP99Ms: l8.holdP99Ms, holdMaxMs: l8.holdMaxMs,
      classes: (l8.classes ?? []).map(row => ({ lockClass: row.lockClass, n: row.n, busyPct: row.busyPct, holdP99Ms: row.holdP99Ms, waitP99Ms: row.waitP99Ms, waitMaxMs: row.waitMaxMs, timeouts: row.timeouts, tries: row.tries })) } : l8,
    instrumented,
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
  // Participant count: every sample after the first load minute against the window's median.
  const counts = (observed.directory?.counts ?? []).filter(row => row.at >= loadStart + 60_000 && row.at <= result.loadEndedAt).map(row => row.live);
  const median = counts.length ? [...counts].sort((a, b) => a - b)[Math.floor(counts.length / 2)] : null;
  const participantCount = { samples: counts.length, median, min: counts.length ? Math.min(...counts) : null, max: counts.length ? Math.max(...counts) : null,
    maxDriftPct: median ? Math.round(1000 * Math.max(...counts.map(value => Math.abs(value - median))) / median) / 10 : null };
  const reloadsDue = mainsHealth.filter(row => row.autoReload && row.slot !== 'candidate').length;
  const reloads = mainsHealth.flatMap(row => row.reloads ?? []);
  const reloadMs = reloads.filter(row => row.ms !== undefined).map(row => row.ms).sort((a, b) => a - b);
  const pin = { pinned, due: reloadsDue, done: reloads.filter(row => !row.error && row.ms !== undefined).length, errors: reloads.filter(row => row.error).map(row => row.error).slice(0, 5),
    publishErrors: reloads.filter(row => row.publishError).length, p50Ms: reloadMs[Math.floor(reloadMs.length / 2)] ?? null, maxMs: reloadMs.at(-1) ?? null };
  const verdicts = {
    a: { name: `live leases renew within ${opt.maxLeaseS} s and every participant stays listed (incl. after the pin change and bridge restart)`,
      pass: leaseCheck.expected > 0 && leaseCheck.directorySamples > 0 && leaseCheck.maxLeaseAgeMs !== null && leaseCheck.maxLeaseAgeMs <= opt.maxLeaseS * 1_000 &&
        leaseCheck.directoryMisses === 0 && restarted && unexpectedExits.length === 0 },
    b: { name: `lock busy <= ${opt.maxBusy}% and FABRIC_MESH_LOCK_TIMEOUT <= ${opt.maxTimeouts}`,
      pass: lock.busyPct !== null && lock.busyPct <= opt.maxBusy && lock.timeouts <= opt.maxTimeouts },
    c: { name: 'canary checks 3-5 (mesh publish+read, durable actor round trip, forced preparation failure recovers)',
      pass: canaryResults.length > 0 && canaryResults.every(item => item.pass) },
    d: { name: `participant count stable within +-${opt.maxCountDriftPct}%`, pass: participantCount.samples > 0 && participantCount.maxDriftPct <= opt.maxCountDriftPct },
    e: { name: `lock wait p99 < ${opt.maxWaitP99S} s`, pass: lock.waitP99Ms !== null && lock.waitP99Ms !== undefined && lock.waitP99Ms < opt.maxWaitP99S * 1_000 },
    f: { name: 'pin change: every autoReload Main self-reloaded onto the candidate', pass: pinned && pin.done === pin.due && pin.errors.length === 0 },
    memory: { name: `total PSS <= ${opt.memBudgetMb} MB`, pass: !memoryExceeded && memory.peakTotalMb <= opt.memBudgetMb },
  };
  const absolutePass = Object.values(verdicts).every(verdict => verdict.pass);
  Object.assign(result, { pass: absolutePass, verdicts, lock, lease: leaseCheck, participantCount, pin, cpu, canary: canaryResults, driver: driven,
    memory: { ...memory, budgetMb: opt.memBudgetMb }, unexpectedExits, restarted, finishedAt: Date.now(), processes: procs.length });
  // The figures the release gate judges, embedded so this report can serve as a --reference alone.
  try { result.metrics = gateMetrics(result, observed); }
  catch (error) { result.metricsError = String(error?.stack ?? error); log(`gate metrics failed (rerun gate.mjs on report.json): ${error?.message ?? error}`); }
  if (reference && result.metrics) {
    const gate = judge(result.metrics, reference.metrics, gateMargins);
    result.gate = { reference: reference.file, referenceMetrics: reference.metrics, ...gate };
    result.absolutePass = absolutePass;
    result.pass = gate.pass;
  }
  exitCode = reference && !result.metrics ? 2 : result.pass ? 0 : 1;
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
  if (r.error && !r.verdicts) return `# Fabric full-load shadow test: ERROR\n\nCandidate ${r.candidate} vs baseline ${r.baseline}\n\n\`\`\`\n${r.error}\n\`\`\`\n`;
  const mark = ok => ok ? 'PASS' : 'FAIL';
  const minutes = ((r.loadEndedAt - r.loadStartedAt) / 60_000).toFixed(1);
  const o = r.options;
  const lines = [
    `# Fabric full-load shadow test: ${mark(r.pass)}${r.gate ? ' (release gate against the reference; absolute checks below are informational)' : ''}`, '',
    `Candidate \`${r.candidate}\` vs baseline \`${r.baseline}\`, ${minutes} min at load; ${o.mains} hub Mains (${Math.round(o.mains * o.baselineFraction)} baseline), ` +
      `${o.hosts} resident hosts x ${o.actors} actors, ${o.spokes} spokes x ${o.spokeMains} Mains, bridges ${r.bridges?.join(', ')} (restarted once: ${r.restarted}), ` +
      `${o.rate}/s + forwarder ${o.forwardPerMin}/min (bursts ~${o.forwardBurst}), wakes every ${o.wakeS} s; ${r.processes} processes.`, '',
    '| check | verdict |', '|---|---|',
    ...Object.entries(r.verdicts).map(([key, verdict]) => `| (${key}) ${verdict.name} | ${mark(verdict.pass)} |`), '',
    '## Load shape', '',
    `- host CPU busy ${r.cpu.busyPct}% (burner ${r.cpu.burner?.fixedDuty != null ? `fixed duty ${r.cpu.burner.fixedDuty}` : `target ${r.cpu.burner?.targetPct ?? 'off'}%`}, ${r.cpu.burner?.threads ?? 0} threads, mean duty ${r.cpu.duty}), CPU PSI some avg10 mean ${r.cpu.psiSome10}% (max ${r.cpu.psiMax}%)`,
    `- hub state.json ${r.seed?.state?.stateBytes} bytes; host leases ${(r.seed?.keep?.existingLeases ?? 0) + (r.seed?.keep?.seededLeases ?? 0)} (${r.seed?.keep?.seededLeases} seeded); ` +
      `seeded participants ${r.seed?.keep?.seededParticipants}; actor records ${o.hosts * o.actors} live + ${r.seed?.keep?.actorRecords} seeded on ${o.hosts} + ${r.seed?.keep?.actorRoots} roots`,
    `- participants listed (live): median ${r.participantCount.median}, min ${r.participantCount.min}, max ${r.participantCount.max}, max drift ${r.participantCount.maxDriftPct}% over ${r.participantCount.samples} samples; spoke mirrors listed min ${r.lease.remoteMirrors?.min} max ${r.lease.remoteMirrors?.max}`,
    `- pin change: ${r.pin.done}/${r.pin.due} self-reloads done (p50 ${r.pin.p50Ms} ms, max ${r.pin.maxMs} ms), errors ${r.pin.errors.length}${r.pin.errors.length ? ` (${r.pin.errors[0]})` : ''}, ops.fabric.reloaded publish failures ${r.pin.publishErrors}`, '',
    '## (a) Leases and directory', '',
    `- expected participants: ${r.lease.expected}; max lease age ${r.lease.maxLeaseAgeMs} ms (limit ${o.maxLeaseS * 1_000}); max host-lease renewal gap ${r.lease.maxHostLeaseGapMs} ms`,
    `- samples over the limit: ${r.lease.overSamples} (${r.lease.participantsOverLimit} participants; ${r.lease.overAfterBridgeRestart} after the bridge restart)`,
    `- directory samples ${r.lease.directorySamples}, misses ${r.lease.directoryMisses} (${r.lease.missingParticipants} participants; ${r.lease.missesAfterBridgeRestart} after the bridge restart)`,
    `- Mains' own confirm age max ${r.lease.mainMaxConfirmAgeMs} ms; write-stalled samples ${r.lease.mainStalledSamples}; unexpected exits ${r.unexpectedExits.length}`, '',
    '## (b, e) Mesh lock', '',
    `- source: ${r.lock.source}; busy ${r.lock.busyPct}% (peak minute ${r.lock.peakMinuteBusyPct}%), threshold ${o.maxBusy}%; wait p99 <= ${r.lock.waitP99Ms} ms (threshold ${o.maxWaitP99S * 1_000} ms)`,
    `- FABRIC_MESH_LOCK_TIMEOUT: ${r.lock.timeouts} (${r.lock.timeoutsPerMin}/min; ${r.lock.timeoutsSource}), threshold ${o.maxTimeouts}; instrumented timeouts in the load window ${r.lock.instrument.timeouts} (start ramp ${r.lock.instrument.rampTimeouts}, teardown ${r.lock.instrument.teardownTimeouts}, not judged), bounded tries ${r.lock.instrument.tries}`,
    `- instrument: ${r.lock.instrumented.n} acquisitions over ${r.lock.instrumented.minutes} complete minutes, busy ${r.lock.instrumented.busyPct}% (peak minute ${r.lock.instrumented.peakMinuteBusyPct}%), wait p50 <= ${r.lock.instrumented.waitP50Ms} ms, p99 <= ${r.lock.instrumented.waitP99Ms} ms, max ${r.lock.instrumented.maxWaitMs} ms; hold max ${r.lock.instrumented.maxHoldMs} ms`,
    `- sampler (.lock present): busy ${r.lock.sampler.busyPct}% (peak minute ${r.lock.sampler.peakMinuteBusyPct}%) over ${r.lock.sampler.samples} samples`,
    ...(r.lock.l8 && !r.lock.l8.error ? [`- L8: ${r.lock.l8.n} acquisitions by ${r.lock.l8.processes} processes; wait p99 <= ${r.lock.l8.waitP99Ms} ms, max ${r.lock.l8.waitMaxMs} ms; hold p99 <= ${r.lock.l8.holdP99Ms} ms; tries ${r.lock.l8.tries}`,
      ...r.lock.l8.classes.map(row => `  - ${row.lockClass}: ${row.n} acq, busy ${Math.round(row.busyPct * 10) / 10}%, wait p99 <= ${row.waitP99Ms} ms, timeouts ${row.timeouts}, tries ${row.tries}`)] : []), '',
    '## (c) Canary (checks 3-5)', '',
    ...r.canary.map(round => `- round ${round.round}: ${mark(round.pass)}; ` + Object.entries(round.checks ?? {}).map(([name, check]) =>
      `${name} ${mark(check.pass)} ${check.ms} ms${check.readMs !== undefined ? ` (read ${check.readMs} ms, spoke ${check.spokeMs ?? 'n/a'} ms)` : ''}${check.recoveredByHeartbeatMs !== undefined ? ` (recovered by heartbeat in ${check.recoveredByHeartbeatMs} ms)` : ''}${check.error ? ` ERROR ${check.error.split('\n')[0]}` : ''}`).join('; ')), '',
    '## Load driver and memory', '',
    `- hub events ${r.driver.publish?.hub?.ok} ok / ${r.driver.publish?.hub?.failed} failed (p99 ${r.driver.publish?.hub?.p99Ms} ms, max ${r.driver.publish?.hub?.maxMs} ms, skipped ${r.driver.publish?.hub?.skipped}); spoke events ${r.driver.publish?.spoke?.ok} ok / ${r.driver.publish?.spoke?.failed} failed`,
    `- forwarder ${r.driver.forward?.ok} ok / ${r.driver.forward?.failed} failed in ${r.driver.forward?.bursts} bursts (max ${r.driver.forward?.maxBurst}; p99 ${r.driver.forward?.p99Ms} ms); review wakes ${r.driver.wakes?.ok} ok / ${r.driver.wakes?.failed} failed (p99 ${r.driver.wakes?.p99Ms} ms)`,
    `- registry saves ${r.driver.saves?.ok} ok / ${r.driver.saves?.failed} failed (p50 ${r.driver.saves?.p50Ms} ms, p99 ${r.driver.saves?.p99Ms} ms, max ${r.driver.saves?.maxMs} ms, skipped ${r.driver.saves?.skipped})`,
    `- peak total PSS ${r.memory.peakTotalMb} MB (budget ${r.memory.budgetMb}; plain RSS sum peak ${r.memory.peakRssSumMb} MB); largest process ${r.memory.peakProcess} ${r.memory.peakProcessMb} MB PSS`,
    ...(r.driver.errors?.length ? ['', 'First driver errors:', ...r.driver.errors.slice(0, 5).map(item => `- ${item.message}`)] : []),
  ];
  const gate = r.gate ? ['', renderGate(r.gate, r.metrics, r.gate.referenceMetrics, { reference: r.gate.reference })] : [];
  return `${[...lines, ...gate].join('\n')}\n`;
}
