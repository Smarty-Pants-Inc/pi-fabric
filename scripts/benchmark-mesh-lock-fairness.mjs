#!/usr/bin/env node
// Mesh .lock admission fairness reproduction (smarty-dev#6477 L0b). Synthetic only: never opens a
// live mesh. Run under `nice -n 19`. M processes contend through MeshLock.withLock with short holds
// (1-5 ms) and rare long ones (100-1900 ms); optional queue-blind contenders model old releases
// (--legacy) and short try-acquires (--try). Reports the handoff gap (release -> next acquire while
// someone waits), wait distribution, where each acquisition came from and timeouts.
//
//   node scripts/benchmark-mesh-lock-fairness.mjs --n=50 [--seconds=12] [--budgetMs=10000]
//     [--pLong=0.003] [--thinkMs=10] [--legacy=0] [--try=0] [--module=/abs/bundle.mjs] [--label=x]
//
// Without --module the current src/mesh/mesh-lock.ts is bundled with esbuild into a temp dir.
import { fork } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const self = fileURLToPath(import.meta.url);
const repo = path.resolve(path.dirname(self), '..');
const args = Object.fromEntries(process.argv.slice(2).map(arg => { const [k, v = 'true'] = arg.replace(/^--/, '').split('='); return [k, v]; }));
const now = () => Number(process.hrtime.bigint()) / 1e6; // host-wide monotonic on Linux
const sleep = ms => Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);

if (args.worker) {
  const { MeshLock } = await import(pathToFileURL(args.module).href);
  const counts = { readdir: 0, stat: 0 };
  const readdir = fs.readdirSync, stat = fs.statSync;
  fs.readdirSync = (...a) => { counts.readdir++; return readdir(...a); };
  fs.statSync = (...a) => { counts.stat++; return stat(...a); };
  const budget = Number(args.budgetMs);
  const lock = new MeshLock(args.root, { lockTimeoutMs: budget }, () => {});
  let seed = Number(args.index) * 7919 + 17;
  const random = () => { seed = (seed * 1103515245 + 12345) % 2147483648; return seed / 2147483648; };
  const legacyAcquire = async operation => {
    // Old-release contest: no queue, plain mkdir with 1-50 ms jitter (tests/fixtures/mesh-legacy-contender.mjs).
    const dir = path.join(args.root, '.lock'), owner = path.join(dir, 'owner');
    const record = `legacy-${process.pid}-${Math.random()}\n${process.pid}\n${Date.now()}\n`;
    const deadline = Date.now() + budget;
    for (;;) {
      try { fs.mkdirSync(dir); fs.writeFileSync(owner, record, { flag: 'wx' }); break; }
      catch (error) {
        if (error.code !== 'EEXIST') throw error;
        if (Date.now() >= deadline) throw Object.assign(new Error('legacy timeout'), { code: 'FABRIC_MESH_LOCK_TIMEOUT' });
        await new Promise(resolve => setTimeout(resolve, 1 + Math.floor(Math.random() * Math.min(50, deadline - Date.now()))));
      }
    }
    try { return operation(); } finally { fs.rmSync(dir, { recursive: true }); }
  };
  process.send('ready');
  process.once('message', async ({ stopAt }) => {
    const records = [];
    const cpu = process.cpuUsage();
    try {
      while (now() < stopAt) {
        await new Promise(resolve => setTimeout(resolve, random() * Number(args.thinkMs)));
        const hold = args.worker === 'queued' && random() < Number(args.pLong) ? 100 + random() * 1800 : 1 + random() * 4;
        const record = { kind: args.worker, worker: Number(args.index), req: now(), hold };
        const operation = () => {
          record.acq = now();
          const fd = fs.openSync(path.join(args.root, 'critical'), 'wx'); // overlapping holders fail loudly
          sleep(hold);
          fs.closeSync(fd);
          fs.unlinkSync(path.join(args.root, 'critical'));
          record.rel = now();
        };
        try {
          if (args.worker === 'legacy') await legacyAcquire(operation);
          else if (args.worker === 'try') await lock.withTryLock(() => lock.withLock(operation, budget, 'other'), 50);
          else await lock.withLock(operation, budget, 'main');
        } catch (error) {
          if (error.code !== 'FABRIC_MESH_LOCK_TIMEOUT') throw error;
          record.timeout = now();
          record.attempts = error.attempts;
        }
        records.push(record);
      }
      const used = process.cpuUsage(cpu);
      process.send({ records, counts, cpuMs: (used.user + used.system) / 1000 });
    } catch (error) { process.send({ error: String(error.stack ?? error) }); process.exitCode = 1; }
    process.disconnect();
  });
} else {
  const n = Number(args.n ?? 20), seconds = Number(args.seconds ?? 12), budgetMs = Number(args.budgetMs ?? 10000);
  const legacy = Number(args.legacy ?? 0), tries = Number(args.try ?? 0);
  const scratch = fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), 'mesh-fair-'));
  let module = args.module;
  if (!module) {
    const { build } = await import('esbuild');
    module = path.join(scratch, 'mesh-lock.mjs');
    await build({ stdin: { contents: 'export { MeshLock } from "./src/mesh/mesh-lock.ts"; export { meshLockQueueDirectory } from "./src/mesh/lock-queue.ts";', resolveDir: repo, loader: 'ts' }, bundle: true, platform: 'node', format: 'esm', outfile: module, logLevel: 'error' });
  }
  const root = path.join(scratch, 'mesh');
  fs.mkdirSync(root);
  const children = [], completions = [], reports = [];
  const launch = (worker, index) => new Promise((resolve, reject) => {
    const child = fork(self, [`--worker=${worker}`, `--index=${index}`, `--module=${module}`, `--root=${root}`, `--budgetMs=${budgetMs}`,
      `--pLong=${args.pLong ?? 0.003}`, `--thinkMs=${args.thinkMs ?? 10}`], { stdio: ['ignore', 'ignore', 'inherit', 'ipc'] });
    children.push(child);
    completions.push(new Promise(done => child.once('exit', code => done(code))));
    child.on('error', reject);
    child.on('message', value => { if (value === 'ready') resolve(child); else reports.push(value); });
    child.once('exit', () => reject(new Error('worker exited before ready')));
  });
  const safety = setTimeout(() => { for (const child of children) child.kill('SIGKILL'); }, (seconds + budgetMs / 1000) * 1000 + 120000);
  try {
    const kinds = [...Array(n).fill('queued'), ...Array(legacy).fill('legacy'), ...Array(tries).fill('try')];
    const workers = await Promise.all(kinds.map((kind, index) => launch(kind, index)));
    const stopAt = now() + 200 + seconds * 1000;
    for (const child of workers) child.send({ stopAt });
    const exits = await Promise.all(completions);
    if (exits.some(code => code !== 0) || reports.some(r => r.error) || reports.length !== kinds.length) throw new Error(JSON.stringify(reports.filter(r => r.error)));
    const all = reports.flatMap(r => r.records);
    const done = all.filter(r => r.acq !== undefined).sort((a, b) => a.acq - b.acq);
    for (let i = 1; i < done.length; i++) if (done[i].acq < done[i - 1].rel) throw new Error('mutual exclusion violated');
    const q = (values, p) => { const s = [...values].sort((a, b) => a - b); return s.length ? s[Math.min(s.length - 1, Math.ceil(p * s.length) - 1)] : 0; };
    const r1 = x => Math.round(x * 10) / 10;
    // Handoff gap: the lock was free although at least one taker was already waiting.
    const gaps = [];
    for (let i = 0; i + 1 < done.length; i++) {
      const rel = done[i].rel;
      const waiting = all.some(r => r !== done[i] && r.req < rel && (r.acq ?? r.timeout) > rel);
      if (waiting) gaps.push(done[i + 1].acq - rel);
    }
    const queued = all.filter(r => r.kind === 'queued');
    const waits = queued.map(r => (r.acq ?? r.timeout) - r.req);
    const fallbackMs = Math.min(10000, budgetMs * 0.8);
    const span = done.length ? done.at(-1).rel - done[0].acq : 1;
    const holdSum = done.reduce((s, r) => s + (r.rel - r.acq), 0);
    const gapSum = gaps.reduce((s, g) => s + g, 0);
    const result = {
      label: args.label ?? path.basename(module), n, legacy, try: tries, seconds, budgetMs,
      acquisitions: done.length, throughputPerS: r1(done.length / (span / 1000)), busyPct: r1(100 * holdSum / span),
      idleWhileWaitingPct: r1(100 * gapSum / span),
      handoffGapMs: { p50: r1(q(gaps, .5)), p99: r1(q(gaps, .99)), mean: r1(gapSum / Math.max(1, gaps.length)), max: r1(q(gaps, 1)) },
      queuedWaitMs: { p50: r1(q(waits, .5)), p99: r1(q(waits, .99)), max: r1(q(waits, 1)) },
      perWaiterMaxWaitMs: r1(Math.max(0, ...Array.from({ length: n }, (_, w) => Math.max(0, ...queued.filter(r => r.worker === w).map(r => (r.acq ?? r.timeout) - r.req))))),
      source: {
        queueHead: queued.filter(r => r.acq !== undefined && r.acq - r.req < fallbackMs).length,
        fallback: queued.filter(r => r.acq !== undefined && r.acq - r.req >= fallbackMs).length,
        legacyBarge: all.filter(r => r.kind === 'legacy' && r.acq !== undefined).length,
        tryBarge: all.filter(r => r.kind === 'try' && r.acq !== undefined).length,
      },
      timeouts: { queued: queued.filter(r => r.timeout).length, legacy: all.filter(r => r.kind === 'legacy' && r.timeout).length, try: all.filter(r => r.kind === 'try' && r.timeout).length },
      scan: { readdirPerS: r1(reports.reduce((s, r) => s + r.counts.readdir, 0) / (span / 1000)), statPerS: r1(reports.reduce((s, r) => s + r.counts.stat, 0) / (span / 1000)), workerCpuMsPerAcq: r1(reports.reduce((s, r) => s + r.cpuMs, 0) / Math.max(1, done.length)) },
    };
    console.log(JSON.stringify(result));
  } finally {
    clearTimeout(safety);
    for (const child of children) if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL');
    await Promise.all(completions);
    fs.rmSync(scratch, { recursive: true, force: true });
  }
}
