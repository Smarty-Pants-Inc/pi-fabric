#!/usr/bin/env node
// Mixed-release SQLite state writer load (smarty-dev#10516). Synthetic only: a private scratch mesh, never a live
// one. Each --group bundles src/mesh/state-sqlite.ts from a checkout (this repo's, or a `git worktree add` of an
// older release) and forks N writers of it; every writer of every group writes ONE shared state.db in a steady loop
// (think, then one writeBatch holding the writer lock 1-5 ms, rarely 100-1900 ms). Reports, per group: commits,
// full-budget lock timeouts, and the p50/p99/max wait for the writer lock.
//
//   node scripts/benchmark-mesh-state-mixed.mjs --group=old:/abs/checkout-8d0af4b0:8 --group=new:.:8
//     [--seconds=300] [--budgetMs=10000] [--thinkMs=20] [--pLong=0.002] [--blockers=0] [--blockP=0.02] [--out=FILE]
//
// Stalled heads (--blockers=K): the first K writers of each group block their event loop for 1-5 s (a synchronous
// busy-wait, as a busy Main's is) on a --blockP share of their writes, at the moment their ticket becomes the FIFO
// head. A blocked head can neither heartbeat nor attempt; a write that never queues does not block.
import { fork } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { monitorEventLoopDelay } from 'node:perf_hooks';

const self = fileURLToPath(import.meta.url);
const argv = process.argv.slice(2);
const args = Object.fromEntries(argv.filter(arg => !arg.startsWith('--group=')).map(arg => { const [k, v = 'true'] = arg.replace(/^--/, '').split('='); return [k, v]; }));
const now = () => Number(process.hrtime.bigint()) / 1e6; // host-wide monotonic on Linux
const spin = ms => { const until = performance.now() + ms; while (performance.now() < until) { /* a synchronous hold */ } };

if (args.worker) {
  const { SqliteStateStore, meshLockQueueDirectory } = await import(pathToFileURL(args.module).href);
  const budget = Number(args.budgetMs);
  const store = await SqliteStateStore.open(args.root, 65536, 1000, { lockTimeoutMs: budget });
  const identity = { id: `bench-${process.pid}`, name: 'bench', kind: 'agent' };
  const queue = meshLockQueueDirectory(path.join(args.root, 'state.db'));
  const isHead = () => {
    try { return fs.readdirSync(queue).filter(name => /^\d{24}-\d+-/.test(name)).sort()[0]?.split('-')[1] === String(process.pid); }
    catch { return false; }
  };
  const blocker = Number(args.index) < Number(args.blockers);
  let seed = Number(args.index) * 7919 + 17;
  const random = () => { seed = (seed * 1103515245 + 12345) % 2147483648; return seed / 2147483648; };
  process.send('ready');
  process.once('message', async ({ stopAt }) => {
    const loop = monitorEventLoopDelay({ resolution: 1 });
    const waits = [];
    let commits = 0, timeouts = 0, headBlocks = 0, i = 0;
    try {
      loop.enable();
      while (now() < stopAt) {
        await new Promise(resolve => setTimeout(resolve, random() * Number(args.thinkMs)));
        const hold = random() < Number(args.pLong) ? 100 + random() * 1800 : 1 + random() * 4;
        let block = blocker && random() < Number(args.blockP) ? 1000 + random() * 4000 : 0;
        const watch = block ? setInterval(() => {
          if (!block || !isHead()) return;
          headBlocks += 1;
          spin(block);
          block = 0;
        }, 5) : undefined;
        const asked = now();
        let acquired;
        try {
          await store.writeBatch({ identity, ops: [], prepare: () => {
            acquired = now();
            spin(hold);
            return [{ kind: 'put', key: `bench/${args.worker}/${args.index}`, value: i++ }];
          } });
          commits += 1;
          waits.push(acquired - asked);
        } catch (error) {
          if (error.code !== 'FABRIC_MESH_LOCK_TIMEOUT') throw error;
          timeouts += 1;
          waits.push(now() - asked);
        } finally { clearInterval(watch); }
      }
      loop.disable();
      const stats = store.stats();
      process.send({ group: args.worker, index: Number(args.index), blocker, commits, timeouts, waits, headBlocks,
        loopP99Ms: loop.percentile(99) / 1e6, fifoJoins: stats.fifoJoins, busyRetries: stats.busyRetries });
    } catch (error) { process.send({ error: String(error.stack ?? error) }); process.exitCode = 1; }
    store.close();
    process.disconnect();
  });
} else {
  const groups = argv.filter(arg => arg.startsWith('--group=')).map(arg => {
    const [label, dir, n] = arg.slice('--group='.length).split(':');
    return { label, dir: path.resolve(dir), n: Number(n) };
  });
  if (!groups.length || groups.some(g => !g.label || !(g.n > 0))) throw new Error('usage: --group=label:/checkout:N (repeatable)');
  const seconds = Number(args.seconds ?? 300), budgetMs = Number(args.budgetMs ?? 10000);
  const profile = { thinkMs: Number(args.thinkMs ?? 20), pLong: Number(args.pLong ?? 0.002), blockers: Number(args.blockers ?? 0),
    blockP: Number(args.blockP ?? 0.02) };
  const scratch = fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), 'mesh-state-mixed-'));
  const { build } = await import('esbuild');
  for (const group of groups) {
    group.module = path.join(scratch, `${group.label}.mjs`);
    await build({ stdin: { contents: 'export { SqliteStateStore } from "./src/mesh/state-sqlite.ts";\n' +
      'export { meshLockQueueDirectory } from "./src/mesh/lock-queue.ts";', resolveDir: group.dir, loader: 'ts' },
    bundle: true, platform: 'node', format: 'esm', outfile: group.module, logLevel: 'error' });
  }
  const root = path.join(scratch, 'mesh');
  fs.mkdirSync(root, { mode: 0o700 });
  const { SqliteStateStore } = await import(pathToFileURL(groups[0].module).href);
  (await SqliteStateStore.open(root, 65536, 1000, { initialize: 'create' })).close();
  const children = [], completions = [], reports = [];
  const launch = (group, index) => new Promise((resolve, reject) => {
    const child = fork(self, [`--worker=${group.label}`, `--index=${index}`, `--module=${group.module}`, `--root=${root}`,
      `--budgetMs=${budgetMs}`, ...Object.entries(profile).map(([key, value]) => `--${key}=${value}`)],
    { stdio: ['ignore', 'ignore', 'inherit', 'ipc'], env: { ...process.env, PI_FABRIC_MESH_CHECKPOINT_LOG: 'off' } });
    children.push(child);
    completions.push(new Promise(done => child.once('exit', code => done(code))));
    child.on('error', reject);
    child.on('message', value => { if (value === 'ready') resolve(child); else reports.push(value); });
    child.once('exit', () => reject(new Error('worker exited before ready')));
  });
  const safety = setTimeout(() => { for (const child of children) child.kill('SIGKILL'); }, (seconds + budgetMs / 1000) * 1000 + 120000);
  try {
    // Interleave the groups' launches so neither starts with a head start.
    const order = [];
    for (let i = 0; i < Math.max(...groups.map(g => g.n)); i++) for (const group of groups) if (i < group.n) order.push([group, i]);
    const workers = await Promise.all(order.map(([group, index]) => launch(group, index)));
    const stopAt = now() + 500 + seconds * 1000;
    for (const child of workers) child.send({ stopAt });
    const exits = await Promise.all(completions);
    if (exits.some(code => code !== 0) || reports.some(r => r.error) || reports.length !== order.length) throw new Error(JSON.stringify(reports.filter(r => r.error)));
    const q = (values, p) => { const s = [...values].sort((a, b) => a - b); return s.length ? s[Math.min(s.length - 1, Math.ceil(p * s.length) - 1)] : 0; };
    const r1 = x => Math.round(x * 10) / 10;
    const sum = (rows, key) => rows.reduce((s, r) => s + r[key], 0);
    const result = { label: args.label ?? groups.map(g => `${g.label}x${g.n}`).join('+'), seconds, budgetMs, ...profile, groups: {} };
    const waitMs = rows => {
      const waits = rows.flatMap(r => r.waits);
      return { p50: r1(q(waits, .5)), p99: r1(q(waits, .99)), max: r1(q(waits, 1)), mean: r1(waits.reduce((s, w) => s + w, 0) / Math.max(1, waits.length)) };
    };
    for (const group of groups) {
      const mine = reports.filter(r => r.group === group.label);
      // `steady`: the writers that never block, so a blocker's own lost time does not hide what the queue did to them.
      const steady = mine.filter(r => !r.blocker);
      result.groups[group.label] = { checkout: group.dir, processes: group.n, commits: sum(mine, 'commits'), timeouts: sum(mine, 'timeouts'),
        timeoutsPerProcess: mine.map(r => r.timeouts), headBlocks: sum(mine, 'headBlocks'), waitMs: waitMs(mine),
        loopP99Ms: r1(Math.max(...mine.map(r => r.loopP99Ms))), fifoJoins: sum(mine, 'fifoJoins'),
        ...steady.length < mine.length ? { steady: { processes: steady.length, commits: sum(steady, 'commits'), timeouts: sum(steady, 'timeouts'),
          waitMs: waitMs(steady) } } : {} };
    }
    const text = JSON.stringify(result);
    if (args.out) fs.writeFileSync(args.out, text + '\n');
    console.log(text);
  } finally {
    clearTimeout(safety);
    for (const child of children) if (child.exitCode === null) child.kill('SIGKILL');
    fs.rmSync(scratch, { recursive: true, force: true });
  }
}
