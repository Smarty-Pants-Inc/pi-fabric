#!/usr/bin/env node
// Build a source snapshot with esbuild, then run this harness against its --module.
// Pin the parent with taskset for reproducible saturation; children inherit affinity/nice.
import { fork } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

const args = Object.fromEntries(process.argv.slice(2).map(arg => arg.replace(/^--/, '').split('=')));
if (args.worker === 'load') {
  process.send('ready');
  process.on('message', () => { for (;;) Math.sqrt(Math.random()); });
} else if (args.worker === 'lock') {
  const { MeshStore } = await import(pathToFileURL(args.module).href);
  const store = new MeshStore(args.root, 65536, 100, { lockTimeoutMs: 7000 });
  process.send('ready');
  process.once('message', async () => {
    const waits = [], holds = [], timeoutWaits = [];
    let timeouts = 0;
    try {
      for (let i = 0; i < Number(args.rounds); i++) {
        const start = performance.now();
        try {
          await store.exclusive(() => {
            waits.push(performance.now() - start);
            const heldAt = performance.now();
            const marker = path.join(args.root, 'critical');
            const fd = fs.openSync(marker, 'wx'); // overlapping holders fail loudly
            const cpu = process.cpuUsage();
            while ((process.cpuUsage(cpu).user + process.cpuUsage(cpu).system) / 1000 < Number(args.cpuMs)) Math.sqrt(Math.random());
            fs.closeSync(fd);
            fs.unlinkSync(marker);
            holds.push(performance.now() - heldAt);
          });
        } catch (error) {
          if (error.code !== 'FABRIC_MESH_LOCK_TIMEOUT') throw error;
          timeouts++;
          timeoutWaits.push(performance.now() - start);
        }
      }
      process.send({ waits, holds, timeouts, timeoutWaits });
    } catch (error) { process.send({ error: String(error.stack ?? error) }); process.exitCode = 1; }
    process.disconnect();
  });
} else {
  if (!args.module) throw new Error('Required: --module=/absolute/path/to/bundled-store.mjs');
  const n = Number(args.n ?? 24), rounds = Number(args.rounds ?? 8);
  const load = Number(args.load ?? 32), cpuMs = Number(args.cpuMs ?? 20);
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-lock-stress-'));
  const children = [], completions = [], reports = [];
  const launch = (worker, module = args.module) => new Promise((resolve, reject) => {
    const child = fork(import.meta.filename, [`--worker=${worker}`, `--module=${module}`, `--root=${root}`, `--rounds=${rounds}`, `--cpuMs=${cpuMs}`], { stdio: ['ignore', 'ignore', 'inherit', 'ipc'] });
    children.push(child);
    const completion = new Promise(done => child.once('exit', (code, signal) => done({ worker, code, signal })));
    completions.push(completion);
    child.on('error', reject);
    child.on('message', value => { if (value === 'ready') resolve(child); else reports.push(value); });
    child.once('exit', () => reject(new Error('Child exited before ready')));
  });
  const started = performance.now();
  // Deadline is a safety valve, not a detached background job. Cleanup joins every child.
  const timer = setTimeout(() => { for (const child of children) child.kill("SIGKILL"); }, rounds * 7000 + 60000);
  try {
    const burners = await Promise.all(Array.from({ length: load }, () => launch('load')));
    const legacyN = Number(args.legacyN ?? 0);
    if (legacyN && !args.legacyModule) throw new Error('legacyN requires --legacyModule');
    const workers = await Promise.all(Array.from({ length: n }, (_, index) => launch('lock', index < legacyN ? args.legacyModule : args.module)));
    for (const child of [...burners, ...workers]) child.send('go');
    const exits = await Promise.all(completions.slice(load));
    clearTimeout(timer);
    if (exits.some(exit => exit.code !== 0) || reports.some(report => report.error) || reports.length !== n) throw new Error(JSON.stringify({ exits, reports }));
    const summarize = values => {
      values.sort((a, b) => a - b);
      const at = q => Math.round((values[Math.min(values.length - 1, Math.ceil(q * values.length) - 1)] ?? 0) * 100) / 100;
      return { p50: at(.5), p99: at(.99), max: at(1), mean: Math.round(values.reduce((sum, value) => sum + value, 0) / Math.max(1, values.length) * 100) / 100 };
    };
    const queue = path.join(root, '.lock.q');
    const remainingTickets = fs.existsSync(queue) ? fs.readdirSync(queue).length : 0;
    if (remainingTickets !== 0 || fs.existsSync(path.join(root, '.lock'))) throw new Error('Leaked lock/tickets');
    console.log(JSON.stringify({ n, rounds, load, cpuMs, legacyN, budgetMs: 7000, acquisitions: reports.reduce((sum, r) => sum + r.waits.length, 0), timeouts: reports.reduce((sum, r) => sum + r.timeouts, 0), waitMs: summarize(reports.flatMap(r => r.waits)), timeoutWaitMs: summarize(reports.flatMap(r => r.timeoutWaits)), holdMs: summarize(reports.flatMap(r => r.holds)), elapsedMs: Math.round(performance.now() - started), mutualExclusion: true, remainingTickets }, null, 2));
  } finally {
    clearTimeout(timer);
    for (const child of children) if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL');
    await Promise.all(completions);
    fs.rmSync(root, { recursive: true, force: true });
  }
}
