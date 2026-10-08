#!/usr/bin/env node
// CPU contention for the shadow test (smarty-dev#6477 stage 1). Busy loops at nice 0, in one
// worker thread per CPU, duty-cycled so the WHOLE host (whatever else runs on it) stays near
// --target-pct busy: lock holders at nice 19 then wait for CPU as they do on Ryzen 1 (PSI 50-60%).
// run.sh starts it before the harness and stops it when the harness ends.
// With --duty D (0..1) the controller is off: every thread busy-loops a fixed D of each slice, so
// D x threads CPUs of nice-0 work always compete with the harness, however busy the host already is
// (on a host the harness alone saturates, the controller would back off to 0 and add nothing).
//   node burner.mjs --target-pct 80 [--threads N] [--slice-ms 50] [--duty D] [--stats FILE]
import fs from 'node:fs';
import os from 'node:os';
import { Worker, isMainThread, workerData } from 'node:worker_threads';
import { fileURLToPath } from 'node:url';

if (!isMainThread) {
  const duty = new Float64Array(workerData.shared);
  const sleeper = new Int32Array(new SharedArrayBuffer(4));
  const sliceMs = workerData.sliceMs;
  // A random phase per thread, so the threads do not all yield the CPUs at the same instant.
  Atomics.wait(sleeper, 0, 0, Math.random() * sliceMs);
  let sink = 0;
  for (;;) {
    const share = duty[0];
    if (share < 0) break;
    const start = performance.now();
    const busyMs = sliceMs * share;
    while (performance.now() - start < busyMs) for (let i = 0; i < 2_000; i++) sink += Math.sqrt(i);
    const rest = sliceMs - (performance.now() - start);
    if (rest > 0) Atomics.wait(sleeper, 0, 0, rest);
  }
  if (sink === -1) process.stdout.write('');
} else {
  const args = {};
  const argv = process.argv.slice(2);
  for (let i = 0; i < argv.length; i += 2) args[argv[i].replace(/^--/, '')] = argv[i + 1];
  const target = Number(args['target-pct'] ?? 80);
  const threads = Math.max(1, Number(args.threads ?? os.cpus().length));
  const sliceMs = Math.max(5, Number(args['slice-ms'] ?? 50));
  const shared = new SharedArrayBuffer(8);
  const duty = new Float64Array(shared);
  const fixedDuty = args.duty === undefined ? undefined : Math.max(0, Math.min(1, Number(args.duty)));
  if (fixedDuty !== undefined && !Number.isFinite(fixedDuty)) throw new Error('Bad --duty');
  duty[0] = fixedDuty ?? Math.min(1, target / 100 / 2);
  const workers = Array.from({ length: threads }, () => new Worker(fileURLToPath(import.meta.url), { workerData: { shared, sliceMs } }));
  const cpu = () => {
    const [, ...fields] = fs.readFileSync('/proc/stat', 'utf8').split('\n')[0].trim().split(/\s+/);
    const [user, nice, system, idle, iowait, irq, softirq, steal] = fields.map(Number);
    const total = user + nice + system + idle + iowait + irq + softirq + steal;
    return { busy: total - idle - iowait, total };
  };
  const psi = () => {
    try {
      const text = fs.readFileSync('/proc/pressure/cpu', 'utf8');
      return Number(/some avg10=([\d.]+)/.exec(text)?.[1] ?? NaN);
    } catch { return null; }
  };
  const samples = [];
  let last = cpu();
  const stats = { pid: process.pid, targetPct: target, threads, sliceMs, fixedDuty: fixedDuty ?? null, startedAt: Date.now(), samples };
  const save = () => {
    if (!args.stats) return;
    try { fs.writeFileSync(`${args.stats}.tmp`, JSON.stringify({ ...stats, savedAt: Date.now() })); fs.renameSync(`${args.stats}.tmp`, args.stats); }
    catch { /* best effort */ }
  };
  // Proportional control on the measured host busy %: other tenants and the harness itself count.
  setInterval(() => {
    const now = cpu();
    const busyPct = 100 * (now.busy - last.busy) / Math.max(1, now.total - last.total);
    last = now;
    if (fixedDuty === undefined) duty[0] = Math.max(0, Math.min(1, duty[0] + 0.4 * (target - busyPct) / 100));
    samples.push({ at: Date.now(), busyPct: Math.round(busyPct * 10) / 10, duty: Math.round(duty[0] * 1000) / 1000, psiSome10: psi() });
    if (samples.length > 20_000) samples.shift();
  }, 1_000);
  setInterval(save, 5_000);
  const stop = () => { duty[0] = -1; save(); setTimeout(() => process.exit(0), 200); };
  process.on('SIGTERM', stop);
  process.on('SIGINT', stop);
  void workers;
}
