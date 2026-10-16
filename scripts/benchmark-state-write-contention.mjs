#!/usr/bin/env node
// Owned-fixture proof: --repo=/absolute/repo [--entry=dist]. Requires explicit TMPDIR, Linux taskset, Node 24.
import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { createJiti } from 'jiti';
const self = fileURLToPath(import.meta.url);
const args = {};
for (let i = 2; i < process.argv.length; i++) {
  const [key, value] = process.argv[i].replace(/^--/, '').split('=');
  args[key] = value ?? process.argv[++i];
}
assert(!args.entry || ['src', 'dist'].includes(args.entry), '--entry must be src or dist');
const repo = args.repo ?? path.resolve(path.dirname(self), '..');
assert(path.isAbsolute(repo), '--repo must be absolute');
assert(process.env.TMPDIR && path.isAbsolute(process.env.TMPDIR), 'explicit absolute TMPDIR required');
const tmp = process.env.TMPDIR;
const jiti = createJiti(import.meta.url, { fsCache: path.join(tmp, 'state-contention-jiti') });
const sleep = ms => new Promise(r => setTimeout(r, ms));
const identity = { id: 'contention-proof', name: 'contention-proof', kind: 'agent' };
async function open(root, initialize = false) {
  if (args.entry === 'dist' && !initialize) {
    const { MeshStore } = await import(path.join(repo, 'dist/mesh.js'));
    const mesh = new MeshStore(root, 65536, 1000, { stateBackend: 'sqlite' });
    return { put: x => mesh.put(x), delete: x => mesh.delete(x), confirmWritable: () => mesh.confirmWritable(),
      listAll: prefix => mesh.list(prefix, 10000), close: () => mesh.closeState() };
  }
  const { SqliteStateStore } = await jiti.import(path.join(repo, 'src/mesh/state-sqlite.ts'));
  return SqliteStateStore.open(root, 65536, 1000, initialize ? { initialize: 'create' } : {});
}
const message = kind => new Promise(resolve => {
  const listener = m => { if (m.kind === kind) { process.off('message', listener); resolve(m); } };
  process.on('message', listener);
});
async function child() {
  const hardStop = setTimeout(() => { console.error('child deadline'); process.exit(124); }, 45000);
  let store;
  try {
    if (args.role === 'worker') {
      store = await open(args.root);
      const go = message('go'); process.send({ kind: 'ready' }); await go;
      const samples = [], errors = []; let version = 0;
      const key = `proof/counter/${args.id}`, tempKey = `proof/temp/${args.id}`;
      for (let i = 0; i < 50; i++) for (const operation of [
        async () => { const row = await store.put({ key, value: i, ifVersion: version, identity }); assert(row.version > version); version = row.version; },
        () => store.put({ key: tempKey, value: i, identity }),
        async () => { assert.equal((await store.delete({ key: tempKey })).deleted, true); },
        () => store.confirmWritable(),
      ]) {
        const start = performance.now();
        try { await operation(); } catch (e) { errors.push({ code: e.code ?? e.name, message: String(e.message) }); }
        samples.push(performance.now() - start);
      }
      process.send({ kind: 'report', id: args.id, samples, errors, version });
    } else if (args.role === 'holder') {
      const { DatabaseSync } = await import('node:sqlite');
      const db = new DatabaseSync(path.join(args.root, 'state.db')); db.exec('BEGIN IMMEDIATE');
      const go = message('go'); process.send({ kind: 'ready' }); await go;
      await sleep(Number(args.hold)); db.exec('COMMIT'); db.close();
    } else {
      const go = message('go'); process.send({ kind: 'ready' }); await go;
      const until = performance.now() + 4000;
      while (performance.now() < until) Math.sqrt(Math.random());
    }
  } finally { store?.close(); clearTimeout(hardStop); process.disconnect(); }
}
async function main() {
  const cpu = fs.readFileSync('/proc/self/status', 'utf8').match(/^Cpus_allowed_list:\s*(\d+)/m)?.[1];
  assert(cpu, 'Linux CPU affinity required');
  const owned = fs.mkdtempSync(path.join(tmp, 'state-contention-'));
  const all = [];
  function launch(root, role, extra = {}) {
    const argv = [self, `--repo=${repo}`, `--entry=${args.entry ?? 'src'}`, `--root=${root}`, `--role=${role}`,
      ...Object.entries(extra).map(([k, v]) => `--${k}=${v}`)];
    const child = spawn('taskset', ['-c', cpu, process.execPath, ...argv], {
      env: { ...process.env, TMPDIR: tmp, JITI_FS_CACHE: path.join(tmp, 'state-contention-jiti') },
      stdio: ['ignore', 'pipe', 'pipe', 'ipc'],
    });
    let stderr = '', report, readyResolve, readyReject;
    child.stderr.on('data', x => { stderr += x; }); child.stdout.resume();
    const ready = new Promise((resolve, reject) => { readyResolve = resolve; readyReject = reject; });
    const done = new Promise(resolve => {
      child.on('message', m => { if (m.kind === 'ready') readyResolve(); if (m.kind === 'report') report = m; });
      child.once('error', e => { readyReject(e); resolve({ code: -1, error: String(e) }); });
      child.once('close', (code, signal) => {
        clearTimeout(timer); readyReject(new Error(`early exit ${role}: ${code} ${stderr}`));
        resolve({ code, signal, stderr, report });
      });
    });
    const timer = setTimeout(() => { child.kill('SIGKILL'); readyReject(new Error(`process deadline ${role}`)); }, 55000);
    const handle = { child, ready, done }; all.push(handle); return handle;
  }
  try {
    for (const variant of ['plain', 'burner', 'hold250', 'hold2000']) {
      const root = path.join(owned, variant); fs.mkdirSync(root); (await open(root, true)).close();
      const workers = Array.from({ length: 8 }, (_, id) => launch(root, 'worker', { id }));
      await Promise.all(workers.map(w => w.ready));
      const helper = variant === 'plain' ? undefined : launch(root, variant === 'burner' ? 'burner' : 'holder',
        { hold: variant === 'hold250' ? 250 : 2000 });
      if (helper) await helper.ready;
      const started = performance.now();
      for (const w of [...workers, ...(helper ? [helper] : [])]) w.child.send({ kind: 'go' });
      const results = await Promise.all(workers.map(w => w.done));
      if (helper) assert.equal((await helper.done).code, 0, 'helper failed');
      const failures = results.filter(r => r.code !== 0 || !r.report);
      const reports = results.flatMap(r => r.report ? [r.report] : []);
      const samples = reports.flatMap(r => r.samples).sort((a, b) => a - b), errors = reports.flatMap(r => r.errors);
      console.log(JSON.stringify({ variant, repo, entry: args.entry ?? 'src', cpu, processes: 8, operations: samples.length,
        elapsedMs: performance.now() - started, p95Ms: samples[Math.ceil(samples.length * .95) - 1], maxMs: samples.at(-1),
        surfacedBusy: errors.filter(e => /SQLITE_BUSY|database is locked/.test(e.code + e.message)).length,
        timeouts: errors.filter(e => /TIMEOUT|timed out|deadline/i.test(e.code + e.message)).length, errors, childFailures: failures }));
      assert.equal(failures.length, 0, 'child failed'); assert.equal(errors.length, 0, 'surfaced write error'); assert.equal(samples.length, 1600);
      const verifier = await open(root);
      try {
        const rows = verifier.listAll('proof/'); assert.equal(rows.length, 8);
        for (const r of reports) {
          const row = rows.find(e => e.key === `proof/counter/${r.id}`);
          assert.equal(row?.value, 49); assert.equal(row?.version, r.version);
          await assert.rejects(verifier.put({ key: row.key, value: 'stale', ifVersion: 0, identity }));
        }
        assert.equal(verifier.listAll('proof/').reduce((sum, r) => sum + r.value, 0), 392);
      } finally { verifier.close(); }
      console.log(JSON.stringify({ variant, finalData: '8 counters=49; sum=392; no temp keys; stale CAS rejected' }));
    }
  } finally {
    for (const h of all) if (h.child.exitCode === null && h.child.signalCode === null) h.child.kill('SIGKILL');
    await Promise.all(all.map(h => h.done));
    fs.rmSync(owned, { recursive: true, force: true });
  }
}
try { await (args.role ? child() : main()); }
catch (error) { console.error(error); process.exitCode = 1; if (process.connected) process.disconnect(); }
