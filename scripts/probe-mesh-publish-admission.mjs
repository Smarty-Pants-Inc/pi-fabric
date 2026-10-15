#!/usr/bin/env node
// Built public-store proof. Synthetic roots only; never accepts --root.
// Normal progress: IPC ready/start/close; timers only pace the workload or bound its lifetime.
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { createHash } from 'node:crypto';
import { spawn } from 'node:child_process';
import { AsyncLocalStorage } from 'node:async_hooks';

const self = fileURLToPath(import.meta.url), baseHead = '014315d34665a8c5f1e36cbe33606fc0d65520c9';
const mode = process.env.MESH_PUBLISH_PROBE_MODE;
const send = message => new Promise(resolve => {
  if (!process.connected) return resolve(false);
  process.send(message, error => resolve(!error));
});
const sleep = ms => new Promise(resolve => setTimeout(resolve, Math.max(0, ms)));
const errorOf = e => Object.fromEntries(['name', 'code', 'kind', 'preLockCode', 'attempts', 'waitedMs',
  'waitersAhead', 'message'].map(k => [k, e?.[k]]).filter(([, v]) => v !== undefined));
const hash = file => createHash('sha256').update(fs.readFileSync(file)).digest('hex');
const identity = caller => ({ id: `probe-${caller}`, name: `probe-${caller}`, kind: 'main' });
const request = (id, caller, durable) => ({ id, topic: 'probe.mesh.publish.admission', kind: 'message',
  from: identity(caller), text: 'synthetic publish admission probe', data: { probeId: id, caller }, ...(durable ? { durable: true } : {}) });
const settings = { stateBackend: 'sqlite', lockProtocol: 1, lockTimeoutMs: 10_000,
  maxEventBytes: 256 * 1024, maxReadEvents: 500, archive: false, dedupe: false, schedules: false,
  residency: false, directCanWakeResidents: false, cliCanWakeResidents: 'production default true; no residency exists' };
const options = signal => ({ stateBackend: 'sqlite', canWakeResidents: () => false, writeSignal: signal });
const ownCpus = () => {
  try { return fs.readFileSync('/proc/self/status', 'utf8').match(/^Cpus_allowed_list:\s*(.+)$/m)?.[1] ?? null; }
  catch { return null; }
};
const metadata = store => ({ backend: store.stateBackend, protocol: store.lockProtocol, diagnostics: store.stateDiagnostics(), allowedCpus: ownCpus() });
const assertProfile = store => {
  const m = metadata(store);
  if (m.backend !== 'sqlite' || m.protocol !== 1 || m.diagnostics.fallback) throw new Error(`profile refused: ${JSON.stringify(m)}`);
  return m;
};
// Only the existing harness seam; originals always run. Root/class cannot by themselves distinguish
// SQLite and mesh-lock samples. Source lock events below retain authoritative store/class/kind.
function recorder() {
  const stats = globalThis[Symbol.for('pi-fabric.mesh.lock-stats')]?.stats;
  if (!stats) throw new Error('existing lock-stats recorder unavailable');
  const als = new AsyncLocalStorage();
  for (const method of ['acquired', 'failed']) {
    const original = stats[method];
    stats[method] = function (...args) {
      const result = original.apply(this, args), row = als.getStore();
      if (row) row.locks.push({ ordinal: row.locks.length + 1, method, root: args[0], class: args[1],
        waitMs: args[2], ...(method === 'acquired' ? { holdMs: args[3] } : { tried: args[3] }) });
      return result;
    };
  }
  return async (store, input, caller, publish, emit) => {
    const row = { id: input.id, caller, pid: process.pid, startedAt: Date.now(), status: 'inflight', locks: [] };
    const t = performance.now(); emit(row);
    try {
      row.profile = assertProfile(store);
      const event = await als.run(row, publish);
      row.ack = { id: event.id, sequence: event.sequence }; row.status = 'ack';
      return event;
    } catch (e) { row.error = errorOf(e); row.status = 'error'; throw e; }
    finally { row.wallMs = performance.now() - t; row.finishedAt = Date.now(); emit(row); }
  };
}
function fuse(deadline) {
  // Own child only; no signals. A fuse exit can censor a committed write: readback reconciles it.
  const timer = setTimeout(() => { void send({ type: 'fuse', pid: process.pid }); process.exit(124); }, Math.max(1, deadline - Date.now()));
  timer.unref();
}
async function child() {
  const c = JSON.parse(process.env.MESH_PUBLISH_PROBE_CONFIG);
  fuse(c.deadline);
  let stopped = false;
  const abort = new AbortController();
  let begin;
  const start = new Promise(resolve => { begin = resolve; });
  const stop = reason => { stopped = true; abort.abort(new Error(reason)); begin({ stopAt: Date.now() }); };
  process.on('disconnect', () => stop('parent IPC EOF'));
  process.on('message', m => {
    if (m.type === 'start') begin(m);
    if (m.type === 'stop') stop('cooperative stop');
  });
  if (mode === 'hog') {
    await send({ type: 'ready', pid: process.pid, allowedCpus: ownCpus() });
    const { stopAt } = await start, began = Date.now(), cpu = process.cpuUsage();
    while (!stopped && Date.now() < stopAt) {
      const t = performance.now(); while (performance.now() - t < 10 * c.duty / 100) { /* finite CPU work */ }
      if (c.duty < 100) await sleep(10 * (100 - c.duty) / 100);
      else await new Promise(resolve => setImmediate(resolve)); // IPC stop/EOF remains serviceable.
    }
    await send({ type: 'done', pid: process.pid, cpu: process.cpuUsage(cpu), wallMs: Date.now() - began });
  } else {
    const { MeshStore } = await import(pathToFileURL(path.join(c.package, 'dist/mesh.js')).href);
    const observe = recorder(), store = new MeshStore(c.root, settings.maxEventBytes, settings.maxReadEvents, options(abort.signal));
    try {
      await store.prepareLock(); store.get('probe/seed');
      await send({ type: 'ready', pid: process.pid, profile: assertProfile(store) });
      const { stopAt } = await start, cpu = process.cpuUsage(), began = Date.now();
      const stop = setTimeout(() => abort.abort(new Error('stress window ended')), Math.max(0, stopAt - Date.now()));
      try {
        for (let i = 0; i < c.rounds && !stopped && Date.now() < stopAt; i++) {
          const input = request(`stress-${c.worker}-${i}`, `stress-${c.worker}`, c.durable);
          try { await observe(store, input, input.from.id.slice(6), () => store.publish(input), row => { void send({ type: 'call', row }); }); }
          catch { /* one attempt per ID; failures remain evidence, never retries */ }
        }
      } finally { clearTimeout(stop); }
      await send({ type: 'done', pid: process.pid, cpu: process.cpuUsage(cpu), wallMs: Date.now() - began });
    } finally { store.closeState(); }
  }
  if (process.connected) process.disconnect();
}
async function preload() {
  const c = JSON.parse(process.env.MESH_PUBLISH_PROBE_CONFIG);
  fuse(c.deadline);
  // Actual publisher remains argv[1], stdin JSON, --review-released. No constructor/options override.
  // There is no production CLI writeSignal seam: its fuse may leave an uncertain commit.
  const { MeshStore } = await import(pathToFileURL(path.join(c.package, 'dist/mesh.js')).href);
  const observe = recorder(), original = MeshStore.prototype.publish;
  MeshStore.prototype.publish = function (input) {
    return observe(this, input, c.caller, () => original.call(this, input), row => { void send({ type: 'inner', row }); });
  };
}
function parse() {
  const c = { workers: 40, rounds: 20, seconds: 60, rate: 0.341, hogs: 2, duty: 100, durable: false };
  for (let i = 2; i < process.argv.length; i++) {
    const key = process.argv[i].slice(2);
    if (process.argv[i] === '--durable') { c.durable = true; continue; }
    if (!['package', 'publisher', 'head', 'workers', 'rounds', 'seconds', 'hogs', 'duty'].includes(key) || !process.argv[i].startsWith('--')) throw new Error(`unknown option ${process.argv[i]}`);
    const value = process.argv[++i]; if (!value || value.startsWith('--')) throw new Error(`missing ${key}`);
    c[key] = ['workers', 'rounds', 'seconds', 'hogs', 'duty'].includes(key) ? Number(value) : value;
  }
  for (const [key, min, max] of [['workers', 1, 40], ['rounds', 1, 20], ['seconds', 1, 90], ['hogs', 0, 12], ['duty', 1, 100]])
    if (!Number.isInteger(c[key]) || c[key] < min || c[key] > max) throw new Error(`invalid bounded ${key}`);
  if (!c.package || !path.isAbsolute(c.package) || (c.publisher && !path.isAbsolute(c.publisher))) throw new Error('--package ABS/dist/mesh.js and optional --publisher ABS required');
  if (!/^[a-f0-9]{40}$/.test(c.head ?? '')) throw new Error('--head FULL_SHA required (caller provenance, not artifact attestation)');
  if (!process.env.TMPDIR || !path.isAbsolute(process.env.TMPDIR)) throw new Error('absolute inherited TMPDIR required');
  if (Object.keys(process.env).some(k => k.startsWith('PI_FABRIC_TEST_'))) throw new Error('test fault-injection environment refused');
  if (process.env.NODE_OPTIONS) throw new Error('NODE_OPTIONS refused: uncontrolled preloads');
  c.mesh = fs.realpathSync(c.package);
  if (path.basename(c.mesh) !== 'mesh.js' || path.basename(path.dirname(c.mesh)) !== 'dist') throw new Error('--package must select the public dist/mesh.js');
  c.package = path.dirname(path.dirname(c.mesh)); if (c.publisher) c.publisher = fs.realpathSync(c.publisher);
  if (JSON.parse(fs.readFileSync(path.join(c.package, 'package.json'), 'utf8')).name !== 'pi-fabric') throw new Error('not pi-fabric');
  return c;
}
function artifact(directory) {
  const files = [];
  function visit(dir) {
    for (const e of fs.readdirSync(dir, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name))) {
      const p = path.join(dir, e.name);
      if (e.isSymbolicLink()) throw new Error(`artifact symlink refused: ${p}`);
      if (e.isDirectory()) visit(p);
      else if (e.name.endsWith('.js')) files.push({ file: path.relative(directory, p), sha256: hash(p) });
    }
  }
  visit(directory);
  return { sha256: createHash('sha256').update(JSON.stringify(files)).digest('hex'), files };
}
function psi() { try { return fs.readFileSync('/proc/pressure/cpu', 'utf8'); } catch (e) { return { unavailable: errorOf(e) }; } }
function scopedPsi() {
  try {
    const group = fs.readFileSync('/proc/self/cgroup', 'utf8').split('\n').find(line => line.startsWith('0::'))?.slice(3);
    return group === undefined ? null : fs.readFileSync(`/sys/fs/cgroup${group}/cpu.pressure`, 'utf8');
  } catch { return null; }
}
function psiDelta(before, after) {
  if (typeof before !== 'string' || typeof after !== 'string') return null;
  return Object.fromEntries(['some', 'full'].map(k => {
    const total = s => Number(s.match(new RegExp(`^${k} .*total=(\\d+)`, 'm'))?.[1]);
    return [k, total(after) - total(before)];
  }));
}
function percentiles(values) {
  const a = values.slice().sort((a, b) => a - b), p = q => a.length ? a[Math.ceil(q * a.length) - 1] : null;
  return { n: a.length, p50: p(.5), p95: p(.95), p99: p(.99), max: a.at(-1) ?? null };
}
async function main() {
  const c = parse(), children = [], rows = new Map(), expected = new Map(), problems = [];
  const provenance = { probe: { file: self, sha256: hash(self) }, package: c.package, meshEntry: c.mesh,
    packageJsonSha256: hash(path.join(c.package, 'package.json')), meshEntrySha256: hash(c.mesh),
    artifact: artifact(path.join(c.package, 'dist')), publisher: c.publisher ? { file: c.publisher, sha256: hash(c.publisher) } : null,
    node: { version: process.version, executable: process.execPath, sha256: hash(process.execPath) } };
  const inheritedTmpdir = process.env.TMPDIR, runRoot = fs.mkdtempSync(path.join(inheritedTmpdir, 'mesh-publish-admission-'));
  process.env.TMPDIR = runRoot;
  const result = { schema: 1, runRoot, inheritedTmpdir, startedAt: new Date().toISOString(), baseHead, callerSuppliedHead: c.head, provenance,
    config: { ...c, ...settings, timeoutSource: 'unchanged production default', windows: 'stress and paced legs overlap from common barrier' },
    capabilities: { publicMeshStore: true, actualCliSelected: !!c.publisher, directWriteSignal: true, cliWriteSignal: false,
      artifactHeadAttested: false, headNote: '--head is caller-supplied; parent must associate it with the hashed build' },
    gates: { performanceImprovement: 'UNASSESSED: parent must compare matched baseline/candidate runs',
      stateWriterBridge: 'UNEXECUTED', mixedRelease: 'UNEXECUTED', watchFailure: 'UNEXECUTED',
      cancellationRecovery: 'UNEXECUTED', fileBackend: 'UNEXECUTED', otherPlatforms: 'UNEXECUTED' } };
  const deadline = Date.now() + 170_000, controller = new AbortController();
  const stop = setTimeout(() => controller.abort(new Error('parent operation window ended')), 150_000);
  let store, observe, workStarted = false;
  const env = { ...process.env, TMPDIR: runRoot, PI_FABRIC_LOCK_STATS: '1', PI_FABRIC_MESH_STATE_BACKEND: 'sqlite', SMARTY_FACTORY_FABRIC_PACKAGE: c.package };
  process.env.PI_FABRIC_LOCK_STATS = '1'; process.env.PI_FABRIC_MESH_STATE_BACKEND = 'sqlite';
  function launch(kind, extra, argv = [self]) {
    const config = { ...c, deadline, ...extra }, entry = { kind, config, messages: [], stdout: '', stderr: '' };
    const child = spawn(process.execPath, argv, { env: { ...env, MESH_PUBLISH_PROBE_MODE: kind,
      MESH_PUBLISH_PROBE_CONFIG: JSON.stringify(config) }, stdio: ['pipe', 'pipe', 'pipe', 'ipc'] });
    entry.child = child; children.push(entry);
    entry.ready = new Promise(resolve => { entry.resolveReady = resolve; });
    entry.closed = new Promise(resolve => child.once('close', (code, signal) => {
      entry.code = code; entry.signal = signal; entry.closedAt = Date.now(); entry.resolveReady(false); resolve(entry);
    }));
    child.on('error', e => { entry.spawnError = errorOf(e); });
    child.on('message', m => {
      if (m.type === 'call') rows.set(m.row.id, m.row);
      else if (m.type === 'inner') entry.inner = m.row;
      else { entry.messages.push(m); if (m.type === 'ready') entry.resolveReady(true); }
    });
    child.stdout.on('data', b => { entry.stdout += b; }); child.stderr.on('data', b => { entry.stderr += b; });
    child.stdin.on('error', e => { entry.stdinError = errorOf(e); });
    return entry;
  }
  try {
    const { MeshStore } = await import(pathToFileURL(path.join(c.package, 'dist/mesh.js')).href);
    observe = recorder();
    // Source requires an imported state.db for ordinary SQLite open. Seed through public file API.
    const seedRoot = fs.mkdtempSync(path.join(runRoot, 'seed-'));
    const seed = new MeshStore(seedRoot, settings.maxEventBytes, settings.maxReadEvents, { stateBackend: 'file', canWakeResidents: () => false });
    try { await seed.put({ key: 'probe/seed', value: { synthetic: true }, identity: identity('seed') }); }
    finally { seed.closeState(); }
    const cliPath = path.join(c.package, 'dist/mesh/mesh-backend-cli.js');
    result.provenance.backendCliSha256 = hash(cliPath);
    const { main: backendMain } = await import(pathToFileURL(cliPath).href);
    let output = '';
    const code = await backendMain(['import', '--scratch-proof-from', path.join(seedRoot, 'state.json'), '--scratch-parent', runRoot, '--json'],
      { stdout: text => { output += text; }, stderr: text => process.stderr.write(text) });
    const imported = JSON.parse(output); result.import = { code, ...imported };
    if (code || !imported.ok || !imported.scratchProof || !path.isAbsolute(imported.root) || path.dirname(imported.root) !== runRoot) throw new Error('supported scratch import failed or returned outside owned parent');
    const root = imported.root; result.root = root;
    store = new MeshStore(root, settings.maxEventBytes, settings.maxReadEvents, options(controller.signal));
    await store.prepareLock(); store.get('probe/seed'); result.profile = assertProfile(store);
    const workers = Array.from({ length: c.workers }, (_, worker) => {
      for (let i = 0; i < c.rounds; i++) expected.set(`stress-${worker}-${i}`, request(`stress-${worker}-${i}`, `stress-${worker}`, c.durable));
      const e = launch('worker', { root, worker }); e.child.stdin.end(); return e;
    });
    const hogs = Array.from({ length: c.hogs }, () => { const e = launch('hog', {}); e.child.stdin.end(); return e; });
    const count = Math.ceil(c.seconds * c.rate), cliCount = c.publisher ? Math.round(count * .43) : 0;
    const plan = Array.from({ length: count }, (_, i) => {
      const kind = Math.floor((i + 1) * cliCount / count) > Math.floor(i * cliCount / count) ? 'cli' : 'main';
      const input = request(`paced-${i}`, `paced-${kind}`, c.durable); expected.set(input.id, input); return { kind, input, offsetMs: i * 1000 / c.rate };
    });
    result.plan = { stress: c.workers * c.rounds, paced: plan, cliCount, mainCount: count - cliCount,
      targetRate: c.rate, scheduledRate: count / c.seconds, realizedCliFraction: cliCount / count };
    if (!(await Promise.all([...workers, ...hogs].map(e => e.ready))).every(Boolean)) throw new Error('child failed before ready barrier');
    result.psiBefore = psi(); result.scopedPsiBefore = scopedPsi(); result.parentAllowedCpus = ownCpus();
    result.barrierAt = Date.now(); result.parentCpuStart = process.cpuUsage(); workStarted = true;
    const stopAt = result.barrierAt + c.seconds * 1000;
    for (const e of [...workers, ...hogs]) e.child.send({ type: 'start', stopAt }, error => { if (error) problems.push({ stage: 'barrier', error: errorOf(error) }); });
    const paced = (async () => {
      for (const { kind, input, offsetMs } of plan) {
        await sleep(result.barrierAt + offsetMs - Date.now());
        if (controller.signal.aborted || Date.now() >= deadline - 20_000) break; // explicit unattempted count, never hidden.
        if (kind === 'main') {
          try { await observe(store, input, 'paced-main', () => store.publish(input), row => {
            row.scheduledAt = result.barrierAt + offsetMs; row.scheduleLagMs = row.startedAt - row.scheduledAt; rows.set(row.id, row);
          }); } catch { /* recorded */ }
        } else {
          const t = performance.now(), row = { id: input.id, caller: 'paced-cli', startedAt: Date.now(), scheduledAt: result.barrierAt + offsetMs, status: 'inflight', locks: [] };
          row.scheduleLagMs = row.startedAt - row.scheduledAt; rows.set(row.id, row);
          const e = launch('preload', { root, caller: row.caller, deadline: Math.min(deadline, Date.now() + 25_000) }, ['--import', self, c.publisher, root, '--review-released']);
          e.child.stdin.end(JSON.stringify(input)); await e.closed;
          row.wallMs = performance.now() - t; row.finishedAt = Date.now(); row.inner = e.inner;
          row.locks = e.inner?.locks ?? []; row.profile = e.inner?.profile;
          try {
            const ack = JSON.parse(e.stdout);
            if (e.code !== 0 || !e.inner || e.inner.status !== 'ack' || ack.id !== input.id || !Number.isSafeInteger(ack.sequence)) throw new Error('CLI exit/receipt/instrumentation mismatch');
            row.ack = { id: ack.id, sequence: ack.sequence }; row.status = 'ack';
          } catch (error) { row.status = e.code === 124 ? 'censored' : 'error'; row.error = e.inner?.error ?? errorOf(error); }
        }
      }
      await sleep(stopAt - Date.now());
    })();
    await Promise.all([paced, ...workers.map(e => e.closed)]);
  } catch (error) { problems.push({ stage: workStarted ? 'harness' : 'precondition', error: errorOf(error) }); }
  finally {
    for (const e of children) if (e.child.connected) e.child.send({ type: 'stop' }, () => {});
    await Promise.all(children.map(e => e.closed)); // Terminal barrier BEFORE readback or ownership return.
    clearTimeout(stop); result.psiAfter = psi(); result.psiTotalDeltaUs = psiDelta(result.psiBefore, result.psiAfter);
    result.scopedPsiAfter = scopedPsi(); result.scopedPsiTotalDeltaUs = psiDelta(result.scopedPsiBefore, result.scopedPsiAfter);
    if (workStarted) result.parentCpu = process.cpuUsage(result.parentCpuStart); delete result.parentCpuStart;
    for (const row of rows.values()) if (row.status === 'inflight') row.status = 'censored';
    try {
      if (store) {
        result.sync = await store.sync();
        const events = []; let after = 0;
        for (;;) {
          const batch = store.read({ after, limit: settings.maxReadEvents });
          if (!batch.length) break; // not short-page termination: completeness requires EMPTY.
          for (const event of batch) {
            if (!Number.isSafeInteger(event.sequence) || event.sequence <= after) throw new Error('nonincreasing/duplicate sequence');
            after = event.sequence; events.push(event);
          }
        }
        const ids = new Map();
        for (const e of events) { if (ids.has(e.id)) throw new Error(`duplicate event ID ${e.id}`); ids.set(e.id, e); }
        const missingAck = [], mismatch = [], unexpected = [], committedWithoutAck = [], duplicateAck = [], ackIds = new Set(), ackSequences = new Set();
        for (const [id, row] of rows) if (row.status === 'ack') {
          if (row.ack.id !== id || !Number.isSafeInteger(row.ack.sequence) || !ids.has(id) || ids.get(id).sequence !== row.ack.sequence) missingAck.push(id);
          if (ackIds.has(row.ack.id) || ackSequences.has(row.ack.sequence)) duplicateAck.push(id);
          ackIds.add(row.ack.id); ackSequences.add(row.ack.sequence);
        }
        for (const [id, e] of ids) {
          const input = expected.get(id);
          if (!input) unexpected.push(id);
          else if (JSON.stringify(e.data) !== JSON.stringify(input.data) || e.text !== input.text || e.topic !== input.topic || e.kind !== input.kind || e.from.id !== input.from.id || e.from.name !== input.from.name || e.from.kind !== 'main') mismatch.push(id);
          if (rows.get(id)?.status !== 'ack') committedWithoutAck.push(id);
        }
        result.readback = { through: after, count: events.length, reachedEmpty: true, missingAck, duplicateAck, mismatch, unexpected, committedWithoutAck,
          missingExpected: [...expected.keys()].filter(id => !ids.has(id)), sequenceGapFreedomRequired: false };
        if (result.sync.through !== after || missingAck.length || duplicateAck.length || mismatch.length || unexpected.length || committedWithoutAck.length) problems.push({ stage: 'readback', error: 'publication reconciliation failed' });
      }
    } catch (error) { problems.push({ stage: 'readback', error: errorOf(error) }); }
    finally { store?.closeState(); }
  }
  const all = [...rows.values()], counts = { expected: expected.size, attempted: all.length,
    ack: all.filter(r => r.status === 'ack').length, errors: all.filter(r => r.status === 'error').length,
    censored: all.filter(r => r.status === 'censored').length, unattempted: expected.size - all.length };
  const pacedRows = all.filter(r => r.caller.startsWith('paced-'));
  result.pacing = { windowMs: c.seconds * 1000, attempted: pacedRows.length,
    startLagMs: percentiles(pacedRows.map(r => r.scheduleLagMs)),
    finishedAt: Math.max(result.barrierAt ?? 0, ...pacedRows.map(r => r.finishedAt ?? r.startedAt)) };
  const callers = {};
  for (const caller of new Set([...expected.values()].map(input => input.data.caller))) {
    const mine = all.filter(r => r.caller === caller), locks = mine.flatMap(r => r.locks);
    const planned = [...expected.values()].filter(input => input.data.caller === caller).length;
    callers[caller] = { expected: planned, unattempted: planned - mine.length, attempted: mine.length, ack: mine.filter(r => r.status === 'ack').length,
      errors: mine.filter(r => r.status === 'error').length, censored: mine.filter(r => r.status === 'censored').length,
      wallAllCompletedMs: percentiles(mine.filter(r => r.status !== 'censored').map(r => r.wallMs)),
      wallAckMs: percentiles(mine.filter(r => r.status === 'ack').map(r => r.wallMs)),
      acquisitionWaitMs: percentiles(locks.map(l => l.waitMs)), holdMs: percentiles(locks.filter(l => l.method === 'acquired').map(l => l.holdMs)),
      perCallSampledWaitMs: percentiles(mine.map(r => r.locks.reduce((sum, l) => sum + l.waitMs, 0))) };
  }
  const taxonomy = {};
  for (const r of all.filter(r => r.error)) {
    const e = r.error, key = [e.preLockCode ?? '', e.code ?? '', e.kind ?? '', e.name ?? ''].join('|'); taxonomy[key] = (taxonomy[key] ?? 0) + 1;
  }
  const lockEvents = [];
  for (const name of ['events.jsonl.1', 'events.jsonl']) {
    const file = path.join(result.root ?? runRoot, 'lock-stats', name);
    if (fs.existsSync(file)) for (const line of fs.readFileSync(file, 'utf8').split('\n').filter(Boolean)) {
      try { const e = JSON.parse(line); if (e.at >= result.barrierAt) lockEvents.push(e); }
      catch (error) { problems.push({ stage: 'lock-events', error: errorOf(error) }); }
    }
  }
  const lockEventTaxonomy = {};
  for (const e of lockEvents) { const key = [e.store, e.class, e.kind].join('|'); lockEventTaxonomy[key] = (lockEventTaxonomy[key] ?? 0) + 1; }
  const failureEvidence = { failedLockCallbacks: all.flatMap(r => r.locks).filter(l => l.method === 'failed' && !l.tried).length,
    timeoutOrPrelockEvents: lockEvents.filter(e => e.kind !== 'held').length };
  if (all.some(r => r.status === 'ack' && !r.locks.some(l => l.method === 'acquired' && l.class === 'publish'))) problems.push({ stage: 'harness', error: 'ack without correlated publish acquisition' });
  if (children.some(e => e.code !== 0 || e.signal || (!e.messages.some(m => m.type === 'done') && e.kind !== 'preload'))) problems.push({ stage: 'children', error: 'abnormal exit or missing native completion' });
  const failed = problems.length || !workStarted || counts.expected === 0 || counts.ack !== counts.expected || counts.errors || counts.censored || counts.unattempted || failureEvidence.failedLockCallbacks || failureEvidence.timeoutOrPrelockEvents;
  Object.assign(result, { status: failed ? 'EVIDENCE_FAILURE' : 'COMPLETE', performanceVerdict: 'UNASSESSED', counts, callers, taxonomy, lockEventTaxonomy, failureEvidence, lockEvents, problems,
    calls: all, children: children.map(({ child, ready, closed, resolveReady, ...e }) => ({ ...e, pid: child.pid })),
    terminalBarrier: children.every(e => e.closedAt !== undefined), endedAt: new Date().toISOString(),
    limitations: ['wait/hold are samples, not an exact wall decomposition or residual cause', 'CPU PSI delta is not a host-cause diagnosis',
      'CLI full wall includes process startup; inner wall is separately retained', 'CLI cannot accept a writeSignal; fuse/EOF may censor a commit',
      'no performance improvement claim without matched paired runs', 'stress/Main/CLI share a common synthetic root and time window'] });
  const out = path.join(runRoot, 'result.json'); result.out = out;
  fs.writeFileSync(out, JSON.stringify(result, null, 2) + '\n', { mode: 0o600 });
  process.stderr.write(`${result.status}: ${JSON.stringify(counts)}; retained ${runRoot}; result ${out}\n`);
  process.stdout.write(JSON.stringify(result) + '\n');
  process.exitCode = failed ? (problems.some(p => ['precondition', 'harness'].includes(p.stage)) ? 2 : 1) : 0;
}
if (mode === 'preload') await preload();
else if (mode === 'worker' || mode === 'hog') await child().catch(async e => { await send({ type: 'fatal', error: errorOf(e) }); process.exitCode = 2; if (process.connected) process.disconnect(); });
else if (process.argv.includes('--help')) process.stdout.write('Usage: node probe-mesh-publish-admission.mjs --package ABS/dist/mesh.js --head FULL_SHA [--publisher ABS_SCRIPT] [--workers 40] [--rounds 20] [--seconds 60] [--hogs 2] [--duty 100] [--durable]\nFresh synthetic root under inherited TMPDIR only. JSON stdout and retained own result.json; diagnostics stderr. No retries, timeout override, signals, or live --root. Parent owns execution/cleanup.\n');
else await main().catch(error => { const receipt = JSON.stringify({ status: 'PRECONDITION_UNKNOWN', performanceVerdict: 'UNASSESSED', error: errorOf(error) }); process.stdout.write(receipt + '\n'); process.stderr.write(receipt + '\n'); process.exitCode = 2; });
