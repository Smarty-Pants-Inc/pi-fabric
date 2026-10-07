#!/usr/bin/env node
// Synthetic-only mesh lock benchmark. Run under nice -n 19; never opens a live mesh.
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { build } from 'esbuild';

const self = fileURLToPath(import.meta.url);
const repo = path.resolve(path.dirname(self), '..');
const identity = { id: 'benchmark', name: 'benchmark', kind: 'main', sessionId: 'synthetic' };
if (process.argv[2] === '--worker') {
  const [bundle, root, out, worker, rounds] = process.argv.slice(3);
  const { MeshStore } = await import(bundle);
  globalThis.__meshLockSamples = [];
  const store = new MeshStore(root, 64 * 1024, 100, { lockTimeoutMs: 120000 });
  for (let n = 0; n < Number(rounds); n++) {
    globalThis.__meshOperation = 'put';
    await store.put({ key: `bench/kv/${worker}`, value: { n }, identity });
    globalThis.__meshOperation = 'get';
    store.get(`bench/kv/${worker}`, { fresh: true });
    globalThis.__meshOperation = 'heartbeat';
    await store.writeBatch({ identity, ops: [
      { kind: 'put', key: `fabric/hosts/${worker}`, value: now => ({ hostId: worker, heartbeatAt: now, leaseExpiresAt: now + 15000 }) },
      { kind: 'put', key: `fabric/sessions/${worker}`, value: now => ({ sessionId: worker, heartbeatAt: now }) },
    ], prepare: view => view.get(`bench/kv/${worker}`) ? [] : [], afterCommit: () => {} });
    globalThis.__meshOperation = 'state';
    await store.put({ key: `state/agents/${worker}`, value: { state: 'running', step: n }, identity });
    globalThis.__meshOperation = 'cursor-write';
    await store.put({ key: `fabric/cursors/${worker}`, value: { cursor: store.latestOffset() }, identity });
    globalThis.__meshOperation = 'publish';
    const event = await store.publish({ topic: 'bench.events', from: identity, data: { worker, n } });
    globalThis.__meshOperation = 'cursor-read';
    store.read({ after: event.sequence - 1, limit: 1 });
    store.tail(store.latestOffset(), 10);
  }
  fs.writeFileSync(out, JSON.stringify(globalThis.__meshLockSamples));
} else {
  const [outArg, processesArg = '1', roundsArg = '30'] = process.argv.slice(2);
  if (!outArg) throw new Error('usage: node scripts/benchmark-mesh-lock.mjs OUTPUT_DIR [PROCESSES] [ROUNDS]');
  const out = path.resolve(outArg);
  fs.mkdirSync(out, { recursive: true });
  const scratch = fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), 'mesh-lock-bench-'));
  try {
    const root = path.join(scratch, 'mesh');
    fs.mkdirSync(root);
    const state = { readGeneration: '11111111-1111-4111-8111-111111111111', format: 1, revisionFormat: 2, highWater: 6000, entries: {}, versions: {}, tombstoneOrder: [] };
    for (let i = 0; i < 4400; i++) {
      const key = `state/participants/p${String(i).padStart(5, '0')}`;
      state.entries[key] = { key, value: { id: `p${i}`, state: 'running', context: 'x'.repeat(560), hostId: `h${i % 32}` }, version: i + 1, updatedAt: 1700000000000, updatedBy: identity };
      state.versions[key] = i + 1;
    }
    let serialized = JSON.stringify(state);
    // Keep realistic varied keys/entries but pin the size to the reported 3.8 decimal MB.
    state.fixturePadding = 'x'.repeat(Math.max(0, 3800000 - Buffer.byteLength(serialized) - 20));
    serialized = JSON.stringify(state);
    fs.writeFileSync(path.join(root, 'state.json'), serialized);
    let bytes = 0, sequence = 0;
    const fd = fs.openSync(path.join(root, 'events.jsonl'), 'w');
    try {
      while (bytes < 40000000) {
        sequence++;
        const line = JSON.stringify({ id: `event-${sequence}`, sequence, topic: 'bench.events', kind: 'message', from: identity, text: 'e'.repeat(1100), createdAt: 1700000000000 + sequence }) + '\n';
        bytes += fs.writeSync(fd, line);
      }
    } finally { fs.closeSync(fd); }
    fs.writeFileSync(path.join(root, 'sequence'), String(sequence));
    const bundle = path.join(scratch, 'store.mjs');
    let source = fs.readFileSync(path.join(repo, 'src/mesh/store.ts'), 'utf8');
    const begin = '      try {\n        this.#writeAbortSignal?.throwIfAborted();\n        return operation();';
    const end = '      } finally {\n        releaseOwned();\n      }';
    if (!source.includes(begin) || !source.includes(end)) throw new Error('lock instrumentation anchor changed');
    source = source.replace(begin, '      const benchStart = process.hrtime.bigint();\n' + begin)
      .replace(end, '      } finally {\n        releaseOwned();\n        const benchEnd = process.hrtime.bigint();\n        (globalThis as any).__meshLockSamples?.push({ op: (globalThis as any).__meshOperation, start: Number(benchStart / 1000n), end: Number(benchEnd / 1000n), us: Number(benchEnd - benchStart) / 1000 });\n      }');
    await build({ stdin: { contents: source, loader: 'ts', resolveDir: path.join(repo, 'src/mesh'), sourcefile: 'store.ts' }, bundle: true, platform: 'node', format: 'esm', outfile: bundle, sourcemap: 'inline' });
    const wallStart = performance.now();
    const workers = await Promise.allSettled(Array.from({ length: Number(processesArg) }, (_, i) => new Promise((resolve, reject) => {
      const child = spawn(process.execPath, ['--cpu-prof', '--cpu-prof-interval=500', `--cpu-prof-dir=${out}`, `--cpu-prof-name=worker-${i}.cpuprofile`, self, '--worker', bundle, root, path.join(out, `holds-${i}.json`), String(i), roundsArg], { stdio: ['ignore', 'pipe', 'pipe'] });
      let log = '';
      child.stdout.on('data', b => { log += b; }); child.stderr.on('data', b => { log += b; });
      child.once('error', reject);
      child.once('exit', code => code === 0 ? resolve() : reject(new Error(`worker ${i}: exit ${code}: ${log}`)));
    })));
    // Join all children, including on failure: cleanup must not race a surviving writer.
    const failed = workers.filter(worker => worker.status === 'rejected');
    if (failed.length) throw new AggregateError(failed.map(worker => worker.reason), 'mesh benchmark workers failed');
    const finalState = JSON.parse(fs.readFileSync(path.join(root, 'state.json'), 'utf8'));
    const expectedHighWater = 6000 + Number(processesArg) * Number(roundsArg) * 5;
    const expectedSequence = sequence + Number(processesArg) * Number(roundsArg);
    if (finalState.highWater !== expectedHighWater || Number(fs.readFileSync(path.join(root, 'sequence'), 'utf8')) !== expectedSequence) {
      throw new Error('synthetic benchmark lost a state transition or event');
    }
    for (let i = 0; i < Number(processesArg); i++) {
      if (finalState.entries[`bench/kv/${i}`]?.value.n !== Number(roundsArg) - 1 ||
        finalState.entries[`state/agents/${i}`]?.value.step !== Number(roundsArg) - 1 ||
        !finalState.entries[`fabric/hosts/${i}`] || !finalState.entries[`fabric/sessions/${i}`] ||
        !finalState.entries[`fabric/cursors/${i}`]) throw new Error(`worker ${i}: missing final state`);
    }
    if (fs.readdirSync(root).some(name => name.endsWith('.prepared.tmp'))) throw new Error('prepared staging leak');
    const validation = { highWater: finalState.highWater, sequence: expectedSequence, lostUpdates: false, stagingLeaks: false };
    const holds = Array.from({ length: Number(processesArg) }, (_, i) => JSON.parse(fs.readFileSync(path.join(out, `holds-${i}.json`), 'utf8'))).flat();
    const distribution = samples => {
      const sorted = samples.map(x => x.us / 1000).sort((a, b) => a - b);
      return { count: sorted.length, medianMs: sorted[Math.floor(sorted.length * .5)], p99Ms: sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * .99))], maxMs: sorted.at(-1), totalMs: sorted.reduce((a, b) => a + b, 0) };
    };
    const categories = new Map(); let sampledUs = 0;
    for (let i = 0; i < Number(processesArg); i++) {
      const profile = JSON.parse(fs.readFileSync(path.join(out, `worker-${i}.cpuprofile`), 'utf8'));
      const windows = JSON.parse(fs.readFileSync(path.join(out, `holds-${i}.json`), 'utf8'));
      const nodes = new Map(profile.nodes.map(n => [n.id, n]));
      const parents = new Map(); for (const n of profile.nodes) for (const c of n.children ?? []) parents.set(c, n.id);
      let at = profile.startTime, w = 0;
      for (let s = 0; s < profile.samples.length; s++) {
        at += profile.timeDeltas[s];
        while (w < windows.length && windows[w].end < at) w++;
        if (w >= windows.length || windows[w].start > at) continue;
        const chain = []; let id = profile.samples[s];
        while (id !== undefined) { chain.push(nodes.get(id)?.callFrame.functionName ?? '(unknown)'); id = parents.get(id); }
        const category = ['readState', 'encodeState', '#writeSignal', 'prepareStateJournal', 'appendStateJournal', '#cacheState', 'writeFileAtomic', '#liveEntriesAfter', '#readLastEventSequence', '#compactEventLog'].find(name => chain.includes(name)) ?? chain[0];
        const us = profile.timeDeltas[s]; sampledUs += us; categories.set(category, (categories.get(category) ?? 0) + us);
      }
    }
    const result = { validation, fixture: { stateBytes: Buffer.byteLength(serialized), eventBytes: bytes, entries: 4400, events: sequence }, processes: Number(processesArg), roundsPerProcess: Number(roundsArg), wallMs: performance.now() - wallStart, hold: distribution(holds), byOperation: Object.fromEntries([...new Set(holds.map(x => x.op))].map(op => [op, distribution(holds.filter(x => x.op === op))])), profile: { sampledLockMs: sampledUs / 1000, byFunction: [...categories].sort((a, b) => b[1] - a[1]).map(([name, us]) => ({ name, percent: 100 * us / sampledUs, sampledMs: us / 1000 })) } };
    fs.writeFileSync(path.join(out, 'summary.json'), JSON.stringify(result, null, 2) + '\n');
    console.log(JSON.stringify(result, null, 2));
  } finally { fs.rmSync(scratch, { recursive: true, force: true }); }
}
