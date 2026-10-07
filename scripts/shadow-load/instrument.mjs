// Preloaded (node --import) into every shadow-test process that runs candidate code. It counts
// FABRIC_MESH_LOCK_TIMEOUT rejections of the candidate's own MeshStore lock entry points, so a
// release without L8 lock stats still reports its lock timeouts, and records the process's peak
// RSS. It also times every hub mesh lock acquisition (wait) and hold off the lock protocol's own
// file operations, so releases with and without L8 lock stats report wait p99 the same way.
// It changes no behaviour: only async methods are wrapped, errors are rethrown as is, and the fs
// wrappers call the original first and only observe.
import fs from 'node:fs';
import path from 'node:path';
import { syncBuiltinESMExports } from 'node:module';
import { AsyncLocalStorage } from 'node:async_hooks';
import { pathToFileURL } from 'node:url';

const release = process.env.SHADOW_RELEASE;
const statsDir = process.env.SHADOW_STATS_DIR;
if (release && statsDir) {
  const { MeshStore } = await import(pathToFileURL(path.join(release, 'dist/mesh.js')).href);
  const role = process.env.SHADOW_ROLE ?? path.basename(process.argv[1] ?? 'node');
  const counts = { role, pid: process.pid, startedAt: Date.now(), timeouts: 0, tries: 0, byMethod: {}, firstTimeoutAt: null,
    lastTimeoutAt: null, lastTimeout: null, times: [], peakRssMb: 0, heapLimitMb: 0 };
  const tryScope = new AsyncLocalStorage();
  const isTimeout = error => error?.code === 'FABRIC_MESH_LOCK_TIMEOUT' || error?.name === 'MeshLockTimeoutError';
  const wrapped = new WeakSet();
  // Exported for a Main that self-reloads onto another release in the same process.
  const wrapMeshStore = Store => {
    const proto = Store?.prototype;
    if (!proto || wrapped.has(proto)) return;
    wrapped.add(proto);
    // Full-budget lock entry points. A registry-fenced (withTryLock) or zero-wait caller fails
    // by design while the lock is busy: that is a bounded try, as L8 classifies it, not a timeout.
    for (const method of ['publish', 'publishBatch', 'put', 'delete', 'writeBatch', 'confirmWritable', 'exclusive']) {
      const original = proto[method];
      if (typeof original !== 'function' || original.constructor.name !== 'AsyncFunction') continue;
      proto[method] = async function (...args) {
        try { return await original.apply(this, args); }
        catch (error) {
          if (isTimeout(error)) {
            const bounded = tryScope.getStore() === true || (method === 'exclusive' && typeof args[1] === 'number' && args[1] < 1_000);
            if (bounded) counts.tries++;
            else {
              counts.timeouts++;
              counts.byMethod[method] = (counts.byMethod[method] ?? 0) + 1;
              counts.firstTimeoutAt ??= Date.now();
              counts.lastTimeoutAt = Date.now();
              // Each timeout's time, so the report can split start ramp, load window and teardown.
              if (counts.times.length < 10_000) counts.times.push(counts.lastTimeoutAt);
              counts.lastTimeout = String(error.message).slice(0, 300);
            }
          }
          throw error;
        }
      };
    }
    if (typeof proto.withTryLock === 'function') {
      const original = proto.withTryLock;
      proto.withTryLock = function (operation, ...rest) { return tryScope.run(true, () => original.call(this, operation, ...rest)); };
    }
  };
  wrapMeshStore(MeshStore);
  globalThis.__shadowWrapMeshStore = wrapMeshStore;

  // Hub lock wait and hold. Every protocol writes the owner record `token\npid\ncreatedAt\n...`,
  // created when the acquisition starts: protocol 1 writes it into <mesh>/.lock after mkdir (the
  // acquisition), protocol 2 into a staging directory renamed onto <mesh>/.lock (the acquisition).
  // The owner renames <mesh>/.lock to .lock.released.<token> on release.
  const hub = process.env.SHADOW_HUB ? path.resolve(process.env.SHADOW_HUB) : undefined;
  const BOUNDS = [5, 10, 25, 50, 100, 250, 500, 1000, 2000, 3000, 5000, 7500, 10000, 15000, Infinity];
  const lock = { bounds: BOUNDS.map(bound => (bound === Infinity ? null : bound)), minutes: {} };
  const bucket = minute => (lock.minutes[minute] ??= { n: 0, holdMs: 0, maxWaitMs: 0, maxHoldMs: 0, hist: BOUNDS.map(() => 0) });
  const held = new Map();
  const pending = new Map();
  if (hub) {
    const ownerPath = path.join(hub, '.lock', 'owner');
    const lockPath = path.join(hub, '.lock');
    const pendingPrefix = path.join(hub, '.lock.pending.');
    const releasedPrefix = path.join(hub, '.lock.released.');
    const acquired = record => {
      const [token, , created] = record.split('\n');
      const now = Date.now();
      const wait = Math.max(0, now - Number(created));
      if (!Number.isFinite(wait)) return;
      held.set(token, now);
      if (held.size > 1_000) held.delete(held.keys().next().value);
      const row = bucket(Math.floor(now / 60_000));
      row.n++;
      row.maxWaitMs = Math.max(row.maxWaitMs, wait);
      row.hist[BOUNDS.findIndex(bound => wait <= bound)]++;
    };
    const realWrite = fs.writeFileSync;
    const realRename = fs.renameSync;
    fs.writeFileSync = function (file, data, ...rest) {
      const out = realWrite.call(this, file, data, ...rest);
      try {
        if (typeof file === 'string' && typeof data === 'string') {
          if (file === ownerPath) acquired(data);
          else if (file.startsWith(pendingPrefix) && file.endsWith(`${path.sep}owner`)) pending.set(path.dirname(file), data);
        }
      } catch { /* observe only */ }
      return out;
    };
    fs.renameSync = function (from, to, ...rest) {
      const out = realRename.call(this, from, to, ...rest);
      try {
        if (to === lockPath && pending.has(from)) { acquired(pending.get(from)); pending.delete(from); }
        else if (from === lockPath && typeof to === 'string' && to.startsWith(releasedPrefix)) {
          const token = to.slice(releasedPrefix.length);
          const at = held.get(token);
          if (at !== undefined) {
            held.delete(token);
            const hold = Date.now() - at;
            const row = bucket(Math.floor(at / 60_000));
            row.holdMs += hold;
            row.maxHoldMs = Math.max(row.maxHoldMs, hold);
          }
        }
      } catch { /* observe only */ }
      return out;
    };
    syncBuiltinESMExports();
  }

  const file = path.join(statsDir, `proc-${role}-${process.pid}.json`);
  const save = () => {
    counts.peakRssMb = Math.max(counts.peakRssMb, Math.round(process.memoryUsage().rss / 1048576));
    try {
      fs.writeFileSync(`${file}.tmp`, JSON.stringify({ ...counts, lock, savedAt: Date.now() }));
      fs.renameSync(`${file}.tmp`, file);
    } catch { /* The stats directory goes with the throwaway root. */ }
  };
  try { counts.heapLimitMb = Math.round((await import('node:v8')).getHeapStatistics().heap_size_limit / 1048576); } catch { /* informational */ }
  setInterval(save, 5_000).unref();
  process.on('exit', save);
}
