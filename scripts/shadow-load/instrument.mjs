// Preloaded (node --import) into every shadow-test process that runs candidate code. It counts
// FABRIC_MESH_LOCK_TIMEOUT rejections of the candidate's own MeshStore lock entry points, so a
// release without L8 lock stats still reports its lock timeouts, and records the process's peak
// RSS. It changes no behaviour: only async methods are wrapped, and errors are rethrown as is.
import fs from 'node:fs';
import path from 'node:path';
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
  const proto = MeshStore.prototype;
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
  const file = path.join(statsDir, `proc-${role}-${process.pid}.json`);
  const save = () => {
    counts.peakRssMb = Math.max(counts.peakRssMb, Math.round(process.memoryUsage().rss / 1048576));
    try {
      fs.writeFileSync(`${file}.tmp`, JSON.stringify({ ...counts, savedAt: Date.now() }));
      fs.renameSync(`${file}.tmp`, file);
    } catch { /* The stats directory goes with the throwaway root. */ }
  };
  try { counts.heapLimitMb = Math.round((await import('node:v8')).getHeapStatistics().heap_size_limit / 1048576); } catch { /* informational */ }
  setInterval(save, 5_000).unref();
  process.on('exit', save);
}
