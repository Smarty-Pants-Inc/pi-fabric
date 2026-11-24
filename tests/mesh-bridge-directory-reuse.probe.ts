import { createHash } from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { expect, it } from 'vitest';
import { MeshStore } from '../src/mesh/store.js';
import { StoreBridgeSide } from '../src/mesh/bridge.js';
import { writeHostLease, hostLeasePath } from '../src/topology/host-leases.js';
import { meshDirectoryStamp } from '../src/topology/publication-generation.js';

// Independent data-safety probe: actual WAL state, atomic lease writer, filesystem
// stamps and native fs.watch subscription. No fake clocks, watchers or state reads.
it('announces a real lease replacement even when its parent directory stamp aliases', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'judge-directory-reuse-'));
  const store = new MeshStore(root, 64 * 1024, 1_000, { stateBackend: 'sqlite' });
  const now = Date.now();
  const id = 'session:judge-stamp-00000000';
  const identity = { id, name: 'judge-stamp', kind: 'main' as const, sessionId: 'judge-stamp-00000000' };
  const host = { format: 1, id, rootId: id, identity, startedAt: now - 1_000,
    incarnationToken: 'judge-stamp-token', updatedAt: now, expiresAt: now + 1_000 };
  const key = 'topology/hosts/' + createHash('sha256').update(id).digest('hex');
  let close: (() => void) | undefined;
  let rawWatch: fs.FSWatcher | undefined;
  try {
    await store.put({ key, identity, value: host });
    const side = new StoreBridgeSide(store, 'remote');
    const replace = (n: number, extension = 0) => writeHostLease(root, { id, rootId: id, identityId: id,
      startedAt: host.startedAt, incarnationToken: host.incarnationToken,
      updatedAt: now + n, expiresAt: now + 60_000 + n + extension });
    let found = false;
    for (let attempt = 0; attempt < 300; attempt++) {
      const n = attempt * 2;
      replace(n);
      const before = meshDirectoryStamp(root, 'host-leases');
      const oldLeaf = fs.statSync(hostLeasePath(root, id), { bigint: true }).ino.toString();
      const changes: string[] = [], failures: string[] = [], rawEvents: string[] = [];
      rawWatch = fs.watch(path.join(root, 'host-leases'), (event, name) => rawEvents.push(`${event}:${String(name)}`));
      close = side.subscribeChanges(kind => changes.push(kind), error => failures.push(error.message));
      replace(n + 1, 30_000);
      const after = meshDirectoryStamp(root, 'host-leases');
      if (before === after) {
        found = true;
        const newLeaf = fs.statSync(hostLeasePath(root, id), { bigint: true }).ino.toString();
        const current = JSON.parse(fs.readFileSync(hostLeasePath(root, id), 'utf8'));
        await new Promise(resolve => setTimeout(resolve, 350));
        const fresh = await side.presence();
        console.log('DIRECTORY_ALIAS_PROBE', JSON.stringify({ attempt, sameDirectoryStamp: true,
          before, after, oldLeaf, newLeaf, leaseExpiry: current.expiresAt,
          expectedExpiry: now + 90_000 + n + 1, freshExpiry: fresh.hosts.find(h => h.record.id === id)?.expiresAt,
          changes, failures, rawEvents }));
        expect(failures).toEqual([]);
        expect(rawEvents.some(event => event.endsWith('.json'))).toBe(true);
        expect(current.expiresAt).toBe(now + 90_000 + n + 1);
        expect(fresh.hosts.find(h => h.record.id === id)?.expiresAt).toBe(current.expiresAt);
        expect(changes).toContain('lease');
        break;
      }
      close(); close = undefined;
      rawWatch.close(); rawWatch = undefined;
    }
    expect(found, 'counterexample not exercised: no natural directory-stamp collision in bounded probe').toBe(true);
  } finally {
    close?.();
    rawWatch?.close();
    store.closeState();
    fs.rmSync(root, { recursive: true, force: true });
  }
});
