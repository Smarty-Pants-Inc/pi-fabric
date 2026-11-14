import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import { MeshStore } from '../src/mesh/store.js';
import { RootInbox } from '../src/topology/root-inbox.js';
import * as shared from '../src/topology/root-inbox-shared.js';
import type { SessionReceiptManager } from '../src/core/session-receipts.js';

// Data-safety / cache invalidation at the real RootInbox boundary.
const cleanup: Array<() => unknown | Promise<unknown>> = [];
afterEach(async () => {
  for (const close of cleanup.splice(0).reverse()) await close();
});

const topic = 'fleet.work.rfl1965';
const peer = { id: 'session:peer', name: 'peer', kind: 'main' as const };
const me = { id: 'session:judge', name: 'judge', kind: 'main' as const };
const encode = (entries: unknown[]) => entries.map(entry => JSON.stringify(entry) + '\n').join('');
const unrelated = (text = 'unchanged-tail'.repeat(30)) => ({
  type: 'message', message: { role: 'user', content: text },
});
const native = (id: string, at = Date.now()) => ({
  type: 'custom_message',
  customType: 'pi-fabric-agent-message',
  timestamp: at,
  details: { id, from: { id: peer.id }, data: { deliveryId: id, topic } },
});

function fixture(initial: unknown[] = []) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'rfl1965-'));
  cleanup.push(() => fs.rmSync(root, { recursive: true, force: true }));
  const file = path.join(root, 'session.jsonl');
  fs.writeFileSync(file, encode(initial));
  const manager: SessionReceiptManager = {
    getEntries: () => [],
    getSessionFile: () => file,
    isPersisted: () => true,
    getSessionId: () => me.id,
  };
  const issuer = shared.rootInboxReceiptIssuer();
  // The original RED baseline lacks this factory; retain its equivalent verifier fallback.
  const factory = shared.rootInboxReceiptVerifier;
  const verifier: shared.RootInboxReceiptVerifier = factory
    ? factory(() => issuer)
    : (details, carrier, accepted) => shared.verifyRootInboxReceipt(issuer, details, carrier, accepted);
  const read = () => shared.confirmedRootInboxSession(manager, verifier);
  const full = () => shared.rootInboxSession(
    fs.readFileSync(file, 'utf8').split('\n').slice(0, -1).map(line => {
      try { return JSON.parse(line); } catch { return null; }
    }),
    verifier,
  );
  const equal = () => {
    const actual = read(), expected = full();
    expect([...actual.delivered!]).toEqual([...expected.delivered!]);
    expect([...actual.deliveredAt!]).toEqual([...expected.deliveredAt!]);
    return actual;
  };
  const mesh = () => {
    const store = new MeshStore(path.join(root, 'mesh'), 64 * 1024, 500);
    cleanup.push(() => store.closeState());
    return store;
  };
  const inbox = (store: MeshStore) => {
    const inbox = new RootInbox(store, me, () => [me.id], { steerGraceMs: 0 });
    cleanup.push(() => inbox.close());
    inbox.start();
    return inbox;
  };
  return { file, read, full, equal, mesh, inbox };
}

describe('RFL independent receipt-cache adversaries', () => {
  it('does not miss a middle-history native receipt after same-inode same-size rewrite (20527)', async () => {
    const at = Date.now();
    const h = fixture([native('old', at), unrelated()]);
    h.equal();
    const old = fs.statSync(h.file);
    // Actual filesystem mutation, no mocked stat. Last 64 bytes and old length stay identical.
    await new Promise(resolve => setTimeout(resolve, 15));
    fs.writeFileSync(h.file, encode([native('new', at), unrelated()]));
    fs.utimesSync(h.file, old.atime, old.mtime);
    const after = fs.statSync(h.file);
    expect(after.ino).toBe(old.ino);
    expect(after.size).toBe(old.size);
    expect(after.ctimeMs).not.toBe(old.ctimeMs);
    const mesh = h.mesh(), inbox = h.inbox(mesh);
    const event = await mesh.publish({
      topic, kind: 'handoff', from: peer, to: me.id,
      text: 'new delivered natively', data: { deliveryId: 'new' },
    });
    const stale = h.read(), truth = h.full();
    const match = shared.eventReceipts(event).some(id => truth.delivered!.has(id));
    expect(match).toBe(true);
    const offered = await inbox.next(stale);
    console.log('RFL same-size rewrite', JSON.stringify({
      oldIno: old.ino, newIno: after.ino, size: after.size,
      oldCtime: old.ctimeMs, newCtime: after.ctimeMs,
      staleMatches: shared.eventReceipts(event).some(id => stale.delivered!.has(id)),
      coldMatches: match, offered: offered.events.map(event => event.data),
    }));
    expect(offered.events, 'a canonical native receipt must suppress its duplicate shadow').toEqual([]);
  });

  it('does not hide unseen work after an earlier receipt is removed but the tail is unchanged (20527)', async () => {
    const h = fixture([native('old'), unrelated()]);
    h.read();
    // Rebuild transcript prefix without this receipt, while retaining exact length and tail.
    const first = fs.readFileSync(h.file, 'utf8').split('\n')[0]!;
    const replacement = JSON.stringify({ type: 'custom', data: 'x'.repeat(first.length - 27) });
    const padded = replacement.length <= first.length
      ? replacement + ' '.repeat(first.length - replacement.length)
      : ' '.repeat(first.length);
    fs.writeFileSync(h.file, padded + '\n' + encode([unrelated()]));
    const mesh = h.mesh(), inbox = h.inbox(mesh);
    const event = await mesh.publish({
      topic, kind: 'handoff', from: peer, to: me.id,
      text: 'unseen old work', data: { deliveryId: 'old' },
    });
    const truth = h.full();
    expect(truth.delivered!.size).toBe(0);
    const offered = await inbox.next(h.read());
    expect(
      offered.events.map(event => event.id),
      'removed receipt must not permanently advance past unseen work',
    ).toEqual([event.id]);
  });

  it('does not miss receipts inserted before a long unchanged suffix (non-append growth)', () => {
    const h = fixture();
    const first = encode([native('old')]);
    const padded = (value: unknown) => JSON.stringify(value).padEnd(511, ' ') + '\n';
    const suffix = padded(unrelated('padding')).repeat(8);
    fs.writeFileSync(h.file, first + suffix);
    h.equal();
    // Insert a whole valid receipt before a repeated 512-byte suffix. The 64-byte guard at
    // the prior complete offset is identical although a receipt appeared in the prefix.
    fs.writeFileSync(h.file, first + padded(native('new')) + suffix);
    h.equal();
  });

  // fabric-store owns both cursor failures: https://github.com/Smarty-Pants-Inc/smarty-dev/issues/20542
  for (const mode of ['same-size-in-place', 'truncate-observed'] as const) {
    it.skip(`a warmed mesh cursor sees replacement work after ${mode} (20542)`, async () => {
      const h = fixture([native('delivered')]);
      const mesh = h.mesh(), inbox = h.inbox(mesh);
      fs.writeFileSync(path.join(mesh.root, 'generation'), '7');
      const original = await mesh.publish({
        topic, kind: 'handoff', from: peer, to: 'session:other',
        text: 'message', data: { deliveryId: 'brand-new' },
      });
      // Original is not addressed to this Main; advances cursor and read hints.
      await inbox.next(h.read());
      const file = path.join(mesh.root, 'events.jsonl');
      const old = fs.readFileSync(file, 'utf8');
      const replacement = {
        ...original,
        id: original.id.replace(/^./, original.id[0] === 'a' ? 'b' : 'a'),
        sequence: original.sequence + 1,
        to: me.id,
      };
      const body = encode([replacement]);
      expect(body.length).toBe(old.length);
      if (mode === 'truncate-observed') {
        fs.truncateSync(file, 0);
        await inbox.next(h.read());
      }
      fs.writeFileSync(file, body);
      fs.writeFileSync(path.join(mesh.root, 'sequence'), String(replacement.sequence));
      expect(
        (await inbox.next(h.read())).events.map(event => event.id),
        'fresh work above the inbox high-water mark must not be hidden by a stale byte-offset hint',
      ).toEqual([replacement.id]);
    });
  }
});
