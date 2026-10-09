import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { connect } from "nats";
import { afterEach, describe, expect, it } from "vitest";
import { NatsKvStateStore } from "../src/mesh/state-nats-kv.js";

const servers = process.env.FABRIC_NATS_TEST_SERVERS;
const roots: string[] = [];
const stores: NatsKvStateStore[] = [];
const identity = { id: "live-contract", name: "live contract", kind: "agent" as const };
const open = async (maxKeys?: number) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-live-kv-")); roots.push(root);
  const store = await NatsKvStateStore.open(root, { servers: servers!.split(","), experimentalNatsKv: true,
    ...(maxKeys !== undefined ? { maxKeys } : {}) });
  stores.push(store); return store;
};
afterEach(async () => {
  for (const store of stores.splice(0)) await store.close();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const bounded = async <T>(promise: Promise<T>): Promise<T> => {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try { return await Promise.race([promise, new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new Error("Live watch deadline spent")), 10_000); })]); }
  finally { clearTimeout(timer); }
};

describe.skipIf(!servers)("real NATS R3 state contract (no mocks)", () => {
  it("uses file-backed R3 and no direct reads; returns acknowledged revisions through every node", async () => {
    const store = await open();
    const entry = await store.put({ key: "topology/participants/" + "e".repeat(64), value: 1, identity, ifVersion: 0 });
    const nc = await connect({ servers: servers!.split(",") });
    try {
      const jsm = await nc.jetstreamManager(); const info = await jsm.streams.info(`KV_${store.bucket}`);
      expect(info.config.num_replicas).toBe(3); expect(info.config.storage).toBe("file"); expect(info.config.allow_direct).toBe(false);
      expect(info.cluster?.replicas).toHaveLength(2); expect(info.cluster?.leader).toBeTruthy();
      const perNode = [];
      for (const endpoint of servers!.split(",")) {
        const reader = await NatsKvStateStore.open(roots[0]!, { servers: endpoint, experimentalNatsKv: true });
        try { expect((await reader.get(entry.key))?.version).toBe(entry.version); perNode.push({ endpoint, version: (await reader.get(entry.key))?.version }); }
        finally { await reader.close(); }
      }
      if (process.env.FABRIC_NATS_EVIDENCE_DIR) fs.writeFileSync(path.join(process.env.FABRIC_NATS_EVIDENCE_DIR, "nats-r3-stream.json"), JSON.stringify({ info, perNode }, null, 2) + "\n");
    } finally { await nc.close(); }
  });
  it("pushes ordered PUT/DEL revisions across keys and filters decoded prefixes", async () => {
    const store = await open(); const watch = await store.watch("github-ingress/repo/");
    try {
      const expected: Array<{ key: string; version: number; operation: string }> = [];
      for (let n = 0; n < 30; n++) {
        const key = `github-ingress/repo/${n % 5}`;
        const entry = await store.put({ key, value: n, identity }); expected.push({ key, version: entry.version, operation: "put" });
        if (n % 3 === 0) {
          const deleted = await store.delete({ key, ifVersion: entry.version }); expected.push({ key, version: deleted.version!, operation: "delete" });
        }
        await store.put({ key: `github-ingress/repository/${n}`, value: "outside prefix", identity });
      }
      const events = await bounded((async () => {
        const result = [];
        for (let n = 0; n < expected.length; n++) { const next = await watch.next(); if (next.done) throw new Error("Watch ended early"); result.push(next.value); }
        return result;
      })());
      expect(events.map(({ key, version, operation }) => ({ key, version, operation }))).toEqual(expected);
      for (let n = 1; n < events.length; n++) expect(events[n]!.version).toBeGreaterThan(events[n - 1]!.version);
      if (process.env.FABRIC_NATS_EVIDENCE_DIR) fs.writeFileSync(path.join(process.env.FABRIC_NATS_EVIDENCE_DIR, "nats-watch-order.json"), JSON.stringify({ expected, actual: events }, null, 2) + "\n");
    } finally { watch.stop(); }
  }, 30_000);
  it("retains old fences on delete/recreate and rejects stale version zero", async () => {
    const store = await open(); const key = "actors/root.with:colon/a._-";
    const first = await store.put({ key, value: { text: "x".repeat(100 * 1024) }, identity, ifVersion: 0 });
    const deleted = await store.delete({ key, ifVersion: first.version });
    await expect(store.put({ key, value: 0, identity, ifVersion: 0 })).rejects.toThrow(/compare-and-swap/);
    await expect(store.delete({ key, ifVersion: first.version })).rejects.toThrow(/compare-and-swap/);
    expect(await store.version(key)).toBe(deleted.version);
    const next = await store.put({ key, value: 2, identity, ifVersion: deleted.version! });
    expect(next.version).toBeGreaterThan(deleted.version!);
  });
  it("rejects capacity overflow without evicting existing keys or fencing tombstones", async () => {
    const store = await open(2);
    await store.put({ key: "limit/a", value: 1, identity }); await store.put({ key: "limit/b", value: 2, identity });
    await expect(store.put({ key: "limit/c", value: 3, identity })).rejects.toThrow();
    expect((await store.listAll("limit/")).map(e => e.key)).toEqual(["limit/a", "limit/b"]);
  });
});
