import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { openAsyncMeshStateStore, type AsyncMeshStateStore } from "../src/mesh/state-async.js";

const identity = { id: "contract", name: "contract", kind: "agent" as const };
const roots: string[] = [];
const stores: AsyncMeshStateStore[] = [];
afterEach(async () => {
  for (const store of stores.splice(0)) await store.close();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const open = async (kind: "file" | "sqlite" | "nats-kv") => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-async-contract-")); roots.push(root);
  const store = await openAsyncMeshStateStore(root, kind === "nats-kv"
    ? { backend: kind, nats: { servers: process.env.FABRIC_NATS_TEST_SERVERS!.split(","), experimentalNatsKv: true } } : { backend: kind });
  stores.push(store); return store;
};

// The older file/SQLite suite also requires per-key revision arithmetic and atomic writeBatch.
// KV intentionally changes arithmetic to global stream revisions and cannot claim atomic batches.
// This suite is the common SINGLE-KEY semantic contract, with no exact numerical assumptions.
for (const kind of ["file", "sqlite", "nats-kv"] as const) {
  describe.skipIf(kind === "nats-kv" && !process.env.FABRIC_NATS_TEST_SERVERS)(`${kind} common async single-key contract`, () => {
    it("defaults to file when no selector is specified", async () => {
      if (kind !== "file") return;
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-default-contract-")); roots.push(root);
      const store = await openAsyncMeshStateStore(root); stores.push(store); expect(store.kind).toBe("file");
    });
    it("creates, detaches reads, rejects CAS conflict, updates and conditionally deletes", async () => {
      const store = await open(kind); expect(store.kind).toBe(kind);
      expect(await store.get("ns/a")).toBeUndefined();
      expect(await store.delete({ key: "ns/a", ifVersion: 0 })).toEqual({ deleted: false });
      const value = { n: 1 }; const first = await store.put({ key: "ns/a", value, identity, ifVersion: 0 });
      value.n = 99; expect((await store.get("ns/a"))?.value).toEqual({ n: 1 });
      const read = await store.get("ns/a"); (read!.value as { n: number }).n = 88;
      expect((await store.get("ns/a"))?.value).toEqual({ n: 1 });
      await expect(store.put({ key: "ns/a", value: 9, identity, ifVersion: 0 })).rejects.toThrow(/compare-and-swap failed/);
      const second = await store.put({ key: "ns/a", value: 2, identity, ifVersion: first.version });
      expect(second.version).toBeGreaterThan(first.version);
      await expect(store.delete({ key: "ns/a", ifVersion: first.version })).rejects.toThrow(/compare-and-swap failed/);
      const deleted = await store.delete({ key: "ns/a", ifVersion: second.version });
      expect(deleted.deleted).toBe(true); expect(deleted.version).toBeGreaterThan(second.version);
      expect(await store.get("ns/a")).toBeUndefined();
      await expect(store.put({ key: "ns/a", value: 3, identity, ifVersion: 0 })).rejects.toThrow(/compare-and-swap failed/);
      const recreated = await store.put({ key: "ns/a", value: 3, identity, ifVersion: deleted.version! });
      expect(recreated.version).toBeGreaterThan(deleted.version!);
      await expect(store.put({ key: "ns/a", value: 4, identity, ifVersion: first.version })).rejects.toThrow(/compare-and-swap failed/);
    });
    it("accepts 100 KiB values, enumerates prefixes in locale order and excludes deletes", async () => {
      const store = await open(kind); const value = { text: "x".repeat(100 * 1024) };
      await store.put({ key: "github-ingress/repo/b", value, identity });
      await store.put({ key: "github-ingress/repo/a", value: 1, identity });
      await store.put({ key: "github-ingress/other/a", value: 2, identity });
      expect((await store.get("github-ingress/repo/b"))?.value).toEqual(value);
      expect((await store.listAll("github-ingress/repo/")).map(e => e.key)).toEqual(["github-ingress/repo/a", "github-ingress/repo/b"]);
      expect((await store.list("github-ingress/repo/", 1)).map(e => e.key)).toEqual(["github-ingress/repo/a"]);
      await store.delete({ key: "github-ingress/repo/a" });
      expect((await store.listAll("github-ingress/repo/")).map(e => e.key)).toEqual(["github-ingress/repo/b"]);
    });
    it("allows exactly one winner among eight stale-CAS contenders", async () => {
      const store = await open(kind); const first = await store.put({ key: "race/a", value: -1, identity });
      const results = await Promise.allSettled(Array.from({ length: 8 }, (_, n) => store.put({ key: "race/a", value: n, identity, ifVersion: first.version })));
      expect(results.filter(r => r.status === "fulfilled")).toHaveLength(1);
      expect(results.filter(r => r.status === "rejected")).toHaveLength(7);
      for (const result of results) if (result.status === "rejected") expect(result.reason).toMatchObject({ message: expect.stringMatching(/compare-and-swap failed/) });
    });
  });
}
