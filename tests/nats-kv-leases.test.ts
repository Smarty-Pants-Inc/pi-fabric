import { spawn } from "node:child_process";
import { once } from "node:events";
import { writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { Kvm, type KV } from "@nats-io/kv";
import { jetstream } from "@nats-io/jetstream";
import type { FabricHostLease } from "../src/topology/host-leases.js";
import { LeaseLostError, LeaseWaitTimeoutError, NatsKvLeaseStore, newLeaseIncarnation } from "../src/topology/nats-kv-leases.js";
import { artifactDirectory, deferred, hasNatsLeaseServer, NatsLeaseCluster, sleep } from "./helpers/nats-lease-cluster.js";

const lease = (id: string, identityId = "owner-a", ttlMs = 2_000, startedAt = Date.now()): FabricHostLease => {
  const now = Date.now();
  return { id, rootId: "root", identityId, startedAt, updatedAt: now, expiresAt: now + ttlMs };
};
const kvOf = (store: NatsKvLeaseStore): KV => (store as unknown as { kv: KV }).kv;

describe.skipIf(!hasNatsLeaseServer())("NatsKvLeaseStore real R3 conformance (sync_interval always; requires local binary)", () => {
  const cluster = new NatsLeaseCluster("conformance");
  let store: NatsKvLeaseStore, peer: NatsKvLeaseStore;
  beforeAll(async () => {
    await cluster.start(); store = cluster.store;
    peer = await NatsKvLeaseStore.open(await cluster.connection(), { bucket: cluster.bucket, maxLeaseMs: 5_000 });
  }, 40_000);
  afterAll(async () => { await cluster.close(); }, 15_000);

  it("maps acquire/renew/release/read/list and preserves file lease payload metadata", async () => {
    const proposed = { ...lease("mapping"), writer: { pid: 1, host: "test", releaseSha: "test", lockProtocol: 2,
      stateBackend: "prototype", startedAt: 1 }, session: { id: "session", startedAt: 1,
      updatedAt: Date.now(), expiresAt: Date.now() + 1_000 } };
    const first = (await store.acquire(proposed, newLeaseIncarnation()))!;
    expect(first.revision).toBeGreaterThan(0); expect(first.lease).toEqual(proposed);
    expect(await peer.acquire(lease("mapping", "owner-b"), newLeaseIncarnation())).toBeUndefined();
    const renewed = await store.renew(first, { ...proposed, updatedAt: Date.now(), expiresAt: Date.now() + 2_000 });
    expect(renewed.revision).toBeGreaterThan(first.revision);
    expect(await peer.read("mapping")).toEqual(renewed);
    expect((await peer.list()).get("mapping")).toEqual(renewed);
    expect(await store.release(first)).toBe(false);
    expect(await store.release(renewed)).toBe(true); expect(await store.read("mapping")).toBeUndefined();
    expect((await store.list()).has("mapping")).toBe(false);
  });

  it("successor publishes between predecessor check and renewal write: successor survives", async () => {
    const predecessor = (await store.acquire(lease("renew-race"), newLeaseIncarnation()))!;
    const checked = deferred(), resume = deferred();
    const read = store.read.bind(store);
    const spy = vi.spyOn(store, "read").mockImplementationOnce(async id => {
      const snapshot = await read(id); checked.resolve(); await resume.promise; return snapshot;
    });
    const renewing = store.renew(predecessor, lease("renew-race", "owner-a", 3_000, predecessor.lease.startedAt));
    const rejected = expect(renewing).rejects.toBeInstanceOf(LeaseLostError);
    await checked.promise;
    try {
      expect(await peer.release(predecessor)).toBe(true);
      const successor = (await peer.acquire(lease("renew-race", "owner-b"), newLeaseIncarnation()))!;
      resume.resolve(); await rejected;
      expect(await peer.read("renew-race")).toEqual(successor); await peer.release(successor);
    } finally { resume.resolve(); spy.mockRestore(); await renewing.catch(() => undefined); }
  });

  it("close-vs-successor cleanup: revision-checked delete leaves successor intact", async () => {
    const predecessor = (await store.acquire(lease("close-race"), newLeaseIncarnation()))!;
    const checked = deferred(), resume = deferred();
    const kv = kvOf(store), remove = kv.delete.bind(kv);
    const spy = vi.spyOn(kv, "delete").mockImplementationOnce(async (key, options) => {
      checked.resolve(); await resume.promise; return remove(key, options);
    });
    const closing = store.release(predecessor);
    await checked.promise;
    try {
      expect(await peer.release(predecessor)).toBe(true);
      const successor = (await peer.acquire(lease("close-race", "owner-b"), newLeaseIncarnation()))!;
      resume.resolve(); expect(await closing).toBe(false);
      expect(await peer.read("close-race")).toEqual(successor); await peer.release(successor);
    } finally { resume.resolve(); spy.mockRestore(); await closing.catch(() => undefined); }
  });

  it.each(["before", "after"])("crash %s publish completion: no stranded lock", async phase => {
    const id = `crash-${phase}`;
    const child = spawn("bun", [resolve("tests/helpers/nats-lease-crash.ts"), cluster.servers.join(","),
      cluster.bucket, id, phase], { stdio: ["ignore", "pipe", "pipe"] });
    const exited = once(child, "exit");
    let stderr = "";
    child.stderr!.on("data", chunk => { stderr += chunk.toString(); });
    try {
      await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error(`Crash child not ready: ${stderr}`)), 5_000);
        let output = "";
        child.stdout!.on("data", chunk => {
          output += chunk.toString();
          if (output.includes(`PUBLISH_${phase.toUpperCase()}`)) { clearTimeout(timer); resolve(); }
        });
        child.once("error", error => { clearTimeout(timer); reject(error); });
        child.once("exit", () => { clearTimeout(timer); reject(new Error(`Early crash child exit: ${stderr}`)); });
      });
      const published = await peer.read(id);
      expect(Boolean(published)).toBe(phase === "after");
      child.kill("SIGKILL"); await exited;
      const start = performance.now();
      const successor = await peer.acquireWaiting(() => lease(id, "owner-b"), newLeaseIncarnation(), { waitMs: 3_000 });
      expect(successor.lease.identityId).toBe("owner-b");
      expect(performance.now() - start).toBeLessThan(2_000);
      expect(await peer.read(id)).toEqual(successor); await peer.release(successor);
    } finally {
      if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
      await exited;
    }
  });

  it("two incarnations in the same millisecond stay distinct; stale renew and close fail", async () => {
    const sameMs = Date.now(), one = newLeaseIncarnation(), two = newLeaseIncarnation();
    expect(one).not.toBe(two);
    const first = (await store.acquire(lease("same-ms", "same-owner", 2_000, sameMs), one))!;
    await store.release(first);
    const second = (await peer.acquire(lease("same-ms", "same-owner", 2_000, sameMs), two))!;
    expect(second.revision).toBeGreaterThan(first.revision);
    await expect(store.renew(first, lease("same-ms", "same-owner", 2_000, sameMs))).rejects.toBeInstanceOf(LeaseLostError);
    expect(await store.release(first)).toBe(false); expect(await peer.read("same-ms")).toEqual(second);
    await peer.release(second);
  });

  it("timeout retry uses watcher wakeups, not read polling; release cannot be missed", async () => {
    const owned = (await store.acquire(lease("wait-timeout", "owner-a", 4_000), newLeaseIncarnation()))!;
    const getSpy = vi.spyOn(kvOf(peer), "get");
    const watch = kvOf(peer).watch.bind(kvOf(peer));
    const watchSpy = vi.spyOn(kvOf(peer), "watch");
    const incarnation = newLeaseIncarnation();
    const start = performance.now();
    await expect(peer.acquireWaiting(() => lease("wait-timeout", "owner-b"), incarnation, { waitMs: 160 }))
      .rejects.toBeInstanceOf(LeaseWaitTimeoutError);
    const readsAtTimeout = getSpy.mock.calls.length;
    expect(readsAtTimeout).toBeLessThanOrEqual(3); // Initial probe + initial watch snapshot, no 100ms read loop.
    expect(performance.now() - start).toBeGreaterThanOrEqual(150);
    const watching = deferred();
    watchSpy.mockImplementationOnce(async options => { const result = await watch(options); watching.resolve(); return result; });
    const waiting = peer.acquireWaiting(() => lease("wait-timeout", "owner-b"), incarnation, { waitMs: 2_000 });
    await watching.promise; await store.release(owned);
    const successor = await waiting;
    expect(successor.incarnation).toBe(incarnation);
    expect(getSpy.mock.calls.length - readsAtTimeout).toBeLessThanOrEqual(5);
    expect(watchSpy.mock.calls.length).toBe(2);
    console.log(JSON.stringify({ scenario: "timeout-watch-retry", readsAtTimeout,
      totalReads: getSpy.mock.calls.length, watchSubscriptions: watchSpy.mock.calls.length, pollingTimers: 0 }));
    getSpy.mockRestore(); watchSpy.mockRestore(); await peer.release(successor);
  });

  it("logical TTL wakes once without polling, rejects expired renew, and allows successor", async () => {
    const owned = (await store.acquire(lease("expiry", "owner-a", 120), newLeaseIncarnation()))!;
    const getSpy = vi.spyOn(kvOf(peer), "get");
    const successor = await peer.acquireWaiting(() => lease("expiry", "owner-b"), newLeaseIncarnation(), { waitMs: 2_000 });
    expect(Date.now()).toBeGreaterThanOrEqual(owned.lease.expiresAt);
    expect(getSpy.mock.calls.length).toBeLessThanOrEqual(5);
    await expect(store.renew(owned, lease("expiry"))).rejects.toBeInstanceOf(LeaseLostError);
    getSpy.mockRestore(); expect(await store.release(owned)).toBe(false); await peer.release(successor);
  });

  it("racing acquires admit only one owner and binding rejects unsafe existing bucket", async () => {
    const results = await Promise.all(Array.from({ length: 16 }, (_, i) =>
      (i % 2 ? store : peer).acquire(lease("acquire-race", `owner-${i}`), newLeaseIncarnation())));
    const winners = results.filter(result => result !== undefined);
    expect(winners).toHaveLength(1); await store.release(winners[0]!);
    const unsafeBucket = `${cluster.bucket}_UNSAFE`;
    await new Kvm(jetstream(cluster.nc)).create(unsafeBucket, { replicas: 1, ttl: 5_000 });
    await expect(NatsKvLeaseStore.open(cluster.nc, { bucket: unsafeBucket, maxLeaseMs: 5_000 })).rejects.toThrow("violates");
  });

  it("abort, invalid TTL/owner/token, and uncertain renewal all fail closed", async () => {
    await expect(store.acquire(lease("too-long", "owner-a", 5_001), newLeaseIncarnation())).rejects.toThrow("TTL");
    await expect(store.acquire(lease("bad-incarnation"), "same-ms")).rejects.toThrow("incarnation");
    const future = Date.now() + 60_000;
    await expect(store.acquire({ ...lease("future"), updatedAt: future, expiresAt: future + 1_000 },
      newLeaseIncarnation())).rejects.toThrow("future-dated");
    const owned = (await store.acquire(lease("fail-closed"), newLeaseIncarnation()))!;
    await expect(store.renew(owned, lease("fail-closed", "different-owner"))).rejects.toBeInstanceOf(LeaseLostError);
    await expect(store.release({ ...owned, revision: 0 })).rejects.toBeInstanceOf(LeaseLostError);
    const kv = kvOf(store), update = kv.update.bind(kv);
    const spy = vi.spyOn(kv, "update").mockImplementationOnce(async (...args) => {
      await update(...args); throw new Error("Injected lost publish acknowledgement");
    });
    await expect(store.renew(owned, lease("fail-closed", "owner-a", 3_000, owned.lease.startedAt)))
      .rejects.toThrow("lost publish acknowledgement");
    spy.mockRestore();
    expect(await store.release(owned)).toBe(false);
    const current = (await peer.read("fail-closed"))!;
    expect(current.revision).toBeGreaterThan(owned.revision);
    const controller = new AbortController();
    const waiting = peer.acquireWaiting(() => lease("fail-closed", "owner-b"), newLeaseIncarnation(),
      { waitMs: 2_000, signal: controller.signal });
    const rejected = expect(waiting).rejects.toThrow("cancel wait");
    await sleep(30); controller.abort(new Error("cancel wait")); await rejected;
    await peer.release(current);
  });

  it("a lost acquire acknowledgement grants no ownership and retries wait for TTL without polling", async () => {
    const kv = kvOf(store), create = kv.create.bind(kv);
    const spy = vi.spyOn(kv, "create").mockImplementationOnce(async (...args) => {
      await create(...args); throw new Error("Injected lost acquire acknowledgement");
    });
    const id = "lost-acquire-ack", incarnation = newLeaseIncarnation();
    await expect(store.acquire(lease(id, "owner-a", 300), incarnation))
      .rejects.toThrow("lost acquire acknowledgement");
    spy.mockRestore();
    const committed = (await peer.read(id))!;
    expect(committed.incarnation).toBe(incarnation);
    const getSpy = vi.spyOn(kvOf(peer), "get");
    const successor = await peer.acquireWaiting(() => lease(id, "owner-b"), newLeaseIncarnation(), { waitMs: 2_000 });
    expect(successor.revision).toBeGreaterThan(committed.revision);
    expect(getSpy.mock.calls.length).toBeLessThanOrEqual(5);
    getSpy.mockRestore(); expect(await store.release(committed)).toBe(false); await peer.release(successor);
  });

  it("measures acquire/renew latency p50/p99 with sync_interval always", async () => {
    const acquire: number[] = [], renew: number[] = [];
    const samples = 200, warmup = 20;
    for (let i = 0; i < samples + warmup; i++) {
      const start = performance.now();
      const owned = (await store.acquire(lease(`latency-${i}`), newLeaseIncarnation()))!;
      const acquiredAt = performance.now();
      const renewed = await store.renew(owned, lease(`latency-${i}`, "owner-a", 2_000, owned.lease.startedAt));
      const renewedAt = performance.now();
      await store.release(renewed);
      if (i >= warmup) { acquire.push(acquiredAt - start); renew.push(renewedAt - acquiredAt); }
    }
    const summary = (values: number[]) => {
      const sorted = values.slice().sort((a, b) => a - b);
      return { p50Ms: sorted[Math.ceil(sorted.length * .50) - 1], p99Ms: sorted[Math.ceil(sorted.length * .99) - 1],
        minMs: sorted[0], maxMs: sorted.at(-1) };
    };
    const result = { samples, warmup, serverVersion: cluster.nc.info?.version,
      replicas: 3, processes: 3, physicalHosts: 1, syncInterval: "always", storage: "file", readMode: "leader API",
      includes: "Adapter end-to-end: leader precheck + CAS publish acknowledgement; sequential uncontended requests",
      acquire: summary(acquire), renew: summary(renew) };
    writeFileSync(join(artifactDirectory(), "latency.json"), JSON.stringify(result, null, 2) + "\n");
    console.log(`LATENCY ${JSON.stringify(result)}`);
    expect(acquire).toHaveLength(samples); expect(renew).toHaveLength(samples);
  }, 30_000);
});
