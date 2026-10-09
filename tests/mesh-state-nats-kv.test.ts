import fs from "node:fs";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { KvEntry, KvWatchOptions, MsgHdrs, StreamConfig } from "nats";
import { MeshBatchConflictError } from "../src/mesh/state-file.js";
import { openAsyncMeshStateStore } from "../src/mesh/state-async.js";
import { decodeNatsKvKey, encodeNatsKvKey, isSupportedNatsKvServer, natsKvBucketForRoot, natsKvPrefixFilter,
  NatsKvStateStore } from "../src/mesh/state-nats-kv.js";

const fake = vi.hoisted(() => ({ connect: vi.fn() }));
vi.mock("nats", async load => ({ ...await load<typeof import("nats")>(), connect: fake.connect }));
const identity = { id: "tester", name: "tester", kind: "agent" as const };
const request = { key: "actors/root/id", value: { n: 1 }, identity };
const conflict = (code: number) => Object.assign(new Error("JetStream test failure"), { api_error: { err_code: code } });

class Queue<T> implements AsyncIterableIterator<T> {
  values: T[] = [];
  stopped = false;
  waiters: Array<(value: IteratorResult<T>) => void> = [];
  [Symbol.asyncIterator]() { return this; }
  push(value: T) { if (!this.stopped) { const resolve = this.waiters.shift(); if (resolve) resolve({ done: false, value }); else this.values.push(value); } }
  async next(): Promise<IteratorResult<T>> {
    if (this.values.length) return { done: false, value: this.values.shift()! };
    if (this.stopped) return { done: true, value: undefined };
    return new Promise(resolve => this.waiters.push(resolve));
  }
  stop() { this.stopped = true; for (const resolve of this.waiters.splice(0)) resolve({ done: true, value: undefined }); }
  async return(): Promise<IteratorResult<T>> { this.stop(); return { done: true, value: undefined }; }
}
class Broker {
  revision = 0;
  state = new Map<string, KvEntry>();
  watchers: Queue<KvEntry>[] = [];
  config: StreamConfig | undefined;
  get = vi.fn(async (key: string) => this.state.get(key) ?? null);
  write(key: string, value: Uint8Array, expected?: number, operation: KvEntry["operation"] = "PUT"): number {
    if (expected !== undefined && expected !== (this.state.get(key)?.revision ?? 0)) throw conflict(10071);
    const revision = ++this.revision;
    const raw: KvEntry = { bucket: "test", key, value: Uint8Array.from(value), created: new Date(), revision,
      operation, length: value.length, json: <T>() => JSON.parse(Buffer.from(value).toString()) as T,
      string: () => Buffer.from(value).toString() };
    this.state.set(key, raw);
    for (const queue of this.watchers) queue.push(raw);
    return revision;
  }
  put = vi.fn(async (key: string, value: Uint8Array, options?: { previousSeq?: number }) => this.write(key, value, options?.previousSeq));
  update = vi.fn(async (key: string, value: Uint8Array, expected: number) => this.write(key, value, expected));
  keys = vi.fn(async (filter: string) => {
    const queue = new Queue<string>();
    const parent = filter.slice(0, -1);
    for (const entry of this.state.values()) if (entry.operation === "PUT" && entry.key.startsWith(parent)) queue.push(entry.key);
    queue.stop(); return queue;
  });
  watch = vi.fn(async (options: KvWatchOptions) => {
    const queue = new Queue<KvEntry>();
    if (options.include === "") for (const raw of this.state.values()) queue.push(raw);
    this.watchers.push(queue); return queue;
  });
  close = vi.fn(async () => undefined);
  publish = vi.fn(async (subject: string, value: Uint8Array, options: { headers: MsgHdrs }) => {
    expect(options.headers.get("KV-Operation")).toBe("DEL");
    return { seq: this.write(subject.split(".").slice(2).join("."), value,
      Number(options.headers.get("Nats-Expected-Last-Subject-Sequence")), "DEL") };
  });
  info = vi.fn(async () => { if (!this.config) throw conflict(10059); return { config: this.config }; });
  add = vi.fn(async (config: StreamConfig) => { this.config = config; return { config }; });
  view = vi.fn(async () => this);
  connection(version = "2.14.7") {
    return { info: { version, max_payload: 2 * 1024 * 1024 }, close: this.close,
      jetstream: () => ({ views: { kv: this.view }, publish: this.publish }),
      jetstreamManager: async () => ({ streams: { info: this.info, add: this.add } }) };
  }
}
let broker: Broker;
const stores: NatsKvStateStore[] = [];
const open = async () => {
  const store = await NatsKvStateStore.open("/mesh/root", { servers: "nats://localhost:4222", experimentalNatsKv: true });
  stores.push(store); return store;
};
beforeEach(() => { broker = new Broker(); fake.connect.mockReset(); fake.connect.mockResolvedValue(broker.connection()); });
afterEach(async () => { for (const store of stores.splice(0)) await store.close(); });

describe("Fabric key codec", () => {
  const shapes = ["a", "a/", "a//", "a///b", "a/b/", "a._-:0/b.c:d", "a/__proto__x", "a/.hidden",
    "topology/participants/" + "f".repeat(64), "actors/root.with:colon/id-0_1", "github-ingress/owner/repo/issues/6477",
    "github-ingress:owner/repo/pr/1", "a".repeat(256)];
  it.each(shapes)("round trips %s without wildcard or empty subject tokens", key => {
    const encoded = encodeNatsKvKey(key);
    expect(decodeNatsKvKey(encoded)).toBe(key);
    expect(encoded).toMatch(/^[a-zA-Z0-9._=\-]+$/);
    expect(encoded).not.toContain("..");
    expect(encoded).not.toMatch(/[*>/]/);
  });
  it("has no collision across punctuation, slash and empty segments", () => {
    const keys = ["a.b", "a/b", "a:b", "a_b", "a-b", "a//b", "a/s/b", "a/", "a/s", "a/_", "a/."];
    expect(new Set(keys.map(encodeNatsKvKey)).size).toBe(keys.length);
  });
  it.each(["", "/a", "a=2e", "a*", "a>z", "a/constructor", "a/__proto__", "a/prototype", "a".repeat(257), "a/é"])("refuses invalid key %s", key => {
    expect(() => encodeNatsKvKey(key)).toThrow(/Invalid Fabric/);
  });
  it.each(["a", "k.a", "k.sa=2E", "k.sa..sb", "k.sa=61", "k.sa=3d", "k.s"])("refuses non-canonical encoding %s", key => {
    expect(() => decodeNatsKvKey(key)).toThrow();
  });
  it("matches every key from the retained local real-state samples", () => {
    const sample = process.env.FABRIC_NATS_KEY_SAMPLE;
    if (!sample) return; // optional local corpus, no fleet path assumption in CI
    const samples = JSON.parse(fs.readFileSync(sample, "utf8")) as Array<{ keys: string[] }>;
    const keys = samples.flatMap(value => value.keys);
    expect(keys.length).toBeGreaterThan(0);
    for (const key of keys) expect(decodeNatsKvKey(encodeNatsKvKey(key)), key).toBe(key);
  });
  it("uses parent subjects for partial-token prefixes", () => {
    expect(natsKvPrefixFilter("")).toBe("k.>");
    expect(natsKvPrefixFilter("top")).toBe("k.>");
    expect(natsKvPrefixFilter("topology/participants/")).toBe("k.stopology.sparticipants.>");
    expect(natsKvPrefixFilter("topology/part")).toBe("k.stopology.>");
    expect(natsKvPrefixFilter("a//")).toBe("k.sa.s.>");
    expect(() => natsKvPrefixFilter("a/*")).toThrow();
  });
  it("names one deterministic bucket per normalized root", () => {
    expect(natsKvBucketForRoot("/mesh/root/../root")).toBe(natsKvBucketForRoot("/mesh/root"));
    expect(natsKvBucketForRoot("/mesh/other")).not.toBe(natsKvBucketForRoot("/mesh/root"));
    expect(natsKvBucketForRoot("/mesh/root")).toMatch(/^FABRIC_STATE_[a-f0-9]{64}$/);
  });
});

describe("NATS KV single-key adapter (mock protocol, NOT real R3 evidence)", () => {
  it("requires the flag before connecting, including the async selector", async () => {
    await expect(NatsKvStateStore.open("/mesh", { servers: "nats://localhost:4222", experimentalNatsKv: false })).rejects.toThrow(/experimentalNatsKv/);
    await expect(openAsyncMeshStateStore("/mesh", { backend: "nats-kv", nats: { servers: "local", experimentalNatsKv: false } })).rejects.toThrow(/experimentalNatsKv/);
    expect(fake.connect).not.toHaveBeenCalled();
  });
  it("provisions R3 file storage with no expiration, no eviction or direct follower reads", async () => {
    const store = await open();
    expect(broker.config).toMatchObject({ name: `KV_${store.bucket}`, num_replicas: 3, storage: "file", max_msgs_per_subject: 1,
      max_msgs: 100_000, max_age: 0, discard: "new", allow_direct: false, deny_delete: true, deny_purge: true });
    expect(broker.view).toHaveBeenCalledWith(store.bucket, { bindOnly: true, allow_direct: false, timeout: 5_000 });
    const other = await open();
    expect(other.bucket).toBe(store.bucket);
    expect(broker.add).toHaveBeenCalledTimes(1);
  });
  it("refuses a conflicting existing bucket instead of mutating or falling back", async () => {
    await open(); broker.config!.allow_direct = true;
    await expect(open()).rejects.toThrow(/Incompatible/);
    expect(broker.add).toHaveBeenCalledTimes(1);
    expect(broker.close).toHaveBeenCalledTimes(1);
  });
  it.each(["2.14.6", "2.13.9", "2.14.7-beta.1", "unknown"])("refuses unqualified server %s and closes", async version => {
    fake.connect.mockResolvedValueOnce(broker.connection(version));
    await expect(open()).rejects.toThrow(/2.14.7/);
    expect(broker.close).toHaveBeenCalledOnce(); expect(broker.add).not.toHaveBeenCalled();
  });
  it("recognizes qualified stable versions", () => {
    expect(isSupportedNatsKvServer("2.14.7")).toBe(true); expect(isSupportedNatsKvServer("2.15.0")).toBe(true);
    expect(isSupportedNatsKvServer("3.0.0")).toBe(true); expect(isSupportedNatsKvServer("1.99.99")).toBe(false);
  });
  it("uses global KV revisions and update(expectedRevision) for CAS", async () => {
    const store = await open();
    const first = await store.put({ ...request, ifVersion: 0 });
    await store.put({ ...request, key: "other", value: 9 });
    const next = await store.compareAndSwap({ ...request, value: 2, ifVersion: first.version });
    expect(next.version).toBe(3);
    expect(broker.update).toHaveBeenCalledWith(encodeNatsKvKey(request.key), expect.any(Buffer), first.version);
    await expect(store.put({ ...request, ifVersion: first.version })).rejects.toBeInstanceOf(MeshBatchConflictError);
    expect((await store.get(request.key))?.value).toBe(2);
  });
  it("retains tombstone fencing, exact delete PubAck, and never auto-resurrects version 0", async () => {
    const store = await open();
    const entry = await store.put({ ...request, ifVersion: 0 });
    await expect(store.delete({ key: request.key, ifVersion: entry.version + 1 })).rejects.toBeInstanceOf(MeshBatchConflictError);
    const deleted = await store.delete({ key: request.key, ifVersion: entry.version });
    expect(deleted).toEqual({ deleted: true, version: 2 });
    expect(await store.get(request.key)).toBeUndefined(); expect(await store.version(request.key)).toBe(2);
    await expect(store.put({ ...request, ifVersion: 0 })).rejects.toBeInstanceOf(MeshBatchConflictError);
    expect(await store.delete({ key: request.key, ifVersion: 2 })).toEqual({ deleted: false });
    await expect(store.delete({ key: request.key, ifVersion: entry.version })).rejects.toBeInstanceOf(MeshBatchConflictError);
    const recreated = await store.put({ ...request, ifVersion: 2 });
    expect(recreated.version).toBe(3);
    await expect(store.put({ ...request, ifVersion: entry.version })).rejects.toBeInstanceOf(MeshBatchConflictError);
  });
  it("keeps caller and reader values detached; permits 100 KiB and rejects oversized before publishing", async () => {
    const store = await open(); const value = { text: "x".repeat(100 * 1024), count: 1 };
    const promise = store.put({ ...request, value }); value.count = 2;
    const written = await promise; expect((written.value as typeof value).count).toBe(1);
    (written.value as typeof value).count = 3;
    expect((await store.get(request.key))?.value).toEqual({ text: "x".repeat(100 * 1024), count: 1 });
    await expect(store.put({ ...request, value: "x".repeat(store.maxValueBytes) })).rejects.toThrow(/exceeds/);
    expect(broker.put).toHaveBeenCalledTimes(1);
  });
  it("uses keys(filter subject), decoded prefix matching, and locale ordering", async () => {
    const store = await open();
    for (const key of ["topology/party/1", "topology/participants/2", "topology/participants/1", "other/a", "topology/a"])
      await store.put({ ...request, key });
    expect((await store.listAll("topology/part")).map(e => e.key)).toEqual(["topology/participants/1", "topology/participants/2", "topology/party/1"]);
    expect(broker.keys).toHaveBeenLastCalledWith("k.stopology.>");
    expect((await store.list("topology/participants/", 1)).map(e => e.key)).toEqual(["topology/participants/1"]);
    expect(broker.keys).toHaveBeenLastCalledWith("k.stopology.sparticipants.>");
    await store.delete({ key: "topology/participants/1" });
    expect((await store.listAll("topology/participants/")).map(e => e.key)).toEqual(["topology/participants/2"]);
  });
  it("watches push events in revision order including delete, without polling get", async () => {
    const store = await open(); const watch = await store.watch("actors/");
    const first = await store.put(request); const second = await store.put({ ...request, value: 2 });
    const deleted = await store.delete({ key: request.key, ifVersion: second.version });
    const gets = broker.get.mock.calls.length;
    const events = [(await watch.next()).value!, (await watch.next()).value!, (await watch.next()).value!];
    expect(events.map(e => [e.operation, e.version])).toEqual([["put", first.version], ["put", second.version], ["delete", deleted.version]]);
    expect(broker.get.mock.calls.length).toBe(gets);
    expect(broker.watch).toHaveBeenCalledWith({ key: "k.sactors.>", ignoreDeletes: false, include: "updates" });
    watch.stop(); expect((await watch.next()).done).toBe(true);
  });
  it("stops watches on abort, return before first next, and store close", async () => {
    const store = await open(); const controller = new AbortController();
    const first = await store.watch("", { signal: controller.signal }); const pending = first.next();
    controller.abort(); expect((await pending).done).toBe(true);
    const second = await store.watch(); await second.return!();
    expect(broker.watchers[1]?.stopped).toBe(true);
    const third = await store.watch(); const waiting = third.next(); await store.close();
    expect((await waiting).done).toBe(true); await expect(store.get("a")).rejects.toThrow(/closed/);
  });
  it("closes a watch thrown into before first next", async () => {
    const store = await open(); const watch = await store.watch(); const error = new Error("caller cancelled");
    await expect(watch.throw!(error)).rejects.toBe(error); expect(broker.watchers[0]?.stopped).toBe(true);
  });
  it("refuses authority operations after reconnecting to an old server", async () => {
    const connection = broker.connection(); fake.connect.mockResolvedValueOnce(connection); const store = await open();
    connection.info.version = "2.14.6";
    await expect(store.get("a")).rejects.toThrow(/minimum version/);
    await expect(store.put(request)).rejects.toThrow(/minimum version/); expect(broker.put).not.toHaveBeenCalled();
  });
  it("validates a concurrent bucket creator without reconfiguring the winner", async () => {
    broker.add.mockImplementationOnce(async config => { broker.config = config; throw conflict(10058); });
    const store = await open(); expect(store.bucket).toBe(natsKvBucketForRoot("/mesh/root"));
    expect(broker.info).toHaveBeenCalledTimes(2); expect(broker.add).toHaveBeenCalledOnce();
  });
  it("rejects insufficient max_payload and invalid capacities before creating a stream", async () => {
    const connection = broker.connection(); connection.info.max_payload = 1_024; fake.connect.mockResolvedValueOnce(connection);
    await expect(open()).rejects.toThrow(/max_payload/); expect(broker.add).not.toHaveBeenCalled();
    await expect(NatsKvStateStore.open("/mesh", { servers: "local", experimentalNatsKv: true, maxKeys: 0 })).rejects.toThrow(/maxKeys/);
    expect(fake.connect).toHaveBeenCalledTimes(1);
  });
  it("propagates connectivity errors, never disguises them as CAS or absence", async () => {
    const store = await open(); const error = new Error("offline"); broker.get.mockRejectedValueOnce(error);
    await expect(store.get("a")).rejects.toBe(error); broker.update.mockRejectedValueOnce(error);
    await expect(store.put({ ...request, ifVersion: 1 })).rejects.toBe(error);
  });
  it.each([-1, 0.5, Number.MAX_SAFE_INTEGER + 1, NaN])("rejects invalid revision %s before writing", async revision => {
    const store = await open(); await expect(store.put({ ...request, ifVersion: revision })).rejects.toThrow(/revision/);
    await expect(store.delete({ key: "a", ifVersion: revision })).rejects.toThrow(/revision/); expect(broker.put).not.toHaveBeenCalled();
  });
});
