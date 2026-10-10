import fs from "node:fs";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { JsMsg, KvEntry, KvWatchOptions, MsgHdrs, OrderedConsumerOptions, StreamConfig } from "nats";
import { MeshBatchConflictError } from "../src/mesh/state-file.js";
import { openAsyncMeshStateStore } from "../src/mesh/state-async.js";
import { decodeNatsKvKey, encodeNatsKvKey, isSupportedNatsKvServer, natsKvBucketForRoot, natsKvPrefixFilter,
  NatsKvStateStore, NatsKvListTimeoutError } from "../src/mesh/state-nats-kv.js";

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
class KeyPage extends Queue<JsMsg> {
  close = vi.fn(async () => { this.stop(); });
}
class Broker {
  keyReads = 0;
  keyPages: KeyPage[] = [];
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
  consumerGet = vi.fn(async (_stream: string, options: Partial<OrderedConsumerOptions>) => {
    const filter = options.filterSubjects as string;
    const parent = filter.slice(filter.indexOf(".k.") + 1, -1);
    const snapshot = [...this.state.values()].filter(entry => entry.key.startsWith(parent) && entry.revision >= (options.opt_start_seq ?? 0))
      .sort((a, b) => a.revision - b.revision);
    let index = 0;
    return {
      info: vi.fn(async () => ({ num_pending: snapshot.length - index })),
      fetch: vi.fn(async ({ max_messages }: { max_messages: number; expires: number }) => {
        const page = new KeyPage(); this.keyPages.push(page);
        for (let n = 0; n < max_messages && index < snapshot.length; n++) {
          const raw = snapshot[index++]!; this.keyReads++;
          page.push({ subject: `${filter.slice(0, filter.indexOf(".k."))}.${raw.key}`, headers: { get: () => raw.operation === "PUT" ? "" : raw.operation },
            info: { pending: snapshot.length - index, streamSequence: raw.revision } } as unknown as JsMsg);
        }
        page.stop(); return page;
      }),
      delete: vi.fn(async () => true),
    };
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
      jetstream: () => ({ views: { kv: this.view }, publish: this.publish, consumers: { get: this.consumerGet } }),
      jetstreamManager: async () => ({ streams: { info: this.info, add: this.add } }) };
  }
}
let broker: Broker;
const stores: NatsKvStateStore[] = [];
const open = async (options: { timeoutMs?: number } = {}) => {
  const store = await NatsKvStateStore.open("/mesh/root", { servers: "nats://localhost:4222", experimentalNatsKv: true, ...options });
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
    expect(natsKvPrefixFilter("shared/missing/")).toBe("k.sshared.smissing.>");
    expect(natsKvPrefixFilter("shared/a.b:c/")).toBe("k.sshared.sa=2eb=3ac.>");
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
  it.each([10071, 10164])("normalizes sequence API error %s for virgin and positive CAS", async code => {
    const store = await open();
    broker.put.mockRejectedValueOnce(conflict(code));
    await expect(store.put({ ...request, ifVersion: 0 })).rejects.toBeInstanceOf(MeshBatchConflictError);
    const entry = await store.put(request);
    broker.update.mockRejectedValueOnce(conflict(code));
    await expect(store.compareAndSwap({ ...request, ifVersion: entry.version })).rejects.toMatchObject({
      key: request.key, expected: entry.version, found: entry.version,
    });
    expect(broker.update).toHaveBeenCalledOnce();
  });
  it.each([10071, 10164])("does not retry an explicit conditional delete conflict %s", async code => {
    const store = await open(); const entry = await store.put(request);
    broker.publish.mockRejectedValueOnce(conflict(code));
    await expect(store.delete({ key: request.key, ifVersion: entry.version })).rejects.toBeInstanceOf(MeshBatchConflictError);
    expect(broker.publish).toHaveBeenCalledOnce();
    expect((await store.get(request.key))?.version).toBe(entry.version);
  });
  it.each([10071, 10164])("retries an unconditional delete sequence conflict %s", async code => {
    const store = await open(); await store.put(request);
    broker.publish.mockRejectedValueOnce(conflict(code));
    expect(await store.delete({ key: request.key })).toEqual({ deleted: true, version: 2 });
    expect(broker.publish).toHaveBeenCalledTimes(2);
  });
  it.each([10071, 10164])("bounds unconditional delete conflict %s retries at eight", async code => {
    const store = await open(); await store.put(request);
    broker.publish.mockRejectedValue(conflict(code));
    await expect(store.delete({ key: request.key })).rejects.toBeInstanceOf(MeshBatchConflictError);
    expect(broker.publish).toHaveBeenCalledTimes(8);
  });
  it("propagates unrelated numeric API errors unchanged for put and delete", async () => {
    const store = await open(); const entry = await store.put(request); const error = conflict(10077);
    broker.update.mockRejectedValueOnce(error);
    await expect(store.put({ ...request, ifVersion: entry.version })).rejects.toBe(error);
    broker.publish.mockRejectedValueOnce(error);
    await expect(store.delete({ key: request.key })).rejects.toBe(error);
    expect(broker.publish).toHaveBeenCalledOnce();
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
    // A bounded page follows KV iteration order, not the globally lowest key.
    expect((await store.list("topology/participants/", 1)).map(e => e.key)).toEqual(["topology/participants/2"]);
    expect(broker.consumerGet).toHaveBeenLastCalledWith(`KV_${store.bucket}`, {
      filterSubjects: `$KV.${store.bucket}.k.stopology.sparticipants.>`, deliver_policy: "last_per_subject", headers_only: true, inactive_threshold: 5_000,
    });
    await store.delete({ key: "topology/participants/1" });
    expect((await store.listAll("topology/participants/")).map(e => e.key)).toEqual(["topology/participants/2"]);
  });
  it("consumes only ten KV keys and leader reads out of 10,000 for a limit-ten listing", async () => {
    const store = await open();
    for (let n = 0; n < 10_000; n++) await store.put({ ...request, key: `shared/item-${String(n).padStart(5, "0")}` });
    const page = await store.list("shared/", 10);
    expect(page.map(entry => entry.key)).toEqual(Array.from({ length: 10 }, (_, n) => `shared/item-${String(n).padStart(5, "0")}`));
    expect(broker.keyReads).toBe(10);
    expect(broker.get).toHaveBeenCalledTimes(10);
    expect(broker.keys).not.toHaveBeenCalled();
    const consumer = await broker.consumerGet.mock.results[0]!.value;
    expect(consumer.fetch).toHaveBeenCalledExactlyOnceWith({ max_messages: 10, expires: expect.any(Number) });
    expect(consumer.fetch.mock.calls[0]![0].expires).toBeLessThanOrEqual(5_000);
    expect(await consumer.info()).toEqual({ num_pending: 9_990 });
    expect(broker.keyPages[0]!.close).toHaveBeenCalledOnce();
    expect(consumer.delete).toHaveBeenCalledOnce();
  });
  it("examines at most one page for a missing partial prefix among 10,000 siblings", async () => {
    const store = await open();
    for (let n = 0; n < 10_000; n++) await store.put({ ...request, key: `shared/item-${n}` });
    expect(await store.list("shared/missing", 1)).toEqual([]);
    expect(broker.keyReads).toBeLessThanOrEqual(1);
    expect(broker.keyPages).toHaveLength(1);
    expect(broker.get).not.toHaveBeenCalled();
  });
  it("paginates sparse partial prefixes and concurrent deletes without refilling a page", async () => {
    const store = await open();
    for (const key of ["topology/other", "topology/part-gone", "topology/part-z", "topology/part-a", "topology/part-unused"])
      await store.put({ ...request, key });
    broker.get.mockImplementationOnce(async () => null);
    const first = await store.listPage("topology/part", 2);
    expect(first).toEqual({ entries: [], examined: 2, nextRevision: 3 });
    expect(broker.keyReads).toBe(2); expect(broker.keyPages).toHaveLength(1);
    const second = await store.listPage("topology/part", 2, first.nextRevision);
    expect(second.entries.map(entry => entry.key)).toEqual(["topology/part-a", "topology/part-z"]);
    expect(second).toMatchObject({ examined: 2, nextRevision: 5 });
    const last = await store.listPage("topology/part", 2, second.nextRevision);
    expect(last.entries.map(entry => entry.key)).toEqual(["topology/part-unused"]);
    expect(last.examined).toBe(1); expect(last.nextRevision).toBeUndefined();
    expect(broker.get.mock.calls.map(([key]) => decodeNatsKvKey(key))).toEqual(["topology/part-gone", "topology/part-z", "topology/part-a", "topology/part-unused"]);
  });
  it("closes the bounded page and deletes its consumer when a leader read fails", async () => {
    const store = await open(); await store.put(request);
    const error = new Error("leader offline"); broker.get.mockRejectedValueOnce(error);
    await expect(store.list("actors/", 1)).rejects.toBe(error);
    expect(broker.keyPages[0]!.close).toHaveBeenCalledOnce();
    const consumer = await broker.consumerGet.mock.results[0]!.value;
    expect(consumer.delete).toHaveBeenCalledOnce();
  });
  it("counts tombstones against the examined limit and resumes without reading their values", async () => {
    const store = await open(); await store.put({ ...request, key: "shared/gone" });
    await store.delete({ key: "shared/gone" }); await store.put({ ...request, key: "shared/live" });
    broker.get.mockClear();
    const first = await store.listPage("shared/", 1);
    expect(first).toEqual({ entries: [], examined: 1, nextRevision: 3 });
    expect(broker.keyReads).toBe(1); expect(broker.get).not.toHaveBeenCalled();
    const last = await store.listPage("shared/", 1, first.nextRevision);
    expect(last.entries.map(entry => entry.key)).toEqual(["shared/live"]);
    expect(last.examined).toBe(1); expect(last.nextRevision).toBeUndefined();
    expect(broker.get).toHaveBeenCalledExactlyOnceWith(encodeNatsKvKey("shared/live"));
  });
  it("never delivers siblings for an absent slash-aligned namespace", async () => {
    const store = await open();
    for (let n = 0; n < 100; n++) await store.put({ ...request, key: `shared/sibling/${n}` });
    expect(await store.listPage("shared/missing/", 1)).toEqual({ entries: [], examined: 0 });
    expect(broker.keyReads).toBe(0); expect(broker.get).not.toHaveBeenCalled();
    expect(broker.consumerGet.mock.calls[0]![1].filterSubjects).toBe(`$KV.${store.bucket}.k.sshared.smissing.>`);
  });
  it("bounds a tombstone-only corpus by examined headers", async () => {
    const store = await open();
    for (let n = 0; n < 100; n++) { const key = `shared/gone-${n}`; await store.put({ ...request, key }); await store.delete({ key }); }
    broker.get.mockClear();
    const page = await store.listPage("shared/", 1);
    expect(page).toEqual({ entries: [], examined: 1, nextRevision: 3 });
    expect(broker.keyReads).toBe(1); expect(broker.get).not.toHaveBeenCalled();
  });
  it("shares one total deadline across multiple slow leader reads and owns cleanup", async () => {
    const store = await open({ timeoutMs: 100 });
    for (let n = 0; n < 10; n++) await store.put({ ...request, key: `shared/item-${n}` });
    const original = broker.get.getMockImplementation()!;
    let now = 0;
    const clock = vi.spyOn(performance, "now").mockImplementation(() => now);
    broker.get.mockImplementation(async key => { now += 60; return original(key); });
    try {
      // No real sleeps: 2 successful RPCs spend 120ms of the ONE 100ms budget.
      // Even already-resolved promises must not bypass an expired monotonic deadline.
      await expect(store.listPage("shared/", 10)).rejects.toMatchObject({ name: "NatsKvListTimeoutError", code: "NATS_KV_LIST_TIMEOUT", examined: 2, timeoutMs: 100 });
      expect(broker.get).toHaveBeenCalledTimes(2);
      expect(broker.keyPages[0]!.close).toHaveBeenCalledOnce();
      const consumer = await broker.consumerGet.mock.results[0]!.value;
      expect(consumer.delete).toHaveBeenCalledOnce();
    } finally { clock.mockRestore(); }
  });
  it("bounds cleanup and preserves an earlier read error", async () => {
    const store = await open({ timeoutMs: 30 }); await store.put(request);
    const consumer = await broker.consumerGet("", { filterSubjects: `$KV.${store.bucket}.k.>` });
    let release!: (value: boolean) => void;
    consumer.delete.mockReturnValueOnce(new Promise(resolve => { release = resolve; }));
    broker.consumerGet.mockResolvedValueOnce(consumer);
    const error = new Error("leader offline"); broker.get.mockRejectedValueOnce(error);
    await expect(store.list("actors/", 1)).rejects.toBe(error);
    expect(consumer.delete).toHaveBeenCalledOnce(); release(true);
  });
  it("times out header iteration and releases the owned page", async () => {
    const store = await open({ timeoutMs: 30 }); await store.put(request);
    const consumer = await broker.consumerGet("", { filterSubjects: `$KV.${store.bucket}.k.>` });
    const page = new KeyPage(); consumer.fetch.mockResolvedValueOnce(page); broker.consumerGet.mockResolvedValueOnce(consumer);
    await expect(store.list("actors/", 1)).rejects.toBeInstanceOf(NatsKvListTimeoutError);
    expect(page.close).toHaveBeenCalledOnce(); expect(page.stopped).toBe(true); expect(consumer.delete).toHaveBeenCalledOnce();
  });
  it.each([0, -1, 0.5, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1])("rejects invalid continuation revision %s", async start => {
    const store = await open(); await expect(store.listPage("shared/", 1, start)).rejects.toThrow(/startRevision/);
    expect(broker.consumerGet).not.toHaveBeenCalled();
  });
  it("does not fetch an empty listing and still deletes the owned consumer", async () => {
    const store = await open(); expect(await store.list("shared/", 10)).toEqual([]);
    const consumer = await broker.consumerGet.mock.results[0]!.value;
    expect(consumer.fetch).not.toHaveBeenCalled(); expect(consumer.delete).toHaveBeenCalledOnce();
    expect(broker.get).not.toHaveBeenCalled();
  });
  it("rejects an expired page with pending keys instead of reporting false absence", async () => {
    const store = await open(); await store.put(request);
    const consumer = await broker.consumerGet("", { filterSubjects: `$KV.${store.bucket}.k.>` });
    const page = new KeyPage(); page.stop(); consumer.fetch.mockResolvedValueOnce(page);
    broker.consumerGet.mockResolvedValueOnce(consumer);
    await expect(store.list("actors/", 1)).rejects.toThrow(/listing page timed out/);
    expect(page.close).toHaveBeenCalledOnce(); expect(consumer.delete).toHaveBeenCalledOnce();
  });
  it("preserves a leader read failure even if consumer cleanup fails too", async () => {
    const store = await open(); await store.put(request);
    const consumer = await broker.consumerGet("", { filterSubjects: `$KV.${store.bucket}.k.>` });
    consumer.delete.mockRejectedValueOnce(new Error("cleanup offline")); broker.consumerGet.mockResolvedValueOnce(consumer);
    const error = new Error("leader offline"); broker.get.mockRejectedValueOnce(error);
    await expect(store.list("actors/", 1)).rejects.toBe(error);
    expect(consumer.delete).toHaveBeenCalledOnce(); expect(broker.keyPages[0]!.close).toHaveBeenCalledOnce();
  });
  it.each([0, -1, 0.5, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1])("rejects invalid listing limit %s before reading KV", async limit => {
    const store = await open(); await expect(store.list("", limit)).rejects.toThrow(/limit/);
    expect(broker.consumerGet).not.toHaveBeenCalled(); expect(broker.keys).not.toHaveBeenCalled(); expect(broker.get).not.toHaveBeenCalled();
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
  it("does not grant read or publish authority when eligibility changes during an awaited RPC", async () => {
    const connection = broker.connection(); fake.connect.mockResolvedValueOnce(connection); const store = await open();
    broker.get.mockImplementationOnce(async () => { connection.info.version = "2.14.6"; return null; });
    await expect(store.get("a")).rejects.toThrow(/minimum version/);
    connection.info.version = "2.14.7";
    broker.put.mockImplementationOnce(async () => { connection.info.version = "2.14.6"; return 1; });
    await expect(store.put(request)).rejects.toThrow(/minimum version/);
  });
  it("returns one shared close promise and awaits the transport even for concurrent closers", async () => {
    const store = await open(); let release!: () => void;
    const closed = new Promise<undefined>(resolve => { release = () => resolve(undefined); }); broker.close.mockReturnValueOnce(closed);
    const first = store.close(), second = store.close(); expect(first).toBe(second);
    let finished = false; second.then(() => { finished = true; }); await Promise.resolve(); expect(finished).toBe(false);
    await expect(store.get("a")).rejects.toThrow(/closed/);
    release(); await Promise.all([first, second]); expect(broker.close).toHaveBeenCalledOnce();
  });
  it.each([-1, 0.5, Number.MAX_SAFE_INTEGER + 1, NaN])("rejects invalid revision %s before writing", async revision => {
    const store = await open(); await expect(store.put({ ...request, ifVersion: revision })).rejects.toThrow(/revision/);
    await expect(store.delete({ key: "a", ifVersion: revision })).rejects.toThrow(/revision/); expect(broker.put).not.toHaveBeenCalled();
  });
});
