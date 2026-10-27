import { DeliverPolicy, JetStreamApiError, type ConsumerConfig, type ConsumerMessages, type JetStreamClient, type JsMsg, type PushConsumer } from "@nats-io/jetstream";
import type { KV } from "@nats-io/kv";
import { describe, expect, it, vi } from "vitest";
import { NatsKvStore } from "../src/transport/nats/kv.js";

// Keep: watcher-state fsync amplification and broker quota refusals are expensive
// to reproduce. Exercise snapshot/ready/live delivery through the config boundary.
const quota = (err_code: number) => new JetStreamApiError({ code: 500, err_code,
  description: err_code === 10028 ? "insufficient memory resources available" : "resource limits exceeded for account" });
const fixture = (bucket: string, failure?: unknown, fileFailure?: unknown) => {
  let callback: ((msg: JsMsg) => void) | undefined;
  let sequence = 0;
  let finish!: () => void;
  const closed = new Promise<void>((resolve) => { finish = resolve; });
  const messages = {
    status: () => (async function* () { await closed; })(), closed: () => closed,
    stop: () => finish(), close: async () => { finish(); },
  } as unknown as ConsumerMessages;
  const deliver = (n: number) => callback?.({
    subject: `$KV.${bucket}.machine-a.p.record`,
    data: new TextEncoder().encode(JSON.stringify({ owner: "watch-test", value: { n } })),
    info: { consumer: "watch-consumer", deliverySequence: ++sequence, streamSequence: sequence, pending: 0 },
  } as JsMsg);
  const consumer = {
    info: async () => ({ name: "watch-consumer", num_pending: 1 }),
    consume: async (options: { callback: (msg: JsMsg) => void }) => {
      callback = options.callback;
      deliver(1);
      return messages;
    },
  } as unknown as PushConsumer;
  const getPushConsumer = vi.fn(async (_stream: string, config: Partial<ConsumerConfig>) => {
    if (config.mem_storage && failure) throw failure;
    if (!config.mem_storage && fileFailure) throw fileFailure;
    return consumer;
  });
  const js = { consumers: { getPushConsumer } } as unknown as JetStreamClient;
  const store = () => new NatsKvStore({} as KV, { bucket, ttlMs: 15_000, markerTtlMs: 300_000 }, js);
  return { getPushConsumer, store, deliver };
};
const exercise = async (f: ReturnType<typeof fixture>) => {
  const controller = new AbortController();
  const watch = f.store().watch<{ n: number }>("machine-a.p.*", controller.signal)[Symbol.asyncIterator]();
  try {
    expect((await watch.next()).value).toMatchObject({ type: "put", entry: { value: { n: 1 }, revision: 1 } });
    expect((await watch.next()).value).toEqual({ type: "ready" });
    f.deliver(2);
    expect((await watch.next()).value).toMatchObject({ type: "put", entry: { value: { n: 2 }, revision: 2 } });
  } finally { controller.abort(); await watch.return?.(); }
};
const creationFailure = async (f: ReturnType<typeof fixture>, error: unknown) => {
  const controller = new AbortController();
  const watch = f.store().watch("machine-a.p.*", controller.signal)[Symbol.asyncIterator]();
  try { await expect(watch.next()).rejects.toBe(error); }
  finally { controller.abort(); await watch.return?.(); }
};

describe("KV watch memory consumer storage", () => {
  it("requests in-memory state without changing snapshot or live delivery", async () => {
    const f = fixture("memory_watch");
    await exercise(f);
    expect(f.getPushConsumer.mock.calls).toEqual([["KV_memory_watch", {
      filter_subjects: "$KV.memory_watch.machine-a.p.*", deliver_policy: DeliverPolicy.LastPerSubject, mem_storage: true,
    }]]);
  });
  it.each([10028, 10002])("retries quota error %i once on file storage and keeps delivering", async (code) => {
    const bucket = `quota_watch_${code}`, f = fixture(bucket, quota(code));
    const warning = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    try {
      await exercise(f);
      expect(f.getPushConsumer.mock.calls).toEqual([true, false].map(mem_storage => [`KV_${bucket}`, {
        filter_subjects: `$KV.${bucket}.machine-a.p.*`, deliver_policy: DeliverPolicy.LastPerSubject, mem_storage,
      }]));
      expect(warning.mock.calls).toEqual([[`[fabric:nats:kv-watch] ${JSON.stringify({
        outcome: "file-storage-fallback", code: `JS_API_${code}`, bucket,
      })}`]]);
    } finally { warning.mockRestore(); }
  });
  it("warns once per bucket across concurrent watches and separate stores", async () => {
    const first = fixture("quota_warning_once", quota(10028));
    const second = fixture("quota_warning_once", quota(10028));
    const other = fixture("quota_warning_other", quota(10028));
    const warning = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    try {
      await Promise.all([exercise(first), exercise(second), exercise(other)]);
      expect(warning.mock.calls.map(([line]) => JSON.parse(String(line).slice("[fabric:nats:kv-watch] ".length)).bucket).sort())
        .toEqual(["quota_warning_once", "quota_warning_other"]);
      expect(first.getPushConsumer).toHaveBeenCalledTimes(2);
      expect(second.getPushConsumer).toHaveBeenCalledTimes(2);
    } finally { warning.mockRestore(); }
  });
  it.each([
    ["permission", new JetStreamApiError({ code: 403, err_code: 10028, description: "permission denied" })],
    ["invalid config", new JetStreamApiError({ code: 400, err_code: 10012, description: "invalid consumer configuration" })],
    ["consumer quota", new JetStreamApiError({ code: 400, err_code: 10026, description: "maximum consumers limit reached" })],
    ["disk quota", new JetStreamApiError({ code: 500, err_code: 10047, description: "insufficient storage resources available" })],
    ["general resources", new JetStreamApiError({ code: 400, err_code: 10023, description: "insufficient resources" })],
    ["message-only error", new Error("insufficient memory resources available")],
  ])("does not downgrade on %s", async (_label, error) => {
    const f = fixture("no_storage_downgrade", error);
    const warning = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    try {
      await creationFailure(f, error);
      expect(f.getPushConsumer).toHaveBeenCalledTimes(1);
      expect(warning).not.toHaveBeenCalled();
    } finally { warning.mockRestore(); }
  });
  it("propagates a failed file retry without a third creation attempt", async () => {
    const error = quota(10028), f = fixture("quota_file_failure", error, error);
    const warning = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    try {
      await creationFailure(f, error);
      expect(f.getPushConsumer.mock.calls.map(([, config]) => config.mem_storage)).toEqual([true, false]);
      expect(warning).toHaveBeenCalledTimes(1);
    } finally { warning.mockRestore(); }
  });
});
