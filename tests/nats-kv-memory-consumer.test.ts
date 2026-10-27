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
  const deliver = (n: number, gap = 0) => callback?.({
    subject: `$KV.${bucket}.machine-a.p.record`,
    data: new TextEncoder().encode(JSON.stringify({ owner: "watch-test", value: { n } })),
    info: { consumer: "watch-consumer", deliverySequence: sequence += gap + 1, streamSequence: sequence, pending: 0 },
  } as JsMsg);
  const consumer = {
    info: async () => ({ name: "watch-consumer", num_pending: 1,
      config: { deliver_subject: "watch-inbox", idle_heartbeat: 30_000_000_000 } }),
    delete: vi.fn(async () => true),
    consume: vi.fn(() => { throw new Error("ordered delivery would race cleanup with SDK resets"); }),
  } as unknown as PushConsumer;
  const delivery = {
    consume: async (options: { callback: (msg: JsMsg) => void }) => {
      callback = options.callback;
      deliver(1);
      return messages;
    },
  } as unknown as PushConsumer;
  const getBoundPushConsumer = vi.fn(async () => delivery);
  const getPushConsumer = vi.fn(async (_stream: string, config: Partial<ConsumerConfig>) => {
    if (config.mem_storage && failure) throw failure;
    if (!config.mem_storage && fileFailure) throw fileFailure;
    return consumer;
  });
  const js = { consumers: { getPushConsumer, getBoundPushConsumer } } as unknown as JetStreamClient;
  const store = () => new NatsKvStore({} as KV, { bucket, ttlMs: 15_000, markerTtlMs: 300_000 }, js);
  return { getPushConsumer, getBoundPushConsumer, store, deliver, consumer, delivery, messages };
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
  // Keep: smarty-dev#17172 — a watch owned server state past its delivery lifetime.
  it.each([false, true])("deletes its owned consumer on return (file fallback=%s)", async (fallback) => {
    const f = fixture(`cleanup_return_${fallback}`, fallback ? quota(10028) : undefined);
    const warning = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    try {
      await exercise(f);
      expect(f.consumer.delete).toHaveBeenCalledTimes(1);
    } finally { warning.mockRestore(); }
  });

  it.each(["closed", "sequence gap"])("deletes the old consumer before a %s restart and leaves other watches alone", async (cause) => {
    const old = fixture("cleanup_restart"), replacement = fixture("cleanup_restart"), other = fixture("cleanup_other");
    old.getPushConsumer.mockResolvedValueOnce(old.consumer).mockResolvedValueOnce(replacement.consumer);
    old.getBoundPushConsumer.mockResolvedValueOnce(old.delivery).mockResolvedValueOnce(replacement.delivery);
    const controller = new AbortController(), otherController = new AbortController();
    const watch = old.store().watch("machine-a.p.*", controller.signal)[Symbol.asyncIterator]();
    const otherWatch = other.store().watch("machine-a.p.*", otherController.signal)[Symbol.asyncIterator]();
    try {
      expect((await watch.next()).value?.type).toBe("put");
      expect((await watch.next()).value?.type).toBe("ready");
      await otherWatch.next();
      if (cause === "closed") old.messages.stop();
      else old.deliver(2, 1);
      expect((await watch.next()).value).toEqual({ type: "reset" });
      expect((await watch.next()).value?.type).toBe("put");
      expect(old.consumer.delete).toHaveBeenCalledTimes(1);
      expect(replacement.consumer.delete).not.toHaveBeenCalled();
      expect(other.consumer.delete).not.toHaveBeenCalled();
    } finally {
      controller.abort(); otherController.abort();
      await watch.return?.(); await otherWatch.return?.();
    }
    expect(replacement.consumer.delete).toHaveBeenCalledTimes(1);
    expect(other.consumer.delete).toHaveBeenCalledTimes(1);
  });

  it.each(["info", "consume"] as const)("deletes after a post-creation %s failure without masking it", async (stage) => {
    const f = fixture(`cleanup_setup_${stage}`), error = new Error(`injected ${stage} failure`);
    if (stage === "info") vi.spyOn(f.consumer, "info").mockRejectedValueOnce(error);
    else vi.spyOn(f.delivery, "consume").mockRejectedValueOnce(error);
    await creationFailure(f, error);
    expect(f.consumer.delete).toHaveBeenCalledTimes(1);
  });

  it.each(["return", "abort"] as const)("deletes on %s without another event", async (end) => {
    const f = fixture(`cleanup_${end}`), controller = new AbortController();
    const watch = f.store().watch("machine-a.p.*", controller.signal)[Symbol.asyncIterator]();
    try {
      await watch.next(); await watch.next();
      if (end === "abort") {
        const pending = watch.next();
        controller.abort();
        expect((await pending).done).toBe(true);
      } else expect((await watch.return?.())?.done).toBe(true);
      expect(f.consumer.delete).toHaveBeenCalledTimes(1);
    } finally { controller.abort(); await watch.return?.(); }
  });

  it("deletes on abort even while the caller is paused at a yielded event", async () => {
    const f = fixture("cleanup_paused_abort"), controller = new AbortController();
    const watch = f.store().watch("machine-a.p.*", controller.signal)[Symbol.asyncIterator]();
    try {
      await watch.next(); await watch.next();
      controller.abort();
      await vi.waitFor(() => expect(f.consumer.delete).toHaveBeenCalledTimes(1));
    } finally { controller.abort(); await watch.return?.(); }
    expect(f.consumer.delete).toHaveBeenCalledTimes(1);
  });

  it("deletes when cancellation overtakes creation, without starting delivery", async () => {
    const f = fixture("cleanup_creation_abort"), controller = new AbortController();
    const info = f.consumer.info.bind(f.consumer);
    vi.spyOn(f.consumer, "info").mockImplementation(async (...args) => {
      controller.abort();
      return info(...args);
    });
    const watch = f.store().watch("machine-a.p.*", controller.signal)[Symbol.asyncIterator]();
    expect((await watch.next()).done).toBe(true);
    expect(f.getBoundPushConsumer).not.toHaveBeenCalled();
    expect(f.consumer.delete).toHaveBeenCalledTimes(1);
  });

  it.each([10014, 10059])("treats missing consumer/stream (%i) as cleanup success", async (err_code) => {
    const f = fixture(`cleanup_missing_${err_code}`);
    vi.mocked(f.consumer.delete).mockRejectedValueOnce(new JetStreamApiError({
      code: 404, err_code, description: "already removed",
    }));
    const warning = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    try { await exercise(f); expect(warning).not.toHaveBeenCalled(); }
    finally { warning.mockRestore(); }
  });

  it("bounds a stuck consumer deletion and logs once", async () => {
    vi.useFakeTimers();
    const f = fixture("cleanup_timeout"), controller = new AbortController();
    vi.mocked(f.consumer.delete).mockImplementation(() => new Promise(() => {}));
    const warning = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const watch = f.store().watch("machine-a.p.*", controller.signal)[Symbol.asyncIterator]();
    try {
      await watch.next(); await watch.next();
      const ending = watch.return?.();
      await vi.advanceTimersByTimeAsync(2_000);
      expect((await ending)?.done).toBe(true);
      expect(warning.mock.calls).toHaveLength(1);
      expect(warning.mock.calls[0]![0]).toContain('"outcome":"consumer-delete-failed"');
      expect(vi.getTimerCount()).toBe(0);
    } finally {
      controller.abort(); await watch.return?.();
      warning.mockRestore(); vi.useRealTimers();
    }
  });

  it("retains the primary setup error when deletion also fails", async () => {
    const f = fixture("cleanup_primary_error"), error = new Error("injected info failure");
    vi.spyOn(f.consumer, "info").mockRejectedValueOnce(error);
    vi.mocked(f.consumer.delete).mockRejectedValueOnce(new Error("injected delete failure"));
    const warning = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    try { await creationFailure(f, error); expect(warning).toHaveBeenCalledTimes(1); }
    finally { warning.mockRestore(); }
  });

  it("logs a delete failure once without throwing into the caller", async () => {
    const f = fixture("cleanup_delete_failure");
    vi.mocked(f.consumer.delete).mockRejectedValueOnce(new Error("injected delete failure"));
    const warning = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    try {
      await exercise(f);
      expect(f.consumer.delete).toHaveBeenCalledTimes(1);
      expect(warning.mock.calls).toHaveLength(1);
      expect(warning.mock.calls[0]![0]).toContain('"outcome":"consumer-delete-failed"');
    } finally { warning.mockRestore(); }
  });

  it("requests in-memory state without changing snapshot or live delivery", async () => {
    const f = fixture("memory_watch");
    await exercise(f);
    expect(f.consumer.consume).not.toHaveBeenCalled();
    expect(f.getBoundPushConsumer).toHaveBeenCalledWith({ deliver_subject: "watch-inbox", idle_heartbeat: 30_000_000_000 });
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
