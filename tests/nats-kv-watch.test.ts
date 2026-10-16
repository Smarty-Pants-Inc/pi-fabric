import {
  DeliverPolicy,
  jetstream,
  jetstreamManager,
  type ConsumerMessages,
  type JetStreamClient,
  type PushConsumer,
} from "@nats-io/jetstream";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { openNatsConnection, type NatsConnectionHandle } from "../src/transport/nats/connection.js";
import { openNatsKv, openNatsKvForMachine, type NatsKvStore } from "../src/transport/nats/kv.js";
import type { TransportKvEvent } from "../src/transport/types.js";
import { createPathBHarness, resolveNatsBinary, type PathBHarness } from "./helpers/nats-cluster.js";

// Keep: the real broker is the trust/data-safety boundary. Only this fixture's
// ephemeral store and consumers are deleted/restarted; no product server is used.
const binary = resolveNatsBinary();
if (!binary && process.env.NATS_AUTH_REQUIRED === "1") throw new Error("NATS_AUTH_REQUIRED=1 but no nats-server executable was found");
const A = "machine-a";
const B = "machine-b";
const OWNER = "runtime-watch#one";
const STREAM = "KV_fabric_presence";
const deferred = <T>() => {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => { resolve = done; });
  return { promise, resolve };
};
const bounded = async <T>(promise: Promise<T>, label: string, ms = 8_000): Promise<T> => {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([promise, new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error(`timed out: ${label}`)), ms);
    })]);
  } finally { clearTimeout(timer); }
};

type Event = TransportKvEvent<{ n: number }>;
type Watch = AsyncIterator<Event>;
const next = async (watch: Watch, ms = 8_000): Promise<Event> => {
  const result = await bounded(watch.next(), "KV watch event", ms);
  expect(result.done).toBe(false);
  return result.value!;
};
const snapshot = async (watch: Watch): Promise<Event[]> => {
  const events: Event[] = [];
  for (let count = 0; count < 100; count++) {
    const event = await next(watch);
    if (event.type === "ready") return events;
    expect(event.type).not.toBe("reset");
    events.push(event);
  }
  throw new Error("snapshot never reached ready");
};

// Scheduling seam only: creation, data, metadata, statuses and consumption still
// come from the installed official ordered push consumer and a real nats-server.
// Hold consumption after creation to deterministically exercise snapshot races.
const gateConsumer = (js: JetStreamClient) => {
  const created = deferred<PushConsumer>();
  const release = deferred<void>();
  const getPushConsumer: JetStreamClient["consumers"]["getPushConsumer"] = async (...args) => {
    const consumer = await js.consumers.getPushConsumer(...args);
    created.resolve(consumer);
    return {
      info: (cached) => consumer.info(cached),
      delete: () => consumer.delete(),
      isPullConsumer: () => consumer.isPullConsumer(),
      isPushConsumer: () => consumer.isPushConsumer(),
      consume: async (options) => {
        await release.promise;
        return consumer.consume(options);
      },
    };
  };
  const client = Object.create(js) as JetStreamClient;
  Object.defineProperty(client, "consumers", { value: { getPushConsumer } });
  return { client, created: created.promise, release: () => release.resolve() };
};

let harness: PathBHarness;
const connections: NatsConnectionHandle[] = [];
const watches: Array<{ controller: AbortController; iterator: Watch }> = [];
const connectMachine = async (machine: typeof A | typeof B): Promise<NatsConnectionHandle> => {
  const user = machine === A ? harness.users.machineA : harness.users.machineB;
  const connection = await openNatsConnection({ ...harness.runtimeOptions(user), machine });
  connections.push(connection);
  return connection;
};
const watch = (store: NatsKvStore, prefix: string) => {
  const controller = new AbortController();
  const iterator = store.watch<{ n: number }>(prefix, controller.signal)[Symbol.asyncIterator]();
  watches.push({ controller, iterator });
  return { controller, iterator };
};
const presence = async (machine: typeof A | typeof B) => {
  const connection = await connectMachine(machine);
  const store = await openNatsKvForMachine(connection.js, "presence", machine, false, connection.onStatus);
  return { connection, store };
};

describe.skipIf(!binary)("real authenticated NATS KV snapshot/ready/reset", () => {
  beforeAll(async () => {
    harness = await createPathBHarness({ binary: binary! });
    const bootstrap = await harness.connect(harness.users.bootstrap);
    await openNatsKvForMachine(jetstream(bootstrap), "presence", A);
  }, 30_000);

  afterEach(async () => {
    watches.forEach(({ controller }) => controller.abort());
    await Promise.all(watches.splice(0).map(async ({ iterator }) => {
      if (iterator.return) await bounded(iterator.return(), "watch cleanup");
    }));
    await Promise.all(connections.splice(0).map((connection) => connection.close()));
  });
  afterAll(async () => { await harness?.stop(); });

  it("makes an empty filtered snapshot ready exactly once, then follows writes", async () => {
    const { store } = await presence(A);
    const key = `${A}.p.empty`;
    const { iterator, controller } = watch(store, key);
    expect(await next(iterator)).toEqual({ type: "ready" });
    const revision = await store.put(key, OWNER, { n: 1 }, 0);
    expect(await next(iterator)).toEqual({ type: "put", entry: { key, machine: A, owner: OWNER, value: { n: 1 }, revision } });
    controller.abort();
    expect(await bounded(iterator.next(), "aborted empty watch")).toEqual({ done: true, value: undefined });
  });

  it("emits every last-per-subject snapshot entry, including the last, before ready", async () => {
    const { store } = await presence(A);
    const keys = ["snapshot-1", "snapshot-2", "snapshot-3"].map((token) => `${A}.p.${token}`);
    const first = await store.put(keys[0]!, OWNER, { n: 0 }, 0);
    const revisions = [await store.put(keys[0]!, OWNER, { n: 1 }, first),
      await store.put(keys[1]!, OWNER, { n: 2 }, 0), await store.put(keys[2]!, OWNER, { n: 3 }, 0)];
    const { iterator } = watch(store, `${A}.p.*`);
    const events = await snapshot(iterator);
    const entries = events.filter((event) => event.type === "put" && keys.includes(event.entry.key));
    expect(entries).toEqual(keys.map((key, index) => ({ type: "put", entry: {
      key, machine: A, owner: OWNER, value: { n: index + 1 }, revision: revisions[index],
    } })));
    const updated = await store.put(keys[2]!, OWNER, { n: 4 }, revisions[2]!);
    expect(await next(iterator)).toMatchObject({ type: "put", entry: { key: keys[2], revision: updated } });
  });

  it("includes a concurrent delete during snapshot and never becomes ready with a stale key", async () => {
    const { connection, store } = await presence(A);
    const key = `${A}.p.snapshot-delete`;
    const revision = await store.put(key, OWNER, { n: 1 }, 0);
    const gate = gateConsumer(connection.js);
    const gatedStore = (await openNatsKv(gate.client, { bucket: "fabric_presence" }, false, connection.onStatus)).withMachine(A);
    const { iterator } = watch(gatedStore, key);
    const first = iterator.next();
    try {
      await bounded(gate.created, "delete snapshot consumer creation");
      expect(await store.delete(key, revision)).toBe(true);
      gate.release();
      const initial = await bounded(first, "delete during snapshot");
      expect(initial.done).toBe(false);
      expect(initial.value).toMatchObject({ type: "gone", key, machine: A, reason: "deleted" });
      expect(await next(iterator)).toEqual({ type: "ready" });
      expect(await store.get(key)).toBeUndefined();
    } finally { gate.release(); }
  });

  it("recognizes the real MaxAge marker when a snapshot key expires before consumption", async () => {
    const bootstrap = await harness.connect(harness.users.bootstrap);
    const js = jetstream(bootstrap);
    const bucket = "watch_expiry";
    const stream = `KV_${bucket}`;
    const policy = { bucket, ttlMs: 1_000, markerTtlMs: 5_000 };
    const store = (await openNatsKv(js, policy)).withMachine(A);
    const key = `${A}.h.snapshot-expiry`;
    await store.put(key, OWNER, { n: 1 }, 0);
    const gate = gateConsumer(js);
    const gatedStore = (await openNatsKv(gate.client, policy, false)).withMachine(A);
    const { iterator } = watch(gatedStore, key);
    const first = iterator.next();
    let observer: ConsumerMessages | undefined;
    try {
      await bounded(gate.created, "expiry snapshot consumer creation");
      const expired = deferred<number>();
      const consumer = await js.consumers.getPushConsumer(stream, {
        filter_subjects: `$KV.${bucket}.${key}`, deliver_policy: DeliverPolicy.LastPerSubject,
      });
      observer = await consumer.consume({ callback: (msg) => {
        if (msg.headers?.get("Nats-Marker-Reason") === "MaxAge") expired.resolve(msg.info.streamSequence);
      } });
      const revision = await bounded(expired.promise, "broker-generated expiry marker");
      gate.release();
      const event = await bounded(first, "expired snapshot entry");
      expect(event.value).toEqual({ type: "gone", key, machine: A, revision, reason: "expired" });
      expect(await next(iterator)).toEqual({ type: "ready" });
      expect(await store.get(key)).toBeUndefined();
    } finally {
      gate.release();
      await observer?.close();
    }
  }, 20_000);

  it("resets after deletion of its owned ephemeral consumer, resnapshots, then resumes", async () => {
    const { connection, store } = await presence(A);
    const key = `${A}.p.consumer-reset`;
    const revision = await store.put(key, OWNER, { n: 1 }, 0);
    const gate = gateConsumer(connection.js);
    const gatedStore = (await openNatsKv(gate.client, { bucket: "fabric_presence" }, false, connection.onStatus)).withMachine(A);
    const { iterator } = watch(gatedStore, key);
    const first = iterator.next();
    const consumer = await bounded(gate.created, "owned consumer creation");
    gate.release();
    expect((await bounded(first, "owned consumer snapshot")).value).toMatchObject({ type: "put", entry: { key, revision } });
    expect(await next(iterator)).toEqual({ type: "ready" });
    const bootstrap = await harness.connect(harness.users.bootstrap);
    const manager = await jetstreamManager(bootstrap);
    const info = await consumer.info(true);
    expect(info.config.deliver_policy).toBe(DeliverPolicy.LastPerSubject);
    expect(info.config.ack_policy).toBe("none");
    expect(info.config.flow_control).toBe(true);
    await manager.consumers.delete(STREAM, info.name);
    // 2.11 does not immediately notify push subscribers of deletion. The official
    // consumer detects it after two missed 30s heartbeats; no polling or fake status.
    expect(await next(iterator, 65_000)).toEqual({ type: "reset" });
    expect(await snapshot(iterator)).toEqual([{ type: "put", entry: { key, machine: A, owner: OWNER, value: { n: 1 }, revision } }]);
    const updated = await store.put(key, OWNER, { n: 2 }, revision);
    expect(await next(iterator)).toMatchObject({ type: "put", entry: { key, revision: updated } });
  }, 80_000);

  it("invalidates readiness on a real broker restart and resumes snapshot/ready/live updates", async () => {
    const { connection, store } = await presence(A);
    const key = `${A}.p.restart`;
    const revision = await store.put(key, OWNER, { n: 1 }, 0);
    const { iterator } = watch(store, key);
    expect(await snapshot(iterator)).toEqual([{ type: "put", entry: { key, machine: A, owner: OWNER, value: { n: 1 }, revision } }]);
    const restart = harness.restart();
    try {
      expect(await next(iterator)).toEqual({ type: "reset" });
      await bounded(restart, "owned broker restart", 25_000);
      expect(await snapshot(iterator)).toEqual([{ type: "put", entry: { key, machine: A, owner: OWNER, value: { n: 1 }, revision } }]);
      const updated = await store.put(key, OWNER, { n: 2 }, revision);
      expect(await next(iterator)).toMatchObject({ type: "put", entry: { key, revision: updated } });
      expect(connection.nc.isClosed()).toBe(false);
    } finally { await restart; }
  }, 40_000);

  it("keeps broker revision order for concurrent updates from both authenticated machines", async () => {
    const { store: a } = await presence(A);
    const { store: b } = await presence(B);
    const { iterator } = watch(a, "*.h.*");
    expect(await next(iterator)).toEqual({ type: "ready" });
    const writes = Array.from({ length: 24 }, (_, index) => {
      const store = index % 2 === 0 ? a : b;
      const machine = index % 2 === 0 ? A : B;
      const key = `${machine}.h.concurrent-${index}`;
      return store.put(key, OWNER, { n: index }, 0).then((revision) => ({ key, revision, n: index }));
    });
    const written = (await Promise.all(writes)).sort((left, right) => left.revision - right.revision);
    const observed: Event[] = [];
    for (let index = 0; index < written.length; index++) observed.push(await next(iterator));
    expect(observed).toEqual(written.map(({ key, revision, n }) => ({ type: "put", entry: {
      key, machine: key.split(".")[0], owner: OWNER, value: { n }, revision,
    } })));
    const updates = await Promise.all(written.map(({ key, revision, n }) => {
      const writer = key.startsWith(`${A}.`) ? a : b;
      return writer.put(key, OWNER, { n: n + 100 }, revision).then((updated) => ({ key, revision: updated, n: n + 100 }));
    }));
    updates.sort((left, right) => left.revision - right.revision);
    const changes: Event[] = [];
    for (let index = 0; index < updates.length; index++) changes.push(await next(iterator));
    expect(changes).toEqual(updates.map(({ key, revision, n }) => ({ type: "put", entry: {
      key, machine: key.split(".")[0], owner: OWNER, value: { n }, revision,
    } })));
  });

  it.each([{ name: "shape", wire: "{}" }, { name: "syntax", wire: "not-json" }])("fails on malformed broker data instead of hiding it behind reset/retry: $name", async ({ name, wire }) => {
    const { store } = await presence(A);
    const key = `${A}.p.malformed-${name}`;
    const { iterator } = watch(store, key);
    expect(await next(iterator)).toEqual({ type: "ready" });
    const bootstrap = await harness.connect(harness.users.bootstrap);
    await jetstream(bootstrap).publish(`$KV.fabric_presence.${key}`, wire);
    await expect(bounded(iterator.next(), "malformed entry failure")).rejects.toMatchObject({ code: "FABRIC_TRANSPORT_INVALID" });
  });

  it("refuses non-serializable values before storing an unreadable record", async () => {
    const { store } = await presence(A);
    const key = `${A}.p.invalid-value`;
    const cycle: Record<string, unknown> = {};
    cycle.self = cycle;
    for (const value of [undefined, () => undefined, cycle]) {
      await expect(store.put(key, OWNER, value, 0)).rejects.toMatchObject({ code: "FABRIC_TRANSPORT_INVALID", persisted: false });
      expect(await store.get(key)).toBeUndefined();
    }
    const revision = await store.put(key, OWNER, null, 0);
    expect(await store.get(key)).toMatchObject({ revision, value: null });
  });

  it("enforces own-machine writes and real create/update/delete CAS conflicts", async () => {
    const { store: a, connection } = await presence(A);
    const { store: b } = await presence(B);
    const config = (await connection.manager.streams.info(STREAM)).config;
    expect(config).toMatchObject({ allow_direct: false, max_msgs_per_subject: 1, storage: "file", max_age: 600_000_000_000 });
    const own = `${A}.p.cas`;
    const foreign = `${B}.p.cas`;
    const first = await a.put(own, OWNER, { n: 1 }, 0);
    await expect(a.put(own, OWNER, { n: 2 }, 0)).rejects.toMatchObject({ code: "FABRIC_KV_CONFLICT", key: own, expected: 0 });
    const updated = await a.put(own, OWNER, { n: 2 }, first);
    await expect(a.put(own, OWNER, { n: 3 }, first)).rejects.toMatchObject({ code: "FABRIC_KV_CONFLICT", expected: first });
    expect(await a.delete(own, first)).toBe(false);
    expect(await a.get(own)).toMatchObject({ revision: updated, value: { n: 2 } });
    expect(await a.delete(own, updated)).toBe(true);
    expect(await a.get(own)).toBeUndefined();
    expect(await a.put(own, OWNER, { n: 3 }, 0)).toBeGreaterThan(updated);
    const foreignRevision = await b.put(foreign, OWNER, { n: 4 }, 0);
    expect(await a.get(foreign)).toMatchObject({ machine: B, revision: foreignRevision });
    await expect(a.put(foreign, OWNER, { n: 5 }, foreignRevision)).rejects.toMatchObject({ code: "FABRIC_TRANSPORT_DENIED" });
    await expect(a.delete(foreign, foreignRevision)).rejects.toMatchObject({ code: "FABRIC_TRANSPORT_DENIED" });
    expect(await b.get(foreign)).toMatchObject({ revision: foreignRevision, value: { n: 4 } });
  });
});
