import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  DeliverPolicy,
  jetstream,
  jetstreamManager,
  type ConsumerMessages,
  type JetStreamClient,
  type PushConsumer,
} from "@nats-io/jetstream";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { openNatsConnection, type NatsConnectionHandle } from "../src/transport/nats/connection.js";
import { openNatsTransport } from "../src/transport/nats/inbox.js";
import { MeshStore } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import * as participantFiles from "../src/topology/participant-files.js";
import { readParticipantFile, readParticipantFiles } from "../src/topology/participant-files.js";
import { NatsLocalProjection } from "../src/topology/nats-local-projection.js";
import { KvParticipantDirectory } from "../src/topology/participant-view.js";
import type { FabricKvHostLease, FabricParticipantRecord } from "../src/topology/types.js";
import { openNatsKv, openNatsKvForMachine, type NatsKvStore } from "../src/transport/nats/kv.js";
import { TransportUnavailableError, type FabricTransport, type TransportBucket, type TransportKv, type TransportKvEvent } from "../src/transport/types.js";
import { ensureTransportStream } from "../src/transport/nats/provision.js";
import { transportInboxStream, transportInboxStreamSubject } from "../src/transport/nats/subjects.js";
import { createPathBHarness, resolveNatsBinary, type PathBHarness } from "./helpers/nats-cluster.js";

// Keep: the real broker is the trust/data-safety boundary. Only this fixture's
// ephemeral store and consumers are deleted/restarted; no product server is used.
const binary = resolveNatsBinary();
if (!binary && process.env.NATS_AUTH_REQUIRED === "1") throw new Error("NATS_AUTH_REQUIRED=1 but no nats-server executable was found");
const A = "machine-a";
const B = "machine-b";
const OWNER = "runtime-watch#one";
const STREAM = "KV_fabric_presence";
const wait = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
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
const recoveryDirectories: ParticipantDirectory[] = [];
const recoveryTransports: Array<{ close(): Promise<void> }> = [];
const recoveryMeshes: MeshStore[] = [];
const recoveryRoots: string[] = [];
const connectMachine = async (machine: typeof A | typeof B): Promise<NatsConnectionHandle> => {
  const user = machine === A ? harness.users.machineA : harness.users.machineB;
  const connection = await openNatsConnection({ ...harness.runtimeOptions(user), machine });
  connections.push(connection);
  return connection;
};
const watch = (store: Pick<NatsKvStore, "watch">, prefix: string) => {
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
    harness = await createPathBHarness({ binary: binary!, tls: {} });
    const bootstrap = await harness.connect(harness.users.bootstrap);
    const manager = await jetstreamManager(bootstrap);
    const js = jetstream(bootstrap);
    await openNatsKvForMachine(js, "leases", A);
    await openNatsKvForMachine(js, "presence", A);
    await ensureTransportStream(manager, { name: transportInboxStream(A), subjects: [transportInboxStreamSubject(A)],
      maxBytes: 1_073_741_824, maxAgeMs: 0, maxMessageBytes: 1_048_576, duplicateWindowMs: 600_000, replicas: 1 });
    await ensureTransportStream(manager, { name: transportInboxStream(B), subjects: [transportInboxStreamSubject(B)],
      maxBytes: 1_073_741_824, maxAgeMs: 0, maxMessageBytes: 1_048_576, duplicateWindowMs: 600_000, replicas: 1 });
  }, 30_000);

  afterEach(async () => {
    await Promise.allSettled(recoveryDirectories.splice(0).map(directory => directory.close()));
    await Promise.allSettled(recoveryTransports.splice(0).map(transport => transport.close()));
    for (const mesh of recoveryMeshes.splice(0)) mesh.closeState();
    for (const root of recoveryRoots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
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
    const transport = await openNatsTransport({ ...harness.runtimeOptions(harness.users.machineA), hostId: "watch-restart" });
    const store = transport.kv("presence");
    const key = `${A}.p.restart`;
    try {
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
      } finally { await restart; }
    } finally { await transport.close(); }
  }, 40_000);

  it("does not resurrect a participant deleted while lease admission is suspended", async () => {
    const id = "session:projection-outage-delete", incarnation = "projection-outage-delete-1";
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "nats-kv-projection-replay-"));
    fs.chmodSync(root, 0o700);
    const mesh = new MeshStore(path.join(root, "mesh"), 65_536, 1_000);
    const transport = await openNatsTransport({ ...harness.runtimeOptions(harness.users.machineA), hostId: id });
    const presence = transport.kv("presence");
    const key = `${transport.self.machine}.p.${createHash("sha256").update(id).digest("hex").slice(0, 32)}`;
    const record: FabricParticipantRecord = {
      format: 1, id, rootId: id, kind: "root", ownerHostId: id, ownerIdentityId: id,
      ownerIncarnation: incarnation, sessionId: id.slice("session:".length), name: "Projection outage delete",
      status: "idle", runner: "pi", transport: "host", capabilities: ["followUp"],
      controlProtocol: "v1", startedAt: Date.now(), updatedAt: Date.now(),
    };
    const owner = `${id}#${incarnation}`;
    let leaseLive = false;
    let holdTombstone = false;
    const tombstoneHeld = deferred<void>(), releaseTombstone = deferred<void>(), tombstoneConsumed = deferred<void>();
    const gatedPresence: TransportKv = {
      ttlMs: presence.ttlMs,
      get: (...args) => presence.get(...args),
      put: (...args) => presence.put(...args),
      delete: (...args) => presence.delete(...args),
      async *watch<V>(prefix: string, signal: AbortSignal): AsyncIterable<TransportKvEvent<V>> {
        for await (const event of presence.watch<V>(prefix, signal)) {
          if (holdTombstone && event.type === "gone" && event.key === key) {
            tombstoneHeld.resolve();
            await releaseTombstone.promise;
          }
          yield event;
          if (holdTombstone && event.type === "gone" && event.key === key) tombstoneConsumed.resolve();
        }
      },
    };
    const projectionTransport = {
      kind: "nats" as const,
      self: transport.self,
      kv(bucket: TransportBucket) { return bucket === "presence" ? gatedPresence : transport.kv(bucket); },
    } as unknown as FabricTransport;
    const projection = new NatsLocalProjection({ mesh, transport: projectionTransport, leaseLive: () => leaseLive });
    const writer = vi.spyOn(participantFiles, "writeParticipantFileIf");
    const observerAbort = new AbortController();
    const observer = presence.watch<unknown>(key, observerAbort.signal)[Symbol.asyncIterator]();
    try {
      const revision = await presence.put(key, owner, record, 0);
      projection.start();
      await bounded(projection.whenReady(), "initial suspended projection snapshot");
      expect(readParticipantFile(mesh.root, `topology/participants/${createHash("sha256").update(id).digest("hex")}`)).toBeUndefined();
      expect((await bounded(observer.next(), "participant watch snapshot")).value)
        .toMatchObject({ type: "put", entry: { key, revision } });
      expect((await bounded(observer.next(), "participant watch ready")).value).toEqual({ type: "ready" });

      holdTombstone = true;
      expect(await presence.delete(key, revision)).toBe(true);
      expect((await bounded(observer.next(), "participant deletion event")).value).toMatchObject({ type: "gone", key });
      await bounded(tombstoneHeld.promise, "projection watch pauses before its tombstone");
      leaseLive = true;
      await bounded(projection.resumeAfterLeaseRecovery(), "projection replay after lease recovery");
      expect(writer).not.toHaveBeenCalled();
      expect(readParticipantFile(mesh.root, `topology/participants/${createHash("sha256").update(id).digest("hex")}`)).toBeUndefined();

      releaseTombstone.resolve();
      await bounded(tombstoneConsumed.promise, "projection consumes the tombstone");
      expect(writer).not.toHaveBeenCalled();
      expect(readParticipantFile(mesh.root, `topology/participants/${createHash("sha256").update(id).digest("hex")}`)).toBeUndefined();
    } finally {
      releaseTombstone.resolve();
      observerAbort.abort();
      await observer.return?.();
      await projection.close();
      await transport.close();
      mesh.closeState();
      fs.rmSync(root, { recursive: true, force: true });
      vi.restoreAllMocks();
    }
  }, 30_000);

  it("recovers both participant directories, KV watches and routed sends after a real same-store server restart", async () => {
    const mainId = "session:directory-restart-main", callerId = "session:directory-restart-caller";
    const mainIncarnation = "restart-main-1", callerIncarnation = "restart-caller-1";
    const mainTransport = await openNatsTransport({ ...harness.runtimeOptions(harness.users.machineA), hostId: mainId });
    const callerTransport = await openNatsTransport({ ...harness.runtimeOptions(harness.users.machineB), hostId: callerId });
    const mainLeaseAbort = new AbortController(), callerLeaseAbort = new AbortController();
    const mainGate = { live: () => true, signal: mainLeaseAbort.signal };
    const callerGate = { live: () => true, signal: callerLeaseAbort.signal };
    const mainDirectory = new KvParticipantDirectory({ transport: mainTransport, lease: mainGate,
      identityId: mainId, rootId: mainId, ownerIncarnation: mainIncarnation });
    const callerDirectory = new KvParticipantDirectory({ transport: callerTransport, lease: callerGate,
      identityId: callerId, rootId: callerId, ownerIncarnation: callerIncarnation });
    const inboxAbort = new AbortController();
    let inbox: AsyncIterator<import("../src/transport/types.js").TransportDelivery<import("../src/transport/types.js").TransportEnvelope>> | undefined;
    const token = (id: string) => createHash("sha256").update(id).digest("hex").slice(0, 32);
    const now = Date.now();
    const record = (id: string, incarnation: string): FabricParticipantRecord => ({ format: 1, id, rootId: id,
      kind: "root", ownerHostId: id, ownerIdentityId: id, ownerIncarnation: incarnation,
      sessionId: id.slice("session:".length), name: id, status: "idle", runner: "pi", transport: "host",
      capabilities: ["followUp"], controlProtocol: "v1", startedAt: now, updatedAt: now });
    const lease = (id: string, incarnation: string): FabricKvHostLease => ({ id, identityId: id, rootId: id,
      incarnation, pid: process.pid, pidStartTime: "12345", bootId: "directory-restart", updatedAt: now, expiresAt: now + 15_000 });
    const publishParticipant = async (transport: typeof mainTransport, machine: string, id: string, incarnation: string) => {
      const owner = `${id}#${incarnation}`;
      await transport.kv("leases").put(`${machine}.h.${token(id)}`, owner, lease(id, incarnation), 0);
      await transport.kv("presence").put(`${machine}.p.${token(id)}`, owner, record(id, incarnation), 0);
    };
    const waitFor = async (predicate: () => boolean, label: string, ms = 8_000): Promise<void> => {
      const deadline = Date.now() + ms;
      while (Date.now() < deadline) { if (predicate()) return; await wait(20); }
      throw new Error(`timed out: ${label}`);
    };
    const makeEnvelope = (id: string) => ({ id, kind: "command" as const, to: { machine: A, hostId: mainId },
      targetId: mainId, operation: "followUp", from: { id: callerId, name: "restart-caller", kind: "main" as const }, body: { id } });
    const sendToMain = async (id: string) => {
      const route = callerDirectory.resolve(mainId);
      expect(route).toMatchObject({ machine: A, ownerHostId: mainId, live: true });
      return callerTransport.send({ ...makeEnvelope(id), to: { machine: route!.machine, hostId: route!.ownerHostId } });
    };
    try {
      mainDirectory.start(); callerDirectory.start();
      await waitFor(() => mainDirectory.ready && callerDirectory.ready, "initial participant directory snapshots");
      await publishParticipant(mainTransport, A, mainId, mainIncarnation);
      await publishParticipant(callerTransport, B, callerId, callerIncarnation);
      await waitFor(() => mainDirectory.resolve(callerId)?.live === true && callerDirectory.resolve(mainId)?.live === true,
        "both participants become routable");
      inbox = mainTransport.inbox(inboxAbort.signal)[Symbol.asyncIterator]();
      const first = await sendToMain("directory-before-restart");
      expect(first.duplicate).toBe(false);
      const firstDelivery = (await bounded(inbox.next(), "pre-restart routed delivery")).value!;
      expect(firstDelivery.payload.id).toBe("directory-before-restart");
      await firstDelivery.ack();

      const started = performance.now();
      const restart = harness.restart();
      try {
        await bounded(waitFor(() => !mainDirectory.ready && !callerDirectory.ready, "both directories invalidate on disconnect", 8_000),
          "directory invalidation", 9_000);
        await expect(callerTransport.send(makeEnvelope("directory-during-restart")))
          .rejects.toBeInstanceOf(TransportUnavailableError);
        await bounded(restart, "owned nats-server restart with retained JetStream store", 25_000);
        await bounded(waitFor(() => mainDirectory.ready && callerDirectory.ready, "both participant directories resnapshot", 10_000),
          "directory recovery after broker restart", 10_500);
        expect(performance.now() - started).toBeLessThan(10_000);
        await waitFor(() => callerDirectory.resolve(mainId)?.live === true, "recovered route is live", 2_000);
        const retry = await sendToMain("directory-after-restart");
        const duplicate = await sendToMain("directory-after-restart");
        expect(retry.duplicate).toBe(false);
        expect(duplicate.duplicate).toBe(true);
        const secondDelivery = (await bounded(inbox.next(), "post-restart routed delivery")).value!;
        expect(secondDelivery.payload.id).toBe("directory-after-restart");
        await secondDelivery.ack();
        await expect(bounded(inbox.next(), "duplicate delivery counterexample", 250)).rejects.toThrow("timed out");
        const inspector = await harness.connect(harness.users.bootstrap);
        try {
          const manager = await jetstreamManager(inspector);
          expect((await manager.streams.info(transportInboxStream(A))).state.messages).toBe(2);
        } finally { await inspector.close(); }
      } finally { await restart; }
    } finally {
      inboxAbort.abort(); mainLeaseAbort.abort(); callerLeaseAbort.abort();
      await inbox?.return?.();
      await Promise.all([mainDirectory.close(), callerDirectory.close(), mainTransport.close(), callerTransport.close()]);
    }
  }, 50_000);

  it("reacquires 20+ participant leases after an outage beyond TTL and closes lost lineage idempotently", async () => {
    const participantCount = 21;
    const directories: ParticipantDirectory[] = [];
    for (let index = 0; index < participantCount; index++) {
      const id = `session:lease-recovery-${String(index).padStart(2, "0")}`;
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-nats-lease-recovery-"));
      fs.chmodSync(root, 0o700);
      recoveryRoots.push(root);
      const mesh = new MeshStore(path.join(root, "mesh"), 65_536, 1_000);
      recoveryMeshes.push(mesh);
      const transportStateDir = path.join(root, "transport-state");
      fs.mkdirSync(transportStateDir, { mode: 0o700 });
      const transport = await openNatsTransport({ ...harness.runtimeOptions(harness.users.machineA), hostId: id });
      recoveryTransports.push(transport);
      const directory = new ParticipantDirectory(mesh, {
        enabled: true, hostId: id, rootId: id, identity: { id, name: id, kind: "main" },
        transport, transportStateDir,
      });
      directory.registerSource((): FabricParticipantRecord[] => {
        const now = Date.now();
        return [{ format: 1, id, rootId: id, kind: "root", ownerHostId: id, ownerIdentityId: id,
          ownerIncarnation: directory.ownerIncarnation, name: id, status: "idle", runner: "pi", transport: "host",
          capabilities: ["followUp"], controlProtocol: "v1", startedAt: now, updatedAt: now }];
      });
      directories.push(directory);
      recoveryDirectories.push(directory);
    }
    await Promise.all(directories.map(directory => directory.start()));
    const waitFor = async (predicate: () => boolean, label: string, ms: number): Promise<void> => {
      const deadline = Date.now() + ms;
      while (Date.now() < deadline) { if (predicate()) return; await wait(25); }
      throw new Error(`timed out: ${label}`);
    };
    const allVisible = (active: readonly ParticipantDirectory[]): boolean => active.every(directory => {
      if (!directory.canConsumeMesh()) return false;
      const peers = directory.list({ scope: "project" });
      return peers.length === active.length && peers.every(peer => !peer.stale && directory.participantView?.resolve(peer.id)?.live === true);
    });
    await waitFor(() => allVisible(directories), "initial 21-participant directory", 30_000);

    const server = harness.serverProcess;
    if (!server) throw new Error("Path B NATS server process is unavailable");
    if (server.exitCode === null && server.signalCode === null) {
      const stopped = new Promise<void>(resolve => server.once("exit", () => resolve()));
      server.kill("SIGTERM");
      await bounded(stopped, "NATS outage begins", 5_000);
    }
    const outageStarted = performance.now();
    await wait(17_000);
    expect(performance.now() - outageStarted).toBeGreaterThanOrEqual(15_000);
    expect(directories.every(directory => !directory.canConsumeMesh())).toBe(true);

    await expect(directories[0]!.closeLineage()).resolves.toBeUndefined();
    await expect(directories[0]!.closeLineage()).resolves.toBeUndefined();
    await expect(directories[0]!.close()).resolves.toBeUndefined();

    await bounded(harness.start(), "NATS restart after expired lease TTL", 25_000);
    const survivors = directories.slice(1);
    await waitFor(() => allVisible(survivors), "20 participant lease reacquisition and replay", 35_000);
    for (const directory of survivors) {
      expect(directory.list({ scope: "project" })).toHaveLength(survivors.length);
    }
    const peerId = survivors[0]!.options.hostId;
    const projectedKey = `topology/participants/${createHash("sha256").update(peerId).digest("hex")}`;
    const projected = readParticipantFiles(survivors[1]!.mesh.root, { maxAgeMs: 0 }).find(entry => entry.key === projectedKey);
    expect(projected?.value).toMatchObject({ id: peerId });
  }, 120_000);

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
    let revision = 0;
    try {
      revision = (await jetstream(bootstrap).publish(`$KV.fabric_presence.${key}`, wire)).seq;
      await expect(bounded(iterator.next(), "malformed entry failure")).rejects.toMatchObject({ code: "FABRIC_TRANSPORT_INVALID" });
    } finally {
      try { if (revision) await store.delete(key, revision); }
      finally { await bootstrap.close(); }
    }
  });

  it("restarts a failed participant watch after verified reconnect and clears failure after replay", async () => {
    const id = "session:watch-failed-recovery";
    const transport = await openNatsTransport({ ...harness.runtimeOptions(harness.users.machineA), hostId: id });
    const leaseAbort = new AbortController();
    const directory = new KvParticipantDirectory({ transport, lease: { live: () => true, signal: leaseAbort.signal },
      identityId: id, rootId: id, ownerIncarnation: "watch-failed-recovery" });
    const waitFor = async (predicate: () => boolean, label: string, ms = 8_000): Promise<void> => {
      const deadline = Date.now() + ms;
      while (Date.now() < deadline) { if (predicate()) return; await wait(20); }
      throw new Error(`timed out: ${label}`);
    };
    try {
      directory.start();
      await waitFor(() => directory.ready, "initial empty snapshots");
      const token = createHash("sha256").update("malformed-watch-recovery").digest("hex").slice(0, 32);
      const key = `${A}.p.${token}`;
      const admin = await harness.connect(harness.users.bootstrap);
      let revision: number;
      try { revision = (await jetstream(admin).publish(`$KV.fabric_presence.${key}`, "{}")).seq; }
      finally { await admin.close(); }
      await waitFor(() => !directory.ready, "malformed row stops participant readiness");
      expect(directory.ready).toBe(false);
      expect(await transport.kv("presence").delete(key, revision)).toBe(true);
      await bounded(harness.restart(), "verified NATS reconnect after malformed row", 25_000);
      await waitFor(() => directory.ready, "watch snapshot replay clears the prior failure", 15_000);
      expect(directory.ready).toBe(true);
    } finally {
      leaseAbort.abort();
      await directory.close();
      await transport.close();
    }
  }, 50_000);

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
