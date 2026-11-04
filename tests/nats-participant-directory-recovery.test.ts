// Keep: Playful reviewer stayed DIRECTORY_NOT_READY for 26.8h on a verified connection.
// Inject only a watch lifecycle fault; snapshots, rows and subsequent updates cross a real broker.
// Expected-skip: no nats-server executable; NATS_AUTH_REQUIRED=1 makes this fatal.
import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { jetstream } from "@nats-io/jetstream";
import * as atomicWrite from "../src/core/atomic-write.js";
import { openNatsConnection, type NatsConnectionHandle } from "../src/transport/nats/connection.js";
import { openNatsKvForMachine } from "../src/transport/nats/kv.js";
import { KvParticipantDirectory } from "../src/topology/participant-view.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { MeshStore } from "../src/mesh/store.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";
import type { FabricTransport, TransportBucket, TransportKv, TransportKvEvent } from "../src/transport/types.js";
import { createPathBHarness, resolveNatsBinary, type PathBHarness } from "./helpers/nats-cluster.js";

const binary = resolveNatsBinary();
if (!binary && process.env.NATS_AUTH_REQUIRED === "1") throw new Error("NATS_AUTH_REQUIRED=1 but no nats-server executable was found");
const token = (id: string) => createHash("sha256").update(id).digest("hex").slice(0, 32);
const pause = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
const waitFor = async (predicate: () => boolean, timeoutMs = 4_000): Promise<void> => {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error("directory did not recover on the still-verified NATS client within 4000ms");
    await pause(10);
  }
};
const untilAbort = (signal: AbortSignal): Promise<void> => new Promise(resolve => {
  if (signal.aborted) { resolve(); return; }
  signal.addEventListener("abort", () => resolve(), { once: true });
});

let harness: PathBHarness;
const cleanup: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  try { for (const close of cleanup.splice(0).reverse()) await close(); }
  finally { vi.useRealTimers(); vi.restoreAllMocks(); }
});

describe.skipIf(!binary)("participant directory recovery without a NATS reconnect", () => {
  beforeAll(async () => {
    harness = await createPathBHarness({ binary: binary!, tls: {} });
    const bootstrap = await harness.connect(harness.users.bootstrap);
    try {
      await openNatsKvForMachine(jetstream(bootstrap), "presence", "machine-a");
      await openNatsKvForMachine(jetstream(bootstrap), "leases", "machine-a");
    } finally { await bootstrap.close(); }
  }, 30_000);
  afterAll(async () => { await harness?.stop(); });

  const fixture = async (bucket: TransportBucket, fault: string) => {
    const id = `session:connected-${bucket}-${fault}`;
    const connection: NatsConnectionHandle = await openNatsConnection({ ...harness.runtimeOptions(harness.users.machineA), machine: "machine-a" });
    cleanup.push(() => connection.close());
    let reconnects = 0;
    const unsubscribe = connection.onStatus(status => { if (status.type === "reconnect") reconnects++; });
    cleanup.push(async () => unsubscribe());
    const stores = {
      presence: await openNatsKvForMachine(connection.js, "presence", "machine-a", false, connection.onStatus),
      leases: await openNatsKvForMachine(connection.js, "leases", "machine-a", false, connection.onStatus),
    };
    const owner = `${id}#one`;
    const now = Date.now();
    const record: FabricParticipantRecord = { format: 1, id, rootId: id, kind: "root", ownerHostId: id, ownerIdentityId: id,
      ownerIncarnation: "one", name: id, runner: "pi", transport: "host", status: "idle", capabilities: ["followUp"],
      controlProtocol: "v1", startedAt: now, updatedAt: now };
    const key = `machine-a.p.${token(id)}`;
    const revision = await stores.presence.put(key, owner, record, 0);
    await stores.leases.put(`machine-a.h.${token(id)}`, owner, { id, rootId: id, identityId: id, incarnation: "one",
      pid: process.pid, pidStartTime: "12345", bootId: "connected-watch", updatedAt: now, expiresAt: now + 15_000 }, 0);
    return { id, owner, key, revision, record, connection, stores, reconnects: () => reconnects };
  };

  it.each(["presence", "leases"] as const)("recovers %s after one silent snapshot stall on a continuously connected client", async bucket => {
    await exercise(bucket, "stall");
  }, 12_000);
  it.each(["presence", "leases"] as const)("recovers %s after one watch error on a continuously connected client", async bucket => {
    await exercise(bucket, "error");
  }, 12_000);
  it.each(["presence", "leases"] as const)("recovers %s after one watch end on a continuously connected client", async bucket => {
    await exercise(bucket, "end");
  }, 12_000);
  it.each(["presence", "leases"] as const)("recovers %s after a reset stalls on a continuously connected client", async bucket => {
    await exercise(bucket, "reset");
  }, 12_000);

  it.each(["error", "stall"])("recovers composed directory startup after one %s, preserving ownership diagnostics and the startup waiter", async fault => {
    const f = await fixture("presence", `startup-${fault}`);
    const id = `${f.id}-runtime`;
    const root = path.join(harness.root, `startup-${fault}`);
    const transportStateDir = path.join(root, "local");
    fs.mkdirSync(transportStateDir, { mode: 0o700, recursive: true });
    const mesh = new MeshStore(path.join(root, "mesh"), 65_536, 1_000, { stateBackend: "file" });
    cleanup.push(async () => mesh.closeState());
    let injected = false;
    const store = f.stores.presence;
    const faulty: TransportKv = {
      ttlMs: store.ttlMs, get: store.get.bind(store), put: store.put.bind(store), delete: store.delete.bind(store),
      async *watch<V>(prefix: string, signal: AbortSignal): AsyncIterable<TransportKvEvent<V>> {
        for await (const event of store.watch<V>(prefix, signal)) {
          if (!injected && prefix === ">" && event.type === "ready") {
            injected = true;
            if (fault === "error") throw new Error("one startup watch lifecycle failure");
            await untilAbort(signal);
            return;
          }
          yield event;
        }
      },
    };
    const transport = { kind: "nats", self: { machine: "machine-a", hostId: id },
      kv: (bucket: TransportBucket) => bucket === "presence" ? faulty : f.stores[bucket] } as unknown as FabricTransport;
    const directory = new ParticipantDirectory(mesh, { enabled: true, hostId: id, rootId: id, identity: { id, name: id, kind: "main" },
      transport, transportStateDir, random: () => 0.5, reapDeadHosts: false });
    directory.registerSource(() => [{ ...f.record, id, rootId: id, ownerHostId: id, ownerIdentityId: id,
      ownerIncarnation: directory.ownerIncarnation }]);
    cleanup.push(() => directory.close());
    vi.spyOn(console, "warn").mockImplementation(() => {});
    const started = performance.now();
    const startup = directory.start();
    void startup.catch(() => {});
    await waitFor(() => injected);
    expect(() => directory.actorOwnership(id)).toThrowError(expect.objectContaining({ code: "FABRIC_DIRECTORY_NOT_READY",
      message: expect.stringMatching(/presence=.*leases=.*sinceMs=\d+/) }));
    await startup;
    expect(performance.now() - started).toBeLessThan(8_000);
    await directory.start();
    expect(directory.canConsumeMesh()).toBe(true);
    expect(directory.actorOwnership(id).size).toBe(0);
    await waitFor(() => directory.participantView?.resolve(id)?.live === true);
    expect(directory.participantView?.resolve(id)).toMatchObject({ live: true });
    expect(f.connection.onStatus.isVerified?.()).toBe(true);
    expect(f.reconnects()).toBe(0);
  }, 15_000);

  const exercise = async (bucket: TransportBucket, fault: string): Promise<void> => {
    const f = await fixture(bucket, fault);
    const store = f.stores[bucket];
    let attempts = 0, injected = false;
    const faulty: TransportKv = {
      ttlMs: store.ttlMs, get: store.get.bind(store), put: store.put.bind(store), delete: store.delete.bind(store),
      async *watch<V>(prefix: string, signal: AbortSignal): AsyncIterable<TransportKvEvent<V>> {
        const first = ++attempts === 1;
        for await (const event of store.watch<V>(prefix, signal)) {
          if (first && event.type === "ready") {
            injected = true;
            if (fault === "end") return;
            if (fault === "error") throw new Error("one watch lifecycle failure");
            if (fault === "reset") { yield event; yield { type: "reset" }; }
            // Match the incident's silent path: puts arrived, but no snapshot-ready follows.
            await untilAbort(signal);
            return;
          }
          yield event;
        }
      },
    };
    const transport = { kind: "nats", self: { machine: "machine-a", hostId: f.id },
      kv: (kind: TransportBucket) => kind === bucket ? faulty : f.stores[kind] } as unknown as FabricTransport;
    const leaseAbort = new AbortController();
    const directory = new KvParticipantDirectory({ transport, lease: { live: () => true, signal: leaseAbort.signal },
      identityId: f.id, rootId: f.id, ownerIncarnation: "one", operationTimeoutMs: 250, random: () => 0.5 });
    cleanup.push(() => directory.close());
    const diagnostic = vi.spyOn(console, "warn").mockImplementation(() => {});
    directory.start();
    await waitFor(() => injected);
    expect(directory.ready).toBe(false);
    let refusal: unknown;
    try { directory.resolve(f.id); } catch (error) { refusal = error; }
    expect(refusal).toMatchObject({ code: "FABRIC_DIRECTORY_NOT_READY" });
    const resumed = directory.whenChanged(f.id, AbortSignal.timeout(4_000));
    void resumed.catch(() => {});
    // KV requests and USER.INFO verification remain healthy during the missing snapshot.
    expect(await f.stores.presence.get(f.key)).toMatchObject({ revision: f.revision });
    expect(f.connection.onStatus.isVerified?.()).toBe(true);
    const started = performance.now();
    await waitFor(() => directory.ready);
    expect(performance.now() - started).toBeLessThan(4_000);
    await resumed;
    expect(refusal).toMatchObject({ message: expect.stringMatching(/presence=.*leases=.*sinceMs=\d+/) });
    expect(attempts).toBe(2);
    expect(f.reconnects()).toBe(0);
    expect(f.connection.onStatus.isVerified?.()).toBe(true);
    expect(directory.resolve(f.id)).toMatchObject({ machine: "machine-a", ownerHostId: f.id, live: true });
    const changed = directory.whenChanged(f.id, AbortSignal.timeout(4_000));
    await f.stores.presence.put(f.key, f.owner, { ...f.record, capabilities: ["stop"], updatedAt: Date.now() }, f.revision);
    await changed;
    expect(directory.resolve(f.id)?.capabilities).toEqual(["stop"]);
    expect(diagnostic.mock.calls.length).toBeLessThanOrEqual(1);
  };
});

// Keep: advancing the real five-minute safety bound is cheaper with a deterministic
// clock. The recovery cases above independently exercise the real broker boundary.
describe("participant snapshot safety net", () => {
  const pendingDirectory = () => {
    let attempts = 0;
    const kv = (bucket: TransportBucket): TransportKv => ({
      ttlMs: bucket === "presence" ? 600_000 : 15_000,
      get: async () => undefined, put: async () => 1, delete: async () => false,
      async *watch<V>(_prefix: string, signal: AbortSignal): AsyncIterable<TransportKvEvent<V>> {
        attempts++;
        if (bucket === "leases") yield { type: "ready" };
        await untilAbort(signal);
      },
    });
    const directory = new KvParticipantDirectory({ transport: { kind: "nats", self: { machine: "machine-a", hostId: "alarm-host" }, kv } as unknown as FabricTransport,
      lease: { live: () => true, signal: new AbortController().signal }, identityId: "alarm-host", rootId: "alarm-host",
      ownerIncarnation: "one", random: () => 0.5 });
    return { directory, attempts: () => attempts };
  };

  it("raises one safe alarm at the existing five-minute bound despite many stalled snapshot retries", async () => {
    vi.useFakeTimers();
    vi.spyOn(console, "warn").mockImplementation(() => {});
    const alarms = vi.spyOn(atomicWrite, "emitMeshJournalAlarm").mockImplementation(() => {});
    const { directory, attempts } = pendingDirectory();
    cleanup.push(() => directory.close());
    const sinceMs = Date.now();
    directory.start();
    await vi.advanceTimersByTimeAsync(atomicWrite.MESH_OUTAGE_ALARM_AFTER_MS - 1);
    expect(alarms).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1);
    expect(alarms).toHaveBeenCalledExactlyOnceWith({ label: "NATS participant directory snapshots", state: "outage", sinceMs,
      durationMs: 300_000, reason: expect.stringContaining("presence=watch-failed:SNAPSHOT_TIMEOUT; retrying, leases=ready") });
    expect(attempts()).toBeGreaterThan(2);
    await vi.advanceTimersByTimeAsync(300_000);
    expect(alarms).toHaveBeenCalledTimes(1);
    expect(directory.ready).toBe(false);
    expect(() => directory.resolve("missing")).toThrowError(expect.objectContaining({ code: "FABRIC_DIRECTORY_NOT_READY" }));
    await directory.close();
    await vi.advanceTimersByTimeAsync(300_000);
    expect(alarms).toHaveBeenCalledTimes(1);
  });

  it.each([100, 5_001])("close joins a stalled watch or pending backoff at %sms and cancels all retries/alarms", async elapsed => {
    vi.useFakeTimers();
    vi.spyOn(console, "warn").mockImplementation(() => {});
    const alarms = vi.spyOn(atomicWrite, "emitMeshJournalAlarm").mockImplementation(() => {});
    const { directory, attempts } = pendingDirectory();
    cleanup.push(() => directory.close());
    directory.start();
    await vi.advanceTimersByTimeAsync(elapsed);
    await directory.close();
    const stoppedAt = attempts();
    await vi.advanceTimersByTimeAsync(600_000);
    expect(attempts()).toBe(stoppedAt);
    expect(alarms).not.toHaveBeenCalled();
    expect(directory.ready).toBe(false);
  });
});
