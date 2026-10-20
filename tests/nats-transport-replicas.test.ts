// Keep: data safety. Never silently open an R1 resource under an R3 policy (or rewrite existing resources).
// The real flow owns an anonymous loopback cluster. Only the credential/USER.INFO connection seam is replaced;
// KV, provisioning, transport send/inbox, JetStream storage, delivery and confirmed ACKs remain real.
import fs from "node:fs";
import path from "node:path";
import { connect } from "@nats-io/transport-node";
import {
  JetStreamApiError, jetstream, jetstreamManager,
  type ConsumerConfig, type ConsumerInfo, type JetStreamClient, type JetStreamManager, type StreamConfig, type StreamInfo,
} from "@nats-io/jetstream";
import { Kvm, type KV, type KvOptions } from "@nats-io/kv";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { NatsConnectionHandle } from "../src/transport/nats/connection.js";
import { openNatsTransport, type NatsTransportOptions } from "../src/transport/nats/inbox.js";
import { runTransportAdmin } from "../src/transport/nats/admin.js";
import { openNatsKv, KV_MARKER_TTL_MS, KV_MAX_VALUE_SIZE, LEASE_TTL_MS, PRESENCE_TTL_MS } from "../src/transport/nats/kv.js";
import { transportStreamConfig } from "../src/transport/nats/provision.js";
import { ADVISORY_CAPTURE_STREAM, transportAdvisoryStreamSubject, transportDurableName, transportInboxStream, transportInboxStreamSubject } from "../src/transport/nats/subjects.js";
import { TransportInvalidError, type FabricTransport, type FabricTransportOptions } from "../src/transport/types.js";
import { loadAgentNatsReplicas, loadGlobalFabricConfig } from "../src/config.js";
import { startNatsElectionCluster, type NatsElectionCluster } from "./helpers/nats-election-cluster.js";
import { waitForNatsStreamReady } from "./helpers/nats-consumer-ready.js";

const connections = vi.hoisted(() => ({ runtime: vi.fn(), bootstrap: vi.fn() }));
vi.mock("../src/transport/nats/connection.js", async importOriginal => ({
  ...(await importOriginal<typeof import("../src/transport/nats/connection.js")>()),
  openNatsConnection: connections.runtime,
  openBootstrapNatsConnection: connections.bootstrap,
}));
vi.mock("../src/config.js", async importOriginal => {
  const actual = await importOriginal<typeof import("../src/config.js")>();
  return {
    ...actual,
    loadAgentNatsReplicas: vi.fn(actual.loadAgentNatsReplicas),
    loadGlobalFabricConfig: vi.fn(() => { throw new Error("replica provisioning must not load global/project config"); }),
  };
});

const MACHINE = "replica-test";
const HOST = "replica-host";
const INBOX = transportInboxStream(MACHINE);
const base: NatsTransportOptions = {
  servers: "nats://127.0.0.1:1", machine: MACHINE, hostId: HOST, credentials: "fixture-only",
  custodyStream: "CUSTODY", livenessStream: "LIVENESS", pathBMachines: [MACHINE],
};
const inboxPolicy = {
  name: INBOX, subjects: [transportInboxStreamSubject(MACHINE)], maxBytes: 1_073_741_824,
  maxAgeMs: 7 * 24 * 60 * 60_000, maxMessageBytes: 1_048_576, duplicateWindowMs: 600_000,
};
const advisoryPolicy = {
  name: ADVISORY_CAPTURE_STREAM,
  subjects: [transportAdvisoryStreamSubject],
  maxBytes: 1_073_741_824,
  maxAgeMs: 30 * 24 * 60 * 60_000,
  maxMessageBytes: 64 * 1024,
  duplicateWindowMs: 600_000,
};
const bucketPolicies = [
  { bucket: "fabric_leases", ttlMs: LEASE_TTL_MS },
  { bucket: "fabric_presence", ttlMs: PRESENCE_TTL_MS },
] as const;
const transports: FabricTransport[] = [];
let scratch: string | undefined;
let cluster: NatsElectionCluster | undefined;
const rawConnections: Awaited<ReturnType<typeof connect>>[] = [];

async function bounded<T>(promise: Promise<T>, label: string, ms = 10_000): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([promise, new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error(`${label} exceeded ${ms}ms`)), ms);
    })]);
  } finally { clearTimeout(timer); }
}

function privateScratch(): string {
  const parent = process.env.TMPDIR;
  if (!parent || !path.isAbsolute(parent)) throw new Error("TMPDIR must be absolute private test scratch");
  scratch = fs.mkdtempSync(path.join(parent, "nats-transport-replicas-"));
  fs.chmodSync(scratch, 0o700);
  return scratch;
}

beforeEach(() => {
  connections.runtime.mockReset();
  connections.bootstrap.mockReset();
  vi.mocked(loadAgentNatsReplicas).mockClear();
  vi.mocked(loadGlobalFabricConfig).mockClear();
});
afterEach(async () => {
  // All cleanup targets were allocated by this test; stop native writers before removing scratch.
  try { await Promise.all(transports.splice(0).map(transport => bounded(transport.close(), "transport close"))); }
  finally {
    try { await Promise.all(rawConnections.splice(0).map(nc => nc.close())); }
    finally {
      try {
        if (cluster) {
          const owned = cluster;
          cluster = undefined;
          await owned.cleanup();
          expect(owned.cleanupReceipt()).toMatchObject({ noLiveChildren: true, scratchRemoved: true });
        }
      } finally {
        vi.restoreAllMocks();
        vi.unstubAllEnvs();
        if (scratch) { const owned = scratch; scratch = undefined; fs.rmSync(owned, { recursive: true, force: true }); }
      }
    }
  }
}, 30_000);

function fakeBroker() {
  const streams = new Map<string, StreamInfo>();
  const consumers = new Map<string, ConsumerInfo>();
  const missing = (err_code: number) => new JetStreamApiError({ code: 404, err_code, description: "fixture resource missing" });
  const streamApi = {
    info: vi.fn(async (name: string) => { const info = streams.get(name); if (!info) throw missing(10059); return structuredClone(info); }),
    add: vi.fn(async (config: StreamConfig) => {
      const info = { config: structuredClone(config) } as StreamInfo;
      streams.set(config.name, info);
      return structuredClone(info);
    }),
    update: vi.fn(async () => { throw new Error("must not update an existing stream"); }),
  };
  const consumerApi = {
    info: vi.fn(async (stream: string, name: string) => {
      const info = consumers.get(`${stream}/${name}`); if (!info) throw missing(10014); return structuredClone(info);
    }),
    add: vi.fn(async (stream: string, config: ConsumerConfig) => {
      const info = { config: structuredClone(config), name: config.durable_name, stream_name: stream } as ConsumerInfo;
      consumers.set(`${stream}/${config.durable_name}`, info);
      return structuredClone(info);
    }),
    update: vi.fn(async () => { throw new Error("must not update an existing consumer"); }),
  };
  const nc = { isClosed: () => false, isDraining: () => false };
  const js = { nc } as unknown as JetStreamClient;
  const handle = {
    nc, js, manager: { streams: streamApi, consumers: consumerApi },
    isVerified: () => true, onStatus: () => () => undefined,
    closed: () => new Promise<void>(() => undefined), close: vi.fn(async () => undefined),
  } as unknown as NatsConnectionHandle;
  connections.runtime.mockResolvedValue(handle);
  connections.bootstrap.mockResolvedValue(handle);
  return { streams, streamApi, consumerApi, js, handle };
}

function fakeKv() {
  let observed = 1;
  const counts = new Map<string, number>();
  const status = vi.fn(async (name: string) => ({
    ttl: name === "fabric_presence" ? PRESENCE_TTL_MS : LEASE_TTL_MS,
    markerTTL: KV_MARKER_TTL_MS, history: 1, storage: "file", replicas: counts.get(name) ?? observed,
    maxValueSize: KV_MAX_VALUE_SIZE, streamInfo: { config: { allow_direct: false } },
  }));
  const update = vi.fn();
  const buckets = new Map<string, KV>();
  const get = async (name: string) => {
    let bucket = buckets.get(name);
    if (!bucket) {
      bucket = { status: () => status(name), update } as unknown as KV;
      buckets.set(name, bucket);
    }
    return bucket;
  };
  const create = vi.spyOn(Kvm.prototype, "create").mockImplementation(get);
  const open = vi.spyOn(Kvm.prototype, "open").mockImplementation(get);
  return { create, open, status, update, setObserved: (replicas: number, bucket?: string) => {
    if (bucket) counts.set(bucket, replicas); else observed = replicas;
  } };
}

function factoryOptions(replicas?: number): FabricTransportOptions {
  return {
    hostId: HOST, mesh: {} as FabricTransportOptions["mesh"],
    config: { transport: "nats", nats: { ...base, ...(replicas === undefined ? {} : { replicas }) } } as unknown as FabricTransportOptions["config"],
  };
}

function assertKvOptions(options: Partial<KvOptions> | undefined, replicas: number, ttl: number) {
  expect(options).toMatchObject({ replicas, ttl, markerTTL: 300_000, history: 1, storage: "file", maxValueSize: 65_536, allow_direct: false });
}

describe("transport replica policy (unit data-safety boundaries)", () => {
  it.each(bucketPolicies.flatMap(policy => [true, false].flatMap(provision => [undefined, 3].map(replicas => ({ ...policy, provision, replicas })))))
    ("$bucket provision=$provision replicas=$replicas creates/opens and verifies the requested count", async ({ bucket, ttlMs, provision, replicas }) => {
      const broker = fakeBroker();
      const kv = fakeKv();
      kv.setObserved(replicas ?? 1);
      const store = await openNatsKv(broker.js, { bucket, ...(replicas === undefined ? {} : { replicas }) }, provision);
      expect(store.ttlMs).toBe(ttlMs);
      const used = provision ? kv.create : kv.open;
      expect(used).toHaveBeenCalledTimes(1);
      expect(used.mock.calls[0]![0]).toBe(bucket);
      assertKvOptions(used.mock.calls[0]![1], replicas ?? 1, ttlMs);
      expect(provision ? kv.open : kv.create).not.toHaveBeenCalled();
      expect(kv.status).toHaveBeenCalledWith(bucket);
      expect(kv.update).not.toHaveBeenCalled();
    });

  it.each(bucketPolicies.flatMap(policy => [true, false].flatMap(provision => [1, 3].map(replicas => ({ ...policy, provision, replicas })))))
    ("$bucket provision=$provision refuses an existing count different from $replicas without update", async ({ bucket, provision, replicas }) => {
      const broker = fakeBroker();
      const kv = fakeKv();
      kv.setObserved(replicas === 1 ? 3 : 1);
      await expect(openNatsKv(broker.js, { bucket, replicas }, provision)).rejects.toBeInstanceOf(TransportInvalidError);
      expect(kv.status).toHaveBeenCalledWith(bucket);
      expect(kv.update).not.toHaveBeenCalled();
      expect(broker.streamApi.update).not.toHaveBeenCalled();
      expect((await kv.status(bucket)).replicas).toBe(replicas === 1 ? 3 : 1);
    });

  it.each(["direct", "factory"] as const)("%s options preserve R1 defaults and carry configured R3 through KV and the inbox", async style => {
    // Separate brokers per count keep omitted-policy coverage independent of configured-policy state.
    for (const replicas of [undefined, 3]) {
      const broker = fakeBroker();
      const kv = fakeKv();
      const wanted = replicas ?? 1;
      kv.setObserved(wanted);
      broker.streams.set(INBOX, { config: transportStreamConfig({ ...inboxPolicy, replicas: wanted }) } as StreamInfo);
      const transport = style === "direct"
        ? await openNatsTransport({ ...base, provision: true, ...(replicas === undefined ? {} : { replicas }) })
        : await openNatsTransport(factoryOptions(replicas));
      transports.push(transport);
      const used = style === "direct" ? kv.create : kv.open;
      expect(used.mock.calls.map(([bucket, options]) => [bucket, options?.replicas])).toEqual([
        ["fabric_leases", wanted], ["fabric_presence", wanted],
      ]);
      expect(kv.status.mock.calls.map(([bucket]) => bucket)).toEqual(["fabric_leases", "fabric_presence"]);
      expect((await broker.streamApi.info(INBOX)).config.num_replicas).toBe(wanted);
      expect(broker.consumerApi.add).toHaveBeenCalledTimes(1);
      if (style === "direct") expect(broker.streams.get("FABRIC_ADV")?.config.num_replicas).toBe(1);
      expect(broker.streamApi.update).not.toHaveBeenCalled();
      await transport.close();
      vi.restoreAllMocks();
    }
  });

  it.each(bucketPolicies)("runtime refuses a mismatched $bucket status before opening an inbox", async ({ bucket }) => {
    const broker = fakeBroker();
    const kv = fakeKv();
    kv.setObserved(3);
    kv.setObserved(1, bucket);
    await expect(openNatsTransport(factoryOptions(3))).rejects.toBeInstanceOf(TransportInvalidError);
    expect(kv.status).toHaveBeenCalledWith(bucket);
    expect(broker.streamApi.info).not.toHaveBeenCalled();
    expect(broker.streamApi.update).not.toHaveBeenCalled();
    expect(broker.consumerApi.add).not.toHaveBeenCalled();
    expect(kv.update).not.toHaveBeenCalled();
    expect(broker.handle.close).toHaveBeenCalledTimes(1);
  });

  it.each(["direct", "factory"] as const)("%s runtime refuses an existing inbox replica mismatch, closes, and never updates it", async style => {
    for (const replicas of [1, 3]) {
      const broker = fakeBroker();
      const kv = fakeKv();
      kv.setObserved(replicas);
      const existing = { config: transportStreamConfig({ ...inboxPolicy, replicas: replicas === 1 ? 3 : 1 }) } as StreamInfo;
      broker.streams.set(INBOX, structuredClone(existing));
      const opening = style === "direct"
        ? openNatsTransport({ ...base, provision: true, replicas })
        : openNatsTransport(factoryOptions(replicas));
      await expect(opening).rejects.toBeInstanceOf(TransportInvalidError);
      expect(broker.streams.get(INBOX)).toEqual(existing);
      expect(broker.streamApi.update).not.toHaveBeenCalled();
      expect(broker.consumerApi.add).not.toHaveBeenCalled();
      expect(broker.handle.close).toHaveBeenCalledTimes(1);
      vi.restoreAllMocks();
    }
  });

  it.each([0, -1, 1.5, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1, null, "3"])("refuses invalid replicas %s before resource access", async invalid => {
    const broker = fakeBroker();
    const kv = fakeKv();
    const replicas = invalid as number;
    await expect(openNatsKv(broker.js, { bucket: "fabric_leases", replicas })).rejects.toBeInstanceOf(TransportInvalidError);
    await expect(openNatsTransport({ ...base, replicas })).rejects.toBeInstanceOf(TransportInvalidError);
    await expect(openNatsTransport(factoryOptions(replicas))).rejects.toBeInstanceOf(TransportInvalidError);
    expect(connections.runtime).not.toHaveBeenCalled();
    expect(kv.create).not.toHaveBeenCalled();
    expect(kv.open).not.toHaveBeenCalled();
  });

  it.each([undefined, 3])("admin reads the agent-only replica count once (configured=%s), including the advisory stream", async replicas => {
    const root = privateScratch();
    const source = JSON.stringify({ mesh: { nats: replicas === undefined ? {} : { replicas } } });
    fs.writeFileSync(path.join(root, "fabric.json"), source);
    vi.stubEnv("PI_CODING_AGENT_DIR", root);
    // This global override must not affect the read-only agent lookup.
    vi.stubEnv("PI_FABRIC_MESH_NATS", JSON.stringify({ replicas: replicas === 3 ? 1 : 3 }));
    vi.stubEnv("PI_FABRIC_NATS_SERVERS", base.servers as string);
    vi.stubEnv("PI_FABRIC_NATS_CREDENTIALS", "fixture-bootstrap-only");
    const broker = fakeBroker();
    const kv = fakeKv();
    kv.setObserved(replicas ?? 1);
    vi.spyOn(process.stdout, "write").mockImplementation(() => true);
    expect(await runTransportAdmin(["provision", "--machines", MACHINE])).toBe(0);
    expect(loadAgentNatsReplicas).toHaveBeenCalledExactlyOnceWith(root);
    expect(loadGlobalFabricConfig).not.toHaveBeenCalled();
    expect(kv.create.mock.calls.map(([bucket, options]) => [bucket, options?.replicas])).toEqual([
      ["fabric_leases", replicas ?? 1], ["fabric_presence", replicas ?? 1],
    ]);
    expect(broker.streams.get(INBOX)?.config.num_replicas).toBe(replicas ?? 1);
    expect(broker.streams.get(ADVISORY_CAPTURE_STREAM)?.config.num_replicas).toBe(replicas ?? 1);
    expect(broker.streamApi.update).not.toHaveBeenCalled();
    expect(broker.handle.close).toHaveBeenCalledTimes(1);
    expect(fs.readFileSync(path.join(root, "fabric.json"), "utf8")).toBe(source);
  });

  it.each(["fabric_leases", "fabric_presence", INBOX, ADVISORY_CAPTURE_STREAM])("admin refuses existing R1 %s under agent R3 policy without updating it", async resource => {
    const root = privateScratch();
    fs.writeFileSync(path.join(root, "fabric.json"), JSON.stringify({ mesh: { nats: { replicas: 3 } } }));
    vi.stubEnv("PI_CODING_AGENT_DIR", root);
    vi.stubEnv("PI_FABRIC_NATS_SERVERS", base.servers as string);
    vi.stubEnv("PI_FABRIC_NATS_CREDENTIALS", "fixture-bootstrap-only");
    const broker = fakeBroker();
    const kv = fakeKv();
    kv.setObserved(3);
    const policy = resource === ADVISORY_CAPTURE_STREAM ? advisoryPolicy : inboxPolicy;
    const existing = { config: transportStreamConfig({ ...policy, replicas: 1 }) } as StreamInfo;
    if (resource === INBOX || resource === ADVISORY_CAPTURE_STREAM) broker.streams.set(resource, structuredClone(existing));
    else kv.setObserved(1, resource);
    await expect(runTransportAdmin(["provision", "--machines", MACHINE])).rejects.toBeInstanceOf(TransportInvalidError);
    expect(loadAgentNatsReplicas).toHaveBeenCalledExactlyOnceWith(root);
    expect(loadGlobalFabricConfig).not.toHaveBeenCalled();
    expect(broker.streamApi.update).not.toHaveBeenCalled();
    expect(kv.update).not.toHaveBeenCalled();
    expect(broker.handle.close).toHaveBeenCalledTimes(1);
    if (resource === INBOX || resource === ADVISORY_CAPTURE_STREAM) expect(broker.streams.get(resource)).toEqual(existing);
    else expect((await kv.status(resource)).replicas).toBe(1);
  });

  it("admin rejects an invalid agent replica policy before connecting or provisioning", async () => {
    const root = privateScratch();
    fs.writeFileSync(path.join(root, "fabric.json"), JSON.stringify({ mesh: { nats: { replicas: 0 } } }));
    vi.stubEnv("PI_CODING_AGENT_DIR", root);
    const broker = fakeBroker();
    const kv = fakeKv();
    await expect(runTransportAdmin(["provision", "--machines", MACHINE])).rejects.toThrow(/replicas/);
    expect(loadAgentNatsReplicas).toHaveBeenCalledExactlyOnceWith(root);
    expect(loadGlobalFabricConfig).not.toHaveBeenCalled();
    expect(connections.bootstrap).not.toHaveBeenCalled();
    expect(kv.create).not.toHaveBeenCalled();
    expect(broker.streamApi.add).not.toHaveBeenCalled();
  });
});

// Expected skip: no explicitly supplied NATS_BIN, or the generic broker-free CI profile.
// Required CI mode overrides both conditions: it must fail, never silently skip broker coverage.
const binary = process.env.NATS_BIN?.trim();
const required = process.env.NATS_SERVER_REQUIRED === "1";
if (required && !binary) throw new Error("NATS_SERVER_REQUIRED=1 but NATS_BIN is missing; an absolute nats-server 2.14.7 binary is required");
const skipCluster = !required && (!binary || process.env.FABRIC_TEST_SKIP_NATS === "1");

it.skipIf(skipCluster)("real NATS 2.14.7 R3 leases, presence and actual transport inbox publish/consume/confirmed ACK", async () => {
  // The helper starts/terminates only its three direct children, on unique ports, inside test-owned scratch.
  const root = privateScratch();
  vi.stubEnv("TMPDIR", root);
  const expectedVersion = process.env.NATS_EXPECTED_SERVER_VERSION ?? "2.14.7";
  expect(expectedVersion).toBe("2.14.7");
  cluster = await startNatsElectionCluster(binary!);
  expect(cluster.nodes).toHaveLength(3);
  // Reject a wrong binary through each real server's monitor response, not just a CLI version string.
  for (const node of cluster.nodes) {
    const response = await fetch(`${node.monitorUrl}/varz`, { signal: AbortSignal.timeout(2_000) });
    expect(response.ok).toBe(true);
    expect(await response.json()).toMatchObject({ version: expectedVersion, server_name: node.name });
  }
  const agentDir = path.join(root, "agent");
  fs.mkdirSync(agentDir, { recursive: true, mode: 0o700 });
  fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({ mesh: { nats: { replicas: 3 } } }));
  vi.stubEnv("PI_CODING_AGENT_DIR", agentDir);
  vi.stubEnv("PI_FABRIC_NATS_SERVERS", cluster.seedUrl);
  vi.stubEnv("PI_FABRIC_NATS_CREDENTIALS", "fixture-bootstrap-only");
  vi.spyOn(process.stdout, "write").mockImplementation(() => true);

  const adminNc = await connect({ servers: [cluster.seedUrl], ignoreClusterUpdates: true, timeout: 2_000 });
  rawConnections.push(adminNc);
  const adminManager = await jetstreamManager(adminNc);
  const adminHandle: NatsConnectionHandle = {
    nc: adminNc, js: jetstream(adminNc), manager: adminManager, isVerified: () => !adminNc.isClosed(), onStatus: () => () => undefined,
    closed: () => adminNc.closed(), close: () => adminNc.close(),
  };
  connections.bootstrap.mockResolvedValue(adminHandle);
  // Exercise the exact provisioning path that consumes the agent-only config, including FABRIC_ADV.
  expect(await runTransportAdmin(["provision", "--machines", MACHINE])).toBe(0);
  expect(loadAgentNatsReplicas).toHaveBeenCalledExactlyOnceWith(agentDir);
  expect(loadGlobalFabricConfig).not.toHaveBeenCalled();

  const nc = await connect({ servers: [cluster.seedUrl], ignoreClusterUpdates: true, timeout: 2_000 });
  rawConnections.push(nc);
  const manager = await jetstreamManager(nc);
  const js = jetstream(nc);
  const handle: NatsConnectionHandle = {
    nc, js, manager, isVerified: () => !nc.isClosed(), onStatus: () => () => undefined,
    closed: () => nc.closed(), close: () => nc.close(),
  };
  connections.runtime.mockResolvedValue(handle);
  // Runtime opens the admin-provisioned R3 resources and exercises the real inbox path.
  const transport = await openNatsTransport({ ...base, servers: cluster.seedUrl, replicas: 3, provision: false });
  transports.push(transport);
  for (const name of ["KV_fabric_leases", "KV_fabric_presence", ADVISORY_CAPTURE_STREAM, INBOX]) {
    await waitForNatsStreamReady(cluster.seedUrl, name);
    const info = await manager.streams.info(name);
    expect(info.config).toMatchObject({ name, storage: "file", num_replicas: 3, allow_direct: false });
    expect(info.cluster?.replicas).toHaveLength(2);
    expect(info.cluster?.replicas?.every(peer => peer.current && !peer.offline)).toBe(true);
  }
  for (const policy of bucketPolicies) {
    // Re-open verifies the actual KV status (including replicas) without creating/updating it.
    await openNatsKv(js, { ...policy, replicas: 3 }, false);
    const status = await (await new Kvm(js).open(policy.bucket)).status();
    expect(status.replicas).toBe(3);
    expect(status.streamInfo.config.num_replicas).toBe(3);
  }
  expect((await manager.streams.info(ADVISORY_CAPTURE_STREAM)).config.num_replicas).toBe(3);
  const durable = transportDurableName(HOST);
  const before = await manager.consumers.info(INBOX, durable);
  expect(await transport.drainState()).toEqual({ pending: 0, ackPending: 0 });
  const envelope = {
    id: "r3-inbox-message", kind: "command" as const, to: transport.self, targetId: "replica-target", operation: "followUp",
    from: { id: HOST, name: "replica-test", kind: "main" as const }, body: { text: "real R3 round trip" },
  };
  const sent = await transport.send(envelope);
  expect(sent).toMatchObject({ id: envelope.id, duplicate: false });
  expect(sent.sequence).toBeGreaterThan(0);
  expect(await transport.drainState()).toEqual({ pending: 1, ackPending: 0 });
  const abort = new AbortController();
  const inbox = transport.inbox(abort.signal)[Symbol.asyncIterator]();
  try {
    const received = await bounded(inbox.next(), "real inbox delivery");
    expect(received.done).toBe(false);
    if (received.done || !received.value) throw new Error("inbox ended without delivery");
    expect(received.value).toMatchObject({ sequence: sent.sequence, sourceMachine: MACHINE, attempt: 1,
      payload: { id: envelope.id, body: envelope.body, to: transport.self } });
    expect(await transport.drainState()).toEqual({ pending: 0, ackPending: 1 });
    await bounded(received.value.ack(), "broker-confirmed R3 ACK");
    expect(await transport.drainState()).toEqual({ pending: 0, ackPending: 0 });
    const after = await manager.consumers.info(INBOX, durable);
    expect(after.created).toBe(before.created);
    expect(after).toMatchObject({ num_pending: 0, num_ack_pending: 0, ack_floor: { stream_seq: sent.sequence, consumer_seq: 1 } });
    // Limits retention keeps the message after ACK; pending=0 must not be achieved by deleting/purging data.
    const stream = await manager.streams.info(INBOX);
    expect(stream.config).toMatchObject({ num_replicas: 3, retention: "limits" });
    expect(stream.state).toMatchObject({ messages: 1, last_seq: sent.sequence });
    const retained = await manager.streams.getMessage(INBOX, { seq: sent.sequence });
    expect(retained).not.toBeNull();
    if (!retained) throw new Error("ACKed Limits-retained inbox message is missing");
    expect(JSON.parse(new TextDecoder().decode(retained.data))).toMatchObject({ format: 1, sourceMachine: MACHINE, envelope: { id: envelope.id, body: envelope.body } });
  } finally {
    abort.abort();
    if (inbox.return) await bounded(inbox.return(), "inbox shutdown");
  }
}, 90_000);
