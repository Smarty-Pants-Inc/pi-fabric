// expected-skip: missing nats-server binary: authenticated real-broker coverage needs NATS_BIN.
// Required: NATS_AUTH_REQUIRED=1 (or PI_FABRIC_NATS_REQUIRED=1). Native B2 is nats-owner-crash.test.ts.
// Keep reason: data safety/trust boundary; real ack-deadline loss and process/server death.
// These are CARRIER tests, not T-B2a/b/c or application exactly-once evidence.
import { spawn, type ChildProcess } from "node:child_process";
import { Buffer } from "node:buffer";
import fs from "node:fs";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { fileURLToPath } from "node:url";
import { setTimeout as sleep } from "node:timers/promises";
import { jetstream, jetstreamManager, type JetStreamManager } from "@nats-io/jetstream";
import { describe, expect, it } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../../src/config.js";
import { MeshStore } from "../../src/mesh/store.js";
import { openFabricTransport } from "../../src/transport/index.js";
import { openNatsConnection } from "../../src/transport/nats/connection.js";
import { openNatsKvForMachine } from "../../src/transport/nats/kv.js";
import { ensureTransportStream } from "../../src/transport/nats/provision.js";
import {
  ADVISORY_CAPTURE_STREAM, transportAdvisoryStreamSubject, transportDurableName,
  transportInboxStream, transportInboxStreamSubject,
} from "../../src/transport/nats/subjects.js";
import type {
  FabricTransport, FabricTransportOptions, TransportDelivery, TransportEnvelope,
} from "../../src/transport/types.js";
import {
  createPathBHarness, resolveNatsBinary, type PathBHarness, type PathBUser,
} from "../helpers/nats-cluster.js";
import { launchLog, same, type LaunchLog, type Owned } from "../helpers/owned-processes.js";

const binary = resolveNatsBinary(); // Invalid explicit NATS_BIN is an error, never a skip.
const required = process.env.NATS_AUTH_REQUIRED === "1" || process.env.PI_FABRIC_NATS_REQUIRED === "1";
const ACK_WAIT_MS = 2_000;
const PRODUCTION_ACK_WAIT_MS = 30_000;
const A_HOST = "runtime:broker-fault-a";
const B_HOST = "runtime:broker-fault-b";
// This file owns carrier faults, not native admission. The separate nats-owner-crash
// suite runs actual CP/Main OS-death claim/custody/ACK-PubAck boundaries.

type Delivery = TransportDelivery<TransportEnvelope>;
type Inbox = AsyncIterator<Delivery>;
type ChildReceipt = {
  type: string;
  pid?: number;
  started?: string;
  node?: string;
  bun?: boolean;
  sourceMachine?: string;
  verification?: string;
  attempt?: number;
  sequence?: number;
  payloadBase64?: string;
  id?: string;
};

async function bounded<T>(promise: PromiseLike<T>, ms: number, label: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new Error(label)), ms); }),
    ]);
  } finally { if (timer) clearTimeout(timer); }
}

function payloadBytes(delivery: Delivery): Buffer {
  // The public port exposes a decoded envelope, not JsMsg.data. Compare its entire UTF-8
  // serialization, including createdAt, NOT just body.text or a newly stamped send attempt.
  return Buffer.from(JSON.stringify(delivery.payload), "utf8");
}

function take(inbox: Inbox, ms = 12_000): Promise<Delivery> {
  const pending = (async () => {
    const result = await bounded(inbox.next(), ms, "real broker did not deliver before the finite deadline");
    if (result.done || !result.value) throw new Error("inbox ended instead of delivering");
    return result.value;
  })();
  // A publish/assertion may fail before its prearmed receive is awaited. Cleanup still
  // aborts the iterator; observe that rejection without hiding it from the real awaiter.
  void pending.catch(() => undefined);
  return pending;
}

function command(to: FabricTransport["self"], id: string): TransportEnvelope {
  return {
    id, kind: "command", to, targetId: "task:broker-fault-target", operation: "followUp",
    from: { id: A_HOST, name: "authenticated fault sender", kind: "main" },
    body: { text: "retained π / newline\n / null", opaque: [0, false, null, "same bytes"] },
  };
}

/** Fresh server/store per case; setup authority is never supplied to a runtime. */
async function fixture() {
  const harness = await createPathBHarness({ binary: binary! });
  const transports: FabricTransport[] = [];
  const meshes: MeshStore[] = [];
  const abort = new AbortController();
  try {
    const bootstrap = await harness.connect(harness.users.bootstrap);
    const manager = await jetstreamManager(bootstrap);
    const js = jetstream(bootstrap);
    const policy = {
      maxBytes: 1_073_741_824, maxAgeMs: 0, maxMessageBytes: 1_048_576,
      duplicateWindowMs: 600_000, replicas: 1 as const,
    };
    // Capture first, then buckets and inboxes, exactly as the authenticated Path B helper test.
    await ensureTransportStream(manager, {
      ...policy, name: ADVISORY_CAPTURE_STREAM, subjects: [transportAdvisoryStreamSubject],
      maxAgeMs: 30 * 86_400_000, maxMessageBytes: 65_536,
    });
    await openNatsKvForMachine(js, "leases", "machine-a", true);
    await openNatsKvForMachine(js, "presence", "machine-a", true);
    for (const machine of ["machine-a", "machine-b"]) {
      await ensureTransportStream(manager, {
        ...policy, name: transportInboxStream(machine), subjects: [transportInboxStreamSubject(machine)],
      });
    }
    await bootstrap.close();

    const verifyUser = async (user: PathBUser): Promise<void> => {
      const connection = await openNatsConnection({
        servers: harness.serverUrl, credentials: user.credsPath, machine: user.machine,
      });
      try {
        // This makes a real USER.INFO request and binds the signed subject and machine grant.
        expect(connection.userInfo).toMatchObject({ data: { user: user.publicKey } });
        const info = connection.userInfo as { data: { permissions: { publish: { allow: string[] } } } };
        expect(info.data.permissions.publish.allow).toContain(`fabric.in.*.*.*.*.${user.machine}`);
      } finally { await connection.close(); }
    };
    await verifyUser(harness.users.machineA);
    await verifyUser(harness.users.machineB);

    const open = async (user: PathBUser, hostId: string): Promise<FabricTransport> => {
      const root = path.join(harness.root, `mesh-${meshes.length}`);
      const mesh = new MeshStore(root, 1_048_576, 100, { stateBackend: "file" });
      meshes.push(mesh);
      // Use A2's public configuration shape; real machine verification stays enabled.
      const config = {
        ...DEFAULT_FABRIC_CONFIG.mesh, root, transport: "nats",
        nats: {
          ...DEFAULT_FABRIC_CONFIG.mesh.nats, servers: [harness.serverUrl],
          machine: user.machine, credentials: user.credsPath,
        },
      } as FabricTransportOptions["config"];
      const transport = await openFabricTransport({ config, hostId, mesh });
      transports.push(transport);
      expect(transport.kind).toBe("nats");
      expect(transport.self).toEqual({ machine: user.machine, hostId });
      return transport;
    };
    return {
      harness, open, abort,
      close: async () => {
        abort.abort();
        try { await Promise.all(transports.map((transport) => transport.close())); }
        finally {
          for (const mesh of meshes) mesh.closeState();
          await harness.stop();
        }
      },
    };
  } catch (error) {
    abort.abort();
    await Promise.allSettled(transports.map((transport) => transport.close()));
    for (const mesh of meshes) mesh.closeState();
    await harness.stop();
    throw error;
  }
}

/**
 * TEST-ONLY administrative override, AFTER binding the real production-policy inbox.
 * Backoff takes precedence over ack_wait on the server: update BOTH and verify BOTH.
 * No mock, patched validator, runtime policy option, or relaxed security credential.
 */
async function setAckWait(harness: PathBHarness, waitMs: number): Promise<void> {
  const bootstrap = await harness.connect(harness.users.bootstrap);
  try {
    const manager = await jetstreamManager(bootstrap);
    const stream = transportInboxStream("machine-b");
    const durable = transportDurableName(B_HOST);
    const before = await manager.consumers.info(stream, durable);
    await manager.consumers.update(stream, durable, {
      ack_wait: waitMs * 1_000_000,
      backoff: Array.from({ length: 5 }, () => waitMs * 1_000_000),
    });
    const after = await manager.consumers.info(stream, durable);
    expect(after.config.ack_wait).toBe(waitMs * 1_000_000);
    expect(after.config.backoff).toEqual(Array.from({ length: 5 }, () => waitMs * 1_000_000));
    expect(after.created).toBe(before.created);
    expect(after.config).toMatchObject({
      filter_subject: before.config.filter_subject,
      ack_policy: before.config.ack_policy,
      deliver_policy: before.config.deliver_policy,
      max_ack_pending: 128, max_deliver: 5,
    });
  } finally { await bootstrap.close(); }
}

async function consumerInfo(harness: PathBHarness) {
  // State receipts after setup use the destination's runtime principal, not bootstrap.
  const runtime = await harness.connect(harness.users.machineB);
  try {
    const manager: JetStreamManager = await jetstreamManager(runtime);
    return await manager.consumers.info(transportInboxStream("machine-b"), transportDurableName(B_HOST));
  } finally { await runtime.close(); }
}

async function serverIncarnation(harness: PathBHarness): Promise<string> {
  const response = await fetch(`${harness.monitorUrl}/varz`, { signal: AbortSignal.timeout(2_500) });
  expect(response.status).toBe(200);
  const value = await response.json() as { server_id?: unknown };
  if (typeof value.server_id !== "string" || !value.server_id) throw new Error("broker has no server incarnation receipt");
  return value.server_id;
}

async function bindAndPrime(a: FabricTransport, b: FabricTransport, signal: AbortSignal): Promise<Inbox> {
  const inbox = b.inbox(signal)[Symbol.asyncIterator]();
  const waiting = take(inbox);
  await a.send(command(b.self, "bind-production-policy"));
  const prime = await waiting;
  expect(prime.payload.id).toBe("bind-production-policy");
  await prime.ack();
  expect(await b.drainState()).toEqual({ pending: 0, ackPending: 0 });
  return inbox;
}

/** Event-driven IPC and close joins. Polling is never the receipt source. */
function observeChild(child: ChildProcess) {
  const queue: ChildReceipt[] = [];
  let waiter: { resolve: (value: ChildReceipt) => void; reject: (error: Error) => void } | undefined;
  let ended = false;
  let failure: Error | undefined;
  const closed = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolve) => {
    child.once("error", (error) => { failure = error; waiter?.reject(error); waiter = undefined; });
    child.once("close", (code, signal) => {
      ended = true;
      waiter?.reject(failure ?? new Error(`owned Node consumer closed before IPC receipt (${code}/${signal})`));
      waiter = undefined;
      resolve({ code, signal });
    });
  });
  child.on("message", (value: unknown) => {
    if (!value || typeof value !== "object" || typeof (value as ChildReceipt).type !== "string") {
      failure = new Error("invalid owned Node consumer IPC receipt");
      waiter?.reject(failure); waiter = undefined; return;
    }
    const receipt = value as ChildReceipt;
    if (receipt.type === "error") {
      failure = new Error("owned Node consumer reported a fixture failure (credentials omitted)");
      waiter?.reject(failure); waiter = undefined; return;
    }
    if (waiter) { const current = waiter; waiter = undefined; current.resolve(receipt); }
    else queue.push(receipt);
  });
  return {
    closed,
    next: async (type: string): Promise<ChildReceipt> => {
      const receipt = queue.shift() ?? await bounded(new Promise<ChildReceipt>((resolve, reject) => {
        if (failure || ended) { reject(failure ?? new Error("consumer already closed")); return; }
        if (waiter) { reject(new Error("concurrent IPC receipt wait")); return; }
        waiter = { resolve, reject };
      }), 20_000, `Node consumer IPC deadline: ${type}`);
      expect(receipt.type).toBe(type);
      return receipt;
    },
  };
}

function ownedRecord(child: ChildProcess, log: LaunchLog, birth?: ChildReceipt): Owned {
  const records = log.owned().filter((entry) => entry.pid === child.pid);
  if (records.length !== 1) throw new Error("consumer lacks one exact preload PID/birth record; refusing signal");
  const record = records[0]!;
  if (record.pid === process.pid || record.pid === process.ppid || !same(record)) {
    throw new Error("consumer PID no longer names the exact owned birth; refusing signal");
  }
  if (birth && (birth.pid !== record.pid || birth.started !== record.started)) {
    throw new Error("IPC birth differs from inherited preload birth; refusing signal");
  }
  return record;
}

function killRecorded(record: Owned): void {
  if (record.pid === process.pid || record.pid === process.ppid || !same(record)) {
    throw new Error("owned Node consumer incarnation changed before SIGKILL");
  }
  process.kill(record.pid, "SIGKILL"); // Only this exact spawn's independently recorded birth, never a group/argv scan.
}

if (required && !binary) {
  it("required authenticated broker prerequisite: NATS_BIN", () => {
    throw new Error("FABRIC_TEST_PREREQUISITE_NATS_BIN: set NATS_BIN to an absolute nats-server executable");
  });
}

describe.skipIf(!binary)("authenticated real broker inbox faults (carrier only, not native CP-B2)", () => {
  it("T-X3b carrier LOSTACK: no final ACK and no NAK; actual ack_wait redelivers identical envelope, then ACK drains 0/0", async () => {
    const f = await fixture();
    try {
      const a = await f.open(f.harness.users.machineA, A_HOST);
      const b = await f.open(f.harness.users.machineB, B_HOST);
      const inbox = await bindAndPrime(a, b, f.abort.signal);
      await setAckWait(f.harness, ACK_WAIT_MS);
      // The administrative override must NOT become an accepted runtime policy or bypass.
      await expect(f.open(f.harness.users.machineB, B_HOST)).rejects.toThrow("NATS consumer configuration mismatch");
      const waiting = take(inbox);
      const publishedAt = performance.now();
      const sent = await a.send(command(b.self, "lost-final-broker-ack"));
      const first = await waiting;
      const bytes = payloadBytes(first);
      expect(first.attempt).toBe(1);
      expect(first.sequence).toBe(sent.sequence);
      expect(first.sourceMachine).toBe("machine-a");
      expect(first.verification).toBe("bridge");
      expect(first.payload).toEqual(sent.envelope);
      expect(await b.drainState()).toEqual({ pending: 0, ackPending: 1 });
      // Deliberately omit final ACK, working and NAK. Only the server's actual deadline can retry.
      const again = await take(inbox);
      expect(performance.now() - publishedAt).toBeGreaterThanOrEqual(ACK_WAIT_MS * 0.75);
      expect(again.attempt).toBeGreaterThanOrEqual(2);
      expect(again.sequence).toBe(first.sequence);
      expect(again.sourceMachine).toBe(first.sourceMachine);
      expect(again.payload.id).toBe(first.payload.id);
      expect(payloadBytes(again).equals(bytes)).toBe(true);
      await again.ack(); // Real double ACK; no application ACK/custody claim is made here.
      expect(await b.drainState()).toEqual({ pending: 0, ackPending: 0 });
      expect(await a.drainState()).toEqual({ pending: 0, ackPending: 0 });
    } finally { await f.close(); }
  }, 60_000);

  it("working() counterexample: no redelivery over three real ack_wait periods, then retry after keepalives stop", async () => {
    const f = await fixture();
    let keepalive: ReturnType<typeof setInterval> | undefined;
    try {
      const a = await f.open(f.harness.users.machineA, A_HOST);
      const b = await f.open(f.harness.users.machineB, B_HOST);
      const inbox = await bindAndPrime(a, b, f.abort.signal);
      await setAckWait(f.harness, ACK_WAIT_MS);
      const waiting = take(inbox);
      await a.send(command(b.self, "working-until-stopped"));
      const first = await waiting;
      expect(first.attempt).toBe(1);
      const bytes = payloadBytes(first);
      let calls = 0;
      // Broker delivery is the event source. The contractual ackWait/3 timer emits WPI only;
      // the bounded hold below is an intentional negative-observation window, not polling.
      first.working();
      keepalive = setInterval(() => { calls++; first.working(); }, Math.floor(ACK_WAIT_MS / 3));
      const next = take(inbox, 18_000);
      const outcome = await Promise.race([
        next.then(() => "redelivered" as const),
        sleep(ACK_WAIT_MS * 3).then(() => "held" as const),
      ]);
      expect(outcome).toBe("held");
      expect(calls).toBeGreaterThanOrEqual(3);
      expect(await b.drainState()).toEqual({ pending: 0, ackPending: 1 });
      clearInterval(keepalive); keepalive = undefined;
      const again = await next;
      expect(again.attempt).toBeGreaterThanOrEqual(2);
      expect(again.sequence).toBe(first.sequence);
      expect(again.sourceMachine).toBe("machine-a");
      expect(payloadBytes(again).equals(bytes)).toBe(true);
      await again.ack();
      expect(await b.drainState()).toEqual({ pending: 0, ackPending: 0 });
    } finally {
      if (keepalive) clearInterval(keepalive);
      await f.close();
    }
  }, 60_000);

  it("carrier crash/rebind: exact owned Node PID dies before ACK; real server restart retains the same host durable and bytes", async () => {
    if (process.release.name !== "node" || "bun" in process.versions) {
      throw new Error("FABRIC_TEST_PREREQUISITE_NODE: consumer must inherit guarded NODE_OPTIONS under Node, not Bun");
    }
    const f = await fixture();
    let child: ChildProcess | undefined;
    let observed: ReturnType<typeof observeChild> | undefined;
    let log: LaunchLog | undefined;
    try {
      const a = await f.open(f.harness.users.machineA, A_HOST);
      const childRoot = fs.mkdtempSync(path.join(f.harness.root, "consumer-"));
      log = launchLog(childRoot);
      const childFile = fileURLToPath(new URL("./broker-fixtures/inbox-consumer.mjs", import.meta.url));
      child = spawn(process.execPath, [childFile], {
        cwd: childRoot,
        env: {
          ...process.env, ...log.env,
          TMPDIR: childRoot, TMP: childRoot, TEMP: childRoot,
          VITE_CACHE_DIR: path.join(childRoot, "vite"),
          XDG_CACHE_HOME: path.join(childRoot, "xdg"),
          npm_config_cache: path.join(childRoot, "npm-cache"),
          JITI_FS_CACHE: path.join(childRoot, "jiti"),
          FABRIC_BROKER_URL: f.harness.serverUrl,
          FABRIC_BROKER_CREDS: f.harness.users.machineB.credsPath,
          FABRIC_BROKER_USER: f.harness.users.machineB.publicKey,
          FABRIC_BROKER_HOST: B_HOST,
        },
        stdio: ["ignore", "ignore", "ignore", "ipc"],
      });
      observed = observeChild(child);
      const birth = await observed.next("birth");
      expect(birth.node).toBeTruthy();
      expect(birth.bun).toBe(false);
      ownedRecord(child, log, birth);
      await observed.next("ready");
      await a.send(command({ machine: "machine-b", hostId: B_HOST }, "child-prime"));
      await observed.next("primed"); // The child ACKed prime after binding under production policy.
      await setAckWait(f.harness, ACK_WAIT_MS);
      const before = await consumerInfo(f.harness);
      const sent = await a.send(command({ machine: "machine-b", hostId: B_HOST }, "held-before-os-death"));
      const held = await observed.next("held");
      expect(held).toMatchObject({
        id: sent.id, attempt: 1, sequence: sent.sequence,
        sourceMachine: "machine-a", verification: "bridge",
        payloadBase64: Buffer.from(JSON.stringify(sent.envelope)).toString("base64"),
      });
      const inHand = await consumerInfo(f.harness);
      expect(inHand.num_ack_pending).toBe(1);
      killRecorded(ownedRecord(child, log, birth));
      const death = await bounded(observed.closed, 5_000, "exact owned Node consumer was not close-joined");
      expect(death.signal).toBe("SIGKILL");
      expect(death.code).toBeNull();
      expect(same({ pid: birth.pid!, started: birth.started! })).toBe(false);
      expect((await consumerInfo(f.harness)).num_ack_pending).toBe(1);

      // Rebind must still pass the production validator. Restore the policy as bootstrap,
      // never teach a runtime to accept weakened policy. A production wait may follow restart.
      await setAckWait(f.harness, PRODUCTION_ACK_WAIT_MS);
      await a.close();
      const oldServer = await serverIncarnation(f.harness);
      await f.harness.restart(); // Actual owned nats-server stop/start, same config and FILE store.
      expect(await serverIncarnation(f.harness)).not.toBe(oldServer);
      const retained = await consumerInfo(f.harness);
      // Server restore may round creation-time submillisecond metadata. Durable
      // identity is the name/filter plus retained in-hand state and attempt/seq below.
      expect(retained.name).toBe(before.name);
      expect(retained.num_ack_pending).toBe(1);
      expect(retained.config.filter_subject).toBe(before.config.filter_subject);
      const rebound = await f.open(f.harness.users.machineB, B_HOST);
      const inbox = rebound.inbox(f.abort.signal)[Symbol.asyncIterator]();
      const again = await take(inbox, 42_000);
      expect(again.attempt).toBeGreaterThanOrEqual(2);
      expect(again.sequence).toBe(sent.sequence);
      expect(again.payload.id).toBe(held.id);
      expect(again.sourceMachine).toBe(held.sourceMachine);
      expect(again.verification).toBe(held.verification);
      expect(payloadBytes(again).toString("base64")).toBe(held.payloadBase64);
      await again.ack();
      expect(await rebound.drainState()).toEqual({ pending: 0, ackPending: 0 });
      expect((await consumerInfo(f.harness)).name).toBe(before.name);
    } finally {
      // Even failed assertions leave no live Node consumer. Never signal an unrecorded PID.
      try {
        if (child && observed) {
          if (child.pid !== undefined && child.exitCode === null && child.signalCode === null) {
            if (!log) throw new Error("missing owned launch log during consumer cleanup");
            killRecorded(ownedRecord(child, log));
          }
          // Always join close, even if exit/signal was already observable on a failure path.
          await bounded(observed.closed, 5_000, "owned Node cleanup close-join deadline");
        }
      } finally { await f.close(); }
    }
  }, 120_000);
});
