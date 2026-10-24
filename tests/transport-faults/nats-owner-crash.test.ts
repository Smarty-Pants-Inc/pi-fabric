// expected-skip: missing nats-server binary: optional real JWT broker prerequisite (NATS_BIN).
// NATS_AUTH_REQUIRED=1 makes missing binary FAIL; invalid explicit NATS_BIN/import/runtime errors never skip.
// expected-skip: win32: POSIX SIGKILL and real file namespace durability barriers required.
// Keep reason: data safety; OS death between actual CP claim, Main custody, ACK PubAck and broker ACK.
// B2 ONLY. Manual known ParticipantView, NOT production roots/leases, B3 consumption, power-cut or HA proof.
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../../src/config.js";
import { MeshStore } from "../../src/mesh/store.js";
import { controlBodyFingerprint } from "../../src/mesh/control-idempotency.js";
import { FabricControlPlane, type FabricControlCommand, type FabricControlAcceptance, type FabricControlResult, type FabricControlRequestOptions } from "../../src/topology/control-plane.js";
import { openFabricTransport } from "../../src/transport/index.js";
import { openBootstrapNatsConnection, openNatsConnection } from "../../src/transport/nats/connection.js";
import { openNatsKvForMachine } from "../../src/transport/nats/kv.js";
import { ensureTransportStream } from "../../src/transport/nats/provision.js";
import { ADVISORY_CAPTURE_STREAM, transportAdvisoryStreamSubject, transportDurableName, transportInboxStream, transportInboxStreamSubject } from "../../src/transport/nats/subjects.js";
import type { FabricTransport, TransportDelivery, TransportEnvelope, TransportSent } from "../../src/transport/types.js";
import { createPathBHarness, resolveNatsBinary, type PathBHarness } from "../helpers/nats-cluster.js";
import { same } from "../helpers/owned-processes.js";
import { OwnedNatsOwner, knownParticipants } from "./control-fixtures/nats-owner.js";
import { OWNER_HOST_ID, SENDER_HOST_ID, TARGET_ID, fixturePaths, seenKey, bounded, type CrashBoundary } from "./control-fixtures/paths.js";

const repository = fileURLToPath(new URL("../../", import.meta.url));
const binary = resolveNatsBinary(); // Invalid explicit setting throws, never becomes optional coverage.
const required = process.env.NATS_AUTH_REQUIRED === "1";
const indeterminate = "Fabric control outcome is indeterminate after owner restart";
type Seen = { commandId: string; hostId: string; targetId: string; bodyDigest: string; ownerIncarnation: string;
  carrier: "nats"; writtenAt: number; acceptance?: FabricControlAcceptance };
type Item = { id: string; deliveryId: string; message: string; handed?: true; inboxEventId?: string };
type Observation = { type: string; commandId: string; pid: number };
type Settlement = { ok: true; value: FabricControlResult } | { ok: false; error: unknown };
type Incoming = Pick<TransportDelivery<TransportEnvelope>, "sourceMachine" | "verification" | "sequence" | "attempt" | "payload">;
// Real delivery/ACK and PubAck events drive these waits; the timer is failure-only.
function receiptLog<T>(label: string) {
  const items: T[] = [], waiters = new Set<(item: T) => void>();
  return {
    items,
    push(item: T) { items.push(item); for (const waiter of [...waiters]) waiter(item); },
    async receipt(predicate: (item: T) => boolean): Promise<T> {
      const previous = items.find(predicate);
      if (previous !== undefined) return previous;
      let waiter: ((item: T) => void) | undefined;
      try {
        return await bounded(new Promise<T>(resolve => {
          waiter = item => { if (predicate(item)) resolve(item); };
          waiters.add(waiter);
        }), label, 20_000);
      } finally { if (waiter) waiters.delete(waiter); }
    },
  };
}
function track(request: Promise<FabricControlResult>) {
  const observation = { settlements: 0 };
  const result = request.then((value): Settlement => { observation.settlements++; return { ok: true, value }; },
    (error: unknown): Settlement => { observation.settlements++; return { ok: false, error }; });
  const settled = bounded(result, "actual sender settlement", 100_000);
  void settled.catch(() => undefined);
  return { observation, settled };
}
function accepted(result: Settlement): FabricControlResult {
  if (!result.ok) throw result.error;
  expect(result.value).toMatchObject({ queued: true, acknowledged: true, routed: "mesh", triggered: false });
  return result.value;
}
function unknown(result: Settlement): void {
  expect(result.ok).toBe(false);
  if (result.ok) throw new Error("Bare claim must not report success");
  expect(result.error).toBeInstanceOf(Error);
  expect((result.error as Error).message).toBe(indeterminate);
  expect(result.error).not.toMatchObject({ notRun: true });
  expect(result.error).not.toMatchObject({ notDelivered: true });
}

async function fixture() {
  const server = await createPathBHarness({ binary: binary!, tls: { dnsNames: ["localhost"], ipAddresses: [] } });
  const root = path.join(server.root, "native-owner");
  const paths = fixturePaths(root);
  const children: OwnedNatsOwner[] = [];
  const meshes: MeshStore[] = [];
  let carrier: FabricTransport | undefined, sender: FabricControlPlane | undefined;
  const commands: TransportSent[] = [];
  const incoming: Incoming[] = [];
  const senderAcks = receiptLog<Incoming>("actual sender delivery.ack returned");
  const cancellationSends = receiptLog<TransportSent>("actual cancellation PubAck returned");
  let ownerEpoch = "not-ready";
  let release!: () => void;
  const intakeGate = new Promise<void>(resolve => { release = resolve; });
  let cleaning: Promise<void> | undefined;
  const close = (): Promise<void> => cleaning ??= (async () => {
    const failures: unknown[] = [];
    const collect = async (operations: Promise<unknown>[]) => {
      for (const result of await Promise.allSettled(operations)) if (result.status === "rejected") failures.push(result.reason);
    };
    release(); // Allow the ONE actual inbox's held observation to unwind, including on failed assertions.
    try {
      await collect(children.map(child => child.killAndJoin()));
      if (sender) await collect([bounded(sender.close(), "sender CP close", 10_000)]);
      if (carrier) await collect([bounded(carrier.close(), "actual sender single-inbox close", 10_000)]);
    } finally {
      try { for (const mesh of meshes) mesh.closeState(); }
      // Always stop our own isolated server, even if a boundary/iterator/store close failed.
      finally { await server.stop(); }
    }
    if (failures.length) throw new AggregateError(failures, "NATS owner-crash cleanup not quiescent");
  })();
  try {
    fs.mkdirSync(root);
    const bootstrap = await openBootstrapNatsConnection(server.runtimeOptions(server.users.bootstrap));
    try {
      const { manager, js } = bootstrap;
      const policy = { maxBytes: 1_073_741_824, maxAgeMs: 0, maxMessageBytes: 1_048_576,
        duplicateWindowMs: 600_000, replicas: 1 as const };
      await ensureTransportStream(manager, { ...policy, name: ADVISORY_CAPTURE_STREAM,
        subjects: [transportAdvisoryStreamSubject], maxAgeMs: 30 * 86_400_000, maxMessageBytes: 65_536 });
      await openNatsKvForMachine(js, "leases", "machine-a", true);
      await openNatsKvForMachine(js, "presence", "machine-a", true);
      for (const machine of ["machine-a", "machine-b"]) await ensureTransportStream(manager, {
        ...policy, name: transportInboxStream(machine), subjects: [transportInboxStreamSubject(machine)],
      });
    } finally { await bootstrap.close(); }
    expect(bootstrap.nc.isClosed()).toBe(true); // Setup authority closed BEFORE opening either runtime.
    expect(server.configText).toContain("sync_interval: always");
    // E12: signed JWT machine tag + server-authenticated subject + expanded exact source grant.
    for (const user of [server.users.machineA, server.users.machineB]) {
      const connection = await openNatsConnection(server.runtimeOptions(user));
      try {
        expect(connection.userInfo).toMatchObject({ data: { user: user.publicKey } });
        const info = connection.userInfo as { data: { permissions: { publish: { allow: string[] } } } };
        expect(info.data.permissions.publish.allow).toContain(`fabric.in.*.*.*.*.${user.machine}`);
      } finally { await connection.close(); }
    }
    const senderMesh = new MeshStore(path.join(server.root, "sender-mesh"), 1_048_576, 1_000, { stateBackend: "file", canWakeResidents: () => false });
    const seen = new MeshStore(paths.seen, 1_048_576, 1_000, { stateBackend: "file" });
    meshes.push(senderMesh, seen);
    const config = { ...DEFAULT_FABRIC_CONFIG.mesh, transport: "nats" as const,
      nats: { ...DEFAULT_FABRIC_CONFIG.mesh.nats, ...server.runtimeOptions(server.users.machineA),
        operatorMachine: "fabric-ops" } };
    carrier = await openFabricTransport({ config, mesh: senderMesh, hostId: SENDER_HOST_ID });
    const real = carrier;
    expect(real.self).toEqual({ machine: "machine-a", hostId: SENDER_HOST_ID });
    const observed: FabricTransport = {
      kind: real.kind, self: real.self,
      send: async (envelope, signal) => {
        const sent = await real.send(envelope, signal);
        if (envelope.kind === "command" && envelope.operation === "followUp") commands.push(sent);
        if (envelope.kind === "command" && envelope.operation === "cancel") cancellationSends.push(sent);
        return sent;
      },
      inbox: async function* (signal) {
        for await (const delivery of real.inbox(signal)) {
          const receipt: Incoming = { sourceMachine: delivery.sourceMachine, verification: delivery.verification,
            sequence: delivery.sequence, attempt: delivery.attempt, payload: delivery.payload };
          incoming.push(receipt);
          // A receive scheduling gate, NOT sender.pause(): NATS pause would refuse sends.
          // This holds the real ACK before CP settlement; it neither forges nor consumes it.
          await intakeGate;
          if (signal.aborted) break;
          yield { ...delivery, ack: async () => {
            await delivery.ack(); // Delegate the ORIGINAL real broker double-ACK before emitting a receipt.
            senderAcks.push(receipt);
          } };
        }
      },
      kv: bucket => real.kv(bucket), drainState: signal => real.drainState(signal), close: () => real.close(),
    };
    sender = new FabricControlPlane(senderMesh, { id: SENDER_HOST_ID, name: "owner-crash-sender", kind: "main" }, {
      enabled: true, hostId: SENDER_HOST_ID, transport: observed, relayOperatorMachine: "fabric-ops",
      ownerIncarnation: "nats-owner-sender", participants: {
        resolve: id => knownParticipants(ownerEpoch).resolve(id), leaseLive: () => true,
        whenChanged: async () => { throw new Error("Manual routing fixture has no directory change events"); },
      }, acknowledgementTimeoutMs: 120_000,
    });
    sender.start(() => { throw new Error("Sender is not a native recipient"); });
    return {
      server, root, paths, commands, incoming, release, close,
      senderAck: (eventId: string) => senderAcks.receipt(item => item.payload.id === `ack:${eventId}`),
      cancellation: (id: string) => cancellationSends.receipt(item => command(item).cancelCommandId === id),
      cancellations: cancellationSends.items,
      launch: (boundary?: CrashBoundary) => {
        const child = new OwnedNatsOwner(repository, root, children.length, server.runtimeOptions(server.users.machineB), boundary);
        children.push(child); return child;
      },
      epoch: (value: unknown) => { ownerEpoch = String(value); },
      request: (key: string, incarnation: string, options: Pick<FabricControlRequestOptions, "signal" | "timeoutMs"> = {}) => sender!.request(OWNER_HOST_ID, TARGET_ID, "followUp",
        { ownerIncarnation: incarnation, message: "retain this real Main followUp" }, OWNER_HOST_ID, { ...options, idempotencyKey: key }),
      record: (id: string): Seen | undefined => seen.get(seenKey(id), { fresh: true })?.value as Seen | undefined,
      journal: (): Item[] => fs.existsSync(paths.journal)
        ? (JSON.parse(fs.readFileSync(paths.journal, "utf8")) as { items: Item[] }).items : [],
      observations: (): Observation[] => fs.existsSync(paths.observations)
        ? fs.readFileSync(paths.observations, "utf8").trim().split("\n").filter(Boolean).map(line => JSON.parse(line) as Observation) : [],
    };
  } catch (error) { await close(); throw error; }
}
async function consumerInfo(server: PathBHarness) {
  const runtime = await openNatsConnection(server.runtimeOptions(server.users.machineB));
  try {
    const { manager } = runtime;
    return await manager.consumers.info(transportInboxStream("machine-b"), transportDurableName(OWNER_HOST_ID));
  } finally { await runtime.close(); }
}
function command(sent: TransportSent): FabricControlCommand { return sent.envelope.body as FabricControlCommand; }
function noConsumption(f: Awaited<ReturnType<typeof fixture>>) {
  expect(f.observations().filter(item => ["unexpected-injection", "unexpected-nak", "unexpected-term"].includes(item.type))).toEqual([]);
  expect(f.journal().every(item => item.handed !== true)).toBe(true);
  expect(fs.existsSync(`${f.paths.journal}.delivered`)).toBe(false);
}

// No broad missing-source/integration skip: only absent optional server and POSIX prerequisite.
describe.skipIf(process.platform === "win32")("native NATS owner OS death / T-B2a,b,c + E24", () => {
  it.skipIf(!binary && !required)("E24 stored successful outcome settles a new CURRENT-epoch public same-key caller after OS death", async () => {
    if (!binary) throw new Error("NATS_AUTH_REQUIRED=1: missing nats-server; set absolute NATS_BIN");
    const f = await fixture();
    try {
      const first = f.launch();
      const ready = await first.receipt(message => message.type === "ready");
      expect(ready).toMatchObject({ pid: first.child.pid, ownerHostId: OWNER_HOST_ID, meshRoot: f.paths.mesh,
        journal: f.paths.journal, carrier: "nats", machine: "machine-b", nativeConsumption: false });
      f.epoch(ready.incarnation);
      f.release();
      const original = track(f.request("e24-stored-success", String(ready.incarnation)));
      const result = accepted(await original.settled);
      expect(f.commands).toHaveLength(1);
      const sent = f.commands[0]!, body = command(sent), id = body.commandId;
      expect(sent.id).not.toBe(id);
      expect(sent.duplicate).toBe(false);
      expect(body).toMatchObject({ ownerIncarnation: ready.incarnation, targetId: TARGET_ID, operation: "followUp" });
      const initialDelivery = await first.receipt(message => message.type === "delivery" && message.commandId === id);
      expect(initialDelivery).toMatchObject({ sourceMachine: "machine-a", verification: "bridge", attempt: 1,
        sequence: sent.sequence, payload: sent.envelope });
      const initialFinal = await first.receipt(message => message.type === "final-ack" && message.commandId === id);
      expect(initialFinal.drain).toEqual({ pending: 0, ackPending: 0 });
      expect((await f.senderAck(sent.id)).payload.body).toMatchObject({ ownerIncarnation: ready.incarnation,
        accepted: true, messageId: result.messageId });
      const stored = f.record(id)!;
      expect(stored).toMatchObject({ carrier: "nats", commandId: id, hostId: OWNER_HOST_ID, targetId: TARGET_ID,
        ownerIncarnation: ready.incarnation, bodyDigest: controlBodyFingerprint(body, sent.envelope.principal),
        acceptance: { accepted: true, messageId: result.messageId, triggered: false } });
      expect(stored.writtenAt).toBeGreaterThan(0);
      const custody = f.journal();
      expect(custody).toMatchObject([{ id: result.messageId, deliveryId: id, inboxEventId: sent.id }]);
      expect(custody).toHaveLength(1);
      expect(f.observations().filter(item => item.type === "handler" && item.commandId === id)).toHaveLength(1);
      expect(original.observation.settlements).toBe(1);
      noConsumption(f);
      const birth = first.ownership();
      expect(birth.pid).toBe(ready.pid);
      expect(birth.started).not.toBe("");
      expect(await first.killAndJoin()).toEqual({ code: null, signal: "SIGKILL" });
      expect(same(birth)).toBe(false);
      expect(first.diagnostics()).toBe("");
      expect(f.record(id)).toEqual(stored);
      expect(f.journal()).toEqual(custody);
      const before = await consumerInfo(f.server);
      expect(before.num_pending).toBe(0);
      expect(before.num_ack_pending).toBe(0);

      const restarted = f.launch();
      const resumed = await restarted.receipt(message => message.type === "ready");
      restarted.ownership();
      expect(resumed).toMatchObject({ ownerHostId: OWNER_HOST_ID, meshRoot: f.paths.mesh, journal: f.paths.journal });
      expect(resumed.incarnation).not.toBe(ready.incarnation);
      f.epoch(resumed.incarnation);
      // A new public invocation captures CURRENT authority; no raw replay or altered digest.
      const retry = track(f.request("e24-stored-success", String(resumed.incarnation)));
      const retryPubAck = await restarted.receipt(message => message.type === "puback" && message.commandId === id);
      // Counterexample-first: pre-E24 source stamps OLD here and fails, without treating
      // the pending request's eventual timeout (or any harness deadline) as product proof.
      expect(retryPubAck.sent).toMatchObject({ duplicate: false, envelope: { body: {
        commandId: id, ownerIncarnation: resumed.incarnation, originalOwnerIncarnation: ready.incarnation,
        bodyDigest: stored.bodyDigest, accepted: true, messageId: result.messageId } } });
      expect(accepted(await retry.settled)).toEqual(result);
      expect(f.commands).toHaveLength(2);
      const retrySent = f.commands[1]!;
      expect(command(retrySent)).toMatchObject({ commandId: id, ownerIncarnation: resumed.incarnation });
      expect(retrySent.id).not.toBe(sent.id);
      expect(retrySent.id).not.toBe(id);
      expect(retrySent.duplicate).toBe(false);
      expect(controlBodyFingerprint(command(retrySent), retrySent.envelope.principal)).toBe(stored.bodyDigest);
      expect(retryPubAck.sent).toMatchObject({ id: `ack:${retrySent.id}` });
      const currentAck = await f.senderAck(retrySent.id);
      expect(currentAck).toMatchObject({ sourceMachine: "machine-b", verification: "bridge",
        payload: { from: { id: OWNER_HOST_ID }, body: { ownerIncarnation: resumed.incarnation,
          originalOwnerIncarnation: ready.incarnation, accepted: true, messageId: result.messageId } } });
      const retryFinal = await restarted.receipt(message => message.type === "final-ack" && message.commandId === id);
      expect(retryFinal.drain).toEqual({ pending: 0, ackPending: 0 });
      expect(f.record(id)).toMatchObject({ ownerIncarnation: ready.incarnation, bodyDigest: stored.bodyDigest,
        acceptance: stored.acceptance! });
      expect(f.journal().filter(item => item.deliveryId === id)).toMatchObject([{ id: result.messageId, inboxEventId: sent.id }]);
      expect(f.journal()).toHaveLength(1);
      expect(f.observations().filter(item => item.type === "handler" && item.commandId === id)).toHaveLength(1);

      const independent = track(f.request("e24-independent", String(resumed.incarnation)));
      const fresh = accepted(await independent.settled);
      expect(f.commands).toHaveLength(3);
      const freshId = command(f.commands[2]!).commandId;
      expect(freshId).not.toBe(id);
      expect(fresh.messageId).not.toBe(result.messageId);
      const freshFinal = await restarted.receipt(message => message.type === "final-ack" && message.commandId === freshId);
      expect(freshFinal.drain).toEqual({ pending: 0, ackPending: 0 });
      expect(f.record(freshId)).toMatchObject({ ownerIncarnation: resumed.incarnation,
        acceptance: { accepted: true, messageId: fresh.messageId } });
      expect(f.journal().filter(item => item.deliveryId === freshId)).toMatchObject([{ id: fresh.messageId }]);
      expect(f.journal()).toHaveLength(2);
      expect(f.observations().filter(item => item.type === "handler")).toHaveLength(2);
      const after = await consumerInfo(f.server);
      expect(after.name).toBe(before.name);
      expect(after.created).toBe(before.created);
      expect(after.num_pending).toBe(0);
      expect(after.num_ack_pending).toBe(0);
      expect(original.observation.settlements).toBe(1);
      expect(retry.observation.settlements).toBe(1);
      expect(independent.observation.settlements).toBe(1);
      expect(f.cancellations).toHaveLength(0);
      expect(f.incoming.every(item => item.sourceMachine === "machine-b")).toBe(true);
      noConsumption(f);
      expect(restarted.diagnostics()).toBe("");
    } finally { await f.close(); }
  }, 180_000);
  it.skipIf(!binary && !required).each([
    ["claim", "T-B2a durable create-only claim before handler"],
    ["custody", "T-B2b actual Main durable custody before outcome"],
    ["ack", "T-B2c ACK-envelope PubAck before final consumer ACK"],
  ] as const)("%s / %s", async (boundary, _description) => {
    if (!binary) throw new Error("NATS_AUTH_REQUIRED=1: missing nats-server; set absolute NATS_BIN");
    const f = await fixture();
    try {
      const first = f.launch(boundary);
      const ready = await first.receipt(message => message.type === "ready");
      expect(ready).toMatchObject({ pid: first.child.pid, ownerHostId: OWNER_HOST_ID, meshRoot: f.paths.mesh,
        journal: f.paths.journal, carrier: "nats", machine: "machine-b", nativeConsumption: false });
      f.epoch(ready.incarnation);
      const observation = new AbortController();
      const original = track(f.request(`native-${boundary}`, String(ready.incarnation), { signal: observation.signal }));
      const fence = await first.receipt(message => message.type === "boundary" && message.point === boundary);
      expect(f.commands).toHaveLength(1);
      const sent = f.commands[0]!, body = command(sent), id = body.commandId;
      expect(sent.id).not.toBe(id); // E14: carrier event id != stable logical command id.
      expect(sent.duplicate).toBe(false);
      expect(body).toMatchObject({ ownerIncarnation: ready.incarnation, targetId: TARGET_ID, operation: "followUp" });
      expect(fence).toMatchObject({ pid: first.child.pid, commandId: id, carrier: "nats" });
      const firstDelivery = await first.receipt(message => message.type === "delivery" && message.commandId === id);
      expect(firstDelivery).toMatchObject({ sourceMachine: "machine-a", verification: "bridge", attempt: 1, sequence: sent.sequence, payload: sent.envelope });
      const stored = f.record(id)!;
      expect(stored).toMatchObject({ carrier: "nats", hostId: OWNER_HOST_ID, commandId: id, targetId: TARGET_ID,
        ownerIncarnation: ready.incarnation, bodyDigest: controlBodyFingerprint(body, sent.envelope.principal) });
      expect(stored.writtenAt).toBeGreaterThan(0);
      const custody = f.journal().filter(item => item.deliveryId === id);
      expect(custody).toHaveLength(boundary === "claim" ? 0 : 1);
      expect(f.observations().filter(item => item.type === "handler" && item.commandId === id)).toHaveLength(boundary === "claim" ? 0 : 1);
      if (boundary === "ack") {
        expect(stored.acceptance).toMatchObject({ accepted: true, messageId: custody[0]!.id });
        expect(fence.sent).toMatchObject({ id: `ack:${sent.id}`, duplicate: false,
          envelope: { body: { ownerIncarnation: ready.incarnation,
            bodyDigest: stored.bodyDigest, accepted: true, messageId: custody[0]!.id } } });
      } else expect(stored.acceptance).toBeUndefined();
      expect(f.observations().filter(item => item.type === "final-ack-call")).toEqual([]);
      expect((await consumerInfo(f.server)).num_ack_pending).toBe(1);
      expect(original.observation.settlements).toBe(0);
      noConsumption(f);
      const birth = first.ownership(); // Independent inherited preload PID + process start identity.
      expect(birth.pid).toBe(fence.pid);
      expect(birth.started).not.toBe("");
      expect(await first.killAndJoin()).toEqual({ code: null, signal: "SIGKILL" });
      expect(same(birth)).toBe(false);
      expect(first.diagnostics()).toBe("");
      expect(f.record(id)).toEqual(stored); // Actual post-death disk readback, not spy-only proof.
      expect(f.journal().filter(item => item.deliveryId === id).map(item => item.id)).toEqual(custody.map(item => item.id));
      const before = await consumerInfo(f.server);
      expect(before.num_ack_pending).toBe(1);

      const restarted = f.launch();
      const resumed = await restarted.receipt(message => message.type === "ready");
      restarted.ownership();
      expect(resumed).toMatchObject({ ownerHostId: OWNER_HOST_ID, meshRoot: f.paths.mesh, journal: f.paths.journal });
      expect(resumed.incarnation).not.toBe(ready.incarnation);
      f.epoch(resumed.incarnation); // All NEW public callers resolve the CURRENT successor.
      // The already pending original attempt still owns its OLD captured epoch (P6 unchanged).
      // No NAK, raw replay, fabricated claim/outcome or shortened broker policy. Native
      // production ack_wait after SIGKILL produces this retained delivery (same bytes/seq).
      const replay = await restarted.receipt(message => message.type === "delivery" && message.commandId === id);
      expect(replay).toMatchObject({ payload: sent.envelope, sequence: sent.sequence, sourceMachine: "machine-a", verification: "bridge" });
      expect(Number(replay.attempt)).toBeGreaterThanOrEqual(2);
      const replayAck = await restarted.receipt(message => message.type === "puback" && message.commandId === id);
      expect(replayAck.sent).toMatchObject({ id: `ack:${sent.id}`, duplicate: boundary === "ack",
        envelope: { from: { id: OWNER_HOST_ID }, to: { machine: "machine-a", hostId: SENDER_HOST_ID },
          body: { commandId: id, ownerIncarnation: resumed.incarnation, originalOwnerIncarnation: ready.incarnation, bodyDigest: stored.bodyDigest,
            ...(boundary === "ack" ? { accepted: true, messageId: custody[0]!.id } : { accepted: false, error: indeterminate }) } } });
      const finalAck = await restarted.receipt(message => message.type === "final-ack" && message.commandId === id);
      expect(finalAck.drain).toEqual({ pending: 0, ackPending: 0 });
      const rebound = await consumerInfo(f.server);
      expect(rebound.name).toBe(before.name);
      expect(rebound.created).toBe(before.created);
      expect(rebound.num_pending).toBe(0);
      expect(rebound.num_ack_pending).toBe(0);
      const replayedRecord = f.record(id)!, replayedJournal = f.journal();
      expect(replayedRecord.ownerIncarnation).toBe(ready.incarnation); // Stored provenance never migrates.
      f.release();
      const delivered = await f.senderAck(sent.id); // P6 has run AND the actual broker double-ACK returned.
      expect(delivered).toMatchObject({ sourceMachine: "machine-b", verification: "bridge",
        payload: { id: `ack:${sent.id}`, from: { id: OWNER_HOST_ID },
          body: { commandId: id, bodyDigest: stored.bodyDigest,
            ownerIncarnation: boundary === "ack" ? ready.incarnation : resumed.incarnation } } });
      let originalSuccess: FabricControlResult | undefined;
      if (boundary === "ack") {
        // Its OLD ACK was really PubAcked before death; the duplicate replay send cannot
        // replace those broker bytes. That valid OLD pending attempt may settle normally.
        originalSuccess = accepted(await original.settled);
        expect(originalSuccess.messageId).toBe(custody[0]!.id);
        expect(f.cancellations).toHaveLength(0);
      } else {
        expect(delivered.payload.body).toMatchObject({ originalOwnerIncarnation: ready.incarnation,
          accepted: false, error: indeterminate });
        expect(original.observation.settlements).toBe(0); // CURRENT replay cannot settle OLD pending.
        observation.abort(); // End observation explicitly, never wait for the 120 s product timeout.
        const cancelled = await original.settled;
        expect(cancelled.ok).toBe(false);
        if (cancelled.ok) throw new Error("Stale pending must remain outcome unknown on cancellation");
        expect(cancelled.error).toMatchObject({ message: `Remote Fabric request cancelled: ${TARGET_ID}` });
        expect(cancelled.error).not.toMatchObject({ notRun: true });
        expect(cancelled.error).not.toMatchObject({ notDelivered: true });
        const cancellation = await f.cancellation(id), cancelBody = command(cancellation);
        expect(cancelBody).toMatchObject({ operation: "cancel", cancelCommandId: id,
          targetId: TARGET_ID, ownerIncarnation: ready.incarnation });
        expect(cancelBody.commandId).not.toBe(id);
        expect(cancellation.id).not.toBe(sent.id);
        const cancelAck = await restarted.receipt(message => message.type === "final-ack" && message.commandId === cancelBody.commandId);
        expect(cancelAck.drain).toEqual({ pending: 0, ackPending: 0 });
        const cancelledDelivery = await f.senderAck(cancellation.id);
        expect(cancelledDelivery.payload.body).toMatchObject({ accepted: false, notRun: true,
          errorCode: "FABRIC_CONTROL_STALE_INCARNATION", ownerIncarnation: resumed.incarnation,
          staleIncarnation: ready.incarnation }); // Refuses the CANCEL, not the original followUp.
        expect(f.record(cancelBody.commandId)).toBeUndefined(); // Old-epoch cancel cannot create a claim.
        expect(f.record(id)).toEqual(replayedRecord); // Nor mutate the original claim/outcome.
        expect(f.journal()).toEqual(replayedJournal);
        expect(f.observations().filter(item => item.type === "handler" && item.commandId === cancelBody.commandId)).toEqual([]);
        expect(f.cancellations).toHaveLength(1);
      }
      expect(original.observation.settlements).toBe(1);

      // PUBLIC same-key retry: CURRENT directory epoch, unchanged semantic digest, new event.
      const after = restarted.receipts.length;
      const retry = track(f.request(`native-${boundary}`, String(resumed.incarnation)));
      const retryAck = await restarted.receipt(message => message.type === "puback" && message.commandId === id, after);
      expect(retryAck.sent).toMatchObject({ duplicate: false, envelope: { body: {
        ownerIncarnation: resumed.incarnation, originalOwnerIncarnation: ready.incarnation, bodyDigest: stored.bodyDigest } } });
      const retried = await retry.settled;
      if (boundary === "ack") expect(accepted(retried)).toEqual(originalSuccess);
      else unknown(retried);
      const retryFinal = await restarted.receipt(message => message.type === "final-ack" && message.commandId === id, after);
      expect(retryFinal.drain).toEqual({ pending: 0, ackPending: 0 });
      expect(f.commands).toHaveLength(2);
      expect(command(f.commands[1]!)).toMatchObject({ commandId: id, ownerIncarnation: resumed.incarnation });
      expect(f.commands[1]!.id).not.toBe(sent.id);
      expect(f.commands[1]!.duplicate).toBe(false);
      expect(retryAck.sent).toMatchObject({ id: `ack:${f.commands[1]!.id}` });
      const currentReply = await f.senderAck(f.commands[1]!.id);
      expect(currentReply).toMatchObject({ sourceMachine: "machine-b", verification: "bridge",
        payload: { body: { commandId: id, ownerIncarnation: resumed.incarnation,
          originalOwnerIncarnation: ready.incarnation, bodyDigest: stored.bodyDigest } } });
      expect(controlBodyFingerprint(command(f.commands[1]!), f.commands[1]!.envelope.principal)).toBe(stored.bodyDigest);
      expect(retry.observation.settlements).toBe(1);
      expect(original.observation.settlements).toBe(1);
      expect(f.record(id)).toMatchObject({ ownerIncarnation: ready.incarnation,
        acceptance: boundary === "ack" ? stored.acceptance! : { accepted: false, error: indeterminate } });
      expect(f.journal().filter(item => item.deliveryId === id).map(item => item.id)).toEqual(custody.map(item => item.id));
      expect(f.observations().filter(item => item.type === "handler" && item.commandId === id)).toHaveLength(boundary === "claim" ? 0 : 1);

      const independent = track(f.request(`native-${boundary}-independent`, String(resumed.incarnation)));
      const fresh = accepted(await independent.settled);
      expect(f.commands).toHaveLength(3);
      const freshId = command(f.commands[2]!).commandId;
      expect(freshId).not.toBe(id);
      const freshFinal = await restarted.receipt(message => message.type === "final-ack" && message.commandId === freshId);
      expect(freshFinal.drain).toEqual({ pending: 0, ackPending: 0 });
      expect(f.record(freshId)?.acceptance).toMatchObject({ accepted: true, messageId: fresh.messageId });
      expect(f.journal().filter(item => item.deliveryId === freshId)).toMatchObject([{ id: fresh.messageId }]);
      expect(f.journal()).toHaveLength(boundary === "claim" ? 1 : 2);
      expect(independent.observation.settlements).toBe(1);
      expect(original.observation.settlements).toBe(1);
      expect(retry.observation.settlements).toBe(1);
      expect(f.incoming.every(item => item.sourceMachine === "machine-b")).toBe(true);
      noConsumption(f);
      expect(restarted.diagnostics()).toBe("");
    } finally { await f.close(); }
  }, 240_000);
});
