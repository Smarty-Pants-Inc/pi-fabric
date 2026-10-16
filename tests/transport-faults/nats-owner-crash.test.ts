// expected-skip: missing nats-server binary: optional real JWT broker prerequisite (NATS_BIN).
// NATS_AUTH_REQUIRED=1 makes missing binary FAIL; invalid explicit NATS_BIN/import/runtime errors never skip.
// expected-skip: win32: POSIX SIGKILL and real file namespace durability barriers required.
// Keep reason: data safety; OS death between actual CP claim, Main custody, ACK PubAck and broker ACK.
// B2 ONLY. Manual known ParticipantView, NOT production roots/leases, B3 consumption, power-cut or HA proof.
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { jetstream, jetstreamManager } from "@nats-io/jetstream";
import { describe, expect, it } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../../src/config.js";
import { MeshStore } from "../../src/mesh/store.js";
import { controlBodyFingerprint } from "../../src/mesh/control-idempotency.js";
import { FabricControlPlane, type FabricControlCommand, type FabricControlAcceptance, type FabricControlResult } from "../../src/topology/control-plane.js";
import { openFabricTransport } from "../../src/transport/index.js";
import { openNatsConnection } from "../../src/transport/nats/connection.js";
import { openNatsKvForMachine } from "../../src/transport/nats/kv.js";
import { ensureTransportStream } from "../../src/transport/nats/provision.js";
import { ADVISORY_CAPTURE_STREAM, transportAdvisoryStreamSubject, transportDurableName, transportInboxStream, transportInboxStreamSubject } from "../../src/transport/nats/subjects.js";
import type { FabricTransport, TransportSent } from "../../src/transport/types.js";
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
  const server = await createPathBHarness({ binary: binary! });
  const root = path.join(server.root, "native-owner");
  const paths = fixturePaths(root);
  const children: OwnedNatsOwner[] = [];
  const meshes: MeshStore[] = [];
  let carrier: FabricTransport | undefined, sender: FabricControlPlane | undefined;
  const commands: TransportSent[] = [];
  const incoming: Array<{ sourceMachine: string; body: unknown }> = [];
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
    const bootstrap = await server.connect(server.users.bootstrap);
    try {
      const manager = await jetstreamManager(bootstrap), js = jetstream(bootstrap);
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
    expect(bootstrap.isClosed()).toBe(true); // Setup authority closed BEFORE opening either runtime.
    expect(server.configText).toContain("sync_interval: always");
    // E12: signed JWT machine tag + server-authenticated subject + expanded exact source grant.
    for (const user of [server.users.machineA, server.users.machineB]) {
      const connection = await openNatsConnection({ servers: server.serverUrl, credentials: user.credsPath, machine: user.machine });
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
      nats: { ...DEFAULT_FABRIC_CONFIG.mesh.nats, servers: [server.serverUrl], machine: "machine-a",
        credentials: server.users.machineA.credsPath, operatorMachine: "fabric-ops" } };
    carrier = await openFabricTransport({ config, mesh: senderMesh, hostId: SENDER_HOST_ID });
    const real = carrier;
    expect(real.self).toEqual({ machine: "machine-a", hostId: SENDER_HOST_ID });
    const observed: FabricTransport = {
      kind: real.kind, self: real.self,
      send: async (envelope, signal) => {
        const sent = await real.send(envelope, signal);
        if (envelope.kind === "command" && envelope.operation === "followUp") commands.push(sent);
        return sent;
      },
      inbox: async function* (signal) {
        for await (const delivery of real.inbox(signal)) {
          incoming.push({ sourceMachine: delivery.sourceMachine, body: delivery.payload.body });
          // A receive scheduling gate, NOT sender.pause(): NATS pause would refuse sends.
          // This holds the real ACK before CP settlement; it neither forges nor consumes it.
          await intakeGate;
          if (signal.aborted) break;
          yield delivery;
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
      launch: (boundary?: CrashBoundary) => {
        const child = new OwnedNatsOwner(repository, root, children.length, server.serverUrl, server.users.machineB.credsPath, boundary);
        children.push(child); return child;
      },
      epoch: (value: unknown) => { ownerEpoch = String(value); },
      request: (key: string, incarnation: string) => sender!.request(OWNER_HOST_ID, TARGET_ID, "followUp",
        { ownerIncarnation: incarnation, message: "retain this real Main followUp" }, OWNER_HOST_ID, { idempotencyKey: key }),
      record: (id: string): Seen | undefined => seen.get(seenKey(id), { fresh: true })?.value as Seen | undefined,
      journal: (): Item[] => fs.existsSync(paths.journal)
        ? (JSON.parse(fs.readFileSync(paths.journal, "utf8")) as { items: Item[] }).items : [],
      observations: (): Observation[] => fs.existsSync(paths.observations)
        ? fs.readFileSync(paths.observations, "utf8").trim().split("\n").filter(Boolean).map(line => JSON.parse(line) as Observation) : [],
    };
  } catch (error) { await close(); throw error; }
}
async function consumerInfo(server: PathBHarness) {
  const runtime = await server.connect(server.users.machineB);
  try {
    const manager = await jetstreamManager(runtime);
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
describe.skipIf(process.platform === "win32")("native NATS owner OS death / frozen T-B2a,b,c", () => {
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
      const original = track(f.request(`native-${boundary}`, String(ready.incarnation)));
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
          envelope: { body: { bodyDigest: stored.bodyDigest, accepted: true, messageId: custody[0]!.id } } });
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
      // Retain the ORIGINAL request's route/epoch snapshot for its unchanged same-key retry.
      // This manual view deliberately models retained retry authority, not live directory re-resolution.
      // No NAK, raw replay, fabricated claim/outcome or shortened broker policy. Native
      // production ack_wait after SIGKILL produces this retained delivery (same bytes/seq).
      const replay = await restarted.receipt(message => message.type === "delivery" && message.commandId === id);
      expect(replay).toMatchObject({ payload: sent.envelope, sequence: sent.sequence, sourceMachine: "machine-a", verification: "bridge" });
      expect(Number(replay.attempt)).toBeGreaterThanOrEqual(2);
      const replayAck = await restarted.receipt(message => message.type === "puback" && message.commandId === id);
      expect(replayAck.sent).toMatchObject({ id: `ack:${sent.id}`, duplicate: boundary === "ack",
        envelope: { from: { id: OWNER_HOST_ID }, to: { machine: "machine-a", hostId: SENDER_HOST_ID },
          body: { commandId: id, ownerIncarnation: ready.incarnation, bodyDigest: stored.bodyDigest,
            ...(boundary === "ack" ? { accepted: true, messageId: custody[0]!.id } : { accepted: false, error: indeterminate }) } } });
      const finalAck = await restarted.receipt(message => message.type === "final-ack" && message.commandId === id);
      expect(finalAck.drain).toEqual({ pending: 0, ackPending: 0 });
      const rebound = await consumerInfo(f.server);
      expect(rebound.name).toBe(before.name);
      expect(rebound.created).toBe(before.created);
      expect(rebound.num_pending).toBe(0);
      expect(rebound.num_ack_pending).toBe(0);
      f.release(); // Original real sender only settles after exact process death/close + replay.
      const result = await original.settled;
      if (boundary === "ack") expect(accepted(result).messageId).toBe(custody[0]!.id);
      else unknown(result);
      expect(original.observation.settlements).toBe(1);

      // PUBLIC unchanged same-key retry: new carrier event, same logical key, stored result.
      // Exact original input/epoch: a newly resolved epoch would be a changed attempt and
      // cannot accept an ACK attesting the old epoch (actual A1 sender verifier).
      const after = restarted.receipts.length;
      const retry = track(f.request(`native-${boundary}`, String(ready.incarnation)));
      const retried = await retry.settled;
      if (boundary === "ack") expect(accepted(retried)).toEqual(accepted(result));
      else unknown(retried);
      await restarted.receipt(message => message.type === "final-ack" && message.commandId === id, after);
      expect(f.commands).toHaveLength(2);
      expect(command(f.commands[1]!).commandId).toBe(id);
      expect(f.commands[1]!.id).not.toBe(sent.id);
      expect(controlBodyFingerprint(command(f.commands[1]!), f.commands[1]!.envelope.principal)).toBe(stored.bodyDigest);
      expect(retry.observation.settlements).toBe(1);
      expect(original.observation.settlements).toBe(1);
      expect(f.record(id)?.acceptance).toMatchObject(boundary === "ack" ? stored.acceptance! : { accepted: false, error: indeterminate });
      expect(f.journal().filter(item => item.deliveryId === id).map(item => item.id)).toEqual(custody.map(item => item.id));
      expect(f.observations().filter(item => item.type === "handler" && item.commandId === id)).toHaveLength(boundary === "claim" ? 0 : 1);

      f.epoch(resumed.incarnation); // Only the independent key resolves the new activation.
      const independent = track(f.request(`native-${boundary}-independent`, String(resumed.incarnation)));
      const fresh = accepted(await independent.settled);
      expect(f.commands).toHaveLength(3);
      const freshId = command(f.commands[2]!).commandId;
      expect(freshId).not.toBe(id);
      await restarted.receipt(message => message.type === "final-ack" && message.commandId === freshId);
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
