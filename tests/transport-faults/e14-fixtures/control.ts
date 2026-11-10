// Keep reason: E14 data safety and authenticated ACK trust boundaries.
// Scaffolding only: claims, outcomes, digest admission and settlement belong to the REAL CP.
// Broker cases MUST use Path B runtime JWTs. Bootstrap is closed after provisioning.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { jetstream, type JetStreamClient } from "@nats-io/jetstream";
import { connect, credsAuthenticator, type NatsConnection } from "@nats-io/transport-node";
import { expect, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../../../src/config.js";
import { MeshStore, type MeshEvent } from "../../../src/mesh/store.js";
import {
  FabricControlPlane, type FabricControlPlaneOptions, type FabricControlHandler,
  type FabricControlAcceptance, type FabricControlCommand,
} from "../../../src/topology/control-plane.js";
import { MeshConsumptionPausedError } from "../../../src/topology/mesh-consumption.js";
import { openFabricTransport } from "../../../src/transport/index.js";
import type {
  FabricTransport, TransportEnvelope, TransportDelivery, TransportSent,
} from "../../../src/transport/types.js";
import { ensureTransportStream } from "../../../src/transport/nats/provision.js";
import { openNatsKv } from "../../../src/transport/nats/kv.js";
import {
  ADVISORY_CAPTURE_STREAM, transportAdvisoryStreamSubject, transportInboxStream,
  transportInboxStreamSubject, transportInboxSubject,
} from "../../../src/transport/nats/subjects.js";
import { createPathBHarness, openJetStream, type PathBHarness, type PathBUser } from "../../helpers/nats-cluster.js";
import {
  createStubBus, identity, commandEnvelope, type StubTransport, type ResolvedEnvelope,
} from "../../helpers/transport-harness.js";
import { Routes, Receipts, objectBody, persistedBoundary, seenKey, seenRoot } from "../control-fixture.js";

export { Receipts, objectBody, seenKey, seenRoot };
export const HOUR = 60 * 60_000;
export const HOLD_MS = 48 * HOUR;
export const OPERATOR_MACHINE = "fabric-ops";
export const senderOwner = { machine: "machine-a", hostId: "session:e14-sender" };
export const receiverOwner = { machine: "machine-b", hostId: "session:e14-owner" };
export const ownerEpoch = "e14-owner-epoch";
export const senderEpoch = "e14-sender-epoch";
export type E14Options = FabricControlPlaneOptions & {
  relayOperatorMachine?: string;
  withdrawHoldUntil?: number;
};
export interface SeenRecord {
  format: 1; hostId: string; commandId: string; targetId: string; expiresAt: number;
  carrier?: "nats"; sequence?: number; bodyDigest?: string; ownerIncarnation?: string;
  acceptance?: FabricControlAcceptance;
  writtenAt?: number;
}
export function blocked(condition: unknown, dependency: string): asserts condition {
  if (!condition) throw new Error(`BLOCKED-ON-SOURCE: ${dependency}`);
}
export function noDefiniteNonDelivery(error: unknown): void {
  expect(error).toBeInstanceOf(Error);
  expect(error).not.toHaveProperty("notRun", true);
  expect(error).not.toHaveProperty("notDelivered", true);
  expect(error).not.toHaveProperty("accepted", true);
  expect(error).not.toHaveProperty("code", "FABRIC_CONTROL_NOT_DELIVERED");
}
export function seenStore(root: string, hostId = receiverOwner.hostId): MeshStore {
  return new MeshStore(seenRoot(root, hostId), 1_048_576, 1_000,
    { stateBackend: "file", lockProtocol: 1, canWakeResidents: () => false });
}
export function readSeen(root: string, commandId: string): SeenRecord | undefined {
  const store = seenStore(root);
  try { return store.get(seenKey(receiverOwner.hostId, commandId), { fresh: true })?.value as SeenRecord | undefined; }
  finally { store.closeState(); }
}
export function observe(raw: FabricTransport) {
  const sent = new Receipts<TransportSent>();
  const delivered = new Receipts<TransportDelivery<TransportEnvelope>>();
  const acked = new Receipts<TransportDelivery<TransportEnvelope>>();
  const transport: FabricTransport = {
    kind: raw.kind, self: raw.self,
    send: async <T>(input: TransportEnvelope<T>, signal?: AbortSignal): Promise<TransportSent<T>> => {
      const receipt = await raw.send(input, signal); sent.emit(receipt); return receipt;
    },
    inbox: async function* (signal) {
      for await (const delivery of raw.inbox(signal)) {
        delivered.emit(delivery);
        yield { ...delivery, ack: async () => { await delivery.ack(); acked.emit(delivery); } };
      }
    },
    kv: bucket => raw.kv(bucket), drainState: signal => raw.drainState(signal), close: () => raw.close(),
  };
  return { raw, transport, sent, delivered, acked };
}
export type Carrier = ReturnType<typeof observe>;

/** No local retry/dedupe/ACK dispatcher: observation wraps the frozen port only. */
export class ControlScene {
  readonly routes = new Routes();
  readonly admissions = new Receipts<FabricControlCommand>();
  readonly senderCarrier: Carrier;
  readonly ownerCarrier: Carrier;
  readonly sender: FabricControlPlane;
  readonly owner: FabricControlPlane;
  readonly senderMesh: MeshStore;
  readonly ownerMesh: MeshStore;
  readonly releases: Array<() => void> = [];
  private retired = false;
  constructor(readonly root: string, sender: FabricTransport, receiver: FabricTransport) {
    this.senderCarrier = observe(sender); this.ownerCarrier = observe(receiver);
    this.senderMesh = new MeshStore(path.join(root, "sender"), 1_048_576, 1_000, { stateBackend: "file", canWakeResidents: () => false });
    this.ownerMesh = new MeshStore(path.join(root, "owner"), 1_048_576, 1_000, { stateBackend: "file", canWakeResidents: () => false });
    for (const [self, epoch] of [[senderOwner, senderEpoch], [receiverOwner, ownerEpoch]] as const) {
      this.routes.set(self.hostId, { machine: self.machine, ownerHostId: self.hostId, ownerIdentityId: self.hostId,
        ownerIncarnation: epoch, capabilities: ["followUp", "steer", "stop"], live: true });
    }
    const make = (mesh: MeshStore, carrier: Carrier, epoch: string): FabricControlPlane => {
      const options: E14Options = { enabled: true, hostId: carrier.raw.self.hostId, ownerIncarnation: epoch,
        transport: carrier.transport, participants: this.routes, controlIncarnationFence: "enforce", pollMs: 20,
        acknowledgementTimeoutMs: 30_000, relayOperatorMachine: OPERATOR_MACHINE };
      const plane = new FabricControlPlane(mesh, identity(carrier.raw.self.hostId), options);
      blocked(plane.transportKind === "nats", "A1 public transportKind must select the injected NATS carrier");
      return plane;
    };
    this.sender = make(this.senderMesh, this.senderCarrier, senderEpoch);
    this.owner = make(this.ownerMesh, this.ownerCarrier, ownerEpoch);
    this.sender.start(() => ({ accepted: false }));
  }
  start(handler: FabricControlHandler = command => ({ accepted: true, messageId: `stored:${command.commandId}` })): void {
    this.owner.start((...args) => { this.admissions.emit(args[0]); return handler(...args); });
  }
  request(key: string, message = "E14 immutable input", timeoutMs = 30_000) {
    return this.sender.request(receiverOwner.hostId, receiverOwner.hostId, "followUp",
      { ownerIncarnation: ownerEpoch, message }, receiverOwner.hostId, { idempotencyKey: key, timeoutMs });
  }
  command(count = 1) {
    return this.senderCarrier.sent.wait(sent => sent.envelope.kind === "command" && sent.envelope.operation === "followUp", "request PubAck", count);
  }
  async consumed(id: string, carrier = this.ownerCarrier, count = 1): Promise<void> {
    await carrier.acked.wait(delivery => delivery.payload.id === id, `confirmed broker ACK ${id}`, count);
  }
  ack(eventId: string) {
    return this.ownerCarrier.sent.wait(sent => sent.id === `ack:${eventId}`, `owner ACK PubAck ${eventId}`);
  }
  seen(id: string) { return readSeen(this.ownerMesh.root, id); }
  rawCommand(id = randomUUID()): TransportEnvelope {
    return commandEnvelope({ id, to: receiverOwner, from: identity(senderOwner.hostId),
      targetId: receiverOwner.hostId, ownerIncarnation: ownerEpoch, replyTo: senderOwner.hostId,
      message: "E14 immutable input", timeoutMs: 30_000 });
  }
  async claimWithoutOutcome(): Promise<SeenRecord> {
    const id = randomUUID(), committed = new Receipts<SeenRecord>();
    const restore = persistedBoundary(seenRoot(this.ownerMesh.root, receiverOwner.hostId), (phase, value) => {
      if (phase === "after" && value.commandId === id && !value.acceptance) {
        this.owner.pause(); committed.emit(value); throw new MeshConsumptionPausedError();
      }
    });
    this.releases.push(restore);
    await this.senderCarrier.transport.send(this.rawCommand(id));
    await committed.wait(record => record.commandId === id, "real create-only NATS claim");
    await this.owner.close(); restore();
    // Retire the test-owned interrupted input with a confirmed broker ACK after
    // CP closure, without manufacturing an outcome or relay disposition.
    const delivery = await this.ownerCarrier.delivered.wait(item => item.payload.id === id, "interrupted claim delivery");
    await delivery.ack();
    expect(await this.ownerCarrier.transport.drainState()).toEqual({ pending: 0, ackPending: 0 });
    const record = this.seen(id)!;
    blocked(record.carrier === "nats", "A1 E14.3 must mark NATS claims carrier:nats");
    blocked(typeof record.writtenAt === "number" && Number.isFinite(record.writtenAt),
      "A1 E17 NATS claim write must carry finite writtenAt");
    expect(record).not.toHaveProperty("acceptance");
    return record;
  }
  async close(): Promise<void> { await this.retire(); }
  /** Retire runtime/carriers but KEEP broker-owned root/seen evidence for the FILE flip. */
  async retire(): Promise<void> {
    if (this.retired) return;
    this.retired = true;
    for (const release of this.releases.splice(0)) release();
    this.sender.pause(); this.owner.pause();
    try { await Promise.all([this.sender.close(), this.owner.close()]); }
    finally {
      await Promise.allSettled([this.senderCarrier.raw.close(), this.ownerCarrier.raw.close()]);
      this.senderMesh.closeState(); this.ownerMesh.closeState();
    }
  }
}

export interface OperatorUser { machine: string; publicKey: string; credsPath: string }
/** Observation sockets use the same DNS identity and CA as verified runtimes. */
async function brokerConnection(broker: PathBHarness, user: PathBUser): Promise<NatsConnection> {
  const options = broker.runtimeOptions(user);
  blocked(broker.tls && path.isAbsolute(broker.tls.caFile), "B TLS fixture requires an absolute CA path");
  return connect({ servers: options.servers, tls: { caFile: broker.tls.caFile },
    authenticator: credsAuthenticator(Uint8Array.from(fs.readFileSync(user.credsPath))),
    inboxPrefix: `_INBOX.${user.publicKey}`, reconnect: false, timeout: 2_500 });
}
export class BrokerScene extends ControlScene {
  private constructor(root: string, sender: FabricTransport, owner: FabricTransport,
    readonly broker: PathBHarness, readonly runtimeA: NatsConnection, readonly runtimeB: NatsConnection) {
    super(root, sender, owner);
  }
  static async open(): Promise<BrokerScene> {
    // No skipIf or environment opt-out: missing binary is a hard prerequisite failure.
    let broker: PathBHarness;
    try { broker = await createPathBHarness({ tls: { dnsNames: ["localhost"], ipAddresses: [] } }); }
    catch (cause) { throw new Error("BLOCKED-ON-SOURCE/PREREQUISITE: real Path B JWT broker is mandatory for E14", { cause }); }
    const opened: FabricTransport[] = [], sockets: NatsConnection[] = [];
    try {
      expect(broker.users.machineA.machine).toBe("machine-a"); expect(broker.users.machineB.machine).toBe("machine-b");
      expect(broker.users.machineA.publicKey).not.toBe(broker.users.machineB.publicKey);
      expect(broker.users.machineA.publicKey).not.toBe(broker.users.bootstrap.publicKey);
      expect(broker.users.machineB.publicKey).not.toBe(broker.users.bootstrap.publicKey);
      const bootstrap = await brokerConnection(broker, broker.users.bootstrap);
      try {
        const { manager, js } = await openJetStream(bootstrap);
        const policy = (name: string, subjects: string[], maxAgeMs = 0, maxMessageBytes = 1_048_576) => ({
          name, subjects, maxBytes: 1_073_741_824, maxAgeMs, maxMessageBytes,
          duplicateWindowMs: 10 * 60_000, replicas: 1,
        });
        await ensureTransportStream(manager, policy(ADVISORY_CAPTURE_STREAM, [transportAdvisoryStreamSubject], 30 * 24 * HOUR, 64 * 1024));
        await openNatsKv(js, { bucket: "fabric_leases", ttlMs: 15_000, markerTtlMs: 5 * 60_000 });
        await openNatsKv(js, { bucket: "fabric_presence", ttlMs: 10 * 60_000, markerTtlMs: 5 * 60_000 });
        for (const machine of ["machine-a", "machine-b"]) {
          await ensureTransportStream(manager, policy(transportInboxStream(machine), [transportInboxStreamSubject(machine)]));
        }
      } finally { await bootstrap.close(); }
      // Factory path, never direct openNatsTransport(admin)/verifyMachine:false.
      const root = path.join(broker.root, "control");
      for (const [self, user] of [[senderOwner, broker.users.machineA], [receiverOwner, broker.users.machineB]] as const) {
        const mesh = new MeshStore(path.join(root, `factory-${self.machine}`), 1_048_576, 1_000, { stateBackend: "file", canWakeResidents: () => false });
        try {
          const runtime = broker.runtimeOptions(user);
          expect(new URL(runtime.servers).hostname).toBe("localhost");
          const config = { ...DEFAULT_FABRIC_CONFIG.mesh, transport: "nats" as const, hostTransport: "nats" as const,
            nats: { ...DEFAULT_FABRIC_CONFIG.mesh.nats, ...runtime, servers: [runtime.servers],
              operatorMachine: OPERATOR_MACHINE } };
          opened.push(await openFabricTransport({ config, mesh, hostId: self.hostId }));
        } finally { mesh.closeState(); }
      }
      const runtimeA = await brokerConnection(broker, broker.users.machineA); sockets.push(runtimeA);
      const runtimeB = await brokerConnection(broker, broker.users.machineB); sockets.push(runtimeB);
      return new BrokerScene(root, opened[0]!, opened[1]!, broker, runtimeA, runtimeB);
    } catch (error) {
      await Promise.allSettled([...opened.map(port => port.close()), ...sockets.map(nc => nc.close())]);
      await broker.stop(); throw error;
    }
  }
  async streamState(machine = receiverOwner.machine) {
    const nc = machine === senderOwner.machine ? this.runtimeA : this.runtimeB;
    return (await (await openJetStream(nc)).manager.streams.info(transportInboxStream(machine))).state;
  }
  operatorUser(): OperatorUser {
    // B-owned public fixture dependency. Never turn the old machine-a maintainer or bootstrap
    // into an operator, and never mint a test-local authority that masks B's missing contract.
    const user = (Reflect.get(this.broker.users, "operator") ?? this.broker.users.maintainer) as OperatorUser | undefined;
    blocked(user && user.machine === OPERATOR_MACHINE && user.publicKey !== this.broker.users.bootstrap.publicKey &&
      user.publicKey !== this.broker.users.machineA.publicKey && user.publicKey !== this.broker.users.machineB.publicKey,
    "B public Path B operator/maintainer fixture must expose a distinct fabric-ops JWT credential outside PATH_B_MACHINES (E14.2/E12/E16b)");
    return user;
  }
  /** Exact resolved wire bytes, including createdAt. send() would restamp createdAt and
   * could only prove same-ID suppression, NOT byte-identical uncertain-PubAck republish. */
  async republishExact(sent: TransportSent) {
    const envelope = sent.envelope;
    expect(typeof envelope.body).not.toBe("function");
    const subject = transportInboxSubject(envelope.to.machine, envelope.to.hostId,
      envelope.kind === "command" ? "cmd" : "ack", envelope.targetId, senderOwner.machine);
    return jetstream(this.runtimeA).publish(subject,
      Buffer.from(JSON.stringify({ format: 1, sourceMachine: senderOwner.machine, envelope })),
      { msgID: sent.id, expect: { streamName: transportInboxStream(envelope.to.machine) }, timeout: 2_500 });
  }
  async publishAck(envelope: TransportEnvelope, source: "operator" | "machine-a" | "machine-b"): Promise<void> {
    const user = source === "operator" ? this.operatorUser() : source === "machine-a" ? this.broker.users.machineA : this.broker.users.machineB;
    const nc = await brokerConnection(this.broker, user as PathBUser);
    try { await publishWire(jetstream(nc), envelope, user.machine); }
    finally { await nc.close(); }
  }
  override async close(): Promise<void> {
    try { await super.close(); }
    finally {
      try { await Promise.all([this.runtimeA.close(), this.runtimeB.close()]); }
      finally { await this.broker.stop(); }
    }
  }
}
async function publishWire(js: JetStreamClient, envelope: TransportEnvelope, sourceMachine: string): Promise<void> {
  expect(typeof envelope.body).not.toBe("function");
  const subject = transportInboxSubject(envelope.to.machine, envelope.to.hostId, "ack", envelope.targetId, sourceMachine);
  await js.publish(subject, Buffer.from(JSON.stringify({ format: 1, sourceMachine,
    envelope: { ...envelope, createdAt: Date.now() } })),
  { msgID: envelope.id, expect: { streamName: transportInboxStream(envelope.to.machine) }, timeout: 2_500 });
}
export function relay(command: TransportSent, patch: Record<string, unknown> = {}): TransportEnvelope {
  const body = objectBody(command.envelope);
  return { id: `relay:${command.id}`, kind: "ack", to: senderOwner, targetId: receiverOwner.hostId,
    operation: patch.accepted === true ? "accepted" : "rejected", from: identity("messaging-operator"), body: {
      version: 1, commandId: body.commandId, commandEventId: command.id, targetId: body.targetId,
      accepted: false, error: "Relay destination unavailable", errorCode: "FABRIC_RELAY_DESTINATION_UNAVAILABLE", retryable: true,
      ...patch,
    } };
}
export function ownerAck(command: TransportSent, patch: Record<string, unknown> = {}): TransportEnvelope {
  const body = objectBody(command.envelope);
  return { id: `ack:${command.id}`, kind: "ack", to: senderOwner, targetId: receiverOwner.hostId,
    operation: patch.accepted === false ? "rejected" : "accepted", from: identity(receiverOwner.hostId), body: {
      version: 1, commandId: body.commandId, targetId: body.targetId, ownerIncarnation: ownerEpoch,
      accepted: true, messageId: "forged-owner-outcome", ...patch,
    } };
}
export function stubScene(): { scene: ControlScene; inbox: StubTransport } {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "e14-unit-trust-")), bus = createStubBus(() => Date.now());
  const inbox = bus.open(senderOwner);
  return { scene: new ControlScene(root, inbox, bus.open(receiverOwner)), inbox };
}
export function injectUnitAck(inbox: StubTransport, envelope: TransportEnvelope, sourceMachine: string): void {
  inbox.enqueue({ ...envelope, createdAt: Date.now() } as ResolvedEnvelope, { sourceMachine, verification: "bridge" });
}

/** Actual FILE carrier, same host root, actual public withdraw/release/cleanup paths. */
export class FileScene {
  readonly root: string;
  readonly mesh: MeshStore;
  readonly sender: FabricControlPlane;
  readonly owner: FabricControlPlane;
  readonly events = new Receipts<MeshEvent>();
  readonly deleted = new Receipts<string>();
  readonly admissions = new Receipts<FabricControlCommand>();
  private readonly restores: Array<() => void> = [];
  private closed = false;
  constructor(root?: string, readonly withdrawHoldUntil?: number) {
    this.root = root ?? fs.mkdtempSync(path.join(os.tmpdir(), "e14-file-"));
    this.mesh = new MeshStore(this.root, 1_048_576, 1_000, { stateBackend: "file", canWakeResidents: () => false });
    const options = (hostId: string, ownerIncarnation: string): E14Options => ({
      enabled: true, hostId, ownerIncarnation, controlIncarnationFence: "enforce", pollMs: 20, acknowledgementTimeoutMs: 100,
      expectedNatsInboxStream: transportInboxStream(hostId === receiverOwner.hostId ? receiverOwner.machine : senderOwner.machine),
      ...(withdrawHoldUntil === undefined ? {} : { withdrawHoldUntil }),
    });
    this.sender = new FabricControlPlane(this.mesh, identity(senderOwner.hostId), options(senderOwner.hostId, senderEpoch));
    this.owner = new FabricControlPlane(this.mesh, identity(receiverOwner.hostId), options(receiverOwner.hostId, ownerEpoch));
    const events = this.events, deleted = this.deleted, localRoot = seenRoot(this.root, receiverOwner.hostId), meshRoot = this.root;
    const publish = MeshStore.prototype.publish;
    const publishSpy = vi.spyOn(MeshStore.prototype, "publish").mockImplementation(async function(this: MeshStore, input) {
      const event = await publish.call(this, input); if (this.root === meshRoot) events.emit(event); return event;
    });
    const batch = MeshStore.prototype.writeBatch;
    const batchSpy = vi.spyOn(MeshStore.prototype, "writeBatch").mockImplementation(async function(this: MeshStore, input) {
      const result = await batch.call(this, input);
      if (this.root === localRoot) for (const op of input.ops) if (op.kind === "delete" && !this.get(op.key, { fresh: true })) deleted.emit(op.key);
      return result;
    });
    this.restores.push(() => publishSpy.mockRestore(), () => batchSpy.mockRestore());
    this.sender.start(() => ({ accepted: false }));
  }
  start(): void {
    this.owner.start(command => { this.admissions.emit(command); return { accepted: true, messageId: `file:${command.commandId}` }; });
  }
  request(key: string, message = "E14 immutable input") {
    return this.sender.request(receiverOwner.hostId, receiverOwner.hostId, "followUp", { message, ownerIncarnation: ownerEpoch },
      receiverOwner.hostId, { idempotencyKey: key, timeoutMs: 100 });
  }
  command(count = 1) {
    return this.events.wait(event => event.topic === "fabric.control.command" && event.kind === "followUp", "FILE append", count);
  }
  async readbackPlane(): Promise<FabricControlPlane> {
    this.owner.pause(); await this.owner.close();
    const plane = new FabricControlPlane(this.mesh, identity(receiverOwner.hostId), { enabled: true, hostId: receiverOwner.hostId,
      ownerIncarnation: ownerEpoch, acknowledgementTimeoutMs: 100, pollMs: 20,
      expectedNatsInboxStream: transportInboxStream(receiverOwner.machine) });
    plane.pause(); return plane;
  }
  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    this.owner.pause(); this.sender.pause();
    try { await Promise.all([this.owner.close(), this.sender.close()]); }
    finally { for (const restore of this.restores.reverse()) restore(); this.mesh.closeState(); }
  }
}
/** One atomic host-config replacement contains BOTH the carrier flip and its 48 h hold.
 * This is the CP rollback composition slice, not A2's installed-host switch/config-parser proof. */
export function writeRollbackFlip(root: string, flipAt = Date.now()): { file: string; holdUntil: number } {
  const file = path.join(root, "e14-host-config.json"), temporary = `${file}.next`;
  const document = { mesh: { transport: "file", nats: { machine: receiverOwner.machine,
    rollbackHoldUntil: new Date(flipAt + HOLD_MS).toISOString() } } };
  fs.writeFileSync(temporary, JSON.stringify(document), { mode: 0o600 }); fs.renameSync(temporary, file);
  const saved = JSON.parse(fs.readFileSync(file, "utf8")) as typeof document;
  expect(saved.mesh.transport).toBe("file"); expect(saved.mesh.nats.machine).toBe(receiverOwner.machine);
  return { file, holdUntil: Date.parse(saved.mesh.nats.rollbackHoldUntil) };
}
