// Keep reason: data safety at real control-seen/native-custody boundaries and ACK trust boundaries.
// This is a source integration fixture, NOT hosted E2E, a broker, a lease engine, or SIGKILL evidence.
import { AsyncLocalStorage } from "node:async_hooks";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { setTimeout as safetyTimeout, clearTimeout as clearSafetyTimeout } from "node:timers";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { expect, vi } from "vitest";
import { MainAgentController, type HeldAgentMessage } from "../../src/main-agent.js";
import { MeshStore, type MeshIdentity } from "../../src/mesh/store.js";
import { MeshLock } from "../../src/mesh/mesh-lock.js";
import {
  FabricControlPlane, type FabricControlAcceptance, type FabricControlCommand,
  type FabricControlHandler, type FabricControlPlaneOptions,
} from "../../src/topology/control-plane.js";
import type { ParticipantRoute, ParticipantView } from "../../src/topology/participant-view.js";
import type {
  FabricTransport, TransportEnvelope, TransportSent, TransportOwner,
} from "../../src/transport/types.js";
import {
  commandEnvelope, createStubBus, identity, owner, resolveEnvelope,
  type ResolvedEnvelope, type StubTransport,
} from "../helpers/transport-harness.js";

export const A1_REASON = "A1 dependency: FabricControlPlane has no public transportKind getter; frozen port types alone do not integrate the carrier";
function publicGetter(name: string): boolean {
  for (let prototype: object | null = FabricControlPlane.prototype; prototype; prototype = Object.getPrototypeOf(prototype)) {
    const descriptor = Object.getOwnPropertyDescriptor(prototype, name);
    if (descriptor) return typeof descriptor.get === "function";
  }
  return false;
}
export const transportIntegrated = publicGetter("transportKind");
export const transportRequired = process.env.FABRIC_TRANSPORT_REQUIRED === "1";
// Canonical process-run ids, not synthetic session:<taskId> surrogate addresses.
export const laneTargetA = "a".repeat(32);
export const laneTargetB = "b".repeat(32);
export const actorTargetA = "d".repeat(32);
export const actorTargetB = "e".repeat(32);
export function assertA1(): void { expect(transportIntegrated, A1_REASON).toBe(true); }
export function carrierKind(plane: FabricControlPlane): unknown { return Reflect.get(plane, "transportKind"); }

/** Event-source observation with a finite REAL deadline even when the scenario advances fake time.
 * No polling, timer-driven stimulus, hidden retries, or unbounded unresolved event waits. */
export class Receipts<T> {
  readonly items: T[] = [];
  private readonly listeners = new Set<() => void>();
  emit(value: T): void { this.items.push(value); for (const notify of [...this.listeners]) notify(); }
  wait(predicate: (value: T) => boolean, label = "receipt", count = 1): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      const inspect = () => {
        const matches = this.items.filter(predicate);
        if (matches.length < count) return;
        clearSafetyTimeout(timer); this.listeners.delete(inspect); resolve(matches[count - 1]!);
      };
      const timer = safetyTimeout(() => {
        this.listeners.delete(inspect); reject(new Error(`Timed out waiting for ${label} (${count})`));
      }, 5_000);
      this.listeners.add(inspect); inspect();
    });
  }
}
export function latch() {
  let release!: () => void;
  const promise = new Promise<void>(resolve => { release = resolve; });
  return { promise, release };
}
export function objectBody(envelope: TransportEnvelope): Record<string, unknown> {
  expect(typeof envelope.body, "observations must come AFTER the carrier's real stamp").not.toBe("function");
  expect(envelope.body).toBeTypeOf("object");
  return envelope.body as Record<string, unknown>;
}

/** Only the frozen directory VIEW is doubled. The control plane owns every admission decision. */
export class Routes implements ParticipantView {
  readonly records = new Map<string, ParticipantRoute>();
  live = true;
  private readonly changes = new Map<string, Set<() => void>>();
  resolve(id: string): ParticipantRoute | undefined { return this.records.get(id); }
  leaseLive(): boolean { return this.live; }
  set(id: string, route: ParticipantRoute): void {
    this.records.set(id, route); for (const notify of this.changes.get(id) ?? []) notify();
  }
  whenChanged(id: string, signal: AbortSignal): Promise<void> {
    signal.throwIfAborted();
    return new Promise<void>((resolve, reject) => {
      const listeners = this.changes.get(id) ?? new Set<() => void>();
      this.changes.set(id, listeners);
      const cleanup = () => { listeners.delete(changed); signal.removeEventListener("abort", abort); };
      const changed = () => { cleanup(); resolve(); };
      const abort = () => { cleanup(); reject(signal.reason); };
      listeners.add(changed); signal.addEventListener("abort", abort, { once: true });
      if (signal.aborted) abort();
    });
  }
}

export interface DeliverySpan {
  id?: string;
  finished: boolean;
  locks: string[];
  globalAttempts: string[];
  localLocks: string[];
}
/** next() establishes the ALS context SYNCHRONOUSLY in the caller before for-await registers
 * its continuation. An async-generator yield alone does NOT carry ALS into its consumer.
 * The span remains live through the actual carrier ack confirmation, including ACK send. */
export class DeliveryLocks {
  readonly storage = new AsyncLocalStorage<DeliverySpan>();
  readonly spans: DeliverySpan[] = [];
  constructor(readonly meshRoot: string) {}
  instrument(): () => void {
    const storage = this.storage, root = path.resolve(this.meshRoot);
    const original = MeshLock.prototype.withLock;
    const spy = vi.spyOn(MeshLock.prototype, "withLock").mockImplementation(function<T>(this: MeshLock,
      operation: () => T, ...args: Parameters<MeshLock["withLock"]> extends [unknown, ...infer R] ? R : never
    ): Promise<T> {
      const span = storage.getStore();
      if (span && !span.finished && path.resolve(this.root) === root) span.globalAttempts.push(this.lockPath);
      return original.call(this, () => {
        if (span && !span.finished) {
          (path.resolve(this.root) === root ? span.locks : span.localLocks).push(this.lockPath);
        }
        return operation();
      }, ...args) as Promise<T>;
    });
    const across = MeshLock.prototype.withLockAcrossAwait;
    const acrossSpy = vi.spyOn(MeshLock.prototype, "withLockAcrossAwait").mockImplementation(function<T>(this: MeshLock,
      operation: () => Promise<T>, timeout?: number, lockClass?: Parameters<MeshLock["withLockAcrossAwait"]>[2],
    ): Promise<T> {
      const span = storage.getStore();
      if (span && !span.finished && path.resolve(this.root) === root) span.globalAttempts.push(this.lockPath);
      return across.call(this, async () => {
        if (span && !span.finished) {
          (path.resolve(this.root) === root ? span.locks : span.localLocks).push(this.lockPath);
        }
        return operation();
      }, timeout, lockClass) as Promise<T>;
    });
    return () => { spy.mockRestore(); acrossSpy.mockRestore(); };
  }
}

export function observeCarrier(raw: StubTransport, locks?: DeliveryLocks) {
  const sent = new Receipts<TransportSent>();
  const delivered = new Receipts<{ id: string; attempt: number; sequence: number }>();
  const acked = new Receipts<{ id: string; attempt: number; sequence: number }>();
  const working = new Receipts<{ id: string; attempt: number; at: number }>();
  const transport: FabricTransport = {
    kind: raw.kind, self: raw.self,
    send: async <T>(envelope: TransportEnvelope<T>, signal?: AbortSignal): Promise<TransportSent<T>> => {
      const receipt = await raw.send(envelope, signal); sent.emit(receipt); return receipt;
    },
    inbox: signal => ({
      [Symbol.asyncIterator]() {
        const iterator = raw.inbox(signal)[Symbol.asyncIterator]();
        return {
          next() {
            const span: DeliverySpan = { finished: false, locks: [], globalAttempts: [], localLocks: [] };
            // Do not replace with storage.run around iterator.next(): the CONSUMER's continuation
            // must inherit the span too, not just the carrier's internal await.
            locks?.storage.enterWith(span);
            return iterator.next().then(next => {
              if (next.done) { span.finished = true; return next; }
              const delivery = next.value;
              const metadata = { id: delivery.payload.id, attempt: delivery.attempt, sequence: delivery.sequence };
              span.id = metadata.id; locks?.spans.push(span); delivered.emit(metadata);
              return { done: false as const, value: {
                ...delivery,
                working: () => { delivery.working(); working.emit({ id: metadata.id, attempt: metadata.attempt, at: Date.now() }); },
                ack: async () => {
                  await delivery.ack(); span.finished = true; acked.emit(metadata);
                },
                term: async (reason: string) => { await delivery.term(reason); span.finished = true; },
              } };
            });
          },
          return: () => iterator.return ? iterator.return() : Promise.resolve({ done: true as const, value: undefined }),
        };
      },
    }),
    kv: bucket => raw.kv(bucket), drainState: signal => raw.drainState(signal), close: () => raw.close(),
  };
  return { raw, transport, sent, delivered, acked, working };
}
export type Carrier = ReturnType<typeof observeCarrier>;

export interface Seen {
  format: 1; hostId: string; commandId: string; targetId: string; expiresAt: number;
  bodyDigest?: string; acceptance?: FabricControlAcceptance;
}
export const seenPrefix = "topology/control-seen/";
export const seenKey = (hostId: string, id: string) => seenPrefix + createHash("sha256").update(`${hostId}\0${id}`).digest("hex");
export const seenRoot = (root: string, hostId: string) => path.join(root, "control-seen", createHash("sha256").update(hostId).digest("hex").slice(0, 32));

/** Observe ACTUAL MeshStore commits, including E17's commit-time value callbacks.
 * Never preview/restamp a callback: the backend invokes it once at its real CAS boundary,
 * and receives its exact generated value. Before hooks are synchronous (no await under
 * the store lock); an async delay belongs outside writeBatch, as in T-X3d. */
export function persistedBoundary(root: string, hook: (phase: "before" | "after", value: Seen) => void): () => void {
  const match = (value: unknown): value is Seen => typeof value === "object" && value !== null &&
    Reflect.get(value, "format") === 1 && typeof Reflect.get(value, "commandId") === "string";
  const put = MeshStore.prototype.put;
  const putSpy = vi.spyOn(MeshStore.prototype, "put").mockImplementation(async function(this: MeshStore, input) {
    const watched = path.resolve(this.root) === path.resolve(root) && input.key.startsWith(seenPrefix) && match(input.value);
    if (watched) hook("before", input.value as Seen);
    const result = await put.call(this, input);
    if (watched) hook("after", input.value as Seen);
    return result;
  });
  const batch = MeshStore.prototype.writeBatch;
  const batchSpy = vi.spyOn(MeshStore.prototype, "writeBatch").mockImplementation(async function(this: MeshStore, input) {
    if (path.resolve(this.root) !== path.resolve(root)) return batch.call(this, input);
    const values: Seen[] = [];
    const ops = input.ops.map(op => {
      if (op.kind !== "put" || !op.key.startsWith(seenPrefix)) return op;
      const original = op.value;
      if (typeof original !== "function") {
        if (match(original)) { hook("before", original); values.push(original); }
        return op;
      }
      return { ...op, value: (writtenAt: number) => {
        const value: unknown = original(writtenAt);
        if (match(value)) { hook("before", value); values.push(value); }
        return value;
      } };
    });
    const result = await batch.call(this, { ...input, ops });
    for (const value of values) hook("after", value);
    return result;
  });
  return () => { putSpy.mockRestore(); batchSpy.mockRestore(); };
}

type InjectedOptions = FabricControlPlaneOptions & {
  transport?: FabricTransport; participants?: ParticipantView; inboxLanes?: number;
};
export class ControlFixture {
  readonly root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-control-fault-"));
  readonly meshRoot = path.join(this.root, "recipient");
  readonly senderOwner = owner("test-a", "session:sender");
  readonly receiverOwner = owner("test-b", "session:receiver");
  readonly bus = createStubBus(() => Date.now());
  readonly routes = new Routes();
  readonly receiverRoutes = new Routes();
  readonly locks = new DeliveryLocks(this.meshRoot);
  readonly senderCarrier = observeCarrier(this.bus.open(this.senderOwner));
  receiverCarrier = observeCarrier(this.bus.open(this.receiverOwner), this.locks);
  readonly sender: FabricControlPlane;
  receiver: FabricControlPlane;
  readonly journal = path.join(this.meshRoot, "main-followups", "receiver.json");
  readonly main: MainAgentController;
  readonly nativeSends = vi.fn();
  readonly nativeAbort = vi.fn();
  readonly admissions = new Receipts<FabricControlCommand>();
  private readonly planes: FabricControlPlane[] = [];
  private readonly stores: MeshStore[] = [];
  private readonly release: Array<() => void> = [];
  constructor(readonly lanes = 8) {
    assertA1();
    this.sender = this.makePlane(path.join(this.root, "sender"), this.senderOwner, this.senderCarrier, this.routes, "sender-epoch");
    this.receiver = this.makePlane(this.meshRoot, this.receiverOwner, this.receiverCarrier, this.receiverRoutes, "owner-epoch");
    this.route(this.receiverOwner.hostId);
    this.route(laneTargetA); this.route(laneTargetB);
    this.route(actorTargetA, ["ask", "followUp", "steer", "stop"]);
    this.route(actorTargetB, ["ask", "followUp", "steer", "stop"]);
    const senderRoute: ParticipantRoute = { machine: this.senderOwner.machine, ownerHostId: this.senderOwner.hostId,
      ownerIdentityId: this.senderOwner.hostId, ownerIncarnation: this.sender.incarnation,
      capabilities: ["followUp", "steer", "stop"], live: true };
    this.routes.set(this.senderOwner.hostId, senderRoute); this.receiverRoutes.set(this.senderOwner.hostId, senderRoute);
    const pi = { on: () => () => {}, sendMessage: this.nativeSends, sendUserMessage: vi.fn(), getThinkingLevel: () => "high" } as unknown as ExtensionAPI;
    this.main = new MainAgentController(pi, this.receiverOwner.hostId, true, this.root, "receiver");
    const context = { isIdle: () => false, hasPendingMessages: () => false, abort: this.nativeAbort,
      sessionManager: { getEntries: () => [], getSessionId: () => "receiver", isPersisted: () => false } } as unknown as ExtensionContext;
    this.main.attachFollowUpDrain(context, 60_000, this.journal);
    this.sender.start(() => ({ accepted: false, error: "sender has no recipient" }));
  }
  private makePlane(root: string, self: TransportOwner, carrier: Carrier, participants: ParticipantView, incarnation: string): FabricControlPlane {
    const mesh = new MeshStore(root, 64 * 1024, 1_000, { stateBackend: "file", lockProtocol: 1 }); this.stores.push(mesh);
    const options: InjectedOptions = { enabled: true, hostId: self.hostId, ownerIncarnation: incarnation,
      controlIncarnationFence: "enforce", pollMs: 20, acknowledgementTimeoutMs: 120_000,
      transport: carrier.transport, participants, inboxLanes: this.lanes,
      ...(carrier.transport.kind === "nats" ? { relayOperatorMachine: "fabric-ops" } : {}) };
    const plane = new FabricControlPlane(mesh, identity(self.hostId), options); this.planes.push(plane);
    expect(carrierKind(plane), "A1 must USE the injected carrier, not silently use the file log").toBe(carrier.transport.kind);
    expect(Reflect.get(plane, "participantView"), "A1 must expose the injected frozen ParticipantView").toBe(participants);
    return plane;
  }
  route(id: string, capabilities: readonly string[] = ["followUp", "steer", "stop"]): void {
    const route: ParticipantRoute = { machine: this.receiverOwner.machine, ownerHostId: this.receiverOwner.hostId,
      ownerIdentityId: this.receiverOwner.hostId, ownerIncarnation: this.receiver.incarnation, capabilities, live: true };
    this.routes.set(id, route); this.receiverRoutes.set(id, route);
  }
  start(handler: FabricControlHandler = this.nativeAdmission): void {
    this.receiver.start(async (...args) => { this.admissions.emit(args[0]); return handler(...args); });
  }
  readonly nativeAdmission: FabricControlHandler = (command, from, _signal, verification) => {
    if (command.operation === "stop") {
      this.main.stop(); return { accepted: true, messageId: command.commandId };
    }
    if (command.operation !== "followUp" && command.operation !== "steer") throw new Error(`No native fixture handler for ${command.operation}`);
    const result = this.main.deliverAgent({ from, verification: verification === "bridge" ? "bridge" : "mesh",
      message: command.message ?? "control fault input", delivery: command.operation, deliveryId: command.commandId,
      ...(command.data === undefined ? {} : { data: command.data }),
      ...(command.principal === undefined ? {} : { principal: command.principal }),
    });
    return { accepted: true, messageId: result.messageId, ...this.main.queueDepth() };
  };
  keep(release: () => void): void { this.release.push(release); }
  async restart(incarnation = this.receiver.incarnation): Promise<void> {
    this.receiver.pause(); await this.receiver.close();
    await this.receiverCarrier.raw.close();
    this.receiverCarrier = observeCarrier(this.bus.open(this.receiverOwner), this.locks);
    this.receiver = this.makePlane(this.meshRoot, this.receiverOwner, this.receiverCarrier, this.receiverRoutes, incarnation);
    for (const id of [...this.routes.records.keys()]) {
      const route = this.routes.records.get(id)!;
      if (route.ownerHostId === this.receiverOwner.hostId) this.route(id, route.capabilities);
    }
  }
  // Raw scenarios may intentionally choose equal ids; request() never does. eventId lets
  // boundary tests exercise distinct ids without rewriting any generated semantic body fields.
  raw(commandId: string, options: { eventId?: string; target?: string; operation?: string; timeoutMs?: number; message?: string; incarnation?: string } = {}): TransportEnvelope {
    const envelope = commandEnvelope({ id: commandId, to: this.receiverOwner, from: identity(this.senderOwner.hostId),
      targetId: options.target ?? this.receiverOwner.hostId, operation: options.operation ?? "followUp",
      ownerIncarnation: options.incarnation ?? this.receiver.incarnation, timeoutMs: options.timeoutMs ?? 120_000,
      message: options.message ?? "control fault input" });
    return { ...envelope, id: options.eventId ?? commandId };
  }
  async publish(envelope: TransportEnvelope): Promise<ResolvedEnvelope> {
    return (await this.senderCarrier.transport.send(envelope)).envelope;
  }
  replay(envelope: ResolvedEnvelope, attempt = 2) {
    return this.receiverCarrier.raw.enqueue(envelope, { sourceMachine: this.senderOwner.machine, verification: "bridge", attempt });
  }
  // Carrier receipts are keyed by commandEventId; no logical-id remapping here.
  async ack(commandEventId: string, count = 1): Promise<Record<string, unknown>> {
    return objectBody((await this.receiverCarrier.sent.wait(sent => sent.envelope.kind === "ack" && sent.id === `ack:${commandEventId}`, `ACK PubAck ${commandEventId}`, count)).envelope);
  }
  async settled(commandEventId: string, count = 1): Promise<void> {
    await this.receiverCarrier.acked.wait(receipt => receipt.id === commandEventId, `broker ack ${commandEventId}`, count);
  }
  // Native custody/control-seen are keyed by logical commandId, not the carrier id.
  seen(commandId: string): Seen | undefined {
    const store = new MeshStore(seenRoot(this.meshRoot, this.receiverOwner.hostId), 64 * 1024, 1_000, { stateBackend: "file", lockProtocol: 1 });
    try { return store.get(seenKey(this.receiverOwner.hostId, commandId), { fresh: true })?.value as Seen | undefined; }
    finally { store.closeState(); }
  }
  journalItems(): HeldAgentMessage[] {
    return fs.existsSync(this.journal) ? (JSON.parse(fs.readFileSync(this.journal, "utf8")) as { items: HeldAgentMessage[] }).items : [];
  }
  async dispose(): Promise<void> {
    for (const release of this.release.splice(0)) release();
    for (const plane of this.planes) plane.pause();
    await Promise.allSettled(this.planes.map(plane => plane.close()));
    this.main.closeFollowUpDrain();
    await Promise.allSettled([this.senderCarrier.raw.close(), this.receiverCarrier.raw.close()]);
    for (const store of this.stores) store.closeState();
    fs.rmSync(this.root, { recursive: true, force: true });
  }
}
export function resolved(envelope: TransportEnvelope): ResolvedEnvelope { return resolveEnvelope(envelope, Date.now()); }
export function fromOwner(f: ControlFixture): MeshIdentity { return identity(f.receiverOwner.hostId); }
