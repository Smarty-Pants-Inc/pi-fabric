// Lane L2 unit fakes for the NATS carrier (smarty-dev#15310): an in-memory ledger, presence, and a
// BridgeNatsPort that records sends and feeds inbox deliveries. Not a broker: L4's harness owns E2E.
import type { BridgePresence } from "../../src/mesh/bridge.js";
import type {
  FilePresenceEntry, NatsCarrierDirection, NatsCarrierLedger, NatsCarrierOutcome, NatsCarrierPresence, NatsCarrierRecord,
  NatsPresenceEntry,
} from "../../src/mesh/bridge-nats-carrier.js";
import type {
  BridgeNatsAckRecord, BridgeNatsClaimInput, BridgeNatsClaimResult, BridgeNatsCommandRecord, BridgeNatsDirection, BridgeNatsLedger,
  BridgeNatsMirrorSet, BridgeNatsPort, BridgeNatsPortDelivery, BridgeNatsPortResolved, BridgeNatsPresenceRecord, BridgeNatsPresenceView,
  BridgeNatsSeenRecord,
} from "../../src/mesh/bridge-nats-types.js";
import type { MeshIdentity } from "../../src/mesh/store.js";
import { TransportUncertainError, type TransportEnvelope, type TransportSent } from "../../src/transport/types.js";

export const BRIDGE_MACHINE = "ryzen1-bridge";
export const POD_MACHINE = "v2pod";
export const LINK = "v2";

export const fileLead: MeshIdentity = { id: "session:11111111-1111-4111-8111-111111111111", name: "file-lead", kind: "main" };
export const natsLead: MeshIdentity = { id: "session:22222222-2222-4222-8222-222222222222", name: "moved-lead", kind: "main" };
export const otherNats: MeshIdentity = { id: "session:33333333-3333-4333-8333-333333333333", name: "other-v2", kind: "main" };
export const linkIdentity: MeshIdentity = { id: `bridge:${LINK}`, name: "mesh-bridge", kind: "main" };

export class MemoryLedger implements NatsCarrierLedger {
  readonly records = new Map<string, NatsCarrierRecord>();
  readonly settled = new Map<string, NatsCarrierOutcome>();
  async claim(record: NatsCarrierRecord): Promise<{ created: boolean; record: NatsCarrierRecord }> {
    const key = `${record.direction}:${record.destinationCommandId}`;
    const existing = this.records.get(key);
    if (existing) return { created: false, record: existing };
    this.records.set(key, JSON.parse(JSON.stringify(record)) as NatsCarrierRecord);
    return { created: true, record: this.records.get(key)! };
  }
  async lookup(direction: NatsCarrierDirection, id: string): Promise<NatsCarrierRecord | undefined> {
    return this.records.get(`${direction}:${id}`);
  }
  async settle(direction: NatsCarrierDirection, id: string, outcome: NatsCarrierOutcome): Promise<void> {
    this.settled.set(`${direction}:${id}`, outcome);
  }
}

export const route = (identity: MeshIdentity, machine: string, incarnation = `inc-${identity.name}`,
  extra: Partial<NatsPresenceEntry["route"]> = {}): NatsPresenceEntry => ({
  identity, route: { machine, ownerHostId: identity.id, ownerIdentityId: identity.id, ownerIncarnation: incarnation,
    capabilities: [], live: true, ...extra },
});

export class FakePresence implements NatsCarrierPresence {
  readonly natsEntries = new Map<string, NatsPresenceEntry>([
    [natsLead.id, route(natsLead, POD_MACHINE)],
    [otherNats.id, route(otherNats, POD_MACHINE)],
    // File roots and the link's identity are mirrored into KV under the bridge machine (L1).
    [fileLead.id, route(fileLead, BRIDGE_MACHINE)],
    [linkIdentity.id, route(linkIdentity, BRIDGE_MACHINE)],
  ]);
  readonly fileEntries = new Map<string, FilePresenceEntry>([[fileLead.id, { identity: fileLead, hostId: fileLead.id }]]);
  nats(id: string): NatsPresenceEntry | undefined { return this.natsEntries.get(id); }
  file(id: string): FilePresenceEntry | undefined { return this.fileEntries.get(id); }
}

export interface FakeDelivery extends BridgeNatsPortDelivery {
  acked: number;
  termed: string[];
  worked: number;
}

export class FakePort implements BridgeNatsPort {
  readonly machine = BRIDGE_MACHINE;
  readonly sent: Array<TransportEnvelope<Record<string, unknown>> & { body: Record<string, unknown> }> = [];
  readonly attempts: string[] = [];
  readonly mirrors: BridgeNatsMirrorSet[] = [];
  readonly routes = new Map<string, BridgeNatsPortResolved>();
  natsPresence: BridgePresence = { hosts: [], participants: [], reserved: [] };
  /** The next N sends throw TransportUncertainError (nothing recorded as sent). */
  uncertain = 0;
  #sequence = 0;
  #stream = 0;
  readonly #queue: FakeDelivery[] = [];
  #wake: (() => void) | undefined;
  closed = false;

  async presence(): Promise<BridgePresence> { return this.natsPresence; }
  watchPresence(): () => void { return () => undefined; }
  resolve(id: string): BridgeNatsPortResolved | undefined { return this.routes.get(id); }
  /** KV records as L1 lists them: one per resolved route, machine from the (fake) KV key. */
  presenceView(): BridgeNatsPresenceView {
    const record = (resolved: BridgeNatsPortResolved): BridgeNatsPresenceRecord => ({
      id: resolved.identity.id, name: resolved.identity.name, kind: resolved.identity.kind,
      rootId: resolved.identity.kind === "main" ? resolved.identity.id : resolved.route.ownerHostId,
      ownerHostId: resolved.route.ownerHostId, ownerIdentityId: resolved.route.ownerIdentityId,
      ownerIncarnation: resolved.route.ownerIncarnation, machine: resolved.route.machine, live: resolved.route.live,
      observedAt: Date.now(),
    });
    return { side: "nats", records: id => { const resolved = this.routes.get(id); return resolved ? [record(resolved)] : []; } };
  }
  async mirror(set: BridgeNatsMirrorSet): Promise<void> { this.mirrors.push(set); }
  async fence(): Promise<void> {}
  async withdraw(): Promise<void> {}
  async close(): Promise<void> { this.closed = true; this.#wake?.(); }

  async send<T>(envelope: TransportEnvelope<T>): Promise<TransportSent<T>> {
    const body = (typeof envelope.body === "function" ? (envelope.body as (at: number) => T)(Date.now()) : envelope.body) as T;
    this.attempts.push(envelope.id);
    if (this.uncertain > 0) {
      this.uncertain -= 1;
      throw new TransportUncertainError(envelope.id, body);
    }
    const copy = JSON.parse(JSON.stringify({ ...envelope, body })) as TransportEnvelope<Record<string, unknown>> & { body: Record<string, unknown> };
    this.sent.push(copy);
    return { id: envelope.id, sequence: ++this.#sequence, duplicate: false, envelope: { ...envelope, body, createdAt: Date.now() } };
  }

  /** Queue one inbox delivery; returns its handle (ack/term counters). onAck runs inside ack(), before it counts. */
  deliver(envelope: Record<string, unknown>, sourceMachine = POD_MACHINE, streamSequence = ++this.#stream, onAck?: () => void): FakeDelivery {
    this.#stream = Math.max(this.#stream, streamSequence);
    const delivery: FakeDelivery = {
      envelope: { createdAt: Date.now(), ...envelope } as unknown as FakeDelivery["envelope"],
      sourceMachine, streamSequence, attempt: 1, acked: 0, termed: [], worked: 0,
      async ack() { onAck?.(); delivery.acked += 1; },
      async term(reason: string) { delivery.termed.push(reason); },
      nak() {},
      working() { delivery.worked += 1; },
    };
    this.#queue.push(delivery);
    this.#wake?.();
    return delivery;
  }

  async *inbox(signal: AbortSignal): AsyncIterable<BridgeNatsPortDelivery> {
    while (!signal.aborted && !this.closed) {
      const next = this.#queue.shift();
      if (next) { yield next; continue; }
      await new Promise<void>(resolve => {
        this.#wake = resolve;
        signal.addEventListener("abort", () => resolve(), { once: true });
      });
      this.#wake = undefined;
    }
  }
}

/** An in-memory stand-in for L3's durable ledger: the contract's claim statuses, no persistence. */
export class MemoryBridgeNatsLedger implements BridgeNatsLedger {
  readonly records = new Map<string, BridgeNatsCommandRecord>();
  async claim(input: BridgeNatsClaimInput): Promise<BridgeNatsClaimResult> {
    const existing = this.records.get(input.commandId);
    if (existing) {
      return { status: existing.bodyDigest !== input.bodyDigest ? "conflict"
        : existing.sourceMachine !== input.sourceMachine || existing.replyTo !== input.replyTo ? "mismatch" : "duplicate", record: existing };
    }
    const claimedAt = Date.now();
    const record: BridgeNatsCommandRecord = JSON.parse(JSON.stringify({ ...input, v: 1, state: "claimed", claimedAt,
      expiresAt: Math.max(input.deadlineAt, claimedAt) + 86_400_000 })) as BridgeNatsCommandRecord;
    this.records.set(input.commandId, record);
    return { status: "new", record };
  }
  lookup(commandId: string): BridgeNatsCommandRecord | undefined { return this.records.get(commandId); }
  lookupOriginal(direction: BridgeNatsDirection, requesterId: string, original: string): BridgeNatsCommandRecord | undefined {
    return [...this.records.values()].find(record => record.direction === direction && record.requesterId === requesterId &&
      record.originalCommandId === original);
  }
  async markUncertain(commandId: string): Promise<BridgeNatsCommandRecord> { return this.#set(commandId, { state: "uncertain" }); }
  async markSent(commandId: string): Promise<BridgeNatsCommandRecord> { return this.#set(commandId, { state: "sent" }); }
  async settle(commandId: string, ack: BridgeNatsAckRecord): Promise<BridgeNatsCommandRecord> {
    const record = this.records.get(commandId)!;
    return record.ack ? record : this.#set(commandId, { state: "acked", ack });
  }
  async markAckForwarded(commandId: string): Promise<BridgeNatsCommandRecord> { return this.#set(commandId, { state: "ackForwarded" }); }
  pendingResends(): readonly BridgeNatsCommandRecord[] { return [...this.records.values()].filter(record => record.state === "claimed" || record.state === "uncertain"); }
  pendingAcks(): readonly BridgeNatsCommandRecord[] { return [...this.records.values()].filter(record => record.state === "acked"); }
  seen(): BridgeNatsSeenRecord | undefined { return undefined; }
  async markSeen(direction: BridgeNatsDirection, eventId: string): Promise<BridgeNatsSeenRecord> {
    return { v: 1, eventId, direction, seenAt: Date.now(), expiresAt: Date.now() + 86_400_000 };
  }
  async close(): Promise<void> {}
  #set(commandId: string, change: Partial<BridgeNatsCommandRecord>): BridgeNatsCommandRecord {
    const next = { ...this.records.get(commandId)!, ...change } as BridgeNatsCommandRecord;
    this.records.set(commandId, next);
    return next;
  }
}

/** Let the inbox pump move queued deliveries in hand. */
export const settle = async (): Promise<void> => {
  for (let index = 0; index < 5; index++) await new Promise<void>(resolve => setImmediate(resolve));
};
