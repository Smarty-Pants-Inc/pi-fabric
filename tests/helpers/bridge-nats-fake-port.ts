// In-memory fake of L1's raw BridgeNatsPort (src/mesh/bridge-nats-types.ts), for L2's NatsCarrierSide unit
// tests (smarty-dev#15310, A' lane L4). One FakeNatsBroker holds every machine's KV presence + leases and one
// inbox stream per destination machine; `broker.port(machine)` is the bridge's port on that machine.
//
// It keeps the semantics the carrier must respect, and nothing more:
// - the credential-bound source machine is the sending port's machine, never an envelope field;
// - Nats-Msg-Id dedupe: a second send with the same envelope id stores nothing and reports `duplicate`;
// - a stamp body runs once per attempt, synchronously, before storage;
// - at-least-once inbox in stream order, one iterator per port; ack/term are final; nak and an AckWait
//   expiry (`expireAckWait`) redeliver with attempt + 1; max_deliver 5 then the message is dead;
// - KV presence is live only as a presence + lease pair with the same incarnation; an id with rows on two
//   machines is ambiguous and resolves to nothing;
// - mirror() writes only under the port's own machine, refuses an id that has a row on another machine,
//   and fails after fence()/withdraw().
// Faults are explicit and counted: `failNextSend(kind)`, `failPresenceWatch(error)`, `dropLease(...)`.
import type { BridgeHost, BridgePresence } from "../../src/mesh/bridge.js";
import type {
  BridgeNatsMirrorSet, BridgeNatsPort, BridgeNatsPortDelivery, BridgeNatsPortResolved, BridgeNatsPresenceRecord, BridgeNatsPresenceView,
} from "../../src/mesh/bridge-nats-types.js";
import type { MeshIdentity } from "../../src/mesh/store.js";
import type { FabricParticipantRecord } from "../../src/topology/types.js";
import {
  TransportUnavailableError, TransportUncertainError, type TransportEnvelope, type TransportSent,
} from "../../src/transport/types.js";

const CANONICAL_ROOT = /^session:[A-Za-z0-9-]{8,128}$/;
const MAX_DELIVER = 5;
/** participant-view.ts / MIRROR_FRESH_MS: a lease's lifetime after its last write. */
export const FAKE_LEASE_TTL_MS = 15_000;

export interface FakeKvRow {
  readonly machine: string;
  readonly record: FabricParticipantRecord;
  /** Undefined = no lease (not live). */
  lease?: { readonly incarnation: string; readonly expiresAt: number };
}

export interface FakeStoredMessage {
  readonly sequence: number;
  /** Destination machine (the stream). */
  readonly machine: string;
  readonly sourceMachine: string;
  readonly envelope: TransportEnvelope & { readonly body: unknown; readonly createdAt: number };
  delivered: number;
  state: "pending" | "inflight" | "acked" | "termed" | "dead";
  termReason?: string;
}

export type FakeSendFault =
  /** The broker stored it; the PubAck is lost. */
  | "uncertainStored"
  /** The publish never reached the broker; the sender still cannot know. */
  | "uncertainLost"
  /** Definitely not stored. */
  | "unavailable";

export interface FakeSendAttempt {
  readonly machine: string;
  readonly envelope: TransportEnvelope & { readonly body: unknown; readonly createdAt: number };
  readonly outcome: "stored" | "duplicate" | FakeSendFault;
  readonly sequence?: number;
}

const kind = (record: FabricParticipantRecord): MeshIdentity["kind"] => record.kind === "root" ? "main" : record.kind === "actor" ? "actor" : "agent";

export class FakeNatsBroker {
  readonly rows = new Map<string, FakeKvRow>();
  readonly messages: FakeStoredMessage[] = [];
  readonly sends: FakeSendAttempt[] = [];
  readonly ports = new Map<string, FakeBridgeNatsPort>();
  readonly #watchers = new Set<{ changed: () => void; failed: (error: Error) => void }>();
  readonly #wakers = new Set<() => void>();
  #sequence = 0;
  constructor(readonly now: () => number = Date.now) {}

  #key(machine: string, id: string) { return `${machine}\0${id}`; }
  notifyPresence(): void { for (const watcher of [...this.#watchers]) watcher.changed(); }
  /** @internal */ watch(watcher: { changed: () => void; failed: (error: Error) => void }): () => void {
    this.#watchers.add(watcher);
    return () => { this.#watchers.delete(watcher); };
  }
  failPresenceWatch(error = new Error("fake presence watch failed")): void { for (const watcher of [...this.#watchers]) watcher.failed(error); }
  /** @internal */ wake(): void { for (const wake of [...this.#wakers]) wake(); }
  /** @internal */ waitForWork(signal: AbortSignal): Promise<void> {
    return new Promise((resolve) => {
      const done = () => { this.#wakers.delete(done); signal.removeEventListener("abort", done); resolve(); };
      this.#wakers.add(done);
      signal.addEventListener("abort", done, { once: true });
    });
  }

  /** Put a participant (presence) and, unless `live` is false, its matching lease under `machine`. */
  putParticipant(machine: string, record: FabricParticipantRecord, live = true): FakeKvRow {
    const row: FakeKvRow = { machine, record: structuredClone(record),
      ...(live && record.ownerIncarnation ? { lease: { incarnation: record.ownerIncarnation, expiresAt: this.now() + FAKE_LEASE_TTL_MS } } : {}) };
    this.rows.set(this.#key(machine, record.id), row);
    this.notifyPresence();
    return row;
  }
  /** A canonical root `session:<uuid>` live on `machine`. */
  putRoot(machine: string, id: string, incarnation: string, extra: Partial<FabricParticipantRecord> = {}): FakeKvRow {
    const now = this.now();
    return this.putParticipant(machine, { format: 1, id, rootId: id, kind: "root", ownerHostId: id, ownerIdentityId: id,
      ownerIncarnation: incarnation, sessionId: id.slice("session:".length), name: extra.name ?? id, status: "idle", runner: "pi",
      transport: "host", capabilities: ["steer", "followUp", "fabric"], controlProtocol: "v1", startedAt: now, updatedAt: now, ...extra });
  }
  /** Lease expiry: the presence row stays (600 s TTL), the route stops being live. */
  dropLease(machine: string, id: string): void {
    const row = this.rows.get(this.#key(machine, id));
    if (row) { delete row.lease; this.notifyPresence(); }
  }
  removeParticipant(machine: string, id: string): void {
    if (this.rows.delete(this.#key(machine, id))) this.notifyPresence();
  }
  rowsFor(id: string): FakeKvRow[] { return [...this.rows.values()].filter((row) => row.record.id === id); }
  rowsOn(machine: string): FakeKvRow[] { return [...this.rows.values()].filter((row) => row.machine === machine); }
  live(row: FakeKvRow): boolean {
    return row.lease !== undefined && row.lease.incarnation === row.record.ownerIncarnation && row.lease.expiresAt > this.now();
  }

  /** Store one envelope from `sourceMachine` into `envelope.to.machine`'s stream (a NATS-side peer, or a port). */
  publish(sourceMachine: string, envelope: TransportEnvelope): { sequence: number; duplicate: boolean; stored: FakeStoredMessage } {
    const machine = envelope.to.machine;
    const existing = this.messages.find((message) => message.machine === machine && message.envelope.id === envelope.id);
    if (existing) return { sequence: existing.sequence, duplicate: true, stored: existing };
    const createdAt = this.now();
    const body = typeof envelope.body === "function" ? (envelope.body as (at: number) => unknown)(createdAt) : envelope.body;
    const stored: FakeStoredMessage = { sequence: ++this.#sequence, machine, sourceMachine, delivered: 0, state: "pending",
      envelope: structuredClone({ ...envelope, body, createdAt }) };
    this.messages.push(stored);
    this.wake();
    return { sequence: stored.sequence, duplicate: false, stored };
  }
  messagesTo(machine: string, filter: (message: FakeStoredMessage) => boolean = () => true): FakeStoredMessage[] {
    return this.messages.filter((message) => message.machine === machine && filter(message));
  }
  /** AckWait elapsed: every in-flight delivery on `machine` becomes deliverable again. */
  expireAckWait(machine: string): number {
    let count = 0;
    for (const message of this.messagesTo(machine, (entry) => entry.state === "inflight")) { message.state = "pending"; count++; }
    if (count) this.wake();
    return count;
  }

  port(machine: string, options: { admit?: (record: FabricParticipantRecord, machine: string) => boolean } = {}): FakeBridgeNatsPort {
    if (this.ports.has(machine)) throw new Error(`fake port for ${machine} already open`);
    const port = new FakeBridgeNatsPort(this, machine, options.admit);
    this.ports.set(machine, port);
    return port;
  }
}

export class FakeBridgeNatsPort implements BridgeNatsPort {
  readonly mirrors: Array<{ set: BridgeNatsMirrorSet; observedAt: number }> = [];
  /** Ids mirror() refused because another machine already has a row for them. */
  readonly refusedMirror: string[] = [];
  readonly faults: FakeSendFault[] = [];
  fenced = false;
  closed = false;
  workingCalls = 0;
  #iterating = false;
  readonly #closing = new AbortController();
  constructor(readonly broker: FakeNatsBroker, readonly machine: string,
    private readonly admit?: (record: FabricParticipantRecord, machine: string) => boolean) {}

  failNextSend(fault: FakeSendFault): void { this.faults.push(fault); }

  async presence(): Promise<BridgePresence> {
    const observedAt = this.broker.now();
    const hosts: BridgeHost[] = [];
    const participants: FabricParticipantRecord[] = [];
    for (const row of this.broker.rows.values()) {
      const record = row.record;
      if (row.machine === this.machine || !this.broker.live(row) || record.kind !== "root" || !CANONICAL_ROOT.test(record.id) ||
        record.id !== record.rootId || record.id !== record.ownerHostId || record.id !== record.ownerIdentityId) continue;
      if (this.broker.rowsFor(record.id).length !== 1) continue; // ambiguous: not routable, not mirrored
      if (this.admit && !this.admit(record, row.machine)) continue;
      const expiresAt = Math.min(row.lease!.expiresAt, observedAt + FAKE_LEASE_TTL_MS);
      hosts.push({ record: { format: 1, id: record.id, rootId: record.rootId,
        identity: { id: record.id, name: record.name, kind: "main", ...(record.sessionId ? { sessionId: record.sessionId } : {}) },
        incarnationToken: record.ownerIncarnation!, startedAt: record.startedAt, updatedAt: observedAt, expiresAt }, expiresAt, observedAt });
      participants.push(structuredClone(record));
    }
    return { hosts, participants, reserved: [] };
  }

  watchPresence(changed: () => void, failed: (error: Error) => void): () => void {
    return this.broker.watch({ changed, failed });
  }

  resolve(id: string): BridgeNatsPortResolved | undefined {
    const rows = this.broker.rowsFor(id);
    if (rows.length !== 1) return undefined;
    const { machine, record } = rows[0]!;
    return {
      route: { machine, ownerHostId: record.ownerHostId, ownerIdentityId: record.ownerIdentityId, ownerIncarnation: record.ownerIncarnation ?? "",
        capabilities: [...record.capabilities], live: this.broker.live(rows[0]!) },
      identity: { id: record.id, name: record.name, kind: kind(record), ...(record.sessionId ? { sessionId: record.sessionId } : {}) },
    };
  }

  presenceView(): BridgeNatsPresenceView {
    const observedAt = this.broker.now();
    return { side: "nats", records: (id: string): readonly BridgeNatsPresenceRecord[] => this.broker.rowsFor(id).map((row) => ({
      id: row.record.id, name: row.record.name, kind: kind(row.record), rootId: row.record.rootId,
      ...(row.record.sessionId ? { sessionId: row.record.sessionId } : {}),
      ownerHostId: row.record.ownerHostId, ownerIdentityId: row.record.ownerIdentityId,
      ...(row.record.ownerIncarnation ? { ownerIncarnation: row.record.ownerIncarnation } : {}),
      machine: row.machine, live: this.broker.live(row), observedAt,
    })) };
  }

  async mirror(set: BridgeNatsMirrorSet, observedAt: number): Promise<void> {
    if (this.fenced || this.closed) throw new Error(`fake port ${this.machine}: mirror after fence/close`);
    this.mirrors.push({ set: structuredClone(set), observedAt });
    const wanted = new Map<string, FabricParticipantRecord>();
    for (const record of set.participants) wanted.set(record.id, record);
    const link = set.linkIdentity, now = this.broker.now();
    wanted.set(link.id, { format: 1, id: link.id, rootId: link.id, kind: "root", ownerHostId: link.id, ownerIdentityId: link.id,
      ownerIncarnation: `link-${this.machine}`, name: link.name, status: "idle", runner: "pi", transport: "host",
      capabilities: ["followUp"], controlProtocol: "v1", startedAt: now, updatedAt: now });
    for (const row of this.broker.rowsOn(this.machine)) if (!wanted.has(row.record.id)) this.broker.removeParticipant(this.machine, row.record.id);
    for (const record of wanted.values()) {
      if (this.broker.rowsFor(record.id).some((row) => row.machine !== this.machine)) { this.refusedMirror.push(record.id); continue; }
      this.broker.putParticipant(this.machine, record);
    }
  }

  async fence(): Promise<void> { this.fenced = true; }
  async withdraw(): Promise<void> {
    this.fenced = true;
    for (const row of this.broker.rowsOn(this.machine)) this.broker.removeParticipant(this.machine, row.record.id);
  }

  async send<T>(envelope: TransportEnvelope<T>, _signal?: AbortSignal): Promise<TransportSent<T>> {
    if (this.closed) throw new TransportUnavailableError(`fake port ${this.machine} closed`);
    const fault = this.faults.shift();
    const createdAt = this.broker.now();
    const body = (typeof envelope.body === "function" ? (envelope.body as (at: number) => T)(createdAt) : envelope.body) as T;
    const attempt = structuredClone({ ...envelope, body, createdAt }) as TransportEnvelope & { body: unknown; createdAt: number };
    if (fault === "unavailable") {
      this.broker.sends.push({ machine: this.machine, envelope: attempt, outcome: fault });
      throw new TransportUnavailableError(`fake send of ${envelope.id} refused`);
    }
    if (fault === "uncertainLost") {
      this.broker.sends.push({ machine: this.machine, envelope: attempt, outcome: fault });
      throw new TransportUncertainError(envelope.id, body);
    }
    const stored = this.broker.publish(this.machine, { ...envelope, body });
    this.broker.sends.push({ machine: this.machine, envelope: attempt, outcome: fault ?? (stored.duplicate ? "duplicate" : "stored"), sequence: stored.sequence });
    if (fault === "uncertainStored") throw new TransportUncertainError(envelope.id, body);
    return { id: envelope.id, sequence: stored.sequence, duplicate: stored.duplicate,
      envelope: { ...envelope, body, createdAt } as TransportSent<T>["envelope"] };
  }

  async *inbox(signal: AbortSignal): AsyncIterable<BridgeNatsPortDelivery> {
    if (this.#iterating) throw new Error(`fake port ${this.machine}: one inbox iterator per port`);
    this.#iterating = true;
    const stop = AbortSignal.any([signal, this.#closing.signal]);
    try {
      while (!stop.aborted) {
        const next = this.broker.messagesTo(this.machine, (message) => message.state === "pending").sort((a, b) => a.sequence - b.sequence)[0];
        if (!next) { await this.broker.waitForWork(stop); continue; }
        if (next.delivered >= MAX_DELIVER) { next.state = "dead"; continue; }
        next.delivered += 1;
        next.state = "inflight";
        const attempt = next.delivered;
        const settle = (state: FakeStoredMessage["state"]) => {
          if (next.state !== "inflight" || next.delivered !== attempt) throw new Error(`fake delivery ${next.sequence}/${attempt} is not in flight`);
          next.state = state;
        };
        yield {
          envelope: structuredClone(next.envelope), sourceMachine: next.sourceMachine, streamSequence: next.sequence, attempt,
          ack: async () => { settle("acked"); },
          term: async (reason: string) => { settle("termed"); next.termReason = reason; },
          nak: () => { settle("pending"); this.broker.wake(); },
          working: () => { this.workingCalls += 1; },
        };
      }
    } finally { this.#iterating = false; }
  }

  async close(): Promise<void> {
    this.closed = true;
    this.#closing.abort();
    this.broker.wake();
    this.broker.ports.delete(this.machine);
  }
}
