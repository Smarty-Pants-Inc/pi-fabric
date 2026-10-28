import { readFileRetrying, writeJsonAtomic } from "../core/atomic-write.js";
import { participantFromEntry } from "../topology/participant-directory.js";
import { readParticipantFiles } from "../topology/participant-files.js";
import type { FabricParticipantRecord } from "../topology/types.js";
import {
  DirectoryNotReadyError, TransportUncertainError, TransportUnavailableError, type TransportSent,
} from "../transport/types.js";
import type { BridgeChange } from "./change-notifier.js";
import {
  admitPresence, BRIDGE_PROTOCOL_VERSION, BridgeMirrorDeferredError, LEASE_LAPSE_DEFER_MS, MeshBridge, StoreBridgeSide, type BridgedIds, type BridgeHead, type BridgePresence, type BridgePublish,
  type BridgeRead, type BridgeSide, type MeshBridgeOptions,
} from "./bridge.js";
import {
  carrierDeliveryFromPort, NatsBridgeCarrier, NatsCarrierRefusal, NatsCarrierUnavailable, type FilePresenceEntry, type NatsCarrierAuthorize,
  type NatsCarrierEnvelope, type NatsCarrierFileAnswer, type NatsCarrierLedger, type NatsCarrierPresence, type NatsCarrierSettle,
  type NatsInbound, type NatsOutbound, type NatsPresenceEntry,
} from "./bridge-nats-carrier.js";
import { NatsTopicTranslator, type NatsTopicAuthorize } from "./bridge-nats-topics.js";
import {
  BRIDGE_NATS_ACK_TOPIC, BRIDGE_NATS_CONTROL_TOPIC, bridgeNatsLinkIdentity, type BridgeNatsPort, type BridgeNatsPortDelivery,
  type BridgeNatsPresenceRecord, type BridgeNatsPresenceView, type NatsCarrierSideContract,
} from "./bridge-nats-types.js";
import type { MeshEvent, MeshIdentity, MeshStore } from "./store.js";
import { isMeshRetryableBusy } from "./state-backend.js";

/**
 * The file-mesh half of the A' carrier (smarty-dev#15310, lane L2): the BridgeSide MeshBridge uses
 * as its `remote`, over lane L1's raw BridgeNatsPort. MeshBridge keeps every rule it applies to a
 * file peer (sender from presence, addressed recipients, held mirrors at commit, cursors, dedupe
 * receipts); this side turns what crosses into NATS envelopes and back with NatsBridgeCarrier
 * (control) and NatsTopicTranslator (N3), and owns the hop's two hand-offs:
 *
 * - toNats: publish() resolves only after the PubAck. An uncertain outcome is resent with the same
 *   envelope id and bytes a bounded number of times, then thrown (retryable): MeshBridge keeps its
 *   cursor before the event and re-presents it, and the ledger hands back the same envelope.
 * - toFile: inbox deliveries get this side's own sequence when MeshBridge first reads them. A
 *   delivery is ACKed to the broker only when MeshBridge reads again from a cursor at or past it,
 *   that is after its durable file append (or its deliberate drop). Deliveries the carrier answers
 *   itself (refusals, settled ACKs, cancels naming nothing) are ACKed or TERMed in stream order.
 *
 * Sequences are this side's, not the stream's: a redelivery after a restart, or a recreated stream,
 * is presented again above the saved cursor, never ACKed unread. Presenting one twice is safe: the
 * file store's dedupe receipts (mapped commandId, ACK stamp) and MeshBridge's bridged-id recovery
 * make the second a no-op.
 */

const FILE_PRESENCE_TTL_MS = 1_000;
const RESEND_ATTEMPTS = 3;
const RESEND_DELAY_MS = 250;
/** JetStream redelivers an in-hand message after its ack wait; extend well inside the shortest one. */
const WORKING_INTERVAL_MS = 5_000;
/** In-hand deliveries this side holds before it stops pulling (backpressure, not loss). */
const MAX_IN_HAND = 512;
const PARTICIPANT_PREFIX = "topology/participants/";

/**
 * A transient NATS-side condition (an outage, a KV gap, an uncertain PubAck) as MeshBridge's own retryable
 * error: the direction backs off and retries from its durable cursor instead of ending the link. Anything
 * else a side throws is terminal for MeshBridge's run, which is right for real faults and wrong for these.
 */
export class NatsCarrierDeferredError extends BridgeMirrorDeferredError {
  override readonly name = "NatsCarrierDeferredError";
  constructor(readonly reason: unknown) {
    super(false, 0);
    this.message = `Fabric NATS side is temporarily unavailable; retrying with backoff: ${message(reason)}`;
    this.cause = reason;
  }
}

const TRANSIENT_CODES: ReadonlySet<unknown> = new Set([
  "FABRIC_TRANSPORT_UNAVAILABLE", "FABRIC_TRANSPORT_UNCERTAIN", "FABRIC_TRANSPORT_CAPACITY", "FABRIC_DIRECTORY_NOT_READY",
  // L1's port refuses a send from an identity it does not hold live in KV right now (a lease write missed in an outage).
  "FABRIC_SENDER_UNVERIFIED", "FABRIC_NATS_CARRIER_TARGET_UNAVAILABLE",
]);

/** Whether an error is a NATS-side condition that clears by itself (retry), not a fault (end the link). */
export const isNatsCarrierTransient = (error: unknown): boolean =>
  error instanceof NatsCarrierUnavailable || error instanceof NatsCarrierUncertainError ||
  error instanceof TransportUncertainError || error instanceof TransportUnavailableError || error instanceof DirectoryNotReadyError ||
  (typeof error === "object" && error !== null && TRANSIENT_CODES.has((error as { code?: unknown }).code));

const deferTransient = (error: unknown): unknown =>
  error instanceof BridgeMirrorDeferredError || !isNatsCarrierTransient(error) ? error : new NatsCarrierDeferredError(error);

export class NatsCarrierUncertainError extends Error {
  override readonly name = "NatsCarrierUncertainError";
  constructor(readonly envelopeId: string) {
    super(`NATS publish outcome is unknown for ${envelopeId}; the same envelope is resent on the next pass`);
  }
}

/** Resolved KV presence, only when live with a machine and an owner epoch. */
export const natsPresenceEntry = (port: Pick<BridgeNatsPort, "resolve">, id: string): NatsPresenceEntry | undefined => {
  const resolved = port.resolve(id);
  if (!resolved?.route.live || !resolved.route.machine || !resolved.route.ownerIncarnation || resolved.identity.id !== id) return undefined;
  return { ...resolved, identity: copy(resolved.identity), route: { ...resolved.route, capabilities: [...resolved.route.capabilities] } };
};

const copy = (identity: MeshIdentity): MeshIdentity => ({
  id: identity.id, name: identity.name, kind: identity.kind,
  ...(typeof identity.sessionId === "string" ? { sessionId: identity.sessionId } : {}),
});

const MESH_KIND: Readonly<Record<string, MeshIdentity["kind"] | undefined>> = { root: "main", actor: "actor", agent: "agent" };

/**
 * Native, live file presence: root hosts from StoreBridgeSide's own snapshot (never a mirror) and
 * actor participants whose owner is one of those hosts. One snapshot serves calls within its TTL.
 * view() is the same snapshot as L3's BridgeNatsPresenceView: one record per participant row,
 * native or mirrored (any link), so the gate can see an id that answers twice.
 */
export class FileCarrierPresence {
  #at = Number.NEGATIVE_INFINITY;
  #entries = new Map<string, FilePresenceEntry>();
  #actors: FabricParticipantRecord[] = [];
  #records = new Map<string, BridgeNatsPresenceRecord[]>();

  constructor(readonly side: StoreBridgeSide, readonly now: () => number = Date.now, readonly ttlMs = FILE_PRESENCE_TTL_MS) {}

  async refresh(force = false): Promise<void> {
    const now = this.now();
    if (!force && now - this.#at < this.ttlMs) return;
    const presence = await this.side.presence();
    const entries = new Map<string, FilePresenceEntry>();
    const hosts = new Map<string, MeshIdentity>();
    for (const host of presence.hosts) {
      const record = host.record;
      if (record.remoteHost !== undefined || record.bridgeVia !== undefined || !(host.expiresAt > now)) continue;
      hosts.set(record.id, record.identity);
      // A Main root: its id, host id and identity id are one; it replies from its own host.
      if (record.identity.id === record.id) entries.set(record.id, { identity: copy(record.identity), hostId: record.id });
    }
    const actors = new Map<string, FabricParticipantRecord | null>();
    const records = new Map<string, BridgeNatsPresenceRecord[]>();
    const store = this.side.store;
    const mirrorHosts = new Map<string, number>();
    for (const entry of store.listAll("topology/hosts/", { fresh: true })) {
      const value = entry.value as Partial<{ id: unknown; remoteHost: unknown; expiresAt: unknown }> | undefined;
      if (typeof value?.id === "string" && typeof value.remoteHost === "string" && typeof value.expiresAt === "number") {
        mirrorHosts.set(value.id, value.expiresAt);
      }
    }
    for (const entry of [...store.listAll(PARTICIPANT_PREFIX, { fresh: true }), ...readParticipantFiles(store.root, { maxAgeMs: 0 })]) {
      const participant = participantFromEntry(entry);
      const kind = participant && MESH_KIND[participant.kind];
      if (participant && kind) {
        const mirroredBy = participant.bridgeVia ?? participant.remoteHost;
        const list = records.get(participant.id) ?? [];
        // The state row and the participant file of one native are one record, not an ambiguity.
        if (!list.some(record => record.mirroredBy === mirroredBy && record.ownerHostId === participant.ownerHostId &&
            record.ownerIdentityId === participant.ownerIdentityId && record.rootId === participant.rootId)) {
          list.push(Object.freeze({
            id: participant.id, name: participant.name, kind, rootId: participant.rootId,
            ...(participant.sessionId !== undefined ? { sessionId: participant.sessionId } : {}),
            ownerHostId: participant.ownerHostId, ownerIdentityId: participant.ownerIdentityId,
            ...(participant.ownerIncarnation !== undefined ? { ownerIncarnation: participant.ownerIncarnation } : {}),
            ...(mirroredBy !== undefined ? { mirroredBy } : {}),
            live: participant.retiredAt === undefined && (mirroredBy === undefined
              ? hosts.get(participant.ownerHostId)?.id === participant.ownerIdentityId
              : (mirrorHosts.get(participant.ownerHostId) ?? 0) > now),
            observedAt: now,
          }));
          records.set(participant.id, list);
        }
      }
      if (!participant || participant.kind !== "actor" || participant.remoteHost !== undefined || participant.bridgeVia !== undefined ||
          participant.retiredAt !== undefined) continue;
      const owner = hosts.get(participant.ownerHostId);
      if (!owner || owner.id !== participant.ownerIdentityId) continue;
      const seen = actors.get(participant.id);
      // Two different records for one actor id: ambiguous, never an identity.
      actors.set(participant.id, seen === undefined || (seen && seen.ownerHostId === participant.ownerHostId &&
        seen.name === participant.name) ? participant : null);
    }
    const live: FabricParticipantRecord[] = [];
    for (const [id, participant] of actors) {
      if (!participant || entries.has(id)) continue;
      entries.set(id, { identity: { id, name: participant.name, kind: "actor" }, hostId: participant.ownerHostId });
      live.push(participant);
    }
    this.#entries = entries;
    this.#actors = live;
    this.#records = records;
    this.#at = now;
  }

  /** L3's file-side presence view over the last snapshot. */
  view(): BridgeNatsPresenceView {
    const records = this.#records;
    return { side: "file", records: id => records.get(id) ?? [], list: () => [...records.values()].flat() };
  }

  file(id: string): FilePresenceEntry | undefined {
    const entry = this.#entries.get(id);
    return entry && { identity: copy(entry.identity), hostId: entry.hostId };
  }

  /** Live native file actors from the last refresh (candidates for the KV mirror, filtered by the allow-list). */
  actors(): readonly FabricParticipantRecord[] { return this.#actors; }
}

const delay = (ms: number): Promise<void> => new Promise(resolve => { setTimeout(resolve, ms).unref?.(); });
const message = (error: unknown): string => error instanceof Error ? error.message : String(error);

export interface NatsCarrierSideOptions {
  /** The link-local cursor; its sender-lapse sidecar uses the same atomic, durable write as MeshBridge. */
  readonly cursorPath: string;
  /** Lane L1's raw NATS port. */
  readonly port: BridgeNatsPort;
  /** The hub's file side (MeshBridge `local`): native presence for outbound identity and actor mirrors. */
  readonly file: StoreBridgeSide;
  /** The link name: MeshBridge remoteName and the remoteHost of NATS mirrors in the file mesh. */
  readonly link: string;
  readonly ledger: NatsCarrierLedger;
  readonly authorize: NatsCarrierAuthorize;
  readonly authorizeTopic: NatsTopicAuthorize;
  /** Which file identities the link mirrors into KV (the allow-list's file entries). Default: none. */
  readonly mirrorAllowed?: (participant: Pick<FabricParticipantRecord, "id" | "kind" | "rootId" | "name">) => boolean;
  /** Add allow-listed native file actors to the mirror set (default true); false when the port lists them itself. */
  readonly mirrorActors?: boolean;
  readonly now?: () => number;
  readonly log?: (line: string) => void;
  readonly resendAttempts?: number;
  readonly resendDelayMs?: number;
  readonly filePresenceTtlMs?: number;
  readonly topicTimeoutMs?: number;
  readonly workingIntervalMs?: number;
  readonly maxInHand?: number;
  /** Bound on an inbound target's KV wait (default/shared maximum 30 s from the event's FIRST lapse). */
  readonly presenceWaitMs?: number;
  /** Only these inbound targets (allow.file) may wait for KV visibility; any other is refused at once. */
  readonly waitForTarget?: (targetId: string) => boolean;
}

interface SenderLeaseLapse {
  readonly sequence: number;
  readonly id: string;
  readonly sourceMachine: string;
  readonly senderId: string;
  readonly startedAt: number;
  readonly budgetMs?: number;
  readonly reason?: string;
  readonly detail?: string;
  /** Written durably before sending the unconfirmed broker TERM. */
  readonly terminalReason?: string;
}

interface InHand {
  delivery: BridgeNatsPortDelivery;
  /** This side's sequence, assigned when MeshBridge first reads past its predecessors. */
  sequence?: number;
  /** The carrier's conversion, cached so a reread presents the same event. */
  event?: MeshEvent;
  settle?: NatsCarrierSettle;
  /** Coalesce an unchanged failed predicate while the delivery is held. */
  senderFailure?: string;
  /** First observation survives a failed write or a temporarily full journal. */
  lapse?: SenderLeaseLapse;
  lapseUnrecorded?: boolean;
}

export class NatsCarrierSide implements NatsCarrierSideContract {
  readonly side = "nats" as const;
  readonly protocolVersion = BRIDGE_PROTOCOL_VERSION;
  readonly port: BridgeNatsPort;
  readonly carrier: NatsBridgeCarrier;
  readonly topics: NatsTopicTranslator;
  readonly filePresence: FileCarrierPresence;
  readonly #file: StoreBridgeSide;
  readonly #link: string;
  readonly #now: () => number;
  readonly #log: (line: string) => void;
  readonly #mirrorAllowed: NonNullable<NatsCarrierSideOptions["mirrorAllowed"]>;
  readonly #mirrorActors: boolean;
  readonly #resendAttempts: number;
  readonly #resendDelayMs: number;
  readonly #workingIntervalMs: number;
  readonly #maxInHand: number;
  readonly #presenceWaitMs: number;
  readonly #senderLapsePath: string;
  #senderLeaseLapses: SenderLeaseLapse[] = [];
  #senderLapsesDirty = false;
  /** In-hand deliveries in stream order, by stream sequence. */
  readonly #inHand = new Map<number, InHand>();
  /** Highest sequence this side has given out; never below a cursor MeshBridge read from. */
  #sequence = 0;
  #abort = new AbortController();
  #pump: Promise<void> | undefined;
  #pumpError: Error | undefined;
  #room: (() => void) | undefined;
  #working: ReturnType<typeof setInterval> | undefined;
  readonly #listeners = new Set<{ changed: (kind: BridgeChange) => void; failed: (error: Error) => void }>();
  #lastMirror: { hosts: BridgePresence["hosts"]; participants: BridgePresence["participants"] } | undefined;
  #fileRenewal: Promise<void> | undefined;
  #fileRenewalPending = false;
  #fenced = false;

  constructor(options: NatsCarrierSideOptions) {
    this.port = options.port;
    this.#file = options.file;
    this.#link = options.link;
    this.#now = options.now ?? Date.now;
    this.filePresence = new FileCarrierPresence(options.file, this.#now, options.filePresenceTtlMs);
    const presence: NatsCarrierPresence = {
      nats: id => natsPresenceEntry(this.port, id),
      natsInbound: id => this.port.resolve(id),
      file: id => this.filePresence.file(id),
    };
    this.carrier = new NatsBridgeCarrier({ link: options.link, machine: options.port.machine, presence, ledger: options.ledger,
      authorize: options.authorize, now: this.#now, ...(options.waitForTarget ? { waitForTarget: options.waitForTarget } : {}) });
    this.topics = new NatsTopicTranslator({ link: options.link, machine: options.port.machine, presence, ledger: options.ledger,
      authorize: options.authorizeTopic, now: this.#now,
      ...(options.topicTimeoutMs !== undefined ? { timeoutMs: options.topicTimeoutMs } : {}) });
    this.#log = options.log ?? (() => undefined);
    this.#mirrorAllowed = options.mirrorAllowed ?? (() => false);
    this.#mirrorActors = options.mirrorActors ?? true;
    this.#resendAttempts = Math.max(1, options.resendAttempts ?? RESEND_ATTEMPTS);
    this.#resendDelayMs = Math.max(0, options.resendDelayMs ?? RESEND_DELAY_MS);
    this.#workingIntervalMs = Math.max(1, options.workingIntervalMs ?? WORKING_INTERVAL_MS);
    this.#maxInHand = Math.max(1, options.maxInHand ?? MAX_IN_HAND);
    this.#presenceWaitMs = Math.min(LEASE_LAPSE_DEFER_MS, Math.max(0, options.presenceWaitMs ?? LEASE_LAPSE_DEFER_MS));
    this.#senderLapsePath = `${options.cursorPath}.sender-lease-lapse`;
    try {
      const saved = JSON.parse(readFileRetrying(this.#senderLapsePath)) as { v: number; lapses: SenderLeaseLapse[] };
      if (saved?.v !== 1 || !Array.isArray(saved.lapses) || saved.lapses.length > this.#maxInHand ||
          !saved.lapses.every(lapse => lapse && Number.isSafeInteger(lapse.sequence) && lapse.sequence > 0 &&
            typeof lapse.id === "string" && lapse.id && typeof lapse.sourceMachine === "string" && lapse.sourceMachine &&
            typeof lapse.senderId === "string" && lapse.senderId && Number.isFinite(lapse.startedAt) &&
            (lapse.budgetMs === undefined || (Number.isFinite(lapse.budgetMs) && lapse.budgetMs >= 0 && lapse.budgetMs <= LEASE_LAPSE_DEFER_MS)) &&
            (lapse.reason === undefined || typeof lapse.reason === "string") &&
            (lapse.detail === undefined || typeof lapse.detail === "string") &&
            (lapse.terminalReason === undefined || typeof lapse.terminalReason === "string"))) {
        throw new Error("Invalid NATS sender lease-lapse metadata");
      }
      this.#senderLeaseLapses = saved.lapses;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; // Never restart a corrupt window.
    }
  }

  // -------------------------------------------------------------------------------------------
  // Inbox: one iterator, in stream order, bounded in hand, kept alive until ACK/TERM.

  #start(): void {
    if (this.#pump || this.#abort.signal.aborted) return;
    const signal = this.#abort.signal;
    this.#working = setInterval(() => {
      for (const held of this.#inHand.values()) {
        try { held.delivery.working(); } catch { /* the ack deadline lapses; the redelivery replaces it */ }
      }
    }, this.#workingIntervalMs);
    this.#working.unref?.();
    this.#pump = (async () => {
      try {
        for await (const delivery of this.port.inbox(signal)) {
          if (signal.aborted) break;
          const known = this.#inHand.get(delivery.streamSequence);
          // A redelivery replaces the delivery handle and keeps the conversion and sequence.
          if (known) known.delivery = delivery;
          else this.#inHand.set(delivery.streamSequence, { delivery });
          this.#notify("events");
          while (!signal.aborted && this.#inHand.size >= this.#maxInHand) {
            await new Promise<void>(resolve => { this.#room = resolve; });
          }
        }
        if (!signal.aborted) throw new Error("NATS inbox ended");
      } catch (error) {
        if (signal.aborted) return;
        this.#pumpError = error instanceof Error ? error : new Error(String(error));
        for (const listener of this.#listeners) listener.failed(this.#pumpError);
      }
    })();
  }

  #notify(kind: BridgeChange): void {
    for (const listener of this.#listeners) listener.changed(kind);
  }

  #release(stream: number, acknowledged = false): void {
    // Only a broker-confirmed ACK proves completion. A missing process-local handle (including
    // after restart), or an unconfirmed TERM, must never erase a delivery's original deadline.
    // Persist this safe reclamation with the next first-lapse write, not on unchanged retries.
    if (acknowledged) this.#senderLeaseLapses = this.#senderLeaseLapses.filter(lapse => lapse.sequence !== stream);
    this.#inHand.delete(stream);
    if (this.#inHand.size < this.#maxInHand && this.#room) {
      const room = this.#room;
      this.#room = undefined;
      room();
    }
  }

  /** In stream order: everything assigned so far, then everything not yet assigned. */
  #ordered(): Array<[number, InHand]> {
    return [...this.#inHand].sort(([a], [b]) => a - b);
  }

  #pending(): number {
    let unassigned = 0;
    for (const held of this.#inHand.values()) if (held.sequence === undefined) unassigned += 1;
    return unassigned;
  }

  // -------------------------------------------------------------------------------------------
  // BridgeSide

  async latestSequence(): Promise<number> {
    this.#start();
    return this.#sequence + this.#pending();
  }

  /** A fresh link starts below every in-hand delivery: nothing in the inbox is skipped unread. */
  async latestCursor(): Promise<BridgeHead> {
    this.#start();
    return { through: this.#sequence };
  }

  async watchdogHead(_timeoutMs: number): Promise<number> {
    if (this.#pumpError) throw this.#pumpError;
    return this.latestSequence();
  }

  subscribeChanges(changed: (kind: BridgeChange) => void, failed: (error: Error) => void): () => void {
    const listener = { changed, failed };
    this.#listeners.add(listener);
    const unwatch = this.port.watchPresence(() => {
      changed("presence");
      this.#fileRenewalPending = true;
      this.#renewFileMirror();
    }, failed);
    this.#start();
    if (this.#pumpError) queueMicrotask(() => failed(this.#pumpError!));
    return () => {
      this.#listeners.delete(listener);
      unwatch();
    };
  }

  /** The live KV watch renews inbound leases independently of a slow bidirectional pass or
   * drain. Coalesce bursts, but resample after changes during a write. Never restamp a cached
   * snapshot or change membership here: only the ordinary full sync installs/removes owners. */
  #renewFileMirror(): void {
    if (this.#fileRenewal || this.#fenced || this.#listeners.size === 0) return;
    this.#fileRenewal = (async () => {
      while (this.#fileRenewalPending && !this.#fenced && this.#listeners.size > 0) {
        this.#fileRenewalPending = false;
        const local = await this.#file.presence();
        // Anchor before the NATS read/queue/write, exactly as MeshBridge's inbound sample does.
        const observedAt = this.#now();
        const claimed = await this.presence();
        if (this.#fenced || this.#listeners.size === 0) return;
        const admitted = admitPresence(claimed, new Set(local.reserved), this.#link);
        await this.#file.renewMirroredLeases(admitted.hosts, observedAt);
      }
    })().catch(error => {
      if (this.#fenced || this.#listeners.size === 0) return;
      if (error instanceof BridgeMirrorDeferredError || isMeshRetryableBusy(error)) {
        // No new authority on failure. The normal sync and the next KV update may retry.
        this.#log(`NATS file mirror renewal deferred: ${message(error)}`);
      } else {
        const failure = error instanceof Error ? error : new Error(message(error));
        for (const listener of this.#listeners) listener.failed(failure);
      }
    }).finally(() => {
      this.#fileRenewal = undefined;
      if (this.#fileRenewalPending) this.#renewFileMirror();
    });
  }

  async presence(): Promise<BridgePresence> {
    try { return await this.port.presence(); } catch (error) { throw deferTransient(error); }
  }

  /**
   * File roots (and allow-listed file actors) into KV under the bridge machine, plus the link's own
   * identity. Only allow-listed identities are mirrored. A partial snapshot adds and renews, never withdraws.
   */
  async mirror(presence: Parameters<BridgeSide["mirror"]>[0], observedAt?: number): Promise<void> {
    await this.filePresence.refresh(true);
    const hostIds = new Set<string>();
    const participants = presence.participants.filter(participant => participant.kind === "root" && this.#mirrorAllowed(participant));
    if (this.#mirrorActors) for (const actor of this.filePresence.actors()) if (this.#mirrorAllowed(actor)) participants.push(actor);
    for (const participant of participants) hostIds.add(participant.ownerHostId);
    const hosts = presence.hosts.filter(host => hostIds.has(host.record.id));
    let set = { hosts, participants };
    const partial = presence.partial === true || (presence.droppedCount ?? 0) > 0 || (presence.dropped?.length ?? 0) > 0;
    if (partial && this.#lastMirror) {
      const ids = new Set(hosts.map(host => host.record.id));
      const pids = new Set(participants.map(participant => participant.id));
      set = { hosts: [...hosts, ...this.#lastMirror.hosts.filter(host => !ids.has(host.record.id))],
        participants: [...participants, ...this.#lastMirror.participants.filter(participant => !pids.has(participant.id))] };
    }
    try {
      await this.port.mirror({ ...set, linkIdentity: bridgeNatsLinkIdentity(this.#link) }, observedAt ?? this.#now());
    } catch (error) { throw deferTransient(error); }
    if (!partial) this.#lastMirror = set;
  }

  /**
   * MeshBridge's restart recovery asks which hub events already reached this side. Envelope ids are
   * deterministic and every destination dedupes them (broker window, then the owner's seen claim
   * by mapped commandId), so re-presenting is the recovery: nothing is listed.
   */
  async bridgedIds(after: number): Promise<BridgedIds> { return { ids: [], through: after, done: true }; }

  async fence(): Promise<void> {
    this.#fenced = true;
    await this.#fileRenewal;
    await this.port.fence();
  }
  async withdraw(): Promise<void> {
    await this.fence();
    await this.port.withdraw();
  }

  close(_error?: Error): void {
    this.#fenced = true;
    this.#abort.abort();
    clearInterval(this.#working);
    this.#room?.();
    void this.port.close().catch(() => undefined);
  }

  closeGracefully(error?: Error): void { this.close(error); }

  /** toNats: one event MeshBridge admitted toward the NATS side. Resolves only after the PubAck. */
  async publish(event: BridgePublish): Promise<{ sequence: number }> {
    // Identity and the allow gate read the file side's presence: at most one TTL old, never stale.
    await this.filePresence.refresh();
    let plan: NatsOutbound;
    try {
      plan = event.topic === BRIDGE_NATS_CONTROL_TOPIC || event.topic === BRIDGE_NATS_ACK_TOPIC
        ? await this.carrier.outbound(event) : await this.topics.outbound(event);
    } catch (error) {
      // A refused command from a verified file sender is answered (notRun, stable code) before MeshBridge drops it.
      if (error instanceof NatsCarrierRefusal && error.answer) await this.#answerFileSender(error.answer);
      throw deferTransient(error);
    }
    if (plan.kind === "absorb") {
      this.#log(`toNats: ${event.topic} ${event.data.bridge.id} needs no send: ${plan.reason}`);
      return { sequence: 0 };
    }
    if (plan.kind === "answer") {
      // A recorded answer replayed to the source: it must land (a retry re-presents the event), never be logged away.
      await this.#writeFileAnswer(plan.answer);
      return { sequence: 0 };
    }
    let sent: TransportSent<Record<string, unknown>>;
    try { sent = await this.#send(plan.envelope); } catch (error) { throw deferTransient(error); }
    // An ACK going back to a NATS sender is that command's forward; a command is now sent.
    if (plan.settle) await this.carrier.settle(plan.settle, true);
    else if (plan.record?.direction === "toNats" && plan.envelope.kind === "command") await this.carrier.sent(plan.record, sent.sequence);
    return { sequence: sent.sequence };
  }

  /**
   * The refusal ACK, through the local side's own bridged publication: durable, deduped by its stamp, and
   * committed only while this link still holds the owner it speaks for (the held fence).
   */
  async #answerFileSender(answer: NatsCarrierFileAnswer): Promise<void> {
    try { await this.#writeFileAnswer(answer); }
    catch (error) { this.#log(`toNats: could not write ${answer.stampId}: ${message(error)}`); }
  }

  #writeFileAnswer(answer: NatsCarrierFileAnswer): Promise<{ sequence: number }> {
    return this.#file.publish({ topic: BRIDGE_NATS_ACK_TOPIC, kind: answer.data.accepted === true ? "accepted" : "rejected",
      from: answer.from, to: answer.to, data: { ...answer.data, bridge: { from: this.#link, id: answer.stampId } } }, [answer.from.id]);
  }

  /**
   * toFile: deliveries after `after`, converted. Reading from `after` is MeshBridge's statement that
   * everything at or below it is durably appended or deliberately dropped: those are ACKed first.
   */
  async read(after: number): Promise<BridgeRead> {
    this.#start();
    if (this.#pumpError) throw this.#pumpError;
    await this.#ackThrough(after);
    // A cursor saved by an earlier process: new deliveries are numbered above it, never below.
    if (after > this.#sequence) this.#sequence = after;
    const events: MeshEvent[] = [];
    let through = after;
    for (const [stream, held] of this.#ordered()) {
      if (held.sequence !== undefined && held.sequence <= after) continue;
      try {
        const event = await this.#inbound(stream, held);
        if (event) events.push(event);
        else {
          through = Math.max(through, held.sequence ?? after); // A rechecked cached event may now be deliberately refused.
          continue;
        }
      } catch (error) {
        // Stop in order before the delivery that failed; it is retried on the next read.
        if (!events.length) throw deferTransient(error);
        this.#log(`toFile: stopped before stream sequence ${stream}: ${message(error)}`);
        break;
      }
      through = Math.max(through, held.sequence!);
    }
    return { events, through };
  }

  async #inbound(stream: number, held: InHand): Promise<MeshEvent | undefined> {
    const delivery = held.delivery;
    const envelopeId = typeof delivery.envelope?.id === "string" ? delivery.envelope.id : `stream:${stream}`;
    const retained = held.lapse ?? this.#savedLapse(delivery);
    const now = this.#now();
    if (retained) this.#saveSenderLapses(); // Retry an uncertain write before ANY admission.
    // Check BEFORE conversion/ledger claim, independently of recovered source/target health.
    // A cached event already belongs to MeshBridge's append-fence/cursor deadline path.
    if (retained && (retained.terminalReason || (!held.event && this.#lapseExpired(retained, now)))) {
      this.#senderLapseSince(delivery);
      await this.#termLapse(stream, held, retained);
      return undefined;
    }
    // Even a converted event must re-check LIVE source presence when the bridge retries it.
    await this.filePresence.refresh();
    let plan: NatsInbound;
    try { plan = await this.carrier.inbound(carrierDeliveryFromPort(delivery)); }
    catch (error) {
      if ((error as { code?: unknown })?.code !== "FABRIC_DIRECTORY_NOT_READY") throw error;
      if (held.senderFailure !== "not-ready") {
        this.#log("toFile: sender-live wait predicate=not-ready remainingAuthorityMs=unknown");
        held.senderFailure = "not-ready";
      }
      // Directory readiness is another lapse, not an unbounded escape around the event's
      // durable first-lapse deadline. No unverified sender identity belongs in this diagnostic.
      plan = { kind: "wait", reason: "directory is not ready", senderLeaseLapse: true,
        senderDiagnostic: { failure: "not-ready" } };
    }
    const diagnostic = plan.kind === "wait" || plan.kind === "refused" ? plan.senderDiagnostic : undefined;
    const detail = diagnostic ? `; predicate=${diagnostic.failure}` +
      (diagnostic.senderId ? ` sender=${diagnostic.senderId}` : "") +
      ` remainingAuthorityMs=${diagnostic.remainingAuthorityMs ?? "unknown"}` : "";
    if (diagnostic && plan.kind === "wait" && held.senderFailure !== diagnostic.failure) {
      this.#log(`toFile: sender-live defer${detail}`);
      held.senderFailure = diagnostic.failure;
    }
    switch (plan.kind) {
      case "event": {
        if (held.event) return held.event; // Keep its original cursor sequence and conversion.
        // Record the owner's answer BEFORE the file append that hands it to its sender: a same-key resend
        // racing that append must find it recorded (replayAck), never wait for an ACK already consumed.
        // The delivery is acked, and the answer marked forwarded, only after the append is durable.
        if (plan.settle) await this.carrier.settle(plan.settle);
        held.sequence = ++this.#sequence;
        held.event = { ...plan.event, sequence: held.sequence };
        if (plan.settle) held.settle = plan.settle;
        return held.event;
      }
      case "reply":
        await this.#send(plan.envelope);
        this.#log(`toFile: answered ${envelopeId} for the hop: ${plan.reason}`);
        await delivery.ack();
        this.#release(stream, true);
        return undefined;
      case "settled": {
        if (plan.settle) await this.carrier.settle(plan.settle);
        if (plan.record?.origin === "topic") {
          if (plan.ack?.accepted !== true) {
            this.#log(`toNats: topic followUp for ${plan.record.sourceEventId} to ${plan.record.targetId} was not accepted: ` +
              `${typeof plan.ack?.error === "string" ? plan.ack.error.slice(0, 200) : "no reason given"}`);
          }
          const retry = await this.topics.retry(plan.record, plan.ack);
          if (retry?.kind === "send") {
            const resent = await this.#send(retry.envelope);
            if (retry.record) await this.carrier.sent(retry.record, resent.sequence);
          }
        }
        await delivery.ack();
        this.#release(stream, true);
        return undefined;
      }
      case "wait": {
        const budget = plan.senderLeaseLapse ? LEASE_LAPSE_DEFER_MS : this.#presenceWaitMs;
        const startedAt = this.#now(); // Anchor before the optional broker reclamation lookup.
        const ackFloor = this.#savedLapse(delivery) ? 0 : await this.#senderLapseAckFloor();
        this.#senderLapseSince(delivery, startedAt, { budgetMs: budget, reason: plan.reason, detail }, ackFloor);
        const lapse = held.lapse!;
        if (!this.#lapseExpired(lapse, this.#now())) throw new NatsCarrierDeferredError(new Error(plan.reason));
        await this.#termLapse(stream, held, lapse);
        return undefined;
      }
      case "refused":
        this.#log(`toFile: refused ${envelopeId} from ${delivery.sourceMachine}: ${plan.reason}${detail}`);
        await delivery.term(plan.reason);
        this.#release(stream);
        return undefined;
    }
  }

  #savedLapse(delivery: BridgeNatsPortDelivery): SenderLeaseLapse | undefined {
    return this.#senderLeaseLapses.find(lapse => lapse.sequence === delivery.streamSequence && lapse.id === delivery.envelope.id &&
      lapse.sourceMachine === delivery.sourceMachine && lapse.senderId === delivery.envelope.from.id);
  }

  #lapseExpired(lapse: SenderLeaseLapse, now: number): boolean {
    return lapse.startedAt > now || now - lapse.startedAt >= (lapse.budgetMs ?? LEASE_LAPSE_DEFER_MS);
  }

  #saveSenderLapses(): void {
    if (!this.#senderLapsesDirty) return;
    writeJsonAtomic(this.#senderLapsePath, { v: 1, lapses: this.#senderLeaseLapses }, { mode: 0o600, durable: true });
    this.#senderLapsesDirty = false; // A failed post-rename barrier is NOT a durable receipt.
  }

  async #termLapse(stream: number, held: InHand, lapse: SenderLeaseLapse): Promise<void> {
    const reason = lapse.terminalReason ?? lapse.reason ?? "sender lease-lapse deadline expired";
    if (!lapse.terminalReason) {
      const terminal = { ...lapse, terminalReason: reason };
      held.lapse = terminal;
      const saved = this.#savedLapse(held.delivery);
      if (saved) {
        this.#senderLeaseLapses = this.#senderLeaseLapses.map(entry => entry === saved ? terminal : entry);
        this.#senderLapsesDirty = true;
      }
    }
    this.#saveSenderLapses(); // Durable terminal disposition BEFORE TERM, unless the cap forced memory-only retention.
    this.#log(`toFile: refused ${held.delivery.envelope.id} from ${held.delivery.sourceMachine} after ` +
      `${lapse.budgetMs ?? LEASE_LAPSE_DEFER_MS} ms: ${reason}${lapse.detail ?? ""}`);
    await held.delivery.term(reason);
    this.#release(stream);
  }

  async #senderLapseAckFloor(): Promise<number> {
    if (this.#senderLeaseLapses.length < this.#maxInHand || !this.port.ackFloor) return 0;
    try {
      const floor = await this.port.ackFloor();
      if (Number.isSafeInteger(floor) && floor >= 0) return floor;
    } catch (error) {
      // An unavailable floor cannot prove settlement, but must not wedge new deliveries.
      this.#log(`toFile: sender lapse reclamation unavailable: ${message(error)}`);
    }
    return 0;
  }

  /** Bounded link-local retention metadata, not a spool; never take the mesh state lock.
   * Reclaim only terminal entries proven complete by the consumer ack floor, NOT by age
   * or process-local absence. At capacity, keep the new window in hand without wedging.
   * Recovery without its tombstone checks live presence normally; only a renewed WAIT
   * uses the immutable broker publish time, so healthy old redeliveries are never aged out. */
  #senderLapseSince(delivery: BridgeNatsPortDelivery, startedAt = this.#now(),
    context: Pick<SenderLeaseLapse, "budgetMs" | "reason" | "detail"> = {}, ackFloor = 0): number {
    const held = this.#inHand.get(delivery.streamSequence)!;
    const saved = this.#savedLapse(delivery);
    if (saved) {
      // Diagnostics may change (sender -> directory gap) without renewing or rewriting
      // the first timestamp. Persist the latest reason only when marking terminal.
      held.lapse = { ...saved, ...context };
      this.#saveSenderLapses();
      return saved.startedAt;
    }
    const now = this.#now();
    // Only a proven WAIT/append lapse enters here. Never age out the healthy path.
    // A lost journal cannot renew a lapsed redelivery. Missing/future broker metadata
    // conservatively expires that WAIT, without trusting sender-controlled createdAt.
    const publishedAt = delivery.publishedAt;
    const anchor = delivery.attempt > 1
      ? (typeof publishedAt === "number" && Number.isFinite(publishedAt) && publishedAt <= now ? publishedAt : now - LEASE_LAPSE_DEFER_MS)
      : now;
    const lapse = held.lapse ?? { sequence: delivery.streamSequence, id: delivery.envelope.id,
      sourceMachine: delivery.sourceMachine, senderId: delivery.envelope.from.id,
      startedAt: Math.min(startedAt, now, anchor), ...context };
    held.lapse = lapse; // Keep the SAME first timestamp even if the write/barrier/cap fails.
    const lapses = [...this.#senderLeaseLapses];
    if (lapses.length >= this.#maxInHand) {
      const reclaimable = lapses.filter(entry => entry.terminalReason && entry.sequence <= ackFloor)
        .sort((a, b) => a.startedAt - b.startedAt || a.sequence - b.sequence)[0];
      if (reclaimable) lapses.splice(lapses.indexOf(reclaimable), 1);
      else {
        if (!held.lapseUnrecorded) this.#log("toFile: sender lapse journal full with no broker-confirmed reclamation; " +
          "retaining this lapse in memory only; restart uses live presence and broker publish time");
        held.lapseUnrecorded = true;
        this.#saveSenderLapses();
        return lapse.startedAt;
      }
    }
    held.lapseUnrecorded = false;
    this.#senderLeaseLapses = [...lapses, lapse];
    this.#senderLapsesDirty = true;
    this.#saveSenderLapses();
    return lapse.startedAt;
  }

  /** Share the first-lapse time in either order: carrier -> append fence, or append fence -> carrier.
   * This is retention metadata only, never authority, and the matching record is not rewritten. */
  retainLeaseLapse(event: MeshEvent, startedAt?: number): number | undefined {
    const held = [...this.#inHand.values()].find(held => held.event?.id === event.id && held.sequence === event.sequence);
    if (!held) return startedAt;
    if (startedAt !== undefined) return this.#senderLapseSince(held.delivery, startedAt);
    return (held.lapse ?? this.#savedLapse(held.delivery))?.startedAt;
  }

  /** Broker ACK after the durable append: every in-hand delivery whose sequence MeshBridge read past. */
  async #ackThrough(after: number): Promise<void> {
    for (const [stream, held] of this.#ordered()) {
      if (held.sequence === undefined || held.sequence > after) continue;
      try {
        await held.delivery.ack();
      } catch (error) {
        // The append is durable; a lost broker ACK is only a redelivery the dedupe receipts absorb.
        this.#log(`toFile: broker ACK of stream sequence ${stream} failed (retried next read): ${message(error)}`);
        continue;
      }
      if (held.settle) {
        try { await this.carrier.settle(held.settle, true); }
        catch (error) { this.#log(`toFile: ledger settle for stream sequence ${stream} failed: ${message(error)}`); }
      }
      this.#release(stream, true);
    }
  }

  /** One envelope, PubAck-confirmed. An uncertain outcome is resent with the same id and bytes, then thrown. */
  async #send(envelope: NatsCarrierEnvelope): Promise<TransportSent<Record<string, unknown>>> {
    let last: unknown;
    for (let attempt = 0; attempt < this.#resendAttempts; attempt++) {
      if (attempt > 0) await delay(this.#resendDelayMs * 2 ** (attempt - 1));
      try {
        return await this.port.send(envelope);
      } catch (error) {
        if (!(error instanceof TransportUncertainError || (error as { code?: unknown } | undefined)?.code === "FABRIC_TRANSPORT_UNCERTAIN")) throw error;
        last = error;
      }
    }
    throw Object.assign(new NatsCarrierUncertainError(envelope.id), { cause: last });
  }
}

export interface NatsFileLinkOptions extends Omit<NatsCarrierSideOptions, "file"> {
  /** The hub's file mesh. */
  readonly store: MeshStore;
  /** This hub's bridge name (MeshBridge localName). */
  readonly localName: string;
  readonly cursorPath: string;
  /** Further MeshBridge options (alarms, watchdog, rates); never `relay` in PR 1. */
  readonly bridge?: Omit<MeshBridgeOptions, "localName" | "remoteName" | "local" | "remote" | "cursorPath" | "relay" | "log">;
}

/**
 * The link: the existing MeshBridge with the file store as its local StoreBridgeSide (peer = the
 * link name) and the NATS carrier side as its remote. No relay hook: spoke relays are PR 2, and a
 * missing hook fails cross-spoke commands closed in MeshBridge.
 */
export const createNatsFileLink = (options: NatsFileLinkOptions): { bridge: MeshBridge; local: StoreBridgeSide; remote: NatsCarrierSide } => {
  const local = new StoreBridgeSide(options.store, options.link, options.now ?? Date.now, true);
  const remote = new NatsCarrierSide({ ...options, file: local });
  const bridge = new MeshBridge({
    ...options.bridge, localName: options.localName, remoteName: options.link, local, remote, cursorPath: options.cursorPath,
    ...(options.log ? { log: options.log } : {}),
  });
  return { bridge, local, remote };
};
