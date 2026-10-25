// A' bridge, the raw NATS port (smarty-dev#15310, lane L1): L3's `BridgeNatsPort`
// (bridge-nats-types.ts), and only that. L2's NatsCarrierSide is the BridgeSide MeshBridge sees; it
// consumes this port and owns the file event <-> TransportEnvelope mapping, deterministic ids,
// resends, and ACK only after the durable file append.
//
// The port: KV presence read (roots for the file mirror; every record for the identity gate), the
// KV mirror (file roots, allow-listed file actors and the link's own identity, with leases), send,
// and the machine-wide inbox (`fabric.in.<bridgeMachine>.>`, every hostId) with ack/term.
//
// Trust: a delivery's source machine is the credential-bound last subject token, cross-checked
// against the payload; the destination must be this machine; nothing in a body names a machine.
// A command goes out only FROM an identity this link holds live in KV (a NATS receiver would refuse
// anything else as unverified); the link never publishes into its own machine's inbox.
import { randomUUID } from "node:crypto";
import { performance } from "node:perf_hooks";
import type { MeshIdentity } from "./event-log.js";
import type { BridgePresence } from "./bridge.js";
import {
  bridgeNatsLinkIdentity,
  type BridgeNatsMirrorSet,
  type BridgeNatsPort,
  type BridgeNatsPortDelivery,
  type BridgeNatsPortResolved,
  type BridgeNatsPresenceRecord,
  type BridgeNatsPresenceView,
} from "./bridge-nats-types.js";
import { controlBodyFingerprint } from "./control-idempotency.js";
import { NatsBridgePresence, type NatsBridgePresenceOptions } from "./bridge-nats-presence.js";
import type { NatsFileActorRead } from "./bridge-nats-actors.js";
import type { MeshStore } from "./store.js";
import type { FabricParticipantRecord } from "../topology/types.js";
import type { ConsumerMessages } from "@nats-io/jetstream";
import type { NatsConnectionHandle } from "../transport/nats/connection.js";
import { DirectoryNotReadyError, TransportUncertainError, type FabricTransport, type TransportEnvelope, type TransportSent } from "../transport/types.js";
import { hashToken } from "../transport/nats/subjects.js";

const MACHINE_TOKEN = /^[a-z0-9][a-z0-9-]{0,31}$/;
const SELF_GROUP = "self";
const SELF_CHECK_MS = 5_000;
/** One lease TTL plus the safety margin: the longest a predecessor's lease can outlive its last renewal. */
const SELF_TAKEOVER_MS = 17_000;
const SELF_TAKEOVER_POLL_MS = 500;

const validMessageId = (id: unknown): id is string =>
  typeof id === "string" && id.length > 0 && id === id.trim() && !/[\u0000-\u001f\u007f]/.test(id);
const record = (value: unknown): Record<string, unknown> | undefined =>
  typeof value === "object" && value !== null && !Array.isArray(value) ? value as Record<string, unknown> : undefined;
const coded = (message: string, code: string): Error => Object.assign(new Error(message), { code });

/** The JetStream message surface the port needs (a subset of @nats-io/jetstream JsMsg). */
export interface NatsBridgeInboxMessage {
  readonly subject: string;
  readonly data: Uint8Array;
  readonly info: { readonly streamSequence: number; readonly deliveryCount: number };
  ackAck(): Promise<boolean>;
  nak(delayMs?: number): void;
  term(reason?: string): void;
  working(): void;
}

/** One durable consumer over this machine's whole inbox stream. */
export interface NatsBridgeInboxSource {
  /** Messages in stream order; ends on abort; throws only on a terminal setup refusal. */
  consume(signal: AbortSignal): AsyncIterable<NatsBridgeInboxMessage>;
  /** False while a reconnect awaits USER.INFO re-verification: settle nothing then. */
  isVerified(): boolean;
}

/** BridgeNatsPortDelivery plus what the decode already proved. The port never settles it itself. */
export interface NatsBridgeDelivery extends BridgeNatsPortDelivery {
  /** The resolved wire envelope, exactly as admitted (body and createdAt included). */
  readonly envelope: TransportEnvelope & { readonly body: unknown; readonly createdAt: number };
  /** The addressed owner host on this machine (a mirrored file root's or actor's host, or the link's own). */
  readonly toHostId: string;
  /** Commands only: controlBodyFingerprint over the raw wire body and principal, before any hydration. */
  readonly bodyDigest?: string;
}

/** Strict decode: the subject, payload and destination must agree, as the native NATS inbox requires. */
export const decodeNatsBridgeMessage = (machine: string, message: Pick<NatsBridgeInboxMessage, "subject" | "data">):
  { envelope: NatsBridgeDelivery["envelope"]; sourceMachine: string; bodyDigest?: string } => {
  let value: unknown;
  try { value = JSON.parse(Buffer.from(message.data).toString("utf8")); } catch { throw new Error("invalid transport envelope JSON"); }
  const wire = record(value);
  const envelope = record(wire?.envelope);
  const to = record(envelope?.to);
  const from = record(envelope?.from);
  const sourceMachine = wire?.sourceMachine;
  if (!wire || wire.format !== 1 || typeof sourceMachine !== "string" || !MACHINE_TOKEN.test(sourceMachine) || !envelope ||
    !validMessageId(envelope.id) || (envelope.kind !== "command" && envelope.kind !== "ack") ||
    typeof envelope.targetId !== "string" || envelope.targetId.length === 0 ||
    typeof envelope.operation !== "string" || envelope.operation.length === 0 || !from || typeof from.id !== "string" ||
    !to || typeof to.machine !== "string" || typeof to.hostId !== "string" || to.hostId.length === 0 ||
    typeof envelope.createdAt !== "number" || !Number.isFinite(envelope.createdAt) || !("body" in envelope)) {
    throw new Error("invalid transport envelope");
  }
  const parts = message.subject.split(".");
  if (parts.length !== 7 || parts[0] !== "fabric" || parts[1] !== "in" || parts[2] !== machine ||
    parts[3] !== hashToken(to.hostId) || parts[4] !== (envelope.kind === "command" ? "cmd" : "ack") ||
    parts[5] !== hashToken(envelope.targetId) || parts[6] !== sourceMachine) throw new Error("transport subject/payload mismatch");
  if (to.machine !== machine) throw new Error("transport destination mismatch");
  const resolved = JSON.parse(JSON.stringify(envelope)) as NatsBridgeDelivery["envelope"];
  return { envelope: resolved, sourceMachine,
    ...(resolved.kind === "command" ? { bodyDigest: controlBodyFingerprint(resolved.body, resolved.principal) } : {}) };
};

/**
 * The real source: one durable (`br_<hash(link identity)>`) over `fabric.in.<machine>.>`, created
 * with the transport inbox's own consumer policy. The bridge machine is the link's alone, so this
 * durable sees every mirrored host's commands and ACKs.
 */
export const natsBridgeInboxSource = (connection: NatsConnectionHandle, options: { machine: string; link: string }): NatsBridgeInboxSource => {
  if (!MACHINE_TOKEN.test(options.machine)) throw new TypeError("Invalid NATS bridge machine");
  const stream = `FABRIC_IN_${options.machine}`;
  const policy = {
    stream, durable: `br_${hashToken(bridgeNatsLinkIdentity(options.link).id)}`, filterSubject: `fabric.in.${options.machine}.>`,
    description: JSON.stringify({ fabricBridgeLink: options.link, machine: options.machine }), maxAckPending: 128,
  };
  return {
    isVerified: () => connection.isVerified(),
    async *consume(signal) {
      const [{ ensureTransportConsumer }, { classifyNatsInboxTerminalError }] = await Promise.all([
        import("../transport/nats/provision.js"), import("../transport/nats/inbox.js")]);
      const { Readable } = await import("node:stream");
      for (let attempt = 0; !signal.aborted; attempt++) {
        if (attempt > 0) {
          const delay = Math.min(5_000, 250 * 2 ** Math.min(attempt - 1, 5));
          await new Promise<void>(resolve => {
            const done = (): void => { clearTimeout(timer); signal.removeEventListener("abort", done); resolve(); };
            const timer = setTimeout(done, delay);
            signal.addEventListener("abort", done, { once: true });
          });
          if (signal.aborted) return;
        }
        const queue = new Readable({ objectMode: true, read() {} });
        const generation = new AbortController();
        const stop = (): void => { if (!queue.destroyed && !queue.readableEnded) queue.push(null); };
        signal.addEventListener("abort", stop, { once: true });
        generation.signal.addEventListener("abort", stop, { once: true });
        let messages: ConsumerMessages | undefined;
        let statusTask: Promise<void> | undefined;
        try {
          try {
            if (!connection.isVerified()) throw new Error("NATS connection is not verified");
            const info = await ensureTransportConsumer(connection.manager.consumers, policy);
            if (info.config.filter_subject !== policy.filterSubject || info.config.description !== policy.description) {
              throw coded("NATS bridge inbox owner/filter mismatch", "FABRIC_TRANSPORT_INVALID");
            }
            const consumer = await connection.js.consumers.get(stream, policy.durable);
            messages = await consumer.consume({ max_messages: 16, callback: msg => {
              // A native reconnect may replay SUBs before USER.INFO: never admit those.
              if (connection.isVerified() && !signal.aborted && !generation.signal.aborted && !queue.destroyed) queue.push(msg);
            } });
            void messages.closed().then(error => {
              if (error && !queue.destroyed) queue.destroy(error instanceof Error ? error : new Error("NATS bridge inbox failed"));
              else stop();
            }, error => { if (!queue.destroyed) queue.destroy(error instanceof Error ? error : new Error("NATS bridge inbox failed")); });
            statusTask = (async () => {
              for await (const status of messages!.status()) {
                if (signal.aborted || generation.signal.aborted) return;
                if (["heartbeats_missed", "consumer_deleted", "consumer_not_found", "stream_not_found", "reset",
                  "ordered_consumer_recreated", "no_responders"].includes(status.type)) { generation.abort(); return; }
              }
            })().catch(() => { generation.abort(); });
            for await (const message of queue as AsyncIterable<NatsBridgeInboxMessage>) {
              if (signal.aborted || generation.signal.aborted) break;
              if (!connection.isVerified()) { generation.abort(); break; }
              attempt = 0;
              yield message;
            }
          } catch (error) {
            if (signal.aborted) return;
            const terminal = classifyNatsInboxTerminalError(error);
            if (terminal) throw terminal;
          }
        } finally {
          signal.removeEventListener("abort", stop);
          if (messages) await messages.close().catch(() => undefined);
          await statusTask;
          queue.destroy();
        }
      }
    },
  };
};

export interface NatsBridgePortOptions extends Omit<NatsBridgePresenceOptions, "transport"> {
  /** The bridge machine's NATS transport; its hostId is the link identity's host (replyTo of link commands). */
  readonly transport: FabricTransport;
  readonly inbox: NatsBridgeInboxSource;
  /** The link's own identity (default bridgeNatsLinkIdentity(link)). */
  readonly identity?: MeshIdentity;
  /** One fresh listing of native file actors (bridge-nats-actors.ts); absent = actors come only in the mirror set. */
  readonly fileActors?: () => NatsFileActorRead;
  /** How long start() waits for a predecessor's link identity lease to expire (default one TTL plus margin). */
  readonly takeoverMs?: number;
  /** Runs once at the end of close(): the factory closes the transport and inbox connection it opened. */
  readonly onClose?: () => Promise<void>;
}

/** The fields of an allow entry the presence admission reads (L3's BridgeNatsAllowEntryInput and L5's entry both fit). */
export type NatsBridgeAllowLike =
  | { readonly kind: "root"; readonly id: string; readonly machine?: string }
  | { readonly kind: "actor"; readonly rootId: string; readonly name: string; readonly id?: string };

/**
 * Presence admission from the link config's allow section: `allow.file` names what may be held in
 * KV (toNats), `allow.nats` what may be mirrored into the file mesh (toFile). Exact ids only; an
 * actor by its owner root and name (and its id when pinned). A NATS root pinned to a machine is
 * admitted only from that machine.
 */
export const natsBridgeAdmit = (allow: { readonly file?: readonly NatsBridgeAllowLike[]; readonly nats?: readonly NatsBridgeAllowLike[] }) => {
  const file = Object.freeze([...(allow.file ?? [])].map(entry => Object.freeze({ ...entry })));
  const nats = Object.freeze([...(allow.nats ?? [])].map(entry => Object.freeze({ ...entry })));
  return (record: FabricParticipantRecord, direction: "toNats" | "toFile", machine?: string): boolean =>
    (direction === "toNats" ? file : nats).some(entry => entry.kind === "root"
      ? record.kind === "root" && record.id === entry.id && record.rootId === entry.id &&
        (direction === "toNats" || entry.machine === undefined || entry.machine === machine)
      : direction === "toNats" && record.kind === "actor" && record.rootId === entry.rootId && record.name === entry.name &&
        (entry.id === undefined || entry.id === record.id));
};

const meshKind = (kind: FabricParticipantRecord["kind"]): MeshIdentity["kind"] | undefined =>
  kind === "root" ? "main" : kind === "actor" ? "actor" : kind === "agent" ? "agent" : undefined;

const isPartial = (set: { hosts?: unknown; participants?: unknown; partial?: unknown; droppedCount?: unknown; dropped?: unknown }): boolean =>
  set.partial === true || (typeof set.droppedCount === "number" && set.droppedCount > 0) ||
  (Array.isArray(set.dropped) && set.dropped.length > 0) || !Array.isArray(set.hosts) || !Array.isArray(set.participants);

export class NatsBridgePort implements BridgeNatsPort {
  readonly machine: string;
  readonly link: string;
  readonly identity: MeshIdentity;
  /** Presence both ways and the KV mirror writer (bridge-nats-presence.ts). */
  readonly kv: NatsBridgePresence;
  readonly #transport: FabricTransport;
  readonly #inbox: NatsBridgeInboxSource;
  readonly #now: () => number;
  readonly #abort = new AbortController();
  readonly #takeoverMs: number;
  #selfRecord: FabricParticipantRecord | undefined;
  #selfTimer: ReturnType<typeof setInterval> | undefined;
  #selfPass: Promise<void> | undefined;
  #inboxActive = false;
  #closed = false;

  constructor(readonly options: NatsBridgePortOptions) {
    const { transport } = options;
    if (transport.kind !== "nats" || !MACHINE_TOKEN.test(transport.self.machine) || typeof transport.self.hostId !== "string" ||
      transport.self.hostId.length === 0) throw new TypeError("The NATS bridge port needs the bridge machine's NATS transport");
    this.machine = transport.self.machine;
    this.link = options.link;
    this.identity = Object.freeze({ ...(options.identity ?? bridgeNatsLinkIdentity(options.link)) });
    this.#transport = transport;
    this.#inbox = options.inbox;
    this.#now = options.now ?? Date.now;
    this.#takeoverMs = options.takeoverMs ?? SELF_TAKEOVER_MS;
    const monotonic = options.monotonic ?? (() => performance.now());
    this.kv = new NatsBridgePresence({ ...options, transport, now: this.#now, monotonic });
  }

  /** Start the KV watches, wait for both snapshots, then hold the link's own identity live in KV. */
  async start(signal?: AbortSignal): Promise<void> {
    if (this.#closed) throw new Error("NATS bridge port is closed");
    this.kv.start();
    await this.kv.whenReady(signal);
    // A predecessor this process cannot prove gone (another boot or host, or still alive) keeps its
    // lease until the broker expires it: retry for one lease TTL plus margin, then refuse to start.
    const deadline = performance.now() + this.#takeoverMs;
    for (;;) {
      try { await this.#ensureSelf(true); break; }
      catch (error) {
        const code = (error as { code?: unknown } | null)?.code;
        if (code !== "FABRIC_BRIDGE_NATS_LEASE_CONFLICT" || performance.now() >= deadline || signal?.aborted) throw error;
        this.options.log?.(`nats port: link identity ${this.identity.id} still leased by a predecessor; waiting for it to expire`);
        await new Promise<void>(resolve => {
          const done = (): void => { clearTimeout(timer); signal?.removeEventListener("abort", done); resolve(); };
          const timer = setTimeout(done, SELF_TAKEOVER_POLL_MS);
          signal?.addEventListener("abort", done, { once: true });
        });
      }
    }
    this.#selfTimer ??= setInterval(() => { void this.#ensureSelf(false).catch(() => undefined); }, SELF_CHECK_MS);
    this.#selfTimer.unref?.();
  }

  /** The link's own KV participant: a root that is never a target (no capabilities), live while open. */
  get selfRecord(): Readonly<FabricParticipantRecord> | undefined { return this.#selfRecord; }

  #ensureSelf(throwOnRefusal: boolean): Promise<void> {
    if (this.#selfPass) return this.#selfPass;
    const pass = (async () => {
      if (this.#closed || this.kv.writer.fenced) return;
      const id = this.identity.id;
      if (this.#selfRecord && this.kv.writer.holds(id)) return;
      const now = this.#now();
      // A restarted link finds its predecessor's row under this machine (a crash leaves it for up to the
      // 600 s presence TTL). Reuse that incarnation so the row can be adopted; the lease is adopted only
      // from this process or a provably exited predecessor, so a second live link on this machine is refused.
      const previous = this.kv.reader.records(id).find(route => route.machine === this.machine && route.kind === "root" &&
        route.rootId === id && route.ownerHostId === this.#transport.self.hostId && route.ownerIdentityId === id)?.ownerIncarnation;
      this.#selfRecord = {
        format: 1, id, kind: "root", rootId: id, ownerHostId: this.#transport.self.hostId, ownerIdentityId: id,
        ownerIncarnation: previous ?? randomUUID(), name: this.identity.name, status: "running", runner: "pi", transport: "host",
        capabilities: [], startedAt: now, updatedAt: now, controlProtocol: "v1",
      };
      const outcome = await this.kv.writer.replace(SELF_GROUP, [{ record: this.#selfRecord, until: Number.POSITIVE_INFINITY }], true);
      if (outcome.refused.length > 0) {
        this.#selfRecord = undefined;
        if (throwOnRefusal) throw coded(`NATS bridge link identity ${id} refused: ${outcome.refused[0]!.code}`, outcome.refused[0]!.code);
      }
    })();
    this.#selfPass = pass;
    void pass.finally(() => { if (this.#selfPass === pass) this.#selfPass = undefined; }).catch(() => undefined);
    return pass;
  }

  #assertViewReady(): void {
    if (!this.kv.reader.ready) throw new DirectoryNotReadyError("NATS bridge KV presence is not ready; the answer is unknown, retry");
  }

  /** Live, unique, admitted canonical NATS roots on other machines (never this machine's own rows). */
  presence(): Promise<BridgePresence> { return this.kv.presence(); }

  /** Another machine's presence or lease change. The KV watches retry on their own; `failed` is never called. */
  watchPresence(changed: () => void, failed: (error: Error) => void): () => void {
    return this.kv.subscribeChanges(() => { changed(); }, failed);
  }

  /**
   * ParticipantView semantics: undefined is a FACT (absent, or on two machines). Without a KV
   * snapshot the answer is unknown, and that throws DirectoryNotReadyError: hold, never refuse.
   */
  resolve(id: string): BridgeNatsPortResolved | undefined {
    this.#assertViewReady();
    const route = this.kv.resolve(id);
    const kind = route && meshKind(route.kind);
    if (!route || !kind) return undefined;
    return {
      route: { machine: route.machine, ownerHostId: route.ownerHostId, ownerIdentityId: route.ownerIdentityId,
        ownerIncarnation: route.ownerIncarnation, capabilities: [...route.capabilities], live: route.live },
      identity: { id: route.id, name: route.record.name, kind,
        ...(route.record.sessionId !== undefined ? { sessionId: route.record.sessionId } : {}) },
    };
  }

  /** Every KV record (every machine, live or not) as the identity gate reads it; `list()` serves the dry run. */
  presenceView(): BridgeNatsPresenceView {
    const map = (id?: string): readonly BridgeNatsPresenceRecord[] => {
      this.#assertViewReady();
      const observedAt = this.#now();
      return this.kv.reader.records(id).flatMap(route => {
        const kind = meshKind(route.kind);
        if (!kind) return [];
        return [{
          id: route.id, name: route.record.name, kind, rootId: route.rootId,
          ...(route.record.sessionId !== undefined ? { sessionId: route.record.sessionId } : {}),
          ownerHostId: route.ownerHostId, ownerIdentityId: route.ownerIdentityId, ownerIncarnation: route.ownerIncarnation,
          machine: route.machine, live: route.live, observedAt,
        }];
      });
    };
    return { side: "nats", records: (id: string) => typeof id === "string" ? map(id) : [], list: () => map() };
  }

  /**
   * Hold the file side in KV: MeshBridge's outbound roots, allow-listed file actors (from the set and
   * from `fileActors`), and the link identity. A refused row is logged and skipped; only a fenced
   * port or a foreign link identity throws.
   */
  async mirror(set: BridgeNatsMirrorSet, observedAt: number): Promise<void> {
    if (this.#closed || this.kv.writer.fenced) throw coded("NATS bridge port is fenced", "FABRIC_BRIDGE_NATS_FENCED");
    if (set?.linkIdentity?.id !== this.identity.id) throw coded("NATS bridge mirror names another link identity", "FABRIC_TRANSPORT_INVALID");
    const hosts = Array.isArray(set.hosts) ? set.hosts : [];
    const participants = Array.isArray(set.participants) ? set.participants : [];
    await this.kv.mirror({ ...(set as Partial<BridgePresence>), hosts, participants }, observedAt);
    let actors = participants.filter(participant => participant?.kind === "actor");
    let sampledAt = Number.isFinite(observedAt) ? Math.min(observedAt, this.#now()) : this.#now();
    let complete = !isPartial(set);
    let readable = true;
    if (this.options.fileActors) {
      try {
        const read = this.options.fileActors();
        // One claim per actor: the native listing wins over the same actor in the set (two claims
        // for one id in a pass would refuse both).
        const listed = new Set(read.actors.map(actor => actor.id));
        actors = [...actors.filter(actor => !listed.has(actor.id)), ...read.actors];
        sampledAt = Math.min(sampledAt, read.sampledAt);
        complete = true; // The native listing, not the roots snapshot, is the actors' authority.
      } catch (error) {
        // Unreadable is not absence: keep the current actor claims to their own deadlines.
        readable = false;
        try { this.options.onError?.(error); } catch { /* diagnostic only */ }
      }
    }
    if (readable) await this.kv.mirrorActors(actors, { hosts, sampledAt, complete });
    await this.#ensureSelf(false);
  }

  fence(): Promise<void> {
    if (this.#selfTimer) clearInterval(this.#selfTimer);
    this.#selfTimer = undefined;
    return this.kv.fence();
  }

  async withdraw(): Promise<void> {
    if (this.#selfTimer) clearInterval(this.#selfTimer);
    this.#selfTimer = undefined;
    await this.kv.withdraw();
  }

  /**
   * TransportSent, or TransportUncertainError (resend with the same id and bytes). Refused before
   * any publish: a command FROM an identity this link does not hold live in KV (a receiver would
   * refuse it as unverified), and this machine as the destination (a loop through the link).
   */
  async send<T>(envelope: TransportEnvelope<T>, signal?: AbortSignal): Promise<TransportSent<T>> {
    if (this.#closed || this.kv.writer.fenced) throw coded("NATS bridge port is fenced", "FABRIC_BRIDGE_NATS_FENCED");
    if (!validMessageId(envelope?.id) || envelope.id.startsWith("relay:") || (envelope.kind !== "command" && envelope.kind !== "ack") ||
      (envelope.kind === "command") === envelope.id.startsWith("ack:")) {
      throw coded("Invalid NATS bridge envelope id", "FABRIC_TRANSPORT_INVALID");
    }
    if (!MACHINE_TOKEN.test(envelope.to?.machine ?? "") || envelope.to.machine === this.machine) {
      throw coded("NATS bridge destination must be another machine", "FABRIC_TRANSPORT_INVALID");
    }
    const fromId = envelope.from?.id;
    const held = typeof fromId !== "string" ? "absent" : fromId === this.identity.id ? "live" : this.kv.writer.holdState(fromId);
    if (envelope.kind === "command" && held === "absent") {
      throw coded("NATS bridge sender is not held live by this link", "FABRIC_SENDER_UNVERIFIED");
    }
    if (envelope.kind === "command" && held === "renewing") {
      // An outage has kept its lease unconfirmed: hold the command for a same-id resend, never refuse it.
      throw new TransportUncertainError(envelope.id, envelope.body, { cause: coded("NATS bridge sender lease is unconfirmed", "FABRIC_TRANSPORT_UNAVAILABLE") });
    }
    try {
      return await this.#transport.send(envelope, signal);
    } catch (error) {
      const code = (error as { code?: unknown } | null)?.code;
      if (code === "FABRIC_TRANSPORT_UNCERTAIN" || code === "FABRIC_TRANSPORT_DENIED" || code === "FABRIC_TRANSPORT_INVALID") throw error;
      // Everything else (unavailable during an outage or reconnect, capacity, the transport's retry
      // horizon, an unclassified failure) is retryable with the same id and bytes: report it as
      // uncertain, which is what the carrier resends, never as a final refusal that drops the message.
      throw new TransportUncertainError(envelope.id, envelope.body, { cause: error });
    }
  }

  /**
   * This machine's inbox, one iterator at a time, in stream order. A malformed or misaddressed
   * message is TERMed here; everything else is yielded unsettled.
   */
  async *inbox(signal: AbortSignal): AsyncIterable<NatsBridgeDelivery> {
    if (this.#closed) throw new Error("NATS bridge port is closed");
    if (this.#inboxActive) throw new Error("Only one NATS bridge inbox iterator is allowed");
    this.#inboxActive = true;
    const lifetime = AbortSignal.any([signal, this.#abort.signal]);
    try {
      for await (const message of this.#inbox.consume(lifetime)) {
        if (lifetime.aborted) break;
        let decoded: ReturnType<typeof decodeNatsBridgeMessage>;
        try { decoded = decodeNatsBridgeMessage(this.machine, message); }
        catch (error) {
          if (this.#inbox.isVerified()) message.term(error instanceof Error ? error.message : "malformed transport envelope");
          continue;
        }
        let finished = false;
        const assertVerified = (): void => {
          if (!this.#inbox.isVerified()) {
            throw coded("NATS bridge delivery is suspended until USER.INFO re-verifies", "FABRIC_TRANSPORT_UNAVAILABLE");
          }
        };
        yield {
          envelope: decoded.envelope, sourceMachine: decoded.sourceMachine, toHostId: decoded.envelope.to.hostId,
          streamSequence: message.info.streamSequence, attempt: message.info.deliveryCount,
          ...(decoded.bodyDigest !== undefined ? { bodyDigest: decoded.bodyDigest } : {}),
          working: () => { if (!finished && this.#inbox.isVerified()) message.working(); },
          ack: async () => {
            if (finished) return;
            assertVerified();
            if (!await message.ackAck()) throw new Error("NATS bridge broker ACK was not confirmed");
            finished = true;
          },
          nak: (delayMs?: number) => {
            if (finished) return;
            assertVerified();
            message.nak(delayMs);
            finished = true;
          },
          term: async (reason: string) => {
            if (finished) return;
            assertVerified();
            message.term(reason);
            finished = true;
          },
        };
      }
    } finally {
      this.#inboxActive = false;
    }
  }

  /** Stops the inbox (in-hand deliveries are neither acked nor nacked) and fences the mirror. */
  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    this.#abort.abort(new Error("NATS bridge port closed"));
    if (this.#selfTimer) clearInterval(this.#selfTimer);
    this.#selfTimer = undefined;
    await this.#selfPass?.catch(() => undefined);
    try {
      // A clean stop releases the link identity at once, so a restart never waits on its own lease.
      // Mirrored rows stay fenced: their leases lapse within one TTL, or the restarted link adopts them.
      if (!this.kv.writer.fenced) await this.kv.writer.release(SELF_GROUP).catch(error => { this.options.onError?.(error); });
      await this.kv.close();
    } finally { await this.options.onClose?.(); }
  }
}

/** The fields of the link config (L5's NatsLinkConfig) the port reads; NatsLinkConfig is assignable to it. */
export interface NatsBridgeSideConfig {
  readonly link: string;
  readonly nats: {
    readonly servers: readonly string[];
    readonly machine: string;
    /** Absolute path of the bridge machine's user.creds file. */
    readonly credentials: string;
    readonly caFile: string;
    readonly org?: string;
    readonly custodyStream: string;
    readonly livenessStream: string;
    readonly pathBMachines: readonly string[];
  };
  readonly allow: { readonly file?: readonly NatsBridgeAllowLike[]; readonly nats?: readonly NatsBridgeAllowLike[] };
}

export interface OpenNatsBridgeSideOptions {
  readonly signal?: AbortSignal;
  readonly log?: (line: string) => void;
  readonly onError?: (error: unknown) => void;
  /** The hub mesh store the link already opened (StoreBridgeSide's): enables allow-listed file actors. */
  readonly fileStore?: MeshStore;
}

/**
 * Open the bridge machine's port from the link config: a NATS transport whose hostId is the link
 * identity (send, KV), a second verified connection for the machine-wide inbox, presence admission
 * from `config.allow`, and file actors when `fileStore` is given. Resolves once both KV snapshots are
 * ready and the link identity is live. Event grants (PR 2) stay unused here: an agent credential may
 * carry more than this port asks for, never less.
 */
export const openNatsBridgeSide = async (config: NatsBridgeSideConfig, options: OpenNatsBridgeSideOptions = {}): Promise<NatsBridgePort> => {
  const [{ openNatsTransport }, { openNatsConnection }] = await Promise.all([
    import("../transport/nats/inbox.js"), import("../transport/nats/connection.js")]);
  const identity = bridgeNatsLinkIdentity(config.link);
  const common = {
    servers: [...config.nats.servers], machine: config.nats.machine, credentials: config.nats.credentials,
    tls: { caFile: config.nats.caFile }, custodyStream: config.nats.custodyStream, livenessStream: config.nats.livenessStream,
    pathBMachines: [...config.nats.pathBMachines], ...(config.nats.org === undefined ? {} : { org: config.nats.org }),
    ...(options.signal ? { signal: options.signal } : {}),
  };
  const transport = await openNatsTransport({ ...common, hostId: identity.id, name: `nats-link:${config.link}`, events: false, provision: false });
  let connection: NatsConnectionHandle;
  try { connection = await openNatsConnection({ ...common, name: `nats-link-inbox:${config.link}` }); }
  catch (error) { await transport.close().catch(() => undefined); throw error; }
  let port: NatsBridgePort | undefined;
  try {
    let fileActors: (() => NatsFileActorRead) | undefined;
    if (options.fileStore) {
      const { createFileActorDirectory, readFileActorsForMirror } = await import("./bridge-nats-actors.js");
      const directory = await createFileActorDirectory(options.fileStore, config.link);
      fileActors = () => readFileActorsForMirror(directory);
    }
    port = new NatsBridgePort({
      transport, inbox: natsBridgeInboxSource(connection, { machine: config.nats.machine, link: config.link }),
      link: config.link, identity, admit: natsBridgeAdmit(config.allow),
      ...(fileActors ? { fileActors } : {}), ...(options.log ? { log: options.log } : {}),
      ...(options.onError ? { onError: options.onError } : {}),
      onClose: async () => { await Promise.allSettled([transport.close(), connection.close()]); },
    });
    await port.start(options.signal);
    return port;
  } catch (error) {
    if (port) await port.close().catch(() => undefined);
    else await Promise.allSettled([transport.close(), connection.close()]);
    throw error;
  }
};
