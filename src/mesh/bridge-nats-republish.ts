// A' bridge PR 2, N4: v2 work topics into the hub file mesh (smarty-dev#15310, lane L1).
//
// A v2 Main publishes its `fleet.work.*` events (issue work, WAIT/ETA records, task completions) to the
// org's FABRIC_EV_<ORG> stream, never to the Ryzen 1 file mesh. This leg consumes them with
// bridge-nats-events.ts and republishes each allowed one into the hub file mesh as the same topic, so
// the factory and other file-mesh consumers see it.
//
// - Sender and topic: L3's gate decides, and nothing else does. `verifyEventSender` binds `from.id` to ONE
//   live, fresh, allow-listed KV root on its pinned machine (never the bridge's), and `allowRepublish`
//   needs the topic in that entry's `republishTopics` (default none). The events leg checks the KV view
//   first as well. The file event is authored as the gate's `author` (the link, `bridge:<link>`); the
//   gate's verified sender (from KV presence, never the body) is kept in `data.bridge.sender`.
// - Recipient: an addressed event keeps its `to` only when that names the hub's factory or a file-side
//   allow-listed root or actor (`recipientAllowed`); any other addressed event is refused
//   (BRIDGE_NATS_NOT_ALLOWED), so a v2 Main can never address an arbitrary hub participant through the
//   link. An unaddressed (topic-only) event crosses as it is.
// - Stamp: `data.bridge = { from: <link>, id: <NATS message id>, sender }`, the file bridge's own
//   convention, so no bridge ever forwards it again; an event that already carries a stamp, or that
//   claims one of this link's mirrors, is refused by the events leg (no loops).
// - Exactly once: the file publish is keyed (`dedupeKey` = link + NATS message id), so a redelivery or
//   a crash between the append and the broker ACK appends nothing new; the ledger's seen record skips
//   the store for a known id. The broker is ACKed only after the durable append.
// - Bounded: per-link token buckets for events and bytes (the file bridge's factory defaults); over the
//   bound an event is NAKed and redelivered later, never dropped.
//
// Grant (org-approved shape): consume + ACK on the org's own FABRIC_EV_<ORG>, one pull durable named
// under the bridge machine (`bev_<machine>`). Nothing here publishes into FABRIC_EV or creates, updates or
// deletes a stream; the start proves the grant on that durable and refuses without it.
//
// Residual (threat model): event subjects carry no source machine, so a machine holding the events
// publish grant can claim another live root's id. The machine pin narrows it to that root's own machine.
import path from "node:path";
import { writeJsonAtomic } from "../core/atomic-write.js";
import { openNatsConnection, type NatsConnectionHandle } from "../transport/nats/connection.js";
import { natsEventsStreamName } from "../transport/nats/provision.js";
import type { MeshEvent, MeshIdentity, MeshPublishInput } from "./event-log.js";
import {
  BRIDGE_NATS_REFUSAL, type BridgeNatsPort, type BridgeNatsPortResolved, type BridgeNatsPresenceView, type BridgeNatsRepublishDecision,
  type BridgeNatsRepublishRequest, type BridgeNatsVerified, type BridgeNatsVerifiedIdentity,
} from "./bridge-nats-types.js";
import { HUB_FACTORY_RECIPIENT, isWorkTopic } from "./bridge-topics.js";
import { ensureNatsBridgeEventConsumer, natsBridgeEventsDurable, natsBridgeEventSource, runNatsBridgeEvents,
  type NatsBridgeEventDelivery, type NatsBridgeEventDisposition, type NatsBridgeEventRefusal, type NatsBridgeEventSource,
} from "./bridge-nats-events.js";
import type { NatsBridgeSideConfig } from "./bridge-nats-port.js";

/** The file bridge's factory-exception defaults (bridge.ts DEFAULT_FACTORY_RATE). */
export const NATS_REPUBLISH_RATE = Object.freeze({ perMinute: 30, burst: 60, bytesPerMinute: 512 * 1024, byteBurst: 1024 * 1024 });
export const NATS_REPUBLISH_TOPICS = Object.freeze(["fleet.work.>"]);

/** The events leg's codes, L3's gate codes (`BRIDGE_NATS_*`), or this leg's own two. */
export type NatsRepublishRefusal = NatsBridgeEventRefusal | "BRIDGE_NATS_NOT_A_WORK_TOPIC" | "BRIDGE_NATS_EVENT_TOO_LARGE" |
  (string & {});

export interface NatsRepublishCounters {
  republished: number;
  /** Redeliveries and replays that appended nothing new (ledger seen or the keyed append's receipt). */
  duplicates: number;
  /** NAKed because the per-link rate bound was spent; redelivered later. */
  rateDeferred: number;
  refused: Partial<Record<NatsRepublishRefusal, number>>;
  /** Wall clock of the last republish. */
  lastAt?: number;
}

/** The two N4 methods of L3's identity gate (BridgeNatsIdentityGate). */
export interface NatsRepublishGate {
  verifyEventSender(claimedId: string, presence: BridgeNatsPresenceView, other: BridgeNatsPresenceView): BridgeNatsVerified;
  allowRepublish(request: BridgeNatsRepublishRequest): BridgeNatsRepublishDecision;
}

/** Each side's presence, read for every decision (the port's KV view and the hub's file view). */
export interface NatsRepublishViews {
  /** Brings the file view up to date first (the carrier's TTL-cached refresh); a failure retries the event. */
  refresh?(): Promise<void>;
  nats(): BridgeNatsPresenceView;
  file(): BridgeNatsPresenceView;
}

const notReady = (error: unknown): boolean => (error as { code?: unknown } | null)?.code === "FABRIC_DIRECTORY_NOT_READY";

/** A token bucket refilled continuously; `take` spends from both or neither. */
class RateBound {
  #events: number;
  #bytes: number;
  #at: number;
  constructor(readonly rate: NatsRepublishRate, readonly now: () => number) {
    this.#events = rate.burst;
    this.#bytes = rate.byteBurst;
    this.#at = now();
  }
  take(bytes: number): boolean {
    const now = this.now();
    const elapsed = Math.max(0, now - this.#at) / 60_000;
    this.#at = now;
    this.#events = Math.min(this.rate.burst, this.#events + elapsed * this.rate.perMinute);
    this.#bytes = Math.min(this.rate.byteBurst, this.#bytes + elapsed * this.rate.bytesPerMinute);
    if (this.#events < 1 || this.#bytes < bytes) return false;
    this.#events -= 1;
    this.#bytes -= bytes;
    return true;
  }
}

export interface NatsRepublishOptions {
  readonly link: string;
  readonly org: string;
  /** The bridge's own machine: a sender live there is one of this link's mirrors. */
  readonly machine: string;
  /** The hub file mesh (StoreBridgeSide's store). */
  readonly store: { publish(input: MeshPublishInput): Promise<MeshEvent> };
  readonly source: NatsBridgeEventSource;
  /** The port's resolve(): KV presence for a sender id (the events leg's first check). */
  readonly resolve: (id: string) => BridgeNatsPortResolved | undefined;
  /** L3's gate: the only sender and topic decision. */
  readonly gate: NatsRepublishGate;
  readonly views: NatsRepublishViews;
  /**
   * Whether an addressed event may keep its `to` in the file mesh: a file-side allow-listed root or actor
   * (the link passes its allow.file check). The hub factory is always a recipient; absent, no participant is.
   */
  readonly recipientAllowed?: (to: string) => boolean;
  /** L3's ledger, for the seen skip (the keyed append alone already guarantees one append). */
  readonly ledger?: {
    seen(direction: "toFile", eventId: string): unknown;
    markSeen(direction: "toFile", eventId: string, receipt?: { destinationId?: string; destinationSequence?: number }): Promise<unknown>;
  };
  readonly topics?: readonly string[];
  readonly rate?: NatsRepublishRate;
  readonly maxEventBytes?: number;
  readonly now?: () => number;
  readonly log?: (line: string) => void;
  readonly onError?: (error: unknown) => void;
  /** L5's link counters (bridge-nats-counters.ts): the three `republish.*` names `nats-link status` shows. */
  readonly counters?: NatsRepublishLinkCounters;
}

export interface NatsRepublishRate { readonly perMinute: number; readonly burst: number; readonly bytesPerMinute: number; readonly byteBurst: number }

/** The part of L5's NatsLinkCounters this leg uses (its contract names, exactly). */
export interface NatsRepublishLinkCounters {
  add(name: "republish.accepted" | "republish.duplicate" | "republish.refused"): void;
}

const seenId = (link: string, messageId: string): string => `republish:${link}:${messageId}`;

/**
 * The keyed file append for one admitted event: authored as the gate's `author`, the gate's verified
 * sender in the stamp. Exported for the integration's dry run and tests.
 */
export const natsRepublishInput = (link: string, delivery: NatsBridgeEventDelivery,
  admitted: { readonly author: MeshIdentity; readonly sender: BridgeNatsVerifiedIdentity }): MeshPublishInput & { data: Record<string, unknown> } => {
  const { event, messageId } = delivery;
  const data = typeof event.data === "object" && event.data !== null && !Array.isArray(event.data)
    ? event.data as Record<string, unknown> : event.data === undefined ? {} : { value: event.data };
  const verified = admitted.sender;
  const sender: MeshIdentity & { machine: string } = { id: verified.id, name: verified.name, kind: verified.kind,
    ...(verified.sessionId !== undefined ? { sessionId: verified.sessionId } : {}), machine: verified.machine ?? delivery.senderMachine };
  return {
    topic: event.topic, kind: event.kind, from: { ...admitted.author },
    ...(typeof event.to === "string" ? { to: event.to } : {}),
    ...(typeof event.text === "string" ? { text: event.text } : {}),
    // A principal never crosses on a republished event: the hub has no way to verify it.
    data: { ...data, bridge: { from: link, id: messageId, sender } },
    dedupeKey: `bridge-nats-republish:${link}:${messageId}`,
    durable: true,
  };
};

/**
 * Run the republish leg until `signal` aborts. Resolves when the leg ends; rejects on a terminal
 * consumer failure (the link decides whether that stops it).
 */
export const runNatsRepublish = (options: NatsRepublishOptions, signal: AbortSignal):
  { readonly done: Promise<void>; counters(): NatsRepublishCounters } => {
  const now = options.now ?? Date.now;
  const counters: NatsRepublishCounters = { republished: 0, duplicates: 0, rateDeferred: 0, refused: {} };
  const count = (name: "republish.accepted" | "republish.duplicate" | "republish.refused"): void => {
    try { options.counters?.add(name); } catch (error) { options.onError?.(error); }
  };
  const refuse = (code: NatsRepublishRefusal): NatsBridgeEventDisposition => {
    counters.refused[code] = (counters.refused[code] ?? 0) + 1;
    count("republish.refused");
    return "refused";
  };
  const bound = new RateBound(options.rate ?? NATS_REPUBLISH_RATE, now);
  const maxBytes = options.maxEventBytes ?? 256 * 1024;
  const reported = new Set<string>();
  const onEvent = async (delivery: NatsBridgeEventDelivery): Promise<NatsBridgeEventDisposition> => {
    const { event, messageId } = delivery;
    if (!isWorkTopic(event.topic)) return refuse("BRIDGE_NATS_NOT_A_WORK_TOPIC");
    // L3's gate, on both sides' presence as it is now. An unknown view is not a refusal: see the event again later.
    let verified: BridgeNatsVerified;
    try {
      await options.views.refresh?.();
      verified = options.gate.verifyEventSender(event.from.id, options.views.nats(), options.views.file());
    } catch (error) {
      if (notReady(error)) return "retry";
      throw error;
    }
    const decision = verified.ok ? options.gate.allowRepublish({ sender: verified.identity, topic: event.topic, kind: event.kind })
      : verified;
    if (!decision.ok) {
      const key = `${event.from.id}\0${event.topic}\0${decision.refuse}`;
      if (!reported.has(key) && reported.size < 1_000) {
        reported.add(key);
        options.log?.(`nats republish: refused ${event.topic} from ${event.from.id}: ${decision.refuse} (${decision.reason})`);
      }
      return refuse(decision.refuse);
    }
    // An addressed event reaches only the hub factory or a file-side allow-listed identity, never any hub participant.
    if (typeof event.to === "string" && event.to !== HUB_FACTORY_RECIPIENT && options.recipientAllowed?.(event.to) !== true) {
      const key = `${event.from.id}\0${event.topic}\0to\0${event.to}`;
      if (!reported.has(key) && reported.size < 1_000) {
        reported.add(key);
        options.log?.(`nats republish: refused ${event.topic} from ${event.from.id} to ${event.to}: ${BRIDGE_NATS_REFUSAL.notAllowed} ` +
          "(not the hub factory or a file-side allow-listed root or actor)");
      }
      return refuse(BRIDGE_NATS_REFUSAL.notAllowed);
    }
    const id = seenId(options.link, messageId);
    if (options.ledger?.seen("toFile", id)) { counters.duplicates += 1; count("republish.duplicate"); return "accepted"; }
    const input = natsRepublishInput(options.link, delivery, decision);
    const bytes = Buffer.byteLength(JSON.stringify(input.data)) + Buffer.byteLength(input.text ?? "");
    if (bytes > maxBytes) return refuse("BRIDGE_NATS_EVENT_TOO_LARGE");
    if (!bound.take(bytes)) { counters.rateDeferred += 1; return "retry"; }
    // Keyed and durable: a replay after a crash returns the first append's receipt and appends nothing.
    const appended = await options.store.publish(input);
    counters.republished += 1;
    counters.lastAt = now();
    count("republish.accepted");
    await options.ledger?.markSeen("toFile", id, { destinationId: appended.id, destinationSequence: appended.sequence })
      .catch(error => { options.onError?.(error); });
    return "accepted";
  };
  const done = runNatsBridgeEvents({
    org: options.org, machine: options.machine, topics: options.topics ?? NATS_REPUBLISH_TOPICS, source: options.source,
    resolve: options.resolve, onEvent,
    onRefused: code => { counters.refused[code] = (counters.refused[code] ?? 0) + 1; count("republish.refused"); },
    ...(options.onError ? { onError: options.onError } : {}),
  }, signal);
  return { done, counters: () => ({ ...counters, refused: { ...counters.refused } }) };
};

/** The bridge machine's credential has no events grant: N4 cannot run, so a link configured for it refuses to start. */
export class NatsRepublishGrantError extends Error {
  readonly code = "FABRIC_BRIDGE_NATS_EVENTS_GRANT_MISSING";
  constructor(readonly stream: string, readonly durable: string, options?: ErrorOptions) {
    super(`FABRIC_BRIDGE_NATS_EVENTS_GRANT_MISSING: nats.events is on, but the bridge credential lacks the ${stream} ` +
      `consume grant (durable ${durable}: CONSUMER.CREATE/INFO/MSG.NEXT and ACK). Re-issue it with that grant, or turn nats.events off`, options);
    this.name = "NatsRepublishGrantError";
  }
}

/** What the link reads to start N4 (L5's NatsLinkConfig is assignable). */
export interface NatsRepublishLinkConfig {
  readonly link: string;
  readonly stateDir: string;
  readonly nats: NatsBridgeSideConfig["nats"] & { readonly events?: boolean };
}

export interface NatsRepublishHandle {
  readonly done: Promise<void>;
  counters(): NatsRepublishCounters;
  stop(): Promise<void>;
}

/**
 * `<stateDir>/republish.json` (0600): the leg's state for `nats-link status`: `running`, `stopped`, or
 * `refused` with the code and reason when the consume grant is missing. Counts live in L5's counters.json.
 */
export const natsRepublishStatePath = (stateDir: string): string => path.join(stateDir, "republish.json");

/** A broker refusal of the consume grant: a publish permission violation on the JS API, or an API 403. */
const isPermissionRefusal = (error: unknown): boolean => {
  for (let depth = 0, cause = error; depth < 6 && typeof cause === "object" && cause !== null; depth++) {
    const { name, code } = cause as { name?: unknown; code?: unknown };
    if (name === "PermissionViolationError" || name === "NatsAuthorizationError" || code === "FABRIC_NATS_AUTHORIZATION" ||
      code === "PERMISSIONS_VIOLATION" || code === 403) return true;
    try {
      if (typeof (cause as { apiError?: unknown }).apiError === "function" &&
        (cause as { apiError(): { code?: number } }).apiError().code === 403) return true;
    } catch { /* not an API error */ }
    cause = (cause as { cause?: unknown }).cause;
  }
  return false;
};

/**
 * Start N4 for a link whose config sets `nats.events` (undefined otherwise). It opens its own connection
 * and proves the consume grant on its own durable before anything runs: a credential without it refuses
 * the start (NatsRepublishGrantError) rather than running a link that silently republishes nothing.
 */
export const startNatsRepublishLink = async (config: NatsRepublishLinkConfig, options: {
  readonly store: NatsRepublishOptions["store"];
  readonly port: Pick<BridgeNatsPort, "resolve" | "machine">;
  readonly gate: NatsRepublishGate;
  readonly views: NatsRepublishViews;
  readonly recipientAllowed?: NatsRepublishOptions["recipientAllowed"];
  readonly ledger?: NatsRepublishOptions["ledger"];
  readonly signal: AbortSignal;
  readonly log: (line: string) => void;
  readonly counters?: NatsRepublishLinkCounters;
  /** Test seam: the connection (default openNatsConnection with the standard agent policy). */
  readonly connect?: (input: Parameters<typeof openNatsConnection>[0]) => Promise<NatsConnectionHandle>;
}): Promise<NatsRepublishHandle | undefined> => {
  if (config.nats.events !== true) return undefined;
  const org = config.nats.org;
  if (typeof org !== "string" || org.length === 0) throw new TypeError("nats-link: republish (nats.events) needs nats.org");
  const file = natsRepublishStatePath(config.stateDir);
  const durable = natsBridgeEventsDurable(config.nats.machine);
  const record = (body: Record<string, unknown>): void => {
    try { writeJsonAtomic(file, { v: 1, link: config.link, at: new Date().toISOString(), durable, ...body }, { mode: 0o600 }); }
    catch (error) { options.log(`nats republish: state not written: ${error instanceof Error ? error.message : String(error)}`); }
  };
  // The org grants the bridge machine consume + ACK on its own FABRIC_EV durable only (no publish into
  // FABRIC_EV, no stream writes). That is not the full events template, so the connection is verified
  // with the standard agent policy (which accepts extra grants) and the grant is proven by using it:
  // look the durable up or create it. A refusal there refuses the start, recorded for `nats-link status`.
  const connection = await (options.connect ?? openNatsConnection)({
    servers: [...config.nats.servers], machine: config.nats.machine, credentials: config.nats.credentials,
    tls: { caFile: config.nats.caFile }, custodyStream: config.nats.custodyStream, livenessStream: config.nats.livenessStream,
    pathBMachines: [...config.nats.pathBMachines], name: `nats-link-events:${config.link}`, signal: options.signal,
  });
  const sourceOptions = { org, machine: config.nats.machine, topics: NATS_REPUBLISH_TOPICS };
  try {
    await ensureNatsBridgeEventConsumer(connection, sourceOptions);
  } catch (error) {
    await connection.close().catch(() => undefined);
    if (isPermissionRefusal(error)) {
      const refused = new NatsRepublishGrantError(natsEventsStreamName(org), durable, { cause: error });
      record({ state: "refused", code: refused.code, reason: refused.message });
      throw refused;
    }
    throw error;
  }
  const abort = new AbortController();
  const stop = (): void => { abort.abort(); };
  options.signal.addEventListener("abort", stop, { once: true });
  record({ state: "running" });
  const leg = runNatsRepublish({
    link: config.link, org, machine: options.port.machine, store: options.store,
    source: natsBridgeEventSource(connection, sourceOptions),
    resolve: id => options.port.resolve(id), gate: options.gate, views: options.views,
    ...(options.recipientAllowed ? { recipientAllowed: options.recipientAllowed } : {}),
    ...(options.ledger ? { ledger: options.ledger } : {}), log: options.log,
    onError: error => options.log(`nats republish: ${error instanceof Error ? error.message : String(error)}`),
    ...(options.counters ? { counters: options.counters } : {}),
  }, abort.signal);
  // A terminal consumer failure is the operator's to see; the link's control traffic goes on.
  void leg.done.catch(error => {
    if (abort.signal.aborted) return;
    record({ state: "failed", reason: error instanceof Error ? error.message.slice(0, 500) : String(error).slice(0, 500) });
    options.log(`nats republish: stopped: ${error instanceof Error ? error.message : String(error)}`);
  });
  options.log(`nats republish: consuming ${NATS_REPUBLISH_TOPICS.join(", ")} from FABRIC_EV for ${config.link}`);
  let stopping: Promise<void> | undefined;
  return {
    done: leg.done,
    counters: leg.counters,
    stop: () => stopping ??= (async () => {
      stop();
      options.signal.removeEventListener("abort", stop);
      await leg.done.catch(() => undefined);
      record({ state: "stopped" });
      await connection.close().catch(() => undefined);
    })(),
  };
};
