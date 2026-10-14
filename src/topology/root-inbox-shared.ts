import { createHash } from "node:crypto";
import { confirmedSessionReceiptSnapshot, type SessionReceiptManager } from "../core/session-receipts.js";
import type { MeshEvent } from "../mesh/store.js";

/**
 * What the eager extension needs of the root inbox (root-inbox.ts): ids, delivery receipts, the
 * session's receipt view and the injected message. The cursor itself (RootInbox, with its acked ids
 * and high-water mark, smarty-dev#8962) is only reached through the lazy runtime state, so it stays
 * out of the startup graph; root-inbox.ts re-exports the public names defined here.
 */

/**
 * The canonical id namespace of root sessions. A root accepts a name in it only when the name is
 * its own id, never a session name that looks like one: the mesh bridge routes ids in this
 * namespace to other hosts, so native name delivery must stay disjoint from it (smarty-dev#2004).
 */
export const ROOT_ID_PREFIX = "session:";
export const ROOT_INBOX_CUSTOM_TYPE = "pi-fabric-inbox";
/** The custom type Main gives a delivered steer or follow-up (src/main-agent.ts deliverAgent). */
const AGENT_MESSAGE_CUSTOM_TYPE = "pi-fabric-agent-message";
/** Expire addressed work on first drain, including cursors saved by older runtimes. */
export const INBOX_HORIZON_MS = 2 * 60 * 60_000;
/**
 * One injection (R-token-waste): at most this many events are rendered in full, with at most this
 * much event text in total and per event. Action events bypass the row/total-text limits, but
 * retain the per-event cap. Overflow remains pending; a summary is never a delivery receipt.
 */
export const INBOX_INJECT_MAX_EVENTS = 8;
export const INBOX_INJECT_MAX_TEXT_BYTES = 12 * 1024;
export const INBOX_INJECT_EVENT_TEXT_BYTES = 4 * 1024;

export interface RootInboxEvent extends MeshEvent {
  /** Shaped on drain only: old actionable work never requests a turn. */
  deliveredLate?: true;
}

export interface RootInboxBatch {
  events: RootInboxEvent[];
  /** End of the contiguous durable prefix; priority lookahead may include higher sequences. */
  through: number;
  /** Expired addressed shadows skipped in this drain, never delivered as work. */
  skippedStale?: number;
  horizonMs?: number;
}

export interface RootInboxReconcileOptions {
  /** Failed/aborted runs acknowledge held work, but admit and deliver nothing newer. */
  commitOnly?: boolean;
  /** A turn boundary is positive progress evidence for the pending-batch wedge alarm. */
  turnEnd?: boolean;
}

/** Authoritative shown-only receipt data bound to the issuer's rendered carrier. */
export interface RootInboxReceiptSnapshot {
  readonly ids: readonly string[];
  readonly receipts: readonly string[];
  readonly digest: string;
}

/** Authentication precedes scan-local nonce consumption; failed claims never burn a nonce. */
export type RootInboxReceiptVerifier = (
  details: unknown, carrier: unknown, accepted: Set<string>,
) => RootInboxReceiptSnapshot | undefined;

// Only the trusted raw-content projection can populate this cache; public digest fields are ignored.
const projectedInboxDigests = new WeakMap<object, string>();
export const rootInboxCarrierDigest = (carrier: unknown): string => {
  const fields = carrier && typeof carrier === "object" ? carrier as { content?: unknown } : {};
  const cached = projectedInboxDigests.get(fields);
  if (cached && !("content" in fields)) return cached;
  // Native string content and its single text-block context representation render identically.
  const content = fields.content;
  const rendered = typeof content === "string" ? [content] : Array.isArray(content)
    ? content.map(part => part && typeof part === "object" && part.type === "text" && typeof part.text === "string"
      ? part.text : { part }) : [];
  return createHash("sha256").update(JSON.stringify(rendered)).digest("hex");
};

export const workKey = (data: unknown): string | undefined => {
  const key = data && typeof data === "object" ? (data as { key?: unknown }).key : undefined;
  return typeof key === "string" && key.trim() ? key.trim() : undefined;
};

const receipt = (fromId: string, kind: string, value: string): string =>
  createHash("sha256").update(JSON.stringify([fromId, kind, value])).digest("hex");

/** A delivery identity outranks work-key/ref fallbacks, never the other way around. */
export const deliveryIdentity = (data: unknown): { kind: string; value: string } | undefined => {
  if (!data || typeof data !== "object") return undefined;
  const fields = data as Record<string, unknown>;
  if (typeof fields.deliveryId === "string" && fields.deliveryId) return { kind: "delivery", value: fields.deliveryId };
  if (typeof fields.messageId === "string" && fields.messageId) return { kind: "id", value: fields.messageId };
  return undefined;
};
/**
 * A work key or ref claims work within one topic only (review-fleet-2 security P1 on #852): a
 * sender can reuse a key across topics, so a sender+key receipt let an unrelated event match an
 * old receipt and be suppressed. The topic is part of the receipt. Receipts of the older
 * topic-less kinds ("key", "ref") are never matched again: without a verifiable topic the event
 * is injected, because a duplicate shadow is cheap and a lost event is not.
 */
export const topicWorkReceipt = (fromId: string, kind: "key" | "ref", topic: string, value: string): string =>
  // smarty-dev#12140: only a native carrier (a steer or follow-up) claims its key/ref. A mesh
  // event has a unique envelope id, and two distinct events may share sender, topic and key, so
  // mesh events never store a key/ref receipt. The "native-topic-*" kind also retires every
  // older "topic-*" hash, which inbox carriers and cursors recorded for mesh events: such a
  // legacy receipt can no longer suppress a distinct new event (fail open, at least once).
  receipt(fromId, `native-topic-${kind}`, JSON.stringify([topic, value]));
/** A non-empty data.ref, the fallback work identity of a native carrier without a key. */
export const workRef = (data: unknown): string | undefined => {
  const ref = data && typeof data === "object" ? (data as { ref?: unknown }).ref : undefined;
  return typeof ref === "string" && ref.trim() ? ref.trim() : undefined;
};
/** The topic a native carrier names for its work (data.topic); without one its key/ref claims nothing. */
const carrierTopic = (data: unknown): string | undefined => {
  const topic = data && typeof data === "object" ? (data as { topic?: unknown }).topic : undefined;
  return typeof topic === "string" && topic.trim() ? topic.trim() : undefined;
};
/** Native carrier receipts only: its explicit identity, else its topic-scoped key/ref claim. */
const carrierReceipts = (fromId: string, data: unknown, topic: string | undefined): string[] => {
  if (!data || typeof data !== "object") return [];
  const identity = deliveryIdentity(data);
  if (identity) return [receipt(fromId, identity.kind, identity.value)];
  if (topic === undefined) return [];
  const key = workKey(data);
  const ref = workRef(data);
  return key ? [topicWorkReceipt(fromId, "key", topic, key)] : ref ? [topicWorkReceipt(fromId, "ref", topic, ref)] : [];
};
export const eventReceipt = (id: string): string => receipt("", "event", id);
/**
 * Receipts that a native carrier records for the same identity under its other kind: a shadow's
 * messageId is often the control commandId the follow-up arrived with (recorded as "delivery"),
 * and a shadow's deliveryId can be the carrier id (recorded as "id"). RootInbox uses these for
 * matching, not batch dedup; inbox carriers also include them for display identity projection.
 */
export const crossIdentityReceipts = (fromId: string, data: unknown): string[] => {
  const identity = deliveryIdentity(data);
  if (!identity) return [];
  return [receipt(fromId, identity.kind === "delivery" ? "id" : "delivery", identity.value)];
};
/**
 * A mesh event's receipts: its unique envelope id, a host-only publication key and an explicit
 * messageId/deliveryId. Never a sender/topic/key/ref fallback (smarty-dev#12140): a work key is
 * not unique per notification, so a consumed event must not suppress a distinct one with the
 * same key. Native keyed carriers still suppress their shadows (RootInbox #steered).
 */
export const eventReceipts = (event: MeshEvent): string[] => {
  const identity = deliveryIdentity(event.data);
  return [
    eventReceipt(event.id), receipt(event.from.id, "id", event.id),
    // Host-only publication keys are mesh-wide, even when a different maintenance owner
    // retries. Carry the key into recipient-scoped confirmed session/inbox receipts too.
    ...(event.dedupeKey ? [receipt("", "dedupe", event.dedupeKey)] : []),
    ...(identity ? [receipt(event.from.id, identity.kind, identity.value)] : []),
  ];
};
/**
 * Display-only shadow metadata for an inbox carrier's receipt group (smarty-dev#12140): an event
 * with an explicit identity carries the other-kind receipt too, because native carriers record
 * deliveryId as "delivery" while mesh shadows record messageId as "id" (and vice versa). Factory
 * shadows carry commandId instead: acceptControl gives the native carrier that same deliveryId.
 * A key or ref is only a work claim, not a notification identity, so key/ref-only shadows fail
 * open when a distinct event reuses the same work key. An explicit deliveryId/messageId still
 * outranks commandId; sender-qualified shared identity alone proves the carrier delivered it.
 */
const shadowDisplayReceipts = (event: MeshEvent): string[] => {
  if (deliveryIdentity(event.data)) return crossIdentityReceipts(event.from.id, event.data);
  const commandId = event.data && typeof event.data === "object" ? (event.data as { commandId?: unknown }).commandId : undefined;
  return typeof commandId === "string" && commandId ? [receipt(event.from.id, "delivery", commandId)] : [];
};
/** What the session's own entries say it holds. */
export interface RootInboxSession {
  /** Inbox messages collectively holding all of these event ids. */
  holdsBatch(ids: readonly string[]): boolean;
  /** An agent message (a steer or follow-up) from this sender that carried this work key and topic. */
  holdsSteer(fromId: string, key: string, topic: string): boolean;
  /** Confirmed native/inbox receipts from the whole history, not the recent-entry window. */
  delivered?: ReadonlySet<string>;
  /** Canonical session-entry times, so rescanning history cannot renew old receipts. */
  deliveredAt?: ReadonlyMap<string, number>;
}

/** Only receipt identity and a private raw-content digest survive the canonical-file scan.
 * Delivered bodies and arbitrary metadata stay durable in Pi, not duplicated in this cache. */
export const projectRootInboxReceipt = (value: unknown): unknown => {
  const object = (item: unknown): Record<string, unknown> | undefined =>
    item !== null && typeof item === "object" ? item as Record<string, unknown> : undefined;
  const carried = (item: unknown): Record<string, unknown> | undefined => {
    const row = object(item);
    if (!row) return undefined;
    const from = object(row.from), data = object(row.data);
    return {
      id: row.id, chain: row.chain, deliveryId: row.deliveryId,
      ...(from ? { from: { id: from.id } } : {}),
      ...(data ? { data: { key: data.key, ref: data.ref, topic: data.topic, deliveryId: data.deliveryId, messageId: data.messageId } } : {}),
    };
  };
  const entry = object(value);
  if (!entry) return value;
  const details = object(entry.details);
  const projected = {
    type: entry.type, id: entry.id, timestamp: entry.timestamp, customType: entry.customType,
    ...(details ? { details: {
      ...carried(details),
      ...(typeof details.nonce === "string" ? { nonce: details.nonce } : {}),
      ids: details.ids, receipts: details.receipts,
      // Keep malformed shapes unchanged: consumers retain their existing
      // validation/failure behavior; projection must not manufacture a receipt.
      items: Array.isArray(details.items) ? details.items.map(carried) : details.items,
    } } : {}),
  };
  if (entry.customType === ROOT_INBOX_CUSTOM_TYPE) projectedInboxDigests.set(projected, rootInboxCarrierDigest(entry));
  return projected;
};

const matchesInboxReceipt = (line: string): boolean =>
  line.includes(ROOT_INBOX_CUSTOM_TYPE) || line.includes(AGENT_MESSAGE_CUSTOM_TYPE);

const matchesMainReceipt = (line: string): boolean => line.includes(AGENT_MESSAGE_CUSTOM_TYPE) || line.includes('\"session\"');
/** Original native carrier IDs, for a retired/dead Main's inbox rotation. This uses
 * the same cached canonical receipt/barrier path as the root shadow inbox. */
export const confirmedMainInboxIds = (manager: SessionReceiptManager, sessionId: string): Set<string> => {
  const snapshot = confirmedSessionReceiptSnapshot(manager, matchesMainReceipt, projectRootInboxReceipt);
  const ids = new Set<string>();
  if (!snapshot.count) return ids;
  type Entry = { type?: string; id?: string; customType?: string; details?: { id?: string; chain?: string; items?: Array<{ id?: string; chain?: string }> } };
  const header = snapshot.entries.get(0) as Entry | undefined;
  if (header?.type !== "session" || header.id !== sessionId) throw new Error("Main inbox session receipt identity mismatch");
  for (const value of snapshot.entries.values()) {
    const entry = value as Entry;
    if (entry.type !== "custom_message" || entry.customType !== AGENT_MESSAGE_CUSTOM_TYPE) continue;
    for (const item of [entry.details, ...(entry.details?.items ?? [])]) {
      if (typeof item?.id === "string") ids.add(item.id);
      if (typeof item?.chain === "string") ids.add(item.chain);
    }
  }
  return ids;
};

/** Persisted sessions must use confirmed file receipts, never Pi's pre-write memory index.
 * A failed barrier supplies no positive delivery evidence, so shadows remain recoverable. */
const verifiedInboxReceipt = (
  entry: { details?: unknown }, verifier: RootInboxReceiptVerifier | undefined, accepted: Set<string>,
): RootInboxReceiptSnapshot | undefined => {
  if (!verifier) return undefined;
  const snapshot = verifier(entry.details, entry, accepted);
  if (!snapshot || !Array.isArray(snapshot.ids) || !Array.isArray(snapshot.receipts)) return undefined;
  // Do not let a verifier's mutable return value become shared reader state.
  return Object.freeze({
    ids: Object.freeze(snapshot.ids.filter((id): id is string => typeof id === "string")),
    receipts: Object.freeze(snapshot.receipts.filter((id): id is string => typeof id === "string")),
    digest: snapshot.digest,
  });
};

export const confirmedRootInboxSession = (
  manager: SessionReceiptManager, verifier?: RootInboxReceiptVerifier,
): RootInboxSession => {
  try {
    const snapshot = confirmedSessionReceiptSnapshot(manager, matchesInboxReceipt, projectRootInboxReceipt);
    return inboxReceiptSession(snapshot.entries, snapshot.count, 500, verifier);
  } catch {
    return rootInboxSession([]);
  }
};

/** A receipt snapshot of canonical entries; async inbox reads must not retain the history.
 * Batch presence retains its compatibility lookback; delivery identity does not. Unsigned inbox
 * carriers are intentionally fail-open: only the live issuer verifier may grant inbox authority. */
export const rootInboxSession = (
  entries: readonly unknown[], lookbackOrVerifier: number | RootInboxReceiptVerifier = 500,
  verifier?: RootInboxReceiptVerifier,
): RootInboxSession => {
  const lookback = typeof lookbackOrVerifier === "number" ? lookbackOrVerifier : 500;
  const receiptVerifier = typeof lookbackOrVerifier === "function" ? lookbackOrVerifier : verifier;
  return inboxReceiptSession(
    Object.keys(entries).map((key): [number, unknown] => [Number(key), entries[Number(key)]]),
    entries.length, lookback, receiptVerifier,
  );
};

const inboxReceiptSession = (
  entries: Iterable<readonly [number, unknown]>, count: number, lookback = 500,
  verifier?: RootInboxReceiptVerifier,
): RootInboxSession => {
  const batchIds = new Set<string>();
  const accepted = new Set<string>();
  let hasBatch = false;
  const steers = new Map<string, Set<string>>();
  const delivered = new Set<string>();
  const deliveredAt = new Map<string, number>();
  const remember = (id: string, at: number): void => {
    delivered.add(id);
    deliveredAt.set(id, Math.max(deliveredAt.get(id) ?? Number.NEGATIVE_INFINITY, at));
  };
  // Persisted snapshots contain receipts only, with original history positions. All-history
  // identities/legacy steers are needed for dedup; only batch presence uses a lookback.
  for (const [index, value] of [...entries].reverse()) {
    type Carried = { from?: { id?: unknown }; data?: unknown; id?: unknown; deliveryId?: unknown };
    const entry = value as { timestamp?: string; type?: string; customType?: string; details?: Carried & { ids?: unknown; items?: unknown; receipts?: unknown } } | undefined;
    if (entry?.type !== "custom_message") continue;
    const parsedAt = Date.parse(entry.timestamp ?? "");
    const at = Number.isFinite(parsedAt) ? parsedAt : Date.now();
    if (entry.customType === ROOT_INBOX_CUSTOM_TYPE) {
      const receipt = verifiedInboxReceipt(entry, verifier, accepted);
      if (receipt) {
        if (index >= Math.max(0, count - lookback)) {
          hasBatch = true;
          for (const id of receipt.ids) batchIds.add(id);
        }
        // Mixed-version inbox messages carried only globally unique mesh event ids.
        for (const id of receipt.ids) remember(eventReceipt(id), at);
        for (const id of receipt.receipts) remember(id, at);
      }
    }
    if (entry.customType !== AGENT_MESSAGE_CUSTOM_TYPE) continue;
    // A batch of followUps (smarty-dev#1495) carries each one in items.
    const carried: Carried[] = Array.isArray(entry.details?.items) ? entry.details.items as Carried[] : [entry.details ?? {}];
    for (const item of carried) {
      const fromId = item?.from?.id;
      const key = workKey(item?.data);
      const topic = carrierTopic(item?.data);
      if (typeof fromId !== "string") continue;
      for (const id of carrierReceipts(fromId, item.data, topic)) remember(id, at);
      if (typeof item.id === "string") remember(receipt(fromId, "id", item.id), at);
      if (typeof item.deliveryId === "string") remember(receipt(fromId, "delivery", item.deliveryId), at);
      // A carrier without a topic claims no work key: its shadow is injected (fail open).
      if (key === undefined || topic === undefined) continue;
      const keys = steers.get(fromId) ?? new Set<string>();
      keys.add(JSON.stringify([topic, key]));
      steers.set(fromId, keys);
    }
  }
  return {
    delivered, deliveredAt,
    holdsBatch: (ids) => hasBatch && ids.every((id) => batchIds.has(id)),
    holdsSteer: (fromId, key, topic) => steers.get(fromId)?.has(JSON.stringify([topic, key])) ?? false,
  };
};

/** Whether a session's recent entries hold the inbox message for these event ids. */
export const sessionHoldsInboxBatch = (
  entries: readonly unknown[], ids: readonly string[], lookback = 500,
  verifier?: RootInboxReceiptVerifier,
): boolean => {
  if (!verifier) return false;
  const missing = new Set(ids);
  const accepted = new Set<string>();
  for (let index = entries.length - 1; index >= Math.max(0, entries.length - lookback); index--) {
    const entry = entries[index] as { type?: string; customType?: string; details?: unknown } | undefined;
    if (entry?.type !== "custom_message" || entry.customType !== ROOT_INBOX_CUSTOM_TYPE) continue;
    const receipt = verifiedInboxReceipt(entry, verifier, accepted);
    if (!receipt) continue;
    for (const id of receipt.ids) missing.delete(id);
    if (missing.size === 0) return true;
  }
  return false;
};

const escapeXml = (value: string): string =>
  value.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");

const bounded = (text: string, sequence: number): string => {
  if (Buffer.byteLength(text) <= INBOX_INJECT_EVENT_TEXT_BYTES) return text;
  const cut = Buffer.from(text).subarray(0, INBOX_INJECT_EVENT_TEXT_BYTES).toString("utf8").replace(/\uFFFD$/u, "");
  return `${cut}\n[cut at ${INBOX_INJECT_EVENT_TEXT_BYTES} bytes; read the whole event with mesh.read({ after: ${sequence - 1}, limit: 1 })]`;
};

/** Shared internally with priority lookahead; ordinary durable admission stays bounded. */
export const ROOT_INBOX_ACTION_KINDS = new Set(["blocked", "blocker", "ask", "handoff"]);
const INBOX_PRIORITY = ["blocked", "blocker", "ask", "handoff", "done", "wait", "answer", "info", "progress", "status", "ack"];
export const compareRootInboxEvents = (a: MeshEvent, b: MeshEvent): number => {
  // Scheduled waits only enter the mesh once due. Keep the legacy completion spelling with done.
  const rank = (kind: string): number => {
    const index = INBOX_PRIORITY.indexOf(kind === "completion" ? "done" : kind);
    return index < 0 ? INBOX_PRIORITY.length : index;
  };
  return rank(a.kind) - rank(b.kind) || a.createdAt - b.createdAt || a.sequence - b.sequence;
};

/** Overflow is withheld work, not a receipt. All action rows retain their per-event text cap. */
export const rootInboxBudget = (events: readonly MeshEvent[]): { shown: MeshEvent[]; overflow: MeshEvent[] } => {
  const shown: MeshEvent[] = [], overflow: MeshEvent[] = [];
  let bytes = 0;
  let full = false;
  for (const event of [...events].sort(compareRootInboxEvents)) {
    const size = Math.min(Buffer.byteLength(event.text ?? ""), INBOX_INJECT_EVENT_TEXT_BYTES);
    if (!ROOT_INBOX_ACTION_KINDS.has(event.kind) &&
      (full || shown.length >= INBOX_INJECT_MAX_EVENTS || (shown.length > 0 && bytes + size > INBOX_INJECT_MAX_TEXT_BYTES))) {
      full = true;
      overflow.push(event);
      continue;
    }
    shown.push(event);
    bytes += size;
  }
  return { shown, overflow };
};

const overflowLine = (events: readonly MeshEvent[]): string => {
  const sequences = events.map((event) => event.sequence);
  const first = Math.min(...sequences), last = Math.max(...sequences);
  const senders = [...new Set(events.map((event) => event.from.name || event.from.id))];
  const from = senders.slice(0, 3).map((name) => JSON.stringify(name)).join(", ") + (senders.length > 3 ? ` and ${senders.length - 3} more` : "");
  return `<overflow count="${events.length}">+${events.length} more addressed event(s) not shown (sequence ${first}-${last}, from ${escapeXml(from)}); ` +
    `${events.length} more pending, shown next turn.</overflow>`;
};

/** A single passive summary, never stale event bodies or a batch of work to act on. */
export const rootInboxSummary = (batch: RootInboxBatch) => ({
  customType: "pi-fabric-inbox-summary",
  content: `Fabric inbox: skipped ${batch.skippedStale ?? 0} addressed shadows older than ${batch.horizonMs ?? INBOX_HORIZON_MS} ms; no stale work injected.`,
  display: true,
  details: { skippedStale: batch.skippedStale ?? 0, horizonMs: batch.horizonMs ?? INBOX_HORIZON_MS },
});
/**
 * The one message that brings a batch into the session. Without an explicit `overflow`, the batch
 * is cut to the injection budget itself. Only shown event ids/receipts enter details: an overflow
 * summary neither acknowledges work nor prevents that work from being shown on the next turn.
 */
export const rootInboxMessage = (input: readonly MeshEvent[], extra?: readonly MeshEvent[]) => {
  // A re-read shadow can repeat an event id; the carrier names each event once (smarty-dev#5518).
  const seen = new Set<string>();
  const unique = (list: readonly MeshEvent[]) => list.filter(event => !seen.has(event.id) && !!seen.add(event.id));
  const batch = unique(input);
  const overflow = extra === undefined ? undefined : unique(extra);
  const { shown: events, overflow: rest } = overflow === undefined ? rootInboxBudget(batch) : { shown: [...batch], overflow: [...overflow] };
  const all = [...events, ...rest];
  return {
  customType: ROOT_INBOX_CUSTOM_TYPE,
  content: [
    `<fabric-inbox count="${all.length}"${rest.length ? ` shown="${events.length}"` : ""}>`,
    "Work events addressed to you that no steer or follow-up delivered (a shadow copy can repeat a message you already saw):",
    ...events.map((event) => {
      const data = (event.data && typeof event.data === "object" ? event.data : {}) as { ref?: unknown; key?: unknown };
      const attributes = [
        `id="${event.id}"`, `sequence="${event.sequence}"`, `topic="${escapeXml(event.topic)}"`, `kind="${escapeXml(event.kind)}"`,
        `from_name=${JSON.stringify(event.from.name)}`, `from_id=${JSON.stringify(event.from.id)}`,
        ...(typeof data.ref === "string" ? [`ref=${JSON.stringify(data.ref.slice(0, 256))}`] : []),
        ...(typeof data.key === "string" ? [`key=${JSON.stringify(data.key.slice(0, 256))}`] : []),
        `at="${new Date(event.createdAt).toISOString()}"`,
      ];
      return `<event ${attributes.join(" ")}>${escapeXml(bounded(event.text ?? "", event.sequence))}</event>`;
    }),
    ...(rest.length ? [overflowLine(rest)] : []),
    "</fabric-inbox>",
  ].join("\n"),
  display: true,
  // Each shown group starts with its event receipt; withheld identities never enter the carrier.
  details: { ids: events.map((event) => event.id), receipts: events.flatMap((event) => [...eventReceipts(event), ...shadowDisplayReceipts(event)]) },
  };
};
