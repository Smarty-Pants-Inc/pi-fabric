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
 * much event text in total and per event; every further event of the batch is summarized in one
 * line. A 20-event batch was ~15k tokens per turn; the budget keeps one injection near 3k.
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
  /** The sequence the cursor moves to once these events are in the session. */
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
  /** A new run or completed settle admits exact-session work without the shadow grace. */
  priority?: boolean;
}

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

/** Only receipt identity survives the canonical-file scan. Delivered bodies and
 * arbitrary metadata are already durable in Pi; caching them again defeats its
 * cold entries. Preserve every field consumed by both receipt readers below. */
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
  return {
    type: entry.type, id: entry.id, timestamp: entry.timestamp, customType: entry.customType,
    ...(details ? { details: {
      ...carried(details), ids: details.ids, receipts: details.receipts,
      // Keep malformed shapes unchanged: consumers retain their existing
      // validation/failure behavior; projection must not manufacture a receipt.
      items: Array.isArray(details.items) ? details.items.map(carried) : details.items,
    } } : {}),
  };
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
export const confirmedRootInboxSession = (manager: SessionReceiptManager): RootInboxSession => {
  try {
    const snapshot = confirmedSessionReceiptSnapshot(manager, matchesInboxReceipt, projectRootInboxReceipt);
    return inboxReceiptSession(snapshot.entries, snapshot.count);
  } catch {
    return rootInboxSession([]);
  }
};

/** A receipt snapshot of canonical entries; async inbox reads must not retain the history.
 * Batch presence retains its compatibility lookback; delivery identity does not. */
export const rootInboxSession = (entries: readonly unknown[], lookback = 500): RootInboxSession =>
  inboxReceiptSession(Object.keys(entries).map((key): [number, unknown] => [Number(key), entries[Number(key)]]), entries.length, lookback);

const inboxReceiptSession = (entries: Iterable<readonly [number, unknown]>, count: number, lookback = 500): RootInboxSession => {
  const batchIds = new Set<string>();
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
    if (entry.customType === ROOT_INBOX_CUSTOM_TYPE && Array.isArray(entry.details?.ids)) {
      if (index >= Math.max(0, count - lookback)) {
        hasBatch = true;
        for (const id of entry.details.ids) if (typeof id === "string") batchIds.add(id);
      }
      // Mixed-version inbox messages carried only globally unique mesh event ids.
      for (const id of entry.details.ids) if (typeof id === "string") remember(eventReceipt(id), at);
      if (Array.isArray(entry.details.receipts)) {
        for (const id of entry.details.receipts) if (typeof id === "string") remember(id, at);
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
export const sessionHoldsInboxBatch = (entries: readonly unknown[], ids: readonly string[], lookback = 500): boolean => {
  const missing = new Set(ids);
  for (let index = entries.length - 1; index >= Math.max(0, entries.length - lookback); index--) {
    const entry = entries[index] as { type?: string; customType?: string; details?: { ids?: unknown } } | undefined;
    if (entry?.type !== "custom_message" || entry.customType !== ROOT_INBOX_CUSTOM_TYPE) continue;
    if (Array.isArray(entry.details?.ids)) {
      for (const id of entry.details.ids) if (typeof id === "string") missing.delete(id);
      if (missing.size === 0) return true;
    }
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

/** Split one injection into the events rendered in full and the overflow summarized in one line. */
export const rootInboxBudget = (events: readonly MeshEvent[]): { shown: MeshEvent[]; overflow: MeshEvent[] } => {
  const shown: MeshEvent[] = [];
  let bytes = 0;
  for (const event of events) {
    const size = Math.min(Buffer.byteLength(event.text ?? ""), INBOX_INJECT_EVENT_TEXT_BYTES);
    if (shown.length >= INBOX_INJECT_MAX_EVENTS || (shown.length > 0 && bytes + size > INBOX_INJECT_MAX_TEXT_BYTES)) break;
    shown.push(event);
    bytes += size;
  }
  return { shown, overflow: events.slice(shown.length) };
};

const overflowLine = (events: readonly MeshEvent[]): string => {
  const sequences = events.map((event) => event.sequence);
  const first = Math.min(...sequences), last = Math.max(...sequences);
  const senders = [...new Set(events.map((event) => event.from.name || event.from.id))];
  const from = senders.slice(0, 3).map((name) => JSON.stringify(name)).join(", ") + (senders.length > 3 ? ` and ${senders.length - 3} more` : "");
  return `<overflow count="${events.length}">+${events.length} more addressed event(s) not shown (sequence ${first}-${last}, from ${escapeXml(from)}); ` +
    `they count as delivered and will not be injected again; read them with mesh.read({ after: ${first - 1}, limit: ${last - first + 1} }) if you need them.</overflow>`;
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
 * is cut to the injection budget itself; every event id (shown or summarized) is in details.
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
  // Each group starts with the event receipt (the renderer's boundary); display metadata trails it.
  // Overflow has no XML row/boundary: put its groups first so the last shown row's group
  // ends at receipts.length without inheriting unrelated overflow identities. IDs stay in delivery order.
  details: { ids: all.map((event) => event.id), receipts: [...rest, ...events].flatMap((event) => [...eventReceipts(event), ...shadowDisplayReceipts(event)]) },
  };
};
