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
 * much event text in total and per event. Action events bypass the ordinary row/total-text limits,
 * but retain the per-event cap and share a hard rendered-payload cap. Overflow remains pending;
 * a summary is never a delivery receipt.
 */
export const INBOX_INJECT_MAX_EVENTS = 8;
export const INBOX_INJECT_MAX_TEXT_BYTES = 12 * 1024;
export const INBOX_INJECT_EVENT_TEXT_BYTES = 4 * 1024;
/**
 * Per turn, all rendered action rows plus their newlines: XML expansion, metadata and cut notices
 * count too. A conservative one-token/UTF-8-byte planning allowance is 131072 tokens: below half
 * the configured Sol context (272000 tokens), not a tokenizer-independent guarantee. Bounds the
 * incremental inbox burst, not arbitrary full-history/model-context usage; short bursts (including
 * 230/236 actions) still fit. Carrier wrappers and
 * summaries are separate.
 */
export const INBOX_INJECT_MAX_ACTION_RENDERED_BYTES = 128 * 1024;

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
  /** A new run or completed settle admits exact-session work without the shadow grace. */
  priority?: boolean;
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

const trimmedField = (data: unknown, field: string): string | undefined => {
  const value = data && typeof data === "object" ? (data as Record<string, unknown>)[field] : undefined;
  return typeof value === "string" ? value.trim() || undefined : undefined;
};
export const workKey = (data: unknown): string | undefined => trimmedField(data, "key");

const receipt = (fromId: string, kind: string, value: string): string =>
  createHash("sha256").update(JSON.stringify([fromId, kind, value])).digest("hex");

/** Delivery identity precedence: deliveryId > messageId > commandId > work-key/ref fallbacks. */
export const deliveryIdentity = (data: unknown): { kind: string; value: string } | undefined => {
  if (!data || typeof data !== "object") return undefined;
  for (const field of ["deliveryId", "messageId", "commandId"]) {
    const value = (data as Record<string, unknown>)[field];
    if (typeof value === "string" && value) return { kind: field === "messageId" ? "id" : "delivery", value };
  }
  return undefined;
};
/**
 * A work key or ref claims work within one topic only (review-fleet-2 security P1 on #852): a
 * sender can reuse a key across topics, so a sender+key receipt let an unrelated event match an
 * old receipt and be suppressed. The topic is part of the receipt. Receipts of the older
 * topic-less kinds ("key", "ref") are never matched again: without a verifiable topic the event
 * is injected, because a duplicate shadow is cheap and a lost event is not.
 */
/** Native carriers claim topic-scoped work; mesh events retain their own identities. */
export const topicWorkReceipt = (fromId: string, kind: "key" | "ref", topic: string, value: string): string =>
  receipt(fromId, `native-topic-${kind}`, JSON.stringify([topic, value]));
const workReceipts = (fromId: string, data: unknown, topic: string | undefined): string[] => {
  const identity = deliveryIdentity(data);
  if (identity) return [receipt(fromId, identity.kind, identity.value)];
  // Only native carriers supply a topic; mesh events must never claim key/ref work.
  if (topic !== undefined) for (const kind of ["key", "ref"] as const) {
    const value = trimmedField(data, kind);
    if (value) return [topicWorkReceipt(fromId, kind, topic, value)];
  }
  return [];
};
export const eventReceipt = (id: string): string => receipt("", "event", id);
/**
 * Receipts that a native carrier records for the same identity under its other kind: a shadow's
 * messageId is often the control commandId the follow-up arrived with (recorded as "delivery"),
 * and a shadow's deliveryId can be the carrier id (recorded as "id"). Matching only, never stored.
 */
export const crossIdentityReceipts = (fromId: string, data: unknown): string[] => {
  const identity = deliveryIdentity(data);
  if (!identity) return [];
  return [receipt(fromId, identity.kind === "delivery" ? "id" : "delivery", identity.value)];
};
export const eventReceipts = (event: MeshEvent): string[] => [
  eventReceipt(event.id), receipt(event.from.id, "id", event.id),
  ...(event.dedupeKey ? [receipt("", "dedupe", event.dedupeKey)] : []),
  // Control data is a protocol envelope; nested work identities belong to the native carrier.
  ...(event.topic === "fabric.control.command" ? [] : [
    ...workReceipts(event.from.id, event.data, undefined),
    ...crossIdentityReceipts(event.from.id, event.data),
  ]),
];
/** What the session's own entries say it holds. */
export interface RootInboxSession {
  /** Inbox messages collectively holding all of these event ids. */
  holdsBatch(ids: readonly string[]): boolean;
  /** Agent-carrier presence for this sender/key/topic, including explicit identities.
   * Legacy suppression additionally requires a matching, dated native-topic receipt. */
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
      ...(data ? { data: { key: data.key, ref: data.ref, topic: data.topic, deliveryId: data.deliveryId, messageId: data.messageId, commandId: data.commandId } } : {}),
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
      // Preserve malformed shapes: projection must not manufacture a receipt.
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

/** Only canonical native UTC ISO or integer epoch-ms can date a receipt claim. */
const canonicalReceiptTimestamp = (value: unknown): number | undefined => {
  const at = typeof value === "number" ? value
    : typeof value === "string" && value.length === 24 ? Date.parse(value)
    : Number.NaN;
  if (!Number.isInteger(at) || at <= 0 || at > Date.now() + 60_000) return undefined;
  if (typeof value === "string" && new Date(at).toISOString() !== value) return undefined;
  return at;
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
  const remember = (id: string, at: number | undefined): void => {
    delivered.add(id);
    if (at !== undefined) deliveredAt.set(id, Math.max(deliveredAt.get(id) ?? Number.NEGATIVE_INFINITY, at));
  };
  // Persisted snapshots contain receipts only, with original history positions. All-history
  // identities/legacy steers are needed for dedup; only batch presence uses a lookback.
  for (const [index, value] of [...entries].reverse()) {
    type Carried = { from?: { id?: unknown }; data?: unknown; id?: unknown; deliveryId?: unknown };
    const entry = value as { timestamp?: unknown; type?: string; customType?: string; details?: Carried & { ids?: unknown; items?: unknown; receipts?: unknown } } | undefined;
    if (entry?.type !== "custom_message") continue;
    // Invalid canonical timestamps never renew a legacy claim at scan time. Exact native
    // identity replay evidence remains available without granting dated fallback authority.
    const at = canonicalReceiptTimestamp(entry.timestamp);
    if (entry.customType === ROOT_INBOX_CUSTOM_TYPE) {
      const receipt = verifiedInboxReceipt(entry, verifier, accepted);
      if (receipt) {
        if (index >= Math.max(0, count - lookback)) {
          hasBatch = true;
          for (const id of receipt.ids) batchIds.add(id);
        }
        for (const id of receipt.ids) remember(eventReceipt(id), at);
        for (const id of receipt.receipts) remember(id, at);
      }
    }
    const agent = entry.customType === AGENT_MESSAGE_CUSTOM_TYPE;
    // Native admitted steer receipts retain main's #1226 extraction. Inbox receipts above
    // require the private shown-content verifier; serialized details alone grant nothing.
    if (agent && Array.isArray(entry.details?.receipts)) {
      for (const id of entry.details.receipts) if (typeof id === "string") remember(id, at);
    }
    if (!agent) continue;
    // A batch of followUps (smarty-dev#1495) carries each one in items.
    const carried: Carried[] = Array.isArray(entry.details?.items) ? entry.details.items as Carried[] : [entry.details ?? {}];
    for (const item of carried) {
      const fromId = item?.from?.id;
      const key = workKey(item?.data);
      // A native carrier without data.topic claims no key/ref work.
      const topic = trimmedField(item?.data, "topic");
      const explicit = !!deliveryIdentity(item?.data) || (typeof item?.deliveryId === "string" && !!item.deliveryId);
      if (typeof fromId !== "string") continue;
      // A caller-supplied top-level deliveryId is explicit-only too; retain data identity
      // receipts when present, but do not record a native-topic key/ref for this carrier.
      for (const id of workReceipts(fromId, item.data, explicit ? undefined : topic)) remember(id, at);
      if (typeof item.id === "string") remember(receipt(fromId, "id", item.id), at);
      if (typeof item.deliveryId === "string") remember(receipt(fromId, "delivery", item.deliveryId), at);
      // Presence is independent of suppression authority: explicit carriers also hold this
      // sender/topic/key, but only identity-less carriers above can record native-topic receipts.
      // holdsSteer alone never suppresses work; a matching, dated native-topic receipt is mandatory.
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

const renderEvent = (event: MeshEvent): string => {
  const data = (event.data && typeof event.data === "object" ? event.data : {}) as { ref?: unknown; key?: unknown };
  const attributes = [
    `id="${event.id}"`, `sequence="${event.sequence}"`, `topic="${escapeXml(event.topic)}"`, `kind="${escapeXml(event.kind)}"`,
    `from_name=${JSON.stringify(event.from.name)}`, `from_id=${JSON.stringify(event.from.id)}`,
    ...(typeof data.ref === "string" ? [`ref=${JSON.stringify(data.ref.slice(0, 256))}`] : []),
    ...(typeof data.key === "string" ? [`key=${JSON.stringify(data.key.slice(0, 256))}`] : []),
    `at="${new Date(event.createdAt).toISOString()}"`,
  ];
  return `<event ${attributes.join(" ")}>${escapeXml(bounded(event.text ?? "", event.sequence))}</event>`;
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

/** Overflow is withheld work, not a receipt. Action overflow withholds the remaining action suffix. */
export const rootInboxBudget = (events: readonly MeshEvent[]): { shown: MeshEvent[]; overflow: MeshEvent[] } => {
  const shown: MeshEvent[] = [], overflow: MeshEvent[] = [];
  let bytes = 0;
  let full = false;
  let actionBytes = 0;
  let actionsFull = false;
  for (const event of [...events].sort(compareRootInboxEvents)) {
    if (ROOT_INBOX_ACTION_KINDS.has(event.kind)) {
      const renderedBytes = Buffer.byteLength(renderEvent(event)) + 1; // The row's joining newline.
      if (actionsFull || actionBytes + renderedBytes > INBOX_INJECT_MAX_ACTION_RENDERED_BYTES) {
        actionsFull = true;
        overflow.push(event);
        continue;
      }
      actionBytes += renderedBytes;
    }
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
    ...events.map(renderEvent),
    ...(rest.length ? [overflowLine(rest)] : []),
    "</fabric-inbox>",
  ].join("\n"),
  display: true,
  // Main's event identities only: unseen overflow never claims native key/ref work.
  details: { ids: events.map((event) => event.id), receipts: events.flatMap(eventReceipts) },
  };
};
