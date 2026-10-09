import { createHash } from "node:crypto";
import { MeshBackgroundQueue } from "../core/atomic-write.js";
import { confirmedSessionReceiptSnapshot, type SessionReceiptManager } from "../core/session-receipts.js";
import type { MeshEvent, MeshIdentity, MeshStore } from "../mesh/store.js";

/**
 * A root session's durable inbox (smarty-dev#754 §3.2 step 3): work events addressed to this root
 * on `fleet.*` topics that no steer or follow-up delivered. Senders publish a shadow record there
 * before they steer (the #1255 rule); a steer can time out or reach a root that is shutting down,
 * but the event stays in the mesh and in its archive. The root reconciles at turn start and when a
 * completed run settles, from a processing cursor in mesh state.
 *
 * A batch is saved as pending before it is delivered, and the cursor moves past it only once the
 * session's own entries hold its message. Until then every reconcile delivers it again, so work
 * survives a stop between the save and the session's write: delivery is at least once. A work
 * event is skipped when the recipient has a confirmed native/inbox receipt for its sender and
 * work identity, persisted across reloads. Queued messages are not receipts. Addressed shadows
 * older than the configurable horizon are expired on first drain without buying model turns,
 * except answers, blockers, handoffs and completions: those remain actionable work.
 */
export const ROOT_INBOX_PREFIX = "topology/inbox/";
export const WORK_TOPIC_PREFIX = "fleet.";
/**
 * The canonical id namespace of root sessions. A root accepts a name in it only when the name is
 * its own id, never a session name that looks like one: the mesh bridge routes ids in this
 * namespace to other hosts, so native name delivery must stay disjoint from it (smarty-dev#2004).
 */
export const ROOT_ID_PREFIX = "session:";
export const ROOT_INBOX_CUSTOM_TYPE = "pi-fabric-inbox";
/** The custom type Main gives a delivered steer or follow-up (src/main-agent.ts deliverAgent). */
const AGENT_MESSAGE_CUSTOM_TYPE = "pi-fabric-agent-message";
/** A shadow record newer than this waits a reconcile, so its steer can arrive first and win. */
const STEER_GRACE_MS = 60_000;
/** Expire addressed work on first drain, including cursors saved by older runtimes. */
const INBOX_HORIZON_MS = 2 * 60 * 60_000;
const NEVER_EXPIRE_KINDS = new Set(["answer", "blocker", "handoff", "completion"]);
const LATE_DELIVERY_HOUR_MS = 60 * 60_000;
const inboxHorizonMs = (): number => {
  const text = process.env.PI_FABRIC_INBOX_HORIZON_MS;
  const value = Number(text);
  return text?.trim() && Number.isFinite(value) && value > 0 ? value : INBOX_HORIZON_MS;
};
/** The cursor is saved when it moves past a delivered batch, and otherwise at most this often. */
const SAVE_INTERVAL_MS = 10 * 60_000;
/** One batch holds at most this many events and this much text; a longer text is cut. */
const MAX_BATCH_EVENTS = 20;
/** Bound persisted dedup independently of session lifetime; leave 25% for mesh/headroom. */
const MAX_DELIVERED_RECEIPTS = 1024;
const RECEIPT_VALUE_FRACTION = 0.75;
const MAX_BATCH_TEXT_BYTES = 32 * 1024;
const MAX_EVENT_TEXT_BYTES = 8 * 1024;
/**
 * Every idle wake is a model turn (smarty-dev#1579), so an idle root wakes at most this often,
 * unless the batch holds an urgent event. PI_FABRIC_INBOX_WAKE_COOLDOWN_MS overrides it.
 */
const WAKE_COOLDOWN_MS = 5 * 60_000;
const URGENT_KINDS = new Set(["p0", "steer"]);
/** Each idle wake publishes one event here, so the fleet's wakes per hour can be counted. */
export const ROOT_INBOX_WAKE_TOPIC = "fabric.inbox.wake";
const wakeCooldownMs = (): number => {
  const value = Number(process.env.PI_FABRIC_INBOX_WAKE_COOLDOWN_MS);
  return process.env.PI_FABRIC_INBOX_WAKE_COOLDOWN_MS?.trim() && Number.isFinite(value) && value >= 0 ? value : WAKE_COOLDOWN_MS;
};

export interface RootInboxBatch {
  events: MeshEvent[];
  /** The sequence the cursor moves to once these events are in the session. */
  through: number;
  /** Expired addressed shadows skipped in this drain, never delivered as work. */
  skippedStale?: number;
  horizonMs?: number;
}

interface RootInboxState {
  after: number;
  pending?: { through: number; ids: string[] };
  /** Hashed delivery/work identities, scoped by this recipient's state key. */
  delivered?: string[];
  /** Receipt times parallel to delivered. Legacy string-only states migrate on first save. */
  deliveredAt?: number[];
}

const workKey = (data: unknown): string | undefined => {
  const key = data && typeof data === "object" ? (data as { key?: unknown }).key : undefined;
  return typeof key === "string" && key.trim() ? key.trim() : undefined;
};

const receipt = (fromId: string, kind: string, value: string): string =>
  createHash("sha256").update(JSON.stringify([fromId, kind, value])).digest("hex");

/** A delivery identity outranks work-key/ref fallbacks, never the other way around. */
const deliveryIdentity = (data: unknown): { kind: string; value: string } | undefined => {
  if (!data || typeof data !== "object") return undefined;
  const fields = data as Record<string, unknown>;
  if (typeof fields.deliveryId === "string" && fields.deliveryId) return { kind: "delivery", value: fields.deliveryId };
  if (typeof fields.messageId === "string" && fields.messageId) return { kind: "id", value: fields.messageId };
  return undefined;
};
const workReceipts = (fromId: string, data: unknown): string[] => {
  if (!data || typeof data !== "object") return [];
  const identity = deliveryIdentity(data);
  if (identity) return [receipt(fromId, identity.kind, identity.value)];
  const fields = data as Record<string, unknown>;
  const key = workKey(data);
  const ref = typeof fields.ref === "string" && fields.ref.trim() ? fields.ref.trim() : undefined;
  return key ? [receipt(fromId, "key", key)] : ref ? [receipt(fromId, "ref", ref)] : [];
};
const eventReceipt = (id: string): string => receipt("", "event", id);
const eventReceipts = (event: MeshEvent): string[] => [
  eventReceipt(event.id), receipt(event.from.id, "id", event.id),
  // Host-only publication keys are mesh-wide, even when a different maintenance owner
  // retries. Carry the key into recipient-scoped confirmed session/inbox receipts too.
  ...(event.dedupeKey ? [receipt("", "dedupe", event.dedupeKey)] : []),
  ...workReceipts(event.from.id, event.data),
];
/** What the session's own entries say it holds. */
export interface RootInboxSession {
  /** Inbox messages collectively holding all of these event ids. */
  holdsBatch(ids: readonly string[]): boolean;
  /** An agent message (a steer or follow-up) from this sender that carried this work key. */
  holdsSteer(fromId: string, key: string): boolean;
  /** Confirmed native/inbox receipts from the whole history, not the recent-entry window. */
  delivered?: ReadonlySet<string>;
  /** Canonical session-entry times, so rescanning history cannot renew old receipts. */
  deliveredAt?: ReadonlyMap<string, number>;
}

export class RootInbox {
  #state: RootInboxState | undefined;
  #saved: string | undefined;
  #savedAt = 0;
  #wokeAt = Number.NEGATIVE_INFINITY;
  #delivered = new Map<string, number>();
  readonly #notifications = new MeshBackgroundQueue("root inbox wake notification");

  close(): Promise<void> { return this.#notifications.close(); }

  constructor(
    readonly mesh: MeshStore,
    readonly identity: MeshIdentity,
    /** The ids and names a sender may address this root by (its id first). */
    readonly names: () => readonly string[],
    readonly options: { now?: () => number; steerGraceMs?: number; pageSize?: number; wakeCooldownMs?: number; horizonMs?: number } = {},
  ) {}

  get key(): string {
    return ROOT_INBOX_PREFIX + createHash("sha256").update(this.identity.id).digest("hex").slice(0, 32);
  }

  /**
   * The batch to deliver now. A pending batch the session holds is committed first; one it does
   * not hold is delivered again. A new batch stops at the first event younger than the steer
   * grace and at the batch bounds, so the cursor never passes an event it has not admitted.
   */
  async next(session: RootInboxSession): Promise<RootInboxBatch> {
    const state = this.#load();
    let receiptsChanged = this.#trimReceipts();
    receiptsChanged = JSON.stringify(state) !== this.#saved || receiptsChanged;
    receiptsChanged = this.#remember(session.delivered ?? [], session.deliveredAt) || receiptsChanged;
    let skippedStale = 0;
    if (state.pending) {
      const pending = this.#reread(state.after, state.pending);
      if (session.holdsBatch(state.pending.ids)) {
        this.#remember(pending.flatMap(eventReceipts));
      } else {
        // A missing retained event proves neither delivery nor expiry. Keep its id pending.
        const found = new Set(pending.map((event) => event.id));
        const missing = state.pending.ids.filter((id) => !found.has(id) &&
          !this.#delivered.has(eventReceipt(id)) && !session.delivered?.has(eventReceipt(id)));
        // A native delivery can arrive after the pending save. Recheck it on recovery too.
        const events = pending.filter((event) => {
          if (this.#stale(event)) { skippedStale++; return false; }
          if (!this.#steered(event, session)) return true;
          this.#remember(eventReceipts(event));
          return false;
        }).map((event) => this.#late(event));
        if (events.length || missing.length) {
          state.pending.ids = [...events.map((event) => event.id), ...missing];
          // Retry a failed pending-cursor save before delivery: in-memory pending alone
          // is not durable admission, including after a mesh acquisition timeout.
          await this.#save(true);
          return { events, through: state.pending.through, ...(skippedStale ? { skippedStale, horizonMs: this.#horizon() } : {}) };
        }
      }
      state.after = Math.max(state.after, state.pending.through);
      delete state.pending;
      await this.#save(true);
    }
    const batch = this.#scan(state.after, session, true, (event) => {
      receiptsChanged = this.#remember(eventReceipts(event)) || receiptsChanged;
    });
    skippedStale += batch.skippedStale ?? 0;
    if (skippedStale) { batch.skippedStale = skippedStale; batch.horizonMs = this.#horizon(); }
    if (batch.events.length === 0) {
      state.after = batch.through;
      // Expiry and new receipts are durable immediately: a restart must not repeat the summary.
      await this.#save(receiptsChanged || skippedStale > 0);
      return batch;
    }
    state.pending = { through: batch.through, ids: batch.events.map((event) => event.id) };
    await this.#save(true);
    return batch;
  }

  /**
   * The batch an idle root wakes for now (smarty-dev#1595), or undefined. It is `next`, at most
   * once per wake cooldown; an urgent event (kind p0 or steer) wakes at once. Inside the cooldown
   * nothing is saved, so a batch held back does not block an urgent event behind it as pending.
   * The urgency check looks past the batch bounds (review F1): an urgent event behind a full batch
   * still wakes the root at once. The wake brings the batches in order, one per completed run
   * (the settle path has no cooldown), so the cursor never skips an event.
   * `idle` is checked again after the read: a turn that started meanwhile takes the batch itself.
   */
  async wake(session: RootInboxSession, idle: () => boolean): Promise<RootInboxBatch | undefined> {
    const cooling = this.#now() - this.#wokeAt < (this.options.wakeCooldownMs ?? wakeCooldownMs());
    const urgent = this.#peek(session).some((event) => URGENT_KINDS.has(event.kind));
    if (cooling && !urgent) return undefined;
    const batch = await this.next(session);
    if (!idle()) return undefined;
    // A stale-only drain reports once but must not buy a model turn.
    if (batch.events.length === 0) return batch.skippedStale ? batch : undefined;
    const reason = urgent ? "p0" : "idle";
    this.#wokeAt = this.#now();
    void this.#notifications.enqueue(() => this.mesh.publish({
      topic: ROOT_INBOX_WAKE_TOPIC, kind: "idle-wake", from: this.identity,
      data: { count: batch.events.length, reason, ids: batch.events.map((event) => event.id) },
    }));
    return batch;
  }

  /**
   * Set the start boundary now (review F3): a root with no saved cursor starts at the present
   * when it becomes available, not at its first read, so an event sent between the two counts.
   */
  start(): void {
    this.#load();
  }

  /** Every event `next` would bring now or in later batches, without the batch bounds or a save. */
  #peek(session: RootInboxSession): MeshEvent[] {
    const state = this.#load();
    const pending = state.pending && !session.holdsBatch(state.pending.ids) ? state.pending : undefined;
    const after = state.pending ? Math.max(state.after, state.pending.through) : state.after;
    return [...(pending ? this.#reread(state.after, pending).filter((event) =>
      !this.#stale(event) && !this.#steered(event, session)) : []), ...this.#scan(after, session, false).events];
  }

  #scan(after: number, session: RootInboxSession, bounded = true, onDelivered?: (event: MeshEvent) => void): RootInboxBatch {
    const now = this.#now();
    const names = new Set(this.names().filter((name) =>
      name.trim() && (name === this.identity.id || !name.startsWith(ROOT_ID_PREFIX))));
    const cutoff = now - (this.options.steerGraceMs ?? STEER_GRACE_MS);
    const pageSize = this.options.pageSize ?? 500;
    const events: MeshEvent[] = [];
    let through = after;
    let bytes = 0;
    let skippedStale = 0;
    const seen = new Set<string>();
    const result = (): RootInboxBatch => ({ events, through, ...(skippedStale ? { skippedStale, horizonMs: this.#horizon() } : {}) });
    for (;;) {
      const page = this.mesh.read({ after: through, limit: pageSize });
      for (const event of page) {
        if (event.createdAt > cutoff) return result();
        if (event.topic.startsWith(WORK_TOPIC_PREFIX) && event.to !== undefined && names.has(event.to)) {
          if (this.#stale(event)) skippedStale++;
          else if (this.#steered(event, session)) onDelivered?.(event);
          else if (!eventReceipts(event).some((id) => seen.has(id))) {
            const size = Math.min(Buffer.byteLength(event.text ?? ""), MAX_EVENT_TEXT_BYTES);
            if (bounded && (events.length >= MAX_BATCH_EVENTS || (events.length > 0 && bytes + size > MAX_BATCH_TEXT_BYTES))) {
              return result();
            }
            events.push(this.#late(event));
            bytes += size;
            for (const id of eventReceipts(event)) seen.add(id);
          }
        }
        through = Math.max(through, event.sequence);
      }
      if (page.length < pageSize) return result();
    }
  }

  // Positive delivery evidence only, qualified by sender and this recipient's state key.
  // Before Pi records a native message, its shadow remains recoverable (at least once).
  #steered(event: MeshEvent, session: RootInboxSession): boolean {
    const key = deliveryIdentity(event.data) ? undefined : workKey(event.data);
    return eventReceipts(event).some((id) => this.#delivered.has(id) || session.delivered?.has(id)) ||
      (key !== undefined && session.holdsSteer(event.from.id, key));
  }

  #remember(ids: Iterable<string>, times?: ReadonlyMap<string, number>): boolean {
    let changed = false;
    const cutoff = this.#now() - this.#horizon();
    for (const id of ids) {
      // Missing times (legacy/custom callers) retain the original observation time.
      const at = times?.get(id) ?? this.#delivered.get(id) ?? this.#now();
      if (at < cutoff) continue;
      const previous = this.#delivered.get(id);
      if (previous === undefined || at > previous) {
        this.#delivered.set(id, at);
        changed = true;
      }
    }
    return changed;
  }

  /** Oldest first, by canonical receipt time, not the most recent history scan. */
  #trimReceipts(): boolean {
    const before = this.#delivered.size;
    const cutoff = this.#now() - this.#horizon();
    let entries = [...this.#delivered].filter(([, at]) => at >= cutoff)
      .sort((a, b) => a[1] - b[1]).slice(-MAX_DELIVERED_RECEIPTS);
    const state = this.#state!;
    const assign = (offset: number): void => {
      state.delivered = entries.slice(offset).map(([id]) => id);
      state.deliveredAt = entries.slice(offset).map(([, at]) => at);
    };
    // Account for the actual cursor and pending IDs too. Byte-size (not count) protects
    // configured small mesh values; count also caps memory at the default/larger limit.
    const budget = Math.floor(this.mesh.maxEventBytes * RECEIPT_VALUE_FRACTION);
    let low = 0, high = entries.length;
    while (low < high) {
      const middle = Math.floor((low + high) / 2);
      assign(middle);
      if (Buffer.byteLength(JSON.stringify(state), "utf8") > budget) low = middle + 1;
      else high = middle;
    }
    assign(low);
    entries = entries.slice(low);
    this.#delivered = new Map(entries);
    return before !== entries.length;
  }

  #horizon(): number { return this.options.horizonMs ?? inboxHorizonMs(); }

  #neverExpire(event: MeshEvent): boolean {
    if (event.to === undefined) return false;
    return NEVER_EXPIRE_KINDS.has(event.kind);
  }

  #stale(event: MeshEvent): boolean {
    return event.createdAt < this.#now() - this.#horizon() && !this.#neverExpire(event);
  }

  #late(event: MeshEvent): MeshEvent {
    const age = this.#now() - event.createdAt;
    if (age <= this.#horizon() || !this.#neverExpire(event)) return event;
    const hours = Math.floor(age / LATE_DELIVERY_HOUR_MS);
    return { ...event, text: `(delivered late: created ${new Date(event.createdAt).toISOString()}, ${hours} h ago) ${event.text ?? ""}` };
  }

  #reread(after: number, pending: NonNullable<RootInboxState["pending"]>): MeshEvent[] {
    const ids = new Set(pending.ids);
    const found: MeshEvent[] = [];
    const pageSize = this.options.pageSize ?? 500;
    for (let cursor = after; cursor < pending.through;) {
      const page = this.mesh.read({ after: cursor, limit: pageSize });
      if (page.length === 0) break;
      for (const event of page) {
        if (event.sequence > pending.through) return found;
        if (ids.has(event.id)) found.push(event);
        cursor = Math.max(cursor, event.sequence);
      }
      if (page.length < pageSize) break;
    }
    return found;
  }

  #load(): RootInboxState {
    if (this.#state) return this.#state;
    const value = this.mesh.get(this.key)?.value as Partial<RootInboxState> | undefined;
    const saved = typeof value?.after === "number" && Number.isSafeInteger(value.after);
    const pending = value?.pending;
    // A root with no cursor starts at the present: the inbox is for what it misses from now on.
    this.#state = {
      after: saved ? value!.after! : this.mesh.latestSequence(),
      ...(Array.isArray(value?.delivered) ? { delivered: value.delivered.filter((id): id is string => typeof id === "string") } : {}),
      ...(pending && Array.isArray(pending.ids) && typeof pending.through === "number"
        ? { pending: { through: pending.through, ids: pending.ids.filter((id): id is string => typeof id === "string") } }
        : {}),
    };
    this.#delivered = new Map((this.#state.delivered ?? []).map((id, index) => {
      const at = value?.deliveredAt?.[index];
      return [id, typeof at === "number" && Number.isFinite(at) ? at : this.#now()];
    }));
    this.#saved = saved ? JSON.stringify(value) : undefined;
    this.#trimReceipts();
    this.#savedAt = saved ? this.#now() : 0;
    return this.#state;
  }

  async #save(now: boolean): Promise<void> {
    this.#trimReceipts();
    const text = JSON.stringify(this.#state);
    if (text === this.#saved) return;
    if (!now && this.#now() - this.#savedAt < SAVE_INTERVAL_MS) return;
    await this.mesh.put({ key: this.key, value: JSON.parse(text) as RootInboxState, identity: this.identity });
    this.#saved = text;
    this.#savedAt = this.#now();
  }

  #now(): number {
    return this.options.now?.() ?? Date.now();
  }
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
      ...(data ? { data: { key: data.key, ref: data.ref, deliveryId: data.deliveryId, messageId: data.messageId } } : {}),
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
      if (typeof fromId !== "string") continue;
      for (const id of workReceipts(fromId, item.data)) remember(id, at);
      if (typeof item.id === "string") remember(receipt(fromId, "id", item.id), at);
      if (typeof item.deliveryId === "string") remember(receipt(fromId, "delivery", item.deliveryId), at);
      if (key === undefined) continue;
      const keys = steers.get(fromId) ?? new Set<string>();
      keys.add(key);
      steers.set(fromId, keys);
    }
  }
  return {
    delivered, deliveredAt,
    holdsBatch: (ids) => hasBatch && ids.every((id) => batchIds.has(id)),
    holdsSteer: (fromId, key) => steers.get(fromId)?.has(key) ?? false,
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
  if (Buffer.byteLength(text) <= MAX_EVENT_TEXT_BYTES) return text;
  const cut = Buffer.from(text).subarray(0, MAX_EVENT_TEXT_BYTES).toString("utf8").replace(/\uFFFD$/u, "");
  return `${cut}\n[cut at ${MAX_EVENT_TEXT_BYTES} bytes; read the whole event with mesh.read({ after: ${sequence - 1}, limit: 1 })]`;
};

/** A single passive summary, never stale event bodies or a batch of work to act on. */
export const rootInboxSummary = (batch: RootInboxBatch) => ({
  customType: "pi-fabric-inbox-summary",
  content: `Fabric inbox: skipped ${batch.skippedStale ?? 0} addressed shadows older than ${batch.horizonMs ?? INBOX_HORIZON_MS} ms; no stale work injected.`,
  display: true,
  details: { skippedStale: batch.skippedStale ?? 0, horizonMs: batch.horizonMs ?? INBOX_HORIZON_MS },
});
/** The one message that brings a batch into the session. */
export const rootInboxMessage = (events: readonly MeshEvent[]) => ({
  customType: ROOT_INBOX_CUSTOM_TYPE,
  content: [
    `<fabric-inbox count="${events.length}">`,
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
    "</fabric-inbox>",
  ].join("\n"),
  display: true,
  details: { ids: events.map((event) => event.id), receipts: events.flatMap(eventReceipts) },
});
