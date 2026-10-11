import { createHash } from "node:crypto";
import { MeshBackgroundQueue } from "../core/atomic-write.js";
import { confirmedSessionReceiptSnapshot, type SessionReceiptManager } from "../core/session-receipts.js";
import type { MeshEvent, MeshIdentity, MeshStore } from "../mesh/store.js";

/**
 * A root session's durable inbox (smarty-dev#754 §3.2 step 3): work events addressed to this root
 * on `fleet.*` topics that no steer or follow-up delivered. Senders publish a shadow record there
 * before they steer (the #1255 rule); a steer can time out or reach a root that is shutting down,
 * but the event stays in the mesh and in its archive. The root reconciles at run start, every
 * turn boundary and every settle, from a processing cursor in mesh state.
 *
 * A batch is saved as pending before it is delivered, and the cursor moves past it only once the
 * session's own entries hold its message. Until then every reconcile delivers it again, so work
 * survives a stop between the save and the session's write: delivery is at least once. A work
 * event is skipped when the recipient has a confirmed native/inbox receipt for its sender and
 * work identity, persisted across reloads. Queued messages are not receipts. Addressed shadows
 * older than the configurable horizon are expired on first drain without buying model turns.
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
 * One injection (R-token-waste): at most this many events are rendered in full, with at most this
 * much event text in total and per event; every further event of the batch is summarized in one
 * line. A 20-event batch was ~15k tokens per turn; the budget keeps one injection near 3k.
 */
export const INBOX_INJECT_MAX_EVENTS = 8;
export const INBOX_INJECT_MAX_TEXT_BYTES = 12 * 1024;
export const INBOX_INJECT_EVENT_TEXT_BYTES = 4 * 1024;
/**
 * Every idle wake is a model turn (smarty-dev#1579), so an idle root wakes at most this often,
 * unless the batch holds an urgent event. PI_FABRIC_INBOX_WAKE_COOLDOWN_MS overrides it.
 */
const WAKE_COOLDOWN_MS = 5 * 60_000;
const URGENT_KINDS = new Set(["p0", "steer"]);
/** Each idle wake publishes one event here, so the fleet's wakes per hour can be counted. */
export const ROOT_INBOX_WAKE_TOPIC = "fabric.inbox.wake";
/** Observational, once per pending range, only when a turn proves the root is active. */
export const ROOT_INBOX_ALARM_TOPIC = "fleet.alarm.root-inbox";
const PENDING_ALARM_MS = 15 * 60_000;
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

export interface RootInboxReconcileOptions {
  /** Failed/aborted runs acknowledge held work, but admit and deliver nothing newer. */
  commitOnly?: boolean;
  /** A turn boundary is positive progress evidence for the pending-batch wedge alarm. */
  turnEnd?: boolean;
}

interface RootInboxState {
  after: number;
  pending?: { through: number; ids: string[]; since: number; alarmed?: boolean };
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
/**
 * A work key or ref claims work within one topic only (review-fleet-2 security P1 on #852): a
 * sender can reuse a key across topics, so a sender+key receipt let an unrelated event match an
 * old receipt and be suppressed. The topic is part of the receipt. Receipts of the older
 * topic-less kinds ("key", "ref") are never matched again: without a verifiable topic the event
 * is injected, because a duplicate shadow is cheap and a lost event is not.
 */
const topicWorkReceipt = (fromId: string, kind: "key" | "ref", topic: string, value: string): string =>
  receipt(fromId, `topic-${kind}`, JSON.stringify([topic, value]));
/** The topic a native carrier names for its work (data.topic); without one its key/ref claims nothing. */
const carrierTopic = (data: unknown): string | undefined => {
  const topic = data && typeof data === "object" ? (data as { topic?: unknown }).topic : undefined;
  return typeof topic === "string" && topic.trim() ? topic.trim() : undefined;
};
const workReceipts = (fromId: string, data: unknown, topic: string | undefined): string[] => {
  if (!data || typeof data !== "object") return [];
  const identity = deliveryIdentity(data);
  if (identity) return [receipt(fromId, identity.kind, identity.value)];
  if (topic === undefined) return [];
  const fields = data as Record<string, unknown>;
  const key = workKey(data);
  const ref = typeof fields.ref === "string" && fields.ref.trim() ? fields.ref.trim() : undefined;
  return key ? [topicWorkReceipt(fromId, "key", topic, key)] : ref ? [topicWorkReceipt(fromId, "ref", topic, ref)] : [];
};
const eventReceipt = (id: string): string => receipt("", "event", id);
/**
 * Receipts that a native carrier records for the same identity under its other kind: a shadow's
 * messageId is often the control commandId the follow-up arrived with (recorded as "delivery"),
 * and a shadow's deliveryId can be the carrier id (recorded as "id"). Matching only, never stored.
 */
const crossIdentityReceipts = (fromId: string, data: unknown): string[] => {
  const identity = deliveryIdentity(data);
  if (!identity) return [];
  return [receipt(fromId, identity.kind === "delivery" ? "id" : "delivery", identity.value)];
};
const eventReceipts = (event: MeshEvent): string[] => [
  eventReceipt(event.id), receipt(event.from.id, "id", event.id),
  // Host-only publication keys are mesh-wide, even when a different maintenance owner
  // retries. Carry the key into recipient-scoped confirmed session/inbox receipts too.
  ...(event.dedupeKey ? [receipt("", "dedupe", event.dedupeKey)] : []),
  ...workReceipts(event.from.id, event.data, event.topic),
];
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
  async next(session: RootInboxSession, options: RootInboxReconcileOptions = {}): Promise<RootInboxBatch> {
    const state = this.#load();
    let receiptsChanged = this.#trimReceipts();
    receiptsChanged = JSON.stringify(state) !== this.#saved || receiptsChanged;
    receiptsChanged = this.#remember(session.delivered ?? [], session.deliveredAt) || receiptsChanged;
    let skippedStale = 0;
    if (state.pending) {
      if (options.turnEnd) await this.#alarmPending(state).catch(error => {
        // An observational alarm must never keep confirmed work wedged behind publication.
        console.warn(`[pi-fabric] root inbox alarm: ${error instanceof Error ? error.message : String(error)}`);
      });
      const holdsBatch = session.holdsBatch(state.pending.ids);
      if (options.commitOnly && !holdsBatch) {
        await this.#save(receiptsChanged);
        return { events: [], through: state.after };
      }
      const pending = this.#reread(state.after, state.pending);
      if (holdsBatch) {
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
        });
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
    if (options.commitOnly) {
      await this.#save(receiptsChanged);
      return { events: [], through: state.after };
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
    state.pending = { through: batch.through, ids: batch.events.map((event) => event.id), since: this.#now() };
    await this.#save(true);
    return batch;
  }

  /**
   * The batch an idle root wakes for now (smarty-dev#1595), or undefined. It is `next`, at most
   * once per wake cooldown; an urgent event (kind p0 or steer) wakes at once. Inside the cooldown
   * nothing is saved, so a batch held back does not block an urgent event behind it as pending.
   * The urgency check looks past the batch bounds (review F1): an urgent event behind a full batch
   * still wakes the root at once. Turn/settle reconciliation brings the batches in order
   * without this idle cooldown, so the cursor never skips an event.
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
   * Record injected events as consumed (R-token-waste): once a batch message entered a turn, its
   * events are never injected again, even if the canonical session file does not (yet) confirm the
   * message. Their receipts join the durable delivered set, so the pending batch commits at the
   * next reconcile and a later shadow of the same work is suppressed too.
   */
  async consume(ids: readonly string[], receipts: readonly string[] = []): Promise<void> {
    this.#load();
    this.#trimReceipts();
    const changed = this.#remember([...ids.map(eventReceipt), ...receipts]);
    await this.#save(changed);
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
            events.push(event);
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
    const has = (id: string): boolean => this.#delivered.has(id) || session.delivered?.has(id) === true;
    if (eventReceipts(event).some(has) || crossIdentityReceipts(event.from.id, event.data).some(has)) return true;
    const key = workKey(event.data);
    if (key === undefined) return false;
    // A shadow that carries its own messageId/deliveryId still matches a follow-up or steer that
    // carried the same work key on the same topic and no identity of its own: that carrier claimed
    // the key there. A carrier with a different identity is distinct work (its key receipt is never
    // recorded), and a key receipt on another topic or with no topic never suppresses (see above).
    if (deliveryIdentity(event.data)) return has(topicWorkReceipt(event.from.id, "key", event.topic, key));
    return session.holdsSteer(event.from.id, key, event.topic);
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
      if (Buffer.byteLength(JSON.stringify(state), "utf8") >= budget) low = middle + 1;
      else high = middle;
    }
    assign(low);
    entries = entries.slice(low);
    this.#delivered = new Map(entries);
    return before !== entries.length;
  }

  #horizon(): number { return this.options.horizonMs ?? inboxHorizonMs(); }

  #stale(event: MeshEvent): boolean { return event.createdAt < this.#now() - this.#horizon(); }

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

  async #alarmPending(state: RootInboxState): Promise<void> {
    const pending = state.pending!;
    const heldMs = this.#now() - pending.since;
    if (pending.alarmed || heldMs <= PENDING_ALARM_MS) return;
    // A publication receipt also deduplicates a reload after publish but before cursor save.
    await this.mesh.publish({
      topic: ROOT_INBOX_ALARM_TOPIC, kind: "alarm", from: this.identity,
      dedupeKey: `root-inbox-alarm:${this.identity.id}:${state.after}:${pending.through}`,
      data: { rootId: this.identity.id, pending: { after: state.after, through: pending.through, ids: pending.ids }, heldMs },
    });
    pending.alarmed = true;
  }

  #load(): RootInboxState {
    if (this.#state) return this.#state;
    const stored = this.mesh.get(this.key);
    const value = stored?.value as Partial<RootInboxState> | undefined;
    const saved = typeof value?.after === "number" && Number.isSafeInteger(value.after);
    const pending = value?.pending;
    // A root with no cursor starts at the present: the inbox is for what it misses from now on.
    this.#state = {
      after: saved ? value!.after! : this.mesh.latestSequence(),
      ...(Array.isArray(value?.delivered) ? { delivered: value.delivered.filter((id): id is string => typeof id === "string") } : {}),
      ...(pending && Array.isArray(pending.ids) && typeof pending.through === "number"
        ? { pending: { through: pending.through, ids: pending.ids.filter((id): id is string => typeof id === "string"),
          since: typeof pending.since === "number" && Number.isFinite(pending.since) ? pending.since : stored?.updatedAt ?? this.#now(),
          ...(pending.alarmed === true ? { alarmed: true } : {}),
        } }
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
      for (const id of workReceipts(fromId, item.data, topic)) remember(id, at);
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
export const rootInboxMessage = (batch: readonly MeshEvent[], overflow?: readonly MeshEvent[]) => {
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
  details: { ids: all.map((event) => event.id), receipts: all.flatMap(eventReceipts) },
  };
};
