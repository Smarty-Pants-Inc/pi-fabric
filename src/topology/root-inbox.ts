import { createHash } from "node:crypto";
import { MeshBackgroundQueue } from "../core/atomic-write.js";
import type { MeshEvent, MeshIdentity, MeshStore } from "../mesh/store.js";
import {
  INBOX_HORIZON_MS, ROOT_ID_PREFIX, crossIdentityReceipts, deliveryIdentity, eventReceipt, eventReceipts, topicWorkReceipt, workKey,
  type RootInboxBatch, type RootInboxEvent, type RootInboxReconcileOptions, type RootInboxSession,
} from "./root-inbox-shared.js";

export {
  INBOX_INJECT_EVENT_TEXT_BYTES, INBOX_INJECT_MAX_EVENTS, INBOX_INJECT_MAX_TEXT_BYTES, ROOT_ID_PREFIX, ROOT_INBOX_CUSTOM_TYPE,
  confirmedMainInboxIds, confirmedRootInboxSession, projectRootInboxReceipt, rootInboxBudget, rootInboxMessage, rootInboxSession,
  rootInboxSummary, sessionHoldsInboxBatch,
  type RootInboxBatch, type RootInboxEvent, type RootInboxReconcileOptions, type RootInboxSession,
} from "./root-inbox-shared.js";

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
 * older than the configurable horizon are expired on first drain without buying model turns,
 * except answers, blockers, handoffs and completions: those remain actionable work.
 *
 * An event whose message entered a turn is acknowledged by id (smarty-dev#8962): it is never
 * injected again, whatever the session file confirms. A fully acknowledged batch moves the cursor
 * at once. A per-root high-water mark (never lowered, merged from the stored state on every save)
 * plus a bounded set of out-of-order acked ids keep a re-scan from an older cursor, a reload or a
 * stale writer from injecting anything at or below the mark again.
 */
export const ROOT_INBOX_PREFIX = "topology/inbox/";
export const WORK_TOPIC_PREFIX = "fleet.";
/** A shadow record newer than this waits a reconcile, so its steer can arrive first and win. */
const STEER_GRACE_MS = 60_000;
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
/** Acked ids kept above the high-water mark; a pending batch holds at most MAX_BATCH_EVENTS. */
const MAX_ACKED_IDS = 256;
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
/** Observational, once per pending range, only when a turn proves the root is active. */
export const ROOT_INBOX_ALARM_TOPIC = "fleet.alarm.root-inbox";
const PENDING_ALARM_MS = 15 * 60_000;
const wakeCooldownMs = (): number => {
  const value = Number(process.env.PI_FABRIC_INBOX_WAKE_COOLDOWN_MS);
  return process.env.PI_FABRIC_INBOX_WAKE_COOLDOWN_MS?.trim() && Number.isFinite(value) && value >= 0 ? value : WAKE_COOLDOWN_MS;
};

interface RootInboxState {
  after: number;
  pending?: { through: number; ids: string[]; since: number; alarmed?: boolean };
  /** Hashed delivery/work identities, scoped by this recipient's state key. */
  delivered?: string[];
  /** Receipt times parallel to delivered. Legacy string-only states migrate on first save. */
  deliveredAt?: number[];
  /** High-water mark above `after`: no event at or below it is injected again. Never lowered. */
  ackedThrough?: number;
  /** Ids acked out of order above the mark, each with an upper bound of its mesh sequence. */
  acked?: Array<[string, number]>;
}

const sequenceOf = (value: unknown): number =>
  typeof value === "number" && Number.isSafeInteger(value) ? value : Number.NEGATIVE_INFINITY;
const parseAcks = (value: unknown): Array<[string, number]> => Array.isArray(value)
  ? value.filter((item): item is [string, number] => Array.isArray(item) && typeof item[0] === "string" &&
    typeof item[1] === "number" && Number.isFinite(item[1])).map(([id, bound]) => [id, bound])
  : [];

export class RootInbox {
  #state: RootInboxState | undefined;
  #saved: string | undefined;
  #savedAt = 0;
  #wokeAt = Number.NEGATIVE_INFINITY;
  #delivered = new Map<string, number>();
  #acks = new Map<string, number>();
  readonly #notifications = new MeshBackgroundQueue("root inbox wake notification");
  /** An ack is one-shot: its own queue retries a mesh lock timeout instead of dropping it. */
  readonly #ackWrites = new MeshBackgroundQueue("root inbox ack");

  async close(): Promise<void> { await Promise.all([this.#ackWrites.close(), this.#notifications.close()]); }

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
    if (state.pending && options.turnEnd) await this.#alarmPending(state).catch(error => {
      // An observational alarm must never keep confirmed work wedged behind publication.
      console.warn(`[pi-fabric] root inbox alarm: ${error instanceof Error ? error.message : String(error)}`);
    });
    // An ack can land while this read awaits the mesh: it may commit this very batch.
    const held = state.pending;
    if (held) {
      // Acked ids are never delivered again (smarty-dev#8962); a fully acked batch commits.
      const unacked = held.through <= this.#mark() ? [] : held.ids.filter((id) => !this.#acks.has(id));
      const holdsBatch = unacked.length === 0 || session.holdsBatch(held.ids);
      if (options.commitOnly && !holdsBatch) {
        await this.#save(receiptsChanged);
        return { events: [], through: state.after };
      }
      const reread = this.#reread(state.after, held);
      const pending = reread.filter((event) => !this.#acked(event));
      if (holdsBatch) {
        this.#remember(pending.flatMap(eventReceipts));
      } else {
        // A missing retained event proves neither delivery nor expiry. Keep its id pending, until
        // the batch itself is older than the stale horizon: its events are older still.
        const found = new Set(reread.map((event) => event.id));
        let missing = unacked.filter((id) => !found.has(id) &&
          !this.#delivered.has(eventReceipt(id)) && !session.delivered?.has(eventReceipt(id)));
        if (missing.length && this.#now() - held.since > this.#horizon()) {
          skippedStale += missing.length;
          missing = [];
        }
        // A native delivery can arrive after the pending save. Recheck it on recovery too.
        const events = pending.filter((event) => {
          if (this.#stale(event)) { skippedStale++; return false; }
          if (!this.#steered(event, session)) return true;
          this.#remember(eventReceipts(event));
          return false;
        }).map((event) => this.#late(event));
        if (events.length || missing.length) {
          held.ids = [...events.map((event) => event.id), ...missing];
          // Retry a failed pending-cursor save before delivery: in-memory pending alone
          // is not durable admission, including after a mesh acquisition timeout.
          await this.#save(true);
          return this.#unacked({ events, through: held.through, ...(skippedStale ? { skippedStale, horizonMs: this.#horizon() } : {}) });
        }
      }
      state.after = Math.max(state.after, held.through);
      if (state.pending === held) delete state.pending;
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
    return this.#unacked(batch);
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
    // A stale-only drain reports once but must not buy a model turn; nor does late actionable work.
    if (batch.events.length === 0) return batch.skippedStale ? batch : undefined;
    const waking = batch.events.filter((event) => !event.deliveredLate);
    if (waking.length === 0) return batch;
    const reason = urgent ? "p0" : "idle";
    this.#wokeAt = this.#now();
    void this.#notifications.enqueue(() => this.mesh.publish({
      topic: ROOT_INBOX_WAKE_TOPIC, kind: "idle-wake", from: this.identity,
      data: { count: waking.length, reason, ids: waking.map((event) => event.id) },
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
    this.ack(ids, receipts);
    await this.#ackWrites.enqueue(() => this.#save(true));
  }

  /**
   * Acknowledge events by id (smarty-dev#8962), in memory at once: a read in flight cannot
   * redeliver them, and `consume` persists the ack. An id of the pending batch is kept until the
   * whole batch is acked; then the cursor and the high-water mark move past the batch.
   */
  ack(ids: readonly string[], receipts: readonly string[] = []): void {
    const state = this.#load();
    this.#trimReceipts();
    this.#remember([...ids.map(eventReceipt), ...receipts]);
    const pending = state.pending;
    for (const id of ids) {
      if (!this.#acks.has(id)) this.#acks.set(id, pending?.ids.includes(id) ? pending.through : state.after);
    }
    if (pending && pending.ids.every((id) => this.#acks.has(id))) {
      state.after = Math.max(state.after, pending.through);
      delete state.pending;
    }
    this.#syncAcks();
  }

  /**
   * Whether an event was acknowledged (smarty-dev#8962): its id is in the acked set, or its
   * sequence is at or below the high-water mark. Reads the cached state only, never the mesh, so the
   * context hook can ask on every request; before the first load nothing counts as acked.
   */
  acked(id: string, sequence?: number): boolean {
    if (!this.#state) return false;
    return this.#acks.has(id) || (sequence !== undefined && sequence <= this.#mark());
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
      !this.#acked(event) && !this.#stale(event) && !this.#steered(event, session)) : []), ...this.#scan(after, session, false).events];
  }

  /** Everything at or below this sequence was delivered and acked, or decided: never again. */
  #mark(): number {
    const state = this.#state!;
    return Math.max(state.after, state.ackedThrough ?? Number.NEGATIVE_INFINITY);
  }

  #acked(event: MeshEvent): boolean { return event.sequence <= this.#mark() || this.#acks.has(event.id); }

  /** Drop what an ack recorded while this read awaited the mesh. */
  #unacked(batch: RootInboxBatch): RootInboxBatch {
    const events = batch.events.filter((event) => !this.#acked(event));
    return events.length === batch.events.length ? batch : { ...batch, events };
  }

  /** Prune acks the mark covers and bound the rest; keep the stored mark only above the cursor. */
  #syncAcks(): void {
    const state = this.#state!;
    if (state.ackedThrough !== undefined && state.ackedThrough <= state.after) delete state.ackedThrough;
    const mark = this.#mark();
    let entries = [...this.#acks].filter(([, bound]) => bound > mark);
    if (entries.length > MAX_ACKED_IDS) entries = entries.sort((a, b) => a[1] - b[1]).slice(-MAX_ACKED_IDS);
    this.#acks = new Map(entries);
    if (entries.length) state.acked = entries;
    else delete state.acked;
  }

  /**
   * Another writer of this cursor (a reloaded predecessor finishing a read) can hold an older
   * cached view. The stored mark and acks are merged in before a save, so they are never lowered.
   */
  #absorb(): void {
    const value = this.mesh.get(this.key)?.value as Partial<RootInboxState> | undefined;
    if (!value || typeof value !== "object") return;
    const state = this.#state!;
    const theirs = Math.max(sequenceOf(value.after), sequenceOf(value.ackedThrough));
    if (theirs > this.#mark()) state.ackedThrough = theirs;
    for (const [id, bound] of parseAcks(value.acked)) if (!this.#acks.has(id)) this.#acks.set(id, bound);
    this.#syncAcks();
  }

  #scan(after: number, session: RootInboxSession, bounded = true, onDelivered?: (event: MeshEvent) => void): RootInboxBatch {
    const now = this.#now();
    const names = new Set(this.names().filter((name) =>
      name.trim() && (name === this.identity.id || !name.startsWith(ROOT_ID_PREFIX))));
    const cutoff = now - (this.options.steerGraceMs ?? STEER_GRACE_MS);
    const pageSize = this.options.pageSize ?? 500;
    const events: RootInboxEvent[] = [];
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
          if (this.#acked(event)) { /* Delivered and acked, at or below the mark: never again. */ }
          else if (this.#stale(event)) skippedStale++;
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

  #neverExpire(event: MeshEvent): boolean { return event.to !== undefined && NEVER_EXPIRE_KINDS.has(event.kind); }

  #stale(event: MeshEvent): boolean {
    return event.createdAt < this.#now() - this.#horizon() && !this.#neverExpire(event);
  }

  #late(event: MeshEvent): RootInboxEvent {
    const age = this.#now() - event.createdAt;
    if (age <= this.#horizon() || !this.#neverExpire(event)) return event;
    const hours = Math.floor(age / LATE_DELIVERY_HOUR_MS);
    return { ...event, deliveredLate: true, text: `(delivered late: created ${new Date(event.createdAt).toISOString()}, ${hours} h ago) ${event.text ?? ""}` };
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
      ...(saved && sequenceOf(value!.ackedThrough) > value!.after! ? { ackedThrough: value!.ackedThrough! } : {}),
    };
    this.#acks = new Map(parseAcks(value?.acked));
    this.#syncAcks();
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
    this.#absorb();
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
