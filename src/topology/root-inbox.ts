import { createHash } from "node:crypto";
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
 * event is skipped only when the session's entries already hold the steer that carried it (the
 * same sender and work key): an enqueued steer is not yet a delivered one.
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
/** The cursor is saved when it moves past a delivered batch, and otherwise at most this often. */
const SAVE_INTERVAL_MS = 10 * 60_000;
/** One batch holds at most this many events and this much text; a longer text is cut. */
const MAX_BATCH_EVENTS = 20;
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
}

interface RootInboxState {
  after: number;
  pending?: { through: number; ids: string[] };
}

const workKey = (data: unknown): string | undefined => {
  const key = data && typeof data === "object" ? (data as { key?: unknown }).key : undefined;
  return typeof key === "string" && key.trim() ? key.trim() : undefined;
};

/** What the session's own entries say it holds. */
export interface RootInboxSession {
  /** Inbox messages collectively holding all of these event ids. */
  holdsBatch(ids: readonly string[]): boolean;
  /** An agent message (a steer or follow-up) from this sender that carried this work key. */
  holdsSteer(fromId: string, key: string): boolean;
}

export class RootInbox {
  #state: RootInboxState | undefined;
  #saved: string | undefined;
  #savedAt = 0;
  #wokeAt = Number.NEGATIVE_INFINITY;

  constructor(
    readonly mesh: MeshStore,
    readonly identity: MeshIdentity,
    /** The ids and names a sender may address this root by (its id first). */
    readonly names: () => readonly string[],
    readonly options: { now?: () => number; steerGraceMs?: number; pageSize?: number; wakeCooldownMs?: number } = {},
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
    if (state.pending) {
      // Only the session's own record of the message moves the cursor; a pending batch is
      // delivered again, however many times, until then.
      if (!session.holdsBatch(state.pending.ids)) return { events: this.#reread(state.after, state.pending), through: state.pending.through };
      state.after = Math.max(state.after, state.pending.through);
      delete state.pending;
      await this.#save(true);
    }
    const batch = this.#scan(state.after, session);
    if (batch.events.length === 0) {
      state.after = batch.through;
      await this.#save(false);
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
    if (batch.events.length === 0 || !idle()) return undefined;
    const reason = urgent ? "p0" : "idle";
    this.#wokeAt = this.#now();
    void this.mesh.publish({
      topic: ROOT_INBOX_WAKE_TOPIC, kind: "idle-wake", from: this.identity,
      data: { count: batch.events.length, reason, ids: batch.events.map((event) => event.id) },
    }).catch(() => undefined);
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
    return [...(pending ? this.#reread(state.after, pending) : []), ...this.#scan(after, session, false).events];
  }

  #scan(after: number, session: RootInboxSession, bounded = true): RootInboxBatch {
    const now = this.#now();
    const names = new Set(this.names().filter((name) =>
      name.trim() && (name === this.identity.id || !name.startsWith(ROOT_ID_PREFIX))));
    const cutoff = now - (this.options.steerGraceMs ?? STEER_GRACE_MS);
    const pageSize = this.options.pageSize ?? 500;
    const events: MeshEvent[] = [];
    let through = after;
    let bytes = 0;
    for (;;) {
      const page = this.mesh.read({ after: through, limit: pageSize });
      for (const event of page) {
        if (event.createdAt > cutoff) return { events, through };
        if (event.topic.startsWith(WORK_TOPIC_PREFIX) && event.to !== undefined && names.has(event.to) && !this.#steered(event, session)) {
          const size = Math.min(Buffer.byteLength(event.text ?? ""), MAX_EVENT_TEXT_BYTES);
          if (bounded && (events.length >= MAX_BATCH_EVENTS || (events.length > 0 && bytes + size > MAX_BATCH_TEXT_BYTES))) {
            return { events, through };
          }
          events.push(event);
          bytes += size;
        }
        through = Math.max(through, event.sequence);
      }
      if (page.length < pageSize) return { events, through };
    }
  }

  // The session already holds the steer that carried this work: same sender, same work key.
  // Without a key, or before Pi records the steer, the shadow copy comes (at least once).
  #steered(event: MeshEvent, session: RootInboxSession): boolean {
    const key = workKey(event.data);
    return key !== undefined && session.holdsSteer(event.from.id, key);
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
      ...(pending && Array.isArray(pending.ids) && typeof pending.through === "number"
        ? { pending: { through: pending.through, ids: pending.ids.filter((id): id is string => typeof id === "string") } }
        : {}),
    };
    this.#saved = saved ? JSON.stringify(this.#state) : undefined;
    this.#savedAt = saved ? this.#now() : 0;
    return this.#state;
  }

  async #save(now: boolean): Promise<void> {
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

/** The inbox's view of a session's recent entries. */
export const rootInboxSession = (entries: readonly unknown[], lookback = 500): RootInboxSession => ({
  holdsBatch: (ids) => sessionHoldsInboxBatch(entries, ids, lookback),
  holdsSteer: (fromId, key) => {
    for (let index = entries.length - 1; index >= Math.max(0, entries.length - lookback); index--) {
      type Carried = { from?: { id?: unknown }; data?: unknown };
      const entry = entries[index] as { type?: string; customType?: string; details?: Carried & { items?: unknown } } | undefined;
      if (entry?.type !== "custom_message" || entry.customType !== AGENT_MESSAGE_CUSTOM_TYPE) continue;
      // A batch of followUps (smarty-dev#1495) carries each one in items.
      const carried: Carried[] = Array.isArray(entry.details?.items) ? entry.details.items as Carried[] : [entry.details ?? {}];
      if (carried.some((item) => item?.from?.id === fromId && workKey(item.data) === key)) return true;
    }
    return false;
  },
});

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
  details: { ids: events.map((event) => event.id) },
});
