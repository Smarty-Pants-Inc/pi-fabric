import { createHash } from "node:crypto";
import type { MeshEvent, MeshIdentity, MeshStore } from "../mesh/store.js";

/**
 * A root session's durable inbox (smarty-dev#754 §3.2 step 3): work events addressed to this root
 * on `fleet.*` topics that no steer or follow-up delivered. Senders publish a shadow record there
 * before they steer (the #1255 rule); a steer can time out or reach a root that is shutting down,
 * but the event stays in the mesh and in its archive. The root reconciles at turn start and when
 * it settles, from a processing cursor in mesh state that moves only past what it delivered.
 *
 * Delivery is at least once: after a restart, a work event whose steer came in the last minutes
 * before the cursor was saved can come again, marked as a possible repeat.
 */
export const ROOT_INBOX_PREFIX = "topology/inbox/";
export const WORK_TOPIC_PREFIX = "fleet.";
export const ROOT_INBOX_CUSTOM_TYPE = "pi-fabric-inbox";
/** A shadow record newer than this waits a reconcile, so its steer can arrive first and win. */
const STEER_GRACE_MS = 60_000;
/** Steers delivered in this process are remembered this long, to skip their shadow records. */
const DELIVERED_TTL_MS = 6 * 60 * 60_000;
/** The cursor is saved when it delivers, and otherwise at most this often. */
const SAVE_INTERVAL_MS = 10 * 60_000;

export interface RootInboxBatch {
  events: MeshEvent[];
  /** The sequence the cursor may move to once these events are delivered. */
  through: number;
}

const fingerprint = (fromId: string, text: string): string =>
  createHash("sha256").update(`${fromId}\0${text.trim()}`).digest("hex");

export class RootInbox {
  #cursor: number | undefined;
  #saved: number | undefined;
  #savedAt = 0;
  readonly #delivered = new Map<string, number>();

  constructor(
    readonly mesh: MeshStore,
    readonly identity: MeshIdentity,
    /** The ids and names a sender may address this root by (its id first). */
    readonly names: () => readonly string[],
    readonly options: { now?: () => number; steerGraceMs?: number; pageSize?: number } = {},
  ) {}

  get key(): string {
    return ROOT_INBOX_PREFIX + createHash("sha256").update(this.identity.id).digest("hex").slice(0, 32);
  }

  /** A steer or follow-up reached this root: its shadow record is not news. */
  noteDelivered(fromId: string, text: string): void {
    const now = this.#now();
    for (const [key, at] of this.#delivered) if (now - at > DELIVERED_TTL_MS) this.#delivered.delete(key);
    this.#delivered.set(fingerprint(fromId, text), now);
  }

  /**
   * The work events for this root past its cursor that no steer delivered. The scan stops at the
   * first event younger than the steer grace, so the cursor never passes one it has not judged.
   */
  unseen(): RootInboxBatch {
    let through = this.#load();
    const names = new Set(this.names().filter((name) => name.trim()));
    const cutoff = this.#now() - (this.options.steerGraceMs ?? STEER_GRACE_MS);
    const pageSize = this.options.pageSize ?? 500;
    const events: MeshEvent[] = [];
    for (;;) {
      const page = this.mesh.read({ after: through, limit: pageSize });
      for (const event of page) {
        if (event.createdAt > cutoff) return { events, through };
        through = Math.max(through, event.sequence);
        if (!event.topic.startsWith(WORK_TOPIC_PREFIX) || event.to === undefined || !names.has(event.to)) continue;
        if (this.#delivered.has(fingerprint(event.from.id, event.text ?? ""))) continue;
        events.push(event);
      }
      if (page.length < pageSize) return { events, through };
    }
  }

  /** Moves the cursor after a batch. Saves it at once when the batch delivered anything. */
  async advance(batch: RootInboxBatch): Promise<void> {
    this.#cursor = Math.max(this.#load(), batch.through);
    const due = batch.events.length > 0 || this.#now() - this.#savedAt >= SAVE_INTERVAL_MS;
    if (!due || this.#saved === this.#cursor) return;
    await this.mesh.put({ key: this.key, value: { after: this.#cursor }, identity: this.identity });
    this.#saved = this.#cursor;
    this.#savedAt = this.#now();
  }

  #load(): number {
    if (this.#cursor !== undefined) return this.#cursor;
    const value = this.mesh.get(this.key)?.value as { after?: unknown } | undefined;
    const saved = typeof value?.after === "number" && Number.isSafeInteger(value.after) ? value.after : undefined;
    // A root with no cursor starts at the present: the inbox is for what it misses from now on.
    this.#cursor = saved ?? this.mesh.latestSequence();
    this.#saved = saved;
    this.#savedAt = saved === undefined ? 0 : this.#now();
    return this.#cursor;
  }

  #now(): number {
    return this.options.now?.() ?? Date.now();
  }
}

const escapeXml = (value: string): string =>
  value.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");

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
        ...(typeof data.ref === "string" ? [`ref=${JSON.stringify(data.ref)}`] : []),
        ...(typeof data.key === "string" ? [`key=${JSON.stringify(data.key)}`] : []),
        `at="${new Date(event.createdAt).toISOString()}"`,
      ];
      return `<event ${attributes.join(" ")}>${escapeXml(event.text ?? "")}</event>`;
    }),
    "</fabric-inbox>",
  ].join("\n"),
  display: true,
  details: { ids: events.map((event) => event.id) },
});
