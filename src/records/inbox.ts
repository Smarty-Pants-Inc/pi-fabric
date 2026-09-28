import type { RecordEnvelope, RecordsOps } from "./store.js";

/**
 * A consumer's reconcile by processing cursor (C4), for a root session: the records addressed to
 * it (`data.to` is one of its names: asks and handoffs) after its cursor, up to the committed
 * frontier. It runs in the same hooks as pi-fabric#94's root inbox (turn start and a completed
 * run's settle). A batch is saved as pending before delivery, and the cursor moves past it only
 * once the session's own entries hold its message: delivery is at least once, never lost.
 * The cursor lives in the database's `consumers` table, where the idle watchdog reads it.
 */
export const RECORDS_INBOX_CUSTOM_TYPE = "pi-fabric-records";
const MAX_BATCH = 20;
const MAX_RECORD_TEXT_BYTES = 8 * 1024;

export interface RecordsInboxBatch { records: RecordEnvelope[]; through: number }
export interface RecordsInboxSession { holdsBatch(ids: readonly string[]): boolean }

export class RecordsInbox {
  constructor(
    readonly store: RecordsOps,
    /** The consumer id: this root's authenticated participant id. */
    readonly consumer: string,
    /** The names a sender may address this root by (its id first). */
    readonly names: () => readonly string[],
    readonly options: { batch?: number; signal?: AbortSignal } = {},
  ) {}

  #names(): string[] {
    return [...new Set([this.consumer, ...this.names()].map((name) => name.trim()).filter(Boolean))];
  }

  async #save(after: number, pending: { through: number; ids: string[] } | null): Promise<void> {
    await this.store.saveConsumer(this.consumer, after, pending, this.options.signal);
  }

  /** The batch to deliver now; empty when nothing addressed to this root is past its cursor. */
  async next(session: RecordsInboxSession): Promise<RecordsInboxBatch> {
    const state = await this.store.openConsumer(this.consumer, this.#names(), this.options.signal);
    let after = state.after;
    if (state.pending) {
      if (!session.holdsBatch(state.pending.ids)) {
        // Delivered again, however many times, until the session holds it: by its saved ids, so a
        // rename of this root (a new alias filter) cannot strand it.
        return { records: await this.store.byIds(state.pending.ids, this.options.signal), through: state.pending.through };
      }
      after = Math.max(after, state.pending.through);
      await this.#save(after, null);
    }
    const page = await this.store.page({ after, limit: this.options.batch ?? MAX_BATCH, origin: this.store.origin, to: this.#names(), exceptAuthor: this.consumer }, this.options.signal);
    if (page.records.length === 0) {
      if (page.next > after) await this.#save(page.next, null);
      return { records: [], through: page.next };
    }
    await this.#save(after, { through: page.next, ids: page.records.map((record) => record.id) });
    return { records: page.records, through: page.next };
  }
}

/** Whether a session's recent entries hold the records message for these ids. */
export const sessionHoldsRecords = (entries: readonly unknown[], ids: readonly string[], lookback = 500): boolean => {
  for (let index = entries.length - 1; index >= Math.max(0, entries.length - lookback); index--) {
    const entry = entries[index] as { type?: string; customType?: string; details?: { ids?: unknown } } | undefined;
    if (entry?.type !== "custom_message" || entry.customType !== RECORDS_INBOX_CUSTOM_TYPE) continue;
    const held = Array.isArray(entry.details?.ids) ? new Set(entry.details.ids) : undefined;
    if (held && ids.every((id) => held.has(id))) return true;
  }
  return false;
};

/** The records inbox's view of a session's recent entries. */
export const recordsInboxSession = (entries: readonly unknown[]): RecordsInboxSession => ({
  holdsBatch: (ids) => sessionHoldsRecords(entries, ids),
});

const escapeXml = (value: string): string => value.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");

const bounded = (record: RecordEnvelope): string => {
  const text = record.text ?? "";
  if (Buffer.byteLength(text) <= MAX_RECORD_TEXT_BYTES) return text;
  const cut = Buffer.from(text).subarray(0, MAX_RECORD_TEXT_BYTES).toString("utf8").replace(/\uFFFD$/u, "");
  return `${cut}\n[cut at ${MAX_RECORD_TEXT_BYTES} bytes; read it with records.get({ ref: ${JSON.stringify(record.ref)} })]`;
};

/** The one message that brings a batch of records into the session. */
export const recordsInboxMessage = (records: readonly RecordEnvelope[]) => ({
  customType: RECORDS_INBOX_CUSTOM_TYPE,
  content: [
    `<fabric-records count="${records.length}">`,
    "Records addressed to you (asks and handoffs) from the Node's record, delivered at least once:",
    ...records.map((record) => {
      const attributes = [
        `id="${record.id}"`, `sequence="${record.sequence}"`, `ref=${JSON.stringify(record.ref)}`, `kind="${record.kind}"`,
        `from=${JSON.stringify(record.from)}`, ...(record.fromName ? [`from_name=${JSON.stringify(record.fromName)}`] : []),
        `at="${new Date(record.createdAt).toISOString()}"`,
      ];
      const data = Object.keys(record.data).length ? `\n<data>${escapeXml(JSON.stringify(record.data).slice(0, 2048))}</data>` : "";
      return `<record ${attributes.join(" ")}>${escapeXml(bounded(record))}${data}</record>`;
    }),
    "</fabric-records>",
  ].join("\n"),
  display: true,
  details: { ids: records.map((record) => record.id) },
});
