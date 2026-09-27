import type { RecordStore } from "./store.js";

/** What the relay needs from the mesh: MeshStore.publish satisfies it. */
export interface NudgePublisher {
  publish(input: { topic: string; kind: string; to?: string; text: string; data: Record<string, unknown> }): Promise<{ sequence: number }>;
}

const NUDGE_TEXT_BYTES = 2 * 1024;

const cut = (text: string): string => {
  if (Buffer.byteLength(text) <= NUDGE_TEXT_BYTES) return text;
  return `${Buffer.from(text).subarray(0, NUDGE_TEXT_BYTES).toString("utf8").replace(/\uFFFD$/u, "")}…`;
};

/**
 * Commit, then nudge (C4): publishes each committed record's publication row on the mesh topic
 * record/<owner>/<repo>/<n>, in seq order, and marks it published only after the mesh holds it.
 * A crash anywhere in between leaves the row unpublished, and the next flush publishes it again:
 * a nudge is at least once, and receivers deduplicate by record id.
 */
export class PublicationRelay {
  #running: Promise<{ published: number; failed: number }> | undefined;
  #again = false;

  constructor(readonly store: RecordStore, readonly publisher: NudgePublisher, readonly options: { batch?: number } = {}) {}

  /** Publish every unpublished row; concurrent calls share one run and repeat once if asked. */
  flush(): Promise<{ published: number; failed: number }> {
    if (this.#running) {
      this.#again = true;
      return this.#running;
    }
    const run = (async () => {
      let total = { published: 0, failed: 0 };
      do {
        this.#again = false;
        const round = await this.#round();
        total = { published: total.published + round.published, failed: total.failed + round.failed };
        if (round.failed) break;
      } while (this.#again);
      return total;
    })().finally(() => { this.#running = undefined; });
    this.#running = run;
    return run;
  }

  async #round(): Promise<{ published: number; failed: number }> {
    let published = 0;
    const limit = this.options.batch ?? 50;
    for (;;) {
      const result = await this.store.transaction(async (client) => {
        // SKIP LOCKED: another process's relay takes other rows, never the same one twice at once.
        const { rows } = await client.query<{ record_id: string; seq: string; topic: string; recipient: string | null; kind: string; ref: string; author: string; key: string; text: string | null; created_at: Date }>(
          `SELECT p.record_id, p.seq, p.topic, p.recipient, r.kind, r.ref, r.author, r.key, r.text, r.created_at
           FROM publication p JOIN records r ON r.id = p.record_id
           WHERE p.published_at IS NULL AND p.origin = $1 ORDER BY p.seq LIMIT $2 FOR UPDATE OF p SKIP LOCKED`,
          [this.store.origin, limit],
        );
        let done = 0;
        for (const row of rows) {
          try {
            const event = await this.publisher.publish({
              topic: row.topic,
              kind: `record.${row.kind}`,
              ...(row.recipient ? { to: row.recipient } : {}),
              text: cut(row.text ?? `record.${row.kind} on ${row.ref}`),
              data: {
                id: row.record_id, ref: row.ref, sequence: Number(row.seq), origin: this.store.origin, org: this.store.org,
                kind: row.kind, from: row.author, key: row.key, createdAt: row.created_at.getTime(),
              },
            });
            await client.query("UPDATE publication SET published_at = clock_timestamp(), mesh_sequence = $2, attempts = attempts + 1, error = NULL WHERE record_id = $1", [row.record_id, event.sequence]);
            done++;
          } catch (error) {
            // Keep order: stop at the first failure; the row stays unpublished for the next flush.
            await client.query("UPDATE publication SET attempts = attempts + 1, error = $2 WHERE record_id = $1", [row.record_id, String(error instanceof Error ? error.message : error).slice(0, 500)]);
            return { done, failed: 1, more: false };
          }
        }
        return { done, failed: 0, more: rows.length === limit };
      });
      published += result.done;
      if (result.failed || !result.more) return { published, failed: result.failed };
    }
  }

  /** Rows still waiting for their nudge (for status and tests). */
  async unpublished(): Promise<number> {
    return this.store.transaction(async (client) => {
      const { rows } = await client.query<{ n: string }>("SELECT count(*) AS n FROM publication WHERE published_at IS NULL AND origin = $1", [this.store.origin]);
      return Number(rows[0]!.n);
    });
  }
}
