import type { RecordsOps } from "./store.js";

/** What the relay needs from the mesh: MeshStore.publish satisfies it. */
export interface NudgePublisher {
  publish(input: { topic: string; kind: string; to?: string; text: string; data: Record<string, unknown> }): Promise<{ sequence: number }>;
}

const NUDGE_TEXT_BYTES = 2 * 1024;
const BATCH = 50;

const cut = (text: string): string => {
  if (Buffer.byteLength(text) <= NUDGE_TEXT_BYTES) return text;
  return `${Buffer.from(text).subarray(0, NUDGE_TEXT_BYTES).toString("utf8").replace(/\uFFFD$/u, "")}…`;
};

/**
 * Commit, then nudge (C4): publishes each committed record's publication row on the mesh topic
 * record/<owner>/<repo>/<n>, in seq order, and marks it published only after the mesh holds it.
 * A crash anywhere in between leaves the row unpublished (its claim expires), and the next flush,
 * in this process or another, publishes it again: a nudge is at least once, and receivers
 * deduplicate by record id.
 */
export class PublicationRelay {
  #running: Promise<{ published: number; failed: number }> | undefined;
  #again = false;

  constructor(readonly store: RecordsOps, readonly publisher: NudgePublisher, readonly options: { batch?: number } = {}) {}

  /** Publish every unpublished row; concurrent calls share one run and repeat once if asked. */
  flush(signal?: AbortSignal): Promise<{ published: number; failed: number }> {
    if (this.#running) {
      this.#again = true;
      return this.#running;
    }
    const run = (async () => {
      let total = { published: 0, failed: 0 };
      do {
        this.#again = false;
        const round = await this.#round(signal);
        total = { published: total.published + round.published, failed: total.failed + round.failed };
        if (round.failed) break;
      } while (this.#again);
      return total;
    })().finally(() => { this.#running = undefined; });
    this.#running = run;
    return run;
  }

  async #round(signal: AbortSignal | undefined): Promise<{ published: number; failed: number }> {
    let published = 0;
    const limit = this.options.batch ?? BATCH;
    for (;;) {
      const rows = await this.store.claimPublications(limit, signal);
      for (const [index, row] of rows.entries()) {
        let sequence: number;
        try {
          const event = await this.publisher.publish({
            topic: row.topic,
            kind: `record.${row.kind}`,
            ...(row.recipient ? { to: row.recipient } : {}),
            text: cut(row.text ?? `record.${row.kind} on ${row.ref}`),
            data: {
              id: row.recordId, ref: row.ref, sequence: row.sequence, origin: this.store.origin, org: this.store.org,
              kind: row.kind, from: row.from, key: row.key, createdAt: row.createdAt,
            },
          });
          sequence = event.sequence;
        } catch (error) {
          // Keep order: stop at the first failure; the row stays unpublished for the next flush.
          await this.store.failPublication(row, String(error instanceof Error ? error.message : error), signal).catch(() => undefined);
          await this.store.releasePublications(row.claimId, rows.slice(index + 1).map((rest) => rest.recordId), signal).catch(() => undefined);
          return { published, failed: 1 };
        }
        // False when the lease ran out and another relay took the row: it publishes again (at least once).
        if (await this.store.ackPublication(row, sequence, signal)) published++;
      }
      if (rows.length < limit) return { published, failed: 0 };
    }
  }

  /** Rows still waiting for their nudge (for status and tests). */
  unpublished(signal?: AbortSignal): Promise<number> {
    return this.store.unpublished(signal);
  }
}
