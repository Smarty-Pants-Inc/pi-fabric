import type { ArchiveFrontierProvider } from "./admission.js";
import type { RecordStore } from "./store.js";

/**
 * One archive check per target per refresh interval for the whole database (C2). Every Fabric
 * process with records open refreshes its gate; only the process that claims the target's row runs
 * the real check (a `wal-g wal-verify` against the archive), and the others read its result. A
 * claimer that dies stops refreshing the stored frontier, so every gate's lag grows (fail closed)
 * until another process claims the expired row.
 */
export class SharedFrontierProvider implements ArchiveFrontierProvider {
  readonly name: string;

  constructor(readonly inner: ArchiveFrontierProvider, readonly store: () => RecordStore, readonly refreshMs: number) {
    this.name = inner.name;
  }

  async frontier(signal?: AbortSignal): Promise<string | undefined> {
    const store = this.store();
    // ponytail: the claim window is a little shorter than the interval, so the claimer re-claims on time.
    const windowSeconds = Math.max(1, Math.floor(this.refreshMs * 0.9) / 1000);
    const claimed = await store.transaction(async (client) => {
      await client.query("INSERT INTO archive_checks (target) VALUES ($1) ON CONFLICT (target) DO NOTHING", [this.name]);
      const { rowCount } = await client.query(
        "UPDATE archive_checks SET claimed_at = clock_timestamp() WHERE target = $1 AND (claimed_at IS NULL OR claimed_at <= clock_timestamp() - make_interval(secs => $2))",
        [this.name, windowSeconds],
      );
      return rowCount === 1;
    }, "", signal);
    if (claimed) {
      let frontier: string | undefined;
      try {
        frontier = await this.inner.frontier(signal);
      } catch (error) {
        await this.#record(undefined, error instanceof Error ? error.message : String(error));
        throw error;
      }
      await this.#record(frontier, frontier === undefined ? "no recoverable WAL frontier" : undefined);
      return frontier;
    }
    return store.transaction(async (client) => {
      const { rows } = await client.query<{ frontier: string | null }>("SELECT frontier::text AS frontier FROM archive_checks WHERE target = $1", [this.name]);
      return rows[0]?.frontier ?? undefined;
    }, "", signal);
  }

  /** A failed check keeps the last good frontier and records why. */
  async #record(frontier: string | undefined, error: string | undefined): Promise<void> {
    await this.store().transaction(async (client) => {
      await client.query(
        `UPDATE archive_checks SET frontier = coalesce($2::pg_lsn, frontier), checked_at = CASE WHEN $2::pg_lsn IS NULL THEN checked_at ELSE clock_timestamp() END,
          error = $3 WHERE target = $1`,
        [this.name, frontier ?? null, error?.slice(0, 500) ?? null],
      );
    });
  }
}
