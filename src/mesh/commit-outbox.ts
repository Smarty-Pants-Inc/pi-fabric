import { createHash } from "node:crypto";
import { isMeshLockTimeout } from "../core/atomic-write.js";
import type { MeshBatchOperation, MeshBatchView, MeshIdentity, MeshStore } from "./store.js";

// State-domain helpers for lock call sites (smarty-dev#6477 L2b, plan R3 and R11).
//
// withStateFence: a site that only needs the state lock (a read, a wait, or a claim decided on
// state) runs as a state operation, a writeBatch with no operation of its own, instead of
// `exclusive()` on the mesh `.lock`. On the `file` backend that is the same `.lock`; on SQLite it
// is one `BEGIN IMMEDIATE` transaction that writes nothing. The backend decides, not the caller.
//
// CommitOutbox: file side effects of a state commit (host leases, delivery files) are recorded as
// rows in the SAME commit and run after it, against the committed view. A crash between the commit
// and the effect leaves the row; recover() runs it on the next start. Each effect has an
// idempotency key: a later effect with the same key supersedes the earlier one, and the row is
// deleted (version-fenced) once its effect, or a newer one for the same key, has run.

/** Key family of pending commit effects: `mesh/commit-outbox/<scope hash>/<effect key hash>`. */
export const COMMIT_OUTBOX_PREFIX = "mesh/commit-outbox/";

export type StateFenceStore = Pick<MeshStore, "writeBatch" | "withTryLock">;

/**
 * Runs `body` synchronously under the state write lock, on the committed state, and writes
 * nothing. `timeoutMs` bounds the whole fence (0: one zero-wait try); a bounded try whose prepared
 * snapshot moved is retried while the budget lasts. A body that throws is never retried.
 */
export const withStateFence = async <T>(
  mesh: StateFenceStore, identity: MeshIdentity, body: (view: MeshBatchView) => T, timeoutMs?: number,
): Promise<T> => {
  const deadline = timeoutMs === undefined ? undefined : Date.now() + Math.max(0, timeoutMs);
  for (;;) {
    let entered = false;
    let outcome: { value: T } | undefined;
    const fence = (): Promise<unknown> => mesh.writeBatch({ identity, ops: [], prepare: (view) => {
      entered = true;
      outcome = { value: body(view) };
      return [];
    } });
    try {
      await (deadline === undefined ? fence() : mesh.withTryLock(fence, Math.max(0, deadline - Date.now())));
    } catch (error) {
      if (entered || deadline === undefined || !isMeshLockTimeout(error) || Date.now() >= deadline) throw error;
      await new Promise((resolve) => setTimeout(resolve, 1));
      continue;
    }
    if (!outcome) throw new Error("Mesh state fence did not run its body");
    return outcome.value;
  }
};

export interface CommitOutboxEffect {
  kind: string;
  /** Idempotency key: one pending row per key; a later effect with the same key supersedes. */
  key: string;
  payload: unknown;
}

/** Runs one effect. `view` is the committed state. `replay` is true for a row recovered after a
 * crash or retried after a failure: it may be older than files written since, so a handler must
 * never let it regress them. Must be synchronous and must not call the store's writers. */
export type CommitOutboxHandler = (payload: any, view: MeshBatchView, replay: boolean) => void;

interface OutboxRow { format: 1; scope: string; kind: string; key: string; payload: unknown; recordedAt: number }

const digest = (text: string): string => createHash("sha256").update(text).digest("hex");

const rowOf = (value: unknown): OutboxRow | undefined => {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return undefined;
  const row = value as Partial<OutboxRow>;
  return row.format === 1 && typeof row.scope === "string" && typeof row.kind === "string" &&
    typeof row.key === "string" && "payload" in row ? row as OutboxRow : undefined;
};

export type CommitOutboxStore = Pick<MeshStore, "writeBatch" | "listAll">;

export class CommitOutbox {
  /** Every row of this scope starts with it. */
  readonly prefix: string;
  #staged: Array<{ row: string; effect: CommitOutboxEffect }> = [];
  /** Rows whose effect (or a newer one for the same key) ran, by the version that ran. */
  readonly #done = new Map<string, number>();
  /** Rows whose effect threw: retried on the next run and by recover(). */
  readonly #failed = new Set<string>();

  constructor(
    readonly mesh: CommitOutboxStore,
    /** One scope per writer (a bridge link, a resident host): its rows are its own to replay. */
    readonly scope: string,
    readonly identity: MeshIdentity,
    readonly handlers: Readonly<Record<string, CommitOutboxHandler>>,
  ) {
    this.prefix = `${COMMIT_OUTBOX_PREFIX}${digest(scope).slice(0, 32)}/`;
  }

  rowKey(key: string): string {
    return this.prefix + digest(key);
  }

  /** Rows this process ran and has not yet deleted. */
  get retiring(): number {
    return this.#done.size;
  }

  /**
   * For a batch's `prepare`: returns `ops` plus, when the batch commits anything (`ops` not empty,
   * or `durable`), one row per effect and the deletes of rows already run. A batch with nothing
   * to commit records nothing: it loses nothing in a crash, and run() still runs its effects.
   */
  stage(ops: MeshBatchOperation[], effects: CommitOutboxEffect[], options: { durable?: boolean } = {}): MeshBatchOperation[] {
    const byRow = new Map<string, CommitOutboxEffect>();
    for (const effect of effects) {
      const row = this.rowKey(effect.key);
      byRow.delete(row);
      byRow.set(row, effect);
    }
    this.#staged = [...byRow].map(([row, effect]) => ({ row, effect }));
    if (ops.length === 0 && !options.durable) return ops;
    const recordedAt = Date.now();
    const staged: MeshBatchOperation[] = [...ops];
    for (const [row, { kind, key, payload }] of byRow) {
      const value: OutboxRow = { format: 1, scope: this.scope, kind, key, payload, recordedAt };
      staged.push({ kind: "put", key: row, value });
    }
    for (const [row, version] of this.#done) {
      if (!byRow.has(row)) staged.push({ kind: "delete", key: row, ifVersion: version, onConflict: "skip" });
    }
    return staged;
  }

  /** For the same batch's `afterCommit`: runs the staged effects, then retries failed rows.
   * Returns how many of the staged effects ran. */
  run(view: MeshBatchView, halted?: () => boolean): number {
    const staged = this.#staged;
    this.#staged = [];
    let ran = 0;
    for (const row of [...this.#done.keys()]) if (!view.get(row)) this.#done.delete(row);
    for (const { row, effect } of staged) {
      if (halted?.()) return ran;
      if (this.#execute(row, effect.kind, effect.payload, view, false)) ran += 1;
    }
    for (const row of [...this.#failed]) {
      if (halted?.()) return ran;
      const entry = view.get(row);
      const stored = rowOf(entry?.value);
      if (!stored) { this.#failed.delete(row); continue; }
      this.#execute(row, stored.kind, stored.payload, view, true);
    }
    return ran;
  }

  /** Runs every pending row of this scope on the committed state (after a crash or restart), then
   * deletes the rows that ran. Returns how many effects ran. */
  async recover(): Promise<number> {
    // The common start has nothing to replay: a fresh read, and no lock, says so.
    if (this.mesh.listAll(this.prefix, { fresh: true }).length === 0) return 0;
    let ran = 0;
    await this.mesh.writeBatch({
      identity: this.identity,
      ops: [],
      afterCommit: (view) => {
        for (const entry of view.listAll(this.prefix)) {
          const stored = rowOf(entry.value);
          if (!stored || stored.scope !== this.scope) continue;
          if (this.#execute(entry.key, stored.kind, stored.payload, view, true)) ran += 1;
        }
      },
    });
    await this.retire();
    return ran;
  }

  /** Deletes the rows whose effects ran, in one commit. A row recorded again since keeps its newer
   * version (the delete is version-fenced), and is deleted once that effect runs. */
  async retire(): Promise<void> {
    if (this.#done.size === 0) return;
    const done = [...this.#done];
    await this.mesh.writeBatch({
      identity: this.identity,
      ops: done.map(([key, version]): MeshBatchOperation => ({ kind: "delete", key, ifVersion: version, onConflict: "skip" })),
    });
    for (const [key, version] of done) if (this.#done.get(key) === version) this.#done.delete(key);
  }

  #execute(row: string, kind: string, payload: unknown, view: MeshBatchView, replay: boolean): boolean {
    const handler = this.handlers[kind];
    // An unknown kind belongs to another release of this writer: leave its row alone.
    if (!handler) return false;
    try {
      handler(payload, view, replay);
    } catch {
      this.#failed.add(row);
      return false;
    }
    this.#failed.delete(row);
    const version = view.get(row)?.version;
    if (version !== undefined) this.#done.set(row, version);
    return true;
  }
}
