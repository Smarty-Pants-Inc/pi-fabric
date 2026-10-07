import fs from "node:fs";
import path from "node:path";

/**
 * smarty-dev#6337: an activation hung in preparation never fails, so the
 * three-failure owner notice never fires and its queued events wait silently.
 * The resident host's maintenance tick reports such actors once per episode.
 */
export const DEFAULT_STUCK_PREPARING_MS = 600_000;
/** The resident host checks at most this often, whatever its tick rate. */
export const STUCK_PREPARING_CHECK_MS = 10_000;

export interface StuckPreparingLine {
  at: string;
  actorId: string;
  actorName: string;
  rootId: string;
  phase: string;
  since: string;
  stuckMs: number;
  queued: number;
}

export const stuckPreparingPath = (meshRoot: string): string =>
  path.join(meshRoot, "metrics", "stuck-preparing.jsonl");

/** Appends one JSONL line; a failed append never changes the notice. */
export const appendStuckPreparing = (meshRoot: string, line: StuckPreparingLine): void => {
  try {
    const file = stuckPreparingPath(meshRoot);
    fs.mkdirSync(path.dirname(file), { recursive: true });
    fs.appendFileSync(file, JSON.stringify(line) + "\n");
  } catch {
    // Metrics are best effort.
  }
};

export const stuckPreparingNotice = (name: string, stuckMs: number, since: number): string =>
  `Fabric host notice: actor ${name} stuck in preparing for ${Math.floor(stuckMs / 60_000)} min ` +
  `(since ${new Date(since).toISOString()}); its queued events are waiting.`;
