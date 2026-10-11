/**
 * Dead-root activation filter (smarty-dev#6062). A durable actor keeps running on its resident
 * host after the Main that owns it (its root) is gone; every event it reacts to then spends a
 * model run nobody reads. With `agents.deadRootFilter.mode: "on"`, the resident host skips such an
 * activation, but only on a positive "dead" verdict:
 *
 * - the root's own host lease file (the Main's lease, host id = root id) is present, readable,
 *   names that root and expired more than DEAD_ROOT_GRACE_MS ago; and
 * - the root has no live participant record (a record whose owner host lease is unexpired).
 *
 * Everything else runs (fail-open): a live lease, a live participant, a lease that expired only
 * recently, an unreadable or invalid lease or participant file, a missing lease file, a lease
 * stamped in the future (clock doubt), or any read error. Reads are file-only and lock-free
 * (the lease and participant readers). Only "alive" verdicts are cached (DEAD_ROOT_CACHE_MS per
 * root); a "dead" verdict is never reused, so every skip re-reads the lease and participants.
 */
import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import type { FabricDeadRootFilterConfig } from "../config.js";
import { readHostLease, readHostLeaseSnapshot, type FabricHostLease } from "../topology/host-leases.js";
import { participantFilePresent, readParticipantFile } from "../topology/participant-files.js";

/** How long one root's "alive" verdict is reused. A "dead" verdict is never cached. */
export const DEAD_ROOT_CACHE_MS = 60_000;
/** A root lease must have expired at least this long ago before the root counts as dead. */
export const DEAD_ROOT_GRACE_MS = 10 * 60_000;
/** A lease written further than this in the future means the clocks disagree: doubt. */
const CLOCK_SKEW_MS = 5 * 60_000;
const MAX_CACHED_ROOTS = 1024;
const PARTICIPANT_PREFIX = "topology/participants/";

export interface DeadRootVerdict {
  dead: boolean;
  /** Why: "live lease", "no root lease", "root lease expired 42 min ago; no live participant"... */
  reason: string;
}

/** Exact id, id prefix or exact name. No implicit exemptions (a -supervisor runs only if listed). */
export const deadRootExempt = (config: Pick<FabricDeadRootFilterConfig, "exempt">, actor: { id: string; name: string }): boolean =>
  config.exempt.some((entry) => actor.id.startsWith(entry) || actor.name === entry);

const liveUntil = (lease: FabricHostLease): number =>
  Math.max(lease.expiresAt, lease.session?.expiresAt ?? Number.NEGATIVE_INFINITY);

const record = (value: unknown): Record<string, unknown> | undefined =>
  typeof value === "object" && value !== null && !Array.isArray(value) ? value as Record<string, unknown> : undefined;

type ParticipantState = "live" | "absent" | "not-live" | "doubt";

const participantState = (meshRoot: string, rootId: string, rootLease: FabricHostLease | undefined, now: number): ParticipantState => {
  const key = PARTICIPANT_PREFIX + createHash("sha256").update(rootId).digest("hex");
  if (!participantFilePresent(meshRoot, key)) return "absent";
  const value = record(readParticipantFile(meshRoot, key)?.value);
  if (!value || value.id !== rootId || typeof value.ownerHostId !== "string") return "doubt";
  if (value.status === "reloading" && typeof value.reloadUntil === "number" && value.reloadUntil >= now) return "live";
  const owner = value.ownerHostId === rootId ? rootLease : readHostLease(meshRoot, value.ownerHostId);
  if (!owner) return "doubt";
  return liveUntil(owner) >= now ? "live" : "not-live";
};

/** One uncached verdict for a root. Never throws: an error is doubt, and doubt runs. */
export const judgeRoot = (meshRoot: string, rootId: string, now = Date.now()): DeadRootVerdict => {
  try {
    if (!Number.isFinite(now) || now <= 0) return { dead: false, reason: "clock unreadable" };
    if (!rootId) return { dead: false, reason: "no owning root" };
    const snapshot = readHostLeaseSnapshot(meshRoot, rootId);
    const participant = participantState(meshRoot, rootId, snapshot?.lease, now);
    if (participant === "live") return { dead: false, reason: "live participant" };
    if (participant === "doubt") return { dead: false, reason: "participant record unreadable" };
    if (!snapshot) return { dead: false, reason: "no root lease" };
    const lease = snapshot.lease;
    if (!lease) return { dead: false, reason: "root lease unreadable" };
    if (lease.rootId !== rootId) return { dead: false, reason: "root lease names another root" };
    if (lease.updatedAt > now + CLOCK_SKEW_MS) return { dead: false, reason: "root lease is from the future" };
    const until = liveUntil(lease);
    if (until >= now) return { dead: false, reason: "live lease" };
    if (now - until <= DEAD_ROOT_GRACE_MS) return { dead: false, reason: "root lease expired recently" };
    return { dead: true, reason: `root lease expired ${Math.floor((now - until) / 60_000)} min ago; no live participant` };
  } catch {
    return { dead: false, reason: "root state unreadable" };
  }
};

/**
 * Per-root "alive" verdicts for DEAD_ROOT_CACHE_MS (a cached alive only ever runs, which is the
 * fail-open direction). A "dead" verdict is never cached or reused: every skip is decided by fresh
 * reads of the root lease and the participants (file reads; dead roots are rare), so a root that
 * comes back through a lease renewal or a new live participant on any host runs its next event.
 */
export class DeadRootCache {
  readonly #entries = new Map<string, { at: number; verdict: DeadRootVerdict }>();

  constructor(
    readonly meshRoot: string,
    readonly now: () => number = Date.now,
    readonly ttlMs = DEAD_ROOT_CACHE_MS,
  ) {}

  judge(rootId: string): DeadRootVerdict {
    const now = this.now();
    const hit = this.#entries.get(rootId);
    if (hit && !hit.verdict.dead && now >= hit.at && now - hit.at < this.ttlMs) return hit.verdict;
    const verdict = judgeRoot(this.meshRoot, rootId, now);
    if (verdict.dead) {
      this.#entries.delete(rootId);
      return verdict;
    }
    if (this.#entries.size >= MAX_CACHED_ROOTS && !this.#entries.has(rootId)) this.#entries.clear();
    this.#entries.set(rootId, { at: now, verdict });
    return verdict;
  }
}

export interface DeadRootSkipLine {
  at: string;
  actorId: string;
  actorName: string;
  rootId: string;
  eventId: string;
  topic: string;
  reason: string;
}

export const deadRootSkipsPath = (meshRoot: string): string =>
  path.join(meshRoot, "metrics", "dead-root-skips.jsonl");

/** Appends one JSONL line; a failed append never changes the skip. */
export const appendDeadRootSkip = (meshRoot: string, line: DeadRootSkipLine): void => {
  try {
    const file = deadRootSkipsPath(meshRoot);
    fs.mkdirSync(path.dirname(file), { recursive: true });
    fs.appendFileSync(file, JSON.stringify(line) + "\n");
  } catch {
    // Metrics are best effort.
  }
};
