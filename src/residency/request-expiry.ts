import { randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { writeJsonAtomic } from "../core/atomic-write.js";
import { ownedStat } from "../storage/scratch.js";

export const RESIDENT_REQUEST_RETENTION_MS = 24 * 60 * 60 * 1_000;
/** Old hosts reject format 3 before dispatch, including after a rollback. */
export const RESIDENT_EXPIRING_COMMAND_FORMAT = 3 as const;
export const newResidentRequestId = (now = Date.now()): string => `r1-${now}-${randomUUID()}`;
export const residentRequestGeneration = (requestId: string): number | undefined => {
  const match = /^r1-([0-9]{1,16})-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.exec(requestId);
  const time = match ? Number(match[1]) : NaN;
  return Number.isSafeInteger(time) && time >= 0 ? time : undefined;
};

export class ResidentRequestExpiredError extends Error {
  readonly code = "RESIDENT_REQUEST_EXPIRED" as const;
  constructor(requestId: string) {
    super(`Fabric residency request ${requestId} expired; do not replay or reassign its work. Reconcile the original entity through agents.status / agents.actorStatus.`);
    this.name = "ResidentRequestExpiredError";
  }
}

const floorPath = (root: string): string => path.join(root, "request-expiry.json");
/** Corruption/unsafe files are never equivalent to an absent expiry fence. */
export const readResidentRequestExpiry = (root: string): number => {
  const file = floorPath(root);
  try { fs.lstatSync(file); }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return 0;
    throw new Error("Fabric residency expiry fence is unreadable", { cause: error });
  }
  try {
    const stat = ownedStat(file);
    if (!stat?.isFile() || stat.size > 4096) throw new Error("Unsafe expiry fence");
    const value = JSON.parse(fs.readFileSync(file, "utf8")) as { format?: unknown; expiredBefore?: unknown };
    if (value?.format !== 1 || typeof value.expiredBefore !== "number" || !Number.isSafeInteger(value.expiredBefore) || value.expiredBefore < 0) throw new Error("Invalid expiry fence");
    return value.expiredBefore;
  } catch (error) { throw new Error("Fabric residency expiry fence is unreadable", { cause: error }); }
};

/** Only the single fenced resident host advances this constant-size watermark. */
export const advanceResidentRequestExpiry = (root: string, now: number): number => {
  const previous = readResidentRequestExpiry(root);
  const expiredBefore = Math.max(previous, Math.floor(now - RESIDENT_REQUEST_RETENTION_MS), 0);
  // A previous rename may be visible even though its directory barrier failed.
  // Re-establish durability on every scan, also after restart or clock rollback,
  // before the caller can unlink any fence covered by this watermark.
  if (expiredBefore > 0) writeJsonAtomic(floorPath(root), { format: 1, expiredBefore }, { durable: true });
  return expiredBefore;
};

export const assertResidentRequestNotExpired = (root: string, requestId: string, format?: number): void => {
  const generation = residentRequestGeneration(requestId);
  if (format === RESIDENT_EXPIRING_COMMAND_FORMAT && generation === undefined) throw new Error("Invalid Fabric residency request generation");
  if (generation !== undefined && generation < readResidentRequestExpiry(root)) throw new ResidentRequestExpiredError(requestId);
};
