import fs from "node:fs";
import path from "node:path";
import { writeJsonAtomic } from "../core/atomic-write.js";
import { ownedStat } from "../storage/scratch.js";
import { advanceResidentRequestExpiry, residentRequestGeneration, RESIDENT_REQUEST_RETENTION_MS } from "./request-expiry.js";
import { isResidentCommandOperation, readResidentRequestDecision, type ResidentCommandResponse, type ResidentResponseAcknowledgement } from "./protocol.js";

const SAMPLE_INTERVAL_MS = 60_000;
const directories = ["acknowledgements", "decisions", "responses"] as const;
const time = (value: unknown): value is number => typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
const absent = (file: string): boolean => {
  try { fs.lstatSync(file); return false; }
  catch (error) { return (error as NodeJS.ErrnoException).code === "ENOENT"; }
};
const readOwned = <T>(file: string): T | undefined => {
  if (absent(file)) return undefined;
  const stat = ownedStat(file);
  if (!stat?.isFile() || stat.size > 1024 * 1024) throw new Error("Unsafe residency retention entry");
  return JSON.parse(fs.readFileSync(file, "utf8")) as T;
};
const validAck = (value: ResidentResponseAcknowledgement | undefined, id: string): value is ResidentResponseAcknowledgement =>
  value?.format === 1 && value.requestFormat === 3 && value.requestId === id && time(value.completedAt) && time(value.acknowledgedAt) &&
  value.completedAt >= residentRequestGeneration(id)! &&
  value.acknowledgedAt >= value.completedAt && (value.pending === undefined || typeof value.pending === "string");
const validResponse = (value: ResidentCommandResponse | undefined, id: string): value is ResidentCommandResponse =>
  value?.format === 1 && value.requestId === id && typeof value.ok === "boolean" && time(value.completedAt) &&
  (value.pending === undefined || typeof value.pending === "string");

/**
 * A streaming scan on the host's existing request poll. No new timer, no restart
 * of a truncated scan: a large directory cannot starve its later entries.
 * Unknown, legacy, orphan temporary, live and unacknowledged records are retained.
 */
export class ResidentRequestRetention {
  #directory: fs.Dir | undefined;
  #index = 0;
  #nextSample = 0;
  #scanning = false;
  #expiredBefore = 0;
  #now = 0;
  #health = { entries: 0, bytes: 0, unknown: 0, legacy: 0, collected: 0, sampledAt: 0, error: "" };
  constructor(readonly root: string, readonly actorRoots: readonly string[] = []) {}

  due(now = Date.now()): boolean { return this.#scanning || now >= this.#nextSample; }

  close(): void {
    const directory = this.#directory; this.#directory = undefined;
    try { directory?.closeSync(); } catch { this.#health.unknown++; }
  }

  sweep(now: number, liveIds: ReadonlySet<string>, budgetMs = 5, stoppedWritersGone: ReadonlySet<string> = new Set()): void {
    if (!this.#scanning) {
      if (now < this.#nextSample) return;
      this.#scanning = true; this.#index = 0; this.#now = now;
      this.#health = { entries: 0, bytes: 0, unknown: 0, legacy: 0, collected: 0, sampledAt: now, error: "" };
      try { this.#expiredBefore = advanceResidentRequestExpiry(this.root, now); }
      catch { this.#expiredBefore = 0; this.#health.error = "expiry fence unreadable or could not be advanced; collection disabled"; }
    }
    const started = performance.now();
    while (performance.now() - started < budgetMs) {
      const kind = directories[this.#index];
      if (kind === undefined) {
        this.#scanning = false; this.#nextSample = now + SAMPLE_INTERVAL_MS;
        try { writeJsonAtomic(path.join(this.root, "request-retention.json"), this.#health); } catch { /* next sample retries */ }
        return;
      }
      const directory = path.join(this.root, kind);
      if (!this.#directory) {
        if (absent(directory)) { this.#index++; continue; }
        if (!ownedStat(directory)?.isDirectory()) { this.#health.unknown++; this.#index++; continue; }
        try { this.#directory = fs.opendirSync(directory); }
        catch { this.#health.unknown++; this.#index++; continue; }
      }
      let entry: fs.Dirent | null;
      try { entry = this.#directory.readSync(); }
      catch { this.#health.unknown++; this.close(); this.#index++; continue; }
      if (!entry) { this.close(); this.#index++; continue; }
      const file = path.join(directory, entry.name);
      const id = entry.name.endsWith(".json") ? entry.name.slice(0, -5) : "";
      let unknown = false;
      try {
        const generation = residentRequestGeneration(id);
        if (!id || generation === undefined) {
          if (id) this.#health.legacy++; else unknown = true;
        } else {
          const value = readOwned<ResidentResponseAcknowledgement & ResidentCommandResponse>(file);
          if (kind === "acknowledgements") {
            if (!validAck(value, id)) throw new Error("Invalid acknowledgement");
            if (this.#collect(id, value, liveIds, stoppedWritersGone)) { this.#health.collected++; continue; }
          } else if (kind === "responses") {
            if (!validResponse(value, id)) throw new Error("Invalid response");
          } else {
            const decision = readResidentRequestDecision(this.root, id);
            if (!decision || decision.requestFormat !== 3 || (decision.state === "committed" && !isResidentCommandOperation(decision.operation))) throw new Error("Invalid decision");
          }
        }
      } catch { unknown = true; }
      try {
        const stat = fs.lstatSync(file);
        this.#health.entries++; this.#health.bytes += stat.size;
        if (!ownedStat(file)?.isFile()) unknown = true;
      } catch { if (!absent(file)) unknown = true; }
      if (unknown) this.#health.unknown++;
    }
  }

  #collect(id: string, ack: ResidentResponseAcknowledgement, liveIds: ReadonlySet<string>, stoppedWritersGone: ReadonlySet<string>): boolean {
    const generation = residentRequestGeneration(id)!;
    if (generation >= this.#expiredBefore || this.#now - Math.max(ack.completedAt, ack.acknowledgedAt) <= RESIDENT_REQUEST_RETENTION_MS) return false;
    // An unreadable/symlinked exchange directory is a possible pending reference.
    for (const kind of ["requests", "processing"]) {
      const dir = path.join(this.root, kind);
      if ((!absent(dir) && !ownedStat(dir)?.isDirectory()) || !absent(path.join(dir, `${id}.json`))) return false;
    }
    for (const kind of ["decisions", "responses", "agents"]) {
      const dir = path.join(this.root, kind);
      if (!absent(dir) && !ownedStat(dir)?.isDirectory()) throw new Error("Unsafe residency reference directory");
    }
    const decisionPath = path.join(this.root, "decisions", `${id}.json`);
    const decisionValue = readOwned<unknown>(decisionPath);
    const decision = decisionValue === undefined ? undefined : readResidentRequestDecision(this.root, id);
    if (decision && decision.requestFormat !== 3) throw new Error("Legacy decision is not collectable");
    if (decision?.state === "committed" && !/^[A-Za-z0-9_-]+$/.test(decision.id!)) throw new Error("Invalid resident entity ID");
    if (decision?.state === "committed" && (!isResidentCommandOperation(decision.operation) || liveIds.has("*") || liveIds.has(decision.id!))) return false;
    if (ack.pending && (!decision?.id || liveIds.has(decision.id))) return false;
    if (decision?.state === "committed") {
      const metadata = readOwned<{ id?: unknown; handle?: { status?: unknown } }>(path.join(this.root, "agents", `${decision.id}.json`));
      if (metadata !== undefined) {
        if (metadata?.id !== decision.id || !["queued", "running", "completed", "failed", "stopped", "timed_out"].includes(String(metadata.handle?.status))) throw new Error("Unreadable resident agent reference");
        if (metadata.handle?.status === "queued" || metadata.handle?.status === "running") return false;
      }
    }
    if (decision?.state === "committed" && !["spawn", "foreground", "cleanup"].includes(decision.operation!)) {
      for (const actorRoot of this.actorRoots) {
        if (!absent(actorRoot) && !ownedStat(actorRoot)?.isDirectory()) throw new Error("Unsafe actor reference directory");
        const registry = readOwned<{ actors?: Array<{ id?: unknown; status?: unknown; removal?: unknown }> }>(path.join(actorRoot, "actors.json"));
        if (registry !== undefined) {
          if (!Array.isArray(registry?.actors) || !registry.actors.every(actor => actor && typeof actor.id === "string")) throw new Error("Unreadable resident actor reference");
          const actor = registry.actors.find(actor => actor.id === decision.id);
          if (actor) {
            if (!["idle", "queued", "running", "stopped"].includes(String(actor.status))) throw new Error("Unknown resident actor status");
            // Stopped closes admission; it does not join an executing writer.
            // Only the owning host's full writer/drain snapshot can clear this row.
            if (actor.status !== "stopped" || actor.removal !== undefined || !stoppedWritersGone.has(decision.id!)) return false;
          }
        }
        // Revocation removes the registry row before cleanup finishes. Even an
        // unreadable removal marker is a possible pending obligation, not absence.
        const removal = readOwned<{ id?: unknown }>(path.join(actorRoot, `removal-${decision.id}.json`));
        if (removal !== undefined) {
          if (removal?.id !== decision.id) throw new Error("Unreadable resident removal reference");
          return false;
        }
      }
    }
    const responsePath = path.join(this.root, "responses", `${id}.json`);
    const response = readOwned<ResidentCommandResponse>(responsePath);
    if (response !== undefined && (!validResponse(response, id) || response.completedAt !== ack.completedAt || response.pending !== ack.pending)) throw new Error("Unacknowledged or invalid response");
    // The durable watermark was published first. Partial deletion or a crash is
    // safe: any replay is now expired, even if its createdAt is rewritten.
    fs.rmSync(responsePath, { force: true });
    fs.rmSync(decisionPath, { force: true });
    fs.rmSync(path.join(this.root, "acknowledgements", `${id}.json`), { force: true });
    return true;
  }
}
