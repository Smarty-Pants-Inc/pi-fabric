import fs from "node:fs";
import path from "node:path";
import { writeJsonAtomic } from "../core/atomic-write.js";
import { ownedStat } from "../storage/scratch.js";
import { retentionV2Enabled } from "../storage/retention-platform.js";
import { boundedRunTree } from "../storage/reference-scan.js";
import { canRemoveTerminalRun, runTreeExitVeto, compactTerminalRunEvents, retainedActorRunIds, type TerminalRunEventsRetention } from "../storage/retention.js";
import { hasPreservedResidentResult } from "./preserved-result.js";
import { advanceResidentRequestExpiry, residentRequestGeneration, RESIDENT_REQUEST_RETENTION_MS } from "./request-expiry.js";
import { isResidentCommandOperation, readResidentRequestDecision, type ResidentCommandResponse, type ResidentResponseAcknowledgement } from "./protocol.js";

export const RESIDENT_RUN_RETENTION_MS = 24 * 60 * 60 * 1_000;

/** Called under the host fence: by the mesh-wide sweep (storage/retention-cli.ts) for a resident root whose
 * host is proven dead and flock-fenced. Every existing run is then untracked. */
export const sweepResidentRuns = (
  runsRoot: string, now = Date.now(), budgetMs = 100,
  options: TerminalRunEventsRetention & { actorRoots?: readonly string[]; retainRuns?: boolean; retentionMs?: number } = {},
): string[] => {
  const removed: string[] = [];
  if (!ownedStat(runsRoot)?.isDirectory()) return removed;
  const started = performance.now();
  const expired = () => performance.now() - started >= budgetMs;
  const retained = retainedActorRunIds(options.actorRoots ?? []);
  if (retained.has("*")) return removed;
  let directory: fs.Dir;
  try { directory = fs.opendirSync(runsRoot); } catch { return removed; }
  try {
    let entry: fs.Dirent | null;
    while (!expired() && (entry = directory.readSync())) {
      if (!entry.isDirectory() || retained.has(entry.name)) continue;
      const run = path.join(runsRoot, entry.name);
      const stat = ownedStat(run);
      if (!stat?.isDirectory()) continue;
      if (!options.retainRuns && now - stat.mtimeMs > (options.retentionMs ?? RESIDENT_RUN_RETENTION_MS) &&
          !runTreeExitVeto(run, 0, expired, true) && canRemoveTerminalRun(run, expired) &&
          hasPreservedResidentResult(runsRoot, entry.name) && !expired()) {
        try { fs.rmSync(run, { recursive: true, force: true }); removed.push(run); } catch {}
      } else {
        compactTerminalRunEvents(run, {
          ...(options.terminalRunEventsAgeMs !== undefined ? { terminalRunEventsAgeMs: options.terminalRunEventsAgeMs } : {}),
          ...(options.terminalRunEventsMaxBytes !== undefined ? { terminalRunEventsMaxBytes: options.terminalRunEventsMaxBytes } : {}),
          now, expired });
      }
    }
  } finally { directory.closeSync(); }
  return removed;
};


const SAMPLE_INTERVAL_MS = 60_000;
const directories = ["acknowledgements", "decisions", "responses", "runs"] as const;
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
// Cache only a stable, owned registry generation. Include roots and missing files
// so creation, removal, atomic replacement and in-place writes invalidate it.
// "unsafe" is a wildcard veto, not an empty set of references.
const actorReferenceFingerprint = (roots: readonly string[]): string => {
  const identities: unknown[] = [];
  try {
    for (const root of roots) {
      for (const [file, directory] of [[root, true], [path.join(root, "actors.json"), false]] as const) {
        if (absent(file)) { identities.push([file, null]); continue; }
        const stat = ownedStat(file);
        if (!stat || (directory ? !stat.isDirectory() : !stat.isFile())) return "unsafe";
        identities.push([file, stat.dev, stat.ino, stat.mode, stat.uid, stat.size, stat.mtimeMs, stat.ctimeMs]);
      }
    }
    return JSON.stringify(identities);
  } catch { return "unsafe"; }
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
 * The run phase budgets between complete safety-check/atomic-replacement units:
 * at most the in-progress run can overrun a slice. Registry preparation is kept
 * across slices, but its generation and the live set are checked afresh each call.
 */
export class ResidentRequestRetention {
  #directory: fs.Dir | undefined;
  #index = 0;
  #pendingRun: { entry: fs.Dirent; attempts: number } | undefined;
  #resample = false;
  #nextSample = 0;
  #scanning = false;
  #expiredBefore = 0;
  #now = 0;
  #runReferences: { fingerprint: string; ids: Set<string> } | undefined;
  #health = { entries: 0, bytes: 0, unknown: 0, legacy: 0, collected: 0, sampledAt: 0, error: "" };
  constructor(
    readonly root: string,
    readonly actorRoots: readonly string[] = [],
    readonly retention: TerminalRunEventsRetention & { retainRuns?: boolean } = {},
    readonly recoverRunArchives?: (directory: string, expired?: () => boolean) => void,
    readonly custody?: { run: (id: string) => boolean; reference: (id: string) => boolean },
  ) {}

  resample(): void { if (retentionV2Enabled()) { this.#resample = true; this.#nextSample = 0; } }

  due(now = Date.now()): boolean { return this.#scanning || now >= this.#nextSample; }

  close(): void {
    this.#runReferences = undefined;
    this.#pendingRun = undefined;
    const directory = this.#directory; this.#directory = undefined;
    try { directory?.closeSync(); } catch { this.#health.unknown++; }
  }

  sweep(now: number, liveIds: ReadonlySet<string>, budgetMs = 5, stoppedWritersGone: ReadonlySet<string> = new Set()): void {
    if (!retentionV2Enabled()) return this.#mainSweep(now, liveIds, budgetMs, stoppedWritersGone);
    if (!this.#scanning) {
      if (now < this.#nextSample) return;
      this.#scanning = true; this.#index = 0; this.#now = now;
      this.#health = { entries: 0, bytes: 0, unknown: 0, legacy: 0, collected: 0, sampledAt: now, error: "" };
      try { this.#expiredBefore = advanceResidentRequestExpiry(this.root, now); }
      catch { this.#expiredBefore = 0; this.#health.error = "expiry fence unreadable or could not be advanced; collection disabled"; }
    }
    const started = performance.now();
    const expired = () => performance.now() - started >= budgetMs;
    let entries = 0;
    while (!expired() && entries++ < 64) {
      const kind = directories[this.#index];
      if (kind === undefined) {
        this.#scanning = false; this.#nextSample = this.#resample ? now : now + SAMPLE_INTERVAL_MS;
        this.#resample = false;
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
      if (kind === "runs") {
        const fingerprint = actorReferenceFingerprint(this.actorRoots);
        if (this.#runReferences?.fingerprint !== fingerprint) {
          const ids = fingerprint === "unsafe" ? new Set(["*"]) : retainedActorRunIds(this.actorRoots);
          // Do not publish a snapshot if a registry changed during the read.
          // Crucially, no run-directory cursor has advanced yet.
          if (actorReferenceFingerprint(this.actorRoots) !== fingerprint) { this.#runReferences = undefined; return; }
          this.#runReferences = { fingerprint, ids };
          if (expired()) return;
        }
      }
      const pending = kind === "runs" ? this.#pendingRun : undefined;
      this.#pendingRun = undefined;
      let entry: fs.Dirent | null;
      try { entry = pending?.entry ?? this.#directory.readSync(); }
      catch { this.#health.unknown++; this.close(); this.#index++; continue; }
      if (!entry) { this.close(); this.#index++; continue; }
      const file = path.join(directory, entry.name);
      if (kind === "runs") {
        // The request-proof wildcard is not an exit receipt for any particular run.
        const retainedRuns = this.#runReferences!.ids;
        // Incomplete reference preparation is a veto, never authority to walk
        // or discharge unknown custody. Legacy archival has its own full proof.
        const defer = () => {
          // Retry an unsuccessful unit on a fresh slice, but never let a
          // permanently oversized or vetoed tree starve later IDs.
          if ((pending?.attempts ?? 0) < 2) this.#pendingRun = { entry: entry!, attempts: (pending?.attempts ?? 0) + 1 };
        };
        if (entry.isDirectory()) this.recoverRunArchives?.(file, expired);
        // Recovery is resumable/count-bounded, but even its no-op filesystem
        // checks can consume a whole slice on Windows. Finish this run's bounded
        // compaction attempt before stopping BETWEEN runs: restarting recovery
        // first on every deferred attempt otherwise starves the mutation unit.
        // Fresh exit/tree/archive/latest-run checks below still veto mutation
        // if recovery was incomplete or a writer/source appeared meanwhile.
        // Wildcards never authorize request expiry, but run collection has
        // independent fresh native exit/tree proof and O(1) manager custody.
        if (this.custody?.run(entry.name)) continue;
        // Replay one untracked run (and its nested sources) under the host fence,
        // before any compaction/deletion. Never walk the archive during startup
        // or discharge the custody of a live manager's in-memory settlement.
        // The targeted manager callback above independently checks handle custody.
        if (entry.isDirectory() && !liveIds.has(entry.name) &&
            !retainedRuns.has("*") && !retainedRuns.has(entry.name)) {
          // One count-bounded safety-check + atomic replacement is the progress
          // unit. Stop BETWEEN runs on the 5-ms slice deadline: retrying the
          // whole transaction with that same deadline can starve even a three-
          // file run forever on a slow filesystem. Both fresh safety walks are
          // mandatory; no cached proof authorizes mutation. The 64-entry tree
          // cap and predicate fuel also bound growth during those fresh walks.
          let checks = 0;
          const unitExpired = () => ++checks > 4096;
          if (!boundedRunTree(file, unitExpired)) continue;
          const fingerprint = this.#runReferences!.fingerprint;
          // Same exit/result fences as the former startup sweep, now streaming
          // after the lease is up. A terminal marker alone is never exit evidence.
          const stat = ownedStat(file);
          if (this.retention.retainRuns === false && stat && now - stat.mtimeMs > 24 * 60 * 60 * 1_000 &&
              !runTreeExitVeto(file, 0, unitExpired, true) && canRemoveTerminalRun(file, unitExpired) &&
              hasPreservedResidentResult(directory, entry.name) &&
              boundedRunTree(file, unitExpired) && !unitExpired() &&
              actorReferenceFingerprint(this.actorRoots) === fingerprint) {
            try { fs.rmSync(file, { recursive: true, force: true }); } catch { /* retry next scan */ }
            continue;
          }
          const compacted = compactTerminalRunEvents(file, { ...this.retention, now, expired: unitExpired,
            // Another owner can publish a new latest run during a long safety
            // walk. Recheck the registry generation immediately before replace.
            isRetained: () => actorReferenceFingerprint(this.actorRoots) !== fingerprint || !boundedRunTree(file, unitExpired),
          });
          if (!compacted && expired()) { defer(); return; }
        }
        continue;
      }
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

  // Windows scope cut: verbatim main 9387af87 streaming sweep.
  #mainSweep(now: number, liveIds: ReadonlySet<string>, budgetMs = 5, stoppedWritersGone: ReadonlySet<string> = new Set()): void {
    if (!this.#scanning) {
      if (now < this.#nextSample) return;
      this.#scanning = true; this.#index = 0; this.#now = now;
      this.#health = { entries: 0, bytes: 0, unknown: 0, legacy: 0, collected: 0, sampledAt: now, error: "" };
      try { this.#expiredBefore = advanceResidentRequestExpiry(this.root, now); }
      catch { this.#expiredBefore = 0; this.#health.error = "expiry fence unreadable or could not be advanced; collection disabled"; }
    }
    const started = performance.now();
    const expired = () => performance.now() - started >= budgetMs;
    while (!expired()) {
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
      if (kind === "runs") {
        const fingerprint = actorReferenceFingerprint(this.actorRoots);
        if (this.#runReferences?.fingerprint !== fingerprint) {
          const ids = fingerprint === "unsafe" ? new Set(["*"]) : retainedActorRunIds(this.actorRoots);
          // Do not publish a snapshot if a registry changed during the read.
          // Crucially, no run-directory cursor has advanced yet.
          if (actorReferenceFingerprint(this.actorRoots) !== fingerprint) { this.#runReferences = undefined; return; }
          this.#runReferences = { fingerprint, ids };
          if (expired()) return;
        }
      }
      let entry: fs.Dirent | null;
      try { entry = this.#directory.readSync(); }
      catch { this.#health.unknown++; this.close(); this.#index++; continue; }
      if (!entry) { this.close(); this.#index++; continue; }
      const file = path.join(directory, entry.name);
      if (kind === "runs") {
        // The request-proof wildcard is not an exit receipt for any particular run.
        const retainedRuns = this.#runReferences!.ids;
        // Replay one untracked run (and its nested sources) under the host fence,
        // before any compaction/deletion. Never walk the archive during startup
        // or discharge the custody of a live manager's in-memory settlement.
        if (entry.isDirectory() && !liveIds.has(entry.name)) this.recoverRunArchives?.(file);
        if (entry.isDirectory() && !liveIds.has(entry.name) &&
            !retainedRuns.has("*") && !retainedRuns.has(entry.name)) {
          // One complete safety-check + atomic replacement is the progress unit.
          // The poll's budget is soft at this boundary, like a synchronous file
          // read: stop BETWEEN runs, not midway through every retry of a large
          // tree. Never cache worker-exit proofs or skip either fresh safety walk.
          const fingerprint = this.#runReferences!.fingerprint;
          // Same exit/result fences as the former startup sweep, now streaming
          // after the lease is up. A terminal marker alone is never exit evidence.
          const stat = ownedStat(file);
          if (this.retention.retainRuns === false && stat && now - stat.mtimeMs > 24 * 60 * 60 * 1_000 &&
              !runTreeExitVeto(file, 0, undefined, true) && canRemoveTerminalRun(file) &&
              hasPreservedResidentResult(directory, entry.name) &&
              actorReferenceFingerprint(this.actorRoots) === fingerprint) {
            try { fs.rmSync(file, { recursive: true, force: true }); } catch { /* retry next scan */ }
            continue;
          }
          compactTerminalRunEvents(file, { ...this.retention, now,
            // Another owner can publish a new latest run during a long safety
            // walk. Recheck the registry generation immediately before replace.
            isRetained: () => actorReferenceFingerprint(this.actorRoots) !== fingerprint,
          });
        }
        continue;
      }
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
    if (retentionV2Enabled() && decision?.state === "committed" && this.custody?.reference(decision.id!)) return false;
    if (decision?.state === "committed" && (!isResidentCommandOperation(decision.operation) || liveIds.has("*") || liveIds.has(decision.id!))) return false;
    if (ack.pending && (!decision?.id || liveIds.has(decision.id))) return false;
    if (decision?.state === "committed") {
      const metadata = readOwned<{ id?: unknown; handle?: { status?: unknown } }>(path.join(this.root, "agents", `${decision.id}.json`));
      if (metadata !== undefined) {
        if (metadata?.id !== decision.id || !["queued", "running", "completed", "failed", "stopped", "timed_out"].includes(String(metadata.handle?.status))) throw new Error("Unreadable resident agent reference");
        // Spawn handles are immutable admission snapshots. Only a validated
        // saved terminal result can supersede their stale running state; live
        // writers, unresolved trees and cleanup obligations were vetoed above.
        if ((metadata.handle?.status === "queued" || metadata.handle?.status === "running") &&
            !hasPreservedResidentResult(path.join(this.root, "runs"), decision.id!)) return false;
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
