import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { meshLockQueueDirectory } from "./lock-queue.js";
import { censusRecordAlive, censusRecordPrunable } from "./store.js";
import {
  validWriterHost, validWriterLockProtocol, validWriterPid, validWriterReleaseSha, validWriterStartedAt,
  validWriterStateBackend,
  readHostLeases, type FabricHostLease, type MeshWriterRecord,
} from "../topology/host-leases.js";

export interface CensusWriter {
  pid?: number;
  host?: string;
  releaseSha?: string;
  lockProtocol?: number;
  stateBackend?: string;
  startedAt?: number;
  source: "process-record" | "host-lease" | "lock-owner" | "lock-ticket";
  name?: string;
  /** Owner receipt/ticket names that corroborate this process. */
  evidence?: string[];
}

export interface WriterCensus {
  writers: CensusWriter[];
  unknown: CensusWriter[];
  clean: boolean;
}

const processAlive = (pid: number): boolean => censusRecordAlive(pid, undefined);

/**
 * Writer metadata with every invalid or unsupported field left absent. Leases are validated on
 * read; process records are not, so an empty host, a non-positive start time, an unsupported lock
 * protocol or backend must not reach `isKnown`. A non-empty releaseSha is kept, even "unknown",
 * so the report shows it; `isKnown` rejects "unknown".
 */
const leaseWriter = (record: MeshWriterRecord): Omit<CensusWriter, "source" | "name"> => {
  const value = record as unknown as Record<string, unknown>;
  return {
    ...(validWriterPid(value.pid) ? { pid: value.pid } : {}),
    ...(validWriterHost(value.host) ? { host: value.host } : {}),
    ...(typeof value.releaseSha === "string" && value.releaseSha.length > 0 ? { releaseSha: value.releaseSha } : {}),
    ...(validWriterLockProtocol(value.lockProtocol) ? { lockProtocol: value.lockProtocol } : {}),
    ...(validWriterStateBackend(value.stateBackend) ? { stateBackend: value.stateBackend } : {}),
    ...(validWriterStartedAt(value.startedAt) ? { startedAt: value.startedAt } : {}),
  };
};

const fromLease = (lease: FabricHostLease): CensusWriter => ({
  ...(lease.writer ? leaseWriter(lease.writer) : {}), source: "host-lease", name: lease.id,
});

const isKnown = (writer: CensusWriter): boolean => validWriterPid(writer.pid) && validWriterHost(writer.host) &&
  validWriterReleaseSha(writer.releaseSha) && validWriterLockProtocol(writer.lockProtocol) &&
  validWriterStateBackend(writer.stateBackend) && validWriterStartedAt(writer.startedAt);

/**
 * A writer's identity is (host, pid, startedAt): after PID reuse, a stale record and the new
 * process's lease share host and pid but not start time, and must stay two writers.
 */
const writerKey = (writer: CensusWriter): string => writer.pid === undefined
  ? `${writer.source}:${writer.name ?? "?"}`
  : `pid:${writer.host ?? "?"}:${writer.pid}:${writer.startedAt ?? "?"}`;

/**
 * Snapshot current writer leases plus live legacy lock evidence for the L4b cutover tool.
 * Liveness (pid alive, same /proc incarnation) is judged only for exactly this host's records with
 * valid metadata, and only those are deleted when dead (best effort, `censusRecordPrunable`).
 * Another host's pid proves nothing here: its record is never dropped, and is unknown unless an
 * unexpired host lease names that writer: same host, pid and start time (a lease of a later process
 * that reused the pid does not vouch for the record). A record without a valid host, pid or start time, or
 * with unsupported metadata, is retained and counted unknown (fail closed).
 */
export async function census(root: string): Promise<WriterCensus> {
  const writers: CensusWriter[] = [];
  const unknown: CensusWriter[] = [];
  const seen = new Set<string>();
  const host = os.hostname();
  const now = Date.now();
  const leases = [...readHostLeases(root).values()];
  const leased = (writer: CensusWriter): boolean => validWriterStartedAt(writer.startedAt) &&
    leases.some(lease => lease.expiresAt > now && lease.writer?.host === writer.host &&
      lease.writer?.pid === writer.pid && lease.writer?.startedAt === writer.startedAt);
  // Exact local match only: a hostless or empty-host record is neither local nor foreign.
  const local = (writer: { host?: unknown }): boolean => writer.host === host;
  const foreign = (writer: { host?: unknown }): boolean => validWriterHost(writer.host) && writer.host !== host;
  // Lock owners/tickets name only a pid; metadata (and so a host) comes from records and leases.
  const byPid = new Map<number, MeshWriterRecord[]>();
  const remember = (record: MeshWriterRecord): void => { byPid.set(record.pid, [...(byPid.get(record.pid) ?? []), record]); };
  for (const lease of leases) if (lease.writer) remember(lease.writer);
  const add = (writer: CensusWriter): void => {
    const key = writerKey(writer);
    if (seen.has(key)) {
      const prior = writers.find(item => writerKey(item) === key)!;
      if (writer.source === "lock-owner" || writer.source === "lock-ticket") {
        prior.evidence = [...(prior.evidence ?? []), `${writer.source}:${writer.name ?? "unknown"}`];
      }
      return;
    }
    seen.add(key);
    if (writer.source === "lock-owner" || writer.source === "lock-ticket") {
      writer.evidence = [`${writer.source}:${writer.name ?? "unknown"}`];
    }
    writers.push(writer);
    if (!isKnown(writer) || (foreign(writer) && !leased(writer))) unknown.push(writer);
  };
  const addLockEvidence = (pid: number, source: "lock-owner" | "lock-ticket", name: string | undefined): void => {
    const metadata = byPid.get(pid) ?? [];
    const mine = metadata.find(record => local(record) && censusRecordAlive(record.pid, record.startedAt));
    if (processAlive(pid)) add({ ...(mine ? leaseWriter(mine) : { pid }), source, ...(name ? { name } : {}) });
    // A same-pid writer on another (or an unnamed) host may own this evidence: never treat it as
    // dead (fail closed).
    for (const record of metadata.filter(record => !local(record))) add({ ...leaseWriter(record), source, ...(name ? { name } : {}) });
  };
  try {
    const directory = path.join(root, ".writer-census");
    for (const name of fs.readdirSync(directory)) {
      if (!name.endsWith(".json")) continue;
      let text: string;
      try { text = fs.readFileSync(path.join(directory, name), "utf8"); }
      catch (error) {
        // Removed since the listing (its writer exited): gone. Any other read failure is unknown.
        if ((error as NodeJS.ErrnoException).code !== "ENOENT") add({ source: "process-record", name });
        continue;
      }
      let value: unknown;
      try { value = JSON.parse(text); } catch { value = undefined; }
      const fields = typeof value === "object" && value !== null ? value as Record<string, unknown> : undefined;
      // Records are renamed into place whole: an unparsable, foreign-format or pid-less one is
      // not trusted, and not dropped either.
      if (!fields || fields.format !== 1 || !validWriterPid(fields.pid)) {
        add({ source: "process-record", name });
        continue;
      }
      if (censusRecordPrunable(fields, host)) {
        try { fs.rmSync(path.join(directory, name), { force: true }); } catch { /* best effort */ }
        continue;
      }
      const record = fields as unknown as MeshWriterRecord;
      remember(record);
      add({ ...leaseWriter(record), source: "process-record", name });
    }
  } catch { /* old release, before census records */ }

  for (const lease of leases) if (lease.expiresAt > now) add(fromLease(lease));

  try {
    const owner = fs.readFileSync(path.join(root, ".lock", "owner"), "utf8").split(/\r?\n/);
    addLockEvidence(Number(owner[1]), "lock-owner", owner[0] || undefined);
  } catch { /* absent owner record */ }

  try {
    const queue = meshLockQueueDirectory(root);
    for (const name of fs.readdirSync(queue).filter(item => /^\d{24}-\d+-[a-f0-9-]+$/.test(item))) {
      addLockEvidence(Number(name.split("-")[1]), "lock-ticket", name);
    }
  } catch { /* no queue or queue is not readable */ }

  return { writers, unknown, clean: unknown.length === 0 };
}
