import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { meshLockQueueDirectory } from "./lock-queue.js";
import { censusRecordAlive } from "./store.js";
import {
  readHostLeases, validWriterHost, validWriterStartedAt, type FabricHostLease, type MeshWriterRecord,
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
 * Writer metadata with every invalid field left absent. Leases are validated on read; process
 * records are not, so an empty host or a non-positive start time must not reach `isKnown`.
 */
const leaseWriter = (record: MeshWriterRecord): Omit<CensusWriter, "source" | "name"> => {
  const value = record as unknown as Record<string, unknown>;
  return {
    ...(Number.isSafeInteger(value.pid) ? { pid: value.pid as number } : {}),
    ...(validWriterHost(value.host) ? { host: value.host } : {}),
    ...(typeof value.releaseSha === "string" ? { releaseSha: value.releaseSha } : {}),
    ...(typeof value.lockProtocol === "number" ? { lockProtocol: value.lockProtocol } : {}),
    ...(typeof value.stateBackend === "string" ? { stateBackend: value.stateBackend } : {}),
    ...(validWriterStartedAt(value.startedAt) ? { startedAt: value.startedAt } : {}),
  };
};

const fromLease = (lease: FabricHostLease): CensusWriter => ({
  ...(lease.writer ? leaseWriter(lease.writer) : {}), source: "host-lease", name: lease.id,
});

const isKnown = (writer: CensusWriter): boolean => Number.isSafeInteger(writer.pid) && writer.pid! > 0 &&
  validWriterHost(writer.host) && Boolean(writer.releaseSha) && writer.releaseSha !== "unknown" &&
  typeof writer.lockProtocol === "number" && typeof writer.stateBackend === "string" &&
  validWriterStartedAt(writer.startedAt);

const writerKey = (writer: CensusWriter): string =>
  writer.pid === undefined ? `${writer.source}:${writer.name ?? "?"}` : `pid:${writer.host ?? "?"}:${writer.pid}`;

/**
 * Snapshot current writer leases plus live legacy lock evidence for the L4b cutover tool.
 * Liveness (pid alive, same /proc incarnation) is judged only for this host's records, and this
 * host's dead records are deleted (best effort). Another host's pid proves nothing here: its
 * record is never dropped, and is unknown unless an unexpired host lease names that writer.
 */
export async function census(root: string): Promise<WriterCensus> {
  const writers: CensusWriter[] = [];
  const unknown: CensusWriter[] = [];
  const seen = new Set<string>();
  const host = os.hostname();
  const now = Date.now();
  const leases = [...readHostLeases(root).values()];
  const leased = (writer: CensusWriter): boolean => leases.some(lease => lease.expiresAt > now &&
    lease.writer?.host === writer.host && lease.writer?.pid === writer.pid);
  const foreign = (writer: { host?: unknown }): boolean => typeof writer.host === "string" && writer.host !== host;
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
    const local = metadata.find(record => !foreign(record) && censusRecordAlive(record.pid, record.startedAt));
    if (processAlive(pid)) add({ ...(local ? leaseWriter(local) : { pid }), source, ...(name ? { name } : {}) });
    // A same-pid writer on another host may own this evidence: never treat it as dead (fail closed).
    for (const record of metadata.filter(foreign)) add({ ...leaseWriter(record), source, ...(name ? { name } : {}) });
  };
  try {
    const directory = path.join(root, ".writer-census");
    for (const name of fs.readdirSync(directory)) {
      if (!name.endsWith(".json")) continue;
      try {
        const value = JSON.parse(fs.readFileSync(path.join(directory, name), "utf8")) as Record<string, unknown>;
        if (value.format !== 1 || !Number.isSafeInteger(value.pid)) continue;
        if (!foreign(value) && !censusRecordAlive(Number(value.pid), value.startedAt)) {
          try { fs.rmSync(path.join(directory, name), { force: true }); } catch { /* best effort */ }
          continue;
        }
        const record = value as unknown as MeshWriterRecord;
        remember(record);
        add({ ...leaseWriter(record), source: "process-record", name });
      } catch { /* incomplete record is not trusted */ }
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
