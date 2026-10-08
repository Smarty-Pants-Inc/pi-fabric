import fs from "node:fs";
import path from "node:path";
import { meshLockQueueDirectory } from "./lock-queue.js";
import { readHostLeases, type FabricHostLease, type MeshWriterRecord } from "../topology/host-leases.js";

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

const processAlive = (pid: number): boolean => {
  if (!Number.isSafeInteger(pid) || pid <= 0) return false;
  try { process.kill(pid, 0); return true; }
  catch (error) { return (error as NodeJS.ErrnoException).code !== "ESRCH"; }
};

const leaseWriter = (record: MeshWriterRecord): Omit<CensusWriter, "source" | "name"> => ({
  pid: record.pid, host: record.host, releaseSha: record.releaseSha,
  lockProtocol: record.lockProtocol, stateBackend: record.stateBackend, startedAt: record.startedAt,
});

const fromLease = (lease: FabricHostLease): CensusWriter => ({
  ...(lease.writer ? leaseWriter(lease.writer) : {}), source: "host-lease", name: lease.id,
});

const isKnown = (writer: CensusWriter): boolean => typeof writer.pid === "number" &&
  typeof writer.host === "string" && Boolean(writer.releaseSha) && writer.releaseSha !== "unknown" &&
  typeof writer.lockProtocol === "number" && typeof writer.stateBackend === "string" &&
  typeof writer.startedAt === "number";

/** Snapshot current writer leases plus live legacy lock evidence for the L4b cutover tool. */
export async function census(root: string): Promise<WriterCensus> {
  const writers: CensusWriter[] = [];
  const unknown: CensusWriter[] = [];
  const seen = new Set<string>();
  const leases = [...readHostLeases(root).values()];
  const byPid = new Map<number, MeshWriterRecord>(leases.filter(lease => lease.writer).map(lease => [lease.writer!.pid, lease.writer!]));
  const add = (writer: CensusWriter): void => {
    const key = writer.pid === undefined ? `${writer.source}:${writer.name ?? "?"}` : `pid:${writer.pid}`;
    if (seen.has(key)) {
      const prior = writers.find(item => (item.pid === undefined ? `${item.source}:${item.name ?? "?"}` : `pid:${item.pid}`) === key)!;
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
    if (!isKnown(writer)) unknown.push(writer);
  };
  try {
    for (const name of fs.readdirSync(path.join(root, ".writer-census"))) {
      if (!name.endsWith(".json")) continue;
      try {
        const value = JSON.parse(fs.readFileSync(path.join(root, ".writer-census", name), "utf8")) as Record<string, unknown>;
        if (value.format !== 1 || !Number.isSafeInteger(value.pid) || !processAlive(Number(value.pid))) continue;
        const record = value as unknown as MeshWriterRecord;
        byPid.set(record.pid, record);
        add({ ...leaseWriter(record), source: "process-record", name });
      } catch { /* incomplete record is not trusted */ }
    }
  } catch { /* old release, before census records */ }

  for (const lease of leases) if (lease.expiresAt > Date.now()) add(fromLease(lease));

  try {
    const owner = fs.readFileSync(path.join(root, ".lock", "owner"), "utf8").split(/\r?\n/);
    const pid = Number(owner[1]);
    if (processAlive(pid)) {
      const metadata = byPid.get(pid);
      add({ ...(metadata ? leaseWriter(metadata) : { pid }), source: "lock-owner", ...(owner[0] ? { name: owner[0] } : {}) });
    }
  } catch { /* absent owner record */ }

  try {
    const queue = meshLockQueueDirectory(root);
    for (const name of fs.readdirSync(queue).filter(item => /^\d{24}-\d+-[a-f0-9-]+$/.test(item))) {
      const pid = Number(name.split("-")[1]);
      if (!processAlive(pid)) continue;
      const metadata = byPid.get(pid);
      add({ ...(metadata ? leaseWriter(metadata) : { pid }), source: "lock-ticket", name });
    }
  } catch { /* no queue or queue is not readable */ }

  return { writers, unknown, clean: unknown.length === 0 };
}

