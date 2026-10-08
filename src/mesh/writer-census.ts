import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { meshLockQueueDirectory } from "./lock-queue.js";
import { censusHostSlug, censusRecordAlive, censusRecordPrunable } from "./store.js";
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
  /** Every backend the process opened, when more than one; stateBackend is the most dangerous. */
  stateBackends?: string[];
  /** Every lock protocol the process opened, when more than one; lockProtocol is the oldest. */
  lockProtocols?: number[];
  startedAt?: number;
  source: "process-record" | "host-lease" | "lock-owner" | "lock-ticket" | "state-database";
  name?: string;
  /** Owner receipt/ticket names that corroborate this process. */
  evidence?: string[];
  /** Why the census could not read or trust this evidence ("path: errno", "path: invalid"). */
  reason?: string;
}

export interface WriterCensus {
  writers: CensusWriter[];
  unknown: CensusWriter[];
  clean: boolean;
}

const backendRank: Readonly<Record<string, number>> = { file: 0, shadow: 1, sqlite: 2 };

const processAlive = (pid: number): boolean => censusRecordAlive(pid);

/**
 * SQLite state files, durable evidence independent of the census (smarty-dev#6982): any of them
 * proves the root was initialized for a sqlite or shadow writer, and a -wal or -shm proves a
 * connection may be open (SQLite removes both when the last connection closes).
 */
const DATABASE_FILES = ["state-shadow", ...["state.db", path.join("state-shadow", "state.db")]
  .flatMap(database => [database, `${database}-wal`, `${database}-shm`])];

const errno = (error: unknown): string => (error as NodeJS.ErrnoException)?.code ?? "EUNKNOWN";

/**
 * Writer metadata with every invalid or unsupported field left absent. Leases are validated on
 * read; process records are not, so an empty host, a non-positive start time, an unsupported lock
 * protocol or backend must not reach `isKnown`. A non-empty releaseSha is kept, even "unknown",
 * so the report shows it; `isKnown` rejects "unknown".
 */
const leaseWriter = (record: MeshWriterRecord): Omit<CensusWriter, "source" | "name"> => {
  const value = record as unknown as Record<string, unknown>;
  // A process that opened several backends or protocols lists them all; its top-level fields must
  // name the most dangerous backend and the oldest protocol, or they are not trusted (pi-fabric#638).
  const backends = value.stateBackends ?? [value.stateBackend];
  const protocols = value.lockProtocols ?? [value.lockProtocol];
  const backendsAgree = Array.isArray(backends) && backends.includes(value.stateBackend) &&
    backends.every(item => validWriterStateBackend(item) && backendRank[item]! <= backendRank[value.stateBackend as string]!);
  const protocolsAgree = Array.isArray(protocols) && protocols.includes(value.lockProtocol) &&
    protocols.every(item => validWriterLockProtocol(item) && item >= (value.lockProtocol as number));
  return {
    ...(validWriterPid(value.pid) ? { pid: value.pid } : {}),
    ...(validWriterHost(value.host) ? { host: value.host } : {}),
    ...(typeof value.releaseSha === "string" && value.releaseSha.length > 0 ? { releaseSha: value.releaseSha } : {}),
    ...(validWriterLockProtocol(value.lockProtocol) && protocolsAgree ? { lockProtocol: value.lockProtocol } : {}),
    ...(validWriterStateBackend(value.stateBackend) && backendsAgree ? { stateBackend: value.stateBackend } : {}),
    ...(Array.isArray(value.stateBackends) && backendsAgree ? { stateBackends: value.stateBackends as string[] } : {}),
    ...(Array.isArray(value.lockProtocols) && protocolsAgree ? { lockProtocols: value.lockProtocols as number[] } : {}),
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
 * A lock owner or queue ticket names only a pid: it is attributed to a writer by that pid (live here, or
 * another host's record or lease), and evidence no writer names is unknown, never dropped as dead.
 * Only an absent file or directory (ENOENT) is no evidence: any other read failure of the census
 * directory, a record, a host lease, the lock owner or the lock queue, and an owner record without
 * a valid pid, is counted unknown with its path and errno, so the census is never clean on it.
 */
export async function census(root: string): Promise<WriterCensus> {
  const writers: CensusWriter[] = [];
  const unknown: CensusWriter[] = [];
  const seen = new Set<string>();
  const host = os.hostname();
  const now = Date.now();
  const leaseProblems: string[] = [];
  const leases = [...readHostLeases(root, leaseProblems).values()];
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
    const mine = metadata.find(record => local(record) && censusRecordAlive(record.pid, record));
    const others = metadata.filter(record => !local(record));
    const alive = processAlive(pid);
    if (alive) add({ ...(mine ? leaseWriter(mine) : { pid }), source, ...(name ? { name } : {}) });
    // A same-pid writer on another (or an unnamed) host may own this evidence: never treat it as
    // dead (fail closed).
    for (const record of others) add({ ...leaseWriter(record), source, ...(name ? { name } : {}) });
    // pi-fabric#638 round 8: lock owners and tickets name a pid but no host or boot, so a pid not
    // live here never proves the evidence is this host's dead process: a pre-census remote writer
    // leaves exactly this. Evidence no writer record or lease names is unknown, never dropped.
    if (!alive && others.length === 0) {
      add({ pid, source, ...(name ? { name } : {}),
        reason: `${source} ${name ?? "unknown"}: pid ${pid} is not live on ${host} and no writer record or lease names it` });
    }
  };
  const initialized: string[] = [];
  for (const name of DATABASE_FILES) {
    const file = path.join(root, name);
    try { fs.lstatSync(file); initialized.push(file); }
    catch (error) { if (errno(error) !== "ENOENT") add({ source: "state-database", name: file, reason: `${file}: ${errno(error)}` }); }
  }
  const directory = path.join(root, ".writer-census");
  let names: string[] = [];
  try { names = fs.readdirSync(directory); }
  catch (error) {
    // Absent on a root without SQLite state: an old release, before census records. Absent on an
    // initialized root (its records were removed), or unreadable: its records are unknown.
    if (errno(error) !== "ENOENT") add({ source: "process-record", name: directory, reason: `${directory}: ${errno(error)}` });
    else if (initialized.length > 0) {
      add({ source: "process-record", name: directory,
        reason: `${directory}: ENOENT on a root with ${initialized.map(file => path.relative(root, file)).join(", ")}` });
    }
  }
  const ownPrefix = `${censusHostSlug(host)}-`;
  for (const name of names) {
    if (!name.endsWith(".json")) {
      // A record is renamed into place whole: a temporary of this host's dead pid is a torn write.
      // Anything else (a live writer's write in flight, a renamed record) hides a writer: unknown.
      const temporary = name.startsWith(ownPrefix) ? /^(\d+)-\d+\.json\.[0-9a-f]+\.tmp$/.exec(name.slice(ownPrefix.length)) : null;
      if (temporary && !processAlive(Number(temporary[1]))) {
        try { fs.rmSync(path.join(directory, name), { force: true }); } catch { /* best effort */ }
      } else add({ source: "process-record", name, reason: `${path.join(directory, name)}: not a census record` });
      continue;
    }
    let text: string;
    try { text = fs.readFileSync(path.join(directory, name), "utf8"); }
    catch (error) {
      // Removed since the listing (its writer exited): gone. Any other read failure is unknown.
      if (errno(error) !== "ENOENT") {
        add({ source: "process-record", name, reason: `${path.join(directory, name)}: ${errno(error)}` });
      }
      continue;
    }
    let value: unknown;
    try { value = JSON.parse(text); } catch { value = undefined; }
    const fields = typeof value === "object" && value !== null ? value as Record<string, unknown> : undefined;
    // Records are renamed into place whole: an unparsable, foreign-format or pid-less one is
    // not trusted, and not dropped either.
    if (!fields || fields.format !== 1 || !validWriterPid(fields.pid)) {
      add({ source: "process-record", name, reason: `${path.join(directory, name)}: invalid` });
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

  for (const problem of leaseProblems) add({ source: "host-lease", name: problem, reason: problem });
  for (const lease of leases) if (lease.expiresAt > now) add(fromLease(lease));

  const ownerPath = path.join(root, ".lock", "owner");
  let owner: string[] | undefined;
  try { owner = fs.readFileSync(ownerPath, "utf8").split(/\r?\n/); }
  catch (error) {
    // Absent: no lock holder. Unreadable: an unknown holder.
    if (errno(error) !== "ENOENT") add({ source: "lock-owner", name: ownerPath, reason: `${ownerPath}: ${errno(error)}` });
  }
  if (owner) {
    // The owner record is renamed into place whole: one without a valid pid names an unknown holder.
    const pid = Number(owner[1]);
    if (validWriterPid(pid)) addLockEvidence(pid, "lock-owner", owner[0] || undefined);
    else add({ source: "lock-owner", name: ownerPath, reason: `${ownerPath}: invalid` });
  }

  let queue: string | undefined;
  try {
    queue = meshLockQueueDirectory(root);
    for (const name of fs.readdirSync(queue).filter(item => /^\d{24}-\d+-[a-f0-9-]+$/.test(item))) {
      addLockEvidence(Number(name.split("-")[1]), "lock-ticket", name);
    }
  } catch (error) {
    // Absent root or queue: no tickets. Any other failure hides tickets: unknown.
    if (errno(error) !== "ENOENT") {
      const where = queue ?? root;
      add({ source: "lock-ticket", name: where, reason: `${where}: ${errno(error)}` });
    }
  }

  // An open SQLite connection must be some known writer's: without one, it is an unrecorded
  // writer (a deleted record, a legacy release, a crash that left the files; fail closed).
  const sqliteWriter = writers.some(writer => !unknown.includes(writer) &&
    (writer.stateBackends ?? [writer.stateBackend]).some(backend => backend === "sqlite" || backend === "shadow"));
  for (const file of initialized.filter(item => /-(wal|shm)$/.test(item))) {
    if (!sqliteWriter) add({ source: "state-database", name: file, reason: `${file}: open without a known sqlite or shadow writer` });
  }

  return { writers, unknown, clean: unknown.length === 0 };
}
