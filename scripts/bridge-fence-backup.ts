import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { encodeMeshStateMovedMarker } from "../src/mesh/backend-fence.js";
import type { MeshStore, MeshBatchOperation, MeshIdentity } from "../src/mesh/store.js";
import { writeHostLease } from "../src/topology/host-leases.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";

const hash = (text: string) => createHash("sha256").update(text).digest("hex");
const keyFor = (prefix: string, id: string) => `${prefix}/${hash(id)}`;
const who = (id: string): MeshIdentity => ({ id, name: id, kind: "main" });
const label = (id: string) => `FENCE-${hash(id).slice(0, 8)}`;
export function backupManifest(seed: string) {
  if (!/^[A-Za-z0-9-]{8,80}$/.test(seed)) throw new Error("fixture seed must be 8–80 ASCII letters, digits or hyphens");
  const namespace = `fnb-${hash(seed).slice(0, 20)}`;
  const hubId = `session:${namespace}-hub`;
  const drivers = Array.from({ length: 100 }, (_, n) => `session:${namespace}-driver-${n}`);
  const sources = Array.from({ length: 6 }, (_, n) => `session:${namespace}-source-${n}`);
  const fillers = Array.from({ length: 583 }, (_, n) => `session:${namespace}-file-${n}`);
  const peers = Array.from({ length: 6 }, (_, n) => `${namespace}-spoke${n}`);
  const localName = `${namespace}-hub`;
  const workKeys = drivers.map((_, n) => `fence-repro/${namespace}/session-work/${n}`);
  const allIds = [hubId, ...drivers, ...sources, ...fillers];
  if (!allIds.every(id => /^session:[A-Za-z0-9-]{8,128}$/.test(id))) throw new Error("noncanonical synthetic root");
  const aliases = [...allIds, ...allIds.map(label), ...peers, localName,
    ...peers.map(peer => `bridge/${peer}`), `bridge/${localName}`];
  if (new Set(aliases).size !== aliases.length) throw new Error("synthetic manifest has an internal alias collision");
  const outboxPrefixes = [...peers, localName].map(peer => `mesh/commit-outbox/${hash(`bridge/${peer}`).slice(0, 32)}/`);
  const keys = [...allIds.flatMap(id => [keyFor("topology/hosts", id), keyFor("topology/participants", id)]), ...workKeys];
  return { namespace, hubId, drivers, sources, fillers, peers, localName, workKeys, aliases, keys, outboxPrefixes,
    historyPrefix: `${namespace}-history-`, deliveryPrefix: `${namespace}-delivery-`, commandPrefix: `${namespace}-command-` };
}
export type BackupManifest = ReturnType<typeof backupManifest>;

const sqlite = () => process.getBuiltinModule("node:sqlite") as typeof import("node:sqlite");
const writeJson = (file: string, value: unknown) => fs.writeFileSync(file, `${JSON.stringify(value, null, 2)}\n`, { mode: 0o600 });

/** No SQL handle ever targets the staged original. Read/hash via a checked, no-follow FD only. */
export function backupSourceHash(file: string, expected: string, destination?: string) {
  if (!path.isAbsolute(file) || fs.realpathSync(file) !== file || !/^[0-9a-f]{64}$/.test(expected)) throw new Error("invalid backup pin/path");
  const parent = fs.lstatSync(path.dirname(file));
  const before = fs.lstatSync(file);
  if (!parent.isDirectory() || (parent.mode & 0o077) || !before.isFile() || (before.mode & 0o077) || before.uid !== process.getuid?.())
    throw new Error("backup must be an owned private regular file in a private directory");
  const fd = fs.openSync(file, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW | fs.constants.O_NONBLOCK);
  let target: number | undefined;
  try {
    const stat = fs.fstatSync(fd);
    if (!stat.isFile() || stat.ino !== before.ino || stat.dev !== before.dev) throw new Error("backup identity changed");
    if (destination) target = fs.openSync(destination, "wx", 0o600);
    const digest = createHash("sha256"), buffer = Buffer.alloc(1024 * 1024);
    let bytes = 0;
    for (;;) {
      const count = fs.readSync(fd, buffer, 0, buffer.length, null);
      if (!count) break;
      digest.update(buffer.subarray(0, count)); bytes += count;
      if (target !== undefined) {
        let at = 0;
        while (at < count) at += fs.writeSync(target, buffer, at, count - at);
      }
    }
    const after = fs.fstatSync(fd), named = fs.lstatSync(file);
    if (after.size !== stat.size || bytes !== stat.size || after.mtimeMs !== stat.mtimeMs || after.ctimeMs !== stat.ctimeMs ||
      named.ino !== stat.ino || named.dev !== stat.dev) throw new Error("backup changed while reading");
    const sha256 = digest.digest("hex");
    if (sha256 !== expected) throw new Error("backup SHA-256 mismatch");
    return { sha256, bytes };
  } finally { if (target !== undefined) fs.closeSync(target); fs.closeSync(fd); }
}

/** Aggregate-only output; row material remains in helper memory, never in reports/errors. */
export function captureBackup(root: string) {
  const db = new (sqlite().DatabaseSync)(path.join(root, "state.db"), { readOnly: true });
  try {
    db.exec("PRAGMA trusted_schema = OFF"); db.exec("BEGIN");
    const counts: Record<string, number> = {}, hashes: Record<string, string> = {};
    const kv = new Map<string, string>();
    for (const [table, order] of [["kv", "key"], ["meta", "name"], ["changes", "seq"], ["tombstones", "key"]]) {
      const digest = createHash("sha256"); let count = 0;
      for (const row of db.prepare(`SELECT * FROM ${table} ORDER BY ${order}`).iterate()) {
        const text = JSON.stringify(row); digest.update(`${Buffer.byteLength(text)}:${text}\n`); count++;
        if (table === "kv") kv.set(String(row.key), hash(text));
      }
      counts[table!] = count; hashes[table!] = digest.digest("hex");
    }
    const counters = Object.fromEntries(db.prepare("SELECT name, value FROM meta WHERE name IN ('high_water','commit_no','state_bytes','tombstone_ord','epoch','schema')").all()
      .map(row => [String(row.name), Number(row.value)]));
    const identity = Object.fromEntries(db.prepare("SELECT name, value FROM meta WHERE name IN ('backend','store_id')").all()
      .map(row => [String(row.name), String(row.value)]));
    const feed = db.prepare("SELECT count(*) AS rows, min(seq) AS minSeq, max(seq) AS maxSeq, min(commit_no) AS minCommit, max(commit_no) AS maxCommit FROM changes").get()!;
    const kvBytes = Number(db.prepare("SELECT coalesce(sum(bytes),0) AS n FROM kv").get()?.n);
    const tombBytes = Number(db.prepare("SELECT coalesce(sum(2*length(CAST(key AS BLOB))+32),0) AS n FROM tombstones").get()?.n);
    const maxVersion = Number(db.prepare("SELECT max(n) AS n FROM (SELECT max(version) AS n FROM kv UNION ALL SELECT max(version) AS n FROM tombstones)").get()?.n ?? 0);
    const maxOrd = Number(db.prepare("SELECT max(ord) AS n FROM tombstones").get()?.n ?? 0);
    const metadataConsistent = counters.state_bytes === kvBytes + tombBytes && counters.high_water! >= maxVersion &&
      counters.tombstone_ord! >= maxOrd && counters.commit_no! >= Number(feed.maxCommit ?? 0);
    const quickCheck = db.prepare("PRAGMA quick_check").get()?.quick_check === "ok";
    if (identity.backend !== "sqlite" || !identity.store_id || counters.schema !== 1 || !Number.isSafeInteger(counters.epoch) ||
      counters.epoch! < 1 || !metadataConsistent || !quickCheck) throw new Error("private backup identity/accounting/integrity unsupported");
    const pageSize = Number(db.prepare("PRAGMA page_size").get()?.page_size);
    const pageCount = Number(db.prepare("PRAGMA page_count").get()?.page_count);
    const freePages = Number(db.prepare("PRAGMA freelist_count").get()?.freelist_count);
    return { kv, summary: { counts, hashes, counters, feed, metadataConsistent, quickCheck: "ok", pageSize, pageCount, freePages,
      logicalDbBytes: pageSize * pageCount, privacy: "aggregate counts/counters + complete logical-table hashes only; no key-derived labels or row values" } };
  } finally { if (db.isTransaction) db.exec("ROLLBACK"); db.close(); }
}
export type BackupCapture = ReturnType<typeof captureBackup>;
export function backupDelta(initial: BackupCapture, end: BackupCapture) {
  let unchanged = 0, changed = 0, removed = 0, added = 0;
  for (const [key, value] of initial.kv) {
    if (!end.kv.has(key)) removed++;
    else if (end.kv.get(key) === value) unchanged++;
    else changed++;
  }
  for (const key of end.kv.keys()) if (!initial.kv.has(key)) added++;
  return { initialKvHash: initial.summary.hashes.kv, endKvHash: end.summary.hashes.kv, unchanged, changed, removed, added,
    counterDeltas: Object.fromEntries(Object.entries(initial.summary.counters).map(([key, value]) => [key, end.summary.counters[key]! - value])),
    countDeltas: Object.fromEntries(Object.entries(initial.summary.counts).map(([key, value]) => [key, end.summary.counts[key]! - value])),
    policy: "Natural native retention, expiry and handoff cleanup permitted; no mutation guard/filter or restoration of imported rows" };
}

/** Fail BEFORE any fixture writes. Scans all scalar aliases conservatively, including malformed topology. */
export function validateBackupNamespace(root: string, manifest: BackupManifest) {
  const db = new (sqlite().DatabaseSync)(path.join(root, "state.db"), { readOnly: true });
  let collisions = 0, malformed = 0, examined = 0;
  const aliases = new Set(manifest.aliases), keys = new Set(manifest.keys);
  const strings = (value: unknown): void => {
    if (typeof value === "string") { if (aliases.has(value) || value.includes(manifest.namespace)) collisions++; }
    else if (Array.isArray(value)) value.forEach(strings);
    else if (value && typeof value === "object") for (const [key, child] of Object.entries(value)) { strings(key); strings(child); }
  };
  try {
    db.exec("PRAGMA trusted_schema = OFF"); db.exec("BEGIN");
    for (const table of ["kv", "tombstones", "changes"]) for (const row of db.prepare(`SELECT key FROM ${table}`).iterate()) {
      const key = String(row.key); examined++;
      if (keys.has(key) || key.includes(manifest.namespace) || manifest.outboxPrefixes.some(prefix => key.startsWith(prefix))) collisions++;
    }
    for (const row of db.prepare("SELECT value, updated_by FROM kv").iterate()) {
      for (const text of [row.value, row.updated_by]) {
        try { strings(JSON.parse(String(text))); } catch { malformed++; }
      }
    }
    if (collisions || malformed) throw new Error(`backup namespace preflight failed: collisions=${collisions}, malformedJson=${malformed}`);
    return { collisions, malformedJson: malformed, examinedKeys: examined, canonicalRootsChecked: manifest.drivers.length + manifest.sources.length + manifest.fillers.length + 1,
      nativeHubRootsToAdd: 101, initialKvRowsToAdd: 302, namespaceHash: hash(JSON.stringify(manifest)),
      method: "all retained KV/tombstone/feed keys + recursive exact scalar aliases and namespace strings across all KV JSON; outbox scope prefixes" };
  } finally { if (db.isTransaction) db.exec("ROLLBACK"); db.close(); }
}

/** Caller MUST first native-create-and-close; MUST NOT have constructed any MeshStore for root. */
export function installBackup(root: string, source: string, expected: string, manifest: BackupManifest, out: string) {
  const preserved = path.join(path.dirname(root), "hub-native-bootstrap"); fs.mkdirSync(preserved, { mode: 0o700 });
  for (const name of ["state.db", "state.db-wal", "state.db-shm", "state.json"]) {
    const file = path.join(root, name);
    if (fs.existsSync(file)) fs.renameSync(file, path.join(preserved, name));
  }
  const sourceReceipt = backupSourceHash(source, expected, path.join(root, "state.db"));
  const copyHash = createHash("sha256").update(fs.readFileSync(path.join(root, "state.db"))).digest("hex");
  if (copyHash !== expected) throw new Error("private copied backup hash mismatch");
  const initial = captureBackup(root);
  const namespaceCheck = validateBackupNamespace(root, manifest);
  fs.writeFileSync(path.join(root, "state.json"), encodeMeshStateMovedMarker(initial.summary.counters.epoch!), { flag: "wx", mode: 0o600 });
  writeJson(path.join(out, "backup-install.json"), { ...sourceReceipt, copyHash, initial: initial.summary, namespaceCheck,
    bootstrapPreservedSeparately: true, movedMarkerEpoch: initial.summary.counters.epoch });
  return initial;
}

export async function seedBackupDrivers(store: MeshStore, manifest: BackupManifest) {
  const now = Date.now(), ops: MeshBatchOperation[] = [];
  const ids = [manifest.hubId, ...manifest.drivers];
  const participant = (id: string) => ({ format: 1, id, kind: "root", rootId: id, ownerHostId: id, ownerIdentityId: id,
    ownerIncarnation: `fence-repro:${id}`, name: id, label: label(id), status: "idle", runner: "pi", transport: "host",
    capabilities: ["steer", "followUp"], startedAt: now - 60000, updatedAt: now, controlProtocol: "v1",
    cwd: `/scratch/lanes/${hash(id).slice(0, 12)}`, model: "sol-6.1", thinking: "xhigh" });
  for (const id of ids) {
    const identity = who(id);
    ops.push({ kind: "put", key: keyFor("topology/hosts", id), identity,
      value: { format: 1, id, rootId: id, identity, startedAt: now - 60000, updatedAt: now, expiresAt: now + 600000 } });
    ops.push({ kind: "put", key: keyFor("topology/participants", id), identity, value: participant(id) });
  }
  manifest.drivers.forEach((id, n) => ops.push({ kind: "put", key: manifest.workKeys[n]!, identity: who(id),
    value: { syntheticFixture: true, sequence: 0, updatedAt: now } }));
  for (let at = 0; at < ops.length; at += 100) await store.writeBatch({ identity: who(manifest.hubId), ops: ops.slice(at, at + 100) });
  for (const id of [...ids, ...manifest.fillers]) writeHostLease(store.root,
    { id, rootId: id, identityId: id, startedAt: now - 60000, updatedAt: now, expiresAt: now + 600000 });
  // Exactly 100 driver files + 288 file-only filler files; hub has a lease but no participant file.
  for (const id of [...manifest.drivers, ...manifest.fillers.slice(0, 288)]) {
    const entry = store.get(keyFor("topology/participants", id), { fresh: true });
    writeParticipantFile(store.root, { key: keyFor("topology/participants", id), version: entry?.version ?? 1,
      updatedAt: now, updatedBy: who(id), value: participant(id) });
  }
  return { hubNativeRootsAdded: ids.length, workRowsAdded: manifest.workKeys.length, initialAdditiveKvRows: ops.length,
    initialHostLeaseFiles: ids.length + manifest.fillers.length, initialParticipantFiles: 388,
    renewingParticipantSubset: 100, renewingLeaseSubset: 100, syntheticNamespace: manifest.namespace,
    limitation: "File-only filler identities model directory scan density, not 684 exported native roots. No imported identity renewed." };
}

/** Affinity is inherited by every normal spawn; never changes host/cgroup configuration. */
export function harnessAffinity() {
  const cpuSet = /^Cpus_allowed_list:\s*(.+)$/m.exec(fs.readFileSync("/proc/self/status", "utf8"))?.[1]?.trim();
  if (!cpuSet) throw new Error("cannot attest harness CPU affinity");
  const cpus = cpuSet.split(",").flatMap(part => {
    const [first, last = first] = part.split("-").map(Number);
    if (!Number.isInteger(first) || !Number.isInteger(last) || last! < first! || last! - first! > 4096) throw new Error("invalid CPU affinity range");
    return Array.from({ length: last! - first! + 1 }, (_, n) => first! + n);
  });
  return { cpuSet, cpus };
}

/** Only our recorded PIDs; no host process discovery. Main-thread schedstat is a proxy, NOT kernel PSI. */
export function ownedSchedule(groups: Record<string, number[]>, cpuSet: string) {
  const at = Date.now(), domain = cgroupDomain();
  const totals: Record<string, { processes: number; sampled: number; missing: number; affinityMismatch: number; cgroupMismatch: number; runtimeNs: number; runDelayNs: number; timeslices: number }> = {};
  for (const [group, pids] of Object.entries(groups)) {
    const value = totals[group] = { processes: pids.length, sampled: 0, missing: 0, affinityMismatch: 0, cgroupMismatch: 0, runtimeNs: 0, runDelayNs: 0, timeslices: 0 };
    for (const pid of pids) {
      if (!Number.isSafeInteger(pid) || pid <= 0) { value.missing++; continue; }
      try {
        const fields = fs.readFileSync(`/proc/${pid}/schedstat`, "utf8").trim().split(/\s+/).map(Number);
        const affinity = /^Cpus_allowed_list:\s*(.+)$/m.exec(fs.readFileSync(`/proc/${pid}/status`, "utf8"))?.[1]?.trim();
        if (fields.length !== 3 || !fields.every(n => Number.isFinite(n) && n >= 0)) throw new Error("unavailable schedstat");
        value.runtimeNs += fields[0]!; value.runDelayNs += fields[1]!; value.timeslices += fields[2]!; value.sampled++;
        if (affinity !== cpuSet) value.affinityMismatch++;
        if (cgroupDomain(pid).hash !== domain.hash) value.cgroupMismatch++;
      } catch { value.missing++; }
    }
  }
  return { at, sampledUntil: Date.now(), cpuSet, cgroupHash: domain.hash, ownScopeAttested: domain.ownScopeAttested, groups: totals,
    scope: "owned process main threads only; recorded controller/writer/reader/CPU PIDs. Scheduler run-delay share is NOT kernel PSI, excludes helper threads and unrelated lanes." };
}
export function ownedScheduleWindow(start: ReturnType<typeof ownedSchedule>, end: ReturnType<typeof ownedSchedule>, cpuCount: number) {
  const durationMs = end.at - start.at;
  const groups = Object.fromEntries(Object.entries(start.groups).map(([group, first]) => {
    const last = end.groups[group]!;
    const runtimeNs = last.runtimeNs - first.runtimeNs, runDelayNs = last.runDelayNs - first.runDelayNs;
    return [group, { runtimeNs, runDelayNs, timeslices: last.timeslices - first.timeslices,
      runDelaySharePct: runtimeNs + runDelayNs > 0 ? runDelayNs / (runtimeNs + runDelayNs) * 100 : null,
      setCpuRuntimePct: durationMs > 0 ? runtimeNs / (durationMs * 1e6 * cpuCount) * 100 : null,
      valid: first.sampled === first.processes && last.sampled === last.processes && !first.affinityMismatch && !last.affinityMismatch &&
        !first.cgroupMismatch && !last.cgroupMismatch && start.ownScopeAttested && end.ownScopeAttested && start.cgroupHash === end.cgroupHash && runtimeNs >= 0 && runDelayNs >= 0 }];
  }));
  const runtimeNs = Object.values(groups).reduce((sum, group) => sum + group.runtimeNs, 0);
  const runDelayNs = Object.values(groups).reduce((sum, group) => sum + group.runDelayNs, 0);
  return { start, end, durationMs, cpuCount, groups, runtimeNs, runDelayNs,
    runDelaySharePct: runtimeNs + runDelayNs > 0 ? runDelayNs / (runtimeNs + runDelayNs) * 100 : null,
    valid: Object.values(groups).every(group => group.valid),
    interpretation: "Owned-main-thread summed runnable delay/(runtime+delay), NOT kernel PSI. Dedicated kernel PSI is independently measured in phasePressure.*.cpuSomePct; this scheduler proxy is supplemental, never substituted for the 45–50% kernel-PSI target." };
}

function cgroupDomain(pid: number | "self" = "self") {
  const relative = fs.readFileSync(`/proc/${pid}/cgroup`, "utf8").split("\n").find(line => line.startsWith("0::"))?.slice(3);
  if (!relative || relative.split("/").includes("..")) throw new Error("cgroup v2 domain unavailable");
  return { relative, hash: hash(relative), ownScopeAttested: /\/fence-b-[A-Za-z0-9-]+\.scope$/.test(relative) };
}
const pressureTotals = (file: string) => {
  const totals: { some: number | null; full: number | null } = { some: null, full: null };
  try { for (const line of fs.readFileSync(file, "utf8").trim().split("\n")) {
    const match = /^(some|full) .*total=(\d+)/.exec(line);
    if (match) totals[match[1] as "some" | "full"] = Number(match[2]);
  } } catch { /* Unavailable is null, never fabricated zero. */ }
  return totals;
};
export function pressureSample() {
  const cpu = fs.readFileSync("/proc/stat", "utf8").split("\n")[0]!.trim().split(/\s+/).slice(1).map(Number);
  const pressure = Object.fromEntries(["cpu", "io", "memory"].map(resource => {
    const totals: { some: number | null; full: number | null } = { some: null, full: null };
    try { for (const line of fs.readFileSync(`/proc/pressure/${resource}`, "utf8").trim().split("\n")) {
      const match = /^(some|full) .*total=(\d+)/.exec(line);
      if (match) totals[match[1] as "some" | "full"] = Number(match[2]);
    } } catch { /* Unavailable PSI is null, never a fabricated zero. */ }
    return [resource, totals];
  }));
  const cgroupCpu: Record<string, number> = {};
  const domain = cgroupDomain();
  const cgroupPressure = Object.fromEntries(["cpu", "io", "memory"].map(resource =>
    [resource, pressureTotals(path.join("/sys/fs/cgroup", domain.relative, `${resource}.pressure`))]));
  try {
    {
      const file = path.join("/sys/fs/cgroup", domain.relative, "cpu.stat");
      for (const line of fs.readFileSync(file, "utf8").trim().split("\n")) {
        const [key, value] = line.split(/\s+/);
        if (key && /^(usage_usec|user_usec|system_usec|nr_periods|nr_throttled|throttled_usec|nr_bursts|burst_usec)$/.test(key)) cgroupCpu[key] = Number(value);
      }
    }
  } catch { /* Parent capacity evidence covers unavailable per-cgroup files. */ }
  return { at: Date.now(), cpu, pressure, loadAverage: fs.readFileSync("/proc/loadavg", "utf8").split(/\s+/).slice(0, 3).map(Number),
    cgroupCpu, cgroupPressure, cgroupHash: domain.hash, ownScopeAttested: domain.ownScopeAttested };
}
export function pressureWindow(start: ReturnType<typeof pressureSample>, end: ReturnType<typeof pressureSample>, cpuCount: number) {
  const elapsedMs = end.at - start.at;
  // guest/guest_nice are already included in user/nice: exclude the last two fields.
  const delta = end.cpu.slice(0, 8).map((value, n) => value - start.cpu[n]!);
  const total = delta.reduce((a, b) => a + b, 0), idle = (delta[3] ?? 0) + (delta[4] ?? 0);
  const psi = (resource: string) => {
    const first = start.pressure[resource]?.some, last = end.pressure[resource]?.some;
    return first == null || last == null || elapsedMs <= 0 ? null : (last - first) / (elapsedMs * 1000) * 100;
  };
  const ownPsi = (resource: string) => {
    const first = start.cgroupPressure[resource]?.some, last = end.cgroupPressure[resource]?.some;
    return first == null || last == null || elapsedMs <= 0 ? null : (last - first) / (elapsedMs * 1000) * 100;
  };
  const usage = end.cgroupCpu.usage_usec! - start.cgroupCpu.usage_usec!;
  return { scope: "dedicated harness scope kernel PSI + separately labeled ambient host context", start, end, elapsedMs, durationMs: elapsedMs,
    ownScopeAttested: start.ownScopeAttested && end.ownScopeAttested && start.cgroupHash === end.cgroupHash,
    cpuBusyPct: elapsedMs > 0 && Number.isFinite(usage) ? usage / (elapsedMs * 1000 * cpuCount) * 100 : null,
    ambientCpuBusyPct: total > 0 ? (total - idle) / total * 100 : null,
    cpuPsiSomePct: psi("cpu"), ioPsiSomePct: psi("io"), memoryPsiSomePct: psi("memory"),
    cpuSomePct: ownPsi("cpu"), ioSomePct: ownPsi("io"), memorySomePct: ownPsi("memory") };
}

/** Dedicated CPU role: IPC-driven, no FS/network work, no allocation-stress, <=8ms work chunks. */
export async function cpuWorker(index: number, next: () => Promise<{ kind: string; until?: number }>, send: (value: unknown) => void) {
  const windows: Array<Record<string, unknown>> = [];
  const disconnected = () => process.exit(1);
  process.once("disconnect", disconnected);
  // IPC is the normal event source; hard lifetime is failure containment, never workload pacing.
  const deadline = setTimeout(() => process.exit(124), 300000);
  let stopRequested = false;
  const onStop = (value: unknown) => { if (value && typeof value === "object" && (value as { kind?: string }).kind === "stop") stopRequested = true; };
  process.on("message", onStop);
  try {
    send({ kind: "ready", role: "cpu", index, pid: process.pid });
    let state = 0x12345678;
    for (let phase = await next(); phase.kind !== "stop"; phase = await next()) {
      if (!["load", "idlebusy"].includes(phase.kind) || !phase.until || phase.until - Date.now() > 65000) throw new Error("invalid CPU phase");
      const start = Date.now(), usage = process.cpuUsage(); let chunks = 0;
      while (Date.now() < phase.until && process.connected && !stopRequested) {
        const end = performance.now() + 8;
        do { for (let n = 0; n < 4096; n++) state = Math.imul(state ^ (state >>> 13), 1664525) + 1013904223 | 0; } while (performance.now() < end && Date.now() < phase.until);
        chunks++; await new Promise<void>(resolve => setImmediate(resolve));
      }
      windows.push({ phase: phase.kind, requestedUntil: phase.until, from: start, stoppedAt: Date.now(), chunks, cpuUsage: process.cpuUsage(usage), checksum: state });
      send({ kind: "phaseDone", role: "cpu", index, phase: phase.kind, at: Date.now() });
    }
    return { ok: true, role: "cpu", index, pid: process.pid, windows };
  } finally { clearTimeout(deadline); process.removeListener("disconnect", disconnected); process.removeListener("message", onStop); }
}

/** Namespaced equivalents of the old retained-file fixtures; no archive/index writes here. */
export function seedBackupFiles(root: string, manifest: BackupManifest, historicalEvents: number) {
  const now = Date.now(), identity = who(manifest.hubId), receiptDir = path.join(root, "event-receipts");
  fs.mkdirSync(receiptDir, { recursive: true, mode: 0o700 });
  for (let n = 0; n < 33669; n++) {
    const dedupeKey = `${manifest.namespace}-retained-receipt-${n}`;
    writeJson(path.join(receiptDir, `${hash(dedupeKey)}.json`), { id: `${manifest.historyPrefix}${n % historicalEvents}`,
      dedupeKey, sequence: n % historicalEvents + 1, topic: "fence.history", kind: "record", from: identity,
      createdAt: now, data: { syntheticRetainedReceipt: true, ordinal: n } });
  }
  for (const directory of ["ingress-cursors", "control-seen", "actors", "lock-stats", "host-lease-commits"])
    fs.mkdirSync(path.join(root, directory), { recursive: true, mode: 0o700 });
  for (let n = 0; n < 114; n++) writeJson(path.join(root, "ingress-cursors", `${hash(`${manifest.namespace}:ingress:${n}`)}.json`), {
    format: 1, topic: `${manifest.namespace}.ingress.${n}`, after: n + 1, updatedAt: now, syntheticAggregateFixture: true,
  });
  for (let n = 0; n < 9519; n++) {
    const dir = path.join(root, "control-seen", hash(`${manifest.namespace}-historical-host-${n}`).slice(0, 32)); fs.mkdirSync(dir, { mode: 0o700 });
    const key = keyFor("topology/control-seen", `${manifest.commandPrefix}retained-${n}`);
    const value = { format: 1, hostId: manifest.hubId, commandId: `${manifest.commandPrefix}retained-${n}`, targetId: manifest.hubId,
      expiresAt: now + 86400000, sequence: n + 1, acceptance: { accepted: true, messageId: `${manifest.namespace}-retained-${n}` } };
    writeJson(path.join(dir, "state.json"), { format: 1, revisionFormat: 2, highWater: 1,
      entries: { [key]: { key, value, version: 1, updatedAt: now, updatedBy: identity } } });
  }
  for (let n = 0; n < 950; n++) {
    const dir = path.join(root, "actors", hash(`${manifest.namespace}-historical-actor-root-${n}`).slice(0, 32)); fs.mkdirSync(dir, { mode: 0o700 });
    writeJson(path.join(dir, "actors.json"), { format: 1, actors: [{ id: `${manifest.namespace}-historical-actor-${n}`, scope: "session", rootId: manifest.hubId,
      name: `${manifest.namespace}-fixture-actor-${n}`, status: "idle", runner: "pi", events: ["agent_end"], topics: ["fleet.work.fence15310"],
      delivery: "followUp", responseMode: "event", triggerTurn: true, coalesce: true, createdAt: now, updatedAt: now }] });
  }
  for (let n = 0; n < 5782; n++) writeJson(path.join(root, "lock-stats", `synthetic-${100000 + n}.json`), {
    version: 1, host: "synthetic", pid: 100000 + n, root, startedAt: now - 86400000, updatedAt: now - 600000,
    writer: { argv1: "bridge-fence-retained-fixture" }, minutes: [],
  });
  for (let n = 0; n < 2245; n++) fs.mkdirSync(path.join(root, "host-lease-commits", hash(`${manifest.namespace}-historical-domain-${n}`).slice(0, 16)), { mode: 0o700 });
  return { eventReceipts: 33669, hostLeaseCommitDomains: 2245, ingressCursorFiles: 114, controlSeenFiles: 9519, actorFiles: 950, lockStatsFiles: 5782,
    assumptions: "Namespaced inert retained file fixtures, not imported runtime identities. Historical receipts do not establish coupled archive crash recovery. No manual archive/index or active lock-owner files." };
}
