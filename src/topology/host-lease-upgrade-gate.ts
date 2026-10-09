import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { LEASE_FORMAT_UUID_MIN, loadedFabricRoot, resolveAgentDir } from "../core/agent-dir.js";
import { readPhysicalHostIdentity, writeJsonAtomic } from "../core/atomic-write.js";

const MAX_PROCESSES = 4096, MAX_FILE_BYTES = 1024 * 1024, MAX_TOTAL_BYTES = 32 * MAX_FILE_BYTES;
const SCAN_DEADLINE_MS = 2000;
export interface PreUuidFabricProcess { pid: number; reason: string }
export class FabricPreUuidProcessAliveError extends Error {
  override readonly name = "FabricPreUuidProcessAliveError";
  readonly code = "FABRIC_PRE_UUID_PROCESS_ALIVE";
  readonly retryable = false;
  readonly pids: number[];
  constructor(readonly hostId: string, readonly processes: PreUuidFabricProcess[]) {
    super(`Fabric host ${hostId}: pre-UUID Fabric process alive: ${processes.map(p => `${p.pid} (${p.reason})`).join(", ")}`);
    this.pids = processes.map(p => p.pid);
  }
}

/** Read-only, one bounded snapshot. Unknown live candidates are blockers, not skipped peers.
 * Loaded paths/records are evidence; the profile's current active pin is NOT loaded-code evidence. */
export const scanPreUuidFabricProcesses = (procRoot = "/proc"): PreUuidFabricProcess[] => {
  const blocked: PreUuidFabricProcess[] = [], uid = process.getuid?.();
  const deadline = Date.now() + SCAN_DEADLINE_MS;
  let bytes = 0, count = 0;
  const read = (file: string): string => {
    if (Date.now() >= deadline || bytes >= MAX_TOTAL_BYTES) throw new Error("census budget exceeded");
    const fd = fs.openSync(file, "r");
    try {
      const buffer = Buffer.alloc(Math.min(MAX_FILE_BYTES, MAX_TOTAL_BYTES - bytes) + 1);
      let length = 0;
      while (length < buffer.length) {
        const got = fs.readSync(fd, buffer, length, buffer.length - length, null);
        if (!got) break;
        length += got;
      }
      bytes += length;
      if (length === buffer.length) throw new Error("census file too large");
      return buffer.subarray(0, length).toString("utf8");
    } finally { fs.closeSync(fd); }
  };
  const rootsOf = (text: string): string[] => [...text.matchAll(/(\/[^\s\0"'=]*\/fabric\/releases\/[a-f0-9]{7,64})(?=\/|\s|\0|$)/gi)].map(match => match[1]!);
  let directory: fs.Dir;
  try {
    if (uid === undefined) throw new Error("UID unavailable");
    directory = fs.opendirSync(procRoot);
  } catch { return [{ pid: process.pid, reason: "process census unavailable" }]; }
  try {
    for (;;) {
      const entry = directory.readSync();
      if (!entry) break;
      if (!/^\d+$/.test(entry.name)) continue;
      const pid = Number(entry.name), base = path.join(procRoot, entry.name);
      if (++count > MAX_PROCESSES || Date.now() >= deadline) {
        blocked.push({ pid, reason: "process census budget exceeded" }); break;
      }
      try {
        if (fs.statSync(base).uid !== uid) continue;
        const stat = read(path.join(base, "stat"));
        const fields = stat.slice(stat.lastIndexOf(")") + 2).split(" ");
        if (fields[0] === "Z" || fields[0] === "X") continue;
        if (!/^\d+$/.test(fields[19] ?? "")) throw new Error("unknown process incarnation");
        const argv = read(path.join(base, "cmdline")).split("\0").filter(Boolean);
        const executable = path.basename(argv[0] ?? "");
        const pi = argv.slice(0, 2).some(arg => /(?:^|\/)(?:pi|pi\.js)$|(?:pi-coding-agent|pi-runtime).*\/cli\.js$/.test(arg));
        // Node/Bun may host Fabric without naming it in argv; inspect them too, fail closed if unknown.
        const candidate = pid === process.pid || pi || /^(?:node|bun)(?:\.exe)?$/.test(executable) || rootsOf(argv.join("\0")).length > 0;
        if (!candidate) {
          if (!argv.length) throw new Error("unknown live process");
          continue;
        }
        const environ = read(path.join(base, "environ"));
        const maps = read(path.join(base, "maps"));
        const roots = new Set([...rootsOf(argv.join("\0")), ...rootsOf(environ), ...rootsOf(maps)]);
        if (pid === process.pid) {
          const ownRoot = loadedFabricRoot(import.meta.url);
          if (ownRoot) roots.add(ownRoot);
        }
        // The runtime record is tied to kernel start ticks; never trust a stale PID's record.
        const profile = environ.split("\0").find(pair => pair.startsWith("PI_CODING_AGENT_DIR="))?.slice("PI_CODING_AGENT_DIR=".length);
        const recordPath = path.join(resolveAgentDir(profile ?? ""), "fabric", "release-processes", `${pid}.json`);
        try {
          const record = JSON.parse(read(recordPath)) as { pid?: unknown; start?: unknown; loadedRoot?: unknown };
          if (record.pid === pid && record.start === fields[19] && typeof record.loadedRoot === "string") roots.add(record.loadedRoot);
        } catch (error) {
          if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
        }
        if (!roots.size) throw new Error("unknown loaded Fabric release");
        for (const root of roots) {
          const marker = JSON.parse(read(path.join(root, "dist", "worker-protocol.json"))) as { leaseFormat?: unknown };
          if (typeof marker.leaseFormat !== "number" || marker.leaseFormat < LEASE_FORMAT_UUID_MIN) {
            blocked.push({ pid, reason: `pre-UUID release ${root}` }); break;
          }
        }
        // Do not accept PID reuse during the snapshot (including reuse by a non-Fabric process).
        const after = read(path.join(base, "stat"));
        if (after.slice(after.lastIndexOf(")") + 2).split(" ")[19] !== fields[19]) throw new Error("process changed during census");
      } catch (error) {
        // ENOENT on a release/record is unknown, not proof that the PROCESS exited.
        try { fs.statSync(base); } catch (statusError) {
          if ((statusError as NodeJS.ErrnoException).code === "ENOENT") continue;
          // EACCES/unknown is NOT proof of exit, even if existsSync would return false.
        }
        if (!blocked.some(item => item.pid === pid)) blocked.push({ pid, reason: `unreadable/unknown process: ${(error as Error).message}` });
      }
    }
  } catch (error) { blocked.push({ pid: process.pid, reason: `process census failed: ${(error as Error).message}` }); }
  finally { directory.closeSync(); }
  return blocked;
};

// Only a census-created permit for this exact claim can be used after a shared COMMIT.
// This lets retained reload authority validate BEFORE the durable boundary, not refuse after it.
const admissions = new WeakMap<() => void, { meshRoot: string; hostId: string; token: string }>();
export const preparedUuidHostClaim = (permit: () => void, meshRoot: string, hostId: string, token: string): (() => void) => {
  const admission = admissions.get(permit);
  if (!admission || admission.meshRoot !== meshRoot || admission.hostId !== hostId || admission.token !== token) {
    throw new TypeError("UUID host claim requires its own census admission receipt");
  }
  admissions.delete(permit); // Single use, scoped to the successful claim callback.
  return permit;
};

/** Marker is physical-host + UID qualified, within this mesh's host-leases directory.
 * Prepare under lease custody; persist ONLY after a successful UUID lease write. No scan on renewals. */
export const prepareFirstUuidHostClaim = (meshRoot: string, hostId: string, token: string): (() => void) => {
  const permit = (mark: () => void): (() => void) => { admissions.set(mark, { meshRoot, hostId, token }); return mark; };
  const physical = readPhysicalHostIdentity(), uid = process.getuid?.();
  if (!physical || uid === undefined) throw new FabricPreUuidProcessAliveError(hostId, [{ pid: process.pid, reason: "physical host/UID unavailable" }]);
  const name = createHash("sha256").update(`${physical.machineId}:${uid}`).digest("hex").slice(0, 32);
  const file = path.join(meshRoot, "host-leases", `.uuid-format-${name}.marker`);
  try {
    if (fs.statSync(file).size > 4096) throw new Error("oversized first-UUID marker");
    const marker = JSON.parse(fs.readFileSync(file, "utf8")) as { leaseFormat?: unknown; machineId?: unknown; uid?: unknown };
    if (marker.leaseFormat === LEASE_FORMAT_UUID_MIN && marker.machineId === physical.machineId && marker.uid === uid) return permit(() => undefined);
    throw new Error("invalid first-UUID marker");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw new FabricPreUuidProcessAliveError(hostId, [{ pid: process.pid, reason: "unreadable/invalid first-UUID marker" }]);
  }
  const blocked = scanPreUuidFabricProcesses();
  if (blocked.length) throw new FabricPreUuidProcessAliveError(hostId, blocked);
  return permit(() => writeJsonAtomic(file, { leaseFormat: LEASE_FORMAT_UUID_MIN, machineId: physical.machineId, uid,
    hostId, incarnationToken: token, pid: process.pid, claimedAt: Date.now() }));
};
