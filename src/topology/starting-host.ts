import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { execFileSync } from "node:child_process";
import { ownProcessIncarnation } from "../core/atomic-write.js";
import { acquireMeshCustodyLock } from "../mesh/custody-lock.js";
import { FabricParticipantStaleError, hostLeasePredecessor, renewHostLease, readHostLeaseCurrent,
  sameHostLeaseOwner, meshWriterLeaseRecord, type FabricHostLease } from "./host-leases.js";
import type { HostLeaseMesh } from "./host-lease-lock.js";

export interface StartingHostClaim { meshRoot: string; lease: FabricHostLease; }
export const HOST_STARTUP_BOUND_MS = 180_000;

/** Fresh creation identity, including at the synchronous durable-commit fence. Fail closed. */
export const localStartTime = (pid: number): string | undefined => {
  try {
    process.kill(pid, 0);
    if (process.platform === "linux") {
      const stat = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
      return stat.slice(stat.lastIndexOf(")") + 2).trim().split(/\s+/)[19];
    }
    if (process.platform === "darwin") return `darwin:${execFileSync("/bin/ps", ["-p", String(pid), "-o", "lstart="],
      { encoding: "utf8", timeout: 2_000, env: { ...process.env, LC_ALL: "C", TZ: "UTC" } }).trim()}`;
    if (process.platform === "win32" && process.env.SystemRoot) return `win32:${execFileSync(
      path.win32.join(process.env.SystemRoot, "System32", "WindowsPowerShell", "v1.0", "powershell.exe"),
      ["-NoLogo", "-NoProfile", "-NonInteractive", "-Command", `(Get-Process -Id ${pid} -ErrorAction Stop).StartTime.ToUniversalTime().Ticks.ToString([System.Globalization.CultureInfo]::InvariantCulture)`],
      { encoding: "utf8", timeout: 2_000, windowsHide: true }).trim()}`;
  } catch { /* Unknown/dead is not authority. */ }
  return undefined;
};

/** Same per-host physical custody domain as MeshStore, without importing the mesh engine. */
const startupMesh = (root: string, lockProtocol: 1 | 2 | 3): HostLeaseMesh => ({ root,
  async leaseCustody(file, operation, timeoutMs = 0, options = {}) {
    const hash = createHash("sha256").update(path.basename(file)).digest("hex");
    const legacy = path.join(root, "host-lease-commits", hash);
    const domain = fs.existsSync(legacy) ? legacy : path.join(root, "host-lease-commits", hash.slice(0, 16));
    const release = await acquireMeshCustodyLock(domain, timeoutMs, { ...options, hostQualified: true, lockProtocol });
    try { return await operation(); } finally { release(); }
  },
});

/** Native announcing Main claims before runtime import, including a clean predecessor exit. */
export const claimStartingHost = async (input: {
  meshRoot: string; cwd: string; id: string; sessionId: string; lockProtocol: 1 | 2 | 3; stateBackend: string;
}): Promise<StartingHostClaim | undefined> => {
  const previous = hostLeasePredecessor(input.meshRoot, input.id);
  if (input.id !== `session:${input.sessionId}`) return undefined;
  if (previous && (previous.mirror || previous.mirrorLease || previous.id !== input.id ||
    previous.rootId !== input.id || previous.identityId !== input.id || previous.session?.id !== input.sessionId ||
    previous.writer && previous.writer.host !== os.hostname())) return undefined;
  const startTime = await ownProcessIncarnation();
  if (!startTime || localStartTime(process.pid) !== startTime) return undefined;
  // Same-process reload retains the existing reload protocol. The legacy census has a
  // creation timestamp too; PID equality alone must never disable takeover.
  if (previous && (previous.process ? previous.process.pid === process.pid && previous.process.host === os.hostname() &&
    previous.process.startTime === startTime : previous.writer?.pid === process.pid &&
    Math.abs(previous.writer.startedAt - (Date.now() - process.uptime() * 1_000)) < 1_000)) return undefined;
  const now = Date.now(), until = now + HOST_STARTUP_BOUND_MS;
  const ownerIncarnation = randomUUID();
  const owner = { pid: process.pid, host: os.hostname(), startTime };
  const lease: FabricHostLease = { id: input.id, rootId: input.id, identityId: input.id,
    startedAt: now, incarnationToken: randomUUID(), ownerIncarnation, process: owner,
    updatedAt: now, expiresAt: until,
    session: { id: input.sessionId, startedAt: now, updatedAt: now, expiresAt: until },
    writer: meshWriterLeaseRecord(input.lockProtocol, input.stateBackend),
    startup: { ...owner, ownerIncarnation, sessionId: input.sessionId, cwd: path.resolve(input.cwd), until },
  };
  await renewHostLease(startupMesh(input.meshRoot, input.lockProtocol), lease,
    { claim: true, expected: previous, ownIncarnation: startTime });
  return { meshRoot: input.meshRoot, lease };
};

/** Exact native authority; no bridge fallback, clock extension, or PID-only liveness. */
export const startingHost = (meshRoot: string, targetId: string): FabricHostLease | undefined => {
  const lease = readHostLeaseCurrent(meshRoot, targetId), s = lease?.startup;
  if (!lease || !s || lease.mirror || lease.mirrorLease || lease.id !== targetId || lease.rootId !== targetId ||
    lease.identityId !== targetId || targetId !== `session:${s.sessionId}` || lease.ownerIncarnation !== s.ownerIncarnation ||
    !lease.incarnationToken || lease.session?.id !== s.sessionId || s.host !== os.hostname() ||
    s.until <= Date.now() || s.until !== (lease.startedAt ?? 0) + HOST_STARTUP_BOUND_MS || lease.expiresAt < s.until ||
    lease.process?.pid !== s.pid || lease.process.host !== s.host || lease.process.startTime !== s.startTime ||
    localStartTime(s.pid) !== s.startTime) return undefined;
  return lease;
};
/** Adoption/retained delivery uses custody, not the now-closed admission window. */
export const assertStartupOwner = (meshRoot: string, captured: FabricHostLease): void => {
  const current = readHostLeaseCurrent(meshRoot, captured.id), s = captured.startup;
  if (!current || !s || !sameHostLeaseOwner(current, captured) || current.ownerIncarnation !== captured.ownerIncarnation ||
    current.rootId !== captured.id || current.identityId !== captured.id || current.session?.id !== s.sessionId ||
    current.process?.pid !== s.pid || current.process.host !== s.host || current.process.startTime !== s.startTime ||
    s.host !== os.hostname() || localStartTime(s.pid) !== s.startTime)
    throw new FabricParticipantStaleError(captured.id, 0);
};

export const assertStartingHost = (meshRoot: string, captured: FabricHostLease): void => {
  const current = startingHost(meshRoot, captured.id);
  if (!current || !sameHostLeaseOwner(current, captured) || current.ownerIncarnation !== captured.ownerIncarnation ||
    current.startup?.sessionId !== captured.startup?.sessionId || current.startup?.cwd !== captured.startup?.cwd ||
    current.startup?.until !== captured.startup?.until) throw new FabricParticipantStaleError(captured.id, 0);
};
