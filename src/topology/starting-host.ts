import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { execFileSync } from "node:child_process";
import { ownProcessIncarnation, validProcessIncarnation } from "../core/atomic-write.js";
import { acquireMeshCustodyLock } from "../mesh/custody-lock.js";
import { FabricParticipantStaleError, FabricHostLeaseSupersededError, FabricHostLeaseLegacyBusyError, hostLeasePredecessor, renewHostLease, readHostLeaseCurrent,
  sameHostLeaseOwner, sameHostLeaseVersion, assertHostLeaseOwner, hostLeasePath, writeHostLease, meshWriterLeaseRecord, type FabricHostLease } from "./host-leases.js";
import { watchBridgeStore } from "../mesh/change-notifier.js";
import type { HostLeaseMesh } from "./host-lease-lock.js";

export interface StartingHostClaim { meshRoot: string; lease: FabricHostLease; }
export const HOST_STARTUP_BOUND_MS = 180_000;

/** Fresh creation identity, including at the synchronous durable-commit fence. Fail closed. */
export const localStartTime = (pid: number): string | undefined => {
  try {
    process.kill(pid, 0);
    if (process.platform === "linux") {
      const stat = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
      const birth = stat.slice(stat.lastIndexOf(")") + 2).trim().split(/\s+/)[19];
      return birth && /^\d+$/.test(birth) ? birth : undefined;
    }
    if (process.platform === "darwin") {
      const birth = execFileSync("/bin/ps", ["-p", String(pid), "-o", "lstart="],
        { encoding: "utf8", timeout: 2_000, env: { ...process.env, LC_ALL: "C", TZ: "UTC" } }).trim();
      return birth ? `darwin:${birth}` : undefined;
    }
    if (process.platform === "win32" && process.env.SystemRoot) {
      const birth = execFileSync(
        path.win32.join(process.env.SystemRoot, "System32", "WindowsPowerShell", "v1.0", "powershell.exe"),
        ["-NoLogo", "-NoProfile", "-NonInteractive", "-Command", `(Get-Process -Id ${pid} -ErrorAction Stop).StartTime.ToUniversalTime().Ticks.ToString([System.Globalization.CultureInfo]::InvariantCulture)`],
        { encoding: "utf8", timeout: 2_000, windowsHide: true }).trim();
      return /^\d+$/.test(birth) ? `win32:${birth}` : undefined;
    }
  } catch { /* Unknown/dead is not authority. */ }
  return undefined;
};

/** Same per-host physical custody domain as MeshStore, without importing the mesh engine. */
const startupMesh = (root: string, lockProtocol: 1 | 2 | 3): HostLeaseMesh => ({ root,
  async leaseCustody(file, operation, timeoutMs = 0, options = {}) {
    const hash = createHash("sha256").update(path.basename(file)).digest("hex");
    const legacy = path.join(root, "host-lease-commits", hash);
    const domain = fs.existsSync(legacy) ? legacy : path.join(root, "host-lease-commits", hash.slice(0, 16));
    // Bootstrap holds no registry locks. Join a clean handoff's in-flight custody
    // release with the existing bounded lock acquisition, rather than failing startup.
    const release = await acquireMeshCustodyLock(domain, Math.max(timeoutMs, 5_000), { ...options, hostQualified: true, lockProtocol });
    try { return await operation(); } finally { release(); }
  },
});

/** Unknown is not dead: only ESRCH or an observed different native birth proves exit. */
const predecessorProcess = (lease: FabricHostLease): "live" | "exited" | "unknown" => {
  const p = lease.process;
  if (!p || p.host !== os.hostname() || !validProcessIncarnation(p.startTime)) return "unknown";
  try { process.kill(p.pid, 0); }
  catch (error) { return (error as NodeJS.ErrnoException).code === "ESRCH" ? "exited" : "unknown"; }
  const birth = localStartTime(p.pid);
  return birth === undefined ? "unknown" : birth === p.startTime ? "live" : "exited";
};

export class FabricHostLeasePredecessorBusyError extends Error {
  override readonly name = "FabricHostLeasePredecessorBusyError";
  readonly code = "FABRIC_HOST_LEASE_PREDECESSOR_BUSY";
  readonly retryable = true;
  constructor(readonly predecessor: FabricHostLease) {
    super(`Fabric host ${predecessor.id}: predecessor retains authority until ${predecessor.expiresAt}`);
  }
}

/** Watch before rereading. Notifications only trigger a new custody check; silence or
 * watch failure waits for ONE expiry timer, never a polling fallback. */
const waitForPredecessorChange = (meshRoot: string, predecessor: FabricHostLease): Promise<void> =>
  new Promise((resolve, reject) => {
    let settled = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let unwatch: (() => void) | undefined;
    const finish = (error?: unknown): void => {
      if (settled) return;
      settled = true;
      if (timer !== undefined) clearTimeout(timer);
      try { unwatch?.(); } catch { /* best effort */ }
      if (error !== undefined) reject(error); else resolve();
    };
    const check = (): void => {
      if (settled) return;
      try {
        if (!sameHostLeaseVersion(hostLeasePredecessor(meshRoot, predecessor.id), predecessor) ||
          predecessorProcess(predecessor) === "exited") finish();
      } catch (error) { finish(error); }
    };
    try {
      const stop = watchBridgeStore(meshRoot, () => finish(), () => { /* Keep the expiry timer. */ });
      if (settled) { stop(); return; }
      unwatch = stop;
    } catch { /* No polling fallback. */ }
    const remaining = predecessor.expiresAt - Date.now();
    // After expiry a proven-live owner stays fenced: lease notifications only, no zero-timer spin.
    if (remaining > 0) timer = setTimeout(() => finish(), Math.min(2_147_483_647, remaining));
    check();
  });

/** Publish only after successful clean withdrawal/drain. The public token alone is NOT
 * release authority: prove this process's native birth again while holding custody. */
export const markHostLeaseCleanHandoff = async (mesh: HostLeaseMesh, owner: FabricHostLease,
  options: { ownIncarnation: string | undefined }): Promise<boolean> =>
  await mesh.leaseCustody(hostLeasePath(mesh.root, owner.id), () => {
    assertHostLeaseOwner(mesh.root, owner);
    const current = hostLeasePredecessor(mesh.root, owner.id)!;
    const p = current.process;
    if (!options.ownIncarnation || !p || p.pid !== process.pid || p.host !== os.hostname() ||
      p.startTime !== options.ownIncarnation || localStartTime(process.pid) !== p.startTime) return false;
    writeHostLease(mesh.root, { ...current, cleanHandoff: current.incarnationToken! });
    assertHostLeaseOwner(mesh.root, owner);
    return true;
  }, 5_000, options);

/** Native announcing Main claims before runtime import, including a clean predecessor exit. */
export const claimStartingHost = async (input: {
  meshRoot: string; cwd: string; id: string; sessionId: string; lockProtocol: 1 | 2 | 3; stateBackend: string;
}): Promise<StartingHostClaim | undefined> => {
  const previous = hostLeasePredecessor(input.meshRoot, input.id);
  if (input.id !== `session:${input.sessionId}`) return undefined;
  if (previous && (previous.mirror || previous.mirrorLease || previous.id !== input.id ||
    previous.rootId !== input.id || previous.identityId !== input.id || previous.session?.id !== input.sessionId ||
    previous.writer && previous.writer.host !== os.hostname()))
    throw new FabricHostLeaseSupersededError(input.id, input.id, previous.startedAt);
  const startTime = await ownProcessIncarnation();
  if (!startTime || localStartTime(process.pid) !== startTime) {
    if (previous) throw new FabricParticipantStaleError(input.id, 0);
    return undefined;
  }
  // Same-process reload retains the existing reload protocol. The legacy census has a
  // creation timestamp too; PID equality alone must never disable takeover.
  if (previous && (previous.process ? previous.process.pid === process.pid && previous.process.host === os.hostname() &&
    previous.process.startTime === startTime : previous.writer?.pid === process.pid &&
    Math.abs(previous.writer.startedAt - (Date.now() - process.uptime() * 1_000)) < 1_000)) return undefined;
  const mesh = startupMesh(input.meshRoot, input.lockProtocol);
  const ownerIncarnation = randomUUID(), incarnationToken = randomUUID();
  const owner = { pid: process.pid, host: os.hostname(), startTime };
  let expected = previous;
  for (;;) {
    // Admission time, not the beginning of a possibly long predecessor wait.
    const now = Date.now(), until = now + HOST_STARTUP_BOUND_MS;
    const lease: FabricHostLease = { id: input.id, rootId: input.id, identityId: input.id,
      startedAt: now, incarnationToken, ownerIncarnation, process: owner,
      updatedAt: now, expiresAt: until,
      session: { id: input.sessionId, startedAt: now, updatedAt: now, expiresAt: until },
      writer: meshWriterLeaseRecord(input.lockProtocol, input.stateBackend),
      startup: { ...owner, ownerIncarnation, sessionId: input.sessionId, cwd: path.resolve(input.cwd), until },
    };
    try {
      await renewHostLease(mesh, lease, { claim: true, expected, ownIncarnation: startTime,
        beforeReplace: current => {
          // Repeat native scope validation on CURRENT bytes under custody, even at expiry.
          if (current.mirror || current.mirrorLease || current.rootId !== input.id || current.identityId !== input.id ||
            current.session?.id !== input.sessionId || current.writer && current.writer.host !== os.hostname())
            throw new FabricHostLeaseSupersededError(input.id, input.id, current.startedAt);
          const state = predecessorProcess(current);
          if (current.incarnationToken && current.cleanHandoff === current.incarnationToken || state === "exited") return;
          // TRANSITIONAL: old releases lack pid+birth. Keep their TTL admission until the
          // D5 wave leaves no identity-less lease in the directory for 24 h; then remove it.
          // A proven-live process NEVER loses custody merely because its lease expired.
          const legacyIdentity = !current.process || !validProcessIncarnation(current.process.startTime);
          // A recorded native identity with an unreadable live probe is NOT a legacy lease.
          if (state === "unknown" && legacyIdentity && current.expiresAt <= Date.now()) return;
          throw new FabricHostLeasePredecessorBusyError(current);
        } });
      return { meshRoot: input.meshRoot, lease };
    } catch (error) {
      if (error instanceof FabricHostLeasePredecessorBusyError) {
        await waitForPredecessorChange(input.meshRoot, error.predecessor);
      } else if (error instanceof FabricHostLeaseLegacyBusyError) {
        // Keep legacy expiry admission, but never spend its wait inside a lease's
        // already-created startup bound. The next attempt gets a fresh clock.
        const current = hostLeasePredecessor(input.meshRoot, input.id);
        if (current) await waitForPredecessorChange(input.meshRoot, current);
      } else if (!(error instanceof FabricHostLeaseSupersededError)) throw error;
      // A clean full close removes its lease. Start a fresh ABSENCE CAS only after a
      // strict absent read, never recapture an intervening owner's token as permission.
      if (!hostLeasePredecessor(input.meshRoot, input.id)) expected = undefined;
      else if (!(error instanceof FabricHostLeasePredecessorBusyError) && !(error instanceof FabricHostLeaseLegacyBusyError)) throw error;
    }
  }
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
