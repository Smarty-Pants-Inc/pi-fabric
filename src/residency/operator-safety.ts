import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { mainGenerationPath, type ResidentMainGeneration } from "./handover.js";
import type { MainMarker } from "./main-marker.js";
import { residentProcessAlive } from "./process-identity.js";
import { ResidentActorAuthorizationError, type ResidentHostConfig } from "./protocol.js";
import type { FabricParticipantSource } from "../topology/types.js";
import type { MeshStore } from "../mesh/store.js";
import { hostEntryLiveness, hostLeasePath, readHostLeaseCurrent } from "../topology/host-leases.js";

export function readMainMarker(meshRoot: string, pid: number, startTime: string): MainMarker | undefined {
  try {
    const file = path.join(meshRoot, "main-markers", `${pid}-${startTime}.json`);
    const value = JSON.parse(fs.readFileSync(file, "utf8")) as MainMarker;
    return value && value.pid === pid && value.startTime === startTime &&
      typeof value.rootId === "string" && typeof value.sessionId === "string" &&
      Number.isFinite(value.createdAt) ? value : undefined;
  } catch { return undefined; }
}

const refuse = (config: ResidentHostConfig, reason: string): never => {
  throw new ResidentActorAuthorizationError(`Resident root ${config.rootId}: ${reason}; use --force-live only for an intentional override`);
};

/** Historical dead PIDs do not exclude a new Main before it publishes its first
 * generation/lease. Inspect current same-user processes, retaining only the
 * non-secret identity bindings. Unreadable/restricted evidence is NOT absence.
 * Children/resident executors inherit the root binding but are not Mains.
 */
function assertNoCurrentMain(config: ResidentHostConfig): void {
  if (process.platform !== "linux" || process.getuid?.() === undefined) refuse(config, "current Main process liveness is unknown (Linux /proc required)");
  try {
    if (/\bhidepid=(?!0\b)\w+/.test(fs.readFileSync("/proc/mounts", "utf8"))) refuse(config, "current Main process liveness is unknown (restricted /proc)");
    const selected = new Set(["PI_FABRIC_MAIN_AGENT_ID", "PI_FABRIC_SESSION_ID", "PI_SESSION_ID", "PI_FABRIC_ROLE_SESSION",
      "PI_FABRIC_PARENT_RUN", "PI_FABRIC_ACTOR_ID", "PI_FABRIC_RESIDENT_CONFIG"]);
    for (const name of fs.readdirSync("/proc")) {
      if (!/^\d+$/.test(name)) continue;
      const directory = `/proc/${name}`;
      let processName = "unreadable";
      try {
        if (fs.statSync(directory).uid !== process.getuid!()) continue;
        const stat = fs.readFileSync(path.join(directory, "stat"), "utf8");
        const close = stat.lastIndexOf(")");
        processName = stat.slice(stat.indexOf("(") + 1, close);
        const fields = stat.slice(close + 2).trim().split(/\s+/);
        if (close < 0 || fields.length < 20 || !/^\d+$/.test(fields[19]!)) refuse(config, "current Main process liveness is unknown (invalid /proc identity)");
        if (fields[0] === "Z" || fields[0] === "X") continue;
        const marker = readMainMarker(config.meshRoot, Number(name), fields[19]!);
        if (marker && (marker.transition !== undefined || marker.fenced !== true)) {
          refuse(config, `Main marker is transitioning or unfenced (PID ${name})`);
        }
        if (!marker) {
          const file = path.join(config.meshRoot, "main-markers", `${name}-${fields[19]}.json`);
          try {
            fs.lstatSync(file);
            refuse(config, `Main marker is unreadable or mismatched (PID ${name})`);
          } catch (error) {
            if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
          }
        }
        if (marker?.rootId && marker.sessionId) {
          if (marker.rootId === config.rootId || marker.sessionId === config.sessionId) {
            refuse(config, `Main process is still alive (Fabric marker for PID ${name})`);
          }
          continue;
        }
        let environment: string;
        try { environment = fs.readFileSync(path.join(directory, "environ"), "utf8"); }
        catch (error) {
          // Linux makes login/service daemons nondumpable even to their UID.
          // These are not Main runtimes. Do not infer this from EACCES alone:
          // an unreadable node/bun/Pi or any unrecognized process still refuses.
          const args = fs.readFileSync(path.join(directory, "cmdline"), "utf8").split("\0");
          const comm = stat.slice(stat.indexOf("(") + 1, close);
          const service = (comm === "sshd" && args[0]?.startsWith("sshd: ")) ||
            (comm === "systemd" && args[0] === "/usr/lib/systemd/systemd" && args[1] === "--user") ||
            (comm === "(sd-pam)" && args[0] === "(sd-pam)") ||
            (comm === "gpg-agent" && args[0] === "/usr/bin/gpg-agent");
          const flag = (key: string): string | undefined => {
            const index = args.indexOf(key);
            return index < 0 ? undefined : args[index + 1];
          };
          const id = flag("--id"), statusFile = flag("--status-file");
          // A pinned executor script with this root's exact run/status argv is
          // not a Main. This also covers its brief nondumpable exec transition.
          // Never classify an arbitrary node/Pi process by its name alone.
          const worker = args[1] === path.resolve(config.workerPath) && !!id && /^[a-f0-9]{32}$/.test(id) &&
            statusFile === path.join(config.residencyRoot, "runs", id, "status.json");
          if ((error as NodeJS.ErrnoException).code === "EACCES" && (service || worker)) continue;
          throw error;
        }
        const env: Record<string, string> = {};
        for (const pair of environment.split("\0")) {
          const equal = pair.indexOf("="), key = pair.slice(0, equal);
          if (selected.has(key)) env[key] = pair.slice(equal + 1);
        }
        if (env.PI_FABRIC_PARENT_RUN || env.PI_FABRIC_ACTOR_ID || env.PI_FABRIC_RESIDENT_CONFIG) continue;
        const sessions = [env.PI_FABRIC_SESSION_ID, env.PI_SESSION_ID, env.PI_FABRIC_ROLE_SESSION].map(value => value?.trim());
        const rootId = env.PI_FABRIC_MAIN_AGENT_ID?.trim();
        if (rootId === config.rootId || sessions.includes(config.sessionId)) {
          refuse(config, "Main process is still alive (current root/session binding)");
        }
        // Match collectHostReleases' Pi argv signature without importing the
        // reporting/profile graph. Native --session resumes may start with no
        // optional env bindings and before any generation/lease publication.
        const args = fs.readFileSync(path.join(directory, "cmdline"), "utf8").split("\0").filter(Boolean);
        const pi = args.slice(0, 2).map(arg => arg.replace(/\\/g, "/")).some(arg =>
          path.posix.basename(arg) === "pi" || /(?:pi-coding-agent|pi-runtime).*\/cli\.js$/.test(arg));
        // Launch environment can remain bound to the previous session after /new.
        // Only a stable, fenced marker may establish that a Pi is bound elsewhere.
        if (pi) refuse(config, `PID ${name}: Main without a Fabric marker (older release or still starting); retry after it publishes, or confirm and pass --force-live`);
      } catch (error) {
        if (error instanceof ResidentActorAuthorizationError) throw error;
        const code = (error as NodeJS.ErrnoException).code ?? "unknown";
        if (["ENOENT", "ESRCH"].includes(code)) continue;
        // Exit/exec can change /proc readability after the first stat. Only
        // current positive evidence of exit or another UID can remove this veto.
        try {
          if (fs.statSync(directory).uid !== process.getuid!()) continue;
          const current = fs.readFileSync(path.join(directory, "stat"), "utf8");
          const state = current.slice(current.lastIndexOf(")") + 2).split(/\s+/)[0];
          if (state === "Z" || state === "X") continue;
        } catch (recheck) {
          if (["ENOENT", "ESRCH"].includes((recheck as NodeJS.ErrnoException).code ?? "")) continue;
        }
        refuse(config, `current Main process liveness is unknown (unreadable /proc evidence for PID ${name} (${processName}): ${code})`);
      }
    }
  } catch (error) {
    if (error instanceof ResidentActorAuthorizationError) throw error;
    refuse(config, "current Main process liveness is unknown (unreadable /proc evidence)");
  }
}

/** A lapsed lease alone is NOT a dead Main. Return the checked evidence so the
 * operator can reject any intervening publication under Main's startup fence.
 */
export function assertDeadResidentMain(config: ResidentHostConfig, participants: Pick<FabricParticipantSource, "get">, mesh: Pick<MeshStore, "get">): string {
  if (process.platform !== "linux") throw new ResidentActorAuthorizationError("operator dead-root control needs Linux /proc evidence; pass --force-live after confirming");
  const root = participants.get(config.rootId, Date.now(), { fresh: true });
  if (root && !root.stale) refuse(config, "Main has a live root lease");
  const key = "topology/hosts/" + createHash("sha256").update(config.rootId).digest("hex");
  const shared = mesh.get(key, { fresh: true });
  if (shared) {
    const value = shared.value as { id?: string; rootId?: string; expiresAt?: number; identity?: { id?: string } };
    if (!value || value.id !== config.rootId || value.rootId !== config.rootId ||
        value.identity?.id !== config.rootId || !Number.isFinite(value.expiresAt)) refuse(config, "shared root lease is invalid");
    if (hostEntryLiveness(shared, new Map()).expiresAt >= Date.now()) refuse(config, "Main has a live root lease");
  }
  const leaseFile = hostLeasePath(config.meshRoot, config.rootId);
  let leasePresent = false;
  try { fs.lstatSync(leaseFile); leasePresent = true; }
  catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") refuse(config, "root lease is unreadable"); }
  const lease = leasePresent ? readHostLeaseCurrent(config.meshRoot, config.rootId) : undefined;
  if (leasePresent) {
    if (!lease || lease.rootId !== config.rootId || lease.identityId !== config.rootId) refuse(config, "root lease is unreadable or invalid");
    if (lease!.expiresAt >= Date.now()) refuse(config, "Main has a live root lease");
  }
  const readIdentity = <T>(file: string): T | undefined => {
    try {
      const value: unknown = JSON.parse(fs.readFileSync(file, "utf8"));
      if (!value || typeof value !== "object" || Array.isArray(value)) refuse(config, "Main process identity is invalid");
      return value as T;
    }
    catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
      return refuse(config, "Main process identity is unreadable or invalid");
    }
  };
  const generation = readIdentity<ResidentMainGeneration>(mainGenerationPath(config.residencyRoot));
  const inbox = readIdentity<{ rootId: string; sessionId: string; pid: number; processStartedAt?: string }>(
    path.join(config.meshRoot, "main-followups", `${encodeURIComponent(config.sessionId)}.owner.json`));
  let known = false;
  for (const owner of [generation && { ...generation, started: generation.processStartTime },
    inbox && { ...inbox, started: inbox.processStartedAt }]) {
    if (!owner) continue;
    if (owner.rootId !== config.rootId || owner.sessionId !== config.sessionId ||
        !Number.isSafeInteger(owner.pid) || owner.pid <= 0 ||
        (owner.started !== undefined && (typeof owner.started !== "string" || !/^\d+$/.test(owner.started)))) {
      refuse(config, "Main process identity is invalid (liveness unknown)");
    }
    known = true;
    if (residentProcessAlive(owner.pid, owner.started)) refuse(config, "Main process is still alive");
  }
  if (!known) refuse(config, "Main process death cannot be established (no recorded identity)");
  assertNoCurrentMain(config);
  return JSON.stringify({ generation, inbox, lease, root, shared });
}
