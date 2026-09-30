import fs from "node:fs";
import { createHash } from "node:crypto";
import path from "node:path";
import { writeJsonAtomic } from "../core/atomic-write.js";
import { ActorManager } from "../actors/manager.js";
import type { FabricActorInfo } from "../actors/types.js";
import { processIdentityState, processStartIdentityState, readProcessStartIdentity, validProcessIdentity, validProcessStartIdentity, type ProcessIdentity } from "../core/process-identity.js";
import type { FabricHostRecord, FabricParticipantRecord } from "../topology/types.js";
import { readHostLeases } from "../topology/host-leases.js";
import type { ResidencyClient } from "./client.js";
import { residentHostId, residentRoot, type ResidentHostConfig, type ResidentHostOwner } from "./protocol.js";

const readRecord = <T>(file: string): T | undefined => {
  try {
    const value: unknown = JSON.parse(fs.readFileSync(file, "utf8"));
    if (typeof value !== "object" || value === null || Array.isArray(value)) throw new Error("Not an ownership record");
    return value as T;
  }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
    throw new Error(`Cannot verify recorded ownership at ${file}`);
  }
};
const canonical = (value: string | undefined): string | undefined => {
  if (!value) return undefined;
  try { return fs.realpathSync.native(value); } catch { return undefined; }
};
const nativeRoot = (root: FabricParticipantRecord | undefined, id: string): root is FabricParticipantRecord =>
  !!root && root.format === 1 && root.kind === "root" && root.id === id && root.rootId === id && !root.remoteHost &&
  typeof root.name === "string" && root.name.length > 0 &&
  root.ownerHostId === id && root.ownerIdentityId === id;
// The general directory tolerates broken records for UI listings. Authority decisions cannot:
// an unreadable/malformed lease is unknown, never evidence that an owner died.
const assertReadableLeaseFiles = (meshRoot: string): void => {
  const dir = path.join(meshRoot, "host-leases");
  let names: string[];
  try { names = fs.readdirSync(dir); }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return; throw error; }
  for (const name of names) {
    if (!name.endsWith(".json")) continue;
    const lease = readRecord<{ format: number; id: string; rootId: string; identityId: string; expiresAt: number; updatedAt: number }>(path.join(dir, name));
    if (!lease || lease.format !== 1 || typeof lease.id !== "string" || typeof lease.rootId !== "string" ||
        typeof lease.identityId !== "string" || !Number.isFinite(lease.expiresAt) || !Number.isFinite(lease.updatedAt) ||
        `${createHash("sha256").update(lease.id).digest("hex").slice(0, 32)}.json` !== name) {
      throw new Error("Cannot prove predecessor root death: unreadable or malformed host lease");
    }
  }
};
const sameIdentity = (a: ProcessIdentity, b: ProcessIdentity): boolean =>
  a.pid === b.pid && a.startTime === b.startTime && a.kernelId === b.kernelId && a.commandLine === b.commandLine;

// A durable actor can have session storage despite durable execution. A rotated Main does not
// load its predecessor's session registry. Look up exact ids only inside the same project's
// physical actor root; a temporary manager denies ALL runnable ownership and uses the normal
// registry/removal implementation for the selected stopped deletion, including cleanup retries.
const removalRegistry = async (client: ResidencyClient, manager: ActorManager, id: string): Promise<{
  actor: FabricActorInfo; manager: ActorManager; temporary?: ActorManager;
}> => {
  const known = manager.cleanupObligation(id) ?? manager.list().find((actor) => actor.id === id);
  if (known) return { actor: known, manager };
  if (!/^[a-f0-9]{32}$/.test(id)) throw new Error(`Unknown Fabric actor: ${id}`);
  const root = canonical(client.options.config.actorRoot);
  if (!root) throw new Error("Cannot verify the project actor registry root");
  const candidates: string[] = [];
  for (const name of fs.readdirSync(root)) {
    const dir = path.join(root, name);
    if (!fs.lstatSync(dir).isDirectory() || canonical(dir) !== dir) continue;
    const registryFile = path.join(dir, "actors.json");
    if (fs.existsSync(registryFile) && !fs.lstatSync(registryFile).isFile()) {
      throw new Error("Cannot verify a symlinked predecessor actor registry");
    }
    const registry = readRecord<{ actors?: Array<{ id?: string }> }>(registryFile);
    if (registry && !Array.isArray(registry.actors)) throw new Error("Cannot verify a predecessor actor registry");
    const marker = path.join(dir, `removal-${id}.json`);
    if (registry?.actors?.some((actor) => actor?.id === id) || fs.existsSync(marker)) candidates.push(dir);
  }
  if (candidates.length !== 1) throw new Error(`${candidates.length ? "Ambiguous" : "Unknown"} Fabric actor: ${id}`);
  const temporary = new ActorManager(client.options.config.sessionId, manager.identity, client.options.mesh,
    client.options.config.mesh, manager.agents, () => {}, {
      persistent: true, actorRoot: candidates[0]!, actorScope: "session", rootId: client.options.config.rootId,
      ...(client.options.config.project ? { project: client.options.config.project } : {}),
      canManageActor: () => false, claimResidency: "session", reapDeadSessionPresence: false, reapOrphanPresence: false,
    });
  try {
    const actor = temporary.cleanupObligation(id) ?? temporary.list().find((actor) => actor.id === id);
    if (!actor) throw new Error(`Cannot verify predecessor actor ${id}`);
    return { actor, manager: temporary, temporary };
  } catch (error) { await temporary.close(); throw error; }
};

/** Main-only, explicit removal; never grants permission to run an orphan's accepted work. */
export const removeDeadPredecessor = async (client: ResidencyClient, manager: ActorManager, id: string) => {
  const { config, mesh, participants } = client.options;
  const caller = participants.get(config.rootId, undefined, { fresh: true });
  const currentProcess = readProcessStartIdentity();
  if (!client.options.mainAgent.local || manager.identity.kind !== "main" || manager.identity.id !== config.rootId ||
      !nativeRoot(caller, config.rootId) || !caller.local || !currentProcess ||
      caller.processIdentity?.pid !== currentProcess.pid || caller.processIdentity.startTime !== currentProcess.startTime ||
      caller.processIdentity.kernelId !== currentProcess.kernelId) throw new Error("Successor removal requires the native Main root");
  const selected = await removalRegistry(client, manager, id);
  const actor = selected.actor;
  try {
    if (canonical(actor.project) === canonical(caller.project) && selected.manager.owns(actor.id)) {
      return await selected.manager.remove(actor.id);
    }
    if (!actor.rootId || actor.rootId === config.rootId || actor.residency !== "durable") {
      throw new Error("Successor removal requires a foreign durable actor");
    }
    const expectedRootId = actor.rootId;
    const dir = residentRoot(mesh.root, expectedRootId);
    const predecessor = readRecord<ResidentHostConfig>(path.join(dir, "config.json"));
    const oldRoot = predecessor?.rootOwner;
    if (!predecessor || predecessor.format !== 1 || predecessor.rootId !== expectedRootId || !nativeRoot(oldRoot, expectedRootId) ||
        typeof predecessor.sessionId !== "string" || !predecessor.sessionId || predecessor.sessionId.includes("/") ||
        typeof predecessor.residencyRoot !== "string") {
      throw new Error("Successor removal requires a recorded native predecessor Main owner");
    }
    const project = canonical(caller.project);
    if (!project || canonical(config.project) !== project || canonical(actor.project) !== project ||
        canonical(oldRoot.project) !== project || canonical(predecessor.project) !== project ||
        canonical(predecessor.meshRoot) !== canonical(mesh.root) ||
        canonical(predecessor.actorRoot) !== canonical(config.actorRoot) ||
        path.resolve(predecessor.residencyRoot) !== dir) {
      throw new Error("Successor removal requires the same project and actor registry");
    }
    if (typeof caller.agentName !== "string" || !caller.agentName || typeof oldRoot.agentName !== "string" || !oldRoot.agentName || caller.agentName !== oldRoot.agentName ||
        !caller.role || caller.role !== oldRoot.role || config.role !== caller.role || predecessor.role !== oldRoot.role ||
        caller.name !== oldRoot.name) throw new Error("Successor removal requires the same agent and role");
    const mainIdentity = oldRoot.processIdentity;
    if (!validProcessStartIdentity(mainIdentity)) throw new Error("Successor removal requires a recorded Main process identity");
    const hostId = residentHostId(expectedRootId);
    const assertRootDead = (): void => {
      const stalled = participants.writeStalled?.();
      if (stalled) throw stalled;
      // A same-session Main can restart before its first participant heartbeat. Do not keep
      // proving death against the old snapshot after config.json recorded a different owner.
      const freshOwner = readRecord<ResidentHostConfig>(path.join(dir, "config.json"))?.rootOwner;
      if (!nativeRoot(freshOwner, expectedRootId) || !validProcessStartIdentity(freshOwner.processIdentity) ||
          freshOwner.processIdentity.pid !== mainIdentity.pid || freshOwner.processIdentity.startTime !== mainIdentity.startTime ||
          freshOwner.processIdentity.kernelId !== mainIdentity.kernelId || freshOwner.agentName !== oldRoot.agentName ||
          freshOwner.role !== oldRoot.role || canonical(freshOwner.project) !== project) {
        if (validProcessStartIdentity(freshOwner?.processIdentity) && processStartIdentityState(freshOwner.processIdentity) === "alive") {
          throw new Error(`Fabric actor ${actor.id} is owned by a live root`);
        }
        throw new Error("Predecessor Main ownership changed while verifying removal");
      }
      const live = participants.list({ scope: "project", fresh: true });
      if (participants.get(expectedRootId, undefined, { fresh: true }) ||
          live.some((record) => record.rootId === expectedRootId && record.ownerHostId !== hostId)) {
        throw new Error(`Fabric actor ${actor.id} is owned by a live root`);
      }
      const known = participants.lastKnown?.(expectedRootId)?.participant;
      if (known && (!nativeRoot(known, expectedRootId) || known.agentName !== oldRoot.agentName ||
          known.role !== oldRoot.role || canonical(known.project) !== project ||
          !validProcessStartIdentity(known.processIdentity) || known.processIdentity.pid !== mainIdentity.pid || known.processIdentity.startTime !== mainIdentity.startTime || known.processIdentity.kernelId !== mainIdentity.kernelId)) {
        throw new Error("Cannot prove predecessor root death: participant identity changed");
      }
      const now = Date.now();
      assertReadableLeaseFiles(mesh.root);
      const leases = readHostLeases(mesh.root);
      if ([...leases.values()].some((lease) => lease.rootId === expectedRootId && lease.id !== hostId && lease.expiresAt >= now) ||
          mesh.listAll("topology/hosts/", { fresh: true }).some((entry) => {
            const host = entry.value as Partial<FabricHostRecord> | null;
            if (!host || host.format !== 1 || typeof host.id !== "string" || typeof host.rootId !== "string" || !Number.isFinite(host.expiresAt)) {
              throw new Error("Cannot prove predecessor root death: malformed shared host lease");
            }
            return host.rootId === expectedRootId && host.id !== hostId && (host.expiresAt ?? Infinity) >= now;
          })) throw new Error(`Fabric actor ${actor.id} is owned by a live root`);
      const state = processStartIdentityState(mainIdentity);
      // A different kernel start proves the recorded Main died even when its PID was reused.
      // Do not read or copy Main argv: only the resident host needs command-line verification.
      if (state !== "dead" && state !== "mismatch") {
        throw new Error(state === "alive" ? `Fabric actor ${actor.id} is owned by a live root`
          : "Cannot prove predecessor Main process is dead");
      }
    };
    const recordedHost = (): ProcessIdentity | undefined => {
      const owner = readRecord<ResidentHostOwner>(path.join(dir, "owner.json"));
      const lock = readRecord<{ pid: number; token: string; processIdentity?: ProcessIdentity }>(path.join(dir, "host.lock"));
      if (!owner && !lock) return undefined; // a clean host shutdown removed both records
      const record = owner ?? lock!;
      if (!validProcessIdentity(record.processIdentity) || record.pid !== record.processIdentity.pid ||
          typeof record.token !== "string" || !record.token || record.pid === process.pid || record.pid === mainIdentity.pid ||
          (owner && (owner.format !== 1 || owner.hostId !== hostId)) ||
          (owner && lock && (owner.token !== lock.token || owner.pid !== lock.pid ||
            !validProcessIdentity(lock.processIdentity) || !sameIdentity(owner.processIdentity!, lock.processIdentity)))) {
        throw new Error("Resident host process identity mismatch (or missing recorded identity); no signal sent");
      }
      return record.processIdentity;
    };
    const assertHostStopped = (): void => {
      const recorded = recordedHost();
      if (recorded && processIdentityState(recorded) !== "dead") {
        throw new Error("Resident host process identity mismatch or host still running; no removal accepted");
      }
    };
    assertRootDead();
    const host = recordedHost();
    if (host && !["alive", "dead"].includes(processIdentityState(host))) {
      throw new Error("Resident host process identity mismatch; no signal sent");
    }
    // Retire the old residency root before stopping it. New hosts check this before and after
    // acquiring host.lock, so an old launcher cannot resurrect a writer behind our registry fence.
    writeJsonAtomic(path.join(dir, "retired.json"), { rootId: expectedRootId, by: manager.identity, mainIdentity }, { durable: true });
    if (host) {
      const signal = (value: NodeJS.Signals): void => {
        assertRootDead();
        const current = recordedHost();
        if (!current || !sameIdentity(current, host)) throw new Error("Resident host process identity mismatch; no signal sent");
        const state = processIdentityState(host);
        if (state === "dead") return;
        if (state !== "alive") throw new Error("Resident host process identity mismatch; no signal sent");
        try { process.kill(host.pid, value); }
        catch (error) { if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error; }
      };
      signal("SIGTERM");
      const waitUntil = async (deadline: number): Promise<void> => {
        // A dying process can briefly have an empty cmdline before becoming a zombie/absent.
        // Keep observing until kernel-proven death; never send another signal on unknown/mismatch.
        while (processIdentityState(host) !== "dead" && Date.now() < deadline) {
          await new Promise((resolve) => setTimeout(resolve, 25));
        }
      };
      // Do not force-kill a host with accepted runs: its normal close drains/stops workers. A host
      // that cannot finish close leaves removal unaccepted, for a later retry or explicit repair.
      await waitUntil(Date.now() + 30_000);
      if (processIdentityState(host) !== "dead") throw new Error("Resident host process identity mismatch or failed to stop");
    }
    assertRootDead();
    assertHostStopped();
    // A further rotation can recover this Main's accepted removal even if it never needed to
    // start a resident host of its own. Keep the same durable ownership evidence as a host launch.
    config.rootOwner = caller;
    writeJsonAtomic(path.join(config.residencyRoot, "config.json"), config, { durable: true });
    return await selected.manager.removeSuccessor(actor.id, expectedRootId, {
      presenceKey: `actors/${predecessor.sessionId}/${actor.id}`,
      assertSafe: () => { assertRootDead(); assertHostStopped(); },
    });
  } finally { await selected.temporary?.close(); }
};
