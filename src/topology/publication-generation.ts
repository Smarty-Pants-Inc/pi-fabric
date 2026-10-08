import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import type { MeshStateEntry } from "../mesh/store.js";
import { hostLeasePath, readHostLeaseCurrent, type FabricHostLease } from "./host-leases.js";
import { readParticipantFile } from "./participant-files.js";

/** Cheap invalidation of a prepared ownership observation. Atomic replacements
 * change the file inode / parent change-time, even within one wall-clock tick.
 * Windows directory timestamps are not replacement receipts: include leaf stamps.
 * This is validation, never authority; callers still prepare a fresh directory read. */
export const publicationGeneration = (meshRoot: string): string => {
  const stamp = (file: string): string => {
    try {
      const stat = fs.statSync(file, { bigint: true });
      return `${stat.dev}:${stat.ino}:${stat.size}:${stat.mtimeNs}:${stat.ctimeNs}`;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") return "absent";
      throw error;
    }
  };
  const files = [path.join(meshRoot, "state.json")];
  for (const name of ["participants", "host-leases"]) {
    const directory = path.join(meshRoot, name);
    files.push(directory);
    if (process.platform === "win32") {
      try { files.push(...fs.readdirSync(directory).filter(file => file.endsWith(".json")).sort().map(file => path.join(directory, file))); }
      catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
    }
  }
  return files.map(stamp).join("|");
};

const stampOf = (file: string): string => {
  try {
    const stat = fs.statSync(file, { bigint: true });
    return `${stat.dev}:${stat.ino}:${stat.size}:${stat.mtimeNs}:${stat.ctimeNs}`;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return "absent";
    throw error;
  }
};

const PARTICIPANT_PREFIX = "topology/participants/";
const HOST_PREFIX = "topology/hosts/";
const LINEAGE_CLOSURE_PREFIX = "topology/lineage-closures/";
const digest = (id: string): string => createHash("sha256").update(id).digest("hex");
const isRecord = (value: unknown): value is Record<string, unknown> => typeof value === "object" && value !== null && !Array.isArray(value);

/** The ownership facts of one participant entry; renewal fields (version, updatedAt,
 * activity status) are not ownership and must not invalidate a prepared save. */
const participantFacts = (entry: MeshStateEntry | undefined): string => {
  if (!entry) return "absent";
  const value = entry.value;
  if (!isRecord(value)) return JSON.stringify(["invalid", value ?? null]);
  return JSON.stringify([entry.key, entry.updatedBy?.id ?? null, value.format ?? null, value.id ?? null, value.kind ?? null,
    value.rootId ?? null, value.ownerHostId ?? null, value.ownerIdentityId ?? null, value.remoteHost ?? null,
    value.residency ?? null, value.actorOwnershipToken ?? null, value.status === "reloading" ? ["reloading", value.reloadUntil ?? null] : null]);
};
const hostFacts = (entry: MeshStateEntry | undefined): string => {
  if (!entry) return "absent";
  const value = entry.value;
  if (!isRecord(value)) return JSON.stringify(["invalid", value ?? null]);
  const identity = isRecord(value.identity) ? value.identity : {};
  return JSON.stringify([entry.key, entry.updatedBy?.id ?? null, value.format ?? null, value.id ?? null, value.rootId ?? null,
    identity.id ?? null, identity.kind ?? null, value.startedAt ?? null, value.remoteHost ?? null]);
};
const leaseFacts = (lease: FabricHostLease | undefined): string => lease
  ? JSON.stringify([lease.id, lease.rootId, lease.identityId, lease.startedAt ?? null, lease.session?.id ?? null, lease.session?.startedAt ?? null])
  : "absent";
const closureFacts = (entry: MeshStateEntry | undefined): string => entry ? JSON.stringify([entry.updatedBy?.id ?? null, entry.value ?? null]) : "absent";

/** One prepared observation of the ownership inputs of specific actors. */
export interface ActorOwnershipObservation {
  /** Narrow validation to the actors the save will actually write (call before unchanged()). */
  scope(ids: Iterable<string>): void;
  /** False once any ownership fact of a scoped actor changed since the observation. */
  unchanged(): boolean;
}

/**
 * smarty-dev#6829: scoped replacement for `publicationGeneration` in registry saves. The
 * fleet-wide directory stamps changed on every heartbeat anywhere on the mesh, so busy
 * roots vetoed almost every save. An actor's ownership decision reads exactly: its own
 * participant record (file and shared entry), its owner host's shared record and file
 * lease, and its root's lineage closure. Observe those, call this BEFORE the ownership
 * decision, and veto only when one of those facts changes. Cheap leaf stamps are the
 * fast path; a changed leaf is re-read and compared on ownership facts, so the owner's
 * own renewals (version/updatedAt/expiresAt) do not veto while a replaced owner lease,
 * a moved or removed participant or a closed lineage still does. Leaf files only, so the
 * Windows leaf-stamp behaviour is the behaviour on every platform.
 * Validation, never authority: callers still decide ownership from a fresh directory read.
 */
export const observeActorOwnership = (
  meshRoot: string,
  actorIds: Iterable<string>,
  openState?: () => (key: string) => MeshStateEntry | undefined,
): ActorOwnershipObservation => {
  const statePath = path.join(meshRoot, "state.json");
  // Leaf stamp first, then content: a write between them changes the stamp and is compared.
  const stateStamp = stampOf(statePath);
  const state = openState?.();
  const participants = new Map<string, { key: string; file: string; stamp: string; facts: string; shared: string; owners: string[]; roots: string[] }>();
  const hosts = new Map<string, { file: string; stamp: string; facts: string; shared: string }>();
  const closures = new Map<string, string>();
  const ownerOf = (entry: MeshStateEntry | undefined): { owner?: string; root?: string } => {
    const value = entry?.value;
    return isRecord(value) ? {
      ...(typeof value.ownerHostId === "string" ? { owner: value.ownerHostId } : {}),
      ...(typeof value.rootId === "string" ? { root: value.rootId } : {}),
    } : {};
  };
  const observeHost = (id: string): void => {
    if (hosts.has(id)) return;
    const file = hostLeasePath(meshRoot, id);
    const stamp = stampOf(file);
    hosts.set(id, { file, stamp, facts: leaseFacts(readHostLeaseCurrent(meshRoot, id)), shared: hostFacts(state?.(HOST_PREFIX + digest(id))) });
  };
  for (const id of actorIds) {
    if (participants.has(id)) continue;
    const hash = digest(id);
    const key = PARTICIPANT_PREFIX + hash;
    const file = path.join(meshRoot, "participants", `${hash}.json`);
    const stamp = stampOf(file);
    const fileEntry = readParticipantFile(meshRoot, key);
    const sharedEntry = state?.(key);
    const owners = [...new Set([ownerOf(fileEntry).owner, ownerOf(sharedEntry).owner].filter((owner): owner is string => owner !== undefined))];
    const roots = [...new Set([ownerOf(fileEntry).root, ownerOf(sharedEntry).root].filter((root): root is string => root !== undefined))];
    participants.set(id, { key, file, stamp, facts: participantFacts(fileEntry), shared: participantFacts(sharedEntry), owners, roots });
    for (const owner of owners) observeHost(owner);
    for (const root of roots) if (!closures.has(root)) closures.set(root, closureFacts(state?.(LINEAGE_CLOSURE_PREFIX + digest(root))));
  }
  let scoped = [...participants.keys()];
  return {
    scope(ids) { scoped = [...new Set(ids)]; },
    unchanged() {
      const selected = scoped.map(id => participants.get(id));
      // An actor outside the observation was never observed: fail closed.
      if (selected.some(entry => entry === undefined)) return false;
      const records = selected as Array<NonNullable<typeof selected[number]>>;
      const owners = new Set(records.flatMap(record => record.owners));
      const roots = new Set(records.flatMap(record => record.roots));
      for (const record of records) {
        if (stampOf(record.file) !== record.stamp && participantFacts(readParticipantFile(meshRoot, record.key)) !== record.facts) return false;
      }
      for (const id of owners) {
        const host = hosts.get(id)!;
        if (stampOf(host.file) !== host.stamp && leaseFacts(readHostLeaseCurrent(meshRoot, id)) !== host.facts) return false;
      }
      if (stampOf(statePath) === stateStamp || !openState) return true;
      const current = openState();
      return records.every(record => participantFacts(current(record.key)) === record.shared) &&
        [...owners].every(id => hostFacts(current(HOST_PREFIX + digest(id))) === hosts.get(id)!.shared) &&
        [...roots].every(root => closureFacts(current(LINEAGE_CLOSURE_PREFIX + digest(root))) === closures.get(root));
    },
  };
};
