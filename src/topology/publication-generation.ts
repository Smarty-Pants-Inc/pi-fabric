import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import type { MeshStateEntry } from "../mesh/store.js";
import { hostEntryLiveness, hostLeasePath, readHostLeaseCurrent, type FabricHostLease } from "./host-leases.js";
import { readParticipantFile } from "./participant-files.js";

const stampOf = (file: string): string => {
  try {
    const stat = fs.statSync(file, { bigint: true });
    return `${stat.dev}:${stat.ino}:${stat.size}:${stat.mtimeNs}:${stat.ctimeNs}`;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return "absent";
    throw error;
  }
};

/** Stamp of one directory beside the mesh (`participants`, `host-leases`): it changes when a file
 * in it is created, replaced by rename or removed. Windows directory timestamps are not replacement
 * receipts, so there the leaf stamps are included. Validation only, never authority: a caller that
 * read the directory before the state transaction (smarty-dev#6477 R11) compares this stamp inside
 * the transaction and reads again only when it moved. */
export const meshDirectoryStamp = (meshRoot: string, name: string): string => {
  const directory = path.join(meshRoot, name);
  const files = [directory];
  if (process.platform === "win32") {
    try { files.push(...fs.readdirSync(directory).filter(file => file.endsWith(".json")).sort().map(file => path.join(directory, file))); }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
  }
  return files.map(stampOf).join("|");
};

/** A mesh whose ACTIVE state backend names its committed revision (smarty-dev#6477 R7, MeshStore).
 * `stateRevision()` is undefined when state.json is the authority (file, shadow): its stat stands
 * in. A backend that commits elsewhere (SQLite) returns its commit stamp, comparable across time
 * and connections. Required, not optional: a source without it would silently stamp a state.json
 * that SQLite commits never update (pi-fabric#640 review round 1, P1). */
export interface PublicationGenerationSource {
  readonly root: string;
  stateRevision(): string | undefined;
}

/** Stamp of the committed shared state: the ACTIVE backend's revision when it names one (SQLite),
 * else the state.json leaf stamp (file, shadow). A bare root names a file-backed mesh (tests, tools
 * without a store); a store names its backend. */
const sharedStateStamp = (mesh: string | PublicationGenerationSource): string => {
  const revision = typeof mesh === "string" ? undefined : mesh.stateRevision();
  return revision === undefined ? stampOf(path.join(typeof mesh === "string" ? mesh : mesh.root, "state.json")) : `revision:${revision}`;
};

/** Cheap invalidation of a prepared ownership observation. Atomic replacements
 * change the file inode / parent change-time, even within one wall-clock tick.
 * This is validation, never authority; callers still prepare a fresh directory read. */
export const publicationGeneration = (mesh: string | PublicationGenerationSource): string => {
  const meshRoot = typeof mesh === "string" ? mesh : mesh.root;
  return [sharedStateStamp(mesh), meshDirectoryStamp(meshRoot, "participants"), meshDirectoryStamp(meshRoot, "host-leases")].join("|");
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

/** pi-fabric#637: the clock-dependent inputs of ParticipantDirectory.get(). A participant is
 * live while a "reloading" record's reloadUntil >= now and its owner's effective expiry (the
 * later of the shared host record and a MATCHING file lease, hostLiveness) >= now. */
const reloadDeadline = (entry: MeshStateEntry | undefined): number | undefined => {
  const value = entry?.value;
  return isRecord(value) && value.status === "reloading" ? (typeof value.reloadUntil === "number" ? value.reloadUntil : 0) : undefined;
};
const hostDeadline = (id: string, entry: MeshStateEntry | undefined, lease: FabricHostLease | undefined): number | undefined => {
  if (entry) return hostEntryLiveness(entry, lease ? new Map([[id, lease]]) : new Map()).expiresAt;
  // No shared record (get() then has no live owner): the file lease is the only clock input.
  return lease?.expiresAt;
};

/** One prepared observation of the ownership inputs of specific actors. */
export interface ActorOwnershipObservation {
  /** Narrow validation to the actors the save will actually write (call before unchanged()). */
  scope(ids: Iterable<string>): void;
  /** False once any ownership fact of a scoped actor changed since the observation, or once
   * the clock reached the earliest lease/reload deadline that was still live when observed. */
  unchanged(): boolean;
  /** Earliest observed live deadline of the scoped actors (Infinity when none). */
  deadline(): number;
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
  mesh: string | PublicationGenerationSource,
  actorIds: Iterable<string>,
  openState?: () => (key: string) => MeshStateEntry | undefined,
  options: { now?: () => number } = {},
): ActorOwnershipObservation => {
  const clock = options.now ?? Date.now;
  // pi-fabric#637: read the clock BEFORE the inputs, as get() does after them; a deadline is
  // kept only while still live at this instant (an already-lapsed lease was not relied on).
  const observedAt = clock();
  const live = (deadline: number | undefined): number => deadline !== undefined && deadline >= observedAt ? deadline : Number.POSITIVE_INFINITY;
  const meshRoot = typeof mesh === "string" ? mesh : mesh.root;
  // Stamp first, then content: a commit between them changes the stamp and is compared. The stamp
  // follows the ACTIVE backend (pi-fabric#640): SQLite commits never touch state.json, so a
  // state.json stamp would pass every shared ownership change on SQLite unseen.
  const stateStamp = sharedStateStamp(mesh);
  const state = openState?.();
  const participants = new Map<string, { key: string; file: string; stamp: string; facts: string; shared: string; owners: string[]; roots: string[]; deadline: number }>();
  const hosts = new Map<string, { file: string; stamp: string; facts: string; shared: string; deadline: number }>();
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
    const lease = readHostLeaseCurrent(meshRoot, id);
    const entry = state?.(HOST_PREFIX + digest(id));
    hosts.set(id, { file, stamp, facts: leaseFacts(lease), shared: hostFacts(entry), deadline: live(hostDeadline(id, entry, lease)) });
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
    participants.set(id, { key, file, stamp, facts: participantFacts(fileEntry), shared: participantFacts(sharedEntry), owners, roots,
      deadline: Math.min(live(reloadDeadline(fileEntry)), live(reloadDeadline(sharedEntry))) });
    for (const owner of owners) observeHost(owner);
    for (const root of roots) if (!closures.has(root)) closures.set(root, closureFacts(state?.(LINEAGE_CLOSURE_PREFIX + digest(root))));
  }
  let scoped = [...participants.keys()];
  const scopedDeadline = (records: ReadonlyArray<{ owners: string[]; deadline: number }>): number =>
    Math.min(Number.POSITIVE_INFINITY, ...records.map(record => record.deadline),
      ...records.flatMap(record => record.owners).map(id => hosts.get(id)?.deadline ?? Number.POSITIVE_INFINITY));
  return {
    scope(ids) { scoped = [...new Set(ids)]; },
    deadline() {
      return scopedDeadline(scoped.flatMap(id => participants.get(id) ?? []));
    },
    unchanged() {
      const selected = scoped.map(id => participants.get(id));
      // An actor outside the observation was never observed: fail closed.
      if (selected.some(entry => entry === undefined)) return false;
      const records = selected as Array<NonNullable<typeof selected[number]>>;
      // pi-fabric#637: lease expiry and reloadUntil change the decision without any write, so
      // no stamp can see them. Once the clock reaches the earliest deadline that was live when
      // observed, the observed decision may be stale: veto, and the retry decides afresh.
      if (clock() >= scopedDeadline(records)) return false;
      const owners = new Set(records.flatMap(record => record.owners));
      const roots = new Set(records.flatMap(record => record.roots));
      for (const record of records) {
        if (stampOf(record.file) !== record.stamp && participantFacts(readParticipantFile(meshRoot, record.key)) !== record.facts) return false;
      }
      for (const id of owners) {
        const host = hosts.get(id)!;
        if (stampOf(host.file) !== host.stamp && leaseFacts(readHostLeaseCurrent(meshRoot, id)) !== host.facts) return false;
      }
      if (sharedStateStamp(mesh) === stateStamp || !openState) return true;
      const current = openState();
      return records.every(record => participantFacts(current(record.key)) === record.shared) &&
        [...owners].every(id => hostFacts(current(HOST_PREFIX + digest(id))) === hosts.get(id)!.shared) &&
        [...roots].every(root => closureFacts(current(LINEAGE_CLOSURE_PREFIX + digest(root))) === closures.get(root));
    },
  };
};
