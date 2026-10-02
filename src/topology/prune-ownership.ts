import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import type { MeshStateEntry } from "../mesh/store.js";
import type { FabricHostLease } from "./host-leases.js";

const object = (value: unknown): Record<string, unknown> | undefined =>
  typeof value === "object" && value !== null && !Array.isArray(value) ? value as Record<string, unknown> : undefined;
const text = (value: unknown): value is string => typeof value === "string" && value.length > 0;
const timestamp = (value: unknown): value is number => typeof value === "number" && Number.isFinite(value) && value >= 0;
const hash = (value: string) => createHash("sha256").update(value).digest("hex");
const missing = (error: unknown): boolean => object(error)?.code === "ENOENT";

/** Destructive maintenance never consults the tolerant discovery caches. */
export const assertPruneOwnershipDead = (meshRoot: string, root: string): {
  state: MeshStateEntry[]; participants: MeshStateEntry[]; liveRoots: Set<string>;
} => {
  const unknown = (reason: string): never => { throw new Error(`Cannot prove lineage ${root} dead: ${reason}`); };
  const live = (reason: string): never => { throw new Error(`Cannot prune live lineage ${root}: ${reason}`); };
  const read = (file: string, absent = false): unknown => {
    try {
      if (!fs.lstatSync(file).isFile()) return unknown(`invalid ownership file ${file}`);
      return JSON.parse(fs.readFileSync(file, "utf8"));
    } catch (error) {
      if (absent && missing(error)) return undefined;
      return unknown(`unreadable or invalid ownership evidence ${file}: ${String(error)}`);
    }
  };
  const listing = (dir: string): string[] => {
    try {
      if (!fs.lstatSync(dir).isDirectory()) return unknown(`invalid ownership directory ${dir}`);
      return fs.readdirSync(dir).filter(name => name.endsWith(".json"));
    } catch (error) {
      if (missing(error)) return [];
      return unknown(`unreadable ownership directory ${dir}: ${String(error)}`);
    }
  };
  const entryOf = (value: unknown, key: string, where: string): MeshStateEntry => {
    const entry = object(value);
    if (entry?.key !== key || !timestamp(entry.updatedAt) || !Number.isSafeInteger(entry.version) ||
        Number(entry.version) <= 0 || !text(object(entry.updatedBy)?.id)) unknown(`invalid ownership entry ${where}`);
    return entry as unknown as MeshStateEntry;
  };
  const leases = new Map<string, FabricHostLease>();
  const leaseDir = path.join(meshRoot, "host-leases");
  for (const name of listing(leaseDir)) {
    const file = path.join(leaseDir, name);
    const value = object(read(file));
    if (value?.format !== 1 || !text(value.id) || name !== `${hash(value.id).slice(0, 32)}.json` ||
        !text(value.rootId) || !text(value.identityId) || !timestamp(value.updatedAt) || !timestamp(value.expiresAt) ||
        value.expiresAt < value.updatedAt) unknown(`invalid host lease ${file}`);
    const lease = value as unknown as FabricHostLease;
    leases.set(lease.id, lease);
    if (lease.rootId === root && lease.expiresAt >= Date.now()) live(`host lease ${lease.id} is live`);
  }
  const stateFile = path.join(meshRoot, "state.json");
  const stateValue = read(stateFile, true);
  const state: MeshStateEntry[] = [];
  if (stateValue !== undefined) {
    const envelope = object(stateValue);
    const entries = object(envelope?.entries);
    if ((envelope?.format !== 1 && envelope?.format !== 2) || !entries) unknown(`invalid shared ownership state ${stateFile}`);
    for (const [key, value] of Object.entries(entries!)) state.push(entryOf(value, key, stateFile));
  }
  const participants: MeshStateEntry[] = [];
  const participantDir = path.join(meshRoot, "participants");
  for (const name of listing(participantDir)) {
    const file = path.join(participantDir, name);
    const value = object(read(file));
    if (!/^[a-f0-9]{64}\.json$/.test(name) || value?.format !== 1) unknown(`invalid participant ownership file ${file}`);
    participants.push(entryOf(value, `topology/participants/${name.slice(0, -5)}`, file));
  }
  const ambiguousRoots = new Set<string>();
  const hosts = new Map<string, { rootId: string; identityId: string; expiresAt: number }>();
  for (const entry of state.filter(entry => entry.key.startsWith("topology/hosts/"))) {
    const host = object(entry.value);
    const identity = object(host?.identity);
    if (host?.format !== 1 || !text(host.id) || entry.key !== `topology/hosts/${hash(host.id)}` ||
        !text(host.rootId) || !text(identity?.id) || entry.updatedBy.id !== identity.id ||
        !timestamp(host.updatedAt) || !timestamp(host.startedAt) || !timestamp(host.expiresAt) ||
        host.expiresAt < host.updatedAt) unknown(`invalid host ownership ${entry.key}`);
    const id = host!.id as string;
    const rootId = host!.rootId as string;
    const identityId = identity!.id as string;
    const expiresAt = host!.expiresAt as number;
    hosts.set(id, { rootId, identityId, expiresAt });
    const lease = leases.get(id);
    if (lease && (lease.rootId !== rootId || lease.identityId !== identityId)) {
      ambiguousRoots.add(rootId); ambiguousRoots.add(lease.rootId);
      if (rootId === root || lease.rootId === root) unknown(`conflicting host lease ownership ${id}`);
    }
    if (rootId === root && expiresAt >= Date.now()) live(`shared host lease ${id} is live`);
  }
  for (const entry of [...participants, ...state.filter(entry => entry.key.startsWith("topology/participants/"))]) {
    const participant = object(entry.value);
    if (participant?.format !== 1 || !text(participant.id) || entry.key !== `topology/participants/${hash(participant.id)}` ||
        !text(participant.rootId) || !text(participant.ownerHostId) || !text(participant.ownerIdentityId) ||
        (participant.residency !== undefined && participant.residency !== "session" && participant.residency !== "durable") ||
        !["root", "agent", "actor"].includes(String(participant.kind)) ||
        entry.updatedBy.id !== participant.ownerIdentityId || !timestamp(participant.updatedAt)) {
      unknown(`invalid participant ownership ${entry.key}`);
    }
    const participantRoot = participant!.rootId as string;
    const host = hosts.get(participant!.ownerHostId as string);
    const lease = leases.get(participant!.ownerHostId as string);
    for (const owner of [host, lease]) {
      if (owner && (owner.rootId !== participantRoot || owner.identityId !== participant!.ownerIdentityId)) {
        ambiguousRoots.add(participantRoot); ambiguousRoots.add(owner.rootId);
        if (participantRoot === root) unknown(`conflicting participant ownership ${entry.key}`);
      }
    }
  }
  // Only strict, fresh and non-conflicting ownership evidence may cancel an adopted receipt ID.
  const liveRoots = new Set([...leases.values(), ...hosts.values()]
    .filter(owner => owner.expiresAt >= Date.now() && !ambiguousRoots.has(owner.rootId))
    .map(owner => owner.rootId));
  return { state, participants, liveRoots };
};
