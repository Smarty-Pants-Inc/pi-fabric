import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { readFileRetrying, writeJsonAtomic } from "../core/atomic-write.js";
import type { MeshStore } from "../mesh/store.js";
import { retainRootRegistration, forgetRetainedRootRegistration } from "./root-registration-retention.js";

export interface RootRegistrationIdentity {
  sessionId: string;
  rootId: string;
  fabricSessionId: string;
  /** Only an explicit persisted Pi name; never the generic Main/peer display label. */
  name?: string | undefined;
}

export interface RootRegistrationOwner {
  id: string;
  pid: number;
  host: string;
  /** Linux process start tick prevents a recycled PID from holding a dead claim. */
  startTime: string;
}

interface Registration extends RootRegistrationIdentity {
  format: 1;
  owner: RootRegistrationOwner;
}

// A reload replaces modules, not the native session's process. Do not accept an inherited env
// owner token: another Pi with a copied session must not masquerade as that process's refresh.
const INCARNATION = Symbol.for("pi-fabric.root-registration-incarnation");
const processIncarnation = (): string => {
  const globals = globalThis as typeof globalThis & { [INCARNATION]?: string };
  return globals[INCARNATION] ??= randomUUID();
};
const processStat = (pid: number): { state: string; startTime: string } | undefined => {
  try {
    const stat = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
    const fields = stat.slice(stat.lastIndexOf(")") + 2).split(" ");
    return { state: fields[0] ?? "", startTime: fields[19] ?? "" };
  } catch { return undefined; }
};

const ownerAlive = (owner: RootRegistrationOwner): boolean => {
  // Native meshes are host-local; mirrors do not claim here. If one is put on a shared mount,
  // another hostname is unknown, never provably dead (no unsafe cross-host PID takeover).
  if (owner.host !== os.hostname()) return true;
  try { process.kill(owner.pid, 0); } catch (error) {
    return (error as { code?: unknown }).code !== "ESRCH";
  }
  const stat = processStat(owner.pid);
  return stat?.state !== "Z" && stat?.state !== "X" &&
    (!owner.startTime || !stat?.startTime || owner.startTime === stat.startTime);
};

const sameOwner = (left: RootRegistrationOwner, right: RootRegistrationOwner): boolean =>
  left.id === right.id && left.pid === right.pid && left.host === right.host && left.startTime === right.startTime;
const fileName = (owner: RootRegistrationOwner): string =>
  createHash("sha256").update(owner.id).digest("hex") + ".json";
const isObject = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);
const registrationOf = (text: string, name: string): Registration => {
  let value: unknown;
  try { value = JSON.parse(text); } catch { /* fail closed below */ }
  if (isObject(value) && value.format === 1 &&
    [value.sessionId, value.rootId, value.fabricSessionId].every(field => typeof field === "string" && field.length > 0) &&
    (value.name === undefined || typeof value.name === "string") && isObject(value.owner) &&
    typeof value.owner.id === "string" && value.owner.id.length > 0 &&
    typeof value.owner.pid === "number" && Number.isSafeInteger(value.owner.pid) && value.owner.pid > 0 &&
    typeof value.owner.host === "string" && value.owner.host.length > 0 && typeof value.owner.startTime === "string" &&
    fileName(value.owner as unknown as RootRegistrationOwner) === name) return value as unknown as Registration;
  throw new Error(`Unreadable Fabric root registration ${name}; ownership is unknown, not dead`);
};

export class DuplicateLiveRootError extends Error {
  readonly code = "FABRIC_DUPLICATE_LIVE_ROOT";
  constructor(readonly candidate: RootRegistrationIdentity, readonly incumbent: RootRegistrationIdentity & { owner?: RootRegistrationOwner }, readonly conflict: string) {
    super(`Duplicate live Fabric root refused: ${conflict}; candidate session ${candidate.sessionId} ` +
      `would share ownership with session ${incumbent.sessionId} (root ${incumbent.rootId}, ` +
      `name ${JSON.stringify(incumbent.name ?? "")}, ${incumbent.owner
        ? `pid ${incumbent.owner.pid} on ${incumbent.owner.host}` : "live participant lease"}). ` +
      "Use a fresh session and unique name, clear inherited Fabric root/session overrides, or close the existing owner first.");
    this.name = "DuplicateLiveRootError";
  }
}

/**
 * Admission before any root mailbox, actor registry, or participant publication. Claims are
 * mesh-local, one small file per process/session. The existing mesh lock serializes rare
 * admission/rename/release decisions; idle roots need no new heartbeat or eager engine import.
 * A paused but live process retains ownership, even if participant leases have lapsed.
 */
export class RootRegistrationGuard {
  readonly #dir: string;
  readonly #alive: (owner: RootRegistrationOwner) => boolean;
  #claimed: RootRegistrationOwner | undefined;
  #sessionId: string | undefined;
  readonly #reportedUnknownNames = new Set<string>();
  #closed = false;

  constructor(readonly mesh: MeshStore, readonly options: {
    /** Synthetic-owner/liveness seams for private tests, not runtime configuration. */
    owner?: RootRegistrationOwner;
    ownerAlive?: (owner: RootRegistrationOwner) => boolean;
    /** Fresh live native roots, including older runtimes that have not written claims yet. */
    publishedRoots?: () => readonly { rootId: string; sessionId?: string; name: string; rootRegistrationOwnerId?: string }[];
    onUnknownNameOwnership?: (warning: string) => void;
  } = {}) {
    this.#dir = path.join(mesh.root, "root-registrations");
    this.#alive = options.ownerAlive ?? ownerAlive;
  }

  get ownerId(): string | undefined { return this.#claimed?.id; }
  get sessionId(): string | undefined { return this.#sessionId; }

  async claim(identity: RootRegistrationIdentity): Promise<void> {
    if (this.#closed) throw new Error("Fabric root registration is closed");
    if (![identity.sessionId, identity.rootId, identity.fabricSessionId].every(value => typeof value === "string" && value.trim())) {
      throw new Error("Fabric root registration requires native session and lineage IDs");
    }
    const name = identity.name?.trim();
    const candidate: RootRegistrationIdentity = { ...identity, name: name || undefined };
    const owner = this.options.owner ?? {
      id: `${processIncarnation()}:${identity.sessionId}`, pid: process.pid, host: os.hostname(),
      startTime: processStat(process.pid)?.startTime ?? "",
    };
    const unknownNames: string[] = [];
    await this.mesh.exclusive(() => {
      if (this.#closed) throw new Error("Fabric root registration is closed");
      fs.mkdirSync(this.#dir, { recursive: true, mode: 0o700 });
      const dead: { file: string; ownerId: string }[] = [];
      const registeredRoots = new Map<string, Set<string>>();
      for (const name of fs.readdirSync(this.#dir)) {
        if (!name.endsWith(".json")) continue;
        const file = path.join(this.#dir, name);
        const current = registrationOf(readFileRetrying(file), name);
        const owners = registeredRoots.get(current.rootId) ?? new Set<string>();
        owners.add(current.owner.id);
        registeredRoots.set(current.rootId, owners);
        if (sameOwner(current.owner, owner)) continue;
        if (!this.#alive(current.owner)) { dead.push({ file, ownerId: current.owner.id }); continue; }
        const conflict = current.sessionId === candidate.sessionId ? "native session ID is already live" :
          current.rootId === candidate.rootId ? "root lineage is already live" :
          current.fabricSessionId === candidate.fabricSessionId ? "actor persistence session/lineage is already live" :
          candidate.name && current.name === candidate.name ? `name ${JSON.stringify(candidate.name)} is already live` : undefined;
        if (conflict) throw new DuplicateLiveRootError(candidate, current, conflict);
      }
      // Existing participant leases also protect roots from before this guard's rollout.
      // A process claim (including a provably dead predecessor) supplies stronger ownership
      // evidence for its root; do not mistake its leftover lease or our own refresh for a rival.
      const roots = this.options.publishedRoots?.() ?? [];
      for (const root of roots) {
        if (root.rootRegistrationOwnerId && registeredRoots.get(root.rootId)?.has(root.rootRegistrationOwnerId)) continue;
        const conflict = root.sessionId === candidate.sessionId ? "native session ID has a live participant lease" :
          root.sessionId === candidate.fabricSessionId ? "actor persistence session/lineage has a live participant lease" :
          root.rootId === candidate.rootId ? "root lineage has a live participant lease" :
          candidate.name && root.name !== "main" && root.name === candidate.name ?
            `name ${JSON.stringify(candidate.name)} has a live participant lease` : undefined;
        if (conflict) throw new DuplicateLiveRootError(candidate, {
          sessionId: root.sessionId ?? root.rootId, rootId: root.rootId,
          fabricSessionId: root.sessionId ?? root.rootId, name: root.name,
        }, conflict);
        if (candidate.name && root.name === "main") {
          unknownNames.push(`Cannot verify persisted name ownership for ${JSON.stringify(candidate.name)}: ` +
            `live legacy root ${root.rootId} publishes generic main without a matching process ownership token. ` +
            "A copied-name duplicate is possible; the named root is admitted, not proven unique. " +
            "Use a fresh unique name and reload or close the older owner before relying on named routing.");
        }
      }
      // Keep proof of death while that process's old participant lease is still visible.
      // Otherwise a refresh/reload of its successor could mistake the leftover lease for a
      // live unguarded root, particularly when the successor reused a name with a new UUID.
      // Cleanup can fail: finish it before committing ownership so a rejected rename keeps
      // the previous reservation in sync with the runtime's last admitted routing alias.
      for (const item of dead) {
        if (!roots.some(root => root.rootRegistrationOwnerId === item.ownerId)) fs.rmSync(item.file, { force: true });
      }
      writeJsonAtomic(path.join(this.#dir, fileName(owner)), { format: 1, ...candidate, owner });
      this.#claimed = owner;
      this.#sessionId = candidate.sessionId;
      forgetRetainedRootRegistration(candidate.sessionId, `${this.mesh.root}\0${owner.id}`);
    });
    for (const warning of unknownNames) {
      if (this.#reportedUnknownNames.has(warning) || this.#reportedUnknownNames.size >= 1_000) continue;
      this.#reportedUnknownNames.add(warning);
      this.options.onUnknownNameOwnership?.(warning);
    }
  }

  async close(options: { preserve?: boolean; onRetire?: () => Promise<void> } = {}): Promise<void> {
    this.#closed = true;
    // Native reload disposes this object but leaves its live process/session claim for the
    // replacement module. Never create a claim-free interval while that owner PID is live.
    if (options.preserve) {
      if (this.#claimed && this.#sessionId) {
        retainRootRegistration(this.#sessionId, `${this.mesh.root}\0${this.#claimed.id}`, async () => {
          await options.onRetire?.();
          await this.close();
        });
      }
      return;
    }
    await this.mesh.exclusive(() => {
      const owner = this.#claimed;
      if (!owner) return;
      const name = fileName(owner);
      const file = path.join(this.#dir, name);
      let text: string;
      try { text = readFileRetrying(file); } catch (error) {
        if ((error as { code?: unknown }).code === "ENOENT") {
          if (this.#sessionId) forgetRetainedRootRegistration(this.#sessionId, `${this.mesh.root}\0${owner.id}`);
          this.#claimed = undefined; return;
        }
        throw error;
      }
      if (sameOwner(registrationOf(text, name).owner, owner)) fs.rmSync(file, { force: true });
      this.#claimed = undefined;
      if (this.#sessionId) forgetRetainedRootRegistration(this.#sessionId, `${this.mesh.root}\0${owner.id}`);
      // The runtime closes its participant directory before releasing admission. Retire dead
      // predecessors whose leftover records are now absent, under the same serialization.
      const roots = this.options.publishedRoots?.() ?? [];
      for (const name of fs.readdirSync(this.#dir)) {
        if (!name.endsWith(".json")) continue;
        const file = path.join(this.#dir, name);
        const current = registrationOf(readFileRetrying(file), name);
        if (!this.#alive(current.owner) && !roots.some(root => root.rootRegistrationOwnerId === current.owner.id)) {
          fs.rmSync(file, { force: true });
        }
      }
    });
  }
}
