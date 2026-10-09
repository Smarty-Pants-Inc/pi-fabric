import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { MeshIdentity, MeshStateEntry, MeshStore, MeshReadOptions } from "../mesh/store.js";
import { processStartTime } from "../residency/process-identity.js";
import { PARTICIPANT_NAME_PATTERN } from "./participant-name.js";
import type { FabricParticipantRecord } from "./types.js";

/** Selector custody, NOT authenticated identity: same-UID processes are inside fleet trust. */
export interface MainNameOwner {
  host: string;
  pid: number;
  processStartedAt?: string;
}

let processOwner: MainNameOwner | undefined;
/** Lazy once per process: presence and binding use exactly the same witness. */
export const mainNameProcessOwner = (): MainNameOwner => {
  if (!processOwner) {
    const started = processStartTime(process.pid);
    processOwner = { host: os.hostname(), pid: process.pid, ...(started ? { processStartedAt: started } : {}) };
  }
  return { ...processOwner };
};

export interface MainNameBinding {
  format: 1;
  name: string;
  sessionId: string;
  herdrPane?: string;
  hostId: string;
  owner: MainNameOwner;
}

const digest = (name: string): string => createHash("sha256").update(name).digest("hex");
export const mainNameBindingKey = (name: string): string => `topology/main-names/${digest(name)}`;
export const MAIN_NAME_REBINDING_PREFIX = "topology/main-name-rebindings/";

const bindingOf = (entry: MeshStateEntry | undefined, name: string): MainNameBinding | undefined => {
  if (!entry) return undefined;
  const b = entry.value as MainNameBinding | undefined;
  if (!b || b.format !== 1 || b.name !== name || typeof b.sessionId !== "string" || !b.sessionId ||
    typeof b.hostId !== "string" || !b.hostId || (b.herdrPane !== undefined && typeof b.herdrPane !== "string") ||
    !b.owner || typeof b.owner.host !== "string" || !b.owner.host ||
    !Number.isSafeInteger(b.owner.pid) || b.owner.pid <= 0 ||
    (b.owner.processStartedAt !== undefined && (typeof b.owner.processStartedAt !== "string" || !/^\d+$/.test(b.owner.processStartedAt)))) {
    // Corruption is not an empty slot or a death witness. Do not overwrite it.
    throw new Error(`Invalid durable Main name binding: ${name}`);
  }
  return b;
};

export const readMainNameBinding = (mesh: MeshStore, name: string, read: MeshReadOptions = { fresh: true }): MainNameBinding | undefined =>
  bindingOf(mesh.get(mainNameBindingKey(name), read), name);

/** Absence/lease expiry/closure/reload are never death. Never inspect a remote host's pid locally. */
export const mainNameOwnerDead = (binding: Pick<MainNameBinding, "owner">): boolean => {
  if (!binding.owner || binding.owner.host !== os.hostname() || !Number.isSafeInteger(binding.owner.pid) || binding.owner.pid <= 0 ||
    (binding.owner.processStartedAt !== undefined && (typeof binding.owner.processStartedAt !== "string" || !/^\d+$/.test(binding.owner.processStartedAt)))) return false;
  try { process.kill(binding.owner.pid, 0); }
  catch (error) { return (error as NodeJS.ErrnoException).code === "ESRCH"; }
  // Same Linux start-tick witness as residentProcessAlive; unreadable identity is unknown,
  // not dead. In particular EPERM/EIO and a missing lease cannot authorize rebinding.
  const started = binding.owner.processStartedAt;
  const actual = processStartTime(binding.owner.pid);
  return started !== undefined && actual !== undefined && actual !== started;
};

/** Called before local Main presence publication, and again on its heartbeat. */
export const claimMainName = async (mesh: MeshStore, identity: MeshIdentity, root: FabricParticipantRecord): Promise<void> => {
  if (identity.kind !== "main" || root.kind !== "root" || root.interactive === false || root.remoteHost ||
    !root.sessionId || root.id !== identity.id || root.ownerIdentityId !== identity.id ||
    !PARTICIPANT_NAME_PATTERN.test(root.name)) return;
  const key = mainNameBindingKey(root.name);
  const entry = mesh.get(key, { fresh: true });
  const previous = bindingOf(entry, root.name);
  const next: MainNameBinding = {
    format: 1, name: root.name, sessionId: root.sessionId,
    ...(root.herdrPane ? { herdrPane: root.herdrPane } : {}), hostId: root.ownerHostId,
    owner: mainNameProcessOwner(),
  };
  const sameSession = previous?.sessionId === next.sessionId;
  if (previous && !sameSession && !mainNameOwnerDead(previous)) return;
  if (previous && JSON.stringify(previous) === JSON.stringify(next)) return;
  // Inspect the witness before custody; immutable observed binding + exact CAS prevent a
  // newer continuation from being reclaimed. The next heartbeat retries a lost CAS.
  await mesh.writeBatch({ identity, ops: [], prepare: view => {
    const current = view.get(key);
    if (current?.version !== entry?.version || JSON.stringify(current?.value) !== JSON.stringify(entry?.value)) return [];
    const version = view.version(key);
    return [
      { kind: "put", key, value: next, ifVersion: version },
      ...(previous && !sameSession ? [{
        kind: "put" as const, key: `${MAIN_NAME_REBINDING_PREFIX}${digest(root.name)}/${version + 1}`,
        value: (at: number) => ({ format: 1, name: root.name, previous, next, reboundAt: at, reason: "owner-process-dead" }),
        ifVersion: 0,
      }] : []),
    ];
  } });
};

const FIXED_PRINCIPAL_NAMES = ["org", "org-kate", "org-marisela", "org-deputy", "org-preview-paul"];

/** Read launch/project setup first, then the fleet checkout; unavailable config keeps fixed denies. */
export const principalMainNames = (cwd = process.cwd()): ReadonlySet<string> => {
  const names = new Set(FIXED_PRINCIPAL_NAMES);
  const candidates: string[] = [];
  for (let dir = path.resolve(cwd); ; dir = path.dirname(dir)) {
    candidates.push(path.join(dir, "setup", "org.json"));
    if (path.dirname(dir) === dir) break;
  }
  candidates.push(path.join(os.homedir(), "smarty", "smarty-pants", "setup", "org.json"));
  for (const file of candidates) {
    let fd: number | undefined;
    try {
      fd = fs.openSync(file, fs.constants.O_RDONLY | (fs.constants.O_NONBLOCK ?? 0));
      const stat = fs.fstatSync(fd);
      if (!stat.isFile() || stat.size > 256 * 1024) continue;
      const bytes = Buffer.alloc(256 * 1024 + 1);
      const count = fs.readSync(fd, bytes, 0, bytes.length, 0);
      if (count > 256 * 1024) continue;
      const config = JSON.parse(bytes.subarray(0, count).toString("utf8")) as {
        principals?: Array<{ orgInstance?: { herdrAgent?: unknown } }>;
      };
      if (!Array.isArray(config.principals)) continue;
      for (const principal of config.principals) {
        const name = principal?.orgInstance?.herdrAgent;
        if (typeof name === "string" && PARTICIPANT_NAME_PATTERN.test(name)) names.add(name);
      }
      break;
    } catch { /* unavailable setup: fixed denies still apply */ }
    finally { if (fd !== undefined) fs.closeSync(fd); }
  }
  return names;
};
