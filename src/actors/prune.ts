import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import type { MeshIdentity, MeshStateEntry, MeshStore } from "../mesh/store.js";
import { removeParticipantFileIf, readParticipantFiles } from "../topology/participant-files.js";
import { residentRoot } from "../residency/protocol.js";
import { residentProcessAlive } from "../residency/process-identity.js";
import { FileLockBusy, kernelFenceAvailable, lockFile } from "../residency/file-lock.js";
import { ActorRegistryStore } from "./registry-store.js";

export interface ActorPruneRequest { root: string; dryRun?: boolean }
export interface ActorPruneResult {
  root: string;
  dryRun: boolean;
  actors: Array<{ id: string; name: string; registry: string }>;
  files: string[];
  stateKeys: string[];
  removed: { actors: number; files: number; stateKeys: number };
}
const hash = (value: string) => createHash("sha256").update(value).digest("hex");
const record = (value: unknown): Record<string, unknown> | undefined =>
  typeof value === "object" && value !== null && !Array.isArray(value) ? value as Record<string, unknown> : undefined;
const exists = (file: string): boolean => {
  try { fs.lstatSync(file); return true; }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return false; throw error; }
};
const read = (file: string): unknown => JSON.parse(fs.readFileSync(file, "utf8"));

/** Hold the resident host's existing kernel fence until cleanup completes; never unlink its inode. */
const residentFence = async <T>(mesh: MeshStore, root: string, operation: () => Promise<T>): Promise<T> => {
  const dir = residentRoot(mesh.root, root);
  const locked = path.join(dir, "host.lock");
  const refuse = () => { throw new Error(`Cannot prune live lineage ${root}: resident owner/host lock is live`); };
  const checkOwner = () => {
    for (const file of [path.join(dir, "owner.json"), locked]) {
      if (!exists(file)) continue;
      let owner: Record<string, unknown> | undefined;
      try { owner = record(read(file)); }
      catch { if (file === locked && process.platform === "linux") continue; throw new Error(`Cannot prove lineage ${root} dead: unreadable resident owner`); }
      if ((!Number.isSafeInteger(owner?.pid) || Number(owner?.pid) <= 0) &&
          (file !== locked || process.platform !== "linux")) {
        throw new Error(`Cannot prove lineage ${root} dead: invalid resident owner`);
      }
      if (typeof owner?.pid === "number" && residentProcessAlive(owner.pid,
          typeof owner.processStartTime === "string" ? owner.processStartTime : undefined)) refuse();
    }
  };
  checkOwner();
  let fd: number | undefined;
  if (exists(locked) && process.platform === "linux") {
    if (!kernelFenceAvailable()) throw new Error(`Cannot prove lineage ${root} dead: resident kernel fence unavailable`);
    try { fd = await lockFile(locked, 0, true, false); }
    catch (error) { if (error instanceof FileLockBusy) refuse(); throw error; }
  }
  try { checkOwner(); return await operation(); }
  finally { if (fd !== undefined) fs.closeSync(fd); }
};

/** Rare explicit maintenance path, loaded only at agents.prune first use. */
export const pruneActorRoot = async (
  request: ActorPruneRequest,
  options: { roots: string[]; mesh: MeshStore; identity: MeshIdentity; assertDead: () => void;
    canManageActor?: ((id: string) => boolean | undefined) | undefined; adoptionGraceMs: number },
): Promise<ActorPruneResult> => {
  const { root } = request;
  const dryRun = request.dryRun === true;
  options.assertDead();
  return residentFence(options.mesh, root, async () => {
    options.assertDead();
    const stores = [...new Set(options.roots)].map(at => ({ at, store: new ActorRegistryStore(at) }));
    const actors: ActorPruneResult["actors"] = [];
    const files = new Set<string>();
    const filesByRoot = new Map<string, Set<string>>();
    const addFile = (at: string, file: string) => {
      files.add(file); filesByRoot.set(at, new Set([...(filesByRoot.get(at) ?? []), file]));
    };
    const selected = new Map<string, Set<string>>();
    const proveUnowned = (rows: Array<Record<string, unknown> & { id: string }>) => {
      for (const row of rows.filter(row => row.rootId === root)) {
        if (options.canManageActor?.(row.id) !== undefined ||
            (typeof row.adoptedAt === "number" && Date.now() - row.adoptedAt < options.adoptionGraceMs)) {
          throw new Error(`Cannot prune live lineage ${root}: actor owner or adoption fence is live`);
        }
      }
    };
    const readRecords = (at: string, store: ActorRegistryStore): Array<Record<string, unknown> & { id: string }> => {
      const registry = path.join(at, "actors.json");
      if (!exists(registry)) return [];
      // Tolerant passive records() reads cannot authorize a destructive plan, including at commit.
      const parsed = record(store.read());
      if (!Array.isArray(parsed?.actors) || parsed.actors.some(value => typeof record(value)?.id !== "string")) {
        throw new Error(`Cannot prune: invalid actor registry ${registry}`);
      }
      return parsed.actors as Array<Record<string, unknown> & { id: string }>;
    };
    for (const { at, store } of stores) {
      const registry = path.join(at, "actors.json");
      if (exists(registry)) {
        const rows = readRecords(at, store);
        proveUnowned(rows);
        for (const row of rows.filter(row => row.rootId === root)) {
          if (!/^[a-f0-9]{32}$/.test(row.id)) throw new Error(`Cannot prune: invalid actor id ${row.id}`);
          actors.push({ id: row.id, name: typeof row.name === "string" ? row.name : row.id, registry });
          selected.set(at, new Set([...(selected.get(at) ?? []), row.id]));
        }
      }
      if (!exists(at)) continue;
      for (const file of fs.readdirSync(at)) {
        const match = /^removal-([a-f0-9]{32})\.json$/.exec(file);
        if (!match) continue;
        const marker = path.join(at, file);
        if (!fs.lstatSync(marker).isFile()) throw new Error(`Cannot prune: invalid removal marker ${marker}`);
        const cleanup = record(read(marker));
        if (record(cleanup?.owner)?.rootId !== root) continue;
        // A prepared or obsolete marker never authorizes deleting a now-adopted actor.
        if (store.records().some(row => row.id === match[1] && row.rootId !== root)) continue;
        selected.set(at, new Set([...(selected.get(at) ?? []), match[1]!]));
        addFile(at, path.join(at, file)); addFile(at, path.join(at, match[1]!));
      }
      for (const id of selected.get(at) ?? []) addFile(at, path.join(at, id));
      if (root.startsWith("session:")) addFile(at, path.join(at, "bindings", `${hash(root.slice(8))}.json`));
    }
    const state = options.mesh.listAll("", { fresh: true });
    const session = root.startsWith("session:") ? root.slice(8) : undefined;
    const inboxKey = `topology/inbox/${hash(root).slice(0, 32)}`;
    const belongs = (entry: MeshStateEntry): boolean => {
      const value = record(entry.value);
      if (entry.key === inboxKey) return true;
      if (entry.key.startsWith("actors/")) return typeof value?.rootId === "string"
        ? value.rootId === root : (session !== undefined && entry.key.startsWith(`actors/${session}/`));
      return entry.key.startsWith("topology/participants/") && value?.rootId === root;
    };
    const entries = state.filter(belongs);
    const participantFiles = readParticipantFiles(options.mesh.root, { maxAgeMs: 0 }).filter(belongs);
    const presentFiles = [...files].filter(exists).sort();
    const result: ActorPruneResult = { root, dryRun, actors, files: presentFiles,
      stateKeys: [...new Set([...entries, ...participantFiles].map(entry => entry.key))].sort(),
      removed: { actors: 0, files: 0, stateKeys: 0 } };
    if (dryRun) return result;
    options.assertDead();
    if (!actors.length && !presentFiles.length && !result.stateKeys.length) return result;
    // Keep the existing append-only owner audit, not a replacement or deletion of history.
    await options.mesh.publish({ topic: "ops.owner", kind: "actor.prune", from: options.identity,
      data: { root, actors, files: presentFiles, stateKeys: result.stateKeys } });
    for (const { at, store } of stores) {
      if (!exists(at) || (!(selected.get(at)?.size) &&
          !presentFiles.some(file => filesByRoot.get(at)?.has(file)))) continue;
      await store.withLock(() => {
        options.assertDead();
        const rows = readRecords(at, store);
        proveUnowned(rows);
        const ids = selected.get(at) ?? new Set<string>();
        // Adoption is fenced by this exact registry lock. A racing adopter wins; never delete its files.
        if (rows.some(row => (ids.has(row.id) && row.rootId !== root) || (row.rootId === root && !ids.has(row.id)))) {
          throw new Error(`Cannot prune lineage ${root}: actor ownership changed; retry the plan`);
        }
        // Files first: an I/O failure leaves the durable registry row so an explicit retry can finish.
        for (const file of presentFiles.filter(file => filesByRoot.get(at)?.has(file))) {
          if (!exists(file)) continue;
          if (/^removal-[a-f0-9]{32}\.json$/.test(path.basename(file)) && record(record(read(file))?.owner)?.rootId !== root) {
            throw new Error(`Cannot prune lineage ${root}: removal ownership changed`);
          }
          fs.rmSync(file, { recursive: true, force: true }); result.removed.files++;
        }
        const removed = rows.filter(row => row.rootId === root);
        if (removed.length) store.write(rows.filter(row => row.rootId !== root), { durable: true });
        result.removed.actors += removed.length;
      });
    }
    for (const entry of participantFiles) {
      options.assertDead();
      if (await removeParticipantFileIf(options.mesh, entry.key,
        current => { options.assertDead(); return current.version === entry.version && belongs(current); })) result.removed.stateKeys++;
    }
    if (entries.length) {
      options.assertDead();
      const removed = await options.mesh.writeBatch({ identity: options.identity, ops: entries.map(entry => ({
        kind: "delete" as const, key: entry.key, ifVersion: entry.version, onConflict: "abort" as const,
        condition: () => { options.assertDead(); return true; },
      })) });
      result.removed.stateKeys += removed.filter(item => item.applied && !participantFiles.some(entry => entry.key === item.key)).length;
    }
    return result;
  });
};
