import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import type { MeshIdentity, MeshStateEntry, MeshStore } from "../mesh/store.js";
import { removeParticipantFileIf } from "../topology/participant-files.js";
import { residentRoot } from "../residency/protocol.js";
import { processStartTime } from "../residency/process-identity.js";
import { assertPruneOwnershipDead } from "../topology/prune-ownership.js";
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

/** The exact startup flock inode, held through every registry/file/state commit. Never unlink it. */
const residentFence = async <T>(mesh: MeshStore, root: string, dryRun: boolean,
  operation: (checkOwner: () => void) => Promise<T>): Promise<T> => {
  const dir = residentRoot(mesh.root, root);
  const locked = path.join(dir, "host.lock");
  const unknown = (reason: string): never => { throw new Error(`Cannot prove lineage ${root} dead: ${reason}`); };
  const refuse = (): never => { throw new Error(`Cannot prune live lineage ${root}: resident owner/host lock is live`); };
  let fd: number | undefined;
  const checkOwner = () => {
    if (fd !== undefined) {
      const held = fs.fstatSync(fd);
      const current = fs.lstatSync(locked);
      if (!current.isFile() || current.dev !== held.dev || current.ino !== held.ino) unknown("resident startup fence changed");
    }
    for (const file of [path.join(dir, "owner.json"), locked]) {
      if (!exists(file)) continue;
      if (!fs.lstatSync(file).isFile()) unknown(`invalid resident owner ${file}`);
      let owner: Record<string, unknown> | undefined;
      try {
        const contents = fs.readFileSync(file, "utf8");
        // Linux's persistent flock inode is empty until its first host claims it.
        if (file === locked && process.platform === "linux" && contents === "") continue;
        owner = record(JSON.parse(contents));
      } catch { unknown(`unreadable resident owner ${file}`); }
      if (!Number.isSafeInteger(owner?.pid) || Number(owner?.pid) <= 0 ||
          (owner?.processStartTime !== undefined && (typeof owner.processStartTime !== "string" || !/^\d+$/.test(owner.processStartTime)))) {
        unknown(`invalid resident owner ${file}`);
      }
      const pid = Number(owner!.pid);
      try { process.kill(pid, 0); }
      catch (error) {
        if ((error as NodeJS.ErrnoException).code === "ESRCH") continue;
        if ((error as NodeJS.ErrnoException).code === "EPERM") refuse();
        unknown(`unreadable resident process ${pid}: ${String(error)}`);
      }
      // Only positive PID-reuse evidence can disqualify a live process.
      const started = processStartTime(pid);
      if (process.platform !== "linux" || owner!.processStartTime === undefined ||
          started === undefined || started === owner!.processStartTime) refuse();
    }
  };
  checkOwner();
  if (!dryRun && !kernelFenceAvailable()) unknown("resident startup fence unavailable; destructive prune requires Linux flock/setpriv");
  if (!dryRun || (exists(locked) && process.platform === "linux")) {
    if (!kernelFenceAvailable()) unknown("resident kernel fence unavailable");
    if (!dryRun) fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
    try { fd = await lockFile(locked, 0, true, !dryRun); }
    catch (error) { if (error instanceof FileLockBusy) refuse(); throw error; }
  }
  try { checkOwner(); return await operation(checkOwner); }
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
  assertPruneOwnershipDead(options.mesh.root, root);
  options.assertDead();
  return residentFence(options.mesh, root, dryRun, async checkOwner => {
    const assertDead = () => {
      checkOwner();
      assertPruneOwnershipDead(options.mesh.root, root);
      options.assertDead();
    };
    assertDead();
    const stores = [...new Set(options.roots)].map(at => ({ at, store: new ActorRegistryStore(at) }));
    const actors: ActorPruneResult["actors"] = [];
    const files = new Set<string>();
    const filesByRoot = new Map<string, Set<string>>();
    const addFile = (at: string, file: string) => {
      files.add(file); filesByRoot.set(at, new Set([...(filesByRoot.get(at) ?? []), file]));
    };
    const selected = new Map<string, Set<string>>();
    const preserved = new Set<string>();
    // Missing residency is the legacy session default; unknown/new scopes are never selected.
    const eligible = (row: Record<string, unknown>): boolean =>
      row.rootId === root && (row.residency === "session" || row.residency === undefined);
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
      if (parsed?.format !== 1 || !Array.isArray(parsed.actors) || parsed.actors.some(value => {
        const row = record(value);
        return typeof row?.id !== "string" || !/^[a-f0-9]{32}$/.test(row.id) ||
          typeof row.rootId !== "string" || !row.rootId ||
          (row.residency !== undefined && row.residency !== "session" && row.residency !== "durable") ||
          (row.adoptedAt !== undefined && (typeof row.adoptedAt !== "number" || !Number.isFinite(row.adoptedAt) || row.adoptedAt < 0));
      }) || new Set(parsed.actors.map(value => record(value)!.id)).size !== parsed.actors.length) {
        throw new Error(`Cannot prove lineage ${root} dead: invalid actor registry ownership ${registry}`);
      }
      return parsed.actors as Array<Record<string, unknown> & { id: string }>;
    };
    for (const { at, store } of stores) {
      const registry = path.join(at, "actors.json");
      if (exists(registry)) {
        const rows = readRecords(at, store);
        proveUnowned(rows);
        for (const row of rows.filter(row => row.rootId === root && !eligible(row))) preserved.add(row.id);
        for (const row of rows.filter(eligible)) {
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
        const owner = record(cleanup?.owner);
        if (selected.get(at)?.has(match[1]!) && (owner?.rootId !== root || owner.residency !== "session")) {
          throw new Error(`Cannot prove lineage ${root} dead: conflicting removal ownership ${marker}`);
        }
        if (owner?.rootId !== root) continue;
        if (cleanup?.id !== match[1]) throw new Error(`Cannot prove lineage ${root} dead: invalid removal ownership ${marker}`);
        if (owner.residency !== "session") {
          if (selected.get(at)?.has(match[1]!)) throw new Error(`Cannot prove lineage ${root} dead: conflicting removal scope ${marker}`);
          preserved.add(match[1]!); continue;
        }
        // A prepared/obsolete marker cannot delete an adopted or excluded actor.
        if (readRecords(at, store).some(row => row.id === match[1] && !eligible(row))) continue;
        selected.set(at, new Set([...(selected.get(at) ?? []), match[1]!]));
        addFile(at, path.join(at, file)); addFile(at, path.join(at, match[1]!));
      }
      for (const id of selected.get(at) ?? []) addFile(at, path.join(at, id));
    }
    const ownership = assertPruneOwnershipDead(options.mesh.root, root);
    const selectedIds = new Set([...selected.values()].flatMap(ids => [...ids]));
    for (const entry of [...ownership.state, ...ownership.participants]) {
      const value = record(entry.value);
      if ((entry.key.startsWith("actors/") || value?.kind === "actor") && value?.rootId === root &&
          typeof value.id === "string" && (!selectedIds.has(value.id) || value.residency === "durable")) {
        if (selectedIds.has(value.id)) throw new Error(`Cannot prove lineage ${root} dead: conflicting actor residency ${value.id}`);
        preserved.add(value.id);
      }
    }
    // Shared root metadata may also be needed by excluded durable actors.
    if (root.startsWith("session:") && preserved.size === 0) {
      for (const { at } of stores) addFile(at, path.join(at, "bindings", `${hash(root.slice(8))}.json`));
    }
    const session = root.startsWith("session:") ? root.slice(8) : undefined;
    const inboxKey = `topology/inbox/${hash(root).slice(0, 32)}`;
    const belongs = (entry: MeshStateEntry): boolean => {
      const value = record(entry.value);
      if (value?.residency === "durable" || (typeof value?.id === "string" && preserved.has(value.id))) return false;
      if (entry.key === inboxKey) return preserved.size === 0;
      if (entry.key.startsWith("actors/")) {
        const id = entry.key.split("/").at(-1)!;
        if (!selectedIds.has(id)) return false;
        if (!value || (value.id !== undefined && value.id !== id) ||
            (value.rootId !== undefined && typeof value.rootId !== "string") ||
            (value.residency !== undefined && value.residency !== "session")) {
          throw new Error(`Cannot prove lineage ${root} dead: invalid actor state ownership ${entry.key}`);
        }
        return typeof value.rootId === "string" ? value.rootId === root :
          (session !== undefined && entry.key.startsWith(`actors/${session}/`));
      }
      if (!entry.key.startsWith("topology/participants/") || value?.rootId !== root) return false;
      if (value.kind === "actor") return typeof value.id === "string" && selectedIds.has(value.id);
      return value.kind === "root" ? preserved.size === 0 : value.residency === "session";
    };
    const entries = ownership.state.filter(belongs);
    const participantFiles = ownership.participants.filter(belongs);
    const presentFiles = [...files].filter(exists).sort();
    const result: ActorPruneResult = { root, dryRun, actors, files: presentFiles,
      stateKeys: [...new Set([...entries, ...participantFiles].map(entry => entry.key))].sort(),
      removed: { actors: 0, files: 0, stateKeys: 0 } };
    if (dryRun) { assertDead(); return result; }
    assertDead();
    if (!actors.length && !presentFiles.length && !result.stateKeys.length) return result;
    // Keep the existing append-only owner audit, not a replacement or deletion of history.
    await options.mesh.publish({ topic: "ops.owner", kind: "actor.prune", from: options.identity,
      data: { root, actors, files: presentFiles, stateKeys: result.stateKeys } });
    for (const { at, store } of stores) {
      if (!exists(at) || (!(selected.get(at)?.size) &&
          !presentFiles.some(file => filesByRoot.get(at)?.has(file)))) continue;
      await store.withLock(() => {
        assertDead();
        const rows = readRecords(at, store);
        proveUnowned(rows);
        const ids = selected.get(at) ?? new Set<string>();
        // Adoption is fenced by this exact registry lock. A racing adopter wins; never delete its files.
        if (rows.some(row => (ids.has(row.id) && !eligible(row)) || (eligible(row) && !ids.has(row.id)) ||
            (row.rootId === root && !eligible(row) && !preserved.has(row.id)))) {
          throw new Error(`Cannot prune lineage ${root}: actor ownership changed; retry the plan`);
        }
        // Files first: an I/O failure leaves the durable registry row so an explicit retry can finish.
        for (const file of presentFiles.filter(file => filesByRoot.get(at)?.has(file))) {
          if (!exists(file)) continue;
          if (/^removal-[a-f0-9]{32}\.json$/.test(path.basename(file))) {
            const owner = record(record(read(file))?.owner);
            if (owner?.rootId !== root || owner.residency !== "session") {
              throw new Error(`Cannot prune lineage ${root}: removal ownership changed`);
            }
          }
          fs.rmSync(file, { recursive: true, force: true }); result.removed.files++;
        }
        const removed = rows.filter(row => ids.has(row.id) && eligible(row));
        if (removed.length) store.write(rows.filter(row => !ids.has(row.id)), { durable: true });
        result.removed.actors += removed.length;
      });
    }
    for (const entry of participantFiles) {
      assertDead();
      if (await removeParticipantFileIf(options.mesh, entry.key,
        current => { assertDead(); return current.version === entry.version && belongs(current); })) result.removed.stateKeys++;
    }
    if (entries.length) {
      assertDead();
      const removed = await options.mesh.writeBatch({ identity: options.identity, ops: entries.map(entry => ({
        kind: "delete" as const, key: entry.key, ifVersion: entry.version, onConflict: "abort" as const,
        condition: () => { assertDead(); return true; },
      })) });
      result.removed.stateKeys += removed.filter(item => item.applied && !participantFiles.some(entry => entry.key === item.key)).length;
    }
    return result;
  });
};
