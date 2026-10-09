/**
 * Dead-root actor removal (smarty-dev#7817).
 *
 * `archiveActorForRemoval` is the archive both operator remove paths take before anything is
 * deleted or revoked: the format operators made by hand on 10-09 (a tar of the actor's directory
 * plus its registry row, in `actors-<root>-<stamp>/` with a `SHA256SUMS`).
 *
 * `removeActorOffline` removes a durable actor of a root whose resident host is proven dead,
 * under that root's host.lock fence with the retention sweep's fences (dead holders, a claimable
 * flock, no blocked waiter). It keeps the live path's order: archive, removal record (durable),
 * registry revocation under the registry lock, then cleanup, then the removal record goes.
 */
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { writeJsonAtomic } from "../core/atomic-write.js";
import { MeshStore } from "../mesh/store.js";
import { lockFile } from "../residency/file-lock.js";
import { assertResidentOperatorConfirmed, readResidentOperatorEvidence } from "../residency/operator-safety.js";
import { residentActorRoots, type ResidentHostConfig } from "../residency/protocol.js";
import { runTreeExitVeto } from "../storage/retention.js";
import { ownedStat, processAlive } from "../storage/scratch.js";
import { ActorBindingStore } from "./binding-store.js";
import { ActorRegistryStore } from "./registry-store.js";

type Row = Record<string, unknown> & { id: string };

const stamp = (at: number): string => new Date(at).toISOString().replace(/[-:]/g, "").replace(/\.\d+Z$/, "Z");
const rootTag = (rootId: string): string => rootId.replace(/^session:/, "").replace(/[^A-Za-z0-9_-]/g, "_").slice(0, 8);

/** Tar the actor directory and its registry row, with SHA256SUMS, before any removal step. */
export const archiveActorForRemoval = (archiveRoot: string, rootId: string, row: Row, actorDirectory: string, at = Date.now()): string => {
  if (!/^[A-Za-z0-9_-]+$/.test(row.id)) throw new Error(`Unsafe actor id for archive: ${row.id}`);
  const directory = path.join(archiveRoot, `actors-${rootTag(rootId)}-${stamp(at)}`);
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  const tar = path.join(directory, `${row.id}.tar`);
  if (fs.existsSync(tar)) throw new Error(`Actor archive already exists: ${tar}`);
  const files: string[] = [];
  if (fs.existsSync(actorDirectory)) {
    const result = spawnSync("tar", ["-cf", tar, "-C", path.dirname(actorDirectory), path.basename(actorDirectory)], { stdio: ["ignore", "ignore", "pipe"] });
    if (result.status !== 0) {
      fs.rmSync(tar, { force: true });
      throw new Error(`Actor archive failed (tar ${result.status ?? result.error}): ${String(result.stderr ?? "").trim()}`);
    }
    files.push(tar);
  }
  const rowFile = path.join(directory, `${row.id}.registry.json`);
  writeJsonAtomic(rowFile, row, { durable: true, space: 2 });
  files.push(rowFile);
  const sums = path.join(directory, "SHA256SUMS");
  const existing = fs.existsSync(sums) ? fs.readFileSync(sums, "utf8") : "";
  const lines = files.map(file => `${createHash("sha256").update(fs.readFileSync(file)).digest("hex")}  ${path.basename(file)}\n`).join("");
  fs.writeFileSync(sums, existing + lines, { mode: 0o600 });
  for (const file of [tar, sums]) {
    if (!fs.existsSync(file)) continue;
    const fd = fs.openSync(file, "r"); try { fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
  }
  return directory;
};

const readOwned = (file: string): Record<string, unknown> => {
  const stat = ownedStat(file);
  if (!stat?.isFile() || stat.size > 1024 * 1024) throw new Error(`Unsafe or unreadable custody record: ${file}`);
  return JSON.parse(fs.readFileSync(file, "utf8"));
};
const absent = (file: string): boolean => {
  try { fs.lstatSync(file); return false; }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return true; throw error; }
};
/** Same dead-holder rule as the retention sweep's claimFence: a recorded pid that no longer exists. */
const deadHolder = (file: string, optional = false): boolean => {
  if (optional && absent(file)) return true;
  const holder = readOwned(file);
  return typeof holder.pid === "number" && Number.isSafeInteger(holder.pid) && holder.pid > 0 && !processAlive(holder.pid);
};
/** A blocked flock request on this inode: a host is waking on this root (/proc/locks `->` lines).
 * Same approach as the retention sweep's lockWaiter. ponytail: matched by inode only; a collision on
 * another device refuses (fail safe). */
const lockWaiter = (fd: number): boolean => {
  const inode = fs.fstatSync(fd).ino;
  try {
    return fs.readFileSync("/proc/locks", "utf8").split("\n")
      .some(line => line.includes("->") && line.split(/\s+/).some(field => field.endsWith(`:${inode}`)));
  } catch { return true; } // unknown: treat as contended
};

/** Claim a dead resident's host.lock as the retention sweep does; throws unless the fence is ours. */
const claimFence = async (directory: string): Promise<number> => {
  const lock = path.join(directory, "host.lock");
  if (process.platform !== "linux") throw new Error("Offline removal needs Linux flock fences");
  if (!ownedStat(lock)?.isFile() || !deadHolder(lock) || !deadHolder(path.join(directory, "owner.json"), true)) {
    throw new Error("Root resident host is not proven dead: host.lock or owner.json has a live or unknown holder");
  }
  let fd: number;
  try { fd = await lockFile(lock, 0, true); }
  catch (error) { throw new Error(`Root resident host.lock is held: ${error instanceof Error ? error.message : String(error)}`); }
  try {
    if (lockWaiter(fd)) throw new Error("A host is waking on this root (host.lock has a waiter); it wins");
    if (!deadHolder(lock) || !deadHolder(path.join(directory, "owner.json"), true)) throw new Error("Root resident holder changed");
    return fd;
  } catch (error) { fs.closeSync(fd); throw error; }
};

export interface OfflineRemoveResult {
  offline: true;
  dryRun: boolean;
  actor: { id: string; name: unknown; status: unknown; registry: string };
  operatorEvidence: ReturnType<typeof readResidentOperatorEvidence>;
  archive?: string;
  cleaned?: boolean;
  pending?: string;
}

export const removeActorOffline = async (directory: string, config: ResidentHostConfig, selector: string,
  options: { dryRun?: boolean; confirmDeadRoot?: string } = {}): Promise<OfflineRemoveResult> => {
  const dryRun = options.dryRun === true;
  const fd = await claimFence(directory);
  let mesh: MeshStore | undefined;
  try {
    const recheck = (): void => {
      if (lockWaiter(fd)) throw new Error("A host is waking on this root (host.lock has a waiter); it wins");
    };
    const config2 = readOwned(path.join(directory, "config.json"));
    if (config2.format !== 1 || config2.rootId !== config.rootId || typeof config2.residencyRoot !== "string" ||
        path.resolve(config2.residencyRoot) !== path.resolve(directory) || config2.meshRoot !== config.meshRoot) {
      throw new Error("Resident root identity changed under the fence");
    }
    mesh = new MeshStore(config.meshRoot, config.mesh.maxEventBytes, config.mesh.maxReadEvents,
      { lockProtocol: config.mesh.lockProtocol, stateBackend: config.mesh.stateBackend });
    const evidence = readResidentOperatorEvidence(config, mesh);
    assertResidentOperatorConfirmed(evidence, options.confirmDeadRoot, dryRun);
    const check = (): void => {
      recheck();
      assertResidentOperatorConfirmed(readResidentOperatorEvidence(config, mesh!), options.confirmDeadRoot);
    };
    // Exact id/name within this root's durable actors only, as the live operator path.
    const matches: Array<{ store: ActorRegistryStore; root: string; row: Row }> = [];
    for (const root of new Set(Object.values(residentActorRoots(config)))) {
      if (absent(path.join(root, "actors.json"))) continue;
      const store = new ActorRegistryStore(root);
      for (const row of store.snapshot().actors) {
        if (row.rootId === config.rootId && row.residency === "durable" && (row.id === selector || row.name === selector)) {
          matches.push({ store, root, row: row as Row });
        }
      }
    }
    if (matches.length !== 1) throw new Error(matches.length ? `Ambiguous resident actor: ${selector}` : `Unknown Fabric actor: ${selector}`);
    const { store, root, row } = matches[0]!;
    const id = row.id;
    const actorDirectory = path.join(root, id);
    if (typeof row.sessionFile !== "string" || path.resolve(row.sessionFile) !== path.join(actorDirectory, "session.jsonl")) {
      throw new Error(`Actor ${id} session is outside its registry root`);
    }
    const summary = { id, name: row.name, status: row.status, registry: root };
    // A dead resident may leave a recorded run; remove only when its worker tree is proven exited.
    const inFlight = (row.inFlightRun as { id?: unknown } | undefined)?.id ?? (row.preparing as { runId?: unknown } | undefined)?.runId;
    if (row.preparing !== undefined && typeof inFlight !== "string") throw new Error(`Actor ${id} has an unfinished preparation; start its resident to drain it`);
    if (typeof inFlight === "string") {
      const run = path.join(actorDirectory, "runs", inFlight);
      const veto = !/^[A-Za-z0-9_-]+$/.test(inFlight) || absent(run) ? "run directory missing" : runTreeExitVeto(run, 0, undefined, true, true);
      if (veto) throw new Error(`Actor ${id} in-flight run ${inFlight} is not proven exited: ${veto}`);
    }
    if (dryRun) return { offline: true, dryRun, actor: summary, operatorEvidence: evidence };

    // 1. Archive first: nothing is deleted or revoked before it is taken.
    check();
    const archive = archiveActorForRemoval(path.join(directory, "archives"), config.rootId, row, actorDirectory);
    // 2. The durable removal record, as #commitRemove: a later owner start finishes from it.
    const presenceKey = `actors/${config.sessionId}/${id}`;
    const marker = path.join(root, `removal-${id}.json`);
    const cleanup = { id, sessionDir: actorDirectory, presenceKey,
      ...(typeof row.lastRunId === "string" ? { lastRunId: row.lastRunId } : {}),
      owner: { name: String(row.name ?? id), rootId: config.rootId, residency: "durable",
        requestedAt: (row.removal as { requestedAt?: number } | undefined)?.requestedAt ?? Date.now() } };
    check();
    writeJsonAtomic(marker, cleanup, { durable: true });
    // 3. Revoke the registry row under the registry lock.
    await store.withLock(() => {
      check();
      const current = store.snapshot().actors;
      if (!current.some(actor => actor.id === id)) return;
      store.write(current.filter(actor => actor.id !== id), { durable: true });
    });
    if (store.snapshot().actors.some(actor => actor.id === id)) throw new Error(`Fabric actor ${id}: registry revocation did not commit`);
    // 4. Cleanup, then the record goes. A failure keeps the record for the next owner start.
    try {
      await new ActorBindingStore(config.sessionId, root).delete(id);
      fs.rmSync(actorDirectory, { recursive: true, force: true });
      await mesh.delete({ key: presenceKey });
      fs.rmSync(marker, { force: true });
      return { offline: true, dryRun, actor: summary, operatorEvidence: evidence, archive, cleaned: true };
    } catch (error) {
      return { offline: true, dryRun, actor: summary, operatorEvidence: evidence, archive, cleaned: false,
        pending: `removal cleanup failed: ${error instanceof Error ? error.message : String(error)} (the next resident start finishes it)` };
    }
  } finally {
    mesh?.closeState();
    fs.closeSync(fd);
  }
};
