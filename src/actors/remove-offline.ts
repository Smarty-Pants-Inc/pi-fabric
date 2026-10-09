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
import os from "node:os";
import path from "node:path";
import { writeFileAtomic, writeJsonAtomic } from "../core/atomic-write.js";
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

/** One lstat-verified entry of an actor tree: never followed, compared by identity before removal. */
interface TreeEntry { path: string; dev: number; ino: number; directory: boolean }

/** The actor directory, walked with lstat and pinned by an O_NOFOLLOW directory descriptor. */
export interface PinnedActorTree { directory: string; entries: TreeEntry[]; fd?: number; close(): void }

/** Removal archives and deletes only what this OS user provably owns; without getuid it cannot. */
export const assertOwnershipProvable = (): void => {
  if (typeof process.getuid !== "function") {
    throw new Error("actor removal is not supported where file ownership cannot be proven (smarty-dev#7858)");
  }
};

const verifiedEntry = (file: string): TreeEntry => {
  assertOwnershipProvable();
  const uid = process.getuid!();
  const stat = fs.lstatSync(file);
  if (stat.isSymbolicLink()) throw new Error(`Actor tree has a symlink: ${file}`);
  if (!stat.isFile() && !stat.isDirectory()) throw new Error(`Actor tree has a special file: ${file}`);
  if (stat.uid !== uid) throw new Error(`Actor tree entry is not owned by this OS user: ${file}`);
  return { path: file, dev: stat.dev, ino: stat.ino, directory: stat.isDirectory() };
};
const sameEntry = (entry: TreeEntry, stat: fs.Stats): boolean =>
  stat.dev === entry.dev && stat.ino === entry.ino && stat.isDirectory() === entry.directory && !stat.isSymbolicLink();

/** Walk the actor directory (parents first) and pin it; refuses symlinks, special files and other owners. */
export const pinActorTree = (directory: string): PinnedActorTree => {
  assertOwnershipProvable();
  const tree: PinnedActorTree = { directory, entries: [], close() { if (this.fd !== undefined) { fs.closeSync(this.fd); delete this.fd; } } };
  try { fs.lstatSync(directory); }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return tree; throw error; }
  const root = verifiedEntry(directory);
  if (!root.directory) throw new Error(`Actor directory is not a directory: ${directory}`);
  // Windows has no O_DIRECTORY/O_NOFOLLOW; the lstat-then-fstat identity check below still refuses a swapped-in link.
  tree.fd = fs.openSync(directory, fs.constants.O_RDONLY | (fs.constants.O_DIRECTORY ?? 0) | (fs.constants.O_NOFOLLOW ?? 0));
  try {
    if (!sameEntry(root, fs.fstatSync(tree.fd))) throw new Error(`Actor directory changed while it was pinned: ${directory}`);
    const walk = (entry: TreeEntry, depth: number): void => {
      tree.entries.push(entry);
      if (!entry.directory) return;
      if (depth > 64) throw new Error(`Actor tree is too deep: ${entry.path}`);
      for (const name of fs.readdirSync(entry.path).sort()) walk(verifiedEntry(path.join(entry.path, name)), depth + 1);
    };
    walk(root, 0);
  } catch (error) { tree.close(); throw error; }
  return tree;
};

/** The pinned descriptor and path still name the walked actor directory. */
const assertPinned = (tree: PinnedActorTree): void => {
  const root = tree.entries[0];
  if (!root || tree.fd === undefined) return;
  if (!sameEntry(root, fs.fstatSync(tree.fd)) || !sameEntry(root, fs.lstatSync(tree.directory))) {
    throw new Error(`Actor directory was replaced: ${tree.directory}`);
  }
};

/** Remove only the walked entries, children first, each re-verified by identity; any mismatch stops. */
export const removePinnedActorTree = (tree: PinnedActorTree): void => {
  assertPinned(tree);
  for (const entry of [...tree.entries].reverse()) {
    let stat: fs.Stats;
    try { stat = fs.lstatSync(entry.path); }
    catch (error) { throw new Error(`Actor tree entry vanished before removal: ${entry.path} (${(error as Error).message})`); }
    if (!sameEntry(entry, stat)) throw new Error(`Actor tree entry changed before removal: ${entry.path}`);
    if (entry.directory) fs.rmdirSync(entry.path); else fs.unlinkSync(entry.path);
  }
};

/** Tar the verified actor tree and its registry row, with SHA256SUMS, before any removal step. */
/** Evidence text kept with a --main-stopped assertion is capped at 64 KiB. */
export const MAIN_STOPPED_EVIDENCE_MAX_BYTES = 64 * 1024;

/** The audit record of a --main-stopped assertion: who, which root, when, and the evidence used. */
export interface MainStoppedAudit {
  format: 1;
  mainStopped: true;
  rootId: string;
  assertedAt: string;
  operator: { user: string | null; agent: string | null; session: string | null; pid: number; host: string };
  evidence: string;
}

/** Built by the operator's own CLI process from its environment and the required evidence. */
export const mainStoppedAudit = (rootId: string, evidence: string, env: NodeJS.ProcessEnv = process.env): MainStoppedAudit => {
  const text = Buffer.from(evidence, "utf8").subarray(0, MAIN_STOPPED_EVIDENCE_MAX_BYTES).toString("utf8");
  if (!text.trim()) throw new Error("--main-stopped needs --evidence <text> or --evidence-file <path>: the Herdr pane/agent listing and the process check for that Main's session");
  return { format: 1, mainStopped: true, rootId, assertedAt: new Date().toISOString(),
    operator: { user: env.USER ?? env.LOGNAME ?? null, agent: env.PI_FABRIC_AGENT_NAME ?? null,
      session: env.PI_FABRIC_SESSION_ID ?? env.PI_SESSION_ID ?? null, pid: process.pid, host: os.hostname() },
    evidence: text };
};

/** Exact audit shape for this root, with non-empty evidence within the cap. */
export const validMainStoppedAudit = (value: unknown, rootId: string): value is MainStoppedAudit => {
  const audit = value as MainStoppedAudit | undefined;
  const nullableString = (field: unknown): boolean => field === null || typeof field === "string";
  return !!audit && typeof audit === "object" && audit.format === 1 && audit.mainStopped === true && audit.rootId === rootId &&
    typeof audit.assertedAt === "string" && Number.isFinite(Date.parse(audit.assertedAt)) &&
    !!audit.operator && typeof audit.operator === "object" && nullableString(audit.operator.user) &&
    nullableString(audit.operator.agent) && nullableString(audit.operator.session) &&
    Number.isSafeInteger(audit.operator.pid) && typeof audit.operator.host === "string" &&
    typeof audit.evidence === "string" && audit.evidence.trim().length > 0 &&
    Buffer.byteLength(audit.evidence, "utf8") <= MAIN_STOPPED_EVIDENCE_MAX_BYTES;
};

export const MAIN_STOPPED_AUDIT_REQUIRED = "--main-stopped needs --evidence <text> or --evidence-file <path>: the Herdr pane/agent listing and the process check for that Main's session";

export const archiveActorForRemoval = (archiveRoot: string, rootId: string, row: Row, tree: PinnedActorTree, at = Date.now(),
  assertion?: MainStoppedAudit & { requestId?: string }): string => {
  if (!/^[A-Za-z0-9_-]+$/.test(row.id)) throw new Error(`Unsafe actor id for archive: ${row.id}`);
  assertPinned(tree);
  const directory = path.join(archiveRoot, `actors-${rootTag(rootId)}-${stamp(at)}`);
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  const tar = path.join(directory, `${row.id}.tar`);
  if (fs.existsSync(tar)) throw new Error(`Actor archive already exists: ${tar}`);
  const files: string[] = [];
  if (tree.entries.length) {
    // Exactly the verified list: no recursion and no -h, so tar follows nothing.
    const parent = path.dirname(tree.directory);
    const list = tree.entries.map(entry => path.relative(parent, entry.path) + "\0").join("");
    const result = spawnSync("tar", ["-cf", tar, "-C", parent, "--no-recursion", "--null", "-T", "-"],
      { input: list, stdio: ["pipe", "ignore", "pipe"] });
    if (result.status !== 0) {
      fs.rmSync(tar, { force: true });
      throw new Error(`Actor archive failed (tar ${result.status ?? result.error}): ${String(result.stderr ?? "").trim()}`);
    }
    files.push(tar);
  }
  const rowFile = path.join(directory, `${row.id}.registry.json`);
  writeJsonAtomic(rowFile, row, { durable: true, space: 2 });
  files.push(rowFile);
  if (assertion) {
    const assertionFile = path.join(directory, `${row.id}.operator.json`);
    writeJsonAtomic(assertionFile, assertion, { durable: true, space: 2 });
    files.push(assertionFile);
  }
  const sums = path.join(directory, "SHA256SUMS");
  const existing = fs.existsSync(sums) ? fs.readFileSync(sums, "utf8") : "";
  const lines = files.map(file => `${createHash("sha256").update(fs.readFileSync(file)).digest("hex")}  ${path.basename(file)}\n`).join("");
  // Windows refuses fsync on a read-only handle (EPERM): sync the tar through a writable one.
  if (files.includes(tar)) { const fd = fs.openSync(tar, "r+"); try { fs.fsyncSync(fd); } finally { fs.closeSync(fd); } }
  writeFileAtomic(sums, existing + lines, { durable: true });
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

/** Test seam only: runs after each completed removal step, before the next liveness check. */
export const offlineRemovalHooks: { afterStep?: (step: "revoke" | "bindings" | "tree") => void | Promise<void> } = {};

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
  options: { dryRun?: boolean; confirmDeadRoot?: string; mainStoppedAudit?: MainStoppedAudit } = {}): Promise<OfflineRemoveResult> => {
  const dryRun = options.dryRun === true;
  assertOwnershipProvable(); // before the fence, any record or any mutation
  const fd = await claimFence(directory);
  let mesh: MeshStore | undefined;
  let tree: PinnedActorTree | undefined;
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
    // The same operator assertion and live-observation vetoes as the live path (smarty-dev#7956: automatic proof).
    if (options.mainStoppedAudit !== undefined && !validMainStoppedAudit(options.mainStoppedAudit, config.rootId)) {
      throw new Error(MAIN_STOPPED_AUDIT_REQUIRED);
    }
    const mainStopped = options.mainStoppedAudit !== undefined;
    const evidence = readResidentOperatorEvidence(config, mesh, undefined, { mainStopped });
    assertResidentOperatorConfirmed(evidence, options.confirmDeadRoot, dryRun, mainStopped);
    const check = (): void => {
      recheck();
      assertResidentOperatorConfirmed(readResidentOperatorEvidence(config, mesh!, undefined, { mainStopped }), options.confirmDeadRoot, false, mainStopped);
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
    // Walk and pin the actor tree first: a symlink, special file or foreign entry refuses before any archive.
    tree = pinActorTree(actorDirectory);
    if (dryRun) return { offline: true, dryRun, actor: summary, operatorEvidence: evidence };

    const assertion = options.mainStoppedAudit!;
    // 1. Archive first: nothing is deleted or revoked before it is taken.
    check();
    const archive = archiveActorForRemoval(path.join(directory, "archives"), config.rootId, row, tree, Date.now(), assertion);
    // 2. The durable removal record, as #commitRemove: a later owner start finishes from it.
    const presenceKey = `actors/${config.sessionId}/${id}`;
    const marker = path.join(root, `removal-${id}.json`);
    const cleanup = { id, sessionDir: actorDirectory, presenceKey,
      ...(typeof row.lastRunId === "string" ? { lastRunId: row.lastRunId } : {}),
      owner: { name: String(row.name ?? id), rootId: config.rootId, residency: "durable",
        requestedAt: (row.removal as { requestedAt?: number } | undefined)?.requestedAt ?? Date.now() },
      operatorAssertion: assertion };
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
    // The full liveness check (lease and root participant) runs again, under host.lock, right before
    // each destructive step. ponytail: a Main publishes under the mesh state and participant-file locks,
    // which this offline process does not hold across a tree removal; the remaining window is one step,
    // and it is safe because a Main appearing there stops the next step and the removal record stays.
    try {
      await offlineRemovalHooks.afterStep?.("revoke");
      check();
      await new ActorBindingStore(config.sessionId, root).delete(id);
      await offlineRemovalHooks.afterStep?.("bindings");
      check();
      removePinnedActorTree(tree);
      await offlineRemovalHooks.afterStep?.("tree");
      check();
      await mesh.delete({ key: presenceKey });
      fs.rmSync(marker, { force: true });
      return { offline: true, dryRun, actor: summary, operatorEvidence: evidence, archive, cleaned: true };
    } catch (error) {
      return { offline: true, dryRun, actor: summary, operatorEvidence: evidence, archive, cleaned: false,
        pending: `removal cleanup failed: ${error instanceof Error ? error.message : String(error)} (the next resident start finishes it)` };
    }
  } finally {
    tree?.close();
    mesh?.closeState();
    fs.closeSync(fd);
  }
};
