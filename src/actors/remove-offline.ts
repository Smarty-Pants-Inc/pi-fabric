/**
 * Offline dead-root actor removal (smarty-dev#7817).
 *
 * `removeActorOffline` removes a durable actor of a root whose resident host is proven dead, under that
 * root's host.lock fence with the retention sweep's fences (dead holders, a claimable flock, no blocked
 * waiter). Order: archive the registry row and audit record into a fresh archive directory, the durable
 * removal record, registry revocation under the registry lock, then the actor directory is MOVED (one
 * rename(2), never followed, never a recursive delete) into that archive directory, then the record goes.
 * Deleting archives is left to retention (smarty-dev#7916).
 */
import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { writeFileAtomic, writeJsonAtomic } from "../core/atomic-write.js";
import { MeshStore } from "../mesh/store.js";
import { lockFile } from "../residency/file-lock.js";
import { ownProcessIncarnation } from "../core/atomic-write.js";
import { withStateFence } from "../mesh/commit-outbox.js";
import { acquireMainPublicationFence, deleteRootActorTree } from "../topology/main-publication-fence.js";
import { withParticipantFileTryLock } from "../topology/participant-files.js";
import { assertResidentOperatorConfirmed, readResidentOperatorEvidence, type MainToolEvidence } from "../residency/operator-safety.js";
import { residentActorRoots, residentHostId, residentRoot, type ResidentHostConfig } from "../residency/protocol.js";
import { runTreeExitVeto } from "../storage/retention.js";
import { ownedStat, processAlive } from "../storage/scratch.js";
import { ActorBindingStore } from "./binding-store.js";
import { ActorRegistryStore } from "./registry-store.js";

type Row = Record<string, unknown> & { id: string };

const stamp = (at: number): string => new Date(at).toISOString().replace(/[-:]/g, "").replace(/\.\d+Z$/, "Z");

/** Removal moves only what this OS user provably owns; without getuid it cannot. */
export const assertOwnershipProvable = (): void => {
  if (typeof process.getuid !== "function") {
    throw new Error("actor removal is not supported where file ownership cannot be proven (smarty-dev#7858)");
  }
};

/** The actor directory itself, pinned by identity: a real directory (not a link) owned by this user. */
interface PinnedActorRoot { dev: number; ino: number }
const pinActorRoot = (directory: string): PinnedActorRoot | undefined => {
  assertOwnershipProvable();
  let stat: fs.Stats;
  try { stat = fs.lstatSync(directory); }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined; throw error; }
  if (stat.isSymbolicLink() || !stat.isDirectory()) throw new Error(`Actor directory is not a directory: ${directory}`);
  if (stat.uid !== process.getuid!()) throw new Error(`Actor directory is not owned by this OS user: ${directory}`);
  const fd = fs.openSync(directory, fs.constants.O_RDONLY | (fs.constants.O_DIRECTORY ?? 0) | (fs.constants.O_NOFOLLOW ?? 0));
  try {
    const held = fs.fstatSync(fd);
    if (held.dev !== stat.dev || held.ino !== stat.ino) throw new Error(`Actor directory changed while it was pinned: ${directory}`);
  } finally { fs.closeSync(fd); }
  return { dev: stat.dev, ino: stat.ino };
};

const ARCHIVE_EXDEV = "archive dir is on another filesystem or mount (EXDEV); refusing before any change (smarty-dev#8159)";
const ARCHIVE_CHANGED = "archive path changed during removal; refusing (smarty-dev#8159)";

/** The identity (st_dev, inode) of each archive path component, pinned at the first check. */
interface ChainLink { path: string; dev: number; ino: number; follow: boolean }
type ArchiveChain = ChainLink[];
const linkOf = (file: string, stat: fs.Stats, follow = false): ChainLink => ({ path: file, dev: stat.dev, ino: stat.ino, follow });

/** Re-verify the whole pinned chain: same identity, still a real directory (no link). Synchronous. */
const reverifyChain = (chain: ArchiveChain): void => {
  for (const link of chain) {
    let stat: fs.Stats;
    try { stat = link.follow ? fs.statSync(link.path) : fs.lstatSync(link.path); }
    catch { throw new Error(`${ARCHIVE_CHANGED}: ${link.path}`); }
    if (stat.dev !== link.dev || stat.ino !== link.ino || !stat.isDirectory() || (!link.follow && stat.isSymbolicLink())) {
      throw new Error(`${ARCHIVE_CHANGED}: ${link.path}`);
    }
  }
};

/** A real directory (lstat: not a link) owned by this OS user, or undefined when absent. */
const ownedDirectory = (file: string): fs.Stats | undefined => {
  let stat: fs.Stats;
  try { stat = fs.lstatSync(file); }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined; throw error; }
  if (stat.isSymbolicLink() || !stat.isDirectory() || stat.uid !== process.getuid!()) {
    throw new Error(`Archive path component is not a real directory owned by this OS user: ${file}; refusing before any change (smarty-dev#8159)`);
  }
  return stat;
};

/**
 * The archive root, verified before ANY state change (smarty-dev#8159): every component from the mesh root
 * down to archives is a real directory owned by this user (lstat, no link), archives resolves to exactly
 * <real residency root>/archives, and it is on the pinned actor directory's filesystem. A missing archives
 * is created by a plain, non-recursive mkdir (only when `create`), then verified again.
 */
const verifyArchiveRoot = (meshRoot: string, residencyRoot: string, realResidencyRoot: string, pin: PinnedActorRoot | undefined,
  create: boolean, chain: ArchiveChain = []): string => {
  const residency = path.join(path.resolve(meshRoot), "residency");
  // The mesh root itself may be reached through a configured link; it is pinned by its target's identity.
  chain.push(linkOf(path.resolve(meshRoot), fs.statSync(meshRoot), true));
  if (path.dirname(path.resolve(residencyRoot)) !== residency) throw new Error(`Resident root is outside the mesh residency directory: ${residencyRoot}`);
  for (const component of [residency, path.resolve(residencyRoot)]) {
    const stat = ownedDirectory(component);
    if (!stat) throw new Error(`Archive path component is missing: ${component}; refusing before any change (smarty-dev#8159)`);
    chain.push(linkOf(component, stat));
  }
  if (fs.realpathSync(residencyRoot) !== realResidencyRoot) throw new Error(`Resident root changed: ${residencyRoot}; refusing before any change (smarty-dev#8159)`);
  // Before any mkdir: archives is made inside the residency root, so that must be on the actor's filesystem.
  if (pin && fs.lstatSync(path.resolve(residencyRoot)).dev !== pin.dev) throw new Error(ARCHIVE_EXDEV);
  const archiveRoot = path.join(path.resolve(residencyRoot), "archives");
  let stat = ownedDirectory(archiveRoot);
  if (!stat && create) {
    fs.mkdirSync(archiveRoot, { mode: 0o700 }); // plain mkdir: the parent is verified above
    stat = ownedDirectory(archiveRoot);
  }
  const placed = stat ?? fs.lstatSync(path.resolve(residencyRoot)); // a dry run: where archives would be made
  if (stat && fs.realpathSync(archiveRoot) !== path.join(realResidencyRoot, "archives")) {
    throw new Error(`Archive dir resolves elsewhere: ${archiveRoot}; refusing before any change (smarty-dev#8159)`);
  }
  if (pin && placed.dev !== pin.dev) throw new Error(ARCHIVE_EXDEV);
  if (stat) chain.push(linkOf(archiveRoot, stat));
  return archiveRoot;
};

/** A fresh, exclusive archive directory: a plain mkdir fails if the name exists; verified after. */
const createArchiveDirectory = (archiveRoot: string, id: string, dev: number, chain: ArchiveChain, at = Date.now()): string => {
  reverifyChain(chain);
  const directory = path.join(archiveRoot, `${id}.${stamp(at)}.${randomUUID()}`);
  try { fs.mkdirSync(directory, { mode: 0o700 }); }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code === "EEXIST") throw new Error(`Archive target already exists: ${directory}; refusing (an archive is never overwritten or merged)`);
    throw error;
  }
  const stat = ownedDirectory(directory);
  if (!stat || stat.dev !== dev) throw new Error(`${ARCHIVE_EXDEV}: ${directory}`);
  chain.push(linkOf(directory, stat));
  return directory;
};

/** Registry row and audit record, with SHA256SUMS, written into the archive before any removal step. */
const writeArchiveRecords = (archive: string, row: Row, assertion: MainStoppedAudit): void => {
  const files: string[] = [];
  for (const [name, value] of [[`${row.id}.registry.json`, row], [`${row.id}.operator.json`, assertion]] as const) {
    const file = path.join(archive, name);
    writeJsonAtomic(file, value, { durable: true, space: 2 });
    files.push(file);
  }
  const lines = files.map(file => `${createHash("sha256").update(fs.readFileSync(file)).digest("hex")}  ${path.basename(file)}\n`).join("");
  writeFileAtomic(path.join(archive, "SHA256SUMS"), lines, { durable: true });
};

/** Move the pinned actor directory into the archive by one rename(2) to a child name of the fresh archive
 * directory. rename never follows the source: a swapped-in link moves as a link, and the post-check refuses
 * (moving it back). EXDEV refuses; there is no copy fallback. */
// ponytail: the whole chain is re-verified synchronously right before each rename; the residual is the few
// syscalls between that check and rename(2), open only to this same OS user (smarty-dev#7800 carries a
// directory-fd based rename that closes it).
const moveActorTreeToArchive = (actorDirectory: string, pin: PinnedActorRoot, archive: string, chain: ArchiveChain, name = "tree"): string => {
  const target = path.join(archive, name);
  reverifyChain(chain);
  try { fs.lstatSync(target); throw new Error(`Archive target already exists: ${target}`); }
  catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
  try { fs.renameSync(actorDirectory, target); }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code === "EXDEV") {
      throw new Error(`Actor directory and archive root are on different filesystems (EXDEV); refusing, no copy: ${actorDirectory}`);
    }
    throw error;
  }
  const moved = fs.lstatSync(target);
  if (moved.isSymbolicLink() || !moved.isDirectory() || moved.dev !== pin.dev || moved.ino !== pin.ino || moved.uid !== process.getuid!()) {
    let restored = false;
    try { fs.lstatSync(actorDirectory); }
    catch {
      try { reverifyChain(chain); fs.renameSync(target, actorDirectory); restored = true; } catch { /* reported below */ }
    }
    throw new Error(`Actor directory was replaced before the archive rename (moved entry is not the pinned directory); ` +
      `${restored ? "moved back" : `left at ${target}`}; refusing`);
  }
  return target;
};

/** Evidence text kept with a --main-stopped assertion is capped at 64 KiB. */
export const MAIN_STOPPED_EVIDENCE_MAX_BYTES = 64 * 1024;

/** The audit record of a --main-stopped assertion: who, which root, when, and the operator's attestation.
 * --main-stopped is an OPERATOR ATTESTATION, not a machine proof (automatic proof: smarty-dev#7956); the
 * tool adds its own observation (toolEvidence) at removal time. */
export interface MainStoppedAudit {
  format: 1;
  mainStopped: true;
  rootId: string;
  assertedAt: string;
  operator: { user: string | null; agent: string | null; session: string | null; pid: number; host: string };
  operatorAttestation: string;
  /** Added by the remover at removal time: what the tool itself observed. */
  toolEvidence?: MainToolEvidence;
}

/** Built by the operator's own CLI process from its environment and the required evidence. */
export const mainStoppedAudit = (rootId: string, evidence: string, env: NodeJS.ProcessEnv = process.env): MainStoppedAudit => {
  const text = Buffer.from(evidence, "utf8").subarray(0, MAIN_STOPPED_EVIDENCE_MAX_BYTES).toString("utf8");
  if (!text.trim()) throw new Error("--main-stopped needs --evidence <text> or --evidence-file <path>: the Herdr pane/agent listing and the process check for that Main's session");
  return { format: 1, mainStopped: true, rootId, assertedAt: new Date().toISOString(),
    operator: { user: env.USER ?? env.LOGNAME ?? null, agent: env.PI_FABRIC_AGENT_NAME ?? null,
      session: env.PI_FABRIC_SESSION_ID ?? env.PI_SESSION_ID ?? null, pid: process.pid, host: os.hostname() },
    operatorAttestation: text };
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
    typeof audit.operatorAttestation === "string" && audit.operatorAttestation.trim().length > 0 &&
    Buffer.byteLength(audit.operatorAttestation, "utf8") <= MAIN_STOPPED_EVIDENCE_MAX_BYTES;
};

export const MAIN_STOPPED_AUDIT_REQUIRED = "--main-stopped needs --evidence <text> or --evidence-file <path>: the Herdr pane/agent listing and the process check for that Main's session";

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

/** The operator's final liveness check, inside the state transaction and under the root participant's
 * key lock: a Main publication either precedes it (and it refuses) or is refused by the fence. */
export const fencedOperatorCheck = async (mesh: MeshStore, identity: { id: string; name: string; kind: "agent" },
  rootId: string, assert: (get: (key: string) => ReturnType<MeshStore["get"]>) => void): Promise<void> => {
  await offlineRemovalHooks.beforeFinalCheck?.();
  const incarnation = await ownProcessIncarnation();
  const rootKey = "topology/participants/" + createHash("sha256").update(rootId).digest("hex");
  await withStateFence(mesh, identity, view => withParticipantFileTryLock(mesh, rootKey, incarnation, () =>
    assert(key => view.get(key))), 10_000);
};

/** Test seam only: runs after each completed removal step, before the next liveness check. */
export const offlineRemovalHooks: {
  afterStep?: (step: "marker" | "revoke" | "bindings" | "tree") => void | Promise<void>;
  /** After the first chain check and the final liveness check, right before the tree move. */
  beforeTreeMove?: () => void | Promise<void>;
  /** Before every fenced final check, offline and live. */
  beforeFinalCheck?: () => void | Promise<void>;
} = {};

export interface OfflineRemoveResult {
  offline: true;
  dryRun: boolean;
  actor: { id: string; name: unknown; status: unknown; registry: string };
  operatorEvidence: ReturnType<typeof readResidentOperatorEvidence>;
  archive?: string;
  /** Dry run: where the archive would go and the audit record it would keep. */
  plan?: { archiveRoot: string; archive: string; audit: MainStoppedAudit };
  cleaned?: boolean;
  pending?: string;
}

/**
 * The offline removal's own pending record: a distinct name (`offline-removal-<id>.json`), so the
 * ActorManager's generic cleanup (which deletes the session directory of a `removal-<id>.json`) never
 * touches it. Written durably BEFORE the tree rename, with the archive destination and the pinned
 * identities, so the next offline remove of that actor resumes the ONE checked rename; nothing ever
 * deletes the source tree (smarty-dev#8159).
 */
interface OfflineRemovalMarker {
  format: 1; kind: "offline-removal"; id: string; rootId: string; sessionDir: string; archive: string;
  pin: PinnedActorRoot | null; chain: ArchiveChain; presenceKey: string; operatorAssertion: MainStoppedAudit;
}
export const offlineMarkerPath = (actorRoot: string, id: string): string => path.join(actorRoot, `offline-removal-${id}.json`);
const readOfflineMarker = (actorRoot: string, id: string, rootId: string): OfflineRemovalMarker | undefined => {
  if (!/^[A-Za-z0-9_-]+$/.test(id)) return undefined;
  const file = offlineMarkerPath(actorRoot, id);
  if (absent(file)) return undefined;
  const marker = readOwned(file) as unknown as OfflineRemovalMarker;
  if (marker.format !== 1 || marker.kind !== "offline-removal" || marker.id !== id || marker.rootId !== rootId ||
      marker.sessionDir !== path.join(actorRoot, id) || typeof marker.archive !== "string" || !Array.isArray(marker.chain) ||
      path.dirname(marker.archive) !== marker.chain.at(-2)?.path || marker.chain.at(-1)?.path !== marker.archive) {
    throw new Error(`Unreadable offline removal record: ${file}; refusing`);
  }
  return marker;
};

export const removeActorOffline = async (directory: string, config: ResidentHostConfig, selector: string,
  options: { dryRun?: boolean; confirmDeadRoot?: string; mainStoppedAudit?: MainStoppedAudit } = {}): Promise<OfflineRemoveResult> => {
  const dryRun = options.dryRun === true;
  assertOwnershipProvable(); // before the fence, any record or any mutation
  // The root's existing resident startup claim, held for the whole removal, in the host's own order
  // (startup claim, then host.lock): no resident host can start on this root meanwhile; it gets
  // ResidentHostAlreadyRunning. A busy claim (a host starting now) refuses. Same lock path and helper.
  let startupClaim: number;
  try { startupClaim = await lockFile(path.join(directory, "host-fence-establish.lock"), 0, true); }
  catch (error) {
    throw new Error(`Root resident startup claim is busy (a resident host is starting): ${error instanceof Error ? error.message : String(error)}`);
  }
  let fd: number;
  try { fd = await claimFence(directory); }
  catch (error) { fs.closeSync(startupClaim); throw error; }
  let mesh: MeshStore | undefined;
  let releaseFence: (() => void) | undefined;
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
    // Input errors first: resolve the actor before any liveness or identity check.
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
    // An interrupted offline removal whose registry row is already revoked: resume from its record.
    let resumed: { root: string; marker: OfflineRemovalMarker } | undefined;
    if (matches.length === 0) {
      for (const root of new Set(Object.values(residentActorRoots(config)))) {
        const marker = readOfflineMarker(root, selector, config.rootId);
        if (marker) resumed = { root, marker };
      }
    }
    if (matches.length !== 1 && !resumed) throw new Error(matches.length ? `Ambiguous resident actor: ${selector}` : `Unknown Fabric actor: ${selector}`);
    // The same operator assertion and live-observation vetoes as the live path (smarty-dev#7956: automatic proof).
    if (options.mainStoppedAudit !== undefined && !validMainStoppedAudit(options.mainStoppedAudit, config.rootId)) {
      throw new Error(MAIN_STOPPED_AUDIT_REQUIRED);
    }
    const mainStopped = options.mainStoppedAudit !== undefined;
    const evidence = readResidentOperatorEvidence(config, mesh, undefined, { mainStopped });
    // A dry run runs every check a real removal runs (smarty-dev#7817); it reports "would refuse".
    const wouldRefuse = (error: unknown): never => {
      throw new Error(`would refuse: ${error instanceof Error ? error.message : String(error)}`);
    };
    try { assertResidentOperatorConfirmed(evidence, options.confirmDeadRoot, false, mainStopped); }
    catch (error) { if (dryRun) wouldRefuse(error); throw error; }
    const incarnation = await ownProcessIncarnation();
    const identity = { id: residentHostId(config.rootId), name: "fabric-actors remove", kind: "agent" as const };
    const rootKey = "topology/participants/" + createHash("sha256").update(config.rootId).digest("hex");
    const check = async (): Promise<void> => {
      recheck();
      await fencedOperatorCheck(mesh!, identity, config.rootId, get =>
        assertResidentOperatorConfirmed(readResidentOperatorEvidence(config, { get }, undefined, { mainStopped }),
          options.confirmDeadRoot, false, mainStopped));
    };
    const root = matches[0]?.root ?? resumed!.root;
    const row = matches[0]?.row;
    const store = matches[0]?.store ?? new ActorRegistryStore(root);
    const id = row?.id ?? resumed!.marker.id;
    const actorDirectory = path.join(root, id);
    if (row && (typeof row.sessionFile !== "string" || path.resolve(row.sessionFile) !== path.join(actorDirectory, "session.jsonl"))) {
      throw new Error(`Actor ${id} session is outside its registry root`);
    }
    const summary = { id, name: row?.name ?? id, status: row?.status ?? "removing", registry: root };
    if (row) {
      // A dead resident may leave a recorded run; remove only when its worker tree is proven exited.
      const inFlight = (row.inFlightRun as { id?: unknown } | undefined)?.id ?? (row.preparing as { runId?: unknown } | undefined)?.runId;
      if (row.preparing !== undefined && typeof inFlight !== "string") throw new Error(`Actor ${id} has an unfinished preparation; start its resident to drain it`);
      if (typeof inFlight === "string") {
        const run = path.join(actorDirectory, "runs", inFlight);
        const veto = !/^[A-Za-z0-9_-]+$/.test(inFlight) || absent(run) ? "run directory missing" : runTreeExitVeto(run, 0, undefined, true, true);
        if (veto) throw new Error(`Actor ${id} in-flight run ${inFlight} is not proven exited: ${veto}`);
      }
    }
    const markerPath = offlineMarkerPath(root, id);
    let marker = resumed?.marker ?? readOfflineMarker(root, id, config.rootId);
    let pin: PinnedActorRoot | undefined;
    let recreated: PinnedActorRoot | undefined;
    let chain: ArchiveChain;
    let archiveRoot: string;
    if (marker) {
      // Resume: the recorded destination and the identities pinned by the interrupted run; nothing is re-made.
      try {
        chain = marker.chain;
        reverifyChain(chain);
        pin = marker.pin ?? undefined;
        const current = pinActorRoot(actorDirectory);
        let moved: fs.Stats | undefined;
        try { moved = fs.lstatSync(path.join(marker.archive, "tree")); } catch { /* not moved yet */ }
        const archived = !!pin && !!moved && !moved.isSymbolicLink() && moved.dev === pin.dev && moved.ino === pin.ino;
        if (moved && !archived) throw new Error(`Archived tree is not the pinned directory; refusing`);
        if (current && (!pin || current.dev !== pin.dev || current.ino !== pin.ino)) {
          // The pinned tree is already archived and something (a resident start) made a new directory at the
          // actor path: it is moved into the same archive too (checked the same way), never deleted.
          if (!archived) throw new Error(`Actor directory changed since the interrupted removal: ${actorDirectory}; refusing`);
          if (current.dev !== moved!.dev) throw new Error(ARCHIVE_EXDEV);
          recreated = current;
        }
        if (!current && pin && !archived) throw new Error(`Actor directory is gone and not in the recorded archive: ${actorDirectory}; refusing`);
        archiveRoot = path.dirname(marker.archive);
      } catch (error) { if (dryRun) wouldRefuse(error); throw error; }
    } else {
      // Pin the actor directory (a real directory owned by this user) and verify (and, for a real run,
      // create) the archive root before any state change (smarty-dev#8159).
      pin = pinActorRoot(actorDirectory);
      chain = [];
      archiveRoot = verifyArchiveRoot(config.meshRoot, residentRoot(config.meshRoot, config.rootId), fs.realpathSync(directory), pin, !dryRun, chain);
    }
    if (dryRun) return { offline: true, dryRun, actor: summary, operatorEvidence: evidence,
      plan: { archiveRoot, archive: marker?.archive ?? path.join(archiveRoot, `${id}.<time>.<random>`),
        audit: { ...options.mainStoppedAudit!, toolEvidence: evidence.toolEvidence! } } };

    // The root Main publication fence (smarty-dev#7817), after every pre-change check and before the first
    // check() below; released in finally. A Main refuses to publish its root participant while it stands,
    // in the same atomic step as the write; each check runs under that root's participant key lock inside
    // the state transaction, so a Main publication either precedes the check (and refuses) or is refused.
    releaseFence = await acquireMainPublicationFence(config.meshRoot, config.rootId);
    if (!marker) {
      // The tool's own observation at removal time, kept beside the operator's attestation.
      const assertion: MainStoppedAudit = { ...options.mainStoppedAudit!,
        toolEvidence: readResidentOperatorEvidence(config, mesh, undefined, { mainStopped }).toolEvidence! };
      // 1. Archive first: the registry row and audit record into a fresh, exclusive archive directory.
      await check();
      const archive = createArchiveDirectory(archiveRoot, id, pin?.dev ?? fs.lstatSync(archiveRoot).dev, chain);
      writeArchiveRecords(archive, row!, assertion);
      // 2. The pending record, durable BEFORE the rename: the destination and the pinned identities, so a
      // crash anywhere after it resumes the one checked rename and never loses or deletes the tree.
      marker = { format: 1, kind: "offline-removal", id, rootId: config.rootId, sessionDir: actorDirectory, archive,
        pin: pin ?? null, chain, presenceKey: `actors/${config.sessionId}/${id}`, operatorAssertion: assertion };
      await check();
      writeJsonAtomic(markerPath, marker, { durable: true });
    }
    const pending = marker;
    await offlineRemovalHooks.afterStep?.("marker");
    // 3. Move the actor directory into the archive, BEFORE the registry and binding changes, so a refused
    // move (a changed archive path, a replaced actor directory, EXDEV) leaves them untouched: nothing to roll
    // back. Under the Main publication fence, after the final check.
    if (recreated) {
      await deleteRootActorTree(config.meshRoot, config.rootId, () => {
        moveActorTreeToArchive(actorDirectory, recreated!, pending.archive, chain, `tree.recreated.${randomUUID()}`);
      }, check);
    } else if (pin && !absent(actorDirectory)) {
      try {
        await deleteRootActorTree(config.meshRoot, config.rootId, async () => {
          await offlineRemovalHooks.beforeTreeMove?.();
          moveActorTreeToArchive(actorDirectory, pin!, pending.archive, chain);
        }, check);
      } catch (error) {
        // A refused move rolls the pending record back (the registry and bindings were never touched), unless
        // the tree did reach the archive and could not be moved back: then the record stays to resume from.
        if (absent(path.join(pending.archive, "tree"))) fs.rmSync(markerPath, { force: true });
        throw error;
      }
    }
    await offlineRemovalHooks.afterStep?.("tree");
    // 4. Revoke the registry row under the registry lock, then the bindings, presence and the record. The
    // full liveness check runs again before each step; a failure keeps the record for a rerun.
    try {
      await store.withLock(async () => {
        await check();
        const current = store.snapshot().actors;
        if (!current.some(actor => actor.id === id)) return;
        store.write(current.filter(actor => actor.id !== id), { durable: true });
      });
      if (store.snapshot().actors.some(actor => actor.id === id)) throw new Error(`Fabric actor ${id}: registry revocation did not commit`);
      await offlineRemovalHooks.afterStep?.("revoke");
      await check();
      await new ActorBindingStore(config.sessionId, root).delete(id);
      await offlineRemovalHooks.afterStep?.("bindings");
      await check();
      await mesh.delete({ key: pending.presenceKey });
      fs.rmSync(markerPath, { force: true });
      return { offline: true, dryRun, actor: summary, operatorEvidence: evidence, archive: pending.archive, cleaned: true };
    } catch (error) {
      return { offline: true, dryRun, actor: summary, operatorEvidence: evidence, archive: pending.archive, cleaned: false,
        pending: `removal cleanup failed: ${error instanceof Error ? error.message : String(error)} (rerun the removal to finish it)` };
    }
  } finally {
    releaseFence?.();
    mesh?.closeState();
    fs.closeSync(fd);
    fs.closeSync(startupClaim);
  }
};
