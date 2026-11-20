/** Explicit, offline, rename-only retirement of a dead root's residency (#20687). */
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { claimDeadResidentFence, residentLockHasWaiter, validMainStoppedAudit, MAIN_STOPPED_AUDIT_REQUIRED,
  fencedOperatorCheck, type MainStoppedAudit } from "../actors/remove-offline.js";
import { FabricActorQueuedWorkError } from "../actors/types.js";
import { syncDirectoryChain } from "../core/atomic-write.js";
import { resolveOfflineMeshStateBackend } from "../mesh/backend-fence.js";
import { meshStateDocumentProblem } from "../mesh/state-file.js";
import { MeshStore } from "../mesh/store.js";
import { processAlive } from "../storage/scratch.js";
import { runTreeExitVeto } from "../storage/retention.js";
import { assertPinnedDirectory, linkOf, moveActorTreeToArchive, reverifyChain, type ArchiveChain } from "../storage/tree-archive.js";
import { mainPublicationFencePath } from "../topology/main-publication-fence.js";
import { kernelFenceAvailable, lockFile } from "./file-lock.js";
import { assertResidentOperatorConfirmed, readResidentOperatorEvidence } from "./operator-safety.js";
import { residentActorRoots, residentHostId, residentRoot, type ResidentHostConfig } from "./protocol.js";
import { assertNoWatchdogCustody } from "./watchdog-custody.js";

type Json = Record<string, unknown>;
const statAt = (file: string): fs.Stats | undefined => fs.lstatSync(file, { throwIfNoEntry: false });
const ownedDirectory = (directory: string): fs.Stats => {
  const stat = fs.lstatSync(directory);
  if (!stat.isDirectory() || stat.isSymbolicLink() || stat.uid !== process.getuid?.()) {
    throw new Error(`Retirement needs a real directory owned by this OS user: ${directory}`);
  }
  return stat;
};
const readOwned = (file: string): Json => {
  const fd = fs.openSync(file, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW | fs.constants.O_NONBLOCK);
  try {
    const stat = fs.fstatSync(fd);
    if (!stat.isFile() || stat.uid !== process.getuid?.() || stat.size > 16 * 1024 * 1024) throw new Error(`Unsafe custody record: ${file}`);
    const value: unknown = JSON.parse(fs.readFileSync(fd, "utf8"));
    if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error(`Invalid custody record: ${file}`);
    return value as Json;
  } finally { fs.closeSync(fd); }
};
const countArray = (value: Json, key: string, file: string): number => {
  if (value[key] === undefined) return 0;
  if (!Array.isArray(value[key])) throw new Error(`Invalid ${key} custody in ${file}`);
  return value[key].length;
};

/** Read persisted work without constructing ActorManager (which would restore/consume it).
 * Offline retirement is deliberately stricter than live remove: ALL dead letters, stop archives,
 * in-flight records and unconfirmed preparations veto. There is no dropQueued bypass. */
const assertActorsQuiescent = (config: ResidentHostConfig): void => {
  const seen = new Set<string>();
  for (const root of new Set(Object.values(residentActorRoots(config)))) {
    if (!statAt(root)) continue;
    ownedDirectory(root);
    const registry = path.join(root, "actors.json");
    if (!statAt(registry)) continue;
    const saved = readOwned(registry);
    if (saved.format !== 1 || !Array.isArray(saved.actors)) throw new Error(`Invalid actor registry: ${registry}`);
    for (const value of saved.actors) {
      if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error(`Invalid actor row: ${registry}`);
      const row = value as Json;
      if (row.rootId !== config.rootId) continue;
      if (typeof row.id !== "string" || !/^[A-Za-z0-9_-]+$/.test(row.id)) throw new Error(`Invalid actor identity: ${registry}`);
      if (!["dormant", "idle", "stopped", "failed"].includes(String(row.status)) ||
          row.inFlightRun !== undefined || row.preparing !== undefined || row.removal !== undefined) {
        throw new Error(`Actor ${row.id} has live or unresolved run custody; retirement refused`);
      }
      seen.add(row.id);
      const directory = path.join(root, row.id);
      const counts = { queued: 0, parked: 0, overflow: 0, inFlight: 0, settled: 0, deadLetter: 0,
        nonemptyDeadLetterFiles: 0, stopArchiveFiles: 0, pendingOutcome: 0 };
      // Public/legacy rows may retain backlog counts as well as queue checkpoints.
      for (const key of ["queued", "parked", "overflow", "deadLetter"] as const) {
        if (row[key] === undefined) continue;
        if (!Number.isSafeInteger(row[key]) || (row[key] as number) < 0) throw new Error(`Invalid actor ${key}: ${row.id}`);
        counts[key] += row[key] as number;
      }
      if (statAt(directory)) {
        ownedDirectory(directory);
        for (const name of fs.readdirSync(directory)) {
          const file = path.join(directory, name);
          if (/^queue-.*\.json$/.test(name)) {
            const queue = readOwned(file);
            if (queue.format !== 1 || !Array.isArray(queue.items)) throw new Error(`Invalid actor queue: ${file}`);
            // Parked, overflow, deferred and interrupted items share this persisted items array.
            counts.queued += queue.items.length;
            counts.settled += countArray(queue, "settledWindows", file);
            counts.pendingOutcome += countArray(queue, "pendingDeliveryOutcomes", file);
          } else if (name === "dead-letter.jsonl") {
            const stat = fs.lstatSync(file);
            if (!stat.isFile() || stat.uid !== process.getuid?.()) throw new Error(`Unsafe dead-letter custody: ${file}`);
            // No trace/JSONL reads or rewrites are needed to veto nonempty work.
            if (stat.size > 0) counts.nonemptyDeadLetterFiles++;
          } else if (name === "stopped-queues") {
            ownedDirectory(file);
            counts.stopArchiveFiles += fs.readdirSync(file).length;
          } else if (name === "runs") assertRunsExited(file);
        }
      }
      if (Object.values(counts).some(count => count > 0)) throw new FabricActorQueuedWorkError("remove", row.id, counts,
        `Actor ${row.id} has held work; retire-residency refused and nothing moved: ${JSON.stringify(counts)}`);
    }
  }
  const routesFile = path.join(config.residencyRoot, "wake-routes.json");
  if (statAt(routesFile)) {
    const routes = readOwned(routesFile);
    if (routes.rootId !== config.rootId || !Array.isArray(routes.actors)) throw new Error(`Invalid retained actor routes: ${routesFile}`);
    for (const actor of routes.actors) {
      if (!actor || typeof actor.id !== "string" || !seen.has(actor.id)) {
        throw new Error("Retained wake actor has no matching registry identity; work custody is unknown, retirement refused");
      }
    }
  }
};
const assertRunsExited = (directory: string): void => {
  if (!statAt(directory)) return;
  ownedDirectory(directory);
  for (const name of fs.readdirSync(directory)) {
    const run = path.join(directory, name);
    ownedDirectory(run);
    const veto = runTreeExitVeto(run, 0, undefined, true, true);
    if (veto) throw new Error(`Run ${run} has unresolved custody: ${veto}; retirement refused`);
  }
};
const assertDeadHolder = (file: string): void => {
  if (!statAt(file)) return;
  const value = readOwned(file);
  if (typeof value.pid !== "number" || !Number.isSafeInteger(value.pid) || value.pid <= 0 || processAlive(value.pid)) {
    throw new Error(`Residency has a live or unknown holder in ${file}; retirement refused`);
  }
};
const assertQuiescent = (directory: string, config: ResidentHostConfig): void => {
  assertDeadHolder(path.join(directory, "host.lock"));
  assertDeadHolder(path.join(directory, "main-generation.json"));
  const ownerFile = path.join(directory, "owner.json");
  assertDeadHolder(ownerFile);
  if (statAt(ownerFile)) {
    const handover = readOwned(ownerFile).handover as Json | undefined;
    const launcher = handover?.launcher as Json | undefined;
    if (handover && (!launcher || typeof launcher.pid !== "number" || !Number.isSafeInteger(launcher.pid) ||
        launcher.pid <= 0 || processAlive(launcher.pid))) throw new Error("Resident launcher is live or unknown; retirement refused");
  }
  assertNoWatchdogCustody(directory); // A dead owner never discharges unknown escaped-worker custody.
  for (const name of ["requests", "processing", "delivery-outbox"]) {
    const file = path.join(directory, name);
    if (!statAt(file)) continue;
    ownedDirectory(file);
    if (fs.readdirSync(file).length) throw new Error(`Residency has pending ${name} custody; retirement refused`);
  }
  const agents = path.join(directory, "agents");
  if (statAt(agents)) {
    ownedDirectory(agents);
    for (const name of fs.readdirSync(agents)) {
      const metadata = readOwned(path.join(agents, name));
      // The handle is an admission snapshot, not terminal/worker-exit evidence. Require the
      // actual run's native exit proof and the Main's completion-consumption receipt instead.
      if (metadata.rootId !== config.rootId || typeof metadata.runDirectory !== "string" ||
          typeof metadata.completionConsumedAt !== "number" || !Number.isFinite(metadata.completionConsumedAt)) {
        throw new Error(`Resident agent ${name} has unresolved work; retirement refused`);
      }
      const veto = runTreeExitVeto(metadata.runDirectory, 0, undefined, true, true);
      if (veto) throw new Error(`Resident agent ${name} has unresolved custody: ${veto}; retirement refused`);
    }
  }
  assertRunsExited(path.join(directory, "runs"));
  assertActorsQuiescent(config);
};

export async function retireResidency(directory: string, config: ResidentHostConfig,
  options: { dryRun?: boolean; confirmDeadRoot?: string; mainStoppedAudit?: MainStoppedAudit }): Promise<{
    dryRun: boolean; retired: boolean; archive?: string; auditFile?: string;
    operatorEvidence: ReturnType<typeof readResidentOperatorEvidence>;
    plan: { rename: { from: string; to: string }; auditFile: string; create: string[]; delete: never[] };
  }> {
  if (process.platform !== "linux" || !process.getuid || !kernelFenceAvailable()) throw new Error("Residency retirement needs Linux flock/setpriv and provable file ownership");
  if (!validMainStoppedAudit(options.mainStoppedAudit, config.rootId)) throw new Error(MAIN_STOPPED_AUDIT_REQUIRED);
  const dryRun = options.dryRun === true;
  const pin = ownedDirectory(directory);
  const meshRoot = fs.realpathSync(config.meshRoot);
  if (fs.realpathSync(config.residencyRoot) !== directory || residentRoot(meshRoot, config.rootId) !== directory) {
    throw new Error("Resident directory/config identity mismatch");
  }
  const chain: ArchiveChain = [linkOf(meshRoot, ownedDirectory(meshRoot)),
    linkOf(path.resolve(config.meshRoot), fs.statSync(config.meshRoot), true),
    linkOf(path.dirname(directory), ownedDirectory(path.dirname(directory)))];
  const configBytes = fs.readFileSync(path.join(directory, "config.json"), "utf8");
  if (JSON.stringify(readOwned(path.join(directory, "config.json"))) !== JSON.stringify(config)) {
    throw new Error("Resident config changed since selection");
  }
  const archiveRoot = path.join(meshRoot, "residency-retired");
  const container = path.join(archiveRoot, `${path.basename(directory)}.${Date.now()}.${randomUUID()}`);
  const archive = path.join(container, "tree"), auditFile = path.join(container, "operator.json");
  const publication = mainPublicationFencePath(meshRoot, config.rootId);
  const locks = ["host-fence-establish.lock", "host.lock", "wake.lock", "wake-intent.lock", "handover.lock"].map(name => path.join(directory, name));
  const create = [...locks.filter(file => !statAt(file)), ...(!statAt(path.dirname(publication)) ? [path.dirname(publication)] : []),
    ...(!statAt(publication) ? [publication] : []), ...(!statAt(archiveRoot) ? [archiveRoot] : []), container, auditFile];
  const plan = { rename: { from: directory, to: archive }, auditFile, create, delete: [] as never[] };
  const held: Array<{ file: string; fd: number }> = [];
  let mesh: MeshStore | undefined;
  try {
    // Refuse known live/work custody before creating anything. Existing fences are probed first,
    // in the host's startup order; a held late fence must not create earlier missing lock files.
    assertQuiescent(directory, config);
    if (!statAt(path.join(directory, "host.lock"))) throw new Error("Resident host.lock is missing; holder identity unavailable");
    for (const file of locks) {
      if (!statAt(file)) continue;
      const fd = file === path.join(directory, "host.lock") ? await claimDeadResidentFence(directory) : await lockFile(file, 0, true);
      held.push({ file, fd });
    }
    if (statAt(publication)) held.push({ file: publication, fd: await lockFile(publication, 0, true) });
    const backend = resolveOfflineMeshStateBackend(meshRoot, meshStateDocumentProblem);
    mesh = new MeshStore(meshRoot, config.mesh.maxEventBytes, config.mesh.maxReadEvents, { lockProtocol: config.mesh.lockProtocol, stateBackend: backend });
    const check = (get: MeshStore["get"]): ReturnType<typeof readResidentOperatorEvidence> => {
      reverifyChain(chain);
      assertPinnedDirectory(directory, pin);
      if (fs.readFileSync(path.join(directory, "config.json"), "utf8") !== configBytes) throw new Error("Resident config changed under the retirement fence");
      for (const { file, fd } of held) {
        const named = fs.lstatSync(file), locked = fs.fstatSync(fd);
        if (named.isSymbolicLink() || named.ino !== locked.ino || named.dev !== locked.dev || residentLockHasWaiter(fd)) {
          throw new Error(`Residency fence changed or has a live waiter: ${file}`);
        }
      }
      assertQuiescent(directory, config);
      const evidence = readResidentOperatorEvidence(config, { get }, undefined, { mainStopped: true });
      assertResidentOperatorConfirmed(evidence, options.confirmDeadRoot, false, true);
      return evidence;
    };
    const evidence = check(mesh.get.bind(mesh));
    const existingArchive = statAt(archiveRoot);
    if (existingArchive) {
      const stat = ownedDirectory(archiveRoot);
      if (stat.dev !== pin.dev) throw new Error("Residency archive is on another filesystem (EXDEV); no copy allowed");
      chain.push(linkOf(archiveRoot, stat));
    } else if (ownedDirectory(meshRoot).dev !== pin.dev) throw new Error("Residency archive is on another filesystem (EXDEV); no copy allowed");
    if (dryRun) {
      check(mesh.get.bind(mesh));
      return { dryRun, retired: false, operatorEvidence: evidence, plan };
    }
    for (const file of locks) {
      if (!held.some(lock => lock.file === file)) held.push({ file, fd: await lockFile(file, 0, true) });
    }
    // Same Main publication fence as remove, but retain its inode and bytes after releasing it.
    // In particular, do not use remove's fence finalizer, which unlinks its fence record.
    if (!statAt(path.dirname(publication))) fs.mkdirSync(path.dirname(publication), { mode: 0o700 });
    ownedDirectory(path.dirname(publication));
    if (!held.some(lock => lock.file === publication)) held.push({ file: publication, fd: await lockFile(publication, 0, true) });
    await fencedOperatorCheck(mesh, { id: residentHostId(config.rootId), name: "fabric-actors retire-residency", kind: "agent" }, config.rootId, get => {
      const current = check(get);
      if (!existingArchive) { fs.mkdirSync(archiveRoot, { mode: 0o700 }); chain.push(linkOf(archiveRoot, ownedDirectory(archiveRoot))); }
      reverifyChain(chain);
      fs.mkdirSync(container, { mode: 0o700 }); // Exclusive reservation; never merge or overwrite.
      chain.push(linkOf(container, ownedDirectory(container)));
      const fd = fs.openSync(auditFile, fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | fs.constants.O_NOFOLLOW, 0o600);
      try {
        fs.writeFileSync(fd, JSON.stringify({ ...options.mainStoppedAudit, toolEvidence: current.toolEvidence, plan }, null, 2) + "\n");
        fs.fsyncSync(fd);
      } finally { fs.closeSync(fd); }
      // No await between final custody proof and the one whole-tree rename.
      check(get);
      moveActorTreeToArchive(directory, pin, container, chain);
      try { syncDirectoryChain(container); syncDirectoryChain(path.dirname(directory)); }
      catch (error) { throw new Error(`Residency was moved to ${archive} (audit ${auditFile}); namespace sync failed: ${String(error)}; retained, do not restart it`); }
    });
    return { dryRun, retired: true, archive, auditFile, operatorEvidence: evidence, plan };
  } finally {
    try { mesh?.closeState(); }
    finally { for (const { fd } of held.reverse()) fs.closeSync(fd); }
  }
}
