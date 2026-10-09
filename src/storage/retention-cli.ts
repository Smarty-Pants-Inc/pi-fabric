import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { ActorRegistryStore } from "../actors/registry-store.js";
import { lockFile } from "../residency/file-lock.js";
import { readMeshStateMovedMarker } from "../mesh/backend-fence.js";
import { sweepResidentRuns } from "../residency/retention.js";
import { actorRunReferencedNow, compactTerminalRunEvents, pruneActorRunArchives, pruneActorSessionBackups, retainedActorRunIds } from "./retention.js";
import { ownedStat, processAlive } from "./scratch.js";

const read = (file: string, maxBytes = 1024 * 1024): Record<string, unknown> => {
  const stat = ownedStat(file);
  if (!stat?.isFile() || stat.size > maxBytes) throw new Error(`Unsafe or unreadable custody record: ${file}`);
  return JSON.parse(fs.readFileSync(file, "utf8"));
};
const absent = (file: string): boolean => {
  try { fs.lstatSync(file); return false; }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return true; throw error; }
};
const directories = (root: string): string[] => {
  if (!ownedStat(root)?.isDirectory()) return [];
  return fs.readdirSync(root).map(name => path.join(root, name)).filter(file => ownedStat(file)?.isDirectory());
};
const deadHolder = (file: string, optional = false): boolean => {
  if (optional && absent(file)) return true;
  const holder = read(file);
  return typeof holder.pid === "number" && Number.isSafeInteger(holder.pid) && holder.pid > 0 && !processAlive(holder.pid);
};

const treeBytes = (file: string, depth = 0): number => {
  try {
    const stat = fs.lstatSync(file);
    if (!stat.isDirectory() || depth > 32) return stat.size;
    return fs.readdirSync(file).reduce((sum, name) => sum + treeBytes(path.join(file, name), depth + 1), 0);
  } catch { return 0; }
};

/** An operator hold on deletions (smarty-dev#7766): while present, --apply changes nothing. */
export const MESH_RETENTION_HOLD = ".mesh-retention-hold.json";
/** The ruling that allows deletions on a switched backend: `{ "epoch": <the moved marker's epoch> }`. */
export const MESH_RETENTION_APPROVAL = ".mesh-retention-approved.json";
/** Why --apply must not delete now, or undefined. A backend switch (a moved marker at a new epoch) holds
 * deletions until a ruling names that epoch, so no automatic sweep deletes during or after a switch. */
// The caller has proven meshRoot an owned directory. Any hold entry (lstat: a link counts) holds. The approval is
// read from one O_NOFOLLOW descriptor that fstat proves our own regular, not group/world-writable file, so a
// swapped-in link or foreign file never supplies an epoch. Where ownership cannot be proven (no getuid: Windows),
// no approval is ever valid, so a switched mesh deletes nothing there (fail closed).
const applyRefusal = (meshRoot: string): string | undefined => {
  if (!absent(path.join(meshRoot, MESH_RETENTION_HOLD))) return `deletions are held (${MESH_RETENTION_HOLD})`;
  const moved = readMeshStateMovedMarker(meshRoot);
  if (moved === undefined) return undefined;
  let approved: unknown;
  try { approved = readPrivate(path.join(meshRoot, MESH_RETENTION_APPROVAL)).epoch; } catch { /* absent, unsafe or unreadable: not approved */ }
  return approved === moved.epoch ? undefined
    : `the mesh switched to ${moved.backend} at epoch ${moved.epoch}; deletions need a ruling in ${MESH_RETENTION_APPROVAL}`;
};
const readPrivate = (file: string, maxBytes = 64 * 1024): Record<string, unknown> => {
  const fd = fs.openSync(file, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW | fs.constants.O_NONBLOCK);
  try {
    const stat = fs.fstatSync(fd);
    if (!stat.isFile() || stat.size > maxBytes || typeof process.getuid !== "function" || stat.uid !== process.getuid() ||
        (stat.mode & 0o022) !== 0) {
      throw new Error(`Unsafe record: ${file}`);
    }
    return JSON.parse(fs.readFileSync(fd, "utf8"));
  } finally { fs.closeSync(fd); }
};
/** Whether another process waits for the flock on this descriptor's file: a host waking now (Linux /proc/locks
 * lists blocked requests as `->` lines). ponytail: matched by inode only; a collision on another device just
 * skips that root (fail safe). */
const lockWaiter = (fd: number): boolean => {
  const inode = fs.fstatSync(fd).ino;
  try {
    return fs.readFileSync("/proc/locks", "utf8").split("\n")
      .some(line => line.includes("->") && line.split(/\s+/).some(field => field.endsWith(`:${inode}`)));
  } catch { return true; } // unknown: treat as contended
};

/** Offline compaction; with `runRetentionMs`, also age-based run removal (smarty-dev#3252). Existing host flocks must
 * exist and be claimable, diagnostic holders must be proven dead, and actor roots
 * must belong to those fenced resident roots. Live/legacy/unknown custody is skipped.
 * Dry-run never creates locks, markers or files (flock on an existing inode only). */
export const sweepMeshRetention = async (meshRoot: string, options: {
  dryRun?: boolean; now?: number;
  /** Remove terminal actor runs older than this, mesh-wide (the actor archive TTL). Unset: compaction only. */
  runRetentionMs?: number;
  /** Removal age of a proven-dead resident root's runs (default: the live host's 24 h). */
  residentRunRetentionMs?: number;
} = {}) => {
  const dryRun = options.dryRun !== false;
  const now = options.now ?? Date.now();
  const removedRuns: string[] = [];
  const changes: Array<{ path: string; beforeBytes: number; afterBytes: number }> = [];
  const skipped: Array<{ path: string; reason: string }> = [];
  const residents = new Map<string, string>();
  const ambiguous = new Set<string>();
  const registries: string[] = [];
  const discover = (root: string, depth = 0, found = registries): void => {
    if (absent(root)) return;
    if (depth > 4 || !ownedStat(root)?.isDirectory()) throw new Error(`Actor references are uncertain: ${root}`);
    if (!absent(path.join(root, "actors.json"))) found.push(root);
    for (const name of fs.readdirSync(root)) {
      if (["runs", "handoff-session", "child-completions"].includes(name)) continue;
      const child = path.join(root, name); const stat = ownedStat(child);
      if (!stat) throw new Error(`Actor references are uncertain: ${child}`);
      if (stat.isDirectory()) discover(child, depth + 1, found);
    }
  };
  if (!ownedStat(meshRoot)?.isDirectory()) throw new Error("Mesh root must be an owned directory");
  // The deletion gate (smarty-dev#7766), after the root is proven ours. Re-checked before every deletion below;
  // a hold, a new epoch or a lost approval stops every later deletion of this sweep (sticky).
  let halted: string | undefined;
  const gate = (): boolean => {
    if (dryRun || halted !== undefined) return halted !== undefined;
    halted = applyRefusal(meshRoot);
    if (halted !== undefined) skipped.push({ path: meshRoot, reason: `${halted}; no further deletion` });
    return halted !== undefined;
  };
  if (gate()) return { dryRun, changes, removedRuns, bytesBefore: 0, bytesAfter: 0, skipped: [{ path: meshRoot, reason: halted! }] };
  const actorsRoot = path.join(meshRoot, "actors");
  discover(actorsRoot);
  // pi-fabric#645 review round 4: every final pre-delete check re-discovers the registry roots. A root
  // created (or removed) since discovery may hold a cross-root reference the reread of `registries`
  // cannot see: every later delete fails closed and the sweep reports it. Sticky once changed.
  const rootSet = (roots: readonly string[]) => [...roots].sort().join("\0");
  const discovered = rootSet(registries);
  let rootsChanged = false;
  const registryRootsChanged = (): boolean => {
    if (rootsChanged) return true;
    let current: string | undefined;
    try { const found: string[] = []; discover(actorsRoot, 0, found); current = rootSet(found); } catch { /* uncertain: changed */ }
    if (current !== discovered) {
      rootsChanged = true;
      skipped.push({ path: actorsRoot, reason: "actor registry roots changed during the sweep; no further run is deleted" });
    }
    return rootsChanged;
  };
  // The dead host's flock fence on one residency root: claimed, re-proven and released around
  // that root's own work only (smarty-dev#7766). Holding every idle root's host.lock for the
  // whole sweep made each of them unwakeable while it ran (60fe5904, 10-09: a 7-day apply held
  // 30 roots for 13+ min at 100% CPU and every Main wake timed out). A host that woke since
  // discovery holds its lock: this root is skipped and the host wins.
  const claimFence = async (root: string): Promise<{ fd: number; rootId: string }> => {
    const lock = path.join(root, "host.lock");
    if (process.platform !== "linux" || !ownedStat(lock)?.isFile() ||
        !deadHolder(lock) || !deadHolder(path.join(root, "owner.json"), true)) throw new Error("live or unknown holder");
    const fd = await lockFile(lock, 0, true);
    try {
      if (!deadHolder(lock) || !deadHolder(path.join(root, "owner.json"), true)) throw new Error("holder changed");
      const config = read(path.join(root, "config.json"));
      if (config.format !== 1 || typeof config.rootId !== "string" ||
          typeof config.residencyRoot !== "string" || path.resolve(config.residencyRoot) !== path.resolve(root) ||
          typeof config.meshRoot !== "string" || path.resolve(config.meshRoot) !== path.resolve(meshRoot)) throw new Error("unknown root identity");
      return { fd, rootId: config.rootId };
    } catch (error) { fs.closeSync(fd); throw error; }
  };
  // veto(), before each deletion under the fence: the deletion gate, a host waiting for this root's lock (it wins:
  // the rest of this root's work is skipped and the fence released), or a holder that is no longer proven dead.
  const fenced = async (rootId: string, root: string, work: (veto: () => boolean) => void | Promise<void>): Promise<void> => {
    let claim: { fd: number; rootId: string } | undefined;
    let vetoed: string | undefined;
    try {
      claim = await claimFence(root);
      if (claim.rootId !== rootId) throw new Error("root identity changed");
      const fd = claim.fd;
      const veto = (): boolean => {
        if (vetoed === undefined) {
          if (gate()) vetoed = "deletions halted";
          else if (lockWaiter(fd)) vetoed = "a host is waking on this root; it wins";
          else {
            try { if (!deadHolder(path.join(root, "host.lock")) || !deadHolder(path.join(root, "owner.json"), true)) vetoed = "holder no longer proven dead"; }
            catch { vetoed = "holder unknown"; }
          }
        }
        return vetoed !== undefined;
      };
      await work(veto);
    } catch (error) { skipped.push({ path: root, reason: String(error) }); }
    finally {
      if (claim) fs.closeSync(claim.fd);
      if (vetoed !== undefined) skipped.push({ path: root, reason: vetoed });
    }
  };
  { // ponytail: the former try/finally's scope, kept to keep this diff small; no fence outlives its root's work now.
    for (const root of directories(path.join(meshRoot, "residency"))) {
      try {
        const { fd, rootId } = await claimFence(root);
        fs.closeSync(fd);
        if (residents.has(rootId) || ambiguous.has(rootId)) {
          residents.delete(rootId); ambiguous.add(rootId); throw new Error("duplicate root identity");
        }
        residents.set(rootId, root);
      } catch (error) { skipped.push({ path: root, reason: String(error) }); }
    }
    const retained = retainedActorRunIds(registries);
    if (retained.has("*")) return { dryRun, changes, removedRuns, bytesBefore: 0, bytesAfter: 0, skipped: [...skipped, { path: meshRoot, reason: "unknown actor run references" }] };
    const compactRuns = (runs: string, veto: () => boolean = gate) => {
      if (retained.has("*")) return;
      for (const run of directories(runs)) if (!retained.has(path.basename(run))) {
        compactTerminalRunEvents(run, { ...options, dryRun, onCompact: change => changes.push(change),
          isRetained: () => { if (veto()) return true; const current = retainedActorRunIds(registries); return current.has("*") || current.has(path.basename(run)); },
        });
      }
    };
    for (const [rootId, root] of residents) await fenced(rootId, root, veto => {
      // The dead host's own startup sweep, under its flock fence: the same exit, tree,
      // preserved-result and latest-run fences as a live host's removal (smarty-dev#3252).
      if (options.runRetentionMs !== undefined) {
        // Same final pre-delete check as the actor-run pass below: every discovered registry is
        // re-read uncached before each delete; a vanished or unreadable one vetoes (review round 2).
        // A dry run takes the same selection and fences without mutating, so its preview lists
        // the runs --apply deletes (review round 3). Sizes are taken before the final check, which also
        // re-discovers the registry roots, and the check and delete run back to back (review round 4).
        removedRuns.push(...sweepResidentRuns(path.join(root, "runs"), now, 10 * 60 * 1_000, {
          actorRoots: registries, retainRuns: false, requireRegistries: true, dryRun, acceptPidReuse: true,
          isRetained: () => veto() || registryRootsChanged(), measure: treeBytes,
          onRemove: (run, bytes) => changes.push({ path: run, beforeBytes: bytes, afterBytes: 0 }),
          onCompact: change => changes.push(change),
          ...(options.residentRunRetentionMs !== undefined ? { retentionMs: options.residentRunRetentionMs } : {}),
        }));
      } else compactRuns(path.join(root, "runs"), veto);
    });
    // With runRetentionMs the mesh-wide pass below covers these actors (and every other one).
    for (const registryRoot of residents.size && options.runRetentionMs === undefined ? registries : []) {
      // One resident root at a time, under that root's own brief fence (smarty-dev#7766).
      const prune = (rootId: string, veto: () => boolean) => {
        // Match ActorRegistryStore/retainedActorRunIds: owned registries contain
        // instructions and message history and have no byte-size protocol limit.
        // Diagnostic records keep the bounded reader above.
        const registry = read(path.join(registryRoot, "actors.json"), Number.MAX_SAFE_INTEGER);
        if (!Array.isArray(registry.actors)) throw new Error("unreadable actor registry");
        for (const actor of registry.actors) {
          if (!actor || actor.rootId !== rootId || actor.removal !== undefined ||
              typeof actor.id !== "string" || !/^[A-Za-z0-9_-]+$/.test(actor.id) || typeof actor.sessionFile !== "string" ||
              path.resolve(actor.sessionFile) !== path.join(path.resolve(registryRoot), actor.id, "session.jsonl")) continue;
          const actorRoot = path.dirname(actor.sessionFile);
          if (!ownedStat(actorRoot)?.isDirectory()) continue;
          compactRuns(path.join(actorRoot, "runs"), veto);
          // Pending launch/drain/removal is not a joined writer receipt for session history.
          if (actor.inFlightRun !== undefined || actor.preparing !== undefined || !["idle", "stopped"].includes(actor.status)) continue;
          pruneActorSessionBackups(actor.sessionFile, { dryRun, stop: veto, onPrune: change => changes.push({ path: change.path, beforeBytes: change.bytes, afterBytes: 0 }) });
        }
      };
      let rootIds: Set<string>;
      try {
        const registry = read(path.join(registryRoot, "actors.json"), Number.MAX_SAFE_INTEGER);
        if (!Array.isArray(registry.actors)) throw new Error("unreadable actor registry");
        rootIds = new Set(registry.actors.map((actor: Record<string, unknown> | null) => actor?.rootId)
          .filter((rootId: unknown): rootId is string => typeof rootId === "string" && residents.has(rootId)));
      } catch (error) { skipped.push({ path: registryRoot, reason: String(error) }); continue; }
      for (const rootId of rootIds) await fenced(rootId, residents.get(rootId)!, async veto => {
        try {
          if (!absent(path.join(registryRoot, "actors.json.lock"))) throw new Error("actor registry has a holder or uncertain lock");
          if (dryRun) prune(rootId, veto); else await new ActorRegistryStore(registryRoot).withLock(() => prune(rootId, veto));
        } catch (error) { skipped.push({ path: registryRoot, reason: String(error) }); }
      });
    }
    if (options.runRetentionMs !== undefined) {
      // Mesh-wide, independent of any owner being alive (smarty-dev#3252, #5652): the owning
      // manager's own pruneRuns rule (terminal, safe exit-proven tree, not the latest run),
      // plus every registry's latest, in-flight, preparing and pending-removal runs, re-read when a
      // registry changes. The final pre-delete check re-reads every discovered registry uncached;
      // an unreadable or vanished registry is a wildcard veto (pi-fabric#645 review round 1).
      const fingerprint = () => registries.map(root => {
        const stat = ownedStat(path.join(root, "actors.json"));
        return stat ? `${stat.ino}:${stat.size}:${stat.mtimeMs}` : "-";
      }).join("|");
      let references = { key: fingerprint(), ids: retained };
      const referenced = (id: string): boolean => {
        const key = fingerprint();
        if (key !== references.key) references = { key, ids: retainedActorRunIds(registries) };
        return references.ids.has("*") || references.ids.has(id);
      };
      const referencedNow = (id: string): boolean =>
        actorRunReferencedNow(registries, id, { requireRegistries: true }) || registryRootsChanged();
      for (const registryRoot of registries) {
        try {
          const registry = read(path.join(registryRoot, "actors.json"), Number.MAX_SAFE_INTEGER);
          if (!Array.isArray(registry.actors)) throw new Error("unreadable actor registry");
          const rows = new Map<string, Record<string, any>>();
          for (const actor of registry.actors) if (actor && typeof actor.id === "string") rows.set(actor.id, actor);
          for (const actorRoot of directories(registryRoot)) {
            const id = path.basename(actorRoot), row = rows.get(id);
            // Removal (pending or accepted) owns its runs until it finishes.
            if (!/^[A-Za-z0-9_-]+$/.test(id) || row?.removal !== undefined || !absent(path.join(registryRoot, `removal-${id}.json`))) continue;
            const inFlight = typeof row?.inFlightRun?.id === "string" ? row.inFlightRun.id : undefined;
            pruneActorRunArchives({
              runsDirectory: path.join(actorRoot, "runs"), retentionMs: options.runRetentionMs, now, dryRun, acceptPidReuse: true,
              ...(typeof row?.lastRunId === "string" ? { latestRunId: row.lastRunId } : {}),
              isRetained: (run, final) => gate() || run === inFlight || (final ? referencedNow(run) : referenced(run)),
              // Sized before the final check; check and delete back to back (review round 4).
              measure: treeBytes,
              onRemove: (run, bytes) => { changes.push({ path: run, beforeBytes: bytes, afterBytes: 0 }); removedRuns.push(run); },
              onCompact: change => changes.push(change),
            });
            // Rotation history of an actor whose writer is joined: keep only the newest backup.
            if (row && row.inFlightRun === undefined && row.preparing === undefined && ["idle", "stopped"].includes(row.status) &&
                typeof row.sessionFile === "string" && path.resolve(row.sessionFile) === path.join(path.resolve(actorRoot), "session.jsonl")) {
              pruneActorSessionBackups(row.sessionFile, { dryRun, stop: gate, onPrune: change => changes.push({ path: change.path, beforeBytes: change.bytes, afterBytes: 0 }) });
            }
          }
        } catch (error) { skipped.push({ path: registryRoot, reason: String(error) }); }
      }
    }
  }
  return { dryRun, changes, removedRuns, bytesBefore: changes.reduce((sum, item) => sum + item.beforeBytes, 0),
    bytesAfter: changes.reduce((sum, item) => sum + item.afterBytes, 0), skipped };
};

/** Write the apply's report (smarty-dev#7766): a fresh O_EXCL|O_NOFOLLOW 0600 temp file beside it, fsync, then an
 * atomic rename, so a swapped-in link is never followed and a reader never sees a partial report. */
export const writeReport = (file: string, text: string): void => {
  const temp = path.join(path.dirname(file), `.${path.basename(file)}.${process.pid}.${Date.now()}.tmp`);
  const fd = fs.openSync(temp, fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | fs.constants.O_NOFOLLOW, 0o600);
  try {
    try { fs.writeSync(fd, text); fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
    fs.renameSync(temp, file);
  } catch (error) { fs.rmSync(temp, { force: true }); throw error; }
};

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const args = process.argv.slice(2);
  const take = (flag: string): string | undefined => {
    const at = args.indexOf(flag);
    return at < 0 ? undefined : args.splice(at, 2)[1] ?? "";
  };
  const report = take("--report");
  const value = take("--runs-older-than");
  const [root, mode = "--dry-run"] = args;
  const runRetentionMs = value === undefined ? undefined : Number(value);
  if (!root || !["--dry-run", "--apply"].includes(mode) || args.length > 2 || report === "" ||
      (runRetentionMs !== undefined && (!Number.isSafeInteger(runRetentionMs) || runRetentionMs < 60 * 60 * 1_000))) {
    console.error("Usage: node dist/storage/retention-cli.js <mesh-root> [--dry-run|--apply] [--runs-older-than <ms, >= 1 h>] [--report <file>]"); process.exitCode = 2;
  } else {
    try {
      const text = JSON.stringify(await sweepMeshRetention(path.resolve(root), {
        dryRun: mode !== "--apply", ...(runRetentionMs !== undefined ? { runRetentionMs } : {}),
      }), null, 2);
      if (report !== undefined) writeReport(path.resolve(report), `${text}\n`); else console.log(text);
    }
    catch (error) { console.error(error); process.exitCode = 1; }
  }
}
