import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { ActorRegistryStore } from "../actors/registry-store.js";
import { lockFile } from "../residency/file-lock.js";
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
  const fences: number[] = [];
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
  try {
    for (const root of directories(path.join(meshRoot, "residency"))) {
      let fd: number | undefined;
      try {
        const lock = path.join(root, "host.lock");
        if (process.platform !== "linux" || !ownedStat(lock)?.isFile() ||
            !deadHolder(lock) || !deadHolder(path.join(root, "owner.json"), true)) throw new Error("live or unknown holder");
        fd = await lockFile(lock, 0, true);
        if (!deadHolder(lock) || !deadHolder(path.join(root, "owner.json"), true)) throw new Error("holder changed");
        const config = read(path.join(root, "config.json"));
        if (config.format !== 1 || typeof config.rootId !== "string" ||
            typeof config.residencyRoot !== "string" || path.resolve(config.residencyRoot) !== path.resolve(root) ||
            typeof config.meshRoot !== "string" || path.resolve(config.meshRoot) !== path.resolve(meshRoot)) throw new Error("unknown root identity");
        if (residents.has(config.rootId) || ambiguous.has(config.rootId)) {
          residents.delete(config.rootId); ambiguous.add(config.rootId); throw new Error("duplicate root identity");
        }
        residents.set(config.rootId, root); fences.push(fd); fd = undefined;
      } catch (error) { skipped.push({ path: root, reason: String(error) }); }
      finally { if (fd !== undefined) fs.closeSync(fd); }
    }
    const retained = retainedActorRunIds(registries);
    if (retained.has("*")) return { dryRun, changes, removedRuns, bytesBefore: 0, bytesAfter: 0, skipped: [...skipped, { path: meshRoot, reason: "unknown actor run references" }] };
    const compactRuns = (runs: string) => {
      if (retained.has("*")) return;
      for (const run of directories(runs)) if (!retained.has(path.basename(run))) {
        compactTerminalRunEvents(run, { ...options, dryRun, onCompact: change => changes.push(change),
          isRetained: () => { const current = retainedActorRunIds(registries); return current.has("*") || current.has(path.basename(run)); },
        });
      }
    };
    for (const root of residents.values()) {
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
          isRetained: registryRootsChanged, measure: treeBytes,
          onRemove: (run, bytes) => changes.push({ path: run, beforeBytes: bytes, afterBytes: 0 }),
          onCompact: change => changes.push(change),
          ...(options.residentRunRetentionMs !== undefined ? { retentionMs: options.residentRunRetentionMs } : {}),
        }));
      } else compactRuns(path.join(root, "runs"));
    }
    // With runRetentionMs the mesh-wide pass below covers these actors (and every other one).
    for (const registryRoot of residents.size && options.runRetentionMs === undefined ? registries : []) {
      const prune = () => {
        // Match ActorRegistryStore/retainedActorRunIds: owned registries contain
        // instructions and message history and have no byte-size protocol limit.
        // Diagnostic records keep the bounded reader above.
        const registry = read(path.join(registryRoot, "actors.json"), Number.MAX_SAFE_INTEGER);
        if (!Array.isArray(registry.actors)) throw new Error("unreadable actor registry");
        for (const actor of registry.actors) {
          if (!actor || typeof actor.rootId !== "string" || !residents.has(actor.rootId) || actor.removal !== undefined ||
              typeof actor.id !== "string" || !/^[A-Za-z0-9_-]+$/.test(actor.id) || typeof actor.sessionFile !== "string" ||
              path.resolve(actor.sessionFile) !== path.join(path.resolve(registryRoot), actor.id, "session.jsonl")) continue;
          const actorRoot = path.dirname(actor.sessionFile);
          if (!ownedStat(actorRoot)?.isDirectory()) continue;
          compactRuns(path.join(actorRoot, "runs"));
          // Pending launch/drain/removal is not a joined writer receipt for session history.
          if (actor.inFlightRun !== undefined || actor.preparing !== undefined || !["idle", "stopped"].includes(actor.status)) continue;
          pruneActorSessionBackups(actor.sessionFile, { dryRun, onPrune: change => changes.push({ path: change.path, beforeBytes: change.bytes, afterBytes: 0 }) });
        }
      };
      try {
        if (!absent(path.join(registryRoot, "actors.json.lock"))) throw new Error("actor registry has a holder or uncertain lock");
        if (dryRun) prune(); else await new ActorRegistryStore(registryRoot).withLock(prune);
      } catch (error) { skipped.push({ path: registryRoot, reason: String(error) }); }
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
              isRetained: (run, final) => run === inFlight || (final ? referencedNow(run) : referenced(run)),
              // Sized before the final check; check and delete back to back (review round 4).
              measure: treeBytes,
              onRemove: (run, bytes) => { changes.push({ path: run, beforeBytes: bytes, afterBytes: 0 }); removedRuns.push(run); },
              onCompact: change => changes.push(change),
            });
            // Rotation history of an actor whose writer is joined: keep only the newest backup.
            if (row && row.inFlightRun === undefined && row.preparing === undefined && ["idle", "stopped"].includes(row.status) &&
                typeof row.sessionFile === "string" && path.resolve(row.sessionFile) === path.join(path.resolve(actorRoot), "session.jsonl")) {
              pruneActorSessionBackups(row.sessionFile, { dryRun, onPrune: change => changes.push({ path: change.path, beforeBytes: change.bytes, afterBytes: 0 }) });
            }
          }
        } catch (error) { skipped.push({ path: registryRoot, reason: String(error) }); }
      }
    }
  } finally { for (const fd of fences) fs.closeSync(fd); }
  return { dryRun, changes, removedRuns, bytesBefore: changes.reduce((sum, item) => sum + item.beforeBytes, 0),
    bytesAfter: changes.reduce((sum, item) => sum + item.afterBytes, 0), skipped };
};

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const [root, mode = "--dry-run", flag, value] = process.argv.slice(2);
  const runRetentionMs = flag === "--runs-older-than" ? Number(value) : undefined;
  if (!root || !["--dry-run", "--apply"].includes(mode) || (flag !== undefined && (flag !== "--runs-older-than" ||
      !Number.isSafeInteger(runRetentionMs) || runRetentionMs! < 60 * 60 * 1_000)) || process.argv.length > 6) {
    console.error("Usage: node dist/storage/retention-cli.js <mesh-root> [--dry-run|--apply] [--runs-older-than <ms, >= 1 h>]"); process.exitCode = 2;
  } else {
    try {
      console.log(JSON.stringify(await sweepMeshRetention(path.resolve(root), {
        dryRun: mode !== "--apply", ...(runRetentionMs !== undefined ? { runRetentionMs } : {}),
      }), null, 2));
    }
    catch (error) { console.error(error); process.exitCode = 1; }
  }
}
