import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { ActorRegistryStore } from "../actors/registry-store.js";
import { lockFile } from "../residency/file-lock.js";
import { compactTerminalRunEvents, pruneActorSessionBackups, retainedActorRunIds } from "./retention.js";
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

/** Offline one-time compaction, not a new collector/TTL. Existing host flocks must
 * exist and be claimable, diagnostic holders must be proven dead, and actor roots
 * must belong to those fenced resident roots. Live/legacy/unknown custody is skipped.
 * Dry-run never creates locks, markers or files (flock on an existing inode only). */
export const sweepMeshRetention = async (meshRoot: string, options: { dryRun?: boolean; now?: number } = {}) => {
  const dryRun = options.dryRun !== false;
  const changes: Array<{ path: string; beforeBytes: number; afterBytes: number }> = [];
  const skipped: Array<{ path: string; reason: string }> = [];
  const fences: number[] = [];
  const residents = new Map<string, string>();
  const ambiguous = new Set<string>();
  const registries: string[] = [];
  const discover = (root: string, depth = 0): void => {
    if (absent(root)) return;
    if (depth > 4 || !ownedStat(root)?.isDirectory()) throw new Error(`Actor references are uncertain: ${root}`);
    if (!absent(path.join(root, "actors.json"))) registries.push(root);
    for (const name of fs.readdirSync(root)) {
      if (["runs", "handoff-session", "child-completions"].includes(name)) continue;
      const child = path.join(root, name); const stat = ownedStat(child);
      if (!stat) throw new Error(`Actor references are uncertain: ${child}`);
      if (stat.isDirectory()) discover(child, depth + 1);
    }
  };
  if (!ownedStat(meshRoot)?.isDirectory()) throw new Error("Mesh root must be an owned directory");
  discover(path.join(meshRoot, "actors"));
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
    if (retained.has("*")) return { dryRun, changes, bytesBefore: 0, bytesAfter: 0, skipped: [...skipped, { path: meshRoot, reason: "unknown actor run references" }] };
    const compactRuns = (runs: string) => {
      if (retained.has("*")) return;
      for (const run of directories(runs)) if (!retained.has(path.basename(run))) {
        compactTerminalRunEvents(run, { ...options, dryRun, onCompact: change => changes.push(change),
          isRetained: () => { const current = retainedActorRunIds(registries); return current.has("*") || current.has(path.basename(run)); },
        });
      }
    };
    for (const root of residents.values()) compactRuns(path.join(root, "runs"));
    for (const registryRoot of residents.size ? registries : []) {
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
  } finally { for (const fd of fences) fs.closeSync(fd); }
  return { dryRun, changes, bytesBefore: changes.reduce((sum, item) => sum + item.beforeBytes, 0),
    bytesAfter: changes.reduce((sum, item) => sum + item.afterBytes, 0), skipped };
};

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const [root, mode = "--dry-run"] = process.argv.slice(2);
  if (!root || !["--dry-run", "--apply"].includes(mode) || process.argv.length > 4) {
    console.error("Usage: node dist/storage/retention-cli.js <mesh-root> [--dry-run|--apply]"); process.exitCode = 2;
  } else {
    try { console.log(JSON.stringify(await sweepMeshRetention(path.resolve(root), { dryRun: mode !== "--apply" }), null, 2)); }
    catch (error) { console.error(error); process.exitCode = 1; }
  }
}
