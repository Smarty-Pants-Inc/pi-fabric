import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { ActorRegistryStore } from "../actors/registry-store.js";
import { lockFile } from "../residency/file-lock.js";
import { isMeshStateMovedMarker, readMeshStateMovedMarker } from "../mesh/backend-fence.js";
import { meshStateDocumentProblem } from "../mesh/state-file.js";
import { setWindowsVerifiedRemoval, startRemovalTally, stopRemovalTally, windowsPrivate, windowsRemoveHelper, type DeleterTally } from "./verified-remove.js";
import { sweepResidentRuns } from "../residency/retention.js";
import { sweepResidentGenerationRecords } from "../residency/generation-retention.js";
import { compactTerminalRunEvents, pruneActorRunArchives, pruneActorSessionBackups, registryReferences } from "./retention.js";
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
  // Ownership of the mesh's files cannot be proven without a uid (Windows): --apply never deletes there (a dry run
  // still previews), whatever the marker says.
  // smarty-dev#7858: on Windows the verified helper (present and digest-verified) proves owner SIDs and removes by
  // handle; without it both gates stay closed.
  const windows = process.platform === "win32" && windowsRemoveHelper() !== undefined;
  if (typeof process.getuid !== "function" && !windows) return "file ownership cannot be proven on this platform; --apply deletes nothing";
  // Verified removal is directory-relative through /proc/self/fd (smarty-dev#7916), or the Windows helper; elsewhere
  // --apply stays off, as on main (smarty-dev#7858).
  if (process.platform !== "linux" && !windows) return "retention --apply needs verified removal (Linux, or the Windows helper; smarty-dev#7858); --apply deletes nothing";
  if (!absent(path.join(meshRoot, MESH_RETENTION_HOLD))) return `deletions are held (${MESH_RETENTION_HOLD})`;
  const doubt = meshStateDoubt(meshRoot);
  if (doubt !== undefined) return `the mesh state is unknown (${doubt}); --apply deletes nothing`;
  const moved = readMeshStateMovedMarker(meshRoot);
  if (moved === undefined) return undefined;
  let approved: unknown;
  try { approved = readPrivate(path.join(meshRoot, MESH_RETENTION_APPROVAL)).epoch; } catch { /* absent, unsafe or unreadable: not approved */ }
  return approved === moved.epoch ? undefined
    : `the mesh switched to ${moved.backend} at epoch ${moved.epoch}; deletions need a ruling in ${MESH_RETENTION_APPROVAL}`;
};
/** smarty-dev#7916 design 2: before any deletion state.json must be, in full, either the moved marker with our own
 * private state.db beside it, or a state document whose envelope and every entry validate. Absent, linked, foreign,
 * oversized, damaged or partly valid: doubt, and --apply refuses. */
// The gate runs before every deletion: a full parse is reused only while state.json keeps the same identity.
let validatedState: string | undefined;
const meshStateDoubt = (meshRoot: string): string | undefined => {
  const file = path.join(meshRoot, "state.json");
  const stamp = (stat: fs.Stats | undefined) => stat && `${file}:${stat.dev}:${stat.ino}:${stat.size}:${stat.mtimeMs}:${stat.ctimeMs}`;
  const before = stamp(ownedStat(file));
  if (before !== undefined && before === validatedState) return undefined;
  let value: unknown;
  try { value = readPrivate(file, 256 * 1024 * 1024); }
  catch (error) { return `state.json is absent, unsafe or not JSON: ${(error as Error).message}`; }
  if (isMeshStateMovedMarker(value)) {
    const db = ownedStat(path.join(meshRoot, "state.db"));
    return db?.isFile() && (db.mode & 0o022) === 0 ? undefined : "state.json is the moved marker but state.db is not our private file";
  }
  const problem = meshStateDocumentProblem(value);
  if (problem !== undefined) return `state.json: ${problem}`;
  if (before !== undefined && stamp(ownedStat(file)) === before) validatedState = before;
  return undefined;
};
const readPrivate = (file: string, maxBytes = 64 * 1024): Record<string, unknown> => {
  const fd = fs.openSync(file, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW | fs.constants.O_NONBLOCK);
  try {
    const stat = fs.fstatSync(fd);
    // Windows (smarty-dev#7858, pi-fabric#913): the verified helper proves on a handle that we own it and that no SID
    // but ours, SYSTEM or Administrators may write it (the DACL); POSIX mode bits mean nothing there.
    const owned = process.platform === "win32" ? windowsPrivate(file, fs.fstatSync(fd, { bigint: true }))
      : typeof process.getuid === "function" && stat.uid === process.getuid() && (stat.mode & 0o022) === 0;
    if (!stat.isFile() || stat.size > maxBytes || !owned) throw new Error(`Unsafe record: ${file}`);
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
/** The deleters --apply reaches; every one removes through verifiedRemove (smarty-dev#7916). A deleter added to this
 * sweep without it must stay dry-run, with the message below. */
export const MESH_RETENTION_APPLY_DISABLED = "retention --apply is disabled for this deleter until smarty-dev#7916 (verified-inode deletion); run --dry-run";
export interface MeshRetentionSummary {
  /** Per deleter: removed (dry run: would remove), bytes, refusals by reason (with up to 3 example paths). */
  deleters: Record<string, DeleterTally>;
  /** Underlying custody-gate doubt (hold, epoch, state.json, platform), or undefined. This is a preview diagnostic,
   * not apply permission: the exported boundary and CLI still unconditionally refuse apply until #10678. */
  applyRefusal?: string;
  /** Actor registries read by this sweep: count, total parses, the most parses of one registry (1 unless it changed). */
  registries: { count: number; parses: number; maxParsesPerRegistry: number; rootRewalks: number };
}
/** Stable code of {@link MeshRetentionApplyDisabledError}. */
export const MESH_RETENTION_APPLY_DISABLED_CODE = "ERR_MESH_RETENTION_APPLY_DISABLED";
/** sweepMeshRetention(meshRoot, { dryRun: false }) rejects with this, before it reads the mesh or arms any deleter:
 * apply stays disabled for every in-process caller (not only the CLI) until smarty-dev#10678 (owner-attributed
 * callback fencing) lands. There is no bypass; a dry run (the default) still previews. */
export class MeshRetentionApplyDisabledError extends Error {
  readonly code = MESH_RETENTION_APPLY_DISABLED_CODE;
  readonly issue = "smarty-dev#10678";
  constructor() {
    super("sweepMeshRetention: apply (dryRun: false) is disabled until smarty-dev#10678 (owner-attributed callback fencing) lands; nothing was read or changed. Use a dry run.");
    this.name = "MeshRetentionApplyDisabledError";
  }
}
// The last sweep's registry parse counts and root re-walks, for its summary (the sweep has many early returns).
let registryParses: ReadonlyMap<string, number> = new Map();
let registryRewalks = 0;
export const sweepMeshRetention = async (meshRoot: string, options: {
  dryRun?: boolean; now?: number;
  /** Remove terminal actor runs older than this, mesh-wide (the actor archive TTL). Unset: compaction only. */
  runRetentionMs?: number;
  /** Removal age of a proven-dead resident root's runs (default: the live host's 24 h). */
  residentRunRetentionMs?: number;
} = {}) => {
  // smarty-dev#10678 (pi-fabric#1014 SEC P2-1): refuse at the first statement. The private sweep pins every callback
  // to a literal dry run, so later reads of a caller-owned getter cannot enable an apply.
  if (options.dryRun === false) throw new MeshRetentionApplyDisabledError();
  const tally = startRemovalTally();
  registryParses = new Map(); registryRewalks = 0;
  // The operator's sweep removes through the Windows helper (smarty-dev#7858); in-process sweeps do not.
  setWindowsVerifiedRemoval(true);
  try {
    const result = await sweep(meshRoot, options, true);
    const parses = [...registryParses.values()];
    const summary: MeshRetentionSummary = { deleters: tally, registries: {
      count: parses.length, parses: parses.reduce((sum, count) => sum + count, 0), maxParsesPerRegistry: Math.max(0, ...parses),
      rootRewalks: registryRewalks } };
    if (ownedStat(meshRoot)?.isDirectory()) { const refusal = applyRefusal(meshRoot); if (refusal !== undefined) summary.applyRefusal = refusal; }
    return { ...result, summary };
  } finally { stopRemovalTally(); setWindowsVerifiedRemoval(false); }
};
/** The operator's lines: per deleter, then the one total (GiB) on its own line. */
export const summaryLines = (dryRun: boolean, summary: MeshRetentionSummary): string[] => {
  const lines: string[] = [];
  let bytes = 0;
  for (const [name, item] of Object.entries(summary.deleters).sort(([a], [b]) => a.localeCompare(b))) {
    bytes += item.bytes;
    const refused = Object.values(item.refused).reduce((sum, count) => sum + count, 0);
    lines.push(`${name}: ${dryRun ? "would delete" : "deleted"} ${item.removed} (${item.bytes} bytes), refused ${refused}`);
    for (const [reason, count] of Object.entries(item.refused)) {
      const held = item.refusedBytes[reason];
      lines.push(`  refused ${count}${held ? ` (${(held / 1024 ** 3).toFixed(3)} GiB)` : ""}: ${reason}`);
    }
  }
  if (summary.applyRefusal) lines.push(`--apply would refuse now: ${summary.applyRefusal}`);
  lines.push(`registries: ${summary.registries.count} read, ${summary.registries.parses} parses (at most ${summary.registries.maxParsesPerRegistry} per registry), roots re-walked ${summary.registries.rootRewalks} times`);
  lines.push(`${dryRun ? "WOULD_FREE_GIB" : "FREED_GIB"}=${(bytes / 1024 ** 3).toFixed(3)}`);
  return lines;
};
// dryRun is the literal true until smarty-dev#10678: the apply branches below are kept for that protocol, unreachable.
const sweep = async (meshRoot: string, options: {
  dryRun?: boolean; now?: number; runRetentionMs?: number; residentRunRetentionMs?: number;
}, dryRun: true) => {
  const now = options.now ?? Date.now();
  const removedRuns: string[] = [];
  const changes: Array<{ path: string; beforeBytes: number; afterBytes: number }> = [];
  const skipped: Array<{ path: string; reason: string }> = [];
  const residents = new Map<string, string>();
  const ambiguous = new Set<string>();
  const registries: string[] = [];
  // Any entry that is not ours, a link or hard-linked makes the registry roots uncertain. An entry that is gone between
  // the listing and its lstat is skipped (smarty-dev#9058) but counted: a walk that skipped one vetoes the candidate whose
  // check it serves, and the next check walks again (see checkedFresh).
  let vanished = 0;
  const discover = (root: string, depth = 0, found = registries): void => {
    if (absent(root)) { if (depth > 0) vanished++; return; }
    if (depth > 4 || !ownedStat(root)?.isDirectory()) throw new Error(`Actor references are uncertain: ${root}`);
    if (!absent(path.join(root, "actors.json"))) found.push(root);
    let names: string[];
    try { names = fs.readdirSync(root); }
    catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") { vanished++; return; } throw error; }
    for (const name of names) {
      if (["runs", "handoff-session", "child-completions"].includes(name)) continue;
      const child = path.join(root, name); const stat = ownedStat(child);
      if (!stat) { if (absent(child)) { vanished++; continue; } throw new Error(`Actor references are uncertain: ${child}`); }
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
  // #9160: each direct actors/ entry's lstat type, dev and ino (never mtime: actor activity must not stop the sweep),
  // taken before discovery so a swap during or after the walk differs. undefined: unknown listing or stat (fail closed).
  const entryKind = (stat: fs.Stats): string => stat.isDirectory() ? "directory" : stat.isFile() ? "file" : stat.isSymbolicLink() ? "symlink" : "other";
  // CODE-P1 (smarty-dev#9058): the project registry actors/actors.json is saved by ActorRegistryStore's renameAtomic, so a
  // normal save is a new inode. While it is a regular, single-link file owned by this uid, not writable by others, its identity is kind+dev+uid
  // (not the inode): such a generation is not a replacement. This proves no writer provenance; it accepts a safe regular
  // generation per the registry's own contract, and the stamp in every final check still vetoes an overlapping save. Any
  // other kind, owner or link count (or no getuid) keeps the exact inode identity, so that flip is a sticky replacement.
  // Windows: no uid/mode, so any registry change stops planning (fail closed); smarty-dev#7858
  // smarty-dev#976 B1: the project store's withLock dir actors/actors.json.lock is a new inode per generation. Only an
  // owned private actual directory, empty or holding only an owned private single-link regular `owner` with a valid
  // token/pid/time(/start[/label]) record read through O_NOFOLLOW, earns the kind+dev+uid role. Safe shape with missing, torn or
  // malformed content: undefined (unknown, no identity recorded). Any physically unsafe shape: the exact raw identity.
  const lockIdentity = (stat: fs.Stats): string | undefined | false => {
    const uid = typeof process.getuid === "function" ? process.getuid() : undefined;
    const lockDir = path.join(actorsRoot, "actors.json.lock");
    if (process.platform === "win32" || uid === undefined || !stat.isDirectory() || stat.uid !== uid || (stat.mode & 0o077) !== 0) return false;
    const names = fs.readdirSync(lockDir);
    if (names.some(name => name !== "owner")) return false;
    let record = "";
    if (names.length === 1) {
      const owner = fs.lstatSync(path.join(lockDir, "owner"), { throwIfNoEntry: false });
      if (owner !== undefined) {
        if (!owner.isFile() || owner.nlink !== 1 || owner.uid !== uid || (owner.mode & 0o077) !== 0) return false;
        let fd: number;
        try { fd = fs.openSync(path.join(lockDir, "owner"), fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW | fs.constants.O_NONBLOCK); }
        catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined; throw error; }
        try {
          const held = fs.fstatSync(fd);
          if (!held.isFile() || held.nlink !== 1 || held.uid !== uid || (held.mode & 0o077) !== 0) return false;
          if (held.dev !== owner.dev || held.ino !== owner.ino) return undefined;
          if (held.size > 256) return undefined;
          const buffer = Buffer.alloc(257), bytes = fs.readSync(fd, buffer, 0, buffer.length, 0);
          if (bytes !== held.size) return undefined; // a short read or a growth is unknown; never trust a prefix
          record = buffer.subarray(0, bytes).toString("utf8");
          const canonical = fs.lstatSync(path.join(lockDir, "owner"), { throwIfNoEntry: false });
          if (canonical && (!canonical.isFile() || canonical.nlink !== 1 || canonical.uid !== uid || (canonical.mode & 0o077) !== 0)) return false;
          if (!canonical || canonical.dev !== held.dev || canonical.ino !== held.ino || canonical.size !== held.size ||
              canonical.mtimeMs !== held.mtimeMs || canonical.ctimeMs !== held.ctimeMs || canonical.mode !== held.mode ||
              canonical.uid !== held.uid || canonical.nlink !== held.nlink) return undefined;
        } finally { fs.closeSync(fd); }
      }
    }
    const after = fs.lstatSync(lockDir, { throwIfNoEntry: false });
    if (after && (!after.isDirectory() || after.uid !== uid || (after.mode & 0o077) !== 0)) return false;
    if (!after || after.dev !== stat.dev || after.ino !== stat.ino || after.mode !== stat.mode || after.uid !== stat.uid ||
        after.mtimeMs !== stat.mtimeMs || after.ctimeMs !== stat.ctimeMs || after.nlink !== stat.nlink) return undefined;
    // ponytail (#8526): exact legacy 3/4 fields or 5 with a bounded ASCII label; empty start needs that label.
    // Absolute EOF keeps extra fields, including an extra empty field, unknown.
    const fields = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\n([1-9]\d*)\n(0|[1-9]\d*)\n(?:\d+\n|\d*\n[\x20-\x7e]{1,64}\n)?(?![\s\S])/.exec(record);
    return fields && Number.isSafeInteger(Number(fields[1])) && Number.isSafeInteger(Number(fields[2]))
      ? `directory\0${stat.dev}\0uid:${uid}\0project-registry-lock` : undefined;
  };
  const entryIdentity = (name: string, stat: fs.Stats): string | undefined => {
    if (name === "actors.json.lock") { const role = lockIdentity(stat); if (role !== false) return role; }
    return entryIdentityRaw(name, stat);
  };
  const entryIdentityRaw = (name: string, stat: fs.Stats): string =>
    name === "actors.json" && stat.isFile() && stat.nlink === 1 && (stat.mode & 0o022) === 0 &&
      typeof process.getuid === "function" && stat.uid === process.getuid()
      ? `file\0${stat.dev}\0uid:${stat.uid}\0project-registry`
      : `${entryKind(stat)}\0${stat.dev}\0${stat.ino}`;
  // #10635: a listed entry gone at its lstat (ENOENT) re-lists, at most 3 listings per call. Gone and staying gone is a
  // benign removal. Every identity seen within this call is kept and compared with all later reads: a different one is
  // a replacement (sticky `stop`, then undefined). A name that vanished before any identity was seen and comes back is judged by `replaced`
  // against the sweep's first-ever identities. Any other error, or a third listing with a vanished entry, is unknown.
  let stop: string | undefined; // sticky: set only by a replacement
  const readable = (identity: string): string => identity.split("\0").join("/");
  const entries = (): Map<string, string> | undefined => {
    const seen = new Map<string, string>();
    for (let attempt = 0; attempt < 3; attempt++) {
      try {
        if (absent(actorsRoot)) return new Map();
        const current = new Map<string, string>();
        let vanished = false;
        for (const name of fs.readdirSync(actorsRoot)) {
          let stat: fs.Stats;
          try { stat = fs.lstatSync(path.join(actorsRoot, name)); }
          catch (error) {
            if ((error as NodeJS.ErrnoException).code !== "ENOENT") return undefined;
            vanished = true; continue;
          }
          const identity = entryIdentity(name, stat);
          if (identity === undefined) return undefined; // a lock generation in progress: unknown, nothing recorded
          if (seen.has(name) && seen.get(name) !== identity) { stop ??= `an actors entry changed type or identity: ${name} ${readable(seen.get(name)!)} -> ${readable(identity)}`; return undefined; }
          seen.set(name, identity); current.set(name, identity);
        }
        if (!vanished) return current;
      } catch { return undefined; }
    }
    return undefined;
  };
  // smarty-dev#9058: every candidate is decided at its own final check, `checkedFresh` below, which surrounds the actual
  // refs.fresh() read. Only a replaced entry or a replaced/moved actors/ is sticky (#9160, #10635); any other change or
  // uncertainty keeps only the candidate whose check it overlaps, and the next check re-walks and recovers.
  // Canonical runtime registry roots are actors/actors.json and actors/<session>/actors.json (fabric-runtime-state.ts
  // `actorRoots`). This is not an exhaustive ownership claim: registries under supported non-canonical config/run-root
  // overrides are not attributed here, which stays unresolved under smarty-dev#10678 (this dry run frees nothing; apply
  // is refused). Over the canonical roots an observation is: actors/'s own identity, every direct entry's type/dev/ino, and an exact lstat stamp (or absence) of
  // actors.json in actors/ and in EVERY current direct directory. Directory timestamps are not used (a coarse tick hides a
  // create). It never stats the ~10k actor homes below (smarty-dev#7916); only a changed name or presence re-walks.
  const rootIdentity = (): string | undefined => {
    try {
      const stat = fs.lstatSync(actorsRoot, { throwIfNoEntry: false });
      return stat === undefined ? "absent" : ownedStat(actorsRoot)?.isDirectory() ? `${stat.dev}\0${stat.ino}` : undefined;
    } catch { return undefined; }
  };
  let firstRoot = rootIdentity();
  const listing = entries();
  if (!listing || firstRoot === undefined) throw new Error(`Actor references are uncertain: ${actorsRoot}`);
  // First identity ever seen per name, never dropped: a same-named entry that differs (even after a removal the
  // re-walk accepted) stops the sweep. Only new names may be absorbed by a re-walk.
  const identities = new Map(listing);
  const replaced = (current: Map<string, string>): string | undefined => {
    const found = [...current].find(([name, identity]) => identities.has(name) && identities.get(name) !== identity);
    return found && `an actors entry changed type or identity: ${found[0]} ${readable(identities.get(found[0])!)} -> ${readable(found[1])}`;
  };
  const stampOf = (file: string): string | undefined => {
    const stat = fs.lstatSync(file, { throwIfNoEntry: false });
    return stat && `${entryKind(stat)}:${stat.dev}:${stat.ino}:${stat.size}:${stat.mtimeMs}:${stat.ctimeMs}:${stat.nlink}:${stat.uid}`;
  };
  /** The observation, or why it is unknown (with `stop` set when that is a replacement). */
  const observe = (): { key: string; full: string; current: Map<string, string> } | string => {
    const root = rootIdentity();
    if (root !== undefined && firstRoot !== "absent" && root !== firstRoot) return stop ??= "the actors directory was replaced or moved";
    const current = entries();
    if (root === undefined || !current) return "the actors listing is unknown";
    const change = replaced(current);
    if (change !== undefined) return stop ??= change;
    if (firstRoot === "absent" && root !== "absent") firstRoot = root;
    try {
      const sorted = [...current].sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0);
      const probes = [actorsRoot, ...sorted.filter(([, identity]) => identity.startsWith("directory\0")).map(([name]) => path.join(actorsRoot, name))]
        .map(directory => [directory, stampOf(path.join(directory, "actors.json"))] as const);
      const key = [root, ...sorted.map(entry => entry.join("\0")), ...probes.map(([directory, stamp]) => `${directory}\0${stamp !== undefined}`)].join("\n");
      // The raw lock-dir and owner generation (B1): the role key never hides a physical replacement within a bracket.
      const lock = path.join(actorsRoot, "actors.json.lock");
      const raw = current.has("actors.json.lock") ? `\n${stampOf(lock) ?? ""}\n${stampOf(path.join(lock, "owner")) ?? ""}` : "";
      return { key, full: `${key}\n${probes.map(([, stamp]) => stamp ?? "").join("\n")}${raw}`, current };
    } catch (error) { return `an actors.json probe failed: ${(error as Error).message}`; }
  };
  const first = observe();
  discover(actorsRoot);
  // Registries ever read: one whose directory is still present but whose actors.json is missing stays in the read set,
  // so requireRegistries vetoes instead of reading it as empty.
  const known = new Set(registries);
  // Every registry parsed once for the whole sweep (smarty-dev#7916); a final check re-stats each registry and
  // re-parses only one whose actors.json changed. The early-skip snapshot is the first read's ids without "*": never a
  // later read, so one uncertain check cannot skip later candidates before their own final check.
  const refs = registryReferences(registries, { requireRegistries: true });
  const early = new Set([...refs.snapshot()].filter(id => id !== "*"));
  // The tree the read set was discovered from; undefined forces a re-walk (unknown, or a walk that skipped a vanished entry).
  let adopted = typeof first === "string" || vanished > 0 ? undefined : first.key;
  const reported = new Set<string>();
  const veto = (reason: string): Set<string> => {
    if (!reported.has(reason)) { reported.add(reason); skipped.push({ path: actorsRoot, reason }); }
    return new Set(["*"]);
  };
  /** The final check of one candidate: the closed world before, the read, the same world after. `owner`: the registry that
   * must be in the read set and unchanged throughout (the candidate's own). */
  const unknown = (reason: string): Set<string> => stop !== undefined
    ? veto(`actor registry roots changed during the sweep (${stop}); no further run is deleted`)
    : veto(`actor registry roots are uncertain (${reason}); that candidate is kept`);
  // smarty-dev#976 A: an own registry that is our regular, single-link, private file.
  const ownerSafe = (file: string): boolean => {
    const stat = ownedStat(file);
    return stat?.isFile() === true && stat.nlink === 1 &&
      (typeof process.getuid !== "function" || (stat.mode & 0o022) === 0);
  };
  // smarty-dev#976 A: at most 3 complete observations; only an otherwise-safe raw overlap (post.full !== pre.full) retries.
  // Every concrete id read on any attempt is kept (a reference removed before a stable attempt still keeps).
  const checkedFresh = (owner?: string): Set<string> => {
    const union = new Set<string>();
    for (let attempt = 0; attempt < 3; attempt++) {
    if (stop !== undefined) return unknown(stop);
    const pre = observe();
    if (typeof pre === "string") return unknown(pre);
    try {
      if (pre.key !== adopted) {
        adopted = undefined;
        const found: string[] = [], before = vanished;
        try { discover(actorsRoot, 0, found); }
        catch (error) { return unknown((error as Error).message); }
        if (vanished !== before) return veto("an actors entry vanished during the registry walk; that candidate is kept");
        for (const root of known) if (!found.includes(root) && !absent(root)) found.push(root);
        for (const root of found) known.add(root);
        registries.splice(0, registries.length, ...found); registryRewalks++;
        for (const [name, identity] of pre.current) if (!identities.has(name)) identities.set(name, identity);
        adopted = pre.key;
      }
      const ownFile = owner === undefined ? undefined : path.join(owner, "actors.json");
      const ownMissing = "a candidate's own actor registry is missing or changed; that candidate is kept";
      const ownerBefore = ownFile === undefined ? undefined : stampOf(ownFile);
      if (ownFile !== undefined && (!registries.includes(owner!) || ownerBefore === undefined || !ownerSafe(ownFile))) return veto(ownMissing);
      const ids = refs.fresh();
      if (ids.has("*")) return ids;
      for (const id of ids) union.add(id);
      const post = observe();
      if (typeof post === "string") return unknown(post);
      const ownerAfter = ownFile === undefined ? undefined : stampOf(ownFile);
      if (ownFile !== undefined && (!registries.includes(owner!) || ownerAfter === undefined || !ownerSafe(ownFile))) return veto(ownMissing);
      if (post.full === pre.full) return ownerAfter !== ownerBefore ? veto(ownMissing) : union;
    } catch (error) { return veto(`actor registry check failed (${(error as Error).message}); that candidate is kept`); }
    }
    return veto("actor registries changed during a final check; that candidate is kept");
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
    registryParses = refs.parses;
    // The one wrapped reference set, shared by the actor pass and sweepResidentRuns (whose isRetained precedes its fresh()).
    const checked = { ...refs, snapshot: () => early, fresh: () => checkedFresh() };
    const referencedNow = (id: string, owner?: string): boolean => {
      const current = checkedFresh(owner);
      return current.has("*") || current.has(id);
    };
    const registryRows = (registryRoot: string): Record<string, any>[] => {
      const rows = refs.actors(registryRoot);
      if (!rows) throw new Error("unreadable actor registry");
      return rows;
    };
    const compactRuns = (runs: string, veto: () => boolean = gate, owner?: string) => {
      for (const run of directories(runs)) if (!early.has(path.basename(run))) {
        compactTerminalRunEvents(run, { ...options, dryRun, onCompact: change => changes.push(change),
          isRetained: () => veto() || referencedNow(path.basename(run), owner),
        });
      }
    };
    for (const [rootId, root] of residents) await fenced(rootId, root, veto => {
      // Always preview stale diagnostic records under the dead host's own flock fence; apply remains unreachable.
      sweepResidentGenerationRecords(root, now, veto, true, change => changes.push(change));
      // The dead host's own startup sweep, under its flock fence: the same exit, tree,
      // preserved-result and latest-run fences as a live host's removal (smarty-dev#3252).
      if (options.runRetentionMs !== undefined) {
        // Same final pre-delete check as the actor-run pass below: before each delete every discovered registry's
        // stamp is re-validated against the cached references and a changed registry is re-parsed; a vanished or unreadable one vetoes (review round 2).
        // A dry run takes the same selection and fences without mutating, so its preview lists
        // the runs --apply deletes (review round 3). Sizes are taken before the final check, which also
        // re-discovers the registry roots, and the check and delete run back to back (review round 4).
        removedRuns.push(...sweepResidentRuns(path.join(root, "runs"), now, 10 * 60 * 1_000, {
          actorRoots: registries, references: checked, retainRuns: false, requireRegistries: true, dryRun, acceptPidReuse: true,
          isRetained: () => veto(), measure: treeBytes,
          onRemove: (run, bytes) => changes.push({ path: run, beforeBytes: bytes, afterBytes: 0 }),
          onCompact: change => changes.push(change),
          ...(options.residentRunRetentionMs !== undefined ? { retentionMs: options.residentRunRetentionMs } : {}),
        }));
      } else compactRuns(path.join(root, "runs"), veto);
    });
    // With runRetentionMs the mesh-wide pass below covers these actors (and every other one).
    // Copies: checkedFresh replaces the list's contents during the sweep.
    for (const registryRoot of residents.size && options.runRetentionMs === undefined ? [...registries] : []) {
      // One resident root at a time, under that root's own brief fence (smarty-dev#7766).
      const prune = (rootId: string, veto: () => boolean) => {
        // Match ActorRegistryStore/retainedActorRunIds: owned registries contain
        // instructions and message history and have no byte-size protocol limit.
        // Diagnostic records keep the bounded reader above.
        for (const actor of registryRows(registryRoot)) {
          if (!actor || actor.rootId !== rootId || actor.removal !== undefined ||
              typeof actor.id !== "string" || !/^[A-Za-z0-9_-]+$/.test(actor.id) || typeof actor.sessionFile !== "string" ||
              path.resolve(actor.sessionFile) !== path.join(path.resolve(registryRoot), actor.id, "session.jsonl")) continue;
          const actorRoot = path.dirname(actor.sessionFile);
          if (!ownedStat(actorRoot)?.isDirectory()) continue;
          compactRuns(path.join(actorRoot, "runs"), veto, registryRoot);
          // Pending launch/drain/removal is not a joined writer receipt for session history.
          if (actor.inFlightRun !== undefined || actor.preparing !== undefined || !["idle", "stopped"].includes(actor.status)) continue;
          pruneActorSessionBackups(actor.sessionFile, { dryRun, stop: veto, onPrune: change => changes.push({ path: change.path, beforeBytes: change.bytes, afterBytes: 0 }) });
        }
      };
      let rootIds: Set<string>;
      try {
        rootIds = new Set(registryRows(registryRoot).map((actor: Record<string, unknown> | null) => actor?.rootId)
          .filter((rootId: unknown): rootId is string => typeof rootId === "string" && residents.has(rootId)));
      } catch (error) { skipped.push({ path: registryRoot, reason: String(error) }); continue; }
      for (const rootId of rootIds) await fenced(rootId, residents.get(rootId)!, async veto => {
        try {
          if (!absent(path.join(registryRoot, "actors.json.lock"))) throw new Error("actor registry has a holder or uncertain lock");
          if (dryRun) prune(rootId, veto); else await new ActorRegistryStore(registryRoot).withLock(() => prune(rootId, veto), undefined, "retention");
        } catch (error) { skipped.push({ path: registryRoot, reason: String(error) }); }
      });
    }
    if (options.runRetentionMs !== undefined) {
      // Mesh-wide, independent of any owner being alive (smarty-dev#3252, #5652): the owning
      // manager's own pruneRuns rule (terminal, safe exit-proven tree, not the latest run),
      // plus every registry's latest, in-flight, preparing and pending-removal runs, re-read when a
      // registry changes. The final pre-delete check stamp-validates the cached references and re-parses changed registries;
      // an unreadable or vanished registry is a wildcard veto (pi-fabric#645 review round 1).
      // Early skips judge only from the first read's ids; the final pre-delete check is referencedNow with this registry
      // as the candidate's own (smarty-dev#7916, #9058).
      for (const registryRoot of [...registries]) {
        try {
          const rows = new Map<string, Record<string, any>>();
          for (const actor of registryRows(registryRoot)) if (actor && typeof actor.id === "string") rows.set(actor.id, actor);
          for (const actorRoot of directories(registryRoot)) {
            const id = path.basename(actorRoot), row = rows.get(id);
            // Removal (pending or accepted) owns its runs until it finishes; so does an offline removal, which moves the
            // whole actor directory into its archive (smarty-dev#9058: its runs leave actors/ with it).
            if (!/^[A-Za-z0-9_-]+$/.test(id) || row?.removal !== undefined || !absent(path.join(registryRoot, `removal-${id}.json`)) ||
                !absent(path.join(registryRoot, `offline-removal-${id}.json`))) continue;
            const inFlight = typeof row?.inFlightRun?.id === "string" ? row.inFlightRun.id : undefined;
            pruneActorRunArchives({
              runsDirectory: path.join(actorRoot, "runs"), retentionMs: options.runRetentionMs, now, dryRun, acceptPidReuse: true,
              ...(typeof row?.lastRunId === "string" ? { latestRunId: row.lastRunId } : {}),
              isRetained: (run, final) => gate() || run === inFlight || (final ? referencedNow(run, registryRoot) : early.has(run)),
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
  // Helper verification alone does not prove every callback's owner/reference fence. --apply stays refused until
  // the wider protocol in smarty-dev#10678 lands (scope cut from smarty-dev#7916 PR-b).
  const runRetentionMs = value === undefined ? undefined : Number(value);
  if (!root || !["--dry-run", "--apply"].includes(mode) || args.length > 2 || report === "" ||
      (runRetentionMs !== undefined && (!Number.isSafeInteger(runRetentionMs) || runRetentionMs < 60 * 60 * 1_000))) {
    console.error("Usage: node dist/storage/retention-cli.js <mesh-root> [--dry-run] [--runs-older-than <ms, >= 1 h>] [--report <file>]"); process.exitCode = 2;
  } else if (mode === "--apply") {
    console.error("retention-cli: --apply is disabled until smarty-dev#10678 (owner-attributed callback fencing) lands; nothing was changed. Use --dry-run.");
    process.exitCode = 2;
  } else {
    try {
      const result = await sweepMeshRetention(path.resolve(root), {
        dryRun: mode !== "--apply", ...(runRetentionMs !== undefined ? { runRetentionMs } : {}),
      });
      const text = JSON.stringify(result, null, 2);
      if (report !== undefined) writeReport(path.resolve(report), `${text}\n`); else console.log(text);
      // The operator's summary, on stderr so stdout stays one JSON document.
      for (const line of summaryLines(result.dryRun, result.summary)) console.error(line);
    }
    catch (error) { console.error(error); process.exitCode = 1; }
  }
}
