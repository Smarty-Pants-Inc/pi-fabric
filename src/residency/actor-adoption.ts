import { randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { writeJsonAtomic } from "../core/atomic-write.js";
import { ActorRegistryStore } from "../actors/registry-store.js";

/**
 * smarty-dev#5919: crash-safe custody move of one durable actor from a dead root's
 * registry into a live root's registry. The intent file is written before any
 * mutation; the first registry write is the commit point. Recovery inspects the
 * registries, never trusts the recorded phase alone:
 * - target names the adopted row: committed; finish source cleanup;
 * - only the source names the dead lineage: uncommitted; roll back the copy;
 * - neither names it (crash between remove and add): roll forward from the snapshot.
 * At every crash point exactly one registry row, or the intent snapshot, names the actor.
 */
export type ActorAdoptionPhase = "prepared" | "copied" | "removed" | "added";
export type ActorAdoptionScope = "project" | "session";

/**
 * The intent names roots only by id and scope. Recovery never takes a filesystem path from
 * the intent file: both actor roots come from the resolver, which derives them from the
 * config-verified resident configurations (this host's own root and the dead root's).
 */
export interface ActorAdoptionIntent {
  format: 1;
  actorId: string;
  fromRootId: string;
  intoRootId: string;
  scope: ActorAdoptionScope;
  /** The adopted row (rootId = intoRootId), committed to the target registry. */
  row: Record<string, unknown> & { id: string };
  phase: ActorAdoptionPhase;
  createdAt: number;
}

export interface ActorAdoptionRoots { sourceActorRoot: string; targetActorRoot: string }

/** Configured actor roots per scope for a (dead root, live root) pair; throws when either root is not config-verified. */
export type ActorAdoptionRootResolver = (fromRootId: string, intoRootId: string) => Record<ActorAdoptionScope, ActorAdoptionRoots>;

/** A tampered, foreign or unverifiable adoption intent: refused before any registry or filesystem operation. */
export class ActorAdoptionIntentError extends Error {
  override readonly name = "ActorAdoptionIntentError";
}

export interface ActorAdoptionMove {
  actorId: string;
  fromRootId: string;
  intoRootId: string;
  scope: ActorAdoptionScope;
  intentFile: string;
  /** The config-verified roots; the move and any recovery use only these. */
  resolveRoots: ActorAdoptionRootResolver;
  /** Re-run under the registry fences immediately before the commit; throw to refuse. */
  check?: (row: Record<string, unknown> & { id: string }) => void;
  /** Test hook: throwing here simulates a crash right after the named phase. */
  fault?: (phase: ActorAdoptionPhase) => void;
}

export type ActorAdoptionRecovery = "none" | "rolledBack" | "rolledForward" | "completed";

export const actorAdoptionIntentPath = (residencyRoot: string, actorId: string): string =>
  path.join(residencyRoot, "adoptions", `${actorId}.json`);

const ACTOR_ID = /^[a-f0-9]{32}$/;
const SCOPES: readonly ActorAdoptionScope[] = ["project", "session"];

/** Resolve, then realpath the deepest existing ancestor (an actor root may not exist yet). */
export const canonicalActorRoot = (root: string): string => {
  const resolved = path.resolve(root);
  const rest: string[] = [];
  let existing = resolved;
  for (;;) {
    try { return path.join(fs.realpathSync.native(existing), ...rest.reverse()); }
    catch (error) {
      const code = (error as NodeJS.ErrnoException).code;
      if (code !== "ENOENT" && code !== "ENOTDIR") throw error;
      const parent = path.dirname(existing);
      if (parent === existing) return resolved;
      rest.push(path.basename(existing));
      existing = parent;
    }
  }
};

export const adoptedActorRow = (row: Record<string, unknown> & { id: string }, fromRootId: string,
  intoRootId: string, now = Date.now()): Record<string, unknown> & { id: string } => {
  const earlier = Array.isArray(row.adoptedFrom) ? row.adoptedFrom.filter((root): root is string => typeof root === "string") : [];
  return { ...row, rootId: intoRootId,
    adoptedFrom: [...new Set([fromRootId, ...earlier])].filter((root) => root !== intoRootId),
    adoptedAt: now, updatedAt: now };
};

const samePath = (a: string, b: string): boolean => path.resolve(a) === path.resolve(b);

const writeIntent = (file: string, intent: ActorAdoptionIntent): void => {
  fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
  writeJsonAtomic(file, intent, { space: 2, durable: true });
};

const assertIntentFile = (file: string, actorId: unknown): void => {
  if (typeof actorId !== "string" || !ACTOR_ID.test(actorId)) {
    throw new ActorAdoptionIntentError(`Invalid actor id in adoption intent ${file}`);
  }
  if (path.basename(file) !== `${actorId}.json`) {
    throw new ActorAdoptionIntentError(`Adoption intent ${file} does not match its adoption id ${actorId}`);
  }
};

const verifiedRoots = (file: string, resolveRoots: ActorAdoptionRootResolver, fromRootId: string, intoRootId: string) => {
  let roots: Record<ActorAdoptionScope, ActorAdoptionRoots>;
  try { roots = resolveRoots(fromRootId, intoRootId); }
  catch (error) {
    if (error instanceof ActorAdoptionIntentError) throw error;
    throw new ActorAdoptionIntentError(`Adoption intent ${file} names roots that are not config-verified: ${error instanceof Error ? error.message : String(error)}`);
  }
  return roots;
};

/** A recorded legacy path equals the configured root exactly (as recorded: resolved, or its realpath). */
const legacyMatches = (recorded: unknown, configured: string): boolean =>
  typeof recorded === "string" && (recorded === path.resolve(configured) || recorded === canonicalActorRoot(configured));

interface VerifiedIntent { intent: ActorAdoptionIntent; roots: ActorAdoptionRoots }

/** Every check runs before any registry or filesystem operation; any mismatch fails closed. */
const readIntent = (file: string, resolveRoots: ActorAdoptionRootResolver): VerifiedIntent | undefined => {
  let parsed: ActorAdoptionIntent & { sourceActorRoot?: unknown; targetActorRoot?: unknown; scope?: unknown };
  try { parsed = JSON.parse(fs.readFileSync(file, "utf8")) as typeof parsed; }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
    throw new ActorAdoptionIntentError(`Unreadable actor adoption intent ${file}: ${error instanceof Error ? error.message : String(error)}`);
  }
  if (typeof parsed !== "object" || parsed === null) throw new ActorAdoptionIntentError(`Invalid actor adoption intent ${file}`);
  assertIntentFile(file, parsed.actorId);
  if (parsed.format !== 1 || typeof parsed.fromRootId !== "string" || !parsed.fromRootId ||
      typeof parsed.intoRootId !== "string" || !parsed.intoRootId || parsed.fromRootId === parsed.intoRootId ||
      typeof parsed.row !== "object" || parsed.row === null || parsed.row.id !== parsed.actorId ||
      parsed.row.rootId !== parsed.intoRootId) {
    throw new ActorAdoptionIntentError(`Invalid actor adoption intent ${file}`);
  }
  const roots = verifiedRoots(file, resolveRoots, parsed.fromRootId, parsed.intoRootId);
  const legacy = parsed.sourceActorRoot !== undefined || parsed.targetActorRoot !== undefined;
  const matches = (scope: ActorAdoptionScope) => legacyMatches(parsed.sourceActorRoot, roots[scope].sourceActorRoot) &&
    legacyMatches(parsed.targetActorRoot, roots[scope].targetActorRoot);
  let scope: ActorAdoptionScope | undefined;
  if (parsed.scope !== undefined) {
    if (!SCOPES.includes(parsed.scope as ActorAdoptionScope)) throw new ActorAdoptionIntentError(`Invalid adoption scope in ${file}`);
    scope = parsed.scope as ActorAdoptionScope;
    if (legacy && !matches(scope)) throw new ActorAdoptionIntentError(`Adoption intent ${file} names actor roots other than the configured ones`);
  } else {
    // A pre-fix intent recorded paths: accepted only when they are exactly the configured roots.
    if (!legacy) throw new ActorAdoptionIntentError(`Adoption intent ${file} names no scope`);
    scope = SCOPES.find(matches);
    if (!scope) throw new ActorAdoptionIntentError(`Adoption intent ${file} names actor roots other than the configured ones`);
  }
  const { sourceActorRoot: _source, targetActorRoot: _target, ...rest } = parsed;
  return { intent: { ...rest, scope }, roots: {
    sourceActorRoot: canonicalActorRoot(roots[scope].sourceActorRoot),
    targetActorRoot: canonicalActorRoot(roots[scope].targetActorRoot) } };
};

const copyActorDirectory = (source: string, target: string): void => {
  if (!fs.existsSync(source)) return;
  const staging = `${target}.adopting-${randomUUID()}`;
  fs.cpSync(source, staging, { recursive: true, errorOnExist: true, force: false, preserveTimestamps: true });
  fs.renameSync(staging, target);
};

const removeStaging = (target: string): void => {
  const parent = path.dirname(target), prefix = `${path.basename(target)}.adopting-`;
  let names: string[] = [];
  try { names = fs.readdirSync(parent); } catch { return; }
  for (const name of names) if (name.startsWith(prefix)) fs.rmSync(path.join(parent, name), { recursive: true, force: true });
};

/** Call with both registry fences held. */
const recoverLocked = (intentFile: string, intent: ActorAdoptionIntent, roots: ActorAdoptionRoots, source: ActorRegistryStore,
  target: ActorRegistryStore): ActorAdoptionRecovery => {
  const id = intent.actorId;
  const shared = samePath(roots.sourceActorRoot, roots.targetActorRoot);
  const sourceRows = source.records();
  const targetRows = shared ? sourceRows : target.records();
  const sourceRow = sourceRows.find((row) => row.id === id);
  const targetRow = targetRows.find((row) => row.id === id);
  const targetDir = path.join(roots.targetActorRoot, id), sourceDir = path.join(roots.sourceActorRoot, id);
  let outcome: ActorAdoptionRecovery;
  if (targetRow?.rootId === intent.intoRootId) {
    // Committed. A source duplicate of the dead lineage must never run again.
    if (!shared && sourceRow) source.write(sourceRows.filter((row) => row.id !== id), { durable: true });
    outcome = "completed";
  } else if (sourceRow?.rootId === intent.fromRootId && (shared || !targetRow)) {
    // Not committed: the dead lineage still owns its row. Undo only our own copy.
    if (!shared) { removeStaging(targetDir); fs.rmSync(targetDir, { recursive: true, force: true }); }
    fs.rmSync(intentFile, { force: true });
    return "rolledBack";
  } else if (!sourceRow && !targetRow) {
    // A crash between remove and add: the intent snapshot is the only copy. Roll forward.
    if (!fs.existsSync(targetDir) && !shared) copyActorDirectory(sourceDir, targetDir);
    target.write([...targetRows, intent.row], { durable: true });
    outcome = "rolledForward";
  } else {
    throw new Error(`Actor adoption intent for ${id} conflicts with the registries; manual repair required (${intentFile})`);
  }
  if (!shared) { removeStaging(targetDir); fs.rmSync(sourceDir, { recursive: true, force: true }); }
  fs.rmSync(intentFile, { force: true });
  return outcome;
};

const stores = (roots: ActorAdoptionRoots) => {
  const source = new ActorRegistryStore(roots.sourceActorRoot);
  const target = samePath(roots.sourceActorRoot, roots.targetActorRoot) ? source : new ActorRegistryStore(roots.targetActorRoot);
  return { source, target };
};

/** Finish or undo an interrupted move. Safe to run any number of times. */
export const recoverActorAdoption = async (intentFile: string, resolveRoots: ActorAdoptionRootResolver): Promise<ActorAdoptionRecovery> => {
  const pending = readIntent(intentFile, resolveRoots);
  if (!pending) return "none";
  const { source, target } = stores(pending.roots);
  return ActorRegistryStore.withLocks([source, target], () => {
    const current = readIntent(intentFile, resolveRoots);
    if (!current) return "none";
    if (!samePath(current.roots.sourceActorRoot, pending.roots.sourceActorRoot) ||
        !samePath(current.roots.targetActorRoot, pending.roots.targetActorRoot)) {
      throw new ActorAdoptionIntentError(`Adoption intent ${intentFile} changed its roots under the registry fences`);
    }
    return recoverLocked(intentFile, current.intent, current.roots, source, target);
  });
};

/** Recover every intent under a residency root's adoptions directory. */
export const recoverActorAdoptions = async (residencyRoot: string,
  resolveRoots: ActorAdoptionRootResolver): Promise<Record<string, ActorAdoptionRecovery>> => {
  const directory = path.join(residencyRoot, "adoptions");
  let names: string[] = [];
  try { names = fs.readdirSync(directory); } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return {};
    throw error;
  }
  const results: Record<string, ActorAdoptionRecovery> = {};
  for (const name of names.filter((entry) => /^[a-f0-9]{32}\.json$/.test(entry)).sort()) {
    results[name.slice(0, -5)] = await recoverActorAdoption(path.join(directory, name), resolveRoots);
  }
  return results;
};

/** Two-phase custody move (see module comment). Returns the committed adopted row. */
export const moveActorCustody = async (move: ActorAdoptionMove): Promise<Record<string, unknown> & { id: string }> => {
  if (!ACTOR_ID.test(move.actorId)) throw new Error(`Invalid Fabric actor id: ${move.actorId}`);
  if (move.fromRootId === move.intoRootId) throw new Error("An actor cannot be adopted into its own root");
  if (!SCOPES.includes(move.scope)) throw new Error(`Invalid adoption scope: ${String(move.scope)}`);
  assertIntentFile(move.intentFile, move.actorId);
  await recoverActorAdoption(move.intentFile, move.resolveRoots);
  const configured = verifiedRoots(move.intentFile, move.resolveRoots, move.fromRootId, move.intoRootId)[move.scope];
  const roots: ActorAdoptionRoots = { sourceActorRoot: canonicalActorRoot(configured.sourceActorRoot),
    targetActorRoot: canonicalActorRoot(configured.targetActorRoot) };
  const { source, target } = stores(roots);
  const shared = source === target;
  return ActorRegistryStore.withLocks([source, target], () => {
    const id = move.actorId;
    const sourceRows = source.records();
    const row = sourceRows.find((candidate) => candidate.id === id);
    if (!row) throw new Error(`Unknown Fabric actor in the dead root's registry: ${id}`);
    if (row.rootId !== move.fromRootId) {
      throw new Error(`Fabric actor ${id} belongs to ${String(row.rootId)}, not to the dead root ${move.fromRootId}`);
    }
    if (row.residency !== "durable") throw new Error(`Fabric actor ${id} is not durable`);
    if (row.removal !== undefined) throw new Error(`Fabric actor ${id} has a pending removal`);
    const targetRows = shared ? sourceRows : target.records();
    const targetDir = path.join(roots.targetActorRoot, id), sourceDir = path.join(roots.sourceActorRoot, id);
    if (!shared && (targetRows.some((candidate) => candidate.id === id) || fs.existsSync(targetDir))) {
      throw new Error(`The live root's registry already holds Fabric actor ${id}`);
    }
    move.check?.(row);
    const adopted = adoptedActorRow(row, move.fromRootId, move.intoRootId);
    // Root ids and scope only: never a path a later recovery could be steered by.
    const intent: ActorAdoptionIntent = { format: 1, actorId: id, fromRootId: move.fromRootId,
      intoRootId: move.intoRootId, scope: move.scope, row: adopted, phase: "prepared", createdAt: Date.now() };
    writeIntent(move.intentFile, intent);
    move.fault?.("prepared");
    if (!shared) {
      copyActorDirectory(sourceDir, targetDir);
      writeIntent(move.intentFile, { ...intent, phase: "copied" });
      move.fault?.("copied");
      source.write(sourceRows.filter((candidate) => candidate.id !== id), { durable: true });
      writeIntent(move.intentFile, { ...intent, phase: "removed" });
      move.fault?.("removed");
      target.write([...targetRows, adopted], { durable: true });
    } else {
      // One registry: remove and add are a single atomic rewrite of the row.
      source.write(sourceRows.map((candidate) => candidate.id === id ? adopted : candidate), { durable: true });
    }
    writeIntent(move.intentFile, { ...intent, phase: "added" });
    move.fault?.("added");
    if (!shared) fs.rmSync(sourceDir, { recursive: true, force: true });
    fs.rmSync(move.intentFile, { force: true });
    return adopted;
  });
};
