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

export interface ActorAdoptionIntent {
  format: 1;
  actorId: string;
  fromRootId: string;
  intoRootId: string;
  sourceActorRoot: string;
  targetActorRoot: string;
  /** The adopted row (rootId = intoRootId), committed to the target registry. */
  row: Record<string, unknown> & { id: string };
  phase: ActorAdoptionPhase;
  createdAt: number;
}

export interface ActorAdoptionMove {
  actorId: string;
  fromRootId: string;
  intoRootId: string;
  sourceActorRoot: string;
  targetActorRoot: string;
  intentFile: string;
  /** Re-run under the registry fences immediately before the commit; throw to refuse. */
  check?: (row: Record<string, unknown> & { id: string }) => void;
  /** Test hook: throwing here simulates a crash right after the named phase. */
  fault?: (phase: ActorAdoptionPhase) => void;
}

export type ActorAdoptionRecovery = "none" | "rolledBack" | "rolledForward" | "completed";

export const actorAdoptionIntentPath = (residencyRoot: string, actorId: string): string =>
  path.join(residencyRoot, "adoptions", `${actorId}.json`);

const ACTOR_ID = /^[a-f0-9]{32}$/;

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

const readIntent = (file: string): ActorAdoptionIntent | undefined => {
  let parsed: ActorAdoptionIntent;
  try { parsed = JSON.parse(fs.readFileSync(file, "utf8")) as ActorAdoptionIntent; }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
    throw new Error(`Unreadable actor adoption intent ${file}: ${error instanceof Error ? error.message : String(error)}`);
  }
  if (parsed?.format !== 1 || typeof parsed.actorId !== "string" || !ACTOR_ID.test(parsed.actorId) ||
      typeof parsed.fromRootId !== "string" || typeof parsed.intoRootId !== "string" ||
      typeof parsed.sourceActorRoot !== "string" || typeof parsed.targetActorRoot !== "string" ||
      typeof parsed.row !== "object" || parsed.row === null || parsed.row.id !== parsed.actorId ||
      parsed.row.rootId !== parsed.intoRootId) {
    throw new Error(`Invalid actor adoption intent ${file}`);
  }
  return parsed;
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
const recoverLocked = (intentFile: string, intent: ActorAdoptionIntent, source: ActorRegistryStore,
  target: ActorRegistryStore): ActorAdoptionRecovery => {
  const id = intent.actorId;
  const shared = samePath(intent.sourceActorRoot, intent.targetActorRoot);
  const sourceRows = source.records();
  const targetRows = shared ? sourceRows : target.records();
  const sourceRow = sourceRows.find((row) => row.id === id);
  const targetRow = targetRows.find((row) => row.id === id);
  const targetDir = path.join(intent.targetActorRoot, id), sourceDir = path.join(intent.sourceActorRoot, id);
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

const stores = (intent: Pick<ActorAdoptionIntent, "sourceActorRoot" | "targetActorRoot">) => {
  const source = new ActorRegistryStore(intent.sourceActorRoot);
  const target = samePath(intent.sourceActorRoot, intent.targetActorRoot) ? source : new ActorRegistryStore(intent.targetActorRoot);
  return { source, target };
};

/** Finish or undo an interrupted move. Safe to run any number of times. */
export const recoverActorAdoption = async (intentFile: string): Promise<ActorAdoptionRecovery> => {
  const pending = readIntent(intentFile);
  if (!pending) return "none";
  const { source, target } = stores(pending);
  return ActorRegistryStore.withLocks([source, target], () => {
    const intent = readIntent(intentFile);
    return intent ? recoverLocked(intentFile, intent, source, target) : "none";
  });
};

/** Recover every intent under a residency root's adoptions directory. */
export const recoverActorAdoptions = async (residencyRoot: string): Promise<Record<string, ActorAdoptionRecovery>> => {
  const directory = path.join(residencyRoot, "adoptions");
  let names: string[] = [];
  try { names = fs.readdirSync(directory); } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return {};
    throw error;
  }
  const results: Record<string, ActorAdoptionRecovery> = {};
  for (const name of names.filter((entry) => /^[a-f0-9]{32}\.json$/.test(entry)).sort()) {
    results[name.slice(0, -5)] = await recoverActorAdoption(path.join(directory, name));
  }
  return results;
};

/** Two-phase custody move (see module comment). Returns the committed adopted row. */
export const moveActorCustody = async (move: ActorAdoptionMove): Promise<Record<string, unknown> & { id: string }> => {
  if (!ACTOR_ID.test(move.actorId)) throw new Error(`Invalid Fabric actor id: ${move.actorId}`);
  if (move.fromRootId === move.intoRootId) throw new Error("An actor cannot be adopted into its own root");
  await recoverActorAdoption(move.intentFile);
  const { source, target } = stores(move);
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
    const targetDir = path.join(move.targetActorRoot, id), sourceDir = path.join(move.sourceActorRoot, id);
    if (!shared && (targetRows.some((candidate) => candidate.id === id) || fs.existsSync(targetDir))) {
      throw new Error(`The live root's registry already holds Fabric actor ${id}`);
    }
    move.check?.(row);
    const adopted = adoptedActorRow(row, move.fromRootId, move.intoRootId);
    const intent: ActorAdoptionIntent = { format: 1, actorId: id, fromRootId: move.fromRootId,
      intoRootId: move.intoRootId, sourceActorRoot: path.resolve(move.sourceActorRoot),
      targetActorRoot: path.resolve(move.targetActorRoot), row: adopted, phase: "prepared", createdAt: Date.now() };
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
