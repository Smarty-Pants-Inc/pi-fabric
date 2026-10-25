/**
 * D3 seat drain (smarty-dev#15310, DRAIN-DESIGN.r1 §1.7): ONE process-global slot per Main root.
 *
 * It holds the drain record (binding), the Fabric gate's closed state (which also refuses
 * spawn/create) and the native fence's release handle. It lives on globalThis under a registered
 * symbol, so every Fabric module generation in this process reads the same slot: an extension
 * reload reopens nothing, and an undo releases through the ORIGINAL handle (Pi keeps the fence
 * across a reload, and that release still reopens it). A process restart starts with no slot,
 * which is the rollback relaunch path. Nothing here is persisted.
 */
export interface MainDrainSlotBinding {
  kind: "host" | "pod";
  run: string;
  runSha256: string;
  operationId: string;
  generation: string;
  phase: string;
  podUid: string | null;
  boundAt: number;
  /** Host bindings: the canonical RUN and the seat lock's inode proved at bind; every later op must prove the SAME lock. */
  runDir: string | null;
  lock: { dev: string; ino: string } | null;
}
/** challenge r2 N8: one refused input, without its text. */
export interface MainDrainRefusal { at: number; path: string; fromId: string | null; id: string | null; textSha256: string | null }
export interface MainDrainSlot {
  gate?: { closedAt: number };
  binding?: MainDrainSlotBinding;
  fence?: { release(): void };
  fenceError?: string;
  /** N1: the drain's own children, admitted through the gate until the native fence is taken. */
  children?: { runIds: Set<string>; actorIds: Set<string> };
  admitted?: Array<{ at: number; path: string; fromId: string | null; id: string | null }>;
  /** N8: the refusal ledger of the current binding (bounded); ledgerOverflow counts entries that did not fit. */
  ledger?: MainDrainRefusal[];
  ledgerOverflow?: number;
  /** N2: actors this operation stopped (the stop resolved and the worker is proven gone), with their recorded public spec. */
  stopped?: Array<{ id: string; spec: Record<string, unknown>; stoppedAt: number }>;
  /** Every actor this operation sent a stop for, recorded BEFORE the stop: proven, failed or unproven alike. */
  stopAttempted?: Array<{ id: string; spec: Record<string, unknown>; stoppedAt: number }>;
  /** A clean exit was accepted (pod binding): it is final, and every later mutating op is refused. */
  exitingAt?: number;
}

const SLOTS = Symbol.for("pi-fabric.main.drain-slot.v1");
const slots = (): Map<string, MainDrainSlot> =>
  ((globalThis as Record<symbol, unknown>)[SLOTS] ??= new Map<string, MainDrainSlot>()) as Map<string, MainDrainSlot>;

/** The root's slot, created on first write. */
export const drainSlot = (rootId: string): MainDrainSlot => {
  let slot = slots().get(rootId);
  if (!slot) { slot = {}; slots().set(rootId, slot); }
  return slot;
};
/** Read without creating. */
export const peekDrainSlot = (rootId: string): MainDrainSlot | undefined => slots().get(rootId);
/** Drop an empty slot (nothing closed, bound, held, or stopped by an operation and not yet restored). */
export const pruneDrainSlot = (rootId: string): void => {
  const slot = slots().get(rootId);
  if (slot && !slot.gate && !slot.binding && !slot.fence && !slot.stopped?.length && !slot.stopAttempted?.length) slots().delete(rootId);
};

const LEDGER_LIMIT = 1_000;
/** Append to a bounded list (the first LEDGER_LIMIT entries); returns false when the entry did not fit. */
export const appendBounded = <T>(list: T[], entry: T): boolean => {
  if (list.length >= LEDGER_LIMIT) return false;
  list.push(entry);
  return true;
};
