/**
 * D3 seat drain (smarty-dev#15310): the process-wide inbox gate for this host's live Main.
 *
 * While a root is draining, its MainAgentController refuses NEW Fabric deliveries before any
 * admission, journal write or Pi handoff. The sender gets a retryable refusal and keeps its
 * record, so nothing is dropped. The gate lives on globalThis so that a live extension reload
 * (a new module generation in the same process) keeps it. A process restart (the rollback
 * relaunch path) starts with it open. Only the owner-only drain control (server.ts) changes it.
 */
const GATES = Symbol.for("pi-fabric.main.drain-gates.v1");

export interface MainDrainGate {
  readonly rootId: string;
  readonly closedAt: number;
}

const gates = (): Map<string, MainDrainGate> =>
  ((globalThis as Record<symbol, unknown>)[GATES] ??= new Map<string, MainDrainGate>()) as Map<string, MainDrainGate>;

export const FABRIC_MAIN_DRAINING = "FABRIC_MAIN_DRAINING";

/** A definite pre-admission refusal: nothing was journaled or handed to Pi. Retry later or address the successor. */
export class FabricMainDrainingError extends Error {
  override readonly name = "FabricMainDrainingError";
  readonly code = FABRIC_MAIN_DRAINING;
  readonly retryable = true;
  readonly notRun = true;
  constructor(readonly rootId: string) {
    super(`${FABRIC_MAIN_DRAINING}: Main ${rootId} is draining for a seat move and admits no new messages; ` +
      "this message was not delivered or recorded. Retry later; after the move, address the same root again.");
  }
}

/** Close the root's inbox for new deliveries. Idempotent; the first closing time is kept. */
export const closeMainInbox = (rootId: string, now = Date.now()): MainDrainGate => {
  const existing = gates().get(rootId);
  if (existing) return existing;
  const gate = { rootId, closedAt: now };
  gates().set(rootId, gate);
  return gate;
};

/** Reopen the root's inbox (rollback of a failed drain while the same process lives). */
export const reopenMainInbox = (rootId: string): boolean => gates().delete(rootId);

export const mainInboxDrainGate = (rootId: string): MainDrainGate | undefined => gates().get(rootId);

/** Throw the retryable refusal when the root is draining. */
export const assertMainInboxOpen = (rootId: string): void => {
  if (gates().has(rootId)) throw new FabricMainDrainingError(rootId);
};
