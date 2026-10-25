import { createHash } from "node:crypto";
import { appendBounded, drainSlot, peekDrainSlot, pruneDrainSlot } from "./slot.js";

/**
 * D3 seat drain (smarty-dev#15310): the Fabric gate of this host's live Main.
 *
 * While a root is draining, its MainAgentController refuses NEW Fabric deliveries before any
 * admission, journal write or Pi handoff; deliverUser, agents.spawn and actors.create refuse
 * too. The refusal is visible and typed (FABRIC_MAIN_DRAINING, notRun). A retrying producer
 * keeps its record and delivers later; a non-retrying one sees the refusal: "visible refusal",
 * not "exactly once" (DRAIN-DESIGN.r1 finding 8). The state is in the process-global drain slot
 * (slot.ts): a live reload keeps it, and a process restart starts with it open.
 */
export interface MainDrainGate {
  readonly rootId: string;
  readonly closedAt: number;
}

export const FABRIC_MAIN_DRAINING = "FABRIC_MAIN_DRAINING";

/** A definite pre-admission refusal: nothing was journaled, handed to Pi or started. */
export class FabricMainDrainingError extends Error {
  override readonly name = "FabricMainDrainingError";
  readonly code = FABRIC_MAIN_DRAINING;
  readonly retryable = true;
  readonly notRun = true;
  constructor(readonly rootId: string) {
    super(`${FABRIC_MAIN_DRAINING}: Main ${rootId} is draining for a seat move and admits no new messages or work; ` +
      "this request was not delivered, recorded or started. Retry after the move to the same address, with a NEW message " +
      "(a retry with the same idempotency key replays this refusal).");
  }
}

/** Close the root's gate. Idempotent; the first closing time is kept. */
export const closeMainInbox = (rootId: string, now = Date.now()): MainDrainGate => {
  const slot = drainSlot(rootId);
  slot.gate ??= { closedAt: now };
  return { rootId, closedAt: slot.gate.closedAt };
};

/** Reopen the root's gate (an undo, same process). True when it was closed. */
export const reopenMainInbox = (rootId: string): boolean => {
  const slot = peekDrainSlot(rootId);
  const closed = slot?.gate !== undefined;
  if (slot) delete slot.gate;
  pruneDrainSlot(rootId);
  return closed;
};

export const mainInboxDrainGate = (rootId: string): MainDrainGate | undefined => {
  const gate = peekDrainSlot(rootId)?.gate;
  return gate ? { rootId, closedAt: gate.closedAt } : undefined;
};

/** What a live admission tells the gate about itself; the dry-run probes pass none (no exemption, no ledger). */
export interface MainDrainAdmission {
  path: string;
  from?: { id?: unknown; kind?: unknown } | undefined;
  deliveryId?: string | undefined;
  eventId?: string | undefined;
  text?: string | undefined;
}

const AGENT_COMPLETION = "agent-completion:";

/**
 * THE gate function: live admission and the dry-run probes both call it.
 * challenge r2 N1: until the native fence is taken, the drain's OWN children still deliver: the
 * completion of a run spawned before `begin`, and messages from the root's own session actors that
 * are not yet stopped. Only new outside input is refused. N8: each live refusal is appended (no text)
 * to the current binding's ledger.
 */
export const assertMainInboxOpen = (rootId: string, admission?: MainDrainAdmission): void => {
  const slot = peekDrainSlot(rootId);
  if (!slot?.gate) return;
  if (!admission) throw new FabricMainDrainingError(rootId);
  const fromId = typeof admission.from?.id === "string" ? admission.from.id : null;
  const id = admission.deliveryId ?? admission.eventId ?? null;
  // The sender identity is the verified envelope's (never a body field), and a completion counts only when its
  // run IS that sender: a peer cannot borrow a child's run id through its own command id.
  const runId = admission.deliveryId?.startsWith(AGENT_COMPLETION) === true ? admission.deliveryId.slice(AGENT_COMPLETION.length) : undefined;
  const child = slot.children !== undefined && !slot.fence && fromId !== null && (
    (runId !== undefined && admission.from?.kind === "agent" && fromId === runId && slot.children.runIds.has(runId)) ||
    (admission.from?.kind === "actor" && slot.children.actorIds.has(fromId)));
  if (child) {
    appendBounded(slot.admitted ??= [], { at: Date.now(), path: admission.path, fromId, id });
    return;
  }
  appendBounded(slot.ledger ??= [], { at: Date.now(), path: admission.path, fromId, id,
    textSha256: admission.text === undefined ? null : createHash("sha256").update(admission.text).digest("hex") });
  throw new FabricMainDrainingError(rootId);
};
