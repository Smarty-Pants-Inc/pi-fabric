import type { MeshIdentity } from "./mesh/store.js";

export type FabricInterruptErrorCode = "FABRIC_INTERRUPT_NOT_AUTHORIZED" | "FABRIC_INTERRUPT_RATE_LIMITED";
export class FabricInterruptNotAuthorizedError extends Error {
  override readonly name = "FabricInterruptNotAuthorizedError";
  readonly code = "FABRIC_INTERRUPT_NOT_AUTHORIZED";
  constructor(sender: string, target: string) {
    super(`Sender ${sender} is not authorized to interrupt Main ${target}`);
  }
}
export class FabricInterruptRateLimitedError extends Error {
  override readonly name = "FabricInterruptRateLimitedError";
  readonly code = "FABRIC_INTERRUPT_RATE_LIMITED";
  constructor(sender: string, target: string) {
    super(`Sender ${sender} must wait 60 seconds between interrupts to Main ${target}`);
  }
}
export const interruptErrorCode = (error: unknown): FabricInterruptErrorCode | undefined => {
  const code = error instanceof Error && "code" in error ? error.code : undefined;
  return code === "FABRIC_INTERRUPT_NOT_AUTHORIZED" || code === "FABRIC_INTERRUPT_RATE_LIMITED" ? code : undefined;
};
/** Registry facts only: a same-name peer, adopted actor, or arbitrary actor under this root is insufficient. */
export const isMainInterruptSupervisor = (sender: MeshIdentity, rootId: string, actor: {
  id: string; rootId?: string; supervisorFor?: string; status: string; removal?: unknown;
} | undefined): boolean => sender.kind === "actor" && actor?.id === sender.id && actor.rootId === rootId &&
  actor.supervisorFor === rootId && actor.status !== "stopped" && !actor.removal;

export interface FabricInterruptAuthority {
  /** Host configuration only. Names are matched only for verified Main identities. */
  interruptFrom?: () => readonly string[];
  /** Native owner-minted registry binding, not mesh presence or request.data. */
  isSupervisor?: (sender: MeshIdentity, rootId: string) => boolean;
}

/** Native supervisor convention, designated only at creation by its owning Main. */
export const isMainSupervisorRequest = (request: {
  name: string; responseMode?: string; events?: readonly string[]; triggerTurn?: boolean; delivery?: string;
}): boolean => /(?:^|-)supervisor$/.test(request.name.trim()) && request.responseMode === "directive" &&
  request.events?.includes("agent_settled") === true && request.delivery === "steer" && request.triggerTurn === true;
