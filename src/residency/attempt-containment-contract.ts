import type { ResidentLaunchSpec } from "./handover.js";

/** Receipts are produced by the launcher's retained containment custody, never a reopened owner path. */
export interface AttemptLaunchReceipt {
  attemptId: string;
  scope: string;
  cgroupPath: string;
  launchedInScope: true;
  at: number;
}
export interface AttemptMembership {
  attemptId: string;
  scope: string;
  pids: number[];
  populated: boolean;
}
export interface AttemptExitReceipt {
  attemptId: string;
  scope: string;
  populated: false;
  at: number;
}
export class ContainmentUnavailableError extends Error {
  readonly code = "CONTAINMENT_UNAVAILABLE" as const;
  constructor(readonly reason = "Attempt-owned containment custody is unavailable; retain the serving resident") {
    super(reason); this.name = "ContainmentUnavailableError";
  }
}
export class AttemptExitUnconfirmedError extends Error {
  readonly code = "ATTEMPT_EXIT_UNCONFIRMED" as const;
  readonly attemptId: string;
  readonly remainingPids: number[];
  constructor(input: { attemptId: string; remainingPids: number[] }) {
    super(`Whole-attempt exit is unconfirmed for ${input.attemptId}`);
    this.name = "AttemptExitUnconfirmedError";
    this.attemptId = input.attemptId;
    this.remainingPids = [...input.remainingPids];
  }
}
export interface AttemptContainment {
  launchInScope(spec: ResidentLaunchSpec, attemptId: string): Promise<AttemptLaunchReceipt>;
  attemptMembership(attemptId: string): Promise<AttemptMembership>;
  /** ABSOLUTE epoch-millisecond deadline, not a duration. Does not imply termination authority. */
  joinAttemptExit(attemptId: string, deadlineAtMs: number): Promise<AttemptExitReceipt>;
}
const unavailable = async (): Promise<never> => { throw new ContainmentUnavailableError(); };
/** No runtime opt-in. Real adapters belong exclusively to launcher custody. */
export const failClosedContainment: AttemptContainment = Object.freeze({
  launchInScope: unavailable,
  attemptMembership: unavailable,
  joinAttemptExit: unavailable,
});
