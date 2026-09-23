export interface GradeResultRecord {
  valid: boolean;
  resolved: boolean;
  error: string | null;
  parsedTests: number;
  requiredTests: number;
  requiredTestsPassed: number;
}
export interface ExecutionEvidence {
  setupOk: boolean;
  testStarted: boolean;
  testExitCode: number | null;
  stdout: string;
  stderr: string;
}
export interface ControlArm {
  result: GradeResultRecord;
  execution: ExecutionEvidence;
}
export interface NoopVerdict {
  status: "pass" | "fail";
  acceptable: boolean;
  reason: string;
}
export interface GoldVerdict {
  status: "pass" | "fail";
  reason: string;
}
export interface ControlVerdict {
  ok: boolean;
  noop: NoopVerdict;
  gold: GoldVerdict;
}
export interface CandidateVerdict {
  status: "pass" | "fail" | "unobserved";
  validComparison: boolean;
  reason: string;
}
export interface RequestLifecycleLimits {
  maximumRequestUsd: Record<string, number>;
}
export interface UnsettledRequest {
  number: number;
  model: string;
}
export interface RequestLifecycleState {
  uncertain: boolean;
  unsettled: UnsettledRequest[];
  heldUsd: number;
  knownUsd: number;
}
export function classifyControls(arms: { noop: ControlArm | null; gold: ControlArm | null }): ControlVerdict;
export function classifyCandidate(input: {
  result: GradeResultRecord | null;
  controlsPassed: boolean;
  execution: ExecutionEvidence | null;
}): CandidateVerdict;
export function analyzeRequestLifecycle(
  events: Array<Record<string, unknown>>,
  limits: RequestLifecycleLimits,
): RequestLifecycleState;
export function summarizeAttempts(rows: Array<Record<string, unknown>>): {
  processed: number;
  attempted: number;
  skipped: number;
  graded: number;
  resolved: number;
  executedPairs: number;
};
export function planResume<T extends { id: string }>(
  schedule: T[],
  checkpoints: Array<{ id: string; phase: string }>,
): { runnable: T[]; needsRecovery: T[]; completed: T[] };
export function comparePairs(rows: Array<Record<string, unknown>>): Record<string, unknown>;
