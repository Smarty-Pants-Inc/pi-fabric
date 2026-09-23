// Test seam for the drift probe's pure stage aggregation.
export interface DriftStageLabel {
  count: number;
  serviceMs: Record<string, number>;
  listedFiles?: number;
}
export interface DriftStageKind {
  count: number;
  serviceMs: Record<string, number>;
  wallMs: Record<string, number>;
  bytes?: Record<string, number>;
  labels: Record<string, DriftStageLabel>;
}
export interface DriftStageSummary {
  samples: number;
  wallMs: Record<string, number>;
  serviceMs: Record<string, number>;
  kinds: Record<string, DriftStageKind>;
}
export function summarizeStage(
  runs: Array<{ wallMs: number; totalServiceMs: number; kinds: Record<string, unknown> }>,
): DriftStageSummary;
