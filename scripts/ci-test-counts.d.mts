export type ParityPhase = "related" | "direct";
export type PhaseFiles = Partial<Record<ParityPhase, readonly string[]>>;
export interface PhaseParity {
  expected: number;
  actual: number;
  missing: string[];
  extra: string[];
  duplicated: string[];
  ok: boolean;
}
export function compareFileParity(
  expected: Record<ParityPhase, readonly string[]>,
  shards: readonly PhaseFiles[],
): Record<ParityPhase, PhaseParity>;
