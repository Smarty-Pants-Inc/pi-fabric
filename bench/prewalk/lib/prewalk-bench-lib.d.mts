// Type declarations for bench/prewalk/lib/prewalk-bench-lib.mjs, used by tests.
export interface Stats { n: number; min: number; median: number; p95: number; max: number }
export interface RunFileInput {
  path: string;
  gzipped: boolean;
  rawBytes: number;
  rawSha256: string;
  gzSha256: string | null;
  run: Record<string, unknown>;
}
export interface QueueCell {
  id: string;
  config: Record<string, unknown>;
  samples: Record<string, number>;
  totalMs: Record<string, Stats | null>;
  perWorker: Record<string, { workers: number; medianOfWorkerMediansMs: number; spreadMs: number } | null>;
  requestsMedian: Record<string, Stats | null>;
  contextBytesMedian: Record<string, Stats | null>;
  deltaMedianTotalMs: number | null;
}
export interface DriftGroup {
  id: string;
  samples: Record<string, number>;
  wallMs: Record<string, Stats | null>;
  cpuMs: Record<string, Stats | null>;
  claims: Record<string, number>;
}
export interface BaselineVsClean {
  id: string;
  baselineWallMs: Record<string, Stats | null>;
  cleanWallMs: Record<string, Stats | null>;
  deltaCleanMinusBaselineMs: Record<string, number | null>;
}
export interface Comparison {
  schemaVersion: number;
  tool: string;
  generatedAt: string;
  labels: string[];
  units: Record<string, string>;
  runs: Array<{
    label: string;
    path: string;
    rawSha256: string;
    gzSha256: string | null;
    rawBytes: number;
    status: unknown;
    findings: unknown[];
  }>;
  compatibility: { checks: Record<string, string>; comparable: boolean; hostComparable: boolean; notes: string[] };
  queue: { cells: QueueCell[] };
  drift: { groups: DriftGroup[]; cleanVsBaseline: BaselineVsClean[] };
  caveats: string[];
}
export function sha256(value: string | Uint8Array): string;
export function round(value: number, digits?: number): number;
export function stats(values: number[]): Stats;
export function roundStats(value: Stats | null | undefined, digits?: number): Stats | null;
export function groupRows<T, R>(rows: T[], key: (row: T) => string, summarize: (rows: T[]) => R): Array<{ id: string } & R>;
export function writeExclusive(outPath: string, contents: string): string;
export function readRunFile(inputPath: string): RunFileInput;
export function compatibilityReport(inputs: RunFileInput[]): { checks: Record<string, string>; comparable: boolean; hostComparable: boolean; notes: string[] };
export function compareQueue(inputs: RunFileInput[], labels: string[]): QueueCell[];
export function compareDrift(inputs: RunFileInput[], labels: string[]): DriftGroup[];
export function compareBaselineVsClean(groups: DriftGroup[], labels: string[]): BaselineVsClean[];
export function buildComparison(inputs: RunFileInput[], labels?: string[]): Comparison;
export function archiveRaw(rawPath: string, gzPath?: string | null, options?: { removeRaw?: boolean }): {
  rawPath: string;
  gzPath: string;
  rawSha256: string;
  gzSha256: string;
  rawBytes: number;
  gzBytes: number;
  verified: boolean;
  removedRaw: boolean;
  checksums: string[];
};
export interface SnapshotEntry { sha256?: string; link?: string }
export type SnapshotTreeInput = Record<string, string | SnapshotEntry>;
export interface SnapshotComparison { same: boolean; changed: string[]; added: string[]; removed: string[] }
export function snapshotTree(root: string, options?: { relativeTo?: string; skip?: string[] }): Record<string, SnapshotEntry>;
export function compareSnapshots(before: SnapshotTreeInput, after: SnapshotTreeInput): SnapshotComparison;
