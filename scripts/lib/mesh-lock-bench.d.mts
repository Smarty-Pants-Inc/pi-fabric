export interface MeshLockLoad {
  participants: number;
  processes: number;
  seed: number;
  durationS: number;
  warmupS: number;
  stateMb: number;
  heartbeatS: number;
  hostRenewS: number;
  putRate: number;
  deleteRate: number;
  publishRate: number;
  steerRate: number;
  dirReadRate: number;
  registryKeys: number;
  pollMs: number;
  lockTimeoutMs: number;
  lockProtocol: number;
}
export type PlannedKind = "heartbeat" | "hostRenew" | "put" | "delete" | "publish" | "steer" | "dirRead";
export interface PlannedOp {
  id: number;
  t: number;
  kind: PlannedKind;
  p: number;
  key?: number;
  to?: number;
  phase: "warmup" | "measure";
}
export interface RatchetCheck {
  metric: string;
  class: "loadInsensitive" | "timing";
  baseline: number | undefined;
  current: number | undefined;
  limit?: number;
  regressPct?: number;
  gated: boolean;
  ok: boolean;
  missing?: boolean;
}
export interface RatchetSubject {
  load: MeshLockLoad;
  loadInsensitive: Record<string, number>;
  timing: Record<string, number>;
}
export const DEFAULT_LOAD: Readonly<MeshLockLoad>;
export const L8_BOUNDS_MS: readonly number[];
export const LOAD_INSENSITIVE_METRICS: readonly string[];
export const TIMING_METRICS: readonly string[];
export function seededRandom(seed: number): () => number;
export function planSchedule(load: MeshLockLoad): PlannedOp[];
export function participantsOf(load: MeshLockLoad, index: number): number[];
export function processOf(load: MeshLockLoad, p: number): number;
export function histogramIndex(ms: number): number;
export function histogramPercentile(histogram: readonly number[], fraction: number): number;
export function samplePercentile(samples: readonly number[], fraction: number): number;
export function loadKey(load: MeshLockLoad): string;
export function compareToBaseline(result: RatchetSubject, baseline: RatchetSubject,
  options?: { maxRegressPct?: number; gateTiming?: boolean }): { ok: boolean; problems: string[]; checks: RatchetCheck[] };
