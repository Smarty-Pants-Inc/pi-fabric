// Type declarations for bench/prewalk/lib/prewalk-live-evidence.mjs, used by tests
// and the canary telemetry recorder.
export const PREWALK_MESSAGE_PREFIX: string;
export type PrewalkMessageSource = "live" | "session";
export interface PrewalkMessage {
  source: PrewalkMessageSource;
  customType: string;
  details: unknown;
  at: number | null;
  id?: string | null;
}
export interface MergedPrewalkMessage {
  customType: string;
  details: unknown;
  at: number | null;
  sources: PrewalkMessageSource[];
  sessionEntryId?: string | null;
}
export function parseJsonLines(text: string, source?: string): Array<Record<string, unknown>>;
export function assertCompleteRecording(
  events: Array<Record<string, unknown>>,
  telemetry: Array<Record<string, unknown>>,
  options?: { requireSessionHeader?: boolean; sessionId?: string; allowAborted?: boolean },
): void;
export function parseIndentedStatus(text: string): Record<string, unknown>;
// Recognizes a status returned alone, nested in a batched Fabric envelope, or
// as a JSON envelope; null means "not a status observation" (quoted text,
// descriptive projection, malformed or foreign content).
export function extractPrewalkStatus(text: string): Record<string, unknown> | null;
// --- Request-contract helpers (opt-in fixture evidence) --------------------
export interface RequestContract {
  markers: Record<string, string>;
  exactlyOnce: string[];
  present: string[];
  ordered: string[];
  absentBefore: string[];
}
export function parseRequestContract(text: string, source?: string): RequestContract;
export interface RequestScan {
  matches: Record<string, Array<[number, number, number]>>;
  truncated: boolean;
}
export function scanRequestMessages(
  messages: readonly unknown[],
  markers: Record<string, string>,
  limits?: { maxMatches?: number; maxChars?: number },
): RequestScan;
export function requestContractEvidenceProblems(
  records: Array<Record<string, unknown>>,
  contract: RequestContract,
  contractSha256: string,
): string[];
export function requestContractPayloadProblems(
  records: Array<Record<string, unknown>>,
  contract: RequestContract,
  executorModel: string,
): string[];

export const CHECK_STATUSES: readonly string[];
export interface CheckRecord { name: string; status: "pass" | "fail" | "unobserved"; detail: unknown; axis: string }
export interface CheckLedger {
  add(name: string, status: "pass" | "fail" | "unobserved", detail?: unknown, axis?: string): boolean;
  checks(): CheckRecord[];
  axes(): Record<string, Array<{ name: string; status: string }>>;
  ok(): boolean;
  toJSON(): { ok: boolean; checks: CheckRecord[]; axes: Record<string, Array<{ name: string; status: string }>> };
}
export function createCheckLedger(): CheckLedger;
export function livePrewalkMessages(events: Array<Record<string, unknown>>): PrewalkMessage[];
export function persistedPrewalkMessages(entries: readonly unknown[]): PrewalkMessage[];
export function prewalkMessageKey(message: PrewalkMessage): string;
export function mergePrewalkMessages(live: PrewalkMessage[], persisted: PrewalkMessage[]): MergedPrewalkMessage[];
export interface AssistantTimelineItem {
  at: number | null;
  model: string | null;
  usage: Record<string, unknown> | null;
  stopReason: string | null;
}
export function assistantTimeline(events: Array<Record<string, unknown>>): AssistantTimelineItem[];
export function toolResultUsages(events: Array<Record<string, unknown>>): Array<Record<string, unknown>>;
export interface PerModelUsage {
  requests: number;
  input: number;
  output: number;
  cacheRead: number;
  cacheWrite: number;
  totalTokens: number;
  recordedCostEstimateUsd: number;
}
export function aggregateUsage(
  timeline: AssistantTimelineItem[],
  toolUsages?: Array<Record<string, unknown>>,
): Record<string, PerModelUsage>;
export function usageMatches(
  actual: Record<string, PerModelUsage> | undefined,
  expected: Record<string, PerModelUsage> | undefined,
  tolerance?: number,
): { ok: boolean; mismatches: Array<Record<string, unknown>> };
export interface TelemetryModelSelect { at: number | null; model: string | null; previous: string | null }
export interface TelemetryCompaction { type: string; at: number | null; reason: string | null; error: string | null }
export interface TelemetryTimeline {
  modelSelects: TelemetryModelSelect[];
  compaction: TelemetryCompaction[];
  boundaries: Record<string, number | null>;
}
export function telemetryTimeline(telemetryEvents: Array<Record<string, unknown>>): TelemetryTimeline;
export interface CompactionArrivalBounds { startAt: number | null; endAt: number | null }
export function arrivalCompactionBounds(
  events: Array<Record<string, unknown>>,
  arrivals: Array<Record<string, unknown>> | null | undefined,
): CompactionArrivalBounds | null;
export interface CompactionPhaseRecord {
  outcome: string;
  at: number | null;
  reason: string | null;
  error: string | null;
  attemptMs: number | null;
}
export interface PhaseAttribution {
  handoffAt: number | null;
  returnAt: number | null;
  assistantSpanMs: number | null;
  preHandoffMainMs: number | null;
  executorIntervalMs: number | null;
  executorAssistantSpanMs: number | null;
  returnMs: number | null;
  compaction: CompactionPhaseRecord | null;
  provenance: Record<string, string>;
  missing: string[];
}
export function attributePhases(options: {
  timeline: AssistantTimelineItem[];
  telemetry?: TelemetryTimeline;
  compactionArrival?: CompactionArrivalBounds | null;
}): PhaseAttribution;
export interface CellAnalysis {
  name: string;
  prewalkMessages: MergedPrewalkMessage[];
  perModel: Record<string, PerModelUsage>;
  phases: PhaseAttribution;
  assistantCount: number;
  modelSelects: TelemetryModelSelect[];
  compactionEvents: TelemetryCompaction[];
  livePrewalkCount: number;
  persistedPrewalkCount: number;
}
export interface TaskCheckSpec { testFile: string; artifact: string }
export function parseTaskCheckSpec(text: string, source?: string): TaskCheckSpec;
export function taskCheckReceiptProblems(
  receipt: unknown,
  options?: { artifactSha256?: string | null },
): string[];

export function analyzeCell(options: {
  name: string;
  liveEvents: Array<Record<string, unknown>>;
  persistedEntries?: Array<Record<string, unknown>>;
  telemetryEvents?: Array<Record<string, unknown>>;
  compactionArrival?: CompactionArrivalBounds | null;
}): CellAnalysis;
