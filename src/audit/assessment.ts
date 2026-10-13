import type { FabricExecutionOutcomeV1, FabricExecutionTraceObserver } from "./trace.js";

export const FABRIC_ASSESSMENT_TRACE_KIND = "pi-fabric.assessment" as const;
export const FABRIC_ASSESSMENT_TRACE_VERSION = 1 as const;
export const FABRIC_ASSESSMENT_TRACE_MAX_BYTES = 128 * 1024;

const MAX_OPERATIONS = 1_024;
const MAX_REF_CHARS = 256;
const MAX_MODEL_CHARS = 128;
const MAX_USAGE_ENTRIES = 64;

export type FabricAssessmentSourceV1 = "agent" | "jev" | "classifier" | "provider";

export interface FabricAssessmentUsageV1 {
  input: number;
  output: number;
  cacheRead: number;
  cacheWrite: number;
  totalTokens: number;
  /** USD, present only when the operation reported a cost. */
  cost?: number;
}

export interface FabricAssessmentOperationV1 {
  /** Joins with the execution trace operation of the same sequence. */
  sequence: number;
  ref: string;
  outcome: FabricExecutionOutcomeV1;
  durationMs: number;
  source?: FabricAssessmentSourceV1;
  model?: string;
  usage?: FabricAssessmentUsageV1;
}

export interface FabricAssessmentTraceV1 {
  kind: typeof FABRIC_ASSESSMENT_TRACE_KIND;
  version: typeof FABRIC_ASSESSMENT_TRACE_VERSION;
  outcome: FabricExecutionOutcomeV1;
  /** Whole-program wall time. */
  durationMs: number;
  operations: FabricAssessmentOperationV1[];
  /** Totals cover every observed operation, including dropped ones. */
  totals: {
    operations: number;
    succeeded: number;
    failed: number;
    usage: FabricAssessmentUsageV1;
  };
  counts: { droppedOperations: number };
}

export interface FabricAssessmentAttribution {
  source?: FabricAssessmentSourceV1;
  model?: string;
  usage?: FabricAssessmentUsageV1;
}

interface MutableAssessmentOperation {
  sequence: number;
  ref: string;
  startedAt: number;
  endedAt?: number;
  outcome?: FabricExecutionOutcomeV1;
  attribution?: FabricAssessmentAttribution;
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const count = (value: unknown): number =>
  typeof value === "number" && Number.isFinite(value) && value > 0 ? value : 0;

/**
 * Reads the usage shapes Fabric operations already report: Pi `Usage`
 * (`cost.total`), agent run `AgentUsage` (`cost` number), and Jev
 * (`input_tokens`/`output_tokens`). Anything else yields no usage.
 */
export const readFabricAssessmentUsage = (value: unknown): FabricAssessmentUsageV1 | undefined => {
  if (!isRecord(value)) return undefined;
  const jev = "input_tokens" in value || "output_tokens" in value;
  if (!jev && !("input" in value) && !("output" in value)) return undefined;
  const input = count(jev ? value.input_tokens : value.input);
  const output = count(jev ? value.output_tokens : value.output);
  const cacheRead = jev ? 0 : count(value.cacheRead);
  const cacheWrite = jev ? 0 : count(value.cacheWrite);
  const reportedTotal = jev ? 0 : count(value.totalTokens);
  const cost = typeof value.cost === "number"
    ? value.cost
    : isRecord(value.cost) ? value.cost.total : undefined;
  return {
    input,
    output,
    cacheRead,
    cacheWrite,
    totalTokens: reportedTotal || input + output + cacheRead + cacheWrite,
    ...(typeof cost === "number" && Number.isFinite(cost) && cost >= 0 ? { cost } : {}),
  };
};

const addUsage = (
  total: FabricAssessmentUsageV1,
  usage: FabricAssessmentUsageV1,
): void => {
  total.input += usage.input;
  total.output += usage.output;
  total.cacheRead += usage.cacheRead;
  total.cacheWrite += usage.cacheWrite;
  total.totalTokens += usage.totalTokens;
  if (usage.cost !== undefined) total.cost = (total.cost ?? 0) + usage.cost;
};

const emptyUsage = (): FabricAssessmentUsageV1 => ({
  input: 0,
  output: 0,
  cacheRead: 0,
  cacheWrite: 0,
  totalTokens: 0,
});

const sourceForRef = (ref: string): FabricAssessmentSourceV1 =>
  ref.startsWith("agents.") ? "agent" : ref.startsWith("jev.") ? "jev" : "provider";

const boundedModel = (value: unknown): string | undefined =>
  typeof value === "string" && value.trim() ? value.trim().slice(0, MAX_MODEL_CHARS) : undefined;

/** Usage and model reported at the top level, under `agent` (handoff), or by a bounded array of runs. */
const attributionFromResult = (
  ref: string,
  result: unknown,
): FabricAssessmentAttribution | undefined => {
  const entries = Array.isArray(result)
    ? result.slice(0, MAX_USAGE_ENTRIES)
    : [isRecord(result) && isRecord(result.agent) ? result.agent : result];
  let usage: FabricAssessmentUsageV1 | undefined;
  const models = new Set<string>();
  for (const entry of entries) {
    if (!isRecord(entry)) continue;
    const reported = readFabricAssessmentUsage(entry.usage);
    if (!reported) continue;
    usage ??= emptyUsage();
    addUsage(usage, reported);
    const model = boundedModel(entry.model);
    if (model) models.add(model);
  }
  if (!usage) return undefined;
  return {
    source: sourceForRef(ref),
    ...(models.size === 1 ? { model: [...models][0]! } : {}),
    usage,
  };
};

const roundMs = (value: number): number => Math.max(0, Math.round(value * 1_000) / 1_000);

/**
 * Opt-in, non-deterministic companion to the execution trace: timings,
 * model attribution, and token usage per operation. It never holds
 * arguments, results, source, or error prose. Memory stays bounded: past
 * MAX_OPERATIONS detailed rows, settled operations fold into the totals.
 */
export class FabricAssessmentRecorder implements FabricExecutionTraceObserver {
  readonly #operations = new Map<number, MutableAssessmentOperation>();
  readonly #overflow = new Map<number, MutableAssessmentOperation>();
  readonly #overflowTotals = { succeeded: 0, failed: 0, usage: emptyUsage() };
  readonly #startedAt: number;
  #observed = 0;
  #sealed = false;

  constructor(readonly now: () => number = () => performance.now()) {
    this.#startedAt = now();
  }

  issued(sequence: number, ref: string): void {
    if (this.#sealed) return;
    this.#observed++;
    const operation = { sequence, ref: ref.slice(0, MAX_REF_CHARS), startedAt: this.now() };
    if (this.#operations.size < MAX_OPERATIONS) this.#operations.set(sequence, operation);
    else this.#overflow.set(sequence, operation);
  }

  settled(sequence: number, outcome: FabricExecutionOutcomeV1, result: unknown): void {
    if (this.#sealed) return;
    const operation = this.#operations.get(sequence) ?? this.#overflow.get(sequence);
    if (!operation) return;
    operation.outcome = outcome;
    operation.endedAt = this.now();
    if (result !== undefined && !operation.attribution) {
      try {
        const attribution = attributionFromResult(operation.ref, result);
        if (attribution) operation.attribution = attribution;
      } catch {
        // Attribution is best effort and never affects the program.
      }
    }
    if (this.#overflow.delete(sequence)) this.#fold(operation, outcome);
  }

  /** Explicit attribution for usage that is not part of an operation result (classifier). */
  attribute(sequence: number, attribution: FabricAssessmentAttribution): void {
    const operation = this.#sealed ? undefined : this.#operations.get(sequence);
    if (!operation) {
      if (!this.#sealed && attribution.usage) addUsage(this.#overflowTotals.usage, attribution.usage);
      return;
    }
    const model = boundedModel(attribution.model);
    operation.attribution = {
      ...(attribution.source ? { source: attribution.source } : {}),
      ...(model ? { model } : {}),
      ...(attribution.usage ? { usage: attribution.usage } : {}),
    };
  }

  #fold(operation: MutableAssessmentOperation, outcome: FabricExecutionOutcomeV1): void {
    if (outcome === "succeeded") this.#overflowTotals.succeeded++;
    else this.#overflowTotals.failed++;
    if (operation.attribution?.usage) addUsage(this.#overflowTotals.usage, operation.attribution.usage);
  }

  seal(outcome: FabricExecutionOutcomeV1): FabricAssessmentTraceV1 {
    // Unsettled operations take the typed final outcome, as in the trace.
    const fallback: FabricExecutionOutcomeV1 =
      outcome === "timed_out" ? "timed_out" : outcome === "aborted" ? "aborted" : "failed";
    for (const operation of this.#overflow.values()) this.#fold(operation, operation.outcome ?? fallback);
    this.#overflow.clear();
    this.#sealed = true;
    const end = this.now();
    const usage = emptyUsage();
    addUsage(usage, this.#overflowTotals.usage);
    let succeeded = this.#overflowTotals.succeeded;
    let failed = this.#overflowTotals.failed;
    const all = [...this.#operations.values()].sort((a, b) => a.sequence - b.sequence);
    const operations = all.map((operation): FabricAssessmentOperationV1 => {
      // Bridge calls see an aborted signal for deadlines too; the typed run
      // outcome is authoritative, matching the execution trace.
      const operationOutcome = operation.outcome === "aborted" && outcome === "timed_out"
        ? "timed_out"
        : operation.outcome ?? fallback;
      if (operationOutcome === "succeeded") succeeded++;
      else failed++;
      const attribution = operation.attribution;
      if (attribution?.usage) addUsage(usage, attribution.usage);
      return {
        sequence: operation.sequence,
        ref: operation.ref,
        outcome: operationOutcome,
        durationMs: roundMs((operation.endedAt ?? end) - operation.startedAt),
        ...(attribution?.source ? { source: attribution.source } : {}),
        ...(attribution?.model ? { model: attribution.model } : {}),
        ...(attribution?.usage ? { usage: { ...attribution.usage } } : {}),
      };
    });
    const trace: FabricAssessmentTraceV1 = {
      kind: FABRIC_ASSESSMENT_TRACE_KIND,
      version: FABRIC_ASSESSMENT_TRACE_VERSION,
      outcome,
      durationMs: roundMs(end - this.#startedAt),
      operations,
      totals: { operations: this.#observed, succeeded, failed, usage },
      counts: { droppedOperations: this.#observed - operations.length },
    };
    return trimFabricAssessmentTrace(trace, FABRIC_ASSESSMENT_TRACE_MAX_BYTES);
  }
}

const serializedBytes = (value: unknown): number =>
  Buffer.byteLength(JSON.stringify(value), "utf8");

/** Drops trailing operations (never totals) until the projection fits. */
export const trimFabricAssessmentTrace = (
  trace: FabricAssessmentTraceV1,
  maxBytes: number,
): FabricAssessmentTraceV1 => {
  let bytes = serializedBytes(trace);
  while (bytes > maxBytes && trace.operations.length > 0) {
    const operation = trace.operations.pop()!;
    trace.counts.droppedOperations++;
    bytes -= serializedBytes(operation) + (trace.operations.length > 0 ? 1 : 0);
  }
  return trace;
};

const isUsage = (value: unknown): value is FabricAssessmentUsageV1 =>
  isRecord(value) &&
  ["input", "output", "cacheRead", "cacheWrite", "totalTokens"].every(
    (key) => typeof value[key] === "number" && Number.isFinite(value[key]),
  ) &&
  (value.cost === undefined || (typeof value.cost === "number" && Number.isFinite(value.cost)));

export const isFabricAssessmentTraceV1 = (value: unknown): value is FabricAssessmentTraceV1 => {
  try {
    if (!isRecord(value)) return false;
    if (value.kind !== FABRIC_ASSESSMENT_TRACE_KIND || value.version !== FABRIC_ASSESSMENT_TRACE_VERSION) return false;
    if (typeof value.durationMs !== "number" || !Array.isArray(value.operations)) return false;
    if (!isRecord(value.totals) || !isUsage(value.totals.usage)) return false;
    if (!isRecord(value.counts) || !Number.isSafeInteger(value.counts.droppedOperations)) return false;
    return value.operations.every((operation) =>
      isRecord(operation) &&
      Number.isSafeInteger(operation.sequence) &&
      typeof operation.ref === "string" &&
      typeof operation.outcome === "string" &&
      typeof operation.durationMs === "number" &&
      (operation.model === undefined || typeof operation.model === "string") &&
      (operation.usage === undefined || isUsage(operation.usage)),
    ) && serializedBytes(value) <= FABRIC_ASSESSMENT_TRACE_MAX_BYTES;
  } catch {
    return false;
  }
};

export const readFabricAssessmentTraceV1 = (value: unknown): FabricAssessmentTraceV1 | undefined =>
  isFabricAssessmentTraceV1(value) ? value : undefined;
