import {
  ENTROPY_INVOCATION_STAGES,
  ENTROPY_METRIC_VERSION,
  ENTROPY_WEIGHTS,
  type EntropyModelReport,
  type EntropyRefReport,
  type EntropyRepairRowInput,
  type EntropyReport,
  type EntropyShapeSignature,
  type EntropySurfaceSnapshot,
  type EntropyTotals,
  type EntropyTraceInput,
} from "./types.js";
import {
  compareCodeUnits,
  roundMetric,
  shannonEntropyBits,
  shapeSignature,
  signatureDistance,
  staticFreedomFromSchema,
} from "./fingerprint.js";

// The meter is a pure function of its inputs: typed trace operations, an
// optional surface snapshot, and the normalized repair table. It reads no
// clocks, no randomness, and no prose — only the residues of non-canonical
// behavior Fabric already records. Same inputs and metric version, same
// report, on every run.

export interface EntropyMeterInput {
  traces: readonly EntropyTraceInput[];
  surface?: EntropySurfaceSnapshot;
  repairs?: readonly EntropyRepairRowInput[];
  catalogDigest?: string;
}

export interface EntropyTraceWindow {
  file: string;
  /** Session-reader snapshots are immutable; appends retain existing trace identities. */
  traces: readonly EntropyTraceInput[];
}

const DISCOVERY_PREFIX = "fabric.discovery.";
const WORKFLOW_PREFIX = "fabric.workflow.";
const DEFAULT_TASK_KEY = "(none)";
const TOP_SIGNATURES = 8;

interface RefAccumulator {
  calls: number;
  succeeded: number;
  failed: number;
  signatures: Map<string, number>;
  stages: Map<string, number>;
  churnPairs: number;
  churnSum: number;
}

// The core pass without per-model attribution; measureEntropy wraps it so
// each model's behavioral terms come from the same formula over its own
// traces, measured against the same surface.
type EntropyReportCore = Omit<EntropyReport, "byModel">;

interface MeasureState {
  surfaceByRef: Map<string, unknown>;
  lexiconByRef: Map<string, number>;
  lexiconRows: number;
  totals: EntropyTotals;
  refs: Map<string, RefAccumulator>;
  taskSequences: Map<string, Map<string, number>>;
  churnPairs: number;
  churnSum: number;
}

const createMeasureState = (input: EntropyMeterInput): MeasureState => {
  const surfaceByRef = new Map<string, unknown>();
  if (input.surface) {
    for (const action of input.surface.actions) surfaceByRef.set(action.ref, action.inputSchema);
  }
  const lexiconByRef = new Map<string, number>();
  let lexiconRows = 0;
  for (const row of input.repairs ?? []) {
    lexiconByRef.set(row.ref, (lexiconByRef.get(row.ref) ?? 0) + 1);
    lexiconRows += 1;
  }
  return {
    surfaceByRef,
    lexiconByRef,
    lexiconRows,
    totals: {
      traces: input.traces.length,
      operations: 0,
      actionOperations: 0,
      discoveryOperations: 0,
      workflowOperations: 0,
      succeeded: 0,
      failed: 0,
      aborted: 0,
      timedOut: 0,
      invocationRejections: 0,
      invocationRejectionsPer1k: 0,
    },
    refs: new Map(),
    taskSequences: new Map(),
    churnPairs: 0,
    churnSum: 0,
  };
};

const accumulatorFor = (state: MeasureState, ref: string): RefAccumulator => {
  const existing = state.refs.get(ref);
  if (existing) return existing;
  const created: RefAccumulator = {
    calls: 0,
    succeeded: 0,
    failed: 0,
    signatures: new Map<string, number>(),
    stages: new Map<string, number>(),
    churnPairs: 0,
    churnSum: 0,
  };
  state.refs.set(ref, created);
  return created;
};

const accumulateTrace = (state: MeasureState, sourceTrace: EntropyTraceInput): void => {
  const taskKey = sourceTrace.taskKey ?? DEFAULT_TASK_KEY;
  const sequence: string[] = [];
  const pendingFailed = new Map<string, string>();
  for (const operation of sourceTrace.operations) {
    state.totals.operations += 1;
    if (operation.ref.startsWith(DISCOVERY_PREFIX)) {
      state.totals.discoveryOperations += 1;
      continue;
    }
    if (operation.ref.startsWith(WORKFLOW_PREFIX)) {
      state.totals.workflowOperations += 1;
      continue;
    }
    state.totals.actionOperations += 1;
    sequence.push(operation.ref);
    const acc = accumulatorFor(state, operation.ref);
    acc.calls += 1;
    if (operation.outcome === "succeeded") {
      state.totals.succeeded += 1;
      acc.succeeded += 1;
    } else if (operation.outcome === "failed") {
      state.totals.failed += 1;
      acc.failed += 1;
    } else if (operation.outcome === "aborted") {
      state.totals.aborted += 1;
    } else {
      state.totals.timedOut += 1;
    }
    const signature = shapeSignature(operation.args);
    acc.signatures.set(signature, (acc.signatures.get(signature) ?? 0) + 1);
    if (operation.outcome === "failed") {
      const stage = operation.failureStage ?? "unknown";
      acc.stages.set(stage, (acc.stages.get(stage) ?? 0) + 1);
      if (ENTROPY_INVOCATION_STAGES.includes(stage)) state.totals.invocationRejections += 1;
    }
    const pending = pendingFailed.get(operation.ref);
    if (pending !== undefined) {
      const distance = signatureDistance(pending, signature);
      acc.churnPairs += 1;
      acc.churnSum += distance;
      state.churnPairs += 1;
      state.churnSum += distance;
      pendingFailed.delete(operation.ref);
    }
    if (operation.outcome === "failed") pendingFailed.set(operation.ref, signature);
  }
  if (sequence.length > 0) {
    const key = sequence.join("→");
    const perTask = state.taskSequences.get(taskKey) ?? new Map<string, number>();
    perTask.set(key, (perTask.get(key) ?? 0) + 1);
    state.taskSequences.set(taskKey, perTask);
  }
};

const finalizeMeasure = (
  input: EntropyMeterInput,
  state: MeasureState,
): EntropyReportCore => {
  const refEntries = [...state.refs.entries()].sort(([left], [right]) =>
    compareCodeUnits(left, right),
  );
  const refReports: EntropyRefReport[] = [];
  let staticFreedomTotal = 0;
  let staticWeighted = 0;
  let shapeWeighted = 0;
  let failureStageWeighted = 0;
  for (const [ref, acc] of refEntries) {
    const shapeEntropyBits = shannonEntropyBits([...acc.signatures.values()]);
    const failureStageEntropyBits = shannonEntropyBits([...acc.stages.values()]);
    const refChurnRate = acc.churnPairs > 0 ? roundMetric(acc.churnSum / acc.churnPairs) : 0;
    const lexicon = state.lexiconByRef.get(ref) ?? 0;
    const staticFreedom = state.surfaceByRef.has(ref)
      ? staticFreedomFromSchema(state.surfaceByRef.get(ref))
      : 0;
    staticFreedomTotal += staticFreedom;
    staticWeighted += ENTROPY_WEIGHTS.staticFreedom * staticFreedom * acc.calls;
    shapeWeighted += shapeEntropyBits * acc.calls;
    failureStageWeighted += failureStageEntropyBits * acc.failed;
    const invocationRejections = ENTROPY_INVOCATION_STAGES.reduce(
      (sum, stage) => sum + (acc.stages.get(stage) ?? 0),
      0,
    );
    const score = roundMetric(invocationRejections / acc.calls);
    const shapeSignatures: EntropyShapeSignature[] = [...acc.signatures.entries()]
      .map(([signature, count]) => ({ signature, count }))
      .sort(
        (left, right) => right.count - left.count || compareCodeUnits(left.signature, right.signature),
      )
      .slice(0, TOP_SIGNATURES);
    refReports.push({
      ref,
      calls: acc.calls,
      succeeded: acc.succeeded,
      failed: acc.failed,
      shapeSignatures,
      shapeEntropyBits,
      failureStageEntropyBits,
      churnRate: refChurnRate,
      lexiconRows: lexicon,
      staticFreedom,
      score,
    });
  }

  const navigationRatio = state.totals.actionOperations > 0
    ? roundMetric(state.totals.discoveryOperations / state.totals.actionOperations)
    : 0;
  let flowWeighted = 0;
  let flowOccurrences = 0;
  for (const perTask of state.taskSequences.values()) {
    const counts = [...perTask.values()];
    const occurrences = counts.reduce((sum, value) => sum + value, 0);
    flowWeighted += shannonEntropyBits(counts) * occurrences;
    flowOccurrences += occurrences;
  }
  const flowEntropyBits = flowOccurrences > 0
    ? roundMetric(flowWeighted / flowOccurrences)
    : 0;
  state.totals.invocationRejectionsPer1k = state.totals.actionOperations > 0
    ? roundMetric((state.totals.invocationRejections / state.totals.actionOperations) * 1000)
    : 0;
  // Operational burden is a rejection fraction, not entropy or capability size.
  const score = state.totals.actionOperations > 0
    ? roundMetric(state.totals.invocationRejections / state.totals.actionOperations)
    : 0;
  // Call-weighted schema freedom is diagnostic only, independent of corpus size.
  const staticScore = state.totals.actionOperations > 0
    ? roundMetric(staticWeighted / state.totals.actionOperations)
    : 0;
  const behavioralScore = score;
  const sortedRefs = [...refReports].sort(
    (left, right) => right.score - left.score || compareCodeUnits(left.ref, right.ref),
  );

  return {
    metricVersion: ENTROPY_METRIC_VERSION,
    catalogDigest: input.catalogDigest ?? "",
    totals: state.totals,
    shapeEntropyBits: state.totals.actionOperations > 0
      ? roundMetric(shapeWeighted / state.totals.actionOperations)
      : 0,
    failureStageEntropyBits: state.totals.failed > 0
      ? roundMetric(failureStageWeighted / state.totals.failed)
      : 0,
    churnRate: state.churnPairs > 0 ? roundMetric(state.churnSum / state.churnPairs) : 0,
    navigationRatio,
    flowEntropyBits,
    lexiconRows: state.lexiconRows,
    staticFreedom: roundMetric(staticFreedomTotal),
    staticScore,
    behavioralScore,
    score,
    refs: sortedRefs,
  };
};

interface CachedTraceWindow {
  traces: readonly EntropyTraceInput[];
  state: MeasureState;
  models: Map<string, MeasureState>;
}

const mergeCounts = (target: Map<string, number>, source: Map<string, number>): void => {
  for (const [key, count] of source) target.set(key, (target.get(key) ?? 0) + count);
};

const mergeMeasureState = (target: MeasureState, source: MeasureState): void => {
  for (const key of Object.keys(source.totals) as (keyof EntropyTotals)[]) {
    target.totals[key] += source.totals[key];
  }
  target.churnPairs += source.churnPairs;
  target.churnSum += source.churnSum;
  for (const [ref, acc] of source.refs) {
    const into = accumulatorFor(target, ref);
    for (const key of ["calls", "succeeded", "failed", "churnPairs", "churnSum"] as const) into[key] += acc[key];
    mergeCounts(into.signatures, acc.signatures);
    mergeCounts(into.stages, acc.stages);
  }
  for (const [task, sequences] of source.taskSequences) {
    let into = target.taskSequences.get(task);
    if (!into) target.taskSequences.set(task, into = new Map());
    mergeCounts(into, sequences);
  }
};

/** Bounded, session-owned cache. Never pass mutable caller-owned traces here. */
export class SessionEntropyMeter {
  readonly #windows = new Map<string, CachedTraceWindow>();

  async measure(
    windows: readonly EntropyTraceWindow[],
    input: Omit<EntropyMeterInput, "traces">,
    signal?: AbortSignal,
  ): Promise<EntropyReport> {
    const reportInput = { ...input, traces: [] };
    const combined = createMeasureState(reportInput);
    const models = new Map<string, MeasureState>();
    for (const window of windows) {
      let cached = this.#windows.get(window.file);
      if (cached?.traces !== window.traces) {
        const append = cached && cached.traces.length <= window.traces.length &&
          cached.traces.every((trace, index) => trace === window.traces[index]);
        if (!append) cached = { traces: [], state: createMeasureState({ traces: [] }), models: new Map() };
        const entry = cached!;
        // Publish only a complete update; a failed accumulation cannot leave a
        // half-applied append available for the next turn.
        this.#windows.delete(window.file);
        for (let index = entry.traces.length; index < window.traces.length; index++) {
          const trace = window.traces[index]!;
          accumulateTrace(entry.state, trace);
          entry.state.totals.traces++;
          if (trace.model) {
            let model = entry.models.get(trace.model);
            if (!model) entry.models.set(trace.model, model = createMeasureState({ traces: [] }));
            accumulateTrace(model, trace);
            model.totals.traces++;
          }
          if ((index + 1) % COOPERATIVE_TRACE_CHUNK === 0) {
            await yieldToLoop();
            signal?.throwIfAborted();
          }
        }
        entry.traces = window.traces;
      }
      const entry = cached!;
      this.#windows.delete(window.file);
      this.#windows.set(window.file, entry);
      while (this.#windows.size > 16) this.#windows.delete(this.#windows.keys().next().value!);
      mergeMeasureState(combined, entry.state);
      for (const [name, source] of entry.models) {
        let model = models.get(name);
        if (!model) models.set(name, model = createMeasureState(reportInput));
        mergeMeasureState(model, source);
      }
      await yieldToLoop();
      signal?.throwIfAborted();
    }
    const report = finalizeMeasure(reportInput, combined);
    const byModel = [...models.entries()].sort(([a], [b]) => compareCodeUnits(a, b)).map(([model, state]) => {
      const scoped = finalizeMeasure(reportInput, state);
      return { model, operations: scoped.totals.operations, actionOperations: scoped.totals.actionOperations,
        succeeded: scoped.totals.succeeded, invocationRejections: scoped.totals.invocationRejections,
        invocationRejectionsPer1k: scoped.totals.invocationRejectionsPer1k, behavioralScore: scoped.behavioralScore };
    });
    return { ...report, byModel };
  }
}

const measureOnce = (input: EntropyMeterInput): EntropyReportCore => {
  const state = createMeasureState(input);
  for (const trace of input.traces) accumulateTrace(state, trace);
  return finalizeMeasure(input, state);
};

export const measureEntropy = (input: EntropyMeterInput): EntropyReport => {
  const report = measureOnce(input);
  const groups = new Map<string, EntropyTraceInput[]>();
  for (const sourceTrace of input.traces) {
    if (!sourceTrace.model) continue;
    const group = groups.get(sourceTrace.model) ?? [];
    group.push(sourceTrace);
    groups.set(sourceTrace.model, group);
  }
  // Only stamped traces attribute; everything else stays in the global
  // report. Sorted iteration keeps the breakdown order stable.
  const byModel: EntropyModelReport[] = [...groups.entries()]
    .sort(([left], [right]) => compareCodeUnits(left, right))
    .map(([model, traces]) => {
      const scoped = measureOnce({ ...input, traces });
      return {
        model,
        operations: scoped.totals.operations,
        actionOperations: scoped.totals.actionOperations,
        succeeded: scoped.totals.succeeded,
        invocationRejections: scoped.totals.invocationRejections,
        invocationRejectionsPer1k: scoped.totals.invocationRejectionsPer1k,
        behavioralScore: scoped.behavioralScore,
      };
    });
  return { ...report, byModel };
};

const yieldToLoop = (): Promise<void> =>
  new Promise((resolve) => setImmediate(resolve));

const COOPERATIVE_TRACE_CHUNK = 64;

const measureOnceAsync = async (
  input: EntropyMeterInput,
): Promise<EntropyReportCore> => {
  const state = createMeasureState(input);
  await yieldToLoop();
  for (let index = 0; index < input.traces.length; index += 1) {
    accumulateTrace(state, input.traces[index]!);
    if ((index + 1) % COOPERATIVE_TRACE_CHUNK === 0) await yieldToLoop();
  }
  return finalizeMeasure(input, state);
};

// Cooperative meter for extension hooks. Trace accumulation yields in fixed
// chunks in both the global and per-model passes, preserving exact synchronous
// results while letting Pi repaint and process input during a large corpus.
export const measureEntropyAsync = async (
  input: EntropyMeterInput,
): Promise<EntropyReport> => {
  const report = await measureOnceAsync(input);
  const groups = new Map<string, EntropyTraceInput[]>();
  for (const sourceTrace of input.traces) {
    if (!sourceTrace.model) continue;
    const group = groups.get(sourceTrace.model) ?? [];
    group.push(sourceTrace);
    groups.set(sourceTrace.model, group);
  }
  const byModel: EntropyModelReport[] = [];
  for (const [model, traces] of [...groups.entries()].sort(([left], [right]) =>
    compareCodeUnits(left, right),
  )) {
    const scoped = await measureOnceAsync({ ...input, traces });
    byModel.push({
      model,
      operations: scoped.totals.operations,
      actionOperations: scoped.totals.actionOperations,
      succeeded: scoped.totals.succeeded,
      invocationRejections: scoped.totals.invocationRejections,
      invocationRejectionsPer1k: scoped.totals.invocationRejectionsPer1k,
      behavioralScore: scoped.behavioralScore,
    });
  }
  return { ...report, byModel };
};
