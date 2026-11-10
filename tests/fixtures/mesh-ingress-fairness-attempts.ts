// Attempt controller for the physical ingress fairness probe. Only insufficient measured
// canonical contention is a precondition miss. Hold share is diagnostic, not admission:
// removing I/O under the lock must not invalidate an otherwise contended scenario.
// Setup, source, structural, deciding and cleanup failures are always final.
export const FAIRNESS_MAX_ATTEMPTS = 3;
export const CONTENTION_MIN_CONCURRENT_WAITERS = 2;
// Twelve repeated acquisitions (the fixture has 12 publisher loops), not one lucky snapshot.
// This counts acquisitions, not distinct loops or configured processes.
export const CONTENTION_MIN_ACQUISITIONS = 12;

export type CanonicalAcquisition = { began: string; ended: string; concurrentWaiters: number };
export type FairnessContention = {
  acquisitionsInWindow: number;
  acquisitionsWithConcurrentWaiters: number;
  maxConcurrentWaiters: number;
};

/** Existing canonical FIFO receipts, sampled by the holder at each actual acquisition. */
export const measureFairnessContention = (intervals: readonly CanonicalAcquisition[], from: bigint,
  durationMs: number): FairnessContention => {
  const until = from + BigInt(durationMs) * 1_000_000n;
  const measured: FairnessContention = { acquisitionsInWindow: 0, acquisitionsWithConcurrentWaiters: 0, maxConcurrentWaiters: 0 };
  for (const interval of intervals) {
    const began = BigInt(interval.began), ended = BigInt(interval.ended);
    if (ended < began || !Number.isSafeInteger(interval.concurrentWaiters) || interval.concurrentWaiters < 0) {
      throw new Error("invalid canonical acquisition metadata");
    }
    if (began < from || began >= until) continue;
    measured.acquisitionsInWindow += 1;
    measured.maxConcurrentWaiters = Math.max(measured.maxConcurrentWaiters, interval.concurrentWaiters);
    if (interval.concurrentWaiters >= CONTENTION_MIN_CONCURRENT_WAITERS) measured.acquisitionsWithConcurrentWaiters += 1;
  }
  return measured;
};

export const realContentionPresent = (measurement: FairnessContention): boolean =>
  measurement.maxConcurrentWaiters >= CONTENTION_MIN_CONCURRENT_WAITERS
  && measurement.acquisitionsWithConcurrentWaiters >= CONTENTION_MIN_ACQUISITIONS;

export const INGRESS_FAIRNESS_RESULT_LABEL = "INGRESS_FAIRNESS_RESULT";
export const INGRESS_FAIRNESS_INVALID_ATTEMPT_LABEL = "INGRESS_FAIRNESS_INVALID_ATTEMPT";

export type FairnessAttemptRecord<O> = { attempt: number; heldPct: number; contention: FairnessContention; valid: boolean; observation: O };

export type FairnessAttemptScenario<O> = {
  /** Runs one fresh scenario through its measurement. */
  run(attempt: number): Promise<O>;
  heldPct(observation: O): number;
  contention(observation: O): FairnessContention;
  /** Surfaces every measured attempt, before any further check can fail. */
  report(record: FairnessAttemptRecord<O>, invalid: readonly FairnessAttemptRecord<O>[]): void;
  /** Non-retryable checks that every measured attempt must pass, contended or not. */
  verify(observation: O): Promise<void>;
  /** Releases and awaits everything the attempt owns. Always settles before the next attempt. */
  cleanup(attempt: number): Promise<void>;
  /** The deciding assertions, run only for the first sufficiently contended attempt. */
  decide(record: FairnessAttemptRecord<O>, invalid: readonly FairnessAttemptRecord<O>[]): void;
};

const contentionMinimum = { concurrentWaiters: CONTENTION_MIN_CONCURRENT_WAITERS, acquisitions: CONTENTION_MIN_ACQUISITIONS };

export const fairnessAttemptLine = <O>(record: FairnessAttemptRecord<O>, invalid: readonly FairnessAttemptRecord<O>[],
  payload: Readonly<Record<string, unknown>>): string => {
  const label = record.valid ? INGRESS_FAIRNESS_RESULT_LABEL : INGRESS_FAIRNESS_INVALID_ATTEMPT_LABEL;
  return `${label} ${JSON.stringify({ ...payload, attempt: {
    number: record.attempt, maxAttempts: FAIRNESS_MAX_ATTEMPTS, heldSharePct: record.heldPct,
    validContention: record.valid, contention: record.contention, contentionMinimum,
    priorInvalidAttempts: invalid.map(prior => ({ attempt: prior.attempt, heldSharePct: prior.heldPct, contention: prior.contention })),
  } })}`;
};

const measureAttempt = async <O>(scenario: FairnessAttemptScenario<O>, attempt: number,
  invalid: readonly FairnessAttemptRecord<O>[]): Promise<FairnessAttemptRecord<O>> => {
  const observation = await scenario.run(attempt);
  const heldPct = scenario.heldPct(observation);
  if (!Number.isFinite(heldPct)) throw new Error(`ingress fairness attempt ${attempt} measured no finite hold share (${heldPct})`);
  const contention = scenario.contention(observation);
  if (Object.values(contention).some(value => !Number.isSafeInteger(value) || value < 0)
    || contention.acquisitionsWithConcurrentWaiters > contention.acquisitionsInWindow
    || (contention.acquisitionsInWindow === 0 && contention.maxConcurrentWaiters !== 0)
    || (contention.acquisitionsWithConcurrentWaiters > 0 && contention.maxConcurrentWaiters < CONTENTION_MIN_CONCURRENT_WAITERS)) {
    throw new Error(`ingress fairness attempt ${attempt} measured invalid canonical contention`);
  }
  const record = { attempt, heldPct, contention, valid: realContentionPresent(contention), observation };
  scenario.report(record, invalid);
  await scenario.verify(observation);
  return record;
};

/** Returns the deciding attempt once its assertions pass; throws on any final failure. */
export const runFairnessAttempts = async <O>(scenario: FairnessAttemptScenario<O>): Promise<FairnessAttemptRecord<O>> => {
  const invalid: FairnessAttemptRecord<O>[] = [];
  for (let attempt = 1; attempt <= FAIRNESS_MAX_ATTEMPTS; attempt += 1) {
    let outcome: { record: FairnessAttemptRecord<O> } | { error: unknown };
    try {
      outcome = { record: await measureAttempt(scenario, attempt, invalid) };
    } catch (error) {
      outcome = { error };
    }
    try {
      await scenario.cleanup(attempt);
    } catch (cleanupError) {
      if ("error" in outcome) {
        throw new AggregateError([outcome.error, cleanupError], `ingress fairness attempt ${attempt} failed and its cleanup also failed`);
      }
      throw cleanupError;
    }
    if ("error" in outcome) throw outcome.error;
    const { record } = outcome;
    if (record.valid) {
      scenario.decide(record, invalid);
      return record;
    }
    invalid.push(record);
  }
  throw new Error(`production-shaped publishers must make at least ${CONTENTION_MIN_ACQUISITIONS} canonical acquisitions `
    + `with at least ${CONTENTION_MIN_CONCURRENT_WAITERS} concurrent waiters in the observation window; `
    + `no attempt of ${FAIRNESS_MAX_ATTEMPTS} reached it: `
    + invalid.map(record => `attempt ${record.attempt} ${JSON.stringify(record.contention)} (${record.heldPct}% held)`).join(", "), { cause: invalid });
};
