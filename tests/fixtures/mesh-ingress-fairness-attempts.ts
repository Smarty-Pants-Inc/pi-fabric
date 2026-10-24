// Attempt controller for the physical ingress fairness probe. Only a measured canonical .lock
// hold-share outside 50–70% is a precondition miss; that attempt is reported, fully cleaned up,
// and replaced by a fresh scenario, with at most three attempts in total. The first attempt in the
// band decides: its assertions run once and a failure there is final. Setup, structural and
// cleanup failures are never treated as precondition misses.
export const FAIRNESS_MAX_ATTEMPTS = 3;
const HOLD_SHARE_MIN_PCT = 50;
const HOLD_SHARE_MAX_PCT = 70;

export const INGRESS_FAIRNESS_RESULT_LABEL = "INGRESS_FAIRNESS_RESULT";
export const INGRESS_FAIRNESS_INVALID_ATTEMPT_LABEL = "INGRESS_FAIRNESS_INVALID_ATTEMPT";

export type FairnessAttemptRecord<O> = { attempt: number; heldPct: number; valid: boolean; observation: O };

export type FairnessAttemptScenario<O> = {
  /** Runs one fresh scenario through its measurement. */
  run(attempt: number): Promise<O>;
  heldPct(observation: O): number;
  /** Surfaces every measured attempt, before any further check can fail. */
  report(record: FairnessAttemptRecord<O>, invalid: readonly FairnessAttemptRecord<O>[]): void;
  /** Non-retryable checks that every measured attempt must pass, in band or not. */
  verify(observation: O): Promise<void>;
  /** Releases and awaits everything the attempt owns. Always settles before the next attempt. */
  cleanup(attempt: number): Promise<void>;
  /** The deciding assertions, run only for the first attempt in the band. */
  decide(record: FairnessAttemptRecord<O>, invalid: readonly FairnessAttemptRecord<O>[]): void;
};

export const holdShareInBand = (heldPct: number): boolean =>
  Number.isFinite(heldPct) && heldPct >= HOLD_SHARE_MIN_PCT && heldPct <= HOLD_SHARE_MAX_PCT;

export const fairnessAttemptLine = <O>(record: FairnessAttemptRecord<O>, invalid: readonly FairnessAttemptRecord<O>[],
  payload: Readonly<Record<string, unknown>>): string => {
  const label = record.valid ? INGRESS_FAIRNESS_RESULT_LABEL : INGRESS_FAIRNESS_INVALID_ATTEMPT_LABEL;
  return `${label} ${JSON.stringify({ ...payload, attempt: {
    number: record.attempt, maxAttempts: FAIRNESS_MAX_ATTEMPTS, heldSharePct: record.heldPct,
    validHoldShareBand: record.valid, holdShareBandPct: [HOLD_SHARE_MIN_PCT, HOLD_SHARE_MAX_PCT],
    priorInvalidAttempts: invalid.map(prior => ({ attempt: prior.attempt, heldSharePct: prior.heldPct })),
  } })}`;
};

const measureAttempt = async <O>(scenario: FairnessAttemptScenario<O>, attempt: number,
  invalid: readonly FairnessAttemptRecord<O>[]): Promise<FairnessAttemptRecord<O>> => {
  const observation = await scenario.run(attempt);
  const heldPct = scenario.heldPct(observation);
  if (!Number.isFinite(heldPct)) throw new Error(`ingress fairness attempt ${attempt} measured no finite hold share (${heldPct})`);
  const record = { attempt, heldPct, valid: holdShareInBand(heldPct), observation };
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
  throw new Error(`production-shaped publishers must hold canonical .lock for ${HOLD_SHARE_MIN_PCT}–${HOLD_SHARE_MAX_PCT}% `
    + `of the measured scenario; no attempt of ${FAIRNESS_MAX_ATTEMPTS} reached it: `
    + invalid.map(record => `attempt ${record.attempt} ${record.heldPct}%`).join(", "), { cause: invalid });
};
