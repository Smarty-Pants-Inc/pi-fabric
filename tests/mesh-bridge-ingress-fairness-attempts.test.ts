import { expect, it } from "vitest";
import {
  FAIRNESS_MAX_ATTEMPTS, fairnessAttemptLine, holdShareInBand, INGRESS_FAIRNESS_INVALID_ATTEMPT_LABEL,
  INGRESS_FAIRNESS_RESULT_LABEL, runFairnessAttempts, type FairnessAttemptRecord, type FairnessAttemptScenario,
} from "./fixtures/mesh-ingress-fairness-attempts.js";

// Deterministic cases for the attempt controller that the physical ingress fairness probe runs.
// Each scripted attempt owns a live resource that only its cleanup releases, after a real timer
// turn. A later attempt refuses to start while any earlier resource is live, so cleanup-before-retry
// is observed behaviour. Shares and progress times are ones the physical probe has measured.
const PROGRESS_BOUND_MS = 2_300; // the physical probe's unchanged observation window

type Script = { heldPct: number; progressAtMs?: number; runError?: Error; verifyError?: Error; cleanupError?: Error };
type Observation = { attempt: number; heldPct: number; progressAtMs: number | undefined };

const scripted = (scripts: Script[]) => {
  const events: string[] = [];
  const lines: string[] = [];
  const live = new Set<number>();
  const scenario: FairnessAttemptScenario<Observation> = {
    run: async attempt => {
      events.push(`run:${attempt}`);
      if (live.size > 0) throw new Error(`attempt ${attempt} started while attempt ${[...live].join(",")} was still live`);
      const script = scripts[attempt - 1];
      if (script === undefined) throw new Error(`no script for attempt ${attempt}`);
      live.add(attempt);
      await new Promise<void>(resolve => setImmediate(resolve));
      if (script.runError) throw script.runError;
      return { attempt, heldPct: script.heldPct, progressAtMs: script.progressAtMs };
    },
    heldPct: observation => observation.heldPct,
    report: (record, invalid) => {
      events.push(`report:${record.attempt}`);
      lines.push(fairnessAttemptLine(record, invalid, { durableIngressProgressAtMs: record.observation.progressAtMs ?? null }));
    },
    verify: async ({ attempt }) => {
      events.push(`verify:${attempt}`);
      await new Promise<void>(resolve => setImmediate(resolve));
      const error = scripts[attempt - 1]?.verifyError;
      if (error) throw error;
    },
    cleanup: async attempt => {
      events.push(`cleanup:${attempt}`);
      await new Promise<void>(resolve => setTimeout(resolve, 5));
      live.delete(attempt);
      events.push(`cleaned:${attempt}`);
      const error = scripts[attempt - 1]?.cleanupError;
      if (error) throw error;
    },
    decide: ({ attempt, observation }) => {
      events.push(`decide:${attempt}`);
      expect(observation.progressAtMs ?? Number.POSITIVE_INFINITY,
        `toLocal ingress must have a durable destination event and cursor checkpoint within ${PROGRESS_BOUND_MS} ms`)
        .toBeLessThanOrEqual(PROGRESS_BOUND_MS);
    },
  };
  const labelled = (label: string) => lines.filter(line => line.startsWith(`${label} `))
    .map(line => JSON.parse(line.slice(label.length + 1)) as { durableIngressProgressAtMs: number | null;
      attempt: { number: number; maxAttempts: number; heldSharePct: number; validHoldShareBand: boolean;
        holdShareBandPct: number[]; priorInvalidAttempts: { attempt: number; heldSharePct: number }[] } });
  return {
    scenario, events, lines, live,
    results: () => labelled(INGRESS_FAIRNESS_RESULT_LABEL),
    invalidLines: () => labelled(INGRESS_FAIRNESS_INVALID_ATTEMPT_LABEL),
    runs: () => events.filter(event => event.startsWith("run:")),
  };
};

const cycle = (attempt: number): string[] =>
  [`run:${attempt}`, `report:${attempt}`, `verify:${attempt}`, `cleanup:${attempt}`, `cleaned:${attempt}`];

const rejection = async (work: Promise<unknown>): Promise<unknown> => work.then(
  value => { throw new Error(`expected a rejection, got ${JSON.stringify(value)}`); },
  (error: unknown) => error);

it("allows three attempts in total and treats 50 and 70 percent as inside the band", () => {
  expect(FAIRNESS_MAX_ATTEMPTS).toBe(3);
  expect([49.99, 50, 62.11, 70, 70.01, Number.NaN, Number.POSITIVE_INFINITY].map(holdShareInBand))
    .toEqual([false, true, true, true, false, false, false]);
});

it("cleans up a below-band attempt before a fresh valid-band attempt passes", async () => {
  const run = scripted([
    { heldPct: 48.3, progressAtMs: 145.8 },
    { heldPct: 62.11, progressAtMs: 134.9 },
    { heldPct: 62.52, progressAtMs: 142.6 },
  ]);
  const accepted = await runFairnessAttempts(run.scenario);
  expect(accepted).toMatchObject({ attempt: 2, heldPct: 62.11, valid: true });
  expect(run.events).toEqual([...cycle(1), ...cycle(2), "decide:2"]);
  expect(run.live.size).toBe(0);
  expect(run.invalidLines()).toEqual([{ durableIngressProgressAtMs: 145.8, attempt: { number: 1, maxAttempts: 3,
    heldSharePct: 48.3, validHoldShareBand: false, holdShareBandPct: [50, 70], priorInvalidAttempts: [] } }]);
  expect(run.results()).toEqual([{ durableIngressProgressAtMs: 134.9, attempt: { number: 2, maxAttempts: 3,
    heldSharePct: 62.11, validHoldShareBand: true, holdShareBandPct: [50, 70],
    priorInvalidAttempts: [{ attempt: 1, heldSharePct: 48.3 }] } }]);
  expect(run.lines).toHaveLength(2);
});

it("accepts the inclusive band edge and the inclusive progress bound", async () => {
  const run = scripted([{ heldPct: 49.99, progressAtMs: 120 }, { heldPct: 70, progressAtMs: PROGRESS_BOUND_MS }]);
  await expect(runFairnessAttempts(run.scenario)).resolves.toMatchObject({ attempt: 2, heldPct: 70 });
  expect(run.events).toEqual([...cycle(1), ...cycle(2), "decide:2"]);
});

it("fails an above-band miss followed by a valid-band progress violation without a third attempt", async () => {
  const run = scripted([
    { heldPct: 75.36, progressAtMs: 650.3 },
    { heldPct: 56.23, progressAtMs: 3_347.5 },
    { heldPct: 62.11, progressAtMs: 134.9 },
  ]);
  const error = await rejection(runFairnessAttempts(run.scenario));
  expect(String(error)).toContain(`within ${PROGRESS_BOUND_MS} ms`);
  expect(run.events).toEqual([...cycle(1), ...cycle(2), "decide:2"]);
  expect(run.runs()).toEqual(["run:1", "run:2"]);
  expect(run.live.size).toBe(0);
  expect(run.invalidLines().map(line => line.attempt.heldSharePct)).toEqual([75.36]);
  expect(run.results()).toHaveLength(1);
  expect(run.results()[0]).toMatchObject({ durableIngressProgressAtMs: 3_347.5,
    attempt: { number: 2, heldSharePct: 56.23, priorInvalidAttempts: [{ attempt: 1, heldSharePct: 75.36 }] } });
});

it("never retries a progress violation in the first attempt, which is already in the band", async () => {
  const run = scripted([{ heldPct: 56.23, progressAtMs: 3_347.5 }, { heldPct: 62.11, progressAtMs: 134.9 }]);
  const error = await rejection(runFairnessAttempts(run.scenario));
  expect(String(error)).toContain(`within ${PROGRESS_BOUND_MS} ms`);
  expect(run.events).toEqual([...cycle(1), "decide:1"]);
  expect(run.invalidLines()).toEqual([]);
  expect(run.results().map(line => line.attempt.number)).toEqual([1]);
});

it("fails after three invalid attempts and surfaces every share and observation", async () => {
  const run = scripted([
    { heldPct: 48.3, progressAtMs: 145.8 },
    { heldPct: 75.36, progressAtMs: 650.3 },
    { heldPct: 70.01, progressAtMs: 3_347.5 },
    { heldPct: 62.11, progressAtMs: 134.9 },
  ]);
  const error = await rejection(runFairnessAttempts(run.scenario));
  expect(error).toBeInstanceOf(Error);
  const { message, cause } = error as Error;
  expect(message).toContain("must hold canonical .lock for 50–70% of the measured scenario");
  expect(message).toContain("attempt 1 48.3%, attempt 2 75.36%, attempt 3 70.01%");
  expect((cause as FairnessAttemptRecord<Observation>[]).map(record =>
    [record.attempt, record.valid, record.observation.progressAtMs])).toEqual([[1, false, 145.8], [2, false, 650.3], [3, false, 3_347.5]]);
  expect(run.events).toEqual([...cycle(1), ...cycle(2), ...cycle(3)]);
  expect(run.live.size).toBe(0);
  expect(run.results()).toEqual([]);
  expect(run.invalidLines().map(line => [line.attempt.number, line.attempt.priorInvalidAttempts.length,
    line.durableIngressProgressAtMs])).toEqual([[1, 0, 145.8], [2, 1, 650.3], [3, 2, 3_347.5]]);
});

it("awaits cleanup after a setup error and does not retry it as a precondition miss", async () => {
  const setup = new Error("contender 0 exited 1/null");
  const run = scripted([{ heldPct: 48.3, runError: setup }, { heldPct: 62.11, progressAtMs: 134.9 }]);
  await expect(runFairnessAttempts(run.scenario)).rejects.toBe(setup);
  expect(run.events).toEqual(["run:1", "cleanup:1", "cleaned:1"]);
  expect(run.live.size).toBe(0);
  expect(run.lines).toEqual([]);
});

it("keeps a failed structural check on an out-of-band attempt final, with its observation reported", async () => {
  const structural = new Error("expected 167 intervals to have a length of 168");
  const run = scripted([{ heldPct: 48.3, progressAtMs: 145.8, verifyError: structural }, { heldPct: 62.11, progressAtMs: 134.9 }]);
  await expect(runFairnessAttempts(run.scenario)).rejects.toBe(structural);
  expect(run.events).toEqual(cycle(1));
  expect(run.invalidLines().map(line => line.attempt.heldSharePct)).toEqual([48.3]);
  expect(run.results()).toEqual([]);
});

it("stops before any retry when cleanup of an invalid attempt fails", async () => {
  const teardown = new Error("bridge shutdown timed out after 10 s");
  const run = scripted([{ heldPct: 75.36, progressAtMs: 650.3, cleanupError: teardown }, { heldPct: 62.11, progressAtMs: 134.9 }]);
  await expect(runFairnessAttempts(run.scenario)).rejects.toBe(teardown);
  expect(run.events).toEqual(cycle(1));
});

it("surfaces both errors when an attempt and its cleanup both fail", async () => {
  const setup = new Error("SQLite writer exited before SQLITE_HELD");
  const teardown = new Error("scratch removal failed");
  const run = scripted([{ heldPct: 48.3, runError: setup, cleanupError: teardown }, { heldPct: 62.11, progressAtMs: 134.9 }]);
  const error = await rejection(runFairnessAttempts(run.scenario));
  expect(error).toBeInstanceOf(AggregateError);
  const { errors } = error as AggregateError;
  expect(errors).toHaveLength(2);
  expect(errors[0]).toBe(setup);
  expect(errors[1]).toBe(teardown);
  expect(run.events).toEqual(["run:1", "cleanup:1", "cleaned:1"]);
});

it("does not retry a share that is not a finite measurement", async () => {
  const run = scripted([{ heldPct: Number.NaN, progressAtMs: 120 }, { heldPct: 62.11, progressAtMs: 134.9 }]);
  await expect(runFairnessAttempts(run.scenario)).rejects.toThrow("measured no finite hold share");
  expect(run.events).toEqual(["run:1", "cleanup:1", "cleaned:1"]);
  expect(run.lines).toEqual([]);
});
