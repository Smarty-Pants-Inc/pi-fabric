import { expect, it } from "vitest";
import {
  CONTENTION_MIN_ACQUISITIONS, CONTENTION_MIN_CONCURRENT_WAITERS, FAIRNESS_MAX_ATTEMPTS,
  fairnessAttemptLine, measureFairnessContention, realContentionPresent, INGRESS_FAIRNESS_INVALID_ATTEMPT_LABEL,
  INGRESS_FAIRNESS_RESULT_LABEL, runFairnessAttempts, type CanonicalAcquisition, type FairnessContention,
  type FairnessAttemptRecord, type FairnessAttemptScenario,
} from "./fixtures/mesh-ingress-fairness-attempts.js";

// Keep reason: failure paths (source errors, cleanup failures and contention misses) cannot be
// reached cheaply/reliably in the physical multiprocess probe. Scripted attempts own live resources;
// a later attempt cannot start until cleanup has actually settled, after a real timer turn.
const PROGRESS_BOUND_MS = 2_300; // the physical probe's unchanged observation window
const START = 1_000_000_000n;
const acquisitions = (count: number, concurrentWaiters: number, startMs = 0): CanonicalAcquisition[] =>
  Array.from({ length: count }, (_, index) => {
    const began = START + BigInt(startMs + index * 20) * 1_000_000n;
    return { began: began.toString(), ended: (began + 14_000_000n).toString(), concurrentWaiters };
  });
const ENOUGH = measureFairnessContention(acquisitions(12, 2), START, PROGRESS_BOUND_MS);
const LOW = measureFairnessContention(acquisitions(168, 0), START, PROGRESS_BOUND_MS);

type Script = { heldPct: number; contention?: FairnessContention; progressAtMs?: number | undefined;
  runError?: Error; verifyError?: Error; cleanupError?: Error };
type Observation = { attempt: number; heldPct: number; contention: FairnessContention; progressAtMs: number | undefined };

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
      return { attempt, heldPct: script.heldPct, contention: script.contention ?? ENOUGH, progressAtMs: script.progressAtMs };
    },
    heldPct: observation => observation.heldPct,
    contention: observation => observation.contention,
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
      attempt: { number: number; maxAttempts: number; heldSharePct: number; validContention: boolean;
        contention: FairnessContention; contentionMinimum: { concurrentWaiters: number; acquisitions: number };
        priorInvalidAttempts: { attempt: number; heldSharePct: number; contention: FairnessContention }[] } });
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

it("requires repeated acquisitions with at least two concurrent canonical waiters, not configured processes or hold share", () => {
  expect(FAIRNESS_MAX_ATTEMPTS).toBe(3);
  expect(CONTENTION_MIN_CONCURRENT_WAITERS).toBe(2);
  expect(CONTENTION_MIN_ACQUISITIONS).toBe(12);
  expect(ENOUGH).toEqual({ acquisitionsInWindow: 12, acquisitionsWithConcurrentWaiters: 12, maxConcurrentWaiters: 2 });
  const oneWaiter = measureFairnessContention(acquisitions(168, 1), START, PROGRESS_BOUND_MS);
  const oneSnapshot = measureFairnessContention([...acquisitions(1, 20), ...acquisitions(167, 0)], START, PROGRESS_BOUND_MS);
  const tooFew = measureFairnessContention(acquisitions(11, 2), START, PROGRESS_BOUND_MS);
  const outsideWindow = measureFairnessContention(acquisitions(12, 2, PROGRESS_BOUND_MS), START, PROGRESS_BOUND_MS);
  expect([LOW, oneWaiter, oneSnapshot, tooFew, outsideWindow, ENOUGH].map(realContentionPresent))
    .toEqual([false, false, false, false, false, true]);
});

it("counts only acquisitions beginning inside the observation window, including its start but not its end", () => {
  const measured = measureFairnessContention([
    ...acquisitions(1, 10, -1), ...acquisitions(1, 2, 0), ...acquisitions(1, 3, PROGRESS_BOUND_MS - 1),
    ...acquisitions(12, 99, PROGRESS_BOUND_MS),
  ], START, PROGRESS_BOUND_MS);
  expect(measured).toEqual({ acquisitionsInWindow: 2, acquisitionsWithConcurrentWaiters: 2, maxConcurrentWaiters: 3 });
});

it("rejects malformed acquisition metadata rather than retrying it as low contention", () => {
  for (const concurrentWaiters of [-1, 1.5, Number.NaN, Number.POSITIVE_INFINITY]) {
    expect(() => measureFairnessContention(acquisitions(12, concurrentWaiters), START, PROGRESS_BOUND_MS))
      .toThrow("invalid canonical acquisition metadata");
  }
  expect(() => measureFairnessContention([{ began: "2", ended: "1", concurrentWaiters: 2 }], START, PROGRESS_BOUND_MS))
    .toThrow("invalid canonical acquisition metadata");
});

it("marks a harmless low-contention fixture INVALID even at the old accepted share, then accepts lower-share real contention", async () => {
  const run = scripted([
    { heldPct: 61.77, contention: LOW, progressAtMs: 161.8 },
    { heldPct: 42.09, progressAtMs: 86 },
    { heldPct: 62.52, progressAtMs: 142.6 },
  ]);
  const accepted = await runFairnessAttempts(run.scenario);
  expect(accepted).toMatchObject({ attempt: 2, heldPct: 42.09, contention: ENOUGH, valid: true });
  expect(run.events).toEqual([...cycle(1), ...cycle(2), "decide:2"]);
  expect(run.live.size).toBe(0);
  expect(run.invalidLines()).toEqual([{ durableIngressProgressAtMs: 161.8, attempt: { number: 1, maxAttempts: 3,
    heldSharePct: 61.77, validContention: false, contention: LOW,
    contentionMinimum: { concurrentWaiters: 2, acquisitions: 12 }, priorInvalidAttempts: [] } }]);
  expect(run.results()).toEqual([{ durableIngressProgressAtMs: 86, attempt: { number: 2, maxAttempts: 3,
    heldSharePct: 42.09, validContention: true, contention: ENOUGH, contentionMinimum: { concurrentWaiters: 2, acquisitions: 12 },
    priorInvalidAttempts: [{ attempt: 1, heldSharePct: 61.77, contention: LOW }] } }]);
  expect(run.lines).toHaveLength(2);
});

it.each([42.09, 48.9, 61.77, 75.36])("keeps %s percent diagnostic and accepts the inclusive progress bound under actual contention", async heldPct => {
  const run = scripted([{ heldPct, progressAtMs: PROGRESS_BOUND_MS }]);
  await expect(runFairnessAttempts(run.scenario)).resolves.toMatchObject({ attempt: 1, heldPct });
  expect(run.events).toEqual([...cycle(1), "decide:1"]);
});

it("makes a deciding progress failure FINAL after an insufficient-contention attempt", async () => {
  const run = scripted([
    { heldPct: 61.77, contention: LOW, progressAtMs: 161.8 },
    { heldPct: 44.08, progressAtMs: 3_347.5 },
    { heldPct: 62.11, progressAtMs: 134.9 },
  ]);
  const error = await rejection(runFairnessAttempts(run.scenario));
  expect(String(error)).toContain(`within ${PROGRESS_BOUND_MS} ms`);
  expect(run.events).toEqual([...cycle(1), ...cycle(2), "decide:2"]);
  expect(run.runs()).toEqual(["run:1", "run:2"]);
  expect(run.live.size).toBe(0);
  expect(run.invalidLines().map(line => line.attempt.heldSharePct)).toEqual([61.77]);
  expect(run.results()).toHaveLength(1);
});

it.each([3_347.5, undefined])("never retries a deciding first-attempt progress violation (%s), even below the old share band", async progressAtMs => {
  const run = scripted([{ heldPct: 42.09, progressAtMs }, { heldPct: 62.11, progressAtMs: 134.9 }]);
  const error = await rejection(runFairnessAttempts(run.scenario));
  expect(String(error)).toContain(`within ${PROGRESS_BOUND_MS} ms`);
  expect(run.events).toEqual([...cycle(1), "decide:1"]);
  expect(run.invalidLines()).toEqual([]);
  expect(run.results().map(line => line.attempt.number)).toEqual([1]);
});

it("fails after three insufficient-contention attempts and surfaces every measurement", async () => {
  const run = scripted([
    { heldPct: 48.3, contention: LOW, progressAtMs: 145.8 },
    { heldPct: 75.36, contention: LOW, progressAtMs: 650.3 },
    { heldPct: 61.77, contention: LOW, progressAtMs: 3_347.5 },
    { heldPct: 62.11, progressAtMs: 134.9 },
  ]);
  const error = await rejection(runFairnessAttempts(run.scenario));
  expect(error).toBeInstanceOf(Error);
  const { message, cause } = error as Error;
  expect(message).toContain("at least 12 canonical acquisitions with at least 2 concurrent waiters in the observation window");
  expect((cause as FairnessAttemptRecord<Observation>[]).map(record =>
    [record.attempt, record.valid, record.contention])).toEqual([[1, false, LOW], [2, false, LOW], [3, false, LOW]]);
  expect(run.events).toEqual([...cycle(1), ...cycle(2), ...cycle(3)]);
  expect(run.live.size).toBe(0);
  expect(run.results()).toEqual([]);
  expect(run.invalidLines().map(line => [line.attempt.number, line.attempt.priorInvalidAttempts.length,
    line.durableIngressProgressAtMs])).toEqual([[1, 0, 145.8], [2, 1, 650.3], [3, 2, 3_347.5]]);
});

it.each(["contender 0 exited 1/null", "SourceDedupeScanChanged"])("keeps setup/source failure %s final and awaits cleanup", async message => {
  const setup = new Error(message);
  const run = scripted([{ heldPct: 48.3, contention: LOW, runError: setup }, { heldPct: 62.11, progressAtMs: 134.9 }]);
  await expect(runFairnessAttempts(run.scenario)).rejects.toBe(setup);
  expect(run.events).toEqual(["run:1", "cleanup:1", "cleaned:1"]);
  expect(run.live.size).toBe(0);
  expect(run.lines).toEqual([]);
});

it.each(["expected 167 intervals to have a length of 168", "SourceDedupeScanChanged"])("keeps structural/source failure %s on a low-contention attempt final", async message => {
  const structural = new Error(message);
  const run = scripted([{ heldPct: 48.3, contention: LOW, progressAtMs: 145.8, verifyError: structural },
    { heldPct: 62.11, progressAtMs: 134.9 }]);
  await expect(runFairnessAttempts(run.scenario)).rejects.toBe(structural);
  expect(run.events).toEqual(cycle(1));
  expect(run.invalidLines().map(line => line.attempt.heldSharePct)).toEqual([48.3]);
  expect(run.results()).toEqual([]);
});

it("stops before any retry when cleanup of an invalid attempt fails", async () => {
  const teardown = new Error("bridge shutdown timed out after 10 s");
  const run = scripted([{ heldPct: 75.36, contention: LOW, progressAtMs: 650.3, cleanupError: teardown },
    { heldPct: 62.11, progressAtMs: 134.9 }]);
  await expect(runFairnessAttempts(run.scenario)).rejects.toBe(teardown);
  expect(run.events).toEqual(cycle(1));
});

it("surfaces both errors when an attempt and its cleanup both fail", async () => {
  const setup = new Error("SQLite writer exited before SQLITE_HELD");
  const teardown = new Error("scratch removal failed");
  const run = scripted([{ heldPct: 48.3, contention: LOW, runError: setup, cleanupError: teardown },
    { heldPct: 62.11, progressAtMs: 134.9 }]);
  const error = await rejection(runFairnessAttempts(run.scenario));
  expect(error).toBeInstanceOf(AggregateError);
  expect((error as AggregateError).errors).toEqual([setup, teardown]);
  expect(run.events).toEqual(["run:1", "cleanup:1", "cleaned:1"]);
});

it("does not retry a share that is not a finite diagnostic measurement", async () => {
  const run = scripted([{ heldPct: Number.NaN, progressAtMs: 120 }, { heldPct: 62.11, progressAtMs: 134.9 }]);
  await expect(runFairnessAttempts(run.scenario)).rejects.toThrow("measured no finite hold share");
  expect(run.events).toEqual(["run:1", "cleanup:1", "cleaned:1"]);
  expect(run.lines).toEqual([]);
});

it.each([
  { ...ENOUGH, acquisitionsWithConcurrentWaiters: 13 },
  { ...ENOUGH, maxConcurrentWaiters: 1 },
  { ...ENOUGH, acquisitionsInWindow: Number.NaN },
])("does not retry malformed measured contention %j", async contention => {
  const run = scripted([{ heldPct: 42.09, contention, progressAtMs: 86 }, { heldPct: 61.77, progressAtMs: 134.9 }]);
  await expect(runFairnessAttempts(run.scenario)).rejects.toThrow("measured invalid canonical contention");
  expect(run.events).toEqual(["run:1", "cleanup:1", "cleaned:1"]);
  expect(run.lines).toEqual([]);
});
