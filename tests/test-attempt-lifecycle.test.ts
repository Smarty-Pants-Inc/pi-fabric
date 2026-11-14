import { createHook } from "node:async_hooks";
import { getEventListeners } from "node:events";
import { setImmediate as nextTurn, setTimeout as delay } from "node:timers/promises";
import { expect, it, vi } from "vitest";
import { observeTestAttempt } from "./helpers/test-attempt-lifecycle.js";

// Keep reason: RFL pi-fabric#1977 F1 caught Vitest 4.1's reused aborted signal.
// Harmless readiness polling from the RFL experiment; cancellation/cleanup logic
// is the ACTUAL helper used by residency-stopped-writer-retention.test.ts.
const waitForAttempt = (signal: AbortSignal) => async (predicate: () => boolean) => {
  signal.throwIfAborted();
  const deadline = Date.now() + 30_000;
  while (!predicate() && Date.now() < deadline) await delay(10, undefined, { signal });
  signal.throwIfAborted();
  expect(predicate()).toBe(true);
};

// These retries/timeouts are intentional regression stimuli, NOT claims that the
// hosted stopped-writer integration rows pass on their first attempts.
for (const timeouts of [1, 2]) {
  let attempts = 0;
  let cleanupJoined = 0;
  let successfulReady = false;
  const cancellations: boolean[] = [];
  let sharedSignal: AbortSignal | undefined;
  it.for([timeouts])("intentionally times out %s attempt(s), joins cleanup, then succeeds with a ready retry", {
    timeout: 100, retry: timeouts,
  }, async (_, context) => {
    const attemptNumber = ++attempts;
    expect(cleanupJoined).toBe(attemptNumber - 1);
    if (sharedSignal) {
      expect(context.signal).toBe(sharedSignal);
      expect(context.signal.aborted).toBe(true);
    } else sharedSignal = context.signal;
    const listenersBefore = getEventListeners(context.signal, "abort").length;
    const attempt = observeTestAttempt(context, 1_000, waitForAttempt);
    const ready = attemptNumber > timeouts;
    try {
      await attempt.waitFor(() => ready);
      successfulReady = true;
      expect(attempt.signal.aborted).toBe(false);
      expect(attemptNumber).toBe(timeouts + 1);
    } finally {
      try {
        cancellations.push(attempt.isCancelled());
        // Force an asynchronous finally, which Vitest's callback timeout alone
        // does not join. No later attempt may start until this completes.
        await delay(10);
        cleanupJoined = attemptNumber;
      } finally { attempt.finishCleanup(); }
      expect(getEventListeners(context.signal, "abort").length).toBeLessThanOrEqual(listenersBefore);
    }
  });
  it(`completed all ${timeouts + 1} intentional attempts and their cleanup`, () => {
    expect(attempts).toBe(timeouts + 1);
    expect(cleanupJoined).toBe(attempts);
    expect(successfulReady).toBe(true);
    expect(cancellations).toEqual([...Array<boolean>(timeouts).fill(true), false]);
  });
}

it("forwards a live context abort promptly and disposes its polling timer/listeners", async (context) => {
  const source = new AbortController();
  const reason = new Error("cancel this attempt");
  const predicate = vi.fn(() => false);
  const attempt = observeTestAttempt({ signal: source.signal, task: context.task, onTestFinished: context.onTestFinished }, 1_000, waitForAttempt);
  const timers = new Set<number>();
  let sampling = true;
  const hook = createHook({
    init(id, type) { if (sampling && type === "Timeout") timers.add(id); },
    destroy(id) { timers.delete(id); },
  }).enable();
  try {
    const waiting = attempt.waitFor(predicate);
    sampling = false;
    expect(timers.size).toBe(1);
    expect(getEventListeners(source.signal, "abort")).toHaveLength(1);
    const started = performance.now();
    source.abort(reason);
    await expect(waiting).rejects.toMatchObject({ name: "AbortError", cause: reason });
    expect(performance.now() - started).toBeLessThan(1_000);
    expect(attempt.isCancelled()).toBe(true);
    expect(predicate).toHaveBeenCalledTimes(1);
    await nextTurn();
    expect(timers.size).toBe(0);
    expect(getEventListeners(source.signal, "abort")).toHaveLength(0);
    expect(getEventListeners(attempt.signal, "abort")).toHaveLength(0);
  } finally {
    attempt.finishCleanup();
    hook.disable();
  }
});

it("a pre-aborted waiter rejects before readiness without allocating a timer or listener", async () => {
  const source = new AbortController();
  const reason = new Error("already cancelled");
  source.abort(reason);
  const predicate = vi.fn(() => true);
  let timers = 0;
  const hook = createHook({ init(_id, type) { if (type === "Timeout") timers++; } }).enable();
  try {
    await expect(waitForAttempt(source.signal)(predicate)).rejects.toBe(reason);
    expect(predicate).not.toHaveBeenCalled();
    expect(timers).toBe(0);
    expect(getEventListeners(source.signal, "abort")).toHaveLength(0);
  } finally { hook.disable(); }
});

it("observes run-cancellation skip state when a stale context cannot emit another abort", async (context) => {
  const source = new AbortController();
  source.abort(new Error("previous attempt timed out"));
  // Installed runner.cancel first aborts the shared controller, then marks each
  // running task skipped. Model that public task transition, not a second abort
  // event (an already-aborted controller cannot emit one).
  const task = { ...context.task, mode: "run" as const };
  const attempt = observeTestAttempt({ signal: source.signal, task, onTestFinished: context.onTestFinished }, 1_000, waitForAttempt);
  const predicate = vi.fn(() => false);
  try {
    const waiting = attempt.waitFor(predicate);
    Object.assign(task, { mode: "skip" });
    await expect(waiting).rejects.toThrow("Test run cancelled");
    expect(attempt.signal.aborted).toBe(true);
    expect(attempt.isCancelled()).toBe(true);
    expect(predicate).toHaveBeenCalledTimes(1);
  } finally { attempt.finishCleanup(); }
});

it("removes a live context bridge when successful cleanup finishes", async (context) => {
  const source = new AbortController();
  const attempt = observeTestAttempt({ signal: source.signal, task: context.task, onTestFinished: context.onTestFinished }, 1_000, waitForAttempt);
  try {
    await attempt.waitFor(() => true);
    expect(attempt.isCancelled()).toBe(false);
    expect(getEventListeners(source.signal, "abort")).toHaveLength(1);
  } finally { attempt.finishCleanup(); }
  expect(getEventListeners(source.signal, "abort")).toHaveLength(0);
});
