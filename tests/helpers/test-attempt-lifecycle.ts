import type { TestContext } from "vitest";

type WaitForAttempt = (signal: AbortSignal) => (predicate: () => boolean) => Promise<void>;

// Vitest 4.1 reuses context.signal after a timeout. Each callback needs its own
// controller: only a context signal still live at entry can forward new aborts.
export const observeTestAttempt = (
  context: Pick<TestContext, "signal" | "task" | "onTestFinished">,
  timeout: number,
  waitForAttempt: WaitForAttempt,
) => {
  const controller = new AbortController();
  const forwardAbort = () => controller.abort(context.signal.reason);
  if (!context.signal.aborted) context.signal.addEventListener("abort", forwardAbort, { once: true });

  // Run cancellation marks running tasks skipped, even when the shared signal
  // was already aborted. Observe that public task state in the existing polling
  // loop (no extra timer), and before deciding whether finalizers may touch spies.
  const isCancelled = () => {
    if (context.task.mode === "skip") controller.abort(new Error("Test run cancelled"));
    return controller.signal.aborted;
  };
  const wait = waitForAttempt(controller.signal);
  const waitFor = (predicate: () => boolean) => wait(() => {
    isCancelled();
    controller.signal.throwIfAborted();
    return predicate();
  });

  let resolveCleanup!: () => void;
  const finished = new Promise<void>(resolve => { resolveCleanup = resolve; });
  context.onTestFinished(() => {
    // onTestFinished runs BEFORE onTestFailed. Abort synchronously before joining
    // finally, including a second timeout whose stale context emits no event.
    controller.abort();
    return finished;
  }, timeout);
  const finishCleanup = () => {
    context.signal.removeEventListener("abort", forwardAbort);
    resolveCleanup();
  };
  return { signal: controller.signal, waitFor, isCancelled, finishCleanup };
};
