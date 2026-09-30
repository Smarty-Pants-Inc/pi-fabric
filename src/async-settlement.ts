const abortError = (signal: AbortSignal): Error => {
  const reason = signal.reason;
  if (reason instanceof Error) return reason;
  return new Error(typeof reason === "string" && reason ? reason : "Operation aborted");
};

// Identity, not text/name/prototype: guest exceptions can reproduce every public field.
const mainCeilingReasons = new WeakSet<Error>();

export const createMainExecutionCeilingError = (timeoutMs: number): Error => {
  const reason = new Error(`MainExecutionCeilingError: Main ceiling hit after ${timeoutMs}ms ` +
    "(executor.mainMaxTimeoutMs). Spawned agents keep running detached and report results as completion messages; check agents.status/list.");
  mainCeilingReasons.add(reason);
  return reason;
};

export const isMainExecutionCeilingError = (reason: unknown): reason is Error =>
  reason instanceof Error && mainCeilingReasons.has(reason);

/** The fixed Main watchdog's host-only reason; ordinary cancellation stays unchanged. */
export const mainExecutionCeilingAbortReason = (signal: AbortSignal | undefined): Error | undefined => {
  const reason: unknown = signal?.reason;
  return signal?.aborted && isMainExecutionCeilingError(reason) ? reason : undefined;
};

/** Preserve launch cancellation except for the genuine Main observation ceiling. */
export const withoutMainExecutionCeiling = (signal: AbortSignal | undefined): AbortSignal | undefined => {
  if (!signal) return undefined;
  const controller = new AbortController();
  const forward = (): void => {
    if (!mainExecutionCeilingAbortReason(signal)) controller.abort(signal.reason);
  };
  if (signal.aborted) forward();
  else signal.addEventListener("abort", forward, { once: true });
  return controller.signal;
};

export const throwIfAborted = (signal: AbortSignal | undefined): void => {
  if (signal?.aborted) throw abortError(signal);
};

/** Timers cannot observe synchronous preparation/serialization overruns. */
export const throwIfExecutionExpired = (context: {
  signal: AbortSignal | undefined;
  checkExecutionBudget?: () => void;
}): void => {
  // A shorter timeout or Escape remains the first cause even if cleanup later
  // crosses Main's wall deadline. Only live work can spend the Main budget.
  throwIfAborted(context.signal);
  context.checkExecutionBudget?.();
  throwIfAborted(context.signal);
};

const raceWithAbort = <T>(
  operation: PromiseLike<T>,
  signal: AbortSignal | undefined,
): Promise<T> => {
  if (!signal) return Promise.resolve(operation);
  if (signal.aborted) return Promise.reject(abortError(signal));

  return new Promise<T>((resolve, reject) => {
    let settled = false;
    const finish = (callback: () => void): void => {
      if (settled) return;
      settled = true;
      signal.removeEventListener("abort", onAbort);
      callback();
    };
    const onAbort = (): void => finish(() => reject(abortError(signal)));
    signal.addEventListener("abort", onAbort, { once: true });
    Promise.resolve(operation).then(
      (value) => finish(() => resolve(value)),
      (error) => finish(() => reject(error)),
    );
  });
};

export const runAbortable = <T>(
  signal: AbortSignal | undefined,
  operation: () => T | PromiseLike<T>,
): Promise<T> => {
  try {
    throwIfAborted(signal);
    return raceWithAbort(Promise.resolve(operation()), signal);
  } catch (error) {
    return Promise.reject(error);
  }
};

export const settleWithin = async (
  operations: Iterable<PromiseLike<unknown>>,
  timeoutMs: number,
): Promise<boolean> => {
  const pending = [...operations].map((operation) => Promise.resolve(operation));
  if (pending.length === 0) return true;

  let timer: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      Promise.allSettled(pending).then(() => true),
      new Promise<false>((resolve) => {
        timer = setTimeout(() => resolve(false), Math.max(0, timeoutMs));
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
};
