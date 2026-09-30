import type { FabricResidentOutcomeReceipt, FabricSandboxResult } from "./runtime/kernel.js";

// Cancellation is not a safe rejection once a durable mutation may have committed.
// Effects are shared only along one invocation's signal lineage, never with a
// registry/provider shutdown signal (which is shared by unrelated invocations).
// Keep receipts until that signal is collected: a guest may receive a successful
// host result, then be interrupted before it can return the ID to its caller.
type CancellationEffect = (reason: Error) => Error | undefined;
const cancellationEffects = new WeakMap<AbortSignal, Set<CancellationEffect>>();
const effectsFor = (signal: AbortSignal): Set<CancellationEffect> => {
  let effects = cancellationEffects.get(signal);
  if (!effects) cancellationEffects.set(signal, effects = new Set());
  return effects;
};

export const shareCancellationEffects = (signal: AbortSignal, parent?: AbortSignal): AbortSignal => {
  if (parent) cancellationEffects.set(signal, effectsFor(parent));
  return signal;
};

/** The callback must synchronously fence cancellation, not wait for a remote host. */
export const registerCancellationEffect = (signal: AbortSignal | undefined, effect: CancellationEffect): void => {
  if (signal) effectsFor(signal).add(effect);
};

/** Settle every effect BEFORE presenting cancellation, even if the guest cannot resume. */
export const cancellationError = (signal: AbortSignal | undefined, reason: Error): Error => {
  const errors: Error[] = [];
  for (const effect of signal ? cancellationEffects.get(signal) ?? [] : []) {
    try {
      const error = effect(reason);
      if (error) errors.push(error);
    } catch (error) {
      // A failed settlement must never look like a proved, safe rejection.
      errors.push(new Error(`Cancellation outcome unknown; do not retry or reassign. ${String(error)}`, { cause: error }));
    }
  }
  if (errors.length === 0) return reason;
  if (errors.length === 1) return errors[0]!;
  return new AggregateError(errors, errors.map((error) => error.message).join("\n"), { cause: reason });
};

// These errors come from host-installed cancellation effects, not serialized
// guest errors. Aggregate settlement must retain the entire receipt list.
const residentOutcomeReceipts = (error: Error): FabricResidentOutcomeReceipt[] => {
  if (error instanceof AggregateError) {
    return error.errors.flatMap(cause => cause instanceof Error ? residentOutcomeReceipts(cause) : []);
  }
  const receipt = (error as Error & { residentOutcome?: FabricResidentOutcomeReceipt }).residentOutcome;
  return receipt ? [receipt] : [];
};

/** Apply the invocation's complete receipt ledger at any engine's outer boundary.
 * Mutate in place because QuickJS returns from try before its async finally runs.
 * Completed runs become failures when teardown found unawaited resident work,
 * even if its reply arrives during the grace window before cleanup aborts.
 * Ordinary successful replies keep their handles and are not uncertainty.
 */
export const preserveCancellationOutcome = <T extends Pick<FabricSandboxResult, "value" | "terminationReason" | "error" | "residentOutcomes">>(
  result: T,
  signal: AbortSignal,
  interrupted = signal.aborted,
): T => {
  if (!interrupted) return result;
  const reason = new Error(result.error ?? "Fabric guest ended before its host calls settled");
  const outcome = cancellationError(signal, reason);
  if (outcome !== reason) {
    result.value = undefined;
    result.error = outcome.message;
    const receipts = residentOutcomeReceipts(outcome);
    if (receipts.length > 0) result.residentOutcomes = [...new Map(receipts.map(receipt => [receipt.requestId, receipt])).values()];
    if (result.terminationReason === "completed") result.terminationReason = "runtime_error";
  }
  return result;
};

const abortError = (signal: AbortSignal): Error => {
  const reason = signal.reason;
  return cancellationError(signal, reason instanceof Error ? reason
    : new Error(typeof reason === "string" && reason ? reason : "Operation aborted"));
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
