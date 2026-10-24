import { AsyncLocalStorage } from "node:async_hooks";

/** Reserve time for the guest worker to receive and handle a typed lock refusal. */
export const MESH_INVOCATION_DEADLINE_MARGIN_MS = 50;

export interface MeshInvocationContext {
  readonly signal?: AbortSignal;
  /** Live epoch deadline: host-call floors and human waits may move it. */
  readonly deadlineAt: () => number;
  /** Failed, cancelled, or expired: refuses the acquisitions the dispatch began while live, never later ones
   * (a closed store still reaches host timers armed during the call; pi-fabric#1269 review P1-1). */
  closed: boolean;
  /** Successful dispatches preserve ordinary detached handles and refresh timers. */
  detached: boolean;
  readonly expired: boolean;
}

export const meshInvocationStorage = new AsyncLocalStorage<MeshInvocationContext>();

/** Host-only: publication owed by an already committed actor, not a new guest write.
 * Exit only for this continuation; never detach the dispatch or its sibling writers.
 * Store shutdown signals and the ordinary lock budget still apply. */
export const withCommittedMeshPublication = <T>(operation: () => T): T => meshInvocationStorage.exit(operation);

export const withMeshInvocationContext = async <T>(
  context: Omit<MeshInvocationContext, "closed" | "detached" | "expired">,
  operation: () => T | Promise<T>,
  // Dispatchers can require canonical success even when a failed call resolves.
  confirmSuccess: () => boolean = () => true,
): Promise<T> => {
  // A successful detached handle must not keep the caller's eventual runtime
  // abort attached to mesh work that was already waiting when the handle returned.
  const lifetime = context.signal ? new AbortController() : undefined;
  const scope: MeshInvocationContext = {
    ...context,
    ...(lifetime ? { signal: lifetime.signal } : {}),
    closed: false,
    detached: false,
    get expired(): boolean {
      return this.closed || (!this.detached && (this.signal?.aborted === true || this.deadlineAt() <= Date.now()));
    },
  };
  const onAbort = (): void => { if (!scope.detached) lifetime?.abort(context.signal?.reason); };
  if (context.signal?.aborted) onAbort();
  else context.signal?.addEventListener("abort", onAbort, { once: true });
  return meshInvocationStorage.run(scope, async () => {
    try {
      const value = await operation();
      // Confirmed detached work outlives its caller; only unsuccessful dispatches
      // permanently close the inherited scope. A sibling never shares this object.
      if (scope.expired || !confirmSuccess()) scope.closed = true;
      else scope.detached = true;
      return value;
    } catch (error) {
      scope.closed = true;
      throw error;
    } finally {
      context.signal?.removeEventListener("abort", onAbort);
      if (!scope.detached) scope.closed = true;
    }
  });
};
