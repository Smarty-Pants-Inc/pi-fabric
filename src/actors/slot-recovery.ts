/** One owned startup/reconciliation barrier for every scope in an ActorDirectory.
 * No timer and no scheduler: native startup or an explicit retry owns the promise.
 * An unsuccessful scope keeps ALL protected scopes parked; ordinary actors do not
 * consult this barrier. Registration happens synchronously during construction.
 */
export class ActorSlotRecoveryParkedError extends AggregateError {
  readonly code = "FABRIC_ACTOR_SLOT_RECOVERY_PARKED";
  constructor(errors: readonly unknown[]) {
    super([...errors], `Protected actors remain parked: ${errors.map(error => error instanceof Error ? error.message : String(error)).join("; ")}`);
    this.name = "ActorSlotRecoveryParkedError";
  }
}

export class ActorSlotRecovery {
  readonly #stages = new Set<() => Promise<void>>();
  readonly #onReady = new Set<() => void>();
  #pending: Promise<void> | undefined;
  #ready = false;
  #error: ActorSlotRecoveryParkedError | undefined;

  get ready(): boolean { return this.#ready; }
  status(): { ready: boolean; error?: { code: "FABRIC_ACTOR_SLOT_RECOVERY_PARKED"; message: string } } {
    return { ready: this.#ready, ...(this.#error ? { error: { code: this.#error.code, message: this.#error.message } } : {}) };
  }

  register(stage: () => Promise<void>, onReady?: () => void): void {
    if (this.#pending || this.#ready) throw new Error("Actor slot recovery scope registered after startup");
    this.#stages.add(stage);
    if (onReady) this.#onReady.add(onReady);
  }

  recover(): Promise<void> {
    if (this.#pending) return this.#pending;
    this.#ready = false;
    this.#error = undefined;
    // Join every scope even on failure: no unowned materializer may escape an
    // early Promise.all rejection and cross a later retry's readiness boundary.
    const pending = Promise.allSettled([...this.#stages].map(stage => Promise.resolve().then(stage)))
      .then(results => {
        const failures = results.flatMap(result => result.status === "rejected" ? [result.reason] : []);
        if (failures.length) {
          // This typed boundary covers ONLY the registered protected stages.
          // Startup may catch this code and keep unrelated ordinary actors usable;
          // the shared readiness stays closed and the diagnostic remains observable.
          this.#error = new ActorSlotRecoveryParkedError(failures);
          throw this.#error;
        }
        this.#ready = true;
        for (const ready of this.#onReady) ready();
      }).catch(error => { this.#ready = false; throw error; })
      .finally(() => { if (this.#pending === pending) this.#pending = undefined; });
    return this.#pending = pending;
  }
}
