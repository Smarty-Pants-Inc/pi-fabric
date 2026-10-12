type Invalidators = { call?: () => void; result?: () => void };
type CardReference = WeakRef<object>;

/** Tracks live fabric_exec cards without owning the host's rendered transcript. */
export class FabricToolDisplayController {
  // The host owns rendererState. A callback closes over ToolExecutionComponent,
  // which in turn owns that state, so the callback must be an ephemeron value,
  // not a strong value beside a WeakRef. The otherwise-unreachable cycle can
  // then be collected after compaction, transcript rebuild or reader disposal.
  readonly #cards = new Map<string, CardReference>();
  #invalidators = new WeakMap<object, Invalidators>();
  readonly #collected = new FinalizationRegistry<{ id: string; reference: CardReference }>(
    ({ id, reference }) => {
      // A rebuilt card may reuse its call id before the old finalizer runs.
      if (this.#cards.get(id) === reference) this.#cards.delete(id);
    },
  );
  #pendingRefresh: Array<{ id: string; reference: CardReference }> = [];
  #refreshDrainScheduled = false;

  observe(
    toolCallId: string,
    kind: "call" | "result",
    invalidate: () => void,
    owner: object = invalidate,
  ): void {
    const previous = this.#cards.get(toolCallId);
    if (previous?.deref() !== owner) {
      if (previous) this.#collected.unregister(previous);
      const reference = new WeakRef(owner);
      this.#cards.set(toolCallId, reference);
      this.#collected.register(owner, { id: toolCallId, reference }, reference);
    }
    const invalidators = this.#invalidators.get(owner) ?? {};
    invalidators[kind] = invalidate;
    this.#invalidators.set(owner, invalidators);
  }

  refresh(): void {
    for (const [id, reference] of this.#cards) {
      if (reference.deref()) this.#pendingRefresh.push({ id, reference });
      else {
        this.#cards.delete(id);
        this.#collected.unregister(reference);
      }
    }
    this.#scheduleRefreshDrain();
  }

  // Queue only weak owners, never callbacks: a pending settings refresh must
  // not extend a discarded card's lifetime. Resolve the newest invalidator at
  // drain time, in small batches so a save cannot block the interactive path.
  #scheduleRefreshDrain(): void {
    if (this.#refreshDrainScheduled) return;
    this.#refreshDrainScheduled = true;
    setImmediate(() => {
      this.#refreshDrainScheduled = false;
      const batch = this.#pendingRefresh.splice(0, REFRESH_CARDS_PER_TICK);
      for (const { id, reference } of batch) {
        if (this.#cards.get(id) !== reference) continue;
        const owner = reference.deref();
        const invalidators = owner && this.#invalidators.get(owner);
        // Call and result resolve to the same host component; one refresh
        // covers the complete card, including a streaming call without result.
        const invalidate = invalidators && (invalidators.result ?? invalidators.call);
        try { invalidate?.(); }
        catch { /* A transcript component may already have been disposed. */ }
      }
      if (this.#pendingRefresh.length > 0) this.#scheduleRefreshDrain();
    });
  }

  clear(): void {
    this.#pendingRefresh = [];
    for (const reference of this.#cards.values()) this.#collected.unregister(reference);
    this.#cards.clear();
    this.#invalidators = new WeakMap();
  }
}

const REFRESH_CARDS_PER_TICK = 3;
