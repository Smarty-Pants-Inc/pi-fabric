/** A host-owned receipt for a result observation, never serialized to the guest. */
export class ResultConsumption {
  #pending: Array<{ consume: () => void; abandon: (() => void) | undefined }> = [];
  #settled = false;

  readonly defer = (consume: () => void, abandon?: () => void): void => {
    if (this.#settled) { this.#call(abandon); return; }
    this.#pending.push({ consume, abandon });
  };

  /** A registry admits to the runtime; the runtime commits at its delivery boundary. */
  commit(defer?: (consume: () => void, abandon?: () => void) => void): void {
    if (this.#settled) return;
    this.#settled = true;
    const pending = this.#pending;
    this.#pending = [];
    for (const receipt of pending) {
      if (defer) defer(receipt.consume, receipt.abandon);
      else this.#call(receipt.consume);
    }
  }

  abandon(): void {
    if (this.#settled) return;
    this.#settled = true;
    const pending = this.#pending;
    this.#pending = [];
    for (const receipt of pending) this.#call(receipt.abandon);
  }

  #call(callback: (() => void) | undefined): void {
    try { callback?.(); } catch {
      // A failed durable receipt remains unread and can retry on the next poll.
      // Receipt bookkeeping must not mask the original publication outcome.
    }
  }
}
