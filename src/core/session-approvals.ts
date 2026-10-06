import type { FabricRisk } from "../protocol.js";

/** Shared approval state; optional classifier/dialog implementation loads on first use. */
export class FabricSessionApprovals {
  readonly approvedRisks = new Set<FabricRisk>();
  #tail: Promise<void> = Promise.resolve();

  async serialize<T>(request: () => Promise<T>): Promise<T> {
    const previous = this.#tail;
    let release: (() => void) | undefined;
    this.#tail = new Promise<void>((resolve) => {
      release = resolve;
    });
    await previous;
    try {
      return await request();
    } finally {
      release?.();
    }
  }
}
