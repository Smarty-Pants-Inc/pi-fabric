/**
 * Degenerate-loop breaker for fabric_exec. Identical guest code back-to-back
 * is how lower-tier models burn a session (2026-09-15: 220x `bash echo noop`
 * on glm-5.3-flash). Display names and payload cosmetics are deliberately
 * excluded from the fingerprint: cosmetic variation is part of the loop
 * signature, not evidence of new work. Any differing code resets the count.
 */
export const FABRIC_REPEAT_WARN = 3;
export const FABRIC_REPEAT_BLOCK = 6;

export const fabricRepeatWarnText = (count: number, blockAt: number): string =>
  `[circuit-breaker] identical fabric_exec code has now run ${count} times in a row; at ${blockAt} identical runs will be blocked. Change the approach or finish your turn.`;

export const fabricRepeatBlockText = (count: number): string =>
  `Circuit breaker: this exact fabric_exec code has now executed ${count} times in a row and was blocked. Re-running identical code adds no information. Summarize what you already know and finish your turn; if the call is genuinely required, change the code materially.`;

export class FabricRepeatGuard {
  #code: string | undefined;
  #count = 0;

  constructor(
    /** Consecutive identical executions that start producing a warning. */
    readonly warnAt: number,
    /** Consecutive identical executions that get blocked outright. */
    readonly blockAt: number,
  ) {}

  observe(code: string): { count: number; blocked: boolean; warn: boolean } {
    this.#count = code === this.#code ? this.#count + 1 : 1;
    this.#code = code;
    return { count: this.#count, blocked: this.#count >= this.blockAt, warn: this.#count >= this.warnAt };
  }
}
