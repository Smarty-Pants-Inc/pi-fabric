export const GUARD_BUDGET_REASON =
  "Blocked: shell command exceeds the guard's structural analysis budget; " +
  "split it into smaller commands so process-name and shared-tmp safety can be checked.";

/** Structural units cover visited characters, copied strings, and collection entries, not time. */
const MAX_WORK = 2_000_000;
const MAX_READER_DEPTH = 32;

/** Catch only at the public guard boundary; exhaustion must refuse both destructive verdicts. */
export class GuardBudgetExceeded extends Error {
  constructor() {
    super("Shell guard structural budget exhausted");
    this.name = "GuardBudgetExceeded";
  }
}

/** One instance per tool-call scan, shared by readers, collection, replay, and nested scripts. */
export class GuardBudget {
  private remaining: number;
  private readers = 0;
  private exhausted = false;

  constructor(units = MAX_WORK) {
    if (!Number.isSafeInteger(units) || units < 0) throw new GuardBudgetExceeded();
    this.remaining = units;
  }

  /** Reserve BEFORE the walk, copy, push, join, or expansion this work permits. */
  spend(units = 1): void {
    if (this.exhausted || !Number.isSafeInteger(units) || units < 0 || units > this.remaining) {
      // Exhaustion is sticky even if an internal caller accidentally catches it.
      this.exhausted = true;
      this.remaining = 0;
      throw new GuardBudgetExceeded();
    }
    this.remaining -= units;
  }

  /** Quote/balanced-reader recursion is separate from the scanner's existing script depth cap. */
  enterReader(): void {
    this.spend();
    if (this.readers >= MAX_READER_DEPTH) {
      this.exhausted = true;
      this.remaining = 0;
      throw new GuardBudgetExceeded();
    }
    this.readers += 1;
  }

  /** Pair each successful enterReader with a finally block. Never refunds cumulative work. */
  leaveReader(): void {
    this.readers -= 1;
  }

  /**
   * Preflight reference expansion before rendering any replacement or expanded output.
   * `resolve` must return an EXISTING string (e.g. a map value), without constructing it.
   * `render` may apply a length-preserving quote mask AFTER the complete reservation.
   * The expression must be global and cannot match an empty string. Its lastIndex is untouched.
   */
  replace(text: string, expression: RegExp, resolve: (match: RegExpExecArray) => string,
    render: (value: string, match: RegExpExecArray) => string = (value) => value): string {
    this.spend(2 * text.length + 1);
    if (!expression.global || expression.sticky) throw new GuardBudgetExceeded();
    const regex = new RegExp(expression.source, expression.flags);
    let length = text.length;
    let count = 0;
    for (let match = regex.exec(text); match; match = regex.exec(text)) {
      this.spend(match[0].length + 1);
      if (match[0].length === 0) throw new GuardBudgetExceeded();
      length += resolve(match).length - match[0].length;
      count += 1;
    }
    // Rendered pieces, untouched slices, final join, and their collection entries.
    this.spend(3 * length + 2 * count + 1);
    if (count === 0) return text;
    const parts: string[] = [];
    let cursor = 0;
    regex.lastIndex = 0;
    for (let match = regex.exec(text); match; match = regex.exec(text)) {
      const value = resolve(match);
      const replacement = render(value, match);
      if (replacement.length !== value.length) throw new GuardBudgetExceeded();
      parts.push(text.slice(cursor, match.index), replacement);
      cursor = match.index + match[0].length;
    }
    parts.push(text.slice(cursor));
    return parts.join("");
  }
}
