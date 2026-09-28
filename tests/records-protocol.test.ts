import { describe, expect, it } from "vitest";
import { LineReader, MAX_LINE_BYTES, withinBudget } from "../src/records/protocol.js";

describe("records protocol framing (F10)", () => {
  const read = (chunks: string[]) => {
    const lines: string[] = [];
    let overflow = 0;
    const reader = new LineReader((line) => lines.push(line), () => { overflow++; });
    for (const chunk of chunks) reader.push(chunk);
    return { lines, overflow };
  };

  it("refuses an overlong line wherever the stream splits it, even inside one chunk", () => {
    const long = "x".repeat(MAX_LINE_BYTES + 10);
    expect(read([`${long}\n`])).toEqual({ lines: [], overflow: 1 });
    expect(read([long.slice(0, 1000), `${long.slice(1000)}\n`])).toMatchObject({ overflow: 1 });
    // Counterexample: a line at the limit, however split, is delivered.
    const atLimit = "y".repeat(MAX_LINE_BYTES);
    expect(read([`${atLimit}\n`]).lines).toEqual([atLimit]);
    expect(read([atLimit.slice(0, 7), atLimit.slice(7), "\n"]).lines).toEqual([atLimit]);
  });

  it("keeps a prefix within the budget, and always at least one item", () => {
    const big = { text: "z".repeat(100_000) };
    expect(withinBudget([big, big, big], 250_000)).toHaveLength(2);
    expect(withinBudget([big], 10)).toHaveLength(1);
    expect(withinBudget([], 10)).toEqual([]);
  });
});
