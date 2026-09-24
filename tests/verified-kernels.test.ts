import { describe, expect, it } from "vitest";
import * as kernel from "../src/verified/generated/kernel.js";
import {
  acceptCompactionCut, acceptMemoryChunk, acceptSampleAccounting, acceptSummaryBounds,
  assertCertificateFacts, bendList, boundedEffectResources,
} from "../src/verified/policy.js";
import { acceptsExpansionPage } from "../src/verified/memory.js";
import { effectConflictsBetween, registrationEffect, summarizeEffects } from "../src/components/effect-policy.js";
import { sampleAddressed } from "../src/compaction/bounds.js";

// These are ABI/refinement checks of the emitted implementation, not substitutes
// for the universal Bend proofs. In particular they exercise native JS numbers,
// constructor fields, BigInts, and the adapter's actual source observations.
describe("compiled Bend policy bridge", () => {
  // Laws prove the Boolean functions universally. These asymmetric vectors
  // distinguish ABI argument positions and output tags without truth-table grids.
  it("preserves policy argument positions and outcome encodings", () => {
    expect(kernel.transitionCurrent(false, true, true, false)).toBe(true);
    expect(kernel.transitionCurrent(true, true, true, false)).toBe(false);
    expect(kernel.transitionCurrent(false, false, true, false)).toBe(false);
    expect(kernel.transitionCurrent(false, true, false, false)).toBe(false);
    expect(kernel.transitionCurrent(false, true, true, true)).toBe(false);
    expect(kernel.canClose(true, false, false, false)).toBe(true);
    expect(kernel.canClose(false, false, false, false)).toBe(false);
    expect(kernel.canClose(true, true, false, false)).toBe(false);
    expect(kernel.canClose(true, false, true, false)).toBe(false);
    expect(kernel.canClose(true, false, false, true)).toBe(false);
    expect(kernel.cleanupState(false, false)).toBe(0);
    expect(kernel.cleanupState(false, true)).toBe(1);
    expect(kernel.cleanupState(true, false)).toBe(2);
    expect(kernel.cleanupState(true, true)).toBe(2);
    expect(kernel.pointerCurrent(true, true)).toBe(true);
    expect(kernel.pointerCurrent(false, true)).toBe(false);
    expect(kernel.pointerCurrent(true, false)).toBe(false);
    expect(kernel.coverageComplete(true, false)).toBe(true);
    expect(kernel.coverageComplete(true, true)).toBe(false);
    expect(kernel.coverageComplete(false, false)).toBe(false);
    expect(kernel.lineageSelected(false, true)).toBe(true);
    expect(kernel.lineageSelected(true, false)).toBe(true);
    expect(kernel.lineageSelected(false, false)).toBe(false);
    expect(kernel.useNormalized(false, true, true, true)).toBe(true);
    expect(kernel.useNormalized(true, true, true, true)).toBe(false);
    expect(kernel.useNormalized(false, false, true, true)).toBe(false);
    expect(kernel.useNormalized(false, true, false, true)).toBe(false);
    expect(kernel.useNormalized(false, true, true, false)).toBe(false);
    expect(kernel.headReadable(true, true, true, false, false)).toBe(true);
    expect(kernel.headReadable(true, true, false, true, true)).toBe(true);
    expect(kernel.headReadable(false, true, true, false, false)).toBe(false);
    expect(kernel.headReadable(true, false, true, false, false)).toBe(false);
    expect(kernel.headReadable(true, true, false, true, false)).toBe(false);
    expect(kernel.headReadable(true, true, false, false, true)).toBe(false);
    expect(kernel.knownConflict(true, true, false)).toBe(true);
    expect(kernel.knownConflict(true, false, true)).toBe(true);
    expect(kernel.knownConflict(false, true, true)).toBe(false);
    expect(kernel.knownConflict(true, false, false)).toBe(false);
  });

  it("distinguishes unknown-footprint ABI fields on both sides", () => {
    const cases = [
      [true, true, false, false, false, false, true],
      [true, false, false, false, false, true, true],
      [true, false, true, false, false, false, false],
      [false, true, true, false, true, true, false],
      [true, false, false, true, false, false, false],
      [false, false, true, true, false, false, true],
      [false, false, false, true, true, false, true],
    ] as const;
    for (const [lu, luo, lo, ru, ruo, ro, expected] of cases) {
      expect(kernel.unknownConflict(lu, luo, lo, ru, ruo, ro)).toBe(expected);
      expect(kernel.unknownConflict(ru, ruo, ro, lu, luo, lo)).toBe(expected);
    }
  });

  it("never loses a late conflicting resource or treats wildcard as a literal name", () => {
    const many = [...Array.from({ length: 64 }, (_, i) => `r${i}`), "shared"];
    expect(boundedEffectResources(many)).toEqual(["*"]);
    expect(boundedEffectResources(["x".repeat(257)])).toEqual(["*"]);
    expect(boundedEffectResources(["", "a"])).toEqual(["*"]);
    expect(boundedEffectResources(["*"])).toEqual(["*"]);
    expect(boundedEffectResources(["a", "a", "b"])).toEqual(["a", "b"]);
    expect(boundedEffectResources(undefined)).toEqual(["*"]);
    const summarize = (resources: string[]) => summarizeEffects([registrationEffect({ resources, ordering: "ordered" })]);
    expect(effectConflictsBetween(summarize(many), summarize(["shared"]))).toEqual([
      { resources: ["*"], reason: "unknown_resource" },
    ]);
  });

  it("checks numeric comparisons without U32 truncation and fails malformed Nat boundaries closed", () => {
    for (const count of [0, 1, 63, 64, 65, 2 ** 32, Number.MAX_SAFE_INTEGER]) {
      expect(kernel.footprintFits(BigInt(count), 64n, true)).toBe(count <= 64);
      expect(acceptSummaryBounds(count, count, count, count)).toBe(true);
      expect(acceptSummaryBounds(count + 1, count, 0, 0)).toBe(false);
    }
    for (const bad of [-1, 1.5, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1]) {
      expect(acceptSampleAccounting(bad, 0, 0, 0)).toBe(false);
      expect(acceptSummaryBounds(bad, 32_768, 0, 0)).toBe(false);
    }
  });

  it("refuses arithmetic outside the pinned backend domain without throwing", () => {
    const max = 2 ** 48 - 1;
    expect(acceptSampleAccounting(max, max, 0, max)).toBe(true);
    expect(acceptSampleAccounting(max + 1, max, 1, max + 1)).toBe(false);
    expect(acceptSampleAccounting(max, max, 1, max)).toBe(false);
    expect(acceptMemoryChunk({ start: max, end: max, total: max, complete: true }, 0, max, max)).toBe(true);
    expect(acceptMemoryChunk({ start: max, end: max + 1, total: max + 1, complete: true }, 1, max, max + 1)).toBe(false);
  });

  it("rejects a crossing pair, stale marker, ineligible cut, or oversize tail, including late batches", () => {
    const base = { eligible: true, afterPrevious: true, retained: 9, budget: 10, boundary: 5 };
    const safe = { first: 0, last: 4, hasCall: true, hasResult: true };
    const cross = { ...safe, last: 5 };
    expect(acceptCompactionCut(base, [safe])).toBe(true);
    expect(acceptCompactionCut(base, [cross])).toBe(false);
    expect(acceptCompactionCut(base, [{ ...cross, hasResult: false }])).toBe(true);
    expect(acceptCompactionCut({ ...base, eligible: false }, [])).toBe(false);
    expect(acceptCompactionCut({ ...base, afterPrevious: false }, [])).toBe(false);
    expect(acceptCompactionCut({ ...base, retained: 11 }, [])).toBe(false);
    expect(acceptCompactionCut(base, [...Array.from({ length: 10_000 }, () => safe), cross])).toBe(false);
    expect(acceptCompactionCut(base, [{ ...safe, first: -1 }])).toBe(false);
  });

  // This still tests a TypeScript producer: Bend checks its accounting, not
  // which source entries and omitted addresses the producer selects.
  it("conserves addressed samples over all small sizes and limits", () => {
    for (let total = 0; total <= 30; total++) for (let limit = 0; limit <= 12; limit++) {
      const source = Array.from({ length: total }, (_, i) => ({ entryId: `e${i}` }));
      const sample = sampleAddressed(source, limit);
      expect(acceptSampleAccounting(total, sample.values.length, sample.omitted, limit)).toBe(true);
      expect(sample.values.length + sample.omitted).toBe(total);
      if (sample.omitted > 0) {
        expect(source.some((entry) => entry.entryId === sample.omittedFirstEntryId)).toBe(true);
        expect(source.some((entry) => entry.entryId === sample.omittedLastEntryId)).toBe(true);
      }
    }
    expect(acceptSampleAccounting(10, 4, 5, 4)).toBe(false);
  });

  it("maps chunk offsets, lengths, completion and progress into the proved checker", () => {
    const partial = { start: 2, end: 5, total: 8, complete: false };
    expect(acceptMemoryChunk(partial, 3, 2, 8)).toBe(true);
    expect(acceptMemoryChunk({ ...partial, complete: true }, 3, 2, 8)).toBe(false);
    expect(acceptMemoryChunk(partial, 2, 2, 8)).toBe(false);
    expect(acceptMemoryChunk(partial, 3, 3, 8)).toBe(false);
    expect(acceptMemoryChunk(partial, 3, 2, 9)).toBe(false);
    expect(acceptMemoryChunk({ ...partial, end: 8, complete: true }, 6, 2, 8)).toBe(true);
    expect(acceptMemoryChunk({ ...partial, end: 8 }, 6, 2, 8)).toBe(false);
    expect(acceptMemoryChunk({ start: 0, end: 0, total: 0, complete: true }, 0, 0, 0)).toBe(true);
    expect(acceptMemoryChunk({ ...partial, end: 2 }, 0, 2, 8)).toBe(false);
    expect(acceptMemoryChunk({ ...partial, end: 1 }, 0, 2, 8)).toBe(false);
    expect(acceptMemoryChunk({ ...partial, end: 9 }, 7, 2, 8)).toBe(false);
  });

  it("checks post-trimming text against source and verifies the actual continuation", () => {
    const source = [{ index: 4, text: "a😀bc" }];
    const first = { index: 4, text: "a😀", textRange: { start: 0, end: 3, total: 5, complete: false } };
    expect(acceptsExpansionPage([first], source, 0, 0, { position: 0, textOffset: 3 })).toBe(true);
    const last = { index: 4, text: "bc", textRange: { start: 3, end: 5, total: 5, complete: true } };
    expect(acceptsExpansionPage([last], source, 0, 3, { position: 1, textOffset: 0 })).toBe(true);
    expect(acceptsExpansionPage([{ ...first, text: "bad" }], source, 0, 0, { position: 0, textOffset: 3 })).toBe(false);
    expect(acceptsExpansionPage([first], source, 0, 0, { position: 1, textOffset: 0 })).toBe(false);
    expect(acceptsExpansionPage([], source, 0, 0, { position: 0, textOffset: 0 })).toBe(false);
  });

  it("requires every certificate condition and consumes an active token only once", () => {
    // all_sound/all_complete cover arbitrary conjunctions. Keep each host
    // diagnostic position and one multiple-failure precedence case, not 2^8 rows.
    const facts = (row: readonly boolean[]) => {
      const fact = (i: number) => ({ valid: row[i]!, error: `condition ${i}` });
      return [fact(0), fact(1), fact(2), fact(3), fact(4), fact(5), fact(6), fact(7)] as const;
    };
    const valid = Array<boolean>(8).fill(true);
    expect(kernel.certificateAccepted(bendList(valid))).toBe(true);
    expect(() => assertCertificateFacts(facts(valid))).not.toThrow();
    for (let index = 0; index < valid.length; index++) {
      const row = [...valid]; row[index] = false;
      expect(kernel.certificateAccepted(bendList(row))).toBe(false);
      expect(() => assertCertificateFacts(facts(row))).toThrow(`condition ${index}`);
    }
    const multiple = [...valid]; multiple[2] = false; multiple[6] = false;
    expect(kernel.certificateAccepted(bendList(multiple))).toBe(false);
    expect(() => assertCertificateFacts(facts(multiple))).toThrow("condition 2");
    expect(() => assertCertificateFacts([] as unknown as Parameters<typeof assertCertificateFacts>[0])).toThrow("Invalid Schema");
    const first = kernel.consume(true);
    expect(first).toEqual({ $: "Tuple", fst: true, snd: false });
    expect(kernel.consume(first.snd)).toEqual({ $: "Tuple", fst: false, snd: false });
  });
});
