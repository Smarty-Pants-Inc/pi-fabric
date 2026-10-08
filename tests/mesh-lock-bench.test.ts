import { describe, expect, it } from "vitest";
import {
  DEFAULT_LOAD, L8_BOUNDS_MS, compareToBaseline, histogramPercentile, participantsOf, planSchedule, processOf,
  samplePercentile,
} from "../scripts/lib/mesh-lock-bench.mjs";
import { LOCK_STATS_BOUNDS_MS } from "../src/mesh/commit-stats.js";

const result = (loadInsensitive: Record<string, number>, timing: Record<string, number> = {}) => ({
  load: { ...DEFAULT_LOAD },
  loadInsensitive: { acquisitionsPerOp: 1, holdsPerOp: 1, stateRewritesPerOp: 0.2, bytesRewrittenPerOp: 1000, timeoutsPer10kOps: 0, ...loadInsensitive },
  timing: { lockWaitP95Ms: 10, lockHoldP95Ms: 5, receiptP95Ms: 200, lockBusyPct: 3, ...timing },
});

describe("mesh lock benchmark (smarty-dev#6477 L9a)", () => {
  it("plans the same fixed load from the same seed", () => {
    const a = planSchedule({ ...DEFAULT_LOAD });
    const b = planSchedule({ ...DEFAULT_LOAD });
    expect(b).toEqual(a);
    expect(planSchedule({ ...DEFAULT_LOAD, seed: 1 })).not.toEqual(a);
    const measured = a.filter(op => op.phase === "measure");
    // 80 participants every 5 s for 120 s, plus ~7.45 op/s of mix and a few host renewals.
    expect(measured.filter(op => op.kind === "heartbeat").length).toBe(80 * 24);
    expect(measured.length).toBeGreaterThan(2_500);
    expect(measured.length).toBeLessThan(3_100);
    for (const op of a) {
      expect(op.t).toBeGreaterThanOrEqual(0);
      expect(op.t).toBeLessThan((DEFAULT_LOAD.warmupS + DEFAULT_LOAD.durationS) * 1000);
      if (op.kind === "steer") expect(op.to).not.toBe(op.p);
    }
    expect(new Set(a.map(op => processOf(DEFAULT_LOAD, op.p)))).toEqual(new Set([0, 1, 2, 3, 4, 5, 6, 7]));
  });

  it("partitions participants so every worker process gets at least one, consistently", () => {
    const pairs: Array<[number, number]> = [[5, 4], [7, 3], [80, 8], [2, 2], [9, 1]];
    for (const [participants, processes] of pairs) {
      const load = { ...DEFAULT_LOAD, participants, processes };
      const seen: number[] = [];
      for (let index = 0; index < processes; index++) {
        const mine = participantsOf(load, index);
        expect(mine.length, `${participants}/${processes} worker ${index}`).toBeGreaterThanOrEqual(1);
        // Balanced: sizes differ by at most one.
        expect(mine.length).toBeGreaterThanOrEqual(Math.floor(participants / processes));
        expect(mine.length).toBeLessThanOrEqual(Math.ceil(participants / processes));
        for (const p of mine) expect(processOf(load, p)).toBe(index);
        seen.push(...mine);
      }
      expect(seen).toEqual(Array.from({ length: participants }, (_, p) => p));
    }
    expect(participantsOf({ ...DEFAULT_LOAD, participants: 5, processes: 4 }, 0)).toEqual([0, 1]);
    expect(participantsOf({ ...DEFAULT_LOAD, participants: 7, processes: 3 }, 2)).toEqual([5, 6]);
    // The default 80/8 partition is unchanged (contiguous blocks of 10), so the committed baseline stays valid.
    for (let p = 0; p < DEFAULT_LOAD.participants; p++) expect(processOf(DEFAULT_LOAD, p)).toBe(Math.floor(p / 10));
    expect(() => participantsOf({ ...DEFAULT_LOAD, participants: 3, processes: 4 }, 0)).toThrow(/at least one/);
  });

  it("mirrors L8's histogram bounds and percentile", () => {
    expect([...L8_BOUNDS_MS]).toEqual([...LOCK_STATS_BOUNDS_MS]);
    const histogram = new Array(L8_BOUNDS_MS.length + 1).fill(0);
    histogram[0] = 94;
    histogram[5] = 6;
    expect(histogramPercentile(histogram, 0.95)).toBe(50);
    expect(samplePercentile([5, 1, 4, 2, 3], 0.95)).toBe(5);
    expect(samplePercentile([], 0.95)).toBe(0);
  });

  it("fails the ratchet on a >10% load-insensitive regression only", () => {
    const baseline = result({});
    expect(compareToBaseline(result({ acquisitionsPerOp: 1.09 }), baseline).ok).toBe(true);
    const regressed = compareToBaseline(result({ bytesRewrittenPerOp: 1200 }), baseline);
    expect(regressed.ok).toBe(false);
    expect(regressed.problems[0]).toMatch(/^bytesRewrittenPerOp/);
    // A zero baseline allows zero: one timeout at a generous budget is a regression.
    expect(compareToBaseline(result({ timeoutsPer10kOps: 3.6 }), baseline).ok).toBe(false);
    // Timing is reported, and gated only on request (a dedicated runner).
    const slow = result({}, { lockHoldP95Ms: 50 });
    expect(compareToBaseline(slow, baseline).ok).toBe(true);
    expect(compareToBaseline(slow, baseline, { gateTiming: true }).ok).toBe(false);
  });

  it("refuses to compare a different load", () => {
    const other = { ...result({}), load: { ...DEFAULT_LOAD, putRate: 2 } };
    const comparison = compareToBaseline(other, result({}));
    expect(comparison.ok).toBe(false);
    expect(comparison.problems[0]).toMatch(/load differs/);
  });
});
