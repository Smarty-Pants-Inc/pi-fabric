import { describe, expect, it, vi } from "vitest";
import { FabricToolDisplayController } from "../src/ui/tool-display.js";

// refresh() drains card invalidations in small batches across event-loop turns
// (one setImmediate per batch), so tests advance the loop explicitly.
const flushDrainTurns = async (turns: number): Promise<void> => {
  for (let index = 0; index < turns; index++) {
    await new Promise<void>((resolve) => setImmediate(resolve));
  }
};

describe("FabricToolDisplayController.refresh", () => {
  it("keeps callbacks on the host-owned state, using the newest call/result pair", async () => {
    const controller = new FabricToolDisplayController();
    const owner = {};
    const oldCall = vi.fn(), oldResult = vi.fn(), currentResult = vi.fn();
    controller.observe("card", "call", oldCall, owner);
    controller.observe("card", "result", oldResult, owner);
    controller.refresh();
    controller.observe("card", "result", currentResult, owner);
    await flushDrainTurns(2);
    expect(currentResult).toHaveBeenCalledOnce();
    expect(oldCall).not.toHaveBeenCalled();
    expect(oldResult).not.toHaveBeenCalled();
  });

  it("does not combine callbacks from rebuilt cards sharing a call id", async () => {
    const controller = new FabricToolDisplayController();
    const oldOwner = {}, newOwner = {};
    const oldResult = vi.fn(), newCall = vi.fn();
    controller.observe("reused", "result", oldResult, oldOwner);
    controller.refresh(); // queued old work must also be discarded on replacement
    controller.observe("reused", "call", newCall, newOwner);
    controller.refresh();
    await flushDrainTurns(2);
    expect(newCall).toHaveBeenCalledOnce();
    expect(oldResult).not.toHaveBeenCalled();
  });

  it("invalidates each card once even when call and result kinds are registered", async () => {
    const controller = new FabricToolDisplayController();
    const call = vi.fn();
    const result = vi.fn();
    controller.observe("card-a", "call", call);
    controller.observe("card-a", "result", result);

    controller.refresh();
    await flushDrainTurns(2);

    // Both kinds resolve to the same host component whose invalidate()
    // re-renders the whole card, so one call per card is the full refresh.
    expect(result).toHaveBeenCalledOnce();
    expect(call).not.toHaveBeenCalled();
  });

  it("falls back to the call invalidator for cards without a result yet", async () => {
    const controller = new FabricToolDisplayController();
    const call = vi.fn();
    controller.observe("streaming-card", "call", call);

    controller.refresh();
    await flushDrainTurns(2);

    expect(call).toHaveBeenCalledOnce();
  });

  it("drains at most a few cards per event-loop turn so one save cannot block the UI", async () => {
    const controller = new FabricToolDisplayController();
    const counts = new Map<string, number>();
    for (let index = 0; index < 8; index++) {
      const id = `card-${index}`;
      controller.observe(id, "call", vi.fn(() => {
        counts.set(id, (counts.get(id) ?? 0) + 1);
      }));
    }

    controller.refresh();
    await flushDrainTurns(1);
    const afterFirstTurn = [...counts.values()].reduce((sum, count) => sum + count, 0);
    expect(afterFirstTurn).toBe(3);

    await flushDrainTurns(2);
    const afterAllTurns = [...counts.values()].reduce((sum, count) => sum + count, 0);
    expect(afterAllTurns).toBe(8);
  });

  it("swallows disposed-card invalidators and keeps draining the rest", async () => {
    const controller = new FabricToolDisplayController();
    const disposed = vi.fn(() => {
      throw new Error("component disposed");
    });
    const alive = vi.fn();
    controller.observe("disposed-card", "call", disposed);
    controller.observe("alive-card", "call", alive);

    controller.refresh();
    await flushDrainTurns(2);

    expect(disposed).toHaveBeenCalledOnce();
    expect(alive).toHaveBeenCalledOnce();
  });

  it("drops pending refresh work on clear so rebuilt transcripts are not touched", async () => {
    const controller = new FabricToolDisplayController();
    const invalidate = vi.fn();
    controller.observe("card-a", "call", invalidate);

    controller.refresh();
    controller.clear();
    await flushDrainTurns(2);

    expect(invalidate).not.toHaveBeenCalled();

    // Cards re-observed after the clear refresh normally.
    controller.observe("card-b", "call", invalidate);
    controller.refresh();
    await flushDrainTurns(2);
    expect(invalidate).toHaveBeenCalledOnce();
  });
});
