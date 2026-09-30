import { describe, expect, it, vi } from "vitest";
import { ResultConsumption } from "../src/result-consumption.js";

describe("host result consumption receipts", () => {
  it("transfers admission to runtime delivery without eager consumption", () => {
    const registry = new ResultConsumption(); const runtime = new ResultConsumption();
    const consume = vi.fn(); const abandon = vi.fn();
    registry.defer(consume, abandon); registry.commit(runtime.defer); registry.abandon();
    expect(consume).not.toHaveBeenCalled(); expect(abandon).not.toHaveBeenCalled();
    runtime.commit(); runtime.commit(); runtime.abandon();
    expect(consume).toHaveBeenCalledOnce(); expect(abandon).not.toHaveBeenCalled();
  });
  it("abandons undelivered and late receipts once without masking rejection", () => {
    const receipts = new ResultConsumption(); const consume = vi.fn(); const abandon = vi.fn();
    receipts.defer(consume, abandon); receipts.abandon(); receipts.abandon(); receipts.commit();
    expect(abandon).toHaveBeenCalledOnce(); expect(consume).not.toHaveBeenCalled();
    receipts.defer(consume, abandon); expect(abandon).toHaveBeenCalledTimes(2);
    const failed = new ResultConsumption(); failed.defer(() => { throw new Error("receipt storage unavailable"); });
    expect(() => failed.commit()).not.toThrow();
  });
});