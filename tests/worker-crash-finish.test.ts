import { describe, expect, it, vi } from "vitest";
import { createCrashFinisher } from "../src/worker/crash-finish.js";

const harness = (cleanup: () => Promise<void>) => {
  const calls: string[] = [];
  const report = vi.fn((error: unknown) => { calls.push(`report:${error instanceof Error ? error.message : String(error)}`); });
  const exit = vi.fn((code: number) => { calls.push(`exit:${code}`); });
  const retain = vi.fn(() => { calls.push("retain"); });
  const log = vi.fn();
  const crash = createCrashFinisher({ cleanup, settled: async () => { calls.push("settled"); }, report, exit, log, retain });
  return { crash, calls, report, exit, retain, log };
};

describe("worker crash finisher", () => {
  it("publishes the crash only after cleanup and settlement, then exits", async () => {
    const h = harness(async () => { h.calls.push("cleanup"); });
    await h.crash.finish(new Error("stream broke"));
    expect(h.crash.pending).toBe(true);
    expect(h.calls).toEqual(["cleanup", "settled", "report:stream broke", "exit:1"]);
    expect(h.retain).not.toHaveBeenCalled();
  });

  it("still reports a terminal failure naming both causes when cleanup rejects", async () => {
    const h = harness(async () => { throw new Error("Execution group did not confirm exit after cleanup"); });
    await h.crash.finish(new Error("stream broke"));
    expect(h.report).toHaveBeenCalledOnce();
    expect(h.calls).toEqual(["report:stream broke; crash cleanup unresolved: Execution group did not confirm exit after cleanup", "retain"]);
    expect(h.exit).not.toHaveBeenCalled(); // execution custody is retained, not abandoned
    expect(h.log).toHaveBeenCalledWith(expect.stringContaining("Crash cleanup unresolved; retaining execution custody"));
  });

  it("retains custody even if the failure report itself throws, and handles one crash only", async () => {
    const h = harness(async () => { throw new Error("cleanup failed"); });
    h.report.mockImplementation(() => { throw new Error("status write failed"); });
    await expect(h.crash.finish("first")).rejects.toThrow("status write failed");
    expect(h.retain).toHaveBeenCalledOnce();
    await h.crash.finish("second");
    expect(h.report).toHaveBeenCalledOnce();
  });
});
