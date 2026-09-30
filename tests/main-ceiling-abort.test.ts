import { describe, expect, it } from "vitest";
import { mainExecutionCeilingAbortReason } from "../src/async-settlement.js";

const message = "MainExecutionCeilingError: Main ceiling hit after 700ms (executor.mainMaxTimeoutMs).";

describe("Main ceiling host abort reason", () => {
  it("preserves the original Main watchdog Error object", () => {
    const reason = new Error(message);
    const controller = new AbortController();
    expect(mainExecutionCeilingAbortReason(controller.signal)).toBeUndefined();
    controller.abort(reason);
    expect(mainExecutionCeilingAbortReason(controller.signal)).toBe(reason);
  });

  it.each([
    undefined,
    new Error("Escape"),
    new Error("Execution cancelled"),
    new Error("Execution timed out"),
    new Error("MainExecutionCeilingError: Main ceiling hit after 700ms"),
    message,
  ])("does not classify ordinary or untrusted cancellation as a Main ceiling: %s", reason => {
    const controller = new AbortController();
    controller.abort(reason);
    expect(mainExecutionCeilingAbortReason(controller.signal)).toBeUndefined();
    expect(mainExecutionCeilingAbortReason(undefined)).toBeUndefined();
  });
});
