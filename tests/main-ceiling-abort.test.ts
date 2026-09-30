import { describe, expect, it } from "vitest";
import { createMainExecutionCeilingError, mainExecutionCeilingAbortReason, withoutMainExecutionCeiling, throwIfExecutionExpired } from "../src/async-settlement.js";

const message = "MainExecutionCeilingError: Main ceiling hit after 700ms (executor.mainMaxTimeoutMs).";

describe("Main ceiling host abort reason", () => {
  it("preserves the original Main watchdog Error object", () => {
    const reason = createMainExecutionCeilingError(700);
    const controller = new AbortController();
    expect(mainExecutionCeilingAbortReason(controller.signal)).toBeUndefined();
    controller.abort(reason);
    expect(mainExecutionCeilingAbortReason(controller.signal)).toBe(reason);
  });

  it("preserves an ordinary first abort rather than promoting it after a budget overrun", () => {
    const controller = new AbortController();
    const escape = new Error("Escape");
    controller.abort(escape);
    let checkedBudget = false;
    try {
      throwIfExecutionExpired({ signal: controller.signal, checkExecutionBudget() {
        checkedBudget = true;
        throw createMainExecutionCeilingError(700);
      } });
      throw new Error("expected cancellation");
    } catch (reason) { expect(reason).toBe(escape); }
    expect(checkedBudget).toBe(false);
  });

  it("does not confer the brand by cloning or inheriting from a host reason", () => {
    const trusted = createMainExecutionCeilingError(700);
    for (const reason of [new Error(trusted.message), structuredClone(trusted), Object.create(trusted)]) {
      const controller = new AbortController(); controller.abort(reason);
      expect(mainExecutionCeilingAbortReason(controller.signal)).toBeUndefined();
    }
  });

  it.each([false, true])("filters only genuine Main ceiling cancellation, including pre-abort=%s", preAborted => {
    for (const reason of [createMainExecutionCeilingError(700), new Error(message), new Error("Escape"), new Error("Execution timed out")]) {
      const controller = new AbortController();
      if (preAborted) controller.abort(reason);
      const child = withoutMainExecutionCeiling(controller.signal)!;
      if (!preAborted) controller.abort(reason);
      expect(child.aborted).toBe(!mainExecutionCeilingAbortReason(controller.signal));
      if (child.aborted) expect(child.reason).toBe(reason);
    }
  });

  it.each([
    undefined,
    new Error(message),
    Object.assign(new Error(message), { name: "MainExecutionCeilingError" }),
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
