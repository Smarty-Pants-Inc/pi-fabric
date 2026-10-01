import { describe, expect, it, vi } from "vitest";
import { createMainExecutionCeilingError, mainExecutionCeilingAbortReason, registerCancellationEffect } from "../src/async-settlement.js";
import { ExecutionDeadline } from "../src/runtime/execution-deadline.js";
import { QuickJsRuntime } from "../src/runtime/quickjs-runtime.js";
import { NodeProcessRuntime } from "../src/runtime/node-process-runtime.js";
import { MontyRuntime } from "../src/runtime/monty-runtime.js";
import { CPythonRuntime } from "../src/runtime/cpython-runtime.js";
import { captureRuntimeDeadline } from "./helpers/early-runtime-deadline.js";
import { executeAfterAdmission } from "./helpers/admission-clock.js";

const runtimes = { quickjs: QuickJsRuntime, "node-process": NodeProcessRuntime, monty: MontyRuntime, cpython: CPythonRuntime } as const;

describe("shared absolute deadline", () => {
  it("records clamp cause and never slides an expired budget", () => {
    const clock = vi.spyOn(Date, "now").mockReturnValue(1_000);
    const reason = createMainExecutionCeilingError(500);
    try {
      const deadline = new ExecutionDeadline({ timeoutMs: 100, maximumDeadlineAt: 1_500, maximumDeadlineReason: reason });
      expect(deadline.at).toBe(1_100);
      expect(deadline.extend(900_000)).toBe(true);
      expect(deadline.at).toBe(1_500);
      clock.mockReturnValue(1_499);
      expect(deadline.reached).toBe(false);
      clock.mockReturnValue(1_500);
      expect(deadline.reason).toBe(reason);
      expect(deadline.extend(900_000)).toBe(false);
      // Once expired, a backwards wall-clock adjustment cannot change its cause.
      clock.mockReturnValue(1_000);
      expect(deadline.reached).toBe(true);
      expect(deadline.timeoutResult([]).deadlineReason).toBe(reason);
    } finally { clock.mockRestore(); }
  });

  it("a shorter deadline keeps its own cause even when observed after the maximum", () => {
    const clock = vi.spyOn(Date, "now").mockReturnValue(1_000);
    try {
      const deadline = new ExecutionDeadline({ timeoutMs: 100, maximumDeadlineAt: 1_500, maximumDeadlineReason: createMainExecutionCeilingError(500) });
      clock.mockReturnValue(2_000);
      expect(deadline.timeoutResult([])).toMatchObject({ terminationReason: "timed_out", error: "Execution timed out after 100ms" });
      expect(deadline.timeoutResult([]).deadlineReason).toBeUndefined();
    } finally { clock.mockRestore(); }
  });
});

for (const [backend, Runtime] of Object.entries(runtimes)) {
  describe(`runtime-owned Main clamp (${backend})`, () => {
    it("rearms at deadline-1ms and aborts with the exact host brand at the deadline, without an outer watchdog", async () => {
      const reason = createMainExecutionCeilingError(2_000);
      const maximumDeadlineAt = Date.now() + 2_000;
      const timer = captureRuntimeDeadline(backend);
      let signal: AbortSignal | undefined;
      let release: (() => void) | undefined;
      const result = executeAfterAdmission(safety => new Runtime().execute(backend === "monty" || backend === "cpython"
        ? 'return await tools.call(ref="demo.hold", args={})' : 'return tools.call({ ref: "demo.hold", args: {} });',
        async (_ref, _args, hostSignal) => {
          signal = hostSignal;
          return new Promise<void>(resolve => { release = resolve; });
        }, { timeoutMs: 5_000, signal: safety, maximumDeadlineAt, maximumDeadlineReason: reason, memoryLimitBytes: 128 * 1024 * 1024,
          minimumTimeoutMsForHostCall: () => 900_000 }), () => Boolean(signal) && timer.ready(), () => {
        timer.fireEarly(maximumDeadlineAt);
        expect(signal!.aborted).toBe(false);
        timer.fireAt(maximumDeadlineAt);
        expect(mainExecutionCeilingAbortReason(signal)).toBe(reason);
        release!();
      });
      try {
        expect(await result).toMatchObject({ terminationReason: "timed_out", value: undefined, deadlineReason: reason });
        expect((await result).deadlineReason).toBe(reason);
      } finally { timer.restore(); release?.(); await result; }
    });

    it("retains committed resident receipts alongside the exact Main deadline cause", async () => {
      const reason = createMainExecutionCeilingError(2_000);
      const maximumDeadlineAt = Date.now() + 2_000;
      const timer = captureRuntimeDeadline(backend);
      const receipt = { requestId: "merged-request", state: "committed", operation: "createActor",
        entityKind: "actor", id: "merged-actor", ownerHostId: "resident-host" } as const;
      let signal: AbortSignal | undefined;
      let release: (() => void) | undefined;
      const result = executeAfterAdmission(safety => new Runtime().execute(backend === "monty" || backend === "cpython"
        ? 'return await tools.call(ref="demo.hold", args={})' : 'return tools.call({ ref: "demo.hold", args: {} });',
        async (_ref, _args, hostSignal) => {
          signal = hostSignal;
          registerCancellationEffect(hostSignal, () => Object.assign(
            new Error("ResidentOutcomeUnknownError: merged-request committed merged-actor. Do not retry or reassign."),
            { residentOutcome: receipt },
          ));
          return new Promise<void>(resolve => { release = resolve; });
        }, { timeoutMs: 5_000, signal: safety, maximumDeadlineAt, maximumDeadlineReason: reason,
          memoryLimitBytes: 128 * 1024 * 1024, minimumTimeoutMsForHostCall: () => 900_000 }),
        () => Boolean(signal) && timer.ready(), () => {
          timer.fireEarly(maximumDeadlineAt);
          expect(signal!.aborted).toBe(false);
          timer.fireAt(maximumDeadlineAt);
          expect(mainExecutionCeilingAbortReason(signal)).toBe(reason);
          release!();
        });
      try {
        const outcome = await result;
        expect(outcome).toMatchObject({ terminationReason: "timed_out", value: undefined,
          residentOutcomes: [receipt] });
        expect(outcome.deadlineReason).toBe(reason);
        expect(outcome.error).toContain("Do not retry or reassign");
      } finally { timer.restore(); release?.(); await result; }
    });

    it("extends the supplied host clamp record rather than a private runtime copy", async () => {
      const reason = createMainExecutionCeilingError(5_000);
      const maximumDeadlineAt = Date.now() + 5_000;
      const executionDeadline = new ExecutionDeadline({ timeoutMs: 2_000, maximumDeadlineAt, maximumDeadlineReason: reason });
      const timer = captureRuntimeDeadline(backend);
      let signal: AbortSignal | undefined;
      let release: (() => void) | undefined;
      const result = executeAfterAdmission(safety => new Runtime().execute(backend === "monty" || backend === "cpython"
        ? 'return await tools.call(ref="demo.hold", args={})' : 'return tools.call({ ref: "demo.hold", args: {} });',
        async (_ref, _args, hostSignal) => { signal = hostSignal; return new Promise<void>(resolve => { release = resolve; }); },
        { timeoutMs: 2_000, signal: safety, executionDeadline, maximumDeadlineAt, maximumDeadlineReason: reason,
          memoryLimitBytes: 128 * 1024 * 1024, minimumTimeoutMsForHostCall: () => 900_000 }), () => Boolean(signal) && timer.ready(), () => {
        expect(executionDeadline.at).toBe(maximumDeadlineAt);
        timer.fireEarly(maximumDeadlineAt);
        expect(signal!.aborted).toBe(false);
        timer.fireAt(maximumDeadlineAt);
        expect(executionDeadline.reason).toBe(reason);
        expect(mainExecutionCeilingAbortReason(signal)).toBe(reason);
        release!();
      });
      try {
        expect((await result).deadlineReason).toBe(reason);
      } finally { timer.restore(); release?.(); await result; }
    });

    it("does not brand a shorter timeout, even if its timer is observed beyond Main's deadline", async () => {
      const maximumDeadlineAt = Date.now() + 5_000;
      const timer = captureRuntimeDeadline(backend);
      let signal: AbortSignal | undefined;
      let release: (() => void) | undefined;
      const result = executeAfterAdmission(safety => new Runtime().execute(backend === "monty" || backend === "cpython"
        ? 'return await tools.call(ref="demo.hold", args={})' : 'return tools.call({ ref: "demo.hold", args: {} });',
        async (_ref, _args, hostSignal) => { signal = hostSignal; return new Promise<void>(resolve => { release = resolve; }); },
        { timeoutMs: 2_000, signal: safety, maximumDeadlineAt, maximumDeadlineReason: createMainExecutionCeilingError(5_000), memoryLimitBytes: 128 * 1024 * 1024 }), () => Boolean(signal) && timer.ready(), () => {
        timer.fireAt(maximumDeadlineAt + 1);
        expect(signal!.aborted).toBe(true);
        expect(mainExecutionCeilingAbortReason(signal)).toBeUndefined();
        release!();
      });
      try {
        expect(await result).toMatchObject({ terminationReason: "timed_out", value: undefined });
        expect((await result).deadlineReason).toBeUndefined();
        expect((await result).error).not.toContain("Main ceiling");
      } finally { timer.restore(); release?.(); await result; }
    });
  });
}
