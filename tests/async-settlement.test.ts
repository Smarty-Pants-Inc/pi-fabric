import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { cancellationError, preserveCancellationOutcome, registerCancellationEffect, runAbortable, shareCancellationEffects } from "../src/async-settlement.js";
import { commitResidentRequest, registerResidentCancellation, ResidentOutcomeUnknownError, type ResidentCommand } from "../src/residency/protocol.js";

const roots: string[] = [];
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });

describe("cancellation effect settlement", () => {
  it("settles effects synchronously before an outer abort rejection", async () => {
    const controller = new AbortController(); const events: string[] = [];
    registerCancellationEffect(controller.signal, () => { events.push("fenced"); return undefined; });
    const outcome = runAbortable(controller.signal, () => new Promise(() => {}))
      .catch((error: Error) => { events.push("rejected"); return error; });
    const reason = new Error("owned abort"); controller.abort(reason);
    expect(await outcome).toBe(reason); expect(events).toEqual(["fenced", "rejected"]);
  });

  it("shares only an invocation lineage, never an unrelated provider/shutdown lineage", () => {
    const parent = new AbortController(); const shutdown = new AbortController();
    const invocation = shareCancellationEffects(AbortSignal.any([parent.signal, shutdown.signal]), parent.signal);
    const binding = shareCancellationEffects(AbortSignal.any([invocation, shutdown.signal]), invocation);
    const unrelated = shareCancellationEffects(AbortSignal.any([shutdown.signal]));
    const known = new Error("known durable identity"); registerCancellationEffect(binding, () => known);
    const reason = new Error("cancelled");
    expect(cancellationError(parent.signal, reason)).toBe(known);
    expect(cancellationError(invocation, reason)).toBe(known);
    expect(cancellationError(unrelated, reason)).toBe(reason);
    expect(cancellationError(shutdown.signal, reason)).toBe(reason);
  });

  it("aggregates every committed request/entity ID and retains flat immutable receipts through repeated gates", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-cancellation-effects-")); roots.push(root);
    const controller = new AbortController();
    for (const id of ["first", "second"]) {
      const command: ResidentCommand = { format: 1, operation: "spawn", requestId: `request-${id}`, rootId: "root", createdAt: 1, request: { task: id } };
      registerResidentCancellation(controller.signal, root, command);
      commitResidentRequest(root, command, `entity-${id}`, "resident:owner");
    }
    const original = new Error("original cancellation");
    const first = cancellationError(controller.signal, original) as AggregateError;
    expect(first).toBeInstanceOf(AggregateError);
    const repeated = cancellationError(controller.signal, new Error(first.message)) as AggregateError;
    expect(repeated.errors).toEqual(first.errors);
    expect(repeated.message).toBe(first.message);
    for (const id of ["first", "second"]) {
      expect(first.message).toContain(`request-${id}`); expect(first.message).toContain(`entity-${id}`);
    }
    for (const error of first.errors) {
      expect(error).toBeInstanceOf(ResidentOutcomeUnknownError); expect(error.cause).toBe(original);
    }
  });

  it("preserves ordinary success and safe cancellation without changing result identity", () => {
    const controller = new AbortController();
    const result = { value: "handle", terminationReason: "completed" as const, logs: ["retained"] };
    registerCancellationEffect(controller.signal, () => new Error("known receipt"));
    expect(preserveCancellationOutcome(result, controller.signal)).toBe(result);
    expect(result.value).toBe("handle");
    const safe = new AbortController(); safe.abort();
    const failure = { value: undefined, terminationReason: "aborted" as const, error: "cancelled" };
    expect(preserveCancellationOutcome(failure, safe.signal)).toBe(failure);
    expect(failure.error).toBe("cancelled");
  });

  it("replaces false success with all terminal receipts while retaining logs and failure kind", () => {
    const controller = new AbortController();
    registerCancellationEffect(controller.signal, () => new Error("request-one entity-one owner-one; do not reassign"));
    registerCancellationEffect(controller.signal, () => new Error("request-two entity-two owner-two; do not reassign"));
    controller.abort();
    const result: { value: unknown; terminationReason: "completed" | "runtime_error"; logs: string[]; error?: string } = {
      value: "false success", terminationReason: "completed", logs: ["retained"],
    };
    expect(preserveCancellationOutcome(result, controller.signal)).toBe(result);
    expect(result).toMatchObject({ value: undefined, terminationReason: "runtime_error", logs: ["retained"] });
    expect(result.error).toContain("request-one entity-one owner-one");
    expect(result.error).toContain("request-two entity-two owner-two");
    const deadline = { value: undefined, terminationReason: "timed_out" as const, error: "timeout" };
    preserveCancellationOutcome(deadline, controller.signal);
    expect(deadline.terminationReason).toBe("timed_out"); expect(deadline.error).toContain("request-two");
  });

  it("does not label failed effect settlement as a proven safe rejection", () => {
    const controller = new AbortController();
    registerCancellationEffect(controller.signal, () => { throw new Error("unreadable fence"); });
    const error = cancellationError(controller.signal, new Error("cancelled"));
    expect(error.message).toContain("outcome unknown; do not retry or reassign");
    expect(error.message).toContain("unreadable fence");
  });
});
