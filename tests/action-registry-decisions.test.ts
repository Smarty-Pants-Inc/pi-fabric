import { describe, expect, it, vi } from "vitest";
import { FabricExecutionTraceRecorder } from "../src/audit/trace.js";
import {
  ActionRegistry,
  type FabricRegistryInvocationContext,
} from "../src/core/action-registry.js";
import { DECISION_RESULT_MAX_BYTES, MAX_AUDIT_VALUE_CHARS } from "../src/core/action-result.js";
import { DECISION_MAX_BYTES } from "../src/jev/decision-profiles.js";
import type { DecisionResult } from "../src/jev/decision-types.js";
import type { FabricProvider } from "../src/protocol.js";
import { callProgram, jevContext, launch, setupJev } from "./jev-test-helpers.js";

const ERROR = "jev.decide result must be JSON-serializable and at most 16 MiB (UTF-8)";
const GENERIC_CAP = 2 * 1024 * 1024;
const tail = "private-evidence-tail";
const decision = (rawJson = "{}", status: "ok" | "error" = "ok"): DecisionResult => ({
  status,
  provider: "typesafe",
  api: "system-one",
  model: "test-only",
  answers: { q: { type: "refusal", reason: "preserve refusal", extra: [1, 2] } },
  usage: { input_tokens: null, output_tokens: 7 },
  provenance: { kind: "native", extra: { untouched: true } },
  raw: { unknown: [1, "evidence"] },
  rawJson,
  rawComplete: true,
  httpStatus: status === "error" ? 429 : 200,
  error: status === "error" ? { code: 429, message: "rate limited" } : null,
  budget: { exhausted: status === "error", reason: status === "error" ? "rate limit" : "" },
});

function harness(value: unknown, providerName = "jev", actionName = "decide") {
  const registry = new ActionRegistry();
  const descriptor = { name: actionName, description: "Fixture", inputSchema: {}, risk: "read" as const };
  const provider: FabricProvider = {
    name: providerName,
    description: "Fixture (no network)",
    list: async () => [descriptor],
    describe: async name => name === actionName ? descriptor : undefined,
    invoke: async () => value,
  };
  registry.register(provider);
  const trace = new FabricExecutionTraceRecorder();
  const observeInvocation = vi.fn();
  const context: FabricRegistryInvocationContext = {
    ...jevContext(), approve: async () => {}, audits: [], maxResultChars: GENERIC_CAP,
    trace, observeInvocation,
  };
  return { registry, context, trace, observeInvocation };
}

describe("lossless jev.decide registry boundary", () => {
  it("pins the core transport budget to the native API without importing its graph into core", () => {
    expect(DECISION_RESULT_MAX_BYTES).toBe(16 * 1024 * 1024);
    expect(DECISION_RESULT_MAX_BYTES).toBe(DECISION_MAX_BYTES);
  });

  it.each(["ok", "error"] as const)("retains a >2 MiB %s response but bounds audit/UI/trace previews", async status => {
    const value = decision(JSON.stringify({ evidence: "x".repeat(GENERIC_CAP + 1), tail }), status);
    const { registry, context, trace, observeInvocation } = harness(value);
    const serialized = JSON.stringify(value);
    const result = await registry.invoke("jev.decide", {}, context);
    expect(result).toBe(value);
    expect(result).not.toHaveProperty("fabricTruncated");
    expect(JSON.stringify(result)).toBe(serialized);
    expect(context.audits[0]).toMatchObject({ success: true, resultTruncated: false, resultChars: serialized.length });
    const auditPreview = JSON.stringify(context.audits[0]!.result);
    expect(auditPreview.length).toBeLessThanOrEqual(MAX_AUDIT_VALUE_CHARS);
    expect(auditPreview).not.toContain(tail);
    const end = observeInvocation.mock.calls.map(([event]) => event).find(event => event.type === "call_end");
    expect(end).toMatchObject({ success: true });
    expect(JSON.stringify(end.result).length).toBeLessThan(20_000);
    expect(JSON.stringify(end.result)).not.toContain(tail);
    const sealed = trace.seal("succeeded", []);
    expect(sealed.operations[0]).toMatchObject({ outcome: "succeeded" });
    expect(sealed.operations[0]!.resultTruncated).not.toBe(true);
    expect(JSON.stringify(sealed).length).toBeLessThan(70_000);
    expect(JSON.stringify(sealed)).not.toContain(tail);
  });

  it.each(["x", "é", "😀"])("accepts the exact UTF-8 JSON boundary for %s and rejects one byte more", async unit => {
    const value = decision("");
    const remaining = DECISION_RESULT_MAX_BYTES - Buffer.byteLength(JSON.stringify(value), "utf8");
    const width = Buffer.byteLength(unit, "utf8");
    value.rawJson = unit.repeat(Math.floor(remaining / width)) + "x".repeat(remaining % width);
    expect(Buffer.byteLength(JSON.stringify(value), "utf8")).toBe(DECISION_RESULT_MAX_BYTES);
    const { registry, context } = harness(value);
    // The action-specific transport budget applies even with a tiny generic cap.
    context.maxResultChars = 1;
    expect(await registry.invoke("jev.decide", {}, context)).toBe(value);
    value.rawJson += "x";
    expect(Buffer.byteLength(JSON.stringify(value), "utf8")).toBe(DECISION_RESULT_MAX_BYTES + 1);
    // Nor can a caller enlarge the strict transport budget.
    context.maxResultChars = DECISION_RESULT_MAX_BYTES * 2;
    await expect(registry.invoke("jev.decide", {}, context)).rejects.toThrow(new Error(ERROR));
    expect(context.audits[1]).toMatchObject({ success: false, error: ERROR });
    expect(context.audits[1]!.result).toBeUndefined();
  });

  it("counts JSON escaping and the complete result envelope, not rawJson alone", async () => {
    const value = decision('"'.repeat(DECISION_RESULT_MAX_BYTES / 2));
    expect(Buffer.byteLength(value.rawJson!, "utf8")).toBeLessThan(DECISION_RESULT_MAX_BYTES);
    expect(Buffer.byteLength(JSON.stringify(value), "utf8")).toBeGreaterThan(DECISION_RESULT_MAX_BYTES);
    const { registry, context } = harness(value);
    await expect(registry.invoke("jev.decide", {}, context)).rejects.toThrow(new Error(ERROR));
  });

  const circular: Record<string, unknown> = {};
  circular.self = circular;
  it.each([
    ["undefined", undefined],
    ["function", () => tail],
    ["symbol", Symbol(tail)],
    ["bigint", 1n],
    ["circular", circular],
    ["throwing toJSON", { toJSON() { throw new Error(tail); } }],
    ["throwing getter", { get rawJson() { throw new Error(tail); } }],
    ["nested undefined", { raw: { omitted: undefined } }],
    ["nested function", { raw: { omitted: () => tail } }],
    ["nonfinite number", { raw: { coerced: Infinity } }],
  ])("rejects %s with a fixed payload-free failure", async (_name, value) => {
    const { registry, context, observeInvocation, trace } = harness(value);
    await expect(registry.invoke("jev.decide", {}, context)).rejects.toThrow(new Error(ERROR));
    expect(context.audits[0]).toMatchObject({ success: false, error: ERROR });
    expect(context.audits[0]!.result).toBeUndefined();
    const end = observeInvocation.mock.calls.at(-1)![0];
    expect(end).toMatchObject({ type: "call_end", success: false, error: ERROR });
    expect(end.result).toBeUndefined();
    const sealed = trace.seal("failed", []);
    expect(sealed.operations[0]).toMatchObject({ outcome: "failed", failureStage: "invoke" });
    expect(JSON.stringify({ audits: context.audits, end, sealed })).not.toContain(tail);
  });

  it.each([
    ["jev", "evaluate"],
    ["jev", "decideExtra"],
    ["demo", "decide"],
  ])("keeps generic truncation for %s.%s", async (provider, action) => {
    const value = decision("x".repeat(GENERIC_CAP + 1));
    const { registry, context } = harness(value, provider, action);
    const result = await registry.invoke(`${provider}.${action}`, {}, context);
    expect(result).toMatchObject({ fabricTruncated: true, originalChars: JSON.stringify(value).length });
    expect(JSON.stringify(result).length).toBeLessThanOrEqual(GENERIC_CAP);
    expect(context.audits[0]).toMatchObject({ success: true, resultTruncated: true });
  });

  it.each(["ok", "error"] as const)("delivers a >2 MiB %s response through the real managed guest path", async status => {
    const { provider, registry } = setupJev();
    const value = decision(JSON.stringify({ evidence: "x".repeat(GENERIC_CAP + 1), tail }), status);
    // Only decision inference is mocked; registry policy, manager, and QuickJS run normally.
    const invoke = provider.invoke.bind(provider);
    const providerSpy = vi.spyOn(provider, "invoke").mockImplementation(async (name, args, context) =>
      name === "decide" ? value : invoke(name, args, context));
    const registrySpy = vi.spyOn(registry, "invoke");
    try {
      const run = await callProgram(provider, "run", launch(`
        const result = await jev.decide({state: "fixture", questions: {
          q: {type: "boolean", instructions: "Fixture only"}
        }});
        return { status: result.status, length: result.rawJson!.length,
          tail: result.rawJson!.slice(-${tail.length + 2}),
          refusal: result.answers.q.type, provenance: result.provenance,
          usage: result.usage, error: result.error, budget: result.budget };
      `, { requires: ["jev.decide"], limits: { maxEvaluations: 1 } }));
      expect(run.state, run.error).toBe("completed");
      expect(run.evaluations).toBe(1);
      expect(run.result).toEqual({
        status, length: value.rawJson!.length, tail: value.rawJson!.slice(-tail.length - 2),
        refusal: "refusal", provenance: value.provenance, usage: value.usage,
        error: value.error, budget: value.budget,
      });
      expect(registrySpy).toHaveBeenCalledWith("jev.decide", expect.any(Object), expect.objectContaining({
        maxResultChars: DECISION_MAX_BYTES, parentToolCallId: `jev:${run.id}`,
      }));
    } finally {
      registrySpy.mockRestore();
      providerSpy.mockRestore();
      await provider.close();
    }
  }, 15_000);
});
