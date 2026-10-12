import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { describe, expect, it } from "vitest";
import {
  FABRIC_ASSESSMENT_TRACE_MAX_BYTES,
  FabricAssessmentRecorder,
  isFabricAssessmentTraceV1,
  readFabricAssessmentUsage,
} from "../src/audit/assessment.js";
import {
  FABRIC_EXECUTION_DETAILS_MAX_BYTES,
  createFabricPersistedExecutionDetails,
} from "../src/audit/details.js";
import { FabricExecutionTraceRecorder } from "../src/audit/trace.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ActionRegistry } from "../src/core/action-registry.js";
import { FabricExecutionService } from "../src/execution-service.js";
import type { FabricProvider } from "../src/protocol.js";

const fixtureProvider = (
  name: string,
  actions: Record<string, (args: Record<string, unknown>) => unknown>,
): FabricProvider => {
  const descriptors = Object.keys(actions).map((action) => ({
    name: action,
    description: `${name}.${action} fixture`,
    risk: "read" as const,
    inputSchema: { type: "object", additionalProperties: true },
  }));
  return {
    name,
    description: `${name} fixture`,
    async list() {
      return descriptors;
    },
    async describe(action) {
      return descriptors.find((descriptor) => descriptor.name === action);
    },
    async invoke(action, args) {
      await new Promise((resolve) => setTimeout(resolve, 5));
      return actions[action]!(args);
    },
  };
};

const program = `
await workflow.phase("Assess");
const run = await tools.call({ ref: "agents.run", args: { task: "task-secret-text" } });
const verdict = await tools.call({ ref: "jev.evaluate", args: { state: "state-secret-text" } });
const echoed = await tools.call({ ref: "demo.echo", args: { value: "arg-secret-text" } });
await workflow.item({ id: "a", label: "item-label-secret", status: "completed", meta: { m: 1 } });
try { await tools.call({ ref: "demo.fail", args: {} }); } catch {}
return [run, verdict, echoed];
`;

const execute = async (assessment: boolean) => {
  const registry = new ActionRegistry();
  registry.register(fixtureProvider("agents", {
    run: () => ({
      id: "agent-1",
      status: "completed",
      model: "anthropic/fixture-model",
      text: "result-secret-text",
      usage: { input: 100, output: 20, cacheRead: 5, cacheWrite: 1, cost: 0.25 },
    }),
  }));
  registry.register(fixtureProvider("jev", {
    evaluate: () => ({
      model: "jev-fixture",
      answers: { ok: { type: "noul", noul: 0.9 } },
      usage: { input_tokens: 40, output_tokens: 2 },
    }),
  }));
  registry.register(fixtureProvider("demo", {
    echo: (args) => ({ value: args.value }),
    fail: () => {
      throw new Error("error-secret-text");
    },
  }));
  const config = structuredClone(DEFAULT_FABRIC_CONFIG);
  config.fullCodeMode = false;
  config.trace.assessment = assessment;
  const service = new FabricExecutionService(registry, config);
  return service.execute({
    code: program,
    signal: undefined,
    parentToolCallId: "assessment-call",
    context: { cwd: process.cwd(), hasUI: false } as ExtensionContext,
    onPartial() {},
  });
};

describe("assessment projection", () => {
  it("is off by default and leaves the persisted envelope unchanged", async () => {
    expect(DEFAULT_FABRIC_CONFIG.trace.assessment).toBe(false);
    const result = await execute(false);
    expect(result.success, result.error).toBe(true);
    expect(result.assessment).toBeUndefined();
    expect(createFabricPersistedExecutionDetails(result)).not.toHaveProperty("assessment");
  });

  it("records durations, attribution, usage, and totals when enabled", async () => {
    const result = await execute(true);
    expect(result.success, result.error).toBe(true);
    const assessment = result.assessment!;
    expect(isFabricAssessmentTraceV1(assessment)).toBe(true);
    expect(assessment).toMatchObject({ kind: "pi-fabric.assessment", version: 1, outcome: "succeeded" });
    expect(assessment.durationMs).toBeGreaterThan(0);
    expect(assessment.operations.map((operation) => operation.sequence))
      .toEqual(result.trace.operations.map((operation) => operation.sequence));
    expect(assessment.operations.map(({ ref, outcome }) => [ref, outcome])).toEqual(
      result.trace.operations.map(({ ref, outcome }) => [ref, outcome]),
    );
    for (const operation of assessment.operations) {
      expect(operation.durationMs).toBeGreaterThanOrEqual(0);
    }
    const byRef = Object.fromEntries(assessment.operations.map((operation) => [operation.ref, operation]));
    expect(byRef["agents.run"]).toMatchObject({
      source: "agent",
      model: "anthropic/fixture-model",
      usage: { input: 100, output: 20, cacheRead: 5, cacheWrite: 1, totalTokens: 126, cost: 0.25 },
    });
    expect(byRef["agents.run"]!.durationMs).toBeGreaterThan(0);
    expect(byRef["jev.evaluate"]).toMatchObject({
      source: "jev",
      model: "jev-fixture",
      usage: { input: 40, output: 2, totalTokens: 42 },
    });
    expect(byRef["demo.echo"]!.usage).toBeUndefined();
    expect(byRef["demo.fail"]!.outcome).toBe("failed");
    expect(assessment.totals).toEqual({
      operations: assessment.operations.length,
      succeeded: assessment.operations.length - 1,
      failed: 1,
      usage: { input: 140, output: 22, cacheRead: 5, cacheWrite: 1, totalTokens: 168, cost: 0.25 },
    });

    const details = createFabricPersistedExecutionDetails(result);
    expect(details.assessment).toEqual(assessment);
    const serialized = JSON.stringify(details.assessment);
    for (const secret of [
      "task-secret-text",
      "state-secret-text",
      "arg-secret-text",
      "result-secret-text",
      "error-secret-text",
      "item-label-secret",
      "\"m\"",
    ]) {
      expect(serialized).not.toContain(secret);
    }
  });

  it("keeps the deterministic trace byte-for-byte identical", async () => {
    const off = await execute(false);
    const on = await execute(true);
    expect(JSON.stringify(on.trace)).toBe(JSON.stringify(off.trace));
  });

  it("reads Pi, agent, and Jev usage shapes and ignores others", () => {
    expect(readFabricAssessmentUsage({
      input: 1, output: 2, cacheRead: 3, cacheWrite: 4, totalTokens: 10,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0.5 },
    })).toEqual({ input: 1, output: 2, cacheRead: 3, cacheWrite: 4, totalTokens: 10, cost: 0.5 });
    expect(readFabricAssessmentUsage({ input_tokens: 7, output_tokens: 1 }))
      .toEqual({ input: 7, output: 1, cacheRead: 0, cacheWrite: 0, totalTokens: 8 });
    expect(readFabricAssessmentUsage({ tokens: 5 })).toBeUndefined();
    expect(readFabricAssessmentUsage("usage")).toBeUndefined();
  });

  it("attributes classifier usage explicitly and stays bounded under many operations", () => {
    let clock = 0;
    const recorder = new FabricAssessmentRecorder(() => clock++);
    const trace = new FabricExecutionTraceRecorder(recorder);
    const classifier = trace.issueCall("fabric.approval.auto", { action: "demo.write", risk: "write" });
    classifier.succeed({ decision: "allow" });
    recorder.attribute(classifier.sequence, {
      source: "classifier",
      model: "openai/classifier",
      usage: { input: 10, output: 1, cacheRead: 0, cacheWrite: 0, totalTokens: 11, cost: 0.01 },
    });
    for (let index = 0; index < 3_000; index++) {
      const operation = trace.issueCall("agents.run", { task: "x" });
      operation.succeed({ model: "m", usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, cost: 0 } });
    }
    trace.issueCall("demo.pending", {});
    trace.seal("timed_out", []);
    const assessment = recorder.seal("timed_out");
    expect(assessment.operations[0]).toMatchObject({
      ref: "fabric.approval.auto",
      source: "classifier",
      model: "openai/classifier",
    });
    expect(assessment.totals.operations).toBe(3_002);
    expect(assessment.totals.succeeded).toBe(3_001);
    expect(assessment.totals.failed).toBe(1);
    expect(assessment.totals.usage.totalTokens).toBe(11 + 6_000);
    expect(assessment.operations.length).toBeLessThanOrEqual(1_024);
    expect(assessment.counts.droppedOperations).toBe(3_002 - assessment.operations.length);
    expect(Buffer.byteLength(JSON.stringify(assessment), "utf8"))
      .toBeLessThanOrEqual(FABRIC_ASSESSMENT_TRACE_MAX_BYTES);
    expect(isFabricAssessmentTraceV1(assessment)).toBe(true);
  });

  it("trims assessment rows before trace operations in the persisted envelope", () => {
    const recorder = new FabricAssessmentRecorder();
    const trace = new FabricExecutionTraceRecorder(recorder);
    for (let index = 0; index < 520; index++) {
      trace.issueCall("pi.bash", { command: `echo ${"x".repeat(900)} ${index}` }).succeed(undefined);
    }
    const sealed = trace.seal("succeeded", []);
    const assessment = recorder.seal("succeeded");
    const details = createFabricPersistedExecutionDetails({ success: true, trace: sealed, assessment });
    expect(Buffer.byteLength(JSON.stringify(details), "utf8")).toBeLessThanOrEqual(FABRIC_EXECUTION_DETAILS_MAX_BYTES);
    expect(details.assessment!.operations.length).toBeLessThan(assessment.operations.length);
    expect(details.assessment!.totals).toEqual(assessment.totals);
    expect(details.trace.operations.length).toBe(sealed.operations.length);
  });
});
