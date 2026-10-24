import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { describe, expect, it } from "vitest";
import {
  FABRIC_EXECUTION_TRACE_KIND,
  FABRIC_EXECUTION_TRACE_VERSION,
  type FabricExecutionTraceV1,
} from "../src/audit/trace.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ActionRegistry } from "../src/core/action-registry.js";
import { FabricExecutionService } from "../src/execution-service.js";
import type { FabricActionDescriptor } from "../src/protocol.js";
import { formatFailureProgress } from "../src/failure-progress.js";
import { executeAfterAdmission } from "./helpers/admission-clock.js";

const trace = (
  outcome: FabricExecutionTraceV1["outcome"],
  operations: FabricExecutionTraceV1["operations"],
): FabricExecutionTraceV1 => ({
  kind: FABRIC_EXECUTION_TRACE_KIND,
  version: FABRIC_EXECUTION_TRACE_VERSION,
  outcome,
  phases: [],
  operations,
  counts: {
    droppedValues: 0,
    truncatedValues: 0,
    redactedValues: 0,
    droppedOperations: 0,
  },
});

describe("formatFailureProgress", () => {
  it("reports completed refs and paths without exposing results", () => {
    const formatted = formatFailureProgress(trace("failed", [
      {
        type: "call",
        sequence: 0,
        ref: "pi.read",
        args: { path: "src/input.ts", offset: 10, limit: 20 },
        outcome: "succeeded",
        result: "sensitive source text",
      },
      {
        type: "call",
        sequence: 1,
        ref: "pi.edit",
        args: { path: "src/output.ts" },
        outcome: "succeeded",
        result: { ok: true },
      },
      {
        type: "call",
        sequence: 2,
        ref: "pi.bash",
        args: { command: "npm test -- --runInBand" },
        outcome: "failed",
        error: "tests failed",
      },
    ]));

    expect(formatted).toContain("pi.read(src/input.ts); pi.edit(src/output.ts)");
    expect(formatted).toContain("inspect before repeating mutations");
    expect(formatted).not.toContain("sensitive source text");
    expect(formatted).not.toContain("npm test");
  });

  it("stays absent for successful runs or failures with no completed calls", () => {
    expect(formatFailureProgress(trace("succeeded", [
      {
        type: "call",
        sequence: 0,
        ref: "agents.followUp",
        args: {},
        outcome: "succeeded",
        result: { messageId: "message-1", deliveryId: "delivery-1" },
      },
    ]))).toBeUndefined();
    expect(formatFailureProgress(trace("failed", [{
      type: "call",
      sequence: 0,
      ref: "pi.bash",
      args: {},
      outcome: "failed",
    }]))).toBeUndefined();
  });

  it("shows every required side-effect receipt without truncating after eight", () => {
    const operations: FabricExecutionTraceV1["operations"] = Array.from(
      { length: 12 },
      (_, sequence) => ({
        type: "call",
        sequence,
        ref: "agents.followUp",
        args: {},
        outcome: "succeeded",
        result: { messageId: `message-${sequence}`, deliveryId: `delivery-${sequence}` },
      }),
    );
    const formatted = formatFailureProgress(trace("timed_out", operations));
    for (let sequence = 0; sequence < 12; sequence++) {
      expect(formatted).toContain(`messageId=message-${sequence}`);
      expect(formatted).toContain(`deliveryId=delivery-${sequence}`);
    }
    expect(formatted).not.toContain("+4 more");
  });

  it("marks started calls as uncertain and never exposes message text", () => {
    const formatted = formatFailureProgress(trace("aborted", [{
      type: "call",
      sequence: 0,
      ref: "agents.followUp",
      provider: "agents",
      action: "followUp",
      args: { id: "target", message: "private message" },
      outcome: "aborted",
      failureStage: "invoke",
    }]))!;
    expect(formatted).toContain("Started but not confirmed (delivery outcome unknown)");
    expect(formatted).toContain("agents.followUp");
    expect(formatted).not.toContain("private message");
    expect(formatted).not.toContain("Completed before");
  });

  it("retains an available id even when a returned side-effect result failed", () => {
    const formatted = formatFailureProgress(trace("aborted", [{
      type: "call", sequence: 0, ref: "agents.spawn", args: {},
      outcome: "aborted", failureStage: "invoke", result: { id: "spawn-stopped" },
    }]))!;
    expect(formatted).toContain("Started but not confirmed");
    expect(formatted).toContain("agents.spawn[id=spawn-stopped]");
    expect(formatted).not.toContain("Completed before");
  });

  it("uses resolved provider/action names for near-miss trace refs", () => {
    const formatted = formatFailureProgress(trace("failed", [{
      type: "call",
      sequence: 0,
      ref: "agents.followup",
      provider: "agents",
      action: "followUp",
      args: {},
      outcome: "succeeded",
      result: { messageId: "message-normalized" },
    }]))!;
    expect(formatted).toContain("agents.followUp[messageId=message-normalized]");
    expect(formatted).not.toContain("agents.followup");
  });

  it("warns when trace bounds or result eviction make receipts incomplete", () => {
    const formatted = formatFailureProgress({
      ...trace("timed_out", [{
        type: "call",
        sequence: 0,
        ref: "mesh.publish",
        args: {},
        outcome: "succeeded",
        result: { sequence: 4 },
      }]),
      counts: { droppedValues: 1, truncatedValues: 0, redactedValues: 0, droppedOperations: 3 },
    })!;
    expect(formatted).toContain(
      "receipt list incomplete: 3 trace entries dropped; do not assume unlisted calls did not run",
    );
    expect(formatted).toContain(
      "receipt list incomplete: a completed side-effect receipt id was not retained",
    );
  });

  it("lists both actual followUp and publish receipts before a blocking failure", async () => {
    const followUp: FabricActionDescriptor = {
      name: "followUp",
      description: "queue a follow-up",
      inputSchema: {
        type: "object",
        properties: { id: { type: "string" }, message: { type: "string" } },
        required: ["id", "message"],
        additionalProperties: false,
      },
      risk: "agent",
    };
    const publish: FabricActionDescriptor = {
      name: "publish",
      description: "publish one event",
      inputSchema: {
        type: "object",
        properties: { topic: { type: "string" } },
        required: ["topic"],
        additionalProperties: false,
      },
      risk: "network",
    };
    const block: FabricActionDescriptor = {
      name: "block",
      description: "block until cancelled",
      inputSchema: { type: "object", properties: {}, additionalProperties: false },
      risk: "read",
    };
    const registry = new ActionRegistry();
    registry.register({
      name: "agents",
      description: "agents",
      async list() { return [followUp]; },
      async describe(name) { return name === followUp.name ? followUp : undefined; },
      async invoke() {
        return { queued: true, messageId: "message-actual", deliveryId: "delivery-actual", routed: "main" };
      },
    });
    registry.register({
      name: "mesh",
      description: "mesh",
      async list() { return [publish]; },
      async describe(name) { return name === publish.name ? publish : undefined; },
      async invoke() {
        return {
          id: "event-actual",
          sequence: 17,
          topic: "private-topic",
          text: "private event text",
        };
      },
    });
    let admitted = false;
    let admit = () => {};
    registry.register({
      name: "demo",
      description: "demo",
      async list() { return [block]; },
      async describe(name) { return name === block.name ? block : undefined; },
      async invoke(_name, _args, context) {
        admit();
        return new Promise((_resolve, reject) => {
          context.signal?.addEventListener("abort", () => reject(context.signal!.reason), { once: true });
        });
      },
    });
    const config = structuredClone(DEFAULT_FABRIC_CONFIG);
    config.approvals.agent = "allow";
    config.approvals.network = "allow";
    config.approvals.read = "allow";
    config.executor.timeoutMs = 50;
    try {
      const result = await executeAfterAdmission((signal, startClock) => {
        admit = () => { admitted = true; startClock(); };
        return new FabricExecutionService(registry, config).execute({
          code: `
await tools.call({ ref: "agents.followUp", args: { id: "target", message: "private follow-up" } });
await tools.call({ ref: "mesh.publish", args: { topic: "private-topic" } });
await tools.call({ ref: "demo.block", args: {} });
`,
          signal,
          parentToolCallId: "failure-progress-integration",
          context: { cwd: process.cwd(), hasUI: false } as ExtensionContext,
          onPartial() {},
        });
      }, () => admitted);

      expect(result.success).toBe(false);
      const formatted = formatFailureProgress(result.trace)!;
      expect(formatted).toContain("messageId=message-actual");
      expect(formatted).toContain("deliveryId=delivery-actual");
      expect(formatted).toContain("id=event-actual");
      expect(formatted).toContain("sequence=17");
      expect(formatted).toContain("Started but not confirmed (delivery outcome unknown): demo.block");
      expect(formatted).not.toContain("private follow-up");
      expect(formatted).not.toContain("private event text");
    } finally { await registry.close(); }
  });

  it("bounds long ordinary call lists and paths", () => {
    const operations: FabricExecutionTraceV1["operations"] = Array.from(
      { length: 12 },
      (_, sequence) => ({
        type: "call",
        sequence,
        ref: "pi.edit",
        args: { path: `src/${"nested/".repeat(30)}file-${sequence}.ts` },
        outcome: "succeeded",
      }),
    );
    const formatted = formatFailureProgress(trace("failed", operations));
    expect(formatted).toContain("+4 more");
    expect(formatted!.length).toBeLessThan(1_200);
  });
});
