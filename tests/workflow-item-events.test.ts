import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { describe, expect, it, vi } from "vitest";
import { FabricActivityStore } from "../src/activity/store.js";
import { validateWorkflowItemInput } from "../src/activity/workflow-items.js";
import { createFabricPersistedExecutionDetails } from "../src/audit/details.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ActionRegistry } from "../src/core/action-registry.js";
import { FabricExecutionService } from "../src/execution-service.js";
import { FABRIC_WORKFLOW_ITEM_EVENT, type FabricWorkflowItemEventV1 } from "../src/protocol.js";

const context = {
  cwd: process.cwd(),
  hasUI: false,
  sessionManager: { getSessionId: () => "session-1" },
} as unknown as ExtensionContext;

const run = async (
  code: string,
  emit?: (channel: string, data: unknown) => void,
  activity = new FabricActivityStore(),
) => {
  const config = structuredClone(DEFAULT_FABRIC_CONFIG);
  config.fullCodeMode = false;
  const service = new FabricExecutionService(new ActionRegistry(), config, activity);
  service.setEventEmitter(emit);
  const result = await service.execute({
    code,
    signal: undefined,
    parentToolCallId: "items-call",
    context,
    onPartial() {},
  });
  return { result, activity };
};

describe("workflow.item stable ids and meta", () => {
  it("accepts bounded ids, normalizes legacy ids, and fails closed on invalid status or meta", () => {
    const fallback = () => "item-7";
    expect(validateWorkflowItemInput({ id: "pkg/a:b.c_d-1", label: "x" }, fallback)).toEqual({
      id: "pkg/a:b.c_d-1",
      status: "running",
    });
    expect(validateWorkflowItemInput({ label: "x", status: "pending" }, fallback)).toEqual({
      id: "item-7",
      status: "pending",
    });
    // Ids outside the stable grammar keep the historical store normalization.
    expect(validateWorkflowItemInput({ id: "has space", label: "x" }, fallback).id).toBe("has-space");
    expect(validateWorkflowItemInput({ id: "x".repeat(129), label: "x" }, fallback).id).toBe("x".repeat(128));
    expect(validateWorkflowItemInput({ id: "emoji-🙂", label: "x" }, fallback).id).toBe("emoji");
    for (const id of ["", "  ", 7, null]) {
      expect(validateWorkflowItemInput({ id, label: "x" }, fallback).id).toBe("item-7");
    }
    expect(() => validateWorkflowItemInput({ id: "a", status: "done" }, fallback)).toThrow(/status/);
    for (const meta of [[1], null, "text", { when: new Date(0) }, { fn: () => 1 }, { n: Number.NaN }]) {
      expect(() => validateWorkflowItemInput({ id: "a", meta }, fallback)).toThrow(/plain JSON object/);
    }
    expect(() =>
      validateWorkflowItemInput({ id: "a", meta: { blob: "x".repeat(2_100) } }, fallback),
    ).toThrow(/2048 bytes/);
    const meta = { owner: "ci", nested: { attempt: 2, tags: ["a", null, true] } };
    const validated = validateWorkflowItemInput({ id: "a", meta }, fallback);
    expect(validated.meta).toEqual(meta);
    expect(validated.meta).not.toBe(meta);
  });

  it("emits one event per status transition in order, with meta kept out of the trace", async () => {
    const events: FabricWorkflowItemEventV1[] = [];
    const { result, activity } = await run(
      `
await workflow.item({ id: "pkg/a", label: "Package A", status: "pending", meta: { secretMeta: "meta-secret-value" } });
await workflow.item({ id: "pkg/a", label: "Package A", status: "running" });
await workflow.item({ id: "pkg/a", label: "Package A", status: "running", completed: 1, total: 2 });
await workflow.item({ id: "pkg/a", label: "Package A", status: "completed", meta: { attempt: 1 } });
await workflow.item({ label: "Anonymous", status: "blocked" });
await workflow.item({ id: "left-running", label: "Left running" });
return true;
`,
      (channel, data) => {
        expect(channel).toBe(FABRIC_WORKFLOW_ITEM_EVENT);
        events.push(data as FabricWorkflowItemEventV1);
      },
    );
    expect(result.success, result.error).toBe(true);
    expect(events.map(({ itemId, from, to, label }) => ({ itemId, from, to, label }))).toEqual([
      { itemId: "pkg/a", from: undefined, to: "pending", label: "Package A" },
      { itemId: "pkg/a", from: "pending", to: "running", label: "Package A" },
      { itemId: "pkg/a", from: "running", to: "completed", label: "Package A" },
      { itemId: "item-2", from: undefined, to: "blocked", label: "Anonymous" },
      { itemId: "left-running", from: undefined, to: "running", label: "Left running" },
      // The run settles items it leaves running, like the activity store.
      { itemId: "left-running", from: "running", to: "completed", label: "Left running" },
    ]);
    for (const event of events) {
      expect(event).toMatchObject({ version: 1, invocationId: "items-call", sessionId: "session-1" });
      expect(typeof event.at).toBe("number");
    }
    expect(events[0]!.meta).toEqual({ secretMeta: "meta-secret-value" });
    expect(events[1]!.meta).toBeUndefined();
    expect(events[2]!.meta).toEqual({ attempt: 1 });
    expect(activity.get("items-call")!.items.map((item) => [item.id, item.status])).toEqual([
      ["pkg/a", "completed"],
      ["item-2", "blocked"],
      ["left-running", "completed"],
    ]);
    const persisted = JSON.stringify(createFabricPersistedExecutionDetails(result));
    expect(JSON.stringify(result.trace)).not.toContain("meta-secret-value");
    expect(persisted).not.toContain("meta-secret-value");
    expect(result.trace.operations[0]).toMatchObject({
      ref: "fabric.workflow.item",
      args: { id: "pkg/a", status: "pending" },
    });
  });

  it("settles running items as failed when the program fails", async () => {
    const events: FabricWorkflowItemEventV1[] = [];
    const { result } = await run(
      `await workflow.item({ id: "a", label: "A" }); throw new Error("boom");`,
      (_channel, data) => events.push(data as FabricWorkflowItemEventV1),
    );
    expect(result.success).toBe(false);
    expect(events.map(({ from, to }) => [from, to])).toEqual([
      [undefined, "running"],
      ["running", "failed"],
    ]);
  });

  it("normalizes legacy ids instead of failing programs", async () => {
    const emit = vi.fn();
    const { result, activity } = await run(
      `await workflow.item({ id: "has space", label: "Legacy" });`,
      emit,
    );
    expect(result.success).toBe(true);
    expect(activity.get("items-call")!.items.map((item) => item.id)).toEqual(["has-space"]);
    expect(new Set(emit.mock.calls.map(([, event]) => (event as FabricWorkflowItemEventV1).itemId))).toEqual(new Set(["has-space"]));
  });

  it("never lets a throwing listener affect the program", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      const { result } = await run(
        `await workflow.item({ id: "a", label: "A", status: "completed" }); return 1;`,
        () => {
          throw new Error("listener exploded");
        },
      );
      expect(result.success, result.error).toBe(true);
      expect(result.value).toBe(1);
      expect(warn).toHaveBeenCalledWith(expect.stringContaining("listener exploded"));
    } finally {
      warn.mockRestore();
    }
  });

  it("works without an event bus or activity store", async () => {
    const config = structuredClone(DEFAULT_FABRIC_CONFIG);
    config.fullCodeMode = false;
    const service = new FabricExecutionService(new ActionRegistry(), config);
    const result = await service.execute({
      code: `return workflow.item({ label: "Only", status: "completed", meta: { a: 1 } });`,
      signal: undefined,
      parentToolCallId: "bare",
      context: { cwd: process.cwd(), hasUI: false } as ExtensionContext,
      onPartial() {},
    });
    expect(result.success, result.error).toBe(true);
    expect(result.value).toEqual({ id: "item-1", label: "Only", status: "completed" });
  });
});
