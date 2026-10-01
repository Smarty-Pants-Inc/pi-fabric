import {
  ExtensionRunner,
  type ExtensionContext,
  type ToolCallEvent,
  type ToolResultEvent,
} from "@earendil-works/pi-coding-agent";
import { describe, expect, it, vi } from "vitest";
import {
  createFabricPersistedExecutionDetails,
  FabricExecutionTraceRecorder,
  type FabricExecutionOutcomeV1,
} from "../src/audit/index.js";
import { NESTED_TOOL_CALL_ID_PREFIX } from "../src/core/action-registry.js";
import {
  FabricToolLifecycle,
  ownsFabricToolSource,
  type FabricTopLevelToolAuthorizer,
} from "../src/core/tool-ownership.js";

const eventRunner = (
  handlers: Map<string, Array<(event: never, context: never) => unknown>>,
): ExtensionRunner => {
  const runner = Object.create(ExtensionRunner.prototype) as ExtensionRunner;
  Object.assign(runner as unknown as Record<string, unknown>, {
    extensions: [{ path: "/extensions/pi-fabric/index.ts", handlers }],
    createContext: () => ({}),
    errorListeners: new Set(),
  });
  return runner;
};

const failedDetails = (
  outcome: FabricExecutionOutcomeV1,
  failureStage?: "guard" | "invoke",
) => {
  const recorder = new FabricExecutionTraceRecorder();
  if (failureStage) {
    recorder.issueCall(failureStage === "guard" ? "pi.write" : "agents.run", {}).fail(
      failureStage,
      new Error(`${failureStage} failure`),
      outcome,
    );
  }
  return createFabricPersistedExecutionDetails({
    success: false,
    trace: recorder.seal(outcome, [], `${outcome} execution`),
  });
};

const executeThroughPiLifecycle = async (details: unknown) => {
  const toolErrors: Array<{ toolName: string; isError: boolean }> = [];
  const lifecycle = new FabricToolLifecycle(
    () => true,
    () => ({ authorize: async () => {} }),
  );
  const handlers = new Map<string, Array<(event: never, context: never) => unknown>>([
    ["tool_call", [(event) => lifecycle.toolCall(event as unknown as ToolCallEvent)]],
    ["tool_result", [(event) => lifecycle.toolResult(event as unknown as ToolResultEvent)]],
    ["tool_execution_end", [(event) => {
      const end = event as unknown as { toolName: string; isError: boolean };
      if (end.isError) toolErrors.push(end);
    }]],
  ]);
  const runner = eventRunner(handlers);
  const toolCallId = "call-outer";
  await runner.emitToolCall({
    type: "tool_call",
    toolCallId,
    toolName: "fabric_exec",
    input: { code: "return 1" },
  });

  const content = [{ type: "text" as const, text: "original output" }];
  // Pi 0.80.6 treats every returned custom-tool value as successful, even if
  // execute() included isError: true. The lifecycle event therefore starts at
  // false and middleware must repair it before tool_execution_end.
  const patch = await runner.emitToolResult({
    type: "tool_result",
    toolCallId,
    toolName: "fabric_exec",
    input: { code: "return 1" },
    content,
    details,
    isError: false,
  });
  const final = {
    content: patch?.content ?? content,
    details: patch?.details ?? details,
    isError: patch?.isError ?? false,
  };
  await runner.emit({
    type: "tool_execution_end",
    toolCallId,
    toolName: "fabric_exec",
    result: final,
    isError: final.isError,
  });
  return { final, toolErrors };
};

describe("Fabric outer tool lifecycle", () => {
  it.each([
    ["type error", failedDetails("failed")],
    ["runtime error", failedDetails("failed")],
    ["abort", failedDetails("aborted")],
    ["timeout", failedDetails("timed_out")],
    ["nested failure", failedDetails("failed", "invoke")],
    ["Schema guard failure", failedDetails("failed", "guard")],
    ["valid failed trace despite aggregate success", { ...failedDetails("failed"), success: true }],
    ["explicit aggregate failure", { success: false, trace: { invalid: true } }],
  ])("repairs %s through tool_result and triggers tool_error dispatch", async (_label, details) => {
    const { final, toolErrors } = await executeThroughPiLifecycle(details);
    expect(final.isError).toBe(true);
    expect(final.content).toEqual([{ type: "text", text: "original output" }]);
    expect(final.details).toBe(details);
    expect(toolErrors).toEqual([expect.objectContaining({
      toolName: "fabric_exec",
      isError: true,
    })]);
  });

  it("does not mark a valid succeeded trace as an error", async () => {
    const recorder = new FabricExecutionTraceRecorder();
    const details = createFabricPersistedExecutionDetails({
      success: true,
      trace: recorder.seal("succeeded", []),
    });
    const { final, toolErrors } = await executeThroughPiLifecycle(details);
    expect(final.isError).toBe(false);
    expect(toolErrors).toEqual([]);
  });

  it("leaves nested results and live partial update paths unaffected", async () => {
    const lifecycle = new FabricToolLifecycle(
      () => true,
      () => ({ authorize: async () => {} }),
    );
    await lifecycle.toolCall({
      type: "tool_call",
      toolCallId: "call-outer",
      toolName: "fabric_exec",
      input: {},
    });
    const nested = lifecycle.toolResult({
      type: "tool_result",
      toolCallId: `${NESTED_TOOL_CALL_ID_PREFIX}00000000-0000-4000-8000-000000000000`,
      toolName: "fabric_exec",
      input: {},
      content: [{ type: "text", text: "nested" }],
      details: { success: false },
      isError: false,
    });
    expect(nested).toBeUndefined();
    // Partial execute updates are tool_execution_update events, not
    // tool_result events, so this middleware has no partial-result surface.
  });
});

describe("Exclusive native orchestrator prefix gate", () => {
  it.each(["codemode", "tool_search"])("blocks prefixed top-level %s with and without an outer call in schema off", async toolName => {
    for (const tracked of [false, true]) {
      const authorize = vi.fn(async () => {}); // schema.mode=off
      const lifecycle = new FabricToolLifecycle(() => true, () => ({ authorize }), () => undefined, () => false, () => true);
      if (tracked) await lifecycle.toolCall({ type: "tool_call", toolCallId: "outer", toolName: "fabric_exec", input: {} });
      expect(await lifecycle.toolCall({ type: "tool_call", toolCallId: `${NESTED_TOOL_CALL_ID_PREFIX}stale-top-level`, toolName, input: {} })).toMatchObject({ block: true });
      expect(authorize).not.toHaveBeenCalled();
      lifecycle.clear();
    }
  });
});
describe("Direct top-level tool approval gate", () => {
  it.each(["codemode", "tool_search"])("blocks stale native %s calls at execution, independent of loadout visibility", async toolName => {
    let exclusive = true;
    const lifecycle = new FabricToolLifecycle(() => true, () => undefined, () => undefined, () => false, () => exclusive);
    const event = { type: "tool_call" as const, toolCallId: "stale-native", toolName, input: {} };
    await expect(lifecycle.toolCall(event)).resolves.toMatchObject({ block: true });
    await lifecycle.toolCall({ type: "tool_call", toolCallId: "outer", toolName: "fabric_exec", input: {} });
    await expect(lifecycle.toolCall({ ...event, toolCallId: `${NESTED_TOOL_CALL_ID_PREFIX}captured/1`, parentToolCallId: `${NESTED_TOOL_CALL_ID_PREFIX}captured` })).resolves.toMatchObject({ block: true });
    exclusive = false;
    await expect(lifecycle.toolCall(event)).resolves.toBeUndefined();
  });
  it("approves native nested calls instead of inheriting Fabric's prefix exemption", async () => {
    const approve = vi.fn(async () => {});
    const authorize = vi.fn(async () => {});
    const lifecycle = new FabricToolLifecycle(() => true, () => ({ authorize }), () => ({ approve }));
    const context = {} as ExtensionContext;
    await lifecycle.toolCall({ type: "tool_call", toolCallId: "outer", toolName: "fabric_exec", input: {} }, context);
    await lifecycle.toolCall({ type: "tool_call", toolCallId: `${NESTED_TOOL_CALL_ID_PREFIX}captured/1`,
      parentToolCallId: `${NESTED_TOOL_CALL_ID_PREFIX}captured`, toolName: "write", input: {} }, context);
    expect(authorize).toHaveBeenCalledWith("schema.top_level_tool.write", `${NESTED_TOOL_CALL_ID_PREFIX}captured/1`);
    expect(approve).toHaveBeenCalledOnce();
  });
  it("approves native calls while preserving owned and nested Fabric boundaries", async () => {
    const approve = vi.fn(async () => {});
    const lifecycle = new FabricToolLifecycle(
      () => true,
      () => ({ authorize: async () => {} }),
      () => ({ approve }),
    );
    const context = {} as ExtensionContext;

    await lifecycle.toolCall({
      type: "tool_call",
      toolCallId: "call-read",
      toolName: "read",
      input: { path: "README.md" },
    }, context);
    expect(approve).toHaveBeenCalledWith(
      expect.objectContaining({ toolName: "read" }),
      context,
    );

    await lifecycle.toolCall({
      type: "tool_call",
      toolCallId: "call-outer",
      toolName: "fabric_exec",
      input: { code: "return 1" },
    }, context);
    await lifecycle.toolCall({
      type: "tool_call",
      toolCallId: `${NESTED_TOOL_CALL_ID_PREFIX}nested`,
      toolName: "write",
      input: { path: "out.txt", content: "ok" },
    }, context);
    expect(approve).toHaveBeenCalledOnce();
  });
});

describe("Schema top-level tool gate", () => {
  const gate = (mode: "off" | "audit" | "enforce", ownsFabric = true) => {
    const decisions: string[] = [];
    const authorizer: FabricTopLevelToolAuthorizer = {
      async authorize(ref) {
        if (mode === "off") return;
        decisions.push(`${mode}:${ref}`);
        if (mode === "enforce") throw new Error(`blocked ${ref}`);
      },
    };
    return {
      lifecycle: new FabricToolLifecycle(() => ownsFabric, () => authorizer),
      decisions,
    };
  };

  it("uses canonical source provenance rather than SDK/extension metadata claims", () => {
    const entry = "/extensions/pi-fabric/index.ts";
    expect(ownsFabricToolSource([{
      name: "fabric_exec",
      sourceInfo: { path: entry },
    }], entry)).toBe(true);
    const sdkSpoof = [{
      name: "fabric_exec",
      sourceInfo: { path: "/sdk/custom-tools.ts" },
      risk: "read",
      source: "builtin",
      keepVisible: true,
    }];
    expect(ownsFabricToolSource(sdkSpoof, entry)).toBe(false);
    expect(ownsFabricToolSource([{
      name: "fabric_exec",
      sourceInfo: { path: "/extensions/external/index.ts" },
    }], entry)).toBe(false);
  });

  it("allows only this extension's exact top-level fabric_exec in enforce mode", async () => {
    const owned = gate("enforce", true);
    await expect(owned.lifecycle.toolCall({
      type: "tool_call",
      toolCallId: "call-owned",
      toolName: "fabric_exec",
      input: {},
    })).resolves.toBeUndefined();
    expect(owned.decisions).toEqual([]);

    const sdkCustomTool = gate("enforce");
    await expect(sdkCustomTool.lifecycle.toolCall({
      type: "tool_call",
      toolCallId: "call-sdk",
      toolName: "sdk_custom_tool",
      input: {},
    })).rejects.toThrow("blocked schema.top_level_tool.sdk_custom_tool");

    const externalFabricSpoof = gate("enforce", false);
    await expect(externalFabricSpoof.lifecycle.toolCall({
      type: "tool_call",
      toolCallId: "call-external",
      toolName: "fabric_exec",
      input: {},
    })).rejects.toThrow("blocked schema.top_level_tool.fabric_exec");
  });

  it.each([
    "external_extension_tool",
    "spoofed_read_risk",
    "spoofed_source_tool",
    "keep_visible_tool",
    "read",
  ])("blocks top-level %s regardless of descriptor metadata", async (toolName) => {
    const state = gate("enforce");
    await expect(state.lifecycle.toolCall({
      type: "tool_call",
      toolCallId: `call-${toolName}`,
      toolName,
      input: {},
    })).rejects.toThrow(`blocked schema.top_level_tool.${toolName}`);
  });

  it("does not allow native ctx.executeTool to bypass schema enforcement", async () => {
    const state = gate("enforce");
    await state.lifecycle.toolCall({ type: "tool_call", toolCallId: "outer", toolName: "fabric_exec", input: {} });
    await expect(state.lifecycle.toolCall({ type: "tool_call", toolCallId: `${NESTED_TOOL_CALL_ID_PREFIX}captured/1`,
      parentToolCallId: `${NESTED_TOOL_CALL_ID_PREFIX}captured`, toolName: "write", input: {} })).rejects.toThrow("blocked schema.top_level_tool.write");
  });

  it("allows generated nested ids only during an owned outer invocation", async () => {
    const fake = gate("enforce");
    await expect(fake.lifecycle.toolCall({
      type: "tool_call",
      toolCallId: `${NESTED_TOOL_CALL_ID_PREFIX}fake-top-level-id`,
      toolName: "read",
      input: {},
    })).rejects.toThrow("blocked schema.top_level_tool.read");

    const nested = gate("enforce");
    await nested.lifecycle.toolCall({
      type: "tool_call",
      toolCallId: "call-outer",
      toolName: "fabric_exec",
      input: {},
    });
    await expect(nested.lifecycle.toolCall({
      type: "tool_call",
      toolCallId: `${NESTED_TOOL_CALL_ID_PREFIX}nested`,
      toolName: "write",
      input: {},
    })).resolves.toBeUndefined();
    expect(nested.decisions).toEqual([]);
  });

  it.each(["child-first", "outer-first"])("revokes native prefixed child grants in %s result order", async order => {
    const { lifecycle } = gate("enforce");
    const childId = `${NESTED_TOOL_CALL_ID_PREFIX}captured/1`;
    const call = (id: string) => ({ type: "tool_call" as const, toolCallId: id, toolName: "fabric_exec", input: {} });
    const result = (id: string): ToolResultEvent => ({ type: "tool_result", toolCallId: id, toolName: "fabric_exec", input: {}, content: [], details: undefined, isError: false });
    await lifecycle.toolCall(call("outer"));
    await lifecycle.runOwned("outer", undefined, async () => {
      await lifecycle.toolCall({ type: "tool_call", toolCallId: `${NESTED_TOOL_CALL_ID_PREFIX}captured`, toolName: "captured_fixture", input: {} });
      expect(await lifecycle.toolCall({ ...call(childId), parentToolCallId: `${NESTED_TOOL_CALL_ID_PREFIX}captured` })).toBeUndefined();
      const ids = order === "outer-first" ? ["outer", childId] : [childId, "outer"];
      lifecycle.toolResult(result(ids[0]!));
      if (order === "outer-first") {
        await expect(lifecycle.runOwned(childId, undefined, async () => 1)).rejects.toThrow("authorization has ended");
      }
      lifecycle.toolResult(result(ids[1]!));
    });
    await expect(lifecycle.toolCall({ type: "tool_call", toolCallId: `${NESTED_TOOL_CALL_ID_PREFIX}later`, toolName: "write", input: {} })).rejects.toThrow("blocked schema.top_level_tool.write");
  });

  it.each(["settle", "error", "abort", "result", "clear"])("revokes a near-end nested execution on outer %s, without an unrelated root resurrecting it", async end => {
    const { lifecycle } = gate("enforce");
    const controller = new AbortController();
    const childId = `${NESTED_TOOL_CALL_ID_PREFIX}captured/1`;
    const call = (id: string) => ({ type: "tool_call" as const, toolCallId: id, toolName: "fabric_exec", input: {} });
    let release!: () => void;
    const wait = new Promise<void>(resolve => { release = resolve; });
    let nested!: Promise<void>;
    let late!: () => Promise<unknown>;
    await lifecycle.toolCall(call("outer"));
    const owned = lifecycle.runOwned("outer", controller.signal, async () => {
      await lifecycle.toolCall({ type: "tool_call", toolCallId: `${NESTED_TOOL_CALL_ID_PREFIX}captured`, toolName: "captured_fixture", input: {} });
      expect(await lifecycle.toolCall({ ...call(childId), parentToolCallId: `${NESTED_TOOL_CALL_ID_PREFIX}captured` })).toBeUndefined();
      late = () => lifecycle.toolCall({ ...call(`${NESTED_TOOL_CALL_ID_PREFIX}captured/2`), parentToolCallId: `${NESTED_TOOL_CALL_ID_PREFIX}captured` });
      // Start just before the outer returns; the child does not settle first.
      nested = lifecycle.runOwned(childId, undefined, async () => {
        await wait;
        expect(await late()).toMatchObject({ block: true });
        expect(await lifecycle.toolCall({ type: "tool_call", toolCallId: `${NESTED_TOOL_CALL_ID_PREFIX}child-write`, toolName: "write", input: {} })).toMatchObject({ block: true });
      });
      if (end === "abort") controller.abort();
      if (end === "clear") lifecycle.clear();
      if (end === "result") lifecycle.toolResult({ type: "tool_result", toolCallId: "outer", toolName: "fabric_exec", input: {}, content: [], details: undefined, isError: false });
      if (end === "error") throw new Error("outer failed");
    });
    if (end === "error") await expect(owned).rejects.toThrow("outer failed");
    else await owned;
    // No result is required for execute-time revocation; prefix-only probes fail now.
    await expect(lifecycle.toolCall({ type: "tool_call", toolCallId: `${NESTED_TOOL_CALL_ID_PREFIX}later-write`, toolName: "write", input: {} })).rejects.toThrow("blocked schema.top_level_tool.write");
    expect(await late()).toMatchObject({ block: true });
    await lifecycle.toolCall(call("unrelated-outer"));
    release();
    await nested;
    await expect(lifecycle.runOwned(childId, undefined, async () => 1)).rejects.toThrow("authorization has ended");
    lifecycle.clear();
  });

  it.each(["authorize", "approve"])("refuses native nested work whose outer settles during awaited %s", async stage => {
    let release!: () => void;
    const wait = new Promise<void>(resolve => { release = resolve; });
    const authorize = vi.fn(async () => { if (stage === "authorize") await wait; });
    const approve = vi.fn(async () => { if (stage === "approve") await wait; });
    const lifecycle = new FabricToolLifecycle(() => true, () => ({ authorize }), () => ({ approve }));
    await lifecycle.toolCall({ type: "tool_call", toolCallId: "outer", toolName: "fabric_exec", input: {} });
    let native!: Promise<unknown>;
    await lifecycle.runOwned("outer", undefined, async () => {
      native = lifecycle.toolCall({ type: "tool_call", toolCallId: `${NESTED_TOOL_CALL_ID_PREFIX}captured/1`, parentToolCallId: `${NESTED_TOOL_CALL_ID_PREFIX}captured`, toolName: "write", input: {} }, {} as ExtensionContext);
      // Allow the hook to reach its asynchronous approval/authorization wait.
      await Promise.resolve();
    });
    release();
    expect(await native).toMatchObject({ block: true });
    if (stage === "authorize") expect(approve).not.toHaveBeenCalled();
    lifecycle.clear();
  });

  it("does not let a retained native parent borrow a different executing outer grant", async () => {
    const { lifecycle } = gate("enforce");
    const call = (id: string) => ({ type: "tool_call" as const, toolCallId: id, toolName: "fabric_exec", input: {} });
    const parentId = `${NESTED_TOOL_CALL_ID_PREFIX}old-captured`;
    const child = { ...call(`${parentId}/1`), parentToolCallId: parentId };
    await lifecycle.toolCall(call("old-outer"));
    let release!: () => void;
    const hold = new Promise<void>(resolve => { release = resolve; });
    let entered!: () => void;
    const ready = new Promise<void>(resolve => { entered = resolve; });
    const old = lifecycle.runOwned("old-outer", undefined, async () => {
      await lifecycle.toolCall({ type: "tool_call", toolCallId: parentId, toolName: "captured_fixture", input: {} });
      entered(); await hold;
    });
    await ready;
    await lifecycle.toolCall(call("new-outer"));
    await lifecycle.runOwned("new-outer", undefined, async () => {
      expect(await lifecycle.toolCall(child)).toMatchObject({ block: true }); // old parent is still live, but not ours
      release(); await old;
      expect(await lifecycle.toolCall(child)).toMatchObject({ block: true }); // old parent has settled
    });
    lifecycle.clear();
  });

  it("keeps outer failure-status repair after bound execute settles and repairs tracked native prefixed results", async () => {
    const { lifecycle } = gate("enforce");
    const call = { type: "tool_call" as const, toolCallId: "outer", toolName: "fabric_exec", input: {} };
    const childId = `${NESTED_TOOL_CALL_ID_PREFIX}captured/1`;
    await lifecycle.toolCall(call);
    const execute = vi.fn(async () => {
      await lifecycle.toolCall({ type: "tool_call", toolCallId: `${NESTED_TOOL_CALL_ID_PREFIX}captured`, toolName: "captured_fixture", input: {} });
      await lifecycle.toolCall({ ...call, toolCallId: childId, parentToolCallId: `${NESTED_TOOL_CALL_ID_PREFIX}captured` });
      return { content: [], details: { success: false } };
    });
    const tool = lifecycle.bindExecution({ name: "fabric_exec", execute } as unknown as import("@earendil-works/pi-coding-agent").ToolDefinition<any, any, any>);
    await tool.execute("outer", {}, undefined, undefined, {} as Parameters<typeof tool.execute>[4]);
    for (const id of [childId, "outer"]) {
      expect(lifecycle.toolResult({ type: "tool_result", toolCallId: id, toolName: "fabric_exec", input: {}, content: [], details: { success: false }, isError: false })).toEqual({ isError: true });
    }
    expect(execute).toHaveBeenCalledOnce();
  });

  it("records would-block in audit mode and leaves off mode unchanged", async () => {
    const audit = gate("audit");
    await expect(audit.lifecycle.toolCall({
      type: "tool_call",
      toolCallId: "call-audit",
      toolName: "sdk_custom_tool",
      input: {},
    })).resolves.toBeUndefined();
    expect(audit.decisions).toEqual([
      "audit:schema.top_level_tool.sdk_custom_tool",
    ]);

    const off = gate("off");
    await expect(off.lifecycle.toolCall({
      type: "tool_call",
      toolCallId: "call-off",
      toolName: "external_extension_tool",
      input: {},
    })).resolves.toBeUndefined();
    expect(off.decisions).toEqual([]);
  });
});
