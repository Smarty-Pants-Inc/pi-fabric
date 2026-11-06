import { describe, expect, it } from "vitest";
import { projectToolResultLogEvent } from "../src/worker/tool-result-log.js";
import { TranscriptAccumulator } from "../src/ui/transcript-parser.js";

// Keep reason: data-safety boundary between guarded public tool content and
// persistence. These are configured FAKE values, not a detector/redactor test.
const FAKE = "FAKE-RUN-LOG-configured/credential+only?";
const ENCODED_FAKE = encodeURIComponent(FAKE);
const content = [{ type: "text", text: "[guarded final result]" }];
const finalEvent = (message: Record<string, unknown> = {}) => ({
  type: "message_end",
  message: { role: "toolResult", toolCallId: "call-a", toolName: "fake_tool", content, ...message },
});

// Synchronous and stateless: no worker process, guard replacement, or timers.
describe("projectToolResultLogEvent persistence boundary", () => {
  it.each(["pi", "pi-durable", "claude"])("withholds raw execution/results and non-final toolResult copies (%s)", runner => {
    const result = { content: [{ type: "text", text: FAKE }], details: { encoded: ENCODED_FAKE } };
    const events = [
      { type: "tool_execution_update", toolCallId: "call-a", args: { value: FAKE }, partialResult: result },
      { type: "tool_result", toolCallId: "call-a", result },
      { ...finalEvent({ content: result.content }), type: "message_start" },
      { ...finalEvent({ content: result.content }), type: "message_update" },
    ];
    for (const event of events) expect(projectToolResultLogEvent(event, runner)).toBeUndefined();
  });

  it.each(["pi", "pi-durable", "claude"])("projects immutable raw-end metadata without FAKE payloads (%s)", runner => {
    const event = {
      type: "tool_execution_end", toolCallId: "native-id", toolName: "fake_tool", isError: true, durationMs: 4.5,
      result: { content: FAKE }, details: FAKE, structuredContent: ENCODED_FAKE,
      resultMetadata: { content: FAKE }, opaque: FAKE, parentToolCallId: FAKE,
    };
    const before = structuredClone(event);
    Object.freeze(event);
    const projected = projectToolResultLogEvent(event, runner);
    expect(projected).toStrictEqual({
      type: "tool_execution_end", toolCallId: "native-id", toolName: "fake_tool", isError: true, durationMs: 4.5,
    });
    expect(JSON.stringify(projected)).not.toContain(FAKE);
    expect(JSON.stringify(projected)).not.toContain(ENCODED_FAKE);
    expect(event).toStrictEqual(before);
    for (const invalid of [undefined, null, 42, {}, []]) {
      expect(projectToolResultLogEvent({ ...event, toolCallId: invalid }, runner)).toBeUndefined();
      expect(projectToolResultLogEvent({ ...event, toolName: invalid, isError: "true", durationMs: Infinity }, runner)).toStrictEqual({
        type: "tool_execution_end", toolCallId: "native-id",
      });
    }
  });

  it.each(["pi", "pi-durable"])("persists only exact final guarded content and allowlisted metadata (%s)", runner => {
    for (const isError of [true, false]) {
      const event = {
        ...finalEvent({
          isError, durationMs: 12.5,
          details: { value: FAKE, encoded: ENCODED_FAKE },
          structuredContent: { value: FAKE },
          nestedCall: { result: FAKE }, terminate: FAKE, opaque: { value: FAKE },
        }),
        result: { content: FAKE }, details: { value: FAKE }, opaque: ENCODED_FAKE,
      };
      expect(projectToolResultLogEvent(event, runner)).toStrictEqual({
        type: "tool_execution_end", toolCallId: "call-a", toolName: "fake_tool",
        result: { content }, isError, durationMs: 12.5,
      });
    }
  });

  it.each(["pi", "pi-durable"])("preserves percent-encoded final TEXT exactly; encoding detection belongs to the shared guard (%s)", runner => {
    const finalContent = [
      { type: "text", text: `Literal percent-encoded content: ${ENCODED_FAKE}\n%25 stays %25.` },
      { type: "text", text: "second guarded block\n  unchanged whitespace  " },
    ];
    expect(projectToolResultLogEvent(finalEvent({ content: finalContent }), runner)).toStrictEqual({
      type: "tool_execution_end", toolCallId: "call-a", toolName: "fake_tool",
      result: { content: finalContent }, isError: false,
    });
  });

  it.each([0, 3.75])("retains finite duration %s", durationMs => {
    expect(projectToolResultLogEvent(finalEvent({ durationMs }), "pi")).toStrictEqual({
      type: "tool_execution_end", toolCallId: "call-a", toolName: "fake_tool",
      result: { content }, isError: false, durationMs,
    });
  });

  it.each([undefined, null, NaN, Infinity, -Infinity, "12", { durationMs: 12 }])(
    "omits absent/non-finite/non-numeric duration %s", durationMs => {
      expect(projectToolResultLogEvent(finalEvent({ durationMs }), "pi-durable")).toStrictEqual({
        type: "tool_execution_end", toolCallId: "call-a", toolName: "fake_tool",
        result: { content }, isError: false,
      });
    },
  );

  it.each([undefined, null, "true", 1])("does not promote non-boolean isError %s", isError => {
    expect(projectToolResultLogEvent(finalEvent({ isError }), "pi")?.isError).toBe(false);
  });

  it.each(["pi", "pi-durable"])("rejects missing/invalid canonical identifying fields (%s)", runner => {
    for (const key of ["toolCallId", "toolName"]) {
      const missing: Record<string, unknown> = { ...finalEvent().message };
      delete missing[key];
      expect(projectToolResultLogEvent({ type: "message_end", message: missing }, runner)).toBeUndefined();
      for (const invalid of [undefined, null, 42, false, [], {}]) {
        expect(projectToolResultLogEvent(finalEvent({ [key]: invalid }), runner)).toBeUndefined();
      }
    }
  });

  it.each(["claude", "custom-runner"])("does not treat a non-Pi final frame as guarded (%s)", runner => {
    expect(projectToolResultLogEvent(finalEvent(), runner)).toBeUndefined();
  });

  it.each(["pi", "pi-durable"])("has only metadata completion when the final canonical event never arrives (%s)", runner => {
    const events = [
      { ...finalEvent({ content: [{ type: "text", text: FAKE }] }), type: "message_start" },
      { type: "tool_execution_update", toolCallId: "call-a", partialResult: { content: FAKE } },
      { type: "tool_execution_end", toolCallId: "call-a", toolName: "fake_tool", result: { content: FAKE } },
    ];
    expect(events.map(event => projectToolResultLogEvent(event, runner))).toStrictEqual([
      undefined, undefined, { type: "tool_execution_end", toolCallId: "call-a", toolName: "fake_tool" },
    ]);
  });

  it.each(["pi", "pi-durable"])("keeps interleaved IDs tied only to their own final guarded content (%s)", runner => {
    const a = [{ type: "text", text: "guarded A" }];
    const b = [{ type: "text", text: "guarded B error" }];
    const events = [
      { type: "tool_execution_update", toolCallId: "a", partialResult: { content: FAKE } },
      { type: "tool_execution_end", toolCallId: "b", result: { content: FAKE } },
      finalEvent({ toolCallId: "b", toolName: "fake_b", content: b, isError: true }),
      { type: "tool_execution_end", toolCallId: "a", result: { content: ENCODED_FAKE } },
      finalEvent({ toolCallId: "a", toolName: "fake_a", content: a, isError: false }),
      { type: "tool_execution_end", toolCallId: "no-final", result: { content: FAKE } },
    ];
    expect(events.map(event => projectToolResultLogEvent(event, runner)).filter(event => event !== undefined)).toStrictEqual([
      { type: "tool_execution_end", toolCallId: "b" },
      { type: "tool_execution_end", toolCallId: "b", toolName: "fake_b", result: { content: b }, isError: true },
      { type: "tool_execution_end", toolCallId: "a" },
      { type: "tool_execution_end", toolCallId: "a", toolName: "fake_a", result: { content: a }, isError: false },
      { type: "tool_execution_end", toolCallId: "no-final" },
    ]);
  });

  it.each(["pi", "pi-durable", "claude"])("reduces get_messages success/error payloads to metadata only (%s)", runner => {
    for (const success of [true, false]) {
      const event = {
        type: "response", id: "history-request", command: "get_messages", success,
        data: { messages: [finalEvent({ details: { encoded: ENCODED_FAKE } }).message], nested: { value: FAKE } },
        error: { message: FAKE, body: ENCODED_FAKE }, body: FAKE, details: { encoded: ENCODED_FAKE },
      };
      const before = structuredClone(event);
      expect(projectToolResultLogEvent(event, runner)).toStrictEqual({
        type: "response", id: "history-request", command: "get_messages", success,
      });
      expect(event).toStrictEqual(before);
    }
    expect(projectToolResultLogEvent({
      type: "response", id: 7, command: "get_messages", success: "true", error: FAKE,
    }, runner)).toStrictEqual({ type: "response", command: "get_messages", success: false });
  });

  it.each(["pi", "pi-durable", "claude"])("removes history copies and withholds entry_appended (%s)", runner => {
    const message = finalEvent({ details: { encoded: ENCODED_FAKE } }).message;
    const events = [
      { type: "turn_end", message, toolResults: [message], turnIndex: 2 },
      { type: "agent_end", messages: [message], willRetry: true },
      { type: "entry_appended", entry: { type: "message", message } },
    ];
    const before = structuredClone(events);
    expect(events.map(event => projectToolResultLogEvent(event, runner))).toStrictEqual([
      { type: "turn_end", toolResults: [], turnIndex: 2 }, { type: "agent_end", messages: [], willRetry: true }, undefined,
    ]);
    expect(events).toStrictEqual(before);
    expect(projectToolResultLogEvent({ type: "turn_end", turnIndex: 3 }, runner)).toStrictEqual({ type: "turn_end", turnIndex: 3 });
    expect(projectToolResultLogEvent({ type: "agent_end", willRetry: false }, runner)).toStrictEqual({ type: "agent_end", willRetry: false });
  });

  it("retains only ordered Claude native completion IDs/error flags, never payloads or duplicate envelopes", () => {
    const event = {
      type: "user", opaque: FAKE, tool_use_result: { value: FAKE, tool_use_id: "not-a-native-id" },
      message: { role: "user", details: ENCODED_FAKE, content: [
        { type: "text", text: FAKE },
        { type: "tool_result", tool_use_id: "b", content: FAKE, is_error: true, details: ENCODED_FAKE },
        { type: "tool_result", content: FAKE },
        { type: "tool_result", tool_use_id: "a", content: [{ type: "text", text: ENCODED_FAKE }], is_error: false },
      ] },
    };
    const before = structuredClone(event);
    const projected = projectToolResultLogEvent(event, "claude");
    expect(projected).toStrictEqual({ type: "user", message: { role: "user", content: [
      { type: "tool_result", tool_use_id: "b", is_error: true, content: [] },
      { type: "tool_result", tool_use_id: "a", is_error: false, content: [] },
    ] } });
    expect(event).toStrictEqual(before);

    const transcript = new TranscriptAccumulator();
    transcript.append([{ type: "assistant", message: { role: "assistant", content: [
      { type: "tool_use", id: "a", name: "fake_a" }, { type: "tool_use", id: "b", name: "fake_b" },
    ] } }]);
    expect(transcript.snapshot().entries.map(entry => entry.status)).toStrictEqual(["running", "running"]);
    expect(projected).toBeDefined();
    transcript.append([projected!]);
    expect(transcript.snapshot().entries).toEqual([
      expect.objectContaining({ id: "a", status: "completed", result: [] }),
      expect.objectContaining({ id: "b", status: "failed", result: [] }),
    ]);
  });

  it.each([false, true])("projects Claude tool inputs to immutable native metadata and completes once (error=%s)", isError => {
    const text = { type: "text", text: "ordinary assistant text" };
    const thinking = { type: "thinking", thinking: "native non-tool content", signature: "native-signature" };
    const block = {
      type: "tool_use", id: "native-input-id", name: "fake_tool",
      input: { command: FAKE, nested: { args: [ENCODED_FAKE] } },
      opaque: FAKE, supplemental: { value: ENCODED_FAKE },
    };
    const event = { type: "assistant", message: { role: "assistant", content: [text, block, thinking] } };
    const before = structuredClone(event);
    Object.freeze(block.input.nested.args);
    Object.freeze(block.input.nested);
    Object.freeze(block.input);
    Object.freeze(block);
    Object.freeze(event.message.content);
    Object.freeze(event.message);
    Object.freeze(event);
    const projected = projectToolResultLogEvent(event, "claude");
    expect(projected).toStrictEqual({ type: "assistant", message: { role: "assistant", content: [
      text, { type: "tool_use", id: "native-input-id", name: "fake_tool" }, thinking,
    ] } });
    expect(JSON.stringify(projected)).not.toContain(FAKE);
    expect(JSON.stringify(projected)).not.toContain(ENCODED_FAKE);
    expect(event).toStrictEqual(before);

    const transcript = new TranscriptAccumulator();
    transcript.append([projected!]);
    const running = transcript.snapshot().entries.filter(entry => entry.kind === "tool");
    expect(running).toEqual([expect.objectContaining({ id: "native-input-id", toolName: "fake_tool", status: "running" })]);
    expect(running[0]).not.toHaveProperty("args");
    expect(running[0]).not.toHaveProperty("text");
    expect(transcript.snapshot().entries).toEqual(expect.arrayContaining([
      expect.objectContaining({ text: "ordinary assistant text" }),
    ]));
    const completed = projectToolResultLogEvent({ type: "user", message: { role: "user", content: [
      { type: "tool_result", tool_use_id: "native-input-id", is_error: isError, content: FAKE },
    ] } }, "claude");
    transcript.append([completed!]);
    const tools = transcript.snapshot().entries.filter(entry => entry.kind === "tool");
    expect(tools).toEqual([expect.objectContaining({
      id: "native-input-id", toolName: "fake_tool", status: isError ? "failed" : "completed", result: [],
    })]);
    expect(tools[0]).not.toHaveProperty("args");
    expect(JSON.stringify(transcript.snapshot())).not.toContain(FAKE);
    expect(JSON.stringify(transcript.snapshot())).not.toContain(ENCODED_FAKE);
  });

  it("omits non-string Claude tool-use metadata without retaining input or supplemental fields", () => {
    for (const invalid of [undefined, null, 42, false, [], {}]) {
      expect(projectToolResultLogEvent({ type: "assistant", message: { role: "assistant", content: [
        { type: "tool_use", id: invalid, name: invalid, input: FAKE, opaque: ENCODED_FAKE },
      ] } }, "claude")).toStrictEqual({ type: "assistant", message: { role: "assistant", content: [{ type: "tool_use" }] } });
    }
  });

  it("has no Claude duplicate-envelope fallback without valid native result IDs", () => {
    const events = [
      { type: "user", message: { role: "user", content: "duplicate" }, tool_use_result: { tool_use_id: "a", value: FAKE } },
      { type: "user", message: { role: "user", content: [] }, tool_use_result: null },
      ...[undefined, null, 42, false, [], {}].map(tool_use_id => ({
        type: "user", message: { role: "user", content: [{ type: "tool_result", tool_use_id, content: FAKE }] },
      })),
    ];
    const before = structuredClone(events);
    for (const event of events) expect(projectToolResultLogEvent(event, "claude")).toBeUndefined();
    expect(events).toStrictEqual(before);
  });

  it("does not mutate the original final event needed by in-memory lifecycle/accounting", () => {
    const event = finalEvent({ details: { value: FAKE }, structuredContent: { encoded: ENCODED_FAKE }, isError: true });
    const before = structuredClone(event);
    Object.freeze(event.message);
    Object.freeze(event);
    expect(projectToolResultLogEvent(event, "pi")).toStrictEqual({
      type: "tool_execution_end", toolCallId: "call-a", toolName: "fake_tool", result: { content }, isError: true,
    });
    expect(event).toStrictEqual(before);
    expect(event.message).toHaveProperty("details", { value: FAKE });
  });

  it.each(["pi", "pi-durable", "claude"])("still allows unrelated user/assistant/lifecycle frames unchanged (%s)", runner => {
    const events = [
      { type: "user", message: { role: "user", content: [{ type: "text", text: "tool_result is ordinary prose" }] } },
      { type: "assistant", message: { role: "assistant", content: [{ type: "text", text: "ordinary answer" }] } },
      { type: "message_end", message: { role: "assistant", content: [{ type: "text", text: "final answer" }] } },
      { type: "message_start", message: { role: "user", content: "ordinary prompt" } },
      { type: "agent_start" }, { type: "turn_start", turnIndex: 2 },
      { type: "response", command: "get_state", success: true, data: { running: false } },
    ];
    const before = structuredClone(events);
    for (const event of events) expect(projectToolResultLogEvent(event, runner)).toBe(event);
    expect(events).toStrictEqual(before);
  });
});
