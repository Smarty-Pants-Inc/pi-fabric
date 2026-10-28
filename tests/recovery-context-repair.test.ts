import type { AssistantMessage, Model, ToolCall } from "@earendil-works/pi-ai";
import { convertResponsesMessages } from "@earendil-works/pi-ai/api/openai-responses-shared";
import { normalizeContext } from "@earendil-works/pi-ai/utils/transcript";
import { convertToLlm, type ContextEvent } from "@earendil-works/pi-coding-agent";
import { describe, expect, it } from "vitest";
import { MISSING_TOOL_RESULT_TEXT } from "../src/compaction/orphan-repair.js";
import { RecoveryContextRepairError, repairRecoveryContext } from "../src/recovery/context-repair.js";

type Message = ContextEvent["messages"][number];
type Result = Extract<Message, { role: "toolResult" }>;

// Keep reason: data safety. A committed effect must not look aborted to the next
// model turn, and ambiguous call/result identities must never manufacture success.
const call = (id: string, name = `tool-${id}`): ToolCall => ({
  type: "toolCall", id, name, arguments: { path: `/workspace/${id}`, bytes: "original\nbytes" },
});
const assistant = (calls: ToolCall[], stopReason: AssistantMessage["stopReason"] = "toolUse"): AssistantMessage => ({
  role: "assistant", content: [{ type: "text", text: "working" }, ...calls],
  api: "openai-responses", provider: "openai", model: "recovery-test", stopReason, timestamp: 1,
  usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0,
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
  ...(stopReason === "aborted" || stopReason === "error" ? { errorMessage: "original interruption" } : {}),
});
const result = (id: string, extra: Partial<Result> = {}): Result => ({
  role: "toolResult", toolCallId: id, toolName: `tool-${id}`,
  content: [{ type: "text", text: `durable result ${id}\n  exact bytes\u0000` }],
  isError: false, timestamp: 2, details: { receipt: `receipt-${id}` }, ...extra,
});
const user = (text = "continue"): Message => ({ role: "user", content: text, timestamp: 3 });
const custom = (): Message => ({ role: "custom", customType: "recovery-note", content: "note", display: false, timestamp: 3 });
const results = (messages: Message[]): Result[] => messages.filter((m): m is Result => m.role === "toolResult");
const freeze = <T>(value: T): T => {
  if (value && typeof value === "object") {
    for (const child of Object.values(value)) freeze(child);
    Object.freeze(value);
  }
  return value;
};
const expectUncertain = (messages: Message[], settled: Result[], code: string): void => {
  const before = JSON.stringify({ messages, settled });
  let error: unknown;
  try { repairRecoveryContext(freeze(messages), freeze(settled)); } catch (caught) { error = caught; }
  expect(error).toBeInstanceOf(RecoveryContextRepairError);
  expect(error).toMatchObject({ code });
  expect(JSON.stringify({ messages, settled })).toBe(before);
};

const model: Model<"openai-responses"> = {
  id: "recovery-test", name: "Recovery test", api: "openai-responses", provider: "openai",
  baseUrl: "https://unused.invalid", reasoning: false, input: ["text", "image"],
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 32000, maxTokens: 1024,
};
const providerView = (messages: Message[]) => convertResponsesMessages(
  model, normalizeContext({ messages: convertToLlm(messages) }), new Set(["openai"]),
);

describe("repairRecoveryContext", () => {
  it("preserves an intact projection and the original durable message object", () => {
    const saved = result("a");
    const messages = freeze([user("go"), assistant([call("a")]), saved, user()]);
    expect(repairRecoveryContext(messages, freeze([saved]))).toBe(messages);
  });

  it.each(["Tool execution aborted", "Tool execution failed", MISSING_TOOL_RESULT_TEXT, "No result provided"])(
    "replaces a synthetic/error result (%s) without changing raw history or durable bytes", (text) => {
      const original = assistant([call("a")]);
      const raw = freeze([user("go"), original, result("a", { isError: true, content: [{ type: "text", text }] }), user()]);
      const saved = freeze(result("a", { content: [
        { type: "text", text: "  exact\n\u0000result  " }, { type: "image", mimeType: "image/png", data: "AA==" },
      ], nestedCalls: { complete: false, calls: [{ id: "nested", name: "write", status: "ok" }] } }));
      const rawBytes = JSON.stringify(raw);
      const savedBytes = JSON.stringify(saved);
      const repaired = repairRecoveryContext(raw, freeze([saved]));
      expect(repaired).toEqual([raw[0], original, saved, raw[3]]);
      expect(repaired[2]).toBe(saved);
      expect(repaired[1]).toBe(original);
      expect(JSON.stringify(raw)).toBe(rawBytes);
      expect(JSON.stringify(saved)).toBe(savedBytes);
      expect(repairRecoveryContext(repaired, [saved])).toBe(repaired);
    },
  );

  it("injects absent results immediately under the original call even at the tail", () => {
    const original = assistant([call("a")]);
    const saved = result("a");
    const raw = freeze([user("go"), original]);
    const repaired = repairRecoveryContext(raw, [saved]);
    expect(repaired).toEqual([raw[0], original, saved]);
    expect(repaired[2]).toBe(saved);
    expect(repairRecoveryContext(repaired, [saved])).toBe(repaired);
  });

  it("handles multi-call windows, missing and duplicate results, and late dangling results", () => {
    const first = assistant([call("a"), call("b"), call("c")]);
    const second = assistant([call("d")]);
    const a = result("a");
    const b = result("b");
    const c = result("c");
    const d = result("d");
    const note = custom();
    const raw = freeze([first, result("a", { isError: true }), a, note, user(), second, d, b, d]);
    const repaired = repairRecoveryContext(raw, freeze([d, c, b, a, { ...a }]));
    expect(repaired).toEqual([first, b, c, a, note, raw[4], second, d]);
    expect(results(repaired)).toEqual([b, c, a, d]);
    expect(repairRecoveryContext(repaired, [a, b, c, d])).toBe(repaired);
  });

  it("does not reorder valid results just to match call order", () => {
    const a = result("a");
    const b = result("b");
    const raw = [assistant([call("a"), call("b")]), b, a, user()];
    expect(repairRecoveryContext(raw, [a, b])).toBe(raw);
  });

  it.each<Message>([
    custom(),
    { role: "branchSummary", summary: "summary", fromId: "prior", timestamp: 3 },
    { role: "compactionSummary", summary: "summary", tokensBefore: 10, timestamp: 3 },
    { role: "bashExecution", command: "printf note", output: "note", exitCode: 0, cancelled: false, truncated: false, timestamp: 3 },
  ])("pairs a committed result before a $role entry that Pi converts to a user boundary", (boundary) => {
    const original = assistant([call("a")]);
    const saved = result("a");
    const raw = freeze([original, boundary, saved, user()]);
    const repaired = repairRecoveryContext(raw, [saved]);
    expect(repaired).toEqual([original, saved, boundary, raw[3]]);
    expect(providerView(repaired).filter((item) => item.type === "function_call_output")).toEqual([
      { type: "function_call_output", call_id: "a", output: "durable result a\n  exact bytes\u0000" },
    ]);
    expect(repairRecoveryContext(repaired, [saved])).toBe(repaired);
  });

  it("does not move an intact pair across an excluded bash entry", () => {
    const saved = result("a");
    const raw: Message[] = [assistant([call("a")]), {
      role: "bashExecution", command: "printf note", output: "note", exitCode: 0,
      cancelled: false, truncated: false, timestamp: 3, excludeFromContext: true,
    }, saved];
    expect(repairRecoveryContext(raw, [saved])).toBe(raw);
    expect(providerView(raw).filter((item) => item.type === "function_call_output")).toHaveLength(1);
  });

  it("suppresses unsettled aborted results even across Pi-converted boundaries", () => {
    const original = assistant([call("a")], "aborted");
    const boundary = custom();
    const raw = freeze([original, boundary, result("a", { isError: true }), user()]);
    const repaired = repairRecoveryContext(raw, []);
    expect(repaired).toEqual([original, boundary, raw[3]]);
    expect(providerView(repaired).filter((item) => item.type === "function_call_output")).toEqual([]);
  });

  it.each(["aborted", "error"] as const)("makes a settled %s assistant replayable through actual provider conversion", (stopReason) => {
    const originalCall = call("original_call|fc_original", "fabric_exec");
    originalCall.arguments = { code: "await tools.write({path: '/workspace/x', content: 'x'});", marker: "unchanged" };
    const original = assistant([originalCall], stopReason);
    const saved = result(originalCall.id, { toolName: "fabric_exec", content: [{ type: "text", text: "adapter-settled native result" }] });
    const raw = freeze([user("go"), original, result(originalCall.id, { toolName: "fabric_exec", isError: true }), user()]);
    const bytes = JSON.stringify(raw);
    // This is the actual regression: Pi's provider transform filters the source
    // assistant but leaves its tool result, so simply replacing the result fails.
    expect(providerView(raw).some((item) => item.type === "function_call")).toBe(false);
    expect(providerView(raw).some((item) => item.type === "function_call_output")).toBe(true);

    const repaired = repairRecoveryContext(raw, freeze([saved]));
    expect(repaired[1]).not.toBe(original);
    expect(repaired[1]).toMatchObject({ stopReason: "toolUse", content: [originalCall] });
    expect((repaired[1] as AssistantMessage).content[0]).toBe(originalCall);
    expect(repaired[2]).toBe(saved);
    expect(JSON.stringify(raw)).toBe(bytes);
    const converted = providerView(repaired);
    const calls = converted.filter((item) => item.type === "function_call");
    const outputs = converted.filter((item) => item.type === "function_call_output");
    expect(calls).toEqual([expect.objectContaining({
      type: "function_call", call_id: "original_call", name: "fabric_exec", arguments: JSON.stringify(originalCall.arguments),
    })]);
    expect(outputs).toEqual([{ type: "function_call_output", call_id: "original_call", output: "adapter-settled native result" }]);
    expect(converted.indexOf(calls[0]!)).toBeLessThan(converted.indexOf(outputs[0]!));
    expect(repairRecoveryContext(repaired, [saved])).toBe(repaired);
  });

  describe.each(["aborted", "error"] as const)("interrupted %s provider projection", (stopReason) => {
    it.each([
      { label: "unsigned partial reasoning", thinkingSignature: undefined },
      { label: "malformed reasoning signature", thinkingSignature: '{"type":"reasoning","id":"rs_cut","encrypted_content":"cut' },
      { label: "incomplete signed reasoning item", thinkingSignature: JSON.stringify({ type: "reasoning", id: "rs_cut", status: "in_progress", summary: [], encrypted_content: "truncated-signature" }) },
    ])("projects only original settled calls with $label", ({ thinkingSignature }) => {
      const first = call("original_call|fc_original", "fabric_exec");
      first.arguments = { code: "await tools.read({path: '/workspace/x'});", marker: "exact original arguments" };
      const second = call("second_call|fc_second", "read");
      second.thoughtSignature = "original-call-local-signature";
      second.namespace = "native";
      const original: AssistantMessage = {
        ...assistant([first, second], stopReason),
        responseId: "resp_interrupted", rawStopReason: "incomplete", endTurn: false,
        providerThinkingLevel: "high", thinkingLevel: "high",
        deferred: { provider: "openai", modelId: model.id, api: model.api, id: "resp_interrupted", data: { reasoning: "truncated" } },
        content: [
          { type: "thinking", thinking: "partial unsigned provider thought" },
          first,
          { type: "thinking", thinking: "partial signed provider thought", ...(thinkingSignature === undefined ? {} : { thinkingSignature }) },
          { type: "text", text: "interrupted untrusted free text", textSignature: '{"v":1,"id":"msg_cut","phase":"commentary"}' },
          second,
        ],
      };
      const saved = freeze([
        result(first.id, { toolName: first.name, content: [{ type: "text", text: "original truthful fabric result" }] }),
        result(second.id, { toolName: second.name, isError: true, content: [{ type: "text", text: "original truthful read failure" }] }),
      ]);
      const raw = freeze([original, result(first.id, { toolName: first.name, isError: true }), result(second.id, { toolName: second.name, isError: true })]);
      const before = JSON.stringify({ raw, saved });
      const repaired = repairRecoveryContext(raw, saved);
      // Exercise the real convertToLlm -> normalizeContext -> Responses conversion,
      // not just the context hook: truncated signatures previously threw here.
      const converted = providerView(repaired);
      expect(converted).toEqual([
        { type: "function_call", id: "fc_original", call_id: "original_call", name: first.name, arguments: JSON.stringify(first.arguments) },
        { type: "function_call", id: "fc_second", call_id: "second_call", name: second.name, arguments: JSON.stringify(second.arguments), namespace: "native" },
        { type: "function_call_output", call_id: "original_call", output: "original truthful fabric result" },
        { type: "function_call_output", call_id: "second_call", output: "original truthful read failure" },
      ]);
      const projected = repaired[0] as AssistantMessage;
      expect(projected).not.toBe(original);
      expect(projected).toEqual({
        role: "assistant", content: [first, second], api: original.api, provider: original.provider,
        model: original.model, usage: original.usage, stopReason: "toolUse", timestamp: original.timestamp,
      });
      expect(projected.content).toEqual([first, second]);
      expect(projected.content[0]).toBe(first);
      expect(projected.content[1]).toBe(second);
      expect((projected.content[0] as ToolCall).arguments).toBe(first.arguments);
      expect(projected).not.toHaveProperty("deferred");
      expect(projected).not.toHaveProperty("responseId");
      expect(projected).not.toHaveProperty("errorMessage");
      expect(projected).not.toHaveProperty("rawStopReason");
      expect(repaired[1]).toBe(saved[0]);
      expect(repaired[2]).toBe(saved[1]);
      expect(repairRecoveryContext(repaired, saved)).toBe(repaired);
      expect(providerView(repairRecoveryContext(raw, saved))).toEqual(converted);
      expect(JSON.stringify({ raw, saved })).toBe(before);
      expect(original.stopReason).toBe(stopReason);
    });
  });

  it.each(["toolUse", "stop"] as const)("preserves complete signed reasoning and text on ordinary %s assistants", (stopReason) => {
    const reasoning = { type: "reasoning", id: "rs_complete", status: "completed", summary: [{ type: "summary_text", text: "complete reasoning" }], encrypted_content: "complete-provider-signature" };
    const originalCall = call("normal_call|fc_normal");
    const original: AssistantMessage = {
      ...assistant(stopReason === "toolUse" ? [originalCall] : [], stopReason),
      responseId: "resp_complete", providerThinkingLevel: "high",
      content: [
        { type: "thinking", thinking: "complete reasoning", thinkingSignature: JSON.stringify(reasoning) },
        { type: "text", text: "complete ordinary answer", textSignature: '{"v":1,"id":"msg_complete","phase":"final_answer"}' },
        ...(stopReason === "toolUse" ? [originalCall] : []),
      ],
    };
    const saved = stopReason === "toolUse" ? [result(originalCall.id)] : [];
    const raw = freeze([original, ...saved]);
    const before = JSON.stringify(raw);
    const repaired = repairRecoveryContext(raw, freeze(saved));
    expect(repaired).toBe(raw);
    expect(repaired[0]).toBe(original);
    expect((repaired[0] as AssistantMessage).content).toBe(original.content);
    const converted = providerView(repaired);
    expect(converted[0]).toEqual(reasoning);
    expect(converted[1]).toMatchObject({ type: "message", id: "msg_complete", phase: "final_answer", content: [{ type: "output_text", text: "complete ordinary answer", annotations: [] }] });
    expect(converted).toEqual(providerView(raw));
    expect(JSON.stringify(raw)).toBe(before);
  });

  it("does not revive uncommitted aborted assistants or leave their error results orphaned", () => {
    const aborted = assistant([call("a")], "aborted");
    const error = assistant([call("b")], "error");
    const raw = freeze([aborted, result("a", { isError: true }), user(), error, user()]);
    const repaired = repairRecoveryContext(raw, []);
    expect(repaired).toEqual([aborted, raw[2], error, raw[4]]);
    expect(aborted.stopReason).toBe("aborted");
    expect(error.stopReason).toBe("error");
    expect(providerView(repaired).filter((item) => item.type === "function_call" || item.type === "function_call_output")).toEqual([]);
  });

  it("blocks a partially settled aborted multi-call assistant instead of resurrecting its uncommitted call", () => {
    expectUncertain([assistant([call("a"), call("b")], "aborted"), user()], [result("a")], "unsettled-aborted-call");
  });

  it("keeps a truthful durable error as an error, not an invented effect success", () => {
    const saved = result("a", { isError: true, content: [{ type: "text", text: "destination refused the operation" }] });
    const repaired = repairRecoveryContext([assistant([call("a")], "aborted")], [saved]);
    expect(results(repaired)).toEqual([saved]);
    expect(repaired[1]).toBe(saved);
    expect(saved.isError).toBe(true);
  });

  it("reuses ordinary orphan repair without presenting an unsettled call as success", () => {
    const saved = result("a");
    const raw = [result("ghost"), assistant([call("a"), call("unsettled")]), user()];
    const repaired = repairRecoveryContext(raw, [saved]);
    expect(repaired[0]).toBe(raw[1]);
    expect(results(repaired)).toEqual([saved, expect.objectContaining({
      toolCallId: "unsettled", isError: true, content: [{ type: "text", text: MISSING_TOOL_RESULT_TEXT }],
    })]);
    expect(repairRecoveryContext(repaired, [saved])).toBe(repaired);
  });

  it("rejects a durable result whose original call is absent, including after compaction", () => {
    expectUncertain([custom(), user()], [result("elided")], "missing-original-call");
  });

  it("does not normalize a different result id into the original call id", () => {
    expectUncertain([assistant([call("a|fc_item")])], [result("a", { toolName: "tool-a|fc_item" })], "missing-original-call");
  });

  it("rejects a durable result bound to the wrong tool name", () => {
    expectUncertain([assistant([call("a")])], [result("a", { toolName: "other-tool" })], "tool-name-mismatch");
  });

  it.each([false, true])("rejects conflicting raw tool-name aliases even if isError=%s", (isError) => {
    expectUncertain([assistant([call("a")]), result("a", { toolName: "other-tool", isError })], [result("a")], "tool-name-mismatch");
  });

  it("rejects conflicting durable results for the same original call", () => {
    expectUncertain([assistant([call("a")])], [result("a"), result("a", { content: [{ type: "text", text: "different effect" }] })], "conflicting-settled-results");
  });

  it("rejects a conflicting successful raw result instead of silently choosing a winner", () => {
    expectUncertain([assistant([call("a")]), result("a", { content: [{ type: "text", text: "a different success" }] })], [result("a")], "conflicting-context-result");
  });

  it.each([false, true])("rejects ambiguous original call ids (same assistant=%s)", (sameAssistant) => {
    const raw = sameAssistant
      ? [assistant([call("a"), call("a")])]
      : [assistant([call("a")]), user(), assistant([call("a")])];
    expectUncertain(raw, [result("a")], "ambiguous-original-call");
  });
});
