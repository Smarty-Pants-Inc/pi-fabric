import { describe, expect, it } from "vitest";
import { PiEventProjection } from "../src/worker/event-projection.js";

const project = (input: string, chunkSize = input.length): string => {
  const projection = new PiEventProjection();
  let output = "";
  for (let i = 0; i < input.length; i += chunkSize) {
    output += projection.write(input.slice(i, i + chunkSize));
  }
  return output + projection.end();
};

describe("Pi lifecycle event projection", () => {
  const messages = [{ role: "toolResult", content: [
    { type: "text", text: 'nested: { "willRetry": false } \\ \" \n 界 🚀\u2028\u2029' },
    { type: "image", data: "base64" },
  ] }];

  it.each([1, 2, 3, 7, 64, 1024])("preserves framing and trailing retry metadata in %i-character chunks", (size) => {
    const events = [
      { type: "agent_end", messages, willRetry: true },
      { messages, willRetry: false, type: "agent_end" },
      { type: "turn_end", message: { content: "answer" }, toolResults: messages, turnIndex: 4 },
      { type: "agent_settled" },
    ];
    const output = project(events.map((event) => JSON.stringify(event)).join("\r\n") + "\r\n", size);
    expect(output.split("\r\n").filter(Boolean).map((line) => JSON.parse(line))).toEqual([
      { type: "agent_end", messages: [], willRetry: true },
      { type: "agent_end", messages: [], willRetry: false },
      { type: "turn_end", message: { content: "answer" }, toolResults: [], turnIndex: 4 },
      { type: "agent_settled" },
    ]);
  });

  it("bounds history retention regardless of history size", () => {
    const projection = new PiEventProjection();
    expect(projection.write('{"type":"agent_end","messages":[{"data":"')).toBe('{"type":"agent_end","messages":[]');
    const chunk = "a".repeat(64 * 1024);
    for (let i = 0; i < 1024; i++) expect(projection.write(chunk)).toBe("");
    expect(projection.write('"}],"willRetry":true}\n')).toBe(',"willRetry":true}\n');
    expect(projection.end()).toBe("");
  });

  it("leaves authoritative messages, nested keys, and RPC state unchanged", () => {
    const events = [
      { type: "message_end", message: { role: "assistant", content: "x".repeat(1000), usage: { output: 30 } } },
      { type: "response", data: { messages, toolResults: messages } },
      { type: "tool_execution_end", result: { content: messages, details: { messages } }, isError: true },
      { type: "message_update", assistantMessageEvent: { delta: '\"messages\": [\"not a key\"]' } },
    ];
    const input = events.map((event) => JSON.stringify(event)).join("\n");
    expect(project(input, 7)).toBe(input);
  });

  it("handles escaped property names and long unrelated keys without retaining them", () => {
    const input = '{"type":"agent_end","messa\\u0067es":[{"a":[{},[],null]}],"' + "k".repeat(1000) + '":"keep"}\n';
    expect(JSON.parse(project(input, 1))).toEqual({ type: "agent_end", messages: [], ["k".repeat(1000)]: "keep" });
  });

  it.each(['["unterminated', '[{"nested":true}', '"unterminated', 'tru']) (
    "does not turn an incomplete discarded value into a complete event: %s", (value) => {
      const input = '{"type":"agent_end","messages":' + value;
      expect(() => JSON.parse(project(input, 3))).toThrow();
      const output = project(input + '\n{"type":"agent_settled"}\n', 3).trimEnd().split("\n");
      expect(() => JSON.parse(output[0]!)).toThrow();
      expect(JSON.parse(output[1]!)).toEqual({ type: "agent_settled" });
    },
  );

  it("projects a complete oversized line and following events in one chunk", () => {
    const input = JSON.stringify({ type: "agent_end", messages: [{ content: "ordinary text ".repeat(400_000) }], willRetry: false }) + '\n{"type":"agent_settled"}\n';
    expect(project(input)).toBe('{"type":"agent_end","messages":[],"willRetry":false}\n{"type":"agent_settled"}\n');
  });

  it("does not elide non-array values that violate the lifecycle schema", () => {
    for (const messages of [null, 42, true, "text", { nested: [] }]) {
      const input = JSON.stringify({ type: "agent_end", messages });
      expect(project(input, 1)).toBe(input);
    }
  });

  it("retains a complete final event without a newline", () => {
    expect(JSON.parse(project(JSON.stringify({ type: "agent_end", messages, willRetry: false }), 1)))
      .toEqual({ type: "agent_end", messages: [], willRetry: false });
  });
});
