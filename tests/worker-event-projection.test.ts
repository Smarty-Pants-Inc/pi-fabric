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
    // Image data is the one field stubbed outside lifecycle history (smarty-dev#1907).
    expect(project(input, 7)).toBe(input.replaceAll('"data":"base64"', '"elided":true,"bytes":4'));
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

  describe("image data stub (smarty-dev#1907)", () => {
    const image = (data: string) => ({ type: "image", data, mimeType: "image/png" });
    const toolEnd = (data: string) => ({
      type: "tool_execution_end", toolName: "read", toolCallId: "r1",
      result: { content: [{ type: "text", text: 'Read "data" [image/png]' }, image(data)], details: { data: "x".repeat(70_000) } },
      isError: false,
    });

    it.each([1, 7, 8191, 1 << 20])("stubs every image block in %i-character chunks", (size) => {
      const big = "QUJD".repeat(64 * 1024) + "QQ==";
      const small = "iVBORw0KGgo=";
      const events = [toolEnd(big), { type: "message_end", message: { role: "toolResult", content: [image(small), image("")] } }];
      const input = events.map((event) => JSON.stringify(event)).join("\n") + '\n{"type":"agent_settled"}\n';
      const output = project(input, size);
      expect(output.length).toBeLessThan(1_000 + 70_000);
      const stub = (data: string) => ({ type: "image", mimeType: "image/png", elided: true, bytes: Buffer.from(data, "base64").length });
      expect(output.trimEnd().split("\n").map((line) => JSON.parse(line))).toEqual([
        { ...toolEnd(big), result: { ...toolEnd(big).result, content: [toolEnd(big).result.content[0], stub(big)] } },
        { type: "message_end", message: { role: "toolResult", content: [stub(small), stub("")] } },
        { type: "agent_settled" },
      ]);
    });

    it("stubs only image objects, not other data fields or data before type", () => {
      const big = "a".repeat(70_000);
      const input = [
        { type: "response", data: big },
        { content: [{ data: big, type: "image", mimeType: "image/png" }] },
        { content: [{ type: "text", data: big }] },
        { content: [{ type: "image", data: null, mimeType: "image/png" }] },
      ].map((event) => JSON.stringify(event)).join("\n");
      expect(project(input, 5)).toBe(input);
    });

    // Feed a line far past the worker's 4 MiB cap in chunks. The projection must
    // hold at most a few KiB at any time, then recover on the next line.
    const bounded = (prefix: string, filler: string, suffix: string) => {
      const projection = new PiEventProjection();
      let fed = 0;
      let emitted = 0;
      const write = (text: string) => {
        fed += text.length;
        emitted += projection.write(text).length;
        expect(fed - emitted).toBeLessThan(4096 + 8192 + prefix.length);
      };
      write(prefix);
      for (let i = 0; i < 640; i++) write(filler.repeat(8192 / filler.length));
      write(suffix + "\n");
      const next = projection.write(JSON.stringify({ type: "message_end", content: [image("QUJD")] }) + "\n");
      expect(JSON.parse(next)).toEqual({ type: "message_end", content: [{ type: "image", elided: true, bytes: 3, mimeType: "image/png" }] });
      expect(projection.end()).toBe("");
    };

    it("bounds a long key in an image object and recovers on the next line", () => {
      bounded('{"content":[{"type":"image","', "k", '":0,"data":"QUJD"}]}');
    });

    it("bounds whitespace before image data and recovers on the next line", () => {
      bounded('{"content":[{"type":"image","data":', " ", '"QUJD"}]}');
    });

    it("bounds nesting depth past the line cap and recovers on the next line", () => {
      bounded('{"content":', "[", "");
    });

    it("passes a line through unprojected once nesting exceeds the bound", () => {
      const deep = "[".repeat(300) + JSON.stringify(image("QUJD")) + "]".repeat(300);
      expect(project(deep, 7)).toBe(deep);
      const shallow = "[".repeat(200) + JSON.stringify(image("QUJD")) + "]".repeat(200);
      expect(project(shallow, 7)).toContain('"elided":true');
    });

    it("does not turn a truncated stubbed value into a complete event", () => {
      const input = JSON.stringify(toolEnd("a".repeat(70_000))).slice(0, -80);
      expect(() => JSON.parse(project(input, 4096))).toThrow();
      const output = project(input + '\n{"type":"agent_settled"}\n', 4096).trimEnd().split("\n");
      expect(() => JSON.parse(output[0]!)).toThrow();
      expect(JSON.parse(output[1]!)).toEqual({ type: "agent_settled" });
    });
  });
});
