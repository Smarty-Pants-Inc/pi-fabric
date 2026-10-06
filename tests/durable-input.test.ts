import { describe, expect, it } from "vitest";
import { durableInput } from "../src/durable-input.js";

const image = { type: "image" as const, mimeType: "image/png", data: "aGVsbG8=" };

describe("durable request input", () => {
  it("retains legacy text requests and snapshots image/schema data", () => {
    expect(durableInput({ task: "hello", systemPrompt: "system" }, false)).toMatchObject({ content: "hello", instructions: "system" });
    const schema = { type: "object", properties: { answer: { type: "string" } } };
    const images = [{ ...image }];
    const prepared = durableInput({ task: "look", images, schema }, true);
    images[0]!.data = "Ynll"; schema.properties.answer.type = "number";
    expect(prepared.content).toEqual([{ type: "text", text: "look" }, image]);
    expect(prepared.schema).toEqual({ type: "object", properties: { answer: { type: "string" } } });
  });

  it.each(["", "invalid!", "a", "a===", "=abc", "abc===", "data:image/png;base64,aGVsbG8="])("rejects malformed base64 %s", data => {
    expect(() => durableInput({ task: "look", images: [{ ...image, data }] }, true)).toThrow(/base64/);
  });

  it("refuses unsupported images and bounded image/schema sizes", () => {
    expect(() => durableInput({ task: "look", images: [image] }, false)).toThrow(/does not accept image/);
    expect(() => durableInput({ task: "look", images: [{ ...image, mimeType: "image/svg+xml" }] }, true)).toThrow(/base64/);
    expect(() => durableInput({ task: "look", images: Array.from({ length: 33 }, () => image) }, true)).toThrow(/32 images/);
    expect(() => durableInput({ task: "look", images: [{ ...image, data: "a".repeat(16_777_220) }] }, true)).toThrow(/base64/);
    expect(() => durableInput({ task: "look", images: Array.from({ length: 2 }, () => ({ ...image, data: "a".repeat(8_388_612) })) }, true)).toThrow(/16777216/);
    expect(() => durableInput({ task: "schema", schema: [] as never }, true)).toThrow(/JSON object/);
    expect(() => durableInput({ task: "schema", schema: { description: "x".repeat(262_144) } }, true)).toThrow(/262144/);
  });
});
