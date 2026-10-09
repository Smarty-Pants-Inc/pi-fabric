import { describe, expect, it } from "vitest";
import { missingToolStartIds, toolLifecycleContext, TranscriptAccumulator } from "../src/ui/transcript-parser.js";
import { isCompactToolResult } from "../src/ui/transcript-sanitization.js";

const start = (id: string) => ({ type: "tool_execution_start", toolCallId: id, toolName: "bash", args: { command: id } });
const end = (id: string, result: unknown, isError = false) => ({ type: "tool_execution_end", toolCallId: id, toolName: "bash", result, isError });
const result = (id: string, isError = false) => ({
  type: "message_end",
  message: {
    role: "toolResult", toolCallId: id, toolName: "bash",
    content: [{ type: "text", text: `${id} output` }],
    details: { exitCode: isError ? 1 : 0, nested: { audit: "whole" } }, isError,
  },
});
const payload = (event: ReturnType<typeof result>) => ({ content: event.message.content, details: event.message.details });

describe("TranscriptAccumulator canonical tool results", () => {
  it("correlates interleaved elided ends with canonical messages, without rendering markers", () => {
    const accumulator = new TranscriptAccumulator();
    accumulator.append([start("a"), start("b"), end("b", { elided: true, bytes: 90 }, true), end("a", { elided: true, bytes: 100 })]);
    expect(accumulator.snapshot().entries).toEqual([
      expect.objectContaining({ id: "a", status: "running" }),
      expect.objectContaining({ id: "b", status: "running" }),
    ]);
    expect(accumulator.snapshot().entries.every((entry) => entry.result === undefined)).toBe(true);
    accumulator.append([result("a"), result("b", true)]);
    expect(accumulator.snapshot().entries).toEqual([
      expect.objectContaining({ id: "a", status: "completed", result: payload(result("a")) }),
      expect.objectContaining({ id: "b", status: "failed", result: payload(result("b", true)) }),
    ]);
    expect(JSON.stringify(accumulator.snapshot())).not.toContain("elided");
  });

  it("joins interleaved metadata only at canonical completion, with canonical body precedence", () => {
    const accumulator = new TranscriptAccumulator();
    const metadata = { terminate: true, opaque: { version: 2, values: [false, null, "kept"] } };
    accumulator.append([
      start("a"), start("b"),
      { ...end("b", { elided: true, bytes: 90 }), resultMetadata: { customFlag: 0 } },
      { ...end("a", { elided: true, bytes: 100 }), resultMetadata: { ...metadata, content: "stale", details: "stale" } },
    ]);
    expect(accumulator.snapshot().entries.every((entry) => entry.status === "running" && entry.result === undefined)).toBe(true);
    expect(JSON.stringify(accumulator.snapshot())).not.toContain("terminate");
    accumulator.append([result("a"), result("b", true)]);
    expect(accumulator.snapshot().entries).toEqual([
      expect.objectContaining({ id: "a", status: "completed", result: { ...payload(result("a")), ...metadata } }),
      expect.objectContaining({ id: "b", status: "failed", result: { ...payload(result("b", true)), customFlag: 0 } }),
    ]);
    const completed = accumulator.snapshot();
    accumulator.append([result("a"), result("b", true)]);
    expect(accumulator.snapshot()).toEqual(completed);
  });

  it("clears pending metadata on overwrite, reuse, and full-result completion", () => {
    const accumulator = new TranscriptAccumulator();
    const marker = { ...end("a", { elided: true, bytes: 100 }), resultMetadata: { terminate: true } };
    accumulator.append([start("a"), marker, end("a", { elided: true, bytes: 80 }), result("a")]);
    expect(accumulator.snapshot().entries[0]?.result).toEqual(payload(result("a")));
    accumulator.append([marker, start("a"), result("a")]);
    expect(accumulator.snapshot().entries[1]?.result).toEqual(payload(result("a")));
    accumulator.append([start("a"), marker, end("a", { ...payload(result("a")), opaque: "legacy" }), result("a")]);
    expect(accumulator.snapshot().entries[2]?.result).toEqual({ ...payload(result("a")), opaque: "legacy" });
    accumulator.append([start("a"), result("a")]);
    expect(accumulator.snapshot().entries[3]?.result).toEqual(payload(result("a")));
  });

  it.each([false, true])("reads legacy full results with an opaque elided field (canonical=%s)", (withCanonical) => {
    const accumulator = new TranscriptAccumulator();
    const legacy = { content: [{ type: "text", text: "legacy" }], details: { exitCode: 1 }, elided: true, customFlag: 0 };
    accumulator.append([start("a"), end("a", legacy, true), ...(withCanonical ? [result("a")] : [])]);
    expect(accumulator.snapshot().entries).toEqual([expect.objectContaining({ id: "a", status: "failed", result: legacy })]);
  });

  it.each([
    { elided: true }, { elided: true, bytes: -1 }, { elided: true, bytes: 0.5 },
    { elided: true, bytes: "10" }, { elided: true, bytes: null },
    { elided: true, bytes: NaN }, { elided: true, bytes: Infinity },
    { elided: true, bytes: 10, customFlag: 0 },
    { elided: true, bytes: 10, content: [] }, { elided: true, bytes: 10, details: {} },
    { elided: false, bytes: 10 },
  ])("does not suppress malformed or extended marker-like results: %j", (value) => {
    expect(isCompactToolResult(value)).toBe(false);
    const accumulator = new TranscriptAccumulator();
    accumulator.append([start("a"), end("a", value), result("a")]);
    expect(accumulator.snapshot().entries).toHaveLength(1);
    expect(accumulator.snapshot().entries[0]).toMatchObject({ status: "completed", result: value });
  });

  it.each([0, 1, 120])("accepts only exact compact markers, including bytes=%s", (bytes) => {
    expect(isCompactToolResult({ bytes, elided: true })).toBe(true);
    const accumulator = new TranscriptAccumulator();
    accumulator.append([start("a"), end("a", { bytes, elided: true })]);
    expect(accumulator.snapshot().entries[0]).toMatchObject({ status: "running" });
    expect(accumulator.snapshot().entries[0]?.result).toBeUndefined();
  });

  it("selects nearest context ends and stops at reused lifecycle boundaries", () => {
    const oldMarker = { ...end("a", { elided: true, bytes: 100 }), resultMetadata: { terminate: true } };
    const newMarker = end("a", { elided: true, bytes: 80 });
    const missing = new Set(["a"]);
    expect(toolLifecycleContext([start("a"), oldMarker, newMarker], missing)).toEqual([start("a"), newMarker]);
    expect(toolLifecycleContext([start("a"), oldMarker, result("a"), start("a")], missing)).toEqual([start("a")]);
    expect(toolLifecycleContext([start("a"), oldMarker, result("a")], missing)).toEqual([]);
    expect(toolLifecycleContext([start("a"), oldMarker, end("a", payload(result("a")))], missing)).toEqual([start("a"), end("a", payload(result("a")))]);
    expect(toolLifecycleContext([result("a"), newMarker], missing)).toEqual([newMarker]);
    expect(toolLifecycleContext([newMarker], missing)).toEqual([newMarker]);
    expect(missing).toEqual(new Set(["a"]));
  });

  it("does not fabricate unavailable metadata in a canonical-only page", () => {
    const accumulator = new TranscriptAccumulator();
    accumulator.append([result("a")]);
    expect(accumulator.snapshot().entries[0]?.result).toEqual(payload(result("a")));
  });

  it("keeps old full-result completion and ignores its duplicate canonical message", () => {
    const accumulator = new TranscriptAccumulator();
    const canonical = result("a", true);
    accumulator.append([start("a"), end("a", payload(canonical), true)]);
    const completed = accumulator.snapshot();
    accumulator.append([canonical]);
    expect(accumulator.snapshot()).toEqual(completed);
    expect(completed.entries).toHaveLength(1);
    expect(completed.entries[0]?.status).toBe("failed");
  });

  it("reads a canonical message alone or without details and uses its error flag", () => {
    const canonical = { ...result("a", true), message: { ...result("a", true).message, details: undefined } };
    const accumulator = new TranscriptAccumulator();
    accumulator.append([end("a", { elided: true, bytes: 20 }, false), canonical]);
    expect(accumulator.snapshot().entries).toEqual([expect.objectContaining({
      id: "a", toolName: "bash", status: "failed", result: { content: canonical.message.content },
    })]);
  });

  it("finds context for a canonical-only page without treating paired ends as two calls", () => {
    expect(missingToolStartIds([result("a")])).toEqual(new Set(["a"]));
    expect(missingToolStartIds([start("a"), end("a", { elided: true, bytes: 20 }), result("a")])).toEqual(new Set());
    expect(missingToolStartIds([start("a"), end("a", payload(result("a"))), result("a")])).toEqual(new Set());
  });
});
