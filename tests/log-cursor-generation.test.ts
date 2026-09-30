import fs from "node:fs";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { readJsonlPage, readJsonlPageFromDescriptor } from "../src/log-tail.js";
import { compactTerminalRunLog } from "../src/worker/run-log.js";

const roots: string[] = [];
const logFile = (text: string): string => {
  const scratch = path.resolve(".local");
  fs.mkdirSync(scratch, { recursive: true });
  const root = fs.mkdtempSync(path.join(scratch, "log-cursor-test-"));
  roots.push(root);
  const file = path.join(root, "events.jsonl");
  fs.writeFileSync(file, text);
  return file;
};
const records = (count = 6) => Array.from({ length: count }, (_, index) => JSON.stringify({ index })).join("\n") + "\n";
const indices = (page: ReturnType<typeof readJsonlPage>) => page.lines.map((line) => (line.parsed as { index: number }).index);
const boundPage = (file: string, page: ReturnType<typeof readJsonlPage>) => readJsonlPage(file, 2, page.before, undefined, page.generation);

afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("generation-bound byte log cursors", () => {
  it("returns same-FD dev:ino and keeps legacy numeric paging bindable", () => {
    const file = logFile(records());
    const latest = readJsonlPage(file, 2);
    const descriptor = fs.openSync(file, "r");
    try {
      const stat = fs.fstatSync(descriptor, { bigint: true });
      expect(latest.generation).toBe(`${stat.dev}:${stat.ino}`);
    } finally { fs.closeSync(descriptor); }
    expect(indices(latest)).toEqual([4, 5]);
    const legacy = readJsonlPage(file, 2, latest.before);
    expect(indices(legacy)).toEqual([2, 3]);
    expect(legacy.generation).toBe(latest.generation);
    expect(indices(boundPage(file, legacy))).toEqual([0, 1]);
  });

  it("does not stale on same-inode append", () => {
    const file = logFile(records());
    const latest = readJsonlPage(file, 2);
    fs.appendFileSync(file, `${JSON.stringify({ index: 6 })}\n`);
    const older = boundPage(file, latest);
    expect(indices(older)).toEqual([2, 3]);
    expect(older.generation).toBe(latest.generation);
  });

  it.each(["shorter", "equal", "growing"])("rejects a %s replacement before reading any bytes", (size) => {
    const file = logFile(records());
    const latest = readJsonlPage(file, 2);
    const replacement = size === "shorter" ? records(3) : size === "equal" ? records() : records(10);
    fs.writeFileSync(`${file}.owned-replacement`, replacement);
    fs.renameSync(`${file}.owned-replacement`, file);
    const read = vi.spyOn(fs, "readSync");
    expect(() => boundPage(file, latest)).toThrowError(expect.objectContaining({ name: "cursor-stale" }));
    expect(read).not.toHaveBeenCalled();
    expect(indices(readJsonlPage(file, 2))).toEqual(size === "shorter" ? [1, 2] : size === "equal" ? [4, 5] : [8, 9]);
  });

  it("holds an agents.log byte cursor across actual terminal compaction", () => {
    const content = [{ type: "text", text: "complete durable result ".repeat(100) }];
    const events = [
      { type: "tool_execution_start", toolCallId: "held", toolName: "bash", args: {} },
      { type: "tool_execution_end", toolCallId: "held", toolName: "bash", result: { content }, isError: false },
      { type: "message_end", message: { role: "toolResult", toolCallId: "held", toolName: "bash", content, isError: false } },
      { type: "agent_end" },
    ];
    const original = events.map((event) => JSON.stringify(event)).join("\n") + "\n";
    const file = logFile(original);
    const held = fs.openSync(file, "r");
    try {
      const latest = readJsonlPage(file, 2);
      expect(boundPage(file, latest).lines.map((line) => (line.parsed as { type: string }).type)).toEqual(["tool_execution_start", "tool_execution_end"]);
      expect(compactTerminalRunLog(file, "completed")).toMatchObject({ compacted: 1 });
      expect(() => boundPage(file, latest)).toThrowError(expect.objectContaining({ name: "cursor-stale" }));
      const oldPage = readJsonlPageFromDescriptor(held, 10, undefined, undefined, undefined, latest.generation);
      expect(oldPage.lines.map((line) => line.raw).join("\n") + "\n").toBe(original);
      expect(readJsonlPage(file, 10).generation).not.toBe(latest.generation);
      // A bare legacy offset cannot identify the replaced inode. It remains accepted,
      // but its response carries only the current generation, not a safety claim.
      expect(readJsonlPage(file, 2, latest.before).generation).not.toBe(latest.generation);
    } finally { fs.closeSync(held); }
  });

  it("samples identity and reads the same opened FD even if the path changes during fstat", () => {
    const file = logFile(records());
    const latest = readJsonlPage(file, 2);
    fs.writeFileSync(`${file}.owned-replacement`, records(10));
    const nativeFstat = fs.fstatSync;
    vi.spyOn(fs, "fstatSync").mockImplementationOnce(((...args: Parameters<typeof fs.fstatSync>) => {
      const stat = nativeFstat(...args);
      fs.renameSync(`${file}.owned-replacement`, file);
      return stat;
    }) as typeof fs.fstatSync);
    const page = boundPage(file, latest);
    expect(indices(page)).toEqual([2, 3]);
    expect(page.generation).toBe(latest.generation);
    expect(() => boundPage(file, latest)).toThrowError(expect.objectContaining({ name: "cursor-stale" }));
  });

  it("preserves malformed raw records, byte bounds and ordinary missing-file behavior", () => {
    const file = logFile(`${JSON.stringify({ text: "x".repeat(4096) })}\nnot-json\n{"index":2}\n`);
    const page = readJsonlPage(file, 10, undefined, 1024);
    expect(page.hasMore).toBe(true);
    expect(page.lines.map((line) => line.raw)).toEqual(["not-json", '{"index":2}']);
    expect(page.lines[0]!.parsed).toBeUndefined();
    expect(page.generation).toEqual(expect.any(String));
    expect(readJsonlPage(`${file}.missing`, 2)).toEqual({ lines: [], hasMore: false });
  });
});
