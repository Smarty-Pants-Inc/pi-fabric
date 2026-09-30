import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { afterEach, describe, expect, it, vi } from "vitest";
import { compactTerminalRunLog, MAX_TERMINAL_LOG_WORK_MS } from "../src/worker/run-log.js";
import { TranscriptAccumulator } from "../src/ui/transcript-parser.js";

const directories: string[] = [];
afterEach(() => {
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true });
});

// Exact three-record SOURCE_REVIEW counterexample, including the original bytes.
const reviewed = [
  '{"type":"message","message":{"role":"toolResult","toolCallId":"reused","toolName":"bash","content":[{"type":"text","text":"prior result"}],"isError":false,"timestamp":1}}',
  '{"type":"tool_execution_end","toolCallId":"reused","toolName":"bash","result":{"content":[{"type":"text","text":"new result"}],"details":{"exitCode":0},"terminate":true},"isError":false}',
  '{"type":"message_end","message":{"role":"toolResult","toolCallId":"reused","toolName":"bash","content":[{"type":"text","text":"new result"}],"details":{"exitCode":0},"isError":false,"timestamp":2}}',
];
const lines = (text: string): Array<Record<string, unknown>> => text.trimEnd().split("\n").map((line) => JSON.parse(line));
const transcript = (text: string) => {
  const accumulator = new TranscriptAccumulator();
  accumulator.append(lines(text));
  return accumulator.snapshot().entries;
};
const logFile = (text: string) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "legacy-reuse-"));
  directories.push(directory);
  const file = path.join(directory, "events.jsonl");
  fs.writeFileSync(file, text);
  return file;
};
const completions = [
  { name: "legacy message/toolResult", line: reviewed[0]! },
  { name: "Claude user/tool_result", line: JSON.stringify({ type: "user", message: { role: "user", content: [{ type: "tool_result", tool_use_id: "reused", content: "prior result" }] } }) },
  // The actual Claude receiver checks content, not message.role.
  { name: "Claude user/tool_result without role", line: JSON.stringify({ type: "user", message: { content: [{ type: "tool_result", tool_use_id: "reused", content: "prior result" }] } }) },
];
const expectRetained = (text: string) => {
  const file = logFile(text);
  const before = fs.statSync(file);
  const outcome = compactTerminalRunLog(file, "completed");
  expect(outcome).toEqual({ compacted: 0, beforeBytes: Buffer.byteLength(text), afterBytes: Buffer.byteLength(text) });
  expect(fs.readFileSync(file)).toEqual(Buffer.from(text));
  expect(fs.statSync(file)).toMatchObject({ dev: before.dev, ino: before.ino });
  expect(transcript(fs.readFileSync(file, "utf8"))).toEqual(transcript(text));
};
const expectCompacted = (text: string, entries: number) => {
  const file = logFile(text);
  const outcome = compactTerminalRunLog(file, "completed");
  expect(outcome.compacted).toBe(1);
  expect(outcome.error).toBeUndefined();
  expect(outcome.compactionSkipped).toBeUndefined();
  const after = fs.readFileSync(file, "utf8");
  expect(lines(after).find((event) => event.type === "tool_execution_end")).toMatchObject({ result: { elided: true }, resultMetadata: { terminate: true } });
  // Only the execution-end line changes; original canonical byte/key order stays.
  expect(after.trimEnd().split("\n").at(-1)).toBe(reviewed[2]);
  expect(transcript(after)).toEqual(transcript(text));
  expect(transcript(after)).toHaveLength(entries);
  expect(transcript(after).at(-1)).toMatchObject({ kind: "tool", status: "completed", result: { content: [{ type: "text", text: "new result" }], details: { exitCode: 0 }, terminate: true } });
};

describe("terminal run log mixed-format completion reuse", () => {
  it.each(completions)("retains the full missing-start end after $name", ({ line }) => {
    const text = [line, ...reviewed.slice(1)].join("\n") + "\n";
    const entries = transcript(text);
    expect(entries).toHaveLength(2);
    expect(entries[0]).toMatchObject({ kind: "tool", status: "completed" });
    expect(JSON.stringify(entries[0]?.result)).toContain("prior result");
    expect(entries[1]).toMatchObject({ kind: "tool", status: "completed", result: { content: [{ type: "text", text: "new result" }], details: { exitCode: 0 }, terminate: true } });
    expectRetained(text);
  });

  it.each(completions)("conservatively retains same-ID completion after the canonical ($name)", ({ line }) => {
    expectRetained([...reviewed.slice(1), line].join("\n") + "\n");
  });

  it("still compacts the unique missing-start pair when only the prior completion is deleted", () => {
    expectCompacted(reviewed.slice(1).join("\n") + "\n", 1);
  });

  it("still compacts an ordinary unique start/end/canonical lifecycle", () => {
    const start = JSON.stringify({ type: "tool_execution_start", toolCallId: "reused", toolName: "bash", args: {} });
    expectCompacted([start, ...reviewed.slice(1)].join("\n") + "\n", 1);
  });

  it.each(completions)("does not disqualify a unique pair for an unrelated ID ($name)", ({ line }) => {
    expectCompacted([line.replaceAll("reused", "unrelated"), ...reviewed.slice(1)].join("\n") + "\n", 2);
  });

  it("does not treat a legacy completion alone as a canonical substitute", () => {
    expectRetained([reviewed[1], reviewed[2]!.replace('"message_end"', '"message"')].join("\n") + "\n");
  });

  it("does not treat a Claude completion alone as a canonical substitute", () => {
    expectRetained([reviewed[1], completions[1]!.line].join("\n") + "\n");
  });

  it("keeps two scans plus disjoint canonical rereads with unrelated Claude completions", () => {
    const count = 32;
    const text = Array.from({ length: count }, (_, index) => {
      const toolCallId = `unique-${index}`;
      const content = [{ type: "text", text: "x".repeat(2048) }];
      return [
        { type: "user", message: { content: [{ type: "tool_result", tool_use_id: `unrelated-${index}`, content: "prior" }] } },
        { type: "tool_execution_start", toolCallId, toolName: "bash", args: {} },
        { type: "tool_execution_end", toolCallId, toolName: "bash", result: { content }, isError: false },
        { type: "message_end", message: { role: "toolResult", toolCallId, toolName: "bash", content, isError: false } },
      ].map((event) => `${JSON.stringify(event)}\n`).join("");
    }).join("");
    const file = logFile(text);
    const reads = vi.spyOn(fs, "readSync");
    const parses = vi.spyOn(JSON, "parse");
    const clock = vi.spyOn(performance, "now").mockReturnValue(0);
    try {
      expect(compactTerminalRunLog(file, "completed")).toMatchObject({ compacted: count });
      expect(reads.mock.results.reduce((sum, result) => sum + Number(result.value), 0)).toBeLessThanOrEqual(3 * Buffer.byteLength(text));
      expect(parses.mock.calls.length).toBeLessThanOrEqual(3 * count * 4);
      expect(reads.mock.calls.filter((args) => Number(args[4]) === 0)).toHaveLength(2);
      expect(reads.mock.calls.length).toBeLessThanOrEqual(2 * Math.ceil(Buffer.byteLength(text) / (64 * 1024)) + count);
    } finally {
      reads.mockRestore();
      parses.mockRestore();
      clock.mockRestore();
    }
  });

  it("retains full bytes/inode and cleans the temporary file at the elapsed-work bound", () => {
    const text = reviewed.slice(1).join("\n") + "\n";
    const file = logFile(text);
    const inode = fs.statSync(file).ino;
    const writes = vi.spyOn(fs, "writeSync");
    const clock = vi.spyOn(performance, "now").mockImplementation(() => writes.mock.calls.length ? MAX_TERMINAL_LOG_WORK_MS : 0);
    try {
      const outcome = compactTerminalRunLog(file, "completed");
      expect(outcome).toMatchObject({ compacted: 0, beforeBytes: Buffer.byteLength(text), afterBytes: Buffer.byteLength(text), compactionSkipped: expect.stringContaining("MAX_TERMINAL_LOG_WORK_MS") });
      expect(outcome.error).toBeUndefined();
      expect(writes).toHaveBeenCalled();
    } finally {
      writes.mockRestore();
      clock.mockRestore();
    }
    expect(fs.statSync(file).ino).toBe(inode);
    expect(fs.readFileSync(file, "utf8")).toBe(text);
    expect(fs.readdirSync(path.dirname(file))).toEqual(["events.jsonl"]);
  });

  it("retains a unique pair whose canonical details have different key order", () => {
    const end = JSON.parse(reviewed[1]!);
    end.result.details = { exitCode: 0, signal: null };
    const canonical = JSON.parse(reviewed[2]!);
    canonical.message.details = { signal: null, exitCode: 0 };
    expectRetained([end, canonical].map((event) => JSON.stringify(event)).join("\n") + "\n");
  });
});
