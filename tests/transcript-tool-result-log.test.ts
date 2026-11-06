import "./fixtures/conversation-host.js";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { TranscriptAccumulator } from "../src/ui/transcript-parser.js";
import { NativeConversationReader } from "../src/ui/conversation-native-reader.js";
import { projectToolResultLogEvent } from "../src/worker/tool-result-log.js";

// Keep reason: RLG-1 data-safety/lifecycle regression through the real public
// readers. Completion must not require persisting an unguarded result.
const FAKE = "FAKE-RAW-RESULT-not-for-persistence";
const start = (toolCallId: string, toolName = "fake_tool", args = { generation: 1 }) => ({
  type: "tool_execution_start", toolCallId, toolName, args,
});
const rawEnd = (toolCallId: string, isError = false) => ({
  type: "tool_execution_end", toolCallId, toolName: "fake_tool", isError, durationMs: 3,
  result: { content: [{ type: "text", text: FAKE }], details: { secret: FAKE } },
  details: FAKE, structuredContent: FAKE, opaque: FAKE,
});
const canonical = (toolCallId: string, text: string, isError = false) => ({
  type: "message_end", message: {
    role: "toolResult", toolCallId, toolName: "fake_tool", isError,
    content: [{ type: "text", text }], details: { secret: FAKE },
  },
});
const project = (event: Record<string, unknown>, runner = "pi"): Record<string, unknown> => {
  const projected = projectToolResultLogEvent(event, runner);
  expect(projected).toBeDefined();
  return projected!;
};
const directories: string[] = [];
afterEach(() => {
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true });
});
const file = () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "transcript-result-log-"));
  directories.push(directory);
  return path.join(directory, "events.jsonl");
};
const jsonl = (events: Record<string, unknown>[]) => events.map(event => `${JSON.stringify(event)}\n`).join("");

describe("metadata-only completion and guarded upgrades", () => {
  it.each(["pi", "pi-durable"])("closes no-canonical/nested calls and upgrades the identical entry with its original parent (%s)", runner => {
    const transcript = new TranscriptAccumulator();
    transcript.append([start("outer", "fabric_exec"), start("fabric_child")]);
    const child = transcript.entries[1]!;
    const raw = rawEnd("fabric_child", true);
    const before = structuredClone(raw);
    const metadata = project(raw, runner);
    expect(JSON.stringify(metadata)).not.toContain(FAKE);
    transcript.append([metadata]);
    expect(child).toMatchObject({ id: "fabric_child", status: "failed", parentId: "outer", depth: 1 });
    expect(child.result).toBeUndefined();
    // A different active parent at final-message time must not steal the child.
    transcript.append([start("other-parent", "fabric_exec")]);
    const final = canonical("fabric_child", "[guarded result]\n  exact whitespace  ", true);
    transcript.append([project(final, runner)]);
    expect(transcript.entries).toHaveLength(3);
    expect(transcript.entries[1]).toBe(child);
    expect(child).toMatchObject({ status: "failed", parentId: "outer", depth: 1, result: { content: final.message.content } });
    transcript.append([project(rawEnd("outer"), runner), project(rawEnd("other-parent"), runner)]);
    expect(transcript.entries.every(entry => entry.status !== "running")).toBe(true);
    expect(raw).toStrictEqual(before);
    expect(JSON.stringify(transcript.snapshot())).not.toContain(FAKE);
  });

  it("keeps interleaved IDs separate and never loses a final result to a repeated metadata end", () => {
    const transcript = new TranscriptAccumulator();
    transcript.append([start("a"), start("b"), project(rawEnd("b", true)), project(rawEnd("a"))]);
    const [a, b] = transcript.entries;
    transcript.append([project(canonical("a", "guarded A")), project(canonical("b", "guarded B", true))]);
    transcript.append([project(rawEnd("a", true)), project(rawEnd("b"))]);
    expect(transcript.entries).toHaveLength(2);
    expect(transcript.entries[0]).toBe(a);
    expect(transcript.entries[1]).toBe(b);
    expect(a).toMatchObject({ id: "a", status: "completed", result: { content: [{ type: "text", text: "guarded A" }] } });
    expect(b).toMatchObject({ id: "b", status: "failed", result: { content: [{ type: "text", text: "guarded B" }] } });
  });

  it("treats a reused ID plus a new start as a new lifecycle, not an upgrade of the old entry", () => {
    const transcript = new TranscriptAccumulator();
    transcript.append([start("same"), project(rawEnd("same", true)), project(canonical("same", "first", true))]);
    const old = transcript.entries[0]!;
    transcript.append([start("same", "fake_tool", { generation: 2 })]);
    const current = transcript.entries[1]!;
    expect(current).not.toBe(old);
    expect(current).toMatchObject({ status: "running", args: { generation: 2 } });
    transcript.append([project(rawEnd("same")), project(canonical("same", "second"))]);
    expect(transcript.entries).toHaveLength(2);
    expect(old).toMatchObject({ status: "failed", result: { content: [{ type: "text", text: "first" }] } });
    expect(current).toMatchObject({ status: "completed", result: { content: [{ type: "text", text: "second" }] } });
    // A third lifecycle can have no canonical result at all.
    transcript.append([start("same"), project(rawEnd("same"))]);
    expect(transcript.entries).toHaveLength(3);
    expect(transcript.entries[2]).toMatchObject({ status: "completed" });
    expect(transcript.entries[2]!.result).toBeUndefined();
  });

  it("also accepts an unprojected canonical message after metadata completion without changing legacy full-end authority", () => {
    const transcript = new TranscriptAccumulator();
    transcript.append([start("a"), project(rawEnd("a"))]);
    const entry = transcript.entries[0];
    transcript.append([{ type: "message_end", message: {
      role: "toolResult", toolCallId: "a", toolName: "fake_tool", content: [{ type: "text", text: "canonical" }],
    } }]);
    expect(transcript.entries).toHaveLength(1);
    expect(transcript.entries[0]).toBe(entry);
    expect(entry?.result).toEqual({ content: [{ type: "text", text: "canonical" }] });
    transcript.append([{ type: "message_end", message: {
      role: "toolResult", toolCallId: "a", toolName: "fake_tool", content: [{ type: "text", text: "duplicate" }],
    } }]);
    expect(entry?.result).toEqual({ content: [{ type: "text", text: "canonical" }] });
  });

  it.each(["pi", "pi-durable"])("closes and upgrades through NativeConversationReader.read with immutable snapshots (%s)", runner => {
    const eventsFile = file();
    const source = { id: "real-reader", status: "running", eventsFile };
    const reader = new NativeConversationReader();
    const events = [start("outer", "fabric_exec"), start("fabric_child"), start("b"),
      project(rawEnd("fabric_child", true), runner), project(rawEnd("b"), runner), project(rawEnd("outer"), runner)];
    fs.writeFileSync(eventsFile, jsonl(events));
    const completed = reader.read(source);
    expect(completed.streaming.active).toBe(false);
    expect(completed.streaming.tools).toHaveLength(3);
    expect(completed.streaming.tools.find(tool => tool.toolCallId === "fabric_child")).toMatchObject({ status: "failed", isError: true });
    expect(completed.streaming.tools.every(tool => tool.result === undefined)).toBe(true);
    const before = structuredClone(completed);
    const a = canonical("fabric_child", "guarded child", true);
    const b = canonical("b", "guarded B");
    fs.appendFileSync(eventsFile, jsonl([project(b, runner), project(a, runner)]));
    const upgraded = reader.read(source);
    expect(upgraded.streaming.tools).toHaveLength(3);
    expect(upgraded.streaming.tools.find(tool => tool.toolCallId === "fabric_child")).toMatchObject({ status: "failed", result: { content: a.message.content } });
    expect(upgraded.streaming.tools.find(tool => tool.toolCallId === "b")).toMatchObject({ status: "completed", result: { content: b.message.content } });
    expect(completed).toStrictEqual(before);
    fs.appendFileSync(eventsFile, jsonl([start("b", "fake_tool", { generation: 2 })]));
    const restarted = reader.read(source);
    expect(restarted.streaming.tools).toHaveLength(3);
    expect(restarted.streaming.tools.find(tool => tool.toolCallId === "b")).toMatchObject({ status: "running", args: { generation: 2 } });
    expect(restarted.streaming.tools.find(tool => tool.toolCallId === "b")?.result).toBeUndefined();
    fs.appendFileSync(eventsFile, jsonl([project(rawEnd("b", true), runner), project(canonical("b", "second B", true), runner)]));
    const final = reader.read(source);
    expect(final.streaming.active).toBe(false);
    expect(final.streaming.tools).toHaveLength(3);
    expect(final.streaming.tools.find(tool => tool.toolCallId === "b")).toMatchObject({ status: "failed", result: { content: [{ type: "text", text: "second B" }] } });
    expect(fs.readFileSync(eventsFile, "utf8")).not.toContain(FAKE);
  });
});
