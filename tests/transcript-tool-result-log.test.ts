import "./fixtures/conversation-host.js";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { missingToolStartIds, toolLifecycleContext, TranscriptAccumulator } from "../src/ui/transcript-parser.js";
import { NativeConversationReader } from "../src/ui/conversation-native-reader.js";
import { AgentTranscriptReader } from "../src/ui/transcript.js";
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
  it.each(["native", "pi", "pi-durable"])("closes no-canonical/nested calls and upgrades the identical entry with its original parent (%s)", runner => {
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
    transcript.append([runner === "native" ? final : project(final, runner)]);
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

  it("keeps distinct missing-start full execution ends separate and authoritative for a reused ID", () => {
    const transcript = new TranscriptAccumulator();
    const first = {
      type: "tool_execution_end", toolCallId: "reused", toolName: "bash", isError: true,
      result: { content: [{ type: "text", text: "first result" }], details: { exitCode: 1 }, terminate: true },
    };
    const second = {
      ...first, isError: false,
      result: { content: [{ type: "text", text: "second result" }], details: { exitCode: 0 }, terminate: false },
    };
    transcript.append([first, canonical("reused", "must not replace first")]);
    const old = transcript.entries[0];
    transcript.append([second, canonical("reused", "must not replace second", true)]);
    expect(transcript.entries).toHaveLength(2);
    expect(transcript.entries[0]).toBe(old);
    expect(old).toMatchObject({ status: "failed", result: first.result });
    expect(transcript.entries[1]).toMatchObject({ status: "completed", result: second.result });
  });

  it.each([
    { name: "legacy message/toolResult", previous: { type: "message", message: {
      role: "toolResult", toolCallId: "reused", toolName: "bash",
      content: [{ type: "text", text: "prior result" }], isError: false, timestamp: 1,
    } } },
    { name: "Claude user/tool_result", previous: { type: "user", message: {
      role: "user", content: [{ type: "tool_result", tool_use_id: "reused", content: "prior result" }],
    } } },
    { name: "Claude user/tool_result without role", previous: { type: "user", message: {
      content: [{ type: "tool_result", tool_use_id: "reused", content: "prior result" }],
    } } },
  ])("retains the reported missing-start full end after $name", ({ previous }) => {
    const transcript = new TranscriptAccumulator();
    const full = {
      type: "tool_execution_end", toolCallId: "reused", toolName: "bash",
      result: { content: [{ type: "text", text: "new result" }], details: { exitCode: 0 }, terminate: true },
      isError: false,
    };
    const final = { type: "message_end", message: {
      role: "toolResult", toolCallId: "reused", toolName: "bash",
      content: full.result.content, details: full.result.details, isError: false, timestamp: 2,
    } };
    transcript.append([previous]);
    const prior = transcript.entries[0];
    transcript.append([full, final]);
    expect(transcript.entries).toHaveLength(2);
    expect(transcript.entries[0]).toBe(prior);
    expect(prior).toMatchObject({ status: "completed" });
    expect(JSON.stringify(prior?.result)).toContain("prior result");
    expect(transcript.entries[1]).toMatchObject({ status: "completed", result: full.result });
    // A canonical-only page must recover this full end, not the older call's
    // start, including Claude completions whose message has no role field.
    const missing = missingToolStartIds([final]);
    expect(missing).toEqual(new Set(["reused"]));
    expect(toolLifecycleContext([start("reused"), previous, full], missing)).toEqual([full]);
  });

  it.each([true, false])("keeps mixed legacy and repeated missing-start Claude completions distinct (role=%s)", withRole => {
    const transcript = new TranscriptAccumulator();
    const claude = (text: string, isError: boolean) => ({ type: "user", message: {
      ...(withRole ? { role: "user" } : {}),
      content: [{ type: "tool_result", tool_use_id: "reused", content: text, is_error: isError }],
    } });
    transcript.append([{ type: "message", message: {
      role: "toolResult", toolCallId: "reused", toolName: "bash", content: [{ type: "text", text: "legacy" }],
    } }, claude("second", true), claude("third", false)]);
    expect(transcript.entries).toHaveLength(3);
    expect(transcript.entries[0]).toMatchObject({ status: "completed", result: { content: [{ type: "text", text: "legacy" }] } });
    expect(transcript.entries[1]).toMatchObject({ status: "failed", result: "second" });
    expect(transcript.entries[2]).toMatchObject({ status: "completed", result: "third" });
  });

  // F1's recorded RED was the native message_end receiver, not the projected
  // tool_execution_end receiver. Keep both paths explicit in this regression.
  it.each([
    { receiver: "native", withRole: true }, { receiver: "native", withRole: false },
    { receiver: "pi", withRole: true }, { receiver: "pi", withRole: false },
    { receiver: "pi-durable", withRole: true }, { receiver: "pi-durable", withRole: false },
  ])("preserves a payload-less Claude entry before a distinct $receiver completion (role=$withRole)", ({ receiver, withRole }) => {
    const transcript = new TranscriptAccumulator();
    const previous = { type: "user", message: {
      ...(withRole ? { role: "user" } : {}),
      content: [{ type: "tool_result", tool_use_id: "reused", is_error: true }],
    } };
    const final = { type: "message_end", message: {
      role: "toolResult", toolCallId: "reused", toolName: "bash", isError: false,
      content: [{ type: "text", text: "new guarded result" }], timestamp: 2,
    } };
    transcript.append([previous]);
    const prior = transcript.entries[0]!;
    const before = structuredClone(prior);
    const completion = receiver === "native" ? final : project(final, receiver);
    expect(completion.type).toBe(receiver === "native" ? "message_end" : "tool_execution_end");
    transcript.append([completion]);
    expect(transcript.entries).toHaveLength(2);
    expect(transcript.entries[0]).toBe(prior);
    expect(prior).toStrictEqual(before);
    expect(prior).toMatchObject({ status: "failed" });
    expect(prior.result).toBeUndefined();
    const current = transcript.entries[1]!;
    expect(current).not.toBe(prior);
    expect(current).toMatchObject({ status: "completed", result: { content: final.message.content } });
    // Once the new payload exists, true native copies and metadata repeats
    // must neither append again nor downgrade either completed entry.
    transcript.append([final, project(rawEnd("reused", true)), final]);
    expect(transcript.entries).toHaveLength(2);
    expect(transcript.entries[1]).toBe(current);
    expect(current).toMatchObject({ status: "completed", result: { content: final.message.content } });
    expect(prior).toStrictEqual(before);
  });

  it.each([
    { name: "nonempty", content: "prior result" },
    { name: "empty", content: [] },
    { name: "null", content: null },
  ])("keeps a defined $name Claude payload authoritative over native canonical copies", ({ content }) => {
    const transcript = new TranscriptAccumulator();
    transcript.append([{ type: "user", message: {
      content: [{ type: "tool_result", tool_use_id: "reused", content, is_error: true }],
    } }]);
    const prior = transcript.entries[0];
    transcript.append([canonical("reused", "must not replace prior")]);
    expect(transcript.entries).toHaveLength(1);
    expect(transcript.entries[0]).toBe(prior);
    expect(prior).toMatchObject({ status: "failed", result: content });
  });

  it("tracks the latest result-less completion without weakening metadata or compact-marker repeat guards", () => {
    const transcript = new TranscriptAccumulator();
    transcript.append([rawEnd("reused"), { type: "user", message: {
      content: [{ type: "tool_result", tool_use_id: "reused", is_error: true }],
    } }]);
    const [full, prior] = transcript.entries;
    const before = structuredClone(prior);
    transcript.append([
      project(rawEnd("reused")),
      { type: "tool_execution_end", toolCallId: "reused", result: { elided: true, bytes: 20 }, resultMetadata: { terminate: true } },
    ]);
    expect(transcript.entries).toHaveLength(2);
    transcript.append([{ type: "message_end", message: {
      role: "toolResult", toolCallId: "reused", toolName: "bash", content: [{ type: "text", text: "new result" }],
    } }]);
    expect(transcript.entries).toHaveLength(3);
    expect(transcript.entries[0]).toBe(full);
    expect(transcript.entries[1]).toBe(prior);
    expect(prior).toStrictEqual(before);
    expect(prior?.result).toBeUndefined();
    expect(transcript.entries[2]?.result).toEqual({ content: [{ type: "text", text: "new result" }] });
  });

  it.each(["native", "pi", "pi-durable"])("preserves F1 through AgentTranscriptReader.read (%s)", receiver => {
    const logFile = file();
    const final = { type: "message_end", message: {
      role: "toolResult", toolCallId: "reused", toolName: "bash", content: [{ type: "text", text: "new result" }],
    } };
    fs.writeFileSync(logFile, jsonl([
      { type: "user", message: { content: [{ type: "tool_result", tool_use_id: "reused" }] } },
      receiver === "native" ? final : project(final, receiver),
    ]));
    const result = new AgentTranscriptReader().read({ id: "f1", status: "completed", logFile });
    expect(result.entries).toHaveLength(2);
    expect(result.entries[0]?.result).toBeUndefined();
    expect(result.entries[1]).toMatchObject({ status: "completed", result: { content: final.message.content } });
    expect(JSON.stringify(result)).not.toContain(FAKE);
  });

  it.each([true, false])("does not borrow an older start or parent across a payload-less Claude bootstrap boundary (role=%s)", withRole => {
    const logFile = file();
    const previous = { type: "user", message: {
      ...(withRole ? { role: "user" } : {}),
      content: [{ type: "tool_result", tool_use_id: "fabric_reused" }],
    } };
    const final = { type: "message_end", message: {
      role: "toolResult", toolCallId: "fabric_reused", toolName: "bash", content: [{ type: "text", text: "new result" }],
    } };
    const history = [start("old-parent", "fabric_exec"), start("fabric_reused"), previous];
    expect(toolLifecycleContext(history, missingToolStartIds([final]))).toEqual([]);
    // Push the old completion outside the reader's 40-record visible page.
    fs.writeFileSync(logFile, jsonl([...history,
      ...Array.from({ length: 40 }, (_, index) => ({ type: "message", message: { role: "user", content: `padding ${index}` } })), final,
    ]));
    const result = new AgentTranscriptReader().read({ id: "f1-page", status: "completed", logFile });
    const tools = result.entries.filter(entry => entry.kind === "tool");
    expect(result.hasMore).toBe(true);
    expect(tools).toHaveLength(1);
    expect(tools[0]).toMatchObject({ status: "completed", result: { content: final.message.content } });
    expect(tools[0]?.args).toBeUndefined();
    expect(tools[0]?.parentId).toBeUndefined();
    expect(tools[0]?.depth).toBeUndefined();
  });

  it.each([false, true])("keeps a unique missing-start full-end/canonical pair authoritative (compact=%s)", compact => {
    const transcript = new TranscriptAccumulator();
    const content = [{ type: "text", text: "unique result" }];
    const payload = { content, details: { exitCode: 0 }, terminate: true };
    const final = { type: "message_end", message: {
      role: "toolResult", toolCallId: "unique", toolName: "bash", content, details: payload.details,
    } };
    transcript.append([
      { type: "tool_execution_end", toolCallId: "unique", toolName: "bash", result: compact ? { elided: true, bytes: 80 } : payload,
        ...(compact ? { resultMetadata: { terminate: true } } : {}) },
      final, final,
    ]);
    expect(transcript.entries).toHaveLength(1);
    expect(transcript.entries[0]).toMatchObject({ status: "completed", result: payload });
  });

  it.each(["native", "pi", "pi-durable"])("still upgrades the identical missing-start metadata-only entry (%s)", runner => {
    const transcript = new TranscriptAccumulator();
    transcript.append([project(rawEnd("missing", true), runner)]);
    const entry = transcript.entries[0];
    expect(entry).toMatchObject({ id: "missing", status: "failed" });
    expect(entry?.result).toBeUndefined();
    const final = canonical("missing", "guarded missing-start result", true);
    transcript.append([runner === "native" ? final : project(final, runner)]);
    expect(transcript.entries).toHaveLength(1);
    expect(transcript.entries[0]).toBe(entry);
    expect(entry).toMatchObject({ status: "failed", result: { content: [{ type: "text", text: "guarded missing-start result" }] } });
    expect(JSON.stringify(transcript.snapshot())).not.toContain(FAKE);
  });

  it("does not upgrade an older awaiting metadata-only entry after a reused explicit start", () => {
    const transcript = new TranscriptAccumulator();
    transcript.append([start("same"), project(rawEnd("same", true))]);
    const old = transcript.entries[0];
    transcript.append([start("same", "fake_tool", { generation: 2 }), canonical("same", "new lifecycle")]);
    expect(transcript.entries).toHaveLength(2);
    expect(transcript.entries[0]).toBe(old);
    expect(old).toMatchObject({ status: "failed", args: { generation: 1 } });
    expect(old?.result).toBeUndefined();
    expect(transcript.entries[1]).toMatchObject({ status: "completed", args: { generation: 2 }, result: { content: [{ type: "text", text: "new lifecycle" }], details: { secret: "[redacted]" } } });
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
