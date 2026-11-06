import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";

// Keep: the real startup artifact guard caught the prior 54-file ownership
// regression. These boundary counterexamples protect the mechanical ownership
// fix's public module bindings, JSONL/cache identity, and mock interception.
const directories: string[] = [];
const log = (): string => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "transcript-owner-"));
  directories.push(directory);
  return path.join(directory, "agent.jsonl");
};
const user = (id: string, content: string): Record<string, unknown> => ({
  type: "message", id, message: { role: "user", content },
});

afterEach(() => {
  vi.restoreAllMocks();
  vi.doUnmock("../src/ui/transcript-parser.js");
  vi.doUnmock("../src/ui/transcript-reader.js");
  vi.resetModules();
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true });
});

describe("transcript owner compatibility", () => {
  it("keeps the public reader constructor identical and imports without filesystem work", async () => {
    vi.resetModules();
    const open = vi.spyOn(fs, "openSync");
    const stat = vi.spyOn(fs, "fstatSync");
    const read = vi.spyOn(fs, "readSync");
    const reader = await import("../src/ui/transcript-reader.js");
    const transcript = await import("../src/ui/transcript.js");
    expect(transcript.AgentTranscriptReader).toBe(reader.AgentTranscriptReader);
    expect(open).not.toHaveBeenCalled();
    expect(stat).not.toHaveBeenCalled();
    expect(read).not.toHaveBeenCalled();
    const instance = new reader.AgentTranscriptReader();
    expect(instance.read({ id: "missing", status: "running" }).entries).toEqual([]);
    expect(open).not.toHaveBeenCalled();
  });

  it("keeps the public parser binding interceptable by both projection and reader", async () => {
    vi.resetModules();
    const snapshots = vi.fn();
    vi.doMock("../src/ui/transcript-parser.js", async (importOriginal) => {
      const original = await importOriginal<typeof import("../src/ui/transcript-parser.js")>();
      return {
        ...original,
        TranscriptAccumulator: class extends original.TranscriptAccumulator {
          override snapshot(...args: Parameters<InstanceType<typeof original.TranscriptAccumulator>["snapshot"]>) {
            snapshots();
            return super.snapshot(...args);
          }
        },
      };
    });
    const { projectAgentTranscript, AgentTranscriptReader } = await import("../src/ui/transcript.js");
    expect(projectAgentTranscript([user("projected", "projected text")]).entries[0]?.text).toBe("projected text");
    expect(snapshots).toHaveBeenCalledTimes(1);
    const file = log();
    fs.writeFileSync(file, `${JSON.stringify(user("read", "read text"))}\n`);
    expect(new AgentTranscriptReader().read({ id: "a", status: "running", logFile: file }).entries[0]?.text).toBe("read text");
    expect(snapshots).toHaveBeenCalledTimes(2);
  });

  it("keeps transcript's reader re-export bound to the public reader module", async () => {
    vi.resetModules();
    class MockReader {}
    vi.doMock("../src/ui/transcript-reader.js", () => ({ AgentTranscriptReader: MockReader }));
    const { AgentTranscriptReader } = await import("../src/ui/transcript.js");
    expect(AgentTranscriptReader).toBe(MockReader);
  });

  it("reads complete JSONL only, retains cache identity, and replaces a rotated file", async () => {
    const { AgentTranscriptReader, projectAgentTranscript } = await import("../src/ui/transcript.js");
    const { TranscriptAccumulator, parsedEvents, parseRaw } = await import("../src/ui/transcript-parser.js");
    const file = log();
    const direct = "Fabric actor message from direct:" + JSON.stringify({ source: "direct", payload: { message: "actor text" } });
    const events = [
      user("actor", direct),
      { type: "tool_execution_start", toolCallId: "t", toolName: "read", args: { authorization: "secret", path: "a.ts" } },
      { type: "tool_execution_end", toolCallId: "t", toolName: "read", result: { content: "done" } },
    ];
    const raw = events.map((event) => JSON.stringify(event));
    const pending = JSON.stringify(user("pending", "next text"));
    fs.writeFileSync(file, `${raw.join("\r\n")}\r\nnot-json\n${pending}`);
    const source = { id: "a", status: "running", logFile: file };
    const reader = new AgentTranscriptReader();
    const first = reader.read(source);
    expect(first.entries.map((entry) => entry.id)).toEqual(["actor", "t"]);
    expect(first.entries[0]?.text).toBe("actor text");
    expect(first.entries[1]?.args).toEqual({ authorization: "[redacted]", path: "a.ts" });
    expect(reader.read({ ...source, id: "same-path" })).toBe(first);
    const parsed = parsedEvents(raw.map((text, offset) => ({ raw: text, offset })));
    const accumulator = new TranscriptAccumulator();
    accumulator.append(parsed);
    expect(first.entries).toEqual(accumulator.snapshot().entries);
    expect(first.entries).toEqual(projectAgentTranscript(events).entries);
    expect(parseRaw("[]")).toBeUndefined();
    expect(parseRaw("not-json")).toBeUndefined();
    fs.appendFileSync(file, "\n");
    expect(reader.read(source).entries.at(-1)?.text).toBe("next text");
    fs.renameSync(file, `${file}.old`);
    fs.writeFileSync(file, `${JSON.stringify(user("replacement", "new inode"))}\n`);
    const replaced = reader.read(source);
    expect(replaced.entries.map((entry) => entry.id)).toEqual(["replacement"]);
    expect(replaced).not.toBe(first);
    reader.clear();
    expect(reader.read(source)).not.toBe(replaced);
  });
});
