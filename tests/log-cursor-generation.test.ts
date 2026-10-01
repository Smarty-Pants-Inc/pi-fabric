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

  describe.each(["quiescent", "Windows EPERM denial", "Windows EBUSY denial", "POSIX held-FD replacement", "Windows native denial"])("terminal compaction: %s", (capability) => {
    it.skipIf((capability.startsWith("POSIX") && process.platform === "win32") || (capability === "Windows native denial" && process.platform !== "win32"))("holds an agents.log byte cursor across actual terminal compaction", () => {
      const content = [{ type: "text", text: "complete durable result ".repeat(100) }];
      const events = [
        { type: "tool_execution_start", toolCallId: "held", toolName: "bash", args: {} },
        { type: "tool_execution_end", toolCallId: "held", toolName: "bash", result: { content }, isError: false },
        { type: "message_end", message: { role: "toolResult", toolCallId: "held", toolName: "bash", content, isError: false } },
        { type: "agent_end" },
      ];
      const original = events.map((event) => JSON.stringify(event)).join("\n") + "\n";
      const file = logFile(original);
      let held = capability === "quiescent" ? undefined : fs.openSync(file, "r");
      try {
        const latest = readJsonlPage(file, 2);
        expect(boundPage(file, latest).lines.map((line) => (line.parsed as { type: string }).type)).toEqual(["tool_execution_start", "tool_execution_end"]);
        if (capability.startsWith("Windows")) {
          const code = capability === "Windows native denial" ? undefined : capability.includes("EPERM") ? "EPERM" : "EBUSY";
          const rename = code === undefined ? undefined : vi.spyOn(fs, "renameSync").mockImplementation((_from, to) => {
            expect(to).toBe(file);
            const stat = fs.fstatSync(held!, { bigint: true });
            expect(`${stat.dev}:${stat.ino}`).toBe(latest.generation);
            throw Object.assign(new Error(`injected Windows ${code}: open destination`), { code });
          });
          // Bound retries without sleeping; the production 500ms bound is unchanged.
          const sleep = code === undefined ? undefined : vi.spyOn(Atomics, "wait").mockReturnValue("timed-out");
          try {
            const outcome = compactTerminalRunLog(file, "completed");
            expect(outcome).toMatchObject({ compacted: 0,
              beforeBytes: Buffer.byteLength(original), afterBytes: Buffer.byteLength(original) });
            if (code !== undefined) {
              expect(outcome.error).toContain(code);
              expect(rename).toHaveBeenCalledTimes(8);
            } else {
              expect(outcome.error ?? outcome.compactionSkipped).toMatch(/EPERM|EBUSY|EACCES|MAX_TERMINAL_LOG_WORK_MS=.*full log retained/);
            }
          } finally { rename?.mockRestore(); sleep?.mockRestore(); }
          expect(fs.readFileSync(file, "utf8")).toBe(original);
          expect(fs.readdirSync(path.dirname(file))).toEqual(["events.jsonl"]);
          expect(indices(boundPage(file, latest))).toEqual(indices(readJsonlPageFromDescriptor(held!, 2, latest.before)));
          fs.closeSync(held!);
          held = undefined;
        }
        const outcome = compactTerminalRunLog(file, "completed");
        expect(outcome).toMatchObject({ compacted: 1 });
        expect(outcome.error).toBeUndefined();
        expect(outcome.compactionSkipped).toBeUndefined();
        expect(() => boundPage(file, latest)).toThrowError(expect.objectContaining({ name: "cursor-stale" }));
        if (held !== undefined) {
          const oldPage = readJsonlPageFromDescriptor(held, 10, undefined, undefined, undefined, latest.generation);
          expect(oldPage.lines.map((line) => line.raw).join("\n") + "\n").toBe(original);
        }
        expect(readJsonlPage(file, 10).generation).not.toBe(latest.generation);
        // A bare legacy offset cannot identify the replaced inode. It remains accepted,
        // but its response carries only the current generation, not a safety claim.
        expect(readJsonlPage(file, 2, latest.before).generation).not.toBe(latest.generation);
      } finally { if (held !== undefined) fs.closeSync(held); }
    });
  });

  it.skipIf(process.platform === "win32")("POSIX held-target rename: samples identity and reads the same opened FD even if the path changes during fstat", () => {
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

  it.each(["EPERM", "EBUSY"])("reads the admitted FD after Windows %s denies path replacement during fstat", (code) => {
    const file = logFile(records());
    const latest = readJsonlPage(file, 2);
    const replacement = `${file}.owned-replacement`;
    fs.writeFileSync(replacement, records(10));
    const nativeFstat = fs.fstatSync;
    let admitted: number | undefined;
    const rename = vi.spyOn(fs, "renameSync").mockImplementation(() => {
      expect(admitted).toBeDefined();
      expect(nativeFstat(admitted!, { bigint: true }).ino).toBe(fs.statSync(file, { bigint: true }).ino);
      throw Object.assign(new Error(`injected Windows ${code}: open destination`), { code });
    });
    const stat = vi.spyOn(fs, "fstatSync").mockImplementationOnce(((...args: Parameters<typeof fs.fstatSync>) => {
      admitted = args[0];
      const sampled = nativeFstat(...args);
      // Catch only the injected path mutation, not a production read failure.
      expect(() => fs.renameSync(replacement, file)).toThrowError(expect.objectContaining({ code }));
      return sampled;
    }) as typeof fs.fstatSync);
    const close = vi.spyOn(fs, "closeSync");
    try {
      const page = boundPage(file, latest);
      expect(indices(page)).toEqual([2, 3]);
      expect(page.generation).toBe(latest.generation);
      expect(rename).toHaveBeenCalledTimes(1);
      expect(close).toHaveBeenCalledWith(admitted);
      expect(fs.readFileSync(file, "utf8")).toBe(records());
    } finally { rename.mockRestore(); stat.mockRestore(); close.mockRestore(); }
    // Once the admitted handle is closed, native replacement MUST work.
    fs.renameSync(replacement, file);
    expect(() => boundPage(file, latest)).toThrowError(expect.objectContaining({ name: "cursor-stale" }));
  });

  it("samples a real archived FD while the unheld current path is rebound during fstat", () => {
    const file = logFile(records());
    const latest = readJsonlPage(file, 2);
    const archive = `${file}.archive`;
    // Move BEFORE admission: Windows need not rename any open destination.
    fs.renameSync(file, archive);
    fs.writeFileSync(file, records());
    const replacement = `${file}.owned-replacement`;
    fs.writeFileSync(replacement, records(10));
    const nativeOpen = fs.openSync;
    const nativeFstat = fs.fstatSync;
    let admitted: number | undefined;
    const open = vi.spyOn(fs, "openSync").mockImplementationOnce((name, flags, mode) => {
      expect(name).toBe(file);
      admitted = nativeOpen(archive, flags, mode);
      return admitted;
    });
    const stat = vi.spyOn(fs, "fstatSync").mockImplementationOnce(((...args: Parameters<typeof fs.fstatSync>) => {
      expect(args[0]).toBe(admitted);
      const sampled = nativeFstat(...args);
      fs.renameSync(replacement, file);
      return sampled;
    }) as typeof fs.fstatSync);
    const read = vi.spyOn(fs, "readSync");
    const close = vi.spyOn(fs, "closeSync");
    try {
      const page = boundPage(file, latest);
      expect(indices(page)).toEqual([2, 3]);
      expect(page.generation).toBe(latest.generation);
      expect(read).toHaveBeenCalled();
      expect(read.mock.calls.every(([fd]) => fd === admitted)).toBe(true);
      expect(close).toHaveBeenCalledWith(admitted);
    } finally { open.mockRestore(); stat.mockRestore(); read.mockRestore(); close.mockRestore(); }
    expect(fs.readFileSync(file, "utf8")).toBe(records(10));
    expect(() => boundPage(file, latest)).toThrowError(expect.objectContaining({ name: "cursor-stale" }));
    expect(indices(readJsonlPage(file, 2))).toEqual([8, 9]);
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
