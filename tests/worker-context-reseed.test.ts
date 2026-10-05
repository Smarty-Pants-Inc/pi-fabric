import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, expect, it, vi } from "vitest";
import { buildSessionContext, SessionManager } from "@earendil-works/pi-coding-agent";
import { readPiSessionHeader } from "../src/core/pi-session-header.js";
import { reseedActorSession, summarizeActorState } from "../src/worker/context-reseed.js";

const roots: string[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  roots.splice(0).forEach(root => fs.rmSync(root, { recursive: true, force: true }));
});
const setup = (recent = "Recent decision: preserve pending review /tmp/report.md", rootChars = 1_200_000) => {
  const cwd = fs.mkdtempSync(path.join(os.tmpdir(), "actor-reseed-")); roots.push(cwd);
  const file = path.join(cwd, "session.jsonl");
  fs.writeFileSync(file, [
    { type: "session", version: 3, id: "registered-old", timestamp: new Date().toISOString(), cwd },
    { type: "message", id: "giant", parentId: null, timestamp: new Date().toISOString(),
      message: { role: "user", content: "OLD_GOAL " + "x".repeat(rootChars), timestamp: 1 } },
    { type: "message", id: "recent", parentId: "giant", timestamp: new Date().toISOString(),
      message: { role: "user", content: recent, timestamp: 2 } },
  ].map(entry => JSON.stringify(entry)).join("\n") + "\n", { mode: 0o600 });
  return { file, cwd, before: fs.readFileSync(file, "utf8") };
};
const recovery = { summary: "", summaryTokens: 8192, estimate: (text: string) => Math.ceil(text.length / 4),
  tokens: 300_000, contextWindow: 272_000, reason: "compaction unavailable", runId: "reseed-run" };

it("atomically publishes the complete fresh registered header and bounded recent state, retaining full audit bytes", () => {
  const s = setup();
  const rename = fs.renameSync;
  const witness = vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
    expect(to).toBe(s.file);
    expect(fs.readFileSync(s.file, "utf8")).toBe(s.before);
    const candidate = fs.readFileSync(from, "utf8");
    expect(candidate).toContain("fabric-context-reseed");
    expect(candidate).toContain("pending review /tmp/report.md");
    expect(readPiSessionHeader(String(from))?.id).not.toBe("registered-old");
    rename(from, to);
  });
  const result = reseedActorSession(s.file, s.cwd, recovery);
  expect(witness).toHaveBeenCalledOnce();
  expect(result.oldSessionId).toBe("registered-old");
  expect(result.sessionId).not.toBe(result.oldSessionId);
  expect(readPiSessionHeader(s.file)?.id).toBe(result.sessionId);
  expect(fs.readFileSync(result.archived, "utf8")).toBe(s.before);
  const native = SessionManager.open(s.file);
  expect(native.getSessionId()).toBe(result.sessionId);
  const messages = buildSessionContext(native.getBranch()).messages;
  expect(messages).toHaveLength(1);
  expect(JSON.stringify(messages)).toContain("pending review /tmp/report.md");
  expect(recovery.estimate(JSON.stringify(messages))).toBeLessThan(8192);
  const note = native.getBranch().find(entry => entry.type === "custom");
  expect(note).toMatchObject({ customType: "fabric-context-reseed", data: { oldSessionId: "registered-old", sessionId: result.sessionId, tokens: 300_000, contextWindow: 272_000 } });
  if (process.platform !== "win32") expect(fs.statSync(s.file).mode & 0o777).toBe(0o600);
});

it("failed publication leaves the registered session intact, archives audit bytes and cleans the temp file", () => {
  const s = setup();
  vi.spyOn(fs, "renameSync").mockImplementation(() => { throw new Error("rename failed"); });
  expect(() => reseedActorSession(s.file, s.cwd, recovery)).toThrow("rename failed");
  expect(fs.readFileSync(s.file, "utf8")).toBe(s.before);
  expect(fs.readdirSync(s.cwd).some(name => name.endsWith(".tmp"))).toBe(false);
  const backup = fs.readdirSync(s.cwd).find(name => name.endsWith(".context-reseed.bak"))!;
  expect(fs.readFileSync(path.join(s.cwd, backup), "utf8")).toBe(s.before);
});

it("bounds summaries to the selected window budget even with huge messages and no room for state", () => {
  const s = setup();
  reseedActorSession(s.file, s.cwd, { ...recovery, summary: "goal " + "y".repeat(1_200_000), summaryTokens: 256 });
  const messages = buildSessionContext(SessionManager.open(s.file).getBranch()).messages;
  expect(recovery.estimate(JSON.stringify(messages))).toBeLessThan(300);
  const noRoom = setup();
  reseedActorSession(noRoom.file, noRoom.cwd, { ...recovery, summaryTokens: 0 });
  expect(buildSessionContext(SessionManager.open(noRoom.file).getBranch()).messages).toEqual([]);
});

// Exercise Windows contracts on every CI host: writable-only flushing, no
// directory fsync, and closed-candidate atomic replace under sharing contention.
const withWin32 = (action: (handles: Map<number, { file: string; flags: unknown }>, syncedFiles: Set<string>) => void, failAt?: "archive" | "candidate") => {
  const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
  const open = fs.openSync, close = fs.closeSync, sync = fs.fsyncSync;
  const handles = new Map<number, { file: string; flags: unknown }>();
  const syncedFiles = new Set<string>();
  const opened = vi.spyOn(fs, "openSync").mockImplementation((file, flags, mode) => {
    const fd = open(file, flags, mode); handles.set(fd, { file: String(file), flags }); return fd;
  });
  const closed = vi.spyOn(fs, "closeSync").mockImplementation(fd => { close(fd); handles.delete(fd); });
  const synced = vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
    const handle = handles.get(fd)!;
    if (fs.fstatSync(fd).isDirectory() || handle.flags === "r" || handle.flags === fs.constants.O_RDONLY) {
      throw Object.assign(new Error("Windows cannot flush a read-only handle"), { code: "EPERM" });
    }
    if (failAt && handle.file.endsWith(failAt === "archive" ? ".context-reseed.bak" : ".tmp")) {
      throw Object.assign(new Error("disk flush failed"), { code: "EIO" });
    }
    if (handle.file.endsWith(".context-reseed.bak")) expect(handle.flags).toBe("r+");
    sync(fd); syncedFiles.add(handle.file);
  });
  try {
    Object.defineProperty(process, "platform", { ...platform, value: "win32" });
    action(handles, syncedFiles);
    expect(handles.size).toBe(0);
  } finally {
    Object.defineProperty(process, "platform", platform);
    synced.mockRestore(); closed.mockRestore(); opened.mockRestore();
  }
};

it.each(["EPERM", "EACCES", "EBUSY", "EEXIST"])("forced-win32 flushes writable archive and retries %s replacement without losing #493 binding", code => {
  const s = setup();
  const rename = fs.renameSync;
  let attempts = 0;
  withWin32((handles, syncedFiles) => {
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      expect(to).toBe(s.file);
      expect(fs.readFileSync(s.file, "utf8")).toBe(s.before);
      expect([...handles.values()].some(handle => handle.file === String(from))).toBe(false);
      expect(syncedFiles.has(String(from))).toBe(true);
      expect([...syncedFiles].some(file => file.endsWith(".context-reseed.bak"))).toBe(true);
      const candidate = SessionManager.open(String(from));
      expect(candidate.getBranch().find(entry => entry.type === "custom")).toMatchObject({
        data: { oldSessionId: "registered-old", sessionId: candidate.getSessionId(), runId: recovery.runId },
      });
      if (++attempts < 3) throw Object.assign(new Error("sharing contention"), { code });
      rename(from, to);
    });
    const result = reseedActorSession(s.file, s.cwd, recovery);
    expect(attempts).toBe(3);
    expect(readPiSessionHeader(s.file)?.id).toBe(result.sessionId);
    expect(fs.readFileSync(result.archived, "utf8")).toBe(s.before);
    expect(JSON.stringify(buildSessionContext(SessionManager.open(s.file).getBranch()).messages)).toContain("pending review");
    expect(fs.readdirSync(s.cwd).some(name => name.endsWith(".tmp"))).toBe(false);
  });
});

it("forced-win32 exhausts replacement retries with old session intact and complete audit archive", () => {
  const s = setup();
  withWin32(() => {
    const renamed = vi.spyOn(fs, "renameSync").mockImplementation(() => {
      expect(fs.readFileSync(s.file, "utf8")).toBe(s.before);
      throw Object.assign(new Error("permanent sharing violation"), { code: "EPERM" });
    });
    expect(() => reseedActorSession(s.file, s.cwd, recovery)).toThrow("permanent sharing violation");
    expect(renamed).toHaveBeenCalledTimes(8);
    expect(readPiSessionHeader(s.file)?.id).toBe("registered-old");
    expect(fs.readdirSync(s.cwd).some(name => name.endsWith(".tmp"))).toBe(false);
    const backup = fs.readdirSync(s.cwd).find(name => name.endsWith(".context-reseed.bak"))!;
    expect(fs.readFileSync(path.join(s.cwd, backup), "utf8")).toBe(s.before);
  });
});

it.each(["archive", "candidate"])("forced-win32 does not suppress a real %s durability failure", stage => {
  const s = setup();
  withWin32(() => {
    const rename = vi.spyOn(fs, "renameSync");
    expect(() => reseedActorSession(s.file, s.cwd, recovery)).toThrow("disk flush failed");
    expect(rename).not.toHaveBeenCalled();
    expect(fs.readFileSync(s.file, "utf8")).toBe(s.before);
    expect(fs.readdirSync(s.cwd).some(name => name.endsWith(".tmp"))).toBe(false);
  }, stage as "archive" | "candidate");
});

it("forced-win32 bounds huge summaries and can publish without room for historical state", () => {
  withWin32(() => {
    for (const summaryTokens of [256, 0]) {
      const s = setup();
      reseedActorSession(s.file, s.cwd, { ...recovery, summaryTokens, summary: "x".repeat(1_200_000) });
      const messages = buildSessionContext(SessionManager.open(s.file).getBranch()).messages;
      expect(recovery.estimate(JSON.stringify(messages))).toBeLessThan(300);
      if (summaryTokens === 0) expect(messages).toEqual([]);
    }
  });
});

it.each(["omit", "replace", "inactive-branch"])("over-cap fallback preserves native visibility for %s", mode => {
  const s = setup("REMOVED_STATE_SENTINEL", 5_100_000);
  const native = SessionManager.open(s.file);
  if (mode === "omit") native.appendContextEdit("recent", null);
  if (mode === "replace") native.appendContextEdit("recent", { content: "REPLACED_CURRENT_STATE" });
  if (mode === "inactive-branch") native.branch("giant");
  native.appendMessage({ role: "user", content: "CURRENT_PENDING_WORK", timestamp: Date.now() });
  const visible = JSON.stringify(buildSessionContext(native.getBranch()).messages);
  expect(visible.length).toBeGreaterThan(5_000_000);
  expect(visible).not.toContain("REMOVED_STATE_SENTINEL");
  const before = fs.readFileSync(s.file, "utf8");
  const read = fs.readSync;
  const sizes: number[] = [];
  const witness = vi.spyOn(fs, "readSync").mockImplementation((...args: Parameters<typeof fs.readSync>) => {
    sizes.push(args[1].byteLength); return read(...args);
  });
  const result = reseedActorSession(s.file, s.cwd, recovery);
  witness.mockRestore();
  expect(Math.max(...sizes)).toBeLessThanOrEqual(65_536);
  const seeded = JSON.stringify(buildSessionContext(SessionManager.open(s.file).getBranch()).messages);
  expect(seeded).toContain("CURRENT_PENDING_WORK");
  expect(seeded).not.toContain("REMOVED_STATE_SENTINEL");
  if (mode === "replace") expect(seeded).toContain("REPLACED_CURRENT_STATE");
  expect(fs.readFileSync(result.archived, "utf8")).toBe(before);
});

it("fallback respects the latest native compaction and edits on its retained range", () => {
  const s = setup("REMOVED_STATE_SENTINEL");
  const native = SessionManager.open(s.file);
  const oldCompaction = native.appendCompaction("OLD_SUMMARY_SENTINEL", "recent", 300_000);
  const kept = native.appendMessage({ role: "user", content: "KEPT_ORIGINAL_SENTINEL", timestamp: Date.now() });
  native.appendContextEdit(kept, { content: "KEPT_REPLACEMENT" });
  native.appendCompaction("CURRENT_COMPACTION_SUMMARY", oldCompaction, 300_000);
  native.appendMessage({ role: "user", content: "CURRENT_PENDING_WORK", timestamp: Date.now() });
  const visible = JSON.stringify(buildSessionContext(native.getBranch()).messages);
  expect(visible).not.toMatch(/REMOVED_STATE_SENTINEL|OLD_SUMMARY_SENTINEL|KEPT_ORIGINAL_SENTINEL/);
  reseedActorSession(s.file, s.cwd, recovery);
  const seeded = JSON.stringify(buildSessionContext(SessionManager.open(s.file).getBranch()).messages);
  expect(seeded).not.toMatch(/REMOVED_STATE_SENTINEL|OLD_SUMMARY_SENTINEL|KEPT_ORIGINAL_SENTINEL/);
  expect(seeded).toContain("KEPT_REPLACEMENT");
  expect(seeded).toContain("CURRENT_COMPACTION_SUMMARY");
  expect(seeded).toContain("CURRENT_PENDING_WORK");
});

it("fallback excludes uncertain pre-compaction ranges when the retained boundary is outside its bounded suffix", () => {
  const s = setup("UNCERTAIN_RANGE_SENTINEL");
  const native = SessionManager.open(s.file);
  native.appendCompaction("CURRENT_COMPACTION_SUMMARY", "giant", 300_000);
  native.appendMessage({ role: "user", content: "CURRENT_PENDING_WORK", timestamp: Date.now() });
  reseedActorSession(s.file, s.cwd, recovery);
  const seeded = JSON.stringify(buildSessionContext(SessionManager.open(s.file).getBranch()).messages);
  expect(seeded).not.toContain("UNCERTAIN_RANGE_SENTINEL");
  expect(seeded).toContain("CURRENT_COMPACTION_SUMMARY");
  expect(seeded).toContain("CURRENT_PENDING_WORK");
});

it("fallback applies only the latest active edit, never an inactive branch edit", () => {
  const s = setup("VISIBLE_RECENT_STATE");
  const native = SessionManager.open(s.file);
  native.appendContextEdit("recent", null);
  native.branch("recent");
  native.appendContextEdit("recent", { content: "OLD_REPLACEMENT_SENTINEL" });
  native.appendContextEdit("recent", { content: "CURRENT_REPLACEMENT" });
  native.appendMessage({ role: "user", content: "CURRENT_PENDING_WORK", timestamp: Date.now() });
  reseedActorSession(s.file, s.cwd, recovery);
  const seeded = JSON.stringify(buildSessionContext(SessionManager.open(s.file).getBranch()).messages);
  expect(seeded).toContain("CURRENT_REPLACEMENT");
  expect(seeded).toContain("CURRENT_PENDING_WORK");
  expect(seeded).not.toMatch(/VISIBLE_RECENT_STATE|OLD_REPLACEMENT_SENTINEL/);
});

it("summarizes recent visible state without replaying old system instructions, images or hidden reasoning", () => {
  const summary = summarizeActorState([
    ...Array.from({ length: 20 }, () => ({ role: "user", content: "ancient discarded objective" })),
    { role: "system", content: "STALE_SYSTEM" },
    { role: "assistant", content: [{ type: "thinking", thinking: "SECRET_REASONING" }, { type: "text", text: "PENDING_REVIEW" },
      { type: "image", data: "IMAGE_DATA" }, { type: "toolCall", name: "read", arguments: { path: "/tmp/current.md" } }] },
    { role: "user", content: "RECENT_CONSTRAINT " + "x".repeat(1_000_000) + " ARTIFACT_PATH" },
  ]);
  expect(summary).toContain("PENDING_REVIEW"); expect(summary).toContain("/tmp/current.md");
  expect(summary).toContain("RECENT_CONSTRAINT"); expect(summary).toContain("ARTIFACT_PATH");
  expect(summary).not.toMatch(/STALE_SYSTEM|SECRET_REASONING|IMAGE_DATA/);
  expect(summary.length).toBeLessThan(16_500);
});
