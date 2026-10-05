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
const setup = (recent = "Recent decision: preserve pending review /tmp/report.md") => {
  const cwd = fs.mkdtempSync(path.join(os.tmpdir(), "actor-reseed-")); roots.push(cwd);
  const file = path.join(cwd, "session.jsonl");
  fs.writeFileSync(file, [
    { type: "session", version: 3, id: "registered-old", timestamp: new Date().toISOString(), cwd },
    { type: "message", id: "giant", parentId: null, timestamp: new Date().toISOString(),
      message: { role: "user", content: "OLD_GOAL " + "x".repeat(1_200_000), timestamp: 1 } },
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
