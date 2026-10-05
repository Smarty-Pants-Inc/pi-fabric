import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { stalledSessionResumeError } from "../src/worker/stall-session.js";

const roots: string[] = [];
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
const header = { type: "session", version: 3, id: "native", timestamp: new Date().toISOString(), cwd: process.cwd() };
const user = { type: "message", id: "user", parentId: null, message: { role: "user", content: "original task" } };
const assistant = { type: "message", id: "abort", parentId: "user", message: { role: "assistant", provider: "fixture", model: "offline", stopReason: "aborted", content: [] } };
const check = async (entries: unknown[], suffix = "", sessionId = "native") => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-stall-session-")); roots.push(root);
  const file = path.join(root, "session.jsonl");
  fs.writeFileSync(file, entries.map(entry => JSON.stringify(entry)).join("\n") + "\n" + suffix);
  return stalledSessionResumeError(file, process.cwd(), "fixture/offline", sessionId);
};
describe("post-drain stalled native session admission", () => {
  it("accepts preserved context with the finalized admitted abort", async () => {
    expect(await check([header, user, assistant])).toBeUndefined();
  });
  it("round-trips Unicode separators in user, completed tool and aborted assistant history", async () => {
    const text = "left\u2028middle\u2029right";
    const tool = { type: "message", id: "tool", parentId: "user", message: { role: "toolResult", content: [{ type: "text", text }] } };
    expect(await check([header, { ...user, message: { ...user.message, content: text.repeat(10_000) } }, tool,
      { ...assistant, parentId: "tool", message: { ...assistant.message, content: [{ type: "text", text }] } }])).toBeUndefined();
  });
  it("accepts LF-only framing with CRLF and a final record without a newline", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-stall-session-")); roots.push(root);
    const file = path.join(root, "session.jsonl");
    fs.writeFileSync(file, [header, user, assistant].map(entry => JSON.stringify(entry)).join("\r\n"));
    expect(await stalledSessionResumeError(file, process.cwd(), "fixture/offline", "native")).toBeUndefined();
  });
  it.each([
    ["header only", [header]],
    ["no user", [header, { ...assistant, parentId: null }]],
    ["wrong assistant model", [header, user, { ...assistant, message: { ...assistant.message, model: "other" } }]],
    ["not finalized abort", [header, user, { ...assistant, message: { ...assistant.message, stopReason: "toolUse" } }]],
    ["broken chain", [header, user, { ...assistant, parentId: "missing" }]],
    ["duplicate entry", [header, user, assistant, assistant]],
    ["invalid header", [{ ...header, version: 999 }, user, assistant]],
    ["foreign cwd", [{ ...header, cwd: path.join(process.cwd(), "other") }, user, assistant]],
  ])("rejects %s rather than starting a new task", async (_name, entries) => {
    expect(await check(entries as unknown[])).toEqual(expect.any(String));
  });
  it("rejects truncated JSON and mismatched native identity", async () => {
    expect(await check([header, user, assistant], '{"type":')).toContain("invalid");
    expect(await check([header, user, assistant], "", "other")).toContain("mismatched");
  });
  it("requires context on the active branch, not an abandoned branch", async () => {
    expect(await check([header, user, assistant, { ...assistant, id: "other", parentId: null }])).toEqual(expect.any(String));
  });
  it("rejects missing files", async () => {
    expect(await stalledSessionResumeError(path.join(os.tmpdir(), "missing-fabric-stall-session", "session.jsonl"), process.cwd(), "fixture/offline")).toContain("unavailable");
  });
});
