import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { SessionManager, type ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentCompletionInbox, AGENT_COMPLETION_MESSAGE_TYPE } from "../src/agents/completion-inbox.js";
import type { AgentRunResult } from "../src/agents/types.js";

const roots: string[] = []; const inboxes: AgentCompletionInbox[] = [];
afterEach(() => { for (const inbox of inboxes.splice(0)) inbox.close(); vi.restoreAllMocks(); for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "inbox-durable-r3-")); roots.push(root);
  const manager = SessionManager.create(root, path.join(root, "sessions"));
  const handlers = new Map<string, (event: any, ctx: ExtensionContext) => unknown>(); const sendMessage = vi.fn();
  const ctx = { hasUI: false, isIdle: () => false, hasPendingMessages: () => false, sessionManager: manager } as unknown as ExtensionContext;
  const inbox = new AgentCompletionInbox({ on: (name: string, fn: any) => handlers.set(name, fn), sendMessage } as any, ctx); inboxes.push(inbox);
  const delivered = vi.fn();
  inbox.enqueue({ id: "a", name: "worker", status: "completed", text: "private outcome", startedAt: 1, finishedAt: 2 } as AgentRunResult, delivered);
  handlers.get("turn_end")!({ message: { role: "assistant", stopReason: "stop" } }, ctx);
  const carrier = sendMessage.mock.calls[0]![0];
  const confirm = () => handlers.get("context")!({}, ctx);
  const append = () => manager.appendCustomMessageEntry(carrier.customType, carrier.content, carrier.display, carrier.details);
  const assistant = () => manager.appendMessage({ role: "assistant", content: [{ type: "text", text: "persist" }], api: "openai-completions", provider: "fixture", model: "fixture", usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } }, stopReason: "stop", timestamp: Date.now() });
  return { root, manager, delivered, confirm, append, assistant, sendMessage };
};
describe("Astra 1: persisted completion carrier barrier", () => {
  it("does not receipt a fresh Pi carrier that exists only in getEntries", () => {
    const h = fixture(); h.append();
    expect(h.manager.getEntries().some(e => e.type === "custom_message" && e.customType === AGENT_COMPLETION_MESSAGE_TYPE)).toBe(true);
    expect(fs.existsSync(h.manager.getSessionFile()!)).toBe(false);
    h.confirm(); h.confirm(); expect(h.delivered).not.toHaveBeenCalled();
    h.assistant(); h.confirm(); h.confirm(); expect(h.delivered).toHaveBeenCalledOnce();
    expect(h.sendMessage).toHaveBeenCalledOnce();
  });
  it("does not receipt an entry left in memory after an append failed", () => {
    const h = fixture(); h.assistant(); const file = h.manager.getSessionFile()!;
    fs.rmSync(file); fs.mkdirSync(file);
    expect(h.append).toThrow(); expect(h.manager.getEntries().some(e => e.type === "custom_message")).toBe(true);
    h.confirm(); h.confirm(); expect(h.delivered).not.toHaveBeenCalled();
  });
  it("retains the outcome when a required durability barrier fails; retries without another carrier", () => {
    const h = fixture(); h.assistant(); h.append();
    const sync = vi.spyOn(fs, "fsyncSync").mockImplementation(() => { throw new Error("sync failed"); });
    h.confirm(); h.confirm(); expect(h.delivered).not.toHaveBeenCalled();
    sync.mockRestore(); h.confirm(); expect(h.delivered).toHaveBeenCalledOnce(); expect(h.sendMessage).toHaveBeenCalledOnce();
  });
  it.skipIf(process.platform === "win32")("requires the containing-directory barriers too", () => {
    const h = fixture(); h.assistant(); h.append();
    const sync = fs.fsyncSync;
    const failed = vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      if (fs.fstatSync(fd).isDirectory()) throw new Error("directory sync failed");
      sync(fd);
    });
    h.confirm(); expect(h.delivered).not.toHaveBeenCalled();
    failed.mockRestore(); h.confirm(); expect(h.delivered).toHaveBeenCalledOnce();
  });
  it("requires a session header matching the current Pi session", () => {
    const h = fixture(); h.assistant(); h.append();
    const file = h.manager.getSessionFile()!; const lines = fs.readFileSync(file, "utf8").split("\n");
    const header = JSON.parse(lines[0]!); lines[0] = JSON.stringify({ ...header, id: "wrong-session" });
    fs.writeFileSync(file, lines.join("\n")); h.confirm(); h.confirm(); expect(h.delivered).not.toHaveBeenCalled();
    lines[0] = JSON.stringify(header); fs.writeFileSync(file, lines.join("\n"));
    h.confirm(); expect(h.delivered).toHaveBeenCalledOnce();
  });
  it("does not receipt a torn carrier line until it is complete", () => {
    const h = fixture(); h.assistant(); h.append(); const file = h.manager.getSessionFile()!;
    const size = fs.statSync(file).size; fs.truncateSync(file, size - 1);
    h.confirm(); expect(h.delivered).not.toHaveBeenCalled();
    fs.appendFileSync(file, "\n"); h.confirm(); expect(h.delivered).toHaveBeenCalledOnce();
  });
  it("does not treat in-memory-only sessions as durable receipts", () => {
    const h = fixture(); (h.manager as any).getSessionFile = () => undefined; h.append();
    h.confirm(); expect(h.delivered).not.toHaveBeenCalled();
  });
});
