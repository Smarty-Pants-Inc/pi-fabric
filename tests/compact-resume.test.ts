import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { SessionManager, type ExtensionAPI, type ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { CompactController } from "../src/core/compact-controller.js";
import { beginCompactResume, inferCompactResume, LIVE_COMPACT_RESUME_REFUSAL, recoverCompactResume,
  RESTART_COMPACT_RESUME_REFUSAL, reportCompactResumeRefusal, settleCompactResume } from "../src/core/compact-resume.js";
import { compactResumeMessage } from "../src/compaction/resume-delivery.js";
import { CompactProvider } from "../src/providers/compact-provider.js";
import type { FabricInvocationContext } from "../src/protocol.js";

const roots: string[] = [];
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
const warm = (manager: SessionManager) => {
  manager.appendMessage({ role: "assistant", content: [{ type: "text", text: "I will compact, then start X." }],
    api: "anthropic-messages", provider: "test", model: "test", timestamp: 2, stopReason: "stop",
    usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, totalTokens: 2,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } } });
};
const harness = (text = "Compact first, then start item X", manager = SessionManager.inMemory(process.cwd())) => {
  if (text) manager.appendMessage({ role: "user", content: text, timestamp: 1 });
  let options: Parameters<ExtensionContext["compact"]>[0];
  const sendUserMessage = vi.fn();
  const notify = vi.fn();
  const pi = { appendEntry: (type: string, data: unknown) => manager.appendCustomEntry(type, data), sendUserMessage } as unknown as ExtensionAPI;
  const context = { sessionManager: manager, compact: vi.fn((args: typeof options) => { options = args; }),
    hasUI: true, ui: { notify } } as unknown as ExtensionContext;
  const controller = new CompactController({
    onBegin: intent => beginCompactResume(pi, intent),
    onSettled: (intent, status, ctx) => settleCompactResume(pi, intent, status, ctx),
  });
  const provider = new CompactProvider(controller);
  const invocation: FabricInvocationContext = { extensionContext: context, cwd: process.cwd(),
    signal: undefined, parentToolCallId: "compact-test", nestedToolCallId: "nested", update() {} };
  const witness = () => manager.appendCompaction("compacted", manager.getBranch()[0]!.id, 1000);
  const callbackAgain = () => options!.onComplete!({ summary: "compacted", firstKeptEntryId: "kept", tokensBefore: 1000 });
  const complete = () => { witness(); callbackAgain(); };
  return { manager, pi, context, controller, provider, invocation, sendUserMessage, notify, witness, complete, callbackAgain,
    fail: (message: string) => options!.onError!(new Error(message)) };
};
const refusals = (manager: SessionManager) => manager.getBranch().filter(e => e.type === "custom" && e.customType === "fabric-compact-resume" && (e.data as any)?.state === "cancelled");

describe("compaction safe floor: automatic resume disabled", () => {
  it.each(["Compact first, then start item X", "Compact. Then start item X", "Compact\nThen start item X", "Compact and start item X", "Compact afterwards start item X"])("identifies pending work only to refuse it: %s", text => {
    expect(inferCompactResume(harness(text).context)).toBe("start item X");
  });

  it("commits successful live compaction but durably refuses its next step once", async () => {
    const h = harness();
    await h.provider.invoke("request", { instructions: "Keep the map" }, h.invocation);
    const intent = h.controller.status().pending!;
    expect(intent.resume).toBe("start item X");
    const committing = h.controller.maybeCommit(h.context);
    h.complete(); h.callbackAgain(); await committing;
    expect(h.controller.status()).toMatchObject({ last: { status: "committed", summary: "compacted" } });
    expect(h.controller.status().pending).toBeUndefined();
    expect(refusals(h.manager)).toHaveLength(1);
    expect(refusals(h.manager)[0]).toMatchObject({ data: { id: intent.resumeId, resume: "start item X", reason: LIVE_COMPACT_RESUME_REFUSAL } });
    expect(h.notify).toHaveBeenCalledWith(expect.stringContaining("Automatic compaction resume is disabled"), "warning");
    expect(recoverCompactResume(h.pi, h.context)).toBe(0);
    await h.controller.maybeCommit(h.context);
    expect(h.sendUserMessage).not.toHaveBeenCalled();
    expect(h.context.compact).toHaveBeenCalledTimes(1);
  });

  it("round-5 P1: an aborted settlement with an already-cleared signal cannot start any continuation", async () => {
    const h = harness();
    await h.provider.invoke("request", { resume: "Work with side effects" }, h.invocation);
    // Pi clears the low-level run before agent_settled(outcome=aborted). Main
    // still commits pending compaction here; the floor must not start a turn.
    expect(h.context.signal).toBeUndefined();
    const committing = h.controller.maybeCommit(h.context);
    h.complete(); await committing;
    h.witness(); // Nor may a later unrelated compaction revive that work.
    expect(recoverCompactResume(h.pi, h.context)).toBe(0);
    expect(recoverCompactResume(h.pi, h.context, true)).toBe(0);
    expect(h.sendUserMessage).not.toHaveBeenCalled();
    expect(refusals(h.manager)[0]).toMatchObject({ data: { resume: "Work with side effects", reason: LIVE_COMPACT_RESUME_REFUSAL } });
  });

  it("durably refuses a pre-start abort even though onBegin never journaled pending work", async () => {
    const h = harness();
    const abort = new AbortController(); abort.abort();
    Object.assign(h.context, { signal: abort.signal });
    h.controller.request({ resume: "Start X" });
    await h.controller.maybeCommit(h.context);
    expect(h.context.compact).not.toHaveBeenCalled();
    expect(h.controller.status().last?.status).toBe("failed");
    expect(refusals(h.manager)[0]).toMatchObject({ data: { reason: LIVE_COMPACT_RESUME_REFUSAL } });
    expect(h.sendUserMessage).not.toHaveBeenCalled();
  });

  it.each(["Compaction cancelled", "API quota exceeded", "Nothing to compact (session too small)"])("refuses failed/cancelled live compaction: %s", async message => {
    const h = harness();
    await h.provider.invoke("request", {}, h.invocation);
    const committing = h.controller.maybeCommit(h.context); h.fail(message); await committing;
    h.witness();
    expect(recoverCompactResume(h.pi, h.context)).toBe(0);
    expect(refusals(h.manager)[0]).toMatchObject({ data: { reason: LIVE_COMPACT_RESUME_REFUSAL } });
    expect(h.sendUserMessage).not.toHaveBeenCalled();
  });

  it.each([undefined, { id: "principal-A", binding: "herdr-client" }])("durably refuses restart A (%j) rather than lending fresh B's principal", principal => {
    const h = harness();
    const intent = h.controller.request({ resume: "Start X" });
    beginCompactResume(h.pi, intent); h.witness();
    if (principal) h.manager.appendCustomEntry("fabric-compact-resume", { id: intent.resumeId, resume: "Start X", state: "pending", principal });
    const restarted = harness("", h.manager);
    expect(recoverCompactResume(restarted.pi, restarted.context, true)).toBe(1);
    expect(refusals(restarted.manager)[0]).toMatchObject({ data: { id: intent.resumeId, state: "cancelled", reason: RESTART_COMPACT_RESUME_REFUSAL } });
    const fresh = "Fresh task B";
    restarted.manager.appendMessage({ role: "user", content: fresh, timestamp: 3 });
    expect(restarted.manager.getBranch().at(-1)).toMatchObject({ type: "message", message: { content: fresh } });
    restarted.witness();
    expect(recoverCompactResume(restarted.pi, restarted.context, true)).toBe(0);
    expect(recoverCompactResume(restarted.pi, restarted.context)).toBe(0);
    expect(restarted.sendUserMessage).not.toHaveBeenCalled();
  });

  it.each([false, true])("retire uncommitted legacy work durably, startup=%s", startup => {
    const h = harness();
    beginCompactResume(h.pi, h.controller.request({ resume: "Old interrupted work" }));
    expect(recoverCompactResume(h.pi, h.context, startup)).toBe(1);
    h.witness();
    expect(recoverCompactResume(h.pi, h.context, startup)).toBe(0);
    expect(h.sendUserMessage).not.toHaveBeenCalled();
  });

  it.each(["", "Transport annotation\n"])("does not change already admitted legacy receipt-bearing work: %j", prefix => {
    const h = harness();
    const intent = h.controller.request({ resume: "Start X" });
    beginCompactResume(h.pi, intent); h.witness();
    h.manager.appendMessage({ role: "user", content: prefix + compactResumeMessage({ id: intent.resumeId!, resume: intent.resume! }), timestamp: 3 });
    expect(recoverCompactResume(h.pi, h.context, true)).toBe(0);
    expect(refusals(h.manager)).toHaveLength(0);
    expect(h.sendUserMessage).not.toHaveBeenCalled();
  });

  it.each(["Compact", "/compact", "Please compact your context first.", "Implement X", "Compact with instructions: keep the plan", "Compact. Keep the failing test name in the summary.", "Compact\nKeep the failing test name in the summary.", "Compact first.\nPreserve the plan and test names in the summary."])("plain/manual compaction remains main's idle behavior: %s", async text => {
    const h = harness(text);
    await h.provider.invoke("request", { instructions: "Keep the failing test name in the summary" }, h.invocation);
    const committing = h.controller.maybeCommit(h.context); h.complete(); await committing;
    expect(h.context.compact).toHaveBeenCalledWith(expect.objectContaining({ customInstructions: "Keep the failing test name in the summary" }));
    expect(h.controller.status().last?.status).toBe("committed");
    expect(h.sendUserMessage).not.toHaveBeenCalled();
    expect(h.manager.getBranch().filter(e => e.type === "custom")).toHaveLength(0);
    expect(h.notify).not.toHaveBeenCalled();
  });

  it("keeps explicit resume separate from summary instructions, and empty text opts out", async () => {
    const h = harness("Implement X");
    await h.provider.invoke("request", { resume: "Run the failing test", instructions: "Preserve the map" }, h.invocation);
    const committing = h.controller.maybeCommit(h.context); h.complete(); await committing;
    expect(h.context.compact).toHaveBeenCalledWith(expect.objectContaining({ customInstructions: "Preserve the map" }));
    expect(refusals(h.manager)[0]).toMatchObject({ data: { resume: "Run the failing test" } });
    const idle = harness();
    await idle.provider.invoke("request", { resume: "" }, idle.invocation);
    const compact = idle.controller.maybeCommit(idle.context); idle.complete(); await compact;
    expect(refusals(idle.manager)).toHaveLength(0);
    expect(idle.notify).not.toHaveBeenCalled();
    expect(h.sendUserMessage).not.toHaveBeenCalled();
    expect(idle.sendUserMessage).not.toHaveBeenCalled();
  });

  it("uses only the latest user/steer/followUp instruction", () => {
    const h = harness();
    h.manager.appendMessage({ role: "user", content: "[fabric-follow-up:12345678-1234-1234-1234-123456789abc]\nCompact first, then run the tests", timestamp: 2 });
    expect(inferCompactResume(h.context)).toBe("run the tests");
    h.manager.appendMessage({ role: "user", content: "Never mind, just compact", timestamp: 3 });
    expect(inferCompactResume(h.context)).toBeUndefined();
  });

  it("does not journal cancelled or replaced advisory intents", async () => {
    const h = harness();
    h.controller.request({ resume: "Old task" }); h.controller.cancel();
    h.controller.request({ resume: "Another old task" }); h.controller.request({});
    const committing = h.controller.maybeCommit(h.context); h.complete(); await committing;
    expect(h.manager.getBranch().filter(e => e.type === "custom")).toHaveLength(0);
    expect(h.sendUserMessage).not.toHaveBeenCalled();
  });

  it.each(["live", "restart"])("persists %s refusal across a real SessionManager reopen", async mode => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-compact-refused-")); roots.push(root);
    const h = harness(undefined, SessionManager.create(root, path.join(root, "sessions"))); warm(h.manager);
    await h.provider.invoke("request", {}, h.invocation);
    const compact = h.controller.maybeCommit(h.context);
    if (mode === "live") { h.complete(); await compact; }
    else h.witness(); // Simulate death after compaction and before onComplete.
    const reopened = harness("", SessionManager.open(h.manager.getSessionFile()!));
    expect(recoverCompactResume(reopened.pi, reopened.context, true)).toBe(mode === "live" ? 0 : 1);
    const again = harness("", SessionManager.open(h.manager.getSessionFile()!));
    expect(refusals(again.manager)[0]).toMatchObject({ data: { reason: mode === "live" ? LIVE_COMPACT_RESUME_REFUSAL : RESTART_COMPACT_RESUME_REFUSAL } });
    expect(recoverCompactResume(again.pi, again.context, true)).toBe(0);
    expect(again.sendUserMessage).not.toHaveBeenCalled();
    if (mode === "restart") { h.callbackAgain(); await compact; }
    expect(h.sendUserMessage).not.toHaveBeenCalled();
  });

  it("reports restart refusal to UI only when pending work was retired", () => {
    const h = harness();
    reportCompactResumeRefusal(h.context, 0, true);
    expect(h.notify).not.toHaveBeenCalled();
    reportCompactResumeRefusal(h.context, 1, true);
    expect(h.notify).toHaveBeenCalledWith(expect.stringContaining("original admission cannot be proven"), "warning");
  });
});
