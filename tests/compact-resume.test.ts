import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { AgentSession, SessionManager, type ExtensionAPI, type ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { CompactController } from "../src/core/compact-controller.js";
import { beginCompactResume, inferCompactResume, recoverCompactResume, resumeCompactInput, settleCompactResume } from "../src/core/compact-resume.js";
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
  const queued: string[] = [];
  const sendUserMessage = vi.fn((message: string) => { queued.push(message); });
  const pi = { appendEntry: (type: string, data: unknown) => manager.appendCustomEntry(type, data), sendUserMessage } as unknown as ExtensionAPI;
  const context = { sessionManager: manager, compact: (args: typeof options) => { options = args; } } as unknown as ExtensionContext;
  const controller = new CompactController({
    onBegin: intent => beginCompactResume(pi, intent),
    onSettled: (intent, status, ctx) => settleCompactResume(pi, intent, status, ctx),
  });
  const provider = new CompactProvider(controller);
  const invocation: FabricInvocationContext = { extensionContext: context, cwd: process.cwd(),
    signal: undefined, parentToolCallId: "compact-test", nestedToolCallId: "nested", update() {} };
  const witness = () => manager.appendCompaction("compacted", manager.getBranch()[0]!.id, 1000);
  const complete = () => { witness(); options!.onComplete!({ summary: "compacted", firstKeptEntryId: "kept", tokensBefore: 1000 }); };
  const admit = () => { for (const content of queued.splice(0)) manager.appendMessage({ role: "user", content, timestamp: 3 }); };
  return { manager, pi, context, controller, provider, invocation, queued, sendUserMessage, witness, complete, admit,
    fail: (message: string) => options!.onError!(new Error(message)),
    callbackAgain: () => options!.onComplete!({ summary: "duplicate", firstKeptEntryId: "kept", tokensBefore: 1000 }) };
};

describe("compaction continuation", () => {
  it.each(["Compact first, then start item X", "Compact. Then start item X", "Compact\nThen start item X", "Compact and start item X", "Compact afterwards start item X"])("retains explicit sequencing: %s", text => {
    expect(inferCompactResume(harness(text).context)).toBe("start item X");
  });

  it("stages committed startup work without inference and consumes the worker's exact admitted prompt once", () => {
    const h = harness();
    beginCompactResume(h.pi, h.controller.request({ resume: "Start X" })); h.witness();
    const text = recoverCompactResume(h.pi, h.context, true, "stage")!;
    expect(text).toContain("Resume after compaction: Start X");
    expect(h.sendUserMessage).not.toHaveBeenCalled();
    expect(resumeCompactInput(h.pi, h.context, text)).toBeUndefined();
    h.manager.appendMessage({ role: "user", content: text, timestamp: 3 });
    expect(resumeCompactInput(h.pi, h.context, "another input")).toBeUndefined();
    expect(recoverCompactResume(h.pi, h.context, true, "stage")).toBeUndefined();
    recoverCompactResume(h.pi, h.context);
    expect(h.sendUserMessage).not.toHaveBeenCalled();
  });

  it("coalesces direct RPC recovery with one input and retries an unadmitted startup after restart", () => {
    const h = harness();
    beginCompactResume(h.pi, h.controller.request({ resume: "Start X" })); h.witness();
    recoverCompactResume(h.pi, h.context, true, "stage");
    const restarted = harness("", h.manager);
    recoverCompactResume(restarted.pi, restarted.context, true, "stage");
    const input = resumeCompactInput(restarted.pi, restarted.context, "Continue the task")!;
    expect(input.action).toBe("transform");
    expect(input.text).toContain("Continue the task\n\nResume after compaction: Start X");
    expect(resumeCompactInput(restarted.pi, restarted.context, "Continue again")).toBeUndefined();
    expect(restarted.sendUserMessage).not.toHaveBeenCalled();
  });

  it("does not inject a startup snapshot after its branch intent is cancelled", () => {
    const h = harness();
    const intent = h.controller.request({ resume: "Start X" });
    beginCompactResume(h.pi, intent); h.witness();
    recoverCompactResume(h.pi, h.context, true, "stage");
    settleCompactResume(h.pi, intent, "failed", h.context);
    expect(resumeCompactInput(h.pi, h.context, "new work")).toBeUndefined();
  });

  it("recognizes every durable receipt when multiple committed intents share one startup prompt", () => {
    const h = harness();
    for (const resume of ["Start X", "Start Y"]) beginCompactResume(h.pi, h.controller.request({ resume }));
    h.witness();
    const text = recoverCompactResume(h.pi, h.context, true, "stage")!;
    expect(text).toContain("Start X"); expect(text).toContain("Start Y");
    h.manager.appendMessage({ role: "user", content: text, timestamp: 3 });
    const restarted = harness("", h.manager);
    expect(recoverCompactResume(restarted.pi, restarted.context, true, "stage")).toBeUndefined();
    recoverCompactResume(restarted.pi, restarted.context);
    expect(restarted.sendUserMessage).not.toHaveBeenCalled();
  });

  it("infers pending user work, resumes one user turn in the same session, and never repeats", async () => {
    const h = harness();
    await h.provider.invoke("request", { instructions: "Keep the map" }, h.invocation);
    expect(h.controller.status().pending?.resume).toBe("start item X");
    const committing = h.controller.maybeCommit(h.context);
    expect(h.sendUserMessage).not.toHaveBeenCalled();
    h.complete(); h.callbackAgain();
    await committing;
    recoverCompactResume(h.pi, h.context);
    await h.controller.maybeCommit(h.context);
    expect(h.sendUserMessage).toHaveBeenCalledTimes(1);
    expect(h.sendUserMessage).toHaveBeenCalledWith(expect.stringContaining("Resume after compaction: start item X"), { deliverAs: "followUp" });
    h.admit();
    recoverCompactResume(h.pi, h.context);
    expect(h.sendUserMessage).toHaveBeenCalledTimes(1);
    expect(inferCompactResume(h.context)).toBeUndefined();
  });

  it.each(["Compact", "/compact", "Please compact your context first.", "Implement X", "Compact with instructions: keep the plan", "Compact. Keep the failing test name in the summary.", "Compact\nKeep the failing test name in the summary.", "Compact first.\nPreserve the plan and test names in the summary."])("plain/manual compaction stays idle: %s", async text => {
    const h = harness(text);
    await h.provider.invoke("request", { instructions: "Keep the failing test name in the summary" }, h.invocation);
    const committing = h.controller.maybeCommit(h.context); h.complete(); await committing;
    recoverCompactResume(h.pi, h.context);
    expect(h.sendUserMessage).not.toHaveBeenCalled();
    expect(h.manager.getBranch().filter(e => e.type === "custom")).toHaveLength(0);
  });

  it.each(["Compaction cancelled", "API quota exceeded", "Nothing to compact (session too small)"])("does not resume failed/cancelled compaction: %s", async message => {
    const h = harness();
    await h.provider.invoke("request", {}, h.invocation);
    const committing = h.controller.maybeCommit(h.context); h.fail(message); await committing;
    h.witness(); // A later unrelated manual compaction cannot revive the failed request.
    recoverCompactResume(h.pi, h.context);
    expect(h.sendUserMessage).not.toHaveBeenCalled();
  });

  it("uses explicit self-requested next steps and lets empty resume disable inference", async () => {
    const h = harness("Implement X");
    await h.provider.invoke("request", { resume: "Run the failing test" }, h.invocation);
    const committing = h.controller.maybeCommit(h.context); h.complete(); await committing;
    expect(h.queued[0]).toContain("Resume after compaction: Run the failing test");
    const idle = harness();
    await idle.provider.invoke("request", { resume: "" }, idle.invocation);
    const compact = idle.controller.maybeCommit(idle.context); idle.complete(); await compact;
    expect(idle.sendUserMessage).not.toHaveBeenCalled();
  });

  it("reads the latest user/steer/followUp message, not an older compound request", () => {
    const h = harness();
    h.manager.appendMessage({ role: "user", content: [{ type: "text", text: "Please compact your context first, then run the tests" }], timestamp: 2 });
    expect(inferCompactResume(h.context)).toBe("run the tests");
    h.manager.appendMessage({ role: "user", content: "Never mind, just compact", timestamp: 3 });
    expect(inferCompactResume(h.context)).toBeUndefined();
  });

  it("infers a tracked worker followUp despite its persisted transport envelope", async () => {
    const h = harness("[fabric-follow-up:12345678-1234-1234-1234-123456789abc]\nCompact first, then start item X");
    await h.provider.invoke("request", {}, h.invocation);
    expect(h.controller.status().pending?.resume).toBe("start item X");
    const committing = h.controller.maybeCommit(h.context); h.complete(); await committing;
    expect(h.sendUserMessage).toHaveBeenCalledTimes(1);
  });

  it("does not journal cancelled or replaced advisory intents", async () => {
    const h = harness();
    h.controller.request({ resume: "Old task" }); h.controller.cancel();
    h.controller.request({ resume: "Another old task" }); h.controller.request({});
    const committing = h.controller.maybeCommit(h.context); h.complete(); await committing;
    expect(h.sendUserMessage).not.toHaveBeenCalled();
  });

  it("recovers a restart after compact commits but before resume is admitted, without a second delivery after another restart", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-compact-resume-")); roots.push(root);
    const h = harness(undefined, SessionManager.create(root, path.join(root, "sessions")));
    warm(h.manager); // Pi persists a session once an assistant message exists.
    await h.provider.invoke("request", {}, h.invocation);
    const compact = h.controller.maybeCommit(h.context);
    h.witness(); // Simulate a process death before onComplete can schedule its user turn.
    const file = h.manager.getSessionFile()!;
    const restarted = harness("", SessionManager.open(file));
    recoverCompactResume(restarted.pi, restarted.context);
    recoverCompactResume(restarted.pi, restarted.context);
    expect(restarted.sendUserMessage).toHaveBeenCalledTimes(1);
    restarted.admit();
    const again = harness("", SessionManager.open(file));
    recoverCompactResume(again.pi, again.context);
    expect(again.sendUserMessage).not.toHaveBeenCalled();
    // Close the first process's simulated in-flight Promise; its scheduling is
    // irrelevant to the persisted receipt but leaves no running test work.
    h.callbackAgain(); await compact;
  });

  it("retries a queued but unpersisted resume on restart, rather than treating scheduling as a durable receipt", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-compact-queued-")); roots.push(root);
    const h = harness(undefined, SessionManager.create(root, path.join(root, "sessions"))); warm(h.manager);
    await h.provider.invoke("request", {}, h.invocation);
    const compact = h.controller.maybeCommit(h.context); h.complete(); await compact;
    expect(h.sendUserMessage).toHaveBeenCalledTimes(1);
    const restarted = harness("", SessionManager.open(h.manager.getSessionFile()!));
    recoverCompactResume(restarted.pi, restarted.context); restarted.admit();
    expect(restarted.sendUserMessage).toHaveBeenCalledTimes(1);
    expect(restarted.manager.getBranch().filter(e => e.type === "message" && e.message.role === "user" &&
      typeof e.message.content === "string" && e.message.content.startsWith("Resume after compaction:"))).toHaveLength(1);
  });

  it("retires a restart before compact commits so an unrelated later compact cannot revive it", () => {
    const h = harness();
    const intent = h.controller.request({ resume: "Old interrupted work" });
    beginCompactResume(h.pi, intent);
    recoverCompactResume(h.pi, h.context, true);
    h.witness();
    recoverCompactResume(h.pi, h.context, true);
    expect(h.sendUserMessage).not.toHaveBeenCalled();
  });

  it("recognizes the persisted receipt ID even when user admission adds a transport prefix", async () => {
    const h = harness();
    await h.provider.invoke("request", {}, h.invocation);
    const compact = h.controller.maybeCommit(h.context); h.complete(); await compact;
    h.manager.appendMessage({ role: "user", content: `Transport annotation\n${h.queued[0]}`, timestamp: 3 });
    const restarted = harness("", h.manager);
    recoverCompactResume(restarted.pi, restarted.context, true);
    expect(restarted.sendUserMessage).not.toHaveBeenCalled();
  });

  it("uses Pi's real settled deferral: no new turn starts inside remaining settled handlers", async () => {
    const h = harness();
    const timeline: string[] = [];
    const session = Object.create(AgentSession.prototype) as AgentSession;
    const internals = session as unknown as Record<string, any>;
    Object.assign(internals, {
      _deferredSettledActions: [], _eventListeners: [],
      _resolveIdleWaitIfIdle: () => {},
      _extensionRunner: { emit: async () => {
        timeline.push("handler:start");
        await h.controller.maybeCommit(h.context);
        timeline.push("handler:end");
        expect(timeline).not.toContain("user:turn");
      } },
      prompt: async function(this: Record<string, any>, text: string, options: unknown) {
        if (this._isEmittingAgentSettled) return AgentSession.prototype.prompt.call(session, text, options as never);
        timeline.push("user:turn"); h.manager.appendMessage({ role: "user", content: text, timestamp: 3 });
      },
    });
    h.pi.sendUserMessage = (text, options) => { void session.sendUserMessage(text, options); };
    await h.provider.invoke("request", {}, h.invocation);
    const settling = internals._emitAgentSettled();
    h.complete(); await settling;
    expect(timeline).toEqual(["handler:start", "handler:end", "user:turn"]);
    recoverCompactResume(h.pi, h.context);
    expect(timeline.filter(e => e === "user:turn")).toHaveLength(1);
  });
});
