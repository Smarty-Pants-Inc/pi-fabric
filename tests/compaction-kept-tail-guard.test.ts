import { createAssistantMessageEventStream, type AssistantMessage } from "@earendil-works/pi-ai";
import { SessionManager, type ExtensionAPI, type ExtensionContext, type SessionBeforeCompactEvent } from "@earendil-works/pi-coding-agent";
import { describe, expect, it, vi } from "vitest";
import { handleKeptTailBeforeCompact, KEPT_TAIL_TOOL_RESULT_MAX_BYTES, registerCompactionHook } from "../src/compaction/hook.js";
import { registerLazyCompactionHook } from "../src/compaction/lazy-hook.js";

// Keep reason: data safety and the recorded kept-tail overflow failure. Exercise
// the real native compact() and its prompts, not a replacement summary function.
const usage = { input: 10, output: 5, cacheRead: 0, cacheWrite: 0, totalTokens: 15, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } };
const assistant = (content: AssistantMessage["content"]): AssistantMessage => ({
  role: "assistant", content, api: "offline-kept-tail", provider: "offline-kept-tail", model: "fixture", stopReason: "stop", timestamp: 1, usage,
});
const text = (value: string) => [{ type: "text" as const, text: value }];
const bytes = (content: unknown) => Buffer.byteLength(JSON.stringify(content), "utf8");
const exactContent = (size: number) => text("x".repeat(size - bytes(text(""))));
const toolCall = (id: string, name = "read", path = `${id}.ts`) => ({ type: "toolCall" as const, id, name, arguments: { path } });
const user = (manager: SessionManager, content: string) => manager.appendMessage({ role: "user", content, timestamp: 1 });
const reply = (manager: SessionManager, toolCallId: string, content: ReturnType<typeof text>, extra = {}) => manager.appendMessage({
  role: "toolResult", toolCallId, toolName: "read", content, isError: false, timestamp: 2, ...extra,
});
const fixture = () => {
  const manager = SessionManager.inMemory();
  user(manager, "Ordinary earlier task: preserve the existing implementation.");
  const first = manager.appendMessage(assistant([toolCall("a")]));
  return { manager, first };
};
const eventFor = (manager: SessionManager, first: string): SessionBeforeCompactEvent => {
  const projected = manager.buildSessionProjection().entries;
  const index = projected.findIndex(entry => entry.sourceEntry.id === first);
  return {
    type: "session_before_compact", branchEntries: manager.getBranch(), reason: "overflow", willRetry: true, signal: new AbortController().signal,
    preparation: {
      firstKeptEntryId: first, messagesToSummarize: projected.slice(0, index).flatMap(entry => entry.messages), turnPrefixMessages: [],
      isSplitTurn: false, previousSummary: "Previous native checkpoint and pending constraints.", tokensBefore: 700_000,
      fileOps: { read: new Set(["earlier.ts"]), written: new Set(), edited: new Set(["changed.ts"]) },
      settings: { enabled: true, reserveTokens: 1024, keepRecentTokens: 1000 },
    },
    customInstructions: "Preserve ordinary work and unresolved requirements.",
  };
};
const harness = (manager: SessionManager) => {
  const requests: { messages: unknown; options: unknown; model: unknown }[] = [];
  const auth = vi.fn(async () => ({ ok: true, apiKey: "session-key", headers: { "x-session": "yes", "x-remove": null }, baseUrl: "http://session.invalid", env: { SESSION_FIXTURE: "1" } }));
  const stream = vi.fn((model: unknown, context: { messages: unknown }, options: unknown) => {
    requests.push({ messages: context.messages, options, model });
    const response = assistant(text("Ordinary native task summary; keep working on the implementation."));
    const result = createAssistantMessageEventStream();
    result.push({ type: "done", reason: "stop", message: response });
    result.end();
    return result;
  });
  const notify = vi.fn();
  const context = {
    sessionManager: manager, model: { id: "fixture", provider: "offline-kept-tail", api: "offline-kept-tail", baseUrl: "http://original.invalid", reasoning: false, maxTokens: 2048, contextWindow: 128_000 },
    modelRegistry: { getApiKeyAndHeaders: auth, stream }, hasUI: false, ui: { notify }, thinkingLevel: "off",
  } as unknown as ExtensionContext;
  const appendEntry = vi.fn((customType: string, data?: unknown) => { manager.appendCustomEntry(customType, data); });
  const pi = { appendEntry } as unknown as ExtensionAPI;
  return { context, pi, requests, auth, stream, notify, appendEntry };
};
const notice = (size: number, id: string) => `[tool reply of ${size} bytes from read omitted; raw entry ${id} kept in the session file]`;

describe("oversized projected kept-tail guard", () => {
  it.each([KEPT_TAIL_TOOL_RESULT_MAX_BYTES - 1, KEPT_TAIL_TOOL_RESULT_MAX_BYTES])("returns nothing at %i serialized content bytes, without native summary work", size => {
    const { manager, first } = fixture();
    reply(manager, "a", exactContent(size), { details: { deliberatelyNotModelContent: "z".repeat(80_000) } });
    const h = harness(manager);
    expect(bytes(exactContent(size))).toBe(size);
    expect(handleKeptTailBeforeCompact(eventFor(manager, first), h.context, h.pi)).toBeUndefined();
    expect(h.auth).not.toHaveBeenCalled();
    expect(h.stream).not.toHaveBeenCalled();
    expect(h.appendEntry).not.toHaveBeenCalled();
  });

  it.each(["x".repeat(51_174), "\"\\\n".repeat(10_000), "界😀".repeat(10_000)])("measures projected JSON escaping/UTF-8, advances the cut, and preserves native context (%#)", value => {
    const { manager, first } = fixture();
    const content = text(value);
    expect(bytes(content)).toBeGreaterThan(KEPT_TAIL_TOOL_RESULT_MAX_BYTES);
    const giant = reply(manager, "a", content, { details: { rawOnly: "RAW_METADATA".repeat(100_000) } });
    const next = user(manager, "Later work must remain verbatim.");
    const h = harness(manager);
    const event = eventFor(manager, first);
    event.preparation.turnPrefixMessages = [{ role: "user", content: "Existing native turn prefix.", timestamp: 0 }];
    event.preparation.isSplitTurn = true;
    const beforeRaw = JSON.stringify(manager.getEntries());
    const beforePreparation = JSON.stringify(event.preparation, (_key, value) => value instanceof Set ? [...value] : value);
    return Promise.resolve(handleKeptTailBeforeCompact(event, h.context, h.pi)).then(result => {
      expect(result?.compaction).toMatchObject({ firstKeptEntryId: next, tokensBefore: 700_000, usage });
      expect(result?.compaction?.summary).toContain("Ordinary native task summary");
      expect(result?.compaction?.summary).toContain(notice(bytes(content), giant));
      expect(result?.compaction?.details).toEqual({ readFiles: ["a.ts", "earlier.ts"], modifiedFiles: ["changed.ts"] });
      expect(h.requests).toHaveLength(1);
      const prompt = JSON.stringify(h.requests[0]!.messages);
      expect(prompt).toContain(notice(bytes(content), giant));
      expect(prompt).toContain("Ordinary earlier task");
      expect(prompt).toContain("Existing native turn prefix");
      expect(prompt).toContain("Previous native checkpoint");
      expect(prompt).toContain(event.customInstructions);
      expect(prompt).not.toContain("Later work must remain verbatim");
      expect(prompt).not.toContain("RAW_METADATA");
      expect(Buffer.byteLength(prompt, "utf8")).toBeLessThan(16_000);
      expect(h.requests[0]!.options).toMatchObject({ apiKey: "session-key", headers: { "x-session": "yes", "x-remove": null }, env: { SESSION_FIXTURE: "1" }, signal: event.signal, cacheRetention: "none" });
      expect(h.requests[0]!.model).toMatchObject({ baseUrl: "http://session.invalid" });
      expect(JSON.stringify(manager.getEntries())).toBe(beforeRaw);
      expect(JSON.stringify(event.preparation, (_key, value) => value instanceof Set ? [...value] : value)).toBe(beforePreparation);
    });
  });

  it.each(["stub", "omit"] as const)("ignores a raw giant whose active context edit is %s", mode => {
    const { manager, first } = fixture();
    const giant = reply(manager, "a", text("raw giant ".repeat(300_000)));
    manager.appendContextEdit(giant, mode === "stub" ? { content: text("Already guarded by the existing context edit.") } : null);
    const h = harness(manager);
    expect(handleKeptTailBeforeCompact(eventFor(manager, first), h.context, h.pi)).toBeUndefined();
    expect(h.stream).not.toHaveBeenCalled();
    expect(JSON.stringify(manager.getEntry(giant))).toContain("raw giant ");
  });

  it("closes all parallel siblings and multiple giant groups before retaining a raw omission entry", async () => {
    const { manager, first } = fixture();
    // Replace the first fixture call with a separate multi-call group before its results.
    manager.appendMessage(assistant([toolCall("b"), toolCall("c", "edit", "tail-edit.ts")]));
    const giantA = reply(manager, "a", text("A".repeat(90_000)));
    const giantB = reply(manager, "b", text("B".repeat(100_000)));
    reply(manager, "c", text("Normal sibling work must appear in the native summary."), {
      nestedCalls: { complete: true, calls: [{ id: "c/1", name: "write", arguments: { path: "nested.ts" }, status: "ok", durationMs: 1 }] },
    });
    const failed = manager.appendMessage({ ...assistant(text("Abandoned overflow attempt")), stopReason: "error" });
    manager.appendContextEdit(failed, null);
    const h = harness(manager);
    const result = await handleKeptTailBeforeCompact(eventFor(manager, first), h.context, h.pi);
    expect(result?.compaction?.firstKeptEntryId).toBe(failed);
    expect(result?.compaction?.summary).toContain(notice(bytes(text("A".repeat(90_000))), giantA));
    expect(result?.compaction?.summary).toContain(notice(bytes(text("B".repeat(100_000))), giantB));
    const prompt = JSON.stringify(h.requests[0]!.messages);
    expect(prompt).toContain("Normal sibling work");
    expect(prompt).not.toContain("Abandoned overflow attempt");
    expect(result?.compaction?.details).toMatchObject({ modifiedFiles: ["changed.ts", "nested.ts", "tail-edit.ts"] });
    expect(h.appendEntry).not.toHaveBeenCalled();
  });

  it("finds giants anywhere after the original kept boundary, not only its first tool group", async () => {
    const { manager, first } = fixture();
    reply(manager, "a", text("small first reply"));
    manager.appendMessage(assistant([toolCall("b")]));
    const giant = reply(manager, "b", text("x".repeat(100_000)));
    const next = user(manager, "Retain this later request.");
    const h = harness(manager);
    const result = await handleKeptTailBeforeCompact(eventFor(manager, first), h.context, h.pi);
    expect(result?.compaction?.firstKeptEntryId).toBe(next);
    expect(result?.compaction?.summary).toContain(notice(bytes(text("x".repeat(100_000))), giant));
    expect(JSON.stringify(h.requests[0]!.messages)).toContain("small first reply");
  });

  it("uses one append-only marker after multiple giants and the complete latest parallel group", async () => {
    const { manager, first } = fixture();
    const giant = reply(manager, "a", text("x".repeat(100_000)));
    manager.appendMessage(assistant([toolCall("b"), toolCall("c")]));
    const secondGiant = reply(manager, "b", text("y".repeat(90_000)));
    const last = reply(manager, "c", text("Last normal parallel sibling"));
    const before = JSON.stringify(manager.getEntries());
    const h = harness(manager);
    const result = await handleKeptTailBeforeCompact(eventFor(manager, first), h.context, h.pi);
    expect(h.appendEntry).toHaveBeenCalledOnce();
    expect(manager.getEntry(result!.compaction!.firstKeptEntryId)).toMatchObject({ type: "custom", customType: "fabric.kept-tail-boundary", parentId: last });
    expect(result!.compaction!.summary).toContain(notice(bytes(text("x".repeat(100_000))), giant));
    expect(result!.compaction!.summary).toContain(notice(bytes(text("y".repeat(90_000))), secondGiant));
    expect(JSON.stringify(h.requests[0]!.messages)).toContain("Last normal parallel sibling");
    expect(JSON.stringify(manager.getEntries().slice(0, -1))).toBe(before);
    const compaction = result!.compaction!;
    manager.appendCompaction(compaction.summary, compaction.firstKeptEntryId, compaction.tokensBefore, compaction.details, true, compaction.usage);
    expect(JSON.stringify(manager.buildSessionContext())).not.toContain("x".repeat(1000));
    expect(manager.getEntry(giant)).toMatchObject({ message: { content: text("x".repeat(100_000)) } });
  });

  it.each(["auth", "cancel", "summary", "summary-aborted", "no-boundary-api"] as const)("vetoes %s failure without an unsafe native fallback or boundary append", async failure => {
    const { manager, first } = fixture();
    reply(manager, "a", text("x".repeat(100_000)));
    const h = harness(manager);
    const event = eventFor(manager, first);
    if (failure === "auth") h.auth.mockResolvedValue({ ok: false, error: "summary auth denied" } as never);
    if (failure === "cancel") {
      const operation = new AbortController();
      event.signal = operation.signal;
      h.auth.mockImplementation(async () => { operation.abort(); return { ok: true } as never; });
    }
    if (failure === "summary" || failure === "summary-aborted") h.stream.mockImplementation(() => {
      const stream = createAssistantMessageEventStream();
      const stopReason = failure === "summary" ? "error" as const : "aborted" as const;
      const response = { ...assistant(text("Incomplete summary")), stopReason, errorMessage: "401 summary denied" };
      stream.push({ type: "error", reason: stopReason, error: response }); stream.end(); return stream;
    });
    const before = JSON.stringify(manager.getEntries());
    const result = await handleKeptTailBeforeCompact(event, h.context, failure === "no-boundary-api" ? undefined : h.pi);
    expect(result).toEqual({ cancel: true });
    expect(h.appendEntry).not.toHaveBeenCalled();
    expect(JSON.stringify(manager.getEntries())).toBe(before);
    if (failure !== "summary" && failure !== "summary-aborted") expect(h.stream).not.toHaveBeenCalled();
    expect(h.notify).toHaveBeenCalledWith(expect.stringContaining("Kept-tail compaction stopped"), "error");
  });

  it.each([registerCompactionHook, registerLazyCompactionHook])("runs before either current engine and before threshold vetoes (%#)", async register => {
    const { manager, first } = fixture();
    reply(manager, "a", text("x".repeat(100_000)));
    const next = user(manager, "kept");
    const h = harness(manager);
    let handler!: (event: SessionBeforeCompactEvent, context: ExtensionContext) => unknown;
    const pi = { ...h.pi, on(name: string, callback: typeof handler) { if (name === "session_before_compact") handler = callback; } } as unknown as ExtensionAPI;
    register(pi, { getEngine: () => "pi", getThresholdTokens: () => 1_000_000 });
    const event = eventFor(manager, first); event.reason = "threshold";
    const result = await handler(event, h.context);
    expect(result).toMatchObject({ compaction: { firstKeptEntryId: next } });
  });
});
