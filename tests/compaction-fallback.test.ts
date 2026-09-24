import { describe, expect, it } from "vitest";
import { SessionManager, type SessionMessageEntry, type SessionBeforeCompactEvent, type ExtensionAPI, type ExtensionContext } from "@earendil-works/pi-coding-agent";
import { compileFabricSummary, computeCut, rawContextTokens, registerCompactionHook } from "../src/compaction/hook.js";
import { MAX_SUMMARY_BYTES, utf8Bytes } from "../src/compaction/bounds.js";

const assistant = (content: unknown[]) => ({
  role: "assistant", content, api: "anthropic-messages", provider: "test", model: "test", stopReason: "stop", timestamp: 1,
  usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
}) as Extract<SessionMessageEntry["message"], { role: "assistant" }>;
const longTurn = () => {
  const session = SessionManager.inMemory();
  session.appendMessage({ role: "user", content: "Original task", timestamp: 1 });
  session.appendMessage(assistant([{ type: "text", text: "Earlier result" }]));
  session.appendMessage({ role: "user", content: "Long autonomous turn", timestamp: 2 });
  for (let i = 0; i < 40; i++) session.appendMessage(assistant([{ type: "text", text: `Stage ${i}: ${"x".repeat(16_000)}` }]));
  session.appendMessage(assistant([{ type: "text", text: "Latest progress" }]));
  return session;
};
const keptTokens = (session: SessionManager, id: string) => id
  ? rawContextTokens(session.getBranch().slice(session.getBranch().findIndex(e => e.id === id))) : 0;

describe("bounded compaction fallback", () => {
  it("bounds a huge latest turn without inventing a model window", () => {
    const session = longTurn();
    const entries = session.getBranch();
    expect(rawContextTokens(entries)).toBeGreaterThan(100_000);
    const result = compileFabricSummary(entries, rawContextTokens(entries));
    if (!("compaction" in result)) throw new Error(result.reason);
    expect(result.compaction.firstKeptEntryId).not.toBe("");
    expect(keptTokens(session, result.compaction.firstKeptEntryId)).toBeLessThanOrEqual(20_000);
    expect(result.compaction.details?.budget).toBeUndefined();
    expect(utf8Bytes(result.compaction.summary)).toBeLessThanOrEqual(MAX_SUMMARY_BYTES);
  });

  it.each([undefined, 0, NaN, Infinity])("honors a supplied tail limit without a usable window (%s)", (contextWindow) => {
    const session = longTurn();
    const cut = computeCut(session.getBranch(), { tokensBefore: 200_000, budget: { ...(contextWindow !== undefined ? { contextWindow } : {}), targetContextRatio: 0.65, reserveTokens: 16384, keepRecentTokens: 1000 } });
    if (!cut.ok) throw new Error("expected cut");
    expect(keptTokens(session, cut.firstKeptEntryId)).toBeLessThanOrEqual(1000);
    expect(cut.budget).toBeUndefined();
  });

  it("uses preparation settings in the hook when model metadata is missing", () => {
    let handler: ((event: SessionBeforeCompactEvent, context: ExtensionContext) => { compaction?: { firstKeptEntryId: string } } | undefined) | undefined;
    registerCompactionHook({ on(name: string, candidate: unknown) { if (name === "session_before_compact") handler = candidate as typeof handler; } } as ExtensionAPI,
      { getEngine: () => "fabric", getTargetContextRatio: () => 0.65 });
    const session = longTurn();
    const result = handler!({ branchEntries: session.getBranch(), preparation: { tokensBefore: 200_000, settings: { keepRecentTokens: 1000, reserveTokens: 16384 } } } as SessionBeforeCompactEvent, {} as ExtensionContext);
    if (!result?.compaction) throw new Error("expected compaction");
    expect(keptTokens(session, result.compaction.firstKeptEntryId)).toBeLessThanOrEqual(1000);
  });

  it("keeps delayed call/result pairs together while splitting an oversized turn", () => {
    const session = SessionManager.inMemory();
    session.appendMessage({ role: "user", content: "Start", timestamp: 1 });
    const call = session.appendMessage(assistant([{ type: "toolCall", id: "pair", name: "read", arguments: { path: "a.ts" } }]));
    session.appendMessage({ role: "user", content: "Next turn before the result", timestamp: 2 });
    for (let i = 0; i < 20; i++) session.appendMessage(assistant([{ type: "text", text: "x".repeat(16_000) }]));
    const result = session.appendMessage({ role: "toolResult", toolCallId: "pair", toolName: "read", content: [{ type: "text", text: "done" }], isError: false, timestamp: 3 });
    const latest = session.appendMessage(assistant([{ type: "text", text: "Latest progress" }]));
    const cut = computeCut(session.getBranch());
    if (!cut.ok) throw new Error("expected cut");
    expect(cut.firstKeptEntryId).toBe(latest);
    expect(cut.summarized.map(e => e.id)).toEqual(expect.arrayContaining([call, result]));
  });

  it("does not accumulate previous summaries over repeated fallback cuts", () => {
    const session = longTurn();
    for (let cycle = 0; cycle < 4; cycle++) {
      const entries = session.getBranch();
      const result = compileFabricSummary(entries, rawContextTokens(entries));
      if (!("compaction" in result)) throw new Error(result.reason);
      expect(keptTokens(session, result.compaction.firstKeptEntryId)).toBeLessThanOrEqual(20_000);
      session.appendCompaction(result.compaction.summary, result.compaction.firstKeptEntryId, rawContextTokens(entries), result.compaction.details, true);
      expect(session.buildSessionContext().messages.filter(m => m.role === "compactionSummary")).toHaveLength(1);
      for (let i = 0; i < 12; i++) session.appendMessage(assistant([{ type: "text", text: "x".repeat(16_000) }]));
    }
  });
});
