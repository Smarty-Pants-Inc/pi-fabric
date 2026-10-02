import { describe, expect, it } from "vitest";
import { SessionManager, convertToLlm, type TurnEndEvent } from "@earendil-works/pi-coding-agent";
import { ActivationWindow } from "../src/worker/activation-window.js";
import { compactActivationTools } from "../src/worker/activation-compaction.js";

const assistant = (ids: string[]) => ({
  role: "assistant" as const, content: ids.map(id => ({type: "toolCall" as const, id, name: "read", arguments: {path: id}})),
  api: "openai-completions", provider: "test", model: "test", stopReason: "toolUse" as const, timestamp: 2,
  usage: {input: 1, output: 1, cacheRead: 0, cacheWrite: 0, totalTokens: 2, cost: {input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0}},
});
const fixture = () => {
  const session = SessionManager.inMemory("/repo");
  session.appendMessage({role: "system", content: "", sections: {preamble: "IMMUTABLE_PROMPT"}, timestamp: 1} as never);
  session.appendMessage({role: "user", content: "EXACT_ACTIVATION_DIRECTIONS", timestamp: 1});
  const batch = (ids: string[], text = "large result " + "x".repeat(20_000)) => {
    const message = assistant(ids);
    const messageEntryId = session.appendMessage(message);
    const toolResults = ids.map(id => ({role: "toolResult" as const, toolCallId: id, toolName: "read", content: [{type: "text" as const, text}], isError: false, timestamp: 3}));
    const toolResultEntryIds = toolResults.map(message => session.appendMessage(message));
    const projection = session.buildSessionProjection();
    return {type: "turn_end", turnIndex: 0, entries: [], continue: false, outcome: "completed",
      message, messageEntryId, toolResults, toolResultEntryIds,
      context: {contextEntries: projection.entries, contextMessages: projection.messages,
        llmMessages: convertToLlm(projection.messages), pendingMessages: [], canContinue: true}} satisfies TurnEndEvent;
  };
  const apply = (plan: NonNullable<ReturnType<typeof compactActivationTools>>) => {
    for (const draft of plan.entries) {
      if (draft.type === "compaction") session.appendCompaction(draft.summary, draft.firstKeptEntryId, 9000, draft.details, true);
      else if (draft.type === "context_edit") session.appendContextEdit(draft.targetId, draft.replacement);
    }
    return session.buildSessionContext().messages.filter(message => message.role !== "system");
  };
  return {session, batch, apply};
};

describe("activation native tool compaction", () => {
  it("protects a whole parallel batch and preserves raw journal messages through repeated compactions", () => {
    const f = fixture();
    f.batch(["old"]);
    const event = f.batch(["latest-a", "latest-b"]);
    const before = f.session.getBranch();
    const window = new ActivationWindow([]);
    window.project(event.context.contextMessages.filter(message => message.role !== "system"));
    const plan = compactActivationTools(event)!;
    expect(plan.entries.filter(entry => entry.type === "context_edit")).toHaveLength(1);
    expect(plan.messages.filter(message => message.role === "toolResult" && message.toolCallId.startsWith("latest"))).toEqual(event.toolResults);
    window.authorizeCompaction(plan.summary, plan.messages);
    const projected = f.apply(plan);
    expect(window.project(projected)).toEqual(projected);
    expect(f.session.getBranch().slice(0, before.length)).toEqual(before);
    const next = f.batch(["newest"]);
    const second = compactActivationTools(next)!;
    expect(second.entries.filter(entry => entry.type === "context_edit")).toHaveLength(2);
    window.authorizeCompaction(second.summary, second.messages);
    const reprojected = f.apply(second);
    expect(window.project(reprojected)).toEqual(reprojected);
    expect(reprojected.filter(message => message.role === "compactionSummary")).toHaveLength(1);
    expect(reprojected.find(message => message.role === "user")).toMatchObject({content: "EXACT_ACTIVATION_DIRECTIONS"});
    expect(f.session.getBranch().slice(0, before.length)).toEqual(before);
  });

  it("does not compact a latest-only exchange or already small results", () => {
    const f = fixture();
    expect(compactActivationTools(f.batch(["only"]))).toBeUndefined();
    f.batch(["small"], "small result");
    const plan = compactActivationTools(f.batch(["latest"]));
    expect(plan!.entries.filter(entry => entry.type === "context_edit")).toHaveLength(1);
    expect(plan!.messages.find(message => message.role === "toolResult" && message.toolCallId === "small")).toMatchObject({content: [{type: "text", text: "small result"}]});
  });

  it.each(["summary", "retained", "unapproved"])("rejects an altered native compaction: %s", mode => {
    const f = fixture();
    f.batch(["old"]);
    const event = f.batch(["latest"]);
    const plan = compactActivationTools(event)!;
    const window = new ActivationWindow([]);
    window.project(event.context.contextMessages.filter(message => message.role !== "system"));
    if (mode !== "unapproved") window.authorizeCompaction(plan.summary, plan.messages);
    const projected = f.apply(plan);
    if (mode === "summary") (projected[0] as {summary: string}).summary += "FORGED";
    if (mode === "retained") projected.pop();
    expect(() => window.project(projected)).toThrow(/authorized compaction|current activation/);
  });

  it("checks the raw system witness after compaction while accepting only its checkpoint timestamp change", () => {
    const f = fixture();
    f.batch(["old"]);
    const event = f.batch(["latest"]);
    const plan = compactActivationTools(event)!;
    const window = new ActivationWindow([]);
    window.authorizeCompaction(plan.summary, plan.messages);
    window.project(f.apply(plan));
    const records = event.context.contextMessages.filter(message => message.role === "system");
    const running = f.session.buildSessionContext().messages;
    expect(() => window.verifySystem(records, running)).not.toThrow();
    const rewritten = {...records[0], sections: {preamble: "FORGED"}} as never;
    expect(() => window.verifySystem([rewritten], running)).toThrow(/system records/);
    expect(() => window.verifySystem(records, [rewritten, ...running.slice(1)])).toThrow(/system prompt/);
  });
});
