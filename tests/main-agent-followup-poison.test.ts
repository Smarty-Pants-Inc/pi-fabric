import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { MainAgentController } from "../src/main-agent.js";

// smarty-dev#1826: a followUp whose sender had no name was held for a busy Main; every hand-over
// then threw, the item stayed first, and every later followUp was acked but never delivered (7 h).
type Handler = (event: unknown, ctx: ExtensionContext) => unknown;
const fakePi = () => {
  const handlers = new Map<string, Handler[]>();
  const sent: Array<{ message: { content: string; details: Record<string, any> } }> = [];
  const pi = {
    on: (name: string, fn: Handler) => {
      handlers.set(name, [...(handlers.get(name) ?? []), fn]);
      return () => handlers.set(name, (handlers.get(name) ?? []).filter((h) => h !== fn));
    },
    sendMessage: vi.fn((message) => { sent.push({ message }); }),
    sendUserMessage: vi.fn(),
    getThinkingLevel: () => "off",
  } as unknown as ExtensionAPI;
  const emit = (name: string, event: unknown, ctx: ExtensionContext) => {
    for (const handler of handlers.get(name) ?? []) handler(event, ctx);
  };
  return { pi, sent, emit };
};
const ids = (sent: Array<{ message: { details: Record<string, any> } }>) =>
  sent.flatMap((entry) => (entry.message.details.items ?? [entry.message.details]).map((item: { id: string }) => item.id));

describe("Main followUp drain with a poisoned or nameless item (smarty-dev#1826)", () => {
  const roots: string[] = [];
  let warn: ReturnType<typeof vi.spyOn>;
  beforeEach(() => { warn = vi.spyOn(console, "warn").mockImplementation(() => undefined); });
  afterEach(() => {
    warn.mockRestore();
    for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  });

  const start = (journal: string, entries: unknown[], state: { idle: boolean }) => {
    const fake = fakePi();
    const ctx = {
      isIdle: () => state.idle, hasPendingMessages: () => false, sessionManager: { getEntries: () => entries },
    } as unknown as ExtensionContext;
    const main = new MainAgentController(fake.pi, "session:root", true, path.dirname(journal), "root");
    main.attachFollowUpDrain(ctx, 120_000, journal);
    return { ...fake, ctx, main };
  };
  const tempJournal = () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-poison-"));
    roots.push(dir);
    return path.join(dir, "main-followups", "root.json");
  };
  // Settle: release, then the session holds what was sent, so the journal empties.
  const settle = (run: ReturnType<typeof start>, entries: unknown[], state: { idle: boolean }) => {
    state.idle = true;
    run.emit("agent_before_settle", { outcome: "completed", context: { pendingMessages: [] } }, run.ctx);
    for (const entry of run.sent) entries.push({ type: "custom_message", customType: "pi-fabric-agent-message", details: entry.message.details });
    run.emit("agent_settled", { outcome: "completed" }, run.ctx);
  };

  it("(a) delivers a nameless followUp to a busy Main under its id, and the next one too", () => {
    const journal = tempJournal();
    const entries: unknown[] = [];
    const state = { idle: false };
    const run = start(journal, entries, state);
    const nameless = run.main.deliverAgent({
      from: { id: "knowledge-graph:friction-miner", kind: "main" } as never, message: "friction", delivery: "followUp",
    });
    const normal = run.main.deliverAgent({ from: { id: "knowledge-lead", name: "lead", kind: "agent" }, message: "next", delivery: "followUp" });
    expect(run.sent).toHaveLength(0);                                  // held while busy
    settle(run, entries, state);
    expect(ids(run.sent)).toEqual([nameless.messageId, normal.messageId]);
    expect(run.sent[0]!.message.content).toContain('from_name="knowledge-graph:friction-miner" from_id="knowledge-graph:friction-miner"');
    expect(run.main.queueDepth().pendingFollowUps).toBe(0);
    expect(fs.existsSync(journal)).toBe(false);
  });

  it("(b) drops a journal item that cannot be rendered, with a report, and delivers the rest", () => {
    const journal = tempJournal();
    fs.mkdirSync(path.dirname(journal), { recursive: true });
    const from = { id: "agent-a", name: "a", kind: "agent" };
    fs.writeFileSync(journal, JSON.stringify({
      version: 1,
      items: [
        { id: "poison", from, message: "bad", sentAt: -9e15 },          // an invalid date: toISOString throws
        { id: "good-1", from, message: "one", sentAt: Date.now() - 2 },
        { id: "good-2", from, message: "two", sentAt: Date.now() - 1 },
      ],
    }));
    const entries: unknown[] = [];
    const state = { idle: false };
    const run = start(journal, entries, state);
    settle(run, entries, state);
    expect(ids(run.sent)).toEqual(["good-1", "good-2"]);
    expect(warn).toHaveBeenCalledWith(expect.stringMatching(/dropped undeliverable followUp poison from agent-a: .*Invalid time value/));
    expect(fs.existsSync(journal)).toBe(false);
    // Nothing is held, so the next followUp to an idle Main goes at once.
    const later = run.main.deliverAgent({ from: from as never, message: "later", delivery: "followUp" });
    expect(ids(run.sent).at(-1)).toBe(later.messageId);
  });

  it("(c) rejects a sender without an id or kind", () => {
    const run = start(tempJournal(), [], { idle: false });
    expect(() => run.main.deliverAgent({ from: { name: "x", kind: "agent" } as never, message: "m", delivery: "followUp" }))
      .toThrow(/sender with a string id and kind/);
    expect(() => run.main.deliverAgent({ from: { id: "x", name: "x" } as never, message: "m", delivery: "steer" }))
      .toThrow(/sender with a string id and kind/);
    expect(run.main.queueDepth().pendingFollowUps).toBe(0);
    expect(run.sent).toHaveLength(0);
  });

  it("(d) replays a nameless item journalled by an older runtime", () => {
    const journal = tempJournal();
    fs.mkdirSync(path.dirname(journal), { recursive: true });
    fs.writeFileSync(journal, JSON.stringify({
      version: 1,
      items: [{ id: "old", from: { id: "knowledge-graph:friction-miner", kind: "main" }, message: "held", sentAt: Date.now() }],
    }));
    const entries: unknown[] = [];
    const state = { idle: true };
    const run = start(journal, entries, state);
    expect(ids(run.sent)).toEqual(["old"]);
    expect(run.sent[0]!.message.content).toContain('from_name="knowledge-graph:friction-miner"');
    expect(run.sent[0]!.message.details.from).toEqual({ id: "knowledge-graph:friction-miner", name: "knowledge-graph:friction-miner", kind: "main" });
  });
});
