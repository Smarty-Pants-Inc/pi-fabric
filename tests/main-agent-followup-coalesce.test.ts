import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fauxAssistantMessage, fauxProvider, fauxToolCall } from "@earendil-works/pi-ai";
import {
  createAgentSession,
  DefaultResourceLoader,
  ModelRuntime,
  SessionManager,
  type AgentSession,
  type ExtensionAPI,
  type ExtensionContext,
} from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { FOLLOW_UP_LIMITS, MainAgentController } from "../src/main-agent.js";

// smarty-dev#1495: a sender that notifies on each state change left ~26 stale wakes for one
// busy Main. A held followUp with the same (sender id, data.coalesceKey) is replaced in place.
type Handler = (event: unknown, ctx: ExtensionContext) => unknown;
const fakePi = () => {
  const handlers = new Map<string, Handler[]>();
  const sent: Array<{ message: { content: string; details: Record<string, any> }; options: { deliverAs: string; triggerTurn: boolean } }> = [];
  const pi = {
    on: (name: string, fn: Handler) => {
      handlers.set(name, [...(handlers.get(name) ?? []), fn]);
      return () => handlers.set(name, (handlers.get(name) ?? []).filter((h) => h !== fn));
    },
    sendMessage: vi.fn((message, options) => { sent.push({ message, options }); }),
    sendUserMessage: vi.fn(),
    getThinkingLevel: () => "off",
  } as unknown as ExtensionAPI;
  const emit = (name: string, event: unknown, ctx: ExtensionContext) => {
    for (const handler of handlers.get(name) ?? []) handler(event, ctx);
  };
  return { pi, sent, emit };
};
const busyContext = (entries: unknown[] = [], state = { idle: false }) => ({
  isIdle: () => state.idle, hasPendingMessages: () => false, signal: { aborted: false },
  sessionManager: { getEntries: () => entries },
}) as unknown as ExtensionContext;
const from = (name: string) => ({ id: `agent-${name}`, name, kind: "agent" as const });
const toolTurn = { message: { role: "assistant", stopReason: "toolUse" } };
/** The messages of every delivered item, in order. */
const delivered = (sent: ReturnType<typeof fakePi>["sent"]) => sent.flatMap((entry) =>
  [...entry.message.content.matchAll(/<fabric-agent-message [^>]*>\n([^\n<]*)/g)].map((match) => match[1]));

const roots: string[] = [];
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
const journalPath = () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-coalesce-"));
  roots.push(dir);
  return { dir, journal: path.join(dir, "main-followups", "root.json") };
};

describe("Main followUp coalescing (unit)", () => {
  beforeEach(() => { vi.useFakeTimers(); vi.setSystemTime(new Date("2026-09-28T06:00:00Z")); });
  afterEach(() => { vi.useRealTimers(); });

  const setup = (journal?: string) => {
    const fake = fakePi();
    const ctx = busyContext();
    const main = new MainAgentController(fake.pi, "session:root", true, "/tmp/project", "root");
    main.attachFollowUpDrain(ctx, 120_000, journal);
    return { ...fake, ctx, main };
  };
  const wake = (head: string, key = "repo#7/state", sender = "factory") =>
    ({ from: from(sender), message: `head ${head}`, delivery: "followUp" as const, data: { coalesceKey: key, head } });

  it("three same-key followUps while busy arrive as ONE, the newest, keeping the first one's place", () => {
    const { main, sent, emit, ctx } = setup();
    main.deliverAgent({ from: from("other"), message: "before", delivery: "followUp" });
    const first = main.deliverAgent(wake("h1"));
    vi.advanceTimersByTime(60_000);
    main.deliverAgent({ from: from("other"), message: "after", delivery: "followUp" });
    const second = main.deliverAgent(wake("h2"));
    const third = main.deliverAgent(wake("h3"));
    expect(first.coalesced).toBeUndefined();
    expect(second).toMatchObject({ queued: true, coalesced: true, replacedMessageId: first.messageId, pendingFollowUps: 1 });
    expect(third).toMatchObject({ coalesced: true, replacedMessageId: second.messageId, pendingFollowUps: 1, oldestAgeS: 60 });
    expect(main.queueDepth().pendingFollowUps).toBe(3);
    vi.advanceTimersByTime(60_000);                      // the chain is due on the first one's wait
    emit("turn_end", toolTurn, ctx);
    expect(delivered(sent)).toEqual(["before", "head h3"]);   // "after" has waited 60 s only
    vi.advanceTimersByTime(60_000);
    emit("turn_end", toolTurn, ctx);
    expect(delivered(sent)).toEqual(["before", "head h3", "after"]);
    const item = sent[0]!.message.details.items[1];
    expect(item).toMatchObject({ id: third.messageId, data: { head: "h3" }, chain: first.messageId, generation: 2, replaces: second.messageId });
    expect(item.replacedAt).toBe("2026-09-28T06:01:00.000Z");
    expect(sent[0]!.message.content).toContain('sent_at="2026-09-28T06:00:00.000Z" replaced_at="2026-09-28T06:01:00.000Z">\nhead h3');
  });

  it("different keys, different senders with the same key, and no key are all delivered", () => {
    const { main, sent, emit, ctx } = setup();
    const results = [
      main.deliverAgent(wake("a", "repo#1/state")),
      main.deliverAgent(wake("b", "repo#2/state")),
      main.deliverAgent(wake("c", "repo#1/state", "reviewer")),
      main.deliverAgent({ from: from("factory"), message: "plain 1", delivery: "followUp" }),
      main.deliverAgent({ from: from("factory"), message: "plain 2", delivery: "followUp", data: { head: "x" } }),
      // Not a usable key: empty, too long, or not a string. Each stays its own followUp.
      main.deliverAgent({ ...wake("d", ""), message: "empty key" }),
      main.deliverAgent({ ...wake("e", ""), message: "empty key again" }),
      main.deliverAgent({ ...wake("f", "k".repeat(201)), message: "long key" }),
      main.deliverAgent({ ...wake("g", "k".repeat(201)), message: "long key again" }),
      main.deliverAgent({ from: from("factory"), message: "number key", delivery: "followUp", data: { coalesceKey: 7 } }),
      main.deliverAgent({ from: from("factory"), message: "number key again", delivery: "followUp", data: { coalesceKey: 7 } }),
    ];
    expect(results.every((result) => result.coalesced === undefined)).toBe(true);
    vi.advanceTimersByTime(120_000);
    emit("turn_end", toolTurn, ctx);
    expect(delivered(sent)).toEqual([
      "head a", "head b", "head c", "plain 1", "plain 2", "empty key", "empty key again",
      "long key", "long key again", "number key", "number key again",
    ]);
  });

  it("a 200-character key coalesces", () => {
    const { main } = setup();
    main.deliverAgent(wake("a", "k".repeat(200)));
    expect(main.deliverAgent(wake("b", "k".repeat(200))).coalesced).toBe(true);
  });

  it("never replaces a followUp already handed to Main: the next one with that key is new", () => {
    const { main, sent, emit, ctx } = setup();
    const first = main.deliverAgent(wake("h1"));
    vi.advanceTimersByTime(120_000);
    emit("turn_end", toolTurn, ctx);                     // handed over at the boundary
    const second = main.deliverAgent(wake("h2"));
    expect(second.coalesced).toBeUndefined();
    vi.advanceTimersByTime(120_000);
    emit("turn_end", toolTurn, ctx);
    expect(sent.map((entry) => entry.message.details.id)).toEqual([first.messageId, second.messageId]);
  });

  it("a replacement does not count twice against the quotas", () => {
    const { main } = setup();
    for (let index = 0; index < FOLLOW_UP_LIMITS.senderItems; index++) main.deliverAgent(wake(`h${index}`, `k${index}`));
    // Full for new items, but a replacement takes the replaced item's slot.
    expect(() => main.deliverAgent(wake("new", "k-new"))).toThrow(/per sender/);
    for (let round = 0; round < 3; round++) expect(main.deliverAgent(wake(`again${round}`, "k0")).coalesced).toBe(true);
    const bytes = setup();
    const big = "x".repeat(100 * 1024);
    bytes.main.deliverAgent({ ...wake("a", "big"), message: big });
    bytes.main.deliverAgent({ ...wake("b", "other"), message: big });
    expect(bytes.main.deliverAgent({ ...wake("c", "big"), message: big }).coalesced).toBe(true);
  });

  it("rewrites the journal for a replacement; a failed write keeps the replaced item", () => {
    const { journal } = journalPath();
    const { main } = setup(journal);
    const first = main.deliverAgent(wake("h1"));
    const second = main.deliverAgent(wake("h2"));
    const items = JSON.parse(fs.readFileSync(journal, "utf8")).items as Array<{ id: string; chain?: string; generation?: number }>;
    expect(items.map((item) => item.id)).toEqual([second.messageId]);
    expect(items[0]).toMatchObject({ chain: first.messageId, generation: 1, replaces: first.messageId });
    fs.rmSync(journal);
    fs.mkdirSync(journal);                               // the next write cannot replace a directory
    expect(() => main.deliverAgent(wake("h3"))).toThrow(/could not record/);
    expect(main.queueDepth().pendingFollowUps).toBe(1);
    fs.rmSync(journal, { recursive: true });
    expect(main.deliverAgent(wake("h4"))).toMatchObject({ coalesced: true, replacedMessageId: second.messageId });
  });

  it("after a restart with a replaced entry in the journal, the newest is delivered exactly once, the superseded never", () => {
    const { dir, journal } = journalPath();
    const firstPi = fakePi();
    const main = new MainAgentController(firstPi.pi, "session:root", true, dir, "root");
    main.attachFollowUpDrain(busyContext(), 120_000, journal);
    const first = main.deliverAgent(wake("h1"));
    const second = main.deliverAgent(wake("h2"));
    main.closeFollowUpDrain();                           // the process stops
    expect(firstPi.sent).toHaveLength(0);
    // An older writer's journal could still list the superseded item: it must not come back.
    const written = JSON.parse(fs.readFileSync(journal, "utf8"));
    written.items.unshift({ id: first.messageId, from: from("factory"), message: "head h1", sentAt: written.items[0].sentAt, data: { coalesceKey: "repo#7/state" } });
    fs.writeFileSync(journal, JSON.stringify(written));
    const entries: unknown[] = [];
    const restart = () => {
      const next = fakePi();
      const controller = new MainAgentController(next.pi, "session:root", true, dir, "root");
      controller.attachFollowUpDrain(busyContext(entries, { idle: true }), 120_000, journal);
      return { next, controller };
    };
    const again = restart();
    expect(again.next.sent.map((entry) => entry.message.details.id)).toEqual([second.messageId]);
    expect(delivered(again.next.sent)).toEqual(["head h2"]);
    entries.push({ type: "custom_message", customType: "pi-fabric-agent-message", details: again.next.sent[0]!.message.details });
    again.controller.closeFollowUpDrain();
    const third = restart();
    expect(third.next.sent).toHaveLength(0);
    expect(fs.existsSync(journal)).toBe(false);
  });

  // review/astra F1 on pi-fabric#114, rounds 1 and 2: a 34-message burst, then a restart whose
  // journal still lists stale copies (as this writer wrote them earlier), with the newest still
  // held, handed to Pi, or already in the session.
  for (const stage of ["held", "handed", "in the session"] as const) {
    it(`after a 34-message burst, the newest ${stage}, a restart never brings back an earlier one`, () => {
      const { dir, journal } = journalPath();
      const firstPi = fakePi();
      const entries: unknown[] = [];
      const main = new MainAgentController(firstPi.pi, "session:root", true, dir, "root");
      main.attachFollowUpDrain(busyContext(entries), 120_000, journal);
      // Counterexample: an earlier same-key followUp already handed over is its own message.
      main.deliverAgent(wake("h0"));
      vi.advanceTimersByTime(120_000);
      firstPi.emit("turn_end", toolTurn, busyContext(entries));
      entries.push({ type: "custom_message", customType: "pi-fabric-agent-message", details: firstPi.sent[0]!.message.details });
      firstPi.emit("turn_end", toolTurn, busyContext(entries));                // confirmed: out of the journal
      main.deliverAgent({ from: from("other"), message: "plain", delivery: "followUp" });
      const copies: unknown[] = [];
      const burst = Array.from({ length: 34 }, (_, index) => {
        vi.advanceTimersByTime(1_000);
        const result = main.deliverAgent(wake(`h${index + 1}`));
        copies.push(JSON.parse(fs.readFileSync(journal, "utf8")).items.find((item: { id: string }) => item.id === result.messageId));
        return result;
      });
      main.deliverAgent(wake("x1", "repo#8/state"));
      main.deliverAgent(wake("r1", "repo#7/state", "reviewer"));          // same key, another sender
      if (stage !== "held") {
        vi.advanceTimersByTime(120_000);
        firstPi.emit("turn_end", toolTurn, busyContext(entries));             // all handed to Pi
        expect(delivered(firstPi.sent.slice(1))).toEqual(["plain", "head h34", "head x1", "head r1"]);
        if (stage === "in the session") {
          entries.push({ type: "custom_message", customType: "pi-fabric-agent-message", details: firstPi.sent[1]!.message.details });
          firstPi.emit("turn_end", toolTurn, busyContext(entries));           // confirmed: h34 leaves the journal
          expect(fs.existsSync(journal)).toBe(false);
        }
      }
      main.closeFollowUpDrain();
      const written = fs.existsSync(journal) ? JSON.parse(fs.readFileSync(journal, "utf8")) : { version: 1, items: [] };
      written.items.push(copies[0], copies[1], copies[20]);                   // stale: h1, h2, h21
      fs.writeFileSync(journal, JSON.stringify(written));
      const next = fakePi();
      const again = new MainAgentController(next.pi, "session:root", true, dir, "root");
      again.attachFollowUpDrain(busyContext(entries, { idle: true }), 120_000, journal);
      next.emit("agent_before_settle", { outcome: "completed", context: { pendingMessages: [] } }, busyContext(entries));
      const expected = stage === "in the session" ? [] : ["plain", "head h34", "head x1", "head r1"];
      expect(delivered(next.sent)).toEqual(expected);
      expect(next.sent.flatMap((entry) => entry.message.details.items?.map((item: { id: string }) => item.id) ?? [entry.message.details.id]))
        .not.toContain(burst[0]!.messageId);
    });
  }
});

const sessions: AgentSession[] = [];
afterEach(() => { for (const session of sessions.splice(0)) session.dispose(); });
const waitFor = async (predicate: () => boolean, timeoutMs = 5_000): Promise<void> => {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("timed out");
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
};

// The #102 real-Pi harness: Main chains tool calls; wakes arrive mid-tool.
describe("Main followUp coalescing in a real Pi session", () => {
  it("three same-key wakes during a tool call reach Main as one message, the newest; another key still arrives", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-coalesce-"));
    roots.push(root);
    const faux = fauxProvider();
    const modelRuntime = await ModelRuntime.create({ modelsPath: null, refreshOnCreate: false, authPath: path.join(root, "auth.json") });
    modelRuntime.registerNativeProvider(faux.provider);
    let main: MainAgentController | undefined;
    const gates: Array<() => void> = [];
    let toolsStarted = 0;
    const loader = new DefaultResourceLoader({
      cwd: root, agentDir: path.join(root, "agent"), noSkills: true, noPromptTemplates: true, noThemes: true, noContextFiles: true,
      extensionFactories: [{
        name: "coalesce",
        factory: (pi: ExtensionAPI) => {
          pi.registerTool({
            name: "work", label: "work", description: "blocks until released", parameters: Type.Object({}),
            execute: async () => {
              toolsStarted++;
              await new Promise<void>((resolve) => gates.push(resolve));
              return { content: [{ type: "text", text: "done" }], details: {} };
            },
          });
          pi.on("session_start", (_event, ctx) => {
            main = new MainAgentController(pi, "session:root", true, root, "root");
            main.attachFollowUpDrain(ctx, 100, path.join(root, "journal.json"));
          });
        },
      }],
    });
    await loader.reload();
    const { session } = await createAgentSession({
      cwd: root, agentDir: path.join(root, "agent"), modelRuntime, model: faux.getModel(), resourceLoader: loader,
      sessionManager: SessionManager.inMemory(root), tools: ["work"],
    });
    sessions.push(session);
    await session.bindExtensions({});
    await waitFor(() => main !== undefined);
    faux.setResponses([
      fauxAssistantMessage(fauxToolCall("work", {}), { stopReason: "toolUse" }),
      fauxAssistantMessage(fauxToolCall("work", {}), { stopReason: "toolUse" }),
      fauxAssistantMessage("all done"),
    ]);
    const run = session.prompt("chain");
    await waitFor(() => toolsStarted === 1);
    const factory = { id: "actor:factory", name: "factory", kind: "actor" as const };
    const ids = ["h1", "h2", "h3"].map((head) => main!.deliverAgent({
      from: factory, message: `PR head ${head}`, delivery: "followUp", data: { coalesceKey: "pi-fabric#7/state", head },
    }));
    main!.deliverAgent({ from: factory, message: "PR 8 merged", delivery: "followUp", data: { coalesceKey: "pi-fabric#8/state" } });
    expect(ids[2]).toMatchObject({ coalesced: true, replacedMessageId: ids[1]!.messageId, pendingFollowUps: 1 });
    await new Promise((resolve) => setTimeout(resolve, 150));       // due, but mid-tool
    gates.shift()!();                                               // the boundary
    while (toolsStarted < 2 || gates.length) {
      await waitFor(() => gates.length > 0 || toolsStarted >= 2, 2_000).catch(() => undefined);
      gates.shift()?.();
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    await run;
    const custom = session.messages.filter((message) => message.role === "custom") as Array<{ content: string; details: { id: string; items?: Array<{ id: string }> } }>;
    const text = custom.map((message) => message.content).join("\n");
    expect(text.match(/PR head h\d/g)).toEqual(["PR head h3"]);
    expect(text).toContain("PR 8 merged");
    const deliveredIds = custom.flatMap((message) => message.details.items?.map((item) => item.id) ?? [message.details.id]);
    expect(deliveredIds).toContain(ids[2]!.messageId);
    expect(deliveredIds).not.toContain(ids[0]!.messageId);
    expect(deliveredIds).not.toContain(ids[1]!.messageId);
    expect(main!.queueDepth().pendingFollowUps).toBe(0);
  });
});
