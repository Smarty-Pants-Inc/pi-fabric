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

// Held followUps preserve every original, even with the same sender and data.coalesceKey.
// Keep FIFO, quota, restart and real-Pi delivery coverage at this data-safety boundary.
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

describe("Main followUp retention (unit)", () => {
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

  it("three same-key followUps while busy retain their arrival order and individual flush waits", () => {
    const { main, sent, emit, ctx } = setup();
    main.deliverAgent({ from: from("other"), message: "before", delivery: "followUp" });
    const first = main.deliverAgent(wake("h1"));
    vi.advanceTimersByTime(60_000);
    main.deliverAgent({ from: from("other"), message: "after", delivery: "followUp" });
    const second = main.deliverAgent(wake("h2"));
    const third = main.deliverAgent(wake("h3"));
    expect(first.coalesced).toBeUndefined();
    expect(second).toMatchObject({ queued: true, pendingFollowUps: 2 });
    expect(third).toMatchObject({ pendingFollowUps: 3, oldestAgeS: 60 });
    expect([second.coalesced, third.coalesced, third.replacedMessageId]).toEqual([undefined, undefined, undefined]);
    expect(main.queueDepth().pendingFollowUps).toBe(5);
    vi.advanceTimersByTime(60_000);
    emit("turn_end", toolTurn, ctx);
    expect(delivered(sent)).toEqual(["before", "head h1"]);   // later arrivals have waited 60 s only
    vi.advanceTimersByTime(60_000);
    emit("turn_end", toolTurn, ctx);
    expect(delivered(sent)).toEqual(["before", "head h1", "after", "head h2", "head h3"]);
    const items = sent.flatMap(entry => entry.message.details.items);
    expect(items.filter(item => item.from.id === from("factory").id).map(item => item.id))
      .toEqual([first.messageId, second.messageId, third.messageId]);
    expect(items.every(item => item.chain === undefined && item.replaces === undefined && item.replacedAt === undefined)).toBe(true);
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

  it("a 200-character key also retains both originals", () => {
    const { main } = setup();
    main.deliverAgent(wake("a", "k".repeat(200)));
    expect(main.deliverAgent(wake("b", "k".repeat(200)))).toMatchObject({ pendingFollowUps: 2 });
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

  it("every same-key original counts against the unchanged item and byte quotas", () => {
    const { main } = setup();
    for (let index = 0; index < FOLLOW_UP_LIMITS.senderItems; index++) main.deliverAgent(wake(`h${index}`, `k${index}`));
    expect(() => main.deliverAgent(wake("new", "k-new"))).toThrow(/per sender/);
    for (let round = 0; round < 3; round++) expect(() => main.deliverAgent(wake(`again${round}`, "k0"))).toThrow(/per sender/);
    const bytes = setup();
    const big = "x".repeat(100 * 1024);
    bytes.main.deliverAgent({ ...wake("a", "big"), message: big });
    bytes.main.deliverAgent({ ...wake("b", "other"), message: big });
    expect(() => bytes.main.deliverAgent({ ...wake("c", "big"), message: big })).toThrow(/per sender/);
    expect(bytes.main.queueDepth().pendingFollowUps).toBe(2);
  });

  it("journals every original; a failed append keeps all earlier items", () => {
    const { journal } = journalPath();
    const { main } = setup(journal);
    const first = main.deliverAgent(wake("h1"));
    const second = main.deliverAgent(wake("h2"));
    const items = JSON.parse(fs.readFileSync(journal, "utf8")).items as Array<{ id: string; chain?: string; generation?: number }>;
    expect(items.map((item) => item.id)).toEqual([first.messageId, second.messageId]);
    expect(items.every(item => item.chain === undefined && item.generation === undefined)).toBe(true);
    fs.rmSync(journal);
    fs.mkdirSync(journal);                               // the next write cannot replace a directory
    expect(() => main.deliverAgent(wake("h3"))).toThrow(/could not record/);
    expect(main.queueDepth().pendingFollowUps).toBe(2);
    fs.rmSync(journal, { recursive: true });
    expect(main.deliverAgent(wake("h4"))).toMatchObject({ pendingFollowUps: 3 });
  });

  it("after a restart with a duplicate journal entry, every original is delivered exactly once", () => {
    const { dir, journal } = journalPath();
    const firstPi = fakePi();
    const main = new MainAgentController(firstPi.pi, "session:root", true, dir, "root");
    main.attachFollowUpDrain(busyContext(), 120_000, journal);
    const first = main.deliverAgent(wake("h1"));
    const second = main.deliverAgent(wake("h2"));
    main.closeFollowUpDrain();                           // the process stops
    expect(firstPi.sent).toHaveLength(0);
    // A duplicate journal copy must not cause a second delivery of the same original.
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
    expect(again.next.sent.flatMap(entry => entry.message.details.items.map((item: { id: string }) => item.id)))
      .toEqual([first.messageId, second.messageId]);
    expect(delivered(again.next.sent)).toEqual(["head h1", "head h2"]);
    entries.push({ type: "custom_message", customType: "pi-fabric-agent-message", details: again.next.sent[0]!.message.details });
    again.controller.closeFollowUpDrain();
    const third = restart();
    expect(third.next.sent).toHaveLength(0);
    expect(fs.existsSync(journal)).toBe(false);
  });

  // A burst must survive restart in every handoff stage; duplicate journal copies never replay.
  for (const stage of ["held", "handed", "in the session"] as const) {
    it(`after a 34-message burst ${stage}, a restart retains every unconsumed original once`, () => {
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
        expect(delivered(firstPi.sent.slice(1))).toEqual(["plain", ...burst.map((_, index) => `head h${index + 1}`), "head x1", "head r1"]);
        if (stage === "in the session") {
          entries.push({ type: "custom_message", customType: "pi-fabric-agent-message", details: firstPi.sent[1]!.message.details });
          firstPi.emit("turn_end", toolTurn, busyContext(entries));           // confirmed: all originals leave the journal
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
      const expected = stage === "in the session" ? [] : ["plain", ...burst.map((_, index) => `head h${index + 1}`), "head x1", "head r1"];
      expect(delivered(next.sent)).toEqual(expected);
      const deliveredIds = next.sent.flatMap((entry) => entry.message.details.items?.map((item: { id: string }) => item.id) ?? [entry.message.details.id]);
      expect(deliveredIds.filter(id => id === burst[0]!.messageId)).toHaveLength(stage === "in the session" ? 0 : 1);
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
describe("Main followUp retention in a real Pi session", () => {
  it("every same-key wake during a tool call reaches Main in arrival order with its sender identity", async () => {
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
    const reviewer = { id: "actor:reviewer", name: "reviewer", kind: "actor" as const };
    ids.push(main!.deliverAgent({ from: reviewer, message: "PR 7 reviewed", delivery: "followUp", data: { coalesceKey: "pi-fabric#7/state" } }));
    ids.push(main!.deliverAgent({ from: factory, message: "PR 8 merged", delivery: "followUp", data: { coalesceKey: "pi-fabric#8/state" } }));
    expect(ids.every(result => result.coalesced === undefined && result.replacedMessageId === undefined)).toBe(true);
    expect(main!.queueDepth().pendingFollowUps).toBe(5);
    await new Promise((resolve) => setTimeout(resolve, 150));       // due, but mid-tool
    gates.shift()!();                                               // the boundary
    while (toolsStarted < 2 || gates.length) {
      await waitFor(() => gates.length > 0 || toolsStarted >= 2, 2_000).catch(() => undefined);
      gates.shift()?.();
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    await run;
    type Item = { id: string; from: typeof factory };
    const custom = session.messages.filter((message) => message.role === "custom") as Array<{ content: string; details: Item & { items?: Item[] } }>;
    const text = custom.map((message) => message.content).join("\n");
    expect(text.match(/PR head h\d|PR 7 reviewed|PR 8 merged/g)).toEqual(["PR head h1", "PR head h2", "PR head h3", "PR 7 reviewed", "PR 8 merged"]);
    const originals = custom.flatMap(message => message.details.items ?? [message.details]);
    expect(originals.map(item => item.id)).toEqual(ids.map(result => result.messageId));
    expect(originals.map(item => item.from)).toEqual([factory, factory, factory, reviewer, factory]);
    expect(main!.queueDepth().pendingFollowUps).toBe(0);
  });
});
