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
import { MainAgentController } from "../src/main-agent.js";
import { rootInboxSession } from "../src/topology/root-inbox.js";

// smarty-dev#1495: a followUp to a Main that chains turns arrived about an hour late.
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
  return { pi, sent, emit, handlers };
};
const context = (state: { idle: boolean; aborted?: boolean }) => ({
  isIdle: () => state.idle,
  hasPendingMessages: () => false,
  signal: { get aborted() { return state.aborted ?? false; } },
}) as unknown as ExtensionContext;
const from = (name: string) => ({ id: `agent-${name}`, name, kind: "agent" as const });
const toolTurn = { message: { role: "assistant", stopReason: "toolUse" } };

describe("Main followUp drain (unit)", () => {
  beforeEach(() => { vi.useFakeTimers(); vi.setSystemTime(new Date("2026-09-27T20:00:00Z")); });
  afterEach(() => { vi.useRealTimers(); });

  const setup = (flushMs = 120_000, idle = false) => {
    const fake = fakePi();
    const state = { idle, aborted: false };
    const ctx = context(state);
    const main = new MainAgentController(fake.pi, "session:root", true, "/tmp/project", "root");
    main.attachFollowUpDrain(ctx, flushMs);
    return { ...fake, state, ctx, main };
  };

  it("delivers to an idle Main as before, with a sent_at header", () => {
    const { main, sent } = setup(120_000, true);
    const result = main.deliverAgent({ from: from("a"), message: "hello", delivery: "followUp" });
    expect(result).toMatchObject({ queued: true, routed: "main", pendingFollowUps: 0, oldestAgeS: 0 });
    expect(sent).toHaveLength(1);
    expect(sent[0]!.options).toEqual({ deliverAs: "followUp", triggerTurn: true });
    expect(sent[0]!.message.content).toContain('delivery="followUp" sent_at="2026-09-27T20:00:00.000Z">');
    expect(sent[0]!.message.details).toMatchObject({ id: result.messageId, delivery: "followUp", sentAt: "2026-09-27T20:00:00.000Z" });
  });

  it("stamps a steer with sent_at and sends it at once, even to a busy Main", () => {
    const { main, sent } = setup();
    main.deliverAgent({ from: from("a"), message: "now", delivery: "steer" });
    expect(sent).toHaveLength(1);
    expect(sent[0]!.options.deliverAs).toBe("steer");
    expect(sent[0]!.message.content).toMatch(/^<fabric-agent-message from_name="a" from_id="agent-a" from_kind="agent" delivery="steer" sent_at="2026-09-27T20:00:00.000Z">/);
  });

  it("holds followUps for a busy Main and reports the queue depth to the sender", () => {
    const { main, sent } = setup();
    expect(main.deliverAgent({ from: from("a"), message: "one", delivery: "followUp" }))
      .toMatchObject({ pendingFollowUps: 1, oldestAgeS: 0 });
    vi.advanceTimersByTime(45_000);
    expect(main.deliverAgent({ from: from("b"), message: "two", delivery: "followUp" }))
      .toMatchObject({ pendingFollowUps: 2, oldestAgeS: 45 });
    expect(sent).toHaveLength(0);
  });

  it("never flushes mid-tool: nothing goes in until a tool boundary", () => {
    const { main, sent, emit, ctx } = setup();
    main.deliverAgent({ from: from("a"), message: "one", delivery: "followUp" });
    vi.advanceTimersByTime(10 * 60_000);                 // one long tool call, no turn_end
    expect(sent).toHaveLength(0);
    emit("turn_end", toolTurn, ctx);                      // the boundary
    expect(sent).toHaveLength(1);
    expect(sent[0]!.options).toEqual({ deliverAs: "steer", triggerTurn: true });
  });

  it("does not flush at a boundary before the followUp has waited flushMs", () => {
    const { main, sent, emit, ctx } = setup();
    main.deliverAgent({ from: from("a"), message: "one", delivery: "followUp" });
    vi.advanceTimersByTime(119_999);
    emit("turn_end", toolTurn, ctx);
    expect(sent).toHaveLength(0);
    vi.advanceTimersByTime(1);
    emit("turn_end", toolTurn, ctx);
    expect(sent).toHaveLength(1);
  });

  it("flushes all due followUps as ONE steer in send order, each marked followUp; later ones wait", () => {
    const { main, sent, emit, ctx } = setup();
    main.deliverAgent({ from: from("a"), message: "first", delivery: "followUp", data: { key: "k1" } });
    vi.advanceTimersByTime(10_000);
    main.deliverAgent({ from: from("b"), message: "second", delivery: "followUp" });
    vi.advanceTimersByTime(120_000);
    main.deliverAgent({ from: from("c"), message: "third", delivery: "followUp" });
    emit("turn_end", toolTurn, ctx);
    expect(sent).toHaveLength(1);
    const { message, options } = sent[0]!;
    expect(options.deliverAs).toBe("steer");
    expect(message.content.indexOf("first")).toBeLessThan(message.content.indexOf("second"));
    expect(message.content).not.toContain("third");
    expect(message.content.match(/delivery="followUp" sent_at=/g)).toHaveLength(2);
    expect(message.content).toContain('sent_at="2026-09-27T20:00:00.000Z"');
    expect(message.content).toContain('sent_at="2026-09-27T20:00:10.000Z"');
    expect(message.details.items.map((item: { from: { name: string } }) => item.from.name)).toEqual(["a", "b"]);
    expect(message.details).toMatchObject({ delivery: "followUp", flushed: true });
    // The root inbox still sees the work key a batched followUp carried.
    const entry = { type: "custom_message", customType: "pi-fabric-agent-message", details: message.details };
    expect(rootInboxSession([entry]).holdsSteer("agent-a", "k1")).toBe(true);
    expect(rootInboxSession([entry]).holdsSteer("agent-c", "k1")).toBe(false);
    expect(main.queueDepth()).toEqual({ pendingFollowUps: 1, oldestAgeS: 0 });
  });

  it("releases held followUps as a followUp when Main goes idle", () => {
    const { main, sent, emit, ctx, state } = setup();
    main.deliverAgent({ from: from("a"), message: "one", delivery: "followUp" });
    main.deliverAgent({ from: from("b"), message: "two", delivery: "followUp" });
    state.idle = true;
    emit("agent_settled", {}, ctx);
    expect(sent).toHaveLength(1);
    expect(sent[0]!.options).toEqual({ deliverAs: "followUp", triggerTurn: true });
    expect(sent[0]!.message.content.indexOf("one")).toBeLessThan(sent[0]!.message.content.indexOf("two"));
    expect(main.queueDepth().pendingFollowUps).toBe(0);
  });

  it("releases held followUps, then the new one, when Main is idle without a settle event", () => {
    const { main, sent, state } = setup();
    main.deliverAgent({ from: from("a"), message: "one", delivery: "followUp" });
    state.idle = true;                                    // e.g. compaction ended; no agent_settled
    main.deliverAgent({ from: from("b"), message: "two", delivery: "followUp" });
    expect(sent).toHaveLength(1);
    expect(sent[0]!.options).toEqual({ deliverAs: "followUp", triggerTurn: true });
    expect(sent[0]!.message.content.indexOf("one")).toBeLessThan(sent[0]!.message.content.indexOf("two"));
  });

  it("the fallback timer releases to a Main that went idle, and never flushes into a busy one", () => {
    const { main, sent, state } = setup();
    main.deliverAgent({ from: from("a"), message: "one", delivery: "followUp" });
    vi.advanceTimersByTime(120_000);
    expect(sent).toHaveLength(0);                         // busy: waits for a boundary
    state.idle = true;
    vi.advanceTimersByTime(120_000);
    expect(sent.map((entry) => entry.options)).toEqual([{ deliverAs: "followUp", triggerTurn: true }]);
  });

  it("does not flush after an aborted turn, and after Escape appends without starting a run", () => {
    const { main, sent, emit, ctx, state } = setup();
    main.deliverAgent({ from: from("a"), message: "one", delivery: "followUp" });
    vi.advanceTimersByTime(130_000);
    emit("turn_end", { message: { role: "assistant", stopReason: "aborted" } }, ctx);
    expect(sent).toHaveLength(0);
    state.idle = true;
    emit("agent_settled", {}, ctx);
    expect(sent).toHaveLength(1);
    expect(sent[0]!.options).toEqual({ deliverAs: "followUp", triggerTurn: false });
  });

  it("with flushMs 0 keeps Pi's own followUp queue", () => {
    const { main, sent, emit, ctx } = setup(0);
    const result = main.deliverAgent({ from: from("a"), message: "one", delivery: "followUp" });
    expect(sent).toHaveLength(1);
    expect(sent[0]!.options).toEqual({ deliverAs: "followUp", triggerTurn: true });
    expect(result).toMatchObject({ pendingFollowUps: 0, oldestAgeS: 0 });
    vi.advanceTimersByTime(130_000);
    emit("turn_end", toolTurn, ctx);
    expect(sent).toHaveLength(1);
  });

  it("passes a non-triggering followUp straight through, as before", () => {
    const { main, sent } = setup();
    main.deliverAgent({ from: from("a"), message: "fyi", delivery: "followUp", triggerTurn: false });
    expect(sent).toHaveLength(1);
    expect(sent[0]!.options).toEqual({ deliverAs: "followUp", triggerTurn: false });
  });

  it("hands held followUps to Pi's queue when the drain closes", () => {
    const { main, sent, handlers } = setup();
    main.deliverAgent({ from: from("a"), message: "one", delivery: "followUp" });
    main.closeFollowUpDrain();
    expect(sent.map((entry) => entry.options)).toEqual([{ deliverAs: "followUp", triggerTurn: true }]);
    expect([...handlers.values()].every((list) => list.length === 0)).toBe(true);
    main.deliverAgent({ from: from("b"), message: "two", delivery: "followUp" });
    expect(sent).toHaveLength(2);
  });
});

const roots: string[] = [];
const sessions: AgentSession[] = [];
afterEach(() => {
  for (const session of sessions.splice(0)) session.dispose();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const waitFor = async (predicate: () => boolean, timeoutMs = 5_000): Promise<void> => {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("timed out");
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
};

// The real Pi agent loop: Main chains tool calls; the followUp must arrive at a tool boundary,
// never inside a call, and behind a steer that was queued before it.
describe("Main followUp drain in a real Pi session", () => {
  it("delivers a due followUp at the next tool boundary, behind an earlier steer, while Main keeps working", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-drain-"));
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
        name: "drain",
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
            main.attachFollowUpDrain(ctx, 150);
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
      fauxAssistantMessage(fauxToolCall("work", {}), { stopReason: "toolUse" }),
      fauxAssistantMessage(fauxToolCall("work", {}), { stopReason: "toolUse" }),
      fauxAssistantMessage("all done"),
    ]);
    const kinds = () => session.messages.map((message) =>
      message.role === "custom" ? `custom:${(message as { details?: { delivery?: string; flushed?: boolean } }).details?.flushed ? "flushed" : "steer"}` : message.role);
    const run = session.prompt("chain");
    await waitFor(() => toolsStarted === 1);
    const result = main!.deliverAgent({ from: { id: "session:peer", name: "peer", kind: "main" }, message: "late news", delivery: "followUp" });
    expect(result.pendingFollowUps).toBe(1);
    await new Promise((resolve) => setTimeout(resolve, 200));       // due, but mid-tool
    main!.deliverAgent({ from: { id: "session:peer", name: "peer", kind: "main" }, message: "urgent", delivery: "steer" });
    expect(kinds()).not.toContain("custom:flushed");
    gates.shift()!();                                               // tool 1 ends: the boundary
    await waitFor(() => toolsStarted === 2);
    await waitFor(() => kinds().includes("custom:flushed"), 2_000).catch(() => undefined);
    // The steer queued first goes in first; the flushed batch follows at the same or next boundary.
    while (!kinds().includes("custom:flushed") && gates.length) {
      gates.shift()!();
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
    const order = kinds().filter((kind) => kind.startsWith("custom:"));
    expect(order).toEqual(["custom:steer", "custom:flushed"]);
    const flushedAt = kinds().indexOf("custom:flushed");
    expect(kinds().slice(flushedAt + 1)).toContain("assistant");  // Main kept working after reading it
    while (gates.length || toolsStarted < 4) {
      await waitFor(() => gates.length > 0, 2_000).catch(() => undefined);
      gates.shift()?.();
    }
    await run;
    const flushed = session.messages.find((message) => message.role === "custom" && (message as { details?: { flushed?: boolean } }).details?.flushed) as { content: string };
    expect(flushed.content).toMatch(/delivery="followUp" sent_at="\d{4}-\d\d-\d\dT[\d:.]+Z">\nlate news/);
    expect(main!.queueDepth().pendingFollowUps).toBe(0);
  });
});
