import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fauxAssistantMessage, fauxProvider, fauxToolCall } from "@earendil-works/pi-ai";
import {
  createAgentSession,
  DefaultResourceLoader,
  ModelRuntime,
  SessionManager,
  SettingsManager,
  type AgentSession,
  type ExtensionAPI,
  type ExtensionContext,
} from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { followUpDrainSupported } from "../src/host-compatibility.js";
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
    emit("agent_settled", { outcome: "completed" }, ctx);
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

  it("after a manual compaction or a branch summary: wakes Main when it completed, appends when it was cancelled or failed", () => {
    for (const [event, payload, triggerTurn] of [
      ["session_compact", { reason: "manual" }, true],
      ["session_tree", {}, true],
      ["session_compact_failed", { reason: "manual", aborted: true }, false],
      ["session_compact_failed", { reason: "manual", aborted: false, errorMessage: "x" }, false],
    ] as const) {
      const { main, sent, emit, ctx, state } = setup();
      main.deliverAgent({ from: from("a"), message: "one", delivery: "followUp" });   // held: compacting
      vi.advanceTimersByTime(10 * 60_000);
      expect(sent).toHaveLength(0);                     // no timer releases it
      state.idle = true;
      emit(event === "session_tree" ? "session_before_tree" : "session_before_compact", { reason: "manual", signal: new AbortController().signal }, ctx);
      emit(event, payload, ctx);
      vi.advanceTimersByTime(25);
      expect(sent.map((entry) => entry.options)).toEqual([{ deliverAs: "followUp", triggerTurn }]);
    }
  });

  // review/astra F2 on pi-fabric#102: Pi stays busy until every handler of the event finished.
  it("after a completed compaction, waits for Main to become idle, then wakes it once", () => {
    const { main, sent, emit, ctx, state } = setup();
    main.deliverAgent({ from: from("a"), message: "one", delivery: "followUp" });
    emit("session_before_compact", { reason: "manual", signal: new AbortController().signal }, ctx);
    emit("session_compact", { reason: "manual" }, ctx);   // still busy: another handler runs
    vi.advanceTimersByTime(5_000);
    expect(sent).toHaveLength(0);
    state.idle = true;
    vi.advanceTimersByTime(25);
    vi.advanceTimersByTime(1_000);
    expect(sent.map((entry) => entry.options)).toEqual([{ deliverAs: "followUp", triggerTurn: true }]);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("after a completed compaction, also wakes Main for a followUp that arrives before it is idle", () => {
    const { main, sent, emit, ctx, state } = setup();
    emit("session_before_compact", { reason: "manual", signal: new AbortController().signal }, ctx);
    emit("session_compact", { reason: "manual" }, ctx);   // nothing held yet
    vi.advanceTimersByTime(1_000);
    main.deliverAgent({ from: from("a"), message: "one", delivery: "followUp" });   // still busy
    expect(sent).toHaveLength(0);
    state.idle = true;
    vi.advanceTimersByTime(25);
    expect(sent.map((entry) => entry.options)).toEqual([{ deliverAs: "followUp", triggerTurn: true }]);
    expect(vi.getTimerCount()).toBe(0);
  });

  // review/astra round 6 on pi-fabric#102: Pi can be idle before it reports the cancel.
  it("never wakes Main for an operation that was cancelled, or whose abort signal Fabric never saw", () => {
    for (const kind of ["cancelled", "no signal"] as const) {
      const { main, sent, emit, ctx, state } = setup();
      const operation = new AbortController();
      if (kind === "cancelled") emit("session_before_compact", { reason: "manual", signal: operation.signal }, ctx);
      main.deliverAgent({ from: from("a"), message: "one", delivery: "followUp" });
      emit("session_compact", { reason: "manual" }, ctx);
      operation.abort();                                   // Escape while a later handler runs
      state.idle = true;                                   // idle before session_compact_failed
      vi.advanceTimersByTime(1_000);
      expect(sent).toHaveLength(0);
      expect(vi.getTimerCount()).toBe(0);
      expect(main.queueDepth().pendingFollowUps).toBe(1);
    }
  });

  it("ends the wait at idle when nothing arrived", () => {
    const { sent, emit, ctx, state } = setup();
    emit("session_before_compact", { reason: "manual", signal: new AbortController().signal }, ctx);
    emit("session_compact", { reason: "manual" }, ctx);
    state.idle = true;
    vi.advanceTimersByTime(25);
    expect(sent).toHaveLength(0);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("drops the pending wake when a later compaction is cancelled, a run starts, or the drain closes", () => {
    for (const end of ["cancelled", "run", "closed"] as const) {
      const { main, sent, emit, ctx, state } = setup();
      main.deliverAgent({ from: from("a"), message: "one", delivery: "followUp" });
      emit("session_before_compact", { reason: "manual", signal: new AbortController().signal }, ctx);
    emit("session_compact", { reason: "manual" }, ctx);
      if (end === "cancelled") { state.idle = true; emit("session_compact_failed", { reason: "manual", aborted: true }, ctx); }
      if (end === "run") emit("agent_start", {}, ctx);
      if (end === "closed") main.closeFollowUpDrain();
      state.idle = true;
      vi.advanceTimersByTime(1_000);
      // cancelled: appended without a run; run: left to that run; closed: handed to Pi's queue.
      expect(sent.map((entry) => entry.options.triggerTurn)).toEqual({ cancelled: [false], run: [], closed: [true] }[end]);
      expect(vi.getTimerCount()).toBe(0);
    }
  });

  it("leaves a compaction inside a run to that run's boundaries", () => {
    const { main, sent, emit, ctx, state } = setup();
    main.deliverAgent({ from: from("a"), message: "one", delivery: "followUp" });
    state.idle = true;
    emit("session_compact", { reason: "threshold" }, ctx);
    emit("session_compact_failed", { reason: "overflow", aborted: true }, ctx);
    vi.advanceTimersByTime(1);
    expect(sent).toHaveLength(0);
  });

  it("does not flush after an aborted turn, and after Escape appends without starting a run", () => {
    const { main, sent, emit, ctx, state } = setup();
    main.deliverAgent({ from: from("a"), message: "one", delivery: "followUp" });
    vi.advanceTimersByTime(130_000);
    emit("turn_end", { message: { role: "assistant", stopReason: "aborted" } }, ctx);
    expect(sent).toHaveLength(0);
    state.idle = true;
    emit("agent_settled", { outcome: "aborted" }, ctx);
    expect(sent).toHaveLength(1);
    expect(sent[0]!.options).toEqual({ deliverAs: "followUp", triggerTurn: false });
  });

  // review/astra F1 on pi-fabric#102: a cancel after the last turn ends normally.
  it("hands held followUps to Pi's queue at agent_before_settle, where Pi decides whether to continue", () => {
    const { main, sent, emit, ctx } = setup();
    main.deliverAgent({ from: from("a"), message: "one", delivery: "followUp" });
    main.deliverAgent({ from: from("b"), message: "two", delivery: "followUp" });
    emit("turn_end", { message: { role: "assistant", stopReason: "stop" } }, ctx);
    emit("agent_before_settle", { outcome: "completed" }, ctx);
    expect(sent.map((entry) => entry.options)).toEqual([{ deliverAs: "followUp", triggerTurn: true }]);
    expect(sent[0]!.message.content.indexOf("one")).toBeLessThan(sent[0]!.message.content.indexOf("two"));
    expect(main.queueDepth().pendingFollowUps).toBe(0);
  });

  it("keeps held followUps at agent_before_settle after an error or an aborted turn", () => {
    for (const [turn, outcome] of [["stop", "error"], ["stop", "aborted"], ["aborted", "completed"]] as const) {
      const { main, sent, emit, ctx } = setup();
      main.deliverAgent({ from: from("a"), message: "one", delivery: "followUp" });
      emit("turn_end", { message: { role: "assistant", stopReason: turn } }, ctx);
      emit("agent_before_settle", { outcome }, ctx);
      expect(sent).toHaveLength(0);
    }
  });

  it("starts a run at agent_settled only for an outcome of completed; no outcome or a cancel appends", () => {
    for (const [outcome, triggerTurn] of [["completed", true], ["aborted", false], ["error", false], [undefined, false]] as const) {
      const { main, sent, emit, ctx, state } = setup();
      main.deliverAgent({ from: from("a"), message: "one", delivery: "followUp" });
      emit("turn_end", { message: { role: "assistant", stopReason: "stop" } }, ctx);
      state.idle = true;
      emit("agent_settled", outcome === undefined ? {} : { outcome }, ctx);
      expect(sent.map((entry) => entry.options)).toEqual([{ deliverAs: "followUp", triggerTurn }]);
    }
  });

  it("runs only on a Pi host with agent_before_settle (0.87.0 or later)", () => {
    expect(followUpDrainSupported("0.86.9")).toBe(false);
    expect(followUpDrainSupported("0.80.6")).toBe(false);
    expect(followUpDrainSupported("0.87.0")).toBe(true);
    expect(followUpDrainSupported("0.88.2")).toBe(true);
    expect(followUpDrainSupported(undefined)).toBe(true);
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

// review/astra F1 on pi-fabric#102: the user cancels while Pi runs agent_before_settle after a
// normal last turn. No aborted turn_end happens; only the settle outcome says "aborted".
describe("Main followUp drain at a cancelled settle in a real Pi session", () => {
  const run = async (cancel: boolean, drainFirst = false) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-drain-settle-"));
    roots.push(root);
    const faux = fauxProvider();
    const modelRuntime = await ModelRuntime.create({ modelsPath: null, refreshOnCreate: false, authPath: path.join(root, "auth.json") });
    modelRuntime.registerNativeProvider(faux.provider);
    let main: MainAgentController | undefined;
    let releaseTool: (() => void) | undefined;
    let releaseSettle: (() => void) | undefined;
    let settling = 0;
    let agentStarts = 0;
    const loader = new DefaultResourceLoader({
      cwd: root, agentDir: path.join(root, "agent"), noSkills: true, noPromptTemplates: true, noThemes: true, noContextFiles: true,
      extensionFactories: [{
        name: "drain",
        factory: (pi: ExtensionAPI) => {
          pi.registerTool({
            name: "work", label: "work", description: "blocks until released", parameters: Type.Object({}),
            execute: async () => {
              await new Promise<void>((resolve) => { releaseTool = resolve; });
              return { content: [{ type: "text", text: "done" }], details: {} };
            },
          });
          pi.on("agent_start", () => { agentStarts++; });
          const blockSettle = () => pi.on("agent_before_settle", async () => {
            settling++;
            if (settling === 1) await new Promise<void>((resolve) => { releaseSettle = resolve; });
          });
          if (!drainFirst) blockSettle();
          pi.on("session_start", (_event, ctx) => {
            main = new MainAgentController(pi, "session:root", true, root, "root");
            main.attachFollowUpDrain(ctx, 60_000);
            if (drainFirst) blockSettle();
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
      fauxAssistantMessage("done"),
      fauxAssistantMessage("second run"),
    ]);
    const prompted = session.prompt("go");
    await waitFor(() => releaseTool !== undefined);
    expect(main!.deliverAgent({ from: { id: "session:peer", name: "peer", kind: "main" }, message: "later", delivery: "followUp" }))
      .toMatchObject({ pendingFollowUps: 1 });
    releaseTool!();
    await waitFor(() => releaseSettle !== undefined);   // the last turn ended normally
    const aborting = cancel ? session.abort() : Promise.resolve();
    releaseSettle!();
    await aborting;
    await prompted.catch(() => undefined);
    await waitFor(() => session.isIdle);
    await new Promise((resolve) => setTimeout(resolve, 300));
    await waitFor(() => session.isIdle);
    const delivered = () => session.messages.filter((message) => message.role === "custom" && (message as { customType?: string }).customType === "pi-fabric-agent-message");
    return { agentStarts, calls: faux.state.callCount, delivered: delivered(), main: main!, session, faux, later: delivered };
  };

  for (const drainFirst of [false, true]) {
    it(`keeps the followUp for the next run, without a new run, when the settle is cancelled (drain handler ${drainFirst ? "first" : "last"})`, async () => {
      const { agentStarts, calls, delivered, main, session, faux, later } = await run(true, drainFirst);
      expect(agentStarts).toBe(1);
      expect(calls).toBe(2);
      expect(delivered).toHaveLength(0);                 // in Pi's own followUp queue, as before the drain
      expect(main.queueDepth().pendingFollowUps).toBe(0);
      faux.appendResponses([fauxAssistantMessage("next"), fauxAssistantMessage("read it")]);
      await session.prompt("next task");
      expect(later()).toHaveLength(1);                   // the user's next run reads it
    });
  }

  it("counterexample: a completed settle delivers the held followUp in a new run", async () => {
    const { agentStarts, calls, delivered } = await run(false);
    expect(calls).toBe(3);                               // Pi continued the run for it
    expect(agentStarts).toBeLessThanOrEqual(2);
    expect(delivered).toHaveLength(1);
    expect((delivered[0] as { details?: { triggerTurn?: boolean } }).details?.triggerTurn).toBe(true);
  });
});

// review/astra round 3 on pi-fabric#102: manual /compact makes an idle Main busy without a run;
// the user cancels it. No turn_end and no settle follows, so nothing may start a run then.
describe("Main followUp drain around a manual compaction in a real Pi session", () => {
  const run = async (cancel: boolean, slowHandler = false, arriveLate = false, cancelInCompletion = false) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-drain-compact-"));
    roots.push(root);
    const faux = fauxProvider();
    const modelRuntime = await ModelRuntime.create({ modelsPath: null, refreshOnCreate: false, authPath: path.join(root, "auth.json") });
    modelRuntime.registerNativeProvider(faux.provider);
    let main: MainAgentController | undefined;
    let releaseCompaction: (() => void) | undefined;
    let releaseSlow: (() => void) | undefined;
    let releaseFailure: (() => void) | undefined;
    let agentStarts = 0;
    const loader = new DefaultResourceLoader({
      cwd: root, agentDir: path.join(root, "agent"), noSkills: true, noPromptTemplates: true, noThemes: true, noContextFiles: true,
      extensionFactories: [{
        name: "drain",
        factory: (pi: ExtensionAPI) => {
          pi.on("agent_start", () => { agentStarts++; });
          // Another extension's failure handler, before the drain's, that takes its time.
          if (cancelInCompletion) pi.on("session_compact_failed", () => new Promise<void>((resolve) => { releaseFailure = resolve; }));
          pi.on("session_before_compact", async (event) => {
            await new Promise<void>((resolve) => { releaseCompaction = resolve; });
            return { compaction: { summary: "summary", firstKeptEntryId: event.preparation.firstKeptEntryId, tokensBefore: event.preparation.tokensBefore } };
          });
          pi.on("session_start", (_event, ctx) => {
            main = new MainAgentController(pi, "session:root", true, root, "root");
            main.attachFollowUpDrain(ctx, 100);   // the wait below passes the flush deadline
            // Another extension's session_compact handler, after the drain's, still working.
            if (slowHandler) pi.on("session_compact", () => new Promise<void>((resolve) => { releaseSlow = resolve; }));
          });
        },
      }],
    });
    await loader.reload();
    const { session } = await createAgentSession({
      cwd: root, agentDir: path.join(root, "agent"), modelRuntime, model: faux.getModel(), resourceLoader: loader,
      sessionManager: SessionManager.inMemory(root), noTools: "all",
      settingsManager: SettingsManager.inMemory({ compaction: { keepRecentTokens: 1 } }),
    });
    sessions.push(session);
    await session.bindExtensions({});
    await waitFor(() => main !== undefined);
    faux.setResponses([fauxAssistantMessage("first answer"), fauxAssistantMessage("second answer")]);
    await session.prompt("first question");
    await session.prompt("second question");
    const startsBefore = agentStarts;
    const callsBefore = faux.state.callCount;
    faux.appendResponses([fauxAssistantMessage("read it")]);
    const compacting = session.compact().catch(() => undefined);   // the cancel rejects
    await waitFor(() => releaseCompaction !== undefined);
    expect(session.isIdle).toBe(false);
    const send = () => expect(main!.deliverAgent({ from: { id: "session:peer", name: "peer", kind: "main" }, message: "during compaction", delivery: "followUp" }))
      .toMatchObject({ pendingFollowUps: 1 });
    if (!arriveLate) send();
    if (cancel) session.abortCompaction();
    releaseCompaction!();
    if (slowHandler) {
      await waitFor(() => releaseSlow !== undefined);
      await new Promise((resolve) => setTimeout(resolve, 200));   // the drain's handler is long done
      expect(session.isIdle).toBe(false);
      if (arriveLate) send();                            // the first followUp, after the drain's handler
      if (cancelInCompletion) {
        session.abortCompaction();                       // Escape while the completion handler runs
        await new Promise((resolve) => setTimeout(resolve, 200));
        releaseSlow!();
        await new Promise((resolve) => setTimeout(resolve, 200));   // any failure handler still blocked
        expect(agentStarts - startsBefore).toBe(0);
        releaseFailure?.();
      }
      releaseSlow!();
    }
    await compacting;
    await new Promise((resolve) => setTimeout(resolve, 300));
    await waitFor(() => session.isIdle);
    const delivered = session.messages.filter((message) => message.role === "custom" && (message as { customType?: string }).customType === "pi-fabric-agent-message");
    releaseFailure?.();
    return { newRuns: agentStarts - startsBefore, newRequests: faux.state.callCount - callsBefore, delivered, main: main! };
  };

  it("keeps the followUp for the next run, without a new run, when the user cancels the compaction", async () => {
    const { newRuns, newRequests, delivered, main } = await run(true);
    expect(newRuns).toBe(0);
    expect(newRequests).toBe(0);
    expect(delivered).toHaveLength(1);
    expect((delivered[0] as { details?: { triggerTurn?: boolean } }).details?.triggerTurn).toBe(false);
    expect(main.queueDepth().pendingFollowUps).toBe(0);
  });

  it("after a completed compaction whose other handler is slow, the followUp still wakes Main once", async () => {
    const { newRuns, newRequests, delivered } = await run(false, true);
    expect(newRuns).toBe(1);
    expect(newRequests).toBe(1);
    expect(delivered).toHaveLength(1);
  });

  it("a first followUp arriving while a later completion handler still runs also wakes Main once", async () => {
    const { newRuns, newRequests, delivered, main } = await run(false, true, true);
    expect(newRuns).toBe(1);
    expect(newRequests).toBe(1);
    expect(delivered).toHaveLength(1);
    expect(main.queueDepth().pendingFollowUps).toBe(0);
  });

  it("a cancel while a completion handler runs starts no run, even with a slow failure handler", async () => {
    const { newRuns, newRequests, delivered, main } = await run(false, true, false, true);
    expect(newRuns).toBe(0);
    expect(newRequests).toBe(0);
    // Retained: held for the next run (Pi 0.87.0 completes it anyway), or appended without a run.
    const appended = delivered.filter((message) => (message as { details?: { triggerTurn?: boolean } }).details?.triggerTurn === false);
    expect(main.queueDepth().pendingFollowUps + appended.length).toBe(1);
    expect(delivered.length).toBe(appended.length);
  });

  it("counterexample: after a completed compaction the followUp wakes Main", async () => {
    const { newRuns, newRequests, delivered } = await run(false);
    expect(newRuns).toBe(1);
    expect(newRequests).toBe(1);
    expect(delivered).toHaveLength(1);
    expect((delivered[0] as { details?: { triggerTurn?: boolean } }).details?.triggerTurn).toBe(true);
  });
});
