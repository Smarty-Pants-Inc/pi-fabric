import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { fauxAssistantMessage, fauxProvider } from "@earendil-works/pi-ai";
import { createAgentSession, DefaultResourceLoader, ModelRuntime, SessionManager, SettingsManager,
  type ExtensionAPI, type ExtensionContext } from "@earendil-works/pi-coding-agent";
import { MainAgentController, type HeldAgentMessage } from "../src/main-agent.js";
import { diagnostics } from "../src/core/diagnostics.js";

// Keep reason: data safety at the actual Main journal/native handoff boundary. These tests
// deliberately KEEP all temporary evidence: no fixture, journal or JSONL is removed.
const controllers: MainAgentController[] = [];
const closes: Array<() => Promise<void>> = [];
const journals = new Set<string>();
beforeEach(() => {
  const remove = fs.rmSync;
  vi.spyOn(fs, "rmSync").mockImplementation((file, options) => {
    // Only this suite's payload journals are retained. Other product fs semantics stay real.
    // Empty-journal removal itself is recorded-call evidence, not a deletion-path test.
    if (typeof file === "string" && journals.has(file)) {
      fs.appendFileSync(`${file}.retained-removals.jsonl`, JSON.stringify({ file, options }) + "\n");
      return;
    }
    remove(file, options);
  });
});
afterEach(async () => {
  for (const close of closes.splice(0).reverse()) await close();
  for (const main of controllers.splice(0)) main.closeFollowUpDrain();
  vi.useRealTimers();
  vi.restoreAllMocks();
});
const from = { id: "session:root", name: "Main", kind: "main" as const };
const peer = { id: "session:peer", name: "peer", kind: "main" as const };
const fixture = (flushMs = 60_000, predicate: (() => boolean) | null = () => false, journal?: string) => {
  const root = journal ? path.dirname(journal) : fs.mkdtempSync(path.join(os.tmpdir(), "main-recovery-hold-"));
  journal ??= path.join(root, "journal.json");
  journals.add(journal);
  const entries: unknown[] = [];
  const state = { idle: true, preflight: false };
  const handlers = new Map<string, Array<(event: any, ctx: ExtensionContext) => unknown>>();
  const sent: Array<{ message: any; options: any }> = [];
  const users: Array<{ text: string; options: any }> = [];
  const pi = {
    on(name: string, handler: (event: any, ctx: ExtensionContext) => unknown) {
      handlers.set(name, [...(handlers.get(name) ?? []), handler]);
      return () => handlers.set(name, (handlers.get(name) ?? []).filter(fn => fn !== handler));
    },
    sendMessage: vi.fn((message: any, options: any) => { sent.push({ message, options }); }),
    sendUserMessage: vi.fn((text: string, options: any) => { users.push({ text, options }); }),
    getThinkingLevel: () => "off",
  } as unknown as ExtensionAPI;
  const ctx = { isIdle: () => state.idle, isSettling: () => false, isPromptPending: () => state.preflight,
    hasPendingMessages: () => false, signal: new AbortController().signal, abort: vi.fn(), compact: vi.fn(),
    sessionManager: { getEntries: () => entries },
  } as unknown as ExtensionContext;
  const main = new MainAgentController(pi, from.id, true, root, "root", true, undefined, {}, undefined, root, true);
  controllers.push(main);
  if (predicate) main.setRecoveryDrainPredicate(predicate); // MUST precede journal open/replay
  const bind = () => main.attachFollowUpDrain(ctx, flushMs, journal);
  const emit = (name: string, event: any = {}) => { for (const fn of [...(handlers.get(name) ?? [])]) fn(event, ctx); };
  const rows = (): HeldAgentMessage[] => JSON.parse(fs.readFileSync(journal!, "utf8")).items;
  const admit = (id: string, delivery: "steer" | "followUp" | "nextTurn" = "followUp", extra = {}) => main.deliverAgent({
    from: peer, verification: "mesh", message: id, delivery, deliveryId: id, ...extra,
  });
  return { root, journal, entries, state, sent, users, pi, ctx, main, bind, emit, rows, admit };
};
const boundary = { outcome: "completed", message: { stopReason: "toolUse" }, context: { pendingMessages: [] } };
const persisted = (id: string) => ({ type: "custom_message", customType: "pi-fabric-agent-message", details: { id } });

describe("Main live recovery drain", () => {
  it.each([0, 60_000])("parks all policies and composer input durably, opens once in original order (flush=%s)", flush => {
    let open = false;
    const f = fixture(flush, () => open); f.bind();
    const deadlineMs = Date.now() + 42_000;
    const a = f.admit("first", "followUp", { data: { deadlineMs }, outcomeSend: { eventId: "first", from: peer.id, to: from.id, mode: "followUp" } });
    const user = f.main.deliverUser("literal user", "steer");
    const b = f.admit("priority", "steer", { from, priority: "interrupt" });
    const c = f.admit("passive", "nextTurn", { triggerTurn: false });
    expect([a, user, b, c].map(row => row.triggered)).toEqual([false, false, false, false]);
    expect(f.rows().map(row => row.id)).toEqual([a.messageId, user.messageId, b.messageId, c.messageId]);
    expect(f.rows()[0]!.data).toEqual({ deadlineMs });
    expect(f.admit("first")).toMatchObject({ duplicate: true, messageId: a.messageId, triggered: false });
    for (const event of ["input", "agent_start", "turn_end", "agent_before_settle", "agent_settled", "session_tree", "session_compact", "session_ready"]) {
      f.emit(event, event === "input" ? { source: "interactive" } : boundary);
    }
    f.main.flushHeldAtNextBoundary(); f.emit("turn_end", boundary); f.main.confirmInbox();
    expect(f.sent).toEqual([]); expect(f.users).toEqual([]); expect(f.ctx.abort).not.toHaveBeenCalled();
    expect(fs.existsSync(`${f.journal}.delivered`) && JSON.parse(fs.readFileSync(`${f.journal}.delivered`, "utf8")).ids.length).toBeFalsy();
    const order: string[] = [];
    vi.mocked(f.pi.sendMessage).mockImplementation((message: any, options: any) => { order.push(message.details.id); f.sent.push({ message, options }); });
    vi.mocked(f.pi.sendUserMessage).mockImplementation((text: any, options: any) => { order.push(user.messageId); f.users.push({ text, options }); });
    open = true; f.main.reconsiderRecoveryDrain(); f.main.reconsiderRecoveryDrain();
    expect(order).toEqual([a.messageId, user.messageId, b.messageId, c.messageId]);
    expect(f.users).toEqual([{ text: "literal user", options: { deliverAs: "steer" } }]);
    expect(f.sent.map(row => row.options)).toEqual([
      { deliverAs: "followUp", triggerTurn: true }, { deliverAs: "steer", triggerTurn: true }, { deliverAs: "nextTurn", triggerTurn: false },
    ]);
    expect(f.rows()[0]!.data).toEqual({ deadlineMs });
    expect(f.ctx.abort).not.toHaveBeenCalled();
  });

  it("loads retained journal before startup release but defers native receipt consumption until open", () => {
    let open = false;
    const f = fixture(60_000, () => open);
    const item = { id: "already-native", from: peer, message: "retained", sentAt: 11, deliveryId: "stable", handed: true,
      outcomeSend: { eventId: "stable", from: peer.id, to: from.id, mode: "followUp" } };
    fs.writeFileSync(f.journal, JSON.stringify({ version: 1, items: [item] }));
    f.entries.push(persisted(item.id)); f.bind();
    f.main.confirmInbox(); f.emit("turn_end", boundary); f.emit("agent_settled", boundary);
    expect(f.sent).toEqual([]); expect(f.rows()).toHaveLength(1);
    expect(fs.existsSync(`${f.journal}.delivered`)).toBe(false);
    expect(fs.existsSync(path.join(f.root, "delivery-outcomes"))).toBe(false);
    open = true; f.main.reconsiderRecoveryDrain();
    expect(f.sent).toEqual([]);
    expect(JSON.parse(fs.readFileSync(`${f.journal}.delivered`, "utf8")).ids).toEqual(["stable"]);
  });

  it("defers legacy superseded-chain consumption without replaying an obsolete item on open", () => {
    let open = false;
    const f = fixture(60_000, () => open);
    const first = { id: "obsolete", from: peer, sentAt: 1, message: "old", deliveryId: "old-key", chain: "chain", generation: 0 };
    const latest = { ...first, id: "latest", sentAt: 2, message: "new", deliveryId: "new-key", generation: 1 };
    fs.writeFileSync(f.journal, JSON.stringify({ version: 1, items: [first, latest] }));
    f.bind(); f.main.confirmInbox();
    expect(f.sent).toEqual([]); expect(f.rows()).toHaveLength(2);
    expect(fs.existsSync(`${f.journal}.delivered`)).toBe(false);
    open = true; f.main.reconsiderRecoveryDrain(); f.main.reconsiderRecoveryDrain();
    expect(f.sent.map(row => row.message.details.id)).toEqual([latest.id]);
    expect(f.rows().map(row => row.id)).toEqual([latest.id]);
    expect(JSON.parse(fs.readFileSync(`${f.journal}.delivered`, "utf8")).ids).toEqual([first.deliveryId]);
  });

  it("replays an unhanded startup item once, retaining its original identity, timestamp and deadline", () => {
    let open = false;
    const f = fixture(0, () => open);
    const item = { id: "native-id", from: peer, message: "original", sentAt: 100, deliveryId: "original-delivery", data: { deadlineMs: 999 }, deliverAs: "steer", triggerTurn: true };
    fs.writeFileSync(f.journal, JSON.stringify({ version: 1, items: [item] }));
    f.bind(); expect(f.sent).toEqual([]);
    open = true; f.main.reconsiderRecoveryDrain(); f.main.reconsiderRecoveryDrain();
    expect(f.sent).toHaveLength(1);
    expect(f.sent[0]!.message.details).toMatchObject({ id: item.id, deliveryId: item.deliveryId, data: item.data, sentAt: new Date(100).toISOString() });
  });

  it("does not release a newer closed epoch on stale reconsider, halt, input, reload or rebind", () => {
    let open = false;
    const f = fixture(60_000, () => open); f.bind(); const receipt = f.admit("epoch-1");
    const staleWake = () => f.main.reconsiderRecoveryDrain();
    open = true; f.main.setRecoveryDrainPredicate(() => false); staleWake();
    f.main.halt(); f.emit("input", { source: "interactive" }); f.main.prepareReload(); f.bind();
    f.emit("agent_start"); f.emit("agent_settled", boundary); staleWake();
    expect(f.sent).toEqual([]); expect(f.rows().map(row => row.id)).toEqual([receipt.messageId]);
    f.main.setRecoveryDrainPredicate(() => true); f.main.reconsiderRecoveryDrain();
    expect(f.sent).toHaveLength(1);
  });

  it("guards due flush and authorized priority interruption during an active tool", () => {
    vi.useFakeTimers();
    let open = true;
    const f = fixture(10, () => open); f.bind(); f.state.idle = false;
    f.emit("tool_execution_start", { toolCallId: "tool" });
    f.admit("held-followup"); open = false;
    f.admit("interrupt", "steer", { from, priority: "interrupt" });
    vi.advanceTimersByTime(100); f.main.flushHeldAtNextBoundary(); f.emit("turn_end", boundary); f.emit("agent_settled", boundary);
    expect(f.sent).toEqual([]); expect(f.ctx.abort).not.toHaveBeenCalled(); expect(f.rows()).toHaveLength(2);
  });

  it("refuses storage-only acceptance without a journal and suppresses close's legacy native fallback", () => {
    const f = fixture();
    const legacy = new MainAgentController(f.pi, from.id, true, f.root, "root"); controllers.push(legacy);
    f.state.idle = false; legacy.attachFollowUpDrain(f.ctx, 60_000);
    legacy.deliverAgent({ from: peer, message: "legacy", delivery: "followUp" });
    legacy.setRecoveryDrainPredicate(() => false);
    expect(() => legacy.deliverAgent({ from: peer, message: "new", delivery: "steer" })).toThrow(/without an open journal/);
    expect(() => legacy.deliverUser("new user", "steer")).toThrow(/without an open journal/);
    legacy.closeFollowUpDrain();
    expect(f.sent).toEqual([]); expect(legacy.drainSnapshot().held).toBe(1);
  });

  it("keeps ordinary no-predicate delivery, priority and flush=0 behavior", () => {
    const f = fixture(0, null); // Same construction/bind as held cases; no predicate installed.
    const ordinary = f.main; f.bind();
    expect(ordinary.deliverAgent({ from: peer, message: "ordinary", delivery: "followUp" }).triggered).toBe(true);
    ordinary.deliverUser("ordinary user", "steer");
    f.state.idle = false; f.emit("tool_execution_start", { toolCallId: "tool" });
    ordinary.deliverAgent({ from, verification: "mesh", message: "HOLD", delivery: "steer", priority: "interrupt" });
    expect(f.ctx.abort).toHaveBeenCalledOnce(); expect(f.users).toHaveLength(1); expect(f.sent).toHaveLength(1);
  });

  it("fails closed with a diagnostic when the predicate throws", () => {
    const warning = vi.spyOn(diagnostics, "warn").mockImplementation(() => {});
    const f = fixture(60_000, () => { throw new Error("epoch read failed"); }); f.bind(); f.admit("retained");
    f.main.reconsiderRecoveryDrain(); f.main.confirmInbox(); f.emit("turn_end", boundary);
    expect(f.sent).toEqual([]); expect(f.rows()).toHaveLength(1);
    expect(warning).toHaveBeenCalledWith("src/main-agent.ts", expect.stringContaining("predicate failed"), expect.any(Error));
  });

  it("does not retry an ambiguous native handoff merely because recovery closes and opens", () => {
    let open = false;
    const f = fixture(60_000, () => open); f.bind(); const receipt = f.admit("ambiguous");
    vi.mocked(f.pi.sendMessage).mockImplementation(() => { throw new Error("queued, response lost"); });
    open = true; f.main.reconsiderRecoveryDrain();
    open = false; f.main.reconsiderRecoveryDrain(); f.bind();
    open = true; f.main.reconsiderRecoveryDrain(); f.emit("turn_end", boundary);
    expect(f.pi.sendMessage).toHaveBeenCalledOnce();
    expect(f.rows()[0]).toMatchObject({ id: receipt.messageId, handed: true });
  });

  it("rechecks the live predicate between native handoffs and keeps the remaining item pending", () => {
    let open = false;
    const f = fixture(60_000, () => open); f.bind();
    const first = f.admit("first", "steer"); const second = f.admit("second", "steer");
    vi.mocked(f.pi.sendMessage).mockImplementationOnce((message: any, options: any) => {
      f.sent.push({ message, options }); open = false;
    });
    open = true; f.main.reconsiderRecoveryDrain();
    expect(f.sent.map(row => row.message.details.id)).toEqual([first.messageId]);
    expect(f.main.drainSnapshot().heldIds).toEqual([second.messageId]);
    open = true; f.main.reconsiderRecoveryDrain(); f.main.reconsiderRecoveryDrain();
    expect(f.sent.map(row => row.message.details.id)).toEqual([first.messageId, second.messageId]);
  });

  it("leaves handed composer input pending across rebind instead of fabricating a receipt or retry", () => {
    let open = false;
    const f = fixture(60_000, () => open); f.bind(); const receipt = f.main.deliverUser("user retained", "followUp");
    open = true; f.main.reconsiderRecoveryDrain(); f.bind(); f.emit("turn_end", boundary); f.main.reconsiderRecoveryDrain();
    expect(f.users).toHaveLength(1); expect(f.rows()[0]).toMatchObject({ id: receipt.messageId, handed: true });
    expect(fs.existsSync(`${f.journal}.delivered`)).toBe(false);
  });

  it("blocks provider recovery compaction and retry wakes, including an already-armed edge", () => {
    vi.useFakeTimers();
    let open = true;
    const f = fixture(60_000, () => open); f.bind();
    f.emit("turn_end", { message: { stopReason: "error", errorMessage: "400 invalid_request_error: compaction_affinity_missing" } });
    open = false; vi.advanceTimersByTime(1_000); f.emit("agent_end"); f.emit("session_compact", { reason: "manual" });
    f.main.reconsiderRecoveryDrain(); vi.advanceTimersByTime(1_000);
    expect(f.ctx.compact).not.toHaveBeenCalled(); expect(f.sent).toEqual([]);
  });

  it("blocks an already-armed provider retry deadline until explicit reopening", () => {
    vi.useFakeTimers();
    let open = true;
    const f = fixture(60_000, () => open); f.bind();
    f.emit("agent_start");
    f.emit("turn_end", { message: { stopReason: "error", errorMessage: "503 upstream connect error" } });
    f.emit("agent_before_settle", { outcome: "error" }); f.emit("agent_settled", { outcome: "error" });
    const retry = f.admit("provider-retry");
    expect(retry).toMatchObject({ queued: true, triggered: false });
    expect(vi.getTimerCount()).toBeGreaterThan(0);
    open = false; vi.advanceTimersByTime(120_000); f.main.reconsiderRecoveryDrain();
    expect(f.sent).toHaveLength(0);
    // The same index also stores provider state; its presence is not consumption.
    expect(JSON.parse(fs.readFileSync(`${f.journal}.delivered`, "utf8")).ids).toEqual([]);
    open = true; f.main.reconsiderRecoveryDrain();
    expect(f.sent.map(row => row.message.details.id)).toEqual([retry.messageId]);
  });

  it("holds a preflight wake that was armed before the epoch closed", () => {
    vi.useFakeTimers();
    let open = true;
    const f = fixture(60_000, () => open); f.bind(); f.state.preflight = true; f.admit("preflight");
    open = false; f.state.preflight = false; vi.advanceTimersByTime(100);
    expect(f.sent).toEqual([]); expect(f.rows()).toHaveLength(1);
    open = true; f.main.reconsiderRecoveryDrain(); expect(f.sent).toHaveLength(1);
  });

  it("accepts a rotation/records carrier with its original id without waking Main", () => {
    const f = fixture(); f.bind();
    f.main.receiveInboxItem({ id: "original-carrier", from: peer, sentAt: 123, message: "records wake", deliveryId: "records-key",
      inboxKey: "work/key", inboxReceipts: ["original-event"], data: { deadlineMs: 456 } });
    expect(f.sent).toEqual([]);
    expect(f.rows()[0]).toMatchObject({ id: "original-carrier", sentAt: 123, inboxReceipts: ["original-event"], data: { deadlineMs: 456 } });
  });

  it("uses the real native Main harness: no startup/input dispatch before one owner reconsider", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "main-recovery-native-"));
    const faux = fauxProvider(); faux.setResponses([fauxAssistantMessage("received"), fauxAssistantMessage("received next")]);
    const runtime = await ModelRuntime.create({ modelsPath: null, refreshOnCreate: false, authPath: path.join(root, "absent-auth.json") });
    runtime.registerNativeProvider(faux.provider);
    const settings = SettingsManager.inMemory({ compaction: { enabled: false }, retry: { enabled: false } });
    let open = false;
    let main!: MainAgentController;
    const journal = path.join(root, "main.json");
    journals.add(journal);
    fs.writeFileSync(journal, JSON.stringify({ version: 1, items: [{ id: "retained-native-id", from: peer,
      sentAt: 123, message: "startup replay", deliveryId: "retained-delivery", deliverAs: "followUp", triggerTurn: true }] }));
    const loader = new DefaultResourceLoader({ cwd: root, agentDir: path.join(root, "agent"), settingsManager: settings,
      noExtensions: true, noSkills: true, noPromptTemplates: true, noThemes: true, noContextFiles: true,
      extensionFactories: [(pi: ExtensionAPI) => {
        pi.on("session_start", (_event, ctx) => {
          main = new MainAgentController(pi, from.id, true, root, "root"); controllers.push(main);
          main.setRecoveryDrainPredicate(() => open); main.attachFollowUpDrain(ctx, 60_000, journal);
        });
      }],
    });
    await loader.reload();
    const { session } = await createAgentSession({ cwd: root, agentDir: path.join(root, "agent"), modelRuntime: runtime,
      model: faux.getModel(), resourceLoader: loader, settingsManager: settings, sessionManager: SessionManager.inMemory(root), tools: [] });
    closes.push(async () => { await session.abort(); main.closeFollowUpDrain(); session.dispose(); });
    await session.bindExtensions({});
    const receipt = main.deliverAgent({ from: peer, message: "retained real input", delivery: "followUp", deliveryId: "native-original" });
    expect(session.messages).toEqual([]); expect(session.isStreaming).toBe(false);
    expect(JSON.parse(fs.readFileSync(journal, "utf8")).items.map((item: HeldAgentMessage) => item.id)).toEqual(["retained-native-id", receipt.messageId]);
    expect(fs.existsSync(`${journal}.delivered`)).toBe(false);
    open = true; main.reconsiderRecoveryDrain(); main.reconsiderRecoveryDrain();
    await vi.waitFor(() => expect(session.messages.some(message => message.role === "assistant")).toBe(true), { timeout: 5_000 });
    await session.waitForIdle();
    expect(session.messages.filter(message => message.role === "custom" && message.customType === "pi-fabric-agent-message")
      .map(message => (message as { details: { id: string } }).details.id)).toEqual(["retained-native-id", receipt.messageId]);
  }, 10_000);
});
