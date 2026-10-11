import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { MainAgentController, type FabricMainProviderDeadEvent } from "../src/main-agent.js";

// smarty-dev#8742: a non-retryable provider error is never provider-backoff.
type Handler = (event: unknown, ctx: ExtensionContext) => unknown;
type CompactOptions = { onComplete?: (result: unknown) => void; onError?: (error: Error) => void };
const affinity = `409 ${JSON.stringify({ type: "error", error: { type: "invalid_request_error", message: "compaction_affinity_missing" } })}`;
const errorTurn = (errorMessage: string) => ({ message: { role: "assistant", stopReason: "error", errorMessage } });
const okTurn = { message: { role: "assistant", stopReason: "stop" } };
const from = (name: string) => ({ id: `agent-${name}`, name, kind: "agent" as const });

const setup = (flushMs = 120_000) => {
  const handlers = new Map<string, Handler[]>();
  const sent: Array<{ message: { customType: string; content: string }; options: { deliverAs: string; triggerTurn: boolean } }> = [];
  const pi = {
    on: (name: string, fn: Handler) => {
      handlers.set(name, [...(handlers.get(name) ?? []), fn]);
      return () => handlers.set(name, (handlers.get(name) ?? []).filter((h) => h !== fn));
    },
    sendMessage: vi.fn((message, options) => { sent.push({ message, options }); }),
    sendUserMessage: vi.fn(),
    getThinkingLevel: () => "off",
  } as unknown as ExtensionAPI;
  const state = { idle: false };
  const compactions: CompactOptions[] = [];
  const notify = vi.fn();
  const ctx = {
    isIdle: () => state.idle,
    hasPendingMessages: () => false,
    signal: { aborted: false },
    hasUI: true,
    ui: { notify },
    compact: vi.fn((options: CompactOptions) => { compactions.push(options); }),
  } as unknown as ExtensionContext;
  const alarms: FabricMainProviderDeadEvent[] = [];
  const released: unknown[] = [];
  const main = new MainAgentController(pi, "session:root", true, "/tmp/project", "root", true,
    (event) => { released.push(event); }, (event) => { alarms.push(event); });
  main.attachFollowUpDrain(ctx, flushMs);
  const emit = (name: string, event: unknown) => { for (const handler of handlers.get(name) ?? []) handler(event, ctx); };
  /** The failed run ends: Pi settles with an error outcome and goes idle. */
  const settleError = () => { state.idle = true; emit("agent_before_settle", { outcome: "error" }); emit("agent_settled", { outcome: "error" }); };
  const compactOk = (index = 0) => {
    emit("session_before_compact", { reason: "manual", signal: new AbortController().signal });
    emit("session_compact", { reason: "manual" });
    compactions[index]!.onComplete?.({});
  };
  return { main, sent, state, ctx, emit, compactions, notify, alarms, released, settleError, compactOk };
};

beforeEach(() => { vi.useFakeTimers(); vi.setSystemTime(new Date("2026-10-11T08:00:00Z")); });
afterEach(() => { vi.clearAllTimers(); vi.useRealTimers(); });

describe.each([120_000, 0])("Main non-retryable provider recovery (flushMs=%s, smarty-dev#8742)", (flushMs) => {
  it("compacts once at idle, releases the held work once, and recovers on success", () => {
    const h = setup(flushMs);
    h.emit("turn_end", errorTurn(affinity));
    expect(h.main.providerDeadState).toMatchObject({ phase: "recovering", status: 409 });
    const held = h.main.deliverAgent({ from: from("lead"), message: "status?", delivery: "followUp" });
    expect(held.triggered).toBe(false);
    expect(held.reason).toBe("provider-dead (non-retryable: 409 invalid_request_error: compaction_affinity_missing); compacting once to recover");
    // A long provider message never reaches the receipt (security review P1); it stays well under
    // the control plane's 200-character reason limit.
    const long = setup(flushMs);
    long.emit("turn_end", errorTurn(`409 ${JSON.stringify({ type: "error", error: { type: "invalid_request_error", message: `compaction_affinity_missing ${"z".repeat(300)}` } })}`));
    const longReason = long.main.deliverAgent({ from: from("lead"), message: "x", delivery: "followUp" }).reason!;
    expect(longReason).toBe("provider-dead (non-retryable: 409 invalid_request_error: compaction_affinity_missing); compacting once to recover");
    long.main.closeFollowUpDrain();
    expect(h.compactions).toHaveLength(0);                // never inside the failing hook
    h.settleError();
    vi.advanceTimersByTime(0);
    expect(h.compactions).toHaveLength(1);
    expect(h.main.providerDeadState?.phase).toBe("compacting");
    expect(h.sent).toHaveLength(0);
    h.compactOk();
    vi.advanceTimersByTime(25);
    // Exactly one triggering attempt carries the held work.
    expect(h.sent).toHaveLength(1);
    expect(h.sent[0]!.options).toEqual({ deliverAs: "followUp", triggerTurn: true });
    expect(h.sent[0]!.message.content).toContain("status?");
    h.state.idle = false;
    const during = h.main.deliverAgent({ from: from("peer"), message: "later", delivery: "followUp" });
    expect(during.triggered).toBe(false);
    expect(during.reason).toMatch(/^provider-dead \(non-retryable: .*\); retry after compaction in flight$/);
    h.emit("agent_start", {});
    h.emit("turn_end", okTurn);
    expect(h.main.providerDeadState).toBeUndefined();
    expect(h.notify).toHaveBeenCalledWith(expect.stringContaining("recovered after compaction"), "info");
    h.state.idle = true;
    h.emit("agent_before_settle", { outcome: "completed" });
    h.emit("agent_settled", { outcome: "completed" });
    vi.advanceTimersByTime(60 * 60_000);
    expect(h.sent.map(entry => entry.message.content).join("\n")).toContain("later");
    expect(h.compactions).toHaveLength(1);
    expect(h.alarms).toHaveLength(0);
    h.main.closeFollowUpDrain();
    expect(vi.getTimerCount()).toBe(0);
  });

  it("alarms once when the recovery compaction fails, and holds work without a backoff loop", () => {
    const h = setup(flushMs);
    h.emit("turn_end", errorTurn(affinity));
    h.main.deliverAgent({ from: from("lead"), message: "busy work", delivery: "followUp" });
    h.settleError();
    vi.advanceTimersByTime(0);
    expect(h.compactions).toHaveLength(1);
    h.emit("session_compact_failed", { reason: "manual", aborted: false, willRetry: false, errorMessage: "Compaction failed: summarizer 409" });
    h.compactions[0]!.onError?.(new Error("Compaction failed: summarizer 409"));
    expect(h.alarms).toHaveLength(1);
    expect(h.alarms[0]).toEqual({ kind: "non-retryable", cause: "compaction-failed", status: 409,
      type: "invalid_request_error", code: "compaction_affinity_missing",
      error: "409 invalid_request_error: compaction_affinity_missing", since: "2026-10-11T08:00:00.000Z",
      held: [expect.objectContaining({ from: "agent-lead" })] });
    expect(JSON.stringify(h.alarms)).not.toContain("summarizer");          // compaction error text stays local
    expect(h.alarms[0]!.held.map(item => item.from)).toEqual(["agent-lead"]);
    expect(h.notify).toHaveBeenCalledWith(expect.stringMatching(/^provider-dead \(non-retryable: 409 invalid_request_error: compaction_affinity_missing\): the recovery compaction failed/), "warning");
    const receipt = h.main.deliverAgent({ from: from("peer"), message: "ping", delivery: "followUp" });
    expect(receipt.triggered).toBe(false);
    expect(receipt.reason).toBe("provider-dead (non-retryable: 409 invalid_request_error: compaction_affinity_missing); ops alarmed, held until owner input");
    expect(receipt.reason).not.toContain("backoff");
    // No timed retries: hours pass, nothing is sent, compacted, or alarmed again.
    expect(vi.getTimerCount()).toBe(0);
    vi.advanceTimersByTime(14 * 60 * 60_000);
    h.emit("agent_settled", { outcome: "error" });
    h.emit("turn_end", errorTurn(affinity));
    vi.advanceTimersByTime(60 * 60_000);
    expect(h.sent).toHaveLength(0);
    expect(h.compactions).toHaveLength(1);
    expect(h.alarms).toHaveLength(1);
    expect(h.released).toHaveLength(0);
    expect(h.main.queueDepth().pendingFollowUps).toBe(2);
    // Owner input ends the incident; held work flows at the next boundary.
    h.emit("input", { source: "interactive" });
    expect(h.main.providerDeadState).toBeUndefined();
    h.main.closeFollowUpDrain();
  });

  it("alarms once when the one retry after compaction fails non-retryably again", () => {
    const h = setup(flushMs);
    h.emit("turn_end", errorTurn(affinity));
    h.settleError();
    vi.advanceTimersByTime(0);
    h.compactOk();
    vi.advanceTimersByTime(25);
    // Nothing was held: the failed turn itself is continued, once.
    expect(h.sent).toHaveLength(1);
    expect(h.sent[0]!.message.customType).toBe("pi-fabric-provider-retry");
    expect(h.sent[0]!.options).toEqual({ deliverAs: "followUp", triggerTurn: true });
    h.state.idle = false;
    h.emit("agent_start", {});
    h.emit("turn_end", errorTurn(affinity));
    expect(h.alarms).toEqual([expect.objectContaining({ cause: "failed-after-compaction" })]);
    h.settleError();
    vi.advanceTimersByTime(2 * 60 * 60_000);
    expect(h.sent).toHaveLength(1);
    expect(h.compactions).toHaveLength(1);
    expect(h.alarms).toHaveLength(1);
    // A later human /compact earns one more attempt but never a second alarm.
    h.emit("session_compact", { reason: "manual" });
    vi.advanceTimersByTime(25);
    expect(h.sent).toHaveLength(2);
    h.emit("turn_end", errorTurn(affinity));
    expect(h.alarms).toHaveLength(1);
    h.main.closeFollowUpDrain();
  });

  it("waits for idle on lifecycle edges, with no timer running while Main is busy (review P2)", () => {
    const h = setup(flushMs);
    h.emit("turn_end", errorTurn(affinity));            // the failed run is still active
    vi.advanceTimersByTime(0);                          // the one deferred tick samples busy
    expect(h.compactions).toHaveLength(0);
    expect(vi.getTimerCount()).toBe(0);                 // nothing polls while Main is busy
    vi.advanceTimersByTime(60 * 60_000);
    expect(h.compactions).toHaveLength(0);
    h.settleError();                                    // the run's settle is the idle edge
    vi.advanceTimersByTime(0);
    expect(h.compactions).toHaveLength(1);
    // session_compact runs while Pi is still compacting: the attempt waits, again without a timer.
    h.state.idle = false;
    h.emit("session_compact", { reason: "manual" });
    expect(h.main.providerDeadState?.phase).toBe("probation");
    vi.advanceTimersByTime(60 * 60_000);
    expect(h.sent).toHaveLength(0);
    expect(vi.getTimerCount()).toBe(0);
    h.state.idle = true;
    h.compactions[0]!.onComplete?.({});                 // Pi cleared its compaction state: the idle edge
    vi.advanceTimersByTime(0);
    expect(h.sent).toHaveLength(1);
    expect(h.sent[0]!.message.customType).toBe("pi-fabric-provider-retry");
    expect(h.sent[0]!.options).toEqual({ deliverAs: "followUp", triggerTurn: true });
    expect(h.compactions).toHaveLength(1);
    expect(h.alarms).toHaveLength(0);
    h.main.closeFollowUpDrain();
  });

  it.each(["agent_end", "peer delivery"])("re-arms a recovery step that found Main busy on %s (review P2)", (edge) => {
    const h = setup(flushMs);
    h.emit("turn_end", errorTurn(affinity));
    vi.advanceTimersByTime(0);
    expect(vi.getTimerCount()).toBe(0);
    h.state.idle = true;                                // e.g. a host whose settle carried no edge
    if (edge === "agent_end") h.emit("agent_end", { messages: [] });
    else expect(h.main.deliverAgent({ from: from("lead"), message: "x", delivery: "followUp" }).triggered).toBe(false);
    vi.advanceTimersByTime(0);
    expect(h.compactions).toHaveLength(1);
    expect(h.main.providerDeadState?.phase).toBe("compacting");
    h.main.closeFollowUpDrain();
  });

  it("keeps retryable errors on today's exponential backoff, without compaction or alarm", () => {
    const h = setup(flushMs);
    h.state.idle = true;
    h.emit("turn_end", errorTurn(`529 ${JSON.stringify({ type: "error", error: { type: "overloaded_error", message: "Overloaded" } })}`));
    const receipt = h.main.deliverAgent({ from: from("a"), message: "context", delivery: "followUp" });
    expect(receipt.triggered).toBe(false);
    expect(receipt.reason).toBe("provider-backoff until 2026-10-11T08:01:00.000Z");
    expect(h.main.providerDeadState).toBeUndefined();
    vi.advanceTimersByTime(60_000);
    expect(h.sent).toHaveLength(1);
    expect(h.sent[0]!.options.triggerTurn).toBe(true);
    h.emit("turn_end", errorTurn("503 upstream connect error"));
    const second = h.main.deliverAgent({ from: from("b"), message: "again", delivery: "followUp" });
    expect(second.reason).toBe("provider-backoff until 2026-10-11T08:03:00.000Z");
    expect(h.compactions).toHaveLength(0);
    expect(h.alarms).toHaveLength(0);
    h.main.closeFollowUpDrain();
  });

  it("falls back to backoff when the retry after compaction fails retryably", () => {
    const h = setup(flushMs);
    h.emit("turn_end", errorTurn(affinity));
    h.settleError();
    vi.advanceTimersByTime(0);
    h.compactOk();
    vi.advanceTimersByTime(25);
    h.state.idle = false;
    h.emit("agent_start", {});
    h.emit("turn_end", errorTurn("fetch failed"));
    expect(h.main.providerDeadState).toBeUndefined();
    h.state.idle = true;
    expect(h.main.deliverAgent({ from: from("a"), message: "x", delivery: "followUp" }).reason).toMatch(/^provider-backoff until /);
    expect(h.alarms).toHaveLength(0);
    h.main.closeFollowUpDrain();
  });
});

// PR #841 security review P1: provider text never leaves the Main; unclassified failures fail loud.
describe.each([120_000, 0])("Main provider-dead hardening (flushMs=%s, PR #841 round 2)", (flushMs) => {
  const secret = "SECRET-sk-ant-0123 echoed prompt: password hunter2";
  const leakTurn = errorTurn(`409 ${JSON.stringify({ type: "error", error: { type: "invalid_request_error", message: `compaction_affinity_missing ${secret}` } })}`);
  const leaked = (value: unknown) => /SECRET|hunter2|echoed/.test(JSON.stringify(value));

  it("keeps provider message text out of receipts, notices and the alarm event", () => {
    const h = setup(flushMs);
    h.emit("turn_end", leakTurn);
    const first = h.main.deliverAgent({ from: from("lead"), message: "status?", delivery: "followUp" });
    h.settleError();
    vi.advanceTimersByTime(0);
    h.compactOk();
    vi.advanceTimersByTime(25);
    h.state.idle = false;
    h.emit("agent_start", {});
    h.emit("turn_end", leakTurn);
    h.settleError();
    const after = h.main.deliverAgent({ from: from("peer"), message: "ping", delivery: "followUp" });
    expect(h.alarms).toHaveLength(1);
    expect(h.alarms[0]).toMatchObject({ kind: "non-retryable", cause: "failed-after-compaction", error: "409 invalid_request_error: compaction_affinity_missing" });
    expect(after.reason).toBe("provider-dead (non-retryable: 409 invalid_request_error: compaction_affinity_missing); ops alarmed, held until owner input");
    for (const value of [first, after, h.alarms, h.notify.mock.calls, h.main.providerDeadState]) expect(leaked(value)).toBe(false);
    h.main.closeFollowUpDrain();
  });

  /** One failed run with an error that carries no status, body or known token. */
  const failRun = (h: ReturnType<typeof setup>, errorMessage = "Connection error.") => {
    h.state.idle = false;
    h.emit("agent_start", {});
    h.emit("turn_end", errorTurn(errorMessage));
    h.settleError();
  };

  it("alarms provider-dead (unclassified) on the third unclassified failure in a row and stops silent backoff", () => {
    const h = setup(flushMs);
    failRun(h, `fetch failed: ${secret}`);
    const first = h.main.deliverAgent({ from: from("lead"), message: "status?", delivery: "followUp" });
    expect(first.reason).toBe("provider-backoff until 2026-10-11T08:01:00.000Z");   // a blip: backoff, no alarm
    vi.advanceTimersByTime(60_000);
    expect(h.sent).toHaveLength(1);                                                  // the backoff retry
    failRun(h, "terminated");
    expect(h.alarms).toHaveLength(0);
    // Owner input is not success: it does not reset the count.
    h.emit("input", { source: "interactive" });
    h.main.deliverAgent({ from: from("peer"), message: "ping", delivery: "followUp" });
    failRun(h, `socket hang up ${secret}`);
    expect(h.alarms).toEqual([{ kind: "unclassified", cause: "unclassified-repeated", error: "unclassified", failures: 3,
      since: "2026-10-11T08:01:00.000Z", held: expect.any(Array) }]);
    expect(h.notify).toHaveBeenCalledWith(expect.stringMatching(/^provider-dead \(unclassified\): 3 provider failures in a row without an HTTP status or error body\. The provider's error text is in the session log\./), "warning");
    const receipt = h.main.deliverAgent({ from: from("other"), message: "x", delivery: "followUp" });
    expect(receipt.triggered).toBe(false);
    expect(receipt.reason).toBe("provider-dead (unclassified); 3 failures in a row, ops alarmed, held until owner input");
    // No silent backoff loop: hours pass with no send, no compaction and no second alarm.
    const sends = h.sent.length;
    vi.advanceTimersByTime(14 * 60 * 60_000);
    expect(h.sent).toHaveLength(sends);
    expect(h.compactions).toHaveLength(0);
    expect(h.alarms).toHaveLength(1);
    for (const value of [h.alarms, h.notify.mock.calls, receipt]) expect(leaked(value)).toBe(false);
    // A human /compact earns one attempt; failing the same way holds again without a second alarm.
    h.emit("session_compact", { reason: "manual" });
    vi.advanceTimersByTime(25);
    expect(h.sent.length).toBe(sends + 1);
    failRun(h);
    expect(h.alarms).toHaveLength(1);
    expect(h.main.providerDeadState).toMatchObject({ kind: "unclassified", phase: "alarmed" });
    // A successful turn ends the incident and resets the count.
    h.emit("input", { source: "interactive" });
    h.state.idle = false;
    h.emit("agent_start", {});
    h.emit("turn_end", okTurn);
    expect(h.main.providerDeadState).toBeUndefined();
    failRun(h);
    failRun(h);
    expect(h.alarms).toHaveLength(1);
    h.main.closeFollowUpDrain();
  });

  it("resets the unclassified count on any successful turn, so isolated blips never alarm", () => {
    const h = setup(flushMs);
    for (let round = 0; round < 4; round++) {
      failRun(h);
      failRun(h);
      h.state.idle = false;
      h.emit("agent_start", {});
      h.emit("turn_end", okTurn);
      h.state.idle = true;
      h.emit("agent_settled", { outcome: "completed" });
    }
    // A classified retryable error between them neither counts nor resets.
    failRun(h);
    failRun(h, `529 ${JSON.stringify({ type: "error", error: { type: "overloaded_error", message: "Overloaded" } })}`);
    failRun(h);
    expect(h.alarms).toHaveLength(0);
    failRun(h);
    expect(h.alarms).toEqual([expect.objectContaining({ kind: "unclassified", failures: 3 })]);
    h.main.closeFollowUpDrain();
  });
});
