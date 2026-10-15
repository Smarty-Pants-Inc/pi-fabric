import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { MainAgentController, type FabricMainProviderDeadEvent } from "../src/main-agent.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { providerDeadAlarms } from "../src/topology/stall-alarms.js";

// smarty-dev#8742: a non-retryable provider error is never provider-backoff.
type Handler = (event: unknown, ctx: ExtensionContext) => unknown;
type CompactOptions = { onComplete?: (result: unknown) => void; onError?: (error: Error) => void };
const affinity = `409 ${JSON.stringify({ type: "error", error: { type: "invalid_request_error", message: "compaction_affinity_missing" } })}`;
const errorTurn = (errorMessage: string) => ({ message: { role: "assistant", stopReason: "error", errorMessage } });
const okTurn = { message: { role: "assistant", stopReason: "stop" } };
const from = (name: string) => ({ id: `agent-${name}`, name, kind: "agent" as const });

const setup = (flushMs = 120_000, onProviderDead?: (event: FabricMainProviderDeadEvent) => void, journal?: string,
  initialState: { idle?: boolean; settling?: boolean; aborted?: boolean } = {}) => {
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
  const state = { idle: initialState.idle ?? false, settling: initialState.settling ?? false };
  const compactions: CompactOptions[] = [];
  const notify = vi.fn();
  const ctx = {
    isIdle: () => state.idle,
    isSettling: () => state.settling,
    hasPendingMessages: () => false,
    signal: { aborted: initialState.aborted ?? false },
    hasUI: true,
    ui: { notify },
    compact: vi.fn((options: CompactOptions) => { compactions.push(options); }),
  } as unknown as ExtensionContext;
  const alarms: FabricMainProviderDeadEvent[] = [];
  const released: unknown[] = [];
  const main = new MainAgentController(pi, "session:root", true, "/tmp/project", "root", true,
    (event) => { released.push(event); }, {}, (event) => { alarms.push(event); onProviderDead?.(event); });
  main.attachFollowUpDrain(ctx, flushMs, journal);
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

beforeEach(() => {
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "setInterval", "clearInterval", "Date"] });
  vi.setSystemTime(new Date("2026-10-11T08:00:00Z"));
});
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

  // PR #841 security review (round 2) P1: Pi's native compaction deadline can settle Fabric's own recovery
  // compaction as aborted:true, with a TimeoutError as the operation signal's reason. Only an explicit owner
  // cancel is not a failed recovery; every other end raises the one alarm from the failure event itself.
  it.each([
    ["Pi's native deadline, settled as aborted (TimeoutError)", true, true, undefined],
    ["Pi's native deadline, settled as a failure", false, true, "Compaction failed: Compaction exceeded its 20-minute deadline"],
    ["an extension veto", true, false, undefined],
  ])("alarms once and holds when the recovery compaction ends by %s (security review P1)", (_label, aborted, timeout, errorMessage) => {
    const h = setup(flushMs);
    h.emit("turn_end", errorTurn(affinity));
    h.main.deliverAgent({ from: from("lead"), message: "busy work", delivery: "followUp" });
    h.settleError();
    vi.advanceTimersByTime(0);
    expect(h.compactions).toHaveLength(1);
    expect(h.main.providerDeadState?.phase).toBe("compacting");
    const operation = new AbortController();
    h.emit("session_before_compact", { reason: "manual", signal: operation.signal });
    if (timeout) operation.abort(new DOMException("Compaction exceeded its 20-minute deadline", "TimeoutError"));
    h.emit("session_compact_failed", { reason: "manual", aborted, willRetry: false, ...(errorMessage === undefined ? {} : { errorMessage }) });
    // The failure event alone raises the alarm; it does not depend on a later ctx.compact callback.
    expect(h.alarms).toEqual([expect.objectContaining({ kind: "non-retryable", cause: "compaction-failed", status: 409,
      type: "invalid_request_error", code: "compaction_affinity_missing", held: [expect.objectContaining({ from: "agent-lead" })] })]);
    expect(h.main.providerDeadState).toMatchObject({ kind: "non-retryable", phase: "alarmed", alarmed: true });
    expect(h.notify).toHaveBeenCalledWith(expect.stringMatching(/^provider-dead \(non-retryable: 409 invalid_request_error: compaction_affinity_missing\): the recovery compaction failed/), "warning");
    expect(JSON.stringify([h.alarms, h.notify.mock.calls])).not.toContain("deadline");
    // Pi then rejects ctx.compact: still exactly one alarm.
    h.compactions[0]!.onError?.(new Error("Compaction cancelled"));
    expect(h.alarms).toHaveLength(1);
    // Held, not halted: peer work stays journalled under the provider-dead receipt, with no timer and no send.
    const receipt = h.main.deliverAgent({ from: from("peer"), message: "ping", delivery: "followUp" });
    expect(receipt.triggered).toBe(false);
    expect(receipt.reason).toBe("provider-dead (non-retryable: 409 invalid_request_error: compaction_affinity_missing); ops alarmed, held until owner input");
    expect(vi.getTimerCount()).toBe(0);
    vi.advanceTimersByTime(14 * 60 * 60_000);
    h.emit("agent_settled", { outcome: "error" });
    vi.advanceTimersByTime(60 * 60_000);
    expect(h.sent).toHaveLength(0);
    expect(h.compactions).toHaveLength(1);
    expect(h.alarms).toHaveLength(1);
    expect(h.main.queueDepth().pendingFollowUps).toBe(2);
    // Owner input ends the incident.
    h.emit("input", { source: "interactive" });
    expect(h.main.providerDeadState).toBeUndefined();
    h.main.closeFollowUpDrain();
  });

  it("keeps an alarmed hold terminal when a late compaction failure arrives through a reattached binding", () => {
    const h = setup(flushMs);
    h.emit("turn_end", errorTurn(affinity));
    h.settleError();
    vi.advanceTimersByTime(0);
    expect(h.compactions).toHaveLength(1);
    expect(h.main.providerDeadState?.phase).toBe("compacting");

    // The old compaction is still in flight when a new binding takes over. Reattach itself must
    // alarm the abandoned step; the old native event then arrives through the new binding.
    h.main.attachFollowUpDrain(h.ctx, flushMs);
    expect(h.alarms).toHaveLength(1);
    expect(h.alarms[0]).toMatchObject({ cause: "compaction-failed", held: [] });
    expect(h.main.providerDeadState).toMatchObject({ phase: "alarmed", alarmed: true });
    h.emit("session_compact_failed", { reason: "manual", aborted: false, willRetry: false,
      errorMessage: "Compaction failed: late old binding" });

    const peer = h.main.deliverAgent({ from: from("peer"), message: "do not wake", delivery: "followUp" });
    expect(peer.triggered).toBe(false);
    expect(peer.reason).toBe("provider-dead (non-retryable: 409 invalid_request_error: compaction_affinity_missing); ops alarmed, held until owner input");
    expect(h.main.providerDeadState).toMatchObject({ phase: "alarmed", alarmed: true });
    expect(h.alarms).toHaveLength(1);
    expect(vi.getTimerCount()).toBe(0);
    vi.advanceTimersByTime(14 * 60 * 60_000);
    expect(h.sent).toHaveLength(0);
    expect(h.compactions).toHaveLength(1);
    expect(h.alarms).toHaveLength(1);
    expect(h.main.queueDepth().pendingFollowUps).toBe(1);
    h.main.closeFollowUpDrain();
  });

  it("allows explicit owner input or a healthy completed run to end an alarmed hold", () => {
    const alarm = (h: ReturnType<typeof setup>) => {
      h.emit("turn_end", errorTurn(affinity));
      h.settleError();
      vi.advanceTimersByTime(0);
      h.emit("session_compact_failed", { reason: "manual", aborted: false, willRetry: false });
      expect(h.main.providerDeadState).toMatchObject({ phase: "alarmed", alarmed: true });
    };

    const owner = setup(flushMs);
    alarm(owner);
    owner.main.deliverAgent({ from: from("peer"), message: "owner-held", delivery: "followUp" });
    owner.emit("input", { source: "interactive" });
    expect(owner.main.providerDeadState).toBeUndefined();
    owner.main.closeFollowUpDrain();

    const healthy = setup(flushMs);
    alarm(healthy);
    healthy.main.deliverAgent({ from: from("peer"), message: "healthy-held", delivery: "followUp" });
    healthy.state.idle = false;
    healthy.emit("agent_start", {});
    healthy.emit("turn_end", okTurn);
    healthy.state.idle = true;
    healthy.emit("agent_before_settle", { outcome: "completed" });
    healthy.emit("agent_settled", { outcome: "completed" });
    expect(healthy.main.providerDeadState).toBeUndefined();
    expect(healthy.sent.map(entry => entry.message.content).join("\n")).toContain("healthy-held");
    healthy.main.closeFollowUpDrain();
  });

  it("backs off a retryable failure after human compact reopens an alarmed incident", () => {
    const h = setup(flushMs);
    h.emit("turn_end", errorTurn(affinity));
    h.settleError();
    vi.advanceTimersByTime(0);
    h.emit("session_compact_failed", { reason: "manual", aborted: false, willRetry: false });
    expect(h.main.providerDeadState).toMatchObject({ phase: "alarmed", alarmed: true });

    // An explicit /compact earns one more attempt. Its classified retryable failure must
    // return to ordinary backoff, not leave the post-compaction attempt permanently in flight.
    h.state.idle = true;
    h.emit("session_compact", { reason: "manual" });
    vi.advanceTimersByTime(25);
    expect(h.sent).toHaveLength(1);
    expect(h.sent[0]!.message.customType).toBe("pi-fabric-provider-retry");
    expect(h.main.providerDeadState).toMatchObject({ phase: "probation", alarmed: true, attemptStarted: true });
    h.state.idle = false;
    h.emit("agent_start", {});
    h.emit("turn_end", errorTurn("503 upstream connect error"));
    expect(h.main.providerDeadState).toBeUndefined();
    h.state.idle = true;
    const peer = h.main.deliverAgent({ from: from("peer"), message: "backoff, not stuck", delivery: "followUp" });
    expect(peer.reason).toMatch(/^provider-backoff until /);
    vi.advanceTimersByTime(60_000);
    expect(h.sent).toHaveLength(2);
    expect(h.sent[1]!.message.content).toContain("backoff, not stuck");
    h.main.closeFollowUpDrain();
  });

  it("keeps an owner cancel of the recovery compaction an owner stop, with no alarm from the failure event (security review P1)", () => {
    const h = setup(flushMs);
    h.emit("turn_end", errorTurn(affinity));
    h.settleError();
    vi.advanceTimersByTime(0);
    expect(h.compactions).toHaveLength(1);
    const operation = new AbortController();
    h.emit("session_before_compact", { reason: "manual", signal: operation.signal });
    operation.abort();                                  // Escape: a plain abort, not the deadline
    h.emit("session_compact_failed", { reason: "manual", aborted: true, willRetry: false });
    expect(h.alarms).toHaveLength(0);
    expect(h.main.providerDeadState?.phase).toBe("compacting");
    // Halted: a peer message neither triggers nor gets a provider-dead receipt.
    const receipt = h.main.deliverAgent({ from: from("peer"), message: "ping", delivery: "followUp" });
    expect(receipt.triggered).toBe(false);
    expect(receipt.reason ?? "").not.toMatch(/^provider-dead/);
    vi.advanceTimersByTime(60 * 60_000);
    expect(h.sent.every(entry => entry.options.triggerTurn === false)).toBe(true);   // passive context only
    expect(h.compactions).toHaveLength(1);
    expect(h.alarms).toHaveLength(0);
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

  it("waits for idle on lifecycle edges with only one last-resort deadline, never polling while busy (review P2)", () => {
    const h = setup(flushMs);
    h.emit("turn_end", errorTurn(affinity));            // the failed run is still active
    vi.advanceTimersByTime(0);                          // the one deferred tick samples busy
    expect(h.compactions).toHaveLength(0);
    expect(vi.getTimerCount()).toBeLessThanOrEqual(1);  // at most one five-minute deadline, never a poll
    vi.advanceTimersByTime(4 * 60_000);
    expect(h.compactions).toHaveLength(0);
    expect(vi.getTimerCount()).toBeLessThanOrEqual(1);
    h.settleError();                                    // the run's settle is the idle edge, before deadline
    vi.advanceTimersByTime(0);
    expect(h.compactions).toHaveLength(1);
    expect(h.alarms).toHaveLength(0);
    // session_compact runs while Pi is still compacting: the attempt waits, event-driven.
    h.state.idle = false;
    h.emit("session_compact", { reason: "manual" });
    expect(h.main.providerDeadState?.phase).toBe("probation");
    vi.advanceTimersByTime(30_000);
    expect(h.sent).toHaveLength(0);
    h.state.idle = true;
    h.compactions[0]!.onComplete?.({});                 // Pi cleared its compaction state: the idle edge
    vi.advanceTimersByTime(0);
    expect(h.sent).toHaveLength(1);
    expect(h.sent[0]!.message.customType).toBe("pi-fabric-provider-retry");
    expect(h.sent[0]!.options).toEqual({ deliverAs: "followUp", triggerTurn: true });
    expect(h.compactions).toHaveLength(1);
    expect(h.alarms).toHaveLength(0);
    h.main.closeFollowUpDrain();
    expect(vi.getTimerCount()).toBe(0);
  });

  it.each(["agent_end", "peer delivery"])("re-arms a recovery step that found Main busy on %s (review P2)", (edge) => {
    const h = setup(flushMs);
    h.emit("turn_end", errorTurn(affinity));
    vi.advanceTimersByTime(0);
    expect(vi.getTimerCount()).toBeLessThanOrEqual(1);  // only the bounded deadline may remain; no polling
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
      since: "2026-10-11T08:00:00.000Z", held: expect.any(Array) }]);
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

  it("preserves unclassified failures across same-controller reattach, alarms once on the third, and resets only on success", () => {
    const h = setup(flushMs);
    try {
      failRun(h);
      h.main.attachFollowUpDrain(h.ctx, flushMs);
      vi.advanceTimersByTime(30_000);
      failRun(h);
      h.main.attachFollowUpDrain(h.ctx, flushMs);
      expect(h.alarms).toHaveLength(0);
      vi.advanceTimersByTime(30_000);
      failRun(h);
      expect(h.alarms).toEqual([expect.objectContaining({ kind: "unclassified", cause: "unclassified-repeated",
        failures: 3, since: "2026-10-11T08:00:00.000Z" })]);
      h.main.attachFollowUpDrain(h.ctx, flushMs);
      failRun(h);
      expect(h.alarms).toHaveLength(1);
      expect(h.main.providerDeadState).toMatchObject({ kind: "unclassified", phase: "alarmed" });
      // A real successful provider turn, unlike reload or owner input, ends the streak.
      h.emit("agent_start", {});
      h.emit("turn_end", okTurn);
      h.emit("agent_settled", { outcome: "completed" });
      expect(h.main.providerDeadState).toBeUndefined();
      failRun(h);
      h.main.attachFollowUpDrain(h.ctx, flushMs);
      failRun(h);
      expect(h.alarms).toHaveLength(1);
      expect(h.main.providerDeadState).toBeUndefined();
      failRun(h);
      expect(h.alarms).toHaveLength(2);
      expect(h.alarms[1]).toMatchObject({ kind: "unclassified", failures: 3,
        since: "2026-10-11T08:01:00.000Z" });
    } finally {
      h.main.closeFollowUpDrain();
    }
  });

  it("retains the unclassified count and first failure time in the same delivered index across controller restart", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "main-provider-count-"));
    const journal = path.join(root, "followups.json");
    const first = setup(flushMs, undefined, journal);
    let restarted: ReturnType<typeof setup> | undefined;
    try {
      failRun(first);
      vi.advanceTimersByTime(30_000);
      failRun(first);
      first.main.closeFollowUpDrain();
      expect(fs.existsSync(journal)).toBe(false); // Metadata must survive without a payload journal.
      expect(JSON.parse(fs.readFileSync(`${journal}.delivered`, "utf8")).provider).toMatchObject({
        unclassifiedFailures: 2, unclassifiedSince: new Date("2026-10-11T08:00:00Z").getTime(),
      });
      restarted = setup(flushMs, undefined, journal);
      expect(restarted.alarms).toHaveLength(0);
      failRun(restarted);
      expect(restarted.alarms).toEqual([expect.objectContaining({ kind: "unclassified", failures: 3,
        since: "2026-10-11T08:00:00.000Z" })]);
      restarted.main.attachFollowUpDrain(restarted.ctx, flushMs, journal);
      failRun(restarted);
      expect(first.alarms).toHaveLength(0);
      expect(restarted.alarms).toHaveLength(1);
      expect(restarted.compactions).toHaveLength(0);
      restarted.emit("turn_end", okTurn);
      restarted.emit("agent_settled", { outcome: "completed" });
      restarted.main.closeFollowUpDrain();
      const healthy = setup(flushMs, undefined, journal);
      try {
        failRun(healthy);
        failRun(healthy);
        expect(healthy.alarms).toHaveLength(0);
        failRun(healthy);
        expect(healthy.alarms).toHaveLength(1);
      } finally { healthy.main.closeFollowUpDrain(); }
    } finally {
      first.main.closeFollowUpDrain();
      restarted?.main.closeFollowUpDrain();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it("expires an unclassified streak after the bounded five-minute window across restart", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "main-provider-window-"));
    const journal = path.join(root, "followups.json");
    const first = setup(flushMs, undefined, journal);
    let restarted: ReturnType<typeof setup> | undefined;
    try {
      failRun(first);
      failRun(first);
      first.main.closeFollowUpDrain();
      vi.advanceTimersByTime(5 * 60_000 + 1);
      restarted = setup(flushMs, undefined, journal);
      failRun(restarted);
      failRun(restarted);
      expect(restarted.alarms).toHaveLength(0);
      failRun(restarted);
      expect(restarted.alarms).toEqual([expect.objectContaining({ kind: "unclassified", failures: 3,
        since: "2026-10-11T08:05:00.001Z" })]);
    } finally {
      first.main.closeFollowUpDrain();
      restarted?.main.closeFollowUpDrain();
      fs.rmSync(root, { recursive: true, force: true });
    }
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

// smarty-dev#8881: AgentSession.dispose() and session replacement invalidate the ctx a delayed
// provider-dead step captured, without session_shutdown. Pi's stale ctx throws on every getter.
const staleCtx = (compactions: CompactOptions[], notify: ReturnType<typeof vi.fn>, state: { idle: boolean }) => {
  const life = { live: true };
  const guard = <T>(value: T): T => { if (!life.live) throw new Error("This extension ctx is stale after session replacement or reload."); return value; };
  const ctx = Object.defineProperties({}, {
    isIdle: { get: () => guard(() => state.idle) },
    hasPendingMessages: { get: () => guard(() => false) },
    signal: { get: () => guard({ aborted: false }) },
    hasUI: { get: () => guard(true) },
    ui: { get: () => guard({ notify }) },
    compact: { get: () => guard(vi.fn((options: CompactOptions) => { compactions.push(options); })) },
    sessionManager: { get: () => guard(undefined) },
  }) as unknown as ExtensionContext;
  return { ctx, dispose: () => { life.live = false; } };
};

const setupDisposable = (flushMs: number) => {
  const handlers = new Map<string, Handler[]>();
  const sent: unknown[] = [];
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
  const original = staleCtx(compactions, notify, state);
  const alarms: FabricMainProviderDeadEvent[] = [];
  const main = new MainAgentController(pi, "session:root", true, "/tmp/project", "root", true,
    () => {}, {}, (event) => { alarms.push(event); });
  main.attachFollowUpDrain(original.ctx, flushMs);
  const emit = (name: string, event: unknown, ctx: ExtensionContext = original.ctx) => {
    for (const handler of handlers.get(name) ?? []) handler(event, ctx);
  };
  const settleError = (ctx?: ExtensionContext) => {
    state.idle = true;
    emit("agent_before_settle", { outcome: "error" }, ctx);
    emit("agent_settled", { outcome: "error" }, ctx);
  };
  return { main, sent, state, compactions, notify, alarms, original, emit, settleError };
};

describe.each([120_000, 0])("Main provider-dead delayed work after dispose (flushMs=%s, smarty-dev#8881)", (flushMs) => {
  it("retires a pending zero-delay recovery tick when the session is disposed", () => {
    const h = setupDisposable(flushMs);
    h.emit("turn_end", errorTurn(affinity));
    h.main.deliverAgent({ from: from("lead"), message: "held", delivery: "followUp" });
    h.settleError();                                    // arms the zero-delay recovery tick
    expect(vi.getTimerCount()).toBeGreaterThan(0);
    h.original.dispose();
    expect(() => vi.advanceTimersByTime(0)).not.toThrow();
    expect(h.compactions).toHaveLength(0);
    expect(h.alarms).toHaveLength(0);
    expect(h.sent).toHaveLength(0);
    expect(h.main.providerDeadState?.phase).toBe("recovering");
    expect(h.main.ownsProviderRecovery()).toBe(false);
    vi.advanceTimersByTime(60 * 60_000);
    expect(vi.getTimerCount()).toBe(0);
    expect(h.compactions).toHaveLength(0);
    expect(h.alarms).toHaveLength(0);
  });

  it.each(["onError", "onComplete"] as const)("ignores a recovery compaction %s delivered after dispose", (callback) => {
    const h = setupDisposable(flushMs);
    h.emit("turn_end", errorTurn(affinity));
    h.main.deliverAgent({ from: from("lead"), message: "held", delivery: "followUp" });
    h.settleError();
    vi.advanceTimersByTime(0);
    expect(h.compactions).toHaveLength(1);
    expect(h.main.providerDeadState?.phase).toBe("compacting");
    h.original.dispose();
    h.notify.mockClear();
    if (callback === "onError") h.compactions[0]!.onError?.(new Error("Compaction failed: disposed"));
    else h.compactions[0]!.onComplete?.({});
    vi.advanceTimersByTime(60 * 60_000);
    expect(h.alarms).toHaveLength(0);
    expect(h.notify).not.toHaveBeenCalled();
    expect(h.main.providerDeadState?.phase).toBe("compacting");
    expect(h.compactions).toHaveLength(1);
    expect(h.sent).toHaveLength(0);
    expect(h.main.ownsProviderRecovery()).toBe(false);
    expect(vi.getTimerCount()).toBe(0);
  });

  it.each(["onError", "onComplete"] as const)("keeps a live successor's state when the replaced ctx's compaction %s arrives late", (callback) => {
    const h = setupDisposable(flushMs);
    h.emit("turn_end", errorTurn(affinity));
    h.main.deliverAgent({ from: from("lead"), message: "held", delivery: "followUp" });
    h.settleError();
    vi.advanceTimersByTime(0);
    expect(h.compactions).toHaveLength(1);
    // Session replacement: the old ctx goes stale and a live successor ctx reaches the same Main.
    h.original.dispose();
    const successor = staleCtx(h.compactions, h.notify, h.state);
    h.state.idle = false;
    h.emit("tool_execution_start", { toolCallId: "t1" }, successor.ctx);
    h.emit("tool_execution_end", { toolCallId: "t1" }, successor.ctx);
    h.state.idle = true;
    h.notify.mockClear();
    if (callback === "onError") h.compactions[0]!.onError?.(new Error("Compaction failed: replaced"));
    else h.compactions[0]!.onComplete?.({});
    vi.advanceTimersByTime(60 * 60_000);
    expect(h.alarms).toHaveLength(0);
    expect(h.notify).not.toHaveBeenCalled();
    expect(h.main.providerDeadState?.phase).toBe("compacting");
    expect(h.compactions).toHaveLength(1);
    expect(h.sent).toHaveLength(0);
    // The stale callback never retires the live successor.
    expect(h.main.ownsProviderRecovery()).toBe(true);
    h.main.closeFollowUpDrain();
    expect(vi.getTimerCount()).toBe(0);
  });

  it("preserves a pending recovery retry across reattach and ignores the old binding's late compaction callbacks", () => {
    const h = setupDisposable(flushMs);
    h.emit("turn_end", errorTurn(affinity));
    h.settleError();
    vi.advanceTimersByTime(0);
    expect(h.compactions).toHaveLength(1);
    // The compaction completed, but its retry is still pending while Main is busy.
    h.state.idle = false;
    h.emit("session_compact", { reason: "manual" });
    expect(h.main.providerDeadState?.phase).toBe("probation");
    const incident = { ...h.main.providerDeadState! };
    h.original.dispose();
    const successor = staleCtx(h.compactions, h.notify, h.state);
    h.main.attachFollowUpDrain(successor.ctx, flushMs);
    expect(h.main.providerDeadState).toEqual(incident);
    h.compactions[0]!.onError?.(new Error("Compaction failed: stale"));
    h.compactions[0]!.onComplete?.({});
    vi.advanceTimersByTime(25);
    expect(h.alarms).toHaveLength(0);
    expect(h.main.providerDeadState).toEqual(incident);
    expect(h.sent).toHaveLength(0);
    // The successor resumes the pending attempt, never repeating the completed compaction.
    h.state.idle = true;
    h.emit("agent_end", {}, successor.ctx);
    vi.advanceTimersByTime(25);
    expect(h.sent).toHaveLength(1);
    expect(h.main.providerDeadState?.phase).toBe("probation");
    expect(h.compactions).toHaveLength(1);
    expect(h.alarms).toHaveLength(0);
    h.main.closeFollowUpDrain();
    expect(vi.getTimerCount()).toBe(0);
  });
});

// Keep reason: durable recovery is a data-safety boundary; reload must not buy another five minutes.
describe.each([120_000, 0])("Main durable provider recovery across reload (flushMs=%s)", (flushMs) => {
  it.each(["reattach", "restart"] as const)("keeps the original pending deadline after a four-minute %s and alarms one minute later", (mode) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "main-provider-deadline-"));
    const journal = path.join(root, "followups.json");
    const first = setup(flushMs, undefined, journal);
    let active = first;
    const deadlineAt = Date.now() + 5 * 60_000;
    try {
      // Keep both the earlier unclassified streak and the pending classified episode across restart.
      for (let attempt = 0; attempt < 2; attempt++) {
        first.emit("turn_end", errorTurn("Connection error."));
        first.settleError();
      }
      first.state.idle = false;
      first.emit("turn_end", errorTurn(affinity));
      first.main.deliverAgent({ from: from("lead"), message: "held across reload", delivery: "followUp" });
      vi.advanceTimersByTime(4 * 60_000);
      expect(first.compactions).toHaveLength(0);
      expect(first.alarms).toHaveLength(0);
      first.main.prepareReload();
      expect(JSON.parse(fs.readFileSync(`${journal}.delivered`, "utf8")).provider.dead).toMatchObject({
        phase: "recovering", since: deadlineAt - 5 * 60_000, deadlineAt,
      });
      if (mode === "restart") {
        first.main.closeFollowUpDrain();
        active = setup(flushMs, undefined, journal); // Same journal and .delivered, a new controller.
      } else {
        first.main.attachFollowUpDrain(first.ctx, flushMs, journal);
      }
      expect(active.main.providerDeadState).toMatchObject({ phase: "recovering", deadlineAt });
      expect(JSON.parse(fs.readFileSync(`${journal}.delivered`, "utf8")).provider).toMatchObject({
        unclassifiedFailures: 2, unclassifiedSince: deadlineAt - 5 * 60_000,
        dead: { deadlineAt },
      });
      vi.advanceTimersByTime(60_000 - 1);
      expect(active.alarms).toHaveLength(0);
      expect(active.compactions).toHaveLength(0);
      vi.advanceTimersByTime(1);
      expect(Date.now()).toBe(deadlineAt);
      expect(active.alarms).toEqual([expect.objectContaining({ kind: "non-retryable", cause: "recovery-timeout",
        since: "2026-10-11T08:00:00.000Z", held: [expect.objectContaining({ from: "agent-lead" })] })]);
      expect(active.main.providerDeadState).toMatchObject({ phase: "alarmed", alarmed: true, deadlineAt });
      active.emit("agent_end", {});
      active.emit("session_tree", {});
      vi.advanceTimersByTime(60 * 60_000);
      expect(active.alarms).toHaveLength(1);
      expect(active.compactions).toHaveLength(0);
      expect(active.sent).toHaveLength(0);
      expect(active.main.queueDepth().pendingFollowUps).toBe(1);
    } finally {
      first.main.closeFollowUpDrain();
      if (active !== first) active.main.closeFollowUpDrain();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it.each(["reattach", "restart"] as const)("alarms synchronously before releasing a journalled probation message after an overdue %s attach", (mode) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "main-provider-overdue-probation-"));
    const journal = path.join(root, "followups.json");
    const first = setup(flushMs, undefined, journal);
    let active = first;
    try {
      first.emit("turn_end", errorTurn(affinity));
      first.settleError();
      vi.advanceTimersByTime(0);
      first.state.idle = false;
      first.state.settling = true;
      first.main.deliverAgent({ from: from("peer"), message: "journalled probation work", delivery: "followUp" });
      first.compactOk();
      vi.advanceTimersByTime(0);
      expect(first.main.providerDeadState?.phase).toBe("probation");
      expect(first.main.providerDeadState?.attemptStarted).not.toBe(true);
      expect(JSON.parse(fs.readFileSync(journal, "utf8")).items).toHaveLength(1);
      const deadlineAt = first.main.providerDeadState?.deadlineAt;
      expect(deadlineAt).toBeDefined();

      first.main.prepareReload();
      if (mode === "restart") {
        first.main.closeFollowUpDrain();
        vi.advanceTimersByTime(deadlineAt! - Date.now() + 1);
        active = setup(flushMs, undefined, journal, { idle: true });
      } else {
        vi.advanceTimersByTime(deadlineAt! - Date.now() + 1);
        first.state.idle = true;
        first.state.settling = false;
        first.main.attachFollowUpDrain(first.ctx, flushMs, journal);
      }

      // No timer turn is allowed to be needed: attach itself must alarm before its idle replay.
      expect(active.alarms).toEqual([expect.objectContaining({ cause: "recovery-timeout", held: [expect.objectContaining({ from: "agent-peer" })] })]);
      expect(active.main.providerDeadState).toMatchObject({ phase: "alarmed", alarmed: true, deadlineAt });
      expect(active.sent.every(entry => entry.options.triggerTurn === false)).toBe(true);
      expect(active.compactions).toHaveLength(mode === "restart" ? 0 : 1);
      expect(vi.getTimerCount()).toBe(0);
    } finally {
      first.main.closeFollowUpDrain();
      if (active !== first) active.main.closeFollowUpDrain();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it.each(["owner-halted", "aborted"] as const)("does not alarm an overdue attach when the restored Main is %s", (mode) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "main-provider-overdue-veto-"));
    const journal = path.join(root, "followups.json");
    const first = setup(flushMs, undefined, journal);
    let active = first;
    try {
      first.emit("turn_end", errorTurn(affinity));
      expect(first.main.providerDeadState).toMatchObject({ phase: "recovering" });
      if (mode === "owner-halted") first.main.halt();
      first.main.prepareReload();
      first.main.closeFollowUpDrain();
      const deadlineAt = first.main.providerDeadState?.deadlineAt!;
      vi.advanceTimersByTime(deadlineAt - Date.now() + 1);
      active = setup(flushMs, undefined, journal, { idle: true, aborted: mode === "aborted" });
      expect(active.alarms).toHaveLength(0);
      expect(active.sent).toHaveLength(0);
      expect(active.main.providerDeadState).toMatchObject({ phase: "recovering", deadlineAt });
      vi.advanceTimersByTime(0);
      expect(active.alarms).toHaveLength(0);
    } finally {
      first.main.closeFollowUpDrain();
      if (active !== first) active.main.closeFollowUpDrain();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it("restores an unexpired probation and admits exactly one valid attempt", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "main-provider-probation-"));
    const journal = path.join(root, "followups.json");
    const first = setup(flushMs, undefined, journal);
    let active = first;
    try {
      first.emit("turn_end", errorTurn(affinity));
      first.settleError();
      vi.advanceTimersByTime(0);
      first.state.idle = false;
      first.state.settling = true;
      first.main.deliverAgent({ from: from("peer"), message: "one valid attempt", delivery: "followUp" });
      first.compactOk();
      vi.advanceTimersByTime(0);
      const deadlineAt = first.main.providerDeadState?.deadlineAt!;
      first.main.prepareReload();
      first.main.closeFollowUpDrain();
      vi.advanceTimersByTime(60_000);
      expect(Date.now()).toBeLessThan(deadlineAt);
      active = setup(flushMs, undefined, journal, { idle: true });

      expect(active.alarms).toHaveLength(0);
      expect(active.sent).toHaveLength(1);
      expect(active.sent[0]!.message.content).toContain("one valid attempt");
      expect(active.sent[0]!.options.triggerTurn).toBe(true);
      expect(active.main.providerDeadState).toMatchObject({ phase: "probation", attemptStarted: true });
      const later = active.main.deliverAgent({ from: from("later"), message: "wait", delivery: "followUp" });
      expect(later.triggered).toBe(false);
      expect(later.reason).toBe("provider-dead (non-retryable: 409 invalid_request_error: compaction_affinity_missing); retry after compaction in flight");
      vi.advanceTimersByTime(5 * 60_000);
      expect(active.sent).toHaveLength(1);
      expect(active.alarms).toHaveLength(0);
    } finally {
      first.main.closeFollowUpDrain();
      if (active !== first) active.main.closeFollowUpDrain();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it("does not time out or re-admit an already-admitted postcompaction retry across attach", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "main-provider-admitted-retry-"));
    const journal = path.join(root, "followups.json");
    const first = setup(flushMs, undefined, journal, { idle: true });
    let active = first;
    try {
      first.emit("turn_end", errorTurn(affinity));
      first.settleError();
      vi.advanceTimersByTime(0);
      first.compactOk();
      vi.advanceTimersByTime(25);
      expect(first.sent).toHaveLength(1);
      expect(first.sent[0]!.message.customType).toBe("pi-fabric-provider-retry");
      expect(first.main.providerDeadState).toMatchObject({ phase: "probation", attemptStarted: true });
      expect(fs.existsSync(journal)).toBe(false); // Provider metadata persists without a payload journal.

      first.main.prepareReload();
      first.main.closeFollowUpDrain();
      vi.advanceTimersByTime(5 * 60_000 + 1);
      active = setup(flushMs, undefined, journal, { idle: true });
      active.main.attachFollowUpDrain(active.ctx, flushMs, journal);
      expect(active.sent).toHaveLength(0);
      expect(active.compactions).toHaveLength(0);
      expect(active.alarms).toHaveLength(0);
      expect(active.main.providerDeadState).toMatchObject({ phase: "probation", attemptStarted: true });
      vi.advanceTimersByTime(60 * 60_000);
      expect(active.sent).toHaveLength(0);
      expect(active.alarms).toHaveLength(0);
    } finally {
      first.main.closeFollowUpDrain();
      if (active !== first) active.main.closeFollowUpDrain();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it.each(["reattach", "restart"] as const)("alarms before any compaction retry when %s happens after the absolute deadline", (mode) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "main-provider-overdue-"));
    const journal = path.join(root, "followups.json");
    const first = setup(flushMs, undefined, journal);
    let active = first;
    const deadlineAt = Date.now() + 5 * 60_000;
    try {
      first.emit("turn_end", errorTurn(affinity));
      vi.advanceTimersByTime(4 * 60_000);
      first.main.prepareReload();
      if (mode === "restart") first.main.closeFollowUpDrain();
      vi.advanceTimersByTime(60_000 + 1); // No live drain at the original deadline.
      expect(first.alarms).toHaveLength(0);
      if (mode === "restart") {
        active = setup(flushMs, undefined, journal);
        active.state.idle = true;
      } else {
        first.state.idle = true;
        first.main.attachFollowUpDrain(first.ctx, flushMs, journal);
      }
      // Being idle cannot admit compaction before the already-overdue safety alarm.
      vi.advanceTimersByTime(0);
      expect(active.alarms).toEqual([expect.objectContaining({ kind: "non-retryable", cause: "recovery-timeout",
        since: "2026-10-11T08:00:00.000Z" })]);
      expect(active.main.providerDeadState).toMatchObject({ phase: "alarmed", deadlineAt });
      expect(active.compactions).toHaveLength(0);
      expect(first.compactions).toHaveLength(0);
      active.emit("agent_end", {});
      vi.advanceTimersByTime(5 * 60_000);
      expect(active.alarms).toHaveLength(1);
      expect(active.compactions).toHaveLength(0);
      expect(active.sent).toHaveLength(0);
    } finally {
      first.main.closeFollowUpDrain();
      if (active !== first) active.main.closeFollowUpDrain();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
});

describe("Main provider recovery deadline and final settle fallback (smarty-dev#9057)", () => {
  it.each([
    ["busy", false, false],
    ["settling", true, true],
  ] as const)("alarms once at five minutes while Main is %s; lifecycle edges do not extend its deadline", (_label, idle, settling) => {
    const h = setup();
    h.state.idle = idle;
    h.state.settling = settling;
    h.emit("turn_end", errorTurn(affinity));
    h.main.deliverAgent({ from: from("lead"), message: "held through recovery timeout", delivery: "followUp" });
    vi.advanceTimersByTime(0);                          // recovery tick sees Main busy/settling
    expect(h.compactions).toHaveLength(0);
    expect(vi.getTimerCount()).toBe(1);                 // one deadline, no polling interval
    for (let minute = 1; minute <= 4; minute++) {
      vi.advanceTimersByTime(60_000);
      h.emit(minute % 2 ? "agent_end" : "agent_before_settle", {});
      vi.advanceTimersByTime(0);                        // the lifecycle edge schedules a zero-tick probe
      expect(h.compactions).toHaveLength(0);
      expect(vi.getTimerCount()).toBe(1);               // probe is consumed; deadline is not re-armed/extended
    }
    vi.advanceTimersByTime(60_000);
    expect(h.compactions).toHaveLength(0);
    expect(h.alarms).toHaveLength(1);
    expect(h.alarms[0]).toMatchObject({ kind: "non-retryable", cause: "recovery-timeout", status: 409,
      held: [expect.objectContaining({ from: "agent-lead" })] });
    expect(h.sent).toHaveLength(0);                      // timed-out held work is not handed to Pi
    expect(vi.getTimerCount()).toBe(0);
    vi.advanceTimersByTime(60 * 60_000);
    expect(h.alarms).toHaveLength(1);
    expect(h.compactions).toHaveLength(0);
    h.emit("input", { source: "interactive" });
    expect(vi.getTimerCount()).toBe(0);
    h.main.closeFollowUpDrain();
  });

  it("does not time out the admitted post-compaction retry while its run is in flight", () => {
    const h = setup();
    h.state.idle = true;
    h.emit("turn_end", errorTurn(affinity));
    vi.advanceTimersByTime(0);
    expect(h.compactions).toHaveLength(1);
    h.compactOk();
    vi.advanceTimersByTime(0);
    expect(h.sent).toHaveLength(1);
    expect(h.sent[0]!.message.customType).toBe("pi-fabric-provider-retry");
    h.state.idle = false;
    h.emit("agent_start", {});
    expect(h.main.providerDeadState?.phase).toBe("probation");
    h.main.deliverAgent({ from: from("peer"), message: "wait for retry", delivery: "followUp" });
    h.emit("session_tree", {});
    h.emit("agent_end", {});
    vi.advanceTimersByTime(5 * 60_000);
    expect(h.alarms).toHaveLength(0);
    expect(h.sent).toHaveLength(1);
    expect(h.compactions).toHaveLength(1);
    h.main.closeFollowUpDrain();
  });

  it("counts a missing outcome once at final agent_settled, resets after a same-run success, and ignores legacy aborts", () => {
    const h = setup();
    // Same-run success must clear the failed turn before an outcome-less final boundary.
    h.state.idle = false;
    h.emit("agent_start", {});
    h.emit("turn_end", errorTurn("Connection error."));
    h.emit("turn_end", okTurn);
    h.emit("agent_before_settle", {});
    expect(h.alarms).toHaveLength(0);
    h.state.idle = true;
    h.emit("agent_settled", {});
    expect(h.alarms).toHaveLength(0);

    // Legacy aborts must not count the pending unclassified failure.
    h.state.idle = false;
    h.emit("agent_start", {});
    h.emit("turn_end", errorTurn("Connection error."));
    h.state.idle = true;
    h.emit("agent_settled", { aborted: true });
    expect(h.alarms).toHaveLength(0);
    h.emit("input", { source: "interactive" });

    for (let run = 1; run <= 3; run++) {
      h.state.idle = false;
      h.emit("agent_start", {});
      h.emit("turn_end", errorTurn("Connection error."));
      h.emit("agent_before_settle", {});
      expect(h.alarms).toHaveLength(0);                  // never count before the final boundary
      h.state.idle = true;
      h.emit("agent_settled", {});
      expect(h.alarms).toHaveLength(run === 3 ? 1 : 0);
    }
    expect(h.alarms[0]).toMatchObject({ kind: "unclassified", cause: "unclassified-repeated", failures: 3 });
    h.main.closeFollowUpDrain();
  });

  it("keeps a healthy idle Main timer-free and does not alarm after five minutes", () => {
    const h = setup();
    h.state.idle = true;
    expect(vi.getTimerCount()).toBe(0);
    vi.advanceTimersByTime(5 * 60_000);
    expect(vi.getTimerCount()).toBe(0);
    expect(h.alarms).toHaveLength(0);
    expect(h.compactions).toHaveLength(0);
    expect(h.sent).toHaveLength(0);
    h.main.closeFollowUpDrain();
  });

  it("routes one recovery-timeout from Main to the configured ops.owner session with allowlisted data", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "main-recovery-alarm-"));
    const mesh = new MeshStore(root, 64 * 1024, 500);
    const identity: MeshIdentity = { id: "session:root", sessionId: "root", name: "root", kind: "main" };
    const owner = "session:configured-owner";
    let publish!: Promise<void>;
    const h = setup(120_000, event => { publish = providerDeadAlarms(mesh, identity, identity.id, event, owner); });
    try {
      h.emit("turn_end", errorTurn(affinity));
      vi.advanceTimersByTime(0);
      vi.advanceTimersByTime(5 * 60_000);
      await publish;
      h.emit("agent_end", {});
      h.emit("session_tree", {});
      vi.advanceTimersByTime(60 * 60_000);
      const alarms = mesh.read({ topic: "ops.owner" });
      const addressed = alarms.filter(item => item.kind === "fabric.main.provider-dead" && item.to === owner);
      expect(addressed).toHaveLength(1);
      expect(addressed[0]!.data).toMatchObject({ cause: "recovery-timeout", status: 409, error: "409 invalid_request_error: compaction_affinity_missing" });
      expect(Object.keys(addressed[0]!.data as object).sort()).toEqual(["cause", "code", "error", "held", "heldCount", "kind", "rootId", "since", "status", "type"]);
      expect(h.alarms).toHaveLength(1);
      expect(alarms.filter(item => item.kind === "fabric.main.provider-dead")).toHaveLength(2);
    } finally {
      h.main.closeFollowUpDrain();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
});
