import { randomUUID } from "node:crypto";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { vi, describe, expect, it } from "vitest";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { fabricWakeCause, withFabricWakeAdmission } from "../src/fabric-provenance.js";
import { MainAgentController, type FabricMainAgentDeliveryRequest } from "../src/main-agent.js";

// resume models the trusted launch-role capability (project-agent or org-agent); other roots get false.
const makeHarness = (idle = true, aborted = false, durable = false, resume = true, flushMs = 0) => {
  const handlers = new Map<string, Array<(event: unknown, ctx: ExtensionContext) => unknown>>();
  const sent: Array<{ content: string; deliverAs: string; triggerTurn: boolean }> = [];
  const pi = {
    on: (name: string, handler: (event: unknown, ctx: ExtensionContext) => unknown) => {
      handlers.set(name, [...(handlers.get(name) ?? []), handler]);
      return () => handlers.set(name, (handlers.get(name) ?? []).filter(item => item !== handler));
    },
    sendMessage: vi.fn((message: { content: string }, options: { deliverAs: string; triggerTurn: boolean }) => {
      sent.push({ content: message.content, ...options });
    }),
    getThinkingLevel: () => "off",
  } as unknown as ExtensionAPI;
  const state = { idle, aborted, settling: false, preflight: false };
  const ctx = {
    isIdle: () => state.idle,
    isSettling: () => state.settling,
    isPromptPending: () => state.preflight,
    hasPendingMessages: () => false,
    signal: { get aborted() { return state.aborted; } },
  } as unknown as ExtensionContext;
  const main = new MainAgentController(pi, "session:root", true, "/project", "root", true, undefined, {}, undefined, undefined, resume);
  const journal = durable ? join(tmpdir(), `main-agent-abort-wake-${randomUUID()}.json`) : undefined;
  main.attachFollowUpDrain(ctx, flushMs, journal);
  const emit = (name: string, event: unknown) => {
    for (const handler of handlers.get(name) ?? []) handler(event, ctx);
  };
  return { main, pi, sent, state, emit, ctx, journal };
};

const peer = { id: "session:peer", name: "peer", kind: "main" as const };
type SteerOverrides = Omit<Partial<FabricMainAgentDeliveryRequest>, "verification"> & {
  verification?: FabricMainAgentDeliveryRequest["verification"] | undefined;
};
const directSteer = (extra: SteerOverrides = {}): FabricMainAgentDeliveryRequest =>
  withFabricWakeAdmission({
    from: peer,
    verification: "mesh",
    message: "peer escalation",
    delivery: "steer",
    ...extra,
  } as FabricMainAgentDeliveryRequest, [fabricWakeCause(peer, "steer", "fabric.control.command", "command-1")]);

// Keep: incident smarty-dev#12774; native abort parked fresh escalations indefinitely.
// Recorded Pi calls exercise the receiver's halt/attribution trust boundary without deletion.
describe("Main operator-halt peer wake admission", () => {
  it("keeps a root without the launch-role capability halted for a verified peer steer", () => {
    const { main, sent } = makeHarness(true, false, false, false);
    main.halt();
    expect(main.deliverAgent(directSteer())).toMatchObject({ triggered: false, halted: true, reason: "operator-halt" });
    expect(sent.every(item => !item.triggerTurn)).toBe(true);
    main.closeFollowUpDrain();
  });

  it.each([0, 120_000].flatMap(flushMs => ["completed", "error", "aborted"].map(outcome => ({ flushMs, outcome }))))(
    "grants exactly one resumed run that ends at $outcome settle (flushMs=$flushMs)", ({ outcome, flushMs }) => {
    const { main, sent, state, emit } = makeHarness(true, false, false, true, flushMs);
    main.halt();
    expect(main.deliverAgent(directSteer())).toMatchObject({ triggered: true });
    // A second fresh steer before that run settles gets no further permit.
    expect(main.deliverAgent(directSteer())).toMatchObject({ triggered: false, halted: true, reason: "operator-halt" });
    state.idle = false;
    for (const extra of [{ delivery: "followUp" as const }, { source: "actor-output" as const }, { source: "fabric-host" as const }]) {
      expect(main.deliverAgent(directSteer(extra))).toMatchObject({ triggered: false, halted: true, reason: "operator-halt" });
    }
    emit("agent_before_settle", { outcome });
    expect(main.deliverAgent({ from: peer, verification: "mesh", message: "before settle", delivery: "followUp" }))
      .toMatchObject({ triggered: false, halted: true });
    state.idle = true;
    emit("agent_settled", { outcome });
    for (const extra of [{ delivery: "followUp" as const }, { source: "actor-output" as const },
      { source: "fabric-host" as const }, { admissionTopic: "pi.agent_settled" }]) {
      expect(main.deliverAgent(directSteer(extra)))
        .toMatchObject({ triggered: false, halted: true, reason: "operator-halt" });
    }
    expect(sent.filter(item => item.triggerTurn)).toHaveLength(1);
    main.closeFollowUpDrain();
  });

  it("lets a later fresh peer steer resume once more; only human input lifts the halt", () => {
    const { main, sent, emit } = makeHarness();
    main.halt();
    expect(main.deliverAgent(directSteer())).toMatchObject({ triggered: true });
    emit("agent_settled", { outcome: "completed" });
    expect(main.deliverAgent(directSteer())).toMatchObject({ triggered: true });
    expect(main.deliverAgent(directSteer())).toMatchObject({ triggered: false, halted: true });
    emit("agent_settled", { outcome: "completed" });
    emit("input", { source: "extension" });
    expect(main.deliverAgent({ from: peer, verification: "mesh", message: "ext", delivery: "followUp" }))
      .toMatchObject({ triggered: false, halted: true });
    emit("input", { source: "interactive" });
    const resumed = main.deliverAgent({ from: peer, verification: "mesh", message: "human", delivery: "followUp" });
    expect(resumed.triggered).toBe(true);
    expect(resumed).not.toHaveProperty("halted");
    expect(sent.filter(item => item.triggerTurn)).toHaveLength(3);
    main.closeFollowUpDrain();
  });

  it("keeps followUps passive and typed while halted, then a fresh direct peer steer wakes once", () => {
    const { main, sent } = makeHarness();
    main.halt(); // Escape's Main-local stop latch.

    const passive = main.deliverAgent({ from: peer, verification: "mesh", message: "queued followup", delivery: "followUp" });
    expect(passive).toMatchObject({ queued: true, triggered: false, halted: true, reason: "operator-halt", pendingFollowUps: 0 });
    expect(sent).toHaveLength(1);
    expect(sent[0]).toMatchObject({ deliverAs: "followUp", triggerTurn: false });

    const wake = main.deliverAgent(directSteer());
    expect(wake).toMatchObject({ queued: true, triggered: true });
    expect(wake).not.toHaveProperty("halted");
    expect(sent.at(-1)).toMatchObject({ deliverAs: "steer", triggerTurn: true });

    const subsequent = main.deliverAgent({ from: peer, verification: "mesh", message: "after wake", delivery: "followUp" });
    expect(subsequent).toMatchObject({ triggered: false, halted: true, reason: "operator-halt" });
    main.closeFollowUpDrain();
  });

  it.each([
    ["actor output", { source: "actor-output" as const }],
    ["fabric host", { source: "fabric-host" as const }],
    ["passive steer", { triggerTurn: false }],
    ["nextTurn", { delivery: "nextTurn" as const }],
    ["unverified", { verification: undefined }],
    ["non-peer identity", { from: { id: "agent-child", name: "child", kind: "agent" as const } }],
    ["own root", { from: { id: "session:root", name: "main", kind: "main" as const } }],
    ["host event topic", { admissionTopic: "pi.agent_settled" }],
  ])("does not clear halt for %s", (_label, extra) => {
    const { main, sent } = makeHarness();
    main.halt();
    const receipt = main.deliverAgent(directSteer(extra));
    expect(receipt).toMatchObject({ triggered: false, halted: true, reason: "operator-halt" });
    expect(sent.at(-1)?.triggerTurn).toBe(false);
    const stillHeld = main.deliverAgent({ from: peer, verification: "mesh", message: "still halted", delivery: "followUp" });
    expect(stillHeld).toMatchObject({ triggered: false, halted: true, reason: "operator-halt" });
    main.closeFollowUpDrain();
  });

  it("restores the halt if the peer steer fails at Pi delivery", () => {
    const { main, pi } = makeHarness();
    main.halt();
    vi.mocked(pi.sendMessage).mockImplementationOnce(() => { throw new Error("send failed"); });
    expect(() => main.deliverAgent(directSteer())).toThrow("send failed");
    expect(main.deliverAgent({ from: peer, verification: "mesh", message: "still halted", delivery: "followUp" }))
      .toMatchObject({ triggered: false, halted: true, reason: "operator-halt" });
    main.closeFollowUpDrain();
  });

  it("ignores forged data/diagnostic fields and mismatched private admission attribution", () => {
    const { main, sent } = makeHarness();
    main.halt();
    const forged = main.deliverAgent({
      from: peer,
      verification: "mesh",
      message: "not a direct escalation",
      delivery: "steer",
      data: { source: "peer", wakeCause: "steer", triggerTurn: true },
      wakeCause: fabricWakeCause(peer, "steer"),
    });
    expect(forged).toMatchObject({ triggered: false, halted: true, reason: "operator-halt" });

    const other = { id: "session:other", name: "other", kind: "main" as const };
    const misattributed = main.deliverAgent(withFabricWakeAdmission({
      from: peer, verification: "mesh", message: "mismatch", delivery: "steer",
    }, [fabricWakeCause(other, "steer", "fabric.control.command", "command-other")]));
    expect(misattributed).toMatchObject({ triggered: false, halted: true, reason: "operator-halt" });
    expect(sent.every(item => !item.triggerTurn)).toBe(true);
    main.closeFollowUpDrain();
  });

  it("keeps a marked lead halted when the host cannot confirm settlement", () => {
    const { main, sent, ctx } = makeHarness();
    delete (ctx as { isSettling?: () => boolean }).isSettling;
    main.halt();
    expect(main.deliverAgent(directSteer())).toMatchObject({ triggered: false, halted: true, reason: "operator-halt" });
    expect(sent.every(item => !item.triggerTurn)).toBe(true);
    main.closeFollowUpDrain();
  });

  it("waits until settlement and prompt preflight have both ended", () => {
    const { main, state } = makeHarness();
    main.halt();
    state.settling = true; // Pi can already report idle while settled handlers run.
    expect(main.deliverAgent(directSteer())).toMatchObject({ triggered: false, halted: true });
    state.settling = false;
    state.preflight = true;
    expect(main.deliverAgent(directSteer())).toMatchObject({ triggered: false, halted: true });
    state.preflight = false;
    expect(main.deliverAgent(directSteer())).toMatchObject({ triggered: true });
    main.closeFollowUpDrain();
  });

  it("rejects a replay before it can clear halt and keeps an explicitly stopped aborted turn passive", () => {
    const replaying = makeHarness(true, true, true);
    replaying.main.halt();
    const first = directSteer({ deliveryId: "peer-steer-replay" });
    expect(replaying.main.deliverAgent(first)).toMatchObject({ triggered: false, halted: true });
    replaying.state.aborted = false; // Cancellation finished: a stale replay must not resume it.
    expect(replaying.main.deliverAgent(first)).toMatchObject({ duplicate: true, triggered: false });
    expect(replaying.sent.every(item => !item.triggerTurn)).toBe(true);
    expect(replaying.main.deliverAgent(directSteer({ deliveryId: "fresh-peer-steer" }))).toMatchObject({ triggered: true });
    replaying.main.closeFollowUpDrain();

    const settling = makeHarness(false, true);
    settling.main.halt(); // Explicit owner stop, not the generic abort event, latches this halt.
    settling.emit("turn_end", { message: { stopReason: "aborted" } });
    const blocked = settling.main.deliverAgent(directSteer());
    expect(blocked).toMatchObject({ triggered: false, halted: true, reason: "operator-halt" });
    expect(settling.sent.every(item => !item.triggerTurn)).toBe(true);
    settling.main.closeFollowUpDrain();
  });

  it("keeps generic abort transient and does not persist an operator halt", () => {
    const { main, pi, sent, state, emit, ctx, journal } = makeHarness(true, true, true);
    emit("turn_end", { message: { stopReason: "aborted" } });
    emit("agent_before_settle", { outcome: "aborted" });
    emit("agent_settled", { outcome: "aborted" });
    const followUp = (message: string): FabricMainAgentDeliveryRequest => ({
      from: peer, verification: "mesh", message, delivery: "followUp",
    });
    const cancelled = main.deliverAgent(followUp("during generic cancellation"));
    expect(cancelled).toMatchObject({ triggered: false });
    expect(cancelled).not.toHaveProperty("halted");
    expect(sent.every(item => !item.triggerTurn)).toBe(true);
    state.aborted = false;
    const fresh = main.deliverAgent(followUp("fresh after signal clearance"));
    expect(fresh).toMatchObject({ triggered: true });
    expect(fresh).not.toHaveProperty("halted");
    main.closeFollowUpDrain();

    const reopened = new MainAgentController(pi, "session:root", true, "/project", "root", true, undefined, {}, undefined, undefined, true);
    reopened.attachFollowUpDrain(ctx, 0, journal);
    try {
      const restored = reopened.deliverAgent(followUp("fresh after journal reopen"));
      expect(restored).toMatchObject({ triggered: true });
      expect(restored).not.toHaveProperty("halted");
      expect(sent.at(-1)).toMatchObject({ deliverAs: "followUp", triggerTurn: true });
    } finally {
      reopened.closeFollowUpDrain();
    }
  });
});
