import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { MainAgentController } from "../src/main-agent.js";

const roots: string[] = [];
const mains: MainAgentController[] = [];
const from = { id: "session:sender", name: "sender", kind: "main" as const };
const setup = (flushMs: number, journal = true) => {
  const handlers = new Map<string, Array<(event: any, ctx: ExtensionContext) => unknown>>();
  const sent: Array<{ message: any; options: any }> = [];
  const released = vi.fn();
  const pi = {
    on: (name: string, fn: (event: any, ctx: ExtensionContext) => unknown) => {
      handlers.set(name, [...(handlers.get(name) ?? []), fn]);
      return () => handlers.set(name, (handlers.get(name) ?? []).filter(h => h !== fn));
    },
    sendMessage: vi.fn((message, options) => sent.push({ message, options })),
    getThinkingLevel: () => "off",
  } as unknown as ExtensionAPI;
  const state = { idle: true, aborted: false };
  const ctx = { isIdle: () => state.idle, signal: { get aborted() { return state.aborted; } } } as unknown as ExtensionContext;
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "main-provider-backoff-")); roots.push(root);
  const main = new MainAgentController(pi, "session:root", true, root, "root", true, released);
  mains.push(main);
  main.attachFollowUpDrain(ctx, flushMs, journal ? path.join(root, "followups.json") : undefined);
  const emit = (name: string, event: any = {}) => { for (const fn of handlers.get(name) ?? []) fn(event, ctx); };
  const fail = () => {
    emit("agent_start");
    emit("turn_end", { message: { stopReason: "error" } });
    emit("agent_before_settle", { outcome: "error" });
    emit("agent_settled", { outcome: "error" });
  };
  const deliver = (delivery: "steer" | "followUp" = "followUp", extra = {}) => main.deliverAgent({ from, message: "resume work", delivery, ...extra });
  return { main, pi, ctx, emit, fail, deliver, sent, released, state, root };
};

beforeEach(() => { vi.useFakeTimers(); vi.setSystemTime(new Date("2026-10-01T08:34:16.663Z")); });
afterEach(() => {
  for (const main of mains.splice(0)) main.closeFollowUpDrain();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  vi.useRealTimers();
});

describe.each([0, 120_000])("Main provider backoff (flushMs=%s)", flushMs => {
  it.each(["followUp", "steer"] as const)("allows one %s turn after the first 60s backoff", delivery => {
    const { fail, deliver, sent } = setup(flushMs);
    fail();
    vi.advanceTimersByTime(60_000);
    expect(deliver(delivery)).toMatchObject({ queued: true, triggered: true });
    expect(sent).toHaveLength(1);
    expect(sent[0]!.options).toMatchObject({ deliverAs: delivery, triggerTurn: true });
    expect(vi.getTimerCount()).toBe(0);
  });

  it.each(["followUp", "steer"] as const)("holds a %s inside backoff, journals it and releases once at expiry", delivery => {
    const { fail, deliver, sent, main, root, released } = setup(flushMs);
    fail();
    vi.advanceTimersByTime(10_000);
    const receipt = deliver(delivery, { deliveryId: "durable-wake" });
    expect(receipt).toMatchObject({ queued: true, triggered: false, reason: "provider-backoff until 2026-10-01T08:35:16.663Z", pendingFollowUps: 1 });
    expect(sent).toHaveLength(0);
    expect(fs.readFileSync(path.join(root, "followups.json"), "utf8")).toContain("durable-wake");
    expect(deliver(delivery, { deliveryId: "durable-wake" })).toMatchObject({ duplicate: true, triggered: false });
    expect(vi.getTimerCount()).toBe(1);
    vi.advanceTimersByTime(49_999);
    expect(sent).toHaveLength(0);
    vi.advanceTimersByTime(1);
    expect(sent).toHaveLength(1);
    expect(sent[0]!.options).toMatchObject({ deliverAs: delivery, triggerTurn: true });
    expect(main.queueDepth().pendingFollowUps).toBe(0);
    expect(released).toHaveBeenCalledExactlyOnceWith({ until: "2026-10-01T08:35:16.663Z", messageIds: [receipt.messageId] });
    vi.advanceTimersByTime(60_000);
    expect(sent).toHaveLength(1);
    expect(vi.getTimerCount()).toBe(0);
  });

  it.each(["steer", "followUp"] as const)("replays %s passively after failed compaction without a retry timer, then wakes only fresh work", delivery => {
    const f = setup(flushMs);
    f.main.prepareReload();
    const replay = f.main.deliverAgent({ from, message: "reload replay", delivery, deliveryId: "reload-replay" });
    f.main.closeFollowUpDrain();
    f.state.idle = false;
    const replacement = new MainAgentController(f.pi, "session:root", true, f.root, "root", true, f.released);
    mains.push(replacement);
    replacement.attachFollowUpDrain(f.ctx, flushMs, path.join(f.root, "followups.json"));
    f.state.idle = true;
    f.emit("session_compact_failed", { reason: "manual", errorMessage: "provider unavailable", aborted: false });
    expect(f.sent.map(item => [item.message.details.id, item.options])).toEqual([
      [replay.messageId, { deliverAs: delivery, triggerTurn: false }],
    ]);
    expect(vi.getTimerCount()).toBe(0);
    expect(f.released).not.toHaveBeenCalled();
    const wake = replacement.deliverAgent({ from, message: "fresh wake", delivery: "followUp", deliveryId: "fresh-wake" });
    expect(wake).toMatchObject({ triggered: false, pendingFollowUps: 1 });
    expect(vi.getTimerCount()).toBe(1);
    vi.advanceTimersByTime(59_999);
    expect(f.sent).toHaveLength(1);
    vi.advanceTimersByTime(1);
    expect(f.sent.map(item => [item.message.details.id, item.options])).toEqual([
      [replay.messageId, { deliverAs: delivery, triggerTurn: false }],
      [wake.messageId, { deliverAs: "followUp", triggerTurn: true }],
    ]);
    expect(f.released).toHaveBeenCalledExactlyOnceWith({ until: "2026-10-01T08:35:16.663Z", messageIds: [wake.messageId] });
    expect(vi.getTimerCount()).toBe(0);
  });

  it("releases multiple held followUps as one wake and one event", () => {
    const { fail, deliver, sent, released } = setup(flushMs);
    fail(); const first = deliver(); const second = deliver();
    vi.advanceTimersByTime(60_000);
    expect(sent).toHaveLength(1);
    expect(sent[0]!.message.details.items).toHaveLength(2);
    expect(released).toHaveBeenCalledExactlyOnceWith({ until: "2026-10-01T08:35:16.663Z", messageIds: [first.messageId, second.messageId] });
  });

  it.each(["bytes", "provenance"])("keeps split %s batches behind the next consecutive failure deadline", split => {
    const f = setup(flushMs);
    if (split === "provenance") vi.spyOn(f.main, "supportsProvenance").mockReturnValue(true);
    f.fail();
    const first = f.main.deliverAgent({ from, message: split === "bytes" ? "a".repeat(40_000) : "first", delivery: "followUp", verification: "mesh" });
    const second = f.main.deliverAgent({ from: split === "provenance" ? { ...from, id: "session:other" } : from, message: split === "bytes" ? "b".repeat(40_000) : "second", delivery: "followUp", verification: "mesh" });
    vi.advanceTimersByTime(60_000);
    expect(f.sent).toHaveLength(1);
    expect(f.main.queueDepth().pendingFollowUps).toBe(1);
    expect(f.released).toHaveBeenCalledExactlyOnceWith({ until: "2026-10-01T08:35:16.663Z", messageIds: [first.messageId] });
    // Even an unrelated settlement/release cannot queue a native continuation before the retry outcome.
    f.emit("agent_settled", { outcome: "error" });
    f.fail();
    vi.advanceTimersByTime(119_999);
    expect(f.sent).toHaveLength(1);
    vi.advanceTimersByTime(1);
    expect(f.sent).toHaveLength(2);
    expect(f.sent[1]!.message.details.id).toBe(second.messageId);
    expect(f.main.queueDepth().pendingFollowUps).toBe(0);
    expect(f.released).toHaveBeenLastCalledWith({ until: "2026-10-01T08:37:16.663Z", messageIds: [second.messageId] });
  });

  it.each(["bytes", "provenance"])("keeps split %s batches behind the retry outcome after successful manual compaction", split => {
    const f = setup(flushMs);
    if (split === "provenance") vi.spyOn(f.main, "supportsProvenance").mockReturnValue(true);
    f.fail();
    const first = f.main.deliverAgent({ from, message: split === "bytes" ? "a".repeat(40_000) : "first", delivery: "followUp", verification: "mesh" });
    const second = f.main.deliverAgent({ from: split === "provenance" ? { ...from, id: "session:other" } : from, message: split === "bytes" ? "b".repeat(40_000) : "second", delivery: "followUp", verification: "mesh" });
    vi.advanceTimersByTime(10_000);
    f.state.idle = false;
    const operation = new AbortController();
    f.emit("session_before_compact", { reason: "manual", signal: operation.signal });
    f.emit("session_compact", { reason: "manual" });
    vi.advanceTimersByTime(100);
    expect(f.sent).toHaveLength(0);
    f.state.idle = true;
    vi.advanceTimersByTime(25);
    expect(f.sent).toHaveLength(1);
    expect(f.sent[0]!.message.details.id).toBe(first.messageId);
    expect(f.sent[0]!.options).toEqual({ deliverAs: "followUp", triggerTurn: true });
    expect(f.main.queueDepth().pendingFollowUps).toBe(1);
    expect(f.released).toHaveBeenCalledExactlyOnceWith({ until: "2026-10-01T08:35:16.663Z", messageIds: [first.messageId] });
    // Compaction made no provider call: neither queued batches nor fresh peers may
    // enter Pi's native continuation queue until this retry's outcome is known.
    const fresh = f.deliver("steer");
    expect(fresh).toMatchObject({ triggered: false, reason: "provider-retry in flight" });
    expect(f.main.queueDepth().pendingFollowUps).toBe(2);
    f.fail();
    const retryAt = Date.now() + 120_000;
    vi.advanceTimersByTime(119_999);
    expect(f.sent).toHaveLength(1);
    vi.advanceTimersByTime(1);
    expect(f.sent).toHaveLength(2);
    expect(f.sent[1]!.message.details.id).toBe(second.messageId);
    expect(f.main.queueDepth().pendingFollowUps).toBe(1);
    expect(f.released).toHaveBeenLastCalledWith({ until: new Date(retryAt).toISOString(), messageIds: [second.messageId] });
    // Only a real successful provider turn removes the guard for the remaining peer.
    f.emit("agent_start");
    f.emit("turn_end", { message: { stopReason: "stop" } });
    f.emit("agent_before_settle", { outcome: "completed" });
    f.emit("agent_settled", { outcome: "completed" });
    expect(f.sent).toHaveLength(3);
    expect(f.sent[2]!.message.details.id).toBe(fresh.messageId);
    expect(vi.getTimerCount()).toBe(0);
  });

  it.each(["aborted operation", "missing signal", "owner halt"])("does not release split batches after compaction with %s", control => {
    const f = setup(flushMs);
    f.fail();
    for (const message of ["a", "b"]) f.main.deliverAgent({ from, message: message.repeat(40_000), delivery: "followUp" });
    f.state.idle = false;
    const operation = new AbortController();
    if (control !== "missing signal") f.emit("session_before_compact", { reason: "manual", signal: operation.signal });
    f.emit("session_compact", { reason: "manual" });
    // Owner cancellation can arrive while a later completion handler is still running.
    if (control === "aborted operation") operation.abort();
    if (control === "owner halt") f.main.halt();
    f.state.idle = true;
    vi.advanceTimersByTime(180_000);
    expect(f.sent).toHaveLength(0);
    expect(f.main.queueDepth().pendingFollowUps).toBe(2);
    expect(f.released).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
  });

  it.each(["steer", "followUp"] as const)("releases held %s after successful manual compaction even with drain disabled", delivery => {
    const f = setup(flushMs);
    f.fail(); const wake = f.deliver(delivery);
    f.state.idle = false;
    const operation = new AbortController();
    f.emit("session_before_compact", { reason: "manual", signal: operation.signal });
    f.emit("session_compact", { reason: "manual" });
    expect(f.sent).toHaveLength(0);
    vi.advanceTimersByTime(100);
    expect(f.sent).toHaveLength(0);
    f.state.idle = true;
    vi.advanceTimersByTime(25);
    expect(f.sent).toHaveLength(1);
    expect(f.sent[0]!.options).toEqual({ deliverAs: delivery, triggerTurn: true });
    expect(f.released).toHaveBeenCalledExactlyOnceWith({ until: "2026-10-01T08:35:16.663Z", messageIds: [wake.messageId] });
    vi.advanceTimersByTime(120_000);
    expect(f.sent).toHaveLength(1);
    expect(vi.getTimerCount()).toBe(0);
  });

  it.each(["steer", "followUp"] as const)("holds new %s wakes while a direct deadline retry is in flight", delivery => {
    const f = setup(flushMs);
    f.fail(); vi.advanceTimersByTime(60_000);
    expect(f.deliver().triggered).toBe(true);
    const later = f.deliver(delivery);
    expect(later).toMatchObject({ triggered: false, reason: "provider-retry in flight", pendingFollowUps: 1 });
    expect(f.sent).toHaveLength(1);
    expect(vi.getTimerCount()).toBe(0);
    f.fail();
    vi.advanceTimersByTime(119_999); expect(f.sent).toHaveLength(1);
    vi.advanceTimersByTime(1); expect(f.sent).toHaveLength(2);
    expect(f.sent[1]!.message.details.id).toBe(later.messageId);
  });

  it("reports one release when delivery at the deadline wins the timer race", () => {
    const { fail, deliver, sent, released } = setup(flushMs);
    fail(); deliver();
    vi.setSystemTime(Date.now() + 60_000);
    expect(deliver()).toMatchObject({ triggered: true });
    expect(sent).toHaveLength(1);
    expect(released).toHaveBeenCalledTimes(1);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("releases on successful automatic recovery before the deadline", () => {
    const { fail, deliver, emit, sent, released } = setup(flushMs);
    fail(); deliver(); vi.advanceTimersByTime(10_000);
    // Pi retries can recover in the same run, without another agent_start or user input.
    emit("turn_end", { message: { stopReason: "stop" } });
    emit("agent_before_settle", { outcome: "completed" }); emit("agent_settled", { outcome: "completed" });
    expect(sent).toHaveLength(1);
    expect(sent[0]!.options.triggerTurn).toBe(true);
    expect(released).toHaveBeenCalledTimes(1);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("closing a drain without a journal does not rearm a provider timer", () => {
    const { fail, deliver, main, sent, released } = setup(flushMs, false);
    fail(); deliver(); main.closeFollowUpDrain();
    expect(vi.getTimerCount()).toBe(0);
    expect(sent).toHaveLength(1);
    expect(sent[0]!.options.triggerTurn).toBe(false);
    expect(released).not.toHaveBeenCalled();
  });

  it("doubles consecutive failures, counts lifecycle notifications once, and caps at 30min", () => {
    const { fail, deliver, sent } = setup(flushMs);
    for (const delay of [60_000, 120_000, 240_000, 480_000, 960_000, 1_800_000, 1_800_000]) {
      const failureAt = Date.now();
      fail();
      expect(deliver()).toMatchObject({ triggered: false, reason: `provider-backoff until ${new Date(failureAt + delay).toISOString()}` });
      const count = sent.length;
      vi.advanceTimersByTime(delay - 1);
      expect(sent).toHaveLength(count);
      vi.advanceTimersByTime(1);
      expect(sent).toHaveLength(count + 1);
      expect(sent.at(-1)!.options.triggerTurn).toBe(true);
    }
  });

  it.each(["Escape before delivery", "Escape while held", "aborted signal"])("retains user halt: %s, with no wake timer", cause => {
    const { main, fail, deliver, sent, state, released } = setup(flushMs);
    fail();
    if (cause === "Escape while held") { deliver(); expect(vi.getTimerCount()).toBe(1); }
    if (cause === "aborted signal") { state.aborted = true; } else main.halt();
    expect(deliver()).toMatchObject({ triggered: false });
    expect(vi.getTimerCount()).toBe(0);
    vi.advanceTimersByTime(3_600_000);
    expect(sent.every(item => !item.options.triggerTurn)).toBe(true);
    expect(released).not.toHaveBeenCalled();
  });

  it("counts successive failed turns in one retrying run separately", () => {
    const { emit, deliver } = setup(flushMs);
    emit("agent_start");
    emit("turn_end", { message: { stopReason: "error" } });
    vi.advanceTimersByTime(1_000);
    emit("turn_end", { message: { stopReason: "error" } });
    emit("agent_before_settle", { outcome: "error" });
    emit("agent_settled", { outcome: "error" });
    expect(deliver()).toMatchObject({ reason: `provider-backoff until ${new Date(Date.now() + 120_000).toISOString()}` });
  });

  it("does not report an intentional long backoff as a stalled queue", () => {
    const { fail, deliver } = setup(flushMs);
    for (const delay of [60_000, 120_000, 240_000, 480_000, 960_000]) { fail(); vi.advanceTimersByTime(delay); }
    fail(); deliver(); vi.advanceTimersByTime(601_000);
    expect(deliver()).not.toHaveProperty("stalled");
  });

  it("resets consecutive failures after a successful turn", () => {
    const { emit, fail, deliver, sent } = setup(flushMs);
    fail(); vi.advanceTimersByTime(60_000); deliver();
    fail(); vi.advanceTimersByTime(120_000); deliver();
    emit("agent_start"); emit("turn_end", { message: { stopReason: "stop" } });
    emit("agent_before_settle", { outcome: "completed" }); emit("agent_settled", { outcome: "completed" });
    const count = sent.length;
    const failureAt = Date.now();
    fail();
    expect(deliver()).toMatchObject({ triggered: false, reason: `provider-backoff until ${new Date(failureAt + 60_000).toISOString()}` });
    vi.advanceTimersByTime(60_000);
    expect(sent).toHaveLength(count + 1);
  });

  it("does not arm a retry for passive or nextTurn context", () => {
    const { fail, main, sent } = setup(flushMs);
    fail();
    expect(main.deliverAgent({ from, message: "passive", delivery: "followUp", triggerTurn: false }).triggered).toBe(false);
    expect(main.deliverAgent({ from, message: "context only", delivery: "nextTurn" }).triggered).toBe(false);
    expect(sent).toHaveLength(2);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("holds existing busy followUps through failure settlement instead of stranding them in Pi", () => {
    const { state, deliver, fail, sent } = setup(flushMs);
    if (flushMs === 0) return; // Pi owns the busy queue with the drain disabled.
    state.idle = false; deliver();
    state.idle = true; fail();
    expect(sent).toHaveLength(0);
    vi.advanceTimersByTime(60_000);
    expect(sent).toHaveLength(1);
    expect(sent[0]!.options.triggerTurn).toBe(true);
  });

  it("cancels the timer at reload and close; journalled permission survives reload", () => {
    const { fail, deliver, main, sent, root } = setup(flushMs);
    fail(); const receipt = deliver("steer", { deliveryId: "reload-wake" });
    main.prepareReload();
    expect(vi.getTimerCount()).toBe(0);
    main.closeFollowUpDrain();
    vi.advanceTimersByTime(60_000);
    expect(sent).toHaveLength(0);
    const journal = JSON.parse(fs.readFileSync(path.join(root, "followups.json"), "utf8"));
    expect(journal.items).toContainEqual(expect.objectContaining({ id: receipt.messageId, deliverAs: "steer", triggerTurn: true }));
  });
});
