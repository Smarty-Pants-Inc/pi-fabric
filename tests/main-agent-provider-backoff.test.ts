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
  // A classified retryable error (5xx): an unclassified one (no status or body) alarms on the third
  // in a row instead (smarty-dev#8742, tests/main-provider-dead.test.ts).
  const fail = () => {
    emit("agent_start");
    emit("turn_end", { message: { stopReason: "error", errorMessage: "503 upstream connect error" } });
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
    vi.advanceTimersByTime(50_000);
    expect(f.sent).toHaveLength(0);
    expect(f.released).not.toHaveBeenCalled();
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

  it.each(["steer", "followUp"] as const)("holds a fresh %s racing expiry until preflight and other busy work both end", delivery => {
    const f = setup(flushMs);
    f.fail();
    const oldest = f.main.deliverAgent({ from, message: "a".repeat(40_000), delivery: "followUp" });
    f.state.idle = false;
    let preflight = true;
    Object.assign(f.ctx, { isPromptPending: () => preflight });
    // Move the clock without running the already armed deadline callback.
    vi.setSystemTime(Date.now() + 60_000);
    const newest = f.deliver(delivery, { message: "b".repeat(40_000) });
    expect(newest).toMatchObject({ triggered: false, pendingFollowUps: 2 });
    expect(newest.reason).not.toBe("provider-retry in flight");
    expect(f.sent).toHaveLength(0);
    vi.advanceTimersByTime(60_000);
    expect(f.sent).toHaveLength(0);
    preflight = false;
    vi.advanceTimersByTime(1_000);
    expect(f.sent).toHaveLength(0);
    expect(f.released).not.toHaveBeenCalled();
    expect(f.main.queueDepth().pendingFollowUps).toBe(2);
    f.state.idle = true;
    vi.advanceTimersByTime(25);
    expect(f.sent.map(item => item.message.details.id)).toEqual([oldest.messageId]);
    expect(f.sent[0]!.options).toEqual({ deliverAs: "followUp", triggerTurn: true });
    expect(f.released).toHaveBeenCalledExactlyOnceWith({ until: "2026-10-01T08:35:16.663Z", messageIds: [oldest.messageId] });
    expect(f.main.queueDepth().pendingFollowUps).toBe(1);
    // Compaction cannot clear the admitted attempt or replay its handed item.
    f.emit("session_compact", { reason: "threshold" });
    vi.advanceTimersByTime(120_000);
    expect(f.sent).toHaveLength(1);
    expect(f.deliver("steer").reason).toBe("provider-retry in flight");
    f.emit("agent_start");
    f.emit("turn_end", { message: { stopReason: "stop" } });
    f.emit("agent_before_settle", { outcome: "completed" });
    f.emit("agent_settled", { outcome: "completed" });
    expect(f.sent[1]!.message.details.id).toBe(newest.messageId);
    expect(f.sent.flatMap(item => (item.message.details.items ?? [item.message.details]).map((entry: { id: string }) => entry.id))
      .filter(id => id === oldest.messageId)).toEqual([oldest.messageId]);
    expect(f.main.queueDepth().pendingFollowUps).toBe(0);
    expect(vi.getTimerCount()).toBe(0);
  });

  it.each([false, true].flatMap(preflight =>
    ["cancel before completion", "cancel during completion", "missing signal", "owner halt"].map(control => ({ control, preflight }))))(
    "retains $control after an expired busy deadline through slow completion (preflight=$preflight)", ({ control, preflight: initialPreflight }) => {
      const f = setup(flushMs);
      f.fail();
      for (const message of ["a", "b"]) f.main.deliverAgent({ from, message: message.repeat(40_000), delivery: "followUp" });
      f.state.idle = false;
      let preflight = initialPreflight;
      Object.assign(f.ctx, { isPromptPending: () => preflight });
      const operation = new AbortController();
      if (control !== "missing signal") f.emit("session_before_compact", { reason: "manual", signal: operation.signal });
      vi.advanceTimersByTime(60_000);
      expect(f.sent).toHaveLength(0);
      expect(f.released).not.toHaveBeenCalled();
      if (control === "cancel before completion") operation.abort();
      vi.advanceTimersByTime(1_000);
      f.emit("session_compact", { reason: "manual" });
      if (control === "cancel during completion") operation.abort();
      if (control === "owner halt") f.main.halt();
      preflight = false;
      vi.advanceTimersByTime(1_000);
      expect(f.sent).toHaveLength(0);
      f.state.idle = true;
      vi.advanceTimersByTime(180_000);
      expect(f.sent).toHaveLength(0);
      expect(f.released).not.toHaveBeenCalled();
      expect(f.main.queueDepth().pendingFollowUps).toBe(2);
      expect(vi.getTimerCount()).toBe(0);
    },
  );

  it.each(["steer", "followUp"] as const)("defers the first fresh %s after expiry even with no earlier held item or timer", delivery => {
    const f = setup(flushMs);
    f.fail();
    f.state.idle = false;
    vi.setSystemTime(Date.now() + 60_000);
    const receipt = f.deliver(delivery);
    expect(receipt).toMatchObject({ triggered: false, pendingFollowUps: 1 });
    expect(receipt.reason).not.toBe("provider-retry in flight");
    vi.advanceTimersByTime(1_000);
    expect(f.sent).toHaveLength(0);
    expect(f.released).not.toHaveBeenCalled();
    f.state.idle = true;
    vi.advanceTimersByTime(25);
    expect(f.sent.map(item => item.message.details.id)).toEqual([receipt.messageId]);
    expect(f.sent[0]!.options).toEqual({ deliverAs: delivery, triggerTurn: true });
    expect(f.released).toHaveBeenCalledExactlyOnceWith({ until: "2026-10-01T08:35:16.663Z", messageIds: [receipt.messageId] });
    expect(vi.getTimerCount()).toBe(0);
  });

  it("retains the operation signal when compaction reaches idle before prompt preflight clears", () => {
    const f = setup(flushMs);
    f.fail(); f.deliver();
    f.state.idle = false;
    let preflight = true;
    Object.assign(f.ctx, { isPromptPending: () => preflight });
    const operation = new AbortController();
    f.emit("session_before_compact", { reason: "manual", signal: operation.signal });
    vi.advanceTimersByTime(60_000);
    f.emit("session_compact", { reason: "manual" });
    f.state.idle = true;
    vi.advanceTimersByTime(25);
    expect(f.sent).toHaveLength(0);
    operation.abort();
    preflight = false;
    vi.advanceTimersByTime(1_000);
    expect(f.sent).toHaveLength(0);
    expect(f.released).not.toHaveBeenCalled();
    expect(f.main.queueDepth().pendingFollowUps).toBe(1);
    expect(vi.getTimerCount()).toBe(0);
  });

  // Failure completion can leave another native handler busy without a later settle.
  // Cover both timer ownership states and the native recoverable timeout envelope.
  it.each([false, true].flatMap(expired => ["no-op", "retry pending", "provider error", "native timeout"].map(failure => ({ expired, failure }))))(
    "rearms recoverable $failure after busy failure completion (expired=$expired)", ({ expired, failure }) => {
      const f = setup(flushMs);
      f.fail();
      const oldest = f.deliver("followUp", { message: "a".repeat(40_000), deliveryId: "oldest" });
      const later = f.deliver("followUp", { message: "b".repeat(40_000), deliveryId: "later" });
      const originalDeadline = Date.now() + 60_000;
      const operation = new AbortController();
      f.state.idle = false;
      let preflight = true;
      Object.assign(f.ctx, { isPromptPending: () => preflight });
      f.emit("session_before_compact", { reason: "manual", signal: operation.signal });
      // A fresh delivery must not rescue the FIFO or claim an attempt while still busy.
      const fresh = f.deliver("steer", { deliveryId: "fresh" });
      expect(fresh).toMatchObject({ triggered: false, pendingFollowUps: 3 });
      expect(fresh.reason).not.toBe("provider-retry in flight");
      vi.advanceTimersByTime(expired ? 60_000 : 10_000);
      if (failure === "native timeout") {
        operation.abort(new DOMException("Operation deadline exceeded", "TimeoutError"));
        // A preflight tick may observe the timeout before the failed event arrives.
        vi.advanceTimersByTime(25);
      }
      const deadline = failure === "provider error" ? Date.now() + 120_000 : originalDeadline;
      f.emit("session_compact_failed", {
        reason: "manual", aborted: failure === "native timeout", willRetry: failure === "retry pending",
        errorMessage: failure === "no-op" ? "Nothing to compact (session too small)" : "provider unavailable",
      });
      preflight = false;
      vi.advanceTimersByTime(100);
      expect(f.sent).toHaveLength(0);
      expect(f.released).not.toHaveBeenCalled();
      expect(f.main.queueDepth().pendingFollowUps).toBe(3);
      const journal = JSON.parse(fs.readFileSync(path.join(f.root, "followups.json"), "utf8"));
      expect(journal.items.map((item: { id: string; handed?: boolean }) => [item.id, !!item.handed]))
        .toEqual([oldest, later, fresh].map(item => [item.messageId, false]));
      // No input, settle or further peer delivery is allowed to rescue the retry.
      f.state.idle = true;
      if (deadline > Date.now()) {
        vi.advanceTimersByTime(deadline - Date.now() - 1);
        expect(f.sent).toHaveLength(0);
        expect(f.released).not.toHaveBeenCalled();
        vi.advanceTimersByTime(1);
      } else vi.advanceTimersByTime(25);
      expect(f.sent.map(item => item.message.details.id)).toEqual([oldest.messageId]);
      expect(f.sent[0]!.options).toEqual({ deliverAs: "followUp", triggerTurn: true });
      expect(f.released).toHaveBeenCalledExactlyOnceWith({ until: new Date(deadline).toISOString(), messageIds: [oldest.messageId] });
      expect(f.main.queueDepth().pendingFollowUps).toBe(2);
      expect(f.deliver("followUp", { deliveryId: "fresh" })).toMatchObject({ duplicate: true, triggered: false });
      f.emit("agent_start");
      vi.advanceTimersByTime(180_000);
      expect(f.sent).toHaveLength(1);
      expect(f.deliver("steer", { deliveryId: "in-flight" }).reason).toBe("provider-retry in flight");
      // Only actual provider success opens the rest of the FIFO, with no original replay.
      f.emit("turn_end", { message: { stopReason: "stop" } });
      f.emit("agent_before_settle", { outcome: "completed" });
      f.emit("agent_settled", { outcome: "completed" });
      const handed = f.sent.flatMap(item => (item.message.details.items ?? [item.message.details]).map((entry: { id: string }) => entry.id));
      expect(handed.slice(0, 3)).toEqual([oldest.messageId, later.messageId, fresh.messageId]);
      expect(handed.filter(id => id === oldest.messageId)).toEqual([oldest.messageId]);
      expect(f.main.queueDepth().pendingFollowUps).toBe(0);
      expect(vi.getTimerCount()).toBe(0);
    },
  );

  it.each([false, true].flatMap(expired => [false, true].map(observed => ({ expired, observed }))))(
    "a benign extension decline preserves the retry (expired=$expired, observed signal=$observed)", ({ expired, observed }) => {
      const f = setup(flushMs);
      f.fail();
      const oldest = f.deliver("followUp", { message: "a".repeat(40_000) });
      const later = f.deliver("followUp", { message: "b".repeat(40_000) });
      f.state.idle = false;
      const operation = new AbortController();
      if (observed) f.emit("session_before_compact", { reason: "manual", signal: operation.signal });
      vi.advanceTimersByTime(expired ? 61_000 : 10_000);
      expect(f.sent).toHaveLength(0);
      f.state.idle = true;
      // Pi reports extension {cancel:true} as aborted, without aborting its signal.
      f.emit("session_compact_failed", { reason: "manual", aborted: true, willRetry: false });
      expect(operation.signal.aborted).toBe(false);
      expect(vi.getTimerCount()).toBeGreaterThan(0);
      vi.advanceTimersByTime(expired ? 25 : 50_000);
      expect(f.sent.map(item => item.message.details.id)).toEqual([oldest.messageId]);
      expect(f.main.queueDepth().pendingFollowUps).toBe(1);
      f.emit("agent_start");
      f.emit("turn_end", { message: { stopReason: "stop" } });
      f.emit("agent_before_settle", { outcome: "completed" });
      f.emit("agent_settled", { outcome: "completed" });
      expect(f.sent.map(item => item.message.details.id)).toEqual([oldest.messageId, later.messageId]);
      expect(vi.getTimerCount()).toBe(0);
    },
  );

  it("a declined compaction resumes a pending provider recovery at the idle edge", () => {
    const f = setup(flushMs);
    const compact = vi.fn();
    Object.assign(f.ctx, { hasUI: false, compact });
    f.state.idle = false;
    f.emit("agent_start");
    f.emit("turn_end", { message: { stopReason: "error", errorMessage: `409 ${JSON.stringify({ type: "error", error: { type: "invalid_request_error", message: "compaction_affinity_missing" } })}` } });
    f.emit("session_before_compact", { reason: "manual", signal: new AbortController().signal });
    f.emit("agent_before_settle", { outcome: "error" });
    f.emit("agent_settled", { outcome: "error" });
    vi.advanceTimersByTime(10);
    expect(f.main.providerDeadState?.phase).toBe("recovering");
    expect(compact).not.toHaveBeenCalled();
    f.state.idle = true;
    f.emit("session_compact_failed", { reason: "manual", aborted: true, willRetry: false });
    vi.advanceTimersByTime(10);
    expect(compact).toHaveBeenCalledTimes(1);
  });

  it("a cancelled tree summary cannot veto a later preflight-deferred retry", () => {
    const f = setup(flushMs);
    f.fail();
    const original = f.deliver();
    const tree = new AbortController();
    f.emit("session_before_tree", { signal: tree.signal });
    tree.abort(); // A cancelled summary emits no session_tree completion event.
    let preflight = true;
    Object.assign(f.ctx, { isPromptPending: () => preflight });
    vi.advanceTimersByTime(61_000);
    expect(f.sent).toHaveLength(0);
    preflight = false;
    vi.advanceTimersByTime(100);
    expect(f.sent.map(item => item.message.details.id)).toEqual([original.messageId]);
    expect(vi.getTimerCount()).toBe(0);
  });

  it.each(["deadline", "compaction"])("reports an idle wait, not an expired backoff, after %s", boundary => {
    const f = setup(flushMs);
    f.fail(); f.deliver();
    f.state.idle = false;
    f.emit("session_before_compact", { reason: "manual", signal: new AbortController().signal });
    if (boundary === "deadline") vi.advanceTimersByTime(61_000);
    else f.emit("session_compact", { reason: "manual" });
    expect(f.deliver().reason).toBe("provider-retry waiting for idle");
    expect(f.sent).toHaveLength(0);
  });

  it.each([false, true].flatMap(expired => ["operation cancel", "context abort", "owner halt", "reload", "close", "run start"].map(control => ({ expired, control }))))(
    "retains $control across recoverable failure retirement (expired=$expired)", ({ expired, control }) => {
      const f = setup(flushMs);
      f.fail();
      const original = f.deliver();
      f.state.idle = false;
      let preflight = true;
      Object.assign(f.ctx, { isPromptPending: () => preflight });
      const operation = new AbortController();
      f.emit("session_before_compact", { reason: "manual", signal: operation.signal });
      vi.advanceTimersByTime(expired ? 60_000 : 10_000);
      if (control === "operation cancel") operation.abort();
      if (control === "context abort") f.state.aborted = true;
      f.emit("session_compact_failed", {
        reason: "manual", aborted: control === "operation cancel",
        errorMessage: "Nothing to compact (session too small)",
      });
      if (control === "owner halt") f.main.halt();
      if (control === "reload") f.main.prepareReload();
      if (control === "close") f.main.closeFollowUpDrain();
      if (control === "run start") f.emit("agent_start");
      preflight = false;
      f.state.idle = true;
      vi.advanceTimersByTime(180_000);
      expect(f.sent).toHaveLength(0);
      expect(f.released).not.toHaveBeenCalled();
      expect(vi.getTimerCount()).toBe(0);
      const journal = JSON.parse(fs.readFileSync(path.join(f.root, "followups.json"), "utf8"));
      expect(journal.items.map((item: { id: string; handed?: boolean }) => [item.id, !!item.handed])).toEqual([[original.messageId, false]]);
    },
  );

  it.each([true, false])("retires an expired preflight waiter on cancelled failure before its signal is cleared (idle=%s)", idle => {
    const f = setup(flushMs);
    f.fail();
    const original = f.deliver();
    f.state.idle = false;
    let preflight = true;
    Object.assign(f.ctx, { isPromptPending: () => preflight });
    const operation = new AbortController();
    f.emit("session_before_compact", { reason: "manual", signal: operation.signal });
    vi.advanceTimersByTime(60_000);
    expect(f.sent).toHaveLength(0);
    operation.abort();
    f.state.idle = idle;
    f.emit("session_compact_failed", { reason: "manual", aborted: true, errorMessage: "Compaction cancelled" });
    preflight = false;
    f.state.idle = true;
    vi.advanceTimersByTime(180_000);
    expect(f.sent.every(item => item.options.triggerTurn === false)).toBe(true);
    expect(f.released).not.toHaveBeenCalled();
    const journal = JSON.parse(fs.readFileSync(path.join(f.root, "followups.json"), "utf8"));
    expect(journal.items.map((item: { id: string }) => item.id)).toEqual([original.messageId]);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("yields an expired busy waiter to a real run start in either drain mode", () => {
    const f = setup(flushMs);
    f.fail(); f.deliver();
    f.state.idle = false;
    vi.advanceTimersByTime(60_000);
    expect(f.sent).toHaveLength(0);
    f.emit("agent_start");
    f.state.idle = true;
    vi.advanceTimersByTime(1_000);
    expect(f.sent).toHaveLength(0);
    expect(f.released).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
    f.emit("turn_end", { message: { stopReason: "stop" } });
    f.emit("agent_before_settle", { outcome: "completed" });
    f.emit("agent_settled", { outcome: "completed" });
    expect(f.sent).toHaveLength(1);
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

// A cancelled tree summary emits no completion event, so its aborted signal can outlive it.
// A later compaction that ends before Main's own session_before_compact handler runs (a Pi
// no-op or model check before the hook, or a veto by an extension loaded ahead of Fabric)
// is not that cancellation. Neither is a later operation that a deferred retry waits behind.
describe.each([0, 120_000])("Main provider backoff after a cancelled tree summary (flushMs=%s)", flushMs => {
  type Handoff = { ids: string[]; triggerTurn: boolean; idle: boolean; preflight: boolean };
  const tracked = () => {
    const f = setup(flushMs);
    const handoffs: Handoff[] = [];
    // Record Main's state at each native handoff, then call through to the harness recorder.
    const send = vi.mocked(f.pi.sendMessage);
    const record = send.getMockImplementation()!;
    send.mockImplementation((message: any, options: any) => {
      handoffs.push({
        ids: (message.details.items ?? [message.details]).map((item: { id: string }) => item.id),
        triggerTurn: options.triggerTurn, idle: f.state.idle,
        preflight: (f.ctx as { isPromptPending?: () => boolean }).isPromptPending?.() === true,
      });
      return record(message, options);
    });
    const journal = path.join(f.root, "followups.json");
    // Byte-split batches: one held item per handoff, so FIFO order and the retry latch are visible.
    const big = (deliveryId: string) => f.deliver("followUp", { message: `${deliveryId}: ${"x".repeat(40_000)}`, deliveryId });
    const cancelledTree = () => {
      const tree = new AbortController();
      f.state.idle = false;
      f.emit("session_before_tree", { signal: tree.signal });
      tree.abort(); // Escape: Pi returns cancelled and emits no session_tree.
      f.state.idle = true;
    };
    const triggering = () => handoffs.filter(item => item.triggerTurn).map(item => item.ids);
    const unhanded = (): string[] => fs.existsSync(journal)
      ? JSON.parse(fs.readFileSync(journal, "utf8")).items.filter((item: { handed?: boolean }) => !item.handed).map((item: { id: string }) => item.id)
      : [];
    // Only a completed provider turn opens the rest of the FIFO: each original once, in order.
    const finish = (expected: string[]) => {
      f.emit("agent_start");
      f.emit("turn_end", { message: { stopReason: "stop" } });
      f.emit("agent_before_settle", { outcome: "completed" });
      f.emit("agent_settled", { outcome: "completed" });
      vi.advanceTimersByTime(1_000);
      expect(handoffs.flatMap(item => item.ids), "each original once, FIFO").toEqual(expected);
      for (const item of handoffs.filter(handoff => handoff.triggerTurn)) {
        expect(item, "a triggering handoff only at idle, never during prompt preflight").toMatchObject({ idle: true, preflight: false });
      }
      expect(f.main.queueDepth().pendingFollowUps).toBe(0);
      expect(unhanded()).toEqual([]);
      expect(vi.getTimerCount(), "no leaked timer or waiter").toBe(0);
    };
    return { ...f, handoffs, big, cancelledTree, triggering, unhanded, finish };
  };

  it.each([
    { ending: "nothing to compact", event: { aborted: false, errorMessage: "Compaction failed: Nothing to compact (session too small)" }, backoff: 0 },
    { ending: "already compacted", event: { aborted: false, errorMessage: "Compaction failed: Already compacted" }, backoff: 0 },
    { ending: "earlier extension decline", event: { aborted: true }, backoff: 0 },
    // Not a no-op: a recoverable failure, so the next consecutive backoff applies.
    { ending: "missing model", event: { aborted: false, errorMessage: "Compaction failed: No model selected." }, backoff: 120_000 },
  ])("a later $ending before Main's compaction hook keeps the retry", ({ event, backoff }) => {
    const f = tracked();
    f.fail();
    const original = Date.now() + 60_000;
    const oldest = f.big("oldest"), later = f.big("later");
    vi.advanceTimersByTime(5_000);
    f.cancelledTree();
    vi.advanceTimersByTime(5_000);
    // Main sees no session_before_compact for this operation, only its failure.
    f.emit("session_compact_failed", { reason: "manual", willRetry: false, ...event });
    const deadline = backoff ? Date.now() + backoff : original;
    expect(vi.getTimerCount(), "a retry timer survives a failure that is not a cancel").toBeGreaterThan(0);
    vi.advanceTimersByTime(deadline - Date.now() - 1);
    expect(f.handoffs, "no early release").toEqual([]);
    vi.advanceTimersByTime(1);
    expect(f.triggering(), "held work is not stranded").toEqual([[oldest.messageId]]);
    expect(f.released).toHaveBeenCalledExactlyOnceWith({ until: new Date(deadline).toISOString(), messageIds: [oldest.messageId] });
    const fresh = f.deliver("steer");
    expect(fresh.reason, "a fresh peer waits behind the retry outcome").toBe("provider-retry in flight");
    f.finish([oldest.messageId, later.messageId, fresh.messageId]);
  });

  it("an expiry captured while an earlier extension's compaction hook keeps Main busy retries at the later idle", () => {
    const f = tracked();
    f.fail();
    const original = Date.now() + 60_000;
    const oldest = f.big("oldest"), later = f.big("later");
    f.cancelledTree();
    f.state.idle = false; // compaction: an extension loaded ahead of Fabric is still in its hook
    vi.advanceTimersByTime(61_000);
    expect(f.handoffs, "nothing handed to busy Pi").toEqual([]);
    f.state.idle = true;
    f.emit("session_compact_failed", { reason: "manual", aborted: true, willRetry: false }); // its veto
    vi.advanceTimersByTime(1_000);
    expect(f.triggering(), "held work is not stranded").toEqual([[oldest.messageId]]);
    expect(f.released).toHaveBeenCalledExactlyOnceWith({ until: new Date(original).toISOString(), messageIds: [oldest.messageId] });
    f.finish([oldest.messageId, later.messageId]);
  });

  it("a declined tree summary that starts while a preflight-deferred retry waits keeps that waiter", () => {
    const f = tracked();
    f.fail();
    const oldest = f.big("oldest"), later = f.big("later");
    let preflight = true;
    Object.assign(f.ctx, { isPromptPending: () => preflight });
    vi.advanceTimersByTime(61_000); // expiry during prompt preflight: the retry waits for it
    expect(f.handoffs).toEqual([]);
    f.state.idle = false;
    f.emit("session_before_tree", { signal: new AbortController().signal });
    f.state.idle = true; // an extension declined the summary: no session_tree, signal not aborted
    preflight = false;
    vi.advanceTimersByTime(1_000);
    expect(f.triggering(), "the deferred retry still runs at idle").toEqual([[oldest.messageId]]);
    f.finish([oldest.messageId, later.messageId]);
  });

  it("a tree summary cancelled after the busy expiry captured it still vetoes the retry without losing work", () => {
    const f = tracked();
    f.fail();
    const oldest = f.big("oldest"), later = f.big("later");
    const tree = new AbortController();
    f.state.idle = false;
    f.emit("session_before_tree", { signal: tree.signal });
    vi.advanceTimersByTime(61_000); // expiry during the summary: the waiter captures its signal
    expect(f.handoffs).toEqual([]);
    tree.abort();
    f.state.idle = true;
    vi.advanceTimersByTime(600_000);
    if (flushMs > 0) {
      expect(f.triggering(), "a real cancel of the captured operation: no hidden run").toEqual([]);
      expect(f.released).not.toHaveBeenCalled();
      expect(f.unhanded()).toEqual([oldest.messageId, later.messageId]);
      const newest = f.big("newest"); // a later peer delivery is a release edge
      vi.advanceTimersByTime(100);
      expect(f.triggering()).toEqual([[oldest.messageId]]);
      f.finish([oldest.messageId, later.messageId, newest.messageId]);
    } else {
      // Drain off registers no tree hook, so nothing was captured: the deferred retry runs at idle.
      expect(f.triggering()).toEqual([[oldest.messageId]]);
      f.finish([oldest.messageId, later.messageId]);
    }
  });

  it("a compaction Main saw and that was really cancelled after a cancelled tree still vetoes the retry", () => {
    const f = tracked();
    f.fail();
    const oldest = f.big("oldest"), later = f.big("later");
    f.cancelledTree();
    f.state.idle = false;
    const operation = new AbortController();
    f.emit("session_before_compact", { reason: "manual", signal: operation.signal });
    vi.advanceTimersByTime(61_000);
    operation.abort();
    f.state.idle = true;
    f.emit("session_compact_failed", { reason: "manual", aborted: true, willRetry: false });
    vi.advanceTimersByTime(600_000);
    expect(f.triggering(), "cancel veto: no hidden run").toEqual([]);
    expect(f.released).not.toHaveBeenCalled();
    expect(f.unhanded()).toEqual([oldest.messageId, later.messageId]);
    const newest = f.big("newest"); // a later peer delivery is a release edge
    vi.advanceTimersByTime(100);
    expect(f.triggering()).toEqual([[oldest.messageId]]);
    f.finish([oldest.messageId, later.messageId, newest.messageId]);
  });
});
