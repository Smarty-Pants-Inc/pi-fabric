import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AgentCompletionInbox, AGENT_COMPLETION_MESSAGE_TYPE } from "../src/agents/completion-inbox.js";
import type { AgentRunResult } from "../src/agents/types.js";
import { FabricShellJobStore } from "../src/core/shell-jobs.js";
import { ShellEventInbox, SHELL_MESSAGE_TYPE } from "../src/core/shell-inbox.js";
import { MainAgentController } from "../src/main-agent.js";

// smarty-dev#8881: AgentSession.dispose() and session replacement invalidate the extension ctx without
// emitting session_shutdown, and every getter of the old ctx then throws Pi's stale-ctx error. The
// completion and shell inboxes' idle-check timers and Main's 25 ms wake/preflight intervals (and its
// provider timers) read the captured ctx from the callback with no fence: the first tick after a
// dispose threw out of a timer, an uncaught exception that exits Pi.
const STALE = "This extension ctx is stale after session replacement or reload.";

type Handler = (event: any, ctx: ExtensionContext) => unknown;
type State = { idle: boolean; aborted: boolean; prompt: boolean; stale: boolean };
/** Mirrors ExtensionRunner.invalidate(): after dispose every read of the ctx throws. */
const staleable = (overrides: Partial<State> = {}, extra: Record<string, unknown> = {}) => {
  const state: State = { idle: false, aborted: false, prompt: false, stale: false, ...overrides };
  const notify = vi.fn();
  const target = {
    get hasUI() { return true; },
    ui: { notify },
    get signal() { return { aborted: state.aborted }; },
    isIdle: () => state.idle,
    hasPendingMessages: () => false,
    isPromptPending: () => state.prompt,
    ...extra,
  };
  const ctx = new Proxy(target, {
    get(object, key, receiver) {
      if (state.stale) throw new Error(STALE);
      return Reflect.get(object, key, receiver);
    },
  }) as unknown as ExtensionContext;
  return { ctx, state, notify, dispose: () => { state.stale = true; } };
};
const fakePi = () => {
  const handlers = new Map<string, Handler[]>();
  const sendMessage = vi.fn();
  const pi = {
    on: (name: string, fn: Handler) => {
      handlers.set(name, [...(handlers.get(name) ?? []), fn]);
      return () => handlers.set(name, (handlers.get(name) ?? []).filter(h => h !== fn));
    },
    sendMessage, sendUserMessage: vi.fn(), getThinkingLevel: () => "off",
  } as unknown as ExtensionAPI;
  const emit = (name: string, event: unknown, ctx: ExtensionContext) => { for (const fn of handlers.get(name) ?? []) fn(event, ctx); };
  return { pi, sendMessage, emit };
};
const result = (id: string): AgentRunResult => ({
  id, name: `worker ${id}`, status: "completed", text: `result ${id}`, startedAt: 1, finishedAt: 2,
  task: "work", runner: "pi", transport: "process", cwd: ".", updatedAt: 2, turns: 1, toolCalls: 0,
  usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
});
const sentTypes = (sendMessage: ReturnType<typeof vi.fn>) => sendMessage.mock.calls.map(call => (call[0] as { customType: string }).customType);

const closers: Array<() => unknown> = [];
const roots: string[] = [];
beforeEach(() => { vi.useFakeTimers(); vi.setSystemTime(new Date("2026-10-13T08:00:00Z")); });
afterEach(async () => {
  vi.clearAllTimers();
  vi.useRealTimers();
  for (const close of closers.splice(0)) await close();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("AgentCompletionInbox retires on a disposed session (smarty-dev#8881)", () => {
  const setup = (state: Partial<State> = {}) => {
    const fake = fakePi();
    const host = staleable(state);
    const inbox = new AgentCompletionInbox(fake.pi, host.ctx);
    closers.push(() => inbox.close());
    return { ...fake, ...host, inbox };
  };

  it("an idle check armed before dispose never reads the stale ctx from its timer", async () => {
    const h = setup();                                 // busy: the check finds nothing to do yet
    const delivered = vi.fn();
    h.inbox.enqueue(result("a"), delivered);
    expect(vi.getTimerCount()).toBe(1);
    h.dispose();                                       // no session_shutdown: Pi's dispose emits none
    await expect(vi.advanceTimersByTimeAsync(5_000)).resolves.toBe(vi);
    expect(h.sendMessage).not.toHaveBeenCalled();
    expect(delivered).not.toHaveBeenCalled();         // the outcome stays unread in its journal
    expect(vi.getTimerCount()).toBe(0);
    h.inbox.idleSignal();                              // a later idle edge arms nothing on a retired inbox
    expect(vi.getTimerCount()).toBe(0);
  });

  it("a completion that lands after dispose neither throws into its producer nor arms a timer", async () => {
    const h = setup({ idle: true });
    h.dispose();
    const delivered = vi.fn();
    expect(() => h.inbox.enqueue(result("late"), delivered)).not.toThrow();
    await expect(vi.advanceTimersByTimeAsync(5_000)).resolves.toBe(vi);
    expect(h.sendMessage).not.toHaveBeenCalled();
    expect(delivered).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
  });

  it("a live successor ctx bound by a later event still delivers (counterexample)", async () => {
    const h = setup();
    h.inbox.enqueue(result("a"));
    h.dispose();
    const successor = staleable({ idle: true });
    h.emit("input", {}, successor.ctx);                // the inbox now holds a live ctx
    await vi.advanceTimersByTimeAsync(40);
    expect(sentTypes(h.sendMessage)).toEqual([AGENT_COMPLETION_MESSAGE_TYPE]);
  });

  it("a live idle session delivers, and shutdown close still cancels a pending check", async () => {
    const live = setup({ idle: true });
    live.inbox.enqueue(result("a"));
    await vi.advanceTimersByTimeAsync(40);
    expect(sentTypes(live.sendMessage)).toEqual([AGENT_COMPLETION_MESSAGE_TYPE]);
    const closing = setup({ idle: true });
    closing.inbox.enqueue(result("b"));
    closing.inbox.close();
    await vi.advanceTimersByTimeAsync(1_000);
    expect(closing.sendMessage).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
  });
});

describe("ShellEventInbox retires on a disposed session (smarty-dev#8881)", () => {
  const setup = (state: Partial<State> = {}) => {
    const fake = fakePi();
    const host = staleable(state);
    const jobs = new FabricShellJobStore();
    const inbox = new ShellEventInbox(fake.pi, host.ctx, jobs);
    closers.push(async () => { inbox.close(); await jobs.close(); });
    const finished = async () => {
      const job = jobs.begin("bash", "build");
      job.spill();
      await job.finish(0);
      return job;
    };
    return { ...fake, ...host, jobs, inbox, finished };
  };

  it("a delivery check armed before dispose never reads the stale ctx, and does not re-poll", async () => {
    const h = setup();
    await h.finished();
    expect(h.inbox.pendingCount()).toBe(1);
    h.dispose();
    await expect(vi.advanceTimersByTimeAsync(5_000)).resolves.toBe(vi);
    expect(h.sendMessage).not.toHaveBeenCalled();
    expect(h.inbox.pendingCount()).toBe(0);            // retired like a shutdown close
    await h.finished();                                // a later job event arms nothing
    await expect(vi.advanceTimersByTimeAsync(5_000)).resolves.toBe(vi);
    expect(h.sendMessage).not.toHaveBeenCalled();
  });

  it("a live successor ctx bound by a later event still delivers (counterexample)", async () => {
    const h = setup();
    await h.finished();
    h.dispose();
    const successor = staleable({ idle: true });
    h.emit("input", {}, successor.ctx);
    await vi.advanceTimersByTimeAsync(40);
    expect(sentTypes(h.sendMessage)).toEqual([SHELL_MESSAGE_TYPE]);
  });

  it("a live idle session delivers, and shutdown close still cancels a pending check", async () => {
    const live = setup({ idle: true });
    await live.finished();
    await vi.advanceTimersByTimeAsync(40);
    expect(sentTypes(live.sendMessage)).toEqual([SHELL_MESSAGE_TYPE]);
    const closing = setup({ idle: true });
    await closing.finished();
    closing.inbox.close();
    await vi.advanceTimersByTimeAsync(1_000);
    expect(closing.sendMessage).not.toHaveBeenCalled();
  });
});

describe("Main followUp drain retires its wake loops on a disposed session (smarty-dev#8881)", () => {
  const from = { id: "agent-a", name: "a", kind: "agent" as const };
  const setup = (state: Partial<State> = {}) => {
    const fake = fakePi();
    const host = staleable(state);
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "main-dispose-")); roots.push(root);
    const journal = path.join(root, "followups.json");
    const main = new MainAgentController(fake.pi, "session:root", true, root, "root");
    main.attachFollowUpDrain(host.ctx, 120_000, journal);
    closers.push(() => main.closeFollowUpDrain());
    const deliver = (message = "resume work") => main.deliverAgent({ from, message, delivery: "followUp" });
    return { ...fake, ...host, main, journal, deliver };
  };
  const compactManually = (h: ReturnType<typeof setup>) => {
    h.emit("session_before_compact", { reason: "manual", signal: new AbortController().signal }, h.ctx);
    h.emit("session_compact", { reason: "manual" }, h.ctx);
  };

  it("the post-compaction idle wake stops at its first tick after dispose and keeps the journal", async () => {
    const h = setup();
    const receipt = h.deliver();
    compactManually(h);                                // still busy: the 25 ms wake polls for idle
    await vi.advanceTimersByTimeAsync(100);
    expect(vi.getTimerCount()).toBe(1);
    h.dispose();
    await expect(vi.advanceTimersByTimeAsync(1_000)).resolves.toBe(vi);
    expect(h.sendMessage).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
    expect(fs.readFileSync(h.journal, "utf8")).toContain(receipt.messageId);
    expect(h.main.bindingContext()).toBeUndefined();   // suspended like a session switch
  });

  it("the prompt-preflight wake stops at its first tick after dispose", async () => {
    const h = setup({ idle: true, prompt: true });
    const receipt = h.deliver();
    expect(h.sendMessage).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(1);
    h.dispose();
    await expect(vi.advanceTimersByTimeAsync(1_000)).resolves.toBe(vi);
    expect(h.sendMessage).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
    expect(fs.readFileSync(h.journal, "utf8")).toContain(receipt.messageId);
  });

  it("the provider-backoff wake never reads the disposed ctx", async () => {
    const h = setup({ idle: true });
    h.emit("agent_start", {}, h.ctx);
    h.emit("turn_end", { message: { stopReason: "error", errorMessage: "503 upstream connect error" } }, h.ctx);
    h.emit("agent_before_settle", { outcome: "error" }, h.ctx);
    h.emit("agent_settled", { outcome: "error" }, h.ctx);
    const receipt = h.deliver();
    expect(receipt).toMatchObject({ triggered: false });
    expect(vi.getTimerCount()).toBe(1);
    h.dispose();
    await expect(vi.advanceTimersByTimeAsync(120_000)).resolves.toBe(vi);
    expect(h.sendMessage).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
    expect(fs.readFileSync(h.journal, "utf8")).toContain(receipt.messageId);
  });

  it("a live successor ctx bound by a later event still wakes Main (counterexample)", async () => {
    const h = setup();
    h.deliver();
    compactManually(h);
    h.dispose();
    const successor = staleable({ idle: true });
    h.emit("input", { source: "extension" }, successor.ctx);
    await vi.advanceTimersByTimeAsync(25);
    expect(h.sendMessage).toHaveBeenCalledTimes(1);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("live sessions still wake after preflight and compaction, and close still cancels the loops", async () => {
    const preflight = setup({ idle: true, prompt: true });
    preflight.deliver();
    preflight.state.prompt = false;
    await vi.advanceTimersByTimeAsync(25);
    expect(preflight.sendMessage).toHaveBeenCalledTimes(1);
    const compaction = setup();
    compaction.deliver();
    compactManually(compaction);
    compaction.state.idle = true;
    await vi.advanceTimersByTimeAsync(25);
    expect(compaction.sendMessage).toHaveBeenCalledTimes(1);
    const closing = setup({ idle: true, prompt: true });
    closing.deliver();
    closing.main.closeFollowUpDrain();
    await vi.advanceTimersByTimeAsync(1_000);
    expect(closing.sendMessage).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
  });
});
