import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { installSelfReload, SELF_RELOAD_COMMAND, attemptedSelfReload, type SelfReloadDeps } from "../src/lifecycle/self-reload.js";

let root: string, old: string, next: string, settings: string, slots: string;
let serial = 0;
const shutdowns: Array<() => void> = [];
const inFlight: Array<Promise<void>> = [];
const finishes: Array<() => void> = [];
const activate = (target: string) => {
  fs.writeFileSync(settings, JSON.stringify({ packages: [target] }));
  const date = new Date(Date.now() + ++serial * 1000); fs.utimesSync(settings, date, date);
};
beforeEach(() => {
  vi.useFakeTimers();
  for (const name of ["PI_FABRIC_PARENT_RUN", "PI_FABRIC_ACTOR_ID", "PI_FABRIC_NO_AUTO_RELOAD"]) vi.stubEnv(name, undefined);
  root = fs.mkdtempSync(path.join(os.tmpdir(), "self-reload-admission-"));
  old = path.join(root, "old"); next = path.join(root, "next");
  for (const dir of [old, next]) { fs.mkdirSync(dir); fs.writeFileSync(path.join(dir, "package.json"), JSON.stringify({ name: "pi-fabric" })); }
  settings = path.join(root, "settings.json"); slots = path.join(root, "slots"); activate(old);
});
afterEach(async () => {
  shutdowns.splice(0).forEach(stop => stop());
  finishes.splice(0).forEach(finish => finish());
  await Promise.allSettled(inFlight.splice(0));
  vi.restoreAllMocks(); vi.useRealTimers(); vi.unstubAllEnvs();
  fs.rmSync(root, { recursive: true, force: true });
});
const main = (deps: Partial<SelfReloadDeps> = {}, productionJitter = false) => {
  const id = `admission-${++serial}`;
  const state = { busy: 0, idle: true, pending: false, prompt: false, halted: false };
  const handlers = new Map<string, (event: any, context: any) => unknown>();
  const commands = new Map<string, { handler: (args: string, context: any) => Promise<void> }>();
  const context = {
    mode: "tui", hasUI: false, isIdle: () => state.idle, hasPendingMessages: () => state.pending,
    isPromptPending: () => state.prompt, isSettling: () => false,
    sessionManager: { getSessionId: () => id }, reload: vi.fn(async () => {}),
  };
  const sent: string[] = [];
  const pi = { on: (name: string, handler: any) => handlers.set(name, handler),
    registerCommand: (name: string, command: any) => commands.set(name, command),
    sendUserMessage: (text: string) => { sent.push(text); } };
  const controller = installSelfReload(pi as never, {
    busy: () => state.busy, halted: () => state.halted, autoReloadConfigured: () => true,
    moduleUrl: pathToFileURL(path.join(old, "index.js")).href, settingsPath: settings,
    reloadSlotsDirectory: slots, ...(productionJitter ? {} : { reloadJitterMs: () => 0 }), ...deps,
  });
  controller.sessionStart("startup", context as never);
  const emit = (event: string) => handlers.get(event)?.({}, context);
  shutdowns.push(() => emit("session_shutdown"));
  const execute = (args = "auto") => commands.get(SELF_RELOAD_COMMAND)!.handler(args, context);
  const queue = () => { const job = execute(); inFlight.push(job); return job; };
  return { id, pi, state, context, sent, emit, execute, queue, controller };
};
const hold = (context: ReturnType<typeof main>["context"]) => {
  let finish!: () => void;
  const done = new Promise<void>(resolve => { finish = resolve; });
  finishes.push(finish); context.reload.mockImplementation(() => done);
  return finish;
};
const flush = async () => { await vi.advanceTimersByTimeAsync(0); };

describe("automatic per-host reload admission", () => {
  it("admits at most two of five idle Mains and never drops the remaining targets", async () => {
    const fleet = Array.from({ length: 5 }, () => main({ selfReloadConcurrency: () => 2 }));
    let active = 0, peak = 0, completed = 0;
    const release = fleet.map(agent => {
      const finish = hold(agent.context);
      const impl = agent.context.reload.getMockImplementation()!;
      agent.context.reload.mockImplementation(async () => {
        active++; peak = Math.max(peak, active);
        await impl(); active--; completed++;
      });
      return finish;
    });
    activate(next);
    fleet.forEach(agent => { agent.emit("agent_settled"); agent.queue(); });
    await vi.waitFor(() => expect(active).toBe(2)); // first-use dynamic import needs an IO turn
    expect(peak).toBe(2);
    const waiting = fleet.filter(agent => agent.context.reload.mock.calls.length === 0);
    expect(waiting).toHaveLength(3);
    for (const agent of waiting) expect(attemptedSelfReload(agent.id, next)).toBe(false);
    const winners = fleet.filter(agent => agent.context.reload.mock.calls.length === 1);
    winners.forEach(agent => release[fleet.indexOf(agent)]!()); await flush();
    await vi.advanceTimersByTimeAsync(5_000);
    for (const agent of waiting) { expect(agent.sent).toHaveLength(2); agent.queue(); }
    await flush(); expect(active).toBe(2);
    fleet.filter(agent => agent.context.reload.mock.calls.length === 1).forEach(agent => release[fleet.indexOf(agent)]!());
    await flush(); await vi.advanceTimersByTimeAsync(5_000);
    const last = fleet.find(agent => !agent.context.reload.mock.calls.length)!;
    last.queue(); await flush(); expect(active).toBe(1);
    release[fleet.indexOf(last)]!(); await flush();
    expect(completed).toBe(5); expect(peak).toBe(2);
    fleet.forEach(agent => expect(agent.context.reload).toHaveBeenCalledTimes(1));
    expect(fs.readdirSync(slots)).toEqual([]);
  });

  it("re-arms idle retry when the delivered automatic command finds the host slots full", async () => {
    const first = main({ selfReloadConcurrency: () => 1 });
    const second = main({ selfReloadConcurrency: () => 1, reloadJitterMs: () => 5_000 });
    const finish = hold(first.context); activate(next);
    first.emit("agent_settled"); first.queue();
    await vi.waitFor(() => expect(first.context.reload).toHaveBeenCalledOnce());
    // Actual Pi can enter a delivered command synchronously inside the timer's request.
    // That request then stops the timer, including the command's pre-admission re-arm.
    let delivered!: Promise<void>;
    second.pi.sendUserMessage = (text: string) => { second.sent.push(text); delivered = second.queue(); };
    second.emit("agent_settled"); expect(second.sent).toEqual([]);
    await vi.advanceTimersByTimeAsync(5_000); await delivered;
    expect(second.sent).toHaveLength(1);
    expect(second.context.reload).not.toHaveBeenCalled();
    finish(); await flush();
    await vi.advanceTimersByTimeAsync(5_000); await flush();
    expect(second.sent).toHaveLength(2);
    expect(second.context.reload).toHaveBeenCalledOnce();
  });

  it("defaults to six slots without an explicit config dependency", async () => {
    const fleet = Array.from({ length: 7 }, () => main());
    fleet.forEach(agent => hold(agent.context)); activate(next);
    fleet.forEach(agent => { agent.emit("agent_settled"); agent.queue(); }); await flush();
    expect(fleet.filter(agent => agent.context.reload.mock.calls.length)).toHaveLength(6);
  });

  it("transfers the old module's lease to activation, not session_shutdown", async () => {
    const first = main({ selfReloadConcurrency: () => 1 });
    const second = main({ selfReloadConcurrency: () => 1 });
    hold(first.context); activate(next);
    first.emit("agent_settled"); first.queue(); await flush();
    first.emit("session_shutdown");
    second.emit("agent_settled"); await second.execute();
    expect(second.context.reload).not.toHaveBeenCalled();
    const fresh = installSelfReload(first.pi as never, { busy: () => 0, autoReloadConfigured: () => true,
      moduleUrl: pathToFileURL(path.join(next, "index.js")).href, settingsPath: settings });
    const receipt = fresh.sessionStart("reload", first.context as never)!;
    expect(receipt).toMatchObject({ old: "old", new: "next", releaseSlot: expect.any(Function) });
    expect(fs.readdirSync(slots)).toEqual(["slot-0"]);
    receipt.releaseSlot!();
    await vi.advanceTimersByTimeAsync(5_000); await second.execute();
    expect(second.context.reload).toHaveBeenCalledOnce();
  });

  it("releases on native failure and leaves capacity for another Main", async () => {
    const first = main({ selfReloadConcurrency: () => 1 });
    const second = main({ selfReloadConcurrency: () => 1 }); activate(next);
    first.context.reload.mockRejectedValue(new Error("native failure")); first.emit("agent_settled");
    await expect(first.execute()).rejects.toThrow("native failure");
    second.emit("agent_settled"); await second.execute(); expect(second.context.reload).toHaveBeenCalledOnce();
    expect(fs.readdirSync(slots)).toEqual([]);
  });

  it("manual reload bypasses the full limiter and first-attempt jitter", async () => {
    const auto = main({ selfReloadConcurrency: () => 1 }); hold(auto.context);
    const manual = main({ selfReloadConcurrency: () => 1, reloadJitterMs: () => 30_000 }); activate(next);
    auto.emit("agent_settled"); auto.queue(); await flush();
    manual.emit("agent_settled"); expect(manual.sent).toEqual([]);
    await manual.execute(""); expect(manual.context.reload).toHaveBeenCalledOnce();
    // Native /reload is not intercepted or registered by this controller at all.
    await manual.context.reload(); expect(manual.context.reload).toHaveBeenCalledTimes(2);
  });

  it("0 retains unlimited old behavior, without jitter or any slot filesystem access", async () => {
    fs.writeFileSync(slots, "not a directory");
    const jitter = vi.fn(() => 30_000);
    const fleet = Array.from({ length: 5 }, () => main({ selfReloadConcurrency: () => 0, reloadJitterMs: jitter }));
    fleet.forEach(agent => hold(agent.context)); activate(next);
    fleet.forEach(agent => { agent.emit("agent_settled"); agent.queue(); }); await flush();
    expect(fleet.filter(agent => agent.context.reload.mock.calls.length)).toHaveLength(5);
    expect(jitter).not.toHaveBeenCalled(); expect(fs.readFileSync(slots, "utf8")).toBe("not a directory");
  });

  it("jitter is sampled once per target, spreads first attempts, and still uses idle retry", async () => {
    const jitter = vi.fn(() => 15_000);
    const agent = main({ reloadJitterMs: jitter }); activate(next);
    agent.emit("agent_settled"); expect(agent.sent).toEqual([]);
    await vi.advanceTimersByTimeAsync(10_000); expect(agent.sent).toEqual([]);
    agent.emit("agent_settled"); expect(jitter).toHaveBeenCalledOnce();
    await vi.advanceTimersByTimeAsync(5_000); expect(agent.sent).toHaveLength(1);
    await agent.execute(); expect(agent.context.reload).toHaveBeenCalledOnce();
  });

  it("production jitter spans 0–30 seconds and new targets get a new deadline", async () => {
    const random = vi.spyOn(Math, "random").mockReturnValue(0.999999);
    const agent = main({}, true); activate(next); agent.emit("agent_settled");
    await vi.advanceTimersByTimeAsync(25_000); expect(agent.sent).toEqual([]);
    const third = path.join(root, "third"); fs.mkdirSync(third);
    fs.writeFileSync(path.join(third, "package.json"), JSON.stringify({ name: "pi-fabric" }));
    activate(third); agent.emit("agent_settled");
    await vi.advanceTimersByTimeAsync(25_000); expect(agent.sent).toEqual([]);
    await vi.advanceTimersByTimeAsync(5_000); expect(agent.sent).toHaveLength(1);
    expect(random).toHaveBeenCalledTimes(2);
  });

  it("inaccessible slot state fails closed and stays pending until a later retry", async () => {
    fs.writeFileSync(slots, "blocked");
    const agent = main(); activate(next); agent.emit("agent_settled"); await agent.execute();
    expect(agent.context.reload).not.toHaveBeenCalled(); expect(attemptedSelfReload(agent.id, next)).toBe(false);
    fs.unlinkSync(slots); await vi.advanceTimersByTimeAsync(5_000); await agent.execute();
    expect(agent.context.reload).toHaveBeenCalledOnce();
  });

  it.each(["busy", "pending", "prompt", "halted", "switch"])("rechecks %s after first-use import without consuming a slot or target", async hold => {
    const agent = main(); activate(next); agent.emit("agent_settled");
    const attempt = agent.execute();
    if (hold === "busy") agent.state.busy = 1;
    else if (hold === "switch") agent.controller.sessionStart("switch", agent.context as never);
    else agent.state[hold as "pending" | "prompt" | "halted"] = true;
    await attempt;
    expect(agent.context.reload).not.toHaveBeenCalled(); expect(attemptedSelfReload(agent.id, next)).toBe(false);
    expect(fs.existsSync(slots)).toBe(false);
  });
});
