import { execFile } from "node:child_process";
import { EventEmitter } from "node:events";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ActorChildCompletionStore, ActorChildCompletionWatch } from "../src/actors/child-completions.js";
import { ActorManager } from "../src/actors/manager.js";
import { ActorMeshMonitor } from "../src/actors/mesh-monitor.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import type { AgentRunResult } from "../src/agents/types.js";

const claimLocks = (mkdir: { mock: { calls: unknown[][] } }) =>
  mkdir.mock.calls.filter(([file]) => String(file).endsWith(`${path.sep}.claim.lock`)).length;

it("does not claim an empty spool and re-reads a completion on a subsequent call", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-child-idle-preflight-"));
  const sessionFile = path.join(root, "session.jsonl");
  fs.writeFileSync(sessionFile, "");
  const store = new ActorChildCompletionStore(sessionFile);
  const mkdir = vi.spyOn(fs, "mkdirSync");
  try {
    expect(store.pending()).toEqual([]);
    expect(claimLocks(mkdir)).toBe(0);
    const result = { id: "b".repeat(32), name: "child", status: "completed", text: "saved result",
      startedAt: Date.now(), finishedAt: Date.now() } as AgentRunResult;
    store.enqueue(result, { id: "actor:owner", kind: "actor", runId: "a".repeat(32) });
    mkdir.mockClear();
    expect(store.pending()).toEqual([expect.objectContaining({ result: expect.objectContaining({ id: result.id }) })]);
    expect(claimLocks(mkdir)).toBeGreaterThan(0);
  } finally {
    mkdir.mockRestore();
    fs.rmSync(root, { recursive: true, force: true });
  }
});

// Integration wake proof: a real owner finishes an EMPTY preflight, then a writer in
// another process publishes an envelope. Only the spool watch can wake the owner here:
// the mesh safety net and retention cadence are set far beyond the test deadline.
it("wakes a real idle owner and claims a cross-process completion written after the empty preflight", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-child-idle-wake-"));
  const rootId = "session:root-main";
  const meshRoot = path.join(root, "mesh");
  const mesh = new MeshStore(meshRoot, 64 * 1024, 100);
  const meshConfig = { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 600_000 };
  const agents = new AgentManager(root, DEFAULT_FABRIC_CONFIG.agents, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"), mainAgentId: rootId,
  });
  let release!: () => void;
  const held = new Promise<void>(resolve => { release = resolve; });
  // No model: the activation is held open, so no completion/prune/delete path runs.
  const run = vi.spyOn(agents, "run").mockImplementation(async (request, _signal, onStart) => {
    onStart?.({ id: "c".repeat(32), name: "wake", status: "running", runner: "pi", transport: "process", cwd: root });
    await held;
    return { id: "c".repeat(32), name: "wake", task: request.task, status: "stopped", runner: "pi", transport: "process", cwd: root,
      startedAt: Date.now(), updatedAt: Date.now(), finishedAt: Date.now(), turns: 0, toolCalls: 0, text: "",
      usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 } } as AgentRunResult;
  });
  const pending = vi.spyOn(ActorChildCompletionStore.prototype, "pending");
  const schedule = vi.spyOn(ActorMeshMonitor.prototype, "schedule");
  const owner = new ActorManager("root-main", { id: rootId, name: "owner", kind: "main", sessionId: "root-main" },
    mesh, meshConfig, agents, vi.fn(), {
      persistent: true, actorRoot: path.join(meshRoot, "actors", "root-main"), claimResidency: "session", rootId,
      resolvePiModel: async (model: string) => model, retentionSweepMs: 600_000,
    });
  const mkdir = vi.spyOn(fs, "mkdirSync");
  try {
    const actor = await owner.create({ name: "wake", instructions: "Read child outcomes.", residency: "session",
      delivery: "mailbox", transport: "process", model: "fixture/wake" });
    const sessionFile = actor.sessionFile;
    if (!sessionFile) throw new Error("persistent actor has no session file");
    const spool = path.join(path.dirname(sessionFile), "child-completions");
    // The empty preflight has run (watch armed first, so the spool exists and is empty).
    await vi.waitFor(() => {
      expect(pending.mock.results.some(({ type, value }) => type === "return" && Array.isArray(value) && value.length === 0)).toBe(true);
      expect(fs.readdirSync(spool)).toEqual([]);
    }, { timeout: 5000 });
    await new Promise<void>(resolve => setImmediate(resolve));
    expect(run).not.toHaveBeenCalled();
    schedule.mockClear();
    mkdir.mockClear();

    const id = "d".repeat(32);
    const result = { id, name: "child", status: "completed", text: "late child result", startedAt: Date.now(), finishedAt: Date.now() };
    const script = `
      import { ActorChildCompletionStore } from ${JSON.stringify(path.resolve("src/actors/child-completions.ts"))};
      new ActorChildCompletionStore(${JSON.stringify(sessionFile)})
        .enqueue(${JSON.stringify(result)}, { id: ${JSON.stringify(actor.id)}, kind: "actor", runId: ${JSON.stringify("e".repeat(32))} });
    `;
    await new Promise<void>((resolve, reject) => execFile("bun", ["--eval", script], { timeout: 10_000, maxBuffer: 4096 },
      error => error ? reject(error) : resolve()));
    const written = Date.now();
    expect(fs.existsSync(path.join(spool, `${id}.json`)) || fs.existsSync(path.join(spool, `${id}.receipt`))).toBe(true);

    // Claimed through the existing lock and handed to the mailbox, far below any timer cadence.
    await vi.waitFor(() => {
      expect(JSON.parse(fs.readFileSync(path.join(spool, `${id}.receipt`), "utf8"))).toMatchObject({ id, kind: "mailbox" });
      expect(fs.existsSync(path.join(spool, `${id}.json`))).toBe(false);
    }, { timeout: 4000, interval: 10 });
    expect(Date.now() - written).toBeLessThan(4000);
    expect(schedule).toHaveBeenCalled();
    expect(claimLocks(mkdir)).toBeGreaterThan(0);
    // The full outcome is retained for the activation (no prune/delete path ran).
    expect(fs.existsSync(path.join(spool, `${id}.result.json`))).toBe(true);
    await vi.waitFor(() => expect(run).toHaveBeenCalledTimes(1), { timeout: 4000 });
    expect(run.mock.calls[0]![0].task).toContain(id);

    // Further wakes (including the owner's own receipt/envelope writes) never queue it twice.
    await new Promise(resolve => setTimeout(resolve, 100));
    expect(run).toHaveBeenCalledTimes(1);
    const queued = owner.messages(actor.id).filter(message => message.source === "child-completion" && message.direction === "in");
    expect(queued.length).toBeLessThanOrEqual(1);

    // No hot loop: claim-lock, atomic temp, result and receipt traffic is not a wake.
    schedule.mockClear();
    fs.mkdirSync(path.join(spool, ".claim.lock"));
    fs.rmdirSync(path.join(spool, ".claim.lock"));
    fs.writeFileSync(path.join(spool, `.${"f".repeat(32)}.json.tmp`), "{}");
    fs.renameSync(path.join(spool, `.${"f".repeat(32)}.json.tmp`), path.join(spool, `${"f".repeat(32)}.receipt`));
    fs.writeFileSync(path.join(spool, `${"f".repeat(32)}.result.json`), "{}");
    await new Promise(resolve => setTimeout(resolve, 200));
    expect(schedule).not.toHaveBeenCalled();
  } finally {
    mkdir.mockRestore();
    release();
    await owner.close();
    await agents.close();
    vi.restoreAllMocks();
    await fs.promises.rm(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
  }
}, 30_000);

// Child spool watch failure contract: one rescan + one one-shot re-arm per error, bounded
// backoff with no attempt cap, healthy events reset it, no interval, no hot loop, and close
// joins the native close or a bounded grace when an errored handle never reports it.
type FakeWatcher = EventEmitter & { close: ReturnType<typeof vi.fn>; listener: fs.WatchListener<string> };
const fakeWatchers = (fail: () => boolean = () => false) => {
  const made: FakeWatcher[] = [];
  const watch = vi.spyOn(fs, "watch").mockImplementation(((_dir: fs.PathLike, _options: unknown, listener: fs.WatchListener<string>) => {
    if (fail()) throw Object.assign(new Error("ENOSPC"), { code: "ENOSPC", syscall: "watch" });
    const watcher = Object.assign(new EventEmitter(), { close: vi.fn(), listener }) as FakeWatcher;
    made.push(watcher);
    return watcher as unknown as fs.FSWatcher;
  }) as unknown as typeof fs.watch);
  return { made, watch };
};

it("child watch: an error rescans once and re-arms on one bounded one-shot backoff, reset by healthy events, with no interval", async () => {
  vi.useFakeTimers();
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-child-watch-"));
  const interval = vi.spyOn(globalThis, "setInterval");
  const { made, watch } = fakeWatchers();
  const wake = vi.fn();
  const spool = new ActorChildCompletionWatch(path.join(root, "child-completions"), wake);
  try {
    expect(watch).toHaveBeenCalledOnce();
    expect(vi.getTimerCount()).toBe(0); // healthy idle watch owns no timer
    await vi.advanceTimersByTimeAsync(600_000);
    expect(wake).not.toHaveBeenCalled();
    // Ignored traffic never wakes: claim lock, temp, result, receipts.
    for (const name of [".claim.lock", `.${"f".repeat(32)}.json.tmp`, `${"f".repeat(32)}.result.json`, `${"f".repeat(32)}.receipt`, `${"f".repeat(32)}.live-receipt`]) made[0]!.listener("rename", name);
    expect(wake).not.toHaveBeenCalled();
    made[0]!.listener("rename", `${"a".repeat(32)}.json`);
    expect(wake).toHaveBeenCalledTimes(1);
    // Error: one immediate rescan, the old handle closed, exactly one one-shot re-arm deadline.
    made[0]!.emit("error", new Error("watch failed"));
    expect(wake).toHaveBeenCalledTimes(2);
    expect(made[0]!.close).toHaveBeenCalledOnce();
    expect(vi.getTimerCount()).toBe(2); // the re-arm deadline + the errored handle's close grace
    await vi.advanceTimersByTimeAsync(499);
    expect(watch).toHaveBeenCalledOnce();
    await vi.advanceTimersByTimeAsync(1);
    expect(watch).toHaveBeenCalledTimes(2);
    expect(wake).toHaveBeenCalledTimes(3); // rescan after a successful re-arm
    // A late event or error from the replaced handle is ignored (no double re-arm, no hot loop).
    made[0]!.emit("error", new Error("late"));
    made[0]!.listener("rename", `${"b".repeat(32)}.json`);
    expect(wake).toHaveBeenCalledTimes(3);
    // Consecutive errors with no delivered event double the deadline, capped at 30 s, never exhausted.
    const deadlines: number[] = [];
    for (let attempt = 1; attempt <= 12; attempt++) {
      made.at(-1)!.emit("error", new Error("again"));
      const before = watch.mock.calls.length;
      let waited = 0;
      while (watch.mock.calls.length === before) { await vi.advanceTimersByTimeAsync(100); waited += 100; }
      deadlines.push(waited);
    }
    expect(deadlines.slice(0, 5)).toEqual([1_000, 2_000, 4_000, 8_000, 16_000]);
    expect(deadlines.slice(5).every(ms => ms === 30_000)).toBe(true);
    // A delivered event resets the backoff to the first deadline.
    made.at(-1)!.listener("change", `${"c".repeat(32)}.abandon`);
    made.at(-1)!.emit("error", new Error("after healthy"));
    const before = watch.mock.calls.length;
    await vi.advanceTimersByTimeAsync(500);
    expect(watch.mock.calls.length).toBe(before + 1);
    expect(interval).not.toHaveBeenCalled();
  } finally {
    const closing = spool.close();
    await vi.advanceTimersByTimeAsync(1_000);
    await closing;
    expect(vi.getTimerCount()).toBe(0);
    vi.useRealTimers();
    vi.restoreAllMocks();
    await fs.promises.rm(root, { recursive: true, force: true });
  }
});

it("child watch: an arm failure rescans and retries on one one-shot deadline", async () => {
  vi.useFakeTimers();
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-child-watch-arm-"));
  let fail = true;
  const { watch } = fakeWatchers(() => fail);
  const wake = vi.fn();
  const spool = new ActorChildCompletionWatch(path.join(root, "child-completions"), wake);
  try {
    expect(wake).toHaveBeenCalledOnce();
    expect(vi.getTimerCount()).toBe(1);
    await vi.advanceTimersByTimeAsync(500);
    expect(watch).toHaveBeenCalledTimes(2);
    expect(wake).toHaveBeenCalledTimes(2);
    fail = false;
    await vi.advanceTimersByTimeAsync(1_000);
    expect(watch).toHaveBeenCalledTimes(3);
    expect(wake).toHaveBeenCalledTimes(3); // successful re-arm rescans once
    expect(vi.getTimerCount()).toBe(0);
  } finally {
    const closing = spool.close();
    await vi.advanceTimersByTimeAsync(1_000);
    await closing;
    vi.useRealTimers();
    vi.restoreAllMocks();
    await fs.promises.rm(root, { recursive: true, force: true });
  }
});

it("child watch: close joins a normal native close, and an errored handle that never closes only for a bounded grace", async () => {
  vi.useFakeTimers();
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-child-watch-close-"));
  const { made } = fakeWatchers();
  try {
    // Normal close: resolves on the native close event, not before.
    const healthy = new ActorChildCompletionWatch(path.join(root, "a"), vi.fn());
    let joined = false;
    const closing = healthy.close().then(() => { joined = true; });
    await vi.advanceTimersByTimeAsync(0);
    expect(made[0]!.close).toHaveBeenCalledOnce();
    expect(joined).toBe(false);
    made[0]!.emit("close");
    await closing;
    expect(joined).toBe(true);
    // Errored handle: a native error closes it with no close event. close() must not hang,
    // and must not resolve before the bounded grace, so a late close is still joined.
    const errored = new ActorChildCompletionWatch(path.join(root, "b"), vi.fn());
    made[1]!.emit("error", new Error("native closed"));
    let released = false;
    const late = errored.close().then(() => { released = true; });
    await vi.advanceTimersByTimeAsync(999);
    expect(released).toBe(false);
    await vi.advanceTimersByTimeAsync(1);
    await late;
    expect(released).toBe(true);
    // A late close inside the grace is joined at once.
    const lateClose = new ActorChildCompletionWatch(path.join(root, "c"), vi.fn());
    const third = made.length - 1;
    made[third]!.emit("error", new Error("closing"));
    let early = false;
    const pending = lateClose.close().then(() => { early = true; });
    await vi.advanceTimersByTimeAsync(100);
    expect(early).toBe(false);
    made[third]!.emit("close");
    await pending;
    expect(early).toBe(true);
    expect(vi.getTimerCount()).toBe(0);
  } finally {
    vi.useRealTimers();
    vi.restoreAllMocks();
    await fs.promises.rm(root, { recursive: true, force: true });
  }
});
