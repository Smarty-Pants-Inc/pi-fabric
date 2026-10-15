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
it.each([false, true])("wakes a real idle owner and retries a failed durable queue write (persist failure: %s)", async (persistFailure) => {
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
      resolvePiModel: async (model: string) => model, retentionSweepMs: 600_000, preparationRetryMs: 50,
    });
  const mkdir = vi.spyOn(fs, "mkdirSync");
  let denyQueueWrite = false;
  let refusedWrites = 0;
  const rename = fs.renameSync;
  vi.spyOn(fs, "renameSync").mockImplementation((source, target) => {
    if (denyQueueWrite && path.basename(String(target)).startsWith("queue-") && String(target).endsWith(".json")) {
      refusedWrites++;
      // EIO exercises the owner's retry, not writeJsonAtomic's eight inline EBUSY retries.
      throw Object.assign(new Error("recorded transient queue write refusal"), { code: "EIO" });
    }
    return rename(source, target);
  });
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

    denyQueueWrite = persistFailure;
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
    if (persistFailure) {
      await vi.waitFor(() => expect(refusedWrites).toBeGreaterThan(0), { timeout: 2000 });
      await new Promise(resolve => setTimeout(resolve, 200));
      expect(fs.existsSync(path.join(spool, `${id}.json`))).toBe(true);
      expect(fs.existsSync(path.join(spool, `${id}.receipt`))).toBe(false);
      expect(run).not.toHaveBeenCalled();
      expect(refusedWrites).toBeLessThan(20);
      denyQueueWrite = false;
    }

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
    denyQueueWrite = false;
    mkdir.mockRestore();
    release();
    await owner.close();
    await agents.close();
    vi.restoreAllMocks();
    await fs.promises.rm(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
  }
}, 30_000);

it("releases completion watches on stop, transfer and 200 recorded actor removals without closing the manager", async () => {
  const portableFence = ["linux", "darwin", "win32"].includes(process.platform);
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-child-watch-lifecycle-"));
  const actorRoot = path.join(root, "actors");
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const agents = new AgentManager(root, DEFAULT_FABRIC_CONFIG.agents, { runRoot: path.join(root, "runs") });
  const lost = new Set<string>();
  const live = new Set<fs.FSWatcher>();
  let armed = 0;
  const nativeWatch = fs.watch;
  const watch = vi.spyOn(fs, "watch").mockImplementation(((...args: unknown[]) => {
    const handle: fs.FSWatcher = Reflect.apply(nativeWatch, fs, args);
    if (path.basename(String(args[0])) === "child-completions") {
      armed++;
      live.add(handle);
      handle.once("close", () => live.delete(handle));
    }
    return handle;
  }) as typeof fs.watch);
  const remove = fs.rmSync;
  const removals: string[] = [];
  const recorder = vi.spyOn(fs, "rmSync").mockImplementation((target, options) => {
    const file = path.resolve(String(target));
    // Record data removal, never execute it. Atomic-write lock/temp cleanup is
    // fixture housekeeping, not a real actor/session purge (#4664).
    if (file.startsWith(actorRoot + path.sep) &&
        (path.basename(file).startsWith("queue-") || path.basename(file).startsWith("removal-") ||
         /^[a-f0-9]{32}$/.test(path.basename(file)))) {
      removals.push(file);
      return;
    }
    return remove(target, options);
  });
  const owner = new ActorManager("watch-lifecycle", { id: "session:watch-lifecycle", name: "owner", kind: "main" },
    mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 600_000 }, agents, vi.fn(), {
      persistent: true, actorRoot, retentionSweepMs: 600_000, canManageActor: id => !lost.has(id),
      resolvePiModel: async model => model,
    });
  const create = async (name: string) => {
    const actor = await owner.create({ name, instructions: "Wait", triggerTurn: false });
    // A real monitor pass, not a direct store/watch call; its preflight arms the watch.
    await vi.waitFor(() => expect(live.size).toBe(1), { timeout: 5000, interval: 10 });
    return actor;
  };
  try {
    expect(live.size).toBe(0);
    const stopped = await create("stop-watch");
    await owner.stop(stopped.id);
    expect(live.size).toBe(0); // stop joins native close, manager stays open
    for (let index = 0; index < 200; index++) {
      const actor = await create(`churn-${index}`);
      const result = await owner.remove(actor.id);
      expect(result).toMatchObject({ removed: true });
      if (!portableFence) {
        expect(result).toMatchObject({ cleaned: false, pending:
          "removal cleanup failed: Root actor tree deletion needs the Main publication fence, " +
          `which this platform (${process.platform}) lacks (smarty-dev#8090); follow-up smarty-dev#11335; ` +
          `the root ${actor.rootId} actor tree is kept (a later remove or host start finishes it)` });
      }
      expect(live.size).toBe(0);
    }
    // A real foreign opinion intentionally blocks further registry creation, so
    // exercise transfer after churn rather than bypassing that custody gate.
    const transferred = await create("transfer-watch");
    lost.add(transferred.id);
    owner.listOwned();
    await vi.waitFor(() => expect(live.size).toBe(0), { timeout: 5000, interval: 10 });
    expect(armed).toBeGreaterThanOrEqual(202);
    // Only unsupported platforms keep trees/cleanup markers (smarty-dev#8090, smarty-dev#11335).
    expect(removals.filter(file => /^[a-f0-9]{32}$/.test(path.basename(file))))
      .toHaveLength(portableFence ? 200 : 0);
    expect(owner.status(stopped.id).status).toBe("stopped"); // not manager shutdown
  } finally {
    await owner.close();
    await agents.close();
    watch.mockRestore();
    recorder.mockRestore();
    fs.rmSync(root, { recursive: true, force: true }); // own test fixture cleanup
  }
}, 120_000);

// Child spool watch failure contract: one immediate rescan/re-arm, then finite
// 60/120/240/300 s one-shots. Only delivered events reset the budget. Exhaustion
// exposes observation-degraded and warns once; one60s scan/re-arm keeps late work
// recoverable. Native recovery stops scans; close joins native close or bounded grace.
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

it("child watch: one immediate recovery then minute-scale backoff, reset only by delivered events, with no interval", async () => {
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
    // First failure rescans and re-arms immediately, without a retry timer.
    made[0]!.emit("error", new Error("watch failed"));
    expect(made[0]!.close).toHaveBeenCalledOnce();
    expect(watch).toHaveBeenCalledTimes(2);
    expect(wake).toHaveBeenCalledTimes(3); // immediate rescan + successful re-arm rescan
    // A late event or error from the replaced handle is ignored.
    made[0]!.emit("error", new Error("late"));
    made[0]!.listener("rename", `${"b".repeat(32)}.json`);
    expect(wake).toHaveBeenCalledTimes(3);
    // Arming alone does not reset the budget: each later failure waits its deadline.
    for (const delay of [60_000, 120_000, 240_000]) {
      made.at(-1)!.emit("error", new Error("again"));
      const before = watch.mock.calls.length;
      const wakes = wake.mock.calls.length;
      await vi.advanceTimersByTimeAsync(delay - 1);
      expect(watch.mock.calls.length).toBe(before);
      expect(wake.mock.calls.length).toBe(wakes);
      await vi.advanceTimersByTimeAsync(1);
      expect(watch.mock.calls.length).toBe(before + 1);
    }
    // A delivered event resets the budget to the single immediate recovery.
    made.at(-1)!.listener("change", `${"c".repeat(32)}.abandon`);
    const before = watch.mock.calls.length;
    made.at(-1)!.emit("error", new Error("after healthy"));
    expect(watch.mock.calls.length).toBe(before + 1);
    expect(spool.unavailable).toBe(false);
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

it("child watch: persistent arm failure retries no faster than once per minute, then stays visibly degraded with one warning", async () => {
  vi.useFakeTimers();
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-child-watch-arm-"));
  const { watch } = fakeWatchers(() => true);
  const warning = vi.spyOn(console, "warn").mockImplementation(() => {});
  const wake = vi.fn();
  const spool = new ActorChildCompletionWatch(path.join(root, "child-completions"), wake);
  try {
    expect(watch).toHaveBeenCalledTimes(2); // initial arm + sole immediate recovery
    expect(wake).toHaveBeenCalledOnce();
    expect(vi.getTimerCount()).toBe(1);
    for (const delay of [60_000, 120_000, 240_000, 300_000]) {
      const before = watch.mock.calls.length;
      await vi.advanceTimersByTimeAsync(delay - 1);
      expect(watch.mock.calls.length).toBe(before);
      await vi.advanceTimersByTimeAsync(1);
      expect(watch.mock.calls.length).toBe(before + 1);
    }
    expect(watch).toHaveBeenCalledTimes(6);
    expect(wake).toHaveBeenCalledOnce(); // retry attempts never poll the spool
    expect(spool.unavailable).toBe(true);
    expect(spool.health).toBe("observation-degraded");
    expect(warning).toHaveBeenCalledOnce();
    expect(warning).toHaveBeenCalledWith(expect.stringContaining("observation-degraded"));
    expect(vi.getTimerCount()).toBe(1);
    await vi.advanceTimersByTimeAsync(59_999);
    expect(watch).toHaveBeenCalledTimes(6);
    expect(wake).toHaveBeenCalledOnce();
    await vi.advanceTimersByTimeAsync(1);
    expect(watch).toHaveBeenCalledTimes(7);
    expect(wake).toHaveBeenCalledTimes(2); // bounded degraded scan, not abandoned work
    expect(warning).toHaveBeenCalledOnce();
  } finally {
    const closing = spool.close();
    await vi.advanceTimersByTimeAsync(1_000);
    await closing;
    vi.useRealTimers();
    vi.restoreAllMocks();
    await fs.promises.rm(root, { recursive: true, force: true });
  }
});

it("child watch: late durable work is claimed in one degraded tick; native recovery stops all fallback scans", async () => {
  vi.useFakeTimers();
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-child-degraded-"));
  const session = path.join(root, "session.jsonl");
  fs.writeFileSync(session, "");
  const store = new ActorChildCompletionStore(session);
  const id = "a".repeat(32);
  const file = path.join(store.directory, `${id}.json`);
  const remove = fs.rmSync;
  const removals: string[] = [];
  vi.spyOn(fs, "rmSync").mockImplementation((target, options) => {
    if (String(target) === file) { removals.push(String(target)); return; }
    return remove(target, options);
  });
  let fail = true;
  const { made, watch } = fakeWatchers(() => fail);
  const warning = vi.spyOn(console, "warn").mockImplementation(() => {});
  const claimed: string[] = [];
  const wake = vi.fn(() => {
    for (const { result } of store.pending({ actorId: "actor:owner" })) {
      store.acknowledge(result.id, { handoff: true });
      claimed.push(result.id);
    }
  });
  const spool = new ActorChildCompletionWatch(store.directory, wake);
  try {
    for (const delay of [60_000, 120_000, 240_000, 300_000]) await vi.advanceTimersByTimeAsync(delay);
    expect(spool.health).toBe("observation-degraded");
    expect(wake).toHaveBeenCalledOnce();
    const result = { id, name: "late", status: "completed", text: "late saved result", startedAt: Date.now(), finishedAt: Date.now() } as AgentRunResult;
    store.enqueue(result, { id: "actor:owner", kind: "actor", runId: "b".repeat(32) });
    await vi.advanceTimersByTimeAsync(59_999);
    expect(claimed).toEqual([]);
    await vi.advanceTimersByTimeAsync(1);
    expect(claimed).toEqual([id]);
    expect(store.mailboxClaimed(id)).toBe(true);
    expect(removals.length).toBeGreaterThan(0); // recorded, never executed
    expect(fs.existsSync(store.resultFile(id))).toBe(true);
    expect(warning).toHaveBeenCalledOnce();
    expect(spool.health).toBe("observation-degraded");
    fail = false;
    await vi.advanceTimersByTimeAsync(60_000);
    expect(watch).toHaveBeenCalledTimes(8);
    expect(spool.health).toBe("observing");
    expect(spool.unavailable).toBe(false);
    expect(vi.getTimerCount()).toBe(0);
    const scans = wake.mock.calls.length;
    await vi.advanceTimersByTimeAsync(600_000);
    expect(wake).toHaveBeenCalledTimes(scans);
    expect(claimed).toEqual([id]);
    expect(vi.getTimerCount()).toBe(0);
  } finally {
    const closing = spool.close();
    made.at(-1)?.emit("close");
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
  // Immediate recovery creates a healthy replacement. Make its close observable,
  // while the original errored handle deliberately never reports native close.
  const closeReplacement = () => made.at(-1)!.close.mockImplementation(() => made.at(-1)!.emit("close"));
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
    closeReplacement();
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
    closeReplacement();
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
