// smarty-dev#9855 part A: an unknown-outcome launch held its admission permit forever, so a long-lived Main with
// maxConcurrent such launches admitted no more spawns. A launch proven gone (ENODEV-unconfirmed exit, worker gone)
// now releases its permit in the same step as its settle, exactly once; held permits are counted and alarmed.
import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import type { AgentRunResult } from "../src/agents/types.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { admissionAlarmRecipient } from "../src/topology/stall-alarms.js";

const ENODEV = "Owned scope observation failed: Error: ENODEV: no such device, read";
const roots: string[] = [];
const managers: AgentManager[] = [];
afterEach(async () => {
  vi.restoreAllMocks();
  for (const manager of managers.splice(0)) await manager.close().catch(() => undefined);
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const deadPid = (): number => spawnSync(process.execPath, ["-e", ""]).pid!;
const children: ChildProcess[] = [];
afterEach(() => { for (const child of children.splice(0)) child.kill("SIGKILL"); });
const onLinux = process.platform === "linux" ? it : it.skip;

const setup = (maxConcurrent = 20, onAdmissionAlarm?: (alarm: unknown) => void, onSettled?: (result: AgentRunResult) => void) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-permits-"));
  roots.push(root);
  const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent, budgetUsd: 0, retainRuns: true }, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: root, ...(onAdmissionAlarm ? { onAdmissionAlarm } : {}),
    ...(onSettled ? { onSettled } : {}),
  });
  managers.push(manager);
  /**
   * `count` launches whose outcome is unknown (the ENODEV observation failure). Each has a REAL live worker process
   * whose pid the launch obligation reports to the manager (placement.ts's shape). Its status.json, which the worker
   * can write, names `statusPid` (default: the real pid). `ownPid: false` reports no pid to the manager.
   */
  const unknownLaunches = async (count: number, options: { statusPid?: () => number; ownPid?: boolean } = {}) => {
    const workers: ChildProcess[] = [];
    const launch = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async () => {
      const worker = spawn(process.execPath, ["-e", "setTimeout(() => {}, 120000)"], { stdio: "ignore" });
      workers.push(worker);
      children.push(worker);
      // placement.ts's obligation: the pid it launched and that process's own close event
      const closed = new Promise<void>(resolve => worker.once("exit", () => resolve()));
      throw Object.assign(new Error(ENODEV), { launchOutcome: "unknown",
        ...(options.ownPid === false ? {} : { cleanupPending: true, transport: "process", sessionId: String(worker.pid), closed }) });
    });
    const ids: string[] = [];
    for (let index = 0; index < count; index++) {
      const handle = await manager.spawn({ task: `unknown ${index}`, transport: "process" });
      expect(handle.status).toBe("queued"); // the unknown-launch receipt
      ids.push(handle.id);
    }
    launch.mockRestore();
    ids.forEach((id, index) => fs.writeFileSync(path.join(root, id, "status.json"), JSON.stringify({
      id, status: "running", transport: "process", sessionId: String(options.statusPid?.() ?? workers[index]!.pid), startedAt: 1, task: "t" })));
    /** The workers exit (the real processes end). */
    const exit = async () => {
      await Promise.all(workers.map(worker => new Promise(resolve => { worker.once("exit", resolve); worker.kill("SIGKILL"); })));
    };
    return { ids, exit };
  };
  return { root, manager, unknownLaunches };
};

/** A real worker process; `closed` resolves on its exit (the transport's own close event). */
const liveWorker = () => {
  const child = spawn(process.execPath, ["-e", "setTimeout(() => {}, 120000)"], { stdio: "ignore" });
  children.push(child);
  let alive = true;
  const closed = new Promise<void>(resolve => child.once("exit", () => { alive = false; resolve(); }));
  return { child, closed, alive: () => alive,
    exit: async () => { if (alive) { child.kill("SIGKILL"); await closed; } } };
};

/**
 * A launched (managed) run whose contact is lost with `reason` (default the ENODEV observation failure), with the
 * worker's saved answer. `closedWhileAlive`: the transport reports the run closed at once while its pid still lives.
 */
const lostContactRun = async (options: { reason?: string; closedWhileAlive?: boolean; failCompletedSaves?: { on: boolean; remaining?: number } } = {}) => {
  const alarms: Array<{ kind?: string; runId?: string }> = [];
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-permits-"));
  roots.push(root);
  const settledResults: Array<{ id: string; status: string; text?: string }> = [];
  const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent: 1, budgetUsd: 0, retainRuns: true }, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: root,
    onSettled: result => {
      const failing = options.failCompletedSaves;
      if (result.status === "completed" && failing?.on && (failing.remaining === undefined || failing.remaining-- > 0)) {
        throw new Error("archive or routing failed");
      }
      settledResults.push({ id: result.id, status: result.status, ...(result.text ? { text: result.text } : {}) });
    },
    onAdmissionAlarm: alarm => alarms.push(alarm),
  });
  managers.push(manager);
  const worker = liveWorker();
  const reason = options.reason ?? ENODEV;
  const launch = vi.spyOn(ProcessTransport.prototype, "launch").mockResolvedValueOnce({
    kind: "process", sessionId: String(worker.child.pid), stop: async () => undefined, closed: worker.closed,
    isAlive: async () => !options.closedWhileAlive && worker.alive(),
    lostContact: () => (options.closedWhileAlive || !worker.alive() ? reason : undefined),
  } as never);
  const handle = await manager.spawn({ task: "lost contact", transport: "process" });
  launch.mockRestore();
  const directory = path.join(root, handle.id);
  fs.writeFileSync(path.join(directory, "status.json"), JSON.stringify({ id: handle.id, name: "lost contact", task: "lost contact",
    status: "running", runner: "pi", transport: "process", sessionId: String(worker.child.pid), cwd: process.cwd(), startedAt: Date.now(),
    updatedAt: Date.now(), turns: 0, toolCalls: 0, text: "", usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 } }));
  fs.writeFileSync(path.join(directory, "settlement.json"), JSON.stringify({ version: 1, runId: handle.id, outcome: "completed", text: "SAVED" }));
  const saved = () => settledResults.filter(result => result.id === handle.id && result.status === "completed");
  return { manager, root, handle, directory, worker, saved, alarms };
};

describe("held admission permits (smarty-dev#9855)", () => {
  onLinux("20 unknown launches hold every permit; once they are proven gone, the 21st spawn is admitted", async () => {
    const { root, manager, unknownLaunches } = setup();
    const { ids, exit } = await unknownLaunches(20);
    expect(manager.heldPermits()).toBe(20);
    // The first worker saved its answer before it exited.
    fs.writeFileSync(path.join(root, ids[0]!, "settlement.json"), JSON.stringify({ version: 1, runId: ids[0], outcome: "completed", text: "SAVED" }));
    await exit();
    const next = await manager.spawn({ task: "21st", transport: "process" });
    expect(next.status).not.toBe("queued"); // on 3f975f48 it waits behind 20 leaked permits forever
    expect(manager.heldPermits()).toBe(0);
    for (const id of ids) expect(fs.existsSync(path.join(root, id, "unresolved-worker.json"))).toBe(false);
    expect(await manager.wait(ids[0]!, { timeoutMs: 1_000 })).toMatchObject({ status: "completed", text: "SAVED" });
    expect(await manager.wait(ids[1]!, { timeoutMs: 1_000 })).toMatchObject({ status: "failed", settledLost: true });
  });

  onLinux("a launch whose worker is still alive keeps its permit", async () => {
    const { root, manager, unknownLaunches } = setup(2);
    const { ids } = await unknownLaunches(2); // live workers: not proven gone
    expect(manager.heldPermits()).toBe(2);
    const next = await manager.spawn({ task: "third", transport: "process" });
    expect(next.status).toBe("queued");
    expect(next.heldPermits).toBe(2);
    expect(manager.heldPermits()).toBe(2);
    for (const id of ids) expect(fs.existsSync(path.join(root, id, "unresolved-worker.json"))).toBe(true);
    await manager.stop(next.id).catch(() => undefined);
  });

  onLinux("a permit is released once: a second settle or a replay frees nothing more", async () => {
    const { manager, unknownLaunches } = setup(3);
    await (await unknownLaunches(3)).exit();
    await manager.stop((await manager.spawn({ task: "frees", transport: "process", })).id).catch(() => undefined);
    expect(manager.heldPermits()).toBe(0);
    await manager.settleLostEnodevRuns("no-actor", new Set()); // another settle pass: nothing left to release
    // Capacity is exactly 3 again: three new unknown launches (live workers) are admitted, the fourth spawn waits.
    await unknownLaunches(3);
    const fourth = await manager.spawn({ task: "fourth", transport: "process" });
    expect(fourth.status).toBe("queued");
    expect(fourth.heldPermits).toBe(3);
    await manager.stop(fourth.id).catch(() => undefined);
  });

  onLinux("held permits at half of maxConcurrent raise one alarm, again only after 6 h", async () => {
    const alarms: unknown[] = [];
    const { unknownLaunches } = setup(20, alarm => alarms.push(alarm));
    await unknownLaunches(9);
    expect(alarms).toHaveLength(0);
    await unknownLaunches(1);
    expect(alarms).toEqual([expect.objectContaining({ heldPermits: 10, maxConcurrent: 20 })]);
    await unknownLaunches(2);
    expect(alarms).toHaveLength(1);
    const now = Date.now();
    vi.spyOn(Date, "now").mockReturnValue(now + 6 * 60 * 60 * 1_000 + 1);
    await unknownLaunches(1);
    expect(alarms).toHaveLength(2);
    expect(alarms[1]).toMatchObject({ heldPermits: 12 }); // re-alarmed at the admission attempt, before the 13th hold
  });

  // pi-fabric#939 round 1 P1: proof of exit comes only from the manager's own pid record, never the status file.
  onLinux("a worker that rewrites its status file with a dead pid while it lives keeps its permit", async () => {
    const { root, manager, unknownLaunches } = setup(2);
    const { ids } = await unknownLaunches(2, { statusPid: deadPid }); // forged: run id, transport process, dead pid
    const next = await manager.spawn({ task: "third", transport: "process" });
    expect(next.status).toBe("queued"); // b74fa4bb admitted it: over-admission past maxConcurrent
    expect(manager.heldPermits()).toBe(2);
    for (const id of ids) expect(fs.existsSync(path.join(root, id, "unresolved-worker.json"))).toBe(true);
    await manager.stop(next.id).catch(() => undefined);
  });

  onLinux("with no pid of its own for a launch, the manager never releases its permit, and still counts it", async () => {
    const { manager, unknownLaunches } = setup(2);
    await (await unknownLaunches(2, { ownPid: false, statusPid: deadPid })).exit();
    const next = await manager.spawn({ task: "third", transport: "process" });
    expect(next.status).toBe("queued");
    expect(next.heldPermits).toBe(2);
    await manager.stop(next.id).catch(() => undefined);
  });

  // pi-fabric#939 round 1 P2-a: a state stuck above the threshold re-alarms on an admission attempt, with no new hold.
  onLinux("a stuck held state re-alarms on the next spawn attempt after 6 h", async () => {
    const alarms: unknown[] = [];
    const { manager, unknownLaunches } = setup(2, alarm => alarms.push(alarm));
    await unknownLaunches(2);
    expect(alarms).toHaveLength(1);
    const now = Date.now();
    vi.spyOn(Date, "now").mockReturnValue(now + 6 * 60 * 60 * 1_000 + 1);
    const waiting = await manager.spawn({ task: "attempt", transport: "process" }); // no new held permit
    expect(waiting.status).toBe("queued");
    expect(alarms).toHaveLength(2);
    await manager.stop(waiting.id).catch(() => undefined);
  });
});

// pi-fabric#939 round 2: a launched (managed) run that lost contact settles without its answer; the worker had saved
// it. When the worker is proven gone, the spawner gets that saved answer once, through its completion path, and only
// then is the permit released.
describe("a lost-contact run's saved answer", () => {
  onLinux("reaches the spawner exactly once before its permit is released", async () => {
    const { manager, handle, worker, saved } = await lostContactRun();
    await worker.exit();
    const first = await manager.wait(handle.id, { timeoutMs: 15_000 });
    expect(first.status).not.toBe("completed"); // settled without the answer: the exit was unconfirmed
    // The worker is already gone when the hold begins (round 3): delivered first, then released, with no new spawn.
    await vi.waitFor(() => expect(manager.heldPermits()).toBe(0), { timeout: 5_000, interval: 20 });
    expect(saved()).toEqual([{ id: handle.id, status: "completed", text: "SAVED" }]);
    const next = await manager.spawn({ task: "next", transport: "process" });
    expect(saved()).toEqual([{ id: handle.id, status: "completed", text: "SAVED" }]);
    expect(manager.status(handle.id)).toMatchObject({ status: "completed", text: "SAVED" });
    expect(manager.heldPermits()).toBe(0);
    expect(next.status).not.toBe("queued");
    await manager.settleLostEnodevRuns("no-actor", new Set()); // a second release attempt
    await manager.stop(next.id).catch(() => undefined);
    await manager.spawn({ task: "again", transport: "process" }).then(again => manager.stop(again.id)).catch(() => undefined);
    expect(saved()).toHaveLength(1);
  }, 60_000);

  // pi-fabric#939 round 3 SEC 1: nothing is delivered or released before the exact gate holds.
  onLinux("another unresolved reason: no delivery, the permit stays", async () => {
    const { manager, handle, worker, saved } = await lostContactRun({ reason: "server unreachable; worker may still run" });
    await worker.exit();
    await manager.wait(handle.id, { timeoutMs: 15_000 });
    const probe = await manager.spawn({ task: "probe", transport: "process" }); // an admission pass
    await new Promise(resolve => setTimeout(resolve, 300));
    expect(saved()).toHaveLength(0);
    expect(manager.heldPermits()).toBe(1);
    expect(probe.status).toBe("queued");
    await manager.stop(probe.id).catch(() => undefined);
  }, 60_000);

  onLinux("a missing marker: no delivery, the permit stays, even when the worker exits", async () => {
    const { manager, handle, directory, worker, saved } = await lostContactRun({ closedWhileAlive: true });
    await manager.wait(handle.id, { timeoutMs: 15_000 });
    await vi.waitFor(() => expect(manager.heldPermits()).toBe(1), { timeout: 5_000, interval: 20 });
    fs.rmSync(path.join(directory, "unresolved-worker.json"), { force: true });
    await worker.exit(); // its close event re-checks
    const probe = await manager.spawn({ task: "probe", transport: "process" }); // and an admission pass
    await new Promise(resolve => setTimeout(resolve, 300));
    expect(saved()).toHaveLength(0);
    expect(manager.heldPermits()).toBe(1);
    await manager.stop(probe.id).catch(() => undefined);
  }, 60_000);

  // SEC 2: prove, save, mark completed, release. A failed save keeps it unmarked and retried, then delivered once.
  onLinux("a failed save keeps the permit and retries the delivery; then it is delivered once and released", async () => {
    const failing = { on: true };
    const { manager, handle, directory, worker, saved } = await lostContactRun({ failCompletedSaves: failing });
    await worker.exit();
    await manager.wait(handle.id, { timeoutMs: 15_000 });
    const probe = await manager.spawn({ task: "probe", transport: "process" }); // a pass whose save fails
    await new Promise(resolve => setTimeout(resolve, 300));
    expect(saved()).toHaveLength(0);
    expect(manager.heldPermits()).toBe(1);
    expect(JSON.parse(fs.readFileSync(path.join(directory, "status.json"), "utf8")).status).not.toBe("completed");
    await manager.stop(probe.id).catch(() => undefined);
    failing.on = false;
    const next = await manager.spawn({ task: "retry", transport: "process" }); // the next pass
    expect(saved()).toEqual([{ id: handle.id, status: "completed", text: "SAVED" }]);
    expect(manager.heldPermits()).toBe(0);
    expect(next.status).not.toBe("queued");
    await manager.stop(next.id).catch(() => undefined);
  }, 60_000);
});

// pi-fabric#939 round 4 P1-2: a failing recovered-answer save retries on its own (bounded backoff) and alarms the owner
// once, while a waiter is queued; no spawn event is needed.
describe("a recovered answer whose save fails", () => {
  onLinux("fails twice, then the bounded retry delivers it once and admits the queued waiter, with one owner alarm", async () => {
    const failing = { on: true, remaining: 2 };
    const { manager, handle, worker, saved, alarms } = await lostContactRun({ failCompletedSaves: failing });
    const waiter = await manager.spawn({ task: "waiter", transport: "process" }); // queued behind the held permit
    expect(waiter.status).toBe("queued");
    await worker.exit();
    await manager.wait(handle.id, { timeoutMs: 15_000 });
    await vi.waitFor(() => expect(saved()).toHaveLength(1), { timeout: 15_000, interval: 50 });
    await vi.waitFor(() => expect(manager.status(waiter.id).status).not.toBe("queued"), { timeout: 5_000, interval: 50 });
    expect(manager.heldPermits()).toBe(0);
    expect(failing.remaining).toBeLessThanOrEqual(0);
    expect(alarms.filter(alarm => alarm.kind === "recovered-answer-save-failed")).toEqual([expect.objectContaining({ runId: handle.id })]);
    await new Promise(resolve => setTimeout(resolve, 500));
    expect(saved()).toHaveLength(1);
    await manager.stop(waiter.id).catch(() => undefined);
  }, 60_000);

  onLinux("a permanent failure keeps the permit and raises one owner alarm", async () => {
    const { manager, handle, worker, saved, alarms } = await lostContactRun({ failCompletedSaves: { on: true } });
    const waiter = await manager.spawn({ task: "waiter", transport: "process" });
    await worker.exit();
    await manager.wait(handle.id, { timeoutMs: 15_000 });
    await new Promise(resolve => setTimeout(resolve, 4_000)); // the first retries (1 s, 2 s) also fail
    expect(saved()).toHaveLength(0);
    expect(manager.heldPermits()).toBe(1);
    expect(manager.status(waiter.id).status).toBe("queued");
    expect(alarms.filter(alarm => alarm.kind === "recovered-answer-save-failed")).toHaveLength(1);
    await manager.stop(waiter.id).catch(() => undefined);
  }, 60_000);
});

// pi-fabric#939 round 5: an unknown launch's outcome is saved and routed BEFORE its marker is settled and its permit
// released; until then the marker, custody and permit stay.
describe("a held unknown launch's outcome", () => {
  const held = (failing: { on: boolean }) => {
    const delivered: string[] = [];
    const alarms: Array<{ kind?: string; runId?: string }> = [];
    const env = setup(1, alarm => alarms.push(alarm as { kind?: string }), result => {
      if (result.status === "completed" && failing.on) throw new Error("route save failed");
      if (result.status === "completed" && result.text === "SAVED") delivered.push(String(result.text));
    });
    return { ...env, delivered, alarms };
  };
  const marker = (root: string, id: string) => fs.existsSync(path.join(root, id, "unresolved-worker.json"));

  onLinux("a route save that fails keeps the marker and the permit; the retry delivers it once, with one alarm", async () => {
    const failing = { on: true };
    const { root, manager, unknownLaunches, delivered, alarms } = held(failing);
    const { ids, exit } = await unknownLaunches(1);
    fs.writeFileSync(path.join(root, ids[0]!, "settlement.json"), JSON.stringify({ version: 1, runId: ids[0], outcome: "completed", text: "SAVED" }));
    const waiter = await manager.spawn({ task: "waiter", transport: "process" });
    await exit();
    await new Promise(resolve => setTimeout(resolve, 300));
    expect(delivered).toEqual([]);
    expect(marker(root, ids[0]!)).toBe(true); // a30ec88f removed it before the save
    expect(manager.heldPermits()).toBe(1);
    expect(manager.status(waiter.id).status).toBe("queued");
    failing.on = false;
    await vi.waitFor(() => expect(delivered).toEqual(["SAVED"]), { timeout: 10_000, interval: 50 }); // the bounded retry
    await vi.waitFor(() => expect(manager.status(waiter.id).status).not.toBe("queued"), { timeout: 5_000, interval: 50 });
    expect(marker(root, ids[0]!)).toBe(false);
    expect(alarms.filter(alarm => alarm.kind === "recovered-answer-save-failed")).toEqual([expect.objectContaining({ runId: ids[0] })]);
    await manager.stop(waiter.id).catch(() => undefined);
  }, 60_000);

  onLinux("a failure before the route save (crash point): nothing is lost; marker, answer and permit stay", async () => {
    const { root, manager, unknownLaunches, delivered } = held({ on: true });
    const { ids, exit } = await unknownLaunches(1);
    fs.writeFileSync(path.join(root, ids[0]!, "settlement.json"), JSON.stringify({ version: 1, runId: ids[0], outcome: "completed", text: "SAVED" }));
    await exit();
    await new Promise(resolve => setTimeout(resolve, 300));
    expect(delivered).toEqual([]);
    expect(marker(root, ids[0]!)).toBe(true);
    expect(fs.existsSync(path.join(root, ids[0]!, "settlement.json"))).toBe(true);
    expect(JSON.parse(fs.readFileSync(path.join(root, ids[0]!, "status.json"), "utf8")).status).toBe("running");
    expect(manager.heldPermits()).toBe(1);
  }, 60_000);

  onLinux("a failure after the route save, before the marker settles (crash point): delivered once, then settled", async () => {
    const { root, manager, unknownLaunches, delivered } = held({ on: false });
    const { ids, exit } = await unknownLaunches(1);
    fs.writeFileSync(path.join(root, ids[0]!, "settlement.json"), JSON.stringify({ version: 1, runId: ids[0], outcome: "completed", text: "SAVED" }));
    const rmSync = fs.rmSync.bind(fs);
    let crashes = 1;
    const spy = vi.spyOn(fs, "rmSync").mockImplementation((target, options) => {
      if (String(target).endsWith("unresolved-worker.json") && crashes-- > 0) throw new Error("crash before the marker settles");
      return rmSync(target, options);
    });
    await exit();
    await vi.waitFor(() => expect(delivered).toEqual(["SAVED"]), { timeout: 5_000, interval: 20 });
    await vi.waitFor(() => expect(marker(root, ids[0]!)).toBe(false), { timeout: 10_000, interval: 50 }); // the retry settles it
    expect(manager.heldPermits()).toBe(0);
    expect(delivered).toEqual(["SAVED"]); // not delivered again
    spy.mockRestore();
  }, 60_000);
});

// pi-fabric#939 round 3 P1-a: a spawn already waiting is admitted when a held launch becomes provably gone, with no
// other spawn or settle to trigger the check.
describe("a waiting spawn", () => {
  onLinux("is admitted once the held launch it waits behind is proven gone", async () => {
    const { manager, unknownLaunches } = setup(1);
    const { exit } = await unknownLaunches(1);
    const waiter = await manager.spawn({ task: "waiter", transport: "process" });
    expect(waiter.status).toBe("queued");
    await exit();
    await vi.waitFor(() => expect(manager.status(waiter.id).status).not.toBe("queued"), { timeout: 12_000, interval: 50 });
    expect(manager.heldPermits()).toBe(0);
    await manager.stop(waiter.id).catch(() => undefined);
  }, 30_000);
});

// pi-fabric#939 round 3 P1-b: after a relaunch, the proof must be against the REPLACEMENT worker's pid.
describe("a relaunched run", () => {
  onLinux("keeps its permit while the replacement lives, though the first worker is gone; releases it when the replacement is gone", async () => {
    const { manager } = setup(1);
    const first = liveWorker(), second = liveWorker();
    let launches = 0;
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async () => {
      launches++;
      if (launches === 1) return { kind: "process", sessionId: String(first.child.pid), isAlive: async () => first.alive(),
        stop: async () => undefined, closed: first.closed, lostContact: () => undefined } as never;
      // The replacement's observation fails (ENODEV) and its transport reports it closed while its pid still lives.
      return { kind: "process", sessionId: String(second.child.pid), isAlive: async () => false, stop: async () => undefined,
        closed: second.closed, lostContact: () => ENODEV } as never;
    });
    const handle = await manager.spawn({ task: "relaunched", transport: "process" });
    await first.exit(); // the first attempt dies without a result: a startup retry launches the replacement
    await vi.waitFor(() => expect(launches).toBe(2), { timeout: 15_000, interval: 50 });
    await vi.waitFor(() => expect(manager.heldPermits()).toBe(1), { timeout: 15_000, interval: 50 });
    const waiter = await manager.spawn({ task: "waiter", transport: "process" });
    await new Promise(resolve => setTimeout(resolve, 300));
    expect(manager.heldPermits()).toBe(1); // the old pid is gone, the replacement lives: not proven gone
    expect(manager.status(waiter.id).status).toBe("queued");
    await second.exit();
    await vi.waitFor(() => expect(manager.heldPermits()).toBe(0), { timeout: 12_000, interval: 50 });
    await manager.stop(waiter.id).catch(() => undefined);
    await manager.stop(handle.id).catch(() => undefined);
  }, 60_000);
});

// pi-fabric#939 round 1 P2-b: an owner alarm always has one exact session address, never a name or a broadcast.
describe("admission alarm recipient", () => {
  const main = { id: "session:01a118ab-50cf-75ea-ae00-721c27d6d6cc", name: "main", kind: "main" as const };
  it("uses the owner identity, else the Main's own session, else none", () => {
    expect(admissionAlarmRecipient("session:01a10367-7cd0-7ebd-8922-ee07a35962c9", main)).toBe("session:01a10367-7cd0-7ebd-8922-ee07a35962c9");
    expect(admissionAlarmRecipient(undefined, main)).toBe(main.id);
    expect(admissionAlarmRecipient("  ", main)).toBe(main.id);
    expect(admissionAlarmRecipient(undefined, undefined)).toBeUndefined();
    expect(admissionAlarmRecipient(undefined, { id: "host:resident", name: "resident", kind: "actor" })).toBeUndefined();
  });
  it("never addresses an org instance by name", () => {
    expect(admissionAlarmRecipient("org", main)).toBe(main.id);
    expect(admissionAlarmRecipient("org-kate", undefined)).toBeUndefined();
    expect(admissionAlarmRecipient(undefined, { id: "org", name: "org", kind: "main" })).toBeUndefined();
  });
});
