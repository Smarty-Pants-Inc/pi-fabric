// smarty-dev#9855 part A: an unknown-outcome launch held its admission permit forever, so a long-lived Main with
// maxConcurrent such launches admitted no more spawns. A launch proven gone (ENODEV-unconfirmed exit, worker gone)
// now releases its permit in the same step as its settle, exactly once; held permits are counted and alarmed.
import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
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

const setup = (maxConcurrent = 20, onAdmissionAlarm?: (alarm: unknown) => void) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-permits-"));
  roots.push(root);
  const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent, budgetUsd: 0, retainRuns: true }, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: root, ...(onAdmissionAlarm ? { onAdmissionAlarm } : {}),
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
      throw Object.assign(new Error(ENODEV), { launchOutcome: "unknown",
        ...(options.ownPid === false ? {} : { cleanupPending: true, transport: "process", sessionId: String(worker.pid) }) });
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

describe("held admission permits (smarty-dev#9855)", () => {
  onLinux("20 unknown launches hold every permit; once they are proven gone, the 21st spawn is admitted", async () => {
    const { root, manager, unknownLaunches } = setup();
    const { ids, exit } = await unknownLaunches(20);
    expect(manager.heldPermits()).toBe(20);
    await exit();
    fs.writeFileSync(path.join(root, ids[0]!, "settlement.json"), JSON.stringify({ version: 1, runId: ids[0], outcome: "completed", text: "SAVED" }));
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
    manager.settleLostEnodevRuns("no-actor", new Set()); // another settle pass: nothing left to release
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
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-permits-"));
    roots.push(root);
    const settledResults: Array<{ id: string; status: string; text?: string }> = [];
    const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent: 1, budgetUsd: 0, retainRuns: true }, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: root,
      onSettled: result => settledResults.push({ id: result.id, status: result.status, ...(result.text ? { text: result.text } : {}) }),
    });
    managers.push(manager);
    const worker = spawn(process.execPath, ["-e", "setTimeout(() => {}, 120000)"], { stdio: "ignore" });
    children.push(worker);
    let alive = true;
    worker.once("exit", () => { alive = false; });
    const launch = vi.spyOn(ProcessTransport.prototype, "launch").mockResolvedValueOnce({
      kind: "process", sessionId: String(worker.pid), isAlive: async () => alive, stop: async () => undefined,
      lostContact: () => (alive ? undefined : ENODEV),
    } as never);
    const handle = await manager.spawn({ task: "lost contact", transport: "process" });
    launch.mockRestore();
    const directory = path.join(root, handle.id);
    fs.writeFileSync(path.join(directory, "status.json"), JSON.stringify({ id: handle.id, name: "lost contact", task: "lost contact",
      status: "running", runner: "pi", transport: "process", sessionId: String(worker.pid), cwd: process.cwd(), startedAt: Date.now(),
      updatedAt: Date.now(), turns: 0, toolCalls: 0, text: "", usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 } }));
    fs.writeFileSync(path.join(directory, "settlement.json"), JSON.stringify({ version: 1, runId: handle.id, outcome: "completed", text: "SAVED" }));
    await new Promise(resolve => { worker.once("exit", resolve); worker.kill("SIGKILL"); });
    const first = await manager.wait(handle.id, { timeoutMs: 15_000 });
    expect(first.status).not.toBe("completed"); // settled without the answer: the exit was unconfirmed
    expect(manager.heldPermits()).toBe(1);
    const saved = () => settledResults.filter(result => result.id === handle.id && result.status === "completed");
    expect(saved()).toHaveLength(0);
    const next = await manager.spawn({ task: "next", transport: "process" }); // releases: delivers first
    expect(saved()).toEqual([{ id: handle.id, status: "completed", text: "SAVED" }]);
    expect(manager.status(handle.id)).toMatchObject({ status: "completed", text: "SAVED" });
    expect(manager.heldPermits()).toBe(0);
    expect(next.status).not.toBe("queued");
    manager.settleLostEnodevRuns("no-actor", new Set()); // a second release attempt
    await manager.stop(next.id).catch(() => undefined);
    await manager.spawn({ task: "again", transport: "process" }).then(again => manager.stop(again.id)).catch(() => undefined);
    expect(saved()).toHaveLength(1);
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
