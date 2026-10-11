// smarty-dev#9855 part A: an unknown-outcome launch held its admission permit forever, so a long-lived Main with
// maxConcurrent such launches admitted no more spawns. A launch proven gone (ENODEV-unconfirmed exit, worker gone)
// now releases its permit in the same step as its settle, exactly once; held permits are counted and alarmed.
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";

const ENODEV = "Owned scope observation failed: Error: ENODEV: no such device, read";
const roots: string[] = [];
const managers: AgentManager[] = [];
afterEach(async () => {
  vi.restoreAllMocks();
  for (const manager of managers.splice(0)) await manager.close().catch(() => undefined);
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const deadPid = (): number => spawnSync(process.execPath, ["-e", ""]).pid!;
const onLinux = process.platform === "linux" ? it : it.skip;

const setup = (maxConcurrent = 20, onAdmissionAlarm?: (alarm: unknown) => void) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-permits-"));
  roots.push(root);
  const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent, budgetUsd: 0, retainRuns: true }, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: root, ...(onAdmissionAlarm ? { onAdmissionAlarm } : {}),
  });
  managers.push(manager);
  /** `count` launches whose outcome is unknown (the ENODEV observation failure), each with its worker's own record. */
  const unknownLaunches = async (count: number, pid: () => number) => {
    const launch = vi.spyOn(ProcessTransport.prototype, "launch")
      .mockRejectedValue(Object.assign(new Error(ENODEV), { launchOutcome: "unknown" }));
    const ids: string[] = [];
    for (let index = 0; index < count; index++) {
      const handle = await manager.spawn({ task: `unknown ${index}`, transport: "process" });
      expect(handle.status).toBe("queued"); // the unknown-launch receipt
      ids.push(handle.id);
    }
    launch.mockRestore();
    // Their workers' own records, written once all launched (the worker died, or lives, after its launch).
    for (const id of ids) fs.writeFileSync(path.join(root, id, "status.json"), JSON.stringify({
      id, status: "running", transport: "process", sessionId: String(pid()), startedAt: 1, task: "t" }));
    return ids;
  };
  return { root, manager, unknownLaunches };
};

describe("held admission permits (smarty-dev#9855)", () => {
  onLinux("20 unknown launches hold every permit; once they are proven gone, the 21st spawn is admitted", async () => {
    const { root, manager, unknownLaunches } = setup();
    const ids = await unknownLaunches(20, deadPid);
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
    const ids = await unknownLaunches(2, () => process.pid); // this live process: not proven gone
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
    await unknownLaunches(3, deadPid);
    await manager.stop((await manager.spawn({ task: "frees", transport: "process", })).id).catch(() => undefined);
    expect(manager.heldPermits()).toBe(0);
    manager.settleLostEnodevRuns("no-actor", new Set()); // another settle pass: nothing left to release
    // Capacity is exactly 3 again: three new unknown launches (live workers) are admitted, the fourth spawn waits.
    await unknownLaunches(3, () => process.pid);
    const fourth = await manager.spawn({ task: "fourth", transport: "process" });
    expect(fourth.status).toBe("queued");
    expect(fourth.heldPermits).toBe(3);
    await manager.stop(fourth.id).catch(() => undefined);
  });

  onLinux("held permits at half of maxConcurrent raise one alarm, again only after 6 h", async () => {
    const alarms: unknown[] = [];
    const { unknownLaunches } = setup(20, alarm => alarms.push(alarm));
    await unknownLaunches(9, () => process.pid);
    expect(alarms).toHaveLength(0);
    await unknownLaunches(1, () => process.pid);
    expect(alarms).toEqual([expect.objectContaining({ heldPermits: 10, maxConcurrent: 20 })]);
    await unknownLaunches(2, () => process.pid);
    expect(alarms).toHaveLength(1);
    const now = Date.now();
    vi.spyOn(Date, "now").mockReturnValue(now + 6 * 60 * 60 * 1_000 + 1);
    await unknownLaunches(1, () => process.pid);
    expect(alarms).toHaveLength(2);
    expect(alarms[1]).toMatchObject({ heldPermits: 13 });
  });
});
