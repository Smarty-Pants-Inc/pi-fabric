import childProcess from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { expect, vi } from "vitest";
import { disposeRunTmpDirectory, UNRESOLVED_SCRATCH_FILE } from "../src/storage/run-scratch.js";
import { UNSCOPED_SCRATCH_RETENTION_MS } from "../src/storage/scratch-process-census.js";
import { scratchEvidence } from "./scratch-evidence.js";

/** Observe the real allocation, not a platform assumption: Linux CI can be
 * unscoped too. Scoped runs retain their original immediate-collection checks. */
export const isUnscopedRun = (runDirectory: string): boolean => {
  const file = path.join(runDirectory, UNRESOLVED_SCRATCH_FILE);
  return fs.existsSync(file) && JSON.parse(fs.readFileSync(file, "utf8")).version === 3;
};

/** The same D4 contract as actor-preparation: native close is only a generation
 * receipt. Exercise the real scratch collector with injected scheduling time
 * and a complete parsed census; never mock liveness, boot or namespace identity.
 * Call before the original manager/worktree/save/accounting collection assertions.
 * Scratch proof does not itself authorize removal of the surrounding run tree. */
export const proveUnscopedCollection = async (runDirectory: string): Promise<void> => {
  if (!isUnscopedRun(runDirectory)) return;
  const fenceFile = path.join(runDirectory, UNRESOLVED_SCRATCH_FILE);
  await expect.poll(() => JSON.parse(fs.readFileSync(fenceFile, "utf8")).closedPid,
    { interval: 10, timeout: 5_000 }).toEqual(expect.any(Number));
  const fence = JSON.parse(fs.readFileSync(fenceFile, "utf8"));
  const status = JSON.parse(fs.readFileSync(path.join(runDirectory, "status.json"), "utf8"));
  const scratch = path.join(runDirectory, "tmp");
  expect(status).toMatchObject({ transport: "process", sessionId: String(fence.closedPid), finishedAt: expect.any(Number) });
  expect(["completed", "stopped", "failed", "timed_out"]).toContain(status.status);
  expect(fence).toMatchObject({ version: 3, runDirectory,
    root: { dev: fs.statSync(runDirectory).dev, ino: fs.statSync(runDirectory).ino },
    scratch: { dev: fs.statSync(scratch).dev, ino: fs.statSync(scratch).ino } });
  expect(fence.scope).toBeUndefined();
  expect(fence.launchNonce).toEqual(expect.any(String));
  expect(fence.launchNonce.length).toBeGreaterThan(0);
  expect(fence.lastLaunchAt).toBeGreaterThanOrEqual(fence.allocatedAt);
  expect(fence.closedAt).toBeGreaterThanOrEqual(fence.lastLaunchAt);
  expect(fence.hostEpoch.platform).toBe(process.platform);
  expect(() => process.kill(fence.closedPid, 0)).toThrow(expect.objectContaining({ code: "ESRCH" }));

  const nativeExec = childProcess.execFileSync;
  let holder = false;
  let censusCalls = 0;
  const census = vi.spyOn(childProcess, "execFileSync").mockImplementation((file, args, options) => {
    if (file !== "/bin/ps" && !(options as childProcess.ExecFileSyncOptions)?.env?.PI_FABRIC_CENSUS_PID) {
      return nativeExec(file, args, options as childProcess.ExecFileSyncOptions);
    }
    censusCalls++;
    if (process.platform === "win32") return JSON.stringify([
      { pid: process.pid, ppid: process.ppid, birth: fence.allocatedAt - 60_000, zombie: false, query: false },
      ...(holder ? [{ pid: fence.closedPid, ppid: process.pid, birth: fence.allocatedAt, zombie: false, query: false }] : []),
    ]) as never;
    const birth = (at: number) => new Date(at).toUTCString()
      .replace(/^(\w+), (\d+) (\w+) (\d+) (.*) GMT$/, "$1 $3 $2 $5 $4");
    return `${process.pid} ${process.ppid} ${process.getuid!()} S ${birth(fence.allocatedAt - 60_000)} node\n` +
      (holder ? `${fence.closedPid} ${process.pid} ${process.getuid!()} S ${birth(fence.allocatedAt)} node\n` : "") as never;
  });
  const clock = vi.spyOn(Date, "now");
  const retained = () => {
    expect(disposeRunTmpDirectory(runDirectory)).toBe(false);
    expect(fs.existsSync(runDirectory)).toBe(true);
    expect(fs.existsSync(scratch)).toBe(true);
    expect(JSON.parse(fs.readFileSync(fenceFile, "utf8"))).toEqual(fence);
  };
  try {
    retained();
    expect(censusCalls).toBe(0);
    const ageGate = Math.max(fence.lastLaunchAt, fence.closedAt, status.finishedAt) + UNSCOPED_SCRATCH_RETENTION_MS;
    clock.mockReturnValue(ageGate - 1);
    retained();
    expect(censusCalls).toBe(0);
    clock.mockReturnValue(ageGate + 1);
    holder = true;
    retained();
    expect(censusCalls).toBeGreaterThan(0);
    holder = false;
    censusCalls = 0;
    expect(disposeRunTmpDirectory(runDirectory)).toBe(true);
    expect(censusCalls).toBeGreaterThanOrEqual(2);
    expect(fs.existsSync(scratch)).toBe(false);
    expect(fs.existsSync(fenceFile)).toBe(false);
    // Manager-specific persistence, pending deliveries and worktree gates remain.
    expect(fs.existsSync(runDirectory)).toBe(true);
    scratchEvidence(`collection-contract-${path.basename(runDirectory)}`, { containment: "unscoped", fence,
      runRetainedImmediately: true, retainedBelowAgeGate: true, retainedWithPotentialHolder: true,
      nativeCloseReceiptRetained: true, workerPidConfirmedAbsent: true, scratchRemovedAfterAgeAndEmptyCensus: true,
      finalCensusCalls: censusCalls, retentionClock: "injected Date.now past 24-hour gate, not elapsed real time",
      census: "complete parsed census fixture; native worker liveness, boot identity and pinned namespace unchanged" });
  } finally { clock.mockRestore(); census.mockRestore(); }
};
