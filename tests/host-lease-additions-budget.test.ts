import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { afterEach, describe, expect, it } from "vitest";
import { MeshLockTimeoutError, readPhysicalHostIdentity } from "../src/core/atomic-write.js";
import { HOST_LEASE_ADDITIONS_WAIT_MS, HostLeaseLockBusyError, withHostLeaseLock, type HostLeaseMesh } from "../src/topology/host-lease-lock.js";
import { hostLeasePath } from "../src/topology/host-leases.js";

// smarty-dev#13645 audit N1, notes 1 and 2: a native lease creator's wait for the shared additions gate.
const roots: string[] = [];
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
const sleep = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));

/** A contract fake: the additions gate is entered after `enterAfterMs` on the first call (never, if undefined), and
 * is busy for the whole wait any other call is given. Per-host gates enter at once. The per-host receipt is held by a
 * live process on this host (this one), so it is never recovered: the creator can only wait for its removal. */
const fixture = (enterAfterMs: number | undefined) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "lease-additions-budget-"));
  roots.push(root);
  const file = hostLeasePath(root, "session:budget-00000000");
  const lock = path.join(root, "host-lease-locks", path.basename(file, ".json"));
  fs.mkdirSync(lock, { recursive: true });
  const physical = readPhysicalHostIdentity();
  fs.writeFileSync(path.join(lock, "owner"), JSON.stringify({ token: randomUUID(), host: os.hostname(), pid: process.pid,
    ...(physical ? { physical } : {}) }));
  const waits: number[] = [];
  const mesh: HostLeaseMesh = {
    root,
    leaseCustody: async (gate, operation, timeoutMs = 0) => {
      if (path.basename(gate) !== ".additions") return operation();
      waits.push(Math.round(timeoutMs));
      if (waits.length === 1 && enterAfterMs !== undefined) { await sleep(enterAfterMs); return operation(); }
      await sleep(timeoutMs);
      throw new MeshLockTimeoutError(` (custody lock ${gate})`, 1, 0);
    },
  };
  return { mesh, file, waits };
};

describe("a lease creator's additions wait (smarty-dev#13645 audit N1)", () => {
  it("note 1: a later attempt's additions wait is bounded by what is left of the caller's budget", async () => {
    const { mesh, file, waits } = fixture(300);
    const started = performance.now();
    await expect(withHostLeaseLock(mesh, file, () => 1, { timeoutMs: 1_000 })).rejects.toBeInstanceOf(HostLeaseLockBusyError);
    const elapsedMs = performance.now() - started;
    // 347f75c30: waits=[1000, 1000], elapsed about 2 000 ms for a 1 000 ms budget.
    expect(waits).toHaveLength(2);
    expect(waits[0]).toBeGreaterThan(900);
    expect(waits[1]).toBeLessThanOrEqual(50);
    expect(elapsedMs).toBeLessThan(1_500);
  });

  it("note 1: without a caller budget the wait keeps its own bound (RC9)", async () => {
    const { mesh, file, waits } = fixture(0);
    await expect(withHostLeaseLock(mesh, file, () => 1)).rejects.toBeInstanceOf(HostLeaseLockBusyError);
    expect(waits).toEqual([HOST_LEASE_ADDITIONS_WAIT_MS]);
  });

  it("note 2: an abort during a timed-out additions wait is observed when it ends, not reported as busy", async () => {
    // The additions gate stays busy: the abort lands during the (signal-less) additions wait itself.
    const { mesh, file, waits } = fixture(undefined);
    const controller = new AbortController();
    const reason = new Error("caller aborted");
    const outcome = withHostLeaseLock(mesh, file, () => 1, { timeoutMs: 600, signal: controller.signal })
      .then(() => "resolved", (error: unknown) => error);
    await sleep(50);
    controller.abort(reason);
    // 347f75c30: HostLeaseLockBusyError.
    expect(await outcome).toBe(reason);
    expect(waits).toEqual([600]);
  });
});
