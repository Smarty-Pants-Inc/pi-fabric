import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshBackgroundRetry, MeshLockTimeoutError } from "../src/core/atomic-write.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { withStateFence } from "../src/mesh/commit-outbox.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { readHostLease } from "../src/topology/host-leases.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";

const roots: string[] = [], directories: ParticipantDirectory[] = [];
afterEach(async () => {
  vi.restoreAllMocks();
  for (const root of roots) fs.rmSync(path.join(root, ".lock"), { recursive: true, force: true });
  await Promise.all(directories.splice(0).map(directory => directory.close()));
  vi.useRealTimers();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const timeout = () => new MeshLockTimeoutError(" held by heartbeat test", 1, 0);
const setup = async (fenced = true, admission = true, lockTimeoutMs = 20_000) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "heartbeat-tolerance-")); roots.push(root);
  const identity: MeshIdentity = { id: "session:live", name: "main", kind: "main", sessionId: "live" };
  const mesh = new MeshStore(root, 65_536, 100, { lockTimeoutMs });
  let inFence = false;
  const fenceBudgets: Array<number | undefined> = [];
  const fence = vi.fn();
  const publicationFence = async <T>(publish: () => Promise<T>): Promise<T> => {
    fence(); inFence = true;
    try { return await mesh.withTryLock(() => { fenceBudgets.push(mesh.tryLockBudgetMs); return publish(); }, 0); }
    finally { inFence = false; }
  };
  const wait = vi.fn(async () => {
    expect(inFence).toBe(false);
    await withStateFence(mesh, identity, () => undefined);
  });
  const directory = new ParticipantDirectory(mesh, {
    enabled: true, identity, hostId: identity.id, rootId: identity.id,
    heartbeatMs: 5_000, leaseMs: 15_000, reapDeadHosts: false,
    ...(fenced ? { withPublicationFence: publicationFence } : {}),
    ...(fenced && admission ? { waitForPublicationRetry: wait } : {}),
  });
  const record: FabricParticipantRecord = {
    format: 1, id: identity.id, rootId: identity.id, ownerHostId: identity.id, ownerIdentityId: identity.id,
    kind: "root", name: "main", label: "TEST-1", status: "idle", runner: "pi", transport: "host",
    capabilities: ["steer", "followUp", "fabric"], sessionId: "live", cwd: root,
    startedAt: Date.now(), updatedAt: Date.now(), pendingMessages: false, controlProtocol: "v1",
  };
  const source = vi.fn(() => [{ ...record, updatedAt: Date.now() }]);
  directory.registerSource(source); directories.push(directory);
  await directory.refresh();
  const lockPath = path.join(root, ".lock");
  const hold = () => {
    fs.mkdirSync(lockPath, { mode: 0o700 });
    fs.writeFileSync(path.join(lockPath, "owner"), `held\n${process.pid}\n${Date.now()}\n`);
  };
  const release = () => fs.rmSync(lockPath, { recursive: true, force: true });
  const lease = () => readHostLease(root, identity.id)!;
  return { root, mesh, directory, source, record, fence, fenceBudgets, wait, hold, release, lease };
};

describe("HOT heartbeat tolerance (#6729 / #7176)", () => {
  it("keeps leases and counts zero heartbeat timeouts under 30–80 ms locks at 50% duty for 10 s", async () => {
    const s = await setup();
    const runner = new MeshBackgroundRetry("heartbeat tolerance test");
    const failure = vi.spyOn(runner, "failure");
    vi.useFakeTimers(); vi.spyOn(Math, "random").mockReturnValue(0); // minimum 150 ms jitter
    const start = Date.now();
    let stop = false, rounds = 0, lostLeases = 0;
    const holder = (async () => {
      while (!stop) {
        const width = [30, 55, 80][rounds++ % 3]!;
        s.hold(); await new Promise(resolve => setTimeout(resolve, width)); s.release();
        await new Promise(resolve => setTimeout(resolve, width));
      }
    })();
    const beats = (async () => {
      while (Date.now() - start < 10_000) {
        expect(await runner.run(() => s.directory.refresh(), false)).toBe("done");
        await new Promise(resolve => setTimeout(resolve, 200));
      }
    })();
    const observing = setInterval(() => {
      const lease = s.lease();
      if (!lease || lease.expiresAt < Date.now() || !lease.session || lease.session.expiresAt < Date.now()) lostLeases++;
    }, 5);
    try {
      await vi.advanceTimersByTimeAsync(10_500); await beats;
      stop = true; await vi.advanceTimersByTimeAsync(160); await holder;
      expect(rounds).toBeGreaterThan(80);
      expect(s.wait).toHaveBeenCalled(); // actual typed short tries exercised
      expect(lostLeases).toBe(0); expect(failure).not.toHaveBeenCalled();
      expect(s.directory.writeStalled()).toBeUndefined();
    } finally { stop = true; clearInterval(observing); await vi.advanceTimersByTimeAsync(160); await holder; }
  });

  it.each([false, true])("stops at the frozen TTL minus 2 s, reports one whole-budget timeout and stays fail closed (fenced: %s)", async fenced => {
    const s = await setup(fenced);
    vi.useFakeTimers(); vi.spyOn(Math, "random").mockReturnValue(0);
    const runner = new MeshBackgroundRetry("held past TTL");
    const failure = vi.spyOn(runner, "failure");
    vi.spyOn(console, "warn").mockImplementation(() => {});
    const prior = s.directory.confirmedAt(), at = Date.now();
    s.hold(); const receipt = fs.readFileSync(path.join(s.root, ".lock/owner"), "utf8");
    let spentAt = 0;
    const work = runner.run(() => s.directory.refresh(), false).then(result => { spentAt = Date.now(); return result; });
    await vi.advanceTimersByTimeAsync(12_999);
    expect(failure).not.toHaveBeenCalled();
    expect(s.directory.confirmedAt()).toBe(prior); expect(s.directory.canConsumeMesh()).toBe(false);
    await vi.advanceTimersByTimeAsync(7_001); // same live holder remains past the 15 s TTL
    expect(await work).toBe("retry"); expect(failure).toHaveBeenCalledOnce();
    expect(failure.mock.calls[0]![0]).toBeInstanceOf(MeshLockTimeoutError);
    expect(spentAt - at).toBe(13_000);
    expect(s.lease().expiresAt).toBeGreaterThan(at + 15_000); // renewal did not slide retryUntil
    expect(s.directory.confirmedAt()).toBe(prior); expect(s.directory.canConsumeMesh()).toBe(false);
    expect(s.directory.writeStalled()?.message).toContain("peer visibility is unknown, not empty");
    expect(fs.readFileSync(path.join(s.root, ".lock/owner"), "utf8")).toBe(receipt);
    s.release(); await s.directory.refresh();
    expect(s.directory.writeStalled()).toBeUndefined(); expect(s.directory.canConsumeMesh()).toBe(true);
  });

  it("bounds typed retries to four tries with 150–600 ms jitter and one counted failure", async () => {
    const s = await setup(true, false);
    vi.useFakeTimers(); vi.spyOn(Math, "random").mockReturnValue(0.999999);
    const confirm = vi.spyOn(s.mesh, "confirmWritable").mockRejectedValue(timeout());
    const runner = new MeshBackgroundRetry("four tries");
    const failure = vi.spyOn(runner, "failure"); vi.spyOn(console, "warn").mockImplementation(() => {});
    const work = runner.run(() => s.directory.refresh(), false);
    await vi.advanceTimersByTimeAsync(599); expect(confirm).toHaveBeenCalledOnce(); expect(failure).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1); expect(confirm).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(1_200); expect(await work).toBe("retry");
    expect(confirm).toHaveBeenCalledTimes(4); expect(failure).toHaveBeenCalledOnce();
  });

  it("reselects sources after jitter/admission without widening registry tries", async () => {
    const s = await setup();
    vi.useFakeTimers(); vi.spyOn(Math, "random").mockReturnValue(0);
    const original = s.mesh.confirmWritable.bind(s.mesh);
    const confirm = vi.spyOn(s.mesh, "confirmWritable").mockRejectedValueOnce(timeout()).mockImplementation(original);
    const work = s.directory.refresh();
    await vi.advanceTimersByTimeAsync(0);
    const reads = s.source.mock.calls.length;
    s.record.status = "running";
    await vi.advanceTimersByTimeAsync(150); await work;
    expect(s.wait).toHaveBeenCalledOnce(); expect(s.source.mock.calls.length).toBeGreaterThan(reads);
    expect(s.directory.get(s.record.id)?.status).toBe("running");
    expect(confirm).toHaveBeenCalled();
    expect(s.fenceBudgets).toEqual([0, 0, 0]); // initial, failed heartbeat, fresh publication; no inherited wider scope
  });

  it("does not retry an untyped lookalike error, or initial admission", async () => {
    const s = await setup();
    const error = new Error("Timed out waiting for the Fabric mesh lock: unrelated operation");
    const confirm = vi.spyOn(s.mesh, "confirmWritable").mockRejectedValue(error);
    await expect(s.directory.refresh()).rejects.toBe(error);
    expect(confirm).toHaveBeenCalledOnce(); expect(s.wait).not.toHaveBeenCalled();
    const newcomer = new ParticipantDirectory(s.mesh, { enabled: true, identity: { id: "new", kind: "agent", name: "new" },
      rootId: "new", hostId: "new", withPublicationFence: async () => { throw timeout(); }, reapDeadHosts: false });
    directories.push(newcomer);
    await expect(newcomer.refresh()).rejects.toBeInstanceOf(MeshLockTimeoutError);
    expect(readHostLease(s.root, "new")).toBeUndefined(); expect(newcomer.canConsumeMesh()).toBe(false);
  });

  it("renews a coalesced heartbeat independently and never shortens matching held host/session leases", async () => {
    const s = await setup();
    vi.useFakeTimers();
    const before = s.lease(), prior = s.directory.confirmedAt();
    vi.setSystemTime(Date.now() - 1_000); // a backward clock step must not shorten either expiry
    const confirm = vi.spyOn(s.mesh, "confirmWritable").mockRejectedValue(new Error("ENOSPC"));
    await expect(s.directory.refresh()).rejects.toThrow("ENOSPC");
    expect(s.lease().expiresAt).toBeGreaterThanOrEqual(before.expiresAt);
    expect(s.lease().session!.expiresAt).toBeGreaterThanOrEqual(before.session!.expiresAt);
    expect(s.lease().startedAt).toBe(before.startedAt);
    let acquired!: (at: number) => void, release!: () => void;
    confirm.mockImplementation(callback => { acquired = callback!; return new Promise<void>(resolve => { release = resolve; }); });
    const first = s.directory.refresh(); await vi.advanceTimersByTimeAsync(0);
    await vi.advanceTimersByTimeAsync(14_000);
    const joining = s.directory.refresh();
    expect(s.lease().expiresAt).toBe(Date.now() + 15_000);
    expect(s.directory.confirmedAt()).toBe(prior); expect(s.directory.canConsumeMesh()).toBe(false);
    expect(confirm).toHaveBeenCalledTimes(2);
    acquired(Date.now()); release(); await Promise.all([first, joining]);
    expect(s.directory.canConsumeMesh()).toBe(true);
  });
});
