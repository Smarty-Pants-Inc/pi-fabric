import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, expect, it, vi } from "vitest";
import { MeshLockTimeoutError } from "../src/core/atomic-write.js";
import { withStateFence } from "../src/mesh/commit-outbox.js";
import { MeshStore } from "../src/mesh/store.js";
import { hostLeasePath, readHostLeaseCurrent, writeHostLease } from "../src/topology/host-leases.js";
import { ParticipantDirectory, ParticipantLeaseSupersededError } from "../src/topology/participant-directory.js";

const roots: string[] = [], directories: ParticipantDirectory[] = [];
afterEach(async () => {
  await Promise.all(directories.splice(0).map(directory => directory.close()));
  vi.useRealTimers(); vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const setup = async (wait: (signal: AbortSignal) => Promise<void>, initialBlocked = false) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "prepared-recovery-")); roots.push(root);
  const mesh = new MeshStore(root, 65_536, 100);
  const identity = { id: "resident:lineage", name: "resident", kind: "agent" as const };
  let blocked = initialBlocked, inFence = false;
  const fence = vi.fn();
  const directory = new ParticipantDirectory(mesh, {
    enabled: true, hostId: identity.id, rootId: "session:lineage", identity, reapDeadHosts: false,
    preparePublicationFence: () => () => true,
    withPublicationFence: async publish => {
      fence(); inFence = true;
      try {
        if (blocked) throw new MeshLockTimeoutError(" held by prepared recovery test", 1, 0);
        return await mesh.withTryLock(publish, 50);
      } finally { inFence = false; }
    },
    waitForPublicationRetry: vi.fn(async signal => { expect(inFence).toBe(false); await wait(signal!); }),
  });
  directories.push(directory);
  // Install fake timers before a failed activation schedules its recovery timer.
  vi.useFakeTimers(); vi.spyOn(Math, "random").mockReturnValue(0);
  vi.spyOn(console, "warn").mockImplementation(() => {});
  if (initialBlocked) await expect(directory.start()).rejects.toBeInstanceOf(MeshLockTimeoutError);
  else await directory.start();
  const lease = () => readHostLeaseCurrent(root, identity.id)!;
  return { root, mesh, identity, directory, fence, lease, block: (value: boolean) => { blocked = value; } };
};

it("recovers prepared cold admission without creating a lease before the shared commit", async () => {
  const s = await setup(async () => { s.block(false); }, true);
  expect(readHostLeaseCurrent(s.root, s.identity.id)).toBeUndefined();
  expect(s.directory.canConsumeMesh()).toBe(false);
  await vi.advanceTimersByTimeAsync(50);
  expect(s.directory.options.waitForPublicationRetry).toHaveBeenCalledOnce();
  expect(s.directory.canConsumeMesh()).toBe(true);
  expect(s.lease().identityId).toBe(s.identity.id); expect(s.lease().rootId).toBe("session:lineage");
});

it("reports the prepared short-try failure, then holds one outside-custody admission beyond 2 s", async () => {
  let release!: () => void;
  const gate = new Promise<void>(resolve => { release = resolve; });
  const s = await setup(() => gate);
  const prior = s.directory.confirmedAt(), incarnation = s.lease().startedAt;
  try {
    s.block(true);
    await expect(s.directory.refresh()).rejects.toBeInstanceOf(MeshLockTimeoutError);
    expect(s.directory.options.waitForPublicationRetry).not.toHaveBeenCalled();
    expect(s.directory.confirmedAt()).toBe(prior); expect(s.directory.canConsumeMesh()).toBe(false);
    await vi.advanceTimersByTimeAsync(50);
    await vi.advanceTimersByTimeAsync(2_500);
    expect(s.directory.options.waitForPublicationRetry).toHaveBeenCalledOnce();
    expect(s.fence).toHaveBeenCalledTimes(2); // activation and the failed publication only
    expect(s.lease().identityId).toBe(s.identity.id); expect(s.lease().rootId).toBe("session:lineage");
    expect(s.lease().startedAt).toBe(incarnation);
    s.block(false); release(); await vi.advanceTimersByTimeAsync(0);
    expect(s.directory.confirmedAt()).toBeGreaterThan(prior); expect(s.directory.canConsumeMesh()).toBe(true);
    expect(s.lease().startedAt).toBe(incarnation); expect(s.fence).toHaveBeenCalledTimes(3);
  } finally { release(); }
});

it("refuses a successor seen after prepared background admission without renewing or deleting its lease", async () => {
  let release!: () => void;
  const gate = new Promise<void>(resolve => { release = resolve; });
  const s = await setup(() => gate);
  try {
    s.block(true); await expect(s.directory.refresh()).rejects.toBeInstanceOf(MeshLockTimeoutError);
    await vi.advanceTimersByTimeAsync(50);
    const successor = { ...s.lease(), startedAt: s.lease().startedAt! + 1 };
    writeHostLease(s.root, successor);
    const bytes = fs.readFileSync(hostLeasePath(s.root, successor.id), "utf8");
    s.block(false); release(); await vi.advanceTimersByTimeAsync(0);
    expect(s.fence).toHaveBeenCalledTimes(2); expect(s.directory.canConsumeMesh()).toBe(false);
    await expect(s.directory.refresh()).rejects.toBeInstanceOf(ParticipantLeaseSupersededError);
    await s.directory.closeLineage();
    expect(fs.readFileSync(hostLeasePath(s.root, successor.id), "utf8")).toBe(bytes);
    expect(s.mesh.listAll("topology/lineage-closures/")).toEqual([]);
  } finally { release(); }
});

it.each(["close", "quiesce"] as const)("%s cancels a prepared native FIFO ticket without awaiting or disturbing the holder", async operation => {
  const s = await setup(() => withStateFence(s.mesh, s.identity, () => undefined));
  const lock = path.join(s.root, ".lock");
  fs.mkdirSync(lock); const bytes = `held\n${process.pid}\n${Date.now()}\n`;
  fs.writeFileSync(path.join(lock, "owner"), bytes);
  try {
    s.block(true); await expect(s.directory.refresh()).rejects.toBeInstanceOf(MeshLockTimeoutError);
    await vi.advanceTimersByTimeAsync(50);
    expect(s.directory.options.waitForPublicationRetry).toHaveBeenCalledOnce();
    const signal = vi.mocked(s.directory.options.waitForPublicationRetry!).mock.calls[0]![0]!;
    // Actor stop's presence publisher must not join the host's blocked recovery lane.
    let presenceFinished = false;
    const presence = s.directory.refreshPresence().then(() => { presenceFinished = true; });
    await vi.advanceTimersByTimeAsync(0); expect(presenceFinished).toBe(true); await presence;
    expect(signal.aborted).toBe(false); expect(s.fence).toHaveBeenCalledTimes(2);
    vi.spyOn(s.mesh, "delete").mockResolvedValue({ deleted: false });
    let finished = false;
    const stopping = (operation === "close" ? s.directory.close() :
      // Quiesce still reports its original short publication failure, not the retry budget.
      s.directory.quiesce().catch(error => { expect(error).toBeInstanceOf(MeshLockTimeoutError); }))
      .then(() => { finished = true; });
    await vi.advanceTimersByTimeAsync(0); expect(finished).toBe(true); await stopping;
    expect(signal.aborted).toBe(true); expect(s.directory.canConsumeMesh()).toBe(false);
    expect(fs.readFileSync(path.join(lock, "owner"), "utf8")).toBe(bytes);
    expect(s.fence).toHaveBeenCalledTimes(operation === "close" ? 2 : 3);
  } finally { fs.rmSync(lock, { recursive: true, force: true }); }
});
