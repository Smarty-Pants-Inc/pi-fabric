import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, expect, it, vi } from "vitest";
import { MeshLockTimeoutError } from "../src/core/atomic-write.js";
import { MeshStore } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { readHostLeases } from "../src/topology/host-leases.js";

const roots: string[] = [], directories: ParticipantDirectory[] = [];
afterEach(async () => {
  await Promise.all(directories.splice(0).map(directory => directory.close()));
  vi.useRealTimers(); vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const timeout = () => new MeshLockTimeoutError(" held by test", 1, 0);
const setup = async (wait: () => Promise<void>, options: { heartbeatMs?: number; leaseMs?: number } = {}) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "publication-retry-")); roots.push(root);
  const mesh = new MeshStore(root, 65_536, 100);
  let blocked = false, inFence = false;
  const fence = vi.fn();
  const publicationFence = async <T>(publish: () => Promise<T>): Promise<T> => {
    fence();
    inFence = true;
    try { if (blocked) throw timeout(); return await publish(); }
    finally { inFence = false; }
  };
  const admission = vi.fn(async () => { expect(inFence).toBe(false); await wait(); });
  const identity = { id: "resident", name: "resident", kind: "agent" as const };
  const directory = new ParticipantDirectory(mesh, { enabled: true, hostId: identity.id, rootId: "root", identity,
    withPublicationFence: publicationFence, waitForPublicationRetry: admission, reapDeadHosts: false, ...options });
  directories.push(directory); await directory.start();
  vi.useFakeTimers(); vi.spyOn(console, "warn").mockImplementation(() => {});
  return { root, identity, directory, fence, admission, block: (value: boolean) => { blocked = value; } };
};

it("uses one off-heartbeat full-jitter lane and admits only a fresh commit", async () => {
  let tries = 0;
  const s = await setup(async () => { if (++tries < 5) throw timeout(); s.block(false); });
  vi.spyOn(Math, "random").mockReturnValue(0.999);
  s.block(true); const prior = s.directory.confirmedAt();
  await expect(s.directory.refresh()).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
  for (let actor = 0; actor < 40; actor++) {
    s.directory.scheduleRefresh(); await s.directory.refreshPresence(); expect(s.directory.canConsumeMesh()).toBe(false);
  }
  // Cross the natural 5 s heartbeat: it must not duplicate the active recovery lane.
  for (const [index, wait] of [249, 499, 999, 1_998, 3_996].entries()) {
    await vi.advanceTimersByTimeAsync(wait - 1); expect(s.admission).toHaveBeenCalledTimes(index);
    await vi.advanceTimersByTimeAsync(1); expect(s.admission).toHaveBeenCalledTimes(index + 1);
    if (index < 4) {
      expect(s.directory.confirmedAt()).toBe(prior); expect(s.directory.canConsumeMesh()).toBe(false);
    }
  }
  expect(s.fence).toHaveBeenCalledTimes(3); // initial, failed try, one fresh recovery commit
  expect(s.directory.confirmedAt()).toBeGreaterThan(prior); expect(s.directory.canConsumeMesh()).toBe(true);
});

it("clamps host recovery to the last written lease minus the 250 ms margin", async () => {
  const attempts: number[] = [];
  const s = await setup(async () => {
    attempts.push(Date.now());
    if (attempts.length < 3) throw timeout();
    s.block(false);
  }, { heartbeatMs: 100, leaseMs: 1_000 });
  vi.spyOn(Math, "random").mockReturnValue(0.999);
  s.block(true);
  await expect(s.directory.refresh()).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
  const expiry = readHostLeases(s.root).get(s.identity.id)!.expiresAt;
  await vi.advanceTimersByTimeAsync(249 + 499);
  expect(attempts).toHaveLength(2);
  const remaining = expiry - Date.now() - 250;
  expect(remaining).toBeGreaterThan(0); expect(remaining).toBeLessThan(999);
  await vi.advanceTimersByTimeAsync(remaining - 1); expect(attempts).toHaveLength(2);
  await vi.advanceTimersByTimeAsync(1); expect(attempts).toHaveLength(3);
  expect(attempts[2]).toBe(expiry - 250);
  expect(s.directory.canConsumeMesh()).toBe(true);
});

it("does not spin in the margin after its one deadline acquisition still times out", async () => {
  let attempts = 0;
  const s = await setup(async () => { attempts++; throw timeout(); }, { heartbeatMs: 100, leaseMs: 1_000 });
  vi.spyOn(Math, "random").mockReturnValue(0.999); s.block(true);
  await expect(s.directory.refresh()).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
  const expiry = readHostLeases(s.root).get(s.identity.id)!.expiresAt;
  await vi.advanceTimersByTimeAsync(expiry - Date.now() - 250);
  expect(attempts).toBe(3);
  await vi.advanceTimersByTimeAsync(200);
  expect(attempts).toBe(3); // not 200 one-millisecond retries against the same failed lease
  expect(s.directory.canConsumeMesh()).toBe(false);
});

it.each(["close", "quiesce"] as const)("%s drains the outside-custody wait and cancels its retry timer", async operation => {
  let release!: () => void;
  const gate = new Promise<void>(resolve => { release = resolve; });
  const s = await setup(() => gate);
  try {
    vi.spyOn(Math, "random").mockReturnValue(0);
    s.block(true);
    await expect(s.directory.refresh()).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    await vi.advanceTimersByTimeAsync(1); expect(s.admission).toHaveBeenCalledOnce();
    let finished = false;
    const stopping = s.directory[operation]().then(() => { finished = true; });
    await vi.advanceTimersByTimeAsync(10_000);
    expect(finished).toBe(false); expect(s.admission).toHaveBeenCalledOnce();
    s.block(false); release(); await stopping;
    const fences = s.fence.mock.calls.length;
    await vi.advanceTimersByTimeAsync(750);
    expect(s.admission).toHaveBeenCalledOnce(); expect(s.fence).toHaveBeenCalledTimes(fences);
  } finally { release(); }
});
