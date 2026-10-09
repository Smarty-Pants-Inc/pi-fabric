import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, expect, it, vi } from "vitest";
import { MeshLockTimeoutError } from "../src/core/atomic-write.js";
import { MeshStore } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";

const roots: string[] = [], directories: ParticipantDirectory[] = [];
afterEach(async () => {
  await Promise.all(directories.splice(0).map(directory => directory.close()));
  vi.useRealTimers(); vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const timeout = () => new MeshLockTimeoutError(" held by test", 1, 0);
const setup = async (wait: (signal: AbortSignal) => Promise<void>) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "publication-retry-")); roots.push(root);
  const mesh = new MeshStore(root, 65_536, 100);
  let blocked = false, inFence = false;
  const fence = vi.fn();
  const publicationFence = async <T>(publish: () => Promise<T>): Promise<T> => {
    fence(); inFence = true;
    try { if (blocked) throw timeout(); return await publish(); }
    finally { inFence = false; }
  };
  const admission = vi.fn(async (signal?: AbortSignal) => { expect(inFence).toBe(false); await wait(signal!); });
  const identity = { id: "resident", name: "resident", kind: "agent" as const };
  const directory = new ParticipantDirectory(mesh, { enabled: true, hostId: identity.id, rootId: "root", identity,
    preparePublicationFence: () => () => true,
    withPublicationFence: publicationFence, waitForPublicationRetry: admission, reapDeadHosts: false });
  directories.push(directory);
  vi.useFakeTimers(); await directory.start();
  vi.spyOn(console, "warn").mockImplementation(() => {});
  return { directory, fence, admission, block: (value: boolean) => { blocked = value; } };
};

it("requeues one host on event admission without timer backoff or duplicate mutation retries", async () => {
  let release!: () => void;
  const gate = new Promise<void>(resolve => { release = resolve; });
  const s = await setup(() => gate);
  const timers = vi.spyOn(globalThis, "setTimeout");
  s.block(true); const prior = s.directory.confirmedAt();
  await expect(s.directory.refresh()).rejects.toBeInstanceOf(MeshLockTimeoutError);
  await vi.advanceTimersByTimeAsync(0);
  expect(s.admission).toHaveBeenCalledOnce();
  expect(timers).toHaveBeenCalledOnce(); // only admission-budget cancellation
  expect(timers.mock.calls[0]![1]).toBe(13_000);
  for (let actor = 0; actor < 40; actor++) {
    s.directory.scheduleRefresh(); await s.directory.refreshPresence();
    expect(s.directory.canConsumeMesh()).toBe(false);
  }
  await vi.advanceTimersByTimeAsync(5_000); // existing heartbeat cannot duplicate the FIFO ticket
  expect(s.admission).toHaveBeenCalledOnce(); expect(timers).toHaveBeenCalledOnce();
  expect(s.directory.confirmedAt()).toBe(prior);
  s.block(false); release(); await vi.advanceTimersByTimeAsync(0);
  expect(s.fence).toHaveBeenCalledTimes(3); // initial, short try, fresh recovery
  expect(s.directory.confirmedAt()).toBeGreaterThan(prior); expect(s.directory.canConsumeMesh()).toBe(true);
});

it("a timed-out admission retries only at the next existing heartbeat tick", async () => {
  const s = await setup(async () => { throw timeout(); });
  const timers = vi.spyOn(globalThis, "setTimeout");
  s.block(true);
  await expect(s.directory.refresh()).rejects.toBeInstanceOf(MeshLockTimeoutError);
  await vi.advanceTimersByTimeAsync(0);
  expect(s.admission).toHaveBeenCalledOnce(); expect(timers).toHaveBeenCalledOnce();
  await vi.advanceTimersByTimeAsync(4_999);
  expect(s.admission).toHaveBeenCalledOnce();
  await vi.advanceTimersByTimeAsync(1);
  expect(s.admission).toHaveBeenCalledTimes(2); expect(timers).toHaveBeenCalledTimes(2);
  expect(s.fence).toHaveBeenCalledTimes(3);
});

it("keeps the background admission budget monotonic through wall-clock rollback and independent renewal", async () => {
  let release!: () => void;
  const gate = new Promise<void>(resolve => { release = resolve; });
  const s = await setup(() => gate);
  const timers = vi.spyOn(globalThis, "setTimeout");
  s.block(true);
  await expect(s.directory.refresh()).rejects.toBeInstanceOf(MeshLockTimeoutError);
  await vi.advanceTimersByTimeAsync(0);
  const signal = s.admission.mock.calls[0]![0]!;
  await vi.advanceTimersByTimeAsync(5_000);
  vi.setSystemTime(Date.now() - 60_000);
  await vi.advanceTimersByTimeAsync(7_999); expect(signal.aborted).toBe(false);
  await vi.advanceTimersByTimeAsync(1); expect(signal.aborted).toBe(true);
  expect(timers).toHaveBeenCalledOnce(); expect(s.admission).toHaveBeenCalledOnce();
  s.block(false); release(); await vi.advanceTimersByTimeAsync(0);
  expect(s.directory.canConsumeMesh()).toBe(false); // late admission is no commit
});

it.each(["close", "quiesce"] as const)("%s promptly aborts outside-custody event admission", async operation => {
  let release!: () => void;
  const gate = new Promise<void>(resolve => { release = resolve; });
  const s = await setup(() => gate);
  try {
    s.block(true);
    await expect(s.directory.refresh()).rejects.toBeInstanceOf(MeshLockTimeoutError);
    await vi.advanceTimersByTimeAsync(0); expect(s.admission).toHaveBeenCalledOnce();
    const signal = s.admission.mock.calls[0]![0]!;
    s.block(false);
    let finished = false;
    const stopping = s.directory[operation]().then(() => { finished = true; });
    await vi.advanceTimersByTimeAsync(0);
    expect(finished).toBe(true); expect(signal.aborted).toBe(true); await stopping;
    const fences = s.fence.mock.calls.length;
    await vi.advanceTimersByTimeAsync(750);
    expect(s.admission).toHaveBeenCalledOnce(); expect(s.fence).toHaveBeenCalledTimes(fences);
  } finally { release(); }
});
