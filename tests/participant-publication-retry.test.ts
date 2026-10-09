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
const setup = async (wait: () => Promise<void>) => {
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
    withPublicationFence: publicationFence, waitForPublicationRetry: admission, reapDeadHosts: false });
  directories.push(directory); await directory.start();
  vi.useFakeTimers(); vi.spyOn(console, "warn").mockImplementation(() => {});
  return { directory, fence, admission, block: (value: boolean) => { blocked = value; } };
};

it("uses one off-heartbeat jittered retry lane capped at 2 s, and admits only a fresh commit", async () => {
  let tries = 0, inline = true;
  const s = await setup(async () => { if (inline || ++tries < 4) throw timeout(); s.block(false); });
  vi.spyOn(Math, "random").mockReturnValue(0.999);
  s.block(true); const prior = s.directory.confirmedAt();
  const failed = expect(s.directory.refresh()).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
  await vi.advanceTimersByTimeAsync(1_800); await failed; // spend the four-try heartbeat budget first
  expect(s.admission).toHaveBeenCalledTimes(3); inline = false; s.admission.mockClear();
  for (let actor = 0; actor < 40; actor++) {
    s.directory.scheduleRefresh(); await s.directory.refreshPresence(); expect(s.directory.canConsumeMesh()).toBe(false);
  }
  await vi.advanceTimersByTimeAsync(748); expect(s.admission).not.toHaveBeenCalled();
  await vi.advanceTimersByTimeAsync(1); expect(s.admission).toHaveBeenCalledOnce();
  expect(s.directory.confirmedAt()).toBe(prior); expect(s.directory.canConsumeMesh()).toBe(false);
  await vi.advanceTimersByTimeAsync(1_498); expect(s.admission).toHaveBeenCalledTimes(2);
  await vi.advanceTimersByTimeAsync(1_998); expect(s.admission).toHaveBeenCalledTimes(3);
  // Cross the natural 5 s heartbeat: it must not duplicate the active recovery lane.
  await vi.advanceTimersByTimeAsync(1_998); expect(s.admission).toHaveBeenCalledTimes(4);
  expect(s.fence).toHaveBeenCalledTimes(3); // initial, failed try, one fresh recovery commit
  expect(s.directory.confirmedAt()).toBeGreaterThan(prior); expect(s.directory.canConsumeMesh()).toBe(true);
});

it.each(["close", "quiesce"] as const)("%s drains the outside-custody wait and cancels its retry timer", async operation => {
  let release!: () => void;
  const gate = new Promise<void>(resolve => { release = resolve; });
  let inline = true;
  const s = await setup(() => inline ? Promise.reject(timeout()) : gate);
  try {
    vi.spyOn(Math, "random").mockReturnValue(0);
    s.block(true);
    const failed = expect(s.directory.refresh()).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    await vi.advanceTimersByTimeAsync(450); await failed;
    expect(s.admission).toHaveBeenCalledTimes(3); inline = false; s.admission.mockClear();
    await vi.advanceTimersByTimeAsync(50); expect(s.admission).toHaveBeenCalledOnce();
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
