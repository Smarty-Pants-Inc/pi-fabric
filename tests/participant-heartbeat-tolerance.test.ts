import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshBackgroundRetry, MeshLockTimeoutError } from "../src/core/atomic-write.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { withStateFence } from "../src/mesh/commit-outbox.js";
import { ParticipantDirectory, ParticipantLeaseSupersededError } from "../src/topology/participant-directory.js";
import { hostLeasePath, hostLeasePublishLockPath, acquireHostLeasePublishLock, HostLeasePublishLockBusyError, waitForHostLeasePublication, readHostLease, readHostLeaseCurrent, writeHostLease } from "../src/topology/host-leases.js";
import * as participantFiles from "../src/topology/participant-files.js";
import { createHash } from "node:crypto";
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
const setup = async (fenced = true, admission = true, lockTimeoutMs = 20_000, initialPublish = true, heartbeatMs = 5_000, prepared = false) => {
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
  const wait = vi.fn(async (_signal?: AbortSignal) => {
    expect(inFence).toBe(false);
    await withStateFence(mesh, identity, () => undefined);
  });
  const directory = new ParticipantDirectory(mesh, {
    enabled: true, identity, hostId: identity.id, rootId: identity.id,
    heartbeatMs, leaseMs: 15_000, reapDeadHosts: false,
    ...(prepared ? { preparePublicationFence: () => () => true } : {}),
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
  if (initialPublish) await directory.refresh();
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
  it("100 ms heartbeats do not start a second admission until 60 s after timeout, then only on the next regular tick", async () => {
    const s = await setup(true, true, 20_000, true, 100, true);
    vi.useFakeTimers(); await s.directory.start();
    vi.spyOn(console, "warn").mockImplementation(() => {});
    const confirm = vi.spyOn(s.mesh, "confirmWritable").mockRejectedValue(timeout());
    const subscribe = vi.spyOn(s.mesh, "onLockAdmission").mockReturnValue(() => {});
    let outstanding = 0, maximum = 0;
    s.wait.mockImplementation(signal => new Promise<void>((_resolve, reject) => {
      maximum = Math.max(maximum, ++outstanding);
      signal!.addEventListener("abort", () => { outstanding--; reject(signal!.reason); }, { once: true });
    }));
    await expect(s.directory.refresh()).rejects.toBeInstanceOf(MeshLockTimeoutError);
    await vi.advanceTimersByTimeAsync(0);
    expect(s.wait).toHaveBeenCalledOnce(); expect(outstanding).toBe(1);
    await vi.advanceTimersByTimeAsync(13_000);
    expect(s.wait).toHaveBeenCalledOnce(); expect(outstanding).toBe(0); expect(subscribe).toHaveBeenCalledOnce();
    const timedOutAt = Date.now();
    await vi.advanceTimersByTimeAsync(59_999);
    expect(s.wait).toHaveBeenCalledOnce(); expect(confirm).toHaveBeenCalledOnce();
    expect(s.lease().expiresAt).toBeGreaterThan(Date.now()); // independent renewal kept running
    await vi.advanceTimersByTimeAsync(1);
    expect(Date.now() - timedOutAt).toBe(60_000);
    expect(s.wait).toHaveBeenCalledTimes(2); expect(outstanding).toBe(1); expect(maximum).toBe(1);
    await s.directory.close(); expect(outstanding).toBe(0);
  });

  it("an admission event retries immediately after timeout, coalescing repeated wakes into one outstanding wait", async () => {
    const s = await setup(true, true, 20_000, true, 100, true);
    vi.useFakeTimers(); await s.directory.start();
    vi.spyOn(console, "warn").mockImplementation(() => {});
    let wake!: () => void;
    vi.spyOn(s.mesh, "onLockAdmission").mockImplementation(callback => { wake = callback; return () => {}; });
    const original = s.mesh.confirmWritable.bind(s.mesh);
    const confirm = vi.spyOn(s.mesh, "confirmWritable").mockRejectedValue(timeout());
    let outstanding = 0, maximum = 0, release!: () => void;
    s.wait.mockImplementation(signal => new Promise<void>((resolve, reject) => {
      maximum = Math.max(maximum, ++outstanding);
      release = () => { outstanding--; resolve(); };
      signal!.addEventListener("abort", () => { outstanding--; reject(signal!.reason); }, { once: true });
    }));
    await expect(s.directory.refresh()).rejects.toBeInstanceOf(MeshLockTimeoutError);
    await vi.advanceTimersByTimeAsync(13_000);
    expect(s.wait).toHaveBeenCalledOnce(); expect(outstanding).toBe(0);
    await vi.advanceTimersByTimeAsync(1_000);
    const eventAt = Date.now();
    wake(); wake(); await vi.advanceTimersByTimeAsync(0);
    expect(Date.now()).toBe(eventAt); expect(s.wait).toHaveBeenCalledTimes(2); expect(outstanding).toBe(1);
    wake(); await vi.advanceTimersByTimeAsync(0); expect(s.wait).toHaveBeenCalledTimes(2);
    s.record.status = "running"; confirm.mockImplementation(original);
    release(); await vi.advanceTimersByTimeAsync(0);
    expect(maximum).toBe(1); expect(outstanding).toBe(0);
    expect(s.directory.get(s.record.id)?.status).toBe("running"); expect(s.directory.canConsumeMesh()).toBe(true);
  });

  it("a non-cooperative timed-out callback cannot create a second outstanding native wait", async () => {
    const s = await setup(true, true, 20_000, true, 100, true);
    vi.useFakeTimers(); await s.directory.start();
    vi.spyOn(console, "warn").mockImplementation(() => {});
    vi.spyOn(s.mesh, "onLockAdmission").mockReturnValue(() => {});
    vi.spyOn(s.mesh, "confirmWritable").mockRejectedValue(timeout());
    let finish!: () => void;
    s.wait.mockImplementation(() => new Promise(resolve => { finish = resolve; }));
    await expect(s.directory.refresh()).rejects.toBeInstanceOf(MeshLockTimeoutError);
    await vi.advanceTimersByTimeAsync(73_000);
    expect(s.wait).toHaveBeenCalledOnce(); // fallback joins, never starts a second native callback
    finish(); await vi.advanceTimersByTimeAsync(0);
    await s.directory.close();
  });

  it("the successor attempting publication in the inode-check/rename gap survives and the stale directory stands down", async () => {
    const s = await setup();
    const lease = readHostLeaseCurrent(s.root, s.record.ownerHostId)!, successor = { ...lease, startedAt: lease.startedAt! + 1 };
    const file = hostLeasePath(s.root, lease.id), rename = fs.renameSync.bind(fs), abort = new AbortController();
    let publication: Promise<void> | undefined;
    const spy = vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (String(to) === file && String(from).includes(".renew-") && !publication) {
        expect(() => writeHostLease(s.root, successor)).toThrow(HostLeasePublishLockBusyError);
        publication = waitForHostLeasePublication(s.root, lease.id, abort.signal).then(() => writeHostLease(s.root, successor));
      }
      rename(from, to);
    });
    try {
      const refreshing = s.directory.refresh().catch(error => error);
      expect(publication).toBeDefined();
      await publication; await refreshing; spy.mockRestore();
      await expect(s.directory.refresh()).rejects.toBeInstanceOf(ParticipantLeaseSupersededError);
      expect(s.directory.canConsumeMesh()).toBe(false);
      await s.directory.closeLineage();
      expect(readHostLeaseCurrent(s.root, lease.id)).toEqual(successor);
      await expect(s.directory.start()).rejects.toBeInstanceOf(ParticipantLeaseSupersededError);
    } finally { abort.abort(); }
  });

  it("a held publish lock skips this tick without losing ownership or creating another admission lane", async () => {
    const s = await setup(); vi.useFakeTimers(); await s.directory.start();
    const lease = readHostLeaseCurrent(s.root, s.record.ownerHostId)!;
    const release = acquireHostLeasePublishLock(s.root, lease.id);
    try {
      await vi.advanceTimersByTimeAsync(5_000);
      expect(readHostLeaseCurrent(s.root, lease.id)).toEqual(lease);
      expect(s.directory.canConsumeMesh()).toBe(true);
      expect(s.wait).not.toHaveBeenCalled();
    } finally { release(); }
    await vi.advanceTimersByTimeAsync(5_000);
    expect(readHostLeaseCurrent(s.root, lease.id)?.updatedAt).toBe(Date.now());
    expect(fs.existsSync(hostLeasePublishLockPath(s.root, lease.id))).toBe(false);
  });

  it("a same-host successor after clean release but before closeLineage resumes receives no stale terminal receipt", async () => {
    const s = await setup(); vi.useFakeTimers();
    const close = s.directory.close.bind(s.directory);
    let successor!: ParticipantDirectory;
    vi.spyOn(s.directory, "close").mockImplementationOnce(async () => {
      await close();
      vi.setSystemTime(Date.now() + 1);
      successor = new ParticipantDirectory(s.mesh, { ...s.directory.options });
      directories.push(successor);
      successor.registerSource(() => [{ ...s.record, status: "running", updatedAt: Date.now() }]);
      await successor.refresh();
    });
    await s.directory.closeLineage();
    expect(successor.canConsumeMesh()).toBe(true);
    expect(successor.get(s.record.id)?.status).toBe("running");
    expect(readHostLeaseCurrent(s.root, s.record.ownerHostId)).toBeDefined();
    expect(s.mesh.listAll("topology/lineage-closures/")).toEqual([]);
    await expect(s.directory.start()).rejects.toBeInstanceOf(ParticipantLeaseSupersededError);
  });

  it.each(["file", "state"] as const)("a same-host successor taking over during awaited %s close cleanup keeps all its records and lease", async phase => {
    const s = await setup(); vi.useFakeTimers();
    const old = readHostLeaseCurrent(s.root, s.record.ownerHostId)!;
    const key = (prefix: string, id: string) => prefix + createHash("sha256").update(id).digest("hex");
    let successor!: ParticipantDirectory;
    const takeover = async (): Promise<void> => {
      vi.setSystemTime(Date.now() + 1);
      writeHostLease(s.root, { ...old, expiresAt: Date.now() - 1,
        ...(old.session ? { session: { ...old.session, expiresAt: Date.now() - 1 } } : {}) });
      successor = new ParticipantDirectory(s.mesh, { ...s.directory.options });
      directories.push(successor);
      successor.registerSource(() => [{ ...s.record, status: "running", updatedAt: Date.now() }]);
      await successor.refresh();
    };
    if (phase === "file") {
      const remove = participantFiles.removeParticipantFileIf;
      vi.spyOn(participantFiles, "removeParticipantFileIf").mockImplementationOnce(async (...args) => {
        await takeover(); return remove(...args);
      });
    } else {
      const batch = s.mesh.writeBatch.bind(s.mesh);
      vi.spyOn(s.mesh, "writeBatch").mockImplementationOnce(async args => { await takeover(); return batch(args); });
    }
    await s.directory.closeLineage();
    expect(successor).toBeDefined(); expect(successor.canConsumeMesh()).toBe(true);
    const current = readHostLeaseCurrent(s.root, old.id)!;
    expect(current.startedAt).toBeGreaterThan(old.startedAt!);
    expect(s.mesh.get(key("topology/hosts/", old.id))?.value).toMatchObject({ startedAt: current.startedAt });
    expect(s.mesh.get(key("topology/participants/", s.record.id))?.value).toMatchObject({ ownerStartedAt: current.startedAt, status: "running" });
    expect(participantFiles.readParticipantFile(s.root, key("topology/participants/", s.record.id))?.value)
      .toMatchObject({ ownerStartedAt: current.startedAt, status: "running" });
    expect(s.mesh.get(`sessions/${s.record.sessionId}`)?.value).toMatchObject({ livenessStartedAt: current.startedAt });
    expect(s.mesh.listAll("topology/lineage-closures/")).toEqual([]);
    expect(s.directory.canConsumeMesh()).toBe(false);
  });

  it("keeps leases and counts zero heartbeat timeouts under 30–80 ms locks at 50% duty for 10 s", async () => {
    const s = await setup();
    const runner = new MeshBackgroundRetry("heartbeat tolerance test");
    const failure = vi.spyOn(runner, "failure");
    vi.useFakeTimers();
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
      console.info("heartbeat-contention-probe", JSON.stringify({ durationMs: 10_000, rounds, lostLeases, timeouts: failure.mock.calls.length }));
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
    let wake: (() => void) | undefined;
    vi.spyOn(s.mesh, "onLockAdmission").mockImplementation(callback => { wake = callback; return () => {}; });
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
    s.release(); wake?.(); await s.directory.refresh(); // a real release/admission event, not a heartbeat poll
    expect(s.directory.writeStalled()).toBeUndefined(); expect(s.directory.canConsumeMesh()).toBe(true);
  });

  it("without an admission callback retries once at the existing heartbeat tick, never a new timer", async () => {
    const s = await setup(true, false);
    vi.useFakeTimers(); await s.directory.start();
    const confirm = vi.spyOn(s.mesh, "confirmWritable").mockRejectedValue(timeout());
    vi.spyOn(console, "warn").mockImplementation(() => {});
    const timers = vi.spyOn(globalThis, "setTimeout");
    await expect(s.directory.refresh()).rejects.toBeInstanceOf(MeshLockTimeoutError);
    expect(confirm).toHaveBeenCalledOnce(); expect(timers).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(4_999); expect(confirm).toHaveBeenCalledOnce();
    await vi.advanceTimersByTimeAsync(1); expect(confirm).toHaveBeenCalledTimes(2);
    expect(s.wait).not.toHaveBeenCalled(); expect(timers).not.toHaveBeenCalled();
  });

  it("reselects sources after event admission without widening registry tries", async () => {
    const s = await setup();
    vi.useFakeTimers();
    let release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    s.wait.mockImplementation(() => gate);
    const timers = vi.spyOn(globalThis, "setTimeout");
    const original = s.mesh.confirmWritable.bind(s.mesh);
    const confirm = vi.spyOn(s.mesh, "confirmWritable").mockRejectedValueOnce(timeout()).mockImplementation(original);
    const work = s.directory.refresh();
    await vi.advanceTimersByTimeAsync(0);
    const reads = s.source.mock.calls.length;
    s.record.status = "running";
    release(); await work;
    expect(timers).toHaveBeenCalledOnce(); // only the frozen admission deadline, no retry sleep
    expect(timers.mock.calls[0]![1]).toBe(13_000);
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

  it("allows an absent lease before initial ownership and creates it on admission", async () => {
    const s = await setup(true, true, 20_000, false);
    expect(readHostLeaseCurrent(s.root, s.record.ownerHostId)).toBeUndefined();
    await s.directory.refresh();
    expect(readHostLeaseCurrent(s.root, s.record.ownerHostId)).toMatchObject({
      identityId: s.record.ownerIdentityId, rootId: s.record.rootId,
    });
    expect(s.directory.canConsumeMesh()).toBe(true);
  });

  it("a positive clean release can start a new incarnation, never resume the released owner", async () => {
    const s = await setup();
    const first = s.lease().startedAt;
    await s.directory.close();
    expect(readHostLeaseCurrent(s.root, s.record.ownerHostId)).toBeUndefined();
    await s.directory.start();
    expect(s.lease().startedAt).toBeGreaterThan(first!);
    expect(s.directory.canConsumeMesh()).toBe(true);
  });

  it.each(["refresh", "refreshPresence", "start"] as const)("a missing owned lease supersedes %s without recreating it", async operation => {
    const s = await setup();
    const leasePath = hostLeasePath(s.root, s.record.ownerHostId);
    const prior = s.directory.confirmedAt(), state = fs.readFileSync(path.join(s.root, "state.json"), "utf8");
    const confirm = vi.spyOn(s.mesh, "confirmWritable");
    fs.unlinkSync(leasePath);
    await expect(s.directory[operation]()).rejects.toBeInstanceOf(ParticipantLeaseSupersededError);
    expect(fs.existsSync(leasePath)).toBe(false);
    expect(s.directory.confirmedAt()).toBe(prior); expect(s.directory.canConsumeMesh()).toBe(false);
    await expect(s.directory.refresh()).rejects.toMatchObject({ code: "FABRIC_PARTICIPANT_LEASE_SUPERSEDED" });
    await expect(s.directory.quiesce("reload")).rejects.toBeInstanceOf(ParticipantLeaseSupersededError);
    await s.directory.closeLineage();
    await expect(s.directory.start()).rejects.toBeInstanceOf(ParticipantLeaseSupersededError); // failed close never grants a new incarnation
    expect(confirm).not.toHaveBeenCalled(); expect(fs.existsSync(leasePath)).toBe(false);
    expect(fs.readFileSync(path.join(s.root, "state.json"), "utf8")).toBe(state);
  });

  it.each([true, false])("a lease deleted inside a failed attempt fences retry and failed-write renewal (typed timeout: %s)", async typed => {
    const s = await setup();
    const leasePath = hostLeasePath(s.root, s.record.ownerHostId), prior = s.directory.confirmedAt();
    const confirm = vi.spyOn(s.mesh, "confirmWritable").mockImplementation(async () => {
      fs.unlinkSync(leasePath); throw typed ? timeout() : new Error("ENOSPC");
    });
    await expect(s.directory.refresh()).rejects.toBeInstanceOf(ParticipantLeaseSupersededError);
    expect(confirm).toHaveBeenCalledOnce(); expect(fs.existsSync(leasePath)).toBe(false);
    expect(s.directory.confirmedAt()).toBe(prior); expect(s.directory.canConsumeMesh()).toBe(false);
  });

  it("a lease deleted during event admission stops the old owner before a successor claims it", async () => {
    const s = await setup();
    vi.useFakeTimers(); vi.spyOn(Math, "random").mockReturnValue(0);
    const leasePath = hostLeasePath(s.root, s.record.ownerHostId), previous = s.lease(), prior = s.directory.confirmedAt();
    const confirm = vi.spyOn(s.mesh, "confirmWritable").mockRejectedValueOnce(timeout());
    s.wait.mockImplementation(async () => { fs.unlinkSync(leasePath); });
    const failed = expect(s.directory.refresh()).rejects.toBeInstanceOf(ParticipantLeaseSupersededError);
    await vi.advanceTimersByTimeAsync(0); expect(confirm).toHaveBeenCalledOnce();
    await failed;
    expect(fs.existsSync(leasePath)).toBe(false);
    expect(s.directory.confirmedAt()).toBe(prior); expect(s.directory.canConsumeMesh()).toBe(false);
    expect(s.wait).toHaveBeenCalledOnce(); expect(confirm).toHaveBeenCalledOnce();
    writeHostLease(s.root, { ...previous, startedAt: previous.startedAt! + 1 });
    const bytes = fs.readFileSync(leasePath, "utf8");
    await expect(s.directory.start()).rejects.toBeInstanceOf(ParticipantLeaseSupersededError);
    await expect(s.directory.refresh()).rejects.toBeInstanceOf(ParticipantLeaseSupersededError);
    await s.directory.closeLineage();
    expect(fs.readFileSync(leasePath, "utf8")).toBe(bytes);
  });

  it("a missing owned lease fences timer renewal and a delayed confirmation callback", async () => {
    const s = await setup(); vi.useFakeTimers(); await s.directory.start();
    const leasePath = hostLeasePath(s.root, s.record.ownerHostId), prior = s.directory.confirmedAt();
    let release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    const confirm = vi.spyOn(s.mesh, "confirmWritable").mockImplementation(async callback => {
      await gate; callback?.(Date.now());
    });
    const failed = expect(s.directory.refresh()).rejects.toBeInstanceOf(ParticipantLeaseSupersededError);
    try {
      await vi.advanceTimersByTimeAsync(0); fs.unlinkSync(leasePath);
      vi.spyOn(console, "warn").mockImplementation(() => {});
      await vi.advanceTimersByTimeAsync(5_000);
      expect(fs.existsSync(leasePath)).toBe(false); expect(s.directory.canConsumeMesh()).toBe(false);
      release(); await failed;
      expect(s.directory.confirmedAt()).toBe(prior);
      await vi.advanceTimersByTimeAsync(10_000);
      expect(confirm).toHaveBeenCalledOnce(); expect(fs.existsSync(leasePath)).toBe(false);
    } finally { release(); }
  });

  it.each(["after-open", "after-rename"] as const)("a successor racing renewal %s fences the loser without a later overwrite", async phase => {
    const s = await setup();
    const leasePath = hostLeasePath(s.root, s.record.ownerHostId), previous = s.lease();
    const successor = { ...previous, identityId: "successor", startedAt: previous.startedAt! + 1 };
    const prior = s.directory.confirmedAt();
    const confirm = vi.spyOn(s.mesh, "confirmWritable");
    let bytes = "", raced = false, loserRenames = 0;
    // Out-of-protocol old writer seam: compliant publishers now take the mutex
    // (covered above). Preserve round-2 strict inode/read-back defense as well.
    const replace = () => { fs.writeFileSync(leasePath, JSON.stringify({ format: 1, ...successor })); bytes = fs.readFileSync(leasePath, "utf8"); };
    if (phase === "after-open") {
      const read = fs.readFileSync.bind(fs);
      vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, ...args: unknown[]) => {
        const text = (read as (...args: unknown[]) => unknown)(file, ...args);
        if (typeof file === "number" && !raced) { raced = true; replace(); }
        return text;
      }) as typeof fs.readFileSync);
    } else {
      const rename = fs.renameSync.bind(fs);
      vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
        if (String(to) === leasePath && path.basename(String(from)).startsWith(".renew-")) {
          loserRenames++;
          if (!raced) {
            raced = true;
            rename(from, to);
            replace(); // successor wins publication before our post-rename read
            return;
          }
        }
        rename(from, to);
      });
    }
    await expect(s.directory.refresh()).rejects.toBeInstanceOf(ParticipantLeaseSupersededError);
    expect(raced).toBe(true); expect(loserRenames).toBe(phase === "after-open" ? 0 : 1);
    expect(confirm).not.toHaveBeenCalled(); expect(s.directory.confirmedAt()).toBe(prior);
    expect(s.directory.canConsumeMesh()).toBe(false);
    await expect(s.directory.refresh()).rejects.toBeInstanceOf(ParticipantLeaseSupersededError);
    await expect(s.directory.start()).rejects.toBeInstanceOf(ParticipantLeaseSupersededError);
    await s.directory.closeLineage();
    expect(fs.readFileSync(leasePath, "utf8")).toBe(bytes);
    expect(fs.readdirSync(path.dirname(leasePath)).filter(name => name.startsWith(".renew-"))).toEqual([]);
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

  const replaceLease = (s: Awaited<ReturnType<typeof setup>>, identity = false) => {
    const next = { ...s.lease(), startedAt: s.lease().startedAt! + 1,
      ...(identity ? { identityId: "successor" } : {}) };
    writeHostLease(s.root, next);
    return fs.readFileSync(hostLeasePath(s.root, next.id), "utf8");
  };

  it.each([false, true])("a successor during event admission fences every later renewal and confirmation (identity changes: %s)", async identity => {
    const s = await setup();
    vi.useFakeTimers(); vi.spyOn(Math, "random").mockReturnValue(0);
    const prior = s.directory.confirmedAt();
    let release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    s.wait.mockImplementation(() => gate);
    const confirm = vi.spyOn(s.mesh, "confirmWritable").mockRejectedValue(timeout());
    const failed = expect(s.directory.refresh()).rejects.toBeInstanceOf(ParticipantLeaseSupersededError);
    await vi.advanceTimersByTimeAsync(0); expect(confirm).toHaveBeenCalledOnce();
    const bytes = replaceLease(s, identity);
    release(); await failed;
    expect(s.directory.confirmedAt()).toBe(prior); expect(s.directory.canConsumeMesh()).toBe(false);
    await expect(s.directory.refresh()).rejects.toMatchObject({ code: "FABRIC_PARTICIPANT_LEASE_SUPERSEDED" });
    await expect(s.directory.quiesce("reload")).rejects.toBeInstanceOf(ParticipantLeaseSupersededError);
    await s.directory.closeLineage();
    expect(confirm).toHaveBeenCalledOnce();
    expect(fs.readFileSync(hostLeasePath(s.root, s.lease().id), "utf8")).toBe(bytes);
    expect(s.mesh.listAll("topology/lineage-closures/")).toEqual([]);
  });

  it("rechecks incarnation after admission, before any fresh publication", async () => {
    const s = await setup();
    vi.useFakeTimers(); vi.spyOn(Math, "random").mockReturnValue(0);
    const prior = s.directory.confirmedAt();
    const confirm = vi.spyOn(s.mesh, "confirmWritable").mockRejectedValueOnce(timeout());
    let bytes = "";
    s.wait.mockImplementation(async () => { bytes = replaceLease(s); });
    const failed = expect(s.directory.refresh()).rejects.toBeInstanceOf(ParticipantLeaseSupersededError);
    await vi.advanceTimersByTimeAsync(150); await failed;
    expect(s.wait).toHaveBeenCalledOnce(); expect(confirm).toHaveBeenCalledOnce();
    expect(s.fence).toHaveBeenCalledTimes(2); // initial and failed publication, never a third
    expect(s.directory.confirmedAt()).toBe(prior); expect(s.directory.canConsumeMesh()).toBe(false);
    expect(fs.readFileSync(hostLeasePath(s.root, s.lease().id), "utf8")).toBe(bytes);
  });

  it.each([true, false])("fences timeout and base failed-write renewal when the lease changes inside the failed attempt (typed timeout: %s)", async typed => {
    const s = await setup();
    let bytes = "";
    const confirm = vi.spyOn(s.mesh, "confirmWritable").mockImplementation(async () => {
      bytes = replaceLease(s); throw typed ? timeout() : new Error("ENOSPC");
    });
    const prior = s.directory.confirmedAt();
    await expect(s.directory.refresh()).rejects.toBeInstanceOf(ParticipantLeaseSupersededError);
    expect(confirm).toHaveBeenCalledOnce(); expect(s.directory.confirmedAt()).toBe(prior);
    expect(s.directory.canConsumeMesh()).toBe(false);
    expect(fs.readFileSync(hostLeasePath(s.root, s.lease().id), "utf8")).toBe(bytes);
  });

  it("fences the base timer renewal and a delayed confirmation callback", async () => {
    const s = await setup(); vi.useFakeTimers();
    await s.directory.start();
    const prior = s.directory.confirmedAt();
    let release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    const confirm = vi.spyOn(s.mesh, "confirmWritable").mockImplementation(async callback => {
      await gate; callback?.(Date.now());
    });
    const failed = expect(s.directory.refresh()).rejects.toBeInstanceOf(ParticipantLeaseSupersededError);
    try {
      await vi.advanceTimersByTimeAsync(0);
      const bytes = replaceLease(s);
      vi.spyOn(console, "warn").mockImplementation(() => {});
      await vi.advanceTimersByTimeAsync(5_000); // timer's independent renewal sees the successor
      expect(s.directory.canConsumeMesh()).toBe(false);
      expect(fs.readFileSync(hostLeasePath(s.root, s.lease().id), "utf8")).toBe(bytes);
      release(); await failed;
      expect(s.directory.confirmedAt()).toBe(prior); expect(confirm).toHaveBeenCalledOnce();
      await vi.advanceTimersByTimeAsync(10_000); expect(confirm).toHaveBeenCalledOnce();
      await expect(s.directory.start()).rejects.toBeInstanceOf(ParticipantLeaseSupersededError);
    } finally { release(); }
  });

  it("revalidates shared write preparation after a successor claims the lease", async () => {
    const s = await setup();
    const original = s.mesh.writeBatch.bind(s.mesh);
    const state = fs.readFileSync(path.join(s.root, "state.json"), "utf8");
    let bytes = "";
    vi.spyOn(s.mesh, "writeBatch").mockImplementation(args => original({ ...args, prepare: view => {
      bytes = replaceLease(s); return args.prepare?.(view) ?? [];
    } }));
    s.record.status = "running";
    await expect(s.directory.refresh()).rejects.toBeInstanceOf(ParticipantLeaseSupersededError);
    expect(fs.readFileSync(path.join(s.root, "state.json"), "utf8")).toBe(state);
    expect(fs.readFileSync(hostLeasePath(s.root, s.lease().id), "utf8")).toBe(bytes);
  });

  it("does not start an unbounded acquisition when preparation spent the monotonic budget", async () => {
    const s = await setup(); vi.useFakeTimers();
    const prior = s.directory.confirmedAt();
    const confirm = vi.spyOn(s.mesh, "confirmWritable");
    vi.spyOn(performance, "now").mockReturnValueOnce(0).mockReturnValue(13_001);
    await expect(s.directory.refresh()).rejects.toBeInstanceOf(MeshLockTimeoutError);
    expect(confirm).not.toHaveBeenCalled(); expect(s.directory.confirmedAt()).toBe(prior);
    expect(s.directory.canConsumeMesh()).toBe(false);
  });

  it("spends only the frozen monotonic budget after a backward wall-clock step", async () => {
    const s = await setup();
    vi.useFakeTimers(); vi.spyOn(Math, "random").mockReturnValue(0);
    const start = performance.now(), prior = s.directory.confirmedAt();
    s.hold();
    let spent = 0;
    const failed = expect(s.directory.refresh().finally(() => { spent = performance.now() - start; }))
      .rejects.toBeInstanceOf(MeshLockTimeoutError);
    await vi.advanceTimersByTimeAsync(0); // first try failed; now blocked in native FIFO admission
    vi.setSystemTime(Date.now() - 60_000);
    await vi.advanceTimersByTimeAsync(13_000); await failed;
    expect(spent).toBe(13_000); expect(s.directory.confirmedAt()).toBe(prior);
    expect(s.directory.canConsumeMesh()).toBe(false);
    expect(readHostLeaseCurrent(s.root, s.lease().id)?.startedAt).toBe(s.lease().startedAt);
    s.release();
  });

  it.each(["close", "quiesce"] as const)("%s cancels a blocked native inline admission without awaiting the holder", async operation => {
    const s = await setup();
    vi.useFakeTimers(); vi.spyOn(Math, "random").mockReturnValue(0);
    s.hold();
    const failed = expect(s.directory.refresh()).rejects.toThrow(`Participant directory is ${operation === "close" ? "closed" : "quiescing"}`);
    await vi.advanceTimersByTimeAsync(150); expect(s.wait).toHaveBeenCalledOnce();
    // Teardown may publish its own stopping state, but the retry's FIFO holder
    // must not be awaited. Stub only teardown writes; keep admission genuinely blocked.
    if (operation === "close") vi.spyOn(s.mesh, "delete").mockResolvedValue({ deleted: false });
    else vi.spyOn(s.mesh, "writeBatch").mockResolvedValue([]);
    let finished = false;
    const stopping = s.directory[operation]().then(() => { finished = true; });
    await vi.advanceTimersByTimeAsync(0); await failed;
    expect(finished).toBe(true); await stopping;
    expect(fs.existsSync(path.join(s.root, ".lock"))).toBe(true);
    s.release();
  });
});
