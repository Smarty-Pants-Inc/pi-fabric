// Keep: audit CODE-01/CODE-02; startup liveness and the live/unknown-owner trust boundary.
// In-memory persistence and lock receipts deliberately perform no filesystem deletions.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { FabricHostLease } from "../src/topology/host-leases.js";

const fixture = vi.hoisted(() => ({
  files: new Map<string, string | undefined>(),
  mode: "healthy" as "healthy" | "setup" | "sync-error",
  processState: "live" as "live" | "unknown" | "exited",
  changed: undefined as undefined | ((kind: "events" | "presence" | "lease") => void),
  failed: undefined as undefined | ((error: Error) => void),
  onSetup: undefined as undefined | (() => void),
  stop: vi.fn(), custody: vi.fn(),
  rawBirth: process.platform === "darwin" ? "Mon Jan  1 00:00:00 2024" : "12345",
  birth: process.platform === "darwin" ? "darwin:Mon Jan  1 00:00:00 2024" :
    process.platform === "win32" ? "win32:12345" : "12345",
}));
vi.mock("../src/core/atomic-write.js", async importOriginal => ({
  ...await importOriginal<typeof import("../src/core/atomic-write.js")>(),
  ownProcessIncarnation: async () => fixture.birth,
  readFileRetrying: (file: string) => {
    const text = fixture.files.get(file);
    if (text === undefined) throw Object.assign(new Error("absent"), { code: "ENOENT" });
    return text;
  },
  writeJsonAtomic: (file: string, value: unknown) => { fixture.files.set(file, JSON.stringify(value)); },
}));
vi.mock("node:child_process", async importOriginal => ({
  ...await importOriginal<typeof import("node:child_process")>(),
  execFileSync: () => fixture.rawBirth,
}));
vi.mock("../src/mesh/custody-lock.js", () => ({
  acquireMeshCustodyLock: (...args: unknown[]) => { fixture.custody(...args); return Promise.resolve(() => {}); },
}));
vi.mock("../src/topology/host-lease-lock.js", async importOriginal => ({
  ...await importOriginal<typeof import("../src/topology/host-lease-lock.js")>(),
  // Keep real renewHostLease/commitHostLeaseUnderCustody and beforeReplace validation.
  withHostLeaseLock: (mesh: { leaseCustody: Function }, file: string, operation: () => unknown) =>
    mesh.leaseCustody(file, operation),
}));
vi.mock("../src/mesh/change-notifier.js", () => ({
  watchBridgeStore: vi.fn((_root: string, changed: typeof fixture.changed, failed: typeof fixture.failed) => {
    fixture.changed = changed; fixture.failed = failed;
    fixture.onSetup?.();
    if (fixture.mode === "setup") throw new Error("injected watch setup failure");
    if (fixture.mode === "sync-error") failed!(new Error("injected synchronous watch failure"));
    return fixture.stop;
  }),
}));

import { claimStartingHost, FabricHostLeaseWatchError, HOST_STARTUP_BOUND_MS } from "../src/topology/starting-host.js";
import { hostLeasePath, hostLeasePredecessor, writeHostLease, FabricHostLeaseSupersededError } from "../src/topology/host-leases.js";
import { watchBridgeStore } from "../src/mesh/change-notifier.js";

const ROOT = path.join(os.tmpdir(), "starting-main-wait-in-memory");
const ID = "session:wait-target", PID = 2_000_000_001, NOW = 1_800_000_000_000;
const predecessor = (expiresAt = NOW + 1_000): FabricHostLease => ({
  id: ID, rootId: ID, identityId: ID, incarnationToken: "incumbent", ownerIncarnation: "old-control",
  startedAt: NOW - 1_000, updatedAt: NOW - 100, expiresAt,
  process: { pid: PID, host: os.hostname(), startTime: fixture.birth },
  session: { id: "wait-target", startedAt: NOW - 1_000, updatedAt: NOW - 100, expiresAt },
});
const flush = async () => { for (let i = 0; i < 30; i++) await Promise.resolve(); };
const begin = () => {
  const result: { state: "pending" | "claimed" | "refused"; error?: unknown; value?: Awaited<ReturnType<typeof claimStartingHost>> } = { state: "pending" };
  const done = claimStartingHost({ meshRoot: ROOT, cwd: ROOT, id: ID, sessionId: "wait-target", lockProtocol: 1, stateBackend: "file" })
    .then(value => { result.state = "claimed"; result.value = value; }, error => { result.state = "refused"; result.error = error; });
  return { result, done };
};
const expectWatchRefusal = (result: ReturnType<typeof begin>["result"], original: FabricHostLease) => {
  expect(result.state).toBe("refused");
  expect(result.error).toBeInstanceOf(FabricHostLeaseWatchError);
  expect(result.error).toMatchObject({ code: "FABRIC_HOST_LEASE_WATCH_FAILED", retryable: true, cause: expect.any(Error) });
  expect(hostLeasePredecessor(ROOT, ID)).toEqual(original);
  expect(fixture.custody).toHaveBeenCalledTimes(1);
  expect(vi.getTimerCount()).toBe(0);
};

beforeEach(() => {
  vi.useFakeTimers(); vi.setSystemTime(NOW); vi.clearAllMocks();
  fixture.files.clear(); fixture.mode = "healthy"; fixture.processState = "live";
  fixture.changed = undefined; fixture.failed = undefined; fixture.onSetup = undefined;
  // Deterministic native process fixture; no child is launched and no real PID is signalled.
  vi.spyOn(process, "kill").mockImplementation(((pid: number) => {
    if (pid === PID && fixture.processState !== "live")
      throw Object.assign(new Error(fixture.processState), { code: fixture.processState === "exited" ? "ESRCH" : "EPERM" });
    return true;
  }) as typeof process.kill);
  const read = fs.readFileSync;
  vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, ...args: unknown[]) => {
    if (/^\/proc\/\d+\/stat$/.test(String(file))) return `1 (fixture) ${Array(19).fill("0").join(" ")} 12345`;
    return Reflect.apply(read, fs, [file, ...args]);
  }) as typeof fs.readFileSync);
  if (process.platform === "win32") vi.stubEnv("SystemRoot", process.env.SystemRoot ?? "C:\\Windows");
});
afterEach(() => { vi.clearAllTimers(); vi.useRealTimers(); vi.restoreAllMocks(); vi.unstubAllEnvs(); });

describe("bounded predecessor watch failures", () => {
  it.each(["setup", "sync-error"] as const)("%s before expiry rereads once at expiry then surfaces typed refusal", async mode => {
    fixture.mode = mode;
    const old = predecessor(); writeHostLease(ROOT, old);
    const { result, done } = begin(); await flush();
    expect(result.state).toBe("pending"); expect(vi.getTimerCount()).toBe(1);
    await vi.advanceTimersByTimeAsync(999); expect(result.state).toBe("pending");
    await vi.advanceTimersByTimeAsync(1);
    expectWatchRefusal(result, old); await done;
    await vi.advanceTimersByTimeAsync(HOST_STARTUP_BOUND_MS * 2);
    expect(fixture.custody).toHaveBeenCalledTimes(1);
  });

  it.each(["live", "unknown"] as const)("setup failure after expiry immediately refuses an unchanged %s owner", async state => {
    fixture.mode = "setup"; fixture.processState = state;
    const old = predecessor(NOW - 1); writeHostLease(ROOT, old);
    const { result, done } = begin(); await flush();
    expectWatchRefusal(result, old); await done;
  });

  it.each([false, true])("async watch error (already expired: %s) is bounded and closes the watch", async expired => {
    const old = predecessor(expired ? NOW - 1 : NOW + 1_000); writeHostLease(ROOT, old);
    const { result, done } = begin(); await flush();
    fixture.failed!(new Error("injected asynchronous watcher error")); await flush();
    expect(fixture.stop).toHaveBeenCalledTimes(1);
    if (!expired) { expect(result.state).toBe("pending"); await vi.advanceTimersByTimeAsync(1_000); }
    expectWatchRefusal(result, old); await done;
  });

  it("caps an unavailable watch's long lease wait at the existing startup bound", async () => {
    fixture.mode = "setup";
    const old = predecessor(NOW + HOST_STARTUP_BOUND_MS * 3); writeHostLease(ROOT, old);
    const { result, done } = begin(); await flush();
    await vi.advanceTimersByTimeAsync(HOST_STARTUP_BOUND_MS);
    expectWatchRefusal(result, old); await done;
  });

  it.each(["exit", "removal"] as const)("%s during failed-watch fallback safely claims at the single expiry wake", async change => {
    fixture.mode = "setup"; writeHostLease(ROOT, predecessor());
    const { result, done } = begin(); await flush();
    if (change === "exit") fixture.processState = "exited";
    else fixture.files.set(hostLeasePath(ROOT, ID), undefined); // model absence, not an unlink
    await vi.advanceTimersByTimeAsync(1_000);
    expect(result.state).toBe("claimed"); await done;
    expect(result.value?.lease.incarnationToken).not.toBe("incumbent");
    expect(hostLeasePredecessor(ROOT, ID)?.incarnationToken).toBe(result.value?.lease.incarnationToken);
    expect(fixture.custody).toHaveBeenCalledTimes(2); expect(vi.getTimerCount()).toBe(0);
  });

  it.each(["exit", "removal"] as const)("immediate failure reread catches %s in the watch-setup race", async change => {
    fixture.mode = "setup"; writeHostLease(ROOT, predecessor(NOW - 1));
    fixture.onSetup = () => {
      if (change === "exit") fixture.processState = "exited";
      else fixture.files.set(hostLeasePath(ROOT, ID), undefined);
    };
    const { result, done } = begin(); await flush();
    expect(result.state).toBe("claimed"); await done;
    expect(fixture.custody).toHaveBeenCalledTimes(2);
  });
});

describe("target-lease-only predecessor notifications", () => {
  it("does not shorten a healthy watch's existing positive expiry timer", async () => {
    const old = predecessor(NOW + HOST_STARTUP_BOUND_MS * 3); writeHostLease(ROOT, old);
    const { result, done } = begin(); await flush();
    await vi.advanceTimersByTimeAsync(HOST_STARTUP_BOUND_MS);
    expect(result.state).toBe("pending"); expect(fixture.custody).toHaveBeenCalledTimes(1);
    writeHostLease(ROOT, { ...old, cleanHandoff: old.incarnationToken }); fixture.changed!("presence");
    await flush(); expect(result.state).toBe("claimed"); await done;
    expect(vi.getTimerCount()).toBe(0);
  });

  it("ignores shared events/other leases, preserves heartbeat and handoff-only changes, and never zero-timer spins", async () => {
    const old = predecessor(); writeHostLease(ROOT, old);
    const { result, done } = begin(); await flush();
    const unrelated = { ...old, id: "session:other", rootId: "session:other", identityId: "session:other" };
    for (let i = 0; i < 20; i++) {
      writeHostLease(ROOT, { ...unrelated, updatedAt: NOW + i });
      fixture.changed!(i % 2 ? "events" : "presence");
    }
    await flush(); expect(fixture.custody).toHaveBeenCalledTimes(1);
    expect(watchBridgeStore).toHaveBeenCalledTimes(1); expect(result.state).toBe("pending");
    await vi.advanceTimersByTimeAsync(1_000); // ONE ordinary expiry revalidation, never takeover.
    expect(fixture.custody).toHaveBeenCalledTimes(2); expect(result.state).toBe("pending");
    expect(vi.getTimerCount()).toBe(0);
    fixture.changed!("events"); await flush(); expect(fixture.custody).toHaveBeenCalledTimes(2);
    const heartbeat = { ...old, updatedAt: NOW + 1_000 };
    writeHostLease(ROOT, heartbeat); fixture.changed!("presence"); await flush();
    expect(fixture.custody).toHaveBeenCalledTimes(3); expect(result.state).toBe("pending");
    // No timestamps change: cleanHandoff itself must be in the canonical version comparison.
    writeHostLease(ROOT, { ...heartbeat, cleanHandoff: heartbeat.incarnationToken }); fixture.changed!("presence");
    await flush(); expect(result.state).toBe("claimed"); await done;
    expect(fixture.custody).toHaveBeenCalledTimes(4); expect(vi.getTimerCount()).toBe(0);
  });

  it.each(["exit", "removal", "handoff"] as const)("watch-before-reread catches %s without needing a notification", async change => {
    const old = predecessor(); writeHostLease(ROOT, old);
    fixture.onSetup = () => {
      if (change === "exit") fixture.processState = "exited";
      else if (change === "removal") fixture.files.set(hostLeasePath(ROOT, ID), undefined);
      else writeHostLease(ROOT, { ...old, cleanHandoff: old.incarnationToken });
    };
    const { result, done } = begin(); await flush();
    expect(result.state).toBe("claimed"); await done;
    expect(fixture.custody).toHaveBeenCalledTimes(2); expect(fixture.stop).toHaveBeenCalledTimes(1);
  });

  it.each(["exit", "removal"] as const)("a %s notification permits only the existing safe claim proof", async change => {
    writeHostLease(ROOT, predecessor());
    const { result, done } = begin(); await flush();
    if (change === "exit") fixture.processState = "exited";
    else fixture.files.set(hostLeasePath(ROOT, ID), undefined);
    fixture.changed!("presence"); await flush();
    expect(result.state).toBe("claimed"); await done;
    expect(fixture.custody).toHaveBeenCalledTimes(2); expect(vi.getTimerCount()).toBe(0);
  });

  it("different-token notification wakes but cannot recapture the intervening owner's authority", async () => {
    const old = predecessor(); writeHostLease(ROOT, old);
    const { result, done } = begin(); await flush();
    const replacement = { ...old, incarnationToken: "intervening-owner" };
    writeHostLease(ROOT, replacement); fixture.changed!("presence"); await flush();
    expect(result.state).toBe("refused"); expect(result.error).toBeInstanceOf(FabricHostLeaseSupersededError); await done;
    expect(hostLeasePredecessor(ROOT, ID)).toEqual(replacement); expect(fixture.custody).toHaveBeenCalledTimes(2);
  });
});
