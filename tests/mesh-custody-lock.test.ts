import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { acquireMeshCustodyLock, MeshCustodyUnrecoverableError, MESH_CUSTODY_LOCK_NAME, meshCustodyMode } from "../src/mesh/custody-lock.js";
import { MeshIncarnationPendingError, MeshLockForeignHolderError, MeshLockTimeoutError, ownProcessIncarnation } from "../src/core/atomic-write.js";
import { meshLockQueueDirectory } from "../src/mesh/lock-queue.js";

// smarty-dev#6477 L5: file custody takes its own lock instead of the single mesh .lock.
const roots: string[] = [];
const store = (): MeshStore => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-custody-"));
  roots.push(root);
  return new MeshStore(path.join(root, "mesh"), 64 * 1024, 500, { lockTimeoutMs: 300 });
};
const from: MeshIdentity = { id: "session:custody", sessionId: "custody", name: "custody", kind: "main" };
const owned = (mesh: MeshStore, name: string): boolean => fs.existsSync(path.join(mesh.root, name, "owner"));
// A complete, live owner receipt without an incarnation: never recoverable, like a running holder.
const holdAsLiveProcess = (mesh: MeshStore, name: string): void => {
  fs.mkdirSync(path.join(mesh.root, name), { mode: 0o700 });
  fs.writeFileSync(path.join(mesh.root, name, "owner"), `legacy\n${process.pid}\n${Date.now()}\n`);
};

// Real lock files with a deterministic retry clock: no wall-clock sleeps in convoy/deadline regressions.
const useLockClock = async (): Promise<void> => {
  await ownProcessIncarnation();
  vi.useFakeTimers({ toFake: ["Date", "performance", "setTimeout", "clearTimeout"] });
  vi.spyOn(Math, "random").mockReturnValue(0);
};

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("mesh file custody lock", () => {
  it("defaults to the transition-safe dual mode; only an explicit 'own' drops the mesh lock", () => {
    expect(meshCustodyMode(undefined)).toBe("dual");
    expect(meshCustodyMode("")).toBe("dual");
    expect(meshCustodyMode("mesh")).toBe("dual");
    expect(meshCustodyMode(" OWN ")).toBe("own");
  });

  it("dual: holds the custody lock and then the mesh lock, and releases both", async () => {
    vi.stubEnv("PI_FABRIC_MESH_CUSTODY_LOCK", "");
    const mesh = store();
    const seen = await mesh.custody(() => [owned(mesh, MESH_CUSTODY_LOCK_NAME), owned(mesh, ".lock")]);
    expect(seen).toEqual([true, true]);
    expect(fs.existsSync(path.join(mesh.root, MESH_CUSTODY_LOCK_NAME))).toBe(false);
    expect(fs.existsSync(path.join(mesh.root, ".lock"))).toBe(false);
  });

  it("dual: a pre-L5 process holding only the mesh lock still excludes custody (mixed release)", async () => {
    vi.stubEnv("PI_FABRIC_MESH_CUSTODY_LOCK", "dual");
    const mesh = store();
    holdAsLiveProcess(mesh, ".lock");
    const operation = vi.fn();
    await expect(mesh.custody(operation)).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    expect(operation).not.toHaveBeenCalled();
    // The custody lock taken first is released on the mesh timeout.
    expect(fs.existsSync(path.join(mesh.root, MESH_CUSTODY_LOCK_NAME))).toBe(false);
  });

  // Keep reason: recorded #19677 convoy; exercise real lock files and the custody -> mesh safety boundary.
  it("dual: a mesh-blocked custodian releases custody so a second custodian can finish first", async () => {
    vi.stubEnv("PI_FABRIC_MESH_CUSTODY_LOCK", "dual");
    const mesh = store();
    const other = new MeshStore(mesh.root, 64 * 1024, 500, { lockTimeoutMs: 300 });
    await useLockClock();
    holdAsLiveProcess(mesh, ".lock");
    const order: string[] = [];
    const operation = vi.fn(() => {
      expect([owned(mesh, MESH_CUSTODY_LOCK_NAME), owned(mesh, ".lock")]).toEqual([true, true]);
      order.push("first");
      return "first";
    });
    const queue = meshLockQueueDirectory(mesh.root);
    const readdir = vi.spyOn(fs, "readdirSync");
    const exclusive = vi.spyOn(mesh, "exclusive");
    const first = mesh.custody(operation);
    await vi.advanceTimersByTimeAsync(0);
    expect(operation).not.toHaveBeenCalled();
    // The inner zero-budget try refused, but the overall acquisition remains pending.
    expect(fs.existsSync(path.join(mesh.root, MESH_CUSTODY_LOCK_NAME))).toBe(false);
    expect(exclusive).toHaveBeenCalledTimes(1);
    expect(exclusive.mock.calls[0]![1]).toBe(0);
    expect(readdir.mock.calls.some(([directory]) => String(directory) === queue)).toBe(false);
    expect(mesh.tryLockBudgetMs).toBeUndefined();
    // The legacy mesh-only holder finishes during the first custodian's off-custody backoff.
    fs.rmSync(path.join(mesh.root, ".lock"), { recursive: true });
    await expect(other.custody(() => {
      expect([owned(mesh, MESH_CUSTODY_LOCK_NAME), owned(mesh, ".lock")]).toEqual([true, true]);
      order.push("second");
      return "second";
    })).resolves.toBe("second");
    expect(operation).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(5);
    await expect(first).resolves.toBe("first");
    expect(order).toEqual(["second", "first"]);
    expect(operation).toHaveBeenCalledTimes(1);
    expect(exclusive).toHaveBeenCalledTimes(2);
    expect(fs.existsSync(path.join(mesh.root, MESH_CUSTODY_LOCK_NAME))).toBe(false);
    expect(fs.existsSync(path.join(mesh.root, ".lock"))).toBe(false);
  });

  it.each([
    ["configured default", undefined, undefined, 300],
    ["explicit timeout", 45, undefined, 45],
    ["scoped try budget", undefined, 45, 45],
    ["smaller scoped budget", 90, 45, 45],
    ["smaller explicit timeout", 45, 90, 45],
  ] as const)("dual: %s bounds custody wait, inner attempts and backoff together", async (_label, timeoutMs, tryMs, budget) => {
    vi.stubEnv("PI_FABRIC_MESH_CUSTODY_LOCK", "dual");
    const mesh = store();
    await useLockClock();
    holdAsLiveProcess(mesh, MESH_CUSTODY_LOCK_NAME);
    holdAsLiveProcess(mesh, ".lock");
    // Spend part of the same budget on the outer custody acquisition, not just mesh retries.
    setTimeout(() => fs.rmSync(path.join(mesh.root, MESH_CUSTODY_LOCK_NAME), { recursive: true }), 15);
    const operation = vi.fn();
    const exclusive = vi.spyOn(mesh, "exclusive");
    let settled = false;
    const pending = tryMs === undefined ? mesh.custody(operation, timeoutMs)
      : mesh.withTryLock(() => mesh.custody(operation, timeoutMs), tryMs);
    const outcome = pending.then(value => { settled = true; return value; }, error => { settled = true; return error; });
    await vi.advanceTimersByTimeAsync(budget - 1);
    expect(settled).toBe(false);
    await vi.advanceTimersByTimeAsync(1);
    expect(await outcome).toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    expect(performance.now()).toBe(budget);
    expect(exclusive.mock.calls.length).toBeGreaterThan(1);
    expect(exclusive.mock.calls.every(([, wait]) => wait === 0)).toBe(true);
    expect(operation).not.toHaveBeenCalled();
    expect(fs.existsSync(path.join(mesh.root, MESH_CUSTODY_LOCK_NAME))).toBe(false);
  });

  it.each(["explicit", "scoped"] as const)("dual: a zero %s budget tries once without retrying", async kind => {
    vi.stubEnv("PI_FABRIC_MESH_CUSTODY_LOCK", "dual");
    const mesh = store();
    await useLockClock();
    const operation = vi.fn(() => "entered");
    const acquire = (): Promise<string> => kind === "explicit" ? mesh.custody(operation, 0)
      : mesh.withTryLock(() => mesh.custody(operation), 0);
    const exclusive = vi.spyOn(mesh, "exclusive");
    await expect(acquire()).resolves.toBe("entered");
    expect(operation).toHaveBeenCalledTimes(1);
    operation.mockClear();
    exclusive.mockClear();
    holdAsLiveProcess(mesh, ".lock");
    await expect(acquire()).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    expect(exclusive).toHaveBeenCalledTimes(1);
    expect(exclusive.mock.calls[0]![1]).toBe(0);
    expect(operation).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
    expect(fs.existsSync(path.join(mesh.root, MESH_CUSTODY_LOCK_NAME))).toBe(false);
  });

  it("dual: retries a pre-operation incarnation refusal only after releasing custody", async () => {
    vi.stubEnv("PI_FABRIC_MESH_CUSTODY_LOCK", "dual");
    const mesh = store();
    await useLockClock();
    const exclusive = vi.spyOn(mesh, "exclusive").mockRejectedValueOnce(new MeshIncarnationPendingError());
    const operation = vi.fn(() => "entered");
    const pending = mesh.custody(operation, 100);
    await vi.advanceTimersByTimeAsync(0);
    expect(fs.existsSync(path.join(mesh.root, MESH_CUSTODY_LOCK_NAME))).toBe(false);
    expect(operation).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(5);
    await expect(pending).resolves.toBe("entered");
    expect(exclusive).toHaveBeenCalledTimes(2);
    expect(operation).toHaveBeenCalledTimes(1);
  });

  it.each([
    new Error("callback failed"),
    new MeshLockTimeoutError("callback", 1, 0),
    new MeshIncarnationPendingError(),
  ])("dual: never replays a callback that throws $name", async error => {
    vi.stubEnv("PI_FABRIC_MESH_CUSTODY_LOCK", "dual");
    const mesh = store();
    const exclusive = vi.spyOn(mesh, "exclusive");
    const operation = vi.fn(() => { throw error; });
    await expect(mesh.custody(operation, 100)).rejects.toBe(error);
    expect(operation).toHaveBeenCalledTimes(1);
    expect(exclusive).toHaveBeenCalledTimes(1);
    expect(fs.existsSync(path.join(mesh.root, MESH_CUSTODY_LOCK_NAME))).toBe(false);
    expect(fs.existsSync(path.join(mesh.root, ".lock"))).toBe(false);
  });

  it.each([
    new MeshLockForeignHolderError("foreign", 1, 0, "foreign", ""),
    new MeshCustodyUnrecoverableError("custody.lock"),
    Object.assign(new MeshLockTimeoutError("nonretryable", 1, 0), { retryable: false }),
    new Error("unrelated pre-operation failure"),
  ])("dual: surfaces nonretryable pre-operation $name without retrying", async error => {
    vi.stubEnv("PI_FABRIC_MESH_CUSTODY_LOCK", "dual");
    const mesh = store();
    const exclusive = vi.spyOn(mesh, "exclusive").mockRejectedValue(error);
    const operation = vi.fn();
    await expect(mesh.custody(operation, 100)).rejects.toBe(error);
    expect(exclusive).toHaveBeenCalledTimes(1);
    expect(operation).not.toHaveBeenCalled();
    expect(fs.existsSync(path.join(mesh.root, MESH_CUSTODY_LOCK_NAME))).toBe(false);
  });

  it("own: custody no longer blocks a concurrent mesh publish or state write", async () => {
    vi.stubEnv("PI_FABRIC_MESH_CUSTODY_LOCK", "own");
    const mesh = store();
    const other = new MeshStore(mesh.root, 64 * 1024, 500, { lockTimeoutMs: 300 });
    // A long custody holder (another process's inbox reroute, say) keeps custody...
    const release = await acquireMeshCustodyLock(mesh.root);
    try {
      const started = Date.now();
      // ...while publishes and state writes proceed on the mesh lock without waiting for it.
      await expect(other.publish({ topic: "fleet.work.test", kind: "probe", from, text: "not queued behind custody" }))
        .resolves.toMatchObject({ topic: "fleet.work.test" });
      await expect(other.put({ key: "custody/probe", value: 1, identity: from })).resolves.toMatchObject({ key: "custody/probe" });
      expect(Date.now() - started).toBeLessThan(5_000);
      // Custody itself stays exclusive.
      await expect(other.custody(() => "entered", 50)).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    } finally { release(); }
    // And a custody section no longer holds the mesh lock.
    await expect(other.custody(() => [owned(mesh, MESH_CUSTODY_LOCK_NAME), fs.existsSync(path.join(mesh.root, ".lock"))]))
      .resolves.toEqual([true, false]);
    expect(other.read({ topic: "fleet.work.test" })).toHaveLength(1);
  });

  it("dual: by contrast, a held mesh lock blocks custody while it would not block in own mode", async () => {
    const mesh = store();
    holdAsLiveProcess(mesh, ".lock");
    vi.stubEnv("PI_FABRIC_MESH_CUSTODY_LOCK", "own");
    await expect(mesh.custody(() => "own")).resolves.toBe("own");
    vi.stubEnv("PI_FABRIC_MESH_CUSTODY_LOCK", "dual");
    await expect(mesh.custody(() => "dual")).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
  });

  it("recovers a dead holder's custody lock at once, and keeps a live holder's", async () => {
    vi.stubEnv("PI_FABRIC_MESH_CUSTODY_LOCK", "own");
    const mesh = store();
    holdAsLiveProcess(mesh, MESH_CUSTODY_LOCK_NAME);
    await expect(mesh.custody(() => "live", 50)).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    fs.rmSync(path.join(mesh.root, MESH_CUSTODY_LOCK_NAME), { recursive: true });
    const dead = spawnSync(process.execPath, ["-e", ""]).pid;
    fs.mkdirSync(path.join(mesh.root, MESH_CUSTODY_LOCK_NAME), { mode: 0o700 });
    fs.writeFileSync(path.join(mesh.root, MESH_CUSTODY_LOCK_NAME, "owner"), `dead\n${dead}\n${Date.now()}\n`);
    await expect(mesh.custody(() => "recovered", 1_000)).resolves.toBe("recovered");
  });

  it("a registry-fenced withTryLock budget bounds the custody wait", async () => {
    vi.stubEnv("PI_FABRIC_MESH_CUSTODY_LOCK", "own");
    const mesh = store();
    const release = await acquireMeshCustodyLock(mesh.root);
    try {
      const started = Date.now();
      await expect(mesh.withTryLock(() => mesh.custody(() => "entered"), 0)).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
      expect(Date.now() - started).toBeLessThan(1_000);
    } finally { release(); }
  });
});
