import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore } from "../src/mesh/store.js";
const roots: string[] = [];
afterEach(() => { vi.useRealTimers(); for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
const pause = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
const fixture = (lockProtocol: 1 | 2) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-try-scope-")); roots.push(root);
  const mesh = new MeshStore(root, 65536, 100, { lockProtocol, lockTimeoutMs: 2000 });
  const lock = path.join(root, ".lock"); fs.mkdirSync(lock);
  fs.writeFileSync(path.join(lock, "owner"), `external\n${process.pid}\n${Date.now()}\n`);
  const release = () => fs.rmSync(lock, { recursive: true, force: true });
  return { mesh, release };
};
const identity = { id: "test", name: "test", kind: "agent" as const };

describe("mesh try-lock async scope", () => {
  it.each([1, 2] as const)("bounds every writer without changing concurrent ordinary callers (protocol=%s)", async protocol => {
    const { mesh, release } = fixture(protocol);
    const ordinary = mesh.put({ key: "ordinary", value: 1, identity });
    try {
      const start = performance.now();
      await mesh.withTryLock(async () => {
        for (const write of [
          () => mesh.exclusive(() => { throw new Error("must not enter"); }),
          () => mesh.confirmWritable(),
          () => mesh.put({ key: "try-put", value: 1, identity }),
          () => mesh.delete({ key: "try-delete" }),
          () => mesh.writeBatch({ identity, ops: [{ kind: "put", key: "try-batch", value: 1 }] }),
          () => mesh.publish({ topic: "try-publish", from: identity, text: "must not land" }),
        ]) await expect(write()).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
        await expect(mesh.withTryLock(() => mesh.confirmWritable())).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
      });
      expect(performance.now() - start).toBeLessThan(500);
      release(); await ordinary;
      expect(mesh.listAll().map(entry => entry.key)).toEqual(["ordinary"]);
      expect(mesh.read({ topic: "try-publish" })).toEqual([]);
    } finally { release(); await ordinary; }
  });

  it.each([1, 2] as const)("supports bounded publication tries without nested scope budget expansion (protocol=%s)", async protocol => {
    const { mesh, release } = fixture(protocol);
    try {
      const start = performance.now();
      await mesh.withTryLock(async () => {
        // A nested helper must not turn a 50 ms try into an ordinary 2 s wait.
        await expect(mesh.withTryLock(() => mesh.confirmWritable(), 2000)).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
      }, 50);
      expect(performance.now() - start).toBeLessThan(500);
      const unlock = pause(20).then(release);
      await mesh.withTryLock(() => mesh.confirmWritable(), 100);
      await unlock; // a transient collision may still succeed inside the bounded budget
    } finally { release(); }
  });

  it("restores the ordinary budget after rejection, success and escaped async work", async () => {
    const { mesh, release } = fixture(1);
    await expect(mesh.withTryLock(() => mesh.confirmWritable())).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    let resume!: () => void;
    const gate = new Promise<void>(resolve => { resume = resolve; });
    let escaped!: Promise<void>;
    await mesh.withTryLock(async () => { escaped = gate.then(() => mesh.confirmWritable()); });
    resume();
    const unlock = pause(100).then(release);
    try { await escaped; await unlock; }
    finally { release(); await escaped; await unlock; }
    await expect(mesh.withTryLock(() => mesh.withTryLock(() => mesh.confirmWritable()))).resolves.toBeUndefined();
    expect(fs.existsSync(path.join(mesh.root, ".lock"))).toBe(false);
  });

  it("cancels only the scoped waiter, leaving a concurrent ordinary writer live", async () => {
    const { mesh, release } = fixture(1);
    const abort = new AbortController(), reason = new Error("admission retired");
    const ordinary = mesh.put({ key: "ordinary", value: 1, identity });
    const cancelled = expect(mesh.withTryLock(() => mesh.confirmWritable(), 2_000, abort.signal)).rejects.toBe(reason);
    try {
      await pause(20); abort.abort(reason); await cancelled;
      expect(mesh.get("ordinary")).toBeUndefined();
      expect(fs.existsSync(path.join(mesh.root, ".lock"))).toBe(true);
      release(); await ordinary;
      expect(mesh.get("ordinary")?.value).toBe(1);
      await mesh.withTryLock(() => mesh.confirmWritable(), 100);
    } finally { release(); await ordinary; }
  });

  it("bounds acquisition by monotonic elapsed time despite a backwards wall-clock step", async () => {
    const { mesh, release } = fixture(1);
    vi.useFakeTimers();
    const start = performance.now();
    let spent = 0;
    const failed = expect(mesh.withTryLock(() => mesh.confirmWritable(), 1_000)
      .finally(() => { spent = performance.now() - start; })).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    try {
      await vi.advanceTimersByTimeAsync(100);
      vi.setSystemTime(Date.now() - 60_000);
      await vi.advanceTimersByTimeAsync(900); await failed;
      expect(spent).toBe(1_000);
      expect(fs.existsSync(path.join(mesh.root, ".lock"))).toBe(true);
    } finally { release(); }
  });
});
