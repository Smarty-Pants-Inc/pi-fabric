import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { MeshStore } from "../src/mesh/store.js";

const roots: string[] = [];
const createStore = (lockProtocol: 1 | 2) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-try-lock-")); roots.push(root);
  return new MeshStore(root, 65536, 100, { lockProtocol, lockTimeoutMs: 2000 });
};
const hold = (store: MeshStore) => {
  const lock = path.join(store.root, ".lock");
  fs.mkdirSync(lock); fs.writeFileSync(path.join(lock, "owner"), `live-holder\n${process.pid}\n${Date.now()}\n`);
  return () => fs.rmSync(lock, { recursive: true, force: true });
};
const deferred = () => {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
};
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });

describe("registry-fenced mesh try scope", () => {
  it.each([1, 2] as const)("bounds writes, confirmation, recovery, and nested helpers (protocol=%s)", async protocol => {
    const store = createStore(protocol), release = hold(store);
    try {
      await store.withTryLock(async () => {
        await expect(store.put({ key: "pending", value: true, identity: { id: "test", name: "test", kind: "main" } }))
          .rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
        await expect(store.confirmWritable()).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
        // A nested helper cannot widen the registry-fenced caller's zero-wait budget.
        await expect(store.withTryLock(() => store.exclusive(() => "never", 1000), 1000))
          .rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
      });
      expect(store.get("pending")).toBeUndefined();
    } finally { release(); }
    expect(await store.withTryLock(() => store.exclusive(() => "acquired"))).toBe("acquired");
  });

  it("leaves concurrent ordinary acquisitions on their own wait budget", async () => {
    const store = createStore(1), release = hold(store), entered = deferred(), leave = deferred();
    const scoped = store.withTryLock(async () => {
      entered.resolve(); await leave.promise;
      await expect(store.confirmWritable()).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    });
    await entered.promise;
    // Started outside the async-local scope, while that scope is still active.
    const ordinary = store.exclusive(() => "ordinary");
    leave.resolve(); await scoped;
    release();
    expect(await ordinary).toBe("ordinary");
  });

  it.each([false, true])("clears the shared receipt for escaped async work on exit (throw=%s)", async throws => {
    const store = createStore(1), release = hold(store), gate = deferred();
    let escaped!: Promise<string>;
    const scope = store.withTryLock(async () => {
      escaped = gate.promise.then(() => store.exclusive(() => "escaped"));
      if (throws) throw new Error("operation failed");
    });
    if (throws) await expect(scope).rejects.toThrow("operation failed"); else await scope;
    gate.resolve();
    const timer = setTimeout(release, 100);
    try { expect(await escaped).toBe("escaped"); } finally { clearTimeout(timer); release(); }
  });
});
