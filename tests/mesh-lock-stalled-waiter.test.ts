// smarty-dev#10072: a mesh lock timeout whose waiter itself was not running (largest gap between attempts >= 1 s)
// carries stalled: true and says "waiter stalled", so the timeout meter can tell it from real contention.
import { spawn, type ChildProcess } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { MeshLockTimeoutError } from "../src/core/atomic-write.js";
import { MeshStore } from "../src/mesh/store.js";

const roots: string[] = [];
const stores: MeshStore[] = [];
const children: ChildProcess[] = [];
// Windows (pi-fabric#956): a killed child still holds state.db until it has exited, so rmSync gave EPERM. Wait for each
// child's exit (bounded), then remove with the repo's usual Windows-safe retries.
const exited = (child: ChildProcess): Promise<void> => new Promise<void>((resolve) => {
  if (child.exitCode !== null || child.signalCode !== null) return resolve();
  const timer = setTimeout(resolve, 5_000);
  child.once("exit", () => { clearTimeout(timer); resolve(); });
});
afterEach(async () => {
  const killed = children.splice(0);
  for (const child of killed) child.kill("SIGKILL");
  await Promise.all(killed.map(exited));
  for (const store of stores.splice(0)) store.closeState();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
});
const identity = { id: "session:stalled", name: "stalled", kind: "main" as const };
const sqliteStore = async (lockTimeoutMs: number) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-lock-waiter-"));
  roots.push(root);
  const store = new MeshStore(root, 65536, 1000, { stateBackend: "sqlite", lockTimeoutMs });
  stores.push(store);
  await store.put({ key: "probe/open", value: 0, identity }); // state.db exists before anyone holds it
  return store;
};
const timeoutOf = async (work: Promise<unknown>): Promise<MeshLockTimeoutError> => {
  const error = await work.then(() => undefined, (failure: unknown) => failure);
  expect(error).toBeInstanceOf(MeshLockTimeoutError);
  return error as MeshLockTimeoutError;
};

describe("mesh lock timeout: a stalled waiter is not contention (smarty-dev#10072)", () => {
  it("the waiter's event loop is blocked for about 2 s inside the wait: stalled, and the message says so", async () => {
    const store = await sqliteStore(3_000);
    // Another connection holds state.db's write lock for the whole wait.
    const { DatabaseSync } = await import("node:sqlite");
    const holder = new DatabaseSync(path.join(store.root, "state.db"));
    holder.exec("BEGIN IMMEDIATE");
    try {
      // 100 ms into the wait, the waiter's own event loop stops for 2 s (a paging or CPU stall).
      setTimeout(() => Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 2_000), 100);
      const error = await timeoutOf(store.put({ key: "probe/blocked", value: 1, identity }));
      console.log(JSON.stringify({ case: "blocked", stalled: error.stalled, maxGapMs: error.maxGapMs, message: error.message }));
      expect(error.maxGapMs).toBeGreaterThanOrEqual(1_000);
      expect(error.stalled).toBe(true);
      expect(error.message).toContain(`waiter stalled ${error.maxGapMs} ms`);
    } finally {
      holder.exec("ROLLBACK");
      holder.close();
    }
  }, 20_000);

  it("a second process holds the lock: a plain timeout, not stalled", async () => {
    const store = await sqliteStore(1_500);
    const child = spawn(process.execPath, ["-e", `
      const { DatabaseSync } = require("node:sqlite");
      const db = new DatabaseSync(${JSON.stringify(path.join(store.root, "state.db"))});
      db.exec("BEGIN IMMEDIATE");
      process.stdout.write("held\\n");
      setTimeout(() => {}, 30_000);
    `], { stdio: ["ignore", "pipe", "inherit"] });
    children.push(child);
    await new Promise<void>((resolve, reject) => {
      child.stdout!.once("data", () => resolve());
      child.once("exit", code => reject(new Error(`holder exited ${code}`)));
    });
    const error = await timeoutOf(store.put({ key: "probe/contended", value: 1, identity }));
    console.log(JSON.stringify({ case: "contended", stalled: error.stalled, maxGapMs: error.maxGapMs, message: error.message }));
    expect(error.maxGapMs).toBeLessThan(1_000);
    expect(error.stalled).toBe(false);
    expect(error.message).not.toContain("stalled"); // the temp root's name avoids the word too
  }, 20_000);
});
