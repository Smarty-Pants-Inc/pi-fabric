import { spawn, type ChildProcess } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { spawnDetached } from "../src/agents/transports/process-utils.js";

// Preserve native spawn, but capture its exact ChildProcess before it can close.
vi.mock("node:child_process", async (importOriginal) => {
  const actual = await importOriginal<typeof import("node:child_process")>();
  return { ...actual, spawn: vi.fn(actual.spawn) };
});

type OwnedChild = {
  child: Pick<ChildProcess, "exitCode" | "signalCode" | "kill">;
  closed: Promise<void>;
};

async function cleanupOwnedRoot(root: string, children: OwnedChild[]) {
  for (const { child, closed } of children) {
    // Use the owned native instance, never the handle's artificial death latch
    // or a numeric process/group id that could have been reused after exit.
    if (child.exitCode === null && child.signalCode === null) child.kill("SIGTERM");
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([
        closed,
        new Promise<never>((_, reject) => {
          timer = setTimeout(() => reject(new Error("Owned worker did not close within 10s")), 10_000);
        }),
      ]);
    } finally {
      clearTimeout(timer);
    }
  }
  // Windows directory handles can briefly lag native close. Retry only our
  // mkdtemp root; a persistent EBUSY still fails, and no shared TMP is removed.
  fs.rmSync(root, { recursive: true, force: true, maxRetries: 3, retryDelay: 100 });
}

async function withOwnedWorker(
  source: string,
  run: (handle: Awaited<ReturnType<typeof spawnDetached>>, root: string, child: ChildProcess) => Promise<void>,
) {
  const actual = await vi.importActual<typeof import("node:child_process")>("node:child_process");
  const children: OwnedChild[] = [];
  vi.mocked(spawn).mockImplementation((...args: Parameters<typeof spawn>) => {
    const child = actual.spawn(...args);
    const closed = new Promise<void>((resolve) => child.once("close", () => resolve()));
    children.push({ child, closed });
    return child;
  });
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-spawn-"));
  try {
    const worker = path.join(root, "worker.mjs");
    fs.writeFileSync(worker, source);
    const handle = await spawnDetached(worker, [], root);
    await run(handle, root, children[0]!.child as ChildProcess);
  } finally {
    // Assertion failures must not leave a native worker or a mocked kill behind.
    vi.restoreAllMocks();
    vi.mocked(spawn).mockImplementation(actual.spawn);
    await cleanupOwnedRoot(root, children);
  }
}

afterEach(() => vi.restoreAllMocks());

describe("spawnDetached", () => {
  // dev-lead review D7 on #26: after the worker exited, its numeric id may name an
  // unrelated process (group); stop and liveness must not act on it.
  it("neither signals nor reports alive a worker id after the worker exited, even if the id is reused", async () => {
    await withOwnedWorker("process.exit(0);\n", async (handle) => {
      await vi.waitFor(async () => expect(await handle.isAlive()).toBe(false), { timeout: 10_000, interval: 20 });
      // The id is now reused by an unrelated process: every numeric probe or signal succeeds.
      // (The first false above may come from the probe before the "exit" event; both must latch.)
      const kill = vi.spyOn(process, "kill").mockImplementation(() => true);
      expect(await handle.isAlive()).toBe(false);
      await handle.stop();
      expect(kill).not.toHaveBeenCalled();
    });
  });

  it("latches a worker's exit seen by the probe before its exit event", async () => {
    await withOwnedWorker("setInterval(() => {}, 1_000);\n", async (handle, _root, child) => {
      // The probe reports the worker gone while its native child is still alive.
      const kill = vi.spyOn(process, "kill").mockImplementationOnce(() => {
        throw Object.assign(new Error("no such process"), { code: "ESRCH" });
      });
      expect(await handle.isAlive()).toBe(false);
      expect(child.exitCode).toBeNull();
      expect(child.signalCode).toBeNull();
      kill.mockImplementation(() => true); // the number now names another process
      expect(await handle.isAlive()).toBe(false);
      await handle.stop();
      expect(kill).toHaveBeenCalledTimes(1); // only the first probe; no signal
    });
  });

  it("stops a worker that is still running", async () => {
    await withOwnedWorker("setInterval(() => {}, 1_000);\n", async (handle) => {
      expect(await handle.isAlive()).toBe(true);
      await handle.stop();
      await vi.waitFor(async () => expect(await handle.isAlive()).toBe(false), { timeout: 10_000, interval: 20 });
    });
  });

  it("does not remove its owned root until close, even after native exit", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-spawn-"));
    let releaseClose!: () => void;
    const closed = new Promise<void>((resolve) => { releaseClose = resolve; });
    const child = { exitCode: 0, signalCode: null, kill: vi.fn(() => true) };
    const remove = vi.spyOn(fs, "rmSync");
    const cleanup = cleanupOwnedRoot(root, [{ child, closed }]);
    try {
      await Promise.resolve();
      expect(remove).not.toHaveBeenCalled();
      expect(fs.existsSync(root)).toBe(true);
      expect(child.kill).not.toHaveBeenCalled(); // exited instances must not signal reused ids
      releaseClose();
      await cleanup;
      expect(remove).toHaveBeenCalledExactlyOnceWith(root, {
        recursive: true, force: true, maxRetries: 3, retryDelay: 100,
      });
      expect(fs.existsSync(root)).toBe(false);
    } finally {
      releaseClose();
      await cleanup;
      remove.mockRestore();
    }
  });

  it("closes the native worker and removes only its root when an assertion fails", async () => {
    let ownedRoot = "";
    let nativeChild: ChildProcess | undefined;
    let nativeClosed = false;
    await expect(withOwnedWorker("setInterval(() => {}, 1_000);\n", async (_handle, root, child) => {
      ownedRoot = root;
      nativeChild = child;
      child.once("close", () => { nativeClosed = true; });
      vi.spyOn(process, "kill").mockImplementation(() => true);
      expect("deliberate fixture assertion failure").toBe("success");
    })).rejects.toThrow("deliberate fixture assertion failure");
    expect(nativeClosed).toBe(true);
    expect(nativeChild!.exitCode !== null || nativeChild!.signalCode !== null).toBe(true);
    expect(fs.existsSync(ownedRoot)).toBe(false);
    expect(vi.isMockFunction(process.kill)).toBe(false);
    expect(fs.existsSync(os.tmpdir())).toBe(true);
  });
});
