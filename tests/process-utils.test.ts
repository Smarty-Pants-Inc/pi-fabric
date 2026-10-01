import { spawn, type ChildProcess } from "node:child_process";
import * as childProcess from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { spawnDetached } from "../src/agents/transports/process-utils.js";
import { same, startTime } from "./helpers/owned-processes.js";

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

  it("#2566 item 5 confirms exit before stop resolves when a worker delays SIGTERM", async () => {
    await withOwnedWorker(`import fs from "node:fs";
      process.on("SIGTERM", () => setTimeout(() => process.exit(0), 250));
      fs.writeFileSync("ready", "ready"); setInterval(() => {}, 1_000);`, async (handle, root, child) => {
      await vi.waitFor(() => expect(fs.existsSync(path.join(root, "ready"))).toBe(true));
      await handle.stop();
      expect(child.exitCode !== null || child.signalCode !== null).toBe(true);
      expect(await handle.isAlive()).toBe(false);
    });
  });

  it.skipIf(process.platform === "win32")("#2566 item 5 escalates a SIGTERM-refusing worker and confirms SIGKILL exit", async () => {
    await withOwnedWorker(`import fs from "node:fs";
      process.on("SIGTERM", () => {});
      fs.writeFileSync("ready", "ready"); setInterval(() => {}, 1_000);`, async (handle, root, child) => {
      await vi.waitFor(() => expect(fs.existsSync(path.join(root, "ready"))).toBe(true));
      try {
        await handle.stop();
        expect(child.signalCode).toBe("SIGKILL");
        expect(await handle.isAlive()).toBe(false);
      } finally {
        // Baseline's stop does not escalate: clean only the captured native child.
        if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
      }
    });
  });

  it.skipIf(process.platform !== "linux")("#2566 item 5 retains an observed refusing child after its launcher exits before stop", async () => {
    await withOwnedWorker(`import { spawn } from "node:child_process"; import fs from "node:fs";
      const child = spawn(process.execPath, ["-e", 'const fs=require("node:fs"); process.on("SIGTERM",()=>{}); fs.writeFileSync("child-ready","ready"); setInterval(()=>{},1000);'], { stdio: "ignore" });
      fs.writeFileSync("child.pid", String(child.pid));
      setInterval(() => { if (fs.existsSync("exit-now")) process.exit(0); }, 20);`, async (handle, root, child) => {
      await vi.waitFor(() => expect(fs.existsSync(path.join(root, "child-ready"))).toBe(true));
      const pid = Number(fs.readFileSync(path.join(root, "child.pid"), "utf8"));
      const owned = { pid, started: startTime(pid) };
      expect(owned.started).not.toBe("");
      const executing = () => {
        if (!same(owned)) return false;
        const stat = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
        return !["Z", "X"].includes(stat.slice(stat.lastIndexOf(")") + 2).split(" ")[0]!);
      };
      try {
        expect(await handle.isAlive()).toBe(true); // capture the child's birth while leader lives
        fs.writeFileSync(path.join(root, "exit-now"), "exit");
        await vi.waitFor(() => expect(child.exitCode).toBe(0));
        await handle.stop();
        expect(executing()).toBe(false);
      } finally {
        if (executing() && same(owned)) process.kill(pid, "SIGKILL");
        await vi.waitFor(() => expect(executing()).toBe(false));
      }
    });
  });

  it.skipIf(process.platform !== "linux")("#2566 item 5 never escalates a group whose birth identity changed after TERM", async () => {
    await withOwnedWorker("setInterval(() => {}, 1000);", async (handle, _root, child) => {
      const nativeRead = fs.readFileSync.bind(fs);
      let recycled = false;
      const read = vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, options?: unknown) => {
        const value = nativeRead(file, options as never);
        if (recycled && String(file) === `/proc/${handle.pid}/stat`) {
          const stat = String(value); const split = stat.lastIndexOf(")") + 2;
          const fields = stat.slice(split).trim().split(/\s+/);
          fields[19] = String(BigInt(fields[19]!) + 1n);
          return stat.slice(0, split) + fields.join(" ");
        }
        return value;
      }) as typeof fs.readFileSync);
      const kill = vi.spyOn(process, "kill").mockImplementation((_pid, signal) => {
        if (signal === "SIGTERM") recycled = true;
        return true;
      });
      try {
        await expect(handle.stop()).rejects.toThrow(/ownership\/exit/);
        expect(kill.mock.calls.map((call) => call[1])).toEqual(["SIGTERM"]);
      } finally {
        read.mockRestore(); kill.mockRestore();
        if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
      }
    });
  });

  it.skipIf(process.platform !== "linux")("#2566 item 5 rejects a bounded cleanup that cannot verify exit after SIGKILL", async () => {
    await withOwnedWorker("setInterval(() => {}, 1000);", async (handle, _root, child) => {
      const kill = vi.spyOn(process, "kill").mockImplementation(() => true); // kernel accepts signals; worker remains live
      vi.useFakeTimers();
      try {
        const stopping = handle.stop();
        const outcome = expect(stopping).rejects.toThrow(/did not exit after bounded SIGTERM\/SIGKILL/);
        void outcome.catch(() => undefined); // baseline can settle immediately; still await the assertion below
        await vi.advanceTimersByTimeAsync(4_100);
        await outcome;
        expect(kill.mock.calls.map((call) => call[1])).toEqual(["SIGTERM", "SIGKILL"]);
      } finally {
        vi.useRealTimers(); kill.mockRestore();
        if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
      }
    });
  });

  it.skipIf(process.platform !== "linux")("#2566 item 5 confirms an injected non-Linux refusing group exits before stop returns", async () => {
    vi.spyOn(process, "platform", "get").mockReturnValue("darwin");
    await withOwnedWorker(`import fs from "node:fs";
      process.on("SIGTERM", () => {}); fs.writeFileSync("ready", "ready"); setInterval(() => {}, 1000);`, async (handle, root, child) => {
      await vi.waitFor(() => expect(fs.existsSync(path.join(root, "ready"))).toBe(true));
      try {
        await handle.stop();
        expect(child.signalCode).toBe("SIGKILL");
        expect(await handle.isAlive()).toBe(false);
      } finally { if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL"); }
    });
  });

  it.skipIf(process.platform !== "linux")("#2566 item 5 refuses uncertain injected non-Linux descendants without signalling a gone leader number", async () => {
    vi.spyOn(process, "platform", "get").mockReturnValue("darwin");
    await withOwnedWorker("process.exit(0);", async (handle, _root, child) => {
      await vi.waitFor(() => expect(child.exitCode).toBe(0));
      const ps = vi.spyOn(childProcess, "execFile").mockImplementation(((...args: unknown[]) => {
        const callback = args.at(-1) as (error: null, stdout: string, stderr: string) => void;
        callback(null, `${handle.pid + 1} 0 ${handle.pid} S\n`, "");
        return child;
      }) as typeof childProcess.execFile);
      const kill = vi.spyOn(process, "kill").mockImplementation(() => true);
      vi.useFakeTimers();
      try {
        const assertion = expect(handle.stop()).rejects.toThrow(/did not exit after bounded/);
        void assertion.catch(() => undefined);
        await vi.advanceTimersByTimeAsync(4_100);
        await assertion;
        expect(ps).toHaveBeenCalled();
        expect(kill).not.toHaveBeenCalled();
      } finally { vi.useRealTimers(); ps.mockRestore(); kill.mockRestore(); }
    });
  });

  it.skipIf(process.platform !== "linux")("F1 keeps liveness custody of an observed detached execution after its custodian exits", async () => {
    await withOwnedWorker(`import { spawn } from "node:child_process"; import fs from "node:fs";
      const child = spawn(process.execPath, ["-e", 'const fs=require("node:fs"); process.on("SIGTERM",()=>{}); fs.writeFileSync("detached-ready","ready"); setInterval(()=>{},1000);'], { detached: true, stdio: "ignore" });
      fs.writeFileSync("detached.pid", String(child.pid));
      setInterval(() => { if (fs.existsSync("exit-now")) process.exit(0); }, 20);`, async (handle, root, custodian) => {
      await vi.waitFor(() => expect(fs.existsSync(path.join(root, "detached-ready"))).toBe(true));
      const pid = Number(fs.readFileSync(path.join(root, "detached.pid"), "utf8"));
      const owned = { pid, started: startTime(pid) };
      const live = () => same(owned) && !["Z", "X"].includes(fs.readFileSync(`/proc/${pid}/stat`, "utf8").split(") ")[1]!.split(" ")[0]!);
      try {
        expect(await handle.isAlive()).toBe(true); // birth anchored before reparenting
        fs.writeFileSync(path.join(root, "exit-now"), "exit");
        await vi.waitFor(() => expect(custodian.exitCode).toBe(0));
        expect(await handle.isAlive(), "dead worker alone must not authorize relaunch").toBe(true);
        await handle.stop();
        expect(live()).toBe(false);
        expect(await handle.isAlive()).toBe(false);
      } finally {
        if (live() && same(owned)) process.kill(pid, "SIGKILL");
        await vi.waitFor(() => expect(live()).toBe(false));
      }
    });
  });

  it.skipIf(process.platform !== "linux")("F1 injected non-Linux detached execution prevents killing its custodian without birth identity", async () => {
    await withOwnedWorker(`import { spawn } from "node:child_process"; import fs from "node:fs";
      process.on("SIGTERM", () => {});
      const child = spawn(process.execPath, ["-e", 'const fs=require("node:fs"); process.on("SIGTERM",()=>{}); fs.writeFileSync("portable-ready","ready"); setInterval(()=>{},1000);'], { detached: true, stdio: "ignore" });
      fs.writeFileSync("portable.pid", String(child.pid)); setInterval(()=>{},1000);`, async (handle, root, custodian) => {
      await vi.waitFor(() => expect(fs.existsSync(path.join(root, "portable-ready"))).toBe(true));
      const pid = Number(fs.readFileSync(path.join(root, "portable.pid"), "utf8"));
      const owned = { pid, started: startTime(pid) };
      const platform = vi.spyOn(process, "platform", "get").mockReturnValue("darwin");
      const kill = vi.spyOn(process, "kill");
      try {
        await expect(handle.stop()).rejects.toThrow(/execution exit unconfirmed/);
        expect(kill.mock.calls.filter(call => call[1] === "SIGKILL")).toEqual([]);
        expect(custodian.exitCode).toBeNull();
        expect(custodian.signalCode).toBeNull();
      } finally {
        platform.mockRestore(); kill.mockRestore();
        if (same(owned)) process.kill(pid, "SIGKILL");
        custodian.kill("SIGKILL");
      }
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
