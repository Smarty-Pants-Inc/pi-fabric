import { spawn, type ChildProcess } from "node:child_process";
import { EventEmitter } from "node:events";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { processIsAlive, spawnDetached } from "../src/agents/transports/process-utils.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { taskReturnAddressArguments } from "../src/agents/task-return-address.js";

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
  run: (handle: Pick<Awaited<ReturnType<typeof spawnDetached>>, "pid" | "stop" | "isAlive" | "lostContact">, root: string, child: ChildProcess) => Promise<void>,
  workerArguments?: string[],
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
    const launched = workerArguments === undefined ? undefined : await new ProcessTransport().launch({
      id: "role-test", name: "role-test", cwd: root, workerPath: worker,
      workerArguments: [...workerArguments, "--status-file", path.join(root, "status.json")],
    });
    const handle = launched ? { ...launched, pid: Number(launched.sessionId), lostContact: () => launched.lostContact?.() } : await spawnDetached(worker, [], root);
    await run(handle, root, children[0]!.child as ChildProcess);
  } finally {
    // Assertion failures must not leave a native worker or a mocked kill behind.
    vi.restoreAllMocks();
    vi.mocked(spawn).mockImplementation(actual.spawn);
    await cleanupOwnedRoot(root, children);
  }
}

afterEach(async () => {
  vi.restoreAllMocks(); vi.unstubAllEnvs();
  vi.mocked(spawn).mockImplementation((await vi.importActual<typeof import("node:child_process")>("node:child_process")).spawn);
});

describe("process task role environment (#2998)", () => {
  it.each(["worktree-agent@0123456789ab", undefined])("sets the worker role before exec from parent %s and strips spawner role overrides", async (parentRole) => {
    vi.stubEnv("SMARTY_ROLE", parentRole);
    vi.stubEnv("PI_FABRIC_ROLE", "worktree-agent");
    vi.stubEnv("SMARTY_READ_CLASS", "critical");
    vi.stubEnv("PI_FABRIC_ACTOR_NAME", "parent-actor");
    vi.stubEnv("PI_FABRIC_MAIN_AGENT_ID", "session:parent");
    vi.stubEnv("PI_FABRIC_SESSION_ID", "parent-session");
    const expectedRole = parentRole ? "task-agent@0123456789ab" : "task-agent";
    const source = `import fs from "node:fs";
fs.writeFileSync("env.json", JSON.stringify(Object.fromEntries(Object.entries(process.env).filter(([key]) => [
  "SMARTY_ROLE", "PI_FABRIC_ROLE", "SMARTY_READ_CLASS", "PI_FABRIC_ACTOR_NAME", "PI_FABRIC_MAIN_AGENT_ID", "PI_FABRIC_SESSION_ID"
].includes(key)))));
setInterval(() => {}, 1000);`;
    await withOwnedWorker(source, async (handle, root) => {
      await vi.waitFor(() => expect(fs.existsSync(path.join(root, "env.json"))).toBe(true));
      expect(JSON.parse(fs.readFileSync(path.join(root, "env.json"), "utf8"))).toEqual({
        SMARTY_ROLE: expectedRole, PI_FABRIC_ACTOR_NAME: "parent-actor",
        PI_FABRIC_MAIN_AGENT_ID: "session:parent", PI_FABRIC_SESSION_ID: "parent-session",
      });
      if (process.platform === "linux") {
        const entries = fs.readFileSync(`/proc/${handle.pid}/environ`, "utf8").split("\0");
        expect(entries).toContain(`SMARTY_ROLE=${expectedRole}`);
        expect(entries.some(entry => entry.startsWith("PI_FABRIC_ROLE=") || entry.startsWith("SMARTY_READ_CLASS="))).toBe(false);
      }
      expect(process.env.SMARTY_ROLE).toBe(parentRole);
      expect(process.env.PI_FABRIC_ROLE).toBe("worktree-agent");
      expect(process.env.SMARTY_READ_CLASS).toBe("critical");
    }, ["--name", "--actor-name"]); // A flag-shaped value is not an explicit actor.
  });

  it("injects the bound spawner and native session before the worker itself executes", async () => {
    const args = taskReturnAddressArguments("session:bound-parent", "parent-native-session", "session:bound-parent", {});
    await withOwnedWorker(`import fs from "node:fs";
fs.writeFileSync("binding.json", JSON.stringify({
  spawner: process.env.PI_FABRIC_SPAWNER_ID,
  session: process.env.PI_FABRIC_SPAWNER_SESSION_ID,
  chain: JSON.parse(process.env.PI_FABRIC_SPAWNER_CHAIN),
  targets: JSON.parse(process.env.PI_FABRIC_TASK_ESCALATION_TARGETS),
  processChild: process.env.PI_FABRIC_TASK_PROCESS_CHILD,
}));
setInterval(() => {}, 1000);`, async (handle, root) => {
      await vi.waitFor(() => expect(fs.existsSync(path.join(root, "binding.json"))).toBe(true));
      expect(JSON.parse(fs.readFileSync(path.join(root, "binding.json"), "utf8"))).toEqual({
        spawner: "session:bound-parent", session: "parent-native-session",
        chain: ["session:bound-parent"], targets: [], processChild: "1",
      });
      if (process.platform === "linux") {
        const entries = fs.readFileSync(`/proc/${handle.pid}/environ`, "utf8").split("\0");
        expect(entries).toContain("PI_FABRIC_SPAWNER_ID=session:bound-parent");
        expect(entries).toContain("PI_FABRIC_SPAWNER_SESSION_ID=parent-native-session");
      }
    }, args);
  });

  it("does not relabel generic detached launches such as a resident host", async () => {
    vi.stubEnv("SMARTY_ROLE", "project-agent@0123456789ab");
    vi.stubEnv("PI_FABRIC_ROLE", "project-agent");
    await withOwnedWorker(`import fs from "node:fs";
fs.writeFileSync("env.json", JSON.stringify({role:process.env.SMARTY_ROLE,override:process.env.PI_FABRIC_ROLE}));`, async (_handle, root) => {
      await vi.waitFor(() => expect(fs.existsSync(path.join(root, "env.json"))).toBe(true));
      expect(JSON.parse(fs.readFileSync(path.join(root, "env.json"), "utf8"))).toEqual({
        role: "project-agent@0123456789ab", override: "project-agent",
      });
    });
  });

  it("retains explicit actor role and attribution at the process boundary", async () => {
    vi.stubEnv("SMARTY_ROLE", "review-agent@0123456789ab");
    vi.stubEnv("PI_FABRIC_ROLE", "review-agent");
    vi.stubEnv("SMARTY_READ_CLASS", "critical");
    await withOwnedWorker(`import fs from "node:fs";
fs.writeFileSync("env.json", JSON.stringify({role:process.env.SMARTY_ROLE,override:process.env.PI_FABRIC_ROLE,readClass:process.env.SMARTY_READ_CLASS}));`, async (_handle, root) => {
      await vi.waitFor(() => expect(fs.existsSync(path.join(root, "env.json"))).toBe(true));
      expect(JSON.parse(fs.readFileSync(path.join(root, "env.json"), "utf8"))).toEqual({
        role: "review-agent@0123456789ab", override: "review-agent", readClass: "critical",
      });
    }, ["--actor-id", "actor", "--actor-name", "security-review"]);
  });
});

describe("process private scratch (#3076)", () => {
  it.each([{ args: [] }, { args: ["--actor-name", "scratch-review"] }])("overrides TMPDIR only in the task/actor worker environment (%j)", async ({ args }) => {
    const parentTmpdir = process.env.TMPDIR;
    const parentTmp = process.env.TMP;
    const parentTemp = process.env.TEMP;
    await withOwnedWorker(`import fs from "node:fs";
import os from "node:os";
fs.writeFileSync("env.json", JSON.stringify({tmpdir:process.env.TMPDIR,osTmpdir:os.tmpdir(),tmp:process.env.TMP,temp:process.env.TEMP,mode:fs.statSync(process.env.TMPDIR).mode & 0o777}));
setInterval(() => {}, 1000);`, async (_handle, root) => {
      await vi.waitFor(() => expect(fs.existsSync(path.join(root, "env.json"))).toBe(true));
      const report = JSON.parse(fs.readFileSync(path.join(root, "env.json"), "utf8"));
      expect(report.tmpdir).toBe(path.join(root, "tmp"));
      expect(report.osTmpdir).toBe(report.tmpdir);
      if (process.platform === "win32") expect([report.tmp, report.temp]).toEqual([report.tmpdir, report.tmpdir]);
      else expect(report.mode).toBe(0o700);
      expect(process.env.TMPDIR).toBe(parentTmpdir);
      expect(process.env.TMP).toBe(parentTmp);
      expect(process.env.TEMP).toBe(parentTemp);
    }, args);
  });
});

describe("spawnDetached", () => {
  it("retains the fixed scratch gate rather than migrating custody into a configured slice", async () => {
    const child = Object.assign(new EventEmitter(), { pid: 1234, unref: vi.fn() });
    vi.mocked(spawn).mockClear().mockReturnValueOnce(child as unknown as ChildProcess);
    const scratch = { directory: "/sys/fs/cgroup/private-test", dev: 1, ino: 1, bootId: "test", joinedFile: "joined", launchNonce: "nonce" };
    const warn = vi.fn();
    const handle = await spawnDetached("worker.mjs", [], process.cwd(), undefined, {}, { executable: "systemd-run", slice: "batch.slice", warn }, scratch);
    try {
      expect(warn).toHaveBeenCalledExactlyOnceWith("private scratch containment takes precedence over systemd slice placement");
      expect(spawn).toHaveBeenCalledExactlyOnceWith("/bin/sh", expect.arrayContaining(["-p", "pi-fabric-scratch-gate", scratch.directory]), expect.any(Object));
      const args = vi.mocked(spawn).mock.calls[0]![1] as string[];
      expect(args[2]).toContain('"$1/cgroup.procs"');
      expect(args).not.toContain("--scope");
    } finally {
      child.emit("exit", 0); child.emit("close", 0);
      await handle.waitForClose();
    }
  });
  it.each(["LD_PRELOAD", "LD_AUDIT", "LD_LIBRARY_PATH", "LD_ORIGIN_PATH", "GCONV_PATH"])("refuses a scoped launch before spawn when the explicit environment has %s", async key => {
    vi.mocked(spawn).mockClear();
    const scope = { directory: "unused", dev: 1, ino: 1, bootId: "unused", joinedFile: "unused", launchNonce: "unused" };
    await expect(spawnDetached("worker.mjs", [], process.cwd(), undefined, { ...process.env, [key]: "synthetic-preload" }, scope))
      .rejects.toMatchObject({ name: "WorkerNotStartedError", message: expect.stringContaining("launch gate has a loader hook") });
    expect(spawn).not.toHaveBeenCalled();
  });

  it.each(["confirmed", "timeout"] as const)("bounds the independent captured-close join (%s)", async outcome => {
    vi.useFakeTimers();
    const child = Object.assign(new EventEmitter(), { pid: 1234, unref: vi.fn() });
    vi.mocked(spawn).mockReturnValueOnce(child as unknown as ChildProcess);
    const unconfirmed = vi.fn();
    const handle = await spawnDetached("worker.mjs", [], process.cwd(), { onUnconfirmedExit: unconfirmed });
    const kill = vi.spyOn(process, "kill").mockImplementation(() => true);
    child.emit("exit", 0);
    let joined = false;
    const joining = handle.waitForClose().then(() => { joined = true; });
    try {
      await vi.advanceTimersByTimeAsync(6_999);
      expect(joined, "native exit is not captured close").toBe(false);
      expect(kill).not.toHaveBeenCalled();
      if (outcome === "confirmed") child.emit("close", 0);
      else await vi.advanceTimersByTimeAsync(1);
      await joining;
      expect(joined).toBe(true);
      expect(unconfirmed).toHaveBeenCalledTimes(outcome === "timeout" ? 1 : 0);
      expect(handle.lostContact() !== undefined).toBe(outcome === "timeout");
      child.emit("close", 0);
      await handle.waitForClose();
      expect(handle.lostContact() !== undefined, "late close cannot erase persistent uncertainty").toBe(outcome === "timeout");
      expect(vi.getTimerCount()).toBe(0);
    } finally {
      child.emit("close", 0);
      await joining;
      vi.useRealTimers();
    }
  });


  it("joins native close even after exit, without signalling a reused PID", async () => {
    const child = Object.assign(new EventEmitter(), { pid: 1234, unref: vi.fn() });
    vi.mocked(spawn).mockReturnValueOnce(child as unknown as ChildProcess);
    const handle = await spawnDetached("worker.mjs", [], process.cwd());
    child.emit("exit", 0);
    const kill = vi.spyOn(process, "kill").mockImplementation(() => true);
    let stopped = false;
    const stopping = handle.stop().then(() => { stopped = true; });
    await Promise.resolve();
    expect(stopped).toBe(false);
    expect(kill).not.toHaveBeenCalled();
    child.emit("close", 0);
    await stopping;
    expect(stopped).toBe(true);
  });

  it.each(["tree-first", "worker-first"])("Windows stop joins both the owned tree helper and worker close (%s)", async order => {
    const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
    const child = Object.assign(new EventEmitter(), { pid: 1234, unref: vi.fn(), kill: vi.fn() });
    const killer = Object.assign(new EventEmitter(), { pid: 5678, kill: vi.fn() });
    vi.mocked(spawn).mockReturnValueOnce(child as unknown as ChildProcess).mockReturnValueOnce(killer as unknown as ChildProcess);
    Object.defineProperty(process, "platform", { ...platform, value: "win32" });
    try {
      const handle = await spawnDetached("worker.mjs", [], process.cwd());
      const kill = vi.spyOn(process, "kill").mockImplementation(() => true);
      let stopped = false;
      const first = handle.stop();
      expect(handle.stop()).toBe(first); // one owned tree helper, not two
      const stopping = first.then(() => { stopped = true; });
      expect(spawn).toHaveBeenLastCalledWith("taskkill", ["/pid", "1234", "/T", "/F"], { windowsHide: true, stdio: "ignore" });
      expect(kill).not.toHaveBeenCalled(); // never force-kill only the parent first
      if (order === "tree-first") killer.emit("close", 0);
      else { child.emit("exit", null); child.emit("close", null); }
      await Promise.resolve(); await Promise.resolve();
      expect(stopped).toBe(false);
      if (order === "tree-first") { child.emit("exit", null); child.emit("close", null); }
      else killer.emit("close", 0);
      await stopping;
      expect(stopped).toBe(true);
      expect(await handle.isAlive()).toBe(false);
      expect(child.kill).not.toHaveBeenCalled();
    } finally {
      killer.emit("close", 0); child.emit("close", null);
      Object.defineProperty(process, "platform", platform);
    }
  });

  it("Windows failed helper close ends logical stop only after native close, with immutable debt", async () => {
    vi.useFakeTimers();
    vi.spyOn(process, "emitWarning").mockImplementation(() => {});
    const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
    const child = Object.assign(new EventEmitter(), { pid: 1234, unref: vi.fn(), kill: vi.fn() });
    const killer = Object.assign(new EventEmitter(), { pid: 5678, kill: vi.fn() });
    vi.mocked(spawn).mockReturnValueOnce(child as unknown as ChildProcess).mockReturnValueOnce(killer as unknown as ChildProcess);
    const debt = vi.fn();
    try {
      Object.defineProperty(process, "platform", { ...platform, value: "win32" });
      const handle = await spawnDetached("worker.mjs", [], process.cwd(), { onUnconfirmedExit: debt });
      let stopped = false;
      const stopping = handle.stop().then(() => { stopped = true; });
      killer.emit("error", new Error("failed attempt"));
      child.emit("exit", null); child.emit("close", null);
      await vi.advanceTimersByTimeAsync(0);
      expect(stopped).toBe(false); // Native close alone cannot skip helper custody.
      expect(debt).toHaveBeenCalledOnce();
      killer.emit("close", 1);
      await vi.advanceTimersByTimeAsync(0);
      await stopping; // No unnecessary seven-second delay after both closes.
      expect(stopped).toBe(true);
      const lost = handle.lostContact();
      expect(lost).toContain("Windows process tree termination is unconfirmed");
      killer.emit("close", 0);
      await handle.waitForClose();
      expect(handle.lostContact()).toBe(lost); // Late success cannot erase debt.
      expect(debt).toHaveBeenCalledOnce();
    } finally {
      killer.emit("close", 0); child.emit("close", null);
      Object.defineProperty(process, "platform", platform);
    }
  });

  it("stop returns only after the worker and its native child have exited", async () => {
    await withOwnedWorker(`import fs from "node:fs";
import { spawn } from "node:child_process";
const child = spawn(process.execPath, ["-e", "setInterval(() => {}, 1000)"], { stdio: "ignore" });
child.once("spawn", () => fs.writeFileSync("native-pid", String(child.pid)));
process.on("SIGTERM", () => {
  child.once("close", () => process.exit(0));
  child.kill("SIGTERM");
});`, async (handle, root, child) => {
      await vi.waitFor(() => expect(fs.existsSync(path.join(root, "native-pid"))).toBe(true), { timeout: 10_000 });
      const nativePid = Number(fs.readFileSync(path.join(root, "native-pid"), "utf8"));
      expect(processIsAlive(nativePid)).toBe(true);
      let closed = false;
      child.once("close", () => { closed = true; });
      await handle.stop();
      expect(closed).toBe(true);
      expect(processIsAlive(nativePid), "native child must not retain the worker cwd").toBe(false);
    });
  });
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
      let stopped = false;
      const stopping = handle.stop().then(() => { stopped = true; });
      await Promise.resolve();
      expect(stopped).toBe(false); // the false probe is not native close
      expect(kill).toHaveBeenCalledTimes(1); // only the first probe; no signal
      kill.mockRestore();
      child.kill("SIGTERM");
      await stopping;
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
