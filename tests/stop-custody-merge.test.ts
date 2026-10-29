import { execFileSync, spawn, type ChildProcess } from "node:child_process";
import { EventEmitter } from "node:events";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import * as worktreeCustody from "../src/agents/worktree-manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { spawnDetached } from "../src/agents/transports/process-utils.js";
import type { AgentTransportHandle, AgentTransportLaunch } from "../src/agents/types.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { privateDirTestHooks } from "../src/storage/private-dir.js";
import { hasUnresolvedWorker } from "../src/storage/retention.js";

// The transport-contract cases below mock process.platform to probe the manager's product branch. Run directories are still made on
// the real host, so the private-dir fallback must see the real platform: a mocked "linux" on a Windows runner
// would otherwise demand /proc/self/fd (pi-fabric#875).
const hostPlatform = process.platform;

vi.mock("node:child_process", async importOriginal => {
  const actual = await importOriginal<typeof import("node:child_process")>();
  return { ...actual, spawn: vi.fn(actual.spawn), execFileSync: vi.fn(actual.execFileSync) };
});

const roots: string[] = [];
const movedRoots = new Set<string>();
const securityContainers: string[] = [];
const rawExports: Array<{ receipt: string; before: unknown[] }> = [];
let stressPhases: { started: number; loopFinished?: number; closeFinished?: number } | undefined;
const inventory = (root: string): unknown[] => {
  const rows: unknown[] = [];
  const walk = (relative: string): void => {
    const file = path.join(root, relative), stat = fs.lstatSync(file, { bigint: true });
    expect(stat.isSymbolicLink()).toBe(false);
    expect(stat.isDirectory() || stat.isFile()).toBe(true);
    rows.push({ relative, dev: String(stat.dev), ino: String(stat.ino), directory: stat.isDirectory(),
      ...(stat.isFile() ? { bytes: fs.readFileSync(file).toString("base64") } : {}) });
    if (stat.isDirectory()) for (const name of fs.readdirSync(file).sort()) walk(path.join(relative, name));
  };
  walk(""); return rows;
};
const observeCustody = (): void => {
  movedRoots.clear(); securityContainers.length = 0; rawExports.length = 0; stressPhases = undefined;
  const retain = worktreeCustody.retainCleanupRoot;
  vi.spyOn(worktreeCustody, "retainCleanupRoot").mockImplementation((source, ...args) => {
    const before = inventory(source), moved = retain(source, ...args);
    expect(moved).toBeDefined();
    expect(inventory(moved!.destination)).toEqual(before);
    const prepared = JSON.parse(fs.readFileSync(path.join(path.dirname(moved!.receipt), "custody.prepared.json"), "utf8"));
    expect(prepared).toMatchObject({ id: args[1], source, destination: moved!.destination, state: "prepared" });
    expect(JSON.parse(fs.readFileSync(moved!.receipt, "utf8"))).toMatchObject({ ...prepared, time: expect.any(Number), state: "moved" });
    movedRoots.add(source);
    rawExports.push({ receipt: moved!.receipt, before });
    return moved;
  });
};
const recordSecuritySubprocess = async (): Promise<void> => {
  if (hostPlatform !== "win32") vi.stubEnv("SystemRoot", "C:\\Windows");
  const actual = await vi.importActual<typeof import("node:child_process")>("node:child_process");
  vi.mocked(execFileSync).mockImplementation(((file: string, args: string[], options: any) => {
    const env = options?.env;
    const container = env?.PI_FABRIC_CLEANUP_CONTAINER;
    if (!container) return actual.execFileSync(file, args, options);
    expect(path.win32.basename(file)).toBe("powershell.exe");
    const script = Buffer.from(args[4]!, "base64").toString("utf16le");
    expect(script).toContain("Get-Acl -LiteralPath $directory");
    expect(script).toContain("$item.PSIsContainer");
    expect(script).toContain("[System.IO.FileAttributes]::ReparsePoint");
    expect(script).toContain("$read.AreAccessRulesProtected");
    expect(script).toContain("$read.GetOwner([System.Security.Principal.SecurityIdentifier]).Value");
    expect(script).toContain("[Convert]::ToBase64String($a) -cne [Convert]::ToBase64String($e)");
    let protectedPaths: unknown[];
    if (env.PI_FABRIC_CLEANUP_BATCH === "1") {
      const encoded = options?.input;
      expect(typeof encoded).toBe("string");
      if (typeof encoded !== "string") throw new Error("Cleanup batch stdin must be ASCII base64");
      expect(encoded).toMatch(/^[A-Za-z0-9+/]+={0,2}$/);
      const bytes = Buffer.from(encoded, "base64");
      expect(bytes.toString("base64")).toBe(encoded);
      const json = bytes.toString("utf8");
      expect(Buffer.from(json, "utf8").equals(bytes)).toBe(true);
      const decoded: unknown = JSON.parse(json);
      expect(Array.isArray(decoded)).toBe(true);
      if (!Array.isArray(decoded)) throw new Error("Cleanup batch stdin must decode to an array");
      expect(decoded.length).toBeGreaterThan(1);
      expect(decoded.every((entry: unknown) => typeof entry === "string")).toBe(true);
      if (!decoded.every((entry: unknown) => typeof entry === "string")) {
        throw new Error("Cleanup batch stdin must decode to a string array");
      }
      protectedPaths = decoded;
      expect(protectedPaths[0]).toBe(container);
    } else {
      expect(env.PI_FABRIC_CLEANUP_BATCH).toBe("0");
      expect(options?.input).toBeUndefined();
      protectedPaths = [container];
    }
    for (const protectedPath of protectedPaths) {
      expect(typeof protectedPath).toBe("string");
      if (typeof protectedPath !== "string") throw new Error("Protected cleanup path must be a string");
      expect(protectedPath.length).toBeGreaterThan(0);
      expect(fs.realpathSync(protectedPath)).toBe(protectedPath);
      securityContainers.push(protectedPath);
    }
    return Buffer.alloc(0); // Recorded contract boundary; NEVER native DACL certification.
  }) as typeof execFileSync);
};
afterEach(() => {
  // Evidence COPY is not part of the 20s admission/eviction contract. Export
  // after the native checks, but BEFORE fixture teardown, including failed runs.
  const stage = process.env.PI_TEST_CUSTODY_EVIDENCE_DIR;
  if (stage) {
    fs.mkdirSync(stage, { recursive: true });
    for (const { receipt, before } of rawExports.splice(0)) {
      const out = fs.mkdtempSync(path.join(stage, "stop-contract-"));
      fs.cpSync(path.dirname(receipt), path.join(out, "container"), { recursive: true, dereference: false });
      fs.writeFileSync(path.join(out, "native-inventory.json"), JSON.stringify(before));
    }
    if (stressPhases) {
      const out = fs.mkdtempSync(path.join(stage, "stop-phases-"));
      fs.writeFileSync(path.join(out, "phase-timings.json"), JSON.stringify(stressPhases));
    }
  }
  vi.useRealTimers(); vi.restoreAllMocks(); vi.unstubAllEnvs();
  vi.mocked(spawn).mockReset();
  delete privateDirTestHooks.platform;
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const setup = (retainRuns = false) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-stop-merge-")); roots.push(root);
  privateDirTestHooks.platform = hostPlatform;
  if (hostPlatform === "win32") {
    const fsync = fs.fsyncSync.bind(fs);
    // Only the transport branch is simulated. Native Windows has no directory
    // fsync barrier, even while process.platform is mocked to Linux. Keep real
    // file fsync (including durable stop intent) and all custody assertions.
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      if (!fs.fstatSync(fd).isDirectory()) fsync(fd);
    });
  }
  return new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 60_000,
    maxConcurrent: 1, retainRuns, budgetUsd: 0, notifyOnComplete: false },
  { runRoot: path.join(root, "runs"), workerPath: path.join(root, "worker.mjs") });
};
const stillRunning = async (manager: AgentManager, id: string) => {
  const waiting = expect(manager.wait(id, { timeoutMs: 0 })).rejects.toThrow(/still running/);
  await vi.advanceTimersByTimeAsync(0); await waiting;
};
const unconfirmedClose = async (manager: AgentManager) => {
  const closing = expect(manager.close()).rejects.toThrow(/execution exit unconfirmed/);
  await vi.advanceTimersByTimeAsync(10_000); await closing;
};
const terminal = (request: AgentTransportLaunch) => {
  const status = request.workerArguments[request.workerArguments.indexOf("--status-file") + 1]!;
  fs.writeFileSync(status, JSON.stringify({ id: request.id, name: request.name, task: "terminal",
    status: "completed", runner: "pi", transport: "process", sessionId: "2147483647", cwd: request.cwd,
    startedAt: Date.now(), updatedAt: Date.now(), finishedAt: Date.now(), turns: 1, toolCalls: 0,
    text: "done", usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, cost: 0 } }));
};

describe.skipIf(process.platform !== "linux")("absent native worker stop fence", () => {
  it.each(["unreadable", "new-birth", "late-birth"] as const)("never signals an unowned birth (%s)", async mode => {
    vi.useFakeTimers();
    const pid = 2147483647;
    const child = Object.assign(new EventEmitter(), { pid, unref: vi.fn(), kill: vi.fn() });
    vi.mocked(spawn).mockReturnValueOnce(child as unknown as ChildProcess);
    const nativeRead = fs.readFileSync.bind(fs);
    const stat = nativeRead("/proc/self/stat", "utf8");
    let readCount = 0; let appeared = false;
    vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, options?: unknown) => {
      if (String(file) !== `/proc/${pid}/stat`) return nativeRead(file, options as never);
      if (mode === "unreadable") throw Object.assign(new Error("unreadable birth"), { code: "EACCES" });
      if ((mode === "new-birth" && readCount++ > 0) || appeared) return stat;
      throw Object.assign(new Error("absent"), { code: "ENOENT" });
    }) as typeof fs.readFileSync);
    const kill = vi.spyOn(process, "kill").mockImplementation(() => true);
    const handle = await spawnDetached("worker.mjs", [], process.cwd());
    try {
      if (mode === "late-birth") {
        const stopping = handle.stop();
        expect(kill).toHaveBeenCalledWith(-pid, "SIGTERM");
        appeared = true;
        await vi.advanceTimersByTimeAsync(7_000); await stopping;
        expect(kill).not.toHaveBeenCalledWith(-pid, "SIGKILL");
      } else {
        await expect(handle.stop()).rejects.toThrow(/ownership\/exit/);
        expect(kill).not.toHaveBeenCalled();
      }
      expect(handle.stopDebt?.(), "failed identity checks are never bounded-stop authority").toBeUndefined();
      expect(handle.lostContact()).toBeDefined();
      expect(vi.getTimerCount()).toBe(0);
    } finally { child.emit("exit", 0); child.emit("close", 0); }
  });
});

// Inject only the parent's product branch. These are deterministic contract
// probes, not native Windows process-tree certification.
describe.each(["linux", "win32"] as const)("merge stop custody (%s)", platform => {
  beforeEach(async () => {
    observeCustody();
    // Fake-OS probes record ONLY the ACL subprocess. Real filesystem rename
    // and receipt/object proofs stay active.
    if (platform !== hostPlatform) await recordSecuritySubprocess();
  });
  it.each([false, true])("does not publish unchecked terminal success or release custody (close hook=%s)", async closeHook => {
    vi.useFakeTimers(); vi.spyOn(process, "platform", "get").mockReturnValue(platform);
    let gone = false;
    const stop = vi.fn(async () => {});
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
      terminal(request);
      return { kind: "process", sessionId: "2147483647", stop, isAlive: async () => !gone,
        lostContact: () => gone ? undefined : "unknown execution tree",
        ...(closeHook ? { waitForClose: async () => {}, stopDebt: () => undefined } : {}) };
    });
    const manager = setup();
    const handle = await manager.spawn({ task: "terminal" });
    try {
      await vi.advanceTimersByTimeAsync(2_000);
      expect(await manager.wait(handle.id)).toMatchObject({ status: "failed", errorCode: "CUSTODY_UNCONFIRMED" });
      await expect(manager.stop(handle.id)).rejects.toThrow(/execution exit unconfirmed/);
      expect(stop).not.toHaveBeenCalled();
      expect(hasUnresolvedWorker(manager.runDirectory(handle.id)!)).toBe(true);
      expect((await manager.spawn({ task: "must stay queued" })).status).toBe("queued");
      await unconfirmedClose(manager);
    } finally {
      gone = true;
      await manager.stop(handle.id);
      await manager.close();
    }
  });

  it("keeps a cancelled launch strict even with an exact native stop debt", async () => {
    vi.useFakeTimers(); vi.spyOn(process, "platform", "get").mockReturnValue(platform);
    let enter!: () => void; const entered = new Promise<void>(resolve => { enter = resolve; });
    let finish!: () => void; const gate = new Promise<void>(resolve => { finish = resolve; });
    let debt: string | undefined;
    const transport: AgentTransportHandle = { kind: "process", sessionId: "2147483647",
      isAlive: async () => debt !== undefined, stop: async () => { debt = "native close deadline expired"; },
      waitForClose: async () => {}, lostContact: () => debt, stopDebt: () => debt };
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async () => { enter(); await gate; return transport; });
    const manager = setup(); const abort = new AbortController();
    const spawning = manager.spawn({ task: "cancel during launch" }, abort.signal);
    await entered; abort.abort(); finish();
    const handle = await spawning;
    try {
      await expect(manager.stop(handle.id)).rejects.toThrow(/execution exit unconfirmed/);
      await stillRunning(manager, handle.id);
      expect(hasUnresolvedWorker(manager.runDirectory(handle.id)!)).toBe(true);
      await unconfirmedClose(manager);
    } finally {
      transport.stop = async () => { debt = undefined; };
      debt = undefined;
      await manager.stop(handle.id); await manager.close();
    }
  });

  it("retains a failed release observation after execution was already confirmed", async () => {
    vi.useFakeTimers(); vi.spyOn(process, "platform", "get").mockReturnValue(platform);
    let hung = false; let finish!: () => void;
    const query = new Promise<boolean>(resolve => { finish = () => resolve(false); });
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
      terminal(request);
      return { kind: "process", sessionId: "2147483647", stop: async () => {},
        isAlive: async () => hung ? query : false };
    });
    const manager = setup();
    try {
      const handle = await manager.spawn({ task: "confirmed exit" });
      await manager.wait(handle.id);
      const run = manager.runDirectory(handle.id)!;
      hung = true;
      const checking = expect(manager.checkpointForRelease(Date.now() + 80)).rejects.toThrow(/unconfirmed/);
      await vi.advanceTimersByTimeAsync(100); await checking;
      expect(hasUnresolvedWorker(run)).toBe(true);
      finish(); hung = false;
      await manager.close();
      expect(fs.existsSync(run)).toBe(true);
      expect(hasUnresolvedWorker(run)).toBe(true);
    } finally { finish(); hung = false; await manager.close(); }
  });
});

// Archive admission must interpret real filesystem metadata using the native
// platform, not a transport-branch mock (Windows libuv mode bits are not POSIX
// permissions). Only the Windows ACL subprocess is recorded, NOT certified.
describe("native filesystem stop custody stress", () => {
  beforeEach(async () => {
    observeCustody();
    if (hostPlatform === "win32") await recordSecuritySubprocess();
  });

  it("archives a completed raw run as one exact whole-root custody transfer", async () => {
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
      terminal(request);
      return { kind: "process", sessionId: "2147483647", stop: async () => {}, isAlive: async () => false };
    });
    const manager = setup();
    try {
      const result = await manager.run({ task: "single-root custody proof" });
      expect(result.status).toBe("completed");
      const source = manager.runDirectory(result.id)!;
      const before = inventory(source);
      await manager.close();
      expect(movedRoots).toEqual(new Set([source]));
      expect(rawExports).toHaveLength(1);
      expect(rawExports[0]!.before).toEqual(before);
      const receipt = JSON.parse(fs.readFileSync(rawExports[0]!.receipt, "utf8")) as { destination: string; state: string };
      expect(receipt.state).toBe("moved");
      expect(inventory(receipt.destination)).toEqual(before);
    } finally { await manager.close(); }
  });

  it("releases confirmed custom process admission and evicts settled handles", async () => {
    expect(vi.isMockFunction(Object.getOwnPropertyDescriptor(process, "platform")?.get),
      "native filesystem custody must not use a mocked process.platform").toBe(false);
    expect(process.platform).toBe(hostPlatform);
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
      terminal(request);
      return { kind: "process", sessionId: "2147483647", stop: async () => {}, isAlive: async () => false };
    });
    // This case is specifically the 1,000-handle/transport-admission stress
    // check. Keep roots in place here instead of doing 1,000 durable archive
    // transactions in its 20s budget; the adjacent custody cases verify the
    // default archive/receipt path on real filesystem trees.
    const manager = setup(true);
    let first: string | undefined;
    let firstRoot: string | undefined;
    let firstInventory: unknown[] | undefined;
    stressPhases = { started: performance.now() };
    try {
      for (let index = 0; index < 1_005; index++) {
        const result = await manager.run({ task: `eviction pressure ${index}` });
        if (!first) {
          first = result.id; firstRoot = manager.runDirectory(first)!;
          firstInventory = inventory(firstRoot);
        }
        expect(result.status).toBe("completed");
      }
      expect(manager.runDirectory(first!), "checked dead execution may evict a handle, never its raw root").toBeUndefined();
      stressPhases.loopFinished = performance.now();
    } finally { await manager.close(); stressPhases.closeFinished = performance.now(); }
    expect(inventory(firstRoot!)).toEqual(firstInventory);
    expect(movedRoots.size, "retained-run stress must not archive or delete raw roots").toBe(0);
    expect(securityContainers.length).toBe(hostPlatform === "win32" ? 1_000 : 0);
  }, 20_000);
});
