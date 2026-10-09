import { spawn, type ChildProcess } from "node:child_process";
import * as childProcess from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { EventEmitter } from "node:events";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { ProcessTreeCustodyUnconfirmedError, spawnDetached } from "../src/agents/transports/process-utils.js";
import type { AgentRunRecord, AgentTransportHandle } from "../src/agents/types.js";

vi.mock("node:child_process", async importOriginal => {
  const actual = await importOriginal<typeof import("node:child_process")>();
  return { ...actual, spawn: vi.fn(actual.spawn) };
});

const roots: string[] = [];
const managers: AgentManager[] = [];
afterEach(async () => {
  vi.useRealTimers();
  for (const manager of managers.splice(0)) await manager.close().catch(() => undefined);
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const root = () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-event-liveness-"));
  roots.push(directory);
  return directory;
};
const tick = () => new Promise<void>(resolve => setImmediate(resolve));
const deferred = () => {
  let resolve!: () => void;
  let reject!: (reason: Error) => void;
  const promise = new Promise<void>((done, fail) => { resolve = done; reject = fail; });
  return { promise, resolve, reject };
};

async function nativeWorker(run: (handle: AgentTransportHandle, child: ChildProcess) => Promise<void>) {
  const directory = root();
  const workerPath = path.join(directory, "worker.mjs");
  fs.writeFileSync(workerPath, "setInterval(() => {}, 1000);\n");
  const actual = await vi.importActual<typeof import("node:child_process")>("node:child_process");
  let child!: ChildProcess;
  vi.mocked(spawn).mockImplementation((...args: Parameters<typeof spawn>) => { child = actual.spawn(...args); return child; });
  const handle = await new ProcessTransport().launch({ id: "events", name: "events", cwd: directory, workerPath, workerArguments: [] });
  try { await run(handle, child); }
  finally { await handle.stop(); await handle.waitForClose?.(); }
}

describe("native process event liveness", () => {
  it("arms no recurring liveness timer for an active process transport", async () => {
    const interval = vi.spyOn(globalThis, "setInterval");
    const timeout = vi.spyOn(globalThis, "setTimeout");
    await nativeWorker(async handle => {
      expect(handle.liveness).toBe("events");
      expect(handle.livenessPollIntervalMs).toBeUndefined();
      expect(interval).not.toHaveBeenCalled();
      expect(timeout).not.toHaveBeenCalled();
      expect(await handle.isAlive()).toBe(true);
    });
  });

  it("latches exit/close by the next tick, without a timer or numeric-PID re-probe", async () => {
    await nativeWorker(async (handle, child) => {
      const closed = new Promise<void>(resolve => child.once("close", () => resolve()));
      child.kill("SIGTERM");
      await closed;
      await tick();
      const kill = vi.spyOn(process, "kill");
      expect(await handle.isAlive()).toBe(false);
      expect(kill).not.toHaveBeenCalled();
      await expect(handle.closed).resolves.toBeUndefined();
    });
  });
});

async function unscopedTree(termGraceMs = 7_000, scoped = false) {
  vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout", "setInterval", "clearInterval"] });
  const directory = root();
  const workerPath = path.join(directory, "worker.mjs");
  fs.writeFileSync(workerPath, "");
  const pid = 700_000_001;
  const descendant = pid + 1;
  let primaryAlive = true;
  let descendantAlive = true;
  let scopeEvents: string | undefined;
  let populated = "1";
  const watcher = Object.assign(new EventEmitter(), { close: vi.fn() }) as unknown as fs.FSWatcher;
  if (scoped) vi.spyOn(fs, "watch").mockImplementation((...args: Parameters<typeof fs.watch>) => {
    const listener = args.at(-1);
    if (typeof listener === "function") watcher.on("change", listener);
    return watcher;
  });
  const stat = (member: number) => {
    const fields = Array<string>(20).fill("0");
    fields[0] = "S"; fields[1] = member === pid ? "1" : String(pid);
    fields[2] = String(pid); fields[19] = String(member);
    return `${member} (worker) ${fields.join(" ")}`;
  };
  const nativeRead = fs.readFileSync.bind(fs);
  vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, options?: unknown) => {
    if (scopeEvents && String(file) === scopeEvents) return `populated ${populated}\n`;
    if (String(file) === `/proc/${pid}/stat` && primaryAlive) return stat(pid);
    if (String(file) === `/proc/${descendant}/stat` && descendantAlive) return stat(descendant);
    if ([`/proc/${pid}/stat`, `/proc/${descendant}/stat`].includes(String(file))) {
      throw Object.assign(new Error("gone"), { code: "ENOENT" });
    }
    return nativeRead(file, options as never);
  }) as typeof fs.readFileSync);
  const nativeReaddir = fs.readdirSync.bind(fs);
  const census = vi.spyOn(fs, "readdirSync").mockImplementation(((file: fs.PathLike, options?: unknown) => {
    if (String(file) === "/proc") return [...(primaryAlive ? [String(pid)] : []), ...(descendantAlive ? [String(descendant)] : [])];
    return nativeReaddir(file, options as never);
  }) as typeof fs.readdirSync);
  const kill = vi.spyOn(process, "kill").mockImplementation((_pid, signal) => {
    if (signal === "SIGTERM" || signal === "SIGKILL") descendantAlive = false;
    return true;
  });
  const child = Object.assign(new EventEmitter(), { pid, unref: vi.fn(), channel: {} });
  vi.mocked(spawn).mockImplementationOnce((_command, args) => {
    if (scoped) {
      const marker = args![args!.indexOf("fabric-scope") + 1]!;
      const unit = args!.find(arg => arg.startsWith("--unit="))!.slice(7);
      fs.writeFileSync(`${marker}.cgroup`, `0::/test/${unit}\n`);
      fs.writeFileSync(marker, "admitted");
      scopeEvents = `/sys/fs/cgroup/test/${unit}/cgroup.events`;
    }
    return child as unknown as ChildProcess;
  });
  const unconfirmed = vi.fn();
  const scope = scoped ? { executable: "/fixture-systemd-run", slice: "fixture.slice", warn: vi.fn() } : undefined;
  const handle = await spawnDetached(workerPath, [], directory, { onUnconfirmedExit: unconfirmed }, undefined, scope, termGraceMs, true);
  expect(await handle.isAlive()).toBe(true); // retain the descendant's birth before reparenting
  census.mockClear(); kill.mockClear();
  const timeout = vi.spyOn(globalThis, "setTimeout");
  return {
    handle, census, kill, timeout, unconfirmed, watcher,
    emptyScope: () => { populated = "0"; watcher.emit("change"); },
    depart: () => { descendantAlive = false; },
    close: async () => {
      primaryAlive = false;
      child.emit("exit", 0); child.emit("close", 0);
      await tick();
    },
    scans: () => census.mock.calls.filter(([file]) => String(file) === "/proc").length,
  };
}

describe.skipIf(process.platform !== "linux")("unscoped bounded tree census", () => {
  it.each(["clean", "unconfirmed"] as const)("takes only close and deadline censuses (%s)", async outcome => {
    const f = await unscopedTree();
    let settled = false;
    const receipt = f.handle.treeClosed!.then(() => { settled = true; return undefined; }, error => { settled = true; return error as unknown; });
    await f.close();
    expect(f.scans()).toBe(1);
    expect(await f.handle.isAlive()).toBe(true);
    expect(await f.handle.isAlive()).toBe(true);
    expect(f.scans()).toBe(1); // close notifications cannot duplicate the census
    expect(f.timeout.mock.calls.map(([, ms]) => ms)).toEqual([60_000]);
    expect(f.timeout.mock.results[0]!.value.hasRef()).toBe(false);
    expect(vi.getTimerCount()).toBe(1);
    await vi.advanceTimersByTimeAsync(59_999);
    expect(f.scans()).toBe(1);
    expect(settled).toBe(false);
    expect(f.timeout).toHaveBeenCalledOnce();
    if (outcome === "clean") f.depart();
    await vi.advanceTimersByTimeAsync(1);
    const result = await receipt;
    expect(f.scans()).toBe(2);
    expect(vi.getTimerCount()).toBe(0);
    if (outcome === "clean") {
      expect(result).toBeUndefined();
      expect(f.handle.lostContact()).toBeUndefined();
      expect(f.unconfirmed).not.toHaveBeenCalled();
    } else {
      expect(result).toBeInstanceOf(ProcessTreeCustodyUnconfirmedError);
      expect(result).toMatchObject({ code: "PROCESS_TREE_CUSTODY_UNCONFIRMED", descendants: 1, message: "custody unconfirmed: 1 descendants may remain" });
      expect(f.handle.lostContact()).toBe("custody unconfirmed: 1 descendants may remain");
      expect(f.handle.stopDebt?.()).toBe(f.handle.lostContact());
      expect(f.unconfirmed).toHaveBeenCalledExactlyOnceWith(f.handle.lostContact());
    }
    await vi.advanceTimersByTimeAsync(300_000);
    expect(f.scans()).toBe(2); // even a non-empty tree cannot restart observation
    expect(f.timeout).toHaveBeenCalledOnce();
    if (outcome === "unconfirmed") {
      await f.handle.stop(); // retained births transfer to the existing cleanup path
      expect(f.kill).toHaveBeenCalledWith(-700_000_001, "SIGTERM");
      expect(await f.handle.isAlive()).toBe(false);
      expect(f.handle.stopDebt?.()).toBe("custody unconfirmed: 1 descendants may remain");
      await expect(f.handle.treeClosed).rejects.toBe(result); // cleanup cannot fake a clean receipt
    }
  });

  it("delivers the real transport deadline as a typed manager result and joins descendant cleanup", async () => {
    const f = await unscopedTree();
    const directory = root();
    const watcher = Object.assign(new EventEmitter(), { close: vi.fn() }) as unknown as fs.FSWatcher;
    vi.spyOn(fs, "watch").mockReturnValue(watcher);
    const stop = vi.fn(f.handle.stop);
    vi.spyOn(ProcessTransport.prototype, "launch").mockResolvedValue({
      ...f.handle, kind: "process", sessionId: String(f.handle.pid), liveness: "events", stop, relaunchable: true,
    });
    const manager = new AgentManager(directory, { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 120_000, budgetUsd: 0, sessionExport: false }, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(directory, "runs"),
    });
    managers.push(manager);
    const handle = await manager.spawn({ task: "retained descendant deadline", transport: "process" });
    await f.close();
    expect(f.scans()).toBe(1); // native close's manager query reused pending custody
    await vi.advanceTimersByTimeAsync(59_999);
    expect(manager.status(handle.id).status).toBe("running");
    expect(f.scans()).toBe(1);
    expect(stop).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1);
    expect(await manager.wait(handle.id)).toMatchObject({
      status: "failed", errorCode: "PROCESS_TREE_CUSTODY_UNCONFIRMED", error: "custody unconfirmed: 1 descendants may remain",
    });
    expect(stop).toHaveBeenCalledOnce();
    expect(f.kill).toHaveBeenCalledWith(-700_000_001, "SIGTERM");
    expect(await f.handle.isAlive()).toBe(false);
    expect(f.handle.lostContact()).toBe("custody unconfirmed: 1 descendants may remain");
    expect(fs.existsSync(path.join(manager.runDirectory(handle.id)!, "unresolved-worker.json"))).toBe(true);
    await expect(manager.cleanup(handle.id)).rejects.toThrow(/lost track|unresolved|exit is unconfirmed/);
  });

  it.each(["clean", "unconfirmed"] as const)("uses only two portable POSIX snapshots (%s)", async outcome => {
    vi.spyOn(process, "platform", "get").mockReturnValue("darwin");
    let parentPresent = true;
    let descendantPresent = true;
    const ps = vi.spyOn(childProcess, "execFile").mockImplementation(((...args: unknown[]) => {
      const callback = args.at(-1) as (error: null, stdout: string, stderr: string) => void;
      const query = new EventEmitter() as ChildProcess;
      queueMicrotask(() => {
        callback(null, `${parentPresent ? "700000001 1 700000001 S\n" : ""}${descendantPresent ? "700000002 1 700000001 S\n" : ""}`, "");
        query.emit("close", 0, null);
      });
      return query;
    }) as typeof childProcess.execFile);
    const f = await unscopedTree();
    ps.mockClear();
    const receipt = f.handle.treeClosed!.then(() => undefined, error => error as unknown);
    parentPresent = false;
    await f.close();
    expect(ps).toHaveBeenCalledOnce();
    expect(f.timeout.mock.calls.map(([, ms]) => ms)).toEqual([60_000]);
    expect(await f.handle.isAlive()).toBe(true);
    await vi.advanceTimersByTimeAsync(59_999);
    expect(ps).toHaveBeenCalledOnce();
    if (outcome === "clean") descendantPresent = false;
    await vi.advanceTimersByTimeAsync(1);
    const result = await receipt;
    expect(ps).toHaveBeenCalledTimes(2);
    expect(vi.getTimerCount()).toBe(0);
    if (outcome === "clean") expect(result).toBeUndefined();
    else expect(result).toMatchObject({ code: "PROCESS_TREE_CUSTODY_UNCONFIRMED", descendants: 1 });
    await vi.advanceTimersByTimeAsync(300_000);
    expect(ps).toHaveBeenCalledTimes(2);
  });

  it("preserves a larger existing TERM/KILL grace budget without repeating", async () => {
    const f = await unscopedTree(90_000);
    await f.close();
    expect(f.timeout.mock.calls.map(([, ms]) => ms)).toEqual([92_000]);
    await vi.advanceTimersByTimeAsync(91_999);
    expect(f.scans()).toBe(1);
    f.depart();
    await vi.advanceTimersByTimeAsync(1);
    await expect(f.handle.treeClosed).resolves.toBeUndefined();
    expect(f.scans()).toBe(2);
    expect(vi.getTimerCount()).toBe(0);
  });
});

describe.skipIf(process.platform !== "linux")("scoped tree-empty event delivery", () => {
  it("waits passively for populated-0 and does not arm a census deadline for a live scope", async () => {
    const f = await unscopedTree(7_000, true);
    let settled = false;
    void f.handle.treeClosed!.then(() => { settled = true; });
    await f.close();
    expect(f.scans()).toBe(0);
    expect(f.timeout).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(120_000);
    expect(f.scans()).toBe(0);
    expect(settled).toBe(false);
    f.depart(); f.emptyScope();
    await tick();
    await expect(f.handle.treeClosed).resolves.toBeUndefined();
    expect(f.scans()).toBe(1);
    expect(f.timeout).not.toHaveBeenCalled();
    expect(f.watcher.close).toHaveBeenCalledOnce();
  });
});

describe.skipIf(process.platform !== "linux")("native descendant tree-empty delivery", () => {
  it("settles a real unscoped process at the single deadline census after its descendant exits", async () => {
    const directory = root();
    const workerPath = path.join(directory, "descendant-worker.mjs");
    const descendant = `const fs = require("node:fs");
      const timer = setInterval(() => {
        if (fs.existsSync("exit-descendant")) {
          fs.writeFileSync("descendant-exited", String(Date.now()));
          clearInterval(timer); process.exit(0);
        }
      }, 10);
      setTimeout(() => process.exit(2), 8000);`;
    fs.writeFileSync(workerPath, `import fs from "node:fs";
      import { spawn } from "node:child_process";
      process.on("message", message => {
        if (message.type !== "fabric-execution-custody-ack") return;
        const descendant = spawn(process.execPath, ["-e", ${JSON.stringify(descendant)}], { detached: true, stdio: "ignore" });
        descendant.unref();
        const fields = fs.readFileSync(\`/proc/\${descendant.pid}/stat\`, "utf8");
        const started = fields.slice(fields.lastIndexOf(")") + 2).trim().split(/\\s+/)[19];
        process.send({ type: "fabric-execution-started", pid: descendant.pid, started }, () => {
          fs.writeFileSync("descendant-ready", String(descendant.pid));
          setInterval(() => { if (fs.existsSync("exit-parent")) process.exit(0); }, 10);
        });
      });
      process.send({ type: "fabric-execution-custody" });
      setTimeout(() => process.exit(2), 8000);`);
    const actualSpawn = await vi.importActual<typeof import("node:child_process")>("node:child_process");
    let child!: ChildProcess;
    vi.mocked(spawn).mockImplementation((...args: Parameters<typeof spawn>) => { child = actualSpawn.spawn(...args); return child; });
    const actualLaunch = ProcessTransport.prototype.launch;
    let transport!: AgentTransportHandle;
    const stop = vi.fn<AgentTransportHandle["stop"]>();
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function(this: ProcessTransport, request) {
      transport = await actualLaunch.call(this, request);
      stop.mockImplementation(transport.stop);
      return { ...transport, stop, relaunchable: false };
    });
    const manager = new AgentManager(directory, { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 120_000, budgetUsd: 0, sessionExport: false }, {
      workerPath, runRoot: path.join(directory, "runs"),
    });
    managers.push(manager);
    const handle = await manager.spawn({ task: "native descendant closes later", transport: "process" });
    try {
      await vi.waitFor(() => expect(fs.existsSync(path.join(directory, "descendant-ready"))).toBe(true), { timeout: 3_000 });
      vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout", "setInterval", "clearInterval"] });
      const closed = new Promise<void>(resolve => child.once("close", () => resolve()));
      fs.writeFileSync(path.join(directory, "exit-parent"), "");
      await closed; await tick();
      const closedAt = Date.now();
      expect(await transport.isAlive()).toBe(true);
      expect(manager.status(handle.id).status).toBe("running");
      let treeEmpty = false;
      void transport.treeClosed?.then(() => { treeEmpty = true; });
      await vi.advanceTimersByTimeAsync(59_000);
      expect(treeEmpty).toBe(false);
      expect(stop).not.toHaveBeenCalled();
      fs.writeFileSync(path.join(directory, "exit-descendant"), "");
      await vi.waitFor(() => expect(fs.existsSync(path.join(directory, "descendant-exited"))).toBe(true), { timeout: 500 });
      expect(treeEmpty).toBe(false); // descendant exit is not a polling wake
      expect(manager.status(handle.id).status).toBe("running");
      await vi.advanceTimersByTimeAsync(60_000 - (Date.now() - closedAt));
      const result = await manager.wait(handle.id);
      expect(result.status).toBe("failed");
      expect(result.error).toContain("exited without a result");
      expect(Date.now() - closedAt).toBe(60_000);
      expect(transport.treeClosed).toBeDefined();
      expect(treeEmpty).toBe(true);
      expect(await transport.isAlive()).toBe(false);
      expect(stop).not.toHaveBeenCalled();
    } finally {
      vi.useRealTimers();
      fs.writeFileSync(path.join(directory, "exit-parent"), "");
      fs.writeFileSync(path.join(directory, "exit-descendant"), "");
      await transport.stop(); await transport.waitForClose?.();
    }
  });
});

const statusRecord = (id: string, task: string): AgentRunRecord => ({
  id, name: task, task, status: "running", runner: "pi", transport: "process", cwd: process.cwd(),
  startedAt: Date.now(), updatedAt: Date.now(), turns: 0, toolCalls: 0, text: "", exitCode: null,
  usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
});

function monitored(relaunchable = false) {
  vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout", "setInterval", "clearInterval"] });
  const directory = root();
  const close = deferred();
  const tree = deferred();
  const watcher = Object.assign(new EventEmitter(), { close: vi.fn() }) as unknown as fs.FSWatcher;
  vi.spyOn(fs, "watch").mockReturnValue(watcher);
  let alive = true;
  const read = vi.fn(async () => alive);
  const stop = vi.fn(async () => { alive = false; close.resolve(); tree.resolve(); });
  const launch = vi.spyOn(ProcessTransport.prototype, "launch").mockResolvedValue({
    kind: "process", liveness: "events", closed: close.promise, treeClosed: tree.promise,
    isAlive: read, stop, relaunchable,
  });
  const manager = new AgentManager(directory, { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 1_000, budgetUsd: 0, sessionExport: false }, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(directory, "runs"),
  });
  managers.push(manager);
  return { manager, close, tree, watcher, read, stop, launch, die: () => { alive = false; } };
}

describe("manager event-only process monitoring", () => {
  it("joins cleanup and settles typed unconfirmed custody at its one-shot deadline", async () => {
    const f = monitored(true);
    const handle = await f.manager.spawn({ task: "custody deadline", transport: "process" });
    const failure = new ProcessTreeCustodyUnconfirmedError(2);
    f.tree.reject(failure);
    await tick();
    const result = await f.manager.wait(handle.id);
    expect(result).toMatchObject({ status: "failed", errorCode: "PROCESS_TREE_CUSTODY_UNCONFIRMED", error: "custody unconfirmed: 2 descendants may remain" });
    expect(f.stop).toHaveBeenCalledOnce();
    expect(f.launch).toHaveBeenCalledOnce();
    expect(fs.existsSync(path.join(f.manager.runDirectory(handle.id)!, "unresolved-worker.json"))).toBe(true);
    await expect(f.manager.cleanup(handle.id)).rejects.toThrow(/lost track|unresolved|exit is unconfirmed/);
  });

  it("does not wake or query on 250 ms/1 s ticks; only native close triggers a liveness check", async () => {
    const f = monitored();
    const handle = await f.manager.spawn({ task: "event census", transport: "process" });
    const status = path.join(f.manager.runDirectory(handle.id)!, "status.json");
    const readFile = vi.spyOn(fs, "readFileSync");
    const timeout = vi.spyOn(globalThis, "setTimeout");
    f.watcher.emit("change", "unrelated-file");
    await vi.advanceTimersByTimeAsync(1_500);
    expect(f.read).not.toHaveBeenCalled();
    expect(readFile.mock.calls.filter(([file]) => file === status)).toHaveLength(0);
    expect(timeout).not.toHaveBeenCalled();
    f.die(); f.close.resolve(); f.tree.resolve();
    await tick();
    expect(f.read).toHaveBeenCalledOnce();
    expect((await f.manager.wait(handle.id)).status).toBe("failed");
  });

  it.each(["closed", "treeClosed"])("wakes on %s by the next tick", async event => {
    const f = monitored();
    const handle = await f.manager.spawn({ task: event, transport: "process" });
    await vi.advanceTimersByTimeAsync(500);
    expect(f.read).not.toHaveBeenCalled();
    if (event === "closed") f.close.resolve(); else f.tree.resolve();
    await tick();
    expect(f.read).toHaveBeenCalledOnce();
    // A notification while custody still says alive is not exit permission.
    expect(f.manager.status(handle.id).status).toBe("running");
    f.die(); if (event === "closed") f.tree.resolve(); else f.close.resolve();
    await tick();
    expect((await f.manager.wait(handle.id)).status).toBe("failed");
  });

  it("catches a missed event with exactly one deadline safety read", async () => {
    const f = monitored();
    const handle = await f.manager.spawn({ task: "missed native event", transport: "process" });
    f.die(); // Neither captured promise delivers its notification.
    await vi.advanceTimersByTimeAsync(1_999);
    expect(f.read).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1);
    expect((await f.manager.wait(handle.id)).error).toContain("exited without a result");
    expect(f.read).toHaveBeenCalledOnce();
    await vi.advanceTimersByTimeAsync(10_000);
    expect(f.read).toHaveBeenCalledOnce();
    expect(f.watcher.close).toHaveBeenCalledOnce();
    f.close.resolve(); f.tree.resolve();
  });

  it("observes atomic status publication without probing process liveness", async () => {
    const f = monitored();
    const handle = await f.manager.spawn({ task: "status events", transport: "process" });
    const status = path.join(f.manager.runDirectory(handle.id)!, "status.json");
    const temporary = `${status}.tmp`;
    fs.writeFileSync(temporary, JSON.stringify({ ...statusRecord(handle.id, "status events"), turns: 3, toolCalls: 1 }));
    fs.renameSync(temporary, status);
    f.watcher.emit("rename", "status.json");
    await tick();
    expect(f.manager.status(handle.id)).toMatchObject({ status: "running", turns: 3, toolCalls: 1 });
    expect(f.read).not.toHaveBeenCalled();
    f.die(); f.close.resolve(); f.tree.resolve();
    await tick();
    expect((await f.manager.wait(handle.id)).status).toBe("failed");
  });

  it("does not let a terminal file erase a failed native watcher", async () => {
    const f = monitored();
    const handle = await f.manager.spawn({ task: "terminal without exit proof", transport: "process" });
    const status = path.join(f.manager.runDirectory(handle.id)!, "status.json");
    const record = statusRecord(handle.id, "terminal without exit proof");
    f.close.reject(new Error("native close lost"));
    await tick();
    fs.writeFileSync(status, JSON.stringify({ ...record, status: "completed", finishedAt: Date.now() }));
    f.watcher.emit("rename", "status.json");
    await tick();
    expect(f.read).not.toHaveBeenCalled();
    expect(f.stop).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(2_000);
    expect(await f.manager.wait(handle.id)).toMatchObject({ status: "failed", errorCode: "PROCESS_LIVENESS_WATCH_FAILED" });
    expect(f.read).toHaveBeenCalledOnce();
    f.tree.resolve();
  });

  it.each(["native", "tree", "records"])("keeps a %s watcher failure latched across a later wake and dead process", async source => {
    const f = monitored(true);
    const handle = await f.manager.spawn({ task: "dead after watcher failure", transport: "process" });
    const status = path.join(f.manager.runDirectory(handle.id)!, "status.json");
    const failure = new Error("event watcher unavailable");
    if (source === "native") f.close.reject(failure);
    else if (source === "tree") f.tree.reject(failure);
    else f.watcher.emit("error", failure);
    await tick();
    // A separate status wake must not clear the failure or grant retry admission.
    f.watcher.emit("rename", "status.json");
    await tick();
    f.die();
    fs.writeFileSync(status, JSON.stringify({ ...statusRecord(handle.id, "dead after watcher failure"), status: "completed", finishedAt: Date.now() }));
    if (source === "native") f.tree.resolve(); else f.close.resolve();
    await tick();
    expect(f.manager.status(handle.id)).toMatchObject({ status: "failed", errorCode: "PROCESS_LIVENESS_WATCH_FAILED" });
    expect(await f.manager.wait(handle.id)).toMatchObject({ status: "failed", errorCode: "PROCESS_LIVENESS_WATCH_FAILED" });
    expect(f.launch).toHaveBeenCalledOnce();
    expect(f.stop).not.toHaveBeenCalled();
    expect(fs.existsSync(path.join(f.manager.runDirectory(handle.id)!, "unresolved-worker.json"))).toBe(true);
    f.close.resolve(); f.tree.resolve();
  });

  it("vetoes a startup retry when a watcher fails during the exit join", async () => {
    const f = monitored(true);
    const handle = await f.manager.spawn({ task: "watcher fails during retry", transport: "process" });
    f.die(); f.close.resolve();
    await tick();
    f.watcher.emit("error", new Error("retry watcher unavailable"));
    f.tree.resolve();
    await tick();
    await vi.advanceTimersByTimeAsync(1_000);
    expect(f.launch).toHaveBeenCalledOnce();
    expect(await f.manager.wait(handle.id)).toMatchObject({ status: "failed", errorCode: "PROCESS_LIVENESS_WATCH_FAILED" });
    expect(fs.existsSync(path.join(f.manager.runDirectory(handle.id)!, "unresolved-worker.json"))).toBe(true);
  });

  it("joins a replacement admitted just before the old watcher fails, without clearing the failure", async () => {
    const f = monitored(true);
    const handle = await f.manager.spawn({ task: "watch failure races native retry admission", transport: "process" });
    const replacementStop = vi.fn(async () => undefined);
    f.launch.mockImplementationOnce(async request => {
      expect(request.authorize?.()).toBe(true);
      f.watcher.emit("error", new Error("old watch failed during admission"));
      expect(request.authorize?.()).toBe(false);
      return { kind: "process", liveness: "events", closed: Promise.resolve(), treeClosed: Promise.resolve(),
        isAlive: async () => false, stop: replacementStop };
    });
    f.die(); f.close.resolve(); f.tree.resolve();
    await tick();
    await vi.advanceTimersByTimeAsync(1_000);
    expect(await f.manager.wait(handle.id)).toMatchObject({ status: "failed", errorCode: "PROCESS_LIVENESS_WATCH_FAILED" });
    expect(f.launch).toHaveBeenCalledTimes(2);
    expect(replacementStop).toHaveBeenCalledOnce();
    expect(fs.existsSync(path.join(f.manager.runDirectory(handle.id)!, "unresolved-worker.json"))).toBe(true);
  });

  it("fails closed when the single deadline safety read rejects, without replacing it with polling", async () => {
    const f = monitored();
    const handle = await f.manager.spawn({ task: "unreadable deadline", transport: "process" });
    f.read.mockRejectedValueOnce(new Error("exit observation unreadable"));
    await vi.advanceTimersByTimeAsync(2_000);
    expect(await f.manager.wait(handle.id)).toMatchObject({ status: "failed", errorCode: "PROCESS_LIVENESS_WATCH_FAILED" });
    expect(f.read).toHaveBeenCalledOnce();
    await vi.advanceTimersByTimeAsync(10_000);
    expect(f.read).toHaveBeenCalledOnce();
    f.close.resolve(); f.tree.resolve();
  });

  it.each(["native", "tree", "records"])("fails closed at the deadline with a typed reason after a %s watcher failure", async source => {
    const f = monitored();
    const handle = await f.manager.spawn({ task: "broken watcher", transport: "process" });
    const failure = new Error("event watcher unavailable");
    if (source === "native") f.close.reject(failure);
    else if (source === "tree") f.tree.reject(failure);
    else f.watcher.emit("error", failure);
    await tick();
    await vi.advanceTimersByTimeAsync(1_999);
    expect(f.read).not.toHaveBeenCalled();
    expect(f.manager.status(handle.id).status).toBe("running");
    await vi.advanceTimersByTimeAsync(1);
    const result = await f.manager.wait(handle.id);
    expect(result).toMatchObject({ status: "failed", errorCode: "PROCESS_LIVENESS_WATCH_FAILED" });
    expect(result.error).toContain("event watcher unavailable");
    expect(f.read).toHaveBeenCalledOnce();
    expect(f.stop).not.toHaveBeenCalled();
    expect(fs.existsSync(path.join(f.manager.runDirectory(handle.id)!, "unresolved-worker.json"))).toBe(true);
    await expect(f.manager.cleanup(handle.id)).rejects.toThrow(/lost track|unresolved|exit is unconfirmed/);
    f.close.resolve(); f.tree.resolve();
  });
});
