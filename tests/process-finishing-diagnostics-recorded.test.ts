import { spawn, type ChildProcess } from "node:child_process";
import { EventEmitter } from "node:events";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { Writable } from "node:stream";
import { afterEach, describe, expect, it, vi } from "vitest";
import { spawnDetached } from "../src/agents/transports/process-utils.js";
import { stopTaskCgroup, taskCgroupPids, TaskCgroupStopUnconfirmedError } from "../src/agents/transports/task-cgroup.js";

// PR #722 recorded catch: the capped adapter bypassed forced descendant counting.
// Linux-only retained-fd admission; no real child, signal, cgroup kill or deletion.
vi.mock("node:child_process", async importOriginal => ({
  ...await importOriginal<typeof import("node:child_process")>(),
  spawn: vi.fn(() => { throw new Error("Unexpected real child launch in diagnostics probe"); }),
}));
vi.mock("../src/agents/transports/task-cgroup.js", async importOriginal => {
  const actual = await importOriginal<typeof import("../src/agents/transports/task-cgroup.js")>();
  return { ...actual, taskCgroupPids: vi.fn(actual.taskCgroupPids), stopTaskCgroup: vi.fn() };
});

const exits: (() => void)[] = [];
afterEach(() => {
  exits.splice(0).forEach(exit => exit());
  vi.restoreAllMocks();
  vi.mocked(spawn).mockReset();
  vi.mocked(stopTaskCgroup).mockReset();
  vi.mocked(taskCgroupPids).mockClear();
});

const stat = (pid: number, parent: number, group: number, started: string, state = "S") =>
  `${pid} (recorded) ${[state, parent, group, ...Array(16).fill(0), started].join(" ")}`;

async function fixture() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-finishing-diagnostics-"));
  const workerPid = 2147483647;
  const childPid = 2147483646;
  const zombiePid = 2147483645;
  let leaf = "";
  let relative = "";
  let changed = () => {};
  const gate = new Writable({ write(_chunk, _encoding, done) { done(); } });
  const child = Object.assign(new EventEmitter(), { pid: workerPid, unref: vi.fn(), stdio: [null, null, null, null, gate] });
  const kill = vi.spyOn(process, "kill").mockImplementation(() => { throw new Error("Unexpected process signal in diagnostics probe"); });
  vi.spyOn(fs, "rmSync").mockImplementation(() => {});
  vi.spyOn(fs.promises, "rm").mockResolvedValue();
  const nativeRead = fs.readFileSync.bind(fs);
  const nativeOpen = fs.openSync.bind(fs);
  const nativeLstat = fs.lstatSync.bind(fs);
  const nativeStatfs = fs.statfsSync.bind(fs);
  const mapped = (file: fs.PathLike) => String(file) === `/sys/fs/cgroup${relative}` ? leaf
    : String(file) === `/sys/fs/cgroup${relative}/cgroup.events` ? path.join(leaf, "cgroup.events") : file;
  vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, options?: unknown) => {
    if (String(file) === `/proc/${workerPid}/stat`) return stat(workerPid, 1, workerPid, "100");
    // An orphan in a separate session is still inside the owned capped subtree.
    if (String(file) === `/proc/${childPid}/stat`) return stat(childPid, 1, childPid, "101");
    if (String(file) === `/proc/${zombiePid}/stat`) return stat(zombiePid, 1, zombiePid, "102", "Z");
    if (String(file) === `/proc/${workerPid}/cgroup`) return `0::${relative}\n`;
    return nativeRead(file, options as never);
  }) as typeof fs.readFileSync);
  vi.spyOn(fs, "openSync").mockImplementation((file, flags, mode) => nativeOpen(mapped(file), flags, mode));
  vi.spyOn(fs, "lstatSync").mockImplementation(((file: fs.PathLike, options?: unknown) =>
    nativeLstat(mapped(file), options as never)) as typeof fs.lstatSync);
  vi.spyOn(fs, "statfsSync").mockImplementation(((file: fs.PathLike, options?: unknown) =>
    Object.assign(nativeStatfs(mapped(file), options as never), { type: 0x63677270 })) as typeof fs.statfsSync);
  vi.spyOn(fs, "watch").mockImplementation(((file: fs.PathLike, _options?: unknown, listener?: fs.WatchListener<string>) => {
    if (String(file) === `/sys/fs/cgroup${relative}/cgroup.events`) changed = () => listener?.("change", "cgroup.events");
    return Object.assign(new EventEmitter(), { close: vi.fn(), ref: vi.fn(), unref: vi.fn() });
  }) as unknown as typeof fs.watch);
  vi.mocked(spawn).mockImplementationOnce((_command, args) => {
    const index = args!.indexOf("fabric-scope");
    const marker = args![index + 1]!;
    const unit = args![index + 2]!;
    relative = `/fixture/app.slice/${unit}`;
    leaf = path.join(root, unit);
    fs.mkdirSync(leaf);
    fs.writeFileSync(path.join(leaf, "memory.max"), "1073741824");
    fs.writeFileSync(path.join(leaf, "memory.swap.max"), "0");
    fs.writeFileSync(path.join(leaf, "cgroup.events"), "populated 1\n");
    fs.writeFileSync(path.join(leaf, "cgroup.procs"), `${workerPid}\n${childPid}\n${childPid}\n${zombiePid}\n`);
    fs.writeFileSync(marker, `admitted ${unit}`);
    fs.writeFileSync(`${marker}.cgroup`, `0::${relative}\n`);
    return child as unknown as ChildProcess;
  });
  const empty = () => {
    fs.writeFileSync(path.join(leaf, "cgroup.procs"), "");
    fs.writeFileSync(path.join(leaf, "cgroup.events"), "populated 0\n");
    changed();
  };
  vi.mocked(stopTaskCgroup).mockImplementation(async boundary => {
    expect(boundary.relative).toBe(relative);
    empty();
    return "cgroup.kill";
  });
  const handle = await spawnDetached("worker.mjs", [], root, undefined, undefined,
    { executable: "/recorded/systemd-run", slice: "app.slice", warn: vi.fn(), memoryMaxBytes: 1073741824 }, 7_000, true);
  const closeWorker = () => { child.emit("exit", 0, null); child.emit("close", 0, null); };
  exits.push(() => { closeWorker(); empty(); });
  expect(handle.cgroup).toBe(relative);
  expect(handle.treeClosed).toBeDefined();
  closeWorker(); // Native worker close alone leaves the owned descendant live.
  return { handle, empty, kill };
}

describe.skipIf(process.platform !== "linux")("capped finishing cleanup diagnostics (recorded adapter)", () => {
  it("counts the live orphan birth once, excluding the worker and zombies, before owned stop", async () => {
    const f = await fixture();
    expect(f.handle.forcedCleanupCount?.()).toBe(0);
    await f.handle.stop();
    await f.handle.treeClosed;
    expect(f.handle.forcedCleanupCount?.()).toBe(1);
    await f.handle.stop();
    expect(f.handle.forcedCleanupCount?.()).toBe(1);
    expect(stopTaskCgroup).toHaveBeenCalledOnce();
    expect(f.handle.lostContact()).toBeUndefined();
    expect(f.kill).not.toHaveBeenCalled();
  });

  it("keeps a naturally empty subtree at count zero without issuing owned kill", async () => {
    const f = await fixture();
    f.empty();
    await f.handle.treeClosed;
    await f.handle.stop();
    expect(f.handle.forcedCleanupCount?.()).toBe(0);
    expect(stopTaskCgroup).not.toHaveBeenCalled();
    expect(f.handle.lostContact()).toBeUndefined();
    expect(f.kill).not.toHaveBeenCalled();
  });

  it("still joins owned stop when its diagnostic census fails", async () => {
    const f = await fixture();
    vi.mocked(taskCgroupPids).mockImplementationOnce(() => { throw new Error("diagnostic read failed"); });
    await f.handle.stop();
    await f.handle.treeClosed;
    expect(stopTaskCgroup).toHaveBeenCalledOnce();
    expect(f.handle.forcedCleanupCount?.()).toBe(0);
    expect(f.handle.lostContact()).toBeUndefined();
    expect(f.kill).not.toHaveBeenCalled();
  });

  it("preserves the exact typed owned-stop failure despite a diagnostic census failure", async () => {
    const f = await fixture();
    const failure = new TaskCgroupStopUnconfirmedError(f.handle.cgroup!, [2147483646], "recorded subtree remains populated");
    vi.mocked(taskCgroupPids).mockImplementationOnce(() => { throw new Error("diagnostic read failed"); });
    vi.mocked(stopTaskCgroup).mockRejectedValueOnce(failure);
    await expect(f.handle.stop()).rejects.toBe(failure);
    expect(failure.code).toBe("TASK_CGROUP_STOP_UNCONFIRMED");
    expect(failure.pids).toEqual([2147483646]);
    expect(await f.handle.isAlive()).toBe(true); // Failed owned stop cannot certify the populated tree.
    expect(stopTaskCgroup).toHaveBeenCalledOnce();
    expect(f.handle.forcedCleanupCount?.()).toBe(0);
    expect(f.kill).not.toHaveBeenCalled();
  });
});
