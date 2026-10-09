import fs from "node:fs";
import path from "node:path";
import { EventEmitter } from "node:events";
import { spawn, type ChildProcess, type SpawnOptions } from "node:child_process";
import { afterEach, describe, expect, it, vi } from "vitest";
import * as processUtils from "../src/agents/transports/process-utils.js";
import * as cgroup from "../src/process-cgroup.js";
import { releaseScopedChild, ScopeAdmissionError, spawnScopedExecution } from "../src/worker/scope-spawn.js";

afterEach(() => { vi.restoreAllMocks(); vi.useRealTimers(); });
describe("scope spawn compatibility", () => {
  it.each(["win32", "darwin"] as const)("preserves exact arguments/options/environment on %s", async platform => {
    vi.spyOn(process, "platform", "get").mockReturnValue(platform);
    const find = vi.spyOn(processUtils, "findExecutable");
    const child = new EventEmitter() as ChildProcess, args = ["literal '$value'", "line\nline"];
    const options: SpawnOptions = { env: { KEEP: "literal", XDG_RUNTIME_DIR: "/fixture" }, stdio: "pipe" };
    const launch = vi.fn(() => child);
    expect(await spawnScopedExecution(launch, "target", args, options)).toBe(child);
    expect(launch).toHaveBeenCalledExactlyOnceWith("target", args, options); expect(find).not.toHaveBeenCalled();
  });
  it("preserves direct options when the parent cannot retain scopes", async () => {
    const child = new EventEmitter() as ChildProcess, options: SpawnOptions = { stdio: "ignore" };
    const launch = vi.fn(() => child);
    expect(await spawnScopedExecution(launch, "target", [], options, false)).toBe(child);
    expect(launch.mock.calls[0]).toEqual(["target", [], options]);
  });
});
describe.skipIf(process.platform !== "linux")("spawn-result scope authority", () => {
  const setup = (migrate = false, mark = true) => {
    vi.spyOn(processUtils, "findExecutable").mockImplementation(name => `/fixture/${name}`);
    vi.spyOn(processUtils, "executeFile").mockResolvedValue({ stdout: "/user.slice/user@1002.service\n", stderr: "" });
    const identity = { pid: 123, parent: 1, group: 123, session: 123, started: "1000" };
    vi.spyOn(cgroup, "executionIdentity").mockReturnValue(identity);
    let expected = "", current = "", marker = "";
    const gate = { on: vi.fn(), end: vi.fn((release?: string) => { if (!release) child.emit("close", 125); }) };
    const child = Object.assign(new EventEmitter(), { pid: 123, stdio: [null, null, null, gate], kill: vi.fn() }) as unknown as ChildProcess;
    const receipt = { directory: "", execution: identity, pin: { dev: 1, ino: 2, uid: process.getuid!() },
      members: () => [123], exited: () => false, signal: vi.fn(), dispose: vi.fn(),
      verify: vi.fn(() => { if (current !== expected) throw new Error("Launcher left its spawn scope"); }) } as cgroup.CgroupCustody;
    const pin = vi.spyOn(cgroup, "cgroupCustody").mockImplementation(directory => { receipt.directory = directory; return receipt; });
    vi.spyOn(cgroup, "processScopePath").mockImplementation(() => {
      const snapshot = current;
      if (migrate) current = expected.replace("/app.slice/", "/foreign.slice/");
      return snapshot;
    });
    const launch = vi.fn((_command: string, args: readonly string[], _options: SpawnOptions) => {
      const unit = args.find(arg => arg.startsWith("--unit="))!.slice(7);
      expected = cgroup.scopeDirectory("/user.slice/user@1002.service", "app.slice", unit); current = expected;
      marker = args[args.indexOf("fabric-execution") + 1]!;
      // Same-UID writer advertises a different scope with the SAME basename.
      if (mark) fs.writeFileSync(marker, `0::/foreign.slice/${unit}\n`);
      return child;
    });
    return { child, gate, receipt, pin, launch, expected: () => expected, marker: () => marker };
  };
  it("pins/verifies spawn placement before trusting marker; marker bytes never choose custody", async () => {
    const f = setup(); const read = vi.spyOn(fs, "readFileSync");
    const child = await spawnScopedExecution(f.launch, "target", [], { stdio: "ignore" });
    expect(f.pin).toHaveBeenCalledExactlyOnceWith(f.expected(), f.receipt.execution);
    expect(cgroup.executionCgroups.get(child)?.directory).toBe(f.expected()); expect(f.gate.end).not.toHaveBeenCalled();
    expect(read.mock.calls.some(call => String(call[0]) === f.marker())).toBe(false);
    releaseScopedChild(child); expect(f.gate.end).toHaveBeenCalledExactlyOnceWith("release\n"); child.emit("close", 0);
  });
  it("watches before reading and sees a marker created after subscribe with no sub-second timers", async () => {
    const f = setup(false, false), watch = vi.spyOn(fs, "watch"), exists = vi.spyOn(fs, "existsSync");
    const timers = vi.spyOn(globalThis, "setTimeout"), intervals = vi.spyOn(globalThis, "setInterval");
    const cleared = vi.spyOn(globalThis, "clearTimeout");
    const pending = spawnScopedExecution(f.launch, "target", [], { stdio: "ignore" });
    await Promise.resolve();
    expect(watch).toHaveBeenCalledExactlyOnceWith(path.dirname(f.marker()), expect.any(Function));
    const watcher = watch.mock.results[0]!.value as fs.FSWatcher, close = vi.spyOn(watcher, "close");
    const firstRead = exists.mock.calls.findIndex(call => String(call[0]) === f.marker());
    expect(firstRead).toBeGreaterThanOrEqual(0);
    expect(watch.mock.invocationCallOrder[0]).toBeLessThan(exists.mock.invocationCallOrder[firstRead]!);
    // Real filesystem notification, not a mock callback or a polling wait.
    setImmediate(() => fs.writeFileSync(f.marker(), "admitted"));
    const child = await pending;
    expect(timers.mock.calls.map(call => call[1])).toEqual([5_000]); expect(intervals).not.toHaveBeenCalled();
    expect(cleared).toHaveBeenCalledWith(timers.mock.results[0]!.value); expect(close).toHaveBeenCalledOnce();
    expect(f.receipt.verify).toHaveBeenCalled(); expect(f.gate.end).not.toHaveBeenCalled();
    releaseScopedChild(child); child.emit("close", 0);
  });
  it("reads a marker created during subscription without needing a later notification", async () => {
    const f = setup(false, false), original = fs.watch.bind(fs);
    vi.spyOn(fs, "watch").mockImplementation(((directory: fs.PathLike, check: fs.WatchListener<string>) => {
      const watcher = original(directory, check);
      fs.writeFileSync(f.marker(), "admitted"); return watcher;
    }) as typeof fs.watch);
    const child = await spawnScopedExecution(f.launch, "target", [], { stdio: "ignore" });
    expect(cgroup.executionCgroups.get(child)).toBe(f.receipt);
    releaseScopedChild(child); child.emit("close", 0);
  });
  it("a close during watcher subscription leaves neither a watcher nor a deadline behind", async () => {
    const f = setup(false, false); vi.useFakeTimers();
    const original = fs.watch.bind(fs); let close: ReturnType<typeof vi.spyOn> | undefined;
    vi.spyOn(fs, "watch").mockImplementation(((directory: fs.PathLike, check: fs.WatchListener<string>) => {
      const watcher = original(directory, check); close = vi.spyOn(watcher, "close");
      f.child.emit("close", 125); return watcher;
    }) as typeof fs.watch);
    await expect(spawnScopedExecution(f.launch, "target", [], { stdio: "ignore" }))
      .rejects.toMatchObject({ code: "ERR_SCOPE_ADMISSION", reason: "closed" });
    expect(close).toHaveBeenCalledOnce(); expect(vi.getTimerCount()).toBe(0);
    expect(f.launch).toHaveBeenCalledOnce(); expect(f.child.listenerCount("close")).toBe(0);
  });
  it.each([false, true])("native close before marker is a typed failure without replay (unpinned=%s)", async unpinned => {
    const f = setup(false, false), watch = vi.spyOn(fs, "watch");
    if (unpinned) vi.spyOn(cgroup, "processScopePath").mockReturnValue(undefined);
    const outcome = spawnScopedExecution(f.launch, "target", [], { stdio: "ignore" }).catch(error => error);
    await Promise.resolve();
    const close = vi.spyOn(watch.mock.results[0]!.value as fs.FSWatcher, "close");
    f.child.emit("close", 125);
    const error = await outcome;
    expect(error).toBeInstanceOf(ScopeAdmissionError); expect(error).toMatchObject({ code: "ERR_SCOPE_ADMISSION", reason: "closed" });
    expect(close).toHaveBeenCalledOnce(); expect(f.launch).toHaveBeenCalledOnce(); expect(f.child.kill).not.toHaveBeenCalled();
    expect(f.gate.end).toHaveBeenCalledExactlyOnceWith(); expect(f.child.listenerCount("close")).toBe(0);
    expect(fs.existsSync(path.dirname(f.marker()))).toBe(false);
  });
  it.each([false, true])("abort settles admission and removes its subscription (alreadyAborted=%s)", async alreadyAborted => {
    const f = setup(false, false), controller = new AbortController();
    const remove = vi.spyOn(controller.signal, "removeEventListener"), watch = vi.spyOn(fs, "watch");
    if (alreadyAborted) controller.abort("cancelled");
    const outcome = spawnScopedExecution(f.launch, "target", [], { stdio: "ignore" }, true,
      { executable: "/fixture/systemd-run", slice: "app.slice", warn: vi.fn(), signal: controller.signal }).catch(error => error);
    await Promise.resolve();
    if (!alreadyAborted) controller.abort("cancelled");
    expect(await outcome).toMatchObject({ code: "ERR_SCOPE_ADMISSION", reason: "aborted", cause: "cancelled" });
    expect(remove).toHaveBeenCalledExactlyOnceWith("abort", expect.any(Function));
    expect(watch).toHaveBeenCalledOnce(); expect(f.launch).toHaveBeenCalledOnce();
    expect(f.gate.end).toHaveBeenCalledExactlyOnceWith(); expect(f.child.listenerCount("close")).toBe(0);
  });
  it.each(["watch", "launcher"] as const)("%s errors settle admission, clean up and never replay", async source => {
    const f = setup(false, false), watch = vi.spyOn(fs, "watch"), cause = new Error("fixture failure");
    const outcome = spawnScopedExecution(f.launch, "target", [], { stdio: "ignore" }).catch(error => error);
    await Promise.resolve();
    const watcher = watch.mock.results[0]!.value as fs.FSWatcher, close = vi.spyOn(watcher, "close");
    (source === "watch" ? watcher : f.child).emit("error", cause);
    expect(await outcome).toMatchObject({ code: "ERR_SCOPE_ADMISSION", reason: source, cause });
    expect(close).toHaveBeenCalledOnce(); expect(f.launch).toHaveBeenCalledOnce();
    expect(f.gate.end).toHaveBeenCalledExactlyOnceWith(); expect(f.receipt.dispose).toHaveBeenCalledOnce();
  });
  it("watch setup failure is typed, closes the gate and refuses replay", async () => {
    const f = setup(false, false), cause = new Error("watch unavailable");
    vi.spyOn(fs, "watch").mockImplementation(() => { throw cause; });
    await expect(spawnScopedExecution(f.launch, "target", [], { stdio: "ignore" }))
      .rejects.toMatchObject({ code: "ERR_SCOPE_ADMISSION", reason: "watch", cause });
    expect(f.gate.end).toHaveBeenCalledExactlyOnceWith(); expect(f.launch).toHaveBeenCalledOnce();
    expect(f.child.listenerCount("close")).toBe(0);
  });
  it("unrelated marker-directory events do not poll or reset the single admission deadline", async () => {
    const f = setup(false, false); vi.useFakeTimers();
    const watcher = Object.assign(new EventEmitter(), { close: vi.fn() });
    const watch = vi.spyOn(fs, "watch").mockReturnValue(watcher as unknown as fs.FSWatcher);
    const timers = vi.spyOn(globalThis, "setTimeout");
    const outcome = spawnScopedExecution(f.launch, "target", [], { stdio: "ignore" }).catch(error => error);
    await Promise.resolve();
    const check = watch.mock.calls[0]![1] as fs.WatchListener<string>;
    await vi.advanceTimersByTimeAsync(4_999); check("rename", "unrelated"); check("change", null);
    expect(f.gate.end).not.toHaveBeenCalled(); expect(timers.mock.calls.map(call => call[1])).toEqual([5_000]);
    await vi.advanceTimersByTimeAsync(1);
    expect(await outcome).toMatchObject({ code: "ERR_SCOPE_ADMISSION", reason: "timeout" });
    expect(watcher.close).toHaveBeenCalledOnce(); expect(f.child.listenerCount("close")).toBe(0);
    expect(f.receipt.dispose).toHaveBeenCalledOnce(); expect(f.launch).toHaveBeenCalledOnce();
  });
  it("migration between marker write and pin cannot redirect custody or open the target gate", async () => {
    const f = setup(true);
    await expect(spawnScopedExecution(f.launch, "target", [], { stdio: "ignore" })).rejects.toThrow("left its spawn scope");
    expect(f.pin).toHaveBeenCalledExactlyOnceWith(f.expected(), f.receipt.execution);
    expect(f.launch).toHaveBeenCalledOnce(); expect(f.gate.end).toHaveBeenCalledExactlyOnceWith(); expect(f.child.kill).not.toHaveBeenCalled();
    expect(f.receipt.dispose).toHaveBeenCalledOnce();
  });
  it("a marker written before the first membership check cannot downgrade a migrated launcher", async () => {
    const f = setup(); vi.useFakeTimers(); vi.spyOn(cgroup, "processScopePath").mockReturnValue("/sys/fs/cgroup/foreign.scope");
    const pending = spawnScopedExecution(f.launch, "target", [], { stdio: "ignore" });
    const outcome = pending.then(() => undefined, error => error);
    await vi.advanceTimersByTimeAsync(5_000);
    expect(await outcome).toMatchObject({ message: expect.stringContaining("refusing execution replay") });
    expect(f.pin).not.toHaveBeenCalled(); expect(f.launch).toHaveBeenCalledOnce(); expect(f.child.kill).not.toHaveBeenCalled();
  });
  it("failed pinned admission retains grace then uses atomic scope KILL, never launcher.kill", async () => {
    const f = setup(true); vi.useFakeTimers(); f.gate.end.mockImplementation(() => {});
    vi.mocked(f.receipt.signal).mockImplementation(async () => { f.child.emit("close", 125); });
    const outcome = spawnScopedExecution(f.launch, "target", [], { stdio: "ignore" }).then(() => undefined, error => error);
    await vi.advanceTimersByTimeAsync(1_999); expect(f.receipt.signal).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1); expect((await outcome).message).toContain("left its spawn scope");
    expect(f.receipt.signal).toHaveBeenCalledExactlyOnceWith("SIGKILL"); expect(f.child.kill).not.toHaveBeenCalled();
  });
  it("an unconfirmed unpinned launcher vetoes replay and exposes existing manager cleanup debt", async () => {
    const f = setup(); vi.useFakeTimers(); f.gate.end.mockImplementation(() => {});
    vi.spyOn(cgroup, "processScopePath").mockReturnValue(undefined);
    const outcome = spawnScopedExecution(f.launch, "target", [], { stdio: "ignore" }).then(() => undefined, error => error);
    await vi.advanceTimersByTimeAsync(7_000);
    expect(await outcome).toMatchObject({ launchOutcome: "unknown", cleanupPending: true, sessionId: "123" });
    expect(f.launch).toHaveBeenCalledOnce(); expect(f.child.kill).not.toHaveBeenCalled(); f.child.emit("close", 125);
  });
  it("an unreadable initial launcher birth closes its gate and never leaks or replays target", async () => {
    const f = setup(); vi.spyOn(cgroup, "executionIdentity").mockImplementation(() => { throw new Error("birth unreadable"); });
    await expect(spawnScopedExecution(f.launch, "target", [], { stdio: "ignore" })).rejects.toThrow("birth unreadable");
    expect(f.gate.end).toHaveBeenCalledExactlyOnceWith(); expect(f.launch).toHaveBeenCalledOnce(); expect(f.child.kill).not.toHaveBeenCalled();
  });
  it("a late migration before gate release is rejected rather than replayed", async () => {
    const f = setup(); const child = await spawnScopedExecution(f.launch, "target", [], { stdio: "ignore" });
    f.receipt.verify = () => { throw new Error("late migration"); };
    expect(() => releaseScopedChild(child)).toThrow("late migration"); expect(f.gate.end).not.toHaveBeenCalled();
    f.receipt.dispose(); child.emit("close", 125);
  });
  it("a real launcher close before admission is typed and does not replay the target", async () => {
    vi.spyOn(processUtils, "findExecutable").mockImplementation(name => name === "systemd-run" ? "/bin/false" : "/usr/bin/systemctl");
    vi.spyOn(processUtils, "executeFile").mockResolvedValue({ stdout: "/user.slice/user@1002.service\n", stderr: "" });
    const options: SpawnOptions = { env: process.env, stdio: "ignore" };
    const launch = vi.fn((command: string, args: readonly string[], opts: SpawnOptions) => spawn(command, [...args], opts));
    await expect(spawnScopedExecution(launch, "target", [], options))
      .rejects.toMatchObject({ code: "ERR_SCOPE_ADMISSION", reason: "closed" });
    expect(launch).toHaveBeenCalledOnce();
  });
});
