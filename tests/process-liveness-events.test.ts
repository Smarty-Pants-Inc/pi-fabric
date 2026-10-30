import { spawn, type ChildProcess } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { EventEmitter } from "node:events";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { ProcessTreeCustodyUnconfirmedError, spawnDetached } from "../src/agents/transports/process-utils.js";
import type { AgentTransportHandle } from "../src/agents/types.js";
import { taskCgroupUnavailableReason } from "./helpers/task-cgroup-capability.js";
import * as taskCgroup from "../src/agents/transports/task-cgroup.js";

vi.mock("node:child_process", async importOriginal => {
  const actual = await importOriginal<typeof import("node:child_process")>();
  return { ...actual, spawn: vi.fn(actual.spawn) };
});

const roots: string[] = [];
const managers: AgentManager[] = [];
const fixtureFds = new Map<number, { dev: number; ino: number }>();
const retainFixtureFd = (fd: number): number => { fixtureFds.set(fd, fs.fstatSync(fd)); return fd; };
afterEach(async () => {
  // Restore spies while their fake-clock originals are still installed; doing
  // this after useRealTimers would reinstall a detached fake setTimeout.
  vi.restoreAllMocks();
  vi.useRealTimers();
  vi.unstubAllEnvs();
  for (const [fd, identity] of fixtureFds) {
    try {
      const held = fs.fstatSync(fd);
      if (held.dev === identity.dev && held.ino === identity.ino) fs.closeSync(fd);
    } catch { /* already closed; never close a recycled foreign descriptor */ }
  }
  fixtureFds.clear();
  for (const manager of managers.splice(0)) await manager.close().catch(() => undefined);
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  vi.mocked(spawn).mockReset();
  vi.mocked(spawn).mockImplementation((await vi.importActual<typeof import("node:child_process")>("node:child_process")).spawn);
});
const root = () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-event-liveness-"));
  roots.push(directory);
  return directory;
};
const tick = () => new Promise<void>(resolve => setImmediate(resolve));

const legacyUserEnvironment = (): void => {
  vi.stubEnv("DBUS_SESSION_BUS_ADDRESS", undefined);
  vi.stubEnv("XDG_RUNTIME_DIR", undefined);
};

async function nativeWorker(run: (handle: AgentTransportHandle, child: ChildProcess) => Promise<void>) {
  legacyUserEnvironment(); // receipt/timer assertions name the uncapped legacy route
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

describe("unscoped legacy process liveness", () => {
  it("arms no recurring liveness timer for an active process transport", async () => {
    const interval = vi.spyOn(globalThis, "setInterval");
    const timeout = vi.spyOn(globalThis, "setTimeout");
    await nativeWorker(async handle => {
      expect(handle.liveness).toBeUndefined();
      expect(handle.treeClosed).toBeUndefined();
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

type MarkerFailure = "symlink" | "unowned" | "directory" | "mismatched" | "empty";
type AdmissionOptions = { present?: boolean; race?: boolean; armFailure?: boolean; markerFailure?: MarkerFailure; teardownMarker?: "valid" | MarkerFailure; admitOnCheck?: number };
async function fixtureAdmission(options: AdmissionOptions = {}) {
  vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout", "setInterval", "clearInterval"] });
  const directory = root();
  const workerPath = path.join(directory, "worker.mjs");
  fs.writeFileSync(workerPath, "");
  const controller = new AbortController();
  const removeAbort = vi.spyOn(controller.signal, "removeEventListener");
  const order: string[] = [];
  const watcher = Object.assign(new EventEmitter(), { close: vi.fn(() => { order.push("watch-close"); }) }) as unknown as fs.FSWatcher;
  const children = [700_000_020, 700_000_021].map(pid => Object.assign(new EventEmitter(), { pid, unref: vi.fn(), channel: {}, alive: true }));
  const close = (child = children[0]!) => {
    if (!child.alive) return;
    child.alive = false;
    child.emit("exit", 0); child.emit("close", 0);
  };
  let marker = "";
  let markerIdentity = "";
  let markerFailure: MarkerFailure | undefined;
  const writeMarker = (failure?: MarkerFailure): void => {
    markerFailure = failure;
    fs.rmSync(marker, { recursive: true, force: true });
    if (failure === "directory") { fs.mkdirSync(marker); return; }
    const content = failure === "empty" ? "" : failure === "mismatched" ? `${markerIdentity.slice(0, -1)}!` : markerIdentity;
    if (failure === "symlink") {
      const target = path.join(path.dirname(marker), "other-marker");
      fs.writeFileSync(target, content);
      fs.symlinkSync(target, marker);
    } else fs.writeFileSync(marker, content);
  };
  const nativeRead = fs.readFileSync.bind(fs);
  vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, readOptions?: unknown) => {
    const child = children.find(child => String(file) === `/proc/${child.pid}/stat`);
    if (child?.alive) {
      const fields = Array<string>(20).fill("0");
      fields[0] = "S"; fields[1] = "1"; fields[2] = String(child.pid); fields[19] = String(child.pid);
      return `${child.pid} (worker) ${fields.join(" ")}`;
    }
    if (child || children.some(child => String(file) === `/proc/${child.pid}/cgroup`)) {
      throw Object.assign(new Error("no owned cgroup receipt"), { code: "ENOENT" });
    }
    return nativeRead(file, readOptions as never);
  }) as typeof fs.readFileSync);
  const nativeReaddir = fs.readdirSync.bind(fs);
  vi.spyOn(fs, "readdirSync").mockImplementation(((file: fs.PathLike, readOptions?: unknown) => {
    if (String(file) === "/proc") return children.filter(child => child.alive).map(child => String(child.pid));
    return nativeReaddir(file, readOptions as never);
  }) as typeof fs.readdirSync);
  vi.spyOn(process, "kill").mockImplementation((pid, signal) => {
    const child = children.find(child => child.pid === Math.abs(pid));
    if (signal === "SIGTERM" || signal === "SIGKILL") {
      order.push("stop");
      if (options.teardownMarker) writeMarker(options.teardownMarker === "valid" ? undefined : options.teardownMarker);
      close(child);
    }
    return true;
  });
  vi.mocked(spawn).mockClear();
  vi.mocked(spawn).mockImplementationOnce((_command, args) => {
    const markerIndex = args!.indexOf("fabric-scope") + 1;
    marker = args![markerIndex]!;
    markerIdentity = `admitted ${args![markerIndex + 1]}`;
    if (options.present) writeMarker(options.markerFailure);
    return children[0] as unknown as ChildProcess;
  }).mockImplementationOnce(() => children[1] as unknown as ChildProcess);
  let armed!: () => void;
  const arming = new Promise<void>(resolve => { armed = resolve; });
  const watch = vi.spyOn(fs, "watch").mockImplementation((...args: Parameters<typeof fs.watch>) => {
    order.push("watch");
    expect(String(args[0])).toBe(path.dirname(marker));
    const listener = args.at(-1);
    if (typeof listener === "function") watcher.on("change", listener);
    armed();
    if (options.armFailure) throw new Error("directory watcher unavailable");
    if (options.race) writeMarker(options.markerFailure);
    return watcher;
  });
  const nativeLstat = fs.lstatSync.bind(fs);
  let markerChecks = 0;
  const lstat = vi.spyOn(fs, "lstatSync").mockImplementation(((file: fs.PathLike, fsOptions?: unknown) => {
    if (String(file) === marker) {
      order.push("check");
      if (++markerChecks === options.admitOnCheck) writeMarker();
    }
    const stat = nativeLstat(file, fsOptions as never);
    if (String(file) === marker && markerFailure === "unowned") stat.uid = process.getuid!() + 1;
    return stat;
  }) as typeof fs.lstatSync);
  const interval = vi.spyOn(globalThis, "setInterval");
  const timeout = vi.spyOn(globalThis, "setTimeout");
  const warn = vi.fn();
  const launch = spawnDetached(workerPath, [], directory, { signal: controller.signal }, undefined,
    { executable: "/fixture-systemd-run", slice: "fixture.slice", warn });
  void launch.catch(() => undefined);
  await arming;
  return {
    launch, watcher, watch, controller, removeAbort, order, interval, timeout, warn, children,
    markerChecks: () => lstat.mock.calls.filter(([file]) => String(file) === marker).length,
    writeMarker,
    admit: (filename: string | Buffer = "admitted", failure?: MarkerFailure) => { writeMarker(failure); watcher.emit("change", "rename", filename); },
    close,
    finish: () => { for (const child of children) close(child); },
  };
}

function expectAdmissionCleaned(f: Awaited<ReturnType<typeof fixtureAdmission>>, armed = true) {
  if (armed) expect(f.watcher.close).toHaveBeenCalledOnce();
  else expect(f.watcher.close).not.toHaveBeenCalled(); // constructor returned no watcher
  expect(f.children[0]!.listenerCount("close")).toBeLessThanOrEqual(1); // only native close custody remains
  if (armed) expect(f.removeAbort).toHaveBeenCalledWith("abort", expect.any(Function));
  expect(vi.getTimerCount()).toBe(0);
  expect(f.interval).not.toHaveBeenCalled();
  expect(f.markerChecks()).toBeGreaterThan(0); // admission checks authenticate the marker, never just its name
}

describe.skipIf(process.platform !== "linux")("event-driven scope admission marker", () => {
  it.each(["admitted", Buffer.from("admitted")])("resolves on a matching directory event without polling (%s)", async filename => {
    const f = await fixtureAdmission();
    expect(f.order).toEqual(["watch", "check"]);
    expect(f.watch).toHaveBeenCalledOnce();
    expect(f.timeout.mock.calls.map(([, ms]) => ms)).toEqual([5_000]);
    expect(vi.getTimerCount()).toBe(1);
    f.watcher.emit("change", "rename", "admitted.cgroup");
    f.watcher.emit("change", "rename", null);
    await vi.advanceTimersByTimeAsync(4_999);
    expect(f.markerChecks()).toBe(1);
    expect(spawn).toHaveBeenCalledOnce();
    expect(f.watcher.close).not.toHaveBeenCalled();
    f.admit(filename);
    const handle = await f.launch;
    expect(handle.pid).toBe(f.children[0]!.pid);
    expect(spawn).toHaveBeenCalledOnce();
    expect(f.markerChecks()).toBe(2); // initial check plus authenticating the matching event
    expect(f.warn).not.toHaveBeenCalled();
    expectAdmissionCleaned(f);
    f.finish();
  });

  it("ignores rename-away and other-file events even when their filename resembles admission", async () => {
    const f = await fixtureAdmission();
    f.watcher.emit("change", "rename", "admitted"); // rename/delete notification without a marker
    await tick();
    expect(f.markerChecks()).toBe(2);
    expect(f.watcher.close).not.toHaveBeenCalled();
    expect(spawn).toHaveBeenCalledOnce();
    f.writeMarker();
    f.watcher.emit("change", "rename", "other-file"); // authentic marker is not a matching event
    await tick();
    expect(f.markerChecks()).toBe(2);
    expect(f.watcher.close).not.toHaveBeenCalled();
    f.watcher.emit("change", "rename", Buffer.from("admitted"));
    const handle = await f.launch;
    expect(handle.pid).toBe(f.children[0]!.pid);
    expect(f.markerChecks()).toBe(3);
    expectAdmissionCleaned(f);
    f.finish();
  });

  it.each((["symlink", "unowned", "directory", "mismatched", "empty"] as const).flatMap(failure =>
    [true, false].map(present => ({ failure, present }))))("keeps waiting for $failure marker (initial=$present)", async ({ failure, present }) => {
    const f = await fixtureAdmission({ present, markerFailure: failure });
    if (!present) f.admit("admitted", failure);
    await tick();
    expect(f.watcher.close).not.toHaveBeenCalled();
    expect(spawn).toHaveBeenCalledOnce();
    expect(vi.getTimerCount()).toBe(1);
    if (present) { f.watcher.emit("change", "rename", "admitted"); await tick(); }
    expect(f.markerChecks()).toBe(2);
    expect(f.watcher.close).not.toHaveBeenCalled();
    f.admit(); // a subsequent authentic marker admits this launch
    const handle = await f.launch;
    expect(handle.pid).toBe(f.children[0]!.pid);
    expect(f.markerChecks()).toBe(3);
    expect(handle.treeClosed).toBeUndefined(); // fixture deliberately has no verifiable cgroup receipt
    expectAdmissionCleaned(f);
    f.finish();
  });

  it.each(["valid", "symlink", "unowned", "mismatched"] as const)("authenticates a %s marker arriving during teardown without replaying admission", async teardownMarker => {
    const f = await fixtureAdmission({ teardownMarker });
    await vi.advanceTimersByTimeAsync(5_000);
    const handle = await f.launch;
    expect(handle.pid).toBe(f.children[teardownMarker === "valid" ? 0 : 1]!.pid);
    expect(spawn).toHaveBeenCalledTimes(teardownMarker === "valid" ? 1 : 2);
    if (teardownMarker === "valid") {
      expect(f.warn).toHaveBeenCalledWith("scope admission marker appeared during teardown; the admitted worker was stopped and will be reported lost");
    } else expect(f.warn).toHaveBeenCalledOnce();
    expect(f.markerChecks()).toBe(4); // initial, deadline, pre-stop, then post-stop no-replay checks
    expect(handle.treeClosed).toBeUndefined();
    expectAdmissionCleaned(f);
    f.finish();
  });

  it.each([{ present: true }, { race: true }])("checks once after arming and immediately accepts a present/racing marker: %j", async options => {
    const f = await fixtureAdmission(options);
    const handle = await f.launch;
    expect(f.order).toEqual(["watch", "check", "watch-close"]);
    expect(handle.pid).toBe(f.children[0]!.pid);
    expect(f.markerChecks()).toBe(1);
    expect(f.timeout).not.toHaveBeenCalled();
    expect(spawn).toHaveBeenCalledOnce();
    expectAdmissionCleaned(f);
    f.finish();
  });

  it.each(["deadline", "watcher error"] as const)("keeps an already admitted worker when %s ends the delayed-watch wait", async end => {
    const f = await fixtureAdmission();
    f.writeMarker(); // the scope shim has admitted/exec'd, but inotify has not reached this loop yet
    if (end === "deadline") await vi.advanceTimersByTimeAsync(5_000);
    else f.watcher.emit("error", new Error("watch failed after marker publication"));
    const handle = await f.launch;
    expect(handle.pid).toBe(f.children[0]!.pid);
    expect(f.order).not.toContain("stop");
    expect(spawn).toHaveBeenCalledOnce();
    expect(f.warn).not.toHaveBeenCalled();
    expectAdmissionCleaned(f);
    f.finish();
  });

  it("stops an admitted scope when its launch authority is revoked", async () => {
    const f = await fixtureAdmission();
    f.writeMarker();
    f.controller.abort();
    await expect(f.launch).rejects.toThrow("Agent launch aborted");
    expect(f.order).toContain("stop");
    expect(spawn).toHaveBeenCalledOnce();
    expect(f.warn).not.toHaveBeenCalled();
    expectAdmissionCleaned(f);
    f.finish();
  });

  it("rechecks the marker immediately before teardown when admission races the completed wait", async () => {
    const f = await fixtureAdmission({ admitOnCheck: 3 });
    await vi.advanceTimersByTimeAsync(5_000); // initial and deadline checks miss it
    const handle = await f.launch; // the pre-stop check sees the newly published marker
    expect(handle.pid).toBe(f.children[0]!.pid);
    expect(f.markerChecks()).toBe(3);
    expect(f.order).not.toContain("stop");
    expect(spawn).toHaveBeenCalledOnce();
    expect(f.warn).not.toHaveBeenCalled();
    expectAdmissionCleaned(f);
    f.finish();
  });

  it("uses one 5s deadline, closes the watcher, and falls back to an unscoped legacy launch", async () => {
    const f = await fixtureAdmission();
    await vi.advanceTimersByTimeAsync(4_999);
    expect(f.markerChecks()).toBe(1);
    expect(f.watcher.close).not.toHaveBeenCalled();
    expect(spawn).toHaveBeenCalledOnce();
    await vi.advanceTimersByTimeAsync(1);
    const handle = await f.launch;
    expect(handle.pid).toBe(f.children[1]!.pid);
    expect(handle.treeClosed).toBeUndefined();
    expect(spawn).toHaveBeenCalledTimes(2);
    expect(vi.mocked(spawn).mock.calls[1]![1]).toEqual([expect.stringContaining("worker.mjs")]);
    expect(f.timeout.mock.calls.filter(([, ms]) => ms === 5_000)).toHaveLength(1);
    expect(f.timeout.mock.calls.some(([, ms]) => ms === 10)).toBe(false);
    expect(f.markerChecks()).toBe(4); // initial, deadline, pre-stop, then the no-replay check AFTER teardown
    expect(f.order.indexOf("watch-close")).toBeLessThan(f.order.indexOf("stop"));
    expect(f.warn).toHaveBeenCalledOnce();
    expect(f.warn).toHaveBeenCalledWith("scope admission timed out before an authenticated marker");
    expectAdmissionCleaned(f);
    f.finish();
  });

  it.each(["constructor", "asynchronous"] as const)("falls back to legacy rather than polling on %s watcher failure", async failure => {
    const f = await fixtureAdmission({ armFailure: failure === "constructor" });
    if (failure === "asynchronous") f.watcher.emit("error", new Error("watch failed"));
    const handle = await f.launch;
    expect(handle.pid).toBe(f.children[1]!.pid);
    expect(handle.treeClosed).toBeUndefined();
    expect(spawn).toHaveBeenCalledTimes(2);
    expect(f.warn).toHaveBeenCalledOnce();
    expect(f.timeout.mock.calls.some(([, ms]) => ms === 10)).toBe(false);
    expectAdmissionCleaned(f, failure !== "constructor");
    f.finish();
  });

  it("closes the admission watcher on native close and uses legacy fallback without waiting for the deadline", async () => {
    const f = await fixtureAdmission();
    const started = Date.now();
    f.close();
    const handle = await f.launch;
    expect(handle.pid).toBe(f.children[1]!.pid);
    expect(handle.treeClosed).toBeUndefined();
    expect(Date.now()).toBe(started);
    expectAdmissionCleaned(f);
    f.finish();
  });

  it("closes the watcher and clears its deadline on cancellation without replaying the launch", async () => {
    const f = await fixtureAdmission();
    f.controller.abort();
    await expect(f.launch).rejects.toThrow("Agent launch aborted");
    expect(spawn).toHaveBeenCalledOnce();
    expectAdmissionCleaned(f);
    f.finish();
  });
});

type ReceiptFailure = "missing" | "mismatched" | "mismatched-receipt" | "wrong-slice" | "v1-only" | "hybrid" | "unowned" | "open" | "read" | "watcher-arm" | "watcher-arm-async" | "identity-during-arm" | "read-after-arm" | "wrapper" | "gate";
type TreeOptions = {
  unsampled?: boolean;
  externalGroup?: boolean;
  executionPending?: boolean;
  killFailure?: "ENOENT" | "EACCES" | "EIO";
  diagnosticFailure?: boolean;
  logFailure?: boolean;
  nativeCloseDeferred?: boolean;
  comm?: string;
};
// Optional structural seam keeps HEAD RED behavioral, not a missing-method error.
type TerminalScopeHandle = { terminalScopeCleanup?: () => void };
async function fixtureTree(termGraceMs = 7_000, scoped = false, initialPopulated = "1", failure?: ReceiptFailure, viaTransport = false, capped = false, options: TreeOptions = {}) {
  vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout", "setInterval", "clearInterval"] });
  const directory = root();
  const workerPath = path.join(directory, "worker.mjs");
  fs.writeFileSync(workerPath, "");
  const pid = 700_000_001;
  const descendant = pid + 1;
  let primaryAlive = true;
  let descendantAlive = true;
  let scopeEvents: string | undefined;
  let scopeDirectory: string | undefined;
  let liveCgroup: string | undefined;
  let scopeRemoved: string | undefined;
  // All scoped counterprobes hold real test-owned descriptors. No real cgroup
  // mount file is opened or mutated by this recorded fixture.
  const leaf = path.join(directory, "leaf");
  let heldLeaf = leaf;
  const logFile = path.join(directory, "events.jsonl");
  const commFile = path.join(directory, "daemon.comm");
  fs.writeFileSync(commFile, options.comm ?? "fixture-op-daemon\n");
  fs.writeFileSync(logFile, "");
  if (scoped) {
    fs.mkdirSync(leaf);
    fs.writeFileSync(path.join(leaf, "memory.max"), String(1024 ** 3));
    fs.writeFileSync(path.join(leaf, "memory.swap.max"), "0");
    fs.writeFileSync(path.join(leaf, "cgroup.events"), `populated ${initialPopulated}\nfrozen 0\n`);
    fs.writeFileSync(path.join(leaf, "cgroup.procs"), `${descendant}\n`);
    if (options.killFailure !== "ENOENT") fs.writeFileSync(path.join(leaf, "cgroup.kill"), "");
  }
  const nativeMkdir = fs.mkdirSync.bind(fs);
  if (failure === "wrapper") vi.spyOn(fs, "mkdirSync").mockImplementation(((file: fs.PathLike, options?: unknown) => {
    if (path.basename(String(file)) === "bin") throw new Error("wrapper creation failed");
    return nativeMkdir(file, options as never);
  }) as typeof fs.mkdirSync);
  let boundaryFd: number | undefined;
  let eventsFd: number | undefined;
  const controlFds = new Set<number>();
  const diagnosticFds = new Map<number, "comm" | "procs">();
  const diagnosticReads: Array<{ kind: "comm" | "procs"; length: number }> = [];
  const writes: Array<{ fd: number; dev: number; ino: number; log: string }> = [];
  const order: string[] = [];
  const isEvents = (file: unknown): boolean => String(file) === scopeEvents || /\/proc\/self\/fd\/\d+\/cgroup\.events$/.test(String(file));
  const watcher = Object.assign(new EventEmitter(), { close: vi.fn() }) as unknown as fs.FSWatcher;
  const admissionWatcher = Object.assign(new EventEmitter(), { close: vi.fn() }) as unknown as fs.FSWatcher;
  if (scoped) vi.spyOn(fs, "watch").mockImplementation((...args: Parameters<typeof fs.watch>) => {
    if (!isEvents(args[0])) return admissionWatcher;
    order.push("watch");
    if (failure === "watcher-arm") throw new Error("watcher arm failed");
    const listener = args.at(-1);
    if (typeof listener === "function") watcher.on("change", listener);
    if (failure === "watcher-arm-async") setImmediate(() => watcher.emit("error", new Error("queued arm failure")));
    return watcher;
  });
  const stat = (member: number) => {
    const fields = Array<string>(20).fill("0");
    fields[0] = "S";
    // Already reparented, dead foreign group leader, never birth-admitted.
    fields[1] = member === pid || options.unsampled ? "1" : String(pid);
    fields[2] = String(member === descendant && options.unsampled ? pid + 90 : pid);
    fields[19] = String(member);
    return `${member} (worker) ${fields.join(" ")}`;
  };
  const nativeRead = fs.readFileSync.bind(fs);
  vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, readOptions?: unknown) => {
    if (String(file) === `/proc/${pid}/cgroup`) {
      if (failure === "missing") throw Object.assign(new Error("missing cgroup receipt"), { code: "ENOENT" });
      return liveCgroup;
    }
    if (String(file) === `/proc/${descendant}/comm`) {
      if (options.diagnosticFailure) throw Object.assign(new Error("comm denied"), { code: "EACCES" });
      return options.comm ?? "fixture-op-daemon\n";
    }
    if (String(file).endsWith("/cgroup.procs") && options.diagnosticFailure) throw new Error("diagnostic read failed");
    if (String(file) === `/proc/${pid}/stat` && primaryAlive) return stat(pid);
    if (String(file) === `/proc/${descendant}/stat` && descendantAlive) return stat(descendant);
    if ([`/proc/${pid}/stat`, `/proc/${descendant}/stat`].includes(String(file))) {
      throw Object.assign(new Error("gone"), { code: "ENOENT" });
    }
    return nativeRead(file, readOptions as never);
  }) as typeof fs.readFileSync);
  const nativeLstat = fs.lstatSync.bind(fs);
  let directoryChecks = 0;
  vi.spyOn(fs, "lstatSync").mockImplementation(((file: fs.PathLike, options?: unknown) => {
    if (String(file) === scopeDirectory) {
      const unowned = failure === "unowned" || (failure === "identity-during-arm" && ++directoryChecks > 1);
      const stat = nativeLstat(leaf);
      if (unowned) stat.uid = process.getuid!() + 1;
      return stat;
    }
    return nativeLstat(file, options as never);
  }) as typeof fs.lstatSync);
  const nativeStatfs = fs.statfsSync.bind(fs);
  vi.spyOn(fs, "statfsSync").mockImplementation(((file: fs.PathLike, options?: unknown) => {
    if (String(file) === scopeDirectory || /^\/proc\/self\/fd\/\d+$/.test(String(file))) return { type: 0x63677270 };
    return nativeStatfs(file, options as never);
  }) as typeof fs.statfsSync);
  const nativeOpen = fs.openSync.bind(fs);
  const opened = vi.spyOn(fs, "openSync").mockImplementation((file, flags, mode) => {
    if (/^\/proc\/\d+\/(cmdline|environ)$/.test(String(file))) throw new Error("sensitive diagnostic read forbidden");
    const comm = /^\/proc\/\d+\/comm$/.test(String(file));
    const procs = String(file).endsWith("/cgroup.procs");
    if (comm || procs) {
      if (options.diagnosticFailure) throw Object.assign(new Error("diagnostic denied"), { code: "EACCES" });
      const fd = retainFixtureFd(nativeOpen(comm ? commFile : file, flags, mode));
      diagnosticFds.set(fd, comm ? "comm" : "procs");
      return fd;
    }
    if (String(file) === scopeDirectory) {
      boundaryFd = retainFixtureFd(nativeOpen(leaf, flags, mode));
      return boundaryFd;
    }
    if (String(file).endsWith("/cgroup.kill")) {
      if (options.killFailure === "EACCES") throw Object.assign(new Error("kill denied"), { code: "EACCES" });
      expect(String(file)).toMatch(/^\/proc\/self\/fd\/\d+\/cgroup\.kill$/);
      const fd = retainFixtureFd(nativeOpen(file, flags, mode));
      controlFds.add(fd);
      return fd;
    }
    if (!isEvents(file)) return nativeOpen(file, flags, mode);
    order.push("open");
    if (failure === "open") throw new Error("cannot open events");
    eventsFd = retainFixtureFd(nativeOpen(String(file) === scopeEvents ? path.join(leaf, "cgroup.events") : file, flags, mode));
    closeEvents.mockClear(); // marker opens may have used the same numeric fd
    return eventsFd;
  });
  const nativeReadSync = fs.readSync.bind(fs);
  let eventReads = 0;
  const readEvents = vi.spyOn(fs, "readSync").mockImplementation(((...args: unknown[]) => {
    const diagnostic = diagnosticFds.get(Number(args[0]));
    if (diagnostic) {
      const length = typeof args[3] === "number" ? args[3]
        : (args[2] as { length?: number } | undefined)?.length ?? (args[1] as Buffer).byteLength;
      expect(length).toBeLessThanOrEqual(diagnostic === "comm" ? 64 : 16 * 1024);
      diagnosticReads.push({ kind: diagnostic, length });
    }
    if (args[0] !== eventsFd) return (nativeReadSync as (...values: unknown[]) => number)(...args);
    order.push("read");
    if (failure === "read" || (failure === "read-after-arm" && ++eventReads > 1)) throw new Error("events unreadable");
    if (scopeRemoved) throw Object.assign(new Error(`${scopeRemoved}: read failed`), { code: scopeRemoved });
    return (nativeReadSync as (...values: unknown[]) => number)(...args);
  }) as typeof fs.readSync);
  const nativeClose = fs.closeSync.bind(fs);
  const closeEvents = vi.spyOn(fs, "closeSync").mockImplementation(fd => {
    if (fd === eventsFd) order.push("close");
    controlFds.delete(fd);
    diagnosticFds.delete(fd);
    fixtureFds.delete(fd);
    nativeClose(fd);
  });
  const nativeReaddir = fs.readdirSync.bind(fs);
  const census = vi.spyOn(fs, "readdirSync").mockImplementation(((file: fs.PathLike, options?: unknown) => {
    if (String(file) === "/proc") return [...(primaryAlive ? [String(pid)] : []), ...(descendantAlive ? [String(descendant)] : [])];
    return nativeReaddir(file, options as never);
  }) as typeof fs.readdirSync);
  const kill = vi.spyOn(process, "kill").mockImplementation((target, signal) => {
    const hits = (member: number, group: number): boolean => target > 0 ? target === member : target === -group;
    if (signal === "SIGTERM" || signal === "SIGKILL") {
      order.push(`${signal}:${target}`);
      if (hits(descendant, options.unsampled ? pid + 90 : pid)) descendantAlive = false;
      if (primaryAlive && hits(pid, pid)) {
        primaryAlive = false;
        child.emit("exit", 0);
        if (!options.nativeCloseDeferred) { child.emit("close", 0); order.push("native-close"); }
      }
    }
    return true; // zero signal only probes
  });
  const nativeWrite = fs.writeSync.bind(fs);
  vi.spyOn(fs, "writeSync").mockImplementation(((fd: number, ...args: unknown[]) => {
    if (!controlFds.has(fd)) return (nativeWrite as (...values: unknown[]) => number)(fd, ...args);
    if (options.killFailure === "EIO") throw Object.assign(new Error("kill write failed"), { code: "EIO" });
    const result = (nativeWrite as (...values: unknown[]) => number)(fd, ...args);
    expect(String(args[0])).toBe("1");
    const identity = fs.fstatSync(fd);
    writes.push({ fd, dev: identity.dev, ino: identity.ino, log: nativeRead(logFile, "utf8") });
    order.push("scope-write");
    // Successful held-object write emits membership evidence, never tree proof.
    const original = fs.statSync(path.join(heldLeaf, "cgroup.kill"));
    if (identity.dev === original.dev && identity.ino === original.ino) {
      if (!options.externalGroup) descendantAlive = false;
      fs.writeFileSync(path.join(heldLeaf, "cgroup.events"), "populated 0\nfrozen 0\n");
      watcher.emit("change");
    }
    return result;
  }) as typeof fs.writeSync);
  const nativeAppend = fs.appendFileSync.bind(fs);
  vi.spyOn(fs, "appendFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, ...args: unknown[]) => {
    if (String(file) === logFile) {
      order.push("host-log");
      if (options.logFailure) throw new Error("event sink unavailable");
    }
    return (nativeAppend as (...values: unknown[]) => void)(file, ...args);
  }) as typeof fs.appendFileSync);
  const gate = Object.assign(new EventEmitter(), { writable: failure !== "gate", end: vi.fn(), destroy: vi.fn() });
  const child = Object.assign(new EventEmitter(), { pid, unref: vi.fn(), channel: {}, stdio: [null, null, null, null, gate] });
  vi.mocked(spawn).mockClear();
  vi.mocked(spawn).mockImplementationOnce((_command, args) => {
    if (scoped) {
      const marker = args![args!.indexOf("fabric-scope") + 1]!;
      const unit = args!.find(arg => arg.startsWith("--unit="))!.slice(7);
      const cgroup = `/test/${failure === "wrong-slice" ? "foreign.slice" : "fixture.slice"}/${failure === "mismatched" ? "foreign.scope" : unit}`;
      liveCgroup = failure === "v1-only" ? `1:name=systemd:${cgroup}\n`
        : failure === "hybrid" ? `0::${cgroup}\n1:cpu:/legacy\n` : `0::${cgroup}\n`;
      fs.writeFileSync(`${marker}.cgroup`, failure === "mismatched-receipt" ? "0::/foreign/fixture.slice/foreign.scope\n" : liveCgroup);
      fs.writeFileSync(marker, `admitted ${unit}`);
      scopeDirectory = `/sys/fs/cgroup${cgroup}`;
      scopeEvents = `${scopeDirectory}/cgroup.events`;
    }
    return child as unknown as ChildProcess;
  });
  const unconfirmed = vi.fn();
  const scope = scoped ? { executable: "/fixture-systemd-run", slice: "fixture.slice", warn: vi.fn(), ...(capped ? { memoryMaxBytes: 1024 ** 3 } : {}) } : undefined;
  if (viaTransport) {
    // This is an uncapped receipt mechanism, with fake children/files/clock. Do
    // not select the capped admission protocol or depend on a host executable.
    legacyUserEnvironment();
    fs.writeFileSync(path.join(directory, "systemd-run"), "#!/bin/sh\nexit 125\n", { mode: 0o700 });
    vi.stubEnv("PATH", `${directory}${path.delimiter}${process.env.PATH ?? ""}`);
  }
  const transport = viaTransport ? await new ProcessTransport("fixture.slice").launch({
    id: "receipt-fixture", name: "receipt-fixture", cwd: directory, workerPath,
    workerArguments: ["--id", "receipt-fixture", "--actor-id", "receipt-actor", "--log-file", logFile], onUnconfirmedExit: unconfirmed,
  }) : undefined;
  const handle = transport ? { ...transport, pid: Number(transport.sessionId), closed: transport.closed!, waitForClose: transport.waitForClose!, lostContact: transport.lostContact! }
    : await spawnDetached(workerPath, [], directory, { onUnconfirmedExit: unconfirmed }, undefined, scope, termGraceMs, true);
  if (options.executionPending) child.emit("message", { type: "fabric-execution-custody" });
  if (!options.unsampled) expect(await handle.isAlive()).toBe(true); // sampled default regression mode
  census.mockClear(); kill.mockClear();
  const timeout = vi.spyOn(globalThis, "setTimeout");
  return {
    handle, transport, census, kill, timeout, unconfirmed, watcher, admissionWatcher, opened, readEvents, closeEvents, order,
    pid, descendant, directory, leaf, logFile, writes, diagnosticReads,
    alive: () => descendantAlive,
    cleanup: () => (handle as typeof handle & TerminalScopeHandle).terminalScopeCleanup?.(),
    boundaryFd: () => boundaryFd,
    eventsFd: () => eventsFd,
    replacePath: () => {
      heldLeaf = path.join(directory, "retired-leaf");
      fs.renameSync(leaf, heldLeaf);
      fs.mkdirSync(leaf);
      fs.writeFileSync(path.join(leaf, "cgroup.kill"), "sibling-sentinel");
    },
    emptyScope: () => {
      if (scoped) fs.writeFileSync(path.join(heldLeaf, "cgroup.events"), "populated 0\nfrozen 0\n");
      watcher.emit("change");
    },
    // systemd collects an empty scope (processSlice): the retained events fd then reads ENODEV.
    removeScope: (code = "ENODEV") => { scopeRemoved = code; watcher.emit("change"); },
    depart: () => { descendantAlive = false; },
    close: async () => {
      primaryAlive = false;
      child.emit("exit", 0); child.emit("close", 0);
      order.push("native-close");
      await tick();
    },
    scans: () => census.mock.calls.filter(([file]) => String(file) === "/proc").length,
  };
}

// Keep: native identity trust boundary and the recorded #16970 unsampled-daemon
// failure. All process controls are recorded; only test-temp files have real FDs.
describe.skipIf(process.platform !== "linux")("P2 terminal scope", () => {
  it("T2 target-aware signals never adopt or clear an already-reparented dead-leader daemon", async () => {
    const f = await fixtureTree(7_000, true, "1", undefined, true, false, { unsampled: true });
    expect(f.census).not.toHaveBeenCalled();
    f.kill(f.pid + 50, "SIGTERM");
    f.kill(-f.pid, "SIGKILL");
    f.kill(f.descendant, 0);
    expect(f.alive()).toBe(true);
    await f.close();
    await f.handle.stop();
    expect(f.alive()).toBe(true); // RED precondition: ordinary stop cannot reach it
    expect(f.kill.mock.calls.some(([target, signal]) => signal !== 0 && (target === f.descendant || target === -(f.pid + 90)))).toBe(false);
    let closed = false;
    void f.handle.treeClosed!.then(() => { closed = true; });
    await tick();
    expect(closed).toBe(false);
    f.cleanup();
    expect(f.writes, "T2 missing held-object cleanup: unsampled survivor remains after ordinary stop").toHaveLength(1);
    expect(f.alive()).toBe(false);
    await expect(f.handle.treeClosed).resolves.toBeUndefined();
    expect(f.transport).not.toHaveProperty("cgroup"); // never the capped shortcut
    f.cleanup();
    expect(f.writes).toHaveLength(1);
  });

  it("T10 records fixture daemon pid and comm on the actual host --log-file sink BEFORE write", async () => {
    const f = await fixtureTree(7_000, true, "1", undefined, true, false, { unsampled: true, comm: "fixture-op-daemon\n\u001b[31m" });
    await f.handle.stop(); // cooperative TERM closes primary; scope stays populated
    await expect(f.handle.closed).resolves.toBeUndefined();
    expect(f.alive()).toBe(true);
    f.cleanup();
    const records = fs.readFileSync(f.logFile, "utf8").trim().split("\n").filter(Boolean).map(line => JSON.parse(line));
    const cleanup = records.filter(record => record.type === "scope.cleanup");
    expect(cleanup, "T10 missing typed scope.cleanup record in host-selected worker log").toHaveLength(1);
    expect(cleanup[0]).toMatchObject({ id: "receipt-fixture", ts: expect.any(Number), note: "shared per-user services may be interrupted", members: expect.arrayContaining([expect.objectContaining({ pid: f.descendant, comm: expect.stringContaining("fixture-op-daemon") })]) });
    expect(f.writes).toHaveLength(1);
    expect(f.writes[0]!.log).toContain('"type":"scope.cleanup"');
    expect(f.writes[0]!.log).not.toContain("\u001b");
    expect(f.order.indexOf("native-close")).toBeLessThan(f.order.indexOf("scope-write"));
    expect(f.order.indexOf("host-log")).toBeLessThan(f.order.indexOf("scope-write"));
    await expect(f.handle.treeClosed).resolves.toBeUndefined();
  });

  it("T7 same-path replacement cleans only the retained object and leaves sibling sentinel intact", async () => {
    const f = await fixtureTree(7_000, true, "1", undefined, true, false, { unsampled: true });
    await f.close(); await f.handle.stop();
    const original = fs.statSync(path.join(f.leaf, "cgroup.kill"));
    f.replacePath();
    f.cleanup();
    expect(f.writes, "T7 missing cleanup of original held scope object").toHaveLength(1);
    expect(f.writes[0]).toMatchObject({ dev: original.dev, ino: original.ino });
    expect(fs.readFileSync(path.join(f.leaf, "cgroup.kill"), "utf8")).toBe("sibling-sentinel");
    await expect(f.handle.treeClosed).resolves.toBeUndefined();
  });

  it.each(["dev", "ino"] as const)("T7 refuses retained-directory %s mismatch immediately before write", async field => {
    const f = await fixtureTree(7_000, true, "1", undefined, true, false, { unsampled: true });
    await f.close(); await f.handle.stop();
    const originalFstat = fs.fstatSync.bind(fs);
    const mismatch = vi.spyOn(fs, "fstatSync").mockImplementation(((fd: number, ...args: unknown[]) => {
      const stat = (originalFstat as (...values: unknown[]) => fs.Stats)(fd, ...args);
      if (fd === f.boundaryFd() && f.order.includes("host-log")) stat[field] += 1;
      return stat;
    }) as typeof fs.fstatSync);
    try { f.cleanup(); } catch { /* refusal may throw or retire */ }
    expect(f.writes).toHaveLength(0);
    expect(f.alive()).toBe(true);
    mismatch.mockRestore();
    f.depart(); f.emptyScope();
    await expect(f.handle.treeClosed).resolves.toBeUndefined();
  });

  it("T7 stale callback cannot write or close a reused directory FD", async () => {
    const f = await fixtureTree(7_000, true, "1", undefined, true, false, { unsampled: true });
    await f.close(); await f.handle.stop();
    const fd = f.boundaryFd();
    const foreign = path.join(f.directory, "foreign");
    fs.mkdirSync(foreign); fs.writeFileSync(path.join(foreign, "cgroup.kill"), "foreign-sentinel");
    if (fd !== undefined) fs.closeSync(fd);
    const replacement = fs.openSync(foreign, fs.constants.O_RDONLY | fs.constants.O_DIRECTORY);
    try {
      if (fd !== undefined) expect(replacement).toBe(fd); // real allocator reuse
      try { f.cleanup(); } catch { /* refused identity */ }
      expect(f.writes).toHaveLength(0);
      expect(fs.fstatSync(replacement).ino).toBe(fs.statSync(foreign).ino);
      expect(fs.readFileSync(path.join(foreign, "cgroup.kill"), "utf8")).toBe("foreign-sentinel");
      f.depart(); f.emptyScope(); await tick();
      expect(fs.fstatSync(replacement).isDirectory()).toBe(true);
    } finally { fs.closeSync(replacement); }
  });

  it("T7 removed scope retires capability and closes each owned descriptor once", async () => {
    const baseline = fs.readdirSync("/proc/self/fd").length;
    const f = await fixtureTree(7_000, true, "1", undefined, true, false, { unsampled: true });
    await f.close(); f.depart();
    f.closeEvents.mockClear();
    const directoryFd = f.boundaryFd();
    const eventsFd = f.eventsFd();
    f.removeScope();
    await expect(f.handle.treeClosed).resolves.toBeUndefined();
    f.cleanup(); f.watcher.emit("change"); f.cleanup();
    expect(f.writes).toHaveLength(0);
    expect(f.watcher.close).toHaveBeenCalledOnce();
    expect(f.closeEvents.mock.calls.filter(([fd]) => fd === eventsFd)).toHaveLength(1);
    if (directoryFd !== undefined) expect(f.closeEvents.mock.calls.filter(([fd]) => fd === directoryFd)).toHaveLength(1);
    expect(fs.readdirSync("/proc/self/fd").length).toBe(baseline);
    expect(vi.getTimerCount()).toBe(0);
  });

  it.each(["ENOENT", "EACCES", "EIO"] as const)("T9 %s cgroup.kill failure has no PID/name fallback or fabricated proof", async killFailure => {
    const f = await fixtureTree(7_000, true, "1", undefined, true, false, { unsampled: true, killFailure });
    await f.close(); await f.handle.stop(); f.kill.mockClear();
    for (let i = 0; i < 2; i++) { try { f.cleanup(); } catch { /* expected failed one-shot */ } }
    expect(f.writes).toHaveLength(0);
    expect(f.kill).not.toHaveBeenCalled();
    expect(spawn).toHaveBeenCalledOnce(); // no systemctl or launch replay
    expect(f.alive()).toBe(true);
    let settled = false;
    void f.handle.treeClosed!.then(() => { settled = true; });
    await tick(); expect(settled).toBe(false);
    f.depart(); f.emptyScope(); await expect(f.handle.treeClosed).resolves.toBeUndefined();
  });

  it("T8 watcher debt revokes authority even if the physical scope later empties", async () => {
    const f = await fixtureTree(7_000, true, "1", undefined, true, false, { unsampled: true });
    const error = new Error("owned watcher debt");
    f.watcher.emit("error", error);
    await expect(f.handle.treeClosed).rejects.toBe(error);
    await f.close(); await f.handle.stop();
    try { f.cleanup(); } catch { /* revoked */ }
    expect(f.writes).toHaveLength(0);
    expect(f.alive()).toBe(true);
    f.depart(); f.emptyScope();
    await expect(f.handle.treeClosed).rejects.toBe(error);
    expect(f.handle.lostContact()).toContain("Owned scope watcher failed");
  });

  it("T8 native-close timeout debt cannot authorize cleanup after a delayed close", async () => {
    const f = await fixtureTree(7_000, true, "1", undefined, true, false, { unsampled: true, nativeCloseDeferred: true });
    const stopping = f.handle.stop();
    await vi.advanceTimersByTimeAsync(20_000);
    await stopping;
    expect(f.handle.stopDebt?.()).toContain("did not confirm");
    try { f.cleanup(); } catch { /* positive native close is absent */ }
    expect(f.writes).toHaveLength(0);
    await f.close();
    try { f.cleanup(); } catch { /* immutable native debt remains */ }
    expect(f.writes).toHaveLength(0);
    expect(f.alive()).toBe(true);
    const debt = f.handle.stopDebt?.();
    f.depart(); f.emptyScope(); await tick();
    expect(f.handle.stopDebt?.()).toBe(debt);
  });

  it("T10 bounds observed root inventory, names and serialized host record without reading argv/env", async () => {
    const f = await fixtureTree(7_000, true, "1", undefined, true, false, { unsampled: true, comm: `fixture-op-daemon-${"x".repeat(200)}` });
    fs.writeFileSync(path.join(f.leaf, "cgroup.procs"), Array.from({ length: 90 }, (_, i) => String(f.descendant + i)).join("\n"));
    await f.close(); await f.handle.stop(); f.cleanup();
    const lines = fs.readFileSync(f.logFile, "utf8").trim().split("\n").filter(Boolean);
    const line = lines.find(line => JSON.parse(line).type === "scope.cleanup");
    expect(line, "bounded inventory still requires the real host log record").toBeDefined();
    expect(Buffer.byteLength(line!)).toBeLessThanOrEqual(8 * 1024);
    const entry = JSON.parse(line!);
    expect(entry.members.length).toBeGreaterThan(0);
    expect(entry.members.length).toBeLessThanOrEqual(64);
    expect(f.diagnosticReads.filter(read => read.kind === "procs")).toHaveLength(1);
    expect(f.diagnosticReads.filter(read => read.kind === "comm").length).toBeLessThanOrEqual(64);
    expect(entry.members.every((member: { comm: string }) => Buffer.byteLength(member.comm) <= 64)).toBe(true);
    expect(f.opened.mock.calls.some(([file]) => /\/(cmdline|environ)$/.test(String(file)))).toBe(false);
    expect(f.writes).toHaveLength(1);
    await expect(f.handle.treeClosed).resolves.toBeUndefined();
  });

  it.each(["outside group", "uncertain handoff"] as const)("T10 scope-empty cannot erase %s custody", async mode => {
    const f = await fixtureTree(7_000, true, "1", undefined, true, false, mode === "outside group" ? { externalGroup: true } : { unsampled: true, executionPending: true });
    await f.close();
    f.cleanup();
    f.emptyScope(); // even positive scope emptiness is NOT the uncapped tree proof
    let settled = false;
    void f.handle.treeClosed!.then(() => { settled = true; }, () => { settled = true; });
    await tick(); expect(settled).toBe(false);
    await vi.advanceTimersByTimeAsync(60_000);
    await expect(f.handle.treeClosed).rejects.toBeInstanceOf(ProcessTreeCustodyUnconfirmedError);
    expect(f.handle.stopDebt?.()).toContain("custody unconfirmed");
    const writes = f.writes.length;
    try { f.cleanup(); } catch { /* retired receipt */ }
    expect(f.writes).toHaveLength(writes);
  });

  it.each(["diagnostic", "log"] as const)("T10 %s failure never vetoes held-object cleanup", async failed => {
    const f = await fixtureTree(7_000, true, "1", undefined, true, false, { unsampled: true, diagnosticFailure: failed === "diagnostic", logFailure: failed === "log" });
    await f.close(); await f.handle.stop();
    f.cleanup();
    expect(f.writes, "diagnostic/log errors must not veto cleanup").toHaveLength(1);
    expect(f.alive()).toBe(false);
    await expect(f.handle.treeClosed).resolves.toBeUndefined();
  });
});

describe.skipIf(process.platform !== "linux")("verified cgroup-v2 receipt admission", () => {
  it.each(["missing", "mismatched", "mismatched-receipt", "wrong-slice", "v1-only", "hybrid", "unowned", "open", "read", "watcher-arm", "watcher-arm-async", "identity-during-arm", "read-after-arm"] as const)("falls back to legacy monitoring for %s", async failure => {
    const f = await fixtureTree(7_000, true, "1", failure, true);
    expect(f.transport?.liveness).toBeUndefined();
    expect(f.handle.treeClosed).toBeUndefined();
    expect(f.transport).not.toHaveProperty("liveness");
    expect(f.handle).not.toHaveProperty("treeClosed");
    expect(spawn).toHaveBeenCalledOnce(); // receipt failure must not replay the admitted worker
    expect(vi.mocked(spawn).mock.calls.filter(([command]) => String(command).endsWith("systemd-run"))).toHaveLength(1);
    await f.close();
    expect(f.scans()).toBe(0); // no automatic event census on native close
    expect(f.timeout).not.toHaveBeenCalled();
    expect(await f.handle.isAlive()).toBe(true);
    expect(f.scans()).toBe(1); // main's checked-query census, not an event result
    await vi.advanceTimersByTimeAsync(120_000);
    expect(f.scans()).toBe(1);
    expect(f.timeout).not.toHaveBeenCalled();
    f.depart();
    expect(await f.handle.isAlive()).toBe(false);
    expect(f.handle.lostContact()).toBeUndefined();
    expect(vi.getTimerCount()).toBe(0);
    if (["read", "watcher-arm", "watcher-arm-async", "identity-during-arm", "read-after-arm"].includes(failure)) expect(f.closeEvents.mock.calls.filter(([fd]) => fd === f.eventsFd())).toHaveLength(1);
  });

  it("publishes event custody only after opening, arming and validating our owned v2 scope", async () => {
    const f = await fixtureTree(7_000, true, "1", undefined, true);
    expect(f.transport?.liveness).toBe("events");
    expect(f.handle.treeClosed).toBeInstanceOf(Promise);
    expect(f.order.slice(0, 3)).toEqual(["open", "watch", "read"]);
    expect(f.admissionWatcher.close).toHaveBeenCalledOnce();
    expect(f.readEvents.mock.calls.filter(([fd]) => fd === f.eventsFd()).every(call => (call as unknown[])[4] === 0)).toBe(true);
    await f.close();
    expect(f.scans()).toBe(0);
    f.depart(); f.emptyScope();
    await expect(f.handle.treeClosed).resolves.toBeUndefined();
    expect(f.watcher.close).toHaveBeenCalledOnce();
    expect(f.closeEvents.mock.calls.filter(([fd]) => fd === f.eventsFd())).toHaveLength(1);
    expect(await f.handle.isAlive()).toBe(false);
  });

  // smarty-dev#6829: systemd removed the empty scope's cgroup before Node reported the worker closed.
  it.each(["before", "after"] as const)("confirms exit when the empty scope is removed %s native close", async order => {
    const f = await fixtureTree(7_000, true, "1", undefined, true);
    expect(f.transport?.liveness).toBe("events");
    f.depart();
    if (order === "before") { f.removeScope(); await f.close(); } else { await f.close(); f.removeScope(); }
    await expect(f.handle.treeClosed).resolves.toBeUndefined();
    expect(f.unconfirmed).not.toHaveBeenCalled();
    expect(f.handle.lostContact()).toBeUndefined();
    expect(await f.handle.isAlive()).toBe(false);
  });

  // Only removal (ENODEV) proves an empty scope; any other read error keeps custody.
  it.each(["ENOENT", "EACCES", "EIO"])("retains custody when the events read fails with %s after exit", async code => {
    const f = await fixtureTree(7_000, true, "1", undefined, true);
    f.depart();
    await f.close();
    f.removeScope(code);
    await expect(f.handle.treeClosed).rejects.toMatchObject({ code });
    expect(f.unconfirmed).toHaveBeenCalledOnce();
    expect(f.handle.lostContact()).toContain("Owned scope observation failed");
  });

  it.each(["open", "read", "watcher-arm", "watcher-arm-async", "identity-during-arm", "read-after-arm"] as const)("releases real capped admission descriptors on %s refusal", async failure => {
    const baseline = fs.readdirSync("/proc/self/fd").length;
    await expect(fixtureTree(7_000, true, "1", failure, false, true)).rejects.toBeInstanceOf(taskCgroup.TaskScopeAdmissionError);
    expect(fs.readdirSync("/proc/self/fd").length).toBe(baseline);
    expect(spawn).toHaveBeenCalledOnce(); // refusal must not replay the gated worker
  });

  it.each(["wrapper", "gate"] as const)("tears down and releases an admitted leaf when %s prevents gate release", async failure => {
    const baseline = fs.readdirSync("/proc/self/fd").length;
    const stop = vi.spyOn(taskCgroup, "stopTaskCgroup").mockResolvedValue("cgroup.kill");
    await expect(fixtureTree(7_000, true, "1", failure, false, true)).rejects.toBeInstanceOf(taskCgroup.TaskScopeAdmissionError);
    expect(stop).toHaveBeenCalledOnce();
    expect(fs.readdirSync("/proc/self/fd").length).toBe(baseline);
    expect(spawn).toHaveBeenCalledOnce();
  });

  it("releases real capped descriptors on a populated-zero receipt", async () => {
    const baseline = fs.readdirSync("/proc/self/fd").length;
    const f = await fixtureTree(7_000, true, "1", undefined, false, true);
    const fd = f.boundaryFd()!;
    f.closeEvents.mockClear();
    expect(fs.readdirSync("/proc/self/fd").length).toBe(baseline + 2);
    await f.close(); f.depart(); f.emptyScope();
    await expect(f.handle.treeClosed).resolves.toBeUndefined();
    expect(fs.readdirSync("/proc/self/fd").length).toBe(baseline);
    const stop = vi.spyOn(taskCgroup, "stopTaskCgroup");
    await f.handle.stop();
    expect(stop).not.toHaveBeenCalled();
    expect(f.closeEvents.mock.calls.filter(([closedFd]) => closedFd === fd)).toHaveLength(1);
    expect(f.handle.stopDebt?.()).toBeUndefined();
  });

  // smarty-dev#11920: watcher rejection must not strand the admitted directory fd.
  // Real /proc/self/fd counterprobe: only child/cgroup operations are mocked.
  it("releases the real admitted directory fd after watcher failure then successful stop", async () => {
    const baseline = fs.readdirSync("/proc/self/fd").length;
    const deletes = vi.spyOn(fs, "rmSync");
    const stop = vi.spyOn(taskCgroup, "stopTaskCgroup").mockRejectedValueOnce(new Error("teardown still unconfirmed"));
    const f = await fixtureTree(7_000, true, "1", undefined, false, true);
    const fd = f.boundaryFd()!;
    f.closeEvents.mockClear(); // admission marker may have reused this fd number
    expect(fs.readdirSync("/proc/self/fd").length).toBe(baseline + 2);
    const error = new Error("post-admission capped watcher failure");
    f.watcher.emit("error", error);
    await expect(f.handle.treeClosed).rejects.toBe(error);
    const debt = f.handle.lostContact();
    expect(debt).toContain("Owned scope watcher failed");
    expect(f.handle.stopDebt?.()).toBe(debt);
    expect(f.unconfirmed).toHaveBeenCalledOnce();
    expect(fs.readdirSync("/proc/self/fd").length).toBe(baseline + 1);
    expect(fs.fstatSync(fd).isDirectory()).toBe(true); // still needed for teardown
    await f.close(); f.depart();
    await expect(f.handle.stop()).rejects.toThrow("teardown still unconfirmed");
    expect(fs.readdirSync("/proc/self/fd").length).toBe(baseline + 1);
    expect(fs.fstatSync(fd).isDirectory()).toBe(true); // failed stop cannot surrender it
    stop.mockResolvedValue("cgroup.kill");
    await f.handle.stop();
    expect(stop).toHaveBeenCalledTimes(2);
    expect(stop).toHaveBeenLastCalledWith(expect.objectContaining({ fd }), { termMs: 2_000, killMs: 2_000 });
    expect(fs.readdirSync("/proc/self/fd").length).toBe(baseline);
    expect(f.closeEvents.mock.calls.filter(([closedFd]) => closedFd === fd)).toHaveLength(1);
    expect(() => fs.fstatSync(fd)).toThrow();
    await f.handle.stop(); // descriptor release and successful stop are idempotent
    expect(stop).toHaveBeenCalledTimes(2);
    expect(fs.readdirSync("/proc/self/fd").length).toBe(baseline);
    await expect(f.handle.treeClosed).rejects.toBe(error); // no fabricated clean receipt
    expect(f.handle.lostContact()).toBe(debt);
    expect(f.handle.stopDebt?.()).toBe(debt);
    expect(f.unconfirmed).toHaveBeenCalledOnce();
    expect(deletes).toHaveBeenCalledWith(expect.stringContaining("fabric-scope-"), { recursive: true, force: true });
  });

  it("rejects and retains custody if an admitted watcher later fails", async () => {
    const f = await fixtureTree(7_000, true);
    const error = new Error("post-admission watcher failure");
    f.watcher.emit("error", error);
    await expect(f.handle.treeClosed).rejects.toBe(error);
    expect(f.handle.lostContact()).toContain("Owned scope watcher failed");
    expect(f.unconfirmed).toHaveBeenCalledOnce();
    await f.close(); f.depart();
    await expect(f.handle.treeClosed).rejects.toBe(error); // native exit cannot create a clean event receipt
  });
});

describe.skipIf(process.platform !== "linux")("scoped bounded execution custody", () => {
  it.each(["clean", "unconfirmed"] as const)("takes only close and deadline censuses (%s)", async outcome => {
    const f = await fixtureTree(7_000, true, "0");
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

  it("leaves unscoped close on main's census path with no new receipt or deadline", async () => {
    const f = await fixtureTree();
    expect(f.handle.treeClosed).toBeUndefined();
    await f.close();
    expect(f.scans()).toBe(0); // main performs no census just for native close
    expect(f.timeout).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
    expect(await f.handle.isAlive()).toBe(true);
    expect(f.scans()).toBe(1); // main's explicit checked liveness query
    await vi.advanceTimersByTimeAsync(120_000);
    expect(f.scans()).toBe(1);
    expect(f.timeout).not.toHaveBeenCalled();
    f.depart();
    expect(await f.handle.isAlive()).toBe(false);
    expect(f.handle.lostContact()).toBeUndefined();
  });

  it("preserves a larger existing TERM/KILL grace budget without repeating", async () => {
    const f = await fixtureTree(90_000, true, "0");
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
    const f = await fixtureTree(7_000, true);
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

// A socket on disk does not prove a working user manager or delegated memory.
// Only this real integration skips for a failed real, owned capped-scope probe.
const realScopeUnavailable = taskCgroupUnavailableReason();
describe("real owned systemd user scope", () => {
  it.skipIf(realScopeUnavailable !== undefined)("waits for a real cgroup-v2 descendant after primary native close", async () => {
    const directory = root();
    const workerPath = path.join(directory, "scoped-worker.mjs");
    const descendant = `const fs = require("node:fs");
      const timer = setInterval(() => { if (fs.existsSync("exit-descendant")) { clearInterval(timer); process.exit(0); } }, 10);
      setTimeout(() => process.exit(2), 8000);`;
    fs.writeFileSync(workerPath, `import fs from "node:fs";
      import { spawn } from "node:child_process";
      const child = spawn(process.execPath, ["-e", ${JSON.stringify(descendant)}], { detached: true, stdio: "ignore" });
      child.unref(); fs.writeFileSync("worker-ready", String(child.pid));
      setInterval(() => { if (fs.existsSync("exit-parent")) process.exit(0); }, 10);
      setTimeout(() => process.exit(2), 8000);`);
    const actual = await vi.importActual<typeof import("node:child_process")>("node:child_process");
    vi.mocked(spawn).mockImplementation(actual.spawn);
    const handle = await new ProcessTransport("app.slice").launch({ id: "real-v2", name: "real-v2", cwd: directory, workerPath, workerArguments: [] });
    try {
      expect(handle.liveness).toBe("events");
      expect(handle.treeClosed).toBeInstanceOf(Promise);
      const pid = Number(handle.sessionId);
      const cgroup = fs.readFileSync(`/proc/${pid}/cgroup`, "utf8").trim();
      const { delegatedUserCgroupAvailable } = await import("../src/agents/transports/process-transport.js");
      expect(cgroup).toMatch(delegatedUserCgroupAvailable()
        ? /^0::\/.*\/app\.slice\/fabric-task-.+\.scope$/ : /^0::\/.*\/app\.slice\/fabric-scope-.+\.scope$/);
      expect(fs.statSync(`/sys/fs/cgroup${cgroup.slice(3)}`).uid).toBe(process.getuid!());
      await vi.waitFor(() => expect(fs.existsSync(path.join(directory, "worker-ready"))).toBe(true), { timeout: 3_000 });
      expect(await handle.isAlive()).toBe(true); // retain the separate descendant birth before reparenting
      let treeEmpty = false;
      void handle.treeClosed!.then(() => { treeEmpty = true; }, () => undefined);
      fs.writeFileSync(path.join(directory, "exit-parent"), "");
      await handle.closed;
      expect(await handle.isAlive()).toBe(true);
      expect(treeEmpty).toBe(false);
      fs.writeFileSync(path.join(directory, "exit-descendant"), "");
      await vi.waitFor(() => expect(treeEmpty).toBe(true), { timeout: 3_000 });
      await expect(handle.treeClosed).resolves.toBeUndefined();
      expect(await handle.isAlive()).toBe(false);
      expect(handle.lostContact?.()).toBeUndefined();
    } finally {
      fs.writeFileSync(path.join(directory, "exit-parent"), "");
      fs.writeFileSync(path.join(directory, "exit-descendant"), "");
      await handle.stop(); await handle.waitForClose?.();
    }
  });
});

describe.skipIf(process.platform !== "linux")("unscoped native descendant legacy delivery", () => {
  it("keeps the legacy cadence and settles a real unscoped descendant without a PR deadline", async () => {
    legacyUserEnvironment();
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
      expect(transport.liveness).toBeUndefined();
      expect(transport.treeClosed).toBeUndefined();
      await vi.advanceTimersByTimeAsync(1_000);
      expect(manager.status(handle.id).status).toBe("running");
      expect(stop).not.toHaveBeenCalled();
      fs.writeFileSync(path.join(directory, "exit-descendant"), "");
      await vi.waitFor(() => expect(fs.existsSync(path.join(directory, "descendant-exited"))).toBe(true), { timeout: 500 });
      await vi.advanceTimersByTimeAsync(3_000);
      const result = await manager.wait(handle.id);
      expect(result.status).toBe("failed");
      expect(result.error).toContain("exited without a result");
      expect(Date.now() - closedAt).toBeLessThan(60_000);
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
