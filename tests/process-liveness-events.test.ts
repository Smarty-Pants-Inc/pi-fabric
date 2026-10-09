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

vi.mock("node:child_process", async importOriginal => {
  const actual = await importOriginal<typeof import("node:child_process")>();
  return { ...actual, spawn: vi.fn(actual.spawn) };
});

const roots: string[] = [];
const managers: AgentManager[] = [];
afterEach(async () => {
  // Restore spies while their fake-clock originals are still installed; doing
  // this after useRealTimers would reinstall a detached fake setTimeout.
  vi.restoreAllMocks();
  vi.useRealTimers();
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

type AdmissionOptions = { present?: boolean; race?: boolean; armFailure?: boolean };
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
    if (signal === "SIGTERM" || signal === "SIGKILL") { order.push("stop"); close(child); }
    return true;
  });
  vi.mocked(spawn).mockClear();
  vi.mocked(spawn).mockImplementationOnce((_command, args) => {
    marker = args![args!.indexOf("fabric-scope") + 1]!;
    if (options.present) fs.writeFileSync(marker, "admitted");
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
    if (options.race) fs.writeFileSync(marker, "admitted");
    return watcher;
  });
  const nativeExists = fs.existsSync.bind(fs);
  const exists = vi.spyOn(fs, "existsSync").mockImplementation(file => {
    if (String(file) === marker) order.push("exists");
    return nativeExists(file);
  });
  const stat = vi.spyOn(fs, "statSync");
  const lstat = vi.spyOn(fs, "lstatSync");
  const interval = vi.spyOn(globalThis, "setInterval");
  const timeout = vi.spyOn(globalThis, "setTimeout");
  const warn = vi.fn();
  const launch = spawnDetached(workerPath, [], directory, { signal: controller.signal }, undefined,
    { executable: "/fixture-systemd-run", slice: "fixture.slice", warn });
  void launch.catch(() => undefined);
  await arming;
  return {
    launch, watcher, watch, controller, removeAbort, order, interval, timeout, warn, children,
    markerChecks: () => exists.mock.calls.filter(([file]) => String(file) === marker).length,
    markerStats: () => [...stat.mock.calls, ...lstat.mock.calls].filter(([file]) => String(file) === marker).length,
    admit: (filename: string | Buffer = "admitted") => { fs.writeFileSync(marker, "admitted"); watcher.emit("change", "rename", filename); },
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
  expect(f.markerStats()).toBe(0);
}

describe.skipIf(process.platform !== "linux")("event-driven scope admission marker", () => {
  it.each(["admitted", Buffer.from("admitted")])("resolves on a matching directory event without polling (%s)", async filename => {
    const f = await fixtureAdmission();
    expect(f.order).toEqual(["watch", "exists"]);
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
    expect(f.markerChecks()).toBe(1);
    expect(f.warn).not.toHaveBeenCalled();
    expectAdmissionCleaned(f);
    f.finish();
  });

  it.each([{ present: true }, { race: true }])("checks once after arming and immediately accepts a present/racing marker: %j", async options => {
    const f = await fixtureAdmission(options);
    const handle = await f.launch;
    expect(f.order).toEqual(["watch", "exists", "watch-close"]);
    expect(handle.pid).toBe(f.children[0]!.pid);
    expect(f.markerChecks()).toBe(1);
    expect(f.timeout).not.toHaveBeenCalled();
    expect(spawn).toHaveBeenCalledOnce();
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
    expect(f.markerChecks()).toBe(2); // initial check plus the existing no-replay check AFTER teardown
    expect(f.order.indexOf("watch-close")).toBeLessThan(f.order.indexOf("stop"));
    expect(f.warn).toHaveBeenCalledOnce();
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

type ReceiptFailure = "missing" | "mismatched" | "mismatched-receipt" | "wrong-slice" | "v1-only" | "hybrid" | "unowned" | "open" | "read" | "watcher-arm" | "watcher-arm-async";
async function fixtureTree(termGraceMs = 7_000, scoped = false, initialPopulated = "1", failure?: ReceiptFailure, viaTransport = false) {
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
  let populated = initialPopulated;
  const eventsFd = 700_000_010;
  const order: string[] = [];
  const watcher = Object.assign(new EventEmitter(), { close: vi.fn() }) as unknown as fs.FSWatcher;
  const admissionWatcher = Object.assign(new EventEmitter(), { close: vi.fn() }) as unknown as fs.FSWatcher;
  if (scoped) vi.spyOn(fs, "watch").mockImplementation((...args: Parameters<typeof fs.watch>) => {
    if (String(args[0]) !== scopeEvents) return admissionWatcher;
    order.push("watch");
    if (failure === "watcher-arm") throw new Error("watcher arm failed");
    const listener = args.at(-1);
    if (typeof listener === "function") watcher.on("change", listener);
    if (failure === "watcher-arm-async") setImmediate(() => watcher.emit("error", new Error("queued arm failure")));
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
    if (String(file) === `/proc/${pid}/cgroup`) {
      if (failure === "missing") throw Object.assign(new Error("missing cgroup receipt"), { code: "ENOENT" });
      return liveCgroup;
    }
    if (String(file) === `/proc/${pid}/stat` && primaryAlive) return stat(pid);
    if (String(file) === `/proc/${descendant}/stat` && descendantAlive) return stat(descendant);
    if ([`/proc/${pid}/stat`, `/proc/${descendant}/stat`].includes(String(file))) {
      throw Object.assign(new Error("gone"), { code: "ENOENT" });
    }
    return nativeRead(file, options as never);
  }) as typeof fs.readFileSync);
  const nativeLstat = fs.lstatSync.bind(fs);
  vi.spyOn(fs, "lstatSync").mockImplementation(((file: fs.PathLike, options?: unknown) => {
    if (String(file) === scopeDirectory) return { uid: process.getuid!() + (failure === "unowned" ? 1 : 0), isDirectory: () => true };
    return nativeLstat(file, options as never);
  }) as typeof fs.lstatSync);
  const nativeStatfs = fs.statfsSync.bind(fs);
  vi.spyOn(fs, "statfsSync").mockImplementation(((file: fs.PathLike, options?: unknown) => {
    if (String(file) === scopeDirectory) return { type: 0x63677270 };
    return nativeStatfs(file, options as never);
  }) as typeof fs.statfsSync);
  const nativeOpen = fs.openSync.bind(fs);
  const opened = vi.spyOn(fs, "openSync").mockImplementation((file, flags, mode) => {
    if (String(file) !== scopeEvents) return nativeOpen(file, flags, mode);
    order.push("open");
    if (failure === "open") throw new Error("cannot open events");
    return eventsFd;
  });
  const nativeReadSync = fs.readSync.bind(fs);
  const readEvents = vi.spyOn(fs, "readSync").mockImplementation(((...args: unknown[]) => {
    if (args[0] !== eventsFd) return (nativeReadSync as (...values: unknown[]) => number)(...args);
    order.push("read");
    if (failure === "read") throw new Error("events unreadable");
    const text = `populated ${populated}\nfrozen 0\n`;
    (args[1] as Buffer).write(text);
    return Buffer.byteLength(text);
  }) as typeof fs.readSync);
  const nativeClose = fs.closeSync.bind(fs);
  const closeEvents = vi.spyOn(fs, "closeSync").mockImplementation(fd => {
    if (fd === eventsFd) { order.push("close"); return; }
    nativeClose(fd);
  });
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
  vi.mocked(spawn).mockClear();
  vi.mocked(spawn).mockImplementationOnce((_command, args) => {
    if (scoped) {
      const marker = args![args!.indexOf("fabric-scope") + 1]!;
      const unit = args!.find(arg => arg.startsWith("--unit="))!.slice(7);
      const cgroup = `/test/${failure === "wrong-slice" ? "foreign.slice" : "fixture.slice"}/${failure === "mismatched" ? "foreign.scope" : unit}`;
      liveCgroup = failure === "v1-only" ? `1:name=systemd:${cgroup}\n`
        : failure === "hybrid" ? `0::${cgroup}\n1:cpu:/legacy\n` : `0::${cgroup}\n`;
      fs.writeFileSync(`${marker}.cgroup`, failure === "mismatched-receipt" ? "0::/foreign/fixture.slice/foreign.scope\n" : liveCgroup);
      fs.writeFileSync(marker, "admitted");
      scopeDirectory = `/sys/fs/cgroup${cgroup}`;
      scopeEvents = `${scopeDirectory}/cgroup.events`;
    }
    return child as unknown as ChildProcess;
  });
  const unconfirmed = vi.fn();
  const scope = scoped ? { executable: "/fixture-systemd-run", slice: "fixture.slice", warn: vi.fn() } : undefined;
  const transport = viaTransport ? await new ProcessTransport("fixture.slice").launch({
    id: "receipt-fixture", name: "receipt-fixture", cwd: directory, workerPath, workerArguments: [], onUnconfirmedExit: unconfirmed,
  }) : undefined;
  const handle = transport ? { ...transport, pid: Number(transport.sessionId), closed: transport.closed!, waitForClose: transport.waitForClose!, lostContact: transport.lostContact! }
    : await spawnDetached(workerPath, [], directory, { onUnconfirmedExit: unconfirmed }, undefined, scope, termGraceMs, true);
  expect(await handle.isAlive()).toBe(true); // retain the descendant's birth before reparenting
  census.mockClear(); kill.mockClear();
  const timeout = vi.spyOn(globalThis, "setTimeout");
  return {
    handle, transport, census, kill, timeout, unconfirmed, watcher, admissionWatcher, opened, readEvents, closeEvents, order,
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

describe.skipIf(process.platform !== "linux")("verified cgroup-v2 receipt admission", () => {
  it.each(["missing", "mismatched", "mismatched-receipt", "wrong-slice", "v1-only", "hybrid", "unowned", "open", "read", "watcher-arm", "watcher-arm-async"] as const)("falls back to legacy monitoring for %s", async failure => {
    const f = await fixtureTree(7_000, true, "1", failure, true);
    expect(f.transport?.liveness).toBeUndefined();
    expect(f.handle.treeClosed).toBeUndefined();
    expect(spawn).toHaveBeenCalled();
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
    if (["read", "watcher-arm", "watcher-arm-async"].includes(failure)) expect(f.closeEvents.mock.calls.filter(([fd]) => fd === 700_000_010)).toHaveLength(1);
  });

  it("publishes event custody only after opening, arming and validating our owned v2 scope", async () => {
    const f = await fixtureTree(7_000, true, "1", undefined, true);
    expect(f.transport?.liveness).toBe("events");
    expect(f.handle.treeClosed).toBeInstanceOf(Promise);
    expect(f.order.slice(0, 3)).toEqual(["open", "watch", "read"]);
    expect(f.admissionWatcher.close).toHaveBeenCalledOnce();
    expect(f.readEvents.mock.calls.filter(([fd]) => fd === 700_000_010).every(call => (call as unknown[])[4] === 0)).toBe(true);
    await f.close();
    expect(f.scans()).toBe(0);
    f.depart(); f.emptyScope();
    await expect(f.handle.treeClosed).resolves.toBeUndefined();
    expect(f.watcher.close).toHaveBeenCalledOnce();
    expect(f.closeEvents.mock.calls.filter(([fd]) => fd === 700_000_010)).toHaveLength(1);
    expect(await f.handle.isAlive()).toBe(false);
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

describe.skipIf(process.platform !== "linux")("real owned systemd user scope", () => {
  it.skipIf(!fs.existsSync(`/run/user/${process.getuid?.()}/bus`))("waits for a real cgroup-v2 descendant after primary native close", async () => {
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
      expect(cgroup).toMatch(/^0::\/.*\/app\.slice\/fabric-scope-.+\.scope$/);
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
