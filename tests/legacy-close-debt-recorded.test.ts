import { spawn, type ChildProcess } from "node:child_process";
import { EventEmitter } from "node:events";
import fs from "node:fs";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { spawnDetached } from "../src/agents/transports/process-utils-legacy.js";

// Keep: #17990, the native custody trust boundary. Exercise the real legacy
// handle with recorded child events/procfs and no host launches or signals.
// ActorManager's public paths/publication ordering live in the companion suite.
vi.mock("node:child_process", async importOriginal => ({
  ...await importOriginal<typeof import("node:child_process")>(),
  spawn: vi.fn(() => { throw new Error("Unrecorded native launch"); }),
}));

type Row = { parent: number; group: number; started: string; unreadable?: boolean };
const pid = 900_100_000;
const rows = new Map<number, Row>();
let censusUnknown = false;
let birthError: "ENOENT" | "EACCES" | undefined;
let onSignal: ((target: number, signal: NodeJS.Signals) => void) | undefined;
const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
const error = (code: string) => Object.assign(new Error(`recorded ${code}`), { code });

beforeEach(() => {
  vi.useFakeTimers();
  vi.mocked(spawn).mockReset().mockImplementation(() => { throw new Error("Unrecorded native launch"); });
  rows.clear();
  rows.set(pid, { parent: 1, group: pid, started: "12345" });
  censusUnknown = false;
  birthError = undefined;
  onSignal = undefined;
  const read = fs.readFileSync, list = fs.readdirSync;
  vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, ...args: unknown[]) => {
    const match = /^\/proc\/(\d+)\/stat$/.exec(String(file));
    if (!match) return Reflect.apply(read, fs, [file, ...args]);
    const member = Number(match[1]);
    if (member === pid && birthError) throw error(birthError);
    const row = rows.get(member);
    if (!row) throw error("ENOENT");
    if (row.unreadable) throw error("EACCES");
    const fields = Array<string>(20).fill("0");
    fields[0] = "S"; fields[1] = String(row.parent); fields[2] = String(row.group);
    fields[3] = String(row.group); fields[19] = row.started;
    return `${member} (recorded-native) ${fields.join(" ")}`;
  }) as typeof fs.readFileSync);
  vi.spyOn(fs, "readdirSync").mockImplementation(((file: fs.PathLike, ...args: unknown[]) => {
    if (String(file) !== "/proc") return Reflect.apply(list, fs, [file, ...args]);
    if (censusUnknown) throw error("EACCES");
    return [...rows.keys()].map(String);
  }) as typeof fs.readdirSync);
  vi.spyOn(process, "kill").mockImplementation((target, signal) => {
    if (signal === 0) {
      if (!rows.has(target)) throw error("ESRCH");
      return true;
    }
    if (!onSignal || typeof signal !== "string") throw new Error("Unrecorded native signal");
    onSignal(target, signal as NodeJS.Signals);
    return true;
  });
});

afterEach(() => {
  const timers = vi.getTimerCount();
  vi.useRealTimers();
  vi.restoreAllMocks();
  Object.defineProperty(process, "platform", platform);
  expect(timers, "recorded native operations must leave no timer/writer").toBe(0);
});

async function launch() {
  const child = Object.assign(new EventEmitter(), { pid, unref: vi.fn(), kill: vi.fn() });
  vi.mocked(spawn).mockReturnValueOnce(child as unknown as ChildProcess);
  const unconfirmed = vi.fn();
  const handle = await spawnDetached("recorded-worker.mjs", [], process.cwd(),
    { onUnconfirmedExit: unconfirmed }, undefined, undefined, 100, true);
  const exit = () => { rows.delete(pid); child.emit("exit", 0, null); };
  const close = () => child.emit("close", 0, null);
  const passiveDeadline = async () => {
    const waiting = handle.waitForClose();
    await vi.advanceTimersByTimeAsync(7_000);
    await waiting;
    expect(handle.lostContact()).toMatch(/native close within 7000ms/);
    expect(unconfirmed).toHaveBeenCalledTimes(1);
  };
  return { child, handle, unconfirmed, exit, close, passiveDeadline };
}

describe.skipIf(process.platform !== "linux")("recorded legacy passive close debt (#17990)", () => {
  it("the first debt query joins late native exit/close and an empty birth-checked tree, without private pre-drain", async () => {
    const f = await launch();
    await f.passiveDeadline();
    f.exit(); f.close();
    await f.handle.closed; // Observation, not stop/waitForClose/isAlive repair.
    expect(f.handle.lostContact()).toBeUndefined();
    expect(f.handle.stopDebt?.()).toBeUndefined();
    expect(f.handle.nativeExit?.()).toMatchObject({ pid, processStartTime: "12345", code: 0 });
    await f.handle.stop();
    expect(f.unconfirmed).toHaveBeenCalledTimes(1);
    expect(process.kill).not.toHaveBeenCalled();
    expect(spawn).toHaveBeenCalledTimes(1);
  });

  it("descriptor close without a captured primary exit is not sufficient", async () => {
    const f = await launch();
    await f.passiveDeadline();
    rows.delete(pid); f.close();
    expect(f.handle.nativeExit?.()).toBeUndefined();
    expect(f.handle.lostContact()).toMatch(/native close within 7000ms/);
    // Even a false probe cannot stand in for the captured primary-exit receipt.
    expect(await f.handle.isAlive()).toBe(false);
    expect(f.handle.lostContact()).toMatch(/native close within 7000ms/);
    f.exit();
    expect(f.handle.lostContact()).toBeUndefined();
  });

  it("primary exit without descriptor close retains debt and liveness custody", async () => {
    const f = await launch();
    await f.passiveDeadline();
    f.exit();
    expect(f.handle.lostContact()).toMatch(/native close within 7000ms/);
    expect(await f.handle.isAlive()).toBe(true);
    expect(process.kill).not.toHaveBeenCalled();
    f.close();
  });

  it("late close retains a known separately-grouped descendant until a fresh census proves it gone", async () => {
    const f = await launch();
    rows.set(pid + 1, { parent: pid, group: pid + 1, started: "23456" });
    // Existing legacy capture admits the child only while its parent's birth matches.
    expect(await f.handle.isAlive()).toBe(true);
    await f.passiveDeadline();
    f.exit(); f.close();
    rows.get(pid + 1)!.parent = 1;
    expect(f.handle.lostContact()).toMatch(/native close within 7000ms/);
    expect(await f.handle.isAlive()).toBe(true);
    rows.delete(pid + 1);
    expect(f.handle.lostContact()).toBeUndefined();
    await f.handle.stop();
    expect(vi.mocked(process.kill).mock.calls.filter(([, signal]) => signal !== 0)).toEqual([]);
  });

  it("an empty census plus a pending execution-custody receipt is not a join", async () => {
    const f = await launch();
    f.child.emit("message", { type: "fabric-execution-custody" });
    await f.passiveDeadline();
    f.exit(); f.close();
    expect(f.handle.lostContact()).toMatch(/native close within 7000ms/);
    expect(f.handle.stopDebt?.()).toMatch(/native close within 7000ms/);
    expect(await f.handle.isAlive()).toBe(true);
    expect(process.kill).not.toHaveBeenCalled();
  });

  it.each(["census", "member", "foreign-group"] as const)("late close cannot discharge %s uncertainty", async proof => {
    const f = await launch();
    await f.passiveDeadline();
    f.exit(); f.close();
    if (proof === "census") censusUnknown = true;
    else rows.set(pid + 1, { parent: 1, group: pid, started: "foreign-birth", unreadable: proof === "member" });
    expect(f.handle.lostContact()).toMatch(/native close within 7000ms/);
    expect(f.handle.stopDebt?.()).toMatch(/native close within 7000ms/);
    await expect(f.handle.isAlive()).rejects.toThrow(/recorded EACCES|ownership\/exit/);
    const stopping = expect(f.handle.stop()).rejects.toThrow(/recorded EACCES|ownership\/exit/);
    await vi.advanceTimersByTimeAsync(0);
    await stopping;
    expect(process.kill).not.toHaveBeenCalled();
    // The ownership failure is retryable on this handle, not permission to
    // replace it and not a new immutable termination receipt.
    censusUnknown = false; rows.clear();
    expect(f.handle.lostContact()).toBeUndefined();
    await f.handle.stop();
    expect(f.handle.stopDebt?.()).toBeUndefined();
    expect(process.kill).not.toHaveBeenCalled();
    expect(spawn).toHaveBeenCalledTimes(1);
  });

  it.each(["ENOENT", "EACCES"] as const)("never adopts a second birth after launch-time %s", async code => {
    birthError = code;
    const f = await launch();
    await f.passiveDeadline();
    birthError = undefined;
    rows.get(pid)!.started = "replacement-birth";
    f.child.emit("exit", 0, null); f.close();
    expect(f.handle.nativeExit?.()?.processStartTime).toBeUndefined();
    expect(f.handle.lostContact()).toMatch(/native close within 7000ms/);
    expect(process.kill).not.toHaveBeenCalled();
    expect(spawn).toHaveBeenCalledTimes(1);
  });

  it("does not probe, signal or adopt a reused worker PID after the captured handle exited", async () => {
    const f = await launch();
    await f.passiveDeadline();
    f.exit(); f.close();
    rows.set(pid, { parent: 1, group: pid, started: "replacement-birth" });
    expect(f.handle.lostContact()).toBeUndefined();
    expect(await f.handle.isAlive()).toBe(false);
    await f.handle.stop();
    expect(f.handle.nativeExit?.()?.processStartTime).toBe("12345");
    expect(process.kill).not.toHaveBeenCalled();
    expect(spawn).toHaveBeenCalledTimes(1);
  });

  it("forced-custodian receipt debt supersedes passive debt and survives late close/empty census", async () => {
    const f = await launch();
    await f.passiveDeadline();
    onSignal = (target, signal) => {
      expect(target).toBe(-pid);
      if (signal === "SIGKILL") { f.exit(); f.close(); }
    };
    const stopping = f.handle.stop();
    await vi.advanceTimersByTimeAsync(120);
    await stopping;
    const debt = f.handle.lostContact();
    expect(debt).toMatch(/POSIX worker tree termination is unconfirmed/);
    expect(f.unconfirmed).toHaveBeenCalledTimes(2); // First passive observation, then immutable custody uncertainty.
    expect(f.handle.stopDebt?.()).toBe(debt);
    expect(await f.handle.isAlive()).toBe(false); // False isAlive never erases immutable debt.
    f.close();
    await f.handle.waitForClose();
    expect(f.handle.lostContact()).toBe(debt);
    expect(vi.mocked(process.kill).mock.calls.map(([, signal]) => signal)).toEqual(["SIGTERM", "SIGKILL"]);
  });

  it.each(["helper-failure", "stop-close-deadline"] as const)("Windows %s debt remains immutable after a late native close", async proof => {
    Object.defineProperty(process, "platform", { ...platform, value: "win32" });
    vi.spyOn(process, "emitWarning").mockImplementation(() => {});
    const f = await launch();
    await f.passiveDeadline();
    const helper = Object.assign(new EventEmitter(), { pid: pid + 1, kill: vi.fn() });
    vi.mocked(spawn).mockReturnValueOnce(helper as unknown as ChildProcess);
    const stopping = f.handle.stop();
    if (proof === "helper-failure") {
      helper.emit("error", error("EACCES"));
      f.exit(); f.close();
      helper.emit("close", 1);
      await vi.advanceTimersByTimeAsync(0);
    } else {
      helper.emit("close", 0);
      await vi.advanceTimersByTimeAsync(7_000);
    }
    await stopping;
    const debt = f.handle.lostContact();
    expect(debt).toMatch(proof === "helper-failure" ? /Windows process tree termination is unconfirmed/ : /tree\/native close within 7000ms/);
    f.exit(); f.close();
    await f.handle.waitForClose();
    expect(await f.handle.isAlive()).toBe(false);
    expect(f.handle.lostContact()).toBe(debt);
    expect(f.handle.stopDebt?.()).toBe(debt);
    expect(f.unconfirmed).toHaveBeenCalledTimes(2);
    expect(spawn).toHaveBeenCalledTimes(2); // One captured worker and its one owned helper, never replacement.
    expect(process.kill).not.toHaveBeenCalled();
  });
});
