import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { EventEmitter } from "node:events";
import { spawn, type ChildProcess } from "node:child_process";
import { afterEach, describe, expect, it, vi } from "vitest";
import { watchResidentChild } from "../src/residency/child-lifetime.js";

const sleep = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
async function until(predicate: () => boolean, ms = 5_000): Promise<void> {
  const deadline = Date.now() + ms;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error("Native child fixture deadline");
    await sleep(20);
  }
}
function fakeChild() {
  const events = new EventEmitter();
  events.on("error", () => {});
  const kill = vi.fn(() => true);
  const child = Object.assign(events, { kill }) as unknown as ChildProcess;
  return { child, events, kill };
}
afterEach(() => vi.useRealTimers());

describe("resident native child receipts (not attempt membership)", () => {
  it("records native exit without waiting for inherited pipes to close", async () => {
    const { child, events, kill } = fakeChild();
    const lifetime = watchResidentChild(child);
    events.emit("exit", 7, null);
    expect(await lifetime.exit).toEqual({ code: 7, signal: null });
    expect(lifetime.exited).toBe(true);
    await lifetime.stop();
    expect(kill).not.toHaveBeenCalled();
    events.emit("close", 0, null);
    expect(await lifetime.exit).toEqual({ code: 7, signal: null });
  });

  it("does not mistake a spawn/kill error for an exit receipt", async () => {
    const { child, events } = fakeChild();
    const lifetime = watchResidentChild(child);
    events.emit("error", new Error("ENOENT"));
    expect(lifetime.exited).toBe(false);
    events.emit("close", -2, null);
    expect(await lifetime.exit).toEqual({ code: -2, signal: null });
  });

  it("single-flights stop, escalates TERM, and waits for native exit after KILL", async () => {
    vi.useFakeTimers();
    const { child, events, kill } = fakeChild();
    const lifetime = watchResidentChild(child);
    const stop = lifetime.stop();
    expect(lifetime.stop()).toBe(stop);
    let settled = false;
    void stop.then(() => { settled = true; });
    expect(kill.mock.calls).toEqual([["SIGTERM"]]);
    await vi.advanceTimersByTimeAsync(5_000);
    expect(kill.mock.calls).toEqual([["SIGTERM"], ["SIGKILL"]]);
    expect(settled).toBe(false);
    expect(lifetime.exited).toBe(false);
    events.emit("exit", null, "SIGKILL");
    await vi.advanceTimersByTimeAsync(50);
    await stop;
    expect(settled).toBe(true);
    expect(await lifetime.exit).toEqual({ code: null, signal: "SIGKILL" });
    expect(kill).toHaveBeenCalledTimes(2);
  });

  it.each([true, false])("does not treat kill()=%s or a timeout as checked exit", async success => {
    vi.useFakeTimers();
    const { child, kill } = fakeChild();
    kill.mockReturnValue(success);
    const lifetime = watchResidentChild(child);
    const stop = lifetime.stop();
    const rejected = expect(stop).rejects.toThrow("native exit receipt");
    await vi.advanceTimersByTimeAsync(10_000);
    await rejected;
    expect(lifetime.exited).toBe(false);
    expect(lifetime.stop()).toBe(stop);
    expect(kill).toHaveBeenCalledTimes(2);
  });

  it("keeps signal exceptions as failure, never an exit receipt", async () => {
    const { child, kill } = fakeChild();
    kill.mockImplementation(() => { throw new Error("EPERM"); });
    const lifetime = watchResidentChild(child);
    await expect(lifetime.stop()).rejects.toThrow("EPERM");
    expect(lifetime.exited).toBe(false);
  });
});

// No Linux-only guard: Windows CI exercises its real native process API too.
describe("native process shutdown", () => {
  it("stops a real child and receives its exit before resolving", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-native-child-"));
    const ready = path.join(root, "ready");
    const child = spawn(process.execPath, ["-e", `const fs=require('node:fs');
process.on('SIGTERM',()=>{});fs.writeFileSync(${JSON.stringify(ready)},'ready');setInterval(()=>{},1000);`], { stdio: "ignore" });
    const lifetime = watchResidentChild(child);
    child.on("error", () => {});
    try {
      await until(() => fs.existsSync(ready));
      const stopped = lifetime.stop();
      expect(lifetime.stop()).toBe(stopped);
      await stopped;
      expect(lifetime.exited).toBe(true);
      const receipt = await lifetime.exit;
      if (process.platform !== "win32") expect(receipt.signal).toBe("SIGKILL");
      else expect(receipt.code !== null || receipt.signal !== null).toBe(true);
    } finally {
      if (!lifetime.exited) child.kill("SIGKILL");
      await lifetime.exit;
      fs.rmSync(root, { recursive: true, force: true });
    }
  }, 15_000);

  it("gets a native receipt for a failed executable spawn", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-native-missing-"));
    const child = spawn(path.join(root, "absent-executable"), [], { stdio: "ignore" });
    const lifetime = watchResidentChild(child);
    const errors: Error[] = [];
    child.on("error", error => errors.push(error));
    try {
      const receipt = await lifetime.exit;
      expect(errors).toHaveLength(1);
      expect(receipt.code).not.toBe(0);
      await lifetime.stop();
      expect(lifetime.exited).toBe(true);
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it("does not confuse a detached TERM-resistant pipe holder with child exit", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-native-pipe-"));
    const ready = path.join(root, "helper-ready"), stop = path.join(root, "helper-stop"), exited = path.join(root, "helper-exited");
    // The helper owns its cooperative stop/exit files before it detaches. No
    // numeric PID signaling, ancestry sweep, or external terminal server cleanup.
    const helper = `const fs=require('node:fs');process.on('SIGTERM',()=>{});
fs.writeFileSync(${JSON.stringify(ready)},String(process.pid));
setInterval(()=>{if(fs.existsSync(${JSON.stringify(stop)})){fs.writeFileSync(${JSON.stringify(exited)},'exited');process.exit(0);}},20);`;
    const child = spawn(process.execPath, ["-e", `const {spawn}=require('node:child_process');
const helper=spawn(process.execPath,['-e',${JSON.stringify(helper)}],{detached:true,stdio:['ignore',1,2]});
helper.unref();process.exit(7);`], { stdio: ["ignore", "pipe", "pipe"] });
    const lifetime = watchResidentChild(child);
    child.on("error", () => {});
    let closed = false;
    const close = new Promise<void>(resolve => child.once("close", () => { closed = true; resolve(); }));
    try {
      await until(() => fs.existsSync(ready));
      expect(await lifetime.exit).toEqual({ code: 7, signal: null });
      expect(closed).toBe(false);
      await lifetime.stop();
      expect(fs.existsSync(exited)).toBe(false); // Direct exit says nothing about helper exit.
      expect(closed).toBe(false);
    } finally {
      fs.writeFileSync(stop, "stop");
      if (!lifetime.exited) child.kill("SIGKILL");
      await lifetime.exit;
      await until(() => fs.existsSync(exited));
      await close;
      fs.rmSync(root, { recursive: true, force: true });
    }
  }, 15_000);
});
