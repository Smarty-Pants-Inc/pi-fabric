import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { EventEmitter } from "node:events";
import type { ChildProcess } from "node:child_process";
import { afterEach, describe, expect, it, vi } from "vitest";
import { writeHostLease } from "../src/topology/host-leases.js";
import type { ResidentHostConfig } from "../src/residency/protocol.js";

const mocks = vi.hoisted(() => ({ spawn: vi.fn(), alive: new Set<number>(), births: new Map<number, string>(),
  sessionEmpty: true, lockHook: undefined as (() => void) | undefined }));
vi.mock("cross-spawn", () => ({ default: mocks.spawn }));
vi.mock("../src/residency/process-identity.js", () => ({
  processStartTime: (pid: number) => mocks.births.get(pid) ?? "fixture-birth", residentProcessAlive: (pid: number) => mocks.alive.has(pid),
}));
vi.mock("../src/residency/file-lock.js", () => ({ lockFile: async (file: string) => {
  mocks.lockHook?.();
  const fs = await import("node:fs");
  return fs.openSync(file, "w");
} }));
vi.mock("../src/residency/launcher-owner.js", async importOriginal => ({
  ...await importOriginal<typeof import("../src/residency/launcher-owner.js")>(),
  checkResidentSessionExit: () => ({ empty: mocks.sessionEmpty, reason: mocks.sessionEmpty ? "owned session exited" : "owned session still has members", members: mocks.sessionEmpty ? [] : [900_999] }),
}));
import { supervise } from "../src/residency/launcher.js";

class FakeChild extends EventEmitter {
  pid: number;
  stdout = new EventEmitter();
  stderr = new EventEmitter();
  stdin = { end: vi.fn() };
  signals: string[] = [];
  signalTimes: number[] = [];
  renewing = true;
  onSignal?: (signal: string) => void;
  timer: ReturnType<typeof setInterval>;
  constructor(readonly config: ResidentHostConfig, readonly index: number, alwaysStall: boolean, noLease: boolean) {
    super();
    this.pid = 900_000 + index;
    mocks.alive.add(this.pid);
    mocks.births.set(this.pid, "fixture-birth");
    this.renewing = !alwaysStall;
    setTimeout(() => {
      this.emit("spawn");
      fs.writeFileSync(path.join(config.residencyRoot, "owner.json"), JSON.stringify({
        hostId: "fixture-host", token: "fixture-owner-token", pid: this.pid, processStartTime: "fixture-birth", startedAt: Date.now(),
      }));
      if (!noLease) this.renew();
    }, 0);
    this.timer = setInterval(() => { if (this.renewing && !noLease) this.renew(); }, 25);
  }
  renew(): void {
    writeHostLease(this.config.meshRoot, { id: "fixture-host", rootId: this.config.rootId,
      identityId: "fixture-host", updatedAt: Date.now(), expiresAt: Date.now() + 1000 });
  }
  kill(signal: string): boolean {
    this.signals.push(signal);
    this.signalTimes.push(Date.now());
    this.onSignal?.(signal);
    if (signal === "SIGTERM") expect(fs.readdirSync(path.join(this.config.residencyRoot, "wedges")).some(name => name.startsWith("20"))).toBe(true);
    if (signal === "SIGKILL") {
      clearInterval(this.timer);
      mocks.alive.delete(this.pid);
      this.emit("exit", null, signal);
    }
    return true;
  }
}

const fixture = (settings: ResidentHostConfig["watchdog"] = {}, alwaysStall = false, noLease = false) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "launcher-wedge-unit-"));
  const config = { cwd: root, piBinary: path.join(root, "fixture.mjs"), residencyRoot: root,
    rootId: "fixture-root", meshRoot: path.join(root, "mesh"),
    watchdog: { intervalMs: 50, stallMs: 100, coldStartMs: 0, ...settings } } as ResidentHostConfig;
  fs.mkdirSync(path.join(root, "runs")); fs.mkdirSync(path.join(root, "runs", "one"));
  fs.writeFileSync(path.join(root, "config.json"), JSON.stringify(config));
  fs.writeFileSync(path.join(root, "child-stderr.log"), Array.from({ length: 220 }, (_, i) => `line-${i}`).join("\n") + "\n");
  const children: FakeChild[] = [];
  mocks.spawn.mockImplementation(() => {
    const child = new FakeChild(config, children.length, alwaysStall, noLease);
    children.push(child); return child as unknown as ChildProcess;
  });
  const controller = new AbortController();
  const done = supervise(path.join(root, "config.json"), { signal: controller.signal,
    reportWaitMs: 5, termMs: 30, killWaitMs: 30, proofRetryMs: 50, proofChecks: 3 });
  const events = () => fs.readFileSync(path.join(root, "launcher.log"), "utf8").trim().split("\n").map(line => JSON.parse(line));
  return { root, config, children, controller, done, events, close: async () => {
    for (const child of children) {
      clearInterval(child.timer);
      child.kill = (signal: string) => { child.emit("exit", null, signal); return true; };
    }
    controller.abort(); await vi.advanceTimersByTimeAsync(100);
    try { await done; } finally { fs.rmSync(root, { recursive: true, force: true }); }
  } };
};
afterEach(() => { vi.useRealTimers(); mocks.spawn.mockReset(); mocks.alive.clear(); mocks.births.clear(); mocks.lockHook = undefined; mocks.sessionEmpty = true; });

const clock = () => { vi.useFakeTimers(); vi.setSystemTime(new Date("2026-01-01T00:00:00Z")); };

describe("launcher intentional idle exit", () => {
  it("makes no late diagnostic write even when the host retires before the first owner sample", async () => {
    clock();
    const f = fixture({ enabled: false });
    try {
      await vi.advanceTimersByTimeAsync(1);
      const child = f.children[0]!;
      fs.writeFileSync(path.join(f.root, "idle-exit.json"), JSON.stringify({ format: 1, reason: "root-dead-idle",
        rootId: f.config.rootId, pid: child.pid, token: "fixture-owner-token", at: Date.now(), idleMs: 1 }));
      clearInterval(child.timer);
      mocks.alive.delete(child.pid);
      child.emit("exit", 0, null);
      await vi.advanceTimersByTimeAsync(100);
      await f.done;
      expect(f.children).toHaveLength(1);
      expect(f.events().filter(row => row.event === "child-exit")).toEqual([]);
      expect(fs.existsSync(path.join(f.root, "error.json"))).toBe(false);
    } finally { await f.close(); }
  });
});

describe("launcher watchdog Windows contract", () => {
  it.each([{}, { enabled: true }, { enabled: false }])("disables watchdog on win32 regardless of configuration %j, logging once", async settings => {
    clock();
    const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
    Object.defineProperty(process, "platform", { ...platform, value: "win32" });
    const lock = vi.fn(); mocks.lockHook = lock;
    let f: ReturnType<typeof fixture> | undefined;
    try {
      f = fixture(settings, true);
      await vi.advanceTimersByTimeAsync(5000);
      expect(f.children).toHaveLength(1);
      expect(f.children[0]!.signals).toEqual([]);
      expect(f.children[0]!.stdin.end).not.toHaveBeenCalled();
      expect(lock).not.toHaveBeenCalled();
      expect(fs.existsSync(path.join(f.root, "wedges"))).toBe(false);
      expect(fs.existsSync(path.join(f.root, "watchdog-custody.json"))).toBe(false);
      expect(f.events().filter(row => row.event.startsWith("watchdog-"))).toEqual([
        expect.objectContaining({ event: "watchdog-unsupported", message: "watchdog unsupported on win32" }),
      ]);
      const spawnOptions = mocks.spawn.mock.calls[0]![2];
      expect(spawnOptions.detached).toBe(false);
      expect(spawnOptions.env.NODE_OPTIONS).toBe(process.env.NODE_OPTIONS);
      await vi.advanceTimersByTimeAsync(5000);
      expect(f.events().filter(row => row.event === "watchdog-unsupported")).toHaveLength(1);
    } finally {
      try { await f?.close(); } finally { Object.defineProperty(process, "platform", platform); }
    }
  });
});
// These signal, incarnation and session-evidence contracts require POSIX.
describe.skipIf(process.platform === "win32")("launcher live-child lease watchdog", () => {
  it("F1 captures evidence and stops the wedged child but never authorizes replacement from an empty session or sampled descendants", async () => {
    clock(); const f = fixture({}, true);
    try {
      await vi.advanceTimersByTimeAsync(500);
      const first = f.children[0]!;
      expect(first.signals).toEqual(["SIGUSR2", "SIGTERM", "SIGKILL"]);
      expect(first.signalTimes[1]! - first.signalTimes[0]!).toBe(5);
      expect(first.signalTimes[2]! - first.signalTimes[1]!).toBe(30);
      expect(f.children).toHaveLength(1);
      expect(fs.existsSync(path.join(f.root, "watchdog-custody.json"))).toBe(true);
      await supervise(path.join(f.root, "config.json"));
      expect(f.children).toHaveLength(1); // A separate launcher cannot bypass custody.
      expect(fs.existsSync(path.join(f.root, "wedges", "latest.json"))).toBe(false);
      const stopping = f.events().find(row => row.event === "watchdog-stopping")!;
      expect(fs.readFileSync(path.join(stopping.evidenceDir, "lease.json"), "utf8")).toContain('"id":"fixture-host"');
      expect(fs.readFileSync(path.join(stopping.evidenceDir, "runs-entry-count"), "utf8")).toBe("1");
      const lines = fs.readFileSync(path.join(stopping.evidenceDir, "child-log-tail"), "utf8").split("\n");
      expect(lines).toHaveLength(200); expect(lines[0]).toBe("line-20");
      const deferred = f.events().filter(row => row.event === "watchdog-deferred" && row.proofCheck);
      expect(deferred).toHaveLength(3);
      expect(deferred[0]).toMatchObject({ sessionEmpty: true, reason: expect.stringContaining("complete exit receipts") });
      expect(f.events().filter(row => row.event === "watchdog-giving-up")).toHaveLength(1);
      await vi.advanceTimersByTimeAsync(5000);
      expect(f.children).toHaveLength(1); expect(f.events().filter(row => row.event === "watchdog-deferred" && row.proofCheck)).toHaveLength(3);
      const env = mocks.spawn.mock.calls[0]![2].env;
      expect(env.NODE_OPTIONS).toContain("--report-on-signal --report-signal=SIGUSR2");
    } finally { await f.close(); }
  });
  it("F1 an unexpected native exit during the report allowance retains custody and cannot authorize another launcher", async () => {
    clock(); const f = fixture({}, true);
    try {
      await vi.advanceTimersByTimeAsync(50);
      const child = f.children[0]!;
      child.onSignal = signal => {
        if (signal === "SIGUSR2") {
          expect(fs.existsSync(path.join(f.root, "watchdog-custody.json"))).toBe(true);
          clearInterval(child.timer); mocks.alive.delete(child.pid); child.emit("exit", 1, null);
        }
      };
      await vi.advanceTimersByTimeAsync(500);
      expect(child.signals).toEqual(["SIGUSR2"]);
      expect(f.events().filter(row => row.event === "watchdog-deferred" && row.proofCheck)).toHaveLength(3);
      await supervise(path.join(f.root, "config.json"));
      expect(f.children).toHaveLength(1);
    } finally { await f.close(); }
  });

  it("F1 retries a nonempty containment boundary only to the fixed bound, without a second host", async () => {
    clock(); mocks.sessionEmpty = false; const f = fixture({}, true);
    try {
      await vi.advanceTimersByTimeAsync(500);
      expect(f.events().filter(row => row.event === "watchdog-deferred")).toHaveLength(3);
      expect(f.events().find(row => row.event === "watchdog-deferred")).toMatchObject({ sessionEmpty: false, members: [900_999] });
      mocks.sessionEmpty = true; await vi.advanceTimersByTimeAsync(2000);
      expect(f.children).toHaveLength(1);
    } finally { await f.close(); }
  });
  it.each(["SIGUSR2", "SIGTERM", "SIGKILL"])("F2 renews immediately before %s and aborts that signal and every later signal", async signal => {
    clock(); const f = fixture({}, true);
    try {
      await vi.advanceTimersByTimeAsync(50); const child = f.children[0]!;
      const recover = () => { child.renewing = true; child.renew(); };
      if (signal === "SIGUSR2") mocks.lockHook = recover;
      else child.onSignal = previous => {
        if (previous === (signal === "SIGTERM" ? "SIGUSR2" : "SIGTERM")) setTimeout(recover, 1);
      };
      await vi.advanceTimersByTimeAsync(1000);
      expect(child.signals).toEqual(signal === "SIGUSR2" ? [] : signal === "SIGTERM" ? ["SIGUSR2"] : ["SIGUSR2", "SIGTERM"]);
      expect(f.events().find(row => row.event === "watchdog-aborted")).toMatchObject({ signal, reason: expect.stringContaining("lease renewed") });
      expect(fs.existsSync(path.join(f.root, "watchdog-custody.json"))).toBe(signal !== "SIGUSR2");
      expect(f.children).toHaveLength(1);
    } finally { await f.close(); }
  });
  it.each(["handover", "owner"])("F2 rechecks %s ownership after the report window", async changed => {
    clock(); const f = fixture({}, true);
    try {
      await vi.advanceTimersByTimeAsync(50); const child = f.children[0]!;
      child.onSignal = signal => {
        if (signal !== "SIGUSR2") return;
        if (changed === "handover") fs.writeFileSync(path.join(f.root, "handover.json"), JSON.stringify({ phase: "preparing" }));
        else {
          const file = path.join(f.root, "owner.json"); const owner = JSON.parse(fs.readFileSync(file, "utf8"));
          fs.writeFileSync(file, JSON.stringify({ ...owner, token: "new-generation" }));
          child.renewing = true;
        }
      };
      await vi.advanceTimersByTimeAsync(500);
      expect(child.signals).toEqual(["SIGUSR2"]);
      expect(f.events().find(row => row.event === "watchdog-aborted")).toMatchObject({ signal: "SIGTERM" });
      expect(f.children).toHaveLength(1);
    } finally { await f.close(); }
  });
  it("F2 revalidates again after durable custody publication if storage allowed the lease to renew", async () => {
    clock(); const f = fixture({}, true);
    const original = fs.linkSync;
    const write = vi.spyOn(fs, "linkSync").mockImplementation((from, file) => {
      const result = original(from, file);
      if (String(file).endsWith("watchdog-custody.json")) {
        vi.setSystemTime(new Date(Date.now() + 1));
        f.children[0]!.renewing = true; f.children[0]!.renew();
      }
      return result;
    });
    try {
      await vi.advanceTimersByTimeAsync(1000);
      expect(f.children[0]!.signals).toEqual([]);
      expect(f.events().find(row => row.event === "watchdog-aborted")).toMatchObject({ signal: "SIGUSR2", reason: expect.stringContaining("lease renewed") });
      expect(f.children).toHaveLength(1);
    } finally { write.mockRestore(); await f.close(); }
  });

  it("F3 evidence writes fail, supervision retries, then restored storage permits recovery and owned shutdown", async () => {
    clock(); const f = fixture({}, true);
    const original = fs.writeFileSync;
    const write = vi.spyOn(fs, "writeFileSync").mockImplementation((file, ...args) => {
      if (String(file).includes(`${path.sep}wedges${path.sep}20`)) throw Object.assign(new Error("injected EIO"), { code: "EIO" });
      return original(file, ...args);
    });
    try {
      await vi.advanceTimersByTimeAsync(500);
      expect(f.children[0]!.signals).toEqual([]);
      expect(f.events().filter(row => row.event === "watchdog-evidence-error").length).toBeGreaterThan(1);
      expect(process.listeners("SIGTERM").length).toBeGreaterThan(0);
      write.mockRestore(); await vi.advanceTimersByTimeAsync(500);
      expect(f.children[0]!.signals).toEqual(["SIGUSR2", "SIGTERM", "SIGKILL"]);
      expect(f.events().some(row => row.event === "watchdog-deferred" && row.sessionEmpty)).toBe(true);
      expect(f.children).toHaveLength(1);
    } finally { write.mockRestore(); await f.close(); }
  });
  it("F3 report enumeration errors do not abandon the owned child", async () => {
    clock(); const f = fixture({}, true);
    const original = fs.readdirSync;
    const read = vi.spyOn(fs, "readdirSync").mockImplementation(((file: fs.PathLike, ...args: unknown[]) => {
      if (String(file) === path.join(f.root, "wedges", "reports")) throw new Error("injected report EIO");
      return (original as Function)(file, ...args);
    }) as typeof fs.readdirSync);
    try {
      await vi.advanceTimersByTimeAsync(500);
      expect(f.children[0]!.signals).toEqual(["SIGUSR2", "SIGTERM", "SIGKILL"]);
      expect(f.events().find(row => row.event === "watchdog-evidence-error")).toMatchObject({ stage: "reports" });
    } finally { read.mockRestore(); await f.close(); }
  });
  it("waits for a first lease and then the full cold-start allowance", async () => {
    clock(); const f = fixture({ coldStartMs: 500 }, false, true);
    try {
      await vi.advanceTimersByTimeAsync(5000); expect(f.children[0]!.signals).toEqual([]);
      f.children[0]!.renew(); await vi.advanceTimersByTimeAsync(450); expect(f.children[0]!.signals).toEqual([]);
      await vi.advanceTimersByTimeAsync(200); expect(f.children[0]!.signals).toEqual(["SIGUSR2", "SIGTERM", "SIGKILL"]);
      expect(f.children).toHaveLength(1);
    } finally { await f.close(); }
  });
  it.each(["disabled", "healthy"])("never touches a %s child", async mode => {
    clock(); const f = fixture(mode === "disabled" ? { enabled: false } : {}, mode === "disabled");
    try {
      await vi.advanceTimersByTimeAsync(3000); expect(f.children).toHaveLength(1); expect(f.children[0]!.signals).toEqual([]);
      expect(f.events().some(row => row.event.startsWith("watchdog-"))).toBe(false);
    } finally { await f.close(); }
  });
  it("ignores a different live owner and mismatched or pre-attempt leases", async () => {
    clock(); const f = fixture({}, true);
    try {
      await vi.advanceTimersByTimeAsync(50);
      writeHostLease(f.config.meshRoot, { id: "fixture-host", rootId: "another-root", identityId: "fixture-host", updatedAt: Date.now(), expiresAt: Date.now() + 1000 });
      await vi.advanceTimersByTimeAsync(300); expect(f.children[0]!.signals).toEqual([]);
      writeHostLease(f.config.meshRoot, { id: "fixture-host", rootId: f.config.rootId, identityId: "fixture-host", updatedAt: Date.now() - 1000, expiresAt: Date.now() + 1000 });
      await vi.advanceTimersByTimeAsync(300); expect(f.children[0]!.signals).toEqual([]);
      mocks.alive.add(12345); fs.writeFileSync(path.join(f.root, "owner.json"), JSON.stringify({ hostId: "fixture-host", pid: 12345 }));
      f.children[0]!.renew(); await vi.advanceTimersByTimeAsync(300); expect(f.children[0]!.signals).toEqual([]);
      expect(f.children[0]!.stdin.end).toHaveBeenCalledOnce();
    } finally { await f.close(); }
  });
  it("blocks recovery during an active handover", async () => {
    clock(); const f = fixture({}, true);
    try {
      await vi.advanceTimersByTimeAsync(50); fs.writeFileSync(path.join(f.root, "handover.json"), JSON.stringify({ phase: "preparing" }));
      await vi.advanceTimersByTimeAsync(1000); expect(f.children[0]!.signals).toEqual([]);
      fs.rmSync(path.join(f.root, "handover.json")); await vi.advanceTimersByTimeAsync(300);
      expect(f.children[0]!.signals).toEqual(["SIGUSR2", "SIGTERM", "SIGKILL"]); expect(f.children).toHaveLength(1);
    } finally { await f.close(); }
  });
  it("blocks every signal after PID incarnation changes while retaining supervision", async () => {
    clock(); const f = fixture({}, true);
    try {
      await vi.advanceTimersByTimeAsync(50); mocks.births.set(f.children[0]!.pid, "reused-pid-birth");
      await vi.advanceTimersByTimeAsync(500); expect(f.children[0]!.signals).toEqual([]);
      expect(f.events().find(row => row.event === "watchdog-aborted")).toMatchObject({ signal: "SIGUSR2", reason: expect.stringContaining("incarnation changed") });
    } finally { await f.close(); }
  });
  it("retains five evidence directories without removing reports", async () => {
    clock(); const f = fixture({}, true);
    try {
      for (let i = 1; i <= 7; i++) fs.mkdirSync(path.join(f.root, "wedges", `2025-01-0${i}T00:00:00.000Z`));
      await vi.advanceTimersByTimeAsync(200);
      expect(fs.readdirSync(path.join(f.root, "wedges")).filter(name => /^20/.test(name))).toHaveLength(5);
      expect(fs.statSync(path.join(f.root, "wedges", "reports")).isDirectory()).toBe(true);
    } finally { await f.close(); }
  });
});
