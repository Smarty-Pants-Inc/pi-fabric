import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { EventEmitter } from "node:events";
import type { ChildProcess } from "node:child_process";
import { afterEach, describe, expect, it, vi } from "vitest";
import { hostLeasePath, writeHostLease } from "../src/topology/host-leases.js";
import type { ResidentHostConfig } from "../src/residency/protocol.js";

const mocks = vi.hoisted(() => ({ spawn: vi.fn(), alive: new Set<number>(), births: new Map<number, string>() }));
vi.mock("cross-spawn", () => ({ default: mocks.spawn }));
vi.mock("../src/residency/process-identity.js", () => ({
  processStartTime: (pid: number) => mocks.births.get(pid) ?? "fixture-birth", residentProcessAlive: (pid: number) => mocks.alive.has(pid),
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
        hostId: "fixture-host", pid: this.pid, processStartTime: "fixture-birth", startedAt: Date.now(),
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
    if (signal === "SIGTERM") {
      // Capture has to precede destructive signals, even when TERM never exits.
      expect(fs.readdirSync(path.join(this.config.residencyRoot, "wedges")).some(name => name.startsWith("20"))).toBe(true);
    }
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
  fs.mkdirSync(path.join(root, "runs"));
  fs.mkdirSync(path.join(root, "runs", "one"));
  fs.writeFileSync(path.join(root, "config.json"), JSON.stringify(config));
  fs.writeFileSync(path.join(root, "child-stderr.log"), Array.from({ length: 220 }, (_, i) => `line-${i}`).join("\n") + "\n");
  const children: FakeChild[] = [];
  mocks.spawn.mockImplementation(() => {
    const child = new FakeChild(config, children.length, alwaysStall, noLease);
    children.push(child);
    return child as unknown as ChildProcess;
  });
  const controller = new AbortController();
  const done = supervise(path.join(root, "config.json"), { signal: controller.signal,
    reportWaitMs: 5, termMs: 30, killWaitMs: 30 });
  const events = () => fs.readFileSync(path.join(root, "launcher.log"), "utf8").trim().split("\n").map(line => JSON.parse(line));
  return { root, config, children, controller, done, events, close: async () => {
    // Prevent fixture shutdown from asserting watchdog-only evidence ordering.
    for (const child of children) {
      clearInterval(child.timer);
      child.kill = (signal: string) => { child.emit("exit", null, signal); return true; };
    }
    controller.abort();
    await vi.advanceTimersByTimeAsync(100);
    try { await done; } finally { fs.rmSync(root, { recursive: true, force: true }); }
  } };
};

afterEach(() => { vi.useRealTimers(); mocks.spawn.mockReset(); mocks.alive.clear(); mocks.births.clear(); });

describe("launcher live-child lease watchdog", () => {
  it("captures evidence, reports, TERMs then KILLs, respawns and records one recovered marker only after a new lease", async () => {
    vi.useFakeTimers(); vi.setSystemTime(new Date("2026-01-01T00:00:00Z"));
    const f = fixture();
    try {
      await vi.advanceTimersByTimeAsync(100);
      const first = f.children[0]!;
      first.renewing = false;
      fs.writeFileSync(path.join(f.root, "config.json"), JSON.stringify({ ...f.config, piBinary: "unwanted-new-release.mjs" }));
      await vi.advanceTimersByTimeAsync(155);
      expect(first.signals).toEqual(["SIGUSR2", "SIGTERM"]);
      expect(fs.existsSync(path.join(f.root, "wedges", "latest.json"))).toBe(false);
      await vi.advanceTimersByTimeAsync(245);
      expect(f.children).toHaveLength(2);
      expect(first.signals).toEqual(["SIGUSR2", "SIGTERM", "SIGKILL"]);
      expect(first.signalTimes[1]! - first.signalTimes[0]!).toBe(5);
      expect(first.signalTimes[2]! - first.signalTimes[1]!).toBe(30);
      expect(f.children[1]!.signals).toEqual([]);
      const replacementConfig = mocks.spawn.mock.calls[1]![2].env.PI_FABRIC_RESIDENT_CONFIG;
      expect(JSON.parse(fs.readFileSync(replacementConfig, "utf8"))).toEqual(f.config);
      expect(mocks.spawn.mock.calls[1]![1]).toEqual(mocks.spawn.mock.calls[0]![1]);
      const restarts = f.events().filter(row => row.event === "watchdog-restart");
      expect(restarts).toHaveLength(1);
      const dir = restarts[0]!.evidenceDir;
      expect(fs.readFileSync(path.join(dir, "lease.json"), "utf8")).toContain('"id":"fixture-host"');
      expect(fs.readFileSync(path.join(dir, "runs-entry-count"), "utf8")).toBe("1");
      const lines = fs.readFileSync(path.join(dir, "child-log-tail"), "utf8").split("\n");
      expect(lines).toHaveLength(200); expect(lines[0]).toBe("line-20");
      const marker = JSON.parse(fs.readFileSync(path.join(f.root, "wedges", "latest.json"), "utf8"));
      expect(marker).toMatchObject({ topic: "fleet.residency.fixture-host", kind: "wedge-recovered", hostId: "fixture-host", evidenceDir: dir, restartCount: 1 });
      const lease = JSON.parse(fs.readFileSync(hostLeasePath(f.config.meshRoot, "fixture-host"), "utf8"));
      expect(lease.updatedAt).toBeGreaterThan(restarts[0]!.at);
      const env = mocks.spawn.mock.calls[0]![2].env;
      expect(env.NODE_OPTIONS).toContain("--report-on-signal --report-signal=SIGUSR2");
      expect(env.NODE_OPTIONS).toContain(`--report-directory="${path.join(f.root, "wedges", "reports")}"`);
      await vi.advanceTimersByTimeAsync(1000);
      expect(f.children).toHaveLength(2);
    } finally { await f.close(); }
  });

  it("waits for a first lease and then the full cold-start allowance", async () => {
    vi.useFakeTimers(); vi.setSystemTime(new Date("2026-01-01T00:00:00Z"));
    const f = fixture({ coldStartMs: 500 }, false, true);
    try {
      await vi.advanceTimersByTimeAsync(5000);
      expect(f.children[0]!.signals).toEqual([]);
      f.children[0]!.renew();
      await vi.advanceTimersByTimeAsync(450);
      expect(f.children[0]!.signals).toEqual([]);
      await vi.advanceTimersByTimeAsync(200);
      expect(f.children[0]!.signals).toEqual(["SIGUSR2", "SIGTERM", "SIGKILL"]);
      expect(f.children).toHaveLength(2);
    } finally { await f.close(); }
  });

  it.each(["disabled", "healthy"])("never touches a %s child", async mode => {
    vi.useFakeTimers(); vi.setSystemTime(new Date("2026-01-01T00:00:00Z"));
    const f = fixture(mode === "disabled" ? { enabled: false } : {}, mode === "disabled");
    try {
      await vi.advanceTimersByTimeAsync(3000);
      expect(f.children).toHaveLength(1);
      expect(f.children[0]!.signals).toEqual([]);
      expect(f.events().some(row => row.event.startsWith("watchdog-"))).toBe(false);
    } finally { await f.close(); }
  });

  it("caps restarts at three per hour, keeps the fourth live child and gives up once", async () => {
    vi.useFakeTimers(); vi.setSystemTime(new Date("2026-01-01T00:00:00Z"));
    const f = fixture({ maxRestartsPerHour: 100 }, true);
    try {
      await vi.advanceTimersByTimeAsync(2000);
      expect(f.children).toHaveLength(4);
      expect(f.events().filter(row => row.event === "watchdog-restart")).toHaveLength(3);
      expect(f.events().filter(row => row.event === "watchdog-giving-up")).toHaveLength(1);
      expect(f.children[3]!.signals).toEqual([]);
      await vi.advanceTimersByTimeAsync(5000);
      expect(f.children).toHaveLength(4);
      expect(f.events().filter(row => row.event === "watchdog-giving-up")).toHaveLength(1);
    } finally { await f.close(); }
  });


  it("ignores a different live owner and mismatched or pre-attempt lease metadata", async () => {
    vi.useFakeTimers(); vi.setSystemTime(new Date("2026-01-01T00:00:00Z"));
    const f = fixture({}, true);
    try {
      await vi.advanceTimersByTimeAsync(50);
      writeHostLease(f.config.meshRoot, { id: "fixture-host", rootId: "another-root", identityId: "fixture-host",
        updatedAt: Date.now(), expiresAt: Date.now() + 1000 });
      await vi.advanceTimersByTimeAsync(300);
      expect(f.children[0]!.signals).toEqual([]);
      writeHostLease(f.config.meshRoot, { id: "fixture-host", rootId: f.config.rootId, identityId: "fixture-host",
        updatedAt: Date.now() - 1000, expiresAt: Date.now() + 1000 });
      await vi.advanceTimersByTimeAsync(300);
      expect(f.children[0]!.signals).toEqual([]);
      // A competing owner must not authorize signals against our direct child.
      mocks.alive.add(12345);
      fs.writeFileSync(path.join(f.root, "owner.json"), JSON.stringify({ hostId: "fixture-host", pid: 12345 }));
      f.children[0]!.renew();
      await vi.advanceTimersByTimeAsync(300);
      expect(f.children[0]!.signals).toEqual([]);
      expect(f.children[0]!.stdin.end).toHaveBeenCalledOnce();
    } finally { await f.close(); }
  });

  it("does not interleave watchdog restarts with an active release handover", async () => {
    vi.useFakeTimers(); vi.setSystemTime(new Date("2026-01-01T00:00:00Z"));
    const f = fixture({}, true);
    try {
      await vi.advanceTimersByTimeAsync(50);
      fs.writeFileSync(path.join(f.root, "handover.json"), JSON.stringify({ phase: "preparing" }));
      await vi.advanceTimersByTimeAsync(1000);
      expect(f.children[0]!.signals).toEqual([]);
      expect(f.children).toHaveLength(1);
      fs.rmSync(path.join(f.root, "handover.json"));
      await vi.advanceTimersByTimeAsync(300);
      expect(f.children.length).toBeGreaterThan(1);
    } finally { await f.close(); }
  });

  it("blocks every watchdog signal when the child's observed PID incarnation changes", async () => {
    vi.useFakeTimers(); vi.setSystemTime(new Date("2026-01-01T00:00:00Z"));
    const f = fixture({}, true);
    const result = f.done.catch(error => error as Error);
    try {
      await vi.advanceTimersByTimeAsync(50);
      mocks.births.set(f.children[0]!.pid, "reused-pid-birth");
      await vi.advanceTimersByTimeAsync(200);
      expect((await result as Error).message).toContain("incarnation changed");
      expect(f.children[0]!.signals).toEqual([]);
    } finally { await f.close().catch(() => undefined); }
  });

  it("retains only five evidence directories without removing the report directory", async () => {
    vi.useFakeTimers(); vi.setSystemTime(new Date("2026-01-01T00:00:00Z"));
    const f = fixture({}, true);
    try {
      for (let i = 1; i <= 7; i++) fs.mkdirSync(path.join(f.root, "wedges", `2025-01-0${i}T00:00:00.000Z`));
      await vi.advanceTimersByTimeAsync(200);
      expect(fs.readdirSync(path.join(f.root, "wedges")).filter(name => /^20/.test(name))).toHaveLength(5);
      expect(fs.statSync(path.join(f.root, "wedges", "reports")).isDirectory()).toBe(true);
    } finally { await f.close(); }
  });
});
