import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { EventEmitter } from "node:events";
import { createHash } from "node:crypto";
import type { ChildProcess } from "node:child_process";
import { afterEach, describe, expect, it, vi } from "vitest";
import { writeHostLease } from "../src/topology/host-leases.js";
import { MeshStore, meshOutageAlarmSink } from "../src/mesh/store.js";
import { canonicalResidentWakeConfig } from "../src/residency/wake-index.js";
import * as residentWake from "../src/residency/wake.js";
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
  exit(code = 1): void {
    clearInterval(this.timer); mocks.alive.delete(this.pid); this.emit("exit", code, null);
  }
  ready(maintenanceToken = "fixture-owner-token"): void {
    const file = path.join(this.config.residencyRoot, "owner.json");
    const owner = JSON.parse(fs.readFileSync(file, "utf8"));
    fs.writeFileSync(file, JSON.stringify({ ...owner, readyAt: Date.now(), maintenanceReady: 1 }));
    fs.writeFileSync(path.join(this.config.residencyRoot, "maintenance-ready.json"), JSON.stringify({ token: maintenanceToken }));
    this.emit("message", { event: "resident-ready", root: this.config.residencyRoot, token: owner.token });
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

const ROOT_SESSION = "session:aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee";
const fixture = (settings: ResidentHostConfig["watchdog"] = {}, alwaysStall = false, noLease = false, actorCount = 1, rootId = ROOT_SESSION, startImmediately = true) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "launcher-wedge-unit-"));
  const config = { cwd: root, piBinary: path.join(root, "fixture.mjs"), residencyRoot: root,
    rootId, sessionId: "fixture-session", meshRoot: path.join(root, "mesh"), actorRoot: path.join(root, "actors"),
    mesh: { actorScope: "project", maxEventBytes: 64 * 1024, maxReadEvents: 100, stateBackend: "file" },
    watchdog: { intervalMs: 50, stallMs: 100, coldStartMs: 0, ...settings } } as ResidentHostConfig;
  fs.mkdirSync(config.actorRoot);
  fs.writeFileSync(path.join(config.actorRoot, "actors.json"), JSON.stringify({ format: 1,
    actors: Array.from({ length: actorCount }, (_, i) => ({ id: `actor-${i}`, rootId: config.rootId, residency: "durable" })) }));
  fs.mkdirSync(path.join(root, "runs")); fs.mkdirSync(path.join(root, "runs", "one"));
  fs.writeFileSync(path.join(root, "config.json"), JSON.stringify(config));
  fs.writeFileSync(path.join(root, "child-stderr.log"), Array.from({ length: 220 }, (_, i) => `line-${i}`).join("\n") + "\n");
  const children: FakeChild[] = [];
  mocks.spawn.mockImplementation(() => {
    const child = new FakeChild(config, children.length, alwaysStall, noLease);
    children.push(child); return child as unknown as ChildProcess;
  });
  const controller = new AbortController();
  const done = startImmediately ? supervise(path.join(root, "config.json"), { signal: controller.signal,
    reportWaitMs: 5, termMs: 30, killWaitMs: 30, proofRetryMs: 50, proofChecks: 3 }) : Promise.resolve();
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
const originalExitCode = process.exitCode;
afterEach(() => {
  vi.restoreAllMocks(); vi.unstubAllEnvs(); vi.useRealTimers(); process.exitCode = originalExitCode;
  mocks.spawn.mockReset(); mocks.alive.clear(); mocks.births.clear(); mocks.lockHook = undefined; mocks.sessionEmpty = true;
});

const clock = () => {
  // MeshStore's real setImmediate durability barrier must still finish when an exiting supervisor awaits it.
  vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout", "setInterval", "clearInterval"] });
  vi.setSystemTime(new Date("2026-01-01T00:00:00Z"));
};
const ALARM_TOPIC = "fabric.alarm.resident-unhosted";
const ALARM_TO = "session:11111111-2222-4333-8444-555555555555";
const LEAD_SESSION = "session:99999999-8888-4777-8666-555555555555";
const alarmClock = () => {
  clock(); vi.stubEnv("PI_FABRIC_MESH_ALARM_TO", ALARM_TO); vi.stubEnv("SMARTY_LEAD_SESSION", LEAD_SESSION);
  return vi.spyOn(console, "warn").mockImplementation(() => {});
};
const alarms = (f: ReturnType<typeof fixture>) => {
  const mesh = new MeshStore(f.config.meshRoot, 64 * 1024, 100, { stateBackend: "file", canWakeResidents: () => false });
  try { return mesh.read({ topic: ALARM_TOPIC }); } finally { mesh.closeState(); }
};
const outage = (f: ReturnType<typeof fixture>) => JSON.parse(fs.readFileSync(path.join(f.root, "resident-unhosted.json"), "utf8")) as {
  id: string; since: number; absentSince?: number; reason: string; reportedReason?: string; recovered?: true;
};
const alarmHostId = (rootId: string) => `resident:${createHash("sha256").update(rootId).digest("hex").slice(0, 24)}`;
const refuseWake = (f: ReturnType<typeof fixture>) => {
  fs.writeFileSync(path.join(f.root, "wake-routes.json"), JSON.stringify({ format: 1, rootId: f.config.rootId,
    hostId: "fixture-host", configJson: canonicalResidentWakeConfig(f.config), actors: [{ id: "actor-0", name: "actor", topics: [] }] }));
  fs.writeFileSync(path.join(f.root, "config.json"), JSON.stringify({ ...f.config, piBinary: "refused-config" }));
  fs.writeFileSync(path.join(f.root, "wake-failure.json"), JSON.stringify({ format: 1,
    delivery: { id: "failed-delivery" }, error: "restart snapshot mismatch", failedAt: Date.now() }));
};
const settleAlarms = async () => {
  await vi.dynamicImportSettled();
  await new Promise<void>(resolve => setImmediate(resolve));
};
const closeSuccessor = async (f: ReturnType<typeof fixture>, controller: AbortController, done: Promise<void> | undefined) => {
  for (const child of f.children) {
    clearInterval(child.timer);
    child.kill = (signal: string) => { child.emit("exit", null, signal); return true; };
  }
  controller.abort(); await vi.advanceTimersByTimeAsync(100); await done;
};

// Keep: #10068's native watchdog/custody failure cannot be reached cheaply by a live user flow.
// The child and time are injected; supervision, registry reads, alarm routing and MeshStore publication are real.
describe.skipIf(process.platform === "win32")("launcher resident-unhosted alarm", () => {
  it("publishes custody once at exactly five childless minutes, never at 299999ms or again unchanged", async () => {
    alarmClock(); const timeouts = vi.spyOn(globalThis, "setTimeout"); const wake = vi.spyOn(residentWake, "wakeResidentActors");
    const f = fixture({}, true, false, 46);
    try {
      fs.mkdirSync(path.join(f.config.meshRoot, "residency"), { recursive: true });
      // Duplicate scope rows count once; foreign, session-resident and stopped rows do not need this host.
      const sessionRoot = path.join(f.config.actorRoot, f.config.sessionId); fs.mkdirSync(sessionRoot);
      fs.writeFileSync(path.join(sessionRoot, "actors.json"), JSON.stringify({ format: 1, actors: [
        { id: "actor-0", rootId: f.config.rootId, residency: "durable", status: "waiting" },
        { id: "foreign", rootId: "another-root", residency: "durable" },
        { id: "session-only", rootId: f.config.rootId, residency: "session" },
        { id: "stopped", rootId: f.config.rootId, residency: "durable", status: "stopped" },
      ] }));
      await vi.advanceTimersByTimeAsync(500);
      expect(f.children[0]!.signals).toEqual(["SIGUSR2", "SIGTERM", "SIGKILL"]);
      expect(f.events().filter(row => row.event === "watchdog-giving-up")).toHaveLength(1);
      const since = outage(f).since;
      expect(since).toBe(f.children[0]!.signalTimes[2]); // Independent native SIGKILL/exit timestamp, not the tracker's own clock.
      // No short-lived contender here: its custody refusal must alarm immediately, unlike this live supervisor.
      expect(f.children).toHaveLength(1);
      let returned = false; void f.done.then(() => { returned = true; });
      const deadlines = timeouts.mock.calls.flatMap((args, index) => args[1] === 300_000 ? [timeouts.mock.results[index]!.value] : []);
      expect(deadlines).toHaveLength(1); expect(deadlines[0].hasRef()).toBe(true);
      await vi.advanceTimersByTimeAsync(since + 299_999 - Date.now());
      expect(alarms(f)).toEqual([]);
      await vi.advanceTimersByTimeAsync(1); await settleAlarms();
      expect(alarms(f)).toEqual([expect.objectContaining({ topic: ALARM_TOPIC, kind: "resident-unhosted-outage", to: f.config.rootId,
        createdAt: since + 300_000, from: expect.objectContaining({ id: alarmHostId(f.config.rootId) }),
        data: expect.objectContaining({ root: f.config.rootId, residency: f.root, actorCount: 46, reason: "custody", since, state: "outage",
          hostId: alarmHostId(f.config.rootId), episodeId: expect.any(String), advisory: true }) })]);
      // hostId is deterministic diagnostic correlation, not authenticated sender authority.
      expect(alarms(f)[0]!.from).not.toHaveProperty("verified");
      expect(returned).toBe(false);
      await vi.advanceTimersByTimeAsync(300_000); await settleAlarms();
      await supervise(path.join(f.root, "config.json")); await settleAlarms();
      expect(alarms(f)).toHaveLength(1); expect(f.children).toHaveLength(1);
      expect(wake).not.toHaveBeenCalled(); // Even with mesh/residency present, alarm publication cannot recursively wake residents.
      expect(f.events().filter(row => row.event === "watchdog-deferred" && row.proofCheck)).toHaveLength(3);
    } finally { await f.close(); }
  }, 20_000);

  it("a short-lived custody refusal alarms before returning, even inside the live watchdog's five-minute window", async () => {
    alarmClock(); const f = fixture({}, true);
    try {
      await vi.advanceTimersByTimeAsync(500);
      const since = outage(f).since;
      expect(Date.now() - since).toBeLessThan(300_000); expect(alarms(f)).toEqual([]);
      await supervise(path.join(f.root, "config.json"));
      expect(alarms(f)).toEqual([expect.objectContaining({ to: f.config.rootId,
        data: expect.objectContaining({ state: "outage", since, reason: "custody" }) })]);
      await supervise(path.join(f.root, "config.json"));
      await vi.advanceTimersByTimeAsync(600_000); await settleAlarms();
      expect(alarms(f)).toHaveLength(1); expect(f.children).toHaveLength(1);
    } finally { await f.close(); }
  }, 20_000);

  it("derives the same episode from the same residency and since, even if its local marker is reconstructed", async () => {
    alarmClock(); const f = fixture({ enabled: false }, false, false, 1, ROOT_SESSION, false);
    try {
      fs.writeFileSync(path.join(f.root, "watchdog-custody.json"), "{}");
      await supervise(path.join(f.root, "config.json"));
      const first = outage(f);
      const firstEvent = alarms(f)[0]!;
      expect(firstEvent.data).toMatchObject({ episodeId: expect.any(String), since: Date.now(), state: "outage" });
      fs.unlinkSync(path.join(f.root, "resident-unhosted.json"));
      await supervise(path.join(f.root, "config.json")); // Same root and timestamp, not a random new episode.
      expect(outage(f)).toMatchObject({ id: first.id, since: first.since });
      expect(alarms(f)).toEqual([firstEvent]);
      expect(f.children).toHaveLength(0);
    } finally { await f.close(); }
  }, 20_000);

  it("recovers a persisted outage once only after the successor child proves token-bound maintenance readiness", async () => {
    alarmClock(); const f = fixture({}, true);
    const successor = new AbortController(); let next: Promise<void> | undefined;
    try {
      await vi.advanceTimersByTimeAsync(500);
      await vi.advanceTimersByTimeAsync(outage(f).since + 300_000 - Date.now()); await settleAlarms();
      f.controller.abort(); await vi.advanceTimersByTimeAsync(100); await f.done;
      // Test-only stand-in for an explicit operator drain. The implementation must never clear custody itself.
      fs.unlinkSync(path.join(f.root, "watchdog-custody.json"));
      f.config.watchdog!.enabled = false;
      fs.writeFileSync(path.join(f.root, "config.json"), JSON.stringify(f.config));
      next = supervise(path.join(f.root, "config.json"), { signal: successor.signal });
      await vi.advanceTimersByTimeAsync(50);
      const child = f.children[1]!;
      expect(alarms(f).map(row => (row.data as { state: string }).state)).toEqual(["outage"]);
      child.emit("message", { event: "resident-ready", root: f.root, token: "wrong-generation" });
      child.ready("wrong-maintenance-token"); await settleAlarms();
      expect(alarms(f)).toHaveLength(1);
      const ownerFile = path.join(f.root, "owner.json");
      const legacyOwner = JSON.parse(fs.readFileSync(ownerFile, "utf8"));
      delete legacyOwner.maintenanceReady;
      fs.writeFileSync(ownerFile, JSON.stringify(legacyOwner));
      const ready = () => child.emit("message", { event: "resident-ready", root: f.root, token: legacyOwner.token });
      const maintenanceFile = path.join(f.root, "maintenance-ready.json");
      fs.writeFileSync(maintenanceFile, JSON.stringify({ token: legacyOwner.token }));
      expect(residentWake.residentStartupReady(f.root)).toBe(true); // The separate legacy startup contract remains permissive.
      ready(); await settleAlarms(); expect(alarms(f)).toHaveLength(1); // Copied exact token + missing flag is NOT recovery.
      fs.writeFileSync(ownerFile, JSON.stringify({ ...legacyOwner, maintenanceReady: 0 }));
      expect(residentWake.residentStartupReady(f.root)).toBe(true);
      ready(); await settleAlarms(); expect(alarms(f)).toHaveLength(1); // Nor is an explicit zero flag.
      fs.writeFileSync(ownerFile, JSON.stringify({ ...legacyOwner, maintenanceReady: 1 }));
      fs.unlinkSync(maintenanceFile);
      ready(); await settleAlarms(); expect(alarms(f)).toHaveLength(1);
      fs.writeFileSync(maintenanceFile, JSON.stringify({ token: "wrong-maintenance-token" }));
      ready(); await settleAlarms(); expect(alarms(f)).toHaveLength(1);
      fs.writeFileSync(maintenanceFile, JSON.stringify({ token: legacyOwner.token }));
      ready(); await settleAlarms();
      expect(alarms(f).map(row => (row.data as { state: string }).state)).toEqual(["outage", "recovered"]);
      const firstEpisode = (alarms(f)[0]!.data as { episodeId: string }).episodeId;
      expect(firstEpisode).toEqual(expect.any(String));
      expect(alarms(f)[1]!).toMatchObject({ to: f.config.rootId, from: { id: alarmHostId(f.config.rootId) },
        data: { root: f.config.rootId, residency: f.root, actorCount: 1, hostId: alarmHostId(f.config.rootId), episodeId: firstEpisode, advisory: true,
          reason: "custody", since: (alarms(f)[0]!.data as { since: number }).since, state: "recovered" } });
      child.ready(); ready(); await vi.advanceTimersByTimeAsync(300_000); await settleAlarms();
      expect(alarms(f)).toHaveLength(2); expect(outage(f).recovered).toBe(true);
      child.exit(); await vi.advanceTimersByTimeAsync(100); await next;
      const newOutage = alarms(f)[2]!;
      expect(newOutage).toMatchObject({ kind: "resident-unhosted-outage", data: { state: "outage", reason: "child-exit" } });
      expect((newOutage.data as { episodeId: string }).episodeId).not.toBe(firstEpisode);
      expect(alarms(f)).toHaveLength(3);
      // A late recovery advisory for an old episode cannot clear a newer outage or authorize a restart.
      const current = outage(f);
      const mesh = new MeshStore(f.config.meshRoot, 64 * 1024, 100, { stateBackend: "file", canWakeResidents: () => false });
      try {
        await mesh.publish({ topic: ALARM_TOPIC, kind: "resident-unhosted-recovered", to: f.config.rootId,
          from: { id: alarmHostId(f.config.rootId), name: "late diagnostic", kind: "agent" },
          data: { ...(alarms(f)[1]!.data as Record<string, unknown>) } });
      } finally { mesh.closeState(); }
      ready(); await vi.advanceTimersByTimeAsync(100); await settleAlarms();
      expect(outage(f)).toEqual(current); expect(outage(f).recovered).toBeUndefined();
      expect(alarms(f)).toHaveLength(4); // Three lifecycle diagnostics plus the injected stale advisory, no new recovery.
      expect(f.children).toHaveLength(2);
    } finally {
      await closeSuccessor(f, successor, next); await f.close();
    }
  }, 20_000);

  it.each(["empty", "stopped-only"])("never alarms for zero effective actors (%s), including watchdog give-up", async mode => {
    alarmClock(); const f = fixture({}, true, false, 0);
    try {
      if (mode === "stopped-only") fs.writeFileSync(path.join(f.config.actorRoot, "actors.json"), JSON.stringify({ format: 1, actors: [
        { id: "stopped", rootId: f.config.rootId, residency: "durable", status: "stopped" },
      ] }));
      await vi.advanceTimersByTimeAsync(600_500); await settleAlarms();
      expect(f.children).toHaveLength(1);
      expect(f.events().some(row => row.event === "watchdog-giving-up")).toBe(true);
      expect(alarms(f)).toEqual([]);
      expect(fs.existsSync(path.join(f.root, "resident-unhosted.json"))).toBe(false);
    } finally { await f.close(); }
  }, 20_000);

  it("alarms on a short nonzero child exit immediately, but a merely spawned successor is not recovered", async () => {
    alarmClock(); const f = fixture({ enabled: false });
    const successor = new AbortController(); let next: Promise<void> | undefined;
    try {
      await vi.advanceTimersByTimeAsync(1); f.children[0]!.exit();
      await vi.advanceTimersByTimeAsync(100); await f.done;
      expect(outage(f).reason).toBe("child-exit"); expect(alarms(f)).toHaveLength(1);
      await vi.advanceTimersByTimeAsync(60_000);
      next = supervise(path.join(f.root, "config.json"), { signal: successor.signal });
      await vi.advanceTimersByTimeAsync(300_001); await settleAlarms();
      expect(outage(f).recovered).toBeUndefined(); expect(alarms(f)).toHaveLength(1);
      f.children[1]!.ready(); await settleAlarms();
      expect(outage(f).recovered).toBe(true);
      expect(alarms(f).map(row => (row.data as { state: string }).state)).toEqual(["outage", "recovered"]);
    } finally {
      await closeSuccessor(f, successor, next); await f.close();
    }
  }, 20_000);

  it("a reason change after reporting is still the same outage episode, not a second alarm or respawn", async () => {
    alarmClock(); const f = fixture({ enabled: false });
    try {
      await vi.advanceTimersByTimeAsync(1); f.children[0]!.exit();
      await vi.advanceTimersByTimeAsync(100); await f.done;
      const first = alarms(f)[0]!;
      const since = outage(f).since;
      expect(first).toMatchObject({ to: f.config.rootId, data: { since, state: "outage", reason: "child-exit" } });
      refuseWake(f);
      await expect(supervise(path.join(f.root, "config.json"), { wakeOnly: true })).rejects.toThrow("wake config differs");
      expect(outage(f)).toMatchObject({ since, reason: "wake-refused" });
      expect(alarms(f)).toEqual([first]);
      await vi.advanceTimersByTimeAsync(600_000); await settleAlarms();
      expect(alarms(f)).toEqual([first]); expect(f.children).toHaveLength(1);
    } finally { await f.close(); }
  }, 20_000);

  it.each(["child-exit", "custody", "wake-refused"] as const)("awaits immediate %s publication before supervise returns or throws", async reason => {
    alarmClock(); const f = fixture({ enabled: false }, false, false, 1, ROOT_SESSION, reason === "child-exit");
    let release!: () => void;
    const held = new Promise<void>(resolve => { release = resolve; });
    const original = MeshStore.prototype.publish;
    const publish = vi.spyOn(MeshStore.prototype, "publish").mockImplementation(async function (this: MeshStore, input) {
      await held; return original.call(this, input);
    });
    let observed: Promise<unknown> | undefined;
    try {
      let operation: Promise<void>;
      if (reason === "child-exit") {
        await vi.advanceTimersByTimeAsync(1); f.children[0]!.exit(); operation = f.done;
      } else if (reason === "custody") {
        fs.writeFileSync(path.join(f.root, "watchdog-custody.json"), "{}");
        operation = supervise(path.join(f.root, "config.json"));
      } else {
        refuseWake(f); operation = supervise(path.join(f.root, "config.json"), { wakeOnly: true });
      }
      let returned = false;
      observed = operation.then(() => { returned = true; }, error => { returned = true; return error; });
      await vi.advanceTimersByTimeAsync(100); await settleAlarms();
      expect(publish).toHaveBeenCalledOnce(); expect(returned).toBe(false); expect(alarms(f)).toEqual([]);
      release(); await vi.advanceTimersByTimeAsync(100);
      const error = await observed;
      if (reason === "wake-refused") expect(error).toBeInstanceOf(Error);
      else expect(error).toBeUndefined();
      expect(alarms(f)).toEqual([expect.objectContaining({ to: f.config.rootId,
        data: expect.objectContaining({ reason, state: "outage", root: f.config.rootId }) })]);
      expect(f.children).toHaveLength(reason === "child-exit" ? 1 : 0);
    } finally {
      release(); await vi.advanceTimersByTimeAsync(100); await observed; await f.close();
    }
  }, 20_000);

  it("reports a refused MeshStore publication without inventing delivery or retrying unchanged, and closes its store", async () => {
    const warn = alarmClock(); const f = fixture({}, true);
    const publish = vi.spyOn(MeshStore.prototype, "publish").mockRejectedValueOnce(new Error("injected alarm sink failure"));
    const close = vi.spyOn(MeshStore.prototype, "closeState");
    try {
      await vi.advanceTimersByTimeAsync(500);
      await vi.advanceTimersByTimeAsync(outage(f).since + 300_000 - Date.now()); await settleAlarms();
      expect(publish).toHaveBeenCalledOnce(); expect(close).toHaveBeenCalledOnce();
      expect(warn.mock.calls.some(([line]) => String(line).includes("injected alarm sink failure"))).toBe(true);
      expect(alarms(f)).toEqual([]);
      publish.mockRestore(); await vi.advanceTimersByTimeAsync(300_000); await settleAlarms();
      expect(alarms(f)).toEqual([]); // Persisted attempt is not a delivery receipt; stderr is the fallback.
    } finally { await f.close(); }
  }, 20_000);

  it.each([
    { configured: ALARM_TO, lead: LEAD_SESSION, expected: ALARM_TO },
    { configured: "", lead: LEAD_SESSION, expected: LEAD_SESSION },
  ])("preserves generic mesh-outage env routing ($configured, $lead)", async ({ configured, lead, expected }) => {
    alarmClock(); vi.stubEnv("PI_FABRIC_MESH_ALARM_TO", configured); vi.stubEnv("SMARTY_LEAD_SESSION", lead);
    const f = fixture({ enabled: false }, false, false, 1, ROOT_SESSION, false);
    const mesh = new MeshStore(f.config.meshRoot, 64 * 1024, 100, { stateBackend: "file", canWakeResidents: () => false });
    try {
      const sink = meshOutageAlarmSink(() => ({ mesh,
        from: { id: alarmHostId(f.config.rootId), name: "fixture mesh health", kind: "agent" } }));
      await sink({ label: "fixture mesh health", state: "outage", sinceMs: Date.now(), durationMs: 300_000 });
      expect(mesh.read({ topic: "fabric.alarm.mesh-outage" })).toEqual([expect.objectContaining({ to: expected, kind: "mesh-outage" })]);
      expect(alarms(f)).toEqual([]);
    } finally { mesh.closeState(); await f.close(); }
  }, 20_000);

  it.each([
    { configured: "", lead: "" },
    { configured: "", lead: ALARM_TO },
    { configured: ALARM_TO, lead: LEAD_SESSION },
    { configured: "org", lead: LEAD_SESSION },
  ])("always routes to its own root, ignoring both ambient targets ($configured, $lead)", async ({ configured, lead }) => {
    alarmClock(); vi.stubEnv("PI_FABRIC_MESH_ALARM_TO", configured); vi.stubEnv("SMARTY_LEAD_SESSION", lead);
    const f = fixture({}, true);
    try {
      await vi.advanceTimersByTimeAsync(500);
      await vi.advanceTimersByTimeAsync(outage(f).since + 300_000 - Date.now()); await settleAlarms();
      expect(alarms(f)).toEqual([expect.objectContaining({ to: f.config.rootId,
        data: expect.objectContaining({ root: f.config.rootId, state: "outage" }) })]);
      expect(process.env.PI_FABRIC_MESH_ALARM_TO).toBe(configured);
      expect(process.env.SMARTY_LEAD_SESSION).toBe(lead); // Resident routing never mutates generic mesh-outage routing.
    } finally { await f.close(); }
  }, 20_000);

  it("leaves a structured journal fallback when its own root is not a canonical session, ignoring valid ambient targets", async () => {
    const warn = alarmClock();
    const f = fixture({}, true, false, 1, "fixture-root"); // Explicitly invalid own root: ambient sessions must not rescue routing.
    const publish = vi.spyOn(MeshStore.prototype, "publish");
    try {
      await vi.advanceTimersByTimeAsync(500);
      await vi.advanceTimersByTimeAsync(outage(f).since + 300_000 - Date.now()); await settleAlarms();
      expect(publish).not.toHaveBeenCalled(); expect(alarms(f)).toEqual([]);
      expect(warn.mock.calls.some(([line]) => String(line).startsWith("fabric.alarm.mesh-outage {") && String(line).includes(ALARM_TOPIC))).toBe(true);
      expect(warn.mock.calls.some(([line]) => String(line).includes(ALARM_TOPIC) && String(line).includes("session"))).toBe(true);
      expect(outage(f)).toMatchObject({ reason: "custody", since: expect.any(Number) });
    } finally { await f.close(); }
  }, 20_000);
});

describe("launcher intentional idle exit", () => {
  it("makes no late diagnostic write even when the host retires before the first owner sample", async () => {
    alarmClock();
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
      expect(fs.existsSync(path.join(f.root, "resident-unhosted.json"))).toBe(false);
      await vi.advanceTimersByTimeAsync(300_000); await settleAlarms();
      expect(fs.existsSync(path.join(f.root, "resident-unhosted.json"))).toBe(false);
      expect(alarms(f)).toEqual([]);
    } finally { await f.close(); }
  }, 20_000);
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
