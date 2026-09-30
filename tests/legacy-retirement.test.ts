import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import { ResidencyClient } from "../src/residency/client.js";
import * as retirement from "../src/residency/legacy-retirement.js";
import { residentHostId, type ResidentHostConfig, type ResidentHostOwner } from "../src/residency/protocol.js";
import type { FabricParticipantSource } from "../src/topology/types.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";

vi.mock("node:child_process", async importOriginal => ({
  ...await importOriginal<typeof import("node:child_process")>(),
  execFileSync: vi.fn(() => "100"),
}));

const roots: string[] = [];
afterEach(() => { vi.restoreAllMocks(); roots.splice(0).forEach(root => fs.rmSync(root, { recursive: true, force: true })); });
const fixture = (pid = process.pid) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-retirement-")); roots.push(root);
  const config: ResidentHostConfig = {
    format: 1, rootId: "session:retirement", sessionId: "retirement", cwd: root, projectRoot: root,
    meshRoot: path.join(root, "mesh"), actorRoot: path.join(root, "actors"), residencyRoot: path.join(root, "resident"),
    fullCodeMode: false, agents: DEFAULT_FABRIC_CONFIG.agents, mesh: DEFAULT_FABRIC_CONFIG.mesh,
    retention: DEFAULT_FABRIC_CONFIG.retention, workerPath: "unused", fabricExtensionPath: "new-release",
    piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
  };
  for (const dir of [config.residencyRoot, config.actorRoot, ...["requests", "processing", "agents"].map(dir => path.join(config.residencyRoot, dir))]) fs.mkdirSync(dir, { recursive: true });
  const owner: ResidentHostOwner = { format: 1, hostId: residentHostId(config.rootId), pid, token: "legacy-token", startedAt: Date.now(), readyAt: Date.now() };
  fs.writeFileSync(path.join(config.residencyRoot, "owner.json"), JSON.stringify(owner));
  const participants = { list: () => [] } as unknown as FabricParticipantSource;
  const client = new ResidencyClient({ config, mesh: new MeshStore(config.meshRoot, 65536, 100), participants,
    mainAgent: { local: true } as FabricMainAgentTarget, startupTimeoutMs: 1 });
  return { config, owner, participants, client };
};

describe("legacy retirement fenced idle boundary", () => {
  it.each(["running", "queued", "in-flight", "durable queue", "unknown queue"])("does not cut a %s actor behind an idle participant", async kind => {
    const f = fixture();
    const actor = { id: "actor", rootId: f.config.rootId, residency: "durable", status: kind === "running" || kind === "queued" ? kind : "idle",
      ...(kind === "in-flight" ? { inFlightRun: { id: "run" } } : {}) };
    fs.mkdirSync(path.join(f.config.actorRoot, actor.id));
    fs.writeFileSync(path.join(f.config.actorRoot, "actors.json"), JSON.stringify({ actors: [actor] }));
    if (kind.includes("queue")) fs.writeFileSync(path.join(f.config.actorRoot, actor.id, "queue-legacy.json"), kind === "unknown queue" ? "{" : JSON.stringify({ items: [{ id: "accepted-event" }] }));
    vi.spyOn(f.participants, "list").mockReturnValue([{ id: actor.id, ownerHostId: f.owner.hostId, status: "idle" }] as ReturnType<FabricParticipantSource["list"]>);
    const identity = { pid: f.owner.pid, token: f.owner.token, startTime: "launch-start", commandLine: "pi" };
    vi.spyOn(retirement, "authenticateLegacyOwner").mockReturnValue(identity);
    let stopped = false;
    vi.spyOn(retirement, "legacyProcessStopped").mockImplementation(() => stopped);
    const signals: string[] = [];
    vi.spyOn(retirement, "signalLegacyOwner").mockImplementation((_config, _owner, _identity, signal) => {
      signals.push(signal); stopped = signal === "SIGSTOP"; return true;
    });
    const kill = vi.spyOn(process, "kill").mockReturnValue(true);
    try {
      await expect(f.client.ensureHost()).rejects.toThrow(/draining for release reload/);
      expect(signals.length).toBeGreaterThanOrEqual(2);
      expect(signals.every((signal, index) => signal === (index % 2 === 0 ? "SIGSTOP" : "SIGCONT"))).toBe(true);
      expect(kill.mock.calls.filter(([, signal]) => signal !== 0)).toEqual([]);
      expect(fs.existsSync(path.join(f.config.residencyRoot, "retirement.lock"))).toBe(false);
    } finally { await f.client.close(); fs.rmSync(path.join(f.config.residencyRoot, "owner.json")); }
  });

  it("keeps the fence across the idle check and retires without reopening actor admission", async () => {
    const f = fixture();
    const identity = { pid: f.owner.pid, token: f.owner.token, startTime: "launch-start", commandLine: "pi" };
    vi.spyOn(retirement, "authenticateLegacyOwner").mockReturnValue(identity);
    let stopped = false;
    vi.spyOn(retirement, "legacyProcessStopped").mockImplementation(() => stopped);
    const signals: string[] = [];
    vi.spyOn(retirement, "signalLegacyOwner").mockImplementation((_config, _owner, _identity, signal) => {
      if (signal === "SIGKILL") expect(stopped).toBe(true);
      signals.push(signal); stopped = true; return true;
    });
    const kill = vi.spyOn(process, "kill").mockReturnValue(true);
    try {
      await expect(f.client.ensureHost()).rejects.toThrow(/draining for release reload/);
      expect(signals).toEqual(["SIGSTOP", "SIGKILL"]);
      expect(kill.mock.calls.filter(([, signal]) => signal !== 0)).toEqual([]);
    } finally { await f.client.close(); fs.rmSync(path.join(f.config.residencyRoot, "owner.json")); }
  });

  it("refuses an orphaned owner pointing to an unrelated live process without signalling", async () => {
    const child = spawn(process.execPath, ["-e", "setInterval(() => {}, 1000)"], { stdio: "ignore" });
    const exited = new Promise<void>(resolve => child.once("close", () => resolve()));
    const f = fixture(child.pid!);
    const killProcess = process.kill.bind(process);
    const signals: string[] = [];
    vi.spyOn(process, "kill").mockImplementation((pid, signal) => {
      if (pid === child.pid && signal !== 0) { signals.push(String(signal)); return true; }
      return killProcess(pid, signal);
    });
    try {
      await expect(f.client.ensureHost()).rejects.toThrow(/draining for release reload/);
      expect(signals).toEqual([]);
      expect(child.exitCode).toBeNull();
    } finally { await f.client.close(); vi.restoreAllMocks(); child.kill(); await exited; }
  });
});

// Kernel proc evidence is simulated so every mismatch, including PID reuse, runs on Windows.
describe("legacy signal ownership authentication", () => {
  it.each(["start time", "command line", "owner token", "environment", "missing launch", "parent command"])("refuses %s mismatch immediately before a signal", kind => {
    const f = fixture(987654);
    const now = Date.now(); f.owner.startedAt = now; f.owner.readyAt = now;
    const data = new Map<string, string>([
      [path.join(f.config.residencyRoot, "owner.json"), JSON.stringify(f.owner)],
      [path.join(f.config.residencyRoot, "host.lock"), JSON.stringify({ pid: f.owner.pid, token: f.owner.token })],
      [path.join(f.config.residencyRoot, "launcher.log"), [
        { event: "launcher-started", pid: 1234, at: now - 100, configPath: path.join(f.config.residencyRoot, "config.json") },
        { event: "child-spawned", pid: f.owner.pid, at: now },
      ].map(row => JSON.stringify(row)).join("\n")],
      [`/proc/${f.owner.pid}/stat`, `${f.owner.pid} (pi) S 1234 ${Array(17).fill("0").join(" ")} 1000`],
      [`/proc/${f.owner.pid}/environ`, `PI_FABRIC_RESIDENT_CONFIG=${path.join(f.config.residencyRoot, "config.json")}\0`],
      [`/proc/${f.owner.pid}/cmdline`, "pi\0"],
      ["/proc/1234/cmdline", `/old/residency/launcher.js\0--config\0${path.join(f.config.residencyRoot, "config.json")}\0`],
      ["/proc/uptime", "10 0"],
    ]);
    vi.spyOn(process, "platform", "get").mockReturnValue("linux");
    const read = fs.readFileSync.bind(fs);
    vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, options?: unknown) => {
      const text = data.get(String(file)); return text === undefined ? read(file, options as never) : text;
    }) as typeof fs.readFileSync);
    fs.writeFileSync(path.join(f.config.residencyRoot, "launcher.log"), "trace");
    const kill = vi.spyOn(process, "kill").mockReturnValue(true);
    expect(retirement.authenticateLegacyOwner(f.config, f.owner)).toEqual({ pid: f.owner.pid, token: f.owner.token, startTime: "1000", commandLine: "pi\0" });
    // startTime in ticks is compared against the authenticated snapshot, independent of HZ.
    const identity = { pid: f.owner.pid, token: f.owner.token, startTime: "1000", commandLine: "pi\0" };
    if (kind === "start time") data.set(`/proc/${f.owner.pid}/stat`, data.get(`/proc/${f.owner.pid}/stat`)!.replace(/1000$/, "1"));
    if (kind === "command line") data.set(`/proc/${f.owner.pid}/cmdline`, "unrelated\0");
    if (kind === "owner token") data.set(path.join(f.config.residencyRoot, "host.lock"), JSON.stringify({ pid: f.owner.pid, token: "different" }));
    if (kind === "environment") data.set(`/proc/${f.owner.pid}/environ`, "UNRELATED=1\0");
    if (kind === "missing launch") data.set(path.join(f.config.residencyRoot, "launcher.log"), "{}");
    if (kind === "parent command") data.set("/proc/1234/cmdline", "unrelated\0");
    expect(retirement.signalLegacyOwner(f.config, f.owner, identity, "SIGTERM")).toBe(false);
    expect(kill).not.toHaveBeenCalled();
  });
});
