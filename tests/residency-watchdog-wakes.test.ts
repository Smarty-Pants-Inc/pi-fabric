import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { kernelFenceAvailable } from "../src/residency/file-lock.js";
import { spawnDetached } from "../src/agents/transports/process-utils.js";

vi.mock("../src/residency/file-lock.js", async (original) => ({
  ...await original<typeof import("../src/residency/file-lock.js")>(), kernelFenceAvailable: vi.fn(() => true),
}));
// Owner liveness is the only process probe the client makes; tests drive it explicitly.
const liveness = vi.hoisted(() => ({ alive: true }));
vi.mock("../src/residency/process-identity.js", async (original) => ({
  ...await original<typeof import("../src/residency/process-identity.js")>(),
  residentProcessAlive: vi.fn(() => liveness.alive),
}));
// The real spawnDetached API handle shape; only the OS spawn is replaced.
vi.mock("../src/agents/transports/process-utils.js", async (original) => ({
  ...await original<typeof import("../src/agents/transports/process-utils.js")>(), spawnDetached: vi.fn(),
}));
afterEach(() => {
  vi.mocked(kernelFenceAvailable).mockReset(); vi.mocked(kernelFenceAvailable).mockReturnValue(true);
  vi.mocked(spawnDetached).mockReset(); liveness.alive = true;
});
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import { ResidencyClient } from "../src/residency/client.js";
import { RESIDENT_HOST_FORMAT, type ResidentHostConfig, type ResidentHostOwner } from "../src/residency/protocol.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import type { FabricParticipantSource } from "../src/topology/types.js";

const fixture = (durable: boolean) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-watchdog-wakes-"));
  const config: ResidentHostConfig = {
    format: RESIDENT_HOST_FORMAT, rootId: "session:wakes", sessionId: "wakes", cwd: root, projectRoot: root,
    meshRoot: path.join(root, "mesh"), actorRoot: path.join(root, "actors"), sessionActorRoot: path.join(root, "session-actors"),
    residencyRoot: path.join(root, "resident"), fullCodeMode: true, agents: DEFAULT_FABRIC_CONFIG.agents,
    mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 50 }, retention: DEFAULT_FABRIC_CONFIG.retention,
    workerPath: "worker.js", fabricExtensionPath: "index.js", piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
  };
  if (durable) {
    fs.mkdirSync(config.actorRoot, { recursive: true });
    fs.writeFileSync(path.join(config.actorRoot, "actors.json"), JSON.stringify({ actors: [
      { id: "a", rootId: config.rootId, residency: "durable", status: "idle" },
    ] }));
  }
  fs.mkdirSync(config.residencyRoot, { recursive: true });
  const mesh = new MeshStore(config.meshRoot, config.mesh.maxEventBytes, config.mesh.maxReadEvents);
  const client = new ResidencyClient({ config, mesh, participants: {} as FabricParticipantSource,
    mainAgent: { local: true } as FabricMainAgentTarget, hostPath: path.join(root, "launcher.js") });
  const writeOwner = (launchToken?: string) => {
    const owner: ResidentHostOwner = { format: RESIDENT_HOST_FORMAT, hostId: client.hostId, pid: process.pid,
      token: `owner-${launchToken ?? "adopted"}`, startedAt: Date.now(), readyAt: Date.now(),
      ...(launchToken ? { launchToken } : {}) };
    fs.writeFileSync(path.join(config.residencyRoot, "owner.json"), JSON.stringify(owner));
  };
  return { root, client, config, writeOwner, cleanup: () => fs.rmSync(root, { recursive: true, force: true }) };
};

/** A spawnDetached handle whose `closed` is the launcher's actual ChildProcess close event. */
const launcherHandle = () => {
  let exit!: () => void;
  const closed = new Promise<void>((resolve) => { exit = resolve; });
  const handle = { pid: process.pid, closed, stop: vi.fn(async () => undefined), isAlive: vi.fn(async () => true),
    lostContact: () => undefined, waitForClose: () => closed };
  return { handle, exit };
};

describe("resident watchdog event wakes", () => {
  it("idle start arms no recurring deadline", async () => {
    const { client, cleanup } = fixture(false);
    const fence = vi.mocked(kernelFenceAvailable);
    vi.useFakeTimers();
    try {
      client.start();
      await vi.advanceTimersByTimeAsync(0);
      expect(vi.getTimerCount()).toBe(0);
      fence.mockClear();
      await vi.advanceTimersByTimeAsync(600_000);
      expect(fence).not.toHaveBeenCalled();
      expect(vi.getTimerCount()).toBe(0);
    } finally { await client.close(); vi.useRealTimers(); cleanup(); }
  });

  it("stops after the startup check when no kernel fence exists", async () => {
    const { client, cleanup } = fixture(true);
    vi.mocked(kernelFenceAvailable).mockReturnValue(false);
    const start = vi.spyOn(client, "ensureHost").mockResolvedValue({} as Awaited<ReturnType<typeof client.ensureHost>>);
    vi.useFakeTimers();
    try {
      client.start();
      await vi.advanceTimersByTimeAsync(0);
      expect(vi.getTimerCount()).toBe(0);
      vi.mocked(kernelFenceAvailable).mockClear();
      await vi.advanceTimersByTimeAsync(180_000);
      expect(kernelFenceAvailable).not.toHaveBeenCalled();
      expect(start).not.toHaveBeenCalled();
    } finally { await client.close(); vi.useRealTimers(); cleanup(); }
  });

  it("leaves no timers after close and never rearms from late-settled recovery work", async () => {
    const { client, cleanup } = fixture(true);
    let settle!: () => void;
    const start = vi.spyOn(client, "ensureHost").mockImplementation(() =>
      new Promise((resolve) => { settle = () => resolve({} as Awaited<ReturnType<typeof client.ensureHost>>); }));
    vi.useFakeTimers();
    try {
      client.start();
      await vi.advanceTimersByTimeAsync(0);
      expect(start).toHaveBeenCalledOnce();
      // In-flight pass: no deadline is armed until it settles.
      expect(vi.getTimerCount()).toBe(0);
      await client.close();
      settle();
      await vi.advanceTimersByTimeAsync(120_000);
      expect(vi.getTimerCount()).toBe(0);
      expect(start).toHaveBeenCalledOnce();
    } finally { await client.close(); vi.useRealTimers(); cleanup(); }
  });

  it("rearms at the backoff deadline after a failed in-flight start settles", async () => {
    const { client, cleanup } = fixture(true);
    let fail!: () => void;
    const start = vi.spyOn(client, "ensureHost").mockImplementation(() =>
      new Promise((_, reject) => { fail = () => reject(new Error("fixture start failure")); }));
    vi.useFakeTimers();
    try {
      client.start();
      await vi.advanceTimersByTimeAsync(0);
      expect(start).toHaveBeenCalledOnce();
      await vi.advanceTimersByTimeAsync(30_000); // slow start: no duplicate executor
      expect(start).toHaveBeenCalledOnce();
      fail();
      await vi.advanceTimersByTimeAsync(9_999);
      expect(start).toHaveBeenCalledOnce();
      await vi.advanceTimersByTimeAsync(1);
      expect(start).toHaveBeenCalledTimes(2);
    } finally { await client.close(); vi.useRealTimers(); cleanup(); }
  });

  it("adopted live owner: one 60 s unref liveness deadline detects a silent crash", async () => {
    const { client, writeOwner, cleanup } = fixture(true);
    writeOwner();
    const start = vi.spyOn(client, "ensureHost").mockResolvedValue({} as Awaited<ReturnType<typeof client.ensureHost>>);
    vi.useFakeTimers();
    try {
      client.start();
      await vi.advanceTimersByTimeAsync(0);
      expect(start).not.toHaveBeenCalled();
      expect(vi.getTimerCount()).toBe(1);
      liveness.alive = false; // crashed without touching owner.json
      await vi.advanceTimersByTimeAsync(59_999);
      expect(start).not.toHaveBeenCalled();
      await vi.advanceTimersByTimeAsync(1);
      expect(start).toHaveBeenCalledOnce();
    } finally { await client.close(); vi.useRealTimers(); cleanup(); }
  });

  it("own launcher: observes the actual exit event instantly with no watchdog timer; close stops observing", async () => {
    const { client, writeOwner, cleanup } = fixture(true);
    const launchers: Array<ReturnType<typeof launcherHandle>> = [];
    vi.mocked(spawnDetached).mockImplementation(async (_path, args) => {
      const launched = launcherHandle();
      launchers.push(launched);
      liveness.alive = true;
      writeOwner(args[args.indexOf("--launch-token") + 1]);
      return launched.handle as unknown as Awaited<ReturnType<typeof spawnDetached>>;
    });
    vi.useFakeTimers();
    try {
      client.start();
      await vi.advanceTimersByTimeAsync(0);
      expect(spawnDetached).toHaveBeenCalledOnce();
      // Our own child: no liveness deadline at all.
      expect(vi.getTimerCount()).toBe(0);
      await vi.advanceTimersByTimeAsync(600_000);
      expect(spawnDetached).toHaveBeenCalledOnce();
      // Launcher crashes: the ChildProcess close event wakes recovery immediately.
      liveness.alive = false;
      launchers[0]!.exit();
      await vi.advanceTimersByTimeAsync(0);
      expect(spawnDetached).toHaveBeenCalledTimes(2);
      expect(vi.getTimerCount()).toBe(0);
      await client.close();
      liveness.alive = false;
      launchers[1]!.exit();
      await vi.advanceTimersByTimeAsync(600_000);
      expect(spawnDetached).toHaveBeenCalledTimes(2);
      expect(vi.getTimerCount()).toBe(0);
    } finally { await client.close(); vi.useRealTimers(); cleanup(); }
  });
});
