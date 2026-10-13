import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { kernelFenceAvailable } from "../src/residency/file-lock.js";
import { spawnDetached } from "../src/agents/transports/process-utils.js";

vi.mock("../src/residency/file-lock.js", async (original) => ({
  ...await original<typeof import("../src/residency/file-lock.js")>(), kernelFenceAvailable: vi.fn(() => true),
}));
// Owner liveness is the only process probe the client makes: per-pid, driven by the test.
const dead = vi.hoisted(() => new Set<number>());
vi.mock("../src/residency/process-identity.js", async (original) => ({
  ...await original<typeof import("../src/residency/process-identity.js")>(),
  residentProcessAlive: vi.fn((pid: number) => !dead.has(pid)),
}));
// The real spawnDetached API handle shape; only the OS spawn is replaced.
vi.mock("../src/agents/transports/process-utils.js", async (original) => ({
  ...await original<typeof import("../src/agents/transports/process-utils.js")>(), spawnDetached: vi.fn(),
}));
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import { ResidencyClient } from "../src/residency/client.js";
import { RESIDENT_HOST_FORMAT, residentRoot, type ResidentHostConfig, type ResidentHostOwner } from "../src/residency/protocol.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import type { FabricParticipantSource } from "../src/topology/types.js";

const realSetTimeout = globalThis.setTimeout; // captured before any fake timers
const realSleep = (ms: number) => new Promise<void>((resolve) => realSetTimeout(resolve, ms));
const realRmSync = fs.rmSync;

afterEach(() => {
  vi.mocked(spawnDetached).mockReset(); dead.clear(); vi.restoreAllMocks(); vi.useRealTimers();
});

type Layout = "custom" | "nested-parent-exists" | "nested-parent-later";

const fixture = (layout: Layout, options: { oldBehaviorControl?: boolean } = {}) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-root-recovery-"));
  const meshRoot = path.join(root, "mesh");
  const rootId = "session:root-recovery";
  const config = (): ResidentHostConfig => ({
    format: RESIDENT_HOST_FORMAT, rootId, sessionId: "root-recovery", cwd: root, projectRoot: root,
    meshRoot, actorRoot: path.join(root, "actors"), sessionActorRoot: path.join(root, "session-actors"),
    residencyRoot: layout === "custom" ? path.join(root, "custom", "resident") : residentRoot(meshRoot, rootId),
    fullCodeMode: true, agents: DEFAULT_FABRIC_CONFIG.agents,
    mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 50 }, retention: DEFAULT_FABRIC_CONFIG.retention,
    workerPath: "worker.js", fabricExtensionPath: "index.js", piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
  });
  const residencyRoot = config().residencyRoot;
  fs.mkdirSync(meshRoot, { recursive: true });
  if (layout === "nested-parent-exists") fs.mkdirSync(path.dirname(residencyRoot), { recursive: true });
  if (options.oldBehaviorControl) {
    // Control: restore the pre-fix observation (creation of a root ancestor component is ignored) by
    // filtering only those notifications in the fixture. No production code is changed.
    const watch = fs.watch.bind(fs) as (...args: unknown[]) => fs.FSWatcher;
    const ignored = new Map<string, string>();
    for (let child = residencyRoot, parent = path.dirname(child); parent !== child; child = parent, parent = path.dirname(parent)) {
      ignored.set(parent, path.basename(child));
    }
    vi.spyOn(fs, "watch").mockImplementation(((directory: string, listener: (event: string, name: string | null) => void) =>
      watch(directory, (event: string, name: string | null) => {
        if (name !== null && ignored.get(directory) === name) return;
        listener(event, name);
      })) as unknown as typeof fs.watch);
  }
  // #startHost clears error.json/idle-exit.json: record those product deletions, never perform them.
  const deletions: string[] = [];
  vi.spyOn(fs, "rmSync").mockImplementation(((target: fs.PathLike, opts?: fs.RmOptions) => {
    const file = String(target);
    if (path.dirname(file) === residencyRoot && (path.basename(file) === "error.json" || path.basename(file) === "idle-exit.json")) {
      deletions.push(path.basename(file));
      return;
    }
    realRmSync(target, opts);
  }) as typeof fs.rmSync);

  const actorId = "a".repeat(32);
  const participants = {
    self: () => ({ kind: "actor" }),
    get: (id: string) => id === actorId ? { kind: "actor", ownerHostId: clients.b.hostId, residency: "durable", stale: false } : undefined,
  } as unknown as FabricParticipantSource;
  const client = () => {
    const cfg = config();
    return new ResidencyClient({ config: cfg, mesh: new MeshStore(cfg.meshRoot, cfg.mesh.maxEventBytes, cfg.mesh.maxReadEvents),
      participants, mainAgent: { local: true } as FabricMainAgentTarget, hostPath: path.join(root, "launcher.js") });
  };
  const clients = { a: client(), b: client() };

  // Fake host: spawnDetached publishes the owner; serve() answers createActor by persisting the
  // registry #durableWork reads, then the response file. The launcher handle's `closed` never fires
  // for A (B's launcher is not A's child).
  let nextPid = 900_000;
  const launches: Array<{ pid: number; exit: () => void }> = [];
  vi.mocked(spawnDetached).mockImplementation(async (_path, args) => {
    const pid = ++nextPid;
    let exit!: () => void;
    const closed = new Promise<void>((resolve) => { exit = resolve; });
    launches.push({ pid, exit });
    const owner: ResidentHostOwner = { format: RESIDENT_HOST_FORMAT, hostId: clients.a.hostId, pid, token: `owner-${pid}`,
      startedAt: Date.now(), readyAt: Date.now(), launchToken: args[args.indexOf("--launch-token") + 1]!, requestFence: 1,
      commands: ["createActor"] };
    fs.writeFileSync(path.join(residencyRoot, "owner.json"), JSON.stringify(owner));
    return { pid, closed, stop: vi.fn(async () => undefined), isAlive: vi.fn(async () => !dead.has(pid)),
      lostContact: () => undefined, waitForClose: () => closed } as unknown as Awaited<ReturnType<typeof spawnDetached>>;
  });
  const serve = (): void => {
    const requests = path.join(residencyRoot, "requests");
    let names: string[];
    try { names = fs.readdirSync(requests); } catch { return; }
    for (const name of names) {
      const command = JSON.parse(fs.readFileSync(path.join(requests, name), "utf8")) as { requestId: string; operation: string };
      if (command.operation !== "createActor") continue;
      realRmSync(path.join(requests, name));
      const actor = { id: actorId, rootId, residency: "durable", status: "idle" };
      fs.mkdirSync(config().actorRoot, { recursive: true });
      fs.writeFileSync(path.join(config().actorRoot, "actors.json"), JSON.stringify({ actors: [actor] }));
      fs.mkdirSync(path.join(residencyRoot, "responses"), { recursive: true });
      fs.writeFileSync(path.join(residencyRoot, "responses", `${command.requestId}.json`), JSON.stringify({
        format: RESIDENT_HOST_FORMAT, requestId: command.requestId, ok: true, actor, completedAt: Date.now() }));
    }
  };
  /** Advance the fake clock in small steps while real filesystem notifications are delivered. */
  const pump = async (ms: number, step = 100): Promise<void> => {
    for (let elapsed = 0; elapsed < ms; elapsed += step) {
      serve();
      await vi.advanceTimersByTimeAsync(Math.min(step, ms - elapsed));
      await realSleep(2);
    }
  };
  /** Real-time wait for filesystem notifications, without moving the fake clock. */
  const settle = async (): Promise<void> => { for (let i = 0; i < 25; i++) { serve(); await realSleep(8); await vi.advanceTimersByTimeAsync(0); } };
  return { root, residencyRoot, clients, launches, deletions, pump, settle, actorId,
    cleanup: async () => { await clients.a.close(); await clients.b.close(); vi.restoreAllMocks(); realRmSync(root, { recursive: true, force: true }); } };
};

const fakeClock = () => vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "setInterval", "clearInterval", "Date"] });

describe("resident client: root materialized by a sibling client after an idle start", () => {
  it.each(["custom", "nested-parent-exists", "nested-parent-later"] as const)(
    "%s: A starts idle before the root exists, B.createActor durable, B's owner dies silently -> A recovers within 60 s",
    async (layout) => {
      const f = fixture(layout);
      const { clients, residencyRoot } = f;
      try {
        expect(fs.existsSync(residencyRoot)).toBe(false);
        fakeClock();
        clients.a.start();
        await f.settle();
        // Idle: no deadline, no pending timeout.
        expect(vi.getTimerCount()).toBe(0);
        await f.pump(600_000, 60_000);
        expect(vi.getTimerCount()).toBe(0);
        expect(spawnDetached).not.toHaveBeenCalled();

        // Sibling B (not started) uses the public durable path, materializing the root.
        const created = clients.b.createActor({ name: "durable", residency: "durable" } as never);
        while (vi.mocked(spawnDetached).mock.calls.length === 0 || !fs.existsSync(path.join(f.root, "actors", "actors.json"))) await f.pump(100);
        await expect(created).resolves.toMatchObject({ id: f.actorId });
        expect(fs.existsSync(residencyRoot)).toBe(true);
        expect(f.deletions).toEqual(expect.arrayContaining(["error.json", "idle-exit.json"]));
        await f.settle();
        expect(spawnDetached).toHaveBeenCalledOnce();
        // A adopted B's owner: one 60 s deadline.
        expect(vi.getTimerCount()).toBe(1);

        dead.add(f.launches[0]!.pid); // crash without touching owner.json
        await f.settle();
        expect(spawnDetached).toHaveBeenCalledOnce();
        await f.pump(60_000, 1_000);
        await f.settle();
        expect(spawnDetached).toHaveBeenCalledTimes(2);
        // A's own launcher is now observed by its close event: no timer.
        expect(vi.getTimerCount()).toBe(0);
      } finally { await f.cleanup(); }
    }, 30_000);

  it.each(["custom", "nested-parent-later"] as const)("%s control (pre-fix observation): A never recovers", async (layout) => {
    const f = fixture(layout, { oldBehaviorControl: true });
    const { clients } = f;
    try {
      fakeClock();
      clients.a.start();
      await f.settle();
      const created = clients.b.createActor({ name: "durable", residency: "durable" } as never);
      while (vi.mocked(spawnDetached).mock.calls.length === 0 || !fs.existsSync(path.join(f.root, "actors", "actors.json"))) await f.pump(100);
      await created;
      await f.settle();
      expect(vi.getTimerCount()).toBe(0);
      dead.add(f.launches[0]!.pid);
      await f.pump(600_000, 10_000);
      await f.settle();
      expect(spawnDetached).toHaveBeenCalledOnce();
    } finally { await f.cleanup(); }
  }, 30_000);

  it("same-client ensureHost path from an absent root: own launcher close wakes recovery with no timer", async () => {
    const f = fixture("nested-parent-later");
    const { clients } = f;
    try {
      fakeClock();
      clients.a.start();
      await f.settle();
      const created = clients.a.createActor({ name: "durable", residency: "durable" } as never);
      while (vi.mocked(spawnDetached).mock.calls.length === 0 || !fs.existsSync(path.join(f.root, "actors", "actors.json"))) await f.pump(100);
      await created;
      await f.settle();
      expect(vi.getTimerCount()).toBe(0);
      await f.pump(600_000, 60_000);
      expect(spawnDetached).toHaveBeenCalledOnce();
      dead.add(f.launches[0]!.pid);
      f.launches[0]!.exit();
      await f.settle();
      expect(spawnDetached).toHaveBeenCalledTimes(2);
      expect(vi.getTimerCount()).toBe(0);
    } finally { await f.cleanup(); }
  }, 30_000);

  it("intentional idle exit after sibling materialization is not auto-started", async () => {
    const f = fixture("custom");
    const { clients, residencyRoot } = f;
    try {
      fakeClock();
      clients.a.start();
      await f.settle();
      const created = clients.b.createActor({ name: "durable", residency: "durable" } as never);
      while (vi.mocked(spawnDetached).mock.calls.length === 0 || !fs.existsSync(path.join(f.root, "actors", "actors.json"))) await f.pump(100);
      await created;
      await f.settle();
      const pid = f.launches[0]!.pid;
      fs.writeFileSync(path.join(residencyRoot, "idle-exit.json"), JSON.stringify({ format: 1, reason: "root-dead-idle",
        rootId: "session:root-recovery", pid, token: `owner-${pid}`, at: Date.now(), idleMs: 0 }));
      dead.add(pid);
      await f.settle();
      await f.pump(600_000, 10_000);
      await f.settle();
      expect(spawnDetached).toHaveBeenCalledOnce();
      expect(vi.getTimerCount()).toBe(0);
    } finally { await f.cleanup(); }
  }, 30_000);
});
