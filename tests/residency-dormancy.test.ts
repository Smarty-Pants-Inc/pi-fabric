import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { EventEmitter } from "node:events";
import { describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ResidentHost } from "../src/residency/host.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { retainResidentProcessWork } from "../src/residency/process-work.js";
import { ActorChildCompletionStore } from "../src/actors/child-completions.js";
import { ARCHIVE_PENDING_FILE } from "../src/agents/archive-custody.js";
import { MeshStore } from "../src/mesh/store.js";
import { RESIDENT_HOST_FORMAT, residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import { readWakeJson, residentSleepingPath, residentWakeRequestPath, wakeResidentActors } from "../src/residency/wake.js";
import { superviseWake } from "../src/residency/launcher.js";
import { canonicalResidentWakeConfig, routesAt, indexResidentDeliveries, acknowledgeResidentDelivery, residentWakeCapacityAvailable } from "../src/residency/wake-index.js";
import { requestResidentWake, ResidentWakeConfigMismatch } from "../src/residency/wake.js";
import { processStartTime } from "../src/residency/process-identity.js";
import { AgentMessageRouter } from "../src/providers/agents-message-router.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
// Attribute the timer to its direct caller, not an ancestor that causes an existing
// participant publication throttle or other independently owned deadline.
const scheduledByHost = (stack: string): boolean => {
  const frames = stack.split("\n");
  const timer = frames.findIndex(frame => frame.includes("at setTimeout "));
  return timer >= 0 && (frames[timer + 1]?.includes("ResidentHost.") ?? false);
};
const until = async (done: () => boolean, ms = 8_000) => {
  const started = performance.now();
  while (!done() && performance.now() - started < ms) await sleep(20);
  expect(done()).toBe(true);
};
const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-dormant-"));
  const config: ResidentHostConfig = {
    format: RESIDENT_HOST_FORMAT, rootId: "session:dormancy", sessionId: "dormancy", cwd: root, projectRoot: root,
    meshRoot: path.join(root, "mesh"), actorRoot: path.join(root, "actors"),
    residencyRoot: residentRoot(path.join(root, "mesh"), "session:dormancy"), fullCodeMode: true,
    agents: { ...DEFAULT_FABRIC_CONFIG.agents }, mesh: { ...DEFAULT_FABRIC_CONFIG.mesh },
    retention: { ...DEFAULT_FABRIC_CONFIG.retention }, workerPath: path.resolve("dist/worker.js"),
    fabricExtensionPath: path.resolve("dist/index.js"), piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
  };
  fs.mkdirSync(config.residencyRoot, { recursive: true });
  fs.writeFileSync(path.join(config.residencyRoot, "config.json"), JSON.stringify(config));
  fs.writeFileSync(path.join(config.residencyRoot, "wake-routes.json"), JSON.stringify({
    format: 1, rootId: config.rootId, hostId: "fixture", configJson: canonicalResidentWakeConfig(config), actors: [],
  }));
  const idle = vi.fn();
  const host = new ResidentHost(config, idle);
  return { root, config, host, idle };
};
const fillWakeCapacity = (config: ResidentHostConfig): string[] => {
  const residents: string[] = [];
  for (let n = 0; n < 128; n++) {
    const rootId = `session:capacity-${n}`;
    const resident = residentRoot(config.meshRoot, rootId);
    const saved = { ...config, rootId, residencyRoot: resident };
    fs.mkdirSync(resident, { recursive: true });
    fs.writeFileSync(path.join(resident, "config.json"), JSON.stringify(saved));
    fs.writeFileSync(path.join(resident, "wake-routes.json"), JSON.stringify({ format: 1, rootId, hostId: `host:${n}`,
      configJson: canonicalResidentWakeConfig(saved), actors: [{ id: `actor-${n}`, name: `actor-${n}`, topics: ["capacity.delivery"] }] }));
    residents.push(resident);
  }
  return residents;
};

const fakeRun = (host: ResidentHost, consume: (task: string) => Promise<void> = async () => {}) =>
  vi.spyOn(host.agents, "run").mockImplementation(async (request, _signal, onSpawned) => {
    const id = "1".repeat(32);
    onSpawned?.({ id } as never);
    await consume(request.task);
    return { id, status: "completed", text: "done", toolCalls: 0, startedAt: Date.now(), finishedAt: Date.now() } as never;
  });

// In-process host tests still take the real kernel fence on Linux.
describe("resident dormancy (smarty-dev#6782 / #2264)", () => {
  it("preserves ordinary empty-host idle exit without a persisted restart config", async () => {
    const { root, config, host, idle } = fixture();
    fs.rmSync(path.join(config.residencyRoot, "config.json"));
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      await host.start();
      const now = Date.now();
      vi.spyOn(Date, "now").mockImplementation(() => now + 31_000);
      await until(() => idle.mock.calls.length === 1);
      expect(warn).not.toHaveBeenCalled();
    } finally { vi.restoreAllMocks(); await host.close(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it.each(["actorRoot", "sessionActorRoot", "mesh.actorScope", "authority", "unreadable"] as const)("refuses a %s-only wake config mismatch with a typed error and retains actors", async field => {
    const { root, config, host } = fixture();
    const file = path.join(config.residencyRoot, "config.json");
    const launch = vi.fn(async () => {});
    try {
      await host.start();
      const actor = await host.actors.create({ name: "config-fenced", instructions: "wait", residency: "durable" });
      await until(() => host.actors.status(actor.id).status === "dormant");
      host.actors.pauseForRelease(); await host.actors.checkpointForRelease(); await host.close();
      const before = new ActorRegistryStore(config.actorRoot).records();
      const changed = { ...config,
        ...(field === "actorRoot" ? { actorRoot: path.join(root, "foreign-actors") } :
          field === "sessionActorRoot" ? { sessionActorRoot: path.join(root, "foreign-session-actors") } :
          field === "mesh.actorScope" ? { mesh: { ...config.mesh, actorScope: config.mesh.actorScope === "session" ? "project" : "session" } } :
          { futureAuthority: { grants: ["all"] } }),
      };
      fs.writeFileSync(file, field === "unreadable" ? "{invalid json" : JSON.stringify(changed));
      expect(routesAt(config.residencyRoot)).toBeUndefined();
      await expect(requestResidentWake(config.residencyRoot, { id: "refused" }, launch)).rejects.toBeInstanceOf(ResidentWakeConfigMismatch);
      await expect(requestResidentWake(config.residencyRoot, { id: "refused" }, launch)).rejects.toMatchObject({
        code: "RESIDENT_WAKE_CONFIG_MISMATCH", root: config.residencyRoot,
      });
      if (process.platform !== "win32") {
        await expect(superviseWake(file, launch, { wakeOnly: true })).rejects.toMatchObject({ code: "RESIDENT_WAKE_CONFIG_MISMATCH" });
      }
      expect(launch).not.toHaveBeenCalled();
      expect(new ActorRegistryStore(config.actorRoot).records()).toEqual(before);
      expect(before.find(row => row.id === actor.id)?.status).toBe("dormant");
      expect(fs.existsSync(path.join(config.residencyRoot, "owner.json"))).toBe(false);
      // Identical canonical config, even with reordered object keys, admits the ordinary wake.
      fs.writeFileSync(file, JSON.stringify(Object.fromEntries(Object.entries(config).reverse())));
      await requestResidentWake(config.residencyRoot, { id: "accepted" }, launch);
      expect(launch).toHaveBeenCalledOnce();
    } finally { await host.close(); vi.restoreAllMocks(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it.skipIf(process.platform === "win32")("rechecks exact config before every delivery-owned successor", async () => {
    const { root, config, host } = fixture();
    const file = path.join(config.residencyRoot, "config.json");
    const run = vi.fn(async () => {
      fs.writeFileSync(residentSleepingPath(config.residencyRoot), JSON.stringify({ request: { id: "old" } }));
      fs.writeFileSync(residentWakeRequestPath(config.residencyRoot), JSON.stringify({ id: "crossing" }));
      fs.writeFileSync(file, JSON.stringify({ ...config, sessionActorRoot: path.join(root, "replacement-session") }));
    });
    try {
      await expect(superviseWake(file, run, { wakeOnly: true })).rejects.toMatchObject({ code: "RESIDENT_WAKE_CONFIG_MISMATCH" });
      expect(run).toHaveBeenCalledOnce();
      expect(readWakeJson<{ id: string }>(residentWakeRequestPath(config.residencyRoot))?.id).toBe("crossing");
    } finally { await host.close(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it("rechecks config after committed intent before starting the wake", async () => {
    const { root, config, host } = fixture();
    const file = path.join(config.residencyRoot, "config.json");
    const launch = vi.fn(async () => {});
    try {
      await expect(requestResidentWake(config.residencyRoot, { id: "committed", sequence: 8 }, launch, async () => {
        fs.writeFileSync(file, JSON.stringify({ ...config, actorRoot: path.join(root, "racing-actors") }));
      })).rejects.toMatchObject({ code: "RESIDENT_WAKE_CONFIG_MISMATCH" });
      expect(launch).not.toHaveBeenCalled();
      expect(readWakeJson<{ id: string }>(residentWakeRequestPath(config.residencyRoot))?.id).toBe("committed");
    } finally { await host.close(); vi.restoreAllMocks(); fs.rmSync(root, { recursive: true, force: true }); }
  });


  it.each(["missing", "unreadable", "foreign-root", "foreign-generation", "actorRoot", "sessionActorRoot", "mesh.actorScope", "authority"] as const)("keeps main's idle behavior with a %s wake config until a valid eligibility event", async invalid => {
    const { root, config, host } = fixture();
    const configPath = path.join(config.residencyRoot, "config.json");
    const wake = await import("../src/residency/wake.js");
    const probes = vi.spyOn(wake, "assertResidentWakeWatch");
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    if (invalid === "missing") fs.rmSync(configPath);
    else if (invalid === "unreadable") fs.writeFileSync(configPath, "{broken json");
    else fs.writeFileSync(configPath, JSON.stringify({ ...config,
      ...(invalid === "foreign-root" ? { rootId: "session:foreign" } :
        invalid === "foreign-generation" ? { fabricExtensionPath: "other-release/index.js" } :
        invalid === "actorRoot" ? { actorRoot: path.join(root, "other-actors") } :
        invalid === "sessionActorRoot" ? { sessionActorRoot: path.join(root, "other-session-actors") } :
        invalid === "mesh.actorScope" ? { mesh: { ...config.mesh, actorScope: config.mesh.actorScope === "session" ? "project" : "session" } } :
        { futureAuthority: { grants: ["all"] } }) }));
    try {
      await host.start();
      const actor = await host.actors.create({ name: "no-restart", instructions: "wait", residency: "durable" });
      await sleep(200); // allow the actual event-owned eligibility check to complete
      expect(host.actors.status(actor.id).status).toBe("idle");
      expect(probes).not.toHaveBeenCalled();
      expect(warn).toHaveBeenCalledWith(expect.stringContaining("restart config differs or is unreadable"));
      expect(warn.mock.calls.every(([line]) => !String(line).includes("\n"))).toBe(true);
      fs.writeFileSync(configPath, JSON.stringify(config));
      await host.actors.setInstructions(actor.id, "valid restart now");
      await until(() => host.actors.status(actor.id).status === "dormant");
      expect(probes).toHaveBeenCalledOnce();
    } finally { await host.close(); vi.restoreAllMocks(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it("closes the proven native watcher with its host and re-proves for the next owner", async () => {
    const { root, config, host } = fixture();
    const watch = fs.watch;
    const watchers: fs.FSWatcher[] = [];
    const closes: ReturnType<typeof vi.spyOn>[] = [];
    let next: ResidentHost | undefined;
    vi.spyOn(fs, "watch").mockImplementation((...args: Parameters<typeof fs.watch>) => {
      const watcher = watch(...args);
      if (args[0] === config.residencyRoot) { watchers.push(watcher); closes.push(vi.spyOn(watcher, "close")); }
      return watcher;
    });
    try {
      await host.start();
      const actor = await host.actors.create({ name: "watch-lifetime", instructions: "wait", residency: "durable" });
      await until(() => host.actors.status(actor.id).status === "dormant");
      expect(watchers).toHaveLength(1);
      expect(closes[0]).not.toHaveBeenCalled();
      await host.close();
      expect(closes[0]).toHaveBeenCalledOnce();
      next = new ResidentHost(config, () => {});
      await next.start();
      fakeRun(next);
      await next.actors.ask(actor.id, "wake and settle");
      await until(() => next!.actors.status(actor.id).status === "dormant");
      expect(watchers).toHaveLength(2);
      await next.close();
      expect(closes[1]).toHaveBeenCalledOnce();
    } finally { await next?.close(); await host.close(); vi.restoreAllMocks(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it("retires an outstanding watcher proof before normal host close returns", async () => {
    const { root, config, host } = fixture();
    const watch = fs.watch;
    const close = vi.fn(() => { queueMicrotask(() => watcher.emit("close")); });
    const watcher = Object.assign(new EventEmitter(), { close }) as unknown as fs.FSWatcher;
    let probing = false;
    try {
      await host.start();
      vi.spyOn(fs, "watch").mockImplementation((...args: Parameters<typeof fs.watch>) => {
        if (args[0] !== config.residencyRoot) return watch(...args);
        probing = true; return watcher; // deliberately no proof notification
      });
      const actor = await host.actors.create({ name: "closing-proof", instructions: "wait", residency: "durable" });
      await until(() => probing);
      await host.close();
      expect(close).toHaveBeenCalledOnce();
      expect(host.actors.status(actor.id).status).toBe("idle");
      expect(fs.readdirSync(config.residencyRoot).some(name => name.startsWith(".wake-watch-"))).toBe(false);
    } finally { await host.close(); vi.restoreAllMocks(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it.each([false, true])("keeps the 129th dormancy request resident with one warning (outstanding watermarks: %s)", async outstanding => {
    const { root, config, host, idle } = fixture();
    const wake = await import("../src/residency/wake.js");
    vi.spyOn(wake, "wakeResidentActors").mockResolvedValue(); // retain the existing sleepers' pending work
    const probes = vi.spyOn(wake, "assertResidentWakeWatch");
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const residents = fillWakeCapacity(config);
    const delivery = { id: "outstanding", sequence: 0 };
    try {
      await host.start();
      if (outstanding) await host.mesh.exclusive(() => indexResidentDeliveries(config.meshRoot, { ...delivery,
        topic: "capacity.delivery", kind: "event", from: { id: "publisher", name: "publisher", kind: "main" }, createdAt: 0 }, []));
      const actor = await host.actors.create({ name: "resident-129", instructions: "wait", residency: "durable" });
      await until(() => warn.mock.calls.some(([line]) => String(line).includes("wake capacity")));
      expect(host.actors.status(actor.id).status).toBe("idle");
      expect(probes).not.toHaveBeenCalled();
      await host.actors.setInstructions(actor.id, "still resident at capacity");
      const now = Date.now();
      vi.spyOn(Date, "now").mockImplementation(() => now + 31_000);
      await sleep(200);
      expect(idle).not.toHaveBeenCalled();
      expect(fs.existsSync(residentSleepingPath(config.residencyRoot))).toBe(false);
      expect(warn.mock.calls.filter(([line]) => String(line).includes("wake capacity"))).toHaveLength(1);
      await host.mesh.exclusive(() => {
        if (outstanding) acknowledgeResidentDelivery(config.meshRoot, residents[0]!, delivery);
        fs.rmSync(residents[0]!, { recursive: true, force: true });
        expect(residentWakeCapacityAvailable(config.meshRoot, config.residencyRoot)).toBe(true);
      });
      await host.actors.setInstructions(actor.id, "a slot is available now");
      await until(() => host.actors.status(actor.id).status === "dormant");
      expect(probes).toHaveBeenCalledOnce();
    } finally { await host.close(); vi.restoreAllMocks(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it("rechecks capacity under the publication lock at the final host exit boundary", async () => {
    const { root, config, host, idle } = fixture();
    const wake = await import("../src/residency/wake.js");
    vi.spyOn(wake, "wakeResidentActors").mockResolvedValue();
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      await host.start();
      const actor = await host.actors.create({ name: "late-capacity", instructions: "wait", residency: "durable" });
      await until(() => host.actors.status(actor.id).status === "dormant");
      const residents = fillWakeCapacity(config); // capacity fills AFTER the actor's admission
      const now = Date.now();
      vi.spyOn(Date, "now").mockImplementation(() => now + 31_000);
      await until(() => warn.mock.calls.some(([line]) => String(line).includes("wake capacity")));
      expect(idle).not.toHaveBeenCalled();
      expect(fs.existsSync(residentSleepingPath(config.residencyRoot))).toBe(false);
      expect(readWakeJson<{ pid: number }>(path.join(config.residencyRoot, "owner.json"))?.pid).toBe(process.pid);
      await host.mesh.exclusive(() => fs.rmSync(residents[0]!, { recursive: true, force: true }));
      vi.spyOn(Date, "now").mockImplementation(() => now + 62_000);
      await until(() => idle.mock.calls.length === 1);
      expect(fs.existsSync(residentSleepingPath(config.residencyRoot))).toBe(true);
      expect(warn.mock.calls.filter(([line]) => String(line).includes("wake capacity"))).toHaveLength(1);
    } finally { await host.close(); vi.restoreAllMocks(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it("waits for native watcher close before releasing the owner, including concurrent close callers", async () => {
    const { root, config, host } = fixture();
    const watch = fs.watch;
    const close = vi.fn();
    const watcher = Object.assign(new EventEmitter(), { close, unref: vi.fn() }) as unknown as fs.FSWatcher;
    let notify: fs.WatchListener<string> | undefined;
    let closing: Promise<void> | undefined;
    let concurrent: Promise<void> | undefined;
    try {
      await host.start();
      vi.spyOn(fs, "watch").mockImplementation((...args: Parameters<typeof fs.watch>) => {
        if (args[0] !== config.residencyRoot) return watch(...args);
        notify = args[1] as fs.WatchListener<string>;
        return watcher;
      });
      const actor = await host.actors.create({ name: "native-close-order", instructions: "wait", residency: "durable" });
      await until(() => !!notify);
      const probe = fs.readdirSync(config.residencyRoot).find(name => name.startsWith(".wake-watch-"))!;
      notify!("rename", probe);
      await until(() => host.actors.status(actor.id).status === "dormant");
      let finished = 0;
      closing = host.close().then(() => { finished++; });
      concurrent = host.close().then(() => { finished++; });
      await new Promise(resolve => setImmediate(resolve));
      expect(close).toHaveBeenCalledOnce();
      expect(finished).toBe(0);
      expect(fs.existsSync(path.join(config.residencyRoot, "owner.json"))).toBe(true);
      watcher.emit("close");
      await Promise.all([closing, concurrent]);
      expect(finished).toBe(2);
      expect(fs.existsSync(path.join(config.residencyRoot, "owner.json"))).toBe(false);
    } finally {
      watcher.emit("close");
      await Promise.all([closing, concurrent]);
      await host.close(); vi.restoreAllMocks(); fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it("rechecks process-call custody after wake proof and retries only when the real work settles", async () => {
    const { root, config, host, idle } = fixture();
    const wake = await import("../src/residency/wake.js");
    let prove!: () => void;
    const proof = new Promise<void>(resolve => { prove = resolve; });
    let releaseWork: (() => void) | undefined;
    try {
      await host.start();
      const probes = vi.spyOn(wake, "assertResidentWakeWatch").mockImplementation(() => proof);
      const actor = await host.actors.create({ name: "proof-process-race", instructions: "wait", residency: "durable" });
      await until(() => probes.mock.calls.length === 1);
      releaseWork = retainResidentProcessWork(config.sessionId);
      const commits = vi.spyOn(host.actors, "dormantIdleActors");
      prove();
      await host.participants.refreshPresence();
      await new Promise(resolve => setImmediate(resolve));
      expect(commits).not.toHaveBeenCalled();
      expect(host.actors.status(actor.id).status).toBe("idle");
      const now = Date.now();
      vi.spyOn(Date, "now").mockImplementation(() => now + 60_000);
      await sleep(300);
      expect(idle).not.toHaveBeenCalled();
      expect(host.actors.status(actor.id).status).toBe("idle");
      releaseWork();
      await until(() => host.actors.status(actor.id).status === "dormant");
      expect(probes).toHaveBeenCalledTimes(1);
    } finally { prove(); releaseWork?.(); vi.restoreAllMocks(); await host.close(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it("waits for mesh catch-up before pausing clean exit without restarting the idle window", async () => {
    const { root, host, idle } = fixture();
    let caughtUp = false;
    try {
      await host.start();
      const actor = await host.actors.create({ name: "late-native-watch", instructions: "wait", residency: "durable" });
      await until(() => host.actors.status(actor.id).status === "dormant");
      const current = host.actors.meshCaughtUp.bind(host.actors);
      vi.spyOn(host.actors, "meshCaughtUp").mockImplementation(() => caughtUp && current());
      const checkpoint = vi.spyOn(host.actors, "checkpointForRelease");
      const now = Date.now();
      vi.spyOn(Date, "now").mockImplementation(() => now + 31_000);
      await sleep(150);
      expect(idle).not.toHaveBeenCalled();
      expect(checkpoint).not.toHaveBeenCalled();
      caughtUp = true;
      // No new clock advance or quiet-period timer: the same idle window must finish.
      await until(() => idle.mock.calls.length === 1);
      expect(checkpoint).toHaveBeenCalledOnce();
    } finally { vi.restoreAllMocks(); await host.close(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it("marks an idle actor dormant without losing its subscriptions, then reaches clean host exit", async () => {
    const { root, config, host, idle } = fixture();
    try {
      await host.start();
      const actor = await host.actors.create({ name: "listener", instructions: "wait", residency: "durable", topics: ["test.wake"] });
      await until(() => host.actors.status(actor.id).status === "dormant");
      const record = new ActorRegistryStore(config.actorRoot).records().find(row => row.id === actor.id);
      expect(record?.status).toBe("dormant");
      expect(record?.topics).toEqual(["test.wake"]);
      expect(host.actors.hasActiveDurableActor()).toBe(false);
      const now = Date.now();
      vi.spyOn(Date, "now").mockImplementation(() => now + 31_000);
      await until(() => idle.mock.calls.length === 1);
      await host.close();
      expect(fs.existsSync(path.join(config.residencyRoot, "owner.json"))).toBe(false);
      expect(fs.existsSync(path.join(config.residencyRoot, "config.json"))).toBe(true);
      const archive = readWakeJson<{ version: number; dir: string }>(path.join(config.meshRoot, "event-archive.json"));
      expect(archive?.version).toBe(1);
      expect(fs.statSync(archive!.dir).isDirectory()).toBe(true);
      expect(fs.existsSync(path.join(config.residencyRoot, "wake-routes.json"))).toBe(true);
    } finally { vi.restoreAllMocks(); await host.close(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it("does no probe and arms no dormancy timer for 30 seconds with only ineligible actors, then probes once on eligibility", async () => {
    const { root, config, host } = fixture();
    const wake = await import("../src/residency/wake.js");
    const probes = vi.spyOn(wake, "assertResidentWakeWatch");
    const timers: string[] = [];
    const timeout = globalThis.setTimeout;
    try {
      await host.start();
      const get = host.participants.get.bind(host.participants);
      vi.spyOn(host.participants, "get").mockImplementation((id, ...rest) => id === config.rootId
        ? { id: config.rootId, kind: "root", rootId: config.rootId } as never : get(id, ...rest));
      vi.spyOn(globalThis, "setTimeout").mockImplementation(((...args: Parameters<typeof setTimeout>) => {
        timers.push(new Error().stack ?? "");
        return timeout(...args);
      }) as typeof setTimeout);
      const actor = await host.actors.create({ name: "expected-30s", instructions: "wait", events: ["agent_settled"], residency: "durable" });
      await sleep(30_000);
      expect(host.actors.status(actor.id).status).toBe("idle");
      expect(probes).not.toHaveBeenCalled();
      expect(timers.filter(stack => stack.includes("armDormancySafetyTimer") || stack.includes("assertResidentWakeWatch"))).toHaveLength(0);
      const eligibleAt = performance.now();
      await host.actors.setEvents(actor.id, []);
      await until(() => host.actors.status(actor.id).status === "dormant", 2_500);
      expect(performance.now() - eligibleAt).toBeLessThan(2_500);
      expect(probes).toHaveBeenCalledTimes(1);
      fakeRun(host);
      host.actors.tell(actor.id, "wake again");
      await until(() => host.actors.status(actor.id).status === "dormant", 3_000);
      expect(probes).toHaveBeenCalledTimes(1);
      expect(fs.readdirSync(config.residencyRoot).some(name => name.startsWith(".wake-watch-"))).toBe(false);
    } finally { vi.restoreAllMocks(); await host.close(); fs.rmSync(root, { recursive: true, force: true }); }
  }, 40_000);

  it("re-probes exactly once after a proven native watcher emits error", async () => {
    const { root, config, host } = fixture();
    const watch = fs.watch;
    const watchers: fs.FSWatcher[] = [];
    const wake = await import("../src/residency/wake.js");
    const probes = vi.spyOn(wake, "assertResidentWakeWatch");
    try {
      await host.start();
      vi.spyOn(fs, "watch").mockImplementation((...args: Parameters<typeof fs.watch>) => {
        const watcher = watch(...args);
        if (args[0] === config.residencyRoot) watchers.push(watcher);
        return watcher;
      });
      const actor = await host.actors.create({ name: "watch-error", instructions: "wait", residency: "durable" });
      await until(() => host.actors.status(actor.id).status === "dormant");
      expect(probes).toHaveBeenCalledTimes(1);
      watchers[0]!.emit("error", new Error("native watcher failed after proof"));
      await until(() => watchers.length === 2);
      await sleep(1_200);
      expect(probes).toHaveBeenCalledTimes(2);
      expect(watchers).toHaveLength(2);
      expect(host.actors.status(actor.id).status).toBe("dormant");
    } finally { vi.restoreAllMocks(); await host.close(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it("makes an idle actor dormant at its eligibility event without a dormancy timeout", async () => {
    const { root, config, host } = fixture();
    const wake = await import("../src/residency/wake.js");
    const timers: Array<{ delay: number | undefined; stack: string }> = [];
    const timeout = globalThis.setTimeout;
    try {
      await host.start();
      // Native watcher capability has separate coverage; isolate eligibility scheduling.
      vi.spyOn(wake, "assertResidentWakeWatch").mockResolvedValue();
      const get = host.participants.get.bind(host.participants);
      vi.spyOn(host.participants, "get").mockImplementation((id, ...rest) => id === config.rootId
        ? { id: config.rootId, kind: "root", rootId: config.rootId } as never : get(id, ...rest));
      const actor = await host.actors.create({ name: "eligibility-event", instructions: "wait", events: ["agent_settled"], residency: "durable" });
      expect(host.actors.status(actor.id).status).toBe("idle");
      vi.spyOn(globalThis, "setTimeout").mockImplementation(((...args: Parameters<typeof setTimeout>) => {
        timers.push({ delay: args[1], stack: new Error().stack ?? "" });
        return timeout(...args);
      }) as typeof setTimeout);
      await host.actors.setEvents(actor.id, []);
      await until(() => host.actors.status(actor.id).status === "dormant");
      expect(timers.filter(timer => scheduledByHost(timer.stack))).toEqual([]);
      expect(new ActorRegistryStore(config.actorRoot).records().find(row => row.id === actor.id)?.status).toBe("dormant");
    } finally { vi.restoreAllMocks(); await host.close(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it("a delivery during the wake proof cancels eligibility and settles dormant without a quiet-period timer", async () => {
    const { root, host } = fixture();
    const wake = await import("../src/residency/wake.js");
    let prove!: () => void;
    const proof = new Promise<void>(resolve => { prove = resolve; });
    let finish!: () => void;
    const running = new Promise<void>(resolve => { finish = resolve; });
    const timers: string[] = [];
    const timeout = globalThis.setTimeout;
    try {
      await host.start();
      const probes = vi.spyOn(wake, "assertResidentWakeWatch").mockImplementation(() => proof);
      const runs = fakeRun(host, async () => running);
      vi.spyOn(globalThis, "setTimeout").mockImplementation(((...args: Parameters<typeof setTimeout>) => {
        timers.push(new Error().stack ?? "");
        return timeout(...args);
      }) as typeof setTimeout);
      const actor = await host.actors.create({ name: "proof-delivery-race", instructions: "process", residency: "durable" });
      await until(() => probes.mock.calls.length === 1);
      host.actors.tell(actor.id, "exactly one racing delivery");
      await until(() => runs.mock.calls.length === 1);
      prove();
      await host.participants.refreshPresence();
      expect(host.actors.status(actor.id).status).not.toBe("dormant");
      expect(host.actors.inFlightCount()).toBe(1);
      finish();
      await until(() => host.actors.status(actor.id).status === "dormant");
      expect(host.actors.inFlightCount()).toBe(0);
      expect(runs).toHaveBeenCalledTimes(1);
      expect(runs.mock.calls[0]![0].task).toContain("exactly one racing delivery");
      expect(timers.filter(stack => scheduledByHost(stack))).toEqual([]);
    } finally { prove(); finish(); vi.restoreAllMocks(); await host.close(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it("revalidates live Main protection after the awaited wake proof", async () => {
    const { root, config, host } = fixture();
    const wake = await import("../src/residency/wake.js");
    let prove!: () => void;
    const proof = new Promise<void>(resolve => { prove = resolve; });
    let mainLive = false;
    try {
      await host.start();
      const get = host.participants.get.bind(host.participants);
      vi.spyOn(host.participants, "get").mockImplementation((id, ...rest) => id === config.rootId
        ? mainLive ? { id: config.rootId, kind: "root", rootId: config.rootId } as never : undefined : get(id, ...rest));
      const probes = vi.spyOn(wake, "assertResidentWakeWatch").mockImplementation(() => proof);
      const actor = await host.actors.create({ name: "proof-presence-race", instructions: "supervise", events: ["agent_settled"], residency: "durable" });
      await until(() => probes.mock.calls.length === 1);
      const commits = vi.spyOn(host.actors, "dormantIdleActors");
      mainLive = true;
      prove();
      await until(() => commits.mock.calls.length > 0);
      expect(commits.mock.calls[0]![0]?.has(actor.id)).toBe(true);
      expect(host.actors.status(actor.id).status).toBe("idle");
      mainLive = false;
      await host.actors.setEvents(actor.id, []);
      await until(() => host.actors.status(actor.id).status === "dormant");
    } finally { prove(); vi.restoreAllMocks(); await host.close(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it.each(["rejection", "abort"] as const)("rechecks dormancy after a presence refresh %s clears live Main protection", async outcome => {
    const { root, config, host } = fixture();
    const wake = await import("../src/residency/wake.js");
    const error = Object.assign(new Error(`presence refresh ${outcome}`), { name: outcome === "abort" ? "AbortError" : "Error" });
    let mainLive = true;
    let fail: (() => void) | undefined;
    let update: Promise<unknown> | undefined;
    try {
      await host.start();
      vi.spyOn(wake, "assertResidentWakeWatch").mockResolvedValue();
      const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
      const get = host.participants.get.bind(host.participants);
      vi.spyOn(host.participants, "get").mockImplementation((id, ...rest) => id === config.rootId
        ? mainLive ? { id: config.rootId, kind: "root", rootId: config.rootId } as never : undefined : get(id, ...rest));
      const checks = vi.spyOn(host.actors, "hasDormantIdleActor");
      const actor = await host.actors.create({ name: `failed-presence-${outcome}`, instructions: "supervise", events: ["agent_settled"], residency: "durable" });
      await until(() => checks.mock.calls.some(([protectedIds]) => protectedIds?.has(actor.id)));
      expect(host.actors.status(actor.id).status).toBe("idle");

      const pending = new Promise<void>((_resolve, reject) => { fail = () => reject(error); });
      const refresh = vi.spyOn(host.participants, "refreshPresence").mockImplementationOnce(() => pending);
      checks.mockClear();
      update = host.actors.setInstructions(actor.id, "supervise after presence clears");
      // Consume the mutation's idle check while Main still protects the actor;
      // clearing presence below must not require another actor event or timer.
      await until(() => refresh.mock.calls.length === 1 && checks.mock.calls.some(([protectedIds]) => protectedIds?.has(actor.id)));
      expect(host.actors.status(actor.id).status).toBe("idle");
      mainLive = false;
      fail!();
      await update;
      expect(warn).toHaveBeenCalledWith(`[pi-fabric] host actor presence: ${error.message}`);
      await until(() => host.actors.status(actor.id).status === "dormant");
      expect(new ActorRegistryStore(config.actorRoot).records().find(row => row.id === actor.id)?.status).toBe("dormant");
    } finally { if (update) { fail?.(); await update; } vi.restoreAllMocks(); await host.close(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it.each(["rejection", "abort"] as const)("preserves a presence refresh %s when idle-check scheduling throws", async outcome => {
    const { root, config, host } = fixture();
    const error = Object.assign(new Error(`original presence ${outcome}`), { name: outcome === "abort" ? "AbortError" : "Error" });
    const schedulerError = new Error("idle-check scheduler failure");
    let fail: (() => void) | undefined;
    let update: Promise<unknown> | undefined;
    try {
      await host.start();
      const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
      const get = host.participants.get.bind(host.participants);
      const protection = vi.spyOn(host.participants, "get").mockImplementation((id, ...rest) => id === config.rootId
        ? { id: config.rootId, kind: "root", rootId: config.rootId } as never : get(id, ...rest));
      const checks = vi.spyOn(host.actors, "hasDormantIdleActor");
      const actor = await host.actors.create({ name: `scheduler-error-${outcome}`, instructions: "supervise", events: ["agent_settled"], residency: "durable" });
      const pending = new Promise<void>((_resolve, reject) => { fail = () => reject(error); });
      const refresh = vi.spyOn(host.participants, "refreshPresence").mockImplementationOnce(() => pending);
      checks.mockClear();
      update = host.actors.setInstructions(actor.id, "supervise despite presence failure");
      // Drain the mutation's idle check before failing only the refresh's finalizer.
      await until(() => refresh.mock.calls.length === 1 && checks.mock.calls.some(([protectedIds]) => protectedIds?.has(actor.id)));
      const schedule = vi.spyOn(globalThis, "queueMicrotask").mockImplementationOnce(() => { throw schedulerError; });
      fail!();
      await update;
      expect(schedule).toHaveBeenCalledTimes(1);
      // The host reports B once, while the actor manager still receives and reports A.
      expect(warn.mock.calls).toEqual([
        [`[pi-fabric] resident idle check scheduling failed: ${String(schedulerError)}`],
        [`[pi-fabric] host actor presence: ${error.message}`],
      ]);
      // #7988: the failed enqueue must not latch #idleCheckQueued forever.
      protection.mockRestore();
      checks.mockClear();
      await host.actors.setInstructions(actor.id, "next event can schedule");
      await until(() => host.actors.status(actor.id).status === "dormant");
      expect(schedule.mock.calls.length).toBeGreaterThan(1);
      expect(checks).toHaveBeenCalled();
    } finally { if (update) { fail?.(); await update; } vi.restoreAllMocks(); await host.close(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it.each(["no-op", "failure"] as const)("does not retry a %s dormancy commit without a new event", async outcome => {
    const { root, host } = fixture();
    try {
      await host.start();
      const attempts = vi.spyOn(host.actors, "dormantIdleActors");
      if (outcome === "failure") attempts.mockRejectedValue(new Error("registry unavailable"));
      else attempts.mockResolvedValue(0);
      const actor = await host.actors.create({ name: "event-only-recheck", instructions: "wait", residency: "durable" });
      await until(() => attempts.mock.calls.length > 0);
      // Join the event-owned presence write, then observe beyond the removed 1 s nudge.
      await host.participants.refreshPresence();
      await sleep(100);
      const count = attempts.mock.calls.length;
      await sleep(1_200);
      expect(attempts).toHaveBeenCalledTimes(count);
      expect(host.actors.status(actor.id).status).toBe("idle");
      await host.actors.setEvents(actor.id, ["agent_settled"]);
      await until(() => attempts.mock.calls.length > count);
    } finally { vi.restoreAllMocks(); await host.close(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it.each(["error", "throw", "silent"] as const)("keeps the actor warm and logs the reason when its root watcher is %s", async failure => {
    const { root, config, host, idle } = fixture();
    const originalWatch = fs.watch;
    const watchers: fs.FSWatcher[] = [];
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      await host.start();
      vi.spyOn(fs, "watch").mockImplementation((...args: Parameters<typeof fs.watch>) => {
        if (args[0] !== config.residencyRoot) return originalWatch(...args);
        if (failure === "throw") throw new Error("watch unavailable for root");
        const watcher = Object.assign(new EventEmitter(), {
          close: vi.fn(() => { queueMicrotask(() => watcher.emit("close")); }),
        }) as unknown as fs.FSWatcher;
        watchers.push(watcher);
        if (failure === "error") queueMicrotask(() => watcher.emit("error", new Error("watch failed for root")));
        return watcher;
      });
      const dormant = vi.spyOn(host.actors, "dormantIdleActors");
      const actor = await host.actors.create({ name: "unsupported-root", instructions: "wait", residency: "durable" });
      await until(() => warn.mock.calls.some(([message]) => String(message).includes("resident dormancy disabled")));
      const now = Date.now();
      vi.spyOn(Date, "now").mockImplementation(() => now + 60_000);
      await sleep(1_200);
      expect(host.actors.status(actor.id).status).toBe("idle");
      expect(idle).not.toHaveBeenCalled();
      expect(dormant).not.toHaveBeenCalled();
      expect(warn).toHaveBeenCalledWith(expect.stringContaining(config.residencyRoot));
      expect(warn).toHaveBeenCalledWith(expect.stringContaining(failure === "silent" ? "did not notify" : "watch"));
      expect(watchers.every(watcher => vi.mocked(watcher.close).mock.calls.length === 1)).toBe(true);
      expect(fs.readdirSync(config.residencyRoot).some(name => name.startsWith(".wake-watch-"))).toBe(false);
    } finally { vi.restoreAllMocks(); await host.close(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it("never sleeps an activation with a pending ask/reply", async () => {
    const { root, host, idle } = fixture();
    let reply!: () => void;
    const gate = new Promise<void>(resolve => { reply = resolve; });
    let ask: Promise<unknown> | undefined;
    try {
      await host.start();
      fakeRun(host, async () => gate);
      const actor = await host.actors.create({ name: "asking", instructions: "work", residency: "durable" });
      ask = host.actors.ask(actor.id, "wait for reply");
      await until(() => host.actors.inFlightCount() === 1);
      expect(await host.actors.dormantIdleActors()).toBe(0);
      const now = Date.now();
      vi.spyOn(Date, "now").mockImplementation(() => now + 60_000);
      await sleep(300);
      expect(idle).not.toHaveBeenCalled();
      expect(host.actors.status(actor.id).status).not.toBe("dormant");
      reply(); await ask;
    } finally { reply(); await ask; vi.restoreAllMocks(); await host.close(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it("retains a pending child reply even when the spawning activation has ended", async () => {
    const { root, config, host } = fixture();
    try {
      await host.start();
      const actor = await host.actors.create({ name: "child-reply", instructions: "wait", residency: "durable" });
      const childId = "2".repeat(32);
      const directory = path.join(root, childId);
      fs.mkdirSync(directory);
      fs.writeFileSync(path.join(directory, ARCHIVE_PENDING_FILE), JSON.stringify({ format: 1, awaitingResult: true, ownerPid: process.pid }));
      const store = new ActorChildCompletionStore(path.join(config.actorRoot, actor.id, "session.jsonl"));
      store.trackArchiveSource(childId, directory);
      expect(store.hasPendingReply()).toBe(true);
      expect(await host.actors.dormantIdleActors()).toBe(0);
      await sleep(1_200);
      expect(host.actors.status(actor.id).status).toBe("idle");
      fs.rmSync(path.join(directory, ARCHIVE_PENDING_FILE));
      store.releaseArchiveSource(childId);
      expect(await host.actors.dormantIdleActors()).toBe(1);
    } finally { await host.close(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it("keeps a live Main's supervisor expected, even between activations", async () => {
    const { root, config, host } = fixture();
    try {
      await host.start();
      const actor = await host.actors.create({ name: "supervisor", instructions: "supervise", events: ["agent_settled"], residency: "durable" });
      const get = host.participants.get.bind(host.participants);
      vi.spyOn(host.participants, "get").mockImplementation((id, ...rest) => id === config.rootId
        ? { id: config.rootId, kind: "root", rootId: config.rootId } as never : get(id, ...rest));
      await sleep(1_300);
      expect(host.actors.status(actor.id).status).toBe("idle");
    } finally { await host.close(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it("queues deliveries throughout the wake window and drains exactly once, in order, across two sleeps", async () => {
    const { root, config, host } = fixture();
    let resumed: ResidentHost | undefined;
    let again: ResidentHost | undefined;
    const seen: number[] = [];
    try {
      await host.start();
      const actor = await host.actors.create({ name: "wake-order", instructions: "process", residency: "durable", topics: ["test.wake"], coalesce: false });
      await until(() => host.actors.status(actor.id).status === "dormant");
      // Retain both scope cursors exactly as clean idle exit does.
      host.actors.pauseForRelease();
      await host.actors.checkpointForRelease();
      await host.close();
      const mesh = new MeshStore(config.meshRoot, config.mesh.maxEventBytes, config.mesh.maxReadEvents);
      const launch = vi.fn(async () => {}); // hold boot: delivery must already be durable
      const events = [];
      // Avoid the real spawn in this unit test; exercise the same post-commit router explicitly.
      const wake = await import("../src/residency/wake.js");
      const dispatch = wake.wakeResidentActors;
      const routed = vi.spyOn(wake, "wakeResidentActors").mockImplementation((store, batch) => dispatch(store, batch, launch));
      for (let n = 1; n <= 3; n++) events.push(await mesh.publish({ topic: "test.wake", from: { id: "publisher", name: "publisher", kind: "main" }, data: { n } }));
      expect(launch).toHaveBeenCalledTimes(3);
      expect(mesh.read().filter(event => event.topic === "test.wake").map(event => (event.data as { n: number }).n)).toEqual([1, 2, 3]);
      expect(readWakeJson<{ sequence: number }>(residentWakeRequestPath(config.residencyRoot))?.sequence).toBe(events[2]!.sequence);
      resumed = new ResidentHost(config, () => {});
      await resumed.start();
      fakeRun(resumed, async task => {
        const match = task.match(/"n"\s*:\s*(\d+)/);
        if (match) seen.push(Number(match[1]));
      });
      await until(() => seen.length === 3);
      expect(seen).toEqual([1, 2, 3]);
      await until(() => resumed!.actors.status(actor.id).status === "dormant");
      resumed.actors.pauseForRelease(); await resumed.actors.checkpointForRelease(); await resumed.close();
      again = new ResidentHost(config, () => {});
      await again.start();
      const runs = fakeRun(again);
      await sleep(400);
      expect(runs).not.toHaveBeenCalled();
      expect(seen).toEqual([1, 2, 3]);
      routed.mockRestore(); mesh.closeState();
    } finally { await host.close(); await resumed?.close(); await again?.close(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it("admits a direct dormant message, commits before wake, then gets a real owner ACK and one actor activation", async () => {
    type Ports = ConstructorParameters<typeof AgentMessageRouter>;
    const { root, config, host } = fixture();
    const mesh = new MeshStore(config.meshRoot, config.mesh.maxEventBytes, config.mesh.maxReadEvents);
    const identity = { id: "session:sender", name: "sender", kind: "main" as const };
    const sender = new FabricControlPlane(mesh, identity, { enabled: true, hostId: identity.id, pollMs: 10, acknowledgementTimeoutMs: 50 });
    let woken: ResidentHost | undefined;
    const seen: string[] = [];
    try {
      await host.start();
      const created = await host.actors.create({ name: "direct-listener", instructions: "process", residency: "durable" });
      await until(() => host.actors.status(created.id).status === "dormant");
      const actor = host.actors.status(created.id);
      host.actors.pauseForRelease(); await host.actors.checkpointForRelease(); await host.close();
      const routes = readWakeJson<{ actors: Array<{ participant?: { ownerHostId: string; actorOwnershipToken?: string } }> }>(path.join(config.residencyRoot, "wake-routes.json"));
      expect(routes?.actors[0]?.participant?.actorOwnershipToken).toBe(actor.ownershipToken);
      expect(routes?.actors[0]?.participant?.ownerHostId).toBe(host.hostId);
      sender.start(async () => ({ accepted: false, error: "sender does not own actors" }));
      const wake = await import("../src/residency/wake.js");
      const dispatch = wake.wakeResidentActors;
      let launches = 0;
      vi.spyOn(wake, "wakeResidentActors").mockImplementation((store, events) => dispatch(store, events, async () => {
        launches++;
        expect(mesh.read().some(event => event.topic === "fabric.control.command" && (event.data as { targetId?: string }).targetId === actor.id)).toBe(true);
        const { AgentManager } = await import("../src/agents/manager.js");
        vi.spyOn(AgentManager.prototype, "run").mockImplementation(async (request, _signal, onSpawned) => {
          const id = "1".repeat(32); onSpawned?.({ id } as never); seen.push(request.task);
          return { id, status: "completed", text: "done", toolCalls: 0, startedAt: Date.now(), finishedAt: Date.now() } as never;
        });
        // Cold startup exceeds the ordinary warm ACK deadline; the admitted command
        // must retain its bounded startup budget rather than expire and get resent.
        await sleep(250);
        woken = new ResidentHost(config, () => {});
        await woken.start();
      }));
      const actors = { identity, mesh, owns: () => false, status: (id: string) => {
        if (id !== actor.id) throw new Error(`Unknown Fabric actor: ${id}`); return actor;
      }, validateDirectMessage: host.actors.validateDirectMessage.bind(host.actors), resolveBinding: () => ({}),
      resolveActivationBinding: vi.fn(), tell: vi.fn(), ask: vi.fn(), stop: vi.fn(), steerRemote: vi.fn() };
      const router = new AgentMessageRouter({ status: (id: string) => { throw new Error(`Unknown Fabric agent: ${id}`); } } as unknown as Ports[0],
        actors as Ports[1], { id: identity.id, local: true, matches: (id: string) => id === identity.id, deliverAgent: vi.fn() } as Ports[2],
        { get: () => undefined, scheduleRefresh: vi.fn(), lastKnown: () => undefined } as Ports[3], sender, binding => binding);
      const result = await router.routeMessage(actor.id, "admitted direct delivery", undefined, "followUp");
      expect(result.acknowledged).toBe(true);
      expect(launches).toBe(1);
      expect(mesh.read().filter(event => event.topic === "fabric.control.command")).toHaveLength(1);
      await until(() => seen.length === 1);
      expect(seen[0]).toContain("admitted direct delivery");
      await sleep(100);
      expect(seen).toHaveLength(1);
      expect(actors.tell).not.toHaveBeenCalled();
    } finally { vi.restoreAllMocks(); await sender.close(); await host.close(); await woken?.close(); mesh.closeState(); fs.rmSync(root, { recursive: true, force: true }); }
  }, 15_000);

  it.skipIf(process.platform === "win32").each(["error", "deadline"] as const)("retains committed intent after external-owner watcher %s and replays on the next start", async failure => {
    const { root, config } = fixture();
    const configPath = path.join(config.residencyRoot, "config.json");
    const ownerFile = path.join(config.residencyRoot, "owner.json");
    const requestFile = residentWakeRequestPath(config.residencyRoot);
    const wake = await import("../src/residency/wake.js");
    const originalWait = wake.waitResidentChange;
    const close = vi.fn();
    const watcher = Object.assign(new EventEmitter(), { close }) as unknown as fs.FSWatcher;
    try {
      fs.writeFileSync(ownerFile, JSON.stringify({ pid: process.pid, processStartTime: processStartTime(process.pid), token: "external" }));
      fs.writeFileSync(residentSleepingPath(config.residencyRoot), JSON.stringify({ token: "external", request: { id: "covered" } }));
      fs.writeFileSync(requestFile, JSON.stringify({ format: 1, id: "committed", sequence: 7, requestedAt: Date.now() }));
      const retained = fs.readFileSync(requestFile, "utf8");
      vi.spyOn(wake, "waitResidentChange").mockImplementation((root, ready, _timeout, message) => originalWait(root, ready, 10, message));
      vi.spyOn(fs, "watch").mockImplementation(() => {
        if (failure === "error") queueMicrotask(() => watcher.emit("error", new Error("watch failed")));
        return watcher;
      });
      const run = vi.fn(async () => {
        fs.writeFileSync(residentSleepingPath(config.residencyRoot), JSON.stringify({ request: readWakeJson(requestFile) }));
      });
      await expect(superviseWake(configPath, run, { wakeOnly: true })).resolves.toMatchObject({
        status: "wake-pending", root: config.residencyRoot, reason: "Resident sleep/wake owner did not release",
      });
      expect(run).not.toHaveBeenCalled();
      expect(close).toHaveBeenCalledOnce();
      expect(fs.readFileSync(requestFile, "utf8")).toBe(retained);
      fs.rmSync(ownerFile);
      await superviseWake(configPath, run, { wakeOnly: true });
      expect(run).toHaveBeenCalledOnce();
      expect(fs.readFileSync(requestFile, "utf8")).toBe(retained);
      await superviseWake(configPath, run, { wakeOnly: true });
      expect(run).toHaveBeenCalledOnce(); // the replayed nudge is now covered
    } finally { vi.restoreAllMocks(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it.skipIf(process.platform === "win32")("a delivery crossing the final sleep boundary starts exactly one successor generation", async () => {
    const { root, config } = fixture();
    const configPath = path.join(config.residencyRoot, "config.json");
    try {
      fs.writeFileSync(residentWakeRequestPath(config.residencyRoot), JSON.stringify({ id: "first" }));
      let runs = 0;
      await superviseWake(configPath, async () => {
        runs++;
        fs.writeFileSync(residentSleepingPath(config.residencyRoot), JSON.stringify({ request: { id: runs === 1 ? "first" : "racing" } }));
        if (runs === 1) fs.writeFileSync(residentWakeRequestPath(config.residencyRoot), JSON.stringify({ id: "racing" }));
      });
      expect(runs).toBe(2);
    } finally { fs.rmSync(root, { recursive: true, force: true }); }
  });

  it.skipIf(process.platform === "win32").each(["final release", "covered startup"] as const)("a commit after equal snapshots at %s but before wake.lock release wakes and drains the actor exactly once", async phase => {
    const { root, config, host } = fixture();
    const configPath = path.join(config.residencyRoot, "config.json");
    const mesh = new MeshStore(config.meshRoot, config.mesh.maxEventBytes, config.mesh.maxReadEvents);
    let successor: ResidentHost | undefined;
    let publication: Promise<unknown> | undefined;
    let committed!: () => void;
    const commit = new Promise<void>(resolve => { committed = resolve; });
    let launches = 0;
    const seen: string[] = [];
    const wake = await import("../src/residency/wake.js");
    try {
      await host.start();
      const actor = await host.actors.create({ name: "final-window", instructions: "process", residency: "durable", topics: ["test.final-window"], coalesce: false });
      await until(() => host.actors.status(actor.id).status === "dormant");
      host.actors.pauseForRelease(); await host.actors.checkpointForRelease(); await host.close();
      fs.writeFileSync(residentWakeRequestPath(config.residencyRoot), JSON.stringify({ id: "covered" }));
      fs.writeFileSync(residentSleepingPath(config.residencyRoot), JSON.stringify({ request: { id: "covered" } }));
      const dispatch = wake.wakeResidentActors;
      vi.spyOn(wake, "wakeResidentActors").mockImplementation((store, events) => {
        committed(); // MeshStore calls here only AFTER the event durability barrier.
        return dispatch(store, events, async () => {
          await superviseWake(configPath, async () => {
            launches++;
            successor = new ResidentHost(config, () => {});
            const { AgentManager } = await import("../src/agents/manager.js");
            vi.spyOn(AgentManager.prototype, "run").mockImplementation(async (request, _signal, onSpawned) => {
              const id = "1".repeat(32);
              onSpawned?.({ id } as never);
              seen.push(request.task);
              return { id, status: "completed", text: "done", toolCalls: 0, startedAt: Date.now(), finishedAt: Date.now() } as never;
            });
            await successor.start();
            await until(() => seen.length === 1);
            await until(() => successor!.actors.status(actor.id).status === "dormant");
            successor.actors.pauseForRelease(); await successor.actors.checkpointForRelease(); await successor.close();
            fs.writeFileSync(residentSleepingPath(config.residencyRoot), JSON.stringify({ request: readWakeJson(residentWakeRequestPath(config.residencyRoot)) }));
          }, { wakeOnly: true });
        });
      });
      await superviseWake(configPath, async () => {
        fs.writeFileSync(residentSleepingPath(config.residencyRoot), JSON.stringify({ request: { id: "covered" } }));
      }, { wakeOnly: phase === "covered startup", beforeFinalRelease: async () => {
        // Exact rejected-review window: equality has already been observed; wake.lock is held.
        publication = mesh.publish({ topic: "test.final-window", from: { id: "publisher", name: "publisher", kind: "main" }, data: { n: 7 } });
        await commit;
        expect(mesh.read().filter(event => event.topic === "test.final-window")).toHaveLength(1);
        expect(readWakeJson<{ id: string }>(residentWakeRequestPath(config.residencyRoot))?.id).toBe("covered");
      } });
      await publication;
      expect(launches).toBe(1);
      expect(seen).toHaveLength(1);
      expect(seen[0]).toMatch(/"n"\s*:\s*7/);
      // Another start uses the saved queue/cursor and cannot repeat that actor activation.
      vi.restoreAllMocks();
      await successor?.close();
      successor = new ResidentHost(config, () => {});
      const { AgentManager } = await import("../src/agents/manager.js");
      const repeated = vi.spyOn(AgentManager.prototype, "run").mockResolvedValue({ status: "completed", text: "unexpected replay" } as never);
      await successor.start();
      await sleep(150);
      expect(repeated).not.toHaveBeenCalled();
    } finally {
      await publication; vi.restoreAllMocks(); await host.close(); await successor?.close(); mesh.closeState();
      fs.rmSync(root, { recursive: true, force: true });
    }
  }, 20_000);

  it.skipIf(process.platform === "win32")("serializes wake launchers and never leaves one warm after the sleep boundary", async () => {
    const { root, config } = fixture();
    const configPath = path.join(config.residencyRoot, "config.json");
    let finish!: () => void;
    const gate = new Promise<void>(resolve => { finish = resolve; });
    let active: ReturnType<typeof superviseWake> | undefined;
    try {
      fs.writeFileSync(residentWakeRequestPath(config.residencyRoot), JSON.stringify({ id: "first" }));
      const run = vi.fn(async () => {
        await gate;
        fs.writeFileSync(residentSleepingPath(config.residencyRoot), JSON.stringify({ request: { id: "first" } }));
      });
      active = superviseWake(configPath, run);
      await until(() => run.mock.calls.length === 1);
      await superviseWake(configPath, run);
      expect(run).toHaveBeenCalledTimes(1);
      finish(); await active;
      expect(run).toHaveBeenCalledTimes(1);
      expect(processStartTime(process.pid)).toBeDefined();
    } finally { finish(); await active; fs.rmSync(root, { recursive: true, force: true }); }
  });
});
