import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ResidentHost } from "../src/residency/host.js";
import { ActorManager } from "../src/actors/manager.js";
import { RESIDENT_HOST_FORMAT, residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import { ResidentRequestRetention } from "../src/residency/retention.js";
import { captureSafetyTick, type SafetyTickCapture } from "./helpers/resident-safety-tick.js";

// Windows runs the legacy retention path (retentionV2Enabled() is false on win32). Both paths
// run here on every platform, so a Windows-only difference shows up on Linux too.
const platform = vi.hoisted(() => ({ retentionV2: process.platform !== "win32" }));
vi.mock("../src/storage/retention-platform.js", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../src/storage/retention-platform.js")>()),
  retentionV2Enabled: () => platform.retentionV2,
}));

// smarty-dev#6729: an idle resident host's 50 ms request poll rebuilt the fleet-wide actor
// ownership view (every project participant, every host lease) on every tick for its idle check.
const fixture = (onIdle: () => void = () => {}) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-idle-poll-"));
  const config: ResidentHostConfig = {
    format: RESIDENT_HOST_FORMAT, rootId: "session:idle-poll", sessionId: "idle-poll",
    cwd: root, projectRoot: root, meshRoot: path.join(root, "mesh"),
    actorRoot: path.join(root, "actors"), residencyRoot: residentRoot(path.join(root, "mesh"), "session:idle-poll"),
    fullCodeMode: true, agents: DEFAULT_FABRIC_CONFIG.agents, mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, residentIdleExitMs: 30_000 },
    retention: DEFAULT_FABRIC_CONFIG.retention, workerPath: path.resolve("dist/agents/worker.js"),
    fabricExtensionPath: path.resolve("dist/index.js"), piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
  };
  fs.mkdirSync(config.residencyRoot, { recursive: true });
  const file = path.join(config.residencyRoot, "config.json");
  fs.writeFileSync(file, JSON.stringify(config));
  // A running host's config.json was usually written long ago; a fresh one is re-read until it settles.
  const old = new Date(Date.now() - 3_600_000);
  fs.utimesSync(file, old, old);
  return { root, config, file, host: new ResidentHost(config, onIdle) };
};

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
const waitUntil = async (done: () => boolean, ms = 5_000) => {
  for (let waited = 0; waited < ms && !done(); waited += 20) await sleep(20);
  return done();
};
// Main publishes config.json by an atomic replace.
const replace = (file: string, value: unknown) => {
  fs.writeFileSync(file + ".tmp", JSON.stringify(value));
  fs.renameSync(file + ".tmp", file);
};
const withRetention = (config: ResidentHostConfig, completedRequestMs: number) =>
  ({ ...config, retention: { ...config.retention, completedRequestMs } });
// smarty-dev#11107: the 50 ms request poll and 100 ms maintenance tick are gone. These clock
// tests fire the host's captured slow safety interval instead of waiting for a removed cadence.
let safety: SafetyTickCapture;
beforeEach(() => { safety = captureSafetyTick(); });
afterEach(() => safety.restore());
const tick = (_host: ResidentHost) => safety.tick();
const tickUntil = async (host: ResidentHost, done: () => boolean, ticks = 100) => {
  for (let index = 0; index < ticks && !done(); index++) { await tick(host); await sleep(20); }
  return done();
};

describe("idle resident host polling (smarty-dev#6729)", () => {
  it("rebuilds the actor ownership view once per second of clock time, not on every 50 ms request tick", async () => {
    const { root, file, host } = fixture();
    let now = Date.now();
    // The host's own clock drives the one-second reuse; real 50 ms ticks keep running.
    // Measuring against wall time made the bound depend on how fast the runner was.
    const clock = vi.spyOn(Date, "now").mockImplementation(() => now);
    const reads = vi.spyOn(fs, "readFileSync");
    const configReads = () => reads.mock.calls.filter(([read]) => String(read) === file).length;
    try {
      await host.start();
      // The first maintenance pass reads the overlay once; from then on its stamp is cached.
      expect(await waitUntil(() => configReads() > 0)).toBe(true);
      await host.actors.create({ name: "idle-poll", instructions: "wait", residency: "durable" });
      await host.participants.refresh();
      // Count the fleet-wide listings the idle check itself makes. The actor mesh monitor also
      // lists participants before each of its polls; that cadence is its own and differs by
      // platform (a 250 ms timer on Windows, file-watch wakes plus a 2 s reconcile elsewhere).
      let inCheck = 0;
      let checkListings = 0;
      const list = host.participants.list.bind(host.participants);
      vi.spyOn(host.participants, "list").mockImplementation((...args: Parameters<typeof list>) => {
        if (inCheck > 0) checkListings++;
        return list(...args);
      });
      const active = host.actors.hasOwnedActors.bind(host.actors);
      const checks = vi.spyOn(host.actors, "hasOwnedActors").mockImplementation(() => {
        inCheck++;
        try { return active(); } finally { inCheck--; }
      });
      reads.mockClear();
      // 20 idle samples with the clock still: at most the one current observation the
      // reused one needs. Before: one check (and one fleet-wide listing) on every tick.
      for (let index = 0; index < 20; index++) await tick(host);
      const first = checks.mock.calls.length;
      expect(first).toBeLessThanOrEqual(1);
      for (let second = 1; second <= 2; second++) {
        now += 1_000;
        await tick(host);
        // smarty-dev#11107: samples are event/deadline driven now; the old once-per-second
        // rebuild is an upper bound, never once per tick.
        expect(checks.mock.calls.length).toBeLessThanOrEqual(first + second);
        for (let index = 0; index < 10; index++) await tick(host);
        expect(checks.mock.calls.length).toBeLessThanOrEqual(first + second);
      }
      // Ownership stamps may now reuse the listing too; the old once-per-sample cadence
      // is an upper bound, not a requirement to rebuild unchanged ownership.
      expect(checkListings).toBeLessThanOrEqual(checks.mock.calls.length);
      // Before: the 100 ms maintenance tick re-read and parsed the unchanged config.json each time.
      expect(configReads()).toBe(0);
    } finally {
      clock.mockRestore();
      reads.mockRestore();
      await host.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  describe.each([
    { retentionV2: true, name: "retention v2 (Linux, macOS)" },
    { retentionV2: false, name: "legacy retention (Windows)" },
  ])("$name", ({ retentionV2 }) => {
    const start = async (host: ResidentHost) => {
      platform.retentionV2 = retentionV2;
      // The legacy path applies the overlay at its next sweep: make every request tick one.
      if (!retentionV2) vi.spyOn(ResidentRequestRetention.prototype, "due").mockReturnValue(true);
      await host.start();
    };
    const finish = async (host: ResidentHost, root: string) => {
      platform.retentionV2 = process.platform !== "win32";
      vi.restoreAllMocks();
      await host.close();
      fs.rmSync(root, { recursive: true, force: true });
    };

    it("applies a replaced config.json retention overlay at the next maintenance pass, then caches it", async () => {
      const { root, config, file, host } = fixture();
      try {
        await start(host);
        const reads = vi.spyOn(fs, "readFileSync");
        const configReads = () => reads.mock.calls.flatMap(([read], index) =>
          String(read) === file ? [String(reads.mock.results[index]?.value)] : []);
        replace(file, withRetention(config, 222_222));
        expect(await tickUntil(host, () => configReads().some(text => text.includes("222222")))).toBe(true);
        // Once the replacement is older than the settle window its stamp is trusted again.
        const old = new Date(Date.now() - 3_600_000);
        fs.utimesSync(file, old, old);
        reads.mockClear();
        expect(await tickUntil(host, () => configReads().length > 0)).toBe(true);
        reads.mockClear();
        for (let index = 0; index < 10; index++) await tick(host);
        expect(configReads()).toHaveLength(0);
      } finally {
        await finish(host, root);
      }
    });

    it("re-reads a same-size replacement whose stat stamp did not change (file id 0, one timestamp tick)", async () => {
      const { root, config, file, host } = fixture();
      try {
        await start(host);
        const reads = vi.spyOn(fs, "readFileSync");
        const configReads = () => reads.mock.calls.flatMap(([read], index) =>
          String(read) === file ? [String(reads.mock.results[index]?.value)] : []);
        replace(file, withRetention(config, 111_111));
        // A volume that reports file id 0 and a coarse timestamp: the same-size replacement
        // below, within the same tick, stats identically to this one.
        const stamp = Object.assign(Object.create(Object.getPrototypeOf(fs.statSync(file, { bigint: true })) as object),
          fs.statSync(file, { bigint: true }), { ino: 0n }) as fs.BigIntStats;
        const stat = fs.statSync;
        vi.spyOn(fs, "statSync").mockImplementation(((target: fs.PathLike, options?: fs.StatSyncOptions) =>
          String(target) === file ? stamp : stat(target, options)) as typeof fs.statSync);
        expect(await tickUntil(host, () => configReads().some(text => text.includes("111111")))).toBe(true);
        replace(file, withRetention(config, 222_222));
        expect(fs.statSync(file, { bigint: true })).toBe(stamp);
        // Before: the identical stamp kept the old overlay for good.
        expect(await tickUntil(host, () => configReads().some(text => text.includes("222222")))).toBe(true);
      } finally {
        await finish(host, root);
      }
    });
  });

  it("never exits on a reused actor observation: an idle exit is confirmed by a current check", async () => {
    let idled = 0;
    let now = Date.now();
    // Freeze before construction/startup: fence and recovery latency are not idle time.
    const clock = vi.spyOn(Date, "now").mockImplementation(() => now);
    const { root, host } = fixture(() => { idled++; });
    // smarty-dev#11107: actor ownership changes reach the host as ActorManager events (no tick).
    const actorListeners: Array<() => void> = [];
    const subscribe = ActorManager.prototype.subscribe;
    vi.spyOn(ActorManager.prototype, "subscribe").mockImplementation(function (this: ActorManager, listener) {
      actorListeners.push(listener); return subscribe.call(this, listener);
    });
    try {
      await host.start();
      let active = false;
      const checks = vi.spyOn(host.actors, "hasOwnedActors").mockImplementation(() => active);
      await sleep(200); await tick(host); // Start the continuously dead-and-empty observation window.
      // Just short of the idle window: the next tick takes a current (inactive) observation.
      now += 29_950;
      await sleep(200); await tick(host);
      expect(idled).toBe(0);
      // An actor wakes; the reused observation still says inactive, and the window has elapsed.
      active = true;
      const before = checks.mock.calls.length;
      now += 100;
      await sleep(200); await tick(host);
      expect(checks.mock.calls.length).toBeGreaterThan(before);
      expect(idled).toBe(0);
      // Truly idle for the whole window again: the host exits.
      active = false;
      now += 1_000;
      for (const listener of actorListeners) listener(); // the actor left: its event
      await sleep(200); await tick(host); // First confirmed zero-actor sample starts the new window.
      now += 30_100;
      await sleep(200); await tick(host);
      expect(idled).toBeGreaterThan(0);
    } finally {
      clock.mockRestore();
      await host.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it("counts the idle window from a durable actor run that started and ended between two cached samples", async () => {
    let idled = 0;
    const { root, host } = fixture(() => { idled++; });
    let now = Date.now();
    const clock = vi.spyOn(Date, "now").mockImplementation(() => now);
    try {
      await host.start();
      await sleep(200); await tick(host);
      // Close to the idle window with no actor: the next tick caches an inactive observation.
      now += 29_000;
      await sleep(200); await tick(host);
      expect(idled).toBe(0);
      const sampledAt = now;
      const runs = vi.spyOn(host.agents, "run").mockImplementation(async (_request, _signal, onSpawned) => {
        onSpawned?.({ id: "run-between-samples" } as never);
        return { id: "run-between-samples", status: "completed", text: "done", toolCalls: 0 } as never;
      });
      // A durable actor is created, runs and stops, all inside the same one-second cached sample.
      const actor = await host.actors.create({ name: "between-samples", instructions: "wait", residency: "durable" });
      host.actors.tell(actor.id, "go");
      for (let waited = 0; waited < 5_000 && !(runs.mock.calls.length === 1 && host.actors.status(actor.id).status === "idle"); waited += 20) await sleep(20);
      expect(runs).toHaveBeenCalledTimes(1);
      await host.actors.stop(actor.id);
      expect(host.actors.status(actor.id).status).toBe("stopped");
      // A stopped actor still belongs to this resident. Only removal leaves zero actors.
      await host.actors.remove(actor.id);
      await sleep(200); await tick(host);
      // The clock did not move: by time alone every tick during the run reused the inactive sample.
      expect(Date.now()).toBe(sampledAt);
      const ended = now;
      // Past the window since the host started, but only 1.1 s after the run ended.
      now += 1_100;
      await sleep(200); await tick(host);
      expect(idled).toBe(0);
      now = ended + 29_900;
      await sleep(200); await tick(host);
      expect(idled).toBe(0);
      // The full window after the run ended: the host exits.
      now = ended + 30_100;
      await sleep(200); await tick(host);
      expect(idled).toBeGreaterThan(0);
    } finally {
      clock.mockRestore();
      await host.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
});
