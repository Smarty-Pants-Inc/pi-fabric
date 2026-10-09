import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ResidentHost } from "../src/residency/host.js";
import { RESIDENT_HOST_FORMAT, residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import { ResidentRequestRetention } from "../src/residency/retention.js";

// Windows runs the legacy retention path (retentionV2Enabled() is false on win32). Both paths
// run here on every platform, so a Windows-only difference shows up on Linux too.
const platform = vi.hoisted(() => ({ retentionV2: process.platform !== "win32" }));
vi.mock("../src/storage/retention-platform.js", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../src/storage/retention-platform.js")>()),
  retentionV2Enabled: () => platform.retentionV2,
}));

// The resident request/idle-exit path must not rebuild ownership on a recurring 50 ms tick.
const fixture = (onIdle: () => void = () => {}) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-idle-poll-"));
  const config: ResidentHostConfig = {
    format: RESIDENT_HOST_FORMAT, rootId: "session:idle-poll", sessionId: "idle-poll",
    cwd: root, projectRoot: root, meshRoot: path.join(root, "mesh"),
    actorRoot: path.join(root, "actors"), residencyRoot: residentRoot(path.join(root, "mesh"), "session:idle-poll"),
    fullCodeMode: true, agents: DEFAULT_FABRIC_CONFIG.agents, mesh: DEFAULT_FABRIC_CONFIG.mesh,
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

import { idleDeadlineDriver } from "./helpers/resident-idle-deadline.js";

describe("event-driven resident host idle exit (smarty-dev#6729 / #6782)", () => {
  it("arms no recurring request/idle-exit timer below 60 seconds and does not sample actors between events", async () => {
    const { root, host } = fixture();
    const interval = globalThis.setInterval;
    const hostIntervals: number[] = [];
    vi.spyOn(globalThis, "setInterval").mockImplementation(((...args: Parameters<typeof setInterval>) => {
      const caller = (new Error().stack ?? "").split("\n").find(line => line.includes("/src/"));
      if (caller?.includes("/residency/host.ts")) hostIntervals.push(Number(args[1]));
      return interval(...args);
    }) as typeof setInterval);
    const deadline = idleDeadlineDriver();
    try {
      await host.start();
      expect(await waitUntil(() => deadline.count() === 1)).toBe(true);
      expect(hostIntervals.length).toBeGreaterThan(0);
      expect(hostIntervals.every(ms => ms >= 60_000)).toBe(true);
      const checks = vi.spyOn(host.actors, "hasActiveDurableActor");
      await sleep(1_200);
      expect(checks).not.toHaveBeenCalled();
      expect(deadline.count()).toBe(1);
    } finally { await host.close(); expect(deadline.count()).toBe(0); vi.restoreAllMocks(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it("watches requests and drains a startup backlog larger than one 32-request batch without a poll", async () => {
    const { root, config, host } = fixture();
    const requests = path.join(config.residencyRoot, "requests");
    const responses = path.join(config.residencyRoot, "responses");
    fs.mkdirSync(requests, { recursive: true });
    for (let n = 0; n < 40; n++) fs.writeFileSync(path.join(requests, `backlog-${n}.json`), "{}");
    try {
      await host.start();
      expect(await waitUntil(() => fs.readdirSync(responses).length === 40)).toBe(true);
      fs.writeFileSync(path.join(requests, "watched.json"), "{}");
      expect(await waitUntil(() => fs.existsSync(path.join(responses, "watched.json")), 2_000)).toBe(true);
      expect(fs.readdirSync(requests)).toHaveLength(0);
    } finally { await host.close(); vi.restoreAllMocks(); fs.rmSync(root, { recursive: true, force: true }); }
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
        expect(await waitUntil(() => configReads().some(text => text.includes("222222")))).toBe(true);
        // Once the replacement is older than the settle window its stamp is trusted again.
        const old = new Date(Date.now() - 3_600_000);
        fs.utimesSync(file, old, old);
        reads.mockClear();
        expect(await waitUntil(() => configReads().length > 0)).toBe(true);
        reads.mockClear();
        await sleep(500);
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
        expect(await waitUntil(() => configReads().some(text => text.includes("111111")))).toBe(true);
        replace(file, withRetention(config, 222_222));
        expect(fs.statSync(file, { bigint: true })).toBe(stamp);
        // Before: the identical stamp kept the old overlay for good.
        expect(await waitUntil(() => configReads().some(text => text.includes("222222")), 3_000)).toBe(true);
      } finally {
        await finish(host, root);
      }
    });
  });

  it("checks current actor custody at the one-shot idle deadline and restarts the window on settlement", async () => {
    let idled = 0;
    const { root, host } = fixture(() => { idled++; });
    let now = Date.now();
    const clock = vi.spyOn(Date, "now").mockImplementation(() => now);
    const deadline = idleDeadlineDriver();
    try {
      await host.start();
      expect(await waitUntil(() => deadline.count() === 1)).toBe(true);
      let active = false;
      const checks = vi.spyOn(host.actors, "hasActiveDurableActor").mockImplementation(() => active);
      now += 29_950;
      deadline.fire();
      expect(await waitUntil(() => deadline.count() === 1)).toBe(true);
      expect(idled).toBe(0);
      active = true;
      now += 100;
      deadline.fire();
      await sleep(100);
      expect(checks).toHaveBeenCalled();
      expect(idled).toBe(0);
      expect(deadline.count()).toBe(0); // No idle timer while custody remains active.
      active = false;
      // A genuine actor settlement signal starts a fresh one-shot idle window.
      const actor = await host.actors.create({ name: "settled", instructions: "wait", residency: "durable" });
      await host.actors.stop(actor.id);
      expect(await waitUntil(() => deadline.count() === 1)).toBe(true);
      now += 30_100;
      deadline.fire();
      expect(await waitUntil(() => idled === 1)).toBe(true);
    } finally { clock.mockRestore(); await host.close(); vi.restoreAllMocks(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it("counts the full idle window from an actor run's settlement, not its last periodic sample", async () => {
    let idled = 0;
    const { root, host } = fixture(() => { idled++; });
    let now = Date.now();
    const clock = vi.spyOn(Date, "now").mockImplementation(() => now);
    const deadline = idleDeadlineDriver();
    try {
      await host.start();
      expect(await waitUntil(() => deadline.count() === 1)).toBe(true);
      now += 29_000;
      deadline.fire();
      expect(await waitUntil(() => deadline.count() === 1)).toBe(true);
      const runs = vi.spyOn(host.agents, "run").mockImplementation(async (_request, _signal, onSpawned) => {
        onSpawned?.({ id: "run-between-events" } as never);
        return { id: "run-between-events", status: "completed", text: "done", toolCalls: 0 } as never;
      });
      await host.participants.refresh(); // The wall-clock jump must not expire the host's custody lease.
      const actor = await host.actors.create({ name: "between-events", instructions: "wait", residency: "durable" });
      host.actors.tell(actor.id, "go");
      expect(await waitUntil(() => runs.mock.calls.length === 1 && ["idle", "dormant"].includes(host.actors.status(actor.id).status))).toBe(true);
      await host.actors.stop(actor.id);
      expect(await waitUntil(() => deadline.count() === 1)).toBe(true);
      const ended = now;
      now = ended + 29_900;
      deadline.fire();
      expect(await waitUntil(() => deadline.count() === 1)).toBe(true);
      expect(idled).toBe(0);
      now = ended + 30_100;
      deadline.fire();
      expect(await waitUntil(() => idled === 1)).toBe(true);
    } finally { clock.mockRestore(); await host.close(); vi.restoreAllMocks(); fs.rmSync(root, { recursive: true, force: true }); }
  });

});
