import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ResidentHost } from "../src/residency/host.js";
import { RESIDENT_HOST_FORMAT, residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";

// smarty-dev#6729: an idle resident host's 50 ms request poll rebuilt the fleet-wide actor
// ownership view (every project participant, every host lease) on every tick for its idle check.
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
  fs.writeFileSync(path.join(config.residencyRoot, "config.json"), JSON.stringify(config));
  return { root, config, host: new ResidentHost(config, onIdle) };
};

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

describe("idle resident host polling (smarty-dev#6729)", () => {
  it("rebuilds the actor ownership view about once a second, not on every 50 ms request tick", async () => {
    const { root, host } = fixture();
    try {
      await host.start();
      await host.actors.create({ name: "idle-poll", instructions: "wait", residency: "durable" });
      await host.participants.refresh();
      const checks = vi.spyOn(host.actors, "hasActiveDurableActor");
      const lists = vi.spyOn(host.participants, "list");
      const reads = vi.spyOn(fs, "readFileSync");
      const started = Date.now();
      await sleep(2_000);
      const seconds = (Date.now() - started) / 1_000;
      const configReads = reads.mock.calls.filter(([file]) => String(file) === path.join(host.config.residencyRoot, "config.json")).length;
      // Before: one check (and one fleet-wide participant listing) per 50 ms tick, about 40 here.
      expect(checks.mock.calls.length).toBeLessThanOrEqual(Math.ceil(seconds) + 1);
      expect(lists.mock.calls.length).toBeLessThanOrEqual(Math.ceil(seconds) * 3 + 2);
      // Before: the 100 ms maintenance tick re-read and parsed config.json each time, about 20 here.
      expect(configReads).toBeLessThanOrEqual(1);
    } finally {
      await host.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it("applies a replaced config.json retention overlay at the next maintenance tick", async () => {
    const { root, config, host } = fixture();
    try {
      await host.start();
      await sleep(300);
      const file = path.join(config.residencyRoot, "config.json");
      const retention = { ...config.retention, completedRequestMs: (config.retention as { completedRequestMs?: number }).completedRequestMs ?? 1 };
      const replaced = { ...config, retention: { ...retention, probeMarker: 7 } };
      fs.writeFileSync(file + ".tmp", JSON.stringify(replaced));
      fs.renameSync(file + ".tmp", file);
      const reads = vi.spyOn(fs, "readFileSync");
      await sleep(400);
      const configReads = reads.mock.calls.filter(([read]) => String(read) === file).length;
      expect(configReads).toBeGreaterThanOrEqual(1);
      expect(configReads).toBeLessThanOrEqual(2);
    } finally {
      await host.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it("never exits on a reused actor observation: an idle exit is confirmed by a current check", async () => {
    let idled = 0;
    const { root, host } = fixture(() => { idled++; });
    let now = Date.now();
    try {
      await host.start();
      const clock = vi.spyOn(Date, "now").mockImplementation(() => now);
      let active = false;
      const checks = vi.spyOn(host.actors, "hasActiveDurableActor").mockImplementation(() => active);
      // Just short of the idle window: the next tick takes a current (inactive) observation.
      now += 29_950;
      await sleep(200);
      expect(idled).toBe(0);
      // An actor wakes; the reused observation still says inactive, and the window has elapsed.
      active = true;
      const before = checks.mock.calls.length;
      now += 100;
      await sleep(200);
      expect(checks.mock.calls.length).toBeGreaterThan(before);
      expect(idled).toBe(0);
      // Truly idle for the whole window again: the host exits.
      active = false;
      now += 30_100;
      await sleep(200);
      expect(idled).toBeGreaterThan(0);
      clock.mockRestore();
    } finally {
      await host.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
});
