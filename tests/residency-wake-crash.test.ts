import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { EventLog } from "../src/mesh/event-log.js";
import { MeshArchive } from "../src/mesh/archive.js";
import { MeshStore } from "../src/mesh/store.js";
import { residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import * as wake from "../src/residency/wake.js";
import * as index from "../src/residency/wake-index.js";

describe("resident wake recovery after publisher death", () => {
  it.each([
    ["single", "next publish"], ["batch", "next publish"],
    ["single", "startup"], ["batch", "startup"],
  ] as const)("recovers a %s commit exactly once on %s with no publisher-local state", async (mode, recovery) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-wake-crash-"));
    const meshRoot = path.join(root, "mesh");
    const resident = residentRoot(meshRoot, "session:crashed-publisher");
    const saved = { rootId: "session:crashed-publisher", residencyRoot: resident, cwd: root };
    fs.mkdirSync(resident, { recursive: true });
    fs.writeFileSync(path.join(resident, "config.json"), JSON.stringify(saved));
    fs.writeFileSync(path.join(resident, "wake-routes.json"), JSON.stringify({
      format: 1, rootId: saved.rootId, hostId: "host:listener", configJson: index.canonicalResidentWakeConfig(saved),
      actors: [{ id: "listener", name: "listener", topics: ["crash.delivery"] }],
    }));
    const publisher = new MeshStore(meshRoot, 65_536, 100);
    let recovered: MeshStore | undefined;
    const hosts: Array<{ start(): Promise<void>; close(): Promise<void> }> = [];
    const from = { id: "publisher", name: "publisher", kind: "main" as const };
    try {
      await wake.ensureResidentWakeArchive(publisher);
      // Process-death seam: the archive/live commit completed, but control never
      // returns to MeshStore's wake dispatch. No root-local failure handler runs.
      const death = new Error("publisher died after commit before wake dispatch");
      if (mode === "single") {
        const publish = EventLog.prototype.publish;
        vi.spyOn(EventLog.prototype, "publish").mockImplementationOnce(async function(this: EventLog, input) {
          await publish.call(this, input);
          throw death;
        });
      } else {
        const publishBatch = EventLog.prototype.publishBatch;
        vi.spyOn(EventLog.prototype, "publishBatch").mockImplementationOnce(async function(this: EventLog, inputs) {
          await publishBatch.call(this, inputs);
          throw death;
        });
      }
      const packet = { topic: "crash.delivery", from, durable: true };
      await expect(mode === "single" ? publisher.publish(packet) : publisher.publishBatch([packet])).rejects.toBe(death);
      const [event] = publisher.read();
      expect(event).toBeDefined();
      expect(publisher.read()).toHaveLength(1);
      expect(MeshArchive.fromRoot(meshRoot)?.lookupEntry(event!.sequence)?.event.id).toBe(event!.id);
      expect(fs.existsSync(wake.residentWakeRequestPath(resident))).toBe(false);
      expect(fs.existsSync(path.join(resident, "wake-failure.json"))).toBe(false);
      expect(index.residentUnacknowledgedDeliveries(meshRoot).get(resident)).toEqual({ id: event!.id, sequence: event!.sequence });
      publisher.closeState();
      vi.restoreAllMocks();
      vi.resetModules(); // recovery must use the on-disk watermark, not the dead writer's cache
      const freshIndex = await import("../src/residency/wake-index.js");
      const freshWake = await import("../src/residency/wake.js");
      const { MeshStore: FreshStore } = await import("../src/mesh/store.js");
      recovered = new FreshStore(meshRoot, 65_536, 100);
      expect(freshIndex.residentUnacknowledgedDeliveries(meshRoot).get(resident)?.id).toBe(event!.id);
      const launch = vi.fn(async () => {
        expect(freshWake.readWakeJson(freshWake.residentWakeRequestPath(resident))).toMatchObject({ id: event!.id, sequence: event!.sequence });
        fs.writeFileSync(freshWake.residentSleepingPath(resident), JSON.stringify({
          request: freshWake.readWakeJson(freshWake.residentWakeRequestPath(resident)),
        }));
      });
      const dispatch = freshWake.wakeResidentActors;
      const drain = vi.spyOn(freshWake, "wakeResidentActors").mockImplementation((mesh, events) => {
        // Startup proof cannot accidentally pass via participant presence publication.
        if (recovery === "startup" && events.length) return Promise.resolve();
        return dispatch(mesh, events, launch);
      });
      if (recovery === "next publish") {
        await recovered.publish({ topic: "unrelated.recovery", from });
        expect(launch).toHaveBeenCalledOnce();
        await recovered.publish({ topic: "unrelated.after-recovery", from });
      } else {
        const { ResidentHost } = await import("../src/residency/host.js");
        const config: ResidentHostConfig = {
          format: 1, rootId: "session:recovery", sessionId: "recovery", cwd: root, projectRoot: root,
          meshRoot, actorRoot: path.join(root, "actors"), residencyRoot: residentRoot(meshRoot, "session:recovery"),
          fullCodeMode: false, agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 }, mesh: DEFAULT_FABRIC_CONFIG.mesh,
          retention: DEFAULT_FABRIC_CONFIG.retention, workerPath: "unused", fabricExtensionPath: "unused",
          piBinary: "unused", claudeBinary: "unused", vedaBinary: "unused",
        };
        const first = new ResidentHost(config, () => {});
        hosts.push(first);
        await first.start();
        expect(launch).toHaveBeenCalledOnce();
        expect(drain.mock.calls.some(([, events]) => events.length === 0)).toBe(true);
        await first.close();
        const restarted = new ResidentHost(config, () => {});
        hosts.push(restarted);
        await restarted.start();
      }
      expect(launch).toHaveBeenCalledOnce();
      expect(freshIndex.residentUnacknowledgedDeliveries(meshRoot).size).toBe(0);
      expect(recovered.read({ topic: "crash.delivery" })).toEqual([event]);
    } finally {
      for (const host of hosts) await host.close();
      recovered?.closeState(); publisher.closeState();
      vi.restoreAllMocks();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
});
