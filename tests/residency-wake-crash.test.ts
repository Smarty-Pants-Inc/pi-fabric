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

/** These are integration tests, not a fleet E2E: publisher death between the
 * durable commit and wake dispatch cannot be produced on the real fleet path
 * without fault injection inside MeshStore.publish. The injected throw at its
 * awaited EventLog commit seam runs the real MeshStore, resident host and filesystem.
 * Ordinary delivery is covered by the compiled-launcher residency E2E canary in
 * tests/residency.test.ts ("durable participant residency", #2726 queued tells).
 */
describe("resident wake recovery after publisher death", () => {
  it.each([
    ["single", "next publish"], ["batch", "next publish"],
    ["single", "startup"], ["batch", "startup"],
  ] as const)("recovers a %s commit exactly once on %s with no publisher-local state", async (mode, recovery) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-wake-crash-"));
    const meshRoot = path.join(root, "mesh");
    const resident = residentRoot(meshRoot, "session:crashed-publisher");
    const config: ResidentHostConfig = {
      format: 1, rootId: "session:crashed-publisher", sessionId: "crashed-publisher", cwd: root, projectRoot: root,
      meshRoot, actorRoot: path.join(root, "actors"), residencyRoot: resident,
      fullCodeMode: false, agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 }, mesh: DEFAULT_FABRIC_CONFIG.mesh,
      retention: DEFAULT_FABRIC_CONFIG.retention, workerPath: "unused", fabricExtensionPath: "unused",
      piBinary: "unused", claudeBinary: "unused", vedaBinary: "unused",
    };
    const saved = config;
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
      if (recovery === "startup") {
        // Startup may recover only its OWN registry-owned actors, never another host.
        const { ResidentHost } = await import("../src/residency/host.js");
        const seed = new ResidentHost(config, () => {});
        hosts.push(seed);
        await seed.start();
        const actor = await seed.actors.create({ name: "crash listener", instructions: "listen", residency: "durable", topics: ["crash.delivery"] });
        await vi.waitFor(() => expect(index.retainedRoutesAt(resident)?.actors.some(route => route.id === actor.id)).toBe(true), { timeout: 3_000 });
        seed.actors.pauseForRelease();
        await seed.actors.checkpointForRelease();
        await seed.close();
      }
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
      const [event] = publisher.read({ topic: "crash.delivery" });
      expect(event).toBeDefined();
      expect(publisher.read({ topic: "crash.delivery" })).toHaveLength(1);
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
      const drain = vi.spyOn(freshWake, "wakeResidentActors").mockImplementation((mesh, events, _launch, recoveryOptions) => {
        // Startup proof cannot accidentally pass via participant presence publication.
        if (recovery === "startup" && events.length) return Promise.resolve();
        return dispatch(mesh, events, launch, recoveryOptions);
      });
      if (recovery === "next publish") {
        await recovered.publish({ topic: "unrelated.recovery", from });
        expect(launch).toHaveBeenCalledOnce();
        await recovered.publish({ topic: "unrelated.after-recovery", from });
      } else {
        const { ResidentHost } = await import("../src/residency/host.js");
        const atomic = await import("../src/core/atomic-write.js");
        const writes = vi.spyOn(atomic, "writeJsonAtomic");
        const first = new ResidentHost(config, () => {});
        hosts.push(first);
        await first.start();
        expect(launch).not.toHaveBeenCalled(); // already-live OWN owner, no second process
        expect(freshWake.readWakeJson(freshWake.residentWakeRequestPath(resident))).toMatchObject({ id: event!.id, sequence: event!.sequence });
        expect(drain.mock.calls.some(([, events, , options]) => events.length === 0 && options?.config.rootId === config.rootId)).toBe(true);
        expect(writes.mock.calls.filter(([file]) => file === freshWake.residentWakeRequestPath(resident))).toHaveLength(1);
        await first.close();
        const restarted = new ResidentHost(config, () => {});
        hosts.push(restarted);
        await restarted.start();
      }
      expect(launch).toHaveBeenCalledTimes(recovery === "startup" ? 0 : 1);
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
