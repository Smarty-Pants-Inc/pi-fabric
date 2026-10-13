import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { installInProcessResidentFence } from "./helpers/in-process-resident-fence.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshLockTimeoutError } from "../src/core/atomic-write.js";
import { ResidentHost } from "../src/residency/host.js";
import { RESIDENT_HOST_FORMAT, type ResidentHostConfig } from "../src/residency/protocol.js";
import type { MeshIdentity } from "../src/mesh/store.js";

beforeEach(() => installInProcessResidentFence());

const turn = async () => { for (let i = 0; i < 20; i++) await new Promise(resolve => setImmediate(resolve)); };
const waitFor = async (predicate: () => boolean, ms = 2_000): Promise<number> => {
  const start = Date.now();
  while (!predicate()) {
    if (Date.now() - start >= ms) throw new Error("timed out");
    await new Promise(resolve => setTimeout(resolve, 5));
  }
  return Date.now() - start;
};
const from: MeshIdentity = { id: "session:forwarder", name: "forwarder", kind: "main", sessionId: "forwarder" };

// Real ResidentHost (production constructor: ActorDirectory + MeshStore + AgentManager).
describe("resident host admission regain wakes owned-topic actors", () => {
  it("retains an owned-topic event while a real failed refresh closes the gate and claims it once on recovery", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-resident-regain-"));
    const config: ResidentHostConfig = {
      format: RESIDENT_HOST_FORMAT, rootId: "session:regain-main", sessionId: "regain-main",
      cwd: process.cwd(), projectRoot: process.cwd(), meshRoot: path.join(root, "mesh"),
      actorRoot: path.join(root, "actors"), residencyRoot: path.join(root, "resident"),
      fullCodeMode: true,
      agents: { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 5_000, budgetUsd: 0 },
      mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 600_000 },
      retention: DEFAULT_FABRIC_CONFIG.retention,
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
      fabricExtensionPath: path.resolve("dist/index.js"), piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
      piModels: { available: [{ provider: "fixture", id: "visible" }], aliases: {}, defaultModel: "fixture/visible" },
    };
    const host = new ResidentHost(config);
    let release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    const timeline: Record<string, number> = {};
    try {
      await host.start();
      const t0 = Date.now();
      const actor = await host.actors.create({ name: "regain-advisor", instructions: "Advise.", residency: "durable",
        transport: "process", responseMode: "directive", delivery: "followUp", triggerTurn: true, topics: ["regain.work"] });
      // Hold model admission so the activation stays in preparing: no settlement or prune path runs.
      const admit = host.agents.prepareModelForAdmission.bind(host.agents);
      const admission = vi.spyOn(host.agents, "prepareModelForAdmission").mockImplementation(async (...args) => { await gate; return admit(...args); });
      await turn();
      expect(host.participants.canConsumeMesh()).toBe(true);
      expect(host.actors.status(actor.id).status).toBe("idle");

      // Join pending actor presence, then settle to a steady heartbeat, so the injection belongs to the controlled refresh.
      await host.participants.refreshPresence();
      await host.participants.refresh();
      await host.participants.refresh();
      // Real failed committed refresh: typed mesh-lock timeout at the commit points
      // (confirmWritable for a steady confirm, writeBatch for the host's publication batch).
      const failure = new MeshLockTimeoutError(" (test holder)", 1, 0);
      const confirm = vi.spyOn(host.participants.mesh, "confirmWritable").mockRejectedValue(failure);
      const batch = vi.spyOn(host.participants.mesh, "writeBatch").mockRejectedValue(failure);
      await expect(host.participants.refresh()).rejects.toBe(failure);
      timeline.injectedConfirmCalls = confirm.mock.calls.length;
      timeline.injectedBatchCalls = batch.mock.calls.length;
      confirm.mockRestore();
      batch.mockRestore();
      expect(host.participants.canConsumeMesh()).toBe(false);
      timeline.gateFalseMs = Date.now() - t0;

      // Real matched publication while not admitted; native watch fires, gate refuses.
      await host.mesh.publish({ topic: "regain.work", kind: "work", from, text: "retained" });
      await turn();
      await new Promise(resolve => setTimeout(resolve, 100));
      // Counterexample: before the edge, nothing is claimed.
      expect(host.participants.canConsumeMesh()).toBe(false);
      expect(admission).not.toHaveBeenCalled();
      expect(host.actors.status(actor.id).status).toBe("idle");
      timeline.retainedMs = Date.now() - t0;

      // Recovering real refresh: production onCommittedReady -> scheduleMeshWake. No publish, no test schedule.
      await host.participants.refresh();
      expect(host.participants.canConsumeMesh()).toBe(true);
      timeline.commitMs = Date.now() - t0;
      timeline.preparingAfterCommitMs = await waitFor(() => host.actors.status(actor.id).status === "preparing");
      timeline.admissionAfterCommitMs = await waitFor(() => admission.mock.calls.length > 0, 1_500).catch(() => Number.NaN);
      const claims = admission.mock.calls.length;
      expect(claims).toBeLessThanOrEqual(1);
      expect(Date.now() - t0).toBeLessThan(4_000);   // within the 5 s participant heartbeat

      // Steady next refresh: no edge, no duplicate claim.
      await host.participants.refresh();
      await turn();
      await new Promise(resolve => setTimeout(resolve, 100));
      expect(admission).toHaveBeenCalledTimes(claims);
      expect(host.actors.status(actor.id).status).toBe("preparing");
      timeline.admissionCalls = claims;
      timeline.totalMs = Date.now() - t0;
      console.log(`REGAIN_TIMELINE ${JSON.stringify(timeline)}`);
    } finally {
      release();
      await host.close();
      fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
    }
  }, 30_000);
});
