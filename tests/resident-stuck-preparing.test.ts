import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { installInProcessResidentFence } from "./helpers/in-process-resident-fence.js";
import { stuckPreparingPath } from "../src/actors/stuck-preparing.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ResidentHost } from "../src/residency/host.js";
import { RESIDENT_HOST_FORMAT, residentDeliveryPrefix, type ResidentHostConfig } from "../src/residency/protocol.js";

beforeEach(() => installInProcessResidentFence());

const waitFor = async (predicate: () => boolean, ms = 8_000): Promise<void> => {
  const deadline = Date.now() + ms;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error("Timed out waiting for the stuck-preparing notice");
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
};

// smarty-dev#6337: the resident host's maintenance tick, not a failure, reports a hung preparation.
describe("resident host stuck-preparing notice (smarty-dev#6337)", () => {
  it("sends one durable owner notice to the root Main and one metrics line per episode", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-resident-stuck-"));
    const config: ResidentHostConfig = {
      format: RESIDENT_HOST_FORMAT, rootId: "session:stuck-main", sessionId: "stuck-main",
      cwd: process.cwd(), projectRoot: process.cwd(), meshRoot: path.join(root, "mesh"),
      actorRoot: path.join(root, "actors"), residencyRoot: path.join(root, "resident"),
      fullCodeMode: true,
      agents: { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 5_000, budgetUsd: 0, stuckPreparingMs: 1_000 },
      mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 },
      retention: DEFAULT_FABRIC_CONFIG.retention,
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
      fabricExtensionPath: path.resolve("dist/index.js"), piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
      piModels: { available: [{ provider: "fixture", id: "visible" }], aliases: {}, defaultModel: "fixture/visible" },
    };
    const host = new ResidentHost(config);
    let release!: () => void;
    const gate = new Promise<void>((resolve) => { release = resolve; });
    try {
      await host.start();
      const actor = await host.actors.create({ name: "stuck-advisor", instructions: "Advise.", residency: "durable",
        transport: "process", responseMode: "directive", delivery: "followUp", triggerTurn: true });
      // Model admission hangs before its own deadline is armed: the activation never fails.
      const admit = host.agents.prepareModelForAdmission.bind(host.agents);
      vi.spyOn(host.agents, "prepareModelForAdmission").mockImplementation(async (...args) => { await gate; return admit(...args); });
      host.actors.tell(actor.id, "hang in preparation");
      const prefix = residentDeliveryPrefix(config.rootId);
      const stuck = () => host.mesh.listAll(prefix).map((entry) => (entry.value as { message: string; delivery: string; triggerTurn: boolean }))
        .filter((record) => record.message.includes("stuck in preparing"));
      await waitFor(() => stuck().length > 0);
      expect(host.actors.status(actor.id).status).toBe("preparing");
      expect(stuck()[0]).toMatchObject({ delivery: "followUp", triggerTurn: true });
      expect(stuck()[0]!.message).toMatch(/^Fabric host notice: actor stuck-advisor stuck in preparing for \d+ min \(since \d{4}-\d\d-\d\dT[^)]+Z\); its queued events are waiting\./);
      // Further maintenance ticks in the same episode stay silent.
      await new Promise((resolve) => setTimeout(resolve, 2_500));
      expect(stuck()).toHaveLength(1);
      const lines = fs.readFileSync(stuckPreparingPath(config.meshRoot), "utf8").trim().split("\n").map((line) => JSON.parse(line));
      expect(lines).toEqual([expect.objectContaining({ actorId: actor.id, actorName: "stuck-advisor", rootId: config.rootId })]);
      release();
      await waitFor(() => host.actors.status(actor.id).status === "idle" && host.actors.inFlightCount() === 0);
    } finally {
      release();
      await host.close();
      fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
    }
  }, 30_000);
});
