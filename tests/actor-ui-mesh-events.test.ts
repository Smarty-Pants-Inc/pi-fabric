import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ActorManager } from "../src/actors/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";

describe("actor-monitor UI mesh notifications (#7791)", () => {
  it("uses existing monitor events even when no actor subscribes, isolates observer errors, and unsubscribes", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-ui-mesh-events-"));
    const identity = { id: "session:ui-events", name: "main", kind: "main" as const, sessionId: "ui-events" };
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const agents = new AgentManager(root, DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
    });
    const actors = new ActorManager("ui-events", identity, mesh,
      { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 10 }, agents, () => {}, {
        actorRoot: path.join(root, "actors"), persistent: false, reapDeadSessionPresence: false,
        meshRetentionSweepPath: false, closeGraceMs: 0,
      });
    const observed = vi.fn();
    const changed = vi.fn();
    const broken = actors.subscribeMesh(() => { throw new Error("UI observer failed"); });
    const unsubscribe = actors.subscribeMesh(observed);
    actors.subscribe(changed);
    try {
      await new Promise(resolve => setImmediate(resolve)); // Drain the existing monitor's initial observation.
      await mesh.publish({ topic: "test.ui-notification", kind: "message", from: identity, text: "first" });
      await vi.waitFor(() => expect(observed).toHaveBeenCalledTimes(1), { timeout: 2_000 });
      expect(changed).not.toHaveBeenCalled(); // No local actor transition to hide missing mesh wiring.
      unsubscribe(); broken();
      const second = vi.fn();
      actors.subscribeMesh(second);
      await mesh.publish({ topic: "test.ui-notification", kind: "message", from: identity, text: "second" });
      await vi.waitFor(() => expect(second).toHaveBeenCalledTimes(1), { timeout: 2_000 });
      expect(observed).toHaveBeenCalledTimes(1);
    } finally {
      await actors.close();
      await agents.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
});
