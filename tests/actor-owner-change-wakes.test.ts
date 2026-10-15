import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";

const roots: string[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  vi.useRealTimers();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("ActorManager ownership notifications", () => {
  // Recorded real catch: PR #1164 removed the only wake for already-accepted backlog.
  // Real managers/registry/watches, Node .mjs fake worker only; normal fleet/deletion guards apply.
  it.each(["registry", "lease", "state", "crash"] as const)("adopts accepted backlog on a %s edge, never while its owner lives", async (edge) => {
    if (edge === "crash") vi.useFakeTimers({ toFake: ["setInterval", "clearInterval"] });
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-adoption-edge-"));
    roots.push(root);
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      runRoot: path.join(root, "runs"), workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
    });
    const run = vi.spyOn(agents, "run");
    let ownerAlive = true;
    const host = (name: string) => new ActorManager(name,
      { id: `session:${name}`, name, kind: "main", sessionId: name }, mesh,
      { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {}, {
        actorRoot: path.join(root, "actors"), persistent: true, rootId: `session:${name}`, claimResidency: "session",
        releasePaused: name === "owner", adoptionGraceMs: 0,
        canManageActor: () => name === "owner" ? ownerAlive : undefined,
        lineageAlive: id => id !== "session:owner" || ownerAlive,
        meshCursorPath: path.join(root, `cursor-${name}.json`),
      });
    const owner = host("owner");
    let successor: ActorManager | undefined;
    try {
      const actor = await owner.create({ name: "receiver", instructions: "Work.", responseMode: "text", coalesce: false });
      owner.tell(actor.id, "backlog-a");
      owner.tell(actor.id, "backlog-b");
      successor = host("successor");
      successor.resumeQueued(); // providers/directory bootstrap completes while owner is still alive
      const registryRoot = () => JSON.parse(fs.readFileSync(path.join(root, "actors", "actors.json"), "utf8")).actors[0].rootId;
      const leases = path.join(mesh.root, "host-leases");
      fs.mkdirSync(leases, { recursive: true });
      fs.writeFileSync(path.join(leases, "owner.json"), "{}");
      await new Promise(resolve => setTimeout(resolve, 50)); // deliver the real live-owner notification
      expect(registryRoot()).toBe("session:owner");
      expect(run).not.toHaveBeenCalled();
      if (edge !== "crash") await owner.close();
      if (edge !== "registry") await new Promise(resolve => setTimeout(resolve, 50)); // drain all existing edges while still live
      ownerAlive = false;
      if (edge === "crash") {
        // No close, registry mutation, lease write or mesh event. Only the silent-death safety net.
        await vi.advanceTimersByTimeAsync(59_999);
        expect(run).not.toHaveBeenCalled();
        await vi.advanceTimersByTimeAsync(1);
      }
      if (edge === "lease") fs.writeFileSync(path.join(leases, "owner.json"), '{"dead":true}');
      if (edge === "state") await mesh.put({ key: "test/owner-dead", value: true,
        identity: { id: "session:owner", name: "owner", kind: "main" } });
      // No refreshing status/list/owns call: those reads could mask the missing wake.
      await vi.waitFor(() => expect(run).toHaveBeenCalledTimes(2), { timeout: 5_000 });
      await vi.waitFor(() => expect(successor!.messages(actor.id).filter(message => message.direction === "out" && !message.error)).toHaveLength(2), { timeout: 5_000 });
      expect(registryRoot()).toBe("session:successor");
      expect(run.mock.calls.map(([request]) => request.task).filter(task => task.includes("backlog-a"))).toHaveLength(1);
      expect(run.mock.calls.map(([request]) => request.task).filter(task => task.includes("backlog-b"))).toHaveLength(1);
    } finally {
      await owner.close();
      await successor?.close();
      await agents.close();
      mesh.closeState();
    }
  });
});
