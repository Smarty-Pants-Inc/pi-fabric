import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import { DEFAULT_PARTICIPANT_LEASE_MS } from "../src/topology/participant-directory.js";

const roots: string[] = [];
const waitFor = async (predicate: () => boolean, timeoutMs = 5_000): Promise<void> => {
  const until = performance.now() + timeoutMs;
  while (!predicate()) {
    if (performance.now() >= until) throw new Error("condition did not settle before timeout");
    await new Promise(resolve => setTimeout(resolve, 10));
  }
};

afterEach(() => {
  vi.restoreAllMocks();
  vi.useRealTimers();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("ActorManager ownership notifications", () => {
  // Recorded real catch: PR #1164 removed the only wake for already-accepted backlog.
  // Real managers/registry/watches, Node .mjs fake worker only; normal fleet/deletion guards apply.
  it.each(["registry", "lease", "state", "crash"] as const)("adopts accepted backlog on a %s edge, never while its owner lives", async (edge) => {
    // Keep worker I/O real; drive only the captured ownership deadline and wall-clock seam.
    const timeouts = vi.spyOn(globalThis, "setTimeout");
    const realNow = Date.now;
    let elapsed = 0;
    if (edge === "crash") vi.spyOn(Date, "now").mockImplementation(() => realNow() + elapsed);
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-adoption-edge-"));
    roots.push(root);
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      runRoot: path.join(root, "runs"), workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
    });
    const run = vi.spyOn(agents, "run");
    let ownerAlive = true;
    let actorId: string | undefined;
    let ownerDeadline = Date.now() + DEFAULT_PARTICIPANT_LEASE_MS;
    const live = () => ownerAlive || edge === "crash" && Date.now() <= ownerDeadline;
    const host = (name: string) => new ActorManager(name,
      { id: `session:${name}`, name, kind: "main", sessionId: name }, mesh,
      { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {}, {
        actorRoot: path.join(root, "actors"), persistent: true, rootId: `session:${name}`, claimResidency: "session",
        releasePaused: name === "owner", adoptionGraceMs: 0,
        canManageActor: () => name === "owner" ? live() : undefined,
        lineageAlive: id => id !== "session:owner" || live(),
        ...(edge === "crash" ? {
          snapshotActorOwnership: (_fresh?: boolean, ids?: readonly string[]) => {
            const deadline = live() ? ownerDeadline : Number.POSITIVE_INFINITY;
            return Object.assign(
              new Map((ids ?? (actorId ? [actorId] : [])).flatMap(id => live() ? [[id, name === "owner"] as const] : [])),
              { observedDeadline: () => deadline },
            );
          },
        } : {}),
        meshCursorPath: path.join(root, `cursor-${name}.json`),
      });
    const owner = host("owner");
    let successor: ActorManager | undefined;
    try {
      const actor = await owner.create({ name: "receiver", instructions: "Work.", responseMode: "text", coalesce: false });
      actorId = actor.id;
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
        // No close, registry mutation, lease write or mesh event. Only the observed lease expiry.
        let index = -1;
        for (let candidate = 0; candidate < timeouts.mock.calls.length; candidate++) {
          const ms = timeouts.mock.calls[candidate]?.[1];
          if (typeof ms === "number" && ms > 30_000 && ms <= DEFAULT_PARTICIPANT_LEASE_MS + 1) index = candidate;
        }
        expect(index).toBeGreaterThanOrEqual(0);
        const [wake, delay] = timeouts.mock.calls[index]!;
        const handle = timeouts.mock.results[index]!.value as NodeJS.Timeout;
        expect(handle.hasRef()).toBe(false);
        expect(delay).toBeGreaterThan(80_000); // not the old fixed 60 s interval
        const observedDeadline = ownerDeadline;
        // A last renewal was not notified before the crash. The first expiry wake must
        // observe it and re-arm once, not adopt early or restart a fixed interval.
        ownerDeadline += 17_000;
        elapsed = observedDeadline - realNow() - 1;
        expect(run).not.toHaveBeenCalled();
        expect(registryRoot()).toBe("session:owner");
        clearTimeout(handle);
        elapsed += 2;
        (wake as () => void)();
        await new Promise(resolve => setTimeout(resolve, 50));
        expect(run).not.toHaveBeenCalled();
        expect(registryRoot()).toBe("session:owner");
        let rearmed = -1;
        for (let candidate = 0; candidate < timeouts.mock.calls.length; candidate++) {
          const ms = timeouts.mock.calls[candidate]?.[1];
          if (typeof ms === "number" && ms > 15_000 && ms <= 17_001) rearmed = candidate;
        }
        expect(rearmed).toBeGreaterThan(index);
        const nextHandle = timeouts.mock.results[rearmed]!.value as NodeJS.Timeout;
        expect(nextHandle.hasRef()).toBe(false);
        clearTimeout(nextHandle);
        elapsed = ownerDeadline - realNow() + 1;
        (timeouts.mock.calls[rearmed]![0] as () => void)();
      }
      if (edge === "lease") fs.writeFileSync(path.join(leases, "owner.json"), '{"dead":true}');
      if (edge === "state") await mesh.put({ key: "test/owner-dead", value: true,
        identity: { id: "session:owner", name: "owner", kind: "main" } });
      // No refreshing status/list/owns call: those reads could mask the missing wake.
      await waitFor(() => run.mock.calls.length === 2);
      expect(run).toHaveBeenCalledTimes(2);
      await waitFor(() => successor!.messages(actor.id).filter(message => message.direction === "out" && !message.error).length === 2);
      expect(successor!.messages(actor.id).filter(message => message.direction === "out" && !message.error)).toHaveLength(2);
      expect(registryRoot()).toBe("session:successor");
      if (edge === "crash") expect(Date.now() - (ownerDeadline - DEFAULT_PARTICIPANT_LEASE_MS - 17_000)).toBeLessThan(120_000);
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
