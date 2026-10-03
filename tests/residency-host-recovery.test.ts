import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ResidentHost } from "../src/residency/host.js";
import { readHostLease } from "../src/topology/host-leases.js";
import type { ResidentHostConfig } from "../src/residency/protocol.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";

const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-recovery-"));
  const config: ResidentHostConfig = {
    format: 1, rootId: "session:recovery", sessionId: "recovery", cwd: root, projectRoot: root,
    meshRoot: path.join(root, "mesh"), actorRoot: path.join(root, "actors"), residencyRoot: path.join(root, "resident"),
    fullCodeMode: true, agents: { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: true },
    mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, retention: DEFAULT_FABRIC_CONFIG.retention,
    workerPath: "worker.js", fabricExtensionPath: "index.js", piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
  };
  return { root, config, host: new ResidentHost(config) };
};

describe("resident recovery startup and lease fence (#3864)", () => {
  it("publishes a lease without reading a synthetic 10k-run / 8GB archive", { timeout: 30_000 }, async () => {
    const { root, config, host } = fixture();
    const runs = path.join(config.residencyRoot, "runs");
    const status = path.join(root, "terminal.json");
    fs.writeFileSync(status, JSON.stringify({ status: "completed", text: "archived", updatedAt: 1 }));
    for (let i = 0; i < 10_000; i++) {
      const run = path.join(runs, `archived-${i}`);
      fs.mkdirSync(run, { recursive: true });
      fs.copyFileSync(status, path.join(run, "status.json"));
      const fd = fs.openSync(path.join(run, "events.jsonl"), "w");
      fs.ftruncateSync(fd, 800_000); // sparse: 8 GB logical archive, cheap physical fixture
      fs.closeSync(fd);
    }
    const read = fs.readFileSync;
    let archiveReads = 0, beforeLease = 0;
    const spy = vi.spyOn(fs, "readFileSync").mockImplementation((...args: Parameters<typeof fs.readFileSync>) => {
      if (String(args[0]).startsWith(runs + path.sep)) {
        archiveReads++;
        if (!fs.existsSync(path.join(config.residencyRoot, "owner.json"))) beforeLease++;
      }
      return read(...args);
    });
    try {
      const started = performance.now();
      await host.start();
      const elapsed = performance.now() - started;
      console.info(`3864 startup: 10000 runs / 8000000000 logical bytes, ${elapsed.toFixed(1)} ms, ${archiveReads} archive reads`);
      expect(archiveReads).toBe(0); // structural check, not a flaky millisecond CI threshold
      expect(readHostLease(config.meshRoot, host.hostId)?.expiresAt).toBeGreaterThan(Date.now());
      expect(fs.existsSync(path.join(config.residencyRoot, "owner.json"))).toBe(true);
      await vi.waitFor(() => expect(archiveReads).toBeGreaterThan(0));
      expect(beforeLease).toBe(0);
    } finally { spy.mockRestore(); await host.close(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it.each(["confirmWritable", "writeBatch"] as const)("stops actor, control and lifecycle cursor reads on failed %s renewal, then replays after recovery", async (renewal) => {
    const { root, config, host } = fixture();
    let writes: ReturnType<typeof vi.spyOn> | undefined;
    let confirmations: ReturnType<typeof vi.spyOn> | undefined;
    let tail: ReturnType<typeof vi.spyOn> | undefined;
    let lifecycle: ReturnType<typeof vi.spyOn> | undefined;
    try {
      await host.start();
      await new Promise(resolve => setTimeout(resolve, 50));
      // Unchanged heartbeats certify the shared lock without a batch (#411).
      // Stall both acquisition paths so retries cannot reopen the cursor fence.
      writes = vi.spyOn(host.mesh, "writeBatch").mockRejectedValue(new Error("lease write stalled"));
      confirmations = vi.spyOn(host.mesh, "confirmWritable").mockRejectedValue(new Error("lease write stalled"));
      if (renewal === "writeBatch") {
        // A new participant is a real change, forcing the locked write branch.
        const participant: FabricParticipantRecord = {
          format: 1, id: "agent:recovery-probe", kind: "agent", rootId: config.rootId,
          ownerHostId: host.hostId, ownerIdentityId: host.identity.id, parentId: config.rootId,
          name: "recovery-probe", status: "running", runner: "pi", transport: "process",
          capabilities: [], cwd: root, startedAt: 1, updatedAt: 1, controlProtocol: "v1",
        };
        host.participants.registerSource(() => [participant]);
      }
      await expect(host.participants.refresh()).rejects.toThrow("lease write stalled");
      expect(renewal === "confirmWritable" ? confirmations : writes).toHaveBeenCalled();
      expect(renewal === "confirmWritable" ? writes : confirmations).not.toHaveBeenCalled();
      tail = vi.spyOn(host.mesh, "tail");
      lifecycle = vi.spyOn(host.mesh, "read");
      const event = await host.mesh.publish({ topic: "fleet.recovery", kind: "probe", from: host.identity, data: {} });
      await new Promise(resolve => setTimeout(resolve, 100));
      expect(tail).not.toHaveBeenCalled();
      expect(lifecycle).not.toHaveBeenCalled();
      expect(host.participants.canConsumeMesh()).toBe(false);
      writes.mockRestore();
      confirmations.mockRestore();
      await host.participants.refresh();
      expect(host.participants.canConsumeMesh()).toBe(true);
      await vi.waitFor(() => expect(tail!.mock.results.some((result: { value?: { events?: Array<{ id: string }> } }) => result.value?.events?.some((row: { id: string }) => row.id === event.id))).toBe(true), { timeout: 3_000 });
      expect(host.participants.canConsumeMesh(host.participants.confirmedAt() + 10_001)).toBe(false);
    } finally { writes?.mockRestore(); confirmations?.mockRestore(); tail?.mockRestore(); lifecycle?.mockRestore(); await host.close(); fs.rmSync(root, { recursive: true, force: true }); }
  });
});
