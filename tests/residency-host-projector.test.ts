import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ResidentHost } from "../src/residency/host.js";
import { RESIDENT_HOST_FORMAT, type ResidentHostConfig } from "../src/residency/protocol.js";
import { installInProcessResidentFence } from "./helpers/in-process-resident-fence.js";

// smarty-dev#6477 W1: the residency host runs the L3 projector in shadow mode and stops it on close.

beforeEach(() => installInProcessResidentFence());

const fixture = (stateBackend: "file" | "shadow") => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-host-projector-"));
  const config: ResidentHostConfig = {
    format: RESIDENT_HOST_FORMAT, rootId: "session:projector", sessionId: "projector",
    cwd: process.cwd(), projectRoot: process.cwd(), meshRoot: path.join(root, "mesh"),
    actorRoot: path.join(root, "actors"), residencyRoot: path.join(root, "resident"),
    fullCodeMode: true, agents: DEFAULT_FABRIC_CONFIG.agents, mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, stateBackend },
    retention: DEFAULT_FABRIC_CONFIG.retention, workerPath: path.resolve("dist/agents/worker.js"),
    fabricExtensionPath: path.resolve("dist/index.js"), piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
  };
  fs.mkdirSync(config.residencyRoot, { recursive: true });
  fs.writeFileSync(path.join(config.residencyRoot, "config.json"), JSON.stringify(config));
  return { root, config, host: new ResidentHost(config, vi.fn()) };
};

describe("resident host state projector (W1)", () => {
  it("starts the projector in shadow mode, projects state.json, and stops it on close", async () => {
    const { root, config, host } = fixture("shadow");
    try {
      await host.start();
      expect(host.mesh.stateBackend).toBe("shadow");
      const projector = await host.stateProjector;
      expect(projector).toBeDefined();
      expect(projector!.root).toBe(path.resolve(config.meshRoot));
      expect(projector!.database).toBe(path.join(path.resolve(config.meshRoot), "state-projector", "state.db"));
      await host.mesh.put({ key: "w1/projected", value: 7, identity: host.identity });
      await vi.waitFor(async () => {
        const status = await projector!.tick();
        expect(status.role).toBe("active");
        expect(status.lag.revisions).toBe(0);
        expect(status.appliedGenerations + status.fullResyncs).toBeGreaterThan(0);
      }, { timeout: 15_000, interval: 50 });
      expect(await projector!.verify()).toEqual([]);
      // The mesh root carries no state.db: file-mode writes stay unfenced while the projector runs.
      expect(fs.existsSync(path.join(config.meshRoot, "state.db"))).toBe(false);
      await host.mesh.put({ key: "w1/after-projection", value: 8, identity: host.identity });
      await host.close();
      expect(projector!.status().role).toBe("stopped");
      expect(projector!.status().haltReason).toBeUndefined();
    } finally {
      await host.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  }, 60_000);

  it("starts no projector in file mode", async () => {
    const { root, host } = fixture("file");
    try {
      await host.start();
      expect(host.mesh.stateBackend).toBe("file");
      expect(await host.stateProjector).toBeUndefined();
      expect(fs.existsSync(path.join(host.config.meshRoot, "state.db"))).toBe(false);
    } finally {
      await host.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  }, 60_000);
});
