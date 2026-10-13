import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { readMeshStateMovedMarker } from "../src/mesh/backend-fence.js";
import { MeshStore } from "../src/mesh/store.js";
import { ResidentHost } from "../src/residency/host.js";
import { RESIDENT_HOST_FORMAT, type ResidentHostConfig } from "../src/residency/protocol.js";
import { installInProcessResidentFence } from "./helpers/in-process-resident-fence.js";

beforeEach(() => installInProcessResidentFence());

const fixture = (stateBackend: ResidentHostConfig["mesh"]["stateBackend"]) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-host-backend-"));
  const config: ResidentHostConfig = {
    format: RESIDENT_HOST_FORMAT, rootId: "session:backend", sessionId: "backend",
    cwd: root, projectRoot: root, meshRoot: path.join(root, "mesh"),
    actorRoot: path.join(root, "actors"), residencyRoot: path.join(root, "resident"),
    fullCodeMode: true, agents: DEFAULT_FABRIC_CONFIG.agents,
    mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, stateBackend }, retention: DEFAULT_FABRIC_CONFIG.retention,
    workerPath: path.resolve("dist/agents/worker.js"), fabricExtensionPath: path.resolve("dist/index.js"),
    piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
  };
  fs.mkdirSync(config.residencyRoot, { recursive: true });
  fs.writeFileSync(path.join(config.residencyRoot, "config.json"), JSON.stringify(config));
  return { root, config, host: new ResidentHost(config) };
};

const seedSqlite = async (config: ResidentHostConfig) => {
  const seed = new MeshStore(config.meshRoot, config.mesh.maxEventBytes, config.mesh.maxReadEvents,
    { stateBackend: "sqlite" });
  try {
    expect(seed.stateBackend).toBe("sqlite");
    await seed.put({ key: "backend/retained", value: "sqlite state", identity: { id: "seed", name: "seed", kind: "main" } });
  } finally { seed.closeState(); }
  expect(readMeshStateMovedMarker(config.meshRoot)).toBeDefined();
};

const disagreements = (warn: { mock: { calls: unknown[][] } }) =>
  warn.mock.calls.filter(([message]) => String(message).includes("SQLite moved marker overrides mesh.stateBackend="));

describe("resident host mesh backend selection (smarty-dev#7767)", () => {
  it.each(["file", "shadow"] as const)("opens SQLite on a marked root despite %s config, with one disagreement log", async stateBackend => {
    const { root, config, host } = fixture(stateBackend);
    try {
      await seedSqlite(config);
      const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
      await host.start();
      await host.start(); // An already-started host must not repeat the warning.
      expect(host.mesh.stateBackend).toBe("sqlite");
      expect(host.mesh.get("backend/retained")?.value).toBe("sqlite state");
      expect(await host.stateProjector).toBeUndefined();
      expect(disagreements(warn)).toEqual([[
        `[pi-fabric] resident mesh ${config.meshRoot}: SQLite moved marker overrides mesh.stateBackend=${stateBackend}; opening sqlite`,
      ]]);
      expect(config.mesh.stateBackend).toBe(stateBackend);
    } finally {
      await host.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  }, 60_000);

  it("keeps SQLite without a disagreement log when config agrees with the marker", async () => {
    const { root, config, host } = fixture("sqlite");
    try {
      await seedSqlite(config);
      const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
      await host.start();
      expect(host.mesh.stateBackend).toBe("sqlite");
      expect(host.mesh.get("backend/retained")?.value).toBe("sqlite state");
      expect(disagreements(warn)).toEqual([]);
    } finally {
      await host.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  }, 60_000);

  it("honors file config without a marker or disagreement log", async () => {
    const { root, config, host } = fixture("file");
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      await host.start();
      expect(host.mesh.stateBackend).toBe("file");
      expect(readMeshStateMovedMarker(config.meshRoot)).toBeUndefined();
      expect(fs.existsSync(path.join(config.meshRoot, "state.db"))).toBe(false);
      expect(disagreements(warn)).toEqual([]);
    } finally {
      await host.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  }, 60_000);
});
