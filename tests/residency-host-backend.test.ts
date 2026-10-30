import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { encodeMeshStateNatsMarker, MeshBackendFenceError, readMeshStateMovedMarker } from "../src/mesh/backend-fence.js";
import { importMeshState } from "../src/mesh/backend-migration.js";
import { MeshStore } from "../src/mesh/store.js";
import { ResidentHost } from "../src/residency/host.js";
import { RESIDENT_HOST_FORMAT, type ResidentHostConfig } from "../src/residency/protocol.js";
import { installInProcessResidentFence } from "./helpers/in-process-resident-fence.js";

beforeEach(() => {
  installInProcessResidentFence();
  vi.stubEnv("PI_FABRIC_MESH_STATE_BACKEND", undefined);
});
afterEach(() => vi.unstubAllEnvs());

const fixture = (stateBackend?: ResidentHostConfig["mesh"]["stateBackend"]) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-host-backend-"));
  const config: ResidentHostConfig = {
    format: RESIDENT_HOST_FORMAT, rootId: "session:backend", sessionId: "backend",
    cwd: root, projectRoot: root, meshRoot: path.join(root, "mesh"),
    actorRoot: path.join(root, "actors"), residencyRoot: path.join(root, "resident"),
    fullCodeMode: true, agents: DEFAULT_FABRIC_CONFIG.agents,
    mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, ...(stateBackend !== undefined ? { stateBackend } : {}) }, retention: DEFAULT_FABRIC_CONFIG.retention,
    workerPath: path.resolve("dist/agents/worker.js"), fabricExtensionPath: path.resolve("dist/index.js"),
    piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
  };
  fs.mkdirSync(config.residencyRoot, { recursive: true });
  fs.writeFileSync(path.join(config.residencyRoot, "config.json"), JSON.stringify(config));
  return { root, config, host: new ResidentHost(config) };
};

const seedSqlite = async (config: ResidentHostConfig) => {
  const seed = new MeshStore(config.meshRoot, config.mesh.maxEventBytes, config.mesh.maxReadEvents,
    { stateBackend: "file" });
  try {
    await seed.put({ key: "backend/retained", value: "sqlite state", identity: { id: "seed", name: "seed", kind: "main" } });
  } finally { seed.closeState(); }
  await importMeshState(config.meshRoot);
  expect(readMeshStateMovedMarker(config.meshRoot)).toBeDefined();
};

describe("resident host mesh backend selection (smarty-dev#7767)", () => {
  // Data-safety boundary: a marker must not erase a real explicit operator setting.
  // The real importer requires reader-proof/ACL support unavailable on Windows; other cases still run there.
  it.skipIf(process.platform === "win32").each(["file", "shadow"] as const)("refuses explicit %s on a SQLite root and preserves imported state", async stateBackend => {
    const { root, config, host } = fixture(stateBackend);
    try {
      await seedSqlite(config);
      const marker = fs.readFileSync(path.join(config.meshRoot, "state.json"), "utf8");
      await expect(host.start()).rejects.toThrow(MeshBackendFenceError);
      expect(config.mesh.stateBackend).toBe(stateBackend);
      expect(fs.readFileSync(path.join(config.meshRoot, "state.json"), "utf8")).toBe(marker);
      const reader = new MeshStore(config.meshRoot, config.mesh.maxEventBytes, config.mesh.maxReadEvents);
      try {
        expect(reader.stateBackend).toBe("sqlite");
        expect(reader.get("backend/retained", { fresh: true })?.value).toBe("sqlite state");
      } finally { reader.closeState(); }
    } finally {
      await host.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  }, 60_000);

  it.skipIf(process.platform === "win32").each([undefined, "sqlite"] as const)("opens imported SQLite with backend config %s", async stateBackend => {
    const { root, config, host } = fixture(stateBackend);
    try {
      await seedSqlite(config);
      if (stateBackend === undefined) expect(Object.hasOwn(config.mesh, "stateBackend")).toBe(false);
      await host.start();
      await host.start();
      expect(host.mesh.stateBackend).toBe("sqlite");
      expect(host.mesh.get("backend/retained")?.value).toBe("sqlite state");
      expect(await host.stateProjector).toBeUndefined();
    } finally {
      await host.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  }, 60_000);

  it("refuses explicit file on a NATS marker without replacing the marker or creating SQLite", async () => {
    const { root, config, host } = fixture("file");
    const marker = encodeMeshStateNatsMarker("FABRIC_STATE", 7);
    fs.mkdirSync(config.meshRoot, { recursive: true });
    fs.writeFileSync(path.join(config.meshRoot, "state.json"), marker);
    try {
      await expect(host.start()).rejects.toThrow(MeshBackendFenceError);
      expect(fs.readFileSync(path.join(config.meshRoot, "state.json"), "utf8")).toBe(marker);
      expect(fs.existsSync(path.join(config.meshRoot, "state.db"))).toBe(false);
    } finally {
      await host.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  }, 60_000);

  it.each([undefined, "file"] as const)("opens a fresh root as file with backend config %s", async stateBackend => {
    const { root, config, host } = fixture(stateBackend);
    try {
      await host.start();
      expect(host.mesh.stateBackend).toBe("file");
      expect(readMeshStateMovedMarker(config.meshRoot)).toBeUndefined();
      expect(fs.existsSync(path.join(config.meshRoot, "state.db"))).toBe(false);
    } finally {
      await host.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  }, 60_000);
});
