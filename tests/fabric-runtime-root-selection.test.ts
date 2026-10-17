import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { loadFabricConfig } from "../src/config.js";
import { FabricRuntimeState } from "../src/fabric-runtime-state.js";
import { MeshBackendFenceError, readMeshStateMovedMarker } from "../src/mesh/backend-fence.js";
import { importMeshState } from "../src/mesh/backend-migration.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";

// Data safety and a real runtime boundary: the config loader must not turn an omitted setting
// into an explicit file selection before MeshStore sees an imported SQLite root.
const identity: MeshIdentity = { id: "root-selection-probe", name: "probe", kind: "agent" };
const roots: string[] = [];
const runtimes: FabricRuntimeState[] = [];
const FIXTURE_DEFAULT = Symbol.for("pi-fabric.mesh.sqlite-initialize.test-fixtures");
let fixtureDescriptor: PropertyDescriptor | undefined;
beforeEach(() => {
  fixtureDescriptor = Object.getOwnPropertyDescriptor(globalThis, FIXTURE_DEFAULT);
  Reflect.deleteProperty(globalThis, FIXTURE_DEFAULT);
  vi.stubEnv("PI_FABRIC_MESH_STATE_BACKEND", undefined);
});
afterEach(async () => {
  try {
    for (const runtime of runtimes.splice(0)) await runtime.shutdown();
  } finally {
    if (fixtureDescriptor) Object.defineProperty(globalThis, FIXTURE_DEFAULT, fixtureDescriptor);
    else Reflect.deleteProperty(globalThis, FIXTURE_DEFAULT);
    vi.unstubAllEnvs();
    for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  }
});

const fixture = () => {
  const cwd = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-runtime-root-selection-")));
  roots.push(cwd);
  const agentDir = path.join(cwd, "agent");
  const meshRoot = path.join(cwd, ".pi", "fabric", "mesh");
  fs.mkdirSync(agentDir);
  const configFile = path.join(agentDir, "fabric.json");
  const settings = {
    mesh: { enabled: true }, mcp: { enabled: false, cache: { enabled: false } },
    agents: { enabled: false }, prewalk: { enabled: false, alwaysRearm: false },
  };
  fs.writeFileSync(configFile, JSON.stringify(settings));
  vi.stubEnv("PI_CODING_AGENT_DIR", agentDir);
  vi.stubEnv("PI_FABRIC_PROJECT_ROOT", cwd);
  vi.stubEnv("PI_FABRIC_MESH_ROOT", meshRoot);
  // These are only the host API harness, not replacements for config, backend selection or storage.
  const pi = { events: { emit: vi.fn() }, getThinkingLevel: () => "off", sendMessage: vi.fn() } as unknown as ExtensionAPI;
  const context = {
    cwd, hasUI: false, isProjectTrusted: () => true, isIdle: () => true, hasPendingMessages: () => false,
    modelRegistry: { find: vi.fn(), getApiKeyAndHeaders: vi.fn() },
    sessionManager: { getSessionId: () => "root-selection", getSessionFile: () => undefined, getBranch: () => [], getLeafId: () => undefined },
    ui: { setStatus: vi.fn(), notify: vi.fn() },
  } as unknown as ExtensionContext;
  const unused = path.join(cwd, "unused.mjs");
  fs.writeFileSync(unused, "export default {};");
  const runtime = new FabricRuntimeState(pi, new CapturedToolCatalog(), {
    paths: { extension: unused, worker: unused, residentHost: unused, skills: cwd },
  });
  runtimes.push(runtime);
  return { runtime, context, cwd, agentDir, meshRoot, configFile, settings };
};

const seedFile = async (root: string): Promise<void> => {
  const seed = new MeshStore(root, 64 * 1024, 1_000, { stateBackend: "file" });
  try { await seed.put({ key: "selection/retained", value: { preserved: true }, identity }); }
  finally { seed.closeState(); }
};

const seedImported = async (root: string): Promise<string> => {
  await seedFile(root);
  const imported = await importMeshState(root);
  expect(imported.backend).toBe("sqlite");
  expect(readMeshStateMovedMarker(root)).toBeDefined();
  return fs.readFileSync(path.join(root, "state.json"), "utf8");
};

describe("FabricRuntimeState selects an unconfigured backend per root", () => {
  for (const kind of ["sqlite", "file", "fresh"] as const) {
    // The real importer's reader-proof/ACL support is unavailable on Windows; file/fresh still run.
    it.skipIf(process.platform === "win32" && kind === "sqlite")(`loads omitted config, reads and writes a ${kind} root, and retains data on reopen`, async () => {
      const { runtime, context, cwd, agentDir, meshRoot } = fixture();
      let marker: string | undefined;
      if (kind === "sqlite") marker = await seedImported(meshRoot);
      else if (kind === "file") await seedFile(meshRoot);
      else expect(fs.existsSync(meshRoot)).toBe(false);
      const config = loadFabricConfig({ cwd, agentDir, projectTrusted: true });
      expect(Object.hasOwn(config.mesh, "stateBackend")).toBe(false);

      // No bootstrap config: initialize goes through the public loader and production factory itself.
      await runtime.initialize(context);
      expect(runtime.mesh.stateBackend).toBe(kind === "sqlite" ? "sqlite" : "file");
      if (kind !== "fresh") expect(runtime.mesh.get("selection/retained", { fresh: true })?.value).toEqual({ preserved: true });
      await runtime.mesh.put({ key: "selection/written", value: kind, identity });
      expect(runtime.mesh.get("selection/written", { fresh: true })?.value).toBe(kind);
      await runtime.shutdown();

      const reopened = new MeshStore(meshRoot, 64 * 1024, 1_000);
      try {
        expect(reopened.stateBackend).toBe(kind === "sqlite" ? "sqlite" : "file");
        expect(reopened.get("selection/written", { fresh: true })?.value).toBe(kind);
        if (kind !== "fresh") expect(reopened.get("selection/retained", { fresh: true })?.value).toEqual({ preserved: true });
      } finally { reopened.closeState(); }
      if (marker !== undefined) expect(fs.readFileSync(path.join(meshRoot, "state.json"), "utf8")).toBe(marker);
      else expect(fs.existsSync(path.join(meshRoot, "state.db"))).toBe(false);
    }, 60_000);
  }

  it.skipIf(process.platform === "win32").each(["intact", "removed"] as const)("refuses unconfigured bootstrap and reads with meta.schema %s and an imported root's marker renamed aside", async schemaState => {
    const { runtime, context, cwd, agentDir, meshRoot } = fixture();
    const marker = await seedImported(meshRoot);
    const before = new MeshStore(meshRoot, 64 * 1024, 1_000);
    try { expect(before.get("selection/retained", { fresh: true })?.value).toEqual({ preserved: true }); }
    finally { before.closeState(); }
    const sqlite = process.getBuiltinModule("node:sqlite") as typeof import("node:sqlite");
    let schema: { value: import("node:sqlite").SQLOutputValue } | undefined;
    if (schemaState === "removed") {
      const raw = new sqlite.DatabaseSync(path.join(meshRoot, "state.db"));
      try {
        schema = raw.prepare("SELECT value FROM meta WHERE name = 'schema'").get() as { value: import("node:sqlite").SQLOutputValue } | undefined;
        expect(schema).toBeDefined();
        expect(raw.prepare("DELETE FROM meta WHERE name = 'schema'").run().changes).toBe(1);
      } finally { raw.close(); }
    }
    const stateFile = path.join(meshRoot, "state.json");
    const markerBackup = path.join(meshRoot, "state.json.marker-backup");
    const database = fs.readFileSync(path.join(meshRoot, "state.db"));
    fs.renameSync(stateFile, markerBackup);
    expect(process.env.PI_FABRIC_MESH_STATE_BACKEND).toBeUndefined();
    const config = loadFabricConfig({ cwd, agentDir, projectTrusted: true });
    expect(Object.hasOwn(config.mesh, "stateBackend")).toBe(false);

    // Use the real loader/bootstrap: no injected normalized config or replacement store.
    let refusal: unknown;
    try { await runtime.initialize(context); } catch (error) { refusal = error; }
    expect(refusal).toBeInstanceOf(MeshBackendFenceError);
    expect((refusal as Error).message).toMatch(/state\.json.*state\.db/);
    await runtime.shutdown();
    expect(() => {
      const reader = new MeshStore(meshRoot, 64 * 1024, 1_000);
      try { reader.get("selection/retained", { fresh: true }); }
      finally { reader.closeState(); }
    }).toThrow(MeshBackendFenceError);
    expect(fs.existsSync(stateFile)).toBe(false);
    expect(fs.readFileSync(markerBackup, "utf8")).toBe(marker);
    expect(fs.readFileSync(path.join(meshRoot, "state.db"))).toEqual(database);

    if (schema) {
      const raw = new sqlite.DatabaseSync(path.join(meshRoot, "state.db"));
      try { raw.prepare("INSERT INTO meta(name, value) VALUES ('schema', ?)").run(schema.value); }
      finally { raw.close(); }
    }
    fs.renameSync(markerBackup, stateFile);
    const restored = new MeshStore(meshRoot, 64 * 1024, 1_000);
    try {
      expect(restored.stateBackend).toBe("sqlite");
      expect(restored.get("selection/retained", { fresh: true })?.value).toEqual({ preserved: true });
    } finally { restored.closeState(); }
  }, 60_000);

  it.skipIf(process.platform === "win32").each(["config", "environment"] as const)("refuses explicit file from %s and preserves the imported root", async source => {
    const { runtime, context, meshRoot, configFile, settings } = fixture();
    const marker = await seedImported(meshRoot);
    if (source === "config") fs.writeFileSync(configFile, JSON.stringify({ ...settings, mesh: { ...settings.mesh, stateBackend: "file" } }));
    else vi.stubEnv("PI_FABRIC_MESH_STATE_BACKEND", " FILE ");
    await expect(runtime.initialize(context)).rejects.toThrow(MeshBackendFenceError);
    await runtime.shutdown();
    vi.stubEnv("PI_FABRIC_MESH_STATE_BACKEND", undefined);
    expect(fs.readFileSync(path.join(meshRoot, "state.json"), "utf8")).toBe(marker);
    const reader = new MeshStore(meshRoot, 64 * 1024, 1_000);
    try {
      expect(reader.stateBackend).toBe("sqlite");
      expect(reader.get("selection/retained", { fresh: true })?.value).toEqual({ preserved: true });
    } finally { reader.closeState(); }
  }, 60_000);
});
