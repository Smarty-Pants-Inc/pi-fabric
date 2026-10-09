import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { normalizeFabricConfig } from "../src/config.js";
import { MESH_STATE_BACKEND_KINDS } from "../src/mesh/state-backend-kinds.js";
import { createStateBackend, MESH_STATE_BACKEND_NOT_BUILT_CODE, MeshStateBackendNotBuiltError, resolveMeshStateBackend,
  SqliteStateBackend, STATE_BACKEND_FACTORIES, type StateBackendFactory } from "../src/mesh/state-backend.js";
import { MeshStore, type MeshIdentity, type MeshStoreOptions } from "../src/mesh/store.js";
import { StateFile } from "../src/mesh/state-file.js";

// smarty-dev#7504: the selector is a registry of factories; file/sqlite/shadow pick exactly as before, nats refuses.
const identity: MeshIdentity = { id: "tester", name: "tester", kind: "agent" };
const roots: string[] = [];
const stores: MeshStore[] = [];
const natsFactory = STATE_BACKEND_FACTORIES.nats;

const tempRoot = (): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-selector-"));
  roots.push(root);
  return root;
};
const open = (root: string, options: MeshStoreOptions = {}): MeshStore => {
  const store = new MeshStore(root, 64 * 1024, 1_000, options);
  stores.push(store);
  return store;
};

beforeEach(() => { vi.stubEnv("PI_FABRIC_MESH_STATE_BACKEND", ""); });
afterEach(() => {
  vi.unstubAllEnvs();
  STATE_BACKEND_FACTORIES.nats = natsFactory;
  for (const store of stores.splice(0)) try { store.closeState(); } catch { /* closed by the test */ }
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("state backend selector registry", () => {
  it("registers exactly one factory per known kind", () => {
    expect(Object.keys(STATE_BACKEND_FACTORIES).sort()).toEqual([...MESH_STATE_BACKEND_KINDS].sort());
  });

  it("resolves file, shadow and sqlite exactly as before", () => {
    const table: [Parameters<typeof resolveMeshStateBackend>[0], string | undefined, string][] = [
      [undefined, undefined, "file"], [undefined, "", "file"], [undefined, "sqlite", "sqlite"], [undefined, " Shadow ", "shadow"],
      [undefined, "FILE", "file"], [undefined, "bogus", "file"], ["file", "sqlite", "file"], ["sqlite", "file", "sqlite"],
      ["shadow", undefined, "shadow"],
    ];
    for (const [explicit, env, expected] of table) expect(resolveMeshStateBackend(explicit, env)).toBe(expected);
    expect(() => resolveMeshStateBackend("bogus" as "file")).toThrow(/mesh.stateBackend must be file, shadow, sqlite or nats/);
  });

  it("opens file by default, sqlite from the environment, and an explicit file over the environment", async () => {
    const root = tempRoot();
    const file = open(path.join(root, "a"));
    expect(file.stateBackend).toBe("file");
    expect(file.stateBackendHandle).toBeInstanceOf(StateFile);
    await file.put({ key: "k", value: 1, identity });
    expect(fs.readdirSync(path.join(root, "a")).filter(name => name.startsWith("state.db"))).toEqual([]);
    expect(fs.existsSync(path.join(root, "b", "state.db"))).toBe(false);

    vi.stubEnv("PI_FABRIC_MESH_STATE_BACKEND", "sqlite");
    const sqlite = open(path.join(root, "b"));
    expect(sqlite.stateBackend).toBe("sqlite");
    expect(sqlite.stateBackendHandle).toBeInstanceOf(SqliteStateBackend);
    await sqlite.put({ key: "k", value: 2, identity });
    expect(sqlite.get("k")?.value).toBe(2);
    expect(fs.existsSync(path.join(root, "b", "state.db"))).toBe(true);
    expect(open(path.join(root, "c"), { stateBackend: "file" }).stateBackend).toBe("file");
  });

  it("falls back to file with the reason when a factory reports its kind unavailable", () => {
    const original = STATE_BACKEND_FACTORIES.sqlite;
    STATE_BACKEND_FACTORIES.sqlite = { ...original, unavailable: () => "probe refusal" };
    try {
      const root = tempRoot();
      const backend = createStateBackend({ root, maxEventBytes: 64 * 1024, maxReadEvents: 1_000, lock: undefined as never },
        { stateBackend: "sqlite" });
      expect(backend.kind).toBe("file");
      expect(backend.diagnostics().fallback).toBe("sqlite: probe refusal");
      backend.close();
    } finally { STATE_BACKEND_FACTORIES.sqlite = original; }
  });
});

describe("the nats slot", () => {
  it("resolves from the config, the environment and an explicit option", () => {
    expect(resolveMeshStateBackend(undefined, " NATS ")).toBe("nats");
    expect(resolveMeshStateBackend("nats", "file")).toBe("nats");
    expect(normalizeFabricConfig({ mesh: { stateBackend: "nats" } }).mesh.stateBackend).toBe("nats");
    vi.stubEnv("PI_FABRIC_MESH_STATE_BACKEND", "nats");
    expect(normalizeFabricConfig({ mesh: { stateBackend: "sqlite" } }).mesh.stateBackend).toBe("nats");
  });

  it("refuses clearly, without falling back and without writing state", () => {
    const root = tempRoot();
    for (const [options, env] of [[{ stateBackend: "nats" as const }, ""], [{}, "nats"]] as const) {
      vi.stubEnv("PI_FABRIC_MESH_STATE_BACKEND", env);
      let caught: unknown;
      try { open(path.join(root, `m-${env || "explicit"}`), options); } catch (error) { caught = error; }
      expect(caught).toBeInstanceOf(MeshStateBackendNotBuiltError);
      expect((caught as MeshStateBackendNotBuiltError).code).toBe(MESH_STATE_BACKEND_NOT_BUILT_CODE);
      expect((caught as Error).message).toMatch(/mesh state backend "nats" is not built/);
    }
    const written = fs.readdirSync(root, { recursive: true }).map(String).filter(name => /state\.(json|db)/.test(name));
    expect(written).toEqual([]);
  });

  it("uses a registered nats factory once one exists", () => {
    const registered: StateBackendFactory = { create: (context, options) => Object.assign(new StateFile(context, options), { probe: true }) };
    STATE_BACKEND_FACTORIES.nats = registered;
    const store = open(tempRoot(), { stateBackend: "nats" });
    expect((store.stateBackendHandle as unknown as { probe?: boolean }).probe).toBe(true);
  });
});
