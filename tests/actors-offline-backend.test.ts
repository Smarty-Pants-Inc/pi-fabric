import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ResidentHost } from "../src/residency/host.js";
import { residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import {
  encodeMeshStateMovedMarker,
  encodeMeshStateNatsMarker,
  MeshBackendFenceError,
  meshStateBackendFromMarker,
  readMeshStateMovedMarker,
  resolveOfflineMeshStateBackend,
} from "../src/mesh/backend-fence.js";
import { rollbackMeshState } from "../src/mesh/backend-migration.js";
import { MeshStateBackendNotBuiltError } from "../src/mesh/state-backend.js";
import { meshStateDocumentProblem } from "../src/mesh/state-file.js";
import { SqliteStateStore } from "../src/mesh/state-sqlite.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";

// Decision C keep reason: recorded cutover incident and data safety. These local tests only read
// migrated actor state/start an empty host; the installed CLI dry-run proves remove-offline wiring.
// No product-removal entrypoint, mocks, spies or cleanup deletion. smarty-test-env owns temp cleanup.
const identity: MeshIdentity = { id: "offline-backend-test", name: "offline-backend-test", kind: "agent" };
const scratch = (): string => fs.mkdtempSync(path.join(os.tmpdir(), "fabric-actors-offline-backend-"));
const strictBackend = (root: string) => resolveOfflineMeshStateBackend(root, meshStateDocumentProblem);
const expectFence = (root: string, operation: () => unknown): MeshBackendFenceError => {
  let failure: unknown;
  try { operation(); } catch (error) { failure = error; }
  expect(failure).toBeInstanceOf(MeshBackendFenceError);
  const fence = failure as MeshBackendFenceError;
  expect(fence.message).toContain(path.resolve(root));
  return fence;
};
const treeBytes = (root: string): Record<string, Buffer> => {
  const result: Record<string, Buffer> = {};
  const visit = (directory: string): void => {
    for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
      const file = path.join(directory, entry.name);
      if (entry.isDirectory()) visit(file);
      else result[path.relative(root, file)] = fs.readFileSync(file);
    }
  };
  visit(root);
  return result;
};

const residentConfig = (root: string): ResidentHostConfig => {
  const meshRoot = path.join(root, "mesh"), rootId = "session:offline-backend-root";
  return {
    format: 1, rootId, sessionId: "offline-backend-session", cwd: root, projectRoot: root,
    meshRoot, actorRoot: path.join(root, "actors"), residencyRoot: residentRoot(meshRoot, rootId),
    fullCodeMode: true,
    agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, retainRuns: true },
    // Omitted backend settings stay omitted; the store factory selects this root's backend.
    mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorScope: "session", actorPollMs: 30,
      residentIdleExitMs: 60 * 60_000 },
    retention: { ...DEFAULT_FABRIC_CONFIG.retention, legacyRunArchiveEnabled: false,
      orphanedTempRunMs: Number.MAX_SAFE_INTEGER, oneShotRunMs: Number.MAX_SAFE_INTEGER,
      actorRunArchiveMs: Number.MAX_SAFE_INTEGER, terminalRunEventsAgeMs: Number.MAX_SAFE_INTEGER },
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fabricExtensionPath: path.resolve("dist/index.js"),
    piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
  };
};
const seedSqlite = async (config: ResidentHostConfig) => {
  const actor = { id: "a".repeat(32), name: "durable-offline-sqlite", rootId: config.rootId,
    residency: "durable", status: "idle" };
  const actorKey = `actors/${config.sessionId}/${actor.id}`;
  // Explicit, fenced fresh-root initializer: native node:sqlite, no adapter or fixture-global bypass.
  // This initializer itself writes state.json with encodeMeshStateMovedMarker at the committed epoch.
  const database = await SqliteStateStore.open(config.meshRoot, config.mesh.maxEventBytes, config.mesh.maxReadEvents,
    { initialize: "create" });
  let snapshot: ReturnType<SqliteStateStore["exportState"]>;
  try {
    await database.importState({ entries: {
      [actorKey]: { key: actorKey, value: actor, version: 1, updatedAt: 1, updatedBy: identity },
    } });
    snapshot = database.exportState({ live: true });
  } finally { database.close(); }
  const marker = readMeshStateMovedMarker(config.meshRoot);
  expect(marker).toBeDefined();
  const markerBytes = fs.readFileSync(path.join(config.meshRoot, "state.json"), "utf8");
  expect(markerBytes).toBe(encodeMeshStateMovedMarker(marker!.epoch, marker!.at));
  return { actorKey, actor, snapshot, markerBytes };
};

// Marker/read tests run on native Node 24 platforms. Only resident startup needs Linux flock.
describe("resident backend authority after cutover", () => {
  it("without a marker keeps undefined/file/sqlite/shadow fallbacks for an absent or ordinary state file", async () => {
    const root = scratch();
    const assertFallbacks = (): void => {
      for (const fallback of [undefined, "file", "sqlite", "shadow"] as const) {
        expect(meshStateBackendFromMarker(root, fallback)).toBe(fallback);
      }
    };
    assertFallbacks();
    const mesh = new MeshStore(root, 64 * 1024, 1_000, { stateBackend: "file" });
    try { await mesh.put({ key: "legacy/actor", value: { preserved: true }, identity }); }
    finally { mesh.closeState(); }
    expect(strictBackend(root)).toBe("file");
    const reader = new MeshStore(root, 64 * 1024, 1_000, { stateBackend: "file" });
    try { expect(reader.get("legacy/actor", { fresh: true })?.value).toEqual({ preserved: true }); }
    finally { reader.closeState(); }
    assertFallbacks();
  });

  it("a NATS marker selects nats rather than SQLite and refuses an unprepared NATS backend explicitly", () => {
    const root = scratch();
    const marker = encodeMeshStateNatsMarker("FABRIC_STATE", 7);
    fs.writeFileSync(path.join(root, "state.json"), marker);
    expect(strictBackend(root)).toBe("nats");
    const backend = meshStateBackendFromMarker(root, "file");
    expect(backend).toBe("nats");
    let failure: unknown, mesh: MeshStore | undefined;
    try { mesh = new MeshStore(root, 64 * 1024, 1_000, { stateBackend: backend }); }
    catch (error) { failure = error; }
    finally { mesh?.closeState(); }
    expect(failure).toBeInstanceOf(MeshStateBackendNotBuiltError);
    expect((failure as Error).message).toContain("no NATS state backend is prepared");
    expect(fs.readFileSync(path.join(root, "state.json"), "utf8")).toBe(marker);
    expect(fs.existsSync(path.join(root, "state.db"))).toBe(false);
  });

  it.each([undefined, "file"] as const)(
    "a SQLite marker overrides fallback %s, finds durable actor state and changes no custody bytes", async fallback => {
      const config = residentConfig(scratch());
      if (fallback !== undefined) config.mesh.stateBackend = fallback;
      else expect(Object.hasOwn(config.mesh, "stateBackend")).toBe(false);
      const seeded = await seedSqlite(config);
      const actorDirectory = path.join(config.actorRoot, seeded.actor.id);
      fs.mkdirSync(actorDirectory, { recursive: true, mode: 0o700 });
      fs.writeFileSync(path.join(config.actorRoot, "actors.json"), JSON.stringify({ format: 1, actors: [seeded.actor] }));
      fs.writeFileSync(path.join(actorDirectory, "session.jsonl"), '{"role":"user","content":"preserve me"}\n');
      fs.writeFileSync(path.join(actorDirectory, "actor-state.bin"), Buffer.from([0, 1, 2, 255]));
      const before = treeBytes(config.actorRoot);
      const backend = strictBackend(config.meshRoot);
      expect(backend).toBe("sqlite");
      expect(meshStateBackendFromMarker(config.meshRoot, config.mesh.stateBackend)).toBe(backend);
      const mesh = new MeshStore(config.meshRoot, config.mesh.maxEventBytes, config.mesh.maxReadEvents, { stateBackend: backend });
      try {
        expect(mesh.stateBackend).toBe("sqlite");
        expect(mesh.get(seeded.actorKey, { fresh: true })?.value).toEqual(seeded.actor);
      } finally { mesh.closeState(); }
      expect(treeBytes(config.actorRoot)).toEqual(before);
      expect(fs.readFileSync(path.join(config.meshRoot, "state.json"), "utf8")).toBe(seeded.markerBytes);
      // Logical database identity avoids raw reads of SQLite files while connections may hold POSIX locks.
      const database = await SqliteStateStore.open(config.meshRoot, config.mesh.maxEventBytes, config.mesh.maxReadEvents);
      try { expect(database.exportState({ live: true })).toEqual(seeded.snapshot); }
      finally { database.close(); }
    });

  it("absent state.json and state.db refuse with a rooted fence error", () => {
    const root = scratch();
    const failure = expectFence(root, () => strictBackend(root));
    expect(failure.message).toMatch(/state\.json|state\.db/);
    expect(fs.existsSync(path.join(root, "state.db"))).toBe(false);
  });

  it.each([
    ["empty", ""],
    ["unparseable", "not-json"],
    ["truncated", '{"format":1,"entries":'],
    ["concatenated", '{"format":1,"entries":{}}{"format":1,"entries":{}}'],
    ["invalid-envelope", JSON.stringify({})],
    ["invalid-format", JSON.stringify({ format: 9, entries: {} })],
    ["invalid-entries", JSON.stringify({ format: 1, entries: { "legacy/actor": null } })],
  ] as const)("rejects %s state.json damage without creating state.db", (_label, contents) => {
    const root = scratch();
    fs.writeFileSync(path.join(root, "state.json"), contents);
    expect(meshStateBackendFromMarker(root, "file")).toBe("file");
    expectFence(root, () => strictBackend(root));
    expect(fs.existsSync(path.join(root, "state.db"))).toBe(false);
  });

  it("rejects a valid SQLite marker without its database", () => {
    const root = scratch();
    fs.writeFileSync(path.join(root, "state.json"), encodeMeshStateMovedMarker(3));
    expectFence(root, () => strictBackend(root));
    expect(fs.existsSync(path.join(root, "state.db"))).toBe(false);
  });

  it("rejects a marker whose native database backend or epoch disagrees", async () => {
    const epochRoot = scratch();
    const epochDb = await SqliteStateStore.open(epochRoot, 64 * 1024, 1_000, { initialize: "create" });
    const epochMarker = readMeshStateMovedMarker(epochRoot);
    expect(epochMarker).toBeDefined();
    epochDb.close();
    fs.writeFileSync(path.join(epochRoot, "state.json"), encodeMeshStateMovedMarker(epochMarker!.epoch + 1));
    expectFence(epochRoot, () => strictBackend(epochRoot));

    const backendRoot = scratch();
    const backendDb = await SqliteStateStore.open(backendRoot, 64 * 1024, 1_000, { initialize: "create" });
    const retiredEpoch = await backendDb.retire();
    backendDb.close();
    expect(retiredEpoch).toBeGreaterThan(1);
    expectFence(backendRoot, () => strictBackend(backendRoot));
  });

  it("rejects a NATS marker when a native SQLite database is present", async () => {
    const root = scratch();
    const database = await SqliteStateStore.open(root, 64 * 1024, 1_000, { initialize: "create" });
    const marker = readMeshStateMovedMarker(root);
    expect(marker).toBeDefined();
    database.close();
    fs.writeFileSync(path.join(root, "state.json"), encodeMeshStateNatsMarker("FABRIC_STATE", marker!.epoch));
    expectFence(root, () => strictBackend(root));
  });

  it("rejects unreadable, corrupt, and uninitialized native database fixtures", () => {
    const fixtures: Array<[string, (file: string) => void]> = [
      ["uninitialized", file => fs.writeFileSync(file, "")],
      ["corrupt", file => fs.writeFileSync(file, Buffer.from("not a SQLite database"))],
      ["unreadable", file => { fs.writeFileSync(file, "private"); fs.chmodSync(file, 0o200); }],
    ];
    for (const [_label, makeFixture] of fixtures) {
      const root = scratch();
      const file = path.join(root, "state.db");
      makeFixture(file);
      expectFence(root, () => strictBackend(root));
      expect(fs.existsSync(file)).toBe(true);
    }
  });

  it("selects SQLite from valid native metadata when the marker is only renamed aside", async () => {
    const config = residentConfig(scratch());
    await seedSqlite(config);
    const marker = path.join(config.meshRoot, "state.json");
    const backup = path.join(config.meshRoot, "state.json.marker-backup");
    fs.renameSync(marker, backup);
    expect(fs.existsSync(marker)).toBe(false);
    expect(fs.existsSync(backup)).toBe(true);
    // This intentionally does not open the actual store: the production missing-import-marker fence remains intact.
    expect(strictBackend(config.meshRoot)).toBe("sqlite");
  });

  it("accepts a real rollback file with retained state.db and rejects malformed migration metadata", async () => {
    const config = residentConfig(scratch());
    const seeded = await seedSqlite(config);
    await rollbackMeshState(config.meshRoot);
    expect(fs.existsSync(path.join(config.meshRoot, "state.db"))).toBe(true);
    expect(strictBackend(config.meshRoot)).toBe("file");
    const reader = new MeshStore(config.meshRoot, config.mesh.maxEventBytes, config.mesh.maxReadEvents, { stateBackend: "file" });
    try { expect(reader.get(seeded.actorKey, { fresh: true })?.value).toEqual(seeded.actor); }
    finally { reader.closeState(); }
    const file = path.join(config.meshRoot, "state.json");
    const exported = JSON.parse(fs.readFileSync(file, "utf8")) as Record<string, unknown>;
    expect(exported.backendDigest).toMatch(/^sha256:[a-f0-9]{64}$/);
    fs.writeFileSync(file, JSON.stringify({ ...exported, backendDigest: "sha256:not-a-stamp" }));
    expectFence(config.meshRoot, () => strictBackend(config.meshRoot));
    fs.writeFileSync(file, JSON.stringify({ ...exported, backendEpoch: "not-an-epoch" }));
    expectFence(config.meshRoot, () => strictBackend(config.meshRoot));
  });

  it.skipIf(process.platform !== "linux")("ResidentHost starts in SQLite mode from a config missing stateBackend", async () => {
    const config = residentConfig(scratch());
    const seeded = await seedSqlite(config);
    fs.mkdirSync(config.residencyRoot, { recursive: true, mode: 0o700 });
    fs.mkdirSync(config.actorRoot, { recursive: true, mode: 0o700 });
    fs.writeFileSync(path.join(config.residencyRoot, "config.json"), JSON.stringify(config));
    const saved = JSON.parse(fs.readFileSync(path.join(config.residencyRoot, "config.json"), "utf8")) as ResidentHostConfig;
    expect(Object.hasOwn(saved.mesh, "stateBackend")).toBe(false);
    // No registry actors, runs, requests or removal obligations: automatic maintenance has no product
    // tree to remove. retainRuns and disabled legacy archival preserve the existing retention fences.
    const host = new ResidentHost(saved);
    // A regression can leave startup awaiting lease readiness; close it at a finite deadline, no polling.
    const deadline = setTimeout(() => { void host.close().catch(() => undefined); }, 10_000);
    try {
      await host.start();
      expect(host.mesh.stateBackend).toBe("sqlite");
      expect(host.mesh.get(seeded.actorKey, { fresh: true })?.value).toEqual(seeded.actor);
    } finally { clearTimeout(deadline); await host.close(); }
  });
});
