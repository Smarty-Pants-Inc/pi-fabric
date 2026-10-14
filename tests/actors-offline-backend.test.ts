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
  meshStateBackendFromMarker,
  readMeshStateMovedMarker,
} from "../src/mesh/backend-fence.js";
import { MeshStateBackendNotBuiltError } from "../src/mesh/state-backend.js";
import { SqliteStateStore } from "../src/mesh/state-sqlite.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";

// Decision C keep reason: recorded cutover incident and data safety. These local tests only read
// migrated actor state/start an empty host; the installed CLI dry-run proves remove-offline wiring.
// No product-removal entrypoint, mocks, spies or cleanup deletion. smarty-test-env owns temp cleanup.
const identity: MeshIdentity = { id: "offline-backend-test", name: "offline-backend-test", kind: "agent" };
const scratch = (): string => fs.mkdtempSync(path.join(os.tmpdir(), "fabric-actors-offline-backend-"));
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
  const { stateBackend: _omitted, ...legacyMesh } = DEFAULT_FABRIC_CONFIG.mesh;
  return {
    format: 1, rootId, sessionId: "offline-backend-session", cwd: root, projectRoot: root,
    meshRoot, actorRoot: path.join(root, "actors"), residencyRoot: residentRoot(meshRoot, rootId),
    fullCodeMode: true,
    agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, retainRuns: true },
    // Old on-disk configs genuinely omit this now-required field; do not normalize it into a default.
    mesh: { ...legacyMesh, actorScope: "session", actorPollMs: 30,
      residentIdleExitMs: 60 * 60_000 } as ResidentHostConfig["mesh"],
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
    assertFallbacks();
  });

  it("a NATS marker selects nats rather than SQLite and refuses an unprepared NATS backend explicitly", () => {
    const root = scratch();
    const marker = encodeMeshStateNatsMarker("FABRIC_STATE", 7);
    fs.writeFileSync(path.join(root, "state.json"), marker);
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
      const backend = meshStateBackendFromMarker(config.meshRoot, config.mesh.stateBackend);
      expect(backend).toBe("sqlite");
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
