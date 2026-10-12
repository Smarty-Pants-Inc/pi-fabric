import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ActorManager } from "../src/actors/manager.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { SessionActorOrphans } from "../src/actors/session-orphans.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";

// smarty-dev#7791 / #8526: the live fleet opened an unchanged 33-byte actors.json 7,392 times a minute, because
// every list() and cleanupObligation() went through an open + fstat even when the cache was current.
const cleanups: Array<() => Promise<void> | void> = [];
afterEach(async () => {
  vi.restoreAllMocks();
  for (const cleanup of cleanups.splice(0)) await cleanup();
});

const settled = (file: string, value: unknown) => {
  fs.writeFileSync(file, JSON.stringify(value));
  const old = new Date(Date.now() - 5_000); // older than the racy window, so the generation is cacheable
  fs.utimesSync(file, old, old);
};

const managerOnRegistry = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-open-churn-"));
  const actorRoot = path.join(root, "actors");
  fs.mkdirSync(actorRoot);
  const file = path.join(actorRoot, "actors.json");
  settled(file, { format: 1, actors: [] });
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const identity = { id: "session:open-churn", name: "main", kind: "main" as const, sessionId: "open-churn" };
  const agents = new AgentManager(root, DEFAULT_FABRIC_CONFIG.agents, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
  });
  const manager = new ActorManager("open-churn", identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, enabled: true }, agents, () => {}, {
    actorRoot, persistent: true, canConsumeMesh: () => false, reapDeadSessionPresence: false,
    meshRetentionSweepPath: false, closeGraceMs: 0,
  });
  cleanups.push(async () => {
    await manager.close();
    await agents.close();
    new ActorRegistryStore(actorRoot).releaseReadCache();
    fs.rmSync(root, { recursive: true, force: true });
  });
  return { root, file, mesh, identity, manager };
};

// Opens and path reads of one file (a read through a descriptor is counted at its open).
const countAccess = (file: string) => {
  const opens = vi.spyOn(fs, "openSync");
  const reads = vi.spyOn(fs, "readFileSync");
  return () => opens.mock.calls.filter(([target]) => String(target) === file).length +
    reads.mock.calls.filter(([target]) => String(target) === file).length;
};

describe.skipIf(process.platform !== "linux")("registry open churn (smarty-dev#7791)", () => {
  it("100 list() and 100 cleanupObligation() on an unchanged registry never reopen it", () => {
    const { file, manager } = managerOnRegistry();
    manager.list(); // the first read decodes and caches the settled generation
    const accesses = countAccess(file);
    const absent = "0123456789abcdef0123456789abcdef";
    for (let i = 0; i < 100; i++) {
      manager.list();
      expect(manager.cleanupObligation(absent)).toBeUndefined();
    }
    expect(accesses()).toBe(0);
  });

  it("a write in between is seen at once", () => {
    const { file, manager } = managerOnRegistry();
    const id = "0123456789abcdef0123456789abcdef";
    manager.list();
    const registry = new ActorRegistryStore(path.dirname(file));
    expect((registry.read() as { actors: unknown[] }).actors).toEqual([]);
    fs.writeFileSync(file, JSON.stringify({ format: 1, actors: [{ id, name: "late" }] }));
    expect((registry.read() as { actors: Array<{ id: string }> }).actors.map((actor) => actor.id)).toEqual([id]);
  });

  it("SessionActorOrphans parses a session registry once per generation, not on every list()", () => {
    const { root, mesh, identity } = managerOnRegistry();
    const sessions = path.join(root, "sessions");
    fs.mkdirSync(path.join(sessions, "other"), { recursive: true });
    const file = path.join(sessions, "other", "actors.json");
    settled(file, { format: 1, actors: [{ id: "a".repeat(32), name: "orphan", status: "running" }] });
    cleanups.push(() => new ActorRegistryStore(path.dirname(file)).releaseReadCache());
    const orphans = new SessionActorOrphans(mesh, identity, "own", sessions);
    expect(orphans.list()).toEqual([]);
    const parse = vi.spyOn(JSON, "parse");
    for (let i = 0; i < 100; i++) orphans.list();
    expect(parse).not.toHaveBeenCalled();
    settled(file, { format: 1, actors: [] }); // a new generation is parsed again
    orphans.list();
    expect(parse).toHaveBeenCalled();
  });
});
