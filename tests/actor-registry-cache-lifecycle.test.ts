import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ActorManager } from "../src/actors/manager.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";

describe("ActorManager registry-cache lifecycle (#7791)", () => {
  it.each([true, false])("evicts on close (persistent=%s), including before temporary-root removal", async persistent => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-cache-lifecycle-"));
    const actorRoot = path.join(root, "actors");
    fs.mkdirSync(actorRoot);
    const file = path.join(actorRoot, "actors.json");
    fs.writeFileSync(file, '{"format":1,"actors":[]}');
    // A racy generation would miss even without close-time eviction.
    const old = new Date(Date.now() - 5_000);
    fs.utimesSync(file, old, old);
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const identity = { id: "session:cache-lifecycle", name: "main", kind: "main" as const, sessionId: "cache-lifecycle" };
    const agents = new AgentManager(root, DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
    });
    const actors = new ActorManager("cache-lifecycle", identity, mesh, DEFAULT_FABRIC_CONFIG.mesh, agents, () => {}, {
      actorRoot, persistent, canConsumeMesh: () => false, reapDeadSessionPresence: false,
      meshRetentionSweepPath: false, closeGraceMs: 0,
    });
    const alias = new ActorRegistryStore(path.join(actorRoot, "."));
    const before = alias.read();
    const stamp = fs.statSync(file, { bigint: true });
    const fstat = fs.fstatSync.bind(fs);
    vi.spyOn(fs, "fstatSync").mockImplementation(((fd: number, options?: { bigint?: boolean }) =>
      options?.bigint === true ? stamp : Reflect.apply(fstat, fs, [fd, options])) as typeof fs.fstatSync);
    const remove = fs.rmSync.bind(fs);
    let removed = false;
    vi.spyOn(fs, "rmSync").mockImplementation((name, options) => {
      if (String(name) === actorRoot) {
        removed = true;
        // The cache must already be released at the removal boundary, not just later in close.finally.
        expect(alias.read()).not.toBe(before);
      }
      remove(name, options);
    });
    try {
      await actors.close();
      expect(removed).toBe(!persistent);
      if (!persistent) {
        expect(fs.existsSync(actorRoot)).toBe(false);
        fs.mkdirSync(actorRoot);
        fs.writeFileSync(file, '{"format":1,"actors":[]}');
      }
      const parse = vi.spyOn(JSON, "parse");
      expect(alias.read()).not.toBe(before); // Forced identical identity makes cache eviction observable.
      expect(parse).toHaveBeenCalledTimes(1);
      await actors.close(); // Idempotent close receipt.
    } finally {
      vi.restoreAllMocks();
      await actors.close();
      await agents.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
});
