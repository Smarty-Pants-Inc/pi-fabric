import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ActorDirectory } from "../src/actors/directory.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { ResidentHost } from "../src/residency/host.js";
import { configureFabricDiagnostics, flushFabricDiagnostics, fabricDiagnosticsFile } from "../src/core/diagnostics.js";
import type { ResidentHostConfig } from "../src/residency/protocol.js";

// catch: https://github.com/Smarty-Pants-Inc/smarty-dev/issues/21355
// The registry, resident publisher and mesh commit path remain real. Only the
// directory's two ownership read APIs report a successor during publication.
it.each(["cleared", "newer-block", "newer-streak", "newer-lineage"] as const)(
  "F2 ownership refresh rollback preserves the live durable preparation state (%s)", async replacement => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-reapply-ownership-"));
    vi.stubEnv("SMARTY_ROLE", "worktree-agent");
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(root, "profile"));
    vi.stubEnv("PI_FABRIC_SESSION_ID", undefined);
    configureFabricDiagnostics("reapply-ownership");
    const now = Date.now(), id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const block = { code: "failing-preparation", class: "hard", reason: "seeded preparation recovery fixture",
      since: now, blockedAt: now, count: 1, classification: "permanent: activation preparation kept failing past its retry budget" };
    const streak = { count: 4, notified: true };
    const newerBlock = { ...block, code: "security-policy", reason: "newer security failure", since: now + 1, blockedAt: now + 1 };
    const newerStreak = { count: 5, notified: false };
    const config: ResidentHostConfig = { format: 1, rootId: "session:reapply-ownership", sessionId: "reapply-ownership",
      cwd: root, projectRoot: root, meshRoot: path.join(root, "mesh"), actorRoot: path.join(root, "actors"),
      sessionActorRoot: path.join(root, "session-actors"), residencyRoot: path.join(root, "resident"), fullCodeMode: false,
      agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 }, mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 },
      retention: DEFAULT_FABRIC_CONFIG.retention, workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
      fabricExtensionPath: path.resolve("dist/index.js"), piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda" };
    fs.mkdirSync(config.actorRoot, { recursive: true });
    fs.writeFileSync(path.join(config.actorRoot, "actors.json"), JSON.stringify({ format: 1, actors: [{ id,
      name: "seeded-real-host", instructions: "Original.", createdAt: now, updatedAt: now, rootId: config.rootId,
      residency: "durable", runner: "pi", status: "idle", scope: "project", events: [], topics: [], messages: [],
      activationBlocked: block, failureStreak: streak }] }));
    // Avoid unrelated resident dormancy, not recovery, registry or mesh behavior.
    vi.spyOn(ActorDirectory.prototype, "hasDormantIdleActor").mockReturnValue(false);
    const host = new ResidentHost(config, () => {});
    const registry = new ActorRegistryStore(config.actorRoot);
    try {
      await host.start();
      host.actors.pauseForRelease();
      expect(host.actors.status(id).activationBlocked).toEqual(block);
      vi.spyOn(host.participants, "scheduleRefresh").mockImplementation(() => {});
      let foreign = false, ownershipReads = 0;
      const ownership = host.participants.actorOwnership.bind(host.participants);
      vi.spyOn(host.participants, "actorOwnership").mockImplementation((...args) => {
        const map = new Map(ownership(...args));
        if (foreign) { map.set(id, false); ownershipReads++; }
        return map;
      });
      const get = host.participants.get.bind(host.participants);
      vi.spyOn(host.participants, "get").mockImplementation((...args) => {
        const participant = get(...args);
        if (foreign && args[0] === id && participant) {
          ownershipReads++;
          return { ...participant, ownerHostId: "resident:successor" };
        }
        return participant;
      });
      const refresh = host.participants.refreshPresence.bind(host.participants);
      vi.spyOn(host.participants, "refreshPresence").mockImplementation(async () => {
        // Admission sees the original owner; the real refresh sees the successor
        // consistently through both directory APIs and reloads the manager's row.
        foreign = true;
        try {
          await refresh();
          if (replacement !== "cleared") {
            await registry.update(rows => ({ actors: rows.map(row => row.id !== id ? row : { ...row,
              ...(replacement === "newer-block" ? { activationBlocked: newerBlock } : {}),
              ...(replacement === "newer-streak" ? { failureStreak: newerStreak } : {}),
              ...(replacement === "newer-lineage" ? { rootId: "session:successor", adoptedAt: now + 1,
                adoptedFrom: [config.rootId] } : {}),
            }), durable: true, value: undefined }));
            host.actors.status(id); // Load that committed generation before the undo runs.
            // Guard controls force the same refusal after a newer committed state;
            // the cleared/F2 row above uses the real publisher's deferred result.
            throw new Error("Actor presence publication deferred");
          }
        } finally { foreign = false; }
      });
      await expect(host.actors.setInstructions(id, "Recovery across a custody observation change."))
        .rejects.toThrow("Actor presence publication deferred");
      await flushFabricDiagnostics();
      const diagnostic = fs.readFileSync(fabricDiagnosticsFile("reapply-ownership"), "utf8");
      const current = host.actors.status(id);
      await host.close(); // Join rollback persistence before reading the durable result.
      const persisted = registry.snapshot().actors.find(actor => actor.id === id);
      const expectedBlock = replacement === "cleared" ? block : replacement === "newer-block" ? newerBlock : undefined;
      const expectedStreak = replacement === "cleared" ? streak : replacement === "newer-streak" ? newerStreak : undefined;
      expect(ownershipReads).toBeGreaterThan(0);
      expect(current.activationBlocked).toEqual(expectedBlock);
      expect(persisted?.activationBlocked).toEqual(expectedBlock);
      expect(persisted?.failureStreak).toEqual(expectedStreak);
      expect(persisted?.rootId).toBe(replacement === "newer-lineage" ? "session:successor" : config.rootId);
      expect(diagnostic).toContain(`actor ${id} re-apply publication refused`);
      expect(diagnostic).not.toContain("re-apply cleared");
    } finally {
      vi.restoreAllMocks();
      await host.close();
      await flushFabricDiagnostics();
      vi.unstubAllEnvs();
      fs.rmSync(root, { recursive: true, force: true });
    }
  }, 30_000,
);
