import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";
import { ResidentHost } from "../src/residency/host.js";
import { RESIDENT_HOST_FORMAT, type ResidentHostConfig } from "../src/residency/protocol.js";
import { MeshStore } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { LIVENESS_POLICY_KEY, readHostLease } from "../src/topology/host-leases.js";
import { readParticipantFile } from "../src/topology/participant-files.js";

const participantKey = (id: string) => "topology/participants/" + createHash("sha256").update(id).digest("hex");
const PERIOD_MS = 5_000;

const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-actor-renewal-"));
  const config: ResidentHostConfig = {
    format: RESIDENT_HOST_FORMAT, rootId: "session:absent-main", sessionId: "absent-main",
    cwd: root, projectRoot: root, meshRoot: path.join(root, "mesh"),
    actorRoot: path.join(root, "actors"), residencyRoot: path.join(root, "resident"),
    fullCodeMode: true, agents: DEFAULT_FABRIC_CONFIG.agents, mesh: DEFAULT_FABRIC_CONFIG.mesh,
    retention: DEFAULT_FABRIC_CONFIG.retention, workerPath: path.resolve("dist/agents/worker.js"),
    fabricExtensionPath: path.resolve("dist/index.js"), piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
  };
  fs.mkdirSync(config.residencyRoot, { recursive: true });
  fs.writeFileSync(path.join(config.residencyRoot, "config.json"), JSON.stringify(config));
  const host = new ResidentHost(config);
  const observer = new ParticipantDirectory(new MeshStore(config.meshRoot, 65_536, 1_000), {
    enabled: true, hostId: "observer", rootId: "session:observer",
    identity: { id: "observer", name: "observer", kind: "agent" }, reapDeadHosts: false,
  });
  return { root, config, host, observer };
};

describe("resident actor participant renewal without a Main", () => {
  it.each([
    { mode: "shared", restore: false, touch: false },
    { mode: "files", restore: false, touch: false },
    { mode: "shared", restore: true, touch: false },
    { mode: "files", restore: true, touch: false },
    { mode: "shared", restore: false, touch: true },
  ])("keeps idle actors fresh for three renewal periods ($mode, restore=$restore, touch=$touch)", async ({ mode, restore, touch }) => {
    const { root, config, host: original, observer } = fixture();
    let host = original;
    let now = Date.now();
    // Keep real I/O and timers: only move the wall clock between awaited heartbeats.
    const clock = vi.spyOn(Date, "now").mockImplementation(() => now);
    try {
      if (mode === "files") await observer.mesh.put({ key: LIVENESS_POLICY_KEY,
        value: { version: 1, hostLeases: "files", participants: "files" }, identity: observer.options.identity });
      await host.start();
      const actor = await host.actors.create({ name: "idle-review", instructions: "wait", residency: "durable" });
      await host.participants.refresh();
      if (restore) {
        await host.close();
        host = new ResidentHost(config);
        await host.start();
        expect(host.actors.listOwned().map((record) => record.id)).toContain(actor.id);
      }
      const key = participantKey(actor.id);
      const initial = readParticipantFile(config.meshRoot, key)!;
      expect(initial).toBeDefined();
      expect(observer.get(config.rootId, undefined, { fresh: true })).toBeUndefined();
      expect(fs.existsSync(path.join(config.residencyRoot, "main-generation.json"))).toBe(false);
      let prior = initial.updatedAt;
      for (let period = 1; period <= 3; period++) {
        now += PERIOD_MS;
        // A timestamp-only management write keeps the actor idle. dd65 carried this
        // on each shared heartbeat; RC2's timestamp-only skip is independently red.
        if (touch) await host.actors.setInstructions(actor.id, "wait");
        await host.participants.refresh();
        const record = readParticipantFile(config.meshRoot, key)!;
        // Factory routing reads the participant envelope timestamp, not actor activity time.
        expect.soft(record.updatedAt, `period ${period}`).toBeGreaterThan(prior);
        expect.soft(record.updatedAt, `period ${period}`).toBe(now);
        expect(record.value).toMatchObject({ kind: "actor", rootId: config.rootId, ownerHostId: host.hostId, status: "idle" });
        expect(observer.get(actor.id, undefined, { fresh: true })).toMatchObject({ stale: false, ownerHostId: host.hostId });
        expect(observer.get(config.rootId, undefined, { fresh: true })).toBeUndefined();
        expect(readHostLease(config.meshRoot, host.hostId)?.updatedAt).toBe(now);
        expect(host.actors.status(actor.id).updatedAt).toBe(touch ? now : actor.updatedAt);
        prior = record.updatedAt;
      }
    } finally {
      clock.mockRestore();
      await host.close(); await observer.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it.each(["shared", "files"] as const)("stops renewing a lineage moved by fenced adoption (%s)", async (mode) => {
    const { root, config, host, observer } = fixture();
    const identity = { id: "resident-successor", name: "successor", kind: "agent" as const };
    const successorRoot = "session:successor";
    const successor = new ParticipantDirectory(new MeshStore(config.meshRoot, 65_536, 1_000), {
      enabled: true, hostId: identity.id, rootId: successorRoot, identity, reapDeadHosts: false,
    });
    let now = Date.now();
    const clock = vi.spyOn(Date, "now").mockImplementation(() => now);
    try {
      if (mode === "files") await observer.mesh.put({ key: LIVENESS_POLICY_KEY,
        value: { version: 1, hostLeases: "files", participants: "files" }, identity: observer.options.identity });
      await host.start();
      const actor = await host.actors.create({ name: "moved-review", instructions: "wait", residency: "durable" });
      await host.participants.refresh();
      const key = participantKey(actor.id);
      const prior = readParticipantFile(config.meshRoot, key)!;
      const registry = new ActorRegistryStore(config.actorRoot);
      // Model the adoption commit under the production registry fence, before the
      // new owner's participant is visible. Old directory opinions must not win.
      now += PERIOD_MS;
      await registry.withLock(() => registry.write(registry.records().map((record) => record.id === actor.id
        ? { ...record, rootId: successorRoot, adoptedAt: now, updatedAt: now } : record)));
      await host.participants.refresh();
      expect(readParticipantFile(config.meshRoot, key)).toBeUndefined();
      expect(host.mesh.get(key, { fresh: true })).toBeUndefined();
      successor.registerSource(() => [{ ...(prior.value as FabricParticipantRecord),
        rootId: successorRoot, parentId: successorRoot, ownerHostId: identity.id, ownerIdentityId: identity.id }]);
      await successor.start();
      const moved = readParticipantFile(config.meshRoot, key)!;
      for (let period = 1; period <= 3; period++) {
        now += PERIOD_MS;
        await host.participants.refresh();
        expect(readParticipantFile(config.meshRoot, key)).toEqual(moved);
        expect(observer.get(actor.id, undefined, { fresh: true })).toMatchObject({ rootId: successorRoot, ownerHostId: identity.id });
      }
      await host.close();
      expect(readParticipantFile(config.meshRoot, key)).toEqual(moved);
      expect(registry.records().find((record) => record.id === actor.id)?.rootId).toBe(successorRoot);
    } finally {
      clock.mockRestore();
      await host.close(); await successor.close(); await observer.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
});
