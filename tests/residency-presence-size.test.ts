import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { ResidentHost } from "../src/residency/host.js";
import type { ResidentHostConfig } from "../src/residency/protocol.js";
import { readHostLease } from "../src/topology/host-leases.js";
import { readParticipantFile } from "../src/topology/participant-files.js";

it("isolates a real 1024-byte legacy presence failure across startup and recovery without rapid retries", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "ps-"));
  const config: ResidentHostConfig = {
    format: 1, rootId: "session:s", sessionId: "s", cwd: root, projectRoot: root,
    meshRoot: path.join(root, "m"), actorRoot: path.join(root, "a"), residencyRoot: path.join(root, "r"),
    fullCodeMode: true, agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 },
    mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, maxEventBytes: 1024, actorPollMs: 20 }, retention: DEFAULT_FABRIC_CONFIG.retention,
    workerPath: "worker.js", fabricExtensionPath: "index.js", piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
  };
  const registry = new ActorRegistryStore(config.actorRoot);
  const topics = Array.from({ length: 12 }, (_, i) => `fleet.work.project-${String(i).padStart(2, "0")}.observation`);
  const row = (id: string, name: string, subscriptions: string[]) => ({ id, name, rootId: config.rootId,
    instructions: "wait", residency: "durable", runner: "pi", events: [], topics: subscriptions,
    status: "idle", delivery: "mailbox", triggerTurn: false, responseMode: "text", coalesce: true,
    requirements: [], createdAt: Date.now(), updatedAt: Date.now(), messages: [] });
  const bad = "b".repeat(32), good = "a".repeat(32);
  registry.write([row(bad, "big", topics), row(good, "ok", [])]);
  const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
  // Optimistic writes prepare outside custody; write() is the legacy writer.
  const registryWrites = vi.spyOn(ActorRegistryStore.prototype, "prepare");
  let host = new ResidentHost(config);
  try {
    for (let recovery = 0; recovery < 2; recovery++) {
      await host.start();
      const batch = vi.spyOn(host.mesh, "writeBatch");
      const puts = vi.spyOn(host.mesh, "put");
      expect(host.participants.canConsumeMesh()).toBe(true);
      const participantKey = "topology/participants/" + createHash("sha256").update(good).digest("hex");
      expect(readParticipantFile(config.meshRoot, participantKey)?.value).toMatchObject({ id: good });
      expect(readHostLease(config.meshRoot, host.hostId)?.expiresAt).toBeGreaterThan(Date.now());
      expect(host.actors.status(bad).lastError).toContain("Actor presence omitted: Actor presence exceeds 1024 bytes");
      expect(host.mesh.get(`actors/s/${bad}`)).toBeUndefined();
      expect(host.mesh.get(`actors/s/${good}`)?.value).toMatchObject({ id: good });
      expect(host.mesh.get(`actors/s/${good}`)?.value).not.toHaveProperty("ownershipToken");
      // Exercise the actual store limit, not an injected writeBatch rejection.
      await expect(host.mesh.put({ key: "proof/oversize", identity: host.identity,
        value: host.actors.status(bad) })).rejects.toThrow("Mesh state value exceeds 1024 bytes");
      const before = batch.mock.calls.length;
      puts.mockClear();
      await new Promise(resolve => setTimeout(resolve, 350));
      expect(batch.mock.calls.length - before).toBeLessThanOrEqual(1); // no 20 ms readiness/maintenance retry loop
      expect(puts).not.toHaveBeenCalled(); // no per-actor retries
      expect(host.participants.canConsumeMesh()).toBe(true);
      await host.participants.refresh();
      expect(host.participants.canConsumeMesh()).toBe(true);
      await vi.waitFor(() => expect(registry.records().find(r => r.id === bad)?.presenceError)
        .toContain("Actor presence omitted"), { timeout: 6500 });
      expect(registryWrites.mock.calls.some(([rows, options]) => options?.durable === true &&
        rows.some(row => row.id === bad && typeof row.presenceError === "string"))).toBe(true);
      await host.close();
      batch.mockRestore(); puts.mockRestore();
      expect(registry.records().map(r => r.id).sort()).toEqual([good, bad]); // never drop persisted actor state
      if (!recovery) host = new ResidentHost(config);
    }
    expect(warn.mock.calls.filter(([message]) => String(message).includes("Actor presence omitted"))).toHaveLength(1);
  } finally {
    await host.close(); warn.mockRestore(); registryWrites.mockRestore(); fs.rmSync(root, { recursive: true, force: true });
  }
}, 20000);

it("rejects oversize create and configurable updates before committing actor state", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pa-"));
  const config: ResidentHostConfig = {
    format: 1, rootId: "session:s", sessionId: "s", cwd: root, projectRoot: root,
    meshRoot: path.join(root, "m"), actorRoot: path.join(root, "a"), residencyRoot: path.join(root, "r"),
    fullCodeMode: true, agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 },
    // The separate test above exercises the exact 1024-byte legacy boundary.
    // Creation includes session/log paths; leave room for long isolated TMPDIRs
    // here while still exercising real over-limit create/update rejection.
    mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, maxEventBytes: 2048, actorPollMs: 20 }, retention: DEFAULT_FABRIC_CONFIG.retention,
    workerPath: "worker.js", fabricExtensionPath: "index.js", piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
  };
  const host = new ResidentHost(config);
  const registry = new ActorRegistryStore(config.actorRoot);
  try {
    await host.start();
    const onCommit = vi.fn(), beforeCommit = vi.fn();
    await expect(host.actors.create({ name: "big", instructions: "wait", residency: "durable",
      topics: Array.from({ length: 40 }, (_, i) => `fleet.work.project-${String(i).padStart(2, "0")}.observation`) },
      { asRegistryOwner: true, onCommit, beforeCommit })).rejects.toThrow("Actor presence exceeds 2048 bytes");
    expect(onCommit).not.toHaveBeenCalled(); expect(beforeCommit).not.toHaveBeenCalled();
    expect(registry.records()).toEqual([]);
    const actor = await host.actors.create({ name: "ok", instructions: "wait", residency: "durable" }, { asRegistryOwner: true });
    const before = registry.records();
    await expect(host.actors.setTools(actor.id, ["x".repeat(config.mesh.maxEventBytes)])).rejects.toThrow("Actor presence exceeds 2048 bytes");
    expect(registry.records()).toEqual(before);
    expect(host.actors.status(actor.id).tools).toBeUndefined();
  } finally { await host.close(); fs.rmSync(root, { recursive: true, force: true }); }
}, 15000);
