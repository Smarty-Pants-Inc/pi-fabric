import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { ActorRegistryOwnershipError } from "../src/actors/manager.js";
import { ResidentActorClient } from "../src/residency/actor-client.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";
import { ResidentHost } from "../src/residency/host.js";
import { RESIDENT_HOST_FORMAT, residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import { MeshStore } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { LIVENESS_POLICY_KEY, readHostLease } from "../src/topology/host-leases.js";
import { readParticipantFile } from "../src/topology/participant-files.js";
import * as participantFiles from "../src/topology/participant-files.js";

const participantKey = (id: string) => "topology/participants/" + createHash("sha256").update(id).digest("hex");
const PERIOD_MS = 5_000;

const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-actor-renewal-"));
  const config: ResidentHostConfig = {
    format: RESIDENT_HOST_FORMAT, rootId: "session:absent-main", sessionId: "absent-main",
    cwd: root, projectRoot: root, meshRoot: path.join(root, "mesh"),
    actorRoot: path.join(root, "actors"), residencyRoot: residentRoot(path.join(root, "mesh"), "session:absent-main"),
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

  it.each(["shared", "files"] as const)("closes the predecessor before successor publication without rolling back custody (%s)", async (mode) => {
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
        ? { ...record, rootId: successorRoot, adoptedAt: now, adoptedFrom: [config.rootId], updatedAt: now } : record)));
      const adopted = registry.records().find((record) => record.id === actor.id)!;
      await host.participants.refresh();
      expect(readParticipantFile(config.meshRoot, key)).toBeUndefined();
      expect(host.mesh.get(key, { fresh: true })).toBeUndefined();
      expect.soft(host.actors.owns(actor.id)).toBe(false);
      // Crucial ordering: no successor directory opinion can mask the predecessor's
      // fallback ownership or close-time registry save. Custody must stand on disk.
      await host.close();
      expect(registry.records().find((record) => record.id === actor.id)).toEqual(adopted);
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
      expect(registry.records().find((record) => record.id === actor.id)).toEqual(adopted);
    } finally {
      clock.mockRestore();
      await host.close(); await successor.close(); await observer.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
  it.each(["shared", "files"] as const)("fences delayed renewal against adoption at the write (%s)", async (mode) => {
    const { root, config, host, observer } = fixture();
    let release!: () => void;
    const gate = new Promise<void>((resolve) => { release = resolve; });
    let reached!: () => void;
    const selected = new Promise<void>((resolve) => { reached = resolve; });
    let renewal: Promise<void> | undefined, adoption: Promise<void> | undefined;
    try {
      if (mode === "files") await observer.mesh.put({ key: LIVENESS_POLICY_KEY,
        value: { version: 1, hostLeases: "files", participants: "files" }, identity: observer.options.identity });
      await host.start();
      const actor = await host.actors.create({ name: "delayed-review", instructions: "wait", residency: "durable" });
      await host.participants.refresh();
      // Gate after real source selection but before a real shared/key write.
      // Adoption must not commit while that selected renewal is waiting.
      const batch = host.mesh.writeBatch.bind(host.mesh);
      const writeFile = participantFiles.writeParticipantFileIf;
      const wait = mode === "shared"
        ? vi.spyOn(host.mesh, "writeBatch").mockImplementationOnce(async (input) => {
          reached(); await gate; return batch(input);
        })
        : vi.spyOn(participantFiles, "writeParticipantFileIf").mockImplementationOnce(async (...args) => {
          reached(); await gate; return writeFile(...args);
        });
      renewal = host.participants.refresh();
      await selected;
      const registry = new ActorRegistryStore(config.actorRoot);
      let adopted = false;
      adoption = registry.withLock(() => {
        registry.write(registry.records().map((record) => record.id === actor.id
          ? { ...record, rootId: "session:successor", adoptedAt: Date.now(), adoptedFrom: [config.rootId] } : record));
        adopted = true;
      });
      await new Promise((resolve) => setTimeout(resolve, 30));
      expect(adopted).toBe(false); // source selection did not release the mutation fence
      release(); await renewal; await adoption;
      wait.mockRestore();
      const adoptedRow = registry.records().find((record) => record.id === actor.id);
      await host.participants.refresh();
      expect(readParticipantFile(config.meshRoot, participantKey(actor.id))).toBeUndefined();
      await host.close();
      expect(registry.records().find((record) => record.id === actor.id)).toEqual(adoptedRow);
    } finally {
      release(); await Promise.allSettled([renewal, adoption]);
      await host.close(); await observer.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  const mutationCases = (["shared", "files"] as const).flatMap(mode =>
    (["moved-root", "same-root"] as const).flatMap(move =>
      (["manager", "resident-client"] as const).flatMap(route =>
        (["setInstructions", "setTools"] as const).map(operation => ({ mode, move, route, operation })))));
  it.each(mutationCases)("rejects a management mutation that loses registry custody ($mode, $move, $route, $operation)", async ({ mode, move, route, operation }) => {
    const { root, config, host, observer } = fixture();
    const identity = { id: config.rootId, name: "Main", kind: "main" as const, sessionId: config.sessionId };
    const main = new ParticipantDirectory(new MeshStore(config.meshRoot, 65_536, 1_000), {
      enabled: true, hostId: identity.id, rootId: identity.id, identity, reapDeadHosts: false,
    });
    main.registerSource(() => [{ format: 1, id: identity.id, kind: "root", rootId: identity.id,
      ownerHostId: identity.id, ownerIdentityId: identity.id, name: "Main", status: "idle", residency: "session",
      runner: "pi", transport: "host", capabilities: ["fabric"], cwd: root, sessionId: config.sessionId,
      startedAt: Date.now(), updatedAt: Date.now(), controlProtocol: "v1" }]);
    const client = new ResidentActorClient(config.meshRoot, config.rootId, 8_000);
    let save: Promise<unknown> | undefined;
    let spy: { mockRestore(): void } | undefined;
    try {
      if (mode === "files") await observer.mesh.put({ key: LIVENESS_POLICY_KEY,
        value: { version: 1, hostLeases: "files", participants: "files" }, identity: observer.options.identity });
      await main.start();
      await host.start();
      const actor = await host.actors.create({ name: "save-review", instructions: "wait", residency: "durable", tools: [] });
      await host.participants.refresh();
      // Observe real setter entry, not an assumed command-poll delay. The original
      // setter selects its actor and starts waiting for the production registry lock.
      let reached!: () => void;
      const selected = new Promise<void>(resolve => { reached = resolve; });
      if (operation === "setInstructions") {
        const setter = host.actors.setInstructions.bind(host.actors);
        spy = vi.spyOn(host.actors, "setInstructions").mockImplementation(async (...args) => {
          reached(); return setter(...args);
        });
      } else {
        const setter = host.actors.setTools.bind(host.actors);
        spy = vi.spyOn(host.actors, "setTools").mockImplementation(async (...args) => {
          reached(); return setter(...args);
        });
      }
      const registry = new ActorRegistryStore(config.actorRoot);
      let adoptedRow: Record<string, unknown> | undefined;
      await registry.withLock(async () => {
        const mutation = operation === "setInstructions"
          ? { operation, id: actor.id, instructions: "predecessor edit", replace: true }
          : { operation, id: actor.id, tools: ["bash"] };
        const pending = route === "resident-client"
          ? client.setActor(mutation, undefined, { identity, hostId: identity.id })
          : operation === "setInstructions"
            ? host.actors.setInstructions(actor.id, mutation.instructions!)
            : host.actors.setTools(actor.id, mutation.tools!);
        // Attach a rejection handler immediately, including while custody is held.
        save = pending.catch(error => error);
        await Promise.race([selected, save.then(() => { throw new Error("Mutation settled before entering its setter"); })]);
        registry.write(registry.records().map((record) => record.id === actor.id
          ? { ...record, rootId: move === "moved-root" ? "session:successor" : config.rootId,
            adoptedAt: Date.now(), adoptedFrom: ["session:earlier-custodian"] } : record));
        adoptedRow = registry.records().find((record) => record.id === actor.id);
      });
      const error = await save;
      expect(error).toBeInstanceOf(Error);
      expect((error as Error).message).toMatch(/ownership|owned by another host|generation/i);
      if (route === "manager") expect(error).toBeInstanceOf(ActorRegistryOwnershipError);
      expect(registry.records().find((record) => record.id === actor.id)).toEqual(adoptedRow);
      expect(adoptedRow).toMatchObject({ instructions: "wait", tools: [] });
      await host.participants.refresh();
      expect(host.actors.owns(actor.id)).toBe(false);
      expect(readParticipantFile(config.meshRoot, participantKey(actor.id))).toBeUndefined();
      await host.close();
      expect(registry.records().find((record) => record.id === actor.id)).toEqual(adoptedRow);
    } finally {
      await Promise.allSettled([save]); spy?.mockRestore();
      await host.close(); await main.close(); await observer.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it("does not revoke a successor generation when removal waited for registry custody", async () => {
    const { root, config, host, observer } = fixture();
    let remove: Promise<unknown> | undefined;
    try {
      await host.start();
      const actor = await host.actors.create({ name: "remove-review", instructions: "wait", residency: "durable" });
      await host.participants.refresh();
      const registry = new ActorRegistryStore(config.actorRoot);
      let adoptedRow: Record<string, unknown> | undefined;
      await registry.withLock(async () => {
        // Observe the rejection immediately: its asynchronous commit must lose to
        // an adoption even when rootId is reused by a new registry generation.
        remove = host.actors.remove(actor.id).catch((error) => error);
        await new Promise((resolve) => setTimeout(resolve, 30));
        registry.write(registry.records().map((record) => record.id === actor.id
          ? { ...record, adoptedAt: Date.now(), adoptedFrom: ["session:earlier-custodian"] } : record));
        adoptedRow = registry.records().find((record) => record.id === actor.id);
      });
      expect(await remove).toBeInstanceOf(Error);
      expect(registry.records().find((record) => record.id === actor.id)).toEqual(adoptedRow);
      await host.close();
      expect(registry.records().find((record) => record.id === actor.id)).toEqual(adoptedRow);
    } finally {
      await Promise.allSettled([remove]); await host.close(); await observer.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

});
