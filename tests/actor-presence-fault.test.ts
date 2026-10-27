import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { slotAdmissionFor } from "../src/actors/slot-admission.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";

// Keep: #17018 P2-1, trust-boundary counterexamples. Presence isolation may
// suppress only one public-info presentation, never registry/ownership/slot
// authorization selection. No worker, process signal, or product removal API.
const roots: string[] = [];
const close: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  vi.restoreAllMocks();
  for (const dispose of close.splice(0).reverse()) await dispose();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

async function fixture() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-presence-fault-"));
  roots.push(root);
  const identity = { id: "session:presence-fault", name: "main", kind: "main" as const, sessionId: "presence-fault" };
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const agents = new AgentManager(root, DEFAULT_FABRIC_CONFIG.agents, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
  });
  close.push(() => agents.close());
  const ownership = new Map<string, boolean>();
  let ownershipError: Error | undefined;
  const actors = new ActorManager(identity.sessionId, identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, enabled: true }, agents,
    () => {}, { persistent: true, actorRoot: path.join(root, "actors"), rootId: identity.id,
      claimResidency: "session", reapDeadSessionPresence: false,
      canManageActor: id => ownership.get(id),
      snapshotActorOwnership: () => {
        if (ownershipError) throw ownershipError;
        return new Map(ownership);
      },
    });
  close.push(() => { ownershipError = undefined; return actors.close(); });
  const actor = await actors.create({ name: "authorized actor", instructions: "Wait.", responseMode: "text" });
  ownership.set(actor.id, true);
  expect(actors.listOwned().map(row => row.id)).toEqual([actor.id]);
  return { actors, actor, mesh, ownership, failOwnership: (error: Error) => { ownershipError = error; } };
}

it("listOwned's default remains strict and opt-in cannot swallow registry selection failure", async () => {
  const { actors } = await fixture();
  const error = new Error("registry custody unavailable");
  vi.spyOn(ActorRegistryStore.prototype, "fingerprint").mockImplementation(() => { throw error; });
  const onError = vi.fn();
  for (const operation of [() => actors.listOwned(), () => actors.listOwned(true), () => actors.listOwned(true, onError)]) {
    expect(operation).toThrow(error);
  }
  expect(onError).not.toHaveBeenCalled();
});

it("opt-in cannot swallow an ownership snapshot failure or report it as an actor presentation fault", async () => {
  const { actors, failOwnership } = await fixture();
  const error = new Error("ownership read unavailable");
  failOwnership(error);
  const onError = vi.fn();
  expect(() => actors.listOwned(true, onError)).toThrow(error);
  expect(() => actors.listOwned()).toThrow(error);
  expect(onError).not.toHaveBeenCalled();
});

it("a revoked ownership fence still excludes the actor before public-info isolation", async () => {
  const { actors, actor, ownership } = await fixture();
  const onError = vi.fn();
  ownership.set(actor.id, false);
  expect(actors.listOwned(true, onError)).toEqual([]);
  expect(actors.listOwned()).toEqual([]);
  expect(onError).not.toHaveBeenCalled();
});

it("unreadable slot authorization stays fail-closed, never an isolated presentation failure", async () => {
  const { actors, mesh } = await fixture();
  const slots = slotAdmissionFor(mesh);
  const slotError = new Error("slot authority unreadable");
  vi.spyOn(slots, "requiresEnforcement").mockImplementation(() => { throw slotError; });
  const onError = vi.fn();
  // #slotCurrent already treats unreadable slot authority as unauthorized.
  // That selection must stay outside the newly opt-in presentation catch.
  expect(actors.listOwned(true, onError)).toEqual([]);
  expect(actors.listOwned()).toEqual([]);
  expect(onError).not.toHaveBeenCalled();
});
