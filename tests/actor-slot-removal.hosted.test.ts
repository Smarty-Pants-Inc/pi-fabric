// Hosted-only: actual native actor-directory removal. The unmodified product
// hosted-only predicate excludes this entire suite locally. Never override that guard.
// Keep: #7991 data safety; a stopped status is not native child-settlement proof.
import fs from "node:fs";
import { hostedOnly } from "../scripts/hosted-only.mjs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorDirectory } from "../src/actors/directory.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import type { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";

const state = vi.hoisted(() => ({ slots: undefined as unknown }));
vi.mock("../src/actors/slot-admission.js", () => ({
  slotAdmissionFor: () => state.slots,
  ActorSlotCreateUncertainError: class extends Error { code = "FABRIC_ACTOR_SLOT_CREATE_UNCERTAIN"; },
}));
const roots: string[] = [], directories: ActorDirectory[] = [];
afterEach(async () => {
  await Promise.all(directories.splice(0).map(directory => directory.close()));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
function fixture() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-slot-removal-"));
  roots.push(root);
  const registryRoot = path.join(root, "actors");
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const identity = { id: "session:alpha", name: "main", kind: "main" as const, sessionId: "alpha" };
  let role: { actorId: string; spec: Record<string, unknown>; state: "admitted" | "revoking" | "retired" } | undefined;
  const transitions: string[] = [];
  state.slots = {
    isProtected: (topics: readonly string[]) => topics.includes("fleet.review.test"),
    requiresEnforcement: (topics: readonly string[]) => topics.includes("fleet.review.test"),
    status: () => ({ enforcement: "enforced" }),
    async admit(input: { actorId: string; spec: Record<string, unknown>; beforeCommit?: (id: string) => void }) {
      if (role?.state === "retired" || role?.state === "revoking") throw Object.assign(new Error("permanently occupied"), { code: "FABRIC_ACTOR_SLOT_CLAIM_REFUSED", reason: "occupied" });
      const reused = !!role;
      role ??= { actorId: input.actorId, spec: input.spec, state: "admitted" };
      input.beforeCommit?.(role.actorId);
      return { ...role, reused };
    },
    isCurrent: (id: string) => role?.actorId === id && role.state === "admitted",
    withCurrent<T>(id: string, _topics: readonly string[], registry: string, operation: () => T) {
      expect(fs.existsSync(path.join(registry, "actors.json.lock", "owner"))).toBe(true);
      if (role?.actorId !== id || role.state !== "admitted") throw new Error("revoked");
      return operation();
    },
    async recoverable(registry: string) { return registry === registryRoot && role && role.state !== "retired" ? [role] : []; },
    async beginRemoval(id: string, _topics: readonly string[], registry: string) {
      expect(fs.existsSync(path.join(registry, "actors.json.lock", "owner"))).toBe(true);
      expect(role?.actorId).toBe(id);
      role!.state = "revoking";
      transitions.push("revoking-synced");
    },
    async retire(id: string, _topics: readonly string[], registry: string) {
      expect(fs.existsSync(path.join(registry, "actors.json.lock", "owner"))).toBe(true);
      expect(new ActorRegistryStore(registry).snapshot().actors.some(row => row.id === id)).toBe(false);
      role!.state = "retired";
      transitions.push("retired-synced");
    },
  };
  const sources = new Map<string, string>();
  let unsettled = false;
  const agents = { config: DEFAULT_FABRIC_CONFIG.agents, resolveKernel: () => undefined, defaultModel: () => undefined,
    actorArchiveSources: () => sources, retentionCustodyVeto: () => unsettled,
    run: () => { throw new Error("No worker in this fixture"); }, close: async () => {} } as unknown as AgentManager;
  const open = () => {
    const directory = new ActorDirectory(["alpha", identity, mesh, DEFAULT_FABRIC_CONFIG.mesh, agents, () => {}, {
      persistent: true, rootId: identity.id, claimResidency: "session", releasePaused: true,
      canConsumeMesh: () => false, meshRetentionSweepPath: false, reapDeadSessionPresence: false,
    }], { project: registryRoot, session: path.join(registryRoot, "alpha") }, "project");
    directories.push(directory);
    return directory;
  };
  const request = { name: "review", instructions: "Review.", topics: ["fleet.review.test"], idempotencyKey: "review-v1",
    slotClaim: { key: "factory/review-slots/v1/org/repo/1/base", ifVersion: 1 } };
  return { root, registryRoot, open, request, transitions, sources, setUnsettled: (value: boolean) => { unsettled = value; }, role: () => role };
}

describe.runIf(hostedOnly("actor slot native removal") && process.platform === "linux")("protected removal native projections (hosted only)", () => {
  it("retires only after durable registry removal and refuses exact-token rebirth", async () => {
    const f = fixture(), directory = f.open();
    const actor = await directory.create(f.request);
    await expect(directory.remove(actor.id)).resolves.toEqual({ removed: true });
    expect(f.transitions).toEqual(["revoking-synced", "retired-synced"]);
    expect(f.role()?.actorId).toBe(actor.id);
    expect(fs.existsSync(path.join(f.registryRoot, actor.id))).toBe(false);
    await expect(directory.create(f.request)).rejects.toMatchObject({ code: "FABRIC_ACTOR_SLOT_CLAIM_REFUSED", reason: "occupied" });
    expect(f.role()?.state).toBe("retired");
  });

  it("retains cleanup and permanent revoking occupancy when native execution proof is unknown", async () => {
    const f = fixture(), directory = f.open();
    const actor = await directory.create(f.request);
    f.sources.set("d".repeat(32), path.join(f.root, "unknown-native-source"));
    f.setUnsettled(true);
    await expect(directory.remove(actor.id)).resolves.toMatchObject({ removed: true, cleaned: false, pending: expect.stringContaining("not proven settled") });
    expect(f.transitions).toEqual(["revoking-synced"]);
    expect(f.role()?.state).toBe("revoking");
    expect(fs.existsSync(path.join(f.registryRoot, actor.id))).toBe(true);
    expect(fs.existsSync(path.join(f.registryRoot, `removal-${actor.id}.json`))).toBe(true);
  });

  it("cannot revive a retired ID from a stale native registry snapshot", async () => {
    const f = fixture(), directory = f.open();
    const actor = await directory.create(f.request);
    const stale = new ActorRegistryStore(f.registryRoot).snapshot().bytes!;
    await directory.remove(actor.id);
    await directory.close();
    fs.writeFileSync(path.join(f.registryRoot, "actors.json"), stale);
    const restarted = f.open();
    expect(restarted.listOwned()).toEqual([]);
    await restarted.recoverProtectedActors();
    restarted.resumeQueued();
    expect(restarted.listOwned()).toEqual([]);
    expect(() => restarted.tell(actor.id, "must not run")).toThrow();
    expect(f.role()?.state).toBe("retired");
  });
});
