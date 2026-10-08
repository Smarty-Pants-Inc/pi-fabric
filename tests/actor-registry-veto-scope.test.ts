import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { ActorRegistryStore, ActorRegistryUpdateVetoedError } from "../src/actors/registry-store.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import { formatResidentOutcomePriority } from "../src/output-budget.js";
import { ResidentOutcomeUnknownError, type ResidentCommand } from "../src/residency/protocol.js";
import { writeHostLease } from "../src/topology/host-leases.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";
import { observeActorOwnership, publicationGeneration } from "../src/topology/publication-generation.js";

// smarty-dev#6829: registry saves validated a FLEET-WIDE publication generation (state.json
// plus the participants/ and host-leases/ directory stamps), so any heartbeat anywhere on the
// mesh vetoed a busy root's save. The save must veto only on ITS OWN actors' ownership facts.
const roots: string[] = [];
const closers: Array<() => Promise<unknown>> = [];
beforeEach(() => { vi.spyOn(console, "warn").mockImplementation(() => {}); });
afterEach(async () => {
  vi.restoreAllMocks();
  for (const close of closers.splice(0).reverse()) await close().catch(() => undefined);
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const tempRoot = (): string => { const dir = fs.mkdtempSync(path.join(os.tmpdir(), "registry-veto-scope-")); roots.push(dir); return dir; };
const digest = (id: string): string => createHash("sha256").update(id).digest("hex");

let tick = 0;
const participant = (meshRoot: string, id: string, owner: { hostId: string; identityId: string; rootId: string }, extra: Record<string, unknown> = {}): void => {
  const now = Date.now() + ++tick;
  writeParticipantFile(meshRoot, {
    key: `topology/participants/${digest(id)}`, version: tick, updatedAt: now,
    updatedBy: { id: owner.identityId, name: "owner", kind: "main" },
    value: { format: 1, id, kind: "actor", rootId: owner.rootId, ownerHostId: owner.hostId, ownerIdentityId: owner.identityId,
      name: id, status: "idle", runner: "pi", transport: "host", capabilities: [], startedAt: 1, updatedAt: now, ...extra },
  });
};
const lease = (meshRoot: string, owner: { hostId: string; identityId: string; rootId: string }, startedAt = 1): void => {
  const now = Date.now() + ++tick;
  writeHostLease(meshRoot, { id: owner.hostId, rootId: owner.rootId, identityId: owner.identityId, startedAt, updatedAt: now, expiresAt: now + 60_000 });
};
const OTHER = { hostId: "host:other", identityId: "session:other", rootId: "session:other" };
const SELF = { hostId: "host:self", identityId: "session:self", rootId: "session:self" };

const manager = (dir: string) => {
  const mesh = new MeshStore(path.join(dir, "mesh"), 64 * 1024, 100);
  const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, { runRoot: path.join(dir, "runs") });
  const actors = new ActorManager("scope", { id: "session:scope", name: "main", kind: "main" }, mesh,
    { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 60_000 }, agents, () => {}, { actorRoot: path.join(dir, "actors"), persistent: true });
  closers.push(() => agents.close(), () => actors.close());
  return { actors, meshRoot: mesh.root };
};
const committedRow = (dir: string, id: string): Record<string, unknown> | undefined => {
  for (const file of fs.readdirSync(dir, { recursive: true, encoding: "utf8" }).filter(name => path.basename(name) === "actors.json")) {
    const row = new ActorRegistryStore(path.dirname(path.join(dir, file))).records().find(record => record.id === id);
    if (row) return row;
  }
  return undefined;
};
/** Run `during` inside every prepare-to-commit window: after select() observed ownership, before validate(). */
const duringPrepare = (during: () => void) => {
  const real = ActorRegistryStore.prototype.prepare;
  let windows = 0;
  const spy = vi.spyOn(ActorRegistryStore.prototype, "prepare").mockImplementation(function (this: ActorRegistryStore, ...args) {
    windows++;
    during();
    return real.apply(this, args as Parameters<typeof real>);
  });
  return { spy, windows: () => windows };
};

describe("smarty-dev#6829 registry save validation is scoped to this root's ownership facts", () => {
  it("commits while ANOTHER root's participant and host-lease files are rewritten during every prepare", async () => {
    const dir = tempRoot();
    const { actors, meshRoot } = manager(dir);
    const actor = await actors.create({ name: "busy-root", instructions: "Reply" });
    participant(meshRoot, "actor:other-1", OTHER);
    lease(meshRoot, OTHER);
    const before = publicationGeneration(meshRoot);
    const churn = duringPrepare(() => {
      // Heartbeats elsewhere on the fleet: renewals AND brand-new records.
      participant(meshRoot, "actor:other-1", OTHER);
      participant(meshRoot, `actor:other-${tick}`, OTHER);
      lease(meshRoot, OTHER);
      lease(meshRoot, { ...OTHER, hostId: `host:other-${tick}` });
    });
    await expect(actors.setNice(actor.id, 7)).resolves.toBeDefined();
    expect(churn.windows()).toBeGreaterThan(0);
    // The fleet-wide stamp moved (that alone vetoed every attempt before the fix).
    expect(publicationGeneration(meshRoot)).not.toBe(before);
    expect(committedRow(dir, actor.id)?.nice).toBe(7);
  }, 20_000);

  it("commits while THIS actor's owner only renews its participant record and host lease", async () => {
    const dir = tempRoot();
    const { actors, meshRoot } = manager(dir);
    const actor = await actors.create({ name: "renewing", instructions: "Reply" });
    participant(meshRoot, actor.id, SELF);
    lease(meshRoot, SELF);
    duringPrepare(() => { participant(meshRoot, actor.id, SELF, { status: "busy", turns: tick }); lease(meshRoot, SELF); });
    await expect(actors.setNice(actor.id, 4)).resolves.toBeDefined();
    expect(committedRow(dir, actor.id)?.nice).toBe(4);
  }, 20_000);

  it("still vetoes when THIS actor's owner lease is replaced during prepare", async () => {
    const dir = tempRoot();
    const { actors, meshRoot } = manager(dir);
    const actor = await actors.create({ name: "lease-replaced", instructions: "Reply" });
    participant(meshRoot, actor.id, SELF);
    lease(meshRoot, SELF, 1);
    let incarnation = 1;
    // A restarted owner: same host id, new identity and incarnation, on every attempt.
    const replace = duringPrepare(() => lease(meshRoot, { ...SELF, identityId: `session:self-${++incarnation}` }, incarnation));
    await expect(actors.setNice(actor.id, 5)).rejects.toBeInstanceOf(ActorRegistryUpdateVetoedError);
    expect(replace.windows()).toBeGreaterThan(1);
    expect(committedRow(dir, actor.id)?.nice).not.toBe(5);
    replace.spy.mockRestore();
  }, 20_000);

  it("still vetoes when THIS actor is ceded to another owner during prepare", async () => {
    const dir = tempRoot();
    const { actors, meshRoot } = manager(dir);
    const actor = await actors.create({ name: "ceded", instructions: "Reply" });
    participant(meshRoot, actor.id, SELF);
    lease(meshRoot, SELF);
    let owner = 0;
    const cede = duringPrepare(() => participant(meshRoot, actor.id, ++owner % 2 ? OTHER : SELF));
    await expect(actors.setNice(actor.id, 6)).rejects.toBeInstanceOf(ActorRegistryUpdateVetoedError);
    expect(cede.windows()).toBeGreaterThan(1);
    expect(committedRow(dir, actor.id)?.nice).not.toBe(6);
    cede.spy.mockRestore();
  }, 20_000);
});

describe("observeActorOwnership", () => {
  const setup = () => {
    const meshRoot = path.join(tempRoot(), "mesh");
    fs.mkdirSync(meshRoot, { recursive: true });
    participant(meshRoot, "actor:mine", SELF);
    lease(meshRoot, SELF);
    participant(meshRoot, "actor:theirs", OTHER);
    lease(meshRoot, OTHER);
    return meshRoot;
  };

  it("ignores other roots' records and the owner's renewals", () => {
    const meshRoot = setup();
    const observed = observeActorOwnership(meshRoot, ["actor:mine"]);
    participant(meshRoot, "actor:theirs", { ...OTHER, hostId: "host:moved" });
    participant(meshRoot, "actor:new", OTHER);
    lease(meshRoot, OTHER, 99);
    participant(meshRoot, "actor:mine", SELF, { status: "busy" });
    lease(meshRoot, SELF);
    expect(observed.unchanged()).toBe(true);
  });

  it("vetoes a replaced owner lease, a moved or removed participant, and an unobserved actor", () => {
    const meshRoot = setup();
    const replaced = observeActorOwnership(meshRoot, ["actor:mine"]);
    lease(meshRoot, { ...SELF, identityId: "session:self-2" }, 2);
    expect(replaced.unchanged()).toBe(false);

    const moved = observeActorOwnership(meshRoot, ["actor:mine"]);
    participant(meshRoot, "actor:mine", OTHER);
    expect(moved.unchanged()).toBe(false);

    const removed = observeActorOwnership(meshRoot, ["actor:mine"]);
    fs.rmSync(path.join(meshRoot, "participants", `${digest("actor:mine")}.json`));
    expect(removed.unchanged()).toBe(false);

    const unobserved = observeActorOwnership(meshRoot, ["actor:mine"]);
    unobserved.scope(["actor:mine", "actor:never-observed"]);
    expect(unobserved.unchanged()).toBe(false);
  });

  it("scope() narrows validation to the actors the save writes", () => {
    const meshRoot = setup();
    const observed = observeActorOwnership(meshRoot, ["actor:mine", "actor:theirs"]);
    participant(meshRoot, "actor:theirs", { ...OTHER, hostId: "host:moved" });
    expect(observed.unchanged()).toBe(false);
    observed.scope(["actor:mine"]);
    expect(observed.unchanged()).toBe(true);
  });

  it("compares the shared state's ownership entries only when state.json changed", () => {
    const meshRoot = setup();
    const entries = new Map<string, unknown>([[`topology/hosts/${digest(SELF.hostId)}`, {
      key: `topology/hosts/${digest(SELF.hostId)}`, version: 1, updatedAt: 1, updatedBy: { id: SELF.identityId, name: "s", kind: "main" },
      value: { format: 1, id: SELF.hostId, rootId: SELF.rootId, identity: { id: SELF.identityId, name: "s", kind: "main" }, startedAt: 1, updatedAt: 1, expiresAt: 2 },
    }]]);
    const openState = () => (key: string) => entries.get(key) as never;
    fs.writeFileSync(path.join(meshRoot, "state.json"), "{}");
    const renewed = observeActorOwnership(meshRoot, ["actor:mine"], openState);
    const host = entries.get(`topology/hosts/${digest(SELF.hostId)}`) as { value: Record<string, unknown>; version: number };
    entries.set(`topology/hosts/${digest(SELF.hostId)}`, { ...host, version: 2, value: { ...host.value, updatedAt: 5, expiresAt: 9 } });
    fs.writeFileSync(path.join(meshRoot, "state.json"), "{ }");
    expect(renewed.unchanged()).toBe(true);
    const restarted = observeActorOwnership(meshRoot, ["actor:mine"], openState);
    entries.set(`topology/hosts/${digest(SELF.hostId)}`, { ...host, value: { ...host.value, identity: { id: "session:self-2", name: "s", kind: "main" } } });
    fs.writeFileSync(path.join(meshRoot, "state.json"), "{  }");
    expect(restarted.unchanged()).toBe(false);
  });
});

describe("smarty-dev#6829 resident receipts do not read as done while the registry save is pending", () => {
  const command = { format: 1, operation: "actorRemove", requestId: "r1-1-00000000-0000-0000-0000-000000000000", id: "actor:x" } as unknown as ResidentCommand;
  it("reports a committed actor receipt as accepted with the registry save pending", () => {
    const error = new ResidentOutcomeUnknownError(command, { requestId: command.requestId, state: "committed", id: "actor:x", ownerHostId: "host:a" } as never,
      new ActorRegistryUpdateVetoedError());
    expect(error.residentOutcome.state).toBe("committed");
    expect(error.message).toContain("state=accepted; registry save pending");
    const text = formatResidentOutcomePriority([error.residentOutcome]);
    expect(text).toContain("state=committed (accepted; registry save pending)");
  });
  it("leaves unknown receipts unchanged", () => {
    const error = new ResidentOutcomeUnknownError(command, undefined, new Error("timeout"));
    expect(error.message).not.toContain("pending)");
    expect(error.message).not.toContain("state=accepted");
    expect(formatResidentOutcomePriority([error.residentOutcome])).toContain("- state=unknown, operation=");
  });
});
