import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { AGENTS_ACTION_DESCRIPTORS } from "../src/providers/agents-actions.js";
import { FabricControlPlane, type FabricControlCommand } from "../src/topology/control-plane.js";
import {
  deriveScope,
  normalizeScope,
  scopeCovers,
  senderStamp,
  senderTrusted,
  type FabricScope,
} from "../src/scope.js";

const HOLDER = Symbol.for("pi-fabric:scope:v1");
const holder = globalThis as unknown as Record<symbol, { sealed?: boolean; scope?: FabricScope; error?: string } | undefined>;
const roots: string[] = [];
const agentManagers: AgentManager[] = [];
const actorManagers: ActorManager[] = [];
const planes: FabricControlPlane[] = [];

afterEach(async () => {
  delete holder[HOLDER];
  await Promise.all(planes.splice(0).map((plane) => plane.close()));
  await Promise.all(actorManagers.splice(0).map((manager) => manager.close()));
  await Promise.all(agentManagers.splice(0).map((manager) => manager.close()));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const tempRoot = (prefix: string): string => {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), prefix)));
  roots.push(root);
  return root;
};

const alice = (): FabricScope => normalizeScope({
  principal: { id: "user:alice" },
  grants: [
    { resource: "fs:/repo/**", actions: ["read", "write"] },
    { resource: "memory:*", actions: ["read"] },
  ],
});
const narrower = (parent: FabricScope): FabricScope =>
  deriveScope(parent, [{ resource: "fs:/repo/src/**", actions: ["read"] }]);
const scoped = (scope: FabricScope | undefined): void => {
  holder[HOLDER] = { sealed: true, ...(scope ? { scope } : {}) };
};

const waitFor = async (predicate: () => boolean, timeoutMs = process.env.CI ? 10_000 : 3_000): Promise<void> => {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error("Timed out waiting for state");
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
};

const workerSource = `
import fs from "node:fs";
import path from "node:path";
const args = new Map();
for (let index = 2; index < process.argv.length; index += 2) args.set(process.argv[index].slice(2), process.argv[index + 1]);
const statusFile = args.get("status-file");
fs.mkdirSync(path.dirname(statusFile), { recursive: true });
fs.writeFileSync(path.join(path.dirname(statusFile), "argv.json"), JSON.stringify(Object.fromEntries(args)));
const now = Date.now();
fs.writeFileSync(statusFile, JSON.stringify({
  id: args.get("id"), name: args.get("name"), task: "t", status: "completed",
  runner: args.get("runner"), transport: args.get("transport"), cwd: process.cwd(),
  startedAt: now, updatedAt: now, finishedAt: now, turns: 1, toolCalls: 0, text: "done", exitCode: 0,
  usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, cost: 0 },
}));
fs.writeFileSync(args.get("log-file"), JSON.stringify({ type: "agent_start" }) + "\\n");
`;

describe("durable spawns carry their scope", () => {
  const createManager = () => {
    const dir = tempRoot("pi-fabric-scoped-durable-");
    const workerPath = path.join(dir, "worker.mjs");
    fs.writeFileSync(workerPath, workerSource);
    const runRoot = path.join(dir, "runs");
    const manager = new AgentManager(dir, { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 10_000 }, {
      workerPath, runRoot, fullCodeMode: false,
    });
    agentManagers.push(manager);
    const argv = (id: string): Record<string, string> =>
      JSON.parse(fs.readFileSync(path.join(runRoot, id, "argv.json"), "utf8"));
    return { manager, runRoot, argv };
  };

  it("launches a host-side child with the forwarded scope and narrows from it", async () => {
    const { manager, argv } = createManager();
    const parent = alice();
    // An unscoped resident host: the forwarded scope is the child's scope.
    const inherited = await manager.run({ task: "t", transport: "process", residency: "durable", inheritedScope: parent });
    expect(JSON.parse(argv(inherited.id).scope!)).toEqual(JSON.parse(JSON.stringify(parent)));
    const narrowed = await manager.run({
      task: "t", transport: "process", inheritedScope: JSON.parse(JSON.stringify(parent)) as FabricScope,
      scope: { grants: [{ resource: "memory:*", actions: ["read"] }] },
    });
    expect(normalizeScope(JSON.parse(argv(narrowed.id).scope!)))
      .toMatchObject({ principal: parent.principal, parentDigest: parent.digest });
  });

  it("refuses malformed, forged and uncovered forwarded scopes before launch", async () => {
    const { manager, runRoot } = createManager();
    const parent = JSON.parse(JSON.stringify(alice())) as FabricScope;
    const forged = { ...parent, grants: [{ resource: "fs:*", actions: ["write"] }] } as FabricScope;
    await expect(manager.spawn({ task: "t", transport: "process", inheritedScope: forged }))
      .rejects.toThrow(/Invalid forwarded Fabric scope: .*digest does not match/);
    await expect(manager.spawn({ task: "t", transport: "process", inheritedScope: { grants: [] } as unknown as FabricScope }))
      .rejects.toThrow(/Invalid forwarded Fabric scope/);
    scoped(narrower(alice()));
    await expect(manager.spawn({ task: "t", transport: "process", inheritedScope: parent }))
      .rejects.toThrow(/must be covered by this session's scope/);
    expect(fs.existsSync(runRoot) ? fs.readdirSync(runRoot) : []).toEqual([]);
  });

  it("keeps inheritedScope out of every agent action schema", () => {
    for (const descriptor of AGENTS_ACTION_DESCRIPTORS) {
      const schema = descriptor.inputSchema as { properties?: Record<string, unknown>; additionalProperties?: unknown };
      expect(schema.properties ?? {}).not.toHaveProperty("inheritedScope");
      expect(schema.properties ?? {}).not.toHaveProperty("principalScope");
    }
    const spawn = AGENTS_ACTION_DESCRIPTORS.find((descriptor) => descriptor.name === "spawn")!;
    expect((spawn.inputSchema as { additionalProperties?: unknown }).additionalProperties).toBe(false);
    const create = AGENTS_ACTION_DESCRIPTORS.find((descriptor) => descriptor.name === "create")!;
    expect((create.inputSchema as { additionalProperties?: unknown }).additionalProperties).toBe(false);
  });
});

describe("actor sender trust", () => {
  it("applies the principal trust matrix", () => {
    const actor = alice();
    const same = senderStamp(actor);
    const narrow = senderStamp(narrower(actor));
    const other = senderStamp(normalizeScope({ principal: { id: "user:bob" }, grants: actor.grants }));
    // Unscoped senders keep host authority.
    expect(senderTrusted(undefined, senderStamp(undefined))).toBe(true);
    expect(senderTrusted(actor, { authority: "host" })).toBe(true);
    // A scoped sender never borrows an unscoped actor's authority.
    expect(senderTrusted(undefined, same)).toBe(false);
    // Both scoped: only a covering sender of the same principal.
    expect(senderTrusted(actor, same)).toBe(true);
    expect(senderTrusted(narrower(actor), same)).toBe(true);
    expect(senderTrusted(actor, narrow)).toBe(false);
    expect(senderTrusted(actor, other)).toBe(false);
    // Unstamped (older build) messages: trusted only by an unscoped actor.
    expect(senderTrusted(undefined, undefined)).toBe(true);
    expect(senderTrusted(actor, undefined)).toBe(false);
    // Forged or malformed stamps are never trusted.
    expect(senderTrusted(actor, { ...same, grants: [{ resource: "fs:*", actions: ["write"] }] })).toBe(false);
    expect(senderTrusted(actor, { authority: "root" })).toBe(false);
    expect(senderTrusted(actor, "host")).toBe(false);
    expect(scopeCovers(actor, narrower(actor))).toBe(true);
  });

  it("stamps oversized grants by digest only and trusts them on an exact match", () => {
    const wide = normalizeScope({
      principal: { id: "user:alice" },
      grants: Array.from({ length: 40 }, (_, index) => ({ resource: `fs:/repo/${"x".repeat(100)}${index}/**`, actions: ["read"] })),
    });
    const stamp = senderStamp(wide);
    expect(stamp).toEqual({ authority: "scope", principalId: "user:alice", digest: wide.digest });
    expect(senderTrusted(wide, stamp)).toBe(true);
    expect(senderTrusted(alice(), stamp)).toBe(false);
  });
});

describe("mesh sender stamps", () => {
  it("stamps publishes and schedules with the publishing process authority", async () => {
    const mesh = new MeshStore(path.join(tempRoot("pi-fabric-mesh-sender-"), "mesh"), 64 * 1024, 100);
    const from: MeshIdentity = { id: "session:a", name: "main", kind: "main" };
    expect((await mesh.publish({ topic: "jobs", from })).sender).toEqual({ authority: "host" });
    const scope = alice();
    scoped(scope);
    expect((await mesh.publish({ topic: "jobs", from })).sender).toEqual(senderStamp(scope));
    await mesh.schedule({ topic: "jobs", from, dueAt: Date.now() + 50 });
    scoped(undefined);
    await new Promise((resolve) => setTimeout(resolve, 80));
    const [released] = await mesh.releaseDueSchedules();
    // The releasing (unscoped) host never lends its own authority.
    expect(released!.sender).toEqual(senderStamp(scope));
    const legacy = await mesh.transact((append) => append({ topic: "jobs", from }));
    expect(legacy.sender).toBeUndefined();
  });

  it("hands owners the command event stamp, never a sender inside command data", async () => {
    const meshRoot = path.join(tempRoot("pi-fabric-control-sender-"), "mesh");
    const plane = (id: string) => {
      const value = new FabricControlPlane(new MeshStore(meshRoot, 64 * 1024, 1_000), { id, name: id, kind: "main", sessionId: id }, {
        enabled: true, hostId: id, pollMs: 20, acknowledgementTimeoutMs: 1_000,
      });
      planes.push(value);
      return value;
    };
    const received: FabricControlCommand[] = [];
    const sender = plane("host:sender");
    const receiver = plane("host:receiver");
    sender.start(() => ({ accepted: false }));
    receiver.start((command) => (received.push(command), { accepted: true, messageId: "m" }));
    const scope = narrower(alice());
    scoped(scope);
    await sender.request("host:receiver", "actor:x", "followUp", { message: "hi" });
    expect(received[0]!.sender).toEqual(senderStamp(scope));
    scoped(undefined);
    const forger = new MeshStore(meshRoot, 64 * 1024, 1_000);
    const requestedAt = Date.now();
    await forger.transact((append) => append({
      topic: "fabric.control.command", kind: "followUp", from: { id: "host:forger", name: "f", kind: "main" }, to: "host:receiver",
      data: { version: 1, commandId: "forged", targetId: "actor:x", operation: "followUp", replyTo: "host:forger",
        message: "hi", requestedAt, deadlineAt: requestedAt + 1_000, sender: { authority: "host" } },
    }));
    await waitFor(() => received.length === 2);
    expect(received[1]).not.toHaveProperty("sender");
  });
});

describe("principal-bound actors", () => {
  const setup = () => {
    const root = tempRoot("pi-fabric-scoped-actor-");
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
      runRoot: path.join(root, "runs"),
    });
    agentManagers.push(agents);
    const identity: MeshIdentity = { id: "session:test", name: "main", kind: "main", sessionId: "test" };
    const meshConfig = { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 };
    const create = () => {
      const actors = new ActorManager("test", identity, mesh, meshConfig, agents, () => {}, {
        actorRoot: path.join(root, "actors"),
        persistent: true,
      });
      actorManagers.push(actors);
      return actors;
    };
    return { root, mesh, agents, create };
  };

  it("binds, persists and reloads the creating principal and launches turns with it", async () => {
    const { root, agents, create } = setup();
    const scope = alice();
    scoped(scope);
    const actors = create();
    const runSpy = vi.spyOn(agents, "run");
    const actor = await actors.create({ name: "bound", instructions: "Observe." });
    expect(actor.principal).toEqual({ id: "user:alice", digest: scope.digest });
    await actors.ask(actor.id, "check");
    expect(runSpy.mock.calls[0]![0]).toMatchObject({ inheritedScope: scope });
    expect(runSpy.mock.calls[0]![0].task).not.toContain("UNTRUSTED");
    const registry = JSON.parse(fs.readFileSync(path.join(root, "actors", "actors.json"), "utf8")) as {
      actors: Array<Record<string, unknown>>;
    };
    expect(registry.actors[0]!.principalScope).toEqual(JSON.parse(JSON.stringify(scope)));
    const reloaded = create();
    expect(reloaded.status(actor.id).principal).toEqual(actor.principal);
    // A damaged scope never loads the actor unscoped.
    registry.actors[0]!.principalScope = { ...scope, grants: [{ resource: "fs:*", actions: ["write"] }] };
    fs.writeFileSync(path.join(root, "actors", "actors.json"), JSON.stringify(registry));
    expect(() => create().status(actor.id)).toThrow(/Unknown Fabric actor/);
  });

  it("accepts a forwarded principal on an unscoped host and refuses a forged one", async () => {
    const { create } = setup();
    const actors = create();
    const scope = alice();
    const actor = await actors.create({
      name: "forwarded", instructions: "Observe.", principalScope: JSON.parse(JSON.stringify(scope)) as FabricScope,
    });
    expect(actor.principal).toEqual({ id: "user:alice", digest: scope.digest });
    await expect(actors.create({
      name: "forged", instructions: "Observe.",
      principalScope: { ...scope, principal: { id: "user:bob", issuer: "host" } },
    })).rejects.toThrow(/Invalid forwarded Fabric scope/);
    const unbound = await actors.create({ name: "unbound", instructions: "Observe." });
    expect(unbound).not.toHaveProperty("principal");
  });

  it("marks messages from narrower, foreign or unstamped senders untrusted", async () => {
    const { mesh, agents, create } = setup();
    const scope = alice();
    const actors = create();
    const runSpy = vi.spyOn(agents, "run");
    const bound = await actors.create({ name: "bound", instructions: "Observe.", topics: ["jobs"], principalScope: scope });
    const open = await actors.create({ name: "open", instructions: "Observe.", topics: ["open"] });
    const from: MeshIdentity = { id: "peer", name: "peer", kind: "agent" };
    const tasks = async (count: number) => {
      await waitFor(() => runSpy.mock.calls.length === count);
      return runSpy.mock.calls.map((call) => call[0]);
    };
    // Same principal through a mesh event: trusted.
    await mesh.publish({ topic: "jobs", from, text: "same", sender: senderStamp(scope) });
    // Narrower sender of the same principal: untrusted.
    await mesh.publish({ topic: "jobs", from, text: "narrow", sender: senderStamp(narrower(scope)) });
    // Unstamped event from an older build: untrusted for a bound actor.
    await mesh.transact((append) => append({ topic: "jobs", from, text: "legacy" }));
    let runs = await tasks(3);
    expect(runs.map((run) => run.task.includes("different or narrower principal"))).toEqual([false, true, true]);
    expect(runs.every((run) => run.inheritedScope?.digest === scope.digest)).toBe(true);
    // Unscoped actor: scoped sender untrusted, host and legacy senders trusted.
    await mesh.publish({ topic: "open", from, text: "scoped", sender: senderStamp(scope) });
    await mesh.publish({ topic: "open", from, text: "host" });
    await mesh.transact((append) => append({ topic: "open", from, text: "legacy" }));
    runs = (await tasks(6)).slice(3);
    expect(runs.map((run) => run.task.includes("different or narrower principal"))).toEqual([true, false, false]);
    expect(runs.every((run) => run.inheritedScope === undefined)).toBe(true);
    // A direct tell from this (unscoped) host keeps host authority.
    actors.tell(bound.id, "direct");
    // A remote tell carrying a foreign stamp does not.
    actors.tell(open.id, "remote", undefined, { sender: senderStamp(scope) });
    runs = (await tasks(8)).slice(6);
    expect(runs.map((run) => run.task.includes("different or narrower principal"))).toEqual([false, true]);
    expect(runs[1]!.task).toContain('"untrusted": true');
  });
});
