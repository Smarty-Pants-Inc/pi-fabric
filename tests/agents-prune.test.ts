import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { ActorDirectory } from "../src/actors/directory.js";
import { AgentManager } from "../src/agents/manager.js";
import { GlobalActorRegistry } from "../src/actors/global-registry.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ActionRegistry } from "../src/core/action-registry.js";
import { FabricExecutionService } from "../src/execution-service.js";
import { LifecycleBroker } from "../src/lifecycle/broker.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { actorParticipantRecord } from "../src/topology/records.js";
import { writeHostLease } from "../src/topology/host-leases.js";
import { residentRoot } from "../src/residency/protocol.js";
import { processStartTime } from "../src/residency/process-identity.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { ActorBindingStore } from "../src/actors/binding-store.js";
import { lockFile } from "../src/residency/file-lock.js";

const closers: Array<() => Promise<void>> = [];
const roots: string[] = [];
afterEach(async () => {
  vi.restoreAllMocks();
  for (const close of closers.splice(0).reverse()) await close();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const hash = (text: string) => createHash("sha256").update(text).digest("hex");
const snapshot = (root: string): Record<string, string> => {
  const files: Record<string, string> = {};
  const visit = (at: string) => {
    if (!fs.existsSync(at)) return;
    for (const entry of fs.readdirSync(at, { withFileTypes: true })) {
      const file = path.join(at, entry.name);
      if (entry.isDirectory()) visit(file);
      else files[path.relative(root, file)] = fs.readFileSync(file).toString("base64");
    }
  };
  visit(root); return files;
};
const fixture = async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-prune-")); roots.push(root);
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const open = async (sessionId: string) => {
    const identity: MeshIdentity = { id: `session:${sessionId}`, name: "main", kind: "main", sessionId };
    const main = { id: identity.id, local: true, matches: (id: string) => id === identity.id || id === "main",
      info: () => ({ id: identity.id, name: "Main", kind: "main", status: "idle", runner: "pi", transport: "host", cwd: root,
        sessionId, startedAt: 1, updatedAt: 1, pendingMessages: false, local: true }),
      deliverAgent: () => ({ queued: true, messageId: "unused", routed: "main" }) } as FabricMainAgentTarget;
    const agents = new AgentManager(root, DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, `runs-${sessionId}`) });
    const participants = new ParticipantDirectory(mesh, { enabled: true, hostId: identity.id, rootId: identity.id, identity, reapDeadHosts: false });
    const actorRoots = { project: path.join(mesh.root, "actors"), session: path.join(mesh.root, "actors", sessionId) };
    const actors = new ActorDirectory([sessionId, identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 60_000 }, agents, () => {}, {
      persistent: true, rootId: identity.id, claimResidency: "session", project: "not-a-project-root", role: "worktree-agent",
      canManageActor: id => { const p = participants.get(id); return p ? p.ownerHostId === identity.id : undefined; },
      lineageAlive: id => participants.get(id) !== undefined,
    }], actorRoots, "project");
    participants.registerSource(() => [participants.root(main.info({} as ExtensionContext)),
      ...actors.listOwned().map(actor => actorParticipantRecord(actor, identity.id, identity.id, identity.id, identity.id))]);
    await participants.refresh();
    const lifecycle = new LifecycleBroker(mesh, identity, participants, { enabled: true, pollMs: 60_000, maxReadEvents: 100 }, async () => {});
    const provider = new AgentsProvider(agents, actors, new GlobalActorRegistry(root, 64 * 1024), main, participants, undefined, lifecycle);
    let closed = false;
    const close = async () => { if (closed) return; closed = true; await lifecycle.close(); await actors.close(); await agents.close(); await participants.close(); };
    closers.push(close);
    return { identity, actors, participants, provider, close, actorRoots };
  };
  const owner = await open("old-main");
  const project = await owner.actors.create({ name: "old-project-actor", instructions: "Wait." });
  const session = await owner.actors.create({ name: "old-session-actor", instructions: "Wait.", scope: "session" });
  await owner.participants.refresh();
  const caller = await open("caller");
  const config = structuredClone(DEFAULT_FABRIC_CONFIG); config.approvals.agent = "allow"; config.approvals.read = "allow";
  config.executor.timeoutMs = 10_000;
  const registry = new ActionRegistry(); registry.register(caller.provider);
  const service = new FabricExecutionService(registry, config);
  const run = (code: string) => service.execute({ code, signal: undefined, parentToolCallId: "prune-regression",
    context: { cwd: root, hasUI: false } as ExtensionContext, onPartial() {} });
  return { root, mesh, owner, caller, project, session, run };
};

describe("agents.prune real Fabric path (#2184 item 7)", () => {
  it("prunes an exited non-project owner, files and discovery, preserving other roots and audit", async () => {
    const h = await fixture();
    const oldBinding = new ActorBindingStore("old-main", h.owner.actorRoots.project);
    await oldBinding.setThinking(h.project.id, "low");
    for (const [at, actor] of [[h.owner.actorRoots.project, h.project], [h.owner.actorRoots.session, h.session]] as const) {
      fs.mkdirSync(path.join(at, actor.id), { recursive: true });
      fs.writeFileSync(path.join(at, actor.id, "messages.jsonl"), '{"text":"old mail"}\n');
      fs.writeFileSync(path.join(at, actor.id, "queue-dead.json"), '{"items":[{"payload":"old work"}]}');
      fs.writeFileSync(path.join(at, actor.id, "session.jsonl"), '{"text":"old session"}\n');
    }
    await h.owner.close();
    const own = await h.caller.actors.create({ name: "keep-caller", instructions: "Wait.", scope: "session" });
    const inboxKey = `topology/inbox/${hash(h.owner.identity.id).slice(0, 32)}`;
    await h.mesh.put({ key: inboxKey, value: { after: 0 }, identity: h.owner.identity });
    const described = await h.run('return await tools.describe({ ref: "agents.prune" });');
    expect(described.success, described.error).toBe(true);
    expect(JSON.stringify(described.value)).toContain("dryRun");
    const result = await h.run(`const plan = await agents.prune({ root: "session:old-main" });
      const actors = await agents.actors(); const members = await agents.members({ includeStale: true });
      const listed = await agents.list({ scope: "project" }); return { plan, actors, members, listed };`);
    expect(result.success, result.error).toBe(true);
    const value = result.value as any;
    expect(value.plan.actors.map((a: any) => a.id).sort()).toEqual([h.project.id, h.session.id].sort());
    expect(value.plan.removed.actors).toBe(2);
    expect(value.actors.map((a: any) => a.id)).toEqual([own.id]);
    expect([...value.members, ...value.listed].some((p: any) => [h.project.id, h.session.id].includes(p.id))).toBe(false);
    expect(fs.existsSync(path.join(h.owner.actorRoots.project, h.project.id))).toBe(false);
    expect(fs.existsSync(path.join(h.owner.actorRoots.session, h.session.id))).toBe(false);
    expect(fs.existsSync(oldBinding.filePath!)).toBe(false);
    expect(h.mesh.get(inboxKey, { fresh: true })).toBeUndefined();
    expect(h.mesh.listAll("actors/", { fresh: true }).some(e => e.key.endsWith(h.project.id) || e.key.endsWith(h.session.id))).toBe(false);
    expect(h.mesh.read({ topic: "ops.owner", limit: 100 }).some(e => e.kind === "actor.prune")).toBe(true);
    const again = await h.run('return await agents.prune({ root: "session:old-main" });');
    expect(again.success, again.error).toBe(true); expect((again.value as any).removed.actors).toBe(0);
  });
  it("keeps an actor claimed by a racing adopter under the registry lock", async () => {
    const h = await fixture(); await h.owner.close();
    const original = ActorRegistryStore.prototype.withLock;
    let raced = false;
    vi.spyOn(ActorRegistryStore.prototype, "withLock").mockImplementation(function (this: ActorRegistryStore, operation) {
      return original.call(this, () => {
        if (!raced) {
          raced = true;
          const rows = this.records();
          this.write(rows.map(row => row.id === h.project.id ? { ...row, rootId: "session:winner" } : row));
        }
        return operation();
      }) as ReturnType<typeof original>;
    });
    const result = await h.run('return await agents.prune({ root: "session:old-main" });');
    expect(result.success).toBe(false); expect(result.error).toMatch(/ownership changed/i);
    expect(fs.existsSync(path.join(h.owner.actorRoots.project, h.project.id))).toBe(true);
    expect(new ActorRegistryStore(h.owner.actorRoots.project).records().find(row => row.id === h.project.id)?.rootId).toBe("session:winner");
  });
  it("refuses a recently adopted lineage even before its owner presence appears", async () => {
    const h = await fixture(); await h.owner.close();
    const store = new ActorRegistryStore(h.owner.actorRoots.project);
    await store.withLock(() => store.write(store.records().map(row => ({ ...row, adoptedAt: Date.now() }))));
    const before = snapshot(h.root);
    const result = await h.run('return await agents.prune({ root: "session:old-main" });');
    expect(result.success).toBe(false); expect(result.error).toMatch(/live lineage/i); expect(snapshot(h.root)).toEqual(before);
  });
  it("keeps adopted foreign presence and append-only work events under an old session key", async () => {
    const h = await fixture(); await h.owner.close();
    const foreign = `actors/old-main/${"f".repeat(32)}`;
    await h.mesh.put({ key: foreign, value: { id: "f".repeat(32), rootId: "session:winner" }, identity: h.caller.identity });
    const event = await h.mesh.publish({ topic: "fleet.work", to: h.owner.identity.id, from: h.caller.identity, text: "Keep audit." });
    const result = await h.run('return await agents.prune({ root: "session:old-main" });');
    expect(result.success, result.error).toBe(true);
    expect(h.mesh.get(foreign, { fresh: true })).toBeDefined();
    expect(h.mesh.read({ topic: "fleet.work", limit: 100 }).map(item => item.id)).toContain(event.id);
  });
  it("never creates an absent resident host lock when probing existing ownership", async () => {
    const h = await fixture(); await h.owner.close();
    const dir = residentRoot(h.mesh.root, h.owner.identity.id);
    fs.mkdirSync(dir, { recursive: true });
    const file = path.join(dir, "host.lock");
    await expect(lockFile(file, 0, true, false)).rejects.toMatchObject({ code: "ENOENT" });
    expect(fs.existsSync(file)).toBe(false);
  });
  it("refuses a live root and changes nothing", async () => {
    const h = await fixture(); const before = snapshot(h.root);
    const result = await h.run('return await agents.prune({ root: "session:old-main" });');
    expect(result.success).toBe(false); expect(result.error).toMatch(/live lineage/i);
    expect(snapshot(h.root)).toEqual(before);
  });
  it("dryRun returns the dead-root plan without changing files, state or audit", async () => {
    const h = await fixture(); await h.owner.close(); const before = snapshot(h.root);
    const result = await h.run('return await agents.prune({ root: "session:old-main", dryRun: true });');
    expect(result.success, result.error).toBe(true);
    expect((result.value as any).actors).toHaveLength(2); expect((result.value as any).removed.actors).toBe(0);
    expect(snapshot(h.root)).toEqual(before);
  });
  it("refuses the caller's own root even for dryRun", async () => {
    const h = await fixture(); const before = snapshot(h.root);
    for (const dryRun of [false, true]) {
      const result = await h.run(`return await agents.prune({ root: "session:caller", dryRun: ${dryRun} });`);
      expect(result.success).toBe(false); expect(result.error).toMatch(/own root/i);
    }
    expect(snapshot(h.root)).toEqual(before);
  });
  it.each(["main", "resident"])("refuses a live %s file lease without a visible Main", async role => {
    const h = await fixture(); await h.owner.close();
    writeHostLease(h.mesh.root, { id: role === "main" ? h.owner.identity.id : "resident:lease-only", rootId: h.owner.identity.id,
      identityId: "lease-only", updatedAt: Date.now(), expiresAt: Date.now() + 60_000 });
    const before = snapshot(h.root); const result = await h.run('return await agents.prune({ root: "session:old-main" });');
    expect(result.success).toBe(false); expect(result.error).toMatch(/live lineage/i); expect(snapshot(h.root)).toEqual(before);
  });
  it("refuses a live resident process owner even after its lease expired", async () => {
    const h = await fixture(); await h.owner.close(); const at = residentRoot(h.mesh.root, h.owner.identity.id);
    fs.mkdirSync(at, { recursive: true }); fs.writeFileSync(path.join(at, "owner.json"), JSON.stringify({ pid: process.pid, processStartTime: processStartTime(process.pid) }));
    const before = snapshot(h.root); const result = await h.run('return await agents.prune({ root: "session:old-main" });');
    expect(result.success).toBe(false); expect(result.error).toMatch(/live lineage/i); expect(snapshot(h.root)).toEqual(before);
  });
  it.skipIf(process.platform !== "linux")("refuses a held resident host lock with no owner record", async () => {
    const h = await fixture(); await h.owner.close(); const at = residentRoot(h.mesh.root, h.owner.identity.id);
    fs.mkdirSync(at, { recursive: true }); const fd = await lockFile(path.join(at, "host.lock"), 0, true);
    try {
      const before = snapshot(h.root); const result = await h.run('return await agents.prune({ root: "session:old-main" });');
      expect(result.success).toBe(false); expect(result.error).toMatch(/live lineage/i); expect(snapshot(h.root)).toEqual(before);
    } finally { fs.closeSync(fd); }
  });
});
