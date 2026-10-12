import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ActorManager } from "../src/actors/manager.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";

const cleanups: (() => Promise<void>)[] = [];
afterEach(async () => {
  vi.restoreAllMocks();
  for (const close of cleanups.splice(0).reverse()) await close();
});
const deferred = () => {
  let resolve!: () => void;
  const promise = new Promise<void>(r => { resolve = r; });
  return { promise, resolve };
};
const identity = (id: string): MeshIdentity => ({ id, name: "main", kind: "main", sessionId: id.slice(8) });
const original = "session:original";
const successor = "session:successor";

describe("Security R3 S4 orphan adoption shutdown custody", () => {
  it.each((["session", "durable"] as const).flatMap(residency =>
    (["registry", "mesh"] as const).map(fence => ({ residency, fence })),
  ))("cancels $residency adoption waiting for $fence custody and joins it before close", async ({ residency, fence }) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-r3-"));
    cleanups.push(async () => { fs.rmSync(root, { recursive: true, force: true }); });
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 1_000, { readCacheMs: 0 });
    const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
    });
    cleanups.push(() => agents.close());
    const actorRoot = path.join(root, "actors");
    const config = { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 50 };
    const owner = new ActorManager("original", identity(original), mesh, config, agents, () => {}, {
      actorRoot, persistent: true, rootId: original, project: root, role: "project-agent", claimResidency: residency,
    });
    cleanups.push(() => owner.close());
    const actor = await owner.create({ name: "closing-orphan", instructions: "Retain predecessor work.", residency });
    owner.pauseForRelease();
    const accepted = owner.tell(actor.id, "Retain predecessor work.");
    await owner.close();
    const directory = new ParticipantDirectory(mesh, { enabled: true, hostId: original, rootId: original, identity: identity(original), reapDeadHosts: false });
    cleanups.push(() => directory.close());
    await mesh.put({
      key: "topology/lineage-closures/" + createHash("sha256").update(original).digest("hex"),
      identity: identity(original),
      value: { format: 1, rootId: original, ownerHostId: original, ownerIdentityId: original, closedAt: Date.now() },
    });
    expect(directory.lineageAlive(original)).toBe(false); // Real positive terminal-close proof, not a stale lease.
    const registry = path.join(actorRoot, "actors.json");
    const before = fs.readFileSync(registry, "utf8");
    const queueKey = createHash("sha256").update([original, residency].join("\0")).digest("hex").slice(0, 16);
    const queue = path.join(actorRoot, actor.id, `queue-${queueKey}.json`);
    const backlog = fs.readFileSync(queue, "utf8");
    expect(JSON.parse(backlog).items).toEqual([expect.objectContaining({ id: accepted.messageId, source: "direct" })]);
    const entered = deferred(), release = deferred(), completed = deferred();
    const lock = ActorRegistryStore.prototype.withLock;
    const writeBatch = mesh.writeBatch.bind(mesh);
    const spy = fence === "registry"
      ? vi.spyOn(ActorRegistryStore.prototype, "withLock").mockImplementation(async function<T>(this: ActorRegistryStore, operation: () => T | Promise<T>): Promise<T> {
        entered.resolve(); await release.promise;
        try { return await lock.call(this, operation) as T; } finally { completed.resolve(); }
      })
      // Adoption's state fence (a no-op writeBatch, smarty-dev#6477 L2b), not exclusive().
      : vi.spyOn(mesh, "writeBatch").mockImplementation(async input => {
        if (input.ops.length > 0 || !input.prepare || input.afterCommit) return writeBatch(input);
        expect(fs.existsSync(path.join(registry + ".lock", "owner"))).toBe(true);
        entered.resolve(); await release.promise;
        try { return await writeBatch(input); } finally { completed.resolve(); }
      });
    const next = new ActorManager("successor", identity(successor), mesh, config, agents, vi.fn(), {
      actorRoot, persistent: true, rootId: successor, claimResidency: residency, project: root, role: "project-agent", adoptionGraceMs: 0,
      canManageActor: () => undefined, lineageAlive: id => directory.lineageAlive(id),
    });
    cleanups.push(() => next.close());
    const changes = vi.fn();
    next.subscribe(changes);
    let closed = false, secondClosed = false;
    let closing: Promise<void> | undefined, secondClosing: Promise<void> | undefined;
    try {
      next.listOwned(); await entered.promise;
      closing = next.close().then(() => { closed = true; });
      secondClosing = next.close().then(() => { secondClosed = true; });
      // Drain runnable close continuations while custody is deterministically held.
      await new Promise<void>(resolve => setImmediate(resolve));
      const closedWhileHeld = closed, secondClosedWhileHeld = secondClosed;
      const reads = vi.spyOn(fs, "readFileSync");
      changes.mockClear();
      release.resolve(); await completed.promise; await Promise.all([closing, secondClosing]);
      await new Promise<void>(resolve => setImmediate(resolve));
      const predecessorReads = reads.mock.calls.filter(args => String(args[0]) === queue);
      reads.mockRestore();
      // Assert the core security invariant first so the old head's red proves a late durable claim.
      expect(fs.readFileSync(registry, "utf8")).toBe(before);
      expect(next.status(actor.id).rootId).toBe(original);
      expect(next.owns(actor.id)).toBe(false);
      expect(predecessorReads).toHaveLength(0);
      expect(fs.readFileSync(queue, "utf8")).toBe(backlog);
      expect(fs.readdirSync(path.dirname(queue)).filter(file => file.startsWith("queue-"))).toEqual([path.basename(queue)]);
      expect(changes).not.toHaveBeenCalled();
      expect(closedWhileHeld).toBe(false);
      expect(secondClosedWhileHeld).toBe(false);
      expect(closed).toBe(true); expect(secondClosed).toBe(true);
    } finally {
      release.resolve();
      await Promise.all([closing, secondClosing]);
      spy.mockRestore();
    }
  });
});
