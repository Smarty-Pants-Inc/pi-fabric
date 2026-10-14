import { spawn } from "node:child_process";
import { once } from "node:events";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeAll, describe, expect, it } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { SessionActorOrphans, SESSION_ACTOR_ORPHAN_GRACE_MS } from "../src/actors/session-orphans.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { writeHostLease } from "../src/topology/host-leases.js";

// smarty-dev#9241: a same-session start restarts the session actors that stopped only because
// their host Main exited, and never an explicitly stopped one. The real-Pi relaunch proof is
// tests/session-actor-relaunch-real-pi.test.ts.
let deadPid: number;
beforeAll(async () => {
  const child = spawn(process.execPath, ["-e", ""], { stdio: "ignore" });
  deadPid = child.pid!;
  await once(child, "exit");
});
// smarty-dev#8090, follow-up smarty-dev#11335: off Linux root actor tree deletion refuses (no Main publication fence); the
// removal resolves with the named refusal pending and the tree kept. Linux assertions are unchanged.
const OFF_LINUX_PENDING = new RegExp(`^removal cleanup failed: Root actor tree deletion needs the Main publication fence, which this platform \\(${process.platform}\\) lacks \\(smarty-dev#8090\\); follow-up smarty-dev#11335; the root session:old actor tree is kept \\(a later remove or host start finishes it\\)$`);
const cleanups: Array<() => Promise<void> | void> = [];
afterEach(async () => { for (const cleanup of cleanups.splice(0).reverse()) await cleanup(); });

const eventually = async (check: () => boolean, ms = 10_000) => {
  const deadline = Date.now() + ms;
  while (!check()) {
    if (Date.now() > deadline) throw new Error("timed out");
    await new Promise(resolve => setTimeout(resolve, 10));
  }
};
const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-9241-relaunch-"));
  const mesh = new MeshStore(root, 64 * 1024, 100);
  const actorRoot = path.join(root, "actors", "old");
  const store = new ActorRegistryStore(actorRoot);
  const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
  });
  cleanups.push(() => fs.rmSync(root, { recursive: true, force: true }), () => agents.close());
  const at = Date.now() - SESSION_ACTOR_ORPHAN_GRACE_MS - 10_000;
  const row = (id: string, name: string, changes: Record<string, unknown> = {}) => ({ id: id.repeat(32), name, rootId: "session:old",
    residency: "session", status: "idle", instructions: `Supervise ${name}`, events: ["agent_settled"], topics: ["fleet.work"],
    activationFilter: [{ id: "skip-noise", topic: ["fleet.noise"] }], runner: "pi", requirements: [],
    createdAt: at - 1_000, updatedAt: at, messages: [], ...changes });
  const start = (options: Partial<ConstructorParameters<typeof ActorManager>[6]> = {}) => {
    const identity: MeshIdentity = { id: "session:old", name: "old Main", kind: "main", sessionId: "old" };
    const manager = new ActorManager("old", identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 60_000 }, agents, () => {}, {
      actorRoot, persistent: true, rootId: "session:old", claimResidency: "session", reapDeadSessionPresence: false, ...options,
    });
    cleanups.push(() => manager.close());
    return manager;
  };
  // The Main's process is gone: the real root-gone reconciler (a peer Main or its resident host) retires its rows.
  const hostExit = async () => {
    writeHostLease(root, { id: "session:old", rootId: "session:old", identityId: "session:old", updatedAt: at, expiresAt: at,
      writer: { pid: deadPid, host: os.hostname(), startedAt: at - 1_000, releaseSha: "test", lockProtocol: 1, stateBackend: "file" } });
    const peer = new SessionActorOrphans(mesh, { id: "session:peer", name: "peer", kind: "main", sessionId: "peer" }, "peer",
      path.join(root, "actors"), () => true);
    try { return await peer.reconcile(); } finally { await peer.close(); }
  };
  const persisted = (id: string) => store.records().find(record => record.id === id)!;
  return { actorRoot, store, row, start, hostExit, persisted };
};

describe("same-session restart of host-exit-stopped session actors (smarty-dev#9241)", () => {
  it("restarts a reconciler-stopped actor with its id, definition, filters and topics, persists it and accepts a tell", async () => {
    const { store, row, start, hostExit, persisted } = fixture();
    const supervisor = row("a", "supervisor");
    store.write([supervisor]);
    expect(await hostExit()).toBe(1);
    expect(persisted(supervisor.id)).toMatchObject({ status: "stopped", stopReason: "host-exit", sessionOrphan: { oldRoot: "session:old" } });

    const manager = start();
    expect(manager.status(supervisor.id)).toMatchObject({ id: supervisor.id, name: "supervisor", status: "idle",
      topics: ["fleet.work"], events: ["agent_settled"], activationFilter: supervisor.activationFilter });
    expect(manager.status(supervisor.id).sessionOrphan).toBeUndefined();
    expect(manager.status(supervisor.id).stopReason).toBeUndefined();
    expect(manager.instructions(supervisor.id)).toBe("Supervise supervisor");
    await eventually(() => persisted(supervisor.id).status !== "stopped");
    expect(persisted(supervisor.id).sessionOrphan).toBeUndefined();
    expect(persisted(supervisor.id).stopReason).toBeUndefined();
    expect(persisted(supervisor.id).lastError).toBeUndefined();
    expect(manager.tell(supervisor.id, "after relaunch")).toMatchObject({ queued: true });
  });

  it("keeps an explicitly stopped actor stopped across a host exit and restart; its owner can remove it", async () => {
    const { actorRoot, store, row, start, hostExit, persisted } = fixture();
    const kept = row("b", "kept");
    store.write([kept]);
    const first = start();
    await first.stop(kept.id);
    await first.close();
    expect(persisted(kept.id)).toMatchObject({ status: "stopped", stopReason: "explicit" });
    expect(await hostExit()).toBe(0); // The reconciler skips an already stopped row.
    const second = start();
    expect(second.status(kept.id)).toMatchObject({ status: "stopped", stopReason: "explicit" });
    expect(() => second.tell(kept.id, "still stopped")).toThrow(/is stopped/);
    if (process.platform === "linux") {
      expect(await second.remove(kept.id)).toMatchObject({ removed: true });
    } else {
      const dir = path.join(actorRoot, kept.id);
      fs.mkdirSync(dir, { recursive: true });
      fs.writeFileSync(path.join(dir, "kept-11335.txt"), "history\n");
      expect(await second.remove(kept.id)).toMatchObject({ removed: true, cleaned: false, pending: expect.stringMatching(OFF_LINUX_PENDING) });
      expect(fs.readFileSync(path.join(dir, "kept-11335.txt"), "utf8")).toBe("history\n");
    }
    expect(store.records().map(record => record.id)).not.toContain(kept.id);
  });

  it("restarts a legacy root-gone row, never a legacy, directive, removed, durable or foreign stop", async () => {
    const { store, row, start } = fixture();
    const marker = { oldRoot: "session:old", oldHost: os.hostname(), reason: "Main owner process is dead", lastUpdated: 1, orphanedAt: 2 };
    const legacyOrphan = row("c", "legacy-orphan", { status: "stopped", lastError: "root-gone: orphaned", sessionOrphan: marker });
    const legacyStop = row("d", "legacy-stop", { status: "stopped" });
    const directive = row("e", "directive", { status: "stopped", stopReason: "directive" });
    const removing = row("f", "removing", { status: "stopped", stopReason: "host-exit", removal: { requestedAt: 1 } });
    const durable = row("1", "durable", { status: "stopped", stopReason: "host-exit", residency: "durable" });
    const foreign = row("2", "foreign", { status: "stopped", stopReason: "host-exit", rootId: "session:other",
      sessionOrphan: { ...marker, oldRoot: "session:other" } });
    store.write([legacyOrphan, legacyStop, directive, removing, durable, foreign]);
    const manager = start({ canManageActor: () => true });
    expect(manager.status(legacyOrphan.id)).toMatchObject({ status: "idle" });
    expect(manager.status(legacyOrphan.id).lastError).toBeUndefined();
    expect(manager.status(legacyStop.id)).toMatchObject({ status: "stopped" });
    expect(manager.status(legacyStop.id).stopReason).toBeUndefined();
    expect(manager.status(directive.id)).toMatchObject({ status: "stopped", stopReason: "directive" });
    expect(manager.status(removing.id)).toMatchObject({ status: "stopped" });
    expect(manager.status(durable.id)).toMatchObject({ status: "stopped", stopReason: "host-exit" });
    expect(manager.status(foreign.id)).toMatchObject({ status: "stopped", stopReason: "host-exit" });
  });

  it("a durable resident host never restarts its Main's session actors", async () => {
    const { store, row, start, hostExit } = fixture();
    const supervisor = row("a", "supervisor");
    store.write([supervisor]);
    expect(await hostExit()).toBe(1);
    const resident = start({ claimResidency: "durable", canManageActor: () => false });
    expect(resident.list().find(actor => actor.id === supervisor.id)).toMatchObject({ status: "stopped", stopReason: "host-exit" });
  });
});
