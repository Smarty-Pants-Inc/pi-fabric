import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ActorManager } from "../src/actors/manager.js";
import { deadSessionPresence, reapDeadSessionPresence } from "../src/actors/presence-reaper.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { writeHostLease } from "../src/topology/host-leases.js";

const roots: string[] = [];
const closers: Array<() => Promise<void>> = [];
afterEach(async () => {
  vi.restoreAllMocks();
  await Promise.all(closers.splice(0).map((close) => close()));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const HOUR = 60 * 60 * 1000;
const DAY = 24 * HOUR;
const writer: MeshIdentity = { id: "session:writer", name: "main", kind: "main", sessionId: "writer" };

const store = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-presence-reaper-"));
  roots.push(root);
  return { root, mesh: new MeshStore(path.join(root, "mesh"), 64 * 1024, 100) };
};
const presence = (mesh: MeshStore, session: string, actor: string) =>
  mesh.put({ key: `actors/${session}/${actor}`, value: { id: actor, name: actor, status: "idle" }, identity: writer });
const lease = (mesh: MeshStore, key: string, value: Record<string, unknown>) =>
  mesh.put({ key: `topology/hosts/${key}`, value, identity: writer });

// Every entry below is written now; "now" in the reaper is moved forward to age them.
describe("dead sessions' actor presence with file leases (smarty-dev#816)", () => {
  it("keeps a session whose host lease is renewed only in its file", async () => {
    const { mesh } = store();
    const t0 = Date.now();
    await presence(mesh, "filed", "a1");
    await presence(mesh, "gone", "a1");
    writeHostLease(mesh.root, { id: "session:filed", rootId: "session:filed", identityId: "session:filed", updatedAt: t0, expiresAt: t0 + 2 * DAY });
    const keys = deadSessionPresence(mesh, { ownSessionId: "mine", now: t0 + DAY + HOUR }).map((entry) => entry.key);
    expect(keys).toEqual(["actors/gone/a1"]);
  });
});

describe("dead sessions' actor presence (smarty-dev#448)", () => {
  it("selects only sessions with no lease, legacy entry or presence write within the window", async () => {
    const { mesh } = store();
    const window = 300;                                        // stands in for the day
    const t0 = Date.now();
    await presence(mesh, "dead", "a1");
    await presence(mesh, "dead", "a2");
    await presence(mesh, "mine", "a1");                        // the caller's own session
    await presence(mesh, "leased", "a1");
    await lease(mesh, "session:leased", { id: "session:leased", rootId: "session:leased", expiresAt: t0 + DAY });
    await presence(mesh, "resident", "a1");                     // a resident host of that root keeps it alive
    await lease(mesh, "resident:abc", { id: "resident:abc", rootId: "session:resident", identity: { id: "resident:abc" }, expiresAt: t0 + DAY });
    await presence(mesh, "legacy", "a1");
    await presence(mesh, "expired", "a1");                      // lease expired long ago: dead
    await lease(mesh, "session:expired", { id: "session:expired", rootId: "session:expired", expiresAt: t0 - DAY });
    await presence(mesh, "rewritten", "a1");
    await mesh.put({ key: "actors/dead/a3/extra", value: { id: "extra" }, identity: writer });   // not presence-shaped
    await mesh.put({ key: "actors/dead/mismatch", value: { id: "other" }, identity: writer });
    expect(deadSessionPresence(mesh, { ownSessionId: "mine", deadAfterMs: window })).toEqual([]);   // all recent
    await new Promise((resolve) => setTimeout(resolve, window + 100));
    await mesh.put({ key: "sessions/legacy", value: { id: "session:legacy" }, identity: writer });   // a legacy session entry, now
    await presence(mesh, "rewritten", "a2");                    // one fresh presence write keeps the session
    const keys = deadSessionPresence(mesh, { ownSessionId: "mine", deadAfterMs: window }).map((entry) => entry.key).sort();
    expect(keys).toEqual(["actors/dead/a1", "actors/dead/a2", "actors/expired/a1"]);
  });

  it("keeps a session whose lease lapsed briefly but whose presence is fresh", async () => {
    const { mesh } = store();
    const t0 = Date.now();
    await lease(mesh, "session:reloading", { id: "session:reloading", rootId: "session:reloading", expiresAt: t0 - 2 * DAY });
    await presence(mesh, "reloading", "a1");
    // Measured two days later, the lease is old, but the presence write was within the day.
    expect(deadSessionPresence(mesh, { ownSessionId: "mine", now: t0 + 12 * HOUR })).toEqual([]);
  });

  it("deletes the dead entries in one batch, fenced to the version it saw", async () => {
    const { mesh } = store();
    await presence(mesh, "dead", "a1");
    await presence(mesh, "dead", "a2");
    const original = mesh.writeBatch.bind(mesh);
    const batch = vi.spyOn(mesh, "writeBatch");
    batch.mockImplementationOnce(async (input) => {
      await presence(mesh, "dead", "a2");                       // the session writes it again meanwhile
      return original(input);
    });
    const removed = await reapDeadSessionPresence(mesh, writer, { ownSessionId: "mine", now: Date.now() + 2 * DAY });
    expect(batch).toHaveBeenCalledTimes(1);
    expect(removed).toBe(1);
    expect(mesh.get("actors/dead/a1")).toBeUndefined();
    expect(mesh.get("actors/dead/a2")).toBeDefined();           // the newer write is kept
    // Nothing dead left: no write at all.
    batch.mockClear();
    await reapDeadSessionPresence(mesh, writer, { ownSessionId: "mine", now: Date.now() });
    expect(batch).not.toHaveBeenCalled();
  });

  it("runs on a persistent manager's retention sweep, and not on a secondary scope manager", async () => {
    const { root, mesh } = store();
    await presence(mesh, "gone", "a1");
    const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
    });
    closers.push(() => agents.close());
    const identity: MeshIdentity = { id: "session:host", name: "main", kind: "main", sessionId: "host" };
    const make = (reap: boolean) => {
      const manager = new ActorManager("host", identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {}, {
        actorRoot: path.join(root, reap ? "actors" : "actors-secondary"), persistent: true,
        reapDeadSessionPresence: reap ? { deadAfterMs: 1 } : false,
      });
      closers.push(() => manager.close());
      return manager;
    };
    await new Promise((resolve) => setTimeout(resolve, 20));   // older than the 1 ms test window
    make(false);
    await new Promise((resolve) => setTimeout(resolve, 200));
    expect(mesh.get("actors/gone/a1")).toBeDefined();           // the secondary manager does not reap
    make(true);
    await vi.waitFor(() => expect(mesh.get("actors/gone/a1")).toBeUndefined(), { timeout: 3_000, interval: 20 });
  });
});
