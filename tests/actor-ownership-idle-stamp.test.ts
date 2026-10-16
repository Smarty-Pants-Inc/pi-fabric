import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import { ActorMeshMonitor } from "../src/actors/mesh-monitor.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";

// fv2-11107: ActorManager polls pass their managed ids to the snapshot hook; the real directory
// reuses the poll map only while those actors' ownership inputs hold. Authority stays fresh.
const roots: string[] = [];
const closers: Array<() => Promise<unknown>> = [];
beforeEach(() => { vi.spyOn(console, "warn").mockImplementation(() => {}); });
afterEach(async () => {
  vi.restoreAllMocks();
  for (const close of closers.splice(0).reverse()) await close().catch(() => undefined);
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const digest = (id: string): string => createHash("sha256").update(id).digest("hex");
const SELF: MeshIdentity = { id: "session:scope", name: "main", kind: "main" };
const OTHER: MeshIdentity = { id: "session:other", name: "main", kind: "main" };
const HOST = `host:${SELF.id}`;

const setup = async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "actor-ownership-stamp-")); roots.push(dir);
  const mesh = new MeshStore(path.join(dir, "mesh"), 64 * 1024, 100);
  const writer = new MeshStore(mesh.root, 64 * 1024, 100);
  const directory = new ParticipantDirectory(mesh, {
    enabled: true, hostId: "host:observer", rootId: "session:observer",
    identity: { id: "session:observer", name: "main", kind: "main" }, heartbeatMs: 60_000, leaseMs: 120_000,
  });
  closers.push(() => directory.close());
  const putHost = (identity: MeshIdentity) => writer.put({
    key: "topology/hosts/" + digest(`host:${identity.id}`), identity,
    value: { format: 1, id: `host:${identity.id}`, rootId: identity.id, identity, startedAt: 1, updatedAt: Date.now(), expiresAt: Date.now() + 120_000 },
  });
  const putActor = (id: string, owner: MeshIdentity) => writer.put({
    key: "topology/participants/" + digest(id), identity: owner,
    value: { format: 1, id, kind: "actor", rootId: owner.id, ownerHostId: `host:${owner.id}`, ownerIdentityId: owner.id,
      name: id, status: "idle", runner: "pi", transport: "host", capabilities: [], startedAt: 1, updatedAt: Date.now(), controlProtocol: "v1" },
  });
  await putHost(SELF); await putHost(OTHER);
  const snapshot = vi.fn((fresh = true, actorIds?: readonly string[]) =>
    directory.actorOwnership(HOST, { fresh, ...(fresh ? {} : { actorIds }) }));
  const canManageActor = (id: string, fresh = true) => {
    const participant = directory.get(id, undefined, { fresh });
    return participant ? participant.ownerHostId === HOST : undefined;
  };
  const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, { runRoot: path.join(dir, "runs") });
  const actors = new ActorManager("scope", SELF, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 60_000 }, agents, () => {},
    { actorRoot: path.join(dir, "actors"), persistent: true, canManageActor, snapshotActorOwnership: snapshot });
  closers.push(() => agents.close(), () => actors.close());
  return { actors, directory, writer, snapshot, putActor };
};

// Exercise the real synchronous monitor callback without a worker or a native deletion path.
// releasePaused holds callerless mesh work; only guarded Node fs cleans these private fixtures.
const setupMeshDelivery = () => {
  let dispatch!: ActorMeshMonitor["callbacks"]["onEvent"];
  vi.spyOn(ActorMeshMonitor.prototype, "start").mockImplementation(function (this: ActorMeshMonitor) {
    dispatch = this.callbacks.onEvent;
  });
  vi.spyOn(ActorMeshMonitor.prototype, "schedule").mockImplementation(() => {});
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "actor-mesh-authority-")); roots.push(dir);
  const mesh = new MeshStore(path.join(dir, "mesh"), 64 * 1024, 100);
  const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, { runRoot: path.join(dir, "runs") });
  const run = vi.spyOn(agents, "run").mockImplementation(() => { throw new Error("mesh authority probe must not launch a worker"); });
  closers.push(() => agents.close());
  return { dir, mesh, agents, run, dispatch: (event: Parameters<typeof dispatch>[0]) => dispatch(event) };
};

describe("matching mesh delivery fresh authority (safe local coverage of the hosted-only manager case)", () => {
  it.each([false, true])("refreshes once for eight subscribers through full queues and skips delivered duplicates (snapshot %s)", async (batched) => {
    const { dir, mesh, agents, run, dispatch } = setupMeshDelivery();
    const ids: string[] = [];
    const decisions = vi.fn((_id: string, _fresh?: boolean) => true as boolean | undefined);
    const snapshot = vi.fn((_fresh = true) => new Map(ids.map(id => [id, true])));
    const actors = new ActorManager("scope", SELF, mesh,
      { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 60_000, actorQueueLimit: 2 }, agents, () => {},
      { actorRoot: path.join(dir, "actors"), persistent: false, releasePaused: true,
        canManageActor: decisions, ...(batched ? { snapshotActorOwnership: snapshot } : {}) });
    closers.push(() => actors.close());
    for (let index = 0; index < 8; index++) {
      ids.push((await actors.create({ name: `subscriber-${index}`, instructions: "Watch.", topics: ["team.pulls"], responseMode: "text", coalesce: false })).id);
    }
    for (let index = 0; index < 20; index++) {
      const event = await mesh.publish({ topic: "team.pulls", from: OTHER, data: { index } });
      decisions.mockClear(); snapshot.mockClear();
      expect(dispatch(event)).toBe(true);
      // Only the synchronous delivery slice: async continuations must revalidate separately.
      expect(decisions).toHaveBeenCalledTimes(batched ? 0 : 8);
      expect(snapshot.mock.calls).toEqual(batched ? [[true]] : []);
      expect(decisions.mock.calls).toEqual(batched ? [] : ids.map(id => [id, true]));
      const messages = ids.map(id => actors.messages(id, 100));
      decisions.mockClear(); snapshot.mockClear();
      expect(dispatch(event)).toBe("ignored");
      expect(decisions).not.toHaveBeenCalled();
      expect(snapshot).not.toHaveBeenCalled();
      expect(ids.map(id => actors.messages(id, 100))).toEqual(messages);
    }
    // This is not merely a call-count test: all subscribers retained work and hit overflow.
    expect(ids.map(id => actors.status(id).queued)).toEqual(ids.map(() => 18)); // 2 queued + 16 overflow
    for (const id of ids) {
      const messages = actors.messages(id, 100);
      expect(messages.filter(message => message.direction === "in")).toHaveLength(18);
      expect(messages.filter(message => message.error?.startsWith("Dropped a queued event:"))).toHaveLength(2);
    }
    expect(run).not.toHaveBeenCalled();
  });

  it("refuses a fresh takeover even when the observation would allow, and retries the same unaccepted event", async () => {
    const { dir, mesh, agents, run, dispatch } = setupMeshDelivery();
    let owns = true;
    // Counterexample: omitting true (or passing false) returns the stale permissive view.
    const decisions = vi.fn((_id: string, fresh?: boolean) => fresh === true ? owns : true);
    const actors = new ActorManager("scope", SELF, mesh,
      { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 60_000, actorQueueLimit: 2 }, agents, () => {},
      { actorRoot: path.join(dir, "actors"), persistent: false, releasePaused: true, canManageActor: decisions });
    closers.push(() => actors.close());
    const actor = await actors.create({ name: "takeover", instructions: "Watch.", topics: ["team.pulls"], responseMode: "text", coalesce: false });
    const event = await mesh.publish({ topic: "team.pulls", from: OTHER, text: "must wait for fresh authority" });
    owns = false;
    decisions.mockClear();
    expect(dispatch(event)).toBe("ignored");
    expect(decisions.mock.calls).toEqual([[actor.id, true]]);
    expect(actors.messages(actor.id, 100)).toEqual([]);
    owns = true;
    decisions.mockClear();
    expect(dispatch(event)).toBe(true);
    expect(decisions.mock.calls).toEqual([[actor.id, true]]);
    const messages = actors.messages(actor.id, 100);
    expect(messages).toHaveLength(1);
    expect(messages[0]).toMatchObject({ direction: "in", source: "mesh:team.pulls", data: { id: event.id } });
    decisions.mockClear();
    expect(dispatch(event)).toBe("ignored");
    expect(decisions).not.toHaveBeenCalled();
    expect(actors.messages(actor.id, 100)).toEqual(messages);
    expect(run).not.toHaveBeenCalled();
  });
});

describe("fv2-11107 actor poll ownership stamp gate on a real ParticipantDirectory", () => {
  it("passes managed ids on polls only, reuses across unrelated traffic, and fresh tell sees a takeover", async () => {
    const { actors, directory, writer, snapshot, putActor } = await setup();
    const actor = await actors.create({ name: "owned", instructions: "Reply" });
    await putActor(actor.id, SELF);
    const list = vi.spyOn(directory, "list");
    snapshot.mockClear();
    // Idle poll: managed ids are named; the first builds, the rest reuse.
    expect(actors.listOwned().map((owned) => owned.id)).toEqual([actor.id]);
    expect(snapshot).toHaveBeenLastCalledWith(false, [actor.id]);
    const built = list.mock.calls.length;
    // One poll list plus one fresh arming read after the observation.
    expect(built).toBe(2);
    expect(list.mock.calls[1]?.[0]?.fresh).toBe(true);
    for (let index = 0; index < 5; index++) {
      await writer.put({ key: `traffic/${index}`, value: { index }, identity: OTHER });
      await putActor(`actor:peer-${index}`, OTHER);
      expect(actors.listOwned().map((owned) => owned.id)).toEqual([actor.id]);
    }
    expect(list).toHaveBeenCalledTimes(built);
    // Takeover: the fresh admission path decides from current facts and refuses.
    await putActor(actor.id, OTHER);
    snapshot.mockClear();
    await expect(Promise.resolve().then(() => actors.tell(actor.id, "must not run"))).rejects.toThrow(/another host/);
    expect(snapshot.mock.calls.every(([fresh, ids]) => fresh !== false || ids !== undefined)).toBe(true);
    expect(snapshot.mock.calls.some(([fresh]) => fresh === true)).toBe(true);
    // The poll observes the move too (its observation of this actor changed).
    expect(actors.listOwned()).toEqual([]);
    // Rightful restoration: ownership returns and is visible on the next poll.
    await putActor(actor.id, SELF);
    expect(actors.listOwned().map((owned) => owned.id)).toEqual([actor.id]);
  });

  it("fresh reads never take the poll map even while it is still armed", async () => {
    const { actors, directory, putActor, snapshot } = await setup();
    const actor = await actors.create({ name: "held", instructions: "Reply" });
    await putActor(actor.id, SELF);
    actors.listOwned();
    const held = directory.actorOwnership(HOST, { fresh: false, actorIds: [actor.id] });
    expect(held.get(actor.id)).toBe(true);
    const list = vi.spyOn(directory, "list");
    snapshot.mockClear();
    // A fresh decision always lists exactly, regardless of the armed poll map.
    const fresh = directory.actorOwnership(HOST, { fresh: true, actorIds: [actor.id] });
    expect(fresh).not.toBe(held);
    expect(list).toHaveBeenCalledWith({ scope: "project", fresh: true });
  });
});
