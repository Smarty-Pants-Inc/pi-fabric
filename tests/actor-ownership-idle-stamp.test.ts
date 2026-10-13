import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";

// fv2-11107: ActorManager polls pass their managed ids to the snapshot hook; the real directory
// reuses the poll map only while those actors' ownership inputs hold. Authority stays fresh.
const roots: string[] = [];
const closers: Array<() => Promise<unknown>> = [];
const removeFixture = fs.rmSync;
beforeEach(() => {
  vi.spyOn(console, "warn").mockImplementation(() => {});
  const remove = removeFixture;
  // Product queue removal is recorded/no-op locally; real-delete CI is separate (#4664).
  vi.spyOn(fs, "rmSync").mockImplementation((file, options) => {
    if (/^queue-[a-f0-9]{16}\.json$/.test(path.basename(String(file)))) return;
    return remove(file, options);
  });
});
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

  it.each([false, true])("refuses a stale owner's queue mutation after a same-lineage successor takes ownership (non-empty: %s)", async (nonEmpty) => {
    const { actors, directory, writer, putActor } = await setup();
    const actor = await actors.create({ name: "queue-custody", instructions: "Wait", triggerTurn: false });
    await putActor(actor.id, SELF);
    expect(actors.listOwned().map((owned) => owned.id)).toEqual([actor.id]);
    if (nonEmpty) {
      actors.pauseForRelease(); // Accept durable work without launching a worker.
      actors.tell(actor.id, "stale manager's accepted work");
    }
    if (!actor.sessionFile) throw new Error("persistent actor has no session file");
    const key = digest([SELF.id, actor.residency].join("\0")).slice(0, 16);
    const file = path.join(path.dirname(actor.sessionFile), `queue-${key}.json`);
    const bytes = JSON.stringify({ format: 1, items: [{ id: "successor-item", source: "mesh", payload: { text: "successor work" },
      createdAt: Date.now(), activation: {}, attempts: 0, executionStarted: false, preparationAttempts: 0,
      bindingVersion: 2, principalLineageVersion: 1, launchEvidenceVersion: 1 }], latestActivationSequence: 1,
      mainRevision: 0, taskRevision: 0 });
    // Same root and residency means the successor's queue has the SAME filename.
    // Recording is a no-op, NOT an EPERM protection: old product code must fail
    // the zero-attempt assertion. Never execute a product delete locally (#4664).
    const remove = removeFixture;
    const removals: string[] = [];
    const recorder = vi.spyOn(fs, "rmSync").mockImplementation((target, options) => {
      if (path.resolve(String(target)) === file) { removals.push(String(target)); return; }
      return remove(target, options);
    });
    const rename = fs.renameSync;
    const overwrites: string[] = [];
    const writeRecorder = vi.spyOn(fs, "renameSync").mockImplementation((source, target) => {
      if (path.resolve(String(target)) === file) { overwrites.push(String(target)); return; }
      return rename(source, target);
    });
    const get = vi.spyOn(directory, "get");
    try {
      fs.writeFileSync(file, bytes);
      const successor = "host:same-lineage-successor";
      get.mockClear();
      await writer.put({ key: "topology/hosts/" + digest(successor), identity: SELF,
        value: { format: 1, id: successor, rootId: SELF.id, identity: SELF, startedAt: 2,
          updatedAt: Date.now(), expiresAt: Date.now() + 120_000 } });
      const participantKey = "topology/participants/" + digest(actor.id);
      const current = writer.get(participantKey, { fresh: true });
      if (!current) throw new Error("actor participant missing");
      await writer.put({ key: participantKey, identity: SELF,
        value: { ...(current.value as Record<string, unknown>), ownerHostId: successor, updatedAt: Date.now() } });
      expect(directory.list({ scope: "project", fresh: true }).find(row => row.id === actor.id)?.ownerHostId).toBe(successor);
      expect(actors.listOwned()).toEqual([]); // ownership loss -> park -> persist
      expect(removals).toEqual([]);
      expect(overwrites).toEqual([]);
      expect(fs.readFileSync(file, "utf8")).toBe(bytes);
      expect(get).toHaveBeenCalledWith(actor.id, undefined, { fresh: true });
      expect(vi.mocked(console.warn).mock.calls.filter(([text]) => String(text).includes("queue persistence refused"))).toHaveLength(1);

      // Counterexample: the current owner can still persist its own queue.
      // The recorders observe permission without deleting or replacing the file.
      fs.writeFileSync(file, JSON.stringify({ format: 1, items: [], latestActivationSequence: 0 }));
      await putActor(actor.id, SELF);
      expect(actors.listOwned().map((owned) => owned.id)).toEqual([actor.id]);
      if (nonEmpty) expect(overwrites.length).toBeGreaterThan(0);
      else expect(removals).toEqual([file]);
    } finally {
      await actors.close(); // keep recorder armed through any queued ownership work
      recorder.mockRestore();
      writeRecorder.mockRestore();
    }
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
