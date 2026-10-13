import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
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
  const decisionHook: { beforeReturn?: (() => void) | undefined } = {};
  const canManageActor = (id: string, fresh = true) => {
    const participant = directory.get(id, undefined, { fresh });
    const decision = participant ? participant.ownerHostId === HOST : undefined;
    decisionHook.beforeReturn?.();
    return decision;
  };
  const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, { runRoot: path.join(dir, "runs") });
  const actors = new ActorManager("scope", SELF, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 60_000 }, agents, () => {},
    { actorRoot: path.join(dir, "actors"), persistent: true, canManageActor, snapshotActorOwnership: snapshot });
  closers.push(() => agents.close(), () => actors.close());
  return { actors, directory, writer, snapshot, putActor, decisionHook };
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
      else expect(removals).toEqual([file, file]); // acquired queue read + registry reload both checkpoint under fresh custody
    } finally {
      await actors.close(); // keep recorder armed through any queued ownership work
      recorder.mockRestore();
      writeRecorder.mockRestore();
    }
  });

  it.each([false, true])("serializes a successor transfer between the authority check and queue mutation (write: %s)", async (write) => {
    const { actors, directory, writer, putActor, decisionHook } = await setup();
    const actor = await actors.create({ name: "interleaved-custody", instructions: "Wait", triggerTurn: false });
    await putActor(actor.id, SELF);
    actors.listOwned();
    if (!actor.sessionFile) throw new Error("no session file");
    const actorRoot = path.dirname(path.dirname(actor.sessionFile));
    const file = path.join(path.dirname(actor.sessionFile), `queue-${digest([SELF.id, actor.residency].join("\0")).slice(0, 16)}.json`);
    if (write) actors.pauseForRelease();
    const successorBytes = JSON.stringify({ format: 1, items: [{ id: "new-owner-item", payload: { message: "accepted successor work" } }] });
    const script = `
      import fs from "node:fs";
      import { ActorRegistryStore } from ${JSON.stringify(path.resolve("src/actors/registry-store.ts"))};
      import { MeshStore } from ${JSON.stringify(path.resolve("src/mesh/store.ts"))};
      const registry = new ActorRegistryStore(${JSON.stringify(actorRoot)});
      const mesh = new MeshStore(${JSON.stringify(writer.root)}, 65536, 100);
      try {
        await registry.withLock(async () => {
          await mesh.put({ key: ${JSON.stringify("topology/hosts/" + digest("host:successor"))}, identity: ${JSON.stringify(SELF)},
            value: { format: 1, id: "host:successor", rootId: ${JSON.stringify(SELF.id)}, identity: ${JSON.stringify(SELF)}, startedAt: 2, updatedAt: Date.now(), expiresAt: Date.now()+120000 } });
          const key = ${JSON.stringify("topology/participants/" + digest(actor.id))};
          const current = mesh.get(key, { fresh: true });
          await mesh.put({ key, identity: ${JSON.stringify(SELF)}, value: { ...current.value, ownerHostId: "host:successor", updatedAt: Date.now() } });
          fs.writeFileSync(${JSON.stringify(file)}, ${JSON.stringify(successorBytes)});
        }, 0);
        console.log(JSON.stringify({ busy: false, wrote: true }));
      } catch (error) { console.log(JSON.stringify({ busy: true, wrote: false })); }
    `;
    const successor = () => JSON.parse(execFileSync("bun", ["--eval", script], { timeout: 10_000, encoding: "utf8" }).trim()) as { busy: boolean; wrote: boolean };
    const removals: string[] = [], overwrites: string[] = [];
    const remove = removeFixture, rename = fs.renameSync;
    const rm = vi.spyOn(fs, "rmSync").mockImplementation((target, options) => {
      if (path.resolve(String(target)) === file) { removals.push(file); return; }
      return remove(target, options);
    });
    const replace = vi.spyOn(fs, "renameSync").mockImplementation((source, target) => {
      if (path.resolve(String(target)) === file) { overwrites.push(file); return; }
      return rename(source, target);
    });
    let attempted: { busy: boolean; wrote: boolean } | undefined;
    decisionHook.beforeReturn = () => {
      if (!attempted && new Error().stack?.includes("#persistQueue")) attempted = successor();
    };
    try {
      let mutationError: unknown;
      if (write) actors.tell(actor.id, "ordinary owner persistence");
      else await actors.stop(actor.id).catch(error => { mutationError = error; });
      decisionHook.beforeReturn = undefined;
      console.info(`QUEUE_INTERLEAVE write=${write} successor=${JSON.stringify(attempted)} removals=${removals.length} overwrites=${overwrites.length}`);
      expect(attempted).toEqual({ busy: true, wrote: false }); // same cross-process transfer fence
      expect(mutationError).toBeUndefined();
      // The normal owner operation is still allowed; no hidden second wait/fence.
      expect(write ? overwrites : removals).toEqual([file]);
      removals.length = 0; overwrites.length = 0;
      expect(successor()).toEqual({ busy: false, wrote: true }); // released fence -> successor commits
      expect(directory.get(actor.id, undefined, { fresh: true })?.ownerHostId).toBe("host:successor");
      actors.listOwned();
      expect(removals).toEqual([]); expect(overwrites).toEqual([]);
      expect(fs.readFileSync(file, "utf8")).toBe(successorBytes);
    } finally {
      decisionHook.beforeReturn = undefined;
      await actors.close();
      rm.mockRestore(); replace.mockRestore();
    }
  }, 30_000);

  it.each([false, true])("refuses a replaced queue revision even when fresh ownership still says true (write: %s)", async (write) => {
    const { actors, putActor } = await setup();
    const actor = await actors.create({ name: "revision-custody", instructions: "Wait", triggerTurn: false });
    await putActor(actor.id, SELF);
    actors.listOwned();
    if (!actor.sessionFile) throw new Error("no session file");
    const file = path.join(path.dirname(actor.sessionFile), `queue-${digest([SELF.id, actor.residency].join("\0")).slice(0, 16)}.json`);
    if (write) { actors.pauseForRelease(); actors.tell(actor.id, "accepted first work"); }
    const bytes = JSON.stringify({ format: 1, items: [{ id: "other-generation", payload: { message: "new accepted work" } }] });
    const staging = `${file}.fixture`; fs.writeFileSync(staging, bytes); fs.renameSync(staging, file);
    const attempts: string[] = [];
    const remove = removeFixture, rename = fs.renameSync;
    const rm = vi.spyOn(fs, "rmSync").mockImplementation((target, options) => {
      if (path.resolve(String(target)) === file) { attempts.push("remove"); return; } return remove(target, options);
    });
    const replace = vi.spyOn(fs, "renameSync").mockImplementation((source, target) => {
      if (path.resolve(String(target)) === file) { attempts.push("write"); return; } return rename(source, target);
    });
    try {
      if (write) actors.tell(actor.id, "stale work must not overwrite replacement");
      else await actors.stop(actor.id);
      expect(attempts).toEqual([]);
      expect(fs.readFileSync(file, "utf8")).toBe(bytes);
      expect(actors.status(actor.id).lastError).toContain("queue custody changed");
      expect(vi.mocked(console.warn).mock.calls.filter(([text]) => String(text).includes("queue persistence refused"))).toHaveLength(1);
    } finally { await actors.close(); rm.mockRestore(); replace.mockRestore(); }
  });

  it("single-owner persistence takes one zero-wait registry fence per mutation and saves every accepted item", async () => {
    const { actors, putActor } = await setup();
    const actor = await actors.create({ name: "lock-meter", instructions: "Wait", triggerTurn: false });
    await putActor(actor.id, SELF);
    actors.listOwned(); actors.pauseForRelease();
    if (!actor.sessionFile) throw new Error("no session file");
    const root = path.dirname(path.dirname(actor.sessionFile));
    const lock = path.join(root, "actors.json.lock");
    const file = path.join(path.dirname(actor.sessionFile), `queue-${digest([SELF.id, actor.residency].join("\0")).slice(0, 16)}.json`);
    const mkdir = vi.spyOn(fs, "mkdirSync");
    const rename = vi.spyOn(fs, "renameSync");
    try {
      for (let index = 0; index < 20; index++) actors.tell(actor.id, `accepted ${index}`);
      const attempts = mkdir.mock.calls.filter(([target]) => String(target) === lock).length;
      const writes = rename.mock.calls.filter(([, target]) => String(target) === file).length;
      const items = JSON.parse(fs.readFileSync(file, "utf8")).items;
      console.info(`QUEUE_LOCK_METER mutations=${writes} registryAttempts=${attempts} busyRefusals=0`);
      expect(items).toHaveLength(20);
      expect(writes).toBe(20);
      expect(attempts).toBe(writes);
      expect(fs.existsSync(lock)).toBe(false);
    } finally { mkdir.mockRestore(); rename.mockRestore(); }
  });

  it("a busy registry fence retains accepted work and retries once on the existing pending-work deadline", async () => {
    const { actors, putActor } = await setup();
    const actor = await actors.create({ name: "busy-persistence", instructions: "Wait", triggerTurn: false });
    await putActor(actor.id, SELF);
    actors.listOwned(); actors.pauseForRelease();
    if (!actor.sessionFile) throw new Error("no session file");
    const root = path.dirname(path.dirname(actor.sessionFile));
    const file = path.join(path.dirname(actor.sessionFile), `queue-${digest([SELF.id, actor.residency].join("\0")).slice(0, 16)}.json`);
    let release!: () => void, entered!: () => void;
    const ready = new Promise<void>(resolve => { entered = resolve; });
    const gate = new Promise<void>(resolve => { release = resolve; });
    const holder = new ActorRegistryStore(root).withLock(async () => { entered(); await gate; });
    await ready; // outside holder's AsyncLocalStorage scope, so this is real contention
    const makeDirectory = fs.mkdirSync;
    let queueAttempts = 0, existingAsyncAttempts = 0;
    const mkdir = vi.spyOn(fs, "mkdirSync").mockImplementation(((...args: Parameters<typeof fs.mkdirSync>) => {
      if (String(args[0]) === path.join(root, "actors.json.lock")) {
        if (new Error().stack?.includes("withTryLockSync")) queueAttempts++;
        else existingAsyncAttempts++;
      }
      return Reflect.apply(makeDirectory, fs, args);
    }) as typeof fs.mkdirSync);
    const rename = vi.spyOn(fs, "renameSync");
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    try {
      actors.tell(actor.id, "retained through contention");
      expect(fs.existsSync(file)).toBe(false);
      expect(queueAttempts).toBe(1);
      await vi.advanceTimersByTimeAsync(4999);
      expect(rename.mock.calls.filter(([, target]) => String(target) === file)).toHaveLength(0);
      release(); await holder;
      await vi.advanceTimersByTimeAsync(1);
      expect(JSON.parse(fs.readFileSync(file, "utf8")).items).toHaveLength(1);
      expect(rename.mock.calls.filter(([, target]) => String(target) === file)).toHaveLength(1);
      console.info(`QUEUE_BUSY_METER mutations=1 registryAttempts=${queueAttempts} busyRefusals=1 existingAsyncAttempts=${existingAsyncAttempts}`);
      expect(queueAttempts).toBe(2);
    } finally { release(); await holder; vi.useRealTimers(); mkdir.mockRestore(); rename.mockRestore(); }
  });

  it("refuses a moved ownership generation without a queue-file change even when the host id is unchanged", async () => {
    const { actors, writer, putActor } = await setup();
    const actor = await actors.create({ name: "generation-fence", instructions: "Wait", triggerTurn: false });
    await putActor(actor.id, SELF); actors.listOwned(); actors.pauseForRelease();
    actors.tell(actor.id, "first receipt");
    const key = "topology/participants/" + digest(actor.id);
    const current = writer.get(key, { fresh: true })!;
    await writer.put({ key, identity: SELF, value: { ...(current.value as object), actorOwnershipToken: "successor-generation", updatedAt: Date.now() } });
    const rename = vi.spyOn(fs, "renameSync");
    try {
      actors.tell(actor.id, "must stay pending, never replace the old receipt");
      expect(rename.mock.calls.filter(([, target]) => /^queue-[a-f0-9]{16}\.json$/.test(path.basename(String(target))))).toHaveLength(0);
      expect(actors.status(actor.id).lastError).toContain("queue custody changed");
    } finally { rename.mockRestore(); }
  });

  it("keeps the cancellation fallback under the same queue fence after a canonical storage failure", async () => {
    const { actors, putActor } = await setup();
    const actor = await actors.create({ name: "fallback-fence", instructions: "Wait", triggerTurn: false });
    await putActor(actor.id, SELF); actors.listOwned(); actors.pauseForRelease();
    actors.tell(actor.id, "queued work to cancel");
    if (!actor.sessionFile) throw new Error("no session file");
    const root = path.dirname(path.dirname(actor.sessionFile));
    const file = path.join(path.dirname(actor.sessionFile), `queue-${digest([SELF.id, actor.residency].join("\0")).slice(0, 16)}.json`);
    const before = fs.readFileSync(file, "utf8");
    const rename = fs.renameSync, open = fs.openSync, write = fs.writeSync, truncate = fs.ftruncateSync;
    let targetFd: number | undefined, writes = 0, truncates = 0;
    const replace = vi.spyOn(fs, "renameSync").mockImplementation((source, target) => {
      if (String(target) === file) {
        expect(fs.existsSync(path.join(root, "actors.json.lock", "owner"))).toBe(true);
        throw Object.assign(new Error("injected queue rename failure"), { code: "EIO" });
      }
      return rename(source, target);
    });
    const opened = vi.spyOn(fs, "openSync").mockImplementation(((target: fs.PathLike, flags: number | string, ...args: unknown[]) => {
      const fd = Reflect.apply(open, fs, [target, flags, ...args]);
      if (String(target) === file && typeof flags === "number" && (flags & fs.constants.O_RDWR)) targetFd = fd;
      return fd;
    }) as typeof fs.openSync);
    const written = vi.spyOn(fs, "writeSync").mockImplementation(((fd: number, ...args: unknown[]) => {
      if (fd === targetFd) {
        expect(fs.existsSync(path.join(root, "actors.json.lock", "owner"))).toBe(true);
        writes++; return Number(args[2]); // recorded bytes only, never execute product truncate
      }
      return Reflect.apply(write, fs, [fd, ...args]);
    }) as typeof fs.writeSync);
    const shortened = vi.spyOn(fs, "ftruncateSync").mockImplementation((fd, size) => {
      if (fd === targetFd) { truncates++; return; }
      return truncate(fd, size);
    });
    try {
      expect(actors.haltAll()).toEqual({ halted: 1 });
      expect(writes).toBe(1); expect(truncates).toBe(1);
      expect(fs.readFileSync(file, "utf8")).toBe(before); // recorder kept all fixture bytes intact
    } finally { replace.mockRestore(); opened.mockRestore(); written.mockRestore(); shortened.mockRestore(); }
  });

  it("uses descriptor content when a Windows-like volume has inode zero and indistinguishable timestamps", async () => {
    const { actors, putActor } = await setup();
    const actor = await actors.create({ name: "zero-inode", instructions: "Wait", triggerTurn: false });
    await putActor(actor.id, SELF); actors.listOwned(); actors.pauseForRelease();
    if (!actor.sessionFile) throw new Error("no session file");
    const file = path.join(path.dirname(actor.sessionFile), `queue-${digest([SELF.id, actor.residency].join("\0")).slice(0, 16)}.json`);
    const open = fs.openSync, close = fs.closeSync, lstat = fs.lstatSync, fstat = fs.fstatSync;
    const fds = new Set<number>();
    const coarse = (stat: fs.BigIntStats) => Object.assign(Object.create(Object.getPrototypeOf(stat)), stat, { ino: 0n, mtimeNs: 1n, ctimeNs: 1n });
    const opened = vi.spyOn(fs, "openSync").mockImplementation(((...args: Parameters<typeof fs.openSync>) => {
      const fd = Reflect.apply(open, fs, args); if (String(args[0]) === file) fds.add(fd); return fd;
    }) as typeof fs.openSync);
    const closed = vi.spyOn(fs, "closeSync").mockImplementation(fd => { fds.delete(fd); return close(fd); });
    const endpoint = vi.spyOn(fs, "lstatSync").mockImplementation(((target: fs.PathLike, options?: fs.StatSyncOptions) => {
      const stat = lstat(target, options as fs.StatSyncOptions & { bigint: true });
      return String(target) === file && options?.bigint ? coarse(stat as fs.BigIntStats) : stat;
    }) as typeof fs.lstatSync);
    const held = vi.spyOn(fs, "fstatSync").mockImplementation(((fd: number, options?: fs.StatSyncOptions) => {
      const stat = fstat(fd, options as fs.StatSyncOptions & { bigint: true });
      return fds.has(fd) && options?.bigint ? coarse(stat) : stat;
    }) as typeof fs.fstatSync);
    const rename = vi.spyOn(fs, "renameSync");
    try {
      actors.tell(actor.id, "first");
      const saved = fs.readFileSync(file, "utf8");
      fs.writeFileSync(file, saved.replace("first", "other")); // equal length, all reported stamps equal
      rename.mockClear();
      actors.tell(actor.id, "must not replace a different digest");
      expect(rename.mock.calls.filter(([, target]) => String(target) === file)).toHaveLength(0);
      expect(fs.readFileSync(file, "utf8")).toBe(saved.replace("first", "other"));
    } finally { opened.mockRestore(); closed.mockRestore(); endpoint.mockRestore(); held.mockRestore(); rename.mockRestore(); }
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
