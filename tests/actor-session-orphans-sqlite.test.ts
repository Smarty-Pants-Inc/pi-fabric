import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { SessionActorOrphans } from "../src/actors/session-orphans.js";
import { MeshStore, type MeshIdentity, type MeshStateEntry } from "../src/mesh/store.js";
import { writeHostLease, type FabricHostLease } from "../src/topology/host-leases.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";
import { recordMainSuccessor, registerMainInbox } from "../src/topology/stall-alarms.js";

const hash = (value: string): string => createHash("sha256").update(value).digest("hex");
const participantKey = (id: string): string => `topology/participants/${hash(id)}`;
const tempRoots: string[] = [];
const meshes: MeshStore[] = [];
const repairs: SessionActorOrphans[] = [];

const absentPid = (): number => {
  for (let pid = 2_147_483_647; pid > 2_147_483_627; pid -= 1) {
    try { process.kill(pid, 0); }
    catch (error) { if ((error as NodeJS.ErrnoException).code === "ESRCH") return pid; }
  }
  throw new Error("No absent PID available for the orphan fixture");
};

type SqliteStats = { transactions: number; commits: number; afterCommitReacquired: number };
const sqliteStats = (mesh: MeshStore): SqliteStats =>
  (mesh.stateBackendHandle as unknown as { store: { stats(): SqliteStats } }).store.stats();

const fixture = async (copies: "all" | "legacy-only" = "all") => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-session-orphans-sqlite-"));
  tempRoots.push(root);
  const mesh = new MeshStore(root, 65_536, 1_000, { stateBackend: "sqlite", lockTimeoutMs: 5_000 });
  meshes.push(mesh);
  const old: MeshIdentity = { id: "session:old", sessionId: "old", kind: "main", name: "prior Main" };
  const current: MeshIdentity = { id: "session:new", sessionId: "new", kind: "main", name: "current Main" };
  const at = Date.now() - 300_000;
  const id = "b".repeat(32);
  const actor = { id, name: "project-supervisor", rootId: old.id, project: "trusted-project", residency: "session",
    instructions: "Observe only", runner: "pi", status: "running", createdAt: at - 1_000, updatedAt: at, messages: [] };
  const registry = new ActorRegistryStore(path.join(root, "actors", "old"));
  registry.write([{ ...actor, status: "stopped", stopReason: "host-exit", updatedAt: at,
    sessionOrphan: { oldRoot: old.id, oldHost: os.hostname(), reason: "Main owner process is dead",
      lastUpdated: at, orphanedAt: at, alarmPublishedAt: at } }]);

  const oldOwner = path.join(root, "main-followups", "old.owner.json");
  registerMainInbox(root, old, "old", undefined, "native-lead", "trusted-project");
  registerMainInbox(root, current, "new", undefined, "native-lead", "trusted-project");
  await recordMainSuccessor(mesh, old.id, "old", current.id);
  const registeredOld = JSON.parse(fs.readFileSync(oldOwner, "utf8")) as Record<string, unknown>;
  fs.writeFileSync(oldOwner, JSON.stringify({ ...registeredOld, pid: absentPid(), processStartedAt: "1" }));
  fs.utimesSync(oldOwner, new Date(at), new Date(at));

  const deadLease: FabricHostLease = { id: old.id, rootId: old.id, identityId: old.id, updatedAt: at, expiresAt: at,
    writer: { pid: absentPid(), host: os.hostname(), startedAt: at, releaseSha: "test", lockProtocol: 1, stateBackend: "sqlite" } };
  writeHostLease(root, deadLease);
  writeHostLease(root, { ...deadLease, id: current.id, rootId: current.id, identityId: current.id,
    updatedAt: Date.now(), expiresAt: Date.now() + 60_000,
    writer: { ...deadLease.writer!, pid: process.pid, host: os.hostname(), startedAt: Date.now() } });

  const legacy = `actors/old/${id}`;
  const legacyValue = { ...actor, queued: 7, inFlightRun: { id: "private-run" }, preparing: { id: "private-preparation" } };
  const directoryKey = participantKey(id);
  const historicalClock = vi.spyOn(Date, "now").mockReturnValue(at);
  try {
    await mesh.put({ identity: old, key: legacy, value: legacyValue });
    if (copies === "all") {
      await mesh.put({ identity: old, key: directoryKey, value: { format: 1, id, kind: "actor", rootId: old.id,
        ownerHostId: old.id, ownerIdentityId: old.id, name: actor.name, runner: "pi", residency: "session", transport: "host",
        capabilities: ["steer", "followUp", "fabric", "stop"], status: "running", controlProtocol: "v1",
        startedAt: actor.createdAt, updatedAt: at, actorQueued: 7, actorRun: { id: "private-run", startedAt: at } } });
    }
  } finally { historicalClock.mockRestore(); }
  if (copies === "all") writeParticipantFile(root, mesh.get(directoryKey, { fresh: true })!);

  const history = path.join(root, "actors", "old", id, "session.jsonl");
  fs.mkdirSync(path.dirname(history), { recursive: true });
  fs.writeFileSync(history, "private actor history sentinel\n");
  const repair = new SessionActorOrphans(mesh, current, "new", path.join(root, "actors"), () => true);
  repairs.push(repair);
  const nativeFile = path.join(root, "participants", `${hash(id)}.json`);
  const readNative = (): MeshStateEntry | undefined => {
    try { return JSON.parse(fs.readFileSync(nativeFile, "utf8")) as MeshStateEntry; }
    catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined; throw error; }
  };
  return { root, mesh, old, current, id, actor, registry, repair, legacy, directoryKey, nativeFile, readNative, history };
};

afterEach(async () => {
  vi.restoreAllMocks();
  for (const repair of repairs.splice(0).reverse()) await repair.close();
  for (const mesh of meshes.splice(0).reverse()) mesh.closeState();
  for (const root of tempRoots.splice(0).reverse()) fs.rmSync(root, { recursive: true, force: true });
});

const expectTerminalCopies = (f: Awaited<ReturnType<typeof fixture>>, retired = false): void => {
  const copies = [f.mesh.get(f.legacy, { fresh: true }), f.mesh.get(f.directoryKey, { fresh: true }), f.readNative()];
  expect(copies.every(copy => copy !== undefined)).toBe(true);
  for (const copy of copies) {
    expect(f.repair.validateTombstone(copy!)).toEqual(copy!.value);
    expect(copy!.value).toMatchObject({ status: "stopped", stopReason: "owner-root-gone", capabilities: [],
      ownerRootGoneAt: expect.any(Number), ownerRootGoneBy: f.current.id,
      ...(retired ? { retiredAt: expect.any(Number), retiredBy: f.current.id } : {}) });
    for (const transient of ["actorRun", "actorQueued", "preparing", "inFlightRun", "queued"]) {
      expect(copy!.value).not.toHaveProperty(transient);
    }
  }
  expect(f.readNative()!.updatedBy.id).toBe(f.current.id);
};

const delta = (before: SqliteStats, after: SqliteStats) => ({
  sqliteAcquisitions: after.transactions - before.transactions,
  changedCommits: after.commits - before.commits,
  afterCommitReacquired: after.afterCommitReacquired - before.afterCommitReacquired,
});

type WriteBatchInput = Parameters<MeshStore["writeBatch"]>[0];
type WriteBatch = (input: WriteBatchInput) => ReturnType<MeshStore["writeBatch"]>;
type BatchReceiptTransform = (input: WriteBatchInput, write: WriteBatch) => ReturnType<MeshStore["writeBatch"]>;
type PassReport = { pass: string; stopped: number; writeBatchCalls: number; sqliteAcquisitions: number;
  changedCommits: number; afterCommitReacquired: number; errors: string[] };

const trackWrites = (mesh: MeshStore, transform?: BatchReceiptTransform) => {
  const write = mesh.writeBatch.bind(mesh) as WriteBatch;
  const batches = vi.spyOn(mesh, "writeBatch");
  const errors: string[] = [];
  const message = (error: unknown): string => error instanceof Error ? error.message : String(error);
  batches.mockImplementation(input => {
    let pending: ReturnType<MeshStore["writeBatch"]>;
    try { pending = transform ? transform(input, write) : write(input); }
    catch (error) { errors.push(message(error)); return Promise.reject(error); }
    return pending.catch(error => { errors.push(message(error)); throw error; });
  });
  return { batches, errors };
};

const reconcilePass = async (f: Awaited<ReturnType<typeof fixture>>, name: string,
  writeBatchCalls: () => number, errors: string[]): Promise<PassReport> => {
  const before = sqliteStats(f.mesh);
  const batchCalls = writeBatchCalls();
  const errorCount = errors.length;
  const stopped = await f.repair.reconcile();
  const after = sqliteStats(f.mesh);
  return { pass: name, stopped, writeBatchCalls: writeBatchCalls() - batchCalls, ...delta(before, after), errors: errors.slice(errorCount) };
};

const logPasses = (mesh: MeshStore, passes: PassReport[]): void => {
  console.log(`[sqlite-session-orphan-reconcile] ${JSON.stringify({ backend: mesh.stateBackend, passes })}`);
};

describe("session actor orphan settlement on real SQLite", () => {
  it("commits both directory entries and the native file once, then does no work on later reconcile passes", async () => {
    const f = await fixture();
    const writes = trackWrites(f.mesh);
    const passes: PassReport[] = [];
    passes.push(await reconcilePass(f, "first-settlement", () => writes.batches.mock.calls.length, writes.errors));
    for (let repeat = 1; repeat <= 3; repeat += 1) {
      passes.push(await reconcilePass(f, `repeat-${repeat}`, () => writes.batches.mock.calls.length, writes.errors));
    }
    // Log failures and rollback counters before assertions so a red test retains a useful receipt.
    logPasses(f.mesh, passes);

    expect(f.mesh.stateBackend).toBe("sqlite");
    expect(passes).toEqual([
      { pass: "first-settlement", stopped: 0, writeBatchCalls: 1, sqliteAcquisitions: 2, changedCommits: 1, afterCommitReacquired: 1, errors: [] },
      { pass: "repeat-1", stopped: 0, writeBatchCalls: 0, sqliteAcquisitions: 0, changedCommits: 0, afterCommitReacquired: 0, errors: [] },
      { pass: "repeat-2", stopped: 0, writeBatchCalls: 0, sqliteAcquisitions: 0, changedCommits: 0, afterCommitReacquired: 0, errors: [] },
      { pass: "repeat-3", stopped: 0, writeBatchCalls: 0, sqliteAcquisitions: 0, changedCommits: 0, afterCommitReacquired: 0, errors: [] },
    ]);

    const row = f.registry.records()[0]!;
    expect(row).toMatchObject({ status: "stopped", stopReason: "host-exit",
      sessionOrphan: { oldRoot: f.old.id, directorySettlement: { by: f.current.id, at: expect.any(Number), settledAt: expect.any(Number) } } });
    expectTerminalCopies(f);
    expect(fs.readFileSync(f.history, "utf8")).toBe("private actor history sentinel\n");
  });

  it("also settles a legacy-only advertisement without inventing shared or native copies", async () => {
    const f = await fixture("legacy-only");
    const writes = trackWrites(f.mesh);
    const first = await reconcilePass(f, "legacy-only-settlement", () => writes.batches.mock.calls.length, writes.errors);
    const settled = sqliteStats(f.mesh);
    const repeat = await reconcilePass(f, "legacy-only-repeat", () => writes.batches.mock.calls.length, writes.errors);
    logPasses(f.mesh, [first, repeat]);

    expect(f.mesh.stateBackend).toBe("sqlite");
    expect(first).toEqual({ pass: "legacy-only-settlement", stopped: 0, writeBatchCalls: 1,
      sqliteAcquisitions: 2, changedCommits: 1, afterCommitReacquired: 1, errors: [] });
    expect(repeat).toEqual({ pass: "legacy-only-repeat", stopped: 0, writeBatchCalls: 0,
      sqliteAcquisitions: 0, changedCommits: 0, afterCommitReacquired: 0, errors: [] });
    expect(delta(settled, sqliteStats(f.mesh))).toEqual({ sqliteAcquisitions: 0, changedCommits: 0, afterCommitReacquired: 0 });
    expect(writes.batches).toHaveBeenCalledTimes(1);
    expect(f.registry.records()[0]).toMatchObject({ sessionOrphan: { directorySettlement: { by: f.current.id } } });
    expect(f.mesh.get(f.legacy, { fresh: true })?.value).toMatchObject({ status: "stopped", stopReason: "owner-root-gone", capabilities: [] });
    expect(f.mesh.get(f.directoryKey, { fresh: true })).toBeUndefined();
    expect(f.readNative()).toBeUndefined();
    expect(fs.readFileSync(f.history, "utf8")).toBe("private actor history sentinel\n");
  });

  it("retires through the public API and leaves later reconcile passes write-free", async () => {
    const f = await fixture();
    const writes = trackWrites(f.mesh);
    const first = await reconcilePass(f, "pre-retirement-settlement", () => writes.batches.mock.calls.length, writes.errors);
    writes.batches.mockClear();
    const beforeRetire = sqliteStats(f.mesh);
    const result = await f.repair.retire(f.id, () => {});
    const afterRetire = sqliteStats(f.mesh);
    const beforeRepeat = sqliteStats(f.mesh);
    const repeat = await reconcilePass(f, "post-retirement-repeat", () => writes.batches.mock.calls.length, writes.errors);
    logPasses(f.mesh, [first, repeat]);

    expect(f.mesh.stateBackend).toBe("sqlite");
    expect(first.stopped).toBe(0);
    expect(result).toEqual({ removed: true, retired: true, id: f.id, historyPreserved: true });
    expect(writes.batches).toHaveBeenCalledTimes(1);
    expect(delta(beforeRetire, afterRetire)).toEqual({ sqliteAcquisitions: 2, changedCommits: 1, afterCommitReacquired: 1 });
    expect(repeat).toEqual({ pass: "post-retirement-repeat", stopped: 0, writeBatchCalls: 0,
      sqliteAcquisitions: 0, changedCommits: 0, afterCommitReacquired: 0, errors: [] });
    expect(delta(beforeRepeat, sqliteStats(f.mesh))).toEqual({ sqliteAcquisitions: 0, changedCommits: 0, afterCommitReacquired: 0 });
    expect(writes.batches).toHaveBeenCalledTimes(1);
    expect(f.registry.records()[0]).toMatchObject({ sessionOrphan: { directorySettlement: {
      by: f.current.id, retiredAt: expect.any(Number), retiredBy: f.current.id,
    } } });
    expectTerminalCopies(f, true);
    expect(fs.readFileSync(f.history, "utf8")).toBe("private actor history sentinel\n");
  });

  it("aborts on a shared predecessor changed by a second real SQLite connection before prepare", async () => {
    const f = await fixture();
    const competitor = new MeshStore(f.root, 65_536, 1_000, { stateBackend: "sqlite", lockTimeoutMs: 5_000 });
    meshes.push(competitor);
    const writes = trackWrites(f.mesh);
    let sharedChangedBetweenSnapshotAndPrepare = false;
    const withTryLock = f.mesh.withTryLock.bind(f.mesh);
    vi.spyOn(f.mesh, "withTryLock").mockImplementation(async (operation, timeoutMs) => {
      if (!sharedChangedBetweenSnapshotAndPrepare) {
        const predecessor = competitor.get(f.legacy, { fresh: true })!;
        const historicalClock = vi.spyOn(Date, "now").mockReturnValue(Date.now() - 300_000);
        try {
          await competitor.put({ identity: f.old, key: f.legacy,
            // Keep owner and historical timestamp eligible so the predecessor fence is decisive.
            value: { ...(predecessor.value as Record<string, unknown>), status: "stopped", competitorState: "committed-before-prepare" } });
        } finally { historicalClock.mockRestore(); }
        sharedChangedBetweenSnapshotAndPrepare = true;
      }
      return withTryLock(operation, timeoutMs);
    });

    const pass = await reconcilePass(f, "shared-change-before-prepare", () => writes.batches.mock.calls.length, writes.errors);
    logPasses(f.mesh, [pass]);

    expect(f.mesh.stateBackend).toBe("sqlite");
    expect(sharedChangedBetweenSnapshotAndPrepare).toBe(true);
    expect(pass).toMatchObject({ stopped: 0, writeBatchCalls: 1, sqliteAcquisitions: 1, changedCommits: 0,
      afterCommitReacquired: 0, errors: ["Actor directory copies changed before intent commit"] });
    expect(f.registry.records()[0]?.sessionOrphan).not.toHaveProperty("directorySettlement");
    expect(competitor.get(f.legacy, { fresh: true })?.value).toMatchObject({
      status: "stopped", competitorState: "committed-before-prepare",
    });
    expect(f.mesh.get(f.directoryKey, { fresh: true })?.value).toMatchObject({ status: "running", ownerIdentityId: f.old.id });
    expect(f.readNative()).toMatchObject({ updatedBy: { id: f.old.id }, value: { status: "running" } });
    expect(fs.readFileSync(f.history, "utf8")).toBe("private actor history sentinel\n");
  });

  it("aborts on a native-file predecessor changed after prepare and before beforeCommit", async () => {
    const f = await fixture();
    let prepareReturned = false;
    let nativeChangedAfterPrepare = false;
    const writes = trackWrites(f.mesh, (input, writeBatch) => {
      const isSettlement = input.lockTiming?.operation === "session-orphan-settle";
      const prepare = input.prepare;
      return writeBatch({
        ...input,
        ...(prepare ? { prepare: (view, fileRead) => {
          const ops = prepare(view, fileRead);
          if (isSettlement) prepareReturned = true;
          return ops;
        } } : {}),
        ...(isSettlement ? { beforeCommit: () => {
          if (!prepareReturned) throw new Error("Settlement prepare did not return before beforeCommit");
          const predecessor = f.readNative();
          if (!predecessor) throw new Error("Expected the native participant predecessor");
          const value = predecessor.value as Record<string, unknown>;
          // Keep the original stale owner proof intact; only the file predecessor changes.
          writeParticipantFile(f.root, { ...predecessor, version: predecessor.version + 1,
            value: { ...value, competitorFileState: "committed-after-prepare" } });
          nativeChangedAfterPrepare = true;
          input.beforeCommit?.();
        } } : {}),
      });
    });

    const pass = await reconcilePass(f, "native-file-change-after-prepare", () => writes.batches.mock.calls.length, writes.errors);
    logPasses(f.mesh, [pass]);

    expect(f.mesh.stateBackend).toBe("sqlite");
    expect(prepareReturned).toBe(true);
    expect(nativeChangedAfterPrepare).toBe(true);
    expect(pass).toMatchObject({ stopped: 0, writeBatchCalls: 1, sqliteAcquisitions: 1, changedCommits: 0,
      afterCommitReacquired: 0, errors: ["Actor directory copies changed before intent commit"] });
    expect(f.registry.records()[0]?.sessionOrphan).not.toHaveProperty("directorySettlement");
    expect(f.mesh.get(f.legacy, { fresh: true })?.value).toMatchObject({ status: "running", rootId: f.old.id });
    expect(f.mesh.get(f.directoryKey, { fresh: true })?.value).toMatchObject({ status: "running", ownerIdentityId: f.old.id });
    expect(f.readNative()).toMatchObject({ updatedBy: { id: f.old.id }, value: { status: "running",
      competitorFileState: "committed-after-prepare" } });
    expect(fs.readFileSync(f.history, "utf8")).toBe("private actor history sentinel\n");
  });
});
