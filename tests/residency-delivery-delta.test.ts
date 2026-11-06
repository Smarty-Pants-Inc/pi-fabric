import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { EventEmitter } from "node:events";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { CompletionJournal, pendingCompletions } from "../src/agents/completion-journal.js";
import { MeshStore } from "../src/mesh/store.js";
import { SqliteStateStore } from "../src/mesh/state-sqlite.js";
import { ResidencyClient } from "../src/residency/client.js";
import { MeshLockTimeoutError } from "../src/mesh.js";
import type { AgentRunResult } from "../src/agents/types.js";
import { RESIDENT_HOST_FORMAT, residentDeliveryPrefix, residentHostId, residentResultPath, residentRoot,
  type ResidentHostConfig } from "../src/residency/protocol.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import type { FabricParticipantSource } from "../src/topology/types.js";

// Data safety/performance: exercise the real SQLite read path with an independent writer.
// Client retirement is recorded only; external deletes affect synthetic rows in the temporary fixture.
const sleep = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
const until = async (check: () => boolean) => {
  const end = Date.now() + 3_000;
  while (!check()) { if (Date.now() > end) throw new Error("condition not reached"); await sleep(5); }
};
const result = (id: string, cwd: string): AgentRunResult => ({
  id, name: "legacy", task: "task", status: "completed", runner: "pi", transport: "process",
  cwd, text: "ready later", startedAt: 1, updatedAt: 2, finishedAt: 2, turns: 1, toolCalls: 0,
  usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
});
const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => { for (const cleanup of cleanups.splice(0)) await cleanup(); vi.restoreAllMocks(); });

const fixture = async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "resident-delta-"));
  const config: ResidentHostConfig = {
    format: RESIDENT_HOST_FORMAT, rootId: "session:delta", sessionId: "delta", cwd: root, projectRoot: root,
    mainName: "main", mainStartedAt: 1,
    meshRoot: path.join(root, "mesh"), actorRoot: path.join(root, "actors"), residencyRoot: path.join(root, "resident"),
    fullCodeMode: true, agents: { ...DEFAULT_FABRIC_CONFIG.agents }, mesh: DEFAULT_FABRIC_CONFIG.mesh,
    retention: DEFAULT_FABRIC_CONFIG.retention, workerPath: "worker.js", fabricExtensionPath: "index.js",
    piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
  };
  fs.mkdirSync(path.join(config.meshRoot, "agent-completions"), { recursive: true });
  const mesh = new MeshStore(config.meshRoot, 64 * 1024, 1_000, { stateBackend: "sqlite" });
  const foreign = await SqliteStateStore.open(config.meshRoot, 64 * 1024, 1_000, { changesRetained: 1 });
  const callbacks = new Map<string, (event: string, name: string) => void>();
  vi.spyOn(fs, "watch").mockImplementation(((directory: string, callback: (event: string, name: string) => void) => {
    callbacks.set(directory, callback);
    return Object.assign(new EventEmitter(), { close: vi.fn(), unref: vi.fn() }) as unknown as fs.FSWatcher;
  }) as unknown as typeof fs.watch);
  const deliverAgent = vi.fn();
  const onBackgroundComplete = vi.fn(); // no acknowledgement/source retirement
  const client = new ResidencyClient({ config, mesh, participants: {} as FabricParticipantSource,
    mainAgent: { local: true, deliverAgent } as unknown as FabricMainAgentTarget, onBackgroundComplete });
  const deletion = vi.spyOn(mesh, "delete").mockResolvedValue({ deleted: true });
  const journal = vi.spyOn(CompletionJournal.prototype, "drain");
  const wake = (directory = config.meshRoot, name = "state.db-wal") => callbacks.get(directory)!("change", name);
  const drainWake = async (directory?: string, name?: string) => {
    const before = journal.mock.calls.length;
    wake(directory, name);
    await until(() => journal.mock.calls.length > before);
    await journal.mock.results.at(-1)!.value;
    await sleep(5);
  };
  const start = async () => { client.start(); await until(() => journal.mock.calls.length > 0); await journal.mock.results.at(-1)!.value; await sleep(5); };
  cleanups.push(async () => { await client.close(); foreign.close(); mesh.closeState(); fs.rmSync(root, { recursive: true, force: true }); });
  const identity = { id: "peer", name: "peer", kind: "agent" as const };
  return { root, config, mesh, foreign, client, identity, deliverAgent, onBackgroundComplete, deletion, journal, callbacks, drainWake, start };
};

const seedLegacy = async (f: Awaited<ReturnType<typeof fixture>>) => {
  const rootId = "session:legacy";
  const id = "a".repeat(32);
  const root = residentRoot(f.config.meshRoot, rootId);
  fs.mkdirSync(path.join(root, "results"), { recursive: true });
  fs.writeFileSync(path.join(root, "config.json"), JSON.stringify({ ...f.config, rootId, sessionId: "legacy", residencyRoot: root }));
  const key = residentDeliveryPrefix(rootId) + id;
  await f.foreign.put({ key, identity: { id: residentHostId(rootId), name: "resident", kind: "actor" }, value: {
    format: RESIDENT_HOST_FORMAT, rootId, id, agentCompletionId: id,
    from: { id, name: "legacy", kind: "agent" }, message: "legacy completion", delivery: "followUp", triggerTurn: true,
  } });
  return { id, root, key };
};

describe("resident delivery SQLite delta reads", () => {
  it("N unrelated commits read only key metadata, never select the global snapshot or reread delivery rows/files", async () => {
    const f = await fixture();
    const legacy = await seedLegacy(f);
    for (let i = 0; i < 128; i++) await f.foreign.put({ key: `${residentDeliveryPrefix(f.config.rootId)}seed/${i}`, value: i, identity: f.identity });
    const full = vi.spyOn(SqliteStateStore.prototype, "listAll");
    const exports = vi.spyOn(SqliteStateStore.prototype, "exportLive");
    const deltas = vi.spyOn(SqliteStateStore.prototype, "readDelta");
    const changes = vi.spyOn(SqliteStateStore.prototype, "changesSince");
    const selections = vi.spyOn(f.mesh, "listAllShared");
    const tokens = vi.spyOn(f.mesh, "stateToken");
    const reads = vi.spyOn(fs, "readFileSync");
    await f.start();
    const deliveryScans = () => full.mock.calls.filter(([prefix]) => prefix === "residency/deliveries/").length;
    const initialFull = deliveryScans() + exports.mock.calls.length;
    const deliveryRowsRead = () => full.mock.results.reduce((rows, result, index) =>
      rows + (full.mock.calls[index]?.[0] === "residency/deliveries/" && result.type === "return" ? result.value.length : 0), 0);
    const initialRows = deliveryRowsRead();
    const initialSelections = selections.mock.calls.length;
    const initialTokens = tokens.mock.calls.length;
    const legacyReads = () => reads.mock.calls.filter(([file]) => typeof file === "string" && file.startsWith(legacy.root)).length;
    expect(legacyReads()).toBeGreaterThan(0);
    const initialLegacy = legacyReads();
    for (let i = 0; i < 8; i++) {
      await f.foreign.put({ key: `unrelated/${i}`, value: i, identity: f.identity });
      await f.drainWake();
    }
    // listAllShared filters snapshot.sorted in memory even when SQL reads are incremental.
    // Counting SQL alone missed the CPU regression on every unrelated global revision.
    expect.soft(selections.mock.calls.length - initialSelections).toBe(0);
    expect.soft(tokens.mock.calls.length - initialTokens).toBe(0);
    expect.soft(deliveryScans() + exports.mock.calls.length - initialFull).toBe(0);
    expect.soft(deliveryRowsRead() - initialRows).toBe(0);
    expect.soft(legacyReads()).toBe(initialLegacy);
    expect(deltas).not.toHaveBeenCalled(); // No full-value delta hydration for unrelated keys.
    expect(changes.mock.results.map(result => result.value.changes.length)).toEqual(Array(8).fill(1));
  });

  it.each(["proof", "listing"] as const)("does not swallow prefix writes/deletes racing the %s when advancing its revision", async (phase) => {
    const f = await fixture();
    const legacy = await seedLegacy(f);
    const key = residentDeliveryPrefix(f.config.rootId) + "racing";
    const identity = { id: f.client.hostId, name: "resident", kind: "actor" as const };
    const envelope = (message: string) => ({
      format: RESIDENT_HOST_FORMAT, rootId: f.config.rootId, id: message,
      from: { id: "actor:producer", name: "producer", kind: "actor" },
      message, source: "actor-output", delivery: "followUp", triggerTurn: true,
    });
    await f.foreign.put({ key, identity, value: envelope("before") });
    await f.start();
    expect(f.deliverAgent.mock.calls.map(([request]) => request.message)).toEqual(["before"]);
    const writes: Promise<unknown>[] = [];
    const race = () => {
      const before = f.mesh.stateRevision();
      // An independent connection commits synchronously before these promises settle. Inject
      // AFTER the proof/selection has observed its snapshot, but before it returns to the client.
      writes.push(f.foreign.put({ key, identity, value: envelope("changed") }));
      writes.push(f.foreign.put({ key: key + "-new", identity, value: envelope("new") }));
      writes.push(f.foreign.delete({ key: legacy.key }));
      expect(f.mesh.stateRevision()).not.toBe(before);
    };
    const prove = f.mesh.stateUnchangedSince.bind(f.mesh);
    const proof = vi.spyOn(f.mesh, "stateUnchangedSince");
    const select = f.mesh.listAllShared.bind(f.mesh);
    const selection = vi.spyOn(f.mesh, "listAllShared");
    if (phase === "proof") {
      proof.mockImplementationOnce((...args) => {
        const unchanged = prove(...args);
        expect(unchanged).toBe(true);
        race();
        return unchanged;
      });
    } else {
      // Force a new selection, then land the racing writes after that selection's snapshot.
      await f.foreign.put({ key: key + "-refresh", value: 0, identity: f.identity });
      selection.mockImplementationOnce((...args) => { const entries = select(...args); race(); return entries; });
    }
    await f.drainWake();
    expect(phase === "proof" ? proof : selection).toHaveBeenCalledOnce();
    await Promise.all(writes);
    expect(f.deliverAgent.mock.calls.map(([request]) => request.message)).toEqual(["before"]);
    const reads = vi.spyOn(fs, "readFileSync");
    // A file-dirty hint also proves the deleted pending legacy row left the complete entry cache.
    await f.drainWake(path.join(f.config.meshRoot, "agent-completions"), "ready.json");
    expect(f.deliverAgent.mock.calls.map(([request]) => request.message).sort()).toEqual(["before", "changed", "new"].sort());
    expect(f.deletion.mock.calls.map(([request]) => request.key)).toEqual([key, key, key + "-new"]);
    expect(reads.mock.calls.filter(([file]) => typeof file === "string" && file.startsWith(legacy.root))).toHaveLength(0);
    const settledSelections = selection.mock.calls.length;
    await f.drainWake();
    expect(selection).toHaveBeenCalledTimes(settledSelections);
    expect(f.deliverAgent).toHaveBeenCalledTimes(3);
  });

  it("one trimmed change-feed gap causes exactly one full refresh, then returns to delta reads", async () => {
    const f = await fixture();
    await seedLegacy(f);
    await f.start();
    const full = vi.spyOn(SqliteStateStore.prototype, "listAll");
    const exports = vi.spyOn(SqliteStateStore.prototype, "exportLive");
    const selections = vi.spyOn(f.mesh, "listAllShared");
    const tokens = vi.spyOn(f.mesh, "stateToken");
    for (let i = 0; i < 150; i++) await f.foreign.put({ key: `gap/${i % 3}`, value: i, identity: f.identity });
    await f.drainWake();
    expect(exports).toHaveBeenCalledTimes(1);
    expect(selections).toHaveBeenCalledOnce();
    expect(tokens).toHaveBeenCalledOnce();
    expect(full.mock.calls.filter(([prefix]) => prefix === "residency/deliveries/")).toHaveLength(0);
    await f.foreign.put({ key: "after-gap", value: 1, identity: f.identity });
    await f.drainWake();
    await f.drainWake();
    expect(exports).toHaveBeenCalledTimes(1);
    expect(selections).toHaveBeenCalledOnce();
    expect(tokens).toHaveBeenCalledOnce();
    expect(full.mock.calls.filter(([prefix]) => prefix === "residency/deliveries/")).toHaveLength(0);
  });

  it.each(["filesystem", "next-event safety"])("a same-version pending legacy result becomes ready on its %s wake", async (mode) => {
    const f = await fixture();
    const legacy = await seedLegacy(f);
    await f.start();
    expect(f.onBackgroundComplete).not.toHaveBeenCalled();
    fs.writeFileSync(residentResultPath(legacy.root, legacy.id), JSON.stringify(result(legacy.id, f.root)));
    if (mode === "filesystem") {
      // Existing completion-directory notifications are non-state hints: the mesh token/version
      // need not change. No per-foreign-root watcher graph is necessary.
      await f.drainWake(path.join(f.config.meshRoot, "agent-completions"), "ready.json");
    } else {
      const now = Date.now.bind(Date);
      vi.spyOn(Date, "now").mockImplementation(() => now() + 60_001);
      await f.drainWake(); // No standing timer; an eventual state hint rechecks old pending files.
    }
    // This Main is a different session; the import must still persist the completion journal.
    expect(pendingCompletions(f.config.meshRoot, f.config.projectRoot).find(entry => entry.result.id === legacy.id)?.result.text).toBe("ready later");
    expect(f.mesh.get(legacy.key)?.version).toBe(1);
  });

  it.each(["send", "delete", "deferred delete"])("retries a failed %s at the same snapshot and retains later changed rows", async (failure) => {
    const f = await fixture();
    vi.spyOn(console, "warn").mockImplementation(() => {});
    const prefix = residentDeliveryPrefix(f.config.rootId);
    const envelope = (id: string, rootId = f.config.rootId) => ({
      format: RESIDENT_HOST_FORMAT, rootId, id, from: { id: "actor:producer", name: "producer", kind: "actor" },
      message: id, source: "actor-output", delivery: "followUp", triggerTurn: true,
    });
    const identity = { id: f.client.hostId, name: "resident", kind: "actor" as const };
    await f.foreign.put({ key: prefix + "a", value: envelope("a"), identity });
    // The exact-owner actor record is still ours even under an old foreign mailbox prefix.
    const oldRoot = "session:old-mailbox";
    const oldKey = residentDeliveryPrefix(oldRoot) + "b";
    const oldEntry = await f.foreign.put({ key: oldKey, value: envelope("b", oldRoot), identity });
    if (failure === "send") f.deliverAgent.mockImplementationOnce(() => { throw new Error("send refused"); });
    else f.deletion.mockRejectedValueOnce(failure === "delete" ? new Error("delete refused") : new MeshLockTimeoutError("busy", 1, 100));
    const token = f.mesh.stateToken();
    await f.start();
    await until(() => f.deliverAgent.mock.calls.length >= 3);
    await until(() => f.deletion.mock.calls.length >= (failure === "send" ? 2 : 3));
    expect(f.mesh.stateToken()).toBe(token); // Both retry and later rows were handled without a commit.
    const first = (prefix + "a").localeCompare(oldKey) < 0 ? "a" : "b";
    expect(f.deliverAgent.mock.calls.map(([request]) => request.message).sort()).toEqual(["a", "b", first].sort());
    const attempts = f.deliverAgent.mock.calls.map(([request]) => request.deliveryId);
    expect(new Set(attempts)).toEqual(new Set([`resident:${f.config.rootId}:a`, `resident:${f.config.rootId}:b`]));
    expect(f.deletion).toHaveBeenCalledWith({ key: oldKey, ifVersion: oldEntry.version });
    const settled = f.deliverAgent.mock.calls.length;
    await f.drainWake();
    expect(f.deliverAgent).toHaveBeenCalledTimes(settled);
  });

  it("retains disabled own completions and drains them when notifications resume without a new mesh version", async () => {
    const f = await fixture();
    const id = "b".repeat(32);
    const key = residentDeliveryPrefix(f.config.rootId) + id;
    fs.mkdirSync(path.join(f.config.residencyRoot, "agents"), { recursive: true });
    fs.writeFileSync(path.join(f.config.residencyRoot, "agents", `${id}.json`), JSON.stringify({
      format: RESIDENT_HOST_FORMAT, rootId: f.config.rootId, id, handle: { id },
      runDirectory: path.join(f.config.residencyRoot, "runs", id),
    }));
    vi.spyOn(f.client, "statusAgent").mockReturnValue(result(id, f.root));
    await f.foreign.put({ key, identity: { id: f.client.hostId, name: "resident", kind: "actor" }, value: {
      format: RESIDENT_HOST_FORMAT, rootId: f.config.rootId, id, agentCompletionId: id,
      from: { id, name: "own", kind: "agent" }, message: "completed", delivery: "followUp", triggerTurn: true,
    } });
    f.config.agents.notifyOnComplete = false;
    await f.start();
    expect(f.onBackgroundComplete).not.toHaveBeenCalled();
    expect(f.deletion).not.toHaveBeenCalled();
    expect(f.journal).toHaveBeenLastCalledWith(false);
    f.config.agents.notifyOnComplete = true;
    await f.drainWake();
    expect(f.onBackgroundComplete).toHaveBeenCalledExactlyOnceWith(expect.objectContaining({ id }), expect.any(Function));
    expect(f.deletion).not.toHaveBeenCalled(); // Admission is not a durable consumption receipt.
    expect(f.mesh.get(key)?.version).toBe(1);
  });

  it.each(["stateRevision", "stateUnchangedSince"] as const)("keeps snapshot/delta refresh when the mesh port lacks %s", async (api) => {
    const f = await fixture();
    Object.defineProperty(f.mesh, api, { value: undefined });
    await f.start();
    const tokens = vi.spyOn(f.mesh, "stateToken");
    const selections = vi.spyOn(f.mesh, "listAllShared");
    const deltas = vi.spyOn(SqliteStateStore.prototype, "readDelta");
    await f.foreign.put({ key: "unrelated/fallback", value: 1, identity: f.identity });
    await f.drainWake();
    expect(tokens).toHaveBeenCalledOnce();
    expect(selections).toHaveBeenCalledOnce();
    expect(deltas).toHaveBeenCalledOnce();
  });

  it("an unknown revision cannot certify the cached selection", async () => {
    const f = await fixture();
    const revision = vi.spyOn(f.mesh, "stateRevision").mockReturnValueOnce("unreadable:fixture");
    await f.start();
    const tokens = vi.spyOn(f.mesh, "stateToken");
    const proof = vi.spyOn(f.mesh, "stateUnchangedSince");
    await f.foreign.put({ key: "unrelated/unknown", value: 1, identity: f.identity });
    await f.drainWake();
    expect(proof).toHaveBeenLastCalledWith("unreadable:fixture", ["residency/deliveries/"]);
    expect(proof).toHaveLastReturnedWith(false);
    expect(tokens).toHaveBeenCalledOnce();
    // Once refreshed, the real captured revision may certify an unchanged selection again.
    await f.drainWake();
    expect(revision).toHaveBeenCalledTimes(3);
    expect(proof).toHaveLastReturnedWith(true);
    expect(tokens).toHaveBeenCalledOnce();
  });

  it("keeps the listAll fallback for injected legacy mesh ports", async () => {
    const f = await fixture();
    Object.defineProperty(f.mesh, "stateToken", { value: undefined });
    Object.defineProperty(f.mesh, "listAllShared", { value: undefined });
    const list = vi.spyOn(f.mesh, "listAll");
    await f.start();
    await f.drainWake();
    expect(list.mock.calls.filter(([prefix]) => prefix === "residency/deliveries/")).toHaveLength(2);
  });
});
