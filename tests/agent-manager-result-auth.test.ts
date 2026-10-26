import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import type { AgentPresenceInfo, AgentRunRecord } from "../src/agents/types.js";
import { agentParticipantRecords, SETTLED_AGENT_PUBLISH_MS } from "../src/topology/records.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { writeJsonAtomic } from "../src/core/atomic-write.js";
import { saveFinalAnswerReceipt } from "../src/worker/terminal-answer.js";
import { fakeManagerHeapWorker, heapRunResult } from "./fixtures/manager-heap-probe.js";

// Keep reason: the worker/manager trust boundary reported by Astra on #1122.
// All removal effects are recorded; private fixture roots are never deleted.
const managers: AgentManager[] = [];
const deletionRequests: string[] = [];
const recordedMissing = new Set<string>();
const filesystem = { readFileSync: fs.readFileSync, lstatSync: fs.lstatSync, existsSync: fs.existsSync, renameSync: fs.renameSync };
let failPublication = false;
const missing = () => Object.assign(new Error("recorded removal"), { code: "ENOENT" });
const recordRemoval = (file: fs.PathLike): void => {
  deletionRequests.push(String(file));
  recordedMissing.add(String(file));
};
const releases: Array<() => void> = [];
const root = () => fs.mkdtempSync(path.join(os.tmpdir(), "manager-result-auth-"));
const createManager = (runRoot: string, retainRuns = true,
  options: NonNullable<ConstructorParameters<typeof AgentManager>[2]> = {}): AgentManager => {
  const manager = new AgentManager(process.cwd(), {
    ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, maxConcurrent: 1, retainRuns, sessionExport: false,
  }, { runRoot, workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fullCodeMode: false, ...options });
  managers.push(manager);
  return manager;
};
const recordRunCollection = (directory: string) => {
  const resultFile = path.join(directory, "manager-result.json"), statusFile = path.join(directory, "status.json");
  vi.mocked(fs.promises.rm).mockImplementation(async file => {
    recordRemoval(file);
    if (String(file) === directory) {
      // Model whole-tree removal at the manager's actual removal boundary.
      // The existing lstat/read/exists observations see absence; bytes stay put.
      recordedMissing.add(resultFile);
      recordedMissing.add(statusFile);
    }
  });
  return { resultFile, statusFile };
};
const refuses = async (manager: AgentManager, id: string) => {
  // Observe both public paths before asserting, so the RED run proves that both
  // status and wait accepted the forgery rather than stopping at the first failure.
  let status: unknown;
  try { status = manager.status(id).status; }
  catch (error) { status = (error as { code?: string }).code; }
  const wait = await manager.wait(id).then(result => result.status, error => error.code);
  expect({ status, wait }).toEqual({ status: "FABRIC_MISSING_RESULT", wait: "FABRIC_MISSING_RESULT" });
};

// #17018 keep reason: a real manager publication with a hold hard-link must
// fail result authentication without taking task presence or the whole UI down.
const listForPresence = (manager: AgentManager): AgentPresenceInfo[] => manager.listForPresence();

const hardLinkedEvictedResult = async () => {
  vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
    const handle = await fakeManagerHeapWorker(request, 64);
    if (request.name === "evicted result") {
      const file = request.workerArguments[request.workerArguments.indexOf("--status-file") + 1]!;
      const record = JSON.parse(fs.readFileSync(file, "utf8"));
      // Public byte-pressure eviction, not private-field mutation or count
      // overflow. This single large preview cannot fit the snapshot budget.
      writeJsonAtomic(file, { ...record, nestedAgents: Array.from({ length: 12 }, (_, index) =>
        heapRunResult(`nested-${index}`, 64_000)) });
    }
    return handle;
  });
  const runRoot = root(), manager = createManager(runRoot);
  const result = await manager.run({ name: "evicted result", task: "private result payload", transport: "process", extensions: false });
  const directory = path.join(runRoot, result.id), file = path.join(directory, "manager-result.json");
  expect(result.nestedAgents).toHaveLength(12);
  const opened = vi.spyOn(fs, "openSync");
  // An authenticated backing open proves the completed run's preview was
  // byte-evicted before the attack. It is too large for the UI list as well.
  expect(manager.listForUi().some(record => record.id === result.id)).toBe(false);
  expect(opened.mock.calls.some(([read]) => String(read) === file)).toBe(true);
  opened.mockRestore();
  const hold = fs.mkdtempSync(path.join(runRoot, "trace-hold-"));
  fs.linkSync(file, path.join(hold, "manager-result.json"));
  fs.linkSync(path.join(directory, "status.json"), path.join(hold, "status.json"));
  expect(fs.statSync(file).nlink).toBe(2);
  // A new public run invalidates any UI-list cache after the hard-link. It is
  // also the healthy neighbour whose visibility an unreadable task must not kill.
  const healthy = await manager.run({ name: "healthy neighbour", task: "healthy task", transport: "process", extensions: false });
  await refuses(manager, result.id);
  return { manager, result, healthy, file };
};

const enodevFixture = () => {
  const runRoot = root(), id = "1".repeat(32), actorId = "a".repeat(32), activationId = "item-1";
  const directory = path.join(runRoot, id);
  fs.mkdirSync(directory, { mode: 0o700 });
  const { finishedAt: _finishedAt, ...record } = heapRunResult(id);
  fs.writeFileSync(path.join(directory, "status.json"), JSON.stringify({ ...record, status: "running", actorId }));
  fs.writeFileSync(path.join(directory, "task.txt"), JSON.stringify({ id: activationId }, null, 2));
  const marker = path.join(directory, "unresolved-worker.json"), archive = path.join(directory, "archive-pending.json");
  fs.writeFileSync(marker, JSON.stringify({ reason: "Owned scope observation failed: Error: ENODEV", runId: id }));
  fs.writeFileSync(archive, JSON.stringify({ format: 1, awaitingResult: true, ownerPid: 1 }));
  fs.writeFileSync(path.join(directory, "settlement.json"), JSON.stringify({ version: 1, runId: id, outcome: "completed", text: "recovered answer" }));
  const manager = createManager(runRoot);
  // Recovery must bypass spawn. A request after committed ack must not replay;
  // refuse its fresh-spawn path explicitly so no duplicate worker is launched.
  const spawned = vi.spyOn(manager, "spawn").mockRejectedValue(new Error("new worker not authorized by fixture"));
  return { manager, id, directory, marker, archive, spawned, request: { task: "no rerun", actorId, activationId } };
};

beforeEach(() => {
  deletionRequests.length = 0;
  recordedMissing.clear();
  failPublication = false;
  vi.spyOn(fs, "rmSync").mockImplementation(recordRemoval);
  vi.spyOn(fs, "unlinkSync").mockImplementation(recordRemoval);
  vi.spyOn(fs, "rmdirSync").mockImplementation(recordRemoval);
  vi.spyOn(fs.promises, "rm").mockImplementation(async file => { recordRemoval(file); });
  // Missing-file effects are observations only; no removal reaches the real fs.
  vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, ...args: unknown[]) => {
    if (recordedMissing.has(String(file))) throw missing();
    return Reflect.apply(filesystem.readFileSync, fs, [file, ...args]);
  }) as typeof fs.readFileSync);
  vi.spyOn(fs, "lstatSync").mockImplementation(((file: fs.PathLike, ...args: unknown[]) => {
    if (recordedMissing.has(String(file))) throw missing();
    return Reflect.apply(filesystem.lstatSync, fs, [file, ...args]);
  }) as typeof fs.lstatSync);
  vi.spyOn(fs, "existsSync").mockImplementation(file => !recordedMissing.has(String(file)) && filesystem.existsSync(file));
  vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
    if (failPublication && path.basename(String(to)) === "manager-result.json") {
      throw Object.assign(new Error("seeded publication EIO"), { code: "EIO" });
    }
    filesystem.renameSync(from, to);
    recordedMissing.delete(String(to));
  });
  vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(request => fakeManagerHeapWorker(request));
});
afterEach(async () => {
  failPublication = false;
  for (const release of releases.splice(0)) release();
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  vi.restoreAllMocks();
  // Private roots remain for the environment owner; no local cleanup deletes.
});

describe("authenticated manager terminal results", () => {
  it("isolates presence metadata from a hard-linked byte-evicted result and all result-bearing readers (#17018)", async () => {
    const { manager, result, healthy } = await hardLinkedEvictedResult();
    const forbidden = () => { throw new Error("presence entered a result-bearing reader"); };
    vi.spyOn(manager, "status").mockImplementation(forbidden);
    vi.spyOn(manager, "list").mockImplementation(forbidden);
    vi.spyOn(manager, "listForUi").mockImplementation(forbidden);
    vi.mocked(fs.readFileSync).mockClear();
    vi.mocked(fs.readFileSync).mockImplementation(forbidden);
    vi.mocked(fs.lstatSync).mockClear();
    vi.mocked(fs.lstatSync).mockImplementation(forbidden);
    const opened = vi.spyOn(fs, "openSync").mockImplementation(forbidden);
    const directories = vi.spyOn(fs, "readdirSync").mockImplementation(forbidden);
    const directoryHandles = vi.spyOn(fs, "opendirSync").mockImplementation(forbidden);
    const stats = vi.spyOn(fs, "statSync").mockImplementation(forbidden);
    const descriptorStats = vi.spyOn(fs, "fstatSync").mockImplementation(forbidden);
    const descriptorReads = vi.spyOn(fs, "readSync").mockImplementation(forbidden);
    let records: AgentPresenceInfo[];
    try {
      records = listForPresence(manager);
      expect(fs.readFileSync).not.toHaveBeenCalled();
      expect(fs.lstatSync).not.toHaveBeenCalled();
      expect(opened).not.toHaveBeenCalled();
      expect(directories).not.toHaveBeenCalled();
      expect(directoryHandles).not.toHaveBeenCalled();
      expect(stats).not.toHaveBeenCalled();
      expect(descriptorStats).not.toHaveBeenCalled();
      expect(descriptorReads).not.toHaveBeenCalled();
    } finally {
      // afterEach must still close the genuine manager through ordinary I/O.
      vi.mocked(fs.readFileSync).mockImplementation(((file: fs.PathOrFileDescriptor, ...args: unknown[]) => {
        if (recordedMissing.has(String(file))) throw missing();
        return Reflect.apply(filesystem.readFileSync, fs, [file, ...args]);
      }) as typeof fs.readFileSync);
      vi.mocked(fs.lstatSync).mockImplementation(((file: fs.PathLike, ...args: unknown[]) => {
        if (recordedMissing.has(String(file))) throw missing();
        return Reflect.apply(filesystem.lstatSync, fs, [file, ...args]);
      }) as typeof fs.lstatSync);
      opened.mockRestore();
      directories.mockRestore();
      directoryHandles.mockRestore();
      stats.mockRestore();
      descriptorStats.mockRestore();
      descriptorReads.mockRestore();
    }
    expect(records).toEqual(expect.arrayContaining([
      expect.objectContaining({ id: result.id, name: result.name, status: "completed", runner: result.runner,
        transport: result.transport, startedAt: result.startedAt, updatedAt: result.updatedAt, finishedAt: result.finishedAt }),
      expect.objectContaining({ id: healthy.id, status: "completed" }),
    ]));
    for (const record of records) {
      for (const field of ["task", "text", "value", "error", "warnings", "partialText", "lastCompleteText", "nestedAgents", "turns", "toolCalls", "usage"]) {
        expect(record).not.toHaveProperty(field);
      }
    }
    expect(JSON.stringify(records).length).toBeLessThan(8_192);
    const publish = (now: number) => agentParticipantRecords(records, "root", "host", "identity", "main", new Map(), now);
    expect(publish(result.finishedAt!).map(record => record.id)).toContain(result.id);
    expect(publish(result.finishedAt! + SETTLED_AGENT_PUBLISH_MS + 1).map(record => record.id)).not.toContain(result.id);
    records.find(record => record.id === result.id)!.name = "caller mutation";
    expect(listForPresence(manager).find(record => record.id === result.id)?.name).toBe(result.name);
  });

  it("retains active and queued lifecycle metadata, actor filtering and detached owner facts (#17018)", async () => {
    let alive = true, release!: () => void;
    const closed = new Promise<void>(resolve => { release = () => { alive = false; resolve(); }; });
    releases.push(release);
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
      const handle = await fakeManagerHeapWorker(request, 64, false, { closed, alive: () => alive, stop: async () => release() });
      const file = request.workerArguments[request.workerArguments.indexOf("--status-file") + 1]!;
      const record = JSON.parse(fs.readFileSync(file, "utf8"));
      writeJsonAtomic(file, { ...record, status: "running", finishedAt: undefined,
        startedAt: 101, updatedAt: 202, model: "observed-model", runnerSessionId: "native-session" });
      return handle;
    });
    const manager = createManager(root(), true, { mainAgentId: "main:presence", fabricSessionId: "session:presence" });
    const active = await manager.spawn({ task: "active metadata", transport: "process", extensions: false,
      residency: "durable", capabilityRequirements: ["files"], capabilityDigest: "capability-digest" });
    // Observe real worker progress once, then presence must retain these facts without polling.
    manager.status(active.id);
    const queued = await manager.spawn({ task: "queued metadata", actorId: "actor:presence", actorName: "Actor presence",
      transport: "process", extensions: false });
    const records = manager.listForPresence();
    expect(records).toEqual(expect.arrayContaining([
      expect.objectContaining({ id: active.id, status: "running", startedAt: 101, updatedAt: 202, residency: "durable",
        model: "observed-model", runnerSessionId: "native-session", mainAgentId: "main:presence", fabricSessionId: "session:presence",
        capabilityRequirements: ["files"], capabilityDigest: "capability-digest", spawner: active.spawner }),
      expect.objectContaining({ id: queued.id, status: "queued", queuePosition: 1, actorId: "actor:presence", actorName: "Actor presence",
        mainAgentId: "main:presence", fabricSessionId: "session:presence" }),
    ]));
    const waiting = records.find(record => record.id === queued.id)!;
    expect(waiting.startedAt).toBe(waiting.updatedAt);
    expect(waiting).not.toHaveProperty("finishedAt");
    const projected = agentParticipantRecords(records, "root", "host", "identity", "main", new Map());
    expect(projected.map(record => record.id)).toEqual([active.id]);
    expect(projected[0]).toMatchObject({ startedAt: 101, updatedAt: 202, residency: "durable", capabilities: ["steer", "followUp", "stop"] });
    const running = records.find(record => record.id === active.id)!;
    running.capabilityRequirements!.push("caller mutation");
    if (running.spawner) running.spawner.id = "caller mutation";
    expect(manager.listForPresence().find(record => record.id === active.id)).toMatchObject({
      startedAt: 101, updatedAt: 202, capabilityRequirements: ["files"], spawner: active.spawner,
    });
    const clock = vi.spyOn(Date, "now").mockReturnValue(Date.now() + SETTLED_AGENT_PUBLISH_MS + 1);
    try {
      const later = manager.listForPresence();
      expect(later.find(record => record.id === active.id)).toMatchObject({ startedAt: 101, updatedAt: 202 });
      expect(later.find(record => record.id === queued.id)).toMatchObject({ startedAt: waiting.startedAt, updatedAt: waiting.updatedAt });
    } finally { clock.mockRestore(); }
    const stopped = await manager.stop(queued.id);
    expect(manager.listForPresence().find(record => record.id === queued.id)).toMatchObject({
      status: "stopped", startedAt: stopped.startedAt, updatedAt: stopped.updatedAt, finishedAt: stopped.finishedAt,
    });
    const statusFile = path.join(manager.runDirectory(active.id)!, "status.json");
    const progress = JSON.parse(fs.readFileSync(statusFile, "utf8"));
    writeJsonAtomic(statusFile, { ...progress, updatedAt: 304, finishingAt: 303 });
    expect(manager.status(active.id).status).toBe("finishing");
    const finishing = manager.listForPresence().find(record => record.id === active.id)!;
    expect(finishing).toMatchObject({ status: "finishing", startedAt: 101, updatedAt: 304, finishingAt: 303 });
    expect(finishing).not.toHaveProperty("finishedAt");
    // This raw-finishing fixture has no final-answer receipt: native supervision may
    // fail it during stop. Presence must preserve that actual outcome, not normalize it.
    const stoppedActive = await manager.stop(active.id);
    expect(["stopped", "failed"]).toContain(stoppedActive.status);
    expect(manager.listForPresence().find(record => record.id === active.id)).toMatchObject({
      status: stoppedActive.status, startedAt: 101, updatedAt: stoppedActive.updatedAt, finishedAt: stoppedActive.finishedAt,
    });
  });

  it("projects result-free placement and finish/update expiry without assuming usage exists (#17018)", () => {
    const finishedAt = 1_000;
    const record: AgentPresenceInfo = { id: "presence-placement", name: "placement", status: "completed", runner: "pi",
      transport: "process", cwd: "/work", startedAt: 100, updatedAt: finishedAt, finishedAt,
      placementRunId: "placement-run", placementKind: "remote", placementHost: "placement-host", expectedSeconds: 300,
      recursive: true, sessionId: "transport-session", finalAnswerReceipt: { id: "receipt", recordedAt: finishedAt } };
    const project = (records: AgentPresenceInfo[], now: number) =>
      agentParticipantRecords(records, "root", "host", "identity", "main", new Map(), now);
    const [participant] = project([record], finishedAt + SETTLED_AGENT_PUBLISH_MS);
    expect(participant).toMatchObject({ id: record.id, rootId: "root", ownerHostId: "host", ownerIdentityId: "identity", parentId: "main",
      startedAt: 100, updatedAt: finishedAt, finishedAt, placementRunId: "placement-run", placementKind: "remote",
      placementHost: "placement-host", expectedSeconds: 300, sessionId: "transport-session", capabilities: ["fabric"], finalAnswerReceiptId: "receipt" });
    for (const field of ["turns", "toolCalls", "usage", "task", "text", "value"]) expect(participant).not.toHaveProperty(field);
    expect(project([record], finishedAt + SETTLED_AGENT_PUBLISH_MS + 1)).toEqual([]);
    const { finishedAt: _finishedAt, ...withoutFinish } = record;
    expect(project([withoutFinish], finishedAt + SETTLED_AGENT_PUBLISH_MS + 1)).toEqual([]);
  });

  it("keeps byte-evicted finishing metadata and native custody separate from an unreadable answer (#17018)", async () => {
    let alive = true, release!: () => void;
    const closed = new Promise<void>(resolve => { release = () => { alive = false; resolve(); }; });
    releases.push(release);
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
      const handle = await fakeManagerHeapWorker(request, 64, true, { closed, alive: () => alive, stop: () => closed });
      const file = request.workerArguments[request.workerArguments.indexOf("--status-file") + 1]!;
      const record = JSON.parse(fs.readFileSync(file, "utf8"));
      writeJsonAtomic(file, { ...record, nestedAgents: Array.from({ length: 12 }, (_, index) => heapRunResult(`finishing-${index}`, 64_000)) });
      return handle;
    });
    const runRoot = root(), manager = createManager(runRoot);
    const handle = await manager.spawn({ task: "large finishing metadata", transport: "process", extensions: false });
    const file = path.join(runRoot, handle.id, "manager-result.json");
    await vi.waitFor(() => expect(fs.existsSync(file)).toBe(true));
    const sealed = JSON.parse(fs.readFileSync(file, "utf8"));
    const opened = vi.spyOn(fs, "openSync");
    expect(manager.listForUi()).toEqual([expect.objectContaining({ id: handle.id, status: "finishing" })]);
    expect(opened.mock.calls.some(([read]) => String(read) === file)).toBe(false); // evicted preview is a handle
    const before = manager.listForPresence();
    expect(before).toEqual([expect.objectContaining({ id: handle.id, status: "finishing", startedAt: sealed.startedAt,
      updatedAt: sealed.updatedAt, finalAnswerReceipt: sealed.finalAnswerReceipt })]);
    expect(before[0]?.finishedAt).toBeUndefined();
    fs.linkSync(file, path.join(runRoot, "finishing-hold.json"));
    expect(fs.statSync(file).nlink).toBe(2);
    opened.mockClear();
    expect(manager.listForPresence()).toEqual(before);
    expect(opened).not.toHaveBeenCalled();
    opened.mockRestore();
    expect(manager.isSettled(handle.id)).toBe(false);
    expect(manager.hasRunCustody(handle.id)).toBe(true);
    expect(() => manager.status(handle.id)).toThrow(expect.objectContaining({ code: "FABRIC_MISSING_RESULT" }));
    const [participant] = agentParticipantRecords(before, "root", "host", "identity", "main", new Map());
    expect(participant).toMatchObject({ status: "finishing", capabilities: ["stop"], finalAnswerReceiptId: sealed.finalAnswerReceipt.id });
    expect(participant).not.toHaveProperty("usage");
    release();
    const terminal = await manager.wait(handle.id, { timeoutMs: 2_000 });
    expect(terminal.status).toBe("failed");
    expect(terminal.error).toMatch(/no saved terminal result.*authentication\/integrity failure/);
    expect(manager.listForPresence()[0]).toMatchObject({ status: "failed", startedAt: sealed.startedAt, finishedAt: terminal.finishedAt });
  });

  it("keeps a hard-linked byte-evicted result visible as a per-task UI integrity error, not a fabricated result (#17018)", async () => {
    const { manager, result, healthy } = await hardLinkedEvictedResult();
    let records: ReturnType<AgentManager["listForUi"]> | undefined, failure: unknown;
    try { records = manager.listForUi(); } catch (error) { failure = error; }
    expect(failure, "one task integrity failure must not throw the entire UI list").toBeUndefined();
    expect(records).toEqual(expect.arrayContaining([
      expect.objectContaining({ id: healthy.id, status: "completed" }),
      expect.objectContaining({ id: result.id, status: "completed", error: expect.stringMatching(/FABRIC_MISSING_RESULT.*no saved terminal result/) }),
    ]));
    const unreadable = records!.find(record => record.id === result.id)!;
    expect((unreadable as { error?: string }).error).toMatch(/authenticat|integrity/i);
    for (const field of ["task", "text", "value", "nestedAgents", "usage"]) expect(unreadable).not.toHaveProperty(field);
    expect(manager.listForUi()).toEqual(records); // Cached error remains visible.
    // Presentation isolation must not turn status/wait into a successful result.
    await refuses(manager, result.id);
    expect(await manager.wait(healthy.id)).toMatchObject(healthy);
  });

  it("still returns the genuine full manager-published result through status/wait/stop", async () => {
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(request => fakeManagerHeapWorker(request, 32_000));
    const manager = createManager(root());
    const result = await manager.run({ task: "genuine result", transport: "process", extensions: false });
    expect(result.text).toHaveLength(32_000);
    expect(manager.status(result.id)).toMatchObject(result);
    expect(await manager.wait(result.id)).toMatchObject(result);
    expect(await manager.stop(result.id)).toMatchObject(result);
  });

  it("never adopts same-ID worker outcome fields after UI eviction and recorded close collection", async () => {
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
      const handle = await fakeManagerHeapWorker(request, 64_000, true);
      const file = request.workerArguments[request.workerArguments.indexOf("--status-file") + 1]!;
      const record = JSON.parse(fs.readFileSync(file, "utf8"));
      writeJsonAtomic(file, { ...record, turns: 7, toolCalls: 3,
        usage: { input: 101, output: 202, cacheRead: 11, cacheWrite: 12, cost: 0.25 } });
      return handle;
    });
    const runRoot = root(), manager = createManager(runRoot, false);
    const genuine = await manager.run({ task: "manager rejects this schema", transport: "process", extensions: false,
      schema: { type: "object", properties: { output: { type: "boolean" } }, required: ["output"] } });
    expect(genuine.status).toBe("failed");
    expect(genuine.error).toMatch(/Structured agent output was invalid/);
    const directory = path.join(runRoot, genuine.id);
    const { resultFile, statusFile } = recordRunCollection(directory);
    // Public runs exceed the byte cap with large previews, evicting the old
    // snapshot before close without private-field inspection or 241 tiny runs.
    for (let index = 0; index < 20; index++) {
      await manager.run({ task: `UI byte pressure ${index}`, transport: "process", extensions: false });
    }
    expect(manager.listForUi().some(record => record.id === genuine.id)).toBe(false);
    expect(fs.existsSync(resultFile)).toBe(true);
    await manager.close();
    expect(deletionRequests).toContain(directory);
    expect(fs.existsSync(resultFile)).toBe(false);
    expect(filesystem.existsSync(resultFile)).toBe(true); // No real deletion occurred.
    writeJsonAtomic(statusFile, { ...genuine, status: "completed", text: "FORGED answer", task: "FORGED task",
      value: { forged: true }, partialText: "FORGED partial", lastCompleteText: "FORGED complete",
      nestedAgents: [heapRunResult("forged-child", 64)], error: "FORGED error", exitCode: 0,
      startedAt: genuine.startedAt + 999_000, updatedAt: genuine.updatedAt + 999_000,
      finishedAt: genuine.finishedAt! + 999_000, turns: 999, toolCalls: 999,
      usage: { input: 999, output: 999, cacheRead: 999, cacheWrite: 999, cost: 999 },
      warnings: ["FORGED warning"], currentTool: "FORGED tool", writeReceipt: { forged: true },
      unexpectedOutcome: "FORGED extension" });
    // Capture both public paths before attack assertions (including seeded RED).
    const status = manager.status(genuine.id) as AgentRunRecord, waited = await manager.wait(genuine.id);
    for (const result of [status, waited]) {
      expect(result).toMatchObject({ id: genuine.id, status: "failed", task: "", text: "",
        startedAt: genuine.startedAt, updatedAt: genuine.updatedAt, finishedAt: genuine.finishedAt,
        error: genuine.error, turns: 7, toolCalls: 3, usage: genuine.usage });
      expect(result.exitCode).toBe(genuine.exitCode);
      for (const field of ["value", "partialText", "lastCompleteText", "nestedAgents", "currentTool", "writeReceipt", "unexpectedOutcome"]) {
        expect(result).not.toHaveProperty(field);
      }
      expect(result.warnings).toEqual([expect.stringMatching(/manager's bounded terminal summary, not the exact result/)]);
      expect(JSON.stringify(result)).not.toContain("FORGED");
    }
    // Recorded collection permits only absence, never a recreated foreign file.
    writeJsonAtomic(resultFile, { ...genuine, status: "completed", text: "FORGED recreated result" });
    await refuses(manager, genuine.id);
  });

  it("keeps bounded manager-owned facts after ordinary collection, not retained UI outcome payloads", async () => {
    const longError = `validated failure ${"x".repeat(12_000)}`;
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
      const handle = await fakeManagerHeapWorker(request, 32_000, true);
      const file = request.workerArguments[request.workerArguments.indexOf("--status-file") + 1]!;
      const record = JSON.parse(fs.readFileSync(file, "utf8"));
      writeJsonAtomic(file, { ...record, status: "failed", error: longError, exitCode: 23, turns: 9, toolCalls: 4,
        usage: { input: 21, output: 34, cacheRead: 5, cacheWrite: 8, cost: 0.125 },
        nestedAgents: [heapRunResult("original-child", 64)] });
      return handle;
    });
    const runRoot = root(), manager = createManager(runRoot, false);
    const genuine = await manager.run({ task: "ordinary collected failure", transport: "process", extensions: false });
    const { resultFile } = recordRunCollection(path.join(runRoot, genuine.id));
    // Before collection authentication preserves the exact full outcome, including
    // diagnostics larger than the summary bound and the original nested tree.
    const uncollected = manager.status(genuine.id) as AgentRunRecord, uncollectedWait = await manager.wait(genuine.id);
    for (const result of [uncollected, uncollectedWait]) {
      expect(result.error).toBe(longError);
      expect(result.text).toHaveLength(32_000);
      expect(result.value).toEqual(genuine.value);
      expect(result.nestedAgents?.map(record => record.id)).toEqual(["original-child"]);
    }
    await manager.close();
    expect(fs.existsSync(resultFile)).toBe(false);
    expect(filesystem.existsSync(resultFile)).toBe(true);
    const status = manager.status(genuine.id) as AgentRunRecord, waited = await manager.wait(genuine.id);
    for (const result of [status, waited]) {
      expect(result).toMatchObject({ id: genuine.id, status: "failed", name: genuine.name, runner: genuine.runner,
        transport: genuine.transport, cwd: genuine.cwd, startedAt: genuine.startedAt, updatedAt: genuine.updatedAt,
        finishedAt: genuine.finishedAt, exitCode: 23, turns: 9, toolCalls: 4, usage: genuine.usage, task: "", text: "" });
      expect(result.error).toBe(`${longError.slice(0, 1_000)}…`);
      for (const field of ["value", "partialText", "lastCompleteText", "nestedAgents"]) expect(result).not.toHaveProperty(field);
      expect(result.warnings).toEqual([expect.stringMatching(/manager's bounded terminal summary, not the exact result/)]);
    }
    status.usage.input = 999;
    expect((await manager.wait(genuine.id)).usage.input).toBe(21);
  });

  it("never rereads late nested worker status after settlement, even after UI snapshot eviction", async () => {
    let childStatusFile = "";
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
      const handle = await fakeManagerHeapWorker(request, 64_000);
      if (request.name === "nested source") {
        const statusFile = request.workerArguments[request.workerArguments.indexOf("--status-file") + 1]!;
        const directory = path.join(path.dirname(statusFile), "nested", "original-child");
        fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
        childStatusFile = path.join(directory, "status.json");
        writeJsonAtomic(childStatusFile, { ...heapRunResult("original-child", 64), text: "original nested answer" });
      }
      return handle;
    });
    const manager = createManager(root());
    const genuine = await manager.run({ name: "nested source", task: "freeze discovered nested results", transport: "process", extensions: false });
    expect(genuine.nestedAgents).toEqual([expect.objectContaining({ id: "original-child", text: "original nested answer" })]);
    // Evict the nested snapshot through public byte-pressure operations, so the
    // seeded regression must rediscover disk rather than hit its refresh window.
    for (let index = 0; index < 20; index++) {
      await manager.run({ task: `Nested UI byte pressure ${index}`, transport: "process", extensions: false });
    }
    expect(manager.listForUi().some(record => record.id === genuine.id)).toBe(false);
    writeJsonAtomic(childStatusFile, { ...heapRunResult("original-child", 64),
      text: "FORGED nested answer", value: { forged: true }, error: "FORGED nested error" });
    vi.mocked(fs.readFileSync).mockClear();
    const status = manager.status(genuine.id) as AgentRunRecord, waited = await manager.wait(genuine.id);
    const nestedReads = vi.mocked(fs.readFileSync).mock.calls.filter(([file]) => String(file) === childStatusFile);
    // Capture both public results and read observations before attack assertions.
    for (const result of [status, waited]) expect(result.nestedAgents).toEqual(genuine.nestedAgents);
    expect(nestedReads).toHaveLength(0);
    expect(JSON.stringify([status, waited])).not.toContain("FORGED");
  });

  it("refreshes cached live descendants at settlement inside the discovery throttle, then freezes them", async () => {
    let alive = true, release!: () => void, childStatusFile = "";
    const closed = new Promise<void>(resolve => { release = () => { alive = false; resolve(); }; });
    releases.push(release);
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
      const handle = await fakeManagerHeapWorker(request, 64, false,
        { closed, alive: () => alive, stop: async () => { release(); } });
      const statusFile = request.workerArguments[request.workerArguments.indexOf("--status-file") + 1]!;
      const record = JSON.parse(fs.readFileSync(statusFile, "utf8"));
      writeJsonAtomic(statusFile, { ...record, status: "running", finishedAt: undefined });
      const directory = path.join(path.dirname(statusFile), "nested", "live-child");
      fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
      childStatusFile = path.join(directory, "status.json");
      writeJsonAtomic(childStatusFile, { ...heapRunResult("live-child", 64), status: "running", finishedAt: undefined,
        text: "live nested progress" });
      return handle;
    });
    vi.spyOn(Date, "now").mockReturnValue(Date.now()); // No timer advance can expire the 500 ms cache.
    const manager = createManager(root());
    const handle = await manager.spawn({ task: "live nested discovery", transport: "process", extensions: false });
    const status = manager.status(handle.id) as AgentRunRecord;
    const nestedReads = vi.mocked(fs.readFileSync).mock.calls.filter(([file]) => String(file) === childStatusFile);
    expect(status).toMatchObject({ status: "running",
      nestedAgents: [expect.objectContaining({ id: "live-child", status: "running", text: "live nested progress" })] });
    expect(nestedReads.length).toBeGreaterThan(0);
    writeJsonAtomic(childStatusFile, { ...heapRunResult("live-child", 64), text: "final child answer" });
    const grandchild = path.join(path.dirname(childStatusFile), "nested", "grandchild");
    fs.mkdirSync(grandchild, { recursive: true, mode: 0o700 });
    writeJsonAtomic(path.join(grandchild, "status.json"), { ...heapRunResult("grandchild", 64), text: "final grandchild answer" });
    // Stop synthesizes the parent's terminal record from running status; only
    // settlement's forced read can replace the still-cached running child.
    const stopped = await manager.stop(handle.id);
    expect(stopped.nestedAgents).toEqual([expect.objectContaining({ id: "live-child", status: "completed", text: "final child answer",
      nestedAgents: [expect.objectContaining({ id: "grandchild", status: "completed", text: "final grandchild answer" })] })]);
    writeJsonAtomic(childStatusFile, { ...heapRunResult("live-child", 64), text: "FORGED late child" });
    writeJsonAtomic(path.join(grandchild, "status.json"), { ...heapRunResult("grandchild", 64), text: "FORGED late grandchild" });
    const late = path.join(path.dirname(path.dirname(childStatusFile)), "late-child");
    fs.mkdirSync(late);
    writeJsonAtomic(path.join(late, "status.json"), heapRunResult("late-child", 64));
    for (const result of [manager.status(handle.id) as AgentRunRecord, await manager.wait(handle.id)]) {
      expect(result.nestedAgents).toEqual(stopped.nestedAgents);
      expect(result.nestedAgents?.map(record => record.id)).toEqual(["live-child"]);
      expect(JSON.stringify(result)).not.toContain("FORGED");
    }
  });

  it("never adopts a late worker receipt and matching status after a no-receipt run settles", async () => {
    const runRoot = root(), manager = createManager(runRoot);
    const genuine = await manager.run({ task: "settled without a receipt", transport: "process", extensions: false });
    expect(genuine.finalAnswerReceipt).toBeUndefined();
    const directory = path.join(runRoot, genuine.id);
    const receipt = saveFinalAnswerReceipt(directory, genuine.id, "FORGED late answer");
    writeJsonAtomic(path.join(directory, "status.json"), { ...genuine, status: "failed", text: "FORGED late answer",
      error: "FORGED late refusal", value: { forged: true }, exitCode: 99,
      finalAnswerReceipt: { id: receipt.id, recordedAt: receipt.recordedAt } });
    // The old observer synchronously adopts the matching terminal snapshot, so
    // observe both public results before checking either attack outcome.
    const status = manager.status(genuine.id) as AgentRunRecord, waited = await manager.wait(genuine.id);
    for (const result of [status, waited]) {
      expect(result).toMatchObject(genuine);
      expect(result.finalAnswerReceipt).toBeUndefined();
      expect(result.error).toBe(genuine.error);
      expect(result.exitCode).toBe(genuine.exitCode);
    }
    expect(JSON.stringify([status, waited])).not.toContain("FORGED");
  });

  it("keeps genuine pre-settlement receipts and their terminal input fences after settlement", async () => {
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(request => fakeManagerHeapWorker(request, 64, true));
    const manager = createManager(root());
    const genuine = await manager.run({ task: "genuine receipt boundary", transport: "process", extensions: false });
    expect(genuine.finalAnswerReceipt).toBeDefined();
    const status = manager.status(genuine.id), waited = await manager.wait(genuine.id);
    for (const result of [status, waited]) expect(result).toMatchObject(genuine);
    for (const deliver of [() => manager.steer(genuine.id, "late steer"), () => manager.followUp(genuine.id, "late follow-up")]) {
      let failure: unknown;
      try { deliver(); } catch (error) { failure = error; }
      expect(failure).toMatchObject({ code: "FABRIC_TARGET_TERMINAL", targetId: genuine.id,
        finalAnswerReceiptId: genuine.finalAnswerReceipt!.id, runState: "completed", deliveryStatus: "undelivered" });
    }
  });

  it("publishes monitor-validated schema failure to UI while native close still holds settlement", async () => {
    let alive = true, release!: () => void;
    const closed = new Promise<void>(resolve => { release = () => { alive = false; resolve(); }; });
    releases.push(release);
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
      const handle = await fakeManagerHeapWorker(request, 64, false,
        { closed, alive: () => alive, stop: () => closed });
      const statusFile = request.workerArguments[request.workerArguments.indexOf("--status-file") + 1]!;
      const record = JSON.parse(fs.readFileSync(statusFile, "utf8"));
      writeJsonAtomic(statusFile, { ...record, status: "running", finishedAt: undefined });
      return handle;
    });
    const runRoot = root(), onSettled = vi.fn();
    const manager = new AgentManager(process.cwd(), {
      ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, maxConcurrent: 1, retainRuns: true, sessionExport: false,
    }, { runRoot, workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fullCodeMode: false, onSettled });
    managers.push(manager);
    const handle = await manager.spawn({ task: "monitor-only UI validation", transport: "process", extensions: false,
      schema: { type: "object", properties: { output: { type: "boolean" } }, required: ["output"] } });
    expect(manager.listForUi()).toEqual([expect.objectContaining({ id: handle.id, status: "running" })]);
    let waitReleased = false;
    const waited = manager.wait(handle.id).then(result => { waitReleased = true; return result; });
    const directory = path.join(runRoot, handle.id), statusFile = path.join(directory, "status.json");
    const record = JSON.parse(fs.readFileSync(statusFile, "utf8"));
    const receipt = saveFinalAnswerReceipt(directory, handle.id, record.text);
    writeJsonAtomic(statusFile, { ...record, status: "completed", finishedAt: receipt.recordedAt,
      finalAnswerReceipt: { id: receipt.id, recordedAt: receipt.recordedAt } });
    // Only the real monitor may observe/validate the receipt: status() would
    // repair the UI cache itself and hide the regression under test.
    const resultFile = path.join(directory, "manager-result.json");
    await vi.waitFor(() => expect(fs.existsSync(resultFile)).toBe(true));
    const sealed = JSON.parse(fs.readFileSync(resultFile, "utf8"));
    expect(sealed).toMatchObject({ status: "failed", error: expect.stringMatching(/Structured agent output was invalid/) });
    expect({ alive, waitReleased }).toEqual({ alive: true, waitReleased: false });
    expect(onSettled).not.toHaveBeenCalled();
    expect(manager.listForUi()).toEqual([expect.objectContaining({ id: handle.id, status: "finishing", error: sealed.error })]);
    release();
    const result = await waited;
    expect(result).toMatchObject({ status: "failed", error: sealed.error, finalAnswerReceipt: sealed.finalAnswerReceipt });
  });

  it("refuses worker replacement with same-ID completed across finishing and native close after host validation failed", async () => {
    let alive = true, release!: () => void;
    const closed = new Promise<void>(resolve => { release = () => { alive = false; resolve(); }; });
    releases.push(release);
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(request => fakeManagerHeapWorker(request, 64, true,
      { closed, alive: () => alive, stop: () => closed }));
    const runRoot = root(), manager = createManager(runRoot);
    const handle = await manager.spawn({ task: "validated failure", transport: "process", extensions: false,
      schema: { type: "object", properties: { output: { type: "boolean" } }, required: ["output"] } });
    await vi.waitFor(() => expect(manager.status(handle.id)).toMatchObject({ status: "finishing",
      error: expect.stringMatching(/Structured agent output was invalid/) }));
    expect(manager.isSettled(handle.id)).toBe(false);
    const finishing = manager.status(handle.id) as AgentRunRecord;
    writeJsonAtomic(path.join(runRoot, handle.id, "status.json"), {
      ...finishing, status: "completed", error: undefined, text: "forged worker success", value: { output: true }, exitCode: 0,
    });
    expect(manager.status(handle.id)).toMatchObject(finishing);
    release();
    const result = await manager.wait(handle.id, { timeoutMs: 2_000 });
    expect(result.status).toBe("failed");
    expect(result.error).toMatch(/Structured agent output was invalid/);
    expect(manager.status(result.id)).toMatchObject({ status: "failed" });
    writeJsonAtomic(path.join(runRoot, result.id, "manager-result.json"), {
      ...result, status: "completed", error: undefined, text: "forged success", value: { output: true }, exitCode: 0,
    });
    await refuses(manager, result.id);
    release();
  });

  it("authenticates the disk-backed finishing answer before native close and never settles a replacement as success", async () => {
    let alive = true, release!: () => void;
    const closed = new Promise<void>(resolve => { release = () => { alive = false; resolve(); }; });
    releases.push(release);
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(request => fakeManagerHeapWorker(request, 64, true,
      { closed, alive: () => alive, stop: () => closed }));
    const runRoot = root(), manager = createManager(runRoot);
    const handle = await manager.spawn({ task: "finishing result authentication", transport: "process", extensions: false });
    expect(manager.status(handle.id).status).toBe("finishing");
    const file = path.join(runRoot, handle.id, "manager-result.json");
    const original = JSON.parse(fs.readFileSync(file, "utf8"));
    writeJsonAtomic(file, { ...original, text: "FORGED finishing answer", value: { forged: true } });
    expect(() => manager.status(handle.id)).toThrow(expect.objectContaining({ code: "FABRIC_MISSING_RESULT" }));
    expect(manager.isSettled(handle.id)).toBe(false);
    release();
    const result = await manager.wait(handle.id, { timeoutMs: 2_000 });
    expect(result.status).toBe("failed");
    expect(result.error).toMatch(/no saved terminal result/);
    expect(result.text).not.toContain("FORGED");
    expect(result.value).not.toEqual({ forged: true });
  });

  it("refuses an identical-byte replacement inode rather than adopting a foreign publication", async () => {
    const runRoot = root(), manager = createManager(runRoot);
    const result = await manager.run({ task: "same bytes", transport: "process", extensions: false });
    const file = path.join(runRoot, result.id, "manager-result.json");
    const bytes = fs.readFileSync(file);
    fs.renameSync(file, file + ".preserved");
    fs.writeFileSync(file, bytes, { mode: 0o600 });
    await refuses(manager, result.id);
  });

  it("refuses in-place same-size mutation even with restored mtime", async () => {
    const runRoot = root(), manager = createManager(runRoot);
    const result = await manager.run({ task: "same-size", transport: "process", extensions: false });
    const file = path.join(runRoot, result.id, "manager-result.json");
    const original = fs.readFileSync(file, "utf8");
    const changed = original.replace("same-size", "same-forg");
    expect(changed).not.toBe(original);
    expect(Buffer.byteLength(changed)).toBe(Buffer.byteLength(original));
    const stat = fs.statSync(file);
    fs.writeFileSync(file, changed, { mode: 0o600 });
    fs.utimesSync(file, stat.atime, stat.mtime);
    await refuses(manager, result.id);
  });

  it.skipIf(process.platform === "win32")("refuses a manager-result symlink while preserving the genuine target", async () => {
    const runRoot = root(), manager = createManager(runRoot);
    const result = await manager.run({ task: "symlink result", transport: "process", extensions: false });
    const file = path.join(runRoot, result.id, "manager-result.json");
    const preserved = file + ".preserved";
    fs.renameSync(file, preserved);
    fs.symlinkSync(preserved, file);
    await refuses(manager, result.id);
    expect(fs.existsSync(preserved)).toBe(true);
  });

  it("keeps original and restored generations readable across managers sharing a run root", async () => {
    const runRoot = root(), first = createManager(runRoot);
    const original = await first.run({ task: "reload original answer", transport: "process", extensions: false });
    await first.close();
    const directory = path.join(runRoot, original.id);
    const originalFile = path.join(directory, "manager-result.json");
    const originalBytes = fs.readFileSync(originalFile, "utf8");
    const generations = [{ manager: first, result: original }];
    const savedBackings: Array<{ file: string; bytes: string }> = [];
    for (const status of ["stopped", "failed", "completed"] as const) {
      const restored = createManager(runRoot);
      const result = { ...original, status, text: `restored ${status} answer` };
      restored.restorePreviousRuns([result]);
      generations.push({ manager: restored, result });
      const files = fs.readdirSync(directory).filter(name => name.startsWith("manager-restored-result-"));
      expect(files).toHaveLength(savedBackings.length + 1);
      const file = path.join(directory, files.find(name => !savedBackings.some(saved => saved.file === path.join(directory, name)))!);
      savedBackings.push({ file, bytes: fs.readFileSync(file, "utf8") });
      await restored.close();
      for (const generation of generations) {
        expect(generation.manager.status(original.id)).toEqual(generation.result);
        expect(await generation.manager.wait(original.id)).toEqual(generation.result);
      }
      expect(fs.readFileSync(originalFile, "utf8")).toBe(originalBytes);
      for (const saved of savedBackings) expect(fs.readFileSync(saved.file, "utf8")).toBe(saved.bytes);
    }
    // Isolation is not permission to adopt a foreign replacement of any backing.
    writeJsonAtomic(originalFile, { ...original, text: "foreign original replacement" });
    await refuses(first, original.id);
    writeJsonAtomic(savedBackings[0]!.file, { ...generations[1]!.result, text: "foreign restored replacement" });
    await refuses(generations[1]!.manager, original.id);
    for (const generation of generations.slice(2)) {
      expect(generation.manager.status(original.id)).toEqual(generation.result);
      expect(await generation.manager.wait(original.id)).toEqual(generation.result);
    }
  });

  it("republishes only caller-supplied archives after restart and refuses later disk replacement", async () => {
    const runRoot = root(), first = createManager(runRoot);
    const archived = await first.run({ task: "caller archive", transport: "process", extensions: false });
    await first.close();
    const unknownId = "disk-only-result", unknownDirectory = path.join(runRoot, unknownId);
    fs.mkdirSync(unknownDirectory, { recursive: true, mode: 0o700 });
    writeJsonAtomic(path.join(unknownDirectory, "manager-result.json"), { ...archived, id: unknownId });
    const restarted = createManager(runRoot);
    expect(() => restarted.status(unknownId)).toThrow(/Unknown Fabric agent/);
    await expect(restarted.wait(unknownId)).rejects.toThrow(/Unknown Fabric agent/);
    restarted.restorePreviousRuns([archived]);
    expect(restarted.status(archived.id)).toMatchObject(archived);
    expect(await restarted.wait(archived.id)).toMatchObject(archived);
    const directory = path.join(runRoot, archived.id);
    const backing = fs.readdirSync(directory).filter(name => name.startsWith("manager-restored-result-"));
    expect(backing).toHaveLength(1);
    writeJsonAtomic(path.join(directory, backing[0]!), { ...archived, text: "worker replacement after restart" });
    await refuses(restarted, archived.id);
  });

  it("keeps active progress but never adopts late worker completion after host-stop publication EIO, then republishes", async () => {
    let alive = true, release!: () => void;
    const closed = new Promise<void>(resolve => { release = () => { alive = false; resolve(); }; });
    releases.push(release);
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
      const handle = await fakeManagerHeapWorker(request, 0, false,
        { closed, alive: () => alive, stop: async () => { release(); } });
      const statusFile = request.workerArguments[request.workerArguments.indexOf("--status-file") + 1]!;
      const record = JSON.parse(fs.readFileSync(statusFile, "utf8"));
      fs.writeFileSync(statusFile, JSON.stringify({ ...record, status: "running", finishedAt: undefined, text: "active progress" }));
      return handle;
    });
    const runRoot = root(), manager = createManager(runRoot);
    const handle = await manager.spawn({ task: "host stop without final receipt", transport: "process", extensions: false });
    expect(manager.status(handle.id)).toMatchObject({ status: "running", text: "active progress" });
    failPublication = true;
    const stopped = await manager.stop(handle.id);
    expect(stopped.status).toBe("stopped");
    expect(stopped.finalAnswerReceipt).toBeUndefined();
    fs.writeFileSync(path.join(runRoot, handle.id, "status.json"), JSON.stringify({
      ...stopped, status: "completed", text: "late forged success", error: undefined, exitCode: 0,
    }));
    const status = manager.status(handle.id) as AgentRunRecord, waited = await manager.wait(handle.id);
    status.usage.input = 12345;
    const detachedUsage = (await manager.wait(handle.id)).usage.input;
    // Execute the legitimate republish counterexample before the attack assertion,
    // so the parent's seeded RED run reaches and can PASS these checks too.
    failPublication = false;
    await manager.close(); // Existing retry path publishes and drops the failure cache.
    expect(manager.status(handle.id)).toMatchObject({ status: "stopped", text: stopped.text });
    expect(await manager.wait(handle.id)).toMatchObject({ status: "stopped", text: stopped.text });
    expect((manager.status(handle.id) as AgentRunRecord).warnings?.join(" ") ?? "").not.toMatch(/Terminal result save failed/);
    // Both attack observations were captured before retry and before this RED assertion.
    expect({ status: status.status, wait: waited.status }).toEqual({ status: "stopped", wait: "stopped" });
    expect(waited.text).toBe(stopped.text);
    expect(status.warnings?.join(" ")).toMatch(/Terminal result save failed.*seeded publication EIO/);
    expect(waited.warnings?.join(" ")).toMatch(/Terminal result save failed/);
    expect(detachedUsage).toBe(stopped.usage.input);
    // After retry, disk authentication must govern instead of a new full heap cache.
    writeJsonAtomic(path.join(runRoot, handle.id, "manager-result.json"), { ...stopped, status: "completed" });
    await refuses(manager, handle.id);
  });

  it("independently keeps receipt/schema refusal failed during manager publication EIO", async () => {
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(request => fakeManagerHeapWorker(request, 64, true));
    const runRoot = root(), manager = createManager(runRoot);
    failPublication = true;
    const result = await manager.run({ task: "receipt schema refusal", transport: "process", extensions: false,
      schema: { type: "object", properties: { output: { type: "boolean" } }, required: ["output"] } });
    expect(result).toMatchObject({ status: "failed", error: expect.stringMatching(/Structured agent output was invalid/) });
    expect(result.finalAnswerReceipt).toBeDefined();
    fs.writeFileSync(path.join(runRoot, result.id, "status.json"), JSON.stringify({ ...result,
      status: "completed", text: "late forged receipt success", error: undefined, value: { output: true }, exitCode: 0 }));
    const status = manager.status(result.id), waited = await manager.wait(result.id);
    expect({ status: status.status, wait: waited.status }).toEqual({ status: "failed", wait: "failed" });
    expect(waited.error).toMatch(/Structured agent output was invalid/);
    expect(waited.warnings?.join(" ")).toMatch(/Terminal result save failed.*seeded publication EIO/);
  });

  it.skipIf(process.platform !== "linux")("keeps ENODEV delivery commit retryable through private publication EIO, then commits once", async () => {
    const { manager, id, directory, marker, archive, spawned, request } = enodevFixture();
    expect(await manager.run(request)).toMatchObject({ id, status: "completed", text: "recovered answer", enodevRecovered: true });
    expect(manager.noteEnodevDelivered(id)).toBe(true);
    expect(fs.existsSync(path.join(directory, "enodev-delivered.json"))).toBe(true);
    failPublication = true;
    expect(() => manager.commitEnodevRecovery(id)).toThrow(/seeded publication EIO/);
    expect({ marker: fs.existsSync(marker), removed: deletionRequests.includes(marker),
      archive: fs.existsSync(archive), status: JSON.parse(fs.readFileSync(path.join(directory, "status.json"), "utf8")).status,
    }).toEqual({ marker: true, removed: false, archive: true, status: "running" });
    // A second failed attempt proves the pending commit itself survived.
    expect(() => manager.commitEnodevRecovery(id)).toThrow(/seeded publication EIO/);
    failPublication = false;
    expect(manager.commitEnodevRecovery(id)).toBe(true);
    expect(deletionRequests.filter(file => file === marker)).toHaveLength(1);
    expect(fs.existsSync(marker)).toBe(false);
    expect(fs.existsSync(archive)).toBe(false);
    const status = manager.status(id), waited = await manager.wait(id);
    expect(status).toMatchObject({ id, status: "completed", text: "recovered answer", enodevDelivered: true });
    expect(waited).toMatchObject(status);
    expect(waited.writeReceipt).toMatchObject({ truncated: true });
    expect(JSON.parse(fs.readFileSync(path.join(directory, "status.json"), "utf8")).finishedAt).toBe(waited.finishedAt);
    expect(manager.commitEnodevRecovery(id)).toBe(false);
    expect(spawned).not.toHaveBeenCalled();
    expect(ProcessTransport.prototype.launch).not.toHaveBeenCalled();
    writeJsonAtomic(path.join(directory, "manager-result.json"), { ...waited, text: "foreign recovered answer" });
    await refuses(manager, id);
  });

  it.skipIf(process.platform !== "linux")("independently commits ordinary successful ENODEV recovery once without replay or a new worker", async () => {
    const { manager, id, marker, archive, spawned, request } = enodevFixture();
    expect(await manager.run(request)).toMatchObject({ id, status: "completed", text: "recovered answer", enodevRecovered: true });
    expect(manager.noteEnodevDelivered(id)).toBe(true);
    expect(fs.existsSync(marker)).toBe(true);
    expect(manager.commitEnodevRecovery(id)).toBe(true);
    expect(manager.status(id)).toMatchObject({ id, status: "completed", text: "recovered answer", enodevDelivered: true });
    expect(await manager.wait(id)).toMatchObject({ id, status: "completed", text: "recovered answer", enodevDelivered: true });
    expect(deletionRequests.filter(file => file === marker)).toHaveLength(1);
    expect(fs.existsSync(marker)).toBe(false);
    expect(fs.existsSync(archive)).toBe(false);
    expect(manager.commitEnodevRecovery(id)).toBe(false);
    expect(manager.noteEnodevDelivered(id)).toBe(false);
    expect(spawned).not.toHaveBeenCalled();
    // Same activation is no longer recoverable; this sentinel refuses a fresh
    // launch rather than allowing the fixture to execute any duplicate worker.
    await expect(manager.run(request)).rejects.toThrow(/new worker not authorized by fixture/);
    expect(spawned).toHaveBeenCalledOnce();
    expect(ProcessTransport.prototype.launch).not.toHaveBeenCalled();
  });
});
