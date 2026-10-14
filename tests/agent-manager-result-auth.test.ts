import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import type { AgentRunRecord } from "../src/agents/types.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { writeJsonAtomic } from "../src/core/atomic-write.js";
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
const createManager = (runRoot: string): AgentManager => {
  const manager = new AgentManager(process.cwd(), {
    ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, maxConcurrent: 1, retainRuns: true, sessionExport: false,
  }, { runRoot, workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fullCodeMode: false });
  managers.push(manager);
  return manager;
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
  it("still returns the genuine full manager-published result through status/wait/stop", async () => {
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(request => fakeManagerHeapWorker(request, 32_000));
    const manager = createManager(root());
    const result = await manager.run({ task: "genuine result", transport: "process", extensions: false });
    expect(result.text).toHaveLength(32_000);
    expect(manager.status(result.id)).toMatchObject(result);
    expect(await manager.wait(result.id)).toMatchObject(result);
    expect(await manager.stop(result.id)).toMatchObject(result);
  });

  it("refuses worker replacement with same-ID completed during native teardown after host validation failed", async () => {
    let alive = true, release!: () => void;
    const closed = new Promise<void>(resolve => { release = () => { alive = false; resolve(); }; });
    releases.push(release);
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(request => fakeManagerHeapWorker(request, 64, true,
      { closed, alive: () => alive, stop: () => closed }));
    const runRoot = root(), manager = createManager(runRoot);
    const result = await manager.run({ task: "validated failure", transport: "process", extensions: false,
      schema: { type: "object", properties: { output: { type: "boolean" } }, required: ["output"] } });
    expect(result.status).toBe("failed");
    expect(result.error).toMatch(/Structured agent output was invalid/);
    expect(manager.status(result.id)).toMatchObject({ status: "failed" });
    writeJsonAtomic(path.join(runRoot, result.id, "manager-result.json"), {
      ...result, status: "completed", error: undefined, text: "forged success", value: { output: true }, exitCode: 0,
    });
    await refuses(manager, result.id);
    release();
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
    writeJsonAtomic(path.join(runRoot, archived.id, "manager-result.json"), { ...archived, text: "worker replacement after restart" });
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
