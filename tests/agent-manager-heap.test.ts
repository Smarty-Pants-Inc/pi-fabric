import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { writeJsonAtomic } from "../src/core/atomic-write.js";
import * as removal from "../src/agents/rm.js";
import { canRemoveTerminalRun, runTreeResourceVeto } from "../src/storage/retention.js";
import type { AgentRunRecord, AgentRunResult } from "../src/agents/types.js";
import { fakeManagerHeapWorker, heapRunResult } from "./fixtures/manager-heap-probe.js";

// Decision C: data safety (exact archived outcomes and traversal rejection), and
// smarty-dev#10720's recorded Main OOM. Public manager paths, not source strings.
// All execution is fake and all deletion is recorded. Real deletion stays hosted.
const managers: AgentManager[] = [];
const releases: Array<() => void> = [];
const deletionRequests: string[] = [];
const execute = promisify(execFile);
const root = () => fs.mkdtempSync(path.join(os.tmpdir(), "manager-heap-"));
const createManager = (retainRuns = true, options: ConstructorParameters<typeof AgentManager>[2] = {}) => {
  const directory = root();
  const manager = new AgentManager(process.cwd(), {
    ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, maxConcurrent: 1, retainRuns, sessionExport: false,
  }, { runRoot: directory, workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fullCodeMode: false, ...options });
  managers.push(manager);
  return { manager, directory };
};
beforeEach(() => {
  deletionRequests.length = 0;
  vi.spyOn(fs, "rmSync").mockImplementation(file => { deletionRequests.push(String(file)); });
  vi.spyOn(fs, "unlinkSync").mockImplementation(file => { deletionRequests.push(String(file)); });
  vi.spyOn(fs, "rmdirSync").mockImplementation(file => { deletionRequests.push(String(file)); });
  vi.spyOn(fs.promises, "rm").mockImplementation(async file => { deletionRequests.push(String(file)); });
  vi.spyOn(removal, "removeTree").mockImplementation(async file => { deletionRequests.push(file); });
  vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(request => fakeManagerHeapWorker(request));
});
afterEach(async () => {
  for (const release of releases.splice(0)) release();
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  vi.restoreAllMocks();
  // Deliberately leave private fixture roots for the hosted environment owner.
});

describe("disk-backed previous-run index", () => {
  it("durably backs a legacy restored result before retaining a summary, then demand-loads exact structured data and metadata", async () => {
    const consumed = vi.fn();
    const { manager, directory } = createManager(true, { onResultConsumed: consumed });
    const restored: AgentRunResult = {
      ...heapRunResult("old-run", 128 * 1024), status: "stopped",
      task: "task\ud800\udfff", error: "restored shutdown reason", partialText: "partial\ud800",
      lastCompleteText: "complete\udfff", actorId: "actor-a", actorName: "actor name",
      spawner: { id: "actor-parent", kind: "actor", runId: "activation-parent" },
      runnerSessionIds: ["native-a", "native-b"], mainAgentId: "main-a", fabricSessionId: "session-a",
      value: { nested: { content: "large exact value".repeat(10_000), lone: "\ud800", items: [null, 42, false] } },
      writeReceipt: { paths: ["/exact/path"], commands: ["echo exact"] },
    };
    const expected = structuredClone(restored);
    const run = path.join(directory, "old-run");
    expect(fs.existsSync(run)).toBe(false);
    manager.restorePreviousRuns([restored]);
    const backing = fs.readdirSync(run).filter(name => name.startsWith("manager-restored-result-"));
    expect(backing).toHaveLength(1);
    const file = path.join(run, backing[0]!);
    expect(JSON.parse(fs.readFileSync(file, "utf8"))).toEqual(expected);
    expect(JSON.parse(fs.readFileSync(path.join(directory, "old-run", "status.json"), "utf8"))).toEqual(expected);
    restored.text = "caller mutated source";
    (restored.value as { nested: { content: string } }).nested.content = "mutated";
    restored.runnerSessionIds!.push("mutated");
    expect(manager.status("old-run")).toEqual(expected);
    let consume: (() => void) | undefined;
    expect(await manager.wait("old-run", { deferConsumption(commit) { consume = commit; } })).toEqual(expected);
    expect(consumed).not.toHaveBeenCalled();
    consume!();
    expect(consumed).toHaveBeenCalledExactlyOnceWith("old-run");
    // A same-ID worker/foreign replacement is not a newer archive: the retained
    // manager publication must refuse it until the caller explicitly republishes
    // an in-memory archive through restorePreviousRuns.
    const newer = { ...expected, text: "new exact disk result", value: { disk: "fresh\udfff" } };
    writeJsonAtomic(file, newer);
    expect(() => manager.status("old-run")).toThrow(/no saved terminal result/);
    await expect(manager.wait("old-run")).rejects.toThrow(/no saved terminal result/);
    manager.restorePreviousRuns([newer]);
    expect(fs.readdirSync(run).filter(name => name.startsWith("manager-restored-result-"))).toHaveLength(2);
    expect(manager.status("old-run")).toEqual(newer);
    expect(await manager.wait("old-run")).toEqual(newer);
    expect(await manager.stop("old-run")).toEqual(newer);
    expect(() => manager.steer("old-run", "late")).toThrow(/is stopped: restored shutdown reason/);
  });

  it("fails closed on a durable publication failure without replacing an existing exact result", () => {
    const { manager, directory } = createManager();
    const original = { ...heapRunResult("old-run"), text: "old exact", value: { retained: true } };
    manager.restorePreviousRuns([original]);
    const statusFile = path.join(directory, "old-run", "status.json");
    const rename = fs.renameSync;
    const fail = vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (String(to) === statusFile) throw Object.assign(new Error("publication unavailable"), { code: "EIO" });
      return rename(from, to);
    });
    expect(() => manager.restorePreviousRuns([{ ...original, text: "new unsaved", value: { retained: false } }])).toThrow();
    fail.mockRestore();
    expect(manager.status(original.id)).toEqual(original);
    expect(JSON.parse(fs.readFileSync(statusFile, "utf8"))).toEqual(original);
  });

  it.each([".", "..", "../escape", "nested/run", "nested\\run", "/absolute", "C:\\absolute", "x".repeat(257)])(
    "rejects restored path component %s before filesystem publication", id => {
      const { manager, directory } = createManager();
      const writes = vi.spyOn(fs, "writeFileSync");
      const renames = vi.spyOn(fs, "renameSync");
      const creates = vi.spyOn(fs, "mkdirSync");
      expect(() => manager.restorePreviousRuns([heapRunResult(id)])).toThrow(/Invalid restored Fabric agent id/);
      expect(writes).not.toHaveBeenCalled();
      expect(renames).not.toHaveBeenCalled();
      expect(creates).not.toHaveBeenCalled();
      expect(fs.readdirSync(directory)).toEqual([]);
    },
  );

  it("prunes the previous-run summary index by count, without deleting exact disk outcomes", async () => {
    const { manager, directory } = createManager();
    for (let index = 0; index < 1_005; index++) {
      manager.restorePreviousRuns([{ ...heapRunResult(`old-${index}`), value: { exact: index } }]);
    }
    for (let index = 0; index < 5; index++) {
      expect(() => manager.status(`old-${index}`)).toThrow(/Unknown Fabric agent/);
      expect(fs.existsSync(path.join(directory, `old-${index}`, "status.json"))).toBe(true);
    }
    expect(manager.status("old-5")).toMatchObject({ value: { exact: 5 } });
    expect(await manager.wait("old-1004")).toMatchObject({ value: { exact: 1004 } });
    // Atomic publication records cleanup of its already-renamed temporary file,
    // not collection of a run or its durable outcome.
    expect(deletionRequests.filter(file => !/\.json\.\d+\.[a-f0-9-]+\.tmp$/.test(file))).toEqual([]);
  });

  it("keeps a deferred result-consumption receipt valid after its summary is byte-pruned", async () => {
    const consumed = vi.fn();
    const prepared = vi.fn();
    const { manager } = createManager(true, { onResultConsumed: consumed, onBeforeResultReturned: prepared });
    manager.restorePreviousRuns([{ ...heapRunResult("pending-old"), value: { exact: "already returned" } }]);
    let commit!: () => void;
    const result = await manager.wait("pending-old", { deferConsumption(consume) { commit = consume; } });
    expect(result.value).toEqual({ exact: "already returned" });
    for (let index = 0; index < 180; index++) manager.restorePreviousRuns([
      { ...heapRunResult(`pressure-${index}`), status: "failed", error: "e".repeat(12_000) },
    ]);
    expect(() => manager.status("pending-old")).toThrow(/Unknown Fabric agent/);
    expect(consumed).not.toHaveBeenCalled();
    expect(() => commit()).not.toThrow();
    expect(prepared).toHaveBeenCalledExactlyOnceWith("pending-old");
    expect(consumed).toHaveBeenCalledExactlyOnceWith("pending-old");
  });

  it("applies the summary byte cap even below the run-count cap, and keeps surviving full errors exact", async () => {
    const { manager, directory } = createManager();
    const error = "e".repeat(12_000);
    for (let index = 0; index < 180; index++) manager.restorePreviousRuns([
      { ...heapRunResult(`bytes-${index}`), status: "failed", error, value: { exact: index } },
    ]);
    const survivors = Array.from({ length: 180 }, (_, index) => `bytes-${index}`).filter(id => {
      try { return manager.isSettled(id); } catch { return false; }
    });
    expect(survivors.length).toBeGreaterThan(0);
    expect(survivors.length).toBeLessThan(140);
    expect(() => manager.status("bytes-0")).toThrow(/Unknown Fabric agent/);
    expect(await manager.wait("bytes-179")).toMatchObject({ error, value: { exact: 179 } });
    expect(JSON.parse(fs.readFileSync(path.join(directory, "bytes-0", "status.json"), "utf8")).error).toBe(error);
    expect(deletionRequests.filter(file => !/\.json\.\d+\.[a-f0-9-]+\.tmp$/.test(file))).toEqual([]);
  });
});

describe("manager artifact collection proof (no deletion)", () => {
  it("admits only the real owned manager result/nested artifacts after independent worker exit proof", async () => {
    const { manager, directory } = createManager();
    const result = await manager.run({ task: "collection proof", transport: "process", extensions: false });
    const run = path.join(directory, result.id);
    expect(fs.existsSync(path.join(run, "manager-result.json"))).toBe(true);
    expect(canRemoveTerminalRun(run)).toBe(true);
    expect(runTreeResourceVeto(run, 0, undefined, true, true)).toBeUndefined();
    writeJsonAtomic(path.join(run, "manager-nested.json"), { id: result.id, nestedAgents: [] });
    expect(canRemoveTerminalRun(run)).toBe(true);
    const restarted = createManager(true, { runRoot: directory }).manager;
    restarted.restorePreviousRuns([result]);
    expect(fs.readdirSync(run).filter(name => name.startsWith("manager-restored-result-"))).toHaveLength(1);
    expect(canRemoveTerminalRun(run)).toBe(true);
    // Arbitrary JSON remains unknown beside the exact manager artifacts.
    writeJsonAtomic(path.join(run, "manager-unknown.json"), { id: result.id });
    expect(canRemoveTerminalRun(run)).toBe(false);
    expect(runTreeResourceVeto(run, 0, undefined, true, true)).toBeUndefined();
    expect(fs.existsSync(path.join(run, "manager-result.json"))).toBe(true);
  });

  it.each([
    "manager-restored-result-not-a-uuid.json",
    "manager-restored-result-12345678-1234-1234-8234-123456789abc.json",
    "manager-restored-result-12345678-1234-4234-7234-123456789abc.json",
    "manager-restored-result-12345678-1234-4234-8234-123456789ABC.json",
    "manager-restored-result-12345678-1234-4234-8234-123456789abc.json.extra",
  ])("vetoes noncanonical restored-result filename %s", name => {
    const run = root(), record = heapRunResult("disk-run");
    writeJsonAtomic(path.join(run, "status.json"), record);
    expect(canRemoveTerminalRun(run)).toBe(true);
    writeJsonAtomic(path.join(run, name), record);
    expect(canRemoveTerminalRun(run)).toBe(false);
  });

  it("does not let manager success substitute for a live or unresolved worker status", () => {
    const run = root();
    const record = heapRunResult("disk-run");
    writeJsonAtomic(path.join(run, "manager-result.json"), record);
    writeJsonAtomic(path.join(run, "status.json"), { ...record, sessionId: String(process.pid) });
    expect(canRemoveTerminalRun(run)).toBe(false);
    expect(runTreeResourceVeto(run, 0, undefined, true, true)).toMatch(/worker|identity/);
    writeJsonAtomic(path.join(run, "status.json"), record);
    writeJsonAtomic(path.join(run, "unresolved-worker.json"), { reason: "unknown custody" });
    expect(canRemoveTerminalRun(run)).toBe(false);
    expect(runTreeResourceVeto(run, 0, undefined, true, true)).toMatch(/unresolved worker/);
  });

  it.skipIf(process.platform === "win32")("vetoes linked manager artifacts and linked worker status (Windows symlinks need elevated privileges)", () => {
    const run = root();
    const record = heapRunResult("disk-run");
    writeJsonAtomic(path.join(run, "status.json"), record);
    const other = path.join(root(), "result.json");
    writeJsonAtomic(other, record);
    fs.symlinkSync(other, path.join(run, "manager-result.json"));
    expect(canRemoveTerminalRun(run)).toBe(false);
    // Native-resource proof is deliberately distinct from safe-file collection.
    expect(runTreeResourceVeto(run, 0, undefined, true, true)).toBeUndefined();
    const linkedStatus = root();
    fs.symlinkSync(other, path.join(linkedStatus, "status.json"));
    expect(canRemoveTerminalRun(linkedStatus)).toBe(false);
    expect(runTreeResourceVeto(linkedStatus, 0, undefined, true, true)).toMatch(/unreadable run record/);
    expect(fs.readFileSync(other, "utf8")).toContain("disk-run");
  });
});

describe("manager handles, snapshots and receipt custody", () => {
  it("prunes settled handles without resurrecting previous-run summaries or deleting their records", async () => {
    const { manager, directory } = createManager();
    let first = "", last = "";
    for (let index = 0; index < 1_005; index++) {
      const result = await manager.run({ task: `handle ${index}`, transport: "process", extensions: false });
      if (!first) first = result.id;
      last = result.id;
      // A restore while its live handle exists must not seed a fallback cache.
      if (index === 0) manager.restorePreviousRuns([result]);
    }
    expect(manager.hasRunCustody(first)).toBe(false);
    expect(manager.runDirectory(first)).toBeUndefined();
    expect(() => manager.status(first)).toThrow(/Unknown Fabric agent/);
    await expect(manager.wait(first)).rejects.toThrow(/Unknown Fabric agent/);
    expect(manager.hasRunCustody(last)).toBe(true);
    expect(manager.list()).toHaveLength(1_000);
    expect(manager.listForUi().length).toBeLessThanOrEqual(240);
    expect(fs.existsSync(path.join(directory, first, "status.json"))).toBe(true);
  }, 60_000);

  it("evicts a cleaned handle while recording removal, retaining the exact file for the hosted delete check", async () => {
    const { manager, directory } = createManager(false);
    const result = await manager.run({ task: "mock cleanup", transport: "process", extensions: false });
    const run = path.join(directory, result.id);
    expect(manager.hasRunCustody(result.id)).toBe(true);
    await manager.cleanup(result.id);
    expect(deletionRequests).toContain(run);
    expect(manager.hasRunCustody(result.id)).toBe(false);
    expect(() => manager.status(result.id)).toThrow(/Unknown Fabric agent/);
    expect(JSON.parse(fs.readFileSync(path.join(run, "status.json"), "utf8")).id).toBe(result.id);
  });

  it("preserves full receipt-bound results and native close custody after dropping completion payload caches", async () => {
    let alive = true, release!: () => void;
    const closed = new Promise<void>(resolve => { release = () => { alive = false; resolve(); }; });
    releases.push(release);
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(request => fakeManagerHeapWorker(request, 4 * 1024 * 1024, true,
      { closed, alive: () => alive, stop: () => closed }));
    const onSettled = vi.fn();
    const { manager, directory } = createManager(true, { onSettled });
    const handle = await manager.spawn({ task: "receipt result", transport: "process", extensions: false });
    const finishing = manager.status(handle.id) as AgentRunRecord;
    expect(finishing.status).toBe("finishing");
    expect(manager.isSettled(handle.id)).toBe(false);
    expect(onSettled).not.toHaveBeenCalled();
    await expect(manager.wait(handle.id, { timeoutMs: 10 })).rejects.toMatchObject({ name: "AgentWaitBoundError" });
    await expect(manager.cleanup(handle.id)).rejects.toThrow(/running agent/);
    expect(finishing.text).toHaveLength(4 * 1024 * 1024);
    expect((finishing.value as { output: string }).output).toHaveLength(4 * 1024 * 1024);
    const ui = manager.listForUi()[0] as AgentRunRecord;
    expect(ui.status).toBe("finishing");
    expect(ui.text.length).toBeLessThanOrEqual(16_001);
    expect(ui.value).toMatchObject({ fabricTruncated: true });
    release();
    const full = await manager.wait(handle.id, { timeoutMs: 2_000 });
    expect(full.text).toHaveLength(4 * 1024 * 1024);
    expect((full.value as { output: string }).output).toHaveLength(4 * 1024 * 1024);
    expect(full.finalAnswerReceipt?.id).toBeTypeOf("string");
    expect(onSettled).toHaveBeenCalledOnce();
    expect(manager.hasRunCustody(handle.id)).toBe(true);
    expect((manager.status(handle.id) as AgentRunRecord).text).toBe(full.text);
    expect((await manager.wait(handle.id)).value).toEqual(full.value);
    expect(JSON.parse(fs.readFileSync(path.join(directory, handle.id, "manager-result.json"), "utf8")).value).toEqual(full.value);
    release();
    await manager.close();
  });

  it("preserves a byte-evicted legacy nested snapshot after its source disappears, without retaining an over-budget UI list", async () => {
    let alive = true, release!: () => void;
    const closed = new Promise<void>(resolve => { release = () => { alive = false; resolve(); }; });
    releases.push(release);
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
      const transport = await fakeManagerHeapWorker(request, 0, false,
        { closed, alive: () => alive, stop: async () => { release(); } });
      const file = request.workerArguments[request.workerArguments.indexOf("--status-file") + 1]!;
      const record = JSON.parse(fs.readFileSync(file, "utf8"));
      writeJsonAtomic(file, { ...record, status: "running", finishedAt: undefined });
      return transport;
    });
    const { manager, directory } = createManager();
    const handle = await manager.spawn({ task: "legacy nested history", transport: "process", extensions: false });
    const nested = path.join(directory, handle.id, "nested");
    for (let index = 0; index < 25; index++) {
      const id = `child-${index}`;
      fs.mkdirSync(path.join(nested, id), { recursive: true });
      writeJsonAtomic(path.join(nested, id, "status.json"), heapRunResult(id, 100_000));
    }
    const first = manager.status(handle.id) as AgentRunRecord;
    expect(first.nestedAgents).toHaveLength(25);
    expect(fs.existsSync(path.join(directory, handle.id, "manager-nested.json"))).toBe(true);
    // Model a legacy child removing its source through the read seam only;
    // physical deletion is deliberately not executed in this fleet test.
    const readDirectory = fs.readdirSync;
    const hide = vi.spyOn(fs, "readdirSync").mockImplementation((...args: Parameters<typeof readDirectory>) => {
      if (String(args[0]) === nested) return [];
      return readDirectory(...args);
    });
    const second = manager.status(handle.id) as AgentRunRecord;
    expect(second.nestedAgents).toEqual(first.nestedAgents);
    expect(JSON.stringify(manager.listForUi()).length * 2).toBeLessThanOrEqual(2 * 1024 * 1024);
    expect(manager.hasRunCustody(handle.id)).toBe(true);
    hide.mockRestore();
  });

  it("does not regress host schema validation when the worker rewrites status after final publication and native close", async () => {
    let alive = true, release!: () => void;
    const closed = new Promise<void>(resolve => { release = () => { alive = false; resolve(); }; });
    releases.push(release);
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(request => fakeManagerHeapWorker(request, 32_000, true,
      { closed, alive: () => alive, stop: () => closed }));
    const settled = vi.fn();
    const { manager, directory } = createManager(true, { onSettled: settled });
    const handle = await manager.spawn({ task: "host validation", transport: "process", extensions: false,
      schema: { type: "object", properties: { output: { type: "boolean" } }, required: ["output"] } });
    await vi.waitFor(() => expect(manager.status(handle.id)).toMatchObject({ status: "finishing",
      error: expect.stringMatching(/Structured agent output was invalid/) }));
    const finishing = manager.status(handle.id) as AgentRunRecord;
    expect(manager.isSettled(handle.id)).toBe(false);
    expect(settled).not.toHaveBeenCalled();
    expect((finishing.value as { output: string }).output).toHaveLength(32_000);
    const canonical = path.join(directory, handle.id, "manager-result.json");
    expect(JSON.parse(fs.readFileSync(canonical, "utf8"))).toMatchObject({ status: "failed", error: finishing.error, value: finishing.value });
    const statusFile = path.join(directory, handle.id, "status.json");
    const lateWorkerStatus = { ...finishing, status: "completed", error: undefined, text: "late success",
      value: { output: true }, exitCode: 0, finishedAt: Date.now() + 1_000, updatedAt: Date.now() + 1_000 };
    writeJsonAtomic(statusFile, lateWorkerStatus);
    expect(manager.status(handle.id)).toMatchObject(finishing);
    release();
    const validated = await manager.wait(handle.id, { timeoutMs: 2_000 });
    expect(validated).toMatchObject({ status: "failed", error: finishing.error, value: finishing.value });
    expect(settled).toHaveBeenCalledOnce();
    await manager.close();
    // Model the worker's final writeRunRecord after its native close too.
    writeJsonAtomic(statusFile, lateWorkerStatus);
    expect(manager.status(handle.id)).toMatchObject(validated);
    expect(await manager.wait(handle.id)).toMatchObject(validated);
    expect(JSON.parse(fs.readFileSync(canonical, "utf8"))).toMatchObject(validated);
  });
});

it.each(["ui", "receipt", "restored"] as const)(
  "retains under 24 MiB in a fresh GC child after 100 x 4 MiB %s results through real manager API/UI paths", async mode => {
    const directory = root();
    // Jiti gives the fresh Node child source .js -> .ts resolution; no build,
    // worker process, installation or model is needed. Program text is fixed.
    const program = `import { createJiti } from "jiti";
      import { pathToFileURL } from "node:url";
      const jiti = createJiti(pathToFileURL(process.cwd() + "/index.js").href);
      const { managerHeapProbe } = await jiti.import("./tests/fixtures/manager-heap-probe.ts");
      console.log(JSON.stringify(await managerHeapProbe(process.argv[1], process.argv[2])));`;
    const result = await execute(process.execPath, ["--expose-gc", "--max-old-space-size=4096", "--input-type=module", "-e", program, mode, directory], {
      cwd: process.cwd(), timeout: 90_000, maxBuffer: 1024 * 1024,
    });
    const evidence = JSON.parse(result.stdout.trim());
    expect(evidence).toMatchObject({ mode, runs: 100 });
    expect(evidence.retainedBytes).toBeLessThan(24 * 1024 * 1024);
    expect(evidence.uiBytes).toBeLessThanOrEqual(2 * 1024 * 1024);
  }, 100_000,
);
