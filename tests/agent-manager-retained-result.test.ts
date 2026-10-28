import fs from "node:fs";
import { createHash } from "node:crypto";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";

// Every product removal is recorded, never executed (#4664). The detached
// sweep is recorded at admission too, so a child cannot escape the fs spies.
const removalIntents = vi.hoisted(() => [] as string[]);
const sweepClaims = vi.hoisted(() => [] as string[]);
// Per-target seams model the collection boundary's observable effect (or failure) through reads.
const removalHooks = vi.hoisted(() => new Map<string, () => void>());
vi.mock("../src/agents/rm.js", () => ({
  removeTree: vi.fn(async (target: string) => { removalIntents.push(target); removalHooks.get(target)?.(); }),
}));
vi.mock("../src/storage/verified-remove.js", async original => ({
  ...await original<typeof import("../src/storage/verified-remove.js")>(),
  verifiedRemove: vi.fn((target: string) => {
    removalIntents.push(target);
    return { removed: true };
  }),
}));
vi.mock("../src/storage/retention.js", async original => ({
  ...await original<typeof import("../src/storage/retention.js")>(),
  claimTempRunSweep: vi.fn((root: string) => { sweepClaims.push(root); return false; }),
}));

import { AgentManager } from "../src/agents/manager.js";
import * as cleanupRoots from "../src/agents/worktree-manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { writeJsonAtomic } from "../src/core/atomic-write.js";
import type { AgentRunRecord } from "../src/agents/types.js";
import { fakeManagerHeapWorker } from "./fixtures/manager-heap-probe.js";

const managers: AgentManager[] = [];
const fixtureRoots: string[] = [];
const fsRemovals: string[] = [];

const recordFsRemovals = (): void => {
  vi.spyOn(fs, "rmSync").mockImplementation(file => { fsRemovals.push(String(file)); });
  vi.spyOn(fs, "unlinkSync").mockImplementation(file => { fsRemovals.push(String(file)); });
  vi.spyOn(fs, "rmdirSync").mockImplementation(file => { fsRemovals.push(String(file)); });
  vi.spyOn(fs.promises, "rm").mockImplementation(async file => { fsRemovals.push(String(file)); });
};

/** Sweep-owned managed temp root (no explicit runRoot) under the private test TMPDIR. */
const managedFixture = (options: ConstructorParameters<typeof AgentManager>[2] = {}) => {
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "agent-retained-result-"));
  fixtureRoots.push(tempRoot);
  vi.spyOn(os, "tmpdir").mockReturnValue(tempRoot);
  vi.stubEnv("PI_FABRIC_TMPDIR", undefined);
  vi.stubEnv("PI_FABRIC_RUN_ROOT", undefined);
  vi.stubEnv("PI_FABRIC_DEPTH", "0");
  for (const key of ["PI_FABRIC_BUDGET", "PI_FABRIC_BUDGET_FILE", "PI_FABRIC_BUDGET_ID"]) vi.stubEnv(key, undefined);
  const manager = new AgentManager(process.cwd(), {
    ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, maxConcurrent: 1, sessionExport: false, retainRuns: false,
  }, { workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fullCodeMode: false, ...options });
  managers.push(manager);
  const root = path.join(tempRoot, fs.readdirSync(tempRoot).find(name => name.startsWith("pi-fabric-runs-"))!);
  return { manager, root };
};

const readJson = (file: string): AgentRunRecord => JSON.parse(fs.readFileSync(file, "utf8")) as AgentRunRecord;
const custodyMoves = vi.hoisted(() => [] as Array<{ source: string; destination: string; receipt: string }>);
vi.mock("../src/agents/worktree-manager.js", async original => {
  const actual = await original<typeof import("../src/agents/worktree-manager.js")>();
  return { ...actual, retainCleanupRoot: (...args: Parameters<typeof actual.retainCleanupRoot>) => {
    const move = actual.retainCleanupRoot(...args);
    if (move) custodyMoves.push(move);
    return move;
  } };
});
const heldRoot = (source: string): string => {
  const move = custodyMoves.find(move => move.source === source)!;
  expect(move).toBeDefined();
  const row = { container: path.dirname(move.receipt), ...JSON.parse(fs.readFileSync(move.receipt, "utf8")) };
  expect(row).toMatchObject({ source, destination: expect.any(String), id: path.basename(source), reason: expect.any(String), time: expect.any(Number) });
  expect(JSON.parse(fs.readFileSync(path.join(row.container, "custody.prepared.json"), "utf8"))).toMatchObject({ source, destination: row.destination, id: row.id, state: "prepared" });
  if (process.platform !== "win32") expect(fs.statSync(row.container).mode & 0o777).toBe(0o700);
  expect(fs.existsSync(source)).toBe(false);
  return row.destination;
};

type Disguise = "absent" | "dangling" | "corrupt" | "foreign";
/** Model files' state through read/lstat/exists seams only; nothing is physically deleted. Install once at a time. */
const disguise = (files: string | string[], mode: Disguise = "absent"): () => void => {
  const hidden = new Set([files].flat());
  const read = fs.readFileSync;
  const exists = fs.existsSync;
  const lstat = fs.lstatSync;
  const open = fs.openSync;
  const enoent = () => Object.assign(new Error(`recorded ${mode}`), { code: "ENOENT" });
  // The authenticated manager path opens a descriptor, so path-only seams are
  // insufficient: corrupt/foreign reads are redirected to a real regular file
  // whose inode cannot match the retained publication; absent/dangling opens
  // refuse without touching the preserved original.
  const redirected = new Map<string, string>();
  if (mode === "corrupt" || mode === "foreign") for (const file of hidden) {
    const replacement = `${file}.disguise-${mode}`;
    const source = String(Reflect.apply(read, fs, [file, "utf8"]));
    fs.writeFileSync(replacement, mode === "corrupt"
      ? "{not json"
      : JSON.stringify({ ...JSON.parse(source), id: "foreign-run" }), { mode: 0o600 });
    redirected.set(file, replacement);
  }
  const readSpy = vi.spyOn(fs, "readFileSync").mockImplementation(((...args: unknown[]) => {
    if (!hidden.has(String(args[0]))) return Reflect.apply(read, fs, args);
    if (mode === "corrupt") return "{not json";
    if (mode === "foreign") return JSON.stringify({ ...JSON.parse(String(Reflect.apply(read, fs, [args[0], "utf8"]))), id: "foreign-run" });
    throw enoent();
  }) as typeof fs.readFileSync);
  const existsSpy = vi.spyOn(fs, "existsSync").mockImplementation(target =>
    hidden.has(String(target)) ? mode === "corrupt" || mode === "foreign" : exists(target));
  const lstatSpy = vi.spyOn(fs, "lstatSync").mockImplementation(((...args: unknown[]) => {
    if (!hidden.has(String(args[0])) || mode === "corrupt" || mode === "foreign") return Reflect.apply(lstat, fs, args);
    if (mode === "dangling") {
      return { isSymbolicLink: () => true, isFile: () => false, isDirectory: () => false, nlink: 1, uid: process.getuid?.() ?? 0, size: 0 } as unknown as fs.Stats;
    }
    throw enoent();
  }) as typeof fs.lstatSync);
  const openSpy = vi.spyOn(fs, "openSync").mockImplementation(((...args: unknown[]) => {
    const target = String(args[0]);
    if (!hidden.has(target)) return Reflect.apply(open, fs, args);
    if (mode === "absent" || mode === "dangling") throw enoent();
    return Reflect.apply(open, fs, [redirected.get(target)!, ...args.slice(1)]);
  }) as typeof fs.openSync);
  return () => { readSpy.mockRestore(); existsSpy.mockRestore(); lstatSpy.mockRestore(); openSpy.mockRestore(); };
};

const refusal = async (manager: AgentManager, id: string): Promise<void> => {
  expect(() => manager.status(id)).toThrow(/no saved terminal result/);
  await expect(manager.wait(id)).rejects.toThrow(/no saved terminal result/);
};

afterEach(async () => {
  const outcomes = await Promise.allSettled(managers.splice(0).map(async manager => {
    manager.config.retainRuns = true;
    await manager.close();
  }));
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  const evidence = process.env.PI_TEST_EVIDENCE_DIR;
  for (const source of fixtureRoots.splice(0)) if (evidence) {
    fs.mkdirSync(evidence, { recursive: true });
    let bytes = 0;
    const inventory = (base: string): Array<{ path: string; sha256?: string }> => {
      const rows: Array<{ path: string; sha256?: string }> = [];
      const walk = (relative: string): void => {
        const file = path.join(base, relative), stat = fs.lstatSync(file);
        if (rows.length >= 5_000 || stat.isSymbolicLink() || (!stat.isDirectory() && (!stat.isFile() || stat.nlink !== 1))) throw new Error(`Unsafe result fixture export: ${file}`);
        if (stat.isDirectory()) { rows.push({ path: relative }); for (const name of fs.readdirSync(file).sort()) walk(path.join(relative, name)); }
        else { if ((bytes += stat.size) > 96 * 1024 * 1024) throw new Error("Result fixture export bound exceeded"); rows.push({ path: relative, sha256: createHash("sha256").update(fs.readFileSync(file)).digest("hex") }); }
      };
      walk(""); return rows;
    };
    const before = inventory(source), destination = fs.mkdtempSync(path.join(evidence, "result-"));
    fs.cpSync(source, path.join(destination, "tree"), { recursive: true, dereference: false });
    expect(inventory(source)).toEqual(before);
    expect(inventory(path.join(destination, "tree"))).toEqual(before);
    fs.writeFileSync(path.join(destination, "export.json"), JSON.stringify({ source, destination: path.join(destination, "tree"), method: "pre-teardown copy; SHA256 independent readback; no inode claim", entries: before }));
  }
  removalIntents.splice(0);
  removalHooks.clear();
  fsRemovals.splice(0);
  sweepClaims.splice(0);
  expect(outcomes.filter(outcome => outcome.status === "rejected")).toEqual([]);
});

describe("retained manager outcome at close", () => {
  it("archives the exact whole manager result when host validation failed but the worker later wrote completed", async () => {
    recordFsRemovals();
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(request => fakeManagerHeapWorker(request, 32_000, true));
    const { manager, root } = managedFixture();
    const failedHandle = await manager.spawn({ task: "host validation private brief", transport: "process", extensions: false,
      schema: { type: "object", properties: { output: { type: "boolean" } }, required: ["output"] } });
    const validated = await manager.wait(failedHandle.id, { timeoutMs: 2_000 });
    expect(validated.status).toBe("failed");
    expect(validated.error).toMatch(/Structured agent output was invalid/);
    const run = manager.runDirectory(failedHandle.id)!;
    expect(path.dirname(run)).toBe(root);
    const managerResultFile = path.join(run, "manager-result.json");
    const statusFile = path.join(run, "status.json");
    const taskFile = path.join(run, "task.txt");
    const exact = readJson(managerResultFile);
    expect(exact).toMatchObject({ status: "failed", task: "host validation private brief" });
    expect((exact.value as { output: string }).output).toHaveLength(32_000);
    expect(exact.text).toHaveLength(32_000);
    // The worker's late completed status must not govern retention or public reads.
    writeJsonAtomic(statusFile, { ...validated, status: "completed", error: undefined, text: "late success",
      value: { output: true }, exitCode: 0, finishedAt: Date.now() + 1_000, updatedAt: Date.now() + 1_000 });
    expect(manager.status(failedHandle.id)).toMatchObject({ status: "failed" });
    expect(await manager.wait(failedHandle.id)).toMatchObject({ status: "failed" });

    // Success counterexample: whole-tree rename custody still occurs.
    const successHandle = await manager.spawn({ task: "plain success", transport: "process", extensions: false });
    expect(await manager.wait(successHandle.id, { timeoutMs: 2_000 })).toMatchObject({ status: "completed" });
    const successRun = manager.runDirectory(successHandle.id)!;

    await manager.close();
    expect(fs.readFileSync(path.join(heldRoot(successRun), "task.txt"), "utf8")).toBe("plain success");
    expect(removalIntents).not.toContain(successRun);
    const protectedPaths = [run, taskFile, statusFile, managerResultFile];
    for (const target of [...removalIntents, ...fsRemovals]) {
      if (target === root) {
        // Releasing an owner-marker-only root after both whole moves is not
        // pruning any run; prove the originals already left before admitting it.
        expect(protectedPaths.every(file => !fs.existsSync(file))).toBe(true);
        expect(fs.readdirSync(root)).toEqual([".fabric-owner.json"]);
      } else expect(protectedPaths.some(file => file === target || file.startsWith(target + path.sep))).toBe(false);
    }
    // Empty-root retirement and trace custody are independent.

    const held = heldRoot(run);
    const retained = readJson(path.join(held, "manager-result.json"));
    expect(retained).toEqual(exact);
    expect(retained.task).toBe("host validation private brief");
    expect(retained.status).toBe("failed");
    expect(retained.error).toBe(exact.error);
    expect(retained.text).toHaveLength(32_000);
    expect((retained.value as { output: string }).output).toHaveLength(32_000);
    const status = readJson(path.join(held, "status.json"));
    expect(status).toMatchObject({ id: failedHandle.id, status: "completed", text: "late success" });
    expect(manager.status(failedHandle.id)).toMatchObject({ status: "failed" });
    expect(await manager.wait(failedHandle.id)).toMatchObject({ status: "failed" });
  });

  it("keeps bounded terminal facts after receipt-backed close archival, but rejects an absent result before close", async () => {
    recordFsRemovals();
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(request => fakeManagerHeapWorker(request));
    const { manager } = managedFixture();
    const result = await manager.run({ task: "collected result", transport: "process", extensions: false });
    const run = manager.runDirectory(result.id)!;
    const file = path.join(run, "manager-result.json");
    const exact = readJson(file);
    const unhide = disguise(file);
    expect(() => manager.status(result.id)).toThrow(/no saved terminal result/);
    unhide();
    expect(manager.status(result.id)).toMatchObject({ id: exact.id, status: "completed" });
    const originalBytes = fs.readFileSync(file);
    await manager.close();
    expect(removalIntents).not.toContain(run);
    expect(fs.readFileSync(path.join(heldRoot(run), "manager-result.json"))).toEqual(originalBytes);
    expect(manager.status(result.id)).toMatchObject({ status: "completed", task: expect.any(String) });
    const after = await manager.wait(result.id);
    expect(after.status).toBe("completed");
    expect(after.warnings).toContain("Run files were collected at close; this is the manager's bounded terminal summary, not the exact result");
    expect(await manager.stop(result.id)).toMatchObject({ status: "completed" });
  });

  // F4: an unexpected pre-collection loss is never turned into a close summary or cleanup.
  it.each(["absent", "dangling", "corrupt", "foreign"] as const)(
    "keeps custody and refuses status/wait when a failed manager result is %s before close collection", async mode => {
      recordFsRemovals();
      vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(request => fakeManagerHeapWorker(request, 64, true));
      const { manager } = managedFixture();
      const handle = await manager.spawn({ task: "f4 private brief", transport: "process", extensions: false,
        schema: { type: "object", properties: { output: { type: "boolean" } }, required: ["output"] } });
      expect(await manager.wait(handle.id, { timeoutMs: 2_000 })).toMatchObject({ status: "failed" });
      const run = manager.runDirectory(handle.id)!;
      const file = path.join(run, "manager-result.json");
      const statusFile = path.join(run, "status.json");
      expect(readJson(file)).toMatchObject({ id: handle.id, status: "failed" });
      // A late worker success must never surface in place of the manager's failure.
      writeJsonAtomic(statusFile, { ...readJson(statusFile), status: "completed", error: undefined, text: "late success",
        exitCode: 0, updatedAt: Date.now() + 1_000 });
      const statusBytes = fs.readFileSync(statusFile, "utf8");
      disguise(file, mode);
      await refusal(manager, handle.id);
      // Only close-time collection is under test, not task-write temporary-file cleanup during setup.
      removalIntents.splice(0);
      fsRemovals.splice(0);

      await manager.close();
      // No unauthorized collection or outcome rewrite. Checked-exit marker/lock cleanup is independent.
      expect(fs.readFileSync(statusFile, "utf8")).toBe(statusBytes);
      await refusal(manager, handle.id);
      for (const target of removalIntents) {
        expect(target === run || target.startsWith(run + path.sep) || run.startsWith(target + path.sep), target).toBe(false);
      }
      const protectedPaths = [run, file, statusFile, path.join(run, "task.txt")];
      for (const target of fsRemovals) {
        expect(protectedPaths.some(file => file === target || file.startsWith(target + path.sep)), target).toBe(false);
      }
      expect(await manager.stop(handle.id).then(record => record.status, () => "refused")).not.toBe("completed");
    });

  it("does not count a no-op or EIO-refused close rename as collection", async () => {
    recordFsRemovals();
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(request => fakeManagerHeapWorker(request));
    const { manager } = managedFixture();
    const noop = await manager.run({ task: "noop removal", transport: "process", extensions: false });
    const failing = await manager.run({ task: "failing removal", transport: "process", extensions: false });
    const noopRun = manager.runDirectory(noop.id)!;
    const failingRun = manager.runDirectory(failing.id)!;
    const failingFile = path.join(failingRun, "manager-result.json");
    const retain = cleanupRoots.retainCleanupRoot;
    vi.spyOn(cleanupRoots, "retainCleanupRoot").mockImplementation((source, ...args) =>
      source === noopRun ? undefined : retain(source, ...args));
    const rename = fs.renameSync;
    let refused = false;
    vi.spyOn(fs, "renameSync").mockImplementation((source, destination) => {
      if (String(source) === failingRun) { refused = true; throw Object.assign(new Error("recorded custody failure"), { code: "EIO" }); }
      rename(source, destination);
    });
    await manager.close();
    expect(refused).toBe(true);
    expect(removalIntents).not.toEqual(expect.arrayContaining([noopRun, failingRun]));
    expect(fs.existsSync(noopRun)).toBe(true);
    expect(fs.existsSync(failingRun)).toBe(true);
    // The no-op left the exact file: it still reads exactly, and a later loss is refused.
    expect(manager.status(noop.id)).toMatchObject({ id: noop.id, status: "completed" });
    expect((await manager.wait(noop.id)).warnings ?? []).not.toContain(
      "Run files were collected at close; this is the manager's bounded terminal summary, not the exact result");
    disguise([path.join(noopRun, "manager-result.json"), failingFile]);
    await refusal(manager, noop.id);
    // A swallowed removal failure grants no provenance even if the file then reads absent.
    await refusal(manager, failing.id);
  });

  it("preserves original result and strict descendant proof after committed run move receipt EIO, never replacement authority", async () => {
    recordFsRemovals();
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(request => fakeManagerHeapWorker(request));
    const consumed = vi.fn();
    const { manager } = managedFixture({ onResultConsumed: consumed });
    const result = await manager.run({ task: "original custody result", transport: "process", extensions: false });
    consumed.mockClear();
    const run = manager.runDirectory(result.id)!;
    const originalResult = fs.readFileSync(path.join(run, "manager-result.json"));
    const originalInode = fs.statSync(run).ino;
    const raw = Buffer.from('raw tail\n{"torn":');
    fs.writeFileSync(path.join(run, "raw.NdJsOn.backup"), raw);
    const rename = fs.renameSync;
    let held = "", runMoves = 0, failing = true;
    const fault = vi.spyOn(fs, "renameSync").mockImplementation((source, destination) => {
      if (failing && held && String(destination) === path.join(path.dirname(held), "custody.json")) {
        throw Object.assign(new Error("post-move receipt EIO"), { code: "EIO" });
      }
      rename(source, destination);
      if (String(source) === run) { held = String(destination); runMoves++; }
    });
    await expect(manager.cleanup(result.id)).rejects.toThrow(/retained at.*receipt/);
    expect(consumed).not.toHaveBeenCalled();
    expect(manager.hasRunCustody(result.id)).toBe(true);
    expect(fs.statSync(held).ino).toBe(originalInode);
    expect(fs.readFileSync(path.join(held, "manager-result.json"))).toEqual(originalResult);
    expect(fs.readFileSync(path.join(held, "raw.NdJsOn.backup"))).toEqual(raw);
    const preparedFile = path.join(path.dirname(held), "custody.prepared.json");
    const prepared = fs.readFileSync(preparedFile);
    expect(JSON.parse(prepared.toString())).toMatchObject({ source: run, destination: held, id: result.id, state: "prepared", reason: expect.any(String), time: expect.any(Number) });
    // Canonical absence is not loss of the authenticated, manager-owned held result.
    expect(manager.status(result.id)).toMatchObject({ id: result.id, text: result.text });
    fs.mkdirSync(run);
    fs.writeFileSync(path.join(run, "foreign.JSONL"), "replacement owner");
    fs.writeFileSync(path.join(run, "manager-result.json"), JSON.stringify({ ...result, text: "FORGED canonical result" }));
    expect(manager.status(result.id)).toMatchObject({ id: result.id, text: result.text });
    expect(await manager.wait(result.id)).toMatchObject({ id: result.id, text: result.text });
    consumed.mockClear();
    failing = false;
    const descendant = path.join(held, "nested", "late-worker");
    fs.mkdirSync(descendant, { recursive: true });
    const status = path.join(descendant, "status.json");
    fs.writeFileSync(status, JSON.stringify({ status: "running", transport: "process", sessionId: String(process.pid) }));
    await expect(manager.cleanup(result.id)).rejects.toThrow(/descendant worker may still be running/);
    expect(consumed).not.toHaveBeenCalled();
    fs.writeFileSync(status, JSON.stringify({ status: "running", transport: "process" }));
    await expect(manager.cleanup(result.id)).rejects.toThrow(/unknown descendant identity/);
    expect(consumed).not.toHaveBeenCalled();
    const unhideHeldResult = disguise(path.join(held, "manager-result.json"), "foreign");
    try { await refusal(manager, result.id); } finally { unhideHeldResult(); }
    // Retire this synthetic status without deleting raw evidence or weakening original-tree liveness.
    fs.renameSync(descendant, path.join(path.dirname(held), "retired-synthetic-descendant"));
    await manager.cleanup(result.id);
    expect(consumed).toHaveBeenCalledExactlyOnceWith(result.id);
    expect(runMoves).toBe(1);
    expect(fs.readFileSync(path.join(run, "foreign.JSONL"), "utf8")).toBe("replacement owner");
    expect(fs.readFileSync(preparedFile)).toEqual(prepared);
    expect(fs.readFileSync(path.join(held, "raw.NdJsOn.backup"))).toEqual(raw);
    fault.mockRestore();
  });

  it("keeps known-ID custody and reports a verified external copy without consuming or cleaning an EXDEV original", async () => {
    recordFsRemovals();
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(request => fakeManagerHeapWorker(request));
    const consumed = vi.fn();
    const { manager } = managedFixture({ onResultConsumed: consumed });
    const result = await manager.run({ task: "copy is not cleanup", transport: "process", extensions: false });
    consumed.mockClear();
    expect(manager.status(result.id)).not.toHaveProperty("cleanupStatus");
    const run = manager.runDirectory(result.id)!;
    const original = fs.readFileSync(path.join(run, "manager-result.json")), inode = fs.statSync(run).ino;
    const rename = fs.renameSync, stat = fs.statSync;
    let externalContainer = "";
    // Recorded cross-device boundary only: real native EXDEV is the helper owner's
    // hosted test. The copy, SHA256, independent reopen and original bytes are real.
    vi.spyOn(fs, "statSync").mockImplementation(((...args: Parameters<typeof fs.statSync>) => {
      const value = Reflect.apply(stat, fs, args);
      return String(args[0]) === externalContainer && value ? Object.assign(Object.create(Object.getPrototypeOf(value)), value, { dev: Number(value.dev) + 1 }) : value;
    }) as typeof fs.statSync);
    vi.spyOn(fs, "renameSync").mockImplementation((source, destination) => {
      if (String(source) === run) {
        externalContainer = path.dirname(String(destination));
        throw Object.assign(new Error("recorded cross-device custody"), { code: "EXDEV" });
      }
      rename(source, destination);
    });
    let error: unknown;
    try { await manager.cleanup(result.id); } catch (caught) { error = caught; }
    expect(error).toMatchObject({ code: "CLEANUP_CUSTODY_DEFERRED", cleanupStatus: { originalRetained: true } });
    expect(error).not.toHaveProperty("retainedRoot");
    expect(consumed).not.toHaveBeenCalled();
    expect(manager.hasRunCustody(result.id)).toBe(true);
    expect(fs.statSync(run).ino).toBe(inode);
    expect(fs.readFileSync(path.join(run, "manager-result.json"))).toEqual(original);
    const reported = manager.status(result.id) as AgentRunRecord;
    expect(reported.cleanupStatus).toEqual((error as { cleanupStatus: unknown }).cleanupStatus);
    expect(reported.warnings).toEqual(expect.arrayContaining([expect.stringMatching(/Cleanup deferred.*original retained/)]));
    const status = reported.cleanupStatus!;
    if (process.platform === "linux") {
      expect(status).toMatchObject({ state: "held-in-place", verifiedCopyPath: expect.any(String) });
      expect(fs.readFileSync(path.join(status.verifiedCopyPath!, "manager-result.json"))).toEqual(original);
    } else {
      expect(status.state).toBe("copy-failed");
      expect(status.verifiedCopyPath).toBeUndefined();
    }
    expect(JSON.parse(fs.readFileSync(status.receipt, "utf8"))).toMatchObject({ state: status.state, source: run, originalRetained: true });

    // A later committed rename must stop advertising the earlier in-place copy,
    // even if its separate moved-receipt publication fails after the rename.
    vi.mocked(fs.statSync).mockRestore();
    vi.mocked(fs.renameSync).mockRestore();
    const actualRename = fs.renameSync;
    vi.spyOn(fs, "renameSync").mockImplementation((source, destination) => {
      if (path.basename(String(destination)) === "custody.json" && String(source).endsWith(".tmp") && JSON.parse(fs.readFileSync(String(source), "utf8")).state === "moved") {
        throw Object.assign(new Error("post-copy committed-rename receipt EIO"), { code: "EIO" });
      }
      actualRename(source, destination);
    });
    await expect(manager.cleanup(result.id)).rejects.toHaveProperty("retainedRoot");
    expect(manager.status(result.id)).not.toHaveProperty("cleanupStatus");
    expect(manager.hasRunCustody(result.id)).toBe(true);
    expect(consumed).not.toHaveBeenCalled();
  });

  it("reports the typed missing-result code before collection provenance", async () => {
    recordFsRemovals();
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(request => fakeManagerHeapWorker(request));
    const { manager } = managedFixture();
    const result = await manager.run({ task: "typed missing", transport: "process", extensions: false });
    disguise(path.join(manager.runDirectory(result.id)!, "manager-result.json"), "dangling");
    let caught: unknown;
    try { manager.status(result.id); } catch (error) { caught = error; }
    expect(caught).toMatchObject({ code: "FABRIC_MISSING_RESULT", message: expect.stringMatching(/no saved terminal result/) });
  });
});
