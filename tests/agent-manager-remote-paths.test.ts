import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";

// Keep reason: CA-006, a recorded real catch at the worker/manager/completion
// boundary. Product deletions and detached sweep admission are recorded only.
const removals = vi.hoisted(() => [] as string[]);
const sweepClaims = vi.hoisted(() => [] as string[]);
vi.mock("../src/agents/rm.js", () => ({
  removeTree: vi.fn(async (target: string) => { removals.push(target); }),
}));
vi.mock("../src/storage/verified-remove.js", async original => ({
  ...await original<typeof import("../src/storage/verified-remove.js")>(),
  verifiedRemove: vi.fn((target: string) => { removals.push(target); return { removed: true }; }),
}));
vi.mock("../src/storage/retention.js", async original => ({
  ...await original<typeof import("../src/storage/retention.js")>(),
  claimTempRunSweep: vi.fn((root: string) => { sweepClaims.push(root); return false; }),
}));

import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { CompletionJournal, type CompletionEnvelope, type CompletionRecipient } from "../src/agents/completion-journal.js";
import { ARCHIVE_PENDING_FILE, readPendingRunArchives } from "../src/agents/archive-custody.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { writeJsonAtomic } from "../src/core/atomic-write.js";
import { MeshStore } from "../src/mesh/store.js";
import type { FabricParticipantSource } from "../src/topology/types.js";
import type { AgentRunResult, AgentTransportHandle } from "../src/agents/types.js";
import { heapRunResult } from "./fixtures/manager-heap-probe.js";

const roots: string[] = [];
const managers: AgentManager[] = [];
const stores: MeshStore[] = [];
const readJson = <T>(file: string): T => JSON.parse(fs.readFileSync(file, "utf8")) as T;

const recordRemovals = (): void => {
  vi.spyOn(fs, "rmSync").mockImplementation(file => { removals.push(String(file)); });
  vi.spyOn(fs, "unlinkSync").mockImplementation(file => { removals.push(String(file)); });
  vi.spyOn(fs, "rmdirSync").mockImplementation(file => { removals.push(String(file)); });
  vi.spyOn(fs.promises, "rm").mockImplementation(async file => { removals.push(String(file)); });
  vi.spyOn(fs.promises, "unlink").mockImplementation(async file => { removals.push(String(file)); });
  vi.spyOn(fs.promises, "rmdir").mockImplementation(async file => { removals.push(String(file)); });
};

afterEach(async () => {
  const outcomes = await Promise.allSettled(managers.splice(0).map(manager => manager.close()));
  for (const store of stores.splice(0)) store.closeState();
  // Restore recording seams BEFORE the sole real cleanup: only directories
  // freshly allocated by this file, never shared run roots or session stores.
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  removals.splice(0);
  sweepClaims.splice(0);
  expect(outcomes.filter(outcome => outcome.status === "rejected")).toEqual([]);
});

type NativeRun = { result: AgentRunResult; runDirectory: string; finish(): void };
const fixture = (remote = true, hold = false, failSettlement = false) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "ca006-"));
  roots.push(root);
  const cwd = path.join(root, "caller-worktree");
  fs.mkdirSync(cwd);
  const meshRoot = path.join(root, "mesh");
  const runRoot = path.join(root, "caller-runs");
  for (const key of ["PI_FABRIC_BUDGET", "PI_FABRIC_BUDGET_FILE", "PI_FABRIC_BUDGET_ID"]) vi.stubEnv(key, undefined);
  vi.stubEnv("PI_FABRIC_DEPTH", "0");
  recordRemovals();
  const admitted: CompletionRecipient = {
    rootId: "session:caller", sessionId: "caller", projectRoot: cwd, cwd,
    name: "admitted-main", role: "lane-main", startedAt: 100,
  };
  const foreign: CompletionRecipient = { ...admitted, rootId: "session:foreign", sessionId: "foreign", name: "foreign-main" };
  let live = { ...admitted };
  const mesh = new MeshStore(meshRoot, DEFAULT_FABRIC_CONFIG.mesh.maxEventBytes, 100);
  stores.push(mesh);
  // save() does not need topology or claims; use the real file-backed journal
  // and store without starting any mesh transport, delivery loop or network.
  const participants = { list: () => [], get: () => undefined } as unknown as FabricParticipantSource;
  const journal = new CompletionJournal(meshRoot, () => live, participants, mesh, vi.fn());
  const fixedJournal = new CompletionJournal(meshRoot, admitted, participants, mesh, vi.fn());
  const nativeRuns = new Map<string, NativeRun>();
  vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
    const argument = (name: string): string => request.workerArguments[request.workerArguments.indexOf(name) + 1]!;
    const statusFile = argument("--status-file");
    const runDirectory = path.dirname(statusFile);
    const target = path.join(root, "target", "native", request.id);
    const targetCwd = path.join(target, "worktree");
    fs.mkdirSync(targetCwd, { recursive: true });
    const targetLog = path.join(target, "events.jsonl");
    fs.writeFileSync(targetLog, JSON.stringify({ type: "target.trace", id: request.id }) + "\n");
    fs.appendFileSync(argument("--log-file"), JSON.stringify({ type: "caller.trace", id: request.id }) + "\n");
    // A valid but unrelated manifest makes the wrong locator observable rather
    // than letting a missing target file hide accidental recipient selection.
    writeJsonAtomic(path.join(target, "completion-recipient.json"), { meshRoot, recipient: foreign });
    const now = Date.now();
    const result: AgentRunResult = {
      ...heapRunResult(request.id), name: request.name, task: fs.readFileSync(argument("--task-file"), "utf8"),
      cwd: targetCwd, logFile: targetLog, startedAt: now, updatedAt: now, finishedAt: now,
      text: "recorded native answer", exitCode: 0,
      // Deliberately remote-shaped even for the local counterexample. Only the
      // host-owned transport marker may select truthful remote worker paths.
      sessionId: `placement:${request.id}`, placementKind: "remote", placementHost: "recorded-target",
    };
    let alive = true;
    let release!: () => void;
    const closed = new Promise<void>(resolve => { release = resolve; });
    const finish = () => {
      if (!alive) return;
      writeJsonAtomic(statusFile, result);
      alive = false;
      release();
    };
    nativeRuns.set(request.id, { result, runDirectory, finish });
    if (hold) {
      const { finishedAt: _finishedAt, ...running } = result;
      writeJsonAtomic(statusFile, { ...running, status: "running" });
    } else finish();
    return {
      kind: "process", sessionId: remote ? `placement:${request.id}` : "2147483647",
      relaunchable: false, controls: !remote,
      // Placed handles have no local native-close notification. Exercise that
      // actual monitor branch; local fixtures keep the recorded process close.
      ...(remote ? {} : { closed, waitForClose: () => closed }),
      isAlive: async () => alive, stop: async () => { finish(); },
    } satisfies AgentTransportHandle;
  });
  const settled: { result: AgentRunResult; recipient: CompletionRecipient | undefined }[] = [];
  const background: { result: AgentRunResult; recipient: CompletionRecipient | undefined }[] = [];
  let observeBackground!: () => void;
  const backgroundObserved = new Promise<void>(resolve => { observeBackground = resolve; });
  // The native callback drives observation; the deadline only bounds a missing
  // notification. Foreground wait() intentionally suppresses background delivery.
  const waitForBackground = (): Promise<void> => {
    let timer: ReturnType<typeof setTimeout> | undefined;
    return Promise.race([backgroundObserved, new Promise<never>((_resolve, reject) => {
      timer = setTimeout(() => reject(new Error("background completion callback not observed")), 2_000);
    })]).finally(() => clearTimeout(timer));
  };
  let rejectSettlement = failSettlement;
  const createManager = (recovery = false) => {
    const manager = new AgentManager(cwd, {
      ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, maxConcurrent: 1,
      retainRuns: true, sessionExport: false, notifyOnComplete: true,
    }, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot, meshRoot,
      fullCodeMode: false, completionRecipient: () => ({ ...live }),
      shortTmpdirSource: () => ({ tmpdir: root }),
      onSettled: (result, recipient) => {
        settled.push({ result, recipient });
        if (!recovery && rejectSettlement) throw new Error("recorded journal outage");
        journal.save(result, recipient);
      },
      onBackgroundComplete: (result, recipient) => {
        background.push({ result, recipient });
        journal.save(result, recipient);
        observeBackground();
      },
    });
    managers.push(manager);
    return manager;
  };
  const manager = createManager();
  const envelope = (id: string): CompletionEnvelope => readJson(path.join(meshRoot, "agent-completions",
    `${createHash("sha256").update(id).digest("hex")}.json`));
  return { root, cwd, meshRoot, admitted, foreign, journal, fixedJournal, manager, nativeRuns, settled, background, envelope,
    createManager, waitForBackground, rename: () => { live = { ...foreign, name: "renamed-after-admission" }; },
    allowSettlement: () => { rejectSettlement = false; } };
};

const spawnRequest = { task: "CA-006 recorded worker", transport: "process" as const, extensions: false as const,
  writePolicy: "unrestricted" as const };

const assertPaths = (records: unknown[], expected: { cwd: string; logFile: string }): void => {
  for (const record of records) expect(record).toMatchObject({ status: "completed", exitCode: 0,
    text: "recorded native answer", ...expected });
};

describe("CA-006 native paths and caller-owned completion custody", () => {
  it("preserves remote cwd/logFile through monitor, settlement, wait/status and both actual saved records", async () => {
    const h = fixture();
    const handle = await h.manager.spawn(spawnRequest);
    const result = await h.manager.wait(handle.id, { timeoutMs: 2_000 });
    const native = h.nativeRuns.get(handle.id)!;
    const callerRun = h.manager.runDirectory(handle.id)!;
    expect(callerRun).toBe(native.runDirectory);
    expect(native.result.cwd).not.toBe(h.cwd);
    expect(native.result.logFile).not.toBe(path.join(callerRun, "events.jsonl"));
    const manifest = readJson<{ recipient: CompletionRecipient }>(path.join(callerRun, "completion-recipient.json"));
    expect(manifest.recipient).toEqual(h.admitted);
    assertPaths([result, h.manager.status(handle.id),
      readJson(path.join(callerRun, "status.json")), readJson(path.join(callerRun, "manager-result.json"))],
    { cwd: native.result.cwd, logFile: native.result.logFile! });
    const envelope = h.envelope(handle.id);
    expect(envelope.recipient).toEqual(manifest.recipient);
    expect(h.settled[0]?.recipient).toEqual(manifest.recipient);
    assertPaths([envelope.result], { cwd: native.result.cwd, logFile: native.result.logFile! });
    // Public trace truth does not redirect the manager's private log reader.
    const log = h.manager.readLog(handle.id);
    expect(log.runDirectory).toBe(callerRun);
    expect(log.logFile).toBe(path.join(callerRun, "events.jsonl"));
    expect(log.events.map(event => event.parsed)).toContainEqual({ type: "caller.trace", id: handle.id });
    expect(log.events.map(event => event.parsed)).not.toContainEqual({ type: "target.trace", id: handle.id });
    expect(fs.existsSync(path.join(path.dirname(native.result.logFile!), "status.json"))).toBe(false);
    expect(fs.existsSync(path.join(path.dirname(native.result.logFile!), "manager-result.json"))).toBe(false);
    expect(h.manager.isSettled(handle.id)).toBe(true);
    expect(h.manager.heldPermits()).toBe(0);
    await h.manager.close();
    expect(fs.existsSync(path.join(callerRun, "manager-result.json"))).toBe(true);
    expect(removals).not.toContain(callerRun);
    expect(sweepClaims).toEqual([]);
  });

  it("keeps a local worker canonical despite forged remote cwd/logFile/placement/session fields", async () => {
    const h = fixture(false);
    const handle = await h.manager.spawn(spawnRequest);
    const result = await h.manager.wait(handle.id, { timeoutMs: 2_000 });
    const run = h.manager.runDirectory(handle.id)!;
    const native = h.nativeRuns.get(handle.id)!.result;
    expect(native.sessionId).toBe(`placement:${handle.id}`);
    expect(native.cwd).not.toBe(h.cwd);
    const canonical = { cwd: h.cwd, logFile: path.join(run, "events.jsonl") };
    assertPaths([result, h.manager.status(handle.id), readJson(path.join(run, "status.json")),
      readJson(path.join(run, "manager-result.json")), h.envelope(handle.id).result], canonical);
    expect(result.sessionId).toBe("2147483647");
    expect(h.envelope(handle.id).recipient).toEqual(h.admitted);
    expect(h.manager.heldPermits()).toBe(0);
  });

  it("uses the immutable caller admission for background completion after the live Main changes", async () => {
    const h = fixture(true, true);
    const handle = await h.manager.spawn(spawnRequest);
    const run = h.manager.runDirectory(handle.id)!;
    const manifestBefore = fs.readFileSync(path.join(run, "completion-recipient.json"), "utf8");
    h.manager.detachSignal(handle.id);
    h.rename();
    const native = h.nativeRuns.get(handle.id)!;
    native.finish();
    await h.waitForBackground();
    const result = await h.manager.wait(handle.id, { timeoutMs: 2_000 });
    expect(h.background).toHaveLength(1);
    expect(h.background[0]?.recipient).toEqual(h.admitted);
    expect(h.settled[0]?.recipient).toEqual(h.admitted);
    expect(h.envelope(handle.id).recipient).toEqual(h.admitted);
    expect(fs.readFileSync(path.join(run, "completion-recipient.json"), "utf8")).toBe(manifestBefore);
    assertPaths([result, h.background[0]!.result, h.envelope(handle.id).result],
      { cwd: native.result.cwd, logFile: native.result.logFile! });
  });

  it("retains caller admission in a failed archive and replays it without consulting worker or live-Main locators", async () => {
    const h = fixture(true, false, true);
    const handle = await h.manager.spawn(spawnRequest);
    await h.manager.wait(handle.id, { timeoutMs: 2_000 });
    const run = h.manager.runDirectory(handle.id)!;
    const native = h.nativeRuns.get(handle.id)!;
    const pending = readPendingRunArchives(run);
    expect(pending).toHaveLength(1);
    expect(pending[0]).toMatchObject({ kind: "settlement", recipient: h.admitted });
    assertPaths([pending[0]!.result], { cwd: native.result.cwd, logFile: native.result.logFile! });
    h.rename();
    const recoveredManager = h.createManager(true);
    const read = fs.readFileSync;
    const forbidden = [path.join(run, "completion-recipient.json"),
      path.join(path.dirname(native.result.logFile!), "completion-recipient.json")];
    const manifestReads: string[] = [];
    const readSpy = vi.spyOn(fs, "readFileSync").mockImplementation(((...args: unknown[]) => {
      if (forbidden.includes(String(args[0]))) {
        manifestReads.push(String(args[0]));
        throw new Error("recovery must use the archived admission");
      }
      return Reflect.apply(read, fs, args);
    }) as typeof fs.readFileSync);
    try {
      expect(recoveredManager.recoverPendingArchives(run)).toBe(1);
      expect(manifestReads).toEqual([]);
      expect(h.envelope(handle.id).recipient).toEqual(h.admitted);
      assertPaths([h.envelope(handle.id).result], { cwd: native.result.cwd, logFile: native.result.logFile! });
      // A successful replay requests discharge; removal is deliberately mocked,
      // so its stale marker remaining is not mistaken for a failed recovery.
      expect(removals).toContain(path.join(run, ARCHIVE_PENDING_FILE));
    } finally { readSpy.mockRestore(); h.allowSettlement(); }
  });

  it.each(["absent", "corrupt"] as const)("retains a native completion with %s caller admission without reading or addressing the foreign manifest", async admission => {
    const h = fixture(true, true);
    const handle = await h.manager.spawn(spawnRequest);
    const native = h.nativeRuns.get(handle.id)!;
    const run = h.manager.runDirectory(handle.id)!;
    const callerManifest = path.join(run, "completion-recipient.json");
    const originalManifest = fs.readFileSync(callerManifest, "utf8");
    const foreignManifest = path.join(path.dirname(native.result.logFile!), "completion-recipient.json");
    expect(readJson<{ recipient: CompletionRecipient }>(foreignManifest).recipient).toEqual(h.foreign);
    if (admission === "corrupt") fs.writeFileSync(callerManifest, "{not a caller manifest");
    h.rename(); // Today's Main must not substitute for either missing or corrupt admission.
    const read = fs.readFileSync;
    const callerReads: string[] = [], foreignReads: string[] = [];
    const readSpy = vi.spyOn(fs, "readFileSync").mockImplementation(((...args: unknown[]) => {
      const file = String(args[0]);
      if (file === foreignManifest) foreignReads.push(file);
      if (file === callerManifest) {
        callerReads.push(file);
        // Model genuine ENOENT without deleting any fixture evidence.
        if (admission === "absent") throw Object.assign(new Error("recorded absent caller admission"), { code: "ENOENT" });
      }
      return Reflect.apply(read, fs, args);
    }) as typeof fs.readFileSync);
    try {
      native.finish();
      const result = await h.manager.wait(handle.id, { timeoutMs: 2_000 });
      const expected = { cwd: native.result.cwd, logFile: native.result.logFile! };
      assertPaths([result, h.manager.status(handle.id), readJson(path.join(run, "status.json")),
        readJson(path.join(run, "manager-result.json"))], expected);
      expect(callerReads.length).toBeGreaterThan(0);
      if (admission === "absent") {
        // The real manager helper returned undefined and the runtime-shaped
        // callback forwarded the existing second argument to the actual journal.
        expect(h.settled).toHaveLength(1);
        expect(h.settled[0]?.recipient).toBeUndefined();
        const pending = readPendingRunArchives(run);
        expect(pending).toHaveLength(1);
        expect(pending[0]?.recipient).toBeUndefined();
        assertPaths([pending[0]!.result], expected);
        const callerReadsBeforeRecovery = callerReads.length;
        expect(h.createManager(true).recoverPendingArchives(run)).toBe(0);
        expect(h.settled).toHaveLength(2);
        expect(h.settled[1]?.recipient).toBeUndefined();
        expect(callerReads).toHaveLength(callerReadsBeforeRecovery);
        expect(readPendingRunArchives(run)).toHaveLength(1);
        expect(removals).not.toContain(path.join(run, ARCHIVE_PENDING_FILE));
      } else {
        // Corrupt admission throws before callback/archive staging; the manager's
        // authenticated full result and save-failure custody are retained instead.
        expect(h.settled).toEqual([]);
        expect(readPendingRunArchives(run)).toEqual([]);
      }
      await expect(h.manager.cleanup(handle.id)).rejects.toThrow("Terminal result save failed; run retained");
      expect(h.manager.isSettled(handle.id)).toBe(true);
      expect(h.manager.heldPermits()).toBe(0);
      assertPaths([h.manager.status(handle.id), readJson(path.join(run, "manager-result.json"))], expected);
      expect(foreignReads).toEqual([]);
      expect(h.journal.pending()).toEqual([]);
      expect(h.journal.result(handle.id)).toBeUndefined();
      expect(removals).not.toContain(run);
      expect(sweepClaims).toEqual([]);
    } finally {
      readSpy.mockRestore();
      // Restore only this test's admission seam for normal manager teardown,
      // after asserting that failed publication cannot authorize collection.
      if (admission === "corrupt") fs.writeFileSync(callerManifest, originalManifest);
    }
  });

  it("distinguishes explicit absent admission from legacy lookup and preserves fixed-address fallback", () => {
    const h = fixture();
    const target = path.join(h.root, "foreign-log");
    fs.mkdirSync(target);
    const manifest = path.join(target, "completion-recipient.json");
    writeJsonAtomic(manifest, { meshRoot: h.meshRoot, recipient: h.foreign });
    const result = { ...heapRunResult("c".repeat(32)), cwd: target, logFile: path.join(target, "events.jsonl") };
    const read = fs.readFileSync;
    const manifestReads: string[] = [];
    const readSpy = vi.spyOn(fs, "readFileSync").mockImplementation(((...args: unknown[]) => {
      if (String(args[0]) === manifest) manifestReads.push(manifest);
      return Reflect.apply(read, fs, args);
    }) as typeof fs.readFileSync);
    try {
      h.rename();
      expect(() => h.journal.save(result, undefined)).toThrow("Missing admitted completion recipient");
      expect(h.journal.result(result.id)).toBeUndefined();
      h.fixedJournal.save({ ...result, id: "d".repeat(32) }, undefined);
      expect(h.envelope("d".repeat(32)).recipient).toEqual(h.admitted);
      h.journal.save({ ...result, id: "e".repeat(32) }, h.admitted);
      expect(h.envelope("e".repeat(32)).recipient).toEqual(h.admitted);
      expect(manifestReads).toEqual([]);
    } finally { readSpy.mockRestore(); }
  });

  it("preserves one-argument local-manifest lookup and legacy fixed-address fallback", () => {
    const h = fixture();
    const localRun = path.join(h.root, "legacy-local-run");
    fs.mkdirSync(localRun);
    writeJsonAtomic(path.join(localRun, "completion-recipient.json"), { meshRoot: h.meshRoot, recipient: h.admitted });
    const result = { ...heapRunResult("f".repeat(32)), cwd: localRun, logFile: path.join(localRun, "events.jsonl") };
    h.rename();
    h.journal.save(result);
    expect(h.envelope(result.id)).toEqual({ format: 1, recipient: h.admitted, result });
    const { logFile: _logFile, ...withoutLog } = { ...result, id: "0".repeat(32) };
    h.fixedJournal.save(withoutLog);
    expect(h.envelope(withoutLog.id)).toEqual({ format: 1, recipient: h.admitted, result: withoutLog });
    h.fixedJournal.save({ ...result, id: "1".repeat(32), logFile: path.join(h.root, "no-manifest", "events.jsonl") });
    expect(h.envelope("1".repeat(32)).recipient).toEqual(h.admitted);
  });

  it("gives explicit host admission precedence even over a malformed worker-log manifest", () => {
    const h = fixture();
    const target = path.join(h.root, "foreign-log");
    fs.mkdirSync(target);
    const manifest = path.join(target, "completion-recipient.json");
    fs.writeFileSync(manifest, "{not a manifest");
    const result = { ...heapRunResult("a".repeat(32)), cwd: target, logFile: path.join(target, "events.jsonl") };
    expect(() => h.journal.save(result, h.admitted)).not.toThrow();
    expect(h.envelope(result.id)).toEqual({ format: 1, recipient: h.admitted, result });
    // Legacy path-based callers without explicit host metadata still validate
    // their manifest; the precedence repair must not suppress those errors.
    expect(() => h.journal.save({ ...result, id: "b".repeat(32) })).toThrow();
  });
});
