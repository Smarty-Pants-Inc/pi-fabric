import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { HostedRun } from "../src/agents/hosted-run.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { WorktreeManager } from "../src/agents/worktree-manager.js";
import type { AgentRunRecord, AgentTransportHandle, AgentTransportLaunch } from "../src/agents/types.js";
import { registerAgentRunner, type FabricHostedReporter, type FabricHostedRunner } from "../src/agents/runner-registry.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import * as removal from "../src/agents/rm.js";
import * as logSource from "../src/run-log-source.js";
import * as logTail from "../src/log-tail.js";

const codec = vi.hoisted(() => ({ compress: vi.fn() }));
// Lifecycle cases stub the codec; the public-path case uses it with destructive calls recorded.
vi.mock("../src/storage/run-log-compression.js", () => ({ compressTerminalRunLog: codec.compress }));

const deferred = <T = void>() => {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((done, fail) => { resolve = done; reject = fail; });
  return { promise, resolve, reject };
};
const managers: AgentManager[] = [];
const releases: Array<() => void> = [];
const unregister: Array<() => void> = [];
const deletions: string[] = [];

beforeEach(() => {
  codec.compress.mockReset().mockResolvedValue({ kind: "compressed", originalBytes: 100, compressedBytes: 30 });
  deletions.length = 0;
  // Recorded-call seam: even incidental temporary-file and marker removals must not execute locally.
  vi.spyOn(fs, "rmSync").mockImplementation(file => { deletions.push(String(file)); });
  vi.spyOn(fs, "unlinkSync").mockImplementation(file => { deletions.push(String(file)); });
  vi.spyOn(fs, "rmdirSync").mockImplementation(file => { deletions.push(String(file)); });
  vi.spyOn(removal, "removeTree").mockImplementation(async file => { deletions.push(String(file)); });
  vi.spyOn(WorktreeManager.prototype, "cleanup").mockResolvedValue(false);
  vi.spyOn(ProcessTransport.prototype, "available").mockResolvedValue(true);
  vi.spyOn(console, "warn").mockImplementation(() => {});
  for (const key of ["PI_FABRIC_BUDGET", "PI_FABRIC_BUDGET_FILE", "PI_FABRIC_BUDGET_ID"]) vi.stubEnv(key, undefined);
});

afterEach(async () => {
  for (const release of releases.splice(0)) release();
  await Promise.allSettled(managers.splice(0).map(manager => manager.close()));
  for (const dispose of unregister.splice(0)) dispose();
  vi.unstubAllEnvs();
  vi.restoreAllMocks();
});

const managerFor = (root = fs.mkdtempSync(path.join(os.tmpdir(), "terminal-run-log-")), maxConcurrent = 4) => {
  const manager = new AgentManager(process.cwd(), {
    ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: true, budgetUsd: 0, maxConcurrent,
    sessionExport: false,
  }, { runRoot: root, workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fullCodeMode: false });
  managers.push(manager);
  return manager;
};

const worker = (options: { windows?: boolean; lost?: string; debt?: string; stopJoin?: Promise<void> } = {}) => {
  let alive = true;
  let request!: AgentTransportLaunch;
  const close = deferred();
  releases.push(() => { alive = false; close.resolve(); });
  const transport: AgentTransportHandle = {
    kind: "process", sessionId: "2147483647", isAlive: async () => alive,
    stop: vi.fn(async () => { await options.stopJoin; alive = false; close.resolve(); }),
    closed: close.promise,
    ...(options.windows ? { waitForClose: () => close.promise } : {}),
    ...(options.lost ? { lostContact: () => options.lost } : {}),
    ...(options.debt ? { stopDebt: () => options.debt } : {}),
  };
  const publish = (patch: Partial<AgentRunRecord> = {}) => {
    const now = Date.now();
    const statusFile = request.workerArguments[request.workerArguments.indexOf("--status-file") + 1]!;
    const record: AgentRunRecord = {
      id: request.id, name: request.name, task: "terminal lifecycle", runner: "pi", transport: "process", sessionId: "2147483647",
      cwd: request.cwd, status: "running", startedAt: now, updatedAt: now, turns: 0, toolCalls: 0,
      text: "", usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 }, ...patch,
    };
    fs.writeFileSync(statusFile, JSON.stringify(record));
  };
  return {
    transport, publish,
    launch: async (launch: AgentTransportLaunch) => { request = launch; publish(); return transport; },
    exit: () => { alive = false; close.resolve(); },
  };
};

const spawnWorker = async (fake = worker()) => {
  const manager = managerFor();
  vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(fake.launch);
  const handle = await manager.spawn({ task: "terminal lifecycle", transport: "process", extensions: false });
  const directory = manager.runDirectory(handle.id)!;
  const logFile = path.join(directory, "events.jsonl");
  fs.writeFileSync(logFile, '{"type":"message_end","text":"exact bytes π"}\n');
  return { manager, handle, directory, logFile, fake };
};

const hosted = (options: { startJoin?: Promise<void>; delivery?: Promise<void> } = {}) => {
  let reporter!: FabricHostedReporter;
  const adapter: FabricHostedRunner = {
    kind: "hosted", id: "terminal-log-hosted", label: "Terminal log hosted",
    capabilities: {
      recursiveFabric: false, steer: true, followUp: false, persistentSessions: false,
      kernels: false, handoff: false, modelDiscovery: false, imageInput: false, compaction: false,
      questions: false, sleep: false, writePolicy: false,
    },
    prepare: () => ({ job: "owned" }),
    start: async (_locator, _context, target) => { reporter = target; await options.startJoin; },
    attach: vi.fn(() => {}), liveness: () => "running", stop: () => ({ confirmed: true }),
    steer: async () => { await options.delivery; },
  };
  unregister.push(registerAgentRunner(adapter));
  return { adapter, reporter: () => reporter };
};

// Decision C rows and hosted E2E plans: .local/task-tmp/run-log-lifecycle-decision-c.md.
describe("owner terminal run-log finalization (#3252)", () => {
  it("compresses only a settled exact execution and memoizes concurrent archive joins", async () => {
    const { manager, handle, logFile, fake } = await spawnWorker();
    expect(codec.compress).not.toHaveBeenCalled();
    fake.publish({ status: "completed", text: "canonical result", finishedAt: Date.now() });
    fake.exit();
    await expect(manager.wait(handle.id)).resolves.toMatchObject({ status: "completed", text: "canonical result" });
    await vi.waitFor(() => expect(codec.compress).toHaveBeenCalledExactlyOnceWith(logFile));
    await Promise.all([manager.finalizeRunLog(handle.id), manager.finalizeRunLog(handle.id)]);
    await manager.stop(handle.id);
    expect(codec.compress).toHaveBeenCalledOnce();
    expect(fs.readFileSync(logFile, "utf8")).toContain("exact bytes π");
  });

  it("returns identical public agents.log events after the true-terminal owner compresses its run", async () => {
    // Real codec and public receiver; only the destructive commit is recorded locally.
    codec.compress.mockImplementation(async (file: string) => {
      const actual = await vi.importActual<typeof import("../src/storage/run-log-compression.js")>("../src/storage/run-log-compression.js");
      return actual.compressTerminalRunLog(file);
    });
    const { manager, handle, logFile, fake } = await spawnWorker();
    const events = Array.from({ length: 170 }, (_, index) => JSON.stringify({
      type: "message_end", message: { role: "assistant", content: [{ type: "text", text: `${index}: exact π 😀` }] },
    })).join("\r\n") + "\r\n";
    fs.writeFileSync(logFile, events);
    const before = manager.readLog(handle.id, { lines: 37 });
    fake.publish({ status: "completed", text: "canonical result is independent", finishedAt: Date.now() });
    fake.exit();
    await expect(manager.wait(handle.id)).resolves.toMatchObject({ status: "completed", text: "canonical result is independent" });
    await manager.finalizeRunLog(handle.id);
    expect(codec.compress).toHaveBeenCalledExactlyOnceWith(logFile);
    expect(deletions.filter(file => file === logFile)).toHaveLength(1);
    expect(fs.readFileSync(logFile, "utf8")).toBe(events);
    expect(fs.existsSync(`${logFile}.zst`)).toBe(true);
    // Fixture-only namespace transition models the recorded unlink without executing product deletion.
    fs.renameSync(logFile, `${logFile}.fixture-plain`);
    const after = manager.readLog(handle.id, { lines: 37 });
    expect(after.events).toEqual(before.events);
    expect(after.before).toBe(before.before);
    expect(after.generation).not.toBe(before.generation);
    const { before: cursor, generation } = before;
    if (cursor === undefined || generation === undefined) throw new Error("fixture must produce a generation-bound older-page cursor");
    expect(() => manager.readLog(handle.id, { before: cursor, beforeGeneration: generation })).toThrow(/cursor-stale/);
    expect((await manager.wait(handle.id)).text).toBe("canonical result is independent");
  });

  it.each(["startup-retry", "resume"] as const)("keeps a %s attempt plain until its replacement execution settles", async kind => {
    const first = worker();
    const second = worker();
    const manager = managerFor();
    const launch = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementationOnce(first.launch).mockImplementationOnce(second.launch);
    const handle = await manager.spawn({ task: "retry lifecycle", transport: "process", extensions: false });
    const logFile = path.join(manager.runDirectory(handle.id)!, "events.jsonl");
    fs.writeFileSync(logFile, '{"type":"progress","text":"first attempt"}\n');
    first.publish({ status: kind === "resume" ? "stopped" : "failed", turns: kind === "resume" ? 1 : 0,
      error: kind === "resume" ? "interrupted" : "No API key found" });
    first.exit();
    await vi.waitFor(() => expect(launch).toHaveBeenCalledTimes(2), { timeout: 5_000 });
    expect(codec.compress).not.toHaveBeenCalled();
    await expect(manager.finalizeRunLog(handle.id)).rejects.toThrow(/writer|terminal|settled|quiescen/i);
    fs.appendFileSync(logFile, '{"type":"progress","text":"replacement"}\n');
    second.publish({ status: "completed", text: "replacement result", finishedAt: Date.now() });
    second.exit();
    await expect(manager.wait(handle.id)).resolves.toMatchObject({ status: "completed", text: "replacement result" });
    await manager.finalizeRunLog(handle.id);
    expect(codec.compress).toHaveBeenCalledExactlyOnceWith(logFile);
    expect(fs.readFileSync(logFile, "utf8")).toContain("replacement");
  });

  it("keeps an early final answer plain until its existing native drain closes", async () => {
    const { manager, handle, directory, logFile, fake } = await spawnWorker();
    const receipt = { version: 1, id: "native-receipt", runId: handle.id, recordedAt: Date.now(), text: "immutable answer" };
    fs.writeFileSync(path.join(directory, "final-answer.json"), JSON.stringify(receipt));
    fake.publish({ status: "completed", text: "immutable answer", finalAnswerReceipt: { id: receipt.id, recordedAt: receipt.recordedAt } });
    await expect(manager.wait(handle.id)).resolves.toMatchObject({ text: "immutable answer" });
    expect(codec.compress).not.toHaveBeenCalled();
    let joined = false;
    const joining = manager.finalizeRunLog(handle.id).then(() => { joined = true; });
    await new Promise<void>(resolve => setImmediate(resolve));
    expect(joined).toBe(false);
    fs.appendFileSync(logFile, '{"type":"native_drain","text":"last frame"}\n');
    fake.exit();
    await joining;
    expect(codec.compress).toHaveBeenCalledExactlyOnceWith(logFile);
    expect(fs.readFileSync(logFile, "utf8")).toContain("last frame");
  });

  it("keeps Windows logical settlement plain until captured close and stop join", async () => {
    const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
    const stopJoin = deferred();
    releases.push(() => stopJoin.resolve());
    const fixture = await spawnWorker(worker({ windows: true, stopJoin: stopJoin.promise }));
    const { manager, handle, fake, logFile } = fixture;
    Object.defineProperty(process, "platform", { ...platform, value: "win32" });
    try {
      fake.publish({ status: "completed", text: "Windows outcome", finishedAt: Date.now() });
      await expect(manager.wait(handle.id)).resolves.toMatchObject({ text: "Windows outcome" });
      expect(codec.compress).not.toHaveBeenCalled();
      const stopping = manager.stop(handle.id);
      await vi.waitFor(() => expect(fake.transport.stop).toHaveBeenCalledOnce());
      fake.exit();
      let joined = false;
      const joining = manager.finalizeRunLog(handle.id).then(() => { joined = true; });
      await new Promise<void>(resolve => setImmediate(resolve));
      expect(joined).toBe(false);
      expect(codec.compress).not.toHaveBeenCalled();
      stopJoin.resolve();
      await stopping;
      await joining;
      expect(codec.compress).toHaveBeenCalledExactlyOnceWith(logFile);
      await manager.close();
    } finally {
      stopJoin.resolve(); fake.exit();
      await manager.close();
      Object.defineProperty(process, "platform", platform);
    }
  });

  it.each(["lost contact", "native stop debt"] as const)("does not compress an unknown process exit with %s", async reason => {
    // Independent counterexamples: native debt must veto even without lostContact.
    const fake = worker(reason === "native stop debt" ? { debt: reason } : { lost: reason });
    const { manager, handle, logFile } = await spawnWorker(fake);
    if (reason === "native stop debt") fake.publish({ status: "completed", text: "logical outcome with retained tree debt" });
    fake.exit();
    await expect(manager.wait(handle.id)).resolves.toMatchObject(reason === "native stop debt"
      ? { status: "completed", text: "logical outcome with retained tree debt" }
      : { status: "failed", error: expect.stringContaining(reason) });
    await expect(manager.finalizeRunLog(handle.id)).rejects.toThrow(/writer|exit|custody|quiescen/i);
    expect(codec.compress).not.toHaveBeenCalled();
    expect(manager.retentionCustodyVeto(handle.id)).toBe(true);
    expect(fs.readFileSync(logFile, "utf8")).toContain("exact bytes π");
    await expect(manager.cleanup(handle.id)).rejects.toThrow(/lost track|teardown|terminal|custody/i);
  });

  it("closes the hosted reporter before compression and fences late delivery failures", async () => {
    const delivery = deferred();
    const fake = hosted({ delivery: delivery.promise });
    const manager = managerFor();
    const handle = await manager.spawn({ task: "hosted lifecycle", runner: fake.adapter.id });
    const logFile = path.join(manager.runDirectory(handle.id)!, "events.jsonl");
    const close = vi.spyOn(HostedRun.prototype, "close");
    fake.reporter().transcript({ type: "message_end", message: { role: "assistant", content: "before finish" } });
    manager.steer(handle.id, "pending delivery");
    fake.reporter().finish({ status: "completed", output: "hosted result" });
    // A terminal worker record is not yet the manager's reporter-close fence.
    fake.reporter().transcript({ type: "message_end", message: { role: "assistant", content: "after finish before close" } });
    await expect(manager.wait(handle.id)).resolves.toMatchObject({ text: "hosted result" });
    await manager.finalizeRunLog(handle.id);
    expect(close).toHaveBeenCalledOnce();
    expect(codec.compress).toHaveBeenCalledExactlyOnceWith(logFile);
    expect(fs.readFileSync(logFile, "utf8")).toContain("after finish before close");
    const frozen = fs.readFileSync(logFile, "utf8");
    fake.reporter().transcript({ type: "message_end", message: { role: "assistant", content: "late transcript" } });
    fake.reporter().progress({ text: "late progress" });
    delivery.reject(new Error("late delivery failure"));
    await new Promise<void>(resolve => setImmediate(resolve));
    expect(fs.readFileSync(logFile, "utf8")).toBe(frozen);
    await manager.finalizeRunLog(handle.id);
    expect(codec.compress).toHaveBeenCalledOnce();
  });

  it("leaves a detached hosted run plain and lets only its new owner finalize after attach", async () => {
    const fake = hosted();
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "terminal-hosted-reattach-"));
    const first = managerFor(root);
    const handle = await first.spawn({ task: "durable hosted", runner: fake.adapter.id, residency: "durable" });
    const logFile = path.join(root, handle.id, "events.jsonl");
    fake.reporter().transcript({ type: "message_end", message: { role: "assistant", content: "before detach" } });
    const oldReporter = fake.reporter();
    await first.close();
    await expect(first.finalizeRunLog(handle.id)).rejects.toThrow(/writer|detached|settled|quiescen/i);
    oldReporter.transcript({ type: "message_end", message: { role: "assistant", content: "detached late" } });
    expect(codec.compress).not.toHaveBeenCalled();
    const attaching = deferred();
    releases.push(() => attaching.resolve());
    // Registration binds the original method; configure its implementation rather than
    // replacing an adapter property that the registry has already snapshotted.
    vi.mocked(fake.adapter.attach).mockImplementation(async (_locator, _context, reporter) => {
      reporter.transcript({ type: "message_end", message: { role: "assistant", content: "reattached" } });
      reporter.finish({ status: "completed", output: "new owner result" });
      await attaching.promise;
    });
    const second = managerFor(root);
    const recovery = second.recoverHostedRuns();
    await vi.waitFor(() => expect(second.hasRunCustody(handle.id)).toBe(true));
    expect(codec.compress).not.toHaveBeenCalled();
    attaching.resolve();
    await recovery;
    await expect(second.wait(handle.id)).resolves.toMatchObject({ text: "new owner result" });
    await second.finalizeRunLog(handle.id);
    expect(codec.compress).toHaveBeenCalledExactlyOnceWith(logFile);
    expect(fs.readFileSync(logFile, "utf8")).toContain("reattached");
    expect(fs.readFileSync(logFile, "utf8")).not.toContain("detached late");
  });

  it("joins the finalizer before recorded cleanup without delaying canonical completion", async () => {
    const compressed = deferred<{ kind: "compressed" }>();
    releases.push(() => compressed.resolve({ kind: "compressed" }));
    codec.compress.mockReturnValue(compressed.promise);
    const { manager, handle, fake, logFile } = await spawnWorker();
    fake.publish({ status: "completed", text: "answer before codec" }); fake.exit();
    await expect(manager.wait(handle.id)).resolves.toMatchObject({ text: "answer before codec" });
    await vi.waitFor(() => expect(codec.compress).toHaveBeenCalledExactlyOnceWith(logFile));
    let joined = false;
    const finalizing = manager.finalizeRunLog(handle.id).then(() => { joined = true; });
    let cleanupFinished = false;
    const cleaning = manager.cleanup(handle.id).then(result => { cleanupFinished = true; return result; });
    await new Promise<void>(resolve => setImmediate(resolve));
    expect(joined).toBe(false);
    expect(cleanupFinished).toBe(false);
    expect(WorktreeManager.prototype.cleanup).not.toHaveBeenCalled();
    expect(deletions).not.toContain(manager.runDirectory(handle.id));
    compressed.resolve({ kind: "compressed" });
    await finalizing;
    await expect(cleaning).resolves.toEqual({ cleaned: false }); // retainRuns keeps the real source; cleanup itself is recorded.
    expect(joined).toBe(true);
    expect(cleanupFinished).toBe(true);
    expect(WorktreeManager.prototype.cleanup).toHaveBeenCalledExactlyOnceWith(handle.id, false);
    expect(manager.hasRunCustody(handle.id)).toBe(false);
    expect(codec.compress).toHaveBeenCalledOnce();
  });

  it.each(["mismatch", "integrity"] as const)("surfaces one typed %s codec failure through agents.log and archive joins while retaining plain", async reason => {
    const { manager, handle, fake, logFile } = await spawnWorker();
    const failure = Object.assign(new Error(`Run log ${reason}`), { code: "run-log-codec", reason, path: logFile });
    codec.compress.mockRejectedValue(failure);
    fake.publish({ status: "completed", text: "canonical answer survives", value: { answer: "typed reply" }, replyVia: "tool" }); fake.exit();
    await expect(manager.wait(handle.id)).resolves.toMatchObject({
      status: "completed", text: "canonical answer survives", value: { answer: "typed reply" }, replyVia: "tool",
    });
    await expect(manager.finalizeRunLog(handle.id)).rejects.toBe(failure);
    expect(() => manager.readLog(handle.id)).toThrow(failure);
    expect(() => manager.readLog(handle.id)).toThrow(expect.objectContaining({ code: "run-log-codec", reason, path: logFile }));
    await expect(manager.finalizeRunLog(handle.id)).rejects.toBe(failure);
    await expect(manager.cleanup(handle.id)).rejects.toBe(failure);
    expect(codec.compress).toHaveBeenCalledOnce();
    expect(console.warn).toHaveBeenCalledExactlyOnceWith(`[pi-fabric] ${failure.message}`);
    expect(fs.readFileSync(logFile, "utf8")).toContain("exact bytes π");
    expect(WorktreeManager.prototype.cleanup).not.toHaveBeenCalled();
  });

  it("joins the finalizer before discharging actor source custody", async () => {
    const compressed = deferred<{ kind: "compressed" }>();
    releases.push(() => compressed.resolve({ kind: "compressed" }));
    codec.compress.mockReturnValue(compressed.promise);
    const { manager, handle, fake, directory, logFile } = await spawnWorker();
    const marker = path.join(directory, "actor-run-archive-pending.json");
    fs.writeFileSync(marker, JSON.stringify({ format: 1, runId: handle.id, actorId: "actor", sessionFile: "actor-session.jsonl" }));
    fake.publish({ status: "completed", text: "actor canonical answer" }); fake.exit();
    await expect(manager.wait(handle.id)).resolves.toMatchObject({ text: "actor canonical answer" });
    await vi.waitFor(() => expect(codec.compress).toHaveBeenCalledExactlyOnceWith(logFile));
    let joined = false;
    const committing = manager.commitActorArchive(handle.id, "actor", "actor-session.jsonl").then(() => { joined = true; });
    await new Promise<void>(resolve => setImmediate(resolve));
    expect(joined).toBe(false);
    expect(deletions).not.toContain(marker);
    expect(fs.existsSync(marker)).toBe(true);
    compressed.resolve({ kind: "compressed" });
    await committing;
    expect(deletions.filter(file => file === marker)).toHaveLength(1);
    expect(fs.readFileSync(logFile, "utf8")).toContain("exact bytes π");
    expect(manager.hasRunCustody(handle.id)).toBe(true);
  });

  it("joins recovered terminal sources without requiring a live handle or recompressing history", async () => {
    const { manager, handle, fake, directory, logFile } = await spawnWorker();
    fake.publish({ status: "completed", text: "saved old answer" }); fake.exit();
    await manager.wait(handle.id);
    await manager.finalizeRunLog(handle.id);
    const frozen = fs.readFileSync(logFile, "utf8");
    await manager.cleanup(handle.id);
    expect(manager.hasRunCustody(handle.id)).toBe(false);
    const next = managerFor(path.dirname(directory));
    // This marker is archive custody, not native writer custody. It must not deadlock its own join.
    fs.writeFileSync(path.join(directory, "actor-run-archive-pending.json"), JSON.stringify({
      format: 1, runId: handle.id, actorId: "old-actor", sessionFile: "old-session.jsonl",
    }));
    await next.finalizeRunLog(handle.id);
    expect(next.hasRunCustody(handle.id)).toBe(false);
    expect(codec.compress).toHaveBeenCalledOnce();
    expect(fs.readFileSync(logFile, "utf8")).toBe(frozen);
    expect(next.actorArchiveSources("old-actor", "old-session.jsonl").get(handle.id)).toBe(directory);
  });

  it.each(["live process", "unknown identity", "unresolved worker", "nonterminal", "hosted without owner"] as const)(
    "never invents writer custody for an unowned %s", async reason => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "terminal-unknown-source-"));
      const manager = managerFor(root);
      const id = "b".repeat(32), directory = path.join(root, id);
      fs.mkdirSync(directory);
      fs.writeFileSync(path.join(directory, "events.jsonl"), "unknown writer bytes\n");
      fs.writeFileSync(path.join(directory, "status.json"), JSON.stringify({
        id, runner: "pi", status: reason === "nonterminal" ? "running" : "completed",
        transport: reason === "hosted without owner" ? "hosted" : "process",
        ...(reason !== "unknown identity" ? { sessionId: reason === "live process" ? String(process.pid) : "2147483647" } : {}),
      }));
      if (reason === "unresolved worker") fs.writeFileSync(path.join(directory, "unresolved-worker.json"), "{}");
      await expect(manager.finalizeRunLog(id)).rejects.toThrow(/unknown|unconfirmed|terminal|custody/i);
      expect(codec.compress).not.toHaveBeenCalled();
      expect(manager.hasRunCustody(id)).toBe(false);
      expect(fs.readFileSync(path.join(directory, "events.jsonl"), "utf8")).toBe("unknown writer bytes\n");
    },
  );

  it("joins a cancelled never-launched admission without inventing a writer or starting its codec", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "terminal-queued-source-"));
    const manager = managerFor(root, 1);
    const fake = worker();
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(fake.launch);
    const active = await manager.spawn({ task: "holds admission", transport: "process", extensions: false });
    const queued = await manager.spawn({ task: "never launched", transport: "process", extensions: false });
    expect(queued.status).toBe("queued");
    await expect(manager.finalizeRunLog(queued.id)).rejects.toThrow(/queued custody/i);
    await expect(manager.stop(queued.id)).resolves.toMatchObject({ status: "stopped" });
    await manager.finalizeRunLog(queued.id);
    expect(ProcessTransport.prototype.launch).toHaveBeenCalledOnce();
    expect(fake.transport.stop).not.toHaveBeenCalled();
    expect(codec.compress).not.toHaveBeenCalled();
    expect((await manager.status(active.id)).status).toBe("running");
  });

  it("keeps a typed log-stat failure visible without aborting host shutdown or replacing the result", async () => {
    const { manager, handle, fake, logFile } = await spawnWorker();
    const failure = Object.assign(new Error("Run log stat integrity"), { code: "run-log-codec", reason: "integrity", path: logFile });
    vi.spyOn(logSource, "statRunLogSource").mockImplementation(() => { throw failure; });
    fake.publish({ text: "last canonical prose" });
    await expect(manager.close()).resolves.toBeUndefined();
    await expect(manager.wait(handle.id)).resolves.toMatchObject({ status: "stopped", text: "last canonical prose" });
    expect(fake.transport.stop).toHaveBeenCalledOnce();
    expect(() => manager.readLog(handle.id)).toThrow(failure);
    await expect(manager.finalizeRunLog(handle.id)).rejects.toBe(failure);
    expect(console.warn).toHaveBeenCalledExactlyOnceWith(`[pi-fabric] ${failure.message}`);
    expect(fs.readFileSync(logFile, "utf8")).toContain("exact bytes π");
  });

  it("does not strand a dead worker's canonical failure when log-summary decoding fails", async () => {
    const fake = worker();
    fake.transport.relaunchable = false;
    const { manager, handle, logFile } = await spawnWorker(fake);
    const failure = Object.assign(new Error("Run log summary integrity"), { code: "run-log-codec", reason: "integrity", path: logFile });
    vi.spyOn(logTail, "readRunLogPage").mockImplementation(() => { throw failure; });
    fake.exit();
    await expect(manager.wait(handle.id)).resolves.toMatchObject({
      status: "failed", error: expect.stringContaining("Agent transport exited without a result"),
    });
    expect(() => manager.readLog(handle.id)).toThrow(failure);
    await expect(manager.finalizeRunLog(handle.id)).rejects.toBe(failure);
    expect(console.warn).toHaveBeenCalledExactlyOnceWith(`[pi-fabric] ${failure.message}`);
    expect(fs.readFileSync(logFile, "utf8")).toContain("exact bytes π");
  });

  it("accepts an observable codec skip with plain data and never scans historical runs", async () => {
    codec.compress.mockResolvedValue({ kind: "skipped", reason: "source-too-large" });
    const { manager, handle, fake, directory, logFile } = await spawnWorker();
    const historical = path.join(path.dirname(directory), "a".repeat(32));
    fs.mkdirSync(historical);
    fs.writeFileSync(path.join(historical, "events.jsonl"), "historical owner bytes\n");
    fake.publish({ status: "completed", text: "skip is not failure" }); fake.exit();
    await manager.wait(handle.id);
    await manager.finalizeRunLog(handle.id);
    expect(codec.compress).toHaveBeenCalledExactlyOnceWith(logFile);
    expect(console.warn).toHaveBeenCalledWith(expect.stringContaining("source-too-large"));
    expect(manager.readLog(handle.id).events[0]?.raw).toContain("exact bytes π");
    expect(fs.readFileSync(path.join(historical, "events.jsonl"), "utf8")).toBe("historical owner bytes\n");
  });
});
