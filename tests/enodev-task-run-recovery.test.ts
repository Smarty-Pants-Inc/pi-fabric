// smarty-dev#9855: task-agent runs (no actorId) left "execution exit unconfirmed" by the pre-pi-fabric#849 ENODEV
// scope observation kept unresolved-worker.json forever: #912 recovers actor runs only, and retention vetoes a run with
// the marker. Offline retention now recovers them once: a saved answer goes to the run's recorded spawner when that
// exact Main session can still take it (the completion journal, keyed by run id), else the run settles lost; the
// marker goes, so retention collects the run.
import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { CompletionJournal, pendingCompletions, type CompletionRecipient } from "../src/agents/completion-journal.js";
import { recoverEnodevTaskRun } from "../src/agents/enodev-recovery.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import { processStartTime } from "../src/residency/process-identity.js";
import { sweepTempRunRoots } from "../src/storage/retention.js";
import { writeHostLease } from "../src/topology/host-leases.js";
import type { AgentRunResult } from "../src/agents/types.js";

const ENODEV = "Owned scope observation failed: Error: ENODEV: no such device, read";
const roots: string[] = [];
const children: ChildProcess[] = [];
const managers: AgentManager[] = [];
afterEach(async () => {
  vi.restoreAllMocks();
  for (const manager of managers.splice(0)) await manager.close().catch(() => undefined);
  for (const child of children.splice(0)) child.kill("SIGKILL");
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const tempRoot = (prefix = "fabric-enodev-task-"): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), prefix));
  roots.push(root);
  return root;
};
const deadPid = (): number => spawnSync(process.execPath, ["-e", ""]).pid!;
const livePid = (): number => {
  const child = spawn(process.execPath, ["-e", "setTimeout(() => {}, 60000)"], { stdio: "ignore" });
  children.push(child);
  return child.pid!;
};
const SESSION = "11111111-2222-3333-4444-555555555555";
const recipientOf = (projectRoot: string): CompletionRecipient =>
  ({ rootId: `session:${SESSION}`, sessionId: SESSION, cwd: projectRoot, projectRoot, name: "main", startedAt: 1 });
/** The spawner's Main session holds a live host lease (or `session` names another Main session). */
const liveSpawner = (meshRoot: string, recipient: CompletionRecipient, session = recipient.sessionId): void => {
  const now = Date.now();
  writeHostLease(meshRoot, { id: recipient.rootId, rootId: recipient.rootId, identityId: recipient.rootId, updatedAt: now,
    expiresAt: now + 60_000, startedAt: now, session: { id: session, startedAt: now, updatedAt: now, expiresAt: now + 60_000 } });
};
interface Fixture { pid?: number; birth?: string; actorId?: string; settlement?: unknown; reason?: string; recipient?: CompletionRecipient | null }
const runDir = (runRoot: string, meshRoot: string, id: string, fixture: Fixture = {}): string => {
  const directory = path.join(runRoot, id);
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  fs.writeFileSync(path.join(directory, "status.json"), JSON.stringify({ id, name: "task-run", task: "t", status: "running",
    runner: "pi", transport: "process", startedAt: 1, sessionId: String(fixture.pid ?? deadPid()),
    ...(fixture.birth ? { processStartTime: fixture.birth } : {}), ...(fixture.actorId ? { actorId: fixture.actorId } : {}) }));
  fs.writeFileSync(path.join(directory, "unresolved-worker.json"), JSON.stringify({ reason: fixture.reason ?? ENODEV, markedAt: 1, runId: id }));
  if (fixture.recipient !== null) {
    fs.writeFileSync(path.join(directory, "completion-recipient.json"), JSON.stringify({ meshRoot,
      recipient: fixture.recipient ?? recipientOf(meshRoot), supervisor: { pid: deadPid() } }));
  }
  if (fixture.settlement !== null) {
    fs.writeFileSync(path.join(directory, "settlement.json"),
      JSON.stringify(fixture.settlement ?? { version: 1, runId: id, outcome: "completed", text: "the saved answer" }));
  }
  return directory;
};
const vetoed = (directory: string): boolean => fs.existsSync(path.join(directory, "unresolved-worker.json"));
const status = (directory: string) => JSON.parse(fs.readFileSync(path.join(directory, "status.json"), "utf8"));

describe.runIf(process.platform === "linux")("ENODEV-unconfirmed task-agent runs (smarty-dev#9855)", () => {
  it("(a) a gone worker's saved answer reaches its live spawner once; a second sweep delivers nothing", () => {
    const meshRoot = tempRoot();
    const recipient = recipientOf(meshRoot);
    liveSpawner(meshRoot, recipient);
    const id = "a".repeat(32);
    const directory = runDir(tempRoot(), meshRoot, id);
    // The read-only report's dry run decides the same outcome and writes nothing.
    expect(recoverEnodevTaskRun(directory, { dryRun: true })).toBe("delivered");
    expect(vetoed(directory)).toBe(true);
    expect(pendingCompletions(meshRoot, meshRoot)).toHaveLength(0);
    expect(recoverEnodevTaskRun(directory)).toBe("delivered");
    expect(vetoed(directory)).toBe(false);
    expect(status(directory)).toMatchObject({ status: "completed", text: "the saved answer", enodevDelivered: true });
    const pending = pendingCompletions(meshRoot, meshRoot);
    expect(pending.map(envelope => [envelope.result.id, envelope.result.text, envelope.recipient.sessionId]))
      .toEqual([[id, "the saved answer", SESSION]]);
    // A second sweep: no marker, nothing to do.
    expect(recoverEnodevTaskRun(directory)).toBeUndefined();
    expect(pendingCompletions(meshRoot, meshRoot)).toHaveLength(1);
    // A crash after the save, before the marker went: the retry saves the SAME envelope (keyed by run id).
    fs.writeFileSync(path.join(directory, "unresolved-worker.json"), JSON.stringify({ reason: ENODEV, markedAt: 1, runId: id }));
    expect(recoverEnodevTaskRun(directory)).toBe("delivered");
    expect(pendingCompletions(meshRoot, meshRoot)).toHaveLength(1);
  });

  it("(b) no live spawner: settled lost, nothing delivered, marker cleared", () => {
    for (const [name, spawner] of [["no lease", undefined], ["another Main session", "99999999-0000-0000-0000-000000000000"],
      ["no recipient manifest", null]] as const) {
      const meshRoot = tempRoot();
      const recipient = recipientOf(meshRoot);
      if (typeof spawner === "string") liveSpawner(meshRoot, recipient, spawner);
      const directory = runDir(tempRoot(), meshRoot, "b".repeat(32), spawner === null ? { recipient: null } : {});
      expect(recoverEnodevTaskRun(directory), name).toBe("lost");
      expect(vetoed(directory), name).toBe(false);
      expect(status(directory), name).toMatchObject({ status: "completed", settledLost: true, text: "the saved answer" });
      expect(pendingCompletions(meshRoot, meshRoot), name).toHaveLength(0);
    }
    // No saved answer: failed lost, even with a live spawner.
    const meshRoot = tempRoot();
    liveSpawner(meshRoot, recipientOf(meshRoot));
    const directory = runDir(tempRoot(), meshRoot, "c".repeat(32), { settlement: null });
    expect(recoverEnodevTaskRun(directory)).toBe("lost");
    expect(status(directory)).toMatchObject({ status: "failed", settledLost: true });
    expect(pendingCompletions(meshRoot, meshRoot)).toHaveLength(0);
  });

  it("(c) a live worker, a foreign settlement, another reason or an actor run: untouched, the veto stays", () => {
    const meshRoot = tempRoot();
    liveSpawner(meshRoot, recipientOf(meshRoot));
    const pid = livePid();
    const cases: Array<[string, Fixture]> = [
      ["live worker", { pid, birth: processStartTime(pid)! }],
      ["live worker without a recorded birth", { pid }],
      ["actor run (#912's)", { actorId: "f".repeat(32) }],
      ["other reason", { reason: "execution exit unconfirmed: EIO" }],
    ];
    for (const [name, fixture] of cases) {
      const directory = runDir(tempRoot(), meshRoot, "d".repeat(32), fixture);
      const before = fs.readFileSync(path.join(directory, "status.json"), "utf8");
      expect(recoverEnodevTaskRun(directory), name).toBeUndefined();
      expect(vetoed(directory), name).toBe(true);
      expect(fs.readFileSync(path.join(directory, "status.json"), "utf8"), name).toBe(before);
    }
    // A settlement for another run is no saved answer: settled lost, not delivered.
    const foreign = runDir(tempRoot(), meshRoot, "e".repeat(32), { settlement: { version: 1, runId: "0".repeat(32), outcome: "completed", text: "x" } });
    expect(recoverEnodevTaskRun(foreign)).toBe("lost");
    expect(pendingCompletions(meshRoot, meshRoot)).toHaveLength(0);
  });

  it("forced ENODEV in a real scratch process task: after its manager is gone, retention delivers the answer once", async () => {
    const meshRoot = tempRoot();
    const tmp = tempRoot("fabric-enodev-tmp-");
    const runRoot = path.join(tmp, "pi-fabric-runs-scratch");
    fs.mkdirSync(runRoot, { mode: 0o700 });
    const recipient = recipientOf(meshRoot);
    liveSpawner(meshRoot, recipient);
    const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent: 2, budgetUsd: 0, retainRuns: true }, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot, meshRoot, completionRecipient: recipient });
    managers.push(manager);
    // The scope observation fails with ENODEV on a REAL worker process (the fault the pre-#849 code turned into the marker).
    const worker = spawn(process.execPath, ["-e", "setTimeout(() => {}, 120000)"], { stdio: "ignore" });
    children.push(worker);
    const closed = new Promise<void>(resolve => worker.once("exit", () => resolve()));
    // The manager never sees the exit: it dies before the worker does (no close event reaches it).
    const launch = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async () => {
      throw Object.assign(new Error(ENODEV), { launchOutcome: "unknown", cleanupPending: true, transport: "process",
        sessionId: String(worker.pid), closed: new Promise<void>(() => undefined) });
    });
    const handle = await manager.spawn({ task: "scratch", transport: "process" });
    launch.mockRestore();
    const directory = path.join(runRoot, handle.id);
    expect(vetoed(directory)).toBe(true);
    expect(fs.existsSync(path.join(directory, "completion-recipient.json"))).toBe(true);
    fs.writeFileSync(path.join(directory, "status.json"), JSON.stringify({ id: handle.id, name: "scratch", task: "scratch",
      status: "running", runner: "pi", transport: "process", sessionId: String(worker.pid), startedAt: 1 }));
    // The worker saved its answer, then exited; its manager is gone too (a dead owner, orphaned long ago).
    fs.writeFileSync(path.join(directory, "settlement.json"), JSON.stringify({ version: 1, runId: handle.id, outcome: "completed", text: "SCRATCH ANSWER" }));
    worker.kill("SIGKILL");
    await closed;
    const owner = path.join(runRoot, ".fabric-owner.json");
    const now = Date.now();
    fs.writeFileSync(owner, JSON.stringify({ pid: deadPid(), startedAt: now - 3_600_000, heartbeatAt: now - 3_600_000, orphanedAt: now - 3_600_000 }));
    const sweep = () => sweepTempRunRoots({ tempRoot: tmp, orphanedTempRunRetentionMs: 1_000, oneShotRunRetentionMs: 1_000, now });
    sweep();
    expect(vetoed(directory)).toBe(false);
    // The spawner's Main drains its journal: the answer arrives once.
    expect(pendingCompletions(meshRoot, meshRoot).map(envelope => envelope.result.id)).toEqual([handle.id]);
    const mesh = new MeshStore(meshRoot, 65536, 1000);
    const delivered: AgentRunResult[] = [];
    const journal = new CompletionJournal(meshRoot, recipient, {} as never, mesh,
      (result, done) => { delivered.push(result); done(); });
    await journal.drain();
    sweep();
    await journal.drain();
    expect(delivered.map(result => [result.id, result.status, result.text])).toEqual([[handle.id, "completed", "SCRATCH ANSWER"]]);
  });
});
