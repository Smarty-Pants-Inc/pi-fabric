// smarty-dev#9855: task-agent runs (no actorId) left "execution exit unconfirmed" by the pre-pi-fabric#849 ENODEV
// scope observation kept unresolved-worker.json forever: #912 recovers actor runs only, and retention vetoes a run with
// the marker. Offline retention now settles each such run LOST once and clears the marker, so retention collects it.
// It never delivers a recovered answer (pi-fabric#1027 review round 1, principal-data P1): a same-uid worker can
// rewrite the run's recipient manifest and its exit proof, so nothing recovered here may cross to another session.
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
import { ageRunRoot } from "./helpers/age-run-root.js";
import { retainedRunDirectory } from "./helpers/retained-run-directory.js";
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
const OTHER = "99999999-0000-0000-0000-000000000000";
const recipientOf = (projectRoot: string, session = SESSION): CompletionRecipient =>
  ({ rootId: `session:${session}`, sessionId: session, cwd: projectRoot, projectRoot, name: "main", startedAt: 1 });
/** A Main session that holds a live host lease. */
const liveSpawner = (meshRoot: string, recipient: CompletionRecipient): void => {
  const now = Date.now();
  writeHostLease(meshRoot, { id: recipient.rootId, rootId: recipient.rootId, identityId: recipient.rootId, updatedAt: now,
    expiresAt: now + 60_000, startedAt: now, session: { id: recipient.sessionId, startedAt: now, updatedAt: now, expiresAt: now + 60_000 } });
};
interface Fixture { pid?: number; birth?: string; actorId?: string; settlement?: unknown; reason?: string; recipient?: CompletionRecipient;
  status?: string; text?: string }
const runDir = (runRoot: string, meshRoot: string, id: string, fixture: Fixture = {}): string => {
  const directory = path.join(runRoot, id);
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  fs.writeFileSync(path.join(directory, "status.json"), JSON.stringify({ id, name: "task-run", task: "t", status: fixture.status ?? "running",
    ...(fixture.text !== undefined ? { text: fixture.text } : {}),
    runner: "pi", transport: "process", startedAt: 1, sessionId: String(fixture.pid ?? deadPid()),
    ...(fixture.birth ? { processStartTime: fixture.birth } : {}), ...(fixture.actorId ? { actorId: fixture.actorId } : {}) }));
  fs.writeFileSync(path.join(directory, "unresolved-worker.json"), JSON.stringify({ reason: fixture.reason ?? ENODEV, markedAt: 1, runId: id }));
  fs.writeFileSync(path.join(directory, "completion-recipient.json"), JSON.stringify({ meshRoot,
    recipient: fixture.recipient ?? recipientOf(meshRoot), supervisor: { pid: deadPid() } }));
  if (fixture.settlement !== null) {
    fs.writeFileSync(path.join(directory, "settlement.json"),
      JSON.stringify(fixture.settlement ?? { version: 1, runId: id, outcome: "completed", text: "the saved answer" }));
  }
  return directory;
};
const vetoed = (directory: string): boolean => fs.existsSync(path.join(directory, "unresolved-worker.json"));
const status = (directory: string) => JSON.parse(fs.readFileSync(path.join(directory, "status.json"), "utf8"));
/** Every envelope in the mesh's completion journal, whoever it is addressed to. */
const journal = (meshRoot: string): string[] => {
  try { return fs.readdirSync(path.join(meshRoot, "agent-completions"), { recursive: true }).map(String).filter(name => name.endsWith(".json")); }
  catch { return []; }
};

describe.runIf(process.platform === "linux")("ENODEV-unconfirmed task-agent runs (smarty-dev#9855)", () => {
  it("(a) a saved answer, a live spawner AND a manifest naming another live session: settled lost, nothing delivered", () => {
    const meshRoot = tempRoot();
    const spawner = recipientOf(meshRoot);
    const other = recipientOf(meshRoot, OTHER);
    liveSpawner(meshRoot, spawner);
    liveSpawner(meshRoot, other);
    // The manifest a same-uid worker could have rewritten names another live Main session.
    const directory = runDir(tempRoot(), meshRoot, "a".repeat(32), { recipient: other });
    expect(recoverEnodevTaskRun(directory)).toBe("lost");
    expect(vetoed(directory)).toBe(false);
    expect(status(directory)).toMatchObject({ status: "completed", settledLost: true });
    // Recovery never writes answer text into status.json: the saved answer stays only in settlement.json.
    expect("text" in status(directory)).toBe(false);
    expect(JSON.parse(fs.readFileSync(path.join(directory, "settlement.json"), "utf8")).text).toBe("the saved answer");
    expect(status(directory).enodevDelivered).toBeUndefined();
    expect(journal(meshRoot)).toEqual([]);
    expect(pendingCompletions(meshRoot, meshRoot)).toHaveLength(0);
    // Once: a second sweep finds no marker and changes nothing.
    const settled = fs.readFileSync(path.join(directory, "status.json"), "utf8");
    expect(recoverEnodevTaskRun(directory)).toBeUndefined();
    expect(fs.readFileSync(path.join(directory, "status.json"), "utf8")).toBe(settled);
    expect(journal(meshRoot)).toEqual([]);
  });

  it("(b) no saved answer: failed lost, marker cleared, nothing delivered", () => {
    const meshRoot = tempRoot();
    liveSpawner(meshRoot, recipientOf(meshRoot));
    const directory = runDir(tempRoot(), meshRoot, "c".repeat(32), { settlement: null });
    expect(recoverEnodevTaskRun(directory)).toBe("lost");
    expect(vetoed(directory)).toBe(false);
    expect(status(directory)).toMatchObject({ status: "failed", settledLost: true });
    expect("text" in status(directory)).toBe(false);
    expect(journal(meshRoot)).toEqual([]);
  });

  it("(c) a live worker, an actor run or another reason: untouched, the veto stays", () => {
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
    // A settlement for another run is no saved answer: failed lost.
    const foreign = runDir(tempRoot(), meshRoot, "e".repeat(32), { settlement: { version: 1, runId: "0".repeat(32), outcome: "completed", text: "x" } });
    expect(recoverEnodevTaskRun(foreign)).toBe("lost");
    expect(status(foreign)).toMatchObject({ status: "failed", settledLost: true });
    expect(journal(meshRoot)).toEqual([]);
  });

  // Ryzen 1's real runs (copies from fabric-store) are mostly already terminal (completed with text, or stopped).
  it("(d) a record its manager made terminal keeps its outcome and gains settledLost", () => {
    const meshRoot = tempRoot();
    liveSpawner(meshRoot, recipientOf(meshRoot));
    const stopped = runDir(tempRoot(), meshRoot, "5".repeat(32), { status: "stopped", settlement: null });
    expect(recoverEnodevTaskRun(stopped)).toBe("lost");
    expect(status(stopped)).toMatchObject({ status: "stopped", settledLost: true });
    expect("text" in status(stopped)).toBe(false);
    expect(vetoed(stopped)).toBe(false);
    const completed = runDir(tempRoot(), meshRoot, "6".repeat(32), { status: "completed", text: "the manager's text" });
    expect(recoverEnodevTaskRun(completed)).toBe("lost");
    // A record that already has text keeps it unchanged (not replaced by the saved answer).
    expect(status(completed)).toMatchObject({ status: "completed", text: "the manager's text", settledLost: true });
    expect(journal(meshRoot)).toEqual([]);
  });

  // retention's run-file allowlist gains settlement.json (worker.ts writes it for every completed process run):
  // a terminal run with a dead worker is collected with it; a run whose worker lives is not.
  it("retention collects a terminal run holding settlement.json once its worker is dead, never while it lives", () => {
    const tmp = tempRoot("fabric-enodev-tmp-");
    const runRoot = path.join(tmp, "pi-fabric-runs-settled");
    fs.mkdirSync(runRoot, { mode: 0o700 });
    const run = (id: string, pid: number): string => {
      const directory = path.join(runRoot, id);
      fs.mkdirSync(directory, { mode: 0o700 });
      fs.writeFileSync(path.join(directory, "task.txt"), "t");
      fs.writeFileSync(path.join(directory, "status.json"), JSON.stringify({ id, name: "done", task: "t", status: "completed", text: "answer",
        runner: "pi", transport: "process", startedAt: 1, finishedAt: 2, sessionId: String(pid), processStartTime: processStartTime(pid) ?? "1" }));
      fs.writeFileSync(path.join(directory, "settlement.json"), JSON.stringify({ version: 1, runId: id, outcome: "completed", text: "answer" }));
      return directory;
    };
    const dead = run("7".repeat(32), deadPid());
    const live = run("8".repeat(32), livePid());
    const now = Date.now();
    fs.writeFileSync(path.join(runRoot, ".fabric-owner.json"), JSON.stringify({ pid: deadPid(), startedAt: now - 3_600_000,
      heartbeatAt: now - 3_600_000, orphanedAt: now - 3_600_000 }));
    const settlementBytes = fs.readFileSync(path.join(dead, "settlement.json"));
    // The sweep uses observed filesystem activity, not the synthetic owner times.
    ageRunRoot(runRoot, now - 3_600_000);
    sweepTempRunRoots({ tempRoot: tmp, orphanedTempRunRetentionMs: 1_000, oneShotRunRetentionMs: 1_000, now });
    expect(fs.existsSync(dead)).toBe(false);
    const archived = retainedRunDirectory(dead);
    expect(fs.readFileSync(path.join(archived, "settlement.json"))).toEqual(settlementBytes);
    expect(fs.existsSync(path.join(live, "settlement.json"))).toBe(true);
  });

  it("forced ENODEV in a real scratch process task: retention settles it lost once, delivers nothing, then collects it", async () => {
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
    fs.writeFileSync(path.join(directory, "status.json"), JSON.stringify({ id: handle.id, name: "scratch", task: "scratch",
      status: "running", runner: "pi", transport: "process", sessionId: String(worker.pid), startedAt: 1 }));
    // The worker saved its answer, then exited; its manager is gone too (a dead owner, orphaned long ago).
    fs.writeFileSync(path.join(directory, "settlement.json"), JSON.stringify({ version: 1, runId: handle.id, outcome: "completed", text: "SCRATCH ANSWER" }));
    worker.kill("SIGKILL");
    await closed;
    const owner = path.join(runRoot, ".fabric-owner.json");
    const now = Date.now();
    fs.writeFileSync(owner, JSON.stringify({ pid: deadPid(), startedAt: now - 3_600_000, heartbeatAt: now - 3_600_000, orphanedAt: now - 3_600_000 }));
    // This launch is intentionally mocked before a worker can emit a trace; seed
    // representative raw bytes so the archive assertion covers the log itself.
    const eventsFile = path.join(directory, "events.jsonl");
    if (!fs.existsSync(eventsFile)) fs.writeFileSync(eventsFile, '{"type":"raw","text":"SCRATCH ANSWER"}\n');
    const rawEvents = fs.readFileSync(eventsFile);
    const settlementBytes = fs.readFileSync(path.join(directory, "settlement.json"));
    // Recovery alone first (what the sweep runs before its safety walk): settled lost, once.
    expect(recoverEnodevTaskRun(directory)).toBe("lost");
    expect(vetoed(directory)).toBe(false);
    expect(status(directory)).toMatchObject({ status: "completed", settledLost: true });
    expect("text" in status(directory)).toBe(false);
    expect(recoverEnodevTaskRun(directory)).toBeUndefined();
    // The spawner's Main drains its journal: nothing arrives.
    const mesh = new MeshStore(meshRoot, 65536, 1000);
    const delivered: AgentRunResult[] = [];
    const completions = new CompletionJournal(meshRoot, recipient, {} as never, mesh, (result, done) => { delivered.push(result); done(); });
    await completions.drain();
    expect(delivered).toEqual([]);
    expect(journal(meshRoot)).toEqual([]);
    // With the marker gone and actual fixture activity aged past grace, retention
    // archives the whole run. Its raw trace and saved answer bytes remain intact.
    ageRunRoot(runRoot, now - 3_600_000);
    sweepTempRunRoots({ tempRoot: tmp, orphanedTempRunRetentionMs: 1_000, oneShotRunRetentionMs: 1_000, now });
    expect(fs.existsSync(directory)).toBe(false);
    const archived = retainedRunDirectory(directory);
    expect(fs.readFileSync(path.join(archived, "events.jsonl"))).toEqual(rawEvents);
    expect(fs.readFileSync(path.join(archived, "settlement.json"))).toEqual(settlementBytes);
    await completions.drain();
    expect(delivered).toEqual([]);
  });
});
