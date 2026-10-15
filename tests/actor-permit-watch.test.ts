import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ActorManager } from "../src/actors/manager.js";
import { ActorLogStore } from "../src/actors/log-store.js";
import { ActorChildCompletionStore } from "../src/actors/child-completions.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import type { AgentRunRecord, AgentTransportLaunch } from "../src/agents/types.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import * as processIdentity from "../src/residency/process-identity.js";

// Keep / Decision C (#9031): failure path not cheaply reachable in a user flow,
// plus the worker-to-host trust boundary. Real managers, queues and semaphore;
// only host process identity/native custody are scripted. No real launch, signal
// or product deletion. This is supplemental integration proof, not installed-system E2E.
const eventually = (assertion: () => void) => vi.waitFor(assertion, { timeout: 5_000, interval: 20 });
// Native close is the normal wake. This bounded observation window also gives
// the existing status/liveness monitor time to reject a forged terminal claim.
const observe = () => new Promise(resolve => setTimeout(resolve, 1_500));
const deletionCalls: string[] = [];
const fixtures: Fixture[] = [];
const fixtureRoots: string[] = [];
const workers = new Map<number, Worker>();
const claimedGonePid = 899_999_999;
let nextPid = 900_000_000;
let closing = false;
interface Worker {
  launch: AgentTransportLaunch;
  pid: number;
  alive: boolean;
  identityAlive?: boolean;
  birth?: string;
  probeError?: string;
  descendants: boolean;
  debt?: string;
  record: AgentRunRecord;
  statusFile: string;
  nativeClose: () => void;
}
interface Fixture {
  root: string;
  admission: string;
  agents: AgentManager;
  actors: ActorManager;
  createActors: () => ActorManager;
}

beforeEach(() => {
  closing = false;
  workers.clear();
  fixtureRoots.length = 0;
  deletionCalls.length = 0;
  for (const key of ["PI_FABRIC_DEPTH", "PI_FABRIC_BUDGET", "PI_FABRIC_BUDGET_FILE", "PI_FABRIC_BUDGET_ID",
    "DBUS_SESSION_BUS_ADDRESS", "XDG_RUNTIME_DIR"]) vi.stubEnv(key, undefined);
  vi.spyOn(process, "kill").mockImplementation((pid, signal) => {
    if (signal !== 0) throw new Error("No real or scripted worker signals allowed");
    const worker = workers.get(pid);
    const code = pid === claimedGonePid ? "ESRCH"
      : worker?.probeError ?? (worker && !(worker.identityAlive ?? worker.alive) ? "ESRCH" : undefined);
    if (code) throw Object.assign(new Error(code), { code });
    return true; // Other identities (e.g. this fixture's registry lock) stay live.
  });
  const birth = processIdentity.processStartTime;
  vi.spyOn(processIdentity, "processStartTime").mockImplementation(pid => workers.has(pid) ? workers.get(pid)!.birth : birth(pid));
  const quarantine = (file: fs.PathLike) => {
    let target = path.resolve(String(file));
    // Linux lock release addresses our directory through /proc/self/fd/<fd>.
    // Resolve that pinned parent before applying the fixture-only write fence.
    try { target = path.join(fs.realpathSync(path.dirname(target)), path.basename(target)); } catch { /* absent parent */ }
    deletionCalls.push(target);
    if (!fixtureRoots.some(root => target === root || target.startsWith(root + path.sep))) {
      throw new Error(`Deletion outside this test's fixture: ${target}`);
    }
    // Release metadata locks without deleting bytes. Leave all artifacts in TMPDIR.
    if (fs.existsSync(target)) fs.renameSync(target, `${target}.recorded-removal-${deletionCalls.length}`);
  };
  vi.spyOn(fs, "rmSync").mockImplementation(quarantine);
  vi.spyOn(fs, "rmdirSync").mockImplementation(quarantine);
  vi.spyOn(fs, "unlinkSync").mockImplementation(quarantine);
  vi.spyOn(fs.promises, "rm").mockImplementation(async file => { quarantine(file); });
  vi.spyOn(fs.promises, "rmdir").mockImplementation(async file => { quarantine(file); });
  vi.spyOn(fs.promises, "unlink").mockImplementation(async file => { quarantine(file); });
  vi.spyOn(ActorLogStore.prototype, "pruneRuns").mockImplementation(() => { deletionCalls.push("actor-log-prune"); });
  vi.spyOn(ActorChildCompletionStore.prototype, "prune").mockImplementation(() => { deletionCalls.push("child-completion-prune"); });
  vi.spyOn(AgentManager.prototype, "cleanup").mockImplementation(async id => {
    deletionCalls.push(`agent-cleanup:${id}`);
    return { cleaned: false };
  });
  vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async launch => {
    if (launch.authorize?.() === false) throw new Error("Launch not authorized");
    const arg = (key: string) => launch.workerArguments[launch.workerArguments.indexOf(`--${key}`) + 1]!;
    const pid = nextPid++;
    let nativeClose!: () => void;
    const closed = new Promise<void>(resolve => { nativeClose = resolve; });
    const record: AgentRunRecord = {
      id: launch.id, name: launch.name, task: fs.readFileSync(arg("task-file"), "utf8"), status: "running",
      runner: "pi", transport: "process", sessionId: String(pid), cwd: launch.cwd,
      startedAt: Date.now(), updatedAt: Date.now(), turns: 0, toolCalls: 0, text: "", exitCode: null,
      usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
    };
    const worker: Worker = { launch, pid, alive: true, birth: "12345", descendants: false,
      record, statusFile: arg("status-file"), nativeClose };
    workers.set(pid, worker);
    fs.writeFileSync(worker.statusFile, JSON.stringify(record));
    if (closing) complete(worker); // Failed-test teardown also drains any queued launches.
    return {
      kind: "process", sessionId: String(pid), closed,
      stopDebt: () => worker.debt, lostContact: () => worker.debt,
      waitForClose: async () => {
        if (worker.alive || worker.descendants || worker.debt) throw new Error("Native custody still open");
        await closed;
      },
      isAlive: async () => worker.alive || worker.descendants,
      stop: async () => {
        if (worker.alive || worker.descendants || worker.debt) throw new Error("Owned tree exit unconfirmed");
        await closed;
      },
    };
  });
});

afterEach(async () => {
  closing = true;
  for (const worker of workers.values()) complete(worker);
  // Keep deletion probes installed until every manager has relinquished custody.
  for (const f of [...fixtures].reverse()) {
    // A failed native join can end the baseline monitor before it settles the
    // actor. Explicit fixture stop rejoins only our already-completed fakes.
    for (const id of new Set([...workers.values()].map(worker => worker.launch.id))) {
      await f.agents.stop(id, { consume: false });
    }
    await eventually(() => expect(f.actors.inFlightCount()).toBe(0));
    await f.actors.close();
    await f.agents.close();
  }
  fixtures.length = 0;
  expect(deletionCalls.length).toBeGreaterThan(0);
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
});

function fixture(): Fixture {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-permit-watch-"));
  fixtureRoots.push(root); // Constructors can release metadata locks before fixture() returns.
  const admission = path.join(root, "actors", "admission.json");
  const agents = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents,
    enabled: true, runner: "pi", transport: "process", maxConcurrent: 1, budgetUsd: 0,
    deniedModels: [], timeoutMs: 24 * 60 * 60_000, retainRuns: true, sessionExport: false }, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
    meshRoot: path.join(root, "mesh"), admissionSnapshotPath: admission,
  });
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const identity: MeshIdentity = { id: "session:permit-test", name: "main", kind: "main", sessionId: "permit-test" };
  const createActors = () => new ActorManager("permit-test", identity, mesh,
    { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {}, {
      actorRoot: path.join(root, "actors"), persistent: true, retentionSweepMs: 20,
    });
  const f = { root, admission, agents, actors: createActors(), createActors };
  fixtures.push(f);
  return f;
}
const snapshot = (f: Fixture) => JSON.parse(fs.readFileSync(f.admission, "utf8")) as {
  held: Array<{ runId: string }>; waiting: Array<{ runId: string }>;
};
const launched = async (f: Fixture, actorId: string): Promise<Worker> => {
  let worker!: Worker;
  await eventually(() => {
    const runId = f.actors.status(actorId).inFlightRun?.id;
    worker = [...workers.values()].find(candidate => candidate.launch.id === runId)!;
    expect(worker).toBeDefined();
    expect(snapshot(f).held.map(held => held.runId)).toEqual([worker.launch.id]);
  });
  return worker;
};
const save = (worker: Worker, patch: Partial<AgentRunRecord> & Record<string, unknown>) => {
  worker.record = { ...worker.record, ...patch };
  fs.writeFileSync(worker.statusFile, JSON.stringify(worker.record));
};
function complete(worker: Worker) {
  worker.alive = false;
  worker.descendants = false;
  delete worker.debt;
  delete worker.probeError;
  worker.nativeClose();
  save(worker, { status: "completed", text: "finished", finishedAt: Date.now(), updatedAt: Date.now(), exitCode: 0 });
}
const create = (f: Fixture, name: string) => f.actors.create({ name, instructions: "Respond", responseMode: "text", coalesce: false });
async function blockedSuccessor(f: Fixture) {
  const actor = await create(f, "successor");
  const answer = f.actors.ask(actor.id, "next independent event");
  void answer.catch(() => undefined);
  await eventually(() => expect(snapshot(f).waiting).toHaveLength(1));
  expect(workers.size).toBe(1);
  return { actor, answer };
}

describe("actor worker permit release (#9031)", () => {
  it.each(["absent", "reused"] as const)("host-proven %s worker releases real admission and launches the next activation", async mode => {
    const f = fixture();
    const actor = await create(f, "permit-owner");
    await f.actors.tell(actor.id, "lost event");
    const first = await launched(f, actor.id);
    const answer = f.actors.ask(actor.id, "next activation on the same actor");
    void answer.catch(() => undefined);
    await eventually(() => expect(f.actors.status(actor.id).queued).toBe(1));
    expect(workers.size).toBe(1);
    // A worker-writable live PID cannot override the host's launch identity.
    save(first, { sessionId: String(process.pid) });
    first.alive = false;
    if (mode === "reused") { first.identityAlive = true; first.birth = "67890"; }
    first.nativeClose(); // Independently checked whole-tree/native exit, not just leader absence.
    await eventually(() => expect(f.agents.status(first.launch.id).status).toBe("failed"));
    let second!: Worker;
    await eventually(() => {
      const runId = f.actors.status(actor.id).inFlightRun?.id;
      expect(runId).not.toBe(first.launch.id);
      second = [...workers.values()].find(candidate => candidate.launch.id === runId)!;
      expect(second).toBeDefined();
      expect(snapshot(f).held.map(held => held.runId)).toEqual([second.launch.id]);
    });
    expect(workers.size).toBe(2);
    complete(second);
    await expect(answer).resolves.toMatchObject({ text: "finished" });
    await eventually(() => expect(snapshot(f).held).toEqual([]));
  });

  it.each(["running", "startup-failed", "stopped-after-progress"] as const)("lost %s activation is not automatically retried or relaunched", async mode => {
    const f = fixture();
    const actor = await create(f, "no-replay");
    const answer = f.actors.ask(actor.id, "unique lost event");
    // Observe rejection immediately, without creating an unhandled-rejection race.
    const outcome = answer.then(value => ({ value }), error => ({ error }));
    const worker = await launched(f, actor.id);
    if (mode === "stopped-after-progress") {
      save(worker, { turns: 2, toolCalls: 1, updatedAt: Date.now() });
      await eventually(() => expect(f.agents.status(worker.launch.id)).toMatchObject({ toolCalls: 1 }));
      save(worker, { status: "stopped", error: "Agent transport exited without a result" });
    } else if (mode === "startup-failed") {
      save(worker, { status: "failed", error: "Agent transport exited without a result" });
    }
    worker.alive = false;
    worker.nativeClose();
    // Plain failed semantics only: no new public error code, notice or API required.
    await eventually(() => expect(f.agents.status(worker.launch.id).status).toBe("failed"));
    await eventually(() => expect(f.actors.inFlightCount()).toBe(0));
    expect(await outcome).toHaveProperty("error");
    await observe(); // Also covers the existing startup/resume retry delay.
    expect(workers.size).toBe(1);
    expect(snapshot(f).held).toEqual([]);
    await f.actors.close();
    f.actors = f.createActors();
    await observe();
    expect(workers.size).toBe(1); // No replay by the persistent queue restore path either.
    expect(f.actors.status(actor.id).queued).toBe(0);
  });

  it("success committed during the native/tree join is not overwritten by the earlier running snapshot", async () => {
    const f = fixture();
    const launch = vi.mocked(ProcessTransport.prototype.launch).getMockImplementation()!;
    vi.mocked(ProcessTransport.prototype.launch).mockImplementation(async request => {
      const handle = await launch(request);
      const worker = [...workers.values()].find(candidate => candidate.launch.id === request.id)!;
      return { ...handle, stop: async () => { complete(worker); } };
    });
    const actor = await create(f, "close-race");
    const answer = f.actors.ask(actor.id, "finish during native join");
    const outcome = answer.then(value => ({ value }), error => ({ error }));
    const worker = await launched(f, actor.id);
    worker.alive = false;
    await eventually(() => expect(f.actors.inFlightCount()).toBe(0));
    expect(await outcome).toMatchObject({ value: { text: "finished" } });
    expect(f.agents.status(worker.launch.id).status).toBe("completed");
    expect(workers.size).toBe(1);
    await eventually(() => expect(snapshot(f).held).toEqual([]));
  });

  it.each(["live", "unknown-launch-birth", "empty-launch-birth", "unreadable-current-birth", "EPERM", "EIO"] as const)("%s worker retains its permit and blocks the next actor", async mode => {
    if (mode === "unknown-launch-birth" || mode === "empty-launch-birth") {
      const birth = vi.mocked(processIdentity.processStartTime).getMockImplementation()!;
      const unknownBirth = mode === "empty-launch-birth" ? "" : undefined;
      let captured = false;
      vi.mocked(processIdentity.processStartTime).mockImplementation(pid => {
        if (!workers.has(pid)) return birth(pid);
        if (!captured) { captured = true; return unknownBirth; }
        return "67890"; // A later readable birth cannot fill a missing launch identity.
      });
    }
    const f = fixture();
    const actor = await create(f, "retained");
    await f.actors.tell(actor.id, "still running");
    const worker = await launched(f, actor.id);
    await blockedSuccessor(f);
    if (mode === "unreadable-current-birth") delete worker.birth;
    if (mode === "EPERM" || mode === "EIO") { worker.probeError = mode; worker.birth = "67890"; }
    await observe();
    expect(f.agents.status(worker.launch.id).status).toBe("running");
    expect(snapshot(f).held.map(held => held.runId)).toEqual([worker.launch.id]);
    expect(snapshot(f).waiting).toHaveLength(1);
    expect(workers.size).toBe(1);
  });

  it.each(["sessionId", "gone", "errorCode"] as const)("worker-written %s claim cannot release a live host-owned worker's permit", async claim => {
    const f = fixture();
    const actor = await create(f, "untrusted-status");
    await f.actors.tell(actor.id, "event");
    const worker = await launched(f, actor.id);
    await blockedSuccessor(f);
    if (claim === "sessionId") save(worker, { sessionId: String(claimedGonePid), processStartTime: "99999" });
    if (claim === "gone") save(worker, { workerGone: true, settledLost: true, error: "Actor worker is proven gone" });
    // Deliberately untyped, untrusted disk data; does not require a public WORKER_GONE symbol.
    if (claim === "errorCode") fs.writeFileSync(worker.statusFile, JSON.stringify({ ...worker.record,
      status: "failed", errorCode: "WORKER_GONE", error: "Actor worker is proven gone; execution was lost" }));
    await observe();
    expect(snapshot(f).held.map(held => held.runId)).toEqual([worker.launch.id]);
    expect(snapshot(f).waiting).toHaveLength(1);
    expect(workers.size).toBe(1);
    expect(f.actors.status(actor.id).inFlightRun?.id).toBe(worker.launch.id);
  });

  it.each(["descendants", "native-debt"] as const)("gone leader with %s does not discharge whole-execution custody", async fault => {
    const f = fixture();
    const actor = await create(f, "tree-custody");
    await f.actors.tell(actor.id, "lost event");
    const worker = await launched(f, actor.id);
    await blockedSuccessor(f);
    worker.alive = false;
    if (fault === "descendants") worker.descendants = true;
    else worker.debt = "native tree close unconfirmed";
    worker.nativeClose();
    await observe();
    expect(snapshot(f).held.map(held => held.runId)).toEqual([worker.launch.id]);
    expect(snapshot(f).waiting).toHaveLength(1);
    expect(workers.size).toBe(1);
    expect(f.actors.status(actor.id).inFlightRun?.id).toBe(worker.launch.id);
  });
});
