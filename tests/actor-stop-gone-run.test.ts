import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorManager, FABRIC_ACTOR_ACTIVATION_ALARM_TOPIC, actorAlarmKey } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { writeJsonAtomic } from "../src/core/atomic-write.js";
import { MeshStore } from "../src/mesh/store.js";
import * as processIdentity from "../src/residency/process-identity.js";
import { fakeManagerHeapWorker } from "./fixtures/manager-heap-probe.js";

// Keep: pages-lead's stopped actor abb9a24aeece47a0896d13e5772bbcfa / run
// d8caf331babd44f89cf7d4026cb6917e (#1704). Real actor/agent stop and restore,
// recorded native receipts only: no worker launch, signal, cleanup or trace deletion.
vi.mock("node:child_process", async importOriginal => ({
  ...await importOriginal<typeof import("node:child_process")>(),
  spawn: vi.fn(() => { throw new Error("Unexpected launch in stopped-actor recorded probe"); }),
}));

const deferred = () => {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
};
const closers: Array<() => Promise<void>> = [];
const releases: Array<() => void> = [];
afterEach(async () => {
  for (const release of releases.splice(0)) release();
  try { for (const close of closers.splice(0).reverse()) await close(); }
  finally { vi.restoreAllMocks(); }
  // Keep all fixture records and traces in the test owner's private TMPDIR.
});
const eventually = (check: () => void) => vi.waitFor(check, { timeout: 3_000, interval: 10 });

function fixture() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-stop-gone-"));
  const actorRoot = path.join(root, "actors");
  const runRoot = path.join(root, "runs");
  const identity = { id: "session:stopped-recorded", name: "main", kind: "main" as const, sessionId: "stopped-recorded" };
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const agents = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents,
    budgetUsd: 0, retainRuns: true, sessionExport: false,
  }, { runRoot, workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fullCodeMode: false });
  closers.push(() => agents.close());
  const delivered = vi.fn();
  const manager = () => {
    const actors = new ActorManager("stopped-recorded", identity, mesh,
      { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, delivered, {
        actorRoot, persistent: true, retentionSweepMs: 20, closeGraceMs: 10,
      });
    closers.push(() => actors.close());
    return actors;
  };
  const alarms = (id: string) => mesh.read({ topic: FABRIC_ACTOR_ACTIVATION_ALARM_TOPIC, limit: 100 })
    .filter(event => event.kind === "actor-run-stuck" && (event.data as { actorId?: string }).actorId === id);
  return { root, actorRoot, runRoot, mesh, agents, manager, alarms, delivered };
}

async function heldWorker() {
  const pid = 2147483646;
  let gone = false;
  const tree = deferred(), stopEntered = deferred();
  releases.push(() => { gone = true; tree.resolve(); });
  const kill = process.kill.bind(process);
  vi.spyOn(process, "kill").mockImplementation((candidate, signal) => {
    if (candidate !== pid) return kill(candidate, signal);
    if (signal !== 0) throw new Error("Recorded worker must never be signalled");
    if (gone) throw Object.assign(new Error("recorded worker gone"), { code: "ESRCH" });
    return true;
  });
  const birth = processIdentity.processStartTime;
  vi.spyOn(processIdentity, "processStartTime").mockImplementation(candidate =>
    candidate === pid ? (gone ? undefined : "12345") : birth(candidate));
  vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
    const handle = await fakeManagerHeapWorker(request);
    const arg = (name: string) => request.workerArguments[request.workerArguments.indexOf(name) + 1]!;
    const statusFile = arg("--status-file");
    writeJsonAtomic(statusFile, { ...JSON.parse(fs.readFileSync(statusFile, "utf8")),
      status: "running", finishedAt: undefined, actorId: arg("--actor-id"), sessionId: String(pid),
      turns: 1, toolCalls: 1 });
    fs.writeFileSync(path.join(path.dirname(statusFile), "events.jsonl"), '{"type":"recorded-worker-trace","text":"keep exactly"}\n');
    return { ...handle, sessionId: String(pid), closed: tree.promise, treeClosed: tree.promise,
      waitForClose: () => tree.promise, isAlive: async () => !gone,
      stop: async () => { stopEntered.resolve(); await tree.promise; } };
  });
  return { stopEntered, losePrimary: () => { gone = true; }, finishTree: () => { gone = true; tree.resolve(); } };
}

function savedTraces(directory: string, sessionFile: string) {
  fs.mkdirSync(path.dirname(sessionFile), { recursive: true });
  fs.writeFileSync(sessionFile, '{"type":"session","id":"recorded-stopped-session"}\n');
  const files = [path.join(directory, "events.jsonl"), sessionFile];
  const before = files.map(file => fs.readFileSync(file));
  return () => files.forEach((file, index) => expect(fs.readFileSync(file)).toEqual(before[index]));
}

describe("stopped actor run settlement (#1704)", () => {
  it("default stop starts the real gone-worker stop, keeps native custody until tree exit, and preserves traces", async () => {
    const worker = await heldWorker();
    const f = fixture();
    const actors = f.manager();
    const actor = await actors.create({ name: "stop-gone", instructions: "Work", extensions: false });
    await actors.tell(actor.id, "recorded activation");
    await eventually(() => expect(actors.status(actor.id).inFlightRun).toBeDefined());
    const runId = actors.status(actor.id).inFlightRun!.id;
    const directory = f.agents.runDirectory(runId)!;
    const checkTraces = savedTraces(directory, actor.sessionFile!);
    worker.losePrimary();

    // No wait:true: this is the ordinary agents.stop actor API, not the removal join.
    expect(await actors.stop(actor.id)).toMatchObject({ status: "stopped" });
    await eventually(() => expect(ProcessTransport.prototype.launch).toHaveBeenCalledOnce());
    await eventually(() => expect(f.agents.isSettled(runId)).toBe(false));
    let entered = false;
    void worker.stopEntered.promise.then(() => { entered = true; });
    await eventually(() => expect(entered).toBe(true));
    expect(actors.status(actor.id).inFlightRun?.id).toBe(runId);
    expect(fs.existsSync(path.join(directory, "manager-result.json"))).toBe(false);
    checkTraces();
    const now = Date.now.bind(Date);
    const clock = vi.spyOn(Date, "now").mockImplementation(() => now() + 31 * 60_000);
    await new Promise(resolve => setTimeout(resolve, 150)); // Several existing sweeps beyond the 30-minute stuck threshold.
    expect(actors.status(actor.id).inFlightRun?.id).toBe(runId); // Still held: an absent pointer cannot mask the alarm bug.
    expect(f.alarms(actor.id)).toEqual([]);
    expect(f.agents.isSettled(runId)).toBe(false);
    // The alarm clock is not elapsed native teardown time. Restore it before
    // resolving the exact tree join so its pre-existing deadline stays valid.
    clock.mockRestore();

    worker.finishTree();
    await eventually(() => expect(f.agents.status(runId)).toMatchObject({ status: "stopped", error: "Agent stopped" }));
    await eventually(() => expect(actors.status(actor.id).inFlightRun).toBeUndefined());
    expect(JSON.parse(fs.readFileSync(path.join(directory, "status.json"), "utf8")))
      .toMatchObject({ id: runId, status: "stopped" });
    expect(f.alarms(actor.id)).toEqual([]);
    checkTraces();
  });

  it("a stopped actor cancels a delayed stuck-run claim and never claims another while native custody is held", async () => {
    const worker = await heldWorker();
    const f = fixture();
    const actors = f.manager();
    const actor = await actors.create({ name: "stop-alarm-race", instructions: "Work", extensions: false });
    const now = Date.now.bind(Date);
    let skew = -31 * 60_000;
    vi.spyOn(Date, "now").mockImplementation(() => now() + skew);
    await actors.tell(actor.id, "recorded activation");
    await eventually(() => expect(actors.status(actor.id).inFlightRun).toBeDefined());
    const runId = actors.status(actor.id).inFlightRun!.id;
    const checkTraces = savedTraces(f.agents.runDirectory(runId)!, actor.sessionFile!);
    const alarmKey = actorAlarmKey(actor.id, "actor-run-stuck");
    const claimGate = deferred();
    releases.push(claimGate.resolve);
    const writeBatch = f.mesh.writeBatch.bind(f.mesh);
    let claims = 0;
    vi.spyOn(f.mesh, "writeBatch").mockImplementation(async request => {
      const result = await writeBatch(request);
      if (request.prepare && request.ops.length === 0 && result.some(entry => entry.key === alarmKey && entry.applied)) {
        claims++;
        await claimGate.promise;
      }
      return result;
    });
    worker.losePrimary();
    skew = 0;
    await eventually(() => expect(claims).toBe(1)); // Non-stopped gone workers still alarm.
    await actors.stop(actor.id);
    expect(actors.status(actor.id).inFlightRun?.id).toBe(runId); // Native/tree join still owns this activation.
    claimGate.resolve();
    await new Promise(resolve => setTimeout(resolve, 150)); // Several existing retention sweeps, no new timer in production.
    expect(f.alarms(actor.id)).toEqual([]); // Async claim guard rechecks STOPPED before publish/delivery.
    expect(claims).toBe(1); // Synchronous alarm eligibility also skips this stopped actor.
    worker.finishTree();
    await eventually(() => expect(f.agents.status(runId)).toMatchObject({ status: "stopped" }));
    checkTraces();
  });

  it("does not deliver a repeat stuck-run alarm whose publish returns after stop", async () => {
    const worker = await heldWorker();
    const f = fixture();
    const actors = f.manager();
    const actor = await actors.create({ name: "stop-delivery-race", instructions: "Work", extensions: false });
    const now = Date.now.bind(Date);
    let skew = -(6 * 60 + 31) * 60_000;
    vi.spyOn(Date, "now").mockImplementation(() => now() + skew);
    await actors.tell(actor.id, "recorded activation");
    await eventually(() => expect(actors.status(actor.id).inFlightRun).toBeDefined());
    const runId = actors.status(actor.id).inFlightRun!.id;
    const publishGate = deferred();
    releases.push(publishGate.resolve);
    const publish = f.mesh.publish.bind(f.mesh);
    let published = false;
    vi.spyOn(f.mesh, "publish").mockImplementation(async request => {
      const event = await publish(request);
      if (request.kind === "actor-run-stuck") {
        published = true;
        await publishGate.promise;
      }
      return event;
    });
    worker.losePrimary();
    skew = 0; // Slot 1: unlike slot 0, a repeat alarm would trigger Main delivery.
    await eventually(() => expect(published).toBe(true));
    await actors.stop(actor.id);
    expect(actors.status(actor.id).inFlightRun?.id).toBe(runId);
    publishGate.resolve();
    await new Promise(resolve => setTimeout(resolve, 150));
    expect(f.alarms(actor.id)).toHaveLength(1); // Already-published evidence is retained, never withdrawn.
    expect(f.delivered.mock.calls.filter(([delivery]) => delivery.message.text?.includes("actor-run-stuck"))).toEqual([]);
    worker.finishTree();
    await eventually(() => expect(f.agents.status(runId)).toMatchObject({ status: "stopped" }));
  });

  it("bootstrap settles an old stopped registry row while the host still owns its run handle", async () => {
    const worker = await heldWorker();
    const f = fixture();
    const first = f.manager();
    const actor = await first.create({ name: "old-stopped", instructions: "Work", extensions: false });
    await first.close();
    const handle = await f.agents.spawn({ task: "recorded old activation", actorId: actor.id,
      sessionFile: actor.sessionFile!, transport: "process", extensions: false });
    const directory = f.agents.runDirectory(handle.id)!;
    const checkTraces = savedTraces(directory, actor.sessionFile!);
    const registryFile = path.join(f.actorRoot, "actors.json");
    const registry = JSON.parse(fs.readFileSync(registryFile, "utf8"));
    Object.assign(registry.actors.find((row: { id: string }) => row.id === actor.id), {
      status: "stopped", stopReason: "explicit", stoppedAt: Date.now(), lastRunId: handle.id,
      inFlightRun: { id: handle.id, startedAt: Date.now() - 31 * 60_000 },
    });
    writeJsonAtomic(registryFile, registry);
    worker.losePrimary();
    f.agents.abandon(handle.id); // Legacy default stop detached/abandoned this run before the reload.
    const restored = f.manager();
    expect(restored.status(actor.id).status).toBe("stopped");
    let entered = false;
    void worker.stopEntered.promise.then(() => { entered = true; });
    await eventually(() => expect(entered).toBe(true));
    expect(f.agents.isSettled(handle.id)).toBe(false);
    expect(f.alarms(actor.id)).toEqual([]);
    worker.finishTree();
    await eventually(() => expect(f.agents.status(handle.id)).toMatchObject({ status: "stopped" }));
    expect(restored.status(actor.id).inFlightRun).toBeUndefined();
    expect(restored.status(actor.id).status).toBe("stopped");
    checkTraces();
  });
});
