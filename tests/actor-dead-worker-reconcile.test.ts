import fs from "node:fs";
import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { build } from "esbuild";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorManager, FABRIC_ACTOR_ACTIVATION_ALARM_TOPIC, actorAlarmKey } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import type { AgentTransportHandle } from "../src/agents/types.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";

// Keep: #17191's real dead-worker/blocked-monitor failure and the native session
// data-safety boundary. Real subprocesses and transport custody, not status mocks.
const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
  vi.restoreAllMocks();
}, 30_000);
const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
const until = async (check: () => boolean, label: string, ms = 5_000) => {
  const end = performance.now() + ms;
  while (!check()) {
    if (performance.now() > end) throw new Error(`Timed out: ${label}`);
    await sleep(20);
  }
};

async function setup({ blockMonitor = true, retentionSweepMs }: { blockMonitor?: boolean; retentionSweepMs?: number } = {}) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-dead-reconcile-"));
  const realNow = Date.now.bind(Date);
  let skew = 0;
  let fixedNow: number | undefined;
  vi.spyOn(Date, "now").mockImplementation(() => fixedNow ?? realNow() + skew);
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const identity: MeshIdentity = { id: "session:reconcile", name: "main", kind: "main", sessionId: "reconcile" };
  const agents = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: true }, {
    runRoot: path.join(root, "runs"), workerPath: path.resolve("tests/fixtures/actor-reconcile-worker.mjs"),
  });
  const notices: string[] = [];
  const triggeredNotices: string[] = [];
  // By default recovery must use the existing five-second mesh maintenance/event
  // wake, not a fast retention override or the fifteen-minute archive sweep.
  const actors = new ActorManager("reconcile", identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents,
    ({ message, triggerTurn }) => {
      if (message.text) {
        notices.push(message.text);
        if (triggerTurn) triggeredNotices.push(message.text);
      }
    }, { actorRoot: path.join(root, "actors"), persistent: true, meshRetentionSweepPath: false,
      ...(retentionSweepMs !== undefined ? { retentionSweepMs } : {}) });
  const launch = ProcessTransport.prototype.launch;
  const handles: AgentTransportHandle[] = [];
  let held = false;
  let resume!: () => void;
  let resumeStop = () => {};
  const gate = new Promise<void>(resolve => { resume = resolve; });
  vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function (this: ProcessTransport, request) {
    const native = await launch.call(this, request);
    const handle = blockMonitor && handles.length === 0 ? { ...native, isAlive: async () => {
      // Hold exactly one monitor continuation. Fresh reconcile probes still use
      // the original checked native/tree handle, and teardown can join it.
      if (!held) { held = true; await gate; }
      return native.isAlive();
    } } : native;
    handles.push(handle);
    return handle;
  });
  cleanups.push(async () => {
    fs.writeFileSync(path.join(root, "release-descendant"), "release");
    resume();
    resumeStop();
    // Restore real clock before close: a skewed monitor deadline must not hide
    // an owned child, and stop joins the exact native transport.
    skew = 0;
    fixedNow = undefined;
    for (const handle of handles) await handle.stop();
    await actors.close();
    await agents.close();
    fs.rmSync(root, { recursive: true, force: true });
  });
  const actor = await actors.create({ name: "serial", instructions: "Respond.", responseMode: "text", topics: ["github.demo"], coalesce: false });
  const pulse = async () => { await mesh.publish({ topic: "reconcile.pulse", from: identity }); await sleep(80); };
  const start = async (descendant = false, progress = true) => {
    actors.tell(actor.id, descendant ? "retained writer" : progress ? "HANG_WITH_PROGRESS" : "HANG", descendant
      ? { reconcileDescendantRoot: root } : { replayId: "dead-run-must-not-replay" });
    await until(() => !!actors.status(actor.id).inFlightRun, "worker launch");
    const runId = actors.status(actor.id).inFlightRun!.id;
    const statusFile = path.join(agents.runDirectory(runId)!, "status.json");
    await until(() => fs.existsSync(statusFile) && held, "running status and blocked native observation");
    if (descendant) await until(() => fs.existsSync(path.join(root, "descendant-ready")), "descendant ready");
    actors.tell(actor.id, "NEXT queued item");
    expect(actors.status(actor.id).queued).toBe(1);
    return { runId, statusFile, worker: agents.workerIdentity(runId)! };
  };
  const atBound = (age = 31_000) => { fixedNow = undefined; skew = age; };
  const atExitAge = (age: number) => {
    const exit = handles[0]!.nativeExit?.();
    expect(exit).toBeDefined();
    fixedNow = exit!.exitedAt + age;
  };
  const stuckAlarms = () => mesh.read({ topic: FABRIC_ACTOR_ACTIVATION_ALARM_TOPIC, limit: 200 })
    .filter(event => event.kind === "actor-run-stuck" && (event.data as { actorId?: string }).actorId === actor.id);
  const blockStop = () => {
    const handle = handles[0]!;
    const stop = handle.stop.bind(handle);
    const gate = new Promise<void>(resolve => { resumeStop = resolve; });
    const calls = vi.fn(async () => { await gate; await stop(); });
    handle.stop = calls;
    return { calls, release: () => resumeStop() };
  };
  return { root, mesh, agents, actors, actor, handles, notices, triggeredNotices, start, pulse, atBound, atExitAge, stuckAlarms, resume, blockStop };
}

// Drive the real retention callback without waiting fifteen minutes or racing
// repeated sweeps. This invokes #startRetentionSweep/#sweepRetainedRuns, not the
// receiver's short fallback, and keeps the existing production timer unchanged.
async function legacySweepSetup() {
  let sweep!: () => void;
  const interval = globalThis.setInterval;
  vi.spyOn(globalThis, "setInterval").mockImplementation((callback, delay, ...args) => {
    if (delay === 60_001) sweep = () => callback(...args);
    return interval(callback, delay, ...args);
  });
  const s = await setup({ retentionSweepMs: 60_001 });
  expect(sweep).toBeTypeOf("function");
  return { ...s, sweep: async () => { sweep(); await sleep(120); } };
}

async function crashFixture() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-loss-crash-"));
  cleanups.push(async () => { fs.rmSync(root, { recursive: true, force: true }); });
  const bundle = path.join(root, "owner-bundle.mjs");
  await build({ stdin: { contents: `
    export { ActorManager } from "./src/actors/manager.ts";
    export { AgentManager } from "./src/agents/manager.ts";
    export { ProcessTransport } from "./src/agents/transports/process-transport.ts";
    export { DEFAULT_FABRIC_CONFIG } from "./src/config.ts";
    export { MeshStore } from "./src/mesh/store.ts";
  `, resolveDir: process.cwd(), loader: "ts" }, outfile: bundle, bundle: true, platform: "node", format: "esm", packages: "external" });
  fs.symlinkSync(path.resolve("node_modules"), path.join(root, "node_modules"), process.platform === "win32" ? "junction" : "dir");
  const owner = async (mode: string) => {
    const child = spawn(process.execPath, [path.resolve("tests/fixtures/actor-reconcile-worker.mjs"), "--owner", bundle, root, mode],
      { stdio: ["ignore", "pipe", "pipe"] });
    let output = "";
    child.stdout.on("data", chunk => { output += chunk; });
    child.stderr.on("data", chunk => { output += chunk; });
    const closed = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolve, reject) => {
      child.once("error", reject);
      child.once("close", (code, signal) => resolve({ code, signal }));
    });
    cleanups.push(async () => {
      if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
      await closed;
    });
    const timeout = setTimeout(() => child.kill("SIGKILL"), 20_000);
    try { return { ...await closed, output }; }
    finally { clearTimeout(timeout); }
  };
  const journal = (): Array<{ id: string; task: string }> => fs.readFileSync(path.join(root, "worker-launches.jsonl"), "utf8")
    .trim().split("\n").map(line => JSON.parse(line));
  return { root, owner, journal };
}

const expectKilled = (result: { code: number | null; signal: NodeJS.Signals | null; output: string }) => {
  if (process.platform === "win32") expect(result.code, result.output).not.toBe(0);
  else expect(result.signal, result.output).toBe("SIGKILL");
};

// Keep: #1695's model-facing diagnostic leak, reached through real interrupted
// process workers. These fixture-only paths stay local-safe (no worktree cleanup).
describe("native exit diagnostics stay out of model-facing activity (#1695)", () => {
  const managerFixture = (worker = "fake-worker.mjs") => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "native-exit-summary-"));
    const agents = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: true }, {
      runRoot: root, workerPath: path.resolve("tests/fixtures", worker),
    });
    cleanups.push(async () => { await agents.close(); fs.rmSync(root, { recursive: true, force: true }); });
    return { root, agents };
  };
  const usefulActivity = [
    { type: "worker.progress", message: "useful worker progress" },
    { type: "message_end", text: "useful model answer" },
    { type: "tool_execution_end", toolName: "read" },
    { type: "run.custom", message: "useful unknown activity" },
    { type: "run.native_exit.extra", message: "not the exact diagnostic type" },
  ];
  const appendActivityOnExit = (root: string, useful: boolean) => {
    const launch = ProcessTransport.prototype.launch;
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function (this: ProcessTransport, request) {
      const native = await launch.call(this, request);
      if (!useful) return native; // diagnostic-only case uses ONLY production's raw host diagnostics
      // Registered before AgentManager's host-diagnostic observer and monitor.
      // Copies of this exact host observation exercise a diagnostic-heavy tail;
      // filtering after the six/eight-item limit would lose the useful events.
      void native.primaryExit?.then(() => {
        const exit = native.nativeExit?.();
        if (!exit) return;
        const diagnostic = { type: "run.native_exit", id: request.id, ...exit };
        fs.appendFileSync(path.join(root, request.id, "events.jsonl"), [
          ...(useful ? usefulActivity : []),
          ...Array.from({ length: 12 }, () => diagnostic),
        ].map(event => JSON.stringify(event)).join("\n") + "\n");
      });
      return native;
    });
  };

  it.each([false, true])("resumes an actual stopped worker without native diagnostics in its next task (useful activity %s)", async useful => {
    const { root, agents } = managerFixture();
    appendActivityOnExit(root, useful);
    const result = await agents.run({ task: "RESUME_AFTER_STOP", transport: "process" });
    const raw = fs.readFileSync(path.join(agents.runDirectory(result.id)!, "events.jsonl"), "utf8");
    expect(result.status).toBe("completed");
    expect(result.task).toContain("[Fabric continuation]");
    expect(result.task).not.toMatch(/(?:^|[ |])run\.native_exit(?:[ |:]|$)/);
    expect(raw).toContain('"type":"run.native_exit"');
    if (useful) {
      expect(result.task).toContain("worker.progress: useful worker progress");
      expect(result.task).toContain("message_end: useful model answer");
      expect(result.task).toContain("tool_execution_end: read");
      expect(result.task).toContain("run.custom: useful unknown activity");
      expect(result.task).toContain("run.native_exit.extra: not the exact diagnostic type");
      for (const event of usefulActivity) expect(raw).toContain(JSON.stringify(event));
    } else {
      // The manager's ordinary unknown event remains useful too; only the
      // diagnostic-only tail is omitted, not all model-facing run activity.
      expect(raw).toContain('"type":"write_policy.default"');
      expect(result.task).toContain("write_policy.default");
    }
  }, 30_000);

  it("keeps useful transport-error activity while excluding only the exact native diagnostic", async () => {
    const { root, agents } = managerFixture("fake-worker-transport-death.mjs");
    appendActivityOnExit(root, true);
    const result = await agents.run({ task: "Terminal boot death", transport: "process" });
    expect(result.status).toBe("failed");
    expect(result.error).toContain("Agent transport exited without a result");
    expect(result.error).not.toMatch(/(?:^|[ |])run\.native_exit(?:[ |:]|$)/);
    expect(result.error).toContain("worker.progress: useful worker progress");
    expect(result.error).toContain("message_end: useful model answer");
    expect(result.error).toContain("tool_execution_end: read");
    expect(result.error).toContain("run.custom: useful unknown activity");
    expect(result.error).toContain("run.native_exit.extra: not the exact diagnostic type");
    const raw = fs.readFileSync(path.join(agents.runDirectory(result.id)!, "events.jsonl"), "utf8");
    expect(raw).toContain('"type":"run.native_exit"');
    for (const event of usefulActivity) expect(raw).toContain(JSON.stringify(event));
  }, 30_000);
});

describe("bounded dead actor worker reconciliation (#17191)", () => {
  it.each([undefined, 0, 5_000])("legacy sweep does not wake Main after genuine same-ID completion held behind its alarm claim (exit age %s)", async age => {
    const s = await legacySweepSetup();
    const run = s.agents.run.bind(s.agents);
    let releaseRun!: () => void;
    const runGate = new Promise<void>(resolve => { releaseRun = resolve; });
    let completed: Awaited<ReturnType<typeof run>> | undefined;
    vi.spyOn(s.agents, "run").mockImplementation(async (...args) => {
      const result = await run(...args);
      if (!completed) { completed = result; await runGate; }
      return result;
    });
    let releaseClaim!: () => void;
    const claimGate = new Promise<void>(resolve => { releaseClaim = resolve; });
    cleanups.push(async () => { releaseRun(); releaseClaim(); });
    const key = actorAlarmKey(s.actor.id, "actor-run-stuck");
    const writeBatch = s.mesh.writeBatch.bind(s.mesh);
    let claimed = false;
    vi.spyOn(s.mesh, "writeBatch").mockImplementation(async request => {
      const results = await writeBatch(request);
      if (results.some(result => result.key === key && result.applied)) { claimed = true; await claimGate; }
      return results;
    });
    s.actors.tell(s.actor.id, "STREAM_PREVIEW healthy completion");
    await until(() => !!s.actors.status(s.actor.id).inFlightRun, "healthy streaming launch");
    const runId = s.actors.status(s.actor.id).inFlightRun!.id;
    const statusFile = path.join(s.agents.runDirectory(runId)!, "status.json");
    await until(() => fs.existsSync(statusFile), "healthy streaming running record");
    const read = fs.readFileSync;
    const running = read(statusFile, "utf8");
    expect(JSON.parse(running).status).toBe("running");
    // The actual worker writes a genuine success. Hold its visibility to the
    // host until the sweep/claim, then let the normal manager settle that result.
    let visible = false;
    vi.spyOn(fs, "readFileSync").mockImplementation((file, options) => {
      if (String(file) === statusFile && !visible) return running;
      return read(file, options);
    });
    s.actors.tell(s.actor.id, "NEXT queued item");
    s.atBound(31 * 60_000);
    await s.handles[0]!.closed;
    s.atExitAge(age ?? 0);
    if (age === undefined) s.handles[0]!.nativeExit = () => undefined;
    const observe = vi.spyOn(s.agents, "actorWorkerExit");
    await s.sweep();
    expect(observe).toHaveBeenCalledWith(runId, s.actor.id); // actual sweep evaluated the episode
    visible = true;
    expect(JSON.parse(read(statusFile, "utf8")).status).toBe("completed");
    s.resume();
    await until(() => !!completed, "real success settled, original agents.run still held");
    expect(completed).toMatchObject({ id: runId, status: "completed" });
    expect(s.actors.status(s.actor.id)).toMatchObject({ status: "running", queued: 1, inFlightRun: { id: runId } });
    expect(s.handles).toHaveLength(1);
    releaseClaim();
    await sleep(150);
    expect.soft(s.stuckAlarms()).toHaveLength(0);
    expect.soft(s.triggeredNotices).toHaveLength(0);
    expect.soft(claimed).toBe(false);
    expect(s.agents.actorRunLost(runId)).toBe(false);
    releaseRun();
    await until(() => s.handles.length === 2 && s.actors.status(s.actor.id).status === "idle", "one NEXT after healthy held return");
    expect(s.handles).toHaveLength(2);
  }, 20_000);

  it("legacy sweep requires exact native exit ages 0/5000/5001 and preserves the unfinished joined-run alarm", async () => {
    const s = await legacySweepSetup();
    const first = await s.start();
    const stop = s.blockStop();
    s.atBound(31 * 60_000);
    process.kill(first.worker.pid, "SIGKILL");
    await s.handles[0]!.closed;
    for (const age of [0, 5_000]) {
      s.atExitAge(age); await s.sweep();
      expect.soft(s.stuckAlarms()).toHaveLength(0);
      expect.soft(s.triggeredNotices).toHaveLength(0);
      expect(stop.calls).not.toHaveBeenCalled();
      expect(s.agents.actorRunLost(first.runId)).toBe(false);
      expect(s.actors.status(s.actor.id)).toMatchObject({ queued: 1, inFlightRun: { id: first.runId } });
      expect(s.handles).toHaveLength(1);
    }
    s.atExitAge(5_001); await s.sweep();
    // A durable event is readable before publish resolves and onDeliver runs.
    // Observe the Main boundary too, without releasing the native join.
    await until(() => stop.calls.mock.calls.length === 1 && s.stuckAlarms().length === 1 &&
      s.triggeredNotices.length === 1, "eligible unfinished alarm delivered while join held");
    expect(s.triggeredNotices).toHaveLength(1);
    expect(s.agents.actorRunLost(first.runId)).toBe(false);
    expect(JSON.parse(fs.readFileSync(first.statusFile, "utf8")).status).toBe("running");
    expect(s.handles).toHaveLength(1);
    stop.release();
    await until(() => s.handles.length === 2 && s.actors.status(s.actor.id).status === "idle", "NEXT only after exact join");
    expect(JSON.parse(fs.readFileSync(first.statusFile, "utf8"))).toMatchObject({ status: "failed", error: expect.stringContaining("proven gone") });
  }, 20_000);

  it.each(["current", "completed", "published-completed", "missing-exit", "new-exit", "young-exit", "cleared", "owner-closed"] as const)("legacy sweep rechecks its asynchronous repeat claim through Main delivery (%s)", async change => {
    const s = await legacySweepSetup();
    const run = s.agents.run.bind(s.agents);
    let releaseRun!: () => void, releaseRecovery!: () => void, releaseAlarm!: () => void;
    const runGate = new Promise<void>(resolve => { releaseRun = resolve; });
    const recoveryGate = new Promise<void>(resolve => { releaseRecovery = resolve; });
    const alarmGate = new Promise<void>(resolve => { releaseAlarm = resolve; });
    let completed: Awaited<ReturnType<typeof run>> | undefined;
    cleanups.push(async () => { releaseRun(); releaseRecovery(); releaseAlarm(); });
    vi.spyOn(s.agents, "run").mockImplementation(async (...args) => {
      const result = await run(...args);
      if (!completed) { completed = result; await runGate; }
      return result;
    });
    const reconcile = s.agents.reconcileActorRun.bind(s.agents);
    vi.spyOn(s.agents, "reconcileActorRun").mockImplementation(async (...args) => {
      const result = await reconcile(...args);
      await recoveryGate; // manager may settle, but both actor continuations remain held
      return result;
    });
    s.actors.tell(s.actor.id, "STREAM_PREVIEW healthy completion");
    await until(() => !!s.actors.status(s.actor.id).inFlightRun, "repeat worker launch");
    const episode = s.actors.status(s.actor.id).inFlightRun!;
    const statusFile = path.join(s.agents.runDirectory(episode.id)!, "status.json");
    await until(() => fs.existsSync(statusFile), "stream running record");
    const read = fs.readFileSync;
    const running = read(statusFile, "utf8");
    expect(JSON.parse(running).status).toBe("running");
    let visible = false;
    vi.spyOn(fs, "readFileSync").mockImplementation((file, options) => {
      if (String(file) === statusFile && !visible) return running;
      return read(file, options);
    });
    s.actors.tell(s.actor.id, "NEXT queued item");
    s.atBound(31 * 60_000);
    await s.handles[0]!.closed;
    const stop = s.blockStop();
    s.atExitAge(5_001); await s.sweep();
    await until(() => s.stuckAlarms().length === 1 && stop.calls.mock.calls.length === 1 &&
      s.triggeredNotices.length === 1, "first legitimate alarm delivered and pending join");
    expect(s.triggeredNotices).toHaveLength(1);
    expect(s.agents.actorRunLost(episode.id)).toBe(false);
    // Short recovery is now pending. Only the real legacy retention branch can
    // claim slot 1 six hours later; assert its own threshold/slot below.
    const key = actorAlarmKey(s.actor.id, "actor-run-stuck");
    const writeBatch = s.mesh.writeBatch.bind(s.mesh);
    const publish = s.mesh.publish.bind(s.mesh);
    let held = false;
    vi.spyOn(s.mesh, "writeBatch").mockImplementation(async request => {
      const results = await writeBatch(request);
      if (change !== "published-completed" && results.some(result => result.key === key && result.applied)) {
        held = true; await alarmGate;
      }
      return results;
    });
    vi.spyOn(s.mesh, "publish").mockImplementation(async request => {
      const event = await publish(request);
      if (change === "published-completed" && request.kind === "actor-run-stuck") { held = true; await alarmGate; }
      return event;
    });
    s.atExitAge(6 * 60 * 60_000 + 5_001); await s.sweep();
    await until(() => held, "actual legacy slot-1 claim/publication held");
    expect(s.mesh.get(key)?.value).toMatchObject({ slot: 1, state: "pending", since: episode.startedAt });
    const originalExit = s.handles[0]!.nativeExit!();
    let closing: Promise<void> | undefined;
    if (change === "completed" || change === "published-completed" || change === "cleared") {
      visible = true;
      expect(JSON.parse(read(statusFile, "utf8")).status).toBe("completed");
      stop.release(); s.resume();
      await until(() => !!completed, "genuine manager success with held actor return");
      expect(completed).toMatchObject({ id: episode.id, status: "completed" });
      expect(s.actors.status(s.actor.id).inFlightRun).toMatchObject({ id: episode.id });
      if (change === "cleared") {
        const snapshot = { exitedAt: originalExit!.exitedAt, unfinished: true };
        vi.spyOn(s.agents, "actorWorkerExit").mockReturnValue(snapshot);
        releaseRun(); releaseRecovery();
        await until(() => s.handles.length === 2 && s.actors.status(s.actor.id).status === "idle", "old episode cleared before claim returns");
      }
    } else if (change === "missing-exit") s.handles[0]!.nativeExit = () => undefined;
    else if (change === "new-exit") s.handles[0]!.nativeExit = () => ({ ...originalExit!, exitedAt: originalExit!.exitedAt + 1 });
    else if (change === "young-exit") s.handles[0]!.nativeExit = () => ({ ...originalExit!, exitedAt: Date.now() - 5_000 });
    else if (change === "owner-closed") { closing = s.actors.close(); await sleep(20); }
    releaseAlarm(); await sleep(150);
    expect(s.stuckAlarms()).toHaveLength(change === "current" || change === "published-completed" ? 2 : 1);
    expect(s.triggeredNotices).toHaveLength(change === "current" ? 2 : 1);
    if (change === "current") {
      expect(s.stuckAlarms()[1]).toMatchObject({ data: { runId: episode.id, thresholdMs: 30 * 60_000 } });
      expect(s.actors.status(s.actor.id)).toMatchObject({ queued: 1, inFlightRun: { id: episode.id } });
      expect(s.handles).toHaveLength(1); // repeat may alarm, but no loss/NEXT before native join
      expect(s.agents.actorRunLost(episode.id)).toBe(false);
      expect(JSON.parse(fs.readFileSync(statusFile, "utf8")).status).toBe("running");
    }
    s.handles[0]!.nativeExit = () => originalExit;
    visible = true; stop.release(); s.resume(); releaseRun(); releaseRecovery();
    if (closing) await closing;
    else await until(() => s.handles.length === 2 && s.actors.status(s.actor.id).status === "idle", "one NEXT after joined healthy completion");
  }, 20_000);

  it.each([31_000, 31 * 60_000])("reconciles a healthy completed worker's held continuation without a stuck alarm or Main trigger (age %s)", async age => {
    const s = await setup({ blockMonitor: false, retentionSweepMs: 20 });
    const run = s.agents.run.bind(s.agents);
    let release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    cleanups.push(async () => { release(); });
    let completed: Awaited<ReturnType<typeof run>> | undefined;
    vi.spyOn(s.agents, "run").mockImplementation(async (...args) => {
      const result = await run(...args);
      if (!completed) { completed = result; await gate; }
      return result;
    });
    s.actors.tell(s.actor.id, "normal healthy completion");
    await until(() => !!completed, "healthy manager settlement");
    const first = completed!;
    await s.handles[0]!.closed;
    expect(first.status).toBe("completed");
    expect(s.actors.status(s.actor.id).inFlightRun?.id).toBe(first.id);
    s.actors.tell(s.actor.id, "NEXT queued item");
    s.atBound(age);
    await s.pulse();
    await until(() => s.handles.length === 2 && s.actors.status(s.actor.id).status === "idle", "terminal continuation reconciled");
    expect(s.stuckAlarms()).toHaveLength(0);
    expect(s.triggeredNotices).toHaveLength(0);
    expect(s.agents.actorRunLost(first.id)).toBe(false);
    release();
    await s.pulse();
    expect(s.handles).toHaveLength(2);
    expect(s.stuckAlarms()).toHaveLength(0);
  }, 20_000);

  it("retains the existing run-runtime threshold even after a native exit is over five seconds old", async () => {
    const s = await setup();
    const first = await s.start();
    process.kill(first.worker.pid, "SIGKILL");
    await s.handles[0]!.closed;
    s.atExitAge(5_001); await s.pulse();
    expect(Date.now() - s.actors.status(s.actor.id).inFlightRun!.startedAt).toBeLessThan(15_000);
    expect(s.handles).toHaveLength(1);
    expect(s.stuckAlarms()).toHaveLength(0);
    expect(s.agents.actorRunLost(first.runId)).toBe(false);
  });

  it("does not substitute a diagnostic or status timestamp for an absent exact native exit observation", async () => {
    const s = await setup();
    const first = await s.start();
    process.kill(first.worker.pid, "SIGKILL");
    await s.handles[0]!.closed;
    s.handles[0]!.nativeExit = () => undefined;
    fs.appendFileSync(path.join(path.dirname(first.statusFile), "events.jsonl"), JSON.stringify({
      type: "run.native_exit", exitedAt: 1, pid: first.worker.pid, host: "foreign-host",
    }) + "\n");
    const record = JSON.parse(fs.readFileSync(first.statusFile, "utf8"));
    fs.writeFileSync(first.statusFile, JSON.stringify({ ...record, exitedAt: 1 }));
    s.atBound(); await s.pulse(); await s.pulse();
    expect(s.actors.status(s.actor.id)).toMatchObject({ status: "running", queued: 1, inFlightRun: { id: first.runId } });
    expect(s.handles).toHaveLength(1);
    expect(s.stuckAlarms()).toHaveLength(0);
    expect(s.agents.actorRunLost(first.runId)).toBe(false);
  });

  it("cancels a delayed asynchronous alarm claim when its captured run episode has cleared", async () => {
    const s = await setup();
    const first = await s.start();
    const stop = s.blockStop();
    const writeBatch = s.mesh.writeBatch.bind(s.mesh);
    const key = actorAlarmKey(s.actor.id, "actor-run-stuck");
    let release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    cleanups.push(async () => { release(); });
    let claimed = false;
    vi.spyOn(s.mesh, "writeBatch").mockImplementation(async request => {
      const results = await writeBatch(request);
      if (results.some(result => result.key === key && result.applied)) {
        claimed = true;
        await gate;
      }
      return results;
    });
    process.kill(first.worker.pid, "SIGKILL");
    await s.handles[0]!.closed;
    s.atBound(); await s.pulse();
    await until(() => claimed && stop.calls.mock.calls.length === 1, "claim and native join held");
    const observedExit = s.agents.actorWorkerExit(first.runId, s.actor.id)!;
    expect(observedExit.unfinished).toBe(true);
    const observeExit = s.agents.actorWorkerExit.bind(s.agents);
    // Keep the genuine eligibility snapshot observable after clearing. This
    // isolates the captured-episode guard from a later terminal classification:
    // a stale async claim must not depend on that separate check to cancel.
    vi.spyOn(s.agents, "actorWorkerExit").mockImplementation((runId, actorId) =>
      runId === first.runId && actorId === s.actor.id ? observedExit : observeExit(runId, actorId));
    // Finish the exact original run while its alarm is waiting for the claim.
    // Its next queued activation must not receive the old run's Main notice.
    stop.release();
    await until(() => s.handles.length === 2 && s.actors.status(s.actor.id).status === "idle", "captured episode cleared");
    release(); await s.pulse();
    expect(s.stuckAlarms()).toHaveLength(0);
    expect(s.triggeredNotices.filter(text => text.includes("actor-run-stuck"))).toHaveLength(0);
  }, 20_000);

  it.each(["normal monitor", "blocked fallback"])("keeps the one known-terminal pre-inference retry on the %s path", async pathName => {
    const s = await setup();
    const first = await s.start(false, false);
    const failed = (file: string) => {
      const running = JSON.parse(fs.readFileSync(file, "utf8"));
      fs.writeFileSync(file, JSON.stringify({ ...running, status: "failed", inferenceStarted: false,
        error: "Known terminal pre-inference refusal", finishedAt: Date.now() }));
    };
    failed(first.statusFile);
    process.kill(first.worker.pid, "SIGKILL");
    await until(() => !s.agents.workerIdentity(first.runId)?.open, "captured known-terminal close");
    if (pathName === "normal monitor") s.resume();
    else { s.atBound(); await s.pulse(); }
    await until(() => s.handles.length === 2, "one allowed identified retry", 6_500);
    const secondId = s.actors.status(s.actor.id).inFlightRun!.id;
    const secondFile = path.join(s.agents.runDirectory(secondId)!, "status.json");
    await until(() => fs.existsSync(secondFile), "retry worker status");
    expect(s.agents.actorRunLost(first.runId)).toBe(false);
    expect(JSON.parse(fs.readFileSync(secondFile, "utf8")).task).toContain("dead-run-must-not-replay");
    failed(secondFile);
    process.kill(s.agents.workerIdentity(secondId)!.pid, "SIGKILL");
    await until(() => s.handles.length === 3 && s.actors.status(s.actor.id).status === "idle", "retry exhausted, next input completed", 6_500);
    await s.pulse();
    expect(s.handles).toHaveLength(3); // failed original, its one retry, distinct NEXT
    expect(s.agents.actorRunLost(secondId)).toBe(false);
  }, 20_000);

  it.each([
    ["loss-monitor", "restart"], ["loss-fallback", "restart"], ["loss-fallback", "adopt"],
  ])("fences loss before publication survives a real owner kill (%s -> %s)", async (pathName, restore) => {
    const s = await crashFixture();
    expectKilled(await s.owner(pathName!));
    const state = JSON.parse(fs.readFileSync(path.join(s.root, "state.json"), "utf8"));
    const before = JSON.parse(fs.readFileSync(path.join(s.root, "before.json"), "utf8"));
    const published = JSON.parse(fs.readFileSync(path.join(s.root, "published.json"), "utf8"));
    expect(before.items[0]).toMatchObject({ launchEvidenceVersion: 1, executionStarted: true });
    expect(before.items[0].terminalFailed).toBeUndefined();
    expect(published.verdict).toMatchObject({ status: "failed", error: expect.stringContaining("proven gone") });
    // Soft so the unfixed ordering also reaches the actual replay assertion.
    expect.soft(published.queue.items[0].terminalFailed).toBe(true);
    if (restore === "adopt") {
      // A real owner change, with an older unmarked copy ordered BEFORE the
      // predecessor containing the loss fence. Neither input has outcomeSend.
      const staleRoot = "session:reconcile-stale";
      const key = createHash("sha256").update([staleRoot, "session"].join("\0")).digest("hex").slice(0, 16);
      fs.writeFileSync(path.join(s.root, "actors", state.actorId, `queue-${key}.json`), JSON.stringify(before));
      const file = path.join(s.root, "actors", "actors.json");
      const registry = JSON.parse(fs.readFileSync(file, "utf8"));
      const actor = registry.actors.find((row: { id: string }) => row.id === state.actorId);
      actor.rootId = "session:reconcile-adopter";
      actor.adoptedFrom = [staleRoot, "session:reconcile-crash"];
      fs.writeFileSync(file, JSON.stringify(registry));
    }
    fs.writeFileSync(path.join(s.root, "restarted"), "fresh owner");
    const restarted = await s.owner(restore!);
    expect(restarted.code, restarted.output).toBe(0);
    const launches = s.journal();
    expect(launches.filter(entry => entry.task.includes("lost-original"))).toHaveLength(1);
    expect(launches.filter(entry => entry.task.includes("NEXT queued item"))).toHaveLength(1);
    expect(launches).toHaveLength(2);
    expect(JSON.parse(fs.readFileSync(path.join(s.root, "restored.json"), "utf8")).status).toMatchObject({ status: "idle", queued: 0 });
  }, 35_000);

  it("retains the bounded interrupted restart policy when no terminal loss verdict was published", async () => {
    const s = await crashFixture();
    for (let attempts = 0; attempts <= 3; attempts++) {
      expectKilled(await s.owner("interrupt"));
      const state = JSON.parse(fs.readFileSync(path.join(s.root, "state.json"), "utf8"));
      const saved = JSON.parse(fs.readFileSync(state.queueFile, "utf8"));
      expect(saved.items[0]).toMatchObject({ attempts, executionStarted: true });
      expect(saved.items[0].terminalFailed).toBeUndefined();
      expect(JSON.parse(fs.readFileSync(state.statusFile, "utf8")).status).toBe("running");
      expect(fs.existsSync(path.join(s.root, "published.json"))).toBe(false);
    }
    fs.writeFileSync(path.join(s.root, "restarted"), "fresh owner");
    const restarted = await s.owner("restart");
    expect(restarted.code, restarted.output).toBe(0);
    // Initial execution + three permitted restarts; the fourth interruption
    // exhausts the existing budget. Distinct untouched backlog still executes.
    expect(s.journal().filter(entry => entry.task.includes("lost-original"))).toHaveLength(4);
    expect(s.journal().filter(entry => entry.task.includes("NEXT queued item"))).toHaveLength(1);
  }, 45_000);
  it("keeps accepted hold-labelled GitHub backlog held while bounded reconciliation advances direct work", async () => {
    const s = await setup();
    const first = await s.start(false, false);
    const heldEvent = await s.mesh.publish({ topic: "github.demo", kind: "github.webhook",
      from: { id: "session:github-forwarder", name: "forwarder", kind: "main", sessionId: "github" },
      data: { event: "issue_comment", payload: { action: "created", number: 17191,
        issue: { number: 17191, labels: [{ id: 17191, name: "hold" }] }, marker: "held-across-reconcile" } } });
    await until(() => s.actors.status(s.actor.id).queued === 2, "held-labelled GitHub event admitted behind the active run");

    const changed = await s.actors.setActivationFilter(s.actor.id, ["hold"], undefined, undefined, undefined, { dropQueued: true });
    expect(changed).toMatchObject({ status: "running", queued: 1, movedToDeadLetter: 1 });
    const deadLetter = path.join(s.root, "actors", s.actor.id, "dead-letter.jsonl");
    const held = () => fs.readFileSync(deadLetter, "utf8").trim().split("\n").filter(Boolean)
      .map(line => JSON.parse(line) as { event: { id: string; topic: string; kind: string; data: unknown }; heldBy?: unknown });
    expect(held()).toHaveLength(1);
    expect(held()[0]).toMatchObject({ event: { id: heldEvent.id, topic: "github.demo", kind: "github.webhook",
      data: { payload: { issue: { labels: [{ name: "hold" }] } } } }, heldBy: expect.anything() });

    process.kill(first.worker.pid, "SIGKILL");
    await until(() => !s.agents.workerIdentity(first.runId)?.open, "captured native close");
    s.atBound(); await s.pulse();
    await until(() => s.handles.length === 2 && s.actors.status(s.actor.id).status === "idle", "direct next item after bounded reconcile", 6_500);
    const status = s.actors.status(s.actor.id);
    expect(status).toMatchObject({ id: s.actor.id, status: "idle", queued: 0 });
    expect(status.inFlightRun).toBeUndefined();
    expect(s.handles).toHaveLength(2); // lost worker plus the direct next item; the held event never launched
    const transcript = fs.readFileSync(s.actor.sessionFile!, "utf8");
    expect(transcript).toContain("NEXT queued item"); // direct work is exempt from the GitHub hold filter
    expect(transcript).not.toContain("held-across-reconcile");
    await s.pulse(); await s.pulse();
    expect(s.handles).toHaveLength(2);
    expect(held()).toHaveLength(1);
    expect(held()[0]).toMatchObject({ event: { id: heldEvent.id }, heldBy: expect.anything() });
  }, 20_000);

  it("settles a dead worker despite a blocked run continuation, advances the same serial actor, and reconciles once", async () => {
    const s = await setup();
    const run = s.agents.run.bind(s.agents);
    let releaseRun!: () => void;
    const runGate = new Promise<void>(resolve => { releaseRun = resolve; });
    cleanups.push(async () => { releaseRun(); });
    let calls = 0;
    vi.spyOn(s.agents, "run").mockImplementation(async (...args) => {
      const first = calls++ === 0;
      const result = await run(...args);
      if (first) await runGate; // manager settled, original run continuation still blocked
      return result;
    });
    const first = await s.start(false, false); // no inference evidence, but an explicit replay key
    const stop = s.blockStop();
    // Runtime is already over the existing threshold when the exact host-held
    // transport observes exit. Runtime age cannot substitute for exit age.
    s.atBound();
    process.kill(first.worker.pid, "SIGKILL");
    await s.handles[0]!.closed;
    for (const age of [0, 5_000]) {
      s.atExitAge(age); await s.pulse();
      expect(s.actors.status(s.actor.id)).toMatchObject({ status: "running", queued: 1, inFlightRun: { id: first.runId } });
      expect(s.handles).toHaveLength(1);
      expect(s.stuckAlarms()).toHaveLength(0);
      expect(s.triggeredNotices.filter(text => text.includes("actor-run-stuck"))).toHaveLength(0);
      expect(stop.calls).not.toHaveBeenCalled();
      expect(s.agents.actorRunLost(first.runId)).toBe(false);
      expect(JSON.parse(fs.readFileSync(first.statusFile, "utf8")).status).toBe("running");
    }
    s.atExitAge(5_001); await s.pulse();
    await until(() => stop.calls.mock.calls.length === 1 && s.stuckAlarms().length === 1, "eligible unfinished loss and pending native join");
    // The alarm is allowed while this original episode is still current, but a
    // pending exact stop/close/tree join permits neither verdict nor NEXT.
    expect(s.handles).toHaveLength(1);
    expect(s.agents.actorRunLost(first.runId)).toBe(false);
    expect(JSON.parse(fs.readFileSync(first.statusFile, "utf8")).status).toBe("running");
    expect(s.triggeredNotices.filter(text => text.includes("actor-run-stuck"))).toHaveLength(1);
    stop.release();
    await until(() => s.handles.length === 2, "next queued activation after checked native join", 6_500);
    await until(() => s.actors.status(s.actor.id).status === "idle", "serial queue idle");
    // The next activation may collect the old in-memory handle, but its
    // durable run status and actor archive must retain the loss reason.
    expect(JSON.parse(fs.readFileSync(first.statusFile, "utf8"))).toMatchObject({ status: "failed", error: expect.stringMatching(/proven gone|worker-gone/i) });
    const exits = fs.readFileSync(path.join(path.dirname(first.statusFile), "events.jsonl"), "utf8").trim().split("\n")
      .map(line => JSON.parse(line)).filter(event => event.type === "run.native_exit");
    expect(exits).toHaveLength(1);
    expect(exits[0]).toMatchObject({ id: first.runId, scope: "primary", pid: first.worker.pid,
      signal: process.platform === "win32" ? null : "SIGKILL", code: process.platform === "win32" ? expect.any(Number) : null,
      exitedAt: expect.any(Number), cgroup: { state: expect.stringMatching(/^(observed|unavailable|unsupported)$/) } });
    if (first.worker.startTime) expect(exits[0].processStartTime).toBe(first.worker.startTime);
    expect(s.actors.status(s.actor.id)).toMatchObject({ id: s.actor.id, queued: 0 });
    expect(s.actors.status(s.actor.id).inFlightRun).toBeUndefined();
    expect(s.actors.messages(s.actor.id).filter(message => message.direction === "in")).toHaveLength(2);
    expect(s.stuckAlarms()).toHaveLength(1);
    expect(s.stuckAlarms()[0]).toMatchObject({ data: { runId: first.runId, reason: "worker-gone" } });
    expect(s.notices.filter(text => text.includes("actor-run-stuck"))).toHaveLength(1);
    await s.pulse(); await s.pulse();
    // Release the original blocked monitor after recovery: its late continuation
    // cannot publish again or clear the newer run's activation marker.
    s.resume();
    releaseRun();
    await sleep(250);
    expect(s.handles).toHaveLength(2);
    expect(s.stuckAlarms()).toHaveLength(1);
    expect(s.notices.filter(text => text.includes("actor-run-stuck"))).toHaveLength(1);
    expect(s.actors.messages(s.actor.id).filter(message => message.direction === "out" && message.error)).toHaveLength(1);
  }, 20_000);

  it("recovers the original blocked continuation on the existing idle maintenance wake without new ingress", async () => {
    const s = await setup();
    const first = await s.start(false, false);
    process.kill(first.worker.pid, "SIGKILL");
    await s.handles[0]!.closed;
    s.atBound();
    // No pulse, fast retention sweep, status-driven recovery or new activation.
    await until(() => s.handles.length === 2, "idle maintenance recovers queued NEXT", 6_500);
    await until(() => s.actors.status(s.actor.id).status === "idle", "same serial actor idle");
    expect(JSON.parse(fs.readFileSync(first.statusFile, "utf8"))).toMatchObject({
      status: "failed", error: expect.stringContaining("proven gone"),
    });
    expect(s.handles).toHaveLength(2); // original is never replayed
    s.resume(); await s.pulse();
    expect(s.handles).toHaveLength(2);
  }, 20_000);

  it("coalesces repeated reconciliation while the first native stop/close join is pending", async () => {
    const s = await setup();
    const first = await s.start();
    const stop = s.blockStop();
    process.kill(first.worker.pid, "SIGKILL");
    await until(() => !s.agents.workerIdentity(first.runId)?.open, "captured native close");
    s.atBound(); await s.pulse();
    await until(() => stop.calls.mock.calls.length === 1, "fallback native join");
    await s.pulse(); await s.pulse();
    expect(s.handles).toHaveLength(1);
    expect(s.actors.status(s.actor.id).inFlightRun?.id).toBe(first.runId);
    expect(stop.calls).toHaveBeenCalledTimes(1);
    expect(s.notices.filter(text => text.includes("actor-run-stuck"))).toHaveLength(1);
    stop.release();
    await until(() => s.handles.length === 2, "serial advance after exact join");
    await until(() => s.actors.status(s.actor.id).status === "idle", "queue idle");
    expect(s.stuckAlarms()).toHaveLength(1);
    expect(stop.calls).toHaveBeenCalledTimes(1);
  }, 20_000);

  it("does not publish synthetic loss when its durable queue checkpoint fails, then retries the same join", async () => {
    const s = await setup();
    const first = await s.start(false, false);
    const rename = fs.renameSync;
    const refused = vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (/^queue-.*\.json$/.test(path.basename(String(to)))) throw Object.assign(new Error("queue fence write refused"), { code: "EIO" });
      return rename(from, to);
    });
    process.kill(first.worker.pid, "SIGKILL");
    await until(() => !s.agents.workerIdentity(first.runId)?.open, "captured native close");
    // Exercise the same helper that the normal monitor and blocked fallback use.
    expect(await s.agents.reconcileActorRun(first.runId, s.actor.id)).toBeUndefined();
    expect(JSON.parse(fs.readFileSync(first.statusFile, "utf8")).status).toBe("running");
    expect(s.agents.actorRunLost(first.runId)).toBe(false);
    expect(s.handles).toHaveLength(1);
    refused.mockRestore();
    s.atBound(); await s.pulse();
    await until(() => s.handles.length === 2 && s.actors.status(s.actor.id).status === "idle", "advance only after fence commits", 6_500);
    expect(JSON.parse(fs.readFileSync(first.statusFile, "utf8"))).toMatchObject({ status: "failed", error: expect.stringContaining("proven gone") });
    expect(s.handles).toHaveLength(2); // original never replayed
  }, 20_000);

  it("does not touch a slow but live worker at the bound", async () => {
    const s = await setup();
    const first = await s.start();
    s.atBound(24 * 60 * 60 * 1_000); await s.pulse(); await s.pulse();
    expect(s.actors.status(s.actor.id)).toMatchObject({ status: "running", queued: 1, inFlightRun: { id: first.runId } });
    expect(s.handles).toHaveLength(1);
    expect(s.stuckAlarms()).toHaveLength(0);
    expect(s.notices).toEqual([]);
  });

  it("does not mistake a worker-written completed status for native exit", async () => {
    const s = await setup();
    const first = await s.start();
    const running = JSON.parse(fs.readFileSync(first.statusFile, "utf8"));
    fs.writeFileSync(first.statusFile, JSON.stringify({ ...running, status: "completed", text: "not an exit receipt", finishedAt: Date.now() }));
    s.atBound(); await s.pulse(); await s.pulse();
    expect(s.actors.status(s.actor.id)).toMatchObject({ status: "running", queued: 1, inFlightRun: { id: first.runId } });
    expect(s.handles).toHaveLength(1);
    expect(s.stuckAlarms()).toHaveLength(0);
    fs.writeFileSync(first.statusFile, JSON.stringify(running));
  });

  it.skipIf(process.platform !== "linux").each(["running", "completed"] as const)("retains a surviving descendant's session custody with %s worker status, then advances only after its last write", async status => {
    const s = await setup();
    const first = await s.start(true);
    // Capture the real descendant tree while its parent can still attest it.
    expect(await s.handles[0]!.isAlive()).toBe(true);
    process.kill(first.worker.pid, "SIGKILL");
    await until(() => !s.agents.workerIdentity(first.runId)?.open, "leader native close");
    if (status === "completed") {
      const running = JSON.parse(fs.readFileSync(first.statusFile, "utf8"));
      fs.writeFileSync(first.statusFile, JSON.stringify({ ...running, status, text: "finished worker verdict", finishedAt: Date.now() }));
    }
    s.atBound(); await s.pulse(); await s.pulse();
    expect(s.handles).toHaveLength(1);
    expect(s.actors.status(s.actor.id)).toMatchObject({ queued: 1, inFlightRun: { id: first.runId } });
    expect(fs.readFileSync(s.actor.sessionFile!, "utf8")).not.toContain("reconcileDescendant");
    fs.writeFileSync(path.join(s.root, "release-descendant"), "release");
    await until(() => fs.readFileSync(s.actor.sessionFile!, "utf8").includes("reconcileDescendant"), "descendant last session write");
    await s.pulse();
    await until(() => s.handles.length === 2, "next run after checked descendant exit", 11_500);
    await until(() => s.actors.status(s.actor.id).status === "idle", "queue idle after descendant exit");
    expect(JSON.parse(fs.readFileSync(first.statusFile, "utf8")).status).toBe(status === "running" ? "failed" : "completed");
    expect(s.notices.filter(text => text.includes("actor-run-stuck"))).toHaveLength(status === "running" ? 1 : 0);
  }, 20_000);
});
