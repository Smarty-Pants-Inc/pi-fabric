import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import type { AgentHandleInfo, AgentRunResult } from "../src/agents/types.js";
import { DEFAULT_FABRIC_CONFIG, type FabricAgentConfig } from "../src/config.js";
import { ResidentHost } from "../src/residency/host.js";
import { RESIDENT_HOST_FORMAT, residentActorRoots, type ResidentHostConfig } from "../src/residency/protocol.js";
import { installInProcessResidentFence } from "./helpers/in-process-resident-fence.js";

// Keep reason: recorded real catch Smarty-Pants-Inc/smarty-dev#9031. A silent
// actor held an actual admission permit, invisible outside its resident. This
// is a private host observability/trust boundary, not a source-text contract.
interface AdmissionSnapshot {
  format: 1;
  writtenAt: number;
  cap: number;
  held: Array<{
    actorId: string | null;
    actorName: string | null;
    runId: string | null;
    acquiredAt: number;
    lastProgressAt: number;
  }>;
  waiting: Array<{
    actorId: string | null;
    actorName: string | null;
    since: number;
    runId?: string | null;
  }>;
}

const workerPath = path.resolve("tests/fixtures/admission-snapshot-worker.mjs");
const roots: string[] = [];
const managers: AgentManager[] = [];
const hosts: ResidentHost[] = [];
const controls: string[] = [];
const preparationReleases: Array<() => void> = [];
const delay = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
// The normal source is the manager's existing admission/lifecycle transitions.
// Only these bounded assertions observe the debounced file; no production
// polling or heartbeat is added to make the observations pass.
const eventually = (assertion: () => void) => vi.waitFor(assertion, { timeout: 5_000, interval: 20 });

beforeEach(() => {
  for (const key of [
    "PI_FABRIC_DEPTH", "PI_FABRIC_BUDGET", "PI_FABRIC_BUDGET_FILE", "PI_FABRIC_BUDGET_ID",
    "DBUS_SESSION_BUS_ADDRESS", "XDG_RUNTIME_DIR",
  ]) vi.stubEnv(key, undefined);
});

afterEach(async () => {
  for (const release of preparationReleases.splice(0)) release();
  // Let every real child leave even if an assertion failed before its command.
  for (const controlRoot of controls.splice(0)) {
    fs.writeFileSync(path.join(controlRoot, "release"), "release", { mode: 0o600 });
  }
  try {
    await Promise.all(hosts.splice(0).map((host) => host.close()));
    await Promise.all(managers.splice(0).map((manager) => manager.close()));
  } finally {
    vi.restoreAllMocks();
    vi.unstubAllEnvs();
    for (const root of roots.splice(0)) {
      fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
    }
  }
});

function newRoot(): string {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-admission-snapshot-"));
  roots.push(root);
  return root;
}

function newControl(root: string, name: string): string {
  const controlRoot = path.join(root, "controls", name);
  fs.mkdirSync(controlRoot, { recursive: true, mode: 0o700 });
  controls.push(controlRoot);
  return controlRoot;
}

const command = (controlRoot: string, name: string): void => {
  fs.writeFileSync(path.join(controlRoot, name), name, { mode: 0o600 });
};
const workerReady = (controlRoot: string) => eventually(() => {
  expect(fs.existsSync(path.join(controlRoot, "ready"))).toBe(true);
});

function readSnapshot(file: string): AdmissionSnapshot {
  const snapshot = JSON.parse(fs.readFileSync(file, "utf8")) as AdmissionSnapshot;
  expect(snapshot.format).toBe(1);
  expect(Number.isFinite(snapshot.writtenAt)).toBe(true);
  expect(Number.isInteger(snapshot.cap)).toBe(true);
  expect(Array.isArray(snapshot.held)).toBe(true);
  expect(Array.isArray(snapshot.waiting)).toBe(true);
  for (const held of snapshot.held) {
    expect(Number.isFinite(held.acquiredAt)).toBe(true);
    expect(Number.isFinite(held.lastProgressAt)).toBe(true);
  }
  for (const waiting of snapshot.waiting) expect(Number.isFinite(waiting.since)).toBe(true);
  return snapshot;
}

async function expectSnapshot(file: string, assertion: (snapshot: AdmissionSnapshot) => void): Promise<AdmissionSnapshot> {
  let snapshot!: AdmissionSnapshot;
  await eventually(() => { snapshot = readSnapshot(file); assertion(snapshot); });
  return snapshot;
}

const agentConfig = (maxConcurrent = 1): FabricAgentConfig => ({
  ...DEFAULT_FABRIC_CONFIG.agents,
  enabled: true, runner: "pi", transport: "process", maxConcurrent,
  budgetUsd: 0, deniedModels: [], model: "fixture/visible", timeoutMs: 10_000,
  retainRuns: true, sessionExport: false,
});

function fixture(options: {
  maxConcurrent?: number;
  preparePiModel?: (model: string | undefined) => Promise<string | void>;
  blockedSnapshotParent?: boolean;
} = {}) {
  const root = newRoot();
  const runRoot = path.join(root, "runs");
  fs.mkdirSync(runRoot, { mode: 0o700 });
  const actorRoot = path.join(root, "actors");
  if (options.blockedSnapshotParent) fs.writeFileSync(actorRoot, "not a directory", { mode: 0o600 });
  else fs.mkdirSync(actorRoot, { mode: 0o700 });
  const admissionPath = path.join(actorRoot, "admission.json");
  const manager = new AgentManager(process.cwd(), agentConfig(options.maxConcurrent), {
    workerPath, runRoot, admissionSnapshotPath: admissionPath,
    ...(options.preparePiModel ? { preparePiModel: options.preparePiModel } : {}),
  });
  managers.push(manager);
  return { root, runRoot, actorRoot, admissionPath, manager };
}

interface ActorRun {
  actorId: string;
  actorName: string;
  controlRoot: string;
  launched?: AgentHandleInfo;
  queued?: AgentHandleInfo;
  result: Promise<AgentRunResult>;
}

function runActor(f: ReturnType<typeof fixture>, actorName: string, options: { signal?: AbortSignal; model?: string } = {}): ActorRun {
  const actorId = `actor:${actorName}`;
  const controlRoot = newControl(f.root, actorName);
  const actor = { actorId, actorName, controlRoot } as ActorRun;
  // This is the host actor route used by ActorManager: run(request, signal,
  // onSpawned, authorize, onDowngrade, onQueued, preparation). In particular the
  // authorize callback retains owner cancellation across semaphore queueing;
  // a guest spawn receipt intentionally has different abort semantics.
  actor.result = f.manager.run({
    task: JSON.stringify({ admissionControlRoot: controlRoot }),
    name: `worker-${actorName}`, actorId, actorName,
    sessionFile: path.join(f.root, `${actorName}.jsonl`),
    transport: "process", model: options.model ?? "fixture/visible",
  }, options.signal, (handle) => { actor.launched = handle; }, () => true,
  undefined, (handle) => { actor.queued = handle; }, { timeoutMs: 5_000 });
  // Keep rejection observed during assertions/cleanup; tests still await the
  // original result and fail on an unexpected rejection.
  void actor.result.catch(() => undefined);
  return actor;
}

async function launched(actor: ActorRun): Promise<AgentHandleInfo> {
  await eventually(() => expect(actor.launched).toBeDefined());
  await workerReady(actor.controlRoot);
  return actor.launched!;
}

async function queued(actor: ActorRun): Promise<AgentHandleInfo> {
  await eventually(() => expect(actor.queued?.status).toBe("queued"));
  return actor.queued!;
}

const finish = async (actor: ActorRun): Promise<AgentRunResult> => {
  command(actor.controlRoot, "release");
  const result = await actor.result;
  expect(result.status).toBe("completed");
  return result;
};
const expectEmpty = (file: string) => expectSnapshot(file, (snapshot) => {
  expect(snapshot.held).toEqual([]);
  expect(snapshot.waiting).toEqual([]);
});

async function observeInitialWorkerStatus(f: ReturnType<typeof fixture>, handle: AgentHandleInfo): Promise<void> {
  const record = JSON.parse(fs.readFileSync(path.join(f.runRoot, handle.id, "status.json"), "utf8"));
  // Worker readiness can precede the manager's first status observation. The
  // first real status is progress; only subsequent identical reads are idle.
  await eventually(() => expect(f.manager.status(handle.id)).toMatchObject({ updatedAt: record.updatedAt, sessionId: record.sessionId }));
  await delay(50); // Allow the existing 25 ms snapshot debounce to commit it.
}

describe("actual resident admission permit snapshot (#9031)", () => {
  it("exports a permit during preparation, queues the second actor, then promotes it on real release", async () => {
    let release!: () => void;
    const gate = new Promise<void>((resolve) => { release = resolve; });
    preparationReleases.push(release);
    let preparing = false;
    const f = fixture({ preparePiModel: async (model) => {
      if (model === "fixture/preparing") { preparing = true; await gate; }
      return model;
    } });
    const beforeAdmission = Date.now();
    const first = runActor(f, "preparing", { model: "fixture/preparing" });
    await eventually(() => expect(preparing).toBe(true));
    const held = await expectSnapshot(f.admissionPath, (snapshot) => {
      expect(snapshot.cap).toBe(1);
      expect(snapshot.held).toEqual([expect.objectContaining({ actorId: first.actorId, actorName: first.actorName, runId: expect.any(String) })]);
      expect(snapshot.waiting).toEqual([]);
    });
    const firstPermit = held.held[0]!;
    expect(firstPermit.acquiredAt).toBeGreaterThanOrEqual(beforeAdmission);
    expect(firstPermit.acquiredAt).toBeLessThanOrEqual(held.writtenAt);
    expect(first.launched).toBeUndefined();
    expect(fs.existsSync(path.join(f.runRoot, firstPermit.runId!))).toBe(false);

    const beforeWaiting = Date.now();
    const second = runActor(f, "second");
    const secondQueued = await queued(second);
    const waiting = await expectSnapshot(f.admissionPath, (snapshot) => {
      expect(snapshot.held).toEqual([firstPermit]);
      expect(snapshot.waiting).toEqual([expect.objectContaining({ actorId: second.actorId, actorName: second.actorName, since: expect.any(Number) })]);
    });
    expect(waiting.waiting[0]!.since).toBeGreaterThanOrEqual(beforeWaiting);
    expect(second.launched).toBeUndefined();
    expect(fs.existsSync(path.join(second.controlRoot, "ready"))).toBe(false);

    release();
    const firstHandle = await launched(first);
    expect(firstHandle.id).toBe(firstPermit.runId);
    await expectSnapshot(f.admissionPath, (snapshot) => {
      expect(snapshot.held).toHaveLength(1);
      expect(snapshot.held[0]).toMatchObject({ runId: firstHandle.id, acquiredAt: firstPermit.acquiredAt });
      expect(snapshot.waiting).toHaveLength(1);
    });
    await finish(first);
    const secondHandle = await launched(second);
    expect(secondHandle.id).toBe(secondQueued.id);
    await expectSnapshot(f.admissionPath, (snapshot) => {
      expect(snapshot.held).toEqual([expect.objectContaining({ actorId: second.actorId, actorName: second.actorName, runId: secondHandle.id })]);
      expect(snapshot.held[0]!.acquiredAt).toBeGreaterThanOrEqual(waiting.waiting[0]!.since);
      expect(snapshot.waiting).toEqual([]);
    });
    await finish(second);
    await expectEmpty(f.admissionPath);
  });

  it("removes an aborted host-owned waiter without launching it or releasing the current holder", async () => {
    const f = fixture();
    const first = runActor(f, "holder");
    const firstHandle = await launched(first);
    const owner = new AbortController();
    const waiter = runActor(f, "cancelled-waiter", { signal: owner.signal });
    const waiterHandle = await queued(waiter);
    await expectSnapshot(f.admissionPath, (snapshot) => {
      expect(snapshot.held.map((held) => held.runId)).toEqual([firstHandle.id]);
      expect(snapshot.waiting.map((waiting) => waiting.actorId)).toEqual([waiter.actorId]);
    });
    owner.abort();
    await expect(waiter.result).resolves.toMatchObject({ id: waiterHandle.id, status: "stopped" });
    await expectSnapshot(f.admissionPath, (snapshot) => {
      expect(snapshot.held.map((held) => held.runId)).toEqual([firstHandle.id]);
      expect(snapshot.waiting).toEqual([]);
    });
    expect(waiter.launched).toBeUndefined();
    expect(fs.existsSync(path.join(waiter.controlRoot, "ready"))).toBe(false);
    // An incorrectly released holder would let this third actor start early.
    const next = runActor(f, "next-after-cancel");
    await queued(next);
    await expectSnapshot(f.admissionPath, (snapshot) => {
      expect(snapshot.held.map((held) => held.runId)).toEqual([firstHandle.id]);
      expect(snapshot.waiting.map((waiting) => waiting.actorId)).toEqual([next.actorId]);
    });
    expect(next.launched).toBeUndefined();
    await finish(first);
    await launched(next);
    await finish(next);
    await expectEmpty(f.admissionPath);
    expect(waiter.launched).toBeUndefined();
  });

  it("atomically replaces private JSON while the old target and staged replacement are both valid", async () => {
    const f = fixture();
    const rename = fs.renameSync;
    const replacements: Array<{ source: string; previous: AdmissionSnapshot; staged: AdmissionSnapshot; bytes: string; mode: number }> = [];
    vi.spyOn(fs, "renameSync").mockImplementation((source, target) => {
      if (String(target) === f.admissionPath && fs.existsSync(f.admissionPath)) {
        const bytes = fs.readFileSync(source, "utf8");
        replacements.push({
          source: String(source), previous: readSnapshot(f.admissionPath),
          staged: JSON.parse(bytes) as AdmissionSnapshot, bytes,
          mode: fs.statSync(source).mode & 0o777,
        });
      }
      // Observe the real atomic helper, never replace it with a mock writer.
      return rename(source, target);
    });
    const first = runActor(f, "atomic-holder");
    const firstHandle = await launched(first);
    await expectSnapshot(f.admissionPath, (snapshot) => {
      expect(snapshot.held.map((held) => held.runId)).toEqual([firstHandle.id]);
      expect(snapshot.waiting).toEqual([]);
    });
    const second = runActor(f, "atomic-waiter");
    await queued(second);
    await expectSnapshot(f.admissionPath, (snapshot) => {
      expect(snapshot.waiting.map((waiting) => waiting.actorId)).toEqual([second.actorId]);
    });
    const replacement = replacements.find((entry) => entry.previous.held[0]?.runId === firstHandle.id &&
      entry.previous.waiting.length === 0 && entry.staged.waiting[0]?.actorId === second.actorId);
    expect(replacement).toBeDefined();
    expect(replacement!.source).not.toBe(f.admissionPath);
    expect(path.dirname(replacement!.source)).toBe(path.dirname(f.admissionPath));
    expect(replacement!.staged).toMatchObject({ format: 1, cap: 1 });
    expect(replacement!.staged.held[0]!.runId).toBe(firstHandle.id);
    expect(JSON.parse(fs.readFileSync(f.admissionPath, "utf8"))).toEqual(replacements.at(-1)!.staged);
    if (process.platform !== "win32") {
      expect(replacement!.mode).toBe(0o600);
      expect(fs.statSync(f.admissionPath).mode & 0o777).toBe(0o600);
    }
    await finish(first);
    await launched(second);
    await finish(second);
    await expectEmpty(f.admissionPath);
  });

  it("does not manufacture progress for a frozen worker across elapsed time, status reads and snapshot rewrites", async () => {
    const f = fixture();
    const silent = runActor(f, "silent");
    const handle = await launched(silent);
    await observeInitialWorkerStatus(f, handle);
    const baseline = await expectSnapshot(f.admissionPath, (snapshot) => {
      expect(snapshot.held.map((held) => held.runId)).toEqual([handle.id]);
    });
    const progressAt = baseline.held[0]!.lastProgressAt;
    for (let observation = 0; observation < 4; observation++) {
      await delay(80);
      expect(f.manager.status(handle.id).status).toBe("running");
      expect(f.manager.list().find((run) => run.id === handle.id)?.status).toBe("running");
      expect(readSnapshot(f.admissionPath).held[0]!.lastProgressAt).toBe(progressAt);
    }
    const owner = new AbortController();
    const waiter = runActor(f, "rewrite-only", { signal: owner.signal });
    await queued(waiter);
    await expectSnapshot(f.admissionPath, (snapshot) => {
      expect(snapshot.writtenAt).toBeGreaterThan(baseline.writtenAt);
      expect(snapshot.waiting.map((waiting) => waiting.actorId)).toEqual([waiter.actorId]);
      expect(snapshot.held[0]!.lastProgressAt).toBe(progressAt);
    });
    owner.abort();
    await expect(waiter.result).resolves.toMatchObject({ status: "stopped" });
    await expectSnapshot(f.admissionPath, (snapshot) => {
      expect(snapshot.waiting).toEqual([]);
      expect(snapshot.held[0]!.lastProgressAt).toBe(progressAt);
    });
    expect(Date.now() - progressAt).toBeGreaterThanOrEqual(300);
    await finish(silent);
    await expectEmpty(f.admissionPath);
  });

  it("advances only the worker emitting existing lifecycle events, even without an onLifecycle subscriber", async () => {
    // Two permits are necessary only here: both siblings must be actually held,
    // not a queued actor being mistaken for an idle running sibling.
    const f = fixture({ maxConcurrent: 2 });
    const idle = runActor(f, "idle-sibling");
    const live = runActor(f, "live-sibling");
    const idleHandle = await launched(idle);
    const liveHandle = await launched(live);
    await observeInitialWorkerStatus(f, idleHandle);
    await observeInitialWorkerStatus(f, liveHandle);
    const baseline = await expectSnapshot(f.admissionPath, (snapshot) => {
      expect(snapshot.cap).toBe(2);
      expect(snapshot.held.map((held) => held.runId).sort()).toEqual([idleHandle.id, liveHandle.id].sort());
      expect(snapshot.waiting).toEqual([]);
    });
    const idleAt = baseline.held.find((held) => held.runId === idleHandle.id)!.lastProgressAt;
    let liveAt = baseline.held.find((held) => held.runId === liveHandle.id)!.lastProgressAt;
    const statusPath = path.join(f.runRoot, liveHandle.id, "status.json");
    const frozenStatus = fs.readFileSync(statusPath, "utf8");
    for (const sequence of [1, 2]) {
      await delay(35);
      command(live.controlRoot, `event-${sequence}`);
      await eventually(() => expect(fs.existsSync(path.join(live.controlRoot, `event-${sequence}.done`))).toBe(true));
      const advanced = await expectSnapshot(f.admissionPath, (snapshot) => {
        expect(snapshot.held).toHaveLength(2);
        expect(snapshot.held.find((held) => held.runId === liveHandle.id)!.lastProgressAt).toBeGreaterThan(liveAt);
        expect(snapshot.held.find((held) => held.runId === idleHandle.id)!.lastProgressAt).toBe(idleAt);
      });
      liveAt = advanced.held.find((held) => held.runId === liveHandle.id)!.lastProgressAt;
      expect(fs.readFileSync(statusPath, "utf8")).toBe(frozenStatus);
    }
    await finish(live);
    await expectSnapshot(f.admissionPath, (snapshot) => {
      expect(snapshot.held).toEqual([expect.objectContaining({ runId: idleHandle.id, lastProgressAt: idleAt })]);
    });
    await finish(idle);
    await expectEmpty(f.admissionPath);
  });

  it("does not let snapshot filesystem errors poison failure settlement or promotion, and recovers publication", async () => {
    // A real ENOTDIR path, not a mocked snapshot implementation or permission
    // check (chmod-based failures disappear under privileged test runners).
    const f = fixture({ blockedSnapshotParent: true });
    const first = runActor(f, "fails-with-broken-snapshot");
    await launched(first);
    const second = runActor(f, "promoted-with-broken-snapshot");
    await queued(second);
    command(first.controlRoot, "fail");
    await expect(first.result).resolves.toMatchObject({ status: "failed", error: "Admission fixture requested failure" });
    const secondHandle = await launched(second);
    expect(fs.statSync(f.actorRoot).isFile()).toBe(true);
    expect(secondHandle.status).toBe("running");
    fs.unlinkSync(f.actorRoot);
    fs.mkdirSync(f.actorRoot, { mode: 0o700 });
    command(second.controlRoot, "event-1");
    await expectSnapshot(f.admissionPath, (snapshot) => {
      expect(snapshot.cap).toBe(1);
      expect(snapshot.held).toEqual([expect.objectContaining({ actorId: second.actorId, runId: secondHandle.id })]);
      expect(snapshot.waiting).toEqual([]);
    });
    await finish(second);
    await expectEmpty(f.admissionPath);
  });

  it("wires ResidentHost's real actor admission export to the session actors root", async () => {
    installInProcessResidentFence(true);
    const root = newRoot();
    const config: ResidentHostConfig = {
      format: RESIDENT_HOST_FORMAT, rootId: "session:admission-main", sessionId: "admission-main",
      cwd: process.cwd(), projectRoot: process.cwd(), meshRoot: path.join(root, "mesh"),
      actorRoot: path.join(root, "project-actors"), sessionActorRoot: path.join(root, "session-actors"),
      residencyRoot: path.join(root, "resident"), fullCodeMode: true,
      agents: agentConfig(), mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorScope: "session", actorPollMs: 20 },
      retention: DEFAULT_FABRIC_CONFIG.retention, workerPath,
      fabricExtensionPath: path.resolve("dist/index.js"), piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
      piModels: { available: [{ provider: "fixture", id: "visible" }], aliases: {}, defaultModel: "fixture/visible" },
    };
    const host = new ResidentHost(config);
    hosts.push(host);
    await host.start();
    const controlRoot = newControl(root, "resident-actor");
    const actor = await host.actors.create({
      name: "admission-resident-actor", instructions: "Return a silent directive when released.",
      residency: "durable", transport: "process", responseMode: "directive",
      delivery: "followUp", triggerTurn: false,
    });
    host.actors.tell(actor.id, "Run the controlled admission fixture.", { admissionControlRoot: controlRoot });
    await workerReady(controlRoot);
    const admissionPath = path.join(residentActorRoots(config).session, "admission.json");
    const admitted = await expectSnapshot(admissionPath, (snapshot) => {
      expect(snapshot.cap).toBe(1);
      expect(snapshot.held).toEqual([expect.objectContaining({ actorId: actor.id, actorName: actor.name, runId: expect.any(String) })]);
      expect(snapshot.waiting).toEqual([]);
    });
    const runId = admitted.held[0]!.runId!;
    expect(host.agents.list().find((run) => run.id === runId)).toMatchObject({ id: runId, actorId: actor.id, actorName: actor.name, status: "running" });
    if (process.platform !== "win32") expect(fs.statSync(admissionPath).mode & 0o777).toBe(0o600);
    command(controlRoot, "release");
    await eventually(() => {
      expect(host.actors.status(actor.id).status).toBe("idle");
      expect(host.actors.inFlightCount()).toBe(0);
    });
    await expectEmpty(admissionPath);
    // The actor archives/forgets its completed run; the manager need not retain
    // a public status handle after native release and actor settlement.
    expect(host.actors.status(actor.id).lastRunId).toBe(runId);
  }, 20_000);
});
