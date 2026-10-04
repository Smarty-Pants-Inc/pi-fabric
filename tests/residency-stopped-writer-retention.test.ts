import { execFileSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { beforeEach, expect, it, vi } from "vitest";
import { installInProcessResidentFence } from "./helpers/in-process-resident-fence.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { canRemoveTerminalRun, hasUnresolvedWorker, markUnresolvedWorker } from "../src/storage/retention.js";
import { processAlive } from "../src/storage/scratch.js";
import { cancellationError } from "../src/async-settlement.js";
import { ResidencyClient } from "../src/residency/client.js";
import { ResidentHost, RESIDENT_RUN_RETENTION_MS } from "../src/residency/host.js";
import { ResidentRequestRetention } from "../src/residency/retention.js";
import { RESIDENT_REQUEST_RETENTION_MS } from "../src/residency/request-expiry.js";
import { readResidentRequestDecision, registerResidentCancellation, residentRoot, type ResidentCommand, type ResidentHostConfig } from "../src/residency/protocol.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { MeshStore } from "../src/mesh/store.js";

// A native session Main owns the caller binding independently of the resident executor.
const mainParticipants = (config: ResidentHostConfig) => {
  const identity = { id: config.rootId, name: "live Main", kind: "main" as const, sessionId: config.sessionId };
  const mesh = new MeshStore(config.meshRoot, config.mesh.maxEventBytes, config.mesh.maxReadEvents);
  const participants = new ParticipantDirectory(mesh, {
    enabled: true, hostId: identity.id, rootId: identity.id, identity, reapDeadHosts: false,
  });
  participants.registerSource(() => [{
    format: 1, id: identity.id, rootId: identity.id, kind: "root", name: identity.name, status: "idle",
    ownerHostId: identity.id, ownerIdentityId: identity.id, sessionId: identity.sessionId,
    runner: "pi", transport: "host", capabilities: ["fabric"], controlProtocol: "v1",
    startedAt: Date.now(), updatedAt: Date.now(),
  }]);
  return participants;
};

beforeEach(() => installInProcessResidentFence());

// Recovered-owner cases model a legacy primary-only exit receipt. New transports
// retain observed descendants and drain them at close; the non-restart cases below
// exercise that real tree-custody contract. Workers and persisted trees stay real.
const installLegacyPrimaryExitReceipt = () => {
  const launch = ProcessTransport.prototype.launch;
  vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function (this: ProcessTransport, request) {
    const handle = await launch.call(this, request);
    const primaryAlive = () => processAlive(Number(handle.sessionId));
    return { ...handle, isAlive: async () => primaryAlive(), stop: async () => {
      if (primaryAlive()) await handle.stop();
    } };
  });
};

const waitFor = async (predicate: () => boolean) => {
  const deadline = Date.now() + 5_000;
  while (!predicate() && Date.now() < deadline) await new Promise(resolve => setTimeout(resolve, 10));
  expect(predicate()).toBe(true);
};

it.each(["project", "session"] as const)("public actor stop retains its live writer's creation commitment and reconciliation IDs until checked writer exit, then collects once (%s scope)", async scope => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-stopped-writer-retention-"));
  const rootId = "session:stopped-writer-retention";
  const meshRoot = path.join(root, "mesh");
  const config: ResidentHostConfig = {
    format: 1, rootId, sessionId: "stopped-writer-retention", cwd: root, projectRoot: root,
    meshRoot, actorRoot: path.join(root, "actors"), residencyRoot: residentRoot(meshRoot, rootId),
    fullCodeMode: true, agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 }, mesh: DEFAULT_FABRIC_CONFIG.mesh,
    retention: DEFAULT_FABRIC_CONFIG.retention, workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
    fabricExtensionPath: path.resolve("dist/index.js"), piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
    piModels: { available: [{ provider: "fixture", id: "visible" }], aliases: {}, defaultModel: "fixture/visible" },
  };
  const host = new ResidentHost(config);
  const participants = mainParticipants(config);
  let client: ResidencyClient | undefined;
  let control: FabricControlPlane | undefined;
  let scanTime: number | undefined;
  let scans = 0;
  const nativeSweep = ResidentRequestRetention.prototype.sweep;
  const sweep = vi.spyOn(ResidentRequestRetention.prototype, "sweep").mockImplementation(function (this: ResidentRequestRetention, now, live, _budget, stoppedWritersGone) {
    // Advance only the retention clock: no fake worker/lease clocks or private maintenance calls.
    nativeSweep.call(this, scanTime ?? now, live, 10_000, stoppedWritersGone);
    if (scanTime !== undefined) scans++;
  });
  let due: ReturnType<typeof vi.spyOn> | undefined;
  const remove = vi.spyOn(fs, "rmSync");
  try {
    await participants.start();
    await host.start();
    client = new ResidencyClient({ config, mesh: host.mesh, participants, mainAgent: { local: false } as FabricMainAgentTarget });
    control = new FabricControlPlane(host.mesh, { id: rootId, name: "main", kind: "main", sessionId: config.sessionId },
      { enabled: true, hostId: rootId, pollMs: 20, acknowledgementTimeoutMs: 5_000 });
    control.start(() => ({ accepted: false }));
    const request = { name: "live stopped writer", instructions: "Reply.", residency: "durable" as const, scope,
      transport: "process" as const, responseMode: "text" as const, delivery: "mailbox" as const };
    const create = vi.spyOn(host.actors, "create");
    const actor = await client.createActor(request, AbortSignal.timeout(5_000));
    const requestId = fs.readdirSync(path.join(config.residencyRoot, "decisions")).map(name => name.slice(0, -5))
      .find(id => readResidentRequestDecision(config.residencyRoot, id)?.operation === "createActor")!;
    const decisionPath = path.join(config.residencyRoot, "decisions", `${requestId}.json`);
    const ackPath = path.join(config.residencyRoot, "acknowledgements", `${requestId}.json`);
    const committed = fs.readFileSync(decisionPath, "utf8");
    const ack = JSON.parse(fs.readFileSync(ackPath, "utf8"));
    const command: ResidentCommand = { format: 3, requestId, rootId, operation: "createActor", request, createdAt: ack.completedAt };
    await control.request(client.hostId, actor.id, "followUp", { message: "HANG_WITH_PROGRESS" }, client.hostId);
    await waitFor(() => host.agents.listForUi().some(run => run.actorId === actor.id && "turns" in run && run.turns > 0));
    const writer = host.agents.listForUi().find(run => run.actorId === actor.id)!;
    expect(writer.status).toBe("running");
    expect(await control.request(client.hostId, actor.id, "stop", {}, client.hostId)).toMatchObject({ acknowledged: true });
    expect(await client.actorStatus(actor.id)).toMatchObject({ id: actor.id, status: "stopped" });
    expect(host.agents.status(writer.id)).toMatchObject({ status: "running", actorId: actor.id, turns: 3 });

    scanTime = ack.acknowledgedAt + RESIDENT_REQUEST_RETENTION_MS + 1;
    due = vi.spyOn(ResidentRequestRetention.prototype, "due").mockReturnValue(true);
    await waitFor(() => scans > 0);
    due.mockReturnValue(false);
    expect(fs.readFileSync(decisionPath, "utf8")).toBe(committed);
    expect(JSON.parse(fs.readFileSync(ackPath, "utf8"))).toEqual(ack);
    const cancelled = new AbortController();
    registerResidentCancellation(cancelled.signal, config.residencyRoot, command);
    cancelled.abort();
    const outcome = cancellationError(cancelled.signal, new Error("outer invocation cancelled")) as Error & { residentOutcome: unknown };
    expect(outcome).toMatchObject({ code: "RESIDENT_REQUEST_EXPIRED", residentOutcome: {
      requestId, state: "committed", expired: true, operation: "createActor", entityKind: "actor", id: actor.id, ownerHostId: client.hostId,
    } });
    expect(outcome.message).toMatch(/do not replay or reassign/i);
    expect(Object.isFrozen(outcome.residentOutcome)).toBe(true);
    expect(await client.actorStatus(actor.id)).toMatchObject({ id: actor.id, status: "stopped", inFlightRun: { id: writer.id } });
    expect(create).toHaveBeenCalledTimes(1);
    expect(fs.readFileSync(decisionPath, "utf8")).toBe(committed);

    // Stop only the original fixture worker through the same public route; stop joins physical exit.
    expect(await control.request(client.hostId, writer.id, "stop", {}, client.hostId)).toMatchObject({ acknowledged: true });
    await waitFor(() => host.actors.inFlightCount() === 0);
    expect(host.agents.status(writer.id).status).toBe("stopped");
    const before = scans;
    scanTime = (scanTime ?? 0) + 60_001;
    due.mockReturnValue(true);
    await waitFor(() => scans > before && !fs.existsSync(decisionPath));
    due.mockReturnValue(false);
    expect(fs.existsSync(ackPath)).toBe(false);
    expect(remove.mock.calls.filter(([file]) => file === decisionPath)).toHaveLength(1);
    const collected = scans;
    scanTime = (scanTime ?? 0) + 60_001;
    due.mockReturnValue(true);
    await waitFor(() => scans > collected);
    due.mockReturnValue(false);
    expect(remove.mock.calls.filter(([file]) => file === decisionPath)).toHaveLength(1);
    expect(create).toHaveBeenCalledTimes(1);
  } finally {
    due?.mockRestore(); sweep.mockRestore(); remove.mockRestore();
    // Every run belongs to this isolated fixture. Join them even if setup failed
    // before the original writer ID was observed; actor close alone can await a detached run.
    for (const run of host.agents?.listForUi() ?? []) await host.agents.stop(run.id);
    await control?.close(); await client?.close(); await host.close(); await participants.close();
    vi.restoreAllMocks();
    fs.rmSync(root, { recursive: true, force: true });
  }
}, 20_000);

// POSIX-only: the fixture's nested writer must outlive its crashed primary, which
// relies on POSIX detached process groups. Windows durable residency is unsupported
// (src/residency/host.ts, docs/residency-runtime.md), and there the nested child does
// not survive the primary's exit, so this descendant-tracking claim is not made.
it.skipIf(process.platform === "win32").each([
  ["project", false, "known"], ["session", false, "known"], ["project", true, "known"], ["session", true, "known"],
  ["project", true, "unknown"], ["session", true, "unknown"],
] as const)("a crashed tracked activation retains custody or legacy reconciliation IDs while its real nested writer survives, then collects once (%s scope, restart=%s, descendant=%s)", async (scope, restart, descendant) => {
  if (restart) installLegacyPrimaryExitReceipt();
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-nested-writer-retention-"));
  const rootId = "session:nested-writer-retention";
  const meshRoot = path.join(root, "mesh");
  const crash = path.join(root, "crash-primary");
  const release = path.join(root, "release-nested");
  const observation = path.join(root, "nested-observation.json");
  const config: ResidentHostConfig = {
    format: 1, rootId, sessionId: "nested-writer-retention", cwd: root, projectRoot: root,
    meshRoot, actorRoot: path.join(root, "actors"), residencyRoot: residentRoot(meshRoot, rootId),
    fullCodeMode: true, agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 }, mesh: DEFAULT_FABRIC_CONFIG.mesh,
    retention: DEFAULT_FABRIC_CONFIG.retention, workerPath: path.resolve("tests/fixtures/nested-writer-worker.ts"),
    fabricExtensionPath: path.resolve("dist/index.js"), piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
    piModels: { available: [{ provider: "fixture", id: "visible" }], aliases: {}, defaultModel: "fixture/visible" },
  };
  let host = new ResidentHost(config);
  const participants = mainParticipants(config);
  let client: ResidencyClient | undefined;
  let control: FabricControlPlane | undefined;
  let scanTime: number | undefined;
  let scans = 0;
  const nativeSweep = ResidentRequestRetention.prototype.sweep;
  const sweep = vi.spyOn(ResidentRequestRetention.prototype, "sweep").mockImplementation(function (this: ResidentRequestRetention, now, live, _budget, stoppedWritersGone) {
    nativeSweep.call(this, scanTime ?? now, live, 10_000, stoppedWritersGone);
    if (scanTime !== undefined) scans++;
  });
  let due: ReturnType<typeof vi.spyOn> | undefined;
  const remove = vi.spyOn(fs, "rmSync");
  try {
    await participants.start();
    await host.start();
    client = new ResidencyClient({ config, mesh: host.mesh, participants, mainAgent: { local: false } as FabricMainAgentTarget });
    control = new FabricControlPlane(host.mesh, { id: rootId, name: "main", kind: "main", sessionId: config.sessionId },
      { enabled: true, hostId: rootId, pollMs: 20, acknowledgementTimeoutMs: 5_000 });
    control.start(() => ({ accepted: false }));
    const request = { name: "nested stopped writer", instructions: "Reply.", residency: "durable" as const, scope,
      transport: "process" as const, responseMode: "text" as const, delivery: "mailbox" as const };
    const create = vi.spyOn(host.actors, "create");
    const actor = await client.createActor(request, AbortSignal.timeout(5_000));
    const requestId = fs.readdirSync(path.join(config.residencyRoot, "decisions")).map(name => name.slice(0, -5))
      .find(id => readResidentRequestDecision(config.residencyRoot, id)?.operation === "createActor")!;
    const decisionPath = path.join(config.residencyRoot, "decisions", `${requestId}.json`);
    const ackPath = path.join(config.residencyRoot, "acknowledgements", `${requestId}.json`);
    const committed = fs.readFileSync(decisionPath, "utf8");
    const ack = JSON.parse(fs.readFileSync(ackPath, "utf8"));
    const command: ResidentCommand = { format: 3, requestId, rootId, operation: "createActor", request, createdAt: ack.completedAt };
    await control.request(client.hostId, actor.id, "followUp", { message: "start nested writer", data: { primaryCrashPath: crash, nestedReleasePath: release, nestedObservationPath: observation } }, client.hostId);
    await waitFor(() => fs.existsSync(observation) && host.agents.listForUi().some(run => run.actorId === actor.id && "turns" in run && run.turns > 0));
    const writer = host.agents.listForUi().find(run => run.actorId === actor.id)!;
    const child = JSON.parse(fs.readFileSync(observation, "utf8")) as { id: string; pid: number; statusFile: string };
    expect(Number.isSafeInteger(child.pid) && child.pid > 0).toBe(true);
    expect(processAlive(child.pid)).toBe(true);
    expect(JSON.parse(fs.readFileSync(child.statusFile, "utf8"))).toMatchObject({ id: child.id, status: "running", turns: 4 });
    expect(await control.request(client.hostId, actor.id, "stop", {}, client.hostId)).toMatchObject({ acknowledged: true });
    // Crash only the primary. A dead primary is not a tree-exit receipt: the
    // process transport retains the observed nested writer and the actor drain.
    fs.writeFileSync(crash, "crash");
    await waitFor(() => !processAlive(Number(writer.sessionId)));
    if (restart) {
      // Legacy primary-only receipts can release the actor drain, but never
      // authorize removal of the still-live descendant's ownership evidence.
      await waitFor(() => host.agents.status(writer.id).status === "failed" && host.actors.inFlightCount() === 0);
      expect((await client.actorStatus(actor.id)).inFlightRun).toBeUndefined();
    } else {
      expect(await client.actorStatus(actor.id)).toMatchObject({ id: actor.id, status: "stopped", inFlightRun: { id: writer.id } });
      expect(host.actors.inFlightCount()).toBe(1);
      expect(host.agents.status(writer.id).status).toBe("running");
    }
    let settled = false;
    const settlement = host.agents.wait(writer.id).then(result => { settled = true; return result; });
    expect(processAlive(child.pid)).toBe(true);
    expect(host.agents.runDirectory(writer.id)).toBeDefined(); // Still tracked, disk-scan skip applies.
    expect(hasUnresolvedWorker(host.agents.runDirectory(writer.id)!)).toBe(false);
    await expect(host.agents.cleanup(writer.id)).rejects.toThrow(restart ? /descendant worker may still be running/ : /Cannot clean up a running agent/);
    expect(processAlive(child.pid)).toBe(true);

    scanTime = ack.acknowledgedAt + RESIDENT_REQUEST_RETENTION_MS + 1;
    due = vi.spyOn(ResidentRequestRetention.prototype, "due").mockReturnValue(true);
    await waitFor(() => scans > 0);
    due.mockReturnValue(false);
    expect(fs.existsSync(decisionPath)).toBe(true);
    expect(fs.readFileSync(decisionPath, "utf8")).toBe(committed);
    expect(JSON.parse(fs.readFileSync(ackPath, "utf8"))).toEqual(ack);
    expect(host.agents.retentionReferences().has(writer.id)).toBe(true);
    expect(host.agents.retentionReferences().has(actor.id)).toBe(true);
    const cancelled = new AbortController();
    registerResidentCancellation(cancelled.signal, config.residencyRoot, command);
    cancelled.abort();
    const outcome = cancellationError(cancelled.signal, new Error("late activation cancelled")) as Error & { residentOutcome: unknown };
    expect(outcome).toMatchObject({ code: "RESIDENT_REQUEST_EXPIRED", residentOutcome: {
      requestId, state: "committed", expired: true, operation: "createActor", entityKind: "actor", id: actor.id, ownerHostId: client.hostId,
    } });
    expect(outcome.message).toContain(actor.id);
    expect(outcome.message).toContain(client.hostId);
    expect(outcome.message).not.toContain("not yet known");
    expect(outcome.message).toMatch(/do not replay or reassign/i);
    expect(Object.isFrozen(outcome.residentOutcome)).toBe(true);
    expect(create).toHaveBeenCalledTimes(1);

    expect(settled, "only legacy primary-only receipts can settle before descendant exit").toBe(restart);
    if (restart) {
      const runDirectory = host.agents.runDirectory(writer.id)!;
      expect(config.agents.retainRuns).toBe(false);
      await control.close(); await client.close(); await host.close();
      // Teardown must not erase the dead primary's owning-actor evidence while
      // a descendant still owns the tree, even without an unresolved marker.
      expect(processAlive(child.pid)).toBe(true);
      expect(fs.existsSync(runDirectory)).toBe(true);
      expect(fs.existsSync(child.statusFile)).toBe(true);
      if (descendant === "unknown") {
        // Legacy native descendants may settle without a persisted sessionId.
        // The actual process is still live: terminal status is not exit evidence.
        const { sessionId: _pid, ...legacy } = JSON.parse(fs.readFileSync(child.statusFile, "utf8"));
        fs.writeFileSync(child.statusFile, JSON.stringify({ ...legacy, status: "completed" }));
        // Missing identity now vetoes collection as well as ownership release.
        // Keep the restart probe: neither startup nor maintenance may erase the
        // owning actor's reconciliation IDs before the child publishes exit.
        expect(canRemoveTerminalRun(runDirectory)).toBe(false);
      }
      const aged = new Date(Date.now() - RESIDENT_RUN_RETENTION_MS - 60_000);
      fs.utimesSync(runDirectory, aged, aged);
      host = new ResidentHost(config);
      await host.start(); // Acquires a new owner fence; scans previous owner's runs.
      expect(fs.existsSync(runDirectory)).toBe(true); // Startup cannot erase ownership before maintenance.
      expect(fs.existsSync(child.statusFile)).toBe(true);
      client = new ResidencyClient({ config, mesh: host.mesh, participants, mainAgent: { local: false } as FabricMainAgentTarget });
      const before = scans;
      scanTime = (scanTime ?? 0) + 60_001;
      due.mockReturnValue(true);
      await waitFor(() => scans > before);
      due.mockReturnValue(false);
      expect(host.agents.retentionReferences().has(actor.id)).toBe(true);
      expect(fs.readFileSync(decisionPath, "utf8")).toBe(committed);
      expect(JSON.parse(fs.readFileSync(ackPath, "utf8"))).toEqual(ack);
      const late = new AbortController();
      registerResidentCancellation(late.signal, config.residencyRoot, command);
      late.abort();
      expect(cancellationError(late.signal, new Error("late descendant settlement"))).toMatchObject({
        code: "RESIDENT_REQUEST_EXPIRED", residentOutcome: { requestId, id: actor.id, ownerHostId: client.hostId, expired: true },
      });
      expect(host.actors.status(actor.id)).toMatchObject({ id: actor.id, status: "stopped" });
    }

    fs.writeFileSync(release, "finish nested");
    await waitFor(() => !processAlive(child.pid));
    await waitFor(() => settled && host.actors.inFlightCount() === 0);
    expect(await settlement).toMatchObject({ status: "failed" });
    expect((restart ? host.actors.status(actor.id) : await client.actorStatus(actor.id)).inFlightRun).toBeUndefined();
    expect(JSON.parse(fs.readFileSync(child.statusFile, "utf8"))).toMatchObject({ status: "completed", turns: 5 });
    expect(host.agents.retentionReferences().has(actor.id)).toBe(false);
    const before = scans;
    scanTime = (scanTime ?? 0) + 60_001;
    due.mockReturnValue(true);
    await waitFor(() => scans > before && !fs.existsSync(decisionPath));
    due.mockReturnValue(false);
    expect(fs.existsSync(ackPath)).toBe(false);
    expect(remove.mock.calls.filter(([file]) => file === decisionPath)).toHaveLength(1);
    const collected = scans;
    scanTime = (scanTime ?? 0) + 60_001;
    due.mockReturnValue(true);
    await waitFor(() => scans > collected);
    due.mockReturnValue(false);
    expect(remove.mock.calls.filter(([file]) => file === decisionPath)).toHaveLength(1);
    expect(create).toHaveBeenCalledTimes(1);
  } finally {
    due?.mockRestore(); sweep.mockRestore(); remove.mockRestore();
    // Gates concern only these fixture processes. Join the child before closing
    // the host, including failures before the test observed its identity.
    fs.writeFileSync(release, "cleanup nested");
    fs.writeFileSync(crash, "cleanup primary");
    for (const run of host.agents?.listForUi() ?? []) await host.agents.stop(run.id);
    if (fs.existsSync(observation)) {
      const child = JSON.parse(fs.readFileSync(observation, "utf8")) as { pid: number };
      await waitFor(() => !processAlive(child.pid));
    }
    await control?.close(); await client?.close(); await host.close(); await participants.close();
    vi.restoreAllMocks();
    fs.rmSync(root, { recursive: true, force: true });
  }
}, 30_000);

it.skipIf(process.platform === "win32").each(["closed", "replacement"] as const)("public recovered durable cleanup retains a real live descendant through the client fallback (%s owner)", async owner => {
  installLegacyPrimaryExitReceipt();
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-durable-descendant-cleanup-"));
  const rootId = "session:durable-descendant-cleanup";
  const meshRoot = path.join(root, "mesh");
  const crash = path.join(root, "crash-primary");
  const release = path.join(root, "release-nested");
  const observation = path.join(root, "nested-observation.json");
  const config: ResidentHostConfig = {
    format: 1, rootId, sessionId: "durable-descendant-cleanup", cwd: root, projectRoot: root,
    meshRoot, actorRoot: path.join(root, "actors"), residencyRoot: residentRoot(meshRoot, rootId),
    fullCodeMode: true, agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 }, mesh: DEFAULT_FABRIC_CONFIG.mesh,
    retention: DEFAULT_FABRIC_CONFIG.retention, workerPath: path.resolve("tests/fixtures/nested-writer-worker.ts"),
    fabricExtensionPath: path.resolve("dist/index.js"), piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
    piModels: { available: [{ provider: "fixture", id: "visible" }], aliases: {}, defaultModel: "fixture/visible" },
  };
  const git = (...args: string[]) => execFileSync("git", args, { cwd: root, stdio: "ignore" });
  git("init", "-q"); git("config", "user.name", "Pi Fabric tests"); git("config", "user.email", "pi-fabric-tests@example.invalid");
  fs.writeFileSync(path.join(root, "README.md"), "fixture repository\n"); git("add", "README.md"); git("commit", "-qm", "initial");
  let host = new ResidentHost(config);
  const participants = mainParticipants(config);
  let client: ResidencyClient | undefined;
  try {
    await participants.start();
    await host.start();
    client = new ResidencyClient({ config, mesh: host.mesh, participants, mainAgent: { local: false } as FabricMainAgentTarget });
    const handle = await client.spawnAgent({
      task: JSON.stringify({ primaryCrashPath: crash, nestedReleasePath: release, nestedObservationPath: observation, primaryTerminalFailure: true }),
      residency: "durable", transport: "process", recursive: true, worktree: true,
    }, AbortSignal.timeout(5_000));
    await waitFor(() => fs.existsSync(observation));
    const child = JSON.parse(fs.readFileSync(observation, "utf8")) as { pid: number; statusFile: string };
    const run = host.agents.runDirectory(handle.id)!;
    const metadata = path.join(config.residencyRoot, "agents", `${handle.id}.json`);
    const result = path.join(config.residencyRoot, "results", `${handle.id}.json`);
    fs.writeFileSync(crash, "crash only primary");
    await waitFor(() => host.agents.status(handle.id).status === "failed" && fs.existsSync(result) && !processAlive(Number(handle.sessionId)));
    expect(processAlive(Number(handle.sessionId))).toBe(false);
    expect(processAlive(child.pid)).toBe(true);
    expect(hasUnresolvedWorker(run)).toBe(false);
    await client.close(); await host.close();
    for (const file of [run, child.statusFile, metadata, result]) expect(fs.existsSync(file), file).toBe(true);
    if (owner === "replacement") { host = new ResidentHost(config); await host.start(); }
    client = new ResidencyClient({ config, mesh: host.mesh, participants, mainAgent: { local: false } as FabricMainAgentTarget });
    const join = vi.spyOn(host.agents, "join");
    const committed = () => fs.readdirSync(path.join(config.residencyRoot, "decisions"))
      .map(name => JSON.parse(fs.readFileSync(path.join(config.residencyRoot, "decisions", name), "utf8")))
      .filter(decision => decision.operation === "cleanup" && decision.state === "committed");
    const output = fs.readFileSync(child.statusFile, "utf8");
    const saved = fs.readFileSync(result, "utf8");
    await expect(client.cleanupAgent(handle.id)).rejects.toThrow(/descendant worker may still be running/);
    expect(join).toHaveBeenCalledTimes(owner === "replacement" ? 1 : 0); // Real Unknown Fabric agent route.
    expect(processAlive(child.pid)).toBe(true);
    for (const file of [run, metadata, result, handle.worktree!]) expect(fs.existsSync(file), file).toBe(true);
    expect(execFileSync("git", ["branch", "--list", handle.branch!], { cwd: root, encoding: "utf8" })).toContain(handle.branch);
    expect(fs.readFileSync(child.statusFile, "utf8")).toBe(output);
    expect(fs.readFileSync(result, "utf8")).toBe(saved);
    expect(client.hasAgent(handle.id)).toBe(true);
    expect(committed()).toEqual([]); // Veto precedes the cancellation/commit fence.
    const { sessionId: _pid, ...legacy } = JSON.parse(output);
    fs.writeFileSync(child.statusFile, JSON.stringify({ ...legacy, status: "completed" }));
    await expect(client.cleanupAgent(handle.id)).rejects.toThrow(/unknown descendant identity/);
    expect(fs.existsSync(run)).toBe(true); expect(fs.existsSync(handle.worktree!)).toBe(true);
    expect(committed()).toEqual([]); // Missing identity fails closed even with terminal status.
    fs.writeFileSync(child.statusFile, output);
    fs.writeFileSync(release, "checked nested exit");
    await waitFor(() => !processAlive(child.pid));
    expect(JSON.parse(fs.readFileSync(child.statusFile, "utf8"))).toMatchObject({ status: "completed", turns: 5 });
    await expect(client.cleanupAgent(handle.id)).resolves.toEqual({ cleaned: true });
    for (const file of [run, metadata, result, handle.worktree!]) expect(fs.existsSync(file), file).toBe(false);
    expect(client.hasAgent(handle.id)).toBe(false);
    expect(committed()).toHaveLength(1);
  } finally {
    fs.writeFileSync(release, "cleanup nested"); fs.writeFileSync(crash, "cleanup primary");
    for (const run of host.agents?.listForUi() ?? []) await host.agents.stop(run.id);
    if (fs.existsSync(observation)) {
      const child = JSON.parse(fs.readFileSync(observation, "utf8")) as { pid: number };
      await waitFor(() => !processAlive(child.pid));
    }
    await client?.close(); await host.close(); await participants.close(); vi.restoreAllMocks();
    fs.rmSync(root, { recursive: true, force: true });
  }
}, 30_000);

it("retention ownership uses checked orphan exit evidence, not terminal status, and fails closed on unknown trees", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-orphan-writer-retention-"));
  const runs = path.join(root, "runs");
  const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 }, { runRoot: runs });
  // These are ownership assertions, not elapsed-time assertions. A loaded host can
  // exceed the conservative 5ms scan budget; exercise that veto explicitly below.
  const clock = vi.spyOn(performance, "now").mockReturnValue(0);
  const record = (id: string, actorId: string, sessionId: string, transport = "process") => {
    const run = path.join(runs, id);
    fs.mkdirSync(run, { recursive: true, mode: 0o700 });
    fs.writeFileSync(path.join(run, "status.json"), JSON.stringify({ id, actorId, transport, sessionId, status: "stopped" }), { mode: 0o600 });
    return run;
  };
  try {
    expect(manager.retentionReferences().size).toBe(0); // Proven absent run root, no writer.
    record("live", "live_actor", String(process.pid));
    let refs = manager.retentionReferences();
    expect(refs.has("live")).toBe(true);
    expect(refs.has("live_actor")).toBe(true); // A terminal file never overrides a live PID.
    record("live", "live_actor", "2147483647");
    expect(manager.retentionReferences().has("live_actor")).toBe(false);
    const unresolved = record("unresolved", "unresolved_actor", "2147483647");
    markUnresolvedWorker(unresolved, "worker exit unconfirmed");
    const nested = path.join(record("parent", "parent_actor", "2147483647"), "nested", "child");
    fs.mkdirSync(nested, { recursive: true });
    markUnresolvedWorker(nested, "nested worker exit unconfirmed");
    const unknownChild = path.join(record("unknown_child", "unknown_child_actor", "2147483647"), "nested", "child");
    fs.mkdirSync(unknownChild, { recursive: true });
    fs.writeFileSync(path.join(unknownChild, "status.json"), JSON.stringify({ transport: "process", status: "completed" }), { mode: 0o600 });
    record("external", "external_actor", "pane", "tmux");
    record("missing_pid", "missing_pid_actor", "");
    refs = manager.retentionReferences();
    for (const id of ["unresolved_actor", "parent_actor", "unknown_child_actor", "external_actor", "missing_pid_actor"]) expect(refs.has(id)).toBe(true);
    fs.mkdirSync(path.join(runs, "unknown"));
    expect(manager.retentionReferences().has("*")).toBe(true);
    fs.rmSync(path.join(runs, "unknown"), { recursive: true });
    clock.mockReturnValueOnce(0).mockReturnValue(10);
    expect(manager.retentionReferences().has("*")).toBe(true);
  } finally {
    clock.mockRestore();
    await manager.close();
    fs.rmSync(root, { recursive: true, force: true });
  }
});
