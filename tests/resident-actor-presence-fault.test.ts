import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { expect, it, vi } from "vitest";
import { ActorDirectory } from "../src/actors/directory.js";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { flushFabricDiagnostics } from "../src/core/diagnostics.js";
import { ResidentHost } from "../src/residency/host.js";
import type { ResidentHostConfig } from "../src/residency/protocol.js";
import { ParticipantDirectory, type ParticipantSnapshotSource } from "../src/topology/participant-directory.js";
import { fakeManagerHeapWorker } from "./fixtures/manager-heap-probe.js";

// Keep: the actual resident registration/ActorDirectory failure boundary.
// The companion strict-reader regression authenticates a hard-linked terminal result;
// here a strict-reader error cannot affect inventory while a real activation waits
// for a manager permit. A real presence-source failure still isolates and recovers.
// Worker execution alone is fake; no process is launched or signalled.
it("resident inventory avoids strict result reads but retains real source failures privately", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "resident-actor-presence-fault-"));
  vi.stubEnv("SMARTY_ROLE", "worktree-agent@x");
  vi.stubEnv("PI_CODING_AGENT_DIR", path.join(root, "agent"));
  vi.stubEnv("PI_FABRIC_SESSION_ID", undefined);
  const config: ResidentHostConfig = {
    format: 1, rootId: "session:resident-presence-fault", sessionId: "resident-presence-fault",
    cwd: root, projectRoot: root, meshRoot: path.join(root, "mesh"), actorRoot: path.join(root, "actors"),
    sessionActorRoot: path.join(root, "session-actors"), residencyRoot: path.join(root, "resident"),
    fullCodeMode: false, agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, retainRuns: true,
      maxConcurrent: 1, sessionExport: false, notifyOnComplete: false },
    mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, retention: DEFAULT_FABRIC_CONFIG.retention,
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fabricExtensionPath: path.resolve("dist/index.js"),
    piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
    piModels: { available: [{ provider: "fixture", id: "visible" }], aliases: {}, defaultModel: "fixture/visible" },
  };
  const deferred = () => {
    let resolve!: () => void;
    const promise = new Promise<void>(done => { resolve = done; });
    return { promise, resolve };
  };
  const blockerExit = deferred(), queued = deferred();
  let blockerAlive = true, activationId: string | undefined;
  const sources = new Map<string, ParticipantSnapshotSource>();
  const register = ParticipantDirectory.prototype.registerSource;
  vi.spyOn(ParticipantDirectory.prototype, "registerSource").mockImplementation(function (this: ParticipantDirectory, source, options) {
    if (options?.name) sources.set(options.name, source);
    return register.call(this, source, options);
  });
  vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(request => fakeManagerHeapWorker(request, 0, false,
    request.name === "resident permit holder" ? {
      closed: blockerExit.promise, alive: () => blockerAlive,
      stop: async () => { blockerAlive = false; blockerExit.resolve(); },
    } : undefined));
  const host = new ResidentHost(config, () => {});
  let outcome: Promise<unknown> | undefined;
  let restoreReader: (() => void) | undefined;
  try {
    await host.start();
    expect(host.actors).toBeInstanceOf(ActorDirectory);
    const damaged = await host.actors.create({ name: "resident preparing actor", instructions: "Reply.", residency: "durable",
      transport: "process", responseMode: "text", scope: "project" }, { asRegistryOwner: true });
    const healthy = await host.actors.create({ name: "resident healthy actor", instructions: "Wait.", residency: "durable",
      scope: "project" }, { asRegistryOwner: true });
    const otherScope = await host.actors.create({ name: "resident session actor", instructions: "Wait.", residency: "durable",
      scope: "session" }, { asRegistryOwner: true });
    const blocker = await host.agents.spawn({ name: "resident permit holder", task: "hold the only permit", transport: "process", extensions: false });
    const run = host.agents.run;
    vi.spyOn(host.agents, "run").mockImplementation(function (this: AgentManager, ...args) {
      const onQueued = args[5];
      if (args[0].actorId === damaged.id) args[5] = handle => {
        activationId = handle.id; onQueued?.(handle); queued.resolve();
      };
      return run.apply(this, args);
    });
    const activation = host.actors.ask(damaged.id, "a genuinely queued resident activation");
    outcome = activation.then(value => ({ value }), error => ({ error }));
    await queued.promise;
    expect(host.actors.status(damaged.id)).toMatchObject({ status: "waiting", preparing: { runId: activationId } });
    expect(host.actors.listOwned(true, (_id, error) => { throw error; }).find(actor => actor.id === damaged.id))
      .toMatchObject({ preparing: { runId: activationId, queuePosition: 1 } });
    await host.participants.refresh();
    const status = host.agents.status;
    const integrityError = Object.assign(new Error("resident preparing result is unauthenticated"), { code: "FABRIC_MISSING_RESULT" });
    const strictReader = vi.spyOn(host.agents, "status").mockImplementation(function (this: AgentManager, id) {
      if (id === activationId) throw integrityError;
      return status.call(this, id);
    });
    restoreReader = () => strictReader.mockRestore();
    const reportFailure = vi.fn();
    expect([...sources.keys()].sort()).toEqual(["actors", "agents"]);
    const actorSource = sources.get("actors")!;
    expect(actorSource(reportFailure).map(record => record.id).sort()).toEqual([damaged.id, healthy.id, otherScope.id].sort());
    expect(reportFailure).not.toHaveBeenCalled();
    expect(strictReader).not.toHaveBeenCalled();
    expect(() => host.actors.list()).toThrow(integrityError);
    expect(() => host.actors.listOwned()).toThrow(integrityError);
    expect(() => host.actors.listOwned(true)).toThrow(integrityError);
    expect(() => host.actors.status(damaged.id)).toThrow(integrityError);
    strictReader.mockClear();
    await expect(host.participants.refresh()).resolves.toBeUndefined();
    expect(strictReader).not.toHaveBeenCalled();
    expect(host.participants.isolatedSourceFailures()).toEqual([]);
    for (const actor of [damaged, healthy, otherScope]) expect(host.participants.get(actor.id, undefined, { fresh: true }))
      .toMatchObject({ id: actor.id, kind: "actor", ownerHostId: host.hostId });
    strictReader.mockRestore();

    // A failed result-free queue lookup omits only actors needing that lookup,
    // including across the project/session directory and publication batch boundary.
    const sourceError = Object.assign(new Error("EACCES: presence inventory unavailable"), { code: "EACCES" });
    const presenceReader = vi.spyOn(host.agents, "listForPresence").mockImplementation(() => { throw sourceError; });
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const error = vi.spyOn(console, "error").mockImplementation(() => {});
    const stderr = vi.spyOn(process.stderr, "write").mockReturnValue(true);
    expect(actorSource(reportFailure).map(record => record.id).sort()).toEqual([healthy.id, otherScope.id].sort());
    expect(reportFailure).toHaveBeenCalledExactlyOnceWith(sourceError, damaged.id);
    expect(() => host.actors.listOwned(true, (_id, failure) => { throw failure; })).toThrow(sourceError);
    const batch = host.actors.presenceBatch(true);
    expect(batch.actorIds).not.toContain(damaged.id);
    for (const actor of [healthy, otherScope]) {
      const updated = await host.actors.setInstructions(actor.id, "Updated healthy instructions.");
      expect(host.mesh.get(`actors/${config.sessionId}/${actor.id}`)?.value)
        .toMatchObject({ id: actor.id, instructionsDigest: updated.instructionsDigest });
    }
    await expect(host.participants.refresh()).resolves.toBeUndefined();
    expect(host.participants.isolatedSourceFailures()).toContainEqual({ source: "actors", participantId: damaged.id,
      reason: "participant snapshot source failed", since: expect.any(Number) });
    expect(host.participants.canConsumeMesh()).toBe(true);
    expect(host.participants.get(damaged.id, undefined, { fresh: true })).toBeUndefined();
    for (const actor of [healthy, otherScope]) expect(host.participants.get(actor.id, undefined, { fresh: true }))
      .toMatchObject({ id: actor.id, kind: "actor", ownerHostId: host.hostId });
    expect(host.actors.listOwned(true).map(actor => actor.id)).toContain(damaged.id);
    expect(host.actors.status(damaged.id)).toMatchObject({ preparing: { runId: activationId, queuePosition: 1 } });
    await flushFabricDiagnostics();
    const directory = path.join(root, "agent", "fabric", "diagnostics");
    const diagnostics = fs.readdirSync(directory).filter(file => file.endsWith(".jsonl")).flatMap(file =>
      fs.readFileSync(path.join(directory, file), "utf8").trim().split("\n").filter(Boolean).map(line => JSON.parse(line)));
    for (const source of ["src/topology/participant-directory.ts", "src/actors/manager.ts"])
      expect(diagnostics).toContainEqual(expect.objectContaining({ level: "warning", source,
        message: expect.stringContaining(sourceError.message) }));
    expect(warn).not.toHaveBeenCalled();
    expect(error).not.toHaveBeenCalled();
    expect(stderr).not.toHaveBeenCalled();
    presenceReader.mockRestore();
    // Same queued actor, same id and preparing state: no fabricated fallback row
    // and no lifecycle change are needed for the next good source to return it.
    expect(host.actors.status(damaged.id)).toMatchObject({ status: "waiting", preparing: { runId: activationId } });
    reportFailure.mockClear();
    expect(actorSource(reportFailure).map(record => record.id)).toContain(damaged.id);
    expect(reportFailure).not.toHaveBeenCalled();
    await host.participants.refresh();
    expect(host.participants.get(damaged.id, undefined, { fresh: true })).toMatchObject({ id: damaged.id, kind: "actor" });
    expect(host.participants.isolatedSourceFailures()).toEqual([]);
    blockerAlive = false; blockerExit.resolve();
    await expect(host.agents.wait(blocker.id)).resolves.toMatchObject({ status: "completed" });
    expect(await outcome).not.toHaveProperty("error");
    expect(host.agents.isSettled(activationId!)).toBe(true);
  } finally {
    restoreReader?.();
    blockerAlive = false; blockerExit.resolve();
    await outcome;
    await host.close();
    await flushFabricDiagnostics();
    vi.restoreAllMocks();
    vi.unstubAllEnvs();
    fs.rmSync(root, { recursive: true, force: true });
  }
}, 30_000);
