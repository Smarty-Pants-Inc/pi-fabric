import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorManager, FABRIC_ACTOR_ACTIVATION_ALARM_TOPIC } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { writeJsonAtomic } from "../src/core/atomic-write.js";
import { MeshStore } from "../src/mesh/store.js";
import { heapRunResult } from "./fixtures/manager-heap-probe.js";
import type { AgentRunResult } from "../src/agents/types.js";

// Keep: #1704 pages-lead stopped-run catch and native/result data safety.
// A genuinely new AgentManager reads the old stopped actor and its actual run
// files. No execution, signals, trace cleanup, terminal-record stand-in or replay.
vi.mock("node:child_process", async importOriginal => ({
  ...await importOriginal<typeof import("node:child_process")>(),
  spawn: vi.fn(() => { throw new Error("Unexpected launch during stopped-actor recovery"); }),
}));
const closers: Array<() => Promise<void>> = [];
afterEach(async () => {
  try { for (const close of closers.splice(0).reverse()) await close(); }
  finally { vi.restoreAllMocks(); }
  // Preserve fixture records and raw traces in the test owner's private TMPDIR.
});
const eventually = (check: () => void) => vi.waitFor(check, { timeout: 3_000, interval: 10 });
const pause = () => new Promise<void>(resolve => setTimeout(resolve, 100));
const pid = 2147483646;
const replacementPid = 2147483644;
type RecoverySnapshot = (record: Record<string, unknown>) => Record<string, unknown>;
const runId = "d8caf331babd44f89cf7d4026cb6917e";
const activationId = "recorded-stopped-activation";

async function fixture(options: { alive?: boolean; unknownReplacement?: boolean; recoverySnapshot?: RecoverySnapshot; status?: AgentRunResult["status"]; savedAnswer?: boolean; persistedInFlight?: boolean } = {}) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-stopped-recovery-"));
  const actorRoot = path.join(root, "actors"), runRoot = path.join(root, "runs");
  const identity = { id: "session:stopped-recovery", name: "main", kind: "main" as const, sessionId: "stopped-recovery" };
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const config = { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, retainRuns: true, sessionExport: false };
  const agentOptions = { runRoot, workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fullCodeMode: false };
  const actorOptions = { actorRoot, persistent: true, retentionSweepMs: 20, closeGraceMs: 10 };
  const meshConfig = { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 };
  const oldAgents = new AgentManager(root, config, agentOptions);
  const oldActors = new ActorManager("stopped-recovery", identity, mesh, meshConfig, oldAgents, () => {}, actorOptions);
  const actor = await oldActors.create({ name: "legacy-stopped", instructions: "Work", extensions: false });
  await oldActors.stop(actor.id);
  await oldActors.close();
  await oldAgents.close();

  const directory = path.join(runRoot, runId);
  fs.mkdirSync(directory, { recursive: true });
  const record = { ...heapRunResult(runId), actorId: actor.id, status: options.status ?? "running",
    sessionId: String(pid), startedAt: Date.now() - 31 * 60_000, updatedAt: Date.now() - 60_000,
    ...(options.status ? { finishedAt: Date.now() - 30_000 } : { finishedAt: undefined }), text: "original checkpoint" };
  writeJsonAtomic(path.join(directory, "status.json"), record);
  fs.writeFileSync(path.join(directory, "task.txt"), JSON.stringify({ actorId: actor.id, id: activationId }, null, 2));
  fs.writeFileSync(path.join(directory, "events.jsonl"), '{"type":"recorded-native-trace","text":"retain verbatim"}\n');
  fs.writeFileSync(actor.sessionFile!, '{"type":"session","id":"stopped-recovery-session"}\n');
  // Native observation debt and its actor archive obligation must survive even
  // when primary absence permits logical stopped-result recovery.
  writeJsonAtomic(path.join(directory, "unresolved-worker.json"), { reason: "native observation unknown", markedAt: Date.now() });
  writeJsonAtomic(path.join(directory, "actor-run-archive-pending.json"), {
    format: 1, runId, actorId: actor.id, sessionFile: actor.sessionFile,
  });
  if (options.unknownReplacement || options.recoverySnapshot) {
    writeJsonAtomic(path.join(directory, "archive-pending.json"), { format: 1, awaitingResult: true,
      stopIntent: { reason: "explicit", requestedAt: Date.now(), recoveryRecord: options.recoverySnapshot?.(record) ?? {
        id: runId, actorId: actor.id, transport: "process", status: "running",
        // No PID: a pending replacement must not inherit the old dead PID.
      } } });
  }
  if (options.savedAnswer) writeJsonAtomic(path.join(directory, "settlement.json"), {
    version: 1, runId, outcome: "completed", text: "saved exact answer",
  });
  const registryFile = path.join(actorRoot, "actors.json");
  const registry = JSON.parse(fs.readFileSync(registryFile, "utf8"));
  Object.assign(registry.actors.find((row: { id: string }) => row.id === actor.id), {
    lastRunId: runId, ...(options.persistedInFlight !== false ? { inFlightRun: { id: runId, startedAt: record.startedAt } } : {}),
  });
  writeJsonAtomic(registryFile, registry);
  const retained = ["events.jsonl", "task.txt", "unresolved-worker.json", "actor-run-archive-pending.json",
    ...(options.savedAnswer ? ["settlement.json"] : [])].map(file => path.join(directory, file));
  retained.push(actor.sessionFile!);
  const before = retained.map(file => fs.readFileSync(file));
  const checkRetained = () => retained.forEach((file, index) => expect(fs.readFileSync(file)).toEqual(before[index]));

  const primary = { alive: options.alive ?? false }, replacement = { alive: true };
  const kill = process.kill.bind(process);
  vi.spyOn(process, "kill").mockImplementation((candidate, signal) => {
    if (candidate !== pid && candidate !== replacementPid) return kill(candidate, signal);
    if (signal !== 0) throw new Error("Offline recovery must never signal the recorded worker");
    if (!(candidate === pid ? primary : replacement).alive) throw Object.assign(new Error("recorded ESRCH"), { code: "ESRCH" });
    return true;
  });
  const launch = vi.spyOn(ProcessTransport.prototype, "launch");
  const agents = new AgentManager(root, config, agentOptions); // Fresh host: no live run handle.
  closers.push(() => agents.close());
  const delivered = vi.fn();
  const actors = new ActorManager("stopped-recovery", identity, mesh, meshConfig, agents, delivered, actorOptions);
  closers.push(() => actors.close());
  const readRecord = () => JSON.parse(fs.readFileSync(path.join(directory, "status.json"), "utf8"));
  const readIntent = () => JSON.parse(fs.readFileSync(path.join(directory, "archive-pending.json"), "utf8")).stopIntent;
  const stuckAlarms = () => mesh.read({ topic: FABRIC_ACTOR_ACTIVATION_ALARM_TOPIC, limit: 100 })
    .filter(event => event.kind === "actor-run-stuck" && (event.data as { actorId?: string }).actorId === actor.id);
  return { actor, agents, actors, oldAgents, directory, primary, replacement, record, readRecord, readIntent, checkRetained, launch, delivered, stuckAlarms };
}

describe("legacy stopped actors on a fresh host (#1704)", () => {
  it.each([false, true])("settles a dead checkpoint through durable manager publication, without replay or trace changes (persisted pointer: %s)", async persistedInFlight => {
    const f = await fixture({ persistedInFlight });
    expect(f.agents).not.toBe(f.oldAgents);
    expect(f.agents.hasRunCustody(runId)).toBe(false);
    await eventually(() => expect(f.agents.status(runId)).toMatchObject({ id: runId, status: "stopped", error: "Agent stopped" }));
    expect(f.readRecord()).toMatchObject({ id: runId, status: "stopped", stopIntent: {
      reason: "explicit", path: "actors.stop", by: { kind: "main", id: "session:stopped-recovery" },
    } });
    expect(f.actors.status(f.actor.id)).toMatchObject({ status: "stopped", lastRunId: runId });
    expect(f.actors.status(f.actor.id).inFlightRun).toBeUndefined();
    expect(f.agents.retentionCustodyVeto(runId)).toBe(true); // Logical settlement is NOT native custody release.
    expect(f.stuckAlarms()).toEqual([]);
    expect(f.delivered).not.toHaveBeenCalled();
    expect(f.launch).not.toHaveBeenCalled();
    f.checkRetained();
  });

  it.each([
    { state: "live primary", alive: true },
    { state: "unknown replacement", unknownReplacement: true },
    { state: "unknown replacement with live old checkpoint", alive: true, unknownReplacement: true },
  ])("retains a $state", async ({ state, ...options }) => {
    const f = await fixture(options);
    await pause();
    expect(f.readRecord().status).toBe("running");
    expect(fs.existsSync(path.join(f.directory, "manager-result.json"))).toBe(false);
    expect(() => f.agents.status(runId)).toThrow(); // No authenticated terminal result was adopted.
    expect(f.agents.retentionCustodyVeto(runId)).toBe(true);
    expect(f.actors.status(f.actor.id).status).toBe("stopped");
    expect(f.actors.status(f.actor.id).inFlightRun?.id).toBe(runId); // Retained while liveness/replacement identity is unknown.
    expect(f.stuckAlarms()).toEqual([]);
    expect(f.delivered).not.toHaveBeenCalled();
    expect(f.launch).not.toHaveBeenCalled();
    f.checkRetained();
    if (state === "live primary") {
      f.primary.alive = false;
      // A real run-file event wakes the existing durable-stop observer.
      writeJsonAtomic(path.join(f.directory, "status.json"), f.record);
      await eventually(() => expect(f.agents.status(runId)).toMatchObject({ status: "stopped" }));
      f.checkRetained();
    }
  });

  const unusableSnapshots: Array<[string, RecoverySnapshot]> = [
    ["empty snapshot", () => ({})],
    ["missing id", ({ id: _id, ...rest }) => rest],
    ["mismatching id", record => ({ ...record, id: "f".repeat(32) })],
    ["missing actorId", ({ actorId: _actorId, ...rest }) => rest],
    ["mismatching actorId", record => ({ ...record, actorId: "other-actor" })],
    ["missing transport", ({ transport: _transport, ...rest }) => rest],
    ["mismatching transport", record => ({ ...record, transport: "localterm" })],
  ];
  it.each(unusableSnapshots.flatMap(([state, recoverySnapshot]) => [false, true].map(alive => ({
    state, recoverySnapshot, alive,
  }))))("retains an unusable $state instead of inheriting the old checkpoint (old alive: $alive)", async ({ alive, recoverySnapshot }) => {
    const f = await fixture({ alive, recoverySnapshot });
    await pause();
    expect(f.readRecord().status).toBe("running");
    expect(fs.existsSync(path.join(f.directory, "manager-result.json"))).toBe(false);
    expect(() => f.agents.status(runId)).toThrow();
    expect(f.readIntent()).toMatchObject({ reason: "explicit", path: "actors.stop",
      by: { kind: "main", id: "session:stopped-recovery" } });
    expect(f.readIntent().recoveryRecord).toEqual(recoverySnapshot(f.record));
    expect(f.agents.retentionCustodyVeto(runId)).toBe(true);
    expect(f.actors.status(f.actor.id)).toMatchObject({ status: "stopped", inFlightRun: { id: runId } });
    expect(f.stuckAlarms()).toEqual([]);
    expect(f.delivered).not.toHaveBeenCalled();
    expect(f.launch).not.toHaveBeenCalled();
    f.checkRetained();
  });

  it.each([false, true])("joins the current live snapshot, not the old checkpoint (old alive: %s)", async alive => {
    const f = await fixture({ alive, recoverySnapshot: record => ({ ...record, sessionId: String(replacementPid) }) });
    await pause();
    expect(f.readRecord().status).toBe("running");
    expect(fs.existsSync(path.join(f.directory, "manager-result.json"))).toBe(false);
    expect(() => f.agents.status(runId)).toThrow();
    expect(f.agents.retentionCustodyVeto(runId)).toBe(true);
    expect(f.actors.status(f.actor.id).inFlightRun?.id).toBe(runId);
    f.replacement.alive = false;
    // Only the replacement's exit discharges logical custody, even if the old
    // PID remains alive. A run-file event wakes the existing identity observer.
    writeJsonAtomic(path.join(f.directory, "status.json"), f.record);
    await eventually(() => expect(f.agents.status(runId)).toMatchObject({ status: "stopped" }));
    expect(f.actors.status(f.actor.id).inFlightRun).toBeUndefined();
    expect(f.agents.retentionCustodyVeto(runId)).toBe(true);
    expect(f.stuckAlarms()).toEqual([]);
    expect(f.delivered).not.toHaveBeenCalled();
    expect(f.launch).not.toHaveBeenCalled();
    f.checkRetained();
  });

  it.each(["completed", "failed", "stopped", "timed_out"] as const)("keeps a preexisting %s outcome rather than rewriting it as stopped", async status => {
    const f = await fixture({ status });
    await eventually(() => expect(f.agents.status(runId)).toMatchObject({
      id: runId, status, text: f.record.text, finishedAt: f.record.finishedAt,
    }));
    expect(f.readRecord()).toMatchObject({ status, text: f.record.text, finishedAt: f.record.finishedAt });
    expect(f.launch).not.toHaveBeenCalled();
    f.checkRetained();
  });

  it("preserves a saved completion behind a running checkpoint, without delivering it to a stopped actor", async () => {
    const f = await fixture({ savedAnswer: true });
    await eventually(() => expect(f.agents.status(runId)).toMatchObject({ status: "completed", text: "saved exact answer" }));
    expect(f.delivered).not.toHaveBeenCalled();
    expect(f.launch).not.toHaveBeenCalled();
    f.checkRetained();
  });

  it("a saved completion short-circuits durable-stop replay before a later stopped attempt of the same activation", async () => {
    const f = await fixture({ alive: true });
    // This actor's stopped-host observer stays on the deliberately live run;
    // the two separate attempts below exercise the ordinary active-actor API.
    const actorId = "other-recorded-actor";
    const savedId = "1".repeat(32), stoppedId = "2".repeat(32);
    for (const id of [savedId, stoppedId]) {
      const directory = path.join(path.dirname(f.directory), id);
      fs.mkdirSync(directory, { recursive: true });
      fs.writeFileSync(path.join(directory, "task.txt"), JSON.stringify({ actorId, id: activationId }, null, 2));
      writeJsonAtomic(path.join(directory, "status.json"), { ...heapRunResult(id), actorId,
        status: id === savedId ? "running" : "stopped", sessionId: "2147483645" });
      writeJsonAtomic(path.join(directory, "archive-pending.json"), { format: 1,
        stopIntent: { reason: "explicit", requestedAt: Date.now() } });
      if (id === savedId) writeJsonAtomic(path.join(directory, "settlement.json"), {
        version: 1, runId: id, outcome: "completed", text: "first attempt saved completion",
      });
    }
    expect(await f.agents.run({ task: "must recover, never launch", actorId, activationId, transport: "process" }))
      .toMatchObject({ id: savedId, status: "completed", text: "first attempt saved completion" });
    expect(f.launch).not.toHaveBeenCalled();
    f.checkRetained();
  });
});
