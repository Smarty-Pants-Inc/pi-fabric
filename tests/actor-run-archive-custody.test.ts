import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { hostedOnly } from "../scripts/hosted-only.mjs";
import { ActorLogStore, ActorRunArchiveRefusedError } from "../src/actors/log-store.js";
import { ActorManager } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { diagnostics } from "../src/core/diagnostics.js";
import { MeshStore } from "../src/mesh/store.js";

// Keep: KEEP-ALL-TRACES data safety at the real ActorManager -> archive -> AgentManager cleanup boundary.
// Only model work is replaced by the existing process worker fixture; permissions, copies, fsyncs and deletion are real.
interface Fixture { root: string; volume: string; mesh: MeshStore; actors: ActorManager; agents: AgentManager }
const fixtures: Fixture[] = [];
const HOSTED_ARCHIVES = hostedOnly("outside-TMPDIR actor run archive custody tests");
beforeEach(() => {
  for (const key of ["PI_FABRIC_DEPTH", "PI_FABRIC_BUDGET", "PI_FABRIC_BUDGET_FILE", "PI_FABRIC_BUDGET_ID",
    "PI_FABRIC_ACTOR_SESSION_FILE", "PI_FABRIC_RUN_ROOT"]) vi.stubEnv(key, undefined);
});
afterEach(async () => {
  for (const f of fixtures.splice(0)) {
    // Repair only this test's volume before teardown; refusal assertions happen before this repair.
    fs.chmodSync(f.volume, 0o700);
    await f.actors.close(); await f.agents.close();
    fs.rmSync(f.root, { recursive: true, force: true });
  }
  vi.restoreAllMocks(); vi.unstubAllEnvs();
});

function fixture(outsideTmp = false): Fixture {
  const root = fs.mkdtempSync(path.join(outsideTmp ? os.homedir() : os.tmpdir(), "actor-run-archive-custody-"));
  const volume = path.join(root, "volume"), meshRoot = path.join(volume, "mesh");
  fs.mkdirSync(volume, { mode: 0o700 });
  const mesh = new MeshStore(meshRoot, 64 * 1024, 100);
  const agents = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, runner: "pi", transport: "process",
    model: "test/archive", deniedModels: [], budgetUsd: 0, retainRuns: false, sessionExport: false, extensions: false,
  }, { runRoot: path.join(root, "runs"), meshRoot, workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
    preparePiModel: async model => model });
  const identity = { id: "session:archive-test", name: "Main", kind: "main" as const, sessionId: "archive-test" };
  const actors = new ActorManager(identity.sessionId, identity, mesh,
    { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {},
    { actorRoot: path.join(meshRoot, "actors"), persistent: true });
  const f = { root, volume, mesh, actors, agents };
  fixtures.push(f);
  return f;
}
const idle = (f: ReturnType<typeof fixture>) => vi.waitFor(() => expect(f.actors.inFlightCount()).toBe(0), { timeout: 10000 });
const create = (f: ReturnType<typeof fixture>) => f.actors.create({ name: "archive-test", instructions: "Answer briefly.",
  runner: "pi", transport: "process", extensions: false, model: "test/archive", protected: false });
const marker = (source: string) => path.join(source, "actor-run-archive-pending.json");

describe("actor archive custody through real filesystem cleanup", () => {
  it.skipIf(process.platform === "win32")("keeps a completed run and logs a typed refusal when its archive volume is 0777", async () => {
    const f = fixture(), actor = await create(f);
    const retain = vi.spyOn(ActorLogStore.prototype, "retainRun"), cleanup = vi.spyOn(f.agents, "cleanup");
    const logged = vi.spyOn(diagnostics, "error");
    // Own mesh anchors self-heal; a group-writable volume ancestor is deliberately not healable.
    fs.chmodSync(f.volume, 0o777);
    const result = await f.actors.ask(actor.id, "Complete this run."); await idle(f);
    const source = f.agents.runDirectory(result.runId!)!;
    expect(f.agents.status(result.runId!).status).toBe("completed");
    await expect(retain.mock.results[0]!.value).rejects.toBeInstanceOf(ActorRunArchiveRefusedError);
    expect(fs.existsSync(source)).toBe(true);
    expect(fs.readFileSync(path.join(source, "events.jsonl"), "utf8")).not.toBe("");
    expect(fs.existsSync(marker(source))).toBe(true);
    expect(cleanup.mock.calls.some(([id]) => id === result.runId)).toBe(false);
    expect(f.actors.status(actor.id).pendingRunArchives).toBe(1);
    expect(logged).toHaveBeenCalledWith("src/actors/manager.ts", expect.stringContaining("writable by group or others"));
  });

  it.skipIf(!HOSTED_ARCHIVES)("copies a verified archive before real cleanup removes a successful source", async () => {
    const f = fixture(true), actor = await create(f);
    const retain = vi.spyOn(ActorLogStore.prototype, "retainRun"), cleanup = vi.spyOn(f.agents, "cleanup");
    const result = await f.actors.ask(actor.id, "Complete this run."); await idle(f);
    const source = retain.mock.calls.find(([, id]) => id === result.runId)![2]!;
    await expect(retain.mock.results[0]!.value).resolves.toMatchObject({ archived: true, receipt: { runId: result.runId } });
    expect(fs.existsSync(source)).toBe(false);
    expect(cleanup).toHaveBeenCalledWith(result.runId);
    expect(JSON.parse(fs.readFileSync(path.join(actor.logDir!, result.runId!, "status.json"), "utf8"))).toMatchObject({ id: result.runId, status: "completed" });
    expect(fs.readFileSync(path.join(actor.logDir!, result.runId!, "events.jsonl"), "utf8")).not.toBe("");
    expect(f.actors.status(actor.id).pendingRunArchives).toBe(0);
  });

  it.skipIf(!HOSTED_ARCHIVES || process.platform === "win32")("keeps a refused prior run through later successful activations, then retries and cleans it", async () => {
    const f = fixture(true), actor = await create(f);
    const cleanup = vi.spyOn(f.agents, "cleanup");
    fs.chmodSync(f.volume, 0o777);
    const a = await f.actors.ask(actor.id, "First run."); await idle(f);
    const source = f.agents.runDirectory(a.runId!)!;
    const original = fs.readFileSync(path.join(source, "events.jsonl"), "utf8");
    fs.chmodSync(f.volume, 0o700);
    const refusedEntry = path.join(actor.logDir!, a.runId!, "events.jsonl");
    fs.mkdirSync(refusedEntry, { recursive: true });
    const b = await f.actors.ask(actor.id, "Second run."); await idle(f);
    const c = await f.actors.ask(actor.id, "Third run."); await idle(f);
    expect(fs.readFileSync(path.join(source, "events.jsonl"), "utf8")).toBe(original);
    expect(fs.existsSync(marker(source))).toBe(true);
    expect(cleanup.mock.calls.some(([id]) => id === a.runId)).toBe(false);
    for (const run of [b, c]) expect(fs.existsSync(path.join(actor.logDir!, run.runId!, "events.jsonl"))).toBe(true);
    expect(f.actors.status(actor.id).pendingRunArchives).toBe(1);
    fs.rmdirSync(refusedEntry);
    await f.actors.ask(actor.id, "Retry the pending archive."); await idle(f);
    expect(fs.existsSync(source)).toBe(false);
    expect(fs.readFileSync(refusedEntry, "utf8")).toBe(original);
    expect(cleanup).toHaveBeenCalledWith(a.runId);
    expect(f.actors.status(actor.id).pendingRunArchives).toBe(0);
  }, 30000);

  it.skipIf(!HOSTED_ARCHIVES || process.platform === "win32").each([false, true])("close keeps custody unless the archive really succeeds (repair=%s)", async repair => {
    const f = fixture(true), actor = await create(f);
    const retain = vi.spyOn(ActorLogStore.prototype, "retainRun");
    const logged = vi.spyOn(diagnostics, "error");
    fs.chmodSync(f.volume, 0o777);
    const a = await f.actors.ask(actor.id, "Complete before close."); await idle(f);
    const source = f.agents.runDirectory(a.runId!)!;
    const original = fs.readFileSync(path.join(source, "events.jsonl"), "utf8");
    retain.mockClear(); logged.mockClear();
    if (repair) fs.chmodSync(f.volume, 0o700);
    await f.actors.close(); await f.agents.close();
    expect(retain.mock.calls.some(([, id]) => id === a.runId)).toBe(true);
    if (repair) {
      await expect(retain.mock.results[0]!.value).resolves.toMatchObject({ archived: true });
      expect(fs.existsSync(source)).toBe(false);
      expect(fs.readFileSync(path.join(actor.logDir!, a.runId!, "events.jsonl"), "utf8")).toBe(original);
    } else {
      await expect(retain.mock.results[0]!.value).rejects.toMatchObject({ code: "FABRIC_ACTOR_RUN_ARCHIVE_REFUSED" });
      expect(fs.readFileSync(path.join(source, "events.jsonl"), "utf8")).toBe(original);
      expect(fs.existsSync(marker(source))).toBe(true);
      expect(f.agents.actorArchiveSources(actor.id, actor.sessionFile!).get(a.runId!)).toBe(source);
      expect(f.actors.status(actor.id).pendingRunArchives).toBe(1);
      expect(logged).toHaveBeenCalledWith("src/actors/manager.ts", expect.stringContaining("source retained for retry"));
    }
  });

  it("does not treat an explicit no-source result as cleanup or custody-discharge authority", async () => {
    const f = fixture(), actor = await create(f);
    vi.spyOn(ActorLogStore.prototype, "retainRun").mockResolvedValue({ archived: false, reason: "no-source" });
    const cleanup = vi.spyOn(f.agents, "cleanup"), commit = vi.spyOn(f.agents, "commitActorArchive");
    const a = await f.actors.ask(actor.id, "Complete without an archive receipt."); await idle(f);
    const source = f.agents.runDirectory(a.runId!)!;
    expect(fs.existsSync(path.join(source, "events.jsonl"))).toBe(true);
    expect(fs.existsSync(marker(source))).toBe(true);
    expect(cleanup).not.toHaveBeenCalled(); expect(commit).not.toHaveBeenCalled();
  });
});
