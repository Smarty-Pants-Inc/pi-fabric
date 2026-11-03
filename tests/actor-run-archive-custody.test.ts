import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ActorLogStore, ActorRunArchiveRefusedError } from "../src/actors/log-store.js";
import { ActorManager } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { diagnostics } from "../src/core/diagnostics.js";
import { MeshStore } from "../src/mesh/store.js";
import { fleetSessionArchive } from "../src/storage/session-archive.js";

// Keep: KEEP-ALL-TRACES data safety at the real ActorManager -> archive -> AgentManager cleanup boundary.
// Only model work is replaced by the existing process worker fixture; permissions, copies, fsyncs and deletion are real.
interface Fixture { root: string; volume: string; mesh: MeshStore; actors: ActorManager; agents: AgentManager }
const fixtures: Fixture[] = [];
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

function fixture(persistent = true): Fixture {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-run-archive-custody-"));
  const volume = path.join(root, "volume"), meshRoot = path.join(volume, "mesh");
  fs.mkdirSync(volume, { mode: 0o700 });
  const mesh = new MeshStore(meshRoot, 64 * 1024, 100);
  const agents = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, runner: "pi", transport: "process",
    model: "test/archive", deniedModels: [], budgetUsd: 0, retainRuns: false, sessionExport: false, extensions: false,
  }, { runRoot: path.join(root, "runs"), workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
    preparePiModel: async model => model });
  const identity = { id: "session:archive-test", name: "Main", kind: "main" as const, sessionId: "archive-test" };
  const actors = new ActorManager(identity.sessionId, identity, mesh,
    { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {},
    { actorRoot: path.join(meshRoot, "actors"), persistent });
  const f = { root, volume, mesh, actors, agents };
  fixtures.push(f);
  return f;
}
const idle = (f: ReturnType<typeof fixture>) => vi.waitFor(() => expect(f.actors.inFlightCount()).toBe(0), { timeout: 10000 });
const create = (f: ReturnType<typeof fixture>) => f.actors.create({ name: "archive-test", instructions: "Answer briefly.",
  runner: "pi", transport: "process", extensions: false, model: "test/archive", protected: false });
const marker = (source: string) => path.join(source, "actor-run-archive-pending.json");

// Buffer inventories compare every byte, including unknown/nested files, not just the named trace formats.
function traceBytes(directory: string): Record<string, Buffer> {
  const files: Record<string, Buffer> = {};
  const visit = (relative: string): void => {
    for (const entry of fs.readdirSync(path.join(directory, relative), { withFileTypes: true })) {
      const name = path.join(relative, entry.name);
      if (entry.isDirectory()) visit(name);
      else {
        expect(entry.isFile()).toBe(true);
        files[name] = fs.readFileSync(path.join(directory, name));
      }
    }
  };
  visit("");
  return files;
}

function seedTrace(directory: string, relative: string): void {
  const file = path.join(directory, relative);
  fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
  // Distinct bytes per path, a multibyte character, NUL and an incomplete final line must survive unchanged.
  fs.writeFileSync(file, Buffer.from(`${relative}: trace π\n\0unfinished`, "utf8"), { flag: "wx", mode: 0o600 });
}

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

  it("copies a verified archive before real cleanup removes a successful source", async () => {
    const f = fixture(), actor = await create(f);
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

  it.skipIf(process.platform === "win32")("keeps a refused prior run through later successful activations, then retries and cleans it", async () => {
    const f = fixture(), actor = await create(f);
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

  it.skipIf(process.platform === "win32").each([false, true])("close keeps custody unless the archive really succeeds (repair=%s)", async repair => {
    const f = fixture(), actor = await create(f);
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

  // Keep: data safety at the actor-tree custody boundary, including a REFUSED run still held by AgentManager.
  // POSIX mode-bit refusal is intentionally skipped on Windows, as in the existing refusal flow above.
  it.skipIf(process.platform === "win32").each(["remove", "close"] as const)(
    "%s moves the whole actor tree and keeps every refused-run source byte even with retainRuns:false", async operation => {
      const f = fixture(operation === "remove"), actor = await create(f);
      const retain = vi.spyOn(ActorLogStore.prototype, "retainRun"), cleanup = vi.spyOn(f.agents, "cleanup");
      expect(f.agents.config.retainRuns).toBe(false); // Existing explicit operator opt-out, not a test-only retention override.
      const completed = await f.actors.ask(actor.id, "Archive this first run."); await idle(f);
      const completedDirectory = path.join(actor.logDir!, completed.runId!);
      const completedSource = retain.mock.calls.find(([, id]) => id === completed.runId)![2]!;
      expect(fs.existsSync(completedSource)).toBe(false);
      expect(cleanup).toHaveBeenCalledWith(completed.runId);
      expect(fs.readFileSync(path.join(completedDirectory, "events.jsonl")).length).toBeGreaterThan(0);

      fs.chmodSync(f.volume, 0o777);
      const refused = await f.actors.ask(actor.id, "Refuse the archive of this second run."); await idle(f);
      const refusal = retain.mock.calls.findIndex(([, id]) => id === refused.runId);
      await expect(retain.mock.results[refusal]!.value).rejects.toMatchObject({ code: "FABRIC_ACTOR_RUN_ARCHIVE_REFUSED" });
      const source = f.agents.runDirectory(refused.runId!)!;
      expect(fs.existsSync(marker(source))).toBe(true);
      expect(f.actors.status(actor.id).pendingRunArchives).toBe(1);
      // Repair the ancestor so the WHOLE-tree move can succeed, but keep this individual run's sink refused.
      // No fixture entries are removed to create/repair the refusal.
      fs.chmodSync(f.volume, 0o700);
      fs.mkdirSync(path.join(actor.logDir!, refused.runId!, "events.jsonl"), { recursive: true, mode: 0o700 });
      // These source-only files must survive close/cleanup without relying on a later successful copy.
      for (const relative of ["steer.jsonl", "relaunches.jsonl", "nested/child/events.jsonl", "unknown-trace.bin"]) {
        seedTrace(source, relative);
      }
      await f.actors.stop(actor.id, undefined, true);
      const actorDirectory = path.dirname(actor.sessionFile!), actorRoot = path.dirname(actorDirectory);
      // Include a real completed run archive, hot and already-archived session backups, and a refused merge trace.
      for (const relative of ["steer.jsonl", "relaunches.jsonl", "nested/child/session.jsonl"]) seedTrace(completedDirectory, relative);
      for (const relative of ["session.jsonl.20200101T000000000Z.bak", "session-archive/old-session.jsonl",
        ".activation-refused-merge.jsonl", "unknown/nested-trace.bin"]) seedTrace(actorDirectory, relative);
      expect(fs.readFileSync(actor.sessionFile!).length).toBeGreaterThan(0);
      if (operation === "close") seedTrace(actorRoot, "root-only-trace.jsonl");
      const tree = operation === "remove" ? actorDirectory : actorRoot;
      const before = traceBytes(tree), sourceBefore = traceBytes(source), pinned = fs.lstatSync(tree);
      const rename = fs.renameSync;
      let atRename: Record<string, Buffer> | undefined;
      vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
        const stat = fs.lstatSync(from, { throwIfNoEntry: false });
        if (stat?.dev === pinned.dev && stat.ino === pinned.ino) atRename = traceBytes(String(from));
        return rename(from, to);
      });
      if (operation === "remove") {
        const removed = await f.actors.remove(actor.id);
        expect(removed).toMatchObject({ removed: true });
        expect(removed.pending).toBeUndefined();
      } else await f.actors.close(); // Explicitly the nonpersistent branch, not the existing persistent-close flow.
      await f.agents.close();
      const archive = fleetSessionArchive(f.mesh.root, "session:archive-test");
      const destination = operation === "remove" ? fleetSessionArchive(f.mesh.root, "session:archive-test", actor.id)
        : path.join(archive, "actor-root");
      expect(fs.existsSync(tree)).toBe(false);
      const moved = fs.lstatSync(destination);
      expect([moved.dev, moved.ino]).toEqual([pinned.dev, pinned.ino]); // MOVE, not copy-and-delete.
      expect(atRename).toBeDefined();
      const after = traceBytes(destination);
      expect(after).toEqual(atRename);
      for (const [relative, bytes] of Object.entries(before)) expect(after[relative], relative).toEqual(bytes);
      expect(traceBytes(source)).toEqual(sourceBefore);
      for (const [index, [id]] of cleanup.mock.calls.entries()) {
        if (id === refused.runId) await expect(cleanup.mock.results[index]!.value).rejects.toThrow(/archive/i);
      }
      const records = fs.readFileSync(path.join(archive, "MANIFEST.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line));
      expect(records.filter(record => record.from === tree).map(record => [record.phase, record.to]))
        .toEqual([["MOVING", destination], ["MOVED-TO", destination]]);
    }, 30000,
  );

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
