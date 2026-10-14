import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import { actorTreeDeletionHooks, mainPublicationFenced } from "../src/topology/main-publication-fence.js";

// smarty-dev#11105 item 1, recorded-call probe (#4664: it must never remove actor-tree bytes). An accepted ordinary
// removal waits behind a held run. When the run ends, the drain's cleanup raises synchronously
// (agents.actorArchiveSources throws once). Before the fix the drain exited with its inFlightRun still recorded and the
// finisher's join called agents.stop on the ended run (held here under test control), where an operator escalation
// was answered with nothing on disk. Fixed, the join never calls agents.stop and the finisher holds at the first
// fenced beforeCheck instead. Either way, an operator escalation (wait:false) answered while it is held must already be
// on disk: a manager restarted over the same persisted root, before anything finishes,
// must still see the operator flag. Same patterns as tests/actor-remove-live-fence.test.ts: every directory removal
// at or under the actor root is recorded and NOT performed; metadata locks pass through and release normally.
type Call = { at: string; fenced: boolean; path?: string };
const METADATA_LOCK = /\.lock(\.dead\.[0-9a-f]+)?$/;
const guardActorTrees = (roots: string[], fenced: () => boolean, calls: Call[]) => {
  const inTree = (file: unknown) => {
    const resolved = path.resolve(String(file));
    if (METADATA_LOCK.test(path.basename(resolved))) return false;
    return roots.some(root => resolved === path.resolve(root) || resolved.startsWith(path.resolve(root) + path.sep)) &&
      fs.existsSync(resolved) && fs.statSync(resolved).isDirectory();
  };
  const record = (file: unknown) => { calls.push({ at: "rm", path: path.resolve(String(file)), fenced: fenced() }); };
  const rmSync = fs.rmSync.bind(fs), rmdirSync = fs.rmdirSync.bind(fs) as (file: fs.PathLike, options?: fs.RmDirOptions) => void, rm = fs.promises.rm.bind(fs.promises);
  vi.spyOn(fs, "rmSync").mockImplementation((file, options) => { if (inTree(file)) record(file); else rmSync(file, options); });
  vi.spyOn(fs, "rmdirSync").mockImplementation(((file: fs.PathLike, options?: fs.RmDirOptions) => {
    if (inTree(file)) record(file); else rmdirSync(file, options);
  }) as typeof fs.rmdirSync);
  vi.spyOn(fs.promises, "rm").mockImplementation(async (file, options) => { if (inTree(file)) record(file); else await rm(file, options); });
};

afterEach(() => {
  delete actorTreeDeletionHooks.beforeCheck;
  delete actorTreeDeletionHooks.beforeDelete;
  vi.restoreAllMocks();
});

describe.skipIf(process.platform !== "linux")("operator escalation durability across a restart (smarty-dev#11105)", () => {
  it("an operator upgrade after a drain cleanup error is saved before the answer, and a restarted manager keeps it", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-recorded-11105-"));
    const actorRoot = path.join(root, "actors");
    const rootId = "session:recorded-11105";
    const identity = { id: rootId, name: "main", kind: "main" as const, sessionId: "recorded-11105" };
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const meshConfig = { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 };
    const agents = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents }, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs") });
    // No beforeOperatorCleanup on either manager: a marked cleanup must stay pending ("tree kept").
    const manager = (session: string) => new ActorManager(session, identity, mesh, meshConfig, agents, () => {},
      { actorRoot, rootId, persistent: true, closeGraceMs: 100, removalRetryMs: 5 });
    const fenced = () => mainPublicationFenced(mesh.root, rootId);
    const calls: Call[] = [];
    guardActorTrees([actorRoot], fenced, calls);
    // The first beforeCheck (the first manager's finisher, revoked, under the fence) holds under test control.
    let checkHeld = false;
    let releaseCheck!: () => void;
    const checkGate = new Promise<void>(resolve => { releaseCheck = resolve; });
    actorTreeDeletionHooks.beforeCheck = async () => {
      calls.push({ at: "beforeCheck", fenced: fenced() });
      if (!checkHeld) { checkHeld = true; await checkGate; }
    };
    actorTreeDeletionHooks.beforeDelete = async () => {
      calls.push({ at: "beforeDelete", fenced: fenced() });
      throw new Error("controlled stop before the tree delete");
    };
    const rowOf = (id: string) => (new ActorRegistryStore(actorRoot).records().find(record => record.id === id) as
      { removal?: { requestedAt?: number; operator?: true } } | undefined) ?? null;

    // The held run: ignores its abort until the gate opens.
    let release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    const runId = "a".repeat(32);
    vi.spyOn(agents, "run").mockImplementation(async (_request, _signal, onSpawned) => {
      onSpawned?.({ id: runId } as Parameters<NonNullable<typeof onSpawned>>[0]);
      await gate;
      return { id: runId, status: "stopped", text: "", usage: {} } as unknown as Awaited<ReturnType<typeof agents.run>>;
    });
    // The finisher's join: the first agents.stop stays pending under test control.
    let releaseStop!: () => void;
    const stopGate = new Promise<void>(resolve => { releaseStop = resolve; });
    let stopReleased = false;
    const stopCalls: string[] = [];
    vi.spyOn(agents, "stop").mockImplementation(async (id: string) => {
      stopCalls.push(id);
      await stopGate;
      return { id, status: "stopped", text: "", usage: {} } as unknown as Awaited<ReturnType<typeof agents.stop>>;
    });
    // The drain's cleanup error: once armed, the first actorArchiveSources call outside #retainRunLog throws.
    const archiveSources = agents.actorArchiveSources.bind(agents);
    let archiveArmed = false;
    const archiveThrows: string[] = [];
    vi.spyOn(agents, "actorArchiveSources").mockImplementation((actorId: string, sessionFile: string) => {
      const stack = new Error().stack ?? "";
      if (archiveArmed && !stack.includes("retainRunLog")) {
        archiveArmed = false;
        archiveThrows.push(stack.includes("#drain") ? "drain" : "other");
        throw new Error("controlled synchronous drain cleanup error");
      }
      return archiveSources(actorId, sessionFile);
    });

    const first = manager("recorded-11105");
    let restored: ActorManager | undefined;
    try {
      const actor = await first.create({ name: "escalated", instructions: "Work.", responseMode: "text" });
      const dir = path.join(actorRoot, actor.id);
      const marker = path.join(actorRoot, `removal-${actor.id}.json`);
      const markerOf = () => fs.existsSync(marker) ? JSON.parse(fs.readFileSync(marker, "utf8")) as Record<string, unknown> : null;
      fs.mkdirSync(dir, { recursive: true }); fs.writeFileSync(path.join(dir, "history.sentinel"), "history\n");
      first.tell(actor.id, "go");
      for (const until = Date.now() + 10_000; first.status(actor.id).inFlightRun?.id !== runId;) {
        if (Date.now() > until) throw new Error("run never started");
        await new Promise(resolve => setTimeout(resolve, 10));
      }

      // Ordinary removal, accepted behind the held run.
      const accepted = await first.remove(actor.id, { wait: false });
      expect(accepted.pending).toMatch(/pending behind its in-flight run/);
      const requestedAt = rowOf(actor.id)?.removal?.requestedAt;
      expect(requestedAt).toEqual(expect.any(Number));
      expect(rowOf(actor.id)?.removal).not.toHaveProperty("operator");
      expect(stopCalls).toEqual([]);

      // The run ends and the drain's cleanup raises. The finisher then either holds in agents.stop (the drain left its
      // inFlightRun behind: the baseline sequence) or reaches the held beforeCheck (the run was cleared).
      archiveArmed = true;
      release();
      for (const until = Date.now() + 10_000; !stopCalls.length && !checkHeld;) {
        if (Date.now() > until) throw new Error("finisher never reached agents.stop or beforeCheck");
        await new Promise(resolve => setTimeout(resolve, 5));
      }
      expect(archiveThrows).toEqual(["drain"]);
      console.log(`[11105] after the drain cleanup fault: ${JSON.stringify({ stopCalls, checkHeld })}`);

      // Operator escalation, answered while the first finisher is still held.
      const upgraded = await first.remove(actor.id, { wait: false, operator: true });
      const atAnswer = { stopReleased, stopCalls: [...stopCalls], row: rowOf(actor.id), marker: markerOf(),
        actorRootEntries: fs.readdirSync(actorRoot).sort(), hooks: calls.map(call => call.at) };
      console.log(`[11105] operator answer: ${JSON.stringify(upgraded)}`);
      console.log(`[11105] disk at answer (first finisher held): ${JSON.stringify(atAnswer)}`);
      expect(upgraded).toMatchObject({ removed: true });
      expect(atAnswer.stopReleased).toBe(false);
      expect(atAnswer.hooks.filter(at => at !== "beforeCheck")).toEqual([]); // nothing deleted or finished yet
      // Requirement: the operator flag is durable before the operator caller was answered.
      expect(atAnswer.row?.removal?.operator === true || atAnswer.marker?.operator === true).toBe(true);
      // The fix: the drain's cleanup error no longer leaves the run recorded, so the finisher's join never calls
      // agents.stop on the ended run; it committed (marker, then revocation) and holds under the fence instead.
      expect(stopCalls).toEqual([]);
      expect(checkHeld).toBe(true);

      // Restart over the same persisted root, before any finisher proceeds (the first manager stays frozen in the
      // held finisher; it is neither closed nor resumed). The restarted manager loads the row and marker from disk.
      restored = manager("recorded-11105-restored");
      const atRestart = { stopReleased, row: rowOf(actor.id), marker: markerOf(),
        restoredPending: restored.pendingRemovals().map(entry => ({ id: entry.id, requestedAt: entry.requestedAt })),
        restoredObligation: restored.cleanupObligation(actor.id)?.removal ?? null };
      console.log(`[11105] disk at restart (before finishing): ${JSON.stringify(atRestart)}`);
      expect(atRestart.stopReleased).toBe(false);
      // Requirement: the operator flag is durable before the operator caller was answered.
      expect(atRestart.row?.removal?.operator === true || atRestart.marker?.operator === true).toBe(true);
      if (atRestart.row) expect(atRestart.row.removal).toMatchObject({ requestedAt, operator: true });
      if (atRestart.marker) expect(atRestart.marker).toMatchObject({ id: actor.id, operator: true });

      // The restarted manager, with no resident final check, keeps the marked cleanup pending; the check refuses
      // before beforeDelete, and nothing is removed.
      await restored.finishPendingRemovals().catch(() => undefined);
      await restored.removalSettled(actor.id)?.catch(() => undefined);
      const state = restored.cleanupObligation(actor.id)?.removal?.state ?? restored.pendingRemovals()[0]?.state ?? "";
      console.log(`[11105] restarted finish: ${JSON.stringify({ state, hooks: calls, marker: markerOf() })}`);
      expect(state).toMatch(/tree kept/);
      expect(calls.map(call => call.at)).not.toContain("beforeDelete");
      expect(markerOf()).toMatchObject({ id: actor.id, operator: true });
      expect(calls.filter(call => call.at === "rm")).toEqual([]);
      expect(fs.readFileSync(path.join(dir, "history.sentinel"), "utf8")).toBe("history\n");
      // The first manager's finisher still holds the in-process fence at its held beforeCheck.
      expect(fenced()).toBe(true);
    } finally {
      // Settle every finisher under the guard (rm recorded only, beforeDelete still throws), then close.
      release(); stopReleased = true; releaseStop(); releaseCheck();
      for (const owner of [first, restored]) {
        for (const id of owner ? owner.pendingRemovals().map(entry => entry.id) : []) await owner!.removalSettled(id)?.catch(() => undefined);
      }
      await first.close().catch(() => undefined);
      await restored?.close().catch(() => undefined);
      await agents.close();
      // Logged, not asserted here, so a teardown check never masks the primary failure; every rm was recorded only.
      console.log(`[11105] recorded calls through teardown: ${JSON.stringify(calls)}`);
      // Owned temp root (its parent, not an actor-tree target, so the guard passes it through). The guard stays
      // installed through it, so a late finisher could never perform a real actor-tree delete.
      fs.rmSync(root, { recursive: true, force: true });
      vi.restoreAllMocks();
    }
  }, 60_000);
});
