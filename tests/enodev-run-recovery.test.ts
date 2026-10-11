import { spawn, spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { settleEnodevRun, settleLostEnodevRun } from "../src/agents/enodev-recovery.js";
import { MeshStore } from "../src/mesh/store.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { processStartTime } from "../src/residency/process-identity.js";

// smarty-dev#8935: a run left "execution exit unconfirmed" by the pre-pi-fabric#849 ENODEV scope observation settles
// once from its settlement.json (delivered to the restored activation instead of a rerun), or as lost when its
// activation is gone; any doubt keeps the unresolved-worker veto.
const roots: string[] = [];
const children: Array<{ kill: () => void }> = [];
afterEach(() => {
  vi.restoreAllMocks();
  for (const child of children.splice(0)) child.kill();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const tempRoot = (): string => { const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-enodev-")); roots.push(root); return root; };
const ENODEV = "Owned scope observation failed: Error: ENODEV: no such device, read";
const deadPid = (): number => spawnSync(process.execPath, ["-e", ""]).pid!;
const livePid = (): number => {
  const child = spawn(process.execPath, ["-e", "setTimeout(() => {}, 60000)"], { stdio: "ignore" });
  children.push({ kill: () => child.kill("SIGKILL") });
  return child.pid!;
};
const ACTOR = "a".repeat(32);
interface Fixture { pid?: number; birth?: string; reason?: string; settlement?: unknown; activation?: string }
const runDir = (runRoot: string, id: string, fixture: Fixture = {}): string => {
  const directory = path.join(runRoot, id);
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  const pid = fixture.pid ?? deadPid();
  fs.writeFileSync(path.join(directory, "status.json"), JSON.stringify({ id, name: "actor-run", status: "running", runner: "pi",
    transport: "process", actorId: ACTOR, startedAt: 1, sessionId: String(pid), ...(fixture.birth ? { processStartTime: fixture.birth } : {}) }));
  fs.writeFileSync(path.join(directory, "task.txt"), ["Fabric actor message from direct:",
    JSON.stringify({ source: "direct", payload: { id: "nested-not-the-item" }, id: fixture.activation ?? "item-1" }, null, 2)].join("\n"));
  fs.writeFileSync(path.join(directory, "unresolved-worker.json"), JSON.stringify({ reason: fixture.reason ?? ENODEV, markedAt: 1, runId: id }));
  fs.writeFileSync(path.join(directory, "archive-pending.json"), JSON.stringify({ format: 1, awaitingResult: true, ownerPid: 1 }));
  if (fixture.settlement !== null) {
    fs.writeFileSync(path.join(directory, "settlement.json"), JSON.stringify(fixture.settlement ?? { version: 1, runId: id, outcome: "completed", text: "the saved answer" }));
  }
  return directory;
};
const vetoed = (directory: string): boolean => fs.existsSync(path.join(directory, "unresolved-worker.json"));

describe.runIf(process.platform === "linux")("ENODEV-unconfirmed actor runs (smarty-dev#8935)", () => {
  it("a restored activation gets its earlier run's saved answer exactly once, instead of a rerun", async () => {
    const runRoot = tempRoot();
    const id = "1".repeat(32);
    const directory = runDir(runRoot, id);
    const manager = new AgentManager(tempRoot(), { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 },
      { workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot });
    const spawned = vi.spyOn(manager, "spawn").mockRejectedValue(new Error("launched a rerun"));
    const result = await manager.run({ task: "rerun", actorId: ACTOR, activationId: "item-1" });
    expect(result).toMatchObject({ id, status: "completed", text: "the saved answer", actorId: ACTOR });
    expect(spawned).not.toHaveBeenCalled();
    expect(vetoed(directory)).toBe(false);
    expect(fs.existsSync(path.join(directory, "archive-pending.json"))).toBe(false);
    expect(JSON.parse(fs.readFileSync(path.join(directory, "status.json"), "utf8"))).toMatchObject({ status: "completed", text: "the saved answer" });
    // Once: the same activation again is a real launch.
    await expect(manager.run({ task: "rerun", actorId: ACTOR, activationId: "item-1" })).rejects.toThrow("launched a rerun");
    expect(spawned).toHaveBeenCalledOnce();
  });

  it("keeps the veto for a live pid with no recorded birth", () => {
    const directory = runDir(tempRoot(), "2".repeat(32), { pid: livePid() });
    expect(settleEnodevRun(directory, ACTOR, "item-1")).toBeUndefined();
    expect(vetoed(directory)).toBe(true);
  });

  it("keeps the veto for a live pid whose start time matches the recorded birth, and settles a reused pid", async () => {
    const pid = livePid();
    await vi.waitFor(() => expect(processStartTime(pid)).toBeDefined());
    const same = runDir(tempRoot(), "3".repeat(32), { pid, birth: processStartTime(pid)! });
    expect(settleEnodevRun(same, ACTOR, "item-1")).toBeUndefined();
    expect(vetoed(same)).toBe(true);
    const reused = runDir(tempRoot(), "4".repeat(32), { pid, birth: "1" }); // the recorded worker started at tick 1
    expect(settleEnodevRun(reused, ACTOR, "item-1")).toMatchObject({ status: "completed" });
    expect(vetoed(reused)).toBe(false);
  });

  it("keeps the veto for another unresolved reason, another activation, or a missing or mismatched settlement", () => {
    const runRoot = tempRoot();
    const other = runDir(runRoot, "5".repeat(32), { reason: "Fabric worker 7 did not confirm execution exit after bounded SIGTERM/SIGKILL cleanup" });
    expect(settleEnodevRun(other, ACTOR, "item-1")).toBeUndefined();
    const nested = runDir(runRoot, "6".repeat(32));
    expect(settleEnodevRun(nested, ACTOR, "nested-not-the-item")).toBeUndefined(); // only the item's own id
    const missing = runDir(runRoot, "7".repeat(32), { settlement: null });
    expect(settleEnodevRun(missing, ACTOR, "item-1")).toBeUndefined();
    const mismatched = runDir(runRoot, "8".repeat(32), { settlement: { version: 1, runId: "9".repeat(32), outcome: "completed", text: "x" } });
    expect(settleEnodevRun(mismatched, ACTOR, "item-1")).toBeUndefined();
    for (const directory of [other, nested, missing, mismatched]) expect(vetoed(directory)).toBe(true);
  });

  // Through the real ActorManager restore: the restored in-flight item asks for a run with its activationId, and the
  // earlier ENODEV-unconfirmed run's saved answer completes it. Only the process launch is stubbed.
  // An actor activation interrupted mid-run by a real close, then given the pre-#849 outcome: the worker completed
  // and saved its answer, but its exit stayed unconfirmed (ENODEV). Returns what a restart needs.
  const interruptedRun = async () => {
    const root = tempRoot();
    const runRoot = path.join(root, "runs");
    const actorRoot = path.join(root, "actors");
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const actors = (agents: AgentManager, rootId = "session:reload") => new ActorManager("reload",
      { id: rootId, name: "main", kind: "main", sessionId: rootId.slice(8) }, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 },
      agents, () => {}, { actorRoot, persistent: true, meshCursorPath: path.join(actorRoot, "mesh-cursor.json") });
    const config = { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, retainRuns: true };
    const first = new AgentManager(root, config, { workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot });
    const before = actors(first);
    const actor = await before.create({ name: "reviewer", instructions: "Review.", topics: ["team.pulls"], responseMode: "text" });
    await mesh.publish({ topic: "team.pulls", from: { id: "peer", name: "peer", kind: "actor" }, text: "LIVE_WITH_PROGRESS interrupted" });
    await vi.waitFor(() => expect(before.status(actor.id).status).toBe("running"), { timeout: 10_000, interval: 20 });
    const id = fs.readdirSync(runRoot).find(name => /^[a-f0-9]{32}$/.test(name))!;
    await before.close();
    await first.close();
    const directory = path.join(runRoot, id);
    const status = JSON.parse(fs.readFileSync(path.join(directory, "status.json"), "utf8"));
    // The worker's own record (it ran to completion), whatever the close left: a slow worker may not have written it.
    fs.writeFileSync(path.join(directory, "status.json"), JSON.stringify({ ...status, id, actorId: actor.id, status: "running",
      sessionId: String(deadPid()), transport: "process" }));
    fs.writeFileSync(path.join(directory, "unresolved-worker.json"), JSON.stringify({ reason: ENODEV, markedAt: 1, runId: id }));
    fs.writeFileSync(path.join(directory, "settlement.json"), JSON.stringify({ version: 1, runId: id, outcome: "completed", text: "SAVED_ANSWER" }));
    const second = new AgentManager(root, config, { workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot });
    const launched = vi.spyOn(second, "spawn").mockRejectedValue(new Error("launched a rerun"));
    return { actor, id, directory, actorRoot, actors, second, launched };
  };
  /** The registry now names `adopter` as the actor's root, adopted from the lineage that ran it (its queue file). */
  const adopt = (actorRoot: string, actorId: string, adopter: string) => {
    const store = new ActorRegistryStore(actorRoot);
    store.write(store.snapshot().actors.map(row => row.id === actorId
      ? { ...row, rootId: adopter, adoptedAt: Date.now(), adoptedFrom: [row.rootId] } : row) as unknown as Record<string, unknown>[]);
  };

  it("a restarted actor gets its interrupted activation's saved answer once, with no new launch", async () => {
    const { actor, id, directory, actors, second, launched } = await interruptedRun();
    const after = actors(second);
    try {
      await vi.waitFor(() => expect(after.status(actor.id)).toMatchObject({ lastRunId: id }), { timeout: 15_000, interval: 20 });
      await vi.waitFor(() => expect(after.status(actor.id).status).toBe("idle"), { timeout: 10_000, interval: 20 });
      expect(launched).not.toHaveBeenCalled();
      expect(vetoed(directory)).toBe(false);
      expect(JSON.parse(fs.readFileSync(path.join(directory, "status.json"), "utf8"))).toMatchObject({ status: "completed", text: "SAVED_ANSWER" });
    } finally {
      await after.close();
      await second.close();
    }
  }, 60_000);

  // pi-fabric#912 review: the item can live only in an ADOPTED predecessor's queue file. The lost sweep must wait for
  // that takeover, and an unreadable queue state is unknown, never empty.
  it("an activation pending only in an adopted predecessor queue is recovered after the takeover, not settled as lost", async () => {
    const { actor, id, directory, actorRoot, actors, second, launched } = await interruptedRun();
    adopt(actorRoot, actor.id, "session:adopter");
    const warn = vi.spyOn(console, "warn");
    const after = actors(second, "session:adopter");
    try {
      await vi.waitFor(() => expect(JSON.parse(fs.readFileSync(path.join(directory, "status.json"), "utf8")))
        .toMatchObject({ status: "completed", text: "SAVED_ANSWER" }), { timeout: 15_000, interval: 20 });
      await vi.waitFor(() => expect(after.status(actor.id).status).toBe("idle"), { timeout: 10_000, interval: 20 });
      expect(launched).not.toHaveBeenCalled();
      expect(warn.mock.calls.some(([line]) => String(line).includes("settled as lost"))).toBe(false);
      expect(after.status(actor.id)).toMatchObject({ lastRunId: id });
    } finally {
      await after.close();
      await second.close();
    }
  }, 60_000);

  it("an unreadable predecessor queue leaves the run's marker: nothing settles as lost", async () => {
    const { actor, directory, actorRoot, actors, second } = await interruptedRun();
    adopt(actorRoot, actor.id, "session:adopter");
    const predecessor = fs.readdirSync(path.join(actorRoot, actor.id)).find(name => /^queue-[a-f0-9]{16}\.json$/.test(name))!;
    fs.writeFileSync(path.join(actorRoot, actor.id, predecessor), "{ torn");
    const after = actors(second, "session:adopter");
    try {
      await new Promise(resolve => setTimeout(resolve, 500));
      expect(vetoed(directory)).toBe(true);
      expect(JSON.parse(fs.readFileSync(path.join(directory, "status.json"), "utf8")).status).toBe("running");
    } finally {
      await after.close();
      await second.close();
    }
  }, 60_000);

  it("settles a run as lost when its activation is no longer pending, and logs it once", () => {
    const runRoot = tempRoot();
    const lost = runDir(runRoot, "a".repeat(31) + "1", { activation: "item-dropped" });
    const kept = runDir(runRoot, "a".repeat(31) + "2", { activation: "item-pending" });
    const manager = new AgentManager(tempRoot(), { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 },
      { workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot });
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    manager.settleLostEnodevRuns(ACTOR, new Set(["item-pending"]));
    expect(vetoed(lost)).toBe(false);
    expect(JSON.parse(fs.readFileSync(path.join(lost, "status.json"), "utf8")).status).toBe("completed");
    expect(vetoed(kept)).toBe(true); // a pending activation is claimed by its own run request instead
    expect(warn.mock.calls.filter(([line]) => String(line).includes("settled as lost"))).toHaveLength(1);
    expect(String(warn.mock.calls[0]![0])).toContain(`run ${path.basename(lost)}`);
    expect(String(warn.mock.calls[0]![0])).toContain("item-dropped");
    // A lost run without a settlement receipt is terminal as failed.
    const bare = runDir(runRoot, "b".repeat(32), { activation: "item-gone", settlement: null });
    expect(settleLostEnodevRun(bare, ACTOR, new Set())).toBe("item-gone");
    expect(JSON.parse(fs.readFileSync(path.join(bare, "status.json"), "utf8"))).toMatchObject({ status: "failed" });
  });
});
