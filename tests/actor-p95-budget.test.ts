import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorLogStore } from "../src/actors/log-store.js";
import { ActorManager, FABRIC_ACTOR_ACTIVATION_ALARM_TOPIC } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";

const roots: string[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("actor stuck-run p95 archive budget", () => {
  it("reads only a bounded window from a 20k-entry archive and refreshes on a directory change", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-p95-budget-"));
    roots.push(root);
    const runsDirectory = path.join(root, "actor", "runs");
    fs.mkdirSync(runsDirectory, { recursive: true });
    const actor = { sessionFile: path.join(root, "actor", "session.jsonl") };
    const store = new ActorLogStore(
      { maxEventBytes: 8192 },
      { eventContextChars: 1000 },
      { actorRunArchiveMs: 60_000 },
    );
    let entries = Array.from({ length: 20_000 }, (_, index) => ({
      name: `run-${String(index).padStart(5, "0")}`,
      isDirectory: () => true,
    }));
    let reads = 0;
    let opens = 0;
    const readdir = vi.spyOn(fs, "readdirSync");
    vi.spyOn(fs, "opendirSync").mockImplementation(() => {
      opens++;
      let index = 0;
      return {
        readSync: () => {
          reads++;
          return entries[index++] ?? null;
        },
        closeSync: () => undefined,
      } as unknown as fs.Dir;
    });

    const first = store.retainedRunIdsForP95(actor);
    expect(first).toHaveLength(200);
    expect(reads).toBe(200);
    expect(opens).toBe(1);
    expect(readdir).not.toHaveBeenCalled();

    expect(store.retainedRunIdsForP95(actor)).toEqual(first);
    expect(reads).toBe(200);
    expect(opens).toBe(1);

    entries = [
      { name: "aaaa-counterexample", isDirectory: () => true },
      ...Array.from({ length: 199 }, (_, index) => ({
        name: `replacement-${String(index).padStart(3, "0")}`,
        isDirectory: () => true,
      })),
    ];
    fs.writeFileSync(path.join(runsDirectory, ".refresh"), "refresh");

    const refreshed = store.retainedRunIdsForP95(actor);
    expect(refreshed).toContain("aaaa-counterexample");
    expect(refreshed).not.toContain(first[0]);
    expect(refreshed).toHaveLength(200);
    expect(reads).toBe(400);
    expect(opens).toBe(2);
  });

  // Keep: recorded performance incident #4796 and the alarm safety boundary.
  // This walks the real ActorManager alarm path, not an exported test-only
  // percentile helper. Only the final owned-worker liveness hint is faulted.
  it("uses the sampled p95 in the real stuck-run alarm without alarming before its threshold", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-p95-alarm-"));
    roots.push(root);
    const minute = 60_000, realNow = Date.now.bind(Date);
    let skew = 0;
    vi.spyOn(Date, "now").mockImplementation(() => realNow() + skew);
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const agents = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: true }, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
    });
    const actors = new ActorManager("p95", { id: "session:p95", name: "main", kind: "main", sessionId: "p95" },
      mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {},
      { actorRoot: path.join(root, "actors"), retentionSweepMs: 20 });
    try {
      const actor = await actors.create({ name: "p95-stuck", instructions: "Run", coalesce: false });
      const archives = path.join(path.dirname(actor.sessionFile!), "runs");
      for (let index = 1; index <= 20; index++) {
        const run = path.join(archives, `history-${index}`);
        fs.mkdirSync(run, { recursive: true });
        fs.writeFileSync(path.join(run, "status.json"), JSON.stringify({ startedAt: 1, finishedAt: 1 + index * minute }));
      }
      await actors.tell(actor.id, "HANG_WITH_PROGRESS p95");
      await vi.waitFor(() => {
        const runId = actors.status(actor.id).inFlightRun?.id;
        expect(runId).toBeDefined();
        expect(agents.workerIdentity(runId!)?.open).toBe(true);
      }, { timeout: 15_000 });
      const runId = actors.status(actor.id).inFlightRun!.id;
      const identity = agents.workerIdentity.bind(agents);
      vi.spyOn(agents, "workerIdentity").mockImplementation(id => id === runId
        ? { pid: 2147483647, startTime: undefined, open: false } : identity(id));
      const alarms = () => mesh.read({ topic: FABRIC_ACTOR_ACTIVATION_ALARM_TOPIC, limit: 100 })
        .filter(event => event.kind === "actor-run-stuck" && (event.data as { actorId?: string }).actorId === actor.id);
      skew = 37 * minute;
      await new Promise(resolve => setTimeout(resolve, 100));
      expect(alarms()).toHaveLength(0); // 2 * p95(20 samples) = 38 min, not the 30 min floor.
      skew = 39 * minute;
      await vi.waitFor(() => expect(alarms()).toHaveLength(1), { timeout: 5_000 });
      expect(alarms()[0]!.data).toMatchObject({ thresholdMs: 38 * minute, runId });
      vi.mocked(agents.workerIdentity).mockRestore();
      await agents.stop(runId);
      await actors.stop(actor.id);
    } finally { await actors.close(); await agents.close(); }
  }, 30_000);
});
