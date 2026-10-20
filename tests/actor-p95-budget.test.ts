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
    fs.utimesSync(runsDirectory, new Date(0), new Date(1));

    const refreshed = store.retainedRunIdsForP95(actor);
    expect(refreshed).toContain("aaaa-counterexample");
    expect(refreshed).not.toContain(first[0]);
    expect(refreshed).toHaveLength(200);
    expect(reads).toBe(400);
    expect(opens).toBe(2);

    fs.mkdirSync(path.join(runsDirectory, "ordinary-z"));
    fs.mkdirSync(path.join(runsDirectory, "ordinary-a"));
    expect(store.retainedRunIds(actor)).toEqual([".refresh", "ordinary-a", "ordinary-z"]);
    expect(readdir).toHaveBeenCalledTimes(1);
  });

  // Keep: a failed archive refresh must not manufacture a lower alarm threshold.
  it.each([false, true])("rejects partial windows and retries through success (warm cache: %s)", warmCache => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-p95-partial-"));
    roots.push(root);
    const runsDirectory = path.join(root, "actor", "runs");
    fs.mkdirSync(runsDirectory, { recursive: true });
    const actor = { sessionFile: path.join(root, "actor", "session.jsonl") };
    const store = new ActorLogStore(
      { maxEventBytes: 8192 }, { eventContextChars: 1000 }, { actorRunArchiveMs: 60_000 },
    );
    let names = ["long-history-a", "long-history-b"];
    let incomplete = false, opens = 0, reads = 0;
    const close = vi.fn();
    vi.spyOn(fs, "opendirSync").mockImplementation(() => {
      opens++;
      let index = 0;
      return {
        readSync: () => {
          reads++;
          if (incomplete && index === 1) throw new Error("late directory read failure");
          const name = names[index++];
          return name === undefined ? null : { name, isDirectory: () => true };
        },
        closeSync: close,
      } as unknown as fs.Dir;
    });

    const previous = warmCache ? store.retainedRunIdsForP95(actor) : [];
    expect(previous).toEqual(warmCache ? names : []);
    expect(reads).toBe(warmCache ? 3 : 0); // A clean EOF below 200 is complete.
    fs.utimesSync(runsDirectory, new Date(0), new Date(1));
    names = ["short-history"];
    incomplete = true;
    expect(store.retainedRunIdsForP95(actor)).toEqual(previous);
    expect(store.retainedRunIdsForP95(actor)).toEqual(previous);
    expect(opens).toBe(Number(warmCache) + 2);
    expect(reads).toBe(Number(warmCache) * 3 + 4);
    expect(close).toHaveBeenCalledTimes(opens);

    // The directory signature stays the same across failure and recovery:
    // neither the partial IDs nor its new signature may become a cache hit.
    incomplete = false;
    names = ["recovered-a", "recovered-b"];
    expect(store.retainedRunIdsForP95(actor)).toEqual(names);
    expect(store.retainedRunIdsForP95(actor)).toEqual(names);
    expect(opens).toBe(Number(warmCache) + 3);

    const recovered = [...names];
    actor.sessionFile = path.join(root, "other-actor", "session.jsonl");
    names = ["other-partial"];
    incomplete = true;
    expect(store.retainedRunIdsForP95(actor)).toEqual([]); // Never borrow another path's cache.
    actor.sessionFile = path.join(root, "actor", "session.jsonl");
    expect(store.retainedRunIdsForP95(actor)).toEqual(recovered);
    expect(opens).toBe(Number(warmCache) + 4);
  });

  it("does not infer cache validity from failed stats or discard a complete fallback", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-p95-stat-"));
    roots.push(root);
    fs.mkdirSync(path.join(root, "actor", "runs"), { recursive: true });
    const actor = { sessionFile: path.join(root, "actor", "session.jsonl") };
    const store = new ActorLogStore(
      { maxEventBytes: 8192 }, { eventContextChars: 1000 }, { actorRunArchiveMs: 60_000 },
    );
    let names = ["history-before"];
    let incomplete = false, opens = 0;
    vi.spyOn(fs, "opendirSync").mockImplementation(() => {
      opens++;
      let index = 0;
      return {
        readSync: () => {
          if (incomplete && index === 1) throw new Error("late directory read failure");
          const name = names[index++];
          return name === undefined ? null : { name, isDirectory: () => true };
        },
        closeSync: () => undefined,
      } as unknown as fs.Dir;
    });
    const stat = vi.spyOn(fs, "statSync").mockImplementation(() => { throw new Error("metadata unavailable"); });
    expect(store.retainedRunIdsForP95(actor)).toEqual(["history-before"]);
    names = ["history-after"];
    expect(store.retainedRunIdsForP95(actor)).toEqual(["history-after"]);
    expect(opens).toBe(2); // undefined === undefined is not evidence of an unchanged archive.

    incomplete = true;
    names = ["partial-short"];
    expect(store.retainedRunIdsForP95(actor)).toEqual(["history-after"]);
    expect(store.retainedRunIdsForP95(actor)).toEqual(["history-after"]);
    expect(opens).toBe(4);
    stat.mockRestore();
    expect(store.retainedRunIdsForP95(actor)).toEqual(["history-after"]);
    expect(opens).toBe(5);

    incomplete = false;
    names = ["recovered"];
    expect(store.retainedRunIdsForP95(actor)).toEqual(names);
    expect(store.retainedRunIdsForP95(actor)).toEqual(names);
    expect(opens).toBe(6);
  });

  // Keep: recorded performance incident #4796 and the alarm safety boundary.
  // Both cases walk the real ActorManager alarm with the real fake-worker fixture;
  // the refresh counterexample additionally faults only archive directory reads.
  it.each([false, true])("uses the real stuck-run p95 threshold without an early alarm (failed refresh: %s)", async failedRefresh => {
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
    let actorId: string | undefined;
    let runId: string | undefined;
    let failRefresh = false;
    let restoreArchiveFault: (() => void) | undefined;
    let restoreWorkerIdentity: (() => void) | undefined;
    try {
      const actor = await actors.create({ name: "p95-stuck", instructions: "Run", coalesce: false });
      actorId = actor.id;
      const archives = path.join(path.dirname(actor.sessionFile!), "runs");
      for (let index = 1; index <= 20; index++) {
        const run = path.join(archives, `history-${index}`);
        fs.mkdirSync(run, { recursive: true });
        fs.writeFileSync(path.join(run, "status.json"), JSON.stringify({ startedAt: 1, finishedAt: 1 + index * minute }));
      }
      const shortEntry = fs.readdirSync(archives, { withFileTypes: true }).find(entry => entry.name === "history-1")!;
      const opendir = fs.opendirSync.bind(fs);
      let completeSamples = 0, incompleteSamples = 0;
      const archiveFaultSpy = vi.spyOn(fs, "opendirSync").mockImplementation((...args: Parameters<typeof fs.opendirSync>) => {
        const directory = opendir(...args);
        if (String(args[0]) !== archives) return directory;
        const read = directory.readSync.bind(directory);
        let reads = 0;
        directory.readSync = () => {
          if (failRefresh) {
            if (reads++ === 0) return shortEntry;
            incompleteSamples++;
            throw new Error("late archive read failure after the shortest run");
          }
          const entry = read();
          if (entry === null) completeSamples++;
          return entry;
        };
        return directory;
      });
      restoreArchiveFault = () => archiveFaultSpy.mockRestore();
      await actors.tell(actor.id, "HANG_WITH_PROGRESS p95");
      await vi.waitFor(() => {
        const runId = actors.status(actor.id).inFlightRun?.id;
        expect(runId).toBeDefined();
        expect(agents.workerIdentity(runId!)?.open).toBe(true);
      }, { timeout: 15_000 });
      runId = actors.status(actor.id).inFlightRun!.id;
      // Warm the actual manager's cache while the real worker is still live.
      skew = 31 * minute;
      await vi.waitFor(() => expect(completeSamples).toBeGreaterThan(0), { timeout: 5_000 });
      const identity = agents.workerIdentity.bind(agents);
      const workerIdentitySpy = vi.spyOn(agents, "workerIdentity").mockImplementation(id => id === runId
        ? { pid: 2147483647, startTime: undefined, open: false } : identity(id));
      restoreWorkerIdentity = () => workerIdentitySpy.mockRestore();
      const alarms = () => mesh.read({ topic: FABRIC_ACTOR_ACTIVATION_ALARM_TOPIC, limit: 100 })
        .filter(event => event.kind === "actor-run-stuck" && (event.data as { actorId?: string }).actorId === actor.id);
      if (failedRefresh) {
        failRefresh = true;
        fs.utimesSync(archives, new Date(0), new Date(1));
      }
      skew = 37 * minute;
      if (failedRefresh) await vi.waitFor(() => expect(incompleteSamples).toBeGreaterThan(0), { timeout: 5_000 });
      await new Promise(resolve => setTimeout(resolve, 100));
      expect(alarms()).toHaveLength(0); // A partial 1-min sample must not replace the complete 38-min threshold.
      failRefresh = false;
      skew = 39 * minute;
      await vi.waitFor(() => expect(alarms()).toHaveLength(1), { timeout: 5_000 });
      expect(alarms()[0]!.data).toMatchObject({ thresholdMs: 38 * minute, runId });
      if (failedRefresh) expect(completeSamples).toBeGreaterThan(1);
    } finally {
      failRefresh = false;
      restoreArchiveFault?.();
      restoreWorkerIdentity?.();
      try {
        if (actorId) await actors.stop(actorId, undefined, true);
        else if (runId) await agents.stop(runId);
      } finally {
        try { await actors.close(); }
        finally { await agents.close(); }
      }
    }
  }, 30_000);
});
