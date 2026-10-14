import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";

// Regression proof (fv2-11107): the existing generation/snapshot cache in
// AgentManager.retentionReferences already avoids idle re-enumeration and
// re-proof of historical runs, while retentionCustodyVeto stays a fresh,
// targeted check. Real AgentManager methods; only fs calls are observed.
// No product deletion is executed: retention references and custody vetoes
// are read-only. Fixture cleanup only.

const HISTORY = 220;
const EXITED_PID = "2147483647";
const roots: string[] = [];
const managers: AgentManager[] = [];

afterEach(async () => {
  vi.restoreAllMocks();
  for (const manager of managers.splice(0)) await manager.close();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const writeStatus = (run: string, value: Record<string, unknown>) => {
  fs.mkdirSync(run, { recursive: true });
  fs.writeFileSync(path.join(run, "status.json"), JSON.stringify(value));
};

const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "manager-retention-cache-"));
  roots.push(root);
  const runRoot = path.join(root, "runs");
  fs.mkdirSync(runRoot);
  for (let index = 0; index < HISTORY; index++) {
    writeStatus(path.join(runRoot, `hist-${index}`), { status: "completed", transport: "process", sessionId: EXITED_PID, finishedAt: 1 });
  }
  const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: true }, {
    runRoot, workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
  });
  managers.push(manager);
  return { root, runRoot, manager };
};

/** Records filesystem work attributable to the run root. */
const observe = (runRoot: string) => {
  const counts = { opendirRoot: 0, statusReads: 0, rootStats: 0, statusReadsByRun: new Map<string, number>() };
  const opendir = fs.opendirSync, read = fs.readFileSync, lstat = fs.lstatSync;
  vi.spyOn(fs, "opendirSync").mockImplementation(((...args: Parameters<typeof opendir>) => {
    if (path.resolve(String(args[0])) === runRoot) counts.opendirRoot++;
    return opendir(...args);
  }) as typeof opendir);
  vi.spyOn(fs, "readFileSync").mockImplementation(((...args: Parameters<typeof read>) => {
    const file = String(args[0]);
    if (file.startsWith(runRoot + path.sep) && file.endsWith("status.json")) {
      counts.statusReads++;
      const run = path.relative(runRoot, file).split(path.sep)[0]!;
      counts.statusReadsByRun.set(run, (counts.statusReadsByRun.get(run) ?? 0) + 1);
    }
    return read(...args);
  }) as typeof read);
  vi.spyOn(fs, "lstatSync").mockImplementation(((...args: Parameters<typeof lstat>) => {
    if (path.resolve(String(args[0])) === runRoot) counts.rootStats++;
    return lstat(...(args as [fs.PathLike]));
  }) as typeof lstat);
  return counts;
};

const complete = (manager: AgentManager, now: number) => {
  let refs = new Set(["*"]);
  for (let slice = 0; slice < 50 && refs.has("*"); slice++) refs = manager.retentionReferences({ now, budgetMs: 10_000, maxEntries: 100_000 });
  return refs;
};

describe.skipIf(process.platform === "win32")("AgentManager retention reference cache (POSIX V2)", () => {
  it(`a completed stable-generation snapshot does not re-enumerate or re-read ${HISTORY} historical runs within the interval`, () => {
    const { runRoot, manager } = fixture();
    const now = Date.now();
    const initial = observe(runRoot);
    const refs = complete(manager, now);
    // Exited, terminal, unreferenced history is not protected.
    expect([...refs]).toEqual([]);
    // The first preparation reads every historical status at least once.
    expect(initial.statusReadsByRun.size).toBe(HISTORY);
    expect(initial.opendirRoot).toBeGreaterThanOrEqual(1);
    vi.restoreAllMocks();

    const idle = observe(runRoot);
    const polls = 100;
    for (let poll = 0; poll < polls; poll++) {
      // Idle polls inside the 60-second interval.
      expect([...manager.retentionReferences({ now: now + (poll * 59_000) / polls, budgetMs: 10_000, maxEntries: 100_000 })]).toEqual([]);
    }
    expect(idle.opendirRoot).toBe(0);
    expect(idle.statusReads).toBe(0);
    // Only the bounded generation stat of the run root itself (O(1) per poll).
    expect(idle.rootStats).toBeLessThanOrEqual(polls * 2);
  });

  it("a new directory invalidates the generation and only the new run is proven; history is enumerated, not re-read", () => {
    const { runRoot, manager } = fixture();
    const now = Date.now();
    complete(manager, now);
    // Live writer, with an actor association, so the delta has a visible effect.
    writeStatus(path.join(runRoot, "new-live"), { status: "running", transport: "process", sessionId: String(process.pid), actorId: "actor-new" });
    const delta = observe(runRoot);
    const refs = complete(manager, now + 1_000);
    expect(refs.has("new-live")).toBe(true);
    expect(refs.has("actor-new")).toBe(true);
    expect(refs.has("*")).toBe(false);
    expect(delta.opendirRoot).toBeGreaterThanOrEqual(1);
    expect([...delta.statusReadsByRun.keys()]).toEqual(["new-live"]);
    // Removing the directory invalidates the generation, but the persistent
    // watermark keeps its protective (held) hint until the periodic refresh:
    // a safe false positive, never deletion authority.
    vi.restoreAllMocks();
    fs.rmSync(path.join(runRoot, "new-live"), { recursive: true, force: true });
    expect(complete(manager, now + 2_000).has("new-live")).toBe(true);
    // Snapshot completed at now+2s is current until now+62s; the watermark
    // (set at now) is discarded at the first factory on or after now+60s.
    expect(complete(manager, now + 61_000).has("new-live")).toBe(true);
    expect(complete(manager, now + 62_000).has("new-live")).toBe(false);
  });

  it("an incomplete sliced scan stays wildcard and the targeted custody veto stays true until it completes", () => {
    const { runRoot, manager } = fixture();
    const now = Date.now();
    const sliced = manager.retentionReferences({ now, budgetMs: 10_000, maxEntries: 1 });
    expect(sliced.has("*")).toBe(true);
    // An ID that is not a run directory (e.g. actor) depends on the complete
    // snapshot; an incomplete one is a veto.
    expect(manager.retentionCustodyVeto("actor-unknown")).toBe(true);
    for (let slice = 0; slice < 5; slice++) {
      expect(manager.retentionReferences({ now, budgetMs: 10_000, maxEntries: 3 }).has("*")).toBe(true);
      expect(manager.retentionCustodyVeto("actor-unknown")).toBe(true);
    }
    expect(complete(manager, now).has("*")).toBe(false);
    expect(manager.retentionCustodyVeto("actor-unknown")).toBe(false);
    // A run directory ID is always checked freshly, independent of the cache.
    expect(manager.retentionCustodyVeto("hist-0")).toBe(false);
    // An explicit refresh restarts as wildcard again.
    expect(manager.retentionReferences({ now, budgetMs: 10_000, maxEntries: 1, refresh: true }).has("*")).toBe(true);
    expect(manager.retentionCustodyVeto("actor-unknown")).toBe(true);
    expect(fs.readdirSync(runRoot).length).toBe(HISTORY);
  });

  it("in-place nested live descendant / status changes after a cached clear are vetoed by fresh targeted custody", () => {
    const { runRoot, manager } = fixture();
    const now = Date.now();
    expect([...complete(manager, now)]).toEqual([]);
    for (const id of ["hist-0", "hist-1", "hist-2", "hist-3"]) expect(manager.retentionCustodyVeto(id)).toBe(false);
    const generation = fs.statSync(runRoot);

    // (a) live nested descendant added inside an existing run (root generation unchanged)
    writeStatus(path.join(runRoot, "hist-0", "nested", "child"), { status: "running", transport: "process", sessionId: String(process.pid) });
    // (b) in-place rewrite of status.json to a live writer (same inode, same directory)
    fs.writeFileSync(path.join(runRoot, "hist-1", "status.json"), JSON.stringify({ status: "running", transport: "process", sessionId: String(process.pid) }));
    // (c) in-place rewrite to a terminal record with an unknown PID identity
    fs.writeFileSync(path.join(runRoot, "hist-2", "status.json"), JSON.stringify({ status: "completed", transport: "process" }));
    // Counterexample: a nested descendant whose saved PID has exited is allowed.
    writeStatus(path.join(runRoot, "hist-3", "nested", "child"), { status: "completed", transport: "process", sessionId: EXITED_PID });

    const after = fs.statSync(runRoot);
    expect([after.mtimeMs, after.ctimeMs]).toEqual([generation.mtimeMs, generation.ctimeMs]);
    // The cached snapshot is still current and stale-clear for these runs ...
    const counts = observe(runRoot);
    const cached = manager.retentionReferences({ now: now + 1_000, budgetMs: 10_000, maxEntries: 100_000 });
    expect(counts.opendirRoot).toBe(0);
    for (const id of ["hist-0", "hist-1", "hist-2"]) expect(cached.has(id)).toBe(false);
    // ... but it is only a hint: the targeted custody veto re-proves the tree.
    expect(manager.retentionCustodyVeto("hist-0")).toBe(true);
    expect(manager.retentionCustodyVeto("hist-1")).toBe(true);
    expect(manager.retentionCustodyVeto("hist-2")).toBe(true);
    expect(manager.retentionCustodyVeto("hist-3")).toBe(false);
    expect(counts.statusReadsByRun.get("hist-0")).toBeGreaterThan(0);
    expect(counts.statusReadsByRun.get("hist-1")).toBeGreaterThan(0);
  });
});
