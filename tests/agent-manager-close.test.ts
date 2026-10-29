import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { hostedOnly } from "../scripts/hosted-only.mjs";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { activeBudgetState, appendBudgetLedger, readBudgetLedger } from "../src/agents/budget-ledger.js";
import { hasUnresolvedWorker, runTreeExitVeto } from "../src/storage/retention.js";
import { processAlive } from "../src/storage/scratch.js";
import * as agentRemoval from "../src/agents/rm.js";
import * as retention from "../src/storage/retention.js";

vi.mock("node:child_process", async original => {
  const actual = await original<typeof import("node:child_process")>();
  return { ...actual, execFileSync: vi.fn(actual.execFileSync) };
});
const custodyMoves = vi.hoisted(() => [] as Array<{ source: string; destination: string; receipt: string }>);
vi.mock("../src/agents/worktree-manager.js", async original => {
  const actual = await original<typeof import("../src/agents/worktree-manager.js")>();
  return { ...actual, retainCleanupRoot: (...args: Parameters<typeof actual.retainCleanupRoot>) => {
    const move = actual.retainCleanupRoot(...args);
    if (move) custodyMoves.push(move);
    return move;
  } };
});
const heldRoot = (source: string): string => {
  const move = custodyMoves.find(move => move.source === source)!;
  expect(move).toBeDefined();
  const row = { container: path.dirname(move.receipt), ...JSON.parse(fs.readFileSync(move.receipt, "utf8")) };
  expect(row).toMatchObject({ source, destination: expect.any(String), id: path.basename(source), reason: expect.any(String), time: expect.any(Number) });
  expect(JSON.parse(fs.readFileSync(path.join(row.container, "custody.prepared.json"), "utf8"))).toMatchObject({ source, destination: row.destination, id: row.id, state: "prepared" });
  if (process.platform !== "win32") expect(fs.statSync(row.container).mode & 0o777).toBe(0o700);
  expect(fs.existsSync(source)).toBe(false);
  return row.destination;
};
const hostedDelete = hostedOnly("AgentManager close deletion");
const managers: AgentManager[] = [];
const roots: string[] = [];
const setup = (retainRuns = true, extra: ConstructorParameters<typeof AgentManager>[2] = {}) => {
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "manager-close-test-"));
  roots.push(tempRoot);
  vi.spyOn(os, "tmpdir").mockReturnValue(tempRoot);
  vi.stubEnv("PI_FABRIC_RUN_ROOT", undefined);
  vi.stubEnv("PI_FABRIC_DEPTH", "0");
  for (const key of ["PI_FABRIC_BUDGET", "PI_FABRIC_BUDGET_FILE", "PI_FABRIC_BUDGET_ID"]) vi.stubEnv(key, undefined);
  const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, maxConcurrent: 1, retainRuns, transport: "process", sessionExport: false }, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
    sweepPath: path.resolve("dist/storage/sweep-main.js"), ...extra,
  });
  managers.push(manager);
  const name = fs.readdirSync(tempRoot).find((name) => name.startsWith("pi-fabric-runs-"));
  return { manager, tempRoot, root: name ? path.join(tempRoot, name) : extra.runRoot! };
};
// Native identities and bytes, not a write-buffer hash or the custody helper's
// claimed inventory. Rename may change ctime, so it is deliberately not compared.
const closeInventory = (root: string): unknown[] => {
  const rows: unknown[] = [];
  const walk = (relative: string): void => {
    const file = path.join(root, relative), stat = fs.lstatSync(file, { bigint: true });
    rows.push({ relative, dev: String(stat.dev), ino: String(stat.ino), mode: String(stat.mode),
      type: stat.isSymbolicLink() ? "symlink" : stat.isDirectory() ? "directory" : stat.isFile() ? "file" : "special",
      ...(stat.isSymbolicLink() ? { link: fs.readlinkSync(file) } : {}),
      ...(stat.isFile() ? { bytes: fs.readFileSync(file).toString("base64") } : {}) });
    if (stat.isDirectory()) for (const name of fs.readdirSync(file).sort()) walk(path.join(relative, name));
  };
  walk(""); return rows;
};
const closeProofs: Array<{ source: string; before: unknown[]; destination?: string; receipt?: string }> = [];
const closeProtectionProofs: Array<{ requests: string[][]; refused: boolean }> = [];
afterEach(async context => {
  const closed = await Promise.allSettled(managers.splice(0).map((manager) => manager.close()));
  vi.restoreAllMocks();
  vi.mocked(execFileSync).mockReset();
  vi.unstubAllEnvs();
  const evidence = process.env.PI_TEST_CUSTODY_EVIDENCE_DIR;
  if (evidence) {
    fs.mkdirSync(evidence, { recursive: true, mode: 0o700 });
    const out = fs.mkdtempSync(path.join(evidence, "manager-close-"));
    const fixtures = roots.map((root, index) => {
      const exists = fs.lstatSync(root, { throwIfNoEntry: false }) !== undefined;
      const raw = `fixture-${index}`;
      const inventory = exists ? closeInventory(root) : [];
      if (exists) fs.cpSync(root, path.join(out, raw), { recursive: true, dereference: false });
      return { root, exists, raw: exists ? raw : null, inventory };
    });
    fs.writeFileSync(path.join(out, "fixtures.json"), JSON.stringify({ test: context.task.name, fixtures, proofs: closeProofs,
      protection: closeProtectionProofs, closeOutcomes: closed.map(result => result.status === "fulfilled"
        ? { status: result.status } : { status: result.status, reason: String(result.reason) }) }, null, 2));
  }
  closeProofs.length = 0; closeProtectionProofs.length = 0;
  // Preserve originals and exports, including failed cases. The inherited owner
  // also captures the complete launcher root (including external archive siblings).
  roots.splice(0);
});

describe("AgentManager close storage", () => {
  it.each(["before-close", "during-drain"] as const)("joins a child settled %s without foreground consumption, but real wait still consumes", async (timing) => {
    const fence = vi.fn();
    const consumed = vi.fn();
    const settled = vi.fn();
    const stoppedAtClose = vi.fn();
    const { manager } = setup(true, {
      onBeforeResultReturned: fence, onResultConsumed: consumed,
      onSettled: settled, onStoppedAtClose: stoppedAtClose,
    });
    let release!: () => void;
    const drain = new Promise<void>((resolve) => { release = resolve; });
    let alive = true;
    let statusFile = "";
    const publish = () => {
      const record = JSON.parse(fs.readFileSync(statusFile, "utf8"));
      fs.writeFileSync(statusFile, JSON.stringify({ ...record, status: "completed", finishedAt: Date.now(),
        updatedAt: Date.now(), text: "unread full outcome", value: { private: "structured outcome" } }));
      alive = false;
    };
    const stop = vi.fn(async () => {
      if (timing === "during-drain") { publish(); await drain; }
      alive = false;
    });
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async (request) => {
      statusFile = request.workerArguments[request.workerArguments.indexOf("--status-file") + 1]!;
      fs.writeFileSync(statusFile, JSON.stringify({ id: request.id, name: request.name, task: "unread", status: "running",
        runner: "pi", transport: "process", cwd: request.cwd, startedAt: Date.now(), updatedAt: Date.now(),
        turns: 1, toolCalls: 0, text: "", usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 } }));
      return { kind: "process", isAlive: async () => alive, stop };
    });
    const child = await manager.spawn({ task: "unread", transport: "process", extensions: false });
    if (timing === "before-close") { publish(); await manager.join(child.id); }
    const statusReads = vi.spyOn(fs, "readFileSync");
    const closing = manager.close();
    try {
      if (timing === "during-drain") {
        await vi.waitFor(() => {
          expect(stop).toHaveBeenCalledOnce();
          // Both stop's snapshot and the monitor must see the terminal record
          // before the exact transport receipt releases either continuation.
          expect(statusReads.mock.calls.filter(([file]) => file === statusFile).length).toBeGreaterThanOrEqual(3);
        });
        release();
      }
      await closing;
    } finally { release(); await closing; statusReads.mockRestore(); }
    expect(stop).toHaveBeenCalledOnce();
    expect(settled).toHaveBeenCalledOnce();
    expect(fence).not.toHaveBeenCalled();
    expect(consumed).not.toHaveBeenCalled();
    if (timing === "before-close") expect(stoppedAtClose).not.toHaveBeenCalled();
    await expect(manager.wait(child.id)).resolves.toMatchObject({
      status: "completed", text: "unread full outcome", value: { private: "structured outcome" },
    });
    expect(fence).toHaveBeenCalledExactlyOnceWith(child.id);
    expect(consumed).toHaveBeenCalledExactlyOnceWith(child.id);
  });

  it.runIf(hostedDelete)("removes empty managed roots immediately, without recreating them on repeated close", async () => {
    const { manager, root } = setup();
    const first = manager.close();
    expect(manager.close()).toBe(first);
    await first;
    expect(fs.existsSync(root)).toBe(false);
    await manager.close();
    expect(fs.existsSync(root)).toBe(false);
    await expect(manager.spawn({ task: "late" })).rejects.toThrow("closing");
  });

  it("archives a whole stopped run with retainRuns:false before releasing the managed root", async () => {
    const { manager, root } = setup(false);
    const task = "HANG";
    const handle = await manager.spawn({ task, runner: "pi", extensions: false });
    expect(fs.readdirSync(root).length).toBeGreaterThan(1);
    await manager.close();
    await expect(manager.wait(handle.id)).resolves.toMatchObject({ status: "stopped" });
    const source = path.join(root, handle.id);
    const run = heldRoot(source);
    const taskPath = path.join(source, "task.txt");
    expect(fs.readdirSync(run)).toEqual(expect.arrayContaining(["manager-result.json", "status.json", "task.txt", "events.jsonl", "tmp"]));
    expect(JSON.parse(fs.readFileSync(path.join(run, "manager-result.json"), "utf8"))).toMatchObject({ id: handle.id, status: "stopped" });
    expect(JSON.parse(fs.readFileSync(path.join(run, "manager-result.json"), "utf8"))).toHaveProperty("task", task);
    expect(fs.readFileSync(path.join(run, "task.txt"))).toEqual(Buffer.from(task, "utf8"));
    const digest = createHash("sha256").update(Buffer.from(task, "utf8")).digest("hex");
    const persisted = JSON.parse(fs.readFileSync(path.join(run, "status.json"), "utf8"));
    expect(persisted).toMatchObject({ id: handle.id, status: "stopped", promptPath: taskPath,
      promptSha256: digest, promptBytes: Buffer.byteLength(task, "utf8"),
      inputSha256: digest, inputBytes: Buffer.byteLength(task, "utf8") });
    expect(persisted).toHaveProperty("task", task);
    expect(hasUnresolvedWorker(run)).toBe(false);
  });

  it.runIf(hostedDelete).each(["EBUSY", "EPERM"])("retries transient %s when removing an owned empty root", async (code) => {
    const { manager, root } = setup(false);
    const rmdir = fs.rmdirSync;
    let attempts = 0;
    vi.spyOn(fs, "rmdirSync").mockImplementation((directory) => {
      if (directory === root && attempts++ === 0) {
        expect(fs.readdirSync(root)).toEqual([]);
        throw Object.assign(new Error("Windows directory handle is still closing"), { code });
      }
      return rmdir(directory);
    });
    await manager.close();
    expect(attempts).toBe(2);
    expect(fs.existsSync(root)).toBe(false);
  });

  it("bounds retries for a persistently busy owned empty root", async () => {
    const { manager, root } = setup(false);
    const rmdir = fs.rmdirSync;
    let attempts = 0;
    vi.spyOn(fs, "rmdirSync").mockImplementation((directory) => {
      if (directory === root) {
        attempts++;
        throw Object.assign(new Error("directory remains busy"), { code: "EBUSY" });
      }
      return rmdir(directory);
    });
    const started = Date.now();
    await manager.close();
    expect(Date.now() - started).toBeGreaterThanOrEqual(2_000);
    expect(attempts).toBeGreaterThan(1);
    expect(attempts).toBeLessThanOrEqual(42);
    expect(fs.existsSync(root)).toBe(true);
    expect(fs.readdirSync(root)).toEqual([]);
  });

  it("preserves unknown contents appearing during an empty-root retry", async () => {
    const { manager, root } = setup(false);
    const rmdir = fs.rmdirSync;
    let attempts = 0;
    vi.spyOn(fs, "rmdirSync").mockImplementation((directory) => {
      if (directory === root && attempts++ === 0) {
        fs.writeFileSync(path.join(root, "unrelated"), "not an agent artifact");
        throw Object.assign(new Error("directory handle is still closing"), { code: "EBUSY" });
      }
      return rmdir(directory);
    });
    await manager.close();
    expect(attempts).toBe(2);
    expect(fs.readFileSync(path.join(root, "unrelated"), "utf8")).toBe("not an agent artifact");
  });

  it("archives durable-journal close collection with held bytes and a real receipt (#11199)", async () => {
    // Exercise actual rename custody; product deletion remains recorded.
    vi.spyOn(retention, "claimTempRunSweep").mockReturnValue(false);
    const remove = vi.spyOn(agentRemoval, "removeTree").mockResolvedValue(undefined);
    const { manager, root } = setup(false);
    const result = await manager.run({ task: "hello", runner: "pi", extensions: false });
    const directory = path.join(root, result.id);
    const store = path.join(directory, "durable", "initial", "store");
    fs.mkdirSync(store, { recursive: true });
    fs.writeFileSync(path.join(store, "..", "lease.sqlite"), "");
    fs.writeFileSync(path.join(store, "main.jsonl"), "{}\n");
    expect(retention.canRemoveTerminalRun(directory)).toBe(true);
    await manager.close();
    expect(remove).not.toHaveBeenCalledWith(directory);
    expect(fs.readFileSync(path.join(heldRoot(directory), path.relative(directory, store), "main.jsonl"), "utf8")).toBe("{}\n");
  });

  it.runIf(hostedDelete)("archives a finished run's pi-durable journal before releasing its empty managed root (#229)", async () => {
    const { manager, root } = setup(false);
    const result = await manager.run({ task: "hello", runner: "pi", extensions: false });
    // The pi-durable runner journals beside status.json in the run directory.
    const store = path.join(root, result.id, "durable", "initial", "store");
    fs.mkdirSync(store, { recursive: true });
    fs.writeFileSync(path.join(store, "..", "lease.sqlite"), "");
    fs.writeFileSync(path.join(store, "main.jsonl"), "{}\n");
    await manager.close();
    expect(fs.existsSync(root)).toBe(false);
    expect(fs.readFileSync(path.join(heldRoot(path.join(root, result.id)), "durable", "initial", "store", "main.jsonl"), "utf8")).toBe("{}\n");
  });

  it("retains closed managed run artifacts by default", async () => {
    const { manager, root } = setup(true);
    await manager.run({ task: "hello", runner: "pi", extensions: false });
    await manager.close();
    expect(fs.existsSync(root)).toBe(true);
    expect(JSON.parse(fs.readFileSync(path.join(root, ".fabric-owner.json"), "utf8"))).toMatchObject({ childrenStopped: true, closedAt: expect.any(Number) });
  });

  it("preserves unknown managed-root contents even when deletion was requested", async () => {
    const { manager, root } = setup(false);
    fs.writeFileSync(path.join(root, "unrelated"), "not an agent artifact");
    await manager.close();
    expect(fs.readFileSync(path.join(root, "unrelated"), "utf8")).toBe("not an agent artifact");
  });

  it("does not follow a replaced managed root or a malformed ownership marker", async () => {
    const { manager, root, tempRoot } = setup(false);
    fs.writeFileSync(path.join(root, ".fabric-owner.json"), "{}");
    await manager.close();
    expect(fs.existsSync(root)).toBe(true);
    expect(fs.existsSync(tempRoot)).toBe(true);
  });

  it.runIf(hostedDelete)("does not perform retention scans merely by constructing a manager", async () => {
    const { manager, tempRoot } = setup();
    const sentinel = path.join(tempRoot, "pi-fabric-runs-sentinel");
    fs.mkdirSync(sentinel);
    const marker = { pid: 2147483647, startedAt: 1, heartbeatAt: 1, orphanedAt: 1 };
    fs.writeFileSync(path.join(sentinel, ".fabric-owner.json"), JSON.stringify(marker));
    const second = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 });
    managers.push(second);
    await new Promise((resolve) => setImmediate(resolve));
    expect(fs.existsSync(sentinel)).toBe(true);
    const started = performance.now();
    await manager.close();
    // smarty-dev#2010: close only starts the detached sweep; it never waits for the walk.
    expect(performance.now() - started).toBeLessThan(5_000);
    await vi.waitFor(() => expect(fs.existsSync(sentinel)).toBe(false), { timeout: 20_000 });
  });

  it.runIf(hostedDelete)("starts the close sweep through a real JS runtime under a Bun-compiled Pi (smarty-dev#2010)", async () => {
    const originalExecPath = process.execPath;
    const originalOverride = process.env.PI_FABRIC_NODE_BINARY;
    const { manager, tempRoot } = setup();
    const sentinel = path.join(tempRoot, "pi-fabric-runs-bundled");
    fs.mkdirSync(sentinel);
    fs.writeFileSync(path.join(sentinel, ".fabric-owner.json"), JSON.stringify({ pid: 2147483647, startedAt: 1, heartbeatAt: 1, orphanedAt: 1 }));
    // process.execPath is the Pi executable itself, which cannot run sweep-main.js.
    process.execPath = "/usr/local/bin/pi";
    process.env.PI_FABRIC_NODE_BINARY = originalExecPath;
    try {
      await manager.close();
    } finally {
      process.execPath = originalExecPath;
      if (originalOverride === undefined) delete process.env.PI_FABRIC_NODE_BINARY;
      else process.env.PI_FABRIC_NODE_BINARY = originalOverride;
    }
    await vi.waitFor(() => expect(fs.existsSync(sentinel)).toBe(false), { timeout: 20_000 });
  });

  it.runIf(hostedDelete)("archives an explicit caller run after stopping a worker with retainRuns:false", async () => {
    const caller = fs.mkdtempSync(path.join(os.tmpdir(), "caller-stopped-root-"));
    roots.push(caller);
    const { manager, root } = setup(false, { runRoot: caller });
    const handle = await manager.spawn({ task: "HANG", runner: "pi", extensions: false });
    const run = manager.runDirectory(handle.id)!;
    expect(root).toBe(caller);
    expect(path.dirname(run)).toBe(caller);
    expect(fs.existsSync(path.join(root, ".fabric-owner.json"))).toBe(false);
    expect(fs.readFileSync(path.join(run, "task.txt"), "utf8")).toBe("HANG");
    await manager.close();
    await expect(manager.wait(handle.id)).resolves.toMatchObject({ status: "stopped" });
    expect(fs.readFileSync(path.join(heldRoot(run), "task.txt"), "utf8")).toBe("HANG");
    expect(fs.existsSync(root)).toBe(false);
  });

  it("preserves explicit caller roots with retainRuns:true", async () => {
    const caller = fs.mkdtempSync(path.join(os.tmpdir(), "caller-root-"));
    roots.push(caller);
    fs.writeFileSync(path.join(caller, "mine"), "caller data");
    const { manager } = setup(true, { runRoot: caller });
    await manager.close();
    expect(fs.readFileSync(path.join(caller, "mine"), "utf8")).toBe("caller data");
    expect(fs.existsSync(path.join(caller, ".fabric-owner.json"))).toBe(false);
  });

  it.runIf(hostedDelete).each(["live", "unknown", "exited"] as const)("keeps an evicted parent's owned budget until checked descendant exit (%s)", async (state) => {
    const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "manager-close-eviction-"));
    roots.push(tempRoot);
    vi.spyOn(os, "tmpdir").mockReturnValue(tempRoot);
    vi.stubEnv("PI_FABRIC_TMPDIR", undefined);
    vi.stubEnv("PI_FABRIC_RUN_ROOT", undefined);
    vi.stubEnv("PI_FABRIC_DEPTH", "0");
    for (const key of ["PI_FABRIC_BUDGET", "PI_FABRIC_BUDGET_FILE", "PI_FABRIC_BUDGET_ID"]) vi.stubEnv(key, undefined);
    const config = { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 100, maxConcurrent: 1, retainRuns: false, transport: "process" as const, sessionExport: false };
    const workerPath = path.resolve("tests/fixtures/fake-worker.mjs");
    const manager = new AgentManager(process.cwd(), config, { workerPath, runRoot: path.join(tempRoot, "runs") });
    managers.push(manager);
    const budget = activeBudgetState()!;
    expect(budget.budget).toBe(100);
    appendBudgetLedger(budget.file, { id: "existing-spend", depth: 0, cost: 0.25, tokens: 25, ts: Date.now() });
    const parent = await manager.run({ task: "recursive parent", recursive: true });
    const parentDirectory = manager.runDirectory(parent.id)!;
    // A real nested manager inherits the parent's ledger and launches a real worker.
    vi.stubEnv("PI_FABRIC_DEPTH", "1");
    const childManager = new AgentManager(process.cwd(), { ...config, retainRuns: true }, { workerPath, runRoot: path.join(parentDirectory, "nested") });
    managers.push(childManager);
    vi.stubEnv("PI_FABRIC_DEPTH", "0");
    const child = await childManager.spawn({ task: "HANG_WITH_PROGRESS", extensions: false });
    const childDirectory = childManager.runDirectory(child.id)!;
    const childStatus = path.join(childDirectory, "status.json");
    await vi.waitFor(() => expect(childManager.status(child.id)).toMatchObject({ status: "running", turns: 3 }), { timeout: 5_000 });
    const pid = Number(child.sessionId);
    expect(processAlive(pid)).toBe(true);
    // Persist the real transport identity, as production workers do; UNKNOWN is
    // an ordinary readable record with no identity and no unresolved marker.
    const record = JSON.parse(fs.readFileSync(childStatus, "utf8"));
    const { sessionId: _savedPid, ...withoutIdentity } = record;
    fs.writeFileSync(childStatus, JSON.stringify(state === "unknown" ? withoutIdentity : { ...record, sessionId: String(pid) }));

    // Exercise actual registration, settlement, and #pruneRetainedUiRecords:
    // only pressure-run execution is substituted, never #runs or its handle cap.
    // Both the handle and persisted record need an absent identity: settlement
    // alone cannot release the budget after these handles have been evicted.
    const absentPid = "2147483647";
    expect(processAlive(Number(absentPid))).toBe(false);
    const launch = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async (request) => {
      const arg = (flag: string) => request.workerArguments[request.workerArguments.indexOf(flag) + 1]!;
      fs.writeFileSync(arg("--status-file"), JSON.stringify({
        id: request.id, name: request.name, task: "eviction pressure", status: "completed", runner: "pi", transport: "process", sessionId: absentPid,
        cwd: request.cwd, startedAt: Date.now(), updatedAt: Date.now(), finishedAt: Date.now(), text: "done", turns: 1, toolCalls: 0,
        exitCode: 0, usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
      }));
      return { kind: "process", sessionId: absentPid, isAlive: async () => false, stop: async () => {} };
    });
    try {
      for (let index = 0; index < 1_000; index++) {
        expect((await manager.run({ task: `eviction pressure ${index}`, extensions: false })).status).toBe("completed");
      }
    } finally { launch.mockRestore(); }
    expect(manager.runDirectory(parent.id), "settled parent handle was really evicted").toBeUndefined();
    expect(processAlive(Number(parent.sessionId)), "evicted primary process has exited").toBe(false);
    expect(fs.existsSync(parentDirectory)).toBe(true);
    expect(hasUnresolvedWorker(parentDirectory)).toBe(false);
    if (state === "exited") {
      await childManager.stop(child.id);
      expect(processAlive(pid), "checked child process exit").toBe(false);
      expect(runTreeExitVeto(parentDirectory, 0, undefined, true)).toBeUndefined();
    } else {
      expect(runTreeExitVeto(parentDirectory, 0, undefined, true)).toMatch(state === "unknown" ? /unknown descendant identity/ : /descendant worker may still be running/);
    }
    const accounting = fs.readFileSync(budget.file, "utf8");
    expect(readBudgetLedger(budget.file).cost).toBeGreaterThanOrEqual(0.25);
    await manager.close();
    if (state === "exited") {
      expect(fs.existsSync(path.dirname(budget.file)), "checked descendant exit releases owned budget").toBe(false);
    } else {
      expect(fs.existsSync(budget.file), "evicted live/unknown descendant still owns the ledger").toBe(true);
      expect(fs.readFileSync(budget.file, "utf8")).toBe(accounting);
      const before = readBudgetLedger(budget.file);
      await childManager.stop(child.id);
      expect(processAlive(pid)).toBe(false);
      expect(readBudgetLedger(budget.file).cost, "descendant can still append its final accounting after parent close").toBeGreaterThan(before.cost);
    }
  }, 30_000);

  it("cancels queued admissions on close without launching and retains the active stopped prompt", async () => {
    const { manager, root } = setup(false);
    const active = await manager.spawn({ task: "HANG", extensions: false });
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    try {
      const queued = await manager.spawn({ task: "queued", extensions: false });
      expect(queued.status).toBe("queued");
      await manager.close();
      await expect(manager.wait(queued.id)).resolves.toMatchObject({ status: "stopped" });
      expect(launch).not.toHaveBeenCalled();
      // The queued admission never reached startPrepared, so it never wrote a
      // verified task file. The active run instead moves whole, before its now
      // empty root can retire; no status/task/result-only pruning is allowed.
      expect(fs.existsSync(path.join(root, queued.id))).toBe(false);
      const source = path.join(root, active.id), run = heldRoot(source);
      const taskPath = path.join(source, "task.txt");
      expect(fs.readdirSync(run)).toEqual(expect.arrayContaining(["manager-result.json", "status.json", "task.txt", "events.jsonl", "tmp"]));
      expect(JSON.parse(fs.readFileSync(path.join(run, "manager-result.json"), "utf8"))).toMatchObject({ id: active.id, status: "stopped" });
      expect(JSON.parse(fs.readFileSync(path.join(run, "manager-result.json"), "utf8"))).toHaveProperty("task", "HANG");
      expect(fs.readFileSync(path.join(run, "task.txt"), "utf8")).toBe("HANG");
      const digest = createHash("sha256").update("HANG", "utf8").digest("hex");
      const persisted = JSON.parse(fs.readFileSync(path.join(run, "status.json"), "utf8"));
      expect(persisted).toMatchObject({ id: active.id, status: "stopped", promptPath: taskPath,
        promptSha256: digest, promptBytes: Buffer.byteLength("HANG", "utf8"),
        inputSha256: digest, inputBytes: Buffer.byteLength("HANG", "utf8") });
      expect(persisted).toHaveProperty("task", "HANG");
      expect(hasUnresolvedWorker(run)).toBe(false);
    } finally { launch.mockRestore(); }
  });

  it.runIf(hostedDelete)("waits for a pending spawn to observe close before deleting the root", async () => {
    let resume!: () => void;
    const gate = new Promise<void>((resolve) => { resume = resolve; });
    const { manager, root } = setup(false, { preparePiModel: async () => { await gate; } });
    const spawning = manager.spawn({ task: "hello" });
    const rejected = expect(spawning).rejects.toThrow("closing");
    await Promise.resolve();
    const closed = manager.close();
    resume();
    await Promise.all([rejected, closed]);
    expect(fs.existsSync(root)).toBe(false);
  });
});

// Keep reason: data safety and the Windows custody trust boundary. Prepare
// fixtures on the native host; inject only close's Windows product branch on
// non-Windows. DACL commands are recorded, but roots/renames/receipts/readbacks
// are real. This is NOT native Windows ACL/privacy or POSIX durability proof;
// native ACL coverage remains in worktree-external-custody.
describe("AgentManager close Windows DACL batching contract", () => {
  const closeWindowsContract = async (manager: AgentManager) => {
    const platform = process.platform === "win32" ? undefined : vi.spyOn(process, "platform", "get").mockReturnValue("win32");
    try { await manager.close(); }
    finally { platform?.mockRestore(); }
  };
  const prepare = async () => {
    vi.spyOn(retention, "claimTempRunSweep").mockReturnValue(false);
    const beforeReturn = vi.fn(), consumed = vi.fn(), stoppedAtClose = vi.fn();
    expect(processAlive(2147483647), "the persisted test worker identity is really absent").toBe(false);
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
      const statusFile = request.workerArguments[request.workerArguments.indexOf("--status-file") + 1]!;
      fs.writeFileSync(statusFile, JSON.stringify({ id: request.id, name: request.name, task: "close batch", status: "completed",
        runner: "pi", transport: "process", sessionId: "2147483647", cwd: request.cwd,
        startedAt: Date.now(), updatedAt: Date.now(), finishedAt: Date.now(), text: `unread ${request.id}`, value: { id: request.id },
        turns: 1, toolCalls: 0, exitCode: 0, usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, cost: 0 } }));
      return { kind: "process", sessionId: "2147483647", isAlive: async () => false, stop: async () => {} };
    });
    const fixture = setup(false, { onBeforeResultReturned: beforeReturn, onResultConsumed: consumed, onStoppedAtClose: stoppedAtClose });
    const runs: Array<{ id: string; source: string; before: unknown[]; destination?: string; receipt?: string }> = [];
    for (let index = 0; index < 64; index++) {
      const handle = await fixture.manager.spawn({ task: `close batch ${index}`, extensions: false, transport: "process" });
      await fixture.manager.join(handle.id); // Join, never acknowledge/consume.
      const source = fixture.manager.runDirectory(handle.id)!;
      fs.mkdirSync(path.join(source, "raw"));
      fs.writeFileSync(path.join(source, "raw", "capture.JSONL"), Buffer.from([index, 0, 255, 13, 10]));
      expect(runTreeExitVeto(source, 0, undefined, true)).toBeUndefined();
      expect(fixture.manager.status(handle.id)).toMatchObject({ status: "completed", text: `unread ${handle.id}`, value: { id: handle.id } });
      const run = { id: handle.id, source, before: closeInventory(source) };
      runs.push(run); closeProofs.push(run);
    }
    const unconsumed = () => {
      expect(beforeReturn).not.toHaveBeenCalled();
      expect(consumed).not.toHaveBeenCalled();
      expect(stoppedAtClose).not.toHaveBeenCalled();
    };
    unconsumed();
    return { ...fixture, runs, unconsumed };
  };
  const recordProtection = async (refuse = false) => {
    if (process.platform !== "win32") vi.stubEnv("SystemRoot", "C:\\Windows");
    const actual = await vi.importActual<typeof import("node:child_process")>("node:child_process");
    const requests: string[][] = [];
    closeProtectionProofs.push({ requests, refused: refuse });
    vi.mocked(execFileSync).mockImplementation(((file: string, args: string[], options: { env?: NodeJS.ProcessEnv; input?: string | Buffer }) => {
      if (path.win32.basename(String(file)).toLowerCase() !== "powershell.exe") return actual.execFileSync(file, args, options);
      const encoded = args.indexOf("-EncodedCommand");
      expect(encoded).toBeGreaterThanOrEqual(0);
      const script = Buffer.from(args[encoded + 1]!, "base64").toString("utf16le");
      expect(script).toContain("Set-Acl"); expect(script).toContain("Get-Acl");
      const input = options.input === undefined ? [] : [String(options.input)];
      // The observed PS 5.1 transport uses ASCII base64-wrapped UTF-8 JSON on
      // stdin. Keep both the literal request and its decoded path list.
      if (input.length && script.includes("FromBase64String")) input.push(Buffer.from(input[0]!, "base64").toString("utf8"));
      requests.push([script, ...args, ...Object.entries(options.env ?? {})
        .filter(([key]) => key.startsWith("PI_FABRIC_CLEANUP_CONTAINER"))
        .map(([, value]) => value).filter((value): value is string => value !== undefined),
        ...input]);
      if (refuse) throw Object.assign(new Error("recorded DACL protection/readback refusal"), { code: "EACCES" });
      return Buffer.alloc(0); // Command contract only; no native DACL has been certified.
    }) as typeof execFileSync);
    return requests;
  };
  const observeMoves = (runs: Awaited<ReturnType<typeof prepare>>["runs"], requests: string[][]) => {
    const nativeRename = fs.renameSync;
    const moved = new Set<string>();
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      const run = runs.find(run => run.source === String(from));
      if (run) {
        expect(requests.length, "protection must finish before ANY payload rename").toBeGreaterThan(0);
        const container = path.dirname(String(to));
        // Accept literal or JSON-list command transport without importing a new
        // batching API. Every moved root must be named in an actual request.
        // Do not refuse old-source moves here: let all real readbacks complete,
        // then diagnose the causal N-process regression at the final count.
        expect(requests.some(request => request.some(value => value.includes(container) || value.includes(JSON.stringify(container)))), container).toBe(true);
        expect(moved.has(run.source), "a source is never captured twice").toBe(false);
      }
      nativeRename(from, to);
      if (run) {
        moved.add(run.source); run.destination = String(to); run.receipt = path.join(path.dirname(String(to)), "custody.json");
      }
    });
    return moved;
  };
  const verifyHeld = (run: Awaited<ReturnType<typeof prepare>>["runs"][number]) => {
    expect(run.destination).toBeDefined(); expect(run.receipt).toBeDefined();
    expect(fs.lstatSync(run.source, { throwIfNoEntry: false })).toBeUndefined();
    expect(closeInventory(run.destination!)).toEqual(run.before);
    const prepared = JSON.parse(fs.readFileSync(path.join(path.dirname(run.receipt!), "custody.prepared.json"), "utf8"));
    expect(prepared).toMatchObject({ id: run.id, source: run.source, destination: run.destination, state: "prepared", reason: "agent cleanup run directory", time: expect.any(Number) });
    expect(JSON.parse(fs.readFileSync(run.receipt!, "utf8"))).toMatchObject({ ...prepared, state: "moved", time: expect.any(Number) });
  };

  it("protects 64 collectible roots in ONE Windows DACL process with real per-root custody", async () => {
    const { manager, runs, unconsumed } = await prepare();
    const requests = await recordProtection(), moved = observeMoves(runs, requests);
    await closeWindowsContract(manager);
    expect(moved.size).toBe(64);
    expect(new Set(runs.map(run => run.destination)).size).toBe(64);
    expect(new Set(runs.map(run => run.receipt)).size).toBe(64);
    for (const run of runs) {
      verifyHeld(run);
      expect(manager.status(run.id)).toMatchObject({ status: "completed", text: "", warnings: expect.arrayContaining([expect.stringContaining("collected at close")]) });
    }
    unconsumed(); expect(requests).toHaveLength(1);
  });

  it("refuses a failed Windows DACL batch before movement without acknowledgement or result collection", async () => {
    const { manager, root, runs, unconsumed } = await prepare();
    const requests = await recordProtection(true), moved = observeMoves(runs, requests);
    await closeWindowsContract(manager); // Close's best-effort collection is not custody authority.
    expect(moved.size).toBe(0);
    expect(vi.mocked(fs.renameSync).mock.calls.some(([from]) => runs.some(run => run.source === String(from))),
      "a failed protection batch must refuse even the payload rename attempt").toBe(false);
    const archive = `${root}.trace-archive`;
    if (fs.existsSync(archive)) for (const container of fs.readdirSync(archive)) {
      expect(fs.existsSync(path.join(archive, container, "tree")), "no copied/captured payload after protection failure").toBe(false);
      const receipt = path.join(archive, container, "custody.json");
      if (fs.existsSync(receipt)) expect(JSON.parse(fs.readFileSync(receipt, "utf8")).state).not.toBe("moved");
    }
    for (const run of runs) {
      expect(closeInventory(run.source)).toEqual(run.before);
      expect(run.destination).toBeUndefined(); expect(run.receipt).toBeUndefined();
      expect(manager.status(run.id)).toMatchObject({ status: "completed", text: `unread ${run.id}`, value: { id: run.id } });
    }
    unconsumed(); expect(requests).toHaveLength(1);
  });

  it("does not falsely collect missing or vetoed roots while batching the eligible roots", async () => {
    const { manager, tempRoot, runs, unconsumed } = await prepare();
    const [missing, vetoed, ...eligible] = runs;
    const parked = path.join(tempRoot, "externally-retained-missing-root");
    fs.renameSync(missing!.source, parked); // Real absence; keep every original byte as evidence.
    fs.writeFileSync(path.join(vetoed!.source, "archive-pending.json"), JSON.stringify({ format: 1, awaitingResult: true, ownerPid: process.pid }));
    vetoed!.before = closeInventory(vetoed!.source);
    expect(runTreeExitVeto(vetoed!.source, 0, undefined, true)).toMatch(/archive is pending/);
    const requests = await recordProtection(), moved = observeMoves(runs, requests);
    await closeWindowsContract(manager);
    expect([...moved].sort()).toEqual(eligible.map(run => run.source).sort());
    for (const run of eligible) verifyHeld(run);
    expect(missing!.destination).toBeUndefined(); expect(vetoed!.destination).toBeUndefined();
    expect(closeInventory(parked)).toEqual(missing!.before);
    expect(closeInventory(vetoed!.source)).toEqual(vetoed!.before);
    expect(() => manager.status(missing!.id)).toThrow(/result unavailable or unauthenticated/);
    expect(manager.status(vetoed!.id)).toMatchObject({ status: "completed", text: `unread ${vetoed!.id}`, value: { id: vetoed!.id } });
    unconsumed(); expect(requests).toHaveLength(1);
  });
});
