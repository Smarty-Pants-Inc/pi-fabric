import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";

const removalIntents = vi.hoisted(() => [] as string[]);
const sweepIntents = vi.hoisted(() => [] as string[]);
vi.mock("../src/storage/verified-remove.js", async (importOriginal) => ({
  ...await importOriginal<typeof import("../src/storage/verified-remove.js")>(),
  verifiedRemove: vi.fn((target: string) => {
    sweepIntents.push(target);
    return { removed: true };
  }),
}));
vi.mock("../src/agents/rm.js", () => ({
  // Intentionally non-destructive: these tests inspect manager.close's exact
  // removal plan and never execute product deletion paths.
  removeTree: vi.fn(async (target: string) => { removalIntents.push(target); }),
}));

import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { AgentManager } from "../src/agents/manager.js";
import type { AgentRunRecord } from "../src/agents/types.js";
import { sweepTempRunRoots } from "../src/storage/retention.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { processAlive } from "../src/storage/scratch.js";
import { fakeManagerHeapWorker } from "./fixtures/manager-heap-probe.js";

const managers: AgentManager[] = [];
const roots: string[] = [];
const workerPath = path.resolve("tests/fixtures/fake-worker.mjs");
const nativeTempRoot = os.tmpdir();
const queuedRollbackIntents: string[] = [];

const fixture = (agents: Partial<typeof DEFAULT_FABRIC_CONFIG.agents> = {}, explicitRoot = false) => {
  const tempRoot = fs.mkdtempSync(path.join(nativeTempRoot, "agent-prompt-retention-"));
  roots.push(tempRoot);
  // Keep the real managed ownership/sweep path inside this fixture on every host.
  vi.spyOn(os, "tmpdir").mockReturnValue(tempRoot);
  vi.stubEnv("PI_FABRIC_TMPDIR", undefined);
  vi.stubEnv("PI_FABRIC_RUN_ROOT", undefined);
  vi.stubEnv("PI_FABRIC_DEPTH", "0");
  for (const key of ["PI_FABRIC_BUDGET", "PI_FABRIC_BUDGET_FILE", "PI_FABRIC_BUDGET_ID"]) vi.stubEnv(key, undefined);
  const runRoot = explicitRoot ? path.join(tempRoot, "explicit-runs") : undefined;
  const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, sessionExport: false, ...agents, retainRuns: false }, {
    workerPath, ...(runRoot ? { runRoot } : {}),
  });
  managers.push(manager);
  const root = runRoot ?? path.join(tempRoot, fs.readdirSync(tempRoot).find(name => name.startsWith("pi-fabric-runs-"))!);
  return { root, tempRoot, manager };
};

// Queued rollback bypasses removeTree. Keep this narrow fence registered through
// afterEach's manager.close even when an assertion fails; atomic temp-file cleanup
// still passes through, and fixture cleanup runs only after mocks are restored.
const recordQueuedRollback = (directory: string): void => {
  const remove = fs.rmSync.bind(fs);
  const queuedResult = path.join(directory, "queued-result.json");
  vi.spyOn(fs, "rmSync").mockImplementation((target, options) => {
    if (String(target) === directory || String(target) === queuedResult) {
      queuedRollbackIntents.push(String(target));
      return;
    }
    remove(target, options);
  });
};
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
const hash = (value: string | Buffer) => createHash("sha256").update(value).digest("hex");
const retainedPath = (file: string): string => {
  const move = custodyMoves.find(move => move.source === path.dirname(file));
  return move ? path.join(move.destination, path.basename(file)) : file;
};
const rawStatus = (promptPath: string): string => fs.readFileSync(retainedPath(path.join(path.dirname(promptPath), "status.json")), "utf8");
const savedStatus = (promptPath: string): AgentRunRecord => JSON.parse(rawStatus(promptPath)) as AgentRunRecord;
const expectWholeRun = (source: string, files: Map<string, Buffer>, inode: number): string => {
  const held = heldRoot(source);
  expect(fs.statSync(held).ino).toBe(inode);
  for (const [name, bytes] of files) expect(fs.readFileSync(path.join(held, name)), name).toEqual(bytes);
  return held;
};
const seedRawTrace = (directory: string): Map<string, Buffer> => {
  const files = new Map<string, Buffer>();
  for (const [name, bytes] of [
    ["session.NdJsOn.backup", Buffer.from('session raw π\n{"torn":')],
    ["logs/stderr.log", Buffer.from("stderr raw\u0000tail")],
    ["tmp/opaque.bin", Buffer.from([0, 255, 13, 10])],
  ] as const) {
    fs.mkdirSync(path.dirname(path.join(directory, name)), { recursive: true });
    fs.writeFileSync(path.join(directory, name), bytes);
    files.set(name, bytes);
  }
  for (const name of ["task.txt", "status.json", "manager-result.json", "events.jsonl"]) {
    if (fs.existsSync(path.join(directory, name))) files.set(name, fs.readFileSync(path.join(directory, name)));
  }
  return files;
};

/** Opt-in owned-fixture COPY before any command/global teardown, never inode custody. */
const exportOwnedFixtures = (): void => {
  const evidence = process.env.PI_TEST_EVIDENCE_DIR;
  if (!evidence) return;
  fs.mkdirSync(evidence, { recursive: true });
  for (const source of roots) {
    let bytes = 0;
    const inventory = (base: string): Array<{ path: string; sha256?: string }> => {
      const rows: Array<{ path: string; sha256?: string }> = [];
      const walk = (relative: string): void => {
        const file = path.join(base, relative), stat = fs.lstatSync(file);
        if (rows.length >= 5_000 || stat.isSymbolicLink() || (!stat.isDirectory() && (!stat.isFile() || stat.nlink !== 1))) throw new Error(`Unsafe prompt fixture export: ${file}`);
        if (stat.isDirectory()) { rows.push({ path: relative }); for (const name of fs.readdirSync(file).sort()) walk(path.join(relative, name)); }
        else { if ((bytes += stat.size) > 96 * 1024 * 1024) throw new Error("Prompt fixture export bound exceeded"); rows.push({ path: relative, sha256: hash(fs.readFileSync(file)) }); }
      };
      walk(""); return rows;
    };
    const before = inventory(source), destination = fs.mkdtempSync(path.join(evidence, "prompt-"));
    fs.cpSync(source, path.join(destination, "tree"), { recursive: true, dereference: false });
    expect(inventory(source)).toEqual(before);
    expect(inventory(path.join(destination, "tree"))).toEqual(before);
    fs.writeFileSync(path.join(destination, "export.json"), JSON.stringify({ source, destination: path.join(destination, "tree"), method: "pre-teardown copy; SHA256 independent readback; no inode claim", entries: before }));
  }
};

afterEach(async () => {
  // removeTree/verifiedRemove stay recorded; queued direct rollback stays fenced
  // until every manager close has joined, including on a failed assertion.
  const outcomes = await Promise.allSettled(managers.splice(0).map(async manager => {
    manager.config.retainRuns = true;
    await manager.close();
  }));
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  exportOwnedFixtures();
  // Raw originals still remain owned by the command's inherited test-temp owner.
  roots.splice(0);
  removalIntents.splice(0);
  sweepIntents.splice(0);
  queuedRollbackIntents.splice(0);
  expect(outcomes.filter(outcome => outcome.status === "rejected")).toEqual([]);
});

describe("Agent prompt retention at manager close (#11663)", () => {
  it("archives whole failed and successful trees without rewriting prompt or outcome bytes", async () => {
    const { root, manager } = fixture();
    const prompt = "FAIL_DIRECTIVE\nprivate brief with exact bytes: π\n";
    const failed = await manager.spawn({ name: "failed control", task: prompt, transport: "process" });
    await expect(manager.wait(failed.id)).resolves.toMatchObject({ status: "failed" });
    const failedRecord = manager.status(failed.id) as AgentRunRecord;
    const successful = await manager.spawn({ task: "successful task", transport: "process" });
    await expect(manager.wait(successful.id)).resolves.toMatchObject({ status: "completed" });

    const taskPath = failedRecord.promptPath!;
    const failedDirectory = path.dirname(taskPath);
    const successDirectory = path.join(root, successful.id);
    expect(failedRecord).toMatchObject({ promptSha256: hash(prompt), promptBytes: Buffer.byteLength(prompt), promptPath: expect.any(String) });
    expect(fs.readFileSync(taskPath, "utf8")).toBe(prompt);
    // Windows has no POSIX modes (repository private-dir convention): exact
    // mode bits are POSIX-only; every digest/bytes/artifact assertion remains.
    if (process.platform !== "win32") {
      expect(fs.statSync(taskPath).mode & 0o777).toBe(0o600);
      expect(fs.statSync(failedDirectory).mode & 0o777).toBe(0o700);
    }

    const files = seedRawTrace(failedDirectory), inode = fs.statSync(failedDirectory).ino;
    removalIntents.splice(0);
    await manager.close();
    expectWholeRun(failedDirectory, files, inode);
    expect(fs.readFileSync(path.join(heldRoot(successDirectory), "task.txt"), "utf8")).toBe("successful task");
    expect(removalIntents).not.toContain(successDirectory);
    expect(removalIntents).not.toContain(failedDirectory);
    expect(removalIntents).not.toContain(path.join(failedDirectory, "status.json"));
    expect(removalIntents).not.toContain(taskPath);
    expect(savedStatus(taskPath)).toMatchObject({
      status: "failed", promptSha256: hash(prompt), promptBytes: Buffer.byteLength(prompt), promptPath: taskPath,
      inputSha256: hash(prompt), inputBytes: Buffer.byteLength(prompt),
    });
    expect(savedStatus(taskPath).task).toBe(prompt);
    expect(manager.readLog(failed.id).status?.task).toBe("");
    expect(() => manager.listForUi()).not.toThrow();
  });

  // Stop, AbortSignal cancellation and deadline all use the same managed
  // terminal retention path; the real prompt verifier is never mocked.
  it.each(["stopped", "cancelled", "timed_out"] as const)("archives the whole verified current run after %s", async (outcome) => {
    const { manager } = fixture(outcome === "timed_out" ? { timeoutMs: 1_500 } : {});
    const controller = new AbortController();
    const prompt = `HANG until ${outcome}: current brief π\n`;
    const handle = await manager.spawn({ name: "terminal control", task: prompt, transport: "process" }, controller.signal);
    const terminal = outcome === "timed_out" ? "timed_out" : "stopped";
    if (outcome === "stopped") await expect(manager.stop(handle.id)).resolves.toMatchObject({ status: terminal });
    else {
      if (outcome === "cancelled") controller.abort(new Error("cancel terminal control"));
      await expect(manager.wait(handle.id)).resolves.toMatchObject({ status: terminal });
    }
    const directory = manager.runDirectory(handle.id)!;
    const taskPath = path.join(directory, "task.txt");
    const files = seedRawTrace(directory), inode = fs.statSync(directory).ino;
    const fd = fs.openSync(path.join(directory, "session.NdJsOn.backup"), "a");
    removalIntents.splice(0);
    await manager.close();
    try { fs.writeSync(fd, "late FD tail"); } finally { fs.closeSync(fd); }
    files.set("session.NdJsOn.backup", Buffer.concat([files.get("session.NdJsOn.backup")!, Buffer.from("late FD tail")]));
    expectWholeRun(directory, files, inode);
    expect(removalIntents).not.toContain(directory);
    expect(removalIntents).not.toContain(taskPath);
    expect(removalIntents).not.toContain(path.join(directory, "status.json"));
    expect(fs.readFileSync(retainedPath(taskPath), "utf8")).toBe(prompt);
    expect(savedStatus(taskPath)).toMatchObject({ status: terminal, promptSha256: hash(prompt), promptBytes: Buffer.byteLength(prompt), inputSha256: hash(prompt), inputBytes: Buffer.byteLength(prompt) });
    expect(savedStatus(taskPath).promptRetentionDropped).toBeUndefined();
    expect(savedStatus(taskPath).task).toBe(prompt);
  });

  // A live worker closes with checked ESRCH and a retained pair. Never-admitted
  // queued work has no task artifact: record its direct rollback, never invent one.
  it("retains the stopped active prompt while closing queued admission without a launch", async () => {
    const { root, manager } = fixture({ maxConcurrent: 1 });
    const prompt = "HANG_WITH_PROGRESS\nactive close brief π\n";
    const active = await manager.spawn({ name: "active close control", task: prompt, transport: "process" });
    await vi.waitFor(() => expect(manager.status(active.id)).toMatchObject({ status: "running", turns: 3 }), { timeout: 5_000 });
    const pid = Number((manager.status(active.id) as AgentRunRecord).sessionId);
    expect(Number.isSafeInteger(pid) && pid > 0 && processAlive(pid)).toBe(true);
    const directory = manager.runDirectory(active.id)!;
    const taskPath = path.join(directory, "task.txt");
    const files = seedRawTrace(directory), inode = fs.statSync(directory).ino;
    const launchSpy = vi.spyOn(ProcessTransport.prototype, "launch"); // observer only: passes through
    try {
      const queued = await manager.spawn({ name: "queued control", task: "queued successful task", transport: "process" });
      const queuedDirectory = path.join(root, queued.id);
      recordQueuedRollback(queuedDirectory); // remains installed through afterEach
      expect(manager.status(queued.id)).toMatchObject({ status: "queued" });
      expect(fs.existsSync(path.join(queuedDirectory, "task.txt"))).toBe(false);
      const queuedWait = manager.wait(queued.id);

      removalIntents.splice(0);
      await manager.close();
      await expect(queuedWait).resolves.toMatchObject({ status: "stopped" });
      expect(launchSpy).not.toHaveBeenCalled();
      expect(processAlive(pid), "checked ESRCH for the captured primary").toBe(false);
      expect(removalIntents).not.toContain(directory);
      expect(removalIntents).not.toContain(taskPath);
      expect(removalIntents).not.toContain(path.join(directory, "status.json"));
      // Running status/result files legitimately settle at close; raw trace bytes must not change.
      files.delete("status.json"); files.delete("manager-result.json"); files.delete("events.jsonl");
      expectWholeRun(directory, files, inode);
      expect(fs.readFileSync(retainedPath(taskPath), "utf8")).toBe(prompt);
      expect(savedStatus(taskPath)).toMatchObject({ status: "stopped", promptSha256: hash(prompt), inputSha256: hash(prompt), inputBytes: Buffer.byteLength(prompt) });
      expect(queuedRollbackIntents).toEqual([path.join(queuedDirectory, "queued-result.json"), queuedDirectory]);
      expect(removalIntents).not.toContain(queuedDirectory);
      const heldQueued = heldRoot(queuedDirectory);
      expect(fs.existsSync(path.join(heldQueued, "task.txt"))).toBe(false);
      expect(savedStatus(path.join(heldQueued, "task.txt"))).toMatchObject({ status: "stopped", queuedArchiveCommitted: true });
    } finally {
      launchSpy.mockRestore();
    }
  });

  it.each(["failed", "stopped", "timed_out", "completed"] as const)("archives the whole tree for an explicit-root %s run", async (terminal) => {
    const { root, manager } = fixture(terminal === "timed_out" ? { timeoutMs: 1_500 } : {}, true);
    const prompt = terminal === "failed" ? "FAIL_DIRECTIVE\nexplicit failure" : terminal === "completed" ? "explicit success" : "HANG explicit terminal";
    const handle = await manager.spawn({ name: "explicit root control", task: prompt, transport: "process" });
    if (terminal === "stopped") await expect(manager.stop(handle.id)).resolves.toMatchObject({ status: terminal });
    else await expect(manager.wait(handle.id)).resolves.toMatchObject({ status: terminal });
    const directory = path.join(root, handle.id);

    removalIntents.splice(0);
    await manager.close();
    expect(removalIntents.filter(target => target === directory || target.startsWith(directory + path.sep))).toEqual([]);
    const held = heldRoot(directory);
    expect(savedStatus(path.join(held, "task.txt")).promptRetentionDropped).toBeUndefined();
    expect(fs.readFileSync(path.join(held, "task.txt"), "utf8")).toBe(prompt);
    expect(JSON.parse(fs.readFileSync(path.join(held, "manager-result.json"), "utf8"))).toMatchObject({ id: handle.id, status: terminal });
  });

  it("archives original nested task fields and late raw status without adopting late outcome authority", async () => {
    const { manager } = fixture();
    const prompt = "FAIL_DIRECTIVE\nparent-task-sentinel-π\n";
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
      const transport = await fakeManagerHeapWorker(request);
      const statusFile = request.workerArguments[request.workerArguments.indexOf("--status-file") + 1]!;
      const record = { ...JSON.parse(fs.readFileSync(statusFile, "utf8")), status: "failed", error: "privacy control failure", exitCode: 1 };
      fs.writeFileSync(statusFile, JSON.stringify(record), { mode: 0o600 });
      const child = path.join(path.dirname(statusFile), "nested", "child");
      const grandchild = path.join(child, "nested", "grandchild");
      fs.mkdirSync(grandchild, { recursive: true, mode: 0o700 });
      for (const [directory, id, task] of [[child, "child", "nested-task-sentinel-λ"], [grandchild, "grandchild", "grandchild-task-sentinel-δ"]] as const) {
        fs.writeFileSync(path.join(directory, "status.json"), JSON.stringify({ ...record, id, name: "nested neutral name", task, text: "preserved nested diagnostic" }), { mode: 0o600 });
      }
      return transport; // All nested prompt fields exist before settlement.
    });
    const handle = await manager.spawn({ name: "nested privacy control", task: prompt, transport: "process", extensions: false });
    const result = await manager.wait(handle.id);
    expect(result.status).toBe("failed");
    const record = manager.status(handle.id) as AgentRunRecord;
    expect(record.task).toBe(prompt); // Live task policy is unchanged.
    expect(record.nestedAgents).toEqual(result.nestedAgents);
    expect(record.nestedAgents![0]!.task).toBe("nested-task-sentinel-λ");
    expect(record.nestedAgents![0]!.nestedAgents![0]!.task).toBe("grandchild-task-sentinel-δ");
    const child = path.join(path.dirname(record.promptPath!), "nested", "child");
    fs.writeFileSync(path.join(child, "status.json"), JSON.stringify({ ...record, id: "child", task: "late-task-sentinel", text: "late diagnostic" }));
    const late = path.join(path.dirname(record.promptPath!), "nested", "late-child");
    fs.mkdirSync(late);
    fs.writeFileSync(path.join(late, "status.json"), JSON.stringify({ ...record, id: "late-child", task: "late-task-sentinel", text: "late diagnostic" }));

    const run = path.dirname(record.promptPath!);
    const originalResult = fs.readFileSync(path.join(run, "manager-result.json"));
    const lateStatus = fs.readFileSync(path.join(child, "status.json"));
    await manager.close();
    const held = heldRoot(run);
    expect(fs.readFileSync(path.join(held, "manager-result.json"))).toEqual(originalResult);
    const published = JSON.parse(originalResult.toString());
    expect(published.nestedAgents).toHaveLength(1);
    expect(published.nestedAgents[0].task).toBe("nested-task-sentinel-λ");
    expect(published.nestedAgents[0].nestedAgents[0].task).toBe("grandchild-task-sentinel-δ");
    expect(fs.readFileSync(path.join(held, "nested", "child", "status.json"))).toEqual(lateStatus);
    expect(fs.readFileSync(path.join(held, "nested", "late-child", "status.json"), "utf8")).toContain("late-task-sentinel");
    expect(manager.readLog(handle.id).status!.task).toBe("");
    expect(() => manager.listForUi()).not.toThrow();
  });

  it("archives tampered task bytes unchanged without rewriting the authenticated outcome", async () => {
    const { manager } = fixture();
    const original = "FAIL_DIRECTIVE\noriginal-task-sentinel-π\n";
    const tampered = "tampered-task-sentinel-λ\n";
    const handle = await manager.spawn({ name: "tamper control", task: original, transport: "process" });
    await expect(manager.wait(handle.id)).resolves.toMatchObject({ status: "failed" });
    const taskPath = (manager.status(handle.id) as AgentRunRecord).promptPath!;
    const failedDirectory = path.dirname(taskPath);
    fs.writeFileSync(taskPath, tampered, { mode: 0o600 });

    const files = seedRawTrace(failedDirectory), inode = fs.statSync(failedDirectory).ino;
    removalIntents.splice(0);
    await manager.close();
    expectWholeRun(failedDirectory, files, inode);
    expect(removalIntents).not.toContain(taskPath);
    expect(savedStatus(taskPath)).toMatchObject({ task: original, promptSha256: hash(original), inputSha256: hash(original), inputBytes: Buffer.byteLength(original) });
    expect(fs.readFileSync(retainedPath(taskPath), "utf8")).toBe(tampered);
  });

  it("retains the current failed continuation bytes while preserving the original prompt digest", async () => {
    const { manager } = fixture();
    const original = "RESUME_AFTER_STOP RESUME_FAIL_AFTER_STOP\noriginal brief π\n";
    const handle = await manager.spawn({ name: "continuation control", task: original, transport: "process" });
    await expect(manager.wait(handle.id)).resolves.toMatchObject({ status: "failed" });
    const beforeClose = manager.status(handle.id) as AgentRunRecord;
    const taskPath = beforeClose.promptPath!;
    const bytes = fs.readFileSync(taskPath);
    expect(beforeClose.promptSha256).toBe(hash(original));
    expect(beforeClose.inputSha256).not.toBe(beforeClose.promptSha256);
    expect(hash(bytes)).toBe(beforeClose.inputSha256);
    expect(bytes.byteLength).toBe(beforeClose.inputBytes);

    removalIntents.splice(0);
    await manager.close();
    expect(removalIntents).not.toContain(taskPath);
    expect(removalIntents).not.toContain(path.join(path.dirname(taskPath), "status.json"));
    expect(savedStatus(taskPath)).toMatchObject({
      promptSha256: beforeClose.promptSha256,
      promptBytes: beforeClose.promptBytes,
      inputSha256: beforeClose.inputSha256,
      inputBytes: beforeClose.inputBytes,
    });
    expect(savedStatus(taskPath).promptRetentionDropped).toBeUndefined();
    expect(savedStatus(taskPath).task).toContain("[Fabric continuation]");
    expect(fs.readFileSync(retainedPath(taskPath))).toEqual(bytes);
  });

  it("persists the rewritten current digest when a continuation launch fails before manager close", async () => {
    const { manager } = fixture();
    const original = "RESUME_AFTER_STOP\noriginal launch-failure brief π\n";
    const launch = ProcessTransport.prototype.launch;
    let launches = 0;
    const launchSpy = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function (this: ProcessTransport, request) {
      if (++launches === 2) throw new Error("replacement launch failed");
      return launch.call(this, request);
    });
    let renameSpy: ReturnType<typeof vi.spyOn> | undefined;
    try {
      const handle = await manager.spawn({ name: "launch failure control", task: original, transport: "process" });
      const taskPath = handle.promptPath!;
      const rename = fs.renameSync.bind(fs);
      let sawPreRewriteMismatch = false;
      renameSpy = vi.spyOn(fs, "renameSync").mockImplementation((source, target) => {
        if (String(target) === taskPath) {
          const intermediate = savedStatus(taskPath);
          expect(intermediate.promptSha256).toBe(hash(original));
          expect(intermediate.inputSha256).not.toBe(hash(fs.readFileSync(taskPath)));
          sawPreRewriteMismatch = true;
        }
        rename(source, target);
      });
      const result = await manager.wait(handle.id);
      expect(result.error).toContain("replacement launch failed");
      expect(launches).toBe(2);
      expect(sawPreRewriteMismatch).toBe(true);
      // Simulate host death before close: authenticate the on-disk record now,
      // not the manager-enriched public result or close-time status rewrite.
      const persisted = savedStatus(taskPath);
      const bytes = fs.readFileSync(taskPath);
      expect(persisted.promptSha256).toBe(hash(original));
      expect(persisted.promptBytes).toBe(Buffer.byteLength(original));
      expect(persisted.promptPath).toBe(taskPath);
      expect(persisted.inputSha256).toBe(hash(bytes));
      expect(persisted.inputBytes).toBe(bytes.byteLength);
      expect(persisted.inputSha256).not.toBe(persisted.promptSha256);
    } finally {
      renameSpy?.mockRestore();
      launchSpy.mockRestore();
    }
  });

  it("archives a byte-oversized multibyte prompt and full status unchanged", async () => {
    const { manager } = fixture();
    const prompt = `FAIL_DIRECTIVE\noversize-task-sentinel-π\n${"π".repeat(8_000)}`;
    expect(prompt.length).toBeLessThan(16_000);
    expect(Buffer.byteLength(prompt)).toBeGreaterThan(16_000);
    const handle = await manager.spawn({ name: "oversize control", task: prompt, transport: "process" });
    await expect(manager.wait(handle.id)).resolves.toMatchObject({ status: "failed" });
    const taskPath = (manager.status(handle.id) as AgentRunRecord).promptPath!;
    const failedDirectory = path.dirname(taskPath);

    const files = seedRawTrace(failedDirectory), inode = fs.statSync(failedDirectory).ino;
    removalIntents.splice(0);
    await manager.close();
    expectWholeRun(failedDirectory, files, inode);
    expect(removalIntents).not.toContain(taskPath);
    expect(savedStatus(taskPath)).toMatchObject({ task: prompt, promptSha256: hash(prompt), promptBytes: Buffer.byteLength(prompt), inputSha256: hash(prompt), inputBytes: Buffer.byteLength(prompt) });
    expect(savedStatus(taskPath).promptRetentionDropped).toBeUndefined();
  });

  it.each(["failed", "stopped", "timed_out"] as const)("archives the whole managed %s run before an empty-root sweep", async (terminal) => {
    const { root: managedRoot, tempRoot, manager } = fixture(terminal === "timed_out" ? { timeoutMs: 1_500 } : {});
    const prompt = terminal === "failed" ? "FAIL_DIRECTIVE\nsweep-task-sentinel-π\n" : "HANG sweep-task-sentinel-π\n";
    const handle = await manager.spawn({ name: "sweep control", task: prompt, transport: "process" });
    const result = terminal === "stopped" ? await manager.stop(handle.id) : await manager.wait(handle.id);
    expect(result.status).toBe(terminal);
    const taskPath = (manager.status(handle.id) as AgentRunRecord).promptPath!;
    const directory = path.dirname(taskPath), runRoot = path.dirname(directory);
    expect(path.dirname(runRoot)).toBe(tempRoot);
    expect(runRoot).toBe(managedRoot);
    const files = seedRawTrace(directory), inode = fs.statSync(directory).ino;
    await manager.close();
    expectWholeRun(directory, files, inode);
    expect(savedStatus(taskPath)).toMatchObject({ status: terminal, task: prompt, inputSha256: hash(prompt), inputBytes: Buffer.byteLength(prompt) });
    const options = { tempRoot, orphanedTempRunRetentionMs: 1, oneShotRunRetentionMs: 24 * 60 * 60 * 1_000 };
    expect(sweepTempRunRoots({ ...options, now: result.finishedAt! + options.oneShotRunRetentionMs - 1 }).removedRuns).toEqual([]);
    expect(sweepIntents).not.toContain(directory);
    expect(sweepTempRunRoots({ ...options, now: result.finishedAt! + options.oneShotRunRetentionMs + 1 }).removedRuns).not.toContain(directory);
    expect(sweepIntents).not.toContain(directory);
    expect(fs.readFileSync(retainedPath(taskPath), "utf8")).toBe(prompt);
  });
});
