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
const hash = (value: string | Buffer) => createHash("sha256").update(value).digest("hex");
const rawStatus = (promptPath: string): string => fs.readFileSync(path.join(path.dirname(promptPath), "status.json"), "utf8");
const savedStatus = (promptPath: string): AgentRunRecord => JSON.parse(rawStatus(promptPath)) as AgentRunRecord;
const expectNoRetainedTasks = (record: AgentRunRecord): void => {
  expect(Object.hasOwn(record, "task")).toBe(false);
  for (const nested of record.nestedAgents ?? []) expectNoRetainedTasks(nested);
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
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
  removalIntents.splice(0);
  sweepIntents.splice(0);
  queuedRollbackIntents.splice(0);
  expect(outcomes.filter(outcome => outcome.status === "rejected")).toEqual([]);
});

describe("Agent prompt retention at manager close (#11663)", () => {
  it("retains failed prompt metadata and plans removal only for the successful tree", async () => {
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

    removalIntents.splice(0);
    await manager.close();
    expect(removalIntents).toContain(successDirectory);
    expect(removalIntents).not.toContain(failedDirectory);
    expect(removalIntents).not.toContain(path.join(failedDirectory, "status.json"));
    expect(removalIntents).not.toContain(taskPath);
    expect(savedStatus(taskPath)).toMatchObject({
      status: "failed", promptSha256: hash(prompt), promptBytes: Buffer.byteLength(prompt), promptPath: taskPath,
      inputSha256: hash(prompt), inputBytes: Buffer.byteLength(prompt),
    });
    expectNoRetainedTasks(savedStatus(taskPath));
    expect(rawStatus(taskPath)).not.toContain("private brief with exact bytes");
    expect(rawStatus(taskPath)).not.toContain(JSON.stringify(prompt));
    expect(manager.readLog(failed.id).status?.task).toBe("");
    expect(() => manager.listForUi()).not.toThrow();
  });

  // Stop, AbortSignal cancellation and deadline all use the same managed
  // terminal retention path; the real prompt verifier is never mocked.
  it.each(["stopped", "cancelled", "timed_out"] as const)("retains the verified current prompt after %s", async (outcome) => {
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

    removalIntents.splice(0);
    await manager.close();
    expect(removalIntents).not.toContain(directory);
    expect(removalIntents).not.toContain(taskPath);
    expect(removalIntents).not.toContain(path.join(directory, "status.json"));
    expect(fs.readFileSync(taskPath, "utf8")).toBe(prompt);
    expect(savedStatus(taskPath)).toMatchObject({ status: terminal, promptSha256: hash(prompt), promptBytes: Buffer.byteLength(prompt), inputSha256: hash(prompt), inputBytes: Buffer.byteLength(prompt) });
    expect(savedStatus(taskPath).promptRetentionDropped).toBeUndefined();
    expectNoRetainedTasks(savedStatus(taskPath));
    expect(rawStatus(taskPath)).not.toContain("current brief π");
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
      expect(fs.readFileSync(taskPath, "utf8")).toBe(prompt);
      expect(savedStatus(taskPath)).toMatchObject({ status: "stopped", promptSha256: hash(prompt), inputSha256: hash(prompt), inputBytes: Buffer.byteLength(prompt) });
      expectNoRetainedTasks(savedStatus(taskPath));
      expect(queuedRollbackIntents).toEqual([path.join(queuedDirectory, "queued-result.json"), queuedDirectory]);
      expect(removalIntents).toContain(queuedDirectory);
      expect(fs.existsSync(path.join(queuedDirectory, "task.txt"))).toBe(false);
      // The recorded rollback deliberately leaves the fixture directory present.
      expect(savedStatus(path.join(queuedDirectory, "task.txt"))).toMatchObject({ status: "stopped", queuedArchiveCommitted: true });
    } finally {
      launchSpy.mockRestore();
    }
  });

  it.each(["failed", "stopped", "timed_out", "completed"] as const)("keeps whole-tree removal for an explicit-root %s run", async (terminal) => {
    const { root, manager } = fixture(terminal === "timed_out" ? { timeoutMs: 1_500 } : {}, true);
    const prompt = terminal === "failed" ? "FAIL_DIRECTIVE\nexplicit failure" : terminal === "completed" ? "explicit success" : "HANG explicit terminal";
    const handle = await manager.spawn({ name: "explicit root control", task: prompt, transport: "process" });
    if (terminal === "stopped") await expect(manager.stop(handle.id)).resolves.toMatchObject({ status: terminal });
    else await expect(manager.wait(handle.id)).resolves.toMatchObject({ status: terminal });
    const directory = path.join(root, handle.id);

    removalIntents.splice(0);
    await manager.close();
    expect(removalIntents.filter(target => target === directory || target.startsWith(directory + path.sep))).toEqual([directory]);
    expect(savedStatus(path.join(directory, "task.txt")).promptRetentionDropped).toBeUndefined();
    expect(fs.existsSync(directory)).toBe(true); // Whole-tree intent only, not physical deletion.
  });

  it("omits nested task fields on disk and normalizes them only when read", async () => {
    const { manager } = fixture();
    const prompt = "FAIL_DIRECTIVE\nparent-task-sentinel-π\n";
    const handle = await manager.spawn({ name: "nested privacy control", task: prompt, transport: "process" });
    await expect(manager.wait(handle.id)).resolves.toMatchObject({ status: "failed" });
    const record = manager.status(handle.id) as AgentRunRecord;
    expect(record.task).toBe(prompt); // Live task policy is unchanged.
    const child = path.join(path.dirname(record.promptPath!), "nested", "child");
    const grandchild = path.join(child, "nested", "grandchild");
    fs.mkdirSync(grandchild, { recursive: true, mode: 0o700 });
    for (const [directory, id, task] of [[child, "child", "nested-task-sentinel-λ"], [grandchild, "grandchild", "grandchild-task-sentinel-δ"]] as const) {
      fs.writeFileSync(path.join(directory, "status.json"), JSON.stringify({ ...record, id, name: "nested neutral name", task, text: "preserved nested diagnostic" }), { mode: 0o600 });
    }

    await manager.close();
    const retained = savedStatus(record.promptPath!);
    expect(retained.nestedAgents).toHaveLength(1);
    expect(retained.nestedAgents![0]!.nestedAgents).toHaveLength(1);
    expectNoRetainedTasks(retained);
    for (const sentinel of ["parent-task-sentinel-π", "nested-task-sentinel-λ", "grandchild-task-sentinel-δ"]) expect(rawStatus(record.promptPath!)).not.toContain(sentinel);
    expect(rawStatus(record.promptPath!)).toContain("preserved nested diagnostic");
    const reopened = manager.readLog(handle.id).status!;
    expect(reopened.task).toBe("");
    expect(reopened.nestedAgents![0]!.task).toBe("");
    expect(reopened.nestedAgents![0]!.nestedAgents![0]!.task).toBe("");
    expect(() => manager.listForUi()).not.toThrow();
  });

  it("records task.txt drop on tamper and persists mismatch metadata", async () => {
    const { manager } = fixture();
    const original = "FAIL_DIRECTIVE\noriginal-task-sentinel-π\n";
    const tampered = "tampered-task-sentinel-λ\n";
    const handle = await manager.spawn({ name: "tamper control", task: original, transport: "process" });
    await expect(manager.wait(handle.id)).resolves.toMatchObject({ status: "failed" });
    const taskPath = (manager.status(handle.id) as AgentRunRecord).promptPath!;
    const failedDirectory = path.dirname(taskPath);
    fs.writeFileSync(taskPath, tampered, { mode: 0o600 });

    removalIntents.splice(0);
    await manager.close();
    expect(removalIntents).toContain(taskPath);
    expect(removalIntents).not.toContain(path.join(failedDirectory, "status.json"));
    expect(savedStatus(taskPath)).toMatchObject({ promptRetentionDropped: true, promptRetentionMismatch: true, promptSha256: hash(original), inputSha256: hash(original), inputBytes: Buffer.byteLength(original) });
    expectNoRetainedTasks(savedStatus(taskPath));
    for (const sentinel of ["original-task-sentinel-π", "tampered-task-sentinel-λ", JSON.stringify(original), JSON.stringify(tampered)]) {
      expect(rawStatus(taskPath)).not.toContain(sentinel);
    }
    expect(fs.readFileSync(taskPath, "utf8")).toBe(tampered); // Drop was recorded, not executed.
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
    expectNoRetainedTasks(savedStatus(taskPath));
    expect(rawStatus(taskPath)).not.toContain("original brief π");
    expect(rawStatus(taskPath)).not.toContain("[Fabric continuation]");
    expect(fs.readFileSync(taskPath)).toEqual(bytes);
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

  it("drops a byte-oversized multibyte prompt without leaving fallback task text in status", async () => {
    const { manager } = fixture();
    const prompt = `FAIL_DIRECTIVE\noversize-task-sentinel-π\n${"π".repeat(8_000)}`;
    expect(prompt.length).toBeLessThan(16_000);
    expect(Buffer.byteLength(prompt)).toBeGreaterThan(16_000);
    const handle = await manager.spawn({ name: "oversize control", task: prompt, transport: "process" });
    await expect(manager.wait(handle.id)).resolves.toMatchObject({ status: "failed" });
    const taskPath = (manager.status(handle.id) as AgentRunRecord).promptPath!;
    const failedDirectory = path.dirname(taskPath);

    removalIntents.splice(0);
    await manager.close();
    expect(removalIntents).toContain(taskPath);
    expect(removalIntents).not.toContain(path.join(failedDirectory, "status.json"));
    expect(savedStatus(taskPath)).toMatchObject({ promptRetentionDropped: true, promptSha256: hash(prompt), promptBytes: Buffer.byteLength(prompt), inputSha256: hash(prompt), inputBytes: Buffer.byteLength(prompt) });
    expectNoRetainedTasks(savedStatus(taskPath));
    expect(rawStatus(taskPath)).not.toContain("oversize-task-sentinel-π");
    expect(rawStatus(taskPath)).not.toContain("π".repeat(40));
    expect(rawStatus(taskPath)).not.toContain(JSON.stringify(prompt));
    expect(savedStatus(taskPath).promptRetentionMismatch).toBeUndefined();
    expect(Buffer.byteLength(rawStatus(taskPath))).toBeLessThan(16_000);
  });

  it.each(["failed", "stopped", "timed_out"] as const)("hands a closed managed %s run to the configured one-shot sweep without deleting it", async (terminal) => {
    const { root: managedRoot, tempRoot, manager } = fixture(terminal === "timed_out" ? { timeoutMs: 1_500 } : {});
    const prompt = terminal === "failed" ? "FAIL_DIRECTIVE\nsweep-task-sentinel-π\n" : "HANG sweep-task-sentinel-π\n";
    const handle = await manager.spawn({ name: "sweep control", task: prompt, transport: "process" });
    const result = terminal === "stopped" ? await manager.stop(handle.id) : await manager.wait(handle.id);
    expect(result.status).toBe(terminal);
    const taskPath = (manager.status(handle.id) as AgentRunRecord).promptPath!;
    const directory = path.dirname(taskPath), runRoot = path.dirname(directory);
    expect(path.dirname(runRoot)).toBe(tempRoot);
    expect(runRoot).toBe(managedRoot);
    await manager.close();
    expectNoRetainedTasks(savedStatus(taskPath));
    expect(rawStatus(taskPath)).not.toContain("sweep-task-sentinel-π");
    expect(savedStatus(taskPath)).toMatchObject({ status: terminal, inputSha256: hash(prompt), inputBytes: Buffer.byteLength(prompt) });
    const owner = JSON.parse(fs.readFileSync(path.join(runRoot, ".fabric-owner.json"), "utf8"));
    expect(owner.closedAt).toBeTypeOf("number");
    const options = { tempRoot, orphanedTempRunRetentionMs: 1, oneShotRunRetentionMs: 24 * 60 * 60 * 1_000 };
    expect(sweepTempRunRoots({ ...options, now: result.finishedAt! + options.oneShotRunRetentionMs - 1 }).removedRuns).toEqual([]);
    expect(sweepIntents).not.toContain(directory);
    expect(sweepTempRunRoots({ ...options, now: result.finishedAt! + options.oneShotRunRetentionMs + 1 }).removedRuns).toContain(directory);
    expect(sweepIntents).toContain(directory);
    expect(fs.existsSync(taskPath)).toBe(true); // Recorded intent only; no product deletion executed.
  });
});
