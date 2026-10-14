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

const fixture = (agents: Partial<typeof DEFAULT_FABRIC_CONFIG.agents> = {}) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "agent-prompt-retention-"));
  roots.push(root);
  const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, ...agents, retainRuns: false }, { workerPath, runRoot: root });
  managers.push(manager);
  return { root, manager };
};
const hash = (value: string | Buffer) => createHash("sha256").update(value).digest("hex");
const savedStatus = (promptPath: string): AgentRunRecord =>
  JSON.parse(fs.readFileSync(path.join(path.dirname(promptPath), "status.json"), "utf8")) as AgentRunRecord;

afterEach(async () => {
  // If an assertion fails before the explicit close, prevent test teardown from
  // taking a destructive retention branch (removeTree itself remains mocked).
  await Promise.all(managers.splice(0).map(async manager => {
    manager.config.retainRuns = true;
    await manager.close();
  }));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
  removalIntents.splice(0);
  sweepIntents.splice(0);
  vi.unstubAllEnvs();
});

describe("Agent prompt retention at manager close (#11663)", () => {
  it("retains failed prompt metadata and plans removal only for the successful tree", async () => {
    const { root, manager } = fixture();
    const prompt = "FAIL_DIRECTIVE\nprivate brief with exact bytes: π\n";
    const failed = await manager.spawn({ task: prompt, transport: "process" });
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
  });

  // Counterexample: retention is for failed runs ONLY. A stopped or timed-out
  // run must keep today's whole-tree removal when retainRuns is false, while a
  // failed run in the same manager is still retained. removeTree is mocked, so
  // this inspects the recorded removal plan without executing product deletion.
  it.each(["stopped", "timed_out"] as const)("plans whole-tree removal for a %s run while retaining a failed run", async (terminal) => {
    const { manager } = fixture(terminal === "timed_out" ? { timeoutMs: 1_500 } : {});
    const prompt = "FAIL_DIRECTIVE\ncontrol failed brief\n";
    const failed = await manager.spawn({ task: prompt, transport: "process" });
    await expect(manager.wait(failed.id)).resolves.toMatchObject({ status: "failed" });
    const failedTask = (manager.status(failed.id) as AgentRunRecord).promptPath!;
    const failedDirectory = path.dirname(failedTask);

    const handle = await manager.spawn({ task: `HANG until ${terminal}`, transport: "process" });
    if (terminal === "stopped") await expect(manager.stop(handle.id)).resolves.toMatchObject({ status: "stopped" });
    else await expect(manager.wait(handle.id)).resolves.toMatchObject({ status: "timed_out" });
    const directory = manager.runDirectory(handle.id)!;
    expect(fs.existsSync(path.join(directory, "status.json"))).toBe(true);

    removalIntents.splice(0);
    await manager.close();
    // Exactly one whole-tree intent for the non-failed run; no per-entry
    // retention plan (which would keep status.json/task.txt) for it.
    expect(removalIntents.filter(target => target === directory || target.startsWith(directory + path.sep))).toEqual([directory]);
    expect(savedStatus(path.join(directory, "task.txt")).promptRetentionDropped).toBeUndefined();
    // The failed control is still retained beside its status record.
    expect(removalIntents).not.toContain(failedDirectory);
    expect(removalIntents).not.toContain(failedTask);
    expect(removalIntents).not.toContain(path.join(failedDirectory, "status.json"));
    expect(fs.readFileSync(failedTask, "utf8")).toBe(prompt);
    expect(savedStatus(failedTask)).toMatchObject({ status: "failed", promptSha256: hash(prompt), promptBytes: Buffer.byteLength(prompt) });
  });

  // Close with one running worker and one queued admission: the queued receipt
  // settles stopped without launching, the real primary exits (checked ESRCH),
  // and only the active run's whole tree gets a (mocked) removal intent.
  it("closes a running worker with queued admission, removing only the active tree", async () => {
    const { manager } = fixture({ maxConcurrent: 1 });
    const prompt = "FAIL_DIRECTIVE\nqueued-close failed control\n";
    const failed = await manager.spawn({ task: prompt, transport: "process" });
    await expect(manager.wait(failed.id)).resolves.toMatchObject({ status: "failed" });
    const failedTask = (manager.status(failed.id) as AgentRunRecord).promptPath!;
    const failedDirectory = path.dirname(failedTask);

    const active = await manager.spawn({ task: "HANG_WITH_PROGRESS", transport: "process" });
    await vi.waitFor(() => expect(manager.status(active.id)).toMatchObject({ status: "running", turns: 3 }), { timeout: 5_000 });
    const pid = Number((manager.status(active.id) as AgentRunRecord).sessionId);
    expect(Number.isSafeInteger(pid) && pid > 0 && processAlive(pid)).toBe(true);
    const directory = manager.runDirectory(active.id)!;
    const launchSpy = vi.spyOn(ProcessTransport.prototype, "launch"); // observer only: passes through
    try {
      const queued = await manager.spawn({ task: "queued successful task", transport: "process" });
      expect(manager.status(queued.id)).toMatchObject({ status: "queued" });
      const queuedWait = manager.wait(queued.id);

      removalIntents.splice(0);
      await manager.close();
      await expect(queuedWait).resolves.toMatchObject({ status: "stopped" });
      expect(launchSpy).not.toHaveBeenCalled();
      expect(processAlive(pid), "checked ESRCH for the captured primary").toBe(false);
      expect(removalIntents.filter(target => target === directory || target.startsWith(directory + path.sep))).toEqual([directory]);
      expect(removalIntents.filter(target => target === failedDirectory || target.startsWith(failedDirectory + path.sep))).toEqual([]);
      expect(fs.readFileSync(failedTask, "utf8")).toBe(prompt);
      expect(savedStatus(failedTask)).toMatchObject({ status: "failed", promptSha256: hash(prompt) });
    } finally {
      launchSpy.mockRestore();
    }
  });

  it("records task.txt drop on tamper and persists mismatch metadata", async () => {
    const { manager } = fixture();
    const handle = await manager.spawn({ task: "FAIL_DIRECTIVE\noriginal bytes\n", transport: "process" });
    await expect(manager.wait(handle.id)).resolves.toMatchObject({ status: "failed" });
    const taskPath = (manager.status(handle.id) as AgentRunRecord).promptPath!;
    const failedDirectory = path.dirname(taskPath);
    fs.writeFileSync(taskPath, "tampered bytes\n", { mode: 0o600 });

    removalIntents.splice(0);
    await manager.close();
    expect(removalIntents).toContain(taskPath);
    expect(removalIntents).not.toContain(path.join(failedDirectory, "status.json"));
    expect(savedStatus(taskPath)).toMatchObject({ promptRetentionDropped: true, promptRetentionMismatch: true });
    expect(savedStatus(taskPath).inputSha256).toBeDefined();
  });

  it("retains the current failed continuation bytes while preserving the original prompt digest", async () => {
    const { manager } = fixture();
    const original = "RESUME_AFTER_STOP RESUME_FAIL_AFTER_STOP\noriginal brief π\n";
    const handle = await manager.spawn({ task: original, transport: "process" });
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
      const handle = await manager.spawn({ task: original, transport: "process" });
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

  it("drops an oversized failed prompt but keeps its bounded status record", async () => {
    const { manager } = fixture();
    const prompt = `FAIL_DIRECTIVE\n${"x".repeat(16_100)}`;
    const handle = await manager.spawn({ task: prompt, transport: "process" });
    await expect(manager.wait(handle.id)).resolves.toMatchObject({ status: "failed" });
    const taskPath = (manager.status(handle.id) as AgentRunRecord).promptPath!;
    const failedDirectory = path.dirname(taskPath);

    removalIntents.splice(0);
    await manager.close();
    expect(removalIntents).toContain(taskPath);
    expect(removalIntents).not.toContain(path.join(failedDirectory, "status.json"));
    expect(savedStatus(taskPath)).toMatchObject({ promptRetentionDropped: true });
    expect(savedStatus(taskPath).task.length).toBeLessThanOrEqual(16_001);
    expect(savedStatus(taskPath).promptRetentionMismatch).toBeUndefined();
  });

  it("hands a closed managed root to the configured one-shot sweep without deleting it", async () => {
    const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "agent-prompt-sweep-")); roots.push(tempRoot);
    fs.chmodSync(tempRoot, 0o700);
    vi.stubEnv("PI_FABRIC_TMPDIR", tempRoot);
    vi.stubEnv("PI_FABRIC_RUN_ROOT", undefined);
    const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: false }, { workerPath });
    managers.push(manager);
    const handle = await manager.spawn({ task: "FAIL_DIRECTIVE\nsweep input\n", transport: "process" });
    const result = await manager.wait(handle.id);
    const taskPath = (manager.status(handle.id) as AgentRunRecord).promptPath!;
    const directory = path.dirname(taskPath), runRoot = path.dirname(directory);
    await manager.close();
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
