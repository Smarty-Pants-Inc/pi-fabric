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

const fixture = (agents: Partial<typeof DEFAULT_FABRIC_CONFIG.agents> = {}, explicitRoot = false, onResultConsumed?: (id: string) => void) => {
  const tempRoot = fs.mkdtempSync(path.join(nativeTempRoot, "agent-prompt-retention-"));
  roots.push(tempRoot);
  // Keep the real managed ownership/sweep path inside this fixture on every host.
  vi.spyOn(os, "tmpdir").mockReturnValue(tempRoot);
  vi.stubEnv("PI_FABRIC_TMPDIR", undefined);
  vi.stubEnv("PI_FABRIC_RUN_ROOT", undefined);
  vi.stubEnv("PI_FABRIC_DEPTH", "0");
  // These manager custody tests exercise the legacy process exit fence, not
  // delegated cgroup admission; keep the fixture independent of the host bus.
  vi.stubEnv("DBUS_SESSION_BUS_ADDRESS", undefined);
  vi.stubEnv("XDG_RUNTIME_DIR", undefined);
  for (const key of ["PI_FABRIC_BUDGET", "PI_FABRIC_BUDGET_FILE", "PI_FABRIC_BUDGET_ID"]) vi.stubEnv(key, undefined);
  const runRoot = explicitRoot ? path.join(tempRoot, "explicit-runs") : undefined;
  const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, sessionExport: false, ...agents, retainRuns: false }, {
    workerPath, ...(runRoot ? { runRoot } : {}), ...(onResultConsumed ? { onResultConsumed } : {}),
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
  expect(row).toMatchObject({ source, destination: move.destination, id: path.basename(source), reason: expect.any(String), time: expect.any(Number), state: "moved" });
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

type TerminalControl = "stopped" | "cancelled" | "timed_out";
type RawEntry = { kind: "directory" | "file"; dev: bigint; ino: bigint; mode: number; bytes?: Buffer };
type RawTree = Map<string, RawEntry>;

// Full inventory, including empty directories and every opaque/raw artifact.
// Native identity is asserted here, not inferred from an exported evidence COPY.
const snapshotRawTree = (directory: string): RawTree => {
  expect(fs.realpathSync(directory)).toBe(path.resolve(directory));
  const entries: RawTree = new Map();
  const walk = (relative: string): void => {
    const file = path.join(directory, relative), stat = fs.lstatSync(file, { bigint: true });
    expect(stat.isSymbolicLink(), relative).toBe(false);
    expect(stat.isDirectory() || (stat.isFile() && stat.nlink === 1n), relative).toBe(true);
    entries.set(relative, { kind: stat.isDirectory() ? "directory" : "file", dev: stat.dev, ino: stat.ino,
      mode: Number(stat.mode & 0o777n), ...(stat.isFile() ? { bytes: fs.readFileSync(file) } : {}) });
    if (stat.isDirectory()) for (const name of fs.readdirSync(file).sort()) walk(path.join(relative, name));
  };
  walk("");
  return entries;
};
// close re-joins explicit/deadline stop intent and may atomically republish
// byte-identical status before custody. Bind that actual publication's stage
// inode to its destination; never exempt status bytes or identity at the move.
const observeStatusPublication = (directory: string, raw: RawTree, rename: typeof fs.renameSync) => {
  const observations: Array<{ previous: RawEntry; stage: fs.BigIntStats; bytes: Buffer; published: fs.BigIntStats }> = [];
  const errors: unknown[] = [];
  const observedRename: typeof fs.renameSync = (source, destination) => {
    if (String(destination) !== path.join(directory, "status.json")) return rename(source, destination);
    try {
      const previous = raw.get("status.json")!, stage = fs.lstatSync(source, { bigint: true }), bytes = fs.readFileSync(source);
      rename(source, destination);
      const published = fs.lstatSync(destination, { bigint: true });
      observations.push({ previous, stage, bytes, published });
      raw.set("status.json", { ...previous, dev: stage.dev, ino: stage.ino });
    } catch (error) { errors.push(error); throw error; }
  };
  return { rename: observedRename, verify: () => {
    // Assert OUTSIDE product close's allSettled/catch paths: an observer failure
    // must never disappear into a swallowed shutdown error.
    expect(errors).toEqual([]);
    for (const { previous, stage, bytes, published } of observations) {
      expect(stage.isFile() && !stage.isSymbolicLink() && stage.nlink === 1n).toBe(true);
      expect(bytes).toEqual(previous.bytes);
      expect(Number(stage.mode & 0o777n)).toBe(previous.mode);
      expect(stage.dev).toBe(previous.dev);
      expect({ dev: published.dev, ino: published.ino }).toEqual({ dev: stage.dev, ino: stage.ino });
    }
  } };
};
const expectRawTree = (directory: string, expected: RawTree): void => {
  const actual = snapshotRawTree(directory);
  expect([...actual.keys()]).toEqual([...expected.keys()]);
  for (const [relative, entry] of expected) expect(actual.get(relative), relative).toEqual(entry);
};
const expectArchivedRaw = (source: string, raw: RawTree): string => {
  const held = heldRoot(source);
  expectRawTree(held, raw);
  expect(path.relative(source, held).startsWith(`..${path.sep}`)).toBe(true);
  // Independently walk real ancestors, rather than trusting a mocked archive path.
  for (let current = held; ; current = path.dirname(current)) {
    expect(fs.existsSync(path.join(current, ".git")), current).toBe(false);
    if (path.dirname(current) === current) break;
  }
  return held;
};
const expectNoRemoval = (directory: string): void => {
  const within = (target: string) => target === directory || target.startsWith(directory + path.sep);
  expect(removalIntents.filter(within)).toEqual([]);
  expect(sweepIntents.filter(within)).toEqual([]);
};
const expectTerminalPrompt = (manager: AgentManager, id: string, directory: string, prompt: string, terminal: string): void => {
  const taskPath = path.join(directory, "task.txt");
  expect(fs.readFileSync(retainedPath(taskPath), "utf8")).toBe(prompt);
  expect(savedStatus(taskPath)).toMatchObject({ id, status: terminal, task: prompt,
    promptSha256: hash(prompt), promptBytes: Buffer.byteLength(prompt), inputSha256: hash(prompt), inputBytes: Buffer.byteLength(prompt) });
  expect(savedStatus(taskPath).promptRetentionDropped).toBeUndefined();
  expect(JSON.parse(fs.readFileSync(retainedPath(path.join(directory, "manager-result.json")), "utf8"))).toMatchObject({ id, status: terminal, task: prompt });
  expect(manager.status(id)).toMatchObject({ id, status: terminal });
  expect(manager.listForUi().some(record => record.id === id)).toBe(true);
};
const terminalFixture = async (outcome: TerminalControl) => {
  const consumed = vi.fn();
  const { manager } = fixture(outcome === "timed_out" ? { timeoutMs: 1_500 } : {}, false, consumed);
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
  seedRawTrace(directory);
  // Keep a lifecycle byte control even if this worker emitted no lifecycle rows.
  if (!fs.existsSync(path.join(directory, "lifecycle.jsonl"))) fs.writeFileSync(path.join(directory, "lifecycle.jsonl"), '{"rawLifecycleControl":true}\n', { mode: 0o600 });
  for (const name of ["task.txt", "status.json", "manager-result.json", "events.jsonl", "lifecycle.jsonl", "session.NdJsOn.backup"]) {
    expect(fs.lstatSync(path.join(directory, name)).isFile(), name).toBe(true);
  }
  consumed.mockClear(); // Observation above is not cleanup consumption.
  return { manager, handle, directory, prompt, terminal, consumed };
};

const openDescriptorCustody = async (outcome: TerminalControl, injected: boolean): Promise<void> => {
  const { manager, handle, directory, prompt, terminal, consumed } = await terminalFixture(outcome);
  const traceName = "session.NdJsOn.backup", trace = path.join(directory, traceName);
  const raw = snapshotRawTree(directory), descriptor = fs.openSync(trace, "a");
  const pin = fs.fstatSync(descriptor, { bigint: true });
  expect({ dev: pin.dev, ino: pin.ino }).toEqual({ dev: raw.get(traceName)!.dev, ino: raw.get(traceName)!.ino });
  const statusPublication = observeStatusPublication(directory, raw, fs.renameSync.bind(fs));
  const rename = statusPublication.rename;
  let blocked = injected, fdOpen = true;
  const attempts: Array<{ destination: string; error?: unknown }> = [];
  // Native case is a pass-through observer. Fault injection changes ONLY the
  // exact admitted source rename, never preparation, result auth, or retry.
  const renameSpy = vi.spyOn(fs, "renameSync").mockImplementation((source, destination) => {
    if (String(source) !== directory) return rename(source, destination);
    const attempt: { destination: string; error?: unknown } = { destination: String(destination) };
    attempts.push(attempt);
    try {
      if (blocked) throw Object.assign(new Error("injected open-FD custody refusal"), { code: "EPERM" });
      rename(source, destination);
    } catch (error) { attempt.error = error; throw error; }
  });
  try {
    removalIntents.splice(0);
    await expect(manager.close()).resolves.toBeUndefined(); // NOT a cleaned receipt.
    statusPublication.verify();
    expect(attempts).toHaveLength(1);
    const attempt = attempts[0]!;
    const tail = Buffer.from("late FD tail");
    fs.writeSync(descriptor, tail);
    expect({ dev: fs.fstatSync(descriptor, { bigint: true }).dev, ino: fs.fstatSync(descriptor, { bigint: true }).ino }).toEqual({ dev: pin.dev, ino: pin.ino });
    raw.get(traceName)!.bytes = Buffer.concat([raw.get(traceName)!.bytes!, tail]);
    if (attempt.error === undefined) {
      expect(injected).toBe(false);
      const held = expectArchivedRaw(directory, raw);
      expect(path.join(held, traceName)).toBe(fs.realpathSync(path.join(held, traceName)));
      expect(fs.readFileSync(path.join(held, traceName))).toEqual(raw.get(traceName)!.bytes);
      expectTerminalPrompt(manager, handle.id, directory, prompt, terminal);
    } else {
      // No OS assumption: only an actual observed EPERM can enter this branch.
      expect(attempt.error).toMatchObject({ code: "EPERM" });
      expect(custodyMoves.filter(move => move.source === directory)).toEqual([]);
      const container = path.dirname(attempt.destination);
      const prepared = JSON.parse(fs.readFileSync(path.join(container, "custody.prepared.json"), "utf8"));
      const failed = JSON.parse(fs.readFileSync(path.join(container, "custody.json"), "utf8"));
      expect(prepared).toMatchObject({ format: 1, id: handle.id, source: directory, destination: attempt.destination, state: "prepared", reason: expect.any(String), time: expect.any(Number) });
      expect(failed).toMatchObject({ ...prepared, time: expect.any(Number), state: "rename-failed", error: expect.stringContaining(injected ? "injected open-FD custody refusal" : "EPERM") });
      expect(fs.existsSync(attempt.destination)).toBe(false); // No copied shadow.
      expectRawTree(directory, raw); // Full original + live-FD tail, not a shadow.
      expectTerminalPrompt(manager, handle.id, directory, prompt, terminal);
      expect(manager.readLog(handle.id).status).toMatchObject({ id: handle.id, status: terminal, task: prompt });
      expect(consumed).not.toHaveBeenCalled();
      expectNoRemoval(directory);
      exportRawTree(directory, `failed-${outcome}`);
      exportRawTree(container, `refusal-receipts-${outcome}`);

      // close() is cached. The explicitly authorized retry is public cleanup,
      // and it must archive the SAME original after the fixture FD is released.
      fs.closeSync(descriptor); fdOpen = false; blocked = false;
      const beforeConsume = vi.fn(() => {
        expectArchivedRaw(directory, raw);
        expectTerminalPrompt(manager, handle.id, directory, prompt, terminal);
      });
      await expect(manager.cleanup(handle.id, false, beforeConsume)).resolves.toEqual({ cleaned: true });
      statusPublication.verify();
      expect(attempts).toHaveLength(2);
      expect(attempts[1]!.error).toBeUndefined();
      expect(beforeConsume).toHaveBeenCalledTimes(1);
      expectArchivedRaw(directory, raw);
      // Earlier refusal preparation remains immutable after retry succeeds.
      expect(JSON.parse(fs.readFileSync(path.join(container, "custody.prepared.json"), "utf8"))).toEqual(prepared);
      expect(JSON.parse(fs.readFileSync(path.join(container, "custody.json"), "utf8"))).toEqual(failed);
      expect(consumed).toHaveBeenCalledTimes(1);
    }
    expectNoRemoval(directory);
    exportRawTree(path.dirname(retainedPath(path.join(directory, "task.txt"))), `complete-${outcome}`);
  } finally {
    if (fdOpen) fs.closeSync(descriptor);
    renameSpy.mockRestore();
  }
};

/** Opt-in owned-fixture COPY before any command/global teardown, never inode custody. */
const exportRawTree = (source: string, label: string): void => {
  const evidence = process.env.PI_TEST_CUSTODY_EVIDENCE_DIR;
  if (!evidence) return;
  const relative = path.relative(nativeTempRoot, evidence);
  if (relative === "" || relative === ".." || relative.startsWith(`..${path.sep}`) || path.isAbsolute(relative)) throw new Error("Prompt evidence must stage inside inherited owned TMPDIR");
  fs.mkdirSync(evidence, { recursive: true });
  const inventory = (base: string): Array<{ path: string; kind: string; sha256?: string }> => {
    let bytes = 0;
    const rows: Array<{ path: string; kind: string; sha256?: string }> = [];
    const walk = (relative: string): void => {
      const file = path.join(base, relative), stat = fs.lstatSync(file);
      if (rows.length >= 5_000 || stat.isSymbolicLink() || (!stat.isDirectory() && (!stat.isFile() || stat.nlink !== 1))) throw new Error(`Unsafe prompt fixture export: ${file}`);
      if (stat.isDirectory()) { rows.push({ path: relative, kind: "directory" }); for (const name of fs.readdirSync(file).sort()) walk(path.join(relative, name)); }
      else { if ((bytes += stat.size) > 96 * 1024 * 1024) throw new Error("Prompt fixture export bound exceeded"); rows.push({ path: relative, kind: "file", sha256: hash(fs.readFileSync(file)) }); }
    };
    walk(""); return rows;
  };
  const before = inventory(source), destination = fs.mkdtempSync(path.join(evidence, `${label}-`));
  const identities = before.map(row => {
    const stat = fs.lstatSync(path.join(source, row.path), { bigint: true });
    return { path: row.path, dev: String(stat.dev), ino: String(stat.ino), mode: Number(stat.mode & 0o777n) };
  });
  fs.cpSync(source, path.join(destination, "tree"), { recursive: true, dereference: false });
  expect(inventory(source)).toEqual(before);
  expect(inventory(path.join(destination, "tree"))).toEqual(before);
  fs.writeFileSync(path.join(destination, "export.json"), JSON.stringify({ source, destination: path.join(destination, "tree"), test: expect.getState().currentTestName, platform: process.platform, method: "pre-teardown copy; SHA256 independent readback; no inode claim", entries: before, sourceNativeIdentities: identities }));
};
const exportOwnedFixtures = (): void => {
  for (const source of roots) exportRawTree(source, "prompt");
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

  // Separate unconditional whole-root success from the native open-handle
  // boundary. A native refusal is retained evidence, NEVER a successful move.
  it.each(["stopped", "cancelled", "timed_out"] as const)("archives the whole verified current run after %s with fixture descriptors closed", async outcome => {
    const { manager, handle, directory, prompt, terminal } = await terminalFixture(outcome);
    const trace = path.join(directory, "session.NdJsOn.backup");
    const fd = fs.openSync(trace, "a");
    try { fs.writeSync(fd, "closed FD control tail"); } finally { fs.closeSync(fd); }
    const raw = snapshotRawTree(directory);
    const statusPublication = observeStatusPublication(directory, raw, fs.renameSync.bind(fs));
    const renameSpy = vi.spyOn(fs, "renameSync").mockImplementation(statusPublication.rename);
    removalIntents.splice(0);
    try { await expect(manager.close()).resolves.toBeUndefined(); } finally { renameSpy.mockRestore(); }
    statusPublication.verify();
    expectArchivedRaw(directory, raw);
    expectTerminalPrompt(manager, handle.id, directory, prompt, terminal);
    expectNoRemoval(directory);
  });

  it.each(["stopped", "cancelled", "timed_out"] as const)("preserves native open-FD custody after %s without copy or deletion", async outcome => {
    await openDescriptorCustody(outcome, false);
  });

  // Labeled nonnative fault injection: the real helper still prepares receipts,
  // refuses, and explicitly retries through public manager.cleanup on every OS.
  it.each(["stopped", "cancelled", "timed_out"] as const)("retains injected EPERM evidence after %s until explicit closed-FD cleanup retry", async outcome => {
    await openDescriptorCustody(outcome, true);
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
      fs.mkdirSync(queuedDirectory, { recursive: true, mode: 0o700 });
      const queuedTraces = new Map<string, Buffer>([
        ["events.jsonl", Buffer.from('{"queued":"events-π"}\n')],
        ["session.jsonl", Buffer.from('{"queued":"session-λ"}\n')],
        ["steer.jsonl", Buffer.from('{"queued":"steer-δ"}\n')],
      ]);
      for (const [name, bytes] of queuedTraces) fs.writeFileSync(path.join(queuedDirectory, name), bytes);
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
      expect(queuedRollbackIntents).toEqual([]); // Route settlement must move the whole tree, never unlink its traces.
      expect(removalIntents).not.toContain(queuedDirectory);
      const heldQueued = heldRoot(queuedDirectory);
      expect(fs.existsSync(path.join(heldQueued, "task.txt"))).toBe(false);
      expect(savedStatus(path.join(heldQueued, "task.txt"))).toMatchObject({ status: "stopped", queuedArchiveCommitted: true });
      for (const [name, bytes] of queuedTraces) expect(fs.readFileSync(path.join(heldQueued, name))).toEqual(bytes);
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
