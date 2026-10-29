import childProcess from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { CompletionJournal, type CompletionRecipient } from "../src/agents/completion-journal.js";
import type { AgentRunResult } from "../src/agents/types.js";
import { MeshStore } from "../src/mesh/store.js";
import { writeHostLease } from "../src/topology/host-leases.js";
import { fabricDataRoot } from "../src/storage/temp-root.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { scriptSpawnArgs } from "../src/agents/transports/process-utils.js";
import { FABRIC_RUN_ROOT_PREFIX, RUN_ROOT_SWEEP_MARKER, UNRESOLVED_WORKER_FILE, sweepTempRunRoots, type TempRunSweepRequest } from "../src/storage/retention.js";
import { ageRunRoot } from "./helpers/age-run-root.js";

// The real .ts entry can select Bun and perform native deletion: hosted CI only (vitest.config.ts).
const launches = vi.hoisted(() => [] as Array<{ args: readonly string[]; env: NodeJS.ProcessEnv | undefined; status: number | null; stderr: string }>);
vi.mock("node:child_process", async importOriginal => {
  const actual = await importOriginal<typeof import("node:child_process")>();
  return { ...actual, spawn: (runtime: string, args: readonly string[], options: Parameters<typeof actual.spawn>[2]) => {
    if (!options?.detached) return actual.spawn(runtime, args, options);
    // Exercise the actual detached entry, but join it in the test: no leaked background sweeps.
    const result = actual.spawnSync(runtime, args, { env: options.env, encoding: "utf8", timeout: 20_000 });
    launches.push({ args, env: options.env, status: result.status, stderr: result.stderr });
    return new actual.ChildProcess();
  } };
});
const roots: string[] = [];
const managers: AgentManager[] = [];
const HOUR = 3600000;
const entry = path.resolve("src/storage/sweep-main.ts");
const temp = () => { const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-temp-sweep-")); roots.push(root); return root; };
afterEach(async () => {
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  launches.splice(0); vi.restoreAllMocks(); vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const seed = (tempRoot: string, finishedAt: number) => {
  const root = path.join(tempRoot, FABRIC_RUN_ROOT_PREFIX + "configured");
  const run = path.join(root, "run"); fs.mkdirSync(run, { recursive: true });
  fs.writeFileSync(path.join(root, ".fabric-owner.json"), JSON.stringify({ pid: 2147483647, startedAt: finishedAt, heartbeatAt: finishedAt, closedAt: finishedAt, childrenStopped: true }));
  fs.writeFileSync(path.join(run, "status.json"), JSON.stringify({ status: "completed", transport: "process", sessionId: "2147483647", finishedAt }));
  const log = Array.from({ length: 401 }, (_, sequence) => JSON.stringify({ sequence, text: "x".repeat(80) }) + "\n").join("");
  fs.writeFileSync(path.join(run, "events.jsonl"), log);
  fs.utimesSync(run, finishedAt / 1000, finishedAt / 1000);
  return { root, run, log };
};
const snapshot = (run: string) => ({ mtime: fs.statSync(run).mtimeMs, files: fs.readdirSync(run).sort().map(name => {
  const file = path.join(run, name); const stat = fs.statSync(file);
  return [name, stat.ino, stat.mtimeMs, stat.ctimeMs, fs.readFileSync(file, "utf8")];
}) });

const DEAD_PID = 2147483647;
const abandonedRoot = (tempRoot: string, name: string, owner: Record<string, unknown> = {}) => {
  const root = fs.mkdtempSync(path.join(tempRoot, FABRIC_RUN_ROOT_PREFIX + name + "-"));
  const startedAt = Date.now() - 48 * HOUR;
  fs.writeFileSync(path.join(root, ".fabric-owner.json"), JSON.stringify({ pid: DEAD_PID, startedAt, heartbeatAt: startedAt, ...owner }));
  return root;
};
const unconfirmedTask = (root: string, id = "a".repeat(32), pid = DEAD_PID) => {
  const run = path.join(root, id); fs.mkdirSync(run, { recursive: true, mode: 0o700 });
  fs.writeFileSync(path.join(run, "task.txt"), "session B's retained task");
  fs.writeFileSync(path.join(run, "status.json"), JSON.stringify({ id, status: "running", runner: "pi", transport: "process", sessionId: String(pid), startedAt: 1 }));
  fs.writeFileSync(path.join(run, UNRESOLVED_WORKER_FILE), JSON.stringify({ reason: "Owned scope observation failed: Error: ENODEV: no such device, read", markedAt: 1, runId: id }));
  fs.writeFileSync(path.join(run, "settlement.json"), JSON.stringify({ version: 1, runId: id, outcome: "completed", text: "PRIVATE B ANSWER" }));
  return run;
};
// Fixture-only claim aging: each close still passes the real manager's interval gate.
const ageSweepClaim = (tempRoot: string) => fs.writeFileSync(path.join(tempRoot, RUN_ROOT_SWEEP_MARKER), JSON.stringify({ sweptAt: Date.now() - HOUR }));

describe("configured detached temp sweep", () => {
  it.runIf(process.platform === "linux")("reaches a shared sibling's ENODEV task through manager close, settles lost once after grace, and never delivers its answer", async () => {
    const base = temp();
    const privateA = path.join(base, "session-a"), privateB = path.join(base, "session-b"), shared = path.join(base, "file-data");
    for (const root of [privateA, privateB, shared]) fs.mkdirSync(root, { mode: 0o700 });
    const tmp = vi.spyOn(os, "tmpdir").mockReturnValue(privateB);
    vi.stubEnv("TMPDIR", privateB);
    vi.stubEnv("PI_FABRIC_TMPDIR", undefined); vi.stubEnv("PI_FABRIC_RUN_ROOT", undefined); vi.stubEnv("PI_FABRIC_DEPTH", "0");
    // Old placement negative control: A cannot discover B's private parent. No legacy root is moved.
    const privateRoot = abandonedRoot(fabricDataRoot(), "session-b");
    const privateRun = unconfirmedTask(privateRoot);
    const privateBefore = snapshot(privateRun), privateOwner = fs.readFileSync(path.join(privateRoot, ".fabric-owner.json"), "utf8");
    const grace = 6 * HOUR;
    const sweepFromA = async (override?: string) => {
      tmp.mockReturnValue(privateA); vi.stubEnv("TMPDIR", privateA); vi.stubEnv("PI_FABRIC_TMPDIR", override);
      const count = launches.length;
      const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 }, {
        sweepPath: entry, retention: { ...DEFAULT_FABRIC_CONFIG.retention, orphanedTempRunMs: grace },
      }); managers.push(manager);
      await manager.close();
      expect(launches).toHaveLength(count + 1);
      const launch = launches.at(-1)!;
      expect(launch).toMatchObject({ status: 0, env: { TMPDIR: privateA } });
      const stderrLines = launch.stderr.trim().split(/\r?\n/).filter(Boolean);
      expect(stderrLines.every(line => /^\[pi-fabric\] cleanup retained whole root: .+ -> .+; receipt: .+\/custody\.json$/.test(line))).toBe(true);
      expect(launch.stderr).toBe(stderrLines.length ? `${stderrLines.join("\n")}\n` : "");
      expect(launch.env?.PI_FABRIC_TMPDIR).toBe(override);
      const request = JSON.parse(launch.args[1]!) as TempRunSweepRequest;
      expect(request).toMatchObject({ tempRoot: override ?? privateA, orphanedTempRunRetentionMs: grace });
      expect(path.dirname(request.currentRoot!)).toBe(override ?? privateA);
    };
    await sweepFromA();
    expect(snapshot(privateRun)).toEqual(privateBefore);
    expect(fs.readFileSync(path.join(privateRoot, ".fabric-owner.json"), "utf8")).toBe(privateOwner);

    // New launches share only file data, not TMPDIR. This is the existing public override, not a selector test.
    tmp.mockReturnValue(privateB); vi.stubEnv("TMPDIR", privateB); vi.stubEnv("PI_FABRIC_TMPDIR", shared);
    const sibling = abandonedRoot(fabricDataRoot(), "session-b"), run = unconfirmedTask(sibling);
    const meshRoot = path.join(base, "mesh");
    const recipient: CompletionRecipient = { rootId: "session:session-a", sessionId: "session-a", cwd: base, projectRoot: base, name: "main", startedAt: 1 };
    const now = Date.now();
    writeHostLease(meshRoot, { id: recipient.rootId, rootId: recipient.rootId, identityId: recipient.rootId, updatedAt: now,
      expiresAt: now + HOUR, startedAt: now, session: { id: recipient.sessionId, startedAt: now, updatedAt: now, expiresAt: now + HOUR } });
    // An untrusted recipient manifest points B's saved answer at live A; offline recovery must not use it.
    fs.writeFileSync(path.join(run, "completion-recipient.json"), JSON.stringify({ meshRoot, recipient, supervisor: { pid: DEAD_PID } }));
    const mesh = new MeshStore(meshRoot, 65536, 1000), delivered: AgentRunResult[] = [];
    const completions = new CompletionJournal(meshRoot, recipient, {} as never, mesh, (result, done) => { delivered.push(result); done(); });
    const noDelivery = async () => {
      await completions.drain();
      expect(delivered).toEqual([]);
      const journal = path.join(meshRoot, "agent-completions");
      expect(fs.existsSync(journal) ? fs.readdirSync(journal, { recursive: true }).map(String).filter(name => name.endsWith(".json")) : []).toEqual([]);
    };
    const hold = path.join(run, "keep-for-observation.txt"); fs.writeFileSync(hold, "unknown data vetoes collection");
    const before = snapshot(run), ownerFile = path.join(sibling, ".fabric-owner.json");
    expect(JSON.parse(fs.readFileSync(ownerFile, "utf8"))).not.toHaveProperty("closedAt");
    expect(JSON.parse(fs.readFileSync(ownerFile, "utf8"))).not.toHaveProperty("orphanedAt");
    await sweepFromA(shared);
    const owner = JSON.parse(fs.readFileSync(ownerFile, "utf8"));
    expect(owner.closedAt).toBeUndefined();
    expect(owner.orphanedAt).toBeUndefined(); // Maintenance never forges closure or refreshes a grace marker.
    expect(snapshot(run)).toEqual(before);
    await noDelivery();

    ageSweepClaim(shared); await sweepFromA(shared);
    expect(JSON.parse(fs.readFileSync(ownerFile, "utf8")).orphanedAt).toBe(owner.orphanedAt);
    expect(snapshot(run)).toEqual(before); // Fresh stamp, still inside grace: no recovery or marker clear.
    expect(JSON.parse(fs.readFileSync(path.join(run, "status.json"), "utf8")).status).toBe("running");
    expect(fs.existsSync(path.join(run, UNRESOLVED_WORKER_FILE))).toBe(true);
    await noDelivery();

    // The real detached entry owns Date.now(). Age all synthetic filesystem
    // activity too; a stale heartbeat alone no longer proves an idle tree.
    ageRunRoot(sibling, Date.now() - grace - HOUR);
    ageSweepClaim(shared); await sweepFromA(shared);
    const settled = JSON.parse(fs.readFileSync(path.join(run, "status.json"), "utf8"));
    expect(settled).toMatchObject({ status: "completed", settledLost: true });
    expect(settled.text).toBeUndefined(); expect(settled.enodevDelivered).toBeUndefined();
    expect(fs.existsSync(path.join(run, UNRESOLVED_WORKER_FILE))).toBe(false);
    expect(JSON.parse(fs.readFileSync(path.join(run, "settlement.json"), "utf8")).text).toBe("PRIVATE B ANSWER");
    expect(fs.readFileSync(path.join(run, "task.txt"), "utf8")).toBe("session B's retained task");
    await noDelivery();
    const settledOnce = snapshot(run);
    ageSweepClaim(shared); await sweepFromA(shared);
    expect(snapshot(run)).toEqual(settledOnce); // No second settlement/status rewrite, despite another eligible sweep.
    await noDelivery();

    fs.unlinkSync(hold); // Remove only our preservation fixture, then age its activity.
    ageRunRoot(sibling, Date.now() - grace - HOUR);
    ageSweepClaim(shared); await sweepFromA(shared);
    expect(fs.existsSync(run)).toBe(false); expect(fs.existsSync(sibling)).toBe(false);
    expect(snapshot(privateRun)).toEqual(privateBefore); // Shared discovery never widens to private/nested parents.
    await noDelivery();
  }, 60_000);

  it.runIf(process.platform === "linux")("retains live owners and workers, unsafe or linked roots, and nested ENODEV markers in the shared sweep", async () => {
    const base = temp(), shared = path.join(base, "file-data"), privateA = path.join(base, "session-a");
    for (const root of [shared, privateA]) fs.mkdirSync(root, { mode: 0o700 });
    vi.spyOn(os, "tmpdir").mockReturnValue(privateA); vi.stubEnv("TMPDIR", privateA);
    vi.stubEnv("PI_FABRIC_TMPDIR", shared); vi.stubEnv("PI_FABRIC_RUN_ROOT", undefined); vi.stubEnv("PI_FABRIC_DEPTH", "0");
    const aged = { orphanedAt: Date.now() - 7 * HOUR };
    const liveOwner = abandonedRoot(shared, "live-owner", { pid: process.pid }), ownerRun = unconfirmedTask(liveOwner);
    const liveWorker = abandonedRoot(shared, "live-worker", aged), workerRun = unconfirmedTask(liveWorker, "b".repeat(32), process.pid);
    const malformed = abandonedRoot(shared, "malformed", { ...aged, pid: "unknown" }), malformedRun = unconfirmedTask(malformed);
    const unsafe = abandonedRoot(shared, "unsafe", aged), unsafeRun = unconfirmedTask(unsafe);
    const target = path.join(base, "private-target.txt"); fs.writeFileSync(target, "must survive");
    fs.unlinkSync(path.join(unsafeRun, "task.txt")); fs.symlinkSync(target, path.join(unsafeRun, "task.txt"));
    const linkedTarget = abandonedRoot(base, "linked-target", aged), linkedRun = unconfirmedTask(linkedTarget);
    const linked = path.join(shared, FABRIC_RUN_ROOT_PREFIX + "linked"); fs.symlinkSync(linkedTarget, linked, "dir");
    const nestedRoot = abandonedRoot(shared, "nested", aged), parent = unconfirmedTask(nestedRoot, "c".repeat(32));
    const nested = unconfirmedTask(path.join(parent, "nested"), "d".repeat(32));
    ageRunRoot(nestedRoot, Date.now() - 48 * HOUR);
    const retained = [ownerRun, workerRun, malformedRun, linkedRun, nested];
    const before = retained.map(snapshot);
    const ownerBefore = fs.readFileSync(path.join(liveOwner, ".fabric-owner.json"), "utf8");
    const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 }, {
      sweepPath: entry, retention: { ...DEFAULT_FABRIC_CONFIG.retention, orphanedTempRunMs: 6 * HOUR },
    }); managers.push(manager);
    await manager.close();
    expect(launches).toHaveLength(1); expect(launches[0]).toMatchObject({ status: 0, stderr: "" });
    expect(JSON.parse(launches[0]!.args[1]!).tempRoot).toBe(shared);
    expect(retained.map(snapshot)).toEqual(before);
    expect(fs.readFileSync(path.join(liveOwner, ".fabric-owner.json"), "utf8")).toBe(ownerBefore);
    for (const run of retained) expect(fs.existsSync(path.join(run, UNRESOLVED_WORKER_FILE))).toBe(true);
    // Recovery precedes safeRunTree: unsafe metadata may settle; do not promise byte-identical unsafe records.
    expect(fs.existsSync(unsafeRun)).toBe(true); expect(fs.lstatSync(path.join(unsafeRun, "task.txt")).isSymbolicLink()).toBe(true);
    expect(fs.lstatSync(linked).isSymbolicLink()).toBe(true); expect(fs.readFileSync(target, "utf8")).toBe("must survive");
    expect(fs.existsSync(parent)).toBe(true);
    expect(JSON.parse(fs.readFileSync(path.join(parent, "status.json"), "utf8"))).toMatchObject({ status: "completed", settledLost: true });
    expect(fs.existsSync(path.join(parent, UNRESOLVED_WORKER_FILE))).toBe(false);
    expect(JSON.parse(fs.readFileSync(path.join(nested, "status.json"), "utf8")).status).toBe("running");
  });

  it("forwards the manager's configured compaction age and cap to the real sweep entry", async () => {
    const tempRoot = temp(); vi.spyOn(os, "tmpdir").mockReturnValue(tempRoot);
    vi.stubEnv("PI_FABRIC_TMPDIR", undefined); vi.stubEnv("PI_FABRIC_RUN_ROOT", undefined);
    vi.stubEnv("PI_FABRIC_DEPTH", "0");
    const { run } = seed(tempRoot, Date.now() - 7 * HOUR); const before = snapshot(run);
    const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 }, {
      sweepPath: entry,
      retention: { ...DEFAULT_FABRIC_CONFIG.retention, orphanedTempRunMs: 48 * HOUR, oneShotRunMs: 24 * HOUR,
        terminalRunEventsAgeMs: 24 * HOUR, terminalRunEventsMaxBytes: 1024 },
    }); managers.push(manager);
    await manager.close();
    expect(launches).toHaveLength(1);
    expect(launches[0]).toMatchObject({ status: 0, stderr: "" });
    expect(JSON.parse(launches[0]!.args[1]!)).toMatchObject({ terminalRunEventsAgeMs: 24 * HOUR, terminalRunEventsMaxBytes: 1024 });
    expect(snapshot(run)).toEqual(before);
  });

  it("keeps full raw logs regardless of the old compaction cap, then archives at expiry", async () => {
    const tempRoot = temp();
    const { root, run, log } = seed(tempRoot, Date.now() - 7 * HOUR);
    const request = { tempRoot, orphanedTempRunRetentionMs: 48 * HOUR, oneShotRunRetentionMs: 48 * HOUR,
      terminalRunEventsAgeMs: 24 * HOUR, terminalRunEventsMaxBytes: 1024 };
    const invoke = async () => {
      const [runtime, ...args] = await scriptSpawnArgs(entry, [JSON.stringify(request)]);
      const result = childProcess.spawnSync(runtime!, args, { encoding: "utf8", timeout: 20_000 });
      expect(result.status, result.stderr).toBe(0);
    };
    const before = snapshot(run); await invoke(); expect(snapshot(run)).toEqual(before);
    const finishedAt = Date.now() - 24 * HOUR - 1000;
    // Age the root as well: no worker can finish before its root was created.
    fs.writeFileSync(path.join(root, ".fabric-owner.json"), JSON.stringify({ pid: 2147483647, startedAt: finishedAt, heartbeatAt: finishedAt, closedAt: finishedAt, childrenStopped: true }));
    const record = JSON.parse(fs.readFileSync(path.join(run, "status.json"), "utf8"));
    fs.writeFileSync(path.join(run, "status.json"), JSON.stringify({ ...record, finishedAt }));
    fs.utimesSync(run, finishedAt / 1000, finishedAt / 1000);
    const old = snapshot(run); await invoke();
    expect(fs.readFileSync(path.join(run, "events.jsonl"), "utf8")).toBe(log);
    expect(snapshot(run).mtime).toBe(old.mtime);
    expect(fs.readFileSync(path.join(run, "status.json"), "utf8")).toBe(JSON.stringify({ ...record, finishedAt }));
    expect(sweepTempRunRoots({ ...request, now: finishedAt + 48 * HOUR - 1 }).removedRuns).toEqual([]);
    expect(sweepTempRunRoots({ ...request, now: finishedAt + 48 * HOUR }).removedRuns).toEqual([run]);
    const archive = `${root}.trace-archive`;
    const receipt = JSON.parse(fs.readFileSync(path.join(archive, fs.readdirSync(archive)[0]!, "custody.json"), "utf8"));
    expect(fs.readFileSync(path.join(receipt.destination, "events.jsonl"), "utf8")).toBe(log);
    // Keep the serialized request type part of this entry-point regression.
    const typed: TempRunSweepRequest = request; expect(typed.tempRoot).toBe(tempRoot);
  });

  it("does not truncate raw traces in the entry even after the configured compaction age", async () => {
    const tempRoot = temp(); const { run, log } = seed(tempRoot, Date.now() - 25 * HOUR);
    const request = { tempRoot, orphanedTempRunRetentionMs: 48 * HOUR, oneShotRunRetentionMs: 48 * HOUR,
      terminalRunEventsAgeMs: 24 * HOUR, terminalRunEventsMaxBytes: 1024 };
    const mtime = fs.statSync(run).mtimeMs;
    const [runtime, ...args] = await scriptSpawnArgs(entry, [JSON.stringify(request)]);
    const result = childProcess.spawnSync(runtime!, args, { encoding: "utf8", timeout: 20_000 });
    expect(result.status, result.stderr).toBe(0);
    expect(fs.readFileSync(path.join(run, "events.jsonl"), "utf8")).toBe(log);
    expect(fs.statSync(run).mtimeMs).toBe(mtime);
  });

  it("does not truncate young raw traces even with a shorter configured compaction age", async () => {
    const tempRoot = temp(); const finishedAt = Date.now() - 2 * HOUR;
    const { run, log } = seed(tempRoot, finishedAt);
    // Compare the filesystem timestamp, not the input rounded through utimes.
    const mtime = fs.statSync(run).mtimeMs;
    const request = { tempRoot, orphanedTempRunRetentionMs: 48 * HOUR, oneShotRunRetentionMs: 48 * HOUR,
      terminalRunEventsAgeMs: HOUR, terminalRunEventsMaxBytes: 1024 };
    const [runtime, ...args] = await scriptSpawnArgs(entry, [JSON.stringify(request)]);
    const result = childProcess.spawnSync(runtime!, args, { encoding: "utf8", timeout: 20_000 });
    expect(result.status, result.stderr).toBe(0);
    expect(fs.readFileSync(path.join(run, "events.jsonl"), "utf8")).toBe(log);
    expect(fs.statSync(run).mtimeMs).toBe(mtime);
  });
});
