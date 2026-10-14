import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { fakeManagerHeapWorker } from "./fixtures/manager-heap-probe.js";
import { writeJsonAtomic } from "../src/core/atomic-write.js";
import type { AgentTransportLaunch } from "../src/agents/types.js";
import { recordManagerLaunchRemovals } from "./fixtures/manager-launch-heap-probe.js";
import type { managerLaunchHeapProbe } from "./fixtures/manager-launch-heap-probe.js";

// Recorded Main OOM (smarty-dev#10720), audit findings 1/2: public receiver,
// bootstrap, settlement and retry custody, not private helper/source-text tests.
const execute = promisify(execFile);
const FIVE_MIB = 5 * 1024 * 1024;
const directory = () => fs.mkdtempSync(path.join(os.tmpdir(), "manager-launch-heap-"));
const managers: AgentManager[] = [];
let removals: ReturnType<typeof recordManagerLaunchRemovals>;
beforeEach(() => { removals = recordManagerLaunchRemovals(); });
afterEach(async () => {
  try { await Promise.all(managers.splice(0).map(manager => manager.close())); }
  finally { vi.restoreAllMocks(); removals.restore(); }
  // Private roots remain for their environment owner; no cleanup deletes here.
});

describe("public manager launch receiver/bootstrap", () => {
  it.each([
    { name: " \r\t Alpha\nBeta \t ", task: "original task\nunchanged", expected: "Alpha Beta" },
    { name: "\r\n\t ", task: "original task", expected: "Fabric agent" },
    { name: "x".repeat(59) + "😀tail", task: "original task", expected: "x".repeat(59) + "\ud83d" },
    { task: "\nbody is not empty", expected: "Fabric agent" },
    { task: " \tline\rName\nsecond line", expected: "line Name" },
  ])("keeps replace/trim/60-code-unit/default semantics: $expected", async input => {
    const received: Array<{ name: string; task: string; argumentName: string }> = [];
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
      const args = request.workerArguments;
      received.push({ name: request.name, argumentName: args[args.indexOf("--name") + 1]!,
        task: fs.readFileSync(args[args.indexOf("--task-file") + 1]!, "utf8") });
      return fakeManagerHeapWorker(request);
    });
    const manager = new AgentManager(process.cwd(), {
      ...DEFAULT_FABRIC_CONFIG.agents, runner: "pi", budgetUsd: 0, retainRuns: true, sessionExport: false,
    }, { runRoot: directory(), workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fullCodeMode: false });
    managers.push(manager);
    const { expected, ...request } = input;
    const result = await manager.run({ ...request, transport: "process", extensions: false });
    expect(received).toEqual([{ name: expected, argumentName: expected, task: request.task }]);
    expect(result).toMatchObject({ name: expected, task: request.task, status: "completed" });
    expect((await manager.wait(result.id)).task).toBe(request.task);
    expect(manager.hasRunCustody(result.id)).toBe(true);
    expect(manager.isSettled(result.id)).toBe(true);
  });
});

// Keep reason: authority and native-custody trust boundaries, plus the recorded
// smarty-dev#10720 callback retainer. Exercise stored callbacks via real spawn.
const callbackManager = (maxConcurrent = 1) => {
  const root = directory();
  const manager = new AgentManager(process.cwd(), {
    ...DEFAULT_FABRIC_CONFIG.agents, runner: "pi", budgetUsd: 0, maxConcurrent,
    retainRuns: true, sessionExport: false,
  }, { runRoot: root, workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fullCodeMode: false });
  managers.push(manager);
  return { manager, root };
};
const callbackTask = { task: "small callback task", transport: "process" as const, extensions: false };
const nativeLatch = () => {
  let alive = true, resolve!: () => void;
  const closed = new Promise<void>(done => { resolve = done; });
  return { closed, alive: () => alive, stop: () => closed,
    release() { alive = false; resolve(); } };
};
const runningCallbackWorker = async (request: AgentTransportLaunch, held: ReturnType<typeof nativeLatch>) => {
  const handle = await fakeManagerHeapWorker(request, 0, false, held);
  const file = request.workerArguments[request.workerArguments.indexOf("--status-file") + 1]!;
  const record = JSON.parse(fs.readFileSync(file, "utf8"));
  writeJsonAtomic(file, { ...record, status: "running", finishedAt: undefined, turns: 0 });
  return handle;
};

describe("stored manager callback custody and authority", () => {
  it("marks an unregistered launch, then looks up the current registered run even after settlement", async () => {
    const { manager, root } = callbackManager();
    let saved!: AgentTransportLaunch;
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
      saved = request;
      request.onUnconfirmedExit!("before registration");
      const marker = JSON.parse(fs.readFileSync(path.join(root, request.id, "unresolved-worker.json"), "utf8"));
      expect(marker).toMatchObject({ runId: request.id, transport: "process", reason: "before registration" });
      expect(marker.sessionId).toBeUndefined();
      return fakeManagerHeapWorker(request);
    });
    const handle = await manager.spawn(callbackTask);
    await manager.wait(handle.id, { timeoutMs: 2_000 });
    expect(manager.isSettled(handle.id)).toBe(true);
    // Native custody can report uncertainty after the logical outcome. The
    // callback must survive, and must use the current owner rather than fallback.
    saved.onUnconfirmedExit!("after logical settlement");
    const marker = JSON.parse(fs.readFileSync(path.join(root, handle.id, "unresolved-worker.json"), "utf8"));
    expect(marker).toMatchObject({ runId: handle.id, transport: "process", sessionId: "2147483647",
      reason: "after logical settlement" });
    await expect(manager.cleanup(handle.id)).rejects.toThrow(/lost track of its worker/);
    expect(manager.hasRunCustody(handle.id)).toBe(true);
    expect(removals.recorded).not.toContain(path.join(root, handle.id));
  });

  it.each([false, true])("preserves queued revocation versus predicate errors (throws=%s)", async throws => {
    const { manager } = callbackManager();
    const held = nativeLatch();
    const launched: string[] = [];
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(request => {
      launched.push(request.id);
      return runningCallbackWorker(request, held);
    });
    try {
      const blocker = await manager.spawn(callbackTask);
      const predicateError = new Error("raw authority predicate failed");
      const queued = await manager.spawn(callbackTask, undefined, () => {
        if (throws) throw predicateError;
        return false;
      });
      expect(queued.status).toBe("queued");
      held.release();
      await manager.stop(blocker.id);
      const result = await manager.wait(queued.id, { timeoutMs: 2_000 });
      expect(result.status).toBe(throws ? "failed" : "stopped");
      expect(result.error).toBe(throws ? predicateError.message : "Agent activation no longer authorized");
      expect(launched).toEqual([blocker.id]);
    } finally { held.release(); }
  });

  it("rechecks authority at the final launch boundary after the commit fence", async () => {
    const { manager } = callbackManager();
    let authorized = true;
    const launch = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(request => fakeManagerHeapWorker(request));
    await expect(manager.spawn(callbackTask, undefined, () => authorized, () => { authorized = false; }))
      .rejects.toThrow("Agent activation no longer authorized");
    expect(launch).not.toHaveBeenCalled();
    // Rejection must return the permit rather than permanently queue admission.
    expect((await manager.run(callbackTask)).status).toBe("completed");
  });

  it.each([false, true])("installs native custody before post-launch authority cancellation (throws=%s)", async throws => {
    const { manager, root } = callbackManager();
    const held = nativeLatch();
    let authorized = true, saved!: AgentTransportLaunch;
    const predicateError = new Error("authority threw after launch");
    const launched: string[] = [];
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
      launched.push(request.id);
      if (launched.length > 1) return fakeManagerHeapWorker(request);
      saved = request;
      expect(request.authorize!()).toBe(true);
      const worker = await runningCallbackWorker(request, held);
      authorized = false;
      return worker;
    });
    try {
      const handle = await manager.spawn(callbackTask, undefined, () => {
        if (!authorized && throws) throw predicateError;
        return authorized;
      });
      expect(manager.hasRunCustody(handle.id)).toBe(true);
      expect(manager.isSettled(handle.id)).toBe(false);
      // Stored authority calls the raw predicate, retaining exact exceptions.
      expect(() => saved.authorize!()).toThrow(throws ? predicateError : "Agent activation no longer authorized");
      const queued = await manager.spawn(callbackTask);
      expect(queued.status).toBe("queued");
      expect(launched).toEqual([handle.id]);
      expect(removals.recorded).not.toContain(path.join(root, handle.id));
      held.release();
      expect((await manager.wait(handle.id, { timeoutMs: 2_000 })).status).toBe("stopped");
      expect((await manager.wait(queued.id, { timeoutMs: 2_000 })).status).toBe("completed");
      expect(launched).toEqual([handle.id, queued.id]);
    } finally { held.release(); }
  });

  it("keeps the same authority callback on resume and refuses a revoked replacement worker", async () => {
    const { manager } = callbackManager();
    let authorized = true, firstLaunched = false, saved!: AgentTransportLaunch;
    const prepare = manager.prepareModelForAdmission.bind(manager);
    vi.spyOn(manager, "prepareModelForAdmission").mockImplementation(async (...args) => {
      if (firstLaunched) authorized = false;
      return prepare(...args);
    });
    let attempts = 0, workers = 0;
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
      attempts++;
      if (attempts > 1) expect(request.authorize).toBe(saved.authorize);
      expect(request.authorize!()).toBe(true); // revoked retry throws before worker creation
      workers++;
      saved = request;
      const worker = await fakeManagerHeapWorker(request);
      const file = request.workerArguments[request.workerArguments.indexOf("--status-file") + 1]!;
      const record = JSON.parse(fs.readFileSync(file, "utf8"));
      writeJsonAtomic(file, { ...record, status: "stopped", error: "interrupted task", turns: 3 });
      firstLaunched = true;
      return worker;
    });
    const handle = await manager.spawn(callbackTask, undefined, () => authorized);
    const result = await manager.wait(handle.id, { timeoutMs: 2_000 });
    expect(result.status).toBe("stopped");
    expect(result.error).toBe("interrupted task · relaunch failed: Agent activation no longer authorized");
    expect(attempts).toBe(2);
    expect(workers).toBe(1);
    expect(manager.hasRunCustody(handle.id)).toBe(true);
  });

  it("removes the identical stored abort listener on settlement", async () => {
    const { manager } = callbackManager();
    const controller = new AbortController();
    const added = vi.spyOn(AbortSignal.prototype, "addEventListener");
    const removed = vi.spyOn(AbortSignal.prototype, "removeEventListener");
    const stop = vi.fn(async () => {});
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
      const worker = await fakeManagerHeapWorker(request);
      return { ...worker, stop };
    });
    const handle = await manager.spawn(callbackTask, controller.signal);
    await manager.wait(handle.id, { timeoutMs: 2_000 });
    expect(removed.mock.calls.some(([event, listener], index) => event === "abort" &&
      added.mock.calls.some(([addedEvent, addedListener], addedIndex) => addedEvent === "abort" &&
        addedListener === listener && added.mock.contexts[addedIndex] === removed.mock.contexts[index]))).toBe(true);
    const callsBeforeAbort = stop.mock.calls.length;
    controller.abort();
    expect(stop.mock.calls).toHaveLength(callsBeforeAbort);
    expect(manager.isSettled(handle.id)).toBe(true);
  });
});

// Fixed JavaScript owns the child command. Jiti's transform hook replaces ONLY
// manager.ts in memory; all imports resolve from its actual source location.
// No seeded source files, Git operations, real workers or filesystem removals.
const childProgram = String.raw`
  import fs from "node:fs";
  import path from "node:path";
  import { createHash } from "node:crypto";
  import { createJiti } from "jiti";
  import { pathToFileURL } from "node:url";
  const [mode, root, variant, baselinePath] = process.argv.slice(1);
  const base = pathToFileURL(process.cwd() + "/index.js").href;
  const transformer = createJiti(base, { fsCache: false, tryNative: false });
  const managerPath = path.resolve("src/agents/manager.ts");
  let substituted = 0, sourceSha256 = "";
  const jiti = createJiti(base, { fsCache: false, tryNative: false, transform(options) {
    if (options.filename === managerPath) {
      let source = options.source;
      if (variant === "negative") {
        if (baselinePath) source = fs.readFileSync(baselinePath, "utf8");
        else {
          // Recreate the exact two old boundaries only; every other repair stays.
          source = source.replace("detachedPreview(note, MAX_RESUME_NOTE_CHARS)", "note.slice(0, MAX_RESUME_NOTE_CHARS)");
          source = source.replace('detachedPreview(value.replace(/[\\r\\n\\t]+/g, " ").trim(), MAX_NAME_LENGTH)',
            'value.replace(/[\\r\\n\\t]+/g, " ").trim().slice(0, MAX_NAME_LENGTH)');
        }
        substituted++;
      }
      sourceSha256 = createHash("sha256").update(source).digest("hex");
      return { code: transformer.transform({ ...options, source }) };
    }
    return { code: transformer.transform(options) };
  } });
  const { managerLaunchHeapProbe } = await jiti.import("./tests/fixtures/manager-launch-heap-probe.ts");
  const receipt = await managerLaunchHeapProbe(mode, root);
  console.log(JSON.stringify({ completedReceipt: { ...receipt, variant, substituted, sourceSha256, closed: true } }));
`;

type Receipt = Awaited<ReturnType<typeof managerLaunchHeapProbe>> & {
  variant: "negative" | "fixed"; substituted: number; sourceSha256: string; closed: boolean;
};
const probe = async (mode: "name" | "resume", variant: "negative" | "fixed") => {
  const baselinePath = process.env.PI_MANAGER_LAUNCH_BASELINE ?? "";
  try {
    const result = await execute("node", ["--expose-gc", "--max-old-space-size=4096", "--input-type=module", "-e", childProgram,
      mode, directory(), variant, baselinePath], { cwd: process.cwd(), timeout: 90_000, killSignal: "SIGKILL", maxBuffer: 1024 * 1024 });
    // Print receipts before ANY assertions, including the intentionally leaking control.
    process.stdout.write(result.stdout);
    if (result.stderr) process.stderr.write(result.stderr);
    const rows = result.stdout.trim().split("\n").map(line => JSON.parse(line) as { completedReceipt?: Receipt });
    const receipt = rows.find(row => row.completedReceipt)?.completedReceipt;
    if (!receipt) throw new Error("child did not return a joined completion receipt");
    return receipt;
  } catch (error) {
    const output = error as { stdout?: string; stderr?: string };
    if (output.stdout) process.stdout.write(output.stdout);
    if (output.stderr) process.stderr.write(output.stderr);
    throw error;
  }
};

it.each(["name", "resume"] as const)(
  "%s retains <5 MiB beyond measured intentional state; original public path leaks in the negative control", async mode => {
    // Sequential fresh children bound peak memory, not four simultaneous heaps.
    const negative = await probe(mode, "negative");
    const fixed = await probe(mode, "fixed");
    for (const receipt of [negative, fixed]) {
      expect(receipt).toMatchObject({ mode, runs: 100, parentUnits: 4 * 1024 * 1024,
        initialWorkers: 100, settledHandles: 100, checks: [], closed: true });
      expect(receipt.sourceSha256).toMatch(/^[a-f0-9]{64}$/);
      if (mode === "resume") {
        expect(receipt).toMatchObject({ unavoidableErrorUnits: 419_430_400, resumeTaskUnits: 65_536,
          beforeNoteArrivals: 100, preparationArrivals: 100, launchArrivals: 100,
          custodyAtPreparation: 100, custodyAtLaunch: 100, resumedWorkers: 100 });
        // This verifies the measured baseline really includes the full live errors,
        // rather than excusing arbitrary manager-owned retention by subtraction.
        expect(receipt.observedStoppedStateBytes).toBeGreaterThanOrEqual(receipt.unavoidableErrorUnits);
        expect(receipt.retainedBytes).toBe(receipt.preparationBytes - receipt.stoppedBaselineBytes);
      }
    }
    expect(negative.substituted).toBe(1);
    expect(fixed.substituted).toBe(0);
    expect(negative.sourceSha256).not.toBe(fixed.sourceSha256);
    expect(negative.retainedBytes).toBeGreaterThan(FIVE_MIB);
    expect(fixed.retainedBytes).toBeLessThan(FIVE_MIB);
  },
  // Each of the two joined children has a 90-s process deadline; 190 s is a
  // finite pair budget (100 s cannot cover two worst-case child deadlines).
  190_000,
);
