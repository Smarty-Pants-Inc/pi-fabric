import fs from "node:fs";
import os from "node:os";
import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { WorktreeManager } from "../src/agents/worktree-manager.js";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { SessionManager } from "@earendil-works/pi-coding-agent";
import { decideModelRoute, prepareRouteDispatch, ROUTE_DEADLINE_MS, type RouteEvaluate } from "../src/agents/model-route.js";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { normalizeAgentRunRequest } from "../src/agents/request.js";
import { normalizeFabricConfig, DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import type { JevResponse } from "../src/jev/types.js";

// Keep native spawn, but own its exact child/close event before launch returns.
// Transport liveness can report exit before Windows releases the cwd handle.
vi.mock("node:child_process", async (importOriginal) => {
  const actual = await importOriginal<typeof import("node:child_process")>();
  return { ...actual, spawn: vi.fn(actual.spawn) };
});
const ownedChildren: Array<{ child: ChildProcess; closed: Promise<void> }> = [];
const removeFixtureRoots = async () => {
  await Promise.all(ownedChildren.splice(0).map(async ({ child, closed }) => {
    // Also release a worker if an assertion prevented normal manager shutdown.
    // The owned native instance is the identity, never a possibly reused PID.
    if (child.exitCode === null && child.signalCode === null) child.kill("SIGTERM");
    await closed;
  }));
  for (const dir of roots.splice(0)) fs.rmSync(dir, { recursive: true, force: true });
};
const pin = { model: "test/sol", effort: "high" as const };
const cheap = { model: "test/luna", effort: "medium" as const };
const input = { routeClass: "bounded-lookup", protected: false, pin, candidates: [cheap], parentSessionId: "parent" };
const response = (confidence = .95, probability = .95): JevResponse => ({ model: "jev", answers: {
  route: { type: "choice", choice: "candidate-1", confidence, probabilities: { "candidate-0": 1 - probability, "candidate-1": probability } },
}, usage: { input_tokens: 1, output_tokens: 1 } });
const roots: string[] = [];
const managers: AgentManager[] = [];
const root = () => { const dir = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-route-")); roots.push(dir); return dir; };
const ledgerFile = () => path.join(process.env.PI_CODING_AGENT_DIR!, "fabric/model-routing.jsonl");
beforeEach(async () => {
  vi.stubEnv("PI_CODING_AGENT_DIR", path.join(root(), "agent"));
  const actual = await vi.importActual<typeof import("node:child_process")>("node:child_process");
  vi.mocked(spawn).mockImplementation((...args: Parameters<typeof spawn>) => {
    const child = actual.spawn(...args);
    const closed = new Promise<void>(resolve => child.once("close", () => resolve()));
    ownedChildren.push({ child, closed });
    return child;
  });
});
afterEach(async () => {
  vi.useRealTimers();
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  await removeFixtureRoots();
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
});

describe("routing fixture process lifetime", () => {
  it("keeps roots until native close even after the worker has exited", async () => {
    const dir = root();
    const child = spawn(process.execPath, ["-e", "process.exit(0)"], { cwd: dir, stdio: "ignore" });
    const emit = child.emit.bind(child);
    let releaseClose: (() => void) | undefined;
    const closeObserved = new Promise<void>(resolve => {
      vi.spyOn(child, "emit").mockImplementation((event, ...args) => {
        if (event === "close") {
          releaseClose = () => { releaseClose = undefined; emit(event, ...args); };
          resolve();
          return true;
        }
        return emit(event, ...args);
      });
    });
    let teardown: Promise<void> | undefined;
    try {
      await closeObserved;
      expect(child.exitCode).toBe(0);
      teardown = removeFixtureRoots();
      await Promise.resolve();
      expect(fs.existsSync(dir)).toBe(true);
      releaseClose!();
      await teardown;
      expect(fs.existsSync(dir)).toBe(false);
    } finally {
      releaseClose?.();
      await teardown;
    }
  });
});

describe("shadow model routing", () => {
  it("keeps derived metadata on queued and host-stopped task receipts", async () => {
    const dir = root();
    const manager = new AgentManager(dir, { ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent: 1, retainRuns: true }, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(dir, "runs"),
    }); managers.push(manager);
    const blocker = await manager.spawn({ task: "HANG", transport: "process", protected: true });
    const request = { task: "review security status-groom", transport: "process" as const, protected: false };
    const queued = await manager.spawn(request);
    expect(queued).toMatchObject({ status: "queued", routeClass: "task:pi:process", routeClassSource: "derived", protected: false });
    request.protected = true; // The accepted receipt must retain its admission snapshot.
    expect(await manager.stop(queued.id)).toMatchObject({ status: "stopped", routeClass: "task:pi:process", routeClassSource: "derived", protected: false });
    expect(await manager.stop(blocker.id)).toMatchObject({ status: "stopped", routeClass: "task:pi:process", routeClassSource: "derived", protected: true });
    expect(JSON.parse(fs.readFileSync(path.join(dir, "runs", blocker.id, "status.json"), "utf8")))
      .toMatchObject({ routeClass: "task:pi:process", routeClassSource: "derived", protected: true });
  });
  it.each([true, undefined, null, "review", "security", "audit", "needs-security-pass", "unknown", "false"])("excludes protected or unknown before Jev: %s", async protectedFlag => {
    const evaluate = vi.fn(async () => response());
    const result = await decideModelRoute({ ...input, protected: protectedFlag }, evaluate);
    expect(evaluate).not.toHaveBeenCalled();
    expect(result).toMatchObject({ ...pin, shadowChoice: pin, reasonCode: protectedFlag === true ? "excluded-protected" : "excluded-unknown" });
  });
  it("excludes unknown classes before Jev", async () => {
    const evaluate = vi.fn(async () => response());
    expect((await decideModelRoute({ ...input, routeClass: "unknown" }, evaluate)).reasonCode).toBe("excluded-class");
    expect(evaluate).not.toHaveBeenCalled();
  });
  it("excludes unavailable candidates before Jev", async () => {
    const evaluate = vi.fn(async () => response());
    expect((await decideModelRoute({ ...input, candidatesValid: false }, evaluate)).reasonCode).toBe("invalid-candidates");
    expect(evaluate).not.toHaveBeenCalled();
  });
  it.each([[.89, .99], [.99, .89], [0, 1]])("falls back when either confidence/probability is below .90: %s/%s", async (confidence, probability) => {
    const result = await decideModelRoute(input, async () => response(confidence, probability));
    expect(result).toMatchObject({ ...pin, confidence, probability, shadowChoice: cheap, reasonCode: "low-confidence" });
  });
  it("accepts both thresholds at .90, exactly one Choice and no task text", async () => {
    const evaluate = vi.fn<RouteEvaluate>(async () => response(.9, .9));
    const result = await decideModelRoute(input, evaluate);
    expect(result).toMatchObject({ ...cheap, mode: "shadow", reasonCode: "shadow-choice" });
    expect(evaluate).toHaveBeenCalledTimes(1);
    const request = evaluate.mock.calls[0]?.[0];
    expect(Object.keys((request as unknown as { questions: object }).questions)).toEqual(["route"]);
    expect(JSON.stringify(request)).not.toContain("task");
  });
  it("deduplicates finite candidates and defaults to pin only", async () => {
    const evaluate: RouteEvaluate = async request => {
      expect(request.questions.route).toMatchObject({ criteria: { "candidate-0": pin } });
      return { ...response(), answers: { route: { type: "choice", choice: "candidate-0", confidence: 1, probabilities: { "candidate-0": 1 } } } };
    };
    expect((await decideModelRoute({ ...input, candidates: [pin] }, evaluate)).shadowChoice).toEqual(pin);
  });
  it("records Jev errors with no retry", async () => {
    const evaluate = vi.fn(async () => { throw new Error("network unavailable"); });
    expect(await decideModelRoute(input, evaluate)).toMatchObject({ ...pin, reasonCode: "jev-error" });
    expect(evaluate).toHaveBeenCalledTimes(1);
  });
  it("enforces 2.5 second deadline even on a non-cooperative evaluator", async () => {
    vi.useFakeTimers();
    const evaluate = vi.fn(() => new Promise<JevResponse>(() => {}));
    const pending = decideModelRoute(input, evaluate);
    await vi.advanceTimersByTimeAsync(ROUTE_DEADLINE_MS);
    expect(await pending).toMatchObject({ ...pin, reasonCode: "jev-timeout" });
    expect(evaluate).toHaveBeenCalledTimes(1);
  });
  it("records shorter client timeouts as timeout, not a generic error", async () => {
    expect((await decideModelRoute(input, async () => { throw new Error("Jev request cancelled or timed out"); })).reasonCode).toBe("jev-timeout");
  });
  it("propagates caller cancellation rather than dispatching a fallback", async () => {
    const controller = new AbortController();
    const pending = decideModelRoute(input, () => new Promise<JevResponse>(() => {}), controller.signal);
    controller.abort(new Error("caller cancelled"));
    await expect(pending).rejects.toThrow("caller cancelled");
  });
  it.each([
    {}, { answers: { route: { type: "noul", noul: 1 } } },
    { answers: { route: { ...response().answers.route, choice: "arbitrary-model" } } },
    { answers: { route: { ...response().answers.route, confidence: NaN } } },
    { answers: { route: { ...response().answers.route, confidence: 1.1 } } },
    { answers: { route: { ...response().answers.route, probabilities: { "candidate-1": .95 } } } },
    { answers: { route: { ...response().answers.route, probabilities: { "candidate-0": .95, "candidate-1": .95 } } } },
    { answers: { route: { ...response().answers.route, probabilities: { "candidate-0": -.1, "candidate-1": 1.1 } } } },
  ])("rejects malformed or out-of-list choices: %#", async malformed => {
    expect(await decideModelRoute(input, async () => malformed as unknown as JevResponse)).toMatchObject({ ...pin, reasonCode: "malformed" });
  });
  it("classifies client typed-response validation failures as malformed", async () => {
    expect((await decideModelRoute(input, async () => { throw new Error("TypeSafe returned an invalid or oversized typed response"); })).reasonCode).toBe("malformed");
  });
});

describe("durable route dispatch", () => {
  it("preserves participant preparation arguments and explicitly marks required route pins", async () => {
    const dir = root();
    const preparePiModel = vi.fn(async (model: string | undefined, _requiredPin?: boolean) => model);
    const manager = new AgentManager(dir, DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
      runRoot: path.join(dir, "runs"),
      preparePiModel,
    }); managers.push(manager);
    await expect(manager.prepareModelForAdmission(pin.model, "pi")).resolves.toBe(pin.model);
    expect(preparePiModel).toHaveBeenNthCalledWith(1, pin.model);
    await expect(manager.prepareModelForAdmission(pin.model, "pi", undefined, true)).resolves.toBe(pin.model);
    expect(preparePiModel).toHaveBeenNthCalledWith(2, pin.model, true);
    await expect(manager.prepareModelForAdmission(undefined, "pi")).resolves.toBeUndefined();
    expect(preparePiModel).toHaveBeenNthCalledWith(3, undefined);
    expect(preparePiModel).toHaveBeenCalledTimes(3);
  });
  it("bounds routed preparation before launch and admits a fresh strict pin after timeout", async () => {
    const dir = root();
    const decision = await decideModelRoute(input, async () => response());
    let resume!: () => void;
    const gate = new Promise<void>(resolve => { resume = resolve; });
    let calls = 0;
    const preparePiModel = vi.fn(async (model: string | undefined, _requiredPin?: boolean) => {
      if (++calls === 1) await gate;
      return model;
    });
    const manager = new AgentManager(dir, { ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent: 1 }, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(dir, "runs"), preparePiModel,
    }); managers.push(manager);
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    const onPreparing = vi.fn();
    try {
      await expect(manager.spawn({ task: "blocked pin", routeDecision: decision }, undefined, undefined,
        undefined, undefined, undefined, { timeoutMs: 40, onPreparing })).rejects.toMatchObject({
        name: "AgentLaunchPreparationTimeoutError", code: "FABRIC_AGENT_LAUNCH_PREPARATION_TIMEOUT", launchOutcome: "unlaunched",
      });
      expect(launch).not.toHaveBeenCalled();
      expect(manager.runningCount()).toBe(0);
      expect(onPreparing).toHaveBeenCalledTimes(1);
      const handle = await manager.spawn({ task: "fresh pin", routeDecision: decision }, undefined, undefined,
        undefined, undefined, undefined, { timeoutMs: 40 });
      expect((await manager.wait(handle.id)).status).toBe("completed");
      resume(); await new Promise(resolve => setTimeout(resolve, 80));
      expect(preparePiModel).toHaveBeenNthCalledWith(1, pin.model, true);
      expect(preparePiModel).toHaveBeenNthCalledWith(2, pin.model, true);
      expect(launch).toHaveBeenCalledTimes(1);
      const outcomes = fs.readFileSync(ledgerFile(), "utf8").trim().split("\n").map(line => JSON.parse(line))
        .filter(row => row.status);
      expect(outcomes.map(row => row.status)).toEqual(["failed", "completed"]);
    } finally { resume(); }
  });
  it("applies each strict preparation waiter's deadline to a shared pending promise", async () => {
    const dir = root();
    let resume!: () => void;
    const gate = new Promise<void>(resolve => { resume = resolve; });
    const preparePiModel = vi.fn(async (model: string | undefined, _requiredPin?: boolean) => { await gate; return model; });
    const manager = new AgentManager(dir, DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(dir, "runs"), preparePiModel,
    }); managers.push(manager);
    const owner = new AbortController();
    const first = manager.prepareModelForAdmission(pin.model, "pi", undefined, true, owner.signal);
    try {
      await expect(manager.prepareModelForAdmission(pin.model, "pi", undefined, true, undefined, 40))
        .rejects.toMatchObject({ name: "AgentLaunchPreparationTimeoutError", timeoutMs: 40 });
      expect(preparePiModel).toHaveBeenCalledTimes(1);
      resume();
      await expect(first).resolves.toBe(pin.model);
    } finally { resume(); await first; }
  });
  it.each(["startup", "resume"])("R3 refuses a replacement model during %s recovery", async phase => {
    const dir = root();
    const decision = await decideModelRoute(input, async () => response());
    let preparations = 0;
    const manager = new AgentManager(dir, { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: true }, {
      workerPath: path.resolve(phase === "startup" ? "tests/fixtures/fake-worker-startup-retry.mjs" : "tests/fixtures/fake-worker.mjs"),
      runRoot: path.join(dir, "runs"),
      // Model disappears; ordinary fuzzy preparation would select the similar authenticated model.
      preparePiModel: async () => ++preparations === 1 ? pin.model : "test/sol-next",
    }); managers.push(manager);
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    const result = await manager.run({ task: phase === "startup" ? "Recover startup" : "RESUME_AFTER_CRASH", routeDecision: decision, transport: "process" });
    expect(preparations).toBe(2);
    expect(launch).toHaveBeenCalledTimes(1);
    expect(result.status).toBe("failed");
    expect(result.error).toContain("MODEL_ROUTE_PIN_MISMATCH");
    expect(result).toMatchObject({ routeClass: "bounded-lookup", routeClassSource: "explicit" });
    expect(JSON.parse(fs.readFileSync(path.join(dir, "runs", result.id, "status.json"), "utf8")))
      .toMatchObject({ routeClass: "bounded-lookup", routeClassSource: "explicit" });
    const rows = fs.readFileSync(ledgerFile(), "utf8").trim().split("\n").map(line => JSON.parse(line));
    expect(rows[1]).toMatchObject({ status: "failed", admittedModel: null, admittedEffort: null });
  }, 30000);
  it("R3 binds native identity and cwd to the final worktree before launch", async () => {
    const dir = root(); const final = root();
    vi.spyOn(WorktreeManager.prototype, "create").mockResolvedValue({ gitRoot: dir, path: final, cwd: final, branch: "probe" });
    vi.spyOn(WorktreeManager.prototype, "cleanup").mockResolvedValue(true);
    const launch = ProcessTransport.prototype.launch;
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function (this: ProcessTransport, request) {
      const argv = request.workerArguments;
      const file = argv[argv.indexOf("--session-file") + 1]!;
      const session = SessionManager.open(file);
      expect(session.getCwd()).toBe(final);
      expect(session.getSessionId()).toBe(argv[argv.indexOf("--id") + 1]);
      return launch.call(this, request);
    });
    const manager = new AgentManager(dir, DEFAULT_FABRIC_CONFIG.agents, { workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(dir, "runs") }); managers.push(manager);
    await manager.run({ task: "lookup", worktree: true, routeDecision: await decideModelRoute(input, async () => response()) });
  });
  it("R3 close collects a successful routed session in the default managed root", async () => {
    const dir = root();
    vi.stubEnv("PI_FABRIC_RUN_ROOT", undefined);
    const manager = new AgentManager(dir, { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: false }, { workerPath: path.resolve("tests/fixtures/fake-worker.mjs") }); managers.push(manager);
    const result = await manager.run({ task: "lookup", routeDecision: await decideModelRoute(input, async () => response()) });
    const runRoot = path.dirname(manager.runDirectory(result.id)!);
    roots.push(runRoot);
    expect(fs.existsSync(path.join(runRoot, result.id, "route-session.jsonl"))).toBe(true);
    await manager.close();
    expect(fs.existsSync(runRoot)).toBe(false);
  });
  it.skipIf(process.platform === "win32").each(["parent", "leaf"])("R2 ignores workspace ledger %s links", async kind => {
    const dir = root(); const outside = root(); const target = path.join(outside, "target"); fs.writeFileSync(target, "unchanged");
    if (kind === "parent") { fs.mkdirSync(path.join(dir, ".pi")); fs.symlinkSync(outside, path.join(dir, ".pi/fabric"), "dir"); }
    else { fs.mkdirSync(path.join(dir, ".pi/fabric"), { recursive: true }); fs.symlinkSync(target, path.join(dir, ".pi/fabric/model-routing.jsonl")); }
    const decision = await decideModelRoute(input, async () => response());
    const manager = new AgentManager(dir, DEFAULT_FABRIC_CONFIG.agents, { workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(dir, "runs") }); managers.push(manager);
    await manager.run({ task: "lookup", routeDecision: decision, transport: "process" });
    expect(fs.readdirSync(outside)).toEqual(["target"]); expect(fs.readFileSync(target, "utf8")).toBe("unchanged");
  });
  it.skipIf(process.platform === "win32").each(["link", "fifo"])("R2 refuses %s ledger endpoints without blocking or changing targets", kind => {
    const dir = root(); const outside = path.join(dir, "outside"); fs.writeFileSync(outside, "unchanged");
    const file = path.join(dir, "ledger");
    if (kind === "link") fs.symlinkSync(outside, file); else expect(spawnSync("mkfifo", [file]).status).toBe(0);
    const program = `import { appendRouteRecord } from ${JSON.stringify(path.resolve("src/agents/model-route.ts"))}; try { appendRouteRecord(${JSON.stringify(file)}, {type:"decision"}); process.exit(2); } catch { process.exit(0); }`;
    const result = spawnSync("bun", ["-e", program], { timeout: 3000, killSignal: "SIGKILL" });
    expect(fs.readFileSync(outside, "utf8")).toBe("unchanged"); expect(result.error).toBeUndefined(); expect(result.status).toBe(0);
  });
  it("R2 records every confirmed pre-worker worktree failure", async () => {
    const dir = root();
    vi.spyOn(WorktreeManager.prototype, "create").mockRejectedValue(new Error("worktree denied"));
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    const manager = new AgentManager(dir, DEFAULT_FABRIC_CONFIG.agents, { runRoot: path.join(dir, "runs") }); managers.push(manager);
    const decision = await decideModelRoute(input, async () => response());
    await expect(manager.spawn({ task: "lookup", worktree: true, routeDecision: decision })).rejects.toThrow("worktree denied");
    const file = fs.existsSync(ledgerFile()) ? ledgerFile() : path.join(dir, ".pi/fabric/model-routing.jsonl");
    const rows = fs.readFileSync(file, "utf8").trim().split("\n").map(line => JSON.parse(line));
    expect(rows).toHaveLength(2); expect(rows[1]).toMatchObject({ status: "failed", admittedModel: null, admittedEffort: null }); expect(launch).not.toHaveBeenCalled();
  });
  it.each(["task.txt", "schema.json", "images.json"])("R2 settles failed pre-worker %s writes", async leaf => {
    const dir = root(); const write = fs.writeFileSync;
    vi.spyOn(fs, "writeFileSync").mockImplementation((file, data, options) => { if (String(file).endsWith(leaf)) throw new Error("input write failed"); return write(file, data, options); });
    const decision = await decideModelRoute(input, async () => response());
    const manager = new AgentManager(dir, DEFAULT_FABRIC_CONFIG.agents, { runRoot: path.join(dir, "runs") }); managers.push(manager);
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    await expect(manager.spawn({ task: "lookup", schema: {}, images: [{ type: "image", data: "YQ==", mimeType: "image/png" }], routeDecision: decision })).rejects.toThrow("input write failed");
    const file = fs.existsSync(ledgerFile()) ? ledgerFile() : path.join(dir, ".pi/fabric/model-routing.jsonl");
    const rows = fs.readFileSync(file, "utf8").trim().split("\n").map(line => JSON.parse(line));
    expect(rows).toHaveLength(2); expect(rows[1]).toMatchObject({ status: "failed", admittedModel: null, admittedEffort: null }); expect(launch).not.toHaveBeenCalled();
  });
  it("R-2 queued worktree-preparation cancellation has one stopped receipt and ledger outcome", async () => {
    const dir = root(); const final = root();
    const manager = new AgentManager(dir, { ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent: 1 }, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(dir, "runs"),
    });
    managers.push(manager);
    const blocker = await manager.spawn({ task: "HANG", transport: "process" });
    let release!: () => void; let entered!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    const preparing = new Promise<void>(resolve => { entered = resolve; });
    const create = vi.spyOn(WorktreeManager.prototype, "create").mockImplementation(async () => {
      entered(); await gate;
      return { gitRoot: dir, path: final, cwd: final, branch: "cancelled-preparation" };
    });
    const cleanup = vi.spyOn(WorktreeManager.prototype, "cleanup").mockResolvedValue(true);
    // The occupying worker is already launched: this spy counts only the queued task.
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    try {
      const decision = await decideModelRoute(input, async () => response());
      const queued = await manager.spawn({ task: "cancel during worktree creation", worktree: true, routeDecision: decision, transport: "process" });
      expect(queued.status).toBe("queued");
      await manager.stop(blocker.id);
      await preparing;
      const stopping = manager.stop(queued.id);
      release(); // preparation succeeds after cancellation; the pre-launch check must stop it
      const receipt = await stopping;
      expect(receipt).toMatchObject({ id: queued.id, status: "stopped", error: "Agent launch aborted" });
      expect(await manager.wait(queued.id)).toMatchObject({ status: "stopped" });
      expect(create).toHaveBeenCalledTimes(1);
      expect(cleanup).toHaveBeenCalledWith(queued.id, true);
      expect(launch).not.toHaveBeenCalled();
      const outcomes = fs.readFileSync(ledgerFile(), "utf8").trim().split("\n").map(line => JSON.parse(line))
        .filter(row => row.type === "outcome" && row.decisionId === decision.decisionId);
      expect(outcomes).toHaveLength(1);
      expect(outcomes[0]).toMatchObject({ childSessionId: queued.id, status: receipt.status, admittedModel: null, admittedEffort: null });
    } finally { release(); }
  });

  it("R2 retains and retries a failed queued outcome before cleanup", async () => {
    const dir = root(); const manager = new AgentManager(dir, { ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent: 1 }, { workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(dir, "runs") }); managers.push(manager);
    const blocker = await manager.spawn({ task: "HANG", transport: "process" });
    const decision = await decideModelRoute(input, async () => response());
    const queued = await manager.spawn({ task: "lookup", routeDecision: decision, transport: "process" });
    const open = fs.openSync; let broken = true;
    vi.spyOn(fs, "openSync").mockImplementation((file, flags, mode) => { if (String(file).endsWith("model-routing.jsonl") && broken) throw new Error("temporary ledger outage"); return open(file, flags, mode); });
    const result = await manager.stop(queued.id);
    expect(result.warnings?.join(" ")).toContain("retained");
    await expect(manager.cleanup(queued.id)).rejects.toThrow("retained");
    expect(manager.list().some(run => run.id === queued.id)).toBe(true);
    const pendingFile = path.join(dir, "runs", queued.id, "pending-route-outcome.json");
    expect(JSON.parse(fs.readFileSync(pendingFile, "utf8")).record).toMatchObject({ decisionId: decision.decisionId, status: "stopped" });
    broken = false; await manager.cleanup(queued.id); await manager.stop(blocker.id);
    expect(fs.existsSync(pendingFile)).toBe(false);
    const rows = fs.readFileSync(ledgerFile(), "utf8").trim().split("\n").map(line => JSON.parse(line));
    expect(rows).toHaveLength(2); expect(rows[1]).toMatchObject({ status: "stopped", decisionId: decision.decisionId });
  });

  it("R2 retries the first queued append failure and writes exactly one outcome", async () => {
    const dir = root(); const manager = new AgentManager(dir, { ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent: 1 }, { workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(dir, "runs") }); managers.push(manager);
    const blocker = await manager.spawn({ task: "HANG", transport: "process" });
    const decision = await decideModelRoute(input, async () => response());
    const queued = await manager.spawn({ task: "lookup", routeDecision: decision, transport: "process" });
    const open = fs.openSync; let attempts = 0;
    vi.spyOn(fs, "openSync").mockImplementation((file, flags, mode) => { if (String(file).endsWith("model-routing.jsonl") && ++attempts === 1) throw new Error("first append failed"); return open(file, flags, mode); });
    const result = await manager.stop(queued.id); expect(result.warnings).toBeUndefined(); expect(attempts).toBe(2);
    await manager.cleanup(queued.id); await manager.stop(blocker.id);
    expect(fs.readFileSync(ledgerFile(), "utf8").trim().split("\n")).toHaveLength(2);
  });
  it("R2 retains the exact pending queued join across close during persistent storage failure", async () => {
    const dir = root(); const manager = new AgentManager(dir, { ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent: 1 }, { workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(dir, "runs") }); managers.push(manager);
    await manager.spawn({ task: "HANG", transport: "process" });
    const decision = await decideModelRoute(input, async () => response());
    const queued = await manager.spawn({ task: "lookup", routeDecision: decision, transport: "process" });
    const open = fs.openSync; let attempts = 0;
    vi.spyOn(fs, "openSync").mockImplementation((file, flags, mode) => { if (String(file).endsWith("model-routing.jsonl")) { attempts++; throw new Error("persistent append failure"); } return open(file, flags, mode); });
    await manager.stop(queued.id); expect(attempts).toBe(3);
    await manager.close(); expect(attempts).toBe(6);
    expect(JSON.parse(fs.readFileSync(path.join(dir, "runs", queued.id, "pending-route-outcome.json"), "utf8"))).toMatchObject({ ledger: ledgerFile(), record: { decisionId: decision.decisionId, status: "stopped" } });
    await expect(manager.cleanup(queued.id)).rejects.toThrow("retained");
  });
  it("writes and fsyncs decision before launch, seeds child identity and appends terminal outcome once", async () => {
    const dir = root();
    const file = ledgerFile();
    const decision = await decideModelRoute(input, async () => response());
    const launch = ProcessTransport.prototype.launch;
    const calls: string[][] = [];
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function (this: ProcessTransport, request) {
      const rows = fs.readFileSync(file, "utf8").trim().split("\n").map(line => JSON.parse(line));
      expect(rows).toHaveLength(1);
      expect(rows[0]).toMatchObject({ type: "decision", decisionId: decision.decisionId, shadowChoice: cheap, pin });
      const argv = request.workerArguments;
      calls.push(argv);
      expect(argv[argv.indexOf("--model") + 1]).toBe(pin.model);
      expect(argv[argv.indexOf("--thinking") + 1]).toBe(pin.effort);
      expect(argv[argv.indexOf("--route-header") + 1]).toContain("test%2Fluna-medium/shadow-choice:");
      const sessionFile = argv[argv.indexOf("--session-file") + 1]!;
      expect(SessionManager.open(sessionFile).getSessionId()).toBe(rows[0].childSessionId);
      return launch.call(this, request);
    });
    const manager = new AgentManager(dir, { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: true }, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(dir, "runs"),
    });
    managers.push(manager);
    // Even an internal caller accidentally forwarding the shadow model cannot change the pin.
    const handle = await manager.spawn({ task: "harmless lookup", model: cheap.model, thinking: cheap.effort, transport: "process", routeDecision: decision });
    const outcome = await manager.wait(handle.id);
    expect(outcome).toMatchObject({ status: "completed", model: pin.model, thinking: pin.effort });
    expect(calls).toHaveLength(1);
    const rows = fs.readFileSync(file, "utf8").trim().split("\n").map(line => JSON.parse(line));
    expect(rows).toHaveLength(2);
    expect(rows[1]).toMatchObject({ type: "outcome", decisionId: decision.decisionId, status: "completed", admittedModel: pin.model, admittedEffort: pin.effort, tokens: { input: 1, output: 2 } });
  });
  it.each(["startup-retry", "resume"])("refuses a substitute model on routed %s when the original pin disappears", async recovery => {
    const dir = root();
    const decision = await decideModelRoute(input, async () => response());
    let preparations = 0;
    const manager = new AgentManager(dir, { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: true }, {
      workerPath: path.resolve(recovery === "startup-retry" ? "tests/fixtures/fake-worker-startup-retry.mjs" : "tests/fixtures/fake-worker.mjs"),
      runRoot: path.join(dir, "runs"),
      preparePiModel: async () => ++preparations === 1 ? pin.model : "test/sol-similar",
    }); managers.push(manager);
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    const result = await manager.run({ task: recovery === "startup-retry" ? "Recover startup" : "RESUME_AFTER_STOP", routeDecision: decision, transport: "process" });
    expect(result.status).not.toBe("completed");
    expect(result.error).toContain("MODEL_ROUTE_PIN_MISMATCH");
    expect(launch).toHaveBeenCalledTimes(1);
    expect(preparations).toBe(2);
    const rows = fs.readFileSync(ledgerFile(), "utf8").trim().split("\n").map(line => JSON.parse(line));
    expect(rows.at(-1)).toMatchObject({ decisionId: decision.decisionId, admittedModel: null });
  });
  it.each(["startup-retry", "resume"])("allows routed %s only at the original matching pin", async recovery => {
    const dir = root();
    const decision = await decideModelRoute(input, async () => response());
    const prepared = vi.fn(async () => pin.model);
    const manager = new AgentManager(dir, { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: true }, {
      workerPath: path.resolve(recovery === "startup-retry" ? "tests/fixtures/fake-worker-startup-retry.mjs" : "tests/fixtures/fake-worker.mjs"), runRoot: path.join(dir, "runs"), preparePiModel: prepared,
    }); managers.push(manager);
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    const result = await manager.run({ task: recovery === "startup-retry" ? "Recover startup" : "RESUME_AFTER_STOP", routeDecision: decision, transport: "process" });
    expect(result.status).toBe("completed");
    expect(launch).toHaveBeenCalledTimes(2);
    for (const [args] of launch.mock.calls) expect(args.workerArguments[args.workerArguments.indexOf("--model") + 1]).toBe(pin.model);
  });
  it("dispatches pin and marks record-failed when durable state cannot be written", async () => {
    const dir = root();
    fs.writeFileSync(process.env.PI_CODING_AGENT_DIR!, "not a directory");
    const decision = await decideModelRoute(input, async () => response());
    const manager = new AgentManager(dir, { ...DEFAULT_FABRIC_CONFIG.agents }, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(dir, "runs"),
    });
    managers.push(manager);
    const result = await manager.run({ task: "harmless lookup", transport: "process", routeDecision: decision });
    expect(result).toMatchObject({ status: "completed", model: pin.model, thinking: pin.effort });
    expect(decision).toMatchObject({ ...pin, reasonCode: "record-failed", shadowChoice: cheap });
  });
  it("records a queued decision and its stopped outcome even when it never dispatches", async () => {
    const dir = root();
    const manager = new AgentManager(dir, { ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent: 1 }, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(dir, "runs"),
    });
    managers.push(manager);
    const blocker = await manager.spawn({ task: "HANG", transport: "process" });
    const decision = await decideModelRoute(input, async () => response());
    const queued = await manager.spawn({ task: "harmless lookup", transport: "process", routeDecision: decision });
    expect(queued).toMatchObject({ status: "queued", model: pin.model, thinking: pin.effort });
    await manager.stop(queued.id); await manager.stop(blocker.id);
    const rows = fs.readFileSync(ledgerFile(), "utf8").trim().split("\n").map(line => JSON.parse(line));
    expect(rows).toHaveLength(2);
    expect(rows[1]).toMatchObject({ type: "outcome", decisionId: decision.decisionId, status: "stopped", admittedModel: null, admittedEffort: null });
  });
  it("never passes a partially written seed session after record failure", async () => {
    const dir = root();
    const decision = await decideModelRoute(input, async () => response());
    const write = fs.writeFileSync;
    vi.spyOn(fs, "writeFileSync").mockImplementation((file, data, options) => {
      if (typeof file === "string" && file.endsWith("route-session.jsonl")) {
        write(file, "partial"); throw new Error("disk full");
      }
      return write(file, data, options);
    });
    const prepared = prepareRouteDispatch(decision, dir, path.join(dir, "run"), "child");
    expect(prepared.sessionFile).toBeUndefined();
    expect(prepared.header).toContain("record-failed:");
    expect(decision).toMatchObject({ ...pin, reasonCode: "record-failed" });
  });
  it("supports late outcome joins after a failed record, and idempotent terminal append", async () => {
    const dir = root();
    const runDir = path.join(dir, "run"); fs.mkdirSync(runDir);
    const decision = await decideModelRoute(input, async () => response());
    const prepared = prepareRouteDispatch(decision, dir, runDir, "child-id");
    prepared.outcome({ status: "failed" }); prepared.outcome({ status: "failed" });
    const rows = fs.readFileSync(ledgerFile(), "utf8").trim().split("\n");
    expect(rows).toHaveLength(2);
    expect(JSON.parse(rows[1]!)).toMatchObject({ admittedModel: null, tokens: null });
  });
  it("rejects unresolved auto before manager inheritance", async () => {
    const manager = new AgentManager(root(), DEFAULT_FABRIC_CONFIG.agents); managers.push(manager);
    await expect(manager.spawn({ task: "test", model: "auto" })).rejects.toThrow("Unresolved model");
    expect(() => normalizeAgentRunRequest({ task: "test", model: "auto" }, { runner: "pi", timeoutMs: 1000 })).toThrow("only by agents.spawn");
  });
  it("preserves explicit routing configuration and never creates a default role pin", () => {
    expect(normalizeFabricConfig({}).agents.modelRouting).toBeUndefined();
    expect(normalizeFabricConfig({ agents: { modelRouting: { pinModel: pin.model, pinThinking: pin.effort, shadowCandidates: [cheap] } } }).agents.modelRouting)
      .toEqual({ live: false, pinModel: pin.model, pinThinking: pin.effort, shadowCandidates: [cheap] });
    expect(() => normalizeFabricConfig({ agents: { modelRouting: { shadowCandidates: [{ model: "bad", effort: "bogus" }] } } })).toThrow("Invalid agents.modelRouting");
  });
});
