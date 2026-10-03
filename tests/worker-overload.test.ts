import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import http from "node:http";
import { spawn } from "node:child_process";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import type { AgentRunResult } from "../src/agents/types.js";

// Capture the exact owned native instances, including taskkill helpers. A
// manager's terminal result/liveness latch is not proof its handles closed.
const ownedProcesses: Array<{ pid: number | undefined; closed: Promise<void>; didClose: () => boolean }> = [];
vi.mock("node:child_process", async importOriginal => {
  const actual = await importOriginal<typeof import("node:child_process")>();
  return { ...actual, spawn: vi.fn((...args: Parameters<typeof spawn>) => {
    const child = actual.spawn(...args);
    let didClose = false;
    const closed = new Promise<void>(resolve => child.once("close", () => { didClose = true; resolve(); }));
    ownedProcesses.push({ pid: child.pid, closed, didClose: () => didClose });
    return child;
  }) };
});

const roots: string[] = [];
const managers: AgentManager[] = [];
const servers: http.Server[] = [];
const SCALE = 0.05;
const until = async (ready: () => boolean): Promise<void> => {
  const deadline = Date.now() + 30_000;
  while (!ready()) {
    if (Date.now() >= deadline) throw new Error("Native Pi event did not arrive");
    await new Promise(resolve => setTimeout(resolve, 20));
  }
};
const entries = (file: string): Array<Record<string, any>> => fs.existsSync(file)
  ? fs.readFileSync(file, "utf8").split("\n").filter(Boolean).map(line => JSON.parse(line)) : [];
const events = (result: { logFile?: string }) => entries(result.logFile!);
// Use the test's own timeout/abort, not a shorter startup+retry polling budget.
// Watch the run directory so creating/replacing events.jsonl is observed too.
const waitForEvent = async (manager: Pick<AgentManager, "wait">, id: string, file: string,
  matches: (event: Record<string, any>) => boolean, signal: AbortSignal): Promise<void> => {
  let active = true;
  let check!: () => void;
  const watcher = fs.watch(path.dirname(file), () => check());
  const closed = new Promise<void>(resolve => watcher.once("close", resolve));
  let abort!: () => void;
  try {
    await new Promise<void>((resolve, reject) => {
      check = () => {
        if (!active) return;
        try {
          // A filesystem notification can race the final newline of an append.
          const complete = fs.existsSync(file) ? fs.readFileSync(file, "utf8").split("\n").slice(0, -1) : [];
          if (complete.filter(Boolean).map(line => JSON.parse(line)).some(matches)) resolve();
        } catch (error) { reject(error); }
      };
      abort = () => reject(signal.reason ?? new Error("Native Pi event wait aborted"));
      watcher.once("error", reject);
      signal.addEventListener("abort", abort, { once: true });
      // A terminal run cannot ever produce the required backoff evidence.
      void manager.wait(id).then(result => {
        if (!active) return;
        check();
        reject(new Error(`Native Pi exited before required backoff event: ${explain(result)}`));
      }, reject);
      check(); // Also cover an event written before the watcher was installed.
      if (signal.aborted) abort();
    });
  } finally {
    active = false;
    signal.removeEventListener("abort", abort);
    watcher.close();
    await closed;
  }
};
const explain = (result: AgentRunResult) => JSON.stringify(result) + "\n" + fs.readFileSync(result.logFile!, "utf8").slice(-16_000);
const evidence = (name: string, result: AgentRunResult) => {
  const directory = process.env.FABRIC_OVERLOAD_TEST_EVIDENCE_DIR;
  if (!directory) return;
  fs.mkdirSync(directory, { recursive: true });
  fs.writeFileSync(path.join(directory, name + "-result.json"), JSON.stringify(result, null, 2));
  const run = path.dirname(result.logFile!);
  for (const file of ["events.jsonl", "lifecycle.jsonl", "session.jsonl", "status.json"]) {
    if (fs.existsSync(path.join(run, file))) fs.copyFileSync(path.join(run, file), path.join(directory, name + "-" + file));
  }
};
afterEach(async () => {
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  await Promise.all(ownedProcesses.splice(0).map(({ closed }) => closed));
  await Promise.all(servers.splice(0).map(server => new Promise<void>(resolve => server.close(() => resolve()))));
  vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
});
const setup = async (mode: "burst" | "forever" | "resume" | "400" | "429" | "disconnect" | "controls" | "queue-modes", retry?: Record<string, unknown>) => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-overload-")); roots.push(dir);
  let firstRequestAt = 0;
  let releaseOverload!: () => void;
  const overloadReady = new Promise<void>(resolve => { releaseOverload = resolve; });
  const requests: Array<Record<string, any>> = [];
  const server = http.createServer((request, response) => {
    let body = "";
    request.on("data", chunk => { body += chunk; });
    request.on("end", async () => {
      requests.push(JSON.parse(body)); firstRequestAt ||= Date.now();
      // Hold the first response until public queue-mode controls have reached native Pi.
      if (mode === "queue-modes" && requests.length === 1) await overloadReady;
      if (mode === "disconnect" && requests.length === 1) { response.destroy(); return; }
      const overloaded = mode === "forever" || (mode === "queue-modes" && requests.length <= 5) ||
        (mode === "burst" && Date.now() - firstRequestAt < 90_000 * SCALE) ||
        ((mode === "resume" || mode === "controls") && requests.length === 1);
      if (overloaded || mode === "400" || (mode === "429" && requests.length === 1)) {
        response.writeHead(mode === "400" ? 400 : mode === "429" ? 429 : 503, { "Content-Type": "application/json" });
        response.end(JSON.stringify({ error: { message: mode === "400" ? "invalid_request_error: invalid input" : "server_is_overloaded", type: mode === "400" ? "invalid_request_error" : "server_is_overloaded" } }));
        return;
      }
      response.writeHead(200, { "Content-Type": "text/event-stream" });
      const chunk = (delta: unknown, finish_reason: string | null = null) => response.write(`data: ${JSON.stringify({
        id: "offline", object: "chat.completion.chunk", created: 1, model: "offline",
        choices: [{ index: 0, delta, finish_reason }],
      })}\n\n`);
      const finish = () => {
        chunk({ role: "assistant", content: "survived overload" }); chunk({}, "stop");
        response.end("data: [DONE]\n\n");
      };
      // Keep the resumed turn streaming while native steering/follow-up arrive.
      if (mode === "controls" && requests.length === 2) setTimeout(finish, 700);
      else finish();
    });
  });
  servers.push(server);
  await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
  const port = (server.address() as { port: number }).port;
  const agentDir = path.join(dir, "agent"); fs.mkdirSync(agentDir);
  fs.writeFileSync(path.join(agentDir, "models.json"), JSON.stringify({ providers: {
    "overload-test": { baseUrl: `http://127.0.0.1:${port}/v1`, apiKey: "offline-only", api: "openai-completions", models: [{
      id: "offline", name: "offline", reasoning: false, input: ["text"], contextWindow: 128000, maxTokens: 1024,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
    }] },
  } }));
  const settingsFile = path.join(agentDir, "settings.json");
  const settings = JSON.stringify({ enableInstallTelemetry: false, compaction: { enabled: false }, ...(retry ? { retry } : {}) });
  fs.writeFileSync(settingsFile, settings);
  vi.stubEnv("PI_CODING_AGENT_DIR", agentDir);
  vi.stubEnv("PI_OFFLINE", "1");
  vi.stubEnv("PI_FABRIC_TEST_RECOVERY_TIME_SCALE", String(SCALE));
  const manager = new AgentManager(dir, { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 120_000 }, {
    workerPath: path.resolve(process.env.FABRIC_OVERLOAD_TEST_WORKER ?? "src/worker.ts"),
    piBinary: path.resolve(process.env.FABRIC_OVERLOAD_TEST_PI_BINARY ?? "node_modules/@earendil-works/pi-coding-agent/dist/cli.js"),
    runRoot: path.join(dir, "runs"),
  });
  managers.push(manager);
  const spawn = () => manager.spawn({ task: "Finish ORIGINAL_TASK, preserving completed work.", model: "overload-test/offline", tools: [], extensions: false, transport: "process" });
  return { dir, manager, requests, settingsFile, settings, spawn, releaseOverload };
};

describe("native backoff evidence wait", () => {
  const fixture = () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-overload-event-")); roots.push(root);
    const file = path.join(root, "events.jsonl");
    const manager = { wait: vi.fn(() => new Promise<AgentRunResult>(() => {})) };
    const matches = (event: Record<string, any>) => event.type === "auto_retry_start" && event.delayMs >= 4000;
    return { file, manager, matches, abort: new AbortController() };
  };
  it("observes already-written retry evidence", async () => {
    const s = fixture();
    fs.writeFileSync(s.file, JSON.stringify({ type: "auto_retry_start", delayMs: 4000 }) + "\n");
    await waitForEvent(s.manager, "test", s.file, s.matches, s.abort.signal);
  });
  it("keeps waiting beyond the old 30-second budget and requires a complete matching event", async () => {
    const s = fixture();
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    const waiting = waitForEvent(s.manager, "test", s.file, s.matches, s.abort.signal);
    let arrived = false;
    void waiting.then(() => { arrived = true; });
    try {
      fs.writeFileSync(s.file, JSON.stringify({ type: "auto_retry_start", delayMs: 2000 }) + "\n");
      await vi.advanceTimersByTimeAsync(30_001);
      expect(arrived).toBe(false);
      // The watcher may see a partial append; only the newline commits it.
      fs.appendFileSync(s.file, JSON.stringify({ type: "auto_retry_start", delayMs: 4000 }));
      await vi.advanceTimersByTimeAsync(1000);
      expect(arrived).toBe(false);
      fs.appendFileSync(s.file, "\n");
      await waiting;
      expect(arrived).toBe(true);
    } finally { vi.useRealTimers(); s.abort.abort(); await waiting.catch(() => {}); }
  });
  it("disposes its watcher when the test timeout aborts the wait", async () => {
    const s = fixture();
    const waiting = waitForEvent(s.manager, "test", s.file, s.matches, s.abort.signal);
    const assertion = expect(waiting).rejects.toThrow("test timed out");
    s.abort.abort(new Error("test timed out"));
    await assertion;
    // afterEach can now delete the watched root even on Windows.
  });
});

describe("real Pi process provider recovery (offline localhost)", () => {
  it("survives a 90-second scaled overload burst in one native session, with no kill", async () => {
    const s = await setup("burst"); const handle = await s.spawn();
    const result = await s.manager.wait(handle.id); evidence("burst", result);
    expect(result, explain(result)).toMatchObject({ status: "completed", text: "survived overload" });
    const log = events(result);
    expect(log.filter(event => event.type === "auto_retry_start").map(event => event.delayMs)).toEqual([250, 500, 1000, 2000, 4000]);
    expect(log.some(event => event.type === "fabric_recovery_error" || event.type === "fabric_provider_resume")).toBe(false);
    expect(result.runnerSessionIds).toHaveLength(1);
    const journal = entries(path.join(path.dirname(result.logFile!), "session.jsonl"));
    expect(journal.filter(entry => entry.type === "session")).toHaveLength(1);
    expect(journal[0]!.id).toBe(result.runnerSessionId);
    expect(fs.readFileSync(s.settingsFile, "utf8")).toBe(s.settings);
  }, 120_000);

  it.each(["steering", "follow-up", "both"] as const)("preserves task-local retry defaults after %s queue-mode setters in a native process", async controls => {
    const s = await setup("queue-modes"); const handle = await s.spawn();
    try {
      await until(() => s.requests.length === 1);
      if (controls !== "follow-up") expect(s.manager.setSteeringMode(handle.id, "all").queued).toBe(true);
      if (controls !== "steering") expect(s.manager.setFollowUpMode(handle.id, "all").queued).toBe(true);
      // Observe the native setters' persisted modes, not just enqueueing the RPC.
      await until(() => {
        const saved = JSON.parse(fs.readFileSync(s.settingsFile, "utf8"));
        return (controls === "follow-up" || saved.steeringMode === "all") &&
          (controls === "steering" || saved.followUpMode === "all");
      });
    } finally { s.releaseOverload(); }
    const result = await s.manager.wait(handle.id); evidence("queue-modes-" + controls, result);
    const saved = JSON.parse(fs.readFileSync(s.settingsFile, "utf8"));
    const directory = process.env.FABRIC_OVERLOAD_TEST_EVIDENCE_DIR;
    if (directory) fs.copyFileSync(s.settingsFile, path.join(directory, "queue-modes-" + controls + "-settings.json"));
    expect(saved).not.toHaveProperty("retry");
    expect(saved).toEqual({ ...JSON.parse(s.settings),
      ...(controls !== "follow-up" ? { steeringMode: "all" } : {}),
      ...(controls !== "steering" ? { followUpMode: "all" } : {}),
    });
    expect(result, explain(result)).toMatchObject({ status: "completed", text: "survived overload" });
    const log = events(result);
    expect(log.filter(event => event.type === "auto_retry_start").map(event => event.delayMs)).toEqual([250, 500, 1000, 2000, 4000]);
    expect(log.some(event => event.type === "fabric_provider_resume" || event.type === "fabric_recovery_error")).toBe(false);
    expect(result.runnerSessionIds).toHaveLength(1);
    const journal = entries(path.join(path.dirname(result.logFile!), "session.jsonl"));
    expect(journal.filter(entry => entry.type === "session")).toHaveLength(1);
  }, 120_000);

  it("keeps explicit user retries after both public queue-mode setters in a native process", async () => {
    const retry = { maxRetries: 6, baseDelayMs: 11, maxAgentDelayMs: 176 };
    const s = await setup("queue-modes", retry); const handle = await s.spawn();
    try {
      await until(() => s.requests.length === 1);
      expect(s.manager.setSteeringMode(handle.id, "all").queued).toBe(true);
      expect(s.manager.setFollowUpMode(handle.id, "all").queued).toBe(true);
      await until(() => {
        const saved = JSON.parse(fs.readFileSync(s.settingsFile, "utf8"));
        return saved.steeringMode === "all" && saved.followUpMode === "all";
      });
    } finally { s.releaseOverload(); }
    const result = await s.manager.wait(handle.id); evidence("queue-modes-explicit", result);
    expect(JSON.parse(fs.readFileSync(s.settingsFile, "utf8"))).toEqual({ ...JSON.parse(s.settings), steeringMode: "all", followUpMode: "all", retry });
    expect(result, explain(result)).toMatchObject({ status: "completed", text: "survived overload" });
    expect(events(result).filter(event => event.type === "auto_retry_start").map(event => event.delayMs)).toEqual([11, 22, 44, 88, 176]);
    expect(events(result).some(event => event.type === "fabric_provider_resume")).toBe(false);
    expect(result.runnerSessionIds).toHaveLength(1);
  }, 120_000);

  it("fails at the same ten-minute recovery bound across relaunches, retaining the session", async () => {
    const s = await setup("forever"); const handle = await s.spawn();
    const result = await s.manager.wait(handle.id); evidence("bound", result);
    expect(result, explain(result)).toMatchObject({ status: "failed" });
    expect(result.error).toContain("10-minute bound");
    expect(result.runnerSessionIds).toHaveLength(1);
    expect(fs.existsSync(path.join(path.dirname(result.logFile!), "session.jsonl"))).toBe(true);
    expect(events(result).filter(event => event.type === "fabric_provider_resume" && event.phase === "starting").length).toBeLessThanOrEqual(3);
    expect(events(result).filter(event => event.type === "auto_retry_start").some(event => event.delayMs === 8000)).toBe(true);
  }, 120_000);

  it("resumes an exhausted retryable Pi child on the exact same file and records the lifecycle", async () => {
    const s = await setup("resume", { maxRetries: 0 }); const handle = await s.spawn();
    const result = await s.manager.wait(handle.id); evidence("resume", result);
    expect(result, explain(result)).toMatchObject({ status: "completed", text: "survived overload" });
    expect(s.requests).toHaveLength(2);
    expect(JSON.stringify(s.requests[1])).toContain("ORIGINAL_TASK");
    expect(JSON.stringify(s.requests[1])).toContain("Continue the task");
    expect(result.runnerSessionIds).toHaveLength(1);
    expect(events(result).filter(event => event.type === "fabric_provider_resume")).toMatchObject([
      { attempt: 1, delayMs: 1500, phase: "scheduled" }, { attempt: 1, delayMs: 1500, phase: "starting" },
    ]);
    expect(entries(path.join(path.dirname(result.logFile!), "lifecycle.jsonl")).some(event => event.event === "run.resumed")).toBe(true);
    expect(fs.readFileSync(s.settingsFile, "utf8")).toBe(s.settings);
  }, 120_000);

  it("delivers steer and followUp queued after scheduled recovery exactly once to the resumed session", async () => {
    const s = await setup("controls", { maxRetries: 0 }); const handle = await s.spawn();
    const logFile = path.join(s.dir, "runs", handle.id, "events.jsonl");
    await until(() => entries(logFile).some(event => event.type === "fabric_provider_resume" && event.phase === "scheduled"));
    expect(s.manager.steer(handle.id, "RECOVERY_STEER_ONLY_ONCE").queued).toBe(true);
    expect(s.manager.followUp(handle.id, "RECOVERY_FOLLOW_UP_ONLY_ONCE").queued).toBe(true);
    // More than two poll ticks while the old child's real stdin is ended.
    await new Promise(resolve => setTimeout(resolve, 650));
    expect(entries(logFile).some(event => event.type === "fabric_provider_resume" && event.phase === "starting")).toBe(false);
    const result = await s.manager.wait(handle.id); evidence("recovery-controls", result);
    expect(result, explain(result)).toMatchObject({ status: "completed" });
    expect(result.runnerSessionIds).toHaveLength(1);
    const journal = entries(path.join(path.dirname(result.logFile!), "session.jsonl"));
    expect(journal.filter(entry => entry.type === "session")).toHaveLength(1);
    const users = journal.filter(entry => entry.type === "message" && entry.message?.role === "user")
      .map(entry => entry.message.content.map((part: any) => part.text ?? "").join(""));
    for (const text of ["RECOVERY_STEER_ONLY_ONCE", "RECOVERY_FOLLOW_UP_ONLY_ONCE"]) {
      expect(users.filter(message => message === text), explain(result)).toHaveLength(1);
      expect(s.requests.some(request => JSON.stringify(request).includes(text))).toBe(true);
    }
    expect(events(result).filter(event => event.type === "fabric_provider_resume" && event.phase === "starting")).toHaveLength(1);
  }, 120_000);

  it.each(["429", "disconnect"] as const)("resumes a retryable %s with Pi's classification family", async mode => {
    const s = await setup(mode, { maxRetries: 0 }); const handle = await s.spawn();
    const result = await s.manager.wait(handle.id); evidence(mode, result);
    expect(result, explain(result)).toMatchObject({ status: "completed", text: "survived overload" });
    expect(s.requests).toHaveLength(2);
    expect(result.runnerSessionIds).toHaveLength(1);
    expect(events(result).filter(event => event.type === "fabric_provider_resume" && event.phase === "starting")).toHaveLength(1);
  }, 120_000);

  it("never exceeds three same-session resumes even when the user's Pi retries are disabled", async () => {
    const s = await setup("forever", { maxRetries: 0 }); const handle = await s.spawn();
    const result = await s.manager.wait(handle.id); evidence("resume-cap", result);
    expect(result, explain(result)).toMatchObject({ status: "failed" });
    expect(result.error).toContain("exhausted 3 same-session resumes");
    expect(result.runnerSessionIds).toHaveLength(1);
    expect(s.requests).toHaveLength(4);
    expect(events(result).filter(event => event.type === "fabric_provider_resume" && event.phase === "starting").map(event => event.delayMs)).toEqual([1500, 3000, 6000]);
  }, 120_000);

  it.for(["native retry", "transport resume"])("stops promptly during %s backoff, with no later resume", { timeout: 120_000 }, async (backoff, { signal }) => {
    const s = await setup("forever", backoff === "transport resume" ? { maxRetries: 0 } : undefined);
    const handle = await s.spawn();
    const logFile = path.join(s.dir, "runs", handle.id, "events.jsonl");
    await waitForEvent(s.manager, handle.id, logFile, event => backoff === "native retry"
      ? event.type === "auto_retry_start" && event.delayMs >= 4000
      : event.type === "fabric_provider_resume" && event.phase === "scheduled", signal);
    const worker = ownedProcesses.find(child => String(child.pid) === handle.sessionId)!;
    const before = Date.now(); await s.manager.stop(handle.id);
    expect(worker.didClose(), "stop must join the owned native worker close").toBe(true);
    const result = await s.manager.wait(handle.id); evidence(backoff.replace(" ", "-"), result);
    expect(result, explain(result)).toMatchObject({ status: "stopped" });
    expect(Date.now() - before).toBeLessThan(2000);
    expect(events(result).some(event => event.type === "fabric_provider_resume" && event.phase === "starting")).toBe(false);
    expect(result.runnerSessionIds).toHaveLength(1);
  });

  it("never resumes a nonretryable 400 response", async () => {
    const s = await setup("400"); const handle = await s.spawn();
    const result = await s.manager.wait(handle.id); evidence("400", result);
    expect(result, explain(result)).toMatchObject({ status: "failed" });
    expect(s.requests).toHaveLength(1);
    expect(result.error).toContain("400");
    expect(events(result).some(event => event.type === "fabric_provider_resume" || event.type === "auto_retry_start")).toBe(false);
  }, 120_000);
});
