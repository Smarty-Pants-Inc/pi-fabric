import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import http from "node:http";
import { spawnSync } from "node:child_process";
import { describe, expect, it, vi } from "vitest";
import { SessionManager } from "@earendil-works/pi-coding-agent";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { findExecutable } from "../src/agents/transports/process-utils.js";
import { decideModelRoute } from "../src/agents/model-route.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { FABRIC_RUN_ROOT_PREFIX, markRunRootActive, markRunRootClosed, sweepTempRunRoots } from "../src/storage/retention.js";

const decision = (model = "test/sol") => decideModelRoute({ routeClass: "bounded-lookup", protected: true,
  pin: { model, effort: "high" }, candidates: [], parentSessionId: "parent" }, async () => { throw new Error("excluded"); });
const git = (cwd: string, ...args: string[]) => {
  const result = spawnSync("git", args, { cwd, encoding: "utf8" });
  if (result.status !== 0) throw new Error(result.stderr);
  return result.stdout;
};
const repository = (root: string) => {
  const repo = path.join(root, "repo"); fs.mkdirSync(repo);
  git(repo, "init", "-q"); git(repo, "config", "user.name", "offline-test"); git(repo, "config", "user.email", "offline@example.invalid");
  fs.writeFileSync(path.join(repo, "tracked.txt"), "committed-worktree");
  git(repo, "add", "."); git(repo, "commit", "-qm", "fixture");
  fs.writeFileSync(path.join(repo, "tracked.txt"), "dirty-parent");
  return repo;
};

describe("R3 routed session boundaries", () => {
  it("I-1 refuses a failed final-cwd rebind before launch and settles the same decision", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-route-rebind-failure-")); const repo = repository(root);
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(root, "agent"));
    const manager = new AgentManager(repo, { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: true, nice: 19 }, { workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs") });
    const launch = vi.spyOn(ProcessTransport.prototype, "launch"); const rename = fs.renameSync;
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => { if (String(to).endsWith("route-session.jsonl")) throw new Error("rebind disk failure"); return rename(from, to); });
    try {
      const route = await decision();
      await expect(manager.spawn({ task: "must not launch", worktree: true, routeDecision: route, transport: "process" })).rejects.toThrow("rebind disk failure");
      expect(launch).not.toHaveBeenCalled();
      const rows = fs.readFileSync(path.join(root, "agent/fabric/model-routing.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line));
      expect(rows).toHaveLength(2); expect(rows[1]).toMatchObject({ type: "outcome", decisionId: route.decisionId, childSessionId: rows[0].childSessionId, status: "failed", admittedModel: null });
      expect(git(repo, "worktree", "list", "--porcelain").match(/^worktree /gm)).toHaveLength(1);
    } finally { await manager.close(); vi.restoreAllMocks(); vi.unstubAllEnvs(); fs.rmSync(root, { recursive: true, force: true }); }
  });
  it.each(["immediate", "queued", "startup-retry", "resume"])("I-1 binds every %s launch to final worktree with the same child ID", async mode => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-route-boundary-"));
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(root, "agent"));
    const repo = repository(root);
    const manager = new AgentManager(repo, { ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent: 1, retainRuns: true, nice: 19 }, {
      workerPath: path.resolve(mode === "startup-retry" ? "tests/fixtures/fake-worker-startup-retry.mjs" : "tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
    });
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    let child: string | undefined;
    try {
      const blocker = mode === "queued" ? await manager.spawn({ task: "HANG", transport: "process" }) : undefined;
      const handle = await manager.spawn({ task: mode === "startup-retry" ? "Recover startup" : mode === "resume" ? "RESUME_AFTER_STOP" : "lookup", worktree: true, routeDecision: await decision(), transport: "process" }); child = handle.id;
      if (blocker) { expect(handle.status).toBe("queued"); await manager.stop(blocker.id); }
      const result = await manager.wait(handle.id); expect(result.status).toBe("completed");
      const launches = launch.mock.calls.map(([request]) => request).filter(request => request.id === handle.id);
      expect(launches).toHaveLength(mode === "startup-retry" || mode === "resume" ? 2 : 1);
      for (const request of launches) {
        const sessionFile = request.workerArguments[request.workerArguments.indexOf("--session-file") + 1]!;
        const session = SessionManager.open(sessionFile);
        expect(session.getSessionId()).toBe(handle.id);
        expect(session.getCwd()).toBe(result.cwd);
        expect(request.cwd).toBe(result.cwd);
        expect(fs.readFileSync(path.join(session.getCwd(), "tracked.txt"), "utf8")).toBe("committed-worktree");
      }
      expect(fs.readFileSync(path.join(repo, "tracked.txt"), "utf8")).toBe("dirty-parent");
    } finally { await manager.close(); if (child) await manager.cleanup(child, true).catch(() => {}); vi.restoreAllMocks(); vi.unstubAllEnvs(); fs.rmSync(root, { recursive: true, force: true }); }
  }, 30000);

  it.skipIf(!findExecutable("pi") || !fs.existsSync("dist/worker.js"))("I-1 real Pi native bash writes marker only in the routed worktree", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-route-native-")); const repo = repository(root);
    const requests: Array<Record<string, any>> = [];
    const server = http.createServer((request, response) => {
      let body = ""; request.on("data", data => { body += data; }); request.on("end", () => {
        const payload = JSON.parse(body); requests.push(payload);
        const tool = payload.messages.find((message: { role: string }) => message.role === "tool");
        response.writeHead(200, { "Content-Type": "text/event-stream" });
        const chunk = (delta: unknown, finish_reason: string | null = null) => response.write(`data: ${JSON.stringify({ id: "offline", object: "chat.completion.chunk", created: 1, model: "offline", choices: [{ index: 0, delta, finish_reason }] })}\n\n`);
        if (!tool) {
          chunk({ role: "assistant", tool_calls: [{ index: 0, id: "cwd-probe", type: "function", function: { name: "bash", arguments: JSON.stringify({ command: "cat tracked.txt; printf '\\n'; pwd; printf 'native-marker' > route-marker.txt" }) } }] }); chunk({}, "tool_calls");
        } else { chunk({ role: "assistant", content: String(tool.content) }); chunk({}, "stop"); }
        response.end("data: [DONE]\n\n");
      });
    });
    await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
    const agent = path.join(root, "agent"); fs.mkdirSync(agent);
    fs.writeFileSync(path.join(agent, "models.json"), JSON.stringify({ providers: { "route-offline": { baseUrl: `http://127.0.0.1:${(server.address() as { port: number }).port}/v1`, apiKey: "offline-only", api: "openai-completions", models: [{ id: "offline", name: "offline", reasoning: true, input: ["text"], contextWindow: 32000, maxTokens: 1024, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } }] } } }));
    fs.writeFileSync(path.join(agent, "settings.json"), JSON.stringify({ enableInstallTelemetry: false, compaction: { enabled: false } }));
    vi.stubEnv("PI_CODING_AGENT_DIR", agent); vi.stubEnv("PI_OFFLINE", "1");
    const manager = new AgentManager(repo, { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 120000, retainRuns: true, sessionExport: false, nice: 19 }, { workerPath: path.resolve("dist/worker.js"), piBinary: findExecutable("pi")!, runRoot: path.join(root, "runs"), fullCodeMode: false });
    let child: string | undefined;
    try {
      const handle = await manager.spawn({ task: "Probe checkout with bash", routeDecision: await decision("route-offline/offline"), transport: "process", worktree: true, extensions: false, tools: ["bash"] }); child = handle.id;
      const result = await manager.wait(handle.id);
      expect(result, JSON.stringify(result) + (result.logFile ? fs.readFileSync(result.logFile, "utf8").slice(-8000) : "")).toMatchObject({ status: "completed" });
      expect(requests).toHaveLength(2);
      expect(result.text).toContain("committed-worktree"); expect(result.text).toContain(result.cwd);
      expect(fs.readFileSync(path.join(result.cwd, "route-marker.txt"), "utf8")).toBe("native-marker");
      expect(fs.existsSync(path.join(repo, "route-marker.txt"))).toBe(false);
      expect(fs.readFileSync(path.join(repo, "tracked.txt"), "utf8")).toBe("dirty-parent");
      console.info("native Pi boundary proof", JSON.stringify({ childId: result.id, actualToolOutput: result.text,
        worktree: result.worktree, marker: path.join(result.cwd, "route-marker.txt"),
        markerContent: fs.readFileSync(path.join(result.cwd, "route-marker.txt"), "utf8"),
        parentMarkerExists: fs.existsSync(path.join(repo, "route-marker.txt")), parentContent: fs.readFileSync(path.join(repo, "tracked.txt"), "utf8") }));
    } finally { await manager.close(); if (child) await manager.cleanup(child, true).catch(() => {}); await new Promise<void>(resolve => server.close(() => resolve())); vi.unstubAllEnvs(); fs.rmSync(root, { recursive: true, force: true }); }
  }, 150000);

  it("I-2 collects a successful routed session on default-root close", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-route-close-"));
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(root, "agent")); vi.stubEnv("PI_FABRIC_RUN_ROOT", undefined);
    const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: false, nice: 19 }, { workerPath: path.resolve("tests/fixtures/fake-worker.mjs") });
    let runRoot: string | undefined;
    try {
      const result = await manager.run({ task: "lookup", routeDecision: await decision(), transport: "process" });
      expect(result.status).toBe("completed"); runRoot = path.dirname(path.dirname(result.logFile!));
      expect(fs.existsSync(path.join(runRoot, result.id, "route-session.jsonl"))).toBe(true);
      const rows = fs.readFileSync(path.join(root, "agent/fabric/model-routing.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line));
      expect(rows.map(row => row.type)).toEqual(["decision", "outcome"]);
      await manager.close(); expect(fs.existsSync(runRoot)).toBe(false);
    } finally { await manager.close(); vi.unstubAllEnvs(); if (runRoot) fs.rmSync(runRoot, { recursive: true, force: true }); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it.each(["closed", "orphan"])("I-2 collects expired %s route sessions but preserves pending/live/link/unknown/unresolved controls", state => {
    const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-route-retention-"));
    const runRoot = path.join(tempRoot, FABRIC_RUN_ROOT_PREFIX + state);
    try {
      markRunRootActive(runRoot, 1);
      for (const kind of ["successful", "pending", "live", "link", "unknown", "unresolved"]) {
        const run = path.join(runRoot, kind); fs.mkdirSync(run);
        fs.writeFileSync(path.join(run, "status.json"), JSON.stringify({ status: "completed", finishedAt: 1, transport: "process", ...(kind === "live" ? { sessionId: String(process.pid) } : {}) }));
        if (kind === "link") fs.symlinkSync(path.join(run, "status.json"), path.join(run, "route-session.jsonl"));
        else fs.writeFileSync(path.join(run, "route-session.jsonl"), "private native session");
        if (kind === "pending") fs.writeFileSync(path.join(run, "pending-route-outcome.json"), "{}");
        if (kind === "unknown") fs.writeFileSync(path.join(run, "route-session.jsonl.bak"), "private unknown");
        if (kind === "unresolved") fs.writeFileSync(path.join(run, "unresolved-worker.json"), "{}");
      }
      if (state === "closed") markRunRootClosed(runRoot, 1, true);
      else fs.writeFileSync(path.join(runRoot, ".fabric-owner.json"), JSON.stringify({ pid: 2147483647, startedAt: 1, heartbeatAt: 1, orphanedAt: 1 }));
      const sweep = (now: number) => sweepTempRunRoots({ tempRoot, now, orphanedTempRunRetentionMs: 100, oneShotRunRetentionMs: 200 });
      expect(sweep(99).removedRuns).toEqual([]); expect(fs.existsSync(path.join(runRoot, "successful"))).toBe(true);
      sweep(202);
      expect(fs.existsSync(path.join(runRoot, "successful"))).toBe(false);
      for (const kind of ["pending", "live", "link", "unknown", "unresolved"]) expect(fs.existsSync(path.join(runRoot, kind))).toBe(true);
    } finally { fs.rmSync(tempRoot, { recursive: true, force: true }); }
  });
});
