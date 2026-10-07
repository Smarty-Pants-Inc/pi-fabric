import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import http from "node:http";
import { spawn } from "node:child_process";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { processStartTime } from "../src/residency/process-identity.js";
import { processAlive } from "../src/storage/scratch.js";
import { RESIDENT_RUN_RETENTION_MS, sweepResidentRuns } from "../src/residency/host.js";
import { findExecutable } from "../src/agents/transports/process-utils.js";
import { decideModelRoute } from "../src/agents/model-route.js";
import { ActorLogStore } from "../src/actors/log-store.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { fabricTurnProvenance } from "../src/fabric-provenance.js";
import { FABRIC_RUN_ROOT_PREFIX, canRemoveTerminalRun, markRunRootActive, markRunRootClosed, runTreeExitVeto, sweepTempRunRoots } from "../src/storage/retention.js";

// Terminal publication is not the native worker's close receipt. Join the exact
// owned instance before treating its emitted run tree as a quiescent fixture.
const ownedProcesses: Array<{ pid: number | undefined; closed: Promise<void> }> = [];
vi.mock("node:child_process", async importOriginal => {
  const actual = await importOriginal<typeof import("node:child_process")>();
  return { ...actual, spawn: vi.fn((...args: Parameters<typeof spawn>) => {
    const child = actual.spawn(...args);
    ownedProcesses.push({ pid: child.pid, closed: new Promise<void>(resolve => child.once("close", resolve)) });
    return child;
  }) };
});
const workerPath = path.resolve("dist/worker.js");
const roots: string[] = [];
const managers: AgentManager[] = [];
const servers: http.Server[] = [];
const temporary = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-production-retention-"));
  roots.push(root);
  return root;
};
afterEach(async () => {
  try {
    await Promise.all(managers.splice(0).map(manager => manager.close()));
    await Promise.all(ownedProcesses.splice(0).map(({ closed }) => closed));
  }
  finally {
    await Promise.all(servers.splice(0).map(server => new Promise<void>(resolve => server.close(() => resolve()))));
    vi.unstubAllEnvs();
    for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  }
});

// Real built worker AND native Pi; the only substitute is a loopback model endpoint.
// In particular native Pi loads the production principal-delivery hook and consumes
// the ingress item before it can emit a successful result. No fake-worker fixtures.
const productionRun = async (principal: boolean, retainRuns = true, nestedRunRoot?: string, routed = true) => {
  expect(fs.existsSync(workerPath), "build the production worker before running this suite").toBe(true);
  const piBinary = findExecutable("pi");
  expect(piBinary, "native Pi is required for the offline production proof").toBeTruthy();
  const root = temporary();
  const requests: Array<{ messages: Array<{ role: string; content: unknown }> }> = [];
  const server = http.createServer((request, response) => {
    let body = "";
    request.on("data", data => { body += data; });
    request.on("end", () => {
      requests.push(JSON.parse(body));
      response.writeHead(200, { "Content-Type": "text/event-stream" });
      const chunk = (delta: unknown, finish_reason: string | null = null) => response.write(`data: ${JSON.stringify({ id: "offline", object: "chat.completion.chunk", created: 1, model: "offline", choices: [{ index: 0, delta, finish_reason }] })}\n\n`);
      chunk({ role: "assistant", content: "production retention proof" });
      chunk({}, "stop");
      response.end("data: [DONE]\n\n");
    });
  });
  servers.push(server);
  await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
  const agent = path.join(root, "agent"); fs.mkdirSync(agent, { mode: 0o700 });
  fs.writeFileSync(path.join(agent, "models.json"), JSON.stringify({ providers: { "retention-offline": {
    baseUrl: `http://127.0.0.1:${(server.address() as { port: number }).port}/v1`, apiKey: "offline-only", api: "openai-completions",
    models: [{ id: "offline", name: "offline", reasoning: true, input: ["text"], contextWindow: 32000, maxTokens: 1024, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } }],
  } } }));
  fs.writeFileSync(path.join(agent, "settings.json"), JSON.stringify({ enableInstallTelemetry: false, compaction: { enabled: false } }));
  vi.stubEnv("PI_CODING_AGENT_DIR", agent); vi.stubEnv("PI_OFFLINE", "1"); vi.stubEnv("PI_FABRIC_RUN_ROOT", undefined);
  const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 30000, retainRuns, sessionExport: false, nice: 19 }, {
    workerPath, piBinary: piBinary!, fullCodeMode: false, ...(nestedRunRoot ? { runRoot: nestedRunRoot } : {}),
  });
  managers.push(manager);
  const routeDecision = await decideModelRoute({ routeClass: "bounded-lookup", protected: true,
    pin: { model: "retention-offline/offline", effort: "high" }, candidates: [], parentSessionId: "parent" }, async () => { throw new Error("excluded"); });
  const task = "Harmless production retention probe";
  // Unrouted ordinary process Pi tasks persist their native session as <run>/session.jsonl.
  const result = await manager.run({ task, ...(routed ? { routeDecision } : { model: "retention-offline/offline", thinking: "high" as const }), transport: "process", extensions: false, tools: [],
    ...(principal ? { provenance: fabricTurnProvenance({ id: "parent", name: "parent", kind: "main" }, "steer", "mesh", { id: "offline-human", binding: "voice-call" }) } : {}),
  });
  const run = path.dirname(result.logFile!); const runRoot = path.dirname(run); roots.push(runRoot);
  expect(result, JSON.stringify(result)).toMatchObject({ status: "completed", text: "production retention proof", exitCode: 0 });
  // The manager returns a published terminal result, not an OS exit receipt.
  // Copies preserve the worker PID, so even a successful archive must stay put
  // while that PID is live. Confirm exit before using this tree as an expired,
  // quiescent fixture; never weaken retention's independent live-writer fence.
  const worker = ownedProcesses.find(child => String(child.pid) === result.sessionId);
  expect(worker, "the production worker must have an owned close receipt").toBeDefined();
  await worker!.closed;
  expect(processAlive(Number(result.sessionId))).toBe(false);
  expect(requests).toHaveLength(1);
  expect(JSON.stringify(requests[0]!.messages.filter(message => message.role === "user"))).toContain(task);
  expect(fs.readdirSync(path.join(run, "deliveries"))).toEqual([]);
  expect(fs.existsSync(path.join(run, "task.txt.provenance.json"))).toBe(principal);
  expect(fs.existsSync(path.join(run, "route-session.jsonl"))).toBe(routed);
  expect(fs.existsSync(path.join(run, "session.jsonl"))).toBe(!routed);
  if (routed) {
    const rows = fs.readFileSync(path.join(agent, "fabric/model-routing.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line));
    expect(rows.map(row => row.type)).toEqual(["decision", "outcome"]);
  }
  if (retainRuns) {
    // A raw child close is not the manager's scratch-disposal receipt. Close the
    // real retaining owner before copying an expired fixture: custody is pinned
    // to this namespace and cannot be transferred by cpSync. Keep every emitted
    // persistent artifact, but never copy still-owned scratch into safe controls.
    await manager.close();
    for (const name of ["tmp", "unresolved-scratch.json", "scratch-scope-joined.json"]) {
      expect(fs.existsSync(path.join(run, name)), `owned close must dispose ${name} before copying`).toBe(false);
    }
  }
  console.info("production artifact tree", JSON.stringify({ principal, run, files: fs.readdirSync(run).sort(), deliveries: fs.readdirSync(path.join(run, "deliveries")), status: result.status }));
  return { manager, result, run, runRoot };
};

const unsafeKinds = ["unknown-delivery", "pending-delivery", "delivery-link", "sidecar-link", "directory-link", "hardlink", "pending-outcome", "live", "unresolved", "unknown-json", "unknown-directory"] as const;
type UnsafeKind = typeof unsafeKinds[number];
const makeUnsafe = (run: string, kind: UnsafeKind) => {
  const delivery = path.join(run, "deliveries");
  if (kind === "unknown-delivery") fs.writeFileSync(path.join(delivery, "unknown.txt"), "private unknown data");
  if (kind === "pending-delivery") fs.writeFileSync(path.join(delivery, "00000000-0000-0000-0000-000000000000.json"), JSON.stringify({
    message: "unconsumed ingress", delivery: "steer", provenance: fabricTurnProvenance({ id: "parent", name: "parent", kind: "main" }, "steer", "mesh", { id: "offline-human", binding: "voice-call" }),
  }));
  if (kind === "delivery-link") fs.symlinkSync(path.join(run, "task.txt"), path.join(delivery, "item.json"));
  if (kind === "sidecar-link" || kind === "hardlink") {
    const sidecar = path.join(run, "task.txt.provenance.json"); fs.unlinkSync(sidecar);
    if (kind === "sidecar-link") fs.symlinkSync(path.join(run, "task.txt"), sidecar);
    else fs.linkSync(path.join(run, "task.txt"), sidecar);
  }
  if (kind === "directory-link") { fs.rmdirSync(delivery); fs.symlinkSync(path.join(run, "handoff-session"), delivery, "dir"); }
  if (kind === "pending-outcome") fs.writeFileSync(path.join(run, "pending-route-outcome.json"), "{}");
  if (kind === "live") {
    // Replace the full identity: keeping the exited worker's birth time models PID reuse.
    const file = path.join(run, "status.json");
    fs.writeFileSync(file, JSON.stringify({ ...JSON.parse(fs.readFileSync(file, "utf8")), sessionId: String(process.pid), processStartTime: processStartTime(process.pid) }));
  }
  if (kind === "unresolved") fs.writeFileSync(path.join(run, "unresolved-worker.json"), "{}");
  if (kind === "unknown-json") fs.writeFileSync(path.join(run, "unowned.provenance.json"), "{}");
  if (kind === "unknown-directory") fs.mkdirSync(path.join(run, "other-deliveries"));
};

// Copy the entire emitted tree, including native session and delivery artifacts.
const copyRun = (source: string, destination: string) => { fs.cpSync(source, destination, { recursive: true }); return destination; };

describe("I-2 production-worker ingress retention", () => {
  it("a completed native descendant persists its process identity and releases a retained failed parent's ownership debt", async () => {
    const root = temporary();
    const runs = path.join(root, "runs");
    const parent = path.join(runs, "failed-parent");
    fs.mkdirSync(parent, { recursive: true, mode: 0o700 });
    fs.writeFileSync(path.join(parent, "status.json"), JSON.stringify({
      id: "failed-parent", actorId: "native-descendant-actor", status: "failed", transport: "process", sessionId: "2147483647",
    }), { mode: 0o600 });
    // The same explicit nested run-root API used by recursive workers, with a
    // real compiled worker + native Pi (loopback model, not a fabricated child record).
    const child = await productionRun(false, true, path.join(parent, "nested"), false);
    expect(fs.statSync(path.join(child.run, "session.jsonl")).isFile()).toBe(true);
    const status = JSON.parse(fs.readFileSync(path.join(child.run, "status.json"), "utf8"));
    expect(status).toMatchObject({ status: "completed", transport: "process", sessionId: child.result.sessionId });
    if (process.platform === "linux") expect(status.processStartTime).toMatch(/^\d+$/);
    expect(runTreeExitVeto(parent, 0, undefined, true)).toBeUndefined();
    expect(canRemoveTerminalRun(parent)).toBe(true);
    // Unsafe controls on the same emitted tree keep the parent's ownership.
    for (const kind of ["live", "unresolved", "session-link", "unknown-json"] as const) {
      const control = path.join(root, "controls", kind); copyRun(parent, control);
      const nested = path.join(control, "nested", path.basename(child.run));
      if (kind === "session-link") { fs.unlinkSync(path.join(nested, "session.jsonl")); fs.symlinkSync(path.join(nested, "task.txt"), path.join(nested, "session.jsonl")); }
      else makeUnsafe(nested, kind);
      expect(canRemoveTerminalRun(control), kind).toBe(false);
    }
    const recovered = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 }, { runRoot: runs });
    managers.push(recovered);
    // This proves ownership semantics, not a 5ms synchronous filesystem speed
    // budget. A scheduler pause mid-scan must conservatively protect this actor;
    // explicitly exercise that branch, then give the same joined tree a complete
    // scan with a deterministic clock (without changing the production budget).
    const clock = vi.spyOn(performance, "now").mockReturnValueOnce(0).mockReturnValueOnce(0).mockReturnValue(6);
    try {
      const interrupted = recovered.retentionReferences();
      expect(interrupted.has("native-descendant-actor")).toBe(true);
      expect(interrupted.has("*")).toBe(true);
      clock.mockReturnValue(0);
      const complete = recovered.retentionReferences({ refresh: true });
      expect(complete.has("native-descendant-actor")).toBe(false);
      expect(complete.has("*")).toBe(false);
    } finally { clock.mockRestore(); }
    const aged = new Date(Date.now() - RESIDENT_RUN_RETENTION_MS - 60_000);
    fs.utimesSync(parent, aged, aged);
    // Strict resident startup collection still accepts this checked-exited native tree.
    expect(sweepResidentRuns(runs, Date.now(), 10_000)).toEqual([parent]);
    expect(fs.existsSync(parent)).toBe(false);
  }, 45000);
  it.each([false, true])("collects default-root close with principal provenance=%s", async principal => {
    const { manager, runRoot } = await productionRun(principal, false);
    await manager.close();
    expect(fs.existsSync(runRoot)).toBe(false);
  }, 45000);

  it("default-root close preserves unknown, pending-delivery and symlink controls", async () => {
    const { manager, run, runRoot } = await productionRun(true, false);
    for (const kind of ["unknown-delivery", "pending-delivery", "delivery-link"] as const) makeUnsafe(copyRun(run, path.join(runRoot, kind)), kind);
    await manager.close();
    expect(fs.existsSync(runRoot)).toBe(true);
    for (const kind of ["unknown-delivery", "pending-delivery", "delivery-link"]) expect(fs.existsSync(path.join(runRoot, kind))).toBe(true);
  }, 45000);

  it.each(["closed", "orphan"])("expires the complete %s emitted tree but retains unsafe controls", async state => {
    const plain = await productionRun(false); const attributed = await productionRun(true);
    const tempRoot = temporary(); const root = path.join(tempRoot, FABRIC_RUN_ROOT_PREFIX + state);
    const base = Math.max(plain.result.finishedAt!, attributed.result.finishedAt!);
    markRunRootActive(root, base);
    copyRun(plain.run, path.join(root, "plain")); copyRun(attributed.run, path.join(root, "attributed"));
    for (const kind of unsafeKinds) makeUnsafe(copyRun(attributed.run, path.join(root, kind)), kind);
    if (state === "closed") markRunRootClosed(root, base, true);
    else fs.writeFileSync(path.join(root, ".fabric-owner.json"), JSON.stringify({ pid: 2147483647, startedAt: base, heartbeatAt: base, orphanedAt: base }));
    const sweep = (now: number) => sweepTempRunRoots({ tempRoot, now, orphanedTempRunRetentionMs: 200, oneShotRunRetentionMs: 200 });
    expect(sweep(base + 99).removedRuns).toEqual([]);
    for (const name of ["plain", "attributed", ...unsafeKinds]) expect(fs.existsSync(path.join(root, name))).toBe(true);
    sweep(base + 201);
    // Check the negative controls first so they are exercised on the pre-fix tree too.
    for (const kind of unsafeKinds) expect(fs.existsSync(path.join(root, kind)), kind).toBe(true);
    for (const name of ["plain", "attributed"]) expect(fs.existsSync(path.join(root, name)), name).toBe(false);
  }, 90000);

  it("expires actor archives with copied nested production runs but retains unsafe descendants and latest", async () => {
    const plain = await productionRun(false); const attributed = await productionRun(true);
    const root = temporary(); const actor = { sessionFile: path.join(root, "actor.jsonl"), lastRunId: "latest" };
    const base = Math.max(plain.result.finishedAt!, attributed.result.finishedAt!);
    const store = new ActorLogStore({ maxEventBytes: DEFAULT_FABRIC_CONFIG.mesh.maxEventBytes }, DEFAULT_FABRIC_CONFIG.mesh, { actorRunArchiveMs: 60_000 });
    fs.mkdirSync(path.join(root, "sources"), { mode: 0o700 });
    for (const kind of ["successful", "latest", ...unsafeKinds] as const) {
      const source = copyRun(plain.run, path.join(root, "sources", kind));
      // cpSync's destination creation must not inherit a group-writable umask.
      fs.chmodSync(source, 0o700);
      // Each renamed top-level fixture represents a distinct actor run. Keep its
      // execution receipt bound to that run, just as the production manager does;
      // nested copies retain their original identities and all unsafe controls.
      const receiptFile = path.join(source, "route-dispatch-receipt.json");
      const receipt = JSON.parse(fs.readFileSync(receiptFile, "utf8"));
      expect(receipt.runId).toBe(plain.result.id);
      fs.writeFileSync(receiptFile, JSON.stringify({ ...receipt, runId: kind }));
      copyRun(plain.run, path.join(source, "nested", "plain"));
      const nested = copyRun(attributed.run, path.join(source, "nested", "attributed"));
      if (kind !== "successful" && kind !== "latest") makeUnsafe(nested, kind);
      // Exercise production's selective top-level archive and recursive nested copy.
      await store.retainRun(actor, kind, source);
      const archive = path.join(root, "runs", kind);
      expect(fs.readdirSync(path.join(archive, "nested", "attributed")).sort()).toEqual(fs.readdirSync(nested).sort());
      expect(fs.existsSync(path.join(archive, "deliveries"))).toBe(false);
      expect(JSON.parse(fs.readFileSync(path.join(archive, "route-dispatch-receipt.json"), "utf8"))).toEqual({ ...receipt, runId: kind });
      // cpSync makes independent copies of hardlinked source files. Reintroduce
      // an actual multiply-linked file in the archive to test the collector veto.
      if (kind === "hardlink") makeUnsafe(path.join(archive, "nested", "attributed"), kind);
    }
    store.pruneRuns(actor, base + 99);
    expect(store.retainedRunIds(actor)).toContain("successful");
    store.pruneRuns(actor, base + 60_001);
    for (const kind of ["latest", ...unsafeKinds]) expect(store.retainedRunIds(actor), kind).toContain(kind);
    expect(store.retainedRunIds(actor)).not.toContain("successful");
  }, 90000);
});
