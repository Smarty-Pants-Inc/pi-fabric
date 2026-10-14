import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import type { AgentTransportLaunch } from "../src/agents/types.js";
import { ResidentHost } from "../src/residency/host.js";
import { residentIdleExitPath } from "../src/residency/idle-exit.js";
import { residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import { residentSleepingPath } from "../src/residency/wake.js";

const sleep = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
const roots: string[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

async function fixture() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "resident-retained-custody-"));
  roots.push(root);
  const meshRoot = path.join(root, "mesh");
  const config: ResidentHostConfig = { format: 1, rootId: "session:retained-custody", sessionId: "retained-custody",
    cwd: root, projectRoot: root, meshRoot, actorRoot: path.join(root, "actors"),
    residencyRoot: residentRoot(meshRoot, "session:retained-custody"), fullCodeMode: true,
    agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, retainRuns: true, maxConcurrent: 1 },
    mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, residentIdleExitMs: 2_000 }, retention: DEFAULT_FABRIC_CONFIG.retention,
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fabricExtensionPath: path.resolve("dist/index.js"),
    piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
    piModels: { available: [{ provider: "fixture", id: "visible" }], aliases: {}, defaultModel: "fixture/visible" } };
  fs.mkdirSync(config.residencyRoot, { recursive: true });
  fs.writeFileSync(path.join(config.residencyRoot, "config.json"), JSON.stringify(config));
  let now = Date.now();
  vi.spyOn(Date, "now").mockImplementation(() => now);
  const idle = vi.fn();
  const host = new ResidentHost(config, idle);
  await host.start();
  await sleep(150);
  return { root, config, host, idle, now: () => now,
    advance: async (ms: number) => { now += ms; await sleep(150); } };
}

// Keep: #722's execution-custody boundary. Only native transport receipts are simulated;
// settlement, permit retention/release, resident subscriptions and idle decisions are real.
describe("resident retained native custody", () => {
  it.skipIf(process.platform !== "linux").each(["idle-exit", "dormant"] as const)(
    "prevents %s through bounded unconfirmed finishing, then resumes after exact custody release", async mode => {
    const f = await fixture();
    let releaseClosed!: () => void;
    const closed = new Promise<void>(resolve => { releaseClosed = resolve; });
    let confirmed = false;
    const sessionId = "2147483647"; // Absent PID: no real process tree is launched or signalled.
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async (request: AgentTransportLaunch) => {
      const statusFile = request.workerArguments[request.workerArguments.indexOf("--status-file") + 1]!;
      fs.writeFileSync(statusFile, JSON.stringify({ id: request.id, name: request.name, task: "custody fixture",
        status: "finishing", runner: "pi", transport: "process", sessionId, cwd: request.cwd,
        startedAt: f.now(), updatedAt: f.now(), turns: 1, toolCalls: 0, text: "finishing",
        usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 } }));
      return { kind: "process", sessionId, closed, finishingGraceMs: 20,
        stop: async () => { if (!confirmed) throw new Error("fixture cleanup did not confirm exit"); },
        isAlive: async () => !confirmed };
    });
    const inRunTree = (target: fs.PathLike) => String(target).startsWith(path.join(f.config.residencyRoot, "runs") + path.sep);
    const removed: string[] = [];
    const rmSync = fs.rmSync.bind(fs);
    const unlinkSync = fs.unlinkSync.bind(fs);
    const promisesRm = fs.promises.rm.bind(fs.promises);
    // Recorded-call proof: no product run-tree deletion is executed locally.
    vi.spyOn(fs, "rmSync").mockImplementation(((target: fs.PathLike, options?: fs.RmOptions) => {
      if (inRunTree(target)) { removed.push(String(target)); return; }
      return rmSync(target, options);
    }) as typeof fs.rmSync);
    vi.spyOn(fs, "unlinkSync").mockImplementation(((target: fs.PathLike) => {
      if (inRunTree(target)) { removed.push(String(target)); return; }
      return unlinkSync(target);
    }) as typeof fs.unlinkSync);
    vi.spyOn(fs.promises, "rm").mockImplementation((async (target: fs.PathLike, options?: fs.RmOptions) => {
      if (inRunTree(target)) { removed.push(String(target)); return; }
      return promisesRm(target, options);
    }) as typeof fs.promises.rm);
    try {
      const handle = await f.host.agents.spawn({ task: "custody fixture", transport: "process" });
      await vi.waitFor(() => expect(f.host.agents.isSettled(handle.id)).toBe(true), { timeout: 5_000 });
      expect(await f.host.agents.wait(handle.id)).toMatchObject({ status: "failed", errorCode: "CUSTODY_UNCONFIRMED" });
      expect(f.host.agents.heldPermits()).toBe(1);
      expect(f.host.agents.hasRetainedCustody()).toBe(true);
      const actor = mode === "dormant"
        ? await f.host.actors.create({ name: "custody-dormancy", instructions: "wait", residency: "durable" }) : undefined;
      const threshold = actor ? 30_000 : 2_000;
      await f.advance(threshold + 1);
      expect(f.idle).not.toHaveBeenCalled();
      if (actor) expect(f.host.actors.status(actor.id).status).toBe("idle");
      expect(fs.existsSync(residentIdleExitPath(f.config.residencyRoot))).toBe(false);
      expect(fs.existsSync(residentSleepingPath(f.config.residencyRoot))).toBe(false);

      // The larger dormant window also crosses the already-started cleanup join's
      // 7s bound. Let that old join finish before publishing a later exact receipt.
      if (actor) await sleep(7_100);
      // Model the kernel ENODEV receipt; the real manager proves its exact owned PID absent,
      // releases the permit, and notifies the resident through its existing UI subscription.
      const runDirectory = f.host.agents.runDirectory(handle.id)!;
      const marker = path.join(runDirectory, "unresolved-worker.json");
      fs.writeFileSync(marker, JSON.stringify({ version: 1,
        reason: "Owned scope observation failed: Error: ENODEV", runId: handle.id, transport: "process" }));
      confirmed = true;
      releaseClosed();
      await vi.waitFor(() => expect(f.host.agents.hasRetainedCustody()).toBe(false), { timeout: 5_000 });
      expect(f.host.agents.heldPermits()).toBe(0);
      if (actor) await vi.waitFor(() => expect(f.host.actors.status(actor.id).status).toBe("dormant"), { timeout: 5_000 });
      else await sleep(150); // First idle request tick starts the fresh continuous window.
      await f.advance(threshold - 1);
      expect(f.idle).not.toHaveBeenCalled();
      await f.advance(1);
      expect(f.idle).toHaveBeenCalledExactlyOnceWith(mode);
      expect(fs.existsSync(mode === "dormant" ? residentSleepingPath(f.config.residencyRoot)
        : residentIdleExitPath(f.config.residencyRoot))).toBe(true);
      expect(removed).toContain(marker);
      expect(fs.existsSync(marker)).toBe(true); // The recorder, not a real deletion, discharged the call.
      expect(fs.existsSync(runDirectory)).toBe(true);
    } finally {
      confirmed = true;
      releaseClosed();
      await f.host.close();
    }
  });

  it("keeps the ordinary no-custody idle-exit threshold", async () => {
    const f = await fixture();
    try {
      expect(f.host.agents.hasRetainedCustody()).toBe(false);
      await f.advance(1_999);
      expect(f.idle).not.toHaveBeenCalled();
      await f.advance(1);
      expect(f.idle).toHaveBeenCalledExactlyOnceWith("idle-exit");
    } finally { await f.host.close(); }
  });
});
