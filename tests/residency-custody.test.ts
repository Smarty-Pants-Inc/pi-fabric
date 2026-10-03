import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import * as processUtils from "../src/agents/transports/process-utils.js";
import { ResidencyClient } from "../src/residency/client.js";
import { ResidentHost } from "../src/residency/host.js";
import { residentHostId, type ResidentHostConfig } from "../src/residency/protocol.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import { launchLog, same } from "./helpers/owned-processes.js";

const executing = (pid: number, started: string) => {
  if (!same({ pid, started })) return false;
  const stat = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
  return !["Z", "X"].includes(stat.slice(stat.lastIndexOf(")") + 2).split(" ")[0]!);
};
const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-custody-r2-"));
  const config: ResidentHostConfig = {
    format: 1, rootId: "session:custody", sessionId: "custody", cwd: root, projectRoot: root,
    meshRoot: path.join(root, "mesh"), actorRoot: path.join(root, "actors"), residencyRoot: path.join(root, "resident"),
    fullCodeMode: false, agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, timeoutMs: 60_000 },
    mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, retention: DEFAULT_FABRIC_CONFIG.retention,
    workerPath: path.resolve("dist/worker.js"), fabricExtensionPath: path.resolve("dist/index.js"),
    piBinary: path.resolve("tests/fixtures/refusing-execution.mjs"), claudeBinary: "claude", vedaBinary: "veda",
    piModels: { available: [{ provider: "fixture", id: "visible" }], aliases: {}, defaultModel: "fixture/visible" },
  };
  fs.mkdirSync(config.residencyRoot);
  const host = new ResidentHost(config);
  const options = { config, mesh: host.mesh, participants: host.participants, mainAgent: { local: false } as FabricMainAgentTarget };
  return { root, config, host, options };
};
afterEach(() => { vi.restoreAllMocks(); vi.unstubAllEnvs(); });

describe.skipIf(process.platform !== "linux")("round 2 execution custody", () => {
  // The former Windows-parent-only injection still ran a Linux group-tracking
  // worker, so it was never a Windows tree proof. Round 4 excludes that new tree
  // contract; windows-custody-scope.test.ts now drives BOTH Windows branches and
  // native worker-e2e's kill-worker case remains required on Windows CI.
  it.each(["linux", "darwin"] as const)("F1 ProcessTransport -> real worker retains its separately detached refusing execution until stop settles (%s POSIX parent)", async platformName => {
    const platform = platformName === "linux" ? undefined : vi.spyOn(process, "platform", "get").mockReturnValue(platformName);
    const { root, config } = fixture();
    const ready = path.join(root, "execution.json");
    vi.stubEnv("REFUSING_EXECUTION_READY", ready);
    const manager = new AgentManager(root, config.agents, { workerPath: config.workerPath, piBinary: config.piBinary, runRoot: path.join(root, "runs") });
    const launch = ProcessTransport.prototype.launch;
    let workerPid = 0;
    const capture = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function (this: ProcessTransport, request) {
      const handle = await launch.call(this, request); workerPid = Number(handle.sessionId); return handle;
    });
    let leaf: { pid: number; started: string } | undefined;
    try {
      const run = await manager.spawn({ task: "refuse TERM", transport: "process" });
      await vi.waitFor(() => expect(fs.existsSync(ready)).toBe(true), { timeout: 10_000 });
      leaf = JSON.parse(fs.readFileSync(ready, "utf8"));
      const group = (pid: number) => fs.readFileSync(`/proc/${pid}/stat`, "utf8").split(") ")[1]!.split(" ")[2];
      expect(group(workerPid)).not.toBe(group(leaf!.pid));
      await manager.stop(run.id);
      expect(executing(leaf!.pid, leaf!.started), "stop must not settle while execution remains authorized").toBe(false);
      const record = JSON.parse(fs.readFileSync(path.join(root, "runs", run.id, "status.json"), "utf8"));
      expect(record.status, "worker's five-second child cleanup must finish before custodian escalation").toBe("stopped");
    } finally {
      capture.mockRestore(); platform?.mockRestore();
      if (leaf && executing(leaf.pid, leaf.started)) process.kill(leaf.pid, "SIGKILL");
      await manager.close();
      if (leaf) await vi.waitFor(() => expect(executing(leaf!.pid, leaf!.started)).toBe(false));
      fs.rmSync(root, { recursive: true, force: true });
    }
  }, 20_000);

  it("F1 aborted startup after actual restored actor execution retains and stops every detached group", async () => {
    const { root, config, host, options } = fixture();
    let client: ResidencyClient | undefined;
    const launches = launchLog(root);
    for (const [key, value] of Object.entries(launches.env)) vi.stubEnv(key, value);
    vi.stubEnv("REFUSING_EXECUTION_READY", path.join(root, "restored-execution.json"));
    const live = () => launches.owned().filter(owned => executing(owned.pid, owned.started));
    try {
      await host.start();
      const actor = await host.actors.create({ name: "restored", instructions: "work", residency: "durable", scope: "project", model: "fixture/visible", transport: "process", extensions: false });
      await host.close();
      // Seed host ran in this still-live test process; clear only its diagnostic
      // bytes after its confirmed close, retaining the established fence inode.
      fs.writeFileSync(path.join(config.residencyRoot, "host.lock"), "");
      const lineage = createHash("sha256").update([config.rootId, "durable"].join("\0")).digest("hex").slice(0, 16);
      const now = Date.now();
      // Persisted predecessor work; this fixture does not leave a live prior worker
      // (#2566 item 6 remains separate). The new real host performs the restoration.
      fs.writeFileSync(path.join(config.actorRoot, actor.id, `queue-${lineage}.json`), JSON.stringify({ format: 1, latestActivationSequence: 1, items: [{
        id: "restored-work", source: "direct", payload: { message: "restore me" }, createdAt: now,
        activation: { kind: "direct", id: "restored-work", source: "direct", sequence: 1, createdAt: now },
        binding: {}, bindingMode: "owner-defaults", bindingVersion: 2,
      }] }));
      fs.writeFileSync(path.join(config.residencyRoot, "config.json"), JSON.stringify(config));
      client = new ResidencyClient({ ...options, hostPath: path.resolve("tests/fixtures/restoring-resident-launcher.mjs"), startupTimeoutMs: 30_000 });
      const starting = client.ensureHost().catch(error => error);
      await vi.waitFor(() => expect(fs.existsSync(path.join(root, "restored-execution.json"))).toBe(true), { timeout: 10_000 }).catch(error => {
        const diagnostics = ["restore-error.json", "restore-state.json", "launcher.log"].map(file => { try { return fs.readFileSync(path.join(config.residencyRoot, file), "utf8"); } catch { return "absent"; } });
        throw new Error(`${error}\n${diagnostics.join("\n")}`);
      });
      expect(live().some(owned => owned.argv[0] === config.workerPath)).toBe(true);
      expect(live().some(owned => owned.argv[0] === config.piBinary)).toBe(true);
      expect(fs.existsSync(path.join(config.residencyRoot, "owner.json"))).toBe(false);
      await client.close();
      expect(await starting).toMatchObject({ message: "Fabric residency client is closed" });
      expect(live(), "aborted startup must join restored execution, not just the launcher group").toEqual([]);
    } finally {
      for (const owned of live()) if (same(owned)) process.kill(owned.pid, "SIGKILL");
      await client?.close(); await host.close();
      await vi.waitFor(() => expect(live()).toEqual([]), { timeout: 5_000 });
      fs.rmSync(root, { recursive: true, force: true });
    }
  }, 20_000);

  it("F2 real delayed losing launcher exits before ensureHost and close settle; ready winner stays untouched", async () => {
    const { root, config, host, options } = fixture();
    const launches = launchLog(root);
    for (const [key, value] of Object.entries(launches.env)) vi.stubEnv(key, value);
    const loser = new ResidencyClient({ ...options, hostPath: path.resolve("tests/fixtures/stubborn-resident-launcher.mjs"), startupTimeoutMs: 30_000 });
    const starting = loser.ensureHost().catch(error => error);
    const live = () => launches.owned().filter(owned => executing(owned.pid, owned.started));
    try {
      await vi.waitFor(() => expect(fs.existsSync(path.join(config.residencyRoot, "stubborn-child-ready"))).toBe(true), { timeout: 5_000 });
      expect(live()).toHaveLength(2);
      await host.start(); // A competing winner claims the actual kernel fence and publishes readiness.
      const owner = fs.readFileSync(path.join(config.residencyRoot, "owner.json"), "utf8");
      expect(await starting).toMatchObject({ pid: process.pid });
      expect(live()).toEqual([]);
      await loser.close();
      expect(live()).toEqual([]);
      expect(fs.readFileSync(path.join(config.residencyRoot, "owner.json"), "utf8")).toBe(owner);
    } finally {
      for (const owned of live()) if (same(owned)) process.kill(owned.pid, "SIGKILL");
      await starting; await loser.close(); await host.close();
      await vi.waitFor(() => expect(live()).toEqual([]));
      fs.rmSync(root, { recursive: true, force: true });
    }
  }, 15_000);

  it("F2 exact launch token transfers owned readiness without stopping the winner on client close", async () => {
    const { root, config, options } = fixture();
    const stop = vi.fn(async () => {});
    const client = new ResidencyClient(options);
    const spawn = vi.spyOn(processUtils, "spawnDetached").mockImplementation(async (_path, argv) => {
      const launchToken = argv[argv.indexOf("--launch-token") + 1];
      expect(launchToken).toMatch(/^[0-9a-f-]{36}$/);
      fs.writeFileSync(path.join(config.residencyRoot, "owner.json"), JSON.stringify({ format: 1, hostId: residentHostId(config.rootId), pid: process.pid, token: "ours", launchToken, startedAt: Date.now(), readyAt: Date.now() }));
      return { pid: process.pid + 1, stop, isAlive: async () => true, lostContact: () => undefined, waitForClose: async () => {} };
    });
    try {
      expect(await client.ensureHost()).toMatchObject({ token: "ours" });
      await client.close();
      expect(stop).not.toHaveBeenCalled();
    } finally { await client.close(); spawn.mockRestore(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it.each([false, true])("F2 concurrent ready winner joins its losing launcher; unconfirmed cleanup retained through close=%s", async (failCleanup) => {
    const { root, config, options } = fixture();
    const winner = new ResidencyClient(options);
    const loser = new ResidencyClient(options);
    const owner = { format: 1 as const, hostId: residentHostId(config.rootId), pid: process.pid, token: "winner", startedAt: Date.now(), readyAt: Date.now() };
    let release!: () => void;
    const stopping = new Promise<void>(resolve => { release = resolve; });
    const stop = vi.fn(async () => { if (failCleanup) throw new Error("loser exit unconfirmed"); await stopping; });
    const spawn = vi.spyOn(processUtils, "spawnDetached").mockImplementation(async () => {
      fs.writeFileSync(path.join(config.residencyRoot, "owner.json"), JSON.stringify(owner));
      return { pid: process.pid + 1, stop, isAlive: async () => true, lostContact: () => undefined, waitForClose: async () => {} };
    });
    try {
      let settled = false;
      const starting = loser.ensureHost().then(value => { settled = true; return value; }, error => { settled = true; return error; });
      await vi.waitFor(() => expect(spawn).toHaveBeenCalledTimes(1));
      expect(await winner.ensureHost()).toEqual(owner);
      await new Promise(resolve => setTimeout(resolve, 50));
      expect(stop).toHaveBeenCalledTimes(1);
      expect(fs.readFileSync(path.join(config.residencyRoot, "owner.json"), "utf8")).toBe(JSON.stringify(owner));
      if (failCleanup) {
        expect(await starting).toMatchObject({ message: "loser exit unconfirmed" });
        await expect(loser.close()).rejects.toThrow("loser exit unconfirmed");
        expect(stop).toHaveBeenCalledTimes(2);
        stop.mockImplementation(async () => {});
        await loser.close();
        expect(stop).toHaveBeenCalledTimes(3);
      } else {
        expect(settled).toBe(false);
        release();
        expect(await starting).toEqual(owner);
        await loser.close();
        expect(stop).toHaveBeenCalledTimes(1);
      }
    } finally {
      release(); stop.mockImplementation(async () => {});
      await loser.close(); await winner.close();
      spawn.mockRestore(); fs.rmSync(root, { recursive: true, force: true });
    }
  });
});
