import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { installInProcessResidentFence } from "./helpers/in-process-resident-fence.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import { runResidentHostFromConfigPath } from "../src/residency/host.js";
import { ResidencyClient } from "../src/residency/client.js";
import { kernelFenceAvailable } from "../src/residency/file-lock.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { launchLog, same, stopAllOwned } from "./helpers/owned-processes.js";
import { RESIDENT_HOST_FORMAT, residentDeliveryPrefix, residentHostId, residentResultPath, type ResidentDeliveryRecord, type ResidentHostConfig } from "../src/residency/protocol.js";

afterEach(() => vi.restoreAllMocks());

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
const wait = async (check: () => boolean, timeoutMs = 10_000) => {
  const deadline = Date.now() + timeoutMs;
  while (!check()) { if (Date.now() >= deadline) throw new Error("Resident outbox did not recover"); await sleep(25); }
};
const setup = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "resident-outbox-"));
  const config: ResidentHostConfig = {
    format: RESIDENT_HOST_FORMAT, rootId: "session:outbox", sessionId: "outbox", cwd: process.cwd(), projectRoot: root,
    meshRoot: path.join(root, "mesh"), actorRoot: path.join(root, "actors"), sessionActorRoot: path.join(root, "session-actors"),
    residencyRoot: path.join(root, "resident"), fullCodeMode: true,
    agents: { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 10_000, notifyOnComplete: true },
    mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorScope: "session", actorPollMs: 20 }, retention: DEFAULT_FABRIC_CONFIG.retention,
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fabricExtensionPath: path.resolve("dist/index.js"),
    piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
    piModels: { available: [{ provider: "provider", id: "visible" }], aliases: {}, defaultModel: "provider/visible" },
  };
  fs.mkdirSync(config.residencyRoot);
  const configPath = path.join(config.residencyRoot, "config.json");
  fs.writeFileSync(configPath, JSON.stringify(config));
  return { root, config, configPath, outbox: path.join(config.residencyRoot, "delivery-outbox"), mesh: new MeshStore(config.meshRoot, 65536, 1000) };
};

describe("resident producer durable outbox", () => {
  it.skipIf(process.platform !== "linux")("F6 live watchdog recovers a sole completed task after locked idle exit exactly once without explicit restart", { timeout: 120_000 }, async () => {
    const f = setup();
    const launches = launchLog(f.root);
    for (const [key, value] of Object.entries(launches.env)) vi.stubEnv(key, value);
    const completions = vi.fn();
    const participants = new ParticipantDirectory(f.mesh, { enabled: true, hostId: f.config.rootId, rootId: f.config.rootId,
      identity: { id: f.config.rootId, name: "live Main", kind: "main" }, reapDeadHosts: false });
    const client = new ResidencyClient({
      config: f.config, mesh: f.mesh, participants,
      mainAgent: { local: true } as FabricMainAgentTarget,
      hostPath: path.resolve("dist/residency/launcher.js"),
      onBackgroundComplete: (result, delivered) => { completions(result); delivered(); },
    });
    // Both the original and recovered hosts use the real compiled launcher -> Pi.
    // An in-process host would leave a live Vitest PID in the legacy-owner fence.
    const ensure = vi.spyOn(client, "ensureHost");
    const lock = path.join(f.config.meshRoot, ".lock");
    try {
      await participants.start();
      const handle = await client.spawnAgent({ task: "LIVE_WITH_PROGRESS", transport: "process", residency: "durable" });
      const id = handle.id;
      const owner = JSON.parse(fs.readFileSync(path.join(f.config.residencyRoot, "owner.json"), "utf8"));
      const originalHost = launches.owned().find(({ pid }) => pid === owner.pid)!;
      expect(originalHost).toBeDefined();
      expect(originalHost.argv).toContain(path.resolve("dist/residency/pi-entry.js"));
      ensure.mockClear();
      client.start();
      await f.mesh.exclusive(() => undefined);
      fs.mkdirSync(lock);
      fs.writeFileSync(path.join(lock, "owner"), `resident-test\n${process.pid}\n${Date.now()}\n`);
      await wait(() => fs.existsSync(residentResultPath(f.config.residencyRoot, id)));
      await wait(() => fs.existsSync(f.outbox) && fs.readdirSync(f.outbox).length === 1);
      const entry = fs.readdirSync(f.outbox)[0]!;
      const pending = JSON.parse(fs.readFileSync(path.join(f.outbox, entry), "utf8"));
      expect(pending.agentCompletionId).toBe(id);
      expect(completions).not.toHaveBeenCalled();
      await wait(() => !same(originalHost), 75_000); // actual idle shutdown and process exit
      // Inject a lock-release delay across the watchdog tick. It can observe the
      // absent owner before this test observes process exit; an automatic ensureHost
      // is legal while the mesh is still locked. Count recovery over the whole exit,
      // not relative to our 25 ms polling loop's observation of it.
      await wait(() => ensure.mock.calls.length > 0, 10_000);
      await sleep(250);
      expect(fs.existsSync(path.join(f.config.residencyRoot, "owner.json"))).toBe(false);
      expect(fs.existsSync(path.join(f.outbox, entry))).toBe(true);
      for (const root of [f.config.actorRoot, f.config.sessionActorRoot!]) {
        const registry = path.join(root, "actors.json");
        expect(fs.existsSync(registry) ? JSON.parse(fs.readFileSync(registry, "utf8")).actors : []).toEqual([]);
      }
      expect(completions).not.toHaveBeenCalled();
      expect(ensure).toHaveBeenCalledOnce();
      fs.rmSync(lock, { recursive: true });
      // Only the live watchdog starts recovery: no explicit restart or new spawn.
      try {
        await wait(() => completions.mock.calls.length > 0, 45_000);
      } catch (error) {
        const diagnostics: Record<string, unknown> = { kernelFence: kernelFenceAvailable(), watchdogStarts: ensure.mock.calls.length, pending };
        for (const file of ["owner.json", "error.json", "launcher.log", "child-stderr.log"]) {
          try { diagnostics[file] = fs.readFileSync(path.join(f.config.residencyRoot, file), "utf8"); } catch { /* absent */ }
        }
        throw new Error(`${String(error)}; recovery diagnostics: ${JSON.stringify(diagnostics)}`);
      }
      await wait(() => fs.readdirSync(f.outbox).length === 0 &&
        f.mesh.listAll(residentDeliveryPrefix(f.config.rootId), { fresh: true }).length === 0);
      expect(completions).toHaveBeenCalledExactlyOnceWith(expect.objectContaining({ id, status: "completed", text: "live attempt 1 complete" }));
      expect(ensure).toHaveBeenCalledOnce();
      const metadata = JSON.parse(fs.readFileSync(path.join(f.config.residencyRoot, "agents", `${id}.json`), "utf8"));
      expect(metadata.completionConsumedAt).toEqual(expect.any(Number));
      // Multiple watchdog/poll cycles must neither redeliver nor relaunch the completed task.
      await sleep(5_100);
      expect(completions).toHaveBeenCalledOnce();
      expect(ensure).toHaveBeenCalledOnce();
      const workers = launches.owned().filter(({ argv }) => argv[0] === f.config.workerPath);
      expect(workers).toHaveLength(1);
      expect(workers[0]!.argv).toContain(id);
      expect(launches.owned().filter(({ argv }) => argv[0] === client.options.hostPath)).toHaveLength(2);
    } finally {
      fs.rmSync(lock, { recursive: true, force: true });
      await client.close();
      await stopAllOwned(launches.owned());
      await participants.close();
      ensure.mockRestore(); vi.unstubAllEnvs();
      fs.rmSync(f.root, { recursive: true, force: true });
    }
  });

  it("retains a completed durable task beyond idle exit and delivers once after host restart", { timeout: 100_000 }, async () => {
    // Explicit same-process fixture adapter, never the real subprocess watchdog above.
    installInProcessResidentFence();
    const f = setup();
    const controller = new AbortController();
    let running = runResidentHostFromConfigPath(f.configPath, controller.signal);
    let restarted: Promise<void> | undefined;
    const second = new AbortController();
    const lock = path.join(f.config.meshRoot, ".lock");
    try {
      await wait(() => fs.existsSync(path.join(f.config.residencyRoot, "owner.json")));
      const requestId = "completion";
      fs.writeFileSync(path.join(f.config.residencyRoot, "requests", `${requestId}.json`), JSON.stringify({
        format: RESIDENT_HOST_FORMAT, rootId: f.config.rootId, requestId, createdAt: Date.now(), operation: "spawn",
        request: { task: "LIVE_WITH_PROGRESS", transport: "process", residency: "durable" },
      }));
      const response = path.join(f.config.residencyRoot, "responses", `${requestId}.json`);
      await wait(() => fs.existsSync(response));
      const spawned = JSON.parse(fs.readFileSync(response, "utf8"));
      expect(spawned.ok, JSON.stringify(spawned)).toBe(true);
      const id = spawned.handle.id;
      // Hold the actual default mesh lock, not a shortened or mocked acquisition.
      await f.mesh.exclusive(() => undefined);
      fs.mkdirSync(lock);
      fs.writeFileSync(path.join(lock, "owner"), `resident-test\n${process.pid}\n${Date.now()}\n`);
      await wait(() => fs.existsSync(residentResultPath(f.config.residencyRoot, id)));
      await wait(() => fs.existsSync(f.outbox) && fs.readdirSync(f.outbox).length === 1);
      const entry = fs.readdirSync(f.outbox)[0]!;
      const pending = JSON.parse(fs.readFileSync(path.join(f.outbox, entry), "utf8"));
      expect(pending.agentCompletionId).toBe(id);
      let exited = false;
      void running.then(() => { exited = true; });
      await wait(() => exited, 75_000); // actual 30-second idle threshold plus close lock waits
      await running;
      expect(fs.existsSync(path.join(f.config.residencyRoot, "owner.json"))).toBe(false);
      expect(fs.existsSync(path.join(f.outbox, entry))).toBe(true);
      expect(f.mesh.listAll(residentDeliveryPrefix(f.config.rootId), { fresh: true })).toHaveLength(0);
      fs.rmSync(lock, { recursive: true });
      restarted = runResidentHostFromConfigPath(f.configPath, second.signal);
      await wait(() => f.mesh.listAll(residentDeliveryPrefix(f.config.rootId), { fresh: true }).length === 1);
      await wait(() => fs.readdirSync(f.outbox).length === 0);
      const delivered = f.mesh.listAll(residentDeliveryPrefix(f.config.rootId), { fresh: true })[0]!;
      expect(delivered.value).toEqual(pending);
      await sleep(200);
      expect(f.mesh.listAll(residentDeliveryPrefix(f.config.rootId), { fresh: true })).toHaveLength(1);
    } finally {
      fs.rmSync(lock, { recursive: true, force: true });
      controller.abort(); second.abort();
      await running; await restarted;
      fs.rmSync(f.root, { recursive: true, force: true });
    }
  });

  it.each([false, true])("replays commit-before-unlink without duplicating an envelope (consumed=%s)", async consumed => {
    installInProcessResidentFence();
    const f = setup();
    const controller = new AbortController();
    const record: ResidentDeliveryRecord = { format: RESIDENT_HOST_FORMAT, id: "stable-id", rootId: f.config.rootId,
      from: { id: "actor:producer", name: "producer", kind: "actor" }, delivery: "followUp", triggerTurn: true,
      message: "durable actor output", createdAt: Date.now() };
    fs.mkdirSync(f.outbox);
    fs.writeFileSync(path.join(f.outbox, `${record.id}.json`), JSON.stringify(record));
    const key = residentDeliveryPrefix(record.rootId) + record.id;
    const committed = await f.mesh.put({ key, value: record, identity: { id: residentHostId(record.rootId), name: "host", kind: "agent" }, ifVersion: 0 });
    if (consumed) await f.mesh.delete({ key, ifVersion: committed.version });
    const running = runResidentHostFromConfigPath(f.configPath, controller.signal);
    try {
      await wait(() => fs.readdirSync(f.outbox).length === 0);
      expect(f.mesh.get(key, { fresh: true })).toEqual(consumed ? undefined : committed);
    } finally { controller.abort(); await running; fs.rmSync(f.root, { recursive: true, force: true }); }
  });
});
