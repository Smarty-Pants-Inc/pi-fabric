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
// The producer and watchdog consume only committed envelopes. Atomic durable writes
// expose a parseable .json.<pid>.<uuid>.tmp while fsync waits, before the rename.
const outboxEntries = (outbox: string): string[] =>
  fs.existsSync(outbox) ? fs.readdirSync(outbox).filter(entry => entry.endsWith(".json")) : [];
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

// Public durable spawns come from a live session Main, not the resident executor.
const mainParticipants = (f: ReturnType<typeof setup>) => {
  const identity = { id: f.config.rootId, name: "live Main", kind: "main" as const, sessionId: f.config.sessionId };
  const participants = new ParticipantDirectory(f.mesh, {
    enabled: true, hostId: identity.id, rootId: identity.id, identity, reapDeadHosts: false,
  });
  participants.registerSource(() => [{
    format: 1, id: identity.id, rootId: identity.id, kind: "root", name: identity.name, status: "idle",
    ownerHostId: identity.id, ownerIdentityId: identity.id, sessionId: identity.sessionId,
    runner: "pi", transport: "host", capabilities: ["fabric"], controlProtocol: "v1",
    startedAt: Date.now(), updatedAt: Date.now(),
  }]);
  return participants;
};

describe("resident producer durable outbox", () => {
  it("waits for a committed envelope instead of selecting its parseable atomic staging file", () => {
    const f = setup();
    const committed = "stable-id.json";
    const staged = `${committed}.${process.pid}.staging.tmp`;
    try {
      fs.mkdirSync(f.outbox);
      fs.writeFileSync(path.join(f.outbox, staged), JSON.stringify({ id: "stable-id", agentCompletionId: "completed-task" }));
      expect(fs.readdirSync(f.outbox)).toHaveLength(1); // the old wait would succeed
      expect(outboxEntries(f.outbox)).toEqual([]);
      fs.renameSync(path.join(f.outbox, staged), path.join(f.outbox, committed));
      expect(outboxEntries(f.outbox)).toEqual([committed]);
      expect(fs.existsSync(path.join(f.outbox, staged))).toBe(false);
      expect(fs.existsSync(path.join(f.outbox, outboxEntries(f.outbox)[0]!))).toBe(true);
    } finally { fs.rmSync(f.root, { recursive: true, force: true }); }
  });
  it.skipIf(process.platform !== "linux")("keeps a completed task host awake while its reply is pending, then delivers once without restart", { timeout: 120_000 }, async () => {
    const f = setup();
    const launches = launchLog(f.root);
    for (const [key, value] of Object.entries(launches.env)) vi.stubEnv(key, value);
    const completions = vi.fn();
    const participants = mainParticipants(f);
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
      await wait(() => outboxEntries(f.outbox).length === 1);
      const entry = outboxEntries(f.outbox)[0]!;
      const pending = JSON.parse(fs.readFileSync(path.join(f.outbox, entry), "utf8"));
      expect(pending.agentCompletionId).toBe(id);
      expect(completions).not.toHaveBeenCalled();
      await sleep(31_000); // R-no-idle: pending response custody is NOT truly idle.
      expect(same(originalHost)).toBe(true);
      expect(fs.existsSync(path.join(f.config.residencyRoot, "owner.json"))).toBe(true);
      expect(fs.existsSync(path.join(f.outbox, entry))).toBe(true);
      for (const root of [f.config.actorRoot, f.config.sessionActorRoot!]) {
        const registry = path.join(root, "actors.json");
        expect(fs.existsSync(registry) ? JSON.parse(fs.readFileSync(registry, "utf8")).actors : []).toEqual([]);
      }
      expect(completions).not.toHaveBeenCalled();
      expect(ensure).not.toHaveBeenCalled();
      fs.rmSync(lock, { recursive: true });
      // The current host keeps reply custody; unlocking lets it finish, with no restart.
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
      expect(ensure).not.toHaveBeenCalled();
      const metadata = JSON.parse(fs.readFileSync(path.join(f.config.residencyRoot, "agents", `${id}.json`), "utf8"));
      expect(metadata.completionConsumedAt).toEqual(expect.any(Number));
      // Multiple watchdog/poll cycles must neither redeliver nor relaunch the completed task.
      await sleep(5_100);
      expect(completions).toHaveBeenCalledOnce();
      expect(ensure).not.toHaveBeenCalled();
      const workers = launches.owned().filter(({ argv }) => argv[0] === f.config.workerPath);
      expect(workers).toHaveLength(1);
      expect(workers[0]!.argv).toContain(id);
      expect(launches.owned().filter(({ argv }) => argv[0] === client.options.hostPath)).toHaveLength(1);
    } finally {
      fs.rmSync(lock, { recursive: true, force: true });
      await client.close();
      await stopAllOwned(launches.owned());
      await participants.close();
      ensure.mockRestore(); vi.unstubAllEnvs();
      fs.rmSync(f.root, { recursive: true, force: true });
    }
  });

  it("retains pending reply custody across explicit shutdown and delivers once after host restart", { timeout: 100_000 }, async () => {
    // Explicit same-process fixture adapter, never the real subprocess watchdog above.
    installInProcessResidentFence();
    const f = setup();
    const participants = mainParticipants(f);
    const client = new ResidencyClient({
      config: f.config, mesh: f.mesh, participants, mainAgent: { local: false } as FabricMainAgentTarget,
    });
    const controller = new AbortController();
    let running = runResidentHostFromConfigPath(f.configPath, controller.signal);
    let restarted: Promise<void> | undefined;
    const second = new AbortController();
    const lock = path.join(f.config.meshRoot, ".lock");
    try {
      await participants.start();
      await wait(() => fs.existsSync(path.join(f.config.residencyRoot, "owner.json")));
      const handle = await client.spawnAgent({ task: "LIVE_WITH_PROGRESS", transport: "process", residency: "durable" });
      const id = handle.id;
      // Hold the actual default mesh lock, not a shortened or mocked acquisition.
      await f.mesh.exclusive(() => undefined);
      fs.mkdirSync(lock);
      fs.writeFileSync(path.join(lock, "owner"), `resident-test\n${process.pid}\n${Date.now()}\n`);
      await wait(() => fs.existsSync(residentResultPath(f.config.residencyRoot, id)));
      await wait(() => outboxEntries(f.outbox).length === 1);
      const entry = outboxEntries(f.outbox)[0]!;
      const pending = JSON.parse(fs.readFileSync(path.join(f.outbox, entry), "utf8"));
      expect(pending.agentCompletionId).toBe(id);
      let exited = false;
      void running.then(() => { exited = true; });
      await sleep(31_000);
      expect(exited).toBe(false); // A pending reply prevents automatic idle exit.
      controller.abort();
      await running; // Explicit shutdown still preserves the outbox for recovery.
      expect(fs.existsSync(path.join(f.config.residencyRoot, "owner.json"))).toBe(false);
      expect(fs.existsSync(path.join(f.outbox, entry))).toBe(true);
      expect(f.mesh.listAll(residentDeliveryPrefix(f.config.rootId), { fresh: true })).toHaveLength(0);
      fs.rmSync(lock, { recursive: true });
      // Recovery replays the accepted completion, not a new spawn: no live Main is needed.
      await client.close();
      await participants.close();
      restarted = runResidentHostFromConfigPath(f.configPath, second.signal);
      await wait(() => f.mesh.listAll(residentDeliveryPrefix(f.config.rootId), { fresh: true }).length === 1);
      await wait(() => fs.readdirSync(f.outbox).length === 0);
      const delivered = f.mesh.listAll(residentDeliveryPrefix(f.config.rootId), { fresh: true })[0]!;
      expect(delivered.value).toEqual(pending);
      await sleep(200);
      expect(f.mesh.listAll(residentDeliveryPrefix(f.config.rootId), { fresh: true })).toHaveLength(1);
    } finally {
      fs.rmSync(lock, { recursive: true, force: true });
      await client.close();
      controller.abort(); second.abort();
      await running; await restarted;
      await participants.close();
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
