import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import { runResidentHostFromConfigPath } from "../src/residency/host.js";
import { RESIDENT_HOST_FORMAT, residentDeliveryPrefix, residentHostId, residentResultPath, type ResidentDeliveryRecord, type ResidentHostConfig } from "../src/residency/protocol.js";

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
  it("retains a completed durable task beyond idle exit and delivers once after host restart", { timeout: 100_000 }, async () => {
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
