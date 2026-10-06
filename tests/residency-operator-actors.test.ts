import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { execFile, spawn, type ChildProcess } from "node:child_process";
import { promisify } from "node:util";
import { once } from "node:events";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { main, resolveResidentDirectory } from "../src/actors-cli.js";
import { ResidentHost } from "../src/residency/host.js";
import { ResidentActorClient } from "../src/residency/actor-client.js";
import { residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import { mainGenerationPath } from "../src/residency/handover.js";
import { processStartTime, residentProcessAlive } from "../src/residency/process-identity.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { hostLeasePath, writeHostLease } from "../src/topology/host-leases.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { MeshStore } from "../src/mesh/store.js";
import { installInProcessResidentFence } from "./helpers/in-process-resident-fence.js";

beforeEach(() => installInProcessResidentFence());
const waitFor = (predicate: () => boolean) => vi.waitFor(() => expect(predicate()).toBe(true), { timeout: 15_000, interval: 30 });
const stopChild = async (child: ChildProcess) => {
  if (child.exitCode !== null || child.signalCode !== null) return;
  const done = once(child, "exit"); child.kill("SIGTERM"); await done;
};
const fixture = async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-operator-"));
  const meshRoot = path.join(root, "mesh");
  const config: ResidentHostConfig = {
    format: 1, rootId: "session:dead-owner", sessionId: "dead-owner", cwd: process.cwd(), projectRoot: process.cwd(),
    meshRoot, actorRoot: path.join(root, "actors"), residencyRoot: residentRoot(meshRoot, "session:dead-owner"),
    fullCodeMode: true, agents: { ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent: 4, budgetUsd: 0 },
    mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 30 }, retention: { ...DEFAULT_FABRIC_CONFIG.retention },
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fabricExtensionPath: path.resolve("dist/index.js"),
    piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
    piModels: { available: [{ provider: "fixture", id: "visible" }], aliases: {}, defaultModel: "fixture/visible" },
  };
  fs.mkdirSync(config.residencyRoot, { recursive: true });
  fs.writeFileSync(path.join(config.residencyRoot, "config.json"), JSON.stringify(config));
  const child = spawn(process.execPath, ["-e", "console.log('ready');setInterval(()=>{},1000)"], { stdio: ["ignore", "pipe", "pipe"] });
  await once(child.stdout!, "data");
  fs.writeFileSync(mainGenerationPath(config.residencyRoot), JSON.stringify({ rootId: config.rootId,
    sessionId: config.sessionId, pid: child.pid, processStartTime: processStartTime(child.pid!), nonce: "test", releaseRoot: process.cwd() }));
  const host = new ResidentHost(config, () => {});
  try { await host.start(); } catch (error) { await stopChild(child); await host.close(); throw error; }
  const cli = async (action: "stop" | "remove", actor: string, flags: string[] = [], resident = config.residencyRoot) => {
    let out = "", err = "";
    const code = await main([action, "--resident", resident, "--actor", actor, "--mesh-root", meshRoot, ...flags],
      { out: text => { out += text; }, err: text => { err += text; } });
    return { code, out, err };
  };
  const create = (name: string) => host.actors.create({ name, instructions: "Run", model: "fixture/visible",
    residency: "durable", transport: "process", extensions: false });
  return { root, host, config, child, cli, create, close: async () => {
    await stopChild(child);
    for (const actor of host.actors.listOwned()) await host.actors.stop(actor.id, undefined, true);
    await host.close(); fs.rmSync(root, { recursive: true, force: true });
  } };
};

describe("same-user resident actor operator", () => {
  it("after Main exits, CLI stops/drains only one actor then removes its registry and participant while the other keeps running", async () => {
    const f = await fixture();
    try {
      const victim = await f.create("victim"), survivor = await f.create("survivor");
      f.host.actors.tell(victim.id, "HANG_WITH_PROGRESS");
      f.host.actors.tell(survivor.id, "HANG_WITH_PROGRESS");
      await waitFor(() => !!f.host.actors.status(victim.id).inFlightRun && !!f.host.actors.status(survivor.id).inFlightRun);
      const runId = f.host.actors.status(victim.id).inFlightRun!.id;
      await waitFor(() => ("turns" in f.host.agents.status(runId) && Number((f.host.agents.status(runId) as { turns?: number }).turns) > 0));
      const runPid = Number(f.host.agents.status(runId).sessionId);
      await f.host.participants.refresh();
      expect(f.host.participants.get(victim.id, Date.now(), { fresh: true })).toBeDefined();
      await stopChild(f.child);
      const dry = await f.cli("stop", victim.name, ["--dry-run"], path.basename(f.config.residencyRoot).slice(0, 12));
      expect(dry).toMatchObject({ code: 0, err: "" });
      expect(f.host.actors.status(victim.id).status).toBe("running");
      const stopped = await f.cli("stop", victim.name);
      expect(stopped).toMatchObject({ code: 0, err: "" });
      expect(JSON.parse(stopped.out).actor.status).toBe("stopped");
      expect(f.host.actors.status(victim.id).inFlightRun).toBeUndefined();
      expect(residentProcessAlive(runPid)).toBe(false);
      expect(f.host.actors.status(survivor.id).status).toBe("running");
      const removed = await f.cli("remove", victim.id);
      expect(removed).toMatchObject({ code: 0, err: "" });
      await waitFor(() => !new ActorRegistryStore(f.config.actorRoot).records().some(actor => actor.id === victim.id));
      await f.host.participants.refresh();
      expect(f.host.participants.get(victim.id, Date.now(), { fresh: true })).toBeUndefined();
      expect(f.host.participants.get(survivor.id, Date.now(), { fresh: true })).toBeDefined();
      expect(f.host.actors.status(survivor.id).status).toBe("running");
    } finally { await f.close(); }
  }, 40_000);

  it("remove an in-flight actor uses terminal drain, normal registry revocation and presence cleanup", async () => {
    const f = await fixture();
    try {
      const victim = await f.create("remove-running"), survivor = await f.create("other");
      f.host.actors.tell(victim.id, "HANG_WITH_PROGRESS");
      f.host.actors.tell(survivor.id, "HANG_WITH_PROGRESS");
      await waitFor(() => !!f.host.actors.status(victim.id).inFlightRun && !!f.host.actors.status(survivor.id).inFlightRun);
      const runId = f.host.actors.status(victim.id).inFlightRun!.id;
      await waitFor(() => ("turns" in f.host.agents.status(runId) && Number((f.host.agents.status(runId) as { turns?: number }).turns) > 0));
      const runPid = Number(f.host.agents.status(runId).sessionId);
      await stopChild(f.child);
      expect(await f.cli("remove", victim.name)).toMatchObject({ code: 0, err: "" });
      await f.host.actors.removalSettled(victim.id);
      await f.host.participants.refresh();
      expect(new ActorRegistryStore(f.config.actorRoot).records().some(actor => actor.id === victim.id)).toBe(false);
      expect(f.host.participants.get(victim.id, Date.now(), { fresh: true })).toBeUndefined();
      expect(residentProcessAlive(runPid)).toBe(false);
      expect(f.host.actors.status(survivor.id).status).toBe("running");
    } finally { await f.close(); }
  }, 40_000);

  it("refuses a live Main even without a lease; force-live is explicit and dry-run is non-mutating", async () => {
    const f = await fixture();
    try {
      const actor = await f.create("live-root");
      for (const action of ["stop", "remove"] as const) {
        const refused = await f.cli(action, actor.id);
        expect(refused.code).toBe(1); expect(refused.err).toContain("Main process is still alive");
      }
      expect((await f.cli("remove", actor.id, ["--force-live", "--dry-run"])).code).toBe(0);
      expect(f.host.actors.status(actor.id).status).toBe("idle");
      expect((await f.cli("stop", actor.id, ["--force-live"])).code).toBe(0);
      expect(f.host.actors.status(actor.id).status).toBe("stopped");
      expect((await f.cli("remove", actor.id, ["--force-live"])).code).toBe(0);
    } finally { await f.close(); }
  }, 30_000);

  it("a live file-only root lease vetoes control after Main process exits", async () => {
    const f = await fixture();
    try {
      const actor = await f.create("leased"); await stopChild(f.child);
      writeHostLease(f.config.meshRoot, { id: f.config.rootId, rootId: f.config.rootId, identityId: f.config.rootId,
        updatedAt: Date.now(), expiresAt: Date.now() + 60_000 });
      const result = await f.cli("remove", actor.id);
      expect(result.code).toBe(1); expect(result.err).toContain("live root lease");
      expect(f.host.actors.status(actor.id).status).toBe("idle");
    } finally { await f.close(); }
  }, 30_000);

  it("live shared-state root presence is independently checked by the executor", async () => {
    const f = await fixture();
    const identity = { id: f.config.rootId, name: "Main", kind: "main" as const, sessionId: f.config.sessionId };
    const mainDirectory = new ParticipantDirectory(new MeshStore(f.config.meshRoot, 65536, 100), {
      enabled: true, hostId: f.config.rootId, rootId: f.config.rootId, identity,
    });
    mainDirectory.registerSource(() => [{ format: 1, id: identity.id, kind: "root", rootId: identity.id,
      ownerHostId: identity.id, ownerIdentityId: identity.id, name: "Main", status: "idle", residency: "session",
      runner: "pi", transport: "host", capabilities: ["fabric"], sessionId: f.config.sessionId, cwd: f.config.cwd,
      startedAt: Date.now(), updatedAt: Date.now(), controlProtocol: "v1" }]);
    try {
      const actor = await f.create("shared-lease"); await mainDirectory.start(); await stopChild(f.child);
      fs.rmSync(hostLeasePath(f.config.meshRoot, f.config.rootId), { force: true });
      const result = await f.cli("stop", actor.id);
      expect(result.code).toBe(1); expect(result.err).toContain("live root lease");
    } finally { await mainDirectory.close(); await f.close(); }
  }, 30_000);

  it("an orphan shared host lease also blocks control even without a participant", async () => {
    const f = await fixture();
    const identity = { id: f.config.rootId, name: "Main", kind: "main" as const, sessionId: f.config.sessionId };
    const directory = new ParticipantDirectory(new MeshStore(f.config.meshRoot, 65536, 100), {
      enabled: true, hostId: f.config.rootId, rootId: f.config.rootId, identity,
    });
    try {
      const actor = await f.create("bare-shared-lease"); await directory.start(); await stopChild(f.child);
      fs.rmSync(hostLeasePath(f.config.meshRoot, f.config.rootId), { force: true });
      expect(f.host.participants.get(f.config.rootId, Date.now(), { fresh: true })).toBeUndefined();
      expect((await f.cli("remove", actor.id)).err).toContain("live root lease");
    } finally { await directory.close(); await f.close(); }
  }, 30_000);

  it("inbox PID evidence, malformed safety data and symlinked channels fail closed", async () => {
    const f = await fixture();
    try {
      const actor = await f.create("inbox-owner");
      const inbox = path.join(f.config.meshRoot, "main-followups", `${f.config.sessionId}.owner.json`);
      fs.mkdirSync(path.dirname(inbox), { recursive: true });
      fs.writeFileSync(inbox, JSON.stringify({ rootId: f.config.rootId, sessionId: f.config.sessionId,
        pid: f.child.pid, processStartedAt: processStartTime(f.child.pid!) }));
      fs.rmSync(mainGenerationPath(f.config.residencyRoot));
      expect((await f.cli("stop", actor.id)).err).toContain("Main process is still alive");
      await stopChild(f.child);
      fs.writeFileSync(inbox, "{bad JSON");
      expect((await f.cli("remove", actor.id)).code).toBe(1);
      fs.writeFileSync(inbox, JSON.stringify({ rootId: f.config.rootId, sessionId: f.config.sessionId, pid: f.child.pid }));
      const requestDir = path.join(f.config.residencyRoot, "requests");
      fs.renameSync(requestDir, requestDir + "-real");
      fs.symlinkSync(requestDir + "-real", requestDir, "dir");
      expect((await f.cli("remove", actor.id)).err).toContain("not owned by this OS user");
      expect(f.host.actors.status(actor.id).status).toBe("idle");
    } finally { await f.close(); }
  }, 30_000);

  it("packaged bin enforces live-Main refusal, dead-Main removal and clean unknown-id exit", async () => {
    const f = await fixture();
    const run = promisify(execFile);
    const argv = (action: string, id: string) => [path.resolve("bin/fabric-actors"), action,
      "--resident", f.config.residencyRoot, "--actor", id];
    try {
      const actor = await f.create("bin-target");
      await expect(run(process.execPath, argv("remove", actor.id))).rejects.toMatchObject({ code: 1,
        stderr: expect.stringContaining("Main process is still alive") });
      await stopChild(f.child);
      const result = await run(process.execPath, argv("remove", actor.name));
      expect(JSON.parse(result.stdout)).toMatchObject({ ok: true, action: "remove", resident: f.config.residencyRoot });
      await f.host.participants.refresh();
      expect(f.host.participants.get(actor.id, Date.now(), { fresh: true })).toBeUndefined();
      await expect(run(process.execPath, argv("stop", "unknown-id"))).rejects.toMatchObject({ code: 1,
        stderr: expect.stringContaining("Unknown Fabric actor") });
    } finally { await f.close(); }
  }, 30_000);

  it("unknown actor, invalid flags, missing process evidence and ambiguous resident prefixes fail cleanly", async () => {
    const f = await fixture();
    try {
      const actor = await f.create("known"); await stopChild(f.child);
      const unknown = await f.cli("remove", "not-an-actor");
      expect(unknown.code).toBe(1); expect(unknown.err).toContain("Unknown Fabric actor");
      expect(f.host.actors.status(actor.id).status).toBe("idle");
      const client = new ResidentActorClient(f.config.meshRoot, f.config.rootId);
      await expect(client.operatorActor("remove", actor.id, { forceLive: "yes" as unknown as boolean })).rejects.toThrow("Invalid resident operator");
      const ownerPath = path.join(f.config.residencyRoot, "owner.json");
      const owner = JSON.parse(fs.readFileSync(ownerPath, "utf8"));
      fs.writeFileSync(ownerPath, JSON.stringify({ ...owner, commands: owner.commands.filter((op: string) => op !== "operatorActor") }));
      await expect(client.operatorActor("remove", actor.id)).rejects.toThrow("older release");
      expect(fs.readdirSync(path.join(f.config.residencyRoot, "requests"))).toHaveLength(0);
      fs.writeFileSync(ownerPath, JSON.stringify(owner));
      fs.rmSync(mainGenerationPath(f.config.residencyRoot));
      expect((await f.cli("remove", actor.id)).err).toContain("no recorded identity");
      fs.mkdirSync(path.join(f.config.meshRoot, "residency", path.basename(f.config.residencyRoot).slice(0, 12) + "-other"));
      expect(() => resolveResidentDirectory(path.basename(f.config.residencyRoot).slice(0, 12), f.config.meshRoot)).toThrow("Ambiguous");
    } finally { await f.close(); }
  }, 30_000);
});
