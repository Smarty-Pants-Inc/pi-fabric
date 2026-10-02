import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { afterEach, describe, expect, it, vi } from "vitest";
import * as recoveryPolicy from "../src/residency/handover.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { ResidentHost } from "../src/residency/host.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { TmuxTransport } from "../src/agents/transports/tmux-transport.js";
import { ScreenTransport } from "../src/agents/transports/screen-transport.js";
import { ResidencyClient } from "../src/residency/client.js";
import { processStartTime } from "../src/residency/process-identity.js";
import {
  residentLaunchSpec, validateLaunchSpec, assertHandoverTopology, assertPreviousLaunchSpec, ownHandoverPlan,
  handoverPath, handoverCustodyPath, handoverOutcomePath, mainGenerationPath, readHandoverJson,
  writeHandoverImmutable, writeHandoverState, writeLaunchSnapshot, decideHandover,
  type ResidentHandoverState, type ResidentLauncherIdentity,
} from "../src/residency/handover.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import type { ResidentHostConfig, ResidentHostOwner } from "../src/residency/protocol.js";

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
async function until(predicate: () => boolean, timeout = 7_000): Promise<void> {
  const deadline = Date.now() + timeout;
  while (!predicate()) { if (Date.now() >= deadline) throw new Error("Timed out at resident release boundary"); await sleep(10); }
}
function release(root: string, name: string): string {
  const dir = path.join(root, name);
  fs.mkdirSync(path.join(dir, "dist/residency"), { recursive: true });
  fs.writeFileSync(path.join(dir, "package.json"), JSON.stringify({ name: "pi-fabric", type: "module" }));
  fs.writeFileSync(path.join(dir, "dist/index.js"), "// immutable fixture extension\n");
  fs.writeFileSync(path.join(dir, "dist/residency/pi-entry.js"), "// immutable fixture entry\n");
  // A no-inference probe has its own isolated status; business activations
  // still use the existing deterministic native fake-worker fixture.
  fs.writeFileSync(path.join(dir, "dist/worker.js"), `import fs from 'node:fs';
const args=new Map();for(let i=2;i<process.argv.length;i+=2)args.set(process.argv[i].slice(2),process.argv[i+1]);
if(args.get('resident-startup-probe')==='true'){
 // Startup success includes cleanup, so persist the probe's own checked identity.
 const identity={sessionId:String(process.pid)};
 if(process.platform==='linux'){const stat=fs.readFileSync('/proc/'+process.pid+'/stat','utf8');identity.processStartTime=stat.slice(stat.lastIndexOf(')')+2).trim().split(/\\s+/)[19];}
 const now=Date.now();fs.writeFileSync(args.get('status-file'),JSON.stringify({id:args.get('id'),name:args.get('name'),task:'probe',status:'completed',runner:'pi',transport:'process',...identity,cwd:args.get('cwd'),startedAt:now,updatedAt:now,finishedAt:now,text:'resident worker startup verified',turns:0,toolCalls:0,usage:{input:0,output:0,cacheRead:0,cacheWrite:0,cost:0}}));
}else await import(${JSON.stringify(pathToFileURL(path.resolve("tests/fixtures/fake-worker.mjs")).href)});
`);
  return dir;
}
// The original protocol-only cases below mock ONLY the new recovery gate.
// Production and all round-3 safety cases run the fail-closed policy unchanged.
afterEach(() => vi.restoreAllMocks());
async function fixture(protocolOnly = true) {
  if (protocolOnly && "assertAutomaticReleaseRecovery" in recoveryPolicy) vi.spyOn(recoveryPolicy, "assertAutomaticReleaseRecovery").mockImplementation(() => {});
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-handover-"));
  const a = release(root, "A"), b = release(root, "B");
  const identity = { id: "session:release", name: "Main", kind: "main" as const, sessionId: "release" };
  const config: ResidentHostConfig = {
    format: 1, rootId: identity.id, sessionId: identity.sessionId, cwd: root, projectRoot: root,
    meshRoot: path.join(root, "mesh"), actorRoot: path.join(root, "actors"), sessionActorRoot: path.join(root, "session-actors"),
    residencyRoot: path.join(root, "resident"), fullCodeMode: true,
    agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, notifyOnComplete: false, nice: 10 },
    mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, retention: DEFAULT_FABRIC_CONFIG.retention,
    workerPath: path.join(a, "dist/worker.js"), fabricExtensionPath: path.join(a, "dist/index.js"),
    piBinary: process.execPath, claudeBinary: "fixture-missing-claude", vedaBinary: "fixture-missing-veda",
    piModels: { available: [{ provider: "fixture", id: "visible" }], aliases: {}, defaultModel: "fixture/visible" },
  };
  fs.mkdirSync(config.residencyRoot);
  fs.writeFileSync(path.join(config.residencyRoot, "config.json"), JSON.stringify(config));
  const mesh = new MeshStore(config.meshRoot, config.mesh.maxEventBytes, config.mesh.maxReadEvents);
  const participants = new ParticipantDirectory(mesh, { enabled: true, hostId: identity.id, rootId: identity.id, identity });
  participants.registerSource(() => [{ format: 1, id: identity.id, kind: "root", rootId: identity.id,
    ownerHostId: identity.id, ownerIdentityId: identity.id, name: "Main", status: "idle", residency: "session",
    runner: "pi", transport: "host", capabilities: ["fabric"], cwd: root, sessionId: identity.sessionId,
    startedAt: Date.now(), updatedAt: Date.now(), controlProtocol: "v1" }]);
  await participants.start();
  const previous = residentLaunchSpec(config, path.join(a, "dist/residency/pi-entry.js"));
  const targetConfig = { ...config, workerPath: path.join(b, "dist/worker.js"), fabricExtensionPath: path.join(b, "dist/index.js") };
  const target = residentLaunchSpec(targetConfig, path.join(b, "dist/residency/pi-entry.js"));
  const launcher: ResidentLauncherIdentity = { pid: process.pid, processStartTime: processStartTime(process.pid)!,
    token: "existing-launcher", entry: previous.entry, runtime: previous.runtime };
  const idle = vi.fn();
  const host = new ResidentHost(previous.config, idle, undefined, { launcher, spec: previous });
  await host.start();
  const client = new ResidencyClient({ config: target.config, mesh, participants,
    hostPath: path.join(b, "dist/residency/launcher.js"), mainAgent: { id: identity.id, local: true } as FabricMainAgentTarget });
  const state = () => readHandoverJson<ResidentHandoverState>(handoverPath(config.residencyRoot));
  let successor: ResidentHost | undefined;
  return { root, config, previous, target, launcher, host, client, idle, state, participants,
    successor: () => successor,
    startSuccessor: async () => {
      const plan = state()!.plan;
      successor = new ResidentHost(target.config, () => {}, undefined, { launcher, spec: target, attempt: { id: plan.id, kind: "target" } });
      await successor.start(); return successor;
    },
    startFallback: async () => {
      await successor?.close();
      const plan = state()!.plan;
      successor = new ResidentHost(previous.config, () => {}, undefined, { launcher, spec: previous, attempt: { id: plan.id, kind: "fallback" } });
      await successor.start(); return successor;
    },
    close: async () => { await client.close(); await host.close(); await successor?.close(); await participants.close(); fs.rmSync(root, { recursive: true, force: true }); },
  };
}

describe.skipIf(process.platform !== "linux")("resident release plan and idle-point custody", () => {
  it.each(["broken-B", "disposed-Main"] as const)("defers %s before A exits and serves the same acknowledged queue on A", async kind => {
    const f = await fixture(false);
    try {
      const actor = await f.host.actors.create({ name: "deferred-on-A", instructions: "Reply", residency: "durable", responseMode: "text", tools: [], delivery: "mailbox" });
      f.host.actors.tell(actor.id, "LIVE_WITH_PROGRESS");
      await until(() => !!f.host.actors.status(actor.id).inFlightRun);
      const receipt = f.host.actors.tell(actor.id, "queued-before-deferral");
      if (kind === "broken-B") fs.writeFileSync(f.target.config.workerPath, "throw Error('broken B startup');");
      await f.client.reconcileRelease();
      await until(() => ["custody", "cancelled"].includes(f.state()?.phase ?? ""));
      expect(f.state()?.phase).toBe("cancelled");
      expect(f.state()?.error).toMatch(/attempt.*exit|recovery.*unavailable/i);
      if (kind === "disposed-Main") await f.client.close();
      expect(f.idle).not.toHaveBeenCalled();
      expect(fs.existsSync(handoverCustodyPath(f.config.residencyRoot, f.state()!.plan.id))).toBe(false);
      expect(readHandoverJson<ResidentHostOwner>(path.join(f.config.residencyRoot, "owner.json"))?.releaseRoot).toBe(f.previous.releaseRoot);
      await until(() => f.host.actors.messages(actor.id).filter(m => m.direction === "out").length === 2);
      const messages = f.host.actors.messages(actor.id);
      expect(messages.filter(m => m.direction === "in" && m.id === receipt.messageId)).toHaveLength(1);
      expect(f.host.actors.status(actor.id).id).toBe(actor.id);
      expect(f.host.mesh.read({ topic: "host.reloaded" })).toHaveLength(0);
    } finally { await f.close(); }
  }, 20_000);

  it.each([["tmux", "failed"], ["tmux", "hung"], ["screen", "failed"], ["screen", "hung"]] as const)("cancels release for terminal live %s on %s observation and preserves its files", async (kind, observation) => {
    const f = await fixture(false);
    const adapter = kind === "tmux" ? TmuxTransport.prototype : ScreenTransport.prototype;
    const available = vi.spyOn(adapter, "available").mockResolvedValue(true);
    let handle: Awaited<ReturnType<ProcessTransport["launch"]>> | undefined;
    let fault: "none" | "failed" | "hung" = "none";
    let check: Promise<void> | undefined;
    let releaseQuery!: () => void;
    const query = new Promise<boolean>(resolve => { releaseQuery = () => resolve(false); });
    const launch = vi.spyOn(adapter, "launch").mockImplementation(async request => {
      handle = await new ProcessTransport().launch(request);
      return { ...handle, kind, relaunchable: false, livenessPollIntervalMs: 10,
        isAlive: async () => fault === "hung" ? query : fault === "failed" ? false : handle!.isAlive() };
    });
    try {
      const info = await f.host.agents.spawn({ task: "HANG until stopped", transport: kind });
      const run = path.join(f.config.residencyRoot, "runs", info.id);
      const status = path.join(run, "status.json");
      await until(() => fs.existsSync(status));
      const record = JSON.parse(fs.readFileSync(status, "utf8"));
      fs.writeFileSync(status, JSON.stringify({ ...record, status: "failed", error: "terminal UI does not prove exit", finishedAt: Date.now() }));
      await f.host.agents.wait(info.id);
      expect(await handle!.isAlive()).toBe(true);
      {
        fault = observation;
        check = f.host.agents.checkpointForRelease();
        const outcome = await Promise.race([check.then(() => "accepted", () => "vetoed"), sleep(400).then(() => "hung")]);
        expect(outcome).toBe("vetoed");
        await f.client.reconcileRelease();
        await until(() => ["cancelled", "custody"].includes(f.state()?.phase ?? ""));
        expect(f.state()?.phase).toBe("cancelled");
        expect(f.idle).not.toHaveBeenCalled();
        expect(fs.existsSync(run)).toBe(true);
        expect(await handle!.isAlive()).toBe(true);
        // A's ordinary actor admission is resumed, not destructively closed.
        const actor = await f.host.actors.create({ name: `served-${observation}`, instructions: "Reply", residency: "durable", tools: [], responseMode: "text" });
        f.host.actors.tell(actor.id, "after cancellation");
        await until(() => f.host.actors.messages(actor.id).some(m => m.direction === "out"));
      }
      releaseQuery(); fault = "failed";
      await f.host.close();
      expect(fs.existsSync(run)).toBe(true);
      expect(await handle!.isAlive()).toBe(true);
    } finally { releaseQuery(); fault = "none"; await check?.catch(() => undefined); await handle?.stop(); launch.mockRestore(); available.mockRestore(); await f.close(); }
  }, 20_000);

  it("freezes a missing absolute optional binary even if it is later installed", async () => {
    const f = await fixture(false);
    try {
      const missing = path.join(f.root, "optional-claude");
      const spec = residentLaunchSpec({ ...f.previous.config, claudeBinary: missing }, f.previous.entry);
      expect(spec.config.claudeBinary).not.toBe(missing);
      expect(spec.config.claudeBinary.startsWith(f.previous.releaseRoot)).toBe(true);
      fs.writeFileSync(missing, "#!/bin/sh\nexit 0\n", { mode: 0o700 });
      expect(() => validateLaunchSpec(spec)).not.toThrow();
      expect(fs.existsSync(spec.config.claudeBinary)).toBe(false);
    } finally { await f.close(); }
  });

  it("client pins the generic target runtime for a bundled Main and script Pi CLI", async () => {
    const f = await fixture(false);
    const exec = process.execPath;
    const runtime = fs.realpathSync(exec);
    const bundled = path.join(f.root, "pi"); fs.copyFileSync(runtime, bundled);
    try {
      f.client.options.config.piBinary = path.resolve("tests/fixtures/resident-probe-pi.mjs");
      process.execPath = bundled; vi.stubEnv("PI_FABRIC_NODE_BINARY", runtime);
      await f.client.reconcileRelease();
      expect(f.state()!.plan.target.config.piBinary).toBe(path.resolve("tests/fixtures/resident-probe-pi.mjs"));
      expect(f.state()!.plan.target.runtime).toBe(runtime);
      expect(f.state()!.plan.target.runtime).not.toBe(bundled);
    } finally { process.execPath = exec; vi.unstubAllEnvs(); await f.close(); }
  });

  it("cancellation and custody share one immutable CAS; neither can overwrite the winner", async () => {
    const f = await fixture();
    try {
      expect(decideHandover(f.config.residencyRoot, { id: "cancel-wins", state: "cancelled" }).state).toBe("cancelled");
      expect(decideHandover(f.config.residencyRoot, { id: "cancel-wins", state: "custody" }).state).toBe("cancelled");
      expect(decideHandover(f.config.residencyRoot, { id: "custody-wins", state: "custody" }).state).toBe("custody");
      expect(decideHandover(f.config.residencyRoot, { id: "custody-wins", state: "cancelled" }).state).toBe("custody");
    } finally { await f.close(); }
  });

  it("pins both launch specs and the shared-chunk closure; changed desired config cannot poison fallback", async () => {
    const f = await fixture();
    try {
      const saved = writeLaunchSnapshot(f.config.residencyRoot, f.previous);
      fs.writeFileSync(path.join(f.config.residencyRoot, "config.json"), JSON.stringify({ ...f.target.config, kernel: "python" }));
      expect(JSON.parse(fs.readFileSync(saved, "utf8"))).toEqual(f.previous.config);
      expect(() => writeHandoverImmutable(saved, f.target.config)).toThrow();
      expect(() => validateLaunchSpec(f.previous)).not.toThrow();
      expect(path.isAbsolute(f.previous.config.claudeBinary)).toBe(true);
      expect(f.previous.config.claudeBinary.startsWith(f.previous.releaseRoot)).toBe(true);
      expect(() => assertPreviousLaunchSpec(f.previous, f.target)).toThrow(/healthy loaded A/);
      expect(() => assertPreviousLaunchSpec(f.previous, { ...f.previous, config: { ...f.previous.config, piBinary: f.target.config.workerPath } })).toThrow(/healthy loaded A/);
      expect(() => assertPreviousLaunchSpec(f.previous, { ...f.previous, config: { ...f.previous.config, kernel: "python" } })).not.toThrow();
      fs.mkdirSync(path.join(f.target.releaseRoot, "dist/chunks"));
      fs.writeFileSync(path.join(f.target.releaseRoot, "dist/chunks/shared.mjs"), "// changed transitive closure");
      expect(() => validateLaunchSpec(f.target)).toThrow(/snapshot changed/);
      expect(() => assertHandoverTopology(f.previous, { ...f.target, config: { ...f.target.config, actorRoot: "other" } })).toThrow(/actorRoot/);
    } finally { await f.close(); }
  });

  it("never exits without launcher custody, and an obsolete Main nonce reversibly resumes A", async () => {
    const f = await fixture();
    try {
      await f.client.reconcileRelease();
      await until(() => ["custody", "cancelled"].includes(f.state()?.phase ?? ""));
      expect(f.state()?.phase, f.state()?.error).toBe("custody");
      await sleep(150);
      expect(f.idle).not.toHaveBeenCalled();
      const owner = readHandoverJson<ResidentHostOwner>(path.join(f.config.residencyRoot, "owner.json"))!;
      const plan = f.state()!.plan;
      expect(ownHandoverPlan(plan, owner, f.launcher, process.pid)).toBe(true);
      expect(ownHandoverPlan(plan, owner, { ...f.launcher, token: "competing-launcher" }, process.pid)).toBe(false);
      expect(ownHandoverPlan({ ...plan, old: { ...plan.old, token: "other-generation" } }, owner, f.launcher, process.pid)).toBe(false);
      fs.writeFileSync(mainGenerationPath(f.config.residencyRoot), JSON.stringify({ ...plan.main, nonce: "reloaded-again" }));
      await until(() => f.state()?.phase === "cancelled");
      expect(f.idle).not.toHaveBeenCalled();
      expect(readHandoverJson<ResidentHostOwner>(path.join(f.config.residencyRoot, "owner.json"))?.token).toBe(owner.token);
      const actor = await f.host.actors.create({ name: "still-served-on-A", instructions: "Reply", residency: "durable", responseMode: "text", tools: [] });
      f.host.actors.tell(actor.id, "after cancellation");
      await until(() => f.host.actors.messages(actor.id).some((m) => m.direction === "out"));
    } finally { await f.close(); }
  });

  it("disposing Main while its PID lives cancels uncommitted preparation", async () => {
    const f = await fixture();
    try {
      await f.client.reconcileRelease(); await until(() => f.state()?.phase === "custody");
      const main = f.state()!.plan.main;
      await f.client.close();
      await until(() => f.state()?.phase === "cancelled");
      expect(readHandoverJson<{ nonce: string }>(mainGenerationPath(f.config.residencyRoot))?.nonce).not.toBe(main.nonce);
      expect(processStartTime(main.pid)).toBe(main.processStartTime);
      expect(f.idle).not.toHaveBeenCalled();
    } finally { await f.close(); }
  });

  it("cancels release for a terminal failed record with a lost live worker", async () => {
    const f = await fixture();
    const launch = ProcessTransport.prototype.launch;
    const handles: Array<Awaited<ReturnType<typeof launch>>> = [];
    const spy = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function (this: ProcessTransport, request) {
      const handle = await launch.call(this, request); handles.push(handle);
      return { ...handle, relaunchable: false, isAlive: async () => false, lostContact: () => "unreachable pane may still run" };
    });
    const stop = vi.spyOn(f.host.agents, "stop");
    try {
      const result = await f.host.agents.run({ task: "HANG until stopped", transport: "process" });
      expect(result.status).toBe("failed");
      expect(await handles[0]!.isAlive()).toBe(true);
      await f.client.reconcileRelease();
      await until(() => ["cancelled", "custody"].includes(f.state()?.phase ?? ""));
      expect(f.state()?.phase).toBe("cancelled");
      expect(f.state()?.error).toMatch(/worker|quiescence/i);
      expect(f.idle).not.toHaveBeenCalled();
      expect(fs.existsSync(handoverCustodyPath(f.config.residencyRoot, f.state()!.plan.id))).toBe(false);
      expect(stop).not.toHaveBeenCalled();
      expect(await handles[0]!.isAlive()).toBe(true);
    } finally { stop.mockRestore(); spy.mockRestore(); for (const handle of handles) await handle.stop(); await f.close(); }
  }, 20_000);

  it("cancels release for a pending launch with no registered UI run", async () => {
    const f = await fixture();
    const launch = ProcessTransport.prototype.launch;
    let reached = false; let releaseLaunch!: () => void;
    const gate = new Promise<void>(resolve => { releaseLaunch = resolve; });
    const spy = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function (this: ProcessTransport, request) {
      reached = true; await gate; return launch.call(this, request);
    });
    const spawning = f.host.agents.spawn({ task: "pending launch", transport: "process" });
    try {
      await until(() => reached);
      expect(f.host.agents.listForUi()).toEqual([]);
      await f.client.reconcileRelease();
      await until(() => ["cancelled", "custody"].includes(f.state()?.phase ?? ""));
      expect(f.state()?.phase).toBe("cancelled");
      expect(f.state()?.error).toMatch(/pending launch/);
      expect(f.idle).not.toHaveBeenCalled();
      expect(fs.existsSync(handoverCustodyPath(f.config.residencyRoot, f.state()!.plan.id))).toBe(false);
    } finally { releaseLaunch(); await spawning; spy.mockRestore(); await f.close(); }
  });

  it.each(["unresolved", "live", "unknown", "nested"])("cancels release for an earlier host's %s run obligation", async (kind) => {
    const f = await fixture();
    try {
      const run = path.join(f.config.residencyRoot, "runs", "earlier-host"); fs.mkdirSync(run, { recursive: true });
      fs.writeFileSync(path.join(run, "status.json"), JSON.stringify({ status: "failed", transport: "process", sessionId: kind === "live" ? String(process.pid) : "999999999" }));
      if (kind === "unresolved") fs.writeFileSync(path.join(run, "unresolved-worker.json"), "{}");
      if (kind === "unknown") fs.writeFileSync(path.join(run, "status.json"), "corrupt");
      if (kind === "nested") {
        const nested = path.join(run, "nested", "survivor"); fs.mkdirSync(nested, { recursive: true });
        fs.writeFileSync(path.join(nested, "unresolved-worker.json"), "{}");
      }
      await f.client.reconcileRelease();
      await until(() => ["cancelled", "custody"].includes(f.state()?.phase ?? ""));
      expect(f.state()?.phase).toBe("cancelled");
      expect(f.idle).not.toHaveBeenCalled();
      expect(fs.existsSync(run)).toBe(true);
      expect(fs.existsSync(handoverCustodyPath(f.config.residencyRoot, f.state()!.plan.id))).toBe(false);
    } finally { await f.close(); }
  });

  it("checked persistence failure cancels prepare without releasing A or acknowledging lost input", async () => {
    const f = await fixture();
    const checkpoint = vi.spyOn(f.host.actors, "checkpointForRelease").mockRejectedValue(new Error("fsync failure"));
    try {
      await f.client.reconcileRelease();
      await until(() => f.state()?.phase === "cancelled");
      expect(f.state()?.error).toContain("fsync failure");
      expect(f.idle).not.toHaveBeenCalled();
      expect(fs.existsSync(handoverCustodyPath(f.config.residencyRoot, f.state()!.plan.id))).toBe(false);
    } finally { checkpoint.mockRestore(); await f.close(); }
  });

  it("finishes in-flight A runs in both scopes, checkpoints queued IDs/cursors, then B serves them once after commitment", async () => {
    const f = await fixture();
    const stopped = vi.spyOn(f.host.agents, "stop");
    try {
      const actors = await Promise.all((["project", "session"] as const).map((scope) => f.host.actors.create({
        name: `held-${scope}`, scope, instructions: "Reply", residency: "durable", responseMode: "text", tools: [], delivery: "mailbox",
      })));
      for (const actor of actors) f.host.actors.tell(actor.id, "LIVE_WITH_PROGRESS");
      await until(() => actors.every((actor) => { const state = f.host.actors.status(actor.id); return state.status === "running" && !!state.inFlightRun; }));
      const runs = actors.map((actor) => f.host.actors.status(actor.id).inFlightRun!.id);
      const queued = actors.map((actor) => f.host.actors.tell(actor.id, "queued-on-A").messageId);
      await f.client.reconcileRelease();
      expect(f.state()?.phase).toBe("preparing");
      expect(f.idle).not.toHaveBeenCalled();
      await until(() => f.state()?.phase === "custody");
      expect(f.host.actors.inFlightCount()).toBe(0);
      expect(stopped).not.toHaveBeenCalled();
      expect(f.idle).not.toHaveBeenCalled();
      for (const [index, actor] of actors.entries()) {
        expect(f.host.actors.status(actor.id).lastRunId).toBe(runs[index]);
        const dir = path.join(actor.scope === "session" ? f.config.sessionActorRoot! : f.config.actorRoot, actor.id);
        const file = fs.readdirSync(dir).find((name) => name.startsWith("queue-") && name.endsWith(".json"))!;
        const saved = JSON.parse(fs.readFileSync(path.join(dir, file), "utf8"));
        expect(saved).toMatchObject({ cleanHandover: true, items: [{ id: queued[index], attempts: 0 }] });
        expect(f.host.actors.messages(actor.id).filter((m) => m.direction === "out")).toHaveLength(1);
      }
      const cursors = ["project", "session"].map((scope) => JSON.parse(fs.readFileSync(path.join(f.config.residencyRoot, `actor-mesh-cursor.json.${scope}`), "utf8")));
      const inode = fs.statSync(path.join(f.config.residencyRoot, "host.lock")).ino;
      const plan = f.state()!.plan;
      writeHandoverImmutable(handoverCustodyPath(f.config.residencyRoot, plan.id), { id: plan.id, launcher: f.launcher });
      await until(() => f.idle.mock.calls.length === 1);
      await f.host.close();
      expect(fs.statSync(path.join(f.config.residencyRoot, "host.lock")).ino).toBe(inode);
      const b = await f.startSuccessor();
      expect(b.actors.inFlightCount()).toBe(0); // restored backlog remains gated during startup probe
      for (const actor of actors) expect(b.actors.status(actor.id).id).toBe(actor.id);
      expect(b.mesh.read({ topic: "host.reloaded" })).toEqual([]);
      writeHandoverState(f.config.residencyRoot, plan, "complete");
      await until(() => actors.every((actor) => b.actors.messages(actor.id).filter((m) => m.direction === "out").length === 2));
      const events = b.mesh.read({ topic: "host.reloaded" });
      expect(events).toHaveLength(1);
      expect(events[0]?.data).toEqual({ old: f.previous.releaseRoot, new: f.target.releaseRoot, transaction: plan.id });
      const serving = readHandoverJson<ResidentHostOwner>(path.join(f.config.residencyRoot, "owner.json"))!;
      expect(serving.attempt).toBeUndefined();
      // A later reversible transaction must not hide the serving B generation.
      writeHandoverState(f.config.residencyRoot, { ...plan, id: "later-c" }, "cancelled");
      expect((await f.client.ensureHost()).token).toBe(serving.token);
      for (const [index, scope] of ["project", "session"].entries()) {
        const now = JSON.parse(fs.readFileSync(path.join(f.config.residencyRoot, `actor-mesh-cursor.json.${scope}`), "utf8"));
        expect(now.cursor).toBeGreaterThanOrEqual(cursors[index].cursor);
      }
    } finally { stopped.mockRestore(); await f.close(); }
  }, 20_000);

  it("a newer same-PID Main runtime invalidates committed custody while no business-ready owner exists", async () => {
    const f = await fixture();
    let reloaded: ResidencyClient | undefined;
    try {
      await f.client.reconcileRelease(); await until(() => f.state()?.phase === "custody");
      const plan = f.state()!.plan;
      writeHandoverImmutable(handoverCustodyPath(f.config.residencyRoot, plan.id), { id: plan.id, launcher: f.launcher });
      await until(() => f.idle.mock.calls.length === 1); await f.host.close();
      await f.startSuccessor();
      reloaded = new ResidencyClient(f.client.options);
      await reloaded.reconcileRelease();
      const newer = readHandoverJson<{ nonce: string; pid: number }>(mainGenerationPath(f.config.residencyRoot))!;
      expect(newer.pid).toBe(plan.main.pid);
      expect(newer.nonce).not.toBe(plan.main.nonce);
      expect(f.state()?.plan.id).toBe(plan.id); // never rewrite A/B custody
      await f.client.reconcileRelease();
      expect(readHandoverJson(mainGenerationPath(f.config.residencyRoot))).toEqual(newer);
    } finally { await reloaded?.close(); await f.close(); }
  });

  it("untouched backlog keeps its restore budget through staged B failure and A fallback", async () => {
    const f = await fixture();
    try {
      const actor = await f.host.actors.create({ name: "rollback-backlog", instructions: "Reply", residency: "durable", scope: "session", responseMode: "text", tools: [], delivery: "mailbox" });
      f.host.actors.tell(actor.id, "LIVE_WITH_PROGRESS");
      await until(() => !!f.host.actors.status(actor.id).inFlightRun);
      const receipt = f.host.actors.tell(actor.id, "untouched-backlog");
      await f.client.reconcileRelease();
      await until(() => f.state()?.phase === "custody");
      const plan = f.state()!.plan;
      writeHandoverImmutable(handoverCustodyPath(f.config.residencyRoot, plan.id), { id: plan.id, launcher: f.launcher });
      await until(() => f.idle.mock.calls.length === 1); await f.host.close();
      await f.startSuccessor();
      const fallback = await f.startFallback();
      const dir = path.join(f.config.sessionActorRoot!, actor.id);
      const file = fs.readdirSync(dir).find((name) => name.startsWith("queue-") && name.endsWith(".json"))!;
      expect(JSON.parse(fs.readFileSync(path.join(dir, file), "utf8"))).toMatchObject({ cleanHandover: true, items: [{ id: receipt.messageId, attempts: 0 }] });
      expect(fallback.actors.inFlightCount()).toBe(0);
      writeHandoverState(f.config.residencyRoot, plan, "fallback");
      await until(() => fallback.actors.messages(actor.id).filter((m) => m.direction === "out").length === 2);
      expect(fallback.mesh.read({ topic: "host.reloaded" })).toHaveLength(0);
    } finally { await f.close(); }
  }, 20_000);

  it("an indeterminate host.reloaded publication is never retried or followed by business activation", async () => {
    const f = await fixture();
    try {
      await f.client.reconcileRelease(); await until(() => f.state()?.phase === "custody");
      const plan = f.state()!.plan;
      writeHandoverImmutable(handoverCustodyPath(f.config.residencyRoot, plan.id), { id: plan.id, launcher: f.launcher });
      await until(() => f.idle.mock.calls.length === 1); await f.host.close();
      const b = await f.startSuccessor();
      const publish = vi.spyOn(b.mesh, "publish").mockRejectedValue(new Error("indeterminate event publication"));
      writeHandoverState(f.config.residencyRoot, plan, "complete");
      await until(() => publish.mock.calls.length === 1); await sleep(200);
      expect(publish).toHaveBeenCalledTimes(1);
      expect(b.actors.inFlightCount()).toBe(0);
      publish.mockRestore();
    } finally { await f.close(); }
  });

  it("does not reset the root/target retry budget by changing model/config overlays or runtime nonce", async () => {
    const f = await fixture();
    try {
      const outcome = handoverOutcomePath(f.config.residencyRoot, f.target);
      writeHandoverImmutable(outcome, { id: "failed-target", target: f.target.releaseRoot });
      const changed = residentLaunchSpec({ ...f.target.config, kernel: "python", pythonRuntime: "cpython" }, f.target.entry);
      expect(handoverOutcomePath(f.config.residencyRoot, changed)).toBe(outcome);
      await f.client.reconcileRelease();
      await sleep(100);
      expect(f.state()).toBeUndefined();
      expect(f.idle).not.toHaveBeenCalled();
    } finally { await f.close(); }
  });
});
