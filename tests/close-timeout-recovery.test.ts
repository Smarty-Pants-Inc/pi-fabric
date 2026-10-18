import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import {
  actorActivationFromRun, closeTimeoutDelivered, noteCloseTimeoutDelivered, peekCloseTimeoutRun, peekEnodevRun,
  recordProcessScope, recoverDurableActorStop, scopeCgroupPathAbsent, scopeUnitAbsent, settleCloseTimeoutRun,
  settleLostEnodevRun, writeCloseTimeoutOperatorReceipt,
} from "../src/agents/enodev-recovery.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { persistRunStopIntent } from "../src/agents/archive-custody.js";
import { processStartTime } from "../src/residency/process-identity.js";
import { main as actorsCli } from "../src/actors-cli.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import { residentRoot, residentHostId, type ResidentHostConfig } from "../src/residency/protocol.js";
import { ROOT_PARTICIPANT_FRESH_MS } from "../src/residency/operator-safety.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";
import { rootParticipantKey } from "./helpers/dead-root-fixture.js";

const roots: string[] = [];
const children: ChildProcess[] = [];
const actors: ActorManager[] = [];
const managers: AgentManager[] = [];
afterEach(async () => {
  vi.restoreAllMocks();
  for (const manager of actors.splice(0)) await manager.close().catch(() => undefined);
  for (const manager of managers.splice(0)) await manager.close().catch(() => undefined);
  for (const child of children.splice(0)) child.kill("SIGKILL");
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
}, 60_000);
const tempRoot = (): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-close-timeout-"));
  roots.push(root);
  return root;
};
const deadPid = (): number => spawnSync(process.execPath, ["-e", ""]).pid!;
const livePid = (): number => {
  const child = spawn(process.execPath, ["-e", "setTimeout(() => {}, 60000)"], { stdio: "ignore" });
  children.push(child);
  return child.pid!;
};
const RUN = "a".repeat(32);
const ACTOR = "b".repeat(32);
const ACTIVATION = "activation-1";
const SCOPE = "fabric-scope-abc123.scope";
const CLOSE_REASON = (id: string) => `Error: Agent ${id} execution exit unconfirmed; custody retained: Owned process worker did not confirm tree/native close within 7000ms`;
interface FixtureOptions {
  id?: string; actorId?: string; activationId?: string; pid?: number; birth?: string; status?: string;
  markerReason?: string; settlement?: unknown; scopeRecorded?: boolean; scopePresent?: boolean; taskActorId?: string;
}
interface Fixture { root: string; runRoot: string; cgroupRoot: string; directory: string; id: string; cgroupPath: string }
const fixture = (options: FixtureOptions = {}, rootOverride?: string, runRootOverride?: string): Fixture => {
  const root = rootOverride ?? tempRoot(), runRoot = runRootOverride ?? path.join(root, "runs"), cgroupRoot = path.join(root, "cgroups");
  fs.mkdirSync(runRoot, { recursive: true, mode: 0o700 });
  fs.mkdirSync(cgroupRoot, { recursive: true, mode: 0o700 });
  const id = options.id ?? RUN, actorId = options.actorId ?? ACTOR, activationId = options.activationId ?? ACTIVATION;
  const directory = path.join(runRoot, id), cgroupPath = `/user.slice/user-${process.getuid?.() ?? 1000}.slice/user@${process.getuid?.() ?? 1000}.service/batch.slice/${SCOPE}`;
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  const pid = options.pid ?? deadPid();
  fs.writeFileSync(path.join(directory, "status.json"), JSON.stringify({ id, name: "actor-run", status: options.status ?? "completed",
    runner: "pi", transport: "process", actorId, sessionId: String(pid), ...(options.birth ? { processStartTime: options.birth } : {}),
    exitCode: 0, finishedAt: 20 }));
  const taskActorId = options.taskActorId ?? actorId;
  fs.writeFileSync(path.join(directory, "task.txt"), [
    `Fabric activation: ${JSON.stringify({ actorId: taskActorId, activationId, batchIds: [activationId] })}`,
    "Fabric actor message:", JSON.stringify({ id: activationId }),
  ].join("\n"));
  fs.writeFileSync(path.join(directory, "unresolved-worker.json"), JSON.stringify({
    reason: options.markerReason ?? CLOSE_REASON(id), markedAt: 10, runId: id, transport: "process", sessionId: String(pid),
  }));
  if (options.settlement !== null) fs.writeFileSync(path.join(directory, "settlement.json"), JSON.stringify(options.settlement ?? {
    version: 1, runId: id, outcome: "completed", text: "saved actor answer",
  }));
  if (options.scopeRecorded !== false) recordProcessScope(directory, id, SCOPE, cgroupPath);
  if (options.scopePresent) fs.mkdirSync(path.join(cgroupRoot, cgroupPath.slice(1)), { recursive: true, mode: 0o700 });
  return { root, runRoot, cgroupRoot, directory, id, cgroupPath };
};
const operator = () => ({ uid: process.getuid?.() ?? null, user: "test-operator", pid: process.pid });
const runtimeRoot = (root: string, rootId: string) => {
  const meshRoot = path.join(root, "mesh"), residencyRoot = residentRoot(meshRoot, rootId), runRoot = path.join(residencyRoot, "runs");
  fs.mkdirSync(runRoot, { recursive: true, mode: 0o700 });
  const config: ResidentHostConfig = {
    format: 1, rootId, sessionId: rootId.slice("session:".length), cwd: root, projectRoot: root, meshRoot,
    actorRoot: path.join(root, "actors"), residencyRoot, fullCodeMode: false,
    agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 }, mesh: { ...DEFAULT_FABRIC_CONFIG.mesh },
    retention: { ...DEFAULT_FABRIC_CONFIG.retention }, workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
    fabricExtensionPath: path.resolve("dist/index.js"), piBinary: process.execPath, claudeBinary: process.execPath, vedaBinary: process.execPath,
  };
  fs.mkdirSync(meshRoot, { recursive: true, mode: 0o700 });
  fs.writeFileSync(path.join(residencyRoot, "config.json"), JSON.stringify(config), { mode: 0o600 });
  fs.writeFileSync(path.join(residencyRoot, "owner.json"), JSON.stringify({ format: 1, hostId: residentHostId(rootId),
    pid: process.pid, processStartTime: processStartTime(process.pid), token: "test-owner", startedAt: Date.now(), readyAt: Date.now() }), { mode: 0o600 });
  return { meshRoot, residencyRoot, runRoot, config };
};
const recordRootParticipant = (meshRoot: string, rootId: string, pid: number, startTime: string, updatedAt: number) =>
  writeParticipantFile(meshRoot, { key: rootParticipantKey(rootId), value: { id: rootId, rootId, kind: "root",
    ownerHostId: rootId, ownerIdentityId: rootId, status: "active",
    mainProcess: { pid, host: os.hostname(), startTime } }, version: 1, updatedAt,
    updatedBy: { id: rootId, name: "main", kind: "main" } });

describe.runIf(process.platform === "linux")("scope-close-timeout actor recovery", () => {
  it("recovers the saved answer and discharges custody only after the matching activation's delivery receipt", () => {
    const f = fixture();
    expect(scopeCgroupPathAbsent(f.cgroupPath, f.cgroupRoot)).toBe(true);
    expect(peekCloseTimeoutRun(f.directory, ACTOR, ACTIVATION, f.cgroupRoot)).toMatchObject({
      id: f.id, status: "completed", text: "saved actor answer",
    });
    expect(settleCloseTimeoutRun(f.directory, ACTOR, ACTIVATION, f.cgroupRoot)).toBeUndefined();
    expect(fs.existsSync(path.join(f.directory, "unresolved-worker.json"))).toBe(true);
    expect(fs.existsSync(path.join(f.directory, "settlement.json"))).toBe(true);
    noteCloseTimeoutDelivered(f.directory, f.id, ACTIVATION);
    expect(closeTimeoutDelivered(f.directory, f.id, ACTIVATION)).toBe(true);
    expect(settleCloseTimeoutRun(f.directory, ACTOR, ACTIVATION, f.cgroupRoot)).toMatchObject({
      id: f.id, status: "completed", text: "saved actor answer", closeTimeoutRecovered: true,
    });
    expect(fs.existsSync(path.join(f.directory, "unresolved-worker.json"))).toBe(false);
    for (const receipt of ["scope.json", "settlement.json", "close-timeout-delivered.json"]) {
      expect(fs.existsSync(path.join(f.directory, receipt)), `${receipt} remains as evidence`).toBe(true);
    }
    expect(settleCloseTimeoutRun(f.directory, ACTOR, ACTIVATION, f.cgroupRoot)).toBeUndefined();
  });

  it("vetoes each missing or contradictory fact", () => {
    const live = livePid();
    const sameBirth = processStartTime(live)!;
    expect(sameBirth).toBeDefined();
    const cases: Array<[string, FixtureOptions, string?]> = [
      ["live worker with matching recorded birth", { pid: live, birth: sameBirth }],
      ["live worker without a recorded birth", { pid: live }],
      ["cgroup still present", { scopePresent: true }],
      ["old run without scope or operator receipt", { scopeRecorded: false }],
      ["missing settlement", { settlement: null }],
      ["wrong settlement run id", { settlement: { version: 1, runId: "c".repeat(32), outcome: "completed", text: "x" } }],
      ["non-completed settlement", { settlement: { version: 1, runId: RUN, outcome: "failed", text: "x" } }],
      ["status not completed", { status: "running" }],
      ["task actor mismatch", { taskActorId: "d".repeat(32) }],
      ["wrong unresolved reason", {}, "a different unresolved reason"],
    ];
    for (const [label, options, reason] of cases) {
      const f = fixture({ ...options, ...(reason ? { markerReason: reason } : {}) });
      expect(peekCloseTimeoutRun(f.directory, ACTOR, ACTIVATION, f.cgroupRoot), label).toBeUndefined();
      expect(fs.existsSync(path.join(f.directory, "unresolved-worker.json")), label).toBe(true);
    }
    const wrongActor = fixture();
    expect(peekCloseTimeoutRun(wrongActor.directory, "e".repeat(32), ACTIVATION, wrongActor.cgroupRoot)).toBeUndefined();
    const wrongActivation = fixture();
    expect(peekCloseTimeoutRun(wrongActivation.directory, ACTOR, "different-activation", wrongActivation.cgroupRoot)).toBeUndefined();
  });

  it("an old run requires explicit authority after the owning Main is stale and gone, then records that proof", async () => {
    const root = tempRoot(), rootId = "session:dead-close-owner", runtime = runtimeRoot(root, rootId);
    const f = fixture({ scopeRecorded: false }, root, runtime.runRoot);
    recordRootParticipant(runtime.meshRoot, rootId, 2147483647, "1", Date.now() - 3 * ROOT_PARTICIPANT_FRESH_MS);
    expect(peekCloseTimeoutRun(f.directory, ACTOR, ACTIVATION, f.cgroupRoot)).toBeUndefined();
    const out: string[] = [], err: string[] = [];
    const code = await actorsCli(["recover-close-timeout", "--run-root", f.runRoot, "--run-id", f.id, "--scope-unit", SCOPE,
      "--authorized-by", "org", "--authority-ref", "https://github.com/Smarty-Pants-Inc/pi-fabric/issues/4313#issuecomment-1234"],
    { out: text => out.push(text), err: text => err.push(text) }, {
      platform: "linux", getuid: () => process.getuid!(),
    }, f.cgroupRoot);
    expect(code, err.join("")).toBe(0);
    const response = JSON.parse(out.join(""));
    expect(response).toMatchObject({ runId: f.id, scopeUnit: SCOPE,
      authorization: { authorizedBy: "org", authorityRef: "https://github.com/Smarty-Pants-Inc/pi-fabric/issues/4313#issuecomment-1234",
        rootProof: { rootId, participantResolution: "not-found", mainLiveness: expect.stringContaining("stale:") } },
      custody: "awaiting the restored activation's delivery and queue acknowledgement" });
    expect(JSON.parse(fs.readFileSync(path.join(f.directory, "close-timeout-operator.json"), "utf8"))).toMatchObject({
      version: 1, runId: f.id, actorId: ACTOR, activationId: ACTIVATION, scopeUnit: SCOPE,
      operator: { user: process.env.SUDO_USER ?? process.env.USER ?? "test-operator" },
      authorization: { authorizedBy: "org", authorityRef: "https://github.com/Smarty-Pants-Inc/pi-fabric/issues/4313#issuecomment-1234",
        rootProof: { rootId, participantResolution: "not-found", staleBoundMs: ROOT_PARTICIPANT_FRESH_MS } },
    });
    expect(peekCloseTimeoutRun(f.directory, ACTOR, ACTIVATION, f.cgroupRoot)).toMatchObject({ text: "saved actor answer" });
    expect(fs.existsSync(path.join(f.directory, "unresolved-worker.json"))).toBe(true); // receipt authorizes; normal queue ack discharges
  });

  it("refuses missing authority for a dead root and both operator and authorization overrides for a live root", async () => {
    const deadRoot = tempRoot(), deadId = "session:dead-auth-required", deadRuntime = runtimeRoot(deadRoot, deadId);
    const deadRun = fixture({ scopeRecorded: false }, deadRoot, deadRuntime.runRoot);
    recordRootParticipant(deadRuntime.meshRoot, deadId, 2147483647, "1", Date.now() - 3 * ROOT_PARTICIPANT_FRESH_MS);
    const deadErr: string[] = [];
    const deadCode = await actorsCli(["recover-close-timeout", "--run-root", deadRun.runRoot, "--run-id", deadRun.id, "--scope-unit", SCOPE],
      { out: () => {}, err: text => deadErr.push(text) }, { platform: "linux", getuid: () => process.getuid!() }, deadRun.cgroupRoot);
    expect(deadCode).toBe(1);
    expect(deadErr.join("")).toContain("--authorized-by and --authority-ref are required");
    expect(fs.existsSync(path.join(deadRun.directory, "close-timeout-operator.json"))).toBe(false);

    const liveRoot = tempRoot(), liveId = "session:live-auth-refused", liveRuntime = runtimeRoot(liveRoot, liveId);
    const liveRun = fixture({ scopeRecorded: false }, liveRoot, liveRuntime.runRoot);
    const ownerPid = livePid();
    recordRootParticipant(liveRuntime.meshRoot, liveId, ownerPid, processStartTime(ownerPid)!, Date.now());
    const args = ["recover-close-timeout", "--run-root", liveRun.runRoot, "--run-id", liveRun.id, "--scope-unit", SCOPE];
    const authError: string[] = [];
    const authCode = await actorsCli([...args, "--authorized-by", "lead-name", "--authority-ref",
      "https://github.com/Smarty-Pants-Inc/pi-fabric/issues/4313#issuecomment-1"],
    { out: () => {}, err: text => authError.push(text) }, { platform: "linux", getuid: () => process.getuid!() }, liveRun.cgroupRoot);
    expect(authCode).toBe(1);
    expect(authError.join("")).toContain("Dead-root authorization refused");
    const ownerError: string[] = [];
    const ownerCode = await actorsCli(args, { out: () => {}, err: text => ownerError.push(text) },
      { platform: "linux", getuid: () => process.getuid!() }, liveRun.cgroupRoot);
    expect(ownerCode).toBe(1);
    expect(ownerError.join("")).toContain("Live-root recovery is owner-only");
    expect(fs.existsSync(path.join(liveRun.directory, "close-timeout-operator.json"))).toBe(false);
  });

  it("does not let legacy stop-intent recovery paths bypass close-timeout scope proof", async () => {
    const f = fixture({ settlement: null });
    persistRunStopIntent(f.directory, "deadline");
    expect(peekEnodevRun(f.directory, ACTOR, ACTIVATION)).toBeUndefined();
    expect(settleLostEnodevRun(f.directory, ACTOR, new Set())).toBeUndefined();
    expect(await recoverDurableActorStop(f.runRoot, ACTOR, ACTIVATION, new AbortController().signal)).toBeUndefined();
    expect(fs.existsSync(path.join(f.directory, "unresolved-worker.json"))).toBe(true);
  });

  it("does not trust an operator-supplied unit while that cgroup exists or its path traverses a symlink", () => {
    const f = fixture({ scopeRecorded: false, scopePresent: true });
    expect(writeCloseTimeoutOperatorReceipt(f.directory, f.id, SCOPE, operator(), f.cgroupRoot)).toBeUndefined();
    expect(fs.existsSync(path.join(f.directory, "close-timeout-operator.json"))).toBe(false);
    expect(scopeUnitAbsent(SCOPE, f.cgroupRoot)).toBe(false);
    const real = path.join(f.root, "real-cgroup");
    fs.mkdirSync(real, { recursive: true });
    fs.symlinkSync(real, path.join(f.cgroupRoot, "redirect"));
    expect(scopeCgroupPathAbsent("/redirect/fabric-scope-abc123.scope", f.cgroupRoot)).toBe(false);
  });

  it("a live owner consumes the operator receipt without restart, delivers the held activation, and starts queued work", async () => {
    const root = tempRoot(), rootId = "session:live-close-owner", runtime = runtimeRoot(root, rootId);
    const runRoot = runtime.runRoot, actorRoot = path.join(root, "actors"), cgroupRoot = path.join(root, "cgroups");
    fs.mkdirSync(cgroupRoot, { recursive: true, mode: 0o700 });
    recordRootParticipant(runtime.meshRoot, rootId, process.pid, processStartTime(process.pid)!, Date.now());
    const workerPath = path.join(root, "worker.mjs");
    fs.writeFileSync(workerPath, `import fs from "node:fs";
const args = new Map(); for (let i=2;i<process.argv.length;i+=2) args.set(process.argv[i].slice(2),process.argv[i+1]);
const id=args.get("id"), actorId=args.get("actor-id"), statusFile=args.get("status-file"), text="completed actor answer", now=Date.now();
fs.writeFileSync(statusFile,JSON.stringify({id,name:args.get("name"),task:"synthetic actor task",status:"completed",runner:"pi",transport:"process",actorId,sessionId:String(process.pid),exitCode:0,startedAt:now,finishedAt:now,text,turns:1,toolCalls:0,usage:{input:1,output:1,cacheRead:0,cacheWrite:0,cost:0}}));
fs.writeFileSync(statusFile.replace("status.json","settlement.json"),JSON.stringify({version:1,runId:id,outcome:"completed",text}));\n`);
    const mesh = new MeshStore(runtime.meshRoot, 64 * 1024, 1000);
    const config = { ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent: 2, budgetUsd: 0, retainRuns: true };
    const agents = new AgentManager(root, config, { workerPath, runRoot, meshRoot: runtime.meshRoot });
    managers.push(agents);
    const originalLaunch = ProcessTransport.prototype.launch;
    let launches = 0;
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function (this: ProcessTransport, request) {
      const handle = await originalLaunch.call(this, request);
      launches++;
      if (launches !== 1) return handle;
      const lost = () => "Owned process worker did not confirm tree/native close within 7000ms";
      return { ...handle, lostContact: lost, stopDebt: lost };
    });
    const actorManager = new ActorManager("live-close-owner",
      { id: rootId, name: "main", kind: "main", sessionId: "live-close-owner" }, mesh,
      { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {}, {
        actorRoot, persistent: true, meshCursorPath: path.join(actorRoot, "mesh-cursor.json"),
      });
    actors.push(actorManager);
    const actor = await actorManager.create({ name: "close-timeout", instructions: "Respond with a short message.", topics: ["close-timeout"], responseMode: "text", coalesce: false });
    actorManager.tell(actor.id, "first activation");
    await vi.waitFor(() => {
      const id = fs.readdirSync(runRoot).find(name => /^[a-f0-9]{32}$/.test(name));
      expect(id).toBeDefined();
      const directory = path.join(runRoot, id!);
      expect(JSON.parse(fs.readFileSync(path.join(directory, "status.json"), "utf8")).status).toBe("completed");
      expect(fs.existsSync(path.join(directory, "unresolved-worker.json"))).toBe(true);
    }, { timeout: 15_000, interval: 20 });
    const firstRunId = fs.readdirSync(runRoot).find(name => /^[a-f0-9]{32}$/.test(name))!;
    const firstRun = path.join(runRoot, firstRunId);
    expect(fs.existsSync(path.join(firstRun, "scope.json"))).toBe(false); // legacy run: no structured scope receipt
    await vi.waitFor(() => expect(agents.runningCount()).toBe(1), { timeout: 10_000, interval: 20 });
    for (let index = 1; index <= 4; index++) actorManager.tell(actor.id, `queued activation ${index}`);
    await new Promise(resolve => setTimeout(resolve, 250));
    expect(launches).toBe(1); // held first activation prevents all four queued activations from launching
    const output: string[] = [], error: string[] = [];
    vi.stubEnv("PI_FABRIC_AGENT_NAME", "");
    vi.stubEnv("PI_FABRIC_PARENT_RUN", "");
    vi.stubEnv("PI_FABRIC_ACTOR_ID", "");
    const code = await actorsCli(["recover-close-timeout", "--run-root", runRoot, "--run-id", firstRunId, "--scope-unit", SCOPE],
      { out: text => output.push(text), err: text => error.push(text) }, { platform: "linux", getuid: () => process.getuid!() }, cgroupRoot);
    expect(code, error.join("")).toBe(0);
    await vi.waitFor(() => expect(launches).toBe(5), { timeout: 15_000, interval: 20 });
    await vi.waitFor(() => expect(actorManager.status(actor.id).status).toBe("idle"), { timeout: 15_000, interval: 20 });
    expect(fs.existsSync(path.join(firstRun, "unresolved-worker.json"))).toBe(false);
    expect(fs.existsSync(path.join(firstRun, "close-timeout-delivered.json"))).toBe(true);
    const firstActivation = actorActivationFromRun(firstRun);
    expect(firstActivation).toBeDefined();
    expect(JSON.parse(fs.readFileSync(path.join(firstRun, "close-timeout-delivered.json"), "utf8"))).toMatchObject({
      runId: firstRunId, activationId: firstActivation!.activationId,
    });
    const outputs = mesh.read({ after: 0, limit: 1_000 }).filter(event =>
      event.topic === "fabric.actor.output" && event.from.id === actor.id && event.text === "completed actor answer");
    expect(outputs).toHaveLength(5); // one recovered delivery and all four queued activations, each published once
    expect(actorManager.status(actor.id)).toMatchObject({ lastRunId: expect.any(String), status: "idle" });
  }, 60_000);

  it("a receipt written while the old resident is alive is consumed once by the new release after restore", async () => {
    const root = tempRoot(), rootId = "session:restart-close-owner", runtime = runtimeRoot(root, rootId);
    const cgroupRoot = path.join(root, "cgroups");
    fs.mkdirSync(cgroupRoot, { recursive: true, mode: 0o700 });
    const oldResident = spawn(process.execPath, ["-e", "setTimeout(() => {}, 60000)"], { stdio: "ignore" });
    children.push(oldResident);
    await vi.waitFor(() => expect(processStartTime(oldResident.pid!)).toBeDefined());
    const f = fixture({ scopeRecorded: true }, root, runtime.runRoot);
    const id = f.id;
    const directory = f.directory;
    const deadMainPid = 2147483647;
    recordRootParticipant(runtime.meshRoot, rootId, deadMainPid, "1", Date.now() - 3 * ROOT_PARTICIPANT_FRESH_MS);
    expect(processStartTime(oldResident.pid!)).toBeDefined(); // old resident remains on the old code path
    const out: string[] = [], err: string[] = [];
    const code = await actorsCli(["recover-close-timeout", "--run-root", runtime.runRoot, "--run-id", id, "--scope-unit", SCOPE,
      "--authorized-by", "org", "--authority-ref", "https://github.com/Smarty-Pants-Inc/pi-fabric/issues/4313#issuecomment-1234"],
      { out: text => out.push(text), err: text => err.push(text) }, { platform: "linux", getuid: () => process.getuid!() }, cgroupRoot);
    expect(code, err.join("")).toBe(0);
    expect(fs.existsSync(path.join(directory, "close-timeout-operator.json"))).toBe(true);
    expect(fs.existsSync(path.join(directory, "unresolved-worker.json"))).toBe(true);
    oldResident.kill("SIGTERM");
    await new Promise<void>(resolve => oldResident.once("exit", () => resolve()));
    children.splice(children.indexOf(oldResident), 1);

    const restored = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 },
      { workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: runtime.runRoot });
    managers.push(restored);
    const spawned = vi.spyOn(restored, "spawn").mockRejectedValue(new Error("rerun forbidden"));
    try {
      const recovered = await restored.run({ task: "restore", actorId: ACTOR, activationId: ACTIVATION });
      expect(recovered).toMatchObject({ id, status: "completed", text: "saved actor answer", enodevRecovered: true, closeTimeoutRecovered: true });
      expect(spawned).not.toHaveBeenCalled();
      expect(restored.noteEnodevDelivered(id)).toBe(true);
      expect(restored.commitEnodevRecovery(id)).toBe(true);
      expect(fs.existsSync(path.join(directory, "unresolved-worker.json"))).toBe(false);
      expect(fs.existsSync(path.join(directory, "close-timeout-delivered.json"))).toBe(true);
      await expect(restored.run({ task: "queued after restore", actorId: ACTOR, activationId: "activation-2" })).rejects.toThrow("rerun forbidden");
      expect(spawned).toHaveBeenCalledOnce();
    } finally { await restored.close(); managers.splice(managers.indexOf(restored), 1); }
  }, 60_000);
});
