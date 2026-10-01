import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { spawn } from "node:child_process";
import { describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { ResidencyClient } from "../src/residency/client.js";
import { residentProcessAlive } from "../src/residency/process-identity.js";
import { handoverPath, handoverCustodyPath, readHandoverJson, type ResidentHandoverState } from "../src/residency/handover.js";
import type { ResidentHostConfig, ResidentHostOwner } from "../src/residency/protocol.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import { launchLog, stopAllOwned } from "./helpers/owned-processes.js";

const sleep = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
async function until(predicate: () => boolean, ms = 15_000): Promise<void> {
  const deadline = Date.now() + ms;
  while (!predicate()) { if (Date.now() >= deadline) throw Error("native release deferral deadline"); await sleep(20); }
}
describe.skipIf(process.platform !== "linux" || !fs.existsSync("dist/residency/launcher.js"))("client to compiled launcher/host pre-exit deferral (Pi wire substitute)", () => {
  it.each(["broken-script-B", "disposed-Main", "crash-at-first-publication"] as const)("keeps acknowledged actor queue served on A for %s with distinct bundled Main/runtime", async mode => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-real-host-defer-"));
    const ownership = launchLog(root);
    let summary: Record<string, unknown> | undefined;
    const exec = process.execPath; const runtime = fs.realpathSync(exec);
    const bundled = path.join(root, "pi"); fs.copyFileSync(runtime, bundled);
    const successorDir = path.join(root, "generic"); fs.mkdirSync(successorDir);
    const successorRuntime = path.join(successorDir, "node"); fs.copyFileSync(runtime, successorRuntime); fs.chmodSync(successorRuntime, 0o700);
    const [a, b] = ["A", "B"].map(name => {
      const release = path.join(root, name); fs.mkdirSync(release);
      fs.cpSync("dist", path.join(release, "dist"), { recursive: true });
      fs.writeFileSync(path.join(release, "package.json"), JSON.stringify({ type: "module" }));
      fs.symlinkSync(path.resolve("node_modules"), path.join(release, "node_modules"), "dir");
      fs.writeFileSync(path.join(release, "dist/worker.js"), `await import(${JSON.stringify(pathToFileURL(path.resolve("tests/fixtures/fake-worker.mjs")).href)});`);
      return release;
    }) as [string, string];
    // Crash after the FIRST durable handover publication, before a possible second
    // cancellation. Old code exposes preparing; deferred code must publish cancelled directly.
    const crashReceipt = path.join(root, 'first-handover-publication.json');
    if (mode === 'crash-at-first-publication') {
      const preload = path.join(root, 'crash-handover-preload.mjs');
      fs.writeFileSync(preload, `import fs from 'node:fs'; import path from 'node:path';
const remove = fs.rmSync;
fs.rmSync = function(file, options) {
 const config = process.env.PI_FABRIC_RESIDENT_CONFIG;
 if (config) {
  const handover = path.join(path.dirname(config), 'handover.json');
  if (String(file).startsWith(handover + '.') && String(file).endsWith('.tmp') && fs.existsSync(handover)) {
   const state = JSON.parse(fs.readFileSync(handover, 'utf8'));
   fs.writeFileSync(${JSON.stringify(crashReceipt)}, JSON.stringify({phase:state.phase,pid:process.pid}));
   process.kill(process.pid, 'SIGKILL');
  }
 }
 return remove.call(this, file, options);
};`);
      ownership.env.NODE_OPTIONS += ` --import=${pathToFileURL(preload).href}`;
    }
    const identity = { id: "session:pre-exit", name: "Main", kind: "main" as const, sessionId: "pre-exit" };
    const config: ResidentHostConfig = { format: 1, rootId: identity.id, sessionId: identity.sessionId, cwd: root, projectRoot: root,
      meshRoot: path.join(root, "mesh"), actorRoot: path.join(root, "actors"), residencyRoot: path.join(root, "resident"), fullCodeMode: true,
      agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, notifyOnComplete: false, retainRuns: true, nice: 19 },
      mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, retention: DEFAULT_FABRIC_CONFIG.retention,
      workerPath: path.join(a, "dist/worker.js"), fabricExtensionPath: path.join(a, "dist/index.js"),
      piBinary: path.resolve("tests/fixtures/resident-host-wire-pi.mjs"), claudeBinary: "missing-claude", vedaBinary: "missing-veda",
      piModels: { available: [{ provider: "fixture", id: "A" }], aliases: {}, defaultModel: "fixture/A" } };
    fs.mkdirSync(config.residencyRoot);
    const configPath = path.join(config.residencyRoot, "config.json"); fs.writeFileSync(configPath, JSON.stringify(config));
    const mesh = new MeshStore(config.meshRoot, config.mesh.maxEventBytes, config.mesh.maxReadEvents);
    const participants = new ParticipantDirectory(mesh, { enabled: true, hostId: identity.id, rootId: identity.id, identity });
    participants.registerSource(() => [{ format: 1, id: identity.id, kind: "root", rootId: identity.id, ownerHostId: identity.id, ownerIdentityId: identity.id,
      name: "Main", status: "idle", residency: "session", runner: "pi", transport: "host", capabilities: ["fabric", "followUp"], cwd: root,
      sessionId: identity.sessionId, startedAt: Date.now(), updatedAt: Date.now(), controlProtocol: "v1" }]);
    await participants.start();
    const mainAgent = { id: identity.id, local: true } as FabricMainAgentTarget;
    const clientA = new ResidencyClient({ config, mesh, participants, mainAgent, hostPath: path.join(a, "dist/residency/launcher.js") });
    const targetConfig = { ...config, workerPath: path.join(b, "dist/worker.js"), fabricExtensionPath: path.join(b, "dist/index.js") };
    if (mode === "broken-script-B") {
      const broken = path.join(b, "pi-cli.mjs"); fs.writeFileSync(broken, "throw Error('normal B loader failure');"); targetConfig.piBinary = broken;
    }
    const clientB = new ResidencyClient({ config: targetConfig, mesh, participants, mainAgent, hostPath: path.join(b, "dist/residency/launcher.js") });
    const control = new FabricControlPlane(mesh, identity, { enabled: true, hostId: identity.id, pollMs: 20, acknowledgementTimeoutMs: 5_000 });
    control.start(() => ({ accepted: false }));
    const launcher = spawn(runtime, [path.join(a, "dist/residency/launcher.js"), "--config", configPath], { env: { ...process.env, ...ownership.env, PI_FABRIC_NODE_BINARY: runtime }, stdio: ["ignore", "ignore", "pipe"] });
    let stderr = ""; launcher.stderr.on("data", data => { stderr += data; });
    const exited = new Promise<void>((resolve, reject) => { launcher.once("error", reject); launcher.once("close", () => resolve()); });
    const state = () => readHandoverJson<ResidentHandoverState>(handoverPath(config.residencyRoot));
    const ownerPath = path.join(config.residencyRoot, "owner.json");
    try {
      await until(() => !!readHandoverJson<ResidentHostOwner>(ownerPath));
      const owner = readHandoverJson<ResidentHostOwner>(ownerPath)!;
      expect(owner.handover?.launcher.pid).toBe(launcher.pid);
      const actor = await clientA.createActor({ name: "preserved-actor", instructions: "Reply", residency: "durable", tools: [], responseMode: "text", delivery: "mailbox" });
      await control.request(clientA.hostId, actor.id, "followUp", { message: "LIVE_WITH_PROGRESS" }, clientA.hostId);
      await until(() => !!participants.get(actor.id, Date.now(), { fresh: true })?.actorRun);
      const queued = await control.request(clientA.hostId, actor.id, "followUp", { message: "queued-before-intent" }, clientA.hostId);
      expect(queued.acknowledged).toBe(true);
      process.execPath = bundled; vi.stubEnv("PI_FABRIC_NODE_BINARY", successorRuntime);
      if (mode === 'crash-at-first-publication') {
        const releaseRequest = clientB.reconcileRelease().catch(error => error);
        await until(() => fs.existsSync(crashReceipt));
        await exited; // The original launcher has no custody and must exit, too.
        await clientB.close(); await releaseRequest;
        expect(JSON.parse(fs.readFileSync(crashReceipt, 'utf8')).phase).toBe('cancelled');
        process.execPath = exec; vi.unstubAllEnvs();
        vi.stubEnv('NODE_OPTIONS', ownership.env.NODE_OPTIONS);
        vi.stubEnv('PI_FABRIC_TEST_LAUNCH_LOG', ownership.env.PI_FABRIC_TEST_LAUNCH_LOG);
        vi.stubEnv('PI_FABRIC_NODE_BINARY', runtime);
        const recovered = await clientA.ensureHost();
        expect(recovered.token).not.toBe(owner.token);
        expect(recovered.releaseRoot).toBe(a);
      } else {
        await clientB.reconcileRelease();
      }
      await until(() => ["cancelled", "custody", "released", "starting"].includes(state()?.phase ?? ""));
      expect(state()?.plan.target.runtime).toBe(successorRuntime);
      expect(state()?.plan.target.runtime).not.toBe(bundled);
      expect(state()?.phase).toBe("cancelled");
      if (mode === "disposed-Main") await clientB.close();
      process.execPath = exec; vi.unstubAllEnvs();
      const registryFile = path.join(config.actorRoot, "actors.json");
      const actorMessages = (): Array<{ direction: string; id: string }> =>
        JSON.parse(fs.readFileSync(registryFile, "utf8")).actors.find((record: { id: string }) => record.id === actor.id)?.messages ?? [];
      await until(() => actorMessages().filter(message => message.direction === "out").length === 2);
      const messages = actorMessages();
      expect(messages.filter(message => message.direction === "in" && message.id === queued.messageId)).toHaveLength(1);
      expect((await clientA.actorStatus(actor.id)).id).toBe(actor.id);
      const serving = readHandoverJson<ResidentHostOwner>(ownerPath)!;
      if (mode === 'crash-at-first-publication') expect(serving.token).not.toBe(owner.token);
      else expect(serving.token).toBe(owner.token);
      expect(residentProcessAlive(serving.pid, serving.processStartTime)).toBe(true);
      expect(fs.existsSync(handoverCustodyPath(config.residencyRoot, state()!.plan.id))).toBe(false);
      expect(mesh.read({ topic: "host.reloaded" })).toHaveLength(0);
      const traces = fs.readFileSync(path.join(config.residencyRoot, "launcher.log"), "utf8").trim().split("\n").map(line => JSON.parse(line));
      const successorSpawns = traces.filter(trace => trace.event === "child-spawned" && (trace.kind === "target" || trace.kind === "fallback"));
      expect(successorSpawns).toHaveLength(0);
      summary = { case: mode, actorId: actor.id, acknowledgedQueuedId: queued.messageId,
        queuedInputCount: messages.filter(message => message.direction === "in" && message.id === queued.messageId).length,
        completedOutputs: messages.filter(message => message.direction === "out").length, ownerPid: owner.pid, launcherPid: launcher.pid,
        ownerTokenUnchanged: readHandoverJson<ResidentHostOwner>(ownerPath)?.token === owner.token,
        targetRuntime: state()?.plan.target.runtime, bundledMain: bundled, phase: state()?.phase,
        successorSpawns: successorSpawns.length, custodyPublished: false, reloadedEvents: mesh.read({ topic: "host.reloaded" }).length,
        firstPublication: mode === "crash-at-first-publication" ? JSON.parse(fs.readFileSync(crashReceipt, "utf8")) : undefined,
        recoveryToken: mode === "crash-at-first-publication" ? serving.token : undefined,
        recoveryPid: mode === "crash-at-first-publication" ? serving.pid : undefined,
        recoveryRelease: mode === "crash-at-first-publication" ? serving.releaseRoot : undefined };
    } catch (error) { throw new Error(`${error instanceof Error ? error.stack : error}; state: ${JSON.stringify(state())}; launcher stderr: ${stderr}`); }
    finally {
      process.execPath = exec; vi.unstubAllEnvs();
      await clientA.close(); await clientB.close(); await control.close(); await participants.close();
      if (launcher.exitCode === null && launcher.signalCode === null) launcher.kill("SIGTERM");
      await exited;
      const liveOwned = () => ownership.owned().filter(owned => {
        try {
          const stat = fs.readFileSync(`/proc/${owned.pid}/stat`, "utf8");
          return stat.slice(stat.lastIndexOf(")") + 2).split(" ")[0] !== "Z";
        } catch { return false; }
      });
      const remaining = liveOwned();
      if (remaining.length) await stopAllOwned(remaining, 2_000, 5_000);
      const stillLive = liveOwned();
      if (summary) {
        summary.ownedProcessesRemainingAfterCleanup = stillLive.length;
        const output = process.env.FABRIC_RELEASE_PROOF_OUT;
        if (output) { fs.mkdirSync(output, { recursive: true }); fs.writeFileSync(path.join(output, `${mode}.json`), JSON.stringify(summary, null, 2)); }
      }
      expect(stillLive).toEqual([]);
      fs.rmSync(root, { recursive: true, force: true });
    }
  }, 35_000);
});
