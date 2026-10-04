import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import http from "node:http";
import { pathToFileURL } from "node:url";
import { spawn } from "node:child_process";
import { describe, expect, it } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ResidentActorClient } from "../src/residency/actor-client.js";
import { ResidencyClient } from "../src/residency/client.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import { residentRoot, type ResidentHostConfig, type ResidentHostOwner } from "../src/residency/protocol.js";
import { watchResidentChild } from "../src/residency/child-lifetime.js";
import { MeshStore } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { launchLog, same } from "./helpers/owned-processes.js";

const sleep = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
const live = (pid: number): boolean => {
  try { const text = fs.readFileSync(`/proc/${pid}/stat`, "utf8"); return text.slice(text.lastIndexOf(")") + 2).split(" ")[0] !== "Z"; }
  catch { return false; }
};
const until = async (test: () => boolean, ms = 30_000) => {
  const end = Date.now() + ms;
  while (!test()) { if (Date.now() >= end) throw new Error("native serving watchdog deadline"); await sleep(25); }
};

describe.skipIf(process.platform !== "linux")("serving resident watchdog with real native Pi", () => {
  it("F1 an already-admitted launcher with delayed native Pi refuses custody before serving or replaying escaped work", async () => {
    const launcherPath = path.resolve("dist/residency/launcher.js");
    const piBinary = fs.realpathSync(path.resolve("node_modules/@earendil-works/pi-coding-agent/dist/cli.js"));
    expect(fs.existsSync(launcherPath), "fresh build required").toBe(true);
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "serving-watchdog-native-")));
    const owned = launchLog(root);
    const agentDir = path.join(root, "agent"); fs.mkdirSync(agentDir);
    const identity = { id: "session:serving-watchdog", name: "Main", kind: "main" as const, sessionId: "serving-watchdog" };
    const meshRoot = path.join(root, "mesh");
    const helperFile = path.join(root, "escaped-helper.mjs");
    const intermediateFile = path.join(root, "intermediate.mjs");
    const helperReceipt = path.join(root, "escaped-helper.json");
    const releaseHelper = path.join(root, "release-helper");
    fs.writeFileSync(helperFile, `import fs from 'node:fs';
fs.writeFileSync(${JSON.stringify(helperReceipt)},JSON.stringify({pid:process.pid}));
setInterval(()=>{if(fs.existsSync(${JSON.stringify(releaseHelper)}))process.exit(0);},25);`);
    fs.writeFileSync(intermediateFile, `import {spawn} from 'node:child_process';import fs from 'node:fs';
fs.writeFileSync(${JSON.stringify(path.join(root, "intermediate.json"))},JSON.stringify({pid:process.pid}));
const helper=spawn(process.execPath,[${JSON.stringify(helperFile)}],{detached:true,stdio:'ignore'});helper.unref();`);
    const stallFlag = path.join(root, "stall-host");
    const stalledReceipt = path.join(root, "host-stalled.json");
    const resumeFlag = path.join(root, "resume-host");
    const stallPreload = path.join(root, "stall-native-host.mjs");
    // Only the real native resident CLI gets this fault; activation workers/Pis
    // keep executing. No owner/lease/actor/checkpoint bytes are manufactured.
    fs.writeFileSync(stallPreload, `import fs from 'node:fs';import {spawn} from 'node:child_process';
if(process.argv.includes(${JSON.stringify(path.resolve("dist/residency/pi-entry.js"))})) {
 const intermediate=spawn(process.execPath,[${JSON.stringify(intermediateFile)}],{stdio:'ignore'});intermediate.unref();
 const timer=setInterval(()=>{
  if(!fs.existsSync(${JSON.stringify(stallFlag)}))return;
  clearInterval(timer);fs.writeFileSync(${JSON.stringify(stalledReceipt)},JSON.stringify({pid:process.pid}));
  const wait=new Int32Array(new SharedArrayBuffer(4));
  while(!fs.existsSync(${JSON.stringify(resumeFlag)}))Atomics.wait(wait,0,0,100);
 },20);
}`);
    const delayedReceipt = path.join(root, "delayed-native-child.json");
    const releaseDelayed = path.join(root, "release-delayed-child");
    const delayPreload = path.join(root, "delay-native-child.mjs");
    // Pause only the second launcher's actual native Pi, after CLI preflight and
    // spawn but before extension loading. No production admission seam is used.
    fs.writeFileSync(delayPreload, `import fs from 'node:fs';
if(process.argv.includes(${JSON.stringify(path.resolve("dist/residency/pi-entry.js"))})) {
 fs.writeFileSync(${JSON.stringify(delayedReceipt)},JSON.stringify({pid:process.pid}));
 const wait=new Int32Array(new SharedArrayBuffer(4));
 while(!fs.existsSync(${JSON.stringify(releaseDelayed)}))Atomics.wait(wait,0,0,100);
}`);
    let inferences = 0;
    const pending: http.ServerResponse[] = [];
    const server = http.createServer((request, response) => {
      request.resume();
      request.on("end", () => { inferences++; pending.push(response); });
    });
    await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
    fs.writeFileSync(path.join(agentDir, "models.json"), JSON.stringify({ providers: { "watchdog-offline": {
      baseUrl: `http://127.0.0.1:${(server.address() as { port: number }).port}/v1`, apiKey: "offline-fixture-only", api: "openai-completions",
      models: [{ id: "activation", name: "activation", reasoning: false, input: ["text"], contextWindow: 200000, maxTokens: 4096,
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } }],
    } } }));
    fs.writeFileSync(path.join(agentDir, "settings.json"), JSON.stringify({ enableInstallTelemetry: false, compaction: { enabled: false }, retry: { enabled: false } }));
    fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({ fullCodeMode: true,
      agents: { notifyOnComplete: false, retainRuns: true, nice: 19, sessionExport: false }, mesh: { enabled: true, actorPollMs: 20 },
      mcp: { enabled: false }, memory: { enabled: false }, jev: { enabled: false }, residency: { enabled: false }, prewalk: { enabled: false, alwaysRearm: false } }));
    const config: ResidentHostConfig = { format: 1, rootId: identity.id, sessionId: identity.sessionId, cwd: root, projectRoot: root,
      meshRoot, actorRoot: path.join(root, "actors"), residencyRoot: residentRoot(meshRoot, identity.id), fullCodeMode: true,
      agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, notifyOnComplete: false, retainRuns: true, nice: 19, sessionExport: false, timeoutMs: 120_000 },
      mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, retention: DEFAULT_FABRIC_CONFIG.retention,
      workerPath: path.resolve("dist/worker.js"), fabricExtensionPath: path.resolve("dist/index.js"),
      piBinary, claudeBinary: "missing-claude", vedaBinary: "missing-veda",
      piModels: { available: [{ provider: "watchdog-offline", id: "activation" }], aliases: {}, defaultModel: "watchdog-offline/activation" },
      watchdog: { intervalMs: 100, stallMs: 6_000, coldStartMs: 0 } };
    fs.mkdirSync(config.residencyRoot, { recursive: true });
    const configPath = path.join(config.residencyRoot, "config.json"); fs.writeFileSync(configPath, JSON.stringify(config));
    const mesh = new MeshStore(meshRoot, config.mesh.maxEventBytes, config.mesh.maxReadEvents);
    const participants = new ParticipantDirectory(mesh, { enabled: true, hostId: identity.id, rootId: identity.id, identity });
    participants.registerSource(() => [{ format: 1, id: identity.id, kind: "root", rootId: identity.id, ownerHostId: identity.id, ownerIdentityId: identity.id,
      name: identity.name, status: "idle", residency: "session", runner: "pi", transport: "host", capabilities: ["fabric", "followUp"], cwd: root,
      sessionId: identity.sessionId, startedAt: Date.now(), updatedAt: Date.now(), controlProtocol: "v1" }]);
    await participants.start();
    const control = new FabricControlPlane(mesh, identity, { enabled: true, hostId: identity.id, pollMs: 20, acknowledgementTimeoutMs: 5_000 });
    control.start(() => ({ accepted: false }));
    const client = new ResidentActorClient(meshRoot, identity.id);
    const launcher = spawn(process.execPath, [launcherPath, "--config", configPath], { stdio: ["ignore", "ignore", "pipe"], env: {
      ...process.env, ...owned.env, NODE_OPTIONS: `${owned.env.NODE_OPTIONS} --import=${pathToFileURL(stallPreload).href}`,
      PI_OFFLINE: "1", PI_CODING_AGENT_DIR: agentDir, PI_FABRIC_MESH_ROOT: meshRoot,
      PI_FABRIC_PROJECT_ROOT: root, PI_FABRIC_RUN_ROOT: path.join(root, "runs"), PI_FABRIC_AGENT_DIR: path.join(root, "exports"),
    } });
    const lifetime = watchResidentChild(launcher); let stderr = "";
    launcher.stderr!.on("data", chunk => { stderr = `${stderr}${chunk}`.slice(-6000); });
    launcher.on("error", () => {});
    const traces = () => {
      try { return fs.readFileSync(path.join(config.residencyRoot, "launcher.log"), "utf8").trim().split("\n").map(line => JSON.parse(line)); }
      catch { return []; }
    };
    const ownerFile = path.join(config.residencyRoot, "owner.json");
    let delayedLifetime: ReturnType<typeof watchResidentChild> | undefined;
    let delayedPid: number | undefined;
    try {
      await until(() => fs.existsSync(ownerFile) && fs.existsSync(path.join(config.residencyRoot, "maintenance-ready.json")));
      const owner: ResidentHostOwner = JSON.parse(fs.readFileSync(ownerFile, "utf8"));
      expect(owned.owned().find(record => record.pid === owner.pid)?.argv).toContain(path.resolve("dist/residency/pi-entry.js"));
      expect(owner.handover?.launcher.pid).toBe(launcher.pid);
      await until(() => fs.existsSync(helperReceipt));
      const helper = JSON.parse(fs.readFileSync(helperReceipt, "utf8"));
      await until(() => {
        const stat = fs.readFileSync(`/proc/${helper.pid}/stat`, "utf8"); const fields = stat.slice(stat.lastIndexOf(")") + 2).split(" ");
        const intermediate = JSON.parse(fs.readFileSync(path.join(root, "intermediate.json"), "utf8"));
        return !live(intermediate.pid) && Number(fields[1]) !== intermediate.pid && Number(fields[1]) !== owner.pid && Number(fields[3]) === helper.pid;
      });
      const actor = await client.createActor({ name: "watchdog-in-flight", instructions: "Wait for the offline response.", residency: "durable",
        model: "watchdog-offline/activation", transport: "process", tools: [], extensions: false, delivery: "mailbox", responseMode: "text" });
      await until(() => !!participants.get(actor.id, Date.now(), { fresh: true }));
      const admitted = await control.request(owner.hostId, actor.id, "followUp", { message: "keep this activation in flight" }, owner.hostId);
      expect(admitted.acknowledged).toBe(true);
      await until(() => inferences === 1 && !!participants.get(actor.id, Date.now(), { fresh: true })?.actorRun);
      const queued = await control.request(owner.hostId, actor.id, "followUp", { message: "queued work must not be replayed by a second host" }, owner.hostId);
      expect(queued.acknowledged).toBe(true);
      await until(() => participants.get(actor.id, Date.now(), { fresh: true })?.actorQueued === 1);
      // Admit a second real launcher while custody is absent and A still owns
      // host.lock. Its native child must not reach admission until A has exited.
      expect(fs.existsSync(path.join(config.residencyRoot, "watchdog-custody.json"))).toBe(false);
      const delayed = spawn(process.execPath, [launcherPath, "--config", configPath], { stdio: "ignore", env: {
        ...process.env, ...owned.env, NODE_OPTIONS: `${owned.env.NODE_OPTIONS} --import=${pathToFileURL(delayPreload).href}`,
        PI_OFFLINE: "1", PI_CODING_AGENT_DIR: agentDir, PI_FABRIC_MESH_ROOT: meshRoot,
        PI_FABRIC_PROJECT_ROOT: root, PI_FABRIC_RUN_ROOT: path.join(root, "runs"), PI_FABRIC_AGENT_DIR: path.join(root, "exports"),
      } });
      delayedLifetime = watchResidentChild(delayed); delayed.on("error", () => {});
      await until(() => fs.existsSync(delayedReceipt) && traces().filter(row => row.event === "child-spawned").length === 2);
      delayedPid = JSON.parse(fs.readFileSync(delayedReceipt, "utf8")).pid;
      expect(live(delayedPid!)).toBe(true);
      expect(fs.existsSync(path.join(config.residencyRoot, "watchdog-custody.json"))).toBe(false);
      fs.writeFileSync(stallFlag, "stall");
      await until(() => fs.existsSync(stalledReceipt));
      expect(JSON.parse(fs.readFileSync(stalledReceipt, "utf8")).pid).toBe(owner.pid);
      await until(() => traces().some(row => row.event === "watchdog-deferred" && row.proofCheck), 65_000);
      expect(live(owner.pid)).toBe(false); expect(live(helper.pid)).toBe(true);
      expect(traces().filter(row => row.event === "child-spawned")).toHaveLength(2);
      expect(inferences).toBe(1); expect(lifetime.exited).toBe(false);
      const readyBefore = fs.readFileSync(path.join(config.residencyRoot, "maintenance-ready.json"), "utf8");
      // A is dead, custody is published, and the escaped helper is still live.
      // Release the child which passed the old unfenced preflight before custody.
      fs.writeFileSync(releaseDelayed, "release");
      await until(() => delayedLifetime!.exited);
      await delayedLifetime.exit;
      expect(live(delayedPid!)).toBe(false); expect(live(helper.pid)).toBe(true);
      // Prove it reached the actual host fence, not an earlier launcher refusal.
      expect(JSON.parse(fs.readFileSync(path.join(config.residencyRoot, "host.lock"), "utf8")).pid).toBe(delayedPid);
      expect(fs.readFileSync(path.join(config.residencyRoot, "maintenance-ready.json"), "utf8"), "delayed child must not publish readiness").toBe(readyBefore);
      expect(inferences, "delayed child must not replay acknowledged work").toBe(1);
      expect(JSON.parse(fs.readFileSync(ownerFile, "utf8")).token).toBe(owner.token);
      const refusal = JSON.parse(fs.readFileSync(path.join(config.residencyRoot, "error.json"), "utf8"));
      expect(refusal.error).toContain("watchdog custody");
      expect(refusal.launcherPid).toBe(delayed.pid);
      expect(fs.existsSync(path.join(config.residencyRoot, "watchdog-custody.json"))).toBe(true);
      const restartingClient = new ResidencyClient({ config, mesh, participants,
        mainAgent: { id: identity.id, local: true } as FabricMainAgentTarget });
      try { await expect(restartingClient.ensureHost()).rejects.toThrow("watchdog custody"); }
      finally { await restartingClient.close(); }
      // A new CLI also refuses this root; custody does not expire on old PID exit.
      const duplicate = spawn(process.execPath, [launcherPath, "--config", configPath], { stdio: "ignore", env: { ...process.env, ...owned.env } });
      const duplicateLifetime = watchResidentChild(duplicate); duplicate.on("error", () => {});
      await duplicateLifetime.exit;
      expect(traces().filter(row => row.event === "child-spawned")).toHaveLength(2); expect(inferences).toBe(1);
      await sleep(1100);
      expect(traces().filter(row => row.event === "child-spawned")).toHaveLength(2); expect(inferences).toBe(1);
      fs.writeFileSync(releaseHelper, "release"); await until(() => !live(helper.pid)); await sleep(1100);
      expect(traces().filter(row => row.event === "child-spawned")).toHaveLength(2); expect(inferences).toBe(1);
      expect(fs.existsSync(path.join(config.residencyRoot, "wedges", "latest.json"))).toBe(false);
      console.info("F1 real serving watchdog proof", JSON.stringify({ nativePi: piBinary, launcher: launcher.pid, oldHost: owner.pid,
        escapedHelper: helper.pid, delayedHost: delayedPid, delayedChildRefused: true, actor: actor.id, queuedMessage: queued.messageId, inferences, servingAttempts: 1, nativeAttempts: 2, automaticRespawn: "deferred" }));
    } catch (error) {
      let childLog = ""; try { childLog = fs.readFileSync(path.join(config.residencyRoot, "child-stderr.log"), "utf8").slice(-10000); } catch { /* not started */ }
      throw new Error(`${error instanceof Error ? error.stack : error}\nlauncher: ${stderr}\nchild: ${childLog}\ntraces: ${JSON.stringify(traces().slice(-8))}`);
    } finally {
      fs.writeFileSync(resumeFlag, "resume"); fs.writeFileSync(releaseHelper, "release"); fs.writeFileSync(releaseDelayed, "release");
      for (const response of pending) response.end();
      await control.close(); await participants.close();
      if (!lifetime.exited) launcher.kill("SIGTERM");
      for (const record of owned.owned()) if (same(record) && live(record.pid)) {
        try { process.kill(record.pid, "SIGKILL"); } catch { /* owned process exited */ }
      }
      await lifetime.exit;
      if (delayedLifetime) await delayedLifetime.exit;
      await until(() => owned.owned().every(record => !same(record) || !live(record.pid)));
      await new Promise<void>(resolve => { server.closeAllConnections(); server.close(() => resolve()); });
      fs.rmSync(root, { recursive: true, force: true });
    }
  }, 100_000);
});
