import fs from "node:fs";
import http from "node:http";
import path from "node:path";
import { createHash, randomUUID } from "node:crypto";
import { spawn, type ChildProcess } from "node:child_process";
import { fileURLToPath } from "node:url";
import { build } from "esbuild";
import { describe, expect, it } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { findExecutable } from "../src/agents/transports/process-utils.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import type { FabricActorInfo, FabricActorMessage } from "../src/actors/types.js";
import { watchResidentChild } from "../src/residency/child-lifetime.js";
import { residentRoot, type ResidentHostConfig, type ResidentHostOwner } from "../src/residency/protocol.js";
import type { ResidentMainGeneration } from "../src/residency/handover.js";
import { residentGenerationExitPath, type ResidentGenerationExit } from "../src/residency/generation-exit.js";
import { residentDrainReceiptPath, type ResidentDrainReceipt } from "../src/residency/drain.js";
import type { ResidentDrainReceipt as CliReceipt } from "../src/resident-drain-cli.js";
import { launchLog, same } from "./helpers/owned-processes.js";
import { residentDrainTemp } from "./helpers/resident-drain-temp.js";

// Keep: native launch/owner trust boundary and accepted queue data safety. The
// only substitute is inference (loopback SSE). Both launches use the real
// ResidencyClient.ensureHost; reconcileRelease creates Main authority itself.
// No owner, Main generation, actor record, queue or exit receipt is fabricated.
const repo = fileURLToPath(new URL("../", import.meta.url));
const readJson = <T>(file: string): T => JSON.parse(fs.readFileSync(file, "utf8")) as T;
const live = (pid: number): boolean => {
  try {
    const stat = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
    return !["Z", "X"].includes(stat.slice(stat.lastIndexOf(")") + 2).split(" ")[0]!);
  } catch { return false; }
};
type QueueItem = { id: string; payload?: { id?: string }; attempts?: number; executionStarted?: boolean };
type QueueSnapshot = { items: QueueItem[] };
type Published = { id: string; sequence: number };

// Filesystem commits and native child messages/exits drive normal observation.
// Each timer is a finite failure bound, not a repeating liveness poll.
function observeFixture(root: string) {
  const listeners = new Set<() => void>();
  let watchError: Error | undefined;
  const wake = () => { for (const listener of [...listeners]) listener(); };
  const watcher = fs.watch(root, { recursive: true, persistent: false }, wake);
  watcher.on("error", error => { watchError = error; wake(); });
  return { wake, close: () => watcher.close(),
    until: (test: () => boolean, what: string, ms = 60_000): Promise<void> => new Promise((resolve, reject) => {
      const finish = (error?: unknown) => { clearTimeout(timer); listeners.delete(check); if (error) reject(error); else resolve(); };
      const check = () => { try { if (watchError) throw watchError; if (test()) finish(); } catch (error) { finish(error); } };
      const timer = setTimeout(() => finish(new Error(`Native operator exit deadline: ${what}`)), ms);
      listeners.add(check); check();
    }),
  };
}

async function compileMain(root: string, oldRelease: string) {
  const runtimeRoot = path.join(root, "main-runtime");
  const entry = path.join(runtimeRoot, "dist/fixture/exit-main.mjs");
  fs.mkdirSync(path.dirname(entry), { recursive: true, mode: 0o700 });
  fs.copyFileSync(path.join(oldRelease, "package.json"), path.join(runtimeRoot, "package.json"));
  fs.cpSync(path.join(oldRelease, "dist/native"), path.join(runtimeRoot, "dist/native"), { recursive: true });
  fs.symlinkSync(path.join(oldRelease, "node_modules"), path.join(runtimeRoot, "node_modules"), "dir");
  const nativeDigests = Object.fromEntries(fs.readdirSync(path.join(runtimeRoot, "dist/native")).flatMap(name => {
    const file = path.join(runtimeRoot, "dist/native", name);
    return fs.statSync(file).isFile() && !name.endsWith(".json")
      ? [[name, createHash("sha256").update(fs.readFileSync(file)).digest("hex")]] : [];
  }));
  const sourceRoot = fs.existsSync(path.join(oldRelease, "src/residency/client.ts")) ? oldRelease : repo;
  const source = (file: string) => JSON.stringify(path.join(sourceRoot, "src", file));
  const contents = `import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import { pathToFileURL } from "node:url";
import { ParticipantDirectory } from ${source("topology/participant-directory.ts")};
import { FabricControlPlane } from ${source("topology/control-plane.ts")};
import { ResidencyClient } from ${source("residency/client.ts")};
import { processStartTime } from ${source("residency/process-identity.ts")};
const { config, identity, oldRelease, targetRelease } = JSON.parse(fs.readFileSync(process.argv[2], "utf8"));
const { MeshStore } = await import(pathToFileURL(path.join(oldRelease, "dist/mesh.js")).href);
const { ResidentActorClient } = await import(pathToFileURL(path.join(oldRelease, "dist/residency/actor-client.js")).href);
const mesh = new MeshStore(config.meshRoot, config.mesh.maxEventBytes, config.mesh.maxReadEvents);
const participants = new ParticipantDirectory(mesh, { enabled: true, hostId: identity.id, rootId: identity.id, identity });
const startedAt = Date.now();
participants.registerSource(() => [{ format: 1, id: identity.id, kind: "root", rootId: identity.id,
  ownerHostId: identity.id, ownerIdentityId: identity.id, name: identity.name, status: "idle", residency: "session",
  runner: "pi", transport: "host", capabilities: ["fabric", "followUp"], cwd: config.cwd, sessionId: identity.sessionId,
  startedAt, updatedAt: Date.now(), controlProtocol: "v1",
  mainProcess: { pid: process.pid, host: os.hostname(), startTime: processStartTime(process.pid) } }]);
const control = new FabricControlPlane(mesh, identity, { enabled: true, hostId: identity.id, pollMs: 20, acknowledgementTimeoutMs: 30_000 });
const actors = new ResidentActorClient(config.meshRoot, identity.id);
const makeClient = (desired, release) => new ResidencyClient({ config: desired, mesh, participants,
  mainAgent: { id: identity.id, local: true }, hostPath: path.join(release, "dist/residency/launcher.js") });
let client = makeClient(config, oldRelease);
await participants.start(); control.start(() => ({ accepted: false }));
const owner = await client.ensureHost();
await client.reconcileRelease();
client.start();
process.send({ ready: true, owner, main: JSON.parse(fs.readFileSync(path.join(config.residencyRoot, "main-generation.json"), "utf8")) });
process.on("message", async request => {
  try {
    let result;
    if (request.operation === "create") result = await actors.createActor(request.input);
    else if (request.operation === "status") result = await actors.actorStatus(request.input.id);
    else if (request.operation === "publish") result = await mesh.publish({ topic: request.input.topic,
      to: request.input.id, from: request.input.batched ? { id: identity.id + ":topic-source", name: "Native topic source", kind: "agent", sessionId: identity.sessionId } : identity,
      text: request.input.text, data: { proof: request.input.text } });
    else if (request.operation === "unbatch") result = await actors.setActor({ operation: "setBatching", id: request.input.id,
      batchWindowMs: 0 }, undefined, { identity, hostId: identity.id });
    else if (request.operation === "adopt-target") {
      // This is the normal runtime reload BEFORE drain, not a maintenance pause:
      // A client closes, B selects desired paths and adopts still-running A.
      await client.close();
      const desired = { ...config, fabricExtensionPath: path.join(targetRelease, "dist/index.js"), workerPath: path.join(targetRelease, "dist/worker.js") };
      client = makeClient(desired, targetRelease);
      client.syncPiModels();
      const adopted = await client.ensureHost();
      let deferred;
      try { await client.reconcileRelease(); } catch (error) { deferred = String(error); }
      client.start();
      const beforeLaunch = JSON.parse(fs.readFileSync(path.join(config.residencyRoot, "config.json"), "utf8"));
      fs.writeFileSync(path.join(config.cwd, "ordinary-launch-request.json"), JSON.stringify({ beforeLaunch, adopted, deferred,
        hostPath: path.join(targetRelease, "dist/residency/launcher.js") }));
      result = { owner: adopted, beforeLaunch, main: JSON.parse(fs.readFileSync(path.join(config.residencyRoot, "main-generation.json"), "utf8")) };
    } else if (request.operation === "launch-target") {
      // B Main has remained active throughout --exit. Its normal watchdog may
      // already have launched B; ensureHost joins that launch or starts it now.
      const beforeLaunch = JSON.parse(fs.readFileSync(path.join(config.residencyRoot, "config.json"), "utf8"));
      const owner = await client.ensureHost();
      await client.reconcileRelease();
      result = { owner, beforeLaunch };
    } else if (request.operation === "close") {
      await client.close(); await control.close(); await participants.close(); mesh.closeState();
      process.send({ id: request.id, result: { closed: true } }, () => process.exit(0)); return;
    } else throw new Error("Unknown native operator fixture request");
    process.send({ id: request.id, result });
  } catch (error) { process.send({ id: request.id, error: String(error && error.stack || error) }); }
});`;
  const compiled = await build({ absWorkingDir: sourceRoot, stdin: { contents, resolveDir: sourceRoot, sourcefile: "exit-main-driver.ts" },
    outfile: entry, bundle: true, packages: "external", platform: "node", format: "esm", target: "node24", metafile: true,
    define: { __FABRIC_NATIVE_DIGESTS__: JSON.stringify(JSON.stringify(nativeDigests)) }, logLevel: "silent" });
  fs.writeFileSync(path.join(root, "main-compile.json"), JSON.stringify({ sourceRoot, entry, nativeDigests,
    sha256: createHash("sha256").update(fs.readFileSync(entry)).digest("hex"), inputs: Object.keys(compiled.metafile!.inputs) }, null, 2));
  return entry;
}

function mainRpc(child: ChildProcess, wake: () => void) {
  const pending = new Map<string, { resolve: (value: unknown) => void; reject: (error: Error) => void; timer: ReturnType<typeof setTimeout> }>();
  let ready: { owner: ResidentHostOwner; main: ResidentMainGeneration } | undefined;
  const fail = (error: Error) => {
    for (const waiter of pending.values()) { clearTimeout(waiter.timer); waiter.reject(error); }
    pending.clear(); wake();
  };
  child.on("error", fail); child.on("exit", () => fail(new Error("Native fixture Main exited")));
  child.on("message", (message: { ready?: boolean; owner: ResidentHostOwner; main: ResidentMainGeneration; id?: string; result?: unknown; error?: string }) => {
    if (message.ready) ready = { owner: message.owner, main: message.main };
    const waiter = message.id ? pending.get(message.id) : undefined;
    if (waiter) { pending.delete(message.id!); clearTimeout(waiter.timer); if (message.error) waiter.reject(new Error(message.error)); else waiter.resolve(message.result); }
    wake();
  });
  return { ready: () => ready,
    call: <T>(operation: string, input: unknown = {}): Promise<T> => new Promise((resolve, reject) => {
      const id = randomUUID();
      const timer = setTimeout(() => { pending.delete(id); reject(new Error(`Native Main RPC deadline: ${operation}`)); }, 60_000);
      pending.set(id, { resolve: value => resolve(value as T), reject, timer });
      child.send({ id, operation, input }, error => { if (error) fail(error); });
    }),
  };
}

describe.skipIf(process.platform !== "linux")("native manual resident drain --exit", () => {
  it.each(["idle-queued", "in-flight"] as const)("%s: checks native stop/refusal and ordinary launch recovery", async mode => {
    const oldRelease = fs.realpathSync(process.env.FABRIC_DRAIN_TEST_OLD_RELEASE ?? process.env.OLD_RELEASE ?? repo);
    const privateTemp = residentDrainTemp(`resident-drain-exit-${mode}-`);
    const { root } = privateTemp;
    const newInput = process.env.FABRIC_DRAIN_TEST_NEW_RELEASE ?? process.env.NEW_RELEASE;
    const defaultTarget = path.join(root, "target-release");
    if (!newInput) {
      fs.mkdirSync(defaultTarget, { mode: 0o700 });
      fs.cpSync(path.join(oldRelease, "dist"), path.join(defaultTarget, "dist"), { recursive: true });
      fs.copyFileSync(path.join(oldRelease, "package.json"), path.join(defaultTarget, "package.json"));
      fs.symlinkSync(path.join(oldRelease, "node_modules"), path.join(defaultTarget, "node_modules"), "dir");
    }
    const targetRelease = fs.realpathSync(newInput ?? defaultTarget);
    expect(targetRelease).not.toBe(oldRelease);
    const node = findExecutable("node"); expect(node, "native Node runtime required").toBeTruthy();
    const piBinary = fs.realpathSync(path.join(oldRelease, "node_modules/@earendil-works/pi-coding-agent/dist/cli.js"));
    for (const release of [oldRelease, targetRelease]) for (const artifact of ["dist/index.js", "dist/worker.js", "dist/mesh.js",
      "dist/residency/launcher.js", "dist/residency/pi-entry.js", "dist/residency/actor-client.js", "dist/resident-drain-cli.js"]) {
      expect(fs.statSync(path.join(release, artifact)).isFile(), `real compiled artifact required: ${release}/${artifact}`).toBe(true);
    }
    const oldWorker = fs.statSync(path.join(oldRelease, "dist/worker.js")); const newWorker = fs.statSync(path.join(targetRelease, "dist/worker.js"));
    expect(`${oldWorker.dev}:${oldWorker.ino}`, "target must not alias the old build").not.toBe(`${newWorker.dev}:${newWorker.ino}`);
    const observation = observeFixture(root); const until = observation.until;
    const owned = launchLog(root);
    const agentDir = path.join(root, "agent"); fs.mkdirSync(agentDir, { mode: 0o700 });
    const meshRoot = path.join(root, "mesh");
    const identity = { id: `session:operator-exit-${mode}`, name: "Main", kind: "main", sessionId: `operator-exit-${mode}` };
    const config: ResidentHostConfig = { format: 1, rootId: identity.id, sessionId: identity.sessionId, cwd: root, projectRoot: root,
      meshRoot, actorRoot: path.join(root, "actors"), sessionActorRoot: path.join(root, "session-actors"), residencyRoot: residentRoot(meshRoot, identity.id), fullCodeMode: true,
      agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, notifyOnComplete: false, retainRuns: true, nice: 19, sessionExport: false, timeoutMs: 180_000 },
      mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20, residentIdleExitMs: 180_000 }, retention: DEFAULT_FABRIC_CONFIG.retention,
      workerPath: path.join(oldRelease, "dist/worker.js"), fabricExtensionPath: path.join(oldRelease, "dist/index.js"), piBinary,
      claudeBinary: "missing-claude", vedaBinary: "missing-veda", watchdog: { enabled: false },
      piModels: { available: [{ provider: "offline-fixture", id: "reply" }], aliases: {}, defaultModel: "offline-fixture/reply" } };
    fs.mkdirSync(config.residencyRoot, { recursive: true, mode: 0o700 });
    const setup = path.join(root, "main-setup.json"); fs.writeFileSync(setup, JSON.stringify({ config, identity, oldRelease, targetRelease }));
    const env: NodeJS.ProcessEnv = Object.fromEntries(["PATH", "LANG", "LC_ALL", "GITHUB_ACTIONS", "CI", "RUNNER_ENVIRONMENT", "RUNNER_TEMP", "PI_TEST_FLEET_HOST"]
      .flatMap(key => process.env[key] === undefined ? [] : [[key, process.env[key]]]));
    Object.assign(env, owned.env, privateTemp.childEnvironment(owned.env.NODE_OPTIONS!), { PI_OFFLINE: "1", PI_CODING_AGENT_DIR: agentDir,
      PI_FABRIC_MESH_ROOT: meshRoot, PI_FABRIC_PROJECT_ROOT: root, PI_FABRIC_RUN_ROOT: path.join(root, "runs"),
      PI_FABRIC_AGENT_DIR: path.join(root, "exports"), PI_FABRIC_PI_BINARY: piBinary, PI_FABRIC_NODE_BINARY: node });
    expect(env.NODE_OPTIONS).toContain("test-deletion-guard.mjs"); expect(env.NODE_OPTIONS).toContain("launch-preload.mjs");
    const requests: Array<{ index: number; model: string; messages: unknown }> = [];
    const responses = new Map<number, http.ServerResponse>(); const pending = new Set<http.ServerResponse>();
    let autoReply = false;
    const reply = (index: number) => {
      const response = responses.get(index)!;
      expect(response.destroyed || response.writableEnded, "accepted inference must not be aborted by exit refusal").toBe(false);
      response.writeHead(200, { "Content-Type": "text/event-stream" });
      const chunk = (delta: unknown, finish_reason: string | null = null) => response.write(`data: ${JSON.stringify({
        id: `native-exit-${index}`, object: "chat.completion.chunk", created: 1, model: "reply", choices: [{ index: 0, delta, finish_reason }],
      })}\n\n`);
      chunk({ role: "assistant", content: `EXIT_REPLY_${index}` }); chunk({}, "stop"); response.end("data: [DONE]\n\n");
    };
    const server = http.createServer((request, response) => {
      pending.add(response); response.once("close", () => { pending.delete(response); observation.wake(); });
      let body = ""; request.on("data", chunk => { body += chunk; });
      request.on("end", () => {
        const payload = JSON.parse(body) as { model: string; messages: unknown };
        const index = requests.length + 1; requests.push({ index, ...payload }); responses.set(index, response);
        fs.appendFileSync(path.join(root, "provider-requests.jsonl"), JSON.stringify(requests.at(-1)) + "\n");
        if (autoReply) reply(index); observation.wake();
      });
    });
    const registries = { project: new ActorRegistryStore(config.actorRoot), session: new ActorRegistryStore(config.sessionActorRoot!) };
    const actors: Array<FabricActorInfo & { scope: "project" | "session" }> = [];
    const queueItems = (actor: typeof actors[number]): QueueItem[] => {
      const actorRoot = actor.scope === "project" ? config.actorRoot : config.sessionActorRoot!;
      const directory = path.join(actorRoot, actor.id);
      return fs.readdirSync(directory).filter(name => /^queue-.*\.json$/.test(name)).sort()
        .flatMap(name => readJson<QueueSnapshot>(path.join(directory, name)).items);
    };
    const history = (actor: typeof actors[number]): FabricActorMessage[] => {
      const registry = registries[actor.scope]; const row = registry.records().find(record => record.id === actor.id);
      return row ? registry.messages(row) as FabricActorMessage[] : [];
    };
    const ownerPath = path.join(config.residencyRoot, "owner.json");
    let mainChild: ChildProcess | undefined; let mainLifetime: ReturnType<typeof watchResidentChild> | undefined;
    const commandLifetimes: Array<ReturnType<typeof watchResidentChild>> = [];
    let stderr = ""; let proof: Record<string, unknown> | undefined;
    try {
      await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
      fs.writeFileSync(path.join(agentDir, "models.json"), JSON.stringify({ providers: { "offline-fixture": {
        baseUrl: `http://127.0.0.1:${(server.address() as { port: number }).port}/v1`, apiKey: "offline-fixture-only", api: "openai-completions",
        models: [{ id: "reply", name: "reply", reasoning: false, input: ["text"], contextWindow: 200000, maxTokens: 4096,
          cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } }],
      } } }));
      fs.writeFileSync(path.join(agentDir, "settings.json"), JSON.stringify({ enableInstallTelemetry: false, compaction: { enabled: false }, retry: { enabled: false } }));
      fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({ fullCodeMode: true,
        agents: { notifyOnComplete: false, retainRuns: true, nice: 19, sessionExport: false }, mesh: { enabled: true, actorPollMs: 20 },
        mcp: { enabled: false }, memory: { enabled: false }, jev: { enabled: false }, residency: { enabled: false }, prewalk: { enabled: false, alwaysRearm: false } }));
      const mainEntry = await compileMain(root, oldRelease);
      mainChild = spawn(node!, [mainEntry, setup], { cwd: root, env, stdio: ["ignore", "ignore", "pipe", "ipc"] });
      mainLifetime = watchResidentChild(mainChild); const rpc = mainRpc(mainChild, observation.wake);
      mainChild.stderr!.on("data", chunk => { stderr = `${stderr}${chunk}`.slice(-18000); });
      void mainLifetime.exit.then(observation.wake);
      await until(() => { if (mainLifetime!.exited) throw new Error(`Real Main startup failed: ${stderr}`); return !!rpc.ready(); }, "ordinary client launches A and records Main authority");
      const { owner } = rpc.ready()!;
      let mainGeneration = rpc.ready()!.main;
      expect(owner).toMatchObject({ releaseRoot: oldRelease, drain: { abi: "fabric-resident-drain-1" }, requestFence: 1 });
      expect(mainGeneration).toMatchObject({ pid: mainChild.pid, rootId: identity.id, sessionId: identity.sessionId, releaseRoot: oldRelease });
      expect(readJson<ResidentMainGeneration>(path.join(config.residencyRoot, "main-generation.json"))).toEqual(mainGeneration);
      const ownerBefore = fs.readFileSync(ownerPath, "utf8");
      await until(() => [owner.pid, owner.handover!.launcher.pid].every(pid => owned.owned().some(row => row.pid === pid)), "A host and launcher self-recorded births");
      const aHost = owned.owned().find(row => row.pid === owner.pid)!;
      const aLauncher = owned.owned().find(row => row.pid === owner.handover!.launcher.pid)!;
      expect(aHost.started).toBe(owner.processStartTime); expect(aHost.argv).toContain(path.join(oldRelease, "dist/residency/pi-entry.js"));
      expect(aLauncher.argv[0]).toBe(path.join(oldRelease, "dist/residency/launcher.js"));
      const scopes = mode === "idle-queued" ? ["project", "session"] as const : ["project"] as const;
      const publications: Published[] = [];
      for (const scope of scopes) {
        const topic = `native.operator-exit.${scope}`;
        const actor = await rpc.call<FabricActorInfo>("create", { name: `operator-exit-${scope}`, scope, instructions: "Reply briefly to each input.",
          residency: "durable", model: "offline-fixture/reply", transport: "process", tools: [], extensions: false, delivery: "mailbox",
          responseMode: "text", coalesce: false, topics: [topic], batchWindowMs: mode === "idle-queued" ? (scope === "session" ? 60_000 : 300_000) : 0 });
        actors.push({ ...actor, scope });
        // Main messages intentionally bypass batching. A real non-Main topic
        // producer exercises the idle accepted-queue path instead.
        publications.push(await rpc.call<Published>("publish", { id: actor.id, topic, text: `ORIGINAL_${scope}_INPUT`, batched: mode === "idle-queued" }));
      }
      await until(() => actors.every((actor, index) => queueItems(actor).some(item => item.payload?.id === publications[index]!.id)), "real topic inputs accepted into original durable queues");
      if (mode === "in-flight") await until(() => requests.length === 1 && responses.has(1), "accepted native worker inference in flight");
      else expect(requests).toHaveLength(0);
      const adopted = await rpc.call<{ owner: ResidentHostOwner; beforeLaunch: ResidentHostConfig; main: ResidentMainGeneration }>("adopt-target");
      expect(adopted.owner.token).toBe(owner.token);
      expect(adopted.beforeLaunch).toMatchObject({ fabricExtensionPath: path.join(targetRelease, "dist/index.js"), workerPath: path.join(targetRelease, "dist/worker.js") });
      mainGeneration = adopted.main;
      expect(mainGeneration).toMatchObject({ pid: mainChild.pid, rootId: identity.id, releaseRoot: targetRelease });
      expect(fs.readFileSync(ownerPath, "utf8")).toBe(ownerBefore);
      expect(owned.owned().filter(row => row.argv.some(arg => arg.startsWith(path.join(targetRelease, "dist") + path.sep)))).toEqual([]);
      const oldLaunchesBeforeExit = owned.owned().filter(row => row.argv[0] === path.join(oldRelease, "dist/residency/launcher.js")).map(row => row.pid);
      const queuesBefore = actors.map(actor => ({ actorId: actor.id, scope: actor.scope, topics: actor.topics, items: queueItems(actor) }));
      fs.writeFileSync(path.join(root, "queues-before.json"), JSON.stringify(queuesBefore, null, 2));
      const command = path.join(repo, "bin/fabric-resident-drain");
      const cli = spawn(node!, [command, "--exit", "--resident", config.residencyRoot, "--mesh-root", meshRoot,
        "--expected-release", oldRelease, "--target-release", targetRelease], { cwd: root, env, stdio: ["ignore", "pipe", "pipe"] });
      const cliLifetime = watchResidentChild(cli); commandLifetimes.push(cliLifetime);
      let stdout = ""; let cliStderr = "";
      cli.stdout!.on("data", chunk => { stdout += chunk; }); cli.stderr!.on("data", chunk => { cliStderr += chunk; });
      const closed = new Promise<void>((resolve, reject) => { cli.once("close", () => resolve()); cli.once("error", reject); });
      const deadline = setTimeout(() => { void cliLifetime.stop().catch(() => undefined); }, 135_000);
      let cliExit: Awaited<typeof cliLifetime.exit>;
      try { cliExit = await cliLifetime.exit; await closed; } finally { clearTimeout(deadline); }
      expect(cliExit.signal, cliStderr).toBeNull();
      expect(stdout.trim().split("\n")).toHaveLength(1);
      fs.writeFileSync(path.join(root, "drain-cli.stdout"), stdout);
      fs.writeFileSync(path.join(root, "drain-cli.stderr"), cliStderr);
      const receipt = JSON.parse(stdout) as CliReceipt;
      expect(stdout).not.toContain(owner.token); expect(stdout).not.toContain(owner.handover!.launcher.token); expect(stdout).not.toContain(mainGeneration.nonce);
      expect(receipt).toMatchObject({ resident: path.relative(meshRoot, config.residencyRoot), expected: path.relative(meshRoot, oldRelease), target: path.relative(meshRoot, targetRelease) });
      expect(owned.owned().filter(row => row.argv[0] === path.join(oldRelease, "dist/residency/launcher.js")).map(row => row.pid)).toEqual(oldLaunchesBeforeExit);
      if (mode === "in-flight") {
        expect(owned.owned().filter(row => row.argv.some(arg => arg.startsWith(path.join(targetRelease, "dist") + path.sep)))).toEqual([]);
        expect(cliExit.code).toBe(1);
        expect(receipt).toMatchObject({ ok: false, phase: "refused", outcome: "refused", code: "DRAIN_EXIT_ACTORS_BUSY" });
        expect(fs.readFileSync(ownerPath, "utf8")).toBe(ownerBefore);
        expect(same(aHost) && live(aHost.pid)).toBe(true); expect(same(aLauncher) && live(aLauncher.pid)).toBe(true);
        expect(fs.existsSync(residentGenerationExitPath(config.residencyRoot, owner.token))).toBe(false);
        expect(queueItems(actors[0]!)).toEqual(queuesBefore[0]!.items);
        expect(responses.get(1)!.destroyed || responses.get(1)!.writableEnded).toBe(false);
        reply(1);
        await until(() => history(actors[0]!).filter(message => message.direction === "out").length === 1 && queueItems(actors[0]!).length === 0, "in-flight work completes once on refused A");
        expect(history(actors[0]!).filter(message => message.direction === "out").map(message => message.text)).toEqual(["EXIT_REPLY_1"]);
        proof = { mode, oldRelease, targetRelease, mainGeneration, owner, receipt, cliExit, queuesBefore, requests, sameOwnerServing: true, targetLaunches: 0 };
      } else {
        expect(cliExit.code, `${cliStderr}\n${stdout}`).toBe(0);
        expect(receipt).toMatchObject({ ok: true, phase: "exited", outcome: "exited", drain: { admissionHeld: true, terminal: true,
          checkpointReadback: true, hostExit: { pid: owner.pid, reason: "stopped", processExited: true } } });
        expect(receipt.reason).toContain("not launched or verified serving");
        const terminal = readJson<ResidentDrainReceipt>(residentDrainReceiptPath(config.residencyRoot, receipt.drain!.id));
        const stopped = readJson<ResidentGenerationExit>(residentGenerationExitPath(config.residencyRoot, owner.token));
        expect(terminal).toMatchObject({ owner: { pid: owner.pid, token: owner.token, processStartTime: owner.processStartTime },
          phase: "exited", outcome: "exited", admissionHeld: true, checkpointReadback: true });
        expect(stopped).toMatchObject({ generation: owner.token, pid: owner.pid, writer: "host", reason: "stopped" });
        expect(same(aHost) && live(aHost.pid)).toBe(false);
        if (fs.existsSync(ownerPath)) expect(readJson<ResidentHostOwner>(ownerPath).token).not.toBe(owner.token);
        // The ordinary client's detached launcher has no public native-close
        // handle. After the CLI's process-exit proof, allow one finite scheduling
        // grace, then independently read the exact launch-recorded birth once.
        await new Promise<void>(resolve => setTimeout(resolve, 1_000));
        expect(same(aLauncher) && live(aLauncher.pid), "A launcher exited; any B launch belongs to the active Main's ordinary path").toBe(false);
        expect(actors.map(actor => queueItems(actor))).toEqual(queuesBefore.map(row => row.items));
        for (const actor of actors) {
          expect(registries[actor.scope].records().find(row => row.id === actor.id)).toMatchObject({ id: actor.id, topics: actor.topics });
        }
        const relaunched = await rpc.call<{ owner: ResidentHostOwner; beforeLaunch: ResidentHostConfig }>("launch-target");
        const next = relaunched.owner;
        expect(relaunched.beforeLaunch).toMatchObject({ fabricExtensionPath: path.join(targetRelease, "dist/index.js"), workerPath: path.join(targetRelease, "dist/worker.js") });
        expect(readJson<ResidentHostConfig>(path.join(config.residencyRoot, "config.json"))).toMatchObject(relaunched.beforeLaunch);
        expect(next).toMatchObject({ hostId: owner.hostId, releaseRoot: targetRelease, fabricExtensionPath: path.join(targetRelease, "dist/index.js") });
        expect(next.token).not.toBe(owner.token); expect(next.configDigest).not.toBe(owner.configDigest);
        await until(() => owned.owned().some(row => row.pid === next.pid), "ordinary target launch self-recorded host birth");
        expect(owned.owned().find(row => row.pid === next.pid)!.argv).toContain(path.join(targetRelease, "dist/residency/pi-entry.js"));
        const recovered = await Promise.all(actors.map(actor => rpc.call<FabricActorInfo>("status", { id: actor.id })));
        recovered.forEach((actor, index) => {
          expect(actor).toMatchObject({ id: actors[index]!.id, topics: actors[index]!.topics, queued: queuesBefore[index]!.items.length });
          expect(actor.inFlightRun).toBeUndefined();
          expect(queueItems(actors[index]!)).toEqual(queuesBefore[index]!.items);
        });
        autoReply = true;
        // The existing remote setter addresses project actors only. Exercise
        // it there; session input resumes under its real retained batch policy.
        for (const actor of actors.filter(actor => actor.scope === "project")) await rpc.call("unbatch", { id: actor.id });
        await until(() => actors.every(actor => history(actor).filter(message => message.direction === "out").length === 1 && queueItems(actor).length === 0), "B executes both recovered original queues once");
        expect(requests).toHaveLength(2);
        for (const [index, actor] of actors.entries()) {
          expect(history(actor).filter(message => message.direction === "in" &&
            (message.data as { id?: string } | undefined)?.id === publications[index]!.id)).toHaveLength(1);
          expect(history(actor).filter(message => message.direction === "out")).toHaveLength(1);
          expect(history(actor).some(message => message.error !== undefined || message.stale === true)).toBe(false);
          expect(JSON.stringify(requests.filter(request => JSON.stringify(request.messages).includes(`ORIGINAL_${actor.scope}_INPUT`)))).not.toBe("[]");
        }
        expect(owned.owned().filter(row => row.argv[0] === path.join(oldRelease, "dist/residency/launcher.js")).map(row => row.pid)).toEqual(oldLaunchesBeforeExit);
        expect(owned.owned().filter(row => row.argv[0] === path.join(targetRelease, "dist/residency/launcher.js"))).toHaveLength(1);
        expect(owned.owned().filter(row => row.argv[0] === path.join(targetRelease, "dist/worker.js"))).toHaveLength(2);
        proof = { mode, oldRelease, targetRelease, mainGeneration, owner, receipt, cliExit, terminal, stopped, queuesBefore, next, recovered,
          requests, ordinaryLaunch: readJson(path.join(root, "ordinary-launch-request.json")), histories: actors.map(actor => ({ id: actor.id, messages: history(actor) })) };
      }
      fs.writeFileSync(path.join(root, "proof.json"), JSON.stringify(proof, null, 2));
      await rpc.call("close"); expect(await mainLifetime.exit).toEqual({ code: 0, signal: null });
    } catch (error) {
      let childLog = ""; try { childLog = fs.readFileSync(path.join(config.residencyRoot, "child-stderr.log"), "utf8").slice(-18000); } catch { /* no host yet */ }
      fs.writeFileSync(path.join(root, "failure.json"), JSON.stringify({ error: String(error), stderr, childLog, requests }, null, 2));
      throw new Error(`${error instanceof Error ? error.stack : error}\nretained native fixture: ${root}\nMain: ${stderr}\nHost: ${childLog}`);
    } finally {
      for (const response of pending) if (!response.writableEnded) response.end();
      try {
        await Promise.all(commandLifetimes.filter(lifetime => !lifetime.exited).map(lifetime => lifetime.stop()));
        if (mainLifetime && !mainLifetime.exited) await mainLifetime.stop();
      } finally {
        // Cleanup is not an attempt-containment claim. Signal only processes
        // that recorded their own exact launch birth inside this private fixture.
        for (const row of owned.owned()) if (same(row) && live(row.pid)) {
          try { process.kill(row.pid, "SIGKILL"); } catch { /* exact birth already exited */ }
        }
        if (mainLifetime) await mainLifetime.exit;
        for (const registry of Object.values(registries)) registry.releaseReadCache();
        await new Promise<void>(resolve => { server.closeAllConnections(); server.close(() => resolve()); });
        await new Promise<void>(resolve => setTimeout(resolve, 1_000));
        const liveFixturePids = owned.owned().filter(row => same(row) && live(row.pid)).map(row => row.pid);
        const cleanup = { retained: true, liveFixturePids, launches: owned.owned() };
        fs.writeFileSync(path.join(root, "cleanup.json"), JSON.stringify(cleanup, null, 2)); observation.close();
        expect(liveFixturePids, "all fixture-owned native processes must be stopped").toEqual([]);
        privateTemp.assertNoOutsideDeletion();
        const proofOut = process.env.FABRIC_DRAIN_PROOF_OUT;
        if (proof && proofOut) {
          const evidenceDir = path.resolve(`${proofOut}.artifacts`, `exit-${mode}`); fs.mkdirSync(evidenceDir, { recursive: true, mode: 0o700 });
          for (const name of ["proof.json", "cleanup.json", "queues-before.json", "ordinary-launch-request.json", "main-compile.json", "provider-requests.jsonl", "launches.jsonl", "deletion-guard-audit.jsonl"]) {
            const file = path.join(root, name);
            if (fs.existsSync(file)) fs.writeFileSync(path.join(evidenceDir, name), fs.readFileSync(file), { flag: "wx", mode: 0o600 });
          }
          fs.mkdirSync(path.dirname(path.resolve(proofOut)), { recursive: true }); fs.appendFileSync(proofOut, JSON.stringify({ ...proof, cleanup, evidenceDir }) + "\n");
        }
        // Retain all fixture files; no cleanup deletion runs in this suite.
      }
    }
  }, 240_000);
});
